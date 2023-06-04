/* =======================================================================
 *   (c) Copyright Hewlett-Packard Development Company, L.P., 2008
 *
 *   This program is free software: you can redistribute it and/or modify
 *   it under the terms of version 2 the GNU General Public License as
 *   published by the Free Software Foundation.
 *   
 *   This program is distributed in the hope that it will be useful,
 *   but WITHOUT ANY WARRANTY; without even the implied warranty of
 *   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *   GNU General Public License for more details.
 *   
 *   You should have received a copy of the GNU General Public License
 *   along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * =======================================================================
 */
/* 
 * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
 *
 *
 * Facility:
 *
 *     AdvFS 
 *
 * Abstract:
 *
 *      Routines for tag directory functions.
 *
 */

#include <ms_public.h>
#include <ms_privates.h>
#include <ms_assert.h>
#include <ms_osf.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE BS_TAGDIR

/* 
 * Viewing tag directories as an abstract data type, the operations are:
 *
 *     Given a bitfile set, allocate a tagmap structure and
 *     return the corresponding tag (tagdir_alloc_tag)
 *
 *     Given a tag and a bitfile set, initialize the corresponding 
 *     tagmap structure (tagdir_insert_tag)
 *
 *     Given a tag and a bitfile set, initialize the corresponding 
 *     tagmap structure (tagdir_set_bfdesc)
 *
 *     Given a tag and a bitfile set,  deallocate the corresponding 
 *     tagmap structure (tagdir_remove_tag)
 *
 *     Given a tag and a bitfile set, return the corresponding virtual 
 *     disk index, and bitfile metadata table cell id (tagdir_lookup)
 *
 *     Given a tag and a bitfile set, return the virtual disk index,
 *     tag, and bitfile metadata table cell id of the next
 *     in-use tagmap struct (tagdir_lookup_next)
 *
 *     Given a bitfile set, return the page number of the first page
 *     in the free list, the page number of the first unmapped page, and
 *     the page number of the first uninitialized page. (tagdir_get_info)
 *     
 * The implementation uses a two level hierarchy.  A page consists of
 * a number of tagmaps.  When a page contains at least one
 * available tagmap, it is linked into a singly linked free list
 * of such pages.  Within a page the free tagmaps are linked into a 
 * subordinate singly linked list.
 *
 * To allocate a tag, remove the first tagmap from the first
 * page from its subordinate free list.  If it is the last free tagmap 
 * on the page, remove the page from the list of free pages. 
 *
 * To deallocate a tag, place the  tagmap on the head of the subordinate 
 * free list within its page.  If the page is already on the list of free 
 * pages, there is nothing else to do.  Otherwise, this page was previously 
 * full and this deallocation will force the page back to the head of the 
 * list of free pages.
 *
 * Finally, we must always be able to find (at mount time) the head of the 
 * free list, the next page to allocate, and the next page to initialize.
 */

/*
 * For AdvFS on-disk version = FIRST_RBMT_VERSION, the mapping of
 * reserved tags to <virtual disk, mcell> pairs has been changed to the
 * following:
 *
 * -(BFM_RSVD_TAGS) --> <1, BFM_RBMT>
 * -(BFM_RSVD_TAGS + 1) --> <1, BFM_SBM>
 * -(BFM_RSVD_TAGS + 2) --> <1, BFM_BFSDIR>
 * -(BFM_RSVD_TAGS + 3) --> <1, BFM_FTXLOG>
 * -(BFM_RSVD_TAGS + 4) --> <1, BFM_BMT>
 * -(BFM_RSVD_TAGS + 6) --> <2, BFM_RBMT>, etc.
 *
 * Thus, -BFM_RSVD_TAGS is the largest (least negative) reserved tag.
 * Also virtual disk indices are 1 based.
 *
 ************************************************************************
 *
 * Within a page, the relationship between slot numbers, subordinate free
 * list numbers, and tag numbers is as follows (assume 4 slots per page):
 *
 *     __________________________________________________
 *     |slot#        0   1   2   3  |  0   1   2   3 ...|
 *     |free list#   -   2   3   4  |  1   2   3   4 ...|
 *     |tag #        -   1   2   3  |  4   5   6   7 ...|
 *     --------------------------------------------------
 */

/*
 * Note that this module only guarantees
 * consistency of the free list.  Races between create/delete,
 * lookup/delete, migrate/delete, etc., must be handled at a higher level.
 *
 * TAG_DIR_LOCK must be done before (or always after) rbf_pinpg because
 * it is possible for a pin page operation to block waiting for another pin
 * page operation on that same page to finish.  Otherwise, two threads A 
 * and B can deadlock as follows:
 *
 *      |           A                           B
 *      |
 * time |       get lock
 *      |                               pin page (succeeds)
 *      V       pin page (blocks)
 *                                      get lock (blocks)
 *
 * This phenomenon can happen on a hot page, such as the first page
 * of the tag directory.  The current solution is to take the lock
 * before pinning the page.  This is necessary to preserve consistency
 * of the free list.  Unfortunately, parallelism is reduced because
 * pin page can block in disk I/O.  It seems difficult to recode
 * so that the lock can be taken after pages are pinned.  Such a plan
 * will probably need two locks, one for page zero of the tag
 * directory and one for all other pages.
 *
 * TODO: Implement locking after pin page by using two locks.
 */

/* Prototypes for static functions. */

static statusT
tagdir_set_tagmap_common(
    tagInfoT *tip,
    int markInUse
    );

/* Forward declartions for undo/rtdn functions. */
opxT extend_tagdir_redo_opx;
opxT tagdir_write_undo_opx;
opxT switch_root_tagdir_redo_opx;

/* Types used by undo/rtdn functions. */
typedef struct {
    bfTagT tag;
    bfSetIdT bfSetId;
    bsTMapT map;
} tagDataRecT;

typedef struct {
    bfSetIdT       bfSetId;
    bs_meta_page_t unInitPage;
} tagUnInitPageRedoT;

#define FIRST_SETTABLE_TAG 6

/*
 * tagdir_get_info
 *
 * Return information needed to initialize the bfSet structure.
 *    
 * Note: no locking is done.  
 *
 * TODO: Is a free list operation racing with this function possible?
 */

statusT
tagdir_get_info(
    bfSetT *bfSetp,                         /* in - bitfile set desc ptr */
    bs_meta_page_t *firstUninitPage,        /* out - next tagdir page to use */
    bs_meta_page_t *firstUnmappedPage,      /* out - first unmapped tagdir page */
    bs_meta_page_t *freeListHead,           /* out - start of free list */
    int *bfCnt                              /* out - num bitfiles in set */
    )
{
    bfAccessT *tdap;
    statusT sts;
    bsTDirPgT *tdpgp;
    bfPageRefHT pgRef;
    bs_meta_page_t pg;


    tdap = bfSetp->dirBfAp;

    *firstUnmappedPage = tdap->bfaNextFob / tdap->bfPageSz;
    *bfCnt = 0;
    *firstUninitPage = 0;
    *freeListHead = 0;      /* indicates empty free list */

    if (tdap->bfaNextFob == 0) {
        return (EOK);
    }

    /*
     * The page number of the head of the free list and the page number
     * of the first uninitialized page is stored in the first tagmap
     * structure on page 0.
     */
    if ((sts = bs_refpg(&pgRef,
                        (void *)&tdpgp,
                        bfSetp->dirBfAp, 
                        0, 
                        FtxNilFtxH,
                        MF_VERIFY_PAGE)) != EOK) {
        return (sts);
    }
    *freeListHead = tdpgp->tMapA[0].tmFreeListHead;
    *firstUninitPage = tdpgp->tMapA[0].tmUninitPg;
    *bfCnt = tdpgp->tpNumAllocTMaps;
    (void) bs_derefpg(pgRef, BS_CACHE_IT);

    for (pg = 1; pg < *firstUninitPage ; pg++) {
        sts = bs_refpg(&pgRef, 
                       (void *)&tdpgp, 
                       bfSetp->dirBfAp, 
                       pg,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            return sts;
        }

        *bfCnt += tdpgp->tpNumAllocTMaps;

        (void) bs_derefpg(pgRef, BS_CACHE_IT);
    }

    return EOK;
}

/*
 * init_tagdir_opx
 *
 * Register the FTX agents for tag directory management.
 */


void
init_tagdir_opx(void)
{
    statusT sts;

    if ((sts = ftx_register_agent_n(FTA_BS_TAG_EXTEND_TAGDIR_V1, 
                                  0, 0, extend_tagdir_redo_opx)) != EOK) {

        ADVFS_SAD1("init_tagdir_opx: can't register agent", sts);
    }

    if ((sts = ftx_register_agent(FTA_BS_TAG_WRITE_V1, 
                                  tagdir_write_undo_opx, 0)) != EOK) {

        ADVFS_SAD1("init_tagdir_opx: can't register agent", sts);
    }

    if ((sts = ftx_register_agent_n(FTA_BS_SWITCH_ROOT_TAGDIR_V1,
                                  0, 0, switch_root_tagdir_redo_opx)) != EOK) {

        ADVFS_SAD1("init_tagdir_opx: can't register agent", sts);
    }
}

/*
 * Marks tags as not in use.  Later bs_close_one will reallocate
 * the tag number back onto the free list of tags.
 * bs_close_one is used because it is always called for the last
 * accessor of the file.
 */

void
tagdir_remove_tag(
    bfSetT *bfSetp,      /* in - bitfile set from which to free tag */
    bfTagT *tp,          /* in - pointer to tag */
    ftxHT ftxH           /* in - transaction handle */
    )
{
    statusT sts;
    bs_meta_page_t tagPg;
    int slot;
    rbfPgRefHT pgRef;
    bsTDirPgT *tdpgp;
    bsTMapT *tdmap;
    int addToFreeList;

    tagPg = TAGTOPG(tp);
    slot = TAGTOSLOT(tp);

    /* Pseudo-tag should not come here. */
    MS_SMP_ASSERT(BS_BFTAG_PSEUDO(*tp) == 0);

    ADVFTM_SETTAGDIR_LOCK(bfSetp, ftxH);

    /* Pin the page containing the tag to remove. */
    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, tagPg, 
                         BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        ADVFS_SAD1("tagdir_remove_tag:  can't pin page", tagPg);
    }

    tdmap = &tdpgp->tMapA[slot];

    /*
     * Because this routine is called by bs_delete_rtdn_opx() and
     * bs_delete_rtdn_opx() is called when the file was removed, and
     * may be called again at recovery time, BS_TD_IN_USE must not be
     * included in the assertion.
     */

    /* BS_TD_IN_USE no longer stored in tmSeqNo, it is stored in tmFlags  */
    MS_SMP_ASSERT(tp->tag_seq == tdmap->tmSeqNo);
    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));
    /*
     * *** IMPORTANT *** 
     *
     * Only clear the BS_TD_IN_USE bit now.  Until the file is removed from
     * the deferred delete list, this is the only copy of the file flags and
     * they will be needed during cluster failover or domain activation
     * after a crash.
     *
     * Why?  The file may still be open and in a cluster failover the new
     * server will need to be able to re-open the file from the deferred
     * delete list on behalf of the client node that still has the file
     * open.  Things like the dataSafety bits are in here, as well as file
     * behavioral bits (current and future).  If we clear these bits now, we
     * will not be able to successful failover the still opened file.  
     *
     * During crash recovery the deferred delete list processing finally
     * deletes the file by re-opening it and then allowing the normal last
     * close processing to clean up the storage.
     *
     * The rest of the flag bits will be cleared when deferred delete list
     * processing is complete and this tag is put on the tagdir free list.
     */
    tdmap->tmFlags &= ~(BS_TD_IN_USE);
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_293);

}

/*
 * For tag allocation/removal, the following invarinant is maintained:
 * If a tag is on the free list then its sequence number is the next
 * sequence number to use.  Thus sequence numbers are incremented upon
 * removal of a tag, and slots are (possibly) marked dead at that time.
 *
 * Sequence numbers start with one.  A tag with a sequence number of
 * zero is valid for lookup since sequence numbers will be ignored in
 * the test for a match.  It is unclear whether this wildcard feature
 * is really needed.
 */


void
tagdir_tag_to_freelist(
    bfSetT *bfSetp,      /* in - bitfile set from which to free tag */
    bfTagT *tp,          /* in - pointer to tag */
    ftxHT ftxH,          /* in - transaction handle */
    int set_tag_to_unused/* in - if TRUE, turn off in-use flag */
    )
{
    statusT sts;
    bs_meta_page_t tagPg;
    int slot;
    rbfPgRefHT pgRef;
    bsTDirPgT *tdpgp;
    bsTMapT *tdmap;
    int addToFreeList;
    unsigned int seqno=0;

    /* Pseudo-tag should not come here. */
    MS_SMP_ASSERT(BS_BFTAG_PSEUDO(*tp) == 0);

    tagPg = TAGTOPG(tp);
    slot = TAGTOSLOT(tp);

    ADVFTM_SETTAGDIR_LOCK(bfSetp, ftxH);

    /* Pin the page containing the tag to remove. */
    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, tagPg,
                         BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        domain_panic(bfSetp->dmnP, "tagdir_tag_to_freelist:  can't pin page %d",
                     tagPg);
        return;
    }

    tdmap = &tdpgp->tMapA[slot];
    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));

    /*
     * If domain panic happened before bs_delete_rtdn_opx() was called
     * during rbf_delete(), then bfs_open() would fail and
     * bs_tagdir_remove() would not be called, and BS_TD_IN_USE bit
     * would still be set so we cannot do the assertion below.
     */
    if (! set_tag_to_unused && ! bfSetp->dmnP->dmn_panic) {
        /*
         * The following assert checks to make sure the IN_USE flag is NOT
         * still set on the tag to be deleted if the set_tag_to_unused 
         * flag is not set.
         */
        MS_SMP_ASSERT((tp->tag_seq == tdmap->tmSeqNo) &&
                       !(tdmap->tmFlags & BS_TD_IN_USE));
    }

    /* 
     * zero out all bsTMap flags when tag is freed 
     * - no current need for persistent flag info for freed tags 
     */
    tdmap->tmFlags = 0;
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_294);

    rbf_pin_record(pgRef, (void *)&tdpgp->tpPgHdr, sizeof(struct bsTDirPgHdr));

    tdpgp->tpNumAllocTMaps--;
    bfSetp->bfCnt--;
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_295);

    if (++tdmap->tmSeqNo > BS_TD_MAX_SEQ) {
        /*
         * Allow wrapping for Tag Sequence Num since it won't be an issue 
         * for the log with 31 bits to use.  Need to reserve bit 32 for
         * BS_PSEUDO_TAG.
         */
        tdmap->tmSeqNo = 1;
    }

    /*
     * Place the freed tagmap on the head of the subordinate tagmap
     * list within the page.
     */
    addToFreeList = tdpgp->tpNextFreeMap == 0;
    tdmap->tmNextMap = tdpgp->tpNextFreeMap;
    tdpgp->tpNextFreeMap = slot + 1;

    /*
     * If freeing a tag on a page that was previously full, place this
     * page at the head of the free list.
     */
    if (addToFreeList) {
        bsTDirPgT *tdpg0p;

        /* Pin page 0, if not already pinned. */
        if (tagPg != 0) {
            if ((sts = rbf_pinpg(&pgRef, (void **)&tdpg0p,
                                 bfSetp->dirBfAp, (uint64_t)0,
                                 BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
                domain_panic(bfSetp->dmnP,
                             "tagdir_tag_to_freelist:  can't pin page %d", 0);
                return;
            }
        } else {
            tdpg0p = tdpgp;
        }
        /*
         * It is necessary to update the in-memory free list head from
         * the on-disk version because when this routine is called
         * during recovery, only the on-disk value is correct, as the
         * value redo records have been applied to it, whereas the
         * in-memory copy is likely stale.
         */

        bfSetp->tagFrLst = tdpg0p->tMapA[0].tmFreeListHead;

        tdpgp->tpNextFreePage = bfSetp->tagFrLst;

        /* Update the on-disk free list head. */

        rbf_pin_record(pgRef, (void *)&tdpg0p->tMapA[0], sizeof(bsTMapT));
        tdpg0p->tMapA[0].tmFreeListHead = tagPg + 1;
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_296);

        /* Update the in-core free list head. */

        bfSetp->tagFrLst = tagPg + 1;
    }

}


/*
 * init_next_tag_page - this will init a tag page, allocating
 * additional storage if necessary.  It is assumed to always be the
 * next uninit'd page, that is, skipping beyond the first uninit'd
 * page will not work.
 */

static statusT
init_next_tag_page(
                   bs_meta_page_t tagPg,        /* in - page to init */
                   bfSetT* bfSetp,              /* in - ptr to bitfile set */
                   bfAccessT* bfap,             /* in - access struct ptr */
                   ftxHT ftxH,                  /* in - ftx handle */
                   int checkmigtrunclock        /* in */
                   )
{
    rbfPgRefHT pgRef;
    statusT sts;
    bsTDirPgT *tdpgp;
    int slot;
    int i;

    if (tagPg == bfSetp->tagUnMpPg) {
        sts = rbf_add_stg(bfSetp->dirBfAp, 
                          tagPg * bfSetp->dirBfAp->bfPageSz, 
                          BS_TD_XPND_PGS * bfSetp->dirBfAp->bfPageSz, 
                          ftxH,
                          checkmigtrunclock?STG_MIG_STG_HELD:STG_NO_FLAGS);
        if (sts != EOK) {
            return sts;
        }

        bfSetp->tagUnMpPg += BS_TD_XPND_PGS;

        if (BS_BFTAG_RSVD( bfap->tag ) ||
            (lk_get_state( bfap->stateLk) != ACC_CREATING)) {
            /*
             * Set done mode to skip undo for sub ftxs, in this case,
             * the rbf_add_stg, so it will not be undone after the
             * current ftx is done.  This is because the new page will
             * be initialized and the global free list updated, so the
             * new page of tags will become visible after either this
             * parent ftx completes, or the system restarts.
             *
             * If this tag allocation is being done within a subftx
             * which contains the creation of the tagdir then don't
             * do the special done mode.  If the entire ftx fails
             * then we need to allow the storage to be freed.
             */
            ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_297);
            ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
            ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_298);
        }
    }

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp,
                         tagPg, BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        return sts;
    }

    /* 
     * Initialize the page header and the subordinate free list.
     * Note that some the tag page header is initialized via bzero.
     */
    rbf_pin_record(pgRef, (void *)tdpgp, sizeof(bsTDirPgT));
    bzero( (char*)tdpgp, sizeof(bsTDirPgHdrT));
    tdpgp->tpMagicNumber = TAG_MAGIC;
    tdpgp->tpFsCookie = bfSetp->dmnP->dmnCookie;
    tdpgp->tpCurrPage = tagPg;
    /*
     * point the new page to the existing free list.  the free list
     * will be pointed to this page further on.
     */
    tdpgp->tpNextFreePage  = bfSetp->tagFrLst;
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_299);

    for (i = 0; i < BS_TD_TAGS_PG; i++) {
        tdpgp->tMapA[ i ].tmFlags = 0;
        tdpgp->tMapA[ i ].tmSeqNo = 1;
        tdpgp->tMapA[ i ].tmNextMap = i + 2;
    }

    tdpgp->tMapA[BS_TD_TAGS_PG - 1].tmNextMap = 0;

    tdpgp->tMapA[ 0 ].tmFreeListHead = 1;

    /* 
     * Page 0 is special in that the first tagmap struct is used 
     * to store some metadata information.  If pg0, start free list
     * at slot 3 (1 based) (this corresponds to tag num 2).  
     */
    slot = tagPg ==  0 ? ADVFS_LOWEST_TAG_NUM : 0;
    tdpgp->tpNextFreeMap = slot + 1;

    /* Pin page 0 of the tagdir, if not already pinned. */
    if (tagPg != 0) {
        if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                             (uint64_t)0, BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
            return sts;
        }
    }

    /* Update the on-disk free list pointer. */
    rbf_pin_record(pgRef, (void *)&tdpgp->tMapA[0], sizeof(bsTMapT));
    tdpgp->tMapA[0].tmFreeListHead = tagPg + 1;

    /* Update the in-core free list head. */
    bfSetp->tagFrLst = tagPg + 1;

    /* Update the on-disk first uninitialized page pointer. */
    tdpgp->tMapA[0].tmUninitPg = tagPg + 1;

    /* Update the in-core first uninitialized page pointer. */
    bfSetp->tagUnInPg = tagPg + 1;
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_300);

    return sts;
}


/* 
 * tagdir_alloc_tag
 *
 * Allocates a new tag in the specified bitfile set.  This returns
 * with the tag dir locked in the parent ftx.
 */


statusT
tagdir_alloc_tag(
    ftxHT parentFtxH,           /* in - transaction handle */
    mcellUIdT* mcellUIdp,       /* in/out - ptr mcell uid (tag rtnd) */
    bfSetT* bfSetp,             /* in - ptr to bitfile set */
    bsTDirPgT **tdpgpp,         /* out - pointer to tagdir page hdr */
    bsTMapT **tdmapp,           /* out - ptr to tagdir map entry */
    rbfPgRefHT *tdpgRefp,       /* out - tagdir page ref */
    int checkmigtrunclock       /* in */
    )
{
    bsTMapT *tdmap;
    bs_meta_page_t tagPg;
    rbfPgRefHT pgRef;
    statusT sts;
    bsTDirPgT *tdpgp;
    bsTDirPgT* tdpg0p;
    unsigned int slot;
    ftxHT ftxH;
    domainT *dmnP = bfSetp->dmnP;

    ADVFTM_SETTAGDIR_LOCK(bfSetp, parentFtxH);

    MS_SMP_ASSERT(bfSetp->tagUnInPg <= bfSetp->tagUnMpPg);

    /*
     * Pick up the page number from the free list.  Note that it is
     * "1 based" so that zero means the list is empty.
     */
getfreeslot:
    if ((tagPg = bfSetp->tagFrLst) == 0) {
        tagUnInitPageRedoT uipr;

        if ((sts = FTX_START_N(FTA_BS_TAG_EXTEND_TAGDIR_V1, &ftxH, parentFtxH, 
                               dmnP)) != EOK) {
            return (sts);
        }
        /*
         * The free list is empty.  Put a new page at the head of
         * the free list.  The new page may have been preallocated.
         */
        tagPg = bfSetp->tagUnInPg;

        sts = init_next_tag_page( tagPg, bfSetp, bfSetp->dirBfAp, ftxH,
                                 checkmigtrunclock);
        if ( sts != EOK ) {
            goto fail;
        }

        /* Set op redo record to update uninit page during recovery */

        uipr.bfSetId = bfSetp->bfSetId;
        uipr.unInitPage = bfSetp->tagUnInPg;

        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_301);
        ftx_done_urdr(ftxH, FTA_BS_TAG_EXTEND_TAGDIR_V1, 0,0,0,0,
                      sizeof(uipr), (void*)&uipr );
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_302);

    } else {
        /*
         * At this point, the free list is not empty.
         */
        tagPg--;                     /* 1 based */
    }

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                         tagPg, BS_NIL, parentFtxH, MF_VERIFY_PAGE)) != EOK) {
        return sts;
    }

    if ( tdpgp->tpCurrPage != tagPg ) {
        return E_BAD_TAGDIR;
    }

    slot = tdpgp->tpNextFreeMap;
    --slot;  /* this cleverly makes unsigned slot > BS_TD_TAGS_PG if 0 */

    if (  slot > BS_TD_TAGS_PG ) {
        goto badfreelist;
    }

    tdmap = &tdpgp->tMapA[slot];

    if ( !(tdmap->tmFlags & BS_TD_IN_USE) ){
#ifdef ADVFS_SMP_ASSERT
        /* we shouldn't return tag 1 if not the RootTagDir */
        if (!BS_BFTAG_EQL(bfSetp->dirTag, staticRootTagDirTag)) {
            MS_SMP_ASSERT(MKTAGNUM(tagPg, slot) != 1);
        }
        /*
         * Other tmFlags can still be on even when the BS_TD_IN_USE bit is
         * cleared (its a deferred delete list thing), however, the
         * assumption is that these flags get consistently cleared when the
         * tag is placed on the free list.  This assert is just checking
         * that assumption.
         */
        MS_SMP_ASSERT(tdmap->tmFlags == 0);
#endif

        *tdpgpp = tdpgp;
        *tdmapp = tdmap;
        *tdpgRefp = pgRef;
        mcellUIdp->ut.tag.tag_num = MKTAGNUM(tagPg, slot);

        /*
         * Don't allow a file to be created with a tag larger than
         * MAX_ADVFS_TAGS.
         */
        if (mcellUIdp->ut.tag.tag_num > MAX_ADVFS_TAGS) {
            return ETAG_OVF;
        }

        mcellUIdp->ut.tag.tag_seq = tdmap->tmSeqNo;
        
        return EOK;

    }

    /*
     * The remaining code is an error path executed only when free
     * list corruption has been detected.
     */

badfreelist:
    if ((sts = FTX_START_N(FTA_BS_TAG_PATCH_FREELIST_V1, &ftxH, 
                           parentFtxH, dmnP)) != EOK) {
        return (sts);
    }
    /* 
     * free list is corrupt on this page.  kill the free list. 
     * goto to the next tag dir page in the "page free list".
     */

    printf( "ADVFS: BAD TAGDIR; pg = %d, dmnid = %lx, tag = %ld\n",
            tagPg, *(int64_t*)&bfSetp->bfSetId.domainId,
            bfSetp->bfSetId.dirTag.tag_num );

    /* need to pin page in context of this subftx */
    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                         tagPg, BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        goto fail;
    }

    rbf_pin_record(pgRef, (void *)&tdpgp->tpNextFreeMap, 
                   sizeof(tdpgp->tpNextFreeMap));
    tdpgp->tpNextFreeMap = 0;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_303);
    rbf_pin_record(pgRef, (void *)&tdpgp->tpNumAllocTMaps, 
                   sizeof(tdpgp->tpNumAllocTMaps));
    tdpgp->tpNumAllocTMaps = BS_TD_TAGS_PG;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_304);

    if (tagPg != 0) {
        if ((sts = rbf_pinpg(&pgRef, (void **)&tdpg0p, bfSetp->dirBfAp, 
                             (unsigned long)0, BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
            goto fail;
        }
    } else {
        tdpg0p = tdpgp;
    }

    /* Update the on-disk and in-core free list pointers. */

    rbf_pin_record(pgRef, (void *)&tdpg0p->tMapA[0], sizeof(bsTMapT));

    /*
     * Terminate the free list to avoid loops.
     */

    bfSetp->tagFrLst = 0;

    tdpg0p->tMapA[0].tmFreeListHead = bfSetp->tagFrLst;

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_305);

    ftx_done_n(ftxH, FTA_BS_TAG_PATCH_FREELIST_V1);

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_306);

    /* try again */

    goto getfreeslot;

fail:
    /*
     * This error path is only used if a subtransaction has been
     * started, else simply return the error.
     */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_471);
    ftx_fail(ftxH);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_472);
    return (sts);
}



/*
 * extend_tagdir_redo_opx
 *
 * This opx routine keeps the first unitialized page for a tag
 * directory in sync with on-disk redo updates during recovery.
 */

void
extend_tagdir_redo_opx(
                       ftxHT ftxH,      /* in - ftx */
                       int opRecSz,     /* in - redo rec size */
                       void *opRec      /* in - ptr to redo rec */
                       )
{
    tagUnInitPageRedoT uipp;
    statusT sts;
    bfSetT* bfSetp;
    domainT *dmnP = ftxH.dmnP;

    bcopy(opRec, &uipp, sizeof(tagUnInitPageRedoT));

    if ( !BS_UID_EQL(uipp.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(uipp.bfSetId.domainId, dmnP->dualMountId)) )
        {
           /* I can't hit this at all. */
           /* I'm not sure it's needed, but I'll leave it in. */
            uipp.bfSetId.domainId = dmnP->domainId;
        } else {
            ADVFS_SAD0("extend_tagdir_redo_opx: domainId mismatch");
        }
    }

    sts = bfs_open(&bfSetp, uipp.bfSetId, BFS_OP_IGNORE_DEL, ftxH);
    if ( sts != EOK ) 
    {
        if (sts == E_NO_SUCH_BF_SET)
        {
            /* The bitfile set may have been created and removed before
             * the log was rolled forward. Just ignore the error and
             * let recovery continue
             */

            goto HANDLE_EXCEPTION;
        }

        domain_panic(dmnP, "extend_tagdir_redo_opx: can't open bitfile set");
        goto HANDLE_EXCEPTION;
    }

    bfSetp->tagUnInPg = uipp.unInitPage;

    if ((sts = bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF)) != EOK) {
        domain_panic(dmnP, "extend_tagdir_redo_opx: can't close bitfile set");
        goto HANDLE_EXCEPTION;
    }

HANDLE_EXCEPTION:
    return;
}



/* 
 * tagdir_insert_tag
 *
 * Initialize a new tag in the specified bitfile set.  This assumes
 * the parent ftx has the tag dir locked from a previous call to
 * tagdir_alloc_tag.  This pair of routines is only intended to be
 * called from bitfile create. 
 */


statusT
tagdir_insert_tag(
                  ftxHT parentFtxH,
                  mcellUIdT* mcellUIdp, /* in - mcelluid ptr */
                  bfSetT* bfSetp,       /* in - ptr to bitfile set */
                  bsTDirPgT *tdpgp,
                  bsTMapT *tdmap,
                  bfTagFlagsT tagFlags, 
                  rbfPgRefHT tdpgRef
                  )
{
    statusT sts;
    int nextTMap;

    /*
     * tmNextMap must be plucked out of the tag dir map before other
     * fields in the union struct are stored, which will destroy it.
     */
    nextTMap = tdmap->tmNextMap;

    rbf_pin_record(tdpgRef, (void *)tdmap, sizeof(bsTMapT));
    /*
     * assert required values for setting tdmap->tmFlags 
     * currently includes:
     * - dataSafety value
     */
    MS_SMP_ASSERT(tagFlags & BS_DATA_SAFETY_MASK);
    tdmap->tmFlags = BS_TD_IN_USE | tagFlags;
    tdmap->tmBfMCId = mcellUIdp->mcell;
    ADVFS_CRASH_RECOVERY_TEST(parentFtxH.dmnP, AdvfsCrash_307);

    /*
     * Take the tagmap struct off the head of the subordinate free list.
     * Update number allocated tmaps counter.
     */

    rbf_pin_record(tdpgRef, (void *)&tdpgp->tpNextFreeMap, 
                   sizeof(tdpgp->tpNextFreeMap));
    tdpgp->tpNextFreeMap = nextTMap;
    ADVFS_CRASH_RECOVERY_TEST(parentFtxH.dmnP, AdvfsCrash_308);
    rbf_pin_record(tdpgRef, (void *)&tdpgp->tpNumAllocTMaps, 
                   sizeof(tdpgp->tpNumAllocTMaps));
    tdpgp->tpNumAllocTMaps++;
    ADVFS_CRASH_RECOVERY_TEST(parentFtxH.dmnP, AdvfsCrash_309);

    bfSetp->bfCnt++;

    /*
     * Update the page free list if this is the last tagmap struct
     * on the page's subordinate free list.
     */
    if (nextTMap == 0) {
        rbfPgRefHT pgRef;
        bsTDirPgT* tdpg0p;

        /* Pin page 0 of the tagdir */
        sts  = rbf_pinpg(&pgRef, (void **)&tdpg0p, bfSetp->dirBfAp, 
                         (uint64_t)0, BS_NIL, parentFtxH, MF_VERIFY_PAGE);
        if ( sts != EOK) {
            return sts;
        }

        /* Update the on-disk and in-core free list pointers. */
        rbf_pin_record(pgRef, (void *)&tdpg0p->tMapA[0], sizeof(bsTMapT));
        tdpg0p->tMapA[0].tmFreeListHead = bfSetp->tagFrLst
                                       = tdpgp->tpNextFreePage;
        ADVFS_CRASH_RECOVERY_TEST(parentFtxH.dmnP, AdvfsCrash_310);
    }
    return (EOK);
}



/* 
 * tagdir_set_bfdesc_common
 *
 * Initialize a new tag in the specified bitfile set.
 */


static statusT
tagdir_set_tagmap_common(
    tagInfoT *tip,
    int markInUse
    )
{
    bsTMapT *tdmap;
    bs_meta_page_t tagPg;
    int slot;
    bfSetT *bfSetp;
    ftxHT ftxH;
    statusT sts;
    rbfPgRefHT pgRef;
    bsTDirPgT *tdpgp;
    tagDataRecT tagDataRec;

    bfSetp = tip->bfSetp;

    tagPg = TAGTOPG(&tip->tag);
    slot = TAGTOSLOT(&tip->tag);

    if ((sts = FTX_START_N(FTA_BS_TAG_WRITE_V1, &ftxH, tip->ftxH, 
                           tip->dmnP)) != EOK) {
        return (sts);
    }

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, tip->bfSetp->dirBfAp, 
                    tagPg, BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_473);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_474);
        return (sts);
    }

    tdmap = &tdpgp->tMapA[slot];

    /* Save old values in case of an undo. */
    tagDataRec.tag = tip->tag;
    tagDataRec.map = *tdmap;
    tagDataRec.bfSetId = tip->bfSetp->bfSetId;

    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));
    if ( markInUse == TRUE ) {
        tdmap->tmFlags |= BS_TD_IN_USE; 
    }
    tdmap->tmBfMCId = tip->bfMCId;

    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_311);

    ftx_done_u(ftxH, FTA_BS_TAG_WRITE_V1, sizeof(tagDataRecT), 
               (void *)&tagDataRec);
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_312);
    return (EOK);
}


statusT
advfs_bs_tagdir_update_tagmap(
        bfTagT tag,             /* in - tag to update. Used to find tag entry */
        bfSetT *bf_set_p,       /* in - fileset for tag to update */
        bfTagFlagsT tag_flags,  /* in - flags to update */
        bfMCIdT mcell_id,       
                    /* in - mcell_id to update in tag. This may not be valid. */
        ftxHT parent_ftx,       /* in - parent ftx */
        tdu_flags_t flags       /* in - type of op */
        )
{
    bsTMapT *tdmap;
    bs_meta_page_t tagPg;
    int slot;
    ftxHT ftxH;
    statusT sts;
    rbfPgRefHT pgRef;
    bsTDirPgT *tdpgp;
    tagDataRecT tagDataRec;

    tagPg = TAGTOPG(&tag);
    slot = TAGTOSLOT(&tag);

    MS_SMP_ASSERT((parent_ftx.hndl == FtxNilFtxH.hndl) || 
            (parent_ftx.dmnP == bf_set_p->dmnP));

    if (flags == TDU_NO_FLAGS) {
        /* nothing to do */
        return EOK;
    }

    if ((sts = FTX_START_N(FTA_BS_TAG_WRITE_V1, &ftxH, parent_ftx, 
                           bf_set_p->dmnP)) != EOK) {
        domain_panic( bf_set_p->dmnP, 
                "advfs_bs_tagdir_update_tagmap: failed to start a transaction");
        return (sts);
    }

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bf_set_p->dirBfAp, 
                    tagPg, BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_475);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_476);
        return (sts);
    }

    tdmap = &tdpgp->tMapA[slot];

    /* Save old values in case of an undo. */
    tagDataRec.tag = tag;
    tagDataRec.map = *tdmap;
    tagDataRec.bfSetId = bf_set_p->bfSetId;

    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));
    if (flags & TDU_SET_FLAGS) {
        tdmap->tmFlags |= tag_flags; 
    } else if (flags & TDU_UNSET_FLAGS) {
        tdmap->tmFlags &= ~(tag_flags);
    }
    ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_313);

    if (flags & TDU_SET_MCELL) {
        tdmap->tmBfMCId = mcell_id;
    }

    if (flags & TDU_SKIP_FTX_UNDO) {
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_314);
        ftx_special_done_mode(ftxH, FTXDONE_SKIP_SUBFTX_UNDO);
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_315);
        ftx_done_n(ftxH, FTA_BS_TAG_WRITE_V1);
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_316);
    } else {
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_317);
        ftx_done_u(ftxH, FTA_BS_TAG_WRITE_V1, sizeof(tagDataRecT), 
                (void *)&tagDataRec);
        ADVFS_CRASH_RECOVERY_TEST(bf_set_p->dmnP, AdvfsCrash_318);
    }

    return (EOK);
}
/*
 * tagdir_write_undo_opx
 *
 * Overwrite a tagmap struct with whatever is in the undo record.
 */


void
tagdir_write_undo_opx(
    ftxHT ftxH,         /* in - transaction handle */
    int opRecSz,        /* in - undo record size */
    void *opRec         /* in - ptr to undo record */
    )
{
    tagDataRecT rp;
    bfSetT *bfSetp;
    bsTMapT *tdmap;
    bsTDirPgT *tdpgp;
    rbfPgRefHT pgRef;
    bs_meta_page_t tagPg;
    unsigned slot;
    domainT *dmnP;
    statusT sts;
    int bfs_opened = 0;

    bcopy(opRec, &rp, sizeof(tagDataRecT));
    
    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(rp.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag &  BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(rp.bfSetId.domainId, dmnP->dualMountId)) ) {
            rp.bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("tagdir_write_undo_opx: domainId mismatch");
        }
    }

   /*
     * If we are on-line, we do not have to open the fileset, only
     * do a lookup.
     */

    if (dmnP->state != BFD_ACTIVATED) {
        sts = bfs_open(&bfSetp, rp.bfSetId, BFS_OP_IGNORE_DEL, ftxH);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP,"tagdir_write_undo_opx: can't open bitfile set");
            goto HANDLE_EXCEPTION;
        }
        bfs_opened = 1;
    } else {
        bfSetp = bfs_lookup( rp.bfSetId);
        MS_SMP_ASSERT( BFSET_VALID(bfSetp));
    }

    tagPg = TAGTOPG(&rp.tag);
    slot = TAGTOSLOT(&rp.tag);

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                         tagPg, BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        ADVFS_SAD1("tagdir_write_undo_opx: can't pin page", tagPg);
    }

    tdmap = &tdpgp->tMapA[slot];
    MS_SMP_ASSERT(rp.tag.tag_seq == tdmap->tmSeqNo);

    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));
    *tdmap = rp.map;
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_319);

    if ( bfs_opened ) {
        if ((sts = bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF)) != EOK) {
            domain_panic(ftxH.dmnP,
                         "tagdir_write_undo_opx: can't close bitfile set");
            goto HANDLE_EXCEPTION;
        }
    }
HANDLE_EXCEPTION:
    return;
}


/*
 * tagdir_lookup
 *
 * Given a tag, return bitfile descriptor (mcell id) and bit flags
 * - wrapper for call to tagdir_lookup_full()
 *
 */
statusT
tagdir_lookup(
        bfSetT *bfSetp,             /* in - bitfile set desc ptr */
        bfTagT *tp,                 /* in/out - pointer to tag to look up */
        bfMCIdT *bfMCId             /* out - primary mcell id */
        )
{
    bfTagFlagsT tagFlags;

    return (tagdir_lookup_full(bfSetp, tp, bfMCId, &tagFlags));

}


/*
 * tagdir_lookup_full
 *
 * Given a tag, return bitfile descriptor (mcell id) and bit flags
 *
 * NOTE: The tagdir_lookup_full() routine should ONLY be called by
 *       routines that have to initialialize access structure fields
 *       where the corresponding values are stored in the bitflags of
 *       the bsTMap structure.
 *
 * bitfile bitflags current usage: (see details in bs_ods.h)
 *   - BS_TD_IN_USE
 *   - dataSafety
 *   
 * The staticRootTagDirTag does not technically belong to any fileset.
 * Passing it in as the tp (even with dmnP->bfSetDirp) will result in
 * ENO_SUCH_TAG.
 *
 * NOTE:  tagFlags may still be returned with a value even if ENO_SUCH_TAG
 *        is returned.  This is because a file on the deferred delete list
 *        will have its BS_TD_IN_USE bit cleared, but the tag will not be on
 *        the free list and all the other tmFlags (like the dataSafety bits)
 *        are still properly set.  In a Cluster failover situation, if a
 *        client still has a deleted file open, the cluster must be able to
 *        re-open the file when the file system is activated on a different
 *        server node.  In order to properly re-open the file, the tagFlags
 *        are needed.  So the rule is that bs_access_one() will try to get
 *        the tag, if it gets ENO_SUCH_TAG, it will use find_del_entry() to
 *        see if the file is on the deferred delete list.  If the file is,
 *        then bs_access_one() still needs to use the tagFlags from the tag
 *        entry, as they are the only copy of the tmFlags for the file.
 */
statusT
tagdir_lookup_full(
    bfSetT *bfSetp,             /* in - bitfile set desc ptr */
    bfTagT *tp,                 /* in/out - pointer to tag to look up */
    bfMCIdT *bfMCId,            /* out - primary mcell id */
    bfTagFlagsT *tagFlags       /* out - bitfile bit flags */
    )
{
    bs_meta_page_t tagPg;
    int slot;
    bfPageRefHT pgRef;
    bsTDirPgT *tdpgp;
    bsTMapT *tdmap;
    statusT sts;
    domainT *domain = bfSetp->dmnP;
    
    *tagFlags = 0;

    if (BS_BFTAG_RSVD(*tp)) {

        bfMCId->cell = BS_BFTAG_CELL(*tp);
        bfMCId->volume = BS_BFTAG_VDI(*tp);
        bfMCId->page = 0;            /* reserved cells always on page 0! */

        /* validate the tag */

        if (FETCH_MC_CELL(*bfMCId) >= BFM_RSVD_TAGS) {
            return (ENO_SUCH_TAG);
        }

        /* Verify the volume portion of the tag.  The volume must
         * represent either a valid vdT struct, or a vdT struct in
         * the BSR_VD_VIRGIN state that is being set up by the
         * current thread.
         */    
        if (!vd_htop_if_valid(bfMCId->volume, domain, FALSE, FALSE) ) {
            return (ENO_SUCH_TAG);
        }

        /* The reserved tag is valid. Set the tagFlags here since
         * reserved tags do not have an entry in the tag file.
         *
         * - currently only the dataSafety bits are being used
         * 
         */
        if (BS_IS_TAG_OF_TYPE(*tp, BFM_FTXLOG)) {
            *tagFlags = BFD_LOG;
        } else {
            *tagFlags = BFD_METADATA;
        }

        return (EOK);
    }
    
        
    if (!bfSetp) {
        return ENO_SUCH_TAG;
    }      

    tagPg = TAGTOPG(tp);
    slot = TAGTOSLOT(tp);

    if (tagPg >= bfSetp->tagUnInPg) {
        return ENO_SUCH_TAG;
    }

    if ((sts = bs_refpg(&pgRef,
                        (void **)&tdpgp,
                        bfSetp->dirBfAp, 
                        tagPg,
                        FtxNilFtxH,
                        MF_VERIFY_PAGE)) != EOK) {
        return (sts);
    }

    tdmap = &tdpgp->tMapA[slot];
    MS_SMP_ASSERT(tdmap->tmBfMCId.volume != VOL_TERM);

    /*
     * *** IMPORTANT ***
     *
     * We return the tmFlags bits even if the BS_TD_IN_USE bit is cleared.
     * A file that is currently on the deferred delete list will have only
     * its BS_TD_IN_USE bit cleared, but all the other bits (like the
     * dataSafety bits) will still be set.  These other flag bits remain set
     * until the file is removed from the deferred delete list and the tag
     * is placed on the free list.
     *
     * However, callers to tagdir_lookup_full() should not use the bits
     * unless they use something like find_del_entry() to verify that the
     * file is currently on the deferred delete list.
     */
    tp->tag_flags = *tagFlags = tdmap->tmFlags;

    if ( ((tdmap->tmFlags & BS_TD_IN_USE) == 0) || (tp->tag_num == 0) ) {
        (void) bs_derefpg(pgRef, BS_CACHE_IT);  /* ignore further errors */
        return (ENO_SUCH_TAG);
    }

    if (tp->tag_seq != 0) {
        /* 
         * A non-zero sequence number is used when a match on
         * sequence numbers is requested.
         */
        if (tdmap->tmSeqNo != tp->tag_seq) {
            bs_derefpg(pgRef, BS_CACHE_IT);
            return (ENO_SUCH_TAG);
        }
    }
    else {
        /* 
         * A zero sequence number is used when a wild card lookup 
         * is requested.  The current sequence number is returned 
         * in the tag.
         */
        tp->tag_seq = tdmap->tmSeqNo;
    }   

    *bfMCId = tdmap->tmBfMCId;

    if ((sts = bs_derefpg(pgRef, BS_CACHE_IT)) != EOK) {
        return (sts);
    }

    if ( *tagFlags & BS_TD_OUT_OF_SYNC_SNAP) {
        return E_OUT_OF_SYNC_SNAPSHOT;
    }

    if (sts = check_mcell_ptr (*bfMCId, *tp, domain) != EOK ) {
        printf("tagdir_lookup: Invalid primary mcell (%d, %ld, %d) for tag \
                %ld in domain %s, fileset %s\n", bfMCId->volume, bfMCId->page, 
                bfMCId->cell, tp->tag_num, domain->domainName,
                ((fileSetNodeT*)bfSetp->fsnp)->f_mntfromname);
    }

    return (sts);
}

/*
 * tagdir_lookup2
 *
 * Find first valid tag with an index number no smaller then
 * that of the input tag.
 *
 * This is needed to support the semantics of bs_bfs_get_info.
 */


statusT
tagdir_lookup2(
    bfSetT *bfSetp,             /* in - bitfile set desc ptr */
    bfTagT *tp,                 /* in,out - pointer to tag */
    bfMCIdT *bfMCId             /* out - bitfile metadata cell */
    )
{
    statusT sts;

    sts = tagdir_lookup(bfSetp, tp, bfMCId);
    switch (sts) {
        case EOK:
            return (EOK);
        case ENO_SUCH_TAG:
            sts = tagdir_lookup_next(bfSetp, tp, bfMCId);
            return (sts);
        default:
            return (sts);
    }
    MS_SMP_ASSERT(FALSE);
    return (EOK); /* Satisfy the compiler */
}

/*
 * tagdir_lookup_next
 *
 * Iterates through all the in-use tags in a tag directory.  If the tag
 * passed in is NilBfTag, then the call to tagdir_lookup_next returns the
 * first tag.  The caller may then iterate through all the in-use tags 
 * by invoking tagdir_lookup_next with the tag returned by the preceding call.
 *
 * ENO_SUCH_TAG is the return status when the iteration is finished.
 *
 * This function uses no locks.  If the caller plans to iterate and wants
 * a consistent view, then it must hold the tagdir freelist semaphore.
 */


statusT
tagdir_lookup_next(
    bfSetT *bfSetp,             /* in - bitfile set desc pointer */
    bfTagT *tp,                 /* in,out - pointer to tag */
    bfMCIdT *bfMCId             /* out - bitfile metadata cell */
    )
{
    bs_meta_page_t tagPg;
    int slot;
    bfPageRefHT pgRef;
    bsTDirPgT *tdpgp;
    bsTMapT *tdmap;
    int isReferenced = 0;
    domainT *domain = bfSetp->dmnP;
    vdT *vdp;

    tagPg = TAGTOPG(tp);
    slot = TAGTOSLOT(tp);
   
    if (tagPg >= bfSetp->tagUnInPg) {
        return ENO_SUCH_TAG;
    }

    while (1) {
        if (slot == BS_TD_TAGS_PG - 1) {
            /*
             * Dereference the old page if it has been referenced.
             */
            if (isReferenced) {
                (void) bs_derefpg(pgRef, BS_CACHE_IT);
                isReferenced = 0;
            }
            if (++tagPg >= bfSetp->tagUnInPg) {
                return (ENO_SUCH_TAG);
            }
            slot = 0;
        }
        else {
            slot++;
        }
        if (isReferenced == 0) {
            if (bs_refpg(&pgRef,
                        (void **)&tdpgp,
                        bfSetp->dirBfAp,
                        tagPg,
                        FtxNilFtxH,
                        MF_VERIFY_PAGE) !=  EOK) {
                return (ENO_SUCH_TAG);
            }
            isReferenced = 1;
        }

        MS_SMP_ASSERT(tdpgp->tpCurrPage == tagPg);
        tdmap = &tdpgp->tMapA[slot];

	if ( (tdmap->tmFlags & BS_TD_IN_USE) == 0 ) {
            continue;
        }
        
        *bfMCId = tdmap->tmBfMCId;
        tp->tag_seq = tdmap->tmSeqNo;
        tp->tag_num = MKTAGNUM(tagPg, slot);
        tp->tag_flags = tdmap->tmFlags;
        (void) bs_derefpg(pgRef, BS_CACHE_IT);

        if ( !(vdp = vd_htop_if_valid(bfMCId->volume, domain, FALSE, FALSE)) ) {

            if ( bfSetp->fsnp != NULL ) {
                domain_panic(domain,
         "tagdir_lookup_next: invalid volume index %d for tag %ld in fileset %s",
                   bfMCId->volume, tp->tag_num,
                   ((fileSetNodeT*)bfSetp->fsnp)->f_mntfromname);
            } else {
                domain_panic(domain,
     "tagdir_lookup_next: invalid volume index %d for tag %ld in fileset tag %ld",
                   bfMCId->volume, tp->tag_num, bfSetp->bfSetId.dirTag.tag_num);
            }
            return (E_BAD_TAGDIR);
        }
        if ( bfMCId->cell >= BSPG_CELLS ) {
            if ( bfSetp->fsnp != NULL ) {
                domain_panic(domain,
         "tagdir_lookup_next: invalid mcell number %d for tag %ld in fileset %s",
                   bfMCId->cell, tp->tag_num,
                   ((fileSetNodeT*)bfSetp->fsnp)->f_mntfromname);
            } else {
                domain_panic(domain,
     "tagdir_lookup_next: invalid mcell number %d for tag %ld in fileset tag %ld",
                   bfMCId->cell, tp->tag_num, bfSetp->bfSetId.dirTag.tag_num);
            }
            return (E_BAD_TAGDIR);
        }
        if ( FETCH_MC_PAGE(*bfMCId) >= vdp->bmtp->bfaNextFob ) {
            if ( bfSetp->fsnp != NULL ) {
                domain_panic(domain,
      "tagdir_lookup_next: invalid BMT page number %ld for tag %ld in fileset %s",
                   bfMCId->page, tp->tag_num,
                   ((fileSetNodeT*)bfSetp->fsnp)->f_mntfromname);
            } else {
                domain_panic(domain,
  "tagdir_lookup_next: invalid BMT page number %ld for tag %ld in fileset tag %ld",
                    bfMCId->page, tp->tag_num, bfSetp->bfSetId.dirTag.tag_num);
            }

            return (E_BAD_TAGDIR);
        }

        return EOK;
    }
    MS_SMP_ASSERT(FALSE);
    return (EOK); /* Satisfy the compiler */
} 


/*
 * bs_switch_root_tagdir
 *
 * This function moves the root tag directory to the specified disk.
 *
 * NOTE:  This function assumes a small root tag directory.  If the root tag
 * directory is large, this function can block the log wraps because it
 * the tag directory copy within a transaction.
 */


statusT
bs_switch_root_tagdir (
                       domainT *dmnP,       /* in */
                       vdIndexT newVdIndex  /* in */
                       )
{
    bfSetT *bfSet;
    bsMPgT *bmtPage;
    int closeNewFlag = 0;
    int derefFlag = 0;
    bsDmnMAttrT *dmnMAttr;
    int failFtxFlag = 0;
    ftxHT ftxH;
    int i;
    vdT *logVd;
    bfAccessT *newBfAccessp;
    char *newPage;
    bs_meta_page_t newPageCnt;
    bfTagT newTag;
    vdT *newVd;
    char *oldPage;
    bs_meta_page_t oldPageCnt;
    vdT *oldVd;
    bfPageRefHT pgPin;
    bfPageRefHT pgRef;
    rbfPgRefHT rbfPgPin;
    statusT sts;
    ftxHT subFtxH;
    int mcell_index;
    struct bfAccess *mdap;


    if ((newVdIndex < 1) || (newVdIndex > BS_MAX_VDI)) {
        RAISE_EXCEPTION (EBAD_VDI);
    }

    /*
     * Start an exclusive transaction.  This blocks new transaction starts and
     * waits for old transactions to complete.
     */
    sts = FTX_START_EXC(FTA_NULL, &ftxH, dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    /*
     * No other switcher can be active so get the current tag and finish
     * the disk check.
     */

    oldVd = VD_HTOP(BS_BFTAG_VDI(dmnP->bfSetDirTag), dmnP);
    newVd = VD_HTOP(newVdIndex, dmnP);

    if ((newVd == NULL) || (newVdIndex == oldVd->vdIndex)) {
        RAISE_EXCEPTION (EBAD_VDI);
    }

    /* reclaim any CFS vnodes that are hanging around on vnode free list */
    sts = bs_reclaim_cfs_rsvd_vn( dmnP->bfSetDirAccp);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /*
     * Open the new tag directory.  Note that all disks have a tag directory
     * bitfile; they are all initialized to be empty.
     */

    BS_BFTAG_RSVD_INIT (newTag, newVdIndex, BFM_BFSDIR);

    sts = bfm_open_ms (&newBfAccessp, dmnP, newVdIndex, BFM_BFSDIR);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    closeNewFlag = 1;

    /*
     * NOTE: Assume the tag directory starts at page offset 0.
     */
    oldPageCnt = dmnP->bfSetDirAccp->bfaNextFob / dmnP->bfSetDirAccp->bfPageSz;
    newPageCnt = newBfAccessp->bfaNextFob / newBfAccessp->bfPageSz;

    if (newPageCnt < oldPageCnt) {
        /*
         * Start a sub transaction so that we can set the done mode to
         * skip undo for sub ftxs, in this case, the rbf_add_stg().  
         * If the parent transaction fails, we can't undo the storage
         * because add storage undo isn't supported for reserved bitfiles.
         * This is because we cannot free page 0 mcells.
         *
         * NOTE:  If we fail the parent ftx, the new storage is unusable.
         */
        sts = FTX_START_N(FTA_NULL, &subFtxH, ftxH, dmnP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        sts = rbf_add_stg( newBfAccessp,
                           newPageCnt * newBfAccessp->bfPageSz,
                           (oldPageCnt - newPageCnt) * newBfAccessp->bfPageSz,
                           subFtxH, STG_NO_FLAGS);
        if (sts != EOK) {
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_477);
            ftx_fail (subFtxH);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_478);
            RAISE_EXCEPTION (sts);
        }
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_320);
        ftx_special_done_mode (subFtxH, FTXDONE_SKIP_SUBFTX_UNDO);
        
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_321);

        ftx_done_n (subFtxH, FTA_BS_SWITCH_ROOT_TAGDIR_V1);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_322);
    }

    /*
     * Copy the contents of the current tag directory to the new tag directory.
     */

    for (i = 0; i < oldPageCnt; i++) {

        sts = bs_refpg (&pgRef,
                        (void *)&oldPage,
                        dmnP->bfSetDirAccp,
                        i,
                        FtxNilFtxH,
                        MF_VERIFY_PAGE);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
        derefFlag = 1;

        sts = bs_pinpg (&pgPin,
                        (void *)&newPage, 
                        newBfAccessp,
                        i,
                        FtxNilFtxH,
                        MF_NO_VERIFY);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        /*
         * For now, we assume all files have the same size.  This needs to
         * be fixed if that assumption changes.
         */
        MS_SMP_ASSERT(dmnP->bfSetDirAccp->bfPageSz == newBfAccessp->bfPageSz);
        flmemcpy (oldPage, newPage, newBfAccessp->bfPageSz * ADVFS_FOB_SZ);

        sts = bs_unpinpg (pgPin, logNilRecord, BS_DIRTY);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        derefFlag = 0;
        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

    }  /* end for */

    /*
     * After the flush, both the old and the new tag directories will be
     * identical.
     */
    sts = fcache_vn_flush(&newBfAccessp->bfVnode, 0, 0, NULL, 
                          FVF_WRITE | FVF_SYNC);

    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    closeNewFlag = 0;
    sts = bs_close (newBfAccessp, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Update the modifiable domain attributes record.  This record is on the
     * same disk as the log.
     */

    logVd = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);

    mdap = logVd->rbmtp;
    mcell_index = BFM_RBMT_EXT;
    sts = rbf_pinpg( &rbfPgPin, (void*)&bmtPage, mdap, 0, BS_NIL, ftxH, MF_VERIFY_PAGE );
    if ( sts != EOK ) { 
        RAISE_EXCEPTION (sts);
    }

    dmnMAttr = bmtr_find (&(bmtPage->bsMCA[mcell_index]), BSR_DMN_MATTR,
                                                        logVd->dmnP);
    if (dmnMAttr == NULL) {
        RAISE_EXCEPTION (EBMTR_NOT_FOUND);
    }

    rbf_pin_record( rbfPgPin,
                    &(dmnMAttr->bfSetDirTag),
                    sizeof(dmnMAttr->bfSetDirTag) );
    dmnMAttr->bfSetDirTag = newTag;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_323);

    bs_bfs_switch_root_tagdir (dmnP, newTag);

    /* FIX - is any synchronization needed here? */
    bfSet = dmnP->bfSetDirp;
    dmnP->bfSetDirAccp = bfSet->dirBfAp;
    dmnP->bfSetDirTag = newTag;

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_324);
    ftx_done_urdr (
                   ftxH,
                   FTA_BS_SWITCH_ROOT_TAGDIR_V1,
                   0, NULL,  /* undo */
                   0, NULL,  /* root done */
                   sizeof (newTag), &newTag  /* op redo */
                   );
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_325);

    /*
     * Checkpoint the log so there can be no undo or redo for the
     * old root tag directory.
     */
    advfs_lgr_checkpoint_log (dmnP);

    return EOK;

HANDLE_EXCEPTION:
    if (derefFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }
    if (closeNewFlag != 0) {
        bs_close (newBfAccessp, 0);
    }
    if (failFtxFlag != 0) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_479);
        ftx_fail (ftxH);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_480);
    }

    return sts;
} /* end bs_switch_root_tagdir */

/*
 * switch_root_tagdir_redo_opx
 *
 * This function is the operational redo routine for the transaction
 * created by bs_switch_root_tagdir().  This function switches the root
 * bitfile set's tag directory and resets the in-mem domain structure's
 * "bfSetDirTag" and "bfSetDirAccp" fields.
 *
 * This function closes a window in bs_switch_root_tagdir() between the
 * end of the transaction and the checkpointing of the log.  If the system
 * crashes in this window and the transaction's log record was written to
 * disk but the bmt page was not written to disk, the next domain activation
 * opens the old root tag directory.  This function switches to the new
 * root tag directory.  Note that this function unconditionally does the switch.
 * If the bmt page was written to disk, the domain activation openned the
 * correct root tag directory and this function is a nop.
 */


void
switch_root_tagdir_redo_opx (
                             ftxHT ftxH,
                             int redoRecSize,
                             void *redoRecAddr
                             )
{
    bfSetT *bfSet;
    domainT *domain;
    bfTagT newTag;

    domain = ftxH.dmnP;

    bcopy(redoRecAddr, &newTag, sizeof(bfTagT));

    bs_bfs_switch_root_tagdir (domain, newTag);

    bfSet = domain->bfSetDirp;
    domain->bfSetDirAccp = bfSet->dirBfAp;
    domain->bfSetDirTag = newTag;

    return;
}  /* end switch_root_tagdir_redo_opx */


