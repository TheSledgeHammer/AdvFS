/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
/* 
 * =======================================================================
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
 *
 *
 * Facility:
 *
 *      MSFS/bs
 *
 * Abstract:
 *
 *      Routines for tag directory functions.
 *
 * Date:
 *
 *      Fri Jul 20 12:27:39 1990
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_tagdir.c,v $ $Revision: 1.1.131.5 $ (DEC) $Date: 2008/02/12 13:07:05 $"

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_assert.h>
#include <msfs/ms_osf.h>

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
 * Note that BFM_RSVD_CELLS_V3 == BFM_RSVD_TAGS
 *
 * For AdvFS on-disk version < BFM_ODS_LAST_VERSION,
 * The mapping of reserved tags to <virtual disk, mcell> pairs is
 *
 * -BFM_RSVD_CELLS_V3 --> <1, BFM_BMT_V3>
 * -(BFM_RSVD_CELLS_V3 + 1) --> <1, BFM_SBM>
 * -(BFM_RSVD_CELLS_V3 + 3) --> <1, BFM_BFSDIR>
 * -(BFM_RSVD_CELLS_V3 + 4) --> <1, BFM_FTXLOG>
 * -(BFM_RSVD_CELLS_V3 + 5) --> <2, BFM_BMT_V3>, etc.
 *
 * Thus, -BFM_RSVD_CELLS is the largest (least negative) reserved tag.
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
    bfSetIdT bfSetId;
    uint32T unInitPage;
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
    unsigned long *firstUninitPage,         /* out - next page to use */
    unsigned long *firstUnmappedPage,       /* out - first unmapped page */
    unsigned long *freeListHead,            /* out - start of free list */
    int *bfCnt                              /* out - num bitfiles in set */
    )
{
    bfAccessT *tdap;
    statusT sts;
    bsTDirPgT *tdpgp;
    bfPageRefHT pgRef;
    uint32T pg;


    tdap = bfSetp->dirBfAp;

    *firstUnmappedPage = tdap->nextPage;
    *bfCnt = 0;
    *firstUninitPage = 0;
    *freeListHead = 0;      /* indicates empty free list */

    if (tdap->nextPage == 0) {
        return (EOK);
    }

    /*
     * The page number of the head of the free list and the page number
     * of the first uninitialized page is stored in the first tagmap
     * structure on page 0.
     */
    if ((sts = bs_refpg(&pgRef, (void *)&tdpgp, bfSetp->dirBfAp, 
                   (unsigned long) 0, BS_SEQ_AHEAD)) != EOK) {
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
                       BS_SEQ_AHEAD);
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
    unsigned long tagPg;
    int slot;
    rbfPgRefHT pgRef;
    bsTDirPgT *tdpgp;
    bsTMapT *tdmap;
    int addToFreeList;

    tagPg = TAGTOPG(tp);
    slot = TAGTOSLOT(tp);

    /* Pseudo-tag should not come here. */
    MS_SMP_ASSERT(BS_BFTAG_PSEUDO(*tp) == 0);

    TAG_DIR_LOCK(bfSetp, ftxH)

    /* Pin the page containing the tag to remove. */
    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, tagPg, 
                         BS_NIL, ftxH)) != EOK) {
        ADVFS_SAD1("tagdir_remove_tag:  can't pin page", sts);
    }

    tdmap = &tdpgp->tMapA[slot];

    /*
     * Because this routine is called by bs_delete_rtdn_opx() and
     * bs_delete_rtdn_opx() is called when the file was removed, and
     * may be called again at recovery time, BS_TD_IN_USE must not be
     * included in the assertion.
     */

    MS_SMP_ASSERT(tp->seq == (uint32T) tdmap->tmSeqNo
        || ((uint16T)~BS_TD_IN_USE & tp->seq) == (uint32T) tdmap->tmSeqNo);
    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));
    tdmap->tmSeqNo &= (uint16T)~BS_TD_IN_USE;
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
    unsigned long tagPg;
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

    TAG_DIR_LOCK(bfSetp, ftxH)

    /* Pin the page containing the tag to remove. */
    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, tagPg,
                         BS_NIL, ftxH)) != EOK) {
        domain_panic(bfSetp->dmnP,
                     "tagdir_tag_to_freelist returned %d:  can't pin page %d",
                     sts, tagPg);
        return;
    }

    tdmap = &tdpgp->tMapA[slot];
    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));

    /*
     * If domain panic happened before bs_delete_rtdn_opx() was called
     * during rbf_delete(), then rbf_bfs_open() would fail and
     * bs_tagdir_remove() would not be called, and BS_TD_IN_USE bit
     * would still be set so we cannot do the assertion below.
     */
    if (set_tag_to_unused || bfSetp->dmnP->dmn_panic) {
        tdmap->tmSeqNo &= (uint16T)~BS_TD_IN_USE;
    }
    else {
        /*
         * The following assert checks to make sure the IN_USE flag is NOT
         * still set on the tag to be deleted if the set_tag_to_unused 
         * flag is not set.
         */
        MS_SMP_ASSERT(tp->seq == ((uint32T) tdmap->tmSeqNo ^ BS_TD_IN_USE) );
    }

    rbf_pin_record(pgRef, (void *)&tdpgp->tpPgHdr, sizeof(struct bsTDirPgHdr));

    tdpgp->tpNumAllocTMaps--;
    bfSetp->bfCnt--;

    if (++tdmap->tmSeqNo == BS_TD_DEAD_SLOT) {
        /*
         * The slot has been reused the maximum number of times.
         */
        int tagsOnPg = tagPg == 0 ? BS_TD_TAGS_PG - 1 : BS_TD_TAGS_PG;

        if (++tdpgp->tpNumDeadTMaps == tagsOnPg) {
            /*
             * The entire page has only dead slots.  Nothing to do
             * here for now.
             */
        }
        return;
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
                                 bfSetp->dirBfAp, (unsigned long)0,
                                 BS_NIL, ftxH)) != EOK) {
                domain_panic(bfSetp->dmnP,
                        "tagdir_tag_to_freelist returned %d:  can't pin page 0",
                        sts);
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

        /* Update the in-core free list head. */

        bfSetp->tagFrLst = tagPg + 1;
    }
}


/*
 * An mcell marked for delete on the deferred delete list may have
 * already freed its tag when bs_close_one() called
 * tagdir_tag_to_freelist() - the system crashed before bs_close_one()
 * completed del_dealloc_stg() and removed the mcell from the deferred
 * delete list.  Or, the file may have been deleted but still
 * referenced when the system crashed so tagdir_tag_to_freelist()
 * must be called now.  Determine which is the case and call
 * tagdir_tag_to_freelist() if necessary.
 */

void
tagdir_freetag_dellist(
    bsMCT *mcp,        /* in - primary mcell from deferred delete list */
    vdT *pvdp          /* in - vd of deferred delete list */
    )
{
    statusT sts;
    bfSetIdT bfSetId;
    bfSetT *bfSetp = NULL;
    domainT *dmnP = pvdp->dmnP;
    unsigned long tagPg;
    int slot, free_slot;
    bfPageRefHT refPgH;
    bsTDirPgT *tdpgp;
    bsTMapT *tdmap = NULL;
    ftxHT ftxH;


    bfSetId.domainId = dmnP->domainId;
    bfSetId.dirTag = mcp->bfSetTag;

    sts = rbf_bfs_open( &bfSetp, bfSetId, BFS_OP_IGNORE_DEL,
                        FtxNilFtxH );
    if (sts != EOK) {
        domain_panic( dmnP, "tagdir_freetag_dellist: rbf_bfs_open failed" );
        goto HANDLE_EXCEPTION;
    }

    tagPg = TAGTOPG( &mcp->tag );
    slot = TAGTOSLOT( &mcp->tag );

    sts = bs_refpg( &refPgH, (void **)&tdpgp, bfSetp->dirBfAp,
                    tagPg, BS_NIL);
    if ( sts != EOK ) {
        domain_panic( dmnP, "tagdir_freetag_dellist: bs_refpg failed");
        goto HANDLE_EXCEPTION;
    }

    tdmap = &tdpgp->tMapA[slot];

    if ( tdmap->tmSeqNo & BS_TD_IN_USE ) {
        /*
         * If the slot is marked in-use, it must have been freed in
         * bs_close_one() and reused before the mcell was removed
         * from the deferred delete list - two different transactions,
         * so, no work to do here.
         */
        goto HANDLE_EXCEPTION;
    }

    /*
     * The slot is not in use.  Determine if it was moved to the
     * freelist before the system crashed.  If it's there, no
     * work to do.  Otherwise, call tagdir_tag_to_freelist()
     * to place it on the freelist.
     */

    for ( free_slot = tdpgp->tpNextFreeMap - 1;
          free_slot >= 0 && free_slot != slot;
          free_slot = tdpgp->tMapA[free_slot].tmNextMap - 1 );

    if ( free_slot != slot ) {
        sts = FTX_START_N( FTA_TAG_TO_FREELIST, &ftxH, FtxNilFtxH,
                          dmnP, 1 );
        if ( sts != EOK ) {
            domain_panic( dmnP, "tagdir_freetag_dellist: ftx_start failed");
            goto HANDLE_EXCEPTION;
        }
        tagdir_tag_to_freelist( bfSetp, &mcp->tag, ftxH, FALSE );
        ftx_done_n( ftxH, FTA_TAG_TO_FREELIST );
    }

HANDLE_EXCEPTION:

    if ( tdmap )
        bs_derefpg(refPgH, BS_CACHE_IT);

    if ( bfSetp )
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
}


/*
 * init_next_tag_page - this will init a tag page, allocating
 * additional storage if necessary.  It is assumed to always be the
 * next uninit'd page, that is, skipping beyond the first uninit'd
 * page will not work.
 */

static statusT
init_next_tag_page(
                   unsigned long tagPg, /* in - page to init */
                   bfSetT* bfSetp,      /* in - ptr to bitfile set */
                   bfAccessT* bfap,     /* in - access struct ptr */
                   ftxHT ftxH,           /* in - ftx handle */
                   int checkmigtrunclock        /* in */
                   )
{
    rbfPgRefHT pgRef;
    statusT sts;
    bsTDirPgT *tdpgp;
    int slot;
    int i;

    if (tagPg == bfSetp->tagUnMpPg) {
        sts = rbf_add_stg(bfSetp->dirBfAp, tagPg, BS_TD_XPND_PGS, ftxH,
                          checkmigtrunclock);
        if (sts != EOK) {
            return sts;
        }

        bfSetp->tagUnMpPg += BS_TD_XPND_PGS;

        if (BS_BFTAG_RSVD( bfap->tag ) ||
            (lk_get_state( bfap->stateLk) != ACC_FTX_TRANS)) {
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
            ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
        }
    }

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp,
                         tagPg, BS_NIL, ftxH)) != EOK) {
        return sts;
    }

    /* 
     * Initialize the page header and the subordinate free list.
     * Note that some the tag page header is initialized via bzero.
     */
    rbf_pin_record(pgRef, (void *)tdpgp, sizeof(bsTDirPgT));
    bzero( (char*)tdpgp, sizeof(bsTDirPgHdrT));
    tdpgp->tpCurrPage = tagPg;
    /*
     * point the new page to the existing free list.  the free list
     * will be pointed to this page further on.
     */
    tdpgp->tpNextFreePage  = bfSetp->tagFrLst;

    for (i = 0; i < BS_TD_TAGS_PG; i++) {
        /* ugly, but overlays seqNo and zeroes vdIndex */
        tdpgp->tMapA[i].tmFreeListHead = 1;
        tdpgp->tMapA[i].tmNextMap = i + 2;
    }

    tdpgp->tMapA[BS_TD_TAGS_PG - 1].tmNextMap = 0;

    /* 
     * Page 0 is special in that the first tagmap struct is used 
     * to store some metadata information.
     */
    slot = tagPg ==  0 ? 1 : 0;
    tdpgp->tpNextFreeMap = slot + 1;

    /* Pin page 0 of the tagdir, if not already pinned. */
    if (tagPg != 0) {
        if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                             (unsigned long)0, BS_NIL, ftxH)) != EOK) {
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
    unsigned long tagPg;
    rbfPgRefHT pgRef;
    statusT sts;
    bsTDirPgT *tdpgp;
    bsTDirPgT* tdpg0p;
    unsigned int slot;
    ftxHT ftxH;
    domainT *dmnP = bfSetp->dmnP;


    TAG_DIR_LOCK(bfSetp, parentFtxH)

    MS_SMP_ASSERT(bfSetp->tagUnInPg <= bfSetp->tagUnMpPg);

    /*
     * Pick up the page number from the free list.  Note that it is
     * "1 based" so that zero means the list is empty.
     */
getfreeslot:
    if ((tagPg = bfSetp->tagFrLst) == 0) {
        tagUnInitPageRedoT uipr;

        if ((sts = FTX_START_N(FTA_BS_TAG_EXTEND_TAGDIR_V1, &ftxH, parentFtxH, 
                               dmnP, 2)) != EOK) {
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

        ftx_done_urdr(ftxH, FTA_BS_TAG_EXTEND_TAGDIR_V1, 0,0,0,0,
                      sizeof(uipr), (void*)&uipr );

    } else {
        /*
         * At this point, the free list is not empty.
         */
        tagPg--;                     /* 1 based */
    }

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                         tagPg, BS_NIL, parentFtxH)) != EOK) {
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

    if ( !(tdmap->tmSeqNo & BS_TD_IN_USE) ){

        if ( tdmap->tmSeqNo >= BS_TD_DEAD_SLOT ) {
            return E_BAD_TAGDIR;
        }

        *tdpgpp = tdpgp;
        *tdmapp = tdmap;
        *tdpgRefp = pgRef;
        mcellUIdp->ut.tag.num = MKTAGNUM(tagPg, slot);
        mcellUIdp->ut.tag.seq = tdmap->tmSeqNo | BS_TD_IN_USE;

        return EOK;

    }

    /*
     * The remaining code is an error path executed only when free
     * list corruption has been detected.
     */

badfreelist:
    if ((sts = FTX_START_N(FTA_BS_TAG_PATCH_FREELIST_V1, &ftxH, 
                           parentFtxH, dmnP, 2)) != EOK) {
        return (sts);
    }
    /* 
     * free list is corrupt on this page.  kill the free list. 
     * goto to the next tag dir page in the "page free list".
     */

    aprintf( "ADVFS: BAD TAGDIR; pg = %d, dmnid = %lx, tag = %d\n",
            tagPg, *(long*)&bfSetp->bfSetId.domainId,
            bfSetp->bfSetId.dirTag.num );

    /* need to pin page in context of this subftx */
    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                         tagPg, BS_NIL, ftxH)) != EOK) {
        goto fail;
    }

    rbf_pin_record(pgRef, (void *)&tdpgp->tpNextFreeMap, 
                   sizeof(tdpgp->tpNextFreeMap));
    tdpgp->tpNextFreeMap = 0;
    rbf_pin_record(pgRef, (void *)&tdpgp->tpNumAllocTMaps, 
                   sizeof(tdpgp->tpNumAllocTMaps));
    tdpgp->tpNumAllocTMaps = BS_TD_TAGS_PG;

    if (tagPg != 0) {
        if ((sts = rbf_pinpg(&pgRef, (void **)&tdpg0p, bfSetp->dirBfAp, 
                             (unsigned long)0, BS_NIL, ftxH)) != EOK) {
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

    ftx_done_n(ftxH, FTA_BS_TAG_PATCH_FREELIST_V1);

    /* try again */

    goto getfreeslot;

fail:
    /*
     * This error path is only used if a subtransaction has been
     * started, else simply return the error.
     */
    ftx_fail(ftxH);
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
    tagUnInitPageRedoT* uipp = (tagUnInitPageRedoT*)opRec;
    statusT sts;
    bfSetT* bfSetp;
    domainT *dmnP = ftxH.dmnP;


    if ( !BS_UID_EQL(uipp->bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(uipp->bfSetId.domainId, dmnP->dualMountId)) ) {
           /* I can't hit this at all. */
           /* I'm not sure it's needed, but I'll leave it in. */
            uipp->bfSetId.domainId = dmnP->domainId;
        } else {
            ADVFS_SAD0("extend_tagdir_redo_opx: domainId mismatch");
        }
    }

    sts = rbf_bfs_open(&bfSetp, uipp->bfSetId, BFS_OP_IGNORE_DEL, ftxH);
    if ( sts != EOK ) {
        if (sts == E_NO_SUCH_BF_SET) {
            /* The bitfile set may have been created and removed before
             * the log was rolled forward. Just ignore the error and
             * let recovery continue
             */

            goto HANDLE_EXCEPTION;
        }

        domain_panic(dmnP, "extend_tagdir_redo_opx: can't open bitfile set");
        goto HANDLE_EXCEPTION;
    }

    bfSetp->tagUnInPg = uipp->unInitPage;

    bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

HANDLE_EXCEPTION:
}


/* 
 * tagdir_insert_tag
 *
 * Initialize a new tag in the specified bitfile set.  This assumes
 * the parent ftx has the tag dir locked from a previous call to
 * tagdir_alloc_tag.  This pair of routines is only intended to be
 * called from bitfile create.  And don't even think about jacketing
 * them with all sorts of overhead to be usable from anywhere else
 * after I've been up all night squashing these routines.
 */

statusT
tagdir_insert_tag(
                  ftxHT parentFtxH,
                  mcellUIdT* mcellUIdp, /* in - mcelluid ptr */
                  bfSetT* bfSetp,       /* in - ptr to bitfile set */
                  bsTDirPgT *tdpgp,
                  bsTMapT *tdmap,
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
    tdmap->tmSeqNo |= BS_TD_IN_USE;
    tdmap->tmVdIndex = mcellUIdp->vdIndex;
    tdmap->tmBfMCId = mcellUIdp->mcell;

    /*
     * Take the tagmap struct off the head of the subordinate free list.
     * Update number allocated tmaps counter.
     */

    rbf_pin_record(tdpgRef, (void *)&tdpgp->tpNextFreeMap, 
                   sizeof(tdpgp->tpNextFreeMap));
    tdpgp->tpNextFreeMap = nextTMap;
    rbf_pin_record(tdpgRef, (void *)&tdpgp->tpNumAllocTMaps, 
                   sizeof(tdpgp->tpNumAllocTMaps));
    tdpgp->tpNumAllocTMaps++;

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
                         (unsigned long)0, BS_NIL, parentFtxH);
        if ( sts != EOK) {
            return sts;
        }

        /* Update the on-disk and in-core free list pointers. */
        rbf_pin_record(pgRef, (void *)&tdpg0p->tMapA[0], sizeof(bsTMapT));
        tdpg0p->tMapA[0].tmFreeListHead = bfSetp->tagFrLst
                                       = tdpgp->tpNextFreePage;
    }
    return (EOK);
}


/*
 * tagdir_set_next_tag - routine to reset the free list to use the
 * specified tag next, if free, else return an error.
 */

statusT
tagdir_set_next_tag(
                    bfSetT *bfSetp,     /* in - bitfile set desc ptr */
                    bfTagT *tag         /* in - next tag value to use */
                    )
{
    statusT sts;
    bsTDirPgT *tdpgntp, *tdpg0p;
    rbfPgRefHT pgntRef, pg0Ref;
    unsigned long tagPg;
    int slot;
    unsigned long pg;
    ftxHT ftxH;
    bsTMapT *tdmap;
    bsTMapT *prevTdmap;
    int prevFound;
    uint32T seq;
    bfAccessT *bfAccess;

    /*
     *  Tag values below this are always in use, so don't bother to
     *  do any work.
     */
    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        return E_ILLEGAL_CLONE_OP;
    }
    if (tag->num < FIRST_SETTABLE_TAG) {
        return E_TAG_EXISTS;
    }

    /*
     *  If there is a cached access structure, check it's state.
     *  (if one isn't cached, grab_bsacc() will allocate one).
     */
    sts = EOK;
    bfAccess = grab_bsacc (bfSetp, *tag, FALSE, NULL);
    if (bfAccess != NULL) {
        /*
         *  Is this thread the only accessor of the access struct?
         */
        if (bfAccess->refCnt == 1) {
            /*
             *  Chris says just fail if we find one in some state other than
             *  ACC_INVALID.
             */
            if (lk_get_state (bfAccess->stateLk) != ACC_INVALID) {
                sts = E_TAG_EXISTS;
            }

        } else {
            /*
             *  Some other thread is accessing the file.  Can't use it for
             *  next tag.
             */
            sts = E_TAG_EXISTS;
        }
        /*
         *  Done poking around in access struct - drop it by decrementing
         *  ref count.  Ensure we don't poke it again by zapping addr.
         */
        DEC_REFCNT (bfAccess);

        /* grab_bsacc() seized bfap->bfaLock.  Make sure we unlock it. */
        mutex_unlock (&bfAccess->bfaLock);
        bfAccess = NULL;
    }

    /*
     *  If we failed above, we get out here.
     */
    if (sts == E_TAG_EXISTS) {
        return(sts);
    }

    tagPg = tag->num / BS_TD_TAGS_PG;
    slot = tag->num % BS_TD_TAGS_PG;

    for ( pg = bfSetp->tagUnInPg; pg <= tagPg; pg += 1 ) {

        /* Prevent races with migrate code paths */
        MIGTRUNC_LOCK_READ( &(bfSetp->dirBfAp->xtnts.migTruncLk) );
        sts = FTX_START_N( FTA_BS_SET_NEXT_TAG_V1, &ftxH, FtxNilFtxH,
                    bfSetp->dmnP, 2);
        if ( sts != EOK ) {
            MIGTRUNC_UNLOCK( &(bfSetp->dirBfAp->xtnts.migTruncLk) );
            return sts;
        }
        TAG_DIR_LOCK(bfSetp, ftxH)

        sts = init_next_tag_page( pg, bfSetp, bfSetp->dirBfAp, ftxH, 1 );
        MIGTRUNC_UNLOCK( &(bfSetp->dirBfAp->xtnts.migTruncLk) );

        ftx_done_n( ftxH, FTA_BS_SET_NEXT_TAG_V1 );
        if ( sts != EOK ) {
            return sts;
        }
    }
    /*
     * actually rearrange the free list to point to the slot we want
     * to use.  First pin that page to make sure it's not already in
     * use.  Then, if the free list doesn't already point to that page
     * directly, follow it to unlink it.  Then mess with the slot list
     * on the page itself.  Don't forget to stuff the sequence number
     * correctly.
     */
    sts = FTX_START_N( FTA_BS_SET_NEXT_TAG_V1, &ftxH, FtxNilFtxH,
                bfSetp->dmnP, 2);
    if ( sts != EOK ) {
        return sts;
    }
    TAG_DIR_LOCK(bfSetp, ftxH)
    sts = rbf_pinpg( &pgntRef, (void**)&tdpgntp, bfSetp->dirBfAp, tagPg,
                    BS_NIL, ftxH );
    if ( sts != EOK ) {
        goto fail;
    }

    if ( tdpgntp->tMapA[slot].tmSeqNo & BS_TD_IN_USE ) {
        sts = E_TAG_EXISTS;
        goto fail;
    }

    if ( tagPg != 0 ) {
        sts = rbf_pinpg( &pg0Ref, (void**)&tdpg0p, bfSetp->dirBfAp,
                        (unsigned long)0, BS_NIL, ftxH );
        if ( sts != EOK ) {
            goto fail;
        }

    } else {
        tdpg0p = tdpgntp;
        pg0Ref = pgntRef;
    }

    /*
     *  Reorder free list (if necessary) such that target page is first page
     *  on free list.
     */
    if (bfSetp->tagFrLst == 0) {
        sts = EBAD_TAG;
        goto fail;
    }

    if (tagPg != bfSetp->tagFrLst-1) {
        rbfPgRefHT prevPgRef;
        bsTDirPgT *prevTdpgp;
        unsigned long prevTagPg;

        /*
         *  First we need to find the free list entry that points to
         *  the target page.
         */
        prevTagPg = bfSetp->tagFrLst-1;
        prevFound = FALSE;
        while (!prevFound) {
            /*
             *  Pin the page (unless it's page 0, which we pinned above).
             */
            if (prevTagPg != 0) {
                sts = rbf_pinpg(&prevPgRef, (void**)&prevTdpgp, 
                                bfSetp->dirBfAp,
                                prevTagPg, BS_NIL, ftxH);
                if ( sts != EOK ) {
                    goto fail;
                }
            } else {
                prevPgRef = pg0Ref;
                prevTdpgp = tdpg0p;
            }

            if (prevTdpgp->tpNextFreePage == 0) {
                break;
            } else if (prevTdpgp->tpNextFreePage-1 == tagPg) {
                prevFound = TRUE;
                break;
            } else {
                unsigned long oldPrevPg = prevTagPg;

                prevTagPg = prevTdpgp->tpNextFreePage-1;

                if (oldPrevPg != 0) {   /* don't reref page 0, needed below */
                    rbf_deref_page(prevPgRef, BS_RECYCLE_IT);
                }
            }
        }

        if (!prevFound) {
            sts = EBAD_TAG;
            goto fail;
        }

        /*
         *  Update previous page free list link to point past target page
         *  [prevPgRef, prevTdpgp].
         */
        rbf_pin_record(prevPgRef, (void *)&prevTdpgp->tpNextFreePage,
                       sizeof(prevTdpgp->tpNextFreePage));
        prevTdpgp->tpNextFreePage = tdpgntp->tpNextFreePage;

        /*
         *  Update target page free list link to point to current head of
         *  free list [pgntRef, tdpgntp].
         */
        rbf_pin_record(pgntRef, (void *)&tdpgntp->tpNextFreePage,
                       sizeof(tdpgntp->tpNextFreePage));
        tdpgntp->tpNextFreePage = bfSetp->tagFrLst;

        /*
         *  Update on-disk current head of list to point to target page
         *    [pg0Ref, tdpg0p].
         *    (on-disk and in-core)  
         */
        rbf_pin_record(pg0Ref, (void *)&tdpg0p->tMapA[0], sizeof(bsTMapT));
        tdpg0p->tMapA[0].tmFreeListHead = tagPg + 1;

        /*
         *  Update in-core current head of list to point to target page
         */
        bfSetp->tagFrLst = tagPg + 1;
    }

    MS_SMP_ASSERT(bfSetp->tagFrLst == tagPg + 1);
    MS_SMP_ASSERT(tdpgntp->tpCurrPage == tagPg);

    /*
     *  Now modify slot list on target page (if necessary) such that desired
     *  slot is first free slot on the page.
     */
    if (tdpgntp->tpNextFreeMap != (slot + 1)) {
        int prevSlot;

        prevSlot = tdpgntp->tpNextFreeMap - 1; 

        /*
         *  Iterate thru slots on page until we find the slot before
         *  the target slot.
         */
        while ((prevSlot >= 0) &&
               (tdpgntp->tMapA[prevSlot].tmNextMap != (slot + 1))) {
            prevSlot = tdpgntp->tMapA[prevSlot].tmNextMap - 1;
        }

        if (prevSlot < 0) {
            sts = EBAD_TAG;
            goto fail;
        }

        tdmap = &tdpgntp->tMapA[slot];
        prevTdmap = &tdpgntp->tMapA[prevSlot];

        /*
         *  Update previous slot to point past target slot.
         */
        rbf_pin_record(pgntRef, (void *)&prevTdmap->tmNextMap,
                       sizeof(prevTdmap->tmNextMap));
        prevTdmap->tmNextMap = tdmap->tmNextMap;

        /*
         *  Make target slot point to old first slot in list.
         */
        rbf_pin_record(pgntRef, (void *)&tdmap->tmNextMap,
                       sizeof(tdmap->tmNextMap));
        tdmap->tmNextMap = tdpgntp->tpNextFreeMap;

        /*
         *  Make head of slot list point to target slot.
         */
        rbf_pin_record(pgntRef, (void *)&tdpgntp->tpNextFreeMap,
                       sizeof(tdpgntp->tpNextFreeMap));
        tdpgntp->tpNextFreeMap = (slot + 1);
    }

    /*
     *  Target page is now at head of free page list.
     *  Target tag slot is at head of free slots list on that page.
     *  Set tag's sequence number to desired value.
     */
    seq = tag->seq;
    if (seq & BS_TD_IN_USE) {
        /*
         *  If specified sequence number includes "in-use" flag,
         *  turn that flag off so that tagdir_alloc_tag() will be able
         *  to assign the slot.
         */
        seq &= ~(BS_TD_IN_USE);
    }
    rbf_pin_record(pgntRef, (void *)&tdpgntp->tMapA[slot].tmSeqNo,
                   sizeof(tdpgntp->tMapA[slot].tmSeqNo));
    tdpgntp->tMapA[slot].tmSeqNo = seq;

    ftx_done_n( ftxH, FTA_BS_SET_NEXT_TAG_V1 );
    return EOK;
fail:
    ftx_fail( ftxH );
    return sts;
}


/*
 * Change what the tag points to.
 */

statusT
tagdir_reset_tagmap(
    tagInfoT *tip
    )
{
    return (tagdir_set_tagmap_common(tip, FALSE));
}


/*
 * Stuff the tag in the clone.
 */

statusT
tagdir_stuff_tagmap(
    tagInfoT *tip
    )
{
    return (tagdir_set_tagmap_common(tip, TRUE));
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
    unsigned long tagPg;
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
                           tip->dmnP, 1)) != EOK) {
        return (sts);
    }

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                    tagPg, BS_NIL, ftxH)) != EOK) {
        ftx_fail(ftxH);
        return (sts);
    }

    tdmap = &tdpgp->tMapA[slot];

    /* Save old values in case of an undo. */
    tagDataRec.tag = tip->tag;
    tagDataRec.map = *tdmap;
    bs_bfs_get_set_id(tip->bfSetp, &tagDataRec.bfSetId);

    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));
    if ( markInUse == TRUE ) {
        tdmap->tmSeqNo |= BS_TD_IN_USE;
    }
    tdmap->tmVdIndex = tip->vdIndex;
    tdmap->tmBfMCId = tip->bfMCId;

    ftx_done_u(ftxH, FTA_BS_TAG_WRITE_V1, sizeof(tagDataRecT), 
               (void *)&tagDataRec);
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
    tagDataRecT *rp = (tagDataRecT *) opRec;
    bfSetT *bfSetp;
    bsTMapT *tdmap;
    bsTDirPgT *tdpgp;
    rbfPgRefHT pgRef;
    unsigned long tagPg;
    unsigned slot;
    domainT *dmnP;
    statusT sts;
    int bfs_opened = 0;


    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(rp->bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag &  BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(rp->bfSetId.domainId, dmnP->dualMountId)) ) {
            rp->bfSetId.domainId = dmnP->domainId;
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
        sts = rbf_bfs_open(&bfSetp, rp->bfSetId, BFS_OP_IGNORE_DEL, ftxH);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP,"tagdir_write_undo_opx: can't open bitfile set");
            goto HANDLE_EXCEPTION;
        }
        bfs_opened = 1;
    } else {
        bfSetp = bs_bfs_lookup_desc( rp->bfSetId);
        MS_SMP_ASSERT( BFSET_VALID(bfSetp));
    }

    tagPg = TAGTOPG(&rp->tag);
    slot = TAGTOSLOT(&rp->tag);

    if ((sts = rbf_pinpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                         tagPg, BS_NIL, ftxH)) != EOK) {
        ADVFS_SAD1("tagdir_write_undo_opx: can't pin page", sts);
    }

    tdmap = &tdpgp->tMapA[slot];
    MS_SMP_ASSERT(rp->tag.seq == (uint32T) tdmap->tmSeqNo);

    rbf_pin_record(pgRef, (void *)tdmap, sizeof(bsTMapT));
    *tdmap = rp->map;

    if ( bfs_opened ) {
        bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    }
HANDLE_EXCEPTION:
}


/*
 * tagdir_lookup
 *
 * Given a tag, return bitfile descriptor and virtual disk index.
 *
 * The staticRootTagDirTag does not technically belong to any fileset.
 * Passing it in as the tp (even with dmnP->bfSetDirp) will result in
 * ENO_SUCH_TAG.
 */

statusT
tagdir_lookup(
    bfSetT *bfSetp,             /* in - bitfile set desc ptr */
    bfTagT *tp,                 /* in/out - pointer to tag to look up */
    bfMCIdT *bfMCId,            /* out - bitfile metadata cell */
    vdIndexT *vdIndex           /* out - virtual disk index */
    )
{
    unsigned long tagPg;
    int slot;
    bfPageRefHT pgRef;
    bsTDirPgT *tdpgp;
    bsTMapT *tdmap;
    statusT sts;
    domainT *domain = bfSetp->dmnP;

    if (BS_BFTAG_RSVD(*tp)) {

        bfMCId->cell = BS_BFTAG_CELL(*tp);
        *vdIndex     = BS_BFTAG_VDI(*tp);
        bfMCId->page = 0;            /* reserved cells always on page 0! */

        /* validate the tag */

        if (bfMCId->cell >= BFM_RSVD_TAGS) {
            return (ENO_SUCH_TAG);
        }

        /* Verify the vdIndex portion of the tag.  The vdIndex must
         * represent either a valid vdT struct, or a vdT struct in
         * the BSR_VD_VIRGIN state that is being set up by the
         * current thread.
         */    
        if ( vd_htop_if_valid(*vdIndex, domain, FALSE, FALSE) ) {
            return (EOK);
        } else { 
            /* All other conditions, this tag is no good */
            return (ENO_SUCH_TAG);
        }
    }
        
    if (!bfSetp) {
        return ENO_SUCH_TAG;
    }      

    tagPg = TAGTOPG(tp);
    slot = TAGTOSLOT(tp);

    if (tagPg >= bfSetp->tagUnInPg) {
        return ENO_SUCH_TAG;
    }

    if ((sts = bs_refpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, 
                        tagPg, BS_NIL)) != EOK) {
        return (sts);
    }

    tdmap = &tdpgp->tMapA[slot];

    if ( (tp->num == 0) || ((tdmap->tmSeqNo & BS_TD_IN_USE) == 0) ) {    
        (void) bs_derefpg(pgRef, BS_CACHE_IT);  /* ignore further errors */
        return (ENO_SUCH_TAG);
    }

    if (tp->seq != 0) {
        /* 
         * A non-zero sequence number is used when a match on
         * sequence numbers is requested.
         */
        if (tdmap->tmSeqNo != tp->seq) {
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
        tp->seq = tdmap->tmSeqNo;
    }   

    *bfMCId = tdmap->tmBfMCId;
    *vdIndex = tdmap->tmVdIndex;

    if ((sts = bs_derefpg(pgRef, BS_CACHE_IT)) != EOK) {
        return (sts);
    }

    if (sts = check_mcell_ptr (*vdIndex, *bfMCId, *tp, domain) != EOK ) {
        printf("tagdir_lookup: Invalid primary mcell (%d, %d, %d) for tag \
                %d in domain %s, fileset %s\n", *vdIndex, bfMCId->page, 
                bfMCId->cell, tp->num, domain->domainName,
                bfSetp->fsnp->mountp->m_stat.f_mntfromname);
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
    bfMCIdT *bfMCId,            /* out - bitfile metadata cell */
    vdIndexT *vdIndex           /* out - virtual disk index */
    )
{
    statusT sts;

    sts = tagdir_lookup(bfSetp, tp, bfMCId, vdIndex);
    switch (sts) {
        case EOK:
            return (EOK);
        case ENO_SUCH_TAG:
            sts = tagdir_lookup_next(bfSetp, tp, bfMCId, vdIndex);
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
    bfMCIdT *bfMCId,            /* out - bitfile metadata cell */
    uint16T *vdIndex            /* out - virtual disk index */
    )
{
    unsigned long tagPg;
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
            if (bs_refpg(&pgRef, (void **)&tdpgp, bfSetp->dirBfAp, tagPg, 
                                BS_SEQ_AHEAD) !=  EOK) {
                return (ENO_SUCH_TAG);
            }
            isReferenced = 1;
        }

        MS_SMP_ASSERT(tdpgp->tpCurrPage == tagPg);
        tdmap = &tdpgp->tMapA[slot];

        if ((tdmap->tmSeqNo & BS_TD_IN_USE) == 0) {
            continue;
        }
        
        *bfMCId = tdmap->tmBfMCId;
        *vdIndex = tdmap->tmVdIndex;
        tp->seq = tdmap->tmSeqNo;
        tp->num = MKTAGNUM(tagPg, slot);
        (void) bs_derefpg(pgRef, BS_CACHE_IT);

        if ( !(vdp = vd_htop_if_valid(*vdIndex, domain, FALSE, FALSE)) ) {

            if ( bfSetp->fsnp != NULL ) {
                domain_panic(domain,
         "tagdir_lookup_next: invalid volume index %d for tag %d in fileset %s",
                   *vdIndex, tp->num,
                   bfSetp->fsnp->mountp->m_stat.f_mntfromname);
            } else {
                domain_panic(domain,
     "tagdir_lookup_next: invalid volume index %d for tag %d in fileset tag %d",
                   *vdIndex, tp->num, bfSetp->bfSetId.dirTag.num);
            }
            return (E_BAD_TAGDIR);
        }
        if ( bfMCId->cell >= BSPG_CELLS ) {
            if ( bfSetp->fsnp != NULL ) {
                domain_panic(domain,
         "tagdir_lookup_next: invalid mcell number %d for tag %d in fileset %s",
                   bfMCId->cell, tp->num,
                   bfSetp->fsnp->mountp->m_stat.f_mntfromname);
            } else {
                domain_panic(domain,
     "tagdir_lookup_next: invalid mcell number %d for tag %d in fileset tag %d",
                   bfMCId->cell, tp->num, bfSetp->bfSetId.dirTag.num);
            }
            return (E_BAD_TAGDIR);
        }
        if ( bfMCId->page >= vdp->bmtp->nextPage ) {
            if ( bfSetp->fsnp != NULL ) {
                domain_panic(domain,
      "tagdir_lookup_next: invalid BMT page number %d for tag %d in fileset %s",
                   bfMCId->page, tp->num,
                   bfSetp->fsnp->mountp->m_stat.f_mntfromname);
            } else {
                domain_panic(domain,
  "tagdir_lookup_next: invalid BMT page number %d for tag %d in fileset tag %d",
                    bfMCId->page, tp->num, bfSetp->bfSetId.dirTag.num);
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
    uint32T newPageCnt;
    bfTagT newTag;
    vdT *newVd;
    char *oldPage;
    uint32T oldPageCnt;
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
    sts = FTX_START_EXC(FTA_NULL, &ftxH, dmnP, 1);
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
    oldPageCnt = dmnP->bfSetDirAccp->nextPage;
    newPageCnt = newBfAccessp->nextPage;

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
        sts = FTX_START_N(FTA_NULL, &subFtxH, ftxH, dmnP, 0);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        sts = rbf_add_stg( newBfAccessp,
                           newPageCnt,
                           oldPageCnt - newPageCnt,
                           subFtxH, FALSE );
        if (sts != EOK) {
            ftx_fail (subFtxH);
            RAISE_EXCEPTION (sts);
        }
        ftx_special_done_mode (subFtxH, FTXDONE_SKIP_SUBFTX_UNDO);

        ftx_done_n (subFtxH, FTA_BS_SWITCH_ROOT_TAGDIR_V1);
    }

    /*
     * Copy the contents of the current tag directory to the new tag directory.
     */

    for (i = 0; i < oldPageCnt; i++) {

        sts = bs_refpg (&pgRef, (void *)&oldPage, dmnP->bfSetDirAccp, i, BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }
        derefFlag = 1;

        sts = bs_pinpg (&pgPin, (void *)&newPage, 
                        newBfAccessp, i, BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        bcopy (oldPage, newPage, ADVFS_PGSZ);

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
    sts = bfflush (newBfAccessp, 0, 0, FLUSH_IMMEDIATE);
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

    if ( RBMT_THERE(dmnP) ) {
        mdap = logVd->rbmtp;
        mcell_index = BFM_RBMT_EXT;
    } else {
        mdap = logVd->bmtp;
        mcell_index = BFM_BMT_EXT_V3;
    }
    sts = rbf_pinpg( &rbfPgPin, (void*)&bmtPage, mdap, 0, BS_NIL, ftxH );
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

    bs_bfs_switch_root_tagdir (dmnP, newTag);

    /* FIX - is any synchronization needed here? */
    bfSet = dmnP->bfSetDirp;
    dmnP->bfSetDirAccp = bfSet->dirBfAp;
    dmnP->bfSetDirTag = newTag;

    ftx_done_urdr (
                   ftxH,
                   FTA_BS_SWITCH_ROOT_TAGDIR_V1,
                   0, NULL,  /* undo */
                   0, NULL,  /* root done */
                   sizeof (newTag), &newTag  /* op redo */
                   );

    /*
     * Checkpoint the log so there can be no undo or redo for the
     * old root tag directory.
     */
    lgr_checkpoint_log (dmnP);

    return EOK;

HANDLE_EXCEPTION:
    if (derefFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }
    if (closeNewFlag != 0) {
        bs_close (newBfAccessp, 0);
    }
    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
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

    newTag = *((bfTagT *)redoRecAddr);

    bs_bfs_switch_root_tagdir (domain, newTag);

    bfSet = domain->bfSetDirp;
    domain->bfSetDirAccp = bfSet->dirBfAp;
    domain->bfSetDirTag = newTag;

    return;
}  /* end switch_root_tagdir_redo_opx */
