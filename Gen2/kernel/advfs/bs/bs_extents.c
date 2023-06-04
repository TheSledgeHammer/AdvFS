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
 *  AdvFS
 *
 * Abstract:
 *
 *  bs_extents.c
 *  This module contains routines related to on-disk and in-memory extents.
 *
 */

#include <ms_public.h>
#include <ms_privates.h>
#include <bs_extents.h>
#include <bs_inmem_map.h>
#include <bs_migrate.h>
#include <bs_snapshot.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE BS_EXTENTS

/*
 * protos
 */

opxT undo_mcell_cnt;

void
undo_mcell_cnt (
                ftxHT ftxH,
                int size,
                void* address
                );

opxT undo_upd_xtnt_rec;

void
undo_upd_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void* address
                   );

opxT undo_cre_xtnt_rec;

void
undo_cre_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void* address
                   );

static
void
update_xtnt_array (
                   rbfPgRefHT pgPin,  /* in */
                   bsInMemSubXtntMapT *subXtntMap,  /* in */
                   uint16_t *bsXACnt,  /* in */
                   bsXtntT *bsXA  /* in */
                   );

static
statusT
load_inmem_xtnt_map (
                     bfAccessT *bfap,           /* in, modified */
                     bsXtntRT *xtntRec,         /* in */
                     bf_fob_t *ret_total_fob_cnt, /* out */
                     bsInMemXtntMapT **new_xtntmap /* out */
                     );

static
statusT
x_load_inmem_xtnt_map (
                       struct bfAccess *bfap,      /* in, modified */
                       struct bsInMemXtntMap **new_xtntmap /* out */
        );

static
statusT
load_from_xtra_xtnt_rec (
                         bfAccessT *bfap,                  /* in, modified */
                         bfMCIdT mcellId,                  /* in */
                         bsInMemSubXtntMapT *subXtntMap,   /* in */
                         bf_fob_t last_fob,    /* in */
                         bfMCIdT *nextMcellId              /* out */
                         );

static
statusT
load_from_bmt_xtra_xtnt_rec (
                             domainT *dmnP,  /* in */
                             bfMCIdT mcellId,  /* in */
                             bf_vd_blk_t vd_blk,
                             bsInMemSubXtntMapT *subXtntMap,  /* in */
                             bfMCIdT *nextMcellId,  /* out */
                             bf_vd_blk_t *next_vd_blk
                             );

static
statusT
load_from_xtnt_array (
                      int cnt,  /* in */
                      bsXtntT *bsXA,  /* in */
                      bsInMemSubXtntMapT *subXtntMap  /* in */
                      );

/* Debug functions.   */
statusT
odm_print_xtnt_map (
                    domainT *domain,            /* in */
                    bfTagT bfTag,               /* in */
                    vdIndexT mcellVdIndex,      /* in */
                    bs_meta_page_t mcellPage,   /* in */
                    uint32_t mcellCell          /* in */
                    );

statusT
odm_check_xtnt_map (
                    domainT *domain,            /* in */
                    bfTagT bfTag,               /* in */
                    vdIndexT mcellVdIndex,      /* in */
                    bs_meta_page_t mcellPage,   /* in */
                    uint32_t mcellCell          /* in */
                    );


int CheckXtnts = FALSE;

typedef struct mcellCntUndoRec
{
    uint32_t type;
    bfMCIdT mCId;
    bfTagT bfTag;
    uint32_t mcellCnt;
} mcellCntUndoRecT;

/*
 * undo_mcell_cnt
 *
 * This function is called when an on-disk extent transaction must be backed
 * out.  The subtransactions that were completed before this subtransaction
 * have not been backed out while the subtransactions after this subtransaction
 * have been backed out.
 *
 * This function assumes that the transaction was a subtransaction and that
 * the appropriate locks are still held.  During run-time, the parent
 * transaction took out the locks.  During recovery, the parent transaction's
 * undo routine took out the locks.
 *
 * Transaction:  The primary extent record's "mcellCnt" field is modified.
 */

void
undo_mcell_cnt (
                ftxHT ftxH,
                int size,
                void *address
                )
{
    bsMPgT *bmtp;
    domainT *dmnP;
    bsMCT *mcellp;
    rbfPgRefHT pgRef;
    statusT sts;
    mcellCntUndoRecT undoRec;
    bsXtntRT *xtntRec;
    vdT *vdp;
    struct bfAccess *mdap;

    bcopy(address, &undoRec, sizeof(mcellCntUndoRecT));

    dmnP = ftxH.dmnP;

    vdp = VD_HTOP(undoRec.mCId.volume, dmnP);

    if ( BS_BFTAG_RSVD(undoRec.bfTag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }
    sts = rbf_pinpg ( &pgRef,
                      (void *)&bmtp,
                      mdap,
                      undoRec.mCId.page,
                      BS_NIL,
                      ftxH,
                      MF_NO_VERIFY);
    if (sts != EOK) {
        /* FIX - correct action? */
        ADVFS_SAD1("undo_mcell_cnt: rbf_pinpg failed ", (long) BSERRMSG(sts));
    }

    mcellp = &(bmtp->bsMCA[undoRec.mCId.cell]);

    /*
     * Find the primary extent record and restore the mcell count.
     */

    xtntRec = bmtr_find (mcellp, BSR_XTNTS, vdp->dmnP);
    if (xtntRec == NULL) {
        ADVFS_SAD0("undo_mcell_cnt: can't find extent record");
    }

    rbf_pin_record( pgRef,
                    &xtntRec->mcellCnt,
                    sizeof(xtntRec->mcellCnt));
    xtntRec->mcellCnt = undoRec.mcellCnt;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_169);

    return;

}  /* end undo_mcell_cnt */


/*
 * undo_restore_orig_xtntmap
 *
 * This function will reload the on-disk extentmaps. This is meant to be
 * run AFTER all on-disk undo's for the addition or removal of storage
 * have run. At this point it is now safe to read from disk the extent
 * maps and replace the existing extent maps.
 *
 * It is assumed that all UFC pages covering the ranges that the
 * storage is changing for have been invalidated. Currently this is
 * done in add_stg_undo and the removal code has done this prior to
 * removing the storage.
 * 
 * We will no longer throw out extent maps. Other than at file open
 * the extents should always be valid for a file. 
 *
 * The final key assumption in this routine is that any time this routine is
 * called via ftx_fail, the bfap on which it is called is still in open
 * (non-zero refCnt) OR we are in recovery.
 * 
 */

void
undo_restore_orig_xtntmap (
                   ftxHT ftxH,
                   int32_t size,
                   void *address
                   )
{

    domainT *dmnP;
    statusT sts = 0;
    restoreOrigXtntmapUndoRecT undoRec;
    bsInMemXtntT *xtnts;
    int16_t bfs_is_open = 0;       /* true if bitfile set was opened here */
    int16_t bmt_or_rbmt;
    int16_t bfap_is_open = 0;
    bsInMemXtntMapT *orig_xtntmap;
    bfSetT *bfSetp;
    vdT *vd;
    bfAccessT *bfap;

    bcopy(address, &undoRec, sizeof(restoreOrigXtntmapUndoRecT));

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(undoRec.setId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(undoRec.setId.domainId, dmnP->dualMountId)) ) {
            undoRec.setId.domainId = dmnP->domainId;
        }
        else {
            domain_panic(dmnP,"undo_restore_orig_xtntmap: domainId mismatch");
            goto HANDLE_EXCEPTION;
        }
    }
    if (dmnP->state != BFD_ACTIVATED) {
        /* We are in recovery, must open the bitfile set.  Remember that we
         * must close this on the way out.
         */
        sts = bfs_open (&bfSetp, undoRec.setId, BFS_OP_IGNORE_DEL, ftxH);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP," undo_restore_orig_xtntmap: rbf_bfs_access error");
            goto HANDLE_EXCEPTION;
        }
        bfs_is_open = 1;
    } else {
        /* Just lookup the bitfile set handle.  */
        bfSetp = bfs_lookup( undoRec.setId );
        MS_SMP_ASSERT( BFSET_VALID(bfSetp) );
    }

    bmt_or_rbmt = BS_BFTAG_RSVD(undoRec.bfTag) &&
                    (BS_BFTAG_RBMT(undoRec.bfTag) || BS_IS_TAG_OF_TYPE( undoRec.bfTag, BFM_BMT));

    if (bmt_or_rbmt) {
        vd = VD_HTOP(undoRec.bfap_or_vd.vdIndex, dmnP);
        if (BS_BFTAG_RBMT(undoRec.bfTag)){
            bfap = vd->rbmtp;
        } else {
            bfap = vd->bmtp;
        }
    } else {        
        /*
         * If the domain was activated prior to entering the routine 
         * (bfs_is_open==0), simply grab the bfap pointer from the undoRec.
         * The non recovery case implies that this thread is in a failed
         * transaction and already has the bfap open.
         */
        if (!bfs_is_open) {
            /* non recovery case */
            bfap = undoRec.bfap_or_vd.bfap; 
            MS_SMP_ASSERT(bfap);

        } else {
            /* must be the recovery case */
 	    /*
             * We need a pointer to the bfap.  Unless we are in recovery the
             * bfap already exists and has a reference on it.  Normally we would
             * call bs_access which would also open any parent snapshots.  In
             * this case, we will call bs_access_one so that we only need to
             * call bs_close_one.  If we try to call bs_close it will try to
             * take the bfaSnapLock which will still be held when failing a
             * storage add transaction.
             */
            sts = bs_access_one (&bfap, undoRec.bfTag, bfSetp, ftxH,
                                 BF_OP_INTERNAL);
            if (sts != EOK) {
                if (sts != ENO_SUCH_TAG) {
                    domain_panic(ftxH.dmnP,
                            "undo_restore_orig_xtntmap: bs_access_one error %d",
                            sts);
                }
            
            /* else
             * We must allow for a failed access on a file that was removed.
             * A file could have been deleted but held open and storage added 
             * to it. At recovery we will remove the file and then attempt to
             * run this undo routine. The storage has all been cleaned up.
             * The purpose of this undo routine is to make sure the in-memory
             * extent maps are correct. Since the file is gone this undo can
             * be skipped.
             */
                goto HANDLE_EXCEPTION;
            }
            bfap_is_open = 1;
        }
    }

    /* We are ready to clean up the in memory extent maps. At this
     * point we can be operating on a meta-data file or a user-data
     * file including the BMT or RBMT. We set up this undo to run such
     * that ALL on-disk changes for the undoing of storage have taken
     * place. It is now safe to reload the extent maps from disk to
     * their original state prior to this storage operation.
     *
     * This undo is strictly an in-memory only undo, but does need to
     * be run during recovery. For example an insert to a directory
     * could be run causing the extent maps to be read in. Next this
     * insert may have occured to a page that was added to the directory
     * which is then removed. The extent maps still reflect that this
     * directory page exists in the file. It is therefor imperative that
     * we load in the correct extent maps in all cases.
     *
     * We will first read the extent maps into a temporary extent map
     * while holding the extent map lock for read ( see the comment
     * below for why). Next we will obtain the extent map lock for 
     * write and swap this copy in as the real extent map.
     *
     * During this time the extents are visible to outside threads. However
     * we are holding locks that will prevent any pages in these extents
     * from being read (getpage):
     *
     * User-data - filelock for write and the Migstg lock
     * BMT/RBMT - vd' mcell locks
     * Directory/index - file lock for write.
     * ACL's - Not sure.
     *
     * Thus by preventing lookups on these files the extents that map
     * the storage we are changing will not be accessed. This leaves
     * pages that are still in the cache. These pages can be flushed
     * by VHAND or SYNCD for example. This has been taken care of
     * however because if the extent maps were made visible, then the
     * add_stg_undo routine had to have run already which has
     * invalidated these pages from the cache. Or in the case of
     * removal of storage, we invalidated these pages from the cache
     * before removing the extents.  If we failed before merging the
     * extents, then no pages in this range can be in the cache anyway.
     * So by the time we get here there should be no pages in the cache
     * for the range of extent maps that we are about to change.
     */
    
    xtnts = &(bfap->xtnts);

    /* Read the extent maps from disk while only holding the xtntMap_lk
     * for reading. We can not block for UFC pages or memory while holding
     * this lock exclusively. VHAND would not be able to make progress if
     * we happened to have all of the UFC pages in cache since it needs our
     * xtntMap_lk for read in order to steal our pages.
     */

    sts = x_load_inmem_xtnt_map(bfap, &orig_xtntmap);
    if (sts != EOK) {
        domain_panic(dmnP,"undo_restore_orig_xtntmap: can't load xtnts");
        goto HANDLE_EXCEPTION;
    }

    /* Now we will obtain the extent map lock for write and
     * drop back in the orignal extents
     */

    ADVRWL_XTNTMAP_WRITE(&bfap->xtntMap_lk);
    ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);
    

    imm_delete_xtnt_map (xtnts->xtntMap);

    xtnts->xtntMap = orig_xtntmap;
    bfap->bfaNextFob = imm_get_next_fob(xtnts);
    
    ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);

HANDLE_EXCEPTION:

    if (bfap_is_open) {
        sts = bs_close_one(bfap, MSFS_CLOSE_NONE, ftxH);
        if (sts != EOK) {
            domain_panic(ftxH.dmnP,"undo_restore_orig_xtntmap --- bs_close failed ---");
        }
    }

    if ( bfs_is_open ) {
        sts = bs_bfs_close (bfSetp, ftxH, BFS_OP_DEF);
        if (sts != EOK) {
          domain_panic(ftxH.dmnP,"undo_restore_orig_xtntmap --- bs_bfs_close failed ---");
        }
    }

    return;
}


typedef struct updXtntRecUndoRec
{
    uint16_t type;
    uint16_t xCnt;
    uint16_t index;
    uint16_t cnt;
    bfMCIdT mCId;
    bfTagT bfTag;
    union {
        bsXtntT xtnt[BMT_XTNTS];
        bsXtntT xtraXtnt[BMT_XTRA_XTNTS];
    } bsXA;
} updXtntRecUndoRecT;



/*
 * undo_upd_xtnt_rec
 *
 * This function when an on-disk extent transaction must be backed out.  The
 * subtransactions that were completed before this subtransaction have not been
 * backed out while the subtransactions after this subtransaction have been
 * backed out.
 *
 * This function assumes that the transaction was a subtransaction and that
 * the appropriate locks are still held.  During run-time, the parent
 * transaction took out the locks.  During recovery, the parent transaction's
 * undo routine took out the locks.
 *
 * Transaction:  The primary or extra extent record's descriptors were updated.
 */

void
undo_upd_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void *address
                   )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint16_t *bsXACnt;
    domainT *domain;
    uint16_t i;
    bsMCT *mcell;
    rbfPgRefHT pgPin;
    statusT sts;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    updXtntRecUndoRecT undoRec;
    vdT *vdp;
    struct bfAccess *mdap;

    /*
     * Copy address locally to avoid potential unaligned access
     * to the integer aligned log page.
     */

    bcopy (address, &undoRec, size);

    domain = ftxH.dmnP;

    vdp = VD_HTOP(undoRec.mCId.volume, domain);

    if ( BS_BFTAG_RSVD(undoRec.bfTag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }
    sts = rbf_pinpg (&pgPin,
                     (void *)&bmt,
                     mdap,
                     undoRec.mCId.page,
                     BS_NIL,
                     ftxH, 
                     MF_NO_VERIFY);
    if (sts != EOK) {
        /* FIX - correct action? */
        ADVFS_SAD1("undo_upd_xtnt_rec: rbf_pinpg failed ", (long)BSERRMSG(sts));
    }

    mcell = &(bmt->bsMCA[undoRec.mCId.cell]);

    switch (undoRec.type) {

      case BSR_XTNTS:

        xtntRec = bmtr_find (mcell, BSR_XTNTS, vdp->dmnP);
        if (xtntRec == NULL) {
            ADVFS_SAD0("undo_upd_xtnt_rec: can't find extent record");
        }
        bsXACnt = &(xtntRec->xCnt);
        bsXA = xtntRec->bsXA;

        break;

      case BSR_XTRA_XTNTS:

        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vdp->dmnP);
        if (xtraXtntRec == NULL) {
            ADVFS_SAD0("undo_upd_xtnt_rec: can't find extra extent record");
        }
        bsXACnt = &(xtraXtntRec->xCnt);
        bsXA = xtraXtntRec->bsXA;

        break;

      default:
        ADVFS_SAD1("undo_upd_xtnt_rec: bad extent record type", undoRec.type);
    }  /* end switch */

    rbf_pin_record (
                    pgPin,
                    &(bsXA[undoRec.index]),
                    sizeof (bsXtntT) * undoRec.cnt
                    );

    for (i = 0; i < undoRec.cnt; i++) {
        bsXA[undoRec.index + i] = undoRec.bsXA.xtnt[i];
        ADVFS_CRASH_RECOVERY_TEST(domain, AdvfsCrash_170);
    }

    rbf_pin_record (pgPin, bsXACnt, sizeof (*bsXACnt));
    *bsXACnt = undoRec.xCnt;
    ADVFS_CRASH_RECOVERY_TEST(domain, AdvfsCrash_171);

    return;

}  /* end undo_upd_xtnt_rec */


typedef struct creXtntRecUndoRec
{
    bfMCIdT mcellId;
} creXtntRecUndoRecT;

/*
 * undo_cre_xtnt_rec
 *
 * This function backs out an on-disk extent transaction for
 * the rbmt mcell RBMT_RSVD_CELL.  (All other cases are handled by the
 * mcell allocate routine.)  The subtransactions that were
 * completed before this subtransaction have not been backed
 * out while the subtransactions after this subtransaction
 * have been backed out.
 *
 * Transaction:  The mcell's tag, set tag, rec type and rec
 * count need to be cleared for the RBMT_RSVD_CELL case.
 */

void
undo_cre_xtnt_rec (
                   ftxHT ftxH,
                   int size,
                   void *address
                   )
{
    bsMPgT *rbmt;
    domainT *domain;
    bsMCT *mcp;
    rbfPgRefHT pgPin;
    statusT sts;
    creXtntRecUndoRecT undoRec;
    vdT *vdp;
    struct bfAccess *mdap,*bfap;
    bsMRT *rec;

    /*
     * Copy address locally to avoid potential unaligned access
     * to the integer aligned log page.
     */

    bcopy (address, &undoRec, size);

    MS_SMP_ASSERT( undoRec.mcellId.cell == RBMT_RSVD_CELL );

    domain = ftxH.dmnP;

    vdp = VD_HTOP(undoRec.mcellId.volume, domain);
    mdap = vdp->rbmtp;

    sts = rbf_pinpg (&pgPin,
                     (void *)&rbmt,
                     mdap,
                     undoRec.mcellId.page,
                     BS_NIL,
                     ftxH,
		     MF_VERIFY_PAGE);
    if (sts != EOK) {
        /* FIX - correct action? */
        ADVFS_SAD1("undo_cre_xtnt_rec: rbf_pinpg failed ", (long)BSERRMSG(sts));
    }

    mcp = &(rbmt->bsMCA[undoRec.mcellId.cell]);
    rec = (bsMRT *)&(mcp->bsMR0[0]);

    rbf_pin_record (
                    pgPin,
                    &mcp->mcTag,
                    (int)((char*)mcp->bsMR0 - (char*)&mcp->mcTag)
                    );

    mcp->mcTag         = NilBfTag;
    mcp->mcBfSetTag    = NilBfTag;

    rec->type = BSR_NIL;
    rec->bCnt = sizeof (bsMRT);

    return;

}  /* end undo_cre_xtnt_rec */

/*
 * init_bs_xtnts_opx
 *
 * This function registers the on-disk extent code as a transaction agent.
 */

void
init_bs_xtnts_opx ()
{
    statusT sts;

    sts = ftx_register_agent(
                             FTA_BS_XTNT_UPD_MCELL_CNT_V1,
                             &undo_mcell_cnt,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(0):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_UPD_REC_V1,
                             &undo_upd_xtnt_rec,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(1):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_CRE_REC,
			     &undo_cre_xtnt_rec,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(2):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_CRE_XTRA_REC_V1,
                             NULL,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(3):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_REWRITE_MAP_V1,
                             NULL,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(5):register failure", sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_XTNT_RELOAD_ORIG_XTNTMAP,
                             &undo_restore_orig_xtntmap,
                             NULL
                             );

    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_xtnts_opx(6):register failure", sts);
    }

    return;

} /* end init_bs_xtnts_opx */

/*
 * odm_remove_mcells_from_xtnt_map
 *
 * This function removes the specified mcells from the bitfile's extent
 * chain and adjusts the mcell count.  It is left to the caller to either
 * re-use the mcells or to deallocate them.
 *
 * The mcells to remove are the "origStart" + 1 mcell thru the "origEnd"
 * mcell.  The "origStart" mcell is needed to remove the first mcell.
 *
 * This function assumes that the caller has exclusive access to both
 * the in-mem and on-disk extent maps.
 * 
 * This function assumes that the file is not reserved.
 */

statusT
odm_remove_mcells_from_xtnt_map (
                                 domainT *domain,  /* in */
                                 bfTagT bfTag, /* in */
                                 bsInMemXtntMapT *xtntMap,  /* in */
                                 uint32_t start_index,
                                 ftxHT parentFtx  /* in */
                                 )
{
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    
    if (xtntMap->origEnd == xtntMap->origStart) {
        return EOK;
    }
    
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

    MS_SMP_ASSERT((subXtntMap->type == BSR_XTRA_XTNTS)||
                  (subXtntMap->type == BSR_XTNTS));
    
    MS_SMP_ASSERT( (subXtntMap->type != BSR_XTNTS ) || 
                   (start_index == 1) );
    
    if (start_index == 0)
    {
        MS_SMP_ASSERT(xtntMap->origStart > 0);
        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart-1]);
    }
    
    sts = bmt_unlink_mcells ( domain,
                              bfTag,
                              subXtntMap->mcellId,
                              xtntMap->subXtntMap[xtntMap->origStart + start_index].mcellId,
                              xtntMap->subXtntMap[xtntMap->origEnd].mcellId,
                              parentFtx,
			      CHAINMCID
        );
    if (sts != EOK) {
        return sts;
    }
        
    sts = update_mcell_cnt (
                            domain,
                            bfTag,
                            xtntMap->hdrMcellId,
                            xtntMap->hdrType,
                            xtntMap->origStart - xtntMap->origEnd + (start_index -1) ,
                            parentFtx
        );
    return sts;

}  /* end odm_remove_mcells_from_xtnt_map */

/*
 * odm_create_xtnt_map
 *
 * This function creates an on-disk extent map using the in-memory extent map
 * specified by the caller.
 *
 * This function assumes that the caller has shared or exclusive access to
 * the in-mem extent map.  In other words, this function assumes that the
 * in-mem extent map cannot change.
 */

statusT
odm_create_xtnt_map (
                     bfAccessT *bfap,  /* in */
                     bfSetT *bfSetp,  /* in */
                     bfTagT bfTag,  /* in */
                     bsInMemXtntMapT *xtntMap,  /* in */
                     ftxHT parentFtx,  /* in */
                     bfMCIdT *bfMcellId  /* out */
                     )
{
    domainT *domain;
    uint32_t i;
    bfMCIdT mcellId;
    bsInMemSubXtntMapT *nextSubXtntMap;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;

    if (BS_BFTAG_RSVD (bfTag)) {
        return ENOT_SUPPORTED;
    }
    domain = bfap->dmnP;

    /*
     * Link all the mcells together.
     */

    subXtntMap = &(xtntMap->subXtntMap[0]);
    for (i = 1; i < xtntMap->cnt; i++) {
        nextSubXtntMap = &(xtntMap->subXtntMap[i]);
        sts = bmt_link_mcells (
                               domain,
                               bfap->tag,
                               subXtntMap->mcellId,
                               nextSubXtntMap->mcellId,
                               nextSubXtntMap->mcellId,
                               parentFtx
                               );
        if (sts != EOK) {
            return sts;
        }
        subXtntMap = nextSubXtntMap;
    }  /* end while */

    /*
     * If there is extent information in the primary mcell, then there is
     * no need to create a new extent header record and mcell.  Just use 
     * the one we have.
     */
    if (xtntMap->subXtntMap[0].type == BSR_XTNTS) {
        *bfMcellId = xtntMap->subXtntMap[0].mcellId;
    }
    else {
        sts = create_xtnt_map_hdr(
                                  bfap,
                                  bfSetp,
                                  bfTag,
                                  xtntMap->subXtntMap[0].mcellId,
                                  xtntMap->subXtntMap[xtntMap->cnt - 1].mcellId,
                                  parentFtx,
                                  &mcellId
                                  );
        if (sts != EOK) {
            return sts;
        }

        *bfMcellId = mcellId;
    }

    return EOK;

}  /* end odm_create_xtnt_map */

/*
 * create_xtnt_map_hdr
 *
 * This function creates the extent map header and links the mcells onto it.
 */

statusT
create_xtnt_map_hdr (
                     bfAccessT *bfap,  /* in */
                     bfSetT *bfSetp,  /* in */
                     bfTagT bfTag,  /* in */
                     bfMCIdT firstMcellId,  /* in */
                     bfMCIdT lastMcellId,  /* in */
                     ftxHT parentFtx,  /* in */
                     bfMCIdT *bfMcellId  /* out */
                     )
{
    domainT *domain;
    bsInMemSubXtntMapT emptySubXtntMap;
    bfMCIdT primMcellId;
    statusT sts;
    vdT *vd;

    /*
     * Allocate an mcell and initialize a primary extent record.
     */

    domain = bfap->dmnP;
    vd = VD_HTOP(firstMcellId.volume, domain);
    sts = bmt_alloc_mcell (
                           bfap,
                           (vdIndexT) firstMcellId.volume, 
                           BMT_NORMAL_MCELL,
                           bfSetp->dirTag,
                           bfTag,
                           0,  /* linkSeg */
                           parentFtx,
                           &primMcellId,
                           TRUE
                           );
    if (sts == EOK) {
        MS_SMP_ASSERT(primMcellId.volume == firstMcellId.volume);
        sts = imm_init_sub_xtnt_map (
                                     &emptySubXtntMap,
                                     0L, /* fobOffset */
                                     0L, /* fobCnt */
                                     primMcellId,
                                     BSR_XTNTS,
                                     BMT_XTNTS,
                                     1
                                     );
        if (sts == EOK) {
            sts = odm_create_xtnt_rec (
                                       bfap,
                                       bfSetp,
                                       &emptySubXtntMap,
                                       parentFtx
                                       );
            imm_unload_sub_xtnt_map (&emptySubXtntMap);
            if (sts != EOK) {
                return sts;
            }
        } else {
            return sts;
        }
    } else {
        return sts;
    }
    /*
     * Link the first mcell the new primary extent record.
     */
    sts = bmt_link_mcells (
                           domain,
                           bfap->tag,
                           primMcellId,
                           firstMcellId,
                           lastMcellId,
                           parentFtx
                          );
    if (sts != EOK) {
        return sts;
    }

    *bfMcellId = primMcellId;

    return EOK;

}  /* end create_xtnt_map_hdr */

/*
 * x_update_ondisk_xtnt_map
 *
 * This function saves the modified contents of an in-memory extent map
 * in the corresponding on-disk extent map mcells.
 *
 * Before calling this function, the caller must set the in-memory extent
 * map's "updateStart" and "updateEnd" fields.  These fields specify which
 * in-memory sub extent map entries to save.
 *
 * Also, the caller must set each modified sub extent map's "updateStart"
 * and "updateEnd" fields.  These fields specify which extent descriptor
 * array entries to save.
 *
 * If updateStart's value is equal to the number of array entries, the sub
 * extent map was not modified and nothing is saved.  Typically, this type
 * of entry is followed by a new sub extent map.  The unmodified entry's
 * mcell is needed because it will point to the new entry's mcell.
 *
 * If a sub extent map entry is new and is not the first sub extent map, the
 * previous sub extent map entry must exist.  This is necessary so that the
 * previous entry's mcell can point to the new entry's mcell.
 *
 * The thread must synchronize access to the on-disk extent map.
 *
 * NOTE:
 * 
 * The subXtntMap->mcellState inidicates which actions to perform.
 *
 * INITIAILIZED - Mcell already linked properly, and the xtnt rec exists
 *                just update the on-disk xtnt rec.
 * NEW_MCELL - Create on-disk xtnt recorc and link together mcells.
 * 
 * USED_MCELL - The caller previously called bmt_unlink_mcells on this
 *              mcell!!!!! Update the existing xtnt record and link the
 *              mcell on-disk.
 */

statusT
x_update_ondisk_xtnt_map (
                          domainT *domain,  /* in */
                          bfAccessT *bfap,  /* in */
                          bsInMemXtntMapT *xtntMap,  /* in */
                          ftxHT parentFtx  /* in */
                          )
{
    uint32_t i;
    uint32_t mcellCnt = 0;
    statusT sts = EOK;
    bsInMemSubXtntMapT *subXtntMap;
    bfMCIdT prevMcellId;

    /*
     * Update the extent records.
     */

    for (i = xtntMap->updateStart; i <= xtntMap->updateEnd; i++) 
    {
        
        subXtntMap = &(xtntMap->subXtntMap[i]);

        if (subXtntMap->updateStart < subXtntMap->cnt) {

            if (subXtntMap->mcellState == ADV_MCS_NEW_MCELL)
            {

                /* This subxtnt has a brand new Mcell, we must create its
                 * on-disk xtnt rec.
                 */
                
                sts = odm_create_xtnt_rec (
                                           bfap,
                                           bfap->bfSetp,
                                           subXtntMap,
                                           parentFtx
                                          );
                if (sts != EOK) 
                {
                    return sts;
                }
                if (subXtntMap->type == BSR_XTRA_XTNTS) 
                    mcellCnt++;
            }
            else
            {
                /* This is not a new MCELL so just update the xtnt rec on-disk */

                sts = update_xtnt_rec (domain, bfap->tag, subXtntMap, parentFtx);
                if (sts != EOK) 
                {
                    return sts;
                }
            }
            if ((subXtntMap->mcellState == ADV_MCS_NEW_MCELL) ||
                (subXtntMap->mcellState == ADV_MCS_USED_MCELL))
            {
                subXtntMap->mcellState = ADV_MCS_INITIALIZED;
                    
                /* Test to see if the subxtnt we are updating is the
                 * first on the update list. If it is then we are creating
                 * the MCELL for it and must link it in at the origStart.
                 * otherwise just link the list to the previous one on the
                 * update list.
                 */

                if (i == xtntMap->updateStart) 
                {
                    prevMcellId = xtntMap->subXtntMap[xtntMap->origStart - 1].mcellId;
                }
                else
                {
                    prevMcellId = xtntMap->subXtntMap[i - 1].mcellId;
                }

                sts = bmt_link_mcells (
                                       domain,
                                       bfap->tag,
                                       prevMcellId,
                                       subXtntMap->mcellId,
                                       subXtntMap->mcellId,
                                       parentFtx
                                      );

                if (sts != EOK) {
                    return sts;
                }
            }
        }
    }  /* end for */

    if (mcellCnt > 0) {
        sts = update_mcell_cnt (
                                domain,
                                bfap->tag,
                                xtntMap->hdrMcellId,
                                xtntMap->hdrType,
                                mcellCnt,
                                parentFtx
                                );
    }

    return sts;

}  /* end x_update_ondisk_xtnt_map */


/* 
 * update_xtnt_rec
 *
 * This function updates the on-disk extent record by copying the
 * modified portion of the corresponding in-memory sub extent map
 * to the record.
 */

statusT
update_xtnt_rec ( 
                 domainT *domain,  /* in */
                 bfTagT bfTag, /* in */
                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                 ftxHT parentFtx  /* in */
                 )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint16_t *bsXACnt;
    int failFtxFlag = 0;
    ftxHT ftx;
    uint16_t i;
    bsMCT *mcell;
    bfMCIdT mcellId;
    rbfPgRefHT pgPin;
    statusT sts;
    updXtntRecUndoRecT undoRec;
    int undoRecSize;
    vdT *vd;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    struct bfAccess *mdap;

    mcellId = subXtntMap->mcellId;

    vd = VD_HTOP(mcellId.volume, domain);

    sts = FTX_START_META(FTA_BS_XTNT_UPD_REC_V1, &ftx, parentFtx, vd->dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg ( &pgPin, (void*)&bmt, mdap, mcellId.page, BS_NIL, ftx,
                      MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    mcell = &(bmt->bsMCA[mcellId.cell]);

    switch (subXtntMap->type) {

      case BSR_XTNTS:

        xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
        if (xtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        bsXACnt = &(xtntRec->xCnt);
        bsXA = xtntRec->bsXA;

        break;

      case BSR_XTRA_XTNTS:

        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        bsXACnt = &(xtraXtntRec->xCnt);
        bsXA = xtraXtntRec->bsXA;

        break;

      default:

        ADVFS_SAD0("update_xtnt_rec: bad extent record type");
        
    }  /* end switch */

    undoRec.type = subXtntMap->type;
    undoRec.mCId = mcellId;
    undoRec.bfTag = bfTag;
    undoRec.xCnt = *bsXACnt;
    undoRec.index = subXtntMap->updateStart;
    undoRec.cnt = (subXtntMap->updateEnd - subXtntMap->updateStart) + 1;

    MS_SMP_ASSERT(subXtntMap->updateEnd >= subXtntMap->updateStart);

    for (i = 0; i < undoRec.cnt; i++) {
        undoRec.bsXA.xtnt[i] = bsXA[undoRec.index + i];
    }  /* end for */

    undoRecSize = (char*)(&undoRec.bsXA.xtnt[undoRec.cnt]) - (char*)(&undoRec);

    update_xtnt_array (pgPin, subXtntMap, bsXACnt, bsXA);
    
    ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_172);
    ftx_done_u (ftx, FTA_BS_XTNT_UPD_REC_V1, undoRecSize, &undoRec);
    ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_173);

    return EOK;

HANDLE_EXCEPTION:

    if (failFtxFlag != 0) {
        ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_376);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_377);
    }
    return sts;

}  /* end update_xtnt_rec */

/*
 * update_xtnt_array
 *
 * This function updates an on-disk extent descriptor array by copying the
 * modified portion of the corresponding in-memory extent descriptor array
 * to the on-disk array.
 */

static
void
update_xtnt_array (
                   rbfPgRefHT pgPin,  /* in */
                   bsInMemSubXtntMapT *subXtntMap,  /* in */
                   uint16_t *bsXACnt,  /* in */
                   bsXtntT *bsXA  /* in */
                   )
{
    uint16_t i;
    uint16_t xtntCnt;

    /*
     * Save the modified portion of the in-memory extent descriptor array
     * in the on-disk extent descriptor array.
     */

    xtntCnt = (subXtntMap->updateEnd - subXtntMap->updateStart) + 1;
    rbf_pin_record( pgPin,
                    &(bsXA[subXtntMap->updateStart]),
                    sizeof (bsXtntT) * xtntCnt);

    for (i = subXtntMap->updateStart; i <= subXtntMap->updateEnd; i++) {

        bsXA[i] = subXtntMap->bsXA[i];
        ADVFS_CRASH_RECOVERY_TEST(pgPin.dmnP, AdvfsCrash_174);

    }  /* end for */

    rbf_pin_record (pgPin, bsXACnt, sizeof (*bsXACnt));
    *bsXACnt = subXtntMap->cnt;
    ADVFS_CRASH_RECOVERY_TEST(pgPin.dmnP, AdvfsCrash_175);

    return;

}  /* end update_xtnt_array */

/*
 * odm_create_xtnt_rec
 *
 * This function initializes an extent record in the sub extent map's
 * mcell and copies the extent descriptors from the in-memory map to
 * the record.
 */

statusT
odm_create_xtnt_rec (
                     bfAccessT *bfap,  /* in */
                     bfSetT *bfSetp,  /* in */
                     bsInMemSubXtntMapT *subXtntMap,  /* in */
                     ftxHT parentFtx  /* in */
                     )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint16_t *bsXACnt;
    ftxHT ftx;
    bsMCT *mcell;
    bfMCIdT mcellId;
    rbfPgRefHT pgPin;
    statusT sts;
    vdT *vd;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    bsBfAttrT *odattrp;
    struct bfAccess *mdap;
    creXtntRecUndoRecT undoRec;

    mcellId = subXtntMap->mcellId;

    vd = VD_HTOP(mcellId.volume, bfap->dmnP);

    sts = FTX_START_META(FTA_BS_XTNT_CRE_REC, &ftx, parentFtx, vd->dmnP);
    if (sts != EOK) {
        return sts;
    }

    if ( BS_BFTAG_RSVD(bfap->tag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg ( &pgPin, (void*)&bmt, mdap, mcellId.page, BS_NIL, ftx,
                     MF_VERIFY_PAGE);
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_378);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_379);
        return sts;
    }
    mcell = &(bmt->bsMCA[mcellId.cell]);

    switch (subXtntMap->type) {
      case BSR_XTNTS:
        odattrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), mcell);
        if (odattrp == 0) {
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_380);
            ftx_fail (ftx);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_381);
            return ENO_XTNTS;
        }

        /* pin mcell record */
        rbf_pin_record(pgPin, mcell->bsMR0, sizeof(mcell->bsMR0));

        /* set on-disk attributes */
        odattrp->state = BSRA_VALID;
        odattrp->bfPgSz = bfap->bfPageSz;

        xtntRec = bmtr_assign(BSR_XTNTS, sizeof(struct bsXtntR), mcell);
        if (xtntRec == 0) {
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_382);
            ftx_fail (ftx);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_383);
            return ENO_XTNTS;
        }

        xtntRec->chainMCId = bsNilMCId;
        xtntRec->rsvd1 = 0;
        xtntRec->rsvd2 = 0;
        bzero((char *)&xtntRec->delRst, sizeof(delRstT));

        /* 
         * No need to pin records; the above call to
         * rbf_pin_record() has pinned the entire mcell.
         */
        xtntRec->mcellCnt = 1;
        bsXACnt = &(xtntRec->xCnt);
        bsXA = &(xtntRec->bsXA[0]);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_176);
        break;

      case BSR_XTRA_XTNTS:
        xtraXtntRec = bmtr_assign_rec (
                                       BSR_XTRA_XTNTS,
                                       sizeof (bsXtraXtntRT),
                                       mcell,
                                       pgPin
                                       );
        if (xtraXtntRec == NULL) {
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_384);
            ftx_fail (ftx);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_385);
            return ENO_XTNTS;
        }

        bsXACnt = &(xtraXtntRec->xCnt);
        bsXA = &(xtraXtntRec->bsXA[0]);

        break;

      default:
        ADVFS_SAD0("odm_create_xtnt_rec: bad extent record type");
        
    }  /* end switch */

    subXtntMap->updateStart = 0;
    subXtntMap->updateEnd = subXtntMap->cnt - 1;
    update_xtnt_array (pgPin, subXtntMap, bsXACnt, bsXA);

    /*
     * Undo is not necessary for most mcells because we are 
     * initializing the first record in a new mcell.  When we 
     * undo the mcell allocation the record will be deleted.  
     * Undo is necessary for the RBMT_RSVD_CELL only.  The RBMT_RSVD_CELL
     * is pre-allocated and will not be freed.  Therefore its record will
     * not be cleared.  The undo will clear the record.
     */
                         
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_177);
    if ( BS_BFTAG_RSVD(bfap->tag) &&
         subXtntMap->mcellId.cell == RBMT_RSVD_CELL &&
         subXtntMap->type == BSR_XTRA_XTNTS ) {                         
         undoRec.mcellId = subXtntMap->mcellId;
         ftx_done_u (ftx, FTA_BS_XTNT_CRE_REC, sizeof(creXtntRecUndoRecT), &undoRec );
     } else {
         ftx_done_u (ftx, FTA_BS_XTNT_CRE_REC, 0, NULL );
     }
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_178);

    return EOK;

}  /* end odm_create_xtnt_rec */

/*
 * x_detach_extent_chain
 *
 * This function unlinks the on-disk extent records from a file's primary mcell.
 * 
 * This function returns the mcell pointer of the previous mcell that
 * points to the removed mcell.  If the first extent record
 * mcell is at the head of the extent chain, the previous mcell is
 * nil.
 *
 * The caller of this function must own the bitfile's mcell list lock
 * and free the mcell chain pointed to by retfreedVdIndex and
 * retfreedMcellId.
 */

statusT
x_detach_extent_chain (
                       bfAccessT *bfap,       /* in */
                       bsInMemXtntMapT *xtntMap,  /* in */
                       ftxHT parentFtxH,          /* in */
                       bfMCIdT *retPrevMcellId,   /* out */
                       bfMCIdT *retfreedMcellId   /* out */
                       )
{
    bsMPgT *bmt;
    bfMCIdT chainMcellId;
    bsMCT *mcell;
    bfPageRefHT pgRef;
    bfMCIdT prevMcellId;
    statusT sts;
    vdT *vd;
    bsXtntRT *xtntRec;
    bsXtntT tempXtnt[BMT_XTNTS];
    bsInMemSubXtntMapT tempSubXtnt;
    uint32_t starting_index;

    if (BS_BFTAG_RSVD (bfap->tag)) {
        return ENOT_SUPPORTED;
    }

    /*
     * Get the primary extent record's extent mcell chain pointer.
     */
    vd = VD_HTOP(bfap->primMCId.volume, bfap->dmnP);
    sts = bs_refpg(&pgRef,
                   (void*)&bmt,
                   vd->bmtp,
                   bfap->primMCId.page,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        return sts;
    }

    mcell = &(bmt->bsMCA[bfap->primMCId.cell]);
    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        bs_derefpg (pgRef, BS_CACHE_IT);
        return (ENO_XTNTS);
    }

    chainMcellId = xtntRec->chainMCId;

    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        return sts;
    }

    /*
     * Invalidate the extent information in the primary mcell.
     */
   
    /*
     * The semantics of this routine are to modify the on-disk mcell
     * chain but to leave the extent maps unchanged.  When the file
     * has a BSR_XTNTS record, we temporarily zap the in-memory
     * fields just so that update_xtnt_rec() does the right thing.
     * After that call, we put the extent map back to the way it
     * was in memory. We aren't protected so we need to use a
     * temporary extent map here.
     */
    tempSubXtnt = xtntMap->subXtntMap[0];
    tempSubXtnt.bsXA = &(tempXtnt[0]);

    tempSubXtnt.cnt = 1;
    tempSubXtnt.updateStart = 0;
    tempSubXtnt.updateEnd = 0;
    tempXtnt[0].bsx_fob_offset= 0;
    tempXtnt[0].bsx_vd_blk = XTNT_TERM;

    sts = update_xtnt_rec( bfap->dmnP,
                           bfap->tag,
                           &tempSubXtnt,
                           parentFtxH);
        
    /*
     * If there is no chain off the primary mcell, we're done.
     */
    if (MCID_EQL(chainMcellId, bsNilMCId)) {
        *retPrevMcellId = bsNilMCId;
        *retfreedMcellId = bsNilMCId;
        return (sts);
    }

    prevMcellId = bfap->primMCId;
    *retPrevMcellId = bsNilMCId;
    starting_index=1;
    sts = bmt_unlink_mcells ( bfap->dmnP,
                              bfap->tag,
                              prevMcellId,
                              xtntMap->subXtntMap[starting_index].mcellId,
                              xtntMap->subXtntMap[xtntMap->cnt - 1].mcellId,
                              parentFtxH,
                              CHAINMCID
                             );
    if (sts != EOK) {
        return sts;
    }

    *retfreedMcellId = xtntMap->subXtntMap[0].mcellId;

    return EOK;

}  /* end x_detach_extent_chain */

/*
 * update_mcell_cnt
 *
 * This function updates the "mcellCnt" field in an extent record.
 */

statusT
update_mcell_cnt (
                  domainT *domain,  /* in */
                  bfTagT bfTag, /* in */
                  bfMCIdT bfMcellId,  /* in */
                  uint32_t type,  /* in */
                  int32_t mcellCnt,  /* in */
                  ftxHT parentFtx  /* in */
                  )
{
    bsMPgT *bmt;
    ftxHT ftx;
    bsMCT *mcell;
    uint32_t *mcellCntAddr;
    rbfPgRefHT pgPin;
    statusT sts;
    mcellCntUndoRecT undoRec;
    vdT *vd;
    bsXtntRT *xtntRec;
    struct bfAccess *mdap;

    vd = VD_HTOP(bfMcellId.volume, domain);

    sts = FTX_START_META(FTA_BS_XTNT_UPD_MCELL_CNT_V1, &ftx, parentFtx, vd->dmnP);
    if (sts != EOK) {
        return sts;
    }

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = rbf_pinpg ( &pgPin, (void*)&bmt, mdap, bfMcellId.page, BS_NIL, ftx,
                      MF_VERIFY_PAGE);
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_386);
        ftx_fail (ftx);
        ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_387);
        return sts;
    }

    mcell = &(bmt->bsMCA[bfMcellId.cell]);

    switch (type) {

      case BSR_XTNTS:

        xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
        if (xtntRec == NULL) {
            ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_388);
            ftx_fail (ftx);
            ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_389);
            return (ENO_XTNTS);
        }
        mcellCntAddr = &(xtntRec->mcellCnt);

        break;

      default:

        ADVFS_SAD0("update_mcell_cnt: bad extent record type");

    }  /* end switch */

    undoRec.type = type;
    undoRec.mCId = bfMcellId;
    undoRec.bfTag = bfTag;
    undoRec.mcellCnt = *mcellCntAddr;

    rbf_pin_record (pgPin, mcellCntAddr, sizeof (*mcellCntAddr));
    *mcellCntAddr +=  mcellCnt;

    ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_179);
    ftx_done_u (ftx, FTA_BS_XTNT_UPD_MCELL_CNT_V1, sizeof (undoRec), &undoRec);
    ADVFS_CRASH_RECOVERY_TEST(vd->dmnP, AdvfsCrash_180);

    return EOK;

}  /* end update_mcell_cnt */


/*
 * x_create_inmem_xtnt_map
 *
 * This function creates the specified bitfile's in-memory extent map from
 * the bitfile's on-disk extent map.
 *
 * The caller must own the mcellList_lk lock and must exclusively own the
 * xtntMap_lk lock.  Or, have exclusive access to both the on-disk and
 * in-memory extent maps.
 */

statusT
x_create_inmem_xtnt_map (
                         bfAccessT *bfap,  /* in, modified */
                         bsMCT *bfMcellp   /* in */
                         )
{
    statusT     sts;
    bsXtntRT    *xtntRec;
    int32_t     free_record = FALSE;
    vdT         *vdp;
    bsInMemXtntMapT     *new_xtntMap;
    bsInMemXtntT        *xtnts = &bfap->xtnts;



    /* 
     * If this is a snapshot without it's own metadata, dummy up an 
     * xtntRec so we don't load the parents extents. 
     */
    if (bfap->bfaFlags & BFA_VIRGIN_SNAP) {
        xtntRec = (bsXtntRT*)ms_malloc( sizeof ( bsXtntRT ) );
        free_record = TRUE;
        xtntRec->chainMCId = bsNilMCId;
        xtntRec->mcellCnt = 1;
        xtntRec->xCnt = 1;
        xtntRec->rsvd1 = 0;
        xtntRec->rsvd2 = 0;
        xtntRec->bsXA[0].bsx_fob_offset = 0;
        xtntRec->bsXA[0].bsx_vd_blk = XTNT_TERM;
        xtntRec->bsXA[1].bsx_fob_offset = 0;
        xtntRec->bsXA[1].bsx_vd_blk = XTNT_TERM;
        bzero( (char *)&xtntRec->delRst, sizeof( delRstT) );
    } else {
        vdp = VD_HTOP(bfap->primMCId.volume, bfap->dmnP);
        xtntRec = bmtr_find (bfMcellp, BSR_XTNTS, vdp->dmnP);
    }
    if (xtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }


    sts = load_inmem_xtnt_map( bfap, 
                                xtntRec, 
                                &xtnts->bimxAllocFobCnt,
                                &new_xtntMap);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }


    ADVRWL_XTNTMAP_WRITE(&(bfap->xtntMap_lk));
    ADVRWL_VHAND_XTNTMAP_WRITE( &bfap->vhand_xtntMap_lk );

    /*
     * The old extent maps should have already been cleared
     */

    MS_SMP_ASSERT(xtnts->validFlag == XVT_INVALID);

    xtnts->xtntMap = new_xtntMap;
    xtnts->validFlag = XVT_VALID;
  
    bfap->bfaNextFob = imm_get_next_fob (xtnts);


    ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));

HANDLE_EXCEPTION:

    if (free_record) {
        ms_free( xtntRec );
    }

    return sts;

}  /* end x_create_inmem_xtnt_map */

/*
 * x_load_inmem_xtnt_map
 *
 * This function loads a bitfile's in-memory extent maps from its
 * on-disk extent maps. The maps will be returned in a temporary
 * extentmap. The caller must place them into the bfap and delete
 * the existing one if necessary.
 *
 * This function can modify the extent map pointers and the valid flag
 * in the extent map header.
 *
 * When called, this routine assumes that 
 * mcellListLk is WRITE locked
 */
static
statusT
x_load_inmem_xtnt_map (
                       bfAccessT *bfap,      /* in, modified */
                       bsInMemXtntMapT **new_xtntmap /* out */
                       )
{
    bsMPgT *bmt;
    int derefFlag = 0;
    bsMCT *mcell;
    bfPageRefHT pgRef;
    statusT sts;
    bf_fob_t total_fob_cnt;
    vdT *vd;
    bsXtntRT *xtntRec;
    struct bfAccess *mdap;
    domainT *domain = bfap->dmnP;
    bsInMemXtntT *xtnts = &bfap->xtnts;

    MS_SMP_ASSERT( new_xtntmap != NULL );
    /* We need to be holding the mcell lock for write or the mig storage
     * lock for read or write. 
     */
    vd = VD_HTOP(bfap->primMCId.volume, domain);

    if ( BS_BFTAG_RSVD(bfap->tag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = bs_refpg(&pgRef,
                   (void*)&bmt,
                   mdap,
                   bfap->primMCId.page,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefFlag = 1;

    mcell = &(bmt->bsMCA[bfap->primMCId.cell]);
    if ( check_mcell_hdr(mcell, bfap) != EOK ) {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }
    if ( CheckXtnts && check_BSR_XTNTS_rec(xtntRec, bfap, vd) != EOK ) {
        RAISE_EXCEPTION (E_BAD_BMT);
    }

    sts = load_inmem_xtnt_map( bfap, xtntRec, &total_fob_cnt, new_xtntmap);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    derefFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    xtnts->validFlag = XVT_VALID;

    return sts;

HANDLE_EXCEPTION:

    if (derefFlag != 0) {
        (void) bs_derefpg (pgRef, BS_CACHE_IT);  /* Ignore status */
    }
    return sts;

}  /* end x_load_inmem_xtnt_map */


/*
 * x_lock_inmem_xtnt_map
 *
 * When called, this routine assumes that no locks are held.
 * When this routine returns successfully, one of two locks will be
 * held depending on the value of xtnt_flags supplied:
 *  1. X_LOAD_REFERENCE  - xntnMap_lk is returned READ locked.
 *  2. X_LOAD_UPDATE     - mcellListLk is returned WRITE locked.
 *
 * In addition to the above flags, either XTNT_NO_WAIT or XTNT_WAIT_OK must
 * be passed in as part of xtnt_flags.
 *
 * The current use of the xtntMap_lk and mcellList_lk locks guarantee
 * that once this routine returns the extent maps in memory cannot be
 * modified by another thread until the calling routine drops the lock
 * that is returned locked.
 *
 * Callers should specify X_LOAD_REFERENCE if they need to reference, but not
 * modify the in-memory extent maps, and specify X_LOAD_UPDATE if they
 * intend to modify the extent maps.
 */

statusT
x_lock_inmem_xtnt_map (
                       bfAccessT *bfap,      /* in, modified */
                       xtntMapFlagsT xtnt_flags   /* in */
                       )
{
    MS_SMP_ASSERT( (xtnt_flags & X_LOAD_REFERENCE || 
                    xtnt_flags & X_LOAD_UPDATE) &&
                   (xtnt_flags & XTNT_WAIT_OK ||
                    xtnt_flags & XTNT_NO_WAIT) );

    if (xtnt_flags & X_LOAD_REFERENCE) {
        /* Check if we can wait.  If so, acquire the lock */
        if (xtnt_flags & XTNT_WAIT_OK) {
            ADVRWL_XTNTMAP_READ( &(bfap->xtntMap_lk) );
        } else {
            if ( ADVRWL_XTNTMAP_TRY_READ(&bfap->xtntMap_lk) != RWLCK_SUCCESS ) {
                return(E_WOULD_BLOCK);
            }
        }
    } else {/* xtnt_flags & X_LOAD_UPDATE */
        /* Check if we can wait.  If so, acquire the locks.
         */
        if (xtnt_flags & XTNT_WAIT_OK) {
            ADVRWL_MCELL_LIST_WRITE( &(bfap->mcellList_lk) );
        } else {
            /* Can't wait, do a lock try otherwise return
             * E_WOULD_BLOCK.
             */
            if ( ADVRWL_MCELL_LIST_TRY_WRITE( &bfap->mcellList_lk ) != RWLCK_SUCCESS) {
                return E_WOULD_BLOCK;
            }
        }
    }
    MS_SMP_ASSERT(bfap->xtnts.validFlag == XVT_VALID);
    return (EOK);

}  /* end x_lock_inmem_xtnt_map */


/*
 * load_inmem_xtnt_map
 *
 * This function loads a bitfile's in-memory extent maps from its on-disk
 * extent maps.
 *
 * This function does not modify any fields in the extent map header other than
 * the extent map pointers.
 *
 * The caller must own the mcellList_lk lock and must exclusively own the
 * xtntMap_lk lock.  Or, have exclusive access to both the on-disk and
 * in-memory extent maps. 
 *
 * This function initializes the sub extent map for the specified
 * extent record and optionally loads the extent descriptors into the
 * sub extent map.  If there are extra extent records, this function
 * calls load_from_xtra_xtnt_rec() for each record.
 */

static
statusT
load_inmem_xtnt_map (
                    bfAccessT *bfap,               /* in */
                    bsXtntRT *xtntRec,             /* in */
                    bf_fob_t *alloc_fob_cnt,       /* out */
                    bsInMemXtntMapT **new_xtntmap  /* out */
                    )
{
    uint32_t i;
    bfMCIdT mcellId;
    bfMCIdT nextMcellId;
    bf_fob_t fob_cnt; /* A count of file offset blocks */
    bf_fob_t fob_offset; /* Offset in 1k file offset blocks */
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bf_fob_t total_fob_cnt; /* a count of 1k file offset blks */
    bsInMemXtntMapT *xtntMap = NULL;
    bfAccessT *mdap;
    domainT *dmnP = bfap->dmnP;
    bfMCIdT bfMcellId = bfap->primMCId;
    vdT *vdp;
    bf_vd_blk_t next_vd_blk, vd_blk; 
    bf_fob_t last_fob; /* The file 1k block */

    /*
    ** We will believe it for now and get that many subextent maps, but
    ** later we will follow the mcell chain. If there are more mcells
    ** than mcellCnt we will allocate 64K more.
    ** Remember that if mcellCnt is 0 we still get 1 subextent map.
    */

    sts = imm_create_xtnt_map (
                               dmnP,
                               xtntRec->mcellCnt,
                               &xtntMap
                              );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtntMap->hdrType = BSR_XTNTS;
    xtntMap->hdrMcellId = bfMcellId;
    xtntMap->allocVdIndex = -1;

    MS_SMP_ASSERT(xtntMap->subXtntMap != NULL);
    subXtntMap = &(xtntMap->subXtntMap[0]);

    sts = imm_init_sub_xtnt_map (
                                 subXtntMap,
                                 0L,  /* fob_offset*/
                                 0L,  /* fob_cnt */
                                 bfMcellId,
                                 BSR_XTNTS,
                                 BMT_XTNTS,
                                 xtntRec->xCnt
                                 );
    if (sts == EOK) {
        sts = load_from_xtnt_array (
                                    xtntRec->xCnt,
                                    xtntRec->bsXA,
                                    subXtntMap
                                    );
        if (sts == EOK) {
            xtntMap->cnt = 1;
        } else {
            if (sts == ENO_XTNTS) {
                domain_panic (dmnP, "load_from_xtnt_array called from load_from_xtnt_rec: cnt must be non-zero");
                sts = E_DOMAIN_PANIC;
            }
            RAISE_EXCEPTION (sts);
        }
    } else {
        RAISE_EXCEPTION (sts);
    }

    nextMcellId = xtntRec->chainMCId;
    next_vd_blk = xtntRec->bsXA[0].bsx_vd_blk;

    /*
     * Extra extent records.
     */
    subXtntMap = xtntMap->subXtntMap;
    vdp = VD_HTOP( bfMcellId.volume, dmnP );
    mdap = vdp->rbmtp;

    if (mdap != NULL) {
        while ( nextMcellId.volume != bsNilVdIndex ) {
            if ( xtntMap->cnt >= xtntMap->maxCnt ) {
                domain_panic (dmnP, "has > 4G extent mcells ");
                sts = E_DOMAIN_PANIC;
                RAISE_EXCEPTION (sts);
            }

            last_fob = subXtntMap[xtntMap->cnt - 1].bssxmFobOffset +
                       subXtntMap[xtntMap->cnt - 1].bssxmFobCnt;
            mcellId = nextMcellId;
            sts = load_from_xtra_xtnt_rec( bfap,
                                           mcellId,
                                           &subXtntMap[xtntMap->cnt],
                                           last_fob,
                                           &nextMcellId);
            /* Check if mounting vd and opening BMT prior to doing
             * recovery.  This can happen if the next BMT page extents
             * have not been flushed to disk, but we crashed, and will
             * update the BMT extents during recovery (has not happened
             * yet since vdp->bmtp is still NULL or state is
             * BFD_RECOVER_REDO).
             */
            if ( sts == EOK          &&
                 nextMcellId.volume == 0    &&
                 (vdp->bmtp == NULL  ||
                  dmnP->state == BFD_RECOVER_REDO) ) {
                 xtntMap->cnt++;
                 break;
            } else if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            xtntMap->cnt++;
        }  /* end for */
    } else {  /* mdap == NULL */
        /*
         * If there is no BMT yet, this must be a reserved file.
         * Note that load_from_bmt_xtra_xtnt_rec() makes the 
         * assumption that the BMT's extent records are in memory.
         */
        MS_SMP_ASSERT(BS_BFTAG_RSVD(bfap->tag));
        /*
         * This is only for the RBMT.
         * stg_add_rbmt_stg ensures that mcellCnt will not wrap for RBMT.
         */
        for (i = 1; i < xtntRec->mcellCnt; i++) 
        {
            mcellId = nextMcellId;
            vd_blk= next_vd_blk;

            sts = load_from_bmt_xtra_xtnt_rec (
                                               dmnP,
                                               mcellId,
                                               vd_blk,
                                               &(subXtntMap[xtntMap->cnt]),
                                               &nextMcellId,
                                               &next_vd_blk
                                              );

            /* If we are rebooting after a crash, but we have not
             * yet done recovery, then the last RBMT subextent
             * record may not have been flushed to disk, but will
             * be fixed by the recovery pass.  (This only happens
             * if the crash occurs very soon after the RBMT
             * extension has been committed.) In this case, we
             * need to continue to mount the domain [via
             * setup_vd()] with the RBMT extents that are
             * currently OK, and remap the RBMT extents in the
             * operational redo phase of recovery. This should
             * never happen on the first RBMT extension since that
             * update is limited to page 0.  */

            if ((sts == ENO_XTNTS) && 
                (i == (xtntRec->mcellCnt-1)) &&
                (mcellId.page != 0))
            {
                break;
            }
            else if (sts != EOK)
            {
                RAISE_EXCEPTION (sts);
            }
            xtntMap->cnt++;
        }  /* end for */
    }

    xtntMap->validCnt = xtntMap->cnt;
    xtntMap->updateStart = xtntMap->cnt;

    fob_offset = xtntMap->subXtntMap[0].bssxmFobOffset;
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
    fob_cnt = (subXtntMap->bssxmFobOffset + 
               subXtntMap->bssxmFobCnt) - fob_offset;

    sts = imm_get_alloc_fob_cnt (xtntMap, fob_offset, fob_cnt, &total_fob_cnt);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtntMap->bsxmNextFob = fob_offset + fob_cnt;
    MS_SMP_ASSERT(xtntMap != NULL);
    *new_xtntmap = xtntMap;
    *alloc_fob_cnt = total_fob_cnt;
    MS_SMP_ASSERT(imm_check_xtnt_map(*new_xtntmap) == EOK);
    return sts;

HANDLE_EXCEPTION:
    if (xtntMap != NULL) {
        imm_delete_xtnt_map (xtntMap);
    }

    return sts;

}  /* end load_from_xtnt_rec */


/*
 * load_from_xtra_xtnt_rec
 *
 * This function initializes the sub extent map for the specified
 * extra extent record and optionally loads the extent descriptors into
 * the in-memory extent mcell.
 */

static
statusT
load_from_xtra_xtnt_rec (
                         bfAccessT *bfap,                  /* in */
                         bfMCIdT mcellId,                  /* in */
                         bsInMemSubXtntMapT *subXtntMap,   /* in */
                         bf_fob_t last_fob,    /* in */
                         bfMCIdT *nextMcellId              /* out */
                         )
{
    bsMPgT *bmtp;
    int derefPgFlag = 0;
    bsMCT *mcellp;
    bsXtraXtntRT *xtraXtntRec;
    bfPageRefHT pgRef;
    statusT sts;
    vdT *vdp;
    struct bfAccess *mdap;
    domainT *dmnP = bfap->dmnP;

       
    vdp = VD_HTOP(mcellId.volume, dmnP);
    if ( BS_BFTAG_RSVD(bfap->tag) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }

    MS_SMP_ASSERT(TEST_META_PAGE(mcellId.page, mdap) == EOK);
    sts = bs_refpg(&pgRef,
                   (void*)&bmtp,
                   mdap,
                   mcellId.page,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        return sts;
    }
    derefPgFlag = 1;

    MS_SMP_ASSERT(TEST_CELL(mcellId.cell) == EOK);
    mcellp = &(bmtp->bsMCA[mcellId.cell]);
    if ( check_mcell_hdr(mcellp, bfap) != EOK ) {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    xtraXtntRec = bmtr_find (mcellp, BSR_XTRA_XTNTS, vdp->dmnP);
    if (xtraXtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }
    if ( CheckXtnts &&
         check_BSR_XTRA_XTNTS_rec(xtraXtntRec, bfap, vdp, last_fob) != EOK )
    {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    sts = imm_init_sub_xtnt_map (
                                 subXtntMap,
                                 0L,  /* fobOffset */
                                 0L,  /* fobCnt */
                                 mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 xtraXtntRec->xCnt
                                 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = load_from_xtnt_array (
                                xtraXtntRec->xCnt,
                                xtraXtntRec->bsXA,
                                subXtntMap
                                );
    if (sts != EOK) {
        if (sts == ENO_XTNTS) {
            domain_panic (dmnP, "load_from_xtnt_array called from load_from_xtra_xtnt_rec: cnt must be non-zero");
            sts = E_DOMAIN_PANIC;
        }
        RAISE_EXCEPTION (sts);
    }

    *nextMcellId = mcellp->mcNextMCId;

    sts = bs_derefpg (pgRef, BS_CACHE_IT);

    return sts;

HANDLE_EXCEPTION:
    if (derefPgFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* end load_from_xtra_xtnt_rec */

/*
 * load_from_bmt_xtra_xtnt_rec
 *
 * This function initializes the sub extent map for the specified
 * extra extent record and optionally loads the extent descriptors into
 * the in-memory extent mcell.
 *
 * NOTE:  The bmt's on-disk extra extent records are assumed to be in memory.
 */

static
statusT
load_from_bmt_xtra_xtnt_rec (
                             domainT *dmnP,  /* in */
                             bfMCIdT mcellId,  /* in */
                             bf_vd_blk_t vd_blk,
                             bsInMemSubXtntMapT *subXtntMap,  /* in */
                             bfMCIdT *nextMcellId,  /* out */
                             bf_vd_blk_t *next_vd_blk
                             )
{
    bsMPgT *bmtp;
    bsMPgT *rbmtPg=NULL;
    bsMCT *mcellp;
    statusT sts;
    int xtntCnt;
    bsXtraXtntRT *xtraXtntRec;
    vdT *vdp;


    /* Page zero for the rbmt is already in memory so just grab it from there
     */

    vdp = VD_HTOP(mcellId.volume, dmnP);

    if (mcellId.page == 0) 
    {
        bmtp = dmnP->metaPagep;
        mcellp = &(bmtp->bsMCA[mcellId.cell]);
    }
    else
    {
        rbmtPg = (struct bsMPg *)ms_malloc_no_bzero(sizeof(struct bsMPg));

        /* Raw read the bmt page. */
        if (sts = advfs_raw_io( vdp->devVp, 
                                vd_blk, 
                                sizeof(struct bsMPg) / ADVFS_FOB_SZ /
                                 ADVFS_FOBS_PER_DEV_BSIZE,
                                RAW_READ,
                                rbmtPg)) 
        {
            RAISE_EXCEPTION (sts);
        }
        mcellp = &(rbmtPg->bsMCA[mcellId.cell]);
    }

    xtraXtntRec = bmtr_find (mcellp, BSR_XTRA_XTNTS, vdp->dmnP);
    if (xtraXtntRec == NULL) 
    {
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    sts = imm_init_sub_xtnt_map (
                                 subXtntMap,
                                 0L,  /* fobOffset */
                                 0L,  /* fobCnt */
                                 mcellId,
                                 BSR_XTRA_XTNTS,
                                 BMT_XTRA_XTNTS,
                                 xtraXtntRec->xCnt
                                 );
    if (sts != EOK) 
    {
        RAISE_EXCEPTION (sts);
    }

    sts = load_from_xtnt_array (
                                xtraXtntRec->xCnt,
                                xtraXtntRec->bsXA,
                                subXtntMap
                                );
    if (sts != EOK) 
    {
        if (sts == ENO_XTNTS)
        {
            domain_panic (dmnP, "load_from_xtnt_array called from load_from_bmt_xtra_xtnt_rec: cnt must be non-zero");
            sts = E_DOMAIN_PANIC;
        }
        RAISE_EXCEPTION (sts);
    }

    if (mcellp->mcNextMCId.volume != bsNilVdIndex) {
        if (mcellp->mcNextMCId.volume != mcellId.volume) {
            ADVFS_SAD0("load_from_bmt_xtra_xtnt_rec: bad mcellp->nextMCId.volume");
        }
    }

    *nextMcellId = mcellp->mcNextMCId;
    *next_vd_blk = xtraXtntRec->bsXA[0].bsx_vd_blk; /* Next Page of the RBMT */

HANDLE_EXCEPTION:

    if (rbmtPg != NULL) 
    {
        ms_free(rbmtPg);
    }

    return sts;
}  /* end load_from_bmt_xtra_xtnt_rec */

/*
 * load_from_xtnt_array
 *
 * This function copies entries from the on-disk extent descriptor
 * array to the in-memory extent mcell.  The on-disk descriptors
 * are ordered in ascending page order.
 *
 * NOTE: This routine is called to load into a temporary extentmap.
 * It is ultimately called when loading in the extent maps from disk
 * which now only load into a temporary map that is not visible to
 * other threads and thus requires no extent map locking.
 */

static
statusT
load_from_xtnt_array (
                      int cnt,  /* in */
                      bsXtntT *bsXA,  /* in */
                      bsInMemSubXtntMapT *subXtntMap  /* in */
                      )
{
    uint32_t inMem;
    uint32_t onDisk;
    statusT sts;

    /*
     * Check to see if cnt > 0.  If it is not, we return ENO_XTNTS so that
     * the callers of load_from_xtnt_array() can domain_panic.
     *
     * When file panics are supported, this domain_panic should be converted
     * into a file panic.
     */
    if (!cnt) {
        return ENO_XTNTS;
    }

    inMem = 0;
    for (onDisk = 0; onDisk < cnt; onDisk++) {
        if (inMem >= subXtntMap->maxCnt) {
            sts = imm_extend_sub_xtnt_map (subXtntMap);
            if (sts != EOK) {
                return sts;
            }
        }
        subXtntMap->bsXA[inMem] = bsXA[onDisk];
        inMem++;
    }  /* end for */

    subXtntMap->cnt = inMem;
    subXtntMap->updateStart = subXtntMap->cnt;

    subXtntMap->bssxmFobOffset = subXtntMap->bsXA[0].bsx_fob_offset;
    subXtntMap->bssxmFobCnt = 
        subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset -
        subXtntMap->bssxmFobOffset;

    return EOK;

}  /* end load_from_xtnt_array */


#ifdef ADVFS_DEBUG
/*
 * odm_check_xtnt_map
 *
 * This functions checks the specified on-disk extent map.  If something is
 * weird, SADs all around.
 */

statusT
odm_check_xtnt_map (
                    domainT *domain,            /* in */
                    bfTagT bfTag,               /* in */
                    vdIndexT mcellVdIndex,      /* in */
                    bs_meta_page_t mcellPage,   /* in */
                    uint32_t mcellCell          /* in */
                    )
{
    bsMPgT *bmt;
    int derefPageFlag = 0;
    uint32_t j;
    bsMCT *mcell;
    bfMCIdT mcellId;
    bfPageRefHT pgRef;
    bf_fob_t prev_fob; /* Used to make sure extents are increasing */
    statusT sts;
    vdT *vd;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    bfAccessT *mdap;

    vd = VD_HTOP(mcellVdIndex, domain);

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = bs_refpg(&pgRef,
                   (void*)&bmt,
                   mdap,
                   mcellPage,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellCell]);
    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        ADVFS_SAD0 ("odm_check_xtnt_map: no xtntRec");
    }

    /* Store off mcell page info */
    mcellId.volume = mcellVdIndex;
    mcellId.page = mcellPage;
    mcellId.cell = mcellCell;


    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ( MCID_EQL(mcellId, bsNilMCId) ) {
        RAISE_EXCEPTION (EOK);
    }

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = VD_HTOP(mcellId.volume, domain)->rbmtp;
    } else {
        mdap = VD_HTOP(mcellId.volume, domain)->bmtp;
    }

    sts = bs_refpg(&pgRef,
                  (void*)&bmt,
                  mdap,
                  mcellId.page,
                  FtxNilFtxH,
                  MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellId.cell]);
    xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
    if (xtraXtntRec != NULL) {
        
        prev_fob = xtraXtntRec->bsXA[0].bsx_fob_offset;
        /* Verify extents make sense. This checks that bsx_fob_offset
         * is monotonically increasing */
        for (j = 1; j < xtraXtntRec->xCnt; j++) {
            if (xtraXtntRec->bsXA[j].bsx_fob_offset <= prev_fob) {
                ADVFS_SAD0 ("odm_check_xtnt_map: bad xtraXtntRec bsx_fob_offset");
            }
            prev_fob = xtraXtntRec->bsXA[j].bsx_fob_offset;
        }  /* end for */
    } else {
        xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
        /* Verify that the extents in the primary mcell are monotonically
         * increasing */
        if (xtntRec != NULL) {
            if (xtntRec->bsXA[1].bsx_fob_offset<=
                xtntRec->bsXA[0].bsx_fob_offset) {
                ADVFS_SAD0 ("odm_check_xtnt_map: bad xtntRec bsx_fob_offset");
            }
        }
        else {
            ADVFS_SAD0 ("odm_check_xtnt_map: no xtnt or xtra rec");
        }
    }


    mcellId = xtntRec->chainMCId;
    

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    while ( !MCID_EQL(mcellId, bsNilMCId) ) {

        if ( BS_BFTAG_RSVD(bfTag) ) {
            mdap = VD_HTOP(mcellId.volume, domain)->rbmtp;
        } else {
            mdap = VD_HTOP(mcellId.volume, domain)->bmtp;
        }
        sts = bs_refpg(&pgRef,
                       (void*)&bmt,
                       mdap,
                       mcellId.page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        derefPageFlag = 1;

        mcell = &(bmt->bsMCA[mcellId.cell]);
        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec == NULL) {
            ADVFS_SAD0 ("odm_check_xtnt_map: no xtraXtntRec");
        }

        prev_fob = xtraXtntRec->bsXA[0].bsx_fob_offset;
        for (j = 1; j < xtraXtntRec->xCnt; j++) {
            /* verify that bsx_fob_offset is increasing through the
             * extents */
            if ( xtraXtntRec->bsXA[j].bsx_fob_offset <= prev_fob ) {
                ADVFS_SAD0 ("odm_check_xtnt_map: bad xtraXtntRec bsx_fob_offset");
            }
            prev_fob = xtraXtntRec->bsXA[j].bsx_fob_offset;
        }  /* end for */

        mcellId = mcell->mcNextMCId;

        derefPageFlag = 0;
        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

    }  /* end while */

    return EOK;

HANDLE_EXCEPTION:
    if (derefPageFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* end odm_check_xtnt_map */

/*
 * odm_print_xtnt_map
 *
 * This function prints the specified on-disk extent map.
 *
 * The logic in this function is strange.  Verify it before using it
 * anywhere.  It appears to read the same mcell twice before printing the
 * header info.
 */

statusT
odm_print_xtnt_map (
                    domainT *domain,            /* in */
                    bfTagT bfTag,               /* in */
                    vdIndexT mcellVdIndex,      /* in */
                    bs_meta_page_t mcellPage,   /* in */
                    uint32_t mcellCell          /* in */
                    )
{
    bsMPgT *bmt;
    int derefPageFlag = 0;
    uint32_t j;
    bsMCT *mcell;
    bfMCIdT mcellId;
    bfPageRefHT pgRef;
    statusT sts;
    vdT *vd;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;
    bfAccessT *mdap;

    vd = VD_HTOP(mcellVdIndex, domain);

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = vd->rbmtp;
    } else {
        mdap = vd->bmtp;
    }

    sts = bs_refpg(&pgRef,
                   (void*)&bmt,
                   mdap,
                   mcellPage,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellCell]);
    xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
    if (xtntRec == NULL) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    /* Get the next mcell in the chain */
    mcellId.volume = mcellVdIndex;
    mcellId.page = mcellPage;
    mcellId.cell = mcellCell;
    

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ( MCID_EQL(mcellId, bsNilMCId) ) {
        RAISE_EXCEPTION (EOK);
    }

    if ( BS_BFTAG_RSVD(bfTag) ) {
        mdap = VD_HTOP(mcellId.volume, domain)->rbmtp;
    } else {
        mdap = VD_HTOP(mcellId.volume, domain)->bmtp;
    }
    sts = bs_refpg(&pgRef,
                   (void*)&bmt,
                   mdap,
                   mcellId.page,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefPageFlag = 1;

    mcell = &(bmt->bsMCA[mcellId.cell]);
    /* Process this mcell */ 
    xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
    if (xtraXtntRec != NULL) {

        /* Print mcell header info */
        printf ("xtraXtntRec - volume: %d, mcellId (page.cell): %d.%ld\n",
                mcellId.volume, mcellId.page, mcellId.cell);
       
        /* Print xtnts in the mcell */
        for (j = 0; j < xtraXtntRec->xCnt; j++) {
            printf ("    bsx_fob_offset: %d (0x%08x), bsx_vd_blk: %d (0x%08x)\n",
                    xtraXtntRec->bsXA[j].bsx_fob_offset, 
                    xtraXtntRec->bsXA[j].bsx_fob_offset,
                    xtraXtntRec->bsXA[j].bsx_vd_blk, 
                    xtraXtntRec->bsXA[j].bsx_vd_blk);

        }  /* end for */
        
    } else {
        xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
        if (xtntRec != NULL) {
            /* Print primary mcell header */
            printf ("xtntRec - volume: %d, mcellId (page.cell): %d.%ld\n",
                mcellId.volume, mcellId.page, mcellId.cell);
            /* Print primary mcell extents--max 2 */
            printf ("    bsx_fob_offset: %d (0x%08x), bsx_vd_blk: %d (0x%08x)\n",
                    xtntRec->bsXA[0].bsx_fob_offset, 
                    xtntRec->bsXA[0].bsx_fob_offset,
                    xtntRec->bsXA[0].bsx_vd_blk,
                    xtntRec->bsXA[0].bsx_vd_blk);
            printf ("    bsx_fob_offset: %d (0x%08x), bsx_vd_blk: %d (0x%08x)\n",
                    xtntRec->bsXA[1].bsx_fob_offset, 
                    xtntRec->bsXA[1].bsx_fob_offset,
                    xtntRec->bsXA[1].bsx_vd_blk,
                    xtntRec->bsXA[1].bsx_vd_blk);
        } else {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
    }


    mcellId = xtntRec->chainMCId;

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    while ( !MCID_EQL(mcellId, bsNilMCId) ) {

        if ( BS_BFTAG_RSVD(bfTag) ) {
            mdap = VD_HTOP(mcellId.volume, domain)->rbmtp;
        } else {
            mdap = VD_HTOP(mcellId.volume, domain)->bmtp;
        }
        sts = bs_refpg(&pgRef,
                       (void*)&bmt,
                       mdap,
                       mcellId.page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        derefPageFlag = 1;

        mcell = &(bmt->bsMCA[mcellId.cell]);
        xtraXtntRec = bmtr_find (mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }

        /* Print mcell info */
        printf ("xtraXtntRec - volume: %d, mcellId (page.cell): %d.%ld\n",
                mcellId.volume, mcellId.page, mcellId.cell);

        /* Print xtnts records */
        for (j = 0; j < xtraXtntRec->xCnt; j++) {

            printf ("    bsx_fob_offset: %d (0x%08x), bsx_vd_blk: %d (0x%08x)\n",
                    xtraXtntRec->bsXA[j].bsx_file_offset_file, 
                    xtraXtntRec->bsXA[j].bsx_fob_offset,
                    xtraXtntRec->bsXA[j].bsx_vd_blk, 
                    xtraXtntRec->bsXA[j].bsx_vd_blk);

        }  /* end for */

        mcellId = mcell->mcNextMCId;

        derefPageFlag = 0;
        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

    }  /* end while */

    return EOK;

HANDLE_EXCEPTION:
    if (derefPageFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* end odm_print_xtnt_map */

#endif /* ADVFS_DEBUG */

/*
 * Deallocate all extent map structures for a
 * given in-memory extent map.
 */
void
x_dealloc_extent_maps(bsInMemXtntT *xtnts) 
{
    bsInMemXtntMapT       *xtntMap;

    if (xtnts->validFlag != XVT_INVALID) {
        if (xtnts->xtntMap != NULL) {
            imm_delete_xtnt_map (xtnts->xtntMap);
            xtnts->xtntMap = NULL;
        }
        if (xtnts->copyXtntMap != NULL) {
            xtntMap = xtnts->copyXtntMap;
            imm_delete_xtnt_map (xtntMap);
            xtnts->copyXtntMap = NULL;
        }
        xtnts->validFlag = XVT_INVALID;
    }

}

/*
 * advfs_bs_is_offset_mapped()
 *
 * Check if the specified file offset is mapped. 
 * If it is a sparse hole, return FALSE. 
 * Otherwise return TRUE.
 *
 * Unless the caller is holding the file lock, the extent maps may change
 * after calling advfs_get_blkmap_in_range().
 */
int32_t 
advfs_bs_is_offset_mapped(
    bfAccessT *bfap,    /* in - file's access structure */
    off_t      offset   /* in - file offset */
    )
{
    extent_blk_desc_t *extent_blk_head;
    statusT            status;
    uint64_t           not_mapped; /* returned from
                                    * advfs_get_blkmap_in_range */

    extent_blk_head = NULL;

    /* 
     * If the extent map is not valid, don't bother 
     */
    if (bfap->xtnts.validFlag != XVT_VALID) {
        /*
         * NOT sure if this is a valid assert.  If the maps have not been
         * loaded yet, but there really is a map, then we might return that
         * there is a hole here when in reality there is storage that should
         * be handled.  If a situation comes up where this assumption is
         * wrong, then remove the assert, but handle the case of unknown
         * storage.
         */
        MS_SMP_ASSERT(0);
        return FALSE;
    }

    status = advfs_get_blkmap_in_range(
        bfap, 
        bfap->xtnts.xtntMap,
        &offset,
        1,                        /* See if one byte at offset is mapped.
                                   * That is sufficient to know if the page
                                   * is mapped */
        &extent_blk_head,
        &not_mapped,              /* If set, this offset needs storage, so
                                   * it's not mapped. */
        RND_VM_PAGE,              /* Just round to 4k.  The entire
                                   * allocation unit will be mapped or
                                   * unmapped the same */
        EXB_ONLY_HOLES,           /* We will just ask for holes. This will
                                   * cause the not_mapped flag to be set if
                                   * storage is needed which means the page
                                   * is not mapped */
        XTNT_WAIT_OK|XTNT_NO_MAPS /* We really don't care what the mappings
                                   * are, as long as we know if it's mapped
                                   * or not */
                             );
    if ((status != EOK) || not_mapped) {
        /* 
         * We would need storage for this, so it's a sparse hole 
         */
        return FALSE;
    } else {
        return TRUE;
    }
}


/*
 * BEGIN_IMS
 *
 * advfs_get_blkmap_in_range - This function serves as the primary means of
 * getting sparseness or blockmap information for a file.  The file will
 * walk extent maps and generate a linked list of extent_blk_desc_t
 * structures each of which describes a contiguous region (extent) for the
 * file and provides the starting disk block for the range along with the
 * length of the range.
 *
 * Parameters:
 *      IN      bfap - bfAccess of structure whose block maps are required.
 *      IN      xtnt_map - xtnt maps for the access structure to be mapped.  
 *                         Could be either the extent maps or copy extent
 *                         maps for a file.
 *      IN/OUT  offset - on input, the desired starting offset for the range
 *                       to be mapped.  
 *                       on output, the rounded, actual starting offset of
 *                       the range that was mapped.
 *      IN      length - length in bytes of the range to be mapped.  The
 *                       actual length of the sub of the extent_blk_desc_t's
 *                       returned may be greater than the length parameter
 *                       due to rounding.
 *      IN/OUT  **extent_blk_desc - A pointer to a pointer to the head of a
 *                      singly linked list of extent_blk_desc_t structures.
 *                      On return, this will point to the head of a
 *                      malloc'ed list.  This memory should be freed by
 *                      calling advfs_free_blkmaps.
 *      IN/OUT  *xtnt_count - If *xtnt_count is non-null, it has one of two
 *                      meanings.  If EXB_ONLY_HOLES is requested as the
 *                      block map type, then this value will be the number
 *                      of holes found.  If the XTNT_NO_MAPS flag is set and
 *                      EXB_ONLY_HOLES then this will be TRUE on return and
 *                      a fast return will occur as soon as the first hole
 *                      is encounters.  If EXB_ONLY_STG or EXB_COMPLETE is
 *                      requested, this will be the number of extents
 *                      in the range that meet the criertia (stg or holes
 *                      and stg).  XTNT_NO_MAPS has no affect in this case
 *                      on the value, only on the maps returned.
 *      IN round_type   The type of rounding to be done. See round_type_t
 *                      definition for a description of rounding options.
 *      IN extent_blk_map_type  The type of extents to be described in link
 *                      list. See extent_blk_map_type_t for description of
 *                      options.
 *      IN blkmap_flags Flags for function.  Flags are in the set of 
 *                      { XTNT_LOCKS_HELD, XTNT_WAIT_OK, XTNT_NO_WAIT }       
 *
 * Return Values:
 *      0 - Success
 *      E_NO_MEM - Linked list could not be malloced.
 *      E_WOULD_BLOCK - XTNT_NO_WAIT was passed and function must block.
 *      EBAD_PARAMS - Parameters were invalid
 *      E_OFFSET_NOT_BLK_ALIGNED - RND_NONE was passed in as an option but
 *                    offset was not aligned on a DEV_BSIZE.
 *      EIO - x_load_inmem_xtnt_map returned an IO error reading the BMT.
 *
 *
 *
 * Entry/Exit Conditions:
 *      On entry, blkmap_flags has either XTNT_WAIT_OK or XTNT_NO_WAIT set.
 *      If the xtntMap_lk is already held of the bfap given, then
 *              XTNT_LOCKS_HELD must be passed in.  
 *      If the xtntMap_lk is not already owned, then caller must guarentee
 *              the extent maps remain valid on return.  
 *      The thread may sleep while doing IO to load the extent maps unless
 *              XTNT_NO_WAIT or XTNT_LOCKS_HELD is set.
 *      On error, all malloced memory is freed, all counts and extents are
 *              undefined.
 *     
 * Algorithm:
 * This function walks the extent maps and generates a linked list of
 * extent_blk_desc_t structures.  The function will malloc extent_blk_desc_t
 * stuctures as required, chaining them to the pointer passed in as the head
 * of the list.
 *
 * END_IMS
 */
statusT
advfs_get_blkmap_in_range (
        bfAccessT       *bfap,    /* IN - Access struct for file            */
        bsInMemXtntMapT *xtnt_map,/* IN - Extent map to use to              *
                                   * generate range maps                    */
        off_t           *offset,  /* IN - offset in file to start range map */
                                  /* OUT - offset adjusted to correct       * 
                                   * alignment                              */
        size_t          length,   /* IN - length of range to map            */
        extent_blk_desc_t **extent_blk_desc,    
                                /* IN - pointer to an extent_blk_desc       *
                                 * OUT - pointer to head of list that maps  *
                                 * the given range                          */
        uint64_t        *xtnt_count,   /* IN - a pointer or NULL            *
                                   * OUT - Overloaded meaning. See Above    */
        round_type_t    round_type, /* IN - type of rounding to be performed */
        extent_blk_map_type_t extent_blk_map_type,
                                /* IN - determines the map type. (sparse,   *
                                 * stg, both                                */ 
        xtntMapFlagsT   blkmap_flags   /* In - flags for the function       */
        )        

{
        off_t end_offset;       /* Offset of offset + length */
        bf_fob_t start_fob;     /* Fob of start of range to map */
        bf_fob_t end_fob;       /* Fob of end of range to map */
        bf_fob_t cur_fob = 0;   /* Temp fob offset */
        bf_fob_t adj_block = 0;
        bsInMemXtntDescIdT xtnt_desc_id;/* Id of xtnt_desc */
        bsXtntDescT xtnt_desc;          /* xtnt_desc pointer used  */
        extent_blk_desc_t *cur_range = NULL;  /* Pointer to range */
        extent_blk_desc_t *prev_range = NULL; /* Pointer to range */
        int32_t         error = 0;                      /* Error status */
        int32_t         unlock_xtntlock=0;
        int32_t         do_snap_xtnts = FALSE;
        bfAccessT*      source_bfap = NULL;

        statusT sts = 0;                /* Status return values */


        /* 
         * Assert that one of EXB_COMPLETE, EXB_ONLY_HOLES, or
         * EXB_ONLY_STG is set, then assert that ONLY one of the three is
         * set.  The assert is too long to be clear.  The ONLY part is left
         * out.
         */
        MS_SMP_ASSERT( (extent_blk_map_type & EXB_COMPLETE) ||
                       (extent_blk_map_type & EXB_ONLY_HOLES) ||
                       (extent_blk_map_type & EXB_ONLY_STG) );

        MS_SMP_ASSERT( length >= 0 );

        MS_SMP_ASSERT( (blkmap_flags & XTNT_WAIT_OK) ||
                       (blkmap_flags & XTNT_NO_WAIT) );
        
        /* Check for invalid parameter */
        if ( (xtnt_map == NULL) || (*offset < 0) || 
             (extent_blk_desc == NULL) || 
             ((blkmap_flags & XTNT_NO_MAPS) && (xtnt_count == NULL)) ) {
            return EBAD_PARAMS;
        }

        /* Check for blocking potential */
        if ((blkmap_flags & XTNT_NO_WAIT) && 
            (bfap->xtnts.validFlag != XVT_VALID)) {
            return E_WOULD_BLOCK;
        }

        /* If XTNT_LOCKS_HELD is set in the flags, then assert that the
         * extents are valid.  Otherwise, call x_load_inmem_xtnt_map to load
         * the extents and acquire the locks.  */
        if (blkmap_flags & XTNT_LOCKS_HELD) {
            MS_SMP_ASSERT(bfap->xtnts.validFlag == XVT_VALID);
#ifdef ADVFS_SMP_ASSERT
            /* Verify parent extents are also valid */
            {
                bfAccessT*      parent_bfap = bfap->bfaParentSnap;
                while (parent_bfap != NULL) {
                    MS_SMP_ASSERT( parent_bfap->xtnts.validFlag == XVT_VALID );
                    parent_bfap = parent_bfap->bfaParentSnap;
                }
            }
#endif
            if ( (bfap->bfaParentSnap != NULL) &&
                 !(extent_blk_map_type & EXB_DO_NOT_INHERIT) ) {
                do_snap_xtnts = TRUE;
            }
        } else {

            /* 
             * Appropriately lock the extent maps.  If EXB_DO_NOT_INHERIT is
             * set, or if there are no snap parents, then simply lock the
             * extent maps for bfap.  Otherwise, acquire the parent's locks.
             */
            if ( (extent_blk_map_type & EXB_DO_NOT_INHERIT) ||
                 (bfap->bfaParentSnap == NULL) ) {
                /* 
                 * Returns with the extent maps valid and locked for reading. 
                 * Implicitly respects the XTNT_NO_WAIT flag through
                 * blkmap_flags
                 */
                sts = x_lock_inmem_xtnt_map(bfap, X_LOAD_REFERENCE | blkmap_flags);
            } else {
                /* This is a child snapshot that needs to ref the parents */
                sts = advfs_acquire_xtntMap_locks( 
                            bfap,
                            X_LOAD_REFERENCE | blkmap_flags,
                            (extent_blk_map_type & EXB_DO_NOT_INHERIT) ? 
                                SF_LOCAL_BLKMAP : SF_SNAP_NOFLAGS 
                            );
                do_snap_xtnts = TRUE;
            }
            if (sts == E_WOULD_BLOCK) {
                /* 
                 * x_load_inmem_xtnt_map would need to block for IO or
                 * locking.  Assert we are in an XTNT_NO_WAIT case.
                 */
                MS_SMP_ASSERT(blkmap_flags & XTNT_NO_WAIT);
                return E_WOULD_BLOCK;
            } else if (sts != EOK) {
                return sts;
            }
            /*
             * Must unlock the extent maps, but do it the same way we locked
             * them (include the parents if they were locked )
             */
            unlock_xtntlock=1;
        }

        *extent_blk_desc = NULL;
        end_offset = *offset + length;

        start_fob = ADVFS_OFFSET_TO_FOB_DOWN(*offset);
        end_fob = ADVFS_OFFSET_TO_FOB_UP(end_offset);

        switch (round_type) {
            case RND_VM_PAGE:
                start_fob = ADVFS_ROUND_FOB_DOWN_TO_VM_PAGE(start_fob);
                break;
            case RND_MIGRATE:
            case RND_ALLOC_UNIT:
            case RND_ENTIRE_HOLE:
                /*
                 * If RND_ALLOC_UNIT was specified, we'll further round
                 * to an allocation unit if we find that the request 
                 * lands in a hole.
                 */
                if (bfap->bfPageSz * ADVFS_FOB_SZ >= VM_PAGE_SZ) {
                    start_fob = ADVFS_ROUND_FOB_DOWN_TO_VM_PAGE(start_fob);
                }
                else {
                    start_fob = 
                        ADVFS_ROUND_FOB_DOWN_TO_ALLOC_UNIT(bfap, start_fob);
                }
                break;
            case RND_NONE:
                /* Make sure offset is aligned on DEV_BSIZE boundary */
                if (*offset % DEV_BSIZE) {
                    if (unlock_xtntlock) {
                        if (do_snap_xtnts) {
                            advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS);
                        } else {
                            ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
                        }
                    }
                     return E_OFFSET_NOT_BLK_ALIGNED;
                }
                break;
            default:
                if (unlock_xtntlock) {
                    if (do_snap_xtnts) {
                        advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
                    } else {
                        ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
                    }
                }
                return EBAD_PARAMS;
        }

        source_bfap = bfap;
        cur_fob = start_fob;
        error = FALSE;
        cur_range = NULL;
        prev_range = NULL;

        /* 
         * Set *offset so we will return the target round value. If we
         * do a fast fail because XTNT_NO_MAP and EXB_ONLY_HOLES, then
         * this needs to be set already.
         */
        *offset = ADVFS_FOB_TO_OFFSET(start_fob);

        if (xtnt_count != NULL) {
            *xtnt_count = 0;
        }

        /* Get the xtnt descriptor for the range that start_fob falls in.
         * If the start_fob is past the last extent, E_RANGE_NOT_MAPPED will
         * be returned. */
        if (do_snap_xtnts) {
            sts = advfs_get_snap_xtnt_desc( start_fob,
                                        bfap, 
                                        &xtnt_desc_id,
                                        &xtnt_desc,
                                        &source_bfap);

        } else {
            sts = imm_get_xtnt_desc(start_fob, 
                              xtnt_map, 
                              FALSE, /* updateFlg */
                              &xtnt_desc_id,
                              &xtnt_desc);
        }
        if ( sts != E_RANGE_NOT_MAPPED) {
            /* We have a starting xtnt_desc to process.  If we didn't get
             * one, we will skip down to the end of this if and return it
             * as a hole */

            /*
             * Handle special case for RND_MIGRATE where we might need
             * to account for an adjacent leading hole, or a start_fob
             * within a hole.
             */
            if (round_type == RND_MIGRATE) {
                /* 
                 * This has not been modified to deal with snapshot extents.
                 */
                MS_SMP_ASSERT( do_snap_xtnts == FALSE );
                if ((blkmap_flags & XTNT_NO_MAPS)) {
                    adj_block = xtnt_desc.bsxdVdBlk;

                    if (xtnt_count != NULL) {
                        (*xtnt_count)++;
                    }
                } else {
                /*
                 * Check for adjacent leading hole.  A hole must
                 * be migrated with its trailing storage.
                 */
                    if (start_fob > 0 && start_fob == xtnt_desc.bsxdFobOffset) {
                         bsXtntDescT xtnt_desc2;           /* xtnt_desc pointer used  */
                         bsInMemXtntDescIdT xtnt_desc_id2; /* Id of xtnt_desc */

                                         if (imm_get_xtnt_desc(start_fob-1, 
                                               xtnt_map, 
                                               FALSE, /* updateFlg */
                                               &xtnt_desc_id2,
                                               &xtnt_desc2) != E_RANGE_NOT_MAPPED) {
                             /* check if preceding extent is a hole */
                             if (xtnt_desc2.bsxdVdBlk == XTNT_TERM ||
                                 xtnt_desc2.bsxdVdBlk == COWED_HOLE) {
                                 start_fob = xtnt_desc2.bsxdFobOffset;
                                 cur_fob = start_fob;
                                 bcopy(&xtnt_desc2, &xtnt_desc, sizeof(bsXtntDescT));
                                 bcopy(&xtnt_desc_id2, &xtnt_desc_id, sizeof(bsInMemXtntDescIdT));
                             }

                         }
                    }
                }
                /*
                 * start_fob is within a hole
                 */
                if ((xtnt_desc.bsxdVdBlk == XTNT_TERM ||
                     xtnt_desc.bsxdVdBlk == COWED_HOLE)) {
                    start_fob = xtnt_desc.bsxdFobOffset;
                    cur_fob   = start_fob;

                    if (end_fob < (xtnt_desc.bsxdFobOffset + xtnt_desc.bsxdFobCnt)) {
                        end_fob = xtnt_desc.bsxdFobOffset + xtnt_desc.bsxdFobCnt;
                    }

                }
            }

            /*
             * If we're rounding to allocation units and we are in a hole,
             * further round down start_fob to the allocation unit.
             */
            if ((round_type == RND_ALLOC_UNIT) &&
                ((xtnt_desc.bsxdVdBlk == XTNT_TERM) || 
                 (xtnt_desc.bsxdVdBlk == COWED_HOLE))) {
                cur_fob = start_fob = ADVFS_ROUND_FOB_DOWN_TO_ALLOC_UNIT(bfap, start_fob);
                *offset = ADVFS_FOB_TO_OFFSET(start_fob);
            }

            /* 
             * Deal with the RND_ENTIRE_HOLE case so that we correctly take
             * the entire hole extent.  For snapshots, it is necessary to be
             * able to COW an entire hole at once rather than doing it in
             * smaller pieces 
             */
            if ( ( round_type == RND_ENTIRE_HOLE ) &&
                 ( (xtnt_desc.bsxdVdBlk == XTNT_TERM)  ||
                   (xtnt_desc.bsxdVdBlk == COWED_HOLE) ) ) {
                cur_fob = start_fob = xtnt_desc.bsxdFobOffset;
                *offset = ADVFS_FOB_TO_OFFSET( start_fob );
            }

            /* Build block maps in range */
            do {
                /*
                 * For RND_MIGRATE we want to coalesce adjacent logical blocks
                 * into one block descriptor.
                 */
                if (round_type == RND_MIGRATE && prev_range && 
                    !ADVFS_HOLE(prev_range) &&
                    !(xtnt_desc.bsxdVdBlk == XTNT_TERM ||
                      xtnt_desc.bsxdVdBlk == COWED_HOLE) &&
                    !(blkmap_flags & XTNT_NO_MAPS)) {

                     cur_range->ebd_byte_cnt += ADVFS_FOB_TO_OFFSET(xtnt_desc.bsxdFobCnt);
                     cur_fob += xtnt_desc.bsxdFobCnt;

                /* Make sure that either we want both holes and stg extents,
                 * or we want holes only and this extent is a hole, or we
                 * want stg only and this extent is storage. */
                } else if ( (extent_blk_map_type & EXB_COMPLETE) ||
                     ( (extent_blk_map_type & EXB_ONLY_HOLES) && 
                       ( (xtnt_desc.bsxdVdBlk == XTNT_TERM) ||
                         (xtnt_desc.bsxdVdBlk == COWED_HOLE) ) ) ||
                     ( (extent_blk_map_type & EXB_ONLY_STG) &&
                       ( (xtnt_desc.bsxdVdBlk != XTNT_TERM) && 
                         (xtnt_desc.bsxdVdBlk != COWED_HOLE) ) ) ) {

                    if (!(blkmap_flags & XTNT_NO_MAPS)) {
                        /* Allocate an extent_blk_desc structure. */
                        if (blkmap_flags & XTNT_NO_WAIT) {
                            cur_range = (extent_blk_desc_t*)
                                ms_malloc_no_wait_or_bzero(
                                              sizeof(extent_blk_desc_t));
                            if (cur_range == NULL) {
                                /* 
                                 * EWOULD_BLOCK needs to be returned, but first,
                                 * free all the extent maps allocated so far.
                                 */
                                advfs_free_blkmaps(extent_blk_desc);
                                if (unlock_xtntlock) {
                                    if (do_snap_xtnts) {
                                        advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
                                    } else {
                                        ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
                                    }
                                }
                                return E_WOULD_BLOCK;
                            }
                        } else {
                            /* XTNT_WAIT_OK case. */
                            cur_range = (extent_blk_desc_t*)
                                ms_malloc_no_bzero(sizeof(extent_blk_desc_t));
                        }

                        /* Point passed in pointer to head of list */
                        if (*extent_blk_desc == NULL) {
                            *extent_blk_desc = cur_range;
                        } else {
                            /* Or link the new range to the last range */
                            prev_range->ebd_next_desc = cur_range;
                        }

                        prev_range = cur_range;
                    
                        /* Initialize the cur_range */
                        cur_range->ebd_offset = ADVFS_FOB_TO_OFFSET(cur_fob);
                        cur_range->ebd_snap_fwd = NULL;
                        cur_range->ebd_bfap = source_bfap;

                        /* 
                         * Assert that either we want to look at the merged
                         * extent maps of a snapshot, or that we don't want
                         * to (EXB_DO_NOT_INHERIT) and the extent we have
                         * comes from bfap 
                         */
                        MS_SMP_ASSERT( !(extent_blk_map_type & EXB_DO_NOT_INHERIT) ||
                                ( (extent_blk_map_type & EXB_DO_NOT_INHERIT) &&
                                  (source_bfap == bfap) ) );


                        /* Conditionally initialize remaining fields */
                        if ( ( (extent_blk_map_type & EXB_ONLY_HOLES) ||
                               (extent_blk_map_type & EXB_COMPLETE) ) &&
                             ( (xtnt_desc.bsxdVdBlk == XTNT_TERM) ||
                               (xtnt_desc.bsxdVdBlk == COWED_HOLE) ) ) {
                            /* If this is a hole extent and we want holes */
                            if (round_type == RND_ENTIRE_HOLE) {
                                cur_range->ebd_byte_cnt = 
                                    ADVFS_FOB_TO_OFFSET( xtnt_desc.bsxdFobOffset+
                                                         xtnt_desc.bsxdFobCnt -
                                                         cur_fob );
                            } else {
                                cur_range->ebd_byte_cnt = 
                                    min( ADVFS_FOB_TO_OFFSET(xtnt_desc.bsxdFobOffset+ 
                                                         xtnt_desc.bsxdFobCnt - 
                                                         cur_fob),
                                     ADVFS_FOB_TO_OFFSET(end_fob - cur_fob) );
                            }
                            cur_range->ebd_vd_index = 0;
                            /* Either XTNT_TERM or COWED_HOLE */
                            cur_range->ebd_vd_blk = xtnt_desc.bsxdVdBlk;
                        } else if ( ( (extent_blk_map_type & EXB_ONLY_STG) ||
                                      (extent_blk_map_type & EXB_COMPLETE) ) &&
                                    ( (xtnt_desc.bsxdVdBlk != XTNT_TERM) &&
                                      (xtnt_desc.bsxdVdBlk != COWED_HOLE) ) ) {
                            /* If this is an extent with storage and we want
                             * storage */
                            cur_range->ebd_byte_cnt = 
                                min( ADVFS_FOB_TO_OFFSET(xtnt_desc.bsxdFobOffset+ 
                                                         xtnt_desc.bsxdFobCnt - 
                                                         cur_fob),
                                     ADVFS_FOB_TO_OFFSET(end_fob - cur_fob) );
                            cur_range->ebd_vd_index = xtnt_desc.volIndex;
                            /* Offset the vd blk into the extent based on our
                             * starting point (cur_fob) */
                            cur_range->ebd_vd_blk = xtnt_desc.bsxdVdBlk +
                                ((cur_fob - xtnt_desc.bsxdFobOffset)/
                                  ADVFS_FOBS_PER_DEV_BSIZE);

                        } else {
                            /* We shouldn't get here */
                            MS_SMP_ASSERT(0);
                        }
                        /* We have finished this extent_blk_desc.  prev_range
                         * now points to the range we just initialized. So we
                         * just need to update cur_fob and we are ready for the
                         * next iteration of the loop */
                        MS_SMP_ASSERT( (cur_range->ebd_byte_cnt % ADVFS_FOB_SZ) == 0);
                        cur_fob += ADVFS_OFFSET_TO_FOB_DOWN(cur_range->ebd_byte_cnt);

                        /*
                         * We need to count this extent as being in the list 
                         */
                        if (xtnt_count != NULL) {
                            (*xtnt_count)++;
                        }
                    } else {
                        /* 
                         * We are asking for XTNT_NO_MAPS which means only
                         * an xtnt_count and round values are requested.  If
                         * This is EXB_ONLY_HOLES, return TRUE, otherwise
                         * bump xtnt_count and continue.
                         */
                        if (extent_blk_map_type & EXB_ONLY_HOLES) {
                            /* There should be no reason to pass NULL in
                             * with XTNT_NO_MAPS... but the code would
                             * support it. */
                            MS_SMP_ASSERT(xtnt_count != NULL);
                            *xtnt_count = TRUE;
                            if (unlock_xtntlock) {
                                if (do_snap_xtnts) {
                                    advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
                                } else {
                                    ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
                                }
                            }
                            return EOK;
                        } else {
                            /* There should be no reason to pass NULL in
                             * with XTNT_NO_MAPS... but the code would
                             * support it. */
                            MS_SMP_ASSERT(xtnt_count != NULL);
                            if (round_type == RND_MIGRATE) {
                                /*
                                 * We don't want to increment the xtnt count, if there
                                 * is a hole in the middle of two extents which are
                                 * physically adjacent.  Note: this doesn't account
                                 * for block offsets that appear to be physically adjacent,
                                 * yet are on different volumes.  The liklihood that this
                                 * would occur seems extremely low.
                                 */
                                if (adj_block != xtnt_desc.bsxdVdBlk) {
                                    (*xtnt_count)++;
                                }
                                adj_block = xtnt_desc.bsxdVdBlk + xtnt_desc.bsxdFobCnt;
                                
                            } else {
                                (*xtnt_count)++;
                            }
                            /* 
                             * We also need to advance to the next extent,
                             * but can't use the extent_blk_desc_t info
                             * because we never generated it.
                             */
                            MS_SMP_ASSERT( ( (xtnt_desc.bsxdFobOffset+ 
                                              xtnt_desc.bsxdFobCnt) % bfap->bfPageSz) == 0 );
                            cur_fob = xtnt_desc.bsxdFobOffset+ xtnt_desc.bsxdFobCnt;

                        }
                    }
                }
                else {
                    /* We don't want this extent.  It is a hole and we only
                     * want storage or it is storage and we only want holes
                     */
                    MS_SMP_ASSERT( ( (xtnt_desc.bsxdFobOffset+ 
                                      xtnt_desc.bsxdFobCnt) % bfap->bfPageSz) == 0 );
                    cur_fob = xtnt_desc.bsxdFobOffset+ xtnt_desc.bsxdFobCnt;
                }

                /* 
                 * Get the next extent descriptor.  Either use the snapshot
                 * routines, or the direct extent map routines.
                 */
                if (do_snap_xtnts) {
                    sts = advfs_get_next_snap_xtnt_desc( bfap,
                                                        &xtnt_desc_id,
                                                        &xtnt_desc,
                                                        &source_bfap );
                } else {
                    sts = imm_get_next_xtnt_desc(xtnt_map, &xtnt_desc_id, &xtnt_desc);
                }
            } while ( (cur_fob < end_fob) && (sts == EOK) && (!error) );

        } /* END of imm_get_xtnt_desc if */

        
        /* We have processed all the extents that are mapped.  We may
         * have a request beyond the end of the file, or the mapped
         * extents.  Getpage requires these be returned as holes. */
        if ( (cur_fob < end_fob) &&
             ( (extent_blk_map_type & EXB_COMPLETE) ||
               (extent_blk_map_type & EXB_ONLY_HOLES) ) ) {

            if (!(blkmap_flags & XTNT_NO_MAPS)) {
                /* Allocate an extent_blk_desc structure.  Fix the
                 * M_WAITOK to support the no wait flag */
                if (blkmap_flags & XTNT_NO_WAIT) {
                    cur_range = (extent_blk_desc_t*)
                        ms_malloc_no_wait_or_bzero(sizeof(extent_blk_desc_t));
                    if (cur_range == NULL) {
                        /* 
                         * EWOULD_BLOCK needs to be returned, but first,
                         * free all the extent maps allocated so far.
                         */
                        advfs_free_blkmaps(extent_blk_desc);
                        if (unlock_xtntlock) {
                            if (do_snap_xtnts) {
                                advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
                            } else {
                                ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
                            }
                        }
                        return E_WOULD_BLOCK;
                    }
                } else {
                    /* XTNT_WAIT_OK case */
                    cur_range = (extent_blk_desc_t*)
                                ms_malloc_no_bzero(sizeof(extent_blk_desc_t));
                }

                /* We got the memory we wanted */

                /* Initialize the extent_blk_desc */
                cur_range->ebd_snap_fwd = NULL;
                cur_range->ebd_bfap = bfap;

                if (prev_range != NULL) {
                    /* We have already started the list.  We need to chain
                     * onto prev_range, not the head of the list */
                    prev_range ->ebd_next_desc = cur_range;
                } else { /* We need to link to head of list. */
                    *extent_blk_desc = cur_range;
                }
                /*
                 * If we're rounding to allocation units and we haven't
                 * yet processed any extents, the caller is asking for
                 * information about a hole so further round down cur_fob to 
                 * the allocation unit.
                 */
                if ((cur_fob == start_fob) && 
                    (round_type == RND_ALLOC_UNIT)) {
                    start_fob = 
                    cur_fob = ADVFS_ROUND_FOB_DOWN_TO_ALLOC_UNIT(bfap, cur_fob);
                    *offset = ADVFS_FOB_TO_OFFSET(start_fob);
                }

                /* Initialize the cur_range structure */
               prev_range = cur_range;
               cur_range->ebd_offset = ADVFS_FOB_TO_OFFSET(cur_fob);
               cur_range->ebd_byte_cnt = ADVFS_FOB_TO_OFFSET(end_fob - cur_fob);


               /*
                * If we are request all holes, then it must include the
                * hole to the end of the file, or end_fob whichever is
                * greater. We also need to make sure to back up to the
                * start of the last extent since we are beyond the last
                * storage and want the entire hole.
                */
               if (round_type == RND_ENTIRE_HOLE) {
                   int32_t sub_xtnt_idx;
                   int32_t desc_idx;
                   MS_SMP_ASSERT( bfap->dataSafety != BFD_LOG );

                   /*
                    * Deal with backing up to last storage.
                    */
                   sub_xtnt_idx = xtnt_map->validCnt - 1;
                   desc_idx = xtnt_map->subXtntMap[sub_xtnt_idx].cnt-1;
                   start_fob = cur_fob =
                       xtnt_map->subXtntMap[sub_xtnt_idx].bsXA[desc_idx].bsx_fob_offset;
                    *offset = ADVFS_FOB_TO_OFFSET(start_fob);

                    cur_range->ebd_offset = ADVFS_FOB_TO_OFFSET(cur_fob);


                   /*
                    * Deal with extending to EOF.
                    */
                   if (bfap->dataSafety == BFD_METADATA) {
                       cur_range->ebd_byte_cnt = 
                           (end_fob * ADVFS_FOB_SZ)- (cur_fob * ADVFS_FOB_SZ);
                   } else {
                       cur_range->ebd_byte_cnt = 
                           max((off_t)(end_fob * ADVFS_FOB_SZ),
                               bfap->file_size) - (cur_fob * ADVFS_FOB_SZ);
                   }
               }


               cur_range->ebd_vd_index = 0;
               cur_range->ebd_vd_blk = XTNT_TERM;

                /*
                 * We need to count this extent as being in the list 
                 */
                if (xtnt_count != NULL) {
                    (*xtnt_count)++;
                }
            } else {
                MS_SMP_ASSERT( blkmap_flags & XTNT_NO_MAPS );
                /* 
                 * We just need to bump the count if no maps were requested and
                 * we want holes.  If EXB_HOLES_ONLY, *xtnt_count gets TRUE,
                 * otherwise, it is incremented.
                */
                if (extent_blk_map_type & EXB_ONLY_HOLES) {
                    MS_SMP_ASSERT( *extent_blk_desc == NULL);
                    /* There should be no reason to pass NULL in
                     * with XTNT_NO_MAPS... but the code would
                     * support it. */
                    MS_SMP_ASSERT(xtnt_count != NULL);
                    *xtnt_count = TRUE;
                    /*
                     * If we're rounding to allocation units and we haven't
                     * yet processed any extents, the caller is asking for
                     * information about a hole so further round down cur_fob to 
                     * the allocation unit.
                     */
                    if ((cur_fob == start_fob) && 
                        (round_type == RND_ALLOC_UNIT)) {
                        start_fob = 
                            cur_fob = ADVFS_ROUND_FOB_DOWN_TO_ALLOC_UNIT(bfap, cur_fob);
                        *offset = ADVFS_FOB_TO_OFFSET(start_fob);
                    }
                    if (unlock_xtntlock) {
                        if (do_snap_xtnts) {
                            advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
                        } else {
                            ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
                        }
                    }
                    return EOK;
                } else if (extent_blk_map_type & EXB_COMPLETE) {
                   /* There should be no reason to pass NULL in
                    * with XTNT_NO_MAPS... but the code would
                    * support it. */
                   MS_SMP_ASSERT(xtnt_count != NULL);
                    (*xtnt_count)++;
                } 
                MS_SMP_ASSERT( !(extent_blk_map_type & EXB_ONLY_STG) );
            }
        }

        /* Terminate list if a list was created.  Any time we added an
         * element to the list, we updated cur_range. It started as NULL so
         * if we never added anything to the list, it will still be NULL, as
         * will the *extent_blk_desc pointer */
        if (cur_range != NULL) {
            cur_range->ebd_next_desc = NULL;
        }

        /* 
         * For RND_MIGRATE, we can't have a list which ends in a hole.
         * Trailing storage must always accompany a hole.
         */ 
        if (round_type == RND_MIGRATE) {
            extent_blk_desc_t *last_range = *extent_blk_desc;  /* Pointer to range */
            /*
             * Check for list with multiple elements, and we end in a hole.
             */
            if (cur_range != *extent_blk_desc && ADVFS_HOLE(cur_range)) {
                while (last_range->ebd_next_desc != cur_range) {
                    last_range = last_range->ebd_next_desc;
                }
                last_range->ebd_next_desc = NULL;
                advfs_free_blkmaps(&cur_range);
            }
        }


        if (!(blkmap_flags & XTNT_LOCKS_HELD))  {
            /* We didn't come in with locks held, so don't return with them
             * held */
            if (do_snap_xtnts) {
                advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
            } else {
                ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
            }
        }

        if (error) {
            advfs_free_blkmaps(extent_blk_desc);
            return error;
        }

        /* Success! */
        return EOK;

}


/*
 * BEGIN_IMS
 *
 * advfs_free_blkmaps - This function frees the memory for a list of
 * extent_blk_desc_t structures returned from advfs_get_blkmaps_in_range. 
 *
 * Parameters:
 *      IN      extent_blk_desc - A pointer to a pointer to a list of
 *      extent_blk_desc_t structures.  
 *
 * Return Values:
 *      0 - Success
 *      EBAD_PARAMS - Parameters were invalid
 *
 *
 * Entry/Exit Conditions:
 *      On entry, *extent_blk_desc != NULL or EBAD_PARAMS will be returned.
 *      On exit, *extent_blk_desc == NULL and all memory in the list has
 *              been freed.
 *     
 * Algorithm:
 * This function walks the extent_blk_desc list and frees each structure.
 *
 * END_IMS
 */

statusT
advfs_free_blkmaps(
        extent_blk_desc_t** extent_blk_desc
        )        

{
    extent_blk_desc_t* cur_extent;  /* current to be freed */
    extent_blk_desc_t* next_extent; /* next in list to be freed */

    /* Make sure we have a valid list */
    if (extent_blk_desc == NULL) {
        return EBAD_PARAMS;
    } else if (*extent_blk_desc == NULL) {
        /* 
         * This list has nothing in it, all memory is already freed.
         */
        return EOK;
    }
        


    cur_extent = * extent_blk_desc;

    /* Loop and free memory */
    do {
        next_extent = cur_extent->ebd_next_desc;
        ms_free(cur_extent);
        cur_extent = next_extent;
    } while (cur_extent != NULL);

    *extent_blk_desc = NULL;

    return EOK;
    

}
