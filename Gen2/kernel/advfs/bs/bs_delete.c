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
 *  AdvFS Storage System.
 *
 * Abstract:
 *
 *  bs_delete.c
 *  This module contains the top level bitfile delete routines.
 *
 */

#include <sys/param.h>
#include <sys/user.h>
#include <sys/vnode.h>                /* struct vnode */

#include <ms_public.h>
#include <ms_privates.h>
#include <ms_assert.h>
#include <bs_bmt.h>
#include <bs_delete.h>
#include <bs_stg.h>
#include <ms_osf.h>
#include <bs_public.h>
#include <bs_params.h>
#include <bs_index.h>
#include <bs_access.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE BS_DELETE

/*
 * Define the maximum number of pages that may be pinned in
 * a subtransaction.
 */
#define DEL_MAX_PAGES 7         /* should agree with FTX_MX_PINP */

/*
 * Operation type for managing the deferred delete list.
 */
#define DEL_ADD 0
#define DEL_REMOVE 1

/*
 * When the rstIndex field in the delFtxDescT structure has
 * this value, it indicates that the corresponding extent 
 * array has had all its SBM bits cleared.
 */
#define NORESTART ((uint32_t) -1)

/* Internal Types */

/*
 * delOpRecT 
 *
 * Defines a deleted bitfile by its tag, mcid and bitfile set.
 */
typedef struct {
    bfTagT tag;
    bfMCIdT mcid;
    bfSetIdT bfSetId;
    lkStatesT prevAccState;
    bfStatesT prevBfState;
    uint32_t setBusy;
} delOpRecT;

/*
 * defDelOpRecT 
 * 
 * Ftx opx rec for deferred deletes.
 */
typedef struct {
    bfTagT tag;
    bfSetIdT bfSetId;
    short prevDeleteWithClone;
} defDelOpRecT;

/*
 * Undo structure for undoing adding to or removing from
 * global deferred delete list.
 */
typedef struct {
    bfMCIdT  mcid;
    uint32_t type;
} delListRecT;

/*
 * Structure to describe a delete ftx transaction.
 */
typedef struct {
    bf_fob_t dfdPgSize;         /* page size of bitfile (not SBM) in 1k fobs */
    bsXtntRT *pxp;              /* primary extent record pointer */
    uint32_t rstIndex;          /* xtnt to start with */
    bf_fob_t dfdRstFobOffset;   /* offset into start xtnt in 1k fobs */
    bfMCIdT rstMCId;            /* mcell to start with */
    rbfPgRefHT pinPgH;          /* page handle */
    ftxHT ftxH;                 /* transaction handle */
} delFtxDescT;

/* Internal prototypes. */

static statusT
rbf_delete_int(
    bfAccessT* bfap,
    ftxHT parentFtxH
    );

void
reset_ondisk_bf_state (
                       domainT *dmnP,  /* in */
                       bfMCIdT mcellId,  /* in */
                       bfStatesT bfState,  /* in */
                       ftxHT ftxH  /* in */
                       );

static statusT
del_range(
    bf_vd_blk_t start_vd_blk,    /* in - start of range to zero */
    bf_vd_blk_t *vd_blk_cnt,     /* in - number of blocks to free  
                                  *  out number of blocks actually freed */
    int *pinPages,               /* in/out - maximum number of pages that can be
                                    pinned / number of pages that were pinned */
    vdT *vdp,                    /* in - virtual disk containing range */
    bf_fob_t pgSz,               /* in - page size in 1k fobs */
    ftxHT ftxH                   /* in transaction handle */
    );

static statusT
del_xtnt_array(
    bfMCIdT pmcid,          /* in - mcell ID of primary mcell */
    bfMCIdT mcid,           /* in - mcell ID of extra or shadow xtnts record */
    vdT *pvdp,              /* in - pointer to virtual disk of primary */
    vdT *vdp,               /* in - pointer to virtual disk of xtra or shad */
    bfMCIdT *nextMCId       /* out - mcell ID of next mcell */
    );

static statusT
advfs_pin_ddl_link(
    bfAccessT *bmtbfap,
    bfMCIdT mcid,
    uint32_t pin_rec,
    rbfPgRefHT *pgRefp,
    delLinkT **zlp,
    vdT *vdp,           /* in - Volume pointer */
    ftxHT ftxH
    );

static statusT
advfs_pin_ddl_header(
    bfAccessT* bmtbfap,
    delLinkT **zlp,
    vdT *vdp,           /* in - Volume Pointer */
    ftxHT ftxH
    );

static statusT 
del_clean_fix_ddl(
    vdT *vdp,
    bfPageRefHT  pgRef
    );

static statusT
ddl_complete_delete(
    bfTagT setTag,
    bfTagT fileTag,
    vdT *vdp
    );

static filesetnode_init(
    bfSetT *bfSetp,
    fileSetNodeT **fsnpA
    );

static statusT
del_ftx_start(
    bfMCIdT pcmid,
    vdT *pvdp,
    delFtxDescT *ddp
    );

static void
del_ftx_done(
    delFtxDescT *ddp     
    );

/* Undo routines. */

opxT del_bitfile_list_undo;
opxT bs_deferred_delete_undo_opx;
opxT bs_delete_undo_opx;
opxT bs_delete_rtdn_opx;

/*
 *        OVERVIEW OF DELETION FOR VERY LARGE OR FRAGMENTED BITFILES
 *
 * A global list (the deferred-delete list, DDL) is a doubly linked list which 
 * is chained through fields maintained in the extent record of primary 
 * mcells. This list contains mcells that describe disk storage that must
 * be deallocated.
 *
 * The DDL list itself is entirely on one virtual disk.
 *
 * After the transaction tree which puts the mcell list on the DDL as a
 * subtransaction has committed,  then a new root "delete" transaction may
 * be started to free clusters in the storage bitmap as well as 
 * mcells.  All the SBM storage represented by the primary mcell's
 * normal chain of extents is freed, and then the mcell chain itself is
 * released to the free mcell pool.
 * 
 * The "delete" transaction must be started only when no other transactions
 * exist in the current thread.  It is actually implemented as a
 * chain of independent root transactions in order not to consume too
 * many resources from the transaction management subsystem.  Nevertheless,
 * the transaction chain is atomic:  either all of the component
 * transactions execute or none of them do.  
 *
 * The public interfaces to this file are:
 *
 *   rbf_delete                 - delete any bitfile (clones too)
 *   rbf_delete_int             - mark bitfile as deleting
 *   bs_delete                  - front end to rbf_delete_int
 *
 *   del_dealloc_stg            - actually remove storage - applies
 *                                 to bitfile deletion or range removal
 *
 *   del_add_to_del_list        - add mcell to DDL
 *   del_remove_from_del_list   - remove mcell from DDL
 *
 *   init_bs_delete_opx         - called during system initialization
 *   del_init_mcell_list        - called during system initialization
 *
 *   del_clean_mcell_list       - called during recovery 
 */

/*
 * bs_deferred_delete_undo_opx
 *
 * This routine puts the 'deleteWithClone' bitfile attribute
 * back to the state it was in before rbf_delete() was called.
 */

void
bs_deferred_delete_undo_opx(
    ftxHT ftxH,         /* in - transaction handle */
    int opRecSz,        /* in - undo record size */
    void *opRec         /* in - ptr to undo record */
    )
{
    defDelOpRecT defDelOpRec;
    statusT sts = EOK;
    bsBfAttrT bfAttr;
    bfSetT *bfSetp=NULL;
    bfAccessT *bfap=NULL;
    domainT *dmnP;
    struct vnode *nullvp = NULL;

    bcopy(opRec, &defDelOpRec, sizeof(defDelOpRecT));

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(defDelOpRec.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(defDelOpRec.bfSetId.domainId, dmnP->dualMountId)) ) {
            defDelOpRec.bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("bs_deferred_delete_undo_opx: domainId mismatch");
        }
    }

    sts = bfs_open(&bfSetp, defDelOpRec.bfSetId, BFS_OP_IGNORE_DEL, ftxH);
    if (sts != EOK) { 
      domain_panic(ftxH.dmnP,"deferred delete undo: bfs_open() failed");
      goto HANDLE_EXCEPTION;
    }

    sts = bs_access(&bfap, defDelOpRec.tag, bfSetp, ftxH, BF_OP_INTERNAL, &nullvp);
    if (sts != EOK) { 
      domain_panic(ftxH.dmnP,"deferred delete undo: bs_access() failed");
      goto HANDLE_EXCEPTION;
    }

    /*
     * In order to prevent a hierarchy violation we grab the
     * BfSetTblLock prior to calling bmtr_get_rec_n_lk which
     * will acquire mcellList_lk.  Later, when we call
     * bs_bfs_close() we will already have the lock.
     */
    ADVRWL_BFSETTBL_WRITE( dmnP );

    sts = bmtr_get_rec_n_lk( bfap,
                             BSR_ATTR, 
                             &bfAttr, 
                             (uint16_t) sizeof( bfAttr ),
                             BMTR_LOCK );
    if (sts != EOK) {
        ADVFS_SAD1 ("deferred delete undo: get attr err",sts);
    }


    sts = bmtr_update_rec_int( bfap,
                               BSR_ATTR, 
                               &bfAttr, 
                               (uint16_t) sizeof( bfAttr ),
                               ftxH,
                               BMTR_UNLOCK,
                               0 );
    if (sts != EOK) {
        ADVFS_SAD1 ("deferred delete undo: set attr err", sts);
    }

HANDLE_EXCEPTION:
    if (bfap) bs_close(bfap, MSFS_CLOSE_NONE);
    if (bfSetp) bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

    return;
}

/*
 * rbf_delete
 *
 * TODO - update this comment
 *
 * Undoable delete bitfile.  Most of the real work is done by the
 * root done operations.  This is because freeing disk space and
 * mcells can't be done until the transaction has completed.  If we
 * did delete the space before the transaction completed and the 
 * transaction failed instead then we would have no hope of getting
 * back the original contents of the bitfile.  Therefore, this routine
 * simply defers most of its work to several root done operations.
 */

statusT
rbf_delete(
    bfAccessT *bfap,          /* in - bitfile's access structure */
    ftxHT parentFtxH          /* in - handle to parent transaction */
    )
{
    statusT sts = EOK;
    ftxHT ftxH;
    int ftxStarted = FALSE;

    /*
     * Be sure to review pre-snapshot code when implementing deferred
     * deletion (delete with children) since there was a lot of useful code
     * there.
     */

    ftxH = parentFtxH;
    MS_SMP_ASSERT(bfap->bfaFirstChildSnap == NULL && 
            !(bfap->bfaFlags & BFA_SNAP_CHANGE));

    sts = rbf_delete_int( bfap, ftxH );

    return (sts);
}

/*
 * ASSUMPTIONS:
 *
 * This routine may be called only from rbf_delete() or bs_bfs_delete() (and
 * its subroutines).  If it is called from rbf_delete() then the bfset state
 * is BUSY or DELETING.  If it is called from bs_bfs_delete() then the state
 * is DELETING or DELETING_CLONE.  In both cases the state will not change
 * between now and root done or undo.  If the state is BUSY, root done or
 * undo will set it to READY.
 */

static statusT
rbf_delete_int(
    bfAccessT* bfap,          /* in - bitfile's access structure */
    ftxHT parentFtxH          /* in - handle to parent transaction */
    )
{
    ftxHT ftxH, subFtxH;
    int ftxFlag = FALSE;
    vdT *vdp;
    domainT *dmnP;
    statusT sts = EOK;
    rbfPgRefHT pgref;
    bsMPgT *bmtpgp;
    bsMCT *mcp;
    bsBfAttrT *bfAttrp;
    delOpRecT delOpRec;
      
    dmnP   = bfap->dmnP;

    if (parentFtxH.hndl == 0) {
        ftxFlag = 1;
        sts = FTX_START_N(FTA_BS_DEL_DELETE_V1, &ftxH, parentFtxH, 
                          bfap->dmnP);
        if (sts != EOK) {
            ADVFS_SAD1 ("rbf_delete_int: ftx start failed", sts);
        }
    }
    else {
        ftxH = parentFtxH;
    }
    
    if (bfap->refCnt == 0) {
        sts = EINVALID_HANDLE;
        goto done;
    }

    /*
     * Once the bitfile state is set to BSRA_DELETING, the close operation
     * which decrements the refCnt to zero will free the the resources
     * associated with the bitfile.  This has the following consequence:
     * close must not be called from anywhere within the present
     * transaction tree.  Otherwise, resources may be freed before the
     * current delete transaction has committed.
     *
     * Another way of saying this is: the refCnt of the bitfile must not go 
     * to zero before the delete transaction's done record is in 
     * the (in-memory) log.
     *
     * Put the state lock into the ACC_INIT_TRANS state.  This state will
     * become ACC_VALID when the transaction commits.
     */
    ADVSMP_BFAP_LOCK(&bfap->bfaLock);

    lk_wait_for (&(bfap->stateLk), &bfap->bfaLock, ACC_VALID);
    delOpRec.prevAccState = ACC_VALID;
    (void) lk_set_state(&bfap->stateLk, ACC_INIT_TRANS);

    delOpRec.prevBfState = bfap->bfState;
    bfap->bfState = BSRA_DELETING;

    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

    sts = FTX_START_N(FTA_BS_DEL_DELETE_V1, &subFtxH, ftxH, 
                      bfap->dmnP);
    if (sts != EOK) {
        ADVFS_SAD1 ("rbf_delete_int: ftx start failed", sts);
    }

    /*
     * Set the bitfile attribute state to BSRA_DELETING.
     */

    vdp = VD_HTOP(bfap->primMCId.volume, dmnP);

    sts = rbf_pinpg(&pgref, (void *) &bmtpgp, vdp->bmtp,
                     bfap->primMCId.page, BS_NIL, subFtxH, MF_VERIFY_PAGE);

    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_366);
        domain_panic(dmnP, "rbf_delete_int: failed to access bmt pg %ld sts %d",
                     bfap->primMCId.page, sts);

        ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, subFtxH);
        ftx_fail(subFtxH);
        sts =  E_DOMAIN_PANIC;
        goto done;
    }

    mcp = &bmtpgp->bsMCA[bfap->primMCId.cell];

    bfAttrp = (bsBfAttrT *) bmtr_find(mcp, BSR_ATTR, vdp->dmnP);
    if (bfAttrp == NULL) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_367);
        domain_panic(dmnP, "rbf_delete_int: can't find bf attributes");
        ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, subFtxH);
        ftx_fail(subFtxH);
        sts =  E_DOMAIN_PANIC;
        goto done;
    }

    rbf_pin_record(pgref, &bfAttrp->state, (uint32_t)sizeof(bfAttrp->state));
    bfAttrp->state = BSRA_DELETING;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_139);

    /* bfAttrp->transitionId = get_ftx_id(subFtxH); */

    delOpRec.tag = bfap->tag;
    delOpRec.mcid = bfap->primMCId;
    delOpRec.setBusy = 0;
    delOpRec.bfSetId = bfap->bfSetp->bfSetId;

    ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, subFtxH);

    /*
     * Note that the root done deletes the bitfile's tag.
     */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_140);
    ftx_done_urd(subFtxH, FTA_BS_DEL_DELETE_V1, 
                 (int32_t)sizeof(delOpRecT), (void *)&delOpRec,
                 (int32_t)sizeof(delOpRecT), (void *)&delOpRec);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_141);

    if ((sts = del_add_to_del_list(bfap->primMCId, vdp, 1, ftxH)) != EOK) {
        domain_panic(dmnP, "rbf_delete_int: can't add bitfile to del list\ndel_add_to_del_list returned: %d ", sts);
        sts =  E_DOMAIN_PANIC;
    }
 
done:
    if (ftxFlag) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_142);
        ftx_done_n(ftxH, FTA_BS_DEL_DELETE_V1); 
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_143);
    }

    return (sts);
}

/*
 * bs_delete_undo_opx
 *
 * Restore the on-disk bitfile state.
 *
 * This function assumes that the bitfile's state is BSRA_DELETING.
 */

void
bs_delete_undo_opx(
    ftxHT ftxH,         /* in - transaction handle */
    int opRecSz,        /* in - undo record size */
    void *opRec         /* in - ptr to undo record */
    )
{
    delOpRecT recp;
    statusT sts = EOK;
    struct bfAccess *bfap = NULL;
    domainT *dmnP = NULL;
    bfSetT *bfSetp = NULL;

    bcopy(opRec, &recp, sizeof(delOpRecT));

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(recp.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(recp.bfSetId.domainId, dmnP->dualMountId)) ) {
            recp.bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("bs_delete_undo_opx: domainId mismatch");
        }
    }

    sts = bfs_open( &bfSetp, recp.bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if (sts != EOK) { 
        domain_panic(ftxH.dmnP,"bs_delete_undo_opx: bs_bfs_access() failed");
        goto HANDLE_EXCEPTION;
    }


    /*
     * Get the bitfile's access struct.  In recovery, we typically will not
     * find the bfap, but it is possible that a redo operation opened the
     * file and brought it into cache.  This is especially likely for files
     * in the root tag directory.
     */ 
    bfap = advfs_lookup_valid_bfap(bfSetp, 
                                       recp.tag, 
                                       BF_OP_INMEM_ONLY |
				       BF_OP_INTERNAL |
				       BF_OP_OVERRIDE_SMAX,
                                       ftxH ); 
    /* 
     * advfs_lookup_valid_bfap will acquire the bfap->bfaLock 
     */

    if (bfap != NULL) {
        MS_SMP_ASSERT( lk_get_state( bfap->stateLk ) != ACC_INIT_TRANS );
        /*
         * If we got a bfap, then update it.  It should not be in
         * ACC_INIT_TRANS.  If we are not in recovery, we want to wait until
         * it is valid.  Otherwise, if we are in recovery, we just want to
         * make sure it is not ACC_INIT_TRANS which would indicate we just
         * created it in advfs_lookup_valid_bfap and we should not have done
         * that.
         *
         * We could be in BFD_ACTIVATED if a parent transaction to a
         * deletion is being failed and causing this undo to run. 
         */
        if (dmnP->state == BFD_ACTIVATED) {
            /*
             * We have a refCnt on the access structure so it can't be freed.
             * This will allow us to do a lk_wait_for without worrying about the
             * memory going away.
             */
            lk_wait_for (&(bfap->stateLk), &bfap->bfaLock, ACC_VALID);
        }

        /* 
         * Set the state temporarily to ACC_INIT_TRANS so no one else tries to
         * mess with the access struct until we get back and reset
         * the bfState.
         */
        (void) lk_set_state(&bfap->stateLk, ACC_INIT_TRANS);
        ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

        reset_ondisk_bf_state (
                               dmnP, 
                               recp.mcid,
                               recp.prevBfState,
                               ftxH
                               );
        ADVSMP_BFAP_LOCK(&bfap->bfaLock);
        (void) lk_set_state(&bfap->stateLk, ACC_VALID);
        bfap->bfState = recp.prevBfState;

        /* 
         * This decrements the refCnt put on in advfs_lookup_valid_bfap.  It
         * will free the bfaLock unconditionally 
         */
        DEC_REFCNT( bfap, DRC_CLOSED_LIST );

    } else {
        /*
         * Crash/recovery.
         */
        reset_ondisk_bf_state (
                               dmnP, 
                               recp.mcid,
                               recp.prevBfState,
                               ftxH
                               );
    }

HANDLE_EXCEPTION:
    if (bfSetp) bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    return;
}

/*
 * This function resets a bitfile's on-disk state.
 */

void
reset_ondisk_bf_state (
                       domainT *dmnP,  /* in */
                       bfMCIdT mcellId,  /* in */
                       bfStatesT bfState,  /* in */
                       ftxHT ftxH  /* in */
                       )
{
    bsBfAttrT *bfAttrp;
    bsMPgT *bmtpgp;
    bsMCT *mcp;
    rbfPgRefHT pgref;
    statusT sts;
    vdT *vdp;

    vdp = VD_HTOP(mcellId.volume, dmnP);

    sts = rbf_pinpg( &pgref, (void *) &bmtpgp, vdp->bmtp,
                    mcellId.page, BS_NIL, ftxH, MF_VERIFY_PAGE );
    if (sts != EOK) {
        domain_panic(dmnP,
                    "reset_ondisk_bf_state: failed to access bmt pg %d, err %d",
                    mcellId.page, sts);
        goto done;
    }
    mcp = &bmtpgp->bsMCA[mcellId.cell];

    bfAttrp = (bsBfAttrT *) bmtr_find( mcp, BSR_ATTR, vdp->dmnP);
    if ( bfAttrp == NULL) {
        domain_panic(dmnP, "reset_ondisk_bf_state: can't find bf attributes");
        goto done;
    }

    rbf_pin_record( pgref, &bfAttrp->state, (int32_t)sizeof( bfAttrp->state ) );
    bfAttrp->state = bfState; 
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_144);

done:
    return;

}  /* end reset_ondisk_bf_state */


void
bs_delete_rtdn_opx(
    ftxHT ftxH,         /* in - transaction handle */
    int opRecSz,        /* in - undo record size */
    void *opRec         /* in - ptr to undo record */
    )
{
    delOpRecT recp;
    statusT sts = EOK;
    bfSetT *bfSetp;
    domainT *dmnP;
    int bfset_open = 0;

    bcopy(opRec, &recp, sizeof(delOpRecT));

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(recp.bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(recp.bfSetId.domainId, dmnP->dualMountId)) ) {
            recp.bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("bs_delete_rtdn_opx: domainId mismatch");
        }
    }

    if (dmnP->state != BFD_ACTIVATED) {
        sts = bfs_open( &bfSetp, recp.bfSetId, BFS_OP_IGNORE_DEL, ftxH );
        if (sts != EOK) { 
          domain_panic(ftxH.dmnP,"bs_delete_rtdn_opx: bfs_open() failed");
          goto HANDLE_EXCEPTION;
        }
        bfset_open = 1;
    } else {
        bfSetp = bfs_lookup( recp.bfSetId );
    }
    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );

    if ( bfset_open ) {
        /*
         * In order to prevent a hierarchy violation we grab the
         * BfSetTblLock prior to calling tagdir_remove_tag().
         * Later, when we call bs_bfs_close() we will already
         * have the lock.
         */
        ADVRWL_BFSETTBL_WRITE( dmnP );
    }

    tagdir_remove_tag( bfSetp, &recp.tag, ftxH );

    if ( bfset_open ) {
        bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    }

HANDLE_EXCEPTION:
    return;
}

/*
 * init_bs_delete_opx
 *
 * Initializes the delete agent's ftx operations.
 */

void
init_bs_delete_opx( void )
{
    statusT sts;

    sts = ftx_register_agent( FTA_BS_DEL_DELETE_V1,
                              &bs_delete_undo_opx,      /* undo */
                              &bs_delete_rtdn_opx       /* root done */
                              );
    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_delete_opx: ftx register failure1", sts);
    }

    sts = ftx_register_agent( FTA_BS_DEL_DEFERRED_V1,
                              &bs_deferred_delete_undo_opx, /* undo */
                              0 /* root done */
                              );
    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_delete_opx: ftx register failure2", sts);
    }

    sts = ftx_register_agent(FTA_BS_DEL_ADD_LIST_V1, 
                             del_bitfile_list_undo, 
                             (opxT *) 0);
    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_delete_opx: ftx register failure3", sts);
    }

    sts = ftx_register_agent(FTA_BS_DEL_REM_LIST_V1, 
                             del_bitfile_list_undo, 
                             (opxT *) 0);
    if (sts != EOK) {
        ADVFS_SAD1 ("init_bs_delete_opx: ftx register failure3", sts);
    }
}


#ifdef ADVFS_SMP_ASSERT
int AdvfsCheckDDL = 1;
#endif

/*
 * Add a bitfile to the global deferred delete list.
 * 
 * Up to three (r)bmt pages are pinned.
 *
 * TODO: This must be synchronized with migrate.  That is, migrate 
 * must be able to detect that a file is on the list and refrain
 * from migrating the file until the file is removed from the list.
 */

statusT
del_add_to_del_list(
    bfMCIdT mcid,
    vdT *vdp,
    int ftxFlag,        /* in - don't start subtransaction if zero */
    ftxHT parentFtxH
    )
{
    domainT *dmnP;
    statusT sts;
    delLinkRT *hrp;     /* header */
    delLinkT *frp;      /* first in list */
    delLinkT *nrp;      /* new entry */
    ftxHT ftxH;
    rbfPgRefHT pgRef;
    bsXtntRT *xrp;
    bsMPgT *bmtp;
    bsMCT *mcp;
    delListRecT delListRec;
#ifdef ADVFS_SMP_ASSERT
    bfPageRefHT bmtPgRef;
    bfMCIdT ddlMcellId;
    int delFlag;
#endif

    dmnP = vdp->dmnP;

/*
 * Make sure this mcell isn't already on the DDL.  
 * This is trap code.
 */
#ifdef ADVFS_SMP_ASSERT
    if (AdvfsCheckDDL) {
        if ((sts = bs_refpg(&bmtPgRef,
                            (void **)&bmtp,
                            vdp->bmtp,
                            mcid.page, 
                            FtxNilFtxH,
                            MF_VERIFY_PAGE)) != EOK) {
            goto done;
        }
        mcp = &bmtp->bsMCA[mcid.cell];
        sts = del_find_del_entry(dmnP, vdp->vdIndex, mcp->mcBfSetTag,
                                 mcp->mcTag, &ddlMcellId, &delFlag);
        if ((sts == EOK) && 
            ((mcid.page == ddlMcellId.page) && 
             (mcid.cell == ddlMcellId.cell))) {
            /*
             * This should never happen.  It means we're about to put an
             * mcell onto the DDL that is already there.
             */
            ADVFS_SAD0("trap code: duplicate mcell on DDL!\n");
        }
        sts = bs_derefpg (bmtPgRef, BS_CACHE_IT);
        if (sts != EOK) {
            goto done;
        }
    }
#endif

    if (ftxFlag) {
        if ((sts = FTX_START_N(FTA_BS_DEL_ADD_LIST_V1, &ftxH, parentFtxH,
                               vdp->dmnP)) != EOK){
            return (sts);
        }
    }
    else {
        ftxH = parentFtxH;
    }

    FTX_LOCKWRITE(&vdp->del_list_lk, ftxH);

    if ((sts = rbf_pinpg(&pgRef, (void **)&bmtp, vdp->bmtp, mcid.page, 
                         BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        goto done;
    }
    mcp = &bmtp->bsMCA[mcid.cell];
    if ((xrp = (bsXtntRT *)bmtr_find(mcp, BSR_XTNTS, vdp->dmnP)) == NULL) {
        ADVFS_SAD0 ("del_add_to_del_list: can't find primary xtnt rec");
    }

    /*
     * Make sure that clearing the SBM starts at the beginning by
     * initializing the crash restart fields.
     */
    if (dmnP->state == BFD_ACTIVATED) {
        /*
         * NOTE:  Only do this during runtime, not recovery.  If we are
         * in recovery, we must preserve the delete re-start fields so
         * that we don't process this mcell multiple times and get the
         * infamous "can't clear bits twice" abort.
         */
        rbf_pin_record(pgRef, (void *)&xrp->delRst, (int32_t)sizeof(xrp->delRst));
        xrp->delRst.drtFobOffset = 0;
        xrp->delRst.xtntIndex = 0;
        xrp->delRst.mcid = bsNilMCId;
        xrp->delRst.drtQuotaBlks= 0;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_145);
    }

    if ((sts = advfs_pin_ddl_header(vdp->bmtp, &hrp, vdp, ftxH)) != EOK) {
        goto done;
    }
    if ((sts = advfs_pin_ddl_link(vdp->bmtp, 
                                  mcid, 
                                  TRUE,
                                  NULL, 
                                  &nrp, 
                                  vdp, 
                                  ftxH)) != EOK) {
        goto done;
    }
    if (!MCID_EQL(hrp->nextMCId, bsNilMCId)) {
        if ((sts = advfs_pin_ddl_link(vdp->bmtp, 
                                      hrp->nextMCId, 
                                      TRUE,
                                      NULL,
                                      &frp, vdp, ftxH)) != EOK) {
            hrp->nextMCId = bsNilMCId;

            printf("AdvFS: Corrupted deferred delete list in domain '%s'!\n",
                                vdp->dmnP->domainName);
            printf("       Status = %d\n", sts);
            printf("       No data has been lost but some disk space\n");
            printf("       in this domain may no longer be usable.\n");

            sts = EOK;
        }
        else {
            frp->prevMCId = mcid;
        }
    }
    nrp->prevMCId = bsNilMCId;
    nrp->nextMCId = hrp->nextMCId;
    hrp->nextMCId = mcid;

done:
    if (ftxFlag) {
        if (sts == EOK) {
            delListRec.mcid = mcid;
            delListRec.type = DEL_ADD;

            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_146);
            ftx_done_u(ftxH, FTA_BS_DEL_ADD_LIST_V1, 
                       (int32_t)sizeof(delListRecT), (void *) &delListRec);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_147);
        }
        else {
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_148);
            ftx_done_n(ftxH, FTA_BS_DEL_ADD_LIST_V1); 
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_149);
        }
    }
    return (sts);
}

/*
 * Remove a bitfile from global deferred delete list.  If ftxFlag is 
 * nonzero, a subtransaction is started.  Up to three (r)bmt pages are pinned.
 *
 * TODO: This must be synchronized with migrate.
 */

statusT
del_remove_from_del_list(
    bfMCIdT mcid,
    vdT *vdp,
    int ftxFlag,
    ftxHT parentFtxH
    )
{
    domainT *dmnP;
    statusT sts;
    delLinkRT *hrp;     /* header */
    delLinkT *drp;      /* entry to be removed */
    delLinkT *frp;      /* following entry or null */
    delLinkT *prp;      /* previous entry or null */
    int first, last;
    delListRecT delListRec;
    ftxHT ftxH;
    rbfPgRefHT pgRef;

    dmnP = vdp->dmnP;

    if (ftxFlag) {
        if ((sts = FTX_START_N(FTA_BS_DEL_REM_LIST_V1, &ftxH, parentFtxH, 
                               vdp->dmnP)) != EOK) {
            return (sts);
        }
    }
    else {
        ftxH = parentFtxH;
    }

    FTX_LOCKWRITE(&vdp->del_list_lk, ftxH);


    if ((sts = advfs_pin_ddl_link(vdp->bmtp, 
                                  mcid, 
                                  FALSE,
                                  &pgRef,
                                  &drp,
                                  vdp, ftxH)) != EOK) {
        goto done;
    }
    last = 1;
    /*
     * This code contains  a work-around for case at system startup where
     * we are walking the list from the begining and our mcNextMCId ptr points
     * to a free mcell.  Instead of panicing, we truncate the list.  We
     * may lose some space, but hey, we're still running.  I didn't
     * code the case where we are pulling an mcell out of the middle of
     * the list and we find the prevMCId ptr points to a free mcell.  I
     * haven't seen that case yet, although others may have.
     */
    if (!MCID_EQL(drp->nextMCId, bsNilMCId)) {
        if ((sts = advfs_pin_ddl_link(vdp->bmtp, 
                                      drp->nextMCId, 
                                      TRUE,
                                      NULL, &frp, vdp, ftxH)) != EOK) {

            rbf_pin_record(pgRef, drp, (int32_t) sizeof(delLinkT));
            drp->nextMCId = bsNilMCId;

            last = 1;

            ms_uaprintf("AdvFS: Corrupted deferred delete list in domain '%s'!\n",
                                vdp->dmnP->domainName);
            ms_uaprintf("       Status = %d\n", sts);
            ms_uaprintf("       No data has been lost but some disk space\n");
            ms_uaprintf("       in this domain may no longer be usable.\n");

            sts = EOK;
        } else last = 0;
    }
    if (MCID_EQL(drp->prevMCId, bsNilMCId)) {
        if ((sts = advfs_pin_ddl_header(vdp->bmtp, &hrp, vdp, ftxH)) != EOK) {
            goto done;
        }
        first = 1;
    }
    else {
        if ((sts = advfs_pin_ddl_link(vdp->bmtp, 
                                      drp->prevMCId, 
                                      TRUE,
                                      NULL, &prp, vdp, ftxH)) != EOK) {
            goto done;
        }
        first = 0;
    }

    if (first) {
        hrp->nextMCId = drp->nextMCId;
    }
    else {
        prp->nextMCId = drp->nextMCId;
    }
    if (!last) {
        frp->prevMCId = drp->prevMCId;
    }

done:
    if (ftxFlag) {
        if (sts == EOK) {
            delListRec.mcid = mcid;
            delListRec.type = DEL_REMOVE;

            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_150);
            ftx_done_u(ftxH, FTA_BS_DEL_REM_LIST_V1, 
                       (int32_t) sizeof(delListRecT), (void *)&delListRec);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_151);
        }
        else {
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_152);
            ftx_done_n(ftxH, FTA_BS_DEL_REM_LIST_V1); 
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_153);
        }
    }
    return (sts);
}

void
del_bitfile_list_undo(
    ftxHT ftxH,         /* in - transaction handle */
    int opRecSz,        /* in - undo record size */
    void *opRec         /* in - ptr to undo record */
    )
{
    delListRecT zrp;
    statusT sts;
    vdT *vdp;
    domainT *dmnP;

    bcopy(opRec, &zrp, sizeof(delListRecT));

    dmnP = ftxH.dmnP;
    vdp = VD_HTOP((vdIndexT)zrp.mcid.volume, dmnP);

    if (zrp.type == DEL_ADD) {
        /*
         * Undo of add to del list operation.
         */
        if ((sts = del_remove_from_del_list(zrp.mcid, vdp, 0, ftxH)) != EOK) {
            domain_panic (dmnP, "del_bitfile_list_undo: del_remove_from_del_list returned: %d", sts);
            return;
        }
    }
    else if (zrp.type == DEL_REMOVE) {
        /*
         * Undo of remove from del list operation.
         */
        if ((sts = del_add_to_del_list(zrp.mcid, vdp, 0, ftxH)) != EOK) {
            domain_panic (dmnP, "del_bitfile_list_undo: del_add_to_del_list returned: %d", sts);
            return;
        }
    }
    else {
        ADVFS_SAD0 ("del_bitfile_list_undo: bad undo type");
    }
}

/*
 * For a given mcell, pin its BMT page and pin the record containing its
 * del link.  An out parameter specifies a pointer to the del link record.
 */

static statusT
advfs_pin_ddl_link(
    bfAccessT *bmtbfap,
    bfMCIdT mcid,
    uint32_t pin_rec,
    rbfPgRefHT *pgRefp,
    delLinkT **zlp,
    vdT *vdp,           /* in - Volume pointer */
    ftxHT ftxH
    )
{
    rbfPgRefHT pgRef;
    bsXtntRT *rp;
    bsMPgT *bmtp;
    bsMCT *mcp;
    statusT sts;

    if ((sts = rbf_pinpg(&pgRef, (void **)&bmtp, bmtbfap, mcid.page, 
                         BS_NIL, ftxH, MF_VERIFY_PAGE)) != EOK) {
        return (sts);
    }

    mcp = &bmtp->bsMCA[mcid.cell];
    if ((rp = (bsXtntRT *)bmtr_find(mcp, BSR_XTNTS, vdp->dmnP)) == NULL) {
        return E_CORRUPT_LIST;
    }
    if (pin_rec) {
        rbf_pin_record(pgRef, (void *)&rp->delLink, (int32_t) sizeof(delLinkT));
    } else {
        /* Return a handle so the caller can pin the record if needed */
        MS_SMP_ASSERT(pgRefp != NULL);
        *pgRefp = pgRef;
    }
    *zlp = &rp->delLink;
    ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_154);
    return (EOK);
}

/*
 * Pin the page and the record containing the del list header in a 
 * given BMT.  An out parameter specifies a pointer to the del link record.
 */

static statusT
advfs_pin_ddl_header(
    bfAccessT* bmtbfap,
    delLinkT **zlp,
    vdT *vdp,           /* in - volume pointer */
    ftxHT ftxH
    )
{
    rbfPgRefHT pgRef;
    bsMPgT *bmtp;
    bsMCT *mcp;
    statusT sts;
    bs_meta_page_t pgnum;


    pgnum = MCELL_LIST_PAGE;

    sts = rbf_pinpg(&pgRef, (void **)&bmtp, bmtbfap, pgnum, BS_NIL, ftxH, MF_VERIFY_PAGE);
    if (sts != EOK) {
        return (sts);
    }

    mcp = &bmtp->bsMCA[MCELL_LIST_CELL];
    if((*zlp = (delLinkT *)bmtr_find(mcp,BSR_DEF_DEL_MCELL_LIST,
                                        vdp->dmnP)) == NULL) {
        domain_panic(vdp->dmnP, "advfs_pin_ddl_header: bmtr_find can't find list head failed");
        return E_DOMAIN_PANIC;
    }

    rbf_pin_record(pgRef, (void *)(*zlp), sizeof(delLinkRT));
    ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_155);
    return (EOK);
}

/*
 * After domain activation, fix bitfiles which are in a partially
 * deleted state.  There is a delete list per virtual disk.  The primary
 * mcell for a bitfile can only go on the delete list for its virtual
 * disk.  (Chained mcells containing extra extent records could lie on
 * other virtual disks.)
 *
 * There are 4 cases to consider for mcell chains on the DDL:
 * 1) A file is deleted, but still open, in a cluster environment.
 *    In this case the file is not deleted yet. CFS will keep the file
 *    open and the new serving node will delete the storage on the last close.
 * 2) A file is deleted but still open at the time of a single node crash.
 *    The primary mcell on the DDL will conatin a BMTR_FS_STATS record and a
 *    BSR_ATTR record. The state field in the BSR_ATTR record will be
 *    BSRA_DELETING. This file never saw a final close. Open the file and
 *    close it. This will clean up the frag (if any) and the quotas. The
 *    disk storage will be free in bs_close_one.
 * 3) A file is deleted and finished the close transaction in bs_close_one
 *    but did not complete the second (stg_remove_stg_finish) storage
 *    removal transactions. The primary mcell on the DDL will conatin a
 *    BMTR_FS_STATS record and a BSR_ATTR record. The state field in the
 *    BSR_ATTR record will be BSRA_INVALID. Processing the DDL for this
 *    mcell chain consists of freeing the disk storage.
 * 4) A file is truncated and the second (stg_remove_stg_finish) storage
 *    removal transactions did not complete. The (psuedo) primary mcell on the
 *    conatins a a BSR_ATTR record. There is no BMTR_FS_STATS record. The
 *    state field in the BSR_ATTR record will be BSRA_VALID. There are two
 *    possible outcomes, both covered by del_dealloc_stg. If the file has
 *    a clone, the storage may be given to the clone. Otherwise the storage
 *    is freed.
 */

/*
 * If users set this to 1 via dbx, we'll panic a domain if it
 * has a corrupted DDL.  Generally, we just truncate the DDL.
 */
int AdvfsCaptureBadDDL = 0;

statusT
del_clean_mcell_list(
    vdT *vdp,
    uint64_t flag
    )
{
    bsMPgT* bmtp;
    bs_meta_page_t mcellListPage;
    bfMCIdT mcellId;
    bfMCIdT nextMcellId;
    delLinkRT *zlp;
    bsMCT* mcellp;
    bfPageRefHT  pgRef;
    statusT sts;
    bsXtntRT *pxp;      /* pointer to primary extent record */
    bsBfAttrT *attrp;   /* pointer to bitfile attributes */
    struct fs_stat *stats_ptr = NULL; /* pointer to bmt stats record */
    bfSetIdT bfSetId;
    bfSetT *bfSetp;
    bfAccessT* bfap;
    int delete_this_mcell, ftxStarted;
    ftxHT ftxH;
    
    mcellListPage = (bs_meta_page_t ) MCELL_LIST_PAGE;

    if ((bs_meta_page_t )(vdp->bmtp->bfaNextFob / vdp->bmtp->bfPageSz) <= mcellListPage) {
        /* 
         * Since the migrate mcell inuse listhead is maintained on page
         * MCELL_LIST_PAGE of the BMT, we must assume that the list is
         * empty if this page doesn't exist.
         */
        return (EOK);
    }

    if ((sts = bs_refpg(&pgRef,
                        (void*)&bmtp,
                        vdp->bmtp,
                        mcellListPage,
                        FtxNilFtxH,
                        MF_VERIFY_PAGE)) != EOK) {
        ADVFS_SAD0 ("del_clean_mcell_list: Can't ref MCELL_LIST_PAGE of BMT");
    }

    mcellp = &bmtp->bsMCA[MCELL_LIST_CELL];
    if ((zlp = (delLinkRT *) bmtr_find(mcellp, BSR_DEF_DEL_MCELL_LIST,
                                                vdp->dmnP)) == NULL) {
        ADVFS_SAD0 ("del_clean_mcell_list: Can't find del mcell list record");
    }

    mcellId = zlp->nextMCId;

    if ((sts = bs_derefpg(pgRef, BS_CACHE_IT)) != EOK) {
        ADVFS_SAD0 ("del_clean_mcell_list: Can't deref MCELL_LIST_PAGE of BMT");
    }

#ifdef ADVFS_DEBUG
    if (MCID_EQL(mcellId, bsNilMCId)) {
        printf("ADVFS delete pending list empty.");
    }
    else {
        printf("ADVFS recovering partially cleared storage:\n    ");
    }
#endif

    bfSetId.domainId = vdp->dmnP->domainId;
    while (!MCID_EQL(mcellId, bsNilMCId)) {

        if ((sts = bs_refpg(&pgRef,
                            (void*)&bmtp,
                            vdp->bmtp, 
                            mcellId.page,
                            FtxNilFtxH,
                            MF_VERIFY_PAGE)) != EOK) {
            return (sts);       /* XXX */
        }

        mcellp = &bmtp->bsMCA[mcellId.cell];

#ifdef ADVFS_DEBUG
        printf("page %ld cell %d set %ld tag %ld\n", 
                mcellId.page, mcellId.cell, mcellp->mcBfSetTag, mcellp->tag);
#endif

        /*
         * If this is a cluster we may not want to delete the file yet.
         */
        delete_this_mcell = FALSE;
        bfSetId.dirTag = mcellp->mcBfSetTag;
        if (clu_is_ready() && (flag & DMNA_FAILOVER)) {
            bfSetp = bfs_lookup(bfSetId); 
            if (bfSetp == NULL) {
                delete_this_mcell = TRUE;
            } else {
                bfap = advfs_lookup_valid_bfap(bfSetp, 
                                               mcellp->mcTag, 
                                               BF_OP_INMEM_ONLY|BF_OP_INTERNAL,
                                               FtxNilFtxH ); 
                if (bfap == NULL) {
                    delete_this_mcell = TRUE;
                } else {
                    MS_SMP_ASSERT( lk_get_state( bfap->stateLk ) != ACC_INIT_TRANS);
                    ADVFS_ACCESS_LOGIT( bfap, "del_clean_mcell_list skipped bfap" );
                    /* 
                     * DEC_REFCNT will decrement the refCnt and unlock the
                     * bfap.  It must have been opened by a cluster client
                     * during a failover.
                     */
                    DEC_REFCNT( bfap, DRC_CLOSED_LIST );
                }
            }
        } else delete_this_mcell = TRUE;

        /*
         * Find the primary extent record.  Must do this before
         * possibly calling del_dealloc_stg() since that routine
         * will eventually free the mcell, making it impossible
         * to find any records in it.
         */
        pxp = (bsXtntRT *) bmtr_find(mcellp, BSR_XTNTS, vdp->dmnP);
        if ( pxp == NULL) {
            /* Corrupt DDL. Patch it up then return. */
            (void)del_clean_fix_ddl( vdp, pgRef );
            return (EOK);
        }

        nextMcellId = pxp->delLink.nextMCId;

        if (delete_this_mcell) {

            /* check if there are frags to be deallocated */
            stats_ptr = (struct fs_stat *) bmtr_find (mcellp, BMTR_FS_STAT, 
                                                      vdp->dmnP); 
            /* we are assuming above that the BMTR_FS_STAT record 
               will remain in the primary mcell by calling bmtr_find().
               In case this assumption changes, we'd need to follow
               the mcell chain when looking for the record. */

            /* Furthermore if we do not find a BMTR_FS_STAT record then
             * we will assume that the mcell is a psuedo mcell from a 
             * truncation which will not have associated frags, and
             * therfore can be skipped
             */

            if ( stats_ptr ) {
                attrp = (bsBfAttrT *)bmtr_find(mcellp, BSR_ATTR, vdp->dmnP);
                if ( attrp == NULL ) {
                    /* Panic the domain */

                    bs_derefpg (pgRef, BS_CACHE_IT);
                    domain_panic(vdp->dmnP,
                             "del_clean_mcell_list: del_dealloc_stg_failed");
                    return (sts);
                }

                if ( attrp->state == BSRA_DELETING ) {
                    /*
                     * The final close never happened.
                     * Open the file and close it. This will be the last
                     * close that frees the storage and set quotas.
                     */
                    sts = ddl_complete_delete(mcellp->mcBfSetTag, mcellp->mcTag, vdp);
                    if ( sts == EOK ) {
                        /* The file has gone thru a final close. */
                        /* The storage has been freed. */
                        delete_this_mcell = FALSE;
                    } else {
                        /*
                        ** If ddl_complete_delete failed we can still free the
                        ** disk storage. However the quotas may be wrong.
                        */
                        printf("del_clean_mcell_list: ddl_complete_delete failed (%d)\n", sts);
                        printf("        Quotas in domain \"%s\", fileset tag %d may be wrong.\n",
                          vdp->dmnP->domainName, mcellp->mcBfSetTag);
                    }
                }
            }

            if ( delete_this_mcell == TRUE ) {
                if ((sts = del_dealloc_stg(mcellId, vdp)) != EOK) {
                    /* Panic the domain */

                    bs_derefpg (pgRef, BS_CACHE_IT);
                    domain_panic(vdp->dmnP,
                             "del_clean_mcell_list: del_dealloc_stg_failed");
                    return E_DOMAIN_PANIC;
                }
            }
        } /* end if delete_this_mcell */

        mcellId = nextMcellId;

        if ((sts = bs_derefpg (pgRef, BS_CACHE_IT)) != EOK) {
            return (sts);       /* XXX */
        }
    }

    return (EOK);
}

/*
 * The DDL is corrupted.  This is bad but it is not critical.
 * Inform the user that storage may be lost and ask him to
 * report the problem.  Then, patch the DDL header so the DDL
 * is empty.
 */
static statusT 
del_clean_fix_ddl( vdT *vdp, bfPageRefHT  pgRef )
{
    statusT sts;
    ftxHT ftxH;
    bsMPgT *bmtp;
    bsMCT* mcellp;
    delLinkRT *zlp;
    rbfPgRefHT bmtPgRef;
    bs_meta_page_t mcellListPage;

    mcellListPage = (bs_meta_page_t )MCELL_LIST_PAGE;

    ms_uaprintf("AdvFS: Corrupted deferred delete list in domain '%s'!\n",
                        vdp->dmnP->domainName);
    ms_uaprintf("       No data has been lost but some disk space\n");
    ms_uaprintf("       in this domain may no longer be usable.\n");

    /*
     * If AdvfsCaptureBadDDL (resettable on the fly via dbx)  is 1,
     * panic the domain.  This will allow us to capture the domain
     * in its corrupted state, if necessary.
     */
    if (AdvfsCaptureBadDDL == 1) {
        sts = bs_derefpg (pgRef, BS_RECYCLE_IT);
        domain_panic(vdp->dmnP, "Corrupted DDL!");
        return (E_DOMAIN_PANIC);
    }

    /*
     * Ignore the status of the deref; it's not critical.
     */
    sts = bs_derefpg (pgRef, BS_RECYCLE_IT);

    sts = FTX_START_N(FTA_BS_DEL_TRUNCATE_DDL, &ftxH, FtxNilFtxH, vdp->dmnP);
    if ( sts != EOK) {
        domain_panic(vdp->dmnP, "del_clean_fix_ddl: Can't start transaction");
        return E_DOMAIN_PANIC;
    }

    sts = rbf_pinpg(&bmtPgRef, (void **)&bmtp, vdp->bmtp,
                    mcellListPage, BS_NIL, ftxH, MF_VERIFY_PAGE);
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_370);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_371);
        domain_panic(vdp->dmnP, "del_clean_fix_ddl: Can't pin DDL header page");
        return E_DOMAIN_PANIC;
    }

    mcellp = &bmtp->bsMCA[MCELL_LIST_CELL];
    zlp = (delLinkT *)bmtr_find(mcellp,BSR_DEF_DEL_MCELL_LIST, vdp->dmnP);
    if ( zlp == NULL ) {
        ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_372);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_373);
        domain_panic(vdp->dmnP, "del_clean_fix_ddl: Can't find DDL hdr record");
        return E_DOMAIN_PANIC;
    }

    rbf_pin_record(bmtPgRef, (void *)(zlp), (int32_t) sizeof(delLinkRT));
    zlp->nextMCId = zlp->prevMCId = bsNilMCId;
    ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_156);

    ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_157);
    ftx_done_n(ftxH, FTA_BS_DEL_TRUNCATE_DDL);
    ADVFS_CRASH_RECOVERY_TEST(vdp->dmnP, AdvfsCrash_158);

    return EOK;
}

/*
 * Open the file on the DDL and perform the "last close". This will
 * credit the quotas to the owner of the file if the storage is deleted
 * during recovery.
 * This happens when a file that is deleted is held open (by another thread)
 * and the system crashes.
 */
static statusT
ddl_complete_delete( bfTagT setTag, bfTagT fileTag, vdT *vdp )
{
    statusT sts;
    domainT *dmnP = vdp->dmnP;
    bfSetT *bfSetp = NULL;
    bfSetIdT bfSetId;
    struct vnode *vp = NULL;
    bfAccessT *bfap = NULL;
    struct fsContext *cp = NULL;
    uint32_t i;
    fileSetNodeT *fsnp = NULL;
    quotaInfoT *qip;
    struct bfNode *bnp;

    bfSetId.domainId = dmnP->domainId;
    bfSetId.dirTag = setTag;

    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_IGNORE_DEL, FtxNilFtxH );
    if ( sts != EOK ) {
        return sts;
    }

    /*
    ** We are starting a faux mount. If filesetnode_init does not find the
    ** fileset mounted it will alloc fsnp and put it on FilesetHead.
    ** We must set up several conditions for following routines.
    ** 1) bs_get_bfset_params (from advfs_fs_quota_activate=>get_quota_config) 
    **    needs bfSetp->fsnp==null.
    ** 2) qi_init (from advfs_fs_quota_activate) needs bnp->accessp since qf_get
    **    (from advfs_fs_quota_activate) didn't set it since mp was null.
    */

    sts = filesetnode_init( bfSetp, &fsnp );

    if ( sts != EOK && sts != EBUSY ) {
        goto cleanup;
    }
    if ( sts == EOK ) {
        /* We opened the fileset. CFS is not holding it open. */
        MS_SMP_ASSERT(bfSetp->fsRefCnt == 1 );
        /* CFS does not have the FS open. We are doing a faux mount. */
        sts = advfs_fs_quota_activate( fsnp );
        if ( sts != EOK ) {
            goto cleanup;
        }

        bfSetp->fsnp = fsnp;
    } else {  /* EBUSY */
        /* CFS has this fileset open. fsnp not malloced. */
        MS_SMP_ASSERT(bfSetp->fsRefCnt > 1 );
        MS_SMP_ASSERT(bfSetp->fsnp != NULL);
    }

    /* Open the file on the deferred delete list to get the number of pages. */
    /* This open expects to have a v_count put on it, so it is not an
     * internal open */
    sts = bs_access_one( &bfap,
                         fileTag,
                         bfSetp,
                         FtxNilFtxH,
                         BF_OP_FIND_ON_DDL);
    vp = &bfap->bfVnode;
    if ( sts != EOK ) {
        goto cleanup;
    }

    MS_SMP_ASSERT(bfap != NULL);
    MS_SMP_ASSERT(&bfap->bfVnode== vp);

    /* CFS is not holding this file open, else we would not be here. 
     * We want to dummy up the fsContext structure.  */
    MS_SMP_ASSERT(bfap->bfSetp->fsnp != NULL);
    cp = &bfap->bfFsContext;
    cp->fsc_fsnp = bfap->bfSetp->fsnp;
    cp->dirstamp = 0;

    for (i = 0; i < MAXQUOTAS; i++) {
        cp->diskQuot[i] = NULLDQUOT;
    }
    cp->quotaInitialized = 1;

    ADVRWL_FILECONTEXT_WRITE_LOCK(cp);

    (void)advfs_fs_attach_quota( cp, FtxNilFtxH );

    cp->initialized = 1;
    ADVRWL_FILECONTEXT_UNLOCK(cp);


    /*
     * If the fileSetNode's mount pointer isn't yet initialized,
     * fill in the fileSetNode's mount pointer with what the vnode has.
     * This will satisfy any functions such as chk_blk_quota() 
     * that need a mount structure hanging off the fsnp.
     */
    bnp = (bfNodeT*)vp->v_data;
    bnp->accessp = bfap;

    bfap->file_size = cp->dir_stats.st_size;

cleanup:
    if ( vp ) {
        /* This call to VN_RELE is the heart of this routine. */
        /* This is the last close of the file. The quotas are */
        /* adjusted, the storage is freed, the tag is freed. */
        VN_RELE(&bfap->bfVnode);
    }

    if ( fsnp != NULL ) {

        ADVRWL_FILESETLIST_WRITE(&FilesetLock );

        *fsnp->fsPrev = fsnp->fsNext;
        if (fsnp->fsNext != NULL) {
            fsnp->fsNext->fsPrev = fsnp->fsPrev;
        }

        ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );

        MS_SMP_ASSERT(bfSetp->fsnp == fsnp);
        bfSetp->fsnp = NULL;

        /*
         * We should only be in here if we did an open on the quota
         * files during recovery.  The quota files were opened using a
         * complete faux mount meaning we used the advfs_meta_vfs structure
         * as the mount point (struct vfs).  We must make sure to not only
         * VN_RELE/close these files, but that bfs_close below invalidates
         * the access structures.
         * */
        if (fsnp->quotaStatus & QSTS_USR_QUOTA_MT) {
            VN_RELE(&fsnp->qi[USRQUOTA].qiAccessp->bfVnode);
        }
        if (fsnp->quotaStatus & QSTS_GRP_QUOTA_MT) {
            VN_RELE(&fsnp->qi[GRPQUOTA].qiAccessp->bfVnode);
        }

        for (qip = &fsnp->qi[0]; qip < &fsnp->qi[MAXQUOTAS]; qip++) {
            ADVMTX_QILOCK_DESTROY( &qip->qiLock );
        }
        ADVSMP_FILESET_DESTROY(&fsnp->filesetMutex);
        ADVSMP_SPACE_USED_DESTROY(&fsnp->spaceUsed_lock);

        ms_free( fsnp );
    }

    if ( bfSetp ) {
        bs_bfs_close( bfSetp, FtxNilFtxH, BFS_OP_DEF );
    }

    return sts;
}

/*
** Set up fileSetNode
** This is called from ddl_complete_delete which is not going to mount
** the fileset. We only need to do enough to satisfy bs_access_one & vrele.
**
** Returns EOK if filesetnode_init mallocs fsnp and adds it to FilesetHead.
** Returns EBUSY if fsnp exists and is on FilesetHead.
** Returns other errors for other problems.
*/

static statusT
filesetnode_init( bfSetT *bfSetp, fileSetNodeT **fsnpA )
{
    fileSetNodeT *fsnp;
    bfSetParamsT *bfSetParamsp;
    quotaInfoT *qip;
    statusT sts;
    bfSetParamsT bfSetParams;
    fileSetNodeT *fsp;

    MS_SMP_ASSERT(!BS_BFS_EQL(bfSetp->bfSetId, nilBfSetId));

    fsnp = (fileSetNodeT*)ms_malloc(sizeof(fileSetNodeT));
    fsnp->filesetMagic = FSMAGIC;
    fsnp->bfSetId = bfSetp->bfSetId;
    fsnp->bfSetp = bfSetp;
    fsnp->dmnP = bfSetp->dmnP;
    /*
     * This is done to make the domain look valid for a file open.  Make sure that any opens
     * done on this vfs structure (for files) are closed before continuing
     * in the mount processes.
     */
    fsnp->vfsp = &advfs_meta_vfs;

    ADVRWL_FILESETLIST_WRITE(&FilesetLock);
    for ( fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext ) {
        if ( BS_BFS_EQL(bfSetp->bfSetId, fsp->bfSetId) ) {
            ADVRWL_FILESETLIST_UNLOCK(&FilesetLock);
            /* For non cluster, we are the only accessor of this fs. */
            /* If this fs is accessed we must be in a cluster. */
            /*
             * If CFS has this fileset mounted during this recovery
             * it cannot become unmounted until the recovery is complete.
             * So bfSetp->fsnp is not going to disappear.
             */
            MS_SMP_ASSERT(clu_is_ready());
            ms_free(fsnp);
            return EBUSY;
        }
    }

    /*
     * Link this fileset into the list of mounted filesets.
     * Racing mounts will fail.
     */
    if (FilesetHead != NULL) {
        FilesetHead->fsPrev = &fsnp->fsNext;
    }
    fsnp->fsNext = FilesetHead;
    fsnp->fsPrev = &FilesetHead;
    FilesetHead = fsnp;

    ADVRWL_FILESETLIST_UNLOCK(&FilesetLock);

    bfSetParamsp = &bfSetParams;
    sts = bs_get_bfset_params( bfSetp, bfSetParamsp, 0 );
    if ( sts != EOK ) {
        /*
         * Need to remove faux filesetnode from the FilesetHead list before
         * freeing the memory.
         */
        ADVRWL_FILESETLIST_WRITE(&FilesetLock );
        *fsnp->fsPrev = fsnp->fsNext;
        if (fsnp->fsNext != NULL) {
            fsnp->fsNext->fsPrev = fsnp->fsPrev;
        }
        ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );

        ms_free(fsnp);
        return sts;
    }

    fsnp->fsFlags = 0;

    fsnp->rootTag.tag_num = ROOT_FILE_TAG;
    fsnp->rootTag.tag_seq = 1;
    fsnp->tagsTag.tag_num = TAGS_FILE_TAG;
    fsnp->tagsTag.tag_seq = 1;

    fsnp->quotaStatus = bfSetParamsp->quotaStatus &
                        ~(QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN);
    fsnp->fileHLimit = bfSetParamsp->fileHLimit;
    fsnp->fileSLimit = bfSetParamsp->fileSLimit;
    fsnp->fileTLimit = bfSetParamsp->fileTLimit;
    fsnp->blkSLimit = bfSetParamsp->blkSLimit;
    fsnp->blkTLimit = bfSetParamsp->blkTLimit;
    fsnp->blkHLimit = bfSetParamsp->blkHLimit;

    fsnp->qi[USRQUOTA].qiTag.tag_num = USER_QUOTA_FILE_TAG; 
    fsnp->qi[USRQUOTA].qiTag.tag_seq = 1;
    fsnp->qi[GRPQUOTA].qiTag.tag_num = GROUP_QUOTA_FILE_TAG;
    fsnp->qi[GRPQUOTA].qiTag.tag_seq = 1; 

    ADVSMP_FILESET_INIT( &fsnp->filesetMutex );
    ADVSMP_SPACE_USED_INIT( &fsnp->spaceUsed_lock );

    for ( qip = &fsnp->qi[0]; qip < &fsnp->qi[MAXQUOTAS]; qip++ ) {
        ADVMTX_QILOCK_INIT( &qip->qiLock );
    }

    fsnp->rootAccessp = NULL;
    fsnp->root_vp = NULL;
    bzero( (void*)&fsnp->fileSetStats, sizeof(fileSetStatsT) );

    /*
     * Make this faux mount look real from the perspective of
     * advfs_lookup_valid_bfap.  When an access structure is opened, the
     * fsnp->vfsp will be grabbed out of the bfSet.  Therefore, we need to
     * make sure the bfSet points to the fsnp.
     */
    bfSetp->fsnp = fsnp;

    *fsnpA = fsnp;

    return EOK;
}

/*
 * Clear all of the SBM bits owned by extent records in the normal
 * chain of mcells.  The primary and all chained mcells are freed.
 *       
 * This function is implemented as a sequence of independent root 
 * transactions, with each transaction preserving consistency.
 *
 * The caller is resposible for preventing races.  For bitfile "delete"
 * the mcellList_lk in the access structure is used.  If file migration
 * uses this function some other locking scheme will be needed since
 * there is usually no access structure  available at migration time.
 *
 * Caller must not hold any locks.
 */

statusT
del_dealloc_stg(
    bfMCIdT pmcid,      /* in - primary mcell ID */
    vdT *pvdp           /* in - virtual disk of primary mcell */
    ) 
{
    domainT *dmnP;
    vdT *vdp;
    bfMCIdT mcid;
    bfMCIdT nextMCId;
    bfPageRefHT pgref;
    bsMPgT* bmtp;
    bsMCT* mcp;
    bsXtntRT *pxp;      /* primary xtnt ptr */
    vdIndexT vdIndex;
    statusT sts;
    int mutexisLocked =0;
    bsBfAttrT *attrp;

    dmnP = pvdp->dmnP;

    /*
     * Indicate one more entry is actively being processed.
     */
    ADVRWL_DDLACTIVE_READ( &(pvdp->ddlActiveLk) );

    if ((sts = bs_refpg(&pgref,
                        (void **)&bmtp,
                        pvdp->bmtp,
                        pmcid.page,
                        FtxNilFtxH,
                        MF_VERIFY_PAGE)) != EOK) 
    {
        domain_panic( dmnP,
                      "del_dealloc_stg: Can't ref BMT page");
        RAISE_EXCEPTION( sts );
    }

    mcp = &bmtp->bsMCA[pmcid.cell];

    /*
     * Find the primary xtnt record.
     */
    pxp = (bsXtntRT *)bmtr_find(mcp, BSR_XTNTS, pvdp->dmnP);
    if ( pxp == NULL ) 
    {
        
        domain_panic( dmnP,
                      "del_dealloc_stg: no primary xtnt rec");
        
        bs_derefpg(pgref, BS_CACHE_IT);

        RAISE_EXCEPTION( ENO_XTNTS );
    }

    /*
     * First extent is always stored in the primary mcell.
     */
    if (pxp->xCnt > 1) {
        mcid = pmcid;
        MS_SMP_ASSERT(FETCH_MC_VOLUME(mcid) == pvdp->vdIndex);
    }
    else {
        mcid = pxp->chainMCId;
    }
    vdIndex = FETCH_MC_VOLUME(mcid);

    bs_derefpg(pgref, BS_CACHE_IT);

    if ( vdIndex != 0 ) {
        /*
         * Since the extent chain is nonempty, the bitfile must have
         * storage.  Clear these SBM bits.
         */
        vdp = VD_HTOP(vdIndex, dmnP);
    
        /*
         * Loop over all the chained extent records.  SBM bits in the
         * delete range are cleared.
         */

        while (1) {
            if ((sts = del_xtnt_array(pmcid, mcid, pvdp, vdp, 
                                      &nextMCId)) != EOK) {
                domain_panic( dmnP,
                              "del_dealloc_stg: Can't clear xtnt array");
                RAISE_EXCEPTION( sts );
            }
            if (!nextMCId.volume) {
                break;
            }
            mcid = nextMCId;
            vdp = VD_HTOP(mcid.volume, dmnP);
        }
    }

    bmt_free_bf_mcells( pvdp, pmcid, FtxNilFtxH, TRUE );

    ADVRWL_DDLACTIVE_UNLOCK( &(pvdp->ddlActiveLk) );

    /*
     * If a thread is waiting on the completion of this entry, wake it.
     */


    ADVMTX_DOMAIN_LOCK (&(dmnP->dmnMutex));
    mutexisLocked = 1;

    if ((pmcid.page == pvdp->ddlActiveWaitMCId.page) &&
        (pmcid.cell == pvdp->ddlActiveWaitMCId.cell)) {

        cv_broadcast(&pvdp->ddlActiveWaitCv, NULL, CV_NULL_LOCK);
        ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));
        mutexisLocked = 0;
    }

    if ( mutexisLocked ) ADVMTX_DOMAIN_UNLOCK (&(dmnP->dmnMutex));

    return (EOK);

HANDLE_EXCEPTION:

    ADVRWL_DDLACTIVE_UNLOCK( &(pvdp->ddlActiveLk) );
    return(sts);
}

/*
 * Process the array of extents in a xtra or shadow extent 
 * array (within a single mcell), clearing SBM bits for the extents 
 * lying in the specified range.  One or more root transactions will 
 * be started and committed.  After each root transaction, the 
 * on-disk data structures will be consistent.
 *
 * Each root transaction saves restart information in the primary
 * extent record to be used during crash recovery.
 */

static statusT
del_xtnt_array(
    bfMCIdT pmcid,          /* in - mcell ID of primary mcell */
    bfMCIdT mcid,           /* in - mcell ID of extra or shadow xtnts record */
    vdT *pvdp,              /* in - pointer to virtual disk of primary */
    vdT *vdp,               /* in - pointer to virtual disk of xtra or shad */
    bfMCIdT *nextMCId       /* out - mcell ID of next mcell */
    )
{
    bfPageRefHT refPgH;
    bsXtntT *xp, *fxp;
    int xCnt;
    int pinPages;
    bsMPgT* bmtp;
    bsMCT *mcp;
    bsXtntRT *prp;
    bsXtraXtntRT *xrp;
    delFtxDescT dsc;
    statusT sts;
    domainT *dmnP;
    int pgRefed = FALSE;

    *nextMCId = bsNilMCId;
    dmnP = vdp->dmnP;

    if ((sts = bs_refpg(&refPgH,
                        (void **)&bmtp,
                        vdp->bmtp, 
                        mcid.page,
                        FtxNilFtxH,
                        MF_VERIFY_PAGE)) != EOK) {
       domain_panic(dmnP, "del_xtnt_array: Can't ref BMT page for chain mcell");
       RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }
    pgRefed = TRUE;
    mcp = &bmtp->bsMCA[mcid.cell];

    if ((xrp = (bsXtraXtntRT *)bmtr_find(mcp, BSR_XTRA_XTNTS, vdp->dmnP))
                                                                != NULL) {
        fxp = &xrp->bsXA[0];
        xCnt = xrp->xCnt;
        *nextMCId = mcp->mcNextMCId;
    }
    else if ((prp = (bsXtntRT *)bmtr_find(mcp, BSR_XTNTS, vdp->dmnP)) != NULL) {
        fxp = &prp->bsXA[0];
        xCnt = prp->xCnt;
        *nextMCId = prp->chainMCId;
    }
    else {
        domain_panic(dmnP, "del_xtnt_array: No prim or xtra extents");
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    del_ftx_start(pmcid, pvdp, &dsc);

    if (MCID_EQL(dsc.rstMCId, bsNilMCId)) {
        if (dsc.rstIndex == NORESTART) {
            /*
             * This condition arises when we've previously deallocated
             * all the storage for this bitfile but the system crashed
             * before the corresponding primary mcell was removed from
             * the deferred delete chain.  There is nothing to do.
             */
            bs_derefpg(refPgH, BS_CACHE_IT);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_159);
            ftx_done_n(dsc.ftxH, FTA_BS_DEL_FTX_START_V1);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_160);
            return (EOK);
         }
         else {
            /*
             * This should only happen the first time we've been called
             * on behalf of this primary mcell.  (rstMCId is set to nil
             * and rstIndex is set to 0 when the mcell is placed on the 
             * deferred delete chain.)
             * 
             * Make sure that all the initial values in the delFtxDescT
             * structure have correct initial values.
             */
            dsc.rstMCId = mcid;
        }
    }
    else if (!MCID_EQL(dsc.rstMCId, mcid)) {
        /*
         * This condition should only arise during crash recovery.  The
         * primary mcell must have had more than one chained extent
         * mcell, and at least one mcell on this list has been fully
         * processed.  If there are more mcells on the chain, we 
         * expect to be called back with mcid pointing to the next mcell.
         *
         * Note that there is no need to call del_ftx_done because no
         * on-disk state needs to be updated.
         */
        bs_derefpg(refPgH, BS_CACHE_IT);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_161);
        ftx_done_n(dsc.ftxH, FTA_BS_DEL_FTX_START_V1);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_162);

        return (EOK);
    }

    pinPages = DEL_MAX_PAGES - 1; /* One page pinned by del_ftx_start */

    FTX_LOCKMUTEX( &vdp->stgMap_lk, dsc.ftxH);

    for (xp = fxp + dsc.rstIndex; xp <= fxp + xCnt - 2; xp++) {
        bf_fob_t start_fob;   /* start fob */
        bf_fob_t end_fob;     /* end fob */
        bf_fob_t fob_cnt;     /* number of fobs */
        bf_vd_blk_t start_vd_blk;         /* start block */
        bf_vd_blk_t vd_blk_cnt;           /* number of blocks */
        bf_vd_blk_t tmp_vd_blk;           /* scratch variable */

        start_vd_blk = xp->bsx_vd_blk;
        if ( start_vd_blk == XTNT_TERM || start_vd_blk== COWED_HOLE ) {
            continue;
        }

        start_fob = xp->bsx_fob_offset + dsc.dfdRstFobOffset;
        fob_cnt = (xp + 1)->bsx_fob_offset - start_fob;
        end_fob = start_fob + fob_cnt - 1;

        /*
         * At this point [start_fob, end_fob] describes an interval in the bitfile
         * whose SBM storage is to be freed.  Convert this to an 
         * interval [start_vd_blk, start_vd_blk + vd_blk_cnt- 1] which describes the corresponding SBM
         * interval.
         */
        start_vd_blk += (start_fob - xp->bsx_fob_offset) / ADVFS_FOBS_PER_DEV_BSIZE;
        vd_blk_cnt = fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE;

        while (1) {
            tmp_vd_blk = vd_blk_cnt;
            if ((sts = del_range(start_vd_blk, &tmp_vd_blk, &pinPages, 
                            vdp, dsc.dfdPgSize, dsc.ftxH)) != EOK) {
                ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_374);
                ftx_fail( dsc.ftxH );
                ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_375);
                domain_panic(dmnP, "del_xtnt_array: Can't clear range");
                RAISE_EXCEPTION(E_DOMAIN_PANIC);
            }

            vd_blk_cnt -= tmp_vd_blk;
            start_vd_blk += tmp_vd_blk;

            if (vd_blk_cnt == 0) {
                /*
                 * Finished this xtnt map entry.  Set up restart to
                 * begin at start of the next entry.
                 */
                dsc.rstIndex = (xp - fxp) + 1;
                dsc.dfdRstFobOffset = 0;
            }
            else {
                /*
                 * Could not finish this extent map entry.  Record
                 * the page offset relative to the start of the
                 * extent.
                 */
                dsc.rstIndex = xp - fxp;
                MS_SMP_ASSERT(((start_vd_blk - xp->bsx_vd_blk) % 
                                dsc.dfdPgSize) == 0);
                /* Implicit assumption that vd blk sz = fob sz = 1k */
                dsc.dfdRstFobOffset = (start_vd_blk - xp->bsx_vd_blk) * 
                    ADVFS_FOBS_PER_DEV_BSIZE;
            }

            if (pinPages > 0) {
                /* 
                 * This break is a little tricky.  It relies on this
                 * inner while loop being the last statement in the
                 * enclosing for loop.  Thus, go back to the top of
                 * the for loop.
                 */
                break;
            }

            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_163);
            del_ftx_done(&dsc);
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_164);
            del_ftx_start(pmcid, pvdp, &dsc);

            FTX_LOCKMUTEX( &vdp->stgMap_lk, dsc.ftxH);

            /*
             * Reset maximum number of pages that may be pinned.
             */
            pinPages = DEL_MAX_PAGES - 1;

            if (vd_blk_cnt == 0) {
                break;  /* to top of for loop */
            }
        }       /* end of while loop */
    }   /* end of for loop */

    /*
     * Setup the delete descriptor record to start with the next mcell
     * in the extent chain.
     */
    dsc.rstMCId = *nextMCId;
    dsc.rstIndex = 0;
    dsc.dfdRstFobOffset = 0;

    if (MCID_EQL(dsc.rstMCId, bsNilMCId)) {
        /*
         * Indicate that we're all done.
         */
        dsc.rstIndex = NORESTART;
    }      
  
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_165);
    del_ftx_done(&dsc);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_166);
    bs_derefpg(refPgH, BS_CACHE_IT);

    return (EOK);

HANDLE_EXCEPTION:

    if (pgRefed) {
        bs_derefpg(refPgH, BS_CACHE_IT);
    }

    return (sts) ;
}


/*
 * Zero a range of pages in the storage bit map.
 */

static statusT
del_range(
    bf_vd_blk_t start_vd_blk,  /* in - start of range to zero */
    bf_vd_blk_t *vd_blk_cnt,   /* in- number of blocks to free
                                * out - number of blocks actually freed */
    int *pinPages,      /* in/out - maximum number of pages that can be
                           pinned / number of pages that were pinned */
    vdT *vdp,           /* in - virtual disk containing range */
    bf_fob_t pgSz,           /* in - page size in 1k fobs */
    ftxHT ftxH          /* in transaction handle */
    )
{
    statusT sts;

    sbm_howmany_blks(start_vd_blk, vd_blk_cnt, pinPages, vdp, pgSz);

    if (*vd_blk_cnt> 0) {
        if ((sts = sbm_return_space_no_sub_ftx(vdp, 
                                               start_vd_blk, 
                                               *vd_blk_cnt, 
                                               ftxH))
          != EOK) {
            ADVFS_SAD0 ("del_range: could not return SBM space");
        }
    }

    return EOK;
}

/*
 * Start a transaction and pin the page containing the primary mcell.
 * This page is kept pinned until del_ftx_done is called.  All fields
 * in the delFtxDescT structrure are initialized.
 */

static statusT
del_ftx_start(
    bfMCIdT pmcid,
    vdT *pvdp,
    delFtxDescT *ddp
    )
{
    bsMPgT* bmtp;
    bsMCT *pmcp;
    bsXtntRT *rp;
    statusT sts;
    bsBfAttrT *bfattrp;

    if ((sts = FTX_START_N(FTA_BS_DEL_FTX_START_V1, &ddp->ftxH, FtxNilFtxH,
                           pvdp->dmnP)) != EOK) {
        ADVFS_SAD0 ("del_ftx_start: Can't start transaction");
    }

    if ((sts = rbf_pinpg(&ddp->pinPgH, (void **)&bmtp, pvdp->bmtp, 
                         pmcid.page, BS_NIL, ddp->ftxH, MF_VERIFY_PAGE)) != EOK) {
        ADVFS_SAD0 ("del_ftx_start: Can't ref BMT page for primary mcell");
    }

    pmcp = &bmtp->bsMCA[pmcid.cell];

    /*
     * Find the primary attributes record to get the bfPgSz 
     */
    if ((bfattrp = (bsBfAttrT *) bmtr_find(pmcp, BSR_ATTR, pvdp->dmnP)) == NULL) {
       ADVFS_SAD0 ("del_ftx_start: no primary attributes record") ;
    }
    
    /*
     * Find the primary xtnt record.
     */
    if ((rp = (bsXtntRT *) bmtr_find(pmcp, BSR_XTNTS, pvdp->dmnP))
                                                                == NULL) {
        ADVFS_SAD0 ("del_ftx_start: no primary xtnt record");
    }
    ddp->pxp = rp;
    ddp->dfdRstFobOffset = rp->delRst.drtFobOffset;
    ddp->rstIndex = rp->delRst.xtntIndex;
    ddp->rstMCId = rp->delRst.mcid;
    ddp->dfdPgSize = bfattrp->bfPgSz;

    return (EOK);
}

static void
del_ftx_done(
    delFtxDescT *ddp     
    )
{
    rbf_pin_record(ddp->pinPgH, (void *)&ddp->pxp->delRst, 
                   (int32_t) sizeof(ddp->pxp->delRst));

    ddp->pxp->delRst.xtntIndex = ddp->rstIndex;
    ddp->pxp->delRst.drtFobOffset = ddp->dfdRstFobOffset;
    ddp->pxp->delRst.mcid = ddp->rstMCId;

    ADVFS_CRASH_RECOVERY_TEST(ddp->ftxH.dmnP, AdvfsCrash_167);
    ftx_done_n(ddp->ftxH, FTA_BS_DEL_FTX_START_V1);
    ADVFS_CRASH_RECOVERY_TEST(ddp->ftxH.dmnP, AdvfsCrash_168);
}

/*
 * del_find_del_entry
 *
 * This function scans the deferred-delete list for the an mcell that matches
 * the specified bfSetTag and bfTag.  It returns EOK if found or ENO_MORE_MCELLS
 * if not found.
 */

statusT
del_find_del_entry (
                    domainT *domain,  /* in */
                    vdIndexT vdIndex,  /* in */
                    bfTagT bfSetTag,  /* in */
                    bfTagT bfTag,  /* in */
                    bfMCIdT *delMcellId,  /* out */
                    int *delFlag  /* out */
                    )
{
    bsMPgT *bmt;
    delLinkT *delListHead;
    int derefFlag = 0;
    int dFlag;
    bsMCT *mcell;
    bfTagT mcellBfSetTag;
    bfTagT mcellBfTag;
    bfMCIdT mcellId;
    bfMCIdT nextMcellId;
    bfPageRefHT pgRef;
    statusT sts;
    int unlockFlag = 0;
    vdT *vd;
    bsXtntRT *xtntRec;
    bs_meta_page_t pgnum;

    vd = VD_HTOP(vdIndex, domain);

    ADVFTX_VDT_DELLIST_WRITE(&(vd->del_list_lk) );

    unlockFlag = 1;

    /*
     * Examine the first entry on the deferred-delete list.
     */

    pgnum = MCELL_LIST_PAGE;

    sts = bs_refpg(&pgRef,
                   (void *)&bmt,
                   vd->bmtp,
                   pgnum,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    derefFlag = 1;

    mcell = &(bmt->bsMCA[MCELL_LIST_CELL]);

    delListHead = (delLinkT *) bmtr_find (mcell, BSR_DEF_DEL_MCELL_LIST,
                                          vd->dmnP);
    if (delListHead == NULL) {
        ADVFS_SAD0 ("del_find_del_entry: no deferred-delete listhead");
    }
    mcellId = delListHead->nextMCId;

    derefFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    while ((mcellId.page != bsNilMCId.page) ||
           (mcellId.cell != bsNilMCId.cell)) {
        sts = bs_refpg(&pgRef,
                       (void *)&bmt,
                       vd->bmtp, 
                       mcellId.page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        mcell = &(bmt->bsMCA[mcellId.cell]);
        xtntRec = bmtr_find (mcell, BSR_XTNTS, vd->dmnP);
        if (xtntRec == NULL) {
            ADVFS_SAD0 ("del_find_del_entry: no extent record");
        }

        /*
         * Determine what put this entry onto the list.
         *
         * NOTE: There should perhaps be a better way of doing this.  Perhaps
         * an id in the entry.
         */

        if (bmtr_find (mcell, BSR_ATTR, vd->dmnP) != NULL) {
            dFlag = 1;
        } else {
            dFlag = 0;
        }

        nextMcellId = xtntRec->delLink.nextMCId;
        mcellBfSetTag = mcell->mcBfSetTag;
        mcellBfTag = mcell->mcTag;

        sts = bs_derefpg (pgRef, BS_CACHE_IT);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        if (BS_BFTAG_EQL (bfSetTag, mcellBfSetTag) &&
            BS_BFTAG_EQL (bfTag, mcellBfTag)) {
            break;  /* out of while */
        }

        mcellId = nextMcellId;

    }  /* end while */

    unlockFlag = 0;
    ADVFTX_VDT_DELLIST_UNLOCK(&(vd->del_list_lk) );

    if ((mcellId.page == bsNilMCId.page) && (mcellId.cell == bsNilMCId.cell)) {
        sts = ENO_MORE_MCELLS;
    } else {
        *delMcellId = mcellId;
        *delFlag = dFlag;
        sts = EOK;
    }

    return sts;

HANDLE_EXCEPTION:

    if (derefFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }
    if (unlockFlag != 0) {
        ADVFTX_VDT_DELLIST_UNLOCK(&(vd->del_list_lk) );
    }

    return sts;

}  /* end del_find_del_entry */




