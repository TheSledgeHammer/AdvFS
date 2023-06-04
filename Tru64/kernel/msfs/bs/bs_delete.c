/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993                      *
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
 *  ADVFS Storage System.
 *
 * Abstract:
 *
 *  bs_delete.c
 *  This module contains the top level bitfile delete routines.
 *
 * Date:
 *
 *  Fri Sep 21 08:33:50 1990
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_delete.c,v $ $Revision: 1.1.173.11 $ (DEC) $Date: 2007/05/30 15:27:58 $"

#include <sys/param.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_bmt.h>
#include <msfs/bs_delete.h>
#include <msfs/bs_stg.h>
#include <msfs/ms_osf.h>
#include <sys/user.h>
#include <sys/ucred.h>
#include <kern/lock.h>
#include <sys/lock_probe.h>
#include <msfs/bs_params.h>
#include <msfs/bs_stripe.h>
#include <msfs/bs_index.h>

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
#define NORESTART ((uint32T) -1)

/*
 * Returns nonzero iff two mcell IDs are equal.
 */
#define MCIDEQL(i1, i2) (((i1).page == (i2).page) && ((i1).cell == (i2).cell))

#define  DELLIST_LOCK_WRITE( lk ) { \
    lock_write( lk.lock );\
}
#define  DELLIST_UNLOCK( lk ) { \
    lock_done( lk.lock );\
}

/* Internal Types */

/*
 * delOpRecT 
 *
 * Defines a deleted bitfile by its tag, mcid and bitfile set.
 */
typedef struct {
    bfTagT tag;
    bfMCIdT mcid;
    uint32T vdIndex;
    bfSetIdT bfSetId;
    lkStatesT prevAccState;
    bfStatesT prevBfState;
    uint32T setBusy;
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
    bfMCIdT mcid;
    uint32T vdIndex;
    uint32T type;
} delListRecT;

/*
 * Structure to describe a delete ftx transaction.
 */
typedef struct {
    int pgSz;             /* page size of bitfile (not SBM) */
    bsXtntRT *pxp;        /* primary extent record pointer */
    uint32T rstIndex;     /* xtnt to start with */
    uint32T rstOffset;    /* offset into start xtnt */
    bfMCIdT rstMCId;      /* mcell to start with */
    vdIndexT rstVdIndex;  /* vdIndex of above mcell */
    rbfPgRefHT pinPgH;    /* page handle */
    ftxHT ftxH;           /* transaction handle */
} delFtxDescT;

/* Internal prototypes. */

statusT
rbf_delete_int(
    bfAccessT* bfap,
    ftxHT parentFtxH
    );

void
reset_ondisk_bf_state (
                       domainT *dmnP,  /* in */
                       vdIndexT vdIndex,  /* in */
                       bfMCIdT mcellId,  /* in */
                       bfStatesT bfState,  /* in */
                       ftxHT ftxH  /* in */
                       );

void
xfer_xtnts_to_clone (
                     bfMCIdT  pmcid,  /* "primary mcell" id of delete list */
                     vdT      *pvdp,  /* "primary vd" of delete list */
                     bsMCT    *mcp,   /* "primary mcell" of delete list */
                     bsXtntRT *pxp    /* "primary xtnt record" of delete list */
                    );

static void
del_part_xtnt( bfMCIdT pmcid,     /* in - mcell ID of primary mcell */
               vdT *pvdp,         /* in - pointer to virtual disk of primary */
               vdT *vdp,          /* in - virtual disk of storage to free */
               bsXtntT *fxp,      /* in - xtnt that maps storage to free */
               uint32T startpg,   /* in - first page in xtnt to free */
               uint32T pgstofree, /* in - number of pages to free */
               delFtxDescT *dscp);

static statusT
del_range(
    uint32T start,      /* in - start of range to zero */
    uint32T *count,     /* in/out - number of blocks to free / number of
                           blocks actually freed */
    int *pinPages,      /* in/out - maximum number of pages that can be
                           pinned / number of pages that were pinned */
    vdT *vdp,           /* in - virtual disk containing range */
    int pgSz,           /* in - page size */
    ftxHT ftxH          /* in transaction handle */
    );

static statusT
del_xtnt_array(
    bfMCIdT pmcid,          /* in - mcell ID of primary mcell */
    bfMCIdT mcid,           /* in - mcell ID of extra or shadow xtnts record */
    vdT *pvdp,              /* in - pointer to virtual disk of primary */
    vdT *vdp,               /* in - pointer to virtual disk of xtra or shad */
    vdIndexT *nextVdIndex,  /* out - vd index of next mcell */
    bfMCIdT *nextMCId       /* out - mcell ID of next mcell */
    );

static statusT
pin_link(
    bfAccessT* bmtbfap,
    bfMCIdT mcid,
    delLinkT **zlp,
    vdT *vdp,               /* in - pointer to virtual disk */
    ftxHT ftxH
    );

static statusT
pin_hdr(
    bfAccessT* bmtbfap,
    delLinkT **zlp,
    vdT *vdp,           /* in - Volume Pointer */
    ftxHT ftxH
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
    defDelOpRecT defDelOpRec = *(defDelOpRecT *) opRec;
    statusT sts = EOK;
    bsBfAttrT bfAttr;
    bfSetT *bfSetp=NULL;
    bfAccessT *bfap=NULL;
    domainT *dmnP;
    struct vnode *nullvp = NULL;

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

    sts = rbf_bfs_open(&bfSetp, defDelOpRec.bfSetId, BFS_OP_IGNORE_DEL, ftxH);
    if (sts != EOK) { 
      domain_panic(ftxH.dmnP,"deferred delete undo: rbf_bfs_open() failed");
      goto HANDLE_EXCEPTION;
    }

    sts = bs_access(&bfap, defDelOpRec.tag, bfSetp, ftxH, 0, NULLMT, &nullvp);
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
    BFSETTBL_LOCK_WRITE( dmnP )

    sts = bmtr_get_rec_n_lk( bfap,
                             BSR_ATTR, 
                             &bfAttr, 
                             sizeof( bfAttr ),
                             BMTR_LOCK );
    if (sts != EOK) {
        ADVFS_SAD1 ("deferred delete undo: get attr err",sts);
    }

    bfap->deleteWithClone = defDelOpRec.prevDeleteWithClone;
    bfAttr.deleteWithClone = defDelOpRec.prevDeleteWithClone;

    sts = bmtr_update_rec_int( bfap,
                               BSR_ATTR, 
                               &bfAttr, 
                               sizeof( bfAttr ),
                               ftxH,
                               BMTR_UNLOCK,
                               0 );
    if (sts != EOK) {
        ADVFS_SAD1 ("deferred delete undo: set attr err", sts);
    }

HANDLE_EXCEPTION:
    if (bfap) bs_close(bfap, 0);
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
    domainT *dmnP;
    ftxHT ftxH;
    defDelOpRecT defDelOpRec;
    int cnt;
    bfSetT *bfSetp;
    bsBfAttrT bfAttr;
    int setBusy = FALSE, ftxStarted = FALSE;
    
    dmnP = bfap->dmnP;
    bfSetp = bfap->bfSetp;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return E_READ_ONLY;
    }

    if (parentFtxH.hndl == 0) {
        sts = FTX_START_N(FTA_BS_DEL_DEFERRED_V1, &ftxH, parentFtxH, 
                          bfap->dmnP, 1 );
        if (sts != EOK) {
            ADVFS_SAD1 ("rbf_delete: ftx start failed", sts);
        }

        ftxStarted = TRUE;
    } else {
        ftxH = parentFtxH;
    }
    
    COW_LOCK_WRITE( &bfap->cow_lk )
    if (bs_have_clone( bfap->tag, bfSetp->cloneSetp, ftxH )) {

        /*
         * We are deleting the original bitfile.  However, it
         * has a clone that depends on it.  So, we simply mark
         * "delete with clone" for now.  When the clone is deleted then
         * the original will also be deleted.  We also update the quotas
         * since the file is in effect deleted we don't want to charge
         * the user for the file and disk space anymore.
         */

        if (!ftxStarted) {
            sts = FTX_START_N(FTA_BS_DEL_DEFERRED_V1, &ftxH, parentFtxH, 
                              bfap->dmnP, 1 );
            if (sts != EOK) {
                ADVFS_SAD1 ("rbf_delete: ftx start failed", sts);
            }
        }

        if ((IDX_INDEXING_ENABLED(bfap)) && (!IDX_FILE_IS_DIRECTORY(bfap)) ) {
            /* This is an index file. Just update block quotas */

            change_quotas(((bsIdxRecT *)bfap->idx_params)->dir_bfap->bfVp, 0,
                       -((long)bfap->xtnts.allocPageCnt * ADVFS_PGSZ_IN_BLKS),
                       NULL, u.u_cred, 0, ftxH);
        } else if ( bfap->bfVp ) {
            long totalblks;
            totalblks = (long)bfap->xtnts.allocPageCnt * ADVFS_PGSZ_IN_BLKS;

            if (bfap->fragState == FS_FRAG_VALID)
                     totalblks += bfap->fragId.type * 2;
            change_quotas(bfap->bfVp, -1, -totalblks, NULL, u.u_cred, 0, ftxH);
        }

        sts = bmtr_get_rec_n_lk( bfap, 
                                 BSR_ATTR, 
                                 &bfAttr, 
                                 sizeof( bfAttr ),
                                 BMTR_LOCK );
        if (sts != EOK) {
            COW_UNLOCK( &bfap->cow_lk )
            domain_panic(bfap->dmnP, "rbf_delete: bmtr_get_rec_n_lk failed, return code = %d", sts);
            if ( ftxStarted )
                ftx_fail(ftxH);
            return(E_DOMAIN_PANIC);
        }
    
        defDelOpRec.prevDeleteWithClone = bfAttr.deleteWithClone;
        bfap->deleteWithClone = TRUE;
        bfAttr.deleteWithClone = TRUE;
        COW_UNLOCK( &bfap->cow_lk )
 
        sts = bmtr_put_rec_n_unlk_int( bfap, 
                                       BSR_ATTR, 
                                       &bfAttr, 
                                       sizeof( bfAttr ),
                                       ftxH,
                                       BMTR_UNLOCK,
                                       0 );
        if (sts != EOK) {
            domain_panic(bfap->dmnP, "rbf_delete: bmtr_put_rec_n_unlk_int failed, return code = %d", sts);
            if ( ftxStarted )
                ftx_fail(ftxH);
            return(E_DOMAIN_PANIC);
        }
    
        defDelOpRec.tag = bfap->tag;
        defDelOpRec.bfSetId = bfSetp->bfSetId;

        ftx_done_u( ftxH, FTA_BS_DEL_DEFERRED_V1,
                    sizeof( defDelOpRecT ), &defDelOpRec );

        return EOK;

    } else {
        COW_UNLOCK( &bfap->cow_lk )
        if (ftxStarted) {
            ftx_quit( ftxH );
        }

        sts = rbf_delete_int( bfap, parentFtxH );

        return (sts);
    }

    /* NOT REACHED */
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

statusT
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
    bfSetT *bfSetp;
      
    bfSetp = bfap->bfSetp;
    dmnP   = bfap->dmnP;

    if (parentFtxH.hndl == 0) {
        ftxFlag = 1;
        sts = FTX_START_N(FTA_BS_DEL_DELETE_V1, &ftxH, parentFtxH, 
                          bfap->dmnP, 0);
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
    mutex_lock(&bfap->bfaLock);

    lk_wait_for (&(bfap->stateLk), &bfap->bfaLock, ACC_VALID);
    delOpRec.prevAccState = ACC_VALID;
    (void) lk_set_state(&bfap->stateLk, ACC_INIT_TRANS);

    delOpRec.prevBfState = bfap->bfState;
    bfap->bfState = BSRA_DELETING;

    mutex_unlock(&bfap->bfaLock);

    sts = FTX_START_N(FTA_BS_DEL_DELETE_V1, &subFtxH, ftxH, 
                      bfap->dmnP, 1);
    if (sts != EOK) {
        ADVFS_SAD1 ("rbf_delete_int: ftx start failed", sts);
    }

    /*
     * Set the bitfile attribute state to BSRA_DELETING.
     */

    vdp = VD_HTOP(bfap->primVdIndex, dmnP);

    sts = rbf_pinpg(&pgref, (void *) &bmtpgp, vdp->bmtp,
                     bfap->primMCId.page, BS_NIL, subFtxH);

    if (sts != EOK) {
        domain_panic(dmnP, "rbf_delete_int: failed to access bmt pg %d sts %d",
                     bfap->primMCId.page, sts);

        ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, subFtxH);
        ftx_fail(subFtxH);
        sts =  E_DOMAIN_PANIC;
        goto done;
    }

    mcp = &bmtpgp->bsMCA[bfap->primMCId.cell];

    bfAttrp = (bsBfAttrT *) bmtr_find(mcp, BSR_ATTR, vdp->dmnP);
    if (bfAttrp == NULL) {
        domain_panic(dmnP, "rbf_delete_int: can't find bf attributes");
        ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, subFtxH);
        ftx_fail(subFtxH);
        sts =  E_DOMAIN_PANIC;
        goto done;
    }

    rbf_pin_record(pgref, &bfAttrp->state, sizeof(bfAttrp->state));
    bfAttrp->state = BSRA_DELETING;

    /* bfAttrp->transitionId = get_ftx_id(subFtxH); */

    delOpRec.tag = bfap->tag;
    delOpRec.mcid = bfap->primMCId;
    delOpRec.vdIndex = bfap->primVdIndex;
    delOpRec.setBusy = 0;

    bs_bfs_get_set_id(bfap->bfSetp, &delOpRec.bfSetId);

    ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, subFtxH);

    /*
     * Note that the root done deletes the bitfile's tag.
     */
    ftx_done_urd(subFtxH, FTA_BS_DEL_DELETE_V1, 
                 sizeof(delOpRecT), (void *)&delOpRec,
                 sizeof(delOpRecT), (void *)&delOpRec);

    if ((sts = del_add_to_del_list(bfap->primMCId, vdp, 1, ftxH)) != EOK) {
        domain_panic(dmnP, "rbf_delete_int: can't add bitfile to del list\ndel_add_to_del_list returned: %d ", sts);
        sts =  E_DOMAIN_PANIC;
    }
 
done:
    if (ftxFlag) {
        ftx_done_n(ftxH, FTA_BS_DEL_DELETE_V1);
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
    delOpRecT *recp = (delOpRecT *) opRec;
    statusT sts = EOK;
    struct bfAccess *bfap = NULL;
    domainT *dmnP = NULL;
    bfSetT *bfSetp = NULL;

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(recp->bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(recp->bfSetId.domainId, dmnP->dualMountId)) ) {
            recp->bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("bs_delete_undo_opx: domainId mismatch");
        }
    }

    sts = rbf_bfs_open( &bfSetp, recp->bfSetId, BFS_OP_IGNORE_DEL, ftxH );
    if (sts != EOK) { 
        domain_panic(ftxH.dmnP,"bs_delete_undo_opx: bs_bfs_access() failed");
        goto HANDLE_EXCEPTION;
    }

    if (dmnP->state == BFD_ACTIVATED) {

        /*
         * Get the bitfile's access struct.  NOTE:  If we are called
         * during crash recovery then the access struct will typically
         * be in the INVALID state so we can't use the struct.
         */
        bfap = grab_bsacc(bfSetp, recp->tag, TRUE, NULL); /* locks bfap->bfaLock */
        if (bfap == NULL) {
            ADVFS_SAD0 ("bs_delete_undo: grab_bsacc() failed");
        }

        lk_wait_for (&(bfap->stateLk), &bfap->bfaLock, ACC_VALID);

        /* The bfap->bfaLock must be released because it is
         * reseized in a lower layer (bs_pinpg_one_int).  Set the
         * state temporarily to INIT_TRANS so no one else tries to
         * mess with the access struct until we get back and reset
         * the bfState.
         */
        (void) lk_set_state(&bfap->stateLk, ACC_INIT_TRANS);
        mutex_unlock(&bfap->bfaLock);

        reset_ondisk_bf_state (
                               dmnP, 
                               recp->vdIndex,
                               recp->mcid,
                               recp->prevBfState,
                               ftxH
                               );
        mutex_lock(&bfap->bfaLock);
        (void) lk_set_state(&bfap->stateLk, ACC_VALID);
        bfap->bfState = recp->prevBfState;

        /* This decrement is for the grab_bsacc */
        DEC_REFCNT( bfap );

        mutex_unlock( &bfap->bfaLock );
    } else {
        /*
         * Crash/recovery.
         */
        reset_ondisk_bf_state (
                               dmnP, 
                               recp->vdIndex,
                               recp->mcid,
                               recp->prevBfState,
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
                       vdIndexT vdIndex,  /* in */
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

    vdp = VD_HTOP(vdIndex, dmnP);

    sts = rbf_pinpg( &pgref, (void *) &bmtpgp, vdp->bmtp,
                    mcellId.page, BS_NIL, ftxH );
    if (sts != EOK) {
        domain_panic(dmnP,
                    "reset_ondisk_bf_state: failed to access bmt pg %d, err %d",
                    mcellId.page, sts);
        goto end;
    }
    mcp = &bmtpgp->bsMCA[mcellId.cell];

    bfAttrp = (bsBfAttrT *) bmtr_find( mcp, BSR_ATTR, vdp->dmnP);
    if ( bfAttrp == NULL) {
        domain_panic(dmnP, "reset_ondisk_bf_state: can't find bf attributes");
        goto end;
    }

    rbf_pin_record( pgref, &bfAttrp->state, sizeof( bfAttrp->state ) );
    bfAttrp->state = bfState;

end:
    return;

}  /* end reset_ondisk_bf_state */


void
bs_delete_rtdn_opx(
    ftxHT ftxH,         /* in - transaction handle */
    int opRecSz,        /* in - undo record size */
    void *opRec         /* in - ptr to undo record */
    )
{
    delOpRecT *recp = (delOpRecT *) opRec;
    statusT sts = EOK;
    bfSetT *bfSetp;
    domainT *dmnP;
    int bfs_open = 0;

    dmnP = ftxH.dmnP;

    if ( !BS_UID_EQL(recp->bfSetId.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(recp->bfSetId.domainId, dmnP->dualMountId)) ) {
            recp->bfSetId.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("bs_delete_rtdn_opx: domainId mismatch");
        }
    }

    if (dmnP->state != BFD_ACTIVATED) {
        sts = rbf_bfs_open( &bfSetp, recp->bfSetId, BFS_OP_IGNORE_DEL, ftxH );
        if (sts != EOK) { 
          domain_panic(ftxH.dmnP,"bs_delete_rtdn_opx: rbf_bfs_open() failed");
          goto HANDLE_EXCEPTION;
        }
        bfs_open = 1;
    } else {
        bfSetp = bs_bfs_lookup_desc( recp->bfSetId );
    }
    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );

    if ( bfs_open ) {
        /*
         * In order to prevent a hierarchy violation we grab the
         * BfSetTblLock prior to calling tagdir_remove_tag().
         * Later, when we call bs_bfs_close() we will already
         * have the lock.
         */
        BFSETTBL_LOCK_WRITE( dmnP )
    }

    tagdir_remove_tag( bfSetp, &recp->tag, ftxH );

    if ( bfs_open ) {
        bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    }

HANDLE_EXCEPTION:
}


/*
 * bs_delete
 *
 * delete bitfile
 *
 */

statusT
bs_delete(
          bfAccessT *bfap      /* in - bitfile's access structure */
          )
{
    return rbf_delete( bfap, FtxNilFtxH );
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
 * Up to three pages are pinned.
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
 * The trap code will only be active if the
 * /etc/sysconfigtab variable AdvfsCheckDDL is on.
 */
#ifdef ADVFS_SMP_ASSERT
    if (AdvfsCheckDDL) {
        if ((sts = bmt_refpg(&bmtPgRef, (void **)&bmtp, vdp->bmtp, mcid.page, 
                             BS_NIL)) != EOK) {
            goto done;
        }
        mcp = &bmtp->bsMCA[mcid.cell];
        sts = del_find_del_entry(dmnP, vdp->vdIndex, mcp->bfSetTag,
                                 mcp->tag, &ddlMcellId, &delFlag);
        if ((sts == EOK) && 
            ((mcid.page == ddlMcellId.page) && 
             (mcid.cell == ddlMcellId.cell))) {
            /*
             * This should never happen.  It means we're about to put an
             * mcell onto the DDL that is already there.
             */
            ADVFS_SAD0("trap code: duplicate mcell on DDL!\n");
        }
        bs_derefpg(bmtPgRef, BS_CACHE_IT);
    }
#endif

    if (ftxFlag) {
        if ((sts = FTX_START_N(FTA_BS_DEL_ADD_LIST_V1, &ftxH, parentFtxH,
                               vdp->dmnP, 0)) != EOK){
            return (sts);
        }
    }
    else {
        ftxH = parentFtxH;
    }

    FTX_LOCKWRITE(&vdp->del_list_lk, ftxH);

    if ((sts = rbf_pinpg(&pgRef, (void **)&bmtp, vdp->bmtp, mcid.page, 
                         BS_NIL, ftxH)) != EOK) {
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
        rbf_pin_record(pgRef, (void *)&xrp->delRst, sizeof(xrp->delRst));
        xrp->delRst.offset = 0;
        xrp->delRst.xtntIndex = 0;
        xrp->delRst.mcid = bsNilMCId;
        xrp->delRst.vdIndex = 0;
        xrp->delRst.blocks = 0;
    }

    if ((sts = pin_hdr(vdp->bmtp, &hrp, vdp, ftxH)) != EOK) {
        goto done;
    }
    if ((sts = pin_link(vdp->bmtp, mcid, &nrp, vdp, ftxH)) != EOK) {
        goto done;
    }
    if (!MCIDEQL(hrp->nextMCId, bsNilMCId)) {
        if ((sts = pin_link(vdp->bmtp, hrp->nextMCId, &frp, vdp, ftxH)) != EOK) {
            /* Don't let DDL corruption panic the domain: instead, reset the DDL to
             * empty and continue, but issue a message to console. This is consistent
             * with what del_remove_from_del_list() does.
             */
            hrp->nextMCId = bsNilMCId;

            ms_uaprintf("ADVFS: Corrupted deferred delete list in domain '%s'!\n",
                                vdp->dmnP->domainName);
            ms_uaprintf("       Status = %d\n", sts);
            ms_uaprintf("       No data has been lost but some disk space\n");
            ms_uaprintf("       in this domain may no longer be usable.\n");
            ms_uaprintf("       The space can be recovered by running ``fixfdmn''\n");
            ms_uaprintf("       on this domain, when it has been deactivated.\n");
            ms_uaprintf("       Please inform Technical Support of this message.\n");

            sts = EOK;
        } else {
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
            delListRec.vdIndex = vdp->vdIndex;
            delListRec.type = DEL_ADD;

            ftx_done_u(ftxH, FTA_BS_DEL_ADD_LIST_V1, 
                       sizeof(delListRecT), (void *) &delListRec);
        }
        else {
            ftx_done_n(ftxH, FTA_BS_DEL_ADD_LIST_V1);
        }
    }
    return (sts);
}

/*
 * Remove a bitfile from global deferred delete list.  If ftxFlag is 
 * nonzero, a subtransaction is started.  Up to three pages are pinned.
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

    dmnP = vdp->dmnP;

    if (ftxFlag) {
        if ((sts = FTX_START_N(FTA_BS_DEL_REM_LIST_V1, &ftxH, parentFtxH, 
                               vdp->dmnP, 0)) != EOK) {
            return (sts);
        }
    }
    else {
        ftxH = parentFtxH;
    }

    FTX_LOCKWRITE(&vdp->del_list_lk, ftxH);


    if ((sts = pin_link(vdp->bmtp, mcid, &drp, vdp, ftxH)) != EOK) {
        goto done;
    }
    last = 1;
    /*
     * This code contains  a work-around for case at system startup where
     * we are walking the list from the begining and our nextMCId ptr points
     * to a free mcell.  Instead of panicing, we truncate the list.  We
     * may lose some space, but hey, we're still running.  I didn't
     * code the case where we are pulling an mcell out of the middle of
     * the list and we find the prevMCId ptr points to a free mcell.  I
     * haven't seen that case yet, although others may have.
     */
    if (!MCIDEQL(drp->nextMCId, bsNilMCId)) {
        if ((sts = pin_link(vdp->bmtp, drp->nextMCId, &frp, vdp, ftxH)) != EOK) {
            drp->nextMCId = bsNilMCId;
            last = 1;

            ms_uaprintf("ADVFS: Corrupted deferred delete list in domain '%s'!\n",
                                vdp->dmnP->domainName);
            ms_uaprintf("       Status = %d\n", sts);
            ms_uaprintf("       No data has been lost but some disk space\n");
            ms_uaprintf("       in this domain may no longer be usable.\n");
            ms_uaprintf("       The space can be recovered by running ``fixfdmn''\n");
            ms_uaprintf("       on this domain, when it has been deactivated.\n");
            ms_uaprintf("       Please inform Technical Support of this message.\n");

            sts = EOK;
        } else last = 0;
    }
    if (MCIDEQL(drp->prevMCId, bsNilMCId)) {
        if ((sts = pin_hdr(vdp->bmtp, &hrp, vdp, ftxH)) != EOK) {
            goto done;
        }
        first = 1;
    }
    else {
        if ((sts = pin_link(vdp->bmtp, drp->prevMCId, &prp, vdp, ftxH)) != EOK) {
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
            delListRec.vdIndex = vdp->vdIndex;
            delListRec.type = DEL_REMOVE;

            ftx_done_u(ftxH, FTA_BS_DEL_REM_LIST_V1, 
                       sizeof(delListRecT), (void *)&delListRec);
        }
        else {
            ftx_done_n(ftxH, FTA_BS_DEL_REM_LIST_V1);
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
    delListRecT *zrp = (delListRecT *) opRec;
    statusT sts;
    vdT *vdp;
    domainT *dmnP;

    dmnP = ftxH.dmnP;
    vdp = VD_HTOP(zrp->vdIndex, dmnP);

    if (zrp->type == DEL_ADD) {
        /*
         * Undo of add to del list operation.
         */
        if ((sts = del_remove_from_del_list(zrp->mcid, vdp, 0, ftxH)) != EOK) {
            domain_panic (dmnP, "del_bitfile_list_undo: del_remove_from_del_list returned: %d", sts);
            return;
        }
    }
    else if (zrp->type == DEL_REMOVE) {
        /*
         * Undo of remove from del list operation.
         */
        if ((sts = del_add_to_del_list(zrp->mcid, vdp, 0, ftxH)) != EOK) {
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
pin_link(
    bfAccessT *bmtbfap,
    bfMCIdT mcid,
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
                         BS_NIL, ftxH)) != EOK) {
        return (sts);
    }

    mcp = &bmtp->bsMCA[mcid.cell];
    if ((rp = (bsXtntRT *)bmtr_find(mcp, BSR_XTNTS, vdp->dmnP)) == NULL) {
        return E_CORRUPT_LIST;
    }

    rbf_pin_record(pgRef, (void *)&rp->delLink, sizeof(delLinkT));
    *zlp = &rp->delLink;
    return (EOK);
}

/*
 * Pin the page and the record containing the del list header in a 
 * given BMT.  An out parameter specifies a pointer to the del link record.
 */

static statusT
pin_hdr(
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
    unsigned long pgnum;

    if ( RBMT_THERE(bmtbfap->dmnP) ) {
        pgnum = MCELL_LIST_PAGE;
    } else {
        pgnum = MCELL_LIST_PAGE_V3;
    }

    sts = rbf_pinpg(&pgRef, (void **)&bmtp, bmtbfap, pgnum, BS_NIL, ftxH);
    if (sts != EOK) {
        return (sts);
    }

    mcp = &bmtp->bsMCA[MCELL_LIST_CELL];
    if((*zlp = (delLinkT *)bmtr_find(mcp,BSR_DEF_DEL_MCELL_LIST,
                                        vdp->dmnP)) == NULL) {
        domain_panic(vdp->dmnP, "pin_hdr: bmtr_find can't find list head failed");
        return E_DOMAIN_PANIC;
    }

    rbf_pin_record(pgRef, (void *)(*zlp), sizeof(delLinkRT));
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
    u_long flag
    )
{
    bsMPgT* bmtp;
    int mcellListPage;
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
    extern bfAccessT *find_bfap();
    ftxHT ftxH;
    
    if ( RBMT_THERE(vdp->bmtp->dmnP) ) {
        mcellListPage = MCELL_LIST_PAGE;
    } else {
        mcellListPage = MCELL_LIST_PAGE_V3;
    }

    if (vdp->bmtp->nextPage <= mcellListPage) {
        /* 
         * Since the migrate mcell inuse listhead is maintained on page
         * MCELL_LIST_PAGE of the BMT, we must assume that the list is
         * empty if this page doesn't exist.
         */
        return (EOK);
    }

    if ((sts = bmt_refpg(&pgRef, (void*)&bmtp, vdp->bmtp,
                   mcellListPage,  BS_NIL)) != EOK) {
        ADVFS_SAD0 ("del_clean_mcell_list: Can't ref MCELL_LIST_PAGE of BMT");
    }

    mcellp = &bmtp->bsMCA[MCELL_LIST_CELL];
    if ((zlp = (delLinkRT *) bmtr_find(mcellp, BSR_DEF_DEL_MCELL_LIST,
                                                vdp->dmnP)) == NULL) {
        ADVFS_SAD0 ("del_clean_mcell_list: Can't find del mcell list record");
    }

    mcellId = zlp->nextMCId;

    bs_derefpg(pgRef, BS_CACHE_IT);

#ifdef ADVFS_DEBUG
    if (MCIDEQL(mcellId, bsNilMCId)) {
        printf("ADVFS delete pending list empty.");
    }
    else {
        printf("ADVFS recovering partially cleared storage:\n    ");
    }
#endif

    bfSetId.domainId = vdp->dmnP->domainId;
    while (!MCIDEQL(mcellId, bsNilMCId)) {

        if ((sts = bmt_refpg(&pgRef, (void*)&bmtp, vdp->bmtp, 
                            mcellId.page, BS_NIL)) != EOK) {
            return (sts);       /* XXX */
        }

        mcellp = &bmtp->bsMCA[mcellId.cell];

#ifdef ADVFS_DEBUG
        printf("page %d cell %d set %d tag %d\n", 
                mcellId.page, mcellId.cell, mcellp->bfSetTag, mcellp->tag);
#endif

        /*
         * If this is a cluster we may not want to delete the file yet.
         */
        delete_this_mcell = FALSE;
        bfSetId.dirTag = mcellp->bfSetTag;
        if (clu_is_ready() && (flag & M_FAILOVER)) {
            bfSetp = bs_bfs_lookup_desc(bfSetId);
            if (bfSetp == NULL) {
                delete_this_mcell = TRUE;
            } else {
                bfap = find_bfap(bfSetp, mcellp->tag, FALSE, NULL);
                if (bfap == NULL) {
                    delete_this_mcell = TRUE;
                } else if (mcellp->tag.num == 1) {
                    /* 
                     * Since the fragfile (tag 1) is opened on fset mount
                     * and closed on umount, we will always find a bfap 
                     * for it. During failover, if we find these mcells 
                     * on the DDL, we must delete them, otherwise they
                     * will stay on the DDL until the next cluster shutdown.
                     * This is because failover does not do a clean umount.
                     */
                    delete_this_mcell = TRUE;
                    mutex_unlock(&bfap->bfaLock);
                } else {
                    mutex_unlock(&bfap->bfaLock);
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
            del_clean_fix_ddl( vdp, pgRef );
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

                    bs_derefpg(pgRef, BS_CACHE_IT);
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
                    sts = ddl_complete_delete(mcellp->bfSetTag, mcellp->tag, vdp);
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
                          vdp->dmnP->domainName, mcellp->bfSetTag);
                    }
                }
            }

            if ( delete_this_mcell == TRUE ) {
                if ((sts = del_dealloc_stg(mcellId, vdp)) != EOK) {
                    /* Panic the domain */

                    bs_derefpg(pgRef, BS_CACHE_IT);
                    domain_panic(vdp->dmnP,
                             "del_clean_mcell_list: del_dealloc_stg_failed");
                    return E_DOMAIN_PANIC;
                }
            }
        } /* end if delete_this_mcell */

        mcellId = nextMcellId;

        bs_derefpg(pgRef, BS_CACHE_IT);
    }

    return (EOK);
}


/*
 * The DDL is corrupted.  This is bad but it is not critical.
 * Inform the user that storage may be lost and ask him to
 * report the problem.  Then, patch the DDL header so the DDL
 * is empty.
 */
del_clean_fix_ddl( vdT *vdp, bfPageRefHT  pgRef )
{
    statusT sts;
    ftxHT ftxH;
    bsMPgT *bmtp;
    bsMCT* mcellp;
    delLinkRT *zlp;
    rbfPgRefHT bmtPgRef;
    int mcellListPage;

    if ( RBMT_THERE(vdp->bmtp->dmnP) ) {
        mcellListPage = MCELL_LIST_PAGE;
    } else {
        mcellListPage = MCELL_LIST_PAGE_V3;
    }

    ms_uaprintf("ADVFS: Corrupted deferred delete list in domain '%s'!\n",
                        vdp->dmnP->domainName);
    ms_uaprintf("       No data has been lost but some disk space\n");
    ms_uaprintf("       in this domain may no longer be usable.\n");

    /*
     * If AdvfsCaptureBadDDL (resettable on the fly via dbx)  is 1,
     * panic the domain.  This will allow us to capture the domain
     * in its corrupted state, if necessary.
     */
    if (AdvfsCaptureBadDDL == 1) {
        bs_derefpg(pgRef, BS_RECYCLE_IT);
        domain_panic(vdp->dmnP, "Corrupted DDL!");
        return (E_DOMAIN_PANIC);
    }

    bs_derefpg(pgRef, BS_RECYCLE_IT);

    sts = FTX_START_N(FTA_BS_DEL_TRUNCATE_DDL, &ftxH, FtxNilFtxH, vdp->dmnP, 1);
    if ( sts != EOK) {
        domain_panic(vdp->dmnP, "del_clean_fix_ddl: Can't start transaction");
        return E_DOMAIN_PANIC;
    }

    sts = rbf_pinpg(&bmtPgRef, (void **)&bmtp, vdp->bmtp,
                    mcellListPage, BS_NIL, ftxH);
    if (sts != EOK) {
        ftx_fail(ftxH);
        domain_panic(vdp->dmnP, "del_clean_fix_ddl: Can't pin DDL header page");
        return E_DOMAIN_PANIC;
    }

    mcellp = &bmtp->bsMCA[MCELL_LIST_CELL];
    zlp = (delLinkT *)bmtr_find(mcellp,BSR_DEF_DEL_MCELL_LIST, vdp->dmnP);
    if ( zlp == NULL ) {
        ftx_fail(ftxH);
        domain_panic(vdp->dmnP, "del_clean_fix_ddl: Can't find DDL hdr record");
        return E_DOMAIN_PANIC;
    }

    rbf_pin_record(bmtPgRef, (void *)(zlp), sizeof(delLinkRT));
    zlp->nextMCId = zlp->prevMCId = bsNilMCId;

    ftx_done_n(ftxH, FTA_BS_DEL_TRUNCATE_DDL);

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
    uint i;
    fileSetNodeT *fsnp = NULL;
    quotaInfoT *qip;
    struct bfNode *bnp;

    bfSetId.domainId = dmnP->domainId;
    bfSetId.dirTag = setTag;

    sts = rbf_bfs_open( &bfSetp, bfSetId, BFS_OP_IGNORE_DEL, FtxNilFtxH );
    if ( sts != EOK ) {
        return sts;
    }

    /* Clones are only deleted as part of rmfset. However, we may end up
     * in here with a clone file if the domain crashes during a rmfset of
     * a clone fileset.
     */

    /*
    ** We are starting a faux mount. If filesetnode_init does not find the
    ** fileset mounted it will alloc fsnp and put it on FilesetHead.
    ** We must set up several conditions for folowing routines.
    ** 1) bs_get_bfset_params (from quota_activate=>get_quota_config) needs
    **    bfSetp->fsnp==null.
    ** 2) qi_init (from quota_activate) needs bnp->accessp since qf_get
    **    (from quota_activate) didn't set it since mp was null.
    */

    sts = filesetnode_init( bfSetp, &fsnp );

    if ( sts != EOK && sts != EBUSY ) {
        goto cleanup;
    }
    if ( sts == EOK ) {
        /* We opened the fileset. CFS is not holding it open. */
        MS_SMP_ASSERT(bfSetp->fsRefCnt == 1 );
        /* CFS does not have the FS open. We are doing a faux mount. */
        sts = quota_activate( fsnp );
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
    sts = bs_access_one( &bfap,
                         fileTag,
                         bfSetp,
                         FtxNilFtxH,
                         BF_OP_GET_VNODE | BF_OP_FIND_ON_DDL,
                         NULLMT,
                         &vp,
                         NULL);
    if ( sts != EOK ) {
        goto cleanup;
    }

    MS_SMP_ASSERT(bfap != NULL);
    MS_SMP_ASSERT(bfap->bfVp == vp);

    /* CFS is not holding this file open, else we would not be here. */
    MS_SMP_ASSERT(VTOC(vp) == NULL);
    sts = fscontext_init( vp, bfap, &cp );
    if ( sts != EOK ) {
        goto cleanup;
    }

    /*
     * If the fileSetNode's mount pointer isn't yet initialized,
     * fill in the fileSetNode's mount pointer with what the vnode has.
     * This will satisfy any functions such as chk_blk_quota() 
     * that need a mount structure hanging off the fsnp.
     */
    if (!cp->fileSetNode->mountp) {
        cp->fileSetNode->mountp = vp->v_mount;
    }

    bnp = (bfNodeT*)vp->v_data;
    bnp->accessp = bfap;

    bfap->file_size = cp->dir_stats.st_size;
    bfap->fragId = cp->dir_stats.fragId;
    if ((cp->dir_stats.fragId.frag == bsNilFragId.frag) &&
        (cp->dir_stats.fragId.type == bsNilFragId.type))
    {
        bfap->fragState = FS_FRAG_NONE;
    } else {
        bfap->fragState = FS_FRAG_VALID;
    }
    bfap->fragPageOffset = cp->dir_stats.fragPageOffset;

cleanup:
    if ( vp ) {
        /* This call to vrele is the heart of this routine. */
        /* This is the last close of the file. The quotas are */
        /* adjusted, the storage is freed, the tag is freed. */
        vrele(bfap->bfVp);
    }

    if ( fsnp != NULL ) {

        FILESET_WRITE_LOCK(&FilesetLock );

        *fsnp->fsPrev = fsnp->fsNext;
        if (fsnp->fsNext != NULL) {
            fsnp->fsNext->fsPrev = fsnp->fsPrev;
        }

        FILESET_UNLOCK(&FilesetLock );

        MS_SMP_ASSERT(bfSetp->fsnp == fsnp);
        bfSetp->fsnp = NULL;

        if ( fsnp->quotaStatus & QSTS_QUOTA_ON ) {
            vrele(ATOV(fsnp->qi[USRQUOTA].qiAccessp));
            vrele(ATOV(fsnp->qi[GRPQUOTA].qiAccessp));
        }

        for (qip = &fsnp->qi[0]; qip < &fsnp->qi[MAXQUOTAS]; qip++) {
            lock_terminate( &qip->qiLock );
        }
        mutex_destroy(&fsnp->filesetMutex);

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
extern struct lockinfo *ADVfilesetMutex_lockinfo;
extern struct lockinfo *ADVquotaInfoT_qiLock;

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

    FILESET_WRITE_LOCK(&FilesetLock);
    for ( fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext ) {
        if ( BS_BFS_EQL(bfSetp->bfSetId, fsp->bfSetId) ) {
            FILESET_UNLOCK(&FilesetLock);
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

    FILESET_UNLOCK(&FilesetLock);

    bfSetParamsp = &bfSetParams;
    sts = bs_get_bfset_params( bfSetp, bfSetParamsp, 0 );
    if ( sts != EOK ) {
        return sts;
    }

    fsnp->fsFlags = 0;
    if ( bfSetParamsp->cloneId > 0 ) {
        fsnp->fsFlags |= FS_CLONEFSET;
    }

    BS_BFTAG_IDX(fsnp->rootTag) = bfSetParamsp->fsContext[0];
    BS_BFTAG_SEQ(fsnp->rootTag) = bfSetParamsp->fsContext[1];
    BS_BFTAG_IDX(fsnp->tagsTag) = bfSetParamsp->fsContext[2];
    BS_BFTAG_SEQ(fsnp->tagsTag) = bfSetParamsp->fsContext[3];

    fsnp->quotaStatus = bfSetParamsp->quotaStatus &
                        ~(QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN);
    if (fsnp->dmnP->dmnVersion >= FIRST_LARGE_QUOTAS_VERSION) {
        fsnp->quotaStatus |= QSTS_LARGE_LIMITS;
    }
    fsnp->fileHLimit = bfSetParamsp->fileHLimit;
    fsnp->fileSLimit = bfSetParamsp->fileSLimit;
    fsnp->fileTLimit = bfSetParamsp->fileTLimit;
    fsnp->blkSLimit = bfSetParamsp->blkSLimit;
    fsnp->blkTLimit = bfSetParamsp->blkTLimit;
    fsnp->blkHLimit = bfSetParamsp->blkHLimit;

    fsnp->qi[USRQUOTA].qiTag.num = bfSetParamsp->fsContext[4 + 2 * USRQUOTA];
    fsnp->qi[USRQUOTA].qiTag.seq = bfSetParamsp->fsContext[5 + 2 * USRQUOTA];
    fsnp->qi[GRPQUOTA].qiTag.num = bfSetParamsp->fsContext[4 + 2 * GRPQUOTA];
    fsnp->qi[GRPQUOTA].qiTag.seq = bfSetParamsp->fsContext[5 + 2 * GRPQUOTA];

    mutex_init3(&fsnp->filesetMutex, 0, "filesetMutex", ADVfilesetMutex_lockinfo);
    for ( qip = &fsnp->qi[0]; qip < &fsnp->qi[MAXQUOTAS]; qip++ ) {
        lock_setup( &qip->qiLock, ADVquotaInfoT_qiLock, TRUE );
    }

    fsnp->rootAccessp = NULL;
    fsnp->root_vp = NULL;
    fsnp->mountp = NULL;
    bzero( (void*)&fsnp->fileSetStats, sizeof(fileSetStatsT) );

    *fsnpA = fsnp;

    return EOK;
}


/*
** Allocate & set up the context structure
** This is called from ddl_complete_delete which is not going to mount
** the fileset. We only need to do enough to satisfy bs_access_one & vrele.
*/
statusT
fscontext_init( struct vnode *vp, bfAccessT *bfap, struct fsContext **cpA )
{
    struct fsContext *cp;
    statusT sts;
    uint i;

    MS_SMP_ASSERT(VTOC(vp) == NULL);

    cp = vnode_fscontext_allocate(vp);

    MS_SMP_ASSERT(!cp->initialized);
    cp->bf_tag = bfap->tag;

    sts = bmtr_get_rec(bfap, BMTR_FS_STAT, &cp->dir_stats, sizeof(statT));
    if ( sts != EOK ) {
        return sts;
    }

    cp->fs_flag = 0;
    cp->dirstamp = 0;
    cp->undel_dir_tag = NilBfTag;
    cp->last_offset = 0;
    MS_SMP_ASSERT(bfap->bfSetp->fsnp != NULL);
    cp->fileSetNode = bfap->bfSetp->fsnp;

    for (i = 0; i < MAXQUOTAS; i++) {
        cp->diskQuot[i] = NULLDQUOT;
    }
    cp->quotaInitialized = 1;

    FS_FILE_WRITE_LOCK(cp);

    (void)attach_quota( cp, FtxNilFtxH );

    cp->initialized = 1;
    FS_FILE_UNLOCK(cp);

    *cpA = cp;

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
    vdIndexT nextVdIndex;
    bfMCIdT nextMCId;
    bfPageRefHT pgref;
    bsMPgT* bmtp;
    bsMCT* mcp;
    bsXtntRT *pxp;      /* primary xtnt ptr */
    vdIndexT vdIndex;
    bsXtntMapTypeT type;
    statusT sts;
    int mutexisLocked =0;
    bsBfAttrT *attrp;

    dmnP = pvdp->dmnP;

    /*
     * Indicate one more entry is actively being processed.
     */
    DDLACTIVE_LOCK_READ( &(pvdp->ddlActiveLk) )

    if ((sts = bmt_refpg(&pgref, (void **)&bmtp, pvdp->bmtp,
                         pmcid.page, BS_NIL)) != EOK) 
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

    /* if there is a BSR_ATTR record and it indicates the existance of */
    /* a clone, then give the extents to the clone instead of freeing them. */
    attrp = (bsBfAttrT *)bmtr_find(mcp, BSR_ATTR, pvdp->dmnP);
    if ( attrp != NULL && attrp->cloneId && attrp->state == BSRA_VALID ) {
        xfer_xtnts_to_clone(pmcid, pvdp, mcp, pxp);
        bs_derefpg(pgref, BS_CACHE_IT);
        goto done;
    }

    /*
     * If this file has its first extent in the primary mcell, we start there.
     * Otherwise, we start at the mcell pointed to by the primary mcell.
     */
    if ((FIRST_XTNT_IN_PRIM_MCELL(dmnP->dmnVersion, pxp->type)) &&
        (pxp->firstXtnt.xCnt > 1)) {
        mcid = pmcid;
        vdIndex = pvdp->vdIndex;
    }
    else {
        mcid = pxp->chainMCId;
        vdIndex = pxp->chainVdIndex;
    }
    type = pxp->type;

    bs_derefpg(pgref, BS_CACHE_IT);

    if ( vdIndex != 0 ) {
        /*
         * Since the extent chain is nonempty, the bitfile must have
         * storage.  Clear these SBM bits.
         */
        vdp = VD_HTOP(vdIndex, dmnP);
    
        switch (type) {
          case BSXMT_APPEND:
          case BSXMT_STRIPE:
            /*
             * Loop over all the chained extent records.  SBM bits in the
             * delete range are cleared.
             */
            while (1) {
                if ((sts = del_xtnt_array(pmcid, mcid, pvdp, vdp, 
                                          &nextVdIndex, &nextMCId)) != EOK) {
                    domain_panic( dmnP,
                                  "del_dealloc_stg: Can't clear xtnt array");
                    RAISE_EXCEPTION( sts );
                }
                if (!nextVdIndex) {
                    break;
                }
                mcid = nextMCId;
                vdp = VD_HTOP(nextVdIndex, dmnP);
            }
            break;
          default:
        
              MS_SMP_ASSERT(FALSE);
              domain_panic( dmnP,
                            "del_dealloc_stg: unknown type");
              RAISE_EXCEPTION( sts );
        }
    }

done:
    bmt_free_bf_mcells( pvdp, pmcid, FtxNilFtxH, TRUE );

    DDLACTIVE_UNLOCK( &(pvdp->ddlActiveLk) )

    /*
     * If a thread is waiting on the completion of this entry, wake it.
     */


    mutex_lock (&(dmnP->mutex));
    mutexisLocked = 1;

    if ((pmcid.page == pvdp->ddlActiveWaitMCId.page) &&
        (pmcid.cell == pvdp->ddlActiveWaitMCId.cell)) {

        cond_signal(&pvdp->ddlActiveWaitCv);
        mutex_unlock (&(dmnP->mutex));
        mutexisLocked = 0;
    }

    if ( mutexisLocked ) mutex_unlock (&(dmnP->mutex));

    return (EOK);

HANDLE_EXCEPTION:

    DDLACTIVE_UNLOCK( &(pvdp->ddlActiveLk) );
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
    vdIndexT *nextVdIndex,  /* out - vd index of next mcell */
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
    bsShadowXtntT *srp;
    delFtxDescT dsc;
    statusT sts;
    domainT *dmnP;
    int pgRefed = FALSE;

    dmnP = vdp->dmnP;

    if ((sts = bmt_refpg(&refPgH, (void **)&bmtp, vdp->bmtp, 
                        mcid.page, BS_NIL)) != EOK) {
       domain_panic(dmnP, "del_xtnt_array: Can't ref BMT page for chain mcell");
       RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }
    pgRefed = TRUE;
    mcp = &bmtp->bsMCA[mcid.cell];

    if ((xrp = (bsXtraXtntRT *)bmtr_find(mcp, BSR_XTRA_XTNTS, vdp->dmnP))
                                                                != NULL) {
        fxp = &xrp->bsXA[0];
        xCnt = xrp->xCnt;
        *nextVdIndex = mcp->nextVdIndex;
        *nextMCId = mcp->nextMCId;
    }
    else if ((prp = 
              (bsXtntRT *)bmtr_find(mcp, BSR_XTNTS, vdp->dmnP)) != NULL) {
        MS_SMP_ASSERT(FIRST_XTNT_IN_PRIM_MCELL(dmnP->dmnVersion, prp->type))
        fxp = &prp->firstXtnt.bsXA[0];
        xCnt = prp->firstXtnt.xCnt;
        *nextVdIndex = prp->chainVdIndex;
        *nextMCId = prp->chainMCId;
    }
    else if ((srp = 
      (bsShadowXtntT *)bmtr_find(mcp, BSR_SHADOW_XTNTS, vdp->dmnP))
                                                                != NULL) {
        fxp = &srp->bsXA[0];
        xCnt = srp->xCnt;
        *nextVdIndex = mcp->nextVdIndex;
        *nextMCId = mcp->nextMCId;
    }
    else {
        domain_panic(dmnP, "del_xtnt_array: No prim, xtra, or shadow extents");
        RAISE_EXCEPTION(E_DOMAIN_PANIC);
    }

    del_ftx_start(pmcid, pvdp, &dsc);

    if (MCIDEQL(dsc.rstMCId, bsNilMCId)) {
        if (dsc.rstIndex == NORESTART) {
            /*
             * This condition arises when we've previously deallocated
             * all the storage for this bitfile but the system crashed
             * before the corresponding primary mcell was removed from
             * the deferred delete chain.  There is nothing to do.
             */
            bs_derefpg(refPgH, BS_CACHE_IT);
            ftx_done_n(dsc.ftxH, FTA_BS_DEL_FTX_START_V1);
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
            dsc.rstVdIndex = vdp->vdIndex;
        }
    }
    else if (!MCIDEQL(dsc.rstMCId, mcid) || dsc.rstVdIndex != vdp->vdIndex) {
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
        ftx_done_n(dsc.ftxH, FTA_BS_DEL_FTX_START_V1);

        return (EOK);
    }

    pinPages = DEL_MAX_PAGES - 1; /* One page pinned by del_ftx_start */

    FTX_LOCKWRITE( &vdp->stgMap_lk, dsc.ftxH)

    for (xp = fxp + dsc.rstIndex; xp <= fxp + xCnt - 2; xp++) {
        uint32T sp;     /* start page */
        uint32T ep;     /* end page */
        uint32T np;     /* number of pages */
        uint32T sb;     /* start block */
        uint32T nb;     /* number of blocks */
        uint32T zb;     /* scratch variable */

        sb = xp->vdBlk;
        if ( sb == XTNT_TERM || sb == PERM_HOLE_START ) {
            continue;
        }

        sp = xp->bsPage + dsc.rstOffset;
        np = (xp + 1)->bsPage - sp;
        ep = sp + np - 1;

        /*
         * At this point [sp, ep] describes an interval in the bitfile
         * whose SBM storage is to be freed.  Convert this to an 
         * interval [sb, sb + nb - 1] which describes the corresponding SBM
         * interval.
         */
        sb += (sp - xp->bsPage) * dsc.pgSz;
        nb = np * dsc.pgSz;

        while (1) {
            zb = nb;
            if ((sts = del_range(sb, &zb, &pinPages, 
                            vdp, dsc.pgSz, dsc.ftxH)) != EOK) {
                ftx_fail( dsc.ftxH );
                domain_panic(dmnP, "del_xtnt_array: Can't clear range");
                RAISE_EXCEPTION(E_DOMAIN_PANIC);
            }

            nb -= zb;
            sb += zb;

            if (nb == 0) {
                /*
                 * Finished this xtnt map entry.  Set up restart to
                 * begin at start of the next entry.
                 */
                dsc.rstIndex = (xp - fxp) + 1;
                dsc.rstOffset = 0;
            }
            else {
                /*
                 * Could not finish this extent map entry.  Record
                 * the page offset relative to the start of the
                 * extent.
                 */
                dsc.rstIndex = xp - fxp;
                MS_SMP_ASSERT(((sb - xp->vdBlk) % dsc.pgSz) == 0);
                dsc.rstOffset = (sb - xp->vdBlk) / dsc.pgSz;
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

            del_ftx_done(&dsc);
            del_ftx_start(pmcid, pvdp, &dsc);

            FTX_LOCKWRITE( &vdp->stgMap_lk, dsc.ftxH)

            /*
             * Reset maximum number of pages that may be pinned.
             */
            pinPages = DEL_MAX_PAGES - 1;

            if (nb == 0) {
                break;  /* to top of for loop */
            }
        }       /* end of while loop */
    }   /* end of for loop */

    /*
     * Setup the delete descriptor record to start with the next mcell
     * in the extent chain.
     */
    dsc.rstMCId = *nextMCId;
    dsc.rstVdIndex = *nextVdIndex;
    dsc.rstIndex = 0;
    dsc.rstOffset = 0;

    if (MCIDEQL(dsc.rstMCId, bsNilMCId)) {
        /*
         * Indicate that we're all done.
         */
        dsc.rstIndex = NORESTART;
    }      
  
    del_ftx_done(&dsc);
    bs_derefpg(refPgH, BS_CACHE_IT);

    return (EOK);

HANDLE_EXCEPTION:

    if (pgRefed) {
        bs_derefpg(refPgH, BS_CACHE_IT);
    }

    return (sts);
}


/* We have the head of a chain of mcells in hand. The storage described
 * is to be given to a clone file. It's far too late to give this back
 * to the original file if there are any errors. Instead, in case of
 * errors we will try to free all the memory and mark the clone out of sync.
 * If we can't even identify the clone or some of the storage we panic
 * the domain after freeing as much storage as we can.
 *
 * For each page in each extent in each mcell, ask if the page needs to
 * be given to the clone or freed (if the clone already has that page).
 * Gather up consecutive (on the disk) pages and add them to the clone's
 * storage or free the pages.
 *
 * During normal operation the Bfs_delete_lk lock will be held to prevent
 * the clone fileset from being deleted between the time the existance of a
 * clone was detected in stg_remove_stg_start and here. The lock is taken
 * in msfs_setattr and other places that perform both parts of a truncation
 * (bf_setup_truncation & stg_remove_stg_finish). During recovery only the
 * second half of truncation is performed (removing storage from the ddl).
 * We do not need the Bfs_delete_lk in that case since AdvFS is single
 * threaded during recovery and no other thread can remove a fileset until
 * recovery is complete.
 */
void
xfer_xtnts_to_clone (
                     bfMCIdT  pmcid,  /* "primary mcell" id of delete list */
                     vdT      *pvdp,  /* "primary vd" of delete list */
                     bsMCT    *mcp,   /* "primary mcell" of delete list */
                     bsXtntRT *pxp    /* "primary xtnt record" of delete list */
                    )
{
    bfAccessT *cloneap = NULL;
    statusT sts;
    statusT orig_sts;
    bsBfAttrT *bfAttrp;
    rbfPgRefHT attrPgH;
    bfSetIdT bfSetId;
    unsigned long pg;
    bsInMemXtntMapT *xmp = NULL;
    int mapi;
    ulong_t pgstofree;
    ulong_t pgstoadd;
    ulong_t startpg;
    vdIndexT vdIndex;
    bfMCIdT mcid;
    delFtxDescT dsc;
    int stripeIndex = -1;
    bsXtntMapTypeT xtntType = pxp->type;
    int segSz = pxp->segmentSize;
    ulong_t pageOffset;
    int xfererr = FALSE;
    domainT *dmnP = pvdp->dmnP;
    bfSetT *bfSetp = NULL;
    ftxHT ftxH;
    bsInMemXtntMapT *delXtntMap = NULL;
    int firstVdi = 0;
    int firstBlk;
    int firstPg;
    struct vnode *nullvp;

    bfSetId.domainId = dmnP->domainId;
    bfSetId.dirTag = mcp->bfSetTag;

    /* open the clone fileset */

    BFSETTBL_LOCK_WRITE( dmnP )

    sts = rbf_bfs_open(&bfSetp, bfSetId, 
                       BFS_OP_XFER_XTNTS_TO_CLONE, FtxNilFtxH);

    BFSETTBL_UNLOCK( dmnP)

    if ( sts == EOK ) {
        MS_SMP_ASSERT(bfSetp->cloneId > 0);
        MS_SMP_ASSERT(
            (lk_get_state(bfSetp->cloneDelState) == CLONE_DEL_XFER_STG) ||
            (lk_get_state(bfSetp->cloneDelState) == CLONE_DEL_PENDING));

        /* open the clone file that storage will be added to */
        nullvp = NULL;
        sts = bs_access_one( &cloneap,
                             mcp->tag,
                             bfSetp,
                             FtxNilFtxH,
                             0,
                             NULL,
                             &nullvp,
                             NULL);
        if ( sts == EOK ) {
            MS_SMP_ASSERT(cloneap->xtnts.type == BSXMT_APPEND ||
                          cloneap->xtnts.type == BSXMT_STRIPE);
        }
    }

    if ( cloneap && cloneap->outOfSyncClone ) {
        bs_close_one(cloneap, 0, FtxNilFtxH);
        cloneap = NULL;
    }

    if ( cloneap == NULL ) {
        /* prepare to discard all pages */
        xfererr = TRUE;
        orig_sts = sts;
        /* Since we are going to discard all these pages, we don't want to */
        /* interpret them as part of a striped extent. The xtntType test */
        /* at the start of the "for each page in extent" loop needs info */
        /* (eg stripeIndex) that we don't have. */
        xtntType = BSXMT_APPEND;
    }

    /*
     * If this file has its first extent in the primary mcell, we start there.
     * Otherwise, we start at the mcell pointed to by the primary mcell.
     */
    if ((FIRST_XTNT_IN_PRIM_MCELL(dmnP->dmnVersion, pxp->type)) &&
        (pxp->firstXtnt.xCnt > 1)) {
        vdIndex = pvdp->vdIndex;
        mcid = pmcid;
    }
    else {
        vdIndex = pxp->chainVdIndex;
        mcid = pxp->chainMCId;
    }

    if ( cloneap && cloneap->xtnts.type == BSXMT_STRIPE ) {
        int i;

        MS_SMP_ASSERT(cloneap->xtnts.stripeXtntMap);
        MS_SMP_ASSERT(pxp->rsvd1 > 0 &&
                      pxp->rsvd1 <= cloneap->xtnts.stripeXtntMap->cnt);

        for ( i = 0; i < cloneap->xtnts.stripeXtntMap->cnt; i++ ) {
            cloneap->xtnts.stripeXtntMap->xtntMap[i]->allocDeallocPageCnt = 0;
        }

        /*
        ** We have been given a stripe extent.
        ** The stripe index is "passed in" using the rsvd1 field of the
        ** bsXtntRT record in the pseudo primary mcell on the ddl.
        ** The value in rsvd1 is the stripe index + 1 for a striped file.
        */
        stripeIndex = pxp->rsvd1 - 1;
        xmp = cloneap->xtnts.stripeXtntMap->xtntMap[stripeIndex];
    }
    else if (cloneap && (xtntType == BSXMT_STRIPE))
    {
        /* This is the situation where the clone was created
         * before the file was striped. The original may have
         * had pages, been cloned, truncated, then striped.
         * In any case all maxClonePgs are mapped by the clone.
         */

        /* prepare to discard all pages */
        xfererr = TRUE;
        /* Since we are going to discard all these pages, we don't want to */
        /* interpret them as part of a striped extent. The xtntType test */
        /* at the start of the "for each page in extent" loop needs info */
        /* (eg stripeIndex) that we don't have. */
        xtntType = BSXMT_APPEND;
    } else if ( cloneap ) {
        MS_SMP_ASSERT(cloneap->xtnts.type == BSXMT_APPEND);
        xmp = cloneap->xtnts.xtntMap;
    }
        
    /*
     * for each mcell in the delete chain
     */
    while ( vdIndex )
    {
        bsMCT *mcp1;
        bfPageRefHT refPgH;
        bsMPgT* bmtp;
        bsXtntT *fxp;
        int xCnt;
        bsXtraXtntRT *xrp;
        bsShadowXtntT *srp;
        bsXtntRT *xp;
        long tpa = 0;
        vdT *vdp = VD_HTOP(vdIndex, dmnP);

        /* read the mcell in the delete chain */
        sts = bmt_refpg( &refPgH,
                         (void **)&bmtp,
                         vdp->bmtp,
                         mcid.page,
                         BS_NIL);
        if ( sts != EOK ) {
            domain_panic(dmnP, "xfer_xtnts_to_clone: bmt_refpg failed");
            xfererr = TRUE;
            orig_sts = sts;
            break;
        }
        mcp1 = &bmtp->bsMCA[mcid.cell];
        xp = NULL;
        srp = (bsShadowXtntT *)bmtr_find( mcp1,
                                          BSR_SHADOW_XTNTS,
                                          pvdp->dmnP);
        if ( srp != NULL ) {
            fxp = &srp->bsXA[0];
            xCnt = srp->xCnt;
        } else {
            xrp = (bsXtraXtntRT *)bmtr_find( mcp1,
                                             BSR_XTRA_XTNTS,
                                             pvdp->dmnP);
            if ( xrp != NULL) {
                fxp = &xrp->bsXA[0];
                xCnt = xrp->xCnt;
            } else {
                xp = (bsXtntRT *)bmtr_find(mcp1,
                                           BSR_XTNTS,
                                           pvdp->dmnP);
                if ( xp != NULL) {
                    fxp = &xp->firstXtnt.bsXA[0];
                    xCnt = xp->firstXtnt.xCnt;
                }
                else {
                    domain_panic(dmnP,
                                 "xfer_xtnts_to_clone: no extents");
                    xfererr = TRUE;
                    orig_sts = EBMTR_NOT_FOUND;
                    bs_derefpg(refPgH, BS_CACHE_IT);
                    break;
                }
            }
        }

        if ( firstVdi == 0 ) {
            firstVdi = vdIndex;
            firstBlk = fxp[0].vdBlk;
            firstPg = fxp[0].bsPage;
        }

        del_ftx_start(pmcid, pvdp, &dsc);
        if ( MCIDEQL(dsc.rstMCId, bsNilMCId) ) {
            if ( dsc.rstIndex == NORESTART ) {
                bs_derefpg(refPgH, BS_CACHE_IT);
                ftx_done_n(dsc.ftxH, FTA_BS_DEL_FTX_START_V1);
                break;
            } else {
                dsc.rstMCId = mcid;
                dsc.rstVdIndex = vdp->vdIndex;
            }
        } else if ( !MCIDEQL(dsc.rstMCId, mcid) ||
                    dsc.rstVdIndex != vdp->vdIndex) {
            bs_derefpg(refPgH, BS_CACHE_IT);
            ftx_done_n(dsc.ftxH, FTA_BS_DEL_FTX_START_V1);

            /*
             * Prepare to move to the next mcell in the chain.  If we're
             * currently in the primary mcell, the next mcell is found
             * via the chain* fields of the BSR_XTNTS record.  Otherwise,
             * it is found via the next* fields in the mcell header.
             */
            if (xp) {
                vdIndex = xp->chainVdIndex;
                mcid = xp->chainMCId;
            }
            else {
                vdIndex = mcp1->nextVdIndex;
                mcid = mcp1->nextMCId;
            }

            continue;
        }

        MS_SMP_ASSERT(dsc.rstIndex <= xCnt - 1);
        /*
         * for each extent in the subextent
         */
        for ( mapi = dsc.rstIndex; mapi < xCnt - 1; mapi++ ) {
            uint32T startBlk;

            /* Only clones have permanent holes. */
            MS_SMP_ASSERT(fxp[mapi].vdBlk != PERM_HOLE_START);

            pgstofree = pgstoadd = 0;
            startpg = fxp[mapi].bsPage + dsc.rstOffset;
            MS_SMP_ASSERT(startpg < fxp[mapi+1].bsPage);
            /*
             * for each page in the extent
             */
            for ( pg = fxp[mapi].bsPage; pg < fxp[mapi+1].bsPage; pg++ ) {
                if ( xtntType == BSXMT_STRIPE ) {
                    pageOffset = XMPAGE_TO_BFPAGE( stripeIndex,
                                              pg,
                                              cloneap->xtnts.stripeXtntMap->cnt,
                                              segSz);
                } else {
                    pageOffset = pg;
                }

                /* if the page needs to be added to the clone's map */
                if ( xfererr == FALSE &&
                     pageOffset < cloneap->maxClonePgs &&
                     !page_is_mapped(cloneap, pageOffset, NULL, TRUE) ) {
                    /* page_is_mapped returns true for a permanent hole */

                    /* Were there previous pages to be deleted? */
                    if ( pgstofree ) {
                        del_part_xtnt( pmcid,
                                       pvdp,
                                       vdp,
                                       &fxp[mapi],
                                       startpg,
                                       pgstofree,
                                       &dsc);
                        pgstofree = 0;
                        startpg = pg;   /* save the first page to COW */
                        dsc.rstOffset = pg - fxp[mapi].bsPage;
                        del_ftx_done(&dsc);
                        del_ftx_start(pmcid, pvdp, &dsc);
                    }
                    pgstoadd++;
                    tpa++;
                } else {
                    /* Were there previous pages to be COWed? */
                    if ( pgstoadd ) {
                        MS_SMP_ASSERT(startpg >= fxp[mapi].bsPage);
                        if ( fxp[mapi].vdBlk == XTNT_TERM ) {
                            startBlk = PERM_HOLE_START;
                        } else {
                            startBlk = fxp[mapi].vdBlk +
                                       (startpg - fxp[mapi].bsPage) * dsc.pgSz;
                        }
                        sts = xfer_stg( cloneap,
                                        startpg,
                                        pgstoadd,
                                        startBlk,
                                        vdIndex,
                                        dsc.ftxH);
                        if ( sts != EOK ) {
                            /* Free all remaining pages */
                            xfererr = TRUE;
                            orig_sts = sts;
                            /* free the pages we failed to add to the clone */
                            if (startBlk != PERM_HOLE_START)
                            {
                                del_part_xtnt( pmcid, pvdp,
                                               vdp,
                                               &fxp[mapi],
                                               startpg,
                                               pgstoadd,
                                               &dsc);
                            }
                        }
                        pgstoadd = 0;
                        startpg = pg;   /* save the first page to free */
                        dsc.rstOffset = pg - fxp[mapi].bsPage;
                        del_ftx_done(&dsc);
                        del_ftx_start(pmcid, pvdp, &dsc);
                    }
                    if ( fxp[mapi].vdBlk != XTNT_TERM ) {
                        pgstofree++;
                    } else {
                        startpg = pg + 1;
                    }
                }
            }   /* end for pages in extent */

            if ( pgstofree ) {
                del_part_xtnt( pmcid,
                               pvdp,
                               vdp,
                               &fxp[mapi],
                               startpg,
                               pgstofree,
                               &dsc);
            }
            if ( pgstoadd ) {
                MS_SMP_ASSERT(startpg >= fxp[mapi].bsPage);
                if ( fxp[mapi].vdBlk == XTNT_TERM ) {
                    startBlk = PERM_HOLE_START;
                } else {
                    startBlk = fxp[mapi].vdBlk +
                               (startpg - fxp[mapi].bsPage) * dsc.pgSz;
                }
                sts = xfer_stg( cloneap,
                                startpg,
                                pgstoadd,
                                startBlk,
                                vdIndex,
                                dsc.ftxH);
                if ( sts != EOK ) {
                    /* Free all remaining pages */
                    xfererr = TRUE;
                    orig_sts = sts;
                    /* free the pages we failed to add to the clone */
                    if (startBlk != PERM_HOLE_START)
                    {
                        del_part_xtnt( pmcid,
                                       pvdp,
                                       vdp,
                                       &fxp[mapi],
                                       startpg,
                                       pgstoadd,
                                       &dsc);
                    }
                }
            }
            dsc.rstIndex = mapi + 1;
            dsc.rstOffset = 0;
            del_ftx_done(&dsc);
            del_ftx_start(pmcid, pvdp, &dsc);
        }       /* end for extents */

        /* If we added any pages to the clone, update the disk. */
        /* This is all on one disk. */
        if ( tpa != 0 ) {
            MS_SMP_ASSERT(xmp->updateStart <= xmp->maxCnt);
            x_update_ondisk_xtnt_map( cloneap->dmnP,
                                      cloneap,
                                      xmp,
                                      dsc.ftxH);
        }

        /*
         * Prepare to move to the next mcell in the chain.  If we're
         * currently in the primary mcell, the next mcell is found
         * via the chain* fields of the BSR_XTNTS record.  Otherwise,
         * it is found via the next* fields in the mcell header.
         */
        if (xp) {
            vdIndex = xp->chainVdIndex;
            mcid = xp->chainMCId;
        }
        else {
            vdIndex = mcp1->nextVdIndex;
            mcid = mcp1->nextMCId;
        }

        dsc.rstVdIndex = vdIndex;
        dsc.rstMCId = mcid;
        if ( vdIndex == 0 ) {
            dsc.rstIndex = NORESTART;
        } else {
            dsc.rstIndex = 0;
        }
        dsc.rstOffset = 0;
        del_ftx_done(&dsc);
        bs_derefpg(refPgH, BS_CACHE_IT);
    }   /* end while subextents */

    if ( xfererr == FALSE ) {
        /* storage was transfered successfully */
        bs_close_one(cloneap, 0, FtxNilFtxH);
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_XFER_XTNTS_TO_CLONE);
        return;
    }

    if ( cloneap == NULL ) {
        /* clone was deleted as part of a delete fileset before we got here */
        if ( BFSET_VALID(bfSetp) ) {
            bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_XFER_XTNTS_TO_CLONE);
        }
        return;
    }

    /*
     * Mark the clone as out of sync with the original.  This
     * will prevent anyone from accessing this bitfile
     * (refpg() will return an error).
     */

    nullvp = NULL;
    sts = bs_access_one( &cloneap->origAccp,
                         cloneap->tag,
                         bfSetp->origSetp,
                         FtxNilFtxH,
                         0,
                         NULL,
                         &nullvp,
                         NULL);
    if ( sts == EOK ) {
        cloneap->origAccp->noClone = TRUE;
    }

    sts = FTX_START_N(FTA_BS_COW_PG, &ftxH, FtxNilFtxH, dmnP, 0);
    if ( sts != EOK ) {
        domain_panic(dmnP, "xfer_xtnts_to_clone: can't start ftx");
        goto err3;
    }

    print_out_of_sync_msg( cloneap->origAccp, bfSetp->origSetp, orig_sts );
    bs_bf_out_of_sync(cloneap,ftxH);
    bs_bfs_out_of_sync(cloneap->bfSetp, ftxH);
    ftx_done_n(ftxH, FTA_BS_COW_PG);
    
err3:
    bs_close_one(cloneap->origAccp, 0, FtxNilFtxH);
    bs_close_one(cloneap, 0, FtxNilFtxH);
    if ( BFSET_VALID(bfSetp) ) {
        bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_XFER_XTNTS_TO_CLONE);
    }

    return;
}


static void
del_part_xtnt( bfMCIdT pmcid,     /* in - mcell ID of primary mcell */
               vdT *pvdp,         /* in - pointer to virtual disk of primary */
               vdT *vdp,          /* in - virtual disk of storage to free */
               bsXtntT *fxp,      /* in - xtnt that maps storage to free */
               uint32T startpg,   /* in - first page in xtnt to free */
               uint32T pgstofree, /* in - number of pages to free */
               delFtxDescT *dscp)
{
    int pinPages;
    statusT sts;
    uint32T startblk;
    uint32T numblk;
    uint32T blksfreed;

    pinPages = DEL_MAX_PAGES - 1; /* One page pinned by del_ftx_start */

    FTX_LOCKWRITE( &vdp->stgMap_lk, dscp->ftxH)

    startblk = fxp->vdBlk;
    /* This extent maps real storage, not a hole. */
    MS_SMP_ASSERT(startblk != XTNT_TERM);
    MS_SMP_ASSERT(startblk != PERM_HOLE_START);

    startblk += (startpg - fxp->bsPage) * dscp->pgSz;
    numblk = pgstofree * dscp->pgSz;

    while (1) {
        /* Assume all blocks will be freed. */
        /* del_range will change blksfreed if not all blocks are freed. */
        blksfreed = numblk;
        /* del_range always returns EOK */
        (void)del_range ( startblk,
                          &blksfreed,
                          &pinPages,
                          vdp,
                          dscp->pgSz,
                          dscp->ftxH);

        numblk -= blksfreed;
        if ( numblk == 0 ) {
            return;
        }
        startblk += blksfreed;

        dscp->rstOffset = (startblk - fxp->vdBlk) / dscp->pgSz;

        del_ftx_done(dscp);
        del_ftx_start(pmcid, pvdp, dscp);

        FTX_LOCKWRITE( &vdp->stgMap_lk, dscp->ftxH)

        /*
         * Reset maximum number of pages that may be pinned.
         */
        pinPages = DEL_MAX_PAGES - 1;
    }       /* end of while loop */

}


/*
 * Zero a range of pages in the storage bit map.
 */

static statusT
del_range(
    uint32T start,      /* in - start of range to zero */
    uint32T *count,     /* in/out - number of blocks to free / number of
                           blocks actually freed */
    int *pinPages,      /* in/out - maximum number of pages that can be
                           pinned / number of pages that were pinned */
    vdT *vdp,           /* in - virtual disk containing range */
    int pgSz,           /* in - page size */
    ftxHT ftxH          /* in transaction handle */
    )
{
    statusT sts;

    sbm_howmany_blks(start, count, pinPages, vdp, pgSz);

    if (*count > 0) {
        if ((sts = sbm_return_space_no_sub_ftx(vdp, start, *count, ftxH))
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

    if ((sts = FTX_START_N(FTA_BS_DEL_FTX_START_V1, &ddp->ftxH, FtxNilFtxH,
                           pvdp->dmnP, DEL_MAX_PAGES)) != EOK) {
        ADVFS_SAD0 ("del_ftx_start: Can't start transaction");
    }

    if ((sts = rbf_pinpg(&ddp->pinPgH, (void **)&bmtp, pvdp->bmtp, 
                         pmcid.page, BS_NIL, ddp->ftxH)) != EOK) {
        ADVFS_SAD0 ("del_ftx_start: Can't ref BMT page for primary mcell");
    }

    pmcp = &bmtp->bsMCA[pmcid.cell];

    /*
     * Find the primary xtnt record.
     */
    if ((rp = (bsXtntRT *) bmtr_find(pmcp, BSR_XTNTS, pvdp->dmnP))
                                                                == NULL) {
        ADVFS_SAD0 ("del_ftx_start: no primary xtnt record");
    }
    ddp->pxp = rp;
    ddp->rstOffset = rp->delRst.offset;
    ddp->rstIndex = rp->delRst.xtntIndex;
    ddp->rstVdIndex = rp->delRst.vdIndex;
    ddp->rstMCId = rp->delRst.mcid;
    ddp->pgSz = rp->blksPerPage;

    return (EOK);
}


static void
del_ftx_done(
    delFtxDescT *ddp     
    )
{
    rbf_pin_record(ddp->pinPgH, (void *)&ddp->pxp->delRst, 
                   sizeof(ddp->pxp->delRst));

    ddp->pxp->delRst.xtntIndex = ddp->rstIndex;
    ddp->pxp->delRst.offset = ddp->rstOffset;
    ddp->pxp->delRst.mcid = ddp->rstMCId;
    ddp->pxp->delRst.vdIndex = ddp->rstVdIndex;

    ftx_done_n(ddp->ftxH, FTA_BS_DEL_FTX_START_V1);
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
    unsigned long pgnum;

    vd = VD_HTOP(vdIndex, domain);

    DELLIST_LOCK_WRITE(&(vd->del_list_lk) )

    unlockFlag = 1;

    /*
     * Examine the first entry on the deferred-delete list.
     */

    if ( RBMT_THERE(domain) ) {
        pgnum = MCELL_LIST_PAGE;
    } else {
        pgnum = MCELL_LIST_PAGE_V3;
    }

    sts = bmt_refpg( &pgRef, (void *)&bmt, vd->bmtp, pgnum, BS_NIL);
    if (sts != EOK) {
        if (sts == E_PAGE_NOT_MAPPED) {
            /*
             * The list head page doesn't exist yet.  Treat as if it does and
             * the list is empty.
             */
            sts = ENO_MORE_MCELLS;
        }
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
    bs_derefpg(pgRef, BS_CACHE_IT);

    while ((mcellId.page != bsNilMCId.page) || (mcellId.cell != bsNilMCId.cell)) {

        sts = bmt_refpg( &pgRef, (void *)&bmt, vd->bmtp, mcellId.page, BS_NIL);
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
        mcellBfSetTag = mcell->bfSetTag;
        mcellBfTag = mcell->tag;

        bs_derefpg(pgRef, BS_CACHE_IT);

        if (BS_BFTAG_EQL (bfSetTag, mcellBfSetTag) &&
            BS_BFTAG_EQL (bfTag, mcellBfTag)) {
            break;  /* out of while */
        }

        mcellId = nextMcellId;

    }  /* end while */

    unlockFlag = 0;
    DELLIST_UNLOCK(&(vd->del_list_lk) )

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
        bs_derefpg(pgRef, BS_CACHE_IT);
    }
    if (unlockFlag != 0) {
        DELLIST_UNLOCK(&(vd->del_list_lk) )
    }

    return sts;

}  /* end del_find_del_entry */
