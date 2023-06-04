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
 *  AdvFS/bs
 *
 * Abstract:
 *
 *  bs_create.c
 *  This module contains routines used to create bitfiles.
 *
 */
#include <ms_public.h>
#include <ms_privates.h>
#include <ms_assert.h>
#include <bs_extents.h>
#include <bs_snapshot.h>
#include <bs_access.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE BS_CREATE

/*
 * private prototypes.
 */

statusT
new_mcell(
    ftxHT parFtx,         /* in - parent ftx */
    mcellUIdT* mcellUId,  /* in/out - ptr to global mcell id */
    bsBfAttrT* bfAttr,    /* in - ds attributes */
    domainT* dmnP         /* in - domain ptr */
    );

static void
make_mcell_valid(
    mcellUIdT mcelluid,   /* in */
    bfSetT *bfSetp,       /* in */
    ftxHT ftxH            /* in */
    );

static void
kill_mcell(
    bfSetT *bfSetp,
    mcellUIdT* mcelluidp,
    ftxHT ftxH
    );

statusT
dealloc_mcells (
    domainT *dmnP,         /* in */
    bfMCIdT firstMcellId,  /* in */
    ftxHT parentFtxH       /* in */
    );

/*
 *  rbf_create -  recoverable bitfile create
 *
 *  Returns possible error from select_vd, new_mcell
 *  and tagdir_insert_tag. Or returns EOK.
 */

statusT
rbf_create( bfTagT *tag,             /* in or out */
            bfSetT *bfSetp,          /* in */
            bfParamsT *bfParams,     /* in */
            ftxHT parentFtx,         /* in */
            adv_create_flags createFlags, /* in */
            bfTagFlagsT tagFlags)    /* in */
{
    struct domain  *dmnP;
    bsBfAttrT       bfAttr;
    statusT         sts;
    ftxHT           ftxH;
    mcellUIdT       mcellUId;
    bsTDirPgT      *tdpgp;
    bsTMapT        *tdmap;
    rbfPgRefHT      tdpgRef;
    unLkActionT     unlkAction;
    bfAccessT      *bfap = NULL;

    *tag = NilBfTag;
    dmnP = bfSetp->dmnP;

    if (bfSetp->bfSetFlags & BFS_OD_READ_ONLY) {
        return( E_READ_ONLY );
    }
    /* start a new "create" ftx */

    sts = FTX_START_N(FTA_NULL, &ftxH, parentFtx, dmnP);
    if (sts != EOK) {
        ADVFS_SAD1( "rbf_create: ftx start failed", sts );
    }

    /*
     * Setup the bitfile attributes.  The entire structure is
     * initialized field by field here for clarity, as opposed to
     * using an initializer.  These are passed to the new mcell
     * routine.
     */

    /* get bitfile attributes from input parameters */

    bfAttr.bfPgSz  = bfParams->bfpPageSize;
    bfAttr.reqServices = bfParams->cl.reqServices;

    if (SC_EQL( bfAttr.reqServices, nilServiceClass )) {
        /* TEMP - need real 'default service class' support */
        bfAttr.reqServices = defServiceClass;
    }

    /* set ftxid into transition field of bitfile attributes */

    bfAttr.state = BSRA_CREATING;
    bfAttr.transitionId = get_ftx_id(ftxH);
    bfAttr.bfat_orig_file_size = ADVFS_ROOT_SNAPSHOT;
    bfAttr.bfat_rsvd_file_size = 0;
    bfAttr.bfat_del_child_cnt = 0;
    bfAttr.rsvd1 = 0;

    /* bring in the clones... ok, snapshots, but this was the best comment
     * of them all before we changed the terminology. */
    

    /*
     * Get the next tag number to create.  This subroutine acquires
     * the tagdir lock as part of this ftx.  It is held until after
     * the tagdir_insert_tag routine has actually done its thing.
     */
    /* NOTE!!! - mcellUId.ut.tag.seq will NO LONGER be marked INUSE when 
     *           returning from tagdir_alloc_tag() call.  The INUSE flag
     *           will be set in a later cal to tagdir_insert_tag().
     */
    sts = tagdir_alloc_tag(ftxH, &mcellUId, bfSetp, &tdpgp, &tdmap, &tdpgRef,
                           (createFlags & CRT_CHK_MIGSTG_LOCK) ? 1 : 0  );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    /* 
     * Access the bfAccess object.  This bumps the ref count and locks the
     * bfap. 
     */

    bfap = advfs_lookup_valid_bfap(bfSetp, 
                                   mcellUId.ut.tag, 
                                   (createFlags & CRT_INTERNAL) ?
                                        BF_OP_INTERNAL : BF_OP_NO_FLAGS,
                                   ftxH); 

    if (bfap == NULL) {
        RAISE_EXCEPTION( EHANDLE_OVF );
    }

    if (lk_get_state( bfap->stateLk ) != ACC_INIT_TRANS) {
        ADVFS_SAD0("rbf_create: impossible bfAccess state");
    }

    /*
     * We are already in ACC_INIT_TRANS so we can release the lock
     */
    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

    mcellUId.ut.bfsid = bfSetp->bfSetId;

    /* get next mcell and init it */

    sts = new_mcell( ftxH, &mcellUId, &bfAttr, dmnP);
    if (sts != EOK) {RAISE_EXCEPTION( sts );}

    /* insert new tag */

    /* If we are creating a file, it could not possibly have a parent. */
    tagFlags |= BS_TD_ROOT_SNAPSHOT;

    sts = tagdir_insert_tag(ftxH, &mcellUId, bfSetp, tdpgp, tdmap,
                            tagFlags, tdpgRef );
    if (sts != EOK) {RAISE_EXCEPTION( sts );}

    /* Dereference the bfAccess object. Set the ACC_CREATING state
     * that will behave as ACC_VALID in most ways.
     */
    ADVSMP_BFAP_LOCK(&bfap->bfaLock);

    unlkAction = lk_set_state( &bfap->stateLk, ACC_CREATING);

    lk_signal( unlkAction, &bfap->stateLk );

    /* 
     * DEC_REFCNT will unconditionally unlock bfap->bfaLock 
     */
    DEC_REFCNT( bfap, DRC_CLOSED_LIST );

    /* 
     * Note that the new mcell was initialized to the ACC_CREATING
     * state so that any attempts to access it at this time will put
     * the accessor into the ACC_CREATING state, therefore no additional
     * locks appear to be necessary.  The root-done action will change
     * the state of the mcell, as well as take the bsaccess structure
     * out of ACC_CREATING, if some other thread has got it there.
     */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_133);
    ftx_done_urd( ftxH, FTA_BS_BF_CREATE_V1,
                 sizeof(mcellUId), &mcellUId,
                 sizeof(mcellUId), &mcellUId ); 
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_134);

    *tag = mcellUId.ut.tag;
    return( EOK );

HANDLE_EXCEPTION:
    
    if ( bfap ) {
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );

        unlkAction = lk_set_state( &bfap->stateLk, ACC_INVALID );

        lk_signal( unlkAction, &bfap->stateLk );

        /* 
         * DEC_REFCNT will unconditionally unlock bfaLock.  Note that the
         * DRC_CLOSED_LIST flag is passed to DEC_REFCNT, but the
         * ACC_INVALID state will cause it to be deallocated.
         */
        DEC_REFCNT( bfap, DRC_CLOSED_LIST );

    }

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_364);
    ftx_fail( ftxH );
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_365);

    *tag = NilBfTag;
    return sts;
}

/*
 * create_rtdn_opx
 *
 * create root done opx 
 * the typedef opxT is a function prototype; see ftx_public.h
 *
 * No error return.
 *
 */

opxT create_rtdn_opx;

void
create_rtdn_opx(ftxHT ftxH,     /* in - ftx handle */
                int rtdnRecSz,  /* in - root done record size */
                void* opRec     /* in */
                )
{
    mcellUIdT rtdnR;
    statusT sts;
    bfSetT *bfSetp;
    domainT *dmnP;
    int bfs_opened = 0;

    dmnP = ftxH.dmnP;

    bcopy(opRec, &rtdnR, sizeof(mcellUIdT));

    if (rtdnRecSz != sizeof(mcellUIdT)) {
        ADVFS_SAD0("create_rtdn_opx: bad root done record size");
    }

    if ( !BS_UID_EQL(rtdnR.ut.bfsid.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(rtdnR.ut.bfsid.domainId, dmnP->dualMountId)) )
        {
           /* I can't hit this at all. */
           /* I'm not sure it's needed, but I'll leave it in. */
            rtdnR.ut.bfsid.domainId = dmnP->domainId;
        } else {
            ADVFS_SAD0("create_rtdn_opx: domainId mismatch");
        }
    }

    if (dmnP->state != BFD_ACTIVATED) {
        sts = bfs_open(&bfSetp, rtdnR.ut.bfsid, BFS_OP_IGNORE_DEL, ftxH);
        if ( sts != EOK ) {
          domain_panic(ftxH.dmnP, "create_rtdn_opx: failed to open bfs");
          goto HANDLE_EXCEPTION;
        }
        bfs_opened = 1;
    } else {
        bfSetp = bfs_lookup( rtdnR.ut.bfsid );
    }
    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );

    make_mcell_valid( rtdnR, bfSetp, ftxH );

    if ( bfs_opened ) {
        sts = bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
        if ( sts != EOK ) {
          domain_panic(ftxH.dmnP, "create_rtdn_opx: bfs close failed");
          goto HANDLE_EXCEPTION;
        }
    }

HANDLE_EXCEPTION:
    return;
}

/*
 * make_mcell_valid
 *
 * This is called from the root done routine for bitfile create.  It
 * sets the on-disk bitfile state to VALID, as well as waking up any
 * waiters on ACC_CREATING.
 */

static void
make_mcell_valid(
                 mcellUIdT mcelluid,    /* in */
                 bfSetT *bfSetp,        /* in - bitfile set handle */
                 ftxHT ftxH             /* in */
                 )
{
    struct bfAccess* bfap;
    struct bsMPg* bmtp;
    bsBfAttrT* odattr;
    struct vd* vdp;
    struct domain* dmnP;
    rbfPgRefHT pgref;
    statusT sts;

    dmnP = ftxH.dmnP;

    vdp = VD_HTOP(mcelluid.mcell.volume, dmnP);

    /*
     * Open the bfap internally to prevent v_count being bumped.
     * Open only in memory copies since the bfap should still be in cache.  
     */
    bfap = advfs_lookup_valid_bfap(bfSetp, 
                                   mcelluid.ut.tag, 
                                   BF_OP_INTERNAL |
                                   BF_OP_INMEM_ONLY |
				   BF_OP_OVERRIDE_SMAX,
                                   ftxH
                                  );
    /*
     * Bfap may not already be in cache if we are in recovery 
     * which is fine, just update the on disk state.
     */

    /* 
     * The state should always be ACC_CREATING if we have a bfap.
     * If we were last in
     * rbf_create, then we left the access structure in cache in
     * ACC_CREATING.  If we did an rbf_create followed by an access, we
     * would still be in ACC_CREATING.  
     */
    MS_SMP_ASSERT( (!bfap) || (lk_get_state( bfap->stateLk ) == ACC_CREATING) );


    if (bfap) {
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    }

    sts = rbf_pinpg(&pgref, (void*)&bmtp, vdp->bmtp,
                     mcelluid.mcell.page, BS_NIL, ftxH, MF_VERIFY_PAGE);
    if (sts != EOK) {
        ADVFS_SAD2("create_rtdn_opx: failed to access bmt pg N1, err N2",
                   mcelluid.mcell.page, sts );
    }
    if ( bmtp->bmtPageId != FETCH_MC_PAGE(mcelluid.mcell) ) {
        ADVFS_SAD2("create_rtdn_opx: got bmt page N1 instead of N2",
                   bmtp->bmtPageId, mcelluid.mcell.page );
    }

    odattr = bmtr_find( &bmtp->bsMCA[ mcelluid.mcell.cell ], BSR_ATTR, dmnP);
    if (odattr == 0) {
        ADVFS_SAD0("create_rtdn_opx: can't find ds attributes");
    }

    if (odattr->state == BSRA_CREATING) {

        /*
         * It is in the CREATING state, so set it VALID now.  There
         * are a number of ways it can be different states also, in
         * which case the correct action is to do nothing.
         */

        rbf_pin_record(pgref, &odattr->state, sizeof(bfStatesT));
        odattr->state = BSRA_VALID;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_135);

        if (bfap) {
            /*
             * We only want to do this if we were in ACC_CREATING, but we
             * asserted this condition above.  This is done simply
             * because we may have come here straight from rbf_create in which
             * case the bfap is really not fully initialized in cache (it never
             * went through bs_access).  The next access of the file will be
             * through bs_access and will fully initialize the bfap.  The other
             * option is to do a bs_access/bs_close here, but that is predictive
             * without having sufficient information to predict the next usage
             * of the file.  We will set the in memory state to valid
             * so that the next access will reload the bfap.
             */
            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            if (bfap->bfState == BSRA_CREATING) {
                bfap->bfState = BSRA_VALID;
            } else {
                MS_SMP_ASSERT( bfap->bfState == BSRA_INVALID );
                bfap->bfaFlags &= ~BFA_MAPPED;
            }

            ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, ftxH);
        }
    } else {
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    }


    if (bfap) {
        DEC_REFCNT( bfap, DRC_CLOSED_LIST);
    }

}

opxT create_undo_opx;

void
create_undo_opx(ftxHT ftxH,     /* in - ftx handle */
                int undoRecSz,  /* in - undo record size */
                void* opRec     /* in */
                )
{
    mcellUIdT undoRp; 
    statusT sts;
    bfSetT *bfSetp;
    domainT *dmnP = ftxH.dmnP;

    bcopy(opRec, &undoRp, sizeof(undoRp) ); 

    if (undoRecSz != sizeof(mcellUIdT)) {
        ADVFS_SAD0("create_undo_opx: bad root done record size");
    }

    if ( !BS_UID_EQL(undoRp.ut.bfsid.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(undoRp.ut.bfsid.domainId, dmnP->dualMountId)) )
        {
            undoRp.ut.bfsid.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("create_undo_opx: domainId mismatch");
        }
    }

    sts = bfs_open( &bfSetp, undoRp.ut.bfsid, BFS_OP_IGNORE_DEL, ftxH );
    if ( sts != EOK ) {
      domain_panic(dmnP, "create_undo_opx: failed to open bfs");
      goto HANDLE_EXCEPTION;
    }

    /*
     * In order to prevent a hierarchy violation we grab the
     * BfSetTblLock prior to calling tagdir_remove_tag().
     * Later, when we call bs_bfs_close() we will already
     * have the lock.
     */
    ADVRWL_BFSETTBL_WRITE( bfSetp->dmnP );

    tagdir_tag_to_freelist(bfSetp, &undoRp.ut.tag, ftxH, TRUE);

    kill_mcell( bfSetp, &undoRp, ftxH );

    sts = bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    if ( sts != EOK ) {
      domain_panic(ftxH.dmnP,"create_undo_opx: bfs close failed");
      goto HANDLE_EXCEPTION;
    }

HANDLE_EXCEPTION:
    return;
}


/*
 * init_bscreate_opx
 *
 *  register opx routines for create.
 *
 * No error return.
 */


void 
init_bscreate_opx(void)
{
    statusT sts;

    sts = ftx_register_agent_n(FTA_BS_BF_CREATE_V1,
                             &create_undo_opx,
                             &create_rtdn_opx,
                             (opxT *) 0
                             );
    if (sts != EOK) {
        ADVFS_SAD1("init_bscreate_opx:register failure", sts );
    }
}


/*
 * new_mcell 
 *
 * initialize a new metadata cell.  Returns with vd's mcell_lk locked
 * by parent ftx.
 *
 * Returns ENO_MORE_BLKS, EBAD_PARAMS, errors from vd select or EOK
 */


statusT
new_mcell(
          ftxHT parFtx,         /* in - parent ftx */
          mcellUIdT* mcellUIdp, /* in/out - ptr to global mcell id */
          bsBfAttrT* bfAttr,    /* in - ds attributes */
          domainT* dmnP         /* in - domain ptr */
          )
{
    statusT sts;
    bsBfAttrT* odattrp;
    struct bsXtntR* xtntp;
    struct vd* vdp;
    struct bsMC* mcp;
    rbfPgRefHT pgref;

    /* select a vd for the new bitfile */

    while (TRUE) {

        sts = sc_select_vd_for_mcell( &vdp,
                                  parFtx.dmnP,
                                  dmnP->scTbl, 
                                  bfAttr->reqServices );

        if (sts != EOK) {
            return ENO_MORE_BLKS;
        }

        if ( (bfAttr->bfPgSz / ADVFS_FOBS_PER_DEV_BSIZE) % vdp->stgCluster != 0 ) {
            vd_dec_refcnt(vdp);
            return EBAD_PARAMS;
        }

        ADVFTM_MCELL_LOCK( &vdp->mcell_lk );
        if ((sts = bmt_alloc_prim_mcell( 
                                        parFtx, 
                                        mcellUIdp, 
                                        vdp, 
                                        &pgref, 
                                        &mcp)) != ENO_MORE_MCELLS) {
                vd_dec_refcnt(vdp);
                MS_SMP_ASSERT(FETCH_MC_VOLUME(mcellUIdp->mcell) == vdp->vdIndex);
                FTX_ADD_MUTEX(&(vdp->mcell_lk), parFtx);
                break;
        }
        /*
         * Got ENO_MORE_MCELLS; unlock mcell_lk and try next volume
         */
        ADVFTM_MCELL_UNLOCK( &vdp->mcell_lk );
        vd_dec_refcnt(vdp);
    }

    if(sts != EOK) {
        return ENO_MORE_BLKS;
    }

    /* pin mcell record */

    rbf_pin_record(pgref, mcp->bsMR0, sizeof(mcp->bsMR0));

    odattrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), mcp);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_136);
    if (odattrp == 0) {
        ADVFS_SAD0( "new_mcell: can't assign attr record" );
    }

    /* set on-disk attributes */

    *odattrp = *bfAttr;
    MS_SMP_ASSERT(odattrp->bfPgSz != 0);

    /* init a null extent record */

    xtntp = bmtr_assign(BSR_XTNTS, sizeof(struct bsXtntR), mcp);
    if (xtntp == 0) {
        ADVFS_SAD0( "new_mcell: can't assign extent record" );
    }

    xtntp->chainMCId = bsNilMCId;
    xtntp->rsvd1 = 0; 
    xtntp->rsvd2 = 0;
    bzero((char *)&xtntp->delRst , sizeof(delRstT));

    /* 
     * First extent (no longer own structure) is stored in the primary
     * mcell, initialize the first extent fields.
     */
    xtntp->mcellCnt = 1;
    xtntp->xCnt = 1;
    xtntp->bsXA[0].bsx_fob_offset = 0;
    xtntp->bsXA[0].bsx_vd_blk= -1;

    return EOK;
}

/*
 * kill_mcell
 *
 * No error return.
 */

static void
kill_mcell(
           bfSetT *bfSetp,
           mcellUIdT* mcelluidp,
           ftxHT ftxH
           )
{
    struct bfAccess* bfap;
    domainT* dmnP;
    vdT* vdp;
    rbfPgRefHT pgref;
    bsMPgT* bmtpgp;
    statusT sts;
    bsMCT* mcp;
    bsBfAttrT* odattrp;

    dmnP = ftxH.dmnP;

    vdp = VD_HTOP(mcelluidp->mcell.volume, dmnP);

    sts = rbf_pinpg( &pgref, (void*)&bmtpgp, vdp->bmtp,
                    mcelluidp->mcell.page, BS_NIL, ftxH, MF_NO_VERIFY );
    if (sts != EOK) {
        domain_panic(dmnP,
                     "kill_mcell: failed to access bmt pg %ld, err %d", 
                     mcelluidp->mcell.page, sts );
        return;
    }
    if ( bmtpgp->bmtPageId != FETCH_MC_PAGE(mcelluidp->mcell)) {
        domain_panic(dmnP, "kill_mcell: got bmt page %ld instead of %ld",
                     bmtpgp->bmtPageId, mcelluidp->mcell.page );
        return;
    }

    mcp = &bmtpgp->bsMCA[mcelluidp->mcell.cell];

    /* find attributes, set state invalid */

    odattrp = bmtr_find( mcp, BSR_ATTR, dmnP);
    if ( !odattrp ) {
        domain_panic(dmnP, "kill_mcell: can't find bf attributes");
        return;
    }
    rbf_pin_record( pgref, &odattrp->state, sizeof(odattrp->state) );
    odattrp->state = BSRA_INVALID;
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_137);

    /*
     * Open the access structure internal to prevent v_count being bumped.
     */
    bfap = advfs_lookup_valid_bfap(bfSetp, 
                                   mcp->mcTag, 
                                   BF_OP_INTERNAL |
				   BF_OP_INMEM_ONLY |
				   BF_OP_OVERRIDE_SMAX,
                                   ftxH);
    if (bfap != NULL) {
        /*
         * We expect the state here to be either ACC_CREATING or not in memory.
         * If we are failing a transaction, then rbf_create will have run 
         * and put the bfap into ACC_CREATING.  Before create_rtdn_opx runs, 
         * we failed the transaction but still found the bfap in cache.  If we
         * are running in recovery, we are counting on the BSRA_INVALID state
         * set above to prevent anyone from finding this bfap by doing a lookup.
         * Of course, recovery is single threaded, so no one should do this
         * anyways.
         */
        MS_SMP_ASSERT( (lk_get_state( bfap->stateLk ) == ACC_CREATING));

        /*
         * We need to invalidate all dirty pages for this bitfile.
         * The bfaLock must be released while calling UFC.
         */

        ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
        fcache_vn_invalidate( &bfap->bfVnode, 
                              0, /* Entire File */
                              0, /* Entire File */ 
                              NULL,
                              (BS_IS_META(bfap)?FVI_PPAGE:0)|FVI_INVAL );
        ADVSMP_BFAP_LOCK(&bfap->bfaLock);

        bfap->bfState = BSRA_INVALID;

        lk_signal( lk_set_state( &bfap->stateLk, ACC_INVALID ), &bfap->stateLk);

        DEC_REFCNT( bfap, DRC_DEALLOC);
    }
    
    sts = dealloc_mcells (dmnP, mcelluidp->mcell, ftxH);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_138);
    if (sts != EOK) {
        domain_panic(dmnP, "kill_mcell: dealloc_mcells() failed - sts %d",
                     sts);
        return;
    }
}

/*
 * dealloc_mcells
 *
 *
 * This function deallocates a list of mcells, starting with the specified
 * mcell.
 *
 * This function assumes that the mcells are not on any list other than the
 * list specified by the caller and that the caller has exclusive access to
 * the list.
 *
 * This function assumes that the caller is kill_mcell() and that it is an
 * undo routine.
 *
 * NOTE:  This function assumes that the number of mcells to deallocate is small.
 */


statusT
dealloc_mcells (
                domainT *dmnP,  /* in */
                bfMCIdT firstMcellId,  /* in */
                ftxHT parentFtxH  /* in */
                )
{
    bsMPgT* bmtp = NULL;
    bfMCIdT mcellId;
    bsMCT* mcp = NULL;
    bfMCIdT nextMcellId;
    rbfPgRefHT pgPin;
    statusT sts;
    vdT *vdp;

    mcellId = firstMcellId;

    /*
     ** Deallocate each mcell.
     */

    while ( mcellId.volume != bsNilVdIndex) {
        vdp = VD_HTOP(mcellId.volume, dmnP);

        /*
         * Acquire the vd's free mcell list lock just long enough to insert
         * the mcell onto the free list.  Because our caller is an undo routine
         * and the effects of undo routines cannot be undone, we don't have to
         * worry about some other thread allocating the mcell.
         */
        ADVFTM_MCELL_LOCK( &vdp->mcell_lk );

        sts = rbf_pinpg( &pgPin, (void*)&bmtp, vdp->bmtp,
                        mcellId.page, BS_NIL, parentFtxH, MF_NO_VERIFY );
        if (sts != EOK) {
            ADVFTM_MCELL_UNLOCK( &vdp->mcell_lk );
            return sts;
        }
        mcp = &bmtp->bsMCA[ mcellId.cell ];
        nextMcellId = mcp->mcNextMCId;
            
        bmt_free_mcell (parentFtxH, vdp, mcp, mcellId, bmtp, pgPin);

        ADVFTM_MCELL_UNLOCK( &vdp->mcell_lk  );

        /*
         ** Go to next mcell.
         */

        mcellId = nextMcellId;

    }  /* end while */

    return EOK;

}  /* end dealloc_mcells */
