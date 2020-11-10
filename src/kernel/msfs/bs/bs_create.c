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
 *  ADVFS/bs
 *
 * Abstract:
 *
 *  bs_create.c
 *  This module contains routines used to create bitfiles.
 *
 * Date:
 *
 *  Thu Sep 27 15:01:26 1990
 */
/*
 * HISTORY
 * 
 */
#pragma ident "@(#)$RCSfile: bs_create.c,v $ $Revision: 1.1.132.3 $ (DEC) $Date: 2008/02/12 13:06:51 $"

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_assert.h>

#define ADVFS_MODULE BS_CREATE

extern unsigned TrFlags;

/*
 * private prototypes.
 */

statusT
new_mcell(
          ftxHT parFtx,         /* in - parent ftx */
          mcellUIdT* mcellUId,  /* in/out - ptr to global mcell id */
          bsBfAttrT* bfAttr,    /* in - ds attributes */
          domainT* dmnP,        /* in - domain ptr */
          bsInMemXtntT *oxtntp   /* in - ptr to orig extent map */
          );

static statusT
rbf_int_create(
               bfTagT *tag,             /* in or out */
               bfSetT *bfSetp,          /* in */
               bfParamsT *bfParams,     /* in */
               ftxHT parentFtx,         /* in */
               int checkmigtrunclock    /* in */
               );

static void
make_mcell_valid(
                 mcellUIdT mcelluid,    /* in */
                 bfSetT *bfSetp,        /* in */
                 ftxHT ftxH             /* in */
                 );

static void
kill_mcell(
           bfSetT *bfSetp,
           mcellUIdT* mcelluidp,
           ftxHT ftxH
           );

statusT
dealloc_mcells (
                domainT *dmnP,  /* in */
                uint16T firstVdIndex,  /* in */
                bfMCIdT firstMcellId,  /* in */
                ftxHT parentFtxH  /* in */
                );

/*
 * private definitions
 */

/*
 * rbf_create
 *
 * recoverable bitfile create
 *
 * Returns status passed from rbf_int_create call.
 */

statusT
rbf_create(
           bfTagT *tag,                 /* out */
           bfSetT *bfSetp,              /* in */
           bfParamsT *bfParams,         /* in */
           ftxHT parentFtxH,            /* in */
           int checkmigtrunclock        /* in */
           )
{
    statusT sts;

    *tag = NilBfTag;
    
    sts = rbf_int_create( tag, bfSetp, bfParams, parentFtxH,
                         checkmigtrunclock);
    return (sts);
}

/*
 * rbf_int_create 
 *
 *  recoverable bitfile create
 *
 * Returns possible error from select_vd, new_mcell
 * and tagdir_insert_tag. Or returns EOK.
 */

static statusT
rbf_int_create(
               bfTagT *tag,             /* in or out */
               bfSetT *bfSetp,          /* in */
               bfParamsT *bfParams,     /* in */
               ftxHT parentFtx,         /* in */
               int checkmigtrunclock    /* in */
               )
{
    struct domain* dmnP;
    bsBfAttrT bfAttr;
    statusT sts;
    ftxHT ftxH;
    mcellUIdT mcellUId;
    bsTDirPgT* tdpgp;
    bsTMapT* tdmap;
    rbfPgRefHT tdpgRef;
    unLkActionT unlkAction;
    bfAccessT* bfap = NULL;


    dmnP = bfSetp->dmnP;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return E_READ_ONLY;
    }

    if (TrFlags&trCreate) {
        trace_hdr();
    }

    /* start a new "create" ftx */

    sts = FTX_START_N(FTA_NULL, &ftxH, parentFtx, dmnP, 4);
    if (sts != EOK) {
        ADVFS_SAD1( "rbf_int_create: ftx start failed", sts );
    }

    /*
     * Setup the bitfile attributes.  The entire structure is
     * initialized field by field here for clarity, as opposed to
     * using an initializer.  These are passed to the new mcell
     * routine.
     */

    /* get bitfile attributes from input parameters */

    bfAttr.bfPgSz  = bfParams->pageSize;
    bfAttr.cl.extendSize = 0;
    bfAttr.cl.dataSafety = bfParams->cl.dataSafety;
    bfAttr.cl.reqServices = bfParams->cl.reqServices;
    bfAttr.cl.optServices = bfParams->cl.optServices;

    if (SC_EQL( bfAttr.cl.reqServices | bfAttr.cl.optServices, nilServiceClass )) {
        /* TEMP - need real 'default service class' support */
        bfAttr.cl.reqServices = defServiceClass;
        bfAttr.cl.optServices = nilServiceClass;
    }

    /* set ftxid into transition field of bitfile attributes */

    bfAttr.state = BSRA_CREATING;
    bfAttr.transitionId = get_ftx_id(ftxH);

    /* bring in the clones...  */

    bfAttr.cloneId = bfSetp->cloneId;
    bfAttr.cloneCnt = bfSetp->cloneCnt;
    bfAttr.maxClonePgs = 0;
    bfAttr.deleteWithClone = 0;
    bfAttr.outOfSyncClone = 0;

    bzero( (char *) bfAttr.cl.clientArea, sizeof( bfAttr.cl.clientArea ) );

    bfAttr.cl.rsvd1 = 0;
    bfAttr.cl.rsvd2 = 0;
    bfAttr.cl.acl = NilBfTag;          /* rsvd for ACL */
    bfAttr.cl.rsvd_sec1 = 0;           /* rsvd for security stuff */
    bfAttr.cl.rsvd_sec2 = 0;           /* rsvd for security stuff */
    bfAttr.cl.rsvd_sec3 = 0;           /* rsvd for security stuff */

    /*
     * Get the next tag number to create.  This subroutine acquires
     * the tagdir lock as part of this ftx.  It is held until after
     * the tagdir_insert_tag routine has actually done its thing.
     */

    sts = tagdir_alloc_tag(ftxH, &mcellUId, bfSetp, &tdpgp, &tdmap, &tdpgRef,
                           checkmigtrunclock);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    /* access the bfAccess object.  this bumps the ref count. */

    bfap = grab_bsacc(bfSetp, mcellUId.ut.tag, FALSE, NULL); /* locks bfap->bfaLock */
    if (bfap == NULL) {
        RAISE_EXCEPTION( EHANDLE_OVF );
    }

    if (lk_get_state( bfap->stateLk ) != ACC_INVALID) {
        ADVFS_SAD0("rbf_int_create: impossible bfAccess state");
    }

    /*
     * Set the bfAccess object for the to-be-created bitfile into
     * the INIT_TRANS state.  This serializes us with (presumably
     * ill-behaved) other threads that could be attempting to access
     * this tag.  This also serializes us with ill-behaved threads
     * that use the same ftx handle.
     */

    unlkAction = lk_set_state( &bfap->stateLk, ACC_INIT_TRANS );

    mutex_unlock(&bfap->bfaLock);

    mcellUId.ut.bfsid = bfSetp->bfSetId;

    /* get next mcell and init it */

    sts = new_mcell( ftxH, &mcellUId, &bfAttr, dmnP, NULL );
    if (sts != EOK) {RAISE_EXCEPTION( sts );}

    /* insert new tag */

    sts = tagdir_insert_tag(ftxH, &mcellUId, bfSetp, tdpgp, tdmap,
                            tdpgRef );
    if (sts != EOK) {RAISE_EXCEPTION( sts );}

    /* dereference the bfAccess object, set into INVALID state */
    mutex_lock(&bfap->bfaLock);
    unlkAction = lk_set_state( &bfap->stateLk, ACC_INVALID );

    lk_signal( unlkAction, &bfap->stateLk );

    DEC_REFCNT( bfap );
    mutex_unlock(&bfap->bfaLock);

    /* 
     * Note that the new mcell was initialized to the CREATING
     * state so that any attempts to access it at this time will put
     * the accessor into the FTX_TRANS state, therefore no additional
     * locks appear to be necessary.  The root-done action will change
     * the state of the mcell, as well as take the bsaccess structure
     * out of FTX_TRANS, if some other thread has got it there.
     */

    ftx_done_urd( ftxH, FTA_BS_BF_CREATE_V1,
                 sizeof(mcellUId), &mcellUId,
                 sizeof(mcellUId), &mcellUId ); 

    if (TrFlags&trCreate) {
        trace_hdr();
    }

    *tag = mcellUId.ut.tag;
    return( EOK );

HANDLE_EXCEPTION:
    
    if ( bfap ) {
        mutex_lock( &bfap->bfaLock );

        unlkAction = lk_set_state( &bfap->stateLk, ACC_INVALID );

        lk_signal( unlkAction, &bfap->stateLk );

        DEC_REFCNT( bfap );

        mutex_unlock( &bfap->bfaLock );
    }

    ftx_fail( ftxH );

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
    mcellUIdT rtdnR = *(mcellUIdT*)opRec;
    statusT sts;
    bfSetT *bfSetp;
    domainT *dmnP;
    int bfs_opened = 0;

    dmnP = ftxH.dmnP;

    if (rtdnRecSz != sizeof(mcellUIdT)) {
        ADVFS_SAD0("create_rtdn_opx: bad root done record size");
    }

    if ( !BS_UID_EQL(rtdnR.ut.bfsid.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(rtdnR.ut.bfsid.domainId, dmnP->dualMountId)) ) {
           /* I can't hit this at all. */
           /* I'm not sure it's needed, but I'll leave it in. */
            rtdnR.ut.bfsid.domainId = dmnP->domainId;
        } else {
            ADVFS_SAD0("create_rtdn_opx: domainId mismatch");
        }
    }

    if (dmnP->state != BFD_ACTIVATED) {
        sts = rbf_bfs_open( &bfSetp, rtdnR.ut.bfsid, BFS_OP_IGNORE_DEL, ftxH );
        if ( sts != EOK ) {
          domain_panic(ftxH.dmnP, "create_rtdn_opx: failed to open bfs");
          goto HANDLE_EXCEPTION;
        }
        bfs_opened = 1;
    } else {
        bfSetp = bs_bfs_lookup_desc( rtdnR.ut.bfsid );
    }
    MS_SMP_ASSERT( BFSET_VALID(bfSetp) );

    make_mcell_valid( rtdnR, bfSetp, ftxH );

    if ( bfs_opened ) {
        bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);
    }

HANDLE_EXCEPTION:
}


/*
 * make_mcell_valid
 *
 * This is called from the root done routine for bitfile create.  It
 * sets the on-disk bitfile state to VALID, as well as waking up any
 * waiters on FTX_TRANS.
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

    vdp = VD_HTOP(mcelluid.vdIndex, dmnP);

    bfap = grab_bsacc(bfSetp, mcelluid.ut.tag, TRUE, NULL);
    if (bfap == NULL) {
        ADVFS_SAD0("create_rtdn_opx: grab_bsacc failed");
    }

    if (lk_get_state( bfap->stateLk ) == ACC_VALID) {

        /* How could it have gotten to be VALID??? */

        ADVFS_SAD0("create_rtdn_opx: VALID bfAccess state is invalid!!");
    }

    mutex_unlock( &bfap->bfaLock );

    sts = rbf_pinpg(&pgref, (void*)&bmtp, vdp->bmtp,
                     mcelluid.mcell.page, BS_NIL, ftxH);
    if (sts != EOK) {
        ADVFS_SAD2("create_rtdn_opx: failed to access bmt pg N1, err N2",
                   mcelluid.mcell.page, sts );
    }
    if ( bmtp->pageId != mcelluid.mcell.page ) {
        ADVFS_SAD2("create_rtdn_opx: got bmt page N1 instead of N2",
                   bmtp->pageId, mcelluid.mcell.page );
    }

    odattr = bmtr_find( &bmtp->bsMCA[ mcelluid.mcell.cell ], BSR_ATTR, dmnP);
    if (odattr == 0) {
        ADVFS_SAD0("create_rtdn_opx: can't find ds attributes");
    }

    mutex_lock( &bfap->bfaLock );

    if (odattr->state == BSRA_CREATING) {

        /*
         * It is in the CREATING state, so set it VALID now.  There
         * are a number of ways it can be different states also, in
         * which case the correct action is to do nothing.
         */

        rbf_pin_record(pgref, &odattr->state, sizeof(bfStatesT));
        odattr->state = BSRA_VALID;

        if (lk_get_state( bfap->stateLk ) == ACC_FTX_TRANS) {

            bfap->bfState = BSRA_VALID;
            ftx_set_state(&bfap->stateLk, &bfap->bfaLock, ACC_VALID, ftxH);

        }
    }

    DEC_REFCNT( bfap );
    mutex_unlock( &bfap->bfaLock );
}


opxT create_undo_opx;

void
create_undo_opx(ftxHT ftxH,     /* in - ftx handle */
                int undoRecSz,  /* in - undo record size */
                void* opRec     /* in */
                )
{
    mcellUIdT* undoRp = (mcellUIdT*)opRec;
    statusT sts;
    bfSetT *bfSetp;
    domainT *dmnP = ftxH.dmnP;

    if (undoRecSz != sizeof(mcellUIdT)) {
        ADVFS_SAD0("create_undo_opx: bad root done record size");
    }

    if ( !BS_UID_EQL(undoRp->ut.bfsid.domainId, dmnP->domainId) ) {
        if ( (dmnP->dmnFlag & BFD_DUAL_MOUNT_IN_PROGRESS) &&
             (BS_UID_EQL(undoRp->ut.bfsid.domainId, dmnP->dualMountId)) ) {
            undoRp->ut.bfsid.domainId = dmnP->domainId;
        }
        else {
            ADVFS_SAD0("create_undo_opx: domainId mismatch");
        }
    }

    sts = rbf_bfs_open( &bfSetp, undoRp->ut.bfsid, BFS_OP_IGNORE_DEL, ftxH );
    if ( sts != EOK ) {
      domain_panic(dmnP, "create_undo_opx: failed to open bfs");
      goto HANDLE_EXCEPTION;
    }

    /*
     * In order to prevent a hierarchy violation we grab the
     * BfSetTblLock prior to calling tagdir_remove_tag().
     * Later, when we call bs_bfs_close() we will already
     * have the lock.
     * Ensure we donot already hold the lock before grabbing it.
     */
    if (!lock_holder(&dmnP->BfSetTblLock.lock)) {
       BFSETTBL_LOCK_WRITE( bfSetp->dmnP )
    }

    tagdir_tag_to_freelist(bfSetp, &undoRp->ut.tag, ftxH, TRUE);

    kill_mcell( bfSetp, undoRp, ftxH );

    bs_bfs_close(bfSetp, ftxH, BFS_OP_DEF);

HANDLE_EXCEPTION:
}


/*
 * init_bscreate_opx
 *
 * register opx routines for create.
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
 * initialize a new metadata cell.  Returns with mcell list locked
 * by parent ftx.
 *
 * Returns ENO_MORE_BLKS, EBAD_PARAMS, errors from vd select or EOK
 */

statusT
new_mcell(
          ftxHT parFtx,         /* in - parent ftx */
          mcellUIdT* mcellUIdp, /* in/out - ptr to global mcell id */
          bsBfAttrT* bfAttr,    /* in - ds attributes */
          domainT* dmnP,        /* in - domain ptr */
          bsInMemXtntT *oxtntp   /* in - ptr to orig extent map */
          )
{
    statusT sts;
    bsBfAttrT* odattrp;
    struct bsXtntR* xtntp;
    struct vd* vdp;
    struct bsMC* mcp;
    rbfPgRefHT pgref;
    vdIndexT vdIndex;

    /* select a vd for the new bitfile */

    while (TRUE) {

        sts = sc_select_vd_for_mcell( &vdp,
                                  parFtx.dmnP,
                                  dmnP->scTbl, 
                                  bfAttr->cl.reqServices, 
                                  bfAttr->cl.optServices );

        if (sts != EOK) {
            return ENO_MORE_BLKS;
        }

        mcellUIdp->vdIndex = vdp->vdIndex;
    
        if ( bfAttr->bfPgSz % vdp->stgCluster != 0 ) {
            vd_dec_refcnt(vdp);
            return EBAD_PARAMS;
        }

        MCELL_LOCK_WRITE( &vdp->mcell_lk)
        if ((sts = bmt_alloc_prim_mcell( 
                                        parFtx, 
                                        mcellUIdp, 
                                        vdp, 
                                        &pgref, 
                                        &mcp)) != ENO_MORE_MCELLS) {
                vd_dec_refcnt(vdp);
                FTX_ADD_LOCK(&(vdp->mcell_lk), parFtx)
                break;
        }
        /*
         * Got ENO_MORE_MCELLS; unlock mcell_lk and try next volume
         */
        MCELL_UNLOCK( &vdp->mcell_lk)
        vd_dec_refcnt(vdp);
    }

    if (sts != EOK) {
        return ENO_MORE_BLKS;
    }

    /* pin mcell record */

    rbf_pin_record(pgref, mcp->bsMR0, sizeof(mcp->bsMR0));

    odattrp = bmtr_assign(BSR_ATTR, sizeof(bsBfAttrT), mcp);
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

    if (oxtntp && oxtntp->type == BSXMT_STRIPE) {
        MS_SMP_ASSERT(oxtntp->stripeXtntMap);
        xtntp->type = BSXMT_STRIPE;
        xtntp->segmentSize = oxtntp->stripeXtntMap->segmentSize;
    } else {
        xtntp->type = BSXMT_APPEND;
    }

    xtntp->chainVdIndex = bsNilVdIndex;
    xtntp->chainMCId = bsNilMCId;
    xtntp->rsvd1 = bsNilVdIndex;
    xtntp->rsvd2 = bsNilMCId;
    xtntp->blksPerPage = odattrp->bfPgSz;
    bzero((char *)&xtntp->delRst , sizeof(delRstT));

    /* 
     * If the first extent is being stored in the primary
     * mcell, initialize the first extent fields.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(dmnP->dmnVersion, xtntp->type)) {
        xtntp->firstXtnt.mcellCnt = 1;
        xtntp->firstXtnt.xCnt = 1;
        xtntp->firstXtnt.bsXA[0].bsPage = 0;
        xtntp->firstXtnt.bsXA[0].vdBlk = -1;
    }
    else {
       /*
        * These fields should never be used.  Set them to
        * impossible values so they are easily recognizable
        * and so incorrect code paths will quickly trip on 
        * them.
        */
        xtntp->firstXtnt.mcellCnt = -1;
        xtntp->firstXtnt.xCnt = -1;
        xtntp->firstXtnt.bsXA[0].bsPage = -1;
        xtntp->firstXtnt.bsXA[0].vdBlk = -1;
    }

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

    vdp = VD_HTOP(mcelluidp->vdIndex, dmnP);

    sts = rbf_pinpg( &pgref, (void*)&bmtpgp, vdp->bmtp,
                    mcelluidp->mcell.page, BS_NIL, ftxH );
    if (sts != EOK) {
        domain_panic(dmnP,
                     "kill_mcell: failed to access bmt pg %d, err %d", 
                     mcelluidp->mcell.page, sts );
        return;
    }
    if ( bmtpgp->pageId != mcelluidp->mcell.page ) {
        domain_panic(dmnP, "kill_mcell: got bmt page %d instead of %d",
                     bmtpgp->pageId, mcelluidp->mcell.page );
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

    bfap = grab_bsacc(bfSetp, mcp->tag, TRUE, NULL);
    if (bfap == NULL) {
        domain_panic(dmnP, "kill_mcell: grab_bsacc failed");
        return;
    }

    if (lk_get_state( bfap->stateLk ) == ACC_VALID) {

        /* How could it have gotten to be VALID??? */

        DEC_REFCNT( bfap );
        mutex_unlock( &bfap->bfaLock );
        domain_panic(dmnP, "kill_mcell: VALID bfAccess state is invalid!!");
        return;
    }

    if (lk_get_state( bfap->stateLk ) == ACC_FTX_TRANS) {
        /*
         * We need to invalidate all dirty pages for this bitfile (this
         * prevents them from being written out later to a different
         * bitfile that allocates the same blocks.
         * Skip the invalidation for files that have no UBC object.
         * Shadow bfaps of metadata files would have no object for example.
         */
        if (bfap->bfObj) {
            mutex_unlock( &bfap->bfaLock );
            bs_invalidate_pages(bfap, 0, 0, 0);
            mutex_lock( &bfap->bfaLock );
        }
    }

    bfap->bfState = BSRA_INVALID;

    lk_signal( lk_set_state( &bfap->stateLk, ACC_INVALID ), &bfap->stateLk);

    DEC_REFCNT( bfap );
    mutex_unlock( &bfap->bfaLock );
    
    sts = dealloc_mcells (dmnP, vdp->vdIndex, mcelluidp->mcell, ftxH);
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
                uint16T firstVdIndex,  /* in */
                bfMCIdT firstMcellId,  /* in */
                ftxHT parentFtxH  /* in */
                )
{
    bsMPgT* bmtp = NULL;
    bfMCIdT mcellId;
    bsMCT* mcp = NULL;
    bfMCIdT nextMcellId;
    vdIndexT nextVdIndex;
    rbfPgRefHT pgPin;
    statusT sts;
    vdIndexT vdIndex;
    vdT *vdp;

    vdIndex = firstVdIndex;
    mcellId = firstMcellId;

    /*
     ** Deallocate each mcell.
     */

    while ( vdIndex != bsNilVdIndex) {
        vdp = VD_HTOP(vdIndex, dmnP);

        /*
         * Acquire the vd's free mcell list lock just long enough to insert
         * the mcell onto the free list.  Because our caller is an undo routine
         * and the effects of undo routines cannot be undone, we don't have to
         * worry about some other thread allocating the mcell.
         */
        MCELL_LOCK_WRITE( &vdp->mcell_lk)

        sts = rbf_pinpg( &pgPin, (void*)&bmtp, vdp->bmtp,
                        mcellId.page, BS_NIL, parentFtxH );
        if (sts != EOK) {
            MCELL_UNLOCK( &vdp->mcell_lk)
            return sts;
        }
        mcp = &bmtp->bsMCA[ mcellId.cell ];
        nextVdIndex = mcp->nextVdIndex;
        nextMcellId = mcp->nextMCId;
            
        bmt_free_mcell (parentFtxH, vdp, mcp, mcellId, bmtp, pgPin);

        MCELL_UNLOCK( &vdp->mcell_lk)

        /*
         ** Go to next mcell.
         */

        vdIndex = nextVdIndex;
        mcellId = nextMcellId;

    }  /* end while */

    return EOK;

}  /* end dealloc_mcells */
