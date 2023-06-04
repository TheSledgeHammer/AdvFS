/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
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
 *  AdvFS
 *
 * Abstract:
 *
 *  bs_stripe.c
 *  This module contains routines related to stripe.
 *
 * Date:
 *
 *  Thu Jun 25  15:15:56  1992
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_stripe.c,v $ $Revision: 1.1.91.5 $ (DEC) $Date: 2008/02/12 13:07:04 $"

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_migrate.h>
#include <msfs/bs_ods.h>
#include <msfs/bs_service_classes.h>
#include <msfs/bs_stripe.h>
#include <msfs/ms_assert.h>

#define ADVFS_MODULE BS_STRIPE

/*
 * private protos
 */

static
statusT
stripe_zero_size (
                  bfAccessT *bfAccess,  /* in */
                  uint32T segmentCnt,  /* in */
                  uint32T segmentSize,  /* in */
                  ftxHT parentFtxH,  /* in */
                  bsStripeHdrT **newStripeHdr  /* out */
                  );

static
statusT
stripe_zero_size_clone (
                        bfAccessT *bfap,             /* in - clone access */
                        bsInMemXtntT *oXtntp,        /* in - original xtnt */
                        ftxHT parentFtxH,            /* in */
                        bsStripeHdrT **newStripeHdr  /* out */
                        );

static
statusT
stripe_nonzero_size ();

opxT undo_xtnt_rec;

void
undo_xtnt_rec (
               ftxHT ftxH,
               int size,
               void *address
               );

static
statusT
update_xtnt_rec_fields (
                 bfAccessT *bfAccess,  /* in */
                 uint32T segmentSize,  /* in */
                 ftxHT parentFtxH  /* in */
                 );

static
statusT
create_bf_rel_xtnt_descs (
                          uint32T mapIndex,  /* in */
                          uint32T segmentCnt,  /* in */
                          uint32T segmentSize,  /* in */
                          bsXtntDescT *xmXtntDesc,  /* in */
                          bsXtntMapTypeT bsXtntMapType,  /* in */
                          bsInMemXtntMapT *bfXtntMap  /* in, modified */
                          );

/*
 * init_stripe_agent
 *
 * This function registers the stripe code as a transaction agent.
 */

void 
str_register_stripe_agent(void)
{
    statusT sts;

    sts = ftx_register_agent(
                             FTA_BS_STR_STRIPE_V1,
                             NULL,
                             NULL
                             );
    if (sts != EOK) {
        ADVFS_SAD1("str_register_stripe_agent: register failure ",
                   (long) BSERRMSG(sts));
    }

    sts = ftx_register_agent(
                             FTA_BS_STR_UPD_XTNT_REC_V1,
                             &undo_xtnt_rec,
                             NULL
                             );
    if (sts != EOK) {
        ADVFS_SAD1("str_register_stripe_agent: register failure ",
                   (long) BSERRMSG(sts));
    }

#ifdef MOVEMETADATA_LK
    sts = ftx_register_agent(
                             FTA_BS_STR_UPD_XTNT_REC_LOCK_V1,
                             &mig_move_metadata_undo,
                             &mig_move_metadata_root_done
                             );

    if (sts != EOK) {
        ADVFS_SAD1("str_register_stripe_agent: register failure ",BSERRMSG(sts));
    }
#endif  /* MOVEMETADATA_LK */

    return;

} /* end str_register_stripe_agent */


/*
 * bs_stripe
 *
 * This function converts the zero-size bitfile into a striped bitfile.
 */

statusT
bs_stripe (
           struct bfAccess *bfap,     /* in */
           uint32T segmentCnt,        /* in */
           uint32T segmentSize,       /* in */
           long xid                   /* in - CFS transaction id */
           )
{
    bfSetT *bfSet;
    statusT sts;


    if (BS_BFTAG_RSVD (bfap->tag)) {
        return ENOT_SUPPORTED;
    }

    bfSet = bfap->bfSetp;
    if (bfSet->cloneId != BS_BFSET_ORIG) {
        return ENOT_SUPPORTED;
    }

    /* This test only tells us if the file is already striped.
     * It is necessary to again check xtnts.type once we 
     * have the mcellList_lk.
     */

    if (bfap->xtnts.type != BSXMT_APPEND) {
        /*
         * FIX - this should work someday for striped bitfiles.
         * What about shadowed bitfile??
         */
        if (bfap->xtnts.type == BSXMT_STRIPE) {
            sts = E_ALREADY_STRIPED;
        } else {
            sts = ENOT_SUPPORTED;
        }
        return sts;
    }

    if ((segmentCnt <= 1) || (segmentSize == 0)) {
        return EBAD_PARAMS;
    }

    sts = str_stripe (bfap, segmentCnt, segmentSize, xid);

    return sts;

}  /* end bs_stripe */


/*
 * str_stripe
 *
 * This function converts a normal bitfile to a striped bitfile.  The
 * number of disks is specified by segmentCnt.  Each segment of each
 * stripe is allocated from one of these disks.  The same relative
 * segment in each stripe is allocated from the same disk during
 * storage allocation.
 *
 * This function assumes that the bitfile's in-memory extent map exists.
 */

statusT
str_stripe (
            bfAccessT *bfap,     /* in */
            uint32T segmentCnt,  /* in */
            uint32T segmentSize, /* in */
            long xid             /* in - CFS transaction id */
            )
{
    ftxHT ftxH;
    int ftxFailFlag = 0;
    int i;
    bsStripeHdrT *stripeHdr = NULL;
    statusT sts;
    struct vnode *nullvp = NULL;
    bfAccessT *cloneap = NULL;

    sts = FTX_START_XID(FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 0, xid);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }
    ftxFailFlag = 1;

    if ( bfap->bfSetp->cloneSetp != NULL ) {

        /* must have ftxH to test BFS_DELETING & tagdir_lookup */
        if ( (!bs_have_clone( bfap->tag, bfap->bfSetp->cloneSetp, ftxH)) ||
             (bfap->bfSetp->cloneSetp->state == BFS_DELETING)) {
            /* This file has no clone. */
            /* The clone set was created before this file was. */
            goto noclone;
        }

        if ( bfap->bfSetp->cloneSetp->bfSetFlags & BFS_OD_OUT_OF_SYNC ) {
            /* clone bfset exists but is out of sync with the orig */
            bfap->noClone = TRUE;
            goto noclone;
        }

        sts = bs_access_one( &cloneap,
                             bfap->tag,
                             bfap->bfSetp->cloneSetp,
                             ftxH,
                             0,
                             NULL,
                             &nullvp,
                             NULL);
        if ( sts != EOK ) {
            bfap->noClone = TRUE;
            goto noclone;
        }

        bfap->nextCloneAccp = cloneap;
        bfap->noClone = FALSE;
        cloneap->origAccp = bfap;
    }
noclone:

    /*
     * Make sure that the in-memory extent maps are valid.
     * Returns with mcellList_lk write_locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_UPDATE);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    /* Put this lock on list to be released when transaction completes. */
    FTX_ADD_LOCK(&bfap->mcellList_lk, ftxH)

    /* 
     * Recheck under the protection of the mcellList_lk that
     * the file has not already been stripped
     * NOTE mcellList_lk gives us read access to the xtnt map.
     * we'll let the ftx_fail release the lock.
     */

    if (bfap->xtnts.type == BSXMT_STRIPE) {
        RAISE_EXCEPTION(E_ALREADY_STRIPED);
    }

    /*
     * Need to check both file_size and nextPage.  It is necessary
     * to check file_size so that we don't stripe a file with only
     * a frag.  It is necessary to check nextPage since fs_write()
     * doesn't update file_size until after it has added storage.
     */
    if ( bfap->file_size == 0 && bfap->nextPage == 0 ) {

        sts = stripe_zero_size( bfap,
                                segmentCnt,
                                segmentSize,
                                ftxH,
                                &stripeHdr);
    } else {
        sts = stripe_nonzero_size();
    }

    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    for ( i = stripeHdr->cnt - 1; i >= 0; i-- ) {
        sts = x_create_shadow_rec( bfap,
                                   stripeHdr->xtntMap[i],
                                   bsNilVdIndex,
                                   bsNilMCId,
                                   ftxH,
                                   TRUE);
        if ( sts == EOK ) {
            stripeHdr->xtntMap[i]->updateStart = stripeHdr->xtntMap[i]->cnt;
        } else {
            RAISE_EXCEPTION(sts);
        }
    }

    sts = update_xtnt_rec_fields( bfap, segmentSize, ftxH);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);
    }

    /*
     * Update the in-memory extent map.
     */
    FTX_LOCKWRITE(&(bfap->xtntMap_lk), ftxH)
    bfap->xtnts.type = BSXMT_STRIPE;
    imm_delete_xtnt_map(bfap->xtnts.xtntMap);
    bfap->xtnts.xtntMap = NULL;
    bfap->xtnts.stripeXtntMap = stripeHdr;

    /*
     * If a clone file exists we have to stripe it now.
     * If this original file never had pages then the clone will be emtpy also.
     * If this file had pages, then was cloned, then was truncated to zero
     * length, then the clone is fully populated.
     */

    if ( cloneap ) {
        COW_LOCK_WRITE( &bfap->cow_lk )

        MS_SMP_ASSERT(cloneap->xtnts.type == BSXMT_APPEND)
        if ( cloneap->xtnts.xtntMap->cnt == 1 &&
             cloneap->xtnts.xtntMap->subXtntMap[0].pageCnt == 0 ) {
            sts = str_stripe_clone( cloneap, &bfap->xtnts, ftxH);
            MS_SMP_ASSERT(sts == EOK ||
                          sts == ENO_MORE_BLKS);
        }

        bs_close_one( cloneap, 0, ftxH);
        COW_UNLOCK( &bfap->cow_lk)
    }

    ftx_done_u( ftxH, FTA_BS_STR_STRIPE_V1, 0, NULL);

    return sts;

HANDLE_EXCEPTION:

    if ( cloneap ) {
        bs_close_one( cloneap, 0, ftxH);
    }
    if ( ftxFailFlag != 0 ) {
        ftx_fail(ftxH);
    }
    if ( stripeHdr != NULL ) {
        str_delete_stripe_hdr(stripeHdr);
    }

    return sts;

}  /* end str_stripe */


statusT
str_stripe_clone (
                  bfAccessT *bfap,      /* in - clone access */
                  bsInMemXtntT *oXtntp, /* in - original xtnt map */
                  ftxHT pftxH           /* in - parent ftxH */
                  )
{
    ftxHT ftxH;
    int ftxFailFlag = 0;
    int i;
    bsStripeHdrT *stripeHdr = NULL;
    statusT sts;

    sts = FTX_START_N(FTA_NULL, &ftxH, pftxH, bfap->dmnP, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    ftxFailFlag = 1;

    if (bfap->nextPage == 0) {
        sts = stripe_zero_size_clone ( bfap,
                                       oXtntp,
                                       ftxH,
                                       &stripeHdr);
    } else {
        sts = stripe_nonzero_size ();
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    for (i = stripeHdr->cnt - 1; i >= 0; i--) {
        sts = x_create_shadow_rec ( bfap,
                                    stripeHdr->xtntMap[i],
                                    bsNilVdIndex,
                                    bsNilMCId,
                                    ftxH,
                                    TRUE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        stripeHdr->xtntMap[i]->updateStart = stripeHdr->xtntMap[i]->cnt;
    }  /* end for */

    sts = update_xtnt_rec_fields ( bfap,
                                   oXtntp->stripeXtntMap->segmentSize,
                                   ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Update the in-memory extent map.
     */
    FTX_LOCKWRITE(&(bfap->xtntMap_lk), ftxH)
    bfap->xtnts.type = BSXMT_STRIPE;
    imm_delete_xtnt_map (bfap->xtnts.xtntMap);
    bfap->xtnts.xtntMap = NULL;
    bfap->xtnts.stripeXtntMap = stripeHdr;

    ftx_done_u (ftxH, FTA_BS_STR_STRIPE_V1, 0, NULL);

    return sts;

HANDLE_EXCEPTION:

    if (ftxFailFlag != 0) {
        ftx_fail (ftxH);
    }
    if (stripeHdr != NULL) {
        str_delete_stripe_hdr (stripeHdr);
    }

    return sts;
}


/*
 * stripe_zero_size
 *
 * This function creates the stripe header for a zero size bitfile.
 *
 * This function assumes that the caller owns the mcell list lock.
 */

static
statusT
stripe_zero_size (
                  bfAccessT *bfap,             /* in */
                  uint32T segmentCnt,          /* in */
                  uint32T segmentSize,         /* in */
                  ftxHT parentFtxH,            /* in */
                  bsStripeHdrT **newStripeHdr  /* out */
                  )
{
    bfSetT *bfSetDesc;
    vdIndexT *disk = NULL;  /* array of vdIndexs */
    int i;
    bfMCIdT mcellId;
    mcellPoolT mcellType;
    bsStripeHdrT *stripeHdr = NULL;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    int allDisksRefed = FALSE;
    vdT *vdp;
    NEW_VD_SKIP(vd_skip_list);

    disk = (vdIndexT *) ms_malloc (segmentCnt * sizeof (vdIndexT));
    if (disk == NULL) {
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }

    for (i = 0; i < segmentCnt; i++) {
        /*
         * By setting the "desiredBlks" to some future size,
         * sc_select_vd_for_stg() either returns the first disk with that
         * amount or, if no disks have that amount, returns the disk with
         * the most free space.  At the completion of the loop, the disk
         * list will contain disks having at least the amount of requested
         * space and/or the disks having the most free space below the
         * amount of requested space.  Of course, this is no guarantee that the
         * space will be available when storage is added.
         */
        sts = sc_select_vd_for_stg( &vdp,
                                    bfap->dmnP,
                                    bfap->reqServices, 
                                    bfap->optServices,
                                    0, /* no preferred disk */
                                    vd_skip_list,
                                    bfap->bfPageSz * 1024, /* desiredBlks */
                                    bfap->bfPageSz,  /* minDesiredBlks */
                                    FALSE);  /* don't count allocation */
        if (sts != EOK) {
            if (sts == ENO_MORE_BLKS) {
                /*
                 * Assume no more eligible disks.
                 */
                sts = E_NOT_ENOUGH_DISKS;
            }
            RAISE_EXCEPTION (sts);
        }
        disk[i] = vdp->vdIndex;

        /* add to skip list so we allocate next segment from another disk */
        SET_VD_SKIP(vd_skip_list, disk[i]);
    }  /* end for */
    allDisksRefed = TRUE;

    sts = str_create_stripe_hdr (segmentCnt, segmentSize, &stripeHdr);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if (BS_BFTAG_REG(bfap->tag)) {
        mcellType = BMT_NORMAL_MCELL;
    } else {
        mcellType = RBMT_MCELL;
    }
    bfSetDesc = bfap->bfSetp;

    for (i = 0; i < segmentCnt; i++) {
        sts = imm_create_xtnt_map( bfap->bfPageSz,
                                   bfap->dmnP,
                                   0,  /* maxCnt - force default */
                                   0,  /* termPage - not used */
                                   0,  /* termVdIndex - not used */
                                   &(stripeHdr->xtntMap[i]));
        if (sts == EOK) {

            stripeHdr->xtntMap[i]->allocVdIndex = disk[i];

            sts = bmt_alloc_mcell( bfap,
                                   disk[i],
                                   mcellType,
                                   bfSetDesc->dirTag,
                                   bfap->tag,
                                   0,  /* linkSeg */
                                   parentFtxH,
                                   &mcellId,
                                   FALSE);
            if (sts == EOK) {
                subXtntMap = &(stripeHdr->xtntMap[i]->subXtntMap[0]);
                sts = imm_init_sub_xtnt_map( subXtntMap,
                                             0,  /* pageOffset */
                                             0,  /* pageCnt */
                                             disk[i],
                                             mcellId,
                                             BSR_SHADOW_XTNTS,
                                             BMT_SHADOW_XTNTS,
                                             0);  /* maxCnt - force default */
                if (sts == EOK) {
                    subXtntMap->mcellState = NEW_MCELL;
                    subXtntMap->updateStart = 0;
                    subXtntMap->updateEnd = subXtntMap->cnt - 1;
                } else {
                    RAISE_EXCEPTION (sts);
                }
            } else {
                RAISE_EXCEPTION (sts);
            }
        } else {
            RAISE_EXCEPTION (sts);
        }
        stripeHdr->xtntMap[i]->cnt = 1;
        stripeHdr->xtntMap[i]->validCnt = stripeHdr->xtntMap[i]->cnt;
        stripeHdr->xtntMap[i]->updateStart = 0;
        stripeHdr->xtntMap[i]->updateEnd = stripeHdr->xtntMap[i]->cnt - 1;

        stripeHdr->xtntMap[i]->hdrType = subXtntMap->type;
        stripeHdr->xtntMap[i]->hdrVdIndex = subXtntMap->vdIndex;
        stripeHdr->xtntMap[i]->hdrMcellId = subXtntMap->mcellId;
    }  /* end for */

    for (i = 0; i < segmentCnt; i++) {
        vd_dec_refcnt(VD_HTOP(disk[i], bfap->dmnP));
    }

    ms_free (disk);

    *newStripeHdr = stripeHdr;

    return sts;

HANDLE_EXCEPTION:

    if ( allDisksRefed == TRUE ) { 
        i = segmentCnt;
    }
    while ( --i >= 0 ) {
        vd_dec_refcnt(VD_HTOP(disk[i], bfap->dmnP));
    }

    /*
     * If any mcells were allocated, they are set free when the caller
     * calls ftx_fail().
     */

    if (stripeHdr != NULL) {
        str_delete_stripe_hdr (stripeHdr);
    }
    if (disk != NULL) {
        ms_free (disk);
    }

    return sts;

}  /* end stripe_zero_size */


/* Produce a stripe header for the clone using the same striping */
/* information (and disks) as the original file */

static
statusT
stripe_zero_size_clone (
                        bfAccessT *bfap,             /* in - clone access */
                        bsInMemXtntT *oXtntp,        /* in - original xtnt */
                        ftxHT pFtxH,                 /* in */
                        bsStripeHdrT **newStripeHdr  /* out */
                        )
{
    bfSetT *bfSetDesc;
    vdIndexT *disk = NULL;  /* array of vdIndexs */
    int i;
    bfMCIdT mcellId;
    bsStripeHdrT *stripeHdr = NULL;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T segmentCnt;
    int allDisksRefed = FALSE;
    vdT *vdp;

    MS_SMP_ASSERT(oXtntp->validFlag);
    MS_SMP_ASSERT(oXtntp->type == BSXMT_STRIPE);
    MS_SMP_ASSERT(oXtntp->stripeXtntMap);
    segmentCnt = oXtntp->stripeXtntMap->cnt;

    disk = (vdIndexT *) ms_malloc (segmentCnt * sizeof(vdIndexT));
    if (disk == NULL) {
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }

    for (i = 0; i < segmentCnt; i++) {
        MS_SMP_ASSERT(oXtntp->stripeXtntMap->xtntMap[i]);
        disk[i] = oXtntp->stripeXtntMap->xtntMap[i]->allocVdIndex;
        vdp = vd_htop_if_valid(disk[i], bfap->dmnP, TRUE, FALSE);
        if ( vdp == NULL ) {
            RAISE_EXCEPTION( EBAD_VDI );
        }
    }
    allDisksRefed = TRUE;

    sts = str_create_stripe_hdr ( segmentCnt,
                                  oXtntp->stripeXtntMap->segmentSize,
                                  &stripeHdr);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    bfSetDesc = bfap->bfSetp;

    /* can't stripe reserved files, pass BMT_NORMAL_MCELL to bmt_alloc_mcell */
    MS_SMP_ASSERT (BS_BFTAG_REG(bfap->tag));
    for (i = 0; i < segmentCnt; i++) {
        sts = imm_create_xtnt_map ( bfap->bfPageSz,
                                    bfap->dmnP,
                                    0,  /* maxCnt - force default */
                                    0,  /* termPage - not used */
                                    0,  /* termVdIndex - not used */
                                    &stripeHdr->xtntMap[i]);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        stripeHdr->xtntMap[i]->allocVdIndex = disk[i];

        sts = bmt_alloc_mcell ( bfap,
                                disk[i],
                                BMT_NORMAL_MCELL,
                                bfSetDesc->dirTag,
                                bfap->tag,
                                0,  /* linkSeg */
                                pFtxH,
                                &mcellId,
                                TRUE);

        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        subXtntMap = &(stripeHdr->xtntMap[i]->subXtntMap[0]);
        sts = imm_init_sub_xtnt_map ( subXtntMap,
                                      0,  /* pageOffset */
                                      0,  /* pageCnt */
                                      disk[i],
                                      mcellId,
                                      BSR_SHADOW_XTNTS,
                                      BMT_SHADOW_XTNTS,
                                      0);  /* maxCnt - force default */
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        subXtntMap->mcellState = NEW_MCELL;
        subXtntMap->updateStart = 0;
        subXtntMap->updateEnd = subXtntMap->cnt - 1;

        stripeHdr->xtntMap[i]->cnt = 1;
        stripeHdr->xtntMap[i]->validCnt = stripeHdr->xtntMap[i]->cnt;
        stripeHdr->xtntMap[i]->updateStart = 0;
        stripeHdr->xtntMap[i]->updateEnd = stripeHdr->xtntMap[i]->cnt - 1;
        stripeHdr->xtntMap[i]->hdrType = subXtntMap->type;
        stripeHdr->xtntMap[i]->hdrVdIndex = subXtntMap->vdIndex;
        stripeHdr->xtntMap[i]->hdrMcellId = subXtntMap->mcellId;
    }  /* end for */

    for (i = 0; i < segmentCnt; i++) {
        vd_dec_refcnt(VD_HTOP(disk[i], bfap->dmnP));
    }
    ms_free (disk);

    *newStripeHdr = stripeHdr;

    return sts;

HANDLE_EXCEPTION:

    if ( allDisksRefed == TRUE ) { 
        i = segmentCnt;
    }
    while ( --i >= 0 ) {
        vd_dec_refcnt(VD_HTOP(disk[i], bfap->dmnP));
    }
    /*
     * If any mcells were allocated, they are set free when the caller
     * calls ftx_fail().
     */

    if (stripeHdr != NULL) {
        str_delete_stripe_hdr (stripeHdr);
    }
    if (disk != NULL) {
        ms_free (disk);
    }

    return sts;
}


/*
 * stripe_nonzero_size
 *
 * This function creates the stripe header for a non-zero size bitfile.
 */

static statusT 
stripe_nonzero_size(void)
{
    return ENOT_SUPPORTED;  /* FIX - do this someday */

}  /* end stripe_nonzero_size */


typedef struct xtntRecUndoRec
{
    vdIndexT vdIndex;
    bfMCIdT mCId;
    bsXtntMapTypeT type;
    uint32T segmentSize;
} xtntRecUndoRecT;

/*
 * undo_xtnt_rec
 *
 * This function restores the bitfile xtnt record's type and segment size.
 *
 * This function assumes that the caller owns the bitfile's mcell list lock.
 */

void
undo_xtnt_rec (
               ftxHT ftxH,
               int size,
               void *address
               )
{
    struct bsMPg *bmt;
    domainT *domain;
    bsMCT *mcell;
    rbfPgRefHT pgPin;
    statusT sts;
    xtntRecUndoRecT *undoRec;
    bsXtntRT *xtntRec;

    undoRec = (xtntRecUndoRecT *)address;

    domain = ftxH.dmnP;

    sts = rbf_pinpg( &pgPin,
                     (void *)&bmt,
                     VD_HTOP(undoRec->vdIndex, domain)->bmtp,
                     undoRec->mCId.page,
                     BS_NIL,
                     ftxH);
    if (sts != EOK) {
        /* FIX - correct action? */
        ADVFS_SAD1("undo_xtnt_rec --- rbf_pinpg failed --- %s\n",
                   (long) BSERRMSG(sts));
    }

    mcell = &(bmt->bsMCA[undoRec->mCId.cell]);

    /*
     * Find the primary extent record and restore the type and
     * segment size.
     */

    xtntRec = bmtr_find (mcell, BSR_XTNTS, domain);
    if (xtntRec == NULL) {
        ADVFS_SAD0("undo_xtnt_rec --- can't find extent record");
    }

    rbf_pin_record (pgPin, &(xtntRec->type), sizeof (xtntRec->type));
    xtntRec->type = undoRec->type;
    rbf_pin_record( pgPin, &xtntRec->segmentSize, sizeof(xtntRec->segmentSize));
    xtntRec->segmentSize = undoRec->segmentSize;

    return;

}  /* end undo_xtnt_rec */


/*
 * update_xtnt_rec_fields
 *
 * This function updates the on-disk extent record's type and segmentSize
 * fields.
 *
 * This function assumes that the caller owns the bitfile's mcell list lock.
 */

static
statusT
update_xtnt_rec_fields (
                        bfAccessT *bfap,      /* in */
                        uint32T segmentSize,  /* in */
                        ftxHT parentFtxH      /* in */
                        )
{
    bsMPgT *bmt;
    ftxHT ftxH;
    int ftxFailFlag = 0;
    bsMCT *mcell;
    rbfPgRefHT pgPin;
    statusT sts;
    xtntRecUndoRecT undoRec;
    bsXtntRT *xtntRec;

    sts = FTX_START_META(FTA_NULL, &ftxH, parentFtxH, bfap->dmnP, 1);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    ftxFailFlag = 1;

    sts = rbf_pinpg( &pgPin,
                     (void*)&bmt,
                     VD_HTOP(bfap->primVdIndex, bfap->dmnP)->bmtp,
                     bfap->primMCId.page,
                     BS_NIL,
                     ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    mcell = &(bmt->bsMCA[bfap->primMCId.cell]);
    xtntRec = bmtr_find( mcell, BSR_XTNTS, bfap->dmnP);
    if (xtntRec == NULL) {
        ADVFS_SAD0("update_xtnt_rec_fields: can't find extent record");
    }

    undoRec.vdIndex = bfap->primVdIndex;
    undoRec.mCId = bfap->primMCId;
    undoRec.type = xtntRec->type;
    undoRec.segmentSize = xtntRec->segmentSize;

    rbf_pin_record( pgPin,
                    &(xtntRec->type),
                    sizeof (xtntRec->type));
    xtntRec->type = BSXMT_STRIPE;

    rbf_pin_record( pgPin,
                    &(xtntRec->segmentSize),
                    sizeof (xtntRec->segmentSize));
    xtntRec->segmentSize = segmentSize;

    ftx_done_u (ftxH, FTA_BS_STR_UPD_XTNT_REC_V1, sizeof (undoRec), &undoRec);

    return EOK;

HANDLE_EXCEPTION:

    if (ftxFailFlag != 0) {
        ftx_fail (ftxH);
    }

    return sts;

}  /* end update_xtnt_rec_fields */


/*
 * str_create_stripe_hdr
 *
 * This function creates a stripe header including the extent map array
 * and NULLs out the array.
 */

statusT
str_create_stripe_hdr (
                       uint32T segmentCnt,          /* in */
                       uint32T segmentSize,         /* in */
                       bsStripeHdrT **newStripeHdr  /* out */
                       )
{
    uint32T i;
    bsStripeHdrT *stripeHdr;

    stripeHdr = (bsStripeHdrT *) ms_malloc( sizeof (bsStripeHdrT) +
                                      (segmentCnt * sizeof(bsInMemXtntMapT *)));
    if (stripeHdr == NULL) {
        return ENO_MORE_MEMORY;
    }
    stripeHdr->segmentSize = segmentSize;
    stripeHdr->cnt = segmentCnt;
    stripeHdr->xtntMap = (bsInMemXtntMapT **)(stripeHdr + 1);
    for (i = 0; i < segmentCnt; i++) {
        stripeHdr->xtntMap[i] = NULL;
    }

    *newStripeHdr = stripeHdr;

    return EOK;

}  /* end str_create_stripe_hdr */


/*
 * str_delete_stripe_hdr
 *
 * This function deletes a stripe header including the extent maps and the
 * extent map array.
 */

void
str_delete_stripe_hdr (
                       bsStripeHdrT *stripeHdr  /* in */
                       )
{
    uint32T i;

    for (i = 0; i < stripeHdr->cnt; i++) {
        imm_delete_xtnt_map (stripeHdr->xtntMap[i]);
    }  /* end for */

    ms_free (stripeHdr);

    return;

}  /* end str_delete_stripe_hdr */


/*
 * str_calc_page_alloc
 *
 * This function calculates the number of pages that should be allocated
 * to each stripe given the starting page offset and page count.
 */

void
str_calc_page_alloc (
                     uint32T bfPageOffset,    /* in */
                     uint32T bfPageCnt,       /* in */
                     bsStripeHdrT *stripeHdr  /* in */
                     )
{
    uint32T i;
    uint32T pageCnt;
    int mapIndex;
    uint32T totalPageCnt = 0;
    uint32T wholeAllocUnits;
    bsInMemXtntMapT *xtntMap;

    for (i = 0; i < stripeHdr->cnt; i++) {
        stripeHdr->xtntMap[i]->allocDeallocPageCnt = 0;
    }

    /*
     * Partial or whole segment allocation for first stripe segment.
     */

    pageCnt = stripeHdr->segmentSize - (bfPageOffset % stripeHdr->segmentSize);
    if (pageCnt > bfPageCnt) {
        pageCnt = bfPageCnt;
    }
    mapIndex = BFPAGE_TO_MAP( bfPageOffset,
                              stripeHdr->cnt,
                              stripeHdr->segmentSize);
    xtntMap = stripeHdr->xtntMap[mapIndex];
    xtntMap->allocDeallocPageCnt = xtntMap->allocDeallocPageCnt + pageCnt;
    totalPageCnt = totalPageCnt + pageCnt;
    mapIndex = NEXT_MAP (mapIndex, stripeHdr->cnt);

    /*
     * Whole segment allocations for stripe segments.
     */

    wholeAllocUnits = (bfPageCnt - totalPageCnt) / stripeHdr->segmentSize;
    if (wholeAllocUnits > 0) {
        for (i = 0; i < wholeAllocUnits; i++) {
            xtntMap = stripeHdr->xtntMap[mapIndex];
            xtntMap->allocDeallocPageCnt = xtntMap->allocDeallocPageCnt +
              stripeHdr->segmentSize;
            mapIndex = NEXT_MAP (mapIndex, stripeHdr->cnt);
        }  /* end for */
        totalPageCnt = totalPageCnt + (wholeAllocUnits * stripeHdr->segmentSize);
    }

    /*
     * Partial segment allocation for last stripe segment.
     */

    if (totalPageCnt < bfPageCnt) {
        xtntMap = stripeHdr->xtntMap[mapIndex];
        xtntMap->allocDeallocPageCnt = xtntMap->allocDeallocPageCnt +
          (bfPageCnt - totalPageCnt);
    }

    return;

}  /* end str_calc_page_alloc */


/*
 * str_create_bf_rel_xtnt_map
 *
 * This function creates a bitfile-page-relative extent map from an
 * extent-map-page relative extent map.
 */

statusT
str_create_bf_rel_xtnt_map (
                            bsStripeHdrT *stripeHdr,       /* in */
                            uint32T mapIndex,              /* in */
                            bsInMemXtntMapT *xmXtntMap,    /* in */
                            bsXtntMapTypeT bsXtntMapType,  /* in */
                            bsInMemXtntMapT **bfXtntMap    /* out */
                            )
{
    statusT sts;
    bsXtntDescT xmXtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    bsInMemXtntMapT *xtntMap = NULL;

    imm_get_first_xtnt_desc (xmXtntMap, &xtntDescId, &xmXtntDesc);
    if (xmXtntDesc.pageCnt == 0) {
        return ENO_XTNTS;
    }

    /*
     * Create and initialize an empty extent map.
     */
    sts = imm_create_xtnt_map( xmXtntMap->blksPerPage,
                               xmXtntMap->domain,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &xtntMap);
    if (sts == EOK) {
        sts = imm_init_sub_xtnt_map( &(xtntMap->subXtntMap[0]),
                                     0,  /* pageOffset */
                                     0,  /* pageCnt */
                                     xmXtntDesc.volIndex,
                                     bsNilMCId,
                                     BSR_SHADOW_XTNTS,
                                     BMT_SHADOW_XTNTS,
                                     1);  /* maxCnt */
        if (sts == EOK) {
            xtntMap->cnt++;
        } else {
            RAISE_EXCEPTION (sts);
        }
    } else {
        RAISE_EXCEPTION (sts);
    }

    while (xmXtntDesc.pageCnt > 0) {

        sts = create_bf_rel_xtnt_descs( mapIndex,
                                        stripeHdr->cnt,
                                        stripeHdr->segmentSize,
                                        &xmXtntDesc,
                                        bsXtntMapType,
                                        xtntMap);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        imm_get_next_xtnt_desc (xmXtntMap, &xtntDescId, &xmXtntDesc);

    }  /* end while */

    xtntMap->validCnt = xtntMap->cnt;

    *bfXtntMap = xtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if (xtntMap != NULL) {
        imm_delete_xtnt_map (xtntMap);
    }
    return sts;

}  /* end str_create_bf_rel_xtnt_map */


/*
 * create_bf_rel_xtnt_descs
 *
 * This function creates bitfile-page-relative extent descriptors from an
 * extent-map-page relative extent descriptor.  The resulting extent descriptors
 * are created in the specified bitfile-page-relative extent map.
 */

static
statusT
create_bf_rel_xtnt_descs (
                          uint32T mapIndex,              /* in */
                          uint32T segmentCnt,            /* in */
                          uint32T segmentSize,           /* in */
                          bsXtntDescT *xmXtntDesc,       /* in */
                          bsXtntMapTypeT bsXtntMapType,  /* in */
                          bsInMemXtntMapT *bfXtntMap     /* in, modified */
                          )
{
    uint32T bfPageOffset;
    uint32T blkIncrement;
    bsXtntT bsXA[2];
    uint32T pageCnt;
    uint32T startBlkOffset;
    statusT sts;
    uint32T xmEndPageOffset;
    uint32T xmPageCnt;
    uint32T xmPageOffset;

    if (xmXtntDesc->blkOffset == -1) {
        /*
         * Ignore holes.
         */
        return EOK;
    }

    xmPageOffset = xmXtntDesc->pageOffset;
    xmPageCnt = xmXtntDesc->pageCnt;
    xmEndPageOffset = (xmPageOffset + xmPageCnt) - 1;

    startBlkOffset = xmXtntDesc->blkOffset;

    bsXA[1].vdBlk = -1;

    if ((xmEndPageOffset / segmentSize) == (xmPageOffset / segmentSize)) {
        /*
         * Page range starts and ends in the same segment.
         */
        bfPageOffset = XMPAGE_TO_BFPAGE( mapIndex,
                                         xmPageOffset,
                                         segmentCnt,
                                         segmentSize);
        bsXA[0].bsPage = bfPageOffset;
        bsXA[0].vdBlk = startBlkOffset;
        bsXA[1].bsPage = bfPageOffset + xmPageCnt;
        sts = imm_copy_xtnt_descs( xmXtntDesc->volIndex,
                                   &(bsXA[0]),
                                   1,
                                   FALSE,
                                   bfXtntMap,
                                   bsXtntMapType);
        if (sts != EOK) {
            return sts;
        }
    } else {
        /*
         * Page range starts and ends in different segments.
         */

        /*
         * Do partial first segment, if present.
         */
        if ((xmPageOffset % segmentSize) != 0) {

            bfPageOffset = XMPAGE_TO_BFPAGE( mapIndex,
                                             xmPageOffset,
                                             segmentCnt,
                                             segmentSize);
            pageCnt = NEXT_SEGMENT_PAGE (xmPageOffset, segmentSize) - xmPageOffset;
            bsXA[0].bsPage = bfPageOffset;
            bsXA[0].vdBlk = startBlkOffset;
            bsXA[1].bsPage = bfPageOffset + pageCnt;
            sts = imm_copy_xtnt_descs( xmXtntDesc->volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       bfXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                return sts;
            }

            xmPageCnt = xmPageCnt - pageCnt;
            xmPageOffset = xmPageOffset + pageCnt;
            startBlkOffset = startBlkOffset + (pageCnt * bfXtntMap->blksPerPage);
        }

        /*
         * Do full middle segments, if present.
         */

        blkIncrement = segmentSize * bfXtntMap->blksPerPage;

        while (xmPageCnt >= segmentSize) {

            bfPageOffset = XMPAGE_TO_BFPAGE( mapIndex,
                                             xmPageOffset,
                                             segmentCnt,
                                             segmentSize);
            bsXA[0].bsPage = bfPageOffset;
            bsXA[0].vdBlk = startBlkOffset;
            bsXA[1].bsPage = bfPageOffset + segmentSize;
            sts = imm_copy_xtnt_descs( xmXtntDesc->volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       bfXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                return sts;
            }

            xmPageCnt = xmPageCnt - segmentSize;
            xmPageOffset = xmPageOffset + segmentSize;
            startBlkOffset = startBlkOffset + blkIncrement;

        }  /* end while */

        /*
         * Do partial last segment, if present.
         */
        if (xmPageCnt > 0) {
            bfPageOffset = XMPAGE_TO_BFPAGE( mapIndex,
                                             xmPageOffset,
                                             segmentCnt,
                                             segmentSize);
            bsXA[0].bsPage = bfPageOffset;
            bsXA[0].vdBlk = startBlkOffset;
            bsXA[1].bsPage = bfPageOffset + xmPageCnt;
            sts = imm_copy_xtnt_descs( xmXtntDesc->volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       bfXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                return sts;
            }
        }
    }

    return EOK;

}  /* end create_bf_rel_xtnt_descs */
