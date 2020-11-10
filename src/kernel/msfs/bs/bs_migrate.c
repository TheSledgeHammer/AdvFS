/*
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
 *  MFS/bs
 *
 * Abstract:
 *
 *  bs_migrate.c
 *  This module contains routines related to migration.
 *
 *
 * Date:
 *
 *  Thu Jun 28 14:44:45 1991
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_migrate.c,v $ $Revision: 1.1.239.19 $ (DEC) $Date: 2008/02/12 13:06:53 $"

#include <sys/dlm.h>
#include <sys/clu.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_copy.h>
#include <msfs/bs_delete.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_migrate.h>
#include <msfs/bs_stg.h>
#include <msfs/bs_stripe.h>
#include <msfs/ms_osf.h>

#define ADVFS_MODULE BS_MIGRATE

/*
 * Private and protos
 */

static
statusT
migrate_normal_one_disk (
                         bfAccessT *bfap,  /* in */
                         vdIndexT srcVdIndex,  /* in */
                         uint32T bfPageOffset,  /* in */
                         uint32T bfPageCnt,  /* in */
                         vdIndexT dstVdIndex,  /* in */
                         uint64T  dstBlkOffset,  /* in */
                         uint32T  forceFlag, /* in */
                         bsAllocHintT alloc_hint  /* in */
                         );

static
statusT
migrate_normal (
                bfAccessT *bfap,  /* in */
                uint32T srcPageOffset,  /* in */
                uint32T srcPageCnt,  /* in */
                vdIndexT dstVdIndex,  /* in */
                uint64T  dstBlkOffset,  /* in */
                uint32T  forceFlag, /* in */
                bsAllocHintT alloc_hint  /* in */
                );

static
statusT
migrate_stripe (
                bfAccessT *bfap,  /* in */
                vdIndexT srcVdIndex,  /* in */
                uint32T srcPageOffset,  /* in */
                uint32T srcPageCnt,  /* in */
                vdIndexT dstVdIndex,  /* in */
                uint64T  dstBlkOffset,  /* in */
                uint32T  forceFlag, /* in */
                bsAllocHintT alloc_hint  /* in */
                );

static
statusT
get_xm_page_range_info (
                        bsInMemXtntMapT *xtntMap,  /* in */
                        uint32T  xmPageOffset,  /* in */
                        uint32T  xmPageCnt,  /* in */
                        pageRangeT **xmPageRange,  /* out */
                        uint32T *xmPageRangeCnt,  /* out */
                        pageRangeT *xmHoleRange,  /* out */
                        vdIndexT *xmHoleVdIndex  /* out */
                        );

static
statusT
extend_page_range_list (
                        uint32T *maxCnt,  /* in/out */
                        pageRangeT **pageRange  /* in/out */
                        );

static
statusT
switch_stg (
            bfAccessT *bfap,  /* in */
            uint32T stripeIndex,  /* in */
            bsInMemXtntMapT **origXtntMapAddr,  /* in */
            vdIndexT *copyVdIndex,  /* in, modified */
            bfMCIdT *copyMcellId,  /* in, modified */
            bsInMemXtntMapT *copyXtntMap, /* in, modified */
            ftxHT parentFtxH /* in */
            );

static
statusT
move_metadata (
               bfAccessT *bfap,  /* in */
               bfAccessT *cloneBfap,  /* in */
               vdIndexT newVdIndex,  /* in */
               ftxHT parentFtxH  /* in */
               );

static
statusT
reset_block_map (
                 bfAccessT *bfap,  /* in */
                 uint32T bfPageOffset,  /* in */
                 uint32T bfPageCnt  /* in */
                 );

static
statusT
reset_block_map_special (
                         bfAccessT *cloneap, /* in - special case remapping */
                         uint32T bfPageCnt   /* in */
                        );

static
statusT
alloc_copy_stg (
                bfAccessT *bfap,  /* in */
                uint32T startPage,  /* in */
                pageRangeT *xmPageRange,  /* in */
                uint32T xmPageRangeCnt,  /* in */
                vdIndexT vdIndex,  /* in */
                uint64T  dstBlkOffset,  /* in */
                bsAllocHintT alloc_hint, /* in */
                vdIndexT *copyVdIndex,  /* out */
                bfMCIdT *copyMcellId,  /* out */
                bsInMemXtntMapT **copyXtntMap,  /* out */
                uint32T *copyXferSize     /* out  migrate transfer size */
                );

static
statusT
alloc_hole_stg (
                bfAccessT *bfap,  /* in */
                pageRangeT *xmHoleRange,  /* in */
                vdIndexT vdIndex,  /* in */
                vdIndexT *copyVdIndex,  /* out */
                bfMCIdT *copyMcellId,  /* out */
                bsInMemXtntMapT **copyXtntMap /* out */
                );

static
void
reload_pXtntp (
             bsInMemXtntMapT *xmP,
             ssPackLLT *pXtntp
             );

/*
 * mig_register_migrate_agent
 *
 * This function registers the migrate code as a transaction agent.
 */

void
mig_register_migrate_agent ()
{
    statusT sts;

    sts = ftx_register_agent(
                             FTA_BS_MIG_MIGRATE_V1,
                             NULL,  /* undo */
                             NULL  /* root done */
                             );
    if (sts != EOK) {
        ADVFS_SAD1 ("mig_register_migrate_agent 1: ftx_register_agent() failed -",
                    sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_MIG_MOVE_METADATA_EXC_V1,
                             NULL,  /* undo */
                             NULL  /* root done */
                             );
    if (sts != EOK) {
        ADVFS_SAD1 ("mig_register_migrate_agent 2: ftx_register_agent() failed -",
                    sts);
    }

    sts = ftx_register_agent(
                             FTA_BS_MIG_MOVE_METADATA_V1,
                             NULL,  /* undo */
                             NULL  /* root done */
                             );
    if (sts != EOK) {
        ADVFS_SAD1 ("mig_register_migrate_agent 3: ftx_register_agent() failed -",
                    sts);
    }

    return;

}  /* end mig_register_migrate_agent */


/*
 * bs_migrate
 *
 * This function moves the specified pages to the specified virtual disk.
 * The move may de-fragment the pages when they are allocated on the
 * target disk.  This depends on how many pages are moved and how fragmented
 * the target disk is.
 *
 * If a block offset is specified, the block range must be a continuous range
 * of blocks.  If it wraps from the domain's last block to the first block,
 * this function returns an error.
 *
 * This function calls functions that call bs_pinpg_int().  This is necessary
 * so that a cow operation is not performed on the page range when the file has
 * a clone.
 *
 * FIX - Only one active migrate or shadow operation is allowed per bitfile.
 * FIX - What about shadowed files?
 */

statusT
bs_migrate (
            bfAccessT *bfap,  /* in */
            vdIndexT srcVdIndex,  /* in */
            uint32T srcPageOffset,  /* in */
            uint32T srcPageCnt,  /* in */
            vdIndexT dstVdIndex,  /* in */
            uint64T dstBlkOffset,  /* in */
            uint32T forceFlag, /* in */
            bsAllocHintT alloc_hint  /* in */
            )
{
    ftxHT ftxH;
    statusT sts;
    unLkActionT unlockAction;
    vdT *svdp, *dvdp;
    int srcVdBumped = FALSE;
    int dstVdBumped = 0;

    /*
     * FIX - what happens when a bitfile has a clone, we are migrating the
     * original bitfile, and another thread pins a page that migrate is going
     * to or about to work on?  (see bs_cow_pg() and bs_pinpg())
     */

    /* FIX - before we can migrate the root tag directory, must make a change
     * in domain activation so that the root tag directory is opened between
     * pass1 and pass2.  Recovery should open and close it.  Domain activation
     * should open it after recovery.
     */

    /* FIX - if this is the log, return not supported */

    if (BS_BFTAG_RSVD (bfap->tag)) {
        return ENOT_SUPPORTED;
    }

    if ((bfap->xtnts.type != BSXMT_APPEND) &&
        (bfap->xtnts.type != BSXMT_STRIPE)) {
        return ENOT_SUPPORTED;
    }

    /*
     * FIX - If the srcPageCnt is equal to Zero then the file has
     * been truncated to 0 pages.  This is very common when dealing
     * with Mail files.  Instead of returning EBAD_PARAMS, return
     * success as a Zero Page file doesn't need to be migrated as it
     * doesn't have any extents.
     */
    if (srcPageCnt == 0) {
        RAISE_EXCEPTION( EOK );
    }

    /* lock the source vd to prevent it being removed while migrating */
    if (srcVdIndex != (vdIndexT)(-1)) {
        svdp = vd_htop_if_valid(srcVdIndex, bfap->dmnP, TRUE, FALSE);
        if ( svdp == NULL ) {
            return EBAD_VDI;
        } else {
            srcVdBumped = TRUE;
        }
    }

    /* lock the destination vd to prevent it being removed while migrating */
    if (dstVdIndex != (vdIndexT)(-1)) {
        dvdp = vd_htop_if_valid(dstVdIndex, bfap->dmnP, TRUE, FALSE);
        if ( dvdp == NULL ) {
            RAISE_EXCEPTION( EBAD_VDI );
        } else
            dstVdBumped = 1;

        sts = sc_valid_vd (
                           bfap->dmnP,
                           bfap->reqServices,
                           bfap->optServices,
                           dstVdIndex
                           );
        if (sts != EOK) {
            if (sts == E_VOL_NOT_IN_SVC_CLASS) {
                sts = EBAD_VDI;
            }
            RAISE_EXCEPTION( sts );
        }
    }

    if ( (uint32T) dstBlkOffset != (uint32T) (-1)) {
        if ((dstBlkOffset % ADVFS_PGSZ_IN_BLKS) != 0) {
            RAISE_EXCEPTION( E_BLKOFFSET_NOT_PAGE_ALIGNED );
        }
    }

    sts = mig_migrate (
                       bfap,
                       srcVdIndex,
                       srcPageOffset,
                       srcPageCnt,
                       dstVdIndex,
                       dstBlkOffset,
                       forceFlag,
                       alloc_hint
                       );

HANDLE_EXCEPTION:

    if ( srcVdBumped ) {
        vd_dec_refcnt( svdp );
        srcVdBumped = 0;
    }
    if ( dstVdBumped ) {
        vd_dec_refcnt( dvdp );
        dstVdBumped = 0;
    }
    return sts;

} /* end bs_migrate */


/*
 * mig_verify_stripe_page_range -
 *
 * Verify that all the page range pages described by the
 * extent descriptor map are on the same disk.
 */

statusT
mig_verify_stripe_page_range (
                           bfAccessT *bfap,        /* in */
                           uint32T srcPageOffset,  /* in */
                           uint32T srcPageCnt,     /* in */
                           uint32T srcVdIndex,     /* in */
                           bsStripeHdrT **stripeHdrOut, /* out */
                           uint32T *xmPageOffset,   /* out */
                           pageRangeT **xmPageRange,    /* out */
                           uint32T *xmPageRangeCnt, /* out */
                           pageRangeT *xmHoleRange,    /* out */
                           vdIndexT *xmHoleVdIndex,  /* out */
                           uint32T *xmEndPageOffset,/* out */
                           uint32T *mapIndex        /* out */
                              )
{
    statusT sts = EOK;
    int unlockFlag = 0;
    bsInMemXtntMapT *xtntMap;
    uint32T bfEndPageOffset;
    bsXtntDescT xtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    pageRangeT *xmPageRangeLocal = NULL;
    bsStripeHdrT *stripeHdrLocal = NULL;

    /*
     * The caller must specify a source vdIndex.
     */
    if (srcVdIndex == (vdIndexT)(-1)) {
        return(EBAD_PARAMS);
    }

    /*
     * We acquire the on-disk extent map lock instead of the in-memory extent
     * map lock because we use the in-mem extent map's "allocDeallocPageCnt"
     * field.  You must hold the on-disk lock when using this field.  This lock
     * also prevents the in-mem map from changing.
     */
    MCELLIST_LOCK_WRITE(&(bfap->mcellList_lk) )
    unlockFlag = 1;

    stripeHdrLocal = bfap->xtnts.stripeXtntMap;

    /*
     * A convenient way to find the number of pages in the page range that
     * fall within each extent map.
     */
    str_calc_page_alloc (srcPageOffset, srcPageCnt, stripeHdrLocal);

    /*
     * Get the extent map relative page range info.  This also verifies that
     * the page range is mapped by the extent map.
     */

    *mapIndex = BFPAGE_TO_MAP( srcPageOffset,
                               stripeHdrLocal->cnt,
                               stripeHdrLocal->segmentSize);
    xtntMap = stripeHdrLocal->xtntMap[*mapIndex];
    *xmPageOffset = BFPAGE_TO_XMPAGE (
                                     srcPageOffset,
                                     stripeHdrLocal->cnt,
                                     stripeHdrLocal->segmentSize
                                     );
    sts = get_xm_page_range_info (
                                  xtntMap,
                                  *xmPageOffset,
                                  xtntMap->allocDeallocPageCnt,
                                  &(xmPageRangeLocal),
                                  xmPageRangeCnt,
                                  xmHoleRange,
                                  xmHoleVdIndex
                                  );
    if (sts != EOK ) {
        if (sts == E_PAGE_NOT_MAPPED) {
            bfEndPageOffset = (srcPageOffset + srcPageCnt) - 1;
            if (bfEndPageOffset < bfap->nextPage) {
                sts = E_BAD_PAGE_RANGE;
            }
        }
        RAISE_EXCEPTION (sts);
    }

    if (NULL != xmPageRangeLocal) {
        /*
         * Adjust page offset if there is a hole.
         */
        if (xmHoleRange->pageCnt){
            *xmPageOffset = MIN(xmPageRangeLocal[0].pageOffset,
                               xmHoleRange->pageOffset);
        } else {
            *xmPageOffset = xmPageRangeLocal[0].pageOffset;
        }
        *xmEndPageOffset = (xmPageRangeLocal[*xmPageRangeCnt - 1].pageOffset +
                           xmPageRangeLocal[*xmPageRangeCnt - 1].pageCnt) - 1;
    } else {
        *xmPageOffset = xmHoleRange->pageOffset;
        *xmEndPageOffset = (xmHoleRange->pageOffset + xmHoleRange->pageCnt) - 1;
    }

    /*
     * Verify that all the page range pages described by the
     * extent descriptor map are on the same disk.
     */

    imm_get_xtnt_desc (*xmPageOffset, xtntMap, 0, &xtntDescId, &xtntDesc);

    while ((xtntDesc.pageCnt > 0) &&
           (xtntDesc.pageOffset <= *xmEndPageOffset) &&
           (xtntDesc.volIndex == srcVdIndex)) {
        imm_get_next_xtnt_desc (xtntMap, &xtntDescId, &xtntDesc);
    }  /* end while */

    if ((xtntDesc.pageCnt > 0) &&
        (xtntDesc.pageOffset <= *xmEndPageOffset) &&
        (xtntDesc.volIndex != srcVdIndex)) {
        RAISE_EXCEPTION (E_BAD_PAGE_RANGE);
    }

    *xmPageRange = xmPageRangeLocal;
    *stripeHdrOut = stripeHdrLocal;
    /* fall thru to exception handler */

HANDLE_EXCEPTION:

    if (unlockFlag != 0) {
        MCELLIST_UNLOCK(&(bfap->mcellList_lk) )
    }
    return sts;

}  /* end mig_verify_stripe_page_range */


/*
 * mig_migrate
 *
 * This function is the internal interface to migrate.  It assumes that
 * all arguments are valid and that the file type can be migrated.
 *
 * This function calls the appropriate function, based on the bitfile type,
 * to migrate the file.
 *
 * This function assumes that the block range, if specified, is a continuous
 * range of blocks.  It does not work correctly if part of the block range
 * wraps from the domain's last block to the first block.
 *
 * NOTE:  A shadowed bitfile cannot be migrated.
 */

statusT
mig_migrate (
             bfAccessT *srcBfap,  /* in */
             vdIndexT srcVdIndex,  /* in */
             uint32T  srcPageOffset,  /* in */
             uint32T  srcPageCnt,  /* in */
             vdIndexT  dstVdIndex,  /* in */
             uint64T  dstBlkOffset,  /* in */
             uint32T  forceFlag,  /* in */
             bsAllocHintT alloc_hint  /* in */
             )
{
    bfSetT *bfSet;
    int noMetadataFlag;
    bfAccessT *origBfap;
    bfSetT *origBfSet;
    statusT sts=EOK;

    bfSet = srcBfap->bfSetp;
    if (bfSet->cloneId != BS_BFSET_ORIG) {
        /*
         * This is a clone.  If it doesn't have its own metadata, we can't
         * migrate it.
         */
        origBfap = srcBfap->origAccp;
        origBfSet = origBfap->bfSetp;

        /*
         * Synchronize with the "create metadata" portion of cow.
         */
        COW_LOCK_READ( &(srcBfap->cow_lk) )

        noMetadataFlag = 0;
        if (origBfap->cloneCnt != origBfSet->cloneCnt) {
            noMetadataFlag = 1;
        }
        COW_UNLOCK( &(srcBfap->cow_lk) )

        if (noMetadataFlag != 0) {
            return E_NO_CLONE_STG;
        }
    }

    /* If the file is  being created, and it is either a directory
     * or a "slow" symlink, then we might be racing with the file creation
     * code while it is adding storage. Synchronize with it by waiting for
     * the bfState to become valid.
     */

    if (srcBfap->bfState == BSRA_CREATING) {
        lk_wait_while(&srcBfap->stateLk, &srcBfap->bfaLock, ACC_INIT_TRANS);
        if (srcBfap->bfState != BSRA_VALID) {
            return EINVALID_HANDLE;
        }
    }

    if ((srcBfap->xtnts.type != BSXMT_APPEND) &&
        (srcBfap->xtnts.type != BSXMT_STRIPE)) {
        sts = ENOT_SUPPORTED;
        RAISE_EXCEPTION(sts);
    }

    switch (srcBfap->xtnts.type)  {

      case BSXMT_APPEND:

        if (srcVdIndex == (vdIndexT)(-1)) {
           /* moves all the pages in page range on all disk volumes
            * if dstVdIndex==-1, allocate space
            * on service class, otherwise on dstVdIndex only.
            */
            sts = migrate_normal (
                                  srcBfap,
                                  srcPageOffset,
                                  srcPageCnt,
                                  dstVdIndex,
                                  dstBlkOffset, /* cleared location  or -1*/
                                  forceFlag,
                                  alloc_hint
                                  );
        } else {
           /* migrate_normal_one_disk moves only the pages for given srcVdIndex.
            * if dstVdIndex==-1, allocate space on service class, otherwise
            * on dstVdIndex only.  Calls migrate_normal.
            */

            sts = migrate_normal_one_disk (
                                  srcBfap,
                                  srcVdIndex,
                                  srcPageOffset,
                                  srcPageCnt,
                                  dstVdIndex,
                                  dstBlkOffset, /* cleared location or -1 */
                                  forceFlag,
                                  alloc_hint
                                  );
        }

        break;

      case BSXMT_STRIPE:

        sts = migrate_stripe (
                              srcBfap,
                              srcVdIndex,
                              srcPageOffset,
                              srcPageCnt,
                              dstVdIndex,
                              dstBlkOffset, /* cleared location  or -1 */
                              forceFlag,
                              alloc_hint
                              );

        break;

      default:
        sts = ENOT_SUPPORTED;

    }  /* end switch */

HANDLE_EXCEPTION:

    return sts;

}  /* end mig_migrate */


/*
 * migrate_normal_one_disk
 *
 * This function migrates a normal bitfile.  It moves only the pages of the
 * page range that are on the specified source disk.
 */

static
statusT
migrate_normal_one_disk (
                         bfAccessT *bfap,  /* in */
                         vdIndexT srcVdIndex,  /* in */
                         uint32T bfPageOffset,  /* in */
                         uint32T bfPageCnt,  /* in */
                         vdIndexT dstVdIndex,  /* in */
                         uint64T  dstBlkOffset,  /* in */
                         uint32T  forceFlag, /* in */
                         bsAllocHintT alloc_hint  /* in */
                         )
{
    uint32T endPageOffset;
    bsXtntDescT endXtntDesc;
    bsInMemXtntMapT *newOverXtntMap = NULL;
    uint32T nextPageOffset;
    bsInMemXtntMapT *newXtntMap = NULL;
    bsXtntDescT nextXtntDesc;
    bsInMemXtntMapT *overXtntMap = NULL;
    uint32T pageCnt;
    uint32T pageOffset;
    bsXtntDescT startXtntDesc;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    int unlockFlag = 0;
    bsInMemXtntDescIdT xtntDescId;
    bsInMemXtntMapT *xtntMap;
    uint32T newtype,
            newmax;

    xtntMap = bfap->xtnts.xtntMap;

    XTNMAP_LOCK_READ( &(bfap->xtntMap_lk) )
    unlockFlag = 1;

    /*
     * Verify that the page range is mapped by the extent map.  If
     * not, bye bye!
     */

    endPageOffset = (bfPageOffset + bfPageCnt) - 1;
    imm_get_xtnt_desc (endPageOffset, xtntMap, 0, &xtntDescId, &endXtntDesc);
    if (endXtntDesc.pageCnt == 0) {
        RAISE_EXCEPTION (E_PAGE_NOT_MAPPED);
    }

    imm_get_xtnt_desc (bfPageOffset, xtntMap, 0, &xtntDescId, &startXtntDesc);
    if (startXtntDesc.pageCnt == 0) {
        RAISE_EXCEPTION (E_PAGE_NOT_MAPPED);
    }

    if (startXtntDesc.blkOffset == -1) {
        /*
         * The page range starts in a hole.  Does it end in the same hole?
         */
        if (endXtntDesc.pageOffset == startXtntDesc.pageOffset) {
            RAISE_EXCEPTION (E_CANT_MIGRATE_HOLE);
        }
    }

    /*
     * Create a fake overlay map.
     */
    sts = imm_create_xtnt_map (
                               xtntMap->blksPerPage,
                               xtntMap->domain,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &overXtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * If this file can have extent information in the primary mcell,
     * make the first subextent map type BSR_XTNTS.  Otherwise, make
     * it BSR_SHADOW_XTNTS.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(overXtntMap->domain->dmnVersion,
                                 overXtntMap->hdrType)) {
        newtype = BSR_XTNTS;
        newmax = BMT_XTNTS;
    }
    else {
        newtype = BSR_SHADOW_XTNTS;
        newmax = BMT_SHADOW_XTNTS;
    }
    sts = imm_init_sub_xtnt_map (
                                 &(overXtntMap->subXtntMap[0]),
                                 bfPageOffset,
                                 bfPageCnt,
                                 srcVdIndex,
                                 bsNilMCId,
                                 newtype,
                                 newmax,
                                 2  /* maxCnt */
                                 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    overXtntMap->cnt = 1;
    overXtntMap->validCnt = overXtntMap->cnt;

    subXtntMap = &(overXtntMap->subXtntMap[0]);
    subXtntMap->bsXA[1] = subXtntMap->bsXA[0];
    subXtntMap->bsXA[0].vdBlk = 0;
    subXtntMap->bsXA[1].bsPage = bfPageOffset + bfPageCnt;
    subXtntMap->cnt = 2;

    /*
     * Overlay the fake overlay extent map onto the real extent
     * map.  Essentially, clip the real extent map on the requested
     * page range boundary.
     *
     * FIX - there must be a better way of doing this.
     */
    sts = imm_overlay_xtnt_map (
                                xtntMap,
                                overXtntMap,
                                bfap->xtnts.type,
                                &newXtntMap,
                                &newOverXtntMap
                                );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    newOverXtntMap->validCnt = newOverXtntMap->cnt;

    unlockFlag = 0;
    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    imm_get_first_xtnt_desc (newOverXtntMap, &xtntDescId, &startXtntDesc);

    while (startXtntDesc.pageCnt > 0) {

        /*
         * Find the first extent descriptor that describes an extent on
         * the source disk.
         */
        while ((startXtntDesc.pageCnt > 0) &&
               (startXtntDesc.volIndex != srcVdIndex)) {

            imm_get_next_xtnt_desc(newOverXtntMap, &xtntDescId, &startXtntDesc);
        }  /* end while */

        if (startXtntDesc.pageCnt == 0) {
            /*
             * Did not find an entry that has a matching vdIndex.
             */
            break;  /* out of while */
        }

        /*
         * Find the next extent descriptor that describes an extent
         * on a disk other than the source disk.
         */

        nextPageOffset = startXtntDesc.pageOffset + startXtntDesc.pageCnt;
        imm_get_next_xtnt_desc (newOverXtntMap, &xtntDescId, &nextXtntDesc);
        while ((nextXtntDesc.pageCnt > 0) &&
               (nextXtntDesc.volIndex == srcVdIndex)) {

            nextPageOffset = nextXtntDesc.pageOffset + nextXtntDesc.pageCnt;
            imm_get_next_xtnt_desc (newOverXtntMap, &xtntDescId, &nextXtntDesc);
        }  /* end while */

        /*
         * FIX - must adjust blkoffset here somewhere when we go thru the loop
         * more than once.
         */

        pageOffset = startXtntDesc.pageOffset;
        pageCnt = nextPageOffset - pageOffset;

        sts = migrate_normal( bfap,
                              pageOffset,
                              pageCnt,
                              dstVdIndex,
                              dstBlkOffset,
                              forceFlag,
                              alloc_hint);
        if (sts != EOK) {
            if (sts == E_CANT_MIGRATE_HOLE) {
                /*
                 * The descriptor describes a hole.
                 *
                 * FIX - This can happen when there is a hole at the start of
                 * an mcell, we allocate storage at a page offset in the hole
                 * and the disk we get the storage from is different from the
                 * disk on which the mcell lives.  This should be fixed in
                 * add storage.
                 */
                domain_panic (bfap->dmnP, "migrate_normal_one_disk: ");
                RAISE_EXCEPTION(E_DOMAIN_PANIC);
            } else {
                RAISE_EXCEPTION (sts);
            }
        }

        imm_get_next_xtnt_desc (newOverXtntMap, &xtntDescId, &startXtntDesc);
    }  /* end while */

    imm_delete_xtnt_map (overXtntMap);
    imm_delete_xtnt_map (newXtntMap);
    imm_delete_xtnt_map (newOverXtntMap);

    return EOK;

HANDLE_EXCEPTION:

    if (unlockFlag != 0) {
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
    }
    if (overXtntMap != NULL) {
        imm_delete_xtnt_map (overXtntMap);
    }
    if (newXtntMap != NULL) {
        imm_delete_xtnt_map (newXtntMap);
    }
    if (newOverXtntMap != NULL) {
        imm_delete_xtnt_map (newOverXtntMap);
    }

    return sts;

}  /* end migrate_normal_one_disk */


/*
** This routine is called by migrate_normal & migrate_stripe.
** It may call C_CFS_CONDIO_EXCL_MODE_ENTER & CLU_CLXTNT_WRITE.
** return flags clu_excl_flgA & clu_xtntlk_flgA tell if these were called.
** If the file being migrated is an original file it calls
** hold_cloneset to hold the clone fileset in place
** against bs_bfs_delete. cloneSetpA tells if the clone fileset is held.
*/

static statusT
migrate_get_clu_locks( bfAccessT *bfap,
               bfSetT **cloneSetpA,
               int *setHeldA,
               int *clu_excl_flgA,
               int *clu_xtntlk_flgA )
{
    bfSetT *cloneSetp;
    bfAccessT *cloneap;
    int is_clone = (bfap->bfSetp->cloneId != BS_BFSET_ORIG) ? TRUE : FALSE;
    int clu_cow_mode_enter = 0;
    fsid_t fsid;
    statusT sts;
    bfMCIdT mcid;
    vdIndexT vdi;
    int ret;

    if ( !is_clone ) {
        /* Guard against race with rmfset the clone. */
        cloneSetp = hold_cloneset( bfap, TRUE );
        *cloneSetpA = cloneSetp;
        *setHeldA = TRUE;
    } else {
        cloneSetp = NULL;
        *cloneSetpA = NULL;
    }

    /*
     * On cluster systems, if the fileset is mounted,
     * we must revoke the read token on this file until
     * the migration has completed. This is done so that if this file
     * is subsequently opened for directIO, it will re-read the
     * extent maps after this migration has completed.
     * Only do this if this is a non-metadata regular file (VREG)
     * since only these files could be subsequently opened for directIO.
     */
    if ( bfap->dataSafety != BFD_FTX_AGENT  &&
         bfap->dataSafety != BFD_FTX_AGENT_TEMPORARY  &&
         bfap->bfVp->v_type == VREG &&
         bfap->bfSetp->fsnp ) {
        /*
         * We are in a cluster and need to get the directio token
         * exclusively.  Different rules are followed for
         * clones. Since C_CFS_CONDIO_EXCL_MODE_ENTER could get a
         * cnode (cfs vnode) we must not be in a root ftx. On the
         * other hand since the clone is guarenteed (by CFS) not
         * to get a cnode and the bs_cow_pg path is within a root
         * ftx, We can not hold the token exclusively
         * (CC_CFS_COW_MODE_ENTER) and start a root transaction
         * (we would deadlock with COW).
         */

        if ( !is_clone ) {
            ret = CC_CFS_CONDIO_EXCL_MODE_ENTER( bfap->bfVp );
            if ( ret != 0 ) {
                /*
                 * the only reason this can fail is if the system
                 * is out of vnodes
                 */
                return ENO_MORE_MEMORY;
            }

            *clu_excl_flgA = 1;
        } else {
            clu_cow_mode_enter = 1;
        }
    }

    /* Protect clone fileset tag file contents & nextCloneAccp,noClone. */
    COW_LOCK_WRITE( &bfap->cow_lk );

    if ( cloneSetp &&
         tagdir_lookup(cloneSetp, &bfap->tag, &mcid, &vdi) == EOK ) {
        struct vnode *nullvp = NULL;

        /*
         * CFS guarentees that the cnodes will be reffed on
         * the server if a client has the clone open. We
         * however need to open the clone internally in case
         * it is opened externally during the migration. This
         * way we can get a lock (later in this path) that
         * will stop the xtnt maps from being read.
         */
        /*
         * There is a clone fileset. Check to see if this
         * file itself has a clone.
         */
        sts = bs_access_one( &cloneap,
                             bfap->tag,
                             cloneSetp,
                             FtxNilFtxH,
                             0,
                             NULL,
                             &nullvp,
                             NULL );
        if ( sts != EOK ) {
            /* There doesn't appear to be a clone any more so do nothing. */
            bfap->noClone = TRUE;
            COW_UNLOCK( &bfap->cow_lk );
            return sts;
        }

        bfap->nextCloneAccp = cloneap;
        bfap->noClone = FALSE;

        if ( cloneap->dataSafety != BFD_FTX_AGENT  &&
             cloneap->dataSafety != BFD_FTX_AGENT_TEMPORARY ) {
            /*
             * If this file has a clone then the non COWed
             * portion of the clone extent map will be changed
             * and we will need to revoke the read token if it exists.
             * The clu_clonextnt_lk will prevent the clone
             * from being openned if we can't revoke the
             * token (this is all done farther down in this routine).
             */

            clu_cow_mode_enter = 1;
        } else {
            /*
             * This file can not have clu directio
             * so we don't need the access structure
             */
            bs_close_one( cloneap, 0, FtxNilFtxH);
        }
    }

    COW_UNLOCK( &bfap->cow_lk );

    if ( clu_cow_mode_enter ) {
        bfAccessT *cloneap;

        /* We are in a cluster and we are either a clone
         * or an original that has a clone.
         */
        if ( is_clone ) {
            cloneap = bfap;
        } else {
            cloneap = bfap->nextCloneAccp;
        }

        /*
         * Get the cluster dio token exclusively so that clients
         * can not use their cached extent maps. The will need to
         * refresh them from the server after reobtaining their
         * token
         */
        BS_GET_FSID( cloneap->bfSetp, fsid );

        while ( !*clu_xtntlk_flgA ) {
            if ( cloneap->bfSetp->fsnp ) {
                /* Try to get the token. */
                ret = CC_CFS_COW_MODE_ENTER( fsid, cloneap->tag );

                if ( ret == 0 ) {
                    /*
                     * We have the token so no clients can have cached clone
                     * extent maps. Clear the cloneXtntsRetrieved so we
                     * can tell if in the up coming window a client requested
                     * and obtained the clone extents.
                     */
                    cloneap->cloneXtntsRetrieved = 0;

                    CC_CFS_COW_MODE_LEAVE( fsid, cloneap->tag );
                }
            }

            /*
             * No cluster clients currently had cached extent maps for
             * this clone. We need to prevent them from obtaining them
             * however during the rest of this migration. This is done by
             * holding the CLU_CLXTNT_WRITE lock.
             */
            /*
             * At this point either we obtained the token and marked the
             * cloneXtntsRetrieved as clean or we were unable no clients
             * had the clone token. At this point a client could obtain
             * the token and/or the extents. We will deal with this possibility
             * shortly.
             *
             * Note: For a client to obtain the clone extent maps they
             * must have the token, the clu_clonextnt_lk lock AND the
             * cow_lk lock (see bs_cow_pg).
             *
             * Also for hierachy reasons we can NOT release the token while
             * holding the clu_clonextnt_lk lock (it does a vrele which can
             * trigger an msfs_reclaim)
             */

            CLU_CLXTNT_WRITE( &cloneap->clu_clonextnt_lk );
            *clu_xtntlk_flgA = 1;

            /*
             * Lets do the cluster clone shuffle:
             *
             * Since we don't have the token a client could sneak
             * in here and get the token before we lock the clonextnt_lk.
             * This would be bad since we would not know to revoke their
             * token/xtntmaps after the migration is complete.
             *
             * Each time a cluster client request a clone extent map
             * they will (while holding the clonextnt_lk) set the
             * cloneXtntsRetrieved flag in the access structure. This
             * will tell us if they snuck in during the above window.
             *
             * This loop should generally only happen once. If the client
             * did sneak in then they will be holding the token
             * and when we loop we will wait for them to drop the token
             * breaking the loop.
             */

            if ( cloneap->cloneXtntsRetrieved ) {
                cloneap->cloneXtntsRetrieved = 0;
                CLU_CLXTNT_UNLOCK( &cloneap->clu_clonextnt_lk );
                *clu_xtntlk_flgA = 0;
            }
        }

        /*
         * Clients can now get the token
         * but the clu_clonextnt_lk will prevent them from getting
         * the clone's extnet maps until migrate is done.
         */
    }

    return EOK;
}


/*
** Called by migrate_normal & migrate_stripe.
*/
static statusT
pre_reset_block_map_special( bfAccessT *bfap )
{
    bfSetT *cloneSetp;
    statusT sts;
    bfAccessT *cloneap = NULL;
    struct vnode *nullvp = NULL;
    uint32T numPgs;

    MS_SMP_ASSERT(bfap->bfSetp->cloneId == BS_BFSET_ORIG);
    cloneSetp = bfap->bfSetp->cloneSetp;
    MS_SMP_ASSERT(cloneSetp->bfsHoldCnt > 0);  /* this set is held */

    /* This next block identifies migration of a normal file
     * who has a clone. It then calls reset_block_map_special
     * providing the clones bfap. reset_block_map_special is used
     * for remapping cached clone pages that are still mapped by the
     * original file. (non-COWed pages) The pageRange is all pages
     * in the file, page 0 through n where n is the total number of
     * pages that make up the clone.
     */

    /* There is a clone fileset. Check to see if this
     * file itself has a clone.
     */

    COW_LOCK_WRITE( &bfap->cow_lk );

    /*
     * call bs_access_one looking for an in-memory
     * instance only.  We do not want to do any io here.
     */
    sts = bs_access_one( &cloneap,
                         bfap->tag,
                         cloneSetp,
                         FtxNilFtxH,
                         BF_OP_INMEM_ONLY,
                         NULL,
                         &nullvp,
                         bfap );
    if ( sts != EOK ) {
        /* There doesn't appear to be a clone in memory so do nothing. */
        bfap->noClone = TRUE;
        COW_UNLOCK( &bfap->cow_lk );

        return EOK;
    }

    bfap->noClone = FALSE;

    /*
     * Does clone have it's own metadata?
     * If not, don't use maxClonePgs.
     */
    numPgs = (cloneap->cloneId) ? cloneap->maxClonePgs : bfap->nextPage;

    COW_UNLOCK( &bfap->cow_lk );

    /* This sts is returned to the caller. */
    sts = reset_block_map_special ( cloneap, numPgs );
    if ( sts != EOK ) {
        /*
         * Looking for a very highly unlikely scenario.
         * E_OUT_OF_SYNC_CLONE is acceptable due to
         * ENO_MORE_BLKS.
         * If sts != EOK, we call bs_invalidate_pages()
         * on the clone because reset_block_map_special()
         * failed to remap all the in-memory pages,
         * and therefore subsequent readers of those pages
         * may hit the panic
         */
         MS_SMP_ASSERT(sts == E_OUT_OF_SYNC_CLONE);
         bs_invalidate_pages(cloneap,0, 0, 0);
         sts = EOK;
    }

    bs_close_one( cloneap, 0, FtxNilFtxH);  /* close clone  */

    return sts;
}


int clone_migrate_retries = 0;  /* Number of collisions with bs_cow_pg() */

/*
 * migrate_normal
 *
 * This function migrates a normal bitfile.  It moves all the pages within
 * the page range.
 */

static
statusT
migrate_normal (
                bfAccessT *bfap,  /* in */
                uint32T srcPageOffset,  /* in */
                uint32T srcPageCnt,  /* in */
                vdIndexT dstVdIndex,  /* in */
                uint64T dstBlkOffset,   /* in */
                uint32T  forceFlag,      /* in */
                bsAllocHintT alloc_hint  /* in */
                )
{
    bfMCIdT copyMcellId;
    vdIndexT copyVdIndex;
    bsInMemXtntMapT *copyXtntMap = NULL;
    bsInMemXtntT copyXtnts;
    uint32T i,
            copyXferSize = 16; /* set default to 1 page of 16 blocks */
    pageRangeT *pageRange = NULL;
    uint32T pageRangeCnt = 0;
    pageRangeT holeRange;
    vdIndexT holeVdIndex;
    int remCopyStgFlag = 0;
    int unlkmigTrunc = 0;
    statusT sts;
    statusT sts2;
    int switch_stg_ran=0;
    int xtnt_map_list_removed=0;
    struct actRange *arp = NULL;
    ftxHT ftxH = FtxNilFtxH;
    int startedFtxFlag = 0;
    int is_clone = (bfap->bfSetp->cloneId != BS_BFSET_ORIG) ? TRUE : FALSE;
    unsigned long savedCowPgCount;
    unsigned int clu_cow_mode_enter = 0;
    unsigned int do_cluster_cleanup = 0;
    unsigned int do_clxtnt_unlock = 0;
    int truncXferFlg = 0;
    dlm_lkid_t cfs_domain_lock ;
    bfSetT *cloneSetp = NULL;
    int setHeld = FALSE;
    uint32T startPg;

    if ( (bfap->bfVp) && clu_is_ready() ) {
        sts = migrate_get_clu_locks( bfap, &cloneSetp, &setHeld, 
                                     &do_cluster_cleanup, &do_clxtnt_unlock );
        if ( sts != EOK ) {
            RAISE_EXCEPTION( sts );
        }
    }

    /* Allocate an active range structure; this synchronizes migrate
     * with local directIO and msfs_cfs_flush_and_invalidate. In the
     * past it was stated the CFS token mechanism prevented either
     * from occuring, however, corruption from CFS page invalidation
     * while a migrate is running has proven that to be unreliable so
     * now we always lock the backdoor ourselves.
     */
    arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));
    /* Initialize fields; ms_malloc() inited struct to zeros.  */
    arp->arStartBlock = srcPageOffset * bfap->bfPageSz;
    arp->arEndBlock = ((srcPageOffset + srcPageCnt) * bfap->bfPageSz) - 1;
    arp->arState = AR_MIGRATE;
    arp->arDebug = SET_LINE_AND_THREAD(__LINE__);

    /* Put this active range onto the list to block directIO threads
     * in the same range.  This also prevents truncation/invalidation.
     */
    insert_actRange_onto_list( bfap, arp, NULL );   /* may sleep */

    /* Coordinate with xfer_xtnts_to_clone which does not bump cowPgCount */
    if ( bfap->origAccp ) {
        TRUNC_XFER_LOCK_READ(&bfap->origAccp->trunc_xfer_lk);
        truncXferFlg = 1;
    }

get_copy_storage:
    startPg = srcPageOffset;

    /*
     * Need to take the migTruncLk before calling get_xm_page_range_info().
     * This prevents another thread from adding or removing any storage
     * to/from the file until the migration is complete.  Formerly, this
     * lock was taken after calling alloc_copy_stg().  That left a window
     * of time between the releasing of the extent map lock after the
     * call to get_xm_page_range_info() and the taking of the migTrunc_lk.
     * In that window, another thread could add storage to a hole,
     * causing the pageRange produced by get_xm_page_range_info() to become
     * out-of-date.  But the code didn't detect that and at the end of the
     * migration, the pages that had been added to the hole were lost.
     */
    MIGTRUNC_LOCK_WRITE( &(bfap->xtnts.migTruncLk) )
    unlkmigTrunc = 1;

    /*
     * Get the page range information.  The page ranges returned are extent
     * map relative.  This is ok because for an append bitfile, the bitfile
     * relative and extent map relative page offsets are the same.
     *
     * Make sure that the in-memory extent maps are still valid.
     * Returns with xtntMap_lk read-locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = get_xm_page_range_info (
                                  bfap->xtnts.xtntMap,
                                  srcPageOffset,
                                  srcPageCnt,
                                  &pageRange,
                                  &pageRangeCnt,
                                  &holeRange,
                                  &holeVdIndex
                                  );

    XTNMAP_UNLOCK(&(bfap->xtntMap_lk))

    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * For clones, the ordering of the migTruncLk with respect to
     * starting a root transaction is the opposite of the ordering
     * that is done for non-clone files.  Namely, for clones, if
     * a root transaction is to be started and the migTruncLk is to
     * be locked, the root transaction must be started first.  This
     * is to allow us to take the migTruncLk in bs_cow_pg().  Since
     * both alloc_copy_stg() and alloc_hole_stg() start root 
     * transactions, we must now drop the migTruncLk for clone files.
     * This has the effect of opening a window in which new storage
     * may be added to the clone (via COW processing by another thread).
     * Therefore, we will note the clone access structure's cowPgCount
     * and recheck it once we have the migTruncLk held again.  If 
     * another thread came in and added storage to the clone that we
     * "missed", we will retry.
     */
    if (is_clone) {
        savedCowPgCount = bfap->cowPgCount;
        MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) )
        unlkmigTrunc = 0;
    }

    if (pageRange != NULL) {
        
        /* 
         * Adjust page offset if there is a hole.
         */
        if (holeRange.pageCnt) {
            startPg = MIN(pageRange[0].pageOffset, holeRange.pageOffset);
        } else {
            MS_SMP_ASSERT(srcPageOffset == pageRange[0].pageOffset);
        }
        
        sts = alloc_copy_stg (
                              bfap,
                              startPg,
                              pageRange,
                              pageRangeCnt,
                              dstVdIndex,
                              dstBlkOffset,
                              alloc_hint,
                              &copyVdIndex,
                              &copyMcellId,
                              &copyXtntMap,
                              &copyXferSize
            );

    } else {
        /*
         * If we are migrating a permanent hole then
         * the Vd is the requested vd
         */

        if (holeRange.pageType == PERM_HOLE_START) {
            holeVdIndex = dstVdIndex;
        } else {
            RAISE_EXCEPTION (EOK);
        }

        sts = alloc_hole_stg (
                              bfap,
                              &holeRange,
                              holeVdIndex,
                              &copyVdIndex,
                              &copyMcellId,
                              &copyXtntMap
            );
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    
    /*
     * If we're migrating a clone, start a new transaction so that
     * we can relock the migTruncLk in proper order.  This transaction
     * will span the remainder of the migrate.  Once we have the migTruncLk
     * again, check to see if another thread has added more storage to
     * the clone during the interval in which we did not hold the 
     * migTruncLk.  If so, abort the transaction, try to deallocate
     * the storage we allocated above (it is already on the DDL so
     * if the deallocation fails for some reason, we'll ignore that,
     * since it will be cleaned up later), and start over.
     */
    if (is_clone) {
        sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 0);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        startedFtxFlag = 1;
        MIGTRUNC_LOCK_WRITE( &(bfap->xtnts.migTruncLk) )
        unlkmigTrunc = 1;
        if (savedCowPgCount != bfap->cowPgCount) {
            MS_SMP_ASSERT(bfap->cowPgCount > savedCowPgCount);
            MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) )
            unlkmigTrunc = 0;
            startedFtxFlag = 0;
            ftx_fail(ftxH);
            sts2 = del_dealloc_stg(copyMcellId,
                                   VD_HTOP(copyVdIndex, bfap->dmnP));
            clone_migrate_retries++;
            if (pageRange != NULL) 
                ms_free (pageRange);
            goto get_copy_storage;
        }
    }

    remCopyStgFlag = 1;

    copyXtnts.validFlag = 0;
    copyXtnts.xtntMap = NULL;
    copyXtnts.shadowXtntMap = NULL;
    copyXtnts.stripeXtntMap = NULL;
    copyXtnts.copyXtntMap = copyXtntMap;
    copyXtnts.type = BSXMT_APPEND;
    copyXtnts.allocPageCnt = 0;

    /*
     * The "nextValidPage" must be correctly set before inserting the map onto
     * the copy list.
     */
    if (pageRange != NULL) {
        imm_set_next_valid_copy_page (&copyXtnts, pageRange[0].pageOffset);
    }

    /*
     * After inserting the copy extent map into the copy extent map list,
     * any new pages brought into the buffer cache for this bitfile are
     * mapped to both the original's and the copy's storage.
     */

    XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )

    cp_insert_onto_xtnt_map_list (
                                  &(bfap->xtnts.copyXtntMap),
                                  copyXtntMap
                                  );

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    /*
     * Create the copy.
     *
     * Note:  We do not start a transaction before calling the
     * cp_copy_page_range() function.  This is because that function may
     * take a long period of time to complete.  If we start a transaction,
     * our active transaction might cause problems when the logger wraps
     * the log.  Or, many migrates may get started and the active transactions
     * would cause the log to fill up, or at least half way fill up, and
     * not allow the logger to wrap the log.
     */

    /*
     * FIX - cp_copy_page_range must test for intersecting src and dest.
     * If intersection, must know how to move blocks.  This is only needed
     * if the extent maps involved in the copy describe some of the same
     * on-disk storage.  (Why would they ever do this??)
     */

    /*
     * FIX - A possible status is E_PAGE_NOT_MAPPED.  This can happen
     * if storage is removed while the copy is being made.  If so, cleanup
     * and quit.  This must be tested when "remove_stg()" is written.
     */

    /*
     * We don't care how cp_copy_page_range() and switch_stg() complete.
     * For both success and failure, we remove the copy extent map
     * from the copy list, clean the cache and deallocate the copy's
     * storage.
     */

    sts = cp_copy_page_range( bfap,
                              pageRange,
                              pageRangeCnt,
                              &copyXtnts,
                              copyXferSize,
                              forceFlag);
    if (sts == EOK) {
        sts = switch_stg (
                          bfap,
                          0,
                          &(bfap->xtnts.xtntMap),
                          &copyVdIndex,
                          &copyMcellId,
                          copyXtntMap,
                          ftxH
                          );
        if (sts == EOK)
            switch_stg_ran=1;
    }

    if (startedFtxFlag) {
        startedFtxFlag = 0;
        ftx_done_u (ftxH, FTA_BS_MIG_MIGRATE_V1, 0, 0);
    }

    /*
     * New pages that are brought into the buffer cache are still mapped to
     * both the original's and copy's storage.  Set "nextValidPage" so that
     * page-to-block translations fail for the copy.  For any new page, it
     * maps to the original's storage only.
     */

    imm_set_next_valid_copy_page (&copyXtnts, 0);


    XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )

    sts2 = cp_remove_from_xtnt_map_list (
                                         &(bfap->xtnts.copyXtntMap),
                                         copyXtntMap
                                         );
    if (sts2 != EOK) {
        /*
         * We can't delete the copy's in-memory extent map because we assume
         * it is still on the copy extent map list.  When the bitfile's access
         * structure is re-used, the copy extent map is deallocated.
         */
        copyXtntMap = NULL;
        /*
         * We won't deallocate the copy's storage because the copy extent map
         * still maps the storage.  Even though "nextValidPage" prevents any
         * new page from getting mapped to the copy, this (not deallocating
         * the copy's storage) is a safety precaution.
         */
        remCopyStgFlag = 0;
    } else {
        xtnt_map_list_removed = 1;
    }

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    /*
     * For those bitfile pages still in the buffer cache, update each
     * page's block map so that the page maps the original's storage
     * only.  We must do this before deallocating the copy's storage
     * because there may be bitfile pages in the cache whose block map
     * maps both the original's and the copy's storage.
     */

    for (i = 0; i < pageRangeCnt; i++) {
        if (pageRange[i].pageType == STG) {
            sts2 = reset_block_map (bfap,
                                    pageRange[i].pageOffset,
                                    pageRange[i].pageCnt);
            if (sts2 != EOK) {
                /* We can't deallocate the copy's storage because
                 * there may be some bitfile pages still in the cache
                 * that map both the original's and the copy's
                 * pages. The copy's storage is still on the delete
                 * pending list and is deallocated during the next
                 * system boot.  
                 */
                remCopyStgFlag = 0;
            }
        }
    }  /* end for */

    if ( !is_clone && cloneSetp ) {
        sts = pre_reset_block_map_special( bfap );
    }

    /* fall thru to exception handler */

HANDLE_EXCEPTION:

    if ( setHeld ) {
        if ( cloneSetp != NULL ) {
            release_cloneset( cloneSetp );
        } else {
            release_cloneset( bfap->bfSetp );
        }
    }

    if (startedFtxFlag != 0) {
        ftx_fail (ftxH);
    }

    if (pageRange != NULL) {
        ms_free (pageRange);
    }

    if (copyXtntMap != NULL) {
        imm_delete_xtnt_map (copyXtntMap);
    }

    if ( unlkmigTrunc ) {
        MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) )
    }

    if ( truncXferFlg ) {
        MS_SMP_ASSERT( bfap->origAccp != NULL );
        TRUNC_XFER_UNLOCK(&bfap->origAccp->trunc_xfer_lk);
    }

    if ( do_cluster_cleanup ) {
        /* Files open for direct IO need to invalidate the UBC pages
         * which migrate brought in, since another cluster node
         * could be performing i/o directly to disk. This would
         * bypass the cache checking which is normally done for advfs
         * directio on a single system.
         * All pages should already have been flushed to storage by
         * cp_copy_page_range() so we don't need to do that.
         */

        if (bfap->bfVp->v_flag & VDIRECTIO) {

            /* 1. Files open for direct IO need to invalidate the UBC pages
             *    used by migrate to avoid another node writing to the disk
             *    followed by this node reading stale data from the UBC page.
             * 2. No direct IO writer will dirty the page between the flush
             *    and the invalidate since direct IO uses the extent map token
             *    (aka 'read token') that is released below and we have the
             *    active range for these pages which is also released below.
             * NOT a problem for a clone being migrated (read only)?
             */
            ubc_invalidate( bfap->bfObj,
                (vm_offset_t)(srcPageOffset * bfap->bfPageSz * BS_BLKSIZE),
                (vm_size_t)(srcPageCnt * bfap->bfPageSz * BS_BLKSIZE),
                B_DONE);
        }
    }

    if (do_clxtnt_unlock) {

        bfAccessT *cloneap;
        if (is_clone) cloneap = bfap;
        else cloneap = bfap->nextCloneAccp;
        CLU_CLXTNT_UNLOCK(&(cloneap->clu_clonextnt_lk));
        if (!is_clone) bs_close_one( cloneap, 0, FtxNilFtxH);
        do_clxtnt_unlock = 0;
    }

    /* We need to remove and free the actRange struct */
    if ( arp ) {
        mutex_lock(&bfap->actRangeLock);
        MS_SMP_ASSERT( arp->arIosOutstanding == 0 );
        remove_actRange_from_list(bfap, arp);   /* also wakes waiters */
        mutex_unlock(&bfap->actRangeLock);
        ms_free(arp);
    }

    if ( do_cluster_cleanup ) {

        /* On cluster systems, release the read token so that the 
         * file's extent maps will be re-loaded.
         */

        CC_CFS_CONDIO_EXCL_MODE_LEAVE(bfap->bfVp);
        do_cluster_cleanup = 0;
    }

    if ( remCopyStgFlag != 0 ||
         (switch_stg_ran == 0 && xtnt_map_list_removed == 1) ) {
        /*
         * Remove the copy's storage allocation information and deallocate
         * the storage.  If this fails, the storage is deallocated during
         * the next system boot.
         */

        sts2 = del_dealloc_stg (
                                copyMcellId,
                                VD_HTOP(copyVdIndex, bfap->dmnP)
                                );
        /* ignore status */
    }

    return sts;

}  /* end migrate_normal */


/*
 * migrate_stripe
 *
 * This function migrates part of a striped bitfile.  Which part is specified
 * by the "srcVdIndex", "srcPageOffset" and "srcPageCnt" parameters.  This
 * function finds the extent map that describes pages on the specified disk.
 * Then, it moves the page range described by the extent map.
 *
 * This function verifies that each page in the page range is located on the
 * same disk.  If not, it returns an error.
 */

static
statusT
migrate_stripe (
                bfAccessT *bfap,  /* in */
                vdIndexT srcVdIndex,  /* in */
                uint32T srcPageOffset,  /* in */
                uint32T srcPageCnt,  /* in */
                vdIndexT dstVdIndex,  /* in */
                uint64T  dstBlkOffset,  /* in */
                uint32T  forceFlag, /* in */
                bsAllocHintT alloc_hint  /* in */
                )
{
    uint32T bfEndPageOffset;
    uint32T bfPageCnt;
    uint32T bfPageOffset;
    uint32T bfPageRangeCnt = 0;
    pageRangeT *bfPageRange = NULL;
    pageRangeT bfHoleRange;
    vdIndexT bfHoleVdIndex;
    bfMCIdT copyMcellId;
    vdIndexT copyVdIndex;
    bsInMemXtntMapT *bfCopyXtntMap = NULL;
    bsInMemXtntT copyXtnts;
    uint32T i,
            copyXferSize = 16;  /* set the default to one page of 16 blocks */
    uint32T mapIndex;
    int remCopyStgFlag = 0;
    bsStripeHdrT *stripeHdr=NULL;
    statusT sts;
    statusT sts2;
    int unlockFlag = 0;
    int unlkmigTrunc = 0;
    bsInMemXtntMapT *xmCopyXtntMap = NULL;
    uint32T xmEndPageOffset;
    uint32T xmPageCnt;
    uint32T xmPageOffset;
    uint32T xmPageRangeCnt = 0;
    pageRangeT *xmPageRange = NULL;
    pageRangeT xmHoleRange;
    vdIndexT xmHoleVdIndex;
    bsXtntDescT xtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    bsInMemXtntMapT *xtntMap;
    int switch_stg_ran=0;
    int xtnt_map_list_removed=0;
    struct actRange *arp = NULL;
    ftxHT ftxH = FtxNilFtxH;
    int startedFtxFlag = 0;
    int is_clone = (bfap->bfSetp->cloneId != BS_BFSET_ORIG) ? TRUE : FALSE;
    unsigned long savedCowPgCount;
    unsigned int clu_cow_mode_enter = 0;
    unsigned int do_cluster_cleanup = 0;
    unsigned int do_clxtnt_unlock = 0;
    int truncXferFlg = 0;
    dlm_lkid_t cfs_domain_lock ;
    bfSetT *cloneSetp = NULL;
    int setHeld = FALSE;

    /*
     * The caller must specify a source vdIndex.
     */
    if (srcVdIndex == (vdIndexT)(-1)) {
        return(EBAD_PARAMS);
    }

    if ( (bfap->bfVp) &&  clu_is_ready() ) {
        sts = migrate_get_clu_locks( bfap, &cloneSetp, &setHeld,
                                     &do_cluster_cleanup, &do_clxtnt_unlock );
        if ( sts != EOK ) {
            RAISE_EXCEPTION( sts );
        }
    }

    /* Allocate an active range structure; this synchronizes migrate
     * with local directIO and msfs_cfs_flush_and_invalidate. In the
     * past it was stated the CFS token mechanism prevented either
     * from occuring, however, corruption from CFS page invalidation
     * while a migrate is running has proven that to be unreliable so
     * now we always lock the backdoor ourselves.
     */
    arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));
    /* Initialize fields; ms_malloc() inited struct to zeros.  */
    arp->arStartBlock = srcPageOffset * bfap->bfPageSz;
    arp->arEndBlock = ((srcPageOffset + srcPageCnt) * bfap->bfPageSz) - 1;
    arp->arState = AR_MIGRATE;
    arp->arDebug = SET_LINE_AND_THREAD(__LINE__);
 
    /* Put this active range onto the list to block directIO threads
     * in the same range.  This also prevents truncation/invalidation.
     */
    insert_actRange_onto_list( bfap, arp, NULL );   /* may sleep */

    /* Coordinate with xfer_xtnts_to_clone which does not bump cowPgCount */
    if ( bfap->origAccp ) {
        TRUNC_XFER_LOCK_READ(&bfap->origAccp->trunc_xfer_lk);
        truncXferFlg = 1;
    }

get_copy_storage:

    /*
     * Need to take this lock before calling get_xm_page_range_info().
     * This prevents another thread from adding or removing any storage
     * to/from the file until the migration is complete.  Formerly, this
     * lock was taken after calling alloc_copy_stg().  That left a window
     * of time between the releasing of the extent map lock after the
     * call to get_xm_page_range_info() and the taking of the migTrunc_lk.
     * In that window, another thread could add storage to a hole,
     * causing the pageRange produced by get_xm_page_range_info() to become
     * out-of-date.  But the code didn't detect that and at the end of the
     * migration, the pages that had been added to the hole were lost.
     */
    MIGTRUNC_LOCK_WRITE( &(bfap->xtnts.migTruncLk) )
    unlkmigTrunc = 1;

    sts = mig_verify_stripe_page_range (
                                         bfap,
                                         srcPageOffset,
                                         srcPageCnt,
                                         srcVdIndex,
                                         &(stripeHdr),
                                         &xmPageOffset,
                                         &(xmPageRange),
                                         &xmPageRangeCnt,
                                         &xmHoleRange,
                                         &xmHoleVdIndex,
                                         &xmEndPageOffset,
                                         &mapIndex
                                       );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * For clones, the ordering of the migTruncLk with respect to
     * starting a root transaction is the opposite of the ordering
     * that is done for non-clone files.  Namely, for clones, if
     * a root transaction is to be started and the migTruncLk is to
     * be locked, the root transaction must be started first.  This
     * is to allow us to take the migTruncLk in bs_cow_pg().  Since
     * both alloc_copy_stg() and alloc_hole_stg() start root
     * transactions, we must now drop the migTruncLk for clone files.
     * This has the effect of opening a window in which new storage
     * may be added to the clone (via COW processing by another thread).
     * Therefore, we will note the clone access structure's cowPgCount
     * and recheck it once we have the migTruncLk held again.  If
     * another thread came in and added storage to the clone that we
     * "missed", we will retry.
     */
    if (is_clone) {
        savedCowPgCount = bfap->cowPgCount;
        MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) )
        unlkmigTrunc = 0;
    }

    /*
     * All the page range pages are on the same disk.
     */

    if (xmPageRange != NULL) {
        sts = alloc_copy_stg (
                              bfap,
                              xmPageOffset,
                              xmPageRange,
                              xmPageRangeCnt,
                              dstVdIndex,
                              dstBlkOffset,
                              alloc_hint,
                              &copyVdIndex,
                              &copyMcellId,
                              &xmCopyXtntMap,
                              &copyXferSize
                              );
    } else {

        if (xmHoleRange.pageType == PERM_HOLE_START)
            xmHoleVdIndex = dstVdIndex;
        sts = alloc_hole_stg (
                              bfap,
                              &xmHoleRange,
                              xmHoleVdIndex,
                              &copyVdIndex,
                              &copyMcellId,
                              &xmCopyXtntMap
                              );
    }
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * If we're migrating a clone, start a new transaction so that
     * we can relock the migTruncLk in proper order.  This transaction
     * will span the remainder of the migrate.  Once we have the migTruncLk
     * again, check to see if another thread has added more storage to
     * the clone during the interval in which we did not hold the
     * migTruncLk.  If so, abort the transaction, try to deallocate
     * the storage we allocated above (it is already on the DDL so
     * if the deallocation fails for some reason, we'll ignore that,
     * since it will be cleaned up later), and start over.
     */
    if (is_clone) {
        sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 0);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        startedFtxFlag = 1;
        MIGTRUNC_LOCK_WRITE( &(bfap->xtnts.migTruncLk) )
        unlkmigTrunc = 1;
        if (savedCowPgCount != bfap->cowPgCount) {
            MS_SMP_ASSERT(bfap->cowPgCount > savedCowPgCount);
            MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) )
            unlkmigTrunc = 0;
            ftx_fail(ftxH);
            startedFtxFlag = 0;
            sts2 = del_dealloc_stg(copyMcellId,
                                   VD_HTOP(copyVdIndex, bfap->dmnP));
            clone_migrate_retries++;
            if (xmPageRange != NULL) 
                ms_free (xmPageRange);
            goto get_copy_storage;
        }
    }

    remCopyStgFlag = 1;

    /*
     * Create a new extent map from the copy extent map so that the new extent
     * map's page offsets are bitfile relative rather than extent map relative.
     */
    sts = str_create_bf_rel_xtnt_map (
                                      stripeHdr,
                                      mapIndex,
                                      xmCopyXtntMap,
                                      bfap->xtnts.type,
                                      &bfCopyXtntMap
                                      );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Get bitfile relative page range info from the bitfile relative
     * extent map.
     */
    bfPageOffset = XMPAGE_TO_BFPAGE (
                                     mapIndex,
                                     xmPageOffset,
                                     stripeHdr->cnt,
                                     stripeHdr->segmentSize
                                     );
    bfEndPageOffset = XMPAGE_TO_BFPAGE (
                                        mapIndex,
                                        xmEndPageOffset,
                                        stripeHdr->cnt,
                                        stripeHdr->segmentSize
                                        );
    bfPageCnt = (bfEndPageOffset - bfPageOffset) + 1;

    sts = get_xm_page_range_info (
                                  bfCopyXtntMap,
                                  bfPageOffset,
                                  bfPageCnt,
                                  &(bfPageRange),
                                  &(bfPageRangeCnt),
                                  &(bfHoleRange),
                                  &(bfHoleVdIndex)
                                  );
    if (sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }

    copyXtnts.validFlag = 0;
    copyXtnts.xtntMap = NULL;
    copyXtnts.shadowXtntMap = NULL;
    copyXtnts.stripeXtntMap = NULL;
    copyXtnts.copyXtntMap = bfCopyXtntMap;
    copyXtnts.type = BSXMT_STRIPE;
    copyXtnts.allocPageCnt = 0;

    /*
     * The "nextValidPage" must be correctly set before inserting the map onto
     * the copy list.
     */
    if (NULL != bfPageRange) {
        imm_set_next_valid_copy_page (&copyXtnts, bfPageRange[0].pageOffset);
    }

    /*
     * After inserting the copy extent map into the copy extent map list,
     * any new pages brought into the buffer cache for this bitfile are
     * mapped to both the original's and the copy's storage.
     */

    XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )

    cp_insert_onto_xtnt_map_list (
                                  &(bfap->xtnts.copyXtntMap),
                                  bfCopyXtntMap
                                  );

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    /*
     * Create the copy.
     *
     * Note:  We do not start a transaction before calling the
     * cp_copy_page_range() function.  This is because that function
     * may take a long period of time to complete.  If we start a
     * transaction, our active transaction might cause problems when
     * the logger wraps the log.  Or, many migrates may get started
     * and the active transactions would cause the log to fill up,
     * or at least half way fill up, and not allow the logger to wrap the log.
     */

    /*
     * FIX - cp_copy_page_range must test for intersecting src and dest.
     * If intersection, must know how to move blocks.  This is only needed
     * if the extent maps involved in the copy describe some of the same
     * on-disk storage.  (Why would they ever do this??)
     */

    /*
     * FIX - A possible status is E_PAGE_NOT_MAPPED.  This can happen
     * if storage is removed while the copy is being made.  If so, cleanup
     * and quit.  This must be tested when "remove_stg()" is written.
     */

    /*
     * We don't care how cp_copy_page_range and switch_stg() complete.
     * For both success and failure, we remove the copy extent map
     * from the copy list, clean the cache and deallocate the copy's
     * storage.
     */

    sts = cp_copy_page_range( bfap,
                              bfPageRange,
                              bfPageRangeCnt,
                              &copyXtnts,
                              copyXferSize,
                              forceFlag);
    if (sts == EOK) {
        sts = switch_stg (
                          bfap,
                          mapIndex,
                          &(stripeHdr->xtntMap[mapIndex]),
                          &copyVdIndex,
                          &copyMcellId,
                          xmCopyXtntMap,
                          ftxH
                          );
        if (sts == EOK)
            switch_stg_ran=1;
    }

    if (startedFtxFlag) {
        startedFtxFlag = 0;
        ftx_done_u (ftxH, FTA_BS_MIG_MIGRATE_V1, 0, 0);
    }

    /*
     * New pages that are brought into the buffer cache are still mapped to
     * both the original's and copy's storage.  Set "nextValidPage" so that
     * page-to-block translations fail for the copy.  For any new page, it
     * maps to the original's storage only.
     */

    imm_set_next_valid_copy_page (&copyXtnts, 0);

    XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) )

    sts2 = cp_remove_from_xtnt_map_list (
                                         &(bfap->xtnts.copyXtntMap),
                                         bfCopyXtntMap
                                         );
    if (sts2 != EOK) {
        /*
         * We can't delete the copy's in-memory extent map because we assume
         * it is still on the copy extent map list.  When the bitfile's access
         * structure is re-used, the copy extent map is deallocated.
         */
        bfCopyXtntMap = NULL;
        /*
         * We won't deallocate the copy's storage because the copy extent map
         * still maps the storage.  Even though "nextValidPage" prevents any
         * new page from getting mapped to the copy, this (not deallocating
         * the copy's storage) is a safety precaution.
         */
        remCopyStgFlag = 0;
    } else {
        xtnt_map_list_removed = 1;
    }

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    /*
     * For those bitfile pages still in the buffer cache, update each
     * page's block map so that the page maps the original's storage
     * only.  We must do this before deallocating the copy's storage
     * because there may be bitfile pages in the cache whose block map
     * maps both the original's and the copy's storage.
     */

    for (i = 0; i < bfPageRangeCnt; i++) {
        if (bfPageRange[i].pageType == STG) {
            sts2 = reset_block_map (bfap,
                                    bfPageRange[i].pageOffset,
                                    bfPageRange[i].pageCnt);
            if (sts2 != EOK) {
                /*
                 * We can't deallocate the copy's storage because there may be
                 * some bitfile pages still in the cache that map both the
                 * original's and the copy's pages. The copy's storage is still
                 * on the delete pending list and is deallocated during the next
                 * system boot.
                 */
                remCopyStgFlag = 0;
            }
        }
    }  /* end for */

    if ( !is_clone && cloneSetp ) {
        sts = pre_reset_block_map_special( bfap );
    }

    /* fall thru to exception handler */

HANDLE_EXCEPTION:

    if ( setHeld ) {
        if ( cloneSetp != NULL ) {
            release_cloneset( cloneSetp );
        } else {
            release_cloneset( bfap->bfSetp );
        }
    }

    if (startedFtxFlag != 0) {
        ftx_fail (ftxH);
    }

    if (unlockFlag != 0) {
        MCELLIST_UNLOCK(&(bfap->mcellList_lk) )
    }
    if (xmPageRange != NULL) {
        ms_free (xmPageRange);
    }
    if (xmCopyXtntMap != NULL) {
        imm_delete_xtnt_map (xmCopyXtntMap);
    }
    if (bfPageRange != NULL) {
        ms_free (bfPageRange);
    }
    if (bfCopyXtntMap != NULL) {
        imm_delete_xtnt_map (bfCopyXtntMap);
    }

    if ( unlkmigTrunc ) {
        MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) )
    }
    if ( truncXferFlg ) {
        MS_SMP_ASSERT( bfap->origAccp != NULL );
        TRUNC_XFER_UNLOCK(&bfap->origAccp->trunc_xfer_lk);
    }

    if ( do_cluster_cleanup ) {
        /* Files open for direct IO need to invalidate the UBC pages
         * which migrate brought in, since another cluster node
         * could be performing i/o directly to disk. This would
         * bypass the cache checking which is normally done for advfs
         * directio on a single system.
         * All pages should already have been flushed to storage by
         * cp_copy_page_range() so we don't need to do that.
         */

        if (bfap->bfVp->v_flag & VDIRECTIO)
            ubc_invalidate( bfap->bfObj,
                (vm_offset_t)(srcPageOffset * bfap->bfPageSz * BS_BLKSIZE),
                (vm_size_t)(srcPageCnt * bfap->bfPageSz * BS_BLKSIZE),
                B_DONE);

    }
    if (do_clxtnt_unlock) {

        bfAccessT *cloneap;
        if (is_clone) cloneap = bfap;
        else cloneap = bfap->nextCloneAccp;
        CLU_CLXTNT_UNLOCK(&(cloneap->clu_clonextnt_lk));
        if (!is_clone) bs_close_one( cloneap, 0, FtxNilFtxH);
        do_clxtnt_unlock = 0;
    }
    
     /* We need to remove and free the actRange struct */
    if ( arp ) {
        mutex_lock(&bfap->actRangeLock);
        MS_SMP_ASSERT( arp->arIosOutstanding == 0 );
        remove_actRange_from_list(bfap, arp);   /* also wakes waiters */
        mutex_unlock(&bfap->actRangeLock);
        ms_free(arp);
    }

    if ( do_cluster_cleanup ) {

        /* On cluster systems, release the read token so that the 
         * file's extent maps will be re-loaded.
         */

        CC_CFS_CONDIO_EXCL_MODE_LEAVE(bfap->bfVp);
        do_cluster_cleanup = 0;
    }

    if ( remCopyStgFlag != 0 ||
         (switch_stg_ran == 0 && xtnt_map_list_removed == 1) ) {
        /*
         * Remove the copy's storage allocation information and deallocate
         * the storage.  If this fails, the storage is deallocated during
         * the next system boot.
         */

        sts2 = del_dealloc_stg (
                                copyMcellId,
                                VD_HTOP(copyVdIndex, bfap->dmnP)
                                );
        /* ignore status */
    }

    return sts;

}  /* end migrate_stripe */


/*
 * get_xm_page_range_info
 *
 * This function scans the extent map and creates a page range list that
 * summarizes the contiguous page ranges within the specified page range.
 *
 * The page range page offsets are relative to the extent map, not the
 * bitfile.  For normal and shadowed bitfiles, the map-relative and
 * bitfile-relative page offsets are the same.  For striped bitfiles,
 * they are different.
 *
 * The valid pageTypes returned for xmPageRange are :
 * 
 *  STG - indicates that this range is backed by real storage.
 *  PERM_HOLE_START - indicates that this range is a permanent hole 
 *                    (clones only).
 * The valid pageTypes returned for xmHoleRange are:
 *
 *  PERM_HOLE_START - indicates that this range is a permanent hole 
 *                    (clones only).
 *  XTNT_TERM - indicates that this is a normal hole.
 *  
 * The pageType of NOSTG is internal to this routine only.
 *
 * The caller of this function must own the in-memory extent map lock.
 */

#define NOSTG 1


static
statusT
get_xm_page_range_info (
                        bsInMemXtntMapT *xtntMap,  /* in */
                        uint32T  xmPageOffset,  /* in */
                        uint32T  xmPageCnt,  /* in */
                        pageRangeT **xmPageRange,  /* out */
                        uint32T *xmPageRangeCnt,  /* out */
                        pageRangeT *xmHoleRange,  /* out */
                        vdIndexT *xmHoleVdIndex  /* out */
                        )
{
    uint32T cnt;
    uint32T endPageOffset;
    bsXtntDescT endXtntDesc;
    bsInMemXtntDescIdT endXtntDescId;
    uint32T maxCnt;
    pageRangeT *pageRange = NULL;
    uint32T pageOffset;
    statusT sts;
    bsXtntDescT xtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    uint32T xtntEndPageOffset;
    bsXtntDescT hXtntDesc;
    bsInMemXtntDescIdT hXtntDescId;
    uint32T last_extent=0;

    /*
     * Verify that the page range is mapped by the extent map.  If not, bye bye!
     */

    /* Get the extent mapping the beginning of the range */

    MS_SMP_ASSERT(xtntMap != NULL);
    imm_get_xtnt_desc (xmPageOffset, xtntMap, 0, &xtntDescId, &xtntDesc);
    if (xtntDesc.pageCnt == 0) {
        return E_PAGE_NOT_MAPPED;
    }

    if ( xmPageOffset != 0 && xmPageOffset == xtntDesc.pageOffset ) {
        /* The requested starting page is the first page in an extent and */
        /* it is not the first extent. If the previous extent is a hole it */
        /* must be migrated with the hole. */
        imm_get_xtnt_desc( xmPageOffset - 1, xtntMap, 0,
                           &hXtntDescId, &hXtntDesc);
        if (xtntDesc.pageCnt == 0) {
            /* This can't be. The next page is mapped. We don't allow */
            /* extent maps that don't start from zero. */
            domain_panic( xtntMap->domain,
                          "get_xm_page_range_info: imm_get_xtnt_desc failed");
            return E_PAGE_NOT_MAPPED;
        }
        if ( hXtntDesc.blkOffset == XTNT_TERM ) {
            /* The previous extent is a normal hole. */
            /* Migrate this hole alloc with the following storage. */
            xtntDesc = hXtntDesc;
            xtntDescId = hXtntDescId;
        }
    }

    /* Get the extent mapping the end of the range */

    endPageOffset = (xmPageOffset + xmPageCnt) - 1;
    imm_get_xtnt_desc (endPageOffset, xtntMap, 0, &endXtntDescId, &endXtntDesc);

    if (endXtntDesc.pageCnt == 0) {
        return E_PAGE_NOT_MAPPED;
    }

    xmHoleRange->pageOffset = 0;
    xmHoleRange->pageCnt = 0;
    xmHoleRange->pageType = STG;

    /* We need to catch if this is a request to just migrate a hole.
     * We must deal with this, especially in cases where we may be
     * removing a volume. Not allowing a hole to migrate could cause
     * the rmvol to fail.
     */

    if ( (xtntDesc.blkOffset == XTNT_TERM) ||
         (xtntDesc.blkOffset == PERM_HOLE_START) ) {
        /*
         * The page range starts in a hole.
         *
         * Does the page range end in the same hole?
         *
         * Set the xmHoleVdIndex to the volume on which the first
         * extent of real data following the hole resides.  This
         * will make the hole migrate to whereever *that* extent
         * migrates to (or stays at).
         */

        if (endXtntDesc.pageOffset == xtntDesc.pageOffset) {
            xmHoleRange->pageCnt = xtntDesc.pageCnt;
            xmHoleRange->pageOffset = xtntDesc.pageOffset;
            xmHoleRange->pageType = xtntDesc.blkOffset;
            *xmPageRange = NULL;
            *xmPageRangeCnt = 0;

            if (xtntDesc.blkOffset == PERM_HOLE_START) {
                /* This is the special case of a permanent
                 * hole. Return the range in hand and the caller needs
                 * to setup the vd index */

                *xmHoleVdIndex = bsNilVdIndex;
            } else {
                /* Make sure this hole sticks to the storage that follows. */
                /*
                ** In a well behaved domain, we don't need to get the next
                ** xtnt since this hole will already be stuck to the storage
                ** extent that follows (be on the same volume). But because
                ** this error existed before, holes could be left behind
                ** at the end of a subextent on one volume when storage
                ** was migrated to another volume.
                */
                hXtntDescId = xtntDescId;
                imm_get_next_xtnt_desc (xtntMap, &hXtntDescId, &hXtntDesc);
                if ( hXtntDesc.pageCnt == 0 ) {
                    /* This can't be. The next page is mapped. */
                    /* We don't allow xtnt maps to end with a hole. */
                    domain_panic( xtntMap->domain,
                       "get_xm_page_range_info: imm_get_next_xtnt_desc failed");
                    return E_PAGE_NOT_MAPPED;
                }
                *xmHoleVdIndex = hXtntDesc.volIndex;
            }
            return EOK;
        }
        if (xtntDesc.blkOffset == XTNT_TERM) {
            /* If there is a leading hole (not permanent) then
             * we need to inform the caller so that it will stick
             * to the storage that follows it
             */
            xmHoleRange->pageCnt = xtntDesc.pageCnt;
            xmHoleRange->pageOffset = xtntDesc.pageOffset;
            xmHoleRange->pageType = XTNT_TERM;
            /* Make sure this hole sticks to the storage that follows. */
            hXtntDescId = xtntDescId;
            imm_get_next_xtnt_desc (xtntMap, &hXtntDescId, &hXtntDesc);
            if ( hXtntDesc.pageCnt == 0 ) {
                /* This can't be. The next page is mapped. */
                /* We don't allow xtnt maps to end with a hole. */
                domain_panic( xtntMap->domain,
                       "get_xm_page_range_info: imm_get_next_xtnt_desc failed");
                return E_PAGE_NOT_MAPPED;
            }
            *xmHoleVdIndex = hXtntDesc.volIndex;
        }
    }
    
    /* Adjust the starting extent range to be the requested start.
     * In case we are asked to migrate a partial range. If we are 
     * starting with a permanent hole then move the entire hole.
     * If it is a normal hole then it will be skipped.
     */

    if ((xtntDesc.pageOffset < xmPageOffset) &&
        (xtntDesc.blkOffset != PERM_HOLE_START) &&
        (xtntDesc.blkOffset != XTNT_TERM)) {
        xtntDesc.pageCnt -= (xmPageOffset - xtntDesc.pageOffset);
        xtntDesc.pageOffset = xmPageOffset;
    }

    if (endXtntDesc.blkOffset == XTNT_TERM) {
        /*
         * The page range ends in a hole. Cause the while loop
         * below to end before this extent.
         */

        endPageOffset = endXtntDesc.pageOffset - 1;
    }

    MS_SMP_ASSERT(xtntDesc.pageCnt > 0);

    cnt = 0;
    maxCnt = 1;
    pageRange = (pageRangeT *) ms_malloc (sizeof (pageRangeT) * maxCnt);
    if (pageRange == NULL) {
        return ENO_MORE_MEMORY;
    }

    xtntEndPageOffset = xtntDesc.pageOffset;
    pageRange[0].pageType = NOSTG;

    while (!last_extent) {
        last_extent = (xtntEndPageOffset + xtntDesc.pageCnt > endPageOffset);

        switch(xtntDesc.blkOffset) {
        case XTNT_TERM : 

            /* We hit a normal hole. End the page range and
             * start a new page range. If the current pageRange[cnt]
             * is not initialized then do nothing. This handles
             * a leading hole and (maybe impossible) adjacent holes
             */
            if (last_extent) {
                /* This is probably an MS_SMP_ASSERT situation, but 
                 * two consective holes at the end of the file will be
                 * handled. We already dealt with a single hole at the
                 * end of the file by reducing out range. Do it again
                 */
                if (pageRange[cnt].pageType == NOSTG) {
                    /* This is actually a more than two holes in a row case.
                     * just remove the last extent from the page range array
                     */
                    cnt --;
                }
                /* else do nothing. This takes care of it. */
                break;
            }

            if (pageRange[cnt].pageType != NOSTG) {
                cnt++;
                if (cnt == maxCnt) {
                    sts = extend_page_range_list (&maxCnt, &pageRange);
                    if (sts != EOK) {
                        RAISE_EXCEPTION (sts);
                    }
                }
                pageRange[cnt].pageType = NOSTG;
            }

            break;

        case PERM_HOLE_START :
            /* This is a permanent hole. End the current range, add this
             * hole to the range and start a new range.
             */

            if ((pageRange[cnt].pageType == NOSTG) &&
               (cnt > 0) && 
               (pageRange[cnt-1].pageType == PERM_HOLE_START) &&
               (pageRange[cnt-1].pageOffset + pageRange[cnt-1].pageCnt ==
                xtntDesc.pageOffset)) {
                
                /* One could argue that this should be an assertion.
                 * This deals with two consecutive permanent holes.
                 * we just glom them together. I don't beleive we
                 * should ever be in this situation, but we might as
                 * well handle it.
                 */
                
                pageRange[cnt-1].pageCnt += xtntDesc.pageCnt;

                /* Unlike a normal hole which is always (usually ?)
                 * associated with the backing store of the extent
                 * that follows it, permanent holes can be explicity
                 * migrated. We will impose that the entire hole be
                 * moved. Partial migration of a permanent hole is not
                 * allowed.  */

                if ( last_extent ) {
                    cnt--;
                }
                break;
            }

            if (pageRange[cnt].pageType != NOSTG) {
                MS_SMP_ASSERT(pageRange[cnt].pageType == STG);
                /* This is a range in progress. Terminate it
                 * and start a new one.
                 */

                cnt++;
                if ( cnt == maxCnt ) { 
                    sts = extend_page_range_list (&maxCnt, &pageRange);
                    if (sts != EOK) {
                        RAISE_EXCEPTION (sts);
                    }
                }
            }
            pageRange[cnt].pageOffset = xtntDesc.pageOffset;
            pageRange[cnt].pageCnt = xtntDesc.pageCnt;
            pageRange[cnt].pageType = PERM_HOLE_START;
            
            /* We're done don't advance the array */
            if ( last_extent ) {
                break;
            }

            cnt++;
            if ( cnt == maxCnt ) { 
                sts = extend_page_range_list (&maxCnt, &pageRange);
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }
            }

            pageRange[cnt].pageType = NOSTG;
    
            break;

        default: 
            /* We get here if we are a not a hole.
             * Extend the existing range or start a new
             * one.
             */

            if (pageRange[cnt].pageType == NOSTG) {
                pageRange[cnt].pageOffset = xtntDesc.pageOffset;

                /* Add the last extent into our page range
                 * array. Truncate the extent if the requested range
                 * does not fully overlap.  */

                if ( last_extent ) {
                    pageRange[cnt].pageCnt = 
                        (endPageOffset - xtntDesc.pageOffset) + 1;
                } else {
                    pageRange[cnt].pageCnt = xtntDesc.pageCnt;
                }

                pageRange[cnt].pageType = STG;
            } else {
                MS_SMP_ASSERT(pageRange[cnt].pageType == STG);
                if ( last_extent ) {
                    pageRange[cnt].pageCnt += 
                        ((endPageOffset - xtntDesc.pageOffset) + 1);
                } else {
                    pageRange[cnt].pageCnt += xtntDesc.pageCnt;
                }
            }
        }
        
        if ( last_extent ) {
            break;
        }
        
        xtntEndPageOffset += xtntDesc.pageCnt;
        imm_get_next_xtnt_desc (xtntMap, &xtntDescId, &xtntDesc);
        MS_SMP_ASSERT(xtntDesc.volIndex != bsNilVdIndex);
    }
    
    *xmPageRange = pageRange;
    *xmPageRangeCnt = cnt+1;

    return EOK;

HANDLE_EXCEPTION:

    if (pageRange != NULL) {
        ms_free (pageRange);
    }
    return sts;

}  /* end get_xm_page_range_info */


/*
 * extend_page_range_list
 *
 * This function extends the page range list.
 */

static
statusT
extend_page_range_list (
                        uint32T *maxCnt,  /* in/out */
                        pageRangeT **pageRange  /* in/out */
                        )
{
    uint32T i;
    uint32T newMaxCnt;
    pageRangeT *newPageRange;
    uint32T oldMaxCnt;
    pageRangeT *oldPageRange;

    oldPageRange = *pageRange;
    oldMaxCnt = *maxCnt;

    newMaxCnt = oldMaxCnt + 5;
    newPageRange = (pageRangeT *) ms_malloc (sizeof (pageRangeT) * newMaxCnt);
    if (newPageRange == NULL) {
        return ENO_MORE_MEMORY;
    }
    for (i = 0; i < oldMaxCnt; i++) {
        newPageRange[i] = oldPageRange[i];
    }  /* end for */
    ms_free (oldPageRange);

    *maxCnt = newMaxCnt;
    *pageRange = newPageRange;

    return EOK;

}  /* end extend_page_range_list */


/*
 * switch_stg
 *
 * This function switches the storage allocated to the original with the
 * storage allocated to the copy.
 *
 * At the start of this function, the original's allocated storage is described
 * by the in-memory and the on-disk extent maps.  The copy's allocated storage
 * is described by an in-memory extent map and mcells on the delete pending list.
 * This function modifies these data structures so that the copy's storage is
 * described by the original's data structures and the original's storage is
 * described by the copy's data structures.
 *
 * NOTE:  The address of the location that contains the original extent map
 * address cannot change.
 */

static
statusT
switch_stg (
            bfAccessT        *bfap,              /* in */
            uint32T          stripeIndex,        /* in */
            bsInMemXtntMapT  **origXtntMapAddr,  /* in */
            vdIndexT         *copyVdIndex,       /* in, modified */
            bfMCIdT          *copyMcellId,       /* in, modified */
            bsInMemXtntMapT  *copyXtntMap,       /* in, modified */
            ftxHT            parentFtxH          /* in */
            )
{
    bfTagT bfSetTag;
    ftxHT ftxH;
    int failFtxFlag = 0;
    bfMCIdT newCopyMcellId;
    vdIndexT newCopyVdIndex;
    bsInMemXtntMapT *newCopyXtntMap = NULL;
    bsInMemXtntMapT *newOrigXtntMap = NULL;
    bfMCIdT oldCopyMcellId;
    vdIndexT oldCopyVdIndex;
    bsInMemXtntMapT *origXtntMap;
    statusT sts;

    bfSetTag = bfap->bfSetp->dirTag;

    oldCopyVdIndex = *copyVdIndex;
    oldCopyMcellId = *copyMcellId;

    sts = FTX_START_N(FTA_NULL, &ftxH, parentFtxH, bfap->dmnP, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    FTX_LOCKWRITE(&(bfap->mcellList_lk), ftxH)

    /*
     * Get the original extent map's address from the location that points
     * to the extent map.  Passing the extent map address in this manner
     * is needed so that the caller does not have to acquire any locks
     * before calling this function.
     *
     * NOTE:  The address of the location that contains the extent map
     * address cannot change.
     */
    origXtntMap = *origXtntMapAddr;

    /*
     * Switch the storage allocation information in the original's and
     * copy's in-memory extent maps.
     */
    /*
     * FIX - if storage was removed during the copy, cleanup and quit.
     * Implement when "remove_stg()" and "stg_add_stg()" undo are implemented.
     *
     * FIX - the mcellList_lk/xtntMap_lk syncs the "removed_stg" flag.  Since
     * we own the mcellList_lk, we are synched.
     */

    /* 
     * The mcell specified by oldCopyVdIndex and oldCopyMcellId is a 
     * pseudo-primary mcell currently pointing to the mcells described in 
     * copyXtntMap.  Separate the pseudo-primary mcell from the extents; 
     * overlay_xtnt_map() will get only the extents.
     */
    sts = bmt_unlink_mcells ( bfap->dmnP,
                              bfap->tag,
                              oldCopyVdIndex,
                              oldCopyMcellId,
                              copyXtntMap->subXtntMap[0].vdIndex,
                              copyXtntMap->subXtntMap[0].mcellId,
                              copyXtntMap->subXtntMap[copyXtntMap->cnt - 1].vdIndex,
                              copyXtntMap->subXtntMap[copyXtntMap->cnt - 1].mcellId,
                              ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    /* Merge copy into orig returning newOrig. newCopy is an extent map */
    /* of the replaced extents from orig. */
    sts = overlay_xtnt_map ( bfap,
                             stripeIndex,
                             origXtntMap,
                             copyXtntMap,
                             &newOrigXtntMap,
                             &newCopyXtntMap,
                             ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Free the old pseudo-primary mcell from the copy extent. */
    bmt_free_bf_mcells ( VD_HTOP(oldCopyVdIndex, bfap->dmnP),
                         oldCopyMcellId,
                         ftxH,
                         TRUE);

    /* Get a new primary mcell and attach it to the newCopy extent map. */
    sts = create_xtnt_map_hdr ( bfap,
                newCopyXtntMap->subXtntMap[0].vdIndex,
                newCopyXtntMap->subXtntMap[0].mcellId,
                newCopyXtntMap->subXtntMap[newCopyXtntMap->cnt - 1].vdIndex,
                newCopyXtntMap->subXtntMap[newCopyXtntMap->cnt - 1].mcellId,
                0,  /* no clone xfer xtnts here */
                ftxH,
                &newCopyVdIndex,
                &newCopyMcellId);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Put the chain of mcells descibing newCopy storage on the defered */
    /* delete list. */
    sts = del_add_to_del_list ( newCopyMcellId,
                               VD_HTOP(newCopyVdIndex, bfap->dmnP),
                               TRUE,  /* start sub ftx */
                               ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    FTX_LOCKWRITE(&(bfap->xtntMap_lk), ftxH)

    /*
     * Update the original's and copy's in-memory maps so they map the
     * switched storage allocation information.  New pages brought into
     * the buffer cache are still mapped to both the original and the copy.
     */

    imm_delete_sub_xtnt_maps (origXtntMap);
    origXtntMap->hdrType = newOrigXtntMap->hdrType;
    origXtntMap->hdrVdIndex = newOrigXtntMap->hdrVdIndex;
    origXtntMap->hdrMcellId = newOrigXtntMap->hdrMcellId;
    origXtntMap->cnt = newOrigXtntMap->cnt;
    origXtntMap->maxCnt = newOrigXtntMap->maxCnt;
    origXtntMap->updateStart = origXtntMap->cnt;
    origXtntMap->validCnt = origXtntMap->cnt;
    origXtntMap->subXtntMap = newOrigXtntMap->subXtntMap;

    newOrigXtntMap->subXtntMap = NULL;
    imm_delete_xtnt_map (newOrigXtntMap);

    imm_delete_sub_xtnt_maps (copyXtntMap);
    copyXtntMap->hdrType = newCopyXtntMap->hdrType;
    copyXtntMap->hdrVdIndex = newCopyXtntMap->hdrVdIndex;
    copyXtntMap->hdrMcellId = newCopyXtntMap->hdrMcellId;
    copyXtntMap->cnt = newCopyXtntMap->cnt;
    copyXtntMap->maxCnt = newCopyXtntMap->maxCnt;
    copyXtntMap->updateStart = copyXtntMap->cnt;
    copyXtntMap->validCnt = copyXtntMap->cnt;
    copyXtntMap->subXtntMap = newCopyXtntMap->subXtntMap;

    newCopyXtntMap->subXtntMap = NULL;
    imm_delete_xtnt_map (newCopyXtntMap);

    failFtxFlag = 0;
    ftx_done_u (ftxH, FTA_BS_MIG_MIGRATE_V1, 0, 0);

    *copyVdIndex = newCopyVdIndex;
    *copyMcellId = newCopyMcellId;

    return EOK;

HANDLE_EXCEPTION:

    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
    }
    if (newOrigXtntMap != NULL) {
        imm_delete_xtnt_map (newOrigXtntMap);
    }
    if (newCopyXtntMap != NULL) {
        imm_delete_xtnt_map (newCopyXtntMap);
    }

    return sts;

}  /* end switch_stg */


/*
 * bs_move_metadata
 *
 * This function moves a bitfile's non-extent metadata to another disk in
 * the service class.  The non-extent metadata mcells are the primary mcell
 * and all other mcells linked to it thru the mcell header's next mcell
 * pointer.
 *
 * Synchronization:
 *
 * This function must synchronize with readers and writers of bitfile
 * metadata:
 *
 *    1.  Readers are handled by taking out the bitfile's mecllList_lk lock.
 *    Any reader must first acquire the lock before following the mcell list.
 *    This function acquires the lock after starting the transaction because
 *    of the lock's locking protocol: acquire after starting a transaction.
 *
 *    2.  Writers under transaction control are handled by the ftx_start_exc()
 *    function.  The function waits until all transactions finish and then it
 *    blocks any new transaction starts.  This prevents all metadata updates
 *    except ours.
 *
 *    In addition, because all transactions have committed, no undo routines
 *    will run.  The bitfile's primary mcell pointer exists only in its bitfile
 *    set's tag directory and in its in-memory access structure.
 */

statusT
bs_move_metadata (
                  bfAccessT *bfap, /* in */
                  vdT       *vdp   /* in - if NULL, don't care which vd */
                  )
{
    bfSetT *bfSet;
    bfAccessT *cloneBfap;
    int closeCloneFlag = 0;
    int failFtxFlag = 0;
    ftxHT ftxH;
    statusT sts;
    struct vnode *nullvp = NULL;
    int vdRefed=FALSE;

    if (BS_BFTAG_RSVD (bfap->tag)) {
        RAISE_EXCEPTION (ENOT_SUPPORTED);
    }

    if (vdp == NULL ) {
        sts = sc_select_vd_for_mcell (
                                      &vdp,
                                      bfap->dmnP,
                                      bfap->dmnP->scTbl,
                                      bfap->reqServices,
                                      bfap->optServices
                                      );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        vdRefed = TRUE;
    }

    sts = FTX_START_EXC(FTA_NULL, &ftxH, bfap->dmnP, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    bfSet = bfap->bfSetp;

    if (bfSet->cloneId == BS_BFSET_ORIG) {
        /*
         * NOTE: If the bitfile's clone does not exist, we don't have
         * to deal with it.  If the bitfile's clone does exist, we are
         * guaranteed that the clone can't be deleted because we
         * started an exclusive transaction above.
         */
        if ((bfap->cloneCnt == bfSet->cloneCnt) ||
            ((bfap->cloneCnt != bfSet->cloneCnt) &&
             ((!bs_have_clone (bfap->tag, bfSet->cloneSetp, ftxH)) ||
                 (bfSet->cloneSetp->state == BFS_DELETING)))) {
            /*
             * This bitfile does not have a clone.  Or, it does have a clone and
             * the clone has its own metadata.  Or, the bitfile had a clone, the
             * clone did not have its own metadata and the clone was deleted.
             */

            FTX_LOCKWRITE(&(bfap->mcellList_lk), ftxH)

            sts = move_metadata (bfap, NULL, vdp->vdIndex, ftxH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            failFtxFlag = 0;
            ftx_done_n (ftxH, FTA_BS_MIG_MOVE_METADATA_EXC_V1);
        } else {
            /*
             * This bitfile has a clone and the clone does not have its own
             * metadata.
             */

            /*
             * Open the clone.  When the original bitfile set is accessed, all
             * clone bitfile sets are accessed as well.
             */
            sts = bs_access (&cloneBfap, bfap->tag, bfSet->cloneSetp,
                             FtxNilFtxH, 0, NULLMT, &nullvp);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            closeCloneFlag = 1;

            FTX_LOCKWRITE(&(bfap->mcellList_lk), ftxH)
            FTX_LOCKWRITE(&(cloneBfap->mcellList_lk), ftxH)

            sts = move_metadata (bfap, cloneBfap, vdp->vdIndex, ftxH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            failFtxFlag = 0;
            ftx_done_n (ftxH, FTA_BS_MIG_MOVE_METADATA_EXC_V1);

            closeCloneFlag = 0;
            bs_close (cloneBfap, 0);
        }
    } else {
        /*
         * This bitfile is a clone. (Or, a pod. I can never remember.)
         */

        cloneBfap = bfap;

        /*
         * The clone's original is always accessed when the clone is accessed.
         */
        bfap = cloneBfap->origAccp;

        bfSet = bfap->bfSetp;

        if (bfap->cloneCnt == bfSet->cloneCnt) {
            /*
             * The clone has its own metadata.
             */

            FTX_LOCKWRITE(&(cloneBfap->mcellList_lk), ftxH)

            sts = move_metadata (cloneBfap, NULL, vdp->vdIndex, ftxH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            failFtxFlag = 0;
            ftx_done_n (ftxH, FTA_BS_MIG_MOVE_METADATA_EXC_V1);
        } else {
            ftx_quit (ftxH);
        }
    }

    if (vdRefed == TRUE)
        vd_dec_refcnt(vdp);

    return EOK;

HANDLE_EXCEPTION:

    if (vdRefed == TRUE) {
        vd_dec_refcnt(vdp);
    }

    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
    }

    if (closeCloneFlag != 0) {
        bs_close (cloneBfap, 0);
    }

    return sts;

}  /* end bs_move_metadata */


/*
 * move_metadata
 *
 * This function moves the specified bitfile's metadata.
 * (See bs_move_metadata()'s function description.)
 *
 * This function assumes that it has exclusive access to the bitfile's and its
 * clone's metadata.  Also, exclusive access to each bitfile's tag directory
 * entry.
 *
 * NOTE:  When this function returns a status value of EOK, the caller cannot
 * fail the parent transaction as this function cannot undo its in-memory
 * modifications.
 *
 * Added a new variable localVdIndex, this is needed when the primary
 * mcell record chain crosses volumes, and the BSR_XTNTS record contains
 * extent.  This was to keep the kernel from getting into an infinite
 * loop during rmvol.
 *
 * A special case is were the newVdIndex is different from the volume
 * which contains the head of the primary mcell chain, and this mcell
 * contains extent information.  To save having to make large changes to
 * this section of code, a solution was found were if we move the
 * primary mcell to a different mcell on the same volume we get around
 * this problem.
 */

static
statusT
move_metadata (
               bfAccessT *bfap,  /* in */
               bfAccessT *cloneBfap,  /* in */
               vdIndexT newVdIndex,  /* in */
               ftxHT parentFtxH  /* in */
               )
{
    bsMPgT *bmt;
    int delFlag;
    int derefPageFlag = 0;
    domainT *domain;
    ftxHT ftxH;
    int failFtxFlag = 0;
    bsMCT *mcell;
    bsBfAttrT *newAttrRec;
    bfMCIdT newMcellId;
    bsXtntRT *newXtntRec;
    bsBfAttrT *origAttrRec;
    bsXtntRT *origXtntRec;
    ftxHT parentFtx;
    rbfPgRefHT pgPin;
    bfPageRefHT pgRef;
    statusT sts;
    tagInfoT tagInfo;
    vdT *vd;
    bsInMemXtntMapT *xtntMapp = NULL;
    vdIndexT localVdIndex;

    domain = bfap->dmnP;

    /*
     * Get the original's attribute record and primary extent record.
     * We assume that the records are in the primary mcell.
     */

    vd = VD_HTOP(bfap->primVdIndex, domain);
    sts = bmt_refpg( &pgRef,
                     (void*)&bmt,
                     vd->bmtp,
                     bfap->primMCId.page,
                     BS_NIL);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    derefPageFlag = 1;
    mcell = &(bmt->bsMCA[bfap->primMCId.cell]);

    origAttrRec = (bsBfAttrT *) bmtr_find (mcell, BSR_ATTR, vd->dmnP);
    origXtntRec = (bsXtntRT *) bmtr_find (mcell, BSR_XTNTS, vd->dmnP);

    /*
     * If this is a file that supports extent information in the primary
     * mcell, we assume that all extent information in the primary mcell
     * has already been migrated.  If there is extent information in the
     * primary mcell, we can't blindly copy it to a new mcell.
     */
    localVdIndex = newVdIndex;

    if (FIRST_XTNT_IN_PRIM_MCELL(domain->dmnVersion, bfap->xtnts.type)) {
        if (origXtntRec->firstXtnt.xCnt > 1) {
            /*
             * If the mcell is on the volume being removed, raise an
             * exception.
             */
            if (vd->serviceClass == nilServiceClass) {
                RAISE_EXCEPTION(E_STG_ADDED_DURING_MIGRATE_OPER);
            }

            localVdIndex = bfap->primVdIndex;
        }
    }

    /*
     * Copy the original attribute and primary extent records
     * to the new.  They are not copied by bmtr_clone_recs().
     *
     * If a record is missing, we still want to copy this mcell and
     * mcells linked to it.
     */

    sts = FTX_START_N(FTA_NULL, &ftxH, parentFtxH, bfap->dmnP, 1);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    sts = bmt_alloc_mcell (
                           bfap,
                           localVdIndex,
                           BMT_NORMAL_MCELL,
                           mcell->bfSetTag,
                           mcell->tag,
                           mcell->linkSegment,
                           ftxH,
                           &newMcellId,
                           FALSE
                           );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ((origAttrRec != NULL) || (origXtntRec != NULL)) {

        vd = VD_HTOP(localVdIndex, domain);
        sts = rbf_pinpg (
                         &pgPin,
                         (void *)&bmt,
                         vd->bmtp,
                         newMcellId.page,
                         BS_NIL,
                         ftxH
                         );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        mcell = &(bmt->bsMCA[newMcellId.cell]);

        if (origAttrRec != NULL) {
            newAttrRec = bmtr_assign_rec( BSR_ATTR,
                                          sizeof(bsBfAttrT),
                                          mcell,
                                          pgPin);
            if (newAttrRec == NULL) {
                ADVFS_SAD0 ("move_metadata: new bmtr_assign_rec failed: BSR_ATTR");
            }
            rbf_pin_record (pgPin, newAttrRec, sizeof (bsBfAttrT));
            *newAttrRec = *origAttrRec;
        }

        if (origXtntRec != NULL) {
            newXtntRec = bmtr_assign_rec (
                                          BSR_XTNTS,
                                          sizeof (bsXtntRT),
                                          mcell,
                                          pgPin
                                          );
            if (newXtntRec == NULL) {
                ADVFS_SAD0 ("move_metadata: new bmtr_assign_rec failed: BSR_XTNTS");
            }
            rbf_pin_record (pgPin, newXtntRec, sizeof (bsXtntRT));
            *newXtntRec = *origXtntRec;
        }
    }

    failFtxFlag = 0;
    ftx_done_n (ftxH, FTA_BS_MIG_MOVE_METADATA_V1);

    derefPageFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * Determine if the bitfile is on the delete pending list.
     *
     * NOTE: The delete code sets the bitfile's on-disk state to BSRA_DELETING,
     * inserts the bitfile's primary mcell onto the delete pending list and
     * removes the tag from the tag directory.  Since these are metadata
     * updates, the bitfile cannot transition into or out of this state.
     * So, no additional synchronization with "bfState" is needed.
     */
    if (bfap->bfState != BSRA_DELETING) {
        /*
         * Point tag directory to the new primary mcell.
         */
        delFlag = 0;

        tagInfo.bfSetp = bfap->bfSetp;
        tagInfo.dmnP = bfap->dmnP;
        tagInfo.tag = bfap->tag;
        tagInfo.vdIndex = localVdIndex;
        tagInfo.bfMCId = newMcellId;
        tagInfo.ftxH = parentFtxH;

        sts = tagdir_reset_tagmap (&tagInfo);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    } else {
        /*
         * The bitfile is on the delete pending list.  Remove the primary
         * mcell from the previous disk's list and insert it onto the new
         * disk's list.
         */
        delFlag = 1;

        sts = del_remove_from_del_list (
                                        bfap->primMCId,
                                        VD_HTOP(bfap->primVdIndex, domain),
                                        1,  /* start sub-transaction */
                                        parentFtxH
                                        );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        sts = del_add_to_del_list (
                                   newMcellId,
                                   VD_HTOP(localVdIndex, domain),
                                   1,  /* start sub-transaction */
                                   parentFtxH
                                   );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }

    /*
     * Copy the remaining original records and deallocate the original mcells.
     */

    sts = bmtr_clone_recs (
                           bfap->primMCId,
                           bfap->primVdIndex,
                           newMcellId,
                           newVdIndex,
                           localVdIndex,
                           domain,
                           parentFtxH
                           );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    bmt_free_bf_mcells (
                        VD_HTOP(bfap->primVdIndex, domain),
                        bfap->primMCId,
                        parentFtxH,
                        FALSE       /* no del list remove */
                        );

    if ((cloneBfap != NULL) && (delFlag == 0)) {
        /*
         * The bitfile has a clone and the bitfile was not on the
         * delete pending list.
         */
        /*
         * The clone does not have its own metadata.  Point the
         * clone's tag directory and access structure to the
         * original's new primary mcell.
         */
        tagInfo.bfSetp = cloneBfap->bfSetp;
        tagInfo.dmnP = cloneBfap->dmnP;
        tagInfo.tag = cloneBfap->tag;
        tagInfo.vdIndex = localVdIndex;
        tagInfo.bfMCId = newMcellId;
        tagInfo.ftxH = parentFtxH;
        sts = tagdir_reset_tagmap (&tagInfo);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        };

        cloneBfap->primVdIndex = localVdIndex;
        cloneBfap->primMCId = newMcellId;
    }

    /*
     * If we just changed primary mcells for a file that support extent
     * information in the primary mcell, update the in-memory extent
     * map if needed.  Generally, this won't be necessary as rmvol will
     * migrate extent information first but if the primary mcell has no
     * extent information in it, rmvol will not move it during the
     * migration of the other extents and we must update it here.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(domain->dmnVersion, bfap->xtnts.type)) {
        xtntMapp = bfap->xtnts.xtntMap;
        XTNMAP_LOCK_WRITE(&(bfap->xtntMap_lk))
        if ((localVdIndex != xtntMapp->hdrVdIndex) ||
            (newMcellId.page != xtntMapp->hdrMcellId.page) ||
            (newMcellId.cell != xtntMapp->hdrMcellId.cell)) {
            MS_SMP_ASSERT(xtntMapp->hdrVdIndex == 
                          xtntMapp->subXtntMap[0].vdIndex);
            MS_SMP_ASSERT(xtntMapp->hdrMcellId.page == 
                          xtntMapp->subXtntMap[0].mcellId.page);
            MS_SMP_ASSERT(xtntMapp->hdrMcellId.cell == 
                          xtntMapp->subXtntMap[0].mcellId.cell);
            xtntMapp->hdrVdIndex = 
            xtntMapp->subXtntMap[0].vdIndex = localVdIndex;
            xtntMapp->hdrMcellId.page = 
            xtntMapp->subXtntMap[0].mcellId.page = newMcellId.page;
            xtntMapp->hdrMcellId.cell = 
            xtntMapp->subXtntMap[0].mcellId.cell = newMcellId.cell;
            XTNMAP_UNLOCK(&(bfap->xtntMap_lk))

            /* 
             * Do the same for the clone, if necessary.
             */
            if ((cloneBfap != NULL) && (delFlag == 0)) {
                xtntMapp = cloneBfap->xtnts.xtntMap;
                XTNMAP_LOCK_WRITE(&(cloneBfap->xtntMap_lk))
                MS_SMP_ASSERT(xtntMapp->hdrVdIndex == 
                              xtntMapp->subXtntMap[0].vdIndex);
                MS_SMP_ASSERT(xtntMapp->hdrMcellId.page == 
                              xtntMapp->subXtntMap[0].mcellId.page);
                MS_SMP_ASSERT(xtntMapp->hdrMcellId.cell == 
                              xtntMapp->subXtntMap[0].mcellId.cell);
                xtntMapp->hdrVdIndex = 
                xtntMapp->subXtntMap[0].vdIndex = localVdIndex;
                xtntMapp->hdrMcellId.page = 
                xtntMapp->subXtntMap[0].mcellId.page = newMcellId.page;
                xtntMapp->hdrMcellId.cell = 
                xtntMapp->subXtntMap[0].mcellId.cell = newMcellId.cell;
                XTNMAP_UNLOCK(&(cloneBfap->xtntMap_lk))
            }
        } else {
            XTNMAP_UNLOCK(&(bfap->xtntMap_lk))
        }
    }

    bfap->primVdIndex = localVdIndex;
    bfap->primMCId = newMcellId;

    return EOK;

HANDLE_EXCEPTION:

    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
    }
    if (derefPageFlag != 0) {
        (void) bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* move_metadata */


/*
 * reset_block_map
 *
 * This function is called at the end of a file migration when the
 * original extent map has been decoupled from the access structure.
 * 
 * This function scans the buffer cache.  For each page of the specified
 * bitfile found in the cache, it sets the page's block map so that the
 * page is written to the disk locations specified by the bitfile's extent
 * maps.  This effectively changes buffer mappings from pointing at both
 * the original extent map and the new destination extent map to pointing
 * to just the new destination extent map.
 */

static
statusT
reset_block_map (
                 bfAccessT *bfap,  /* in */
                 uint32T bfPageOffset,  /* in */
                 uint32T bfPageCnt  /* in */
                 )
{
    blkDescT blkDesc[BLKDESC_CNT];
    blkMapT blkMap;
    int clearIOTransFlag = 0;
    int derefFlag = 0;
    uint32T i;
    void *page;
    bfPageRefHT pgPin;
    statusT sts;
    int unpinFlag = 0;
    bsUnpinModeT unpinMode;

    blkMap.maxCnt = BLKDESC_CNT;
    blkMap.blkDesc = &(blkDesc[0]);

    /*
     * For each page that is resident in memory, reset its block map
     * to point to just the destination of the migration.
     */
    for (i = bfPageOffset; i < (bfPageOffset + bfPageCnt); i++) {

        if (bs_find_page (bfap, i, 0) != 0) {
            /*
             * Pin the page.  This prevents the page from being written while
             * we modify the buffer's block map.
             *
             * Tell pinpg to Set the FLUSH state bit. This synchronizes
             * this function's page pin with bitfile flush and log trimming.
             * Flag must be explicitly cleared prior to unpinning the page.
             *
             * NOTE: If the page is pinned, the buffer isn't on any queue.
             * It gets put onto a queue at the last unpin.
             * Make sure to clear the buffer's FLUSH flag before unpin.
             */
            sts = bs_pinpg_int (&pgPin, &page, bfap, i, BS_NIL,
                                PINPG_SET_FLUSH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            unpinFlag = 1;

            sts = x_page_to_blkmap (bfap, i, &blkMap);
            if (sts != EOK && sts != W_NOT_WRIT) {
                RAISE_EXCEPTION (sts);
            }

            sts = bs_set_bufstate (pgPin, __LINE__, IO_TRANS, TRUE);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            clearIOTransFlag = 1;

            sts = buf_remap (pgPin, &blkMap);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            clearIOTransFlag = 0;
            sts = bs_clear_bufstate (pgPin, IO_TRANS | REMAP | FLUSH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            unpinFlag = 0;
            unpinMode.cacheHint = BS_RECYCLE_IT;
            unpinMode.rlsMode = BS_NOMOD;
            sts = bs_unpinpg (pgPin, logNilRecord, unpinMode);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
    }  /* end for */
    return EOK;

HANDLE_EXCEPTION:

    if (clearIOTransFlag != 0) {
        bs_clear_bufstate (pgPin, IO_TRANS);
    }
    if (unpinFlag != 0) {
        bs_clear_bufstate (pgPin, FLUSH);
        unpinMode.cacheHint = BS_RECYCLE_IT;
        unpinMode.rlsMode = BS_NOMOD;
        bs_unpinpg (pgPin, logNilRecord, unpinMode);
    }
    return sts;

}  /* end reset_block_map */

static
statusT
reset_block_map_special (
                         bfAccessT *cloneap, /* in - special case remapping */
                         uint32T bfPageCnt   /* in */
                         )
{
    blkDescT blkDesc[BLKDESC_CNT];
    blkMapT blkMap;
    int derefFlag = 0;
    uint32T i;
    void *page;
    bfPageRefHT pgRef;
    statusT sts;

    blkMap.maxCnt = BLKDESC_CNT;
    blkMap.blkDesc = &(blkDesc[0]);

    /*
     * For each page that is resident in memory, reset its block map
     * to point to just the destination of the migration.
     */
    for (i = 0; i < bfPageCnt; i++) {

        if ( (!page_is_mapped(cloneap, i, NULL, TRUE)) &&
             ( page_is_mapped(cloneap->origAccp, i, NULL, TRUE)) &&
             (bs_find_page(cloneap, i, 0) != 0) ) {

            /*
             * Ref the page.  This returns a buffer handle.
             */
            sts = bs_refpg (&pgRef, &page, cloneap, i, BS_NIL);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            derefFlag = 1;

            /*
             * note that we use cloneap->origAccp rather than
             * cloneap to get correct blkmap.
             */
            sts = x_page_to_blkmap (cloneap->origAccp, i, &blkMap);
            if (sts != EOK && sts != W_NOT_WRIT) {
                RAISE_EXCEPTION (sts);
            }

            sts = buf_remap (pgRef, &blkMap);

            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            derefFlag = 0;
            sts = bs_derefpg (pgRef, BS_RECYCLE_IT);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
    }  /* end for */

    return EOK;

HANDLE_EXCEPTION:

    if (derefFlag != 0) {
        bs_derefpg (pgRef, BS_RECYCLE_IT);
    }

    return sts;

}  /* end reset_block_map_special */

/*
 * alloc_copy_stg
 *
 * This function allocates storage for the copy.  The copy's extent map is
 * created and initialized and the storage info is inserted onto the delete
 * pending list.
 */

static
statusT
alloc_copy_stg (
                bfAccessT *bfap,               /* in   get stg for this file */
                uint32T startPage,             /* in   start map at this page */
                pageRangeT *xmPageRange,       /* in   array of extents */
                uint32T xmPageRangeCnt,        /* in   array size */
                vdIndexT vdIndex,              /* in   get stg on this volume */
                uint64T dstBlkOffset, /* in;if hint == BS_ALLOC_MIG_RSVD,make sure we get this blk*/
                bsAllocHintT alloc_hint,       /* in */
                vdIndexT *copyVdIndex,         /* out  stg starts on this vol */
                bfMCIdT *copyMcellId,          /* out  starting mcell */
                bsInMemXtntMapT **copyXtntMap, /* out  return this map */
                uint32T *copyXferSize          /* out  migrate transfer size */
                )
{
    uint32T allocPageCnt;
    uint32T i;
    statusT sts;
    bsInMemXtntMapT *xtntMap = NULL;
    vdT *vdp = NULL;

    MS_SMP_ASSERT(startPage <= xmPageRange[0].pageOffset);

    sts = imm_create_xtnt_map( bfap->bfPageSz,
                               bfap->dmnP,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &xtntMap);
    if ( sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }

    if ( vdIndex == (vdIndexT)(-1) ) {
        for ( i = 0; i < xmPageRangeCnt; i++ ) {
            sts = cp_stg_alloc_from_svc_class( bfap,
                                               xmPageRange[i].pageOffset,
                                               xmPageRange[i].pageCnt,
                                               xmPageRange[i].pageType,
                                               xtntMap,
                                               startPage,
                                               alloc_hint,
                                               copyXferSize
                                               );
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
    } else {
        vdp = vd_htop_already_valid(vdIndex, bfap->dmnP, TRUE);
        *copyXferSize = vdp->wrmaxio;
        for (i = 0; i < xmPageRangeCnt; i++) {
            uint32T fileClust;

            if (vdp->stgCluster == BS_CLUSTSIZE) {
                fileClust = (xmPageRange[i].pageCnt * bfap->bfPageSz) /
                                   BS_CLUSTSIZE;
            } else {
                MS_SMP_ASSERT(vdp->stgCluster == BS_CLUSTSIZE_V3);
                fileClust = (xmPageRange[i].pageCnt * bfap->bfPageSz) /
                                   BS_CLUSTSIZE_V3;
            }

            SC_TBL_LOCK(bfap->dmnP);
            vdp->allocClust += fileClust;
            SC_TBL_UNLOCK(bfap->dmnP);

            sts = cp_stg_alloc_from_one_disk( bfap,
                                              vdIndex,
                                              xmPageRange[i].pageOffset,
                                              xmPageRange[i].pageCnt,
                                              xmPageRange[i].pageType,
                                              xtntMap,
                                              startPage,
                                              &allocPageCnt,
                                              dstBlkOffset,
                                              alloc_hint);

            SC_TBL_LOCK(bfap->dmnP);
            vdp->allocClust -= fileClust;
            SC_TBL_UNLOCK(bfap->dmnP);

            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            if (allocPageCnt != xmPageRange[i].pageCnt) {
                RAISE_EXCEPTION (ENO_MORE_BLKS);
            }
        }
        vd_dec_refcnt(vdp);
    }

    *copyVdIndex = xtntMap->hdrVdIndex;
    *copyMcellId = xtntMap->hdrMcellId;
    *copyXtntMap = xtntMap;

    return EOK;

HANDLE_EXCEPTION:
    if ( vdp != NULL ) {
        vd_dec_refcnt(vdp);
    }
    /* hdrVdIndex remains 0 until stg is alloced. */
    if ( xtntMap->hdrVdIndex ) {
        vdp = VD_HTOP(xtntMap->hdrVdIndex, bfap->dmnP);
        (void)del_dealloc_stg(xtntMap->hdrMcellId, vdp);
    }

    if ( xtntMap != NULL ) {
        imm_delete_xtnt_map( xtntMap);
    }

    return sts;
}


static
statusT
alloc_hole_stg (
                bfAccessT *bfap,  /* in */
                pageRangeT *xmHoleRange,  /* in */
                vdIndexT vdIndex,  /* in */
                vdIndexT *copyVdIndex,  /* out */
                bfMCIdT *copyMcellId,  /* out */
                bsInMemXtntMapT **copyXtntMap  /* out */
                )
{
    uint32T allocPageCnt;
    bfTagT bfSetTag;
    bfMCIdT delMcellId;
    vdIndexT delVdIndex;
    ftxHT ftxH;
    int failFtxFlag = 0;
    uint32T i;
    bfMCIdT newMcellId;
    uint32T pageCnt;
    statusT sts;
    bsInMemXtntMapT *xtntMap = NULL;
    uint32T newtype,
            newmax;
    int vdRefFlg = FALSE;
    vdT *vdp;

    bfSetTag = bfap->bfSetp->dirTag;

    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    sts = imm_create_xtnt_map (
                               bfap->bfPageSz,
                               bfap->dmnP,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &xtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ( vdIndex == (vdIndexT)(-1) ) {
        /* We need an MCELL on a disk in the service class
         * so pick one */

        sts = sc_select_vd_for_mcell( &vdp,
                                      bfap->dmnP,
                                      bfap->dmnP->scTbl,
                                      bfap->reqServices,
                                      bfap->optServices);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        vdRefFlg = TRUE;
        vdIndex = vdp->vdIndex;
    }
 
    sts = stg_alloc_new_mcell (
                               bfap,
                               bfSetTag,
                               bfap->tag,
                               vdIndex,
                               ftxH,
                               &newMcellId,
                               TRUE
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * If this file can have extent information in the primary mcell,
     * make the first subextent map type BSR_XTRA_XTNTS.  Otherwise, make
     * it BSR_SHADOW_XTNTS.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(xtntMap->domain->dmnVersion,
                                 bfap->xtnts.type)) {
        newtype = BSR_XTRA_XTNTS;
        newmax = BMT_XTRA_XTNTS;
    }
    else {
        newtype = BSR_SHADOW_XTNTS;
        newmax = BMT_SHADOW_XTNTS;
    }
    sts = imm_init_sub_xtnt_map (
                                 &(xtntMap->subXtntMap[0]),
                                 xmHoleRange->pageOffset,
                                 xmHoleRange->pageCnt,
                                 vdIndex,
                                 newMcellId,
                                 newtype,
                                 newmax,
                                 0  /* maxCnt - take default */
                                 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    xtntMap->cnt++;
    xtntMap->validCnt = xtntMap->cnt;

    /* 
     * Fix up first extent to be the correct type of hole 
     */

    xtntMap->subXtntMap[0].bsXA[0].vdBlk = xmHoleRange->pageType;

    xtntMap->subXtntMap[0].bsXA[1].bsPage = xmHoleRange->pageCnt + 
                                            xmHoleRange->pageOffset;

    xtntMap->subXtntMap[0].bsXA[1].vdBlk = XTNT_TERM;
    xtntMap->subXtntMap[0].cnt++;

    xtntMap->updateStart = xtntMap->validCnt;

    /*
     * Save to disk the storage allocation information.
     */

    for (i = 0; i < xtntMap->cnt; i++) {
        sts = odm_create_xtnt_rec (
                                   bfap,
                                   -1,  /* allocVdIndex */
                                   &(xtntMap->subXtntMap[i]),
                                   0,  /* no clone xfer xtnts here */
                                   ftxH
                                   );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }  /* end for */

    if ( (xtntMap->cnt > 1) &&
         xtntMap->subXtntMap[0].type == BSR_SHADOW_XTNTS ) {
        update_mcell_cnt(bfap->dmnP, 
                         bfap->tag,
                         xtntMap->subXtntMap[0].vdIndex, 
                         xtntMap->subXtntMap[0].mcellId, 
                         xtntMap->subXtntMap[0].type,
                         xtntMap->cnt - 1, 
                         ftxH);
    }

    pageCnt = (xtntMap->subXtntMap[xtntMap->cnt - 1].pageOffset +
               xtntMap->subXtntMap[xtntMap->cnt - 1].pageCnt) -
                 xtntMap->subXtntMap[0].pageOffset;
    sts = odm_create_xtnt_map (
                               bfap,
                               bfap->bfSetp,
                               bfap->tag,
                               xtntMap,
                               ftxH,
                               &delVdIndex,
                               &delMcellId
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = del_add_to_del_list (
                               delMcellId,
                               VD_HTOP(delVdIndex, bfap->dmnP),
                               TRUE,  /* start sub ftx */
                               ftxH
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ( vdRefFlg ) {
        vd_dec_refcnt(vdp);
    }
    failFtxFlag = 0;
    ftx_done_u (ftxH, FTA_BS_MIG_MIGRATE_V1, 0, 0);

    *copyVdIndex = delVdIndex;
    *copyMcellId = delMcellId;
    *copyXtntMap = xtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if ( vdRefFlg ) {
        vd_dec_refcnt(vdp);
    }
    if (failFtxFlag != 0) {
        ftx_fail (ftxH);
    }
    if (xtntMap != NULL) {
        imm_delete_xtnt_map (xtntMap);
    }
    return sts;

}  /* end alloc_hole_stg */


/*
 * mig_get_stripe_bfpage_list
 * Starting from the page offset found in pXtntp, and ending at the
 * offset in pXtntp, convert the extent's xm page range to a list of bf
 * page descriptions that can be migrated by the caller. Store each
 * description in an array to be free'd by caller.  Any non-EOK return
 * value, then the array has already been freed by this routine!
 */

static statusT
mig_get_stripe_bfpage_list (
                        vdT *vdp,   /* in */
                        bfAccessT *bfap,   /* in */
                        ssPackLLT *pXtntp, /* in */
                        pageRangeT **bfPageRange,  /* out */
                        uint32T *bfPageRangeCnt   /* out */
                        )
{
    bsStripeHdrT *stripeHdrP = bfap->xtnts.stripeXtntMap;
    uint segSz = stripeHdrP->segmentSize;
    uint segCnt = stripeHdrP->cnt;
    uint32T xtntPg = pXtntp->ssPackPageOffset;
    uint32T blkRangeStart = pXtntp->ssPackStartXtBlock;
    uint32T blkRangeEnd = pXtntp->ssPackEndXtBlock;
    uint mapIndex;
    bsInMemXtntMapT *xmP;
    bsInMemSubXtntMapT *sxmP;
    uint xI;
    uint sI;
    pageRangeT *pageRange = NULL;
    uint32T pg;
    uint32T bfPage;
    uint32T prevPage;
    uint32T blk;
    uint cnt;
    uint maxCnt;
    statusT sts;

    *bfPageRangeCnt = 0;

    for ( mapIndex = 0; mapIndex < stripeHdrP->cnt; mapIndex++ ) {
        /* for each stripe extent map */
        xmP = stripeHdrP->xtntMap[mapIndex];

        sts = imm_page_to_sub_xtnt_map( xtntPg, xmP, FALSE, &sI );
        if ( sts == E_PAGE_NOT_MAPPED ) {
            continue;  /* try the next stripe */
        }

        sxmP = &xmP->subXtntMap[sI];
        if ( sxmP->vdIndex != vdp->vdIndex ) {
            continue;  /* try the next stripe */
        }

        sts = imm_page_to_xtnt( xtntPg, sxmP, FALSE, FALSE, &xI );
        if ( sts != EOK ) {
            continue;  /* try the next stripe */
        }

        pg = sxmP->bsXA[xI].bsPage;
        blk = sxmP->bsXA[xI].vdBlk + (xtntPg - pg) * ADVFS_PGSZ_IN_BLKS;
        if ( blk != blkRangeStart ) {
            continue;  /* try the next stripe */
        }

        /* found the extent!, update pXtntp */
        reload_pXtntp( xmP, pXtntp );
        if ( pXtntp->ssPackPageCnt == 0 ) {
            return E_PAGE_NOT_MAPPED;
        }

        xtntPg = pXtntp->ssPackPageOffset;
        maxCnt = 1;
        pageRange = (pageRangeT *)ms_malloc( sizeof(pageRangeT) * maxCnt );
        MS_SMP_ASSERT(pageRange != NULL);  /* ms_malloc waits */

        bfPage = XMPAGE_TO_BFPAGE ( mapIndex, xtntPg, segCnt, segSz );

        cnt = 0;
        pageRange[cnt].pageOffset = bfPage;
        pageRange[cnt].pageCnt = 1;
        prevPage = bfPage;

        for( pg = 1;  pg < pXtntp->ssPackPageCnt;  pg++ ) {

            bfPage = XMPAGE_TO_BFPAGE( mapIndex, xtntPg + pg, segCnt, segSz );

            if ( bfPage == prevPage+1 ) {
                /* still on same segment, continue on */
                prevPage = bfPage;
                pageRange[cnt].pageCnt++;
                continue;
            } else {
                /* start another descriptor */
                cnt++;
                if ( cnt == maxCnt ) {
                    sts = extend_page_range_list( &maxCnt, &pageRange );
                    MS_SMP_ASSERT(sts == EOK);  /* ms_malloc waits */
                }
                pageRange[cnt].pageOffset = bfPage;
                pageRange[cnt].pageCnt = 1;
                prevPage = bfPage;
            }
        } /* end for pg in pXtnt */

        *bfPageRange = pageRange;
        *bfPageRangeCnt = cnt + 1;

        return EOK;
    } /* end for mapIndex */

    if ( pageRange != NULL ) {
        ms_free (pageRange);
    }

    return E_PAGE_NOT_MAPPED;
}

/*
 * mig_pack_vd_range
 * First determine the extents in the clear range.  Next,
 * pack the extents in the input range to the far left of the range.
 * This clears a contiguous range of free pages producing a single extent
 * large enough to hold the defragmented file extent.
 *
 * Suggestion: mig_pack_vd_range() has two sizable, near-identical blocks
 * of code which preempt the thread (reset counters, check the DDL, ...)
 * on exceeding a vfast threshold.  These blocks could be rewritten as a
 * function with two callers.  The gain would be decreased code
 * duplication, and increased readability and maintainability.
 */

statusT
mig_pack_vd_range (
             vdT *vdp,            /* in */
             uint32T  cRangeBeginBlk,     /* in */
             uint64T  cRangeEndBlk,       /* in */
             uint32T  *newcRangeBeginBlk,     /* out */
             uint32T  forceFlag     /* in */
             )
{
    statusT sts=EOK,sts2=EOK;
    domainT *domain = NULL;
    bsInMemXtntMapT *inwayXtntMap = NULL;
    bsInMemSubXtntMapT *subXtntMap = NULL;
    int blksPerPage = ADVFS_PGSZ_IN_BLKS;
    int closeFileFlag = FALSE, skip;
    uint32T sbm_bit_cnt = 0;
    uint32T bmt_bit_cnt = 0;
    uint32T i,k;
    bfSetT *inwayBfSetp = NULL;
    bfSetIdT inwayBfSetId;
    bfAccessT *inwayBfap = NULL;
    uint64T nextcRangeBeginBlk,
            xmPageOffset,
            migPageCntSoFar,
            migPageCnt,
            thdblkMigPageCnt=0,
            dstBlkOffset=0,
            pgsRemaining,
            newBlkOffset,
            bfPageOffset,
            newPageCnt;
    uint32T clearPageCnt;
    ssPackHdrT   *php=NULL;
    ssPackLLT *pXtntp=NULL;
    int another_scan_required;
    extern ss_open_file();
    extern ss_close_file();
    extern ss_block_and_wait();
    extern print_pack_list();
    extern REPLICATED int SSPageCnt;
    struct mount *mp;
    pageRangeT *bfPageRange = NULL;
    uint32T bfPageRangeCnt = 0, mapIndex;
    bfMCIdT mcellId;
    int delFlag;
    int fsetMounted;

    MS_SMP_ASSERT(vdp);
    *newcRangeBeginBlk = 0;
    nextcRangeBeginBlk = dstBlkOffset = cRangeBeginBlk;

    /* optimizer to avoid calling costly bmt scan if there is already
     * nothing in way
     */
    sts = sbm_scan(vdp, cRangeBeginBlk, cRangeEndBlk, &sbm_bit_cnt);
    if (sts != EOK) {
        return sts;
    }
    SS_TRACE(vdp,cRangeBeginBlk, cRangeEndBlk, sbm_bit_cnt, 0);

    if (sbm_bit_cnt) {
        /* some pages are already allocated in our chosen range. */
        domain = vdp->dmnP;
        inwayBfSetId.domainId = domain->domainId;
        nextcRangeBeginBlk = dstBlkOffset = cRangeBeginBlk;
        another_scan_required = TRUE;

        /* loop, repeatedly scanning the bmt and rbmt, finding 
         * those extents that are lying in the clear range passed in.
         */
        while(another_scan_required) {

            /* V5 ONDISK FIX -> call sbm_get_vd_bf_inway for new v5 sbm instead
             * of bmt_get_vd_bf_inway
             */

            /* find tags and pages for all files in range to be packed */
            bmt_bit_cnt=0;
            sts = bmt_get_vd_bf_inway( vdp,     /* puts list on here */
                                   nextcRangeBeginBlk,  /* new scan start */
                                   cRangeEndBlk,    /* scan end */
                                   &another_scan_required,  /* TRUE, FALSE */
                                   &bmt_bit_cnt /* used for clear range verify */
                                 );
            if (sts != EOK) {
                goto _cleanup;
            }

            /* loop through the files with extents in the range we
             * want to pack and migrate the pages in sequence from
             * right to left in the area to be cleared.
             */

            php = &vdp->ssVolInfo.ssPackHdr;
            pXtntp = php->ssPackFwd;
            if ((vdp->ssVolInfo.ssPackHdr.ssPackListCnt > 0) &&
               (another_scan_required)) {
                /* pick up next loop where the list leaves off this loop */
                nextcRangeBeginBlk = php->ssPackBwd->ssPackEndXtBlock;
            }

            while(( php->ssPackListCnt != 0) &&
                  ( pXtntp != (ssPackLLT *)php )) {

                sts = ss_open_file( pXtntp->ssPackBfSetId,
                                    pXtntp->ssPackTag,
                                    &inwayBfSetp,
                                    &inwayBfap,
                                    &fsetMounted);
                if (sts == ENO_SUCH_TAG) {
                    /* File was deleted, or being deleted.
                     * Find out which one.
                     */
                    DDLACTIVE_LOCK_WRITE( &(vdp->ddlActiveLk) )
                    sts = del_find_del_entry ( vdp->dmnP,
                                               vdp->vdIndex,
                                               pXtntp->ssPackBfSetId.dirTag,
                                               pXtntp->ssPackTag,
                                               &mcellId,
                                               &delFlag);
                    DDLACTIVE_UNLOCK( &(vdp->ddlActiveLk) )

                    if (sts == EOK) {
                        /* found on ddl, may be there a long time, abort */
                        sts = ENO_MORE_MCELLS;
                        goto _cleanup;
                    } else if (sts == ENO_MORE_MCELLS) {
                        /* File was deleted, ok */
                        sts = EOK;
                        pXtntp = pXtntp->ssPackFwd;  /* Keep going! */
                        continue;
                    } else
                        goto _cleanup;
                }
                if (sts != EOK) {
                    goto _cleanup;
                }
                closeFileFlag = TRUE;

                /*
                 * load the valid in-memory extent maps so they are recent.
                 */
                sts = x_load_inmem_xtnt_map( inwayBfap, X_LOAD_REFERENCE);
                if (sts != EOK) {
                    goto _cleanup;
                }
                MS_SMP_ASSERT(pXtntp->ssPackStartXtBlock >= cRangeBeginBlk);
                MS_SMP_ASSERT(pXtntp->ssPackEndXtBlock <= cRangeEndBlk);
                MS_SMP_ASSERT(pXtntp->ssPackStartXtBlock <= 
                              pXtntp->ssPackEndXtBlock);
                MS_SMP_ASSERT(pXtntp->ssPackPageCnt <=
                       (cRangeEndBlk - cRangeBeginBlk) / ADVFS_PGSZ_IN_BLKS);
                switch (inwayBfap->xtnts.type) {
                  case BSXMT_APPEND:
                      reload_pXtntp( inwayBfap->xtnts.xtntMap, pXtntp );
                      XTNMAP_UNLOCK( &(inwayBfap->xtntMap_lk) )
                      if ( pXtntp->ssPackPageCnt == 0 ) {
                          break;  /* out of switch, get next pXtnt */
                      }
                      migPageCntSoFar = migPageCnt = 0;
                      xmPageOffset = pXtntp->ssPackPageOffset;
                      inwayXtntMap = inwayBfap->xtnts.xtntMap;
                      newPageCnt = pXtntp->ssPackPageCnt;

                      while (migPageCntSoFar < newPageCnt) {

                          if (thdblkMigPageCnt > SSPageCnt) {
                              /* Close the fileset and bfap before parking so
                               * that the fileset can be deleted if needed.
                               * We don't want to hold up the delete while
                               * waiting for some idle time to run.
                               */

                              sts = ss_close_file( inwayBfSetp,
                                                   inwayBfap,
                                                   fsetMounted );
                              if (sts != EOK) {
                                  goto _cleanup;
                              }
                              closeFileFlag = FALSE;

                              /* wait for next run approval to continue */
                              sts = ss_block_and_wait(vdp);
                              if (sts != EOK) {
                                  goto _cleanup;
                              }
                              /* reset page counter */
                              thdblkMigPageCnt = 0;
                              sts = ss_open_file( pXtntp->ssPackBfSetId,
                                                  pXtntp->ssPackTag,
                                                  &inwayBfSetp,
                                                  &inwayBfap,
                                                  &fsetMounted);
                              if (sts == ENO_SUCH_TAG) {
                                  /* File was deleted, or being deleted.
                                   * Find out which one.
                                   */
                                  DDLACTIVE_LOCK_WRITE( &(vdp->ddlActiveLk) )
                                  sts = del_find_del_entry (
                                                     vdp->dmnP,
                                                     vdp->vdIndex,
                                                     pXtntp->ssPackBfSetId.dirTag,
                                                     pXtntp->ssPackTag,
                                                     &mcellId,
                                                     &delFlag);
                                  DDLACTIVE_UNLOCK( &(vdp->ddlActiveLk) )

                                  if (sts == EOK) {
                                      /* on ddl, may be there a long time, abort */
                                      sts = ENO_MORE_MCELLS;
                                      goto _cleanup;
                                  } else if (sts == ENO_MORE_MCELLS) {
                                      /* File was deleted, ok */
                                      sts = EOK;
                                      break;  /* break to next list entry */
                                  } else
                                      goto _cleanup;
                              }
                              if (sts != EOK) {
                                  goto _cleanup;
                              }
                              closeFileFlag = TRUE;
                          }

                          pgsRemaining = pXtntp->ssPackPageCnt - migPageCntSoFar;
                          newBlkOffset = pXtntp->ssPackStartXtBlock +
                                         (migPageCntSoFar * blksPerPage);

                          /* number of pages allowed to be migrated are
                           * equal to the number of free blocks between
                           * the last destination blk offset and the
                           * start of this extent.  This is allowed
                           * because the extents are ordered by block
                           * number in the pack list and it is operated
                           * on in the same order - from left to right
                           */
                          if ((newBlkOffset - dstBlkOffset) > 0)
                              migPageCnt = (newBlkOffset - dstBlkOffset) /
                                                    blksPerPage;
                          else {
                              /* There is no room on left to pack this extent
                               * into. The sparse range must not have started
                               * on an unused page.  Abort because sparse range
                               * is probably defective due to a bad sbm_scan_v3_v4().
                               */
                              sts = E_RANGE_NOT_CLEARED;
                              goto _cleanup;
                          }
                          /* pack the number allowed from above or the
                           * number remaining
                           */
                          migPageCnt = MIN(migPageCnt, pgsRemaining) ;
                          /* pack only the number allowed by smartstore */
                          migPageCnt = MIN(migPageCnt, SSPageCnt);

                          SS_TRACE(vdp,inwayBfap->tag.num, xmPageOffset,
                                       migPageCnt, dstBlkOffset);
                          sts = migrate_normal (
                                            inwayBfap,
                                            xmPageOffset,
                                            migPageCnt,
                                            vdp->vdIndex,
                                            dstBlkOffset,
                                            forceFlag,
                                            BS_ALLOC_MIG_RSVD /* override lock */
                                            );

                          if (sts != EOK)  {
                              /* ENO_MORE_BLKS indicates the reserved
                               *               space was stolen by a needy
                               *               user application file. -abort
                               * E_PAGE_NOT_MAPPED indicates that part or
                               *               all of the file
                               *               range being migrated was
                               *               truncated before we could
                               *               migrate it. - abort.
                               * ENO_SUCH_TAG - will not occur here because we
                               *                own the access structure.
                               */
                              goto _cleanup;
                          }

                          /* pages migrated since last block. */
                          thdblkMigPageCnt += migPageCnt;
                          /* new page offset */
                          xmPageOffset += migPageCnt;

                          /* pages migrated so far for xtnt entry*/
                          migPageCntSoFar += migPageCnt;

                          /* new blk offset */
                          dstBlkOffset +=  migPageCnt * blksPerPage;

                          /* update the stats before parking or exiting */
                          vdp->dmnP->ssDmnInfo.ssPagesConsol += migPageCnt;

                      } /* end while pages to migrate */

                      break;

                   case BSXMT_STRIPE:

                      sts = mig_get_stripe_bfpage_list (vdp,
                                                        inwayBfap,
                                                        pXtntp,
                                                        &(bfPageRange),
                                                        &bfPageRangeCnt);

                      XTNMAP_UNLOCK( &(inwayBfap->xtntMap_lk) )        

                      if (sts != EOK)  {
                        break;  /* out of switch, get next pXtnt */
                      }

                      for(k=0,skip=FALSE; k<bfPageRangeCnt && !skip; k++) {

                          migPageCntSoFar = migPageCnt = 0;
                          bfPageOffset = bfPageRange[k].pageOffset;
                          newPageCnt = bfPageRange[k].pageCnt;

                          /* Now, migrate the pages to the left, 
                           * thus packing them  together.
                           */
                          while (migPageCntSoFar < newPageCnt) {

                              if (thdblkMigPageCnt > SSPageCnt) {
                                  /* Close the fileset and bfap before parking
                                   * so the fileset can be deleted if needed.
                                   * We don't want to hold up the delete while
                                   * waiting for some idle time to run.
                                   */

                                  sts = ss_close_file( inwayBfSetp,
                                                       inwayBfap,
                                                       fsetMounted );
                                  if (sts != EOK) {
                                      goto _cleanup;
                                  }
                                  closeFileFlag = FALSE;

                                  /* wait for next run approval to continue */
                                  sts = ss_block_and_wait(vdp);
                                  if (sts != EOK) {
                                      goto _cleanup;
                                  }
                                  /* reset page counter */
                                  thdblkMigPageCnt = 0;

                                  sts = ss_open_file( pXtntp->ssPackBfSetId,
                                                      pXtntp->ssPackTag,
                                                      &inwayBfSetp,
                                                      &inwayBfap,
                                                      &fsetMounted);
                                  if (sts == ENO_SUCH_TAG) {
                                      /* File was deleted, or being deleted.
                                       * Find out which one.
                                       */
                                      DDLACTIVE_LOCK_WRITE(&(vdp->ddlActiveLk))
                                      sts = del_find_del_entry (
                                                         vdp->dmnP,
                                                         vdp->vdIndex,
                                                         pXtntp->ssPackBfSetId.dirTag,
                                                         pXtntp->ssPackTag,
                                                         &mcellId,
                                                         &delFlag);
                                      DDLACTIVE_UNLOCK(&(vdp->ddlActiveLk) )

                                      if (sts == EOK) {
                                          /* On ddl, May be there a long
                                           * time, abort.
                                           */
                                          sts = ENO_MORE_MCELLS;
                                          goto _cleanup;
                                      } else if (sts == ENO_MORE_MCELLS) {
                                          /* File was deleted, ok */
                                          sts = EOK;
                                          skip = TRUE;
                                          break;/* next list entry */
                                      } else
                                          goto _cleanup;
                                  }
                                  if (sts != EOK) {
                                      goto _cleanup;
                                  }
                                  closeFileFlag = TRUE;
                              }

                              pgsRemaining = newPageCnt - migPageCntSoFar;
                              newBlkOffset = pXtntp->ssPackStartXtBlock +
                                             (migPageCntSoFar * blksPerPage);

                              /* number of pages allowed to be migrated are
                               * equal to the number of free blocks between
                               * the last destination blk offset and the
                               * start of this extent.  This is allowed
                               * because the extents are ordered by block
                               * number in the pack list and it is operated
                               * on in the same order - from left to right
                               */
                              if ((newBlkOffset - dstBlkOffset) > 0)
                                  migPageCnt = (newBlkOffset - dstBlkOffset) /
                                                             blksPerPage;
                              else {
                                  /* There is no room on left to pack this
                                   * extent into.  The sparse range must not
                                   * have started on an unused page.  Abort
                                   * because sparse range is probably defective
                                   * due to a bad sbm_scan_v3_v4().
                                   */
                                   sts = E_RANGE_NOT_CLEARED;
                                   goto _cleanup;
                              }

                              /* Pack the number we have room for or
                               * or the number remaining.
                               */
                              migPageCnt = MIN(migPageCnt, pgsRemaining) ;
                              /* pack only the number allowed by smartstore */
                              migPageCnt = MIN(migPageCnt, SSPageCnt);

                              SS_TRACE(vdp,inwayBfap->tag.num, bfPageOffset,
                                           migPageCnt, dstBlkOffset);

                              sts = migrate_stripe (inwayBfap,
                                                 vdp->vdIndex,
                                                 bfPageOffset,
                                                 migPageCnt,
                                                 vdp->vdIndex,
                                                 dstBlkOffset,
                                                 forceFlag,
                                                 BS_ALLOC_MIG_RSVD
                                                 );

                              if (sts != EOK)  {
                                  /* ENO_MORE_BLKS indicates the reserved
                                   *               space was stolen by a needy
                                   *               user application file. -abort
                                   * E_PAGE_NOT_MAPPED indicates that part or
                                   *               all of the file
                                   *               range being migrated was
                                   *               truncated before we could
                                   *               migrate it. - abort.
                                   * ENO_SUCH_TAG - will not occur here because we
                                   *                own the access structure.
                                   */
                                  goto _cleanup;
                              }

                              /* pages migrated since last block */
                              thdblkMigPageCnt += migPageCnt;
                              bfPageOffset += migPageCnt;
                              migPageCntSoFar += migPageCnt;
                              dstBlkOffset +=  migPageCnt * blksPerPage;

                              /* update the stats before parking or exiting */
                              vdp->dmnP->ssDmnInfo.ssPagesConsol += migPageCnt;

                          } /* end while migrating the extent */
                      } /* end for each stripe segment in extent found inway. */

                      if (bfPageRange != NULL) {
                          ms_free (bfPageRange);
                          bfPageRange = NULL;
                      }

                      break;

                    default:
                          XTNMAP_UNLOCK( &(inwayBfap->xtntMap_lk) )
                          sts = ENOT_SUPPORTED;
                          goto _cleanup;

                }  /* end switch */

                if ( closeFileFlag ) {
                    sts = ss_close_file( inwayBfSetp, inwayBfap, fsetMounted );
                    if (sts != EOK) {
                        goto _cleanup;
                    }
                    closeFileFlag = FALSE;
                }

                pXtntp = pXtntp->ssPackFwd;  /* Keep going! */

            } /* end while list entries */

            ss_dealloc_pack_list(vdp);

        } /* end while more files to get */
    } /* end if bits */

#ifdef SS_DEBUG
    /* now re-check and see if all bits are free */
    sbm_bit_cnt = 0;
    sts2 = sbm_scan(vdp, dstBlkOffset, cRangeEndBlk, &sbm_bit_cnt);
    if (sts2 != EOK) {
        goto _cleanup;
    }

    if (sbm_bit_cnt) {
        if (vdp->freeMigRsvdStg.start_clust==0) {
            /* space used by system when out of space. - ok */
            ms_printf("mig_pack_vd_range:: sbm_scan 2 vdIndex(%d) start(%ld) \
end(%ld), inuse  sbm_bit_cnt==%d) sts==E_STOLEN_BY_SYSTEM\n",
            vdp->vdIndex,  dstBlkOffset, cRangeEndBlk, sbm_bit_cnt);
        } else {
            /* The bmt may have been updated after vfast scan in
             * bmt_get_vd_bf_inway. - ok
             */
            ms_printf("mig_pack_vd_range:: sbm_scan 2 vdIndex(%d) start(%ld) \
end(%ld), inuse  sbm_bit_cnt==%d) sts==E_STOLEN_BY_SYSTEM\n",
            vdp->vdIndex,  dstBlkOffset, cRangeEndBlk, sbm_bit_cnt);
        }
        sts = E_RANGE_NOT_CLEARED;
        goto _cleanup;
    }
#endif

    *newcRangeBeginBlk = dstBlkOffset;
    SS_TRACE(vdp,0,0,0,dstBlkOffset);

_cleanup:

    if (bfPageRange != NULL) {
        ms_free (bfPageRange);
        bfPageRange = NULL;
    }

    SS_TRACE(vdp,0,0,0,sts);
    ss_dealloc_pack_list(vdp);

    if (closeFileFlag == TRUE) {
        (void) ss_close_file( inwayBfSetp, inwayBfap, fsetMounted );
        closeFileFlag = FALSE;
    }
    return sts;

}  /* end mig_pack_vd_range */

/*
** Given the pack block range and the file page offset
** in pXtntp, find the overlap in the file extents that are now locked.
** If necessary, change the values in pXtntp.
** Special return in pXtntp is:
** ssPackPageCnt == 0 : the entire extent is gone from the range.
*/

static
void
reload_pXtntp ( 
             bsInMemXtntMapT *xmP,
             ssPackLLT *pXtntp
             )
{
    bsInMemSubXtntMapT *sxmP;
    statusT sts;
    uint32T xtntPageOff = pXtntp->ssPackPageOffset;
    uint32T rangeStartBlk = pXtntp->ssPackStartXtBlock;
    uint32T rangeEndBlk = pXtntp->ssPackEndXtBlock;
    uint sI;
    uint xI;
    uint32T p1;
    uint32T p2;
    uint32T xtntStartBlk;
    uint32T xtntEndBlk;
    uint32T blk;
    uint32T pg;

    sts = imm_page_to_sub_xtnt_map( xtntPageOff, xmP, FALSE, &sI );
    if ( sts == E_PAGE_NOT_MAPPED ) {
        pXtntp->ssPackPageCnt = 0;
        return;
    }

    sxmP = &xmP->subXtntMap[sI];
    sts = imm_page_to_xtnt( xtntPageOff, sxmP, FALSE, FALSE, &xI );
    if ( sts != EOK ) {
        pXtntp->ssPackPageCnt = 0;
        return;
    }

    /* First test to see if the file extent we found overlaps the */
    /* vfast block range. If it doesn't the pXtntp values are trash. */
    xtntStartBlk = sxmP->bsXA[xI].vdBlk;
    p1 = sxmP->bsXA[xI].bsPage;
    p2 = sxmP->bsXA[xI + 1].bsPage;
    xtntEndBlk = xtntStartBlk + (p2 - p1) * ADVFS_PGSZ_IN_BLKS;
    if ( xtntStartBlk >= rangeEndBlk || xtntEndBlk <= rangeStartBlk ) {
        /* The file extent is different than the pXtntp extent. */
        /* I guess someone migrated this page range. */
        pXtntp->ssPackPageCnt = 0;
        return;
    }

    blk = MAX( rangeStartBlk, xtntStartBlk );
    pXtntp->ssPackStartXtBlock = blk;

    blk = MIN( rangeEndBlk, xtntEndBlk );
    pXtntp->ssPackEndXtBlock = blk;

    pg = p1 + (pXtntp->ssPackStartXtBlock - xtntStartBlk) /
              ADVFS_PGSZ_IN_BLKS;
    pXtntp->ssPackPageOffset = pg;

    pg = (pXtntp->ssPackEndXtBlock - pXtntp->ssPackStartXtBlock) /
         ADVFS_PGSZ_IN_BLKS;
    pXtntp->ssPackPageCnt = pg;
}

/* end bs_migrate.c */
