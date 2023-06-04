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
 * Facility:
 *
 *  AdvFS
 *
 * Abstract:
 *
 *  This module contains routines related to migration.
 *
 */

#include <sys/types.h>
#include <tcr/dlm.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <bs_delete.h>
#include <bs_extents.h>
#include <bs_inmem_map.h>
#include <bs_migrate.h>
#include <bs_stg.h>
#include <ms_osf.h>
#include <bs_ims.h>
#include <bs_snapshot.h>
#include <tcr/clu.h>
#include <fs/vfs_ifaces.h>    /* struct nameidata,  NAMEIDATA macro */ 
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif



#define ADVFS_MODULE BS_MIGRATE



/*
 * Private and protos
 */

static
statusT
migrate_normal_one_disk (
                         bfAccessT *bfap,                       /* in */
                         vdIndexT srcVdIndex,                   /* in */
                         bf_fob_t bf_fob_offset,    /* in */
                         bf_fob_t bf_fob_cnt,       /* in */
                         vdIndexT dstVdIndex,                   /* in */
                         bf_vd_blk_t dstBlkOffset,              /* in */
                         uint32_t  forceFlag,                    /* in */
                         bsAllocHintT alloc_hint                /* in */
                         );


static
statusT
migrate_normal (
                bfAccessT *bfap,                        /* in */
                bf_fob_t src_fob_offset,    /* in */
                bf_fob_t src_fob_cnt,       /* in */
                vdIndexT dstVdIndex,                    /* in */
                bf_vd_blk_t dstBlkOffset,               /* in */
                uint32_t  forceFlag,                     /* in */
                bsAllocHintT alloc_hint                 /* in */
                );


static
statusT
extend_fob_range_list (
                        uint32_t *maxCnt,  /* in/out */
                        extent_blk_desc_t **fob_range_arry /* in/out */
                       );

static
statusT
switch_stg (
            bfAccessT *bfap,  /* in */
            bsInMemXtntMapT **origXtntMapAddr,  /* in */
            bfMCIdT *copyMcellId,  /* in, modified */
            bsInMemXtntMapT *copyXtntMap, /* in, modified */
            ftxHT parentFtxH /* in */
            );

static
statusT
move_metadata (
               bfAccessT *bfap,  /* in */
               vdIndexT newVdIndex,  /* in */
               ftxHT parentFtxH  /* in */
               );


static
statusT
mig_alloc_copy_stg (
                bfAccessT *bfap,                /* in   get stg for this file */
                bf_fob_t start_fob,             /* in   start map at this fob */
                extent_blk_desc_t *xm_fob_range_arry,        /* in   array of extents */
                vdIndexT vdIndex,               /* in   get stg on this volume */
                bf_vd_blk_t dstBlkOffset, /* in;if hint & BS_ALLOC_MIG_RSVD,make sure we get this blk*/
                bf_fob_t minimum_contiguous_fobs, /* in */
                bsAllocHintT alloc_hint,        /* in */
                bfMCIdT *copyMcellId,           /* out  starting mcell */
                bsInMemXtntMapT **copyXtntMap,  /* out  return this map */
                ftxHT   parent_ftx
                );

static
statusT
mig_alloc_hole_stg (
                bfAccessT *bfap,  /* in */
                extent_blk_desc_t *hole_range,  /* in */
                vdIndexT vdIndex,  /* in */
                bfMCIdT *copyMcellId,  /* out */
                bsInMemXtntMapT **copyXtntMap, /* out */
                ftxHT   parent_ftx
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
 * This function moves the specified extents to the specified virtual disk.
 * The move may defragment the extents when they are allocated on the
 * target disk.  This depends on how many extents are moved and how fragmented
 * the target disk is.
 *
 * If a block offset is specified, the block range must be a continuous range
 * of blocks.  If it wraps from the domain's last block to the first block,
 * this function returns an error.
 */

statusT
bs_migrate (
            bfAccessT *bfap,  /* in */
            vdIndexT srcVdIndex,  /* in */
            bf_fob_t src_fob_offset,  /* in */
            bf_fob_t src_fob_cnt,  /* in */
            vdIndexT dstVdIndex,  /* in */
            bf_vd_blk_t dstBlkOffset,  /* in */
            uint32_t forceFlag, /* in */
            bsAllocHintT alloc_hint  /* in */
            )
{
    ftxHT ftxH;
    statusT sts;
    unLkActionT unlockAction;
    vdT *svdp, *dvdp;
    int32_t srcVdBumped = FALSE,
            dstVdBumped = 0;

    /*
     * FIX - what happens when a bitfile has a clone, we are migrating the
     * original bitfile, and another thread pins a page that migrate is going
     * to or about to work on?  (see bs_cow_pg() and bs_pinpg()) Only
     * applicable to metadata on HPUX.
     */

    /* FIX - before we can migrate the root tag directory, must make a change
     * in domain activation so that the root tag directory is opened between
     * pass1 and pass2.  Recovery should open and close it.  Domain activation
     * should open it after recovery.
     */

    if (BS_BFTAG_RSVD (bfap->tag)) {
        return ENOT_SUPPORTED;
    }

    /*
     * If the src_fob_cnt is equal to Zero then the file has
     * been truncated to 0 fobs.  This is very common when dealing
     * with Mail files.  Instead of returning EBAD_PARAMS, return
     * success as a Zero fobs file doesn't need to be migrated as it
     * doesn't have any extents.
     */
    if (src_fob_cnt == 0) {
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
                           dstVdIndex
                           );
        if (sts != EOK) {
            if (sts == E_VOL_NOT_IN_SVC_CLASS) {
                sts = EBAD_VDI;
            }
            RAISE_EXCEPTION( sts );
        }
    }

    /* 
     * Caller must specify a range that is on allocation unit boundaries.
     */
    if (((src_fob_offset % bfap->bfPageSz) != 0) || 
        ((src_fob_offset + src_fob_cnt) % bfap->bfPageSz != 0)){

        RAISE_EXCEPTION( E_BLKOFFSET_NOT_PAGE_ALIGNED ); /* we need a
                                                          * better
                                                          * error */
    }

    /*
     * If this is a small allocation unit file and the migrate extends 
     * beyond the first VM_PAGE_SZ bytes of the file, align the beginning
     * and end of the migrate on VM_PAGE_SZ boundaries.
     */
    if ((ADVFS_FILE_HAS_SMALL_AU(bfap)) &&
        (src_fob_offset + src_fob_cnt >= (VM_PAGE_SZ / ADVFS_FOB_SZ))) {

        bf_fob_t adjust;

        adjust = src_fob_offset - rounddown(src_fob_offset, 
                                            (VM_PAGE_SZ / ADVFS_FOB_SZ));
        src_fob_offset -= adjust;
        src_fob_cnt += adjust;

        adjust = roundup((src_fob_offset + src_fob_cnt), 
                         (VM_PAGE_SZ / ADVFS_FOB_SZ)) - 
                     (src_fob_offset + src_fob_cnt);
        src_fob_cnt += adjust;
        
    }

    if ( (uint32_t) dstBlkOffset != (uint32_t) (-1)) {
        if ((dstBlkOffset % bfap->bfPageSz) != 0) {
            RAISE_EXCEPTION( E_BLKOFFSET_NOT_PAGE_ALIGNED );
        }
    }

    sts = mig_migrate (
                       bfap,
                       srcVdIndex,
                       src_fob_offset,
                       src_fob_cnt,
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
             bfAccessT   *srcBfap,        /* in */
             vdIndexT     srcVdIndex,     /* in */
             bf_fob_t     src_fob_offset, /* in */
             bf_fob_t     src_fob_cnt,    /* in */
             vdIndexT     dstVdIndex,     /* in */
             bf_vd_blk_t  dstBlkOffset,   /* in */
             uint32_t     forceFlag,      /* in */
             bsAllocHintT alloc_hint      /* in */
             )
{
    statusT            sts=EOK;
    bsXtntDescT        startXtntDesc,
                       endXtntDesc;
    bf_fob_t           end_fob_offset;
    int32_t            unlockFlag = 0;
    bsInMemXtntDescIdT xtntDescId;
    bsInMemXtntMapT   *xtntMap;

    if (srcBfap->bfaFlags & BFA_VIRGIN_SNAP) {
        /* This snapshot has no metdata, it could not have any
         * storage associated with it */
        return E_NO_SNAPSHOT_STG;
    }

    /* 
     * If the file is  being created, and it is either a directory
     * or a "slow" symlink, then we might be racing with the file creation
     * code while it is adding storage. Synchronize with it by waiting for
     * the bfState to become valid.
     *
     * We can only do a lk_wait_while if we know for a fact that we hold
     * some sort of ref on the access structure that will prevent it from
     * being deallocated.  If ACC_INIT_TRANS was set during a bs_access_one
     * it could go to ACC_INVALID -> ACC_DEALLOC.  However, we must have
     * previously done a successful bs_access_one to get here.
     */

    if (srcBfap->bfState == BSRA_CREATING) {
        lk_wait_while(&srcBfap->stateLk, &srcBfap->bfaLock, ACC_INIT_TRANS);
        if (srcBfap->bfState != BSRA_VALID) {
            return EINVALID_HANDLE;
        }
    }

    xtntMap = srcBfap->xtnts.xtntMap;

    ADVRWL_XTNTMAP_READ( &(srcBfap->xtntMap_lk) );
    unlockFlag = 1;
    /*
     * Verify that the extent range is mapped by the extent map.  If
     * not, bye bye!
     */

    end_fob_offset = (src_fob_offset + src_fob_cnt) - 1;
    imm_get_xtnt_desc (end_fob_offset, xtntMap, 0, &xtntDescId, &endXtntDesc);
    if (endXtntDesc.bsxdFobCnt == 0) {
        RAISE_EXCEPTION (E_RANGE_NOT_MAPPED);
    }

    imm_get_xtnt_desc (src_fob_offset, xtntMap, 0, &xtntDescId, &startXtntDesc);
    if (startXtntDesc.bsxdFobCnt == 0) {
        RAISE_EXCEPTION (E_RANGE_NOT_MAPPED);
    }

    if (unlockFlag) {
        ADVRWL_XTNTMAP_UNLOCK( &(srcBfap->xtntMap_lk) );
        unlockFlag = 0;
    }

    if (startXtntDesc.bsxdVdBlk == -1) {
        /*
         * The range starts in a hole.  Does it end in the same hole?
         */
        if (endXtntDesc.bsxdFobOffset == startXtntDesc.bsxdFobOffset) {
            RAISE_EXCEPTION (E_CANT_MIGRATE_HOLE);
        }
    }

    if (srcVdIndex == (vdIndexT)(-1)) {
       /* moves all the storage in range on all disk volumes 
        * if dstVdIndex==-1, allocate space
        * on service class, otherwise on dstVdIndex only.
        */
        sts = migrate_normal (
                              srcBfap,
                              src_fob_offset,
                              src_fob_cnt,
                              dstVdIndex,
                              dstBlkOffset, /* cleared location  or -1*/
                              forceFlag,
                              alloc_hint
                              );
    } else {
       /* migrate_normal_one_disk moves only the storage for given srcVdIndex.
        * if dstVdIndex==-1, allocate space on service class, otherwise
        * on dstVdIndex only.  Calls migrate_normal.
        */

        sts = migrate_normal_one_disk (
                                       srcBfap,
                                       srcVdIndex,
                                       src_fob_offset,
                                       src_fob_cnt,
                                       dstVdIndex,
                                       dstBlkOffset, /* cleared location or -1 */
                                       forceFlag,
                                       alloc_hint
                                       );
    }

HANDLE_EXCEPTION:

    if (unlockFlag) {
        ADVRWL_XTNTMAP_UNLOCK( &(srcBfap->xtntMap_lk) );
    }

    return sts;

}  /* end mig_migrate */

/*
 * migrate_normal_one_disk
 *
 * This function migrates a normal bitfile.  It moves only the storage of the
 * request range that are on the specified source disk.
 */

static
statusT
migrate_normal_one_disk (
                         bfAccessT *bfap,                       /* in */
                         vdIndexT srcVdIndex,                   /* in */
                         bf_fob_t bf_fob_offset,    /* in */
                         bf_fob_t bf_fob_cnt,       /* in */
                         vdIndexT dstVdIndex,                   /* in */
                         bf_vd_blk_t dstBlkOffset,              /* in */
                         uint32_t  forceFlag,                    /* in */
                         bsAllocHintT alloc_hint                /* in */
                         )
{
    bsXtntDescT         startXtntDesc,
                        nextXtntDesc;
    bsInMemXtntMapT    *newOverXtntMap = NULL,
                       *newXtntMap     = NULL,
                       *overXtntMap    = NULL;
    bf_fob_t            fob_cnt,
                        next_fob_offset,
                        fob_offset;
    bsInMemSubXtntMapT *subXtntMap;
    bsInMemXtntDescIdT  xtntDescId;
    uint32_t            newtype,
                        newmax;
    int32_t             unlockFlag = 0;
    statusT             sts;
    bfMCIdT             initMcellId;
    bsInMemXtntMapT    *xtntMap;

    xtntMap = bfap->xtnts.xtntMap;

    ADVRWL_XTNTMAP_READ( &(bfap->xtntMap_lk) );
    unlockFlag = 1;

    /*
     * Create a fake overlay map. We can hold the xtntmap lock
     * for read here but not for write.
     */
    sts = imm_create_xtnt_map (
                               xtntMap->domain,
                               1,  /* maxCnt */
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
    newtype = BSR_XTNTS;
    newmax = BMT_XTNTS;
    initMcellId = bsNilMCId;
    initMcellId.volume = srcVdIndex;
    sts = imm_init_sub_xtnt_map (
                                 &(overXtntMap->subXtntMap[0]),
                                 bf_fob_offset,
                                 bf_fob_cnt,
                                 initMcellId,
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
    subXtntMap->bsXA[0].bsx_vd_blk = 0;
    subXtntMap->bsXA[1].bsx_fob_offset= bf_fob_offset + bf_fob_cnt;
    subXtntMap->cnt = 2;

    /*
     * Overlay the fake overlay extent map onto the real extent
     * map.  Essentially, clip the real extent map on the requested
     * range boundary.
     *
     * FIX - there must be a better way of doing this.
     */
    sts = imm_overlay_xtnt_map (bfap,
                                xtntMap,
                                overXtntMap,
                                &newXtntMap,
                                &newOverXtntMap
                                );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    newOverXtntMap->validCnt = newOverXtntMap->cnt;

    unlockFlag = 0;
    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    imm_get_first_xtnt_desc (newOverXtntMap, &xtntDescId, &startXtntDesc);

    while (startXtntDesc.bsxdFobCnt > 0) {

        /*
         * Find the first extent descriptor that describes an extent on
         * the source disk.
         */
        while ((startXtntDesc.bsxdFobCnt > 0) &&
               (startXtntDesc.volIndex != srcVdIndex)) {

            imm_get_next_xtnt_desc(newOverXtntMap, &xtntDescId, &startXtntDesc);
        }  /* end while */

        if (startXtntDesc.bsxdFobCnt == 0) {
            /*
             * Did not find an entry that has a matching vdIndex.
             */
            break;  /* out of while */
        }

        /*
         * Find the next extent descriptor that describes an extent
         * on a disk other than the source disk.
         */

        next_fob_offset = startXtntDesc.bsxdFobOffset + startXtntDesc.bsxdFobCnt;
        imm_get_next_xtnt_desc (newOverXtntMap, &xtntDescId, &nextXtntDesc);
        while ((nextXtntDesc.bsxdFobCnt > 0) &&
               (nextXtntDesc.volIndex == srcVdIndex)) {

            next_fob_offset = nextXtntDesc.bsxdFobOffset + nextXtntDesc.bsxdFobCnt;
            imm_get_next_xtnt_desc (newOverXtntMap, &xtntDescId, &nextXtntDesc);
        }  /* end while */

        /*
         * FIX - must adjust blkoffset here somewhere when we go thru the loop
         * more than once.
         */

        fob_offset = startXtntDesc.bsxdFobOffset;
        fob_cnt= next_fob_offset - fob_offset;

        sts = migrate_normal( bfap,
                              fob_offset,
                              fob_cnt,
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
        ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );
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
 * migrate_clu_handling
 *
 * handle the case that we are running in a cluster and may need
 * to revoke the read token.
 */
static
statusT
migrate_clu_handling(
                bfAccessT *bfap,                        /* in */
                int32_t *do_cluster_cleanup,
                int32_t *do_clxtnt_unlock
                )
{
#ifdef SNAPSHOTS 
    statusT sts = 0;
    uint32_t clu_cow_mode_enter = 0;
    fsid_t fsid;
#endif

    /* 
     * Until new snapshot code is in place, which will effectively
     * replace most of this routine, handle the cluster case for non
     * snapshots.
     */
    if ( (bfap->bfVnode.v_vfsp) && bfap->bfVnode.v_vfsp->vfs_flag & VFS_CFSONTOP ) { 
        /* On cluster systems, if the fileset is mounted,
         * we must revoke the read token on this
         * the migration has completed. This is done so that if this file
         * is subsequently opened for directIO, it will re-read the
         * extent maps after this migration has completed.   
         * Only do this if this is a non-metadata regular file (VREG)
         * since only these files could be subsequently opened for directIO.
         */
        if ( bfap->dataSafety != BFD_METADATA  &&
             bfap->bfVnode.v_type == VREG &&
             bfap->bfSetp->fsnp) {

            /* We are in a cluster and need to get the directio token
             * exclusively.  Different rules are followed for
             * clones. Since CLU_CFS_CONDIO_EXCL_MODE_ENTER could get a
             * cnode (cfs vnode) we must not be in a root ftx. On the
             * other hand since the clone is guarenteed (by CFS) not
             * to get a cnode and the bs_cow_pg path is within a root
             * ftx, We can not hold the token exclusively
             * (C_CFS_COW_MODE_ENTER) and start a root transaction
             * (we would deadlock with COW).  
             */
            
            if ( CLU_CFS_CONDIO_EXCL_MODE_ENTER(&bfap->bfVnode) ) {
                /*
                 * the only reason this can fail is if the system
                 * is out of vnodes
                 */
                return (ENO_MORE_MEMORY);
            }
            *do_cluster_cleanup = 1;
        }
    }

#ifdef SNAPSHOTS 
        if ( (bfap->bfVnode.v_vfsp) && bfap->bfVnode.v_vfsp->vfs_flag & VFS_CFSONTOP ) { 
        /* On cluster systems, if the fileset is mounted,
         * we must revoke the read token on this
         * the migration has completed. This is done so that if this file
         * is subsequently opened for directIO, it will re-read the
         * extent maps after this migration has completed.   
         * Only do this if this is a non-metadata regular file (VREG)
         * since only these files could be subsequently opened for directIO.
         */
        if ( bfap->dataSafety != BFD_METADATA  &&
             bfap->bfVnode.v_type == VREG &&
             bfap->bfSetp->fsnp) {

            /* We are in a cluster and need to get the directio token
             * exclusively.  Different rules are followed for
             * clones. Since C_CFS_CONDIO_EXCL_MODE_ENTER could get a
             * cnode (cfs vnode) we must not be in a root ftx. On the
             * other hand since the clone is guarenteed (by CFS) not
             * to get a cnode and the bs_cow_pg path is within a root
             * ftx, We can not hold the token exclusively
             * (CC_CFS_COW_MODE_ENTER) and start a root transaction
             * (we would deadlock with COW).  
             */
            
            if (bfap->bfaFirstChildSnap)
            {
                if ( CC_CFS_CONDIO_EXCL_MODE_ENTER(&bfap->bfVnode) ) {
                    /*
                     * the only reason this can fail is if the system
                     * is out of vnodes
                     */
                    return (ENO_MORE_MEMORY);
                }
                *do_cluster_cleanup = 1;
            }
            else clu_cow_mode_enter = 1;
        }

        if ( (!is_clone) && BFSET_VALID(bfap->bfSetp->cloneSetp))
        {
            /* There is a clone fileset. Check to see if this
             * file itself has a clone.
             */

            bfTagT tag;
            bfMCIdT mcid;
            bfAccessT *cloneap;
            struct vnode *nullvp = NULL;
            ADVRWL_COW_WRITE( &(bfap->cow_lk) );
                
            tag=bfap->tag;
                
            if ((BFSET_VALID(bfap->bfSetp->cloneSetp)) &&
                (tagdir_lookup( bfap->bfSetp->cloneSetp, &tag, &mcid) == EOK) &&
                (bfap->bfSetp->cloneSetp->state != BFS_DELETING)) {

                /* CFS guarentees that the cnodes will be reffed on
                 * the server if a client has the clone open. We
                 * however need to open the clone internally in case
                 * it is opened externally during the migration. This
                 * way we can get a lock (later in this path) that
                 * will stop the xtnt maps from being read.  
                 */
                    
                sts = bs_access_one( &cloneap,
                                     bfap->tag,
                                     bfap->bfSetp->cloneSetp,
                                     FtxNilFtxH,
                                     BF_OP_INTERNAL,
                                     &nullvp,
                                     NULL );
                if (sts != EOK) {
                    /* There doesn't appear to be a clone any more so
                     * do nothing?  
                     */
                    bfap->noClone = TRUE;
                    return (sts);
                       
                }
                bfap->nextCloneAccp = cloneap;
                bfap->noClone = FALSE;
                ADVRWL_COW_UNLOCK( &(bfap->cow_lk) );

                if (cloneap->dataSafety != BFD_METADATA) {
                    
                    /* If this file has a clone then the non COWed
                     * portion of the clone extent map will be changed
                     * and we will need to revoke the read token if it
                     * exists.
                     * The clu_clonextnt_lk will prevent the clone
                     * from being openned if we can't revoke the
                     * token 
                     *(this is all done farther down in this
                     * routine).
                     */

                    clu_cow_mode_enter = 1;
                }
                else
                {
                    /* This file can not have clu directio 
                     * so we don't need the access structure
                     */

                    bs_close_one( cloneap, MSFS_CLOSE_NONE, FtxNilFtxH);
                }
            }
            else {/* no clone set or no clone file */
                ADVRWL_COW_UNLOCK( &(bfap->cow_lk) );
            }
        }
        if (clu_cow_mode_enter){
            bfAccessT *cloneap;
            
            /* We are in a cluster and we are either a clone
             * or an original that has a clone.
             */
            
            if (is_clone) cloneap = bfap;
            else cloneap = bfap->nextCloneAccp;
            
            /* Get the cluster dio token exclusively so that clients
             * can not use their cached extent maps. The will need to
             * refresh them from the server after reobtaining their
             * token
             */

            BS_GET_FSID(cloneap->bfSetp,fsid);

            while (!*do_clxtnt_unlock)
            {
                if (cloneap->bfSetp->fsnp &&
                    !(CC_CFS_COW_MODE_ENTER( fsid, cloneap->tag ))){

                    /* We have the token so no clients can have cached clone
                     * extent maps. Clear the cloneXtntsRetrieved so we
                     * can tell if in the up coming window a client requested
                     * and obtained the clone extents.
                     */

                    cloneap->cloneXtntsRetrieved = 0;

                    CC_CFS_COW_MODE_LEAVE( fsid, cloneap->tag );       
                }

                /* No cluster clients currently had cached extent maps for
                 * this clone. We need to prevent them from obtaining them
                 * however during the rest of this migration. This is done by
                 * holding the ADVRWL_CLXTNT_WRITE lock.
                 */

                /* At this point either we obtained the token and marked the
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

                ADVRWL_CLXTNT_WRITE(&(cloneap->clu_clonextnt_lk));
                *do_clxtnt_unlock = 1;

                /* Lets do the cluster clone shuffle:
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

                if (cloneap->cloneXtntsRetrieved) {
                    cloneap->cloneXtntsRetrieved = 0;
                    ADVRWL_CLXTNT_UNLOCK(&(cloneap->clu_clonextnt_lk));
                    *do_clxtnt_unlock = 0;
                }
            }
            /* Clients can now get the token
             * but the clu_clonextnt_lk will prevent them from getting
             * the clone's extnet maps until migrate is done.
             */
        }
    }
#endif /* SNAPSHOTS */
    return (EOK);
}


/*
 * migrate_fill_cache
 * 
 * Fault in all pages in the desired range that are
 * backed to real storage (no holes).  This routine will only fault in pages
 * from src_fob_offset to src_fob_cnt even if the fob_range extends beyond
 * that.  It is assumed that fob_range starts at src_fob_offset. 
 */
static
statusT
migrate_fill_cache(bfAccessT *bfap,                        /* in */
                   extent_blk_desc_t *fob_range,           /* in */
                   bf_fob_t src_fob_offset,                /* in */
                   bf_fob_t src_fob_cnt)                   /* in */
{
    statusT sts = EOK, sts2;
    extent_blk_desc_t *walk_range = fob_range;

    fcache_map_dsc_t *fcmap;
    fcache_as_hdl_t vas;
    faf_status_t faultStatus;
    struct advfs_pvt_param priv_param;
    struct nameidata *ndp;
    struct vnode *saved_vnode;
    off_t  byte_offset = src_fob_offset * ADVFS_FOB_SZ;
    off_t  end_offset = ADVFS_FOB_TO_OFFSET( src_fob_offset + src_fob_cnt );
    size_t map_byte_cnt;
    size_t fault_size; 
    int64_t  byte_cnt;
    size_t   file_size;
    int metadata = BS_IS_META(bfap);


    MS_SMP_ASSERT( fob_range->ebd_offset == byte_offset );

    ndp = NAMEIDATA();

   /* Indicate to getpage this not an mmaper */
    ndp->ni_nameiop |= NI_RW;

    /* we must pass a private param struct as well, or getpage
     * will still think we are an mmaper */
    bzero((char *)&priv_param, sizeof(struct advfs_pvt_param));
    
    vas = metadata ? bfap->dmnP->metadataVas: bfap->dmnP->userdataVas;
    file_size  = bfap->bfaNextFob * ADVFS_FOB_SZ;
    saved_vnode = ndp->ni_vp;
    ndp->ni_vp = &bfap->bfVnode;
    /*
     * Fault in pages for extents mapped to storage.
     */
    while ( (walk_range != NULL) && (walk_range->ebd_offset <= end_offset) )  {
        if (ADVFS_HOLE(walk_range)) {
            /*
             * No need to fault in holes.
             */
            walk_range = walk_range->ebd_next_desc;
            continue;
        } 
        byte_cnt    = walk_range->ebd_byte_cnt;
        byte_offset = walk_range->ebd_offset;

        /*
         * We don't want to fault beyond the request range.
         */
        if (byte_offset + byte_cnt > end_offset) {
            byte_cnt = end_offset - byte_offset;
        }


       /*
        * Don't attempt to map/fault a range that extends beyond the end 
        * of the file.  Otherwise, getpage gets an assertion error.
        * Simply make byte_cnt be equal to the end of file minus the 
        * current offset.
        */
        if (byte_cnt + byte_offset > file_size) {
            byte_cnt = file_size - byte_offset;
            MS_SMP_ASSERT(byte_cnt > 0);
            if (byte_cnt <= 0)
                return(E_BAD_RANGE);
        }

        /* setup these variables for potential readahead in getpage */
        priv_param.app_total_bytes = byte_cnt;

        priv_param.app_starting_offset = walk_range->ebd_offset;
        
        /*
         * Advise advfs_getpage that this is a migrate related fault.  The
         * migStgLk and bfaSnapLock are already held appropriately.
         */
        priv_param.app_flags = APP_MIGRATE_FAULT;

        /* grab the next fob range in the list */
        walk_range  = walk_range->ebd_next_desc;
        
        /*
         * Fault in byte_cnt bytes starting at byte_offset.
         */
        while (byte_cnt > 0) {

            /*
             * For user data, try to map the entire size of the block descriptor.
             * For metadata, we must use the metadata page size in order for
             * the UFC to guarantee that the pages remain together.
             * It's possible that for performance reasons we want user
             * data to map at some fixed size (ie 64k), but for now
             * map as much as possible. 
             */
            map_byte_cnt = metadata ? ADVFS_METADATA_PGSZ : byte_cnt;

            /*
             * Set up the mapping for the range to be faulted.
             */
            fcmap = fcache_as_map(vas, 
                                  &bfap->bfVnode, 
                                  byte_offset, 
                                  map_byte_cnt, 
                                  0,
                                  FAM_READ, 
                                  &sts);
            if (sts) {
                ndp->ni_nameiop &= ~NI_RW;
                ndp->ni_vp = saved_vnode;
                return (sts);
            }

            fault_size = min(map_byte_cnt, fcmap->fm_size);
            sts = fcache_as_fault(fcmap,
                                  (char *)fcmap->fm_vaddr,
                                  &fault_size, /* out - size actually faulted */
                                  &faultStatus,
                                  (uintptr_t)&priv_param,
                                  (FAF_READ | FAF_SYNC));

            /*
             * We might want to consider using FAU_CACHE, instead.
             */
            sts2 = fcache_as_unmap(fcmap, NULL, 0, FAU_FREE);
            if (sts || sts2) {
                ndp->ni_nameiop &= ~NI_RW;
                ndp->ni_vp = saved_vnode;
                return (sts || sts2);
            }

            byte_cnt    -= fault_size;
            byte_offset += fault_size;

        }
    }
    ndp->ni_nameiop &= ~NI_RW;
    ndp->ni_vp = saved_vnode;
    return (sts);
}

/*
 * migrate_normal
 *
 * This function migrates a normal bitfile.  It moves all the storage within
 * the request range.
 */

static
statusT
migrate_normal (
                bfAccessT *bfap,                        /* in */
                bf_fob_t src_fob_offset,                /* in */
                bf_fob_t src_fob_cnt,                   /* in */
                vdIndexT dstVdIndex,                    /* in */
                bf_vd_blk_t dstBlkOffset,               /* in */
                uint32_t  forceFlag,                     /* in */
                bsAllocHintT alloc_hint                 /* in */
                )
{
    statusT sts = 0, sts2, sts3;
    int32_t do_cluster_cleanup    = 0,
            do_clxtnt_unlock      = 0,
            mig_stg_locked        = FALSE,
            bfa_snap_locked       = FALSE; 
    int32_t     ftx_started       = 0;
    bf_fob_t mapped_cnt           = 0;
    bf_fob_t again_fob_offset     = 0;
    bf_fob_t again_fob_cnt        = 0;
    int32_t  do_more_migrate      = FALSE;
    

    uint64_t extent_count         = 0;
    bfMCIdT local_copy_mcell_id   = bsNilMCId;

    bsInMemXtntMapT *copy_xtnt_map = NULL;

    struct actRange *arp = NULL;
    extent_blk_desc_t *fob_range = NULL, *f;

    uint64_t savedCowPgCount;

    off_t offset;
    ftxHT ftxH = FtxNilFtxH;

    struct advfs_pvt_param pvt_param;

    bf_fob_t minimum_contiguous_fobs;

    bzero(&pvt_param, sizeof(struct advfs_pvt_param));


    sts =  migrate_clu_handling(bfap, &do_cluster_cleanup, &do_clxtnt_unlock );

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }


    /*
     * Get the range information.  The request ranges returned are extent
     * map relative.  This is ok because for an append bitfile, the bitfile
     * relative and extent map relative fob offsets are the same.
     */

start_over_with_new_actRange:

    /* Allocate an active range structure; this synchronizes migrate
     * with local directIO and advfs_cfs_flush_and_invalidate. In the
     * past it was stated the CFS token mechanism prevented either
     * from occuring, however, corruption from CFS page invalidation
     * while a migrate is running has proven that to be unreliable so
     * now we always lock the backdoor ourselves.
     */
    arp = advfs_bs_get_actRange(TRUE);
    /* Initialize fields; ms_malloc() inited struct to zeros.  */
    arp->arStartFob = src_fob_offset;
    arp->arEndFob = ((src_fob_offset + src_fob_cnt)) - 1;
    arp->arState = AR_MIGRATE;
    arp->arDebug = SET_LINE_AND_THREAD(__LINE__);

    /*
     * Prevent the cache mode from changing on this file during the
     * migrate.  Otherwise, we could possibly fault in pages
     * outside of our active range due to vm's large page handling.
     * If the file is already opened for direct io, advfs_getpage will
     * only allow vm to fault in pages within our active range. It should
     * be noted that a large migrate could potentially block direct io
     * opens or closes.
     */
    ADVRWL_BFAPCACHEMODE_READ(bfap);

    /* Put this active range onto the list to block directIO threads
     * in the same range.  This also prevents truncation/invalidation.
     */
    (void)insert_actRange_onto_list( bfap, arp, NULL );   /* may sleep */


start_over:
    /*
     * Migrating a child metadata snapshot is non-trivial.  This routine
     * will go through several hoops to ensure that locks are not acquired
     * in a manner that will cause a hang even though the locks will be
     * acquired out of order.  For child metadata snapshots, we will limit
     * the amount of storage migrated in a single whack to the amount of
     * storage that can be added in a single transaction.  If we need to, we
     * will loop several times to migrate the entire requested range.
     */
    if ( IS_COWABLE_METADATA( bfap ) && HAS_SNAPSHOT_PARENT( bfap ) ) {

meta_child_try_again:
        ACCESS_STATS( migrate_normal_migrate_child_metadata );

        FTX_START_N( FTA_BS_MIG_MIGRATE_V1, &ftxH, FtxNilFtxH, bfap->dmnP ); 
        ftx_started = TRUE;

        if (!ADVRWL_MIGSTG_WRITE_TRY( bfap )) {
            ftx_quit( ftxH );
            /* Wait on the lock being available rather than just spinning */
            ADVRWL_MIGSTG_WRITE( bfap );
            ADVRWL_MIGSTG_UNLOCK( bfap );
            ACCESS_STATS( migrate_normal_migrate_child_metadata_looped );

            goto meta_child_try_again;
        }
        
        mig_stg_locked = TRUE;

        /*
         * Make sure the storage allocation routines don't try to finish the
         * transaction we just started since they may not be able to start a
         * new one.
         */
        alloc_hint |= BS_ALLOC_ONE_FTX;
    } else {
        /*
         * Need to take the migStgLk before calling advfs_get_blkmap_in_range().
         * This prevents another thread from adding or removing any storage
         * to/from the file until the migration is complete.  Formerly, this
         * lock was taken after calling mig_alloc_copy_stg().  That left a window
         * of time between the releasing of the extent map lock after the
         * call to advfs_get_blkmap_in_range() and the taking of the migStg_lk.
         * In that window, another thread could add storage to a hole,
         * causing the fob_range produced by get_xm_fob_range_info() to become
         * out-of-date.  But the code didn't detect that and at the end of the
         * migration, the storage that had been added to the hole were lost.
         */
        ADVRWL_MIGSTG_WRITE( bfap );
        mig_stg_locked = TRUE;
    }

    /*
     * This can happen in two cases:
     * 1) When trying to migrate a child metadata snapshot, since we may drop
     *    the migStgLk in the middle of a migrate in order to prevent deadlocks.
     *    In this case, we will have started a transaction that we need to quit.
     * 2) Another migrate may be in progress on this file that has already
     *    dropped the MIGSTG lock (later in this function) but has not yet
     *    cleared the BFA_MIG_IN_PROGRESS flag.
     *
     * In either case, if another migrate is in progress, just wait a second
     * and start over.  
     */
    if (bfap->bfaFlags & BFA_MIG_IN_PROGRESS) {
        if ( ftx_started == TRUE ) {
            ftx_quit( ftxH );
            ftx_started = FALSE;
        }
        ADVRWL_MIGSTG_UNLOCK( bfap );
        mig_stg_locked = FALSE;
        delay(1);
        ACCESS_STATS( migrate_normal_start_over_loop );
        goto start_over;
    } else {
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
        bfap->bfaFlags |= BFA_MIG_IN_PROGRESS;
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    }

    /*
     * Make sure this is Nil so that we can use it as a flag to determine if
     * we need to deallocate storage.
     */
    bfap->xtnts.copyMcellId = bsNilMCId;


    /*
     * Start and finish should be allocation unit size (page size)
     * aligned.
     */
    MS_SMP_ASSERT(src_fob_offset % bfap->bfPageSz == 0);
    MS_SMP_ASSERT((src_fob_offset + src_fob_cnt) % bfap->bfPageSz == 0);

    /*
     * advfs_get_blkmap_in_range needs to round to adjacent leading
     * (left) holes and truncate trailing (right) holes in order to maintain
     * the requirement that XTNT_TERM holes must accompany trailing storage.
     * This rule does not apply to permanent holes (PERM_HOLE_START).  
     * If the offset and size are within a hole, a single whole extent 
     * should be returned and handled later in this routine.
     *
     * NOTE: The migStg_lk is enough protection here. We don't need
     * to bother locking the extent map locks since they can't change.
     */
    offset = (off_t) src_fob_offset * ADVFS_FOB_SZ;
    sts = advfs_get_blkmap_in_range(bfap,
                                    bfap->xtnts.xtntMap,
                                    &offset,
                                    src_fob_cnt * ADVFS_FOB_SZ,
                                              /* length in bytes */
                                    &fob_range,
                                    &extent_count,
                                    RND_MIGRATE,
                                    EXB_COMPLETE|EXB_DO_NOT_INHERIT, /* holes and storage*/
                                    XTNT_WAIT_OK);

    if (sts != EOK || fob_range==NULL) {
        RAISE_EXCEPTION(sts);
    }

    /* 
     * Check to see if we included a preceding hole.
     * Adjust the src_fob_offset and src_fob_cnt accordingly.
     */
    if (offset > fob_range->ebd_offset) {
        MS_SMP_ASSERT(ADVFS_HOLE(fob_range));
        src_fob_offset = ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_offset);
        src_fob_cnt += ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_byte_cnt);
    }

    /* 
     * Check to see if we dropped a trailing hole.
     * Adjust the src_fob_cnt accordingly.
     */
    f = fob_range;
    while (f) {
        mapped_cnt += ADVFS_OFFSET_TO_FOB_DOWN(f->ebd_byte_cnt);
        f = f->ebd_next_desc;
    }
    if (mapped_cnt < src_fob_cnt) {
        src_fob_cnt = mapped_cnt;
    }



    if (ADVFS_HOLE(fob_range) && extent_count==1) {
        if (ADVFS_NORMAL_HOLE(fob_range)) {
            /*
             * migrate of a hole is a no op.
             */
            RAISE_EXCEPTION(EOK);
        }

        /*
         * migrate a COWed hole by itself.  This is handled as a special
         * case as there is no trailing storage.
         */
        sts = mig_alloc_hole_stg (
                              bfap,
                              fob_range,
                              dstVdIndex,
                              &bfap->xtnts.copyMcellId,
                              &copy_xtnt_map,
                              ftxH );
        
    } else {
        /* 
         * If an error occurs, copyMcellId may point to storage that needs
         * to be deallocated.  If the volume is non-zero, del_dealloc_stg 
         * needs to be called after the transaction completes.
         */

        minimum_contiguous_fobs = (ADVFS_FILE_HAS_SMALL_AU(bfap) &&
                                   ((src_fob_offset + src_fob_cnt) >=
                                    (VM_PAGE_SZ / ADVFS_FOB_SZ)) ?
                                   VM_PAGE_SZ / ADVFS_FOB_SZ :
                                   bfap->bfPageSz);

        sts = mig_alloc_copy_stg (
                              bfap,
                              src_fob_offset,
                              fob_range,
                              dstVdIndex,
                              dstBlkOffset,
                              minimum_contiguous_fobs,
                              alloc_hint,
                              &bfap->xtnts.copyMcellId,
                              &copy_xtnt_map,
                              ftxH);
    }

    if ( (sts != EOK) && ( sts != EAGAIN) ) {
        RAISE_EXCEPTION (sts);
    }

    if (sts == EAGAIN) {
        int32_t last_sub_xtnt;
        int32_t last_xtnt;
        /* This should be a snapshot child but we can't assert that is still
         * has a parent since we aren't holding the bfaSnapLock.
         */
        ACCESS_STATS( migrate_normal_eagain_condition );

        MS_SMP_ASSERT( IS_COWABLE_METADATA( bfap ) );
        last_sub_xtnt = copy_xtnt_map->validCnt - 1;
        last_xtnt = copy_xtnt_map->subXtntMap->cnt - 1;
        /*
         * last_sub_xtnt, last_xtnt should be the terminator for the storage
         * we just added.  This is were we need to start adding the next
         * batch of storage if there is any left to add.
         */
        again_fob_offset = copy_xtnt_map->subXtntMap[last_sub_xtnt].bsXA[last_xtnt].bsx_fob_offset;
        /*
         * Make sure that is the terminator 
         */
        MS_SMP_ASSERT( copy_xtnt_map->subXtntMap[last_sub_xtnt].bsXA[last_xtnt].bsx_vd_blk 
                                == XTNT_TERM);
        again_fob_cnt = (src_fob_offset + src_fob_cnt) - again_fob_offset;
        copy_xtnt_map->bsxmNextFob = again_fob_offset;
        /*
         * Adjust the src_fob_cnt to what we will really be doing this time
         * through
         */
        src_fob_cnt = again_fob_offset - src_fob_offset;
        if (again_fob_cnt > 0) {
            do_more_migrate = TRUE;
        }
    } else {
        /*
         * Setup the next valid fob before inserting the copy extents into the
         * extent maps.
         */
        copy_xtnt_map->bsxmNextFob = fob_range->ebd_offset;
    }

    if (ftx_started) {
        /* We are in a unique situation with a child metadata snapshot.  We
         * will indicate to other accessors of this file that a migrate is
         * in progress and may require the switching of storage. 
         */
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
        bfap->bfaFlags |= BFA_SWITCH_STG;
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

        /*
         * We must have started a transaction above.  We don't want to keep
         * this running during the fault, so finish it now.  We'll deal with
         * starting a new one for the switch storage later.
         */
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_229);
        ftx_done_n( ftxH, FTA_BS_MIG_MIGRATE_V1 );
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_230);
        ftx_started = FALSE;
    }


    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_3);


    /*
     * After inserting the copy extent map into the copy extent map list,
     * any new extents brought into the buffer cache for this bitfile are
     * mapped to both the original's and the copy's storage.
     */

    ADVRWL_XTNTMAP_WRITE( &(bfap->xtntMap_lk) );
    ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);
    
    /* Set these up before making the copy extent map visible */

    bfap->migrate_starting_offset = src_fob_offset * ADVFS_FOB_SZ;
    bfap->migrate_ending_offset   = (src_fob_offset + src_fob_cnt) * ADVFS_FOB_SZ;

        
    /*
     * Insert the copy xtnt map into the xtnt map as the copy. 
     */
    bfap->xtnts.copyXtntMap = copy_xtnt_map;
    

    ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    /*
     * Cause the pages in the desired range to be faulted into the cache.
     */
    sts = migrate_fill_cache(bfap, 
                             fob_range,
                             src_fob_offset,
                             src_fob_cnt
                            );

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }


    /*
     * Flush pages.
     */
    pvt_param.app_flags = APP_MIGRATE_FLUSH_ALL;
    sts = fcache_vn_flush(&bfap->bfVnode,
                          src_fob_offset * ADVFS_FOB_SZ,
                          src_fob_cnt    * ADVFS_FOB_SZ,
                          (uintptr_t) &pvt_param,
                          FVF_WRITE|FVF_SYNC);

    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    if ( IS_COWABLE_METADATA( bfap ) && HAS_SNAPSHOT_PARENT( bfap ) ) {

        ADVRWL_MIGSTG_UNLOCK( bfap );


meta_child_try_again2:
        
        if ( bfap->bfaFlags & BFA_SWITCH_STG ) {
            ACCESS_STATS( migrate_normal_bfa_switch_stg_stg );
            FTX_START_N( FTA_BS_MIG_MIGRATE_V1, &ftxH, FtxNilFtxH, bfap->dmnP ); 
            ftx_started = TRUE;

            if (!ADVRWL_MIGSTG_WRITE_TRY( bfap )) {
                ftx_quit( ftxH );
                /* Wait on the lock being available rather than just spinning */
                ADVRWL_MIGSTG_WRITE( bfap );
                ADVRWL_MIGSTG_UNLOCK( bfap );
                
                ACCESS_STATS( migrate_normal_wait_for_migStgLk );

                goto meta_child_try_again2;
            }
        } else {
            /*
             * Another thread has done the switch storage and we no longer
             * need to start a transaction.  We are safe to wait on the
             * migStgLk.
             */
            ACCESS_STATS( migrate_normal_another_thread_did_stg_switch );
            ADVRWL_MIGSTG_WRITE( bfap );
        }
        mig_stg_locked = TRUE;

        /*
         * If BFA_SWITCH_STG is still set, then we didn't race with anyone
         * else trying to get the migStgLk so we will just continue on
         * normally.  If the BFA_SWITCH_STG flag is not set, then another
         * thread has already done the switch storage and we can avoid doing
         * it.
         */
        if (bfap->bfaFlags & BFA_SWITCH_STG) {
            MS_SMP_ASSERT( ftx_started );
            goto mig_switch_stg;
        } else if (bfap->bfaFlags & BFA_SWITCH_STG_ERR) {
            /* Another thread encountered an error trying to switch the
             * storage.  This should be treated as an error and returned.
             */
            ACCESS_STATS( migrate_normal_another_thread_did_stg_switch_err );
            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            bfap->bfaFlags &= ~BFA_SWITCH_STG_ERR;
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
            sts = EIO;
            RAISE_EXCEPTION( sts );
        } else {
            /*
             * The other thread successfully switched the storage, so just
             * skip doing it.
             */
            goto mig_skip_switch_stg;
        }
    } else {

        FTX_START_N( FTA_BS_MIG_MIGRATE_V1, &ftxH, FtxNilFtxH, bfap->dmnP ); 
        ftx_started = TRUE;

    }

mig_switch_stg:

    /*
     * Take the bfaSnapLock while we switch the storage so that any snapshot
     * children block trying to read from the parent's storage.  The
     * children snapshots will acquire the parents bfaSnapLocks but not the
     * migStgLk.  We delay taking this lock until now because until this
     * point, the original storage could still be read from.
     */

    ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
    bfa_snap_locked = TRUE;

    /* 
     * No other thread tried to add storage when we dropped the locks.
     * We need to switch the storage.
     */
    sts = advfs_migrate_switch_stg_full( bfap,
                                ftxH );
    if (sts != EOK) {
        ACCESS_STATS( migrate_normal_switched_stg_and_erred );
        RAISE_EXCEPTION(sts);
    }

mig_skip_switch_stg:

    if (ftx_started == TRUE) {
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_231);
        ftx_done_n( ftxH,  FTA_BS_MIG_MIGRATE_V1 );
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_232);
    }
    ftx_started = FALSE;


    /* 
     * We can now let child snapshots read from the storage again since it
     * is on disk and initialized.
     */
    if (bfa_snap_locked) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
    }
    bfa_snap_locked = FALSE;


    /*
     * We need to perform another synchronous flush to guarantee that any
     * i/o's that were in flight to the original storage have now
     * completed.
     */
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_418);
    pvt_param.app_flags = APP_MIGRATE_FLUSH_WAIT;
    sts = fcache_vn_flush(&bfap->bfVnode,
                          src_fob_offset * ADVFS_FOB_SZ,
                          src_fob_cnt * ADVFS_FOB_SZ,
                          (uintptr_t) &pvt_param,
                          FVF_WRITE|FVF_SYNC);
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_419);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }


    /* fall thru to exception handler */

HANDLE_EXCEPTION:



    if (ftx_started == TRUE) {
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_233);
        ftx_done_n( ftxH, FTA_BS_MIG_MIGRATE_V1 );
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_234);
        ftx_started = FALSE;
    }
    if (fob_range != NULL) {
       advfs_free_blkmaps(&fob_range); 
    }

    if (bfap->xtnts.copyXtntMap != NULL) {

        /*
         * Can't leave dangling reference to deallocated copyXtntMap.  If
         * another thread did the switch storage, they 
         */
        copy_xtnt_map = bfap->xtnts.copyXtntMap;
        ADVRWL_XTNTMAP_WRITE  ( &(bfap->xtntMap_lk) );
        ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);
        bfap->xtnts.copyXtntMap = NULL;
        ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
        ADVRWL_XTNTMAP_UNLOCK ( &(bfap->xtntMap_lk) );
        imm_delete_xtnt_map (copy_xtnt_map);
    }

    /*
     * Grab the copyMcellId of the storage to deallocate before dropping the
     * migStgLk. This may be the original "source" storage or the copy "target" 
     * storage depending on whether errors occured or not.
     */
    local_copy_mcell_id = bfap->xtnts.copyMcellId;
    bfap->xtnts.copyMcellId = bsNilMCId;

    if ( mig_stg_locked ) {
        ADVRWL_MIGSTG_UNLOCK( bfap );
    }

    if (bfa_snap_locked ) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
    }


    /* 
     * The design for direct IO on hpux requires that the pages 
     * that were brought in during a migrate need to be invalidated.
     */

    if ( bfap->dioCnt ) {

        MS_SMP_ASSERT( (src_fob_offset * ADVFS_FOB_SZ) % NBPG == 0 &&
                       (src_fob_cnt    * ADVFS_FOB_SZ) % NBPG == 0 &&
                       (src_fob_cnt    * ADVFS_FOB_SZ)        != 0 );
        sts3 = fcache_vn_invalidate(&bfap->bfVnode, 
                                    (off_t)(src_fob_offset * ADVFS_FOB_SZ),
                                    (size_t)(src_fob_cnt   * ADVFS_FOB_SZ),
                                    NULL,
                                    BS_IS_META(bfap) ? (FVI_PPAGE | FVI_INVAL) :
                                                       (FVI_INVAL));
        MS_SMP_ASSERT(sts3 != EINVAL);
    }


    /* We need to remove and free the actRange struct */
    if ( arp ) {
        ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );
        spin_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);   /* also wakes waiters */
        spin_unlock(&bfap->actRangeLock);
        advfs_bs_free_actRange(arp, TRUE);
        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
    }

     /*
      * Clear out the BFA_MIG_IN_PROGRESS flag to allow the next migrate on
      * this file to occur.
      * */
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    bfap->bfaFlags &= ~BFA_MIG_IN_PROGRESS;
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );


    /* If any storage was allocated volume will be non-zero.
     * The exception to this is if we failed to switch the storage in which
     * case we will orphan the allocated storage until next time the DDL is
     * processed.  This is because the copy xtnts still map the storage.*/
    if ( local_copy_mcell_id.volume ) {
        vdT *vdp;
        vdp = VD_HTOP( local_copy_mcell_id.volume, bfap->dmnP);
        (void)del_dealloc_stg( local_copy_mcell_id, vdp);
    }


    if (do_more_migrate && (sts == EOK)) {
        /* 
         * We must be migrating a child snapshot that is also metadata.  Due
         * to transaction constraints we can only do this in small chunks.
         * so do the next batch.
         */
        ACCESS_STATS( migrate_normal_doing_more_migrating );
        src_fob_offset = again_fob_offset;
        src_fob_cnt = again_fob_cnt;
        do_more_migrate = FALSE;
        goto start_over_with_new_actRange;
    }


    if (do_clxtnt_unlock) {

#ifdef SNAPSHOTS
        MUST UNLOCK ANY LOCKS ACQUIRED IN migrate_clu_handling
#endif
    }

    if ( do_cluster_cleanup ) {

        /* On cluster systems, release the read token so that the 
         * file's extent maps will be re-loaded.
         */

        CLU_CFS_CONDIO_EXCL_MODE_LEAVE(&bfap->bfVnode);
    }

    return sts;

}  /* end migrate_normal */



/*
 * BEGIN_IMS
 *
 * advfs_migrate_switch_stg_full - This routine is responsible for switch
 * the in memory and on disk extents of a file that is being migrated.  The
 * routine may be called from either the thread that is doing the migrate or
 * from another thread that has acquired the migStgLk and found the
 * BFA_SWITCH_STG flag set.   
 *
 *
 * Parameters:
 * 
 * Return Values:
 *      EOK - Success
 *
 * Entry/Exit Conditions:
 *
 * On Entry, the migStgLk for bfap should be held for write.
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_migrate_switch_stg_full(
        bfAccessT        *bfap,              /* in */
        ftxHT            parent_ftx )        /* in */
{
  
    statusT sts;
    typedef enum { NO_CHANGE, FRAGMENTED, CONTIGUOUS } fragStateT;
    fragStateT newState = NO_CHANGE;
    int extentCount;

    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL (&bfap->xtnts.migStgLk, 
                    RWL_WRITELOCKED));

    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    bfap->bfaFlags &= ~BFA_SWITCH_STG;
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

    sts = switch_stg (
            bfap,
            &(bfap->xtnts.xtntMap),
            &bfap->xtnts.copyMcellId,
            bfap->xtnts.copyXtntMap,
            parent_ftx );

    if (sts != EOK) {
        ADVSMP_BFAP_LOCK( &bfap->bfaLock);
        bfap->bfaFlags |= BFA_SWITCH_STG_ERR;
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock);
        return sts;
    }


    /*
     * Reset the next valid fob. 
     */
    bfap->xtnts.copyXtntMap->bsxmNextFob = 0;

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_18);

    ADVRWL_XTNTMAP_WRITE( &(bfap->xtntMap_lk) );
    ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);

    /*
     * Once we remove this copy_xtnt_map, the storage will only be
     * referenced by the original migrate thread.  That thread (which may be
     * the context we are running in) will be responsible for deallocating
     * the storage.
     */
    bfap->xtnts.copyXtntMap = NULL;

    /* 
     * The migrate could leave us with 3 possibilities:
     * 1. no state change (stayed fragmented or stayed contiguous)
     * 2. was fragmented, now contiguous
     * 3. was contiguous, now fragmented
     *
     * We need to perform updates for cases 2 or 3.
     */

    if (BS_BFTAG_REG(bfap->tag)) {
        extentCount = ss_xtnt_counter(bfap);
        if ((extentCount==1) && (bfap->bfaFlags & BFA_FRAGMENTED)) {
            newState = CONTIGUOUS;
        } else if ((extentCount>1) && !(bfap->bfaFlags & BFA_FRAGMENTED)) {
            newState = FRAGMENTED;
        }
    }

    if ((newState == CONTIGUOUS) || (newState == FRAGMENTED)) {
        sts = advfs_bs_tagdir_update_tagmap(bfap->tag,
                                            bfap->bfSetp,
                                            BS_TD_FRAGMENTED,
                                            bsNilMCId,
                                            parent_ftx,
                                            (newState == FRAGMENTED) ?
                                            TDU_SET_FLAGS : TDU_UNSET_FLAGS);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        ss_set_fragmented_tag_page(bfap->tag, bfap->bfSetp);
#ifdef SS_DEBUG
        if (ss_debug>0) {
            printf("vfast-%s %s fragmented - migrate -> tag(%ld)\n",
                       bfap->dmnP->domainName, 
                       ((newState == FRAGMENTED) ? "SET" : "CLR"),
                       bfap->tag.tag_num);
        }
#endif
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
        if (newState == FRAGMENTED) {
            bfap->bfaFlags |= BFA_FRAGMENTED;
        } else { /* CONTIGUOUS */
            bfap->bfaFlags &= ~BFA_FRAGMENTED;
        }
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    }

HANDLE_EXCEPTION:

    ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );

    return sts;


}


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
            bsInMemXtntMapT  **origXtntMapAddr,  /* in */
            bfMCIdT          *copyMcellId,       /* in, modified */
            bsInMemXtntMapT  *copyXtntMap,       /* in, modified */
            ftxHT            parentFtxH          /* in */
            )
{
    bfTagT bfSetTag;
    ftxHT ftxH;
    int32_t failFtxFlag = 0;
    bfMCIdT newCopyMcellId;
    bsInMemXtntMapT *newCopyXtntMap = NULL,
                    *newOrigXtntMap = NULL,
                    *origXtntMap;
    bfMCIdT oldCopyMcellId;
    statusT sts;

    bfSetTag = bfap->bfSetp->dirTag;

    oldCopyMcellId = *copyMcellId;

    sts = FTX_START_N(FTA_NULL, &ftxH, parentFtxH, bfap->dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    FTX_LOCKWRITE(&(bfap->mcellList_lk), ftxH);

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
     * The mcell specified by oldCopyVdIndex and oldCopyMcellId is a 
     * pseudo-primary mcell currently pointing to the mcells described in 
     * copyXtntMap.  Separate the pseudo-primary mcell from the extents; 
     * overlay_xtnt_map() will get only the extents.
     */
    sts = bmt_unlink_mcells ( bfap->dmnP,
                              bfap->tag,
                              oldCopyMcellId,
                              copyXtntMap->subXtntMap[0].mcellId,
                              copyXtntMap->subXtntMap[copyXtntMap->cnt - 1].mcellId,
                              ftxH,
			      CHAINMCID);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    /* Merge copy into orig returning newOrig. newCopy is an extent map */
    /* of the replaced extents from orig. */
    sts = overlay_xtnt_map ( bfap,
                             origXtntMap,
                             copyXtntMap,
                             &newOrigXtntMap,
                             &newCopyXtntMap,
                             ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Free the old pseudo-primary mcell from the copy extent. */
    bmt_free_bf_mcells ( VD_HTOP(oldCopyMcellId.volume, bfap->dmnP),
                         oldCopyMcellId,
                         ftxH,
                         TRUE);

    /* Get a new primary mcell and attach it to the newCopy extent map. */
    sts = create_xtnt_map_hdr ( bfap,
                bfap->bfSetp,
                bfap->tag,
                newCopyXtntMap->subXtntMap[0].mcellId,
                newCopyXtntMap->subXtntMap[newCopyXtntMap->cnt - 1].mcellId,
                ftxH,
                &newCopyMcellId);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Put the chain of mcells descibing newCopy storage on the defered */
    /* delete list. */
    sts = del_add_to_del_list ( newCopyMcellId,
                               VD_HTOP(newCopyMcellId.volume, bfap->dmnP),
                               TRUE,  /* start sub ftx */
                               ftxH);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    ADVRWL_XTNTMAP_WRITE(&(bfap->xtntMap_lk));
    ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);

    /*
     * Update the original's and copy's in-memory maps so they map the
     * switched storage allocation information.  New pages brought into
     * the buffer cache are still mapped to both the original and the copy.
     */

    imm_delete_sub_xtnt_maps (origXtntMap);
    origXtntMap->hdrType = newOrigXtntMap->hdrType;
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
    copyXtntMap->hdrMcellId = newCopyXtntMap->hdrMcellId;
    copyXtntMap->cnt = newCopyXtntMap->cnt;
    copyXtntMap->maxCnt = newCopyXtntMap->maxCnt;
    copyXtntMap->updateStart = copyXtntMap->cnt;
    copyXtntMap->validCnt = copyXtntMap->cnt;
    copyXtntMap->subXtntMap = newCopyXtntMap->subXtntMap;

    newCopyXtntMap->subXtntMap = NULL;
    imm_delete_xtnt_map (newCopyXtntMap);

    ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
    ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
 
    failFtxFlag = 0;
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_235);
    ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );

    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_236);
    ftx_done_u (ftxH, FTA_BS_MIG_MIGRATE_V1, 0, 0);
    ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_237);

    *copyMcellId = newCopyMcellId;

    return EOK;

HANDLE_EXCEPTION:

    if (failFtxFlag != 0) {
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_410);
        ftx_fail (ftxH);
        ADVFS_CRASH_RECOVERY_TEST(ftxH.dmnP, AdvfsCrash_411);
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
    int32_t failFtxFlag         = 0;
    int32_t vdRefed             = FALSE;
    int32_t unlock_snapshots    = FALSE;
    int32_t move_metadata_error = FALSE;
    void * unlock_hdl;

    ftxHT ftxH;
    statusT sts;
    struct vnode *nullvp = NULL;

    if (BS_BFTAG_RSVD (bfap->tag)) {
        RAISE_EXCEPTION (ENOT_SUPPORTED);
    }

    if(vdp == NULL ) {
        sts = sc_select_vd_for_mcell (
                                      &vdp,
                                      bfap->dmnP,
                                      bfap->dmnP->scTbl,
                                      bfap->reqServices
                                      );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        vdRefed = TRUE;
    }

    sts = FTX_START_EXC(FTA_NULL, &ftxH, bfap->dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    bfSet = bfap->bfSetp;


    if (!HAS_SNAPSHOT_CHILD(bfap) || BS_BFTAG_RSVD(bfap->tag)) {
        /* No children snapshots could exist. So just go ahead and migrate
         * the metadata. */
        if (bfap->bfaFlags & BFA_VIRGIN_SNAP) {
            /* This is a child snapshot sharing it's parent's metadata.
             * Nothing to do here */
            ftx_quit (ftxH);
        } else {
            /* Migrate the metadata.  */
            FTX_LOCKWRITE(&(bfap->mcellList_lk), ftxH);

            sts = move_metadata (bfap, vdp->vdIndex, ftxH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            failFtxFlag = 0;
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_238);
            ftx_done_n (ftxH, FTA_BS_MIG_MOVE_METADATA_EXC_V1);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_239);
        }
    } else { 
        /* If we may have snapshots and we aren't a reserved file, and the
         * BFA_SNAP_CHANGE flag is set or we haven't loaded any snapshot
         * children, try to lock their mcellList locks.  Call
         * advfs_snap_migrate_setup to do this.  It will return with
         * the mcell list lock held for all decendent snapshots that share a
         * common mcell with this parent.  bfap's mcellList lock will also
         * be held for write on return.
         */
        if (bfap->bfaFlags & BFA_VIRGIN_SNAP) {
            /* This is has a snapshot child but it doesn't have it's own
             * metadata so there is still nothing to do.
             */
            ftx_quit(ftxH);
        } else {
            /*
             * Acquire the BFA_SNAP_LOCKS and mcellList_lk for all children
             * that share a common mcell with bfap.  The unlock handle will
             * be used to request the locks be dropped without having to
             * know which children snapshots were actually accessed.
             */
            sts = advfs_snap_migrate_setup( bfap, &unlock_hdl, ftxH );
            unlock_snapshots = TRUE;
            MS_SMP_ASSERT( sts == EOK);

            ADVFS_ACCESS_LOGIT( bfap, "bs_move_metadata moving bfap");
            
            sts = move_metadata (bfap, vdp->vdIndex, ftxH);
            if (sts != EOK) {
                move_metadata_error = TRUE;
                RAISE_EXCEPTION (sts);
            }

            failFtxFlag = 0;
            
            if (unlock_snapshots) {
                /* In addition to unlocking the snapshots, we need to update the
                 * primMCId pointer since that has changed (the parent's
                 * metadata was migrated to a new mcell).   Since we have been holding the
                 * bfa_snap_lock for write, none of the snapshots could have gotten
                 * their own metadata.  Note that any mcellList_lk's were dropped
                 * by the ftx_quit or ftx_done above... */
                sts = advfs_snap_migrate_done( bfap, unlock_hdl, bfap->primMCId,
                                               ftxH, move_metadata_error );
                MS_SMP_ASSERT( sts == EOK );
            }
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_240);
            ftx_done_n (ftxH, FTA_BS_MIG_MOVE_METADATA_EXC_V1);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_241);

        }
    } 



    if(vdRefed == TRUE)
        vd_dec_refcnt(vdp);

    return EOK;

HANDLE_EXCEPTION:

    if(vdRefed == TRUE) {
        vd_dec_refcnt(vdp);
    }

    if (failFtxFlag != 0) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_412);
        ftx_fail (ftxH);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_413);
    }

    if (unlock_snapshots) {
        /* In addition to unlocking the snapshots, we need to update the
         * primMCId pointer since that has changed (the parent's metadata
         * was migrated to a new mcell).   Since we have been holding the
         * bfa_snap_lock for write, none of the snapshots could have gotten
         * their own metadata.  Note that any mcellList_lk's were dropped
         * by the ftx_quit or ftx_done above... */
        sts = advfs_snap_migrate_done( bfap, unlock_hdl, bfap->primMCId, 
                                       ftxH, move_metadata_error );
        MS_SMP_ASSERT( sts == EOK );
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
 * snapshots' that share metadata.  Also, exclusive access to each bitfile's tag directory
 * entry.
 *
 * NOTE:  When this function returns a status value of EOK, the caller cannot
 * fail the parent transaction as this function cannot undo its in-memory
 * modifications.
 */

static
statusT
move_metadata (
               bfAccessT *bfap,  /* in */
               vdIndexT newVdIndex,  /* in */
               ftxHT parentFtxH  /* in */
               )
{
    bsMPgT *bmt;
    domainT *domain;
    ftxHT ftxH;
    int32_t derefPageFlag = 0,
            failFtxFlag   = 0,
            delFlag;
    bsMCT *mcell;
    bsBfAttrT *newAttrRec,
              *origAttrRec;
    bsXtntRT *newXtntRec,
             *origXtntRec;
    bfMCIdT newMcellId;
    ftxHT parentFtx;
    rbfPgRefHT pgPin;
    bfPageRefHT pgRef;
    statusT sts;
    tagInfoT tagInfo;
    vdT *vd;
    bsInMemXtntMapT *xtntMapp = NULL;

    domain = bfap->dmnP;

    /*
     * Get the original's attribute record and primary extent record.
     * We assume that the records are in the primary mcell.
     */

    vd = VD_HTOP(bfap->primMCId.volume, domain);
    sts = bs_refpg(&pgRef,
                   (void*)&bmt,
                   vd->bmtp,
                   bfap->primMCId.page,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
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
    if (origXtntRec->xCnt > 1) {
        RAISE_EXCEPTION(E_STG_ADDED_DURING_MIGRATE_OPER);
    }

    /*
     * Copy the original attribute and primary extent records
     * to the new.  They are not copied by advfs_setup_cow().
     *
     * If a record is missing, we still want to copy this mcell and
     * mcells linked to it.
     */

    sts = FTX_START_N(FTA_NULL, &ftxH, parentFtxH, bfap->dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    sts = bmt_alloc_mcell (
                           bfap,
                           newVdIndex,
                           BMT_NORMAL_MCELL,
                           mcell->mcBfSetTag,
                           mcell->mcTag,
                           mcell->mcLinkSegment,
                           ftxH,
                           &newMcellId,
                           FALSE
                           );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    MS_SMP_ASSERT( FETCH_MC_VOLUME(newMcellId) == newVdIndex);
    if ((origAttrRec != NULL) || (origXtntRec != NULL)) {

        vd = VD_HTOP(newMcellId.volume, domain);
        sts = rbf_pinpg (
                         &pgPin,
                         (void *)&bmt,
                         vd->bmtp,
                         newMcellId.page,
                         BS_NIL,
                         ftxH,
                         MF_VERIFY_PAGE
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
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_242);
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

            MS_SMP_ASSERT(origXtntRec->xCnt == 1);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_243);
        }
    }

    failFtxFlag = 0;
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_244);
    ftx_done_n (ftxH, FTA_BS_MIG_MOVE_METADATA_V1);
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_245);

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

        sts = advfs_bs_tagdir_update_tagmap(
                bfap->tag,
                bfap->bfSetp,
                BS_TD_NO_FLAGS,
                newMcellId,
                parentFtxH,
                TDU_SET_MCELL);
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
                                        VD_HTOP(bfap->primMCId.volume, domain),
                                        1,  /* start sub-transaction */
                                        parentFtxH
                                        );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        sts = del_add_to_del_list (
                                   newMcellId,
                                   VD_HTOP(newMcellId.volume, domain),
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

    sts = advfs_snap_copy_mcell_chain(
                           bfap->primMCId,
                           newMcellId,
                           domain,
                           parentFtxH
                           );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    bmt_free_bf_mcells (
                        VD_HTOP(bfap->primMCId.volume, domain),
                        bfap->primMCId, 
                        parentFtxH,
                        FALSE       /* no del list remove */
                        );

    /*
     * If we just changed primary mcells for a file that support extent
     * information in the primary mcell, update the in-memory extent
     * map if needed.  Generally, this won't be necessary as rmvol will
     * migrate extent information first but if the primary mcell has no
     * extent information in it, rmvol will not move it during the
     * migration of the other extents and we must update it here.
     */
    xtntMapp = bfap->xtnts.xtntMap;
    ADVRWL_XTNTMAP_WRITE(&(bfap->xtntMap_lk));
    ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);
    if ( !MCID_EQL(newMcellId, xtntMapp->hdrMcellId) ) {
        MS_SMP_ASSERT( MCID_EQL(xtntMapp->hdrMcellId,xtntMapp->subXtntMap[0].mcellId) );
        MS_SMP_ASSERT(xtntMapp->subXtntMap[0].cnt == 1);
        xtntMapp->hdrMcellId = xtntMapp->subXtntMap[0].mcellId = newMcellId;
        ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
        ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));
        
    } else {
        ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
        ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));
    }

    bfap->primMCId = newMcellId;

    return EOK;

HANDLE_EXCEPTION:

    if (failFtxFlag != 0) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_414);
        ftx_fail (ftxH);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_415);
    }
    if (derefPageFlag != 0) {
        (void) bs_derefpg (pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* move_metadata */

/*
 * mig_alloc_copy_stg
 *
 * This function allocates storage for the copy.  The copy's extent map is
 * created and initialized and the storage info is inserted onto the delete
 * pending list.
 */

static
statusT
mig_alloc_copy_stg (
                bfAccessT *bfap,                /* in   get stg for this file */
                bf_fob_t start_fob,             /* in   start map at this fob */
                extent_blk_desc_t *fob_range,   /* in   map of extents */
                vdIndexT vdIndex,               /* in   get stg on this volume */
                bf_vd_blk_t dstBlkOffset, /* in;if hint & BS_ALLOC_MIG_RSVD,make sure we get this blk*/
                bf_fob_t minimum_contiguous_fobs, /* in */
                bsAllocHintT alloc_hint,        /* in */
                bfMCIdT *copyMcellId,           /* ou  starting mcell */
                bsInMemXtntMapT **copyXtntMap,  /* out  return this map */
                ftxHT   parent_ftx              /* in -- used for snapshot metadata migrates */
                )
{
    bf_fob_t alloc_fob_cnt;
    uint32_t i;
    statusT sts = EOK;
    bsInMemXtntMapT *xtntMap = NULL;
    vdT *vdp = NULL;

    MS_SMP_ASSERT(start_fob <= ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_offset));

    sts = imm_create_xtnt_map( 
                               bfap->dmnP,
                               1,  /* maxCnt */
                               &xtntMap
                             );
    if ( sts != EOK ) {
        RAISE_EXCEPTION (sts);
    }

    if ( vdIndex == (vdIndexT)(-1) ) {
        while (fob_range != NULL) {
            /*
             * Skip holes.
             */
            if (!ADVFS_HOLE(fob_range)) {
                sts = cp_stg_alloc_from_svc_class( 
                               bfap,
                               ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_offset),
                              ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_byte_cnt),
                               minimum_contiguous_fobs,
                               fob_range->ebd_vd_blk,
                               xtntMap,
                               start_fob,
                               alloc_hint,
                               parent_ftx);
                if ( (sts != EOK) && (sts != EAGAIN) ) {
                    RAISE_EXCEPTION (sts);
                } else if (sts == EAGAIN) {
                    goto finish_and_return;
                }
            }
            fob_range = fob_range->ebd_next_desc;
        }
    } else {
        vdp = vd_htop_already_valid(vdIndex, bfap->dmnP, TRUE);
        while (fob_range != NULL) {
            /*
             * Skip holes.
             */
            if (!ADVFS_HOLE(fob_range)) {
                sts = cp_stg_alloc_from_one_disk( 
                               bfap,
                               vdIndex,
                               ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_offset),
                              ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_byte_cnt),
                               minimum_contiguous_fobs,
                               fob_range->ebd_vd_blk,
                               xtntMap,
                               start_fob,
                               &alloc_fob_cnt,
                               dstBlkOffset,
                               alloc_hint,
                               parent_ftx);
                if ( (sts != EOK) && (sts != EAGAIN) ) {
                    RAISE_EXCEPTION (sts);
                }
                /*
                 * EAGAIN means that we couldn't add any more storage
                 * because of transaction reasons, not because of out of
                 * space conditions.  In this case, we will return to the
                 * caller with the extent maps for the allocated storage and
                 * let the caller make another call in.
                 */
                if ( (alloc_fob_cnt != ADVFS_OFFSET_TO_FOB_DOWN(fob_range->ebd_byte_cnt) ) &&
                     (sts != EAGAIN) ) {
                    RAISE_EXCEPTION (ENO_MORE_BLKS);
                } else if (sts == EAGAIN) {
                    goto vd_decr_finish_and_return;
                }
            }
            fob_range = fob_range->ebd_next_desc;
        }
vd_decr_finish_and_return:
        vd_dec_refcnt(vdp);
    }

finish_and_return:


    *copyMcellId = xtntMap->hdrMcellId;
    *copyXtntMap = xtntMap;

    return sts;

HANDLE_EXCEPTION:
    if ( vdp != NULL ) {
        vd_dec_refcnt(vdp);
    }

    /*
     * The caller must be responsible for deallocating storage in
     * copyMcellId on error. 
     */

    if ( xtntMap != NULL ) {
        *copyMcellId = xtntMap->hdrMcellId;
        imm_delete_xtnt_map( xtntMap);
    }

    return sts;
}


/*
 * If parent_ftx is not FtxNilFtxH, then this routine cannot done the
 * transaction and start a new root.  Instead, this routine should as as
 * much storage as it can and return.  Migrate will call back in as
 * necessary.
 */
static
statusT
mig_alloc_hole_stg (
                bfAccessT *bfap,                /* in */
                extent_blk_desc_t *hole_range,  /* in */
                vdIndexT vdIndex,               /* in */
                bfMCIdT *copyMcellId,           /* out */
                bsInMemXtntMapT **copyXtntMap,  /* out */
                ftxHT   parent_ftx)            
{
    bfTagT bfSetTag;
    bfMCIdT delMcellId;
    ftxHT ftxH;
    int32_t  failFtxFlag = 0,
              vdRefFlg   = FALSE;
    bfMCIdT newMcellId;
    bf_fob_t fob_cnt;
    statusT sts;
    bsInMemXtntMapT *xtntMap = NULL;
    uint32_t i,
             newtype,
             newmax;
    vdT *vdp;

    bfSetTag = bfap->bfSetp->dirTag;

    sts = FTX_START_N(FTA_NULL, &ftxH, parent_ftx, bfap->dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    failFtxFlag = 1;

    sts = imm_create_xtnt_map (
                               bfap->dmnP,
                               1,  /* maxCnt */
                               &xtntMap
                              );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    if ( vdIndex == (vdIndexT)(-1) ) 
    {
        /* We need an MCELL on a disk in the service class
         * so pick one */

        sts = sc_select_vd_for_mcell( &vdp,
                                      bfap->dmnP,
                                      bfap->dmnP->scTbl,
                                      bfap->reqServices);
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
    MS_SMP_ASSERT(vdIndex == FETCH_MC_VOLUME(newMcellId));
    MS_SMP_ASSERT(newMcellId.volume != VOL_TERM);

    /*
     * If this file can have extent information in the primary mcell,
     * make the first subextent map type BSR_XTRA_XTNTS.  Otherwise, make
     * it BSR_SHADOW_XTNTS.
     */
    newtype = BSR_XTRA_XTNTS;
    newmax = BMT_XTRA_XTNTS;
    sts = imm_init_sub_xtnt_map (
                                 &(xtntMap->subXtntMap[0]),
                                 ADVFS_OFFSET_TO_FOB_DOWN(hole_range->ebd_offset),
                                 ADVFS_OFFSET_TO_FOB_DOWN(hole_range->ebd_byte_cnt),
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

    xtntMap->subXtntMap[0].bsXA[0].bsx_vd_blk = hole_range->ebd_vd_blk;

    xtntMap->subXtntMap[0].bsXA[1].bsx_fob_offset= ADVFS_OFFSET_TO_FOB_DOWN(hole_range->ebd_byte_cnt) + 
                                            ADVFS_OFFSET_TO_FOB_DOWN(hole_range->ebd_offset);

    xtntMap->subXtntMap[0].bsXA[1].bsx_vd_blk = XTNT_TERM;
    xtntMap->subXtntMap[0].cnt++;

    xtntMap->updateStart = xtntMap->validCnt;

    /*
     * Save to disk the storage allocation information.
     */

    for (i = 0; i < xtntMap->cnt; i++) {
        sts = odm_create_xtnt_rec (
                                   bfap,
                                   bfap->bfSetp,
                                   &(xtntMap->subXtntMap[i]),
                                   ftxH
                                   );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }  /* end for */

    fob_cnt = (xtntMap->subXtntMap[xtntMap->cnt - 1].bssxmFobOffset+
               xtntMap->subXtntMap[xtntMap->cnt - 1].bssxmFobCnt) -
               xtntMap->subXtntMap[0].bssxmFobOffset;
    sts = odm_create_xtnt_map (
                               bfap,
                               bfap->bfSetp,
                               bfap->tag,
                               xtntMap,
                               ftxH,
                               &delMcellId
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    sts = del_add_to_del_list (
                               delMcellId,
                               VD_HTOP(delMcellId.volume, bfap->dmnP),
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

    /*
     * If this transaction fails, it wil be cleaned up by the ddl.
     */
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_246);
    ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_247);
    ftx_done_u (ftxH, FTA_BS_MIG_MIGRATE_V1, 0, 0);
    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_248);

    *copyMcellId = delMcellId;
    *copyXtntMap = xtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if ( vdRefFlg ) {
        vd_dec_refcnt(vdp);
    }
    if (failFtxFlag != 0) {
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_416);
        ftx_fail (ftxH);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_417);
    }
    if (xtntMap != NULL) {
        imm_delete_xtnt_map (xtntMap);
    }
    return sts;

}  /* end alloc_hole_stg */



/*
 * mig_pack_vd_range
 * First determine the extents in the clear range.  Next,
 * pack the extents in the input range to the far left of the range.
 * This clears a contiguous range of free storage producing a single extent
 * large enough to hold the defragmented file extent.
 *
 */

statusT
mig_pack_vd_range (
             vdT *vdp,                          /* in */
             bf_vd_blk_t clr_range_begin_blk,   /* in */
             bf_vd_blk_t clr_range_end_blk,     /* in */
             bf_vd_blk_t *new_clr_range_blk,    /* out */
                /* new_clr_range_blk is the blk that the clr range begins at 
                 * after the packing is complete.  At completion, the range
                 * new_clr_range_blk..clr_range_end_blk is free (usually) */
             uint32_t  forceFlag                 /* in */
             )
{
    statusT sts=EOK,sts2=EOK;
    domainT *domain = NULL;
    bsInMemXtntMapT *inwayXtntMap = NULL;
    int32_t  closeFileFlag = FALSE, skip, sbm_locked=FALSE;
    uint32_t i,
             k,
             bmt_bit_cnt = 0;
    uint64_t sbm_bit_cnt = 0; 
    bfSetT *inwayBfSetp = NULL;
    bfSetIdT inwayBfSetId;
    bfAccessT *inwayBfap = NULL;

    bf_vd_blk_t nextcRangeBeginBlk,
                newBlkOffset,
                dstBlkOffset=0;

    bf_fob_t xtnt_map_fob,
             min_fobs_required,
             mig_fob_cnt_so_far,
             mig_fob_cnt,
    /* thd_blk_mig_fob_cnt is used to control when we block to let other
     * work be done.  When we migrate more than thd_blk_mig_fob_cnt, we
     * close the file and go to sleep for a while. */
             thd_blk_mig_fob_cnt = 0,
             fobs_remaining,
             new_fob_cnt,
             clear_fob_cnt;

    ssPackHdrT   *php=NULL;
    ssPackLLT *pXtntp=NULL;
    int32_t another_scan_required;
    extern bf_fob_t SSFobCnt;
    bfMCIdT mcellId;
    int32_t delFlag,
            fsetMounted;

    MS_SMP_ASSERT(vdp);
    *new_clr_range_blk = 0;
    nextcRangeBeginBlk = dstBlkOffset = clr_range_begin_blk;

    /* optimizer to avoid calling costly bmt scan if there is already
     * nothing in way
     */
    sts = sbm_scan(vdp, clr_range_begin_blk, clr_range_end_blk, &sbm_bit_cnt);
    if(sts != EOK) {
        VFAST_STATS(mig_pack_vd_range_sbm_scan_err);
        return sts;
    }
    SS_TRACE(vdp,clr_range_begin_blk, clr_range_end_blk, sbm_bit_cnt, 0);

    if (sbm_bit_cnt) {
        /* some extents are already allocated in our chosen range. */
        domain = vdp->dmnP;
        inwayBfSetId.domainId = domain->domainId;
        nextcRangeBeginBlk = dstBlkOffset = clr_range_begin_blk;
        another_scan_required = TRUE;

        /* loop, repeatedly scanning the bmt and rbmt, finding those extents
         * that are lying in the clear range passed in.
         */
        while(another_scan_required) {

        /* V5 ONDISK FIX -> call sbm_get_vd_bf_inway for new v5 sbm instead
         * of bmt_get_vd_bf_inway
         */

            /* find tags and fobs for all files in range to be packed */
            bmt_bit_cnt=0;
            sts = bmt_get_vd_bf_inway( vdp,     /* puts list on here */
                                   nextcRangeBeginBlk,  /* new scan start */
                                   clr_range_end_blk,    /* scan end */
                                   &another_scan_required,  /* TRUE, FALSE */
                                   &bmt_bit_cnt /* used for clear range verify */
                                 );
            if (sts != EOK) {
                VFAST_STATS(mig_pack_vd_range_bmt_get_vd_bf_inway_err);
                goto _cleanup;
            }

            /* loop through the files with extents in the range we
             * want to pack and migrate the fobs in sequence from
             * right to left in the area to be cleared.
             */

            php = &vdp->ssVolInfo.ssPackHdr;
            pXtntp = php->ssPackFwd;
            if((vdp->ssVolInfo.ssPackHdr.ssPackListCnt > 0) &&
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
                if(sts == ENO_SUCH_TAG) {
                    /* File was deleted, or being deleted.
                     * Find out which one.
                     */
                    ADVRWL_DDLACTIVE_WRITE( &(vdp->ddlActiveLk) );
                    sts = del_find_del_entry ( vdp->dmnP,
                                               vdp->vdIndex,
                                               pXtntp->ssPackBfSetId.dirTag,
                                               pXtntp->ssPackTag,
                                               &mcellId,
                                               &delFlag);
                    ADVRWL_DDLACTIVE_UNLOCK( &(vdp->ddlActiveLk) );

                    if (sts == EOK) {
                        /* found on ddl, may be there a long time, abort */
                        sts = ENO_MORE_MCELLS;
                        VFAST_STATS(mig_pack_vd_range_no_more_mcells);
                     
                        goto _cleanup;
                    } else if(sts == ENO_MORE_MCELLS) {
                        /* File was deleted, ok */
                        sts = EOK;
                        pXtntp = pXtntp->ssPackFwd;  /* Keep going! */
                        continue;
                    } else {
                        VFAST_STATS(mig_pack_vd_range_del_find_del_entry_err);
                        goto _cleanup;
                    }
                }
                if (sts != EOK) {
                    VFAST_STATS(mig_pack_vd_range_ss_open_file_err);
                    goto _cleanup;
                }
                closeFileFlag = TRUE;

                /* move extent to left an allocation unit at a time , for now */
                mig_fob_cnt_so_far = mig_fob_cnt = 0;
                xtnt_map_fob = pXtntp->ssPackFobOffset;
                inwayXtntMap = inwayBfap->xtnts.xtntMap;
                new_fob_cnt = pXtntp->ssPackFobCount;

                for(i = pXtntp->ssPackFobCount; i > 0 ; i-- ) {

                    sts = imm_get_alloc_fob_cnt( inwayXtntMap,
                                                  xtnt_map_fob,
                                                  i,
                                                  &clear_fob_cnt);
                    /* if E_RANGE_NOT_MAPPED, then part of extent has
                     * been truncated. Find the new end of the extent.
                     */
                    if(sts != E_RANGE_NOT_MAPPED)
                        break;
                } /* end for */

                if((i > 0) && (sts == EOK)) {
                    new_fob_cnt = i;
                } else {
                    if(sts != E_RANGE_NOT_MAPPED) {
                        VFAST_STATS(mig_pack_vd_range_range_not_mapped);
                        goto _cleanup;
                    }
                    /* no pages left in xtnt - continue on to next xtnt */
                    break;  /* out of switch */
                }

                while (mig_fob_cnt_so_far < new_fob_cnt) {

                    if (thd_blk_mig_fob_cnt > SSFobCnt) {
                        /* Close the fileset and bfap before parking so
                         * that the fileset can be deleted if needed.
                         * We don't want to hold up the delete while
                         * waiting for some idle time to run.
                         */

                        sts = ss_close_file( inwayBfSetp,
                                             inwayBfap,
                                             fsetMounted );
                        if (sts != EOK) {
                            VFAST_STATS(mig_pack_vd_range_ss_close_file_err);
                            goto _cleanup;
                        }
                        closeFileFlag = FALSE;

                        /* wait for next run approval to continue */
                        sts = ss_block_and_wait(vdp);
                        if(sts != EOK) {
                            VFAST_STATS(mig_pack_vd_range_ss_block_and_wait_err);
                            goto _cleanup;
                        }
                        /* reset fob counter */
                        thd_blk_mig_fob_cnt = 0;
                        sts = ss_open_file( pXtntp->ssPackBfSetId,
                                            pXtntp->ssPackTag,
                                            &inwayBfSetp,
                                            &inwayBfap,
                                            &fsetMounted);
                        if(sts == ENO_SUCH_TAG) {
                            VFAST_STATS(mig_pack_vd_range_ss_open_file_err2);
                            /* File was deleted, or being deleted.
                             * Find out which one.
                             */
                            ADVRWL_DDLACTIVE_WRITE( &(vdp->ddlActiveLk) );
                            sts = del_find_del_entry (
                                               vdp->dmnP,
                                               vdp->vdIndex,
                                               pXtntp->ssPackBfSetId.dirTag,
                                               pXtntp->ssPackTag,
                                               &mcellId,
                                               &delFlag);
                            ADVRWL_DDLACTIVE_UNLOCK( &(vdp->ddlActiveLk) );

                            if (sts == EOK) {
                                /* on ddl, may be there a long time, abort */
                                sts = ENO_MORE_MCELLS;
                                VFAST_STATS(mig_pack_vd_range_no_more_mcells2);
                                goto _cleanup;
                            } else if(sts == ENO_MORE_MCELLS) {
                                /* File was deleted, ok */
                                sts = EOK;
                                break;  /* break to next list entry */
                            } else {
                                VFAST_STATS(mig_pack_vd_range_del_find_del_entry_err2);
                                goto _cleanup;
                            }
                        }
                        if (sts != EOK) {
                            VFAST_STATS(mig_pack_vd_range_ss_open_file_err2);
                            goto _cleanup;
                        }
                        closeFileFlag = TRUE;
                    }

                    fobs_remaining = pXtntp->ssPackFobCount - mig_fob_cnt_so_far;
                          

                    newBlkOffset = pXtntp->ssPackStartXtBlock +
                                   (mig_fob_cnt_so_far / ADVFS_FOBS_PER_DEV_BSIZE);

                    /* number of fobs allowed to be migrated are
                     * equal to the number of free blocks between
                     * the last destination blk offset and the
                     * start of this extent.  This is allowed
                     * because the extents are ordered by block
                     * number in the pack list and it is operated
                     * on in the same order - from left to right
                     */
                    if((newBlkOffset - dstBlkOffset) > 0)
                        mig_fob_cnt = (newBlkOffset - dstBlkOffset) *
                                          ADVFS_FOBS_PER_DEV_BSIZE;
                                                    
                    else {
                        /* There is no room on left to pack this extent
                         * into. The sparse range must not have started
                         * on an unused fob .  Abort because sparse range
                         * is probably defective due to a bad sbm_scan_v3_v4().
                         */
                        sts = E_RANGE_NOT_CLEARED;
                        goto _cleanup;
                    }
                    /*
                     * The packing algorithm assumes that we will be able
                     * to pack the data perfectly to the left portion of
                     * the packing range.  The existing Tru64 code handles
                     * the case where we might have attempted an overlapping
                     * write, by setting mig_fob_cnt to be no greater than
                     * the newBlkOffset - dstBlkOffset.  Variable allocation
                     * units can cause a case where the packing algorithm
                     * might attempt an overlapping write within an allocation
                     * unit, which is erroneous.  In order to solve this
                     * problem, when this case is detected, we temporarily
                     * migrate 1 allocation unit of data outside of the
                     * pack range and then immediately migrate it to the
                     * original target range.  Small files adds a slight
                     * complication to this in that if the offset is less
                     * than VM_PAGE_SZ, the file can be migrated in
                     * allocation units (1k or 2k), but if the offset
                     * to be migrated is greater than VM_PAGE_SZ, then
                     * the migration must occur in VM_PAGE_SZ chunks.
                     */
                    min_fobs_required = inwayBfap->bfPageSz;
                    /* 
                     * Handle small file case.
                     */
                    if (min_fobs_required * ADVFS_FOB_SZ < VM_PAGE_SZ &&
                        xtnt_map_fob * ADVFS_FOB_SZ >= VM_PAGE_SZ) {
                        min_fobs_required = VM_PAGE_SZ / ADVFS_FOB_SZ;
                        VFAST_STATS(mig_pack_vd_range_small_file_force_vmpagesz);
                    }
                    /*
                     * Too small an amount of space to directly migrate to
                     * without breaking our rule of no overlapping writes
                     * or the rule related to small files.
                     */
                    if (mig_fob_cnt < min_fobs_required) {
                        bf_vd_blk_t tmpBlkOffset = NULL;
                        bf_fob_t tmp_fob_cnt;
                        stgDescT *stg = NULL;
                        VFAST_STATS(mig_pack_vd_range_temp_migrate);
                        mig_fob_cnt = min_fobs_required;
                        /*
                         * get space for mig_fob_cnt.
                         */
                        ADVFTM_VDT_STGMAP_LOCK(&vdp->stgMap_lk);
                        sbm_locked = TRUE;

                        stg = sbm_find_space( vdp,
                                              mig_fob_cnt, /* request */
                                              mig_fob_cnt, /* minimum amt. */
                                              (uint64_t)0, /* dest offset */
                                              BS_ALLOC_MIG_SINGLEXTNT,
                                              &tmpBlkOffset,
                                              &tmp_fob_cnt);
                        if ( (stg == NULL) || (tmp_fob_cnt < mig_fob_cnt) ) {
                            VFAST_STATS(mig_pack_vd_range_temp_migrate_nospace);
                            goto _cleanup;
                        }
                        sts = sbm_lock_range (vdp, tmpBlkOffset, mig_fob_cnt);
                        if (sts != EOK) {
                            VFAST_STATS(mig_pack_vd_range_temp_migrate_nolock);
                            goto _cleanup;
                        }
                        ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk);
                        sbm_locked = FALSE;
                        /*
                         * Temporarily migrate outside of our range.
                         * The normal code will migrate this data to the
                         * dest offset without performing an overlapping write.
                         */
                        sts = migrate_normal (
                                              inwayBfap,
                                              xtnt_map_fob,
                                              mig_fob_cnt,
                                              vdp->vdIndex,
                                              tmpBlkOffset,
                                              forceFlag,
                                              BS_ALLOC_MIG_RSVD 
                                         );
                        if (sts != EOK) {
                            VFAST_STATS(mig_pack_vd_range_temp_migrate_normal_err);
                            (void)sbm_lock_unlock_range (vdp, 0, 0);
                            goto _cleanup;
                        }

                    } else {
                        /* pack the number allowed from above or the
                         * number remaining
                         */
                        mig_fob_cnt = MIN(mig_fob_cnt, fobs_remaining) ;
                        /* pack only the number allowed by smartstore */
                        mig_fob_cnt = MIN(mig_fob_cnt, SSFobCnt);
                    }

                    SS_TRACE(vdp,inwayBfap->tag.num, xtnt_map_fob,
                                 mig_fob_cnt, dstBlkOffset);
                    sts = migrate_normal (
                                          inwayBfap,
                                          xtnt_map_fob,
                                          mig_fob_cnt,
                                          vdp->vdIndex,
                                          dstBlkOffset,
                                          forceFlag,
                                          BS_ALLOC_MIG_RSVD /* override lock */
                                         );

                    if(sts != EOK)  {
                        /* ENO_MORE_BLKS indicates the reserved
                         *               space was stolen by a needy
                         *               user application file. -abort
                         * E_RANGE_NOT_MAPPED indicates that part or
                         *               all of the file
                         *               range being migrated was
                         *               truncated before we could
                         *               migrate it. - abort.
                         * ENO_SUCH_TAG - will not occur here because we
                         *                own the access structure.
                         */
                        VFAST_STATS(mig_pack_vd_range_temp_migrate_normal_err2);
                        goto _cleanup;
                    }

                    /* fobs migrated since last block. */
                    thd_blk_mig_fob_cnt += mig_fob_cnt;
                    /* new fob offset */
                    xtnt_map_fob += mig_fob_cnt;

                    /* fob migrated so far for xtnt entry*/
                    mig_fob_cnt_so_far += mig_fob_cnt;

                    dstBlkOffset +=  mig_fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE;

                    /* update the stats before parking or exiting */
                    vdp->dmnP->ssDmnInfo.ssPagesConsol += mig_fob_cnt;

                } /* end while fob to migrate */

                if ( closeFileFlag ) {
                    sts = ss_close_file( inwayBfSetp, inwayBfap, fsetMounted );
                    if (sts != EOK) {
                        VFAST_STATS(mig_pack_vd_range_ss_close_file_err2);
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
    sts2 = sbm_scan(vdp, dstBlkOffset, clr_range_end_blk, &sbm_bit_cnt);
    if(sts2 != EOK) {
        VFAST_STATS(mig_pack_vd_range_sbm_scan_err2);
        goto _cleanup;
    }

    if(sbm_bit_cnt) {
        if(vdp->freeMigRsvdStg.start_clust==0) {
            /* space used by system when out of space. - ok */
            ms_printf("mig_pack_vd_range:: sbm_scan 2 vdIndex(%d) start(%ld) \
end(%ld), inuse  sbm_bit_cnt==%d) sts==E_STOLEN_BY_SYSTEM\n",
            vdp->vdIndex,  dstBlkOffset, clr_range_end_blk, sbm_bit_cnt);
            VFAST_STATS(mig_pack_vd_range_space_stolen);
        } else {
            /* The bmt may have been updated after vfast scan in
             * bmt_get_vd_bf_inway. - ok
             */
            ms_printf("mig_pack_vd_range:: sbm_scan 2 vdIndex(%d) start(%ld) \
end(%ld), inuse  sbm_bit_cnt==%d) sts==E_STOLEN_BY_SYSTEM\n",
            vdp->vdIndex,  dstBlkOffset, clr_range_end_blk, sbm_bit_cnt);
            VFAST_STATS(mig_pack_vd_range_space_stolen2);
        }
        sts = E_RANGE_NOT_CLEARED;
        goto _cleanup;
    }
#endif

    *new_clr_range_blk = dstBlkOffset;
    SS_TRACE(vdp,0,0,0,dstBlkOffset);

_cleanup:


    SS_TRACE(vdp,0,0,0,sts);
    ss_dealloc_pack_list(vdp);

    if (sbm_locked) {
        ADVFTM_VDT_STGMAP_UNLOCK(&vdp->stgMap_lk);
    }

    if(closeFileFlag == TRUE) {
        (void) ss_close_file( inwayBfSetp, inwayBfap, fsetMounted );
        closeFileFlag = FALSE;
    }
    return sts;

}  /* end mig_pack_vd_range */

/* end bs_migrate.c */
