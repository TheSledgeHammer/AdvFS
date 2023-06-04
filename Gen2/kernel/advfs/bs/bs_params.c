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
 *      AdvFS 
 *
 * Abstract:
 *
 *      This module contains the get/set params routines
 *
 */

#define ADVFS_MODULE   BS_PARAMS

#include <sys/param.h>
#include <sys/mount.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <bs_extents.h>
#include <bs_inmem_map.h>
#include <bs_ims.h>
#include <fs_dir.h>
#include <sys/vnode.h>
#include <ms_osf.h>
#include <ms_assert.h>
#include <advfs/bs_public_kern.h>
#include <advfs/advfs_syscalls.h>
#include <bs_snapshot.h>
#include <tcr/clu.h>



/*
 * BEGIN_IMS
 *
 * advfs_get_xtnt_map - This routine will return 
 *
 * Parameters:
 *
 * bfap - file to get extents for.
 * start_xtnt - extent to start at (pictures as linear chain of extents).
 * array_size - maximum number of extents to return in xtnts_array
 * xtnts_array - user buffer for returning extent info.
 * xtnt_cnt - Number of extents returned in xtnts_array.
 * fob_cnt - If MPF_FOBCNT_ONLY is set, this is the number of fobs in the
 * merged extent maps (all logically allocated storage for a file including
 * parent storage).  Otherwise, fob_cnt is bfaNextFob.
 * 
 * Return Values:
 * EOK - IOs were issued successfully. 
 * EINVALID_HANDLE - bfap was NULL;
 *
 * Entry/Exit Conditions:
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
advfs_get_xtnt_map(
        bfAccessT*      bfap,           /* in - file */
        int32_t         start_xtnt,     /* in - start extent */      
        int32_t         array_size,     /* in - size of buffer */
        bsExtentDescT   xtnts_array[],  /* in/out - extent buffer */
        int32_t         *xtnt_cnt,      /* out - number of extents copied */
        bf_fob_t        *fob_cnt,       /* out - number of fobs */
        vdIndexT*       alloc_vd_idx,   /* out - next vd to alloc from */
        map_flags_t     map_flags)      /* out - disk on which stg is alloced */
{
    off_t               start_off;
    off_t               end_off;
    extent_blk_desc_t*  bfap_extents;
    extent_blk_desc_t*  cur_extent;
    uint64_t            bfap_extent_cnt;
    int64_t             cur_xtnt_idx;
    statusT             sts = EOK;

    *fob_cnt = 0;
    /*
     * Make sure they don't request just FOBs and just XTNTs
     */
    MS_SMP_ASSERT( !( (map_flags & MPF_XTNT_CNT) && 
                      (map_flags & MPF_FOB_CNT) ) );

    if ( bfap == NULL ) {
        return EINVALID_HANDLE;
    }

    /*
     * If BFA_XTNTS_IN_USE is set, it means that someone is trying to do a
     * COW operation in advfs_getpage on this file.  In order to succeed
     * doing the cow, the caller must revoke the CFS token which this thread
     * must hold to be calling from CFS.  Return an error so the calling
     * thread can drop the token and try again after the COW completes.
     */
    if ( (map_flags & MPF_CFS_CALLER) && (bfap->bfaFlags & BFA_XTNTS_IN_USE) ) {
        return E_PLEASE_RETRY;
    }


    /*
     * Acquire the bfaSnapLock to prevent modifications to the extents while
     * reading them.
     */
    ADVRWL_BFAP_SNAP_READ_LOCK( bfap );

    /*
     * Acquire the extent maps for bfap and any parent snapshots.
     */
    sts = advfs_acquire_xtntMap_locks( bfap, 
                        XTNT_WAIT_OK|X_LOAD_REFERENCE,
                        SF_SNAP_NOFLAGS );

    if (sts != EOK) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
        return sts;
    }


    /*
     * Figure out the start and end of the extents to request
     */
    start_off = 0;
    end_off = bfap->bfaNextFob * ADVFS_FOB_SZ;

    /*
     * If the caller passed a 0 sized array, just return the count of
     * extents.  If, however, the caller passed in MPF_FOB_CNT, go ahead and
     * count the fobs and don't just return the extent count.
     */
    if (((array_size == 0) || (map_flags & MPF_XTNT_CNT)) &&
         !(map_flags & MPF_FOB_CNT) ) {
        extent_blk_desc_t *bfap_extents;


        if ( (map_flags & MPF_CFS_CALLER) && (HAS_SNAPSHOT_CHILD(bfap)) ) {
            /*
             * In a cluster with snapshots, we want to return the count 
             * of extents based on the extents subdivded on COW boundaries.  
             * So, we need to get the entire extents map then subdivide and
             * count. This is a longer process but avoids function shipping
             * direct IO writes to the server.
             */
            sts = advfs_get_blkmap_in_range( bfap,
                                bfap->xtnts.xtntMap,
                                &start_off,
                                end_off - start_off,
                                &bfap_extents,
                                &bfap_extent_cnt,
                                RND_ALLOC_UNIT,
                                EXB_COMPLETE,
                                XTNT_WAIT_OK|XTNT_LOCKS_HELD );

            if (sts == EOK) {
                /*
                 * Now split the extents and count them.
                 */
                sts = advfs_snap_split_extents_for_cow( bfap,
                                        bfap_extents,
                                        &bfap_extent_cnt);

                /* bfap_extents is now split logical based on what has been
                 * COWed to the last child snapshot.  bfap_extent_cnt
                 * reflects those splits. 
                 */
                advfs_free_blkmaps( &bfap_extents );

            }

            
        } else {
            sts = advfs_get_blkmap_in_range( bfap,
                                bfap->xtnts.xtntMap,
                                &start_off,
                                end_off - start_off,
                                &bfap_extents,
                                &bfap_extent_cnt,
                                RND_ALLOC_UNIT,
                                EXB_COMPLETE,
                                XTNT_WAIT_OK|XTNT_NO_MAPS|XTNT_LOCKS_HELD );

        }

        *xtnt_cnt = (int32_t)bfap_extent_cnt;
        if (*xtnt_cnt == 0) {
            *xtnt_cnt = 1;
        }

        advfs_drop_xtntMap_locks( bfap, 
                        SF_SNAP_NOFLAGS );
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );

        /* xtnt_cnt is either correct or there was an error. Either way,
         * just return sts now. */
        return sts;

    }


    /*
     * Get the extents for the entire file.  For now, we will get a list of
     * all extents then skip past the first "start_xtnt" of them.  In the
     * future, this could be optimized for files that are BFA_ROOT_SNAPSHOT
     * files by just counting extents in sub extents then calling
     * advfs_get_blkmap_in_range starting at the offset of the extent we
     * want.  This would be difficult for snapshots since the extent maps
     * are composed on the fly.
     */
    sts = advfs_get_blkmap_in_range( bfap,
                                bfap->xtnts.xtntMap,
                                &start_off,
                                end_off - start_off,
                                &bfap_extents,
                                &bfap_extent_cnt,
                                RND_ALLOC_UNIT,
                                EXB_COMPLETE,
                                XTNT_WAIT_OK|XTNT_LOCKS_HELD );

    if (sts != EOK) {
        *xtnt_cnt = 0;
        advfs_drop_xtntMap_locks( bfap, 
                        SF_SNAP_NOFLAGS );
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );

        return sts;
    }


    if ( (map_flags & MPF_CFS_CALLER) && (HAS_SNAPSHOT_CHILD(bfap)) ) {

        /*
         * Now split the extents and count them.
         */
        sts = advfs_snap_split_extents_for_cow( bfap,
                                        bfap_extents,
                                        &bfap_extent_cnt);

        if (sts != EOK) {
            *xtnt_cnt = 0;
            advfs_drop_xtntMap_locks( bfap, 
                    SF_SNAP_NOFLAGS );
            ADVRWL_BFAP_SNAP_UNLOCK( bfap );
            
            advfs_free_blkmaps( &bfap_extents );
            return sts;
        }

    }



    /* 
     * Now skip past the first "start_xtnt" extents.
     */
    cur_extent = bfap_extents;
    for (cur_xtnt_idx = 0; 
            (cur_xtnt_idx < start_xtnt) && (cur_extent); 
            cur_xtnt_idx++) {
        cur_extent = cur_extent->ebd_next_desc;
    }

    /*
     * If the extents requested are beyond the last extent, return ENO_XTNTS
     */
    if (cur_extent == NULL) {
        advfs_drop_xtntMap_locks( bfap, 
                        SF_SNAP_NOFLAGS );
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );

        if (start_xtnt == 0) {
            /* The file has no storage associated with it.  */
            if (array_size) {
                /*
                 * Return a single extent of size 0 so CFS knows there is a
                 * single subxtnt with no extents.  CFS code assumes this
                 * will happen.
                 */
                MS_SMP_ASSERT( array_size > 0 );
                *fob_cnt = 0;
                *xtnt_cnt = 1;
                xtnts_array[0].bsed_fob_offset = 0;
                xtnts_array[0].bsed_fob_cnt = 0;
                xtnts_array[0].bsed_vol_index = FETCH_MC_VOLUME(bfap->primMCId);
                xtnts_array[0].bsed_vd_blk = XTNT_TERM;
            } else {
                *fob_cnt = 0;
                *xtnt_cnt = 0;
            }
            return EOK;
        } else {
            return ENO_XTNTS;
        }
    }


    /* 
     * cur_extent is either at the end of the list and NULL, or points to
     * the extent we want to start copying from.  Do this loop as long as
     * the number of xtnts is less than the array size, or if MPF_FOB_CNT is
     * set, until the end of the extent list.
     */
    *xtnt_cnt = 0;
    while (cur_extent && 
            ((*xtnt_cnt < array_size) || (map_flags & MPF_FOB_CNT)) ) {
        /*
         * If the caller did not request just fob count, then copy the
         * descriptor into the array.
         */
        if ( !(map_flags & MPF_FOB_CNT) ) {
            xtnts_array[*xtnt_cnt].bsed_fob_offset = 
                ADVFS_OFFSET_TO_FOB_DOWN(cur_extent->ebd_offset);
            xtnts_array[*xtnt_cnt].bsed_fob_cnt =
                ADVFS_OFFSET_TO_FOB_DOWN(cur_extent->ebd_byte_cnt);
            xtnts_array[*xtnt_cnt].bsed_vol_index =
                cur_extent->ebd_vd_index;
            xtnts_array[*xtnt_cnt].bsed_vd_blk = 
                cur_extent->ebd_vd_blk;
            (*xtnt_cnt)++;
        } else if ( (cur_extent->ebd_vd_blk != XTNT_TERM) &&
             (cur_extent->ebd_vd_blk != COWED_HOLE) ) {
            /* This is storage so count the fobs */
            MS_SMP_ASSERT( (cur_extent->ebd_offset %
                    bfap->bfPageSz * ADVFS_FOB_SZ) == 0 );
            *fob_cnt += ADVFS_OFFSET_TO_FOB_DOWN(
                    roundup(cur_extent->ebd_byte_cnt,
                        bfap->bfPageSz * ADVFS_FOB_SZ) );
        }

        /* Advance cur_extent and xtnt_cnt */
        cur_extent = cur_extent->ebd_next_desc;
    }

    if ( !(map_flags & MPF_FOB_CNT) ) {
        *fob_cnt = bfap->bfaNextFob;
    }


    *alloc_vd_idx = bfap->xtnts.xtntMap->allocVdIndex;


    sts = advfs_drop_xtntMap_locks( bfap, 
                        SF_SNAP_NOFLAGS );


    /* 
     * If CFS is our caller, set a flag indicating that they have our
     * extents cached somewhere other than this node.
     */
    if (map_flags & MPF_CFS_CALLER) {
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
        bfap->bfaFlags |= BFA_CFS_HAS_XTNTS;
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    }

    ADVRWL_BFAP_SNAP_UNLOCK( bfap );

    advfs_free_blkmaps( &bfap_extents );

    return EOK;
}


/*
 * bs_get_bf_params
 *
 * Return the parameters for a bitfile.
 *
 * Returns EINVALID_HANDLE, or status frem bmtr_scan or
 * bs_derefpg.
 */


statusT
bs_get_bf_params(
                 bfAccessT *bfap,/* in - bfaccess structure */
                 bfParamsT *bfParams,  /* out - parameters for the bf */
                 int lock              /* in */
                )
{
    statusT sts = EOK;

    /*
     * Check validity of bfap.
     */
    if (bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    *bfParams              = bsNilBfParams;

    /* stuff obtained from the access struct */

    bfParams->tag          = bfap->tag;
    if (!(bfap->bfaFlags & BFA_ROOT_SNAPSHOT)) {
        bfParams->bfpSnapType |= ST_HAS_PARENT;
        if (bfap->bfaFlags & BFA_OUT_OF_SYNC) {
            bfParams->bfpSnapType |= ST_SNAP_OUT_OF_SYNC;
        }
    }
    if (HAS_SNAPSHOT_CHILD(bfap)) {
        bfParams->bfpSnapType |= ST_HAS_CHILD;
    }
    bfParams->bfpOrigFileSize = bfap->bfa_orig_file_size;
    bfParams->bfpRsvdFileSize = bfap->rsvd_file_size;
    bfParams->vdIndex      = FETCH_MC_VOLUME(bfap->primMCId);
    ADVRWL_XTNTMAP_READ( &(bfap->xtntMap_lk) );
    bfParams->bfpBlocksAllocated= bfap->xtnts.bimxAllocFobCnt;
    bfParams->bfpNextFob   = bfap->bfaNextFob;
    ADVRWL_XTNTMAP_UNLOCK( &(bfap->xtntMap_lk) );
    bfParams->bfpPageSize  = bfap->bfPageSz;
    bfParams->cl.reqServices  = bfap->reqServices; 

    /* stuff obtained from the on-disk attributes */

    bfParams->cl.rsvd1        = 0;
    bfParams->cl.rsvd2        = 0;

    return sts;
}

/*
 * bs_set_bf_params
 *
 * set the parameters for a bitfile
 *
 * Returns EINVALID_HANDLE, ENOT_SUPPORTED,
 * E_READ_ONLY, and possible error return from bs_pinpg
 *
 */

statusT
bs_set_bf_params(
                 bfAccessT *bfap,/* in - access structure for bitfile */
                 bfParamsT *bfParams   /* in - parameter list */
                )
{
    statusT sts = EOK;
    bsBfAttrT bfattr;
    bfSetT *bfSetp;
    ftxHT ftxH;
    int ftxStarted = FALSE,
        file_lock_held = FALSE;
    struct fsContext *contextp;
    struct vnode *vp;
    
    /*
     * Check validity of bfap.
     */
    if (bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    /*
     * Fileset must be mounted read/write (Clones and filesets
     * mounted readonly will have M_RDONLY set)
     */
    if ((vp = ATOV(bfap)) && (vp->v_vfsp->vfs_flag & VFS_RDONLY)) {
        RAISE_EXCEPTION(E_READ_ONLY);
    }

    contextp = VTOC(&bfap->bfVnode);
    bfSetp = bfap->bfSetp;

    if (bfSetp->bfSetFlags & BFS_OD_READ_ONLY) {
        /* Probably a snapshots that is read only */
        return E_READ_ONLY;
    }

    ADVRWL_FILECONTEXT_WRITE_LOCK(contextp);
    file_lock_held = TRUE;

    sts = FTX_START_N( FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    ftxStarted = TRUE;

    sts = bmtr_get_rec( bfap, 
                        BSR_ATTR, 
                        (void *) &bfattr, 
                        sizeof( bfattr ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    if (!SC_EQL( bfattr.reqServices, bfParams->cl.reqServices)) { 
        /* We currently only support a single service class.
         */

        bfattr.reqServices = bfParams->cl.reqServices; /* req serv class */
        bfap->reqServices = bfParams->cl.reqServices;  /* req service class */

    }

    sts = bmtr_update_rec( bfap, 
                           BSR_ATTR, 
                           &bfattr,
                           sizeof( bfattr ), 
                           ftxH,
                           0);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    ftx_done_n( ftxH, FTA_BS_SET_BF_PARAMS );
    ftxStarted = FALSE;

    ADVRWL_FILECONTEXT_UNLOCK(contextp);
    file_lock_held = FALSE;

    return EOK;

HANDLE_EXCEPTION:

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    if (file_lock_held) {
        ADVRWL_FILECONTEXT_UNLOCK(contextp);
    }

    return sts;
}

/*
 * bs_get_bf_iparams
 *
 * Return the inheritable parameters for a bitfile.
 *
 * Returns EINVALID_HANDLE, or status frem bmtr_scan or
 * bs_derefpg.
 *
 */


statusT
bs_get_bf_iparams(
                  bfAccessT *bfap, /* in - bf access structure */
                  bfIParamsT *bfIParams, /* out - parameters for the bf */
                  int lock               /* in */
                 )
{
    statusT sts = EOK;
    bsBfInheritAttrT bfiattr;

    /*
     * Check validity of bfap.
     */
    if (bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    sts = bmtr_get_rec( bfap, 
                        BSR_BF_INHERIT_ATTR,
                        (void *) &bfiattr, 
                        sizeof( bfiattr ) );
    if (sts != EOK) {
        return sts;
    }

    bzero( bfIParams, sizeof( bfIParams ) );

    /* stuff obtained from the access struct */

    bfIParams->reqServices  = bfiattr.reqServices; 
    bfIParams->rsvd1 = 0;
    bfIParams->rsvd2 = 0;

    return sts;
}

/*
 * bs_set_bf_iparams
 *
 * set the inheritable parameters for a bitfile
 *
 * Returns EINVALID_HANDLE, ENOT_SUPPORTED,
 * E_READ_ONOY, and possible error return from bs_pinpg
 *
 */


statusT
bs_set_bf_iparams(
                  bfAccessT *bfap, /* in - access structure for bitfile */
                  bfIParamsT *bfIParams  /* in - parameter list */
                 )
{
    statusT sts = EOK;
    bsBfInheritAttrT bfiattr;
    bfSetT *bfSetp;
    int ftxStarted = FALSE;
    ftxHT ftxH;
    struct vnode *vp;
    
    /*
     * Check validity of bfap.
     */
    if (bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    /*
     * Fileset must be mounted read/write (Clones and filesets
     * mounted readonly will have M_RDONLY set)
     */
    if ((vp = ATOV(bfap)) && (vp->v_vfsp->vfs_flag & VFS_RDONLY)) {
        RAISE_EXCEPTION(E_READ_ONLY);
    }

    bfSetp = bfap->bfSetp;

    if (bfSetp->bfSetFlags & BFS_OD_READ_ONLY) {
        /* Probably a snapshots that is read only */
        return E_READ_ONLY;
    }


    sts = FTX_START_N( FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP);
    if (sts != EOK) {
        return sts;
    }
    ftxStarted = TRUE;

    sts = bmtr_get_rec( bfap, 
                        BSR_BF_INHERIT_ATTR,
                        (void *) &bfiattr, 
                        sizeof( bfiattr ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    if (!SC_EQL( bfiattr.reqServices, bfIParams->reqServices) ) { 

        bfiattr.reqServices = bfIParams->reqServices; /* req service class */
    }

    sts = bmtr_update_rec( bfap, 
                           BSR_BF_INHERIT_ATTR,
                           &bfiattr,
                           sizeof( bfiattr ), 
                           ftxH,
                           0 );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    ftx_done_n( ftxH, FTA_BS_SET_BFSET_PARAMS );

    return EOK;

HANDLE_EXCEPTION:

    if (ftxStarted) {
        ftx_fail( ftxH );
    }
    
    return sts;
}

/*
 * bs_get_dmn_vd_list
 *
 * Given a domain pointer this routine returns the list of vd indices for
 * all the vds in the domain.
 */

statusT
bs_get_dmn_vd_list(
                   domainT *dmnP,           /* in - the domain pointer */
                   int vdIndexArrayLen,     /* in - number of ints in array */
                   vdIndexT vdIndexArray[], /* out - list of vd indices */
                   int *numVds              /* out - num vds put in array */
                  )
{
    vdIndexT vdcnt, vdi;
    
    /*
     * Check validity of pointer.
     */

    if (dmnP == NULL) {
        return EBAD_DOMAIN_POINTER;
    }

    /*
     * Make sure caller's vdIndexArray is big enough.
     */

    if (vdIndexArrayLen < dmnP->vdCnt) {
        return EBAD_PARAMS;
    }

    /*
     * Scan the domain's vd table and fill in the caller's vdIndexArray.
     */

    vdcnt = 0;
    vdi = 1;

    while ((vdi <= BS_MAX_VDI) && (vdcnt < dmnP->vdCnt )) {
        if ( vd_htop_if_valid(vdi, dmnP, FALSE, FALSE) ) {
            vdIndexArray[vdcnt] = vdi;
            vdcnt++;
        }

        vdi++; 
    }

    *numVds = vdcnt;

    return EOK;
}

/*
 * bs_get_dmn_params
 *
 * Get the parameters for a domain
 *
 * Returns EBAD_DOMAIN_POINTER, possible status
 * from bmtr_scan or bs_derefpg
 */

statusT
bs_get_dmn_params(
                  domainT *dmnP,           /* in - the domain pointer */
                  bfDmnParamsT *dmnParams, /* out - the domain parameters */
                  int lock                 /* in */
                 )
{
    
    /*
     * Check pointer validity.
     */
    if (dmnP == NULL) {
        return EBAD_DOMAIN_POINTER;
    }

    *dmnParams             = bsNilDmnParams;
    dmnParams->bfDomainId  = dmnP->domainId;
    dmnParams->dmnCookie   = dmnP->dmnCookie;
    dmnParams->bfSetDirTag = dmnP->bfSetDirTag;
    dmnParams->curNumVds   = dmnP->vdCnt;
    dmnParams->dmnVersion  = dmnP->dmnVersion;
    dmnParams->maxVds      = dmnP->maxVds;
    dmnParams->userAllocSz = dmnP->userAllocSz;
    dmnParams->metaAllocSz = dmnP->metaAllocSz;
    strcpy( dmnParams->domainName, dmnP->domainName );

    /*
     * We acquire no mutex here, so this is a race.
     * This is a struct copy.
     */
    dmnParams->bcStat       = dmnP->bcStat;
    dmnParams->bmtStat      = dmnP->bmtStat;

    dmnParams->ftxLogTag    = dmnP->ftxLogTag;
    dmnParams->ftxLogFobs   = dmnP->ftxLogPgs * ADVFS_METADATA_PGSZ_IN_FOBS;
    dmnParams->mode         = dmnP->mode;
    dmnParams->uid          = dmnP->uid;
    dmnParams->gid          = dmnP->gid;

    dmnParams->dmnThreshold = dmnP->dmnThreshold;

    return EOK;
}

/*
 * bs_set_dmn_params
 *
 * This routine sets the writable domain parameters in the domain
 * metadata.
 *
 * Returns EBAD_DOMAIN_POINTER, possible error return from
 * bmtr_scan, bs_deref_pg, bs_pinpg, or bs_unpinpg.
 */

statusT
bs_set_dmn_params(
                  domainT *dmnP,           /* in - domain pointer */
                  bfDmnParamsT *dmnParams  /* in - domain params */
                 )
{
    bfAccessT *mdap;
    bfPageRefHT pgref;
    statusT sts;
    struct bsMPg* bmtpg;
    bsDmnMAttrT* dmnmattrp;
    struct vd *logvdp;
    bfMCIdT mcid;
    ftxHT ftxH;

    /*
     * Check validity of pointer.
     */
    if (dmnP == NULL) {
        return EBAD_DOMAIN_POINTER;
    }

    /*
     * Start a transaction to synchronize with switching of the
     * root tag directory and the log (for rmvol).
     */

    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, dmnP);
    if (sts != EOK) {
        return sts;
    }

    logvdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    mdap = logvdp->rbmtp;

    /*
     * find domain mutable attributes record
     */
    mcid = mdap->primMCId;
    ADVFTX_FTXRW_READ( &mdap->mcellList_lk.lock.rw );
    sts = bmtr_scan( (void*)&dmnmattrp,
                     &pgref,
                     BSR_DMN_MATTR,
                     logvdp,
                     &mcid,
                     mdap->tag);
    ADVFTX_FTXRW_UNLOCK( &mdap->mcellList_lk.lock.rw );
    if ( sts != EOK ) {
        goto finished_set_dmn;
    }

    sts = bs_derefpg( pgref, BS_CACHE_IT );
    if ( sts != EOK ) {
        goto finished_set_dmn;
    }

    sts = bs_pinpg(&pgref, (void*)&bmtpg, mdap, mcid.page, FtxNilFtxH, BS_NIL);
    if ( sts != EOK ) {
        goto finished_set_dmn;
    }

    dmnmattrp->uid = dmnParams->uid;
    dmnmattrp->gid = dmnParams->gid;
    dmnmattrp->mode = dmnParams->mode;

    dmnmattrp->dmnThreshold.threshold_upper_limit =
        dmnParams->dmnThreshold.threshold_upper_limit;

    dmnmattrp->dmnThreshold.threshold_lower_limit =
        dmnParams->dmnThreshold.threshold_lower_limit;

    dmnmattrp->dmnThreshold.upper_event_interval  =
        dmnParams->dmnThreshold.upper_event_interval;

    dmnmattrp->dmnThreshold.lower_event_interval  =
        dmnParams->dmnThreshold.lower_event_interval;

    sts = bs_unpinpg( pgref, logNilRecord, BS_DIRTY );

    dmnP->dmnThreshold = dmnParams->dmnThreshold;

finished_set_dmn:
    ftx_done_n(ftxH, FTA_NULL);

    return sts;
}

/*
 * bs_get_vd_params
 *
 * Get the parameters for a vd
 *
 * Returns EBAD_DOMAIN_POINTER, EBAD_VDI, possible error
 * return from bs_derefpg or bmtr_scan.
 */


statusT
bs_get_vd_params(
                 domainT *dmnP,          /* in - domain pointer */
                 vdIndexT vdIndex,       /* in - vd index */
                 bsVdParamsT *vdParams,  /* out - the vd parameters*/
                 int flag                /* in
                                            GETVOLPARAMS_NO_BMT - used to contr
                                            GETVOLPARAMS2 - use cumulative ioq 
                                            GETVOLPARAMS2_CHVOL_OP_A
                                         */
                )
{
    bfAccessT *mdap;
    bfPageRefHT pgref;
    statusT sts;
    struct bsVdAttr* vdattrp;
    struct vd *vdp;
    bfMCIdT mcid;

    /*
     * Check validity of pointer.
     */
    if (dmnP == NULL) {
        return EBAD_DOMAIN_POINTER;
    }

    if ( !(vdp = vd_htop_if_valid(vdIndex, dmnP, TRUE, FALSE)) ) {
        return EBAD_VDI;
    }

    *vdParams              = bsNilVdParams;
    vdParams->freeClusters = vdp->freeClust;
    vdParams->totalClusters = vdp->vdClusters;
    strcpy( vdParams->vdName, vdp->vdName );
    vdParams->bmtXtntPgs = vdp->bmtXtntPgs;

    vdParams->dStat = vdp->dStat;

    /* Allocation unit sizes for metadata and user data files. */
    vdParams->userAllocSz = dmnP->userAllocSz;
    vdParams->metaAllocSz = dmnP->metaAllocSz;

    /*
     * Parameters that can be varied.
     * Note that vdp->vdSize above is the in-use size (raw size & ~0x0f)
     */
    vdParams->vdSize = vdp->vdSize;

    /* Convert the current IO transfer byte sizes into driver blocks. */
    vdParams->rdMaxIo = vdp->current_iosize_rd / DEV_BSIZE;
    vdParams->wrMaxIo = vdp->current_iosize_wr / DEV_BSIZE;

    /* These IO transfer byte sizes originate from the disk drivers. */
    vdParams->max_iosize = vdp->max_iosize;
    vdParams->preferred_iosize = vdp->preferred_iosize;
    
    /* The minimum read/write IO transfer byte size is sized to the largest
     * of either the user data or metadata allocation unit.
     */
    vdParams->min_iosize = (MAX(dmnP->metaAllocSz, dmnP->userAllocSz) /
                            ADVFS_FOBS_PER_DEV_BSIZE) * DEV_BSIZE;

    /*
     * "flag" is used by OP_GET_VOL_PARAMS to specify whether or not to
     * read the bmt-specific data.  This is used (by showfdmn type 
     * functionality) to fetch any available data when the bmt is unavailable,
     * such as when the service class is nil.
     */
    if (flag&GETVOLPARAMS_NO_BMT) {
        vd_dec_refcnt( vdp );
        return EOK;
    }

    /* 
     * If this is a special 'chvol -A', then the user is to trying
     * to clean up after an interrupted rmvol command. Go ahead 
     * and read from the disk.
     */
    if(!(flag&GETVOLPARAMS2_CHVOL_OP_A)) {
        /*
         * If the Service class of this volume is Nill then a rmvol
         * is in progress.  Volume attributes can not be retrieved.
         */
        if(SC_EQL(vdp->serviceClass, nilServiceClass)) {
            vd_dec_refcnt( vdp );
            return EBAD_VDI;
        }
    }


    mdap = vdp->rbmtp;

    /*
     * find vd attributes record  (in BMT metadata cell)
     */
    mcid = mdap->primMCId;
    ADVRWL_BMT_MCELL_LIST_LOCK_READ( &mdap->mcellList_lk.lock.rw );
    sts = bmtr_scan( (void*)&vdattrp,
                     &pgref,
                     BSR_VD_ATTR,
                     vdp,
                     &mcid,
                     mdap->tag);
    ADVRWL_BMT_MCELL_LIST_LOCK_UNLOCK( &mdap->mcellList_lk.lock.rw );
    if ( sts != EOK ) {
        vd_dec_refcnt( vdp );
        return sts;
    }

    vdParams->vdMntId      = vdattrp->vdMntId;
    vdParams->vdState      = vdattrp->state;
    vdParams->vdIndex      = vdattrp->vdIndex;
    vdParams->stgCluster   = vdattrp->sbmBlksBit;
    vdParams->serviceClass = vdattrp->serviceClass;

    sts = bs_derefpg( pgref, BS_CACHE_IT );

    vd_dec_refcnt( vdp );
    return sts;
}

/*
 * bs_set_vd_params
 *
 * set the parameters for a virtual disk
 *
 * Returns EBAD_DOMAIN_POINTER, possible error return from
 * bmtr_scan, bs_derefpg, bs_pinpg, bs_unpinpg
 */


statusT
bs_set_vd_params(
                 domainT *dmnP,         /* in - domain pointer */
                 vdIndexT vdIndex,      /* in - vd index */
                 bsVdParamsT *vdParams  /* in - vd parameters */
                )
{
    statusT sts;
    struct vd *vdp;
    vdIoParamsT vdIoParams;
    ftxHT ftx;
    uint32_t min_iosize;
    /*
     * Check validity of pointer.
     */
    if (dmnP == NULL) {
        return EBAD_DOMAIN_POINTER;
    }

    if ( !(vdp = vd_htop_if_valid(vdIndex, dmnP, TRUE, FALSE)) ) {
        return EBAD_VDI;
    }

    /*
     * Can't set I/O transfer size to be greater than device maximum
     * in term device blocks.
     */
    if (vdParams->rdMaxIo > (vdp->max_iosize / DEV_BSIZE)) {
        vdParams->rdMaxIo = 
            MIN(vdParams->rdMaxIo, vdp->max_iosize / DEV_BSIZE);
    }
    if (vdParams->wrMaxIo > (vdp->max_iosize / DEV_BSIZE)) {
        vdParams->wrMaxIo =
            MIN(vdParams->wrMaxIo, vdp->max_iosize / DEV_BSIZE);
    }

    /*
     * Can't set I/O transfer size to be lower than the largest metadata or
     * user data allocation unit size in terms of device blocks.
     */
    min_iosize = MAX(dmnP->metaAllocSz, dmnP->userAllocSz) /
                 ADVFS_FOBS_PER_DEV_BSIZE;
    
    if (vdParams->rdMaxIo < min_iosize) {
        vdParams->rdMaxIo = min_iosize;
    }
    if (vdParams->wrMaxIo < min_iosize) {
        vdParams->wrMaxIo = min_iosize;
    }

    /*
     * Parameters that can be varied.
     * Update the in-memory vd's current IO transfer read/write byte sizes.
     * Also, store current IO transfer size on disk in terms of device blocks.
     * Zero the vdIoParams reserved fields.
     */
    vdp->current_iosize_rd = vdParams->rdMaxIo * DEV_BSIZE;
    vdp->current_iosize_wr = vdParams->wrMaxIo * DEV_BSIZE;
    vdIoParams.rdMaxIo = vdParams->rdMaxIo;
    vdIoParams.wrMaxIo = vdParams->wrMaxIo;
    vdIoParams.vdiop_reserved1 = 0;
    vdIoParams.vdiop_reserved2 = 0;
    vdIoParams.vdiop_reserved3 = 0;
    vdIoParams.vdiop_reserved4 = 0;

    sts = FTX_START_N(FTA_NULL, &ftx, FtxNilFtxH, dmnP);
    if (sts != EOK) {
        ADVFS_SAD1("bs_set_vd_params(1) - failed to create xact", sts);
    }

    /*
     * Install I/O params in the RBMT.
     */
    sts = bmtr_put_rec( 
        vdp->rbmtp, 
        BSR_VD_IO_PARAMS, 
        &vdIoParams, 
        sizeof( vdIoParamsT ),
        ftx );

    ftx_done_n( ftx, FTA_BS_SET_VD_PARAMS );
    vd_dec_refcnt( vdp );
    return sts;
}
