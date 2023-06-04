/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION  1991, 1992, 1993                     *
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
 *      ADVFS/bs
 *
 * Abstract:
 *
 *      This module contains the get/set params routines
 *      TODO - add locking to these routines
 *
 * Date:
 *
 *      Thu Aug  2 15:40:21 1990
 *
 */
/*
 * HISTORY
 * 
 */
#pragma ident "@(#)$RCSfile: bs_params.c,v $ $Revision: 1.1.169.7 $ (DEC) $Date: 2008/01/03 05:51:03 $"

#define ADVFS_MODULE   BS_PARAMS

#include <sys/param.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_stripe.h>
#include <msfs/bs_ims.h>
#include <msfs/fs_dir.h>
#include <sys/vnode.h>
#include <msfs/ms_osf.h>
#include <sys/lock_probe.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_public.h>
#include <msfs/msfs_syscalls.h>
#include <sys/clu.h>
#include <msfs/bs_index.h>


static 
int
get_stripe_next_page( 
               bfAccessT *bfap, 
               uint32T pg, 
               int cloned,
               int *use_orig_bfap,
               int *start_pgP,      /* out */
               int *nextPageP,      /* out */
               vdIndexT *vdIndexP,  /* out */ 
               uint32T *vdBlkP );   /* out */

/*
 * bs_get_bf_xtnt_map
 *
 * copy the extents of a bitfile into an array
 * 
 * Returns EINVALID_HANDLE, EBAD_PARAMS, or EOK
 */

statusT
bs_get_bf_xtnt_map(
    bfAccessT *bfap,                   /* in - access structure */
    int startXtntMap,                  /* in - extent map at which to start */
    int startXtnt,                     /* in - extent at which to start */
    int xtntArraySize,                 /* in - size of extent array */
    bsExtentDescT xtntsArray[],        /* in - out - array to copy into */
    int *xtntCnt,                      /* out - number of extents copied */
    vdIndexT *allocVdIndex             /* out - disk on which stg is alloc'd. */
)
{
    uint32T cnt;
    int dst;
    uint32T mapIndex;
    int src;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    int unloadFlag = 0;
    bsInMemXtntMapT *xtntMap;
    bsInMemSubXtntMapT *subXtntBase;

    /*
     * Check validity of bfap.
     */
    if( bfap == NULL ) {
        return( EINVALID_HANDLE );
    }

    /*
     * Make sure that the in-memory extent maps are valid.
     * Returns with xtntMap_lk read-locked.
     */
    sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
    if (sts != EOK) {
        return sts;
    }

    switch (bfap->xtnts.type)  {

      case BSXMT_APPEND:

        if (startXtntMap != 1) {
            RAISE_EXCEPTION (EBAD_PARAMS);
        }
        xtntMap = bfap->xtnts.xtntMap;

        break;

      case BSXMT_STRIPE:

        if ((startXtntMap < 1) ||
            (startXtntMap > bfap->xtnts.stripeXtntMap->cnt)) {
            RAISE_EXCEPTION (EBAD_PARAMS);
        }

        xtntMap = bfap->xtnts.stripeXtntMap->xtntMap[startXtntMap - 1];

        break;

      default:

        RAISE_EXCEPTION (EBAD_PARAMS);

    }  /* end switch */

    *allocVdIndex = xtntMap->allocVdIndex;
    subXtntBase = xtntMap->subXtntMap;   /* Base address of malloc'd array */

    if ((startXtnt == 0) && (xtntArraySize == 0)) {
        /* FIX - is subXtntMap->cnt always valid? */
        cnt = 0;
        for (mapIndex = 0; mapIndex < xtntMap->validCnt; mapIndex++) {
            cnt += xtntMap->subXtntMap[mapIndex].cnt;
        }
        *xtntCnt = cnt;
        RAISE_EXCEPTION (EOK);
    }

    if (startXtnt < 0) {
        RAISE_EXCEPTION (EBAD_PARAMS);
    }

    /*
     * Find the starting sub extent map.
     */

    /* FIX - is subXtntMap->cnt always valid? */
    cnt = 0;
    mapIndex = 0;
    while (mapIndex < xtntMap->validCnt) {

        /* This should not change while we have xtntMap_lk seized */
        MS_SMP_ASSERT( xtntMap->subXtntMap == subXtntBase );

        if ( startXtnt < (cnt + xtntMap->subXtntMap[mapIndex].cnt) ) {
            break;  /* out of while */
        } else {
            cnt += xtntMap->subXtntMap[mapIndex].cnt;
        }
        mapIndex++;
    }

    if (mapIndex >= xtntMap->validCnt) {
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    /*
     * Copy extent descriptors to the callers buffer.
     */

    dst = 0;
    src = startXtnt - cnt;
    while ((dst < xtntArraySize) && (mapIndex < xtntMap->validCnt)) {

        subXtntMap = &(xtntMap->subXtntMap[mapIndex]);
        if (subXtntMap->cnt == 0) {  /* FIX - is cnt always valid?? */
            /*
             * The extent descriptors are not cached.  Bring them into
             * memory temporarily.
             */
            sts = imm_load_sub_xtnt_map( bfap, xtntMap, subXtntMap);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            unloadFlag = 1;
        }

        if (subXtntMap->cnt > 1) {

            while ( (dst < xtntArraySize) && (src < (subXtntMap->cnt - 1)) ) {

                xtntsArray[dst].bfPage = subXtntMap->bsXA[src].bsPage;
                xtntsArray[dst].bfPageCnt = subXtntMap->bsXA[src + 1].bsPage -
                  subXtntMap->bsXA[src].bsPage;
                xtntsArray[dst].volIndex = subXtntMap->vdIndex;
                xtntsArray[dst].volBlk = subXtntMap->bsXA[src].vdBlk;

                dst++;
                src++;
            }

            if ( (dst < xtntArraySize) && (src == (subXtntMap->cnt - 1)) ) {
                xtntsArray[dst].bfPage = subXtntMap->bsXA[src].bsPage;
                xtntsArray[dst].bfPageCnt = 0;
                xtntsArray[dst].volIndex = subXtntMap->vdIndex;
                xtntsArray[dst].volBlk = -1;
                dst++;
            }
        } else {
            /*
             * If the termination extent descriptor is the only descriptor,
             * return it so that the caller knows there is a sub extent map
             * with no extent information.
             */
            xtntsArray[dst].bfPage = subXtntMap->bsXA[src].bsPage;
            xtntsArray[dst].bfPageCnt = 0;
            xtntsArray[dst].volIndex = subXtntMap->vdIndex;
            xtntsArray[dst].volBlk = -1;
            dst++;
        }

        if (unloadFlag != 0) {
            unloadFlag = 0;
            imm_unload_sub_xtnt_map (subXtntMap);
        }
        mapIndex++;
        src = 0;
    }  /* end while */

    /* This should not change while we have xtntMap_lk seized */
    MS_SMP_ASSERT( xtntMap->subXtntMap == subXtntBase );

    *xtntCnt = dst;

    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    return EOK;

HANDLE_EXCEPTION:

    if (unloadFlag != 0) {
        imm_unload_sub_xtnt_map (subXtntMap);
    }
    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )

    return sts;

}  /* end bs_get_bf_xtnt_map */


/*
 * bs_get_clone_xtnt_map
 *
 * create a single xtnt map out of the original file and clone xtnt maps.
 * copy the extents of the resulting bitfile into an array
 *
 * NOTE: DO NOT USE THIS ROUTINE TO RETURN BITMAP XTNT MAPS FOR FILES.  USE
 *       BS_GET_BF_XTNT_MAP.  THIS ROUTINE MODIFIES THE XTNT MAPS FOR USE 
 *       BY VDUMP.
 * 
 * NOTE: The "pageCnt" parameter returns different information, depending
 *       on the value of the "flags" parameter.  If "flags" is XTNT_XTNTMAP,
 *       then "pageCnt" returns the number of the last page in use by the
 *       file + 1.  If "flags" is XTNT_PAGECNT_ONLY, "pageCnt" returns the
 *       number of pages in the merged extent map.  These two values could
 *       differ if the original file is sparse.  If "flags" is 
 *       XTNT_PAGECNT_ONLY, "xtntCnt" is not filled in.
 *       If "flags" is XTNT_XTNTCNT_ONLY, "pageCnt" is not filled in.
 * 
 * N.B.  Until clone extent maps are modified to make it clear that
 *       a hole existed in the original file and was then filled in
 *       after cloning, this code will report the wrong value for
 *       XTNT_PAGECNT_ONLY.
 * 
 * Returns EINVALID_HANDLE, EBAD_PARAMS, E_NOT_ENOUGH_XTNTS or EOK
 */

statusT
bs_get_clone_xtnt_map(
    bfAccessT *clone_bfap,       /* in - clone access structure */
    int startXtntMap,            /* in - clone extent map at which to start */
    int startXtnt,               /* in - clone extent at which to start */
    int xtntArraySize,           /* in - clone size of extent array */
    int flags,                   /* in - what work to do */
    bsExtentDescT xtntsArray[],  /* in-out(only if xtntCnt !=0)merged array */
    int *xtntCnt,                /* in-out - # extent entries for merged map */
    vdIndexT *allocVdIndex,      /* out - disk on which next stg is alloc'd. */
    int *pageCnt                 /* out - see note above */
)
{
    bfAccessT *orig_bfap;
    bsInMemXtntMapT *clone_xtntMap, *orig_xtntMap;
    bsInMemXtntMapT *newxtntmap = NULL;
    uint32T clone_cnt, orig_cnt, cnt;
    int dst;
    uint32T mapIndex;
    int src;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    int pagecnt = 0;
    int clonextnt_locked=0;

    /*
     * Check validity of clone_bfap.
     */
    if( clone_bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    /* get the orig pointer */

    orig_bfap = clone_bfap->origAccp;

    if (clu_is_ready()){
        /* If this is a cluster then we need to block from getting the
         * clone xtnt map during migration.
         */
        CLU_CLXTNT_READ(&clone_bfap->clu_clonextnt_lk);
        clonextnt_locked=1;
    }

    /* block access to the clone's extent maps if truncating the original */
    TRUNC_XFER_LOCK_READ(&orig_bfap->trunc_xfer_lk);
    /* make sure the clone extents don't change */
    COW_LOCK_READ( &(orig_bfap->cow_lk) )


    /* Lock the orig map first.
     * Make sure that the in-memory extent maps are valid.
     * This only retrieves xtnt map into memory
     * if changes occur, they occur on the mem copy only
     * This returns with the orig_bfap->xtntMap_lk read-locked.
     */
    sts = x_load_inmem_xtnt_map( orig_bfap, X_LOAD_REFERENCE);
    if (sts != EOK) {
        COW_UNLOCK( &(orig_bfap->cow_lk) )
        TRUNC_XFER_UNLOCK(&orig_bfap->trunc_xfer_lk);
        if (clonextnt_locked) 
            CLU_CLXTNT_UNLOCK(&clone_bfap->clu_clonextnt_lk);
        return sts;
    }

    /* Get the clone map next.
     * This returns with the clone_bfap->xtntMap_lk read-locked.
     */
    sts = x_load_inmem_xtnt_map( clone_bfap, X_LOAD_REFERENCE);
    if (sts != EOK) {
        XTNMAP_UNLOCK( &(orig_bfap->xtntMap_lk) );
        COW_UNLOCK( &(orig_bfap->cow_lk) )
        TRUNC_XFER_UNLOCK(&orig_bfap->trunc_xfer_lk);
        if (clonextnt_locked) 
            CLU_CLXTNT_UNLOCK(&clone_bfap->clu_clonextnt_lk);
        return sts;
    }

    if(clone_bfap->xtnts.type == BSXMT_APPEND)  { 
        /* will be same as orig's type */
        if (startXtntMap != 1) {
            RAISE_EXCEPTION (EBAD_PARAMS);
        }
        clone_xtntMap = clone_bfap->xtnts.xtntMap;
        if(orig_bfap->xtnts.type != BSXMT_APPEND) 
        {
            MS_SMP_ASSERT(orig_bfap->xtnts.type == BSXMT_STRIPE);

            /*
             * This is the case where the file had storage and was
             * cloned. Then it was truncated to zero and striped.
             * The clone must have all the original pages cowed over
             * to it at truncation time. We will force this by setting the
             * original's xtnt map to the clone's xtntmap
             */

            orig_xtntMap = clone_bfap->xtnts.xtntMap;
            
        } else {
            orig_xtntMap = orig_bfap->xtnts.xtntMap;
        }
    } else {
        RAISE_EXCEPTION (EBAD_PARAMS);
    }  /* end if */

    *allocVdIndex = clone_xtntMap->allocVdIndex; /* not used yet - stripes? */

    /*  
     * calculate the size of the merged map 
     */

    /* FIX - is subXtntMap->cnt always valid? */
    clone_cnt = 0;
    orig_cnt = 0;
    for (mapIndex = 0; mapIndex < clone_xtntMap->validCnt; mapIndex++) {
        if (clone_xtntMap->subXtntMap[mapIndex].cnt != 0) {
            if (clone_xtntMap->subXtntMap[mapIndex].cnt > 1) {
                /* Don't count term. extent desc. */
                clone_cnt += clone_xtntMap->subXtntMap[mapIndex].cnt - 1;
            } else {
                clone_cnt++;
            }
        }
    }  /* end for */

    for (mapIndex = 0; mapIndex < orig_xtntMap->validCnt; mapIndex++) {
        if (orig_xtntMap->subXtntMap[mapIndex].cnt != 0) {
            if (orig_xtntMap->subXtntMap[mapIndex].cnt > 1) {
                /* Don't count term. extent desc. */
                orig_cnt += orig_xtntMap->subXtntMap[mapIndex].cnt - 1;
            } else {
                orig_cnt++;
            }
        }
    }  /* end for */

    /*
     * set to orig's size + clone's size for max 
     */

    *xtntCnt = orig_cnt + clone_cnt;

    if (flags & XTNT_XTNTCNT_ONLY) {
        RAISE_EXCEPTION (EOK);
    }

    if (startXtnt < 0) {
        RAISE_EXCEPTION (EBAD_PARAMS);
    }

   /* Merge the maps */

    sts = imm_merge_xtnt_map( orig_bfap,
                              orig_xtntMap,
                              clone_bfap,
                              clone_xtntMap,
                              &newxtntmap);
    if (sts != EOK) {
        XTNMAP_UNLOCK( &(orig_bfap->xtntMap_lk) )
        XTNMAP_UNLOCK( &(clone_bfap->xtntMap_lk) )
        COW_UNLOCK( &(orig_bfap->cow_lk) )
        TRUNC_XFER_UNLOCK(&orig_bfap->trunc_xfer_lk);
        if (clonextnt_locked) 
            CLU_CLXTNT_UNLOCK(&clone_bfap->clu_clonextnt_lk);
        if(newxtntmap != NULL)
            imm_delete_xtnt_map(newxtntmap);
        return sts;

    }

    /*
     * Find the starting sub extent map in the new merged map.
     */

    cnt = 0;
    mapIndex = 0;
    while (mapIndex < newxtntmap->validCnt) {
        if (newxtntmap->subXtntMap[mapIndex].cnt != 0) {
            if (newxtntmap->subXtntMap[mapIndex].cnt > 1) {
                /* Don't count term. extent desc. */
                if (startXtnt < (cnt + (newxtntmap->subXtntMap[mapIndex].cnt - 1))) {
                    break;  /* out of while */
                } else {
                    cnt = cnt + (newxtntmap->subXtntMap[mapIndex].cnt - 1);
                }
            } else {
                if (startXtnt < (cnt + newxtntmap->subXtntMap[mapIndex].cnt)) {
                    break;  /* out of while */
                } else {
                    cnt++;
                }
            }
        }
        mapIndex++;
    }  /* end while */

    if (mapIndex >= newxtntmap->validCnt) {
        *pageCnt = 0;
        RAISE_EXCEPTION (ENO_XTNTS);
    }

    /*
     * If desired, copy extent descriptors from merged map to 
     * the callers buffer.  In any case, get the page count of
     * the merged extent map.
     */

    dst = 0;
    src = startXtnt - cnt;
    while (((flags & XTNT_PAGECNT_ONLY) || (dst < xtntArraySize)) && 
           (mapIndex < newxtntmap->validCnt)) {

        subXtntMap = &(newxtntmap->subXtntMap[mapIndex]);

        if (subXtntMap->cnt > 1) {

            while (((flags & XTNT_PAGECNT_ONLY) || (dst < xtntArraySize)) && 
                   (src < (subXtntMap->cnt - 1))) {

                if (clone_bfap->xtnts.type != BSXMT_APPEND) {
                    RAISE_EXCEPTION (EBAD_PARAMS);
                }

                if (flags & XTNT_XTNTMAP) {
                    xtntsArray[dst].bfPage = subXtntMap->bsXA[src].bsPage;
                    xtntsArray[dst].bfPageCnt = 
                        subXtntMap->bsXA[src + 1].bsPage -
                        subXtntMap->bsXA[src].bsPage;
                    xtntsArray[dst].volIndex = subXtntMap->vdIndex;
                    xtntsArray[dst].volBlk = subXtntMap->bsXA[src].vdBlk;
                    pagecnt += xtntsArray[dst].bfPageCnt;
                }
                else if (flags & XTNT_PAGECNT_ONLY) {
                    /*
                     * If we have a non-terminator extent descriptor
                     * then add in the number of pages it represents
                     * if the extent being described is not beyond 
                     * the clone's page range.
                     */
                    if ( subXtntMap->bsXA[src].vdBlk != XTNT_TERM &&
                         subXtntMap->bsXA[src].vdBlk != PERM_HOLE_START )
                    {
                        if (subXtntMap->bsXA[src].bsPage <
                            clone_bfap->maxClonePgs) {
                            pagecnt += subXtntMap->bsXA[src + 1].bsPage -
                                       subXtntMap->bsXA[src].bsPage;
                        }
                        else {
                            /*
                             * We are now into extents that were added
                             * after the clone was created.  No need to
                             * continue.  We have the pagecnt we wanted.
                             * Force a break out of the nested loops.
                             */
                            src = subXtntMap->cnt - 1;
                            mapIndex = newxtntmap->validCnt - 1;
                        }
                    }
                }

                src++;
                dst++;
            }  /* end while */
        } else {
            /*
             * If the termination extent descriptor is the only descriptor,
             * return it so that the caller knows there is a sub extent map
             * with no extent information.
             */
            if (flags & XTNT_XTNTMAP) {
                xtntsArray[dst].bfPage = subXtntMap->bsXA[src].bsPage;
                xtntsArray[dst].bfPageCnt = 0;
                xtntsArray[dst].volIndex = subXtntMap->vdIndex;
                xtntsArray[dst].volBlk = -1;
            }
            pagecnt = 0;
            dst++;
        }

        mapIndex++;
        src = 0;
    }  /* end while */

    if (!(flags & XTNT_PAGECNT_ONLY))
        *xtntCnt = dst;
    *pageCnt = pagecnt;

    if (clonextnt_locked) {
        /* cloneXtntsRetrieved is protected by clu_clonextnt_lk AND cow_lk. */
        /* bs_cow_pg uses cow_lk. get_clu_clone_locks uses clu_clonextnt_lk. */
        clone_bfap->cloneXtntsRetrieved = 1;
        CLU_CLXTNT_UNLOCK(&clone_bfap->clu_clonextnt_lk);
    }

    XTNMAP_UNLOCK( &(orig_bfap->xtntMap_lk) )
    XTNMAP_UNLOCK( &(clone_bfap->xtntMap_lk) )
    COW_UNLOCK( &(orig_bfap->cow_lk) )
    TRUNC_XFER_UNLOCK(&orig_bfap->trunc_xfer_lk);

    /* if the newxtntmap was not set to the clone(fragment only)
     * then we need to clean up the alloced xtnt map from the
     * merge routine above.
     */
    if (newxtntmap != NULL)
             imm_delete_xtnt_map(newxtntmap);

    return EOK;

HANDLE_EXCEPTION:

    XTNMAP_UNLOCK( &(orig_bfap->xtntMap_lk) )
    XTNMAP_UNLOCK( &(clone_bfap->xtntMap_lk) )
    COW_UNLOCK( &(orig_bfap->cow_lk) )
    TRUNC_XFER_UNLOCK(&orig_bfap->trunc_xfer_lk);
    if (clonextnt_locked) 
        CLU_CLXTNT_UNLOCK(&clone_bfap->clu_clonextnt_lk);
    if(newxtntmap != NULL)
        imm_delete_xtnt_map(newxtntmap);

    return sts;

}  /* end bs_get_clone_xtnt_map */


/*
 * bs_get_stripe_xtnt_map
 *
 * Maps may be either original stripe files or clones of stripe files.
 *
 * This function can be called just to get the extent count or page count
 * or an extent map for a striped file. The flags parameter passed to this
 * function says what needs to be done. 
 * We run through the extent map of the striped file using the function
 * "page_is_mapped" to count the number of extents and number of pages
 * allocated.
 *
 * NOTE: DO NOT USE THIS ROUTINE TO RETURN BITMAP XTNT MAPS FOR FILES.  USE
 *       BS_GET_BF_XTNT_MAP.  THIS ROUTINE MODIFIES THE XTNT MAPS FOR USE BY
 *       VDUMP.
 * NOTE: The "pageCnt" parameter returns different information, depending
 *       on the value of the "flags" parameter.  If "flags" is XTNT_XTNTMAP,
 *       then "pageCnt" returns the number of the last page in use by the
 *       file + 1.  If "flags" is XTNT_PAGECNT_ONLY, "pageCnt" returns the
 *       number of pages allocated.  These two values could
 *       differ if the original file is sparse.  If "flags" is 
 *       XTNT_PAGECNT_ONLY, "xtntCnt" is not filled in.
 *       If "flags" is XTNT_XTNTCNT_ONLY, "pageCnt" is not filled in.
 *
 * 
 * Returns EINVALID_HANDLE, EBAD_PARAMS, ENOT_ENOUGH_XTNTS, or EOK
 */

statusT
bs_get_stripe_xtnt_map(
    bfAccessT *bfap,             /* in - stripe access structure */
    int startXtntMap,            /* in - xtntmap at which to start */
    int startXtnt,               /* in - stripe extent at which to start */
    int xtntArraySize,           /* in - stripe size of extent array */
    int flags,                   /* in - what work to do */
    bsExtentDescT xtntsArray[],  /* in-out(only if xtntCnt !=0)merged array */
    int *xtntCnt,                /* in-out num xtnt entries for merged map */
    int *pageCnt,                /* out - count of pages for merged map*/
    int cloned                   /* in - stripe file or striped clone */
)
{
    /* Note: bfap may be either regular striped file or clone */
    /* if bfap is a clone, then orig_bfap points to original */

    uint32T segmentCnt, segmentSize;
    uint32T pg = 0;
    bsExtentDescT *tmp_xtntsArray = NULL;
    statusT sts = EOK;
    int pagecnt = 0;
    int count=0, size = 0;
    int  nextPage=0;
    int use_orig_bfap = 0, idx, ret;
    int hole_found = 0, start_hole = 0;
    vdIndexT vdIndex;
    uint32T vdBlk, start_pg, pgs;
    int end_stripe = 0;
    off_t fileSize = bfap->file_size;
    uint32T allocPageCnt = bfap->xtnts.allocPageCnt;

    if (bfap->xtnts.type != BSXMT_STRIPE) {
        RAISE_EXCEPTION (EBAD_PARAMS);
    } 

    if (cloned) {
        if (bfap->origAccp->xtnts.type != BSXMT_STRIPE) {
            RAISE_EXCEPTION( EBAD_PARAMS );
        }
    }

    if ( fileSize  == 0 || !(allocPageCnt) ) {
        *xtntCnt = 1;
        *pageCnt = 0;
        if (flags & XTNT_XTNTMAP) {
             xtntsArray[0].bfPage = 0;
             xtntsArray[0].bfPageCnt = 0;
             xtntsArray[0].volIndex = 1;
             xtntsArray[0].volBlk = -1;
        }
        return EOK;
    }

    segmentSize = bfap->xtnts.stripeXtntMap->segmentSize;
    segmentCnt = bfap->xtnts.stripeXtntMap->cnt;

    while (nextPage >= 0) {
        ret = get_stripe_next_page( bfap, pg, cloned, &use_orig_bfap,
                                    &start_pg, &nextPage, &vdIndex, &vdBlk );
 
        if (ret == TRUE) {
            pagecnt += (nextPage - pg);
        }
        if ( nextPage == -1 && pg <= fileSize / ADVFS_PGSZ ) {
            nextPage = NEXT_SEGMENT_PAGE(pg,segmentSize);
        }
        pg = nextPage;
        count++;
    }

    if (flags & XTNT_PAGECNT_ONLY) {
        RAISE_EXCEPTION (EOK);
    }

    if((startXtntMap < 1) ||
        (startXtntMap > bfap->xtnts.stripeXtntMap->cnt)) {
            RAISE_EXCEPTION (EBAD_PARAMS);
    }

    if (startXtnt < 0) {
        RAISE_EXCEPTION (EBAD_PARAMS);
    }

    /* To get the extent map for the file */

    pg = 0;  nextPage=0; pagecnt = 0;
    idx = 0; /* extent map index */
    use_orig_bfap = 0;

    tmp_xtntsArray = (bsExtentDescT *)ms_malloc(count * sizeof(bsExtentDescT));

    while (nextPage >= 0) {
        ret = get_stripe_next_page( bfap, pg, cloned, &use_orig_bfap,
                                    &start_pg, &nextPage, &vdIndex, &vdBlk );
        if ( nextPage == -1 ) {
            if ( pg <= fileSize / ADVFS_PGSZ ) {
                nextPage = NEXT_SEGMENT_PAGE(pg,segmentSize);
                if (!end_stripe && !hole_found) {
                    tmp_xtntsArray[idx].volIndex = vdIndex;
                    tmp_xtntsArray[idx].bfPage = pg;
                    tmp_xtntsArray[idx].bfPageCnt = nextPage - pg;
                    tmp_xtntsArray[idx].volBlk = XTNT_TERM;
                } else {
                    tmp_xtntsArray[idx].bfPageCnt += (nextPage - pg);
                }
                pg = nextPage;
                end_stripe = TRUE;
                continue;
            }
            break;
        }

        /* if we have consecutive holes which are spread across the volumes
         * we combine them together and represent it as  one single hole
         * with the volume index as the first volume which has this consecutive
         * hole.
         */
        if ( vdBlk == XTNT_TERM || vdBlk == PERM_HOLE_START ) {
            if ((!hole_found) && (!end_stripe)) {
                tmp_xtntsArray[idx].volIndex = vdIndex;
                tmp_xtntsArray[idx].bfPage = pg;
                tmp_xtntsArray[idx].bfPageCnt = nextPage - pg;
                tmp_xtntsArray[idx].volBlk = XTNT_TERM;
            } else {
                tmp_xtntsArray[idx].bfPageCnt += (nextPage - pg);
            }
            hole_found = TRUE;
            end_stripe = FALSE;
        } else {
            pgs = BFPAGE_TO_XMPAGE(pg,segmentCnt,segmentSize) - start_pg;
            if(hole_found == TRUE){
                pagecnt += tmp_xtntsArray[idx].bfPageCnt;
                idx++;
            }
            hole_found = FALSE;
            end_stripe = FALSE;

            tmp_xtntsArray[idx].volIndex = vdIndex;
            tmp_xtntsArray[idx].bfPage = pg;
            tmp_xtntsArray[idx].bfPageCnt = nextPage - pg;
            tmp_xtntsArray[idx].volBlk = vdBlk + pgs * ADVFS_PGSZ_IN_BLKS;

            pagecnt += tmp_xtntsArray[idx].bfPageCnt;
            idx++;
        }
        pg = nextPage;
    }

    if (flags & XTNT_XTNTCNT_ONLY) {
        /* return the total size of the extent map. Since it is a striped
         * file we have an array of extent map, array size being the size of
         * of the segment.
         */
        *xtntCnt = idx;
        RAISE_EXCEPTION (EOK);
    }

    idx = idx - startXtnt;
    if (idx == 0) {
	/* return error ENO_XTNTS in case we do not have more extents
	 * to return.
	 */
	size = idx;
	RAISE_EXCEPTION (ENO_XTNTS);
    } else if (idx  < xtntArraySize) {
        size = idx;
    } else {
        size = xtntArraySize;
    }
    bcopy((tmp_xtntsArray+startXtnt), xtntsArray, (size*sizeof(bsExtentDescT)));

HANDLE_EXCEPTION:

    if (flags & XTNT_XTNTMAP)
        *xtntCnt = size;

    if (flags & (XTNT_PAGECNT_ONLY | XTNT_XTNTMAP))
        *pageCnt = pagecnt;

    if (tmp_xtntsArray != NULL )
        ms_free (tmp_xtntsArray);

    return sts;
}

static int
get_stripe_next_page( bfAccessT *bfap, uint32T pg, int cloned, 
                      int *use_orig_bfap, int *start_pgP, int *nextPageP,
                      vdIndexT *vdIndexP, uint32T *vdBlkP )
{
    bfAccessT *orig_bfap = bfap->origAccp;
    int clone_nextPage = 0, ret;
    int segmentSize = bfap->xtnts.stripeXtntMap->segmentSize;

    if (!cloned) {
        ret = page_is_mapped_local(bfap, pg, nextPageP, FALSE,
                                   vdIndexP, vdBlkP, start_pgP, TRUE);
    } else if (*use_orig_bfap) {
        if (pg < bfap->maxClonePgs) {
            /* Don't look at orig file extents past maxClonePgs. */
            ret = page_is_mapped_local(orig_bfap, pg, nextPageP, FALSE,
                                       vdIndexP, vdBlkP, start_pgP, TRUE);

            /* In case of striped files when we reach end of segment
             * nextPage will be set to -1. So don't set nextPage to
             * maxClonePgs when we reach end of segment.
             */
            if (*nextPageP != -1 && *nextPageP > bfap->maxClonePgs) {
                /* Don't allow nextPage to progress beyond maxClonePgs. */
                *nextPageP = bfap->maxClonePgs;
            }
        } else {
            ret = FALSE;
            *nextPageP = -1;
        }
    } else {
        /* Holes and permanent holes return FALSE */
        ret = page_is_mapped_local(bfap, pg, nextPageP, FALSE,
                                   vdIndexP, vdBlkP, start_pgP, TRUE);

        MS_SMP_ASSERT(clone_nextPage <= bfap->maxClonePgs);
        if (ret == FALSE) {   /* hole or permanent hole or not mapped*/
            /* Permanent holes return TRUE */
            ret = page_is_mapped_local(bfap, pg, nextPageP, TRUE,
                                       vdIndexP, vdBlkP, start_pgP, TRUE);
            if (*nextPageP == -1) { /* pg is off the end of file or end of
                                   * stripe */
                clone_nextPage = NEXT_SEGMENT_PAGE(pg,segmentSize);
                if (clone_nextPage >= bfap->maxClonePgs) {
                    clone_nextPage = bfap->maxClonePgs;
                    *use_orig_bfap = TRUE; /* no more clone extents */
                }
            } else {
                clone_nextPage = *nextPageP;
            }
            if (ret == FALSE) {/* its normal hole, look at original */
                ret = page_is_mapped_local(orig_bfap, pg, nextPageP, TRUE,
                                      vdIndexP, vdBlkP, start_pgP, TRUE);
                if (*nextPageP > clone_nextPage)
                    *nextPageP = clone_nextPage;
            } else {
                /* Permanent hole, so consider as hole in clone */
                ret = FALSE;
            }
        }
    }

    return ret;
}

/*
 * bs_get_bkup_xtnt_map
 *
 * ABSTRACT
 * fetch an optimized extent map for regular file, regular file clone,
 * stripe file, or stripe file clone (coming soon) for vdump.
 *
 * BODY
 * if its a normal advfs file - call bs_get_bf_xtnt_map and return map.
 *
 * if its a normal advfs "file clone" it
 * gets the original extent map merged with the clone's extent map
 * returning a composite of both maps.
 *
 * if its a striped advfs file it gets the 1.) original extent maps,
 * 2.) converts them to a bitfile representation, 3.)merges the multiple
 * maps into a single map.
 * It returns the final merged xtnt map.
 *
 * If its a "striped clone" map that needs to be returned, perform the
 * same steps as for a striped file, listed above, except merge each stripe
 * file's extent map and its clone into a single map before performing step 2.
 * This merge is performed on normal "file clone" files already.
 *
 * NOTE: DO NOT USE THIS ROUTINE TO RETURN BITMAP XTNT MAPS FOR FILES.  USE
 *       BS_GET_BF_XTNT_MAP.  THIS ROUTINE MODIFIES THE XTNT MAPS FOR USE BY
 *       VDUMP.
 *
 * Returns EINVALID_HANDLE, EBAD_PARAMS, E_NOT_ENOUGH_XTNTS,
 *         ENOT_SUPPORTED(yet) or EOK
 */

statusT
bs_get_bkup_xtnt_map(
    bfAccessT *bfap,             /* in - clone access structure */
    int startXtntMap,            /* in - clone extent map at which to start */
    int startXtnt,               /* in - clone extent at which to start */
    int xtntArraySize,           /* in - clone size of extent array */
    int flags,                   /* in - what work to do */
    bsExtentDescT xtntsArray[],  /* in-out(only if xtntCnt !=0)merged array */
    int *xtntCnt,                /* in-out - num xtnt entries for merged map */
    vdIndexT *allocVdIndex,      /* out - disk on which next stg is alloc'd. */
    int *pageCnt                 /* out - count of pages for merged map*/
)
{
    statusT sts;
    int cloned;
    bfAccessT *fromBfap=bfap;
    int do_clonextnt_unlock=0;

    /*
     * Check validity of bfap.
     */
    if (bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    switch (bfap->xtnts.type)  {

        case BSXMT_APPEND:

            if (bfap->cloneId == 0) {
                /* file is a regular file or a clone without any changes */
                if (bfap->bfSetp->cloneId != BS_BFSET_ORIG){

                    /* This is a clone that has not had any metadata
                     * (or file pages) COWed. At open time the
                     * original's extents were loaded into it's (the
                     * clone's) in memory extent map.  This is
                     * basically a bug (to be addressed at some future
                     * time). We can not trust these extents since the
                     * original could have been migrated (which does
                     * not cause a COW).  So we need to use the
                     * original's bfap not the clone's */
                    
                    if (clu_is_ready())
                    {
                        /* Since we are about to use the original
                         * access structure for obtaining extent maps,
                         * we must protect against it being migrated
                         * or COWed. Migrate will get the
                         * clu_clonextnt_lk on the clone when
                         * migrating the original and bs_cow/bs_cow_pg
                         * will get the COW lock. So this will protect
                         * us here. 
                         */
                        
                        
                        CLU_CLXTNT_READ(&bfap->clu_clonextnt_lk);
                        COW_LOCK_READ( &(bfap->origAccp->cow_lk) )
                        do_clonextnt_unlock=1;

                    }

                    fromBfap=bfap->origAccp;
                    MS_SMP_ASSERT(fromBfap);
                }

              sts = bs_get_bf_xtnt_map( fromBfap,
                                        startXtntMap,
                                        startXtnt,
                                        xtntArraySize,
                                        xtntsArray,
                                        xtntCnt,
                                        allocVdIndex);
              *pageCnt = 0;

            } else if (bfap->cloneId > 0) {
                /* file is a regular(unstriped) file clone */
              sts = bs_get_clone_xtnt_map( bfap,
                                           startXtntMap,
                                           startXtnt,
                                           xtntArraySize,
                                           flags,
                                           xtntsArray,
                                           xtntCnt,
                                           allocVdIndex,
                                           pageCnt);

            } else {
                return EBAD_PARAMS;
            }

            break;

        case BSXMT_STRIPE:
            if(bfap->cloneId == 0) {
                /* file is a stripe original or a stripe clone  */
                /* without any changes                          */
                if (bfap->bfSetp->cloneId != BS_BFSET_ORIG) {

                    /* This is a clone that has not had any metadata
                     * (or file pages) COWed. At open time the
                     * original's extents were loaded into it's (the
                     * clone's) in memory extent map.  This is
                     * basically a bug (to be addressed at some future
                     * time). We can not trust these extents since the
                     * original could have been migrated (which does
                     * not cause a COW).  So we need to use the
                     * original's bfap not the clone's */
                    
                    if (clu_is_ready())
                    {
                        /* Since we are about to use the original access structure
                         * for obtaining extent maps, we must protect against it
                         * being migrated. Migrate will get the clu_clonextnt_lk
                         * on the clone when migrating the original. So this
                         * will protect us here.
                         *
                         * NOTE: The clu_clonextnt_lk and cow_lk are obtained in 
                         * bs_get_stripe_xtnt_map but only when cloned is TRUE.
                         */

                        CLU_CLXTNT_READ(&bfap->clu_clonextnt_lk);
                        COW_LOCK_READ( &(bfap->origAccp->cow_lk) )
                        do_clonextnt_unlock=1;
                    }

                    fromBfap=bfap->origAccp;
                    MS_SMP_ASSERT(fromBfap);
                }

                cloned = FALSE;

            } else if (bfap->cloneId > 0) {
                /* file is a stripe clone */
                cloned = TRUE;
            } else {
                return EBAD_PARAMS;
            }

            sts = bs_get_stripe_xtnt_map( fromBfap,
                                          startXtntMap,
                                          startXtnt,
                                          xtntArraySize,
                                          flags,
                                          xtntsArray,
                                          xtntCnt,
                                          pageCnt,
                                          cloned);
            break;

        default:

            return EBAD_PARAMS;

    }  /* end switch */

    if (do_clonextnt_unlock) {
        if ( sts == EOK ) {
            /* protected by clu_clonextnt_lk & cow_lk */
            bfap->cloneXtntsRetrieved = 1;
        }
        COW_UNLOCK(&fromBfap->cow_lk); /* orig */
        CLU_CLXTNT_UNLOCK(&bfap->clu_clonextnt_lk);
    }

    return sts;

}  /* end bs_get_bkup_xtnt_map */


/*
 * bs_get_bf_page_cnt
 *
 * This function returns the bitfile's page count, the number of pages allocated
 * to the bitfile.
 */

statusT
bs_get_bf_page_cnt(
                   bfAccessT *bfap,   /* in */
                   uint32T *pageCnt       /* out */
                  )
{
    statusT sts = EOK;
    bfSetT *bfSetp;
    vdIndexT vdindex;
    int xtntCnt;

    bfSetp = bfap->bfSetp;

/* OWENS hope to simplify after aravinda's fix */

    /*
     * If the bitfile is in an original fileset or if
     * the bitfile is in a clone fileset but is 
     * completely instantiated by the original file,
     * use the page count from the access structure.
     * If the bitfile is a clone file with metadata,
     * get its page count from the merged extent map.
     */
    if ((bfSetp->cloneId == BS_BFSET_ORIG) ||
        (bfap->cloneId == BS_BFSET_ORIG)) {
        *pageCnt = bfap->xtnts.allocPageCnt;
        if (IDX_INDEXING_ENABLED(bfap) &&
            IDX_FILE_IS_DIRECTORY(bfap) && 
	    ((bsDirIdxRecT *)bfap->idx_params)->idx_bfap)
        {
            uint32T idx_pages;
            idx_pages = ((bsDirIdxRecT *)bfap->idx_params)->
                                  idx_bfap->xtnts.allocPageCnt;
            *pageCnt += idx_pages;
        }
    } else {
        sts = bs_get_bkup_xtnt_map( bfap,
                                    1,
                                    0,
                                    0,
                                    XTNT_PAGECNT_ONLY,
                                    NULL,
                                    &xtntCnt,
                                    &vdindex,
                                    (int *)pageCnt);

        if (!sts && IDX_INDEXING_ENABLED(bfap) &&
                    IDX_FILE_IS_DIRECTORY(bfap) &&
		    ((bsDirIdxRecT *)bfap->idx_params)->idx_bfap) {
            uint32T idx_pages;
            bfAccessT *idx_bfap = ((bsDirIdxRecT *)bfap->idx_params)->idx_bfap;

            if (idx_bfap->cloneId == BS_BFSET_ORIG) {
                idx_pages = idx_bfap->xtnts.allocPageCnt;
            } else {
                sts = bs_get_bkup_xtnt_map( idx_bfap,
                                    1,
                                    0,
                                    0,
                                    XTNT_PAGECNT_ONLY,
                                    NULL,
                                    &xtntCnt,
                                    &vdindex,
                                    &idx_pages);
            }

            if (!sts)
                *pageCnt += idx_pages;
        }
    }

    return sts;

}  /* end bs_get_bf_page_cnt */


/*
 * bs_get_bf_params
 *
 * Return the parameters for a bitfile.
 *
 * Returns EINVALID_HANDLE, or status frem bmtr_scan
 */

statusT
bs_get_bf_params(
                 bfAccessT *bfap,/* in - bfaccess structure */
                 bfParamsT *bfParams,  /* out - parameters for the bf */
                 int lock              /* in */
                )
{
    statusT sts = EOK;
    bsBfAttrT bfattr;

    /*
     * Check validity of bfap.
     */
    if (bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    sts = bmtr_get_rec( bfap, 
                        BSR_ATTR, 
                        (void *) &bfattr, 
                        sizeof( bfattr ) );
    if (sts != EOK) {
        return sts;
    }

    *bfParams              = bsNilBfParams;

    /* stuff obtained from the access struct */

    bfParams->tag          = bfap->tag;
    bfParams->cloneId      = bfap->cloneId;
    bfParams->cloneCnt     = bfap->cloneCnt;
    bfParams->vdIndex      = bfap->primVdIndex;
    XTNMAP_LOCK_READ( &(bfap->xtntMap_lk) )
    bfParams->numPages     = bfap->xtnts.allocPageCnt;
    bfParams->nextPage     = bfap->nextPage;
    bfParams->cl.extendSize   = bfap->extendSize;
    XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
    bfParams->type         = bfap->xtnts.type;
    bfParams->pageSize     = ADVFS_PGSZ_IN_BLKS;
    bfParams->cl.reqServices  = bfap->reqServices; 
    bfParams->cl.optServices  = bfap->optServices; 
    bfParams->fragId = bfap->fragId;
    bfParams->fragPageOffset = bfap->fragPageOffset;

    if (bfap->xtnts.type == BSXMT_STRIPE) {
        /*
         * Make sure that the in-memory extent maps are valid.
         * Returns with xtntMap_lk read-locked.
         */
        sts = x_load_inmem_xtnt_map( bfap, X_LOAD_REFERENCE);
        if (sts != EOK) {
            return sts;
        }
        bfParams->segmentCnt = bfap->xtnts.stripeXtntMap->cnt;
        bfParams->segmentSize = bfap->xtnts.stripeXtntMap->segmentSize;
        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
    } else {
        bfParams->segmentCnt = 0;
        bfParams->segmentSize = 0;
    }

    /* stuff obtained from the on-disk attributes */

    bfParams->cl.dataSafety   = bfattr.cl.dataSafety;
    bfParams->cl.rsvd1        = 0;
    bfParams->cl.rsvd2        = 0;

    bcopy( bfattr.cl.clientArea, 
           bfParams->cl.clientArea, 
           sizeof( bfParams->cl.clientArea ) );

    return sts;
}


/*
 * bs_set_bf_params
 *
 * set the parameters for a bitfile
 *
 * Returns EINVALID_HANDLE, ENOT_SUPPORTED,
 * E_READ_ONLY, and possible error return from bs_pinpg
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
        file_lock_held = FALSE,
        migtrunc_lock_held = FALSE;
    struct fsContext *contextp;
    struct vnode *vp;
    struct mount *mountp;
    
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
    if ((vp = ATOV(bfap)) && (mountp = VTOMOUNT(vp)) &&
        (mountp->m_flag & M_RDONLY)) {
        RAISE_EXCEPTION(E_READ_ONLY);
    }

    contextp = VTOC(bfap->bfVp);
    bfSetp = bfap->bfSetp;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return E_READ_ONLY;
    }

    if ( bfParams->cl.dataSafety == BFD_FTX_AGENT) {
            /*
             * Notify CFS that atomic write data logging is about to be
             * activated.  To avoid any potential cluster-wide deadlocks, we
             * must notify CFS before we acquire the file_lock.
             */
            (void)CC_CFS_AWDL_ENABLE(vp);
    }
        
    /*
     * Serialize with msfs_mmap() using the file_lock.
     * Hold this lock while we make any updates to the metadata 
     * and the in-memory access structure.
     * XXX This locking will change to using the bfaLock
     * when the granularity of the file_lock is reduced.
     * Sieze the MIGTRUNC lock to protect against changing 
     * data safety while the file is being migrated
     */
    FS_FILE_WRITE_LOCK(contextp);
    file_lock_held = TRUE;

    MIGTRUNC_LOCK_READ(&(bfap->xtnts.migTruncLk));
    migtrunc_lock_held = TRUE;

    sts = FTX_START_N( FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 1 );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    ftxStarted = TRUE;

    /* TODO - need to add some locking; can't rely on mcellList_lk */

    sts = bmtr_get_rec( bfap, 
                        BSR_ATTR, 
                        (void *) &bfattr, 
                        sizeof( bfattr ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    if (!SC_EQL( bfattr.cl.reqServices, bfParams->cl.reqServices ) ||
        !SC_EQL( bfattr.cl.optServices, bfParams->cl.optServices )) {

        XTNMAP_LOCK_WRITE( &(bfap->xtntMap_lk) ) 

        bfattr.cl.reqServices = bfParams->cl.reqServices; /* req serv class */
        bfattr.cl.optServices = bfParams->cl.optServices; /* opt serv class */
        bfap->reqServices = bfParams->cl.reqServices;  /* req service class */
        bfap->optServices = bfParams->cl.optServices;  /* opt service class */

        XTNMAP_UNLOCK( &(bfap->xtntMap_lk) )
    }

    bfattr.cl.rsvd1 = 0;
    bfattr.cl.rsvd2 = 0;

    if (bfParams->cl.extendSize > 0) {
        bfattr.cl.extendSize = bfap->extendSize = bfParams->cl.extendSize;
    }

    if ( bfParams->cl.dataSafety != bfattr.cl.dataSafety ) {
        /*
         * If the caller is requesting that we activate atomic write
         * data logging, first check to be sure that the file is
         * not memory-mapped.  Atomic write data logging and memory
         * mapping should be mutually exclusive.  Also, atomic write
         * data logging may not be activated on files with frags.
         * Allowing files with frags to use atomic write data logging
         * has produced "Can't clear a bit twice" crashes when accessing
         * such files after a crash.  We also don't allow data logging
         * to be activated if the file is open for direct I/O.
         */
        if ( bfParams->cl.dataSafety == BFD_FTX_AGENT) {

            /*
             * In the base, bfap->mmapCnt is 0 when the last mmapper
             * has exited.  In a cluster, bfap->mmapCnt is not 
             * maintained.  Instead, the vp->v_flag is set with
             * VMMAPPED.  This flag is cleared at cfs_inactive() time,
             * which means that the file may no longer be mmapped at
             * the time the flag is checked.  For further explanation,
             * see comments for msfs_unset_mmap().
             *
             * To prevent a regression in base behavior, we check
             * bfap->mmapCnt instead of VMMAPPED in the base.
             */

            if ((!clu_is_ready() && (bfap->mmapCnt > 0)) ||
                (clu_is_ready() && (vp->v_flag & VMMAPPED))) {
                    RAISE_EXCEPTION(ENOTSUP);
            }

            if ((vp->v_flag & VDIRECTIO) || (bfap->fragState != FS_FRAG_NONE)) {
                RAISE_EXCEPTION(ENOTSUP);
            }
        }
        bfattr.cl.dataSafety = bfParams->cl.dataSafety;
    }

    bcopy( bfParams->cl.clientArea, 
           bfattr.cl.clientArea, 
           sizeof( bfattr.cl.clientArea ) );

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

    /*
     * If the dataSafety value is changing, flush any dirty pages, and
     * update the access structure.
     */
    if (bfParams->cl.dataSafety != bfap->dataSafety) {

        /*
         * If we're deactivating atomic write data logging,
         * we have to make sure that a later replay of the
         * log won't overwrite data written after this
         * deactivation.  This requires us to checkpoint the
         * log file.
         * Also, inform the UBC of the change in status for
         * this file/object.  Data logging files may not have
         * their pages selected for UBC flushing. When
         * enabling data logging, tell UBC not to flush
         * dirty cache pages. When disabling data logging,
         * first flush the log and all data logged pages
         * before telling UBC it can now safely flush
         * future dirty cache pages.
         */
        
        if (bfap->dataSafety == BFD_FTX_AGENT) {
            /* Disable data_logging */
            lgr_checkpoint_log(bfap->dmnP);
            UBC_OBJECT_ALLOW_FLUSH(bfap->bfObj);
        }
        else {
            if (bfParams->cl.dataSafety == BFD_FTX_AGENT) {
                /* 
                 * Enable data logging.  Flush any dirty pages,
                 * including dirty mmapped pages.  Must flush preallocated
                 * pages to avoid corrupting UBC LRU's. 
                 */
                msfs_flush_and_invalidate(bfap, FLUSH_PREALLOC | NO_INVALIDATE);
                UBC_OBJECT_PREVENT_FLUSH(bfap->bfObj);
            }
        }

        bfap->dataSafety = bfParams->cl.dataSafety;

        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    }
    MIGTRUNC_UNLOCK(&(bfap->xtnts.migTruncLk));
    migtrunc_lock_held = FALSE;

    FS_FILE_UNLOCK(contextp);
    file_lock_held = FALSE;

    return EOK;

HANDLE_EXCEPTION:

    if (ftxStarted) {
        ftx_fail( ftxH );
    }

    if (migtrunc_lock_held) {
        MIGTRUNC_UNLOCK(&(bfap->xtnts.migTruncLk));
    }

    if (file_lock_held) {
        FS_FILE_UNLOCK(contextp);
    }

    return sts;
}


/*
 * bs_get_bf_iparams
 *
 * Return the inheritable parameters for a bitfile.
 *
 * Returns EINVALID_HANDLE, or status frem bmtr_scan
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

    bfIParams->dataSafety   = bfiattr.dataSafety;
    bfIParams->reqServices  = bfiattr.reqServices;
    bfIParams->optServices  = bfiattr.optServices;
    bfIParams->extendSize   = bfiattr.extendSize;

    bcopy( bfiattr.clientArea, 
           bfIParams->clientArea, 
           sizeof( bfIParams->clientArea ) );

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
    struct mount *mountp;
    
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
    if ((vp = ATOV(bfap)) && (mountp = VTOMOUNT(vp)) &&
        (mountp->m_flag & M_RDONLY)) {
        RAISE_EXCEPTION(E_READ_ONLY);
    }

    bfSetp = bfap->bfSetp;

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        /* Clones are read-only */
        return E_READ_ONLY;
    }

    sts = FTX_START_N( FTA_NULL, &ftxH, FtxNilFtxH, bfap->dmnP, 1 );
    if (sts != EOK) {
        return sts;
    }
    ftxStarted = TRUE;

    /* TODO - need to add some locking; can't rely on mcellList_lk */

    sts = bmtr_get_rec( bfap, 
                        BSR_BF_INHERIT_ATTR,
                        (void *) &bfiattr, 
                        sizeof( bfiattr ) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    if (!SC_EQL( bfiattr.reqServices, bfIParams->reqServices ) ||
        !SC_EQL( bfiattr.optServices, bfIParams->optServices )) {

        bfiattr.reqServices = bfIParams->reqServices; /* req service class */
        bfiattr.optServices = bfIParams->optServices; /* opt service class */
    }

    bfiattr.dataSafety = bfIParams->dataSafety;
    bfiattr.rsvd1      = 0;
    bfiattr.rsvd2      = 0;

    if (bfIParams->extendSize > 0) {
        bfiattr.extendSize = bfIParams->extendSize;
    }

    bcopy( bfIParams->clientArea, 
           bfiattr.clientArea, 
           sizeof( bfiattr.clientArea ) );

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
                   domainT *dmnP,          /* in - the domain pointer */
                   int vdIndexArrayLen,    /* in - number of ints in array */
                   uint32T vdIndexArray[], /* out - list of vd indices */
                   int *numVds             /* out - num vds put in array */
                  )
{
    int vdcnt, vdi;
    
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
 * from bmtr_scan
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
    dmnParams->bfSetDirTag = dmnP->bfSetDirTag;
    dmnParams->curNumVds   = dmnP->vdCnt;
    dmnParams->dmnVersion  = dmnP->dmnVersion;
    dmnParams->maxVds      = dmnP->maxVds;
    strcpy( dmnParams->domainName, dmnP->domainName );

    /*
     * We acquire no mutex here, so this is a race.
     * This is a struct copy.
     */
    dmnParams->bcStat      = dmnP->bcStat;
    dmnParams->bmtStat     = dmnP->bmtStat;

    dmnParams->ftxLogTag   = dmnP->ftxLogTag;
    dmnParams->ftxLogPgs   = dmnP->ftxLogPgs;
    dmnParams->mode        = dmnP->mode;
    dmnParams->uid         = dmnP->uid;
    dmnParams->gid         = dmnP->gid;

    return EOK;
}


/*
 * bs_set_dmn_params
 *
 * This routine sets the writable domain parameters in the domain
 * metadata.
 *
 * Returns EBAD_DOMAIN_POINTER, possible error return from
 * bmtr_scan, bs_pinpg, or bs_unpinpg.
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

    sts = FTX_START_N(FTA_NULL, &ftxH, FtxNilFtxH, dmnP, 0);
    if (sts != EOK) {
        return sts;
    }

    logvdp = VD_HTOP(BS_BFTAG_VDI(dmnP->ftxLogTag), dmnP);
    if ( RBMT_THERE(dmnP) ) {
        mdap = logvdp->rbmtp;
    } else {
        mdap = logvdp->bmtp;
    }

    /*
     * find domain mutable attributes record
     */
    mcid = mdap->primMCId;
    lock_read( &mdap->mcellList_lk.lock );
    sts = bmtr_scan( (void*)&dmnmattrp,
                     &pgref,
                     BSR_DMN_MATTR,
                     logvdp,
                     &mcid,
                     mdap->tag);
    lock_done( &mdap->mcellList_lk.lock );
    if ( sts != EOK ) {
        goto finished_set_dmn;
    }

    bs_derefpg(pgref, BS_CACHE_IT);

    sts = bs_pinpg( &pgref, (void*)&bmtpg, mdap, mcid.page, BS_NIL );
    if ( sts != EOK ) {
        goto finished_set_dmn;
    }

    dmnmattrp->uid = dmnParams->uid;
    dmnmattrp->gid = dmnParams->gid;
    dmnmattrp->mode = dmnParams->mode;

    sts = bs_unpinpg( pgref, logNilRecord, BS_DIRTY );

finished_set_dmn:
    ftx_quit(ftxH);

    return sts;
}


/*
 * bs_get_vd_params
 *
 * Get the parameters for a vd
 *
 * Returns EBAD_DOMAIN_POINTER, EBAD_VDI, possible error
 * return from bmtr_scan.
 */

statusT
bs_get_vd_params(
                 domainT *dmnP,          /* in - domain pointer */
                 uint32T vdIndex,        /* in - vd index */
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
    vdParams->bmtXtntPgs   = vdp->bmtXtntPgs;

    /* fetch cumulative values to provide interval statistics */
    if (flag & GETVOLPARAMS2) {
        vdp->dStat.blockingQ = vdp->blockingQ.queue_cnt;
        vdp->dStat.ubcReqQ = vdp->ubcReqQ.queue_cnt;
        vdp->dStat.flushQ = vdp->flushQ.queue_cnt;
        vdp->dStat.waitLazyQ = vdp->waitLazyQ.queue_cnt;
        vdp->dStat.readyLazyQ = vdp->readyLazyQ.queue_cnt;
        vdp->dStat.consolQ = vdp->consolQ.queue_cnt;
        vdp->dStat.devQ = vdp->devQ.queue_cnt;
    }
    /* fetch snapshot statistics */
    else {
        vdp->dStat.blockingQ = vdp->blockingQ.ioQLen;
        vdp->dStat.ubcReqQ = vdp->ubcReqQ.ioQLen;
        vdp->dStat.flushQ = vdp->flushQ.ioQLen;
        vdp->dStat.waitLazyQ = vdp->waitLazyQ.ioQLen;
        vdp->dStat.readyLazyQ = vdp->readyLazyQ.ioQLen;
        vdp->dStat.consolQ = vdp->consolQ.ioQLen;
        vdp->dStat.devQ = vdp->devQ.ioQLen;
    }

    vdParams->dStat = vdp->dStat;

    /*
     * Parameters that can be varied.
     */
    vdParams->rdMaxIo = vdp->rdmaxio;
    vdParams->wrMaxIo = vdp->wrmaxio;
    vdParams->qtoDev = vdp->qtodev;
    vdParams->blockingFact = vdp->blockingFact;
    vdParams->consolidate = vdp->consolidate;
    vdParams->lazyThresh = vdp->readyLazyQ.lenLimit;
    vdParams->vdSize = vdp->vdSize;

    /* Note that vdp->vdSize above is the in-use size (raw size & ~0x0f) */

    vdParams->max_iosize_rd = vdp->max_iosize_rd;
    vdParams->max_iosize_wr = vdp->max_iosize_wr;
    vdParams->preferred_iosize_rd = vdp->preferred_iosize_rd;
    vdParams->preferred_iosize_wr = vdp->preferred_iosize_wr;

    /*
     * "flag" is used to specify whether or not to
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


    if ( RBMT_THERE(dmnP) ) {
        mdap = vdp->rbmtp;
    } else {
        mdap = vdp->bmtp;
    }

    /*
     * find vd attributes record  (in BMT metadata cell)
     */
    mcid = mdap->primMCId;
    lock_read( &mdap->mcellList_lk.lock );
    sts = bmtr_scan( (void*)&vdattrp,
                     &pgref,
                     BSR_VD_ATTR,
                     vdp,
                     &mcid,
                     mdap->tag);
    lock_done( &mdap->mcellList_lk.lock );
    if ( sts != EOK ) {
        vd_dec_refcnt( vdp );
        return sts;
    }

    vdParams->vdMntId      = vdattrp->vdMntId;
    vdParams->vdState      = vdattrp->state;
    vdParams->vdIndex      = vdattrp->vdIndex;
    vdParams->stgCluster   = vdattrp->stgCluster;
    vdParams->serviceClass = vdattrp->serviceClass;
    vdParams->maxPgSz      = vdattrp->maxPgSz;

    bs_derefpg(pgref, BS_CACHE_IT);

    vd_dec_refcnt( vdp );
    return sts;
}


/*
 * bs_set_vd_params
 *
 * set the parameters for a virtual disk
 *
 * Returns EBAD_DOMAIN_POINTER, possible error return from
 * bmtr_scan, bs_pinpg, bs_unpinpg
 */

statusT
bs_set_vd_params(
                 domainT *dmnP,         /* in - domain pointer */
                 uint32T vdIndex,       /* in - vd index */
                 bsVdParamsT *vdParams  /* in - vd parameters */
                )
{
    statusT sts;
    struct vd *vdp;
    vdIoParamsT vdIoParams;
    ftxHT ftx;

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
     * Can't set I/O size to be greater than device max.
     */
    if (vdParams->rdMaxIo > (vdp->max_iosize_rd / DEV_BSIZE)) {
        ms_uprintf("Warning: %s: Decreasing maximum read transfer size to %d bytes\n",
                   vdp->vdName, vdp->max_iosize_rd);
        vdParams->rdMaxIo = MIN(vdParams->rdMaxIo,
                                    (vdp->max_iosize_rd / DEV_BSIZE));
    }
    if (vdParams->wrMaxIo > (vdp->max_iosize_wr / DEV_BSIZE)) {
        ms_uprintf("Warning: %s: Decreasing maximum write transfer size to %d bytes\n",
                   vdp->vdName, vdp->max_iosize_wr);
        vdParams->wrMaxIo = MIN(vdParams->wrMaxIo,
                                    (vdp->max_iosize_wr / DEV_BSIZE));
    }

    /*
     * Parameters that can be varied.
     */
    vdp->rdmaxio = vdParams->rdMaxIo;
    vdp->wrmaxio = vdParams->wrMaxIo;
    vdp->qtodev = vdParams->qtoDev;
    vdp->blockingFact = vdParams->blockingFact;
    vdp->consolidate = vdParams->consolidate;
    vdp->readyLazyQ.lenLimit = vdParams->lazyThresh;

    vdIoParams.rdMaxIo = vdParams->rdMaxIo;
    vdIoParams.wrMaxIo = vdParams->wrMaxIo;
    vdIoParams.qtoDev = vdParams->qtoDev;
    vdIoParams.consolidate = vdParams->consolidate;
    vdIoParams.blockingFact = vdParams->blockingFact;
    vdIoParams.lazyThresh = vdParams->lazyThresh;

    sts = FTX_START_N(FTA_NULL, &ftx, FtxNilFtxH, dmnP, 0);
    if (sts != EOK) {
        ADVFS_SAD1("bs_set_vd_params(1) - failed to create xact", sts);
    }

    /*
     * Install I/O params in the BMT (V3) or RBMT (V4).
     */
    sts = bmtr_put_rec( 
        (RBMT_THERE(dmnP) ? vdp->rbmtp : vdp->bmtp), 
        BSR_VD_IO_PARAMS, 
        &vdIoParams, 
        sizeof( vdIoParamsT ),
        ftx );

    ftx_done_n( ftx, FTA_BS_SET_VD_PARAMS );

    vd_dec_refcnt( vdp );
    return sts;
}


long
bs_get_avail_mcells(
                    domainT *dmnP
                   )
{
    int vdi;
    int vdcnt;
    int freeBlks;
    long potentialMcells;  /* number of mcells that could be allocated */
    long availMcells = 0;
    struct vd* vdp;
    bfAccessT *bmtap;

    /*
     * For each disk in the domain we calculate the number of mcells and
     * roll it up into a per domain number of mcells.
     */

    vdcnt = vdi = 1;

    while ((vdi <= BS_MAX_VDI) && (vdcnt <= dmnP->vdCnt )) {

        if ( vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE) ) {
            /*
             * NOTE:  I don't acquire the storage bitmap lock because
             * vdp->freeClust could change as soon as I release the lock.
             * In addition, vdp->freeClust is the only changeable vd field
             * that I use.  If I grabbed multiple changeable fields from
             * the vd, I would acquire the appropriate locks so that the
             * fields would be in sync relative to each other.
             */
            freeBlks = vdp->freeClust * vdp->stgCluster;

            /* total mcells (number of pages times mcells per page) */
            bmtap = vdp->bmtp;

            XTNMAP_LOCK_READ( &(bmtap->xtntMap_lk) )

            /*
             * The number of potential bitfiles is limited by the
             * space available for tags and the space available for
             * mcells.  Good luck trying to understand the following
             * computation.  It amounts to multiplying the number of 
             * pages by the harmonic mean of BS_TD_TAGS_PG and BSPG_CELLS.
             */
            potentialMcells = ((freeBlks / ADVFS_PGSZ_IN_BLKS) * BS_TD_TAGS_PG
                               * BSPG_CELLS) / (BS_TD_TAGS_PG + BSPG_CELLS);

            availMcells += bmtap->nextPage * BSPG_CELLS + potentialMcells;

            XTNMAP_UNLOCK( &(bmtap->xtntMap_lk) )

            /* decrement vdRefCnt on vdp before going on to next disk */
            vd_dec_refcnt( vdp );
            vdcnt++;
        }

        vdi++;
    }

    /*
     * only report up to the current max our tag files can handle.
     */
    if (availMcells > MAX_ADVFS_TAGS) {
    
        availMcells = MAX_ADVFS_TAGS;
    }

    return availMcells;
}

/*
 * bs_get_smsync_stats
 *
 * Get the smooth sync statistics for a vd
 *
 * Returns EBAD_DOMAIN_POINTER, EBAD_VDI.
 */

statusT
bs_get_smsync_stats(
                 domainT *dmnP,          /* in - domain pointer */
                 uint32T vdIndex,        /* in - vd index */
                 uint32T *smsyncQ_cnt,   /* out - cnt of bufs added to set of queues */
                 uint32T *smsync         /* out - snapshot of queue lengths */
                )
{
    struct vd *vdp;
    int i;

    /*
     * Check validity of pointer and vd index
     */
    if (dmnP == NULL) {
        return EBAD_DOMAIN_POINTER;
    }
 
    /* Bump the vdRefCnt on this vdp so it can't be removed while
     * we are examining it.  Decrement the refcnt before returning.
     */
    if ( !(vdp = vd_htop_if_valid(vdIndex, dmnP, TRUE, FALSE)) ) {
        return EBAD_VDI;
    }

    *smsyncQ_cnt = 0;

    smsync[0] = vdp->smSyncQ0.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ0.queue_cnt;

    smsync[1] = vdp->smSyncQ1.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ1.queue_cnt;

    smsync[2] = vdp->smSyncQ2.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ2.queue_cnt;

    smsync[3] = vdp->smSyncQ3.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ3.queue_cnt;

    smsync[4] = vdp->smSyncQ4.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ4.queue_cnt;

    smsync[5] = vdp->smSyncQ5.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ5.queue_cnt;

    smsync[6] = vdp->smSyncQ6.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ6.queue_cnt;

    smsync[7] = vdp->smSyncQ7.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ7.queue_cnt;

    smsync[8] = vdp->smSyncQ8.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ8.queue_cnt;

    smsync[9] = vdp->smSyncQ9.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ9.queue_cnt;

    smsync[10] = vdp->smSyncQ10.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ10.queue_cnt;

    smsync[11] = vdp->smSyncQ11.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ11.queue_cnt;

    smsync[12] = vdp->smSyncQ12.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ12.queue_cnt;

    smsync[13] = vdp->smSyncQ13.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ13.queue_cnt;

    smsync[14] = vdp->smSyncQ14.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ14.queue_cnt;

    smsync[15] = vdp->smSyncQ15.ioQLen;
    *smsyncQ_cnt += vdp->smSyncQ15.queue_cnt;

    vd_dec_refcnt( vdp );
    return 0;
}

