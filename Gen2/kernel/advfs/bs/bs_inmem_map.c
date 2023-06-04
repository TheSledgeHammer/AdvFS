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
 *  bs_inmem_map.c
 *  This module contains routines related to in-memory extent maps.
 *
 */

#include <sys/fcntl.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <bs_extents.h>
#include <bs_inmem_map.h>
#include <bs_stg.h>
#include <bs_bmt.h>

#define ADVFS_MODULE BS_INMEM_MAP

/*
 * Private protos
 */

static
bf_fob_t
get_sub_xtnt_map_alloc_fob_cnt (
                                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                                 bf_fob_t fob_offset,   /* in */
                                 uint32_t descStart,  /* in */
                                 bf_fob_t end_fob_offset,  /* in */
                                 uint32_t descEnd  /* in */
                                 );

void
imm_print_subXtnt_map( 
                      bsInMemSubXtntMapT *subXtntMap, 
                      int index 
                     );
void
imm_print_xtnt_map (
                    bsInMemXtntMapT *xtntMap  /* in */
                    );
/*
 * Prototypes of functions used by fcntl(), which calls
 * advfs_get_extent_map()
 */

typedef struct xtntMap {
    struct xtntMap *nextMap;
    int xtntMapNum;
    int curXtnt;
    int xtntCnt;
    bsExtentDescT *xtnts;
} xtntMapT;

typedef struct xtntMapDesc {
    unsigned long fileLength;           /* Total size of file in bytes */
    struct xtntMap *nextMap;
} xtntMapDescT;

static
void
free_xtntmap( 
             xtntMapDescT *xmd
             );

static
statusT
get_real_xtntmap( 
                 struct bfAccess *bfap,
                 xtntMapDescT **xmd,
                 int xtnt_use
                 );

static statusT
imm_load_sub_xtnt_map(
                      bfAccessT *bfap,                 /* in */
                      bsInMemXtntMapT *xtntMap,        /* in */
                      bsInMemSubXtntMapT *subXtntMap   /* in */
                      );


bsInMemSubXtntMapT bsNilSubXtntMap = {0};
bsXtntT NilBsXA = {0};


/*
 * imm_create_xtnt_map
 *
 * This function creates and initializes an in-memory extent map.  If
 * "maxCnt" is 0, it creates an array having a default number of
 * entries.
 */

statusT
imm_create_xtnt_map (
                     domainT *domain,  /* in */
                     uint32_t maxCnt,  /* in */
                     bsInMemXtntMapT **newXtntMap  /* out */
                     )
{
    statusT sts;
    uint32_t i;
    bsInMemXtntMapT *xtntMap;
    bsInMemSubXtntMapT *subXtntMap;

    /* All extent map structure fields get initialized. */
    xtntMap = (bsInMemXtntMapT *) ms_malloc_no_bzero(sizeof(bsInMemXtntMapT));
    xtntMap->domain = domain;
    xtntMap->hdrType = 0;
    xtntMap->hdrMcellId = bsNilMCId;
    xtntMap->bsxmNextFob = 0;
    xtntMap->allocVdIndex = -1;
    xtntMap->origStart = 0;
    xtntMap->origEnd = 0;
    xtntMap->updateStart = 0;
    xtntMap->updateEnd = 0;
    xtntMap->validCnt = 0;
    xtntMap->cnt = 0;
    if (maxCnt > 0) {
        xtntMap->maxCnt = maxCnt;
    } else {
        xtntMap->maxCnt = 1;
    }

    xtntMap->subXtntMap = (bsInMemSubXtntMapT *)
                       ms_malloc(xtntMap->maxCnt * sizeof(bsInMemSubXtntMapT));

    *newXtntMap = xtntMap;

    return EOK;

}  /* imm_create_xtnt_map */

/*
 * imm_extend_xtnt_map
 *
 * This function extends the in-memory extent map by extending the sub
 * extent map array.
 */

statusT
imm_extend_xtnt_map ( bfAccessT *bfap,
                      bsInMemXtntMapT *xtntMap,
                      xtntMapFlagsT flags
    )
{
    uint32_t i;
    bsInMemSubXtntMapT *newSubXtntMap;
    bsInMemSubXtntMapT *tmpSubXtntMap;
    uint32_t maxCnt;

    /*
     * We allocate the in-memory stuff first so that if we run out
     * of memory we don't have to undo anything.
     */

    maxCnt = xtntMap->maxCnt + 1;
    newSubXtntMap = (bsInMemSubXtntMapT*)
                     ms_malloc_no_bzero(maxCnt * sizeof(bsInMemSubXtntMapT));

    /* Zero the new subextent map entries that are not overwritten. */
    for (i = xtntMap->maxCnt; i < maxCnt; i++) {
        newSubXtntMap[i] = bsNilSubXtntMap;
    }
    /* Copy the original subextent maps to the new one. */
    for (i = 0; i < xtntMap->maxCnt; i++) {
        newSubXtntMap[i] = xtntMap->subXtntMap[i];
    }

    if (flags & XTNT_ORIG_XTNTMAP)
    {
        /* We are about to modify the extentMaps that
         * are in the access structure. Other threads
         * could be attempting to read these so we 
         * must get the write lock.
         */

        MS_SMP_ASSERT(bfap != NULL);
        ADVRWL_XTNTMAP_WRITE( &bfap->xtntMap_lk );
        ADVRWL_VHAND_XTNTMAP_WRITE(&bfap->vhand_xtntMap_lk);

        tmpSubXtntMap =  xtntMap->subXtntMap;

        xtntMap->subXtntMap = newSubXtntMap;
        xtntMap->maxCnt = maxCnt;

        ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
        ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );
    } else {
        tmpSubXtntMap =  xtntMap->subXtntMap;

        xtntMap->subXtntMap = newSubXtntMap;
        xtntMap->maxCnt = maxCnt;
    }

    ms_free (tmpSubXtntMap);
    return EOK;

}  /* end imm_extend_xtnt_map */

/*
 * imm_get_first_xtnt_desc
 *
 * This function returns the specified extent map's first extent descriptor
 * and identifier.
 *
 * If the extent map is empty, the function sets "xtntDesc" fob count,
 * fob offset and disk index to 0 and block offset to -1.
 *
 * The caller must have read access to the extent map.
 */


void
imm_get_first_xtnt_desc (
                         bsInMemXtntMapT *xtntMap,  /* in */
                         bsInMemXtntDescIdT *xtntDescId,  /* out */
                         bsXtntDescT *xtntDesc  /* out */
                         )
{
    uint32_t i;
    bsInMemSubXtntMapT *subXtntMap;

    /*
     * Find the first sub extent map with descriptors other than just the
     * termination extent descriptor.
     */

    for (i = 0; i < xtntMap->validCnt; i++) {
        subXtntMap = &(xtntMap->subXtntMap[i]);
        if (subXtntMap->cnt > 1) {
            xtntDescId->subXtntMapIndex = i;
            xtntDescId->xtntDescIndex = 0;
            xtntDesc->bsxdFobOffset = 
                subXtntMap->bsXA[0].bsx_fob_offset;
            xtntDesc->bsxdFobCnt =
                subXtntMap->bsXA[1].bsx_fob_offset - 
                xtntDesc->bsxdFobOffset;
            xtntDesc->volIndex = FETCH_MC_VOLUME(subXtntMap->mcellId);
            xtntDesc->bsxdVdBlk = subXtntMap->bsXA[0].bsx_vd_blk;
            return;
        }
    }  /* end for */

    xtntDescId->subXtntMapIndex = xtntMap->validCnt;
    xtntDescId->xtntDescIndex = 0;
    xtntDesc->bsxdFobOffset = 0;
    xtntDesc->bsxdFobCnt = 0;
    xtntDesc->volIndex = bsNilVdIndex;
    xtntDesc->bsxdVdBlk = -1;

    return;

}  /* end imm_get_first_xtnt_desc */

/*
 * imm_get_xtnt_desc
 *
 * This function returns the extent descriptor and its id that maps the
 * specified fob.  The extent descriptor is returned in "xtntDesc" and
 * its id is returned in "xtntDescId".
 *
 * If the fob is not mapped by the extent map, this function sets "xtntDesc"
 * fob count, fob offset and disk index to 0 and block offset to -1.
 *
 * If the fob is part of a permanent hole (bsx_vd_blk = -2) the fob is "mapped".
 * fob count, fob offset and disk index are set up. Block offset is set to -2.
 *
 * If updateFlg is set, only the update subextents will be searched.
 *
 * The caller must have read access to the extent map.
 *
 * If the fob_offset is not mapped, E_RANGE_NOT_MAPPED will be returned.
 */


statusT
imm_get_xtnt_desc (
                   bf_fob_t fob_offset,  /* in */
                   bsInMemXtntMapT *xtntMap,  /* in */
                   int updateFlg,  /* in */
                   bsInMemXtntDescIdT *xtntDescId,  /* out */
                   bsXtntDescT *xtntDesc  /* out */
                   )
{
    bsInMemSubXtntMapT *subXtntMap;
    uint32_t subXtntMapIndex;
    statusT sts;
    uint32_t xtntDescIndex;

    sts = imm_fob_to_sub_xtnt_map( fob_offset,
                                    xtntMap,
                                    updateFlg,
                                    &subXtntMapIndex);
    if ((sts == EOK) || (sts == E_STG_HOLE)) {
        subXtntMap = &(xtntMap->subXtntMap[subXtntMapIndex]);
        sts = imm_fob_to_xtnt( fob_offset,
                               subXtntMap,
                               updateFlg,
                               FALSE,       /* perm hole returns E_STG_HOLE */
                               &xtntDescIndex);

        if ((sts == EOK) || (sts == E_STG_HOLE)) {
            xtntDescId->subXtntMapIndex = subXtntMapIndex;
            xtntDescId->xtntDescIndex = xtntDescIndex;
            xtntDesc->bsxdFobOffset = 
                subXtntMap->bsXA[xtntDescIndex].bsx_fob_offset;
            xtntDesc->bsxdFobCnt = 
                subXtntMap->bsXA[xtntDescIndex + 1].bsx_fob_offset -
                subXtntMap->bsXA[xtntDescIndex].bsx_fob_offset;
            xtntDesc->volIndex = FETCH_MC_VOLUME(subXtntMap->mcellId);
            xtntDesc->bsxdVdBlk = subXtntMap->bsXA[xtntDescIndex].bsx_vd_blk;
            return 0;
        }
    }

    /*
     * The fob is not mapped.
     */

    xtntDescId->subXtntMapIndex = xtntMap->validCnt;
    xtntDescId->xtntDescIndex = 0;
    xtntDesc->bsxdFobOffset = 0;
    xtntDesc->bsxdFobCnt = 0;
    xtntDesc->volIndex = bsNilVdIndex;
    xtntDesc->bsxdVdBlk = -1;

    return sts;

}  /* end imm_get_xtnt_desc */

/*
 * imm_get_next_xtnt_desc
 *
 * This function returns the next extent descriptor and its id.  The previous
 * extent descriptor is specified by "xtntDescId".  The next extent descriptor
 * is returned in "xtntDesc" and its id is returned in "xtntDescId".
 *
 * If the previous extent descriptor's id is not valid or there are no more
 * extent descriptors, this function sets "xtntDesc" fob count, fob offset
 * and disk index to 0 and block offset to -1.
 *
 * If the function cannot acquire the next extent descriptor either because
 * of an invalid previous descriptor or the previous was the last
 * descriptor, an error of ENO_XTNTS will be returned.  Otherwise, EOK will
 * be returned.
 *
 * Before calling this function, you must call imm_get_first_xtnt_desc() or
 * imm_get_xtnt_desc() to set the previous extent descriptor's id.
 *
 * The caller must have read access to the extent map.
 */


statusT
imm_get_next_xtnt_desc (
                        bsInMemXtntMapT *xtntMap,  /* in */
                        bsInMemXtntDescIdT *xtntDescId,  /* in/out */
                        bsXtntDescT *xtntDesc  /* out */
                        )
{
    bsXtntT *bsXA = NULL;
    uint32_t i;
    bsInMemSubXtntMapT *subXtntMap;

    if (xtntDescId->subXtntMapIndex < xtntMap->validCnt) {
        subXtntMap = &(xtntMap->subXtntMap[xtntDescId->subXtntMapIndex]);
        if (xtntDescId->xtntDescIndex < (subXtntMap->cnt - 1)) {
            /*
             * The previous extent descriptor's id is valid.
             * Find the next descriptor.
             */
            xtntDescId->xtntDescIndex++;
            if (xtntDescId->xtntDescIndex < (subXtntMap->cnt - 1)) {
                bsXA = &(subXtntMap->bsXA[xtntDescId->xtntDescIndex]);
            } else {
                xtntDescId->subXtntMapIndex++;
                /*
                 * Find the next sub extent map with descriptors other than
                 * just the termination extent descriptor.
                 */
                for (i = xtntDescId->subXtntMapIndex; i < xtntMap->validCnt; i++) {
                    subXtntMap = &(xtntMap->subXtntMap[i]);
                    if (subXtntMap->cnt > 1) {
                        xtntDescId->subXtntMapIndex = i;
                        xtntDescId->xtntDescIndex = 0;
                        bsXA = &(subXtntMap->bsXA[xtntDescId->xtntDescIndex]);
                        break;  /* out of for */
                    }
                }  /* end for */
            }
            if (bsXA != NULL) {
                xtntDesc->bsxdFobOffset = bsXA->bsx_fob_offset;
                xtntDesc->bsxdFobCnt =
                        subXtntMap->bsXA[xtntDescId->xtntDescIndex + 1].bsx_fob_offset -
                        bsXA->bsx_fob_offset;
                xtntDesc->volIndex = FETCH_MC_VOLUME(subXtntMap->mcellId);
                xtntDesc->bsxdVdBlk = bsXA->bsx_vd_blk;
                return EOK;
            }
        }
    }

    /*
     * There is no next extent descriptor either because the previous extent
     * descriptor is the last one or the previous extent descriptor's id was bad.
     */

    xtntDescId->subXtntMapIndex = xtntMap->validCnt;
    xtntDescId->xtntDescIndex = 0;
    xtntDesc->bsxdFobOffset = 0;
    xtntDesc->bsxdFobCnt = 0;
    xtntDesc->volIndex = bsNilVdIndex;
    xtntDesc->bsxdVdBlk = -1;

    return ENO_XTNTS;

}  /* end imm_get_next_xtnt_desc */

/*
 * imm_delete_xtnt_map
 *
 * This function deletes an in-memory extent map.
 */


void
imm_delete_xtnt_map (
                     bsInMemXtntMapT *xtntMap  /* in */
                     )
{
    uint32_t i;

    if (xtntMap != NULL) {
        imm_delete_sub_xtnt_maps (xtntMap);
        ms_free (xtntMap);
    }
    return;

}  /* end imm_delete_xtnt_map */

/*
 * imm_delete_sub_xtnt_maps
 *
 * This function deletes an in-memory extent map's sub extent maps.
 */


void
imm_delete_sub_xtnt_maps (
                          bsInMemXtntMapT *xtntMap  /* in */
                          )
{
    uint32_t i;

    if (xtntMap->subXtntMap != NULL) {
        for (i = 0; i < xtntMap->cnt; i++) {
            if (xtntMap->subXtntMap[i].bsXA != NULL) {
                ms_free (xtntMap->subXtntMap[i].bsXA);
            }
        }  /* end for */
        ms_free (xtntMap->subXtntMap);
    }
    return;

}  /* end imm_delete_sub_xtnt_maps */

/*
 * imm_init_sub_xtnt_map
 *
 * This function initializes an in-memory sub extent map, deallocates the
 * descriptor array, if any, and allocates a new descriptor array.
 * If "maxCnt" is 0, it creates an array having a default number of entries.
 *
 * NOTE: We do not need to obtain the extent map lock here since the
 * passed in sub extent map is not accessible due to other locking (ie mcell_list_lk).
 * Threads may be reading the extent maps but will not be in the update range.
 * Also the potential replacement of the descriptors takes place only within
 * the subextentmap which is not accessible.
 */


statusT
imm_init_sub_xtnt_map (
                       bsInMemSubXtntMapT *subXtntMap,  /* in */
                       bf_fob_t fob_offset,  /* in */
                       bf_fob_t fob_cnt,  /* in */
                       bfMCIdT mcellId,  /* in */
                       uint32_t type,  /* in */
                       uint32_t onDiskMaxCnt,  /* in */
                       uint32_t maxCnt  /* in */
                       )
{
    subXtntMap->bssxmFobOffset = fob_offset;
    subXtntMap->bssxmFobCnt = fob_cnt;
    subXtntMap->mcellId = mcellId;
    subXtntMap->mcellState = ADV_MCS_INITIALIZED;
    subXtntMap->type = type;
    subXtntMap->onDiskMaxCnt = onDiskMaxCnt;
    subXtntMap->updateStart = 1;
    subXtntMap->updateEnd = 0;
    subXtntMap->cnt = 1;

    switch ( type ) {
      case BSR_XTNTS:
        MS_SMP_ASSERT(onDiskMaxCnt == BMT_XTNTS); 
        if ( maxCnt ) {
            MS_SMP_ASSERT (maxCnt <= onDiskMaxCnt);
            subXtntMap->maxCnt = maxCnt;
        } else {
            subXtntMap->maxCnt = BMT_XTNTS;
        }
        break;
      case BSR_XTRA_XTNTS:
        MS_SMP_ASSERT(onDiskMaxCnt == BMT_XTRA_XTNTS);
        if ( maxCnt ) {
            MS_SMP_ASSERT(maxCnt <= onDiskMaxCnt);
            subXtntMap->maxCnt = maxCnt;
        } else {
            subXtntMap->maxCnt = 10;   /* arbitary. currently about 1/3 max */
        }
        break;
      default:
        MS_SMP_ASSERT(0);
    }

    subXtntMap->bsXA =
                 (bsXtntT *)ms_malloc(subXtntMap->maxCnt * sizeof(bsXtntT));
    subXtntMap->bsXA[0].bsx_fob_offset = fob_offset;
    subXtntMap->bsXA[0].bsx_vd_blk = XTNT_TERM;

    return EOK;

}  /* imm_init_sub_xtnt_map */

/*
 * imm_copy_sub_xtnt_map
 *
 * This function makes a copy of the specified in-memory sub extent map.
 */


statusT
imm_copy_sub_xtnt_map (
                       bfAccessT *bfap,                     /* in */
                       bsInMemXtntMapT *xtntMap,            /* in */
                       bsInMemSubXtntMapT *oldSubXtntMap,   /* in */
                       bsInMemSubXtntMapT *newSubXtntMap    /* in */
                       )
{
    uint32_t i;
    statusT sts;

    if (oldSubXtntMap->bsXA == NULL) {
        sts = imm_load_sub_xtnt_map( bfap, xtntMap, oldSubXtntMap);
        if (sts != EOK) {
            return sts;
        }
    }

    sts = imm_init_sub_xtnt_map (
                                 newSubXtntMap,
                                 oldSubXtntMap->bssxmFobOffset,
                                 oldSubXtntMap->bssxmFobCnt,
                                 oldSubXtntMap->mcellId,
                                 oldSubXtntMap->type,
                                 oldSubXtntMap->onDiskMaxCnt,
                                 oldSubXtntMap->maxCnt
                                 );
    if (sts != EOK) {
        return sts;
    }

    for (i = 0; i < oldSubXtntMap->cnt; i++) {
        newSubXtntMap->bsXA[i] = oldSubXtntMap->bsXA[i];
    }  /* end for */
    newSubXtntMap->cnt = oldSubXtntMap->cnt;

    newSubXtntMap->updateStart = newSubXtntMap->cnt;

    return EOK;

}  /* end imm_copy_sub_xtnt_map */

/*
 * imm_extend_sub_xtnt_map
 *
 * This function extends the in-memory sub extent map by extending the
 * extent array.
 *
 * NOTE: This routine does not need to obtain the xtntMap_lk when
 * growing the descriptor region in this extent map. This is because
 * we NEVER grow subxtntmaps in the original range of the extent map.
 * So no thread can be looking at this subextentmap while we are 
 * growing it.
 * This can also be used to grow a copy of the extent map but once
 * again the caller has exclusive access to the subxtntmap.
 */


statusT
imm_extend_sub_xtnt_map ( bsInMemSubXtntMapT *subXtntMap  /* in */
    )
{
    uint32_t i;
    bsXtntT *newBsXA;
    uint32_t newMaxCnt;

    newMaxCnt = subXtntMap->maxCnt + 10;
    if (newMaxCnt > subXtntMap->onDiskMaxCnt) {
        if (subXtntMap->maxCnt < subXtntMap->onDiskMaxCnt) {
            newMaxCnt = subXtntMap->onDiskMaxCnt;
        } else {
            if (subXtntMap->maxCnt == subXtntMap->onDiskMaxCnt) {
                /*
                 * Can't extend the array any further.
                 */
                return EXTND_FAILURE;
            } else {
                ADVFS_SAD2 ("imm_extend_sub_xtnt_map - "
                          "in-mem max (N1) > on-disk max (N2)",
                          subXtntMap->maxCnt, subXtntMap->onDiskMaxCnt);
            }
        }
    }

    /*
     * We allocate the in-memory stuff first so that if we run out
     * of memory we don't have to undo anything.
     */
    newBsXA = (bsXtntT *) ms_malloc_no_bzero(newMaxCnt * sizeof (bsXtntT));


    /* Zero the extents in the subextent map that are not ovewritten. */
    for (i = subXtntMap->maxCnt; i < newMaxCnt; i++) {
        newBsXA[i] = NilBsXA;
    }

    /* Copy the original extents to the new subextent map. */
    for (i = 0; i < subXtntMap->maxCnt; i++) {
        newBsXA[i] = subXtntMap->bsXA[i];
    }

    ms_free (subXtntMap->bsXA);

    subXtntMap->bsXA = newBsXA;
    subXtntMap->maxCnt = newMaxCnt;

    return EOK;

}  /* end imm_extend_sub_xtnt_map */

/*
 * imm_load_sub_xtnt_map
 *
 * This function loads the sub extent map's extent descriptor array
 * into the cache from its corresponding on-disk record.
 */


static statusT
imm_load_sub_xtnt_map (
                       bfAccessT *bfap,                  /* in */
                       bsInMemXtntMapT *xtntMap,         /* in */
                       bsInMemSubXtntMapT *subXtntMap    /* in */
                       )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint32_t bsXACnt;
    int derefFlag = 0;
    uint32_t i;
    bsMCT *mcell;
    bfMCIdT mcellId;
    bfPageRefHT pgRef;
    statusT sts;
    vdT *vd;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;

    if (subXtntMap->bsXA != NULL) {
        /*
         * Already in cache.
         */
        return EOK;
    }

    mcellId = subXtntMap->mcellId;

    vd = VD_HTOP(mcellId.volume, xtntMap->domain);

    MS_SMP_ASSERT(TEST_META_PAGE(mcellId.page, vd->bmtp) == EOK);
    sts = bs_refpg(&pgRef,
                   (void*)&bmt,
                   vd->bmtp,
                   mcellId.page,
                   FtxNilFtxH,
                   MF_VERIFY_PAGE);
    if (sts != EOK) {
        return sts;
    }
    derefFlag = 1;

    MS_SMP_ASSERT(TEST_CELL(mcellId.cell) == EOK);
    mcell = &(bmt->bsMCA[mcellId.cell]);
    if ( check_mcell_hdr(mcell, bfap) != EOK ) {
        RAISE_EXCEPTION(E_BAD_BMT);
    }

    switch (subXtntMap->type) {

      case BSR_XTNTS:

        xtntRec = bmtr_find( mcell, BSR_XTNTS, vd->dmnP);
        if (xtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        if ( CheckXtnts && check_BSR_XTNTS_rec(xtntRec, bfap, vd) != EOK ) {
            RAISE_EXCEPTION(E_BAD_BMT);
        }

        bsXACnt = xtntRec->xCnt;
        bsXA = xtntRec->bsXA;

        break;

      case BSR_XTRA_XTNTS:

        xtraXtntRec = bmtr_find( mcell, BSR_XTRA_XTNTS, vd->dmnP);
        if (xtraXtntRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        bsXACnt = xtraXtntRec->xCnt;
        bsXA = xtraXtntRec->bsXA;

        break;

      default:

        ADVFS_SAD0 ("imm_load_sub_xtnt_map - bad extent record type");

    }  /* end switch */

    subXtntMap->bsXA =
        (bsXtntT *)ms_malloc_no_bzero(bsXACnt * sizeof(bsXtntT));
    subXtntMap->cnt = bsXACnt;
    subXtntMap->updateStart = subXtntMap->cnt;
    subXtntMap->maxCnt = subXtntMap->cnt;

    for (i = 0; i < bsXACnt; i++) {
        subXtntMap->bsXA[i] = bsXA[i];
    }  /* end for */

    derefFlag = 0;
    sts = bs_derefpg (pgRef, BS_CACHE_IT);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    return EOK;

HANDLE_EXCEPTION:

    if (derefFlag != 0) {
        bs_derefpg (pgRef, BS_CACHE_IT);
    }
    subXtntMap->cnt = 0;
    if (subXtntMap->bsXA != NULL) {
        ms_free (subXtntMap->bsXA);
        subXtntMap->bsXA = NULL;
    }
    return sts;

}  /* end imm_load_sub_xtnt_map */

/*
 * imm_unload_sub_xtnt_map
 *
 * This function unloads the sub extent map's extent descriptor array from
 * the cache.
 */


void
imm_unload_sub_xtnt_map (
                         bsInMemSubXtntMapT *subXtntMap  /* in */
                         )
{
    if (subXtntMap->bsXA != NULL) {
        ms_free (subXtntMap->bsXA);
        subXtntMap->bsXA = NULL;
        subXtntMap->cnt = 0;  /* FIX - should I leave the count alone? */
    }
    return;

}  /* end imm_unload_sub_xtnt_map */

/*
 * imm_copy_xtnt_descs
 *
 * This function saves the extent descriptors in the next available
 * slots in the next available sub extent map.  If the next sub extent
 * map does not have room, it is expanded.  If it cannot be expanded,
 * the next sub extent map is initialized and the extent descriptor
 * is saved in the new sub extent map.
 *
 * This function assumes that if a descriptor describes a hole, the adjacent
 * descriptors, if any, describe real storage.
 *
 * This function does not copy hole descriptors unless the value of
 * "copyHoleFlag" is non-zero.  Otherwise, hole descriptors are not copied and
 * are created in the current sub extent map as they are needed.
 *
 * This function assumes that the copy extent array's last entry is a
 * termination extent descriptor.
 *
 * The copy count does not include the termination extent descriptor.
 */


statusT
imm_copy_xtnt_descs ( bfAccessT * bfap,      /* in */
                      vdIndexT copyVdIndex,  /* in */
                      bsXtntT *copyBsXA,  /* in */
                      uint32_t copyCnt,  /* in */
                      int16_t copyHoleFlag,  /* in */
                      bsInMemXtntMapT *xtntMap,    /* in */
                      xtntMapFlagsT flags /* in */
                     )
{
    uint32_t cnt;
    uint32_t dst;
    int initFlag = 0;
    int needCnt;
    bsInMemSubXtntMapT *newSubXtntMap;
    uint32_t src;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bfMCIdT initMcellId;

    if (copyCnt == 0) {
        return EOK;
    }

    if ( (copyCnt == 1) &&
         (copyBsXA[0].bsx_vd_blk == XTNT_TERM ||
          copyBsXA[0].bsx_vd_blk == COWED_HOLE) &&
         (copyHoleFlag == 0))
    {
        /*
         * The one copy descriptor describes a hole and the caller did not
         * specify that the hole should be copied.
         */
        return EOK;
    }

    if (xtntMap->cnt > 0) {
        /*
         * If the copy disk index does not match the next available
         * sub extent map's disk index, allocate a new sub extent map.
         *
         * Note that the next sub extent map's starting fob offset is
         * 1 greater than the previous sub extent map's last fob offset.
         */
        subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
        if (copyVdIndex != FETCH_MC_VOLUME(subXtntMap->mcellId)) {
            if (xtntMap->cnt >= xtntMap->maxCnt) {
                sts = imm_extend_xtnt_map (bfap, 
                                           xtntMap, 
                                           flags);
                if (sts != EOK) {
                    return sts;
                }
                subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
            }
            newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt]);
            initMcellId = bsNilMCId;
            initMcellId.volume = copyVdIndex;
            sts = imm_init_sub_xtnt_map (
                     newSubXtntMap,
                     subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset,
                     0,  /* fob_cnt*/
                     initMcellId,
                     BSR_XTRA_XTNTS,
                     BMT_XTRA_XTNTS,
                     0  /* force default maxCnt */
                    );
            if (sts != EOK) {
                return sts;
            }
            xtntMap->cnt++;
            subXtntMap = newSubXtntMap;
        }
    } else {
        /*
         * Empty extent map.
         */
        if (xtntMap->cnt >= xtntMap->maxCnt) {
            sts = imm_extend_xtnt_map (bfap,
                                       xtntMap, 
                                       flags);
            if (sts != EOK) {
                return sts;
            }
        }
        subXtntMap = &(xtntMap->subXtntMap[0]);

        initMcellId = bsNilMCId;
        initMcellId.volume = copyVdIndex;
        sts = imm_init_sub_xtnt_map (
                                     subXtntMap,
                                     0,  /* fob_offset*/
                                     0,  /* fob_cnt */
                                     initMcellId,
                                     BSR_XTNTS,
                                     BMT_XTNTS,
                                     0  /* force default maxCnt */
                                     );
        if (sts != EOK) {
            return sts;
        }
        xtntMap->cnt++;
    }

    /*
     * Copy the extent descriptors.
     */

    src = 0;
    dst = subXtntMap->cnt - 1;
    cnt = 0;
    while (cnt < copyCnt) {

        if ( (copyBsXA[src].bsx_vd_blk != XTNT_TERM &&
              copyBsXA[src].bsx_vd_blk != COWED_HOLE) ||
             (copyHoleFlag != 0) )
        {
            /*
             * Determine the number of new entries needed.
             */
            if (copyBsXA[src].bsx_fob_offset == 
                subXtntMap->bsXA[dst].bsx_fob_offset) {
                /*
                 * Copy onto the termination descriptor and create
                 * a new termination descriptor.
                 */
                needCnt = 2;
            } else {
                /*
                 * Convert the termination descriptor to a hole descriptor,
                 * copy the next descriptor and create a new termination
                 * descriptor.
                 */
                needCnt = 3;
            }

            if (needCnt > (subXtntMap->maxCnt - dst)) {
                /*
                 * Need more entries than are available.
                 */
                sts = imm_extend_sub_xtnt_map (subXtntMap);
                if ((sts != EOK) && (sts != EXTND_FAILURE)) {
                    return sts;
                }
                if ((sts == EXTND_FAILURE) ||
                    ((sts == EOK) && (needCnt > (subXtntMap->maxCnt - dst)))) {

                    /*
                     * Tie off the current sub extent map.
                     */

                    /* We need to be careful here not to end the subxtnt map with a hole.
                     * Check the next to last descriptor and if it is a hole move it to 
                     * the beginning of the newly obtained descriptor. NOTE we DO allow
                     * subxtnts to end with a permenant hole.
                     */

                    if ((dst > 0) && 
                        (subXtntMap->bsXA[dst-1].bsx_vd_blk == XTNT_TERM))
                    {
                        /* The hole now becomes the termination descriptor */
                        
                        dst--;
                        needCnt++;
                    }

                    subXtntMap->bssxmFobCnt = 
                        subXtntMap->bsXA[dst].bsx_fob_offset-
                        subXtntMap->bssxmFobOffset;

                    subXtntMap->cnt = dst + 1;

                    /*
                     * No more room in the sub extent map's extent array.
                     * Overflow into another sub extent map.
                     *
                     * Note that the next sub extent map's starting fob offset
                     * is 1 greater than the previous sub extent map's last
                     * fob offset.
                     */
                    if (xtntMap->cnt >= xtntMap->maxCnt) {
                        sts = imm_extend_xtnt_map (bfap,
                                                   xtntMap, 
                                                   flags);
                        if (sts != EOK) {
                            return sts;
                        }
                        subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
                    }
                    newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt]);
                    initMcellId = bsNilMCId;
                    initMcellId.volume = subXtntMap->mcellId.volume;
                    sts = imm_init_sub_xtnt_map (
                                  newSubXtntMap,
                                  subXtntMap->bsXA[dst].bsx_fob_offset,
                                  0,  /* fob_cnt*/
                                  initMcellId,
                                  BSR_XTRA_XTNTS,
                                  BMT_XTRA_XTNTS,
                                  needCnt
                                );
                    if (sts != EOK) {
                        return sts;
                    }

                    xtntMap->cnt++;
                    subXtntMap = newSubXtntMap;
                    dst = 0;
                }
            }

            if (copyBsXA[src].bsx_fob_offset != 
                subXtntMap->bsXA[dst].bsx_fob_offset) {
                /*
                 * The termination extent descriptor becomes a hole descriptor.
                 */
                dst++;
            }
            subXtntMap->bsXA[dst] = copyBsXA[src];
            dst++;
            subXtntMap->bsXA[dst].bsx_fob_offset = 
                copyBsXA[src + 1].bsx_fob_offset;
            subXtntMap->bsXA[dst].bsx_vd_blk = XTNT_TERM;
            
            MS_SMP_ASSERT((int) dst >= 0);
            MS_SMP_ASSERT(dst < subXtntMap->maxCnt);
        }
        src++;
        cnt++;
    }  /* end while */

    subXtntMap->bssxmFobCnt = 
        subXtntMap->bsXA[dst].bsx_fob_offset - 
        subXtntMap->bssxmFobOffset;

    subXtntMap->cnt = dst + 1;

    return EOK;

}  /* end imm_copy_xtnt_descs */

/*
 * imm_split_desc
 *
 * This function splits the specified descriptor into two descriptors at
 * the specified fob offset.
 */


void
imm_split_desc (
                bf_fob_t fob_offset,  /* in */
                bsXtntT *srcDesc,  /* in */
                bsXtntT *part1Desc,  /* in/modified */
                bsXtntT *part2Desc  /* in/modified */
                )
{
    *part1Desc = *srcDesc;
    part2Desc->bsx_fob_offset = fob_offset;
    if ( srcDesc->bsx_vd_blk == XTNT_TERM || srcDesc->bsx_vd_blk == COWED_HOLE) {
        part2Desc->bsx_vd_blk = srcDesc->bsx_vd_blk;
    } else {
        part2Desc->bsx_vd_blk = srcDesc->bsx_vd_blk +
          ((part2Desc->bsx_fob_offset - srcDesc->bsx_fob_offset) /
            ADVFS_FOBS_PER_DEV_BSIZE);
    }
    return;

}  /* end imm_split_desc */

/*
 * imm_get_alloc_fob_cnt
 *
 * This function calculates the number of allocated fobs described by
 * the extent map's sub extent maps.
 */


statusT
imm_get_alloc_fob_cnt (
                        bsInMemXtntMapT *xtntMap,  /* in */
                        bf_fob_t fob_offset,  /* in */
                        bf_fob_t fob_cnt,  /* in */
                        bf_fob_t *alloc_fob_cnt /* out */
                        )
{
    bsXtntT *bsXA;
    bf_fob_t temp_cnt;
    uint32_t descEnd;
    uint32_t descStart;
    bf_fob_t end_fob_offset;
    uint32_t i;
    uint32_t mapEnd;
    uint32_t mapStart;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bf_fob_t totalCnt = 0;

    if (fob_cnt == 0) {
        *alloc_fob_cnt= 0;
        return EOK;
    }

    end_fob_offset= (fob_offset + fob_cnt) - 1;

    /*
     * Find the sub extent map and the entry that maps the ending
     * fob offset.
     */

    sts = imm_fob_to_sub_xtnt_map (end_fob_offset, xtntMap, 0, &mapEnd);
    if (sts != EOK) {
        return sts;
    }

    sts = imm_fob_to_xtnt( end_fob_offset,
                            &(xtntMap->subXtntMap[mapEnd]),
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_STG_HOLE */
                            &descEnd);
    if ((sts != EOK) && (sts != E_STG_HOLE)) {
        return sts;
    }

    /*
     * Find the sub extent map and the entry that maps the starting
     * fob offset.
     */

    sts = imm_fob_to_sub_xtnt_map (fob_offset, xtntMap, 0, &mapStart);
    if (sts != EOK) {
        return sts;
    }

    sts = imm_fob_to_xtnt( fob_offset,
                            &(xtntMap->subXtntMap[mapStart]),
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_STG_HOLE */
                            &descStart);
    if ((sts != EOK) && (sts != E_STG_HOLE)) {
        return sts;
    }

    /*
     * Count the allocated fobs.
     */

    if (mapEnd == mapStart) {
        totalCnt = get_sub_xtnt_map_alloc_fob_cnt (
                                               &(xtntMap->subXtntMap[mapStart]),
                                               fob_offset,
                                               descStart,
                                               end_fob_offset,
                                               descEnd
                                              );
    } else {

        /*
         * Count the allocated fobs in the first sub extent map from the
         * first fob to the last fob described by the map.
         */
        subXtntMap = &(xtntMap->subXtntMap[mapStart]);
        bsXA = subXtntMap->bsXA;
        temp_cnt = get_sub_xtnt_map_alloc_fob_cnt (
                               subXtntMap,
                               fob_offset,
                               descStart,
                               bsXA[subXtntMap->cnt-1].bsx_fob_offset- 1,
                               subXtntMap->cnt - 2
                              );
        totalCnt = totalCnt + temp_cnt;
        mapStart++;

        /*
         * Count the allocated fobs in the last sub extent map from the
         * first fobs described by the map to the last fobs.
         */
        subXtntMap = &(xtntMap->subXtntMap[mapEnd]);
        bsXA = subXtntMap->bsXA;
        temp_cnt = get_sub_xtnt_map_alloc_fob_cnt (
                                               subXtntMap,
                                               bsXA[0].bsx_fob_offset,
                                               0,
                                               end_fob_offset,
                                               descEnd
                                               );
        totalCnt = totalCnt + temp_cnt;
        mapEnd--;

        /*
         * Count the allocated fobs in the sub extent maps between the
         * first and the last map.
         */
        for (i = mapStart; i <= mapEnd; i++) {
            subXtntMap = &(xtntMap->subXtntMap[i]);
            if (subXtntMap->cnt > 1) {
                bsXA = subXtntMap->bsXA;
                temp_cnt = get_sub_xtnt_map_alloc_fob_cnt (
                                  subXtntMap,
                                  bsXA[0].bsx_fob_offset,
                                  0,
                                  bsXA[subXtntMap->cnt-1].bsx_fob_offset-1,
                                  subXtntMap->cnt - 2
                                 );
                totalCnt = totalCnt + temp_cnt;
            }
        }  /* end for */
    }

    *alloc_fob_cnt= totalCnt;
    return EOK;

}  /* end imm_get_alloc_fob_cnt */

/*
 * get_sub_xtnt_map_alloc_fob_cnt
 *
 * This function calculates the number of allocated fobs described by
 * the sub extent map's extent array.
 */


static
bf_fob_t
get_sub_xtnt_map_alloc_fob_cnt (
                                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                                 bf_fob_t fob_offset,   /* in */
                                 uint32_t descStart,  /* in */
                                 bf_fob_t end_fob_offset,  /* in */
                                 uint32_t descEnd  /* in */
                                 )
{
    bf_fob_t fob_cnt;
    bsXtntT firstPart;
    uint32_t i;
    bsXtntT lastPart;
    
    /*
     * In order to find the end of descEnd, we need descEnd + 1, so make
     * sure we aren't already at the end of the sub extent map
     */
    MS_SMP_ASSERT( descEnd < (subXtntMap->cnt - 1) );

    if (descEnd == descStart) {
        if ( subXtntMap->bsXA[descStart].bsx_vd_blk != XTNT_TERM &&
             subXtntMap->bsXA[descStart].bsx_vd_blk != COWED_HOLE )
        {
            /*
             * Return the count in the descriptors requested that falls
             * within the requested range.
             */
            fob_cnt = MIN(subXtntMap->bsXA[descEnd + 1].bsx_fob_offset,
                          (end_fob_offset + 1)) - 
                      MAX( fob_offset, 
                           subXtntMap->bsXA[descStart].bsx_fob_offset);
        } else {
            fob_cnt = 0;
        }
    } else {

        fob_cnt = 0;
        if (fob_offset != subXtntMap->bsXA[descStart].bsx_fob_offset) {
            /*
             * The first fob is not aligned on an extent boundary.
             * Only count the fobs from the first fobs to the end
             * of the extent.
             */
            if ( subXtntMap->bsXA[descStart].bsx_vd_blk != XTNT_TERM &&
                 subXtntMap->bsXA[descStart].bsx_vd_blk != COWED_HOLE )
            {
                imm_split_desc( fob_offset,
                                &(subXtntMap->bsXA[descStart]),
                                &firstPart,
                                &lastPart);
                fob_cnt = fob_cnt + 
                          (subXtntMap->bsXA[descStart + 1].bsx_fob_offset- 
                           lastPart.bsx_fob_offset);
            }
            descStart++;
        }

        if (end_fob_offset != 
            (subXtntMap->bsXA[descEnd + 1].bsx_fob_offset - 1)) {
            /*
             * The last fob is not aligned on an extent boundary.
             * Only count the fobs from the start of the extent to
             * the last fob.
             */
            if ( subXtntMap->bsXA[descEnd].bsx_vd_blk != XTNT_TERM &&
                 subXtntMap->bsXA[descEnd].bsx_vd_blk != COWED_HOLE )
            {
                imm_split_desc( end_fob_offset + 1,
                                &(subXtntMap->bsXA[descEnd]),
                                &firstPart,
                                &lastPart);
                fob_cnt = fob_cnt + 
                          (lastPart.bsx_fob_offset - 
                           subXtntMap->bsXA[descEnd].bsx_fob_offset);
            }
            descEnd--;
        }

        for (i = descStart; i <= descEnd; i++) {
            if ( subXtntMap->bsXA[i].bsx_vd_blk != XTNT_TERM &&
                 subXtntMap->bsXA[i].bsx_vd_blk != COWED_HOLE )
            {
                fob_cnt = fob_cnt +
                  (subXtntMap->bsXA[i + 1].bsx_fob_offset -
                   subXtntMap->bsXA[i].bsx_fob_offset);
            }
        }  /* end for */
    }
    return fob_cnt;

}  /* end get_sub_xtnt_map_alloc_fob_cnt */

/*
 * imm_get_next_fob
 *
 * This function returns the next fob offset after the bitfile's last fob.
 */


bf_fob_t
imm_get_next_fob (
                  bsInMemXtntT *xtnts /* in */
                 )
{
    bf_fob_t next_fob;
    bsInMemSubXtntMapT *subXtntMap;
    bsInMemXtntMapT *xtntMap;


    subXtntMap = &xtnts->xtntMap->subXtntMap[xtnts->xtntMap->validCnt - 1];
    next_fob = subXtntMap->bssxmFobOffset + subXtntMap->bssxmFobCnt;

    return next_fob;

}  /* end imm_get_next_fob*/

/*
 * imm_get_first_hole
 *
 * This function finds the bitfile's first hole and returns the starting
 * fob offset and count.  If there is no hole, a fob offset and fob  
 * count of 0 are returned.
 */


void
imm_get_first_hole (
                    bsInMemXtntT *xtnts,  /* in */
                    bf_fob_t *ret_fob_offset,  /* out */
                    bf_fob_t *ret_fob_cnt /* out */
                    )
{
    bsXtntDescT xtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    bsInMemXtntMapT *xtntMap;


    xtntMap = xtnts->xtntMap;

    imm_get_first_xtnt_desc (xtntMap, &xtntDescId, &xtntDesc);
    while ( xtntDesc.bsxdVdBlk != XTNT_TERM &&
            xtntDesc.bsxdVdBlk != COWED_HOLE )
    {
       imm_get_next_xtnt_desc (xtntMap, &xtntDescId, &xtntDesc);
    }  /* end while */

    *ret_fob_offset = xtntDesc.bsxdFobOffset;
    *ret_fob_cnt = xtntDesc.bsxdFobCnt;

    return;

}  /* end imm_get_first_hole */


/*
 * imm_get_hole_size
 *
 * This function returns the size of the hole that starts at the specified
 * fob offset. The return value is in 1k file offset blks.
 */


bf_fob_t
imm_get_hole_size (
                   bf_fob_t fob_offset,  /* in */
                   bsInMemXtntT *xtnts  /* in */
                   )
{
    bf_fob_t fob_cnt;
    uint32_t descIndex;
    uint32_t i;
    statusT sts;
    uint32_t subMapIndex;
    bsInMemSubXtntMapT *subXtntMap;
    bsInMemXtntMapT *xtntMap;


    xtntMap = xtnts->xtntMap;
    sts = imm_fob_to_sub_xtnt_map (fob_offset, xtntMap, 0, &subMapIndex);
    if (sts == EOK) {
        subXtntMap = &(xtntMap->subXtntMap[subMapIndex]);
        sts = imm_fob_to_xtnt( fob_offset,
                                subXtntMap,
                                FALSE,   /* Don't use the update maps */
                                FALSE,   /* perm hole returns E_STG_HOLE */
                                &descIndex);
        if (sts == E_STG_HOLE) {
            /*
             * The fob offset is mapped by a sub extent map and the
             * extent descriptor is a hole descriptor.
             */
            fob_cnt = subXtntMap->bsXA[descIndex + 1].bsx_fob_offset- fob_offset;
        } else {
            if (sts == EOK) {
                /*
                 * The fob is mapped and allocated.
                 */
                fob_cnt = 0;
            } else {
                return 0;  /* FIX - masks errors.  is this ok?? */
            }
        }
    } else {
        if (sts == E_STG_HOLE) {
            /*
             * The fob offset is not mapped by any sub extent map and
             * is positioned between to sub extent maps.
             */
            fob_cnt = xtntMap->subXtntMap[subMapIndex + 1].bssxmFobOffset - fob_offset;
        } else {
            return 0;  /* FIX - masks errors.  is this ok?? */
        }
    }

    return fob_cnt;

}  /* end imm_get_hole_size */


/*
 * imm_fob_to_sub_xtnt_map
 *
 * This function searches an in-memory extent map for the sub extent map that
 * maps the specified fob and returns the sub extent map index.  If the fob 
 * in not mapped but its position is between two sub extent maps, it returns
 * the preceeding sub extent map's index.
 * If updateFlg is set this function only looks in the update subextents.
 */


statusT
imm_fob_to_sub_xtnt_map (
                          bf_fob_t fob_offset,  /* in */
                          bsInMemXtntMapT *xtntMap,  /* in */
                          int updateFlg,  /* in */
                          uint32_t *index  /* out */
                          )
{
    int lwr;
    int mid;
    int upr;
    int last;
    statusT sts;

    MS_SMP_ASSERT(xtntMap->validCnt <= xtntMap->cnt);
    MS_SMP_ASSERT(xtntMap->cnt <= xtntMap->maxCnt);
    if ( !updateFlg ) {
        /* look at the valid (ie non update) subextents */
        if (xtntMap->validCnt == 0) {
            return E_RANGE_NOT_MAPPED;
        }

        lwr = 0;
        upr = xtntMap->validCnt - 1;
        last = upr;
    } else {
        /* look at the update subextents */
        MS_SMP_ASSERT(xtntMap->updateStart <= xtntMap->updateEnd);
        MS_SMP_ASSERT(xtntMap->updateEnd < xtntMap->cnt);
        if (xtntMap->cnt == 0) {
            return E_RANGE_NOT_MAPPED;
        }

        lwr = xtntMap->updateStart;
        upr = xtntMap->updateEnd;
        last = upr;
    }

    if ( (fob_offset < xtntMap->subXtntMap[lwr].bssxmFobOffset) ||
         (fob_offset >=
          (xtntMap->subXtntMap[upr].bssxmFobOffset +
           xtntMap->subXtntMap[upr].bssxmFobCnt)) )
    {
        return E_RANGE_NOT_MAPPED;
    }

    mid = (lwr + upr) / 2;

    while (upr > lwr) {

        if (fob_offset < xtntMap->subXtntMap[mid].bssxmFobOffset) {

            upr = mid - 1;
            MS_SMP_ASSERT(xtntMap->subXtntMap[upr].bssxmFobOffset <=
                          xtntMap->subXtntMap[mid].bssxmFobOffset);

        } else {

            /* look mid and above */
            if ( mid + 1 > last ) {
                /* found it */
                break;
            }
                MS_SMP_ASSERT(xtntMap->subXtntMap[mid + 1].bssxmFobOffset >=
                              xtntMap->subXtntMap[mid].bssxmFobOffset);
            if ( fob_offset < xtntMap->subXtntMap[mid+1].bssxmFobOffset) {
                /* found it */
                break;
            } else {
                lwr = mid + 1;
            }
        }

        mid = (lwr + upr) / 2;
    }

    *index = mid;
    return EOK;

} /* end imm_fob_to_sub_xtnt_map */

/*
 * imm_fob_to_xtnt
 *
 * This function searches an in-memory sub extent map for the entry that
 * matches the specified fob and returns the index.
 * If updateFlg is set, only look from updateStart to updateEnd.
 * If the permHoleFlg flag is not set, then fobs that fall within a
 * permanent hole return E_STG_HOLE.
 * If the permHoleFlg flag is set, then fobs that fall within a
 * permanent hole return EOK, not E_STG_HOLE.
 */


statusT
imm_fob_to_xtnt (
                  bf_fob_t fob_offset,  /* in */
                  bsInMemSubXtntMapT *subXtntMap,  /* in */
                  int updateFlg,  /* in */
                  int permHoleFlg,  /* in */
                  uint32_t *index  /* out */
                  )
{
    uint32_t lwr;
    uint32_t mid;
    statusT  sts;
    uint32_t upr;

    if (subXtntMap->cnt == 0) {
        return E_RANGE_NOT_MAPPED;
    }

    if ( !updateFlg ) {
        lwr = 0;
        upr = subXtntMap->cnt - 1 - 1;  /* Exclude the termination descriptor */
                                        /* and convert the count to an index. */
    } else {
        MS_SMP_ASSERT(subXtntMap->updateStart <= subXtntMap->updateEnd);
        MS_SMP_ASSERT(subXtntMap->updateEnd < subXtntMap->cnt);
        lwr = subXtntMap->updateStart;
        upr = subXtntMap->updateEnd - 1;
    }

    if ( (fob_offset < subXtntMap->bsXA[lwr].bsx_fob_offset) ||
         (fob_offset >= subXtntMap->bsXA[upr + 1].bsx_fob_offset) )
    {
        return E_RANGE_NOT_MAPPED;
    }

    while (1) {

        mid = (lwr + upr) / 2;

        if (fob_offset < subXtntMap->bsXA[mid].bsx_fob_offset) {

            upr = mid - 1;
            MS_SMP_ASSERT(subXtntMap->bsXA[upr].bsx_fob_offset<
                          subXtntMap->bsXA[mid].bsx_fob_offset);

        } else {

            if (fob_offset < subXtntMap->bsXA[mid + 1].bsx_fob_offset) {

                if ( subXtntMap->bsXA[mid].bsx_vd_blk == XTNT_TERM ) {
                    sts = E_STG_HOLE;
                } else if ( subXtntMap->bsXA[mid].bsx_vd_blk == COWED_HOLE &&
                            !permHoleFlg ) {
                    sts = E_STG_HOLE;
                } else {
                    sts = EOK;
                }
                *index = mid;
                return sts;

            } else {

                lwr = mid + 1;
                MS_SMP_ASSERT(subXtntMap->bsXA[lwr].bsx_fob_offset>
                              subXtntMap->bsXA[mid].bsx_fob_offset);
            }
        }
    }

} /* end imm_fob_to_xtnt */

/******************************************************************************/
/* overlay_xtnt_map

** This function overlays "overXtntMap" onto the "baseXtntMap".  The results
** are described by "modXtntMap" which is base plus overlay and "replXtntMap"
** which is the portion of base displaced by overlay. The range described
** by the overlay extent map must be a subset of the range described by
** the base extent map.
** The subextents in the base that are not overlaped by overXtntMap are
** copied directly to newXtntMap including their reference to the
** corresponding mcell. Subextents that are fully overlapped by overXtntMap
** are copied to replXtntMap. These subextents also reference an existing
** mcell. Subextents in baseXtntMap that are partially overlapped by
** overXtntMap are split into two subextents. Each new subextent gets a new
** mcell. The old mcell is freed.

** Through out this routine I try to reuse as many mcells as possible.
** The only time I get new mcells is at the beginning and end of the overlay
** extent map. If the first fob of the overlay extent map does not fall
** on a subextent boundary in the base extent map then the original base mcell
** is discarded and 2 new mcells are obetained: one for the first half of the
** original mcell and one for the last half of the original mcell. The same is
** true for the last fob of the overlay extent map. Also the first mcell of
** the overlay extent map may have to be discarded and a new one obtained since
** it is a BSR_SHADOW_XTNTS type and if it is not the first mcell in the
** modified mcell chain it must be an BSR_XTRA_XTNTS type. At most I get 5 new
** mcells, independent of how large the file is, or how large the migrated
** portion is. Limiting the number of mcells obtained under the parent
** transaction to a fixed maximum (not proportional to file size) prevents the
** log from overflowing if the migrated file has many extents.

**  one "box" == subextent (in memory) = mcell (on the disk)
** Case I
**  -------------------------------------
**  |  A  |  B  |  C  |  D  |  E  |  F  |   "Base" extent map
**  -------------------------------------
**            |  G  |  H  | I |             "Over" extent map (new storage)
**            -----------------

**  -------------------------------------
**  |  A  | w |  G  |  H  | I | z |  F  |   "Mod" extent map w/ migrated stg
**  -------------------------------------
**            |x|  C  |  D  |y|             "Repl" extent map (to be freed)
**            -----------------

** step 1) copy first unchanged subextents from Base to Mod (A)
** step 2) copy unchanged part of next Base subextent to Mod subextent (B=>w)
**      2a) get mcell for new Mod subextent (w)
** step 3) copy changed part of Base subextent to Repl subextent (B=>x)
**      3a) get mcell for new Repl subextent (x)
**          put old Base mcell (B) on deferred delete list
** step 4) copy Over subextents to Mod (G,H,I)
** step 5) copy whole changed Base subextents to Repl (C,D)
** step 6) copy changed part of Base subextent to Repl subextent (E=>y)
**      6a) get mcell for new Repl subextent (y)
**          put old Base mcell (E) on deferred delete list
** step 7) copy unchanged part of Base subextent to Mod (E=>z)
**      7a) get mcell for new Mod subextent (z)
** step 8) copy last unchanged subextents from Base to Mod (F)

** Case II
**  -------------------------------------
**  |  A  |  B  |  C  |  D  |  E  |  F  |   Base
**  -------------------------------------
**              |  G    |   H     |         Over
**              -------------------

**  -------------------------------------
**  |  A  |  B  |  G    |   H     |  F  |   Mod
**  -------------------------------------
**              |  C  |  D  |  E  |         Repl
**              -------------------

** 1) copy Base to Mod (A,B)
** 4) copy Over to Mod (G,H)
** 5) copy Base to Repl (C,D,E)
** 8) copy Base to Mod (F)

** Case III
**  |  A  |               =>      |x|B|z|          steps 2,3,4,6,7
**    |B|                           |y|

** Case IV
**  |  A  |               =>      |B| y |          steps 4,6,7
**  |B|                           |x|

** Case V
**  |  A  |               =>      | x |B|          steps 2,3,4
**      |B|                           |y|

** Case VI
**  |  A  |  B  |         =>      | w | C | z |    steps 2,3,4,6,7
**      | C |                         |x|y|

** Case VII
**  |  A  |  B  |         =>      |   C   | y |    steps 4,5,6,7
**  |   C   |                     |  A  |x|

** Case VIII
**  |  A  |  B  |         =>      | x |   C   |    steps 2,3,4,5
**      |   C   |                     |y|  B  |
*/


statusT
overlay_xtnt_map ( bfAccessT *bfap,
                   bsInMemXtntMapT *baseXtntMap,  /* in */
                   bsInMemXtntMapT *overXtntMap,  /* in */
                   bsInMemXtntMapT **retModXtntMap,  /* out */
                   bsInMemXtntMapT **retReplXtntMap,  /* out */
                   ftxHT ftxH)
{
    statusT sts;
    bsInMemXtntMapT *modXtntMap = NULL;
    bsInMemXtntMapT *replXtntMap = NULL;
    bsXtntT part1BsXA;
    bsXtntT part2BsXA;
    int baseSXMapCnt = 0;
    int overSXMapCnt = 0;
    int modSXMapCnt = 0;
    int replSXMapCnt = 0;
    int descIndex;
    bsInMemSubXtntMapT *baseSubXtntMapp;
    bsInMemSubXtntMapT *modSubXtntMapp;
    bsInMemSubXtntMapT *replSubXtntMapp;
    bsInMemSubXtntMapT *overSubXtntMapp;

    bsInMemSubXtntMapT *tempSubXtntMapp;
    bsInMemSubXtntMapT tempSubXtntMap;
    bsXtntT tempXtnt[BMT_XTNTS];
    
    bf_fob_t first_overlay_fob;
    bf_fob_t last_overlay_fob;
    uint32_t moveCnt = 0;
    int i;
    bfMCIdT throwAwayMC;
    int gotMcell = 0;
    bfMCIdT savMC = bsNilMCId;
    bfMCIdT savBaseMC = bsNilMCId;
    int savBaseCnt;
    int mcellCnt = 0;
    int switchedTypes = FALSE;
    int allocated_mcell_in_step_2 = FALSE;
    uint32_t prevXtnt = 0;
    bfMCIdT newMcellId;


    /* Create a new extent map for the "replace" extents. */
    /* The most sub extents we ever need is all of the base, baseXtntMap->cnt.*/
    sts = imm_create_xtnt_map ( 
                                baseXtntMap->domain,
                                baseXtntMap->cnt, /* pre allocate subextents */
                                &replXtntMap
                              );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Create a new extent map for the "modified" extents. */
    /* The most subextents we need is the number in the base - 1 (the overlay */
    /* map overlaps at least one of the base subxtnts) + 2 (two new subxtnts */
    /* at the start and end of the overlay) + the number in the overlay. */
    sts = imm_create_xtnt_map ( 
                                baseXtntMap->domain,
                                baseXtntMap->cnt + overXtntMap->cnt + 1,
                                &modXtntMap
                              );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* 
     * If this file has extent information in the primary mcell, then
     * that is where the mcell count for the extent chain resides.
     * Save it for later comparison with the modXtntMap->cnt.
     */
    mcellCnt = baseXtntMap->cnt;

    first_overlay_fob = overXtntMap->subXtntMap[0].bssxmFobOffset;
    overSubXtntMapp = &overXtntMap->subXtntMap[overXtntMap->cnt - 1];
    last_overlay_fob = overSubXtntMapp->bsXA[overSubXtntMapp->cnt-1].bsx_fob_offset- 1;

    MS_SMP_ASSERT((last_overlay_fob - first_overlay_fob ) >= 0);
    MS_SMP_ASSERT(first_overlay_fob >= 
                  baseXtntMap->subXtntMap[0].bssxmFobOffset);
    MS_SMP_ASSERT(last_overlay_fob <=
       baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].bssxmFobOffset +
       baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].bssxmFobCnt - 1);

/*
** step 1) copy first unchanged sub extent maps from baseXtntMap to modXtntMap
*/

    MS_SMP_ASSERT(baseXtntMap->cnt > 0);
    for ( modSXMapCnt = 0, baseSXMapCnt = 0;
          baseSXMapCnt < baseXtntMap->cnt - 1;
          baseSXMapCnt++, modSXMapCnt++ ) {

        baseSubXtntMapp = &baseXtntMap->subXtntMap[baseSXMapCnt];

        if (first_overlay_fob <
            baseSubXtntMapp->bssxmFobOffset + baseSubXtntMapp->bssxmFobCnt) {
            break;
        }
        /* Currently, the first base mcell says we have baseXtntMap->cnt */
        /* mcells. We may need to change this when we are done. */
        /* Save this here to avoid reading the first base mcell at the end */
        /* of this routine.  */
        mcellCnt = baseXtntMap->cnt;

        MS_SMP_ASSERT(modXtntMap->maxCnt > modSXMapCnt);
        modSubXtntMapp = &modXtntMap->subXtntMap[modSXMapCnt];
        sts = imm_copy_sub_xtnt_map ( bfap,
                                      baseXtntMap,
                                      baseSubXtntMapp,
                                      modSubXtntMapp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        modXtntMap->cnt = modXtntMap->validCnt = modSXMapCnt + 1;
    }

    baseSubXtntMapp = &baseXtntMap->subXtntMap[baseSXMapCnt];
    /* check if overlay lines up on subxtnt boundary. if so skip step 2 & 3 */
    MS_SMP_ASSERT(first_overlay_fob >= baseSubXtntMapp->bssxmFobOffset);
    if ((baseSubXtntMapp->bssxmFobCnt > 0) &&
        (first_overlay_fob != baseSubXtntMapp->bssxmFobOffset)) {

/*
** step 2) copy part of a subextent from baseXtntMap to modXtntMap
*/

        MS_SMP_ASSERT(modXtntMap->maxCnt > modSXMapCnt);

        modSubXtntMapp = &modXtntMap->subXtntMap[modSXMapCnt];
        sts = imm_copy_sub_xtnt_map ( bfap,
                                      baseXtntMap,
                                      baseSubXtntMapp,
                                      modSubXtntMapp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        modXtntMap->cnt = modXtntMap->validCnt = modSXMapCnt + 1;

        part2BsXA.bsx_fob_offset= 0;         /* flag for step 3 */

        /* Find the boundary within a subextent in baseXtntMap of the first */
        /* fob in the overlay. Copy the first part to modXtntMap. If the */
        /* boundary does not lie on an extent boundary, break the extent in */
        /* two. Save descIndex and the second half of the extent for step 3. */
        MS_SMP_ASSERT(modSubXtntMapp->cnt > 1);
        for ( descIndex = 0;
              descIndex < modSubXtntMapp->cnt - 1;
              descIndex++ ) {

            if ( first_overlay_fob >=
                 modSubXtntMapp->bsXA[descIndex + 1].bsx_fob_offset) {
                continue;
            }

            /* if the overlay falls on an extent boundary */
            if ( first_overlay_fob == 
                    modSubXtntMapp->bsXA[descIndex].bsx_fob_offset) {
                modSubXtntMapp->bsXA[descIndex].bsx_vd_blk = XTNT_TERM;
                modSubXtntMapp->cnt = descIndex + 1;
                modSubXtntMapp->updateStart = descIndex + 1;
                break;
            }

            /* the overlay does not fall on an extent boundary */
            imm_split_desc (first_overlay_fob,
                            &modSubXtntMapp->bsXA[descIndex],
                            &part1BsXA,
                            &part2BsXA);
            
            modSubXtntMapp->bsXA[descIndex + 1].bsx_fob_offset = 
                part2BsXA.bsx_fob_offset;
            modSubXtntMapp->bsXA[descIndex + 1].bsx_vd_blk = XTNT_TERM;
            modSubXtntMapp->cnt = descIndex + 2;
            modSubXtntMapp->updateStart = descIndex + 2;
            break;
        }
        modSubXtntMapp->bssxmFobCnt =
          modSubXtntMapp->bsXA[modSubXtntMapp->cnt - 1].bsx_fob_offset -
                                   modSubXtntMapp->bssxmFobOffset;

/*
** step 2a) get new mcell for modXtntMap
** Skip this step if the current base subextent is in the primary mcell.
** The primary mcell will continue to hold the first part of the base
** extent that is being retained in the modXtntMap.
*/

        if (baseXtntMap->subXtntMap[baseSXMapCnt].type != BSR_XTNTS) {
            sts = stg_alloc_new_mcell (bfap,
                                       bfap->bfSetp->dirTag,
                                       bfap->tag,
                                       (vdIndexT) modSubXtntMapp->mcellId.volume,
                                       ftxH,
                                       &newMcellId,
                                       FALSE);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            MS_SMP_ASSERT(modSubXtntMapp->mcellId.volume == newMcellId.volume); 
            modSubXtntMapp->mcellId = newMcellId;
            allocated_mcell_in_step_2 = TRUE;

            /* 
             * This will be the same type (BSR_XTRA_XTNTS)
             * as the subextent copied in step 2 above. It will have the same 
             * position in the mod chain as it had in the base so its type is OK
             */
            MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
            sts = odm_create_xtnt_rec ( bfap,
                                        bfap->bfSetp,
                                        modSubXtntMapp,
                                        ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            if ( mcellCnt == 0 ) {
                /* 
                 * Currently the first mcell (this new one) says we have 1 mcell
                 * in the chain. At the end of this routine we will see if this 
                 * count is OK or if we have to modify the count in the mcell.
                 */
                mcellCnt = 1;
            }

            /* Disconnect the mcell that is being split (and all that follow) */
            /* from the mcells processed in step 1. */
            if ( baseSXMapCnt > 0 ) {
                /* Step 1 copied some mcells. Case I & II */
                sts = bmt_unlink_mcells ( bfap->dmnP,
                          bfap->tag,
                          baseXtntMap->subXtntMap[baseSXMapCnt - 1].mcellId,
                          baseXtntMap->subXtntMap[baseSXMapCnt].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH,
			  CHAINMCID);
            } else {
                /* No mcells were copied in step 1. Case III - VIII */
                sts = x_detach_extent_chain ( bfap,
                                              baseXtntMap,
                                              ftxH,
                                              &throwAwayMC,
                                              &throwAwayMC);
            }
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            /* Attach the new mcell to mcells processed in step 1. */
            if ( modSXMapCnt > 0 ) {
                /* Step 1 copied some mcells. Case I & II */
                sts = bmt_link_mcells ( bfap->dmnP,
                                  bfap->tag,
                                  modXtntMap->subXtntMap[modSXMapCnt-1].mcellId,
                                  modSubXtntMapp->mcellId,
                                  modSubXtntMapp->mcellId,
                                  ftxH);
            } else {
                sts = bmt_link_mcells( bfap->dmnP,
                                       bfap->tag,
                                       bfap->primMCId,
                                       modSubXtntMapp->mcellId,
                                       modSubXtntMapp->mcellId,
                                       ftxH);
            }
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

        } /* If allocating an mcell for the modXtntMap */

        modSXMapCnt++;

/*
** step 3) copy part of subextent from baseXtntMap to replXtntMap
*/

        baseSubXtntMapp = &baseXtntMap->subXtntMap[baseSXMapCnt];
        replSubXtntMapp = &replXtntMap->subXtntMap[0];

        /*
         * We want only XTRA extent records in the deferred delete list code.
         */
        if (baseSubXtntMapp->type == BSR_XTNTS) {
            /* Make temporary copy of the extent map since we can
             * not change the inmemory one without holding locks and
             * we can not call imm_copy_sub_xtnt_map which may allocate
             * memory or read from disk (which probably can't happen
             * but ...) while holding the xtntMap_lk for write.
             * NOTE that we only need a small temp map(BSR_XTNTS).
             */
             
            tempSubXtntMap = *baseSubXtntMapp;
            for (i=0;i<BMT_XTNTS;i++){
                tempXtnt[i] = baseSubXtntMapp->bsXA[i];
            }
            tempSubXtntMap.bsXA = &(tempXtnt[0]);

            tempSubXtntMap.type = BSR_XTRA_XTNTS;
            tempSubXtntMap.onDiskMaxCnt = BMT_XTRA_XTNTS;
            tempSubXtntMapp = baseSubXtntMapp;
            baseSubXtntMapp = &tempSubXtntMap;
            switchedTypes = TRUE;
        }

        sts = imm_copy_sub_xtnt_map ( bfap,
                                      baseXtntMap,
                                      baseSubXtntMapp,
                                      replSubXtntMapp);

        if (switchedTypes) {
            baseSubXtntMapp = tempSubXtntMapp;
            switchedTypes = FALSE;
        }

        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /* Continue processing the same baseXtntMap subextent step 2 worked */
        /* on. Use descIndex and part2BsXA from step 2 to know where to start.*/
        replXtntMap->cnt = replXtntMap->validCnt = 1;
        if ( part2BsXA.bsx_fob_offset != 0 ) {     /* flag from step 2 */
            replSubXtntMapp->bsXA[descIndex] = part2BsXA;
        }
        moveCnt = baseSubXtntMapp->cnt - descIndex;
        if ( descIndex != 0 ) {
            for (i = 0; i < moveCnt; i++) {
                replSubXtntMapp->bsXA[i] = replSubXtntMapp->bsXA[descIndex + i];
            }
        }
        replSubXtntMapp->cnt = moveCnt;
        replSubXtntMapp->updateStart = moveCnt;
        replSubXtntMapp->bssxmFobOffset = 
            replSubXtntMapp->bsXA[0].bsx_fob_offset;
        replSubXtntMapp->bssxmFobCnt = 
            replSubXtntMapp->bsXA[moveCnt - 1].bsx_fob_offset -
            replSubXtntMapp->bssxmFobOffset;

        /* If the overlay covers more than one base extent then */
        /* we are done with this base extent and this replace extent */
        /* otherwise we are still processing this base extent and we need */
        /* to remember at what descriptor we left off for step 7. */
        if ( last_overlay_fob + 1 >=
             replSubXtntMapp->bssxmFobOffset + 
             replSubXtntMapp->bssxmFobCnt ) {

            /* Case I, II, VI - VIII */
            savBaseMC = baseSubXtntMapp->mcellId;    /* for step 3a */
            replSXMapCnt = 1;
            baseSXMapCnt++;
            moveCnt = 0;             /* step 7 will start on a new subextent */
        } else {
            /* Case III - V */
            /* Note that by not incrementing baseSXMapCnt and not setting */
            /* replSXMapCnt we will still be working on the same "base" */
            /* subextent and "replace" subextent in step 6 if no subextents */
            /* are processed in step 5. */
            moveCnt = descIndex;     /* save this descriptor index for step 7.*/
        }

/*
** step 3a) get new mcell for replXtntMap
*/

        /* If we're finished processing this subextent we need to get a new */
        /* mcell and free the old one otherwise we'll do all that in step 6a. */
        if ( replSXMapCnt == 1 ) {
            /* Case I, II, VI - VIII */
            sts = stg_alloc_new_mcell (bfap,
                                       bfap->bfSetp->dirTag,
                                       bfap->tag,
                                       (vdIndexT) replSubXtntMapp->mcellId.volume,
                                       ftxH,
                                       &newMcellId,
                                       FALSE);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            MS_SMP_ASSERT(replSubXtntMapp->mcellId.volume == newMcellId.volume);
            replSubXtntMapp->mcellId = newMcellId;

            MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
            sts = odm_create_xtnt_rec ( bfap,
                                        bfap->bfSetp,
                                        replSubXtntMapp,
                                        ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            /*
             * If the base mcell we just processed is the primary mcell, 
             * we want to update it on disk.  Otherwise, we've allocated 
             * a new mcell to take its place so we can just free it.
             */
            if (baseXtntMap->subXtntMap[baseSXMapCnt-1].type == BSR_XTNTS) {
                MS_SMP_ASSERT( MCID_EQL(baseSubXtntMapp->mcellId,
                                        modSubXtntMapp->mcellId));
                modSubXtntMapp->updateStart = 0;
                modSubXtntMapp->updateEnd = modSubXtntMapp->cnt-1;
                sts = update_xtnt_rec (bfap->dmnP,
                                       bfap->tag,
                                       modSubXtntMapp,
                                       ftxH);
            }
            else {
                sts = deferred_free_mcell( VD_HTOP(savBaseMC.volume, bfap->dmnP),
                                           savBaseMC,
                                           ftxH);
            }
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            gotMcell = 1;              /* for step 5 */
        }
    }   /* end of "if start of overlay divides a base mcell" */

/*
** step 4) copy subextents from overXtntMap to modXtntMap
*/

    if ( modSXMapCnt > 0 ) {
        /* Case I - III, V, VI, VIII */
        /* There already is some mcells in the modified extent chain from */
        /* step 1 and/or 2. Save the end of the modified extent chain to link */
        /* to the mcells moved in step 4. */
        savMC = modXtntMap->subXtntMap[modSXMapCnt - 1].mcellId;

        /* 
         * If the first mcell in the overlay chain is a BSR_SHADOW_XTNTS, yet
         * this mcell is not going to be the first in the modified chain, then
         * we must replace this mcell with a BSR_XTRA_XTNTS mcell. 
         */
        MS_SMP_ASSERT(overXtntMap->cnt);
    } /* if ( modSXMapCnt > 0 ) */

    if ( mcellCnt == 0 ) {
        /* Currently the first mcell (the first one in the overlay chain) */
        /* says we have overXtntMap->cnt mcells. At the end of this routine */
        /* we will see if we have to modify the count in the mcell. */
        mcellCnt = overXtntMap->cnt;
    }

    /*
     * It is possible that modSXMapCnt is zero.  If that is true, we
     * want prevXtnt to be zero.
     */
    if (modSXMapCnt != 0) {
        prevXtnt = modXtntMap->subXtntMap[modSXMapCnt-1].cnt - 1;
    } else {
        prevXtnt = 0;
    }

    /* 
     * Base primary mcell contains no extent.  Put one in there!
     *
     * Special case processing for the following situation:
     *   1. We are migrating a file which is capable of storing extent 
     *      information in the primary mcell.
     *   2. The file is currently occupying two mcells on one or more 
     *      volumes and we are migrating some or all of the file 
     *      to the volume where its primary mcell is.
     * In this case, we want to end up with the primary mcell on the destination
     * volume containing an extent.
     */

    if( (overXtntMap->subXtntMap[0].mcellId.volume == 
         baseXtntMap->subXtntMap[0].mcellId.volume) &&

        /* must have at least 2 mcells */
        (overXtntMap->validCnt == 1) &&
        (baseXtntMap->validCnt > 1) &&

        /* Handles base maps with more than just 2 mcells.
         * Also now handles base map with a different fob count 
         * than the overlay fob count.
         */

        /* offset in overlay matches the first extent offset in base */
        (overXtntMap->subXtntMap[0].bssxmFobOffset ==  
         baseXtntMap->subXtntMap[1].bssxmFobOffset) && 

        /* overlay contains only one extent, base only contains TERM */
        (overXtntMap->subXtntMap[0].cnt == 2) &&
        (baseXtntMap->subXtntMap[0].cnt == 1)) {

        MS_SMP_ASSERT(baseXtntMap->subXtntMap[0].type == BSR_XTNTS);

        modSubXtntMapp = &modXtntMap->subXtntMap[0];
        overSubXtntMapp = &overXtntMap->subXtntMap[0];
        baseSubXtntMapp = &baseXtntMap->subXtntMap[0];
        replSubXtntMapp = &replXtntMap->subXtntMap[0];

        /* 
         * Detach the old BSR_XTRA_XTNTS mcell from the primary mcell.
         */
        sts = x_detach_extent_chain ( bfap,
                                      baseXtntMap,
                                      ftxH,
                                      &throwAwayMC,
                                      &throwAwayMC);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /* 
         * Copy the (only) overlay subextent into the modXtntMap at
         * slot 0.  Then set the type and mcell for modXtntMap slot 0 
         * to be the primary mcell.
         */
        sts = imm_copy_sub_xtnt_map( bfap,
                                     overXtntMap,
                                     overSubXtntMapp,
                                     modSubXtntMapp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        modSubXtntMapp->type = BSR_XTNTS;
        modSubXtntMapp->maxCnt = BMT_XTNTS;
        modSubXtntMapp->mcellId = baseSubXtntMapp->mcellId;
        modSubXtntMapp->onDiskMaxCnt = baseSubXtntMapp->onDiskMaxCnt;

        /*
         * Update the BSR_XTNTS record's extent array for the primary mcell 
         * on disk.
         */
        modSubXtntMapp->updateStart = 0;
        modSubXtntMapp->updateEnd = 1;
        sts = update_xtnt_rec (bfap->dmnP,
                               bfap->tag,
                               modSubXtntMapp,
                               ftxH);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * Update the in-memory extent map.  The on-disk extent map 
         * will be updated by the call to update_mcell_cnt() at the end 
         * of this function.
         */
        modXtntMap->cnt = 1;

        /*
         * Bump base sub extent map counter so step 5 will not copy the
         * new BSR_XTNTS record to the replace extent map.
         */
        baseSXMapCnt = 1;
        modSXMapCnt = 1;

        /*
         * Now that we've copied the storage described by the extent
         * descriptors in the overlay mcell into the primary mcell,
         * we need to make sure that the overlay mcell itself gets
         * deallocated since we're not going to use it.  This involves
         * three steps:
         *   1. Clear out the in-memory extent descriptors in the overlay
         *      extent map.
         *   2. Update the on-disk overlay mcell to clear out the
         *      on-disk copy of the extent map.
         *   3. Copy the overlay subextent to the replace extent map
         *      for later deallocation of the mcell but not of any
         *      extents.
         */
        /* Use a temp submap. We can NOT modify the copyXtntMap in
         * place in the bfap since it is visible to putpage
         */
        MS_SMP_ASSERT(replXtntMap->maxCnt > replSXMapCnt);
        tempSubXtntMap = *overSubXtntMapp;
        tempSubXtntMapp = &tempSubXtntMap;
        tempSubXtntMapp->cnt = 1;
        tempSubXtntMapp->bsXA = &(tempXtnt[0]);
        tempSubXtntMapp->bsXA[0].bsx_fob_offset= 0;
        tempSubXtntMapp->bsXA[0].bsx_vd_blk = XTNT_TERM;

        tempSubXtntMapp->updateStart = 0;
        tempSubXtntMapp->updateEnd = 1;
        sts = update_xtnt_rec (bfap->dmnP,
                               bfap->tag,
                               tempSubXtntMapp,
                               ftxH);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        sts = imm_copy_sub_xtnt_map( bfap,
                                     overXtntMap,
                                     tempSubXtntMapp,
                                     replSubXtntMapp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        
        overSXMapCnt++;
        replSXMapCnt++;

        /* 
         * Set gotMcell so that the first mcell in the replace extent map
         * (from the overlay) and the second mcell in the replace extent
         * map (from the base; second volume) will be linked together.
         */
        gotMcell = TRUE;
    } else
    /* Handle the special cases where there is an extent in the primary
     * mcell of the base already AND:
     *  1. overlay extent is physically contiguous on disk to the
     *     only extent in the primary mcell of the base map.
     *    - make a single extent in the primary mcell describing both extents.
     * -OR-
     *  2. overlay extent's offset is equal to the offset of the 
     *     base's primary mcell offset.
     *    - make the overlay extent reside in the primary mcell.
     */
    if( (overXtntMap->subXtntMap[0].mcellId.volume == 
         baseXtntMap->subXtntMap[0].mcellId.volume) &&

        /* base must have at least 1 mcell */
        (overXtntMap->validCnt == 1) &&
        (baseXtntMap->validCnt > 0) &&

        /* overlay contains one extent, base prim mcell one extent */
        (overXtntMap->subXtntMap[0].cnt == 2) &&
        (baseXtntMap->subXtntMap[0].cnt == 2) &&

        (
        /* overlay will replace prim mcell */
        (overXtntMap->subXtntMap[0].bssxmFobOffset ==
         baseXtntMap->subXtntMap[0].bssxmFobOffset) ||

        /* -or- */
        /* overlay is contiguous to base prim */
        ((baseXtntMap->subXtntMap[0].bssxmFobOffset +
          baseXtntMap->subXtntMap[0].bssxmFobCnt) ==  
          /* base end fob == over start offset */
          overXtntMap->subXtntMap[0].bssxmFobOffset) &&
          /* blocks adjacent */
        ((baseXtntMap->subXtntMap[0].bsXA[0].bsx_vd_blk +
          (baseXtntMap->subXtntMap[0].bsXA[1].bsx_fob_offset) / 
                ADVFS_FOBS_PER_DEV_BSIZE) ==
           overXtntMap->subXtntMap[0].bsXA[0].bsx_vd_blk)
        )
      ) {

        MS_SMP_ASSERT(baseXtntMap->subXtntMap[0].type == BSR_XTNTS);

        overSubXtntMapp = &overXtntMap->subXtntMap[0];
        baseSubXtntMapp = &baseXtntMap->subXtntMap[0];
        modSubXtntMapp = &modXtntMap->subXtntMap[0];
        replSubXtntMapp = &replXtntMap->subXtntMap[0];

        /* 
         * Detach the old BSR_XTRA_XTNTS mcell from the primary mcell 
         * of the base map.
         */
        if(baseXtntMap->validCnt > 1) {
            /* detach base[1] from base primary[0] */
            sts = x_detach_extent_chain ( bfap,
                                      baseXtntMap,
                                      ftxH,
                                      &throwAwayMC,
                                      &throwAwayMC);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }

        /*
         * Copy the (only) overlay subextent into a new mcell at
         * modXtntMap slot 0.
         */
        sts = imm_copy_sub_xtnt_map( bfap,
                                     overXtntMap,
                                     overSubXtntMapp,
                                     modSubXtntMapp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        /* Change the mcell Id on the mod to use the base's mcell Id
         * because we would have to change the tag dir file if we were to
         * start assigning primary mcell ids to files!!
         */
        modSubXtntMapp->type = BSR_XTNTS;
        modSubXtntMapp->maxCnt = BMT_XTNTS;
        /* Set the type and mcell for modXtntMap slot 0 
         * to be the primary mcell.  In other words, set the 
         * mod mcell = base mcell so we reuse the prim mcell on disk.
         */
        modSubXtntMapp->mcellId = baseSubXtntMapp->mcellId;
        modSubXtntMapp->onDiskMaxCnt = baseSubXtntMapp->onDiskMaxCnt;

        if(overXtntMap->subXtntMap[0].bssxmFobOffset ==   
           baseXtntMap->subXtntMap[0].bssxmFobOffset)  {
            /* Overlay extent is NOT contiguous to first extent in base.
             * Overlay extent will replace prim mcell extent at least partially
             *
             *    imm_copy_sub_xtnt_map above updated the new mod mcell 
             *    (which got the id from the old base prim mcell) with the 
             *    overlay's extent.
             *    This results in old base primary mcell with new extent 
             *    from overlay.
             */

            /* set the baseSXMapCnt to the primary mcell so step 5 will 
             * delete the overwritten extent out of the primary mcell.  
             * step 5 correctly deals with an extent in the primary mcell.
             */
            baseSXMapCnt = 0;

        } else {
            /* Overlay extent is contiguous with the extent in the PM base. 
             * Combine the extents from the base prim mcell and the overlay
             * mcell and place the single new extent into the new mod.
             */
            modSubXtntMapp->bsXA[0].bsx_fob_offset 
                = baseSubXtntMapp->bsXA[0].bsx_fob_offset;
            modSubXtntMapp->bsXA[0].bsx_vd_blk  
                = baseSubXtntMapp->bsXA[0].bsx_vd_blk;
            modSubXtntMapp->bsXA[1].bsx_fob_offset 
                = overSubXtntMapp->bsXA[1].bsx_fob_offset;
            modSubXtntMapp->bssxmFobCnt        
                = baseSubXtntMapp->bssxmFobCnt +
                overSubXtntMapp->bssxmFobCnt;
            modSubXtntMapp->bssxmFobOffset     
                = baseSubXtntMapp->bssxmFobOffset;
            /* set the base cnt to the next mcell so step 5 does not try 
             * delete the extent out of the old base primary mcell since
             * we just appended it here.
             */
            baseSXMapCnt = 1;

        } 

        /*
         * Update the BSR_XTNTS record's extent for the primary mcell 
         * on disk (this is the old base primary mcell).
         */
        modSubXtntMapp->updateStart = 0;
        modSubXtntMapp->updateEnd = 1;
        sts = update_xtnt_rec (bfap->dmnP,
                               bfap->tag,
                               modSubXtntMapp,
                               ftxH);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * Update the in-memory extent map.  
         * The on-disk extent map will be updated by the
         * call to update_mcell_cnt() at the end of this function.
         */
        modXtntMap->cnt = 1;

        modSXMapCnt = 1;

        /*
         * Now that we've copied the storage described by the extent
         * descriptors in the overlay mcell into the primary mcell,
         * we need to make sure that the overlay mcell itself gets
         * deallocated since we're not going to use it.
         */
        MS_SMP_ASSERT(replXtntMap->maxCnt > replSXMapCnt);

        sts = deferred_free_mcell ( 
                               VD_HTOP(overSubXtntMapp->mcellId.volume, bfap->dmnP),
                               overSubXtntMapp->mcellId,
                               ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        overSXMapCnt++;

    } else 
    /* 
     * Special append case processing for the following situation:
     *   1. This new overlay extent is contiguous to the last extent in the
     *      previous mod mcell. Update the last mod mcell with the overlay
     *      extent and free the overlay mcell.  
     *  NOTE: This is for non-primary mcells only (modSXMapCnt >= 2) and
     *        all v3 mcells.
     */
    if((((modSXMapCnt >= 2) && /* v4 & non-primary mcells only */
        /* current mod must be in BSR_XTRA_XTNTS mcells */
        (modXtntMap->validCnt >= 2))) &&

        (prevXtnt > 0) &&     /* another non-primary mcells only check */
        (overSXMapCnt == 0) && /* first run through only */
        (overXtntMap->validCnt == 1) && /* overlay must have 1 mcell only */

        (overXtntMap->subXtntMap[0].mcellId.volume ==  /* must be on same vd */
         modXtntMap->subXtntMap[modSXMapCnt-1].mcellId.volume) &&
                  
        /* overlay contains one extent only */
        (overXtntMap->subXtntMap[0].cnt == 2) && 
        /* mod contains at least one extent */
        (modXtntMap->subXtntMap[modSXMapCnt-1].cnt > 1) &&

        /* Overlay extent is only appended to the end of a wholly copied base 
         * mcell's contents, NOT inserted into middle resulting in a split 
         * mcell.
         * To do the middle we would need to change this routine to splice
         * the new overlay extent into the mod mcell, possibly overlying and 
         * cutting into subsequent extents in this mcell, or even possibly 
         * spilling over into a next mcell.
         * Check here is if base was not fully copied in step 1, above,
         * then the baseSXMapCnt will still be pointing to the previous mcell
         * with its fob offset, and the base offset and mod offset will not 
         * match. bssxmFobCnt comparison is also necessary in case the previous
         * mcell is the primary mcell and its offset is 0 just as this mcells.
         */
        (baseXtntMap->subXtntMap[baseSXMapCnt-1].bssxmFobOffset ==
         modXtntMap->subXtntMap[modSXMapCnt-1].bssxmFobOffset) &&
        (baseXtntMap->subXtntMap[baseSXMapCnt-1].bssxmFobCnt ==
         modXtntMap->subXtntMap[modSXMapCnt-1].bssxmFobCnt) &&

        /* overlay fob is contiguous to last mod */
        (modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt].bsx_fob_offset==
                overXtntMap->subXtntMap[0].bsXA[0].bsx_fob_offset) && 

        /* overlay blocks contiguous to last mod blocks */
        ((modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt-1].bsx_vd_blk +   
         ((modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt].bsx_fob_offset -
           modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt-1].bsx_fob_offset) / 
             ADVFS_FOBS_PER_DEV_BSIZE)) ==
           overXtntMap->subXtntMap[0].bsXA[0].bsx_vd_blk)
    ) {

        /* overlay extent is contiguous with the extent in the base */
        /* update the mod mcell header with the new start and end fobs */
        modSubXtntMapp->bsXA[prevXtnt].bsx_fob_offset
            = overSubXtntMapp->bsXA[1].bsx_fob_offset;
        modSubXtntMapp->bssxmFobCnt        
            = modSubXtntMapp->bssxmFobCnt +
              overSubXtntMapp->bssxmFobCnt;
        /*
         * Update the BSR_XTRA_XTNTS record's extent array for the mcell 
         * on disk.
         */
        modSubXtntMapp->updateStart = 0;
        modSubXtntMapp->updateEnd = modSubXtntMapp->cnt-1;
        sts = update_xtnt_rec (bfap->dmnP,
                               bfap->tag,
                               modSubXtntMapp,
                               ftxH);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * Update the in-memory extent map.  
         * The on-disk extent map will be updated by the last update_xtnt_rec.
         */
        modXtntMap->cnt = modXtntMap->validCnt = modSXMapCnt;

        /* unlink any mcells that follow the updated mcell */
        sts = bmt_unlink_mcells (
                       bfap->dmnP,
                       bfap->tag,
                       baseXtntMap->subXtntMap[baseSXMapCnt - 1].mcellId,
                       baseXtntMap->subXtntMap[baseSXMapCnt].mcellId,
                       baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                       ftxH,
		       CHAINMCID);

        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * Now that we've appended the storage described by the extent
         * descriptors in the overlay mcell into the previous non-primary
         * mcell, we need to make sure that the overlay mcell itself gets
         * deallocated since we're not going to use it.
         */

        sts = deferred_free_mcell ( 
                               VD_HTOP(overSubXtntMapp->mcellId.volume, bfap->dmnP),
                               overSubXtntMapp->mcellId,
                               ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        overSXMapCnt++;

    } else {
        /* 
         * Normal processing case for copying overlay extents to 
         * the modXtntMap.
         */

        /*
         * If this file supports the storage of extent information in the
         * primary mcell and the modXtntMap currently has no subextents
         * and the first subextent in the overXtntMap is not of type
         * BSR_XTNTS, as would be the case if a single-mcell file were
         * being migrated, then we need to copy the BSR_XTNTS subextent
         * from the primary mcell into the modXtntMap for consistency before
         * we start copying in the overlay subextents.  The only difference
         * is that we will make the modXtntMap subextent look like there is
         * no extent information in that subextent.  That, in fact, is what
         * will happen when x_detach_xtnt_chain() is called below.
         */
        if ( (modSXMapCnt == 0) &&
            (overXtntMap->subXtntMap[overSXMapCnt].type != BSR_XTNTS)) {

            modSubXtntMapp = &modXtntMap->subXtntMap[modSXMapCnt++];

            sts = imm_copy_sub_xtnt_map( bfap,
                                         baseXtntMap,
                                         &baseXtntMap->subXtntMap[0],
                                         modSubXtntMapp);
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }
            modSubXtntMapp->bssxmFobOffset = 0;
            modSubXtntMapp->bssxmFobCnt = 0;
            modSubXtntMapp->cnt = 1;
            modSubXtntMapp->bsXA[0].bsx_fob_offset= 0;
            modSubXtntMapp->bsXA[0].bsx_vd_blk = XTNT_TERM;
        }

        for ( overSXMapCnt = 0;
              overSXMapCnt < overXtntMap->cnt;
              overSXMapCnt++, modSXMapCnt++ ) {

            MS_SMP_ASSERT(modXtntMap->maxCnt > modSXMapCnt);

            sts = imm_copy_sub_xtnt_map( bfap,
                                         overXtntMap,
                                         &overXtntMap->subXtntMap[overSXMapCnt],
                                         &modXtntMap->subXtntMap[modSXMapCnt]);
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }
        }

        modXtntMap->cnt = modXtntMap->validCnt = modSXMapCnt;

        /* 
         * If modXtntMap has subextents (and mcells) from steps 1 and 2 then 
         * link these new step 4 mcells to those step 1 & 2 mcells. 
         * If we processed no subextents in step 1 or 2 then overXtntMap covers
         * the first baseXtntMap extent and we must disconnect the extent mcells
         * from the primary mcell before linking these new mcells to the 
         * primary mcell. 
         */
        MS_SMP_ASSERT(overXtntMap->cnt);
        if ( savMC.volume ) {
            /* Case I -III, V, VI, VIII */

            /*
             * If the mcells copied to the modXtntMap in step 1 point to 
             * other mcells, we must detach those other mcells before linking 
             * in these new mcells.  Note that if we went through step 2,
             * then the last mcell currently in the modXtntMap will be
             * a freshly-allocated mcell which does not point to any others.
             */
            if (!allocated_mcell_in_step_2) {
                /*
                 * So, there are extents in the modXtntMap from step 1.
                 * These must be from the baseXtntMap prior to its overlap
                 * with the overlayXtntMap.  If the last of these mcells
                 * points to other extent mcells, we need to break that
                 * chain before attaching to it the new step 4 mcells.
                 */
                if (baseSXMapCnt <= baseXtntMap->cnt - 1) {
                    if (baseSXMapCnt > 0) {
                        sts = bmt_unlink_mcells (bfap->dmnP,
                          bfap->tag,
                          savMC,
                          baseXtntMap->subXtntMap[baseSXMapCnt].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH,
			  CHAINMCID);
                    } else if ( baseXtntMap->cnt > 1 ) {
                        sts = bmt_unlink_mcells (bfap->dmnP,
                          bfap->tag,
                          savMC,
                          baseXtntMap->subXtntMap[baseSXMapCnt+1].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH,
			  CHAINMCID);
                    }
                    if ( sts != EOK ) {
                        RAISE_EXCEPTION(sts);
                    }
                }
            }

            sts = bmt_link_mcells ( bfap->dmnP,
                                    bfap->tag,
                                    savMC,
                                    overXtntMap->subXtntMap[0].mcellId,
                              overXtntMap->subXtntMap[overSXMapCnt - 1].mcellId,
                                    ftxH);
        } else {

            /* Case IV & VII */
            sts = x_detach_extent_chain ( bfap,
                                          baseXtntMap,
                                          ftxH,
                                          &throwAwayMC,
                                          &throwAwayMC);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            sts = bmt_link_mcells ( bfap->dmnP,
                                    bfap->tag,
                                    bfap->primMCId,
                                    overXtntMap->subXtntMap[0].mcellId,
                                    overXtntMap->subXtntMap[overSXMapCnt - 1].mcellId,
                                    ftxH);
        }
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    }

/*
** step 5) copy subextents from baseXtntMap to replXtntMap
*/

    for ( ;
          baseSXMapCnt < baseXtntMap->cnt;
          baseSXMapCnt++, replSXMapCnt++ ) {

        baseSubXtntMapp = &baseXtntMap->subXtntMap[baseSXMapCnt];

        if ( last_overlay_fob + 1 <
             baseSubXtntMapp->bssxmFobOffset + 
             baseSubXtntMapp->bssxmFobCnt ) {

            break;
        }

        MS_SMP_ASSERT(replXtntMap->maxCnt > replSXMapCnt);

        replSubXtntMapp = &replXtntMap->subXtntMap[replSXMapCnt];

        /*
         * We want only XTRA extent records in the deferred delete list code.
         */
        if (baseSubXtntMapp->type == BSR_XTNTS) {
            /* Make temporary copy of the extent map since we can
             * not change the inmemory one without holding locks and
             * we can not call imm_copy_sub_xtnt_map which may allocate
             * memory or read from disk (which probably can't happen
             * but ...) while holding the xtntMap_lk for write.
             * NOTE that we only need a small temp map(BSR_XTNTS).
             */
             
            tempSubXtntMap = *baseSubXtntMapp;
            for (i=0;i<BMT_XTNTS;i++){
                tempXtnt[i] = baseSubXtntMapp->bsXA[i];
            }
            tempSubXtntMap.bsXA = &(tempXtnt[0]);

            tempSubXtntMap.type = BSR_XTRA_XTNTS;
            tempSubXtntMap.onDiskMaxCnt = BMT_XTRA_XTNTS;
            tempSubXtntMapp = baseSubXtntMapp;
            baseSubXtntMapp = &tempSubXtntMap;
            switchedTypes = TRUE;
        }

        sts = imm_copy_sub_xtnt_map( bfap,
                                     baseXtntMap,
                                     baseSubXtntMapp,
                                     replSubXtntMapp);

        if (switchedTypes) {
            baseSubXtntMapp = tempSubXtntMapp;
            switchedTypes = FALSE;
        }

        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        replXtntMap->cnt = replXtntMap->validCnt = replSXMapCnt + 1;

        /* 
         * If the subextent we just copied from the base to the replace
         * map is of type BSR_XTNTS, then it was in the base's primary
         * mcell.  Since we don't want to deallocate that mcell, get a
         * new one for the replace sub extent map containing the BSR_XTNTS
         * descriptors and initialize it.
         */
        if (baseSubXtntMapp->type == BSR_XTNTS) {
           sts = stg_alloc_new_mcell ( bfap,
                                       bfap->bfSetp->dirTag,
                                       bfap->tag,
                                       (vdIndexT) replSubXtntMapp->mcellId.volume,
                                       ftxH,
                                       &newMcellId,
                                       FALSE);
           if ( sts != EOK ) {
               RAISE_EXCEPTION(sts);
           }
           MS_SMP_ASSERT(replSubXtntMapp->mcellId.volume == newMcellId.volume);
           replSubXtntMapp->mcellId = newMcellId;
           gotMcell = TRUE;

           sts = odm_create_xtnt_rec ( bfap,
                                       bfap->bfSetp,
                                       replSubXtntMapp,
                                       ftxH);
           if ( sts != EOK ) {
               RAISE_EXCEPTION(sts);
           }

           /*
            * There should be no other mcells in the replace extent map.
            */
          MS_SMP_ASSERT(replSXMapCnt == 0);

        } /* If we just copied a BSR_XTNTS subextent map */

    } /* End loop to copy subextents from baseXtntMap to replXtntMap */

    /* If we step 3 produced an mcell and step 5 processed some subextents, */
    /* link step 5's mcell(s) to step 3's mcell. */
    if ( gotMcell && replSXMapCnt > 1 ) {
        /* Case I */
        sts = bmt_link_mcells ( bfap->dmnP,
                              bfap->tag,
                              replXtntMap->subXtntMap[0].mcellId,
                              replXtntMap->subXtntMap[1].mcellId,
                              replXtntMap->subXtntMap[replSXMapCnt - 1].mcellId,
                              ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    }

    /* If overXtntMap covers the end of the file we are done. */
    if ( baseSXMapCnt == baseXtntMap->cnt ) {
        /* Case V & VIII */
        /* Note that in step 3 if we did not finish processing the base */
        /* subextent, baseSXMapCnt was not incremented. Therefore, case III, */
        /* for example, will not get here. */
        goto done;
    }

    /* check if overlay lines up on subxtnt boundary. if so skip step 6 & 7 */
    baseSubXtntMapp = &baseXtntMap->subXtntMap[baseSXMapCnt];

    if ( last_overlay_fob + 1 !=
         baseSubXtntMapp->bssxmFobOffset + 
         baseSubXtntMapp->bssxmFobCnt ) {

/*
** step 6) copy part of a subextent from baseXtntMap to replXtntMap
*/

        MS_SMP_ASSERT(replXtntMap->maxCnt > replSXMapCnt);

        replSubXtntMapp = &replXtntMap->subXtntMap[replSXMapCnt];
        /* replSXMapCnt will be == 0 in case III where we don't want to copy */
        /* the subextnt map. replSubXtntMapp->cnt will be == 0 in case IV */
        /* where we do want to copy the subextnt map. */
        if ( replSXMapCnt != 0 || replSubXtntMapp->cnt == 0) {

            /*
             * We want only XTRA extent records in the deferred delete list code.
             */
            if (baseSubXtntMapp->type == BSR_XTNTS) {
                /* Make temporary copy of the extent map since we can
                 * not change the inmemory one without holding locks and
                 * we can not call imm_copy_sub_xtnt_map which may allocate
                 * memory or read from disk (which probably can't happen
                 * but ...) while holding the xtntMap_lk for write.
                 * NOTE that we only need a small temp map(BSR_XTNTS).
                 */
                
                tempSubXtntMap = *baseSubXtntMapp;
                for (i=0;i<BMT_XTNTS;i++){
                    tempXtnt[i] = baseSubXtntMapp->bsXA[i];
                }
                tempSubXtntMap.bsXA = &(tempXtnt[0]);
                
                tempSubXtntMap.type = BSR_XTRA_XTNTS;
                tempSubXtntMap.onDiskMaxCnt = BMT_XTRA_XTNTS;
                tempSubXtntMapp = baseSubXtntMapp;
                baseSubXtntMapp = &tempSubXtntMap;
                switchedTypes = TRUE;
            }

            sts = imm_copy_sub_xtnt_map( bfap,
                                         baseXtntMap,
                                         baseSubXtntMapp,
                                         replSubXtntMapp);

            if (switchedTypes) {
                baseSubXtntMapp = tempSubXtntMapp;
                switchedTypes = FALSE;
            }

            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }

            replXtntMap->cnt = replXtntMap->validCnt = replSXMapCnt + 1;
        }

        part2BsXA.bsx_fob_offset= 0;       /* flag for step 7 */

        /* Find the boundary in baseXtntMap of the last fob in the overlay. */
        /* Copy the first part to replXtntMap. If the boundary does not lie */
        /* on an extent boundary, break the extent in two. Save descIndex */
        /* and the second half of the extent for step 7. */
        for ( descIndex = 0;
              descIndex < replXtntMap->subXtntMap[replSXMapCnt].cnt - 1;
              descIndex++ ) {

            if ( last_overlay_fob + 1 >=
                 replSubXtntMapp->bsXA[descIndex + 1].bsx_fob_offset) {
                continue;
            }

            /* if the overlay ends on an extent boundary */
            if ( last_overlay_fob + 1 ==
                 replSubXtntMapp->bsXA[descIndex].bsx_fob_offset) {

                replSubXtntMapp->bsXA[descIndex].bsx_vd_blk = XTNT_TERM;
                replSubXtntMapp->cnt = descIndex + 1;
                replSubXtntMapp->updateStart = descIndex + 1;
                break;
            }

            /* the overlay does not ends on an extent boundary */
            imm_split_desc (last_overlay_fob + 1,
                            &replSubXtntMapp->bsXA[descIndex],
                            &part1BsXA,
                            &part2BsXA);
            replSubXtntMapp->bsXA[descIndex + 1].bsx_fob_offset= part2BsXA.bsx_fob_offset;
            replSubXtntMapp->bsXA[descIndex + 1].bsx_vd_blk = XTNT_TERM;
            replSubXtntMapp->cnt = descIndex + 2;
            replSubXtntMapp->updateStart = descIndex + 2;
            replSubXtntMapp->bssxmFobCnt =
              replSubXtntMapp->bsXA[replSubXtntMapp->cnt - 1].bsx_fob_offset -
              replSubXtntMapp->bssxmFobOffset;
            break;
        }

/*
** step 6a) get new mcell for replXtntMap
*/

        sts = stg_alloc_new_mcell ( bfap,
                                    bfap->bfSetp->dirTag,
                                    bfap->tag,
                                    (vdIndexT) replSubXtntMapp->mcellId.volume,
                                    ftxH,
                                    &newMcellId,
                                    FALSE);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        MS_SMP_ASSERT(replSubXtntMapp->mcellId.volume == newMcellId.volume);
        replSubXtntMapp->mcellId = newMcellId;
        
        MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
        sts = odm_create_xtnt_rec ( bfap,
                                    bfap->bfSetp,
                                    replSubXtntMapp,
                                    ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * If the base mcell is the primary mcell, we want to update
         * it on disk.  Otherwise, we've allocated a new mcell to
         * take its place so we can just free it.
         */
        if (baseSubXtntMapp->type == BSR_XTNTS) {
            MS_SMP_ASSERT( MCID_EQL(baseSubXtntMapp->mcellId, modSubXtntMapp->mcellId));
            modSubXtntMapp->updateStart = 0;
            modSubXtntMapp->updateEnd = modSubXtntMapp->cnt-1;
            sts = update_xtnt_rec (bfap->dmnP,
                                   bfap->tag,
                                   modSubXtntMapp,
                                   ftxH);
        }
        else {
            sts = deferred_free_mcell ( VD_HTOP(baseSubXtntMapp->mcellId.volume,
                                                bfap->dmnP),
                                        baseSubXtntMapp->mcellId,
                                        ftxH);
        }
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /* If there are any mcells from step 3 or 5, link this mcell to them. */
        if ( replSXMapCnt > 0 ) {
            /* Case I, VI, VIII */
            sts = bmt_link_mcells ( bfap->dmnP,
                                bfap->tag,
                                replXtntMap->subXtntMap[replSXMapCnt-1].mcellId,
                                replSubXtntMapp->mcellId,
                                replSubXtntMapp->mcellId,
                                ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }

/*
** step 7) copy part of subextent from baseXtntMap to modXtntMapp
*/

        /* If the overlay is entirely within a baseXtntMap subextent then */
        /* moveCnt defines how many extents were processed in step 3. */
        /* descIndex defines how many extents were processed in step 6. */
        /* The sum defines the extent to begin processing in this step. */
        descIndex += moveCnt;

        MS_SMP_ASSERT(modXtntMap->maxCnt > modSXMapCnt);
        modSubXtntMapp = &modXtntMap->subXtntMap[modSXMapCnt];
        sts = imm_copy_sub_xtnt_map( bfap,
                                     baseXtntMap,
                                     baseSubXtntMapp,
                                     modSubXtntMapp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }


        /* Continue processing the same baseXtntMap subextent step 6 worked */
        /* on. Use descIndex and part2BsXA from step 6 to know where to start.*/
        modXtntMap->cnt = modXtntMap->validCnt = modSXMapCnt + 1;
        if ( part2BsXA.bsx_fob_offset != 0 ) {   /* flag from part 6 */
            modSubXtntMapp->bsXA[descIndex] = part2BsXA;
        }

        moveCnt = baseSubXtntMapp->cnt - descIndex;
        for (i = 0; i < moveCnt; i++) {
            modSubXtntMapp->bsXA[i] = modSubXtntMapp->bsXA[descIndex + i];
        }
        modSubXtntMapp->cnt = moveCnt;
        modSubXtntMapp->updateStart = moveCnt;
        modSubXtntMapp->bssxmFobOffset = 
            modSubXtntMapp->bsXA[0].bsx_fob_offset;
        modSubXtntMapp->bssxmFobCnt = 
            modSubXtntMapp->bsXA[moveCnt - 1].bsx_fob_offset -
            modSubXtntMapp->bssxmFobOffset;

/*
** step 7a) get new mcell for modXtntMap
*/

        sts = stg_alloc_new_mcell ( bfap,
                                    bfap->bfSetp->dirTag,
                                    bfap->tag,
                                    (vdIndexT) modSubXtntMapp->mcellId.volume,
                                    ftxH,
                                    &newMcellId,
                                    FALSE);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        MS_SMP_ASSERT(modSubXtntMapp->mcellId.volume == newMcellId.volume);
        modSubXtntMapp->mcellId = newMcellId;

        /* If this is copied from the first base subextent (case III & IV), */
        /* the type will be BSR_SHADOW_XTNTS. Since this is not the first */
        /* new subextent map (at least, step 4 copied some), we must change */
        /* the type to BSR_XTRA_XTNTS before initializing the new mcell. */
        modSubXtntMapp->type = BSR_XTRA_XTNTS;
        modSubXtntMapp->onDiskMaxCnt = BMT_XTRA_XTNTS;
        MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
        sts = odm_create_xtnt_rec ( bfap,
                                    bfap->bfSetp,
                                    modSubXtntMapp,
                                    ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        MS_SMP_ASSERT(modSXMapCnt > 0);
        sts = bmt_link_mcells ( bfap->dmnP,
                                bfap->tag,
                                modXtntMap->subXtntMap[modSXMapCnt-1].mcellId,
                                modSubXtntMapp->mcellId,
                                modSubXtntMapp->mcellId,
                                ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        baseSXMapCnt++;
        modSXMapCnt++;
    }   /* end of "if end of overlay extents divide a base mcell" */

/*
** step 8) copy subextents from baseXtntMap to modXtntMap
*/

    /* If overXtntMap covers the end of the file we are done. */
    if ( baseSXMapCnt >= baseXtntMap->cnt ) {
        goto done;
    }
 
    MS_SMP_ASSERT(modSXMapCnt>0);
    savMC = modXtntMap->subXtntMap[modSXMapCnt - 1].mcellId;
    savBaseMC = baseXtntMap->subXtntMap[baseSXMapCnt].mcellId;
    savBaseCnt = baseSXMapCnt;
    for ( ;
          baseSXMapCnt < baseXtntMap->cnt;
          baseSXMapCnt++, modSXMapCnt++ ) {

        MS_SMP_ASSERT(modXtntMap->maxCnt > modSXMapCnt);
        sts = imm_copy_sub_xtnt_map( bfap,
                                     baseXtntMap,
                                      &baseXtntMap->subXtntMap[baseSXMapCnt],
                                      &modXtntMap->subXtntMap[modSXMapCnt]);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

    }
    modXtntMap->cnt = modXtntMap->validCnt = modSXMapCnt;

    if ( baseSXMapCnt > savBaseCnt ) {
        /* Case I & II */
        sts = bmt_link_mcells ( bfap->dmnP,
                          bfap->tag,
                          savMC,
                          savBaseMC,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    }

done:
    modXtntMap->updateEnd = 0;
    modXtntMap->updateStart = modXtntMap->cnt - 1;
    modXtntMap->hdrType = modXtntMap->subXtntMap[0].type;
    modXtntMap->hdrMcellId = modXtntMap->subXtntMap[0].mcellId;
    modXtntMap->allocVdIndex = baseXtntMap->allocVdIndex;
    modXtntMap->bsxmNextFob = baseXtntMap->bsxmNextFob;

    /* mcellCnt is the count of mcells in the BSR_SHADOW_XTNTS record. */
    /* modXtntMap->cnt is what the count should be. */
    if ( modXtntMap->cnt != mcellCnt ) {
        sts = update_mcell_cnt ( bfap->dmnP,
                                 bfap->tag,
                                 modXtntMap->hdrMcellId,
                                 modXtntMap->hdrType,
                                 modXtntMap->cnt - mcellCnt,
                                 ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
    }

    MS_SMP_ASSERT(imm_check_xtnt_map(modXtntMap) == EOK);
    *retModXtntMap = modXtntMap;

    replXtntMap->updateEnd = 0;
    replXtntMap->updateStart = replXtntMap->cnt - 1;
    replXtntMap->hdrType = replXtntMap->subXtntMap[0].type;
    replXtntMap->hdrMcellId = replXtntMap->subXtntMap[0].mcellId;
    replXtntMap->allocVdIndex = baseXtntMap->allocVdIndex;

    *retReplXtntMap = replXtntMap;
    return EOK;

HANDLE_EXCEPTION:

    if (modXtntMap != NULL) {
        ms_free (modXtntMap);
    }
    if (replXtntMap != NULL) {
        ms_free (replXtntMap);
    }
    return sts;

}  /* end overlay_xtnt_map */


/*
 * imm_overlay_xtnt_map
 *
 * This function overlays "overXtntMap" onto the "baseXtntMap".  The results
 * are described by "newXtntMap" and "replXtntMap".  The fob range described
 * by the overlay extent map must be a subset of the fob range described by
 * the base extent map.
 *
 * The overlay extent map describes real and, optionally, hole extents. If hole
 * extents are described, they cannot be the first or last extents described by
 * the map.  For every real extent described by the overlay extent map, the
 * extent's range must be described by real extents in the base extent map.
 *
 * This function assumes the base and overlay extent maps are as described by
 * the previous paragraphs.
 *
 * The new extent map describes fobs mapped by both the base and the overlay
 * extent maps.  The fobs from the base extent map are those that do not
 * intersect with real fobs described by the overlay extent map.  The fobs 
 * from the overlay extent map are those real fobs that do intersect with
 * real fobs described by the base extent map.
 *
 * The replace extent map describes fobs mapped by the base extent map that
 * intersect with real fobs mapped by the overlay extent map.
 *
 * The caller must have exclusive access to the overlay and base extent maps.
 */


statusT
imm_overlay_xtnt_map (bfAccessT * bfap,
                      bsInMemXtntMapT *baseXtntMap,  /* in */
                      bsInMemXtntMapT *overXtntMap,  /* in */
                      bsInMemXtntMapT **retNewXtntMap,  /* out */
                      bsInMemXtntMapT **retReplXtntMap  /* out */
                      )
{
    bsXtntDescT baseDesc;
    bsInMemXtntDescIdT baseDescId;
    bsXtntT bsXA[2];
    uint32_t currOverXtnt;
    bsInMemXtntMapT *newXtntMap = NULL;
    bf_fob_t next_base_xtnt_fob; /* Tracks the fob the next extent*
                                              * in the base map begins on     */
    bsXtntDescT overDesc;
    bsInMemXtntDescIdT overDescId;
    bsXtntT part1BsXA;
    bsXtntT part2BsXA;
    bsInMemXtntMapT *replXtntMap = NULL;
    statusT sts;
    vdIndexT vdIndex;
    bfMCIdT initMcellId;

    imm_get_first_xtnt_desc (overXtntMap, &overDescId, &overDesc);

    /*
     * We know that the first replace sub extent map has the same
     * vd index as the first intersecting base sub extent map.  So,
     * initialize the first sub extent map here.
     */
    sts = imm_create_xtnt_map (
                               baseXtntMap->domain,
                               1,  /* maxCnt */
                               &replXtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    imm_get_xtnt_desc( overDesc.bsxdFobOffset,
                       baseXtntMap,
                       0,
                       &baseDescId,
                       &baseDesc);

    initMcellId = bsNilMCId;
    initMcellId.volume = baseDesc.volIndex;
    sts = imm_init_sub_xtnt_map(
                                 &(replXtntMap->subXtntMap[0]),
                                 overDesc.bsxdFobOffset,
                                 0,  /* fob_cnt */
                                 initMcellId,
                                 BSR_XTNTS,
                                 BMT_XTNTS,
                                 0  /* force default maxCnt */
                                 );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }
    replXtntMap->cnt++;

    /*
     * We don't know the vd index of the first entry in the new extent
     * map.  So, let imm_copy_xtnt_descs() figure it out.
     */
    sts = imm_create_xtnt_map (
                               baseXtntMap->domain,
                               1,  /* maxCnt */
                               &newXtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    imm_get_first_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);

    bsXA[1].bsx_vd_blk = XTNT_TERM;

    /*
     * STEP 1
     */

    /*
     * Copy base descriptors to the new extent map until we find a
     * descriptor that describes a range that intersects with
     * the first overlay extent's range.
     */

    next_base_xtnt_fob = baseDesc.bsxdFobOffset + 
        baseDesc.bsxdFobCnt;
    while ((next_base_xtnt_fob <= overDesc.bsxdFobOffset) && 
            (next_base_xtnt_fob != 0)) {

        bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
        bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
        bsXA[1].bsx_fob_offset = next_base_xtnt_fob;
        sts = imm_copy_xtnt_descs( bfap,
                                   baseDesc.volIndex,
                                   &(bsXA[0]),
                                   1,
                                   FALSE,
                                   newXtntMap,
                                   XTNT_TEMP_XTNTMAP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
        next_base_xtnt_fob = baseDesc.bsxdFobOffset + 
            baseDesc.bsxdFobCnt;
    }  /* end while */

    if (baseDesc.bsxdFobOffset < overDesc.bsxdFobOffset ) {
        /*
         * The current base extent's fob offset and the first overlay extent's
         * fob offset are different.  Copy part of the base descriptor to the
         * new extent map.
         */
        bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
        bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
        imm_split_desc (
                        overDesc.bsxdFobOffset,
                        &(bsXA[0]),
                        &part1BsXA,
                        &part2BsXA
                        );
        bsXA[1].bsx_fob_offset = part2BsXA.bsx_fob_offset;
        sts = imm_copy_xtnt_descs( bfap,
                                   baseDesc.volIndex,
                                   &(bsXA[0]),
                                   1,
                                   FALSE,
                                   newXtntMap,
                                   XTNT_TEMP_XTNTMAP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        baseDesc.bsxdFobOffset = part2BsXA.bsx_fob_offset;
        baseDesc.bsxdFobCnt = next_base_xtnt_fob - 
            baseDesc.bsxdFobOffset;
        baseDesc.bsxdVdBlk = part2BsXA.bsx_vd_blk;
    }

    while (1) {

        /*
         * STEP 2
         */

        /*
         * Copy overlay descriptors to the new extent map until we find a hole
         * or we run out of overlay extent descriptors.
         */

        do {

            bsXA[0].bsx_fob_offset = overDesc.bsxdFobOffset;
            bsXA[0].bsx_vd_blk = overDesc.bsxdVdBlk;
            bsXA[1].bsx_fob_offset = overDesc.bsxdFobOffset + 
                overDesc.bsxdFobCnt;
            sts = imm_copy_xtnt_descs (bfap,
                                       overDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       XTNT_TEMP_XTNTMAP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            /*
             * Save the start of the next overlay extent which is about to
             * become the current extent.  The start is either the start of
             * the next real/hole extent or the next fob after the last valid
             * overlay fob.
             */
            currOverXtnt = overDesc.bsxdFobOffset + 
                overDesc.bsxdFobCnt;

            imm_get_next_xtnt_desc (overXtntMap, &overDescId, &overDesc);
        } while (overDesc.bsxdVdBlk != -1);

        /*
         * Set the current overlay extent descriptor's fob offset.  This
         * only has an effect when we ran out of overlay extent descriptors.
         * (imm_get_next_xtnt_desc() returns a fob offset of 0 when there
         * are no more descriptors.)
         */
        overDesc.bsxdFobOffset = currOverXtnt;

        /*
         * Copy base descriptors to the replace extent map until we find a
         * descriptor that describes a range that intersects with
         * the next overlay extent's range or we run out of base extent
         * descriptors.
         */

        next_base_xtnt_fob = baseDesc.bsxdFobOffset + 
            baseDesc.bsxdFobCnt;
        while ((next_base_xtnt_fob <= overDesc.bsxdFobOffset) && 
                (baseDesc.bsxdFobCnt > 0)) {

            bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
            bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
            bsXA[1].bsx_fob_offset = next_base_xtnt_fob;
            sts = imm_copy_xtnt_descs (bfap,
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       replXtntMap,
                                       XTNT_TEMP_XTNTMAP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
            next_base_xtnt_fob = baseDesc.bsxdFobOffset + 
                baseDesc.bsxdFobCnt;
        }  /* end while */

        if (baseDesc.bsxdFobCnt == 0) {
            /*
             * No more base extent descriptors.
             */
            break;  /* out of while */
        }

        if (baseDesc.bsxdFobOffset < overDesc.bsxdFobOffset ) {
            /*
             * The current base extent's fob offset and the current overlay
             * extent's fob offset are different.  Copy part of the base
             * descriptor to the replace extent map.
             */
            bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
            bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
            imm_split_desc (
                            overDesc.bsxdFobOffset,
                            &(bsXA[0]),
                            &part1BsXA,
                            &part2BsXA
                            );
            bsXA[1].bsx_fob_offset = part2BsXA.bsx_fob_offset;
            sts = imm_copy_xtnt_descs (bfap,
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       replXtntMap,
                                       XTNT_TEMP_XTNTMAP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            baseDesc.bsxdFobOffset = part2BsXA.bsx_fob_offset;
            baseDesc.bsxdFobCnt = next_base_xtnt_fob - 
                baseDesc.bsxdFobOffset;
            baseDesc.bsxdVdBlk = part2BsXA.bsx_vd_blk;
        }

        if (overDesc.bsxdFobCnt == 0) {
            /*
             * No more overlay extent descriptors.
             */
            break;  /* out of while */
        }
        imm_get_next_xtnt_desc (overXtntMap, &overDescId, &overDesc);

        /*
         * STEP 3
         */

        /*
         * Copy base descriptors to the new extent map until we find a
         * descriptor that describes a range that intersects with
         * the next overlay extent's range.
         */

        next_base_xtnt_fob = baseDesc.bsxdFobOffset + 
            baseDesc.bsxdFobCnt;
        while ((next_base_xtnt_fob <= overDesc.bsxdFobOffset) && 
                (next_base_xtnt_fob != 0)) {

            bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
            bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
            bsXA[1].bsx_fob_offset = next_base_xtnt_fob;
            sts = imm_copy_xtnt_descs (bfap,
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       XTNT_TEMP_XTNTMAP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
            next_base_xtnt_fob = baseDesc.bsxdFobOffset + 
                baseDesc.bsxdFobCnt;
        }  /* end while */

        if (baseDesc.bsxdFobOffset < overDesc.bsxdFobOffset ) {
            /*
             * FIX - this leg isn't tested yet.
             */
            RAISE_EXCEPTION (ENOT_SUPPORTED);
            /*
             * The current base extent's fob offset and the current overlay
             * extent's fob offset are different.  Copy part of the base
             * descriptor to the new extent map.
             */
            bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
            bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
            imm_split_desc (
                            overDesc.bsxdFobOffset,
                            &(bsXA[0]),
                            &part1BsXA,
                            &part2BsXA
                            );
            bsXA[1].bsx_fob_offset = part2BsXA.bsx_fob_offset;
            sts = imm_copy_xtnt_descs (bfap,
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       XTNT_TEMP_XTNTMAP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            baseDesc.bsxdFobOffset = part2BsXA.bsx_fob_offset;
            baseDesc.bsxdFobCnt = next_base_xtnt_fob - 
                baseDesc.bsxdFobOffset;
            baseDesc.bsxdVdBlk = part2BsXA.bsx_vd_blk;
        }

    }  /* end while */

    /*
     * STEP 4
     */

    /*
     * If there are more base descriptors, copy them to the new extent map.
     */

    if (baseDesc.bsxdFobCnt != 0) {

        bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
        bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
        bsXA[1].bsx_fob_offset = baseDesc.bsxdFobOffset + 
            baseDesc.bsxdFobCnt;
        sts = imm_copy_xtnt_descs( bfap,
                                   baseDesc.volIndex,
                                   &(bsXA[0]),
                                   1,
                                   FALSE,
                                   newXtntMap,
                                   XTNT_TEMP_XTNTMAP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
        while (baseDesc.bsxdFobCnt != 0) {

            bsXA[0].bsx_fob_offset = baseDesc.bsxdFobOffset;
            bsXA[0].bsx_vd_blk = baseDesc.bsxdVdBlk;
            bsXA[1].bsx_fob_offset = baseDesc.bsxdFobOffset + 
                baseDesc.bsxdFobCnt;
            sts = imm_copy_xtnt_descs (bfap,
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       XTNT_TEMP_XTNTMAP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
        }  /* end while */
    }

    *retNewXtntMap = newXtntMap;
    *retReplXtntMap = replXtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if (newXtntMap != NULL) {
        ms_free (newXtntMap);
    }
    if (replXtntMap != NULL) {
        ms_free (replXtntMap);
    }
    return sts;

}  /* end imm_overlay_xtnt_map */

/*
 * imm_copy_xtnt_map
 *
 * This function creates an extent map and copies the entries from the source
 * map into it.
 */

statusT
imm_copy_xtnt_map (
                   bfAccessT *bfap,              /* in */
                   bsInMemXtntMapT *srcXtntMap,  /* in */
                   bsInMemXtntMapT **dstXtntMap  /* out */
                   )
{
    statusT sts;
    bsInMemXtntMapT *xtntMap = NULL;
    int i, cnt=0;


    /*
     * Create the new extent map and copy the original sub extent maps.
     * These are exact copies including the mcell pointers.
     */

    /*
     * Create and initialize an empty extent map.
     */
    cnt = srcXtntMap->validCnt;
    sts = imm_create_xtnt_map( 
                               srcXtntMap->domain,
                               cnt,
                               &xtntMap
                             );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    for (i = 0; i < cnt; i++) {
        sts = imm_copy_sub_xtnt_map( bfap,
                                     srcXtntMap,
                                     &srcXtntMap->subXtntMap[i],
                                     &xtntMap->subXtntMap[i]);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
    }  /* end for */

    xtntMap->cnt = cnt;
    xtntMap->updateStart = xtntMap->cnt;
    xtntMap->validCnt = xtntMap->cnt;

    *dstXtntMap = xtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if (xtntMap != NULL) {
        imm_delete_xtnt_map (xtntMap);
        xtntMap = NULL;
    }
    return sts;

}  /* end imm_copy_xtnt_map */

/*
 * imm_remove_range_map
 *
 * This function removes the specified range's mapping from the extent
 * map.  If the descriptors that describe the range are in the middle
 * of the extent map, the descriptors are replaced with a hole descriptor.
 * If the hole descriptor is adjacent to other hole descriptors, the holes
 * are merged.
 *
 * If the descriptors that describe the range are at the end of the
 * extent map, the descriptors are replaced with the termination descriptor.
 * If the termination descriptor is adjacent to a hole descriptor, the
 * hole descriptor is changed to the termination descriptor.
 *
 * This function does not modify the original extent map.  Instead,
 * it uses the sub extent maps at the end of the extent map to hold
 * the results of the remove operation.  It sets the extent map's
 * "updateStart" and "updateEnd" fields to denote the original
 * sub extent maps without the range mappings.  It sets the extent
 * map's "origStart" and "origEnd" fields to denote which original
 * sub extent maps contain the to-be-removed mappings.
 */


statusT
imm_remove_range_map(bfAccessT *bfap,
                     bf_fob_t fob_offset,   /* in */
                     bf_fob_t fob_cnt,      /* in */
                     bsInMemXtntMapT *xtntMap,          /* in */
                     bf_fob_t *rem_fob_offset,      /* out */
                     bf_fob_t *rem_fob_cnt          /* out */
                     )
{
    bsXtntT bsXA[2];
    bsXtntT desc;
    uint32_t descEnd;
    uint32_t descStart;
    bf_fob_t end_fob_offset;
    uint32_t i;
    bf_fob_t last_fob_offset;
    uint32_t stickyHole=FALSE;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    bfMCIdT initMcellId;

    
    MS_SMP_ASSERT(xtntMap->cnt == xtntMap->validCnt);

    end_fob_offset = (fob_offset + fob_cnt) - 1;

    /*
     * Get the start index and fob offset.
     */

    sts = imm_fob_to_sub_xtnt_map(fob_offset, 
                                   xtntMap, 
                                   0, 
                                   &xtntMap->origStart);
    if (sts != EOK) {
        return sts;
    }
    

    xtntMap->updateStart=0;
    xtntMap->updateEnd=0;

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

    sts = imm_fob_to_xtnt( fob_offset,
                            subXtntMap,
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_STG_HOLE */
                            &descStart);
    if (sts == EOK) {
        MS_SMP_ASSERT(descStart < subXtntMap->cnt - 1);
        if (fob_offset == subXtntMap->bsXA[descStart].bsx_fob_offset) {
            /*
             * The range's starting fob offset is the first
             * fob described by the descriptor.  If the previous
             * descriptor describes a hole, reset the start 
             * offset.
             */
            if (descStart > 0) {
                if ( subXtntMap->bsXA[descStart - 1].bsx_vd_blk == XTNT_TERM ||
                     subXtntMap->bsXA[descStart - 1].bsx_vd_blk == COWED_HOLE )
                {
                    /*
                     * The previous descriptor describes a hole.
                     */
                    descStart--;
                    fob_offset = subXtntMap->bsXA[descStart].bsx_fob_offset;

                    /* Now check if the subxtnt map prior to us is the primary
                     * and pick it up of we can.
                     */
                    if ( (descStart == 0) && (xtntMap->origStart == 1) )
                    {
                        subXtntMap = &(xtntMap->subXtntMap[0]);
                        if (subXtntMap->cnt == 1)
                        {

                            /* If we create a hole at the beginning of
                             * the second subextent map, and the first
                             * subextent map is a primary with no
                             * storage, we may be removing all the
                             * storage. If that is the case we want to
                             * include the primary in the update range
                             * to make sure we don't produce a
                             * BSR_XTRA_XTNTS cell with one extent
                             * descriptor.  */

                            /* The only 1 entry subxtnt allowed is the
                             * primary. We are removing storage from
                             * the * begining for the file so include
                             * this in * the update range in hopes
                             * that we can reuse * it.  */

                            MS_SMP_ASSERT(subXtntMap->bsXA[0].bsx_vd_blk == 
                                          XTNT_TERM);
                            xtntMap->origStart--;
                            descStart = 0;
                            fob_offset = 0;
                        }
                    }
                }
            } else {
                /* Removing storage from first extent in subextent map. */
                while (xtntMap->origStart > 0) 
                {
                    /*
                     * This loop is mainly for cleaning up corruption left
                     * over from older kernels. The exception here is picking
                     * up an orphan primary extent.
                     */

                    subXtntMap =&(xtntMap->subXtntMap[xtntMap->origStart - 1]);

                    if (subXtntMap->cnt > 1) 
                    {
                        if ( subXtntMap->bsXA[subXtntMap->cnt - 2].bsx_vd_blk == 
                             XTNT_TERM ||
                             subXtntMap->bsXA[subXtntMap->cnt - 2].bsx_vd_blk == 
                             COWED_HOLE )
                        {
                            /* The last descriptor in the previous sub
                             * extent map describes a hole.
                             *
                             * NOTE: This situation should be
                             * impossible since holes should never be
                             * at the end of a subxtnt map. However in
                             * the past these type subxtnts were
                             * incorrectly allowed to get out to disk
                             * so we better try to clean them up.  */
                            
                            xtntMap->origStart--;
                            descStart = subXtntMap->cnt - 2;
                            fob_offset = subXtntMap->bsXA[descStart].bsx_fob_offset;
                        }
                        break;
                    }
                    else
                    {
                        /* subXtntMap->cnt == 1 */
                        /* The only 1 entry subxtnt allowed is the
                         * primary. We are removing storage from the
                         * begining of the file so include this in
                         * the update range in hopes that we can reuse
                         * it. 
                         *
                         * OR
                         *
                         * Version 3 domains may have left over on-disk corruption
                         * from older kernels. This seems to show itself as orphan
                         * extents (each with the same starting fob) ultimately
                         * preceeded by an extent made up of only a hole. Clean
                         * this mess up by including these extents in the range 
                         * being removed */
                        
                        MS_SMP_ASSERT(subXtntMap->bsXA[0].bsx_vd_blk == XTNT_TERM);
                        xtntMap->origStart--;
                        descStart=0;
                    }
                }
                
            }
        }
    } else {  /* sts != EOK */
        if (sts == E_STG_HOLE) {
            /*
             * The first fob of the range is in a hole.
             *
             * Note:  This code assumes that two adjacent descriptors cannot
             * both describe a hole.
             */
            fob_offset = subXtntMap->bsXA[descStart].bsx_fob_offset;
            if ( (descStart == 0) && (xtntMap->origStart == 1) )
            {
                subXtntMap = &(xtntMap->subXtntMap[0]);
                if (subXtntMap->cnt == 1)
                {
                    /* The only 1 entry subxtnt allowed is the
                     * primary. We are removing storage from the
                     * begining for the file so include this in the
                     * update range in hopes that we can reuse it.  */
                    
                    MS_SMP_ASSERT(subXtntMap->bsXA[0].bsx_vd_blk == XTNT_TERM);
                    xtntMap->origStart--;
                    descStart=0;
                    fob_offset = 0;
                }
            }
        } else {
            return sts;
        }
    }

    /*
     * Get the end index and fob offset.
     */

    sts = imm_fob_to_sub_xtnt_map( end_fob_offset,
                                   xtntMap,
                                   0,
                                   &xtntMap->origEnd);
    if (sts != EOK) {
        return sts;
    }
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
    sts = imm_fob_to_xtnt( end_fob_offset,
                            subXtntMap,
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_STG_HOLE */
                            &descEnd);
    if (sts == EOK) {
        MS_SMP_ASSERT(descEnd < subXtntMap->cnt - 1);
        if (end_fob_offset == (subXtntMap->bsXA[descEnd + 1].bsx_fob_offset - 1)) {
            /*
             * The range's ending fob offset is the last
             * fob described by the descriptor.  If the next
             * descriptor describes a hole, reset the end fob 
             * offset.
             */
            if (descEnd != (subXtntMap->cnt - 2)) {
                if ( subXtntMap->bsXA[descEnd + 1].bsx_vd_blk == XTNT_TERM ||
                     subXtntMap->bsXA[descEnd + 1].bsx_vd_blk == COWED_HOLE )
                {
                    /*
                     * The next descriptor describes a hole.
                     */
                    descEnd++;
                    end_fob_offset = subXtntMap->bsXA[descEnd + 1].bsx_fob_offset - 1;
                }
            } else {
                if (xtntMap->origEnd != (xtntMap->cnt - 1)) {
                    subXtntMap = &xtntMap->subXtntMap[xtntMap->origEnd + 1];
                    if (subXtntMap->cnt > 1) 
                    {
                        /* We are about to create a hole at the end of
                         * a sub extent map. The rule is that hole
                         * must stick to the storage in front of
                         * it. In other words the subextent map in
                         * front of us must be modified.  */

                        xtntMap->origEnd++;
                        descEnd = 0;
                        stickyHole=TRUE;

                        if ( subXtntMap->bsXA[0].bsx_vd_blk == XTNT_TERM ||
                             subXtntMap->bsXA[0].bsx_vd_blk == COWED_HOLE )
                        {
                            /* The next subxtnt map starts with a hole.
                             * Ignore it by bumping descEnd so we don't
                             * copy it. We will pick it up by increasing the
                             * end_fob_offset 
                             */
                            
                            end_fob_offset = subXtntMap->bsXA[1].bsx_fob_offset - 1;
                            descEnd = 1;                   
                        }
                    }
                }
            }
        }
    } else {/* sts != EOK */
        if (sts == E_STG_HOLE) {
            /*
             * The last fob of the range is in a hole.
             *
             * Note:  This code assumes that two adjacent descriptors cannot
             * both describe a hole.
             */
            end_fob_offset = 
                subXtntMap->bsXA[descEnd + 1].bsx_fob_offset - 1;
            descEnd++;
            stickyHole=TRUE;

        } else {
            return sts;
        }
    }

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->validCnt - 1]);
    last_fob_offset = 
        subXtntMap->bsXA[subXtntMap->cnt - 1].bsx_fob_offset - 1;

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

    if (subXtntMap->bsXA == NULL) {
         domain_panic(xtntMap->domain,
                    "imm_remove_page_map: subXtntMap->bsXA == NULL");
        sts = E_DOMAIN_PANIC;
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }
    
    /* Check to see if the range we are removing starts in the middle
     * of the first subextent or if we are removing all the fobs.
     */

    if((fob_offset > subXtntMap->bsXA[0].bsx_fob_offset) ||
       ((end_fob_offset == last_fob_offset ) && (fob_offset == 0)) ||
       (subXtntMap->type == BSR_XTNTS))
    {
        /*
         * Initialize the first update sub extent map.  It has the same
         * characteristics as the first original sub extent map.
         */

        /*
         * Make room for the update subextent map(s).
         */

        if (xtntMap->cnt >= xtntMap->maxCnt) {
            /* Inform imm_extend_xtnt_map that we are operating on
             * the original and we need to lock the extents when
             * adding memory.
             */
            sts = imm_extend_xtnt_map (bfap,xtntMap,XTNT_ORIG_XTNTMAP);

            if (sts != EOK) {
                return sts;
            }

            subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);
        }

        initMcellId = bsNilMCId;
        initMcellId.volume = subXtntMap->mcellId.volume;
        sts = imm_init_sub_xtnt_map( &(xtntMap->subXtntMap[xtntMap->cnt]),
                                     subXtntMap->bssxmFobOffset,
                                     0,  /* fob_cnt */
                                     initMcellId,
                                     subXtntMap->type,
                                     subXtntMap->onDiskMaxCnt,
                                     0);  /* force default maxCnt */
        if (sts != EOK) {
            return sts;
        }

        xtntMap->updateStart = xtntMap->cnt;
        xtntMap->cnt++;

        /* Copy the descriptors from the first original sub extent map
         * that describe fobs that are not part of the removal range
         * and are positioned before the range.  */

        sts = imm_copy_xtnt_descs( bfap,
                                   (vdIndexT) subXtntMap->mcellId.volume,
                                   subXtntMap->bsXA,
                                   descStart,
                                   FALSE,
                                   xtntMap,
                                   XTNT_ORIG_XTNTMAP /* locking is needed */
                                 );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);
        if (fob_offset != subXtntMap->bsXA[descStart].bsx_fob_offset) {

            /* The start fob offset is not aligned on an extent
             * boundary.  */

            imm_split_desc( fob_offset,
                            &(subXtntMap->bsXA[descStart]),  /* orig */
                            &(bsXA[0]),  /* first part */
                            &desc);  /* last part */
            bsXA[1].bsx_fob_offset = desc.bsx_fob_offset;
            bsXA[1].bsx_vd_blk = XTNT_TERM;

            /* We are operating on the original xtntmap so inform
             * imm_copy_xtnt_descs that locking is needed if the
             * extents are extended
             */

            sts = imm_copy_xtnt_descs( bfap,
                                       (vdIndexT) subXtntMap->mcellId.volume,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       xtntMap,
                                       XTNT_ORIG_XTNTMAP);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

        if((subXtntMap->type == BSR_XTNTS) &&
           (subXtntMap->cnt > 1) &&
           (fob_offset == subXtntMap->bsXA[0].bsx_fob_offset) &&
           (end_fob_offset >= subXtntMap->bsXA[1].bsx_fob_offset))
        {
            /* Our starting subextent lives on the primary. We
             * must force it to be an orphan extent so that the
             * primary mcell can be reused in dealloc_stg
             */
             subXtntMap = &(xtntMap->subXtntMap[xtntMap->updateStart]);
             subXtntMap->bsXA[0].bsx_vd_blk=XTNT_TERM;
             subXtntMap->cnt=1;
        }

        xtntMap->updateEnd=xtntMap->cnt - 1;

    }

    if (end_fob_offset != last_fob_offset) 
    {
        /*
         * The range is not at the end of the mapped fobs.
         * The hole we are creating must stick to the storage 
         * following it.
         */

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
        
        if((xtntMap->origStart != xtntMap->origEnd) ||
           (xtntMap->updateStart == 0) ||
           (xtntMap->origStart == 0))
        {
            
            /* We need a new subxtnt if the range we are removing 
             * ends on a differnt one or we didn't get one already
             */
            
            if (xtntMap->cnt >= xtntMap->maxCnt) {
                sts = imm_extend_xtnt_map (bfap,xtntMap,XTNT_ORIG_XTNTMAP);
                if (sts != EOK) {
                    return sts;
                }
                subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
            }

            initMcellId = bsNilMCId;
            initMcellId.volume = subXtntMap->mcellId.volume;
            sts = imm_init_sub_xtnt_map( &(xtntMap->subXtntMap[xtntMap->cnt]),
                                         fob_offset,
                                         0,  /* fob_cnt*/
                                         initMcellId,
                                         BSR_XTRA_XTNTS,
                                         BMT_XTRA_XTNTS,
                                         0);  /* force default maxCnt */
            if (sts != EOK) {
                return sts;
            }
            
            if(xtntMap->updateStart == 0)
                xtntMap->updateStart = xtntMap->cnt;
            xtntMap->cnt++;

        }
        /*
         * Create a hole descriptor and copy the remaining descriptors.
         */

        bsXA[0].bsx_fob_offset = fob_offset;
        bsXA[0].bsx_vd_blk = XTNT_TERM;
        bsXA[1].bsx_fob_offset = end_fob_offset + 1;
        bsXA[1].bsx_vd_blk = XTNT_TERM;
        /* Indicate that locking needs to be done if extending the
         * extent map.
         */
        sts = imm_copy_xtnt_descs( bfap,
                                   (vdIndexT) subXtntMap->mcellId.volume,
                                   &(bsXA[0]),
                                   1,
                                   TRUE,
                                   xtntMap,
                                   XTNT_ORIG_XTNTMAP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        
        /*
         * Copy the descriptors from the last original sub extent map that
         * describe fobs that are not part of the removal range and are
         * positioned after the range.
         */
        
        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
        if (!stickyHole)
        {
            if (end_fob_offset != 
                    (subXtntMap->bsXA[descEnd + 1].bsx_fob_offset - 1)) {
                /*
                 * The end fob offset is not aligned on an extent boundary.
                 */
                imm_split_desc( end_fob_offset + 1,
                                &(subXtntMap->bsXA[descEnd]),  /* orig */
                                &desc,  /* first part */
                                &(bsXA[0]));  /* last part */
                bsXA[1].bsx_fob_offset = 
                    subXtntMap->bsXA[descEnd + 1].bsx_fob_offset;
                bsXA[1].bsx_vd_blk = XTNT_TERM;
                /* Indicate that locking needs to be done if extending the
                 * extent map.
                 */
                sts = imm_copy_xtnt_descs( bfap,
                                           (vdIndexT) subXtntMap->mcellId.volume,
                                           &(bsXA[0]),
                                           1,
                                           FALSE,
                                           xtntMap,
                                           XTNT_ORIG_XTNTMAP);
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }
                subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
            }
            descEnd++;
        }
        
        /* Indicate that locking needs to be done if extending the
         * extent map.
         */
        sts = imm_copy_xtnt_descs( bfap,
                                   (vdIndexT) subXtntMap->mcellId.volume,
                                   &(subXtntMap->bsXA[descEnd]),
                                   (subXtntMap->cnt - 1) - descEnd,
                                   TRUE,
                                   xtntMap,
                                   XTNT_ORIG_XTNTMAP);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        
        xtntMap->updateEnd=xtntMap->cnt-1;

    }
        
    /*
     * Set each of the update sub extent map's "updateStart" and "updateEnd"
     * fields.
     */
    
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->updateStart]);
    subXtntMap->updateStart = descStart;
    subXtntMap->updateEnd = subXtntMap->cnt - 1;

    for (i = xtntMap->updateStart+1; i <= xtntMap->updateEnd; i++) {
        subXtntMap = &(xtntMap->subXtntMap[i]);
        subXtntMap->updateStart = 0;
        subXtntMap->updateEnd = subXtntMap->cnt - 1;
    }  /* end for */

    *rem_fob_offset = fob_offset;
    *rem_fob_cnt = (end_fob_offset + 1) - fob_offset;

    return sts;

HANDLE_EXCEPTION:

    xtntMap->updateEnd = xtntMap->cnt - 1;

    return sts;

}  /* end imm_remove_range_map*/

#ifdef ADVFS_SMP_ASSERT
/*
** Check the xtnt map for invalid configurattions.
** Each extent fob in a subextent map must be > the last extent fob.
** The starting fob of each subextent map must be the same as the ending
** fob of the previous subextent map. The starting fob of the first
** subextent map must be 0.
** Each subextent bssxmFobCnt is the difference between the first fob in the
** subextent map and the last fob in the subextent map.
** This routine is under ADVFS_SMP_ASSERT since it's only used in asserts.
*/

statusT
imm_check_xtnt_map(bsInMemXtntMapT *xtntMap)
{
    bsInMemSubXtntMapT *subXtntMap;
    bf_fob_t last_fob = 0;
    bf_fob_t cur_fob;
    bf_vd_blk_t vd_blk; /* Temporary holder for block value */
    vdT *vdp;
    int i;
    int j;
    int holeFlg;
    int permHole;


    MS_SMP_ASSERT(xtntMap->domain);

    MS_SMP_ASSERT(xtntMap->validCnt <= xtntMap->cnt);
    MS_SMP_ASSERT(xtntMap->cnt <= xtntMap->maxCnt);
    for ( i = 0; i < xtntMap->validCnt; i++ ) {
        subXtntMap = &xtntMap->subXtntMap[i];

        holeFlg = FALSE;
        permHole = FALSE;

        /* Check counts */
        MS_SMP_ASSERT(subXtntMap->cnt <= subXtntMap->maxCnt);
        MS_SMP_ASSERT(subXtntMap->maxCnt <= subXtntMap->onDiskMaxCnt);

        switch(subXtntMap->type) {
          case BSR_XTNTS:
            MS_SMP_ASSERT(subXtntMap->onDiskMaxCnt == BMT_XTNTS);
            /* only the first subextent map can be type BSR_XTNTS */
            MS_SMP_ASSERT(i == 0);
            break;
          case BSR_XTRA_XTNTS:
            MS_SMP_ASSERT(subXtntMap->onDiskMaxCnt == BMT_XTRA_XTNTS);
            MS_SMP_ASSERT(i != 0);
            /* no empty subextents allowed */

            MS_SMP_ASSERT(subXtntMap->cnt > 1);

            break;
          default:
            MS_SMP_ASSERT(0);
        }

        /* Newly created files come here with no extents and vdIndex == 0 */
        if ( subXtntMap->mcellId.volume == bsNilVdIndex ) {
            /* new files will have only one subextent map */
            MS_SMP_ASSERT(xtntMap->validCnt == 1);
            /* and it will be empty */
            MS_SMP_ASSERT(subXtntMap->cnt == 1);
            continue;
        }

        vdp = VD_HTOP(subXtntMap->mcellId.volume, xtntMap->domain);
        for ( j = 0; j < subXtntMap->cnt; j++ ) {
            cur_fob = subXtntMap->bsXA[j].bsx_fob_offset;
            if ( j == 0 ) {
                /* test bsxm_file_offset_blk */
                MS_SMP_ASSERT(subXtntMap->bssxmFobOffset == cur_fob );
                /* Each subextent starts where the last sub left off. */
                /* The first subextent starts at fob 0. */
                MS_SMP_ASSERT(cur_fob == last_fob);
            } else {
                /* Within a subextent the fobs increase monotonically. */
                MS_SMP_ASSERT(cur_fob > last_fob);
            }
            last_fob = cur_fob;

            /* Check the vd_blk */
            vd_blk = subXtntMap->bsXA[j].bsx_vd_blk;

            if ( vd_blk == XTNT_TERM ) {
                /* Can't have 2 holes in a row, or end a mcell with a hole. */
                MS_SMP_ASSERT(holeFlg == FALSE);
                holeFlg = TRUE;
                permHole = FALSE;
                continue;
            }
            holeFlg = FALSE;
            if ( vd_blk == COWED_HOLE ) {
                /* Can't have 2 permanent holes in a row, they combine. */
                MS_SMP_ASSERT(permHole == FALSE);
                permHole = TRUE;
                continue;
            }
            permHole = FALSE;

            /* We used to verify vd_blk alignment.  There is no good way to
             * do this anymore with variable allocation units and 1k fobs */
            /* As long as DEV_BSIZE > the 1k fob size, alignment shouldn't
             * be an issue */

            MS_SMP_ASSERT(vd_blk < vdp->vdSize);
        }

        MS_SMP_ASSERT(subXtntMap->bssxmFobCnt ==
                      last_fob - subXtntMap->bsXA[0].bsx_fob_offset);
    }

    return EOK;
}

#endif


#ifdef ADVFS_DEBUG
/*
 * imm_print_xtnt_map
 *
 * This function prints the specified in-mem extent map.
 */


void
imm_print_subXtnt_map( bsInMemSubXtntMapT *subXtntMap, int index )
{
    uint32_t j;
    char *type;

    switch (subXtntMap->type) {
        case BSR_XTRA_XTNTS:
            type = "BSR_XTRA_XTNTS";
            break;
        case BSR_XTNTS:
            type = "BSR_XTNTS";
            break;
        default:
            type = "UNKNOWN";
    }

    printf ("  subXtntMap %d\n", index);
    printf ("    bssxmFobOffset: %d, bssxmFobCnt: %d\n",
            subXtntMap->bssxmFobOffset, subXtntMap->bssxmFobCnt);
    printf ("    volume: %d, mcellId (page.cell): %ld.%d\n",
            subXtntMap->mcellId.volume, subXtntMap->mcellId.page,
            subXtntMap->mcellId.cell);
    printf ("    mcellState: %d, type: %s\n",
            subXtntMap->mcellState, type);
    printf ("    onDiskMaxCnt: %d, cnt: %d, maxCnt: %d\n",
            subXtntMap->onDiskMaxCnt, subXtntMap->cnt, subXtntMap->maxCnt);
    printf("    updateStart %d, updateEnd %d\n", subXtntMap->updateStart, subXtntMap->updateEnd);

    for (j = 0; j < subXtntMap->cnt; j++) {
        printf ("    bsx_fob_offset: %d (0x%08x), bsx_vd_blk: %d (0x%08x)\n",
                subXtntMap->bsXA[j].bsx_fob_offset, 
                subXtntMap->bsXA[j].bsx_fob_offset,
                subXtntMap->bsXA[j].bsx_vd_blk, 
                subXtntMap->bsXA[j].bsx_vd_blk);
    }

    return;
}

void
imm_print_xtnt_map (
                    bsInMemXtntMapT *xtntMap  /* in */
                    )
{
    uint32_t i;
    uint32_t j;
    bsInMemSubXtntMapT *subXtntMap;
    char *type;

    for (i = 0; i < xtntMap->validCnt; i++) {

        subXtntMap = &(xtntMap->subXtntMap[i]);

        switch (subXtntMap->type) {
            case BSR_XTRA_XTNTS:
                type = "BSR_XTRA_XTNTS";
                break;
            case BSR_XTNTS:
                type = "BSR_XTNTS";
                break;
            default:
                type = "UNKNOWN";
        }
        printf ("subXtntMap %d\n", i);
        printf ("    bssxmFobOffset: %d, bssxmFobCnt: %d\n",
                subXtntMap->bssxmFobOffset, subXtntMap->bssxmFobCnt);
        printf ("    volume: %d, mcellId (page.cell): %ld.%d\n",
                subXtntMap->mcellId.volume, subXtntMap->mcellId.page,
                subXtntMap->mcellId.cell);
        printf ("    mcellState: %d, type: %s\n",
                subXtntMap->mcellState, type);
        printf ("    onDiskMaxCnt: %d, cnt: %d, maxCnt: %d\n",
                subXtntMap->onDiskMaxCnt, subXtntMap->cnt, subXtntMap->maxCnt);

        for (j = 0; j < subXtntMap->cnt; j++) {
            printf ("    bsx_fob_offset: %d (0x%08x), bsx_vd_blk: %d (0x%08x)\n",
                    subXtntMap->bsXA[j].bsx_fob_offset,
                    subXtntMap->bsXA[j].bsx_fob_offset,
                    subXtntMap->bsXA[j].bsx_vd_blk,
                    subXtntMap->bsXA[j].bsx_vd_blk);
        }  /* end for */

    }  /* end for */

    return;

}  /* end imm_print_xtnt_map */
#endif /* ADVFS_DEBUG */


/*
 * Functions used by advfs_get_extent_map, which is called from fcntl(),
 * and vdump/vrestore.
 */


static
void
free_xtntmap(
    xtntMapDescT *xmd
    )
{
    xtntMapT *xm = NULL, *nextXm = NULL;


    if (xmd != NULL) {

        xm = xmd->nextMap;

        while (xm != NULL) {
            if (xm->xtnts != NULL) {
                ms_free(xm->xtnts);
            }

            nextXm = xm->nextMap;
            ms_free(xm);
            xm = nextXm;
        }

        ms_free(xmd);
    }
}



static
statusT
get_real_xtntmap( 
    struct bfAccess *bfap,
    xtntMapDescT **xmd,
    int xtnt_use
    )
{
    statusT status = EOK;
    xtntMapT *xm = NULL, *nextXm = NULL;
    vdIndexT volIndex;
    int xtntCnt;
    bf_fob_t fob_cnt;
    statusT sts;
    bsExtentDescT fakeXtnts;
    int error;

    /* All xtntMapDescT fields get initialized so no ms_malloc zeroing.*/
    *xmd = (xtntMapDescT *) ms_malloc_no_bzero( sizeof( xtntMapDescT ) );

    (*xmd)->fileLength = bfap->file_size;

    /* first get the xtntCnt so we can allocate
     *  the total number of extents for the merged extent map
     *  correct during the next call to advfs_get_xtnt_map
     */

    sts = advfs_get_xtnt_map(bfap, 
                                0, /* start_xtnt */ 
                                0, /* array size */
                                &fakeXtnts, /* no array */
                                &xtntCnt,   /* Num xtnts */
                                &fob_cnt,   /* bfaNextFob */
                                &volIndex, 
                                MPF_XTNT_CNT); /* flags */


    if (sts != EOK) {
        status = sts;
        goto __error;
    }

    /* All xtntMapT fields get initialized so no ms_malloc zeroing. */
    xm = (xtntMapT *) ms_malloc_no_bzero( sizeof( xtntMapT ) );

    (*xmd)->nextMap = xm;
    xm->nextMap = NULL;
    xm->xtntMapNum = 0;
    xm->curXtnt = 0;
    xm->xtnts = NULL;
    if (xtntCnt != 0) {
        xm->xtnts = (bsExtentDescT *) ms_malloc(xtntCnt * sizeof(bsExtentDescT));

        /* 
         * Check to make sure the maps have not grown since called above.
         * This is a busy-wait loop if the file is extending.
         */

#ifdef SNAPSHOTS
        /* THIS IS BROKEN.  Tru64 tried to deal with the file extending but
         * didn't do it correctly.  Deal with it later.
         */
#endif
        sts = advfs_get_xtnt_map(bfap, 
                                0, /* start_xtnt */ 
                                xtntCnt,    /* array size */
                                xm->xtnts,  /* no array */
                                &xtntCnt,   /* Num xtnts */
                                &fob_cnt,   /* bfaNextFob */
                                &volIndex, 
                                MPF_XTNTMAPS ); /* flags */


    } 

    /* 
     * Reset the xtntCnt to the actual xtntCnt, overwrite the estimate
     */
     
    xm->xtntCnt = xtntCnt;

    if (sts != EOK) {
        status = sts;
        goto __error;
    }

    return status;

__error:

    free_xtntmap(*xmd);
    *xmd = NULL;

    return status;
}


/*
 * This function is called by ioctl(), to retrieve the sparseness map
 * of an AdvFS file.
 *
 * Its intent is to return the LOGICAL extent map of a file, without
 * regard to the physical extent map of the file.  Multiple extents
 * will only be returned if the file in question is a sparse file.
 * All logically contiguous extents will be collapsed into a single
 * extent.
 */
int
advfs_get_extent_map (
                      struct bfAccess   *bfap,          /* in */
                      struct extentmap  *map            /* in/out */
                      )
{
    statusT     sts;
    bf_fob_t curr_fob= 0;
    bf_fob_t curr_fob_cnt = 0;

    xtntMapDescT         *xmd;
    bsExtentDescT        *xtnts;
    xtntMapT             *xm = NULL;
    xtntMapT             *nextXm = NULL;
    int                   curXtnt;
    int                   xtntCnt;
    unsigned long         arrayStart;

    struct extentmapentry kernel_side_extent; /*
                                               * local buffer for use with
                                               * copyout() to map->extent
                                               * which is user memory that
                                               * canot be accessed directly.
                                               */

/*************************************************************************/


    sts = get_real_xtntmap(bfap, &xmd, 1);
    if (xmd == NULL) {
        if (sts == EBADF) {
            return sts;
        } else {
            return EIO;
        }
    }

    if (xmd->nextMap == NULL) {
        free_xtntmap(xmd); 
        return EIO;
    }
    xtnts = xmd->nextMap->xtnts;
    curXtnt = xmd->nextMap->curXtnt;
    xtntCnt = xmd->nextMap->xtntCnt;

    if (map == NULL) {
        free_xtntmap(xmd); 
        return EIO;
    }
    map->numextents = 0;
    arrayStart = map->offset;

    /* curr_fob_cnt must equal curr_fob for determination of the first */
    /* extent and concatenation of adjacent extents.                 */
    curr_fob = 0;
    curr_fob_cnt = curr_fob;
    curXtnt = 0;

    while (curXtnt < xtntCnt) {
        if (xtnts[curXtnt].bsed_vd_blk == XTNT_TERM ||
	    xtnts[curXtnt].bsed_vd_blk == COWED_HOLE) {
            /* This extent is really a hole. */
        }
        else if (xtnts[curXtnt].bsed_fob_offset == curr_fob + curr_fob_cnt) {
	    /* This extent is adjacent to the previous extent */
            curr_fob_cnt += xtnts[curXtnt].bsed_fob_cnt;
        }
        else {
            if (curr_fob_cnt > 0) {
                if (arrayStart <= map->numextents &&
                    map->numextents - arrayStart < map->arraysize) {

                    (map->offset)++;

                    kernel_side_extent.offset = curr_fob * ADVFS_FOB_SZ;
                    kernel_side_extent.size   = curr_fob_cnt * ADVFS_FOB_SZ;

                    copyout(&kernel_side_extent,
                            &(map->extent[map->numextents - arrayStart]),
                            sizeof(struct extentmapentry));
                }
                (map->numextents)++;
            }
            curr_fob= xtnts[curXtnt].bsed_fob_offset;
            curr_fob_cnt = xtnts[curXtnt].bsed_fob_cnt;
        }
        curXtnt++;
    }


    if (xmd->fileLength > curr_fob + curr_fob_cnt) {
	/*
	 * There's a hole at the end of the file.  Don't do anything.
	 */
    }
    else {
	/*
	 * Truncate the current extent to the file length.
	 */
	curr_fob_cnt = xmd->fileLength - curr_fob;
    }

    if (curr_fob_cnt> 0) {
        if (arrayStart <= map->numextents &&
            map->numextents - arrayStart < map->arraysize) {

            (map->offset)++;

            kernel_side_extent.offset = curr_fob * ADVFS_FOB_SZ;
            kernel_side_extent.size   = curr_fob_cnt * ADVFS_FOB_SZ;

            copyout(&kernel_side_extent,
                    &(map->extent[map->numextents - arrayStart]),
                    sizeof(struct extentmapentry));
            
        }
        (map->numextents)++;
    }

    /*
     * Reset the offset if needed, simply to follow the functional spec
     */
    if (arrayStart == 0 && map->numextents == map->offset) {
	map->offset = 0;
    }

    free_xtntmap(xmd);
    return EOK;
}
/* end bs_inmem_map.c */
