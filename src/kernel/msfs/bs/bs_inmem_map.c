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
 *  MFS/bs
 *
 * Abstract:
 *
 *  bs_inmem_map.c
 *  This module contains routines related to in-memory extent maps.
 *
 *
 * Date:
 *
 *  Mon Feb 03 12:53:08 1992
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_inmem_map.c,v $ $Revision: 1.1.103.13 $ (DEC) $Date: 2008/02/15 10:09:11 $"

#include <sys/fcntl.h>
#include <sys/lock_probe.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_stg.h>
#include <msfs/bs_stripe.h>
#include <msfs/bs_bmt.h>

#define ADVFS_MODULE BS_INMEM_MAP

/*
 * Private protos
 */

static
uint32T
get_sub_xtnt_map_alloc_page_cnt (
                                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                                 uint32T blksPerPage,  /* in */
                                 uint32T bfPageOffset,   /* in */
                                 uint32T descStart,  /* in */
                                 uint32T bfEndPageOffset,  /* in */
                                 uint32T descEnd  /* in */
                                 );

static
statusT
merge_adjacent_xtnt_maps (
                          bsInMemXtntMapT *src1XtntMap,  /* in */
                          bsInMemXtntMapT *src2XtntMap,  /* in */
                          bsInMemXtntMapT **newXtntMap  /* out */
                          );

static
statusT
merge_nearby_xtnt_maps (
                        bsInMemXtntMapT *src1XtntMap,  /* in */
                        bsInMemXtntMapT *src2XtntMap,  /* in */
                        bsInMemXtntMapT **newXtntMap  /* out */
                        );

static
statusT
merge_intersect_xtnt_maps (
                           uint32T pageSize,  /* in */
                           bsInMemXtntMapT *src1XtntMap,  /* in */
                           bsInMemXtntMapT *src2XtntMap,  /* in */
                           bsInMemXtntMapT **newXtntMap,  /* out */
                           bsInMemXtntMapT **replaceXtntMap  /* out */
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
    unsigned long pageSize;             /* Page Size in bytes */
    struct xtntMap *nextMap;
} xtntMapDescT;

static
void
free_xtntmap(
             xtntMapDescT *xmd
             );

extern int CheckXtnts;

/*
 * imm_create_xtnt_map
 *
 * This function creates and initializes an in-memory extent map.
 */
statusT
imm_create_xtnt_map (
                     uint32T blksPerPage,  /* in */
                     domainT *domain,  /* in */
                     uint32T maxCnt,  /* in */
                     uint32T termPage,  /* in */
                     vdIndexT termVdIndex, /* in */
                     bsInMemXtntMapT **newXtntMap  /* out */
                     )
{
    statusT sts;
    bsInMemXtntMapT *xtntMap;

    xtntMap = (bsInMemXtntMapT *) ms_malloc (sizeof (bsInMemXtntMapT));
    MS_SMP_ASSERT( xtntMap != NULL );
    xtntMap->subXtntMap = NULL;
    sts = imm_init_xtnt_map( xtntMap,
                             blksPerPage,
                             domain,
                             maxCnt,
                             termPage,
                             termVdIndex);
    if (sts != EOK) {
        imm_delete_xtnt_map (xtntMap);
        return sts;
    }

    *newXtntMap = xtntMap;

    return EOK;
}  /* imm_create_xtnt_map */

/*
 * imm_init_xtnt_map
 *
 * This function initializes an in-memory extent map, deallocates the sub extent
 * map array, if any, and allocates a new array.  If "maxCnt" is 0, it creates
 * an array having a default number of entries.
 */
statusT
imm_init_xtnt_map (
                   bsInMemXtntMapT *xtntMap,  /* in */
                   uint32T blksPerPage,  /* in */
                   domainT *domain,  /* in */
                   uint32T maxCnt,  /* in */
                   uint32T termPage,  /* in */
                   vdIndexT termVdIndex /* in */
                   )
{
    uint32T i;
    bsInMemSubXtntMapT *subXtntMap;

    if (xtntMap->subXtntMap != NULL) {
        for (i = 0; i < xtntMap->cnt; i++) {
            if (xtntMap->subXtntMap[i].bsXA != NULL) {
                ms_free (xtntMap->subXtntMap[i].bsXA);
            }
        }
        ms_free (xtntMap->subXtntMap);
    }

    xtntMap->nextXtntMap = NULL;
    xtntMap->domain = domain;
    xtntMap->hdrType = 0;
    xtntMap->hdrVdIndex = bsNilVdIndex;
    xtntMap->hdrMcellId = bsNilMCId;
    xtntMap->blksPerPage = blksPerPage;
    xtntMap->nextValidPage = 0;
    xtntMap->allocDeallocPageCnt = 0;
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
    /* FIX - alloc from cache or setup cache */
    xtntMap->subXtntMap = (bsInMemSubXtntMapT *)
                        ms_malloc(xtntMap->maxCnt * sizeof(bsInMemSubXtntMapT));
    if (xtntMap->subXtntMap == NULL) {
        return ENO_MORE_MEMORY;
    }

    return EOK;
}  /* imm_init_xtnt_map */

/*
 * imm_extend_xtnt_map
 *
 * This function extends the in-memory extent map by extending the sub
 * extent map array.
 */
statusT
imm_extend_xtnt_map (
                     bsInMemXtntMapT *xtntMap
                     )
{
    uint32T i;
    bsInMemSubXtntMapT *newSubXtntMap;
    uint32T maxCnt;

    /*
     * We allocate the in-memory stuff first so that if we run out
     * of memory we don't have to undo anything.
     */

    maxCnt = xtntMap->maxCnt + 1;
    newSubXtntMap =
            (bsInMemSubXtntMapT*)ms_malloc(maxCnt * sizeof(bsInMemSubXtntMapT));
    if (newSubXtntMap == NULL) {
        return ENO_MORE_MEMORY;
    }

    for (i = 0; i < xtntMap->maxCnt; i++) {
        newSubXtntMap[i] = xtntMap->subXtntMap[i];
    }

    ms_free (xtntMap->subXtntMap);

    xtntMap->subXtntMap = newSubXtntMap;
    xtntMap->maxCnt = maxCnt;

    return EOK;
}  /* end imm_extend_xtnt_map */

/*
** Called from load_from_xtnt_rec or load_from_shadow_rec when the
** short mcellCnt from the disk was not large enough to count the
** mcells (subextents) for this file. Since the mcellCnt was not large
** enough, we know it contained the correct count modulo 64K.
** So we need to allocate 64K more subextents.
** We may be back later to allocate 64K more.
*/
statusT
imm_big_extend_xtnt_map( bsInMemXtntMapT *xtntMap )
{
    uint32T i;
    bsInMemSubXtntMapT *newSubXtntMap;
    uint32T maxCnt;

    /*
     * We allocate the in-memory stuff first so that if we run out
     * of memory we don't have to undo anything.
     */

    maxCnt = xtntMap->maxCnt + 0x10000;
    newSubXtntMap =
            (bsInMemSubXtntMapT*)ms_malloc(maxCnt * sizeof(bsInMemSubXtntMapT));
    /* ms_malloc waits. It never returns ENO_MORE_MEMORY */
    MS_SMP_ASSERT(newSubXtntMap != NULL);

    for (i = 0; i < xtntMap->maxCnt; i++) {
        newSubXtntMap[i] = xtntMap->subXtntMap[i];
    }

    ms_free (xtntMap->subXtntMap);

    xtntMap->subXtntMap = newSubXtntMap;
    xtntMap->maxCnt = maxCnt;

    return EOK;
}

/*
 * imm_get_first_xtnt_desc
 *
 * This function returns the specified extent map's first extent descriptor
 * and identifier.
 *
 * If the extent map is empty, the function sets "xtntDesc" page count,
 * page offset and disk index to 0 and block offset to -1.
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
    uint32T i;
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
            xtntDesc->pageOffset = subXtntMap->bsXA[0].bsPage;
            xtntDesc->pageCnt =
                              subXtntMap->bsXA[1].bsPage - xtntDesc->pageOffset;
            xtntDesc->volIndex = subXtntMap->vdIndex;
            xtntDesc->blkOffset = subXtntMap->bsXA[0].vdBlk;

            return;
        }
    }

    xtntDescId->subXtntMapIndex = xtntMap->validCnt;
    xtntDescId->xtntDescIndex = 0;
    xtntDesc->pageOffset = 0;
    xtntDesc->pageCnt = 0;
    xtntDesc->volIndex = bsNilVdIndex;
    xtntDesc->blkOffset = -1;

    return;

}  /* end imm_get_first_xtnt_desc */

/*
 * imm_get_xtnt_desc
 *
 * This function returns the extent descriptor and its id that maps the
 * specified page.  The extent descriptor is returned in "xtntDesc" and
 * its id is returned in "xtntDescId".
 *
 * If the page is not mapped by the extent map, this function sets "xtntDesc"
 * page count, page offset and disk index to 0 and block offset to -1.
 *
 * If the page is part of a permanent hole (vdBlk = -2) the page is "mapped".
 * Page count, page offset and disk index are set up. Block offset is set to -2.
 *
 * If updateFlg is set, only the update subextents will be searched.
 *
 * The caller must have read access to the extent map.
 */
void
imm_get_xtnt_desc (
                   uint32T pageOffset,  /* in */
                   bsInMemXtntMapT *xtntMap,  /* in */
                   int updateFlg,  /* in */
                   bsInMemXtntDescIdT *xtntDescId,  /* out */
                   bsXtntDescT *xtntDesc  /* out */
                   )
{
    bsInMemSubXtntMapT *subXtntMap;
    uint32T subXtntMapIndex;
    statusT sts;
    uint32T xtntDescIndex;

    sts = imm_page_to_sub_xtnt_map( pageOffset,
                                    xtntMap,
                                    updateFlg,
                                    &subXtntMapIndex);
    if ((sts == EOK) || (sts == E_PAGE_HOLE)) {
        subXtntMap = &(xtntMap->subXtntMap[subXtntMapIndex]);
        sts = imm_page_to_xtnt( pageOffset,
                                subXtntMap,
                                updateFlg,
                                FALSE,       /* perm hole returns E_PAGE_HOLE */
                                &xtntDescIndex);
        if ((sts == EOK) || (sts == E_PAGE_HOLE)) {
            xtntDescId->subXtntMapIndex = subXtntMapIndex;
            xtntDescId->xtntDescIndex = xtntDescIndex;
            xtntDesc->pageOffset = subXtntMap->bsXA[xtntDescIndex].bsPage;
            xtntDesc->pageCnt = subXtntMap->bsXA[xtntDescIndex + 1].bsPage -
              subXtntMap->bsXA[xtntDescIndex].bsPage;
            xtntDesc->volIndex = subXtntMap->vdIndex;
            xtntDesc->blkOffset = subXtntMap->bsXA[xtntDescIndex].vdBlk;

            return;
        }
    }

    /*
     * The page is not mapped.
     */

    xtntDescId->subXtntMapIndex = xtntMap->validCnt;
    xtntDescId->xtntDescIndex = 0;
    xtntDesc->pageOffset = 0;
    xtntDesc->pageCnt = 0;
    xtntDesc->volIndex = bsNilVdIndex;
    xtntDesc->blkOffset = -1;

    return;
}  /* end imm_get_xtnt_desc */

/*
 * imm_get_next_xtnt_desc
 *
 * This function returns the next extent descriptor and its id.  The previous
 * extent descriptor is specified by "xtntDescId".  The next extent descriptor
 * is returned in "xtntDesc" and its id is returned in "xtntDescId".
 *
 * If the previous extent descriptor's id is not valid or there are no more
 * extent descriptors, this function sets "xtntDesc" page count, page offset
 * and disk index to 0 and block offset to -1.
 *
 * Before calling this function, you must call imm_get_first_xtnt_desc() or
 * imm_get_xtnt_desc() to set the previous extent descriptor's id.
 *
 * The caller must have read access to the extent map.
 */
void
imm_get_next_xtnt_desc (
                        bsInMemXtntMapT *xtntMap,  /* in */
                        bsInMemXtntDescIdT *xtntDescId,  /* in/out */
                        bsXtntDescT *xtntDesc  /* out */
                        )
{
    bsXtntT *bsXA = NULL;
    uint32T i;
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
                }
            }
            if (bsXA != NULL) {
                xtntDesc->pageOffset = bsXA->bsPage;
                xtntDesc->pageCnt =
                  subXtntMap->bsXA[xtntDescId->xtntDescIndex + 1].bsPage -
                    bsXA->bsPage;
                xtntDesc->volIndex = subXtntMap->vdIndex;
                xtntDesc->blkOffset = bsXA->vdBlk;

                return;
            }
        }
    }

    /*
     * There is no next extent descriptor either because the previous extent
     * descriptor is the last one or the previous extent descriptor's id was bad.
     */

    xtntDescId->subXtntMapIndex = xtntMap->validCnt;
    xtntDescId->xtntDescIndex = 0;
    xtntDesc->pageOffset = 0;
    xtntDesc->pageCnt = 0;
    xtntDesc->volIndex = bsNilVdIndex;
    xtntDesc->blkOffset = -1;

    return;
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
    uint32T i;

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
    uint32T i;

    if (xtntMap->subXtntMap != NULL) {
        for (i = 0; i < xtntMap->cnt; i++) {
            if (xtntMap->subXtntMap[i].bsXA != NULL) {
                ms_free (xtntMap->subXtntMap[i].bsXA);
            }
        }
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
 */
statusT
imm_init_sub_xtnt_map (
                       bsInMemSubXtntMapT *subXtntMap,  /* in */
                       uint32T pageOffset,  /* in */
                       uint32T pageCnt,  /* in */
                       vdIndexT vdIndex,  /* in */
                       bfMCIdT mcellId,  /* in */
                       uint32T type,  /* in */
                       uint32T onDiskMaxCnt,  /* in */
                       uint32T maxCnt  /* in */
                       )
{
    subXtntMap->pageOffset = pageOffset;
    subXtntMap->pageCnt = pageCnt;
    subXtntMap->vdIndex = vdIndex;
    subXtntMap->mcellId = mcellId;
    subXtntMap->mcellState = INITIALIZED;
    subXtntMap->type = type;
    subXtntMap->onDiskMaxCnt = onDiskMaxCnt;
    subXtntMap->updateStart = 1;
    subXtntMap->updateEnd = 0;
    subXtntMap->cnt = 1;

    switch ( type ) {
      case BSR_XTNTS:
        MS_SMP_ASSERT(onDiskMaxCnt == BMT_XTNTS);
        if ( maxCnt ) {
            /* The first part of this ASSERTion will fail on the
             * miscellaneous bitfile, which, as an exception, has 3
             * bsXA records in its mcell.  The second half of the
             * test checks for this. Note that this test will allow
             * a normal file in a Version 4 domain whose primary mcell
             * is in BMT page 0 mcell 5 to slip thru this test disguised
             * as a miscellaneous bitfile, but that is better than
             * removing this check altogether.
             */
            MS_SMP_ASSERT((maxCnt <= onDiskMaxCnt) ||
                          ( subXtntMap->mcellId.page == 0         &&
                            subXtntMap->mcellId.cell == BFM_MISC  &&
                            maxCnt  == 3                          &&
                            onDiskMaxCnt == 2
                          ) );
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
      case BSR_SHADOW_XTNTS:
        MS_SMP_ASSERT(onDiskMaxCnt == BMT_SHADOW_XTNTS);
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
    if (subXtntMap->bsXA == NULL) {
        return ENO_MORE_MEMORY;
    }
    subXtntMap->bsXA[0].bsPage = pageOffset;
    subXtntMap->bsXA[0].vdBlk = XTNT_TERM;

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
    uint32T i;
    statusT sts;

    if (oldSubXtntMap->bsXA == NULL) {
        sts = imm_load_sub_xtnt_map( bfap, xtntMap, oldSubXtntMap);
        if (sts != EOK) {
            return sts;
        }
    }

    sts = imm_init_sub_xtnt_map (
                                 newSubXtntMap,
                                 oldSubXtntMap->pageOffset,
                                 oldSubXtntMap->pageCnt,
                                 oldSubXtntMap->vdIndex,
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
    }
    newSubXtntMap->cnt = oldSubXtntMap->cnt;

    newSubXtntMap->updateStart = newSubXtntMap->cnt;

    return EOK;
}  /* end imm_copy_sub_xtnt_map */

/*
 * imm_extend_sub_xtnt_map
 *
 * This function extends the in-memory sub extent map by extending the
 * extent array.
 */
statusT
imm_extend_sub_xtnt_map (
                         bsInMemSubXtntMapT *subXtntMap  /* in */
                         )
{
    uint32T i;
    bsXtntT *newBsXA;
    uint32T newMaxCnt;

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

    /* FIX - get from cache */

    newBsXA = (bsXtntT *) ms_malloc (newMaxCnt * sizeof (bsXtntT));
    if (newBsXA == NULL) {
        return ENO_MORE_MEMORY;
    }

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
statusT
imm_load_sub_xtnt_map (
                       bfAccessT *bfap,                  /* in */
                       bsInMemXtntMapT *xtntMap,         /* in */
                       bsInMemSubXtntMapT *subXtntMap    /* in */
                       )
{
    bsMPgT *bmt;
    bsXtntT *bsXA;
    uint32T bsXACnt;
    int derefFlag = 0;
    uint32T i;
    bsMCT *mcell;
    bfMCIdT mcellId;
    bfPageRefHT pgRef;
    bsShadowXtntT *shadowRec;
    statusT sts;
    vdT *vd;
    vdIndexT vdIndex;
    bsXtntRT *xtntRec;
    bsXtraXtntRT *xtraXtntRec;

    if (subXtntMap->bsXA != NULL) {
        /*
         * Already in cache.
         */
        return EOK;
    }

    vdIndex = subXtntMap->vdIndex;
    mcellId = subXtntMap->mcellId;

    vd = VD_HTOP(vdIndex, xtntMap->domain);

    MS_SMP_ASSERT(TEST_PAGE(mcellId.page, vd->bmtp) == EOK);
    sts = bmt_refpg( &pgRef,
                     (void*)&bmt,
                     vd->bmtp,
                     mcellId.page,
                     BS_NIL);
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

        bsXACnt = xtntRec->firstXtnt.xCnt;
        bsXA = xtntRec->firstXtnt.bsXA;

        break;

      case BSR_SHADOW_XTNTS:

        shadowRec = bmtr_find( mcell, BSR_SHADOW_XTNTS, vd->dmnP);
        if (shadowRec == NULL) {
            RAISE_EXCEPTION (ENO_XTNTS);
        }
        if ( CheckXtnts &&
             check_BSR_SHADOW_XTNTS_rec(shadowRec, bfap, vd) != EOK )
        {
            RAISE_EXCEPTION(E_BAD_BMT);
        }

        bsXACnt = shadowRec->xCnt;
        bsXA = shadowRec->bsXA;

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

    subXtntMap->bsXA = (bsXtntT *) ms_malloc (bsXACnt * sizeof (bsXtntT));
    if (subXtntMap->bsXA == NULL) {
        RAISE_EXCEPTION (ENO_MORE_MEMORY);
    }
    subXtntMap->cnt = bsXACnt;
    subXtntMap->updateStart = subXtntMap->cnt;
    subXtntMap->maxCnt = subXtntMap->cnt;

    for (i = 0; i < bsXACnt; i++) {
        subXtntMap->bsXA[i] = bsXA[i];
    }

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
imm_copy_xtnt_descs (
                     vdIndexT copyVdIndex,  /* in */
                     bsXtntT *copyBsXA,  /* in */
                     uint32T copyCnt,  /* in */
                     int copyHoleFlag,  /* in */
                     bsInMemXtntMapT *xtntMap,    /* in */
                     bsXtntMapTypeT mapType /* in */
                     )
{
    uint32T cnt;
    uint32T dst;
    int initFlag = 0;
    int needCnt;
    bsInMemSubXtntMapT *newSubXtntMap;
    uint32T src;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T newtype,
            newmax;

    if (copyCnt == 0) {
        return EOK;
    }

    if ( (copyCnt == 1) &&
         (copyBsXA[0].vdBlk == XTNT_TERM ||
          copyBsXA[0].vdBlk == PERM_HOLE_START) &&
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
         * Note that the next sub extent map's starting page offset is
         * 1 greater than the previous sub extent map's last page offset.
         */
        subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
        if (copyVdIndex != subXtntMap->vdIndex) {
            if (xtntMap->cnt >= xtntMap->maxCnt) {
                sts = imm_extend_xtnt_map (xtntMap);
                if (sts != EOK) {
                    return sts;
                }
                subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
            }
            newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt]);
            sts = imm_init_sub_xtnt_map (
                                         newSubXtntMap,
                                   subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage,
                                         0,  /* pageCnt */
                                         copyVdIndex,
                                         bsNilMCId,
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
            sts = imm_extend_xtnt_map (xtntMap);
            if (sts != EOK) {
                return sts;
            }
        }
        subXtntMap = &(xtntMap->subXtntMap[0]);

        /*
         * If this file can have extent information in the primary mcell,
         * make the first subextent map type BSR_XTNTS.  Otherwise, make
         * it BSR_SHADOW_XTNTS.
         */
        if (FIRST_XTNT_IN_PRIM_MCELL(xtntMap->domain->dmnVersion, mapType)) {
            newtype = BSR_XTNTS;
            newmax = BMT_XTNTS;
        } else {
            newtype = BSR_SHADOW_XTNTS;
            newmax = BMT_SHADOW_XTNTS;
        }
        sts = imm_init_sub_xtnt_map (
                                     subXtntMap,
                                     0,  /* pageOffset */
                                     0,  /* pageCnt */
                                     copyVdIndex,
                                     bsNilMCId,
                                     newtype,
                                     newmax,
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

        if ( (copyBsXA[src].vdBlk != XTNT_TERM &&
              copyBsXA[src].vdBlk != PERM_HOLE_START) ||
             (copyHoleFlag != 0) )
        {
            /*
             * Determine the number of new entries needed.
             */
            if (copyBsXA[src].bsPage == subXtntMap->bsXA[dst].bsPage) {
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
                if ( (sts == EXTND_FAILURE) ||
                     ((sts == EOK) && (needCnt > (subXtntMap->maxCnt - dst))) )
                {

                    /*
                     * Tie off the current sub extent map.
                     */

                    /* We need to be careful here not to end the subxtnt map
                     * with a hole. Check the next to last descriptor and if it
                     * is a hole move it to the beginning of the newly obtained
                     * descriptor. NOTE we DO allow * subxtnts to end with a
                     * permenant hole.
                     */

                    if (dst > 0 && subXtntMap->bsXA[dst-1].vdBlk == XTNT_TERM) {
                        /* The hole now becomes the termination descriptor */
                        dst--;
                        needCnt++;
                    }

                    subXtntMap->pageCnt = subXtntMap->bsXA[dst].bsPage -
                      subXtntMap->pageOffset;

                    subXtntMap->cnt = dst + 1;

                    /*
                     * No more room in the sub extent map's extent array.
                     * Overflow into another sub extent map.
                     *
                     * Note that the next sub extent map's starting page offset
                     * is 1 greater than the previous sub extent map's last
                     * page offset.
                     */
                    if (xtntMap->cnt >= xtntMap->maxCnt) {
                        sts = imm_extend_xtnt_map (xtntMap);
                        if (sts != EOK) {
                            return sts;
                        }
                        subXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt - 1]);
                    }
                    newSubXtntMap = &(xtntMap->subXtntMap[xtntMap->cnt]);
                    sts = imm_init_sub_xtnt_map (
                                                 newSubXtntMap,
                                                 subXtntMap->bsXA[dst].bsPage,
                                                 0,  /* pageCnt */
                                                 subXtntMap->vdIndex,
                                                 bsNilMCId,
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

            if (copyBsXA[src].bsPage != subXtntMap->bsXA[dst].bsPage) {
                /*
                 * The termination extent descriptor becomes a hole descriptor.
                 */
                dst++;
            }
            subXtntMap->bsXA[dst] = copyBsXA[src];
            dst++;
            subXtntMap->bsXA[dst].bsPage = copyBsXA[src + 1].bsPage;
            subXtntMap->bsXA[dst].vdBlk = XTNT_TERM;

            MS_SMP_ASSERT((int) dst >= 0);
            MS_SMP_ASSERT(dst < subXtntMap->maxCnt);
        }
        src++;
        cnt++;
    }  /* end while */

    subXtntMap->pageCnt = subXtntMap->bsXA[dst].bsPage - subXtntMap->pageOffset;

    subXtntMap->cnt = dst + 1;

    return EOK;
}  /* end imm_copy_xtnt_descs */

/*
 * imm_split_desc
 *
 * This function splits the specified descriptor into two descriptors at
 * the specified page offset.
 */
void
imm_split_desc (
                uint32T pageOffset,  /* in */
                uint32T pageSize,  /* in */
                bsXtntT *srcDesc,  /* in */
                bsXtntT *part1Desc,  /* in/modified */
                bsXtntT *part2Desc  /* in/modified */
                )
{
    *part1Desc = *srcDesc;
    part2Desc->bsPage = pageOffset;
    if ( srcDesc->vdBlk == XTNT_TERM || srcDesc->vdBlk == PERM_HOLE_START) {
        part2Desc->vdBlk = srcDesc->vdBlk;
    } else {
        part2Desc->vdBlk = srcDesc->vdBlk +
          ((part2Desc->bsPage - srcDesc->bsPage) * pageSize);
    }

    return;
}  /* end imm_split_desc */

/*
 * imm_get_alloc_page_cnt
 *
 * This function calculates the number of allocated pages described by
 * the extent map's sub extent maps.
 */
statusT
imm_get_alloc_page_cnt (
                        bsInMemXtntMapT *xtntMap,  /* in */
                        uint32T bfPageOffset,  /* in */
                        uint32T bfPageCnt,  /* in */
                        uint32T *allocPageCnt  /* out */
                        )
{
    bsXtntT *bsXA;
    uint32T cnt;
    uint32T descEnd;
    uint32T descStart;
    uint32T endPageOffset;
    uint32T i;
    uint32T mapEnd;
    uint32T mapStart;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T totalCnt = 0;

    if (bfPageCnt == 0) {
        *allocPageCnt = 0;
        return EOK;
    }

    endPageOffset = (bfPageOffset + bfPageCnt) - 1;

    /*
     * Find the sub extent map and the entry that maps the ending
     * page offset.
     */

    sts = imm_page_to_sub_xtnt_map (endPageOffset, xtntMap, 0, &mapEnd);
    if (sts != EOK) {
        return sts;
    }

    sts = imm_page_to_xtnt( endPageOffset,
                            &(xtntMap->subXtntMap[mapEnd]),
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_PAGE_HOLE */
                            &descEnd);
    if ((sts != EOK) && (sts != E_PAGE_HOLE)) {
        return sts;
    }

    /*
     * Find the sub extent map and the entry that maps the starting
     * page offset.
     */

    sts = imm_page_to_sub_xtnt_map (bfPageOffset, xtntMap, 0, &mapStart);
    if (sts != EOK) {
        return sts;
    }

    sts = imm_page_to_xtnt( bfPageOffset,
                            &(xtntMap->subXtntMap[mapStart]),
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_PAGE_HOLE */
                            &descStart);
    if ((sts != EOK) && (sts != E_PAGE_HOLE)) {
        return sts;
    }

    /*
     * Count the allocated pages.
     */

    if (mapEnd == mapStart) {
        totalCnt = get_sub_xtnt_map_alloc_page_cnt (
                                               &(xtntMap->subXtntMap[mapStart]),
                                                    xtntMap->blksPerPage,
                                                    bfPageOffset,
                                                    descStart,
                                                    endPageOffset,
                                                    descEnd
                                                    );
    } else {
        /*
         * Count the allocated pages in the first sub extent map from the
         * first page to the last page described by the map.
         */
        subXtntMap = &(xtntMap->subXtntMap[mapStart]);
        bsXA = subXtntMap->bsXA;
        cnt = get_sub_xtnt_map_alloc_page_cnt (
                                               subXtntMap,
                                               xtntMap->blksPerPage,
                                               bfPageOffset,
                                               descStart,
                                             bsXA[subXtntMap->cnt-1].bsPage - 1,
                                               subXtntMap->cnt - 2
                                               );
        totalCnt = totalCnt + cnt;
        mapStart++;

        /*
         * Count the allocated pages in the last sub extent map from the
         * first page described by the map to the last page.
         */
        subXtntMap = &(xtntMap->subXtntMap[mapEnd]);
        bsXA = subXtntMap->bsXA;
        cnt = get_sub_xtnt_map_alloc_page_cnt (
                                               subXtntMap,
                                               xtntMap->blksPerPage,
                                               bsXA[0].bsPage,
                                               0,
                                               endPageOffset,
                                               descEnd
                                               );
        totalCnt = totalCnt + cnt;
        mapEnd--;

        /*
         * Count the allocated pages in the sub extent maps between the
         * first and the last map.
         */
        for (i = mapStart; i <= mapEnd; i++) {
            subXtntMap = &(xtntMap->subXtntMap[i]);
            if (subXtntMap->cnt > 1) {
                bsXA = subXtntMap->bsXA;
                cnt = get_sub_xtnt_map_alloc_page_cnt (
                                                       subXtntMap,
                                                       xtntMap->blksPerPage,
                                                       bsXA[0].bsPage,
                                                       0,
                                               bsXA[subXtntMap->cnt-1].bsPage-1,
                                                       subXtntMap->cnt - 2
                                                       );
                totalCnt = totalCnt + cnt;
            }
        }
    }

    *allocPageCnt = totalCnt;
    return EOK;

}  /* end imm_get_alloc_page_cnt */

/*
 * get_sub_xtnt_map_alloc_page_cnt
 *
 * This function calculates the number of allocated pages described by
 * the sub extent map's extent array.
 */
static
uint32T
get_sub_xtnt_map_alloc_page_cnt (
                                 bsInMemSubXtntMapT *subXtntMap,  /* in */
                                 uint32T blksPerPage,  /* in */
                                 uint32T bfPageOffset,   /* in */
                                 uint32T descStart,  /* in */
                                 uint32T bfEndPageOffset,  /* in */
                                 uint32T descEnd  /* in */
                                 )
{
    uint32T cnt;
    bsXtntT firstPart;
    uint32T i;
    bsXtntT lastPart;

    if (descEnd == descStart) {
        if ( subXtntMap->bsXA[descStart].vdBlk != XTNT_TERM &&
             subXtntMap->bsXA[descStart].vdBlk != PERM_HOLE_START )
        {
            cnt = (bfEndPageOffset + 1) - bfPageOffset;
        } else {
            cnt = 0;
        }
    } else {

        cnt = 0;
        if (bfPageOffset != subXtntMap->bsXA[descStart].bsPage) {
            /*
             * The first page is not aligned on an extent boundary.
             * Only count the pages from the first page to the end
             * of the extent.
             */
            if ( subXtntMap->bsXA[descStart].vdBlk != XTNT_TERM &&
                 subXtntMap->bsXA[descStart].vdBlk != PERM_HOLE_START )
            {
                imm_split_desc( bfPageOffset,
                                blksPerPage,
                                &(subXtntMap->bsXA[descStart]),
                                &firstPart,
                                &lastPart);
                cnt = cnt + (subXtntMap->bsXA[descStart + 1].bsPage - lastPart.bsPage);
            }
            descStart++;
        }

        if (bfEndPageOffset != (subXtntMap->bsXA[descEnd + 1].bsPage - 1)) {
            /*
             * The last page is not aligned on an extent boundary.
             * Only count the pages from the start of the extent to
             * the last page.
             */
            if ( subXtntMap->bsXA[descEnd].vdBlk != XTNT_TERM &&
                 subXtntMap->bsXA[descEnd].vdBlk != PERM_HOLE_START )
            {
                imm_split_desc( bfEndPageOffset + 1,
                                blksPerPage,
                                &(subXtntMap->bsXA[descEnd]),
                                &firstPart,
                                &lastPart);
                cnt = cnt + (lastPart.bsPage - subXtntMap->bsXA[descEnd].bsPage);
            }
            descEnd--;
        }

        for (i = descStart; i <= descEnd; i++) {
            if ( subXtntMap->bsXA[i].vdBlk != XTNT_TERM &&
                 subXtntMap->bsXA[i].vdBlk != PERM_HOLE_START )
            {
                cnt = cnt +
                  (subXtntMap->bsXA[i + 1].bsPage - subXtntMap->bsXA[i].bsPage);
            }
        }
    }

    return cnt;
}  /* end get_sub_xtnt_map_alloc_page_cnt */

/*
 * imm_get_next_page
 *
 * This function returns the next page offset after the bitfile's last page.
 */
uint32T
imm_get_next_page (
                   bsInMemXtntT *xtnts  /* in */
                   )
{
    uint32T mapIndex;
    uint32T maxNextPage;
    uint32T nextPage;
    uint32T pageOffset;
    bsInMemSubXtntMapT *subXtntMap;
    bsInMemXtntMapT *xtntMap;

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        subXtntMap = &xtnts->xtntMap->subXtntMap[xtnts->xtntMap->validCnt - 1];
        nextPage = subXtntMap->pageOffset + subXtntMap->pageCnt;
        break;

      case BSXMT_STRIPE:

        maxNextPage = 0;
        for (mapIndex = 0; mapIndex < xtnts->stripeXtntMap->cnt; mapIndex++) {
            xtntMap = xtnts->stripeXtntMap->xtntMap[mapIndex];
            subXtntMap = &(xtntMap->subXtntMap[xtntMap->validCnt - 1]);
            pageOffset = subXtntMap->pageOffset + subXtntMap->pageCnt;
            if (pageOffset > 0) {
                nextPage = XMPAGE_TO_BFPAGE( mapIndex,
                                             pageOffset - 1,
                                             xtnts->stripeXtntMap->cnt,
                                             xtnts->stripeXtntMap->segmentSize);
                nextPage++;
                if (nextPage > maxNextPage) {
                    maxNextPage = nextPage;
                }
            }
        }
        nextPage = maxNextPage;
        break;

      default:
        ADVFS_SAD0 ("imm_get_next_page --- bad extent map type");

    }  /* end switch */

    return nextPage;
}  /* end imm_get_next_page */

/*
 * imm_get_first_hole
 *
 * This function finds the bitfile's first hole and returns the starting
 * page offset and count.  If there is no hole, a page offset and page
 * count of 0 are returned.
 */
void
imm_get_first_hole (
                    bsInMemXtntT *xtnts,  /* in */
                    uint32T *retPageOffset,  /* out */
                    uint32T *retPageCnt  /* out */
                    )
{
    bsXtntDescT xtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    bsInMemXtntMapT *xtntMap;

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        xtntMap = xtnts->xtntMap;

        imm_get_first_xtnt_desc (xtntMap, &xtntDescId, &xtntDesc);
        while ( xtntDesc.blkOffset != XTNT_TERM &&
                xtntDesc.blkOffset != PERM_HOLE_START )
        {
            imm_get_next_xtnt_desc (xtntMap, &xtntDescId, &xtntDesc);
        }

        *retPageOffset = xtntDesc.pageOffset;
        *retPageCnt = xtntDesc.pageCnt;

        break;

      case BSXMT_STRIPE:
        /* not yet supported.  fall thru to default. */
      default:
        ADVFS_SAD0 ("imm_get_first_hole --- bad extent map type");

    }  /* end switch */

    return;
}  /* end imm_get_first_hole */


/*
 * imm_get_hole_size
 *
 * This function returns the size of the hole that starts at the specified
 * page offset.
 */
uint32T
imm_get_hole_size (
                   uint32T pageOffset,  /* in */
                   bsInMemXtntT *xtnts  /* in */
                   )
{
    uint32T bfPageOffset;
    uint32T cnt;
    uint32T descIndex;
    uint32T lowBfPageOffset;
    uint32T i;
    uint32T mapIndex;
    uint32T nextSegBfPageOffset;
    uint32T segmentCnt;
    uint32T segmentSize;
    statusT sts;
    uint32T subMapIndex;
    bsInMemSubXtntMapT *subXtntMap;
    uint32T xmPageOffset;
    bsInMemXtntMapT *xtntMap;

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        xtntMap = xtnts->xtntMap;
        sts = imm_page_to_sub_xtnt_map (pageOffset, xtntMap, 0, &subMapIndex);
        if (sts == EOK) {
            subXtntMap = &(xtntMap->subXtntMap[subMapIndex]);
            sts = imm_page_to_xtnt( pageOffset,
                                    subXtntMap,
                                    FALSE,   /* Don't use the update maps */
                                    FALSE,   /* perm hole returns E_PAGE_HOLE */
                                    &descIndex);
            if (sts == E_PAGE_HOLE) {
                /*
                 * The page offset is mapped by a sub extent map and the
                 * extent descriptor is a hole descriptor.
                 */
                cnt = subXtntMap->bsXA[descIndex + 1].bsPage - pageOffset;
            } else {
                if (sts == EOK) {
                    /*
                     * The page is mapped and allocated.
                     */
                    cnt = 0;
                } else {
                    return 0;  /* FIX - masks errors.  is this ok?? */
                }
            }
        } else {
            if (sts == E_PAGE_HOLE) {
                /*
                 * The page offset is not mapped by any sub extent map and
                 * is positioned between to sub extent maps.
                 */
                cnt = xtntMap->subXtntMap[subMapIndex + 1].pageOffset - pageOffset;
            } else {
                return 0;  /* FIX - masks errors.  is this ok?? */
            }
        }
        break;

      case BSXMT_STRIPE:

        segmentCnt = xtnts->stripeXtntMap->cnt;
        segmentSize = xtnts->stripeXtntMap->segmentSize;

        /*
         * Search for the first page that has storage allocated to it.  This
         * is the end of hole page.  We set the initial end of hole page offset
         * to the maximum value.  Because pages between the hole's starting and
         * the ending offsets are mapped by other extent maps, we must check the
         * other maps for an earlier end of hole page.  The final end of hole
         * page is the smallest page offset found in all the maps.
         */

        lowBfPageOffset = -1;  /* largest unsigned value */
        nextSegBfPageOffset = pageOffset;
        mapIndex = BFPAGE_TO_MAP (nextSegBfPageOffset, segmentCnt, segmentSize);

        for (i = 0; i < segmentCnt; i++) {

            xmPageOffset = BFPAGE_TO_XMPAGE( nextSegBfPageOffset,
                                             segmentCnt,
                                             segmentSize);
            xtntMap = xtnts->stripeXtntMap->xtntMap[mapIndex];

            sts = imm_page_to_sub_xtnt_map( xmPageOffset,
                                            xtntMap,
                                            0,
                                            &subMapIndex);
            if ((sts == EOK) || (sts == E_PAGE_HOLE)) {
                if (sts == EOK) {
                    subXtntMap = &(xtntMap->subXtntMap[subMapIndex]);
                    sts = imm_page_to_xtnt( xmPageOffset,
                                            subXtntMap,
                                            FALSE, /* Don't use update maps */
                                            FALSE, /* holes ret E_PAGE_HOLE */
                                            &descIndex);
                    if (sts == E_PAGE_HOLE) {
                        /*
                         * The page is mapped by a hole descriptor.
                         */
                        xmPageOffset = subXtntMap->bsXA[descIndex + 1].bsPage;
                        bfPageOffset = XMPAGE_TO_BFPAGE( mapIndex,
                                                         xmPageOffset,
                                                         segmentCnt,
                                                         segmentSize);
                    } else {
                        if (sts == EOK) {
                            /*
                             * The page is mapped and allocated.
                             */
                            bfPageOffset = nextSegBfPageOffset;
                        } else {
                            /*
                             * Unexpected status.
                             */
                            return 0;  /* FIX - masks errors.  is this ok?? */
                        }
                    }
                } else {
                    /*
                     * The page is not mapped by the sub extent map but is
                     * between this sub extent map and the next one.
                     */
                    xmPageOffset =
                                xtntMap->subXtntMap[subMapIndex + 1].pageOffset;
                    bfPageOffset = XMPAGE_TO_BFPAGE( mapIndex,
                                                     xmPageOffset,
                                                     segmentCnt,
                                                     segmentSize);
                }
                nextSegBfPageOffset = NEXT_SEGMENT_PAGE( nextSegBfPageOffset,
                                                         segmentSize);
                /*
                 * "bfPageOffset" is the next allocated bitfile page as known
                 * by the current extent map.
                 */
                if (bfPageOffset < lowBfPageOffset) {
                    lowBfPageOffset = bfPageOffset;
                    if (lowBfPageOffset < nextSegBfPageOffset) {
                        /*
                         * The hole does not extend into the next segment.  So,
                         * the current segment contains the end of hole.
                         */
                        break;  /* out of loop */
                    }
                }
            } else {
                if (sts == E_PAGE_NOT_MAPPED) {
                    /*
                     * Skip map.  No storage has been allocated beyond this page
                     * offset in this map.  So, the lack of any page map is
                     * treated as a hole.  The hole extends to whatever the
                     * current end of hole is.
                     */
                    nextSegBfPageOffset =
                      NEXT_SEGMENT_PAGE (nextSegBfPageOffset, segmentSize);
                } else {
                    /*
                     * Unexpected status.
                     */
                    return 0;  /* FIX - masks errors.  is this ok?? */
                }
            }
            mapIndex = NEXT_MAP (mapIndex, segmentCnt);
        }

        cnt = lowBfPageOffset - pageOffset;

        break;

      default:
        ADVFS_SAD0 ("imm_get_hole_size --- bad extent map type");

    }  /* end switch */

    return cnt;
}  /* end imm_get_hole_size */

/*
 * imm_get_page_type
 *
 * This function returns the type of the specified page.
 */
statusT
imm_get_page_type (
                   bsInMemXtntT *xtnts,  /* in */
                   uint32T pageOffset  /* in */
                   )
{
    uint32T descIndex;
    uint32T mapIndex;
    statusT sts;
    uint32T xmPageOffset;
    bsInMemXtntMapT *xtntMap;

    switch (xtnts->type)  {

      case BSXMT_APPEND:

        xtntMap = xtnts->xtntMap;
        sts = imm_page_to_sub_xtnt_map (pageOffset, xtntMap, 0, &mapIndex);
        if (sts == EOK) {
            sts = imm_page_to_xtnt( pageOffset,
                                    &(xtntMap->subXtntMap[mapIndex]),
                                    FALSE,   /* Don't use update extents */
                                    FALSE,   /* perm hole returns E_PAGE_HOLE */
                                    &descIndex);
        }
        break;

      case BSXMT_STRIPE:

        xmPageOffset = BFPAGE_TO_XMPAGE( pageOffset,
                                         xtnts->stripeXtntMap->cnt,
                                         xtnts->stripeXtntMap->segmentSize);

        mapIndex = BFPAGE_TO_MAP( pageOffset,
                                  xtnts->stripeXtntMap->cnt,
                                  xtnts->stripeXtntMap->segmentSize);
        xtntMap = xtnts->stripeXtntMap->xtntMap[mapIndex];

        sts = imm_page_to_sub_xtnt_map (xmPageOffset, xtntMap, 0, &mapIndex);
        if (sts == EOK) {
            sts = imm_page_to_xtnt( xmPageOffset,
                                    &(xtntMap->subXtntMap[mapIndex]),
                                    FALSE,   /* Don't use update extents */
                                    FALSE,   /* perm hole returns E_PAGE_HOLE */
                                    &descIndex);
        }
        break;

      default:
        ADVFS_SAD0 ("imm_get_page_type --- bad extent map type");

    }  /* end switch */

    return sts;
}  /* end imm_get_page_type */

/*
 * imm_set_next_valid_copy_page
 *
 * This function sets the "nextValidPage" for the appropriate copy extent maps.
 * It does not verify that the page is mapped by any extent map.
 */
void
imm_set_next_valid_copy_page (
                              bsInMemXtntT *xtnts,  /* in */
                              uint32T bfPageOffset  /* in */
                              )
{
    uint32T mapIndex;
    bsInMemXtntMapT *xtntMap;

    switch (xtnts->type)  {

      case BSXMT_APPEND:
      case BSXMT_STRIPE:

        xtnts->copyXtntMap->nextValidPage = bfPageOffset;

        break;

      default:
        ADVFS_SAD0("imm_set_next_valid_copy_page --- bad extent map type");

    }  /* end switch */

    return;
}  /* end imm_set_next_valid_copy_page */

/*
 * imm_page_to_sub_xtnt_map
 *
 * This function searches an in-memory extent map for the sub extent map that
 * maps the specified page and returns the sub extent map index.  If the page
 * in not mapped but its position is between two sub extent maps, it returns
 * the preceeding sub extent map's index.
 * If updateFlg is set this function only looks in the update subextents.
 */
statusT
imm_page_to_sub_xtnt_map (
                          uint32T pageOffset,  /* in */
                          bsInMemXtntMapT *xtntMap,  /* in */
                          int updateFlg,  /* in */
                          uint32T *index  /* out */
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
            return E_PAGE_NOT_MAPPED;
        }

        lwr = 0;
        upr = xtntMap->validCnt - 1;
        last = upr;
    } else {
        /* look at the update subextents */
        MS_SMP_ASSERT(xtntMap->updateStart <= xtntMap->updateEnd);
        MS_SMP_ASSERT(xtntMap->updateEnd < xtntMap->cnt);
        if (xtntMap->cnt == 0) {
            return E_PAGE_NOT_MAPPED;
        }

        lwr = xtntMap->updateStart;
        upr = xtntMap->updateEnd;
        last = upr;
    }

    if ( (pageOffset < xtntMap->subXtntMap[lwr].pageOffset) ||
         (pageOffset >= (xtntMap->subXtntMap[upr].pageOffset +
                         xtntMap->subXtntMap[upr].pageCnt)) )
    {
        return E_PAGE_NOT_MAPPED;
    }

    mid = (lwr + upr) / 2;

    while (upr > lwr) {

        if (pageOffset < xtntMap->subXtntMap[mid].pageOffset) {

            upr = mid - 1;
            MS_SMP_ASSERT(xtntMap->subXtntMap[upr].pageOffset <=
                          xtntMap->subXtntMap[mid].pageOffset);

        } else {
            /* look mid and above */
            if ( mid + 1 > last ) {
                /* found it */
                break;
            }
                MS_SMP_ASSERT(xtntMap->subXtntMap[mid + 1].pageOffset >=
                              xtntMap->subXtntMap[mid].pageOffset);
            if ( pageOffset < xtntMap->subXtntMap[mid+1].pageOffset ) {
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
} /* end imm_page_to_sub_xtnt_map */

/*
 * imm_page_to_xtnt
 *
 * This function searches an in-memory sub extent map for the entry that
 * matches the specified page and returns the index.
 * If updateFlg is set, only look from updateStart to updateEnd.
 * If the permHoleFlg flag is not set, then pages that fall within a
 * permanent hole return E_PAGE_HOLE.
 * If the permHoleFlg flag is set, then pages that fall within a
 * permanent hole return EOK, not E_PAGE_HOLE.
 */
statusT
imm_page_to_xtnt (
                  uint32T pageOffset,  /* in */
                  bsInMemSubXtntMapT *subXtntMap,  /* in */
                  int updateFlg,  /* in */
                  int permHoleFlg,  /* in */
                  uint32T *index  /* out */
                  )
{
    uint32T lwr;
    uint32T mid;
    statusT sts;
    uint32T upr;

    if (subXtntMap->cnt == 0) {
        return E_PAGE_NOT_MAPPED;
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

    if ( (pageOffset < subXtntMap->bsXA[lwr].bsPage) ||
         (pageOffset >= subXtntMap->bsXA[upr + 1].bsPage) )
    {
        return E_PAGE_NOT_MAPPED;
    }

    while (1) {

        mid = (lwr + upr) / 2;

        if (pageOffset < subXtntMap->bsXA[mid].bsPage) {

            upr = mid - 1;
            MS_SMP_ASSERT(subXtntMap->bsXA[upr].bsPage <
                          subXtntMap->bsXA[mid].bsPage);

        } else {
            if (pageOffset < subXtntMap->bsXA[mid + 1].bsPage) {
                if ( subXtntMap->bsXA[mid].vdBlk == XTNT_TERM ) {
                    sts = E_PAGE_HOLE;
                } else if ( subXtntMap->bsXA[mid].vdBlk == PERM_HOLE_START &&
                            !permHoleFlg )
                {
                    sts = E_PAGE_HOLE;
                } else {
                    sts = EOK;
                }
                *index = mid;

                return sts;
            } else {

                lwr = mid + 1;
                MS_SMP_ASSERT(subXtntMap->bsXA[lwr].bsPage >
                              subXtntMap->bsXA[mid].bsPage);
            }
        }
    }
} /* end imm_page_to_xtnt */

/******************************************************************************/
/* overlay_xtnt_map

** This function overlays "overXtntMap" onto the "baseXtntMap".  The results
** are described by "modXtntMap" which is base plus overlay and "replXtntMap"
** which is the portion of base displaced by overlay. The page range described
** by the overlay extent map must be a subset of the page range described by
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
** extent map. If the first page of the overlay extent map does not fall
** on a subextent boundary in the base extent map then the original base mcell
** is discarded and 2 new mcells are obetained: one for the first half of the
** original mcell and one for the last half of the original mcell. The same is
** true for the last page of the overlay extent map. Also the first mcell of
** the overlay extent map may have to be discarded and a new one obtained since
** it is a BSR_SHADOW_XTNTS type and if it is not the first mcell in the
** modified mcell chain it must be an BSR_XTRA_XTNTS type. At most I get 5 new
** mcells, independent of how large the file is, or how large the migrated
** portion is. Limiting the number of mcells obtained under the parent
** transaction to a fixed maximum (not proportional to file size) prevents the
** log from overflowing if the migrated file has many extents.
** Striped files may have extent maps (segments) before and after the pased in
** baseXtntMap. stripeIndex is the stripe number of the baseXtntMap in the file.
** For non striped files stripeIndex will be 0. The code treats non striped
** files and the first stripe of a file the same.


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
                   uint32T stripeIndex,  /* in */
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
    uint32T firstOverlayPage;
    uint32T lastOverlayPage;
    uint32T moveCnt = 0;
    int i;
    bfMCIdT throwAwayMC;
    vdIndexT throwAwayVdI;
    int gotMcell = 0;
    bfMCIdT savMC;
    vdIndexT savVdI = 0;
    bfMCIdT savBaseMC;
    vdIndexT savBaseVdI = 0;
    int savBaseCnt;
    int mcellCnt = 0;
    int switchedTypes = FALSE;
    int allocated_mcell_in_step_2 = FALSE;
    int extent_map_locked = FALSE;
    bsInMemXtntMapT *prevStripeXtntMap;
    bsInMemSubXtntMapT *prevStripeSubXtntMapp;
    uint32T prevXtnt = 0;
    uint32T nextPg;
    uint dmnVersion = bfap->dmnP->dmnVersion;

    /* Create a new extent map for the "replace" extents. */
    /* The most sub extents we ever need is all of the base, baseXtntMap->cnt.*/
    sts = imm_create_xtnt_map ( baseXtntMap->blksPerPage,
                                baseXtntMap->domain,
                                baseXtntMap->cnt, /* pre allocate subextents */
                                0,  /* termPage - not used */
                                0,  /* termVdIndex - not used */
                                &replXtntMap);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /* Create a new extent map for the "modified" extents. */
    /* The most subextents we need is the number in the base - 1 (the overlay */
    /* map overlaps at least one of the base subxtnts) + 2 (two new subxtnts */
    /* at the start and end of the overlay) + the number in the overlay. */
    sts = imm_create_xtnt_map ( baseXtntMap->blksPerPage,
                                baseXtntMap->domain,
                                baseXtntMap->cnt + overXtntMap->cnt + 1,
                                0,  /* termPage - not used */
                                0,  /* termVdIndex - not used */
                                &modXtntMap);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    /*
     * If this file has extent information in the primary mcell, then
     * that is where the mcell count for the extent chain resides.
     * Save it for later comparison with the modXtntMap->cnt.
     */
    if (FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, bfap->xtnts.type)) {
        mcellCnt = baseXtntMap->cnt;
    }

    firstOverlayPage = overXtntMap->subXtntMap[0].pageOffset;
    overSubXtntMapp = &overXtntMap->subXtntMap[overXtntMap->cnt - 1];
    lastOverlayPage = overSubXtntMapp->bsXA[overSubXtntMapp->cnt-1].bsPage - 1;

    MS_SMP_ASSERT(lastOverlayPage >= firstOverlayPage);
    MS_SMP_ASSERT(firstOverlayPage >= baseXtntMap->subXtntMap[0].pageOffset);
    MS_SMP_ASSERT(lastOverlayPage <=
                  baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].pageOffset +
                  baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].pageCnt - 1);

/*
** step 1) copy first unchanged sub extent maps from baseXtntMap to modXtntMap
*/

    MS_SMP_ASSERT(baseXtntMap->cnt > 0);
    for ( modSXMapCnt = 0, baseSXMapCnt = 0;
          baseSXMapCnt < baseXtntMap->cnt - 1;
          baseSXMapCnt++, modSXMapCnt++ )
    {
        baseSubXtntMapp = &baseXtntMap->subXtntMap[baseSXMapCnt];
        nextPg = baseSubXtntMapp->pageOffset + baseSubXtntMapp->pageCnt;
        if ( firstOverlayPage < nextPg ) {
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
    MS_SMP_ASSERT(firstOverlayPage >= baseSubXtntMapp->pageOffset);
    if ((baseSubXtntMapp->pageCnt > 0) &&
        (firstOverlayPage != baseSubXtntMapp->pageOffset)) {

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

        part2BsXA.bsPage = 0;         /* flag for step 3 */

        /* Find the boundary within a subextent in baseXtntMap of the first */
        /* page in the overlay. Copy the first part to modXtntMap. If the */
        /* boundary does not lie on an extent boundary, break the extent in */
        /* two. Save descIndex and the second half of the extent for step 3. */
        MS_SMP_ASSERT(modSubXtntMapp->cnt > 1);
        for ( descIndex = 0; descIndex < modSubXtntMapp->cnt - 1; descIndex++ ){
            if ( firstOverlayPage >= modSubXtntMapp->bsXA[descIndex+1].bsPage ){
                continue;
            }

            /* if the overlay falls on an extent boundary */
            if ( firstOverlayPage == modSubXtntMapp->bsXA[descIndex].bsPage ) {
                modSubXtntMapp->bsXA[descIndex].vdBlk = XTNT_TERM;
                modSubXtntMapp->cnt = descIndex + 1;
                modSubXtntMapp->updateStart = descIndex + 1;
                break;
            }

            /* the overlay does not fall on an extent boundary */
            imm_split_desc (firstOverlayPage,
                            baseXtntMap->blksPerPage,
                            &modSubXtntMapp->bsXA[descIndex],
                            &part1BsXA,
                            &part2BsXA);
            modSubXtntMapp->bsXA[descIndex + 1].bsPage = part2BsXA.bsPage;
            modSubXtntMapp->bsXA[descIndex + 1].vdBlk = XTNT_TERM;
            modSubXtntMapp->cnt = descIndex + 2;
            modSubXtntMapp->updateStart = descIndex + 2;
            break;
        }
        modSubXtntMapp->pageCnt =
          modSubXtntMapp->bsXA[modSubXtntMapp->cnt - 1].bsPage -
                                   modSubXtntMapp->pageOffset;

/*
** step 2a) get new mcell for modXtntMap
** Skip this step if the current base subextent is in the primary mcell.
** The primary mcell will continue to hold the first part of the base
** extent that is being retained in the modXtntMap.
*/

        if (baseXtntMap->subXtntMap[baseSXMapCnt].type == BSR_XTNTS) {
            MS_SMP_ASSERT(FIRST_XTNT_IN_PRIM_MCELL( dmnVersion,
                            bfap->xtnts.type));
        } else {
            sts = stg_alloc_new_mcell (bfap,
                                       bfap->bfSetp->dirTag,
                                       bfap->tag,
                                       modSubXtntMapp->vdIndex,
                                       ftxH,
                                       &modSubXtntMapp->mcellId,
                                       FALSE);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            allocated_mcell_in_step_2 = TRUE;

            /*
             * This will be the same type (BSR_SHADOW_XTNTS or BSR_XTRA_XTNTS)
             * as the subextent copied in step 2 above. It will have the same
             * position in the mod chain as it had in the base so its type is OK
             */
            MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
            sts = odm_create_xtnt_rec ( bfap,
                                        baseXtntMap->allocVdIndex,
                                        modSubXtntMapp,
                                        0,  /* no clone xfer xtnts here */
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
                          baseXtntMap->subXtntMap[baseSXMapCnt - 1].vdIndex,
                          baseXtntMap->subXtntMap[baseSXMapCnt - 1].mcellId,
                          baseXtntMap->subXtntMap[baseSXMapCnt].vdIndex,
                          baseXtntMap->subXtntMap[baseSXMapCnt].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].vdIndex,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH);
            } else if ( stripeIndex ) {
                /* The file is striped and this is not the first stripe. */
                /* Unlink this stripe from the mcell chain. */
                MS_SMP_ASSERT(bfap->xtnts.type == BSXMT_STRIPE);
                MS_SMP_ASSERT(bfap->xtnts.stripeXtntMap != NULL);
                MS_SMP_ASSERT(stripeIndex < bfap->xtnts.stripeXtntMap->cnt);

                prevStripeXtntMap =
                            bfap->xtnts.stripeXtntMap->xtntMap[stripeIndex - 1];
                prevStripeSubXtntMapp =
                &prevStripeXtntMap->subXtntMap[prevStripeXtntMap->validCnt - 1];
                sts = bmt_unlink_mcells ( bfap->dmnP,
                          bfap->tag,
                          prevStripeSubXtntMapp->vdIndex,
                          prevStripeSubXtntMapp->mcellId,
                          baseXtntMap->subXtntMap[0].vdIndex,
                          baseXtntMap->subXtntMap[0].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].vdIndex,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH);
            } else {
                /* No mcells were copied in step 1. Case III - VIII */
                sts = x_detach_extent_chain ( bfap,
                                              baseXtntMap,
                                              ftxH,
                                              &throwAwayVdI,
                                              &throwAwayMC,
                                              &throwAwayVdI,
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
                                  modXtntMap->subXtntMap[modSXMapCnt-1].vdIndex,
                                  modXtntMap->subXtntMap[modSXMapCnt-1].mcellId,
                                  modSubXtntMapp->vdIndex,
                                  modSubXtntMapp->mcellId,
                                  modSubXtntMapp->vdIndex,
                                  modSubXtntMapp->mcellId,
                                  ftxH);
            } else {
                if ( stripeIndex ) {
                    /* The file is striped and this is not the first stripe. */
                    /* Attach the mod extents to the previous stripe. */

                    prevStripeXtntMap =
                        bfap->xtnts.stripeXtntMap->xtntMap[stripeIndex - 1];
                    prevStripeSubXtntMapp =
                        &prevStripeXtntMap->subXtntMap[prevStripeXtntMap->validCnt - 1];
                }
                sts = bmt_link_mcells( bfap->dmnP,
                                       bfap->tag,
                                       (stripeIndex ? prevStripeSubXtntMapp->vdIndex : bfap->primVdIndex),
                                       (stripeIndex ? prevStripeSubXtntMapp->mcellId : bfap->primMCId),
                                       modSubXtntMapp->vdIndex,
                                       modSubXtntMapp->mcellId,
                                       modSubXtntMapp->vdIndex,
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
         * We want only SHADOW and XTRA extent records in the
         * deferred delete list code.
         */
        XTNMAP_LOCK_WRITE(&bfap->xtntMap_lk);
        extent_map_locked = TRUE;
        if (baseSubXtntMapp->type == BSR_XTNTS) {
            baseSubXtntMapp->type = BSR_XTRA_XTNTS;
            baseSubXtntMapp->onDiskMaxCnt = BMT_XTRA_XTNTS;
            switchedTypes = TRUE;
        }

        sts = imm_copy_sub_xtnt_map ( bfap,
                                      baseXtntMap,
                                      baseSubXtntMapp,
                                      replSubXtntMapp);

        if (switchedTypes) {
            baseSubXtntMapp->type = BSR_XTNTS;
            baseSubXtntMapp->onDiskMaxCnt = BMT_XTNTS;
            switchedTypes = FALSE;
        }

        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        XTNMAP_UNLOCK(&bfap->xtntMap_lk);
        extent_map_locked = FALSE;


        /* Continue processing the same baseXtntMap subextent step 2 worked */
        /* on. Use descIndex and part2BsXA from step 2 to know where to start.*/
        replXtntMap->cnt = replXtntMap->validCnt = 1;
        if ( part2BsXA.bsPage != 0 ) {     /* flag from step 2 */
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
        replSubXtntMapp->pageOffset = replSubXtntMapp->bsXA[0].bsPage;
        replSubXtntMapp->pageCnt = replSubXtntMapp->bsXA[moveCnt - 1].bsPage -
                                   replSubXtntMapp->pageOffset;

        /* If the overlay covers more than one base extent then */
        /* we are done with this base extent and this replace extent */
        /* otherwise we are still processing this base extent and we need */
        /* to remember at what descriptor we left off for step 7. */
        nextPg = replSubXtntMapp->pageOffset + replSubXtntMapp->pageCnt;
        if ( lastOverlayPage + 1 >= nextPg ) {
            /* Case I, II, VI - VIII */
            savBaseMC = baseSubXtntMapp->mcellId;    /* for step 3a */
            savBaseVdI = baseSubXtntMapp->vdIndex;
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
                                       replSubXtntMapp->vdIndex,
                                       ftxH,
                                       &replSubXtntMapp->mcellId,
                                       FALSE);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
            sts = odm_create_xtnt_rec ( bfap,
                                        baseXtntMap->allocVdIndex,
                                        replSubXtntMapp,
                                        0,  /* no clone xfer xtnts here */
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
                MS_SMP_ASSERT(baseSubXtntMapp->vdIndex ==
                              modSubXtntMapp->vdIndex);
                MS_SMP_ASSERT(baseSubXtntMapp->mcellId.cell ==
                              modSubXtntMapp->mcellId.cell);
                MS_SMP_ASSERT(baseSubXtntMapp->mcellId.page ==
                              modSubXtntMapp->mcellId.page);
                modSubXtntMapp->updateStart = 0;
                modSubXtntMapp->updateEnd = modSubXtntMapp->cnt-1;
                sts = update_xtnt_rec (bfap->dmnP,
                                       bfap->tag,
                                       modSubXtntMapp,
                                       ftxH);
            } else {
                sts = deferred_free_mcell( VD_HTOP(savBaseVdI, bfap->dmnP),
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
        savVdI = modXtntMap->subXtntMap[modSXMapCnt - 1].vdIndex;

        /*
         * If the first mcell in the overlay chain is a BSR_SHADOW_XTNTS, yet
         * this mcell is not going to be the first in the modified chain, then
         * we must replace this mcell with a BSR_XTRA_XTNTS mcell.
         */
        MS_SMP_ASSERT(overXtntMap->cnt);
        if (overXtntMap->subXtntMap[0].type == BSR_SHADOW_XTNTS) {
            if ( overXtntMap->cnt > 1 ) {
                /*
                 * Since there is more than one mcell in the overlay chain,
                 * we must unlink the first mcell (the BSR_SHADOW_XTNTS mcell)
                 * from the rest of the chain.
                 */
                sts = bmt_unlink_mcells ( bfap->dmnP,
                          bfap->tag,
                          overXtntMap->subXtntMap[0].vdIndex,
                          overXtntMap->subXtntMap[0].mcellId,
                          overXtntMap->subXtntMap[1].vdIndex,
                          overXtntMap->subXtntMap[1].mcellId,
                          overXtntMap->subXtntMap[overXtntMap->cnt - 1].vdIndex,
                          overXtntMap->subXtntMap[overXtntMap->cnt - 1].mcellId,
                          ftxH);
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }
            }

            /* free the BSR_SHADOW_XTNTS mcell at the end of the transaction */
            sts = deferred_free_mcell (
                  VD_HTOP(overXtntMap->subXtntMap[0].vdIndex, bfap->dmnP),
                  overXtntMap->subXtntMap[0].mcellId,
                  ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            sts = stg_alloc_new_mcell ( bfap,
                                        bfap->bfSetp->dirTag,
                                        bfap->tag,
                                        overXtntMap->subXtntMap[0].vdIndex,
                                        ftxH,
                                        &overXtntMap->subXtntMap[0].mcellId,
                                        FALSE);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            /* Change the in memory subextent type to BSR_XTRA_XTNTS. */
            /* This is used to initialize the mcell. */
            overXtntMap->subXtntMap[0].type = BSR_XTRA_XTNTS;
            overXtntMap->subXtntMap[0].onDiskMaxCnt = BMT_XTRA_XTNTS;
            MS_SMP_ASSERT(bfap->dmnP == overXtntMap->domain);
            sts = odm_create_xtnt_rec ( bfap,
                                        overXtntMap->allocVdIndex,
                                        &overXtntMap->subXtntMap[0],
                                        0,  /* no clone xfer xtnts here */
                                        ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            if ( overXtntMap->cnt > 1 ) {
                /* If there were BSR_XTRA_XTNTS mcells unlinked from the */
                /* BSR_SHADOW_XTNTS mcell above, link new BSR_XTRA_XTNTS mcell*/
                /* to the rest of the overlay chain. */
                sts = bmt_link_mcells ( bfap->dmnP,
                          bfap->tag,
                          overXtntMap->subXtntMap[0].vdIndex,
                          overXtntMap->subXtntMap[0].mcellId,
                          overXtntMap->subXtntMap[1].vdIndex,
                          overXtntMap->subXtntMap[1].mcellId,
                          overXtntMap->subXtntMap[overXtntMap->cnt - 1].vdIndex,
                          overXtntMap->subXtntMap[overXtntMap->cnt - 1].mcellId,
                          ftxH);
                if ( sts != EOK ) {
                    RAISE_EXCEPTION(sts);
                }
            }
        } /* If first overlay subextent is BSR_SHADOW_XTNTS */
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
     *      volumes and we are migrating some or all of the pages
     *      to the volume where its primary mcell is.
     * In this case, we want to end up with the primary mcell on the destination
     * volume containing an extent.
     */

    if ( (FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, bfap->xtnts.type)) &&
         (overXtntMap->subXtntMap[0].vdIndex ==
          baseXtntMap->subXtntMap[0].vdIndex) &&

         /* must have at least 2 mcells */
         (overXtntMap->validCnt == 1) &&
         (baseXtntMap->validCnt > 1) &&

         /* Handles base maps with more than just 2 mcells.
          * Also now handles base map with a different page count
          * than the overlay page count.
          */

         /* offset in overlay matches the first extent offset in base */
         (overXtntMap->subXtntMap[0].pageOffset ==
          baseXtntMap->subXtntMap[1].pageOffset) &&

         /* overlay contains only one extent, base only contains TERM */
         (overXtntMap->subXtntMap[0].cnt == 2) &&
         (baseXtntMap->subXtntMap[0].cnt == 1) )
    {
        bsXtntT *oldbsXA;

        MS_SMP_ASSERT(baseXtntMap->subXtntMap[0].type == BSR_XTNTS);

        modSubXtntMapp = &modXtntMap->subXtntMap[0];
        overSubXtntMapp = &overXtntMap->subXtntMap[0];
        baseSubXtntMapp = &baseXtntMap->subXtntMap[0];
        replSubXtntMapp = &replXtntMap->subXtntMap[0];
        oldbsXA = modSubXtntMapp->bsXA;

        /*
         * Detach the old BSR_XTRA_XTNTS mcell from the primary mcell.
         */
        sts = x_detach_extent_chain ( bfap,
                                      baseXtntMap,
                                      ftxH,
                                      &throwAwayVdI,
                                      &throwAwayMC,
                                      &throwAwayVdI,
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
            modSubXtntMapp->bsXA = oldbsXA;
            RAISE_EXCEPTION(sts);
        }
        ms_free(oldbsXA);

        modSubXtntMapp->type = BSR_XTNTS;
        modSubXtntMapp->maxCnt = BMT_XTNTS;
        modSubXtntMapp->vdIndex = baseSubXtntMapp->vdIndex;
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
        MS_SMP_ASSERT(replXtntMap->maxCnt > replSXMapCnt);

        overSubXtntMapp->cnt = 1;
        overSubXtntMapp->bsXA[0].bsPage = 0;
        overSubXtntMapp->bsXA[0].vdBlk = XTNT_TERM;

        overSubXtntMapp->updateStart = 0;
        overSubXtntMapp->updateEnd = 1;
        sts = update_xtnt_rec (bfap->dmnP,
                               bfap->tag,
                               overSubXtntMapp,
                               ftxH);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        sts = imm_copy_sub_xtnt_map( bfap,
                                     overXtntMap,
                                     overSubXtntMapp,
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
    if ( (FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, bfap->xtnts.type)) &&
         (overXtntMap->subXtntMap[0].vdIndex ==
          baseXtntMap->subXtntMap[0].vdIndex) &&

         /* base must have at least 1 mcell */
         (overXtntMap->validCnt == 1) &&
         (baseXtntMap->validCnt > 0) &&

         /* overlay contains one extent, base prim mcell one extent */
         (overXtntMap->subXtntMap[0].cnt == 2) &&
         (baseXtntMap->subXtntMap[0].cnt == 2) &&

         /* overlay will replace prim mcell */
         ((overXtntMap->subXtntMap[0].pageOffset ==
           baseXtntMap->subXtntMap[0].pageOffset) ||

           /* -or- */
           /* overlay is contiguous to base prim */
           ((baseXtntMap->subXtntMap[0].pageOffset +
             baseXtntMap->subXtntMap[0].pageCnt) ==
             /* base end page == over start offset */
             overXtntMap->subXtntMap[0].pageOffset) &&
             /* blocks adjacent */
           ((baseXtntMap->subXtntMap[0].bsXA[0].vdBlk +
             (baseXtntMap->subXtntMap[0].bsXA[1].bsPage * bfap->bfPageSz)) ==
              overXtntMap->subXtntMap[0].bsXA[0].vdBlk)) )
    {
        bsXtntT *oldbsXA;

        MS_SMP_ASSERT(baseXtntMap->subXtntMap[0].type == BSR_XTNTS);

        overSubXtntMapp = &overXtntMap->subXtntMap[0];
        baseSubXtntMapp = &baseXtntMap->subXtntMap[0];
        modSubXtntMapp = &modXtntMap->subXtntMap[0];
        replSubXtntMapp = &replXtntMap->subXtntMap[0];
        oldbsXA = modSubXtntMapp->bsXA;

        /*
         * Detach the old BSR_XTRA_XTNTS mcell from the primary mcell
         * of the base map.
         */
        if ( baseXtntMap->validCnt > 1 ) {
            /* detach base[1] from base primary[0] */
            sts = x_detach_extent_chain ( bfap,
                                      baseXtntMap,
                                      ftxH,
                                      &throwAwayVdI,
                                      &throwAwayMC,
                                      &throwAwayVdI,
                                      &throwAwayMC);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
        }

        /*
         * Copy the (only) overlay subextent into a new mcell at
         * modXtntMap slot 0.
         */
        modSubXtntMapp->bsXA=NULL;
        sts = imm_copy_sub_xtnt_map( bfap,
                                     overXtntMap,
                                     overSubXtntMapp,
                                     modSubXtntMapp);
        if (sts != EOK) {
            modSubXtntMapp->bsXA = oldbsXA;
            RAISE_EXCEPTION(sts);
        }
        ms_free(oldbsXA);

        /* Change the mcell Id on the mod to use the base's mcell Id
         * because we would have to change the tag dir file if we were to
         * start assigning primary mcell ids to files!!
         */
        modSubXtntMapp->type = BSR_XTNTS;
        modSubXtntMapp->maxCnt = BMT_XTNTS;
        modSubXtntMapp->vdIndex = baseSubXtntMapp->vdIndex;
        /* Set the type and mcell for modXtntMap slot 0
         * to be the primary mcell.  In other words, set the
         * mod mcell = base mcell so we reuse the prim mcell on disk.
         */
        modSubXtntMapp->mcellId = baseSubXtntMapp->mcellId;
        modSubXtntMapp->onDiskMaxCnt = baseSubXtntMapp->onDiskMaxCnt;

        if ( overXtntMap->subXtntMap[0].pageOffset ==
             baseXtntMap->subXtntMap[0].pageOffset )
        {
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
             * delete the overwritten extent out of the primary mcell
             * step 5 correctly deals with an extent in the primary mcell.
             */
            baseSXMapCnt = 0;
        } else {
            /* Overlay extent is contiguous with the extent in the PM base.
             * Combine the extents from the base prim mcell and the overlay
             * mcell and place the single new extent into the new mod.
             */
            modSubXtntMapp->bsXA[0].bsPage = baseSubXtntMapp->bsXA[0].bsPage;
            modSubXtntMapp->bsXA[0].vdBlk  = baseSubXtntMapp->bsXA[0].vdBlk;
            modSubXtntMapp->bsXA[1].bsPage = overSubXtntMapp->bsXA[1].bsPage;
            modSubXtntMapp->pageCnt        = baseSubXtntMapp->pageCnt +
                                             overSubXtntMapp->pageCnt;
            modSubXtntMapp->pageOffset     = baseSubXtntMapp->pageOffset;
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
                               VD_HTOP(overSubXtntMapp->vdIndex, bfap->dmnP),
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
    if ( ((!FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, bfap->xtnts.type) &&
           /* v3-complete base was already copied to mod in step 1 above */
           (modSXMapCnt >= 1) &&
           (baseSXMapCnt >= 1)) ||

          ((modSXMapCnt >= 2) && /* v4 & non-primary mcells only */
          /* current mod must be in BSR_XTRA_XTNTS mcells */
          (modXtntMap->validCnt >= 2))) &&

         (prevXtnt > 0) &&     /* another non-primary mcells only check */
         (overSXMapCnt == 0) && /* first run through only */
         (overXtntMap->validCnt == 1) && /* overlay must have 1 mcell only */

         (overXtntMap->subXtntMap[0].vdIndex ==  /* must be on same vd */
          modXtntMap->subXtntMap[modSXMapCnt-1].vdIndex) &&

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
          * with its page offset, and the base offset and mod offset will not
          * match. pageCnt comparison is also necessary in case the previous
          * mcell is the primary mcell and its offset is 0 just as this mcells.
          */
         (baseXtntMap->subXtntMap[baseSXMapCnt-1].pageOffset ==
          modXtntMap->subXtntMap[modSXMapCnt-1].pageOffset) &&
         (baseXtntMap->subXtntMap[baseSXMapCnt-1].pageCnt ==
          modXtntMap->subXtntMap[modSXMapCnt-1].pageCnt) &&

         /* overlay page is contiguous to last mod */
         (modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt].bsPage ==
                overXtntMap->subXtntMap[0].bsXA[0].bsPage) &&

         /* overlay blocks contiguous to last mod blocks */
         ((modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt-1].vdBlk +
          ((modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt].bsPage -
            modXtntMap->subXtntMap[modSXMapCnt-1].bsXA[prevXtnt-1].bsPage )
                  * bfap->bfPageSz)) ==
            overXtntMap->subXtntMap[0].bsXA[0].vdBlk) )
    {
        /* overlay extent is contiguous with the extent in the base */
        /* update the mod mcell header with the new start and end pages */
        modSubXtntMapp->bsXA[prevXtnt].bsPage = overSubXtntMapp->bsXA[1].bsPage;
        modSubXtntMapp->pageCnt        = modSubXtntMapp->pageCnt +
                                         overSubXtntMapp->pageCnt;
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
                       baseXtntMap->subXtntMap[baseSXMapCnt - 1].vdIndex,
                       baseXtntMap->subXtntMap[baseSXMapCnt - 1].mcellId,
                       baseXtntMap->subXtntMap[baseSXMapCnt].vdIndex,
                       baseXtntMap->subXtntMap[baseSXMapCnt].mcellId,
                       baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].vdIndex,
                       baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                       ftxH);

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
                               VD_HTOP(overSubXtntMapp->vdIndex, bfap->dmnP),
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
        if ( (FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, bfap->xtnts.type)) &&
             (modSXMapCnt == 0) &&
             (overXtntMap->subXtntMap[overSXMapCnt].type != BSR_XTNTS) )
        {

            modSubXtntMapp = &modXtntMap->subXtntMap[modSXMapCnt++];

            sts = imm_copy_sub_xtnt_map( bfap,
                                         baseXtntMap,
                                         &baseXtntMap->subXtntMap[0],
                                         modSubXtntMapp);
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }
            modSubXtntMapp->pageOffset = 0;
            modSubXtntMapp->pageCnt = 0;
            modSubXtntMapp->cnt = 1;
            modSubXtntMapp->bsXA[0].bsPage = 0;
            modSubXtntMapp->bsXA[0].vdBlk = XTNT_TERM;
        }

        for ( overSXMapCnt = 0;
              overSXMapCnt < overXtntMap->cnt;
              overSXMapCnt++, modSXMapCnt++ )
        {
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
        if ( savVdI ) {
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
                          savVdI,
                          savMC,
                          baseXtntMap->subXtntMap[baseSXMapCnt].vdIndex,
                          baseXtntMap->subXtntMap[baseSXMapCnt].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].vdIndex,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH);
                    } else if ( baseXtntMap->cnt > 1 ) {
                        sts = bmt_unlink_mcells (bfap->dmnP,
                          bfap->tag,
                          savVdI,
                          savMC,
                          baseXtntMap->subXtntMap[baseSXMapCnt+1].vdIndex,
                          baseXtntMap->subXtntMap[baseSXMapCnt+1].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].vdIndex,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH);
                    }
                    if ( sts != EOK ) {
                        RAISE_EXCEPTION(sts);
                    }
                }
            }

            sts = bmt_link_mcells ( bfap->dmnP,
                                    bfap->tag,
                                    savVdI,
                                    savMC,
                                    overXtntMap->subXtntMap[0].vdIndex,
                                    overXtntMap->subXtntMap[0].mcellId,
                              overXtntMap->subXtntMap[overSXMapCnt - 1].vdIndex,
                              overXtntMap->subXtntMap[overSXMapCnt - 1].mcellId,
                                    ftxH);
        } else if ( stripeIndex ) {
            prevStripeXtntMap =
                            bfap->xtnts.stripeXtntMap->xtntMap[stripeIndex - 1];
            prevStripeSubXtntMapp =
                &prevStripeXtntMap->subXtntMap[prevStripeXtntMap->validCnt - 1];

            /* Unlink this stripe from the previous stripe. */
            sts = bmt_unlink_mcells (bfap->dmnP,
                          bfap->tag,
                          prevStripeSubXtntMapp->vdIndex,
                          prevStripeSubXtntMapp->mcellId,
                          baseXtntMap->subXtntMap[0].vdIndex,
                          baseXtntMap->subXtntMap[0].mcellId,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].vdIndex,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].mcellId,
                          ftxH);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }

            /* Link the overlay mcells to the previous stripe. */
            sts = bmt_link_mcells ( bfap->dmnP,
                                    bfap->tag,
                                    prevStripeSubXtntMapp->vdIndex,
                                    prevStripeSubXtntMapp->mcellId,
                                    overXtntMap->subXtntMap[0].vdIndex,
                                    overXtntMap->subXtntMap[0].mcellId,
                              overXtntMap->subXtntMap[overSXMapCnt - 1].vdIndex,
                              overXtntMap->subXtntMap[overSXMapCnt - 1].mcellId,
                                    ftxH);
        } else {
            /* Case IV & VII */
            sts = x_detach_extent_chain ( bfap,
                                          baseXtntMap,
                                          ftxH,
                                          &throwAwayVdI,
                                          &throwAwayMC,
                                          &throwAwayVdI,
                                          &throwAwayMC);
            if ( sts != EOK ) {
                RAISE_EXCEPTION(sts);
            }
            sts = bmt_link_mcells ( bfap->dmnP,
                                    bfap->tag,
                                    bfap->primVdIndex,
                                    bfap->primMCId,
                                    overXtntMap->subXtntMap[0].vdIndex,
                                    overXtntMap->subXtntMap[0].mcellId,
                                    overXtntMap->subXtntMap[overSXMapCnt - 1].vdIndex,
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

    for ( ; baseSXMapCnt < baseXtntMap->cnt; baseSXMapCnt++, replSXMapCnt++ ) {
        baseSubXtntMapp = &baseXtntMap->subXtntMap[baseSXMapCnt];
        nextPg = baseSubXtntMapp->pageOffset + baseSubXtntMapp->pageCnt;
        if ( lastOverlayPage + 1 < nextPg ) {
            break;
        }

        MS_SMP_ASSERT(replXtntMap->maxCnt > replSXMapCnt);

        replSubXtntMapp = &replXtntMap->subXtntMap[replSXMapCnt];

        /*
         * We want only SHADOW and XTRA extent records in the
         * deferred delete list code.
         */
        XTNMAP_LOCK_WRITE(&bfap->xtntMap_lk);
        extent_map_locked = TRUE;
        if (baseSubXtntMapp->type == BSR_XTNTS) {
            baseSubXtntMapp->type = BSR_XTRA_XTNTS;
            baseSubXtntMapp->onDiskMaxCnt = BMT_XTRA_XTNTS;
            switchedTypes = TRUE;
        }

        sts = imm_copy_sub_xtnt_map( bfap,
                                     baseXtntMap,
                                     baseSubXtntMapp,
                                     replSubXtntMapp);

        if (switchedTypes) {
            baseSubXtntMapp->type = BSR_XTNTS;
            baseSubXtntMapp->onDiskMaxCnt = BMT_XTNTS;
            switchedTypes = FALSE;
        }

        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        XTNMAP_UNLOCK(&bfap->xtntMap_lk);
        extent_map_locked = FALSE;

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
                                       replSubXtntMapp->vdIndex,
                                       ftxH,
                                       &replSubXtntMapp->mcellId,
                                       FALSE);
           if ( sts != EOK ) {
               RAISE_EXCEPTION(sts);
           }
           gotMcell = TRUE;

           sts = odm_create_xtnt_rec ( bfap,
                                       baseXtntMap->allocVdIndex,
                                       replSubXtntMapp,
                                       0,  /* no clone xfer xtnts here */
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
                              replXtntMap->subXtntMap[0].vdIndex,
                              replXtntMap->subXtntMap[0].mcellId,
                              replXtntMap->subXtntMap[1].vdIndex,
                              replXtntMap->subXtntMap[1].mcellId,
                              replXtntMap->subXtntMap[replSXMapCnt - 1].vdIndex,
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
    nextPg = baseSubXtntMapp->pageOffset + baseSubXtntMapp->pageCnt;
    if ( lastOverlayPage + 1 != nextPg ) {
/*
** step 6) copy part of a subextent from baseXtntMap to replXtntMap
*/

        MS_SMP_ASSERT(replXtntMap->maxCnt > replSXMapCnt);

        replSubXtntMapp = &replXtntMap->subXtntMap[replSXMapCnt];
        /* replSXMapCnt will be == 0 in case III where we don't want to copy */
        /* the subextnt map. replSubXtntMapp->cnt will be == 0 in case IV */
        /* where we do want to copy the subextnt map. */
        if ( replSXMapCnt != 0 || replSubXtntMapp->cnt == 0 ) {

            /*
             * We want only SHADOW and XTRA extent records in the
             * deferred delete list code.
             */
            XTNMAP_LOCK_WRITE(&bfap->xtntMap_lk);
            extent_map_locked = TRUE;
            if (baseSubXtntMapp->type == BSR_XTNTS) {
                baseSubXtntMapp->type = BSR_XTRA_XTNTS;
                baseSubXtntMapp->onDiskMaxCnt = BMT_XTRA_XTNTS;
                switchedTypes = TRUE;
            }

            sts = imm_copy_sub_xtnt_map( bfap,
                                         baseXtntMap,
                                         baseSubXtntMapp,
                                         replSubXtntMapp);

            if (switchedTypes) {
                baseSubXtntMapp->type = BSR_XTNTS;
                baseSubXtntMapp->onDiskMaxCnt = BMT_XTNTS;
                switchedTypes = FALSE;
            }

            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }

            XTNMAP_UNLOCK(&bfap->xtntMap_lk);
            extent_map_locked = FALSE;

            replXtntMap->cnt = replXtntMap->validCnt = replSXMapCnt + 1;
        }

        part2BsXA.bsPage = 0;       /* flag for step 7 */

        /* Find the boundary in baseXtntMap of the last page in the overlay. */
        /* Copy the first part to replXtntMap. If the boundary does not lie */
        /* on an extent boundary, break the extent in two. Save descIndex */
        /* and the second half of the extent for step 7. */
        for ( descIndex = 0;
              descIndex < replXtntMap->subXtntMap[replSXMapCnt].cnt - 1;
              descIndex++ )
        {
            uint32T pg;

            pg = replSubXtntMapp->bsXA[descIndex + 1].bsPage;
            if ( lastOverlayPage + 1 >= pg ) {
                continue;
            }

            /* if the overlay ends on an extent boundary */
            pg = replSubXtntMapp->bsXA[descIndex].bsPage;
            if ( lastOverlayPage + 1 == pg ) {
                replSubXtntMapp->bsXA[descIndex].vdBlk = XTNT_TERM;
                replSubXtntMapp->cnt = descIndex + 1;
                replSubXtntMapp->updateStart = descIndex + 1;
                break;
            }

            /* the overlay does not ends on an extent boundary */
            imm_split_desc (lastOverlayPage + 1,
                            baseXtntMap->blksPerPage,
                            &replSubXtntMapp->bsXA[descIndex],
                            &part1BsXA,
                            &part2BsXA);
            replSubXtntMapp->bsXA[descIndex + 1].bsPage = part2BsXA.bsPage;
            replSubXtntMapp->bsXA[descIndex + 1].vdBlk = XTNT_TERM;
            replSubXtntMapp->cnt = descIndex + 2;
            replSubXtntMapp->updateStart = descIndex + 2;
            replSubXtntMapp->pageCnt =
              replSubXtntMapp->bsXA[replSubXtntMapp->cnt - 1].bsPage -
              replSubXtntMapp->pageOffset;
            break;
        }

/*
** step 6a) get new mcell for replXtntMap
*/

        sts = stg_alloc_new_mcell ( bfap,
                                    bfap->bfSetp->dirTag,
                                    bfap->tag,
                                    replSubXtntMapp->vdIndex,
                                    ftxH,
                                    &replSubXtntMapp->mcellId,
                                    FALSE);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }
        MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
        sts = odm_create_xtnt_rec ( bfap,
                                    baseXtntMap->allocVdIndex,
                                    replSubXtntMapp,
                                    0,  /* no clone xfer xtnts here */
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
            MS_SMP_ASSERT(baseSubXtntMapp->vdIndex == modSubXtntMapp->vdIndex);
            MS_SMP_ASSERT(baseSubXtntMapp->mcellId.cell ==
                          modSubXtntMapp->mcellId.cell);
            MS_SMP_ASSERT(baseSubXtntMapp->mcellId.page ==
                          modSubXtntMapp->mcellId.page);
            modSubXtntMapp->updateStart = 0;
            modSubXtntMapp->updateEnd = modSubXtntMapp->cnt-1;
            sts = update_xtnt_rec (bfap->dmnP,
                                   bfap->tag,
                                   modSubXtntMapp,
                                   ftxH);
        } else {
            savBaseVdI = baseSubXtntMapp->vdIndex;
            sts = deferred_free_mcell ( VD_HTOP(savBaseVdI, bfap->dmnP),
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
                                replXtntMap->subXtntMap[replSXMapCnt-1].vdIndex,
                                replXtntMap->subXtntMap[replSXMapCnt-1].mcellId,
                                replSubXtntMapp->vdIndex,
                                replSubXtntMapp->mcellId,
                                replSubXtntMapp->vdIndex,
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
        if ( part2BsXA.bsPage != 0 ) {   /* flag from part 6 */
            modSubXtntMapp->bsXA[descIndex] = part2BsXA;
        }

        moveCnt = baseSubXtntMapp->cnt - descIndex;
        for (i = 0; i < moveCnt; i++) {
            modSubXtntMapp->bsXA[i] = modSubXtntMapp->bsXA[descIndex + i];
        }
        modSubXtntMapp->cnt = moveCnt;
        modSubXtntMapp->updateStart = moveCnt;
        modSubXtntMapp->pageOffset = modSubXtntMapp->bsXA[0].bsPage;
        modSubXtntMapp->pageCnt = modSubXtntMapp->bsXA[moveCnt - 1].bsPage -
                                   modSubXtntMapp->pageOffset;

/*
** step 7a) get new mcell for modXtntMap
*/

        sts = stg_alloc_new_mcell ( bfap,
                                    bfap->bfSetp->dirTag,
                                    bfap->tag,
                                    modSubXtntMapp->vdIndex,
                                    ftxH,
                                    &modSubXtntMapp->mcellId,
                                    FALSE);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        /* If this is copied from the first base subextent (case III & IV), */
        /* the type will be BSR_SHADOW_XTNTS. Since this is not the first */
        /* new subextent map (at least, step 4 copied some), we must change */
        /* the type to BSR_XTRA_XTNTS before initializing the new mcell. */
        modSubXtntMapp->type = BSR_XTRA_XTNTS;
        modSubXtntMapp->onDiskMaxCnt = BMT_XTRA_XTNTS;
        MS_SMP_ASSERT(bfap->dmnP == baseXtntMap->domain);
        sts = odm_create_xtnt_rec ( bfap,
                                    baseXtntMap->allocVdIndex,
                                    modSubXtntMapp,
                                    0,  /* no clone xfer xtnts here */
                                    ftxH);
        if ( sts != EOK ) {
            RAISE_EXCEPTION(sts);
        }

        MS_SMP_ASSERT(modSXMapCnt > 0);
        sts = bmt_link_mcells ( bfap->dmnP,
                                bfap->tag,
                                modXtntMap->subXtntMap[modSXMapCnt-1].vdIndex,
                                modXtntMap->subXtntMap[modSXMapCnt-1].mcellId,
                                modSubXtntMapp->vdIndex,
                                modSubXtntMapp->mcellId,
                                modSubXtntMapp->vdIndex,
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
    savVdI = modXtntMap->subXtntMap[modSXMapCnt - 1].vdIndex;
    savBaseMC = baseXtntMap->subXtntMap[baseSXMapCnt].mcellId;
    savBaseVdI = baseXtntMap->subXtntMap[baseSXMapCnt].vdIndex;
    savBaseCnt = baseSXMapCnt;
    for ( ; baseSXMapCnt < baseXtntMap->cnt; baseSXMapCnt++, modSXMapCnt++ ) {
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
                          savVdI,
                          savMC,
                          savBaseVdI,
                          savBaseMC,
                          baseXtntMap->subXtntMap[baseXtntMap->cnt - 1].vdIndex,
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
    modXtntMap->hdrVdIndex = modXtntMap->subXtntMap[0].vdIndex;
    modXtntMap->hdrMcellId = modXtntMap->subXtntMap[0].mcellId;
    modXtntMap->allocVdIndex = baseXtntMap->allocVdIndex;
    modXtntMap->nextValidPage = baseXtntMap->nextValidPage;

    /* mcellCnt is the count of mcells in the BSR_SHADOW_XTNTS record. */
    /* modXtntMap->cnt is what the count should be. */
    if ( modXtntMap->cnt != mcellCnt ) {
        sts = update_mcell_cnt ( bfap->dmnP,
                                 bfap->tag,
                                 modXtntMap->hdrVdIndex,
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
    replXtntMap->hdrVdIndex = replXtntMap->subXtntMap[0].vdIndex;
    replXtntMap->hdrMcellId = replXtntMap->subXtntMap[0].mcellId;
    replXtntMap->allocVdIndex = baseXtntMap->allocVdIndex;

    *retReplXtntMap = replXtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if (extent_map_locked) {
        XTNMAP_UNLOCK(&bfap->xtntMap_lk);
    }

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
 * are described by "newXtntMap" and "replXtntMap".  The page range described
 * by the overlay extent map must be a subset of the page range described by
 * the base extent map.
 *
 * The overlay extent map describes real and, optionally, hole extents. If hole
 * extents are described, they cannot be the first or last extents described by
 * the map.  For every real extent described by the overlay extent map, the
 * extent's page range must be described by real extents in the base extent map.
 *
 * This function assumes the base and overlay extent maps are as described by
 * the previous paragraphs.
 *
 * The new extent map describes pages mapped by both the base and the overlay
 * extent maps.  The pages from the base extent map are those that do not
 * intersect with real pages described by the overlay extent map.  The pages
 * from the overlay extent map are those real pages that do intersect with
 * real pages described by the base extent map.
 *
 * The replace extent map describes pages mapped by the base extent map that
 * intersect with real pages mapped by the overlay extent map.
 *
 * The caller must have exclusive access to the overlay and base extent maps.
 */
statusT
imm_overlay_xtnt_map (
                      bsInMemXtntMapT *baseXtntMap,  /* in */
                      bsInMemXtntMapT *overXtntMap,  /* in */
                      bsXtntMapTypeT bsXtntMapType,     /* in */
                      bsInMemXtntMapT **retNewXtntMap,  /* out */
                      bsInMemXtntMapT **retReplXtntMap  /* out */
                      )
{
    bsXtntDescT baseDesc;
    bsInMemXtntDescIdT baseDescId;
    bsXtntT bsXA[2];
    uint32T currOverXtnt;
    bsInMemXtntMapT *newXtntMap = NULL;
    uint32T nextBaseXtnt;
    bsXtntDescT overDesc;
    bsInMemXtntDescIdT overDescId;
    bsXtntT part1BsXA;
    bsXtntT part2BsXA;
    bsInMemXtntMapT *replXtntMap = NULL;
    statusT sts;
    vdIndexT vdIndex;
    uint32T newtype,
            newmax;
    uint dmnVersion;

    imm_get_first_xtnt_desc (overXtntMap, &overDescId, &overDesc);

    /*
     * We know that the first replace sub extent map has the same
     * vd index as the first intersecting base sub extent map.  So,
     * initialize the first sub extent map here.
     */
    sts = imm_create_xtnt_map (
                               baseXtntMap->blksPerPage,
                               baseXtntMap->domain,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &replXtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    imm_get_xtnt_desc( overDesc.pageOffset,
                       baseXtntMap,
                       0,
                       &baseDescId,
                       &baseDesc);

    /*
     * If this file can have extent information in the primary mcell,
     * make the first subextent map type BSR_XTNTS.  Otherwise, make
     * it BSR_SHADOW_XTNTS.
     */
    dmnVersion = replXtntMap->domain->dmnVersion;
    if ( FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, replXtntMap->hdrType) ) {
        newtype = BSR_XTNTS;
        newmax = BMT_XTNTS;
    } else {
        newtype = BSR_SHADOW_XTNTS;
        newmax = BMT_SHADOW_XTNTS;
    }
    sts = imm_init_sub_xtnt_map( &(replXtntMap->subXtntMap[0]),
                                 overDesc.pageOffset,
                                 0,  /* pageCnt */
                                 baseDesc.volIndex,
                                 bsNilMCId,
                                 newtype,
                                 newmax,
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
                               baseXtntMap->blksPerPage,
                               baseXtntMap->domain,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &newXtntMap
                               );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    imm_get_first_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);

    bsXA[1].vdBlk = XTNT_TERM;

    /*
     * STEP 1
     */

    /*
     * Copy base descriptors to the new extent map until we find a
     * descriptor that describes a page range that intersects with
     * the first overlay extent's page range.
     */

    nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
    while ((nextBaseXtnt <= overDesc.pageOffset) && (nextBaseXtnt != 0)) {

        bsXA[0].bsPage = baseDesc.pageOffset;
        bsXA[0].vdBlk = baseDesc.blkOffset;
        bsXA[1].bsPage = nextBaseXtnt;
        sts = imm_copy_xtnt_descs( baseDesc.volIndex,
                                   &(bsXA[0]),
                                   1,
                                   FALSE,
                                   newXtntMap,
                                   bsXtntMapType);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
        nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
    }

    if (baseDesc.pageOffset < overDesc.pageOffset) {
        /*
         * The current base extent's page offset and the first overlay extent's
         * page offset are different.  Copy part of the base descriptor to the
         * new extent map.
         */
        bsXA[0].bsPage = baseDesc.pageOffset;
        bsXA[0].vdBlk = baseDesc.blkOffset;
        imm_split_desc (
                        overDesc.pageOffset,
                        baseXtntMap->blksPerPage,
                        &(bsXA[0]),
                        &part1BsXA,
                        &part2BsXA
                        );
        bsXA[1].bsPage = part2BsXA.bsPage;
        sts = imm_copy_xtnt_descs( baseDesc.volIndex,
                                   &(bsXA[0]),
                                   1,
                                   FALSE,
                                   newXtntMap,
                                   bsXtntMapType);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        baseDesc.pageOffset = part2BsXA.bsPage;
        baseDesc.pageCnt = nextBaseXtnt - baseDesc.pageOffset;
        baseDesc.blkOffset = part2BsXA.vdBlk;
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

            bsXA[0].bsPage = overDesc.pageOffset;
            bsXA[0].vdBlk = overDesc.blkOffset;
            bsXA[1].bsPage = overDesc.pageOffset + overDesc.pageCnt;
            sts = imm_copy_xtnt_descs (
                                       overDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            /*
             * Save the start of the next overlay extent which is about to
             * become the current extent.  The start is either the start of
             * the next real/hole extent or the next page after the last valid
             * overlay page.
             */
            currOverXtnt = overDesc.pageOffset + overDesc.pageCnt;

            imm_get_next_xtnt_desc (overXtntMap, &overDescId, &overDesc);
        } while (overDesc.blkOffset != -1);

        /*
         * Set the current overlay extent descriptor's page offset.  This
         * only has an effect when we ran out of overlay extent descriptors.
         * (imm_get_next_xtnt_desc() returns a page offset of 0 when there
         * are no more descriptors.)
         */
        overDesc.pageOffset = currOverXtnt;

        /*
         * Copy base descriptors to the replace extent map until we find a
         * descriptor that describes a page range that intersects with
         * the next overlay extent's page range or we run out of base extent
         * descriptors.
         */

        nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        while ( nextBaseXtnt <= overDesc.pageOffset && baseDesc.pageCnt > 0 ) {
            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            bsXA[1].bsPage = nextBaseXtnt;
            sts = imm_copy_xtnt_descs( baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       replXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
            nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        }

        if (baseDesc.pageCnt == 0) {
            /*
             * No more base extent descriptors.
             */
            break;  /* out of while */
        }

        if (baseDesc.pageOffset < overDesc.pageOffset) {
            /*
             * The current base extent's page offset and the current overlay
             * extent's page offset are different.  Copy part of the base
             * descriptor to the replace extent map.
             */
            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            imm_split_desc (
                            overDesc.pageOffset,
                            baseXtntMap->blksPerPage,
                            &(bsXA[0]),
                            &part1BsXA,
                            &part2BsXA
                            );
            bsXA[1].bsPage = part2BsXA.bsPage;
            sts = imm_copy_xtnt_descs (
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       replXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            baseDesc.pageOffset = part2BsXA.bsPage;
            baseDesc.pageCnt = nextBaseXtnt - baseDesc.pageOffset;
            baseDesc.blkOffset = part2BsXA.vdBlk;
        }

        if (overDesc.pageCnt == 0) {
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
         * descriptor that describes a page range that intersects with
         * the next overlay extent's page range.
         */

        nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        while ((nextBaseXtnt <= overDesc.pageOffset) && (nextBaseXtnt != 0)) {

            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            bsXA[1].bsPage = nextBaseXtnt;
            sts = imm_copy_xtnt_descs (
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
            nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        }

        if (baseDesc.pageOffset < overDesc.pageOffset) {
            /*
             * FIX - this leg isn't tested yet.
             */
            RAISE_EXCEPTION (ENOT_SUPPORTED);
            /*
             * The current base extent's page offset and the current overlay
             * extent's page offset are different.  Copy part of the base
             * descriptor to the new extent map.
             */
            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            imm_split_desc (
                            overDesc.pageOffset,
                            baseXtntMap->blksPerPage,
                            &(bsXA[0]),
                            &part1BsXA,
                            &part2BsXA
                            );
            bsXA[1].bsPage = part2BsXA.bsPage;
            sts = imm_copy_xtnt_descs (
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            baseDesc.pageOffset = part2BsXA.bsPage;
            baseDesc.pageCnt = nextBaseXtnt - baseDesc.pageOffset;
            baseDesc.blkOffset = part2BsXA.vdBlk;
        }

    }  /* end while */

    /*
     * STEP 4
     */

    /*
     * If there are more base descriptors, copy them to the new extent map.
     */

    if (baseDesc.pageCnt != 0) {

        bsXA[0].bsPage = baseDesc.pageOffset;
        bsXA[0].vdBlk = baseDesc.blkOffset;
        bsXA[1].bsPage = baseDesc.pageOffset + baseDesc.pageCnt;
        sts = imm_copy_xtnt_descs( baseDesc.volIndex,
                                   &(bsXA[0]),
                                   1,
                                   FALSE,
                                   newXtntMap,
                                   bsXtntMapType);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
        while (baseDesc.pageCnt != 0) {

            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            bsXA[1].bsPage = baseDesc.pageOffset + baseDesc.pageCnt;
            sts = imm_copy_xtnt_descs (
                                       baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
        }
    }

    *retNewXtntMap = newXtntMap;
    *retReplXtntMap = replXtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if (newXtntMap != NULL) {
        imm_delete_xtnt_map (newXtntMap);
    }
    if (replXtntMap != NULL) {
        imm_delete_xtnt_map (replXtntMap);
    }

    return sts;

}  /* end imm_overlay_xtnt_map */

/*
 * imm_merge_xtnt_map
 *
 * This function merges "overXtntMap" onto the "baseXtntMap".  The results
 * are described by "newXtntMap".
 *
 * The overlay (clone) extent map describes real and, optionally, hole extents.
 * The base (original) map describes real and, optionally, hole extents.
 *
 * The new extent map describes pages mapped by both the base and the overlay
 * extent maps.  The pages from the base extent map are those that do not
 * intersect with real pages described by the overlay extent map.  The pages
 * from the overlay extent map are those real pages that do intersect with
 * real pages described by the base extent map.
 *
 * The caller must have exclusive access to the overlay and base extent maps.
 *
 * Algorithm:
 *      create new map
 *      Loop
 *         base to new until overlay found
 *         if overlay found, split base
 *                           copy base half to new
 *         overlay to new until hole
 *         move base ptr
 *         exit on no more base map entries
 *         split base
 *         exit on no more overlay map entries
 *      End loop
 *      clean up any more base
 *             or
 *      clean up any more overlays
 */
statusT
imm_merge_xtnt_map (
                    bfAccessT *bfap,                  /* in */
                    bsInMemXtntMapT *baseXtntMap,     /* in */
                    bfAccessT *cloneBfap,             /* in */
                    bsInMemXtntMapT *overXtntMap,     /* in */
                    bsInMemXtntMapT **retNewXtntMap   /* out */
                    )
{
    bsXtntDescT baseDesc;
    bsInMemXtntDescIdT baseDescId;
    bsXtntT bsXA[2];
    uint32T currOverXtnt;
    bsInMemXtntMapT *newXtntMap = NULL;
    uint32T nextBaseXtnt;
    bsXtntDescT overDesc;
    bsInMemXtntDescIdT overDescId;
    bsXtntT part1BsXA;
    bsXtntT part2BsXA;
    statusT sts;
    vdIndexT vdIndex;
    bsXtntMapTypeT bsXtntMapType = bfap->xtnts.type;
    bsInMemSubXtntMapT *sxmP;
    uint xI;

    imm_get_first_xtnt_desc (overXtntMap, &overDescId, &overDesc);

    /* If overlay map contains a fragment only (and nothing else) - return base.
     * This condition arises when only the fragment in the original is
     * modified and the original only had a fragment or a single
     * block + fragment to begin with.
    */
    if ( (overDesc.pageCnt == 0) &&
         (overDesc.pageOffset == 0) &&
         (overDesc.blkOffset == -1) )
    {
        /* Clone file has no extents.  It may have a frag. */
        sts = imm_copy_xtnt_map( bfap, baseXtntMap, &newXtntMap);

        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        if ( cloneBfap->fragState == FS_FRAG_VALID ) {
            /*
             * Clone file consists only of a frag.
             */
            imm_get_xtnt_desc( cloneBfap->fragPageOffset, newXtntMap, FALSE,
                               &overDescId, &overDesc );
            xI = overDescId.xtntDescIndex;
            if ( overDesc.volIndex != bsNilVdIndex ) {
                if ( overDescId.subXtntMapIndex < newXtntMap->validCnt - 1 ) {
                    newXtntMap->validCnt = overDescId.subXtntMapIndex + 1;
                }
                sxmP = &newXtntMap->subXtntMap[overDescId.subXtntMapIndex];
                MS_SMP_ASSERT(sxmP->cnt > 1);
                if ( overDesc.pageCnt > 1 ) {
                    sxmP->bsXA[xI + 1].bsPage = cloneBfap->fragPageOffset;
                    if ( xI < sxmP->cnt - 1 ) {
                        sxmP->cnt = xI + 2;
                        sxmP->bsXA[xI + 1].vdBlk = XTNT_TERM;
                    }
                } else {
                    sxmP->cnt = xI + 1;
                    sxmP->bsXA[xI].vdBlk = XTNT_TERM;
                }
            }
        }

        *retNewXtntMap = newXtntMap;

        return EOK;
    }

    /* get the first non-hole extent descriptor */
    while ( overDesc.blkOffset == -1 ) {
        imm_get_next_xtnt_desc (overXtntMap, &overDescId, &overDesc);
    }

    imm_get_first_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);

    /* If base map contains a fragment only (and nothing else) - return overlay.
     * Condition arises when the original is truncated into the first block.
     */
    if ( (baseDesc.pageCnt == 0) &&
         (baseDesc.pageOffset == 0) &&
         (baseDesc.blkOffset == -1) )
    {
        sts = imm_copy_xtnt_map(bfap, overXtntMap, &newXtntMap);

        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        *retNewXtntMap = newXtntMap;

        return EOK;
    }

    /*
     * We don't know the vd index of the first entry in the new extent
     * map.  So, let imm_copy_xtnt_descs() figure it out.
     */
    sts = imm_create_xtnt_map( baseXtntMap->blksPerPage,
                               baseXtntMap->domain,
                               1,  /* maxCnt */
                               0,  /* termPage - not used */
                               0,  /* termVdIndex - not used */
                               &newXtntMap);
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    bsXA[1].vdBlk = XTNT_TERM;

    while (1) {
        /*
         * STEP 1
         */

        /*
         * Copy base descriptors to the new extent map until we find a
         * descriptor that describes a page range that intersects with
         * the first or next overlay extent's page range.
         */

        nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        while ((nextBaseXtnt <= overDesc.pageOffset) && (nextBaseXtnt != 0)) {

            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            bsXA[1].bsPage = nextBaseXtnt;
            sts = imm_copy_xtnt_descs( baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            newXtntMap->validCnt = newXtntMap->cnt;
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
            nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        }

        if (baseDesc.pageOffset < overDesc.pageOffset) {

            /*
             * STEP 2
             */
            /*
             * The current base extent's page offset and the first overlay
             * extent's page offset are different.  Copy part of the base
             * descriptor to the new extent map.
             */
            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            imm_split_desc( overDesc.pageOffset,
                            baseXtntMap->blksPerPage,
                            &(bsXA[0]),
                            &part1BsXA,
                            &part2BsXA);
            bsXA[1].bsPage = part2BsXA.bsPage;
            sts = imm_copy_xtnt_descs( baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            newXtntMap->validCnt = newXtntMap->cnt;
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            baseDesc.pageOffset = part2BsXA.bsPage;
            baseDesc.pageCnt = nextBaseXtnt - baseDesc.pageOffset;
            baseDesc.blkOffset = part2BsXA.vdBlk;
        } /* end if */

        /*
         * STEP 3
         */

        /*
         * Copy overlay descriptors to the new extent map until we find a hole
         * or we run out of overlay extent descriptors.
         */

        do {
            bsXA[0].bsPage = overDesc.pageOffset;
            bsXA[0].vdBlk = overDesc.blkOffset;
            bsXA[1].bsPage = overDesc.pageOffset + overDesc.pageCnt;
            sts = imm_copy_xtnt_descs( overDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            newXtntMap->validCnt = newXtntMap->cnt;
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            /*
             * Save the start of the next overlay extent which is about to
             * become the current extent.  The start is either the start of
             * the next real/hole extent or the next page after the last valid
             * overlay page.
             */
            currOverXtnt = overDesc.pageOffset + overDesc.pageCnt;

            imm_get_next_xtnt_desc (overXtntMap, &overDescId, &overDesc);
        } while (overDesc.blkOffset != -1);

        /*
         * Set the current overlay extent descriptor's page offset.  This
         * only has an effect when we ran out of overlay extent descriptors.
         * (imm_get_next_xtnt_desc() returns a page offset of 0 when there
         * are no more descriptors.)
         */
        overDesc.pageOffset = currOverXtnt;

        nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        while ( nextBaseXtnt <= overDesc.pageOffset && baseDesc.pageCnt > 0 ) {
            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
            nextBaseXtnt = baseDesc.pageOffset + baseDesc.pageCnt;
        }

        if (baseDesc.pageCnt == 0) {
            /*
             * No more base extent descriptors.
             */
            break;  /* out of while */
        }

        if (baseDesc.pageOffset < overDesc.pageOffset) {
            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            imm_split_desc( overDesc.pageOffset,
                            baseXtntMap->blksPerPage,
                            &(bsXA[0]),
                            &part1BsXA,
                            &part2BsXA);
            bsXA[1].bsPage = part2BsXA.bsPage;
            baseDesc.pageOffset = part2BsXA.bsPage;
            baseDesc.pageCnt = nextBaseXtnt - baseDesc.pageOffset;
            baseDesc.blkOffset = part2BsXA.vdBlk;
        }

        if (overDesc.pageCnt == 0) {
            /*
             * No more overlay extent descriptors.
             */
            break;  /* out of while */
        }

        imm_get_next_xtnt_desc (overXtntMap, &overDescId, &overDesc);
    }  /* end while */

    /*
     * STEP 4
     */

    /*
     * If there are more base descriptors, copy them to the new extent map.
     */

    if (baseDesc.pageCnt != 0) {
        do {
            bsXA[0].bsPage = baseDesc.pageOffset;
            bsXA[0].vdBlk = baseDesc.blkOffset;
            bsXA[1].bsPage = baseDesc.pageOffset + baseDesc.pageCnt;

            if ( cloneBfap->fragState == FS_FRAG_VALID ) {
                /*
                 * Don't copy orig extents past a clone fragment.
                 * If a clone fragment and a orig extent are at the same
                 * page the merged extent map ahould have the clone fragment.
                 */
                MS_SMP_ASSERT(cloneBfap->fragPageOffset >= overDesc.pageOffset);
                if ( cloneBfap->fragPageOffset == bsXA[0].bsPage ) {
                    break;
                }
                if ( cloneBfap->fragPageOffset < bsXA[1].bsPage ) {
                    bsXA[1].bsPage = cloneBfap->fragPageOffset;
                }
            }

            sts = imm_copy_xtnt_descs( baseDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            newXtntMap->validCnt = newXtntMap->cnt;
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (baseXtntMap, &baseDescId, &baseDesc);
        } while (baseDesc.pageCnt != 0) ;
    }

    /*
     * STEP 5
     */

    /*
     * If there are more overlay descriptors, copy them to the new extent map.
     */

    if (overDesc.pageCnt != 0) {
        do {
            bsXA[0].bsPage = overDesc.pageOffset;
            bsXA[0].vdBlk = overDesc.blkOffset;
            bsXA[1].bsPage = overDesc.pageOffset + overDesc.pageCnt;
            sts = imm_copy_xtnt_descs( overDesc.volIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       newXtntMap,
                                       bsXtntMapType);
            newXtntMap->validCnt = newXtntMap->cnt;
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            imm_get_next_xtnt_desc (overXtntMap, &overDescId, &overDesc);
        } while (overDesc.pageCnt != 0) ;
    }

    *retNewXtntMap = newXtntMap;

    return EOK;

HANDLE_EXCEPTION:

    if (newXtntMap != NULL) {
        imm_delete_xtnt_map (newXtntMap);
        newXtntMap = NULL;
    }

    return sts;
} /* end imm_merge_xtnt_map */

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
    sts = imm_create_xtnt_map( srcXtntMap->blksPerPage,
                               srcXtntMap->domain,
                               cnt,
                               0,  /* termPage - not used */
                               bsNilVdIndex,  /* termVdIndex - not used */
                               &xtntMap);
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
    }

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
 * imm_remove_page_map
 *
 * This function removes the specified page range's mapping from the extent
 * map.  If the descriptors that describe the page range are in the middle
 * of the extent map, the descriptors are replaced with a hole descriptor.
 * If the hole descriptor is adjacent to other hole descriptors, the holes
 * are merged.
 *
 * If the descriptors that describe the page range are at the end of the
 * extent map, the descriptors are replaced with the termination descriptor.
 * If the termination descriptor is adjacent to a hole descriptor, the
 * hole descriptor is changed to the termination descriptor.
 *
 * This function does not modify the original extent map.  Instead,
 * it uses the sub extent maps at the end of the extent map to hold
 * the results of the remove operation.  It sets the extent map's
 * "updateStart" and "updateEnd" fields to denote the original
 * sub extent maps without the page mappings.  It sets the extent
 * map's "origStart" and "origEnd" fields to denote which original
 * sub extent maps contain the to-be-removed mappings.
 */
statusT
imm_remove_page_map (
                     uint32T bfPageOffset,  /* in */
                     uint32T bfPageCnt,  /* in */
                     bsInMemXtntMapT *xtntMap,  /* in */
                     bsXtntMapTypeT bsXtntMapType,  /* in */
                     uint32T *remPageOffset,  /* out */
                     uint32T *remPageCnt  /* out */
                     )
{
    bsXtntT bsXA[2];
    bsXtntT desc;
    uint32T descEnd;
    uint32T descStart;
    uint32T endPageOffset;
    uint32T i;
    uint32T lastPageOffset;
    uint32T pageOffset;
    uint32T stickyHole=FALSE;
    statusT sts;
    bsInMemSubXtntMapT *subXtntMap;
    lbnT blk;

    MS_SMP_ASSERT(xtntMap->cnt == xtntMap->validCnt);

    pageOffset = bfPageOffset;
    endPageOffset = (bfPageOffset + bfPageCnt) - 1;

    /*
     * Get the start index and page offset.
     */

    sts = imm_page_to_sub_xtnt_map(pageOffset,
                                   xtntMap,
                                   0,
                                   &xtntMap->origStart);
    if (sts != EOK) {
        return sts;
    }

    xtntMap->updateStart=0;
    xtntMap->updateEnd=0;

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

    sts = imm_page_to_xtnt( pageOffset,
                            subXtntMap,
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_PAGE_HOLE */
                            &descStart);
    if (sts == EOK) {
        MS_SMP_ASSERT(descStart < subXtntMap->cnt - 1);
        if (pageOffset == subXtntMap->bsXA[descStart].bsPage) {
            /*
             * The page range's starting page offset is the first
             * page described by the descriptor.  If the previous
             * descriptor describes a hole, reset the start page
             * offset.
             */
            if (descStart > 0) {
                blk = subXtntMap->bsXA[descStart - 1].vdBlk;
                if ( blk == XTNT_TERM || blk == PERM_HOLE_START ) {
                    /*
                     * The previous descriptor describes a hole.
                     */
                    descStart--;
                    pageOffset = subXtntMap->bsXA[descStart].bsPage;

                    /* Now check if the subxtnt map prior to us is the primary
                     * and pick it up of we can.
                     */
                    if ( (descStart == 0) && (xtntMap->origStart == 1) ) {
                        subXtntMap = &(xtntMap->subXtntMap[0]);
                        if ( subXtntMap->cnt == 1 ) {

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

                            MS_SMP_ASSERT(subXtntMap->bsXA[0].vdBlk ==
                                          XTNT_TERM);
                            xtntMap->origStart--;
                            descStart=0;
                            pageOffset=0;
                        }
                    }
                }
            } else {
                /* Removing storage from first extent in subextent map. */
                while ( xtntMap->origStart > 0 ) {
                    /*
                     * This loop is mainly for cleaning up corruption left
                     * over from older kernels. The exception here is picking
                     * up an orphan primary extent.
                     */

                    subXtntMap =&(xtntMap->subXtntMap[xtntMap->origStart - 1]);

                    if ( subXtntMap->cnt > 1 ) {
                        blk = subXtntMap->bsXA[subXtntMap->cnt - 2].vdBlk;
                        if ( blk == XTNT_TERM || blk == PERM_HOLE_START ) {
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
                            pageOffset = subXtntMap->bsXA[descStart].bsPage;
                        }
                        break;
                    } else {
                        /* subXtntMap->cnt == 1 */
                        /* The only 1 entry subxtnt allowed is the
                         * primary. We are removing storage from the
                         * begining of the file so include this in
                         * the update range in hopes that we can reuse
                         * it.
                         *
                         * OR
                         *
                         * Version 3 domains may have left over on-disk
                         * corruption from older kernels. This seems to show
                         * itself as orphan extents (each with the same starting
                         * page) ultimately preceeded by an extent made up of
                         * only a hole. Clean this mess up by including these
                         * extents in the range being removed */

                        MS_SMP_ASSERT(subXtntMap->bsXA[0].vdBlk == XTNT_TERM);
                        xtntMap->origStart--;
                        descStart=0;
                    }
                }
            }
        }
    } else {  /* sts != EOK */
        if (sts == E_PAGE_HOLE) {
            /*
             * The first page of the page range is in a hole.
             *
             * Note:  This code assumes that two adjacent descriptors cannot
             * both describe a hole.
             */
            pageOffset = subXtntMap->bsXA[descStart].bsPage;
            if ( (descStart == 0) && (xtntMap->origStart == 1) ) {
                subXtntMap = &(xtntMap->subXtntMap[0]);
                if (subXtntMap->cnt == 1) {
                    /* The only 1 entry subxtnt allowed is the
                     * primary. We are removing storage from the
                     * begining for the file so include this in the
                     * update range in hopes that we can reuse it.  */

                    MS_SMP_ASSERT(subXtntMap->bsXA[0].vdBlk == XTNT_TERM);
                    xtntMap->origStart--;
                    descStart=0;
                    pageOffset=0;
                }
            }
        } else {
            return sts;
        }
    }

    /*
     * Get the end index and page offset.
     */

    sts = imm_page_to_sub_xtnt_map( endPageOffset,
                                    xtntMap,
                                    0,
                                    &xtntMap->origEnd);
    if (sts != EOK) {
        return sts;
    }
    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
    sts = imm_page_to_xtnt( endPageOffset,
                            subXtntMap,
                            FALSE,   /* Don't use the update maps */
                            FALSE,   /* perm hole returns E_PAGE_HOLE */
                            &descEnd);
    if (sts == EOK) {
        MS_SMP_ASSERT(descEnd < subXtntMap->cnt - 1);
        if (endPageOffset == (subXtntMap->bsXA[descEnd + 1].bsPage - 1)) {
            /*
             * The page range's ending page offset is the last
             * page described by the descriptor.  If the next
             * descriptor describes a hole, reset the end page
             * offset.
             */
            if (descEnd != (subXtntMap->cnt - 2)) {
                blk = subXtntMap->bsXA[descEnd + 1].vdBlk;
                if ( blk == XTNT_TERM || blk == PERM_HOLE_START ) {
                    /*
                     * The next descriptor describes a hole.
                     */
                    descEnd++;
                    endPageOffset = subXtntMap->bsXA[descEnd + 1].bsPage - 1;
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

                        blk = subXtntMap->bsXA[0].vdBlk;
                        if ( blk == XTNT_TERM || blk == PERM_HOLE_START ) {
                            /* The next subxtnt map starts with a hole.
                             * Ignore it by bumping descEnd so we don't
                             * copy it. We will pick it up by increasing the
                             * endPageOffset
                             */

                            endPageOffset = subXtntMap->bsXA[1].bsPage - 1;
                            descEnd = 1;
                        }
                    }
                }
            }
        }
    } else {/* sts != EOK */
        if (sts == E_PAGE_HOLE) {
            /*
             * The last page of the page range is in a hole.
             *
             * Note:  This code assumes that two adjacent descriptors cannot
             * both describe a hole.
             */
            endPageOffset = subXtntMap->bsXA[descEnd + 1].bsPage - 1;
            descEnd++;
            stickyHole=TRUE;

        } else {
            return sts;
        }
    }

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->validCnt - 1]);
    lastPageOffset = subXtntMap->bsXA[subXtntMap->cnt - 1].bsPage - 1;

    subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

    /*
     * For some reason still unknown the subXtntMap
     * for the frag file was NULL, this caused a KMF when it got
     * to the next if conditional.
     *
     */
    if (NULL == subXtntMap->bsXA) {
        domain_panic(xtntMap->domain,
                     "imm_remove_page_map: subXtntMap->bsXA == NULL");
        sts = E_DOMAIN_PANIC;
        RAISE_EXCEPTION( E_DOMAIN_PANIC );
    }

    /* Check to see if the range we are removing starts in the middle
     * of the first subextent or if we are removing all the pages.
     */
    if ( (pageOffset > subXtntMap->bsXA[0].bsPage) ||
         ((endPageOffset == lastPageOffset) && (pageOffset == 0)) ||
         (subXtntMap->type == BSR_XTNTS) ||
         (subXtntMap->type == BSR_SHADOW_XTNTS) )
    {
        /*
         * Initialize the first update sub extent map.  It has the same
         * characteristics as the first original sub extent map.
         */

        /*
         * Make room for the update subextent map(s).
         */

        if (xtntMap->cnt >= xtntMap->maxCnt) {
            sts = imm_extend_xtnt_map (xtntMap);
            if (sts != EOK) {
                return sts;
            }

            subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);
        }


        sts = imm_init_sub_xtnt_map( &(xtntMap->subXtntMap[xtntMap->cnt]),
                                     subXtntMap->pageOffset,
                                     0,  /* pageCnt */
                                     subXtntMap->vdIndex,
                                     bsNilMCId,
                                     subXtntMap->type,
                                     subXtntMap->onDiskMaxCnt,
                                     0);  /* force default maxCnt */
        if (sts != EOK) {
            return sts;
        }

        xtntMap->updateStart = xtntMap->cnt;
        xtntMap->cnt++;

        /* Copy the descriptors from the first original sub extent map
         * that describe pages that are not part of the removal range
         * and are positioned before the range.  */

        sts = imm_copy_xtnt_descs( subXtntMap->vdIndex,
                                   subXtntMap->bsXA,
                                   descStart,
                                   FALSE,
                                   xtntMap,
                                   bsXtntMapType);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);
        if (pageOffset != subXtntMap->bsXA[descStart].bsPage) {

            /* The start page offset is not aligned on an extent
             * boundary.  */

            imm_split_desc( pageOffset,
                            xtntMap->blksPerPage,
                            &(subXtntMap->bsXA[descStart]),  /* orig */
                            &(bsXA[0]),  /* first part */
                            &desc);  /* last part */
            bsXA[1].bsPage = desc.bsPage;
            bsXA[1].vdBlk = XTNT_TERM;
            sts = imm_copy_xtnt_descs( subXtntMap->vdIndex,
                                       &(bsXA[0]),
                                       1,
                                       FALSE,
                                       xtntMap,
                                       bsXtntMapType);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origStart]);

        if ( ((subXtntMap->type == BSR_XTNTS) ||
              (subXtntMap->type == BSR_SHADOW_XTNTS)) &&
             (subXtntMap->cnt > 1) &&
             (pageOffset == subXtntMap->bsXA[0].bsPage) &&
             (endPageOffset >= subXtntMap->bsXA[1].bsPage) )
        {
            /* Our starting subextent lives on the primary. We
             * must force it to be an orphan extent so that the
             * primary mcell can be reused in dealloc_stg
             */
             subXtntMap = &(xtntMap->subXtntMap[xtntMap->updateStart]);
             subXtntMap->bsXA[0].vdBlk=XTNT_TERM;
             subXtntMap->cnt=1;
        }

        xtntMap->updateEnd=xtntMap->cnt - 1;
    }

    if (endPageOffset != lastPageOffset) {
        /*
         * The page range is not at the end of the mapped pages.
         * The hole we are creating must stick to the storage
         * following it.
         */

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);

        if ( (xtntMap->origStart != xtntMap->origEnd) ||
             (xtntMap->updateStart == 0) ||
             (xtntMap->origStart == 0) )
        {

            /* We need a new subxtnt if the range we are removing
             * ends on a differnt one or we didn't get one already
             */

            if (xtntMap->cnt >= xtntMap->maxCnt) {
                sts = imm_extend_xtnt_map (xtntMap);
                if (sts != EOK) {
                    return sts;
                }
                subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
            }

            sts = imm_init_sub_xtnt_map( &(xtntMap->subXtntMap[xtntMap->cnt]),
                                         pageOffset,
                                         0,  /* pageCnt */
                                         subXtntMap->vdIndex,
                                         bsNilMCId,
                                         BSR_XTRA_XTNTS,
                                         BMT_XTRA_XTNTS,
                                         0);  /* force default maxCnt */
            if (sts != EOK) {
                return sts;
            }

            if ( xtntMap->updateStart == 0 ) {
                xtntMap->updateStart = xtntMap->cnt;
            }

            xtntMap->cnt++;
        }
        /*
         * Create a hole descriptor and copy the remaining descriptors.
         */

        bsXA[0].bsPage = pageOffset;
        bsXA[0].vdBlk = XTNT_TERM;
        bsXA[1].bsPage = endPageOffset + 1;
        bsXA[1].vdBlk = XTNT_TERM;
        sts = imm_copy_xtnt_descs( subXtntMap->vdIndex,
                                   &(bsXA[0]),
                                   1,
                                   TRUE,
                                   xtntMap,
                                   bsXtntMapType);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /*
         * Copy the descriptors from the last original sub extent map that
         * describe pages that are not part of the removal range and are
         * positioned after the range.
         */

        subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
        if (!stickyHole) {
            if (endPageOffset != (subXtntMap->bsXA[descEnd + 1].bsPage - 1)) {
                /*
                 * The end page offset is not aligned on an extent boundary.
                 */
                imm_split_desc( endPageOffset + 1,
                                xtntMap->blksPerPage,
                                &(subXtntMap->bsXA[descEnd]),  /* orig */
                                &desc,  /* first part */
                                &(bsXA[0]));  /* last part */
                bsXA[1].bsPage = subXtntMap->bsXA[descEnd + 1].bsPage;
                bsXA[1].vdBlk = XTNT_TERM;
                sts = imm_copy_xtnt_descs( subXtntMap->vdIndex,
                                           &(bsXA[0]),
                                           1,
                                           FALSE,
                                           xtntMap,
                                           bsXtntMapType);
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }
                subXtntMap = &(xtntMap->subXtntMap[xtntMap->origEnd]);
            }
            descEnd++;
        }

        sts = imm_copy_xtnt_descs( subXtntMap->vdIndex,
                                   &(subXtntMap->bsXA[descEnd]),
                                   (subXtntMap->cnt - 1) - descEnd,
                                   TRUE,
                                   xtntMap,
                                   bsXtntMapType);
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
    }

    *remPageOffset = pageOffset;
    *remPageCnt = (endPageOffset + 1) - pageOffset;

    return sts;

HANDLE_EXCEPTION:

    xtntMap->updateEnd = xtntMap->cnt - 1;

    return sts;
}  /* end imm_remove_page_map */

#ifdef ADVFS_SMP_ASSERT
/*
** Check the xtnt map for invalid configurattions.
** Each extent page in a subextent map must be > the last extent page.
** The starting page of each subextent map must be the same as the ending
** page of the previous subextent map. The starting page of the first
** subextent map must be 0.
** Each subextent pageCnt is the difference between the first page in the
** subextent map and the last page in the subextent map.
** This routine is under ADVFS_SMP_ASSERT since it's only used in asserts.
*/
imm_check_xtnt_map(bsInMemXtntMapT *xtntMap)
{
    bsInMemSubXtntMapT *subXtntMap;
    uint32T lastPg = 0;
    uint32T page;
    uint32T block;
    uint32T pgs;
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

        /* First part of this ASSERT works in all cases except the
         * miscellaneous bitfile; check for valid misc bitfile extents
         * in the second half. The check for BFM_MISC works for both
         * Version 3 and Version 4 domains.
         */
        MS_SMP_ASSERT((subXtntMap->maxCnt <= subXtntMap->onDiskMaxCnt) ||
                      ( subXtntMap->mcellId.page == 0         &&
                        subXtntMap->mcellId.cell == BFM_MISC  &&
                        subXtntMap->type == BSR_XTNTS         &&
                        subXtntMap->cnt  == 3                 &&
                        subXtntMap->bsXA[0].bsPage == 0       &&
                        subXtntMap->bsXA[0].vdBlk  == 0       &&
                        subXtntMap->bsXA[1].bsPage == 2       &&
                        subXtntMap->bsXA[1].vdBlk  == 0x40    &&
                        subXtntMap->bsXA[2].bsPage == 4       &&
                        subXtntMap->bsXA[2].vdBlk  == -1
                      ) );

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

            /* Only make the following assertions for version 4 AdvFS
             *  domains since older domains may have some incorrect
             *  xtnts on disk.  */

            if (xtntMap->domain->dmnVersion >= FIRST_PERM_HOLE_XTNTS_VERSION) {
                MS_SMP_ASSERT(subXtntMap->cnt > 1);
            }

            break;
          case BSR_SHADOW_XTNTS:
            MS_SMP_ASSERT(subXtntMap->onDiskMaxCnt == BMT_SHADOW_XTNTS);
            MS_SMP_ASSERT(i == 0);
            break;
          default:
            MS_SMP_ASSERT(0);
        }

        /* Newly created files come here with no extents and vdIndex == 0 */
        if ( subXtntMap->vdIndex == bsNilVdIndex ) {
            /* new files will have only one subextent map */
            MS_SMP_ASSERT(xtntMap->validCnt == 1);
            /* and it will be empty */
            MS_SMP_ASSERT(subXtntMap->cnt == 1);
            continue;
        }

        vdp = VD_HTOP(subXtntMap->vdIndex, xtntMap->domain);
        for ( j = 0; j < subXtntMap->cnt; j++ ) {
            page = subXtntMap->bsXA[j].bsPage;
            if ( j == 0 ) {
                /* test pageOffset */
                MS_SMP_ASSERT(subXtntMap->pageOffset == page);
                /* Each subextent starts where the last sub left off. */
                /* The first subextent starts at page 0. */
                MS_SMP_ASSERT(page == lastPg);
            } else {
                /* Within a subextent the pages increase monotonically. */
                MS_SMP_ASSERT(page > lastPg);
            }
            lastPg = page;

            /* Check the block */
            block = subXtntMap->bsXA[j].vdBlk;

            /* Only make the following assertions for version 4 AdvFS
             *  domains since older domains may have some incorrect
             *  xtnts on disk.  */

            if ( block == XTNT_TERM ) {
                /* Can't have 2 holes in a row, or end a mcell with a hole. */
                MS_SMP_ASSERT(!(xtntMap->domain->dmnVersion >= FIRST_PERM_HOLE_XTNTS_VERSION) ||
                              (holeFlg == FALSE));
                holeFlg = TRUE;
                permHole = FALSE;
                continue;
            }
            holeFlg = FALSE;
            if ( block == PERM_HOLE_START ) {
                /* Can't have 2 permanent holes in a row, they combine. */
                MS_SMP_ASSERT(!(xtntMap->domain->dmnVersion >= FIRST_PERM_HOLE_XTNTS_VERSION) ||
                              (permHole == FALSE));
                permHole = TRUE;
                continue;
            }
            permHole = FALSE;

            pgs = block / xtntMap->blksPerPage;
            MS_SMP_ASSERT(pgs * xtntMap->blksPerPage ==
                          subXtntMap->bsXA[j].vdBlk);

            MS_SMP_ASSERT(block < vdp->vdSize);
        }

        MS_SMP_ASSERT(subXtntMap->pageCnt ==
                      lastPg - subXtntMap->bsXA[0].bsPage);
    }

    return EOK;
}

#endif


#ifdef ADVFS_DEBUG
/*
 * imm_print_xtnt_map
 *
 * This function prints the specified in-mem extent map.
 *
 * NOTE:  This function assumes a non-striped bitfile.
 */
void
imm_print_subXtnt_map( bsInMemSubXtntMapT *subXtntMap, int index )
{
    uint32T j;
    char *type;

    switch (subXtntMap->type) {
        case BSR_XTRA_XTNTS:
            type = "BSR_XTRA_XTNTS";
            break;
        case BSR_SHADOW_XTNTS:
            type = "BSR_SHADOW_XTNTS";
            break;
        case BSR_XTNTS:
            type = "BSR_XTNTS";
            break;
        default:
            type = "UNKNOWN";
    }

    printf ("  subXtntMap %d\n", index);
    printf ("    pageOffset: %d, pageCnt: %d\n",
            subXtntMap->pageOffset, subXtntMap->pageCnt);
    printf ("    vdIndex: %d, mcellId (page.cell): %d.%d\n",
            subXtntMap->vdIndex, subXtntMap->mcellId.page,
            subXtntMap->mcellId.cell);
    printf ("    mcellState: %d, type: %s\n",
            subXtntMap->mcellState, type);
    printf ("    onDiskMaxCnt: %d, cnt: %d, maxCnt: %d\n",
            subXtntMap->onDiskMaxCnt, subXtntMap->cnt, subXtntMap->maxCnt);
    printf("    updateStart %d, updateEnd %d\n", subXtntMap->updateStart, subXtntMap->updateEnd);

    for (j = 0; j < subXtntMap->cnt; j++) {
        printf ("    bsPage: %d (0x%08x), vdBlk: %d (0x%08x)\n",
                subXtntMap->bsXA[j].bsPage, subXtntMap->bsXA[j].bsPage,
                subXtntMap->bsXA[j].vdBlk, subXtntMap->bsXA[j].vdBlk);
    }

    return;
}

void
imm_print_xtnt_map (
                    bsInMemXtntMapT *xtntMap  /* in */
                    )
{
    uint32T i;
    uint32T j;
    bsInMemSubXtntMapT *subXtntMap;
    char *type;

    for (i = 0; i < xtntMap->validCnt; i++) {
        subXtntMap = &(xtntMap->subXtntMap[i]);

        switch (subXtntMap->type) {
            case BSR_XTRA_XTNTS:
                type = "BSR_XTRA_XTNTS";
                break;
            case BSR_SHADOW_XTNTS:
                type = "BSR_SHADOW_XTNTS";
                break;
            case BSR_XTNTS:
                type = "BSR_XTNTS";
                break;
            default:
                type = "UNKNOWN";
        }

        printf ("subXtntMap %d\n", i);
        printf ("    pageOffset: %d, pageCnt: %d\n",
                subXtntMap->pageOffset, subXtntMap->pageCnt);
        printf ("    vdIndex: %d, mcellId (page.cell): %d.%d\n",
                subXtntMap->vdIndex, subXtntMap->mcellId.page,
                subXtntMap->mcellId.cell);
        printf ("    mcellState: %d, type: %s\n",
                subXtntMap->mcellState, type);
        printf ("    onDiskMaxCnt: %d, cnt: %d, maxCnt: %d\n",
                subXtntMap->onDiskMaxCnt, subXtntMap->cnt, subXtntMap->maxCnt);

        for (j = 0; j < subXtntMap->cnt; j++) {
            printf ("    bsPage: %d (0x%08x), vdBlk: %d (0x%08x)\n",
                    subXtntMap->bsXA[j].bsPage, subXtntMap->bsXA[j].bsPage,
                    subXtntMap->bsXA[j].vdBlk, subXtntMap->bsXA[j].vdBlk);
        }
    }

    return;

}  /* end imm_print_xtnt_map */
#endif ADVFS_DEBUG


/*
 * imm_get_xtnt_map_size
 *
 * This function retrieves the number of extent map entries specified
 * in the in-mem extent map.
 *
 * NOTE:  This function assumes a normal bitfile, not a striped or a reserved
 * bitfile.
 */
statusT
imm_get_xtnt_map_size (
                       bsInMemXtntMapT *xtntMap,  /* in */
                       int *size /* out */
                       )
{
    bsXtntDescT XtntDesc;
    bsInMemXtntDescIdT xtntDescId;
    uint32T i;
    int cnt=0;
    bsInMemSubXtntMapT *subXtntMap;

    for (i = 0; i < xtntMap->validCnt; i++) {

        subXtntMap = &(xtntMap->subXtntMap[i]);
        cnt = cnt + subXtntMap->cnt;

    }

    *size = cnt;

    return EOK;
}


/*
 * Merge contiguous regions in any updated subextent maps associated with
 * the specified extent.
 *
 * This is used in conjunction with the block allocation policy for NFS.
 */
u_int xtnt_compress_cnt = 0;

void
imm_compress_subxtnt_map ( bsInMemXtntT *xtnts, int bfPageSz )
{
    bsInMemXtntMapT *xtntMap;
    bsInMemSubXtntMapT *subXtntMap;
    bsXtntT *bsXA;
    uint32T cnt;
    uint32T subindx;
    uint32T updateStart;
    uint32T i, j, k;

    if (!xtnts->validFlag) {
        return;
    }

    xtnt_compress_cnt++;

    xtntMap = xtnts->xtntMap;

    for ( subindx = xtntMap->updateStart;
          subindx <= xtntMap->updateEnd;
          subindx++ )
    {
        subXtntMap = &xtntMap->subXtntMap[subindx];
        cnt = subXtntMap->cnt;
        updateStart = cnt-1;
        bsXA = subXtntMap->bsXA;

        /*
         * i = input index
         * j = output index
         * k = merge difference
         */
        for ( i = 0, j = 1; i+1 < cnt; i+=k, j++ ) {
            /* find two regions logically and physically contiguous */
            for ( k = 1;
                  i + k < cnt &&
                   (bsXA[i + k].vdBlk - bsXA[i].vdBlk) ==
                   (bsXA[i + k].bsPage - bsXA[i].bsPage) * bfPageSz;
                  k++ );

            bsXA[j].bsPage = bsXA[i+k].bsPage;
            bsXA[j].vdBlk = bsXA[i+k].vdBlk;

            if ( k > 1 && j < updateStart ) {
                updateStart = j;
            }
        }

        /* set up for writing out updated extent data */
        if (updateStart < subXtntMap->updateStart) {
            subXtntMap->updateStart = updateStart;
        }
        subXtntMap->updateEnd = j-1;
        subXtntMap->cnt = j;
    }
}


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


static void
extend_xtnt_map_array(int *cnt, struct extentmapentry **xtnt_array)
{
    int new_count, old_count;
    struct extentmapentry *new_array, *old_array;

    old_count = *cnt;
    old_array = *xtnt_array;

    /* get arbitrarily 100 more extent slots */
    new_count = old_count + 100;

    new_array = (struct extentmapentry *)
            ms_malloc(new_count * sizeof(struct extentmapentry));

    if (old_count != 0) {
        /* copy the old data to the new, extended list */
        bcopy(old_array,new_array,(old_count * sizeof(struct extentmapentry)));
        ms_free(old_array);
    }
    *cnt = new_count;
    *xtnt_array = new_array;
}
/*
 * This function is called by fcntl(), to retrieve the sparseness map
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
    statusT  sts;
    uint32T pg=0;
    uint32T nextPage=0;
    uint32T clone_nextPage=0;
    int ret,setflag=0, i=0;
    struct extentmapentry *xtnts_array;
    struct extentmap local_map;
    int use_orig_pages = 0;
    long size;
    long offset;
    int use_orig_bfap = 0;
    struct bfAccess  *orig_bfap;
    bfSetT *bfSetp;
    int num_extents = 0;
    int cloned_file = 0;
    int xtnt_cnt = 0;
    int found_stg = 0;
    long map_arraysize = 0;
    uint32T segmentSize;
    int not_sparse = 0;
    uint32T pages = 0;
    uint32T maxClonePgs = bfap->file_size / ADVFS_PGSZ + 1;
    off_t fileSize = bfap->file_size;
    uint32T allocPageCnt = bfap->xtnts.allocPageCnt;
    long allocSize = (long)allocPageCnt * ADVFS_PGSZ +
                     bfap->fragId.type * BF_FRAG_SLOT_BYTES;


    extend_xtnt_map_array(&xtnt_cnt, &xtnts_array);

    bfSetp = bfap->bfSetp;

    /* Find whether the file is sparse or not. If its not then we just
     * need to return one extent with the size equal to file size and
     * offset as zero.
     */

    if (bfSetp->cloneId != BS_BFSET_ORIG) {
        cloned_file = 1;
        orig_bfap = bfap->origAccp;
        MS_SMP_ASSERT(orig_bfap);

        if (bfap->cloneId == BS_BFSET_ORIG) {
            /*
             * This is a clone bitfile that has not had anything copied to
             * it so it still uses the orig bitfile's extent map.  Therefore,
             * use the orig bitfile's extent map.
             */
            if (fileSize < ADVFS_PGSZ) {
                not_sparse = TRUE;
            }
            use_orig_bfap = 1;

        } else if ((fileSize / ADVFS_PGSZ >= allocPageCnt) &&
                   allocSize >= fileSize)
        {
            not_sparse = TRUE;
        }
    } else if ( (fileSize / ADVFS_PGSZ >= allocPageCnt) &&
                allocSize >= fileSize )
    {
        not_sparse = TRUE;
    }
    if (not_sparse) {

        /* This is not sparse file so we have only one extent with size
         * equal to file size
         */

        if (fileSize != 0) {
            xtnts_array[0].size= fileSize;
            xtnts_array[0].offset = 0;
            num_extents = 1;
        }
        goto end;
    }

    /* file is sparse, find the sparseness map */

    while (nextPage != XTNT_TERM && nextPage != PERM_HOLE_START) {
        if (i >= xtnt_cnt) {
            extend_xtnt_map_array(&xtnt_cnt, &xtnts_array);
        }

        if (!cloned_file) {
            ret = page_is_mapped(bfap,pg,&nextPage,FALSE);
        } else if (use_orig_bfap) {
            /* Clone bfap has no extents and clone bfap->maxClonePgs */
            /* has not been set. So here we use the local maxClonePgs. */
            if ( pg < maxClonePgs ) {
                /* Don't look at orig file extents past maxClonePgs. */
                ret = page_is_mapped(orig_bfap,pg,&nextPage,FALSE);
                 /* In case of striped files when we reach end of segment
                 * nextPage will be set to -1(XTNT_TERM). So don't set nextPage to
                 * maxClonePgs when we reach end of segment.
                 */
                if (nextPage != XTNT_TERM && nextPage > maxClonePgs) {
                    /* Don't allow nextPage to progress beyond maxClonePgs. */
                    nextPage = maxClonePgs;
                }
            } else {
                ret = FALSE;
                nextPage = XTNT_TERM;
            }
        } else {
            /* when the file is cloned we look at the pages of both the
             * original and clone to get the sparseness map.
             * For the clone file look in the clone first.
             * If the page is mapped to storage OR a permanent hole then use
             * nextPage to find the end of the extent. If the page is mapped
             * to a normal hole in the clone use nextPage to save the end of
             * the hole in the clone's map. If the clone does not map the
             * page (normal hole) then look in the original file's extent
             * map. Look only up to the end of the hole in the clone's extent
             * map. Never go beyond maxClonePages since that was the size of
             * the original file when the fileset was cloned.
             */

            /* Holes and permanent holes return FALSE */
            ret = page_is_mapped(bfap,pg,&nextPage,FALSE);
            MS_SMP_ASSERT(clone_nextPage <= bfap->maxClonePgs);
            if (ret == FALSE) {/* hole or permanent hole or not mapped*/
                /* Permanent holes return TRUE */
                ret = page_is_mapped(bfap,pg,&nextPage,TRUE);
                if (nextPage == XTNT_TERM) { /* pg is off the end of file */
                    /* see comment below about stripe files */
                    if (bfap->xtnts.type == BSXMT_STRIPE) {
                        clone_nextPage = NEXT_SEGMENT_PAGE
                             (pg, bfap->xtnts.stripeXtntMap->segmentSize);
                        if (clone_nextPage >= bfap->maxClonePgs) {
                            clone_nextPage = bfap->maxClonePgs;
                            use_orig_bfap = TRUE;
                        }
                    } else {
                        clone_nextPage = bfap->maxClonePgs;
                        use_orig_bfap = TRUE; /* no more clone extents */
                    }
                } else {
                    clone_nextPage = nextPage;
                }
                if (ret == FALSE) {/* its normal hole, look at original */
                    ret = page_is_mapped(orig_bfap,pg,&nextPage, FALSE);
                    if ( nextPage > clone_nextPage ) {
                        nextPage = clone_nextPage;
                    }
                } else {
                    /* Permanent hole, so consider as hole in clone */
                    ret = FALSE;
                }
            }
        }

        /* The function page_is_mapped() sets the nextPage argument to
         * -1 (XTNT_TERM) at the end of the extent map for the file.
         * In case of striped files this may not be true.
         * The function page_is_mapped() sets the nextPage argument to
         * -1 when it hits the end of extent map for the file on any one
         * of the segment when its is striped across multiple segments.
         * So in case of striped files we will jump to next segment when
         * we hit the end of extent map in one segment and we do that till
         * we go through all the pages for the file.
         */

        if ( nextPage == XTNT_TERM && bfap->xtnts.type == BSXMT_STRIPE &&
             (fileSize > (((long)pg+1) * ADVFS_PGSZ)))
        {
            segmentSize = bfap->xtnts.stripeXtntMap->segmentSize;
            nextPage = NEXT_SEGMENT_PAGE(pg,segmentSize);
        }

        /* if the previous page is a hole and the current page is also
         * hole, this is the case in case of striped files we should not
         * treat this as a hole again, it should be treated as the
         * continuation of the previous hole and hence new entry should
         * not be created in the sparseness map.
         */

        if (ret == TRUE) {
            found_stg = 1;
        }
        if (!setflag && found_stg) {
            offset = (long) pg * ADVFS_PGSZ;
            setflag = 1;
        }

        /*Found a hole so create a new extent*/

        if (ret == FALSE && found_stg) {
            xtnts_array[i].offset = offset;
            xtnts_array[i].size = (long) pages * ADVFS_PGSZ;
            if (offset + xtnts_array[i].size > fileSize) {
                xtnts_array[i].size = fileSize - offset;
            }
            i++;
            setflag = 0;
            found_stg = 0;
            pages = 0;
        }
        if (setflag) {
            pages += nextPage - pg;
        }
        pg = nextPage;
    }

    if (i == 0) {
        /* we fall out of the loop without creating the first extent
         * only for a sparse file composed of just a frag
         */
        MS_SMP_ASSERT(bfap->fragState == FS_FRAG_VALID);
        offset = (long) bfap->fragPageOffset * ADVFS_PGSZ;
        if (fileSize > offset) {
            size = fileSize - offset;
        } else {
            size = (long) bfap->fragPageOffset * ADVFS_PGSZ +
                   bfap->fragId.type * BF_FRAG_SLOT_BYTES;
        }
    } else {
        /* we found at least one extent and must adjust the final extent
         * for a fragment and for file_size so decrement the value of i.
         */
        i--;
        offset = xtnts_array[i].offset;
        size   = xtnts_array[i].size;

        if (bfap->fragState == FS_FRAG_VALID) {
            if ((long)bfap->fragPageOffset * ADVFS_PGSZ == (offset + size)) {
                /* This extent is adjacent to the final extent
                 * so we must tack it on to the end.
                 */
                size += bfap->fragId.type * BF_FRAG_SLOT_BYTES;
            } else {
                /* there is a hole before the frag so add a new extent */
                i++;
                offset =  (long)bfap->fragPageOffset * ADVFS_PGSZ;
                size =  bfap->fragId.type * BF_FRAG_SLOT_BYTES;
            }
        }
    }

    /* truncate the final extent to the file length */
    if ( (fileSize < offset + size) && (fileSize > offset) ) {
        size = fileSize - offset;
    }

    xtnts_array[i].offset = offset;
    xtnts_array[i].size = size;
    /* increment the extents count as we started with zero */
    num_extents = ++i;

end:
    map->numextents = num_extents;
    map_arraysize = MIN(map->arraysize, num_extents) - map->offset;
    if ( map_arraysize > 0 ) {
        bcopy(xtnts_array + map->offset, map->extent,
                          ((sizeof(struct extentmapentry)) * map_arraysize));
    }

    ms_free(xtnts_array);
    return EOK;
}
