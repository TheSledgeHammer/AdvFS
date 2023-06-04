/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
 */
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
 * @(#)$RCSfile: bs_inmem_map.h,v $ $Revision: 1.1.32.3 $ (DEC) $Date: 2006/08/29 16:35:42 $
 */

#ifndef _BS_INMEM_MAP_
#define _BS_INMEM_MAP_

typedef struct {
    uint32T pageOffset;
    uint32T pageCnt;
    vdIndexT volIndex;
    uint32T blkOffset;
} bsXtntDescT;

/*
 * bsInMemXtntDescIdT - Identifies an extent descriptor in an in-memory extent
 * map.  This is used when sequentially fetching extent descriptors from an
 * in-mem extent map.
 */

typedef struct bsInMemXtntDescId {
    uint32T subXtntMapIndex;
    uint32T xtntDescIndex;
} bsInMemXtntDescIdT;

statusT
imm_create_xtnt_map (
                     uint32T blksPerPage,          /* in */
                     domainT *domain,              /* in */
                     uint32T maxCnt,               /* in */
                     uint32T termPage,             /* in */
                     vdIndexT termVdIndex,         /* in */
                     bsInMemXtntMapT **newXtntMap  /* out */
                     );

statusT
imm_init_xtnt_map (
                   bsInMemXtntMapT *xtntMap,  /* in */
                   uint32T blksPerPage,       /* in */
                   domainT *domain,           /* in */
                   uint32T maxCnt,            /* in */
                   uint32T termPage,          /* in */
                   vdIndexT termVdIndex       /* in */
                   );

statusT
imm_extend_xtnt_map (
                     bsInMemXtntMapT *xtntMap
                     );
statusT
imm_get_xtnt_map_size (
                       bsInMemXtntMapT *xtntMap,  /* in */
                       int *size  /* out */
                       );

statusT
imm_compress_xtnt_map (
                       uint32T segmentSize,           /* in */
                       uint32T cnt,                   /* in */
                       bsInMemXtntMapT *xtntMap,      /* in */
                       bsXtntMapTypeT bsXtntMapType,  /* in */
                       bsInMemXtntMapT **newXtntMap   /* out */
                       );
void
imm_get_first_xtnt_desc (
                         bsInMemXtntMapT *xtntMap,        /* in */
                         bsInMemXtntDescIdT *xtntDescId,  /* out */
                         bsXtntDescT *xtntDesc            /* out */
                         );

void
imm_get_xtnt_desc (
                   uint32T pageOffset,              /* in */
                   bsInMemXtntMapT *xtntMap,        /* in */
                   int updateFlg,                   /* in */
                   bsInMemXtntDescIdT *xtntDescId,  /* out */
                   bsXtntDescT *xtntDesc            /* out */
                   );

void
imm_get_next_xtnt_desc (
                        bsInMemXtntMapT *xtntMap,        /* in */
                        bsInMemXtntDescIdT *xtntDescId,  /* in/out */
                        bsXtntDescT *xtntDesc            /* out */
                        );

void
imm_delete_xtnt_map (
                     bsInMemXtntMapT *xtntMap  /* in */
                     );

void
imm_delete_sub_xtnt_maps (
                          bsInMemXtntMapT *xtntMap  /* in */
                          );

statusT
imm_copy_sub_xtnt_map (
                       bfAccessT *bfap,                    /* in */
                       bsInMemXtntMapT *xtntMap,           /* in */
                       bsInMemSubXtntMapT *oldSubXtntMap,  /* in */
                       bsInMemSubXtntMapT *newSubXtntMap   /* in */
                       );

statusT
imm_init_sub_xtnt_map (
                       bsInMemSubXtntMapT *subXtntMap,  /* in */
                       uint32T pageOffset,              /* in */
                       uint32T pageCnt,                 /* in */
                       vdIndexT vdIndex,                /* in */
                       bfMCIdT mcellId,                 /* in */
                       uint32T type,                    /* in */
                       uint32T onDiskMaxCnt,            /* in */
                       uint32T maxCnt                   /* in */
                       );

statusT
imm_extend_sub_xtnt_map (
                         bsInMemSubXtntMapT *subXtntMap  /* in */
                         );

statusT
imm_load_sub_xtnt_map (
                       bfAccessT *bfap,                 /* in */
                       bsInMemXtntMapT *xtntMap,        /* in */
                       bsInMemSubXtntMapT *subXtntMap   /* in */
                       );

void
imm_unload_sub_xtnt_map (
                         bsInMemSubXtntMapT *subXtntMap  /* in */
                         );

statusT
imm_copy_xtnt_descs (
                     vdIndexT copyVdIndex,        /* in */
                     bsXtntT *copyBsXA,           /* in */
                     uint32T copyCnt,             /* in */
                     int copyHoleFlag,            /* in */
                     bsInMemXtntMapT *xtntMap,    /* in */
                     bsXtntMapTypeT bsXtntMapType /* in */
                     );

void
imm_split_desc (
                uint32T pageOffset,  /* in */
                uint32T pageSize,    /* in */
                bsXtntT *srcDesc,    /* in */
                bsXtntT *part1Desc,  /* in/modified */
                bsXtntT *part2Desc   /* in/modified */
                );

statusT
imm_replace_xtnt_desc (
                       uint32T pageSize,            /* in */
                       bsInMemXtntMapT *xtntMap,    /* in */
                       bsInMemXtntMapT *subXtntMap  /* in */
                       );

statusT
imm_get_alloc_page_cnt (
                        bsInMemXtntMapT *xtntMap,  /* in */
                        uint32T bfPageOffset,      /* in */
                        uint32T bfPageCnt,         /* in */
                        uint32T *allocPageCnt      /* out */
                        );

uint32T
imm_get_next_page (
                   bsInMemXtntT *xtnts  /* in */
                   );

uint32T
imm_get_hole_size (
                   uint32T pageOffset,  /* in */
                   bsInMemXtntT *xtnts  /* in */
                   );

statusT
imm_get_page_type (
                   bsInMemXtntT *xtnts,  /* in */
                   uint32T pageOffset    /* in */
                   );

void
imm_set_next_valid_copy_page (
                              bsInMemXtntT *xtnts,  /* in */
                              uint32T bfPageOffset  /* in */
                              );

statusT
imm_page_to_sub_xtnt_map (
                          uint32T pageOffset,        /* in */
                          bsInMemXtntMapT *xtntMap,  /* in */
                          int updateFlg,             /* in */
                          uint32T *index             /* out */
                          );

statusT
imm_page_to_xtnt (
                  uint32T pageOffset,              /* in */
                  bsInMemSubXtntMapT *subXtntMap,  /* in */
                  int updateFlg,                   /* in */
                  int permHoleFlg,                 /* in */
                  uint32T *index                   /* out */
                  );

statusT
imm_merge_xtnt_map (
                    bfAccessT *bfap,               /* in */
                    bsInMemXtntMapT *src1XtntMap,  /* in */
                    bfAccessT *cloneBfap,          /* in */
                    bsInMemXtntMapT *src2XtntMap,  /* in */
                    bsInMemXtntMapT **newXtntMap   /* out */
                    );

statusT
overlay_xtnt_map ( bfAccessT *bfap,                   /* in */
                   uint32T stripeIndex,               /* in */
                   bsInMemXtntMapT *baseXtntMap,      /* in */
                   bsInMemXtntMapT *overXtntMap,      /* in */
                   bsInMemXtntMapT **retModXtntMap,   /* out */
                   bsInMemXtntMapT **retReplXtntMap,  /* out */
                   ftxHT ftxH);

statusT
imm_overlay_xtnt_map (
                      bsInMemXtntMapT *baseXtntMap,  /* in */
                      bsInMemXtntMapT *overXtntMap,  /* in */
                      bsXtntMapTypeT bsXtntMapType,     /* in */
                      bsInMemXtntMapT **retNewXtntMap,  /* out */
                      bsInMemXtntMapT **retReplXtntMap  /* out */
                      );

statusT
imm_copy_xtnt_map (
                   bfAccessT *bfap,              /* in */
                   bsInMemXtntMapT *srcXtntMap,  /* in */
                   bsInMemXtntMapT **dstXtntMap  /* out */
                   );

statusT
imm_remove_page_map (
                     uint32T bfPageOffset,          /* in */
                     uint32T bfPageCnt,             /* in */
                     bsInMemXtntMapT *xtntMap,      /* in */
                     bsXtntMapTypeT bsXtntMapType,  /* in */
                     uint32T *remPageOffset,        /* out */
                     uint32T *remPageCnt            /* out */
                     );

void
imm_compress_subxtnt_map (
                          bsInMemXtntT *xtnts,  /*in */
                          int bfPageSz          /* in */
                          );

#endif  /* _BS_INMEM_MAP_ */
