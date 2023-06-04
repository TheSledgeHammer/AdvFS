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
 *   AdvFS 
 *
 * Abstract:
 *
 *   This header file contains structure definitions and function
 *   prototypes related to in-memory extent maps.
 *
 */

#ifndef _BS_INMEM_MAP_
#define _BS_INMEM_MAP_

#include <advfs/bs_extents.h>

/* FIX - this def should replace bsExtentDescT in bs_public.h. */
struct bsXtntDesc {
    bf_fob_t        bsxdFobOffset;      /* file offset in 1k blks */
    bf_fob_t        bsxdFobCnt;         /* cnt of 1k blks */
    vdIndexT        volIndex;           /* index of volume */
    bf_vd_blk_t     bsxdVdBlk;          /* disk blk of extent's start */
    
}; 
typedef struct bsXtntDesc bsXtntDescT;

/* FIX - bsInMemXtntDescIdT belongs in bs_ims.h. */
/*
 * bsInMemXtntDescIdT - Identifies an extent descriptor in an in-memory extent
 * map.  This is used when sequentially fetching extent descriptors from an
 * in-mem extent map.
 */

struct bsInMemXtntDescId {
    uint32_t subXtntMapIndex;
    uint32_t xtntDescIndex;
};
typedef struct bsInMemXtntDescId bsInMemXtntDescIdT;

statusT
imm_create_xtnt_map (
                     domainT *domain,              /* in */
                     uint32_t maxCnt,               /* in */
                     bsInMemXtntMapT **newXtntMap  /* out */
                     );

statusT
imm_extend_xtnt_map (bfAccessT *bfap,
                     bsInMemXtntMapT *xtntMap,
                     xtntMapFlagsT flags
                     );

statusT
imm_big_extend_xtnt_map(  bfAccessT *bfap,
                          bsInMemXtntMapT *xtntMap,
                          xtntMapFlagsT flags
                        );
void
imm_get_first_xtnt_desc (
                         bsInMemXtntMapT *xtntMap,        /* in */
                         bsInMemXtntDescIdT *xtntDescId,  /* out */
                         bsXtntDescT *xtntDesc            /* out */
                         );

statusT
imm_get_xtnt_desc (
                   bf_fob_t fob_offset, /* in */
                   bsInMemXtntMapT *xtntMap,        /* in */
                   int updateFlg,                   /* in */
                   bsInMemXtntDescIdT *xtntDescId,  /* out */
                   bsXtntDescT *xtntDesc            /* out */
                   );

statusT
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
                       bf_fob_t fob_offset, /* in */
                       bf_fob_t fob_cnt,    /* in */
                       bfMCIdT mcellId,                 /* in */
                       uint32_t type,                    /* in */
                       uint32_t onDiskMaxCnt,            /* in */
                       uint32_t maxCnt                   /* in */
                       );

statusT
imm_extend_sub_xtnt_map (
                         bsInMemSubXtntMapT *subXtntMap  /* in */
                         );

void
imm_unload_sub_xtnt_map (
                         bsInMemSubXtntMapT *subXtntMap  /* in */
                         );

statusT
imm_copy_xtnt_descs (bfAccessT *bfap,
                     vdIndexT copyVdIndex,        /* in */
                     bsXtntT *copyBsXA,           /* in */
                     uint32_t copyCnt,             /* in */
                     int16_t copyHoleFlag,            /* in */
                     bsInMemXtntMapT *xtntMap,     /* in */
                     xtntMapFlagsT flags
                     );

void
imm_split_desc (
                bf_fob_t fob_offset,        /* in */
                bsXtntT *srcDesc,                       /* in */
                bsXtntT *part1Desc,                     /* in/modified */
                bsXtntT *part2Desc                      /* in/modified */
                );

statusT
imm_get_alloc_fob_cnt (
                        bsInMemXtntMapT *xtntMap,               /* in */
                        bf_fob_t fob_offset,        /* in */
                        bf_fob_t fob_cnt,           /* in */
                        bf_fob_t *alloc_fob_cnt     /* out */
                      );

bf_fob_t
imm_get_next_fob (
                   bsInMemXtntT *xtnts  /* in */
                   );

bf_fob_t
imm_get_hole_size (
                   bf_fob_t fob_offset,     /* in */
                   bsInMemXtntT *xtnts                  /* in */
                   );


void
imm_get_first_hole (
                    bsInMemXtntT *xtnts,                   /* in */
                    bf_fob_t *ret_fob_offset,  /* out */
                    bf_fob_t *ret_fob_cnt      /* out */
                   );


void
imm_set_next_valid_copy_fob (
                              bsInMemXtntT *xtnts,              /* in */
                              bf_fob_t fob_offset   /* in */
                              );

statusT
imm_fob_to_sub_xtnt_map (
                          bf_fob_t fob_offset,      /* in */
                          bsInMemXtntMapT *xtntMap,             /* in */
                          int updateFlg,                        /* in */
                          uint32_t *index                        /* out */
                          );

statusT
imm_fob_to_xtnt (
                  bf_fob_t fob_offset, /* in */
                  bsInMemSubXtntMapT *subXtntMap,  /* in */
                  int updateFlg,                   /* in */
                  int permHoleFlg,                 /* in */
                  uint32_t *index                   /* out */
                  );

statusT
overlay_xtnt_map ( bfAccessT *bfap,                   /* in */
                   bsInMemXtntMapT *baseXtntMap,      /* in */
                   bsInMemXtntMapT *overXtntMap,      /* in */
                   bsInMemXtntMapT **retModXtntMap,   /* out */
                   bsInMemXtntMapT **retReplXtntMap,  /* out */
                   ftxHT ftxH);

statusT
imm_overlay_xtnt_map (bfAccessT * bfap,
                      bsInMemXtntMapT *baseXtntMap,  /* in */
                      bsInMemXtntMapT *overXtntMap,  /* in */
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
imm_remove_range_map (bfAccessT *bfap,
                     bf_fob_t fob_offset,           /* in */
                     bf_fob_t fob_cnt,              /* in */
                     bsInMemXtntMapT *xtntMap,                  /* in */
                     bf_fob_t *rem_fob_offset,      /* out */
                     bf_fob_t *rem_fob_cnt          /* out */

                     );

statusT
imm_check_xtnt_map(
                  bsInMemXtntMapT *xtntMap
                  );

#endif  /* _BS_INMEM_MAP_ */

