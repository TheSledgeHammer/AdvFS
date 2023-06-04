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
 *      Advfs
 *
 * Abstract:
 *
 *      Storage Bitmap function prototypes.
 *
 */
#ifndef _SBM_H_
#define _SBM_H_

#include <advfs/bs_stg.h>

#ifdef _KERNEL


extern int32_t AdvfsFixUpSBM;
extern uint32_t AdvfsNotPickyBlkCnt;

statusT
sbm_init(
         vdT* vdp
         );

statusT
sbm_clear_cache(
                vdT *vdp
                );

stgDescT *
sbm_find_space(
               vdT *vdp,
               bf_vd_blk_t requested_blks,
               bf_vd_blk_t minimum_blks,
               bf_vd_blk_t dstBlkOffset,
               bsAllocHintT alloc_hint,
               bf_vd_blk_t *start_blk,
               bf_vd_blk_t *found_blks
               );

void
sbm_howmany_blks(
    bf_vd_blk_t blkOffset,
    bf_vd_blk_t *blkCount,              /* in/out , might be reduced */
    int *pinPages,                      /* in/out */
    vdT *vdp,
    bf_fob_t pgSz
    );

statusT
sbm_remove_space( 
                 vdT *vdp,
                 bf_vd_blk_t startBlk,
                 bf_vd_blk_t blks,
                 stgDescT *stg_desc,
                 ftxHT parentFtx,
                 uint32_t flags
                 );

statusT
sbm_return_space_no_sub_ftx ( 
                  vdT *virtualDiskp,            /* in */
                  bf_vd_blk_t blkOffset,        /* in */
                  bf_vd_blk_t blkCnt,           /* in */
                  ftxHT parentFtx               /* in */
                  );

void
sbm_set_pg_bits(
                uint32_t bitOffset,             /* bit offset into page */
                uint32_t bitCount,              /* number of bits to set */
                struct bsStgBm* sbmPg           /* ptr to sbm buffer */
                );

statusT
sbm_alloc_bits (
                vdT  *vdp,              /* in */
                bf_vd_blk_t  bitOffset, /* in */
                bf_vd_blk_t bitCount,   /* in */
                ftxHT parentFtx         /* in */
                );

void
sbm_dump(
         vdT *vdp
         );

#endif /* #ifdef _KERNEL */

#endif /* _SBM_H_ */

