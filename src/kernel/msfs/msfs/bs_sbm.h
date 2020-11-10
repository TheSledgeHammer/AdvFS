/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 * @(#)$RCSfile: bs_sbm.h,v $ $Revision: 1.1.18.2 $ (DEC) $Date: 2001/12/14 16:51:02 $
 */
#ifndef _SBM_H_
#define _SBM_H_

#include <msfs/bs_stg.h>

statusT
sbm_init(
         vdT* vdp
         );

statusT
sbm_clear_cache(
                vdT *vdp
                );

void *
sbm_find_space(
               vdT *vdp,
               uint32T requested_blks,
               uint64T dstBlkOffset,
               bsAllocHintT alloc_hint,
               uint32T *start_blk,
               uint32T *found_blks
               );

void
sbm_howmany_blks(
    uint32T blkOffset,
    uint32T *blkCount,          /* in/out , might be reduced */
    int *pinPages,              /* in/out */
    vdT *vdp,
    int pgSz
    );

statusT
sbm_remove_space( 
                 vdT *vdp,
                 uint32T startBlk,
                 uint32T blks,
                 stgDescT *stg_desc,
                 ftxHT parentFtx,
                 uint32T flags
                 );

statusT
sbm_return_space_no_sub_ftx ( 
                  vdT *virtualDiskp,  /* in */
                  uint32T blkOffset,  /* in */
                  uint32T blkCnt,  /* in */
                  ftxHT parentFtx  /* in */
                  );

void
sbm_set_pg_bits(
                int bitOffset,          /* bit offset into page */
                int bitCount,           /* number of bits to set */
                struct bsStgBm* sbmPg   /* ptr to sbm buffer */
                );

statusT
sbm_alloc_bits (
                vdT  *vdp,        /* in */
                int   bitOffset,  /* in */
                int   bitCount,   /* in */
                ftxHT parentFtx   /* in */
                );

uint32T
sbm_total_free_space(
                     vdT *vdp
                     );

void
sbm_dump(
         vdT *vdp
         );

#endif /* _SBM_H_ */
