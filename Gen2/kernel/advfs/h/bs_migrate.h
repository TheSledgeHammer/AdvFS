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
 *   prototypes related to bitfile migrate.
 *
 */

#ifndef _BS_MIGRATE_
#define _BS_MIGRATE_

statusT
bs_move_metadata (
                  bfAccessT *bfap,/* in */
                  vdT       *vdp  /* in or NULL */
                  );

statusT
mig_migrate (
             bfAccessT *bfAccess,                       /* in */
             vdIndexT  srcVdIndex,                      /* in */
             bf_fob_t src_fob_offset,                   /* in */
             bf_fob_t src_fob_cnt,                      /* in */
             vdIndexT  dstVdIndex,                      /* in */
             bf_vd_blk_t dstBlkOffset,                  /* in */
             uint32_t  forceFlag,                        /* in */
             bsAllocHintT alloc_hint                    /* in */
             );
statusT
advfs_migrate_switch_stg_full(
        bfAccessT        *bfap,              /* in */
        ftxHT            parent_ftx          /* in */
        );       


statusT
mig_pack_vd_range (
        vdT *vdp,                          /* in */
        bf_vd_blk_t clr_range_begin_blk,   /* in */
        bf_vd_blk_t clr_range_end_blk,     /* in */
        bf_vd_blk_t *new_clr_range_blk,    /* out */
        uint32_t  forceFlag                /* in */
        );

#endif  /* _BS_MIGRATE_ */

