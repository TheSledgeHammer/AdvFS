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
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      Recoverable block pool function prototypes and type definitions.
 *
 */
#ifndef _RBLKPOOL_UTIL_
#define _RBLKPOOL_UTIL_

#include <advfs/tapefmt.h>

#define ERR_BAD_BLK         1
#define END_OF_BLKS        -2

int
hdr_crc_ok(
           struct blk_t *blk  /* in - a pointer to a save-set block */
           );

int
blk_crc_ok(
           struct blk_t *blk  /* in - a pointer to a save-set block. */
           );

typedef char * recov_blk_pool_handle_t;

int
rblk_pool_create( 
   recov_blk_pool_handle_t *rblk_pool_h,
   blk_pool_handle_t blkh,
   int using_xor, 
   int blocks_per_group, 
   int groups, 
   msg_q_handle_t pt_mq
   );

void
rblk_pool_delete( recov_blk_pool_handle_t rblk_pool_h );

int
rblk_get( 
   recov_blk_pool_handle_t rblk_pool_h,
   struct blk_t **blk,             /* out */
   int *blk_id                     /* out */
   );

int
rblk_free( 
   recov_blk_pool_handle_t rblk_pool_h,
   int blk_id                      /* in */
   );

int 
rblk_allocate ( 
   recov_blk_pool_handle_t rblk_pool_h,
   struct blk_t **blk             /* out    */
   );

int 
rblk_release ( 
   recov_blk_pool_handle_t rblk_pool_h,
   struct blk_t *blk              /* in     */
   );


#endif /* _RBLKPOOL_UTIL_ */

/* end rblkpool_util.h */

