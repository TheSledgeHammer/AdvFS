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
 *
 * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
 *
 * Facility:
 *
 *      AdvFS File System
 *
 * Abstract:
 *
 *      Block pool function prototypes and type definitions.
 *
 */

#ifndef _BLKPOOL_UTIL_
#define _BLKPOOL_UTIL_

/* 
 * block pool definitions and routines 
 */
typedef buf_pool_handle_t blk_pool_handle_t;

/*
 * block size must be a multiple of BLK_CHUNK_SZ and must be 
 * >= MIN_BLK_SZ and <= MAX_BLK_SZ.
 */

#define blk_pool_create( blk_pool_h, num_blks, blk_size ) \
    buf_pool_create( (blk_pool_h), (num_blks), (blk_size) )

#define blk_pool_delete( blk_pool_h ) \
    buf_pool_delete( (blk_pool_h) )

#define blk_pool_expand( blk_pool_h, num_blks ) \
    buf_pool_expand( (blk_pool_h), (num_blks) )

#define blk_allocate( blk_pool_h, blk ) \
    buf_allocate( (blk_pool_h), (char **) (blk) )

#define blk_release(  blk_pool_h,  blk ) \
    buf_release( (blk_pool_h), (char *) (blk) )

#define blk_pool_blk_size( blk_pool_h ) \
    buf_pool_buf_size( (blk_pool_h) )


#endif /* _BLKPOOL_UTIL_ */

/* end blkpool_util.h */
