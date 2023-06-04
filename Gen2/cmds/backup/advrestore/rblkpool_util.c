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
 *      Advfs Storage System
 *
 * Abstract:
 *
 *      Recoverable block pool routines.
 *
 */

#include <advfs/backup.h>
#include <advfs/tapefmt.h>
#include <advfs/util.h>
#include <advfs/blkpool_util.h>
#include <rblkpool_util.h>
#include <stdarg.h>
#include <sys/signal.h>
#include "vrestore_msg.h"

extern nl_catd catd;
extern uint64_t Bytes_read;
extern int Src_fd;
extern int (*read_buf)( int fd, char *blk, int cnt );
extern double Version;



/******************************************************************************
 *
 * RECOVERABLE BLOCK POOL
 *
 * HOW IT ALL WORKS!
 *
 * Design Requirements:
 *  
 *  There two major functions that the block manager must support:
 *
 *    - XOR recovery:  When the save-set contains XOR blocks for error
 *          recovery, it is the responsibility of the block manager to
 *          utilize the XOR blocks to recover bad save-set blocks.  See
 *          the routine 'recover_blk()'.
 *
 *    - Support for asynchronous I/O: In order to achieve
 *          good I/O throughput the block manager must support some
 *          form of asynchronous I/O to the save-set device.  Currently
 *          threads are used for this.
 *
 * Design Overview:
 *
 *    Save-sets with XOR blocks have the following format:
 *
 *           BLK1 BLK2 BLK3 XOR.BLK BLK4 BLK5 BLK6 XOR.BLK ...
 *  
 *    The XOR.BLK consists of the XOR of the previous N blocks (3
 *    in this case); the N blocks are called an XOR block group.  To
 *    recover a block in a block group one simple XORs the remaining blocks
 *    in the group and the XOR block.  Note that this technique allows
 *    you to recover a block only if just one block in a group is bad since
 *    you always need N-1 blocks and the XOR block to recover the bad block.
 *
 *    Given this method of recovery it should be obvious that the block
 *    manager must hang on to all blocks in a group until it is certain
 *    that all the blocks in the group are okay.  Rather than getting too
 *    fancy, the block manager currently just keeps all the blocks in a
 *    group in memory until all the blocks in the group have been completely
 *    restored.
 *
 *    To support asynchronous I/O the block manager "reads" blocks via a
 *    ReadThread.  The block manager and the thread share a message queue
 *    and a block pool.  The thread tries to keep all the blocks full and
 *    the block manager is informed of ready blocks via the message queue.
 *    
 *    Since the block manager hangs on to all the blocks in a group when 
 *    the save-set contains XOR blocks the block reads would normally be
 *    done in bursts (start reading blocks, process blocks, release blooks,
 *    repeat the cycle).  This does not take good advantage of the 
 *    asynchrony of the ReadThread.  To achieve better throughput the
 *    block manager is capable of maintaining several block groups where
 *    only one is the group that is being restored; the others are being
 *    asynchronously read in by the ReadThread.  This is basically
 *    "double grouping" instead of the usual double-buffering!
 *
 *    Note that if the save-set does not contain XOR blocks the block
 *    manager releases each block once the block has been restored.
 *
 *    All in-memory blocks are kept in the buffer pool.  The block manager 
 *    keeps track of blocks via a block descriptor array (blkd).  This
 *    array is partitioned into equal-sized groups of blocks.  When the
 *    save-set contains XOR blocks the groups size is equal to the XOR
 *    group size of the save-set plus one for XOR block.  When the
 *    save-set does not contain XOR blocks there is no point in partitioning
 *    the blocks into groups so there is just on large block group.
 *
 */

#include <restore_msg.h>

/*
 * Block descriptor array definitions.
 */
typedef enum { 
    BLK_FREE,              /* the block is not allocated (blk ptr == NULL)  */
    BLK_READY,             /* the block is allocated and correct            */
    BLK_BAD,               /* the block is allocated but corrupted          */
    BLK_FREE_PENDING       /* the block is ready to be deallocated          */
} blk_state_t;

typedef struct blk_desc {
    struct blk_t *blk;     /* pointer to a save-set block                   */
    blk_state_t  state;    /* the state of the corresponding block          */
} blk_desc_t;

typedef struct {
    blk_desc_t *blkd;       /* block descriptor array                        */
    int32_t *first_grp_blk; /* each element contains the number of the first */
                            /*   block in the corresponding group; the array */
                            /*   is indexed by group number                  */
    int32_t cur_grp;        /* current group number                          */
    int32_t nxt_grp;        /* next group number                             */
    int32_t xor;            /* flag indicates if XOR recovery is possible    */
    int32_t blk_size;       /* bytes per block                               */
    int32_t blks;           /* total number of blocks                        */
    int32_t blks_per_grp;   /* number of blocks in each group                */
    int32_t grps;           /* number groups                                 */
    int32_t cur_blk;        /* current block number                          */
    blk_pool_handle_t bph;  /* handle to block pool                          */
    msg_q_handle_t pt_mq;   /* handle to WriteThread message queue           */

} recov_blk_pool_t;

static const recov_blk_pool_t nil_recov_blk_pool = { 0 };

#define XOR_BLOCK \
    (rblkp->blkd[rblkp->cur_blk].blk->bhdr.bheader.flags & BF_XOR_BLOCK)

#define BLOCK_OK  (hdr_crc_ok( rblkp->blkd[rblkp->cur_blk].blk ) && \
(!rblkp->xor || (rblkp->xor && (blk_crc_ok(rblkp->blkd[rblkp->cur_blk].blk)))))


/*
 * hdr_crc_ok - Verifies that a save-set header is valid by checking
 * its CRC.
 *
 * return ERROR or OKAY.
 */
int
hdr_crc_ok(
           struct blk_t *blk  /* in - a pointer to a save-set block */
           )
{
    uint16_t restore_blk_crc,
    backup_blk_crc,
    restore_hdr_crc,
    backup_hdr_crc;

    /*----------------------------------------------------------------------*/

    if (blk == NULL) {
        return FALSE;
    }
    
    /* save CRCs in block header */
    backup_blk_crc = blk->bhdr.bheader.block_crc;

    if (Version >= 6.0) {
        backup_hdr_crc = blk->bhdr.bheader.header_crc;
    } else {
        /*  Saveset was created on Tru64, needs bitswapping */
        backup_hdr_crc = SWAP16(blk->bhdr.bheader.header_crc); 
    }
        
    /* zero out CRCs in block header */
    blk->bhdr.bheader.block_crc  = 0;
    blk->bhdr.bheader.header_crc = 0;

    /* calc CRC of block header */
    restore_hdr_crc = crc16( (u_char *) &blk->bhdr, sizeof( blk->bhdr ) );

    /* restore original CRCs to block */
    blk->bhdr.bheader.header_crc = backup_hdr_crc;
    blk->bhdr.bheader.block_crc  = backup_blk_crc;

    if (restore_hdr_crc != backup_hdr_crc) {
        return FALSE;
    }

    if (Version < 6.0) {
        /*  Saveset was created on Tru64 UNIX.  Need to change the
         *  endian-ness of the rest of the integer data.  Do this after 
         *  the CRC has been validated.
         */

        blk->bhdr.bheader.block_size = SWAP32(blk->bhdr.bheader.block_size);
        blk->bhdr.bheader.flags = SWAP32(blk->bhdr.bheader.flags);
        blk->bhdr.bheader.vol_set_num = SWAP16(blk->bhdr.bheader.vol_set_num);
        blk->bhdr.bheader.vol_sets = SWAP16(blk->bhdr.bheader.vol_sets);
        blk->bhdr.bheader.volume_num = SWAP16(blk->bhdr.bheader.volume_num);
        blk->bhdr.bheader.block_num = SWAP16(blk->bhdr.bheader.block_num);
        blk->bhdr.bheader.block_crc = SWAP16(blk->bhdr.bheader.block_crc);
        blk->bhdr.bheader.xor_blocks = SWAP16(blk->bhdr.bheader.xor_blocks);
        blk->bhdr.bheader.xor_block_num =
                SWAP16(blk->bhdr.bheader.xor_block_num);
        blk->bhdr.bheader.ss_id = SWAP32(blk->bhdr.bheader.ss_id);
        blk->bhdr.bheader.dump_date.tv_sec =
                SWAP32(blk->bhdr.bheader.dump_date.tv_sec);
        blk->bhdr.bheader.dump_date.tv_usec =
                SWAP32(blk->bhdr.bheader.dump_date.tv_usec);
    }

    return TRUE;
}

/* end hdr_crc_ok */

/*
 * blk_crc_ok - Verifies that a save-set block is valid by checking
 * its CRC.
 *
 * This routine assumes that the hdr CRC has already been validated
 * using hdr_crc_ok().
 *
 * returns ERROR or OKAY.
 */

int
blk_crc_ok(
    struct blk_t *blk  /* in - a pointer to a save-set block. */
    )
{
    uint16_t restore_blk_crc,
    backup_blk_crc;

    /*----------------------------------------------------------------------*/

    if (blk == NULL) {
        return FALSE;
    }
    
    /* save CRCs in block header */
    backup_blk_crc = blk->bhdr.bheader.block_crc;

    /* zero out CRCs in block header */
    blk->bhdr.bheader.block_crc  = 0;

    /* calc CRC of block */
    restore_blk_crc = crc16( (u_char *) blk, blk->bhdr.bheader.block_size );

    /* restore original CRCs to block */
    blk->bhdr.bheader.block_crc  = backup_blk_crc;

    if (restore_blk_crc != backup_blk_crc) {
        return FALSE;
    } else {
        return TRUE;
    }
}

/* end blk_crc_ok */

/*
 * rblk_pool_create - This routine initializes the block manager.
 * Specifically, it initializes the global variables, allocates the
 * block descriptor array, allocates the first group block array and
 * allocates the block pool.
 *
 * returns ERROR or OKAY.
 */

int
rblk_pool_create( 
   recov_blk_pool_handle_t *rblk_pool_h,  /* out - handle of new rblk_pool */
   blk_pool_handle_t blkh,   /* in - blk pool handle */
   int using_xor,            /* in - flag when XOR recovery is possible */
   int blocks_per_group,     /* in - num blocks in each block group */
   int groups,               /* in - number of groups */
   msg_q_handle_t pt_mq      /* in - */
   )
{
    int b, g;      /* block and group indices */
    recov_blk_pool_t *rblkp;

    /*------------------------------------------------------------------------*/

    rblkp = (recov_blk_pool_t *) malloc( sizeof( recov_blk_pool_t ) );
    if (rblkp == NULL) {return ERROR;}

    /* initialize global variables from parameters */

    *rblkp = nil_recov_blk_pool;

    rblkp->xor = using_xor;
    rblkp->blks_per_grp = blocks_per_group;
    rblkp->grps = groups;
    rblkp->blks = groups * blocks_per_group;
    rblkp->cur_blk = 0;
    rblkp->pt_mq = pt_mq;

    /* allocate and initialize the block descriptor array */
    rblkp->blkd = (blk_desc_t *) malloc( rblkp->blks * sizeof( blk_desc_t ) );
    if (rblkp->blkd == NULL) {
        free(rblkp);
        return ERROR;
    }

    for (b = 0; b < rblkp->blks; b++) {
        rblkp->blkd[b].blk   = NULL;
        rblkp->blkd[b].state = BLK_FREE;
    }

    /* allocate and initialize the "first group block" array */
    rblkp->first_grp_blk = (int *) malloc ( rblkp->grps * sizeof( int ) );

    if (rblkp->first_grp_blk == NULL) {
        free(rblkp->blkd);
        free(rblkp);
        return ERROR;
    }

    for (g = 0; g < rblkp->grps; g++) {
        rblkp->first_grp_blk[g] = g * rblkp->blks_per_grp;
    }

    rblkp->cur_grp = 0;
    rblkp->nxt_grp = (rblkp->cur_grp + 1) % rblkp->grps;

    rblkp->bph = blkh;
    rblkp->blk_size = blk_pool_blk_size( blkh );
    blk_pool_expand( blkh, rblkp->blks );

    *rblk_pool_h = (recov_blk_pool_handle_t) rblkp;

    return OKAY;
}

/* end rblk_pool_create */

/*
 * rblk_pool_delete - Shuts down the block manager by deallocating
 * everything.
 */

void
rblk_pool_delete(
    recov_blk_pool_handle_t rblk_pool_h  /* in - rblk pool handle */
    )
{
    int b;
    recov_blk_pool_t *rblkp;

    rblkp = (recov_blk_pool_t *) rblk_pool_h;

    for (b = 0; b < rblkp->blks; b++) {
        /* 
         * release all blks that are still allocated; should 
         * this ever happen? 
         */
        if (rblkp->blkd[b].state != BLK_FREE) {
            blk_release( rblkp->bph, rblkp->blkd[b].blk );
        }
    }

    free( rblkp->blkd );
    rblkp->blkd = NULL;
    free( rblkp->first_grp_blk );
    rblkp->first_grp_blk = NULL;

    free( rblkp );
}

/* end rblk_pool_delete */

/*
 * recover_blk - This routine attempts to recover a corrupt block by
 * XORing the remaining good blocks in the corrupt block's XOR group.
 * If there are other bad blocks in the XOR group then none of the bad
 * blocks in the group can be recovered.
 *
 * returns ERROR or OKAY.
 */
static int
recover_blk(
            recov_blk_pool_t *rblkp,  /* in - ptr to rblk pool */
            int bad_blk               /* in - block id */
            )
/*
 * Notes:
 *
 *  1.  The XOR block's CRC cannot be used to validate the XOR block since
 *      it's CRC is the XOR of all the block CRC's of the blocks in the
 *      XOR group.  Therefore, the CRC of a recovered block must be
 *      rechecked to ensure that the XOR block was not corrupt.
 *
 *  2.  Since the end of a save-set may not have the correct number of
 *      blocks in the XOR group, this routine must not assume that all
 *      XOR groups have the correct number of blocks in them.  The
 *      variable 'actual_xor_blks' is used for this purpose.
 */

{
    int 
      result,
      b, 
      actual_xor_blks,                   /* see note 2                      */
      recoverable_bad_blk = TRUE,        /* the bad block can be recovered  */
      rcnt;                              /* bytes read                      */

    mssg_t msg;
    struct blk_t *blk;

    /*----------------------------------------------------------------------*/

    /* mark current block 'bad'; will be changed to 'ready' if recovered  */
    rblkp->blkd[bad_blk].state = BLK_BAD;

    /* get remaining blocks in XOR group and the XOR block */
    b    = bad_blk + 1;
    rcnt = 1;

    while (b < (rblkp->first_grp_blk[rblkp->cur_grp] + rblkp->blks_per_grp)) {
        blk_allocate( rblkp->bph, &blk );

        /*
         * Read the next save-set block.
         */
        result = read_buf( Src_fd, (char *) blk, rblkp->blk_size );

        if (result > 0) {
            /* 
             * Good block. 
             */
            Bytes_read += rblkp->blk_size;

            rblkp->blkd[b].blk = blk;

            if ((rblkp->blkd[b].blk->bhdr.bheader.flags & BF_XOR_BLOCK) || 
                (blk_crc_ok( rblkp->blkd[b].blk ))) {
                /* XOR block or good data block */
                rblkp->blkd[b].state = BLK_READY;
            } else {
                /*
                 * encountered another bad blk in the XOR group; can't
                 * recover!! 
                 */
                rblkp->blkd[b].state = BLK_BAD;
                recoverable_bad_blk = FALSE;
            }

            b++;   /* goto next block */

        } else if (result == ERROR) {
            /* encountered another bad blk in the XOR group; can't recover!!*/
            rblkp->blkd[b].state   = BLK_BAD;
            recoverable_bad_blk = FALSE;

        } else /* result == 0 */ {
            /*
             * No block; end of file.
             */
            blk_release( rblkp->bph, blk );
            break; /* terminate loop */
        }
    }

    actual_xor_blks = b - rblkp->first_grp_blk[rblkp->cur_grp];

    if (recoverable_bad_blk) {
        /* bad block is recoverable so recover it */
        fprintf( stderr, catgets(catd, S_RBLKPOOL_UTIL1, RBLKPOOL_UTIL1, 
            "%s: recovering corrupt block,"), Prog );

        bzero( rblkp->blkd[bad_blk].blk, rblkp->blk_size );
        
        for (b = rblkp->first_grp_blk[rblkp->cur_grp]; 
             b < (rblkp->first_grp_blk[rblkp->cur_grp] + actual_xor_blks); 
             b++) {
            if (b != bad_blk) {
                xor_bufs( (u_char *) rblkp->blkd[bad_blk].blk, 
                         (u_char *) rblkp->blkd[b].blk, 
                         rblkp->blk_size );
            }
        }

        /* clear XOR block bit; it got set in the 'xor_bufs' loop above */
        rblkp->blkd[bad_blk].blk->bhdr.bheader.flags &= ~BF_XOR_BLOCK; 

        if (blk_crc_ok( rblkp->blkd[bad_blk].blk )) {
            /* the block was recovered so change state to 'ready' */
            rblkp->blkd[bad_blk].state = BLK_READY;
            fprintf( stderr, catgets(catd, S_RBLKPOOL_UTIL1, RBLKPOOL_UTIL2, 
                " block recovered\n") );
        } else {
            /* some other blk was corrupt so the blk couldn't be recovered */
            fprintf( stderr, catgets(catd, S_RBLKPOOL_UTIL1, RBLKPOOL_UTIL3, 
                " unable to recover block\n") );
        }
    }

    return OKAY;
}

/* end recover_blk */

/*
 * rblk_get - Gets the next save-set block.
 *
 * returns ERROR or OKAY.
 */
int
rblk_get( 
   recov_blk_pool_handle_t rblk_pool_h,   /* in - rblk pool handle */
   struct blk_t **blk,                    /* out - ptr to next ss blk */   
   int *blk_id                            /* in - block id */
   )
{
    int result, end_of_file = FALSE, read_error = FALSE; 
    int b, status = OKAY, prev_grp = 0;
    mssg_t msg;
    recov_blk_pool_t *rblkp;
    struct blk_t *_blk;

    /*-----------------------------------------------------------------------*/

    rblkp = (recov_blk_pool_t *) rblk_pool_h;
   
    /* 
     * when the save-set has XOR blocks and we are about to switch to a new
     * block group we need to release all the FREE-PENDING blocks in the
     * just-finished group (note that 'rblkp->cur_grp' and 'rblkp->cur_blk' 
     * already refer to the new group; hence the references to prev_grp which 
     * is really the "just-finished" group) 
     */

    if (rblkp->xor && 
        (rblkp->cur_blk == rblkp->first_grp_blk[rblkp->cur_grp])){

        prev_grp = (rblkp->cur_grp + (rblkp->grps + 1)) % rblkp->grps;

        for (b = rblkp->first_grp_blk[prev_grp];
             b < (rblkp->first_grp_blk[prev_grp] + rblkp->blks_per_grp); 
             b++ ) {
            if (rblkp->blkd[b].state == BLK_FREE_PENDING) {
                blk_release( rblkp->bph, rblkp->blkd[b].blk );
                rblkp->blkd[b].state = BLK_FREE;
                rblkp->blkd[b].blk   = NULL;
            }
        }
    }

    if (rblkp->blkd[rblkp->cur_blk].state == BLK_FREE) {
        /* the current block has no data so get the next save-set block */

        blk_allocate( rblkp->bph, &_blk );

        /*
         * Read the next save-set block.
         */
            result = read_buf( Src_fd, (char *) _blk, rblkp->blk_size );

        if (result > 0) {
            /* 
             * Good block. 
             */
            Bytes_read += rblkp->blk_size;
            rblkp->blkd[rblkp->cur_blk].blk = _blk;

        } else if (result == ERROR) {
            read_error = TRUE;

        } else /* result == 0 */ {
            blk_release( rblkp->bph, _blk );
            end_of_file = TRUE;
        }

        if (end_of_file) {
            /* end of save-set */
            return END_OF_BLKS;
        } else {
            /*
             * Need to test if the current block used by BLOCK_OK and 
             * XOR_BLOCK is not NULL first. Otherwise using them on a 
             * read error could cause a core dump since they both use 
             * the current block as a pointer.
             */
            if ((!read_error && BLOCK_OK) || 
                ((rblkp->blkd[rblkp->cur_blk].blk != NULL) && XOR_BLOCK)) {
                rblkp->blkd[rblkp->cur_blk].state = BLK_READY;
            } else if (rblkp->xor) {
                /* block is bad; tape has XOR blocks so recover the block */
                recover_blk( rblkp, rblkp->cur_blk );
            } else {
                /* bad blk; can't recover since tape doesn't have XOR blks */
                rblkp->blkd[rblkp->cur_blk].state = BLK_BAD;
            }
        }
    }

    if (rblkp->blkd[rblkp->cur_blk].state == BLK_READY) {
        /* return ready block */
        *blk    = rblkp->blkd[rblkp->cur_blk].blk;
        *blk_id = rblkp->cur_blk;
    } else if (rblkp->blkd[rblkp->cur_blk].state == BLK_BAD) {
        /* return status indicating block is bad */
        *blk    = rblkp->blkd[rblkp->cur_blk].blk;
        *blk_id = rblkp->cur_blk;
        status  = ERR_BAD_BLK;
    } else {
        /* oops! something is really messed up! */
        fprintf( stderr, 
                 catgets(catd, S_RBLKPOOL_UTIL1, RBLKPOOL_UTIL4, 
                 "Invalid block state <%d> encountered in 'rblk_get'\n"), 
                rblkp->blkd[rblkp->cur_blk].state );
        status = ERROR;
    }

    /* circularly increment current block index */
    rblkp->cur_blk = (rblkp->cur_blk + 1) % rblkp->blks;

    if (rblkp->cur_blk == rblkp->first_grp_blk[rblkp->nxt_grp]) {
        rblkp->cur_grp = rblkp->nxt_grp;
        rblkp->nxt_grp = (rblkp->nxt_grp + 1) % rblkp->grps;
    }

    return status;
}

/* end rblk_get */

/*
 * rblk_free - The routine frees a block (or marks it as
 * free-pending).
 *
 * returns OKAY.
 */
int
rblk_free(
          recov_blk_pool_handle_t rblk_pool_h,  /* in - rblk pool handle */
          int blk_id                            /* in - block id */
          )
{
    recov_blk_pool_t *rblkp;

    /*-----------------------------------------------------------------------*/

    rblkp = (recov_blk_pool_t *) rblk_pool_h;

    if (rblkp->xor) {
        /*
         * if the save-set contains XOR blocks then we cannot release
         * the block until all blocks in the current group have been
         * restored; so mark the block as free-pending and let 'rblk_get'
         * free the block when the block group has been restored
         */
        rblkp->blkd[blk_id].state = BLK_FREE_PENDING;
    } else {
        /* free the block */
        blk_release( rblkp->bph, rblkp->blkd[blk_id].blk );
        rblkp->blkd[blk_id].state = BLK_FREE;
        rblkp->blkd[blk_id].blk   = NULL;
    }
    
    return OKAY;
}

/* end rblk_free */


/*
 * rblk_allocate - allocate a recoverable block.
 *
 * returns value of blk_allocate call.
 */
int 
rblk_allocate ( 
   recov_blk_pool_handle_t rblk_pool_h,
   struct blk_t **blk             /* out    */
   ) 
{
    recov_blk_pool_t *rblkp;
   
    /*-----------------------------------------------------------------------*/

    rblkp = (recov_blk_pool_t *) rblk_pool_h;

    return blk_allocate( rblkp->bph, blk );
}

/*
 * rblk_release - blk_release rblk.
 *
 * returns value of blk_release call.
 */
int 
rblk_release ( 
   recov_blk_pool_handle_t rblk_pool_h,
   struct blk_t *blk              /* in     */
   ) 
{
    recov_blk_pool_t *rblkp;

    /*-----------------------------------------------------------------------*/

    rblkp = (recov_blk_pool_t *) rblk_pool_h;

    return blk_release( rblkp->bph, blk );
}


/* end rblkpool_util.c */
