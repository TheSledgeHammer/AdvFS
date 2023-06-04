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
 *  ADVFS
 *
 * Abstract:
 *
 *  bs_copy.c
 *  This module contains routines related to bitfile page copy.
 *
 *
 * Date:
 *
 *  Thu Jun 28 14:44:45 1991
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_copy.c,v $ $Revision: 1.1.50.3 $ (DEC) $Date: 2008/02/12 13:06:50 $"

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_copy.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_inmem_map.h>
#include <msfs/bs_stg.h>
#include <msfs/bs_migrate.h>
#include <vm/vm_numa.h>
#define ADVFS_MODULE BS_COPY

/*
 * Protos
 */

static
statusT
set_block_map (
               bfAccessT *bfAccess,      /* in */
               uint32T bfPageOffset,     /* in */
               uint32T bfPageCnt,        /* in */
               bsInMemXtntT *copyXtnts,  /* in */
               uint32T copyXferSize,     /* in */
               int *abortFlag            /* in */
               );

static
statusT
verify_page_write (
                   bfAccessT *bfAccess,  /* in */
                   uint32T bfPageOffset,  /* in */
                   uint32T bfPageCnt,  /* in */
                   bsInMemXtntT *copyXtnts,  /* in */
                   int *abortFlag  /* in */
                   );

static
statusT
force_write (
             bfAccessT *bfAccess,      /* in */
             uint32T bfPageOffset,     /* in */
             unsigned char *buf,       /* in */
             bsInMemXtntT *copyXtnts   /* in */
             );

static
statusT
compare_copies (
                bfAccessT *bfAccess,  /* in */
                uint32T bfPageOffset,  /* in */
                uint32T bfPageCnt,  /* in */
                bsInMemXtntT *copyXtnts,  /* in */
                int *abortFlag  /* in */
                );

static int cp_debug = 0;

/*
 * cp_copy_page_range
 *
 * This function creates copies of the specified page range.  It does not
 * restrict access to the page range during the copy operation.  For example,
 * another thread can simultaneously access the bitfile, read and write the
 * bitfile and even add storage to the bitfile!  After the copy is finished,
 * pages can be read/written from/to either the original or the copies.
 *
 * The caller must initialize each extent map so that it maps the specified
 * page range.  Otherwise, the function returns a status of E_PAGE_NOT_MAPPED.
 *
 * The caller must insert the specified extent maps onto the bitfile's in-memory
 * copy extent map list before calling this function.
 *
 * The caller must not fiddle with the extent map list or the specified page
 * range in each extent map while this function is active.
 *
 * This function calls functions that call bs_pinpg_int().  This is necessary
 * so that a cow operation is not performed on the page range when the file has
 * a clone.
 *
 * FIX - Intersecting copies can be active at the same time without any problems
 * or restrictions.
 * 
 * FIX - how does raw interface interact with this function and the block
 * map?  Specifically, the read and write maps are different: read only
 * from the original, write to both the original and the copy.  Should
 * this function and the raw interface sync via a lock.  When one holds
 * the lock, the other is blocked.
 *
 * FIX - What happens if a write fails to original but is successful to the
 * copy, or vice versa??  Is the write re-tried on the failed device?  Probably
 * need to flag the on-disk information stating that the page on this device is
 * wrong.  When you try to read it, you get an read error.  If this is the case,
 * we don't care if the on-disk pages do not match.
 *
 * Changed the interface to take 'forceFlag' as input. This flag
 * indicates whether to ignore any pending truncations and go ahead 
 * (i.e. force) with the copy or to abort it.
 */

statusT
cp_copy_page_range (
                    bfAccessT *bfAccess,      /* in */
                    pageRangeT *bfPageRange,  /* in */
                    uint32T bfPageRangeCnt,   /* in */
                    bsInMemXtntT *copyXtnts,  /* in */
                    uint32T copyXferSize,     /* in */
                    uint32T forceFlag         /* in */
                    )
{
    blkMapT blkMap;
    uint32T i;
    statusT sts;
    bsInMemXtntMapT *xtntMap;
    int flag = 0;

    /* FIX - if log, can't do */

    /*
     * Modify each page's page map so that the page is written to the
     * copy.
     */

    for (i = 0; i < bfPageRangeCnt; i++) {
        if (bfPageRange[i].pageType == STG) {
            sts = set_block_map ( bfAccess,
                                  bfPageRange[i].pageOffset, 
                                  bfPageRange[i].pageCnt, 
                                  copyXtnts,
                                  copyXferSize,
                                  (forceFlag ? &flag : &(bfAccess->xtnts.migTruncLk.l_wait_writers) )
                );
            /* 
             * It seems like a foul thing to do, to pass in the address of a 
             * private member of the lock structure to allow a foreign routine 
             * to access. It also would appear that this may be a little late 
             * in the calling tree to be checking this data. It may be more 
             * appropriate to check this much sooner. But this would require 
             * a more thorough investigation of this code and a clearer 
             * understanding of the intent.
             *
             * LATEST : The reason we're doing this "foul" thing here is that 
             * just before we start the migrate, we check to see if a truncate
             * has come in. If so, we abort the migrate (though caller can 
             * retry) as an optimization and let the truncate in. This way
             * we may end up not having to migrate anything when retried later.
             */
            
            if (sts != EOK) {
                return sts;
            }
            PREEMPT_CHECK(current_thread());
        }
    }  /* end for */

    /*
     * We must be sure that the on-disk original and copy match exactly.
     * If we don't, we return to our caller, the system crashes and
     * the copy and the original could be out of sync.
     *
     * Flush the cache so that the pages are written to the copy.  Only
     * the pages that are not pinned are written.
     */

    sts = bs_bf_flush_nowait ( bfAccess );
    if (sts != EOK) {
        return sts;
    }

    /*
     * Check each page and verify that it was written to each copy.  A page
     * is not written during a flush if the page is pinned.
     */

    for (i = 0; i < bfPageRangeCnt; i++) {
        if (bfPageRange[i].pageType == STG) {
            sts = verify_page_write ( bfAccess,
                                      bfPageRange[i].pageOffset,
                                      bfPageRange[i].pageCnt,
                                      copyXtnts,
                                      (forceFlag ? &flag : &(bfAccess->xtnts.migTruncLk.l_wait_writers) )
                );
            if (sts != EOK) {
                return sts;
            }
            PREEMPT_CHECK(current_thread());
        }
    }  /* end for */

    if (cp_debug != 0) {
        for (i = 0; i < bfPageRangeCnt; i++) {
            if (bfPageRange[i].pageType == STG) {
                sts = compare_copies ( bfAccess,
                                       bfPageRange[i].pageOffset,
                                       bfPageRange[i].pageCnt,
                                       copyXtnts,
                                       (forceFlag ? &flag : &(bfAccess->xtnts.migTruncLk.l_wait_writers) )
                    );
                PREEMPT_CHECK(current_thread());
            }
        }  /* end for */
    }

    return sts;
}  /* end cp_copy_page_range */


/*
 * set_block_map
 *
 * This function sets each page's block map and buffer state so that the
 * page is written to the appropriate on-disk locations.
 *
 * If the page is dirty, the block map is modified so that the page is
 * written to both the original and the copy locations.  The buffer state
 * is not modified.
 *
 * If the page is not dirty, the block map is modified so that the page
 * is written only to the copy locations and the buffer's "remap" state
 * bit is set.  If some other thread unpins (LOG_PAGE | MOD_LAZY | MOD_SYNC)
 * the page, the "remap" bit causes the unpin code to remap the block map
 * so that the page is written to both the original and the copy locations.
 *
 * A buffer's "dirty" state bit is set when the page is unpinned (LOG_PAGE |
 * MOD_LAZY | MOD_SYNC) the first time and is cleared when the page is written
 * to disk.
 *
 * A buffer's "remap" state bit is set by this function and is cleared by
 * the unpin code when another thread unpins (LOG_PAGE | MOD_LAZY | MOD_SYNC).
 * Otherwise, the bit is not cleared until the buffer is re-used for a different
 * page.  The bit is not cleared after the page is written because of the
 * following scenario:
 *
 *    This function pins a page and finds that the "dirty" bit is clear.
 *    The function modifies the block map so that the page is written
 *    to the copy only and sets the "remap" bit.  The page is eventually
 *    written to disk.  No other thread pins/unpins the page before it is
 *    written.  This means that the unpin code has not remapped the block
 *    map.  If another thread pins the buffer after it has been written
 *    and before it is re-used, the block map is not correct.  The map
 *    must be modified so that the page is written to the original as well
 *    as the copy.  Because the "remap" bit is still set, it causes the
 *    unpin code to remap the block map when the thread unpins the page.
 *
 * NOTE: If this function fails, the caller is responsible for the block maps.
 *
 * The BS_MOD_COPY unpin of a buffer that has only 1 writeref (migrate
 * is the only pinner) will place the buffer on the flushQ for quicker
 * writing.  This helps reduce ubc memory consumption by cleaning pages
 * as fast as possible.  A throttle on the number of pages this migrate
 * can unpin to the flushQ is used to stall the migrate if the device is
 * not writing fast enough to keep us below the intended ubc limit.  This
 * prevents migrate from hogging the ubc and degrading normal application
 * performance.
 *
 * For a disk with a 256 block prefered transfer size doing 2 GB migrate, there
 * is a cond_wait issued about every 70 MB of data transfer.  This is nearly 
 * 20 seconds  between issues.  The wallclock time is degraded by about 
 * 10% over the case with unlimited ubc usage and no trips are made through
 * the scheduler.  The ubc usage is constrained to 4 MB for this migrate.
 */

static
statusT
set_block_map (
               bfAccessT *bfAccess,      /* in */
               uint32T bfPageOffset,     /* in */
               uint32T bfPageCnt,        /* in */
               bsInMemXtntT *copyXtnts,  /* in */
               uint32T copyXferSize,     /* in */
               int *abortFlag            /* in */
               )
{
    blkDescT blkDesc[BLKDESC_CNT];
    blkMapT blkMap;
    int clearIOTransFlag = 0;
    unsigned int dirtyFlag;
    uint32T i,
            maxMigrateUbcPgs;
    void *page;
    bfPageRefHT pgPin;
    statusT sts;
    int unpinFlag = 0;
    bsUnpinModeT unpinMode;

    blkMap.maxCnt = BLKDESC_CNT;
    blkMap.blkDesc = &(blkDesc[0]);

    /*
     * Set limit on number of pages we may make dirty to the smaller
     * of the number of pages in 32 prefered I/O transfers for this 
     * device or 25% of max ubc size or 25% of the available vm space,
     * or 512 MB.  Both 32x and 25% are arbitrary.  The 32 transfers 
     * is because we want enough data to keep the device busy so we 
     * don't stop-start. The 25% prevents overflowing a small remaining 
     * vm with a large device xfer limit.
     *
     * In the calculation, note that copyXferSize is in 512-byte
     * blocks, and there are 16 blocks per 8k page.  Also note that the
     * real current max size of ubc is the smaller of the configured 
     * ubc_maxpages limit or the current ubc_pages + vm_free_count, 
     * because vm may be committed to uses outside the ubc.
     */
    
    maxMigrateUbcPgs = MIN(32*(copyXferSize/16), 
                            MIN( CURRENT_UC()->ubc_maxpages/4,
                                 MIN( (CURRENT_UC()->ubc_pages +
                                 CURRENT_VC()->vm_free_count)/4, MAX_COPY_XFER_SIZE)));

    for (i = bfPageOffset; i < (bfPageOffset + bfPageCnt); i++) {

        if (*abortFlag != 0) {
            RAISE_EXCEPTION (E_INVOLUNTARY_ABORT);
        }

        /*
         * Pin the page.  This prevents the page from being written while
         * we modify the buffer's state and block map.
         *
         * Tell pinpg to set the FLUSH state bit. This synchronizes
         * this function's page pin with bitfile flush and log trimming.
         * Flag must be explicitly cleared prior to unpinning the page.
         *
         * NOTE: If the page is pinned, the buffer isn't on any queue.
         * It gets put onto a queue at the last unpin.
         */

        sts = bs_pinpg_int (&pgPin, &page, bfAccess, i, BS_SEQ_AHEAD,
                            PINPG_SET_FLUSH);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        unpinFlag = 1;

        /*
         * Set the next valid page so that any "page-to-block" translation
         * for this page includes the copy's blocks.
         *
         * No lock is necessary when setting the next valid page offset because
         * this thread is the only thread that modifies this copy's extent maps.
         * If another thread does a page-to-block-map translation on this page,
         * it may or may not get the copy's map.  It doesn't matter either way
         * because we are processing the page.
         */

        imm_set_next_valid_copy_page (copyXtnts, i + 1);

        /*
         * Set the buffer's state.  When the state is set, other threads cannot
         * examine the buffer's dirty and remap flags and its block map.
         */

        sts = bs_set_bufstate (pgPin, __LINE__, IO_TRANS, TRUE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        clearIOTransFlag = 1;

        /*
         * Assume that the on-disk page contains valid data and was read into
         * the buffer cache from disk.  It would be nice if the buffer cache
         * could tell us if the on-disk page had ever been written.  If it does
         * not contain valid data, we could skip the next part and process the
         * next page.
         */

        /*
         * Get the buffer's dirty flag.  Because we set IO_TRANS, we block
         * unpinners and prevent them from setting the dirty flag.
         */

        sts = bs_get_bufstate (pgPin, DIRTY, &dirtyFlag);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        if (dirtyFlag == 0) {

            /*
             * The page may or may not have been in the cache and it is not
             * dirty.  Set the buffer's block map so that the contents of the
             * page is written to the copy only.  Also, set the buffer's remap
             * flag.
             *
             * Because we set the buffer's block map based on the dirty flag,
             * we must cause a remap when the buffer transitions from not dirty
             * to dirty.  The remap flag indicates to the unpin code that it
             * must do a remap.  When another thread unpins the buffer, the
             * unpin code examines the remap flag before inserting the buffer
             * onto an i/o queue.  If the remap flag is set, the unpin code sets
             * the buffer's block map so that the buffer's contents is written
             * to the copy as well as the original.  It also clears the remap
             * flag.
             *
             * The unpin code is the only code that clears the remap flag.  The
             * i/o completion routine does not clear this flag.  This is because
             * if the buffer is written, it was only written to the copy.  If
             * another thread pins/unpins the buffer, the buffer's contents must
             * be written to both the copy and the original.
             */

            XTNMAP_LOCK_READ( &(bfAccess->xtntMap_lk) )

            /*
             * NOTE: If multiple copy operations  are in-progress, the block map
             * is set so that the buffer's contents is written to every copy.
             */
            sts = x_copypage_to_blkmap( bfAccess,
                                        &bfAccess->xtnts,
                                        i,
                                        &blkMap);

            XTNMAP_UNLOCK( &(bfAccess->xtntMap_lk) )

            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            sts = bs_set_bufstate (pgPin, __LINE__, REMAP, FALSE);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

        } else {

            /*
             * The page was already in the cache, it has been modified and has
             * not been written to disk.  Since the page has been modified, the
             * page must be written to both the original and the copy.
             */

            sts = x_page_to_blkmap (bfAccess, i, &blkMap);
            if (sts != EOK && sts != W_NOT_WRIT ) {
                RAISE_EXCEPTION (sts);
            }

        }  /* endif dirtyFlag == 0 */

        sts = buf_remap (pgPin, &blkMap);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        clearIOTransFlag = 0;
        sts = bs_clear_bufstate (pgPin, IO_TRANS);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /* bufstate cleared by i/o completion */
        sts = bs_set_bufstate (pgPin, __LINE__, COPY, FALSE);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /* Clear the FLUSH flag to allow other routines to flush page. */
        sts = bs_clear_bufstate (pgPin, FLUSH);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        unpinFlag = 0;
        unpinMode.cacheHint = BS_RECYCLE_IT;
        unpinMode.rlsMode = BS_MOD_COPY;
        sts = bs_unpinpg (pgPin, logNilRecord, unpinMode);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        /*
         * When the device is not writing as fast as we are filling
         * the flushQ, wait for it to drop to 1/2 our limit.  This
         * way we use less ubc and still keep I/O in the pipeline.
         */

        if (bfAccess->migPagesPending >= maxMigrateUbcPgs) {
            mutex_lock(&bfAccess->bfIoLock);
            if (bfAccess->migPagesPending >= maxMigrateUbcPgs) {
                MS_SMP_ASSERT( maxMigrateUbcPgs/2 <= 0x7fff );
                bfAccess->migWait = maxMigrateUbcPgs/2;
                cond_wait(&bfAccess->migWait, &bfAccess->bfIoLock);
            }
            mutex_unlock(&bfAccess->bfIoLock);
        }

        PREEMPT_CHECK(current_thread());

    }  /* end for */
    return EOK;

HANDLE_EXCEPTION:

    if (clearIOTransFlag != 0) {
        bs_clear_bufstate (pgPin, IO_TRANS);
    }
    if (unpinFlag != 0) {
        bs_clear_bufstate (pgPin, FLUSH);
        unpinMode.cacheHint = BS_RECYCLE_IT;
        unpinMode.rlsMode = BS_MOD_COPY;
        bs_unpinpg (pgPin, logNilRecord, unpinMode);
    }
    return sts;
}  /* end set_block_map */


/*
 * verify_page_write
 *
 * This function verifies that the specified page range has been written to
 * disk.  If a page in the range is not in the cache, it assumes that the
 * page was written.  Otherwise, if the page has not been written, it does
 * a raw read-from-original/write-to-copy pair to write the page to the copy.
 */

static
statusT
verify_page_write (
                   bfAccessT *bfAccess,  /* in */
                   uint32T bfPageOffset,  /* in */
                   uint32T bfPageCnt,  /* in */
                   bsInMemXtntT *copyXtnts,  /* in */
                   int *abortFlag  /* in */
                   )
{
    int blkCnt;
    unsigned char *buf = NULL;
    unsigned int copyFlag;
    uint32T i;
    void *page;
    bfPageRefHT pgPin;
    statusT sts;
    int unpinFlag = 0;
    bsUnpinModeT unpinMode;

    blkCnt = bfAccess->bfPageSz;
    buf = (unsigned char *) ms_malloc (blkCnt * BS_BLKSIZE);
    if (buf == NULL) {
        return ENO_MORE_MEMORY;
    }

    /* FIX - only write pages that were in the original.  Is this taken care of
     * by the caller??  What I mean is, the caller never specifies a range that
     * it doesn't want written.
     */
    /*
     * FIX - get help from the buffer cache??  Does it have a list of buffers
     * so that I can just scan the list??
     */

    for (i = bfPageOffset; i < (bfPageOffset + bfPageCnt); i++) {

        if (*abortFlag != 0) {
            RAISE_EXCEPTION (E_INVOLUNTARY_ABORT);
        }

        if (bs_find_page (bfAccess,  i, 0) != 0) {
            /*
             * Pin the page.  This prevents the page from being written while
             * we modify the buffer's state and, optionally, copy the page
             * to the copy location.
             *
             * Tell pinpg to set the FLUSH state bit. This synchronizes
             * this function's page pin with bitfile flush and log trimming.
             *
             * NOTE: If the page is pinned, the buffer isn't on any queue.
             * It gets put onto a queue at the last unpin.
             */
 
            sts = bs_pinpg_int (&pgPin, &page, bfAccess, i, BS_NIL,
                                PINPG_SET_FLUSH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            unpinFlag = 1;

            /*
             * The i/o completion code clears the copy flag.  If it is clear,
             * the buffer's contents were written to disk.  Otherwise, we
             * must write this buffer to the copy location.
             */

            sts = bs_get_bufstate (pgPin, COPY, &copyFlag);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
            if (copyFlag != 0) {

                /*
                 * Because we have the buffer pinned, the buffer's contents
                 * cannot be written to disk.  If the page hasn't been written
                 * to the copy, the page pin blocks writes to the on-disk
                 * original while we copy the page.
                 */

                sts = force_write (bfAccess, i, buf, copyXtnts);
                if (sts != EOK) {
                    RAISE_EXCEPTION (sts);
                }
            }

            /* Clear the FLUSH flag to allow other routines to flush page. */
            sts = bs_clear_bufstate (pgPin, FLUSH);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            unpinFlag = 0;
            unpinMode.cacheHint = BS_RECYCLE_IT;
            unpinMode.rlsMode = BS_NOMOD;
            sts = bs_unpinpg (pgPin, logNilRecord, unpinMode);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
        }
        PREEMPT_CHECK(current_thread());
    }  /* end for */

    ms_free (buf);
    return EOK;

HANDLE_EXCEPTION:

    if (buf != NULL) {
        ms_free (buf);
    }
    if (unpinFlag != 0) {
        bs_clear_bufstate (pgPin, FLUSH);
        unpinMode.cacheHint = BS_RECYCLE_IT;
        unpinMode.rlsMode = BS_NOMOD;
        bs_unpinpg (pgPin, logNilRecord, unpinMode);
    }
    return sts;
}  /* end verify_page_write */


/*
 * force_write
 *
 * This function reads the specified page and writes it to the locations
 * mapped by the copy extent maps.  FIX - is this accurate??
 */

static
statusT
force_write (
             bfAccessT *bfAccess,  /* in */
             uint32T bfPageOffset,  /* in */
             unsigned char *buf,  /* in */
             bsInMemXtntT *copyXtnts  /* in */
             )
{
    blkDescT blkDesc[BLKDESC_CNT];
    blkMapT blkMap;
    int i;
    statusT sts;
    int bfBlkCnt = bfAccess->bfPageSz;

    blkMap.maxCnt = BLKDESC_CNT;
    blkMap.blkDesc = &(blkDesc[0]);

    sts = x_page_to_blkmap (bfAccess, bfPageOffset, &blkMap);
    if (sts != EOK && sts != W_NOT_WRIT) {
        return sts;
    }
    if (blkMap.readCnt <= 0) {
        ADVFS_SAD1("force_write --- bad block map (readCnt: N1)",
                   blkMap.readCnt);
    }

    sts = bs_raw_page (
                       bfAccess,
                       blkMap.blkDesc[blkMap.read].vdIndex,
                       blkMap.blkDesc[blkMap.read].vdBlk,
                       bfBlkCnt,
                       buf,
                       RREAD
                       );
    if (sts != EOK) {
        return sts;
    }

    /*
     * FIX - is this correct?  Even if multiple copies are in progress,
     * we just want to write to our copy?
     */
    sts = x_copypage_to_blkmap( bfAccess,
                                copyXtnts,
                                bfPageOffset,
                                &blkMap);
    if (sts != EOK) {
        return sts;
    }
    if (blkMap.writeCnt <= 0) {
        ADVFS_SAD1("force_write --- bad copy block map (writeCnt: N1)",
                   blkMap.writeCnt);
    }

    for (i = blkMap.write; i < (blkMap.write + blkMap.writeCnt); i++) {

        sts = bs_raw_page (
                           bfAccess,
                           blkMap.blkDesc[i].vdIndex,
                           blkMap.blkDesc[i].vdBlk,
                           bfBlkCnt,
                           buf,
                           RWRITE
                           );
        if (sts != EOK) {
            return sts;
        }

    }  /* end for */

    return sts;
}  /* end force_write */


/*
 * compare_copies
 *
 * This function compares the page contents of the original and the copies.
 * 
 * This is for debug/verification only.
 */

static
statusT
compare_copies (
                bfAccessT *bfAccess,  /* in */
                uint32T bfPageOffset,  /* in */
                uint32T bfPageCnt,  /* in */
                bsInMemXtntT *copyXtnts,  /* in */
                int *abortFlag  /* in */
                )
{
    int blkCnt;
    blkDescT blkDesc[BLKDESC_CNT];
    blkMapT blkMap;
    int byteCnt;
    unsigned char *copyBuf;
    unsigned long *copyLongword;
    unsigned char *origBuf;
    unsigned long *origLongword;
    int i;
    int j;
    int k;
    int longwordCnt;
    void *page;
    bfPageRefHT pgRef;
    statusT sts;
    int unblockUnpinsFlag = 0;
    int unpinFlag = 0;
    bsUnpinModeT unpinMode;

    /* FIX */
    ADVFS_SAD0("compare_copies --- not yet supported");
    return EOK;

#ifdef notfixed

    blkMap.maxCnt = BLKDESC_CNT;
    blkMap.blkDesc = &(blkDesc[0]);

    blkCnt = bfAccess->bfPageSz;
    byteCnt = blkCnt * BS_BLKSIZE;

    /*
     * Allocate storage for the buffer array and buffers.
     */

    origBuf = (unsigned char *) ms_malloc (2 * byteCnt);
    if (origBuf == NULL) {
        return ENO_MORE_MEMORY;
    }
    copyBuf = origBuf + byteCnt;

    origLongword = (unsigned long *) origBuf;
    copyLongword = (unsigned long *) copyBuf;
    longwordCnt = byteCnt / sizeof (unsigned long);

    /* next page offset mapped by copyXtnts */
    while (bfPageOffset < (bfPageOffset + bfPageCnt)) {
        /* do stuff */
        /* next page offset mapped by copyXtnts */
    }  /* end while */

    for (i = bfPageOffset; i < (bfPageOffset + bfPageCnt); i++) {

        if (*abortFlag != 0) {
            RAISE_EXCEPTION (E_INVOLUNTARY_ABORT);
        }

        /*
         * Pin the page so that the on-disk versions do not change.
         * The BS_BLOCK_UNPINS ref hint is needed to synchronize with
         * any thread that unpins and specifies BS_MOD_SYNC.
         */

        sts = bs_pinpg_int (&pgRef, &page, bfAccess, i, BS_BLOCK_UNPINS,
                            PINPG_NOFLAG);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        unpinFlag = 1;
        unblockUnpinsFlag = 1;

        /*
         * Read the original and the copies into the buffers.
         */

        sts = x_page_to_blkmap (bfAccess, i, &blkMap);
        if (sts != EOK && sts != W_NOT_WRIT) {
            RAISE_EXCEPTION (sts);
        }
        if (blkMap.readCnt <= 0) {
            ADVFS_SAD1("compare_copies --- bad block map (readCnt: N1)",
                       blkMap.readCnt);
        }

#if 0
        origLongword[0] = 0xDEADBEEF;  /* FIX - delete after test */
#endif
        sts = bs_raw_page (
                           bfAccess,
                           blkMap.blkDesc[blkMap.read].vdIndex,
                           blkMap.blkDesc[blkMap.read].vdBlk,
                           blkCnt,
                           origBuf,
                           RREAD
                           );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
#if 0
        if (origLongword[0] == 0xDEADBEEF) {
            ADVFS_SAD0("origLongword, dead beef");
        }
#endif
        for (j = 0; j < xtntMapCnt; j++) {

            sts = x_page_to_blk( bfAccess,
                                 i,
                                 xtntMapList[j],
                                 &blkDesc[0]);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

#if 0
            copyLongword[1] = 0xDEADBEEF;  /* FIX - delete after test */
#endif
            sts = bs_raw_page (
                               bfAccess,
                               blkDesc[0].vdIndex,
                               blkDesc[0].vdBlk,
                               blkCnt,
                               copyBuf,
                               RREAD
                               );
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }
#if 0
            if (copyLongword[1] == 0xDEADBEEF) {
                ADVFS_SAD0("copyLongword, dead beef");
            }
#endif
            /*
             * Compare the original and the copy.
             */

            for (k = 0; k < longwordCnt; k++) {

                if (copyLongword[k] != origLongword[k]) {
                    printf("compare_copies --- mismatch - " 
                           "page: %d, copy: %d, longword: %d, expected: %x, "
                           "actual: %x./n",
                           i, j, k, origLongword[k], copyLongword[k]);
                    ADVFS_SAD0( "compare_copies" );
                }
            }  /* end for */
        }  /* end for */

        unblockUnpinsFlag = 0;
        sts = bs_unblock_unpins (pgRef);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        unpinFlag = 0;
        unpinMode.cacheHint = BS_RECYCLE_IT;
        unpinMode.rlsMode = BS_NOMOD;
        sts = bs_unpinpg (pgRef, logNilRecord, unpinMode);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        PREEMPT_CHECK(current_thread());
    }  /* end for */

    ms_free (copyBuf);
    ms_free (origBuf);

    return EOK;

HANDLE_EXCEPTION:

    if (copyBuf != NULL) {
        ms_free (copyBuf);
    }

    if (origBuf != NULL) {
        ms_free (origBuf);
    }

    if (unblockUnpinsFlag != 0) {
        bs_unblock_unpins (pgRef);
    }
    if (unpinFlag != 0) {
        unpinMode.cacheHint = BS_RECYCLE_IT;
        unpinMode.rlsMode = BS_NOMOD;
        bs_unpinpg (pgRef, logNilRecord, unpinMode);
    }

    return sts;
#endif  /* notfixed */
}  /* end compare_copies */


/*
 * cp_insert_onto_xtnt_map_list
 *
 * This function inserts the specified extent map onto the
 * extent map list.
 */

void
cp_insert_onto_xtnt_map_list (
                              bsInMemXtntMapT **xtntMapListhead,  /* in */
                              bsInMemXtntMapT *targetXtntMap  /* in */
                              )
{
    targetXtntMap->nextXtntMap = *xtntMapListhead;
    *xtntMapListhead = targetXtntMap;
    return;

}  /* end cp_insert_onto_xtnt_map_list */


/*
 * cp_remove_from_xtnt_map_list
 *
 * This function removes the specified extent map from the
 * extent map list.
 */

statusT
cp_remove_from_xtnt_map_list (
                              bsInMemXtntMapT **xtntMapListhead,  /* in */
                              bsInMemXtntMapT *targetXtntMap  /* in */
                              )
{
    bsInMemXtntMapT *xtntMap;

    if (targetXtntMap == *xtntMapListhead) {
        *xtntMapListhead = targetXtntMap->nextXtntMap;
        return EOK;
    }

    xtntMap = *xtntMapListhead;
    while (xtntMap != NULL) {
        if (targetXtntMap == xtntMap->nextXtntMap) {
            xtntMap->nextXtntMap = targetXtntMap->nextXtntMap;
            return EOK;
        }
        xtntMap = xtntMap->nextXtntMap;
    }
    return E_XTNT_MAP_NOT_FOUND;
}  /* end cp_remove_from_xtnt_map_list */
