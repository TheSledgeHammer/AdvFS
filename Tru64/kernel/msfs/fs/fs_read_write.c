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
 *   MegaSafe Storage System
 *
 * Abstract:
 *
 *   Procedures to read, and write a file: fs_read, fs_write 
 *
 * Date:
 *
 *   20-Jul-1990
 */            
/*
 * HISTORY
 * 
 * 
 * 
 * 
 */
#pragma ident "@(#)$RCSfile: fs_read_write.c,v $ $Revision: 1.1.468.28 $ (DEC) $Date: 2008/01/31 13:01:09 $" 

#include <msfs/fs_dir_routines.h>
#include <msfs/fs_dir.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/fs_file_sets.h>
#include <sys/param.h>
#include <sys/errno.h>
#include <msfs/ms_osf.h>
#include <sys/conf.h>
#include <sys/unix_defs.h>
#include <sys/mode.h>
#include <sys/mount.h>
#include <sys/vnode.h>
#include <sys/buf.h>
#include <sys/uio.h>
#include <sys/kernel.h>
#include <sys/lock_probe.h>
#include <msfs/ftx_public.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_params.h>
#include <sys/sp_attr.h>
#include <msfs/advfs_evm.h>
#include <sys/syslog.h>
#include <vm/vm_numa.h>
#include <sys/fcntl.h>

#define POSIX1B_MSFS_SYNC_STATS

#ifdef POSIX1B_MSFS_SYNC_STATS
struct {
    u_int fs_write_dsync_flush;
    u_int fs_write_dsync_update;
    u_int fs_write_sync_flush;
    u_int fs_write_sync_update;
    u_int fs_read_sync_flush;
    u_int fs_read_sync_update;
  } msfs_sync_stats = {0,0,0,0,0,0};
#define MSFS_SYNC_STATS(foo) foo

#else

#define MSFS_SYNC_STATS(foo)
#endif  /* ifdef POSIX1B_MSFS_SYNC_STATS */

#define ADVFS_MODULE FS_READ_WRITE

extern TestXtntsFlg;

extern vm_map_t kernel_map;
extern vm_offset_t virtualZeroRangeP;

#ifdef ADVFS_SMP_ASSERT
int ForceStgAddFailure = 0;
#endif /* ADVFS_SMP_ASSERT */

#define FIRST_BLK_IN_PG(offset)  (((offset) / ADVFS_PGSZ) * ADVFS_PGSZ_IN_BLKS)
#define LAST_BLK_IN_PG(offset) (FIRST_BLK_IN_PG(offset)+(ADVFS_PGSZ_IN_BLKS-1))

static char MegaSafePageOfZeros[ADVFS_PGSZ];

static
statusT
uiomove_frag (
           bfFragIdT fragId,  /* in */
           uint32T copyByteOffset,  /* in */
           uint32T copyByteCnt,  /* in */
           struct bfAccess *bfap,  /* in */
           struct uio *uio  /* in */
           );

statusT
bcopy_frag (
            struct vnode* vp,
            char* pgAddr
            );

static statusT
fs_write_direct(bfAccessT *bfap,
                struct uio *uio,
                struct ucred *cred,
                int *number_to_write,
                unsigned long byte_offset_in_page,
                int cowingDone,
                int ioflag,
                actRangeT *arp );

static statusT
fs_read_direct(bfAccessT *bfap,
               struct uio *uio,
               int *nbytes,
               unsigned long byte_offset_in_page);


/*
 * fs_zero_fill_pages
 *
 * Zero fills pages in a specified page range.  Note that the routine
 * will quit as soon as it hits an unallocated page.  Therefore, this
 * routine cannot be used to fill a sparse page range.
 *
 * We have added a performance optimization to skip the pin/zero/unipn
 * loop for any page that will be completely overwritten in fs_write().
 * Pre-allocated pages and partially-written pages should always be
 * zero'd.  In addition, any page that must be written to disk in this
 * routine must be zero'd.  If pages are not zero'd when they should be,
 * we will end up with data corruption of the form where non-zero data 
 * shows up in newly-allocated storage where zeros should be read.
 */
static unsigned
fs_zero_fill_pages(
    struct bfAccess *bfap,         /* in - file's access structure */
    int start_pg,                  /* in - start of page range */
    int end_pg,                    /* in - end of page range */
    u_int pages_to_be_overwritten, /* in - # pgs that will be overwritten */
    u_int first_page_is_overwritten, /* in - boolean */
    int data_logging,              /* in - is data logging on/off? */
    int sparse_alloc,              /* in - are we zero-filling a page allocated
                                           for a sparse write? */
    struct ucred *cred             /* in - credentials of caller */
    )
{
    statusT sts;
    unsigned pg;
    bfPageRefHT page_ref;
    int *page_addr;
    int npages = end_pg - start_pg + 1;
    vm_offset_t vm_start, ret;
    int asynch_io_started = FALSE;
    int bytes_written, bytes_to_transfer, number_to_write;
    long starting_blk_num;

    int force_immediate_write = ((data_logging && sparse_alloc) ||
                                 (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY));

    /* Optimization: if all the pages being allocated are going to
     * be overwritten in fs_write() and there is no reason to desire 
     * zeroing these pages with immediate flushing to disk, then return.
     */
    if ( !force_immediate_write        &&
         first_page_is_overwritten     &&
         (pages_to_be_overwritten >= npages) ) {
        return( npages );
    }

    /*
     * If force_immediate_write = 1 and virtualZeroRangeP != NULL, we want
     * to zero memory quickly and get it out to disk.
     */
    MS_SMP_ASSERT(virtualZeroRangeP != NULL);
    if ( force_immediate_write && ( virtualZeroRangeP != NULL ) ) {
        bytes_to_transfer = npages * ADVFS_PGSZ;
        starting_blk_num = (long)ADVFS_PGSZ_IN_BLKS * (long)start_pg;
        while ( bytes_to_transfer > 0 ) {

            number_to_write = bytes_to_transfer;

            if ( number_to_write > MAX_VIRT_VM_PAGE_RNG * ADVFS_PGSZ ) {
                number_to_write = MAX_VIRT_VM_PAGE_RNG * ADVFS_PGSZ;
            }

            sts = bs_pinpg_direct((void *)virtualZeroRangeP,
                                  number_to_write,
                                  bfap,
                                  starting_blk_num,
                                  UIO_SYSSPACE,
                                  NULL,
                                  NULL,
                                  &bytes_written,
                                  &asynch_io_started,
                                  FALSE,
                                  cred);

            if ( sts != EOK ) {
                MS_SMP_ASSERT(0);  /* We should never get here. */
                goto not_vm_zeroing;
            }

            bytes_to_transfer -= bytes_written;
            starting_blk_num  += bytes_written / BS_BLKSIZE;
        }

        return( npages );
    }

not_vm_zeroing:

    for (pg = start_pg; pg <= end_pg; pg++) {

        if ( !force_immediate_write ) {
            if ( pg == start_pg ) {
                /* account for the first page */
                if ( first_page_is_overwritten ) {
                    pages_to_be_overwritten--;
                    continue;
                }
            } else {
                /* skip any subsequent pages that will be overwritten */
                if ( pages_to_be_overwritten ) {
                    pages_to_be_overwritten--;
                    continue;
                }
            }
        }

        sts = bs_pinpg_int(&page_ref,
                           (void *) &page_addr,
                           bfap,
                           pg,
                           BS_OVERWRITE,
                           PINPG_NOFLAG );

        if (sts != EOK) {
            /* reached end of allocated pages or error; quit */
            return ( pg - start_pg );
        }

        bzero((char *)page_addr, ADVFS_PGSZ);

        (void) bs_unpinpg( page_ref, logNilRecord, BS_DIRTY );
    }

    if ( force_immediate_write ) {
        bfflush(bfap, start_pg, end_pg-start_pg+1, FLUSH_IMMEDIATE);
    }
    return( pg - start_pg );
}

static void
fs_write_cow( bfAccessT *bfap, off_t foffset, off_t nbyte )
{
    bsPageT pg;
    off_t rlimit_fsize;
    off_t file_size_limit;
    bsPageT pgOff = foffset / ADVFS_PGSZ;
    bsPageT pgCnt = (((foffset + nbyte - 1)/ADVFS_PGSZ) - pgOff) + 1;

    rlimit_fsize = u.u_rlimit[RLIMIT_FSIZE].rlim_cur;
    file_size_limit = MIN(rlimit_fsize, max_offset + 1);
    if ( foffset >= file_size_limit ) {
        return;
    }

    if ( foffset + nbyte > file_size_limit ) {
        pgCnt = howmany(file_size_limit - foffset, ADVFS_PGSZ);
    }

    /* cow any pages in the range from pgOff to pgOff+pgCnt, as needed */
    bs_cow( bfap, COW_PINPG, pgOff, pgCnt, FtxNilFtxH );
}

/* efbig_check
 *
 * This routine checks whether the write would exceed limits like
 * max offset (AdvFS maximum file offset) and max file size (RLIMIT_SIZE).
 *
 * The context structure file_lock is held shared or exclusive
 * when this routine is called.
 */

static int
efbig_check( struct bfAccess *bfap,     /* in - file's access structure */
             struct uio *uio,           /* in - structure for uiomove() */
             unsigned long *nbyte,      /* in/out - number of bytes to write */
             unsigned long size,        /* in - file size */
             unsigned long *efbig,      /* in/out - Num bytes not to write */
             int *signal_error,         /* in/out - flag to signal error */
             int ioflag,                /* in - flags for i/o (append, sync) */
             int *data_logging          /* in/out - is data logging on/off? */
           )
{
    unsigned long residual;             /* Number of bytes not to write */
    unsigned long rlimit_fsize;         /* RLIMIT_FSIZE for process */
    unsigned long file_size_limit;      /* MIN(RLIMIT_FSIZE or max_offset) */
    unsigned long future_file_size;     /* Assuming we are extending the */
                                        /* file this is the future size of */
                                        /* the file for limit checking */
                                        /* purposes */

    /*
     * See if the file is attempting to start writing past the maximum file
     * size (AdvFS max file size or per process RLIMIT_FSIZE).  The AdvFS
     * maximum file size is calculated by adding 1 to the AdvFS max_offset.
     */
    rlimit_fsize = u.u_rlimit[RLIMIT_FSIZE].rlim_cur;
    file_size_limit = MIN(rlimit_fsize, max_offset+1);

    if (uio->uio_offset >= file_size_limit) {
        if ( uio->uio_offset >= rlimit_fsize ) {
            /*
             * Exceeding RLIMIT_FSIZE generates a SIGXFSZ signal.
             * There is some debate about whether exceeding the AdvFS
             * max_offset should also generate a SIGXFSZ signal.  See
             * the comments at the beginning of the fs_write() code for
             * more details.  For now, exceeding max_offset does not
             * generate a signal.
             */
            *signal_error = 1;
        }
        *efbig = 0;
        return EFBIG;
    }

    /*
     * See if atomic write data logging is turned on.  Use it here and
     * return the flag so that fs_write() can use it as well.  fs_write()
     * needs to recheck for atomic write data logging on every fs_write()
     * retry pass. 
     */
    *data_logging = (((bfap->dataSafety == BFD_FTX_AGENT) ||
                      (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY)) ? TRUE : 
                                                                       FALSE);

    /*
     * We also have to check to see if we are growing the file past the
     * limits, so calculate the size of the file after the write is finished
     * (assuming this write actually grows the file; if we are not growing
     * the file then this is just the next write offset, and since we would
     * be within the existing file, no errors would be generated). 
     */
    future_file_size = uio->uio_offset + uio->uio_resid;

    if (future_file_size > file_size_limit) {
        if ((ioflag & (IO_SYNC | IO_DSYNC)) ||
            (*data_logging && (uio->uio_resid <= ADVFS_PGSZ))) {
            /*
             * No partial write when IO_SYNC, IO_DSYNC, or data_logging.
             *
             * IO_SYNC and IO_DSYNC are only supposed to return success if
             * they are able to write the entire request to the file.
             * Since we do not have enough room, we will not be able to
             * succeed, so return an error now.  
             *
             * We also do the same for data logging files if the write is
             * less than ADVFS_PGSZ bytes in length.  That is because
             * we currently guarantee that writes to files using atomic
             * write data logging will be atomic in segments of 8k. 
             * Therefore, if we want to write 8k or less and that will put
             * us over the limit, don't write anything. 
             */
            if ( future_file_size > rlimit_fsize ) {
                /*
                 * Since the only way we could perform this write is to
                 * extend the file past RLIMIT_FSIZE, then that falls under
                 * the rules for generating a SIGXFSZ signal.  There is some
                 * debate about whether exceeding the AdvFS max_offset
                 * should also generate a SIGXFSZ signal.  See the comments
                 * at the beginning of the fs_write() code for more details.
                 * For now, exceeding max_offset does not generate a signal.
                 */
                *signal_error = 1;
            }
            *efbig = 0;
            return EFBIG;
        }

        /*
         * The process ulimit (or AdvFS max file size) has been reached. 
         * The standards say we do a partial write of as much as we can.  
         */
        residual = future_file_size - file_size_limit;
        if (*data_logging) {
            /*
               For data logging files (_NOT_ covered by the standards), we
               will write out as many whole 8K segments as possible (because
               that is the way it was architected).  We calculate the number
               of bytes that could be transferred (max - offset) and take
               the modulo 8K of that.  The modulo value is the first part of
               the 8K segment that would have fit under the limit.  Because
               the limit would not allow transferring all of that segment as
               an atomic operation, we need to include the modulo value as
               part of the residual that is not to be written. 
             */
            residual += (file_size_limit - uio->uio_offset) % ADVFS_PGSZ;
        }
        *efbig = residual;              /* return bytes not to be written */
        uio->uio_resid -= residual;     /* decrease size of transfer request */
        *nbyte -= residual;             /* decrease number of bytes to write */
    } else {
        *efbig = 0;
    }

    return 0;
}

/* Display messages and post EVM events when storage is gone in this
 * domain.
 */
static void
fs_post_no_storage_msg( struct vnode *vp,
                        struct bfAccess *bfap )
{
    int set = 0;
    advfs_ev *advfs_event;
    extern int fs_full_threshold;

    /* Error to controlling terminal. */
    if (!AT_INTR_LVL() && !SLOCK_COUNT) {
        uprintf("\n%s: write failed, file system is full\n",
                VTOMOUNT(vp)->m_stat.f_mntonname);
    }

    /*
     * Post EVM event and write to syslog/console only
     * once every fs_full_threshold.
     */
    mutex_lock(&(bfap->dmnP->mutex));
    if ((bfap->dmnP->fs_full_time == 0) ||
        (time.tv_sec - bfap->dmnP->fs_full_time >= fs_full_threshold)) {
        bfap->dmnP->fs_full_time = time.tv_sec;
        set=1;
    }
    mutex_unlock(&(bfap->dmnP->mutex));

    if (set) {
        log(LOG_ERR, "%s: file system full\n",
                             VTOMOUNT(vp)->m_stat.f_mntonname);

        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = bfap->dmnP->domainName;
        advfs_post_kernel_event(EVENT_FDMN_FULL, advfs_event);
        ms_free(advfs_event);
    }
    return;
}
                   

/* Sequential write tuning parameters.
 * aggressiveWriteFlush - Allow a flush during a write call. 
 *                        Set to 0 to delay cache writes until smoothsync
 *                        or UBC flushes dirty pages.
 * aggressiveWritePcnt  - Percent limit of total available UBC pages that
 *                        can be dirty or busy before issuing an aggressive 
 *                        write synchronous flush IO.  If free memory is
 *                        low (vm_free_count < vm_prewrite_target) a flush
 *                        is forced and this limit is ignored.
 * seqWritePgMax - Number of pages to accumulate for each aggressive flush.
 */

int aggressiveWriteFlush = 1;
long aggressiveWritePcnt = 70;
unsigned long seqWritePgMax = 32;

/*    fs_write
 *
 *    Write to a file
 *
 *    Note on atomic write data logging:
 *        (Atomic write) data logging ensures that writes of up to
 *        ADVFS_PGSZ (currently 8192 in msfs/msfs/bs_public.h) bytes from
 *        user applications are done within a single AdvFS transaction.
 *        Such a write may traverse at most two AdvFS pages.
 *
 *    Note on IO_SYNC (O_SYNC or FSYNC) and IO_DSYNC (O_DSYNC or FDSYNC):
 *        The standards (UNIX98) for IO_DSYNC say "The write is complete
 *        only when the data specified in the write request is successfully
 *        transferred and all file system information required to retrieve
 *        the data is successfully transferred."  IO_SYNC says "Identical to
 *        the IO_DSYNC with the addition that all file attributes relative
 *        to the I/O operation (including access time, modification time,
 *        status change time) will be successfully transferred prior to
 *        returning to the calling process."
 *
 *        When I/O Sync is enabled (IO_SYNC, IO_DSYNC, or BFD_SYNC_WRITE),
 *        _AND_ an error occurs, fs_write() restores the uio_resid and
 *        uio_offset fields to their original values.  This is done because
 *        rwuio()/prwuio() will ignore any errors reported if the uio_resid
 *        value has been modified.  There is some discussion about whether
 *        I/O Sync allows for partial writes to be returned when I/O Sync is
 *        enabled.  However the operation of fs_write() is to copy all the
 *        write() data into buffers, and then tell the lower layers to flush
 *        the buffers to disk.  At that point fs_write() doesn't control the
 *        order that the data is written.  Consolidated writes, and other
 *        optimizations may cause the buffers to be written in any order. 
 *        Plus there are RAID controllers that may introduce
 *        additional optimizations.  This makes it difficult to know in an
 *        error situation just which part of the I/O made it to disk and if
 *        that was the first part of the request.  So for AdvFS, we treat
 *        I/O Sync as either all of the data was written or we report an
 *        error.  This means we have to restore the uio_resid and uio_offset
 *        to their original values before returning, so that
 *        rwuio()/prwuio() will report the errors. 
 *
 *        Data logging does have the ability to insure that we know just
 *        what part of the data was written, and return a partial write when
 *        I/O Sync is enabled, however, the UNIX98 standards are unclear as
 *        to whether the I/O Sync wording overrides the partial write
 *        wording.  Until that is clearly resolved, we are taking the
 *        position that if we can not transfer everything requested by the
 *        caller, we will return an error.  This is subject to change. 
 *
 *    Note on I/O sync _AND_ data logging:
 *        I/O sync has priority over data logging, if both I/O sync and
 *        data_logging are enabled and an error occurs, the exit path will
 *        choose the I/O sync error path over the data logging error path.
 *
 *    Note on partial writes:
 *        No partial write is allowed for IO_SYNC, IO_DSYNC, or
 *        data_logging, when the current write will exceed the file size
 *        limit or there is no more storage left for the write.
 *
 *        Async I/O allows for a partial write.  From the UNIX98 spec:  "If
 *        a write() requests that more bytes be written than there is room
 *        for (for example, the ulimit or the physical end of a medium),
 *        only as many bytes as there is room for will be written.  For
 *        example, suppose there is space for 20 bytes more in a file before
 *        reaching a limit.  A write of 512 bytes will return 20.  The next
 *        write of a non-zero number of bytes will give a failure return and
 *        the implementation will generate a SIGXFSZ signal for the thread."
 *
 *        This might be interpreted to mean that a partial write is only
 *        allowed in the cases where a resource limit had been reached
 *        (ulimit, file system full, max file offset, quota exceeded,
 *        etc...) and that there were no problems encountered while writing
 *        the partial amount (async I/O only).  However, any errors that
 *        indicate a failure to perform the I/O should not trigger a partial
 *        write, and the error should be returned.
 *
 *        There are other interpretations that if any error occurs after
 *        data has been copied into a system buffer, that a partial write
 *        should be returned (this only applies to async I/O).
 *
 *        The current async I/O implementation always returns a partial
 *        write on any error.
 *
 *    Note on SIGXFSZ:
 *        There is some debate about SIGXFSZ and when it should be signaled.
 *        The standards (UNIX98) are not explicitly clear.  One view is that
 *        it should be signaled when an EFBIG, ENOSPC, or E_TOO_MANY_BLOCKS
 *        (quota) error conditions _AND_ no data has been transferred yet.
 *        That is to say when the request can not be started because there
 *        is no more room for even the first byte of data and this could be
 *        because the disk is full, the ulimit for the file has been
 *        exceeded, the maximum file offset for file has been exceeded, or
 *        there is no more quota for this user.
 *
 *        Another view is that it only applies to ulimit.
 *
 *        A middle view is that it may apply to some of the above
 *        situations, but because we have not signaled SIGXFSZ for these
 *        other cases before, we should not do it now.  The reason not to
 *        change is that it may break existing customer applications.  So
 *        until we have a better reason we should not change the code now.
 */
extern int strict_posix_osync;

#ifdef ADVFS_NO_ZERO_ALLOCATE
unsigned long ADVFS_NO_ZERO = 0;  /* easy way to identify kernel */
#endif

int
fs_write(
         struct vnode *vp,              /* in - vnode of file to write */
         struct uio *uio,               /* in - structure for uiomove */
         int ioflag,                    /* in - flags - append, sync */
         struct ucred *cred             /* in - credentials of caller */
         )
{
    statusT ret;
    unsigned long page_to_write, starting_byte, size;
    unsigned long efbig = 0;
    unsigned long f_offset, bytes_left, bytes_written;
    unsigned long tmp_page_to_write, tmp_starting_byte, tmp_number_to_write;
    unsigned long tmp_offset, tmp_bytes_left, tmp_bytes_written;
    int number_to_write; /* limited to "int" due to b_bcount of "buf struct" */
    struct fsContext *contextp;
    bfAccessT *bfap;
    bfPageRefHintT refHint;
    bfPageRefHT page_ref;
    char *first_page_addr, *second_page_addr;
    int retry=FALSE, sparse_alloc=FALSE, add_stg_flags;
    int error = 0;
    shareExclLkStateT lk_state;
    unsigned long nbyte;
    enum vtype type;
    domainT *dmnP;
    int32T directIO = 0;
    extern u_int noadd_execaccess;
    int data_logging = FALSE,
        bytes_pinned = 0,
        ftx_started = FALSE,
        first_pin_page_loop = TRUE;
    ftxHT parentftxH = FtxNilFtxH;
    rbfPgRefHT firstPgPin, secondPgPin;
    unsigned long  first_page_write_offset,
                   first_page_to_write,
                   second_page_to_write,
                   first_page_to_flush,
                   last_page_to_flush;
    uint32T  first_page_bytes_to_write,
             second_page_bytes_to_write = 0;
    int migtrunc_locked = FALSE,
        flushed_log = FALSE;
    off_t   orig_offset = uio->uio_offset;
    ssize_t orig_resid  = uio->uio_resid;
    struct uio saved_uio = *uio;
    uint32T end_of_nxtpage, pg_overun;
    int fill_hole = 0;
    int map_lock = 0;
    int uioerror, signal_error = 0;
    int strict_posix_osync_local;
    int need_stg_alloc = FALSE;
    int cowingChecked = FALSE;
    int no_zero_fill = ioflag & ADVFS_IO_NO_ZERO; /* secret flag */
    struct actRange *arp = NULL;

    /*
     * SVID III access rules vs BSD.
     * sys_v_mode predates the grpid mount option, but at present we support
     * both.  This affects group ownership of new inodes and preservation of
     * ISGID over modifications.
     */
    extern int sys_v_mode;
#define BSD_MODE(vp) (((vp)->v_mount->m_flag & M_GRPID) && !sys_v_mode)

    bfap = VTOA(vp);
    contextp = VTOC(vp);
    type = vp->v_type;
    dmnP = GETDOMAINP(VTOMOUNT(vp));
    f_offset = uio->uio_offset;
    nbyte = uio->uio_resid;

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }

    if (nbyte == 0) {
        return(EOK);
    }

    if ( type != VREG ) {
        return EOPNOTSUPP;
    }

    /* If a domain flush is pending, we don't want this I/O to get
     * started and cause that flush to stall.  So block this write
     * from starting until the flush has finished.
     */
    if ( bfap->dmnP->dmnFlag & BFD_DOMAIN_FLUSH_IN_PROGRESS ) {
        mutex_lock( &bfap->dmnP->mutex );
        if ( bfap->dmnP->dmnFlag & BFD_DOMAIN_FLUSH_IN_PROGRESS ) {
            assert_wait((vm_offset_t)(&bfap->dmnP->dmnFlag), FALSE);
            mutex_unlock( &bfap->dmnP->mutex );
            thread_block();
        } else {
            mutex_unlock( &bfap->dmnP->mutex );
        }
    }

    if ( bfap->bfSetp->cloneSetp != NULL ) {
        /* If necessary, cow any pages in this range. All locking is
         * handled internally.
         */
        fs_write_cow( bfap, f_offset, nbyte );
        cowingChecked = TRUE;
    }

    strict_posix_osync_local = strict_posix_osync;

    /*
     * If this file should have forced synchronous writes,
     * tweak the I/O flag to make it happen.
     */
    if (bfap->dataSafety == BFD_SYNC_WRITE) {
        ioflag |= IO_SYNC;
    }

_retry_map:
    if (vp->v_flag & VMMAPPED) {
        /*
         * Keep hierarchy order vm_map lock followed by
         * file_lock.  If uiomove() faults on user buffer
         * the fault path acquires the vm_map lock for read,
         * thus take the vm_map lock recursively.  This
         * hierarchy is necessary to avoid deadlock with
         * user fault on mmapped region which takes vm_map
         * lock for read then calls msfs_getpage() which
         * locks file lock.
         */
        VM_MAP_LOCK_READ_RECURSIVE();
        map_lock = 1;
    }

_retry:

    size = bfap->file_size;
    if (sparse_alloc || (f_offset + nbyte > size) || (ioflag & IO_APPEND)) {

        FS_FILE_WRITE_LOCK( contextp );
        need_stg_alloc = TRUE;

        if ((vp->v_flag & VMMAPPED) && !map_lock) {
            /*
             * File mmapped while obtaining file lock.  Must follow
             * hierarchy vm_map lock followed by file lock to avoid
             * deadlock with user page fault on file page.
             */
            VM_MAP_LOCK_READ_RECURSIVE_TRY(map_lock);
            if (!map_lock) {
                FS_FILE_UNLOCK( contextp );
                goto _retry_map;
            }
        }

        size = bfap->file_size;
        directIO = vp->v_flag & VDIRECTIO;
        lk_state = LKS_EXCL;

        pg_overun = bfap->file_size % ADVFS_PGSZ;
        end_of_nxtpage = (pg_overun) ?
                         (ADVFS_PGSZ - pg_overun) :
                         0 ;

        /* test for a growing file with a hole and preallocated blocks at EOF */

        if ( ( orig_offset > (bfap->file_size + end_of_nxtpage) ) &&
             ( fs_trunc_test(vp) )) {

            ftxHT ftxH;
            uint32T delCnt = 0;
            void *delList;
            ulong pages_used;
            ulong pagesize;

            /* 
             * At the end of the file that is growing only - (indicated by
             * f_offset >= size), there is a hole with preallocated pages
             * filling at least part of the hole.  We need to remove the
             * prealloced pages otherwise a block count (du) will show too
             * many pages for a sparse file.  - fixes a vrestore problem
             *
             * The byte to be written is not within the current page or the
             * next page (we wouldn't want to truncate if it was) AND there
             * already is some preallocated pages from the previous write to
             * this file --- so lets truncate the preallocated pages so the
             * hole is not filled with anything (including a 0'ed,
             * prealloced page)
             *
             * NOTE:  if seeking into a sparse file and writing, there may
             *        still be preallocated blocks in the holes, because
             *        this code is not run in that case. 
             */

            if ( directIO ) {
                /* Allocate an active range structure; needed to prevent
                 * racing directIO writers to the range of pages we
                 * are going to remove storage for.
                 */
                arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));

                /* Set up active range to include all underlying pages.
                 * Use same calculation here as in bf_setup_truncation.
                 */
                pagesize = ADVFS_PGSZ;
                pages_used = (bfap->file_size + pagesize - 1L) / pagesize;
                if ( bfap->fragState == FS_FRAG_VALID ) {
                    --pages_used;
                }
                arp->arStartBlock = pages_used * ADVFS_PGSZ_IN_BLKS;
                arp->arEndBlock = ~0L;
                arp->arState   = AR_TRUNCATE;
                arp->arDebug   = SET_LINE_AND_THREAD(__LINE__);

                insert_actRange_onto_list( bfap, arp, contextp );
            }

            /* Need the bfap lock because of call chain
             * bf_setup_truncation --> stg_remove_stg_start
             */
            MIGTRUNC_LOCK_READ ( &(bfap->xtnts.migTruncLk) );

            /* No need to take the quota locks here because
             * the quota entry already exists. It's only when
             * the file is created or the quota is explicitly
             * set, that storage may be added to the quota file
             *
             * The potential danger path is
             * bf_setup_truncation --> stg_remove_stg_start -->
             * fs_quota_blks --> dqsync --> rbf_add_stg
             * but it is tame in this case.
             */

            FTX_START_N (
                      FTA_FS_WRITE_TRUNC,
                      &ftxH,
                      FtxNilFtxH,
                      contextp->fileSetNode->dmnP,
                      1
                     );

           delCnt = bf_setup_truncation (bfap, ftxH, &delList, FALSE);
           bfap->trunc = FALSE;

           ftx_done_fs (ftxH, FTA_FS_WRITE_TRUNC, 0);

           MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) );

           if ( arp ) {
                /* Remove active range */
                mutex_lock(&bfap->actRangeLock);
                MS_SMP_ASSERT( arp->arIosOutstanding == 0 );
                remove_actRange_from_list(bfap, arp);
                mutex_unlock(&bfap->actRangeLock);
                ms_free(arp);
                arp = (struct actRange *)NULL;
           }

           if( delCnt )  {
               stg_remove_stg_finish (bfap->dmnP, delCnt, delList);
           }
      }

    } else {
        /* we get a recursive read lock in case we fault on the uiomove() below
         * and enter msfs_getpage()
         */
        FS_FILE_READ_LOCK_RECURSIVE( contextp );

        if ((vp->v_flag & VMMAPPED) && !map_lock) {
            /*
             * File mmapped while obtaining file lock.  Must follow
             * hierarchy vm_map lock followed by file lock to avoid
             * deadlock with user page fault on file page.
             */
            VM_MAP_LOCK_READ_RECURSIVE_TRY(map_lock);
            if (!map_lock) {
                FS_FILE_UNLOCK_RECURSIVE( contextp );
                goto _retry_map;
            }
        }

        size = bfap->file_size;
        directIO = vp->v_flag & VDIRECTIO;

        if (f_offset + nbyte > size) {
            /* size shrunk while we slept.  need exclusive lock */
            FS_FILE_UNLOCK_RECURSIVE( contextp );
            goto _retry;
        }
        lk_state = LKS_SHARE;
    }

    if (ioflag & IO_APPEND) {
        f_offset = size;
        uio->uio_offset = size;
    }

    /* If this file is opened for directIO and we anticipate adding
     * storage, then seize an active range to cover the file region
     * where the storage will be added.  This will prevent a race with
     * other threads trying to read this region while the storage is
     * being added.
     */
    if ( directIO && need_stg_alloc ) {

        arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));

        if (ioflag & IO_APPEND) {
            /* We are APPENDing and the file is opened for directIO, so
             * there are some added complications because the directIO
             * path drops the file_lock, which O_APPEND depends on to
             * protect the bfap->file_size.  This seizes an active
             * range from EOF to infinity.
             */
            unsigned long old_size;
_retry_append_range:
            old_size = bfap->file_size;

            /* Set up active range from current eof to infinity. */
            arp->arStartBlock = bfap->file_size / BS_BLKSIZE;
            arp->arEndBlock = ~0L;
            arp->arState   = AR_DIRECTIO;
            arp->arDebug   = SET_LINE_AND_THREAD(__LINE__);

            /* This call can cause file_lock to be dropped temporarily */
            insert_actRange_onto_list( bfap, arp, contextp );

            /* update these in case file size changed while waiting */
            f_offset = size = bfap->file_size;
            uio->uio_offset = size;

            /* When this returns, we have the range from the old eof
             * to infinity, which will be OK to use for this write
             * so long as the eof did not DECREASE while we were
             * waiting for the range.  If it did, then we need to
             * re-get the range.
             */
            if ( bfap->file_size < old_size ) {
                mutex_lock(&bfap->actRangeLock);
                remove_actRange_from_list(bfap, arp);
                mutex_unlock(&bfap->actRangeLock);
                goto _retry_append_range;
            }
        } else {
            /* Normal write, seize an active range covering pages within 
             * the requested write region.  This is needed to prevent a 
             * dio reader that is expecting to read the hole we are writing 
             * into from seeing the storage we are about to add.  The 
             * reader may already have the active range and have dropped 
             * the file lock, so we must be prevented from adding the storage
             * until he has completed his read.
             */
            arp->arStartBlock = FIRST_BLK_IN_PG(f_offset);
            arp->arEndBlock   = LAST_BLK_IN_PG(f_offset+nbyte-1);
            arp->arState      = AR_DIRECTIO;
            arp->arDebug      = SET_LINE_AND_THREAD(__LINE__);

            insert_actRange_onto_list( bfap, arp, contextp );

            /* The active range seized here will be released at the 
             * end of this routine if we are doing a normal directIO,
             * or in IO completion if doing an AIO.  There can be only
             * one asynchronous I/O started via an AIO call, but there
             * can be many calls to fs_write_direct() for normal DIO
             * calls, and we want to hold this range across all of them.
             */
        }
    }

    error = efbig_check(bfap, uio, &nbyte, size, &efbig, 
                        &signal_error, ioflag, &data_logging);
    if (error) 
        goto _error;

    if (!retry) {
        /* starting page and bytes to write */
        tmp_offset = f_offset;
        tmp_page_to_write = tmp_offset / ADVFS_PGSZ;
        tmp_bytes_left = nbyte;
        tmp_bytes_written = 0;
        tmp_starting_byte = tmp_offset % ADVFS_PGSZ;
        tmp_number_to_write = 
            MIN(ADVFS_PGSZ - tmp_starting_byte, tmp_bytes_left);
    }

    add_stg_flags = (data_logging ? WASF_LOGGING : 0) |
                    (sparse_alloc ? WASF_SPARSE : 0) |
                    ((ioflag & (IO_SYNC | IO_DSYNC)) ? WASF_SYNC : 0) |
                    (no_zero_fill ? WASF_NO_ZERO : 0);

    /* We maintain tmp_starting_byte and tmp_bytes_written counters to loop 
     * through all the pages to be written so we can add any missing storage 
     * even though no actual writes are done yet. We need the loop below to 
     * add storage for the case when there is a page with associated storage 
     * between two holes. 
     */

    while ( tmp_bytes_left > 0 ) {
        int pages_allocated = 0;
        bsPageT nextPage;

        if ( !page_is_mapped(bfap, tmp_page_to_write, &nextPage, TRUE) ) {
            unsigned long bytes_to_get;

            if ( lk_state != LKS_EXCL ) {
               /*
                * Page is not allocated so we need to allocate some
                * disk space.  We must have the exclusive lock to
                * continue so we let go of the shared lock and
                * retry the write with the exclusive lock.  We retry
                * because the file size may change between releasing
                * the shared lock and acquiring the exclusive lock.
                */
               sparse_alloc = TRUE;
               FS_FILE_UNLOCK_RECURSIVE( contextp );
               lk_state = LKS_UNLOCKED;

               MS_SMP_ASSERT( !retry );
               if (retry) {
                   /*
                    * It is impossible to retry more than once
                    * since setting sparse_alloc should guarantee
                    * that we get the exclusive lock.
                    * Note: once we get the exclusive lock we hold
                    * it till this routine is done
                    */
                   error = EIO;
                   goto _error;
               }
               retry = TRUE;
               goto _retry;
            }

            pages_allocated = 0;

            /* only allocate up to extent size to prevent quota problems */ 
            bytes_to_get = MIN(tmp_bytes_left,
                               (nextPage - tmp_page_to_write) * ADVFS_PGSZ);

            ret = fs_write_add_stg( bfap,
                                    tmp_page_to_write,
                                    bytes_to_get,
                                    size + tmp_bytes_written,
                                    (unsigned)tmp_starting_byte,
                                    cred,
                                    add_stg_flags,
                                    &pages_allocated);
            if (TestXtntsFlg)
                test_xtnt(bfap);
 
            if (ret != EOK) {
                /* error code must be saved for pinpg loop and cfs */
                error = ret;

                /* If some or all the requested storage was either allocated
                 * or already available then proceed to write what we have
                 * by adjusting the values to be written to avoid both an
                 * E_PAGE_NOT_MAPPED failure and any trailing unaligned
                 * DIO write if the preceding aligned write won't happen.
                 *
                 * Partial writes are not allowed for IO_SYNC but we also
                 * don't want to make a larger window for object reuse so
                 * if we had a sparse allocate, write the data before
                 * reporting failure, else we can just fail with no write.
                 */
                if (tmp_bytes_written == 0 ||
                        ((ioflag & (IO_SYNC|IO_DSYNC)) && !fill_hole))
                    goto _error;

                nbyte -= tmp_bytes_left;
                if (directIO && (uio->uio_rw == UIO_AIORW)) {
                    /* if AIO, then remember that this IO request
                     * was truncated.  The aio_bp->b_resid must
                     * return at least the number of bytes in
                     * tmp_bytes_left.  This value may be increased
                     * later in bs_pinpg_direct() if other parts of
                     * the I/O fail.
                     */
                    struct buf *aio_bp =
                       (struct buf *)uio->uio_iov[2].iov_base;
                    if (ioflag & (IO_SYNC|IO_DSYNC)) {
                        aio_bp->b_driver_un_1.longvalue = 3;
                        aio_bp->b_resid = orig_resid;
                        aio_bp->b_error = error;
                    } else {
                        aio_bp->b_driver_un_1.longvalue = 2;
                        aio_bp->b_resid = tmp_bytes_left;
                    }
                }
                break;

            } else {
                fill_hole = tmp_offset < size;
                contextp->dirty_alloc = TRUE;
            }

        } /* endif ( !page_is_mapped() ) */
	else if ( (ioflag & IO_TRUNC) &&
		( f_offset + nbyte <= bfap->file_size) ){
	    /* We come here if we are truncating and the page
	     * is mapped. So, we avoid copying 0 to the page.
	     */
	    if (lk_state != LKS_UNLOCKED) {
                if (lk_state == LKS_EXCL) {
		    FS_FILE_UNLOCK( contextp );
                } else {
		    FS_FILE_UNLOCK_RECURSIVE( contextp );
		}
	    }

	    if (map_lock) {
        	VM_MAP_LOCK_READ_DONE_RECURSIVE();
    	    }
	
	    if (arp) {
		mutex_lock(&bfap->actRangeLock);
		remove_actRange_from_list(bfap, arp);
		mutex_unlock(&bfap->actRangeLock);
		ms_free(arp);
	    }

	    return(EOK);
	}

        if ( pages_allocated ) {
            /* increment by the number of pages allocated, if possible. */
            tmp_number_to_write = MIN( tmp_bytes_left, 
                (pages_allocated * ADVFS_PGSZ) - tmp_starting_byte );
        } else {
            /* Use the value returned from page_is_mapped() to find the 
             * start of the next extent.
             */
            tmp_number_to_write = MIN( tmp_bytes_left,
                ((nextPage - tmp_page_to_write - 1) * ADVFS_PGSZ ) +
                                  (ADVFS_PGSZ - tmp_starting_byte) );
        }
      
        tmp_offset += tmp_number_to_write;
        tmp_bytes_written += tmp_number_to_write;
        tmp_bytes_left -= tmp_number_to_write;
        tmp_page_to_write = tmp_offset / ADVFS_PGSZ;
        tmp_starting_byte = tmp_offset % ADVFS_PGSZ;
    } /* endwhile fs_write_add_stg() loop */

#ifdef ADVFS_NO_ZERO_ALLOCATE
    /* special code for quickly allocating storage in test filesystems.
     * storage is assigned and reused without being written or cleared.
     */
    if (no_zero_fill) { /* no_zero_fill bails out here */
        ADVFS_NO_ZERO++;  /* easy way to identify test feature used */
        uio->uio_resid -= tmp_bytes_written;
        if (f_offset + tmp_bytes_written > bfap->file_size) {
            bfap->file_size = f_offset+tmp_bytes_written;
        }
        contextp->dirty_alloc = TRUE;
        goto _no_zero_exit;
    }
#endif /* ADVFS_NO_ZERO_ALLOCATE */

    /* starting page and bytes to write */
    page_to_write = f_offset / ADVFS_PGSZ;
    starting_byte = f_offset % ADVFS_PGSZ;
    if (directIO) {
        /*
         * The max transfer size is limited to INT_MAX since bp->b_bcount
         * is an int.
         */
        number_to_write = MIN(nbyte, INT_MAX);
    } else
        number_to_write = MIN( ADVFS_PGSZ - starting_byte, nbyte );

    bytes_left = nbyte;
    bytes_written = 0;

    /*
     * Write each page.
     *
     * Error
     * Handling:  A note on error handling within the loop.  If an error is
     *            detected, set the 'error' variable to the desired value,
     *            then use 'break' to terminate the loop (making sure any
     *            necessary clean-up has been done; locks, mallocs,
     *            resources, etc...).  Just after the loop, a check is made
     *            for errors that may have occurred and the decision to
     *            continue or exit at that point is determined by whether
     *            any data has successfully been written to the file.  This
     *            perserves the XPG4 write() behavior of allowing a partial
     *            write to succeed and returning the number of bytes
     *            successfully written (rwuio()/prwuio() will decide if the
     *            error or the byte count written should be returned to the
     *            application). 
     */

    while (bytes_left > 0) {

        /*
         * check to see that the page to be pinned is not greater than
         * the maximum page supported. If so, return EFBIG.
         */

        if (page_to_write > max_page) {
            error = EFBIG;
            break;
        }

        /*
         * If this file uses data logging and there is no
         * current transaction, start a transaction
         * which will encompass the next one-page portion of this write.
         */
        if (data_logging && !ftx_started) {

            ret = FTX_START_N(FTA_DATA_LOGGING_WRITE, &parentftxH, FtxNilFtxH,
                              dmnP, 2);
            if (ret != EOK) {
                error = ret;
                goto _error;
            }
            ftx_started = TRUE;
            bytes_pinned = 0;
            first_pin_page_loop = TRUE;
            second_page_bytes_to_write = 0;

            /*
             * Save the current status of the uio structure.   We'll
             * restore this if we fail the transaction.  This is necessary
             * so that if, for example, the second uiomove() fails on an
             * error but the first succeeded, it will be clear to the
             * caller that NO I/O was really done.  That will be the case
             * because the call to ftx_fail() will undo any successful
             * uiomove() for the first page written in the transaction.
             */
            saved_uio = *uio;
        }

        if ( number_to_write == ADVFS_PGSZ ) {  /* starting_byte is zero */
            refHint = BS_OVERWRITE;
        } else if ( (page_to_write == vp->v_lastr + 1) ||
                   (starting_byte == 0) ) {
            /*
             * v_lastr is the last page written, so this indicates
             * that we've seen two successive sequential writes.  This
             * is also cleared out when the last accessor goes away on
             * the file.
             */
            refHint = BS_SEQ_AHEAD;
        } else {
            refHint = BS_NIL;
        }

        /* Count the number of sequentially written pages
         * for aggressive write flushing below.
         * Reset the counter whenever a nonsequential page is written.
         */
        if ((page_to_write == vp->v_lastr + 1) || (starting_byte == 0) ) {
            bfap->seqWritePgCnt++;
        }
        else {
            bfap->seqWritePgCnt = 0;
        }

        if (directIO) {
            /*
             * number_to_write will return the actual bytes written.  This
             * will be used to update various variables such as
             * bytes_written, f_offset, bytes_left, etc...
             */
            ret = fs_write_direct(bfap, uio, cred, &number_to_write,
                                  starting_byte, cowingChecked, ioflag, arp);
        }
        else if (data_logging) {

            if (first_pin_page_loop) {
                first_page_to_write = page_to_write;

                ret = rbf_pinpg(&firstPgPin,
                               (void*)&first_page_addr,
                               bfap,
                               first_page_to_write,
                               refHint,
                               parentftxH);
            }
            else {
                second_page_to_write = page_to_write;
                ret = rbf_pinpg(&secondPgPin,
                               (void*)&second_page_addr,
                               bfap,
                               second_page_to_write,
                               refHint,
                               parentftxH);
            }
        }
        else {
            /* normal cached write path.  Call bs_pinpg() if we haven't
             * already checked for cowing; otherwise call bs_pinpg_int()
             * which will not try to cow the page.
             */
            if ( bfap->bfSetp->cloneId != BS_BFSET_ORIG ) {
                /* Don't pin a page for a clone file */
                ret = E_READ_ONLY;
            } else {
                ret = bs_pinpg_int(&page_ref,
                                   (void*)&first_page_addr,
                                   bfap,
                                   page_to_write,
                                   refHint,
                                   PINPG_NOFLAG);
            }
        }


        if ( ret != EOK ) {
            error = (ret == E_READ_ONLY) ? EROFS : ret;
            break;
        }

        /*
         * Not direct I/O, perform the uiomove().
         */
        if (!(directIO)) {
            /*
             * For data logging, pin only up to one page of the write at
             * one time.  If the bytes we're going to change within one
             * transaction span two pages, we need to pin both of those pages
             * before pinning any records within those pages.  This is one of
             * the iron-clad rules of AdvFS transaction code.
             */
            if (data_logging) {

                if (first_pin_page_loop) {
                    first_page_write_offset = starting_byte;
                    first_page_bytes_to_write =
                         MIN(ADVFS_PGSZ - starting_byte, bytes_left);
                    second_page_bytes_to_write = 0;

                    /*
                     * Don't come in here more than once per transaction.
                     */
                    first_pin_page_loop = FALSE;

                    /*
                     * If we are going to need to write more bytes
                     * than those in this page and we can still
                     * pin more bytes in this transaction, pin the
                     * next page.
                     */
                    if ((bytes_left - first_page_bytes_to_write > 0) &&
                        (first_page_bytes_to_write < ADVFS_PGSZ)) {
                        page_to_write = (f_offset + first_page_bytes_to_write) /
                                        ADVFS_PGSZ;
                        continue;
                    }
                }
                else {
                    /*
                     * Second page write offset is always zero.
                     */
                    second_page_bytes_to_write = MIN(ADVFS_PGSZ -
                                                     first_page_bytes_to_write,
                                                     bytes_left -
                                                     first_page_bytes_to_write);
                }

            }
            else {
                first_page_write_offset = starting_byte;
                first_page_bytes_to_write = MIN(ADVFS_PGSZ - starting_byte,
                                                bytes_left);
            }

            uioerror = uiomove(first_page_addr + first_page_write_offset,
                               first_page_bytes_to_write, uio);
            if (uioerror) {
                error = uioerror;
                if (!data_logging) {
                    (void) bs_unpinpg(page_ref, logNilRecord, BS_CLEAN );
                    uio->uio_resid = orig_resid;
                    uio->uio_offset = orig_offset;
                }
                goto _error;
            }

            /*
             * We'll only write to the second page if we're data logging.
             */
            if (second_page_bytes_to_write) {
                uioerror = uiomove(second_page_addr,
                                   second_page_bytes_to_write, uio);
                if (uioerror) {
                    error = uioerror;
                    goto _error;
                }
            }

            /*
             * If we're doing data logging, it's safe to pin records now
             * that no more errors can occur before the transaction commits.
             */
            if (data_logging) {
                rbf_pin_record(firstPgPin,
                               first_page_addr + first_page_write_offset,
                               first_page_bytes_to_write);
                bytes_pinned += first_page_bytes_to_write;
                if (second_page_bytes_to_write > 0) {
                    rbf_pin_record(secondPgPin,
                                   second_page_addr,
                                   second_page_bytes_to_write);
                    bytes_pinned += second_page_bytes_to_write;
                }
            }

            /*
             * If data logging is not on, release the page now.
             * If data logging is on, the page will be released
             * at the completion of the transaction.
             */
            else {
                ret = bs_unpinpg(page_ref, logNilRecord, BS_DIRTY);
                if (ret != EOK) {
                    error = EIO;
                    goto _error;
                }
                /* Aggressively flush some of the file's dirty data
                 * when the UBC exceeds its dirty cache threshold limit
                 * and enough pages have been sequentially written.
                 * This helps avoid draining the UBC cache too quickly
                 * when the amount of sequentially written data exceeds
                 * available cache memory and the I/O can not keep up.
                 * That would potentially put the system into a low memory
                 * (paging) state causing poor response time to processes.
                 *
                 * Turning this feature off delays cache writes until
                 * smoothsync or UBC initiates a flush.
                 */
                if (aggressiveWriteFlush &&
                    (bfap->seqWritePgCnt >= seqWritePgMax)) {
                    ubc_cntl_t uc = CURRENT_UC();
                    vm_cntl_t vc = CURRENT_VC();
                    unsigned long ubc_top = uc->ubc_pages + vc->vm_free_count;
                    if (ubc_top > uc->ubc_maxpages)
                        ubc_top = uc->ubc_maxpages;

                    if (vc->vm_free_count < vc->vm_prewrite_target ||
                        (100 * (uc->ubc_busy_cnt + uc->ubc_dirty_pages)
                            > ubc_top * aggressiveWritePcnt) ) {
                        lsnT seq;
                        bfap->seqWritePgCnt = 0;
                        /* Flush some of the file's dirty pages to disk
                         * and synchronously wait for the IO completion.
                         */
                        bfflush_start(bfap, &seq, 0, seqWritePgMax);
                        bfflush_sync(bfap, seq);
                    }
                }
            }
            number_to_write = first_page_bytes_to_write +
                              second_page_bytes_to_write;
        } /* !directIO */

        bytes_written += number_to_write;
        f_offset += number_to_write;
        bytes_left -= number_to_write;

        if (bytes_left != 0) {
            starting_byte = (f_offset % ADVFS_PGSZ);
            if (directIO) {
                number_to_write = MIN(bytes_left, INT_MAX);
            }
            else {
                number_to_write =
                    MIN( ADVFS_PGSZ - starting_byte, bytes_left);
            }
            page_to_write = f_offset / ADVFS_PGSZ;

            /*
             * If there's more to write but we're doing data logging
             * and we've already pinned one page of bytes, commit the current
             * transaction and start a new one.
             */
            if (data_logging && bytes_pinned == ADVFS_PGSZ) {
                ftx_done_n(parentftxH, FTA_DATA_LOGGING_WRITE);
                ftx_started = FALSE;

                /*
                 * Save the last good values for the uio fields.
                 */
                saved_uio = *uio;

                bytes_pinned = 0;
                first_pin_page_loop = TRUE;
                second_page_bytes_to_write = 0;

                ret = FTX_START_N(FTA_DATA_LOGGING_WRITE, &parentftxH,
                                  FtxNilFtxH, dmnP, 2);
                if (ret != EOK) {
                    error = ret;
                    goto _error;
                }
                ftx_started = TRUE;
            } /* endif (data_logging && bytes_pinned == ADVFS_PGSZ) */
        }
        else { /* (bytes_left == 0) */
            if (data_logging) {

                /*
                 * If we haven't added storage to the file, and we're 
                 * running in O_DSYNC mode or we're running in O_SYNC 
                 * mode but without strict POSIX O_SYNC compliance, 
                 * flush the log at the conclusion of the transaction.  
                 * This will guarantee that the user's data is on disk 
                 * (in the log) when the write() call returns to the 
                 * calling application.  This will not flush the stats 
                 * to disk but we don't have to unless we're in O_SYNC 
                 * mode with strict POSIX compliance.
                 */
                if ((contextp->dirty_alloc == FALSE) &&
                    ((ioflag & IO_DSYNC) || 
                     ((ioflag & IO_SYNC) && !strict_posix_osync_local))) {
                    ftx_special_done_mode( parentftxH, FTXDONE_LOGSYNC );
                    flushed_log = TRUE;
                }
                
                ftx_done_n(parentftxH, FTA_DATA_LOGGING_WRITE);
                ftx_started = FALSE;

                if (flushed_log) {
                    if (ioflag & IO_DSYNC)
                        MSFS_SYNC_STATS(msfs_sync_stats.fs_write_dsync_flush++);
                    else
                        MSFS_SYNC_STATS(msfs_sync_stats.fs_write_sync_flush++);
                }

                /*
                 * Save the last good values for the uio fields.
                 */
                saved_uio = *uio;
            } /* endif data_logging */
        } /* end else (bytes_left == 0) */

        /*
         * Check to see if a domain panic has occurred while we've been
         * writing away.  If it has, then all our I/O requests are being
         * returning as success even though the domain is now out of service.
         */
        if ( bfap->dmnP->dmn_panic ) {
           error = EIO;
           goto _error;
        }

        /* If this is a directIO write being done via the AIO interface,
         * we must not call back into fs_write_direct() because the
         * aio_completion routine has already been called, and it can
         * only be done once.  Instead, return to the user with the
         * number of bytes successfully transferred.
         */
        if (directIO && uio->uio_rw == UIO_AIORW) {
            break;
        }
    } /* endwhile pinpg/uiomove/unpinpg loop */

    /* If we created an active range in this routine and passed it 
     * into fs_write_direct(), release the active range now that the
     * write has been completed.  One exception to this is if we started 
     * an asynchronous I/O via AIO where the active range will be released 
     * in the I/O completion path.
     */
    if ( directIO && arp && 
         (uio->uio_rw != UIO_AIORW || uio->uio_iov[2].iov_len == 0) ) {
        mutex_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);
        mutex_unlock(&bfap->actRangeLock);
        ms_free( arp );
        arp = NULL;
    }

    /*
     * Check to see if there were any errors before any data was
     * successfully written to a page of the file.  If no data has been
     * written, then we go directly to the error exit point.  But if
     * some data has been written to the file, we continue.
     */
    if ( error && bytes_written == 0 )
        goto _error;

    /*
     * This is updated outside the loop so that a single write
     * spanning two pages isn't necessarily interpreted as sequential
     * access.
     */
    vp->v_lastr = page_to_write;

    mutex_lock( &contextp->fsContext_mutex );

    /*
     * update st_size and st_mtime in the file's stat structure
     * IO_NOSTATUPDATE is used by CFS to tell the PFS not to update
     * the mtime and ctime as the data is being written past close.
     *
     * Still update stats as the file_size may have changed.
     * NOTE: We want to make sure that any updates by other threads
     * still go through.
     */
        if (!(ioflag & IO_NOSTATUPDATE)) {
            contextp->fs_flag |= (MOD_MTIME | MOD_CTIME);
        }
    contextp->dirty_stats = TRUE;

    /*
     * if we wrote farther than the last time the file was written,
     * update the file length
     */

    if (f_offset > bfap->file_size) {
        contextp->dirty_alloc = TRUE;
        bfap->file_size = f_offset;
    }

    /*
     * don't clear enforcement mode lock bits, indicated by setgid
     * bit, but no group execute
     */

#if SEC_PRIV
    if (!privileged(SEC_CHMODSUGID, 0)) {
#else
    if (cred->cr_uid != 0) {
#endif
        u_short mask = S_ISUID;

        if ((contextp->dir_stats.st_mode & S_IXGRP) && (BSD_MODE(vp)))
            mask |= S_ISGID;

        if (noadd_execaccess)
                mask |= (S_IXUSR|S_IXGRP|S_IXOTH);

        contextp->dir_stats.st_mode &= ~mask;
    }

    mutex_unlock( &contextp->fsContext_mutex );

    if (noadd_execaccess &&
#if SEC_PRIV
        !privileged(SEC_CHMODSUGID, 0)
#else
        cred->cr_uid != 0
#endif
       ) {
            attribute_t *       acl = pacl_fs_get(vp, ACL_TYPE_ACC_MASK);

            if (acl != (attribute_t *)NO_ATTR &&
                acl != (attribute_t *)UNKNOWN_ATTR_VAL &&
                (pacl_flatten(acl,ACFL_NFSPERM) & ACL_PEXECUTE)) {
                acl = dup_sec_attr(acl);
                /*
                 * The lock must be released before msfs_setproplist_int
                 * is called or else deadlock will occur.
                 * TBD: do something if something fails!
                 */
                LOCK_DONE_SECATTR(vp);
                if (acl) {
                    struct uio *        uiop;

                    (void)pacl_remove_exec_perm(acl, (attribute_t *)0);
                    if (uiop = alloc_setproplist_data(ACL_TYPE_ACC_MASK, acl)) {
                        (void)msfs_setproplist_int(vp, uiop, cred, SET_CTIME,0);
                        FREE((void *)uiop, M_SEC);
                    }
                    FREE(acl, M_SEC);
                }
            } else {
                LOCK_DONE_SECATTR(vp);
            }
        }

    /*
     * if local sync, flush the file. Don't bother locking the context area,
     * it already is. (If it's NFS we will get called at
     * msfs_syncdata to do this later).
     */

    /*
     * if local (non-NFS) sync write, put out the stats in an ftx.
     * (if NFS, will get called for this later).
     */

    /*
     * POSIX 1003.1b sync i/o:  prior to implementation of POSIX 1003.1b
     * synchronized i/o, we checked here for IO_SYNC and in the next level
     * checked for IO_DATAONLY.  With the POSIX 1003.1b implementation, we
     * also check for IO_DATAONLY here so we don't get stopped at the gate.
     * IO_DATAONLY has the same bit value as IO_DSYNC.  For consistency, we
     * will use the POSIX 1003.1b flag IO_DSYNC in place of IO_DATAONLY.
     * Have files opened for directIO come through this path by default
     * since directIO writes are essentially synchronous anyway, and we
     * want to be sure that stats are on disk if the application added
     * storage to a file.
     */
    if ( (ioflag & (IO_SYNC|IO_DSYNC)) || directIO ) {
_no_zero_exit:
        if (data_logging) {
            /*
             * If we already flushed the log, there is no more
             * work to do to guarantee synchronous semantics.
             * Otherwise, start a transaction to write the stats
             * out and flush the log.
             */
            if (!flushed_log) {
                if (ftx_started) {
                    /*
                     * Clean up the started Atomic data logging transaction
                     * before starting a new root transaction to flush the
                     * stats.
                     */
                    ftx_fail(parentftxH);
                    ftx_started = FALSE;
                }
                ret = FTX_START_N(FTA_OSF_FSYNC_V1, &parentftxH, 
                                  FtxNilFtxH, bfap->dmnP, 5);
                if (ret != EOK) {
                    error = ret;
                    goto _error;
                }
                ftx_started = TRUE;

               ret = fs_update_stats( vp, bfap, parentftxH, 0 );
               if (ret != EOK)
               {
                   domain_panic(bfap->dmnP, "fs_write: update stats error");
                   error = ret;
                   goto _error;
               }

               ftx_special_done_mode( parentftxH, FTXDONE_LOGSYNC );
               ftx_done_n( parentftxH, FTA_OSF_FSYNC_V1 );
               ftx_started = FALSE;

               if (ioflag & IO_SYNC) {
                   MSFS_SYNC_STATS(msfs_sync_stats.fs_write_sync_flush++);
                   MSFS_SYNC_STATS(msfs_sync_stats.fs_write_sync_update++);
               }
               else {
                   MSFS_SYNC_STATS(msfs_sync_stats.fs_write_dsync_update++);
                   MSFS_SYNC_STATS(msfs_sync_stats.fs_write_dsync_flush++);
               }
           }
       }
       else {   /* not data logging */
          /*
           * In vfs layer, when ioflag values are determined, O_SYNC
           * takes precedence over O_DSYNC.  So, for POSIX 1003.1b, if we see
           * IO_DSYNC here, it means user wants data only sync i/o.  NFS does
           * the opposite.  If NFS has set IO_SYNC and IO_DATAONLY, IO_DATAONLY
           * takes precedence and takes this path (as equivalent IO_DSYNC).
           */

           /* flush data for either O_SYNC or O_DSYNC. Skip if doing
            * directIO since those writes are implicitly synchronous,
            * and there are no buffers in the cache to flush.
            */
           if ( !directIO ) {
               first_page_to_flush = orig_offset / ADVFS_PGSZ;
               last_page_to_flush = (roundup((orig_offset + orig_resid),
                                          ADVFS_PGSZ)/ADVFS_PGSZ) - 1;
               ret = bfflush(bfap, first_page_to_flush,
                         (last_page_to_flush - first_page_to_flush) + 1,
                         FLUSH_INTERMEDIATE);
               if (ret != EOK) {
                   error = EIO;
                   goto _error;
               }
           }
           if (ioflag & IO_DSYNC)
               MSFS_SYNC_STATS(msfs_sync_stats.fs_write_dsync_flush++);
           else
               MSFS_SYNC_STATS(msfs_sync_stats.fs_write_sync_flush++);

           /* 
            * Update stats if applicable.
            * Prior to the implementation of the strict-posix-osync
            * sysconfig variable(vfs), AdvFS would not flush stats if
            * dirty_alloc was FALSE.  This was in violation of POSIX (and a
            * bug).  So, we need to uphold that default behavior in order to
            * maintain consistant performance between releases.  The
            * customer must set strict-posix-osync to true to comply with
            * the POSIX definition of O_SYNC .
            */
           if ( ((ioflag & IO_SYNC) && strict_posix_osync_local)
                 || (contextp->dirty_alloc == TRUE) )
           {
               MS_SMP_ASSERT(ftx_started == FALSE);
               ret = FTX_START_N(FTA_NULL, 
                                 &parentftxH, 
                                 FtxNilFtxH, 
                                 bfap->dmnP, 
                                 5);
               if (ret != EOK)
               {
                   domain_panic(bfap->dmnP, "fs_write: FTX_START_N error");
                   error = ret;
                   goto _error;
               }
               ftx_started = TRUE;

               ret = fs_update_stats( vp, bfap, parentftxH, 0 );
               if (ret != EOK)
               {
                   domain_panic(bfap->dmnP, "fs_write: update stats error");
                   error = ret;
                   goto _error;
               }

               ftx_special_done_mode( parentftxH, FTXDONE_LOGSYNC );
               ftx_done_n( parentftxH, FTA_OSF_FSYNC_V1 );
               ftx_started = FALSE;

               if (ioflag & IO_SYNC)
                   MSFS_SYNC_STATS(msfs_sync_stats.fs_write_sync_update++);
               else
                   MSFS_SYNC_STATS(msfs_sync_stats.fs_write_dsync_update++);
           }
        } /* end else Not data logging */

        contextp->dirty_alloc = FALSE;
    } /* endif (ioflag & (IO_SYNC|IO_DSYNC)) */

    /*
     * Check to see if a domain panic has occurred while we've been
     * writing away.  If it has, then all our I/O requests are being
     * returning as success even though the domain is now out of service.
     */
    if (bfap->dmnP->dmn_panic) {
        error = EIO;
        goto _error;
    }

    /*
     * 'efbig' will be either zero, or the number of bytes that would not
     * fit because it exceeded the limits.  If it is zero, the following add
     * is a no-op.  If it is non-zero, then we need to add it back into the
     * uio_resid so that the correct number of bytes written will be
     * calculated by rwuio()/prwuio() and returned to the caller and so that
     * vn_write()/vn_pwrite() will calculate the new file offset correctly.
     */
    uio->uio_resid += efbig;
    efbig = 0;                  /* zero efbig incase we take an error path */

    if (error) {
        goto _error;
    }

    if (lk_state != LKS_UNLOCKED) {
        if (lk_state == LKS_EXCL) {
            FS_FILE_UNLOCK( contextp );
        } else {
            FS_FILE_UNLOCK_RECURSIVE( contextp );
        }
    }

    if (map_lock) {
        VM_MAP_LOCK_READ_DONE_RECURSIVE();
    }

    return(0);

_error:

    /* release active range unless AIO has already begun */
    if (arp && (uio->uio_rw != UIO_AIORW || uio->uio_iov[2].iov_len == 0)) {
        mutex_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);
        mutex_unlock(&bfap->actRangeLock);
        ms_free( arp );
    }

    if (ftx_started) {
        /*
         * Clean up the started transaction. 
         */
        ftx_fail(parentftxH);
    }

    if (lk_state != LKS_UNLOCKED) {
        if (lk_state == LKS_EXCL) {
            /*
             * Release any storage which was allocated in this routine but
             * unused due to error.
             */
            ftxHT ftxH;
            uint32T delCnt;
            void *delList;
    
            MIGTRUNC_LOCK_READ ( &(bfap->xtnts.migTruncLk) );
            FTX_START_N (
                         FTA_FS_WRITE_TRUNC,
                         &ftxH,
                         FtxNilFtxH,
                         contextp->fileSetNode->dmnP,
                         1);
            delCnt = bf_setup_truncation (bfap, ftxH, &delList, FALSE);
            bfap->trunc = FALSE;

            ftx_done_fs (ftxH, FTA_FS_WRITE_TRUNC, 0);
            MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) );
            FS_FILE_UNLOCK( contextp );
    
            if (delCnt) {
               stg_remove_stg_finish (bfap->dmnP, delCnt, delList);
            }
        } else {
            FS_FILE_UNLOCK_RECURSIVE( contextp );
        }
    }

    /*
     * For the async I/O case we can blindly add 'efbig' to uio_resid.
     * If it has already been added back into uio_resid, then 'efbig' will
     * have been set to zero (the add becomes a no-op).  Or 'efbig' was
     * always zero and again the add is a no-op.  But if 'efbig' is
     * non-zero, then it needs to be added back into uio_resid for correct
     * vn_write()/vn_pwrite() and rwuio()/prwuio() calculations, so we are
     * doing it now. 
     *
     * If data logging or sync I/O are enabled, their checks are next, and
     * they will correctly reset uio_resid to what they need for correct
     * operations. 
     */
    uio->uio_resid += efbig;

    if (data_logging) {
        /*
         * Reset uio fields to the last known values consistent with what
         * has been confirmed as being on disk.
         */
        uio->uio_offset = saved_uio.uio_offset;
        uio->uio_resid  = saved_uio.uio_resid;

    }
    if ((ioflag & (IO_SYNC | IO_DSYNC)) ) {
        /*
         * I/O Sync has priority over data logging, so if I/O sync is
         * enabled, reset the uio->uio_resid and offset back to their
         * original values.  This is done because rwuio()/prwuio() will
         * ignore an error if a partial write has occurred and I/O sync is
         * not satisfied with a partial write.  
         */
        uio->uio_offset = orig_offset;
        uio->uio_resid  = orig_resid;
    }

    if (map_lock) {
        VM_MAP_LOCK_READ_DONE_RECURSIVE();
    }

    if (error == ENOSPC)
        fs_post_no_storage_msg(vp, bfap);

    if (signal_error) {
        psignal_inthread(u.u_procp, SIGXFSZ);
    }

    return BSERRMAP( error );
} /* fs_write */

/*
 * fs_update_times
 *
 * Used by CFS to tell AdvFS to update the mtime and ctime on a file
 * with delayed dirty writes.
 */

/*
 * fs_update_times
 * 
 * Used by CFS to tell AdvFS to update the mtime and ctime on a file
 * with delayed dirty writes. Also used by CFS concurent directio
 * to update mtime/ctime/atime appropriately.
 */
void
fs_update_times(struct vnode *vp, int attr_flags)
{
    struct fsContext *contextp;
    contextp = VTOC(vp);

    mutex_lock( &contextp->fsContext_mutex );
    if(attr_flags & AT_MTIME)
        contextp->fs_flag |= MOD_MTIME;
    if(attr_flags & AT_ATIME)
        contextp->fs_flag |= MOD_ATIME;
    if(attr_flags & AT_CTIME)
        contextp->fs_flag |= MOD_CTIME;
    /*
     * Don't set dirty_stats if only ATIME update is being requested
     * and the new mount option M_NOATIMES is set.
     */
    if(attr_flags == AT_ATIME)  {
        if(!(vp->v_mount->m_flag & M_NOATIMES) )  {
            contextp->dirty_stats = TRUE;
        }
    } else {
        contextp->dirty_stats = TRUE;
    }
    mutex_unlock( &contextp->fsContext_mutex );
}


/*
 * fs_write_direct()
 *
 * Description:
 *
 * Called by fs_write(), fs_write_direct() will transfer data,
 * specified by the uio struct to disk.  This function will handle
 * all alignment issues and will call bs_pinpg_direct() to perform
 * the disk write.
 *
 * If the transfer doesn't begin and end on a sector boundary, or if
 * the underlying page does not have storage already allocated, a disk
 * read of the disk page will be performed using a temporary buffer.
 * The user's data will then be copied to this buffer and then written
 * synchronously to disk.  This same procedure will be used if the end 
 * portion of the transfer is not an entire disk page.
 *
 * An aligned transfer will be sent in its entirety to bs_pinpg_direct().
 * This function must be called with the file locked for writes
 * (FS_FILE_WRITE_LOCK()).  Note, however, that this lock may be released
 * inside this routine, and reseized before it returns. When this happens,
 * an active Range structure marked for AR_DIRECTIO will be hung off
 * bfap->actRangeList so other threads can know that there is a directio
 * in progress for this range of pages.
 *
 * Parameters:
 *
 *    bfAccessT *bfap                    Bitfile Access Pointer (in)
 *    struct uio *uio                    UIO pointer (in)
 *    struct ucred *cred
 *    int *number_to_write               Number of bytes to write/written.
 *                                       (in/out)
 *    unsigned long byte_offset_in_page  Starting byte offset into first page
 *                                       to be written. (in)
 *    int cowingDone                     TRUE if cowing was already checked
 *                                         in fs_write() 
 *    actRangeT *arp                     Pointer to an active range structure
 *                                       normally this will be NULL; in the
 *                                         O_APPEND case where there is a 
 *                                         racing thread modifying the end of
 *                                         the file, one gets passed in.
 */
static statusT
fs_write_direct(bfAccessT *bfap,
                struct uio *uio,
                struct ucred *cred,
                int *number_to_write,
                unsigned long byte_offset_in_page,
                int cowingDone,
                int ioflag,
                actRangeT *arp )
{
    statusT ret;
    int block_in_page = (int)(byte_offset_in_page / BS_BLKSIZE);
    int byte_offset_in_blk = 
                   (int)(byte_offset_in_page - block_in_page * BS_BLKSIZE );
    ulong page_to_write   = uio->uio_offset / ADVFS_PGSZ;
    ulong starting_block  = (page_to_write * ADVFS_PGSZ_IN_BLKS) + block_in_page;
    ulong base_block      = uio->uio_offset / BS_BLKSIZE;
    ulong last_page = (uio->uio_offset + *number_to_write - 1)/ADVFS_PGSZ;
    ulong last_pg_blk_offset;
    ulong xfer_blknum;
    off_t orig_starting_byte_offset = uio->uio_offset;
    int last_page_nbytes = 0;
    int nbytes = 0, total_bytes_written = 0;
    int trailing_bytes = 0;
    int num_to_write;
    int xfer_bytes;
    int bytes_written, bytes_read;
    char *xfer_addr;
    char *page_addr;
    struct buf *aio_bp = (struct buf *)NULL;
    int asynch_io_started = FALSE;
    int uio_flag = uio->uio_rw;
    int pg_mapped;
    int optimize_output_path = 0;
    int read_lock_held = 0;
    int arp_passed_in = FALSE;
    struct fsContext *contextp = VTOC(bfap->bfVp);
    struct iovec * iov;         /* used for aio */
    struct iovec saved_iovec;   /* used in asserts */

    /*
     * Did this request come from the AIO interface?  Save some addresses
     * from the uio for later (before any updates to the uio structure occur
     * (such as those that occur in uiomove()). 
     */
    if (uio->uio_rw == UIO_AIORW) {
        iov = &uio->uio_iov[2];
        aio_bp = (struct buf *)uio->uio_iov[2].iov_base;
    }

    /*
     * Because uiomove() will not update uio_iov and uio_iovcnt if the copy
     * length goes to zero at the same time uio->uio_iov->iov_len goes to
     * zero, we deal with that here. 
     */
    if ( uio->uio_iovcnt > 1 && uio->uio_iov->iov_len == 0 ) {
        uio->uio_iov++;
        uio->uio_iovcnt--;
    }
    /* 
     * If there are multiple uio->uio_iov[] structures we are only going to
     * process the first one, update the uio structure, and return allowing
     * fs_write() to call us again if need be to process the rest of the
     * uio->uio_iov[] structures.  So if *number_to_write is greater than
     * the iov_len, then use the iov_len.
     *
     * If this code is changed so that num_to_write is allowed to be greater
     * than the iov_len, all the other areas of this routine that increment
     * or decrement the iov_base and iov_len fields will need to be fixed so
     * that they can handle an array of uio->uio_iov structures
     * (readv()/writev() syscalls create them).
     */
    num_to_write = *number_to_write;
    if ( num_to_write > uio->uio_iov->iov_len ) {
        num_to_write = uio->uio_iov->iov_len;
    }
    *number_to_write = 0;

    /* Allocate an active range structure; */
    if ( !arp ) {
        arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));
    } else {
        arp_passed_in = TRUE;
    } 
 
    /* If the transfer begins and ends on a sector boundary, try to 
     * directly transfer regardless of size.  Underlying pages need
     * to have storage allocated to them. 
     */
    if ( !(byte_offset_in_blk) && !(num_to_write % BS_BLKSIZE) ) {
        int pg;
        optimize_output_path = 1;       /* optimistic guess */
        for ( pg = page_to_write; pg <= last_page; pg++ ) {
            if ( !page_is_mapped(bfap, pg, NULL, TRUE) ) {
                optimize_output_path = 0;
                break;
            }
        }
    } 

    /* Setup the actRange struct if it wasn't passed in to us. We 
     * typically restrict, via the actRange structure, the underlying 
     * 512-byte blocks corresponding to the complete AdvFS pages being 
     * written.  As an optimization when we are doing complete-sector 
     * transfers, if the underlying pages are already allocated, we only 
     * set the actRange to correspond to the actual sectors being written.
     */
    if ( !arp_passed_in ) {
        if ( optimize_output_path ) {
            /* set up active range to include just underlying sectors */
            arp->arStartBlock = starting_block;
            arp->arEndBlock = starting_block + 
                                  howmany(num_to_write,BS_BLKSIZE) - 1;
            arp->arState   = AR_DIRECTIO;
            arp->arDebug   = SET_LINE_AND_THREAD(__LINE__);
        } else {
            /* Set up active range to include all underlying pages */
            arp->arStartBlock = page_to_write * ADVFS_PGSZ_IN_BLKS;
            arp->arEndBlock = ((last_page + 1) * ADVFS_PGSZ_IN_BLKS) - 1;
            arp->arState   = AR_DIRECTIO;
            arp->arDebug   = SET_LINE_AND_THREAD(__LINE__);
        }

        /* Put the active range onto the list to prevent concurrent use
         * by migrate or other directIO threads.  Because the file_lock
         * can be dropped while inserting the active range onto the list
         * we may now have a page in our range that is no longer mapped.
         * Deal with this by adding that storage back in blkmap_direct().
         * If the actRange struct was passed in to us, it has already 
         * been put onto the activeRangeList, so don't do it again here.
         */
        insert_actRange_onto_list( bfap, arp, contextp );
    }

    /* Drop the file lock to increase concurrency among directIO threads.
     * Synchronization at this point is up to the active ranges.
     */
    read_lock_held = lock_readers( &(contextp->file_lock) );
    FS_FILE_UNLOCK( contextp );

    if ( optimize_output_path ) {

        /* We are going to skip the 'first and last partial page'
         * logic, and go straight to the DMA transfer of sectors.
         * Setup # bytes to transfer and the user's buffer
         * address.  We also adjust uio fields here.  If an 
         * error occurs, the fields will be corrected.  This
         * complies with the way the rest of the routine is set up.
         */
        nbytes = num_to_write;
        page_addr = uio->uio_iov->iov_base;
        saved_iovec = *uio->uio_iov;

        uio->uio_iov->iov_base += nbytes;
        uio->uio_iov->iov_len  -= nbytes;
        uio->uio_offset        += nbytes;
        uio->uio_resid         -= nbytes;

        goto do_direct_xfer;
    }

    /*
     * Handle transfers that are not full page writes.
     * Always wait for this write to complete.
     */
    if (byte_offset_in_page) {

        /* Allocate a page buffer. ms_malloc zeroes the page for us. */
        char *pgbuf = (char *)ms_malloc(ADVFS_PGSZ);

        nbytes = min(num_to_write, ADVFS_PGSZ - (int)byte_offset_in_page);
        pg_mapped = page_is_mapped(bfap, page_to_write, NULL, TRUE);
        if (pg_mapped) {
            xfer_addr = pgbuf + (block_in_page * BS_BLKSIZE);
            xfer_bytes =
              roundup((byte_offset_in_page - (block_in_page * BS_BLKSIZE)) 
                          + nbytes, BS_BLKSIZE);
            xfer_blknum = base_block;
        }
        else {
            xfer_addr = pgbuf;
            xfer_bytes = ADVFS_PGSZ;
            xfer_blknum = page_to_write * ADVFS_PGSZ_IN_BLKS;
        }

        if (pg_mapped) {
            /*
             * Read in the blocks from disk into the correct page buffer offset,
             * copy in the user's data,
             * then write it back out to disk.
             */
            ret = bs_refpg_direct((void *)xfer_addr,
                                  xfer_bytes,
                                  bfap,
                                  xfer_blknum,
                                  UIO_SYSSPACE,
                                  (struct buf *)NULL,
                                  arp,                 /* actRange struct */
                                  &bytes_read,
                                  &asynch_io_started);

            if ((ret != EOK) || (bytes_read != xfer_bytes)) {
                /*
                 * We had better been able to read those disk blocks!
                 * If we didn't get all the data something is wrong.
                 */
                if (ret == EOK)
                    ret = EIO;
                ms_free(pgbuf);
                goto EXIT;
            }
        }

        /*
         * Copy the user's data into the page buffer at the correct byte
         * offset.
         * Make sure that the uio_rw flag is UIO_WRITE.
         * It could be UIO_AIORW if this is an aio request.
         */
        uio->uio_rw = UIO_WRITE;

        saved_iovec = *uio->uio_iov;    /* used in asserts */

        ret = uiomove(pgbuf + (int) byte_offset_in_page,
                      (long)nbytes,
                      uio);

        uio->uio_rw = uio_flag;

        /*
         * Write the block(s)of page out to disk.  First check that the uiomove
         * succeeded.
         */
        if (!ret) {
            ret = bs_pinpg_direct((void *)xfer_addr,
                                  xfer_bytes,
                                  bfap,
                                  xfer_blknum,
                                  UIO_SYSSPACE,
                                  (struct buf *)NULL,
                                  arp,                 /* actRange struct */
                                  &bytes_written,
                                  &asynch_io_started,
                                  cowingDone,
                                  cred);
        } else {
          nbytes = 0;
        }

        ms_free(pgbuf);

        if ((ret == EOK) && (bytes_written == xfer_bytes)) {
            total_bytes_written = nbytes;
            base_block = (unsigned long) (uio->uio_offset / BS_BLKSIZE);
        }
        else {
            /* If the entire page wasn't written then something is wrong! */
            if (ret == EOK)
                ret = EIO;
            /*
             * Back out count updates performed by uiomove().
             *
             * This works because when we entered the routine we limited the
             * num_to_write value to no more than what was specified in the
             * current iov_len field.  _AND_ uiomove() will not increment
             * uio_iov nor decrement uio_iovcnt if nbytes just causes
             * iov_len to go to zero.  uio_iov and uio_iovcnt will only be
             * updated if there was more data to be transferred then the
             * current uio_iov specifies.  So if the code is changed to
             * allow uiomove() to be called with a copy count greater than
             * what is specified in the current uio_iov structure, then the
             * following code will not work. 
             */
            MS_SMP_ASSERT(uio->uio_iov->iov_len+nbytes <= saved_iovec.iov_len);
            uio->uio_iov->iov_base -= nbytes;
            uio->uio_iov->iov_len += nbytes;
            uio->uio_resid += nbytes;
            uio->uio_offset -= nbytes;
            goto EXIT;
        }
    }

    /*
     * Setup pointers for the final "aligned" write.
     */
    last_page_nbytes = (int)((num_to_write - nbytes) % ADVFS_PGSZ);
    nbytes = (int)(num_to_write - nbytes - last_page_nbytes);
    MS_SMP_ASSERT( nbytes <= uio->uio_iov->iov_len );
    page_addr = uio->uio_iov->iov_base;

    /*
     * Update the uio pointers for the "aligned" write now.  If
     * something goes wrong we'll fix them at that time.
     * 
     * This works because when we entered the routine we limited the
     * num_to_write value to no more than what was specified in the current
     * iov_len field.  But if the code is changed to allow num_to_write to
     * be larger than the current iov_len, then the following will need to
     * be changed to deal with an array of uio->uio_iov structures. 
     */
    saved_iovec = *uio->uio_iov;        /* used in asserts */
    MS_SMP_ASSERT( nbytes <= uio->uio_iov->iov_len );
    uio->uio_iov->iov_base += nbytes;
    uio->uio_iov->iov_len -= nbytes;
    uio->uio_resid -= nbytes;
    uio->uio_offset += nbytes;

    /*
     * If the transfer does not end on a page boundary, then write
     * the blocks in the last page out now.
     * Always wait for it to complete.
     */
    if (last_page_nbytes) {
        char *pgbuf = (char *)ms_malloc(ADVFS_PGSZ);
        trailing_bytes = last_page_nbytes;

        /*
         * Read in the blocks in the last page from disk, copy in the user's
         * data, then write it back out to disk.
         */
        last_page = (unsigned long)
          (orig_starting_byte_offset + total_bytes_written + nbytes) / ADVFS_PGSZ;
        last_pg_blk_offset = last_page * ADVFS_PGSZ_IN_BLKS;

        pg_mapped = page_is_mapped(bfap, last_page, NULL, TRUE);

        if (pg_mapped) {
            xfer_addr = pgbuf;
            xfer_bytes = roundup(last_page_nbytes, BS_BLKSIZE);
            xfer_blknum = last_pg_blk_offset;
        }
        else {
            xfer_addr = pgbuf;
            xfer_bytes = ADVFS_PGSZ;
            xfer_blknum = last_pg_blk_offset;
        }

        ret = EOK;
        if (pg_mapped) {
            ret = bs_refpg_direct((void *)pgbuf,
                                  xfer_bytes,
                                  bfap,
                                  xfer_blknum,
                                  UIO_SYSSPACE,
                                  (struct buf *)NULL,
                                  arp,                 /* actRange struct */
                                  &bytes_read,
                                  &asynch_io_started);
        }

        if ((!pg_mapped) || ((ret == EOK) && (bytes_read == xfer_bytes))) {
            /*
             * Copy the user's data into the buffer.  Note that the
             * uio pointers and counts have already been modified
             * above to skip over the aligned data.
             */
            uio->uio_rw = UIO_WRITE;
            ret = uiomove(pgbuf, (long) last_page_nbytes, uio);

            uio->uio_rw = uio_flag;

            /*
             * Write the blocks out to disk.  First make sure that the
             * uiomove() succeeded.
             */
            if (!ret) {
                ret = bs_pinpg_direct((void *)pgbuf,
                                      xfer_bytes,
                                      bfap,
                                      xfer_blknum,
                                      UIO_SYSSPACE,
                                      (struct buf *)NULL,
                                      arp,                /* actRange struct */
                                      &bytes_written,
                                      &asynch_io_started,
                                      cowingDone,
                                      cred);

                /*
                 * If bs_pinpg_direct() failed, back out count updates
                 * performed by uiomove().  A partial write of these blocks
                 * is not allowed!
                 *
                 * This works because when we entered the routine we limited
                 * the num_to_write value to no more than what was specified
                 * in the current iov_len field.  But if the code is changed
                 * to allow num_to_write to be larger than the current
                 * iov_len, then the following will need to be changed to 
                 * deal with an array of uio->uio_iov structures. 
                 */
                if ((ret != EOK) || (bytes_written != xfer_bytes)) {
                    MS_SMP_ASSERT(uio->uio_iov->iov_len+last_page_nbytes <=
                                  saved_iovec.iov_len);
                    uio->uio_iov->iov_base -= last_page_nbytes;
                    uio->uio_iov->iov_len += last_page_nbytes;
                    uio->uio_resid += last_page_nbytes;
                    uio->uio_offset -= last_page_nbytes;
                    last_page_nbytes = 0;

                    /* synchronous writes are all or nothing */
                    if (ioflag & (IO_SYNC | IO_DSYNC)) {
                        if (ret == EOK)
                            ret = EIO;
                        ms_free(pgbuf);
                        goto EXIT;
                    }

                    /* If we are doing an AIO transfer, remember that the
                     * trailing unaligned write failed.  bs_pinpg_direct()
                     * will need to know this if there is an aligned write
                     * because it will need to calculate aio_bp->b_resid,
                     * and it needs to know if this was successful or not.
                     */
                    if (aio_bp && aio_bp->b_driver_un_1.longvalue == 0)
                        aio_bp->b_driver_un_1.longvalue = 1;  /* fail */
                }
                else {
                    total_bytes_written += last_page_nbytes;
                }
            } else {
                last_page_nbytes = 0;
                if (aio_bp && aio_bp->b_driver_un_1.longvalue == 0)
                    aio_bp->b_driver_un_1.longvalue = 1;  /* fail */
            }
        }
        else {
            last_page_nbytes = 0;
            if (aio_bp && aio_bp->b_driver_un_1.longvalue == 0)
                aio_bp->b_driver_un_1.longvalue = 1;  /* fail */
        }

        /* Free the buffer. */
        ms_free(pgbuf);
    }

    /*
     * Write all full pages or complete sector-sized transfers.
     */
do_direct_xfer:
    if (nbytes) {
        /*
         * Did this request come from the AIO interface?
         */
        if (uio->uio_rw == UIO_AIORW) {
            if ( aio_bp->b_driver_un_1.longvalue != 2 &&
                 aio_bp->b_driver_un_1.longvalue != 3 ) {
                /* Set the resid field to the count from the trailing
                 * unaligned write, whether or not that write was
                 * successful.  If only part of the aligned write makes
                 * it out to disk, trailing unaligned write must NOT be
                 * included in the transfer size since it occurs after
                 * the aligned write in the file. If this IO request was
                 * truncated (longvalue == 2) so no trailing request was
                 * done, we need to leave aio_bp->b_resid alone so it
                 * contains the # of bytes that were truncated from the
                 * request.  We also don't change aio_bp->b_resid if the
                 * IO request is to prevent object reuse on a synchronous
                 * write that had a sparse storage allocation error
                 * (longvalue ==  3).
                 */
                aio_bp->b_resid = trailing_bytes;
            }
        }

        /* Write aligned disk blocks.  "page_to_write" is
         * really "block to write" for direct I/O.
         */
        asynch_io_started = FALSE;
        ret = bs_pinpg_direct((void *)page_addr,
                              nbytes,
                              bfap,
                              base_block,
                              uio->uio_segflg,
                              aio_bp,
                              arp,                 /* actRange struct */
                              &bytes_written,
                              &asynch_io_started,
                              cowingDone,
                              cred);

        /*
         * Did the write complete?
         */
        if (ret != EOK) {
            /*
             * Need to back out the "last block write" which was
             * performed above.
             *
             * This works because when we entered the routine we limited
             * the num_to_write value to no more than what was specified
             * in the current iov_len field.  But if the code is changed
             * to allow num_to_write to be larger than the current
             * iov_len, then the following will need to be changed to deal
             * with an array of uio->uio_iov structures.
             */
            MS_SMP_ASSERT(uio->uio_iov->iov_len+(last_page_nbytes+nbytes) <=
                          saved_iovec.iov_len);
            uio->uio_iov->iov_base -= (last_page_nbytes + nbytes);
            uio->uio_iov->iov_len += (last_page_nbytes + nbytes);
            uio->uio_resid += (last_page_nbytes + nbytes);
            uio->uio_offset -= (last_page_nbytes + nbytes);
            total_bytes_written -= last_page_nbytes;
        }
        else {

            /*
             * Did all the bytes requested get written?  If not, adjust
             * the uio counts.  Also back out the last blocks write.
             *
             * This works because when we entered the routine we limited
             * the num_to_write value to no more than what was specified
             * in the current iov_len field.  But if the code is changed
             * to allow num_to_write to be larger than the current
             * iov_len, then the following will need to be changed to deal
             * with an array of uio->uio_iov structures.
             */
            if (bytes_written != nbytes) {
                MS_SMP_ASSERT(uio->uio_iov->iov_len +
                              (last_page_nbytes+nbytes-bytes_written) <=
                              saved_iovec.iov_len);
                uio->uio_iov->iov_base -= (last_page_nbytes +
                                           (nbytes - bytes_written));
                uio->uio_iov->iov_len += (last_page_nbytes +
                                          (nbytes - bytes_written));
                uio->uio_resid += (last_page_nbytes + (nbytes - bytes_written));
                uio->uio_offset -= (last_page_nbytes + (nbytes - bytes_written));
                total_bytes_written -= last_page_nbytes;

                /* synchronous writes are all or nothing */
                if (ioflag & (IO_SYNC | IO_DSYNC)) {
                    ret = EIO;
                    goto EXIT;
                }
            }

            total_bytes_written += bytes_written;

        }
    }

EXIT:

    /*
     * Make sure that we have updated the uio_iov and uio_iovcnt values
     * This may be redundant and unnecessary, but I was being paranoid.
     * It could most likely be removed if someone wants to test it.
     */
    if ( uio->uio_iovcnt > 1 && uio->uio_iov->iov_len == 0 ) {
        uio->uio_iov++;
        uio->uio_iovcnt--;
    }

    /* Reseize the file lock since we released it in this routine. */
    if ( read_lock_held ) {
        FS_FILE_READ_LOCK_RECURSIVE( contextp );
    } else {
        FS_FILE_WRITE_LOCK( contextp );
    }

    /* Release any active range that we allocated in this routine if
     * we did all synchronous I/O; asynchronous I/O will release the
     * active range when the I/O completes.
     */
    if ( !arp_passed_in && !asynch_io_started ) {
        mutex_lock(&bfap->actRangeLock);
        MS_SMP_ASSERT( arp->arIosOutstanding == 0 );
        remove_actRange_from_list(bfap, arp);   /* also wakes waiters */
        mutex_unlock(&bfap->actRangeLock);
        ms_free(arp);
    }

    /* If anything was written, update number_to_write. */
    if (total_bytes_written) {
        *number_to_write = total_bytes_written;
    }

    /* If this is an AIO write, we need to let msfs_strategy() know
     * when there is still an outstanding I/O.  When this I/O completes
     * bs_io_complete() will perform the AIO callback.  Without the
     * outstanding I/O, msfs_strategy() does the callback.
     */
    if (aio_bp && asynch_io_started) {
        iov->iov_len = 1;
    }

    return(ret);
}


/*
 * fs_write_add_stg
 *
 * fs_write()'s routine for adding disk storage to bitfiles.
 *
 * Also used by msfs_getpage().
 *
 * Note: the file_lock must be exclusively locked upon entry, and the
 * function returns with this lock held.
 * The migTrunc_lk is taken to serialize with the migrate code.  If the
 * file uses atomic write data logging, the migTrunc_lk will already
 * be held when this routine is entered.
 */

int AdvfsDoPreAlloc = 1;

#define MAX_ALLOC_PAGE_CNT 256

/* The following started out at 256 and was reduced to 16. We would
 * like to make it bigger but that may cause problems with CFS.  The
 * smaller value interacts badly with AdvfsNotPickyBlkCnt in bs_sbm.c:
 * if it is smaller than AdvfsNotPickyBlkCnt, the result is spurious
 * fragmentation of large files. It should be greater than
 * AdvfsNotPickyBlkCnt. The downside is that there is increased CPU
 * usage in the allocator, but it amounted to no more than noise in
 * the regression tests. Since CFS won't let us make the
 * preallocation bigger for now, the value of AdvfsNotPickyBlkCnt was
 * reduced to 8 pages (i.e. 128 blocks) in bs_sbm.c.
 */
#define MAX_PREALLOC_PAGES 16

/* const replicates them for quicker NUMA access
 * They can still be modified through dbx.
 */
const int AdvfsMaxAllocPageCnt = MAX_ALLOC_PAGE_CNT;
const int AdvfsMaxPreallocPages = MAX_PREALLOC_PAGES;


int
fs_write_add_stg(
    struct bfAccess *bfap,         /* in - file's access structure */
    unsigned long pg_to_write,     /* in - pinned pg's number */
    unsigned long bytes_to_write,  /* in - bytes that remain to be written */
    unsigned long file_size,       /* in - bytes in file */
    unsigned int  starting_byte_in_page,/* in - first byte written in page */
    struct ucred *cred,            /* in - credentials of caller */
    int add_stg_flags,             /* in - control bit flags */
    int *pgs_allocated             /* out - # pgs allocated */
    )
{
    struct vnode *vp = bfap->bfVp;
    domainT *dmnP = bfap->dmnP;
    unsigned long next_file_pg, pgs_needed, pgs_prealloc, first_pg_to_add;
    statusT sts;
    ftxHT ftxH;
    int ftx_started = FALSE, ret = EOK;
    unsigned long pgs_to_add, pgs_added = 0, pgs_zeroed;
    uint32T  fragPage = bfap->fragPageOffset,
             lastPageAdded,
             firstPageToZero;
    int locked_migtrunc_here = FALSE;
    int data_logging = add_stg_flags & WASF_LOGGING;
    int sparse_alloc = add_stg_flags & WASF_SPARSE;
    int sync_alloc = add_stg_flags & WASF_SYNC;
    int no_zero_fill = add_stg_flags & WASF_NO_ZERO;
    int quotas_on = VTOC(vp)->fileSetNode->quotaStatus &
                    (QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN);
    long quota_change;

    pgs_needed = howmany(bytes_to_write, ADVFS_PGSZ);
    if (pgs_needed > AdvfsMaxAllocPageCnt) {
        pgs_needed = AdvfsMaxAllocPageCnt;
    }
    next_file_pg = howmany(file_size, ADVFS_PGSZ);

    /* assume we will add only what is needed */
    pgs_to_add = pgs_needed;

    /*
     * Figure out how many pages to allocate to the file.  This amount
     * is one of the following:
     *          - the number of pages needed
     *          - an increasing number up to AdvfsMaxPreallocPages
     *          based on page being written and sequential access.
     *
     * Do not perform preallocation for nfs servers (indicated by a
     * nonzero thread-specific data value)
     *
     * DirectIO does not permit preallocation since
     * those pages will not be properly initialized.
     * NFS does not do preallocation for performance (!NFS_SERVER_TSD)
     *
     * When quotas are enforced, preallocation is disabled because we
     * can't easily recover unused storage so we get false limit errors
     *
     * We disable preallocation for no_zero_fill calls as those should
     * be done for the exact amount needed.
     */

    if (!quotas_on && !no_zero_fill
            && AdvfsDoPreAlloc && (pg_to_write + 1) >= next_file_pg
            && !(vp->v_flag & VDIRECTIO) && !NFS_SERVER_TSD) {
        if ( (pg_to_write > 3) && (pg_to_write == (vp->v_lastr + 1)) ) {

            /* 
             * checking that we are not back in the bad interaction range with
             * the sbm
             */
            extern uint32T AdvfsNotPickyBlkCnt;
            MS_SMP_ASSERT( AdvfsNotPickyBlkCnt < AdvfsMaxPreallocPages * ADVFS_PGSZ_IN_BLKS );

            /*
             * if seeing sequential writes, ramp slowly up to maximum
             * preallocation.
             */
            pgs_prealloc = MIN( pg_to_write/4, AdvfsMaxPreallocPages );

#if 0
            /* CFS objected to any large preallocation
               increase, because it might tickle a problem in CFS
               that would be a standards violation, so the max preallocation
               value was left at 16 and the following code is currently
               commented out. We may revisit it later so I am leaving
               it in here. */
            /*
             * CFS objected that the increased preallocation (from a
             * max of 16 pages to a max of 256 pages) might lead to
             * spurious ENOSPC conditions much more often. So we take
             * some pains to reduce that possibility: if the domain is
             * more than 90% full, reduce the preallocation to a max
             * of 256 / 16 = 16 pages; if more than 80% full, reduce it
             * to 256 / 8 = 32 pages, etc. In no case should the max preallocation
             * fall below the AdvfsNotPickyBlkCnt (= 32 pages = 512 blocks).
             */

            if (dmnP->freeBlks < dmnP->totalBlks / 10) /* less than 10% free */
                pgs_prealloc = MIN(pgs_prealloc, AdvfsMaxPreallocPages / 16);
            else if (dmnP->freeBlks < dmnP->totalBlks * 2 / 10) /* less than 20 % free */
                pgs_prealloc = MIN(pgs_prealloc, AdvfsMaxPreallocPages / 8);
            else if (dmnP->freeBlks < dmnP->totalBlks * 3 / 10) /* less than 30 % free */
                pgs_prealloc = MIN(pgs_prealloc, AdvfsMaxPreallocPages / 4);
            else if (dmnP->freeBlks < dmnP->totalBlks * 4 / 10) /* less than 40 % free */
                pgs_prealloc = MIN(pgs_prealloc, AdvfsMaxPreallocPages / 2);
#endif
            if ( pgs_prealloc > pgs_to_add ) {
                pgs_to_add = pgs_prealloc;
            }
        }
    }

    first_pg_to_add = pg_to_write;

    /* make sure that we don't overextend the file beyond max_page */
    pgs_to_add = MIN( pgs_to_add, max_page - first_pg_to_add + 1 );

    /*
     * This lock will prevent migrate from moving the file
     * before the transaction is complete.  This is essential
     * since an undo of this transaction would invoke several
     * undo routines whose undo records refer to a particular
     * volume or a particular mcell.  If migrate were to
     * proceed, the undo of this transaction would likely
     * crash the system.  Note that we must take this lock before
     * starting the transaction since that is the same order
     * that code in the migrate path does it.  If the calling
     * routine has already started a transaction, this lock should
     * already be held.
     */

    /* Need to lock the bfap because of call to stg_add_stg_no_cow. */
    MIGTRUNC_LOCK_READ(&(bfap->xtnts.migTruncLk));

    /* No need to lock the quota locks. Storage is only
     * added to them at file creation, chown and explicit
     * quota setting.
     *
     * The potential danger path is 
     * chk_blk_quota --> dqsync --> rbf_add_stg
     * but it is tame in this case.
     */
    locked_migtrunc_here = TRUE;
    if ((sts = FTX_START_N( FTA_FS_WRITE_ADD_STG_V1, &ftxH, FtxNilFtxH,
                               dmnP, 0 )) != EOK) {
        ret = EIO;
        goto _error_exit;
    }
    ftx_started = TRUE;

    if ( VD_HTOP(bfap->primVdIndex, bfap->dmnP)->bmtp == bfap ) {
        ret = ENOT_SUPPORTED;
        goto _error_exit;
    }

    if ( bfap->bfSetp->cloneId != BS_BFSET_ORIG ) {
        ret = E_READ_ONLY;
        goto _error_exit;
    }

    /* when quotas are being enforced we check those limits before
     * doing the storage allocation, otherwise we allocate first
     * so we know the exact amount we got and we don't need to fix
     * the quota counts afterwards.
     */
    if (quotas_on) {
        long frag_quota = (bfap->fragState == FS_FRAG_VALID &&
                          fragPage >= first_pg_to_add &&
                          fragPage < first_pg_to_add + pgs_to_add)
                        ? bfap->fragId.type * 2 : 0;
        long new_quota = pgs_to_add * ADVFS_PGSZ_IN_BLKS - frag_quota;

        if (!sync_alloc && pgs_to_add > 1) {
            /* partial write supported so tell change_quotas() to
             * give us as much as it can. we need to say where the
             * frag overlaps to align on the correct 8k multiples.
             */
            int frag_point = frag_quota ? (fragPage - first_pg_to_add) *
                                 ADVFS_PGSZ_IN_BLKS + 16 - frag_quota : 0;
            ret = change_quotas(vp, 0, frag_point, &new_quota,
                      cred, ANYALLOC, ftxH);
            pgs_to_add = (new_quota + frag_quota) / ADVFS_PGSZ_IN_BLKS;
        } else {
            ret = change_quotas(vp, 0, new_quota, NULL, cred, 0, ftxH);
        }
        if (ret)
            goto _error_exit;
    }

    /* storage allocator doesn't have "give_me_what_you_can" flag so
     * until that happens do the ugly ENO_MORE_BLKS retry logic here.
     *   1st - pages including any preallocation
     *   2nd - pages caller requested
     *   3rd - dmnP->freeBlks if caller said > 1 page and doing an async write
     *   4rd - 1 page if dmnP->freeBlks > 1 page and doing an async write
     */
    sts = stg_add_stg_no_cow(ftxH, bfap, first_pg_to_add, pgs_to_add,
                             TRUE, (uint32T *)&pgs_added);
    if (sts == ENO_MORE_BLKS) {
        if (pgs_needed < pgs_to_add)
            sts = stg_add_stg_no_cow(ftxH, bfap, first_pg_to_add, pgs_needed,
                                     TRUE, (uint32T *)&pgs_added);
        if (sts == ENO_MORE_BLKS && pgs_needed > 1 && !sync_alloc) {
            unsigned long pgs_free = dmnP->freeBlks / ADVFS_PGSZ_IN_BLKS;
            if (pgs_free) {
                if (pgs_needed > pgs_free)
                    sts = stg_add_stg_no_cow(ftxH, bfap, first_pg_to_add,
                                      pgs_free, TRUE, (uint32T *)&pgs_added);
                if (sts == ENO_MORE_BLKS && pgs_free > 1)
                    sts = stg_add_stg_no_cow(ftxH, bfap, first_pg_to_add, 1,
                                             TRUE, (uint32T *)&pgs_added);
            }
        }
    }

    if (sts != EOK) {
        ret = sts == ENO_MORE_BLKS ? ENOSPC : EIO;
        goto _error_exit;
    }

#ifdef ADVFS_SMP_ASSERT
    /* This is a special failure point for forcing code through the
     * add_stg_undo() routine.  Used in the SMP coverage tests for
     * bfAccess struct manipulation.
     */
    if (ForceStgAddFailure) {
        ret = ENOSPC;
        goto _error_exit;
    }
#endif /* ADVFS_SMP_ASSERT */

    /* now that we know how many pages were really allocated we
     * need to see if it spanned the frag before we expand it.
     * if we spanned the frag, we adjust the quotas based on that
     * page growing only 16-frag_quota (the additional space).
     * 
     * when quotas are enforced and we didn't allocate as many pages
     * as charged, we have to release the excess charged blocks from
     * the quota system.  just to be safe in case the allocator gave
     * us more than the requested pages (probably illegal), we also
     * handle increasing the charged quota blocks.
     */
    if (quotas_on)
        quota_change = (pgs_added - pgs_to_add) * ADVFS_PGSZ_IN_BLKS;
    else
        quota_change = pgs_added * ADVFS_PGSZ_IN_BLKS;

    if (bfap->fragState == FS_FRAG_VALID && fragPage >= first_pg_to_add) {
        if (quotas_on == FALSE) {
            /* only have to remove frag quota count if we spanned it */
            if (fragPage < first_pg_to_add + pgs_added)
                quota_change -= bfap->fragId.type * 2;
        } else if (pgs_added != pgs_to_add &&
                   fragPage < first_pg_to_add + pgs_added) {
            /* an extra allocation could span the frag and a short
             * allocation could miss the frag page we already counted
             */
            if (fragPage >= first_pg_to_add + pgs_to_add)
                quota_change -= bfap->fragId.type * 2;
            else
                quota_change += bfap->fragId.type * 2;
        }
    }

#ifdef ADVFS_NO_ZERO_ALLOCATE
    /* special code for quickly allocating storage in test filesystems.
     * storage is assigned and reused without being written or cleared.
     */
    if (no_zero_fill) {
        if (bfap->fragState == FS_FRAG_VALID &&
            fragPage < first_pg_to_add + pgs_added) {
            ret = copy_and_del_frag(vp, cred, 0, ftxH);
            if (ret)
                goto _error_exit;
        }
    } else
#endif /* ADVFS_NO_ZERO_ALLOCATE */
    if (pgs_added > 0) {
        unsigned int pages_to_be_overwritten;
        unsigned int first_page_is_overwritten = TRUE;
        unsigned long bytes_in_complete_pages;

        /* Figure out how many of the pages we just allocated will
         * be completely overwritten back in fs_write().  Pass this
         * information to fs_zero_fill_pages() so it can optimize
         * if possible.
         */
        bytes_in_complete_pages = bytes_to_write;

        /* Account for partial write in first page */
        if ( starting_byte_in_page ) {
            int n = ADVFS_PGSZ - starting_byte_in_page;
            if ( n > bytes_in_complete_pages ) {
                /* small write - fits totally in this page */
                bytes_in_complete_pages = 0;
            } else {
                bytes_in_complete_pages -= n;
            }
            first_page_is_overwritten = FALSE;
        }

        /* Account for partial write in last page */
        pages_to_be_overwritten = bytes_in_complete_pages / ADVFS_PGSZ;

        /* Account for a single-page partial write. */
        if ( pages_to_be_overwritten == 0 )
            first_page_is_overwritten = FALSE;
            
        
        lastPageAdded = first_pg_to_add + pgs_added - 1;

        if ((bfap->fragState == FS_FRAG_VALID) &&
            (fragPage <= lastPageAdded)) {
            int flushit = data_logging ||
                          (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY);

            /*
             * There is either an overlapping frag or a frag preceding
             * the newly-allocated storage. If any of the newly-allocated
             * pages precede the frag's page, zero-fill them.
             */
            if (fragPage > first_pg_to_add) {
                pgs_zeroed = fs_zero_fill_pages(bfap,
                                                first_pg_to_add,
                                                fragPage - 1,
                                                pages_to_be_overwritten,
                                                first_page_is_overwritten,
                                                data_logging,
                                                sparse_alloc,
                                                cred);
                if (pgs_zeroed != fragPage - first_pg_to_add) {
                    ret = EIO;
                    goto _error_exit;
                }
            }

            /*
             * If any of the newly-allocated pages follow the
             * frag's page, zero-fill them.  Note that we use
             * the MAX() macro because we are handling two cases
             * here.  The first is that the newly-allocated
             * storage is beyond the frag.  The second is that
             * the newly-allocated storage overlaps the frag.
             */
            if (fragPage < lastPageAdded) {
                firstPageToZero = MAX(fragPage + 1, first_pg_to_add);

                /* Adjust which pages will be overwritten if the frag
                 * page is in the middle of the range of pages being added. 
                 */
                if ( firstPageToZero != first_pg_to_add ) {
                    pages_to_be_overwritten -= (fragPage + 1 - first_pg_to_add);
                    if ( !first_page_is_overwritten ) 
                        pages_to_be_overwritten++;
                    if ( pages_to_be_overwritten )
                        first_page_is_overwritten = TRUE;
                }
                pgs_zeroed = fs_zero_fill_pages(bfap,
                                                firstPageToZero,
                                                lastPageAdded,
                                                pages_to_be_overwritten,
                                                first_page_is_overwritten,
                                                data_logging,
                                                sparse_alloc,
                                                cred);
                if (pgs_zeroed != lastPageAdded - firstPageToZero + 1) {
                    ret = EIO;
                    goto _error_exit;
                }
            }

            /*
             * We are either overwriting the frag or writing
             * beyond it.  In either case, copy the frag into
             * the appropriate page and delete it.  Note that
             * if this succeeds, the transaction must not fail
             * since copy_and_del_frag() will call rbf_pinpg()
             * indirectly.  Therefore, no code which could
             * cause us to call ftx_fail() below should follow
             * a successful return from copy_and_del_frag().
             */
            sts = copy_and_del_frag(vp, cred, flushit, ftxH);
            if (sts != EOK) {
                ret = BSERRMAP( sts );
                goto _error_exit;
            }

        }
        else {
            /*
             * There is no overlapping frag.  Just zero-fill the
             * newly allocated pages.
             */

            pgs_zeroed = fs_zero_fill_pages(bfap,
                                            first_pg_to_add,
                                            lastPageAdded,
                                            pages_to_be_overwritten,
                                            first_page_is_overwritten,
                                            data_logging,
                                            sparse_alloc,
                                            cred);

            if (pgs_zeroed != pgs_added) {
                ret = EIO;
                goto _error_exit;
            }
        }
    }

    /* when quotas are not enforced we wait to do the bookkeeping last
     * so that we know the actual allocation and if we fail the storage
     * transaction nothing has to be undone.  when quotas are enforced
     * we may still need an adjustment if the allocation didn't match
     * what we intended to get.
     *
     * we always force the change because it must be legitimate, so the
     * only failure possible is an I/O error which is logged.
     */
    if (quota_change) {
        ret = change_quotas(vp, 0, quota_change, NULL, cred, FORCE, ftxH);
    }

    if (ftx_started) {
        ftx_done_n( ftxH, FTA_FS_WRITE_ADD_STG_V1 );
    }
    if (locked_migtrunc_here) {
        MIGTRUNC_UNLOCK(&(bfap->xtnts.migTruncLk));
    }

    if ( pgs_allocated )
        *pgs_allocated = pgs_added;

    return EOK;

_error_exit:

    if (ftx_started) {
        ftx_fail( ftxH );
    }
    if (locked_migtrunc_here) {
        MIGTRUNC_UNLOCK(&(bfap->xtnts.migTruncLk));
    }

    if ( pgs_allocated )
        *pgs_allocated = 0;

    return ret;
}


#define SEQAHEAD 1
#define PREFETCH  2

/*
 * Limit the number of prefetch pages to limit the amount of buffer cache
 * acquired to read in the pages.
 */
unsigned long max_prefetch_pgs = 64;

/* fs_read
 * Read from a file
 *
 * fs_read passes in ioflag which contains information for POSIX 1003.1b
 * sync i/o.  The uio information comes in as one parameter which will be
 * unpacked within the code (uio->nbyte and uio->f_offset).
 */

int
fs_read(
        struct vnode *vp,              /* in - vnode of file to read    */
        struct uio *uio,               /* in - structure for uiomove    */
        int ioflag,                    /* in - includes sync i/o flags  */
        struct ucred *cred             /* in - credentials of caller    */
        )
{
    statusT ret;
    int valid_data;
    unsigned long page_to_read, bytes_left, starting_byte;
    unsigned long nbyte, f_offset, total_size;
    int number_to_read; /* limited to "int" due to b_bcount of "buf struct" */
    int error = 0;
    bfPageRefHT page_ref;
    char *page_addr;
    int fetch_pages = 0, cur_prefetch_pgs = 0;
    int do_prefetch = 0;
    uint32T fragPageOffset;
    uint32T outOfRangePage;
    bfSetT *bfSetp;
    uint32T nextByteAfterFrag;
    uint32T fragByteCnt;
    int ahead_mode = 0;
    struct fsContext* contextp = VTOC( vp );
    bfAccessT *bfap = VTOA(vp);
    int32T directIO;
    int map_lock = 0;
    int i;
    vm_map_t directio_map = (vm_map_t)NULL;
    vm_offset_t start, end, addr;
    typedef struct page_stats {
        vm_offset_t start;      /* starting page */
        vm_offset_t end;        /* ending page   */
    } pgStatT;
    pgStatT local_pg_stats,
            *pg_statp = NULL, 
            *saved_pg_statp = NULL;
    int pg_stat_cnt;
    int already_read_frag = 0;
    unsigned long orig_page_to_read;
    unsigned long orig_starting_byte;
    unsigned long bytes_read_in_frag;
    unsigned long orig_uio_offset;
    unsigned long orig_iov_len;
    unsigned long orig_f_offset;
    unsigned long bytes_skipped;
    char         *orig_iov_base;


    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }

    f_offset = uio->uio_offset;
    nbyte = uio->uio_resid;          /* number of bytes to read */

    if (nbyte == 0) {
        return(0);
    }

    /* If a domain flush is pending, we don't want this I/O to get
     * started and cause that flush to stall.  So block this read
     * from starting until the flush has finished.
     */
    if ( bfap->dmnP->dmnFlag & BFD_DOMAIN_FLUSH_IN_PROGRESS ) {
        mutex_lock( &bfap->dmnP->mutex );
        if ( bfap->dmnP->dmnFlag & BFD_DOMAIN_FLUSH_IN_PROGRESS ) {
            assert_wait((vm_offset_t)(&bfap->dmnP->dmnFlag), FALSE);
            mutex_unlock( &bfap->dmnP->mutex );
            thread_block();
        } else {
            mutex_unlock( &bfap->dmnP->mutex );
        }
    }

_retry_map:
    if (vp->v_flag & VMMAPPED) {
        /*
         * Keep hierarchy order vm_map lock followed by
         * file_lock.  If uiomove() faults on user buffer
         * the fault path acquires the vm_map lock for read,
         * thus take the vm_map lock recursively.  This
         * hierarchy is necessary to avoid deadlock with
         * user fault on mmapped region which takes vm_map
         * lock for read then calls msfs_getpage() which
         * locks file lock.
         */
        VM_MAP_LOCK_READ_RECURSIVE();
        map_lock = 1;
    }

    /* we get the read lock recursive in case we fault during the uiomove()
     * below and enter msfs_getpage()
     */
    FS_FILE_READ_LOCK_RECURSIVE( contextp );

    if ((vp->v_flag & VMMAPPED) && !map_lock) {
        /* 
         * File mmapped while obtaining file lock.  Must follow
         * hierarchy vm_map lock followed by file lock to avoid
         * deadlock with user page fault on file page.
         */
        VM_MAP_LOCK_READ_RECURSIVE_TRY(map_lock);
        if (!map_lock) {
            FS_FILE_UNLOCK_RECURSIVE( contextp );
            goto _retry_map;
        }
    }

    if (contextp->fs_flag & META_OPEN) {
        /* meta data and fragbf st_size needs to be kept updated */
        bfap->file_size = (off_t)((bfap->bfSetp->cloneId && bfap->cloneId) ?
                                   (off_t)bfap->maxClonePgs * ADVFS_PGSZ :
                                   (off_t)bfap->nextPage * ADVFS_PGSZ);
    }

    total_size = bfap->file_size;

    /*
     * Set the directIO flag
     */
    directIO = vp->v_flag & VDIRECTIO;

    if (f_offset >= total_size) {
        /*
         * reading past end-of-file
         * update st_atime in the file's stat structure
         * for posix
         */

        if (!(ioflag & IO_NOSTATUPDATE)) {
            mutex_lock(&contextp->fsContext_mutex);
            contextp->fs_flag |= MOD_ATIME;
            if (!(vp->v_mount->m_flag & M_NOATIMES))
                contextp->dirty_stats = TRUE;
            mutex_unlock(&contextp->fsContext_mutex);
        }

            error = 0;

        goto _error;
    }

    /*
     * assume for now a page size of 1
     */

    page_to_read   = f_offset / ADVFS_PGSZ; /* starting page to read */
    starting_byte  = f_offset % ADVFS_PGSZ;
    bytes_left     = MIN( nbyte, total_size - f_offset );
    if (directIO)
        number_to_read = MIN( bytes_left, INT_MAX );
    else
        number_to_read = MIN( bytes_left, ADVFS_PGSZ - starting_byte );

    /*
     * If we're reading sequentially designate read ahead.
     * Otherwise, if we're reading >= two pages, schedule
     * a prefetch.
     */
    if( page_to_read == vp->v_lastr + 1 ) {
        ahead_mode = SEQAHEAD;
    } else if( bytes_left > number_to_read ) {
        ahead_mode = PREFETCH;
        fetch_pages = howmany(bytes_left - number_to_read, ADVFS_PGSZ);
        cur_prefetch_pgs = (fetch_pages >= max_prefetch_pgs) ? max_prefetch_pgs :
                           fetch_pages;
        fetch_pages -= cur_prefetch_pgs;
        do_prefetch = 1;
    }

    outOfRangePage = (((f_offset + nbyte) - 1) + ADVFS_PGSZ) / ADVFS_PGSZ;

    /*
     * NOTE:  This test is not synchronized with the setting of the frag
     * state.  If the state is FS_FRAG_NONE, the state can only be set
     * to FS_FRAG_LOADED.  If the state is not FS_FRAG_NONE, the state
     * can only be set to FS_FRAG_NONE if the previous state was
     * FS_FRAG_LOADED.
     */
    if (bfap->fragState == FS_FRAG_NONE) {
        /*
         * This file doesn't have a frag.  Set the page offset to a page not
         * in the read range.
         */
        fragPageOffset = outOfRangePage;
    } else {
        /*
         * The file has a frag.
         */
        fragPageOffset = bfap->fragPageOffset;
    }

    /*
     * read each page and transfer the data to the user's buffer
     */

    /* If we don't have to read the frag page at all, set up to skip
     * the special frag adjustment for DIO at the top of the next loop.
     */
    if ( directIO && ( f_offset + bytes_left <= fragPageOffset * ADVFS_PGSZ ))
        already_read_frag = 2;

    while (bytes_left > 0) {

        /*
         * NOTE: I don't like checking the page offset every time thru the loop
         * but frags seem easier to deal with by doing the check here.  Since
         * this should be a low volume loop (< 8 times), I can live with it.
         * jjw, 22-apr-1993.
         */

        /* If we are doing a read of a directIO file via the AIO interface,
         * and the file has a frag, then we want to read the frag before
         * allowing the asynchronous IO to be spawned.  This will prevent
         * a thread from seeing the AIO completion and using the data in
         * the user's buffer before the frag data has been transferred.
         * When appropriate, this paragraph is entered twice: first to
         * read the frag, and again to reset the values for the rest of
         * the pages to be read.
         */
        if ( directIO                             &&
             ( uio->uio_rw == UIO_AIORW )         &&
             ( fragPageOffset != outOfRangePage ) &&
             ( already_read_frag < 2) ) {
            if ((already_read_frag == 0) && (page_to_read == fragPageOffset)) {
                /* reading only the frag; reordering code not needed */
                already_read_frag = 2;
            } else if (already_read_frag == 0) {
                /* setup to read frag page before the rest of the
                 * pages which will be done via AIO.  Save off the
                 * starting values to restore on next pass.
                 */
                orig_f_offset = f_offset;
                orig_page_to_read = page_to_read;
                page_to_read = fragPageOffset;      /* forces frag read now */
                orig_starting_byte = starting_byte;
                starting_byte = 0;                  /* start of frag page */
                bytes_skipped = (fragPageOffset * ADVFS_PGSZ) - f_offset;
                bytes_read_in_frag = number_to_read - bytes_skipped;
                number_to_read = bytes_read_in_frag;
                orig_uio_offset = uio->uio_offset;
                orig_iov_base   = uio->uio_iov->iov_base;
                orig_iov_len    = uio->uio_iov->iov_len;
                uio->uio_offset = fragPageOffset * ADVFS_PGSZ;
                uio->uio_iov->iov_base += bytes_skipped;
                uio->uio_iov->iov_len   = bytes_read_in_frag;
                already_read_frag = 1;
            } else {                 /* already_read_frag == 1 */
                /* reset original values to read non-frag pages. Some
                 * values are adjusted for the bytes already read.
                 */
                struct buf *aio_bp;

                f_offset = orig_f_offset;
                page_to_read = orig_page_to_read;
                starting_byte = orig_starting_byte;
                number_to_read = uio->uio_resid;
                uio->uio_iov->iov_base = orig_iov_base;
                uio->uio_iov->iov_len  = orig_iov_len - bytes_read_in_frag;
                uio->uio_offset = orig_uio_offset;
                aio_bp = (struct buf *)uio->uio_iov[2].iov_base;
                aio_bp->b_resid -= bytes_read_in_frag;
                already_read_frag = 2;    /* don't come in here again */
            } 
        }

        if (page_to_read != fragPageOffset) {

            if ( bfap->origAccp ) {
                /* If we are doing a directIO read of this clone via
                 * the normal interface (not AIO), then we must map 
                 * the user's pages down now before we take out the 
                 * trunc_xfer_lk.  This resolves a lock hierarchy 
                 * violation between the trunc_xfer_lk and the
                 * vm_umap.vm_lock which is seized in vm_map_pageable().
                 */
                if ( directIO  &&
                     (uio->uio_rw != UIO_AIORW) &&
                     (uio->uio_segflg == UIO_USERSPACE) ) {
                     
                    /* We need to protect the pages associated with each
                     * uio->uio_iov->iov_base.  Typically there will be
                     * only one of these, but if the user called in via
                     * readv(), there may be more, so we must loop
                     * through all here.  We must also save these address
                     * so we can unprotect them later.  
                     */
                    directio_map = current_task()->map;
                    pg_stat_cnt = 0;
                    if (uio->uio_iovcnt > 1) {
                        pg_statp = (pgStatT *)
                            ms_malloc( uio->uio_iovcnt * sizeof(pgStatT) );
                        saved_pg_statp = pg_statp;
                    } else 
                        pg_statp = &local_pg_stats;
                    
                    for (i = 0;  i < uio->uio_iovcnt; i++ ) {
                        addr  = (vm_offset_t)uio->uio_iov[i].iov_base;
                        start = trunc_page(addr);
                        end   = round_page(addr + uio->uio_iov[i].iov_len);
                        ret = vm_map_pageable(directio_map, 
                                              start, 
                                              end, 
                                              VM_PROT_WRITE);
                        if (ret != KERN_SUCCESS) {
                            ret = EFAULT;
                            goto _error;
                        } else {
                            pg_statp->start = start;
                            pg_statp->end   = end;
                            pg_stat_cnt++;
                            pg_statp++;
                        }
                    }
                }
                TRUNC_XFER_LOCK_READ(&bfap->origAccp->trunc_xfer_lk);
            }

            /*
             * If doing direct I/O, use page_addr to pass in the
             * base address.
             */
            if (directIO) {
                ret = fs_read_direct(bfap, uio,
                                     &number_to_read, starting_byte);
                MS_SMP_ASSERT( ret != E_BLKDESC_ARRAY_TOO_SMALL );
            }
            else if( do_prefetch && ahead_mode == PREFETCH ) {
                ret = bs_refpg_fetch(
                               &page_ref,
                               (void *)&page_addr,
                               bfap,
                               page_to_read,
                               BS_SEQ_AHEAD, /* for read ahead */
                               cur_prefetch_pgs
                               );
                do_prefetch = 0;
            }
            else if ( ahead_mode == SEQAHEAD ) {
                ret = bs_refpg(
                               &page_ref,
                               (void *)&page_addr,
                               bfap,
                               page_to_read,
                               BS_SEQ_AHEAD /* for read ahead */
                               );
            } else {
                ret = bs_refpg(
                               &page_ref,
                               (void *)&page_addr,
                               bfap,
                               page_to_read,
                               BS_NIL
                               );

                /*
                 * Recalculate prefetch pages when current subset
                 * has been retrieved. The next page to read is in
                 * addition to the subset of prefetch pages. Adjust the
                 * total remaining fetch pages accordingly.
                 *
                 */
                if ((ahead_mode == PREFETCH) && (--cur_prefetch_pgs <= 0)) {
                    if (fetch_pages > 1) {
                        do_prefetch = 1;
                        cur_prefetch_pgs = (fetch_pages > max_prefetch_pgs) ?
                          max_prefetch_pgs : fetch_pages - 1;
                        fetch_pages = fetch_pages - cur_prefetch_pgs + 1;
                    }
                    else {
                        cur_prefetch_pgs = 0;
                        fetch_pages = 0;
                        ahead_mode = BS_NIL;
                    }
                }
            }

            /* Undo some stuff for clone files */
            if ( bfap->origAccp ) {
                TRUNC_XFER_UNLOCK(&bfap->origAccp->trunc_xfer_lk);

                /* If we protected these pages above, undo that now. 
                 * Also deallocate any storage added for storing the 
                 * page extents.  This is done for directIO reading
                 * of clones only.
                 */
                if (directio_map != (vm_map_t)NULL) {
                    if ( saved_pg_statp )
                        pg_statp = saved_pg_statp;
                    else
                        pg_statp = &local_pg_stats;
                    for (i = 0;  i < pg_stat_cnt; i++ ) {
                        (void)vm_map_pageable(directio_map, 
                                              pg_statp->start, 
                                              pg_statp->end, 
                                              VM_PROT_NONE);
                        pg_statp++;
                    }
                    if (saved_pg_statp)
                        ms_free(saved_pg_statp);

                    saved_pg_statp = pg_statp = NULL;
                    directio_map = (vm_map_t)NULL;
                }
            }

            if (ret == EOK) {
                valid_data = TRUE;

            } else if (ret == E_PAGE_NOT_MAPPED) {

                /*
                 * sparse file.  data page is not valid so we must return zeros.
                 */
                valid_data = FALSE;
                page_addr = MegaSafePageOfZeros;

            } else {
                error = EIO;
                goto _error;
            }

            vp->v_lastr = page_to_read;

            page_addr += starting_byte;

            /*
             * Don't perform the uiomove for direct I/O.
             */
            if (!directIO) {
                error = uiomove(page_addr, (long)number_to_read, uio);
                if (error) {
                    if (valid_data) {
                        bs_derefpg(page_ref, BS_NIL);
                    }
                    goto _error;
                }

                if (valid_data) {
                    bs_derefpg(page_ref, BS_NIL);
                }
            }

        } else {

            /*
             * The current page to read data from is the frag page.
             */
            if (bfap->fragState == FS_FRAG_VALID) {
                int uio_rw = uio->uio_rw;
                nextByteAfterFrag = bfap->fragId.type * 1024;

                if (starting_byte < nextByteAfterFrag) {
                    /*
                     * Copy the bytes from the frag into the
                     * user's buffer.  Don't release the lock
                     * until after the copy so that the frag can't
                     * disappear from under us.
                     */
                    fragByteCnt = MIN (
                                       nextByteAfterFrag - starting_byte,
                                       number_to_read
                                       );
                    bfSetp = bfap->bfSetp;

                    /*
                     * If this request was issued via msfs_strategy()
                     * the uio_rw flag will be UIO_AIORW.  Before
                     * uiomove() can be called the flag must be replaced
                     * with UIO_READ.
                     */
                    uio->uio_rw = UIO_READ;
                    ret = uiomove_frag (
                                     bfap->fragId,
                                     starting_byte,
                                     fragByteCnt,
                                     bfSetp->fragBfAp,
                                     uio
                                     );
                    uio->uio_rw = uio_rw;

                    if (ret != EOK) {
                        error = BSERRMAP(ret);
                        goto _error;
                    }
                } else {

                    fragByteCnt = 0;
                }
                if (number_to_read > fragByteCnt) {
                    /*
                     * Zero fill the remaining bytes.
                     */

                    /* If this ASSERT is ever taken, turn off ASSERTs
                     * (sysconfig -r advfs AdvfsEnableAsserts=0) as a workaround
                     * and then remove this ASSERT from the pools.  This has
                     * been placed here under the hypothesis that filling zeros
                     * beyond the frag is the same as reading beyong end-of-file
                     * and should never be done.
                     */
                    MS_SMP_ASSERT( !uio );

                    uio->uio_rw = UIO_READ;
                    error = uiomove (
                                     MegaSafePageOfZeros,
                                     (long)(number_to_read - fragByteCnt),
                                     uio
                                     );
                    uio->uio_rw = uio_rw;
                    if (error) {
                        goto _error;
                    }
                }
            } else {
                /*
                 * The frag is loaded into the frag page.
                 * Let the normal code read the page.
                 */
                fragPageOffset = outOfRangePage;
                continue;
            }
        }

        /* if ioflag contains IO_RSYNC, POSIX 1003.1b requires that any pending
         * writes be written out to disk prior to the read.  We first call
         * msfs_syncdata which does a flush of the buffer.
         */
        if (ioflag & IO_RSYNC)
        {
            MSFS_SYNC_STATS(msfs_sync_stats.fs_read_sync_flush++);
            if(error = msfs_syncdata(vp, FWRITE, f_offset, (vm_size_t)number_to_read, cred))
                goto _error;
        }

        f_offset += number_to_read;
        bytes_left -= number_to_read;
        starting_byte = (f_offset % ADVFS_PGSZ);
        page_to_read = f_offset / ADVFS_PGSZ;
        if (directIO)
            number_to_read = MIN(bytes_left, INT_MAX);
        else
            number_to_read = MIN(bytes_left, ADVFS_PGSZ);

    } /* end of while loop */

    /*
     * update st_atime in the file's stat structure
     */
    if (!(ioflag & IO_NOSTATUPDATE)) {
        mutex_lock( &contextp->fsContext_mutex );
        contextp->fs_flag |= MOD_ATIME;
        if (!(vp->v_mount->m_flag & M_NOATIMES)) {
            contextp->dirty_stats = TRUE;
        }
        mutex_unlock( &contextp->fsContext_mutex );
    }

    /*
     * For POSIX 1003.1b synchronized i/o file integrity, if ioflag contains
     * IO_RSYNC and IO_SYNC, update metadata now (data was updated above in
     * call to msfs_syncdata()).
     */
    if ((ioflag & (IO_RSYNC|IO_SYNC)) == (IO_RSYNC|IO_SYNC)) {
        int ret;

        MSFS_SYNC_STATS(msfs_sync_stats.fs_read_sync_update++);
        ret = fs_update_stats( vp, bfap, FtxNilFtxH, 0 );
        if (ret != EOK) {
            domain_panic(bfap->dmnP, "fs_read: update stats error");
            error = ret;
            goto _error;
        }
    }
    FS_FILE_UNLOCK_RECURSIVE( contextp );

    if (map_lock) {
        VM_MAP_LOCK_READ_DONE_RECURSIVE();
    }

    return 0;

_error:
    if (directio_map != (vm_map_t)NULL) {
        if ( saved_pg_statp )
            pg_statp = saved_pg_statp;
        else
            pg_statp = &local_pg_stats;
        for (i = 0;  i < pg_stat_cnt; i++ ) {
            (void)vm_map_pageable(directio_map, 
                                  pg_statp->start, 
                                  pg_statp->end, 
                                  VM_PROT_NONE);
            pg_statp++;
        }
        if (saved_pg_statp)
            ms_free(saved_pg_statp);
    }

    FS_FILE_UNLOCK_RECURSIVE( contextp );

    if (map_lock) {
        VM_MAP_LOCK_READ_DONE_RECURSIVE();
    }

    return error;
}   /* fs_read */

/*
 * fs_read_direct()
 *
 * Description:
 *
 * Called by fs_read(),  fs_read_direct() will transfer data,
 * specified by the uio struct, from disk to the user's buffer.
 * This function will handle all alignment issues and will call
 * bs_refpg_direct() to perform the disk read.  If the read involves
 * a fragment the number_to_read will be adjusted so fs_read_direct()
 * will only read the non-fragment bytes and will leave it up to fs_read()
 * to deal with the fragment.
 *
 * If the transfer doesn't begin on a disk sector boundary, a disk
 * read of the entire disk sector will be performed using a temporary
 * buffer.  The requested data will then be copied to the user's buffer.
 * An aligned transfer will be sent in its entirety to bs_refpg_direct().
 * Similarly, any transfer of less than a full sector in length will
 * also require an intermediate copy to a temporary buffer.  This is
 * because drivers may not handle less-than-full-sector-sized transfers.
 * This function should be called with the file read lock taken
 * (FS_FILE_READ_LOCK()).
 *
 * Parameters:
 *
 *    bfAccessT *bfap                    Bitfile Access Pointer (in)
 *    struct uio *uio                    UIO Pointer (in)
 *    int nbytes                         (in)Number of bytes to read (in)
 *                                       (out)Number of bytes actually read(out)
 *    unsigned long byte_offset_in_page  Starting byte offset in page (in)
 */
static statusT
fs_read_direct(bfAccessT *bfap,
               struct uio *uio,
               int *nbytes,
               unsigned long byte_offset_in_page)
{
    statusT ret;
    int block_in_page = byte_offset_in_page / BS_BLKSIZE;
    int byte_offset_in_blk = (int)(byte_offset_in_page - block_in_page * BS_BLKSIZE );
    ulong page_to_read = uio->uio_offset / ADVFS_PGSZ;
    ulong starting_block = (page_to_read * ADVFS_PGSZ_IN_BLKS) + block_in_page;
    ulong ending_block = (page_to_read * ADVFS_PGSZ_IN_BLKS) + 
                         (byte_offset_in_page + *nbytes - 1) / BS_BLKSIZE;
    int number_to_read, bs_number_read, bytes_transferred;
    int uio_flag = uio->uio_rw;
    int asynch_io_started = FALSE;
    int frag_count = 0;
    struct buf *aio_bp = (struct buf *)NULL;
    struct actRange *arp = NULL;
    struct fsContext *contextp = VTOC(bfap->bfVp);
    struct iovec * iov;         /* used for aio */
    ulong middle_bytes;
    long trailing_bytes = 0;

    /*
     * Did this request come from the AIO interface?  Save some addresses
     * from the uio for later (before any updates to the uio structure occur
     * (such as those that occur in uiomove()). 
     */
    if (uio->uio_rw == UIO_AIORW) {
        iov = &uio->uio_iov[2];
        aio_bp = (struct buf *)uio->uio_iov[2].iov_base;
    }

    /*
     * Because uiomove() will not update uio_iov and uio_iovcnt if the copy
     * length goes to zero at the same time uio->uio_iov->iov_len goes to
     * zero, we deal with that here. 
     */
    if ( uio->uio_iovcnt > 1 && uio->uio_iov->iov_len == 0 ) {
        uio->uio_iov++;
        uio->uio_iovcnt--;
    }
    /*
     * If there are multiple uio->uio_iov[] structures we are only going to
     * process the first one, update the uio structure, and return allowing
     * fs_read() to call us again if need be to process the rest of the
     * uio->uio_iov[] structures.  
     *
     * If this code is changed so that *nbytes is allowed to be greater
     * than the iov_len, all the other areas of this routine that increment
     * or decrement the iov_base and iov_len fields will need to be fixed so
     * that they can handle an array of uio->uio_iov structures
     * (readv()/writev() syscalls create them).
     */
    if ( *nbytes > uio->uio_iov->iov_len ) {
        *nbytes = uio->uio_iov->iov_len;
    }

    /*
     * Do we need to worry about frags?  If so, decrement the number of
     * bytes to read by the number of bytes contained in the frag.
     */
    if (bfap->fragState != FS_FRAG_NONE) {
        unsigned long last_page_to_read =
            (uio->uio_offset + *nbytes) / ADVFS_PGSZ;
        if (last_page_to_read >= bfap->fragPageOffset) {
            u_long bytes_before_frag =
                ((u_long)bfap->fragPageOffset*ADVFS_PGSZ)-uio->uio_offset;
            frag_count = *nbytes - bytes_before_frag;
            *nbytes -= frag_count;
        }
    }

    /* How many bytes do we have to read? */
    number_to_read = *nbytes;
    bytes_transferred = 0;

    /* Allocate an active range structure; */
    arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));
 
    /* Initialize fields not zero'd by ms_malloc() */
    arp->arStartBlock = starting_block;
    arp->arEndBlock = ending_block;
    arp->arState   = AR_DIRECTIO;
    arp->arDebug   = SET_LINE_AND_THREAD(__LINE__);

    /* Put the active range onto the list to prevent concurrent use
     * by migrate or other directIO threads.
     */
    insert_actRange_onto_list( bfap, arp, contextp );   /* may sleep */

    /* Do not drop this lock for clones; cannot reseize it without
     * violating the hierarchy.
     */
    if (!( bfap->origAccp ))
        FS_FILE_UNLOCK_RECURSIVE( contextp );

    /* Does the read start on a block boundary?  If not, then we
     * must read the unaligned portion (up to first block
     * boundary) into a temporary buffer, and then copy it to
     * the user's buffer.
     */
    if (byte_offset_in_blk) {
        char *blkbuf = (char *)ms_malloc(BS_BLKSIZE);
        int tmp_bytes = min(number_to_read, BS_BLKSIZE - byte_offset_in_blk);

        /*
         * Read in the whole block and copy the needed bytes
         * to the user's buffer.
         */
        ret = bs_refpg_direct((void *)blkbuf, 
                               BS_BLKSIZE, 
                               bfap,
                               starting_block, 
                               UIO_SYSSPACE,
                               (struct buf *)NULL, 
                               arp,                 /* actRange struct */
                               &bs_number_read, 
                               &asynch_io_started);

        if ((ret == EOK) && (bs_number_read == BS_BLKSIZE)) {
            /*
             * Copy the data to the user's buffer.
             */
            uio->uio_rw = UIO_READ;
            ret = uiomove(blkbuf + byte_offset_in_blk, (long) tmp_bytes, uio);
            uio->uio_rw = uio_flag;
            ms_free(blkbuf);
            if (ret != EOK) {
                goto EXIT;
            }
            number_to_read -= tmp_bytes;
            bytes_transferred += tmp_bytes;
            starting_block++;
            if (aio_bp)
                aio_bp->b_resid -= tmp_bytes;

        } else {
            /*
             * We had better have been able to read the entire block!
             */
            if (ret == EOK)
                ret = EIO;
            ms_free(blkbuf);
            goto EXIT;
        }
    }

    /*
     * If the remaining read is not an integral number of sectors
     * in length, we need to read the trailing odd-sized segment
     * here, and then we can transfer any remaining bytes as one
     * chunk later.
     */
    trailing_bytes = number_to_read % BS_BLKSIZE;
    middle_bytes = number_to_read - trailing_bytes;
    if ( trailing_bytes ) {
        char *blkbuf;
        ulong trailing_block = starting_block + (number_to_read / BS_BLKSIZE);

        /* Malloc a private buffer. */
        blkbuf = (char *)ms_malloc(BS_BLKSIZE);

        /*
         * Read in the whole block and copy the needed bytes
         * to the user's buffer.
         */
        ret = bs_refpg_direct((void *)blkbuf, 
                               BS_BLKSIZE, 
                               bfap,
                               trailing_block, 
                               UIO_SYSSPACE,
                               (struct buf *)NULL, 
                               arp,                 /* actRange struct */
                               &bs_number_read, 
                               &asynch_io_started);

        if ((ret == EOK) && (bs_number_read == BS_BLKSIZE)) {
            /*
             * Copy the data to the user's buffer. Update the
             * uio struct to reflect the position at the end
             * of the buffer, and then reset the values later
             * if necessary for the final read.
             */
            uio->uio_rw = UIO_READ;
            uio->uio_iov->iov_base += middle_bytes;

            ret = uiomove(blkbuf, 
                          (long) trailing_bytes, 
                          uio);

            uio->uio_rw = uio_flag;
            ms_free(blkbuf);
            if (ret != EOK) {
                uio->uio_iov->iov_base -= middle_bytes;
                goto EXIT;
            }
            bytes_transferred += trailing_bytes;
            if (aio_bp)
                aio_bp->b_resid -= trailing_bytes;

        } else {
            /*
             * We had better have been able to read the entire block!
             */
            if (ret == EOK)
                ret = EIO;
            ms_free(blkbuf);
            goto EXIT;
        }
    }

    /*
     * Read in the middle of the data.
     */
    if (middle_bytes) {
        /* 
         * Adjust the uio struct back for middle bytes read.  This means
         * making the uio struct appear that the read of the trailing bytes
         * never occurred.  Later, if the read of the middle blocks
         * succeeds, we will update the uio struct so it appears, on return,
         * the way it would if the read were done as a unit.
         */
        if ( trailing_bytes ) {
            uio->uio_resid += trailing_bytes;
            uio->uio_iov->iov_len += trailing_bytes;
            uio->uio_iov->iov_base -= (middle_bytes + trailing_bytes);
            uio->uio_offset -= trailing_bytes;
            bytes_transferred -= trailing_bytes;
        }

        /*
         * Perform the read.
         */
        asynch_io_started = FALSE;

        MS_SMP_ASSERT(middle_bytes + trailing_bytes <= uio->uio_iov->iov_len);
        ret = bs_refpg_direct((void *)uio->uio_iov->iov_base,
                              middle_bytes,
                              bfap, 
                              starting_block,
                              uio->uio_segflg, 
                              aio_bp,
                              arp,                 /* actRange struct */
                              &bs_number_read, 
                              &asynch_io_started);

        /*
         * Update the uio counts and pointers.
         *
         * This works because when we entered the routine we limited
         * the *nbytes value to no more than what was specified
         * in the current iov_len field.  But if the code is changed
         * to allow *nbytes to be larger than the current
         * iov_len, then the following will need to be changed to deal
         * with an array of uio->uio_iov structures.
         */
        if ( ret == EOK && bs_number_read > 0 ) {
            MS_SMP_ASSERT( bs_number_read <= uio->uio_iov->iov_len );
            uio->uio_iov->iov_base += bs_number_read;
            uio->uio_iov->iov_len -= bs_number_read;
            uio->uio_resid -= bs_number_read;
            uio->uio_offset += bs_number_read;
            bytes_transferred += bs_number_read;

            /* 
             * If we had to do a separate read of the non-sector-sized
             * trailing bytes above, account for that here before returning,
             * If and Only If, we successfully transferred all the
             * intermediate sectors.  Yes it is a waste to redo the partial
             * sector if we need to come back, but that should be such a
             * rare event that it isn't worth worrying about.
             */
            if ( trailing_bytes && bs_number_read == middle_bytes ) {
                uio->uio_iov->iov_base += trailing_bytes;
                uio->uio_offset += trailing_bytes;
                uio->uio_resid -= trailing_bytes;
                uio->uio_iov->iov_len -= trailing_bytes;
                bytes_transferred += trailing_bytes;
            }

            /*
             * If this is an AIO read, we need to let msfs_strategy() know
             * that there is an outstanding I/O.  When this I/O completes
             * bs_io_complete() will perform the AIO callback.
             */
            if (asynch_io_started) {
                iov->iov_len = 1;
            }
        }
    }

EXIT:
    /*
     * Update "nbytes" which on return to fs_read() specifies the number of
     * logically contiguous bytes actually transferred to the user's buffer.
     * What that means is that if we did not transfer all of the
     * middle_bytes, then we do not count the trailing bytes since that
     * would not be logically contiguous.
     */
    *nbytes = bytes_transferred;

    /*
     * Make sure that we have updated the uio_iov and uio_iovcnt values
     * This may be redundant and unnecessary, but I was being paranoid.
     * It could most likely be removed if someone wants to test it.
     */
    if ( uio->uio_iovcnt > 1 && uio->uio_iov->iov_len == 0 ) {
        uio->uio_iov++;
        uio->uio_iovcnt--;
    }

    /* Reseize the file lock for non-clones. */
    if (!( bfap->origAccp ))
        FS_FILE_READ_LOCK_RECURSIVE( contextp );

    /* Cleanup any actRange structs if we did all synchronous I/O; 
     * otherwise, we will have to clean up after the asynchronous I/O
     * has completed. 
     */
    if ( !asynch_io_started || ret != EOK ) {
        /* cleanup if error or synchronous I/O */
        mutex_lock(&bfap->actRangeLock);
        MS_SMP_ASSERT( arp->arIosOutstanding == 0 );
        remove_actRange_from_list(bfap, arp);   /* also wakes waiters */
        mutex_unlock(&bfap->actRangeLock);
        ms_free(arp);
    }

    return(ret);
}


statusT
copy_and_del_frag (
                   struct vnode *vp,
                   struct ucred *cred,
                   int flushit,
                   ftxHT ftxH
                   )
{
    bfPageRefHT pgPin;
    statusT sts;
    char *pgAddr = NULL;
    struct bfAccess *bfap = VTOA(vp);
    uint32T fragPageOffset = bfap->fragPageOffset;
    int quotas_on = VTOC(vp)->fileSetNode->quotaStatus &
                    (QSTS_USR_QUOTA_EN | QSTS_GRP_QUOTA_EN);

    sts = bs_pinpg_int( &pgPin,
                        (void*)&pgAddr,
                        bfap,
                        fragPageOffset,
                        BS_OVERWRITE,
                        PINPG_NOFLAG );

    if (sts != EOK) {
        if (sts != E_PAGE_NOT_MAPPED)
            return(sts);
        else {
            uint32T pgs_added = 0;

            /* when quotas are on we must check the space growth first
             * and if we have the quota but fail to allocate the space
             * then the quota changes will be undone by the ftx_fail
             */
            if (quotas_on) {
                sts = change_quotas(vp, 0,
                                    ADVFS_PGSZ_IN_BLKS - bfap->fragId.type * 2,
                                    NULL, cred, 0, ftxH );
                if (sts != EOK)
                    return(sts);
            }

            sts = stg_add_stg_no_cow( ftxH,
                                      bfap,
                                      fragPageOffset,
                                      1,
                                      TRUE,
                                      &pgs_added );
            if (sts != EOK) {
                goto EXIT_COPY_AND_DEL_FRAG;
            }

            sts = bs_pinpg_int( &pgPin,
                                (void*)&pgAddr,
                                bfap,
                                fragPageOffset,
                                BS_OVERWRITE,
                                PINPG_NOFLAG );
            if (sts != EOK) {
                goto EXIT_COPY_AND_DEL_FRAG;
            }

            /* when quotas are off we just force the growth adjustment
             * now since the only possible failure is an I/O error
             */
            if (!quotas_on)
                sts = change_quotas(vp, 0, pgs_added * ADVFS_PGSZ_IN_BLKS -
                                           bfap->fragId.type * 2,
                                    NULL, cred, FORCE, ftxH);
        }
    }

    sts = bcopy_frag( vp, pgAddr );
    if (sts != EOK) {
        (void) bs_unpinpg (pgPin, logNilRecord, BS_CLEAN);
        goto EXIT_COPY_AND_DEL_FRAG;
    }

    sts = bs_unpinpg(pgPin, logNilRecord,
                     flushit ? BS_WRITETHRU : BS_DIRTY);
    if (sts != EOK) {
      goto EXIT_COPY_AND_DEL_FRAG;
    }

    /*
     * We can deallocate the frag as the frag page is
     * on-disk and has precedence over the frag.
     *
     * NOTE:  This code assumes that we still hold the
     * exclusive lock.  We lock the exclusive lock for
     * synchronous writes.
     */

    return fs_delete_frag (
                          bfap->bfSetp,
                          bfap,
                          vp,
                          cred,
                          TRUE,
                          ftxH
                          );

EXIT_COPY_AND_DEL_FRAG:
        return(sts);
}  /* end copy_and_del_frag */


statusT
bcopy_frag (
            struct vnode* vp,
            char* pgAddr
            )
{
    uint32T fragByteCnt;
    bfPageRefHT pgRef;
    statusT sts;
    uint32T subFrag1ByteCnt;
    uint32T subFrag1ByteOffset;
    uint32T sizeoffset;
    bfAccessT *bfap;
    char *subFragPage = NULL;
    bfFragIdT fragId = (VTOA(vp))->fragId;

    bfap = (GETBFSETP(VTOMOUNT(vp)))->fragBfAp;
    /*
     * Copy the frag into the frag page.  The frag can span two frag
     * file pages.
     */

    fragByteCnt = fragId.type * 1024;

    subFrag1ByteOffset = FRAG2SLOT (fragId.frag) * 1024;
    subFrag1ByteCnt = ADVFS_PGSZ - subFrag1ByteOffset;
    if (subFrag1ByteCnt > fragByteCnt) {
        subFrag1ByteCnt = fragByteCnt;
    }

    sts = bs_refpg (
                    &pgRef,
                    (void*)&subFragPage,
                    bfap,
                    FRAG2PG (fragId.frag),
                    BS_NIL
                    );
    if (sts != EOK) {
        RAISE_EXCEPTION (sts);
    }

    bcopy (subFragPage + subFrag1ByteOffset, pgAddr, subFrag1ByteCnt);

    subFragPage = NULL;
    bs_derefpg(pgRef, BS_CACHE_IT);

    if (subFrag1ByteCnt < fragByteCnt) {
        sts = bs_refpg (
                        &pgRef,
                        (void*)&subFragPage,
                        bfap,
                        FRAG2PG (fragId.frag) + 1,
                        BS_NIL
                        );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }

        bcopy (subFragPage, pgAddr + subFrag1ByteCnt,
               fragByteCnt - subFrag1ByteCnt);

        subFragPage = NULL;
        bs_derefpg(pgRef, BS_CACHE_IT);
    }

    sizeoffset = (VTOA(vp))->file_size;

    if ( (sizeoffset/ADVFS_PGSZ) == (VTOA(vp))->fragPageOffset) {
        sizeoffset = sizeoffset % ADVFS_PGSZ;

        if ( sizeoffset < fragByteCnt ) {
            fragByteCnt = sizeoffset;
        }
    }

    bzero( pgAddr + fragByteCnt, ADVFS_PGSZ - fragByteCnt );

    return EOK;

HANDLE_EXCEPTION:

    if (subFragPage != NULL) {
        bs_derefpg(pgRef, BS_CACHE_IT);
    }

    return sts;

}  /* end bcopy_frag */

/*
 * uiomove_frag
 *
 * This function moves part or all of the specified frag into the
 * specified user mode location.  The value of "copyByteOffset" is
 * relative to the start of the frag.  The frag offset is relative 0.
 *
 * The caller must acquire (shared) the file lock in the context area
 * before calling this function.
 */

static
statusT
uiomove_frag (
           bfFragIdT fragId,  /* in */
           uint32T copyByteOffset,  /* in */
           uint32T copyByteCnt,  /* in */
           struct bfAccess *bfap,  /* in */
           struct uio *uio  /* in */
           )
{
    int err;
    uint32T fragByteCnt;
    statusT sts;
    uint32T subFrag1ByteCnt;
    uint32T subFrag1ByteOffset;
    char *subFrag1Page = NULL;
    bfPageRefHT subFrag1PgRef;
    char *subFrag2Page = NULL;
    bfPageRefHT subFrag2PgRef;

    /*
     * The file has a frag.  If a file has a frag, the frag has precedence
     * over the frag page, if it exists.  Normally, the frag page does
     * not exist and the last page in the file is the page that logically
     * precedes the frag page.  This is not the case if the system crashes
     * after the frag page is allocated but before the frag is deallocated.
     *
     * For an asynchronous write, the frag is deallocated either after the
     * sync at the next close or after the "30 second" sync or after the
     * next fsync syscall.
     *
     * For a synchronous write, the frag is deallocated after the sync at
     * the end of the synchronous write path.
     *
     * FIX - are logged writes handled differently?
     *
     * If this function was not called from the write path, the frag is
     * deallocated after the sync at the next close or after the "30 second"
     * sync or after the next fsync syscall.
     *
     * NOTE:  This solution assumes non-sparse files.  The solution handles
     * sparse files but it may not be optimal.
     */

    fragByteCnt = fragId.type * 1024;

    /*
     * Verify that the byte range ends in the frag.
     */
    if ((copyByteOffset + copyByteCnt) > fragByteCnt) {
        domain_panic(bfap->dmnP, "uiomove_frag: invalid byte range");
        return E_DOMAIN_PANIC;
    }

    subFrag1ByteOffset = FRAG2SLOT (fragId.frag) * 1024;
    subFrag1ByteCnt = ADVFS_PGSZ - subFrag1ByteOffset;
    if (subFrag1ByteCnt > fragByteCnt) {
        subFrag1ByteCnt = fragByteCnt;
    }

    if (copyByteOffset < subFrag1ByteCnt) {
        /*
         * The requested range starts in the first sub frag.
         */
        sts = bs_refpg (
                        &subFrag1PgRef,
                        (void*)&subFrag1Page,
                        bfap,
                        FRAG2PG (fragId.frag),
                        BS_NIL
                        );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        subFrag1Page = subFrag1Page + subFrag1ByteOffset + copyByteOffset;
        subFrag1ByteCnt = subFrag1ByteCnt - copyByteOffset;

        if (copyByteCnt <= subFrag1ByteCnt) {
            /*
             * The requested range ends in the first sub frag.
             */
            err = uiomove (subFrag1Page, copyByteCnt, uio);
            if (err) {
                sts = err;
                RAISE_EXCEPTION (sts);
            }
        } else {
            /*
             * The requested range ends in the second sub frag.
             */

            sts = bs_refpg (
                            &subFrag2PgRef,
                            (void*)&subFrag2Page,
                            bfap,
                            FRAG2PG (fragId.frag) + 1,
                            BS_NIL
                            );
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            err = uiomove (subFrag1Page, subFrag1ByteCnt, uio);
            if (err) {
                sts = err;
                RAISE_EXCEPTION (sts);
            }

            err = uiomove (subFrag2Page, copyByteCnt - subFrag1ByteCnt, uio);
            if (err) {
                sts = err;
                RAISE_EXCEPTION (sts);
            }
            subFrag2Page = NULL;
            bs_derefpg(subFrag2PgRef, BS_CACHE_IT);
        }

        subFrag1Page = NULL;
        bs_derefpg(subFrag1PgRef, BS_CACHE_IT);
    } else {
        /*
         * The requested range starts and ends in the second sub frag.
         */
        sts = bs_refpg (
                        &subFrag2PgRef,
                        (void*)&subFrag2Page,
                        bfap,
                        FRAG2PG (fragId.frag) + 1,
                        BS_NIL
                        );
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        subFrag2Page = subFrag2Page + (copyByteOffset - subFrag1ByteCnt);
        err = uiomove (subFrag2Page, copyByteCnt, uio);
        if (err) {
            sts = err;
            RAISE_EXCEPTION (sts);
        }
        subFrag2Page = NULL;
        bs_derefpg(subFrag2PgRef, BS_CACHE_IT);
    }

    return EOK;

HANDLE_EXCEPTION:

    if (subFrag1Page != NULL) {
        bs_derefpg(subFrag1PgRef, BS_CACHE_IT);
    }
    if (subFrag2Page != NULL) {
        bs_derefpg(subFrag2PgRef, BS_CACHE_IT);
    }

   return sts;

}  /* end uiomove_frag */
