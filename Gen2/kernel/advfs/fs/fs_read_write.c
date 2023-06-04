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
 *   Mainline functions to read from and write to a file.
 *
 */

#include <limits.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <bs_extents.h>             /* for extent_blk_desc_t */
#include <bs_stg.h>
#include <fs_dir_routines.h>
#include <bs_snapshot.h>
#include <bs_migrate.h>
#include <sys/param.h>
#include <sys/errno.h>
#include <sys/conf.h>
#include <sys/stat.h>
#include <sys/mount.h>
#include <sys/vnode.h>
#include <sys/uio.h>
#include <sys/kernel.h>
#include <sys/condvar.h>
#include <advfs_evm.h>
#include <sys/proc_iface.h>      /* for p_rlimit() */
#include <sys/cred.h>            /* for suser() */
#include <sys/signal.h>          /* for SIGXFSZ macro */
#include <fs/vfs_ifaces.h>       /* struct nameidata,  NAMEIDATA macro */
#include <sys/vas.h>             /* for vas_xxx() macros */
#include <h/proc_access.h>       /* for P_VAS() macro */
#include <sys/resource.h>        /* for RLIMIT_FSIZE */
#include <sys/proc_types.h>      /* struct proc */
#include <sys/proc_id.h>         /* proc_pid_self() for proc pid */
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif


#define POSIX1B_MSFS_SYNC_STATS

#ifdef POSIX1B_MSFS_SYNC_STATS
static struct {
        uint32_t fs_write_dsync_flush;
        uint32_t fs_write_dsync_update;
        uint32_t fs_write_sync_flush;
        uint32_t fs_write_sync_update;
        uint32_t fs_read_sync_flush;
        uint32_t fs_read_sync_update;
    } msfs_sync_stats = {0,0,0,0,0,0};
#   define MSFS_SYNC_STATS(foo) foo
#else
#   define MSFS_SYNC_STATS(foo)
#endif  /* ifdef POSIX1B_MSFS_SYNC_STATS */

#define ADVFS_MODULE FS_READ_WRITE

#ifdef ADVFS_SMP_ASSERT
#define KEEP_RDWR_STATS
#endif

#ifdef KEEP_RDWR_STATS

typedef struct rdwr_stats_struct {
    uint32_t read_called_from_write;
    uint32_t read_map_error;
    uint32_t read_uiomove_error;
    uint32_t read_unmap_error;
    uint32_t read_flush_error;
    uint32_t write_metawrite_attempted;
    uint32_t write_map_error;
    uint32_t write_uiomove_error;
    uint32_t write_fault_error;
    uint32_t write_unmap_error;
    uint32_t write_flush_error;
}rdwr_stats_t;

static rdwr_stats_t rdwr_stats;

#define RDWR_STATS( __stat_name ) rdwr_stats.__stat_name++


#else
#define RDWR_STATS( __stat_name )
#endif



/* In order to prevent the potential for hanging a thread in bufpin()
 * or vaslockpages() that requests a very large directIO transfer,
 * we artificially limit dio transfers to this size in advfs_fs_xxx_direct(),
 * and then call that routine multiple times.  This value may be reset to
 * a higher or lower value on the fly via debugger.
 */
static uint64_t advfs_max_dio_transfer_size = 1024 * 1024;   /* 1 Mb */

static statusT
advfs_fs_write_direct(bfAccessT  *bfap,
                struct uio *uio,
                size_t     *bytes_written,
                int         ioflag,
                int32_t     useAio,
                size_t     *vaslockpages_size);

static statusT
advfs_fs_read_direct(bfAccessT *bfap,
               struct uio *uio,
               size_t *bytes_read,
               int32_t useAio,
               size_t *vaslockpages_size);

static statusT
advfs_dio_unaligned_xfer(bfAccessT  *bfap,
               caddr_t     addr,
               int32_t     uio_seg,
               uint64_t    offset,
               size_t      nbyte,
               extent_blk_desc_t *blkmap,
               bf_fob_t    stg_fobs,
               bf_fob_t    xtra_fobs,
               int         flag);
 
void
advfs_fs_cleanup_storage( bfAccessT * bfap,
                          off_t last_off_written,
                          off_t bytes_written,
                          fcache_map_dsc_t *fcmap,
                          struct advfs_pvt_param *pvt_param);

/* advfs_fs_read
 *
 * Read from a file.
 *
 * The main purpose of this routine is to read data from a user file.
 * This interface can be used to read metadata files, typically by
 * AdvFS utilities that have opened them via .tags.  But the normal
 * way that metadata file reads are done within the kernel is the
 * bs_refpg() interface.
 *
 */

int32_t
advfs_fs_read(
        struct vnode *vp,              /* in - vnode of file to read    */
        struct uio *uio,               /* in - structure for uiomove    */
        enum uio_rw rw,                /* in - flags: AIO or not        */
        int32_t ioflag,                    /* in - includes sync i/o flags  */
        struct ucred *cred             /* in - credentials of caller    */
        )
{
    struct fsContext* contextp = VTOC(vp);
    bfAccessT *bfap = VTOA(vp);
    bfSetT *setp = bfap->bfSetp;
    int32_t error = 0;                  /* External return code */
    int32_t internal_error = 0;         /* Internal return code */
    fcache_map_dsc_t *fcmap;            /* UFC mapping for file */
    fcache_as_hdl_t fhdl;               /* Returned from fcache_as_create(). */
    struct advfs_pvt_param pvt_param;   /* Private data for fcache_as_uiomove */
    size_t bytes_left;                  /* Bytes yet to be read */
    size_t bytes_read;                  /* Bytes read by fcache_as_uiomove */
    size_t fault_bytes;                 /* Bytes read by fcache_as_fault() */
    size_t cumulative_bytes_read = 0;   /* # read by entire call so far. */
    size_t fs_limit;
    int64_t ldiff;
    off_t starting_read_offset;         /* Starting offset of the read */
    off_t next_read_offset;             /* Offset at which to read next */
    off_t mapping_offset;
    size_t mapping_size;
    faf_status_t faf_status;            /* Returned by fcache_as_fault() */
    int32_t need_to_unmap = FALSE;      /* TRUE if need to unmap */
    int32_t useDio;                     /* TRUE if file open for directIO */
    int32_t useAio = FALSE;             /* TRUE if called from AIO layer  */
    int32_t asynch_io_was_started = 0;
    int32_t sync_flags = uio->uio_fpflags & (FDSYNC | FSYNC | FRSYNC);
    int32_t overflow=0;                 /* user requested too much */
    void * vaddr;
    struct nameidata * ndp;
    struct vnode *saved_vnode;
    ni_nameiop_t saved_nameiop;

    /*
     * Screen domain panic.
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }
    
    /*
     * Screen out of sync snaps.
     */
    if (bfap->bfaFlags & BFA_OUT_OF_SYNC) {
        return EIO;
    }

    if (uio->uio_offset < 0) {
        return EINVAL;
    }
    
    if (uio->uio_resid == 0) {
        return(0);
    }
    
    /* If the vnode belongs to a shadow (reserved file via .tags), then
     * get the vnode associated w/ the real access struct
     */

    if (vp != &bfap->bfVnode) {
        vp = &bfap->bfVnode;
    }

    /* Mount/Open protection. Check if the read/write is going to 
     * exceed the MAX_LARGE_FILE limit. Else, check against the 
     * MAX_SMALL_FILE limit. Note: O_LARGFEFILE (FLARGEFILE) is 
     * automatically set during open() for 64 bit applications. Also, 
     * vfs_max_large_file is always set during mount.
     */
    if (((uio->uio_fpflags & FLARGEFILE) != 0) &&
	((vp->v_vfsp->vfs_flag & VFS_LARGEFILES) != 0)) {
	/* fs-specific overrides generic limit */
	if(vp->v_vfsp->vfs_max_large_file > 0)
	    fs_limit = (size_t) vp->v_vfsp->vfs_max_large_file;
	else {
	    fs_limit = MAX_LARGE_FILE;
	    MS_SMP_ASSERT(0);  /* we should never get here */
	}
    }
    else {
	fs_limit = MAX_SMALL_FILE;
    }

    /*
     * XXX If FRSYNC and either FDSYNC or FSYNC are set,
     *     we probably should be locking the file lock for exclusive
     *     access here to lock out any updates.  By not doing so, we
     *     allow for the possibility that we could return to the 
     *     application a buffer containing contents that were never
     *     the same as what was on disk.  This could happen if a write
     *     updated the read() range in between the fcache_as_uiomove()
     *     and the fcache_vn_flush() calls.  While this is probably a
     *     bug, it's what Tru64 does so I'm not stopping to fix this now.
     */
    ndp = NAMEIDATA();

    /* Indicate to getpage this not an mmaper */
    /* Save off the nameiop since a page fault in uiomove
     * can end up calling advfs_fs_read!
     */
    saved_nameiop = ndp->ni_nameiop;
    ndp->ni_nameiop |= NI_RW;
    saved_vnode = ndp->ni_vp;
    ndp->ni_vp = vp;
#ifdef ADVFS_SMP_ASSERT
    if (saved_nameiop & NI_RW) {
        RDWR_STATS(read_called_from_write);
    }
#endif

    /* Check if we were called from the AIO layer */
    if ( ndp->ni_nameiop & NI_AIO ) {
        useAio = TRUE;
        ndp->ni_nameiop &= ~NI_AIO;
    }

    ADVRWL_BFAPCACHEMODE_READ(bfap);
    ADVRWL_FILECONTEXT_READ_LOCK(contextp);
    starting_read_offset = next_read_offset = uio->uio_offset;

    if (bfap->dataSafety != BFD_USERDATA) {
        /*
         * Metadata uses a fixed mapping.  We need to align
         * the requested read offset to the mapping size.
         */
        fhdl = bfap->dmnP->metadataVas;
        mapping_offset = rounddown(next_read_offset, ADVFS_METADATA_PGSZ);
        
        /* We can only request a fixed block size at a time. */
        mapping_size  = ADVFS_METADATA_PGSZ;

        /* 
         * Metadata files don't maintain stats, so create a file_size.
         * XXX: If this is a snapshot, calculate this differently, based
         * on size the original file was at the time the snapshot
         * was created.
         */
        bfap->file_size = (off_t)((off_t)bfap->bfaNextFob * ADVFS_FOB_SZ);

        if (bfap->bfaParentSnap) {
            /* 
             * If the file is a snapshot child, then the file size may have been
             * incorrectly calculated from the bfaNextFob.  Must take the max of
             * file_size and bfa_orig_file_size.
             */
            bfap->file_size = max(bfap->file_size, bfap->bfa_orig_file_size );
        }
        
	bytes_left = (size_t) (MIN(uio->uio_resid, 
				   (bfap->file_size - next_read_offset)));
    }
    else {
        fhdl = bfap->dmnP->userdataVas;
	bytes_left = (size_t) (MIN(uio->uio_resid, 
				   (bfap->file_size - next_read_offset)));
        mapping_offset = next_read_offset;
        mapping_size = bytes_left;
    }


    if (next_read_offset >= bfap->file_size) {
        /*
         * Reading past end-of-file.
         * Update st_atime in the file's stat structure
         * for POSIX.
         */

        ADVSMP_FILESTATS_LOCK( &contextp->fsContext_lock );
        contextp->fs_flag |= MOD_ATIME;
        error = 0;

        if (!(setp->bfSetFlags & BFS_IM_NOATIMES))
            contextp->dirty_stats = TRUE;

        ADVSMP_FILESTATS_UNLOCK( &contextp->fsContext_lock );

        goto _error;
    }

    /*
     * Initialize filesytem private information for advfs_getpage().
     */
    pvt_param = Nil_advfs_pvt_param;
    pvt_param.app_total_bytes = (off_t) bytes_left;
    pvt_param.app_starting_offset = starting_read_offset;

    useDio = bfap->dioCnt;       /* 0 = cached IO;  >0 = directIO */

#ifdef ADVFS_SMP_ASSERT
    {
        uint64_t dummy;
        ADVFS_ATOMIC_FETCH_INCR(&bfap->bfa_stats.bfaReads, &dummy);
    }
#endif

    /*
     * Read from the file.
     */
    while (bytes_left > 0) {

	/* A file opened small can read only up to 
	 * MAX_SMALL_FILE, even if some other process
	 * has made this into large file.
	 *
	 * However, if we are already read up to the 
	 * limit (MAX_SMALL_FILE) and we are operating
	 * through a small open and there is more to
	 * read (meaning the file is large) then return
	 * EOVERFLOW. We don't want to lie by returning
	 * EOF under this condition.
	 */

	ldiff = (int64_t) (fs_limit - (size_t) uio->uio_offset);
	if (ldiff <= 0) {
            overflow = 1;    /* return EVOVERFLOW error */
	    break;  /* out of while loop */
	}

        /* If using directIO, just call the DIO routine to do the xfer */
        if ( useDio ) {
            size_t vaslockpages_size;
            size_t vaslockpages_prev;   /* optimized out in performance kern */

	    if (ldiff < bytes_left)
		bytes_read = ldiff;
	    else
		bytes_read = bfap->file_size - uio->uio_offset;
	    
            vaslockpages_size = 0;      /* initial value needs to be zero */
            do {
                vaslockpages_prev = vaslockpages_size;
                error = advfs_fs_read_direct( bfap, 
					      uio, 
					      &bytes_read, 
                                              useAio, 
					      &vaslockpages_size);

                /*
                 * if error == E_REGION_CROSSING_PLEASE_RETRY, then
                 * vaslockpages_size will return with an acceptable size that
                 * stays within the pregion.  So all we have to do is call
                 * advfs_fs_read_direct() again.
                 *
                 * WARNING: Do not add other stuff to this tight inner loop.
                 *          It is _JUST_ for processing the pregion crossing
                 *          and depends on not disturbing the vaslockpages_size
                 *          returned from the first call.  
                 *
                 *          If the call to advfs_fs_read_direct() succeeds, but
                 *          there is still more to read, this will be done by
                 *          the outer loop which will correctly re-zero
                 *          vaslockpages_size for the next transfer attempt and
                 *          pregion crossing check.
                 */
#ifdef ADVFS_SMP_ASSERT
                if ( error == E_PREGION_CROSSING_PLEASE_RETRY ) {
                    MS_SMP_ASSERT( vaslockpages_size != 0 );
                    MS_SMP_ASSERT( vaslockpages_prev == 0 ||
                                   vaslockpages_prev  > vaslockpages_size );
                    MS_SMP_ASSERT( bytes_read == 0 );
                }
#endif /* ADVFS_SMP_ASSERT */
            } while( error == E_PREGION_CROSSING_PLEASE_RETRY );

            MS_SMP_ASSERT( bytes_read <= bytes_left );
            bytes_left -= bytes_read;
            cumulative_bytes_read += bytes_read;

            if ( useAio ) {
                /* If this is a directIO read being done via the AIO 
                 * interface, figure out if an asynchronous IO was started.
                 */
                asynch_io_was_started = uio->uio_iov[1].iov_len;
            }
            if (error) {
                /* If there was a read error, we will not attempt to
                 * transfer any more data.
                 */
                goto _error;
            } 

            /* Prevent a return call into read direct if asynchronous I/O
             * has been started.  This prevents more than one call to the
             * AIO completion routine per request.
             */
            if ( asynch_io_was_started ) 
                break;

            continue;

            /* Note that the rest of this loop is not executed if the
             * file is opened for directIO.
             */

        }  /* end if ( useDio ) */

        /*
         * Set up the mapping for the next part of the read.
         */
        if (!(fcmap = fcache_as_map(fhdl, 
				    vp, 
				    mapping_offset, 
                                    mapping_size, 
				    (size_t)0, 
				    FAM_READ, 
				    &error))) {
            /*
             * If it's our fault, panic.
             */
            if (error == EINVAL) {
                ADVFS_SAD0("advfs_fs_read: bad call to UFC");
            }
            RDWR_STATS(read_map_error);
            goto _error;
        }
        need_to_unmap = TRUE;

        /* We may have mapped to the left of the request for 
         * metadata alignment reasons. We need to adjust the virtual
         * address from the mapping so we read the correct data.
         */

	vaddr = ((char *) fcmap->fm_vaddr) + 
                (next_read_offset - mapping_offset);

	bytes_read = 
	    min(bytes_left, 
		(fcmap->fm_size - 
		 (size_t)(next_read_offset - mapping_offset)));

	if (ldiff < bytes_read)
	    bytes_read = ldiff;

        if (error = fcache_as_uiomove(fcmap, 
				      vaddr, 
				      &bytes_read, 
				      uio, 
				      (uintptr_t)&pvt_param, 
				      FAUI_READ)) {
            /*
             * If it's our fault, panic.
             */
            if (error == EINVAL) {
                ADVFS_SAD0("advfs_fs_read: bad call to UFC");
            }
            RDWR_STATS(read_uiomove_error);
            goto _error;
        }

	cumulative_bytes_read += bytes_read;
        next_read_offset += (off_t)bytes_read;
        mapping_offset = next_read_offset;
        bytes_left -= bytes_read;
        if (bfap->dataSafety == BFD_USERDATA) {
            mapping_size = bytes_left;
        }

        /*
         * If we are going to read more, tear down the mapping for this 
         * part of the read.  But if we are done, keep the mapping until
         * we have checked to see if we had the chance to kick off a
         * read-ahead.
         */
        if (bytes_left > 0) {
            if (error = fcache_as_unmap(fcmap, 
					NULL, 
					(size_t)0, 
					FAU_CACHE)) {
                /*
                 * If it's our fault, panic.
                 */
                if ((error == EINVAL) || (error == EFAULT)) {
                    ADVFS_SAD0("advfs_fs_read: bad call to UFC");
                }
                RDWR_STATS(read_unmap_error);
                goto _error;
            }
            need_to_unmap = FALSE;
        }

    }  /* end while (bytes_left > 0) */ 

    /* Set up for read ahead and unmap from previous read. Unless we're
     * in an overflow condition, since we don't have a read to clean up
     * and since we won't be reading any more.
     */
    if ( !useDio && !overflow ) {
        /*
         * The requested read has completed.  If we never got into
         * advfs_getpage() because all the data was in the cache already,
         * and if advfs_setup_readahead() recommended that we kickoff
         * a read-ahead, force a call into advfs_getpage() now to start
         * the read-ahead.  By setting the APP_READAHEAD_FAST_PATH flag,
         * advfs_getpage() will only kickoff the read-ahead.
         * Ignore any errors since read-ahead is only speculative.
         */
        if (pvt_param.app_flags & APP_KICKOFF_READAHEAD) {
            fault_bytes = 1;
            pvt_param.app_flags |= APP_READAHEAD_FAST_PATH;
            if (internal_error = 
		fcache_as_fault(fcmap, 
				(char *)fcmap->fm_vaddr, 
				&fault_bytes, 
				&faf_status, 
				(uintptr_t)&pvt_param, 
				FAF_READ|FAF_ASYNC|FAF_GPAGE)) {
                /*
                 * If it's our fault, panic.
                 */
                if (internal_error == EFAULT) {
                    ADVFS_SAD0("advfs_fs_read: bad call to UFC");
                }
            }
        }

        /*
         * Now unmap the last part of the read.
         */
        if (error = fcache_as_unmap(fcmap, NULL, (size_t)0, FAU_CACHE)) {
            /*
             * If it's our fault, panic.
             */
            if ((error == EINVAL) || (error == EFAULT)) {
                ADVFS_SAD0("advfs_fs_read: bad call to UFC");
            }
            RDWR_STATS(read_unmap_error);
            goto _error;
        }
        need_to_unmap = FALSE;
    }

    /*
     * Update st_atime in the file's stat structure
     */
    ADVSMP_FILESTATS_LOCK( &contextp->fsContext_lock );
    contextp->fs_flag |= MOD_ATIME;

    if (!(setp->bfSetFlags & BFS_IM_NOATIMES)) {
        contextp->dirty_stats = TRUE;
    }
    ADVSMP_FILESTATS_UNLOCK( &contextp->fsContext_lock );

    /* 
     * If sync_flags contains FRSYNC and either FSYNC or FDSYNC, 
     * POSIX 1003.1b requires that any pending writes be written out 
     * to disk prior to the read.
     */
    if ((sync_flags & FRSYNC) && (sync_flags & (FDSYNC | FSYNC))) {
        if ( !useDio ) {
            MSFS_SYNC_STATS(msfs_sync_stats.fs_read_sync_flush++);
            if (error = fcache_vn_flush(vp, starting_read_offset, 
                                    cumulative_bytes_read, NULL, 
                                    FVF_WRITE | FVF_SYNC)) {
                /*
                 * If it's our fault, panic.
                 */
                if ((error == EINVAL) || (error == EFAULT)) {
                    ADVFS_SAD0("advfs_fs_read: bad call to UFC");
                }
                RDWR_STATS(read_flush_error);
                goto _error;
            }
        }

        /*
         * In addition, if both FRSYNC and FSYNC are set,
         * we also have to update file metadata before returning.
         */
        if (sync_flags & FSYNC) {
            MSFS_SYNC_STATS(msfs_sync_stats.fs_read_sync_update++);
            error = fs_update_stats( vp, bfap, FtxNilFtxH, 0 );
            if (error!= EOK) {
                domain_panic(bfap->dmnP, 
                             "advfs_fs_read: update stats error");
                goto _error;
            }
        }
    }

_error:

    if (overflow) {
	if (error == EOK) {
	    /* user gets back all the data that would fit plus error */
	    error = EOVERFLOW;
	}
    }

    if (need_to_unmap) {
        (void)fcache_as_unmap(fcmap, NULL, (size_t)0, FAU_CACHE);
    }

    ADVRWL_FILECONTEXT_UNLOCK(contextp);

    /* If an asynchronous I/O was started in directIO, the cache mode 
     * lock has already been disowned.
     */
    if ( !asynch_io_was_started )
        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);

    
    ndp->ni_nameiop = saved_nameiop;
    ndp->ni_vp = saved_vnode;
    return error;

}


/* advfs_fs_check_write_limits
 *
 * This routine checks whether the write would exceed limits like
 * max offset (AdvFS maximum file offset) and max file size (RLIMIT_SIZE).
 *
 * The context structure file_lock is held shared when this routine is called.
 */

static int
advfs_fs_check_write_limits( 
	     bfAccessT  *bfap,         /* in - file's bfap */
             int         ioflag,       /* in - flags for i/o (append, sync) */
             struct uio *uio,          /* in - structure for uiomove() */
             size_t     *nbyte,        /* in/out - number of bytes to write */
             uint64_t   *efbig,        /* in/out - Num bytes not to write */
             int32_t    *signal_error  /* in/out - flag to signal error */
           )
{
    size_t fs_limit;             /* 32/62-bit limit on file size */
    size_t residual;             /* Number of bytes not to write */
    size_t rlimit_fsize;         /* RLIMIT_FSIZE for process */
    size_t file_size_limit = ADVFS_MAX_OFFSET+1; /* max file size */
    size_t future_file_size;     /* Assuming we are extending the file  */
                                 /* this is the future size of the file */
                                 /* for limit checking purposes         */
    int32_t sync_flags = uio->uio_fpflags & (FDSYNC | FSYNC| FRSYNC);
    struct vnode *vp;

    /* 
     * Determine the maximum file size allowed by current ulimit() settings.
     */
    
    *signal_error = 0;
    vp = ATOV(bfap);

    if (!(ioflag & IO_NORLIMIT)) {
        rlimit_fsize = p_rlimit(u.u_procp)[RLIMIT_FSIZE].rlim_cur;
    }
    else {
        rlimit_fsize = ADVFS_MAX_OFFSET + 1;
    }

    /*
     * The maximum file size this thread can write is the minimum of the
     * ulimit() setting for file size and the maximum offset that AdvFS
     * currently supports.
     */
    file_size_limit = MIN (rlimit_fsize, file_size_limit );

    /* Mount/Open protection. Check if the read/write is going to 
     * exceed the MAX_LARGE_FILE limit. Else, check against the 
     * MAX_SMALL_FILE limit. Note: O_LARGFEFILE (FLARGEFILE) is 
     * automatically set during open() for 64 bit applications. Also, 
     * vfs_max_large_file is always set during mount.
     */
    if (((uio->uio_fpflags & FLARGEFILE) != 0) &&
	((vp->v_vfsp->vfs_flag & VFS_LARGEFILES) != 0)) {
	/* fs-specific overrides generic limit */
	if (vp->v_vfsp->vfs_max_large_file > 0)
	    fs_limit = (size_t) vp->v_vfsp->vfs_max_large_file;
	else {
	    fs_limit = MAX_LARGE_FILE;
	    MS_SMP_ASSERT(0);  /* we should never get here */
	}
    }
    else {
	fs_limit = MAX_SMALL_FILE;
    }

    if (fs_limit < file_size_limit)
	file_size_limit = fs_limit;

    if ((size_t)uio->uio_offset >= file_size_limit) {
        if ( (size_t)uio->uio_offset >= rlimit_fsize ) {
            /*
             * Exceeding RLIMIT_FSIZE generates a SIGXFSZ signal.
             * There is some debate about whether exceeding 
             * ADVFS_MAX_OFFSET should also generate a SIGXFSZ signal.  See
             * the comments at the beginning of the fs_write() code for
             * more details.  For now, exceeding ADVFS_MAX_OFFSET does not
             * generate a signal.
             */
            *signal_error = 1;
        }
        *efbig = 0;
        return EFBIG;
    }

    /*
     * We also have to check to see if we are growing the file past the
     * limits, so calculate the size of the file after the write is finished
     * (assuming this write actually grows the file; if we are not growing
     * the file then this is just the next write offset, and since we would
     * be within the existing file, no errors would be generated). 
     */
    future_file_size = (size_t) (uio->uio_offset + uio->uio_resid);

    if (future_file_size > file_size_limit) {
        if ((ioflag & IO_SYNC) || (sync_flags & (FSYNC | FDSYNC))) {
            /*
             * FSYNC and FDSYNC are only supposed to return success if
             * they are able to write the entire request to the file.
             * Since we do not have enough room, we will not be able to
             * succeed, so return an error now.  
             */
            if ( future_file_size > rlimit_fsize ) {
                /*
                 * Since the only way we could perform this write is to
                 * extend the file past RLIMIT_FSIZE, then that falls under
                 * the rules for generating a SIGXFSZ signal.  There is some
                 * debate about whether exceeding ADVFS_MAX_OFFSET 
                 * should also generate a SIGXFSZ signal.  See the comments
                 * at the beginning of the advfs_fs_write() code for 
                 * more details.  For now, exceeding ADVFS_MAX_OFFSET does 
                 * not generate a signal.
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
        *efbig = (uint64_t) residual;   /* return bytes not to be written */
        uio->uio_resid -= (long) residual; /* decrease size of xfer request */
        *nbyte -= residual; /* decrease number of bytes to write */
    } else {
        *efbig = 0;
    }
    return 0;
}

/*    advfs_fs_write
 *
 *    Write to a file.
 *
 *    Note on FSYNC (O_SYNC) and FDSYNC (O_DSYNC):
 *        The standards (UNIX98) for FDSYNC say "The write is complete
 *        only when the data specified in the write request is successfully
 *        transferred and all file system information required to retrieve
 *        the data is successfully transferred."  FSYNC says "Identical to
 *        the FDSYNC with the addition that all file attributes relative
 *        to the I/O operation (including access time, modification time,
 *        status change time) will be successfully transferred prior to
 *        returning to the calling process."
 *
 *        When I/O Sync is enabled (FSYNC or FDSYNC), _AND_ an error
 *        occurs, advfs_fs_write() restores the uio_resid and
 *        uio_offset fields to their original values.  This is done
 *        because rwuio()/prwuio() will ignore any errors reported if
 *        the uio_resid value has been modified.  There is some
 *        discussion about whether I/O Sync allows for partial writes
 *        to be returned when I/O Sync is enabled.  However the
 *        operation of advfs_fs_write() is to copy all the write()
 *        data into the cache and then call UFC to flush the cache to
 *        disk. There is no guarantee on the IO order that the cache
 *        is written to disk.
 *
 *        Plus there are volume managers and RAID controllers that may introduce
 *        additional optimizations.  This makes it difficult to know in an
 *        error situation just which part of the I/O made it to disk and if
 *        that was the first part of the request.  So for AdvFS, we treat
 *        I/O Sync as either all of the data was written or we report an
 *        error.  This means we have to restore the uio_resid and uio_offset
 *        to their original values before returning, so that
 *        rwuio()/prwuio() will report the errors. 
 *
 *    Note on partial writes:
 *        No partial write is allowed for FSYNC or FDSYNC when the 
 *        current write will exceed the file size limit or there is 
 *        no more storage left for the write.
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
 *        it should be signaled when an EFBIG or ENOSPC error condition
 *        occurs _AND_ no data has been transferred yet.
 *        That is to say when the request can not be started because there
 *        is no more room for even the first byte of data and this could be
 *        because the disk is full, the ulimit for the file has been
 *        exceeded, the maximum file offset for file has been exceeded, or
 *        there is no more quota for this user.
 *
 *        Another view is that it only applies to ulimit.
 */
int advfs_write_opt = 1;

int
advfs_fs_write(
         struct vnode *vp,              /* in - vnode of file to write */
         struct uio *uio,               /* in - structure for uiomove */
         enum uio_rw rw,                /* in - read/write flags */
         int ioflag,                    /* in - flags - append, sync, etc. */
         struct ucred *cred             /* in - credentials of caller */
         )
{
    struct fsContext* contextp = VTOC(vp);
    bfAccessT *bfap = VTOA(vp);
    int32_t error = 0;                      /* External return code */
    int32_t internal_error = 0;             /* Internal return code */
    int32_t ftx_started = FALSE;            /* TRUE if transaction is extant */
    ftxHT ftxH = FtxNilFtxH;            /* Transaction handle */
    uint64_t efbig = 0;                 /* Bytes we can't write past limits */
    int32_t signal_error = FALSE;       /* TRUE if SIGXFSZ should be sent */
    fcache_map_dsc_t *fcmap;            /* UFC mapping for file */
    struct advfs_pvt_param pvt_param;   /* Private data for fcache_as_uiomove */
    faf_status_t faf_status;            /* Returned by fcache_as_fault() */
    int32_t need_to_unmap = FALSE;          /* TRUE if need to unmap */
    size_t bytes_written,               /* # written by fcache_as_uiomove(). */
           fault_bytes;                 /* Bytes read by fcache_as_fault() */
    size_t cumulative_bytes_written;    /* # written by entire call so far. */
    size_t bytes_left;                  /* Bytes yet to be written */
    off_t next_write_offset;            /* Offset at which to write next */
    int32_t saved_uerror;                   /* Saved off u.u_error */
    off_t orig_offset = uio->uio_offset;/* Save original uio_offset */
    size_t orig_resid = uio->uio_resid; /* Save original uio_resid */
    size_t mapping_size;
    off_t mapping_offset;
    fcache_vn_attr_t fcacheAttr;
    struct nameidata *ndp;
    struct vnode *saved_vnode;
    int32_t useDio;                     /* > 0  if file open for directIO */
    int32_t useAio = FALSE;             /* TRUE if called from AIO layer  */
    int32_t asynch_io_was_started = 0;
    int first_pass = TRUE;
    int32_t sync_flags = uio->uio_fpflags & (FDSYNC | FSYNC | FRSYNC);
    uint32_t small_file_already_had_storage = FALSE;
    fcache_map_dsc_t *zfcmap;            /* Zero-fill UFC map for small files */
    faui_flags_t uiomove_flags = FAUI_WRITE;

    ndp = NAMEIDATA();

    if ( vp->v_type != VREG ) {
        return EOPNOTSUPP;
    }
 
    if (!(bytes_left = uio->uio_resid)) {
        return EOK;
    }

    /*
     * Screen domain panic.
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }

    /*
     * Screen out of sync snaps.
     */
    if (bfap->bfaFlags & BFA_OUT_OF_SYNC) {
        return EIO;
    }

    /*
     * This interface is not to be used for metadata files.
     * Metadata files should only be updated using the
     * transactional bs_pinpg() interface.
     */
    if ((bfap->dataSafety == BFD_METADATA) || (bfap->dataSafety == BFD_LOG)) {
        RDWR_STATS(write_metawrite_attempted);
        return EOPNOTSUPP;
    }

    /* Indicate to getpage this not an mmaper */
    /* Save off the namei flags. An unaligned page fault
     * could cause VM to call vn_write in the middle of this
     * read (we fault on the destination buffer). This can cause
     * our flag to get reset. 
     */
    MS_SMP_ASSERT(!(ndp->ni_nameiop & NI_RW));
    ndp->ni_nameiop |= NI_RW;
    saved_vnode = ndp->ni_vp;
    ndp->ni_vp = vp;

    /* Check if we were called from the AIO layer */
    if ( ndp->ni_nameiop & NI_AIO ) {
        useAio = TRUE;
        ndp->ni_nameiop &= ~NI_AIO;
    }

    /* This following provides synchronization with snapset creation. */
    ADVFS_SNAPSHOT_CREATE_SYNC( bfap );

    /* Get our locks.  If we are extending the file, we need to grab the
     * file write lock so that we can update bfap->file_size atomically.
     *
     * NOTE: Extending the file does _NOT_ always mean we will
     *       allocate storage.  For example, if we are just writing a little
     *       bit, the storage may already be pre-allocated, so
     *       advfs_getpage() will not automatically acquire the file write
     *       lock for us.  That is why it is important for us to get the
     *       file write lock here when there is a possibility that the
     *       file_size may change.
     *
     * Append needs to start at the current end-of-file, and to maintain
     * proper append semantics, the end-of-file can not be changed except by
     * the append write, so the file write lock must be held before grabbing
     * the current file size.
     */
    ADVRWL_BFAPCACHEMODE_READ(bfap);
    if ( (uio->uio_offset + uio->uio_resid) > bfap->file_size ||
         (ioflag & IO_APPEND) ) {
        uiomove_flags |= FAUI_WRITE_EXCL;
        ADVRWL_FILECONTEXT_WRITE_LOCK(contextp);

	if ( ioflag & IO_APPEND ) {
            /* This needs to be done while holding the file write lock */
            uio->uio_offset = bfap->file_size;
	}
    }
    else {
        ADVRWL_FILECONTEXT_READ_LOCK(contextp);
        if ( (uio->uio_offset + uio->uio_resid) > bfap->file_size ) {
            /* Ouch!  Someone truncated the file while we waited for the
             * file read lock.  Now this write will change the file_size so
             * a file write lock is needed.  There is a chance that between
             * dropping the file read lock and getting the file write lock,
             * the file will grow so that this is no longer an extending
             * write, but it is not worth worrying about that now.
             */
            ADVRWL_FILECONTEXT_UNLOCK(contextp);
            uiomove_flags |= FAUI_WRITE_EXCL;
            ADVRWL_FILECONTEXT_WRITE_LOCK(contextp);
        }
    }
    next_write_offset = uio->uio_offset;

    /* We need to initialize this before any potential goto _error's
     */

    pvt_param = Nil_advfs_pvt_param;

    /*
     * Check this write request against any limits.  This call may
     * adjust uio->uio_resid.  If it does, we will readjust this
     * before returning to the caller at the end of advfs_fs_write().
     */
    if (error = advfs_fs_check_write_limits(bfap,   
                                        ioflag,
                                        uio,   /* uio_resid may get adjusted */
                                        &bytes_left,   /* Bytes we can write */
                                        &efbig,      /* Bytes we can't write */
					&signal_error)) {
        goto _error;
    }

    cumulative_bytes_written = 0;

    useDio = bfap->dioCnt;       /* 0 = cached IO;  >0 = directIO */

    /*
     * Initialize filesytem private information for advfs_getpage().
     */
    pvt_param.app_total_bytes = bytes_left;
    pvt_param.app_starting_offset = next_write_offset;

#ifdef ADVFS_SMP_ASSERT
    {
        uint64_t dummy;
        ADVFS_ATOMIC_FETCH_INCR(&bfap->bfa_stats.bfaWrites, &dummy);
    }
#endif

    /*
     * Write to the file.
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
    mapping_offset = next_write_offset;

    while (bytes_left > 0) {

        /* If using directIO, just call the DIO routine to do the xfer */
        if ( useDio ) {
            size_t vaslockpages_size;
            size_t vaslockpages_prev;   /* optimized out in performance kern */

            vaslockpages_size = 0;      /* initial value needs to be zero */
            do {
                bytes_written = 0;
                vaslockpages_prev = vaslockpages_size;
                error = advfs_fs_write_direct( bfap, uio, &bytes_written, 
                                               ioflag, useAio, 
                                               &vaslockpages_size );
                /*
                 * if error == E_REGION_CROSSING_PLEASE_RETRY, then
                 * vaslockpages_size will return with an acceptable size that
                 * stays within the pregion.  So all we have to do is call
                 * advfs_fs_write_direct() again.
                 *
                 * WARNING: Do not add other stuff to this tight inner loop.
                 *          It is _JUST_ for processing the pregion crossing
                 *          and depends on not disturbing the vaslockpages_size
                 *          returned from the first call.  
                 *
                 *          If the call to advfs_fs_write_direct() succeeds,
                 *          but there is still more to write, this will be done
                 *          by the outer loop which will correctly re-zero
                 *          vaslockpages_size for the next transfer attempt and
                 *          pregion crossing check.
                 */
#ifdef ADVFS_SMP_ASSERT
                if ( error == E_PREGION_CROSSING_PLEASE_RETRY ) {
                    MS_SMP_ASSERT( vaslockpages_size != 0 );
                    MS_SMP_ASSERT( vaslockpages_prev == 0 ||
                                   vaslockpages_prev  > vaslockpages_size );
                    MS_SMP_ASSERT( bytes_written == 0 );
                }
#endif /* ADVFS_SMP_ASSERT */
            } while( error == E_PREGION_CROSSING_PLEASE_RETRY );

            MS_SMP_ASSERT( bytes_written <= bytes_left );
            bytes_left -= bytes_written;
            cumulative_bytes_written += bytes_written;
            if (ioflag & IO_APPEND) {
                /* Because the file lock can be dropped in fs_write_direct(),
                 * we have to update the starting offset if the EOF changed.
                 * app_starting_offset is modified so that bfaLastWrittenFob
                 * will be correctly calculated later.
                 */
                next_write_offset = uio->uio_offset;
                if (first_pass) {
                    pvt_param.app_starting_offset =  
                        next_write_offset - bytes_written;
                    first_pass = 0;
                }
            } else 
                next_write_offset += bytes_written;
           
            if ( useAio ) {
                /* If this is a directIO write being done via the AIO 
                 * interface, figure out if an asynchronous IO has been
                 * started. This affects how we will cleanup.
                 */
                asynch_io_was_started = uio->uio_iov[1].iov_len;
            }
            if (error) {
                if ( error == E_PLEASE_RETRY ) {
                    /* This is an explicit request to retry the call. */
                    continue;
                } else {
                   /* There was a write error, we will not attempt to
                    * transfer any more data.
                    */
                    break;
                } 
            } 

            /* Prevent a return call into write direct if asynchronous I/O
             * has been started.  This prevents more than one call to the
             * AIO completion routine per request.
             */
            if ( asynch_io_was_started ) 
                break;

            /* If we need to go back for another pass through the directIO
             * code, update the file size.  This will prevent the storage
             * allocation code from deallocating what we just wrote thinking
             * that it is merely preallocated storage.
             */
            if (bytes_left && next_write_offset > bfap->file_size) {
                bfap->file_size = next_write_offset;
            }

            continue;

            /* Note that the rest of this loop is not executed if the
             * file is opened for directIO.
             */

        }  /* end if ( useDio ) */

        /* If snapshots exist, the mapping size needs to be bounded to
         * prevent large storage allocations. */
        if ( bfap->bfSetp->bfsNumSnapChildren > 0 ) {
            mapping_size = MIN(bytes_left, 
                    (MAX_ALLOC_FOB_CNT * ADVFS_FOB_SZ) / 
                    bfap->bfSetp->bfsNumSnapChildren);
        } else {
            mapping_size = bytes_left;
        }

        /*
         * Set up the mapping for the next part of the write.
         */
        if (!(fcmap = fcache_as_map(bfap->dmnP->userdataVas, vp, 
                                    mapping_offset, mapping_size, (size_t)0,
                                    FAM_WRITE, &error))) {
            /*
             * If it's our fault, panic.
             */
            if (error == EINVAL) {
                ADVFS_SAD0("advfs_fs_write: bad call to UFC");
            }
            RDWR_STATS(write_map_error);
            break;
        }
        need_to_unmap = TRUE;

        /*
         * Set bytes_written to zero so that fcache_as_uiomove() will
         * try to write to the end of the mapping.
         */
        bytes_written = 0;

        /*
         * If this is a small file and we are writing into a new storage
         * allocation unit, force a call into advfs_getpage() so that we
         * will have the opportunity to allocate new storage, if necessary.
         */ 
        if (ADVFS_SMALL_FILE(bfap)) {
            small_file_already_had_storage = (uint32_t)bfap->bfaNextFob; 
            if ((pvt_param.app_starting_offset + pvt_param.app_total_bytes) > 
              (roundup(bfap->file_size, 
                       ((off_t)(bfap->bfPageSz * ADVFS_FOB_SZ))))) {
                error = fcache_as_fault(fcmap, NULL, &bytes_written, 
                                        &faf_status, (uintptr_t)&pvt_param, 
                                        FAF_WRITE|FAF_SYNC|FAF_GPAGE);
                if (bytes_written) {
                    internal_error = uiomove(fcmap->fm_vaddr, 
                                             (int)bytes_written, 
                                             UIO_WRITE, uio);
                    if (internal_error && !error) {
                        error = internal_error;
                    }
                }
            }
            else {
                error = fcache_as_uiomove(fcmap, NULL, &bytes_written, uio, 
                                          (uintptr_t)&pvt_param, 
                                          uiomove_flags | 
                                         (advfs_write_opt ? FAUI_IO_AVOID : 0));
            }
        }
        else {
            error = fcache_as_uiomove(fcmap, NULL, &bytes_written, uio, 
                                      (uintptr_t)&pvt_param, 
                                      uiomove_flags | 
                                      (advfs_write_opt ? FAUI_IO_AVOID : 0));
        }

        /*
         * If this file is currently a small file with storage but we are 
         * changing that by writing beyond VM_PAGE_SZ, the above code will have
         * already backfilled the first VM_PAGE_SZ bytes of the file with
         * storage and zero-filled the VM page in memory.  But that page may
         * not be marked dirty.  Make sure it is by zeroing one byte in 
         * that newly-allocated range.
         * Boy, this is a lot of work.  I sure wish VM would let me set
         * the setdirty bit before I read in the first partial page,
         * which I already zero-filled in advfs_start_blkmap_io().
         *
         * Only do this if we know that the first VM_PAGE_SZ bytes of the
         * file were, indeed, backfilled.  And don't overwrite any errors
         * returned above.
         */
        if ((small_file_already_had_storage) && 
            (orig_offset >= (off_t)(VM_PAGE_SZ)) &&
            (bfap->bfaNextFob >= (VM_PAGE_SZ / ADVFS_FOB_SZ))) {


            /*
             * Map the last byte in the first VM_PAGE_SZ bytes of the file
             * and zero it.
             */
            if (!(zfcmap = fcache_as_map(bfap->dmnP->userdataVas, vp, 
                                        VM_PAGE_SZ - 1, 1, (size_t)0,
                                        FAM_WRITE, &internal_error))) {
                /*
                 * If it's our fault, panic.
                 */
                if (internal_error == EINVAL) {
                    ADVFS_SAD0("advfs_fs_write: bad call to UFC");
                }
                RDWR_STATS(write_map_error);
            }
            else {
                bzero(zfcmap->fm_vaddr, 1);

                if (internal_error = fcache_as_unmap(zfcmap,
                                                 NULL,
                                                 (size_t)0,
                                                 FAU_CACHE | FAU_WBEHIND)) {
                    /*
                     * If it's our fault, panic.
                     */
                    if ((internal_error == EINVAL) || 
                        (internal_error == EFAULT)) {
                        ADVFS_SAD0("advfs_fs_write: bad call to UFC");
                    }
                    RDWR_STATS(write_map_error);
                }
            }

            /*
             * Don't do this more than once.
             */
            small_file_already_had_storage = FALSE;
        }

        cumulative_bytes_written += bytes_written;
        next_write_offset += bytes_written;
        mapping_offset = next_write_offset;
        bytes_left -= bytes_written;
        mapping_size = bytes_left;

        if (error) {
            /*
             * If it's our fault, panic.
             */
            if (error == EINVAL) {
                ADVFS_SAD0("advfs_fs_write: bad call to UFC");
            }
            RDWR_STATS(write_uiomove_error);
            /* We have recorded any storage added in getpage. We now
             * need to remove any storage that we did not overwrite.
             * We also need to zero any storage that was not overwritten
             * and can not be removed (ie allocation unit).
             */
            if (pvt_param.stg_desc.app_stg_end_fob != 0) {
                /* We did add some storage, lets see if we need to
                 * remove it */

                (void) advfs_fs_cleanup_storage( bfap,
                                                 next_write_offset,
                                                 bytes_written,
                                                 fcmap, 
                                                 &pvt_param);
            }
            /* Clear these so we don't try to free the memory again.
             */
            pvt_param.stg_desc.app_stg_end_fob=0;
            pvt_param.stg_desc.next = NULL;  
            break;
        }

        MS_SMP_ASSERT(bytes_written == fcmap->fm_size);

        /*
         * If we are going to write more, tear down the mapping for this 
         * part of the read.  But if we are done, keep the mapping until
         * we have checked to see if we had the chance to kick of a
         * read-ahead.
         */
        if (bytes_left > 0) {
            if (error = fcache_as_unmap(fcmap,
                                        NULL,
                                        (size_t)0,
                                        FAU_CACHE | FAU_WBEHIND)) {
                /*
                 * If it's our fault, panic.
                 */
                if ((error == EINVAL) || (error == EFAULT)) {
                    ADVFS_SAD0("advfs_fs_write: bad call to UFC");
                }
                RDWR_STATS(write_map_error);
                break;
            }
            need_to_unmap = FALSE;
        }

        /*
         * Check to see if a domain panic has occurred while we've been
         * writing away.  If it has, then all our I/O requests are being
         * returning as success even though the domain is now out of service.
         * Note that this particular error is not handled the way that most
         * errors are in this loop.  We go right to the error handler.
         */
        if ( bfap->dmnP->dmn_panic ) {
           error = EIO;
           goto _error;
        }
        /* We are recording any storage added in getpage and passing this
         * info back thru the private params. We hope to just use the imbedded
         * descriptor in the private params structure. If there was more than
         * a single allocation we malloced it and attached it to this struct.
         * since there were no errors we wont need to remove this storage so
         * we can free up this struct now.
         */
        if (pvt_param.stg_desc.next != NULL) {
            struct advfs_pvt_stg_desc *ptr, *fptr;
            fptr = pvt_param.stg_desc.next;
            while(fptr != NULL) {
                ptr=fptr->next;
                ms_free(fptr);
                fptr=ptr;
            }
            pvt_param.stg_desc.next = NULL;  
        }
        pvt_param.stg_desc.app_stg_end_fob=0;

    }  /* end while (bytes_left > 0) */

    /*
     * Check to see if there were any errors.
     *
     * In the async I/O case if any data was successfully written to a page
     * of the file, then we continue and report a partial write to the
     * caller (by returning the error along with a non-zero uio_resid).
     *
     * If synchronous I/O was requested via any of the several flags, then
     * we take the error path and report the error to the caller (by
     * returning the error and the original uio_resid value).
     */
    if ( error &&
            ( (!cumulative_bytes_written) ||
              (ioflag & IO_SYNC)          ||
              (sync_flags & (FSYNC | FDSYNC)) ) ) {
        goto _error;
    }

    if ( !useDio ) {
        /*
         * The requested write() has completed.  If we never got into
         * advfs_getpage() because all the data was in the cache already,
         * and if advfs_setup_readahead() recommended that we kickoff
         * a read-ahead, force a call into advfs_getpage() now to start
         * the read-ahead.  By setting the APP_READAHEAD_FAST_PATH flag,
         * advfs_getpage() will only kickoff the read-ahead.
         * Ignore any errors since read-ahead is only speculative.
         * If the uiomove call came back with an error (like ENOSPC)
         * then lets bag readahead. 
         */
        if ((pvt_param.app_flags & APP_KICKOFF_READAHEAD) &&
            (!error)) {
            fault_bytes = 1;

            MS_SMP_ASSERT(bytes_written > 0);

            pvt_param.app_flags |= APP_READAHEAD_FAST_PATH;
            if (internal_error = fcache_as_fault(fcmap, 
                                             (char *)fcmap->fm_vaddr,
                                             &fault_bytes, &faf_status, 
                                             (uintptr_t)&pvt_param, 
                                             FAF_READ | FAF_SYNC | FAF_GPAGE)) {
                /*
                 * If it's our fault, panic.
                 */
                if (internal_error == EFAULT) {
                    ADVFS_SAD0("advfs_fs_write: bad call to UFC");
                }
                RDWR_STATS(write_fault_error);
            }
        }

        /*
         * Now unmap the last part of the write.
         */
        if (need_to_unmap && 
            (internal_error = fcache_as_unmap(fcmap, 
                                              NULL, 
                                              (size_t)0, 
                                              FAU_CACHE | FAU_WBEHIND))) {
            /*
             * If it's our fault, panic.
             */
            if ((internal_error == EINVAL) || (internal_error == EFAULT)) {
                ADVFS_SAD0("advfs_fs_write: bad call to UFC");
            }
            if (error == 0) {
                error = internal_error;
            }
            RDWR_STATS(write_unmap_error);
            goto _error;
        }
        need_to_unmap = FALSE;
    }

    /*
     * Save the last FOB written so that the storage allocation code
     * can detect sequential allocations and can make better decisions
     * about whether preallocating storage makes sense.
     */
    bfap->bfaLastWrittenFob = ADVFS_OFFSET_TO_FOB_DOWN(
                                     pvt_param.app_starting_offset + 
                                     cumulative_bytes_written - 1);

    ADVSMP_FILESTATS_LOCK( &contextp->fsContext_lock );

    /*
     * Update st_size and st_mtime in the file's stat structure
     * IO_NOSTATUPDATE is used by CFS to tell the PFS not to update
     * the mtime and ctime as the data is being written past close.
     */
    if (!(ioflag & IO_NOSTATUPDATE)) {
        contextp->fs_flag |= (MOD_MTIME | MOD_CTIME);
    }
    contextp->dirty_stats = TRUE;

    /*
     * If we extended the file, update the file length.
     */
    if (next_write_offset > bfap->file_size) {
        bfap->file_size = next_write_offset;
    }

    /*
     * If this is not a privileged thread and it is not the owner
     * of the file being written:
     *
     * 1. Turn off the S_ISUID bit.  If it's already off, this is a no-op.
     *    If it's on, this writer may have patched (Trojan Horse) an
     *    executable that had special privileges associated with it via
     *    the S_ISUID bit.  Since he is not privileged, we turn off the
     *    S_ISUID bit.
     * 2. Similarly, if the S_ISGID bit AND the S_IXGRP bits are both
     *    set, we turn off the S_ISGID bit.  If ONLY the S_ISGID bit is
     *    set, this indicates enforcement mode locking, so leave it on.
     */

    /*
     * Calling suser() may reset u.u_error so save it off so we can 
     * restore it.
     */
    saved_uerror = u.u_error;

    if (!suser() && (cred->cr_uid != contextp->dir_stats.st_uid)) {
        contextp->dir_stats.st_mode &= 
            ~(((contextp->dir_stats.st_mode & S_ISGID) &&
               (contextp->dir_stats.st_mode & S_IXGRP)) ? S_ISUID | S_ISGID : 
                                                          S_ISUID);
    }
    u.u_error = saved_uerror;

    ADVSMP_FILESTATS_UNLOCK( &contextp->fsContext_lock );

    /*
     * If this is a synchronous write, flush the written data to disk.
     */
    if ((ioflag & IO_SYNC) || (sync_flags & (FSYNC | FDSYNC))) {

        if ( !useDio ) {
            if (internal_error = fcache_vn_flush(vp, orig_offset, orig_resid, NULL,
                                    FVF_WRITE | FVF_SYNC)) {
                /*
                 * If it's our fault, panic.
                 */
                if ((internal_error == EINVAL) || (internal_error == EFAULT)) {
                    ADVFS_SAD0("advfs_fs_write: bad call to UFC");
                }
                if (error == 0) {
                    error = internal_error;
                }
                RDWR_STATS(write_flush_error);
                goto _error;
            }

            if (sync_flags & FDSYNC)
                MSFS_SYNC_STATS(msfs_sync_stats.fs_write_dsync_flush++);
            else
                MSFS_SYNC_STATS(msfs_sync_stats.fs_write_sync_flush++);
        }

        /* 
         * Update stats if applicable.
         * If customers want "O_DSYNC" behavior for "O_SYNC" 
         * applications, they can set the "o_sync_is_o_dsync"
         * tunable, which will change the flags on-the-fly in copen().
         */
        if ((ioflag & IO_SYNC) || (sync_flags & FSYNC) || 
            (contextp->dirty_alloc == TRUE)) {

            if (internal_error = FTX_START_N(FTA_NULL, 
                                         &ftxH, 
                                         FtxNilFtxH, 
                                         bfap->dmnP
                                         )) {
                   domain_panic(bfap->dmnP, 
                                "advfs_fs_write: FTX_START_N error");
                   error = EIO;
                   goto _error;
            }
            ftx_started = TRUE;

            if (internal_error = fs_update_stats(vp, bfap, ftxH, 0)) {
                   domain_panic(bfap->dmnP, 
                                "advfs_fs_write: update stats error");
                   error = EIO;
                   goto _error;
            }

            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_460);

            ftx_special_done_mode(ftxH, FTXDONE_LOGSYNC);

            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_9);

            ftx_done_n(ftxH, FTA_OSF_FSYNC_V1);

            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_10);

            ftx_started = FALSE;

            if ((ioflag & IO_SYNC) || (sync_flags & FSYNC)) {
                MSFS_SYNC_STATS(msfs_sync_stats.fs_write_sync_update++);
            }
            else {
                MSFS_SYNC_STATS(msfs_sync_stats.fs_write_dsync_update++);
            }
        } 
        contextp->dirty_alloc = FALSE;
    } 

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
     * vno_rw() will calculate the new file offset correctly.
     */
    uio->uio_resid += efbig;
    efbig = 0;  /* zero efbig in case we take an error path */

    if (error) {
        goto _error;
    }

    ADVRWL_FILECONTEXT_UNLOCK( contextp );

    /* If an asynchronous I/O was started in directIO, the cache mode 
     * lock has already been disowned.
     */
    if ( !asynch_io_was_started )
        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);

    ndp->ni_nameiop &= ~NI_RW;
    ndp->ni_vp = saved_vnode;

    /* 
     * This will wake any sleeping snapset creation threads 
     * (advfs_snap_drain_writes) waiting for this last write to finish 
     */
    ADVFS_SNAPSHOT_DONE_SYNC( bfap );
    return(0);

_error:

    if (ftx_started) {
        /*
         * Clean up the started transaction. 
         */
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_481);
        ftx_fail(ftxH);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_482);
    }

    if (need_to_unmap) {
        (void)fcache_as_unmap(fcmap, NULL, (size_t)0, FAU_CACHE|FAU_WBEHIND);
    }

    ADVRWL_FILECONTEXT_UNLOCK( contextp );

    if (pvt_param.stg_desc.next != NULL) {
        struct advfs_pvt_stg_desc *ptr, *fptr;
        fptr = pvt_param.stg_desc.next;
        while(fptr != NULL) {
            ptr=fptr->next;
            ms_free(fptr);
            fptr=ptr;
        }
    }

    /* If an asynchronous I/O was started in directIO, the cache mode 
     * lock has already been disowned.
     */
    if ( !asynch_io_was_started )
        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);

    ndp->ni_nameiop &= ~NI_RW;
    ndp->ni_vp = saved_vnode;

    /*
     * It is now safe for other snapshots to be created.
     */
    ADVFS_SNAPSHOT_DONE_SYNC( bfap );

    /*
     * For the async I/O case we can blindly add 'efbig' to uio_resid.
     * If it has already been added back into uio_resid, then 'efbig' will
     * have been set to zero (the add becomes a no-op).  Or 'efbig' was
     * always zero and again the add is a no-op.  But if 'efbig' is
     * non-zero, then it needs to be added back into uio_resid for correct
     * vn_write()/vn_pwrite() and rwuio()/prwuio() calculations, so we are
     * doing it now. 
     *
     * If synchronous I/O is enabled, their checks are next, and
     * they will correctly reset uio_resid to what they need for correct
     * operations. 
     */
    uio->uio_resid += efbig;

    /*
     * If I/O sync is enabled, reset the uio->uio_resid and offset 
     * back to their original values.  This is done because 
     * rwuio()/prwuio() will ignore an error if a partial write 
     * has occurred and I/O sync is not satisfied with a partial write.  
     */
    if ((ioflag & IO_SYNC) || (sync_flags & (FSYNC | FDSYNC))) {
        uio->uio_offset = orig_offset;
        uio->uio_resid  = orig_resid;
    }

    /*
     * Send the process a SIGXFSZ signal if we need to do so.
     */
    if ((signal_error) && 
        (IS_XSIG(u.u_procp) || IS_SIGAWARE(u.u_kthreadp, SIGXFSZ))) {
            psignal(u.u_procp, SIGXFSZ);
    }

    return (error);
}

/* This is only called in an error case from advfs_fs_write.
 * If a write caused storage to be added and then failed
 * for some reason (ie bad user buffer), we need to remove
 * any storage that was not overwritten. We may also need 
 * to zero out an allocation unit. NOTE this is now the only
 * place where storage can be removed from the middle of a
 * user data file.
 */
void
advfs_fs_cleanup_storage( bfAccessT * bfap,
                          off_t last_off_written,
                          off_t bytes_written,
                          fcache_map_dsc_t *fcmap, /* UFC map for small files */
                          struct advfs_pvt_param *pvt_param /* private params */
    )
{
    struct advfs_pvt_stg_desc *ptr,*fptr;
    off_t offset_to_zero = bytes_written; /*relative to mapping */
    off_t bytes_to_zero;
    off_t offset_to_remove;
    off_t bytes_to_remove;
    uint32_t delCnt;
    void *delList;
    statusT sts;
    int first = TRUE;
    ftxHT ftxH;

    fptr = &pvt_param->stg_desc;
    while (fptr != NULL) {
        off_t stg_off_start = fptr->app_stg_start_fob * ADVFS_FOB_SZ;
        off_t stg_off_end = (fptr->app_stg_end_fob + 1) * ADVFS_FOB_SZ - 1;

        if (stg_off_end  > last_off_written) {
            /* Some or all of this storage was NOT overwritten
             * we must remove what ever was not written and
             * maybe zero out an allocation unit.
             */
            if (stg_off_start < last_off_written){
                /* Need to keep the allocation unit that the write
                 * ended on and zero it out.
                 */
                offset_to_remove = roundup(last_off_written,
                                           bfap->bfPageSz*ADVFS_FOB_SZ);
                bytes_to_zero = offset_to_remove - last_off_written;
            } else {
                /* This storage needs to be removed completely
                 */
                offset_to_remove = stg_off_start;
                bytes_to_zero = 0;
            }

            bytes_to_remove = (stg_off_end + 1) - offset_to_remove;

            if (bytes_to_remove > 0) {
                /* We need a transaction in order to remove storage.
                 */
                ADVRWL_MIGSTG_READ(bfap);
                
                sts = FTX_START_N (FTA_OSF_SETATTR_1,
                             &ftxH,
                             FtxNilFtxH,
                             bfap->dmnP
                    );
                
                if (sts != EOK) {
                    ADVFS_SAD1( "ftx start failed", sts );
                }

                sts = stg_remove_stg_start (bfap,
                                            offset_to_remove/ADVFS_FOB_SZ,
                                            bytes_to_remove/ADVFS_FOB_SZ,
                                            1,          /* do rel quotas */
                                            ftxH,
                                            &delCnt,
                                            &delList,
                                            TRUE,      /* do COW */
                                            /* force alloc of mcell in */
                                            TRUE,      /* bmt_alloc_mcell */
                                            0L);
                ADVRWL_MIGSTG_UNLOCK(bfap);
                
                if (sts != EOK) {
                    /* If we can't remove it then atleast zero it. */
                    ftx_fail(ftxH);
                    if (last_off_written > offset_to_remove) {
                        offset_to_zero =  last_off_written;
                        bytes_to_zero += bytes_to_remove;
                    } else {
                        offset_to_zero = offset_to_remove;
                        bytes_to_zero += bytes_to_remove;
                    }
                } else {
                    ftx_done_fs (ftxH, FTA_OSF_SETATTR_1);
                    if ( delCnt ) {
                        stg_remove_stg_finish( bfap->dmnP, delCnt, delList );
                    }
                }
            }
            if (bytes_to_zero > 0) {
                /* Some of the storage we added was written and
                 * did not end on an allocation unit boundary
                 * before we errored. We need to zero this storage
                 * since we can't remove it.
                 *
                 * Since we still have a mapping we'll use it.
                 */
                
                bzero((caddr_t)fcmap->fm_vaddr + offset_to_zero, 
                      bytes_to_zero);
            }
        }
        
        ptr=fptr;
        fptr=fptr->next;
        if (!first) {
            ms_free(ptr);
        } else {
            first = FALSE;
        }
    }
}


/*
 * advfs_fs_update_times
 * 
 * Used by CFS to tell AdvFS to update the mtime and ctime on a file
 * with delayed dirty writes. Also used by CFS concurrent direct I/O
 * to update mtime/ctime/atime appropriately.
 */
void
advfs_fs_update_times(
    struct vnode *vp,
    int           attr_flags)
{
    struct fsContext *contextp;
    contextp = VTOC(vp);

    ADVSMP_FILESTATS_LOCK( &contextp->fsContext_lock );
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
        if(!(contextp->fsc_fsnp->bfSetp->bfSetFlags & BFS_IM_NOATIMES) )  {
            contextp->dirty_stats = TRUE;
        }
    } else {
        contextp->dirty_stats = TRUE;
    }
    ADVSMP_FILESTATS_UNLOCK( &contextp->fsContext_lock );

}

/*
 * advfs_fs_write_direct()
 *
 * Description:
 *
 * Called by advfs_fs_write() for files opened for directIO, this routine
 * will transfer data from the application's buffer to disk (bypassing the
 * buffer cache).  This routine will handle all alignment issues.  The uio
 * structure contains all the information needed to do the transfer.
 *
 * If the transfer doesn't begin on a disk sector boundary, a disk
 * read of the entire disk sector will be performed using a temporary
 * buffer.  The requested data will then be copied to this buffer from
 * the application's buffer and then written to disk.  Similarly, any
 * transfer of less than a full sector in length will also require an
 * intermediate copy to a temporary buffer.
 *
 * This function should be called with the file lock held for either
 * shared access (normally) or for exclusive access (if IO_APPEND was
 * requested).
 *
 * Note that the uio fields are not modified as the routine progresses.
 * They are left alone until the routine finishes, and then they are
 * adjusted at the end to reflect the amount of work that has been done.
 * This is partly to aid in debugging and partly to make the code a bit
 * simpler.  The 3 req_xfer_xxx fields are established at the start of
 * the routine and are never modified, thereby giving a convenient
 * reference to the originally requested values.  The work_xfer_xxx
 * fields are used to maintain these same values but are constantly
 * adjusted to maintain the currently-applicable values.  Also note that
 * the req_xfer_size is set to MIN(uio->uio_iov->iov_len, uio->uio_resid)
 * in case advfs_fs_write() adjusted the resid value down because the
 * requested transfer would have exceeded some internal limit.
 *
 * Parameters:
 *
 *    bfAccessT  *bfap               (in)  Bitfile Access Pointer
 *    struct uio *uio                (in)  UIO Pointer
 *    uint64_t   *bytes_written      (out) Number of bytes actually written
 *    int         ioflag             (in)  flags - needed for IO_APPEND
 *    size_t     *vaslockpages_size  (in/out)
 *                                         Initial passed value must be 0.
 *                                         If advfs_fs_write_direct() returns
 *                                         E_PREGION_CROSSING_PLEASE_RETRY,
 *                                         then no I/O was done, because the
 *                                         first size tried crossing a pregion
 *                                         boundary and vaslockpages() will not
 *                                         handle that, so *vaslockpages_size
 *                                         will now contain the new size for
 *                                         the next attempt.  So a non-zero
 *                                         value specifies the reduced size to
 *                                         try.
 */
static statusT
advfs_fs_write_direct(bfAccessT  *bfap,
                      struct uio *uio,
                      size_t     *bytes_written,
                      int         ioflag,
                      int32_t     useAio,
                      size_t     *vaslockpages_size)
{
    struct buf        *aio_bp         = (struct buf *)NULL;
    struct iovec      *iov            = (struct iovec *)NULL;
    struct actRange   *arp            = NULL;
    struct fsContext  *contextp       = VTOC(&bfap->bfVnode);
    struct ioanchor   *ioap           = NULL;
    struct buf        *bp             = NULL;
    extent_blk_desc_t *storage_blkmap = NULL;
    extent_blk_desc_t *blkp           = NULL;
    vdT               *vdp            = NULL;
    anchr_aio_flags_t  wired_with     = ANCHR_AIO_NOFLAGS;
    int                app_pages_wired = FALSE;
    int                app_ftx_started = FALSE; /* transaction started */
    struct advfs_pvt_param pvt_param;
    off_t  saved_file_size;
    int    lock_held_exclusive;
    int    i;
    int    new_stg_leading_fobs = 0;
    int    new_stg_trailing_fobs = 0;
    int    xtntLockHeld = 0;
    int    file_lock_released = 0;
    int    dropped_file_lock = 0;
    int    blkmap_flags;
    int    asynch_io_was_started = 0;
    int    do_asynch = FALSE;
    statusT sts = 0;
    statusT status = EOK;
    statusT remembered_status = EOK;
    off_t   tmp_offset;
    bf_fob_t start_fob, end_fob;
    uint64_t no_of_maps = 0;
    uint64_t trailing_bytes_written = 0;
    uint64_t aligned_bytes_written = 0;
    uint64_t req_xfer_offset = uio->uio_offset;
    uint64_t req_xfer_size   = MIN(uio->uio_iov->iov_len, uio->uio_resid);
    caddr_t  req_xfer_base   = uio->uio_iov->iov_base;
    caddr_t  work_xfer_base  = req_xfer_base;
    uint64_t work_xfer_size;
    uint64_t work_xfer_offset= req_xfer_offset;
    uint64_t bytes_remaining;
    uint64_t aligned_xfer_offset;         /* start of act range in bytes */
    uint64_t aligned_xfer_size;           /* size  of act range in bytes */
    int      vaslockpages_flags;          /* needed by vasunlockpages() */
    vas_t   *vas             = NULL;
    int32_t  has_parent_snap = FALSE,
             new_stg_leading_fobs_only = FALSE;

    MS_SMP_ASSERT ( uio->uio_iov->iov_len != 0 );
    *bytes_written = 0;

    /* Did this request come from the AIO interface?  Save some addresses
     * from the uio for later.
     */
    if ( useAio ) {
        /* If called from AIO and the file is no longer open for
         * directIO, return an error.
         */
        if ( bfap->dioCnt == 0 )
            return EACCES;
        MS_SMP_ASSERT( uio->uio_iovcnt == 1 );
        iov = &uio->uio_iov[1];
        aio_bp = (struct buf *)uio->uio_iov[1].iov_base;
        do_asynch = TRUE;
    } 

    /* Check the input request size; if it exceeds an arbitrarily
     * large value, limit the request.  If there is no error, then
     * advfs_fs_write() will call back in until the whole request
     * has been handled.  This limit was suggested by VM to prevent
     * wiring down too many application pages simultaneously.
     */
    if ( req_xfer_size > advfs_max_dio_transfer_size ) {
        req_xfer_size   = advfs_max_dio_transfer_size;
        
        /* If this is an AIO request, then we will force all partial
         * I/Os generated by this routine (prior to the last one) 
         * to be done synchronously.  This serves the purposes of 
         * preventing: 1) too many pages from being wired concurrently, 
         * and 2) the AIO completion routine from being called more 
         * than once.
         */
        do_asynch = FALSE;
    }
    if ( *vaslockpages_size != 0 && req_xfer_size > *vaslockpages_size ) {

        /* A non-zero value for *vaslockpages_size means the caller tried this
         * I/O once before, but returned with E_PREGION_CROSSING_PLEASE_RETRY
         * and is trying the I/O again, only this time we are suppose to use
         * the *vaslockpages_size returned from the previous attempt.
         */
        req_xfer_size  = *vaslockpages_size;
        do_asynch = FALSE;
    }
    work_xfer_size  = req_xfer_size;
    bytes_remaining = req_xfer_size;

    /* seize the active range for sectors underlying this request to
     * prevent concurrent use by migrate, truncate, or other directIO
     * threads.
     */
    arp = advfs_bs_get_actRange(TRUE);
    arp->arState    = AR_DIRECTIO;

    /* If we are in IO_APPEND mode, then we need to take out an active
     * range now to protect the EOF value before we add storage.
     */
retry:
    if ( ioflag & IO_APPEND ) {
        saved_file_size = bfap->file_size;
        arp->arStartFob = ADVFS_AU_ROUND_DOWN(bfap, bfap->file_size);
        arp->arEndFob   = ~0L;
        arp->arDebug    = SET_LINE_AND_THREAD(__LINE__);
        dropped_file_lock = insert_actRange_onto_list( bfap, arp, contextp );

        if ( dropped_file_lock && bfap->file_size < saved_file_size ) {
            /* if EOF got smaller, then the active range doesn't cover
             * it so must go back and do it again.
             */
            spin_lock(&bfap->actRangeLock);
            remove_actRange_from_list(bfap, arp);
            arp->arDebug = NULL;         
            spin_unlock(&bfap->actRangeLock);
            goto retry;
        } 

        /* The EOF may change while we wait for the active range */
        uio->uio_offset = bfap->file_size;
        req_xfer_offset = work_xfer_offset = bfap->file_size;
    }

    /* Set up the advfs_pvt_param structure for storage addition or
     * potential snapshotting needs.  The APP_ADDSTG_NOCACHE flag is
     * an indicator to advfs_getpage() not to bring pages into storage
     * that are outside the requested range, to bring them into the
     * cache, or to try to zero-fill anything.  Snapshotting may bring
     * pages into the cache; this is OK so long as they are within the
     * active range, since they will be invalidated.  New storage
     * allocated beyond the range requested because of storage alloction
     * unit granularity will be detected and zero-filled later in this
     * routine.
     */
    pvt_param = Nil_advfs_pvt_param;
    pvt_param.app_total_bytes     = req_xfer_size;
    pvt_param.app_starting_offset = req_xfer_offset;
    pvt_param.app_flags           = APP_ADDSTG_NOCACHE;
    pvt_param.arp                 = arp;

    if ( HAS_SNAPSHOT_CHILD( bfap ) ) {
        /* 
         * It is possible we need to COW to a snapshot.
         * advfs_force_cow_and_unlink will be called to do this and it will
         * invalidate the pages when it's done.*
         */
        sts = advfs_force_cow_and_unlink( bfap, 
                                req_xfer_offset, 
                                req_xfer_size,
                                (ioflag & IO_APPEND) ? 
                                SF_NO_UNLINK|SF_INVAL_ALL|SF_DIRECT_IO_APPEND :
                                SF_NO_UNLINK|SF_INVAL_ALL,
                                FtxNilFtxH);
        /*
         * On error, the children should be out of sync, so just ignore it
         */
        if (sts != EOK) {
            SNAP_STATS( direct_write_force_cow_failed );
            sts = EOK;
        }
    }

    /*
     * Add storage by calling advfs_getpage directly.
     */
    sts = advfs_getpage( NULL, &bfap->bfVnode, (off_t *)&work_xfer_offset,
                             (size_t *)&work_xfer_size, FCF_DFLT_WRITE,
                             (uintptr_t)&pvt_param, 0 );

    if ( ! FTX_EQ(pvt_param.app_parent_ftx, FtxNilFtxH) ) {
        /* A transaction has been returned.  That means BFS_OD_OBJ_SAFETY
         * was in play when the storage was allocated, so Direct I/O must
         * write user data or zero all the allocated storage and then
         * ftx_done or fail the transaction.  Since we do not want to try
         * doing that with an async AIO, we will force this request to be
         * synchronous.  This is still better than being forced to first
         * synchronously write all zeros to the newly allocated storage and
         * then overwrite with user data asynchronously.
         */
        app_ftx_started = TRUE; /* transaction started */
        do_asynch = FALSE;
        /*
         * If small allocation units are being used, it is possible that
         * advfs_getpage() will only add storage to back the first
         * VM_PAGE_SZ bytes of the file on this first call.  If so,
         * we need to remember this so that we zero-fill this storage
         * but then report to the caller that we processed zero bytes
         * of the original request.
         */
        if (work_xfer_offset + work_xfer_size <= req_xfer_offset) {
            new_stg_leading_fobs_only = TRUE;
        }
    }
    if ( sts != EOK ) {
        if ( sts == ENOSPC || sts == E_DIO_OBJ_SAFETY_PARTIAL_ALLOC ) {
            statusT sts_space = sts;

            /* If we get ENOSPC or E_DIO_OBJ_SAFETY_PARTIAL_ALLOC, we may
             * have had only part of the storage allocated.  If this is the
             * case, we need to write as much as possible given the storage
             * that is currently allocated.
             */
            if (!(pvt_param.app_flags & APP_ADDEDSTG_NOCACHE)) {
               uint64_t xtnt_count = 0;
               uint64_t stg_bytes_available = 0;
               uint64_t rounding_diff = 0;

                MS_SMP_ASSERT( sts_space != E_DIO_OBJ_SAFETY_PARTIAL_ALLOC );

               /* No storage was able to be allocated.  Check to see if
                * there is any existing storage at the beginning of the
                * range that we can write anyway.
                */
                tmp_offset = ADVFS_ALIGN_OFFSET_TO_VDBLK_DOWN(req_xfer_offset);
                rounding_diff = req_xfer_offset - tmp_offset;
                sts = advfs_get_blkmap_in_range( bfap, bfap->xtnts.xtntMap,
                                             &tmp_offset,
                                             req_xfer_size + rounding_diff,
                                             &storage_blkmap,
                                             &xtnt_count, 
                                             RND_NONE,
                                             EXB_COMPLETE,
                                             XTNT_WAIT_OK);
                if ( xtnt_count ) {
                    /* See how much storage exists that we can write into */
                    blkp = storage_blkmap;
                    while ( blkp ) {
                        uint64_t blkno  = blkp->ebd_vd_blk;

                        if ( blkno == XTNT_TERM  || blkno == COWED_HOLE ) {
                            /* This is a hole; we are done */
                            break;
                        } else {
                            stg_bytes_available += blkp->ebd_byte_cnt;
                            blkp = blkp->ebd_next_desc;
                        }
                    }
                    advfs_free_blkmaps( &storage_blkmap );
                    storage_blkmap = NULL;
                    blkp = NULL;
                   
                    /* If there is some storage, then set req_xfer_size to 
                     * that amount and continue on.  Otherwise, we have 
                     * no storage that we can write to.
                     */
                    if ( stg_bytes_available ) {
                        req_xfer_size = work_xfer_size = bytes_remaining =
                            stg_bytes_available - (req_xfer_offset - tmp_offset);
                    } else {
                        /* no storage available; only blkmap was terminator */
                        status = ENOSPC;
                        goto cleanup;
                    }
                } else {
                    status = ENOSPC;
                    goto cleanup;
                }
            } else {
                /* We got less storage than we requested, and need to scale 
                 * down the amount of data that can be written. 
                 * The allocation failure will always occur at the end of the 
                 * request, but the value that is returned in work_xfer_size
                 * may include some rounding at the beginning of the request 
                 * as well as at the end, so we need to remove any such 
                 * adjustment at the beginning from the size that we can write.
                 */
                uint64_t adjusted_work_xfer_size = work_xfer_size;

                if ((work_xfer_offset < req_xfer_offset) &&
                    (req_xfer_offset < pvt_param.stg_desc.app_stg_start_fob*ADVFS_FOB_SZ) ){

                    /* adjust for leading rounding in work_xfer_size */
                    adjusted_work_xfer_size -= 
                                        (req_xfer_offset - work_xfer_offset);
                }
                /*
                 * If we actually got some storage in the range requested
                 * by the caller, truncate the remaining request to be
                 * that size.  Otherwise, if we got no storage at all in
                 * the range requested by the caller, truncate the remaining
                 * request size to 0.  The reason we might get no storage
                 * at all in the range requested by the caller is that we
                 * might have had to backfill some of the first VM_PAGE_SZ
                 * bytes of the file if we have a small allocation unit.
                 */
                if (work_xfer_offset + work_xfer_size > req_xfer_offset) {
                    req_xfer_size = 
                    bytes_remaining = adjusted_work_xfer_size;
                }
                else {
                    req_xfer_size = bytes_remaining = 0;
                }
            }

            /* Also, remember that we got an ENOSPC error so we can return 
             * this to advfs_fs_write() which will handle it appropriately.
             */
            if ( sts_space == ENOSPC ) {
                remembered_status = ENOSPC;
            }
            else {
                do_asynch = FALSE;
                MS_SMP_ASSERT( sts_space == E_DIO_OBJ_SAFETY_PARTIAL_ALLOC );
            }
        }
        else {             /* ! ENOSPC && ! E_DIO_OBJ_SAFETY_PARTIAL_ALLOC */
            status = sts;
            goto cleanup;
        }
    }

    /* update these in case getpage() modified them */
    work_xfer_offset = req_xfer_offset;
    work_xfer_size   = req_xfer_size;

    /* Check to see if storage was added before or after the actual
     * requested I/O range; this can happen because of storage allocation
     * unit granularity.  If the I/O request starts in the middle of the
     * first newly-allocated chunk, then the new storage that will not be
     * overwritten by application data must be zero'd on disk.  The same
     * is true at the end of the range.  We also check for data alignment
     * at this point since we will handle the zeroing of new storage and
     * merging unaligned data in the same subroutine.
     */
    if (pvt_param.app_flags & APP_ADDEDSTG_NOCACHE) {
        /* Storage has been added. Determine the largest span of fobs
         * that will include all new storage as well as the I/O requested.
         */
        start_fob = MIN(pvt_param.stg_desc.app_stg_start_fob,
                             ADVFS_OFF_TO_FIRST_FOB_IN_VDBLK(req_xfer_offset));
        end_fob   = MAX(pvt_param.stg_desc.app_stg_end_fob,
                             ADVFS_OFF_TO_LAST_FOB_IN_VDBLK(req_xfer_offset,
                                                            req_xfer_size));

        /* Check for leading storage and alignment */
        if (!(req_xfer_offset % DEV_BSIZE) &&
             (pvt_param.stg_desc.app_stg_start_fob >=
              ADVFS_OFFSET_TO_FOB_DOWN(req_xfer_offset))) {
            /* Application data aligns with storage AND the write starts
             * before the offset where the new storage was allocated; 
             * no zeroing needed.
             */
            new_stg_leading_fobs = 0;
        } else {
            /* We have an unaligned request OR the data to be written starts
             * at or above the newly-allocated storage.  See if we need to 
             * zero any fobs before the write, or if the write will completely
             * overwrite the new storage.
             */
            new_stg_leading_fobs = (ADVFS_OFFSET_TO_FOB_DOWN(req_xfer_offset) -
                                    pvt_param.stg_desc.app_stg_start_fob);
            if ( new_stg_leading_fobs < 0 ) {
                /* new stg is after the overwritten data; no stg to zero */
                new_stg_leading_fobs = 0;
            } else if ( req_xfer_offset % DEV_BSIZE ) {
                /* bump by 1 to include the sector that is being merged.
                 * This ensures that we merge the application data with
                 * zeros and write this to the newly-allocated sector 
                 * instead of merging the data with data on disk.
                 */
                new_stg_leading_fobs++;
            }
        }

        /* Now check for trailing storage and alignment */
        if ( !((req_xfer_offset+req_xfer_size)%DEV_BSIZE) &&
              (pvt_param.stg_desc.app_stg_end_fob <=
               ADVFS_OFFSET_TO_FOB_DOWN(req_xfer_offset + req_xfer_size - 1))) {
            /* Application data aligns with storage AND the write finishes
             * after the the new storage; no zeroing needed.
             */
            new_stg_trailing_fobs = 0;
        } else {
            /* We have an unaligned request OR the data will be written past
             * the newly-allocated storage.  See if we need to zero any fobs 
             * beyond the end of the data to be written, or if the write will 
             * completely overwrite the new storage.
             */
            new_stg_trailing_fobs = (pvt_param.stg_desc.app_stg_end_fob -
                ADVFS_OFFSET_TO_FOB_DOWN(req_xfer_offset + req_xfer_size - 1));
            if ( new_stg_trailing_fobs < 0 ) {
                /* overwriting data past new storage; no stg to zero */
                new_stg_trailing_fobs = 0;
            } else if ( (req_xfer_offset+req_xfer_size) % DEV_BSIZE ) {
                new_stg_trailing_fobs++;
            }
        }
    } else {
        /* no new storage was added */
        new_stg_leading_fobs = 0;
        new_stg_trailing_fobs = 0;
        start_fob = ADVFS_OFF_TO_FIRST_FOB_IN_VDBLK(req_xfer_offset);
        end_fob   = ADVFS_OFF_TO_LAST_FOB_IN_VDBLK(req_xfer_offset,
                                                   req_xfer_size);
    }

    /* We should have an active range.  Either it was setup because this is
     * an IO_APPEND, or advfs_getpage() set it up after figuring out if we
     * needed storage and made sure the range covered all allocated storage
     */
    MS_SMP_ASSERT( arp->arDebug );
    MS_SMP_ASSERT( arp->arStartFob <= start_fob );
    MS_SMP_ASSERT( arp->arEndFob   >=   end_fob );

    /* At this point, the variables new_stg_xxx_fobs will contain:
     *
     *  Value of new_stg_xxx_fobs          Meaning
     *  ------------------------- -------------------------------------
     *            0               no new storage was allocated, or the
     *                            application data will overwrite existing
     *                            data.  If there is unaligned data, that data
     *                            must be merged with data FROM THE DISK.
     *
     *           >0               The # of sectors that must be zero'd on disk.
     *                            If there is unaligned data to write as well,
     *                            that data must be merged into the zero'd 
     *                            buffer before the data is written to disk.
     *                            These sectors on disk will be totally 
     *                            overwritten.
     */


    /* Calculate the range (in bytes) of the file that will be modified.  */
    aligned_xfer_offset = start_fob * ADVFS_FOB_SZ;
    aligned_xfer_size = (1 + end_fob - start_fob) * ADVFS_FOB_SZ;


    /* ASSERT that there are no pages in the UFC for our active range */
    ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );

    /* We have an active range, so we can drop the file lock.  Remember
     * whether the file lock is held in shared or exclusive mode.
     */
    MS_SMP_ASSERT( arp->arDebug );
    lock_held_exclusive = ADVRWL_FILECONTEXT_ISWRLOCKED(contextp);
    ADVRWL_FILECONTEXT_UNLOCK( contextp );
    file_lock_released = 1;

    if (HAS_SNAPSHOT_PARENT( bfap ) ) {
        has_parent_snap = TRUE;
        ADVRWL_BFAP_SNAP_READ_LOCK( bfap );
    }


    /* If the write buffer is in user space (this is typical), then wire
     * the application buffer so the driver can DMA from it.  The truncate
     * path is one exception; it passes in a kernel address.  
     * If req_xfer_size is 0, perhaps because advfs_getpage() added storage
     * before the write request but didn't actually add any storage FOR
     * the write request, don't do this.  Example:  1k blocksize filesystem
     * with a 1-byte file in it will consume 1k of disk space.  If write
     * request for 1 byte at offset 4k comes in, advfs_getpage() will first
     * fill the hole at offset 1k for size 3k to avoid a hole of size 
     * less than VM_PAGE_SZ.  It will then go to allocate storage for the
     * write request at offset 4k but if object safety is being used,
     * advfs_getpage() is limited to one call to advfs_bs_add_stg() and
     * will return before adding that storage.  So advfs_getpage() will
     * return a *size of 0.  That will cause req_xfer_size to be set to 0,
     * too.  In this case, we'll just sigh heavily, zero-fill the 3k
     * it allocated, and return to our caller, who will come back in again
     * for the intended request.
     */
    if ((uio->uio_seg == UIOSEG_USER) && (req_xfer_size)) {

        /* These VM routines, when wiring the application buffers, will 
         * seize a pfdat lock on each VM page; these locks will not be
         * released until the pages are unwired.  Because this VM lock is
         * lower in the hierarchy than the AdvFS locks, we must seize the 
         * xtntmap lock now to avoid seizing it when we get xtnt maps 
         * later on, which would be a hierarchy violation.
         */
        if (has_parent_snap) {
            status = advfs_acquire_xtntMap_locks( bfap, 
                    XTNT_WAIT_OK|X_LOAD_REFERENCE,
                    SF_SNAP_NOFLAGS);
            if (status != EOK) {
                goto cleanup;
            }
        } else {
            ADVRWL_XTNTMAP_READ( &(bfap->xtntMap_lk) );
        }
        xtntLockHeld = 1;

        /* We are going to acquire the pfdatlock on the pages in the
         * transfer range.  Because of this _DO_ _NOT_ call any flavor of
         * copyin(), copyout(), or uiomove() as they may experience a
         * protection cache miss, which would cause a deadlock when our
         * thread attempted to get the pfdatlock while we are already
         * holding it.  Instead use advfs_dio_bzero(), advfs_dio_copyin(),
         * or advfs_dio_copyout() on the addresses in the transfer range.
         */
        sts = bufpin( ldusid(req_xfer_base), req_xfer_base, req_xfer_size,
                      B_WRITE );
        if ( sts == 0 ) {  /* bufpin returns 1 on success, else 0 */
            int ret;

            /*
             * Do we have read access to application buffer?
             */
            if ( !luseracc( ldusid(req_xfer_base), req_xfer_base,
                            req_xfer_size, B_READ )) {
                status = EFAULT;                /* no access rights */
                goto cleanup;
            }

            /* Because this I/O is a write to disk and we will only be reading
             * pages in memory, we do not need to worry about any Copy-on-Write
             * pages in the application buffer, hence we do _NOT_ pass the
             * BREAK_COW flag.
             *
             * ASYNCIO will get the pfdat lock on each page of the application
             * buffer to keep vhand from stealing the pages.  However, using
             * ASYNCIO, and depending on just pfdat, means that vaslockpages()
             * will have to loop on faulting any missing pages into memory.
             * This is because after any faulted page completes its I/O, there
             * is a window when the pfdat is cleared and vhand could steal the
             * page.  This might result in multiple I/O faults for the same
             * page.  This should be a very rare situation.
             *
             * We are going to use ASYNCIO regardless of whether this is an
             * AIO or synchronous Direct I/O.  Otherwise for synchronous Direct
             * I/O, using VM_IO_LOCK would require us to hold the vas lock over
             * the entire I/O so that we avoid a potential dead lock situation
             * with some other (non-AdvFS) thread grabbing the vas lock and
             * then stalling on trying to get the pg_lock on a page, while we
             * are trying to get the vas lock to release our pg_lock.  Plus it
             * is anticipated that the prime consumers for Direct I/O will also
             * be using the AIO interface, so ASYNCIO would need to be used
             * anyway.
             */
            *vaslockpages_size = req_xfer_size;
            vaslockpages_flags = ASYNCIO;
            vas = P_VAS(u.u_procp);
            vas_read_lock( vas );
            ret = vaslockpages( req_xfer_base, vaslockpages_size,
                                vaslockpages_flags, B_WRITE, FALSE, NULL );
            vas_unlock(vas);
            if (ret != 0) {
                status = ret;
                goto cleanup;
            }
            wired_with = ANCHR_AIO_VASLOCKPAGES;

            if ( *vaslockpages_size != req_xfer_size ) {

                /* vaslockpages() detected that the requested size crosses a
                 * pregion boundary, and vaslockpages() is not prepared to
                 * handle that.  So, vaslockpages() has modified
                 * *vaslockpages_size to be an acceptable length, that stays in
                 * the same pregion.  We return E_PREGION_CROSSING_PLEASE_RETRY
                 * status and the updated *vaslockpages_size to the caller,
                 * which is responsible for retrying the I/O, passing back the
                 * updated *vaslockpages_size and we will do the rest.  It is a
                 * bit of a kludge, but doing this avoids the lock hierarchy
                 * problems with trying to wire down the buffer sooner.
                 */
                MS_SMP_ASSERT( *vaslockpages_size < req_xfer_size );
                MS_SMP_ASSERT( *vaslockpages_size > 0 );
                app_pages_wired = TRUE; /* forces unwire during cleanup */
                status = E_PREGION_CROSSING_PLEASE_RETRY;
                goto cleanup;
            }
        } else {
            wired_with = ANCHR_AIO_BUFPIN;
        }
        app_pages_wired = TRUE;
    }

    /* If there is new leading storage that must be zero'd or an unaligned
     * leading transfer, do it now.
     */
    if (new_stg_leading_fobs || (req_xfer_offset % DEV_BSIZE) ) {
        uint64_t bytes_to_merge = 0;
        bf_fob_t extra_fobs_to_zero = 0;
        uint64_t xtnt_count = 0;

        /* calculate # bytes of application data that must be merged
         * if the transfer is not leading-aligned.
         */
        if ( req_xfer_offset % DEV_BSIZE ) {
            bytes_to_merge = MIN(req_xfer_size,
                                 DEV_BSIZE - (req_xfer_offset % DEV_BSIZE));
        }

        /* Code optimization.  If the total data transfer will be accomplished
         * within the leading-unaligned write, then do all zeroing of 
         * newly-allocated storage within this write. This differs from
         * the typical leading-unaligned scenario in that this case
         * requires zeroing before and after the data passed in.  The 
         * number of extra fobs to zero are passed into 
         * advfs_dio_unaligned_xfer() so it can allocate a larger buffer 
         * to account for the extra fobs.
         */
        if ( new_stg_leading_fobs && bytes_to_merge == req_xfer_size ) {
            bf_fob_t total_fobs = 1 + end_fob - start_fob;
            MS_SMP_ASSERT( total_fobs <= new_stg_leading_fobs + new_stg_trailing_fobs );
            extra_fobs_to_zero    = total_fobs - new_stg_leading_fobs;
        }

        /* Get the extent map for this region */
        tmp_offset = aligned_xfer_offset;
        blkmap_flags = XTNT_WAIT_OK | (xtntLockHeld ? XTNT_LOCKS_HELD : 0);
        sts = advfs_get_blkmap_in_range( bfap, bfap->xtnts.xtntMap,
                &tmp_offset, 
                (new_stg_leading_fobs + extra_fobs_to_zero) ? 
                  (((new_stg_leading_fobs + extra_fobs_to_zero)/
                     ADVFS_FOBS_PER_DEV_BSIZE) * DEV_BSIZE) : DEV_BSIZE,
                &storage_blkmap, 
                &xtnt_count, 
                RND_NONE, EXB_ONLY_STG,
                blkmap_flags );
        if ( sts != EOK ) {
            MS_SMP_ASSERT( sts != E_OFFSET_NOT_BLK_ALIGNED );
            status = sts;
            goto cleanup;
        }
        MS_SMP_ASSERT( xtnt_count > 0 );       /* there must be storage */
        MS_SMP_ASSERT( storage_blkmap->ebd_vd_index != 0 );

        /* This call will zero-fill any newly-allocated storage that will
         * not be overwritten, as well as merging unaligned data with the
         * underlying data on disk.
         */
        sts = advfs_dio_unaligned_xfer( bfap, req_xfer_base,
                                        uio->uio_seg,
                                        req_xfer_offset % DEV_BSIZE,
                                        bytes_to_merge,
                                        storage_blkmap,
                                        new_stg_leading_fobs,
                                        extra_fobs_to_zero,
                                        B_WRITE );
        if ( sts != EOK ) {
            status = sts;
            goto cleanup;
        }

        /* Account for transfer of bytes in leading unaligned section.
         * Note that we update these variable here, but not at the end
         * of the trailing unaligned section.  That is because the
         * trailing unaligned work is done out-of-order, and we will
         * adjust for that work after the aligned work has completed.
         */
        work_xfer_base += bytes_to_merge;
        work_xfer_size -= bytes_to_merge;
        bytes_remaining -= bytes_to_merge;
        *bytes_written = bytes_to_merge;
        advfs_free_blkmaps( &storage_blkmap );

        /* Since we did this IO in line, we must adjust aio_bp->b_resid
         * for the number of bytes transferred, since these will not be
         * tallied  as an aio completion in the IO completion path.
         */
        if (aio_bp)
            aio_bp->b_resid -= bytes_to_merge;
    }

    /*
     * If the leading unaligned data transfer satisfied the entire
     * request or if advfs_getpage() only added leading fobs, we're
     * done.
     */
    if ((!bytes_remaining) || (new_stg_leading_fobs_only)) {
        goto cleanup;
    }

    /* If there is new trailing storage that must be zero'd or an unaligned
     * trailing transfer, handle it now.
     */
    if (new_stg_trailing_fobs || ((req_xfer_offset+req_xfer_size)%DEV_BSIZE)){
        uint64_t trailing_nbytes = (req_xfer_offset+req_xfer_size) % DEV_BSIZE;
        uint64_t dist_to_trailing_sector = req_xfer_size - trailing_nbytes;

        /* Get the extent map for this sector */
        tmp_offset = ADVFS_ALIGN_OFFSET_TO_VDBLK_DOWN(req_xfer_offset +
                                                      req_xfer_size);
        blkmap_flags = XTNT_WAIT_OK | (xtntLockHeld ? XTNT_LOCKS_HELD : 0);
        sts = advfs_get_blkmap_in_range( bfap, bfap->xtnts.xtntMap,
                &tmp_offset, 
                new_stg_trailing_fobs ? 
                  ((new_stg_trailing_fobs/ADVFS_FOBS_PER_DEV_BSIZE) * DEV_BSIZE)
                  : DEV_BSIZE,
                &storage_blkmap, NULL, RND_NONE, EXB_ONLY_STG,
                blkmap_flags );
        if ( sts != EOK ) {
            MS_SMP_ASSERT( sts != E_OFFSET_NOT_BLK_ALIGNED );
            status = sts;
            goto cleanup;
        }
        MS_SMP_ASSERT( storage_blkmap->ebd_vd_index != 0 );

        /* This call will zero-fill any newly-allocated storage that will
         * not be overwritten, as well as merging unaligned data with the
         * underlying data on disk.
         */
        sts = advfs_dio_unaligned_xfer(bfap,
                                       req_xfer_base + dist_to_trailing_sector,
                                       uio->uio_seg,
                                       0UL,
                                       trailing_nbytes,
                                       storage_blkmap,
                                       new_stg_trailing_fobs,
                                       0UL, B_WRITE );
        /* Even if there is an error in the trailing request, we
         * need to continue on to try any aligned transfer.
         */
        if ( sts == EOK ) {
            trailing_bytes_written = trailing_nbytes;

            /* Since we did this IO in line, we must adjust aio_bp->b_resid
             * for the number of bytes transferred, since these will not be
             * tallied  as an aio completion in the IO completion path.
             */
            if (aio_bp)
                aio_bp->b_resid -= trailing_nbytes;
        }
        bytes_remaining -= trailing_nbytes;  /* account for even on failure */
        advfs_free_blkmaps( &storage_blkmap );
    }

    if (!bytes_remaining)
        goto cleanup;

    /* Adjust aligned values for any unaligned transfers already done. 
     * Until now, the aligned_xfer_* variables have described the region
     * covered by the active range.  We now want to restrict them to 
     * cover the range that will be used for the aligned transfer of
     * data only.  This means adjusting for both leading and trailing
     * storage that was zero'd or unaligned transfers that were already
     * performed. 
     */
    if ( *bytes_written || new_stg_leading_fobs ) {
        uint64_t adj_bytes = new_stg_leading_fobs ?
            ((new_stg_leading_fobs / ADVFS_FOBS_PER_DEV_BSIZE) * DEV_BSIZE) :
            DEV_BSIZE;
        aligned_xfer_offset += adj_bytes;
        aligned_xfer_size   -= adj_bytes;
    }
    if (trailing_bytes_written || new_stg_trailing_fobs ) {
        aligned_xfer_size -= new_stg_trailing_fobs ?
            ((new_stg_trailing_fobs / ADVFS_FOBS_PER_DEV_BSIZE) * DEV_BSIZE) :
            DEV_BSIZE;
    }

    /* Optimistically set this variable; will be modified if the aligned
     * I/O fails.
     */
    aligned_bytes_written = bytes_remaining;

    /* Handling of aligned transfers starts here.  Generate a single
     * I/O for each extent map within the region of the aligned request.
     * If any extent is larger than the preferred transfer size, break
     * it down into a series of I/Os that are each of the preferred
     * transfer size.
     */
    ioap = advfs_bs_get_ioanchor( M_WAITOK );
    ioap->anchr_iocounter   = 1;
    ioap->anchr_flags       = IOANCHORFLG_DIRECTIO;
    ioap->anchr_flags      |= (do_asynch) ? 0   : (IOANCHORFLG_KEEP_ANCHOR | 
                                                   IOANCHORFLG_WAKEUP_ON_LAST_IO);
    ioap->anchr_aio_bp      = (do_asynch) ? aio_bp : NULL;
    ioap->anchr_io_status   = 0;
    ioap->anchr_min_req_offset = aligned_xfer_offset;
    ioap->anchr_min_err_offset = aligned_xfer_offset + aligned_xfer_size;
    if ( do_asynch ) {
        MS_SMP_ASSERT( aio_bp );
        ioap->anchr_aio_info.anchr_actrangep   = arp;
        ioap->anchr_aio_info.anchr_aio_sid     = (uio->uio_seg == UIOSEG_USER)
                                               ? ldusid(req_xfer_base)
                                               : ldsid(req_xfer_base);
        ioap->anchr_aio_info.anchr_aio_address = req_xfer_base;
        ioap->anchr_aio_info.anchr_aio_size    = req_xfer_size;
        ioap->anchr_aio_info.anchr_aio_flags  |= wired_with;
    }

    /* Get the storage extent maps for the file */
    tmp_offset = aligned_xfer_offset;
    blkmap_flags = XTNT_WAIT_OK | (xtntLockHeld ? XTNT_LOCKS_HELD : 0);
    sts = advfs_get_blkmap_in_range( bfap, bfap->xtnts.xtntMap,
                          &tmp_offset, aligned_xfer_size,
                          &storage_blkmap, NULL, RND_NONE, EXB_ONLY_STG,
                          blkmap_flags );
    if ( sts != EOK ) {
        MS_SMP_ASSERT( sts != E_OFFSET_NOT_BLK_ALIGNED );
        status = sts;
        goto cleanup;
    }

    /* Walk through the extent maps, generating a buf struct for each I/O */
    blkp = storage_blkmap;
    while ( blkp ) {
        uint64_t nbytes = MIN( req_xfer_size, blkp->ebd_byte_cnt );
        uint64_t blkno  = blkp->ebd_vd_blk;
        int doing_sub_extent = 0;

        /* If we already built a buf structure (on a previous pass
         * through this loop), start I/O on it before building the 
         * next one.  Bump iocounter to keep it above 0.
         */
        if ( bp ) {
            spin_lock(&ioap->anchr_lock);
            ioap->anchr_iocounter++;
            spin_unlock(&ioap->anchr_lock);
            if ( do_asynch ) 
                ioap->anchr_aio_info.anchr_aio_iosize += bp->b_bcount;
            advfs_bs_startio( bp, ioap, NULL, bfap, vdp, 0 );
            bp = NULL;
        }

        /* Get preferred transfer size for disk. If the blkmap size 
         * exceeds the preferred xfer size, we  will build several buf 
         * structs and do several I/Os to transfer data within this 
         * single extent.
         */
        vdp = VD_HTOP( blkp->ebd_vd_index, bfap->dmnP );
        if ( nbytes > vdp->current_iosize_wr) {
            nbytes = vdp->current_iosize_wr;
            doing_sub_extent = 1;
        }

        /* allocate a buf struct for this I/O and initialize it */
        bp = advfs_bs_get_buf( TRUE, TRUE);
        bp->b_un.b_addr = (caddr_t)( work_xfer_base ); /* adjusted each pass */
        bp->b_spaddr    = (uio->uio_seg == UIOSEG_USER)
                        ? ldusid(work_xfer_base)
                        : ldsid(work_xfer_base);
        bp->b_blkno     = blkno;
        bp->b_foffset   = work_xfer_offset;
        bp->b_bcount    = nbytes;
        bp->b_vp        = &bfap->bfVnode;
        bp->b_proc      = u.u_procp;
        BSETFLAGS(bp,(bufflags_t)(B_WRITE | B_RAW | B_PHYS));
        BSETFLAGS(bp,(bufflags_t)((do_asynch) ? B_ASYNC : B_SYNC));

        /* Now account for this transfer and set up for the next */
        work_xfer_offset += nbytes;
        work_xfer_base   += nbytes;
        work_xfer_size   -= nbytes;
        bytes_remaining  -= nbytes;

        if ( !bytes_remaining ) {
            break;
        } else if ( doing_sub_extent  && (blkp->ebd_byte_cnt - nbytes != 0) ) {
            /* We set up for a partial transfer for this extent map.
             * If there is more to transfer in this extent, fix up extent
             * info to do next section.
             */
            MS_SMP_ASSERT( (int64_t)blkp->ebd_byte_cnt - (int64_t)nbytes >= 0 );
            blkp->ebd_byte_cnt -= nbytes;
            blkp->ebd_vd_blk += ADVFS_BYTES_TO_VDBLKS(nbytes);
        } else {
            /* go to next extent map */
            blkp = blkp->ebd_next_desc;
        }
    }

    /* Send the last (possibly only) I/O on its way, if there is one.
     * Don't bump ioAnchor counter here since it was artificially
     * bumped by one when initialized.  Then wait for the I/O completion
     * if not doing asynchronous I/Os.
     */
    if ( bp ) {

        /* If starting asynchronous I/Os, disown the cacheModeLock.  
         * This will be claimed and released in advfs_bs_io_complete()
         * after the final I/O for the ioAnchor has completed. Also
         * set reason for doing a partial write in aio_bp.
         */
        if ( do_asynch ) {
            /* Since this Direct I/O will be completed asynchronously, now
             * is the best time for us to ASSERT that the active range has
             * no pages in the UFC.  After the upcoming start I/O, this
             * request and the arp will be out of our control.
             */
            ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );

            ioap->anchr_aio_info.anchr_aio_iosize += bp->b_bcount;
            ioap->anchr_aio_info.anchr_lock_key =
                                              ADVRWL_BFAPCACHEMODE_DISOWN(bfap);
            if ( remembered_status )
                aio_bp->b_error = remembered_status;
        }

        advfs_bs_startio( bp, ioap, NULL, bfap, vdp, 0 );

        if ( do_asynch ) {
            /* This is a marker to upper levels that we started an 
             * asynchronous I/O.
             */
            iov->iov_len = 1;
            asynch_io_was_started = 1;
        } else {
            /* Not in asynchronous mode: wait for the I/Os to complete.
             * Avoid calling cv_wait() after cv_broadcast() has already 
             * been issued during IO completion. 
             */
            spin_lock(&ioap->anchr_lock);
            if ( ioap->anchr_flags & IOANCHORFLG_IODONE) {
                spin_unlock(&ioap->anchr_lock);
            } else {
                /* One of these better be set or your not waking up !*/
                MS_SMP_ASSERT(
                   (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) ||
                   (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_LAST_IO));
                cv_wait(&ioap->anchr_cvwait, &ioap->anchr_lock, CV_SPIN,
                        CV_NO_RELOCK);
            }
            MS_SMP_ASSERT(ioap->anchr_flags & IOANCHORFLG_IODONE);

            /* Now check the status of the IO */
            if ( ioap->anchr_io_status != EOK ) {
                status = ioap->anchr_io_status;
                aligned_bytes_written  = ioap->anchr_min_err_offset;
                trailing_bytes_written = 0;    /* These can't be counted */
            }

            /* Account for the bytes transferred in the aio_bp structure
             * if we did a synchronous IO as part of an AIO request.
             * Normally this is done in the IO completion path when the
             * request is handled asynchronously.
             */
            if ( aio_bp ) 
                aio_bp->b_resid -= aligned_bytes_written;
        }
    }

cleanup:
    /* Calculate how much was written.
     */
    *bytes_written += aligned_bytes_written + trailing_bytes_written;

    /* Release the xtntmap lock before checking to see if we wrote all of
     * the storage allocated, otherwise we may need to ftx_fail() an
     * BFS_OD_OBJ_SAFETY transaction and we can not do that while holding
     * the xtntmap lock.
     */
    if ( xtntLockHeld ) {
        if (has_parent_snap) {
            (void)advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
        } else { 
            ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));
        }
    } 

    /* BFS_OD_OBJ_SAFETY is enabled, so we have been given the job of
     * finishing the storage allocation transaction, or failing it.
     */
    if ( app_ftx_started ) {
        MS_SMP_ASSERT( ! do_asynch );
        MS_SMP_ASSERT( ! FTX_EQ(pvt_param.app_parent_ftx, FtxNilFtxH) );

        if ( status != EOK && 
             *bytes_written != req_xfer_size ) {

            /* Something went wrong, and we do not trust that all of the new
             * newly allocated storage was properly initialized, so we fail
             * the transaction and do not keep any of the storage.
             */
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_483);
            ftx_fail( pvt_param.app_parent_ftx );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_484);
            *bytes_written = 0;
        }
        else {
            /* Yea!  All the allocated storage was written, and it is safe
             * to keep this new storage under the BFS_OD_OBJ_SAFETY rules.
             */
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_485);
            ftx_done_n( pvt_param.app_parent_ftx, FTA_FS_WRITE_ADD_STG_V1 );
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_486);
        }
    }

    /* Adjust uio fields for successfully transferred bytes.  Make the
     * uio struct look like it would if we simply iterated thru uiomove().
     *
     * If we needed to fail a BFS_OD_OBJ_SAFETY storage allocation
     * transaction, then *bytes_written will be zero (see above).
     */
    uio->uio_iov->iov_base += *bytes_written;
    uio->uio_iov->iov_len  -= *bytes_written;
    uio->uio_offset        += *bytes_written;
    uio->uio_resid         -= *bytes_written;

    /* handle transfers with multiple uio->uio_iov vectors */
    if ( (uio->uio_iovcnt > 1) && (uio->uio_iov->iov_len == 0) ) {
        uio->uio_iov++;
        uio->uio_iovcnt--;
    }

    if (has_parent_snap) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
    }


    /* unwire application pages if needed. Do before reseize of file lock
     * because pfdat locks could be held, which would cause a deadlock
     * with reseizing the file lock.  If we started an asynchronous write, 
     * these pages will be released upon IO completion.
     */
    if ( app_pages_wired && !asynch_io_was_started ) {
        if ( wired_with & ANCHR_AIO_BUFPIN ) {
            bufunpin( ldusid(req_xfer_base), req_xfer_base,
                      req_xfer_size, B_WRITE);
        } 
        else if ( wired_with & ANCHR_AIO_VASLOCKPAGES ) {
            vasunlockpages( req_xfer_base, *vaslockpages_size,
                            vaslockpages_flags, 
                            ldusid(req_xfer_base), NULL, B_WRITE );
        }
    }

    /* reseize the file lock in appropriate mode */
    if ( file_lock_released ) {
        if ( lock_held_exclusive ) {
            ADVRWL_FILECONTEXT_WRITE_LOCK(contextp);
        }
        else {
            ADVRWL_FILECONTEXT_READ_LOCK(contextp);
        }
    }

    /* Remove active range and ioanchor if no asynchronous I/Os were
     * started. These all get cleaned up in the I/O completion path
     * for asynchronous I/Os.
     */
    if ( (status != EOK) || !asynch_io_was_started ) {
        if (aio_bp) {
            MS_SMP_ASSERT( iov->iov_len == 0 );
        }
        if ( arp->arDebug ) {                /* active range was seized */
            /* Before we drop the active range, ASSERT that none of the
             * pages in this range have showed up unexpectedly in the UFC.
             */
            ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );

            spin_lock(&bfap->actRangeLock);
            remove_actRange_from_list(bfap, arp);
            spin_unlock(&bfap->actRangeLock);
        }
        advfs_bs_free_actRange( arp, TRUE );
        if ( ioap ) {
            advfs_bs_free_ioanchor( ioap, M_WAITOK );
        }
    }

    /* free resources allocated locally */
    if ( storage_blkmap ) {
        (void)advfs_free_blkmaps( &storage_blkmap );
    }

    return( remembered_status != EOK ? remembered_status : status);
}


/*
 * advfs_dio_bzero()
 *
 * The original intended use is for Direct I/O to zero the parts of an I/O
 * buffer that correspond with sparse holes in a read request.
 *
 * It is not intended as a general bzero() replacement.
 *
 * No spin locks may be held when using this routine as it may need to fault
 * in user buffers and that could cause the thread to sleep.
 *
 * If the buffer is in user space, then bufpin() or vaslockpages() must have
 * been called before calling this routine.
 *
 * It is assumed that prior to pinning the users pages into memory, bufpin()
 * or luseracc() was invoked to check the users access rights to the page.
 *
 * NOTE:  If for some reason there are problems using privlbzero() do not
 *        replace this code with a copyout() of a buffer full of zeros.  The
 *        vaslockpages() call will have set the pfdatlock on the user's
 *        buffer and if copyout() has a protection cache miss, it will try
 *        to get the pfdatlock itself, and this will cause a deadlock.
 */
static void
advfs_dio_bzero( void    *to_addr,      /* in - address in I/O buffer */
                 size_t   size,         /* in - bytes to zero */
                 int32_t  uio_seg)      /* in - UIOSEG_USER or other.
                                         *      where other is assumed to be
                                         *      kernel address space, but we
                                         *      will treat anything not
                                         *      UIOSEG_USER as kernel
                                         */
{
    int     sts;
    space_t usid;

    /*
     * We might sleep, so we better not be holding any spinlocks.
     */
    MS_SMP_ASSERT( !spinlocks_held() );

    if ( size == 0 ) {
        return;              /* that was easy */
    }

    /*
     * See if the buffer is actually kernel address space.
     */
    if ( u.u_kthreadp->kt_copycheck_bypass || uio_seg != UIOSEG_USER ) {
        bzero( to_addr, size );
        return;
    } 

    usid = ldusid( to_addr );

    /*
     * By the time we reach here, the buffer should only be user address
     * space.  It is assumed that the user's rights to access this memory
     * was checked by bufpin() or luseracc() in advfs_fs_write_direct() or
     * advfs_fs_read_direct().  If you did not get here via that path, then
     * this routine is unsafe for you to use.
     */
    MS_SMP_ASSERT( luseracc( usid, to_addr, size, B_WRITE ) );
    
    /*
     * Zero the specified range in the user buffer.
     */
    privlbzero( usid, to_addr, size );

    return;

}


/*
 * advfs_dio_copyin()
 *
 * The original intended use is for Direct I/O to copy unaligned parts of
 * the write buffer into a kernel work area so that an aligned I/O can be
 * performed.
 *
 * It is not intended as a general copyin() replacement.
 *
 * No spin locks may be held when using this routine as it may need to fault
 * in user buffers and that could cause the thread to sleep.
 *
 * If the buffer is in user space, then bufpin() or vaslockpages() must have
 * been called before calling this routine.
 *
 * It is assumed that prior to pinning the users pages into memory, bufpin()
 * or luseracc() was invoked to check the users access rights to the page.
 *
 * NOTE:  If for some reason there are problems using privlbcopy() do not
 *        replace this code with a copyin().  The vaslockpages() call will
 *        have set the pfdatlock on the user's buffer and if copyin() has
 *        a protection cache miss, it will try to get the pfdatlock itself,
 *        and this will cause a deadlock.
 */
static int
advfs_dio_copyin(void    *from_addr,    /* in - address in I/O buffer */
                 void    *to_addr,      /* in - kernel destination address */
                 size_t   size,         /* in - length in bytes */
                 int32_t  uio_seg)      /* in - UIOSEG_USER or other.
                                         *      where other is assumed to be
                                         *      kernel address space, but we
                                         *      will treat anything not
                                         *      UIOSEG_USER as kernel
                                         */
{
    int     sts = EOK;
    space_t usid;

    /*
     * We might sleep, so we better not be holding any spinlocks.
     */
    MS_SMP_ASSERT( !spinlocks_held() );

    if ( size == 0 ) {
        return(EOK);              /* that was easy */
    }

    if ( u.u_kthreadp->kt_copycheck_bypass || uio_seg != UIOSEG_USER ) {
        /*
         * Kernel buffer
         */
        bcopy( from_addr, to_addr, size );
        return(EOK);
    } 

    /*
     * By the time we reach here, the buffer should only be user address
     * space.  It is assumed that the user's rights to access this memory
     * was checked by bufpin() or luseracc() in advfs_fs_write_direct() or
     * advfs_fs_read_direct().  If you did not get here via that path, then
     * this routine is unsafe for you to use.
     */
    usid = ldusid( from_addr );
    MS_SMP_ASSERT( luseracc( usid, from_addr, size, B_READ ) );

    sts = privlbcopy( usid, from_addr, ldsid(to_addr), to_addr, size );
    MS_SMP_ASSERT( sts == EOK );

    return(sts);
}


/*
 * advfs_dio_copyout()
 *
 * The original intended use is for Direct I/O to copy unaligned parts of
 * the read request from a kernel work area to the I/O buffer.
 *
 * It is not intended as a general copyout() replacement.
 *
 * No spin locks may be held when using this routine as it may need to fault
 * in user buffers and that could cause the thread to sleep.
 *
 * If the buffer is in user space, then bufpin() or vaslockpages() must have
 * been called before calling this routine.
 *
 * It is assumed that prior to pinning the users pages into memory, bufpin()
 * or luseracc() was invoked to check the users access rights to the page.
 *
 * NOTE:  If for some reason there are problems using privlbcopy() do not
 *        replace this code with a copyout().  The vaslockpages() call will
 *        have set the pfdatlock on the user's buffer and if copyout() has
 *        a protection cache miss, it will try to get the pfdatlock itself,
 *        and this will cause a deadlock.
 */
static int
advfs_dio_copyout(void    *from_addr,   /* in - kernel source address */
                  void    *to_addr,     /* in - address in I/O buffer */
                  size_t   size,        /* in - length in bytes */
                  int32_t  uio_seg)     /* in - UIOSEG_USER or other.
                                         *      where other is assumed to be
                                         *      kernel address space, but we
                                         *      will treat anything not
                                         *      UIOSEG_USER as kernel
                                         */
{
    int     sts = EOK;
    space_t usid;

    /*
     * We might sleep, so we better not be holding any spinlocks.
     */
    MS_SMP_ASSERT( !spinlocks_held() );

    if ( size == 0 ) {
        return(EOK);              /* that was easy */
    }

    if ( u.u_kthreadp->kt_copycheck_bypass || uio_seg != UIOSEG_USER ) {
        /*
         * Kernel buffer
         */
        bcopy( from_addr, to_addr, size );
        return(EOK);
    } 

    /*
     * By the time we reach here, the buffer should only be user address
     * space.  It is assumed that the user's rights to access this memory
     * was checked by bufpin() or luseracc() in advfs_fs_write_direct() or
     * advfs_fs_read_direct().  If you did not get here via that path, then
     * this routine is unsafe for you to use.
     */
    usid = ldusid( to_addr );
    MS_SMP_ASSERT( luseracc( usid, to_addr, size, B_WRITE ) );

    sts = privlbcopy( ldsid(from_addr), from_addr, usid, to_addr, size );
    MS_SMP_ASSERT( sts == EOK );

    return(sts);
}


/*
 * advfs_fs_read_direct()
 *
 * Description:
 *
 * Called by advfs_fs_read() for files opened for directIO, this routine
 * will transfer data from disk to the application's buffer (bypassing the
 * buffer cache).  This routine will handle all alignment issues.  The uio
 * structure contains all the information needed to do the transfer.
 *
 * If the transfer doesn't begin on a disk sector boundary, a disk
 * read of the entire disk sector will be performed using a temporary
 * buffer.  The requested data will then be copied to the application's
 * buffer.  Similarly, any transfer of less than a full sector in length will
 * also require an intermediate copy to a temporary buffer.
 *
 * This function should be called with the file lock held for shared access.
 *
 * Parameters:
 *
 *    bfAccessT *bfap                    (in)  Bitfile Access Pointer
 *    struct uio *uio                    (in)  UIO Pointer
 *    uint64_t *bytes_read               (in)  Number bytes requested to read
 *                                       (out) Number bytes actually read
 *    size_t   *vaslockpages_size        (in/out)
 *                                             Initial passed value must be 0.
 *                                             If advfs_fs_read_direct()
 *                                             returns
 *                                             E_PREGION_CROSSING_PLEASE_RETRY,
 *                                             then no I/O was done, because
 *                                             the first size tried crossing a
 *                                             pregion boundary and
 *                                             vaslockpages() will not handle
 *                                             that, so *vaslockpages_size will
 *                                             now contain the new size for the
 *                                             next attempt.  So a non-zero
 *                                             value specifies the reduced size
 *                                             to try.
 */
static statusT
advfs_fs_read_direct(bfAccessT *bfap,
               struct uio *uio,
               size_t *bytes_read,
               int32_t useAio,
               size_t *vaslockpages_size)
{
    struct buf        *aio_bp         = (struct buf *)NULL;
    struct iovec      *iov            = (struct iovec *)NULL;
    struct actRange   *arp            = NULL;
    struct fsContext  *contextp       = VTOC(&bfap->bfVnode);
    struct ioanchor   *ioap           = NULL;
    struct buf        *bp             = NULL;
    extent_blk_desc_t *storage_blkmap = NULL;
    extent_blk_desc_t *blkp           = NULL;
    vdT               *vdp            = NULL;
    statusT            status         = EOK;
    anchr_aio_flags_t  wired_with     = ANCHR_AIO_NOFLAGS;
    int                app_pages_wired = FALSE;
    int    i;
    int    sts = 0;
    int    xtntLockHeld = 0;
    int32_t     has_parent_snap = FALSE;
    int    blkmap_flags;
    int    asynch_io_was_started = 0;
    int    do_asynch = FALSE;
    off_t tmp_offset;
    uint64_t no_of_maps = 0;
    uint64_t trailing_bytes_read = 0;
    uint64_t aligned_bytes_read = 0;
    uint64_t req_xfer_offset = uio->uio_offset;
    uint64_t req_xfer_size   = MIN(uio->uio_iov->iov_len, 
                                   *bytes_read);
    caddr_t  req_xfer_base   = uio->uio_iov->iov_base;
    uint64_t work_xfer_size;
    caddr_t  work_xfer_base  = req_xfer_base;
    uint64_t work_xfer_offset= req_xfer_offset;
    uint64_t bytes_remaining;
    uint64_t aligned_xfer_offset;         /* start of act range in bytes */
    uint64_t aligned_xfer_size;           /* size  of act range in bytes */
    int      vaslockpages_flags;          /* needed by vasunlockpages() */
    vas_t   *vas             = NULL;

    MS_SMP_ASSERT ( uio->uio_iov->iov_len != 0 );
    *bytes_read = 0;

    /* Did this request come from the AIO interface?  Save some addresses
     * from the uio for later.
     */
    if ( useAio ) {
        /* If called from AIO and the file is no longer open for
         * directIO, return an error.
         */
        if ( bfap->dioCnt == 0 )
            return EACCES;
        MS_SMP_ASSERT( uio->uio_iovcnt == 1 );
        iov = &uio->uio_iov[1];
        aio_bp = (struct buf *)uio->uio_iov[1].iov_base;
        do_asynch = TRUE;
    }

    /* Check the input request size; if it exceeds an arbitrarily
     * large value, limit the request.  If there is no error, then
     * advfs_fs_read() will call back in until the whole request
     * has been handled.  This limit was suggested by VM to prevent
     * wiring down too many application pages simultaneously.
     */
    if ( req_xfer_size > advfs_max_dio_transfer_size ) {
        req_xfer_size   = advfs_max_dio_transfer_size;
        
        /* If this is an AIO request, then we will force all partial
         * I/Os generated by this routine (prior to the last one) 
         * to be done synchronously.  This serves the purposes of 
         * preventing: 1) too many pages from being wired concurrently, 
         * and 2) the AIO completion routine from being called more 
         * than once.
         */
        do_asynch = FALSE;
    }
    if ( *vaslockpages_size != 0 && req_xfer_size > *vaslockpages_size ) {

        /* A non-zero value for *vaslockpages_size means the caller tried this
         * I/O once before, but returned with E_PREGION_CROSSING_PLEASE_RETRY
         * and is trying the I/O again, only this time we are suppose to use
         * the *vaslockpages_size returned from the previous attempt.
         */
        req_xfer_size  = *vaslockpages_size;
        do_asynch = FALSE;
    }
    work_xfer_size  = req_xfer_size;
    bytes_remaining = req_xfer_size;

    /* seize the active range for sectors underlying this request to
     * prevent concurrent use by migrate, truncate, or other directIO
     * threads.
     */
    arp = advfs_bs_get_actRange(TRUE);
    arp->arStartFob = ADVFS_OFF_TO_FIRST_FOB_IN_VDBLK(req_xfer_offset);
    arp->arEndFob   = ADVFS_OFF_TO_LAST_FOB_IN_VDBLK(req_xfer_offset,
                                                     req_xfer_size);
    arp->arState    = AR_DIRECTIO;
    arp->arDebug    = SET_LINE_AND_THREAD(__LINE__);
    insert_actRange_onto_list( bfap, arp, contextp );   /* may sleep */

    aligned_xfer_offset = arp->arStartFob * ADVFS_FOB_SZ;
    aligned_xfer_size   = (1 + arp->arEndFob - arp->arStartFob) * ADVFS_FOB_SZ;

    /* ASSERT that there are no pages in the UFC for our active range */
    ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );

    /* It is now safe to drop the file lock */
    ADVRWL_FILECONTEXT_UNLOCK( contextp );

    if (HAS_SNAPSHOT_PARENT( bfap ) ) {
        has_parent_snap = TRUE;
        ADVRWL_BFAP_SNAP_READ_LOCK( bfap );
    }


    /* If the read buffer is in user space (this is typical), then wire
     * the application buffer so the driver can DMA into it.
     */
    if ( uio->uio_seg == UIOSEG_USER ) {

        /* These VM routines, when wiring the application buffers, will 
         * seize a pfdat lock on each VM page; these locks will not be
         * released until the pages are unwired.  Because this VM lock is
         * lower in the hierarchy than the AdvFS locks, we must seize the 
         * xtntmap lock now to avoid seizing it when we get xtnt maps 
         * later on, which would be a hierarchy violation.
         */
        if (has_parent_snap) {
            status = advfs_acquire_xtntMap_locks( bfap, 
                    XTNT_WAIT_OK|X_LOAD_REFERENCE,
                    SF_SNAP_NOFLAGS);
            if (status != EOK) {
                goto cleanup;
            }
        } else {
            ADVRWL_XTNTMAP_READ( &(bfap->xtntMap_lk) );
        }
        xtntLockHeld = 1;

        /* We are going to acquire the pfdatlock on the pages in the
         * transfer range.  Because of this _DO_ _NOT_ call any flavor of
         * copyin(), copyout(), or uiomove() as they may experience a
         * protection cache miss, which would cause a deadlock when our
         * thread attempted to get the pfdatlock while we are already
         * holding it.  Instead use advfs_dio_bzero(), advfs_dio_copyin(),
         * or advfs_dio_copyout() on the addresses in the transfer range.
         */
        sts = bufpin( ldusid(req_xfer_base), req_xfer_base, req_xfer_size,
                      B_READ );
        if ( sts == 0 ) {
            int ret;

            /*
             * Do we have write access to application buffer?
             */
            if ( !luseracc( ldusid(req_xfer_base), req_xfer_base,
                            req_xfer_size, B_WRITE )) {
                status = EFAULT;                /* no access rights */
                goto cleanup;
            }

            /* Because this I/O is a read from disk, this means we are going to
             * be modifying memory.  So we need to make sure that if any of the
             * pages to be modified are marked Copy-on-Write, that the COW is
             * broken before starting the I/O.  This is why we pass BREAK_COW.
             * BREAK_COW is not needed when writing to disk, as that only
             * requires reading memory.
             *
             * ASYNCIO will get the pfdat lock on each page of the application
             * buffer to keep vhand from stealing the pages.  However, using
             * ASYNCIO, and depending on just pfdat, means that vaslockpages()
             * will have to loop on faulting any missing pages into memory.
             * This is because after any faulted page completes its I/O, there
             * is a window when the pfdat is cleared and vhand could steal the
             * page.  This might result in multiple I/O faults for the same
             * page.  This should be a very rare situation.
             *
             * We are going to use ASYNCIO regardless of whether this is an
             * AIO or synchronous Direct I/O.  Otherwise for synchronous Direct
             * I/O, using VM_IO_LOCK would require us to hold the vas lock over
             * the entire I/O so that we avoid a potential dead lock situation
             * with some other (non-AdvFS) thread grabbing the vas lock and
             * then stalling on trying to get the pg_lock on a page, while we
             * are trying to get the vas lock to release our pg_lock.  Plus it
             * is anticipated that the prime consumers for Direct I/O will also
             * be using the AIO interface, so ASYNCIO would need to be used
             * anyway.
             */
            *vaslockpages_size = req_xfer_size;
            vaslockpages_flags = BREAK_COW | ASYNCIO;
            vas = P_VAS(u.u_procp);
            vas_read_lock( vas );
            ret = vaslockpages( req_xfer_base, vaslockpages_size,
                                vaslockpages_flags, B_READ, FALSE, NULL );
            vas_unlock(vas);
            if (ret != 0) {
                status = ret;
                goto cleanup;
            }
            wired_with = ANCHR_AIO_VASLOCKPAGES;

            if ( *vaslockpages_size != req_xfer_size ) {

                /* vaslockpages() detected that the requested size crosses a
                 * pregion boundary, and vaslockpages() is not prepared to
                 * handle that.  So, vaslockpages() has modified
                 * *vaslockpages_size to be an acceptable length, that stays in
                 * the same pregion.  We return E_PREGION_CROSSING_PLEASE_RETRY
                 * status and the updated *vaslockpages_size to the caller,
                 * which is responsible for retrying the I/O, passing back the
                 * updated *vaslockpages_size and we will do the rest.  It is a
                 * bit of a kludge, but doing this avoids the lock hierarchy
                 * problems with trying to wire down the buffer sooner.
                 */
                MS_SMP_ASSERT( *vaslockpages_size < req_xfer_size );
                MS_SMP_ASSERT( *vaslockpages_size > 0 );
                app_pages_wired = TRUE; /* forces unwire during cleanup */
                status = E_PREGION_CROSSING_PLEASE_RETRY;
                goto cleanup;
            }
        } else {
            wired_with = ANCHR_AIO_BUFPIN;
        }
        app_pages_wired = TRUE;
    }

    /* Handle any leading unaligned region. */
    if ( aligned_xfer_offset != req_xfer_offset ) {
        uint64_t nbytes = MIN(req_xfer_size, 
                              DEV_BSIZE - (req_xfer_offset % DEV_BSIZE));

        /* See if this unaligned sector has storage.  */
        tmp_offset = aligned_xfer_offset;
        blkmap_flags = XTNT_WAIT_OK | (xtntLockHeld ? XTNT_LOCKS_HELD : 0);
        sts = advfs_get_blkmap_in_range( bfap, bfap->xtnts.xtntMap,
                                         &tmp_offset,
                                         DEV_BSIZE,
                                         &storage_blkmap,
                                         &no_of_maps,
                                         RND_NONE,
                                         EXB_ONLY_STG,
                                         blkmap_flags );
        if ( sts != EOK ) {
            MS_SMP_ASSERT( sts != E_OFFSET_NOT_BLK_ALIGNED );
            status = sts;
            goto cleanup;
        }

        if ( no_of_maps == 0 ) {
            /* If this sector is in a hole, fill leading area with zeros */
            advfs_dio_bzero( req_xfer_base, nbytes, uio->uio_seg );
        } else {
            /* This region has storage; read the data */
            sts = advfs_dio_unaligned_xfer( bfap,
                                            req_xfer_base,
                                            uio->uio_seg,
                                            req_xfer_offset % DEV_BSIZE,
                                            nbytes,
                                            storage_blkmap,
                                            0UL, 0UL,
                                            B_READ );
            if (sts != EOK) {
                status = sts;
                goto cleanup;
            }
        }

        /* Account for transfer of bytes in leading unaligned section */
        work_xfer_base += nbytes;
        work_xfer_size -= nbytes;
        bytes_remaining -= nbytes;
        *bytes_read += nbytes;
        (void)advfs_free_blkmaps( &storage_blkmap );

        /* Since we did this IO in line, we must adjust aio_bp->b_resid
         * for the number of bytes transferred, since these will not be
         * tallied  as an aio completion in the IO completion path.
         */
        if (aio_bp)
            aio_bp->b_resid -= nbytes;
    }

    if (!bytes_remaining)        /* leading unaligned did the whole request */
        goto cleanup;

    /* Handle any trailing unaligned read */
    if ( aligned_xfer_offset + aligned_xfer_size !=
         req_xfer_offset + req_xfer_size ) {
        uint64_t trailing_nbytes = (req_xfer_offset+req_xfer_size) % DEV_BSIZE;
        uint64_t dist_to_trailing_sector = req_xfer_size - trailing_nbytes;

        /* See if this unaligned sector has storage.  */
        tmp_offset = ADVFS_ALIGN_OFFSET_TO_VDBLK_DOWN(req_xfer_offset +
                                                      req_xfer_size);
        blkmap_flags = XTNT_WAIT_OK | (xtntLockHeld ? XTNT_LOCKS_HELD : 0);
        sts = advfs_get_blkmap_in_range( bfap, bfap->xtnts.xtntMap,
                                         &tmp_offset,
                                         DEV_BSIZE,
                                         &storage_blkmap,
                                         &no_of_maps,
                                         RND_NONE,
                                         EXB_ONLY_STG,
                                         blkmap_flags );
        if ( sts != EOK ) {
            MS_SMP_ASSERT( sts != E_OFFSET_NOT_BLK_ALIGNED );
            status = sts;
            goto cleanup;
        }

        if ( no_of_maps == 0 ) {
            /* If this sector is in a hole, return zeros */
            advfs_dio_bzero( req_xfer_base + dist_to_trailing_sector, 
                                   trailing_nbytes, uio->uio_seg );
        } else {
            /* Sector has storage; read the data into the trailing region */
            MS_SMP_ASSERT( no_of_maps == 1 );
            sts = advfs_dio_unaligned_xfer( bfap,
                      req_xfer_base + dist_to_trailing_sector,
                      uio->uio_seg,
                      0UL,
                      trailing_nbytes,
                      storage_blkmap,
                      0UL, 
                      0UL,
                      B_READ );
        }
        if (sts == EOK) {
            trailing_bytes_read = trailing_nbytes;

            /* Since we did this IO in line, we must adjust aio_bp->b_resid
             * for the number of bytes transferred, since these will not be
             * tallied  as an aio completion in the IO completion path.
             */
            if (aio_bp)
                aio_bp->b_resid -= trailing_nbytes;
        }
        bytes_remaining -= trailing_nbytes;
        (void)advfs_free_blkmaps( &storage_blkmap );
    }

    if (!bytes_remaining)        /* unaligned xfers did the whole request */
        goto cleanup;

    /* Adjust aligned values for any unaligned transfers already done. */
    if (*bytes_read) {
        aligned_xfer_offset += DEV_BSIZE;
        aligned_xfer_size -= DEV_BSIZE;
    }
    if (trailing_bytes_read)
        aligned_xfer_size -= DEV_BSIZE;

    /* Optimistically set this variable; will be modified if the aligned
     * I/O fails.
     */
    aligned_bytes_read = bytes_remaining;

    /* Handling of aligned transfers starts here.  Generate a single
     * I/O for each extent map within the region of the aligned request.
     * If any extent is larger than the preferred transfer size, break
     * it down into a series of I/Os that are each of the preferred
     * transfer size.
     */
    ioap = advfs_bs_get_ioanchor( M_WAITOK );
    ioap->anchr_iocounter   = 1;            /* set artificially high by 1 */
    ioap->anchr_flags       = IOANCHORFLG_DIRECTIO;
    ioap->anchr_flags      |= (do_asynch) ? 0   : (IOANCHORFLG_KEEP_ANCHOR | 
                                                   IOANCHORFLG_WAKEUP_ON_LAST_IO);
    ioap->anchr_aio_bp      = (do_asynch) ? aio_bp : NULL;
    ioap->anchr_io_status   = 0;
    ioap->anchr_min_req_offset = aligned_xfer_offset;
    ioap->anchr_min_err_offset = aligned_xfer_offset + aligned_xfer_size;
    if ( do_asynch ) {
        MS_SMP_ASSERT( aio_bp );
        ioap->anchr_aio_info.anchr_actrangep   = arp;
        ioap->anchr_aio_info.anchr_aio_sid     = (uio->uio_seg == UIOSEG_USER)
                                               ? ldusid(req_xfer_base)
                                               : ldsid(req_xfer_base);
        ioap->anchr_aio_info.anchr_aio_address = req_xfer_base;
        ioap->anchr_aio_info.anchr_aio_size    = req_xfer_size;
        ioap->anchr_aio_info.anchr_aio_flags  |= wired_with;
    }

    /* Get the extent maps (both storage and hole info) for the file */
    tmp_offset = aligned_xfer_offset;
    blkmap_flags = XTNT_WAIT_OK | (xtntLockHeld ? XTNT_LOCKS_HELD : 0);
    sts = advfs_get_blkmap_in_range( bfap, bfap->xtnts.xtntMap,
                          &tmp_offset, aligned_xfer_size,
                          &storage_blkmap, NULL, RND_NONE, EXB_COMPLETE,
                          blkmap_flags );
    if ( sts != EOK ) {
        MS_SMP_ASSERT( sts != E_OFFSET_NOT_BLK_ALIGNED );
        status = sts;
        goto cleanup;
    }

    /* Walk through the extent maps, generating a buf struct for each I/O */
    blkp = storage_blkmap;
    while ( blkp ) {
        uint64_t nbytes = MIN( work_xfer_size, blkp->ebd_byte_cnt );
        uint64_t blkno  = blkp->ebd_vd_blk;
        int doing_sub_extent = 0;

        if ( blkno == XTNT_TERM  || blkno == COWED_HOLE ) {
            /* this is a hole; just fill application buffer with zeros
             * for the size represented by this block map or request.
             */
            advfs_dio_bzero( work_xfer_base, nbytes, uio->uio_seg );
        } else {
            /* This has storage; get preferred xfer size for disk. If
             * the blkmap size exceeds the preferred xfer size, we
             * will build several buf structs and do several I/Os to
             * transfer data within this single extent.
             */

            /* If we already built a buf structure (on a previous pass
             * through this loop), start I/O on it before building the 
             * next one.  Bump iocounter to keep it above 0.
             */
            if ( bp ) {
                spin_lock(&ioap->anchr_lock);
                ioap->anchr_iocounter++;
                spin_unlock(&ioap->anchr_lock);
                if ( do_asynch )
                    ioap->anchr_aio_info.anchr_aio_iosize += bp->b_bcount;
                advfs_bs_startio( bp, ioap, NULL, bfap, vdp, 0 );
                bp = NULL;
            }

            vdp = VD_HTOP( blkp->ebd_vd_index, bfap->dmnP );
            if ( nbytes > vdp->current_iosize_rd) {
                nbytes = vdp->current_iosize_rd;
                doing_sub_extent = 1;
            }

            /* allocate and initialize a buf struct */
            bp = advfs_bs_get_buf( TRUE, TRUE);
            bp->b_un.b_addr = (caddr_t)( work_xfer_base );
            bp->b_spaddr    = (uio->uio_seg == UIOSEG_USER)
                            ? ldusid(work_xfer_base)
                            : ldsid(work_xfer_base);
            bp->b_blkno     = blkno;
            bp->b_foffset   = work_xfer_offset;
            bp->b_bcount    = nbytes;
            bp->b_vp        = &bfap->bfVnode;
            bp->b_proc      = u.u_procp;
            BSETFLAGS(bp,(bufflags_t)(B_READ | B_RAW | B_PHYS));
            BSETFLAGS(bp,(bufflags_t)((do_asynch) ? B_ASYNC : B_SYNC));
        }

        /* Now account for this transfer and set up for the next */
        work_xfer_offset += nbytes;
        work_xfer_base   += nbytes;
        work_xfer_size   -= nbytes;;
        bytes_remaining  -= nbytes;

        if ( !bytes_remaining ) {
            break;
        } else if ( doing_sub_extent  && (blkp->ebd_byte_cnt - nbytes != 0) ) {
            /* We set up for a partial transfer for this extent map.
             * If there is more to transfer in this extent, fix up extent
             * info to do next section.
             */
            MS_SMP_ASSERT( (int64_t)blkp->ebd_byte_cnt - (int64_t)nbytes >= 0 );
            blkp->ebd_byte_cnt -= nbytes;
            blkp->ebd_vd_blk += ADVFS_BYTES_TO_VDBLKS(nbytes);
        } else {
            /* go to next extent map */
            blkp = blkp->ebd_next_desc;
        }
    }

    /* Send the last (possibly only) I/O on its way, if there is one.
     * Don't bump ioAnchor counter here since it was artificially
     * bumped by one when initialized.  Then wait for the I/O completion
     * if not doing asynchronous I/Os.
     */
    if ( bp ) {
        /* If starting asynchronous I/Os, disown the cacheModeLock.  
         * This will be claimed and released in advfs_bs_io_complete()
         * after the final I/O for the ioAnchor has completed.
         */
        if ( do_asynch ) {
            /* Since this Direct I/O will be completed asynchronously, now
             * is the best time for us to ASSERT that the active range has
             * no pages in the UFC.  After the upcoming start I/O, this
             * request and the arp will be out of our control.
             */
            ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );

            ioap->anchr_aio_info.anchr_aio_iosize += bp->b_bcount;
            ioap->anchr_aio_info.anchr_lock_key =
                                              ADVRWL_BFAPCACHEMODE_DISOWN(bfap);
        }

        advfs_bs_startio( bp, ioap, NULL, bfap, vdp, 0 );

        if ( do_asynch ) {
            /* This is a marker to upper levels that we started an 
             * asynchronous I/O 
             */
            iov->iov_len = 1;
            asynch_io_was_started = 1;
        } else {
            /* Not in asynch mode: wait for the I/Os to complete.
             * Avoid calling cv_wait() after cv_broadcast() has already 
             * been issued during IO completion. 
             */
            spin_lock(&ioap->anchr_lock);
            if ( ioap->anchr_flags & IOANCHORFLG_IODONE) {
                spin_unlock(&ioap->anchr_lock);
            } else {
                /* One of these better be set or your not waking up !*/
                MS_SMP_ASSERT(
                   (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) ||
                   (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_LAST_IO));
                cv_wait(&ioap->anchr_cvwait, &ioap->anchr_lock, CV_SPIN,
                        CV_NO_RELOCK);
            }
            MS_SMP_ASSERT(ioap->anchr_flags & IOANCHORFLG_IODONE);

            /* Now check the status of the IO */
            if ( ioap->anchr_io_status != EOK ) {
                status = ioap->anchr_io_status;
                aligned_bytes_read  = ioap->anchr_min_err_offset;
                trailing_bytes_read = 0;    /* These can't be counted */
            }

            /* Account for the bytes transferred in the aio_bp structure
             * if we did a synchronous IO as part of an AIO request.
             * Normally this is done in the IO completion path when the
             * request is handled asynchronously.
             */
            if ( aio_bp ) 
                aio_bp->b_resid -= aligned_bytes_read;
        }
    }

cleanup:
    /* Adjust uio fields for successfully transferred bytes */
    *bytes_read += aligned_bytes_read + trailing_bytes_read;
    uio->uio_iov->iov_base += *bytes_read;
    uio->uio_iov->iov_len -= *bytes_read;
    uio->uio_offset += *bytes_read;
    uio->uio_resid  -= *bytes_read;

    /* handle transfers with multiple uio->uio_iov vectors */
    if ( (uio->uio_iovcnt > 1) && (uio->uio_iov->iov_len == 0) ) {
        uio->uio_iov++;
        uio->uio_iovcnt--;
    }

    if ( xtntLockHeld ) {
        if (has_parent_snap) {
            (void)advfs_drop_xtntMap_locks( bfap, SF_SNAP_NOFLAGS );
        } else { 
            ADVRWL_XTNTMAP_UNLOCK(&(bfap->xtntMap_lk));
        }
    } 

    if (has_parent_snap) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
    }

    /* unwire application pages if needed. Must do before reseizing
     * file lock because of lock hierarchy ordering with pfdat locks
     * which will be released here.  If asynchronous IO was started,
     * this will be done at IO completion.
     */
    if ( app_pages_wired && !asynch_io_was_started ) {
        if ( wired_with & ANCHR_AIO_BUFPIN ) {
            bufunpin( ldusid(req_xfer_base), req_xfer_base,
                      req_xfer_size, B_READ);
        } 
        else if ( wired_with & ANCHR_AIO_VASLOCKPAGES ) {
            vasunlockpages( req_xfer_base, *vaslockpages_size,
                            vaslockpages_flags, 
                            ldusid(req_xfer_base), NULL, B_READ );
        }
    }

    ADVRWL_FILECONTEXT_READ_LOCK(contextp);

    /* Remove active range and ioanchor if no asynchronous I/Os
     * were started. These all get cleaned up in the I/O completion
     * path for asynchronous I/Os.
     */
    if ( (status != EOK) || !asynch_io_was_started ) {
        if (aio_bp) {
            MS_SMP_ASSERT( iov->iov_len == 0 );
        }
        /* Before we drop the active range, ASSERT that none of the
         * pages in this range have showed up unexpectedly in the UFC.
         */
        ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );

        spin_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);
        spin_unlock(&bfap->actRangeLock);
        advfs_bs_free_actRange( arp, TRUE );
        if ( ioap )
            advfs_bs_free_ioanchor( ioap, M_WAITOK );
    }

    /* free resources allocated locally */
    if ( storage_blkmap )
        (void)advfs_free_blkmaps( &storage_blkmap );

    return(status);
}


/* advfs_dio_unaligned_xfer()
 *
 * Called to do:  1) unaligned transfer of data.  This is defined as a
 *                     transfer that does not start or end on a sector
 *                     boundary.
 *                2) zero-fill sectors that have been newly allocated
 *                     on a directIO write, but that won't be overwritten
 *                     by the application data.  This can happen because
 *                     of storage allocation unit granularity.
 *                3) Both of the above.
 *
 * Parameters:  bfap        IN: bfap for this file.
 *              addr        IN: addr of application buffer into which the
 *                              data will be written (or read from).
 *              uio_seg     IN: pass uio->uio_seg in this argument.  It will
 *                              be used to decide if copyin()/copyout() are
 *                              to be used or bcopy() for a buffer in kernel
 *                              space (like a CFS, NFS, or other kernel mode
 *                              caller).
 *              offset      IN: offset in sector (in bytes) that transfer
 *                              begins.
 *              nbyte       IN: # of bytes of data to transfer.  This may
 *                              be zero if being called only to zero-fill
 *                              blocks on disk in newly-allocated storage.
 *              blkmap      IN: The block map for the storage to be
 *                              read/written.
 *              stg_fobs    IN: # of fobs in newly-allocated storage that
 *                              require some amount of zero-filling.  If
 *                              there is unaligned data to transfer, it will
 *                              go in one of these fobs.  The value will only
 *                              be non-zero on write operations.
 *              xtra_fobs   IN: # of fobs that must be zero'd in addition to
 *                              those passed in 'stg_fobs'.  This is an
 *                              optimization and is only passed as a non-zero
 *                              value when there is an unaligned leading write
 *                              and the data is contained totally within a
 *                              given sector.  This allows us to write the data
 *                              and zero newly-allocated storage before and 
 *                              beyond the data in a single call.
 *              flag        IN: B_READ or B_WRITE depending on whether this
 *                              is a read or write operation. 
 */
static statusT
advfs_dio_unaligned_xfer( bfAccessT  *bfap,
                          caddr_t     addr,
                          int32_t     uio_seg,
                          uint64_t    offset,
                          size_t      nbyte,
                          extent_blk_desc_t *blkmap,
                          bf_fob_t    stg_fobs,
                          bf_fob_t    xtra_fobs,
                          int         flag   )
{
    statusT              status         = EOK;
    int                  write_only     = FALSE;
    size_t               bufsize;
    caddr_t              tmpbuf;
    caddr_t              offset_addr;
    struct ioanchor     *ioap;
    struct buf          *bp;
    struct vd           *vdp;
    extent_blk_desc_t   *current_blkmap;
    struct buf          *copy_buf;
    size_t               whats_left;


    MS_SMP_ASSERT( nbyte || stg_fobs );         /* one must be non-zero */

    /* Get an aligned temporary memory buffer.  It is important to have an
     * aligned memory buffer.  The device drivers (see cdb_res_alloc()) will
     * check to see if the buffer address is aligned to the device driver's
     * idea of cached aligned memory (not necessary the same as the CPU
     * cache alignment).  If the buffer does not pass the test, then the
     * device driver code will allocate its own memory, and do additional
     * memory-to-memory copies to align the data the way the drivers like
     * it.  If we can provide an aligned buffer, this would avoid wasting
     * CPU cycles for the additional memory-to-memory copy operations.
     *
     * The advfs_aligned_arena is setup to return aligned memory
     * (KAT_ALIGN_ON_SIZE_COMPAT), based on the size of the request.  We are
     * always asking for temporary buffers that are sized 1K or larger, so
     * at the very minimum we should always have at least a 1K alignment.
     * The assumption is that 1K aligned memory will be at least as good as
     * what the device drivers need, if not better.  If someone discovers
     * this to be an invalid assumption, we can change the
     * advfs_aligned_arena to provide 4K aligned buffers if necessary.
     */
    bufsize = MAX(DEV_BSIZE,
              (((stg_fobs+xtra_fobs)/ADVFS_FOBS_PER_DEV_BSIZE) * ADVFS_FOB_SZ));
    MS_SMP_ASSERT( bufsize < (size_t)INT_MAX ); /* bp->b_count is an int */

    whats_left = bufsize;

    tmpbuf = (caddr_t)kmem_arena_varalloc( advfs_aligned_arena, 
                                           bufsize, 
                                           M_WAITOK );

    MS_SMP_ASSERT( tmpbuf != NULL                          &&
                   ((uint64_t)tmpbuf & (DEV_BSIZE-1)) == 0 ); /* min aligned? */

    /* Determine if we need to read sector(s) from disk */
    write_only = ( (stg_fobs != 0) && !(flag & B_READ) );

    /* Allocate an ioAnchor and buf structure for the I/Os */
    ioap = advfs_bs_get_ioanchor( M_WAITOK );
    bp   = advfs_bs_get_buf( TRUE, TRUE);

    vdp = VD_HTOP(blkmap->ebd_vd_index, bfap->dmnP );
    MS_SMP_ASSERT( vdp );

    /* Calculate where the offset is located.  If (offset == 0), then this
     * is a trailing write and there is no need to adjust for the number of
     * storage fobs padded into the merge.  
     */
    offset_addr = tmpbuf;
    if (offset) {
        offset_addr = (stg_fobs)
                    ? tmpbuf + ((stg_fobs-1) * ADVFS_FOB_SZ) + offset
                    : tmpbuf                                 + offset;
    }
    MS_SMP_ASSERT( offset_addr         <= tmpbuf + bufsize &&
                   offset_addr + nbyte <= tmpbuf + bufsize );

    if ( write_only ) {
        size_t  nBefore;
        size_t  nAfter;
        caddr_t after;

        /* There is new storage that needs zero filling.
         */
        MS_SMP_ASSERT( sizeof(*tmpbuf)      == 1 &&  /* a lot depends on this */
                       sizeof(*offset_addr) == 1 );

        nBefore = (size_t)offset_addr - (size_t)tmpbuf;
        if ( nBefore != 0 ) {
            bzero( tmpbuf, nBefore );
        }

        nAfter = bufsize - (nBefore + nbyte);
        after  = offset_addr + nbyte;
        if ( nAfter != 0 ) {
            bzero( after, nAfter );
        }
        MS_SMP_ASSERT( nBefore + nbyte + nAfter ==          bufsize &&
                       after           + nAfter == tmpbuf + bufsize &&
                       after                    <= tmpbuf + bufsize );
    }
    else {
        /* A read is needed.  This could be because the application is
         * reading unaligned data, or because it is writing unaligned data
         * and we need to read the data from disk and merge it with the
         * application data before we can write it back.
         *
         * When reading is involved, this unaligned transfer must be to
         * already allocated storage, so there should not be any new storage
         * to zero fill.
         */
        MS_SMP_ASSERT( stg_fobs  == 0              &&
                       xtra_fobs == 0              &&
                       bufsize   == DEV_BSIZE      &&
                       bufsize   >  offset         &&
                       nbyte     >  0              &&
                       bufsize   >= offset + nbyte );

        /* Initialize ioanchor and buf structures */
        ioap->anchr_iocounter   = 1;
        ioap->anchr_flags       = IOANCHORFLG_DIRECTIO | IOANCHORFLG_KEEP_BUF |
                                  IOANCHORFLG_KEEP_ANCHOR | IOANCHORFLG_WAKEUP_ON_LAST_IO;
        ioap->anchr_io_status   = 0;
        ioap->anchr_min_req_offset = 0;

        bp->b_un.b_addr = tmpbuf;
        bp->b_spaddr    = ldsid( tmpbuf );
        bp->b_blkno     = blkmap->ebd_vd_blk;
        bp->b_bcount    = (int32_t)(MIN(bufsize, blkmap->ebd_byte_cnt));
        bp->b_foffset   = blkmap->ebd_offset;
        bp->b_vp        = &bfap->bfVnode;
        bp->b_proc      = u.u_procp;
        BSETFLAGS(bp,(bufflags_t)(B_READ | B_RAW | B_PHYS | B_SYNC));

        /* Start the I/O */
        advfs_bs_startio( bp, ioap, NULL, bfap, vdp, 0UL );

        /* wait for the I/O to complete; avoid calling cv_wait() after
         * the cv_broadcast() has already been issued. 
         */
        spin_lock(&ioap->anchr_lock);
        if ( ioap->anchr_flags & IOANCHORFLG_IODONE) {
            spin_unlock(&ioap->anchr_lock);
        } else {
                /* One of these better be set or your not waking up !*/
            MS_SMP_ASSERT(
                (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) ||
                (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_LAST_IO));
            cv_wait(&ioap->anchr_cvwait, &ioap->anchr_lock, CV_SPIN,
                    CV_NO_RELOCK);
        }
        MS_SMP_ASSERT(ioap->anchr_flags & IOANCHORFLG_IODONE);

        /* Check for I/O error */
        if ( (BGETFLAGS(bp) & B_ERROR) && (bp->b_error != 0) ) {
            status = bp->b_error;
            goto cleanup;
        }

        if ( flag & B_READ ) {
            /* Copy data to application buffer */
            status = advfs_dio_copyout( offset_addr, addr, nbyte, uio_seg );
            goto cleanup;
        }
    }

    /* We are here if there is data to write to disk; see if there is
     * data to be merged.
     */
    if ( nbyte ) {
        /* There is application data to merge. Calculate the location
         * in the temporary buffer to write the application data, and
         * then copy it.  If (offset == 0), then this is a trailing
         * write and there is no need to adjust for the # storage fobs
         * padded in for the merge.  
         */
        status = advfs_dio_copyin( addr, offset_addr, nbyte, uio_seg );
        if ( status != EOK ) {
            goto cleanup;
        }
    }

    /* (Re)initialize the ioAnchor and buf structures. */
    ioap->anchr_iocounter   = 1;
    ioap->anchr_flags       = IOANCHORFLG_DIRECTIO | IOANCHORFLG_KEEP_BUF |
                              IOANCHORFLG_KEEP_ANCHOR | IOANCHORFLG_WAKEUP_ON_LAST_IO;
    ioap->anchr_io_status   = 0;
    ioap->anchr_min_req_offset = 0;
    bp->b_un.b_addr = tmpbuf;
    bp->b_spaddr    = ldsid( tmpbuf );
    bp->b_blkno     = blkmap->ebd_vd_blk;
    bp->b_bcount    = (int32_t)(MIN(bufsize, blkmap->ebd_byte_cnt));
    bp->b_foffset   = blkmap->ebd_offset;
    bp->b_vp        = &bfap->bfVnode;
    bp->b_proc      = u.u_procp;
    ADVFS_INIT_B_FLAGS(bp);
    BSETFLAGS(bp,(bufflags_t)(B_WRITE | B_RAW | B_PHYS | B_SYNC));
    bp->b_error     = 0;

    /*
     * Reset whats_left to represent how much of the intended I/O
     * still needs to be handled.
     */
    whats_left -= bp->b_bcount;

    /*
     * If multiple extents need to be written, launch I/O's to 
     * all of them except for the first one.
     */
    for (current_blkmap = blkmap->ebd_next_desc; 
         current_blkmap; 
         current_blkmap = current_blkmap->ebd_next_desc) {

        /*
         * Add another I/O to wait for to the I/O anchor.
         */
        spin_lock(&ioap->anchr_lock);
        ioap->anchr_iocounter++;
        spin_unlock(&ioap->anchr_lock);

        /*
         * Allocate a copy buf.  The I/O completion will by
         * default free it.
         */
        copy_buf = advfs_bs_get_buf(TRUE, FALSE);

        /*
         * Copy the original buf into this copy buf.
         */
        bcopy(bp,copy_buf,sizeof(struct buf));

        /*
         * Update fields in the copy buf.
         */
        copy_buf->b_foffset = current_blkmap->ebd_offset;
        copy_buf->b_un.b_addr += (copy_buf->b_foffset - bp->b_foffset);
        MS_SMP_ASSERT(current_blkmap->ebd_vd_blk != XTNT_TERM);
        copy_buf->b_blkno = current_blkmap->ebd_vd_blk;
        copy_buf->b_bcount = (int32_t)(MIN(whats_left, 
                                           current_blkmap->ebd_byte_cnt));
        MS_SMP_ASSERT(copy_buf->b_un.b_addr + copy_buf->b_bcount <= 
                      tmpbuf + bufsize);

        /*
         * Reset whats_left to represent how much of the intended I/O
         * still needs to be handled.
         */
        whats_left -= copy_buf->b_bcount;

        /* 
         * Since we have not started the I/O for the first blkmap, 
         * we know that the I/O anchor can not go away.  So it is safe 
         * to get this I/O started. 
         */
        advfs_bs_startio(copy_buf,
                         ioap,
                         NULL,
                         bfap, 
                         VD_HTOP(current_blkmap->ebd_vd_index,
                                 bfap->dmnP),
                         0UL);
    }

    /* Start the I/O for the first (and usually only) blkmap */
    advfs_bs_startio( bp, ioap, NULL, bfap, vdp, 0UL );

    /* 
     * Wait for the I/O to complete.  Avoid calling cv_wait() after
     * the cv_signal() has already been issued. 
     */
    spin_lock(&ioap->anchr_lock);
    if ( ioap->anchr_flags & IOANCHORFLG_IODONE) {
        spin_unlock(&ioap->anchr_lock);
    } else {
                /* One of these better be set or your not waking up !*/
        MS_SMP_ASSERT(
            (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) ||
            (ioap->anchr_flags & IOANCHORFLG_WAKEUP_ON_LAST_IO));
        cv_wait(&ioap->anchr_cvwait, &ioap->anchr_lock, CV_SPIN, CV_NO_RELOCK);
    }
    MS_SMP_ASSERT(ioap->anchr_flags & IOANCHORFLG_IODONE);

    /* Check for I/O error */
    if ( (BGETFLAGS(bp) & B_ERROR) && (bp->b_error != 0) ) {
        status = bp->b_error;
    }

cleanup:
    advfs_bs_free_buf( bp);
    advfs_bs_free_ioanchor( ioap, M_WAITOK );
    (void)kmem_arena_free( tmpbuf, M_WAITOK );

    return( status );
}

