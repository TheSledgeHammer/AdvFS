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
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: msfs_vnops.c,v $ $Revision: 1.1.528.21 $ (DEC) $Date: 2008/01/31 13:03:33 $"

#include <sys/lock_probe.h>
#include <sys/param.h>
#include <sys/systm.h>
#include <sys/time.h>
#include <sys/vnode.h>
#include <sys/vfs_proto.h>
#include <sys/mount.h>
#include <sys/uio.h>
#include <sys/errno.h>
#include <sys/kernel.h>
#include <sys/namei.h>
#include <sys/stat.h>
#include <sys/mode.h>
#include <sys/fcntl.h>
#include <sys/flock.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ftx_public.h>
#include <msfs/ms_osf.h>
#include <msfs/fs_dir.h>
#include <msfs/fs_dir_routines.h>
#include <msfs/fs_quota.h>
#include <sys/buf.h>
#include <sys/lock_probe.h>
#include <sys/secpolicy.h>
#include <sys/sec_objects.h>
#include <sys/ioctl.h>
#include <msfs/bs_index.h>
#include <sys/clu.h>
#include <sys/versw.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_params.h>
#include <sys/sp_attr.h>
#include <sys/specdev.h>
#include <unistd.h>
#include <mach/memory_object.h>
#include <nfs/nfs.h>
#if SEC_BASE
#include <sys/security.h>
#endif
#include <builtin/ux_exception.h>
#include <mach/exception.h>
#include <sys/file.h>

extern void * BsAccessHashTbl;

static int msfs_chown(struct vnode *vp, uid_t uid, gid_t gid, struct ucred *cred, ftxHT parentFtxH, int setctime);

#define ADVFS_MODULE MSFS_VNOPS

/*
 *  The following two macros are used in support of freezefs to help
 *  prevent NFS server hangs due to a frozen domain.  If an NFS server
 *  thread is about to enter a vnop that has the potential to modify
 *  metadata (and therefore block if the domain is frozen) it will
 *  immediately return with the NFS3ERR_JUKEBOX error code.  NFS clients
 *  should treat this error return as retriable and retry the operation
 *  after waiting 10 seconds (or so).  If we did not return an error and
 *  the NFS server thread hung on the frozen domain, the client would
 *  timeout the operation and re-issue the request.  The new request
 *  would cause another server thread to hang and eventually all of
 *  the server threads could end up being blocked on a single frozen
 *  domain.
 */

#define NFS_FREEZE_LOCK(vp) {                                                       \
                                                                                    \
    if ( NFS_SERVER_TSD != 0 ) {                                                    \
        domainT *dmnP = GETDOMAINP ( VTOMOUNT(vp) );                                \
        mutex_lock( &(dmnP->dmnFreezeMutex) );                                      \
        if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) {       \
            mutex_unlock( &dmnP->dmnFreezeMutex );                                  \
            return (NFS3ERR_JUKEBOX);                                               \
        } else {                                                                    \
            dmnP->dmnFreezeRefCnt++;                                                \
            mutex_unlock(&dmnP->dmnFreezeMutex);                                    \
        }                                                                           \
    }                                                                               \
}
#define NFS_FREEZE_UNLOCK(vp)  {                                                    \
                                                                                    \
    if ( NFS_SERVER_TSD != 0 ) {                                                    \
        domainT *dmnP = GETDOMAINP ( VTOMOUNT(vp) );                                \
        mutex_lock( &dmnP->dmnFreezeMutex );                                        \
        dmnP->dmnFreezeRefCnt--;                                                    \
        if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {                \
            thread_wakeup( (vm_offset_t)&dmnP->dmnFreezeWaiting );                  \
        }                                                                           \
        mutex_unlock( &dmnP->dmnFreezeMutex );                                      \
    }                                                                               \
}

int
msfs_noop(void)
{
    return (EOPNOTSUPP);
}

/*struct buf xxbp;*/

/*
 * Global vfs data structures for msfs
 */

int msfs_lookup(),
    msfs_create(struct nameidata *ndp, struct vattr *vap),
    msfs_mknod(struct nameidata *ndp, struct vattr *vap, struct ucred *cred),
    msfs_open(struct vnode **vpp, int mode, struct ucred *cred),
    msfs_close(struct vnode *vp, int fflag, struct ucred *cred),
    msfs_access(struct vnode *vp, int mode, struct ucred *cred),
    msfs_getattr(struct vnode *vp, register struct vattr *vap, struct ucred *cred),
    msfs_setattr(register struct vnode *vp, register struct vattr *vap, register struct ucred *cred),
    msfs_read(struct vnode *vp, register struct uio *uio, int ioflag, struct ucred *cred),
    msfs_write(struct vnode *vp, register struct uio *uio, int ioflag, struct ucred *cred),
    msfs_ioctl(),
    seltrue(),
    msfs_mmap(),
    msfs_fsync(struct vnode *vp, int fflags, struct ucred *cred, int waitfor),
    msfs_seek(struct vnode *vp, off_t oldoff, off_t newoff, struct ucred *cred),
    msfs_remove(struct nameidata *ndp),
    msfs_link(register struct vnode *vp, register struct nameidata *ndp),
    msfs_rename(register struct nameidata *fndp, register struct nameidata *tndp),
    msfs_mkdir(struct nameidata *ndp, struct vattr *vap),
    msfs_rmdir(register struct nameidata *ndp),
    msfs_symlink(struct nameidata *ndp, struct vattr *vap, char *target),
    msfs_readdir(struct vnode *vp, register struct uio *uio, struct ucred *cred, int *eofflagp),
    msfs_readlink(struct vnode *vp, struct uio *uiop, struct ucred *cred),
    msfs_abortop(struct nameidata *ndp, int ret_val),
    msfs_inactive(),
    msfs_reclaim(),
    msfs_bmap(),
    msfs_strategy(),
    msfs_print(struct vnode *vp),
    msfs_page_read(struct vnode *vp, struct uio *uio, struct ucred *cred),
    msfs_page_write(struct vnode *vp, struct uio *uio, struct ucred *cred, memory_object_t pager, vm_offset_t offset),
    msfs_refer(),
    msfs_release(),
    msfs_getpage(),     /* get page */
    msfs_putpage(),     /* put page */
    msfs_write_check(), /* check writability */
    msfs_swap(),        /* swap handler */
    msfs_bread(),       /* buffer read */
    msfs_brelse(),      /* buffer release */
    msfs_lockctl(struct vnode *vp, struct eflock *eld, int flag, struct ucred *cred, pid_t clid, off_t offset),
    /* file locking */
    msfs_setvlocks(struct vnode *vp),
    msfs_syncdata(struct vnode *vp, int fflags, vm_offset_t start, vm_size_t length, struct ucred *cred),
    /* fsync byte range */
    msfs_pathconf(),    /* pathconf */
    msfs_objtovp(),
    msfs_setpgstamp(),
    msfs_fs_cleanup(),
    msfs_fs_replicate(),
    msfs_log_and_meta_flush();
int
    msfs_noop(void),
    msfs_getproplist(), /* Get extended attributes */
    msfs_setproplist(), /* Set extended attributes */
    msfs_delproplist(); /* Delete extended attributes */

/*
 * msfs_vnodeops
 *
 * Defines function pointers to MegaSafe's VFS vnode operations.
 */

struct vnodeops msfs_vnodeops = {
    msfs_lookup,        /* lookup */
    msfs_create,        /* create */
    msfs_mknod,         /* mknod */
    msfs_open,          /* open */
    msfs_close,         /* close */
    msfs_access,        /* access */
    msfs_getattr,       /* getattr */
    msfs_setattr,       /* setattr */
    msfs_read,          /* read */
    msfs_write,         /* write */
    msfs_ioctl,         /* ioctl */
    seltrue,            /* select */
    msfs_mmap,          /* mmap */
    msfs_fsync,         /* fsync */
    msfs_seek,          /* seek */
    msfs_remove,        /* remove */
    msfs_link,          /* link */
    msfs_rename,        /* rename */
    msfs_mkdir,         /* mkdir */
    msfs_rmdir,         /* rmdir */
    msfs_symlink,       /* symlink */
    msfs_readdir,       /* readdir */
    msfs_readlink,      /* readlink */
    msfs_abortop,       /* abortop */
    msfs_inactive,      /* inactive */
    msfs_reclaim,       /* reclaim */
    msfs_bmap,          /* bmap */
    msfs_strategy,      /* strategy */
    msfs_print,         /* print */
    msfs_page_read,     /* page_read */
    msfs_page_write,    /* page_write */
    msfs_swap,          /* swap handler */
    msfs_bread,         /* buffer read */
    msfs_brelse,        /* buffer release */
    msfs_lockctl,       /* file locking */
    msfs_syncdata,      /* fsync byte range */
    msfs_noop,          /* Lock a node */
    msfs_noop,          /* Unlock a node */
    msfs_getproplist,   /* Get extended attributes */
    msfs_setproplist,   /* Set extended attributes */
    msfs_delproplist,   /* Delete extended attributes */
    msfs_pathconf,      /* pathconf */
};

/*
 * Megasafe support for device special files and fifos.
 */
int spec_lookup(),
    spec_open(),
    msfsspec_read(struct vnode *vp, struct uio *uio, int ioflag, struct ucred *cred),
    msfsspec_write(struct vnode *vp, struct uio *uio, int ioflag, struct ucred *cred),
    spec_strategy(),
    spec_bmap(),
    spec_ioctl(),
    spec_select(),
    spec_seek(),
    msfsspec_close(struct vnode *vp, int fflag, struct ucred *cred),
    msfsspec_reclaim(register struct vnode *vp, int flags),
    spec_page_read(),
    spec_page_write(),
    spec_badop(),
    spec_nullop(),
    spec_mmap(),
    spec_swap(),
    spec_lockctl(),
    specvn_pathconf(),
    msfsspec_pathconf(register struct vnode *vp, int name, long *retval);

extern int spec_bread(), spec_brelse();

struct vnodeops spec_bfnodeops = {
    spec_lookup,            /* lookup */
    spec_badop,             /* create */
    spec_badop,             /* mknod */
    spec_open,              /* open */
    msfsspec_close,         /* close */
    msfs_access,            /* access */
    msfs_getattr,           /* getattr */
    msfs_setattr,           /* setattr */
    msfsspec_read,          /* read */
    msfsspec_write,         /* write */
    spec_ioctl,             /* ioctl */
    spec_select,            /* select */
    spec_mmap,              /* mmap */
    spec_nullop,            /* fsync */
    spec_seek,              /* seek */
    spec_badop,             /* remove */
    spec_badop,             /* link */
    spec_badop,             /* rename */
    spec_badop,             /* mkdir */
    spec_badop,             /* rmdir */
    spec_badop,             /* symlink */
    spec_badop,             /* readdir */
    spec_badop,             /* readlink */
    spec_badop,             /* abortop */
    msfs_inactive,          /* inactive */
    msfsspec_reclaim,       /* reclaim */
    spec_bmap,              /* bmap */
    spec_strategy,          /* strategy */
    msfs_print,             /* print */
    spec_badop,             /* page_read */
    spec_badop,             /* page_write */
    spec_swap,              /* swap */
    spec_bread,             /* buffer read */
    spec_brelse,            /* buffer release */
    spec_lockctl,           /* file locking */
    spec_nullop,            /* fsync byte range */
    msfs_noop,              /* Lock a node */
    msfs_noop,              /* Unlock a node */
    msfs_getproplist,       /* Get extended attributes */
    msfs_setproplist,       /* Set extended attributes */
    msfs_delproplist,       /* Delete extended attributes */
    msfsspec_pathconf,      /* pathconf */
};

extern int
    fifo_open(),
    fifo_ioctl(),
    fifo_select(),
    fifo_bread(),
    fifo_brelse(),
    fifo_pathconf();

int msfsfifo_close(struct vnode *vp, int fflag, struct ucred *cred),
    msfsfifo_read(struct vnode *vp, struct uio *uio, int ioflag, struct ucred *cred),
    msfsfifo_write(struct vnode *vp, struct uio *uio, int ioflag, struct ucred *cred),
    msfsfifo_getattr(struct vnode *vp, register struct vattr *vap, struct ucred *cred),
    msfsfifo_reclaim(struct vnode *vp, int flags),
    msfsfifo_pathconf(register struct vnode *vp, int name, long *retval);

struct vnodeops fifo_bfnodeops = {
    spec_lookup,            /* lookup */
    spec_badop,             /* create */
    spec_badop,             /* mknod */
    fifo_open,              /* open */
    msfsfifo_close,         /* close */
    msfs_access,            /* access */
    msfsfifo_getattr,       /* getattr */
    msfs_setattr,           /* setattr */
    msfsfifo_read,          /* read */
    msfsfifo_write,         /* write */
    fifo_ioctl,             /* ioctl */
    fifo_select,            /* select */
    spec_badop,             /* mmap */
    spec_nullop,            /* fsync */
    spec_seek,              /* seek */
    spec_badop,             /* remove */
    spec_badop,             /* link */
    spec_badop,             /* rename */
    spec_badop,             /* mkdir */
    spec_badop,             /* rmdir */
    spec_badop,             /* symlink */
    spec_badop,             /* readdir */
    spec_badop,             /* readlink */
    spec_badop,             /* abortop */
    msfs_inactive,          /* inactive */
    msfsfifo_reclaim,       /* reclaim */
    spec_bmap,              /* bmap */
    spec_badop,             /* strategy */
    msfs_print,             /* print */
    spec_badop,             /* page_read */
    spec_badop,             /* page_write */
    spec_swap,              /* swap */
    fifo_bread,             /* buffer read */
    fifo_brelse,            /* buffer release */
    spec_lockctl,           /* file locking */
    spec_nullop,            /* fsync byte range */
    msfs_noop,              /* Lock a node */
    msfs_noop,              /* Unlock a node */
    msfs_getproplist,       /* Get extended attributes */
    msfs_setproplist,       /* Set extended attributes */
    msfs_delproplist,       /* Delete extended attributes */
    msfsfifo_pathconf,      /* pathconf */
};

struct vfs_ubcops msfs_ubcops = {
    msfs_refer,         /* refer vnode */
    msfs_release,       /* release vnode */
    msfs_getpage,       /* get page */
    msfs_putpage,       /* put page */
    msfs_write_check,   /* check writability */
    msfs_objtovp,       /* get vnode ptr */
    msfs_setpgstamp,    /* set page stamp */
    msfs_fs_cleanup,    /* filesystem memory page cleanup */
    msfs_fs_replicate,  /* page replication */
    NULL,               /* io threshold */
    msfs_log_and_meta_flush,    /* flush log and metadata */
};

/*
 * local function prototytes
 */

/*
 * msfs_chown
 *
 * change owner of file
 */

static int
msfs_chown(
           struct vnode *vp,          /* in - vnode pointer for bitfile */
           uid_t uid,                 /* in - uid to change to */
           gid_t gid,                 /* in - gid to change to */
           struct ucred *cred,        /* in - callers credentials */
           ftxHT parentFtxH,          /* in - transaction handle */
           int setctime
           );

/*
 * msfs_chmod
 *
 * chmod operation on a file
 * this is not directly a vn op, but is called from
 * msfs_setattr
 */
int
msfs_chmod(
           struct vnode *vp,               /* in - vnode of bitfile */
           struct fsContext *file_context, /* in - file context pointer */
           register int mode,              /* in - mode to change to */
           struct ucred *cred              /* in - callers credentials */
           );

/*
 * msfs_mknod
 *
 * Make a fifo or device special file.
 */
msfs_mknod(
    struct nameidata *ndp,   /* in - nameidata pointer */
    struct vattr *vap,       /* in - vnode attributes */
    struct ucred *cred       /* in - caller's credentials */
    );

/*
 * decr_link_count
 *
 * decr the link count of a file and delete if count is 0
 */
decr_link_count(
          struct vnode *vp, /* in - the context pointer of
                                          the file */
          ftxHT ftx_handle  /* in - ftx handle */
          );

/*
 * bzero_frag
 *
 * zero out a frag's contents.  Analogous to bcopy_frag in fs_read_write.c
 */
static statusT
bzero_frag(
    struct bfAccess *bfap,
    uint32T start,
    ftxHT ftxH
    );

/*
 * table used by VTTOTYP macro, which is used by the MAKEMODE macro.
 * The two macros translate the vnode-type to a file type and 'or' it
 * with the creation mode. Used by msfs_create.
 *
 */

int vttotyp_tab[9] = {
    0, S_IFREG, S_IFDIR, S_IFBLK, S_IFCHR, S_IFLNK, S_IFSOCK, S_IFIFO, S_IFMT
};

enum vtype typtovt_tab[16] = {
        VNON, VFIFO, VCHR, VNON, VDIR, VNON, VBLK, VNON,
        VREG, VNON, VLNK, VNON, VSOCK, VNON, VNON, VBAD
};


/*
 * msfs_create
 *
 * create a file. the directory was already opened by
 * msfs_lookup.
 */
int
msfs_create(
            struct nameidata *ndp,   /* in - nameidata pointer */
            struct vattr *vap       /* in - vnode attributes for create */
            )
{
    statusT ret;

    FILESETSTAT( ndp->ni_dvp, msfs_create );

    NFS_FREEZE_LOCK(ndp->ni_dvp);
    ret = fs_create_file(vap, ndp);
    NFS_FREEZE_UNLOCK(ndp->ni_dvp);

    return( ret );
}

/*
 * Mknod vnode call
 */
/* ARGSUSED */
msfs_mknod(
       struct nameidata *ndp,   /* in - nameidata pointer */
       struct vattr *vap,       /* in - vnode attributes */
       struct ucred *cred       /* in - caller's credentials */
       )
{
    int error;

    FILESETSTAT( ndp->ni_dvp, msfs_mknod );

    /*
     * validate va_rdev if VBLK or VCHR, otherwise
     * va_rdev must not be set.
     *
     * XXX: note that the credential arg is not used.
     */
    if (vap->va_type == VBLK || vap->va_type == VCHR) {
        if (vap->va_rdev == (dev_t)VNOVAL) {
            return (EINVAL);
        }
    }
    else {
        if (vap->va_rdev != (dev_t)VNOVAL) {
           return (EINVAL);
        }
    }

    NFS_FREEZE_LOCK(ndp->ni_dvp);
    if (error = fs_create_file(vap, ndp)) {
        NFS_FREEZE_UNLOCK(ndp->ni_dvp);
    }
    /* release the vnode associated with the special file */
    vrele(ndp->ni_vp);

    NFS_FREEZE_UNLOCK(ndp->ni_dvp);
    return (0);
}

/*
 * Check to see if the cache policy of the file is being modified.
 * Otherwise, just return success from open.  msfs_lookup has
 * already opened the file.
 */
msfs_open(
          struct vnode **vpp,   /* in - vnode pointer */
          int mode,             /* in - open mode */
          struct ucred *cred    /* in - credentials of caller */
          )
{
    struct fsContext *contextp = VTOC(*vpp);
    struct bfAccess  *bfap     = VTOA(*vpp);
    int ret = 0;
    extern int Advfs_enable_dio_mount;


    /* Disable the O_CACHE flag for first ship of V5.0.
     * Remove next line to reenable that capability.
     */
    mode &= ~O_CACHE;

    /*
     * Do we need to change the cache policy on the file?
     */
    if (mode & (O_DIRECTIO | O_CACHE)) {

        /*
         * When changing the cache policy, take the file write lock
         * to force all active I/O to complete before the policy
         * change goes into effect.  This will prevent two different
         * policies from being in effect at the same time.  It also
         * allows us to coordinate with data logging activation code.
         * Do not allow the use of direct I/O on a file that uses
         * data logging.
         */
        FS_FILE_WRITE_LOCK(contextp);
        VN_LOCK(*vpp);

        if (mode & O_DIRECTIO) {
            /* Rules du jour when enabling directIO on a file:
             * BaseOS:
             * 1. If file is already open for directio, just return success.
             * 2. If reserved file and we are enabling the hidden mount
             *    '-o directIO' option, return success w/o setting directIO.
             *    If the hidden option is not enabled, return an error.
             *    Reserved files (and AdvFS special files such as the frag
             *    file, bitfile set tag directories, and quota files) are
             *    never opened for directIO.
             * 3. If this is a file other than VREG, return an error.
             * 4. If file is already open for mmapping, return error.
             * 5. If file is open for atomic-write data logging, return error.
             * Additional restrictions for CFS Concurrent DirectIO:
             * 1. Fail opens of metadata files by .tags
             */
            if ((*vpp)->v_flag & VDIRECTIO ) {
                /* Already open for DirectIO, return success */
                ret = EOK;
                VN_UNLOCK(*vpp);
            } else if ( BS_BFTAG_RSVD(bfap->tag) ||
                      ( bfap->dataSafety == BFD_FTX_AGENT ) ||
                      ( bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY )) {
                /* Metadata file; check if we open in cached mode or fail
                 * the open.  In no case do we set the VDIRECTIO bit for
                 * metadata files.
                 * This check also includes the AdvFS 'special' files.
                 * The blanket check for BFD_FTX_AGENT and
                 * BFD_FTX_AGENT_TEMPORARY currently allows
                 * data-logging files to be treated the same as the
                 * special files.  Since we are only using the backdoor
                 * mount for internal testing, this is OK.  If we ever
                 * allow mounting a fileset for directIO to be shipped,
                 * then the above check will have to be restricted to our
                 * special files, but not include data logging files.
                 */
                if ( Advfs_enable_dio_mount &&
                     (*vpp)->v_mount->m_flag & M_DIRECTIO ) {
                    ret = EOK;
                } else {
                    ret = EINVAL;
                }
                VN_UNLOCK(*vpp);
            } else if ( ((*vpp)->v_type != VREG)            ||
                        ((*vpp)->v_flag & VMMAPPED )        ||
                        (bfap->dataSafety == BFD_FTX_AGENT) ||
                        (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY) ||
                        (bfap->real_bfap  &&
                         bfap->real_bfap->dataSafety == BFD_FTX_AGENT) ) {
                ret = EINVAL;
                VN_UNLOCK(*vpp);
            } else if ( clu_is_ready() && (contextp->fs_flag & META_OPEN) ) {
                /* For clusters no reserved files via .tags */
                ret = EINVAL;
                VN_UNLOCK(*vpp);
            } else {
                /* Turn on the directIO flag and flush buffers. If
                 * enabling dio for CFS, then be sure that any storage
                 * allocation transactions are on disk; do this by
                 * flushing the log.  This is necessary because CFS
                 * DIO assumes that if the block is allocated to this
                 * file, it won't be removed on a subsequent crash and
                 * recovery.
                 */
                (*vpp)->v_flag |= VDIRECTIO;
                VN_UNLOCK(*vpp);
                if ( clu_is_ready() && contextp->dirty_alloc ) {
                    lgr_flush( bfap->dmnP->ftxLogP );
                    contextp->dirty_alloc = FALSE;
                }
                bfflush(bfap, 0, 0, FLUSH_INTERMEDIATE);
            }
        } else {
            /* The code for setting O_CACHE, and perhaps turning 
             * directIO off when there are active directIO threads
             * in flight has been removed.  This is dead code since
             * O_CACHE is explicitly turned off above.  If this
             * capability is ever re-enabled, consider using an
             * active range that covers the entire file to block
             * active directIO threads while the open mode is changed.
             */
            MS_SMP_ASSERT( !(mode & O_CACHE) );
        }

        /*
         * Unlock the file
         */
        FS_FILE_UNLOCK(contextp);
    }

    return(ret);
}

/*
 * msfs_close
 *
 * close a file
 */
msfs_close(
           struct vnode *vp,    /* in - vnode of file to close */
           int fflag,           /* in - flags - not used */
           struct ucred *cred   /* in - credentials of caller */
           )
{
    struct fsContext *context_pointer = VTOC( vp );
    ftxHT ftx_handle;
    bfAccessT *bfap = VTOA(vp);

    FILESETSTAT( vp, msfs_close );

    if ((context_pointer->fs_flag & META_OPEN) != 0) {

        return (0);
    }

    NFS_FREEZE_LOCK(vp);

    /*
     * check to see if the stats were changed
     */

    if (context_pointer->dirty_stats == TRUE) {

        /*
         * screen domain panic
         */
        bfap = VTOA(vp);
        if (bfap->dmnP->dmn_panic) {
            NFS_FREEZE_UNLOCK(vp);
            return EIO;
        }

        if ( context_pointer->fs_flag & MOD_MTIME ) {
            /*
             * reset to avoid excess preallocation.
             */
            vp->v_lastr = 0;
        }

        if ( FTX_START_N( FTA_NULL, &ftx_handle, FtxNilFtxH,
                          GETDOMAINP(VTOMOUNT(vp)), 1 ) != EOK ) {
            NFS_FREEZE_UNLOCK(vp);
            return (0);
        }
        /*
         * log an update to the in-memory file stats.
         */

        (void)fs_update_stats(vp, bfap, ftx_handle, 0);

        ftx_done_n(ftx_handle, FTA_FS_CREATE_2);
    }

    NFS_FREEZE_UNLOCK(vp);
    return (0);
}


/*
 * msfs_access
 *
 * check the access for a file
 */
msfs_access(
            struct vnode *vp,   /* in - vnode of file to check */
            int mode,           /* in - mode of caller */
            struct ucred *cred  /* in - credential structure of caller */
            )
{
    int results, ret;
    struct bfNode *bnp;
    struct fsContext *context_ptr;
    udac_t udac;
    bfAccessT *bfap = VTOA(vp);

    FILESETSTAT( vp, msfs_access );

    bnp = (struct bfNode *)&vp->v_data[0];
    context_ptr = bnp->fsContextp;
    if (context_ptr == NIL) {
        return (EACCES);
    }
    if ( BS_BFTAG_RSVD(bfap->tag) ) {
#if SEC_PRIV
        if ( !privileged(SEC_ALLOWDACACCESS, 0) {
            return EACCES;
        }
#else
        if ( suser(cred, &u.u_acflag) != 0 ) {
            return EACCES;
        }
#endif
    }

    /*
     * lock the fsContext area for the file in question
     * while getting the necessary info from it
     */

#if SEC_ARCH
    if (!(mode & SP_STATACC)) {
#endif
#if SEC_ARCH
    }
#endif

    mutex_lock( &context_ptr->fsContext_mutex );
    udac.uid = udac.cuid = context_ptr->dir_stats.st_uid;
    udac.gid = udac.cgid = context_ptr->dir_stats.st_gid;
    udac.mode = context_ptr->dir_stats.st_mode & 0777;
    mutex_unlock( &context_ptr->fsContext_mutex );

    results = sp_vnaccess(vp,  mode, &udac, cred);

    if (!results) {
        ret = 0;
    } else {
        ret = EACCES;
    }

    return (ret);
}


/*
 * msfs_getattr
 *
 * get the attributes for a file
 */
msfs_getattr(
             struct vnode *vp,           /* in - vnode pointer of file */
             register struct vattr *vap, /* out - vnode attribute
                                            structure */
             struct ucred *cred          /* in - callers credentials */
             )
{
    bfAccessT *bfap;
    struct fsContext *context_ptr;
    bfSetT *bfSetp;
    struct mount *mp;
    struct timeval new_time;
    uint32T pageCnt;
    statusT sts;
    struct fileSetNode *fsn;


    FILESETSTAT( vp, msfs_getattr );

    bfap = VTOA(vp);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }

    context_ptr = VTOC(vp);

    if (context_ptr->fs_flag & META_OPEN) {
        /* meta data and fragbf have fake stats */


        mutex_lock( &context_ptr->fsContext_mutex );

        vap->va_atime.tv_sec = 0;
        vap->va_atime.tv_usec = 0;
        vap->va_mtime.tv_sec = 0;
        vap->va_mtime.tv_usec = 0;
        vap->va_ctime.tv_sec = 0;
        vap->va_ctime.tv_usec = 0;
        vap->va_fsid = 0;
        vap->va_fileid = BS_BFTAG_IDX( context_ptr->bf_tag );
        vap->va_gen = BS_BFTAG_SEQ( context_ptr->bf_tag );
        vap->va_mode = S_IFREG;
        vap->va_nlink = 0;
        vap->va_nlink_l = 0;
        vap->va_uid = 0;
        vap->va_gid = 0;
        vap->va_rdev = 0;

        vap->va_flags = 0;
        vap->va_blocksize = ADVFS_PGSZ;
        vap->va_type = vp->v_type;

        /* update file_size */
        bfap->file_size = (off_t)((((bfSetT *)(bfap->bfSetp))->cloneId &&
                                   bfap->cloneId) ?
                                   (off_t)bfap->maxClonePgs * ADVFS_PGSZ :
                                   (off_t)bfap->nextPage * ADVFS_PGSZ);

        vap->va_size = (off_t)bfap->file_size;

        mutex_unlock( &context_ptr->fsContext_mutex );

        /*
         * Get the real number of bytes in the file.
         * Have to do this after the fsContext_mutex
         * is released since bs_get_bf_page_cnt()
         * and the routines it calls may block and
         * probably will take other locks.
         */
        sts = bs_get_bf_page_cnt( bfap, &pageCnt );
        if (sts != EOK) {
            vap->va_bytes = 0;      /* XXX */
        } else {
            vap->va_bytes = (ulong_t)pageCnt * ADVFS_PGSZ;
        }

        return 0;
    }

    mp = VTOMOUNT(vp);
    if (mp != NULL && mp->m_info != NULL) {
        bfSetp = GETBFSETP(mp);
    }

    fsn = GETFILESETNODE(mp);

    /*
     * copy the attributes from the stat structure in the file's
     * in-memory directory entry to the vap structure
     */
    mutex_lock( &context_ptr->fsContext_mutex );
    /*
     * update the time fields if not readonly fs.
     * if readonly just read what's in fscontext
     */
    if ((fsn->fsFlags & FS_CLONEFSET) || (mp->m_flag & M_RDONLY)) {
        /*
         * divide the utime fields by 1000 to put microsecs
         * back to millisecs
         */
        vap->va_atime.tv_sec = context_ptr->dir_stats.st_atime;
        vap->va_atime.tv_usec = context_ptr->dir_stats.st_uatime / 1000;
        vap->va_mtime.tv_sec = context_ptr->dir_stats.st_mtime;
        vap->va_mtime.tv_usec = context_ptr->dir_stats.st_umtime / 1000;
        vap->va_ctime.tv_sec = context_ptr->dir_stats.st_ctime;
        vap->va_ctime.tv_usec = context_ptr->dir_stats.st_uctime / 1000;
        goto no_update;
    }
    if (context_ptr->fs_flag & (MOD_MTIME | MOD_ATIME | MOD_CTIME)) {
        TIME_READ(new_time);
    }
    /*
     * either use the fscontext time or the system time.
     * try to minimize the dividing and multiplying of usecs.
     */
    if (context_ptr->fs_flag & MOD_MTIME) {
        vap->va_mtime.tv_sec = new_time.tv_sec;
        vap->va_mtime.tv_usec = new_time.tv_usec;
        context_ptr->dir_stats.st_mtime = new_time.tv_sec;
        context_ptr->dir_stats.st_umtime = new_time.tv_usec * 1000;
    } else {
        vap->va_mtime.tv_sec = context_ptr->dir_stats.st_mtime;
        vap->va_mtime.tv_usec = context_ptr->dir_stats.st_umtime / 1000;
    }
    if (context_ptr->fs_flag & MOD_ATIME) {
        vap->va_atime.tv_sec = new_time.tv_sec;
        vap->va_atime.tv_usec = new_time.tv_usec;
        context_ptr->dir_stats.st_atime = new_time.tv_sec;
        context_ptr->dir_stats.st_uatime = new_time.tv_usec * 1000;
    } else {
        vap->va_atime.tv_sec = context_ptr->dir_stats.st_atime;
        vap->va_atime.tv_usec = context_ptr->dir_stats.st_uatime / 1000;
    }
    if (context_ptr->fs_flag & MOD_CTIME) {
        vap->va_ctime.tv_sec = new_time.tv_sec;
        vap->va_ctime.tv_usec = new_time.tv_usec;
        context_ptr->dir_stats.st_ctime = new_time.tv_sec;
        context_ptr->dir_stats.st_uctime = new_time.tv_usec * 1000;
    } else {
        vap->va_ctime.tv_sec = context_ptr->dir_stats.st_ctime;
        vap->va_ctime.tv_usec = context_ptr->dir_stats.st_uctime / 1000;
    }
    context_ptr->fs_flag &= ~(MOD_MTIME | MOD_ATIME | MOD_CTIME);

no_update:
    if (mp != NULL && mp->m_info != NULL) {
/*      vap->va_fsid = bs_bfset_get_dev( bfSetp ); */
        vap->va_fsid = bfSetp->dev; /*  == f_fsid.val[0] */
    }
    vap->va_fileid = BS_BFTAG_IDX(context_ptr->dir_stats.st_ino);
    vap->va_mode = context_ptr->dir_stats.st_mode;

    /* set 16-bit and 32-bit link counts in vattr */
    set_vattr_nlinks(vap, context_ptr->dir_stats.st_nlink);
 
    vap->va_uid = context_ptr->dir_stats.st_uid;
    vap->va_gid = context_ptr->dir_stats.st_gid;
    vap->va_rdev = context_ptr->dir_stats.st_rdev;

    vap->va_size = bfap->file_size;
    vap->va_flags = context_ptr->dir_stats.st_flags;
    vap->va_blocksize = ADVFS_PGSZ;
    vap->va_type = vp->v_type;
    vap->va_gen = BS_BFTAG_SEQ(context_ptr->dir_stats.st_ino);

    /*
     * release mutex now because some of the 'bs' routines
     * below grab all sorts of locks and they can go to sleep.
     */

    mutex_unlock( &context_ptr->fsContext_mutex );

    /* the real number of bytes in the file */
    (void)bs_get_bf_page_cnt(bfap, &pageCnt);
    vap->va_bytes = (ulong_t)pageCnt * ADVFS_PGSZ;

    /*
     * fragState and fragId are normally protected by the fragLock
     * in the BfSet structure.  However, in this case we avoid taking
     * that lock and return the most recent size of the file.
     */

    if (bfap->fragState == FS_FRAG_VALID) {
        vap->va_bytes = vap->va_bytes +
            (context_ptr->dir_stats.fragId.type * BF_FRAG_SLOT_BYTES);
    }

    return(0);
}


/*
 * msfs_setattr
 *
 * set the attributes for a file
 */

msfs_setattr(
             register struct vnode *vp,    /* in - vnode pointer of file */
             register struct vattr *vap,   /* in - vnode attribute structure */
             register struct ucred *cred   /* in - callers credentials */
             )
{
    statusT sts=0;


    NFS_FREEZE_LOCK(vp);
    sts = fs_setattr(vp, vap, cred); /* pass msfs_setattr()'s args */
    NFS_FREEZE_UNLOCK(vp);

    return (sts);
}

static int fs_setattr_truncate( bfAccessT *, struct vattr *,
                                struct ucred *, int * );

fs_setattr(
             register struct vnode *vp,    /* in - vnode pointer of file */
             register struct vattr *vap,   /* in - vnode attribute structure */
             register struct ucred *cred)   /* in - callers credentials */
{
    int error = 0;
    statusT sts;
    bfAccessT *bfap;
    struct fsContext *context_ptr;
    uid_t iuid;
    ftxHT ftxH;
    int updateStats = 1;
    int doing_chown = FALSE;
    unsigned long ids[MAXQUOTAS];
    boolean_t quota_mig_trunc_lock[MAXQUOTAS];
    int type;

    /*
     * SVID III access rules vs BSD.
     * sys_v_mode predates the grpid mount option, but at present we support
     * both.  This affects group ownership of new inodes and preservation of
     * ISGID over modifications.
     */
    extern int sys_v_mode;
#define BSD_MODE(vp) (((vp)->v_mount->m_flag & M_GRPID) && !sys_v_mode)

    FILESETSTAT( vp, msfs_setattr );

    bfap = VTOA(vp);
    context_ptr = VTOC(vp);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }

    /*
     * check for unsettable attributes
     */

    if ((vap->va_type != VNON) ||
        (vap->va_nlink != (nlink_t)VNOVAL) ||
        (vap->va_fsid != (int)VNOVAL) ||
        (vap->va_fileid != (int)VNOVAL) ||
        (vap->va_blocksize != (int)VNOVAL) ||
        (vap->va_rdev != (dev_t)VNOVAL) ||
        (vap->va_bytes != VNOVAL) ||
        (vap->va_gen != (u_int)VNOVAL)) {

        return (EINVAL);
    }

    /*
     * update anything that isn't VNOVAL
     */

    /*
     * truncate
     */
    if (vap->va_size != (u_long)VNOVAL) {
        error = fs_setattr_truncate( bfap, vap, cred,  &updateStats );
        if ( error != EOK ) {
            goto _exit;
        }
    }

    iuid = context_ptr->dir_stats.st_uid;

    if (vap->va_ctime.tv_sec != (int)VNOVAL) {

            /*
             * Special purpose fcntl (for F_SETTIMES)
             *
             * No need to check for write access as only su can
             * be in this code path.
             */

            mutex_lock( &context_ptr->fsContext_mutex);
            if ((vap->va_atime.tv_sec != 0) || (vap->va_atime.tv_usec != 0)) {
                context_ptr->dir_stats.st_atime = vap->va_atime.tv_sec;
                context_ptr->dir_stats.st_uatime = vap->va_atime.tv_usec * 1000;
                context_ptr->fs_flag &= ~(MOD_ATIME);
            }
            if ((vap->va_mtime.tv_sec != 0) || (vap->va_mtime.tv_usec != 0)) {
                context_ptr->dir_stats.st_mtime = vap->va_mtime.tv_sec;
                context_ptr->dir_stats.st_umtime = vap->va_mtime.tv_usec * 1000;
                context_ptr->fs_flag &= ~(MOD_MTIME);
            }
            if ((vap->va_ctime.tv_sec != 0) || (vap->va_ctime.tv_usec != 0)) {
                context_ptr->dir_stats.st_ctime = vap->va_ctime.tv_sec;
                context_ptr->dir_stats.st_uctime = vap->va_ctime.tv_usec * 1000;
                context_ptr->fs_flag &= ~(MOD_CTIME);
            }
            context_ptr->dirty_stats = TRUE;
            mutex_unlock( &context_ptr->fsContext_mutex);

            if (sts = fs_update_stats(vp, bfap, FtxNilFtxH, 0) != EOK) {
                domain_panic(bfap->dmnP, "msfs_setattr: fs_update_stats (2) error, return code = %d",sts);
                return EIO;                  /* E_DOMAIN_PANIC */
            }

            error = BSERRMAP(error);
            goto _exit;
    }

    /*
     * utime
     */
    if (vap->va_atime.tv_sec != (int)VNOVAL ||
        vap->va_mtime.tv_sec != (int)VNOVAL) {
        /*
         * check for write access
         */
        if (cred->cr_uid != iuid) {
            if (error = msfs_access(vp, S_IWRITE, cred)) {
                goto _exit;
            }
        }
        mutex_lock( &context_ptr->fsContext_mutex);
        if (vap->va_atime.tv_sec != (int)VNOVAL) {
            context_ptr->dir_stats.st_atime = vap->va_atime.tv_sec;
            context_ptr->dir_stats.st_uatime = vap->va_atime.tv_usec * 1000;
            context_ptr->fs_flag &= ~(MOD_ATIME);
        }
        if (vap->va_mtime.tv_sec != (int)VNOVAL) {
            context_ptr->dir_stats.st_mtime = vap->va_mtime.tv_sec;
            context_ptr->dir_stats.st_umtime = vap->va_mtime.tv_usec * 1000;
            context_ptr->fs_flag &= ~(MOD_MTIME);
        }
        context_ptr->dirty_stats = TRUE;
        mutex_unlock( &context_ptr->fsContext_mutex);
        updateStats = 1;
    }

    /*
     * change mode
     */
    if (vap->va_mode != (u_short) VNOVAL) {
        if (error = msfs_chmod(
                               vp,
                               context_ptr,
                               vap->va_mode,
                               cred)) {
            goto _exit;
        }
        updateStats = 1;
    }
    /*
     * flags
     */
    if (vap->va_flags != (u_int)VNOVAL) {
        if (cred->cr_uid != iuid && (error = suser(cred, &u.u_acflag))) {
            goto _exit;
        }
        mutex_lock( &context_ptr->fsContext_mutex);
        if (cred->cr_uid == 0) {
            context_ptr->dir_stats.st_flags = vap->va_flags;
        } else {
            context_ptr->dir_stats.st_flags &= 0xffff0000;
            context_ptr->dir_stats.st_flags |= vap->va_flags & 0xffff;
        }
        mutex_unlock( &context_ptr->fsContext_mutex);
        updateStats = 1;
    }

    /*
     * The ftx is started here because chown needs an ftx to update
     * quotas.
     */

    /*
     * Take the fsContext write lock to serialize with other threads
     * racing to do chown.
     */
    quota_mig_trunc_lock[USRQUOTA] = quota_mig_trunc_lock[GRPQUOTA] = FALSE;
    if (vap->va_uid != (uid_t) VNOVAL || vap->va_gid != (gid_t) VNOVAL) {
        doing_chown = TRUE;
        FS_FILE_WRITE_LOCK(context_ptr);
        /* only take the lock if we are changing owners */
        ids[USRQUOTA] = (unsigned long) vap->va_uid;
        ids[GRPQUOTA] = (unsigned long) vap->va_gid;
        for (type = 0; type < MAXQUOTAS; type++) {
            if (!quota_page_is_mapped(context_ptr->fileSetNode->qi[type].qiAccessp,
                                      ids[type])) {
                MIGTRUNC_LOCK_READ( &(context_ptr->fileSetNode->
                                      qi[type].qiAccessp->xtnts.migTruncLk) );
                quota_mig_trunc_lock[type] = TRUE;
            }
        }
    }

    FTX_START_N(FTA_OSF_SETATTR_2, &ftxH, FtxNilFtxH,
                context_ptr->fileSetNode->dmnP, 1);

    /*
     * change owner
     */
    if (doing_chown) {

        if (error = msfs_chown(vp, vap->va_uid, vap->va_gid, cred, ftxH,
                        SET_CTIME)) {
            FS_FILE_UNLOCK(context_ptr);
            ftx_fail(ftxH);
            for (type = 0; type < MAXQUOTAS; type++) {
                if (quota_mig_trunc_lock[type]) {
                    MIGTRUNC_UNLOCK( &(context_ptr->fileSetNode->
                                       qi[type].qiAccessp->xtnts.migTruncLk) );
                    quota_mig_trunc_lock[type] = FALSE;
                }
            }
            goto _exit;
        }
        updateStats = 1;
    }

    if ( updateStats ) {

            mutex_lock( &context_ptr->fsContext_mutex);
            context_ptr->fs_flag |= MOD_CTIME;
            context_ptr->dirty_stats = TRUE;
            mutex_unlock( &context_ptr->fsContext_mutex);

        sts = fs_update_stats(vp, bfap, ftxH, 0);
        if (sts != EOK) {
            domain_panic(bfap->dmnP, "msfs_setattr: fs_update_stats (3) error, return code = %d",sts);
            error =  EIO;
        }
    }

    ftx_done_fs(ftxH, FTA_OSF_SETATTR_2, 0);

    if (doing_chown) {
        FS_FILE_UNLOCK(context_ptr);
        for (type = 0; type < MAXQUOTAS; type++) {
            if (quota_mig_trunc_lock[type]) {
                MIGTRUNC_UNLOCK( &(context_ptr->fileSetNode->
                                   qi[type].qiAccessp->xtnts.migTruncLk) );
                quota_mig_trunc_lock[type] = FALSE;
            }
        }
    }

_exit:

}

static int
fs_setattr_truncate( bfAccessT *bfap,
                     struct vattr *vap,
                     struct ucred *credp,
                     int *updateStatsA )
{
    struct vnode *vp = bfap->bfVp;
    struct fsContext *cp = VTOC( vp );
    int error;
    ulong truncationPage;
    uint oddOffset;
    bfAccessT *cloneap;
    bfSetT *cloneSetp;
    int tokenFlg;
    int setHeld = FALSE;
    off_t oldSize;
    void *delList;
    ftxHT ftxH;
    bfPageRefHT pageRef;
    char *pageAddr;
    uint32T delCnt = 0;
    u_short mask = S_ISUID;
    extern u_int noadd_execaccess;
    int remove_exec_perm = 0;
    actRangeT *arp;
    statusT sts;

    error = chk_quota_write( cp );
    if ( error != 0 ) {
        return error;
    }

    /*
     *  XXX - check for write access as in utime below?
     */

    VN_LOCK( vp );
    if ( vp->v_type == VDIR ) {
        VN_UNLOCK(vp);
        return EISDIR;
    }
    VN_UNLOCK(vp);

    if ( bfap->bfSetp->cloneId != BS_BFSET_ORIG ) {
        return EROFS;
    }

    truncationPage = vap->va_size / ADVFS_PGSZ;

    if (vap->va_size) {
        if (vap->va_size > bfap->file_size ||
              (vap->va_size < bfap->file_size &&
               !page_is_mapped(bfap, (uint32T)truncationPage, NULL, FALSE))) {
            /*
             * - file growth via VOP_SETATTR()
             * Instead of calling VOP_SETATTR() to grow a file, vtruncate()
             * writes a byte at the new end of file so we do not expect
             * to grow a file here unless a 3rd party calls us. If they
             * do, we must not allow the file size to extend beyond the
             * end of an existing frag. In fs_write, fs_write_add_stg 
             * will call copy_and_del_frag to handle this.
             *
             * - setting EOF inside a hole in the file
             * vtruncate() can call us to set EOF to an unmapped page
             * and we must allocate it and force the permanent hole in
             * the clone. In fs_write we will perform the cow and allocate
             * storage for the new EOF page.
             */
            struct uio uio;
            struct iovec iov;
            int zero = 0;

            iov.iov_base = (caddr_t)&zero;
            iov.iov_len = 1;
            uio.uio_iov = &iov;
            uio.uio_iovcnt = 1;
            uio.uio_offset = vap->va_size - 1;
            uio.uio_segflg = UIO_SYSSPACE;
            uio.uio_rw = UIO_WRITE;
            uio.uio_resid = 1;
            /* fs_write will check and limit the file size. */
            VOP_WRITE( vp, &uio, IO_TRUNC, credp, error );
            if ( error != 0 ) {
                return error;
            }
        } else if ( vap->va_size < bfap->file_size ) {
            /*
             * If we are going to truncate the file such that the new eof
             * is not on a page boundary and the new last page is mapped,
             * then we need to cow the last page here. We can't call
             * bs_cow_page later after setting the active range since
             * bs_cow_page will also try to set the active range and hang.
             */
            oddOffset = vap->va_size - truncationPage * ADVFS_PGSZ;

            if ( oddOffset &&
                 bfap->bfSetp->cloneSetp != NULL &&
                 page_is_mapped(bfap, (uint32T)truncationPage, NULL, FALSE) )
            {
                /* cow the new last page of this file. */
                bs_cow( bfap, COW_PINPG, truncationPage, 1, FtxNilFtxH );
            }
        }
    }


    if ( clu_is_ready() ) {
        tokenFlg = get_clu_clone_locks( bfap, cp, &cloneSetp, &cloneap );
        setHeld = TRUE;
    } else {
        /* We need the context lock. */
        /* If we didn't call get_clu_clone_locks then take it here. */
        FS_FILE_WRITE_LOCK( cp );
    }

    /* Seize an active range from the truncation point to the
     * end of the file.  When this range is established, we
     * have locked out any directIO writers to this range, 
     * which is what we want.
     */
    /* Allocate an active range structure; */
    arp = (actRangeT *)(ms_malloc( sizeof(actRangeT) ));

    arp->arStartBlock = truncationPage * ADVFS_PGSZ_IN_BLKS;
    arp->arEndBlock = ~0L;             /* biggest possible block */
    arp->arState = AR_TRUNCATE;
    arp->arDebug = SET_LINE_AND_THREAD(__LINE__);
    insert_actRange_onto_list( bfap, arp, cp );

    /* 
     * Prevent rmvol from removing the volume in between 
     * bf_setup_truncation() and stg_remove_stg_finish().
     */
    RMVOL_TRUNC_LOCK_READ( bfap->dmnP );

    /* Prevent access to clone while we truncate original. */
    TRUNC_XFER_LOCK_WRITE( &bfap->trunc_xfer_lk );

    /*
     *  File size and the magnetic extent map must change 'atomically'
     *  with respect to any other write operations being done concurrently
     *  on this file.
     */
    mutex_lock( &cp->fsContext_mutex );

    /*
     *  Update the size and time fields.
     */
    oldSize = bfap->file_size;
    bfap->file_size = vap->va_size;
        cp->fs_flag |= (MOD_CTIME | MOD_MTIME);
    cp->dirty_stats = TRUE;

    mutex_unlock( &cp->fsContext_mutex );

    /*
     *  Update mode (don't clear enforcement mode lock bits).
     *  indicated by setgid bit, but no group execute.
     */
    if ( !cp->dir_stats.st_mode & S_ISGID || cp->dir_stats.st_mode & S_IXGRP ) {
        if ( BSD_MODE(vp) ) {
            mask |= S_ISGID;
        }
    }

    if ( noadd_execaccess ) {
#if SEC_PRIV
        remove_exec_perm = !privileged( SEC_CHMODSUGID, 0 );
#else
        remove_exec_perm = suser( u.u_cred, NULL );
#endif
        if ( remove_exec_perm ) {
            mask |= (S_IXUSR|S_IXGRP|S_IXOTH);
        }
    }

    cp->dir_stats.st_mode &= ~mask;

    if ( remove_exec_perm ) {
        attribute_t *acl;

        acl = pacl_fs_get( vp, ACL_TYPE_ACC_MASK );
        if ( acl != (attribute_t *)NO_ATTR &&
             acl != (attribute_t *)UNKNOWN_ATTR_VAL &&
             (pacl_flatten(acl,ACFL_NFSPERM) & ACL_PEXECUTE) )
        {
            acl = dup_sec_attr( acl );
            /*
             * The lock must be released before
             * msfs_setproplist_int is called or
             * else deadlock will occur.
             * TBD: do something if something fails!
             */
            LOCK_DONE_SECATTR( vp );
            if ( acl ) {
                struct uio *uiop;

                (void)pacl_remove_exec_perm( acl, (attribute_t *)0 );

                uiop = alloc_setproplist_data( ACL_TYPE_ACC_MASK, acl );

                if ( uiop ) {
                    (void)msfs_setproplist_int( vp, uiop, credp, SET_CTIME, 0 );
                    FREE( (void *)uiop, M_SEC );
                }
                FREE( acl, M_SEC );
            }
        } else {
            LOCK_DONE_SECATTR( vp );
        }
    }

    if ( oldSize <= vap->va_size ) {
        /* new size is same or larger - don't truncate */
        goto notrunc;
    }

    /* Prevent races with migrate code paths
     * bf_setup_truncation() requires migTruncLk locked.
     * This lock must be locked before starting a root ftx.
     */
    /* can't be a clone */
    MS_SMP_ASSERT(bfap->bfSetp->cloneId == BS_BFSET_ORIG);
    MIGTRUNC_LOCK_READ( &bfap->xtnts.migTruncLk );

    /* No need to take the quota lock here. Storage is only
     * added to the quota files at file creation, chown
     * and explicit quota setting.
     * We DO need to take it later on in this function
     * because of the msfs_chown call.
     */

    FTX_START_N( FTA_OSF_SETATTR_1, &ftxH, FtxNilFtxH, bfap->dmnP, 1 );

    if ( (bfap->file_size == 0) ||
         (truncationPage < bfap->fragPageOffset) )
    {
        /*
         * Either the file is being truncated to 0 or it is being
         * truncated below the frag page.  In either case, deallocate
         * the frag.
         */
        sts = fs_delete_frag( bfap->bfSetp, bfap, vp, credp, FALSE, ftxH );
        if ( sts != EOK ) {
            domain_panic( bfap->dmnP,
                  "msfs_setattr: fs_delete_frag error, return code = %d", sts );
            ftx_done_fs( ftxH, FTA_OSF_SETATTR_1, 0 );
            MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) )
            error = EIO;             /* E_DOMAIN_PANIC */
            goto notrunc;
        }
    }

    /*
     *  Update magnetic extent map, if necessary (i.e,
     * deallocate magnetic storage).
     */

    if ( fs_trunc_test(vp) ) {
        if ( !bfap->trunc ) {
            delCnt = 0;
        } else {
            /* If there is a clone, storage wiil be xfer'd to the clone. */
            delCnt = bf_setup_truncation(bfap, ftxH, &delList, FALSE);
            bfap->trunc = 0;
        }
    }

    oddOffset = vap->va_size - (truncationPage * ADVFS_PGSZ);

    if ( oddOffset ) {
        /*
         * Zero out the trailing segment.
         */
        if ( page_is_mapped(bfap, (uint32T)truncationPage, NULL, FALSE) ) {
            /* It's on a page. */
            /* We've already cow'ed this page so don'to cow within pinpg. */
            sts = bs_pinpg_int( &pageRef,
                                (void*)&pageAddr,
                                bfap,
                                truncationPage,
                                BS_NIL,
                                PINPG_NOFLAG );
            if ( sts == EOK ) {
                bzero( pageAddr + oddOffset, ADVFS_PGSZ - oddOffset );
                (void)bs_unpinpg( pageRef, logNilRecord, BS_DIRTY );
            }
        } else if ( bfap->fragState == FS_FRAG_VALID &&
                    bfap->fragPageOffset == truncationPage )
        {
            sts = bzero_frag( bfap, oddOffset, ftxH );
        }
    }
    sts = fs_update_stats( vp, bfap, ftxH, 0 );
    if ( sts != EOK ) {
        domain_panic( bfap->dmnP,
             "msfs_setattr: fs_update_stats (1) error, return code = %d", sts );
        ftx_done_fs( ftxH, FTA_OSF_SETATTR_1, 0 );
        MIGTRUNC_UNLOCK( &bfap->xtnts.migTruncLk )
        error = EIO;                 /* E_DOMAIN_PANIC */
        goto notrunc;
    }

    *updateStatsA = 0;

    ftx_done_fs( ftxH, FTA_OSF_SETATTR_1, 0 );

    /* Release the clone locks after the ftx is DONE.*/
    if ( setHeld ) {
        release_clu_clone_locks( bfap, cloneSetp, cloneap, tokenFlg );
        setHeld = FALSE;
    }

    MIGTRUNC_UNLOCK( &bfap->xtnts.migTruncLk );

    if ( delCnt ) {
        stg_remove_stg_finish( bfap->dmnP, delCnt, delList );
    }

notrunc:
    /* Get rid of the activeRange applied above. */
    mutex_lock( &bfap->actRangeLock );
    remove_actRange_from_list( bfap, arp );
    mutex_unlock( &bfap->actRangeLock );
    ms_free( arp );

    TRUNC_XFER_UNLOCK( &bfap->trunc_xfer_lk );

    if ( setHeld ) {
        release_clu_clone_locks( bfap, cloneSetp, cloneap, tokenFlg );
    }

    FS_FILE_UNLOCK( cp );
    RMVOL_TRUNC_UNLOCK( bfap->dmnP );

    return error;
}

/*
 * bzero_frag
 *
 * Zero the contents of a frag.
 */
static statusT
bzero_frag(struct bfAccess *bfap, uint32T start, ftxHT ftxH)
{
    statusT sts = EOK;
    uint32T fragPgOffset = FRAG2PG(bfap->fragId.frag);
    uint32T fpOffset, fpCount, fragSize, zOffset;
    bfPageRefHT pgRef;
    char *pgAddr;

    /*
     * We want to clear data in [start, fragSize).  This could span two frag 
     * file pages.
     */
    fragSize = bfap->fragId.type * BF_FRAG_SLOT_BYTES;
    fpOffset = FRAG2SLOT(bfap->fragId.frag) * BF_FRAG_SLOT_BYTES;
    fpCount = ADVFS_PGSZ - fpOffset;
    zOffset = start;
    if ( fpCount > fragSize ) {
        /* Slot is completely inside the first frag page */
        fpCount = fragSize;
    }
    if ( zOffset >= fpCount ) {
        /* Beginning is beyond end of first frag page */
        fragPgOffset++;
        zOffset -= fpCount;
        fpCount = fragSize - fpCount;
        fpOffset = 0;
    }
    sts = bs_pinpg_ftx(&pgRef,
                       (void *) &pgAddr,
                       bfap->bfSetp->fragBfAp,
                       fragPgOffset,
                       BS_NIL,
                       ftxH);
    if ( sts == EOK ) {
        bzero(pgAddr + fpOffset + zOffset, fpCount - zOffset);
        (void)bs_unpinpg(pgRef, logNilRecord, BS_DIRTY);
        
        /*
         * Now see if we have to zero part of a second frag page.
         */
        if (( fragPgOffset == FRAG2PG(bfap->fragId.frag ) ) &&
            ( fpCount < fragSize )) {
            sts = bs_pinpg_ftx(&pgRef,
                               (void *) &pgAddr,
                               bfap->bfSetp->fragBfAp,
                               fragPgOffset + 1,
                               BS_NIL,
                               ftxH);
            if ( sts == EOK ) {
                bzero(pgAddr, fragSize - fpCount);
                (void)bs_unpinpg(pgRef, logNilRecord, BS_DIRTY);
            }
        }
    }
    return( sts );
}


/*
 * msfs_read
 *
 * read a file
 */
msfs_read(
          struct vnode *vp,         /* in - vnode of file to read */
          register struct uio *uio, /* in - uio struct - info for read */
          int ioflag,               /* in - ioflag - POSIX 1003.1b sync i/o */
          struct ucred *cred        /* in - callers credentials */
          )
{
    int ret;

    FILESETSTAT( vp, msfs_read );

    if ((uio->uio_rw != UIO_READ) && (uio->uio_rw != UIO_AIORW)) {
        ADVFS_SAD0("msfs_read mode incorrect");
    }

    if (uio->uio_resid == 0) {
        return(0);
    }

    if ((long) uio->uio_offset < 0) {
        return(EINVAL);
    }

/*
 * for POSIX 1003.1b, compact the uio data, and add ioflag
 */
    ret = fs_read(
                  vp,
                  uio,
                  ioflag,         /* POSIX 1003.1b sync i/o information */
                  cred
                  );

    return( ret );
}


/*
 * msfs_write
 *
 * write a file
 */
msfs_write(
           struct vnode *vp,          /* in - vnode pointer of file */
           register struct uio *uio,  /* in - uio struct - io information */
           int ioflag,                /* in - flag - append, sync */
           struct ucred *cred         /* in - callers credentials */
           )
{
    int ret;

    FILESETSTAT( vp, msfs_write );

    if ((uio->uio_rw != UIO_WRITE) && (uio->uio_rw != UIO_AIORW)) {
        ADVFS_SAD0("msfs_write mode incorrect");
    }
    if (uio->uio_resid == 0) {
        return(0);
    }
    if ((long) uio->uio_offset < 0) {
        return(EINVAL);
    }

    if ((ret = chk_quota_write(VTOC(vp))) != 0) {
        return ret;
    }

    NFS_FREEZE_LOCK(vp);

    ret = fs_write(
                   vp,
                   uio,
                   ioflag,
                   cred
                   );

    NFS_FREEZE_UNLOCK(vp);
}


#ifdef ADVFS_NO_ZERO_ALLOCATE
/* ioctl for quickly allocating storage in test filesystems has
 * local definitions only so we won't expose them to customers.
 * Note - also defined locally in CFS and test programs/headers.
 */

typedef struct stg_alloc {
    long    stgOffset;       /* in - starting block of allocation */
    long    stgLength;       /* in - length of allocation */
    long    stgReserved;     /* in - future use, must be zero */
    long    stgBytesAlloc;   /* out - # of bytes allocated */
} stg_allocT;

#define ADVFS_TESTSTORAGE  _IOWR('F', 27, struct stg_alloc)
#endif

/*
 * msfs_ioctl()
 *
 * Description:
 *
 * Msfs_ioctl() handles two commands, GETCACHEPOLICY and SETCACHEPOLICY.
 * The cache policies currently supported are CS_DIRECTIO (direct I/O
 * enabled) and CS_CACHE (filesystem's default cache policy is enabled).
 *
 * It now also handles GETMAP.  This retrieves a file's sparseness map
 * in support of the AdvFS backup API project.
 *
 * Parameters:
 *
 * struct vnode *vp      vnode pointer (in)
 * int cmd               Command, GETCACHEPOLICY or SETCACHEPOLICY (in)
 * caddr_t data          CS_DIRECTIO or CS_CACHE (in and out)
 * int fflag             Unused (in)
 * struct ucred *cred    Unused (in)
 */
int
msfs_ioctl(struct vnode *vp, int cmd, caddr_t data,
           int fflag, struct ucred *cred)
{
    int retval;
    int vn_locked = FALSE;


    switch(cmd) {
    case GETCACHEPOLICY:
    case SETCACHEPOLICY:
    {
        /*
         * File type must be VREG.
         */
        if (vp->v_type != VREG) {
            retval = ENOTTY;
            break;
        }

        retval = ESUCCESS;
        if (cmd == GETCACHEPOLICY) {
            *(int *)data = ((vp->v_flag & VDIRECTIO) ? CS_DIRECTIO : CS_CACHE);
        }
        else {
            struct fsContext *contextp = VTOC(vp);
            struct bfAccess  *bfap     = VTOA(vp);
            long mode = *(long *)data;

            /* Note: This SETCACHEPOLICY interface is provided solely for CFS.
             *       There's no way for a user level application to get here
             *       since we check for F_SETCACHEPOLICY in fcntl(), and
             *       return an error in that case.
             */

            /*
             * When changing the cache policy, take the file write lock
             * to force all active I/O to complete before the policy
             * change goes into effect.  This will prevent two different
             * policies from being in effect at the same time.
             */


            FS_FILE_WRITE_LOCK(contextp);
            VN_LOCK(vp);
            vn_locked = TRUE;

            if (mode == CS_DIRECTIO) {
                /*
                 * Don't allow direct I/O if the file is mmapped, 
                 * data logging, or metadata opened via .tags.
                 */
                if ( vp->v_flag & VDIRECTIO ) {
                    /* If DIRECTIO is already set, just return SUCCESS. */
                    ;     
                } else if ( (!(vp->v_flag & VMMAPPED)) &&
                            (bfap->dataSafety != BFD_FTX_AGENT) &&
                            (bfap->dataSafety != BFD_FTX_AGENT_TEMPORARY) &&
                           !(bfap->real_bfap &&
                             bfap->real_bfap->dataSafety == BFD_FTX_AGENT) ) {
                    vp->v_flag |= VDIRECTIO;
                    VN_UNLOCK(vp);
                    vn_locked = FALSE;
                    bfflush(bfap, 0, 0, FLUSH_IMMEDIATE);
                } else {
                    retval = EACCES;
                }
            } else if (mode == CS_CACHE) {
                /* The dead code for reverting to cached policy from
                 * directIO has been removed.  This code is not reached
                 * because of the 'break' above.  If this capablility
                 * is to be reactivated, consider using an active range
                 * for the entire file to block directIO threads while
                 * changing the open mode.
                 */
                MS_SMP_ASSERT( mode != CS_CACHE );
            }

            /* Unlock the file */
            if (vn_locked) 
                VN_UNLOCK(vp);
            FS_FILE_UNLOCK(contextp);

        }
        break;
    }
    case GETMAP:
    {

        /*
         * File type must be VREG.
         */
        if (vp->v_type != VREG) {
            retval = EBADF;
            break;
        }

        retval = advfs_get_extent_map(VTOA(vp), (struct extentmap *) data);
        break;
    }

#ifdef ADVFS_NO_ZERO_ALLOCATE
    /* special code for quickly allocating storage in test filesystems.
     * storage is assigned and reused without being written or cleared.
     */
    case ADVFS_TESTSTORAGE:
    {
        struct stg_alloc *stg = (struct stg_alloc *)data;
        ssize_t count;
        struct uio uio;

        /* assess check specified for special customer release
         * is not used for test kernel so we can stress test quotas
         */
#if 0
        if (suser(cred, NULL)) { /* only root permitted to read any block */
            retval = EACCES;
            break;
        }
#endif
        if (!stg || stg->stgReserved != 0) {
            retval = EINVAL;
            break;
        }

        uio.uio_iovcnt = 1;
        uio.uio_rw = UIO_WRITE;
        uio.uio_segflg = UIO_SYSSPACE;
        uio.uio_offset = stg->stgOffset;
        count = uio.uio_resid = stg->stgLength;
        retval = fs_write(vp, &uio, ADVFS_IO_NO_ZERO, cred);

        /* If some space allocation happened, then we should
         * report the bytes allocated count rather than the retval.
         */
        if (retval) {
            if (uio.uio_resid != count)
                retval = 0;
            else if (retval == EPIPE)
                thread_doexception(current_thread(),
                                   EXC_SOFTWARE, EXC_UNIX_BAD_PIPE, 0);
        }
        count -= uio.uio_resid;
        stg->stgBytesAlloc = (long)count;
        break;
    }
#endif /* ADVFS_NO_ZERO_ALLOCATE */
    default:
        retval = ENOTTY;
    }
    return(retval);
}


/*
 * msfs_fsync
 *
 * used to sync a file (both data and stats).
 * Also used by NFS write gather to sync a file's metadata
 * (fflags = FWRITE_METADATA)
 */

msfs_fsync(
           struct vnode *vp,    /* in - vnode to fsync */
           int fflags,          /* in - flags */
           struct ucred *cred,  /* in - credentials of caller */
           int waitfor          /* in - wait flag */
           )
{
    statusT sts=0;

    FILESETSTAT( vp, msfs_fsync );

    NFS_FREEZE_LOCK(vp);
    sts = fs_fsync(vp, fflags, cred, waitfor);
    NFS_FREEZE_UNLOCK(vp);
    return (sts);
}


fs_fsync(
           struct vnode *vp,    /* in - vnode to fsync */
           int fflags,          /* in - flags */
           struct ucred *cred,  /* in - credentials of caller */
           int waitfor          /* in - wait flag */
           )
{
    struct mount *mp;
    statusT saved_dkResult, ret;
    struct fsContext *file_context;
    bfAccessT *bfap;
    domainT *dmnP;
    ftxHT ftx_handle;

    FILESETSTAT( vp, msfs_fsync );

    mp = VTOMOUNT(vp);
    file_context = VTOC(vp);
    bfap = VTOA(vp);
    dmnP = GETDOMAINP(mp);
    saved_dkResult = bfap->dkResult;

    if ((fflags&(FWRITE_DATA | FWRITE_METADATA)) ==
        (FWRITE_DATA | FWRITE_METADATA)) {
        fflags &= ~(FWRITE_DATA | FWRITE_METADATA);
    }

    /*
     * Take a share lock to block other writers, but no need to block
     * against other readers.
     */
    FS_FILE_READ_LOCK_RECURSIVE(file_context);
    if (!(fflags & FWRITE_METADATA)) {
        /*
         * sync file data
         */
        ret = bfflush(bfap, 0, 0, FLUSH_INTERMEDIATE);
        if (ret != EOK)
        {
            ret = EIO;
            goto _exit;
        }
    }

    if (!(fflags & FWRITE_DATA)) {

        if (file_context->dirty_stats == TRUE) {

            file_context->dirty_alloc = FALSE;
            /*
             * sync metadata
             */
            ret = FTX_START_N(FTA_OSF_FSYNC_V1, &ftx_handle,
                              FtxNilFtxH, dmnP, 11);
            if (ret != EOK)
            {
                ret = EIO;
                goto _exit;
            }

            ret = fs_update_stats(vp, bfap, ftx_handle, 0);
            if (ret != EOK)
            {
                ftx_done_n(ftx_handle, FTA_OSF_FSYNC_V1);
                ret = EIO;
                goto _exit;
            }

            /*
             * Do a syncronous write of the stats (for NFS).
             */
            ftx_special_done_mode(ftx_handle, FTXDONE_LOGSYNC);

            ftx_done_n(ftx_handle, FTA_OSF_FSYNC_V1);
        }
    }
    if (EOK != saved_dkResult)
    {
        ret = EIO;
    }
    else
    {
        ret = EOK;
    }

_exit:
    bfap->dkResult = EOK;
    FS_FILE_UNLOCK_RECURSIVE(file_context);
    return(ret);
}


/*
 * msfs_seek
 *
 * seek on a file
 * nothing to do here
 */
msfs_seek(
          struct vnode *vp,     /* in - vnode of file */
          off_t oldoff,         /* in? - old offset */
          off_t newoff,         /* in - new offset */
          struct ucred *cred    /* in - callers credentials */
          )
{
    FILESETSTAT( vp, msfs_seek );

    if ((long) newoff < 0) {
        return(EINVAL);
    } else {
        return (0);
    }
}


/*
 * msfs_remove
 *
 * remove a file
 */
msfs_remove(
            struct nameidata *ndp  /* in - nameidata structure pointer */
            )
{
    statusT sts;
    struct bfNode *bnp, *dir_bnp, *undel_bnp;
    struct vnode *dvp = NULL, *undel_dir_vp = NULL, *rvp = NULL;
    struct fsContext *rem_context, *dir_context, *undel_context;
    bfAccessT *bfap;
    bfAccessT *idx_bfap = NULL;
    ftxHT ftx_handle;
    bfSetT *bfSetp;
    struct mount *mp;
    domainT *dmnP;
    struct nameidata *undel_ndp = NULL;
    struct utask_nd undel_utnd;
    struct undel_dir_rec bmt_rec;
    int error = 0;
    int type;
    struct fileSetNode *fsnp;
    bfTagT save_dir_tag, save_rem_tag;
    int undel_ndp_refed = FALSE;

    FILESETSTAT( ndp->ni_dvp, msfs_remove );


    undel_ndp = (struct nameidata *) ms_malloc( sizeof( struct nameidata ));

    mp = VTOMOUNT(ndp->ni_vp);
    dmnP = GETDOMAINP(mp);
    bfSetp = GETBFSETP(mp);

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        vrele(ndp->ni_vp);
        vrele(ndp->ni_dvp);
        ms_free( undel_ndp );
        return EIO;
    }

    dvp = ndp->ni_dvp;
    bnp = (struct bfNode *)&ndp->ni_vp->v_data[0];
    dir_bnp = (struct bfNode *)&dvp->v_data[0];
    bfap = VTOA(dvp);
    dir_context = dir_bnp->fsContextp;
    rem_context = bnp->fsContextp;

    /*
     * Check to see if this is an attempt to remove a quota file.
     * The last link to a quota file is never unlinked.
     */
    fsnp = GETFILESETNODE(VTOMOUNT(ndp->ni_vp));
    FS_FILE_WRITE_LOCK(rem_context);
    for (type = 0; type < MAXQUOTAS; type++) {
        /*
         * Never allow removal of quota files if it is the last reference to 
         * it. Modified.  Someone might create a hard link to it, 
         * and there should be a way to remove that link.  As long as there is
         * one reference to the actual tag file, then everything should be fine
         */
        if (BS_BFTAG_EQL(bnp->tag, fsnp->qi[type].qiTag) && 
                rem_context->dir_stats.st_nlink < 2) {
            FS_FILE_UNLOCK(rem_context);
            vrele(ndp->ni_vp);
            vrele(ndp->ni_dvp);
            ms_free( undel_ndp );
            return(EACCES);
        }
    }

    FS_FILE_UNLOCK(rem_context);
    NFS_FREEZE_LOCK(ndp->ni_vp);

remove_it:

    /*
     * check to see if the directory of the to-be-deleted file has an
     * undelete directory. if so, we are really doing a rename...
     */
    if (!BS_BFTAG_NULL(dir_context->undel_dir_tag)) {
       goto rename_it;
    }

remove_it_1:
    FS_FILE_WRITE_LOCK(dir_context);
    FS_FILE_WRITE_LOCK(rem_context);

    if (ndp->ni_dirstamp != dir_context->dirstamp
                || (ndp->ni_nameiop & BADDIRSTAMP)) {
        int ret;
        bfTagT found_bs_tag;
        fs_dir_entry *dir_buffer;
        rbfPgRefHT page_ref;

        ret = seq_search(
                         ndp->ni_dvp,
                         ndp,
                         &found_bs_tag,
                         &dir_buffer,
                         &page_ref,
                         REF_PAGE,        /* ref the page*/
                         FtxNilFtxH
                         );

        if (ret == I_FILE_EXISTS) {
            /*
             * the file name exists; still need to make sure it still
             * refers to our bitfile
             */

            if (!BS_BFTAG_EQL( rem_context->bf_tag, found_bs_tag )) {
                /*
                 * the file was deleted and recreated so the file name now
                 * refers to a new/different bitfile
                 */

                error = ENOENT;
                goto _bail_out;
            }

        } else if (ret == EIO) {
            error = EIO;
            goto _bail_out;

        } else if (ret == I_INSERT_HERE) {
            /* the file disappeared - clean up and get out */
            error = ENOENT;
            goto _bail_out;

        } else {
            /* unknown status return from seq_search() */
            error = EIO;
            goto _bail_out;
        }
    }

    /*
     * start a transaction.  Note: if not running under CFS,
     * ndp->ni_xid must be zero to guarantee uniqueness of
     * ftxId in the transaction log.
     */

    /* Prevent races with migrate code paths */
    /* we don't need to worry about clones, because they are
     * read-only, and we cannot remove a file from a clone.
     */
    MS_SMP_ASSERT(bfap->bfSetp->cloneId == BS_BFSET_ORIG);
    /* Need index lock because of call chain
     * remove_dir_ent --> idx_directory_insert_space --> 
     * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
     * rbf_add_stg
     */
    IDX_GET_BFAP(bfap, idx_bfap);
    if (idx_bfap != NULL) { 
        MIGTRUNC_LOCK_READ( &(idx_bfap->xtnts.migTruncLk) );
    }

    /* No need to take the quota locks here. Storage is added
     * to the quota files only at file creation, chown and
     * explicit quota setting.
     * The potential danger path is:
     * rbf_delete --> chk_blk_quota -->dqsync
     * but it is tame in this case
     */
    
    sts = FTX_START_XID(FTA_OSF_REMOVE_V1, &ftx_handle, FtxNilFtxH,
                        dmnP, 8, ndp->ni_xid);
    if (sts != EOK) {
        ADVFS_SAD1("Error starting transaction in msfs_remove", sts);
    }

    /*
     * remove the entry from the directory.
     */
    /*
     * this routine cannot be ftx_failed after the call to remove_dir (ie
     * from here on)
     */

    /*
     * decr the file's link count and remove it's directory entry. If
     * the link count is not 0, then update it's stats (with new ctime)
     */

    --rem_context->dir_stats.st_nlink;

    if (rem_context->dir_stats.st_nlink == 0) {
        /*
         * Remove the file's entry from the namei cache.  Do this before
         * calling remove_dir_ent() to prevent races with lookups after
         * the file has been unlinked.
         */
        cache_purge(ndp->ni_vp);

        /*
         * Mark the bitfile for delete.  It will get cleaned up when
         * the last deaccessor goes away.  It is done prior to
         * remove_dir_ent to avoid potential pinblock deadlock on the
         * same bmt page.
         */
        rbf_delete(VTOA(ndp->ni_vp), ftx_handle);
        mutex_lock( &rem_context->fsContext_mutex );
        rem_context->fs_flag |= M_DELETE;
        mutex_unlock( &rem_context->fsContext_mutex );
        sts = remove_dir_ent( ndp, ftx_handle );
        if (sts != EOK) 
        {
            mutex_lock( &rem_context->fsContext_mutex );
            rem_context->fs_flag &= ~M_DELETE;
            mutex_unlock( &rem_context->fsContext_mutex );
            goto _error_out;
        }

    } else {
        sts = remove_dir_ent( ndp, ftx_handle );
        if (sts != EOK) 
        {
            goto _error_out;
        }
        mutex_lock( &rem_context->fsContext_mutex );
        rem_context->fs_flag |= MOD_CTIME;
        rem_context->dirty_stats = TRUE;
        mutex_unlock( &rem_context->fsContext_mutex );

        sts = fs_update_stats(ndp->ni_vp, VTOA(ndp->ni_vp), ftx_handle, 0);
        if (sts != EOK) {
            ADVFS_SAD1("Error from fs_udpate_stats in msfs_remove", sts);
        }
    }

    /*
     * complete the transaction
     */

    ftx_done_fs(ftx_handle, FTA_OSF_REMOVE_V1, 0);

    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }

    /*
     * unlock the file and dir
     */

    FS_FILE_UNLOCK(rem_context);
    FS_FILE_UNLOCK(dir_context);

    NFS_FREEZE_UNLOCK(ndp->ni_vp);

    /*
     * release the file and dir vnodes
     */
    vrele(ndp->ni_vp);
    vrele(ndp->ni_dvp);

    ms_free( undel_ndp );
/*    event_post(&xxbp.b_iocomplete);*/


    return(0);

_error_out:
    /*
     * unlock the file and dir
     */
    rem_context->dir_stats.st_nlink++;

    /* fail the ftx before unlocking files */
    ftx_fail(ftx_handle);

    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }

    FS_FILE_UNLOCK(rem_context);
    FS_FILE_UNLOCK(dir_context);

    NFS_FREEZE_UNLOCK(ndp->ni_vp);

    /*
     * release the file and dir vnodes
     */
    vrele(ndp->ni_vp);
    vrele(ndp->ni_dvp);
    ms_free( undel_ndp );

    return (BSERRMAP(sts));

    /*
     * do a rename instead of a remove
     */

rename_it:

    /*
     * bf_get the undelete directory. If bf_get fails, then just go
     * back and do a remove (someone deleted the undelete dir on us).
     */
    sts = bf_get(
                 dir_context->undel_dir_tag,
                 0,
                 DONT_UNLOCK,
                 mp,
                 &undel_dir_vp
                 );

    if ((sts != EOK) || ((VTOA(undel_dir_vp))->deleteWithClone)) {
        /*
         * the undel dir is gone.
         * detach it from this dir, and do a normal remove.
         */

        dir_context->undel_dir_tag = NilBfTag;
        bmt_rec.dir_tag = NilBfTag;

        sts = bmtr_put_rec(
                           bfap,
                           BMTR_FS_UNDEL_DIR,
                           (void *)&bmt_rec,
                           sizeof(struct undel_dir_rec),
                           FtxNilFtxH
                           );
        if (sts != EOK) {
            /* most likely an i/o error; ignore it; update not critical */
        }

        goto remove_it;
    }

    /*
     * Make sure that the user has permission to write
     * into the undelete directory.
     */
    error = msfs_access(undel_dir_vp, S_IWUSR, ndp->ni_cred);
    if (error != EOK) {
        /*
         * failure to vrele() these 3 vnodes results in EBUSY on umount.
         */
        vrele(ndp->ni_vp);
        vrele(ndp->ni_dvp);
        vrele(undel_dir_vp);
        goto _finish;
    }

    /*
     * create a nameidata structure for the undelete directory
     * and fill in enough stuff so that msfs_rename thinks it went
     * thru namei
     * NOTE:  nddup() does a VREF( ndp->ni_cdir ).  This is normally
     * undone via a vrele() in ndrele() which we call at the end.
     * However, since we stuff the undel dir vp into ni_cdir we
     * need to do the vrele() of the current ni_cdir ourselves.
     * The vrele() in ndrele() will deref the undel dir vp.
     */
    undel_ndp->ni_utnd = &undel_utnd;
    nddup(ndp, undel_ndp);
    undel_ndp_refed = TRUE;

    /*
     * set the dir vp, and NULL the file vp
     * fill in the name
     */
    undel_ndp->ni_dvp = undel_dir_vp;
    vrele( undel_ndp->ni_cdir );
    undel_ndp->ni_cdir = undel_dir_vp;
    undel_ndp->ni_vp = NULL;
    VREF(undel_dir_vp);
    undel_ndp->ni_dent.d_namlen = ndp->ni_dent.d_namlen;
    undel_ndp->ni_dirp = (char *)ms_malloc(NAME_MAX);

    (void)bcopy(
                   ndp->ni_dent.d_name,
                   undel_ndp->ni_dirp,
                   ndp->ni_dent.d_namlen
                   );
    
    {
        int i;

        /* Set up the following fields in the undel_ndp so that the call 
         * to insert_seq inside msfs_rename() will either create a namecache 
         * entry for this file, or, if a negative-cache entry is already 
         * present, it will be removed.
         */
        i = vfs_name_hash(ndp->ni_dent.d_name, undel_ndp->ni_dent.d_name,
                         &undel_ndp->ni_hash);
        undel_ndp->ni_ptr = undel_ndp->ni_dirp;
        if (i >= 0) {
            undel_ndp->ni_namelen = i;
            undel_ndp->ni_nameiop = CREATE;
            undel_ndp->ni_makeentry = 1;
        }
    }

    /*
     * make the ndp dirstamp of the undel directory smaller than its fsContext
     * dirstamp to force msfs_rename (it may go new, but that;s OK...).
     * to do a lookup of the 'mving' file in the undel dir. there may
     * already be a file of that name, in which case we want to mv on
     * top of it.
     */
    undel_bnp = (struct bfNode *)&undel_dir_vp->v_data[0];
    undel_context = undel_bnp->fsContextp;
    FS_FILE_READ_LOCK(undel_context);
    undel_ndp->ni_dirstamp = undel_context->dirstamp;
    undel_ndp->ni_nameiop &= ~BADDIRSTAMP;
    --undel_ndp->ni_dirstamp;
    FS_FILE_UNLOCK(undel_context);
    /*
     * do the rename
     */
    save_dir_tag = dir_context->bf_tag;
    save_rem_tag = rem_context->bf_tag;
    if (error = msfs_rename(ndp, undel_ndp)) {
        if (error == EEXIST) {
            if (rem_context->dir_stats.st_nlink > 1) {
                ms_free(undel_ndp->ni_dirp);
                ndrele(undel_ndp);
                sts = bf_get(
                        save_dir_tag,
                        0,
                        DONT_UNLOCK,
                        mp,
                        &dvp
                        );

                if (sts != EOK) {
                    ms_free( undel_ndp );
                    NFS_FREEZE_UNLOCK(ndp->ni_vp);
                    return (sts);
                }
                sts = bf_get(
                        save_rem_tag,
                        0,
                        DONT_UNLOCK,
                        mp,
                        &rvp
                        );

                if (sts != EOK) {
                    vrele(dvp);
                    ms_free( undel_ndp );
                    NFS_FREEZE_UNLOCK(ndp->ni_vp);
                    return (sts);
                }
                dir_bnp = (struct bfNode *)&dvp->v_data[0];
                bfap = VTOA(dvp);
                dir_context = dir_bnp->fsContextp;
                ndp->ni_dvp = dvp;
                ndp->ni_vp = rvp;
                bnp = (struct bfNode *)&ndp->ni_vp->v_data[0];
                rem_context = bnp->fsContextp;
                ndp->ni_dirstamp = dir_context->dirstamp;
                ndp->ni_nameiop &= ~BADDIRSTAMP;
                --ndp->ni_dirstamp;
                goto remove_it_1;
            }
        }
    }

_finish:
    /*
     * release any vnodes?
     */
    ms_free(undel_ndp->ni_dirp);
    if (undel_ndp_refed)
        ndrele(undel_ndp);

    ms_free( undel_ndp );
    NFS_FREEZE_UNLOCK(ndp->ni_vp);
    return (error);

_bail_out:
    FS_FILE_UNLOCK(dir_context);
    FS_FILE_UNLOCK(rem_context);
    NFS_FREEZE_UNLOCK(ndp->ni_vp);
    vrele(ndp->ni_vp);
    vrele(ndp->ni_dvp);
    ms_free( undel_ndp );

}


/*
 * msfs_link
 *
 * Create a hard link to a file.
 *
 */
msfs_link(
          register struct vnode *vp,       /* in - vnode is existing
                                              file to link to */
          register struct nameidata *ndp   /* in - nameidata structure */
          )
{
    bfAccessT *bfap;
    bfAccessT *dir_bfap;
    bfAccessT *idx_bfap = NULL;
    struct fsContext *file_context, *dir_context;
    struct bfNode *bnp, *dbnp;
    struct vnode *dvp = NULL;
    statusT ret;
    struct mount *mp;
    bfSetT *bfSetp;
    domainT *dmnP;
    ftxHT ftx_handle;
    u_int effective_LINK_MAX;

    FILESETSTAT( ndp->ni_dvp, msfs_link );

    mp = VTOMOUNT(vp);
    dmnP = GETDOMAINP(mp);
    bfSetp = GETBFSETP(mp);
    bnp = (struct bfNode *)&vp->v_data[0];
    bfap = VTOA(vp);
    file_context = bnp->fsContextp;
    dvp = ndp->ni_dvp;
    dir_bfap = VTOA(dvp);
    dbnp = (struct bfNode *)&dvp->v_data[0];
    dir_context = dbnp->fsContextp;

    NFS_FREEZE_LOCK(vp);

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        vrele(dvp);
        NFS_FREEZE_UNLOCK(vp);
        return EIO;
    }

    /* make sure it's not a DIR */
    if (vp->v_type == VDIR) {
        vrele(dvp);
        NFS_FREEZE_UNLOCK(vp);
        return(EPERM);
    }


    FS_FILE_WRITE_LOCK(dir_context);
    FS_FILE_WRITE_LOCK(file_context);

    /* make sure the link count is OK */

    if (file_context->dir_stats.st_nlink == 0) {
        FS_FILE_UNLOCK(file_context);
        FS_FILE_UNLOCK(dir_context);
        vrele(dvp);
        NFS_FREEZE_UNLOCK(vp);

        return(ENOENT);
    }

    /*
     * Set the value of effective_LINK_MAX:
     *    DVN3 uses signed short st_nlink, so the limit  is 32K minus change.
     *    DVN4 (or later) with large links disabled uses unsigned short,
     *    so the limit is 64K minus change.
     *    DVN4 (or later) with large links enabled uses unsigned int,
     *    and the limit is set between 64K and 4G in fs_dir.h.
     */

    if ( dmnP->dmnVersion < FIRST_64K_LINK_MAX_VERSION )
        effective_LINK_MAX = _LINK_MAX_SHORT;
    else
        effective_LINK_MAX = _LINK_MAX_USHORT;

    if (file_context->dir_stats.st_nlink >= effective_LINK_MAX) {
        FS_FILE_UNLOCK(file_context);
        FS_FILE_UNLOCK(dir_context);
        vrele(dvp);
        NFS_FREEZE_UNLOCK(vp);
        return(EMLINK);
    }

    /*
     * start a transaction.  Note: if not running under CFS,
     * ndp->ni_xid must be zero to guarantee uniqueness of
     * ftxId in the transaction log.
     */

    /* prevent races between storage and migrate code paths */
    /* we don't need to worry about clones, because they are read-only */

    MS_SMP_ASSERT(dir_bfap->bfSetp->cloneId == BS_BFSET_ORIG);

    /* Need to take the directory lock because of the call
     * chain
     * insert_seq --> rbf_add_stg
     */
    MIGTRUNC_LOCK_READ( &(dir_bfap->xtnts.migTruncLk) );

    /* Need to take the fileset tag directory lock because
     * of the call chain
     *
     * insert_seq --> idx_convert_dir --> idx_create_index_file -->
     * idx_create_index_file_int --> rbf_create --> rbf_int_create -->
     * tagdir_alloc_tag --> init_next_tag_page-->rbf_add_stg
     */
    MIGTRUNC_LOCK_READ( &(dir_bfap->bfSetp->dirBfAp->xtnts.migTruncLk) );

    /* Need to take the index lock because of the call
     * chain
     * insert_seq --> idx_directory_get_space -->
     * idx_directory_insert_space --> idx_directory_insert_space_int -->
     * idx_index_get_free_pgs_int --> rbf_add_stg
     */

    IDX_GET_BFAP(dir_bfap, idx_bfap);
    if (idx_bfap != NULL) { 
        MIGTRUNC_LOCK_READ( &(idx_bfap->xtnts.migTruncLk) );
    }

    /* No need to take the quota locks. The link may add space
     * to the directory but it is owned by whoever owned the
     * original link, so there is an entry in the quota file
     * already.
     * The potential danger path is
     * insert_seq --> chk_blk_quota --> dqsync --> rbf_add_stg
     * but it is tame in this case.
     */

    ret = FTX_START_XID(FTA_OSF_LINK_V1, &ftx_handle, FtxNilFtxH,
                        dmnP, 9, ndp->ni_xid);
    if (ret != EOK) {
        ADVFS_SAD1("Error starting transaction in msfs_link", ret);
    }
    /*
     * up the link count of the file. Do this to the in-memory stats
     * only so that it doesn't have to be backed out if we ftx_fail
     * later.
     */
    file_context->dir_stats.st_nlink++;
    /*
     * create the new directory entry.
     */
    ret = insert_seq(
                     dvp,
                     file_context->bf_tag,
                     vp,
                     HARD_LINK,
                     ndp,
                     bfSetp,
                     mp,
                     ftx_handle
                     );

    /*
     * expect I_FILE_EXISTS, ENOENT or ENO_MORE_BLOCKS from
     * insert_seq.  return others (could be EIO).
     */

    if (ret != EOK) {
        if (ret == I_FILE_EXISTS) {
            ret = EEXIST;
        }

        --file_context->dir_stats.st_nlink;
        ftx_fail(ftx_handle);
        FS_FILE_UNLOCK(file_context);
        FS_FILE_UNLOCK(dir_context);

        if (idx_bfap != NULL) {
            MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
        }
        MIGTRUNC_UNLOCK( &(dir_bfap->bfSetp->dirBfAp->xtnts.migTruncLk) );
        MIGTRUNC_UNLOCK( &(dir_bfap->xtnts.migTruncLk) );

        vrele(dvp);

        NFS_FREEZE_UNLOCK(vp);
        return( BSERRMAP( ret ) );
    }
    /*
     * can't ftx_fail after this.
     */

    /*
     * update the ctime field of the existing file. This will also
     * write the up'ed link count to the file.
     */
    mutex_lock( &file_context->fsContext_mutex );
    file_context->fs_flag |= MOD_CTIME;
    file_context->dirty_stats = TRUE;
    mutex_unlock( &file_context->fsContext_mutex );
    ret = fs_update_stats(vp, bfap, ftx_handle, 0);
    if (ret != EOK) {
        ADVFS_SAD1("Error from fs_udpate_stats in msfs_link", ret);
    }
    /* update the mtime and ctime fields for the dir */
    mutex_lock( &dir_context->fsContext_mutex );
    dir_context->fs_flag |= (MOD_CTIME | MOD_MTIME);
    dir_context->dirty_stats = TRUE;
    dir_context->dirstamp++;
    mutex_unlock( &dir_context->fsContext_mutex );

    ret = fs_update_stats(dvp, VTOA(dvp), ftx_handle, 0);
    if (ret != EOK) {
        ADVFS_SAD1("Error from fs_udpate_stats 1 in msfs_link", ret);
    }

    ftx_done_fs(ftx_handle, FTA_OSF_LINK_V1, 0);

    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }
    MIGTRUNC_UNLOCK( &(dir_bfap->bfSetp->dirBfAp->xtnts.migTruncLk) );
    MIGTRUNC_UNLOCK( &(dir_bfap->xtnts.migTruncLk) );

    /* finish any directory truncation that might have started in insert_seq */

    dir_trunc_finish(dvp);

    /*
     * Put an entry into the namei cache for the newly created link.
     */
    if (ndp->ni_makeentry) {
        ndp->ni_vp = vp;
        ndp->ni_ptr = ndp->ni_dent.d_name;
        cache_enter(ndp);
        ndp->ni_vp = NULLVP;
    }

    FS_FILE_UNLOCK(file_context);
    FS_FILE_UNLOCK(dir_context);
    vrele(dvp);

    NFS_FREEZE_UNLOCK(vp);
    return(0);
}


#define REL_VNODES(_f, _t, _sts) {             \
        if (_f->ni_dvp) vrele(_f->ni_dvp);     \
        if (_f->ni_vp) vrele(_f->ni_vp);       \
        if (_t->ni_dvp) vrele(_t->ni_dvp);     \
        if (_t->ni_vp) vrele(_t->ni_vp);       \
        RAISE_EXCEPTION(_sts);                 \
}
/*
 * msfs_rename
 *
 * rename (mv) a file or directory
 */
msfs_rename(
            register struct nameidata *fndp, /* in - 'from' file */
            register struct nameidata *tndp  /* in - 'to'  file */
            )
{
    struct vnode *from_vp = NULL, *from_dir_vp = NULL, *to_vp = NULL,
    *to_dir_vp = NULL, *removed_to_vp = NULL;
    struct bfNode *from_bnp, *from_dir_bnp, *to_dir_bnp;
    struct bfNode *tbnp, *ttbnp;
    struct fsContext *from_file_context, *from_dir_context,
    *to_file_context=NULLCP, *to_dir_context;
    bfAccessT *from_accessp, *to_accessp, *to_dir_accessp, *from_dir_accessp;
    bfAccessT *from_idx_bfap = NULL, *to_idx_bfap = NULL;
    int dir_mv = 0;
    bfSetT *bfSetp;
    struct mount *mp, *from_mp;
    ftxHT ftx_handle;
    domainT *dmnP;
    statusT ret, sts;
    int stripslash = 0;
    int error;
    fs_dir_entry *dir_buffer;
    char *p;
    bfTagT found_bs_tag, oldparent_tag, newparent_tag,
    keep_to_dir_tag, *tagp;
    rbfPgRefHT page_ref;
    int type;
    struct fileSetNode *fsnp, *from_fsnp;
    struct vnode *save_dvp = NULLVP, *save_vp = NULLVP;
    u_int effective_LINK_MAX;
    boolean_t to_dir_mig_trunc_lock = FALSE,
              to_file_context_locked = FALSE,
              from_dir_mig_trunc_lock = FALSE;
    bfTagT to_tag;

    FILESETSTAT( fndp->ni_dvp, msfs_rename );

    oldparent_tag = NilBfTag;
    newparent_tag = NilBfTag;
    /*
     * set up the 'from' dir and file contexts
     */
    from_vp = fndp->ni_vp;
    from_accessp = VTOA(from_vp);
    from_dir_vp = fndp->ni_dvp;
    from_bnp = (struct bfNode *)&from_vp->v_data[0];
    from_dir_bnp = (struct bfNode *)&from_dir_vp->v_data[0];
    from_file_context = from_bnp->fsContextp;
    from_dir_context = from_dir_bnp->fsContextp;
    from_dir_accessp = VTOA(from_dir_vp);

    from_mp = VTOMOUNT(from_vp);
    from_fsnp = GETFILESETNODE(from_mp);

    /*
     * now some 'to' dir stuff
     */

    to_dir_vp = tndp->ni_dvp;
    to_dir_bnp = (struct bfNode *)&to_dir_vp->v_data[0];
    to_dir_accessp = VTOA(to_dir_vp);
    to_dir_context = to_dir_bnp->fsContextp;

    mp = VTOMOUNT(to_dir_vp);
    bfSetp = GETBFSETP(mp);
    dmnP = GETDOMAINP(mp);
    fsnp = GETFILESETNODE(mp);

    NFS_FREEZE_LOCK(fndp->ni_vp);

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
      REL_VNODES(fndp, tndp, EIO);
    }

    /*
     * don't allow .tags to be moved
     */

    if (BS_BFTAG_EQL(from_file_context->bf_tag,
                     from_fsnp->tagsTag)) {
        if (BS_BFTAG_EQL(from_dir_context->bf_tag,
                         from_fsnp->rootTag)) {
          REL_VNODES(fndp, tndp, EACCES);
        }
    }

    if (tndp->ni_vp) {

        to_tag = ((struct bfNode *)&tndp->ni_vp->v_data[0])->tag;

        /* Don't allow .tags to be overwritten. */
        if (BS_BFTAG_EQL(to_tag, from_fsnp->tagsTag)) {
          REL_VNODES(fndp, tndp, EACCES);
        }

        /* Check if this is an attempt to rename on top of "." */
        if (tndp->ni_dvp == tndp->ni_vp) {
          REL_VNODES(fndp, tndp, EINVAL);
        }

        /*
         * Check to see if this is an attempt to rename on top of a
         * quota file.
         */
        to_tag = ((struct bfNode *)&tndp->ni_vp->v_data[0])->tag;

        for (type = 0; type < MAXQUOTAS; type++) {
            if (BS_BFTAG_EQL(to_tag, fsnp->qi[type].qiTag)) {
              REL_VNODES(fndp, tndp, EACCES);
            }
        }
    }

    /*
     * Make sure this is not a clone fileset and
     * make sure the to and from directories are in the same bfSet
     */

    if ((fsnp->fsFlags & FS_CLONEFSET) ||
        !BS_BFS_EQL(from_dir_bnp->bfSetId, to_dir_bnp->bfSetId)) {
        if (fsnp->fsFlags & FS_CLONEFSET) {
          REL_VNODES(fndp, tndp, EROFS);
        } else {
          REL_VNODES(fndp, tndp, EINVAL);
        }
    }
/*
    event_init(&xxbp.b_iocomplete);
    event_clear(&xxbp.b_iocomplete);
    event_wait(&xxbp.b_iocomplete, FALSE, 0);
*/


again:
    if (from_dir_accessp < to_dir_accessp) {
        FS_FILE_WRITE_LOCK(from_dir_context);
        FS_FILE_WRITE_LOCK(to_dir_context);
    } else {
        if (from_dir_accessp == to_dir_accessp) {
            FS_FILE_WRITE_LOCK(from_dir_context);
        } else {
            FS_FILE_WRITE_LOCK(to_dir_context);
            FS_FILE_WRITE_LOCK(from_dir_context);
        }
    }

    if (tndp->ni_vp) {
        to_file_context = VTOC(tndp->ni_vp);
    }

    /*
     * if 'from' is a directory, disallow '.' and '..'
     */
    if ((from_file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
        register struct dirent *dirp = &fndp->ni_dent;
        if ((dirp->d_namlen == 1 && dirp->d_name[0] == '.') ||
            (from_vp == from_dir_vp) || fndp->ni_isdotdot) {
            error = EINVAL;
            goto err_unlock_dirs;
        }
    }
    FS_FILE_WRITE_LOCK(from_file_context);

    /*
     * if from file was deleted, bail out
     */

    if (from_file_context->dir_stats.st_nlink == 0) {
        error = ENOENT;
        goto err_unlock_ffile;
    }

    /*
     * if it's a dir, save the parent
     */
    if ((from_file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
        oldparent_tag = from_dir_context->bf_tag;
        dir_mv = TRUE;
        stripslash = STRIPSLASH;
        cache_purge(from_vp);
    }
    /*
     * check to see that the 'from' file is still around (someone
     * could have deleted it between lookup and here, in which case
     * we are done after some cleanup). Note that even if someone
     * deleted it, we still have a from_file_context because our lookup
     * accessed the bitfile...
     */

    if (fndp->ni_dirstamp != from_dir_context->dirstamp
                || (fndp->ni_nameiop & BADDIRSTAMP)) {
        ret = seq_search(
                         from_dir_vp,
                         fndp,
                         &found_bs_tag,
                         &dir_buffer,
                         &page_ref,
                         REF_PAGE,        /* ref the page*/
                         FtxNilFtxH
                         );
        if (ret == EIO) {
            /* if EIO clean up and get out */
            error = EIO;
            goto err_unlock_ffile;
        }
        if (ret == I_INSERT_HERE) {
            /* the file disappeared - clean up and get out */
            error = ENOENT;
            goto err_unlock_ffile;
        }

        if (!BS_BFTAG_EQL(from_file_context->bf_tag, found_bs_tag)) {
            error = ENOENT;
            goto err_unlock_ffile;
        }
    }

    /*
     * to_vp is NULL if no output file, or vp if there is one...
     */
    to_vp = NULL;
    if (tndp->ni_vp) {

        /* Ensure that the source and destin are not the same.  This test
         * is needed to solve a possible race condition between link and
         * rename regarding the target file.  The target does not exist
         * initially, but it is found the second time (see 'someone created
         * the file on us' below).  Since the target was newly created,
         * we did a goto 'again:'.  We now need to make sure this new file
         * is not the same as the source file.
         */

        if(tndp->ni_vp == fndp->ni_vp) {
            error = ESUCCESS;
            goto err_unlock_ffile;
        }

        to_vp = tndp->ni_vp;
        to_file_context = VTOC( to_vp );
        to_accessp = VTOA(to_vp);
    }

    /*
     * check to see if a directory is getting a new parent. If so,
     * then check to see that the source dir is not in the hierarchy
     * of the 'to' dir. Also check for write permission of source to
     * alter it's "..".
     */

    if (!BS_BFTAG_EQL(oldparent_tag,
                      to_dir_context->bf_tag)) {
        newparent_tag = to_dir_context->bf_tag;
    }
    if (dir_mv && !BS_BFTAG_NULL(newparent_tag)) {
        if (error = msfs_access(from_vp, S_IWRITE, tndp->ni_cred)) {
            goto err_unlock_ffile;
        }
        do {
            tbnp = (struct bfNode *)&tndp->ni_dvp->v_data[0];
            keep_to_dir_tag = tbnp->tag;
            if (error = check_path_back(from_vp, to_dir_vp,
                                        tndp->ni_cred)) {
                goto err_unlock_ffile;
            }
            to_vp = NULL;
            if (tndp->ni_vp) {
                to_vp = tndp->ni_vp;
            }
            ttbnp = (struct bfNode *)&tndp->ni_dvp->v_data[0];
        } while (!BS_BFTAG_EQL(keep_to_dir_tag, ttbnp->tag));
    }
    /*
     * check the status of the 'to' file. someone may have created or
     * deleted it between lookup and here. It's OK if so, but we need
     * to change what is going on if that happened...
     */
    if (tndp->ni_dirstamp != to_dir_context->dirstamp
                || (tndp->ni_nameiop & BADDIRSTAMP)) {

        /* if dirstamp matches, we know it couldn't have changed...*/
        ret = seq_search(
                         to_dir_vp,
                         tndp,
                         &found_bs_tag,
                         &dir_buffer,
                         &page_ref,
                         REF_PAGE,        /* ref the page*/
                         FtxNilFtxH
                         );
        /* if EIO get out */
        if (ret == EIO) {
            error = EIO;
            goto err_unlock_ffile;
        }
        if (ret == I_INSERT_HERE) {
            /* there is no output file - check this against
               the status when rename was called */
            if (to_vp != NULL) {
                /* there was a file before - that's ok, it's
                   gone, just proceed */
                /*
                 * We need to vrele(to_vp), but, in a cluster the
                 * path vrele()->vdealloc()->getnewvnode()... can end
                 * up in bs_close_one() starting a transaction and
                 * locking locks.  Instead of doing now, remember to 
                 * vrele() on the way out when the directories have been 
                 * unlocked.  Will not "goto again" once to_vp is NULL.
                 */
                removed_to_vp = to_vp;
                tndp->ni_vp = NULL;
                to_vp = NULL;
            }

            /* save # useful pages in nameidata for possible truncation */
            tndp->ni_endoff++;
        } else {
            if (ret == I_FILE_EXISTS) {
                if (to_vp == NULL) {
                    /* someone created the file on us */
                    /* do another namei on it, and try again... */
call_namei:
                    FS_FILE_UNLOCK(from_file_context);
                    FS_FILE_UNLOCK(from_dir_context);
                    if (from_dir_accessp != to_dir_accessp) {
                        FS_FILE_UNLOCK(to_dir_context);
                    }
                    vrele(to_dir_vp);
                    tndp->ni_nameiop =
                    RENAME | WANTPARENT | NOCACHE | stripslash;
                    if (error = namei(tndp)) {
                        /* an error out of namei is unexpected...*/
                        goto err_rele_vnodes;
                    }
                    /*
                     * the to_dir_vp could have been recycled during the namei
                     * because we had to vrele it before the call to namei, so
                     * if it changed, reset it.
                     */

                    if (clu_is_ready()) {
                        int error = 0;
                        save_dvp = tndp->ni_dvp;
                        error = CC_GETPFSVP(save_dvp, &tndp->ni_dvp);
                        if (error) {
                            error = ENOTSUP;
                            goto err_rele_vnodes;
                        }
                        
                        save_vp = tndp->ni_vp;
                        error = CC_GETPFSVP(save_vp, &tndp->ni_vp);
                        if (error) {
                            error = ENOTSUP;
                            goto err_rele_vnodes;
                        }
                    }

                    if (to_dir_vp != tndp->ni_dvp) {
                        to_dir_vp = tndp->ni_dvp;
                    }
                    goto again;
                } else {
                    /*
                     * well there was a file before, and there is one
                     * now. But make sure it is the SAME file (ie
                     * someone could have deleted the original and
                     * then created a new one of the same name. In
                     * that case we need to namei the new one.)
                     */
                    if (!BS_BFTAG_EQL(to_file_context->bf_tag, found_bs_tag)) {
                        /*
                         * just let go of the 'old' file, and call go
                         * back to call namei
                         */
                        if (clu_is_ready()) {
                            if (save_vp)
                                vrele(save_vp);
                            else 
                                vrele(to_vp);
                        } else 
                            vrele(to_vp);
                        goto call_namei;
                    }
                    /* it's the same to-file, but it could have moved */
                }
            } else {
                /* got some unexpected error from search... */
                ADVFS_SAD1("Unknown return value from seq_search in msfs_rename",
                         ret);
            }
        }
    }


    if (to_vp == NULL) {
    /*
     * There was no target file,
     * so insert the new entry in the directory.
     * The call is made with the access handle for the 'to' directory,
     * the bf tag from the 'from' file's fsContext, and the nameidata
     * pointer for the 'to' file for the new file name.
     */
        /*
         * start the transaction now... Note: if not running under CFS,
         * fndp->ni_xid must be zero to guarantee uniqueness of
         * ftxId in the transaction log.
         */

        /* Prevent races with migrate code paths */
        /* we don't need to worry about clones, because they are read-only */

        MS_SMP_ASSERT(from_dir_accessp->bfSetp->cloneId == BS_BFSET_ORIG);
        MS_SMP_ASSERT(to_dir_accessp->bfSetp->cloneId == BS_BFSET_ORIG);

        /* Need to take the to_dir lock because of the call chain
         * insert_seq --> rbf_add_stg on it.
         * this lock is not taken in the else branch of this if, so
         * remember that we have taken it, so we can unlock it at
         * the end.
         */
        MIGTRUNC_LOCK_READ( &(to_dir_accessp->xtnts.migTruncLk) );

        /* Need to take the lock on the ``to'' fileset tag directory
         * because of the call chain
         * insert_seq --> idx_convert_dir-->idx_create_index_file_int -->
         * rbf_create --> rbf_int_create --> tagdir_alloc_tag -->
         * init_next_tag_page --> rbf_add_stg
         */
        MIGTRUNC_LOCK_READ( &(to_dir_accessp->bfSetp->dirBfAp->
                              xtnts.migTruncLk) );
        /* Need to take the migtrunc lock on the ``to'' directory index
         * (if it exists), because of the call chain
         * insert_seq --> idx_insert_filename --> idx_insert_filename_int -->
         * idx_index_get_free_pgs_int --> rbf_add_stg
         */
        IDX_GET_BFAP(to_dir_accessp, to_idx_bfap);
        if (to_idx_bfap != NULL) { 
            MIGTRUNC_LOCK_READ( &(to_idx_bfap->xtnts.migTruncLk) );
        }
        /* this flag remembers all three ``to'' mt locks */
        to_dir_mig_trunc_lock = TRUE;

        /* Need to take the index lock on the from directory index
         * because of the call chain
         * remove_dir_ent --> idx_directory_insert_space -->
         * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
         * rbf_add_stg
         * Also, don't take the same read lock twice due to deadlocks.
         */
        IDX_GET_BFAP(from_dir_accessp, from_idx_bfap);
        if ((from_idx_bfap != NULL) && (from_idx_bfap != to_idx_bfap)) { 
            MIGTRUNC_LOCK_READ( &(from_idx_bfap->xtnts.migTruncLk) );
            from_dir_mig_trunc_lock = TRUE;
        }

        /* No need to take the quota locks. Storage is only added
         * to them at file creation, chown and explicit quota setting.
         * rename cannot change the owner, so it does not add storage
         * to the quota files.
         * The potential danger path is
         * insert_seq --> chk_bl_quota --> dqsync --> rbf_add_stg
         * but it is tame in this case.
         */

        ret = FTX_START_XID(FTA_OSF_RENAME_V1, &ftx_handle, FtxNilFtxH,
                            dmnP, 6, fndp->ni_xid);

        if (ret != EOK) {
            ADVFS_SAD1("error starting ftx in rename", ret);
        }

        /*
         * if moving a dir to a new parent up the parents link count
         * this gets put out to disk later...
         */
        if (dir_mv &&!BS_BFTAG_NULL(newparent_tag)) {
            /*
             * Set the value of effective_LINK_MAX:
             *    DVN3 uses signed short st_nlink, so the limit 
             *    is 32K minus change.
             *    DVN4 (or later) with large links disabled uses 
             *    unsigned short, so the limit is 64K minus change.
             *    DVN4 (or later) with large links enabled uses 
             *    unsigned int, and the limit is set between
             *    64K and 4G in fs_dir.h.
             */

            if ( dmnP->dmnVersion < FIRST_64K_LINK_MAX_VERSION )
                effective_LINK_MAX = _LINK_MAX_SHORT;
            else
                effective_LINK_MAX = _LINK_MAX_USHORT;

            if (to_dir_context->dir_stats.st_nlink >= effective_LINK_MAX) {
                error = EMLINK;
                goto err_fail_ftx;
            }
            to_dir_context->dir_stats.st_nlink++;
        }

        /* now do the actual dir insert... */
        ret = insert_seq(
                         to_dir_vp,
                         from_file_context->bf_tag,
                         from_vp,
                         START_FTX, /* do as a sub-trans */
                         tndp,
                         bfSetp,
                         mp,
                         ftx_handle
                         );
        if (ret != EOK) {
            if (ret == I_FILE_EXISTS) {
                error = EEXIST;
            } else {
                error = BSERRMAP(ret);
            }
            if (dir_mv &&!BS_BFTAG_NULL(newparent_tag)) {
                to_dir_context->dir_stats.st_nlink--;
            }
            goto err_fail_ftx;
        }

        /* Remove the old dir entry. */

        ret = remove_dir_ent( fndp,
                              ftx_handle);
        if (ret != EOK) {
            error = BSERRMAP(ret);
            if (dir_mv &&!BS_BFTAG_NULL(newparent_tag)) {
                to_dir_context->dir_stats.st_nlink--;
            }
            goto err_fail_ftx;
            
        }

        /*
         * update the dirstamp
         */
        to_dir_context->dirstamp++;

        /*
         * update the 'from' fsContext area with new name and parent
         * note that 'from_file_context' has become the 'to' fsContext.
         * write the new fsContext to disk. Do this as a
         * sub-transaction so the stats will not be updated if we crash.
         */
#ifdef ADVFS_DEBUG
        if (tndp->ni_dent.d_namlen < 30) {
            (void)bcopy(
                           tndp->ni_dent.d_name,
                           from_file_context->file_name,
                           tndp->ni_dent.d_namlen + 1
                           );
        } else {
            (void)bcopy(
                           tndp->ni_dent.d_name,
                           from_file_context->file_name,
                           29
                           );
            from_file_context->file_name[29] = '\0';
        }
#endif
        /*
         * update the parent dir tag in the stats
         */
        from_file_context->dir_stats.dir_tag = to_dir_context->bf_tag;
        mutex_lock(&from_file_context->fsContext_mutex);
        from_file_context->fs_flag |= MOD_CTIME;
        from_file_context->dirty_stats = TRUE;
        mutex_unlock(&from_file_context->fsContext_mutex);

        ret = fs_update_stats(from_vp, from_accessp, ftx_handle, 0);
        if (ret != EOK) {
            domain_panic(dmnP, "msfs_rename: fs_udpate_stats failed with status %d", ret);
            error = ret;
            goto err_fail_ftx;
        }

    } else {
        /* to_vp != NULL */
        /*
         * there is a 'to' file.
         *
         * if the 'to' directory is sticky, the user must own
         * either the 'to' directory or the file being renamed 'on
         * top of' or it can;t be done
         */
        if ((to_dir_context->dir_stats.st_mode
             & S_ISVTX) && tndp->ni_cred->cr_uid != 0 &&
            tndp->ni_cred->cr_uid !=
            to_dir_context->dir_stats.st_uid) {
            FS_FILE_READ_LOCK(to_file_context);
            if (to_file_context->dir_stats.st_uid
                != tndp->ni_cred->cr_uid) {
                FS_FILE_UNLOCK(to_file_context);

                error = EPERM;
                goto err_nofail_ftx; /* unlock the to-dir */
            } else {
                FS_FILE_UNLOCK(to_file_context);
            }
        }
        /*
         * if 'to' is a dir, make sure it is empty
         */
        if ((to_file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
            if (to_file_context->dir_stats.st_nlink != 2) {
                error = EEXIST;
                goto err_nofail_ftx;
            }

            FS_FILE_READ_LOCK(to_file_context);

            ret = dir_empty(to_accessp, to_file_context);
            if (ret != EOK) {
                if (ret == I_FILE_EXISTS) {
                    error = EEXIST;
                }
                FS_FILE_UNLOCK(to_file_context);
                goto err_nofail_ftx;
            }

            cache_purge(from_dir_vp);
            cache_purge(to_vp);

            FS_FILE_UNLOCK(to_file_context);
        }
        FS_FILE_WRITE_LOCK(to_file_context);
        to_file_context_locked = TRUE;

        /*
         * start the transaction now... Note: if not running under CFS,
         * fndp->ni_xid must be zero to guarantee uniqueness of
         * ftxId in the transaction log.
         */

        /* Prevent races with migrate code paths */
        /* we don't need to worry about clones, because they are read-only */
        
        MS_SMP_ASSERT(from_dir_accessp->bfSetp->cloneId == BS_BFSET_ORIG);
        MS_SMP_ASSERT(to_dir_accessp->bfSetp->cloneId == BS_BFSET_ORIG);

        /* Need to take the lock on the index of the from directory
         * because of the call chain
         * remove_dir_ent--> idx_directory_insert_space -->
         * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
         * rbf_add_stg
         */
        IDX_GET_BFAP(from_dir_accessp, from_idx_bfap);
        if (from_idx_bfap != NULL) { 
            MIGTRUNC_LOCK_READ( &(from_idx_bfap->xtnts.migTruncLk) );
            from_dir_mig_trunc_lock = TRUE;
        }

        /* No need to take the quota locks - decr_link_count
         * cannot add storage to the quota files.
         */
        ret = FTX_START_XID(FTA_OSF_RENAME_V1, &ftx_handle, FtxNilFtxH,
                            dmnP, 6, fndp->ni_xid);

        if (ret != EOK) {
            ADVFS_SAD1("error starting ftx in rename", ret);
        }

        /* Remove the old dir entry. We need to do this prior to
         * pinning any records since there is a rare chance of
         * needing to fail the ftx.
         */

        ret = remove_dir_ent( fndp,
                              ftx_handle);
        if (ret != EOK) {
            error = BSERRMAP(ret);
            goto err_fail_ftx;
            
        }

        /*
         * get the page and offset from the to-file ndp for the to-dir
         * entry and update the bfTag
         */
        ret = rbf_pinpg(
                        &page_ref,
                        (void *)&dir_buffer,
                        to_dir_accessp,
                        tndp->ni_count,
                        BS_NIL,
                        ftx_handle
                        );
        if (ret != EOK) {
            ADVFS_SAD1("rbf_pinpg error in msfs_rename", ret);
        }
        /*
         * update the dir entry of the 'on top of file' to point
         * to the input (from) file (ie update the tag). note that at
         * this point, no one can lookup the 'on top of' file, since
         * no dir entry points to it any more. Also, we use the 'from'
         * fsContext area (it reflects the real file, the 'to' file
         * actually goes away, we only use it's dir entry)
         */
#ifdef ADVFS_DEBUG
        if (tndp->ni_dent.d_namlen < 30) {
            (void)bcopy(
                           tndp->ni_dent.d_name,
                           from_file_context->file_name,
                           tndp->ni_dent.d_namlen + 1
                           );
        } else {
            (void)bcopy(
                           tndp->ni_dent.d_name,
                           from_file_context->file_name,
                           29
                           );
            from_file_context->file_name[29] = '\0';
        }
#endif
        /* note - the to_file_context isn't locked here. I don't think that
           is a problem...*/

        p = (char *)dir_buffer;
        p += tndp->ni_offset;
        dir_buffer = (fs_dir_entry *)p;

        rbf_pin_record(
                       page_ref,
                       &dir_buffer->fs_dir_header.fs_dir_bs_tag_num,
                       sizeof(uint32T)
                       );
        dir_buffer->fs_dir_header.fs_dir_bs_tag_num =
        from_file_context->bf_tag.num;
        tagp = GETTAGP(dir_buffer);
        rbf_pin_record(
                       page_ref,
                       tagp,
                       sizeof(bfTagT)
                       );
        *tagp = from_file_context->bf_tag;
        to_dir_context->dirstamp++;
        cache_purge( to_vp );
        /*
         * update the dir tag in the stats
         */
        from_file_context->dir_stats.dir_tag = to_dir_context->bf_tag;

        /*
         * decr the link count on the 'on top of' file.
         * if the file still has a link count of < 0 (it had a hard
         * link to it) then write it's stats back.
         *
         * This is done prior to fs_update_stats to avoid potential pinblock
         * deadlocks on the same bmt page (between delete and update).
         */
        decr_link_count(to_vp, ftx_handle);
        if (to_file_context->dir_stats.st_nlink > 0) {
            to_accessp = VTOA(to_vp);
            mutex_lock(&to_file_context->fsContext_mutex);
            to_file_context->fs_flag |= MOD_CTIME;
            to_file_context->dirty_stats = TRUE;
            mutex_unlock(&to_file_context->fsContext_mutex);
            ret = fs_update_stats(to_vp, to_accessp, ftx_handle, 0);
            if (ret != EOK) {
                domain_panic(dmnP, "msfs_rename: fs_udpate_stats failed with status %d", ret);
                error = ret;
                goto err_fail_ftx;                     
            }
        }
        FS_FILE_UNLOCK(to_file_context);
        to_file_context_locked = FALSE;

        ret = fs_update_stats(from_vp, from_accessp, ftx_handle, 0);
        if (ret != EOK) {
            domain_panic(dmnP, "msfs_rename: fs_udpate_stats failed with status %d", ret);
            error = ret;
            goto err_fail_ftx;                     
        }
    } /* end of 'there is a to-file' */

    /*
     * update the ctime and mtime fields of the to-directory
     */
    mutex_lock( &to_dir_context->fsContext_mutex );
    to_dir_context->fs_flag |= (MOD_CTIME | MOD_MTIME);
    to_dir_context->dirty_stats = TRUE;
    mutex_unlock( &to_dir_context->fsContext_mutex );

    /*
     * put the to-dir stats out
     */
    ret = fs_update_stats(to_dir_vp, to_dir_accessp, ftx_handle, 0);
    if (ret != EOK) {
        domain_panic(dmnP, "msfs_rename: fs_udpate_stats failed with status %d", ret);
        error = ret;
        goto err_fail_ftx;                     
    }

    /*
     * now remove the file from the 'from' directory
     */

    /*
     * if the 'from' is a dir with a new parent, update it's ".."
     * entry to point to the new parent, and decr the old parents
     *  link count.
     */

    if (dir_mv && !BS_BFTAG_NULL(newparent_tag)) {
        from_dir_context->dir_stats.st_nlink--;
        ret = fs_update_stats(from_dir_vp, from_dir_accessp, ftx_handle, 0);
        if (ret != EOK) {
            domain_panic(dmnP, "msfs_rename: fs_udpate_stats failed with status %d", ret);
            error = ret;
            goto err_fail_ftx;               
        }
        /* ignore errors from new_parent */
        ret = new_parent(from_vp, ftx_handle, newparent_tag);
        cache_purge(from_dir_vp);
    }

    /*
     * all done
     */


    ftx_done_fs(ftx_handle, FTA_OSF_RENAME_V1, 0);

    if (from_dir_mig_trunc_lock) {
        MIGTRUNC_UNLOCK( &(from_idx_bfap->xtnts.migTruncLk) );
    }

    if (to_dir_mig_trunc_lock) {
        MIGTRUNC_UNLOCK( &(to_dir_accessp->xtnts.migTruncLk) );
        MIGTRUNC_UNLOCK( &(to_dir_accessp->bfSetp->dirBfAp->
                           xtnts.migTruncLk) );
        if (to_idx_bfap != NULL) {
            MIGTRUNC_UNLOCK( &(to_idx_bfap->xtnts.migTruncLk) );
        }
        to_dir_mig_trunc_lock = FALSE;
    }

    FS_FILE_UNLOCK(from_file_context);

    /* finish any directory truncation that might have been started
     * in insert_seq for the 'to' directory. Must be done outside the
     * just-committed transaction.
     */
    dir_trunc_finish(to_dir_vp);

    /*
     * Put a new entry into the namei cache for the renamed file.
     */
    tndp->ni_vp = from_vp;
    tndp->ni_ptr = tndp->ni_dent.d_name;
    cache_enter(tndp);
    tndp->ni_vp = to_vp;

    /*
     * unlock the dirs
     */
    FS_FILE_UNLOCK(to_dir_context);
    if (to_dir_accessp != from_dir_accessp) {
        FS_FILE_UNLOCK(from_dir_context);
    }
    /*
     * release all of the vnodes
     */
    if (clu_is_ready()) {
        if (save_dvp)
            vrele(save_dvp);
        else 
            vrele(to_dir_vp);
        if (to_vp != NULL) {
            if (save_vp)
                vrele(save_vp);
            else
                vrele(to_vp);
        }
    } else {
        vrele(to_dir_vp);
        if (to_vp != NULL) 
            vrele(to_vp);
    }

    NFS_FREEZE_UNLOCK(fndp->ni_vp);

    vrele(from_vp);
    vrele(from_dir_vp);

    if (removed_to_vp)
        vrele(removed_to_vp);

    return (0);

err_nofail_ftx:
    /* unlock the directories */
    FS_FILE_UNLOCK(to_dir_context);
    if (to_dir_accessp != from_dir_accessp) {
        FS_FILE_UNLOCK(from_dir_context);
    }
    FS_FILE_UNLOCK(from_file_context);
    goto nofail;

err_fail_ftx:
    /* fail the transaction */
    ftx_fail(ftx_handle);

    /* unlock the directories */
    FS_FILE_UNLOCK(to_dir_context);
    if (to_dir_accessp != from_dir_accessp) {
        FS_FILE_UNLOCK(from_dir_context);
    }

    if (to_file_context_locked) {
        FS_FILE_UNLOCK(to_file_context);
    }

    FS_FILE_UNLOCK(from_file_context);

    if (from_dir_mig_trunc_lock) { 
        MIGTRUNC_UNLOCK( &(from_idx_bfap->xtnts.migTruncLk) );
    }
    if (to_dir_mig_trunc_lock) {
        MIGTRUNC_UNLOCK( &(to_dir_accessp->xtnts.migTruncLk) );
        MIGTRUNC_UNLOCK( &(to_dir_accessp->bfSetp->dirBfAp->
                           xtnts.migTruncLk) );
        if (to_idx_bfap != NULL) {
            MIGTRUNC_UNLOCK( &(to_idx_bfap->xtnts.migTruncLk) );
        }
        to_dir_mig_trunc_lock = FALSE;
    }

    /* release vnodes */
nofail:

    NFS_FREEZE_UNLOCK(fndp->ni_vp);

    if (clu_is_ready()) {
        if (save_dvp)
            vrele(save_dvp);
        else
            vrele(to_dir_vp);
        vrele(from_dir_vp);
        if (to_vp) {
            if (save_vp)
                vrele(save_vp);
            else
                vrele(to_vp);
        }
    } else {
        vrele(to_dir_vp);
        vrele(from_dir_vp);
        if (to_vp) {
            vrele(to_vp);
        }
    }
    vrele(from_vp);

    if (removed_to_vp)
        vrele(removed_to_vp);

    return(error);

err_unlock_ffile:
    FS_FILE_UNLOCK(from_file_context);
err_unlock_dirs:
    /* unlock the 'from' directory and 'to' dirs */
    FS_FILE_UNLOCK(to_dir_context);
    if (to_dir_accessp != from_dir_accessp) {
        FS_FILE_UNLOCK(from_dir_context);
    }
    if (clu_is_ready()) {
        if (save_dvp)
            vrele(save_dvp);
        else
            vrele(tndp->ni_dvp);
    } else 
        vrele(tndp->ni_dvp);

err_rele_vnodes:

    NFS_FREEZE_UNLOCK(fndp->ni_vp);

    if (tndp->ni_vp) {
        if (clu_is_ready()) {
            if (save_vp)
                vrele(save_vp);
            else
                vrele(tndp->ni_vp);
        } else
            vrele(tndp->ni_vp);
    }
    vrele(from_dir_vp);
    vrele(from_vp);
    if (removed_to_vp)
        vrele(removed_to_vp);

    return (error);

HANDLE_EXCEPTION:

    NFS_FREEZE_UNLOCK(fndp->ni_vp);
    return(sts);

}

decr_link_count(
          struct vnode *vp,    /* in - vnode pointer */
          ftxHT ftx_handle     /* in - ftx handle */
          )
{
    struct fsContext *file_context;

    file_context = VTOC(vp);
    file_context->dir_stats.st_nlink--;
    if (vp->v_type == VDIR) {
        file_context->dir_stats.st_nlink--;
    }
    if (file_context->dir_stats.st_nlink == 0) {
        rbf_delete(VTOA(vp), ftx_handle);
        mutex_lock( &file_context->fsContext_mutex );
        file_context->fs_flag |= M_DELETE;
        mutex_unlock( &file_context->fsContext_mutex );
    }
}


/*
 * msfs_mkdir
 *
 * make a directory
 */
msfs_mkdir(
           struct nameidata *ndp,  /* in - nameidata structure pointer */
           struct vattr *vap       /* in - vnode attributes structure */
           )
{
    statusT ret;

    FILESETSTAT( ndp->ni_dvp, msfs_mkdir );

    NFS_FREEZE_LOCK(ndp->ni_dvp);

    /* create the file */
    ret = fs_create_file(vap, ndp);

    NFS_FREEZE_UNLOCK(ndp->ni_dvp);
}


/*
 * msfs_rmdir
 *
 * remove a directory
 */
msfs_rmdir(
           register struct nameidata *ndp  /* in - nameidata structure */
           )
{
    struct vnode *rvp = NULL, *dvp = NULL;
    bfAccessT *rem_access, *par_access;
    bfAccessT *idx_bfap = NULL;
    struct fsContext *rem_context, *par_context;
    struct bfNode *bnp;
    statusT ret, dirempty_ret;
    ftxHT ftx_handle;
    bfSetT *bfSetp;
    struct mount *mp;
    domainT *dmnP;
    int error = 0, fail_ftx = 0;
    int reset_rem = 0, reset_par = 0;
    boolean_t idx_mig_trunc_lock = FALSE;
    struct fileSetNode *fileSetNp;

    FILESETSTAT( ndp->ni_dvp, msfs_rmdir );

    /* the vp of dir to be removed */
    rvp = ndp->ni_vp;

    /* the vp of its parent dir */
    dvp = ndp->ni_dvp;

    /*
     * the directory being removed will be locked to prevent any changes to it
     * the parent directory was already write locked in lookup
     */

    rem_access = VTOA(rvp);
    par_access = VTOA(dvp);
    bnp = (struct bfNode *)&rvp->v_data[0];
    rem_context = bnp->fsContextp;
    bnp = (struct bfNode *)&dvp->v_data[0];
    par_context = bnp->fsContextp;
    fileSetNp = GETFILESETNODE(VTOMOUNT(rvp));
    mp = VTOMOUNT(ndp->ni_vp);
    dmnP = GETDOMAINP(mp);
    bfSetp = GETBFSETP(mp);

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        vrele( rvp );
        vrele( dvp );
        return EIO;
    }

    NFS_FREEZE_LOCK(rvp);

    if (rem_access < par_access) {
        FS_FILE_WRITE_LOCK(rem_context);
        FS_FILE_WRITE_LOCK(par_context);
    } else {
        FS_FILE_WRITE_LOCK(par_context);
        FS_FILE_WRITE_LOCK(rem_context);
    }

    /*
     * check for an empty directory and clone fileset
     */

    if (fileSetNp->fsFlags & FS_CLONEFSET) {
        error = EROFS;
        goto _error_cleanup;
    }
    dirempty_ret = dir_empty(rem_access, rem_context);
    if (dirempty_ret == I_FILE_EXISTS) {
        error = ENOTEMPTY;
        goto _error_cleanup;
    }
    /*
     * remove the directory. At this point dir_empty returned
     * either EIO (corrupted dir) or EOK (the dir is empty). In either case we
     * are going to ingore the link count on the directory and
     * allow it to be deleted.
     */

    /*
     * don't allow .tags in root to be removed
     */

    if (BS_BFTAG_EQL(rem_context->bf_tag,
                     fileSetNp->tagsTag)) {
        if (BS_BFTAG_EQL(par_context->bf_tag,
            fileSetNp->rootTag)) {

            error = EACCES;
            goto _error_cleanup;
        }
    }

    if (ndp->ni_dirstamp != par_context->dirstamp
                || (ndp->ni_nameiop & BADDIRSTAMP)) {
        int ret2;
        bfTagT found_bs_tag;
        fs_dir_entry *dir_buffer;
        rbfPgRefHT page_ref;

        ret2 = seq_search(
                         ndp->ni_dvp,
                         ndp,
                         &found_bs_tag,
                         &dir_buffer,
                         &page_ref,
                         REF_PAGE,        /* ref the page*/
                         FtxNilFtxH
                         );

        if (ret2 == I_FILE_EXISTS) {
            /*
             * the file name exists; still need to make sure it still
             * refers to our bitfile
             */

            if (!BS_BFTAG_EQL( rem_context->bf_tag, found_bs_tag )) {
                /*
                 * the file was deleted and recreated so the file name now
                 * refers to a new/different bitfile
                 */

                error = ENOENT;
                goto _error_cleanup;
            }

        } else if (ret2 == EIO) {
            error = EIO;
            goto _error_cleanup;

        } else if (ret2 == I_INSERT_HERE) {
            /* the file disappeared - clean up and get out */
            error = ENOENT;
            goto _error_cleanup;

        } else {
            /* unknown status return from seq_search() */
            error = EIO;
            goto _error_cleanup;
        }
    }

    /*
     * Remove the namei cache entry for the removed directory.
     */
    cache_purge(rvp);

    /*
     * decr the parent directory's link count
     */
    par_context->dir_stats.st_nlink--;
    mutex_lock( &par_context->fsContext_mutex );
    par_context->dirty_stats = TRUE;
    mutex_unlock( &par_context->fsContext_mutex );
    reset_par = TRUE;

    /*
     * start a transaction.  Note: if not running under CFS,
     * ndp->ni_xid must be zero to guarantee uniqueness of
     * ftxId in the transaction log.
     */

    /* Prevent races with migrate code paths */
    /* we don't need to worry about clones, because they are read-only */

    MS_SMP_ASSERT(par_access->bfSetp->cloneId == BS_BFSET_ORIG);

    /* Need to lock the index because of the call chain
     * remove_dir_ent --> idx_directory_insert_space -->
     * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
     * rbf_add_stg
     */
    IDX_GET_BFAP(par_access, idx_bfap);
    if (idx_bfap != NULL) { 
        MIGTRUNC_LOCK_READ( &(idx_bfap->xtnts.migTruncLk) );
    }

    /* No need to take the quota locks. rbf_delete will NOT
     * add storage to the quota files. The entry already
     * exists.
     */
    idx_mig_trunc_lock = TRUE;

    ret = FTX_START_XID(FTA_OSF_RMDIR_V1, &ftx_handle, FtxNilFtxH,
                        dmnP, 8, ndp->ni_xid);

    if (ret != EOK) {
        ADVFS_SAD1("Error starting transaction in msfs_rmdir", ret);
    }

    /*
     * Remove the namei cache entry for the removed directory's parent dir.
     */
    cache_purge(dvp);

    /*
     * decr the dir's link count by 2,
     * delete . and .. and mark the file for delete,
     * and delete the entry.  The bitfile delete is done first to
     * avoid potential bmt page pinblock deadlock with the update
     * stats in remove_dir_ent.
     */
    rem_context->dir_stats.st_nlink -= 2;
    reset_rem = TRUE;

    rbf_delete(VTOA(rvp), ftx_handle );

    mutex_lock( &rem_context->fsContext_mutex );
    rem_context->fs_flag |= M_DELETE;
    mutex_unlock( &rem_context->fsContext_mutex );

    /*
     * Remove the index if it is setup. We need to remove it now
     * in case we fail (we will domain panic) and can still call
     * ftx_fail. It isn't actually deleted until the last close.
     */

    if (IDX_INDEXING_ENABLED(VTOA(rvp)))
    {
        ret=idx_remove_index_file(VTOA(rvp),ftx_handle);
        if (ret != EOK)
        {
            error=ret;
            fail_ftx = TRUE;
            goto _error_cleanup;
        }
    }

    ret = remove_dir_ent(
                         ndp,
                         ftx_handle);
    if (ret != EOK) {
        mutex_lock( &rem_context->fsContext_mutex );
        rem_context->fs_flag &= ~M_DELETE;
        mutex_unlock( &rem_context->fsContext_mutex );
        fail_ftx = TRUE;
        error = BSERRMAP(ret);
        goto _error_cleanup;
    }

    /*
     * if the remove dots fails, just ignore it and
     * finish the transaction.
     */
    ret = remove_dots(rvp, ftx_handle);

    /*
     * finish the transaction
     */

    ftx_done_fs(ftx_handle, FTA_OSF_RMDIR_V1, 0);


    /* This close will disassociate the index from the directory */


    if (IDX_INDEXING_ENABLED(VTOA(rvp)))
    {
        idx_close_index_file(VTOA(rvp),IDX_REMOVING_DIR);
    }

    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }

    ndp->ni_dvp = NULL;

    /*
     * unlock dir and rem_dir
     */
    FS_FILE_UNLOCK(rem_context);
    FS_FILE_UNLOCK(par_context);

    NFS_FREEZE_UNLOCK(rvp);

    /*
     * release the deleted dir's vnode and the parent dir vnode
     */
    vrele(rvp);
    vrele(dvp);

    return(0);
    /*
     * only fail the ftx if fail_ftx flag is true.
     */
_error_cleanup:
    if (reset_par) {
    ++par_context->dir_stats.st_nlink;
    }
    if (reset_rem) {
    rem_context->dir_stats.st_nlink += 2;
    }

    /* fail the ftx before unlocking files */
    if (fail_ftx)
        ftx_fail(ftx_handle);

    if (idx_mig_trunc_lock) {
        if (idx_bfap != NULL) {
            MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
        }
    }

    FS_FILE_UNLOCK(rem_context);
    FS_FILE_UNLOCK(par_context);

    NFS_FREEZE_UNLOCK(rvp);

    vrele( rvp );
    vrele( dvp );

    return( error );
}


/*
 * msfs_symlink
 *
 * create a symbolic link
 */
msfs_symlink(
             struct nameidata *ndp,
             struct vattr *vap,
             char *target
             )
{
    statusT ret;

    FILESETSTAT( ndp->ni_dvp, msfs_symlink );

    NFS_FREEZE_LOCK(ndp->ni_dvp);

    vap->va_symlink = target;
    ret = fs_create_file(vap, ndp);

    NFS_FREEZE_UNLOCK(ndp->ni_dvp);
}


/*
 * msfs_readdir
 *
 * read a buffer full of directory entries
 */
msfs_readdir(
             struct vnode *vp,           /* in - vnode pointer of directory */
             register struct uio *uio,   /* in - uio structure */
             struct ucred *cred,         /* in - callers credentials */
             int *eofflagp               /* out - eof flag  */
             )
{
    int error;


    /* If number of bytes to read is 0 or negative, it is invalid */
    if (uio->uio_resid <= 0) {
        return(EINVAL);
    }
    FILESETSTAT( vp, msfs_readdir );


    error = fs_read(
                   vp,
                   uio,
                   0,
                   cred
                   );

    if (uio->uio_offset >= (VTOA(vp))->file_size) {
        *eofflagp = 1;
    } else {
        *eofflagp = 0;
    }

    return(error);
}


/*
 * msfs_readlink
 *
 * read the contents of a symbolic link
 */

msfs_readlink(
              struct vnode *vp,   /* in - vnode pointer of file to read */
              struct uio *uiop,   /* in - uio structure pointer */
              struct ucred *cred  /* in - credentials of caller */
              )
{
    bfAccessT *bfap;
    struct fsContext *context_ptr;
    ulong isize;
    int error;
    char *buffer = NULL;
    statusT sts;

    FILESETSTAT( vp, msfs_readlink );

    bfap = VTOA(vp);
    context_ptr = VTOC(vp);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }


    if (bfap->file_size > uiop->uio_resid) {
        error = ERANGE;
        goto _exit;
    }
    /* if it's a fast link, read it from the bmt record */
    if (((context_ptr->dir_stats.st_mode & S_IFMT) == S_IFLNK) &&
        (bfap->file_size <= bmtr_max_rec_size())) {

        buffer = (char *) ms_malloc( bmtr_max_rec_size() );

        isize = bfap->file_size;
        sts = bmtr_get_rec(bfap,
                           BMTR_FS_DATA,
                           (void *)buffer,
                           (u_short)isize
                           );
        if (sts != EOK) {
            error = BSERRMAP(sts);
        } else {
            error = uiomove(
                            buffer,
                            (long)isize,
                            uiop
                            );
        }
        mutex_lock( &context_ptr->fsContext_mutex );
        context_ptr->dirty_stats = TRUE;
        context_ptr->fs_flag |= MOD_ATIME;
        mutex_unlock( &context_ptr->fsContext_mutex );
        ms_free( buffer );
        goto _exit;
    }
    /* if it;s not a fast link, read it from the file */
    error = msfs_read(vp, uiop, 0, cred);
    if (buffer != NULL)
        ms_free( buffer );

_exit:
    return( error );
}


/*
 * msfs_abortop
 *
 * abort operation called by various vfs routines
 */
int
msfs_abortop(
             struct nameidata *ndp,  /* in - nameidata pointer */
             int ret_val             /* not used */
             )
{
    return 0;
}

int
msfs_bmap(void)
{
    return(ENOSYS);
}

/*
 * msfs_strategy()
 *
 * Description:
 *
 * Msfs_strategy() will perform an Async read or write using information
 * supplied in the buf struct.  It is assumed that this request is coming
 * from aio_rw() and that the user's buffer has already been mapped down.
 *
 */
msfs_strategy(struct buf *bp)
{
    struct file *fp;
    struct uio uio;
    struct iovec vec[3];
    struct vnode *vp;
    int retval;


    /*
     * Make sure we have a vp.
     */
    if (!bp->b_vp) {
        if (bp->b_iodone) {
            bp->b_error = EINVAL;
            bp->b_iodone(bp);
        }
        return(EINVAL);
    }

    /*
     * Save a copy of the vnode pointer for use after calling
     * the AIO completion routine.
     */
    vp = bp->b_vp;


    fp = (struct file *)bp->b_rvp;

    vec[0].iov_base = (caddr_t)bp->b_un.b_addr;
    vec[0].iov_len = bp->b_bcount;
    vec[1].iov_base = (caddr_t)vec;
    vec[1].iov_len = 0;

    /*
     * The third vector is used only by the AdvFS directIO code.
     * The iov_base is used to pass in the bp from aio_rw().
     * The iov_len is used as a flag to determine if there
     * is a queued I/O after fs_read/write() is called.
     */
    vec[2].iov_base = (caddr_t)bp;
    vec[2].iov_len = 0;

    /*
     * If these 2 fields are not of equal size, then care must be taken both
     * here and in aio_rw() that neither truncation nor improper sign
     * extenion occur.  At the very minimum 64 bits are needed otherwise
     * Async I/O operations will not be able to correctly access files
     * greater than several gigabytes.
     *
     * NOTE:  This is different from other Strategy routines.  Typically
     *        they are called to perform I/O on special or raw devices and
     *        not for I/O to files in a file system on a block device.  In
     *        other Strategy routines I/O is performed on sector boundaries
     *        (512 is typical of many systems) so they only pass
     *        the sector block number and not the true offset.  But for
     *        Direct I/O we need the originaly specified byte offset, since
     *        Direct I/O is allowed to non-sector boundaries. 
     *
     *        In fact we _MUST_ allow I/O to non-sector boundaries because
     *        the current implementation enables Direct I/O at the vnode
     *        level.  That means if an application was using Async buffered
     *        I/O operations and a 2nd application came along and opened the
     *        file for Direct I/O, the 1st application would now start to
     *        perform Direct I/O because the common vnode is where the
     *        Direct I/O flag exists.  Also it is possible to mount a file
     *        systems such that all I/Os are Direct I/Os, and in that case
     *        Direct I/O must be able to handle any applications request for
     *        I/O regardless of the offsets is requests. 
     *
     *  NOTE 2:  b_driver_un_1.longvalue may be changed by a driver, so 
     *           get the value out before passing the buf structure anywhere
     *           that might result in its value being changed. 
     */
    MS_SMP_ASSERT(sizeof(bp->b_driver_un_1.longvalue)==sizeof(uio.uio_offset));
    uio.uio_offset = (off_t)bp->b_driver_un_1.longvalue;
    bp->b_driver_un_1.longvalue = 0;

    uio.uio_iov = vec;
    uio.uio_iovcnt = 1;
    uio.uio_resid = bp->b_bcount;
    uio.uio_fmode = 0;
    uio.uio_limit = -1;
    uio.uio_rw = UIO_AIORW;
    uio.uio_segflg = UIO_SYSSPACE;

    /* Set bp's resid to requested transfer size. b_resid is set to 
     * zero in aio_rw(), but the difference between bp->b_bcount and
     * bp->b_resid is reported as the number of bytes transferred by
     * the aio interface routines.  So we set both to the same value
     * here and adjust the b_resid value in the read/write routines
     * as appropriate. 
     */
    bp->b_resid = bp->b_bcount;

    /* clear b_error so only an error modifies it in our I/O paths */
    bp->b_error = 0;

    if (bp->b_flags & B_READ) {
        retval = vn_pread(fp, &uio, bp->b_wcred);
    }
    else {
        retval = vn_pwrite(fp, &uio, bp->b_wcred);
    }

    /*
     * NOTE: These cleanup steps must occur ONLY ONCE and they
     * will normally be performed in the I/O completion paths.
     * We only do the cleanup here when no I/O's were initiated
     * as indicated by our vec[2].iov_len flag not being set.
     * NOTE: If an IO was spawned as part of the read/write, then
     * the aio routines may have free'd or reused the struct that
     * was passed to us as the struct buf; do not modify it here or
     * risk corrupting memory that is in use for some other purpose.
     */
    if (vec[2].iov_len == 0) {

        /* set resid so user can tell number of bytes transferred */
        bp->b_resid = uio.uio_resid;

        /* The AIO code will not accept a partial completion when
         * b_error is set, so it needs to be set to EOK to mimic a
         * normal read()/write().  Otherwise b_error will be either
         * an error retval or the b_error from the IO path.
         */
        if (bp->b_resid && bp->b_resid != bp->b_bcount &&
                /* don't allow partial read until we verify it works */
                !(bp->b_flags & B_READ) &&
                !(fp->f_flag & (O_SYNC | O_DSYNC))) 
            bp->b_error = retval = EOK;
        else if (retval)
            bp->b_error = retval;

        /* For DIO writes, we must decrement v_numoutput */
        if ( !(bp->b_flags & B_READ) &&
              (vp->v_flag & VDIRECTIO) ) {
            bs_aio_write_cleanup(bp->b_vp);
        }

        /* Call the AIO completion routine */
        bp->b_iodone(bp);
    }


    return(retval);
}

#ifndef ADVFS_DEBUG
msfs_print(
    struct vnode *vp
    )
{
}
#else
static char *vtagtypename[] =
   { "VT_NON", "VT_UFS", "VT_NFS", "VT_MFS", "VT_S5FS", "VT_CDFS",
      "VT_DFS", "VT_EFS", "VT_PRFS", "VT_MSFS" };

msfs_print(
    struct vnode *vp
    )
{
    struct bfNode *bp;
    struct fsContext *cp;
    bfAccessT *bfap;
    bfTagT tag;
    bfSetIdT setId;

    bp = (struct bfNode *)(&vp->v_data[0]);
    bfap = bp->accessp;
    cp = bp->fsContextp;
    tag = bp->tag;
    setId = bp->bfSetId;

    if (bfap != NULL && cp != NULL) {
        printf("%s: ", cp->file_name);
    }

    printf("type %s, tag <0x%x, 0x%x>, accessp 0x%x, dirTag.num 0x%x\n",
           vtagtypename[vp->v_tag],
           tag.num,
           tag.seq,
           bfap,
           setId.dirTag.num);
}
#endif

/*
 * MIN_BITS_CONSTANT and its helper MB_TST will calculate the number of bits
 * needed to hold the specified constant (a compile time constant is
 * perferred).  The bit count returned will be sized to include room for an
 * additional sign bit.  For example:  0x4000 would return 16.  15 bits would
 * hold an unsigned 0x4000, but 16 bits are required if there is to be room
 * for a sign bit.
 *
 * _DO_ _NOT_ use this MACRO for calculating the number of bits in a variable.
 * This MACRO is really intended to calculate the bits in a constant so that
 * all the work is done at compile time.  If a variable is used, then the
 * compiler will be forced to expand the MACRO into 64 individual tests where
 * each test will require about 5 instructions to implement, resulting in
 * over 320 instructions.
 */
#define MB_TST(_a,_b) (unsigned long)_a<((unsigned long)1L<<(_b-1)) ? _b :
#define MIN_BITS_CONSTANT(_c) \
    MB_TST(_c,1)  MB_TST(_c,2)  MB_TST(_c,3)  MB_TST(_c,4)  \
    MB_TST(_c,5)  MB_TST(_c,6)  MB_TST(_c,7)  MB_TST(_c,8)  \
    MB_TST(_c,9)  MB_TST(_c,10) MB_TST(_c,11) MB_TST(_c,12) \
    MB_TST(_c,13) MB_TST(_c,14) MB_TST(_c,15) MB_TST(_c,16) \
    MB_TST(_c,17) MB_TST(_c,18) MB_TST(_c,19) MB_TST(_c,20) \
    MB_TST(_c,21) MB_TST(_c,22) MB_TST(_c,23) MB_TST(_c,24) \
    MB_TST(_c,25) MB_TST(_c,26) MB_TST(_c,27) MB_TST(_c,28) \
    MB_TST(_c,29) MB_TST(_c,30) MB_TST(_c,31) MB_TST(_c,32) \
    MB_TST(_c,33) MB_TST(_c,34) MB_TST(_c,35) MB_TST(_c,36) \
    MB_TST(_c,37) MB_TST(_c,38) MB_TST(_c,39) MB_TST(_c,40) \
    MB_TST(_c,41) MB_TST(_c,42) MB_TST(_c,43) MB_TST(_c,44) \
    MB_TST(_c,45) MB_TST(_c,46) MB_TST(_c,47) MB_TST(_c,48) \
    MB_TST(_c,49) MB_TST(_c,50) MB_TST(_c,51) MB_TST(_c,52) \
    MB_TST(_c,53) MB_TST(_c,54) MB_TST(_c,55) MB_TST(_c,56) \
    MB_TST(_c,57) MB_TST(_c,58) MB_TST(_c,59) MB_TST(_c,60) \
    MB_TST(_c,61) MB_TST(_c,62) MB_TST(_c,63) MB_TST(_c,64) \
    65

/*
 * msfs_pathconf - to handle the _PC_FILESIZEBITS and _PC_ACL_EXTENDED, the 
 *                 rest gets passed to the common vn_pathconf.
 */
int
msfs_pathconf(
        register struct vnode *vp,
        int name,
        long *retval)
{
        bfAccessT *bfap;

        switch (name) {
            case _PC_FILESIZEBITS:
                /*
                 * _PC_FILESIZEBITS is looking for the minimum number of
                 * bits needed to represent the maximum size of a file in a
                 * signed varable.  max_offset is an offset and
                 * MIN_BITS_CONSTANT() needs a maximum file size.  In this
                 * case it would be max_offset+1, since max_offset says that
                 * you can lseek to it and still write 1 byte.
                 */
                *retval = MIN_BITS_CONSTANT(max_offset+1);
                return(0);
            case _PC_ACL_EXTENDED:
                *retval = (paclpolicy == ACLS_ENABLED);
                return(0);
            case _PC_LINK_MAX:
                /* See which LINK_MAX should be used. */
                bfap = VTOA(vp);
                if ( bfap->dmnP->dmnVersion < FIRST_64K_LINK_MAX_VERSION )
                    *retval = _LINK_MAX_SHORT;
                else
                    *retval = _LINK_MAX_USHORT;
                return(0);
            default:
                return (vn_pathconf(vp, name, retval));
        }
    /*
     * Satisfy the compiler.  Should never be reached.
     */
    MS_SMP_ASSERT(FALSE);
    return(0);
}


/*
 * msfs_page_read
 *
 *
 */
msfs_page_read(
               struct vnode *vp,   /* in - vnode of file */
               struct uio *uio,    /* in - uio struct */
               struct ucred *cred  /* in - callers credentials */
               )
{
    struct fsContext *file_context;
    bfAccessT *bfap;
    int size;       /* byte count to read */
    u_long bf_size; /* size of file */
    int readable_bytes; /* most that can be read from file */
    int phys;       /* physical flag */
    int bytes_read; /* bytes read so far */
    int error;
    int bytes_left;
    struct fileSetNode *fsnp;


    FILESETSTAT( vp, msfs_page_read );

    error = EINVAL;
    bfap = VTOA(vp);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }

    file_context = VTOC(vp);
    fsnp = GETFILESETNODE(VTOMOUNT(vp));
    size = uio->uio_resid;
    bytes_read = 0;
    phys = 0;
    if ((uio->uio_segflg == UIO_PHYSSPACE)) {
        phys++;
    }


    /*
     * check that what is requested to be read is in the file
     */

    bf_size = bfap->file_size;
    readable_bytes = bf_size - uio->uio_offset;
    if ((readable_bytes <= 0) || size <= 0 ) {

        return (error);
    }

    bytes_left = size;
    /*
     * if reading less than there actually is, the zero the page
     */
    if (phys && readable_bytes < size) {
        pmap_zero_page((vm_offset_t)uio->uio_iov->iov_base);
    }

    do {
        int page_to_read;    /* page # to read */
        int starting_byte;   /* starting byte within the page to read */
        int bytes_to_read;   /* # bytes within the page to read */
        bfPageRefHT page_ref;
        statusT ret;
        char *page_addr;

        /* the page # to read */

        page_to_read = uio->uio_offset / ADVFS_PGSZ;

        /* the byte within the page */

        if (uio->uio_offset < ADVFS_PGSZ) {
            starting_byte = uio->uio_offset;
        } else {
            starting_byte = uio->uio_offset % ADVFS_PGSZ;
        }
        /* don;t read past end of file */

        if (uio->uio_offset + size > bf_size) {
            bytes_left = bf_size - uio->uio_offset;
        }
        bytes_to_read = ADVFS_PGSZ - starting_byte;

        if (bytes_left < bytes_to_read) {
            bytes_to_read = bytes_left;
        }

        VN_LOCK(vp);
        if (page_to_read == vp->v_lastr + 1) {
            VN_UNLOCK(vp);
            ret = bs_refpg(
                           &page_ref,
                           (void *)&page_addr,
                           bfap,
                           page_to_read,
                           BS_SEQ_AHEAD  /* read ahead */
                           );
        } else {
            VN_UNLOCK(vp);
            ret = bs_refpg(
                           &page_ref,
                           (void *)&page_addr,
                           bfap,
                           page_to_read,
                           BS_NIL
                           );

        }
        VN_LOCK(vp);
        vp->v_lastr = page_to_read;
        VN_UNLOCK(vp);

        if (ret != EOK) {
            goto error1;
        }
        if (phys) {
            copy_to_phys(page_addr + starting_byte,
                         uio->uio_iov->iov_base + bytes_read,
                         bytes_to_read);
        } else {
            bcopy(page_addr + starting_byte,
                  uio->uio_iov->iov_base + bytes_read,
                  bytes_to_read);
        }
        bs_derefpg(page_ref, BS_NIL);

        bytes_read += bytes_to_read;
        uio->uio_offset += bytes_to_read;
        uio->uio_resid -= bytes_to_read;
        bytes_left -= bytes_to_read;

    } while (bytes_left > 0);

    if (!phys && (readable_bytes < size)) {
        bzero(uio->uio_iov->iov_base + readable_bytes,
              size - readable_bytes);
    }

    return (0);

error1:
    error = EIO;

msfs_page_read_exit:

    return (error);
}


/*
 * msfs_page_write
 *
 *
 */
msfs_page_write(
                struct vnode *vp,      /* in - vnode pointer */
                struct uio *uio,       /* in - uio structure pointer */
                struct ucred *cred,    /* in - callers credentials */
                memory_object_t pager, /* in - not used */
                vm_offset_t offset     /* in - not used */
                )
{
    int error;

    FILESETSTAT( vp, msfs_page_write );

    error = msfs_write(
                       vp,
                       uio,
                       0,
                       cred
                       );

        return error;
    }


/*
 * Read wrapper for special devices.
 */
msfsspec_read(
    struct vnode *vp,
    struct uio *uio,
    int ioflag,
    struct ucred *cred)
{
    struct fsContext *cp;
    int ret;


    cp = VTOC(vp);
    mutex_lock( &cp->fsContext_mutex );
    cp->fs_flag |= MOD_ATIME;
    cp->dirty_stats = TRUE;
    mutex_unlock( &cp->fsContext_mutex );

    ret = spec_read(vp, uio, ioflag, cred);

    return( ret );
}

/*
 * Write wrapper for special devices.
 */
msfsspec_write(
    struct vnode *vp,
    struct uio *uio,
    int ioflag,
    struct ucred *cred
    )
{
    struct fsContext *cp;
    int ret;


    NFS_FREEZE_LOCK(vp);

    cp = VTOC(vp);
    mutex_lock( &cp->fsContext_mutex );
    cp->fs_flag |= MOD_CTIME | MOD_MTIME;
    cp->dirty_stats = TRUE;
    mutex_unlock( &cp->fsContext_mutex );
    ret = spec_write(vp, uio, ioflag, cred);

    NFS_FREEZE_UNLOCK(vp);
    return( ret );
}

/*
 * Close wrapper for special devices.
 *
 * Update the times on the bitfile then do device close.
 */
msfsspec_close(
    struct vnode *vp,
    int fflag,
    struct ucred *cred)
{
    struct fsContext *cp;
    statusT ret;


    NFS_FREEZE_LOCK(vp);

    cp = VTOC(vp);
    BM(VN_LOCK(vp));
    if (vp->v_usecount > 1) {
        BM(VN_UNLOCK(vp));

        /* TODO This routine has to be fixed. The field cp->dirty_stats
           is not protected here by a fsContex_mutex as it is elsewhere.
           If a mutex is taken before the if then one in the routine
           fs_update_stats must be removed or unlocked in coordination
           with this mutex else the routine might attempt to lock it
           again.
        */
        if (cp->dirty_stats == TRUE) {
            /* ***NOTE - no ftx here for udpate_stats call */
            ret = fs_update_stats(vp, VTOA(vp), FtxNilFtxH,  0);
            if (ret != EOK) {
                ADVFS_SAD1("Error from fs_udpate_stats in msfsspec_close",
                         ret);
            }
        }
    }
    else {
        BM(VN_UNLOCK(vp));
    }
    ret = spec_close(vp, fflag, cred);

    NFS_FREEZE_UNLOCK(vp);
    return( ret );
}

/*
 * Revoke a (potentially referenced) vnode.
 *
 * Note that there MAY be VOP's in flight on this vnode!
 */
msfsspec_revoke(
    register struct vnode *vp,
    int flags
    )
{
    bfAccessT *bfap;
    bfSetT *bfSetp;
    struct bfNode *bnp;
    ulong hash_key;
    extern bfAccessT *find_bfap();
    extern struct vnodeops msfsrevoke_vnodeops;


    /*
     * Take the legs out from under this vnode.
     */
    vp->v_op = &msfsrevoke_vnodeops;

    /* Attribute Update?? */

    /*
     * Do the vnode-to-accessp hokey-pokey.
     */
    bnp = (struct bfNode *)&vp->v_data[0];

    if (!BS_BFS_EQL(bnp->bfSetId, nilBfSetId)) {
        bfSetp = bs_bfs_lookup_desc(bnp->bfSetId);
        if (BFSET_VALID(bfSetp)) {
            if (bfap = find_bfap( bfSetp, bnp->tag, TRUE, NULL)) {
                MS_SMP_ASSERT(vp == bfap->bfVp);
                BS_BFAH_REMOVE(bfap, FALSE);
                if (VTOA(vp) == NULL) {
                    /*
                     * We've removed the bfap from the hash queues so
                     * msfs_reclaim() won't be able to cleanup a bfap
                     * whose vnode was already inactivated when clearalias()
                     * did the vget_nowait().  In that case clear out the
                     * bfap->bfVp now.  This bfap can't be reused, only
                     * recycled or deallocated.
                     */
                    bfap->bfVp = NULL;

                    /*
                     * We have removed the bfAccess structure from the hash
                     * table, we've zapped the bfap->bfVp, so unless we
                     * flush the dirty stats ourselves, then this bfap could
                     * sit on the cleanup_closed_list() forever, effectively
                     * becoming a memory leak.  So flush the dirty stats
                     * so that we can put it on the clean list when
                     * DEC_REFCNT() occurs.
                     */
                    if (VTOC(vp) &&
                        VTOC(vp)->dirty_stats &&
                        bfap->bfState == BSRA_VALID) {

                        RM_ACC_LIST_COND( bfap );
                        bfap->refCnt++;
                        mutex_unlock( &bfap->bfaLock );
                        fs_update_stats(vp, bfap, FtxNilFtxH, 0);
                        mutex_lock( &bfap->bfaLock );
                        DEC_REFCNT( bfap );
                    }
                }
                mutex_unlock(&bfap->bfaLock);
            }
            else {
                /*
                 * Even though we didn't find the bfap, find_bfap() still
                 * locked the hash bucket for us.  We need to unlock it
                 * before we return.
                 */
                hash_key = BS_BFAH_GET_KEY(bfSetp, bnp->tag);
                BS_BFAH_UNLOCK(hash_key);
            }
        }
    }

    return (0);
}

/*
 * Reclaim a device node so that it can be used for other purposes.
 *
 * There must not be anyone else who knows about this node.
 */
msfsspec_reclaim(
    register struct vnode *vp,
    int flags
    )
{
    int error;

    /*
     * If this is really a revoke, call only msfs-specific revoke routine.
     */
    if (flags & VX_REVOKE)
        return (msfsspec_revoke(vp, flags));

    if (!(error = spec_reclaim(vp, flags)))
        error = msfs_reclaim(vp, flags);

    return(error);
}

/*
 * Read wrapper for fifos.
 */
msfsfifo_read(
    struct vnode *vp,
    struct uio *uio,
    int ioflag,
    struct ucred *cred
    )
{
    struct fsContext *cp;
    int ret;


    cp = VTOC(vp);
    mutex_lock( &cp->fsContext_mutex );
    cp->fs_flag |= MOD_ATIME;
    cp->dirty_stats = TRUE;
    mutex_unlock( &cp->fsContext_mutex );
    ret = fifo_read(vp, uio, ioflag, cred);

    return( ret );
}

/*
 * Write wrapper for fifos.
 */
msfsfifo_write(
    struct vnode *vp,
    struct uio *uio,
    int ioflag,
    struct ucred *cred
    )
{
    struct fsContext *cp;
    int ret;


    NFS_FREEZE_LOCK(vp);

    cp = VTOC(vp);
    mutex_lock( &cp->fsContext_mutex );
    cp->fs_flag |= MOD_CTIME | MOD_MTIME;
    cp->dirty_stats = TRUE;
    mutex_unlock( &cp->fsContext_mutex );
    ret = fifo_write(vp, uio, ioflag, cred);

    NFS_FREEZE_UNLOCK(vp);
    return( ret );
}

/*
 * Close wrapper for fifos.
 *
 * Update the times on the bitfile then do device close.
 */
msfsfifo_close(
    struct vnode *vp,
    int fflag,
    struct ucred *cred
    )
{
    struct fsContext *cp;
    statusT ret;


    NFS_FREEZE_LOCK(vp);

    cp = VTOC(vp);
    BM(VN_LOCK(vp));
    if (vp->v_usecount > 1) {
        BM(VN_UNLOCK(vp));
        if (cp->dirty_stats == TRUE) {
            /* NOTE - no ftx here */
            ret = fs_update_stats(vp, VTOA(vp), FtxNilFtxH, 0);
            if (ret != EOK) {
                ADVFS_SAD1("Error from fs_udpate_stats in msfsfifo_close", ret);
            }
        }
    }
    else {
        BM(VN_UNLOCK(vp));
    }
    ret = fifo_close(vp, fflag, cred);

    NFS_FREEZE_UNLOCK(vp);
    return( ret );
}

/*
 * getattr wrapper for fifos.
 */
msfsfifo_getattr(
    struct vnode *vp,
    register struct vattr *vap,
    struct ucred *cred
    )
{
    int error;

    /*
     * Get most attributes from the bitfile, rest
     * from the fifo.
     */
    if (error = msfs_getattr(vp, vap, cred)) {
        return (error);
    }
    error = fifo_getattr(vp, vap, cred);
    return( error );
}

/*
 * Reclaim wrapper for named MSFS pipes.
 */
int
msfsfifo_reclaim(struct vnode *vp, int flags)
{
    int error;

    /*
     * Call fifo_reclaim to free the allocated fifonode.
     */
    if (error = fifo_reclaim(vp, flags)) {
        return (error);
    }
    error = msfs_reclaim(vp, flags);
    return (error);
}


/*
 * msfs_chown
 *
 * chown operation on a file
 * this is not directly a vn op, but is called from msfs_setattr
 *
 * When quotas are compiled in, we must update the quota stats (in
 * quota files) and the file's uid/gid atomically.  This function
 * does update the quota files transactionally, but all other changes
 * are to in-memory structures.  The dir stats are marked as dirty, but
 * the dir stats can't wait to be updated at close time.  The caller
 * must call fs_update stats.
 */

static int
msfs_chown(
           struct vnode *vp,               /* in - vnode of bitfile */
           uid_t uid,                      /* in - uid to change to */
           gid_t gid,                      /* in - gid to change to */
           struct ucred *cred,             /* in - callers credentials */
           ftxHT parentFtxH,               /* in - parent transaction handle */
           int setctime                    /* in - setctime */
           )
{
    int error = 0;
    struct fsContext *file_context = VTOC(vp);


    if (uid == (uid_t)VNOVAL) {
        uid = file_context->dir_stats.st_uid;
    }
    if (gid == (uid_t)VNOVAL) {
        gid = file_context->dir_stats.st_gid;
    }

    /*
     * If the caller doesn't own the file, is trying to change the
     * owner of the file, or is not a member of the target group, then
     * the caller must be superuser.
     */
    if ((cred->cr_uid !=
         file_context->dir_stats.st_uid ||
         uid != file_context->dir_stats.st_uid ||
         !groupmember((gid_t)gid, cred)) &&
         (error = suser(cred, &u.u_acflag))) {
        return (error);
    }

    if ((error = advfs_quota_chown(vp, uid, gid, 0,
                                   cred, parentFtxH)) != EOK) {
        return (BSERRMAP(error));
    }

    file_context->dir_stats.st_uid = uid;
    file_context->dir_stats.st_gid = gid;

    if (cred->cr_uid != 0) {
        file_context->dir_stats.st_mode &= ~S_ISUID;
        file_context->dir_stats.st_mode &= ~S_ISGID;
    }

    if (setctime == SET_CTIME) {
        mutex_lock( &file_context->fsContext_mutex );
        file_context->fs_flag |= MOD_CTIME;
        file_context->dirty_stats = TRUE;
        mutex_unlock( &file_context->fsContext_mutex );
    }

    return (0);
}


/*
 * msfs_chmod
 *
 * chmod operation on a file
 * this is not directly a vn op, but is called from
 * msfs_setattr
 */
int
msfs_chmod(
           struct vnode *vp,               /* in - vnode of bitfile */
           struct fsContext *file_context, /* in - file context pointer */
           register int mode,              /* in - mode to change to */
           struct ucred *cred              /* in - callers credentials */
           )
{
    int error;

    /*
     * lock the in-memory stat structure
     */
    mutex_lock( &file_context->fsContext_mutex );

    /*
     * must be owner or suser
     */

    if (cred->cr_uid !=
        file_context->dir_stats.st_uid &&
        (error = suser(cred, &u.u_acflag))) {
        mutex_unlock( &file_context->fsContext_mutex );
        return (error);
    }

    file_context->dir_stats.st_mode &= ~07777;
    if (cred->cr_uid) {
        if ((file_context->dir_stats.st_mode &
             S_IFMT) != S_IFDIR) {
            mode &= ~S_ISVTX;
        }
        if
        (!groupmember(file_context->dir_stats.st_gid, cred)) {
            mode &= ~S_ISGID;
        }
    }

    file_context->dir_stats.st_mode |= mode & 07777;
    file_context->dirty_stats = TRUE;
    mutex_unlock( &file_context->fsContext_mutex );

    return (0);
}


/*
 * attach an undelete directory to a directory
 */
statusT
attach_undel_dir(
                 char *dir_path,
                 char *undel_dir_path
                 )
{
    statusT sts, ret;
    register struct nameidata *ndp = &u.u_nd;
    struct nameidata *undel_ndp = NULL;
    struct utask_nd undel_utnd;
    register struct vnode *dir_vp = NULL, *undel_dir_vp = NULL;
    register struct vnode *vp1 = NULLVP, *vp2 = NULLVP;
    struct mount *mp, *undel_mp;
    struct fileSetNode *fsnp, *undel_fsnp;
    struct bfNode *bnp, *undel_bnp;
    bfAccessT *bfap;
    struct fsContext *dir_context, *undel_dir_context;
    struct undel_dir_rec bmt_rec;


    undel_ndp = (struct nameidata *) ms_malloc( sizeof( struct nameidata ));

    /*
     * lookup the directory
     */
    ndp->ni_nameiop = LOOKUP | FOLLOW;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = dir_path;

    if (ret = namei(ndp)) {
        ms_free ( undel_ndp );
        return ret;
    }
    vp1 = ndp->ni_vp;
    dir_vp = ndp->ni_vp;

    if (clu_is_ready()) {
        if (ret = CC_GETPFSVP(vp1, &dir_vp)) {
            ret = ENOTSUP;
            goto _exit1;
        }
    }

    /* make sure it's a directory */
    if (dir_vp->v_type != VDIR) {
        ret = ENOTDIR;
        goto _exit1;
    }

    /* make sure it's msfs */
    if (dir_vp->v_tag != VT_MSFS) {
        ret = EDIR_NOT_ADVFS;
        goto _exit1;
    }

    /*
     * Make sure this is not a read-only fileset.
     */
    mp = VTOMOUNT(dir_vp);
    if (mp->m_flag & M_RDONLY) {
        ret = EROFS;
        goto _exit1;
    }

    /*
     * make a clone nameidata and lookup the undelete
     * directory
     */
    undel_ndp->ni_utnd = &undel_utnd;
    nddup(ndp, undel_ndp);
    undel_ndp->ni_nameiop = LOOKUP | FOLLOW;
    undel_ndp->ni_segflg = UIO_SYSSPACE;
    undel_ndp->ni_dirp = undel_dir_path;

    if (ret = namei(undel_ndp)) {
        goto _exit2;
    }

    vp2 = undel_ndp->ni_vp;
    undel_dir_vp = undel_ndp->ni_vp;

    if (clu_is_ready()) {
        if (ret = CC_GETPFSVP(vp2, &undel_dir_vp)) {
            ret = ENOTSUP;
            goto _exit3;
        }
    }

    /* make sure it's a directory */
    if (undel_dir_vp->v_type != VDIR) {
        ret = ENOTDIR;
        goto _exit3;
    }

    /*
     * make sure it's msfs and that we're not attaching a dir to itself
     */

    if (undel_dir_vp->v_tag != VT_MSFS) {
        ret = EUNDEL_DIR_NOT_ADVFS;
        goto _exit3;
    }

    if (undel_dir_vp == dir_vp) {
        ret = EDUPLICATE_DIRS;
        goto _exit3;
    }

    /*
     * make sure the two directories are in the same file set
     */

    fsnp = GETFILESETNODE(mp);
    undel_mp = VTOMOUNT(undel_dir_vp);
    undel_fsnp = GETFILESETNODE(undel_mp);

    if ( fsnp->bfSetp != undel_fsnp->bfSetp ) {
        ret = EDIFF_FILE_SETS;
        goto _exit3;
    }

    /*
     * lock the dir's context area, update the in-mem
     * context area (in case someone else has the dir open)
     * and write the BMTR_UNDEL_DIR record
     */


    bfap = VTOA(dir_vp);
    bnp = (struct bfNode *)&dir_vp->v_data[0];
    dir_context = bnp->fsContextp;
    undel_bnp = (struct bfNode *)&undel_dir_vp->v_data[0];
    undel_dir_context = undel_bnp->fsContextp;
    FS_FILE_WRITE_LOCK(dir_context);
    dir_context->undel_dir_tag =
    undel_dir_context->bf_tag;
    bmt_rec.dir_tag = undel_dir_context->bf_tag;

    sts = bmtr_put_rec(
                       bfap,
                       BMTR_FS_UNDEL_DIR,
                       (void *)&bmt_rec,
                       sizeof(struct undel_dir_rec),
                       FtxNilFtxH
                       );
    if (sts != EOK) {
        if (sts == ENO_MORE_MCELLS) {
            dir_context->undel_dir_tag = NilBfTag;
            ret = sts;
            FS_FILE_UNLOCK(dir_context);
            goto _exit3;
        }
        ADVFS_SAD1("bmtr_put_rec error in attach_undel_dir", sts);
    }

    FS_FILE_UNLOCK(dir_context);

    ret = ESUCCESS;

_exit3:
    /* give back vnodes and extra ndp */
    vrele(vp2);
_exit2:
    ndrele(undel_ndp);
_exit1:
    vrele(vp1);
    ms_free( undel_ndp );

    return(ret);
}


/*
 * detach an undelete directory from a directory
 */
statusT
detach_undel_dir(
                 char *dir_path,
                 long xid
                 )
{
    statusT sts;
    int error;
    register struct nameidata *ndp = &u.u_nd;
    register struct vnode *dir_vp = NULL;
    register struct vnode *vp = NULL;
    struct bfNode *bnp;
    bfAccessT *bfap;
    struct fsContext *dir_context;
    struct undel_dir_rec bmt_rec;
    struct mount *mp;

    /*
     * lookup the directory
     */
    ndp->ni_nameiop = LOOKUP | FOLLOW;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = dir_path;

    if (error = namei(ndp)) {
        RAISE_EXCEPTION(error);
    }
    vp = ndp->ni_vp;
    dir_vp = ndp->ni_vp;

    if (clu_is_ready()) {
        if (error = CC_GETPFSVP(vp, &dir_vp))
        RAISE_EXCEPTION(ENOTSUP);
    }


    /* make sure it's a directory */
    if (dir_vp->v_type != VDIR) {
        RAISE_EXCEPTION(ENOTDIR);
    }

    /* make sure it's msfs */
    if (dir_vp->v_tag != VT_MSFS) {
        RAISE_EXCEPTION(EDIR_NOT_ADVFS);
    }

    /*
     * Make sure this is not a read-only fileset.
     */
    mp = VTOMOUNT(dir_vp);
    if (mp->m_flag & M_RDONLY) {
        RAISE_EXCEPTION(EROFS);
    }


    /*
     * lock the dir's context area, update the in-mem
     * context area (in case someone else has the dir open)
     * and re-write the BMTR_UNDEL_DIR record
     */
    bfap = VTOA(dir_vp);
    bnp = (struct bfNode *)&dir_vp->v_data[0];
    dir_context = bnp->fsContextp;
    FS_FILE_WRITE_LOCK(dir_context);

    if (BS_BFTAG_EQL( dir_context->undel_dir_tag, NilBfTag )) {
        FS_FILE_UNLOCK(dir_context);
        RAISE_EXCEPTION(E_NO_UNDEL_DIR);
    }

    dir_context->undel_dir_tag = NilBfTag;
    bmt_rec.dir_tag = NilBfTag;
    sts = bmtr_put_rec_xid(
                           bfap,
                           BMTR_FS_UNDEL_DIR,
                           (void *)&bmt_rec,
                           sizeof(struct undel_dir_rec),
                           FtxNilFtxH,
                           xid
                           );
    if (sts != EOK) {
        ADVFS_SAD1("bmtr_put_rec error in detach_undel_dir", sts);
    }
    FS_FILE_UNLOCK(dir_context);

HANDLE_EXCEPTION:
    if (vp) {
        vrele(vp);
    }
    return (sts);
}


/*
 * get a directory's undelete dir tag
 */
statusT
get_undel_dir(
              char *dir_path,
              bfTagT *undelDirTag
              )
{
    int error;
    register struct nameidata *ndp = &u.u_nd;
    register struct vnode *dir_vp = NULL;
    register struct vnode *vp = NULL;
    struct bfNode *bnp;
    struct fsContext *dir_context;

    /*
     * lookup the directory
     */
    ndp->ni_nameiop = LOOKUP | FOLLOW;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = dir_path;

    if (error = namei(ndp)) {
        return error;
    }
    vp = ndp->ni_vp;
    dir_vp = ndp->ni_vp;

    if (clu_is_ready()) {
        if (error = CC_GETPFSVP(vp, &dir_vp)) {
            vrele(vp);
            return ENOTSUP;
        }
    }

    /* make sure it's a directory */
    if (dir_vp->v_type != VDIR) {
        vrele(vp);
        return(ENOTDIR);
    }

    /* make sure it's msfs */
    if (dir_vp->v_tag != VT_MSFS) {
        vrele(vp);
        return(EDIR_NOT_ADVFS);
    }

    /*
     * lock the dir's context area, update the in-mem
     * context area (in case someone else has the dir open)
     * and re-write the BMTR_UNDEL_DIR record
     */


    bnp = (struct bfNode *)&dir_vp->v_data[0];
    dir_context = bnp->fsContextp;
    FS_FILE_WRITE_LOCK(dir_context);

    *undelDirTag = dir_context->undel_dir_tag;

    FS_FILE_UNLOCK(dir_context);

    /* give back vnode */
    vrele(vp);
    return(ESUCCESS);
}


/*
 * given a tag, return the file name string and the parent directory
 * tag. Returns ENO_SUCH_TAG if the bs_open fails for the tag. If
 * anything else funny happens (can't find the dir entry or whatever)
 * returns ENO_NAME.
 */
statusT
get_name(
         struct mount *mp,   /* in - mount structure pointer */
         bfTagT bf_tag,      /* in - tag of file */
         char *buffer,       /* in - out, buffer addr to store file
                                name string */
         bfTagT *parent_tag  /* out - tag of parent diirectory */
         )
{
    statusT sts, return_status;
    int i;
    int last_page;
    bfAccessT *bfap=NULL, *dir_bfap=NULL;
    bfSetT *bfSetp;
    bfTagT root_tag;
    char *dir_buffer, *p;
    bfPageRefHT page_ref;
    dirRec *dirRecp;
    statT bf_stats, dir_stats;
    fs_dir_entry *dir_p, *last_p;
    char root_name[4] = "/";
    struct vnode *nullvp;
    struct fsContext *dir_contextp;

    bfSetp = GETBFSETP(mp);
    root_tag = GETROOT(mp);

    if (BS_BFTAG_EQL(bf_tag, root_tag)) {
        strncpy(buffer, root_name, NAME_MAX);
        *parent_tag = root_tag;
        return EOK;
    }
    return_status = ENO_NAME;

    /*
     * access the bf
     */
    nullvp = NULL;
    if (bs_access(&bfap, bf_tag, bfSetp, FtxNilFtxH, BF_OP_GET_VNODE, NULLMT, &nullvp)) {
        return (ENO_SUCH_TAG);
    }


    /*
     * get the bmt stat record for the bf_tag. it will tell the
     * directory tag.
     */
    sts = bmtr_get_rec(
                       bfap,
                       BMTR_FS_STAT,
                       &bf_stats,
                       sizeof(statT)
                       );

    bs_close(bfap, MSFS_DO_VRELE);

    if (sts == EBMTR_NOT_FOUND) {
        /*
         * Assume this bitfile doesn't have any stats.  For example, the
         * frag file doesn't have any stats.
         */
        RAISE_EXCEPTION(ENO_SUCH_TAG);
    } else if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    /*
     * open the directory
     */
    if (!(BS_BFTAG_EQL(bf_stats.dir_tag, root_tag))) {
        nullvp = NULL;
        if (bs_access(&dir_bfap, bf_stats.dir_tag, bfSetp, FtxNilFtxH, BF_OP_GET_VNODE, NULLMT, 
                      &nullvp)) {
            RAISE_EXCEPTION(ENO_NAME);
        }
    } else {
        dir_bfap = GETROOTACCESS(mp);
    }


    /*
     * get the bmt stat record for the directory.
     */
    sts = bmtr_get_rec(dir_bfap, BMTR_FS_STAT, &dir_stats, sizeof(statT));
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    if ( (dir_stats.st_mode & S_IFMT) != S_IFDIR ) {
        /* This isn't a directory */
        RAISE_EXCEPTION(ENO_NAME);
    }
    /*
     * ref the dir pages and look for the dir entry
     */
    dir_contextp = VTOC( dir_bfap->bfVp );
    if ( !dir_contextp ) {
        statusT sts = EOK;

        /* bs_access() can return an access structure with a vnode that
         * has just been allocated.  If this is the case, the vnode still
         * has (v_type == VREG) and no context structure.  All we really
         * want the context structure for is the file_lock to keep other
         * threads from modifying the directory data while we read it.
         * In this (rare) case, allocate a context structure and init
         * minimal # of fields in the bfap, context, and vnode structures.
         */
        sts = fscontext_init( dir_bfap->bfVp, dir_bfap, &dir_contextp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        /* Not a full initialization.  Must be 0 for any later bf_get_l call. */
        dir_contextp->initialized = 0;
    }

    FS_FILE_READ_LOCK( dir_contextp );
    last_page = howmany(dir_stats.st_size, ADVFS_PGSZ);

    for (i = 0; i < last_page; i++) {
        sts = bs_refpg(
                       &page_ref,
                       (void *)&dir_buffer,
                       dir_bfap,
                       i,
                       BS_NIL
                       );
        if (sts != EOK) {
            return_status = EIO;
            break;
        }
        dir_p = (fs_dir_entry *)dir_buffer;
        dirRecp = (dirRec *)(dir_buffer + ADVFS_PGSZ - sizeof(dirRec));
        last_p = (fs_dir_entry *)(dir_buffer +
                                  dirRecp->lastEntry_offset);
        while (dir_p <= last_p) {
            if ((dir_p->fs_dir_header.fs_dir_size == 0) ||
                (dir_p->fs_dir_header.fs_dir_size > 512)) {
                return_status = EIO;
                break;
            }
            if (bf_tag.num == dir_p->fs_dir_header.fs_dir_bs_tag_num) {
                strncpy(buffer, dir_p->fs_dir_name_string, NAME_MAX);
                *parent_tag = bf_stats.dir_tag;
                return_status = EOK;
                break;
            }
            p = (char *)dir_p;
            p += dir_p->fs_dir_header.fs_dir_size;
            dir_p = (fs_dir_entry *)p;
        }

        bs_derefpg(page_ref, BS_NIL);
        if (return_status == EOK) {
            break;
        }
    }

    FS_FILE_UNLOCK( dir_contextp );
    sts = return_status;

HANDLE_EXCEPTION:
    if (dir_bfap) {
        if (dir_bfap != GETROOTACCESS(mp)) {
           bs_close(dir_bfap, MSFS_DO_VRELE);
        } else {
        }
    }

    return sts;
}


statusT
get_name2(
         char *name,         /* in - name of file/dir in same fileset */
         bfTagT bf_tag,      /* in - tag of file */
         char *buffer,       /* in - out, buffer addr to store file
                                name string */
         bfTagT *parent_tag  /* out - tag of parent diirectory */
         )
{
    statusT sts;
    int error;
    register struct nameidata *ndp = &u.u_nd;
    register struct vnode *dir_vp = NULL;
    register struct vnode *vp = NULL;

    /*
     * lookup the file
     */
    ndp->ni_nameiop = LOOKUP;
    ndp->ni_segflg = UIO_SYSSPACE;
    ndp->ni_dirp = name;
    if (error = namei(ndp)) {
        return error;
    }
    vp = ndp->ni_vp;
    dir_vp = ndp->ni_vp;

    if (clu_is_ready()) {
        if (error = CC_GETPFSVP(vp, &dir_vp)) {
            vrele(vp);
            return ENOTSUP;
        }
    }

    /* make sure it's msfs */
    if (dir_vp->v_tag != VT_MSFS) {
        vrele(vp);
        return(ENOSYS);
    }

    sts = get_name( dir_vp->v_mount, bf_tag, buffer, parent_tag );

    /* give back vnode (close it) */
    vrele(vp);

    return( sts );
}


int
msfs_swap (void)     /* swap handler */
{
    return(ENOSYS);
}

/*
 * msfs_lockctl
 *
 * File locking - cut/paste from ufs_lockctl() in ufs_vnops.c
 */

int
msfs_lockctl(
             struct vnode *vp,    /* in - vnode pointer of file */
             struct eflock *eld,  /* in */
             int flag,            /* in */
             struct ucred *cred,  /* in - callers credentials */
             pid_t clid,          /* in */
             off_t offset         /* in */
             )
{

    int error;

    FILESETSTAT( vp, msfs_lockctl );

#ifndef NOTYET
    if (vp->v_type == VBLK || vp->v_type == VFIFO) {
        return (EINVAL);
    }
#else
        /*
         * From nfssrc4.1: removed to pass PCTS
         */
    if ((flag & RGETFLCK == 0) && (flag & RSETFLCK == 0)) {
        if (vp->v_type == VBLK || vp->v_type == VCHR ||
            vp->v_type == VFIFO) {
            return (EINVAL);
        }
    } else {
        if (vp->v_type == VBLK || vp->v_type == VFIFO) {
            return (EINVAL);
        }
    }
#endif  /* NOTYET */

    if (flag & CLNFLCK) {
        error = cleanlocks(vp, clid, 0);
        return( error );
    }
    if (flag & VNOFLCK) {
        error = locked(vp, eld, clid, flag);
        return( error );
    }

    if (flag & GETFLCK) {
        error = getflck(vp, eld, offset, clid, FILE_LOCK);
        return( error );
    }
        if (flag & RGETFLCK) {
            error = getflck(vp, eld, offset, clid, LOCKMGR);
            return( error );
        }

    if (flag & SETFLCK) {
        error = setflck(vp, eld, flag&SLPFLCK, offset, clid, FILE_LOCK);
        return( error );
    }
    if (flag & RSETFLCK) {
        if (eld->l_type == F_UNLKSYS) {
            kill_proc_locks(eld, 0);
            return(0);
        }
        error = setflck(vp, eld, flag&SLPFLCK, offset, clid, LOCKMGR);
        return( error );
    }

    if (flag & ENFFLCK) {
        error = msfs_setvlocks(vp);
        return( error );
    }
    return(EINVAL);
}

/*
 * msfs_setvlocks
 *
 * cut/paste from ufs_setvlocks() in ufs_vnops.c and modified for Megasafe
 */

int
msfs_setvlocks(
               struct vnode *vp    /* in - vnode pointer of file */
               )
{
    struct fsContext *context_ptr = VTOC(vp);

    FILESETSTAT( vp, msfs_setvlocks );

    mutex_lock( &context_ptr->fsContext_mutex );
    VN_LOCK(vp);

    if ((context_ptr->dir_stats.st_mode & S_ISGID) &&
        (!(context_ptr->dir_stats.st_mode & S_IXGRP))) {
        vp->v_flag |= VENF_LOCK;
    }
    mutex_unlock( &context_ptr->fsContext_mutex );

    vp->v_flag |= VLOCKS;

    VN_UNLOCK(vp);

    return (0);
}

/*
 * msfs_syncdata
 *
 * sync a byte range (used by NFS after a gather write)
 */

msfs_syncdata(
              struct vnode *vp,    /* in - vnode to fsync */
              int fflags,          /* in - flags */
              vm_offset_t start,   /* in - starting offset to sync in file */
              vm_size_t length,    /* in - length to sync */
              struct ucred *cred   /* in - credentials of caller */
              )
{
    statusT ret;
    struct fsContext *file_context;
    bfAccessT *bfap = VTOA(vp);
    bsPageT first_page_to_flush,
            last_page_to_flush;

    FILESETSTAT( vp, msfs_syncdata );

    /*
     * If this is a data logging file and we're being called from
     * the NFS server, don't do anything since the the NFS server
     * either already has or shortly will call msfs_fsync() to 
     * flush the log, which includes the user's data.  The user
     * data file will be updated asynchronously.
     */
    if (((bfap->dataSafety == BFD_FTX_AGENT) ||
         (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY))  &&
        (NFS_SERVER_TSD != 0)) {
        return (0);
    }

    NFS_FREEZE_LOCK(vp);

#ifdef notdef
    /*
     * This hurts alot for clients which transfer < 8192 bytes (PC-NFS).
     * We need another mechanism to truncate pre-allocated space for a
     * file being accessed via NFS
     */
    if ((VTOA(vp))->file_size & 0x1FFF)) {
        fs_trunc_test( vp );
    }
#endif

    file_context = VTOC(vp);
    first_page_to_flush = start / ADVFS_PGSZ;
    last_page_to_flush = (roundup((start + length),ADVFS_PGSZ)/ADVFS_PGSZ) - 1;

    /*
     * Take a share lock to block other writers, but no need to block
     * against other readers.
     */
    FS_FILE_READ_LOCK(file_context);

    ret = bfflush(bfap, first_page_to_flush, 
                  (last_page_to_flush - first_page_to_flush) + 1,
                  FLUSH_INTERMEDIATE);

    FS_FILE_UNLOCK(file_context);

    if (ret != EOK) {
      NFS_FREEZE_UNLOCK(vp);
      return(EIO);
    }

    NFS_FREEZE_UNLOCK(vp);
    return (0);
}

int
msfs_objtovp(
    vm_ubc_object_t vop,
    struct vnode **vp)
{
    MS_SMP_ASSERT(vop->vu_ap);        /* have an access structure */
    *vp = vop->vu_ap->bfVp;
    return(0);
}

int
msfs_setpgstamp(
    vm_page_t pp,
    unsigned int tick)
{
    return(0);
}

int
msfsspec_pathconf(
    register struct vnode *vp,
    int name,
    long *retval)
{
    switch (name) {
        case _PC_ACL_EXTENDED:
            *retval = (paclpolicy == ACLS_ENABLED);
            return(0);

        default:
            return(specvn_pathconf(vp, name, retval));
    }
}

int
msfsfifo_pathconf(
    register struct vnode *vp,
    int name,
    long *retval)
{
    switch (name) {
        case _PC_ACL_EXTENDED:
            *retval = (paclpolicy == ACLS_ENABLED);
            return(0);

        default:
            return(fifo_pathconf(vp, name, retval));
    }
}
