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
 */

#include <limits.h>
#include <unistd.h>
#include <sys/privgrp.h>              /* struct privgrp_map for priv_global */
#include <fs/h/filesystems_private.h> /* extern priv_global */
#include <fs/vfs_ifaces.h>            /* struct nameidata,  NAMEIDATA macro */
#include <sys/param.h>
#include <sys/systm.h>
#include <sys/time.h>
#include <sys/vnode.h>
#include <sys/mount.h>
#include <sys/uio.h>
#include <sys/errno.h>
#include <sys/kernel.h>
#include <sys/dnlc.h>
#include <sys/stat.h>
#include <sys/fcntl.h>
#include <sys/buf.h>
#include <sys/ioctl.h>
#include <sys/tty.h>
#include <sys/privgrp.h>
#include <sys/spinlock.h>             /* for lock_t */
#include <nfs/nfs.h>
#include <sys/pathname.h>             /* struct pathname */

#include <ms_public.h>
#include <ms_privates.h>
#include <ftx_public.h>
#include <ms_osf.h>
#include <fs_dir.h>
#include <fs_dir_routines.h>
#include <fs_quota.h>
#include <bs_extents.h>
#include <bs_index.h>
#include <bs_params.h>
#include <ms_assert.h>
#include <aclv.h>
#include <advfs_acl.h>
#include <advfs_evm.h>
#include <bs_access.h>
#include <bs_snapshot.h>
#include <bs_freeze.h>

#include <tcr/clu.h>

#include <sys/fs/advfs_ioctl.h>

#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

/*
 * HP REVISIT - no header file available for enforcement_mode()  
 *            - defined in vfs_lockf.c 
 *
 * Probably should add  "extern int enforcement_mode(struct vnode *);"  BUT
 * the use of externs in *.c files is frowned upon by the clean code czars.  
 */

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
#ifdef NFS_LATER 
/*
 * ifdefed until NFS_SERVER_TSD is activated in nfs_public.h. 
 */

#define NFS_FREEZE_LOCK(vp) {                                                 \
                                                                              \
    if (NFS_SERVER_TSD) {                                                     \
        domainT *dmnP = ADVGETDOMAINP(vp->v_vfsp);                            \
        ADVMTX_DOMAINFREEZE_LOCK( &(dmnP->dmnFreezeMutex) );                         \
        if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) { \
            ADVMTX_DOMAINFREEZE_UNLOCK( &dmnP->dmnFreezeMutex );                     \
            return (NFS3ERR_JUKEBOX);                                         \
        } else {                                                              \
            dmnP->dmnFreezeRefCnt++;                                          \
            ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);                       \
        }                                                                     \
    }                                                                         \
}
#define NFS_FREEZE_UNLOCK(vp)  {                                              \
                                                                              \
    if (NFS_SERVER_TSD) {                                                     \
        domainT *dmnP = ADVGETDOMAINP(vp->v_vfsp);                            \
        ADVMTX_DOMAINFREEZE_LOCK( &dmnP->dmnFreezeMutex );                         \
        dmnP->dmnFreezeRefCnt--;                                              \
        if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {          \
            cv_broadcast( &dmnP->dmnFreezeWaitingCV, NULL, CV_NULL_LOCK );    \
        }                                                                     \
        ADVMTX_DOMAINFREEZE_UNLOCK( &dmnP->dmnFreezeMutex );                         \
    }                                                                         \
}

#else

#define NFS_FREEZE_LOCK(vp) {}
#define NFS_FREEZE_UNLOCK(vp) {}

#endif /* NFS_LATER */

/*
 * Global vfs data structures for msfs
 */

int advfs_lookup(),
    advfs_create(struct vnode *dvp, char *nm, struct vattr *vap, 
                 enum vcexcl exclusive, int mode, struct vnode **vpp, 
                 struct ucred *cred),
    advfs_open(struct vnode **vpp, int mode, struct ucred *cred),
    advfs_close(struct vnode *vp, int fflag, struct ucred *cred),
    advfs_access(struct vnode *vp, int mode, struct ucred *cred),
    advfs_select(struct vnode *vp, int which, struct ucred *cred),
    advfs_getattr(struct vnode *vp,  struct vattr *vap, 
                 struct ucred *cred, enum vsync),
    advfs_setattr(struct vnode *vp,  
                  struct vattr *vap,  struct ucred *cred, int),
    advfs_rdwr(struct vnode *vp,  struct uio *uiop, 
               enum uio_rw rw, int ioflag, struct ucred *cred),
    advfs_ioctl(),
    seltrue(),
    advfs_cachelimit(struct vnode *vp, off_t len, pgcnt_t *location),
    advfs_mmap(),
    advfs_munmap(struct vnode *vp, u_int off, u_long size_bytes, int access),
    advfs_swapfs_len(struct buf *bp),
    advfs_release(struct vnode *vp),
    advfs_readdir2(),
    advfs_readdir3(),
    advfs_fsync(struct vnode *vp, struct ucred *cred, int  fflags),
    advfs_remove(struct vnode *vp, char *nm, struct ucred *cred),
    advfs_link(struct vnode *vp, struct vnode *dvp, char *nm, 
              struct ucred *cred),
    advfs_rename(struct vnode *sdvp, char *snm, 
                 struct vnode *tdvp, char *tnm, struct ucred *cred),
    advfs_mkdir(struct vnode *dvp, char *nm, 
               struct vattr *vap, struct vnode **vpp,   struct ucred *cred),
    advfs_rmdir(struct vnode *vp, char *nm, struct ucred *cred),
    advfs_symlink(struct vnode *dvp, char *lnm, 
		  struct vattr *vap, char *tnm, struct  ucred *cred),
    advfs_readdir(struct vnode *vp, struct uio *uiop, struct ucred *cred),
    advfs_readlink(struct vnode *vp, struct uio *uiop, struct ucred *cred),
    advfs_inactive(),
    advfs_bmap(struct vnode *vp, kern_daddr_t lbn, 
              struct vnode **vpp, kern_daddr_t *bnp, int flags),
    advfs_strategy(struct buf *bp),
    advfs_aiostrategy(struct buf *bp, int flags),
    msfs_print(struct vnode *vp),
    advfs_getpage(),
    advfs_putpage(),
    advfs_bread(),       /* buffer read */
    advfs_brelse(),      /* buffer release */
    advfs_pathconf(struct vnode *vp, int name, int *resultp, 
                   struct ucred *cred),    /* pathconf */
    advfs_lockctl(struct vnode *vp, struct flock *flock, int cmd, 
                 struct ucred *cred, struct file *fp, off_t LB, off_t UB),
    advfs_lockf(struct vnode *vp, int flag, off_t len, struct ucred *cred, 
               struct file *fp, off_t LB, off_t UB),
    advfs_fid(struct vnode *vp, struct fid **fhp),
    advfs_fsctl(struct vnode *vp, int cmd, struct uio *uiop, 
                struct ucred *cred),
    advfs_prealloc(struct vnode *vp, off_t size, int ignore_minfree, 
                   int reserved);

int
advfs_shrlock(
    struct vnode *vp,
    int cmd,
    struct shrlock *shr,
    int flag);

int
advfs_realvp(
    struct vnode *vp, 
    struct vnode **realvpp);

int
advfs_setacl(
    struct vnode *vp,
    int num_tuples,
    struct acl_tuple_user *tupleset,
    int acl_type);
int
advfs_getacl(
    struct vnode *vp,
    int num_tuples,
    struct acl_tuple_user *tupleset,
    int acl_type);

/*
 * msfs_vnodeops
 *
 * Defines function pointers to AdvFS VFS vnode operations.
 */

struct vnodeops advfs_vnodeops = {
    advfs_open,         /* open */
    advfs_close,        /* close */
    advfs_rdwr,         /* read/write */
    advfs_ioctl,        /* ioctl */
    advfs_select,       /* select */
    advfs_getattr,      /* getattr */
    advfs_setattr,      /* setattr */
    advfs_access,       /* access */
    advfs_lookup,       /* lookup */
    advfs_create,       /* create */
    advfs_remove,       /* remove */
    advfs_link,         /* link */
    advfs_rename,       /* rename */
    advfs_mkdir,        /* mkdir */
    advfs_rmdir,        /* rmdir */
    advfs_readdir,      /* readdir */
    advfs_symlink,      /* symlink */
    advfs_readlink,     /* readlink */
    advfs_fsync,        /* fsync */
    advfs_inactive,     /* inactive */
    advfs_bmap,         /* bmap */
    advfs_strategy,     /* strategy */
    advfs_bread,        /* buffer read */
    advfs_brelse,       /* buffer release */
    NULL,               /* pathsend */
    advfs_setacl,       /* Set extended attributes */
    advfs_getacl,       /* Get extended attributes */
    advfs_pathconf,     /* pathconf */
    advfs_pathconf,     /* fpathconf */
    advfs_lockctl,      /* file locking */
    advfs_lockf,         /* lockf */
    advfs_fid,          /* fid */
    advfs_fsctl,        /* fsctl */
    NULL,               /* prefill */
    advfs_getpage,      /* pagein */
    advfs_putpage,      /* pageout */
    NULL,               /* dbddup */
    NULL,               /* dbddealloc */
    advfs_prealloc,     /* prealloc */
    NULL,               /* mapdbd */
    advfs_mmap,         /* mmap */
    advfs_cachelimit,   /* cachelimit */
    NULL,               /* vm_checkpage */
    NULL,               /* vm_contiguous */
    NULL,               /* vm_stopio */
    NULL,               /* vm_read_ahead */
    advfs_release,      /* release */
    advfs_munmap,       /* munmap */
    advfs_swapfs_len,   /* swapfs_len */
    advfs_readdir2,     /* readdir2 */
    advfs_readdir3,     /* readdir3 */
    NULL,               /* vm_commit */
    NULL,               /* vop_dnlc_getpathname */
    advfs_shrlock,      /* shrlock */
    advfs_realvp,       /* realvp */
    advfs_aiostrategy,  /* AIO strategy */
};

/*
 * advfs_chown
 *
 * change owner of file
 */

static
int
advfs_chown(
    struct vnode *vp,          /* in - vnode pointer for bitfile */
    uid_t uid,                 /* in - uid to change to */
    gid_t gid,                 /* in - gid to change to */
    struct ucred *cred,        /* in - callers credentials */
    ftxHT parentFtxH);         /* in - transaction handle */

/*
 * advfs_chmod
 *
 * chmod operation on a file
 * this is not directly a vn op, but is called from
 * advfs_setattr
 */
int
advfs_chmod(
    struct vnode *vp,               /* in - vnode of bitfile */
    struct fsContext *file_context, /* in - file context pointer */
    int mode,                       /* in - mode to change to */
    struct ucred *cred);            /* in - callers credentials */

/*
 * decr_link_count
 *
 * decr the link count of a file and delete if count is 0
 */
int
decr_link_count(
    struct vnode *vp,   /* in - the context pointer of the file */
    ftxHT ftx_handle);  /* in - ftx handle */

/*
 * table used by VTTOTYP macro, which is used by the MAKEMODE macro.
 * The two macros translate the vnode-type to a file type and 'or' it
 * with the creation mode. 
 *
 */
int advfs_vttotyp_tab[] = {
    0,          /* VNON */
    S_IFREG,    /* VREG */
    S_IFDIR,    /* VDIR */
    S_IFBLK,    /* VBLK */
    S_IFCHR,    /* VCHR */
    S_IFLNK,    /* VLNK */
    S_IFSOCK,   /* VSOCK */
    S_IFMT,     /* VBAD */
    S_IFIFO,    /* VFIFO */
    S_IFNWK,    /* VFNWK */
    S_IFDIR     /* VEMPTYDIR */
};

enum vtype advfs_typtovt_tab[] = {
    VNON,
    VFIFO,      /* S_IFIFO 0010000 */
    VCHR,       /* S_IFCHR 0020000 */
    VNON,
    VDIR,       /* S_IFDIR 0040000 */
    VNON,
    VBLK,       /* S_IFBLK 0060000 */
    VNON,
    VREG,       /* S_IFREG 0100000 */
    VFNWK,      /* S_IFNWK 0110000 */
    VLNK,       /* S_IFLNK 0120000 */
    VNON,
    VSOCK,      /* S_IFSOCK 0140000 */
    VNON,
    VNON,
    VBAD        /* S_IFMT 0170000 */
};

/*
 * advfs_cachelimit
 * This is AdvFS's VOP_CACHELIMIT.
 * This routine exists soley to provide compatibilty so that VM
 * does not break. However, AdvFS does not have any dependencies on
 * this routine.
 */
int
advfs_cachelimit(
    struct vnode *vp,
    off_t len,
    pgcnt_t *location)
{
    *location = (pgcnt_t)((pgcnt_t)btorp(len) + 1);
    return 0;
}


/*
 * advfs_create
 *
 * This in advfs's VOP_CREATE.  In HPUX, VOP_CREATE is called with dvp
 * having a reference on it.  We must not VN_RELE the dvp since vns_create
 * will unconditionally do a VN_RELE on dvp when we return.  
 *
 * exclusive create issues seem to have been already resolved in the VFS
 * layer routines (vn_create/vns_create).
 */
advfs_create(
    struct vnode  *dvp,
    char          *name,
    struct vattr  *vap,
    enum   vcexcl  exclusive,
    int            mode,
    struct vnode **vpp,
    struct ucred  *cred)
{
    int ret = 0;
    struct nameidata *ndp = NAMEIDATA();
    ni_nameiop_t      saved_op;
    int               error;
 
    FILESETSTAT( dvp, advfs_create );

    MS_SMP_ASSERT(ndp != NULL);
    saved_op = ndp->ni_nameiop;
    if (!(ndp->ni_nameiop & NI_CREATE)) {
        /*
         * Assumes that the nameidata structure will be properly setup if
         * and only if NI_CREATE is set by the caller (VFS layer), so we can
         * optimize the path.  If we find any other value in ni_nameiop,
         * then do our own lookup to initialize the nameidata structure.
         */
        advfs_lookup_t ali_flags = ALI_NO_FLAGS;

        ndp->ni_nameiop = NI_CREATE;
        error = advfs_lookup_int(ali_flags, ndp, dvp, name, cred, dvp);
        ndp->ni_nameiop = saved_op;     /* setting outside of vfs, must be
                                           restored */
        if ( error != EOK && error != ENOENT ) {
            return(error);
        }
        if ( ndp->ni_vp != NULL ) {
            /*
             * If the local lookup found a file, we need to release the
             * vnode and report EEXIST.  The VFS layer will decide to retry
             * if it is not an exclusive create.
             */
            VN_RELE(ndp->ni_vp);
            return( EEXIST );
        }
    }

    NFS_FREEZE_LOCK(dvp);

    /*
     * fs_create_file should return with v_count and v_scount untouched on
     * dvp.  The new file may have been added to the dnlc, so it may have a
     * v_scount of 1, but it should not have a v_count unless someone is
     * racing an open with us.  We cannot assert anything about v_count's
     * because we hold no locks.
     */
    ret = fs_create_file(vap, ndp, dvp, name, NULL);

    if (ret == EOK){
        *vpp = ndp->ni_vp;
        ADVFS_ACCESS_LOGIT( VTOA( *vpp ), "advfs_create" );

#define	ISVDEV(t) \
        (((t) == VBLK) || ((t) == VCHR) || ((t) == VFIFO) )
	if ((*vpp != NULL) && ISVDEV((*vpp)->v_type)) {
		struct vnode *newvp;

		newvp = specvp(*vpp, (*vpp)->v_rdev ); /* LU */
		VN_RELE(*vpp);
		(*vpp) = newvp;
	}
	if (vap != (struct vattr *)NULL) {
		(void) VOP_GETATTR(*vpp, vap, cred, VASYNC);
	}
    }

    NFS_FREEZE_UNLOCK(dvp);

    return (ret);
}

/*
 * Check to see if the cache policy of the file is being modified.
 * Otherwise, just return success from open.  advfs_lookup has
 * already opened the file.
 */
advfs_open(
    struct vnode **vpp,   /* in - vnode pointer */
    int mode,             /* in - open mode */
    struct ucred *cred)   /* in - credentials of caller */
{
    struct fsContext *contextp = VTOC(*vpp);
    struct bfAccess  *bfap     = VTOA(*vpp);
    struct bfSet     *bfSetp   = (struct bfSet *)bfap->bfSetp;
    int ret = 0;
    int enforced;
    int locked_vnode    = FALSE;
    uint64_t dummy;

    /*
     * If the file being opened (O_TRUNC - create) is
     * locked, then return an error.
     */
    if ((*vpp)->v_type == VREG  && (*vpp)->v_locklist && 
         (mode & O_TRUNC)){
        enforced = enforcement_mode( *vpp );
        VNODE_LOCKX(*vpp, locked_vnode);
        if (ret = lockedx(F_TEST, *vpp, 0, MAX_LOCK_SIZE,
                         L_COPEN, NULL,0,0, enforced)) {
            ret = u.u_error;
            VNODE_UNLOCKX(*vpp, locked_vnode);
            return (ret);
        }
        VNODE_UNLOCKX(*vpp, locked_vnode);
    }

    if ( (mode & FDIRECT) || (bfSetp->bfSetFlags & BFS_IM_DIRECTIO) ) {
        /*
         * Just a safety check that the fcntl(..., F_SETFL, ...) can not
         * modify the FDIRECT bit after a file is opened.  If it could, then
         * we could not depend on finding the FDIRECT bit properly set
         * during close.  That would be bad.
         */
        MS_SMP_ASSERT( FCNTLCANT & FDIRECT );

        /* Rules du jour when enabling directIO on a file:
         * 1. If operating on a cluster, do nothing.  CFS will call
         *    advfs_cfs_set_cachepolicy() to change cache policy on a file.
         * 2. If this is a reserved or metadata file, then return an
         *    error if this is an explicit request to open the file for
         *    directIO.  If this is an open of a reserved file on a
         *    directIO-mounted fileset, just return EOK without doing
         *    anything.  Reserved files and AdvFS special files are
         *    never opened for directIO.
         * 3. If this is a file other than VREG, return an error.
         * 4. If file is already open for mmapping, return an error.
         */
        if ( (*vpp)->v_vfsp->vfs_flag & VFS_CFSONTOP )
            return EOK;

        if ( bfap->dataSafety != BFD_USERDATA ) {
            /* Reserved file; check if we open in cached mode or fail
             * the open.  In no case do we increment bfap->dioCnt for
             * reserved files.
             */
            if (bfSetp->bfSetFlags & BFS_IM_DIRECTIO ) {
                return EOK;
            } else {
                return EINVAL;
            }
        }

        /* When changing the cache policy, take the cache mode lock
         * for exclusive access to force all active I/O to complete 
         * before the policy change goes into effect. However, we  
         * start out with this lock in shared mode until we know that we
         * need to change the mode, and then upgrade the lock if needed.
         */
        ADVRWL_BFAPCACHEMODE_READ(bfap);
retry:
        if ( ((*vpp)->v_type != VREG) ) {
            /* Can't open non-regular files for DIO */
            ret = EINVAL;
        } else if ( ((*vpp)->v_flag & VMMF) && (bfap->dioCnt == 0) ) {
            /* mmapped or mmapping in progress and not open for directIO.
             * Let the mmapper succeed.
             */
            ret = EINVAL;
        } else {
            if ( bfap->dioCnt > 0 ) {
                /* already open for dio, just bump count. Use ATOMIC
                 * increment macro to ensure correct value in case of 
                 * two racing threads doing this increment.
                 */
                ADVFS_ATOMIC_FETCH_INCR(&bfap->dioCnt, &dummy);
            } else {
                /* We are changing from cached to directIO mode. Need
                 * to do this with the cacheModeLock held exclusively
                 * to prevent in-flight I/Os while the mode is changing.
                 */
                if ( !(ADVRWL_BFAPCACHEMODE_HELD_EXCL(bfap)) ) {
                    if ( ADVRWL_BFAPCACHEMODE_UPGRADE(bfap) != RWLCK_SUCCESS ) {
                        /* The upgrade failed; drop the cacheModeLock and
                         * reseize for exclusive access.  Then retry the 
                         * whole operation to check for racing mmap etc.
                         */
                        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
                        ADVRWL_BFAPCACHEMODE_WRITE(bfap);
                        goto retry;
                    }
                }
                /* Must be sure no mmappers snuck in while the lock
                 * was upgraded.
                 */
                if ( (*vpp)->v_flag & VMMF ) {
                    ret = EINVAL;
                } else {
                    fcache_vninfo_t fc_info;

                    /* Finally we can change the cache mode and then
                     * flush and invalidate any UFC buffers for this file
                     * if there are pages in the cache.
                     */
                    fc_info.fvi_pages = 0;
                    ret = fcache_vn_info( &bfap->bfVnode, 
                                           FVINFO_PAGES, 
                                          &fc_info);
                    if ( ret == EINVAL  ||  fc_info.fvi_pages != 0 ) {
                        ret = fcache_vn_flush( &bfap->bfVnode, 
                                           (off_t)0, 
                                           (size_t)0,      /* entire file */
                                           NULL,
                                           FVF_WRITE | FVF_SYNC | FVF_INVAL );
                    }
                    bfap->dioCnt++;
                }
            }
        }
        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
    }
    return(ret);
}


/*
 * advfs_close
 *
 * close a file
 */
advfs_close(
    struct vnode *vp,    /* in - vnode of file to close */
    int fflag,           /* in - flags */
    struct ucred *cred)  /* in - credentials of caller */
{
    struct fsContext *context_pointer = VTOC( vp );
    ftxHT ftx_handle;
    bfAccessT *bfap     = VTOA(vp);
    int nfsFreezeLocked = FALSE;
    int sts             = 0;

    int locked_vnode    = FALSE;

    FILESETSTAT( vp, advfs_close );

    if ((context_pointer->fs_flag & META_OPEN) != 0) {
        goto EXIT;
    }

    NFS_FREEZE_LOCK(vp);
    nfsFreezeLocked = TRUE;

    /* If this file was opened for directIO either explicitly or implicitly,
     * and we are not running on a cluster, then we want to decrement the
     * counter that records the number of threads that have this file open
     * for directIO.  If the counter reaches 0, then the file changes from
     * being open for directIO to being open for cached I/O.
     */
    if ( !(vp->v_vfsp->vfs_flag & VFS_CFSONTOP) && ( (fflag & FDIRECT) ||
         (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO)) ) {
        /*
         * Just a safety check that the fcntl(..., F_SETFL, ...) can not
         * modify the FDIRECT bit after a file is opened.  If it could, then
         * we could not depend on finding the FDIRECT bit properly set
         * during close.  That would be bad.
         */
        MS_SMP_ASSERT( FCNTLCANT & FDIRECT );

        ADVRWL_BFAPCACHEMODE_WRITE(bfap);
       
        if ( fflag & FDIRECT )
            MS_SMP_ASSERT( bfap->dioCnt > 0 );

        if ( bfap->dioCnt )
            bfap->dioCnt--;
        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
    }

    /*
     * check to see if the stats were changed
     */
    if (context_pointer->dirty_stats == TRUE) {

        /*
         * screen domain panic
         */
        bfap = VTOA(vp);
        if (bfap->dmnP->dmn_panic) {
            sts = EIO;
            goto EXIT;
        }

        if ( FTX_START_N( FTA_NULL, &ftx_handle, FtxNilFtxH,
                          ADVGETDOMAINP(vp->v_vfsp)) == EOK ) {
            /*
             * log an update to the in-memory file stats.
             */
  
            (void)fs_update_stats(vp, bfap, ftx_handle, 0);

            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_21);

            ftx_done_n(ftx_handle, FTA_FS_CREATE_2);

            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_22);
        }
    }

EXIT:
    if (nfsFreezeLocked) {
        NFS_FREEZE_UNLOCK(vp);
    }

    if (vp->v_locklist) {
        VNODE_LOCKX(vp, locked_vnode);
        (void) unlock(vp);
        VNODE_UNLOCKX(vp, locked_vnode);
    }

    return sts;
}


/*
 * advfs_getattr
 *
 * get the attributes for a file
 */
advfs_getattr(
    struct vnode *vp,           /* in - vnode pointer of file */
    struct vattr *vap,          /* out - vnode attribute structure */
    struct ucred *cred,         /* in - callers credentials */
    enum vsync vsync)
{
    bfAccessT        *bfap;
    struct fsContext *context_ptr;
    bfSetT           *bfSetp;
    struct vfs       *vfsp;
    timestruc_t       new_time;
    bf_fob_t          fob_cnt;
    statusT           sts;
    fileSetNodeT     *fsn;
    size_t            pageSz;      /* Page size in bytes */

    extern int        advfs_fstype;

    ADVFS_VOP_LOCKSAFE;

    FILESETSTAT( vp, advfs_getattr );

    MS_SMP_ASSERT(vp != NULL);
    vfsp = vp->v_vfsp;
    MS_SMP_ASSERT(vfsp != NULL);
    fsn = ADVGETFILESETNODE(vfsp);
    MS_SMP_ASSERT(fsn != NULL);
    bfSetp = fsn->bfSetp;
    MS_SMP_ASSERT(bfSetp != NULL);
    bfap = VTOA(vp);
    MS_SMP_ASSERT(bfap != NULL);
    context_ptr = VTOC(vp);
    MS_SMP_ASSERT(context_ptr != NULL);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        ADVFS_VOP_LOCKUNSAFE;
        return EIO;
    }

    pageSz = bfap->bfPageSz * ADVFS_FOB_SZ;

    if (context_ptr->fs_flag & META_OPEN) {
        /* meta data has fake stats */
        ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );

        vap->va_type          = vp->v_type;
        vap->va_mode          = S_IFREG;
        vap->va_nlink         = 0;
        vap->va_uid           = 0;
        vap->va_gid           = 0;
        vap->va_fsid          = 0;
        vap->va_nodeid        = BS_BFTAG_IDX( context_ptr->bf_tag );
        vap->va_blocksize     = pageSz;
        vap->va_atime.tv_sec  = 0;
        vap->va_atime.tv_nsec = 0;
        vap->va_mtime.tv_sec  = 0;
        vap->va_mtime.tv_nsec = 0;
        vap->va_ctime.tv_sec  = 0;
        vap->va_ctime.tv_nsec = 0;
        vap->va_rdev          = 0;
        vap->va_realdev       = 0;
        vap->va_basemode      = vap->va_mode;
        vap->va_acl           = 0;
        vap->va_fstype        = vp->v_vfsp->vfs_mtype;
        MS_SMP_ASSERT(vap->va_fstype == advfs_fstype);

	/* Check for System V ACLs */
	if( bfap->bfaFlags & BFA_ACL_NOT_ONDISK ) {
	    vap->va_aclv = 0;
	} else {
	    vap->va_aclv = 1;
	}

        /* update file_size */
        if (bfap->bfa_orig_file_size == ADVFS_ROOT_SNAPSHOT) {
            bfap->file_size = (off_t)bfap->bfaNextFob * ADVFS_FOB_SZ; 
        } else {
            bfap->file_size = bfap->bfa_orig_file_size;
        }

        vap->va_size = (off_t)bfap->file_size;

        ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

        /*
         * Get the real number of bytes in the file.
         * Have to do this after the fsContext_lock
         * is released since advfs_get_xtnt_map()
         * and the routines it calls may block and
         * probably will take other locks.
         */
        {
            bsExtentDescT fake_xtnts;
            int32_t       fake_xtnt_cnt;
            vdIndexT      vd_idx;
            sts = advfs_get_xtnt_map( bfap,
                        0,      /* start xtnt */
                        0, 
                        &fake_xtnts,
                        &fake_xtnt_cnt,
                        &fob_cnt,
                        &vd_idx,
                        MPF_FOB_CNT );
        }

        if (sts != EOK) {
            vap->va_blocks = 0;      /* XXX */
        } else {
            vap->va_blocks = fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE;
        }
        ADVFS_VOP_LOCKUNSAFE;

        return sts;
    }

    /*
     * copy the attributes from the stat structure in the file's
     * in-memory directory entry to the vap structure
     */
    ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );
    /*
     * update the time fields if not readonly fs.
     * if readonly just read what's in fscontext
     */
    if (vfsp->vfs_flag & VFS_RDONLY) {
        vap->va_atime.tv_sec = context_ptr->dir_stats.st_atime;
        vap->va_atime.tv_nsec = context_ptr->dir_stats.st_natime;
        vap->va_mtime.tv_sec = context_ptr->dir_stats.st_mtime;
        vap->va_mtime.tv_nsec = context_ptr->dir_stats.st_nmtime;
        vap->va_ctime.tv_sec = context_ptr->dir_stats.st_ctime;
        vap->va_ctime.tv_nsec = context_ptr->dir_stats.st_nctime;
        goto no_update;
    }
    if (context_ptr->fs_flag & (MOD_MTIME | MOD_ATIME | MOD_CTIME)) {
       ADVFS_GET_SYSTEM_TIME( new_time );
    }
    /*
     * either use the fscontext time or the system time.
     * try to minimize the dividing and multiplying of usecs.
     */
    if (context_ptr->fs_flag & MOD_MTIME) {
        vap->va_mtime.tv_sec = new_time.tv_sec;
        vap->va_mtime.tv_nsec = new_time.tv_nsec;
        context_ptr->dir_stats.st_mtime = new_time.tv_sec;
        MS_SMP_ASSERT(new_time.tv_nsec <= INT_MAX);
        context_ptr->dir_stats.st_nmtime = (int32_t)new_time.tv_nsec;
    } else {
        vap->va_mtime.tv_sec = context_ptr->dir_stats.st_mtime;
        vap->va_mtime.tv_nsec = context_ptr->dir_stats.st_nmtime;
    }
    if (context_ptr->fs_flag & MOD_ATIME) {
        vap->va_atime.tv_sec = new_time.tv_sec;
        vap->va_atime.tv_nsec = new_time.tv_nsec;
        context_ptr->dir_stats.st_atime = new_time.tv_sec;
        MS_SMP_ASSERT(new_time.tv_nsec <= INT_MAX);
        context_ptr->dir_stats.st_natime = (int32_t)new_time.tv_nsec;
    } else {
        vap->va_atime.tv_sec = context_ptr->dir_stats.st_atime;
        vap->va_atime.tv_nsec = context_ptr->dir_stats.st_natime;
    }
    if (context_ptr->fs_flag & MOD_CTIME) {
        vap->va_ctime.tv_sec = new_time.tv_sec;
        vap->va_ctime.tv_nsec = new_time.tv_nsec;
        context_ptr->dir_stats.st_ctime = new_time.tv_sec;
        MS_SMP_ASSERT(new_time.tv_nsec <= INT_MAX);
        context_ptr->dir_stats.st_nctime = (int32_t)new_time.tv_nsec;
    } else {
        vap->va_ctime.tv_sec = context_ptr->dir_stats.st_ctime;
        vap->va_ctime.tv_nsec = context_ptr->dir_stats.st_nctime;
    }
    context_ptr->fs_flag &= ~(MOD_MTIME | MOD_ATIME | MOD_CTIME);

no_update:
    vap->va_fsid      = bfSetp->bfs_dev; /*  == f_fsid.val[0] */
    vap->va_fstype    = vp->v_vfsp->vfs_mtype;
    vap->va_realdev   = bfSetp->bfs_dev;
    vap->va_nodeid    = BS_BFTAG_IDX(context_ptr->dir_stats.st_ino);
    vap->va_basemode  = context_ptr->dir_stats.st_mode;
    vap->va_mode      = vap->va_basemode;
    vap->va_uid       = context_ptr->dir_stats.st_uid;
    vap->va_gid       = context_ptr->dir_stats.st_gid;
    vap->va_rdev      = context_ptr->dir_stats.st_rdev;
    vap->va_size      = bfap->file_size;
    vap->va_blocksize = pageSz;
    vap->va_type      = vp->v_type;
    vap->va_acl       = 0;

    /* Check for System V ACLs */
    if( bfap->bfaFlags & BFA_ACL_NOT_ONDISK ) {
	vap->va_aclv = 0;
    } else {
	vap->va_aclv = 1;
    }

    /*
     * See the comments explaining these asserts in the advfs_link() routine.
     */
    MS_SMP_ASSERT(MAXLINK == 32767);
    MS_SMP_ASSERT(LINK_MAX == 32767);
    MS_SMP_ASSERT(sizeof(((struct vattr *)0)->va_nlink)==sizeof(int16_t));
    MS_SMP_ASSERT(sizeof(((struct stat *)0)->st_nlink)==sizeof(int16_t));
    MS_SMP_ASSERT(sizeof(nlink_t) == sizeof(int16_t));
    /*
     * AdvFS has on-disk storage for greater than 32K nlinks, so this code
     * is just a precatution against a domain modified on a future release
     * being mounted on an older version of HP-UX that only supported 32K
     * nlink_t values.
     */
    vap->va_nlink = context_ptr->dir_stats.st_nlink;
    if ( context_ptr->dir_stats.st_nlink > MAXLINK ) {
        vap->va_nlink = MAXLINK;
    }

    /*
     * release mutex now because some of the 'bs' routines
     * below grab all sorts of locks and they can go to sleep.
     */
    ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

    /* the real number of bytes in the file */
    {
        bsExtentDescT fake_xtnts;
        int32_t       fake_xtnt_cnt;
        vdIndexT      vd_idx;
        sts = advfs_get_xtnt_map( bfap,
                        0,      /* start xtnt */
                        0, 
                        &fake_xtnts,
                        &fake_xtnt_cnt,
                        &fob_cnt,
                        &vd_idx,
                        MPF_FOB_CNT );
    }
    vap->va_blocks = fob_cnt / ADVFS_FOBS_PER_DEV_BSIZE;

    ADVFS_VOP_LOCKUNSAFE;
    return(0);
}


/*
 * advfs_setattr
 *
 * set the attributes for a file
 */

advfs_setattr(
    struct vnode *vp,    /* in - vnode pointer of file */
    struct vattr *vap,   /* in - vnode attribute structure */
    struct ucred *cred,  /* in - callers credentials */
    int    null_time)
{
    statusT sts=0;
    NFS_FREEZE_LOCK(vp);
    sts = fs_setattr(vp, vap, cred, null_time); /* pass advfs_setattr()'s args */
    NFS_FREEZE_UNLOCK(vp);
    return (sts);
}


/*
 * fs_setattr()
 *
 * This routine is called by advfs_setattr().
 */

fs_setattr(
    struct vnode *vp,     /* in - vnode pointer of file */
    struct vattr *vap,    /* in - vnode attribute structure */
    struct ucred *cred,   /* in - callers credentials */
    int    null_time)     /* in - hpux null_time */
{
    int error = 0;
    int oddoffset;
    statusT sts;
    bfAccessT *bfap;
    struct fsContext *context_ptr;
    uid_t iuid;
    ftxHT ftxH;
    bf_fob_t lastFobOffset;
    int updateStats = 1;
    int doing_chown = FALSE;
    off_t quota_rec_offset[MAXQUOTAS];
    int32_t quota_mig_stg_lock[MAXQUOTAS];
    int type;
    int chtime = 0;
    lock_t *sv_lock;


    FILESETSTAT( vp, advfs_setattr );

    MS_SMP_ASSERT(vp);
    bfap = VTOA(vp);
    MS_SMP_ASSERT(bfap != NULL);
    context_ptr = VTOC(vp);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
        return EIO;
    }

    /* we don't allow setattr on a reserved file */
    if (BS_BFTAG_RSVD(bfap->tag)) {
        return EINVAL;
    }
    
    /*
     * check for unsettable attributes
     */
    if ((vap->va_type      != -1) ||
        (vap->va_nlink     != -1) ||
        (vap->va_fsid      != -1) ||
        (vap->va_nodeid    != -1) ||
        (vap->va_blocksize != -1) ||
        (vap->va_rdev      != -1) ||
        (vap->va_realdev   != -1) ||
        (vap->va_blocks    != -1))   {

        return (EINVAL);
    }

    if (vp->v_vfsp->vfs_flag & VFS_RDONLY) {
        return EROFS;
    }

#ifdef WRITEABLE_SNAPSHOTS
    /* 
     * If we are calling advfs_setattr on a child snapshot, make sure the
     * child has its own metadata to store any changes 
     */
    if (HAS_SNAPSHOT_PARENT( bfap ) && (bfap->bfaFlags & BFA_VIRGIN_SNAP) ) {
        MS_SMP_ASSERT( !(bfap->bfaParentSnap->bfaFlags & BFA_VIRGIN_SNAP) );
        ADVFS_ACCESS_LOGIT( bfap, "fs_setattr calling setup cow");
        ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
        sts = advfs_setup_cow( bfap->bfaParentSnap,
                        bfap,
                        SF_SNAP_NOFLAGS,
                        FtxNilFtxH );
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
        if (sts != EOK) {
            return EIO;
        }
    }
#endif


    /*
     * update anything that isn't NODEV
     */

    /* 
     * synchronize with snapshot creation. After this point, any errors need
     * to goto exit to cleanup this counter.
     */
    ADVFS_SNAPSHOT_CREATE_SYNC( bfap );

    /*
     * truncate
     */
    if (vap->va_size != (uint64_t)-1) {
        void *delList;
        bfPageRefHT page_ref;
        char *page_addr;
        uint32_t delCnt = 0;
        uint16_t mask = S_ISUID;
        extern uint32_t noadd_execaccess;
        int remove_exec_perm;
        size_t au_size;                 /* Allocation unit size */
        actRangeT *arp;
        fcache_map_dsc_t *fcmap;        /* UFC mapping for file */
        size_t bytes_left;              /* Bytes yet to be written */
        size_t bytes_to_zero;           /* Bytes to zero in current bzero */
        off_t next_write_offset;        /* Offset at which to write next */
        fau_flags_t unmap_flags;
        int err;
        off_t orig_file_size;
        struct nameidata *ndp = NAMEIDATA();
        struct vnode *saved_vnode;

        au_size = bfap->bfPageSz * ADVFS_FOB_SZ;
        if ((error = chk_quota_write(VTOC(vp))) != 0) {
            goto _exit;
        }

        ADVFS_ACCESS_LOGIT( bfap, "fs_setattr doing truncation");

        /*
         *  XXX - check for write access as in utime below?
         */

        VN_SPINLLOCK(vp, sv_lock);
        if (vp->v_type == VDIR) {
            VN_SPINLUNLOCK(vp, sv_lock);
            error = EISDIR;
            goto _exit;
        }
        VN_SPINLUNLOCK(vp, sv_lock);

        /* Allocate an active range structure; */
        arp = advfs_bs_get_actRange(TRUE);

        /* 
         * Prevent rmvol from removing the volume in between 
         * bf_setup_truncation() and stg_remove_stg_finish().
         */
        ADVRWL_RMVOL_TRUNC_READ(bfap->dmnP);

        ADVRWL_BFAPCACHEMODE_READ(bfap);
        ADVRWL_FILECONTEXT_WRITE_LOCK( context_ptr );

        /* If this file has children snapshots, force a COW of the data to
         * be truncated.  If the file is being extended, there is nothing to
         * do */
        if ( HAS_SNAPSHOT_CHILD( bfap ) ) {

            ADVFS_ACCESS_LOGIT( bfap, "fs_setattr calling force cow");

            (void)advfs_force_cow_and_unlink( bfap,
                                vap->va_size,
                                (size_t)0, /* To the end of the file */
                                vap->va_size == 0 ? 
                                    SF_SNAP_NOFLAGS:SF_NO_UNLINK,
                                    FtxNilFtxH);
            /* If advfs_force_cow_and_unlink failed, it marked the chlidren
             * as out of sync. A domain panic may have occured */
            if (bfap->dmnP->dmn_panic) {
                advfs_bs_free_actRange(arp, TRUE);
                ADVRWL_BFAPCACHEMODE_UNLOCK( bfap );
                ADVRWL_FILECONTEXT_UNLOCK( context_ptr );
                ADVRWL_RMVOL_TRUNC_UNLOCK(bfap->dmnP);
                error = EIO;
                goto _exit;
            }
        }


        /* Seize an active range from the truncation point to the
         * end of the file.  When this range is established, we
         * have locked out any directIO writers to this range, 
         * which is what we want.
         */
        if (bfap->file_size < vap->va_size) {
            /* File is growing, so just to be consistent with Direct I/O
             * writes that grow the file (see advfs_getpage()) 
             */
            arp->arStartFob = ADVFS_AU_ROUND_DOWN(bfap, bfap->file_size);
        }
        else {
            /* File is shrinking */
            arp->arStartFob = ADVFS_AU_ROUND_DOWN(bfap, vap->va_size);
        }
        arp->arEndFob   = ~0L;          /* biggest possible block */
        arp->arState    = AR_TRUNCATE;
        arp->arDebug    = SET_LINE_AND_THREAD(__LINE__);
        (void)insert_actRange_onto_list( bfap, arp, context_ptr );

        /* Convert the new file offset to the user file's allocation
         * unit-aligned file byte offset
         */
        lastFobOffset = rounddown(vap->va_size, au_size);

        /* If the file is being truncated smaller, then zero the file data
         * between the new ending offset and the end of the allocation unit
         * that the new file ending offset will be. 
         */
        if (bfap->file_size > vap->va_size) {
            /* Calculate offset into the user file's ending allocation unit. */
            oddoffset = vap->va_size - lastFobOffset;

            if ((oddoffset) || 
                ((ADVFS_FILE_HAS_SMALL_AU(bfap)) && 
                 (vap->va_size > 0) &&
                 (vap->va_size < (off_t)VM_PAGE_SZ))) {
                /* This method of zeroing the file data must only be done
                 * on user data files and never metadata files that must
                 * use bs_pinpg().
                 */
                MS_SMP_ASSERT(bfap->dataSafety == BFD_USERDATA);

                /*
                 * If the file is not currently a small file but is about
                 * to become one, it is not enough to just zero-fill from
                 * the odd offset to the end of the allocation unit.  We
                 * must zero to the end of the VM page, since that page will
                 * remain in memory.
                 */ 
                if (ADVFS_FILE_HAS_SMALL_AU(bfap) && 
                    !(ADVFS_SMALL_FILE(bfap)) &&
                    roundup(vap->va_size, au_size) < (off_t)VM_PAGE_SZ) {
                    bytes_left = VM_PAGE_SZ - vap->va_size;
                }
                else {
                    bytes_left = min(bfap->file_size - vap->va_size ,
                                     (au_size - oddoffset));
                }


                next_write_offset = vap->va_size;

                while (bytes_left > 0) {
                    /* Set up the mapping for the next part of the write.
                     * This tries to get the full bytes_left but UFC may
                     * return a different map size.
                     */
                    if (!(fcmap = fcache_as_map(bfap->dmnP->userdataVas,
                                                vp, 
                                                next_write_offset,
                                                bytes_left,
                                                (size_t)0,
                                                FAM_WRITE,
                                                &error))) {
                        break;
                    }
                    /* 
                     * Zero the user allocation unit beyond the new
                     * end-of-file, if and only if it is not a sparse hole.
                     * We do this because the unused portion of this
                     * allocation unit could be exposed if the file is
                     * extended via lseek/write or ftruncate in the future.
                     */
                    bytes_to_zero = min(bytes_left,fcmap->fm_size);
                    if ( advfs_bs_is_offset_mapped(bfap, next_write_offset) ) {
                        /*
                         * We are holding the file lock, so use NI_RW
                         * to tell advfs_getpage() that it does not need to
                         * aquire the file lock.
                         */
                        ndp->ni_nameiop |= NI_RW;
                        saved_vnode = ndp->ni_vp;
                        ndp->ni_vp = vp;
                        bzero((caddr_t)fcmap->fm_vaddr, bytes_to_zero);
                        ndp->ni_nameiop &= ~NI_RW;
                        ndp->ni_vp = saved_vnode;
                        /* Unmap sync. We need these out to disk since
                         * we will invalidate them shortly
                         */
                        unmap_flags = FAU_SYNC | FAU_CACHE;
                    }
                    else {
		        /* No modifications made, just unmap. */
                        unmap_flags = FAU_CACHE;
                    }
                    if (error = fcache_as_unmap(fcmap, NULL, 
                                                (size_t)0, unmap_flags)) {
                        break;
		    }

                    bytes_left -= bytes_to_zero;
                    next_write_offset += bytes_to_zero;
                }

                /* If this file is open for directIO, then we must flush
                 * and invalidate any pages that were brought into the 
                 * cache before we drop the active range.
                 */
                if (bfap->dioCnt) {
                    error = fcache_vn_flush(&bfap->bfVnode, 
                                    arp->arStartFob * ADVFS_FOB_SZ,
                                    (size_t)0,  /* the rest of the file */
                                    NULL, 
                                    FVF_WRITE | FVF_SYNC | FVF_INVAL );
                    if (error) {
                        goto notrunc;
                    }
                }
            }
        }
        else if ((ADVFS_SMALL_FILE(bfap)) && 
                 (bfap->bfaNextFob > 0) &&
                 (roundup(bfap->file_size, bfap->bfPageSz * ADVFS_FOB_SZ)  < 
                  roundup(vap->va_size, bfap->bfPageSz * ADVFS_FOB_SZ))) {

            size_t bytes_to_allocate;
            size_t bytes_faulted = 0;
            off_t  file_size_rounded_up_to_au =
                   roundup(bfap->file_size, bfap->bfPageSz * ADVFS_FOB_SZ);
            struct advfs_pvt_param priv_param;
            faf_status_t faf_status;

            /*
             * We are truncating out a small file and the new size ends in
             * an allocation unit that is different from the one in which the
             * file currently ends.  We will allocate storage as necessary
             * to prevent creating a hole smaller than VM_PAGE_SZ.
             */
            if (vap->va_size >= (off64_t)VM_PAGE_SZ) {
                /*
                 * New file size is at least as large as a VM page.  
                 * We'll allocate storage to back the first VM_PAGE_SZ 
                 * bytes of the file.
                 */
                bytes_to_allocate = VM_PAGE_SZ - file_size_rounded_up_to_au;
            }
            else {
                /*
                 * New file size is smaller than a VM page.  This will
                 * remain a small file.  We will only allocate as many
                 * allocation units as necessary.
                 */
                bytes_to_allocate = 
                  roundup(vap->va_size, bfap->bfPageSz * ADVFS_FOB_SZ) -
                  file_size_rounded_up_to_au;
            }

            if (!(fcmap = fcache_as_map(bfap->dmnP->userdataVas, 
                                        &bfap->bfVnode,
                                        file_size_rounded_up_to_au,
                                        bytes_to_allocate, 
                                        0,
                                        FAM_WRITE, 
                                        &error))) {
                /*
                 * If it's our fault, panic.
                 */ 
                if (error == EINVAL) {
                    ADVFS_SAD0("fs_setattr: bad call to UFC");
                }
                goto notrunc;
            }
 
            bzero((caddr_t)&priv_param, sizeof(struct advfs_pvt_param));
            priv_param.app_total_bytes = bytes_to_allocate;
            priv_param.app_starting_offset = file_size_rounded_up_to_au;
            priv_param.app_flags = 0;
            priv_param.app_ra_plan = NULL;
            
            /*
             * This call will cause storage to be allocated.  We explicitly
             * then call bzero() to force the page to be marked dirty.
             * We are holding the file lock, so use NI_RW to tell 
             * advfs_getpage() that it does not need to acquire the file lock.
             */
            ndp->ni_nameiop |= NI_RW;
            saved_vnode = ndp->ni_vp;
            ndp->ni_vp = vp;
            error = fcache_as_fault(fcmap, 
                                    NULL,
                                    &bytes_faulted,
                                    &faf_status,
                                    (uintptr_t)&priv_param,
                                    FAF_WRITE|FAF_SYNC|FAF_GPAGE);
            if (error) {
                ndp->ni_nameiop &= ~NI_RW;
                ndp->ni_vp = saved_vnode;
                /*
                 * If it's our fault, panic.
                 */ 
                if (error == EINVAL) {
                    ADVFS_SAD0("fs_setattr: bad call to UFC");
                }

                (void)fcache_as_unmap(fcmap, NULL, 0, FAU_CACHE | FAU_WBEHIND);
                goto notrunc;
            }

            /*
             * Explicitly re-bzero() one byte so that VM marks the page 
             * as dirty.  We may fault back into advfs_getpage() here
             * so keep the NI_RW flag set so advfs_getpage() doesn't
             * try to re-lock the file lock.
             */
            bzero((caddr_t)fcmap->fm_vaddr, 1);

            ndp->ni_nameiop &= ~NI_RW;
            ndp->ni_vp = saved_vnode;

            error = fcache_as_unmap(fcmap,
                                    NULL,
                                    0,
                                    FAU_CACHE | FAU_WBEHIND);
            if (error) {
                /*
                 * If it's our fault, panic.
                 */ 
                if (error == EINVAL) {
                    ADVFS_SAD0("fs_setattr: bad call to UFC");
                }
                goto notrunc;
            }

            /* If this file is open for directIO, then we must flush
             * and invalidate any pages that were brought into the 
             * cache before we drop the active range.
             */
            if (bfap->dioCnt) {
                error = fcache_vn_flush(&bfap->bfVnode, 
                                arp->arStartFob * ADVFS_FOB_SZ,
                                (size_t)0, /* the rest of the file */
                                NULL, 
                                FVF_WRITE | FVF_SYNC | FVF_INVAL );
                if (error) {
                    goto notrunc;
                }
            }
        }

        /*
         *  File size and the extent map must change atomically with respect 
         *  to any other write operations being done concurrently on this file.
         */
        ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );

        /*
         *  Update the size and time.  If we are growing the size of the
         *  file, then changing bfap->file_size, and thus creating a sparse
         *  hole at the end of the file, is all that we need to do.
         */
        context_ptr->fs_flag |= (MOD_CTIME | MOD_MTIME);
        context_ptr->dirty_stats = TRUE;
        ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

        /*
         *  Update mode (don't clear enforcement mode lock bits).
         *  indicated by setgid bit, but no group execute.
         */
        if ((!context_ptr->dir_stats.st_mode & S_ISGID) ||
            (context_ptr->dir_stats.st_mode & S_IXGRP)) {
            if (BSD_MODE(vp))
                mask |= S_ISGID;
        }

        context_ptr->dir_stats.st_mode &= ~mask;

        if ( bfap->file_size <= vap->va_size ) {

            /* new size is same or larger - nothing to do */
            bfap->file_size = vap->va_size;
            ADVFS_ACCESS_LOGIT( bfap, "fs_setattr finish trunc out");

            goto notrunc;
        } else if (HAS_SNAPSHOT_PARENT( bfap ) ) {
#ifdef WRITEABLE_SNAPSHOTS
            /* If this is a snapshot child that is being truncated, the
             * original file size needs to be updated so we no longer
             * attempt to COW beyond the truncated size. */

            bsBfAttrT  bf_attr_rec;
            sts = bmtr_get_rec( bfap,
                                BSR_ATTR,
                                &bf_attr_rec,
                                sizeof( bsBfAttrT ) );
            if (sts != EOK) {
                /* If we can't update the original file size, there is a
                 * problem.  This snapshot is out of sync, but the domain is
                 * probably paniced too (failed to read metadata). In any
                 * case, domain panic and return an error since we were
                 * dealing with metadata.
                 */
                domain_panic(bfap->dmnP,
                         "advfs_setattr: bmtr_get_rec error, return code = %d",
                         sts);
                ADVRWL_FILECONTEXT_UNLOCK( context_ptr );
                ADVRWL_RMVOL_TRUNC_UNLOCK(bfap->dmnP);
                ADVRWL_BFAPCACHEMODE_UNLOCK( bfap );
                error = EIO;
                goto _exit;
            }
                                        
            bf_attr_rec.bfat_orig_file_size = vap->va_size;

            MS_SMP_ASSERT( !(bfap->bfaFlags & BFA_VIRGIN_SNAP) );
            sts = bmtr_update_rec( bfap, 
                    BSR_ATTR, 
                    &bf_attr_rec,
                    sizeof( bsBfAttrT ),
                    FtxNilFtxH,
                    0 );
            if ( sts != EOK ) {
                domain_panic(bfap->dmnP,
                         "advfs_setattr: bmtr_update_rec(1) error, return code = %d",
                         sts);
                ADVRWL_FILECONTEXT_UNLOCK( context_ptr );
                ADVRWL_RMVOL_TRUNC_UNLOCK(bfap->dmnP);
                ADVRWL_BFAPCACHEMODE_UNLOCK( bfap );
                error = EIO;
                goto _exit;
            }
#endif
        }

        if ((off_t) bfap->bfaNextFob*ADVFS_FOB_SZ < bfap->file_size) {
            /* If the file ended in a hole (ie a prior truncation
             * into a hole) then we need to invalidate any pages
             * in this hole. 
             */

            /* Round off the VM page, it was zeroed above. We will
             * invalidate all pages beyond this. This maybe done again
             * in stg_remove_stg but should be quick.
             */
            size_t new_size = roundup(vap->va_size,VM_PAGE_SZ);

            /* It is important that we not update file size until
             * after this call. Putpage uses file_size.
             */

            MS_SMP_ASSERT( new_size % NBPG == 0 );
            err = fcache_vn_invalidate(&bfap->bfVnode,
                                       new_size,
                                       0, /* trunc to eof */
                                       NULL,
                                       FVI_TRUNC);
            MS_SMP_ASSERT(err == 0);

        }

        orig_file_size  = bfap->file_size;
        bfap->file_size = vap->va_size;

        ADVFS_ACCESS_LOGIT( bfap, "fs_setattr updated file size ");


        /* Prevent races with migrate code paths.
         * bf_setup_truncation() requires migStgLk locked.
         * This lock must be locked before starting a root ftx.
         */
        ADVRWL_MIGSTG_READ( bfap );

        /* No need to take the quota lock here. Storage is only
         * added to the quota files at file creation, chown
         * and explicit quota setting.
         * We DO need to take it later on in this function
         * because of the advfs_chown  call.
         */

        FTX_START_N (
                     FTA_OSF_SETATTR_1,
                     &ftxH,
                     FtxNilFtxH,
                     context_ptr->fsc_fsnp->dmnP
                     );



        if ( fs_trunc_test(vp, FALSE) ) {
            if ( !bfap->trunc ) {
                delCnt = 0;
            } else {
                off_t rnd_file_size;
                off_t rnd_alloc_size;

                sts = bf_setup_truncation( bfap, ftxH, 0L, &delList, &delCnt );

                bfap->trunc = 0;

                if (sts != EOK) {

                    bfap->file_size = orig_file_size;
                    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_491);
                    ftx_fail (ftxH);
                    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_492);
                    ADVRWL_MIGSTG_UNLOCK( bfap );
                    ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
                    ADVRWL_RMVOL_TRUNC_UNLOCK(bfap->dmnP);
                    ADVRWL_FILECONTEXT_UNLOCK( context_ptr );

                    error = sts;
                    goto _exit;
                }
                
                if (((off_t)bfap->bfaNextFob*ADVFS_FOB_SZ < bfap->file_size) &&
                    ((rnd_alloc_size = roundup(bfap->file_size,
                                              bfap->bfPageSz*ADVFS_FOB_SZ)) >
                     (rnd_file_size = roundup(bfap->file_size,
                                              VM_PAGE_SZ)))) {
                    /* If the file now ends in a hole that is more than
                     * a VM_PAGE away from the end of an allocation unit.
                     * The call to remove storage rounds the file size 
                     * up to an allocation unit regardless of if it is
                     * over a hole or not. It will then invalidate all
                     * the pages in this range potentially leaving pages
                     * in the cache. We need to invalidate these now.
                     */

                    err = fcache_vn_invalidate(&bfap->bfVnode,
                                               rnd_file_size,
                                               rnd_alloc_size - rnd_file_size,
                                               NULL,
                                               FVI_INVAL);
                    MS_SMP_ASSERT(err == 0);
                    
                }

            }
        }

        sts = fs_update_stats(vp, bfap, ftxH, 0);
        if (sts != EOK) {
            domain_panic(bfap->dmnP,
                         "advfs_setattr: fs_update_stats (1) error, return code = %d",
                         sts);
            ftx_done_fs (ftxH, FTA_OSF_SETATTR_1);
            ADVRWL_MIGSTG_UNLOCK( bfap );
            ADVRWL_FILECONTEXT_UNLOCK( context_ptr );
            ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
            ADVRWL_RMVOL_TRUNC_UNLOCK(bfap->dmnP);
            error = EIO;  /* E_DOMAIN_PANIC */
            goto _exit;
        }

        updateStats = 0;

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_11);

        ftx_done_fs (ftxH, FTA_OSF_SETATTR_1);

        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_12);

        ADVRWL_MIGSTG_UNLOCK( bfap );

        if ( delCnt ) {
            stg_remove_stg_finish( bfap->dmnP, delCnt, delList );
        }

        /*
         *  Releasing this lock here is required for the mss_truncate
         *  call below, but given the current locking scheme,
         *  it may open up a race between truncate and advfs_fs_write.
         *
         *  TODO - why does it need to be released here?
         */

notrunc:
        /* Get rid of the activeRange applied above. */
        ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );
        spin_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);
        spin_unlock(&bfap->actRangeLock);
        advfs_bs_free_actRange(arp, TRUE);

        ADVRWL_FILECONTEXT_UNLOCK( context_ptr );
        ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
        ADVRWL_RMVOL_TRUNC_UNLOCK(bfap->dmnP);

        if (error) {
            goto _exit;
        }
    }

    iuid = context_ptr->dir_stats.st_uid;
    /*
     * utime
     */
#define OWNERPERM(iuid, cred) \
                 ((iuid == (cred)->cr_uid) || ((cred)->cr_uid == 0))

    if (vap->va_atime.tv_sec != (int64_t)-1 ||
        vap->va_mtime.tv_sec != (int64_t)-1) {
        /*
         * Only need to do further checking if we don't
         * own the file or we are not root.
         */
        if (!OWNERPERM(iuid, cred)) {
            if (!null_time) {
                error = EPERM;
                goto _exit;
            } else if (error = advfs_access(vp, S_IWRITE, cred)) {
                goto _exit;
            }
        }
        ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock);

        /* atime */
        if (vap->va_atime.tv_sec != (int64_t)-1) {
            context_ptr->dir_stats.st_atime = vap->va_atime.tv_sec;
            MS_SMP_ASSERT(vap->va_atime.tv_nsec <= INT_MAX);
            context_ptr->dir_stats.st_natime = (int32_t)vap->va_atime.tv_nsec;
            context_ptr->fs_flag &= ~(MOD_ATIME);
        }
        
        /* mtime */
        if (vap->va_mtime.tv_sec != (int64_t)-1) {
            context_ptr->dir_stats.st_mtime = vap->va_mtime.tv_sec;
            MS_SMP_ASSERT(vap->va_mtime.tv_nsec <= INT_MAX);
            context_ptr->dir_stats.st_nmtime = (int32_t)vap->va_mtime.tv_nsec;
            context_ptr->fs_flag &= ~(MOD_MTIME);
        }

        context_ptr->dirty_stats = TRUE;
        ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock);
        updateStats = 1;
        chtime++;
    }

    if (vap->va_mtime.tv_sec != (int64_t)-1) {
        if (!OWNERPERM(iuid, cred) && 
            (null_time ||      
             vap->va_mtime.tv_nsec < 0 ||
             vap->va_mtime.tv_nsec >= 1000000000L)) {
            if (!(error = advfs_access(vp, S_IWRITE, cred))) {
                timestruc_t new_time;
                ADVFS_GET_SYSTEM_TIME( new_time );
                ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock);
                MS_SMP_ASSERT(new_time.tv_nsec <= INT_MAX);
                context_ptr->dir_stats.st_mtime  = new_time.tv_sec;
                context_ptr->dir_stats.st_nmtime = (int32_t)new_time.tv_nsec;
                context_ptr->dir_stats.st_atime  = new_time.tv_sec;
                context_ptr->dir_stats.st_natime = (int32_t)new_time.tv_nsec;
                ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock);
                updateStats = 1;
            } else {
                goto _exit;
            }
        }
        chtime++;
    }
    
    if ((vap->va_ctime.tv_sec != (int64_t)-1) && (chtime)) {
            /*
             * Special purpose fcntl (for F_SETTIMES)
             *
             * No need to check for write access as only su can
             * be in this code path.
             */

            ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock);
            if ((vap->va_atime.tv_sec != 0) || (vap->va_atime.tv_nsec != 0)) {
                context_ptr->dir_stats.st_atime = vap->va_atime.tv_sec;
                MS_SMP_ASSERT(vap->va_atime.tv_nsec <= INT_MAX);
                context_ptr->dir_stats.st_natime = 
                                                (int32_t)vap->va_atime.tv_nsec;
                context_ptr->fs_flag &= ~(MOD_ATIME);
            }
            if ((vap->va_mtime.tv_sec != 0) || (vap->va_mtime.tv_nsec != 0)) {
                context_ptr->dir_stats.st_mtime = vap->va_mtime.tv_sec;
                MS_SMP_ASSERT(vap->va_mtime.tv_nsec <= INT_MAX);
                context_ptr->dir_stats.st_nmtime = 
                                                (int32_t)vap->va_mtime.tv_nsec;
                context_ptr->fs_flag &= ~(MOD_MTIME);
            }
            if ((vap->va_ctime.tv_sec != 0) || (vap->va_ctime.tv_nsec != 0)) {
                context_ptr->dir_stats.st_ctime = vap->va_ctime.tv_sec;
                MS_SMP_ASSERT(vap->va_ctime.tv_nsec <= INT_MAX);
                context_ptr->dir_stats.st_nctime = 
                                                (int32_t)vap->va_ctime.tv_nsec;
                context_ptr->fs_flag &= ~(MOD_CTIME);
            }
            context_ptr->dirty_stats = TRUE;
            ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock);

            if (sts = fs_update_stats(vp, bfap, FtxNilFtxH, 0) != EOK) {
                domain_panic(bfap->dmnP,
                             "advfs_setattr: fs_update_stats (2) error, return code = %d",
                             sts);
                error = EIO; /* E_DOMAIN_PANIC */
                goto _exit;
            }

            error = BSERRMAP(error);
            goto _exit;
    }

    /*
     * change mode
     */
    if (vap->va_mode != (u_short)-1) {
        if (error = advfs_chmod(
                               vp,
                               context_ptr,
                               vap->va_mode,
                               cred)) {
            goto _exit;
        }
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
    quota_mig_stg_lock[USRQUOTA] = quota_mig_stg_lock[GRPQUOTA] = FALSE;
    if (vap->va_uid != UID_NO_CHANGE || vap->va_gid != GID_NO_CHANGE) {
        doing_chown = TRUE;
        ADVRWL_FILECONTEXT_WRITE_LOCK(context_ptr);
    }



    /*
     * Get the migStgLk for quotas if necessary.
     */
    quota_mig_stg_lock[USRQUOTA] = quota_mig_stg_lock[GRPQUOTA] = FALSE;
    if (doing_chown) {
        /* only take the lock if we are changing owners */
        quota_rec_offset[USRQUOTA] = (off_t)vap->va_uid*sizeof(struct dqblk64);
        quota_rec_offset[GRPQUOTA] = (off_t)vap->va_gid*sizeof(struct dqblk64);
        for (type = 0; type < MAXQUOTAS; type++) {
            if (!advfs_bs_is_offset_mapped(
                    context_ptr->fsc_fsnp->qi[type].qiAccessp,
                    quota_rec_offset[type])) {
                ADVRWL_MIGSTG_READ( context_ptr->fsc_fsnp->
                                      qi[type].qiAccessp );
                quota_mig_stg_lock[type] = TRUE;
            }
        }
    }

    FTX_START_N(FTA_OSF_SETATTR_2, &ftxH, FtxNilFtxH,
                context_ptr->fsc_fsnp->dmnP);


    /*
     * change owner
     */
    if (doing_chown) {

        if (error = advfs_chown(vp, vap->va_uid, vap->va_gid, cred, ftxH )) {
            ADVRWL_FILECONTEXT_UNLOCK(context_ptr);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_493);
            ftx_fail(ftxH);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_494);
            for (type = 0; type < MAXQUOTAS; type++) {
                if (quota_mig_stg_lock[type]) {
                    ADVRWL_MIGSTG_UNLOCK( context_ptr->fsc_fsnp->
                                       qi[type].qiAccessp );
                    quota_mig_stg_lock[type] = FALSE;
                }
            }
            goto _exit;
        }
        updateStats = 1;
    }

    if ( updateStats ) {

        ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock);
        context_ptr->fs_flag |= MOD_CTIME;
        context_ptr->dirty_stats = TRUE;
        ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock);
 
        sts = fs_update_stats(vp, bfap, ftxH, 0);
        if (sts != EOK) {
            domain_panic(bfap->dmnP,
                         "advfs_setattr: fs_update_stats (3) error, return code = %d",sts);
            error =  EIO;
        }
    }

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_23);

    ftx_done_fs(ftxH, FTA_OSF_SETATTR_2);

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_24);

    if (doing_chown) {
        ADVRWL_FILECONTEXT_UNLOCK(context_ptr);
        for (type = 0; type < MAXQUOTAS; type++) {
            if (quota_mig_stg_lock[type]) {
                ADVRWL_MIGSTG_UNLOCK( context_ptr->fsc_fsnp->
                                   qi[type].qiAccessp );
                quota_mig_stg_lock[type] = FALSE;
            }
        }
    }

    /*
     * Sync with snapset creation
     */
    ADVFS_SNAPSHOT_DONE_SYNC( bfap );

    return(0);

_exit:

    /*
     * Remove the count that synchronizes with snapset creation.
     */
    ADVFS_SNAPSHOT_DONE_SYNC( bfap );

    return(error);
}


/*
 * advfs_rdwr() - read or write the specified vnode.
 */
advfs_rdwr(
    struct vnode *vp,     /* in - vnode of the file to read/write    */
    struct uio   *uiop,   /* in - uio struct with what to read/write */
    enum   uio_rw rw,     /* in - read/write flag                    */
    int           ioflag, /* in - flags like, sync, append, etc...   */
    struct ucred *cred)   /* in - caller's credentials               */
{
    int locked_vnode = FALSE;
    int enforced;
    uint16_t error;

    /*
     * Check for any lock ranges that conflict with this request.
     */
    if (vp->v_type == VREG) {
        /*
         * See if the region is locked, and wait for it to
         * become unlocked if it is.  Return only on DEADLOCK
         * or out of table space.
         *
         * NOTE:  I'm wondering why this code is in each VOP_RDWR() and not
         *        part of the VFS layer itself.
         */
        if (vp->v_locklist || vp->v_shrlocks){
            enforced = enforcement_mode(vp);
            VNODE_LOCKX(vp, locked_vnode);
            if (error = lockedx(F_LOCK, vp, uiop->uio_offset,
                  uiop->uio_offset+uiop->uio_iov->iov_len,
                  (rw==UIO_READ)?L_READ:L_WRITE, NULL,
                  0,uiop->uio_fpflags, enforced)) {
                error = u.u_error;
                VNODE_UNLOCKX(vp, locked_vnode);
                return(error);
            }
            VNODE_UNLOCKX(vp, locked_vnode);
        }
    }

    if (uiop->uio_resid == 0)
        return(0);
    if ((long) uiop->uio_offset < 0)
        return(EINVAL);

    /* Process the read or the write as specified by the caller. */
    if ( rw == UIO_READ ) {
        FILESETSTAT( vp, advfs_read );
        return(advfs_fs_read(vp, uiop, rw, ioflag, cred));
    }
    else if ( rw == UIO_WRITE ) {
        int ret;

        FILESETSTAT( vp, advfs_write );
        if ((ret = chk_quota_write(VTOC(vp))) != 0)
            return ret;

        NFS_FREEZE_LOCK(vp);
        ret = advfs_fs_write(vp, uiop, rw, ioflag, cred);
        NFS_FREEZE_UNLOCK(vp);
        return(ret);
    }
    else {
        MS_SMP_ASSERT(0);
        return(EINVAL);
    }
}


/*
 * advfs_ioctl()
 *
 * Description:
 *
 * advfs_ioctl() handles the following commands:
 *
 * - GETCACHEPOLICY and SETCACHEPOLICY -
 * The cache policies currently supported are CS_DIRECTIO (direct I/O
 * enabled) and CS_CACHE (filesystem's default cache policy is enabled).
 *
 * - GETMAP -
 * This retrieves a file's sparseness map in support of the AdvFS 
 * backup API project.
 *
 * - ADVFS_FREEZEFS, ADVFS_FREEZEQUERY, ADVFS_THAWFS
 * This will freeze or thaw a file system, or get the frozen status of
 * a file system. The timeout value is the user-supplied time in seconds
 * that the file system will remain frozen. 
 *
 * Parameters:
 *
 * struct vnode *vp      vnode pointer (in)
 * int cmd               Command, ADVFS_GETCACHEPOLICY, ADVFS_GETMAP, ... (in)
 * caddr_t data          command specific data (in and out)
 * int fflag             file.f_flag from the struct file structure (in)
 * struct ucred *cred    Calling thread's Credentials (in)
 */
int
advfs_ioctl(
    struct vnode *vp,    /* in - vnode of interest */
    int           cmd,   /* in - Operation to be performed */
    void         *data,  /* in/out - cmd dependent data in or out */
    int           fflag, /* in - file.f_flag from struct file */
    struct ucred *cred)  /* in - Caller's Credentials */
{
    struct fileSetNode *fsnp;
    int retval;
    int error;
    int err;
    int freeze_flag = ADVFS_Q_NOFLAG;
    int vn_locked = FALSE;
    lock_t *sv_lock;
    int blockdevice = 0;
    char *tempstring = NULL;
    char *setName=NULL;
    char *dmnName=NULL;
    advfs_ev *advfs_event;

    switch(cmd) {

    case ADVFS_SETEXT:
    {
        /* must not be a readonly file system */
        if (vp->v_vfsp->vfs_flag & VFS_RDONLY) {
            retval = EROFS;
            break;
        }

        /* must be a regular file */
        if (vp->v_type != VREG || VTOA(vp)->dataSafety != BFD_USERDATA) {
            retval = EINVAL;
            break;
        }

        /* now we can call advfs_setext() */
        retval = advfs_setext( vp, (advfs_ext_attr_t *)data );
        break;
    }
    case ADVFS_GETEXT:
    {
        /* must be a regular file */
        if (vp->v_type != VREG || VTOA(vp)->dataSafety != BFD_USERDATA) {
            retval = EINVAL;
            break;
        }

        /* now we can call advfs_getext() */
        retval = advfs_getext( vp, (advfs_ext_attr_t *)data );

        retval = ENOTSUP; /* temporary */
        break;
    }

    case ADVFS_GETCACHEPOLICY:
    {
        /*
         * File type must be VREG.
         */
        if (vp->v_type != VREG) {
            retval = ENOTTY;
            break;
        }

        retval = ESUCCESS;
        *(int *)data = ( VTOA(vp)->dioCnt ) ? 
                       ADVFS_DIRECTIO       : 
                       ADVFS_CACHED;
        break;
    }


    case ADVFS_GETMAP:
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

    case ADVFS_FREEZEFS:
        if ( freeze_flag == ADVFS_Q_NOFLAG ) freeze_flag = ADVFS_Q_FREEZE;
        /* fall through to commom freeze/thaw code */
    case ADVFS_FREEZEQUERY:
        if ( freeze_flag == ADVFS_Q_NOFLAG ) freeze_flag = ADVFS_Q_QUERY;
        /* fall through to commom freeze/thaw code */
    case ADVFS_THAWFS:
    {
        int timeout = 0;
        char *mount_name = VTOFSN(vp)->f_mntonname;

        if ( freeze_flag == ADVFS_Q_NOFLAG ) freeze_flag = ADVFS_Q_THAW;

        /*
         * Check for privileged user
         */
        if ( !suser() ) {
            return (EPERM);
        }

        /*
         *  Check for /, /usr and /var
         */
        if ( strcmp(mount_name,"/")    == 0  ||
             strcmp(mount_name,"/usr") == 0  ||
             strcmp(mount_name,"/var") == 0 ) {
            return(ENOTSUP);
	}

	/* we will only freeze a mount point */
	fsnp = ADVGETFILESETNODE(vp->v_vfsp);
	if (vp != fsnp->root_vp) {
            return(ENOTSUP);
	}

	/* if it's a freeze then get the timeout value */
	if ( freeze_flag == ADVFS_Q_FREEZE ) {
            timeout = *(int *)data;		
            if (timeout == 0 ) {
                timeout = (int)AdvfsFreezefsTimeout;
            } else if (timeout < 0) {
                timeout = 60*60*24*365; /* give it a year... */
            }
	}

	/* do the command */
        if ( freeze_flag == ADVFS_Q_THAW ) {
	    if ( vp->v_vfsp->vfs_flag & VFS_CFSONTOP ) {
		retval = CLU_THAWFS(vp->v_vfsp->vfs_fsid);
	    } else {
		retval = advfs_thaw (vp->v_vfsp);
		
	    }
	} else {
	    if ( vp->v_vfsp->vfs_flag & VFS_CFSONTOP ) {
		retval = CLU_FREEZEFS(vp->v_vfsp->vfs_fsid, freeze_flag, timeout );
	    } else {
		retval = advfs_freeze (vp->v_vfsp, freeze_flag, timeout );
		
	    }
	}

	/* interpret results of a query */
        if ( (freeze_flag == ADVFS_Q_QUERY ) ) {
	    if (retval == ESUCCESS) {
		*(int *)data = 0;
	    } else if (retval == ENOSPC) {
		retval = ESUCCESS;
		*(int *)data = 1;
	    }
	}
	

	tempstring = ms_malloc( MAXPATHLEN );
	error = advfs_parsespec(fsnp->f_mntfromname, 
				tempstring, 
				MAXPATHLEN, 
				&dmnName, 
				&setName, 
				&blockdevice);

	if ( error ) {
            ms_free(tempstring);
	    return error;
	}

	advfs_event = (advfs_ev *)ms_malloc( (uint32_t)sizeof(advfs_ev) );
	advfs_event->domain = dmnName;
	advfs_event->fileset = setName;

	if ( !(vp->v_vfsp->vfs_flag & VFS_CFSONTOP) && !retval ) {
            if ( freeze_flag == ADVFS_Q_FREEZE ) {
		sprintf (advfs_event->formatMsg,
			 (int)sizeof(advfs_event->formatMsg),
			 "Filesystem %s mounted on %s is frozen", 
			 dmnName, mount_name);
		(void)advfs_post_kernel_event(EVENT_FREEZE, advfs_event);
            } else if ( freeze_flag == ADVFS_Q_THAW ) {
		sprintf (advfs_event->formatMsg,
			 (int)sizeof(advfs_event->formatMsg),
			 "Filesystem %s mounted on %s has been thawed", 
			 dmnName, mount_name);
		(void)advfs_post_kernel_event(EVENT_THAW, advfs_event);
            }
        }

        ms_free(tempstring);
	ms_free( advfs_event );

	break;

    }

    default:
        retval = ENOTTY;
    }

    return(retval);
}


/*
 * advfs_fsctl
 */
/* ARGSUSED */
int
advfs_fsctl(
    struct vnode *vp,
    int cmd,
    struct uio *uiop,
    struct ucred *cred)
{
    return(EINVAL);
}

/*
 * advfs_select
 *
 *      Patterned after ufs_select().
 */
int
advfs_select(
    struct vnode *vp,
    int which,
    struct ucred *cred)
{
    MS_SMP_ASSERT(!ISVDEV(vp->v_type));
#undef ISVDEV
    return EINVAL;
}

/*
 * advfs_fsync
 *
 * used to sync a file (both data and stats).
 * Also used by  NFS write gather and CFS to sync a file's metadata.
 * fflags:
 *     FWRITE_DATA - flush only file's user data. Skip metadata stats update.
 *     FWRITE_METADATA - flush only dirty metadata stats.
 *     no flags (default) - flush the file's data and metadata stats to disk.
 *     
 *     If both FWRITE_DATA and FWRITE_METADATA are set, then both
 *     the file's user data and metadata stats will be flushed.
 *  
 */
int
advfs_fsync(
    struct vnode *vp,
    struct ucred *cred,
    int fflags)
{
    struct fsContext *file_context;
    bfAccessT *bfap;
    domainT *dmnP;
    ftxHT ftx_handle;
    statusT sts = EOK;

    FILESETSTAT( vp, advfs_fsync );

    file_context = VTOC(vp);
    bfap = VTOA(vp);
    dmnP = VTOFSN(vp)->dmnP;

    /* If caller sets both the user data only flush and metadata only flush
     * flags, then clear those flags to flush both types of data.
     * Otherwise, the logic in this routine would flush nothing.
     */
    if ((fflags&(FWRITE_DATA | FWRITE_METADATA)) ==
        (FWRITE_DATA | FWRITE_METADATA)) {
        fflags &= ~(FWRITE_DATA | FWRITE_METADATA);
    }

    NFS_FREEZE_LOCK(vp);

    /*
     * Take a shared lock to block other writers, but no need to block
     * against other readers.
     */
    ADVRWL_FILECONTEXT_READ_LOCK(file_context);

    /* sync file data except when caller only wants the metadata flushed. */
    if (!(fflags & FWRITE_METADATA)) {
        sts = fcache_vn_flush(vp, 
                              (off_t)0, 
                              (size_t)0, 
                              NULL, 
                              FVF_WRITE | FVF_SYNC);  
        if (sts != EOK) {
            sts = EIO;
            goto _exit;
        }
    }

    /* Sync file stats only if there are dirty stats to flush. 
     * Implicitly services FWRITE_METADATA flag support.
     * If the FWRITE_DATA flag is set, then caller wants stats update skipped.
     */
    if ((file_context->dirty_stats == TRUE) && !(fflags & FWRITE_DATA)){
        sts = FTX_START_N(FTA_OSF_FSYNC_V1, &ftx_handle, FtxNilFtxH, dmnP);
        if (sts != EOK) {
            sts = EIO;
            goto _exit;
        }
        sts = fs_update_stats(vp, bfap, ftx_handle, 0);
        if (sts != EOK) {
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_495);
            ftx_done_n(ftx_handle, FTA_OSF_FSYNC_V1);
            ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_496);
            sts = EIO;
            goto _exit;
        }
        file_context->dirty_alloc = FALSE;
        /* Do a syncronous write of the stats (for NFS). */
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_497);
        ftx_special_done_mode(ftx_handle, FTXDONE_LOGSYNC);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_25);
        ftx_done_n(ftx_handle, FTA_OSF_FSYNC_V1);
        ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_26);
    }

_exit:
    ADVRWL_FILECONTEXT_UNLOCK(file_context);
    NFS_FREEZE_UNLOCK(vp);
    return (sts);
}

/*
 * advfs_remove
 */
advfs_remove(
    struct vnode *dir_vp,       /* in - directory vnode 
                                 *      VFS has VN_HOLD, VFS will VN_RELE */
    char         *name,         /* in - file name to be deleted */
    struct ucred *cred)         /* in - credentials */
{
    statusT               sts;
    struct nameidata     *ndp;
    ni_nameiop_t          saved_op, int_op;
    bfAccessT            *remove_bfap;
    struct vnode         *remove_vp;
    struct bfNode        *dir_bnp;
    struct bfNode        *undel_bnp;
    struct vnode         *undel_dir_vp = NULL;
    struct vnode         *rvp = NULL;
    struct fsContext     *rem_context;
    struct fsContext     *dir_context;
    struct fsContext     *undel_context;
    bfAccessT            *dir_bfap;
    bfAccessT            *idx_bfap = NULL;
    ftxHT                 ftx_handle;
    bfSetT               *bfSetp;
    domainT              *dmnP;
    int                   error = 0;
    int                   qtype;
    struct fileSetNode   *fsnp;
    bfTagT                save_dir_tag;
    bfTagT                save_rem_tag;
    struct bsUndelDir  bmt_rec;
    int32_t               rem_context_locked = FALSE;
    int32_t               dir_context_locked = FALSE;
    int32_t               nfs_freeze_locked = FALSE;

    FILESETSTAT( dir_vp, advfs_remove );

    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp != NULL);
    saved_op = ndp->ni_nameiop;

    fsnp = VTOFSN(dir_vp);
    dmnP = fsnp->dmnP;
    bfSetp = fsnp->bfSetp;

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        return EIO;
    }

    dir_bnp     = VTOBNP(dir_vp);
    dir_context = dir_bnp->fsContextp;
    dir_bfap    = VTOA(dir_vp);

    if (error = advfs_access(dir_vp, S_IWRITE, cred))
        return (error);

    /*
     * The VFS layer did not give us the vnode of the file to be deleted, so
     * we have to go get it again.  We also want to fill in the nameidata
     * structure, especially the ni_count and ni_offset so we know where 
     * the file name is in the directory.
     *
     * NOTE:  While vn_remove() did do a lookup that filled in all the
     *        nameidata structure information, vns_remove() dropped the
     *        v_count reference on the vnode, so a race condition could have
     *        totally changed the underlying vnode.  Unlikely as it may be,
     *        it could still happen on a system under heavy memory pressure.
     *
     *        A suggested optimization would be to add a new NI_xxx flag to
     *        ni_nameiop that tells advfs_lookup() that we trust the other
     *        nameidata fields, but we need to verify/get the ni_vp value
     *        and if the dnlc has the correct value, please increment
     *        v_count and return the vnode to us, otherwise, please find the
     *        file in the directory and return the vnode with an incremented
     *        v_count.
     *
     *        HOWEVER, if we want to trust the nameidata structure, we MUST
     *        make sure that the caller really did a proper setup of the
     *        nameidata structure, so any optimization should test to see
     *        that the ni_nameiop field is set to NI_DELETE.  If it is not
     *        set to NI_DELETE, then this VOP was called by non-VFS layer
     *        code and we still need to do the full scale advfs_lookup() to
     *        get the correct nameidata information setup.
     *
     * NOTE:  At one time there was code to have advfs_lookup() increment 
     *        v_scount if the request was an NI_DELETE and NI_END_OF_PATH,
     *        then depend on the AdvFS rename, remove, and rmdir VOPs to
     *        decrement the v_scount.  While this sounds good, the problem
     *        is that in the VFS layer there are several error exit paths
     *        after calling VOP_LOOKUP(), that can bypass calling
     *        VOP_RENAME(), VOP_REMOVE(), or VOP_RMDIR().  In situations
     *        like this, no one would decrement v_scount.  
     *
     *        The VFS layer can not be made to deal with this, without
     *        putting a lot of AdvFS specific knowledge into the VFS layer,
     *        since VFS is generic code and does not know that AdvFS took an
     *        _EXTRA_ reference against the file.  If VFS were to call
     *        VN_SOLTRELE() on error exit paths, other file systems would
     *        end up with negative values of v_scount (and if the file
     *        system did not use v_scount, then VN_SOFTRELE() decrements
     *        v_count which would then go negative).  
     *
     *        Plus there is a risk that in the future, another kernel module
     *        developer would look at the VFS code that sets ni_nameiop to
     *        NI_DELETE and attempt to mimic this, but not fully understand
     *        the rules.  They might call VOP_LOOKUP() and then call the
     *        wrong VOP or use the arguments in a way that the VOP_RENAME(),
     *        VOP_REMOVE(), or VOP_RMDIR() do not expect.  The worse case
     *        situation would be that it appears to work for the developer,
     *        but AdvFS can not umount the file system because there is a
     *        vnode (or 2) hanging around with elevated v_scount values.
     *
     *        Any future optimizations must take this into consideration and
     *        should make the actions of VOP_LOOKUP() _NOT_ require house
     *        keeping be done by VOP_RENAME(), VOP_REMOVE(), or VOP_RMDIR().
     */
    ndp->ni_nameiop = NI_DELETE | NI_END_OF_PATH;
    error = advfs_lookup_int(ALI_NO_DNLC, ndp, dir_vp, name, 
                             cred, dir_vp);
    int_op = ndp->ni_nameiop;    /* save setting of NI_BADDIRSTAMP */
    ndp->ni_nameiop = saved_op;  /* setting outside of vfs, must be restored */
    if (error) {
        return(error);
    }

    remove_vp = ndp->ni_vp;

    /* Is this a directory? */
    if (remove_vp->v_type == VDIR) {
	/* 
	 * We should return error immediately if unlink/remove attempted 
	 * on a directory. However, test is expecting unlink of empty 
	 * directory to succeed, so we'll funnel it to rmdir.
	 */

	VN_RELE( remove_vp );

	error = VOP_RMDIR( dir_vp, name, cred );
	if (error == EEXIST) {
	    /* Directory not empty. Translate to EPERM to satisy POSIX. */
	    error = EPERM;
	}

	return (error);
    }

    remove_bfap = VTOA(remove_vp);
    ADVFS_ACCESS_LOGIT( remove_bfap, "advfs_remove: get remove_bfap");

    rem_context = VTOC(remove_vp);

    NFS_FREEZE_LOCK(ndp->ni_vp);
    nfs_freeze_locked = TRUE;

remove_it:

    /*
     * check to see if the directory of the to-be-deleted file has an
     * undelete directory. if so, we are really doing a rename...
     */
    if (!BS_BFTAG_NULL(dir_context->undel_dir_tag)) {
       goto rename_it;
    }

remove_it_1:
    ADVRWL_FILECONTEXT_WRITE_LOCK(dir_context);
    ADVRWL_FILECONTEXT_WRITE_LOCK(rem_context);
    rem_context_locked = TRUE;
    dir_context_locked = TRUE;

    /*
     * Check to see if this is an attempt to remove a quota file.
     * The last link to a quota file is never unlinked.
     */
    for (qtype = 0; qtype < MAXQUOTAS; qtype++) {
        /*
         * Never allow removal of quota files if it is the last reference to it.
         * Modified.  Someone might create a hard link to it, and 
         * there should be a way to remove that link.  As long as there is
         * one reference to the actual tag file, then everything should be fine.
         */
        if (BS_BFTAG_EQL(VTOBNP(remove_vp)->tag, fsnp->qi[qtype].qiTag) && 
                rem_context->dir_stats.st_nlink < 2) {
	    error = EACCES;
	    goto _bail_out;
        }
    }

    if (ndp->ni_dirstamp != dir_context->dirstamp
                || (int_op & NI_BADDIRSTAMP)) {
        int ret;
        bfTagT found_bs_tag;
        fs_dir_entry *dir_buffer;
        rbfPgRefHT page_ref;

        ret = seq_search(
                         dir_vp,
                         ndp,
                         name,
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

_cow_start:
    if (HAS_SNAPSHOT_CHILD(remove_bfap) )  {
        /* The file is going to be deleted. We need to do the full COW
         * before starting the remove transaction (to prevent potential
         * log half full errors).
         */

        if (rem_context->dir_stats.st_nlink == 1) { 
            /* If the file is being deleted, force a COW over the entire
             * file 
             * 
             * synchronize with snapshot creation.  
             */
            ADVFS_SNAPSHOT_CREATE_SYNC( remove_bfap );
            advfs_force_cow_and_unlink( remove_bfap,
                                    0,  /* Start at the beginning */
                                    (size_t)0,  /* The whole file */
                                    SF_SNAP_NOFLAGS,
                                    FtxNilFtxH);
            ADVFS_SNAPSHOT_DONE_SYNC( remove_bfap );
        } else {
            /* The st_nlink will be decremented by the file won't be
             * deleted.  Just access the children so that the metadata is
             * COWed (including the orignal st_nlink count 
             */
            snap_flags_t snap_flags = SF_SNAP_NOFLAGS;
            advfs_access_snap_children( remove_bfap,
                                        FtxNilFtxH,
                                        &snap_flags );
        }
        if (remove_bfap->dmnP->dmn_panic) {
            error = EIO;
            goto _bail_out;
        }

    }

    ADVFS_ACCESS_LOGIT( remove_bfap, "advfs_remove: starting transaction");

    /*
     * start a transaction.  Note: if not running under CFS,
     * ndp->ni_xid must be zero to guarantee uniqueness of
     * ftxId in the transaction log.
     */



    /* Need index lock because of call chain
     * remove_dir_ent --> idx_directory_insert_space --> 
     * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
     * rbf_add_stg
     */
    IDX_GET_BFAP(dir_bfap, idx_bfap);
    if (idx_bfap != NULL) { 
        ADVRWL_MIGSTG_READ( idx_bfap );
    }


    /* No need to take the quota locks here. Storage is added
     * to the quota files only at file creation, chown and
     * explicit quota setting.
     * The potential danger path is:
     * rbf_delete --> chk_blk_quota -->dqsync
     * but it is tame in this case
     */
    
    sts = FTX_START_XID(FTA_OSF_REMOVE_V1, &ftx_handle, FtxNilFtxH,
                        dmnP, ndp->ni_xid);
    if (sts != EOK) {
        ADVFS_SAD1("Error starting transaction in advfs_remove", sts);
    }

    if (HAS_SNAPSHOT_CHILD(remove_bfap) && 
            (remove_bfap->bfaFlags & BFA_SNAP_CHANGE)) 
    {
        /* A snapset was created between the full COW and starting the
         * transaction.  We need to try the COW again.
         */
        if (idx_bfap != NULL) {
            ADVRWL_MIGSTG_UNLOCK( idx_bfap );
        }
        ftx_quit(ftx_handle);
        goto _cow_start;
    }


    /*
     * this routine cannot be ftx_failed after the call to remove_dir (ie
     * from here on)
     */

    /*
     * Remove the file's entry from the dnlc cache.  Do this before
     * calling remove_dir_ent() to prevent races with lookups after
     * the file has been unlinked.  dnlc_remove will do a VN_SOFTRELE on the
     * vnode and the directory vnode. This will remove the v_scount
     * that dnlc_enter put on.  
     */ 
    dnlc_remove(dir_vp, name);
    
    /*
     * dnlc_remove will decrement v_scount and not check that it was non-zero
     * to begin with.  Therefore, if we screwed up our counts and had a
     * vnode with a 0 v_scount that was in the dnlc, then we will now have a
     * negative v_scount.  This will prevent the access structure from ever
     * being deallocated or recycled.
     */

    MS_SMP_ASSERT( remove_vp->v_scount >= 0 );

    /*
     * decr the file's link count and remove it's directory entry. If
     * the link count is not 0, then update it's stats (with new ctime)
     */

    --rem_context->dir_stats.st_nlink;

    if (rem_context->dir_stats.st_nlink == 0) {

        /*
         * Mark the bitfile for delete.  It will get cleaned up when
         * the last deaccessor goes away.  It is done prior to
         * remove_dir_ent to avoid potential pinblock deadlock on the
         * same bmt page.
         */
        rbf_delete(remove_bfap, ftx_handle);
        ADVFS_ACCESS_LOGIT( remove_bfap, "advfs_remove just called rbf_delete");
        ADVSMP_FILESTATS_LOCK( &rem_context->fsContext_lock );
        rem_context->fs_flag |= M_DELETE;
        ADVSMP_FILESTATS_UNLOCK( &rem_context->fsContext_lock );
        sts = remove_dir_ent( dir_vp, ndp, name, ftx_handle, FALSE );
        if (sts != EOK) 
        {
            ADVSMP_FILESTATS_LOCK( &rem_context->fsContext_lock );
            rem_context->fs_flag &= ~M_DELETE;
            ADVSMP_FILESTATS_UNLOCK( &rem_context->fsContext_lock );
            goto _error_out;
        }

    } else {
        sts = remove_dir_ent( dir_vp, ndp, name, ftx_handle, FALSE );
        if (sts != EOK) 
        {
            goto _error_out;
        }
        ADVSMP_FILESTATS_LOCK( &rem_context->fsContext_lock );
        rem_context->fs_flag |= MOD_CTIME;
        rem_context->dirty_stats = TRUE;
        ADVSMP_FILESTATS_UNLOCK( &rem_context->fsContext_lock );

        sts = fs_update_stats(ndp->ni_vp, remove_bfap, ftx_handle, 0);
        if (sts != EOK) {
            ADVFS_SAD1("Error from fs_udpate_stats in advfs_remove", sts);
        }
    }

    /*
     * complete the transaction
     */

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_5);

    ftx_done_fs(ftx_handle, FTA_OSF_REMOVE_V1);

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_6);

    if (idx_bfap != NULL) {
        ADVRWL_MIGSTG_UNLOCK( idx_bfap );
    }

    /*
     * unlock the file and dir
     */

    ADVRWL_FILECONTEXT_UNLOCK(rem_context);
    ADVRWL_FILECONTEXT_UNLOCK(dir_context);
    rem_context_locked = FALSE;
    dir_context_locked = FALSE;

    NFS_FREEZE_UNLOCK(ndp->ni_vp);
    nfs_freeze_locked = FALSE;

/*    event_post(&xxbp.b_iocomplete);*/

    VN_RELE( remove_vp );

    return(0);

_error_out:
    /*
     * unlock the file and dir
     */
    rem_context->dir_stats.st_nlink++;

    /* fail the ftx before unlocking files */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_498);
    ftx_fail(ftx_handle);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_499);

    if (idx_bfap != NULL) {
        ADVRWL_MIGSTG_UNLOCK( idx_bfap );
    }

    if (rem_context_locked) {
        ADVRWL_FILECONTEXT_UNLOCK(rem_context);
    }

    if (dir_context_locked) {
        ADVRWL_FILECONTEXT_UNLOCK(dir_context);
    }

    if (nfs_freeze_locked) {
        NFS_FREEZE_UNLOCK(ndp->ni_vp);
    }

    VN_RELE( remove_vp );

    return (BSERRMAP(sts));

/*---------------------------------------------------------------------------
 * do a rename instead of a remove
 *---------------------------------------------------------------------------*/
rename_it:

    /*
     * To get here, we probably have a dnlc v_scount on the vnode and the
     * bfap is refed.  There is no guarentee of a v_count on the vnode, so
     * we don't want to do a VN_RELE on the vnode.  Additionally, the
     * v_scount is from the dnlc, so we don't want to clear that either
     * (unless it is through a dnlc_remove).
     */

    /*
     * bf_get the undelete directory. If bf_get fails, then just go
     * back and do a remove (someone deleted the undelete dir on us).
     * This will bump the v_count on the undel_dir directory vnode.
     */
    sts = bf_get(
                 dir_context->undel_dir_tag,
                 0,
                 DONT_UNLOCK,
                 fsnp,
                 &undel_dir_vp,
                 NULL,
                 NULL
                 );

    if (sts != EOK) {
        /*
         * the undel dir is gone.
         * detach it from this dir, and do a normal remove.
         */

        dir_context->undel_dir_tag = NilBfTag;
        bmt_rec.dir_tag = NilBfTag;

        sts = bmtr_put_rec(
                           dir_bfap,
                           BMTR_FS_UNDEL_DIR,
                           (void *)&bmt_rec,
                           sizeof(struct bsUndelDir),
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
    error = advfs_access(undel_dir_vp, S_IWUSR, cred);
    if (error != EOK) {
        /*
         * failure to VN_RELE() these vnodes results in EBUSY on umount.
         */
        VN_RELE(undel_dir_vp);
        goto _finish;
    }

    /*
     * set the dir vp, and NULL the file vp
     * fill in the name
     */

    /*
     * do the rename
     */
    save_dir_tag = dir_context->bf_tag;
    save_rem_tag = rem_context->bf_tag;
    error = advfs_rename( dir_vp, name, undel_dir_vp, name, cred );
    VN_RELE(undel_dir_vp);

    if ( error ) {
        if (error == EEXIST) {
            if (rem_context->dir_stats.st_nlink > 1) {
                sts = bf_get(
                        save_dir_tag,
                        0,
                        DONT_UNLOCK,
                        fsnp,
                        &dir_vp,
                        NULL,
                        NULL
                        );

                if (sts != EOK) {
		    error = sts;
		    goto _finish;
                }

                sts = bf_get(
                        save_rem_tag,
                        0,
                        DONT_UNLOCK,
                        fsnp,
                        &rvp,
                        NULL,
                        NULL
                        );

                if (sts != EOK) {
		    error = sts;
		    goto _finish;
                }

                dir_bnp = VTOBNP(dir_vp);
                dir_bfap = VTOA(dir_vp);
                dir_context = dir_bnp->fsContextp;
                ndp->ni_vp = rvp;
                rem_context = VTOC(remove_vp);
                ndp->ni_dirstamp = dir_context->dirstamp;
                ndp->ni_nameiop &= ~NI_BADDIRSTAMP;
                --ndp->ni_dirstamp;
                goto remove_it_1;
            }
        }
    }

_finish:
    /*
     * release any vnodes?
     */
    NFS_FREEZE_UNLOCK(ndp->ni_vp);
    nfs_freeze_locked = FALSE;

    MS_SMP_ASSERT( rem_context_locked == FALSE );
    MS_SMP_ASSERT( dir_context_locked == FALSE );

    VN_RELE( remove_vp );

    return (error);

_bail_out:
    if (dir_context_locked) {
        ADVRWL_FILECONTEXT_UNLOCK(dir_context);
    }

    if (rem_context_locked) {
        ADVRWL_FILECONTEXT_UNLOCK(rem_context);
    }

    if (nfs_freeze_locked) {
        NFS_FREEZE_UNLOCK(ndp->ni_vp);
    }

    VN_RELE( remove_vp );

    return(error);
}

/*
 * advfs_link
 *
 * Create a hard link to a file.
 *
 * vp and dvp both have v_count's against them.  The caller will VN_RELE()
 * so do _NOT_ do it here.
 */
advfs_link(
    struct vnode *vp,     /* in - vnode to of existing file */
    struct vnode *dvp,    /* in - vnode of Target directory */
    char         *nm,     /* in - new name to put in target directory */
    struct ucred *cred)   /* in - credentials */
{
    struct nameidata *ndp;
    ni_nameiop_t      saved_op;
    bfAccessT        *bfap;
    bfAccessT        *dir_bfap;
    bfAccessT        *idx_bfap = NULL;
    struct fsContext *file_context;
    struct fsContext *dir_context;
    struct bfNode    *bnp;
    struct bfNode    *dbnp;
    statusT           ret;
    struct fileSetNode *fsnp;
    bfSetT           *bfSetp;
    domainT          *dmnP;
    ftxHT             ftx_handle;
    uint16_t          effective_LINK_MAX;
    int               error;

    FILESETSTAT( dvp, advfs_link );

    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp != NULL);

    saved_op = ndp->ni_nameiop;
    if (!(ndp->ni_nameiop & NI_CREATE)) {
        /*
         * Assumes that the nameidata structure will be properly setup if
         * and only if NI_CREATE is set by the caller (VFS layer), so we can
         * optimize the path.  If we find any other value in ni_nameiop,
         * then do our own lookup to initialize the nameidata structure.
         */
        advfs_lookup_t ali_flags = ALI_NO_FLAGS;
 
        ndp->ni_nameiop = NI_CREATE;
        error = advfs_lookup_int(ali_flags, ndp, dvp, nm, cred, dvp);
        ndp->ni_nameiop = saved_op;     /* setting outside of vfs, must be
                                           restored */
        if ( error != EOK && error != ENOENT ) {
            return(error);
        }
        if ( ndp->ni_vp != NULL ) {
            /*
             * If the local lookup found a file, we need to release the
             * vnode and report EEXIST.
             */
            VN_RELE(ndp->ni_vp);
            return( EEXIST );
        }
    } else if ( (ndp->ni_vp != NULL) && !(ndp->ni_nameiop & NI_BADDIRSTAMP)) {
        return EEXIST;
    }

    /* N.B.  If NI_CREATE is not set, we cannot just assume that ni_vp is
     * correctly initilaized.  If the file exists, it will be found in
     * fs_create_file and EEXIST will be returned.
     */

    fsnp = VTOFSN(vp);
    dmnP = fsnp->dmnP;
    bfSetp = fsnp->bfSetp;
    bnp = VTOBNP(vp);
    bfap = VTOA(vp);
    file_context = bnp->fsContextp;

    dir_bfap = VTOA(dvp);
    dbnp = VTOBNP(dvp);
    dir_context = dbnp->fsContextp;

    NFS_FREEZE_LOCK(vp);

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        return EIO;
    }

    /* 
     * Make sure we are not trying to link to a directory.
     *
     * Some day we may be asked to change this to work more like the HP-UX
     * HFS file system, which allows suser() to create a hard link to a
     * directory.  If we do this, get someone to look at salvage and fixfdmn
     * to make sure they are OK with it.
     *
     * A change like this may also have ripple effects into the
     * advfs_remove() code.
     *
     * We are ignoring suser() when asked to make a link to a directory
     * based on the link(2) man page which says:
     *
     *     [EPERM] The file named by path1 is a directory and the effective
     *             user ID is not a user who has appropriate privileges.
     *             Some file systems return this error whenever path1 names
     *             a directory, regardless of the user ID.
     */

    /*
     * UNIX98 standard says we do not have to allow hard
     * liks to a directory.
     *
     * Allowing the superuser to create hard links causes too
     * many corruption problems for advfs, so don't do it.
     *
     * Among these problems are the fact tha advfs_rmdir doesn't
     * currently honor the link count, and hence always leaves
     * a dangling link on a rmdir of a linked directory. Also,
     * the behavior of ".." when the first link has been removed
     * would need verification. If these are fixed, add
     * && !suser() to the if below to allow hard links.
     *
     * SysV implemented hard links for directories prior to the
     * implementation of symbolic links. It is much cleaner to
     * use symbolic links to directories and it does not cause
     * file system corruption problems when a directory is
     * removed. Symbolic links should be used instead of hard
     * links.
     */

    if (vp->v_type == VDIR ) {
        return(EPERM);
    } else {
        if (error = advfs_access(dvp, S_IWRITE, cred))
            return (error);
    }

    ADVRWL_FILECONTEXT_WRITE_LOCK(dir_context);
    ADVRWL_FILECONTEXT_WRITE_LOCK(file_context);

    /* make sure the link count is OK */

    if (file_context->dir_stats.st_nlink == 0) {
        ADVRWL_FILECONTEXT_UNLOCK(file_context);
        ADVRWL_FILECONTEXT_UNLOCK(dir_context);

        NFS_FREEZE_UNLOCK(vp);
        return(ENOENT);
    }

    /* 
     * We are assuming that the maximum number of links we can return is
     * limited by several factors.  First is the MAXLINK param.h value is
     * currently 32K.  The va_nlink field is a signed short so we could not
     * pass back via the VFS layer more than 32K anyway.  The stat st_nlink
     * field is a short so we can not return more than 32K to the user.  And
     * finally the POSIX nlink_t typedef is only a short.
     *
     * LINK_MAX is a bit outdated and pathconf(file,_PC_LINK_MAX) is the
     * correct way to determine on a file system by file system basis.
     *
     * OK.  AdvFS is ready to support an nlink value greater than 32K, but
     * before we do, we need to be able to return the value, so that means
     * vattr.va_nlink and stat.st_nlink need to get larger.  One way is for
     * both fields to be changed to unsigned.  I can not directly detect
     * that in an assert, so the next best thing is to look for HP-UX
     * engineers to change LINK_MAX or MAXLINK and hope that we can adapte
     * to this when the assert triggers.  If that happens, then we should be
     * able to change 'effective_LINK_MAX' to use a constant that is nearly
     * 64K (On Tru64 UNIX we limited the uint16_t nlink value to
     * 65533.  This was because -1 and -2 were already used as flags in
     * Tru64 UNIX.
     *
     * The other way AdvFS will be allowed to support larger nlink values is
     * if vattr.va_nlink and stat.st_nlink get larger fields (like int32_t or
     * int64_t).  If that happens, then the sizeof asserts should trigger.
     * When that happens, we just need to adjust 'effective_LINK_MAX' to use
     * a larger constant (checking to make sure it does not exceed our
     * primary Mcell on-disk fs_stat.st_nlink field. 
     *
     * We may also need to worry about what will happen if a domain that is
     * allowed to aquire more than 32767 hard links is mounted on an older
     * system.  Will we do the right thing?  Will all the utilities that
     * look at st_nlinks via stat() and get a truncated value, and what will
     * happen if an older fixfdmn or salvage from the older system are run
     * against this domain while it is mounted on an older system.  So just
     * changing effective_LINK_MAX may not be sufficient.
     *
     * Change or remove the asserts as needed based on current realities in
     * HP-UX.
     */
    MS_SMP_ASSERT(MAXLINK == 32767);
    MS_SMP_ASSERT(LINK_MAX == 32767);
    MS_SMP_ASSERT(sizeof(((struct vattr *)0)->va_nlink) == sizeof(int16_t));
    MS_SMP_ASSERT(sizeof(((struct stat *)0)->st_nlink) == sizeof(int16_t));
    MS_SMP_ASSERT(sizeof(nlink_t) == sizeof(int16_t));
    MS_SMP_ASSERT(sizeof(effective_LINK_MAX) == sizeof(int16_t));

    /*
     * Determine the maximum number of links supported.  If we start to
     * support different on-disk versions that have different maximum nlink
     * values, then make this a conditional test based on the domain
     * version.  That is what we did in Tru64 UNIX.
     */
    effective_LINK_MAX = MAXLINK;       /* Max links supported by HFS */

    if (file_context->dir_stats.st_nlink >= effective_LINK_MAX) {
        ADVRWL_FILECONTEXT_UNLOCK(file_context);
        ADVRWL_FILECONTEXT_UNLOCK(dir_context);
        NFS_FREEZE_UNLOCK(vp);
        return(EMLINK);
    }

    /*
     * start a transaction.  Note: if not running under CFS,
     * ndp->ni_xid must be zero to guarantee uniqueness of
     * ftxId in the transaction log.
     */



    /*
     * Need to take the directory lock because of the call chain
     * insert_seq --> rbf_add_stg
     */
    ADVRWL_MIGSTG_READ( dir_bfap );

    /*
     * Need to take the fileset tag directory lock because of the call chain
     *
     * insert_seq --> idx_convert_dir --> idx_create_index_file -->
     * idx_create_index_file_int --> rbf_create --> rbf_int_create -->
     * tagdir_alloc_tag --> init_next_tag_page-->rbf_add_stg
     */
    ADVRWL_MIGSTG_READ( dir_bfap->bfSetp->dirBfAp );

    /*
     * Need to take the index lock because of the call chain
     * insert_seq --> idx_directory_get_space -->
     * idx_directory_insert_space --> idx_directory_insert_space_int -->
     * idx_index_get_free_pgs_int --> rbf_add_stg
     */
    IDX_GET_BFAP(dir_bfap, idx_bfap); 
    if (idx_bfap != NULL) { 
        ADVRWL_MIGSTG_READ( idx_bfap );
    }


    /*
     * No need to take the quota locks. The link may add space to the
     * directory but it is owned by whoever owned the original link, so
     * there is an entry in the quota file already.
     * The potential danger path is
     * insert_seq --> chk_blk_quota --> dqsync --> rbf_add_stg
     * but it is tame in this case.
     */
    ret = FTX_START_XID(FTA_OSF_LINK_V1, &ftx_handle, FtxNilFtxH,
                        dmnP, ndp->ni_xid);
    if (ret != EOK) {
        ADVFS_SAD1("Error starting transaction in advfs_link", ret);
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
                     nm,
                     HARD_LINK,
                     ndp,
                     bfSetp,
                     fsnp,
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
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_500);
        ftx_fail(ftx_handle);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_501);
        ADVRWL_FILECONTEXT_UNLOCK(file_context);
        ADVRWL_FILECONTEXT_UNLOCK(dir_context);

        if (idx_bfap != NULL) {
            ADVRWL_MIGSTG_UNLOCK( idx_bfap );
        }
        ADVRWL_MIGSTG_UNLOCK( dir_bfap->bfSetp->dirBfAp );
        ADVRWL_MIGSTG_UNLOCK( dir_bfap );

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
    ADVSMP_FILESTATS_LOCK( &file_context->fsContext_lock );
    file_context->fs_flag |= MOD_CTIME;
    file_context->dirty_stats = TRUE;
    ADVSMP_FILESTATS_UNLOCK( &file_context->fsContext_lock );
    ret = fs_update_stats(vp, bfap, ftx_handle, 0);
    if (ret != EOK) {
        ADVFS_SAD1("Error from fs_udpate_stats in advfs_link", ret);
    }
    /* update the mtime and ctime fields for the dir */
    ADVSMP_FILESTATS_LOCK( &dir_context->fsContext_lock );
    dir_context->fs_flag |= (MOD_CTIME | MOD_MTIME);
    dir_context->dirty_stats = TRUE;
    dir_context->dirstamp++;
    ADVSMP_FILESTATS_UNLOCK( &dir_context->fsContext_lock );

    ret = fs_update_stats(dvp, VTOA(dvp), ftx_handle, 0);
    if (ret != EOK) {
        ADVFS_SAD1("Error from fs_udpate_stats 1 in advfs_link", ret);
    }

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_27);

    ftx_done_fs(ftx_handle, FTA_OSF_LINK_V1);

    ADVFS_CRASH_RECOVERY_TEST(bfap->dmnP, AdvfsCrash_28);

    if (idx_bfap != NULL) {
        ADVRWL_MIGSTG_UNLOCK( idx_bfap );
    }
    ADVRWL_MIGSTG_UNLOCK( dir_bfap->bfSetp->dirBfAp );
    ADVRWL_MIGSTG_UNLOCK( dir_bfap );

    /* finish any directory truncation that might have started in insert_seq */

    dir_trunc_finish(dvp);

    dnlc_enter(dvp, nm, vp, NOCRED);
    ADVRWL_FILECONTEXT_UNLOCK(file_context);
    ADVRWL_FILECONTEXT_UNLOCK(dir_context);

    NFS_FREEZE_UNLOCK(vp);
    return(0);
}


/*
 * advfs_rename
 *
 * rename (mv) a file or directory
 *
 * Assumption:  vn_rename() has a vref against sdvp, tdvp, and from_vp.
 *              vn_rename() will VN_RELE() these.  Any additional vref's
 *              applied need to be VN_RELE()'ed before returning.
 *
 *              I'm assuming that it is OK to call advfs_lookup() to get the
 *              vnode for the 'target' file when it exists and we have to
 *              decrement its st_nlink value.  This is vs calling
 *              lookuppnvp(), which will return a CFS vnode instead of a
 *              physical file system vnode.  The old Tru64 UNIX code called
 *              namei() which was much easier than calling lookuppnvp(), so
 *              I've substituted advfs_lookup() which is also easy to call,
 *              but advfs_lookup() will return a physical file system vnode,
 *              and not a CFS vnode.  Mike Milicia does not think this will
 *              be a problem, because we are holding a vref for the parent
 *              directory, advfs_lookup() is a single directory lookup so we
 *              can not cross a file system boundary, and because this is
 *              not be a problem for UFS that does a similar thing.
 *
 * NOTE:  Some of the checks in here appear redundant because they are
 *        already done by our primary customer, the VFS layer.  However, it
 *        is not our only customer.  We could be called by other modules
 *        (even loadable 3rd party modules) and the extra checks make sure
 *        that we are being passed good information and that we do not
 *        corrupt our file system.
 */
advfs_rename(
    struct vnode *sdvp,        /* in - Source directory vnode 
                                *      VFS has VN_HOLD, VFS will VN_RELE */
    char         *snm,         /* in - Source filename 
                                *      VFS has VN_HOLD, VFS will VN_RELE */
    struct vnode *tdvp,        /* in - Target directory vnode 
                                *      VFS has VN_HOLD, VFS will VN_RELE */
    char         *tnm,         /* in - Target filename */
    struct ucred *cred)        /* in - credentials */
{
    struct vnode        *from_vp                = NULL;
    struct vnode        *from_dir_vp            = NULL;
    struct vnode        *to_vp                  = NULL;
    struct vnode        *to_dir_vp              = NULL;
    struct vnode        *to_removed_vp          = NULL;
    struct fsContext    *from_file_context;
    struct fsContext    *from_dir_context;
    struct fsContext    *to_file_context        = NULL;
    struct fsContext    *to_dir_context;
           bfAccessT    *from_accessp;
           bfAccessT    *to_accessp;
           bfAccessT    *to_dir_accessp;
           bfAccessT    *from_dir_accessp;
           bfAccessT    *from_idx_bfap          = NULL;
           bfAccessT    *to_idx_bfap            = NULL;
           bfSetT       *bfSetp;
           ftxHT         ftx_handle;
           domainT      *dmnP;
           fs_dir_entry *dir_buffer;
           char         *p;
           bfTagT        found_bs_tag;
           bfTagT        oldparent_tag;
           bfTagT        newparent_tag;
           bfTagT        keep_to_dir_tag;
           tagSeqT      *seqp;
           rbfPgRefHT    page_ref;
    struct fileSetNode  *to_fsnp;
    struct fileSetNode  *from_fsnp;
           statusT       ret;
           int           type;
           int           to_vp_vrefed           = FALSE;
           int           dir_mv                 = FALSE;
           int           error                  = EOK;
           int32_t       to_dir_mig_stg_lock    = FALSE;
           int32_t       to_file_context_locked = FALSE;
           uint16_t      effective_LINK_MAX;
    struct nameidata    *sndp;
    struct nameidata    *tndp;
    struct nameidata     tnameidata;
    struct nameidata     snameidata;

    FILESETSTAT( sdvp, advfs_rename );

    oldparent_tag = NilBfTag;
    newparent_tag = NilBfTag;

    /*
     * Check if the 'to' file exists.  If it does, then save the location of
     * the name in ni_count and ni_offset.  If it does not exist, then save
     * the location of an available place for the name in ni_count and
     * ni_offset.
     *
     * NOTE:  While vn_rename() did do a lookup on the 'to' file that filled
     *        in all the nameidata structure information, vn_rename()
     *        dropped the v_count reference on the vnode, so a race
     *        condition could have totally changed the underlying vnode.
     *        Unlikely as it may be, it could still happen on a system under
     *        heavy memory pressure.
     *
     *        A suggested optimization would be to add a new NI_xxx flag to
     *        ni_nameiop that tells advfs_lookup() that we trust the other
     *        nameidata fields, but we need to verify/get the ni_vp value
     *        and if the dnlc has the correct value, please increment
     *        v_count and return the vnode to us, otherwise, please find the
     *        file in the directory and return the vnode with an incremented
     *        v_count.
     *
     *        HOWEVER, if we want to trust the nameidata structure, we MUST
     *        make sure that the caller really did a proper setup of the
     *        nameidata structure, so any optimization should test to see
     *        that the ni_nameiop field is set to NI_DELETE.  If it is not
     *        set to NI_DELETE, then this VOP was called by non-VFS layer
     *        code and we still need to do the full scale advfs_lookup() to
     *        get the correct nameidata information setup.
     *
     * NOTE:  At one time there was code to have advfs_lookup() increment 
     *        v_scount if the request was an NI_DELETE and NI_END_OF_PATH,
     *        then depend on the AdvFS rename, remove, and rmdir VOPs to
     *        decrement the v_scount.  While this sounds good, the problem
     *        is that in the VFS layer there are several error exit paths
     *        after calling VOP_LOOKUP(), that can bypass calling
     *        VOP_RENAME(), VOP_REMOVE(), or VOP_RMDIR().  In situations
     *        like this, no one would decrement v_scount.  
     *
     *        The VFS layer can not be made to deal with this, without
     *        putting a lot of AdvFS specific knowledge into the VFS layer,
     *        since VFS is generic code and does not know that AdvFS took an
     *        _EXTRA_ reference against the file.  If VFS were to call
     *        VN_SOLTRELE() on error exit paths, other file systems would
     *        end up with negative values of v_scount (and if the file
     *        system did not use v_scount, then VN_SOFTRELE() decrements
     *        v_count which would then go negative).  
     *
     *        Plus there is a risk that in the future, another kernel module
     *        developer would look at the VFS code that sets ni_nameiop to
     *        NI_DELETE and attempt to mimic this, but not fully understand
     *        the rules.  They might call VOP_LOOKUP() and then call the
     *        wrong VOP or use the arguments in a way that the VOP_RENAME(),
     *        VOP_REMOVE(), or VOP_RMDIR() do not expect.  The worse case
     *        situation would be that it appears to work for the developer,
     *        but AdvFS can not umount the file system because there is a
     *        vnode (or 2) hanging around with elevated v_scount values.
     *
     *        Any future optimizations must take this into consideration and
     *        should make the actions of VOP_LOOKUP() _NOT_ require house
     *        keeping be done by VOP_RENAME(), VOP_REMOVE(), or VOP_RMDIR().
     */
    tndp = &tnameidata;         /* point to our copy of the 'to' nameidata */
    tndp->ni_nameiop = NI_DELETE | NI_END_OF_PATH;
    tndp->ni_xid = 0;
    ret = advfs_lookup_int(ALI_NO_DNLC, tndp, tdvp, tnm, cred, tdvp);
    if ( ret != EOK && ret != ENOENT ) {
        return(ret);
    }

    if ( tndp->ni_vp != NULL ) {
        to_vp_vrefed = TRUE;
        ADVFS_ACCESS_LOGIT( VTOA( tndp->ni_vp), "advfs_rename to_vp" );
    }
    to_vp = tndp->ni_vp;

    /*
     * Need to lookup up the file again.  vn_rename() already did this once
     * but we are not passed the vnode, just the source file name, and
     * vn_rename() trashed the nameidata structure by then doing a lookup
     * for the 'to' file.
     *
     * Calling advfs_lookup() is going to cause the thread specific nameidata
     * structure to have the source file information in it as a residual.
     *
     * We can call advfs_lookup() here even in a CFS environment because we
     * currently hold a reference on the parent directory (thanks to
     * vn_rename()) _AND_ we will continue to hold the reference until after
     * we VN_RELE() the looked up file _PLUS_ we are looking up a single
     * name _AND_ we are not following links that could make us cross a file
     * system boundary.  If any of the above are no longer true, then we
     * would have to do a lot more work that involves lookuppnvp() and CFS
     * callbacks (if you want to know how much more work, then look at the
     * Tru64 UNIX code in advfs_rename() after it calls namei()).
     */
    sndp = &snameidata;
    sndp->ni_nameiop = NI_DELETE | NI_END_OF_PATH;
    sndp->ni_xid = 0;
    ret = advfs_lookup_int(ALI_NO_DNLC, sndp, sdvp, snm, cred, sdvp);
    if ( ret != EOK ) {
	    if ( to_vp_vrefed ) {
            VN_RELE(tndp->ni_vp);
            to_vp_vrefed = FALSE;
        }
        return(ENOENT);
    }
    from_vp = sndp->ni_vp;
    ADVFS_ACCESS_LOGIT( VTOA( from_vp ), "advfs_rename from_vp" );

    /*
     * vn_rename() should already have a v_count reference against the
     * from_vp.  We called advfs_lookup() in order to get the vnode address
     * and to fill in the rest of the nameidata structure (ni_count and
     * ni_offset will be needed by remove_dir_ent() later), which results
     * in an extra v_count reference.
     *
     * However, when we're called from nfs, there is no v_count reference
     * at this point, so we'll leave the reference outstanding in case
     * we're called from NFS. We only have one success and one error path
     * later to release it in.
     */
    MS_SMP_ASSERT( from_vp->v_count > 0 );

    /*
     * set up the 'from' dir and file contexts
     */
    from_accessp = VTOA(from_vp);
    from_file_context = VTOC(from_vp);
    from_fsnp = VTOFSN(from_vp);

    from_dir_vp = sdvp;
    from_dir_context = VTOC(from_dir_vp);
    from_dir_accessp = VTOA(from_dir_vp);
    ADVFS_ACCESS_LOGIT( VTOA( from_dir_vp ), "advfs_rename from_dir_vp" );


    /*
     * now some 'to' dir stuff
     */

    to_dir_vp = tdvp;
    to_dir_accessp = VTOA(to_dir_vp);
    to_dir_context = VTOC(to_dir_vp);
    ADVFS_ACCESS_LOGIT( VTOA( to_dir_vp ), "advfs_rename to_dir_vp" );

    bfSetp = ADVGETBFSETP(to_dir_vp->v_vfsp);
    dmnP = ADVGETDOMAINP(to_dir_vp->v_vfsp);
    to_fsnp = VTOFSN(to_dir_vp);

    NFS_FREEZE_LOCK(from_dir_vp);

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        error = EIO;
        goto need_to_unfreeze;
    }

    /*
     * Check to see that the file system is writable.
     */
    if (to_dir_vp->v_vfsp->vfs_flag & VFS_RDONLY) {
        error = EROFS;
        goto need_to_unfreeze;
    }

    /*
     * Check to see that both the from and to directories are on the same
     * file system.  
     *
     * NOTE:  If we ever allow VXDEVOK to allow moving files between
     *        filesets on in the same domain, then this check will need to
     *        be modified.  But until that day, both files must be on the
     *        same file system.  [vn_rename() makes reference to LOFS being
     *        the only consumer of VXDEVOK today.]
     */
    if (from_dir_vp->v_vfsp != to_dir_vp->v_vfsp) {
        error = EXDEV;
        goto need_to_unfreeze;
    }
    
    /*
     * make sure the 'from' and 'to' names are not null.  This should be
     * caught by the VFS layer, but not all callers are from the VFS layer.
     */
    if ( strlen(snm) == 0 || strlen(tnm) == 0 ) {
        error = ENOENT;
        goto need_to_unfreeze;
    }

    /*
     * Make sure the from file and the to directory are not the same
     */
    if ( from_accessp == to_dir_accessp ) {
	error = EINVAL;
	goto need_to_unfreeze;
    }

    /*
     * don't allow .tags to be moved
     */
    if (BS_BFTAG_EQL(from_file_context->bf_tag, from_fsnp->tagsTag)) {
        if (BS_BFTAG_EQL(from_dir_context->bf_tag, from_fsnp->rootTag)) {
            error = EACCES;
            goto need_to_unfreeze;
        }
    }

    /*
     * Check to see if this is an attempt to rename the quota files.
     */
    for (type = 0; type < MAXQUOTAS; type++) {
        bfTagT from_tag = (VTOBNP(from_vp))->tag;
        if (BS_BFTAG_EQL(from_tag, from_fsnp->qi[type].qiTag)) {
            error = EACCES;
            goto need_to_unfreeze;
        }
    }

    /*
     * Make sure this is not a read-only fileset.
     */
    if (to_fsnp->vfsp && (to_fsnp->vfsp->vfs_flag & VFS_RDONLY)) {
        error = EROFS;
        goto need_to_unfreeze;
    }

    /*
     * check for renaming '.' or '..' or alias of '.' 
     */ 
    if ((strcmp(snm, ".") == 0) || (strcmp(snm, "..") == 0)) {
        error = EEXIST;
        goto need_to_unfreeze;
    }

    /* make sure we can write to target directory */
    if (error=advfs_access(to_dir_vp, S_IWRITE, cred)) {
        goto need_to_unfreeze;
    }

again:
    /*
     * To avoid a locking deadlock between to threads:
     *     Thread A rename("fred/x", "tony/x")
     *     Thread B rename("tony/y", "fred/y")
     * always lock the 2 directories in the same order regardless of which
     * is the source and which is the target directory.  Since there is only
     * a single bfAccess structure for any file that is currently
     * referenced, we always lock the file with the lowest memory address
     * for its bfAccess structure.  It does not matter if at a later time
     * the same 2 directories are allocated bfAccess structures that are in
     * a different memory order.  It is the order when we are trying to take
     * the locks that counts.  The race is between threads trying to lock
     * the current bfAccess structures, not some previous or future
     * incarnation of the bfAccess structure for the directories.
     */
    if (from_dir_accessp < to_dir_accessp) {
        ADVRWL_FILECONTEXT_WRITE_LOCK(from_dir_context);
        ADVRWL_FILECONTEXT_WRITE_LOCK(to_dir_context);
    } else {
        if (from_dir_accessp == to_dir_accessp) {
            ADVRWL_FILECONTEXT_WRITE_LOCK(from_dir_context);
        } else {
            ADVRWL_FILECONTEXT_WRITE_LOCK(to_dir_context);
            ADVRWL_FILECONTEXT_WRITE_LOCK(from_dir_context);
        }
    }

    ADVRWL_FILECONTEXT_WRITE_LOCK(from_file_context);

    /*
     * if from file was deleted, bail out
     */
    if (from_file_context->dir_stats.st_nlink == 0) {
        error = ENOENT;
        goto err_unlock_ffile;
    }

    if ((from_file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
        /*
         * if 'from' is a directory, disallow '.' and '..'
         */
        if ( (snm[0] == '.' && snm[1] == '\0')                  ||
             (snm[0] == '.' && snm[1] == '.' && snm[2] == '\0') ||
             (from_vp == from_dir_vp) ) {
            error = EEXIST;
            goto err_unlock_ffile;
        }

        /*
         * if it's a dir, save the parent
         */
        oldparent_tag = from_dir_context->bf_tag;
        dir_mv = TRUE;
        dnlc_purge_vp(from_vp);
    }

    /*
     * check to see that the 'from' file is still around (someone
     * could have deleted it between lookup and here, in which case
     * we are done after some cleanup). Note that even if someone
     * deleted it, we still have a from_file_context because our lookup
     * accessed the bitfile...
     */

    if (sndp->ni_dirstamp != from_dir_context->dirstamp
                || (sndp->ni_nameiop & NI_BADDIRSTAMP)) {
        ret = seq_search(
                         from_dir_vp,
                         sndp,
                         snm,
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
        bfTagT to_tag = (VTOBNP(tndp->ni_vp))->tag;

        /*
         * Check to see if this is an attempt to rename on top of a
         * quota file.
         */
        for (type = 0; type < MAXQUOTAS; type++) {
            if (BS_BFTAG_EQL(to_tag, to_fsnp->qi[type].qiTag)) {
                error = EACCES;
                goto err_unlock_ffile;
            }
        }

        /* 
         * Ensure that the source and destination are not the same.  This
         * test is needed to solve a possible race condition between link
         * and rename regarding the target file.  The target does not exist
         * initially, but it is found the second time (see 'someone created
         * the file on us' below).  Since the target was newly created, we
         * did a goto 'again:'.  We now need to make sure this new file is
         * not the same as the source file.
         */

        if(tndp->ni_vp == from_vp) {
            error = 0;
            goto err_unlock_ffile;
        }

        to_vp = tndp->ni_vp;
        to_file_context = VTOC( to_vp );
        to_accessp = VTOA(to_vp);

        if ((to_file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
            /*
             * Check to see if this is an attempt to rename over top of
             * either '.' or '..' (that is to say, replace '.' or '..' with
             * the 'from' file).
             */
            if ( (tnm[0] == '.' && tnm[1] == '\0')                  ||
                 (tnm[0] == '.' && tnm[1] == '.' && tnm[2] == '\0') ) {
                error = EEXIST;
                goto err_unlock_ffile;
            }
            if ( tndp->ni_vp == to_dir_vp ) {
                error = EINVAL;
                goto err_unlock_ffile;
            }

            /*
             * The standards say:
             *     "If the old argument points to the pathname of a file
             *     that is not a directory, the new argument must not point
             *     to the pathname of a directory."
             */
            if ( ! dir_mv ) {
                /*
                 * Since 'to' is a directory and 'from' is _NOT_ a
                 * directory, we return an error.
                 */
                error = EISDIR;
                goto err_unlock_ffile;
            }
        }
        else if ( dir_mv ) {
            /*
             * The standards say:
             *     "If the old argument points to the pathname of a
             *     directory, the new argument must not point to the
             *     pathname of a file that is not a directory."
             *
             * Since the 'to' file is _NOT_ a directory, but the 'from' file
             * is a directory, we return an error.
             */
            error = ENOTDIR;
            goto err_unlock_ffile;
        }

        /*
         * Don't allow .tags to be replaced.
         */
        if (BS_BFTAG_EQL(to_file_context->bf_tag, to_fsnp->tagsTag)) {
            if (BS_BFTAG_EQL(to_dir_context->bf_tag, to_fsnp->rootTag)) {
                error = EACCES;
                goto err_unlock_ffile;
            }
        }
    }

    /*
     * check to see if a directory is getting a new parent. If so,
     * then check to see that the source dir is not in the hierarchy
     * of the 'to' dir. Also check for write permission of source to
     * alter it's "..".
     */

    if (!BS_BFTAG_EQL(oldparent_tag, to_dir_context->bf_tag)) {
        newparent_tag = to_dir_context->bf_tag;
    }
    if (dir_mv && !BS_BFTAG_NULL(newparent_tag)) {
        if (error = advfs_access(from_vp, S_IWRITE, cred)) {
            goto err_unlock_ffile;
        }
        do {
            keep_to_dir_tag = VTOBNP(to_dir_vp)->tag;
            if (error = check_path_back(from_vp, to_dir_vp, cred)) {
                goto err_unlock_ffile;
            }
            to_vp = NULL;
            if (tndp->ni_vp) {
                to_vp = tndp->ni_vp;
            }
        } while (!BS_BFTAG_EQL(keep_to_dir_tag, VTOBNP(to_dir_vp)->tag));
    }
    /*
     * check the status of the 'to' file. someone may have created or
     * deleted it between lookup and here. It's OK if so, but we need
     * to change what is going on if that happened...
     */
    if (tndp->ni_dirstamp != to_dir_context->dirstamp
                || (tndp->ni_nameiop & NI_BADDIRSTAMP)) {

        /* if dirstamp matches, we know it couldn't have changed...*/
        ret = seq_search(
                         to_dir_vp,
                         tndp,
                         tnm,
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
                 * We need to VN_RELE(to_vp), but, in a cluster the
                 * path VN_RELE()->vdealloc()->getnewvnode()... can end
                 * up in bs_close_one() starting a transaction and
                 * locking locks.  Instead of doing now, remember to 
                 * VN_RELE() on the way out when the directories have been 
                 * unlocked.  Will not "goto again" once to_vp is NULL.
                 */
                to_removed_vp = to_vp;
                to_vp_vrefed = FALSE;
                tndp->ni_vp = NULL;
                to_vp = NULL;
            }

            /* save # useful pages in nameidata for possible truncation */
            VTOC(to_dir_vp)->fsc_last_dir_fob += VTOA(to_dir_vp)->bfPageSz;
        } else {
            if (ret == I_FILE_EXISTS) {
                if (to_vp == NULL) {
                    /* someone created the file on us */
                    /* do another lookup on it, and try again... */
lookup_to_file:
                    ADVRWL_FILECONTEXT_UNLOCK(from_file_context);
                    ADVRWL_FILECONTEXT_UNLOCK(from_dir_context);
                    if (from_dir_accessp != to_dir_accessp) {
                        ADVRWL_FILECONTEXT_UNLOCK(to_dir_context);
                    }
                    /*
                     * I'm assuming that advfs_lookup() is going to put a
                     * vref on tndp->ni_vp _IF_ the 'to' file is found!
                     *
                     * We can call advfs_lookup() here even in a CFS
                     * environment because we currently hold a reference on
                     * the parent directory (thanks to vn_rename()) _AND_ we
                     * will continue to hold the reference until after we
                     * VN_RELE() the looked up file _PLUS_ we are looking up
                     * a single name _AND_ we are not following links that
                     * could make us cross a file system boundary.  If any
                     * of the above are no longer true, then we would have
                     * to do a lot more work that involves lookuppnvp() and
                     * CFS callbacks (if you want to know how much more
                     * work, then look at the Tru64 UNIX code in
                     * advfs_rename() after it calls namei()).
                     */
                    tndp = &tnameidata;
                    tndp->ni_nameiop = NI_DELETE | NI_END_OF_PATH;
                    tndp->ni_xid = 0;
                    error = advfs_lookup_int(ALI_NO_DNLC, tndp, 
                                             to_dir_vp, tnm, cred, to_dir_vp);
                    if ( error != EOK ) {
                        /* an error out of advfs_lookup() is unexpected...*/
                        goto err_rele_vnodes;
                    }
                    if ( tndp->ni_vp != NULL ) {
                        to_vp_vrefed = TRUE;
                    }
                    goto again;
                } else {
                    /*
                     * well there was a file before, and there is one
                     * now. But make sure it is the SAME file (ie
                     * someone could have deleted the original and
                     * then created a new one of the same name. In
                     * that case we need to lookup the new one.)
                     */
                    if (!BS_BFTAG_EQL(to_file_context->bf_tag, found_bs_tag)) {
                        /*
                         * just let go of the 'old' file, and call go
                         * back to call advfs_lookup().
                         */
                        if ( to_vp_vrefed ) {
                            VN_RELE(tndp->ni_vp);
                            to_vp_vrefed = FALSE;
                        }
                        goto lookup_to_file;
                    }
                    /* it's the same to-file, but it could have moved */
                }
            } else {
                /* got some unexpected error from search... */
                ADVFS_SAD1(
                        "Unknown return value from seq_search in advfs_rename",
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
         * sndp->ni_xid must be zero to guarantee uniqueness of
         * ftxId in the transaction log.
         */




        /* Need to take the to_dir lock because of the call chain
         * insert_seq --> rbf_add_stg on it.
         * this lock is not taken in the else branch of this if, so
         * remember that we have taken it, so we can unlock it at
         * the end.
         */
        ADVRWL_MIGSTG_READ( to_dir_accessp );

        /* Need to take the lock on the ``to'' fileset tag directory
         * because of the call chain
         * insert_seq --> idx_convert_dir-->idx_create_index_file_int -->
         * rbf_create --> rbf_int_create --> tagdir_alloc_tag -->
         * init_next_tag_page --> rbf_add_stg
         */
        ADVRWL_MIGSTG_READ( to_dir_accessp->bfSetp->dirBfAp );        
        /* Need to take the migStgLk on the ``to'' directory index
         * (if it exists), because of the call chain
         * insert_seq --> idx_insert_filename --> idx_insert_filename_int -->
         * idx_index_get_free_pgs_int --> rbf_add_stg
         */
        IDX_GET_BFAP(to_dir_accessp, to_idx_bfap); 
        if (to_idx_bfap != NULL) { 
            ADVRWL_MIGSTG_READ( to_idx_bfap );
        }
        /* this flag remembers all three ``to'' mt locks */
        to_dir_mig_stg_lock = TRUE;

        /* Need to take the index lock on the from directory index
         * because of the call chain
         * remove_dir_ent --> idx_directory_insert_space -->
         * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
         * rbf_add_stg
         */
        IDX_GET_BFAP(from_dir_accessp, from_idx_bfap); 
        if (from_idx_bfap != NULL && from_idx_bfap != to_idx_bfap) { 
            ADVRWL_MIGSTG_READ( from_idx_bfap );
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
                            dmnP, sndp->ni_xid);

        if (ret != EOK) {
            ADVFS_SAD1("error starting ftx in rename", ret);
        }



        /*
         * if moving a dir to a new parent up the parents link count
         * this gets put out to disk later...
         */
        if (dir_mv &&!BS_BFTAG_NULL(newparent_tag)) {
            /*
             * See the comments explaining these asserts in the advfs_link()
             * routine above.
             */
            MS_SMP_ASSERT(MAXLINK == 32767);
            MS_SMP_ASSERT(LINK_MAX == 32767);
            MS_SMP_ASSERT(sizeof(((struct vattr *)0)->va_nlink) == 
                          sizeof(int16_t));
            MS_SMP_ASSERT(sizeof(((struct stat *)0)->st_nlink)  ==
                          sizeof(int16_t));
            MS_SMP_ASSERT(sizeof(nlink_t) == sizeof(int16_t));
            MS_SMP_ASSERT(sizeof(effective_LINK_MAX) == sizeof(int16_t));

            /*
             * Determine the maximum number of links supported.  If we start
             * to support different on-disk versions that have different
             * maximum nlink values, then make this a conditional test based
             * on the domain version.  That is what we did in Tru64 UNIX.
             */
            effective_LINK_MAX = MAXLINK;       /* Max links supported by HFS */

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
                         tnm,
                         START_FTX, /* do as a sub-trans */
                         tndp,
                         bfSetp,
                         to_fsnp,
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

        ret = remove_dir_ent( from_dir_vp,
                              sndp,
                              snm,
                              ftx_handle,
                              TRUE);
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
        ADVSMP_FILESTATS_LOCK( &to_dir_context->fsContext_lock );
        to_dir_context->dirstamp++;
        ADVSMP_FILESTATS_UNLOCK( &to_dir_context->fsContext_lock );

        /*
         * update the 'from' fsContext area with new name and parent
         * note that 'from_file_context' has become the 'to' fsContext.
         * write the new fsContext to disk. Do this as a
         * sub-transaction so the stats will not be updated if we crash.
         */
#ifdef ADVFS_DEBUG
        if ( strlen(tnm) < 30 ) {
            (void)flmemcpy(
                           tnm,
                           from_file_context->file_name,
                           strlen(tnm) + 1
                           );
        } else {
            (void)flmemcpy(
                           tnm,
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
        ADVSMP_FILESTATS_LOCK(&from_file_context->fsContext_lock);
        from_file_context->fs_flag |= MOD_CTIME;
        from_file_context->dirty_stats = TRUE;
        ADVSMP_FILESTATS_UNLOCK(&from_file_context->fsContext_lock);

        ret = fs_update_stats(from_vp, from_accessp, ftx_handle, 0);
        if (ret != EOK) {
            ADVFS_SAD1("Error from fs_udpate_stats in advfs_rename", ret);
        }

    } else { /* to_vp != NULL */
        /*
         * There is a 'to' file.  Instead of doing a remove/add action on
         * the directory, we are going to just update existing directory
         * entry's tag to specify the new file.  The old file will have its
         * st_nlink value decremented to indicate it was removed from the
         * directory.  The approach taken helps insure that we meet the
         * following standard requirement:
         *
         *     "If the link named by the new argument exists, it is removed
         *     and old renamed to new. In this case, a link named new will
         *     remain visible to other processes throughout the renaming
         *     operation and will refer either to the file referred to by
         *     new or old before the operation began."
         *
         * If the 'to' directory is sticky, the user must own
         * either the 'to' directory or the file being renamed 'on
         * top of' or it can't be done
         */
        if ((to_dir_context->dir_stats.st_mode
             & S_ISVTX) && cred->cr_uid != 0 &&
            cred->cr_uid !=
            to_dir_context->dir_stats.st_uid) {
            ADVRWL_FILECONTEXT_READ_LOCK(to_file_context);
            if (to_file_context->dir_stats.st_uid
                != cred->cr_uid) {
                ADVRWL_FILECONTEXT_UNLOCK(to_file_context);

                error = EPERM;
                goto err_unlock_ffile;  /* unlock the to-dir */
            } else {
                ADVRWL_FILECONTEXT_UNLOCK(to_file_context);
            }
        }
        /*
         * if 'to' is a dir, make sure it is empty
         */
        if ((to_file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
            if (to_file_context->dir_stats.st_nlink != 2) {
                error = EEXIST;
                goto err_unlock_ffile;
            }

            ADVRWL_FILECONTEXT_READ_LOCK(to_file_context);

            ret = dir_empty(to_accessp, to_file_context);
            if (ret != EOK) {
                error = ret;
                if (error == I_FILE_EXISTS) {
                    error = EEXIST;
                }
                ADVRWL_FILECONTEXT_UNLOCK(to_file_context);
                goto err_unlock_ffile;
            }

            dnlc_purge_vp(from_dir_vp);
            dnlc_purge_vp(to_vp);

            ADVRWL_FILECONTEXT_UNLOCK(to_file_context);
        }
        ADVRWL_FILECONTEXT_WRITE_LOCK(to_file_context);
        to_file_context_locked = TRUE;

_cow_start:
        if (to_file_context->dir_stats.st_nlink == 1 &&
                HAS_SNAPSHOT_CHILD(VTOA(to_vp))) 
        {
            /* This file is being deleted.  We need to COW all
             * data over to the file's children.
             */
            ADVFS_SNAPSHOT_CREATE_SYNC( VTOA(to_vp) );
            advfs_force_cow_and_unlink(VTOA(to_vp),
                                       0,  /* Start at the beginning */
                                       (size_t)0,  /* To the end of the file */
                                       SF_SNAP_NOFLAGS,
                                       FtxNilFtxH);
            ADVFS_SNAPSHOT_DONE_SYNC( VTOA(to_vp) );
            if (VTOA(to_vp)->dmnP->dmn_panic) {
                error = EIO;
                goto err_unlock_ffile;
            }
        }

        /*
         * start the transaction now... Note: if not running under CFS,
         * sndp->ni_xid must be zero to guarantee uniqueness of
         * ftxId in the transaction log.
         */

        /* Need to take the lock on the index of the from directory
         * because of the call chain
         * remove_dir_ent--> idx_directory_insert_space -->
         * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
         * rbf_add_stg
         */
        IDX_GET_BFAP(from_dir_accessp, from_idx_bfap); 
        if (from_idx_bfap != NULL) { 
            ADVRWL_MIGSTG_READ( from_idx_bfap );
        }

        /* No need to take the quota locks - decr_link_count
         * cannot add storage to the quota files.
         */
        ret = FTX_START_XID(FTA_OSF_RENAME_V1, &ftx_handle, FtxNilFtxH,
                            dmnP, NAMEIDATA()->ni_xid);

        if (ret != EOK) {
            ADVFS_SAD1("error starting ftx in rename", ret);
        }

        if (HAS_SNAPSHOT_CHILD(VTOA(to_vp)) && 
                (VTOA(to_vp)->bfaFlags & BFA_SNAP_CHANGE)) 
        {
            ftx_quit(ftx_handle);
            goto _cow_start;
        }
        
        /* Remove the old dir entry. We need to do this prior to
         * pinning any records since there is a rare chance of
         * needing to fail the ftx.
         */

        ret = remove_dir_ent( from_dir_vp,
                              sndp,
                              snm,
                              ftx_handle,
                              TRUE);
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
                        ftx_handle,
                        MF_VERIFY_PAGE
                        );
        if (ret != EOK) {
            ADVFS_SAD1("rbf_pinpg error in advfs_rename", ret);
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
        if ( strlen(tnm) < 30 ) {
            (void)flmemcpy(
                           tnm,
                           from_file_context->file_name,
                           strlen(tnm) + 1
                           );
        } else {
            (void)flmemcpy(
                           tnm,
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
                       &dir_buffer->fs_dir_bs_tag_num,
                       sizeof(dir_buffer->fs_dir_bs_tag_num)
                       );
        MS_SMP_ASSERT(strlen(tnm)==dir_buffer->fs_dir_namecount);
        MS_SMP_ASSERT(bcmp(dir_buffer->fs_dir_name_string,tnm,strlen(tnm))==0);
        MS_SMP_ASSERT(dir_buffer->fs_dir_bs_tag_num ==
                                    to_file_context->bf_tag.tag_num);
        dir_buffer->fs_dir_bs_tag_num =
            from_file_context->bf_tag.tag_num;
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_502);

        seqp = GETSEQP(dir_buffer);
        rbf_pin_record(
                       page_ref,
                       seqp,
                       sizeof(*seqp)
                       );
        MS_SMP_ASSERT( *seqp == to_file_context->bf_tag.tag_seq);
        *seqp = from_file_context->bf_tag.tag_seq;
        ADVSMP_FILESTATS_LOCK( &to_dir_context->fsContext_lock );
        to_dir_context->dirstamp++;
        ADVSMP_FILESTATS_UNLOCK( &to_dir_context->fsContext_lock );
        dnlc_purge_vp( to_vp );
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_503);
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
            ADVSMP_FILESTATS_LOCK(&to_file_context->fsContext_lock);
            to_file_context->fs_flag |= MOD_CTIME;
            to_file_context->dirty_stats = TRUE;
            ADVSMP_FILESTATS_UNLOCK(&to_file_context->fsContext_lock);
            ret = fs_update_stats(to_vp, to_accessp, ftx_handle, 0);
            if (ret != EOK) {
                ADVFS_SAD1("Error from fs_udpate_stats in advfs_rename 2", ret);
            }
        }
        ADVRWL_FILECONTEXT_UNLOCK(to_file_context);
        to_file_context_locked = FALSE;

        ret = fs_update_stats(from_vp, from_accessp, ftx_handle, 0);
        if (ret != EOK) {
            ADVFS_SAD1("Error from fs_udpate_stats in advfs_rename 1", ret);
        }
    } /* end of 'there is a to-file' */

    /*
     * update the ctime and mtime fields of the to-directory
     */
    ADVSMP_FILESTATS_LOCK( &to_dir_context->fsContext_lock );
    to_dir_context->fs_flag |= (MOD_CTIME | MOD_MTIME);
    to_dir_context->dirty_stats = TRUE;
    ADVSMP_FILESTATS_UNLOCK( &to_dir_context->fsContext_lock );

    /*
     * put the to-dir stats out
     */
    ret = fs_update_stats(to_dir_vp, to_dir_accessp, ftx_handle, 0);
    if (ret != EOK) {
        ADVFS_SAD1("Error from fs_udpate_stats in advfs_rename 2", ret);
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
            ADVFS_SAD1("Error from fs_udpate_stats in advfs_rename 4", ret);
        }
        /* ignore errors from new_parent */
        ret = new_parent(from_vp, ftx_handle, newparent_tag);
        dnlc_purge_vp(from_dir_vp);
    }

    /*
     * all done
     */

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_7);

    ftx_done_fs(ftx_handle, FTA_OSF_RENAME_V1);

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_8);

    if (from_idx_bfap != NULL && from_idx_bfap != to_idx_bfap) {
        ADVRWL_MIGSTG_UNLOCK( from_idx_bfap );
    }

    if (to_dir_mig_stg_lock) {
        ADVRWL_MIGSTG_UNLOCK( to_dir_accessp );
        ADVRWL_MIGSTG_UNLOCK( to_dir_accessp->bfSetp->dirBfAp );
        if (to_idx_bfap != NULL) {
            ADVRWL_MIGSTG_UNLOCK( to_idx_bfap );
        }
        to_dir_mig_stg_lock = FALSE;
    }

    ADVRWL_FILECONTEXT_UNLOCK(from_file_context);

    /* finish any directory truncation that might have been started
     * in insert_seq for the 'to' directory. Must be done outside the
     * just-committed transaction.
     */
    dir_trunc_finish(to_dir_vp);

    /*
     * Put a new entry into the Directory Name Lookup Cache (dnlc) for the
     * renamed file.
     */
    dnlc_enter(to_dir_vp, tnm, from_vp, NOCRED);

    /*
     * unlock the dirs
     */
    ADVRWL_FILECONTEXT_UNLOCK(to_dir_context);
    if (to_dir_accessp != from_dir_accessp) {
        ADVRWL_FILECONTEXT_UNLOCK(from_dir_context);
    }
    /*
     * release all of the vnodes
     */

    if ( to_vp_vrefed ) {
        VN_RELE(tndp->ni_vp);
        to_vp_vrefed = FALSE;
    }
    VN_RELE(from_vp);

    /*
     * Clean up after we lost the delete race.
     */
    if ( to_removed_vp ) {
        VN_RELE(to_removed_vp);
    }

    NFS_FREEZE_UNLOCK(from_dir_vp);

    return(0);

err_fail_ftx:
    /* fail the transaction */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_504);
    ftx_fail(ftx_handle);
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_505);

    if (from_idx_bfap != NULL && from_idx_bfap != to_idx_bfap) { 
        ADVRWL_MIGSTG_UNLOCK( from_idx_bfap );
    }
    if (to_dir_mig_stg_lock) {
        ADVRWL_MIGSTG_UNLOCK( to_dir_accessp );
        ADVRWL_MIGSTG_UNLOCK( to_dir_accessp->bfSetp->dirBfAp );
        if (to_idx_bfap != NULL) {
            ADVRWL_MIGSTG_UNLOCK( to_idx_bfap );
        }
        to_dir_mig_stg_lock = FALSE;
    }

err_unlock_ffile:
    /* unlock the 'to' file */
    if (to_file_context_locked) {
        ADVRWL_FILECONTEXT_UNLOCK(to_file_context);
    }

    /* unlock the 'from' file */
    ADVRWL_FILECONTEXT_UNLOCK(from_file_context);

    /* unlock the 'from' directory and 'to' dirs */
    ADVRWL_FILECONTEXT_UNLOCK(to_dir_context);
    if (to_dir_accessp != from_dir_accessp) {
        ADVRWL_FILECONTEXT_UNLOCK(from_dir_context);
    }

err_rele_vnodes:
need_to_unfreeze:
    /* release vnodes */
    if ( to_vp_vrefed ) {
        VN_RELE(tndp->ni_vp);
        to_vp_vrefed = FALSE;
    }
    
    /*
     * Clean up after we lost the delete race.
     */
    if ( to_removed_vp ) {
        VN_RELE(to_removed_vp);
    }
    VN_RELE(from_vp);

    NFS_FREEZE_UNLOCK(from_dir_vp);

    return(error);
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
        ADVSMP_FILESTATS_LOCK( &file_context->fsContext_lock );
        file_context->fs_flag |= M_DELETE;
        ADVSMP_FILESTATS_UNLOCK( &file_context->fsContext_lock );
    }
    return (0);
}


/*
 * advfs_mkdir
 *
 * make a directory
 */
advfs_mkdir(
           struct vnode  *dvp,  /* in  - Parent Directory */
                  char   *name, /* in  - Name of new directory */
           struct vattr  *vap,  /* in  - Attributes of new directory */
           struct vnode **vpp,  /* out - vnode of created directory */
           struct ucred  *cred) /* in  - Credentials */
{
    struct nameidata *ndp;
    ni_nameiop_t      saved_op;
    statusT           ret;


    FILESETSTAT( dvp, advfs_mkdir );

    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp != NULL);

    saved_op = ndp->ni_nameiop;
    if (!(ndp->ni_nameiop & NI_CREATE)) {
        /*
         * Assumes that the nameidata structure will be properly setup if
         * and only if NI_CREATE is set by the caller (VFS layer), so we can
         * optimize the path.  If we find any other value in ni_nameiop,
         * then do our own lookup to initialize the nameidata structure.
         */
        advfs_lookup_t ali_flags = ALI_NO_FLAGS;

        ndp->ni_nameiop = NI_CREATE;
        ret = advfs_lookup_int(ali_flags, ndp, dvp, name, cred, dvp);
        ndp->ni_nameiop = saved_op;     /* setting outside of vfs, must be
                                           restored */
        if ( ret != EOK && ret != ENOENT ) {
            return(ret);
        }
        if ( ndp->ni_vp != NULL ) {
            /*
             * If the local lookup found a file, we need to release the
             * vnode and report EEXIST.
             */
            VN_RELE(ndp->ni_vp);
            return( EEXIST );
        }
    }

    NFS_FREEZE_LOCK(dvp);

    /* create the file */
    ret = fs_create_file(vap, ndp, dvp, name, NULL);

    if (ret == EOK){
        *vpp = ndp->ni_vp;
    }

    NFS_FREEZE_UNLOCK(dvp);

    return (ret);
}


/*
 * advfs_rmdir
 */
advfs_rmdir(
    struct vnode *dvp,  /* in - vnode of parent directory 
                         *      VFS has VN_HOLD, VFS will VN_RELE */
    char         *name, /* in - name of diecectory to remove */
    struct ucred *cred)
{
    struct nameidata     *ndp                = NULL;
    struct vnode         *rvp                = NULL; /* vnode of directory to
                                                        be removed */
    struct fsContext     *rem_context        = NULL;
    struct fsContext     *par_context        = NULL;
    struct bfNode        *bnp                = NULL;
    struct fileSetNode   *fsnp               = NULL;
    bfAccessT            *par_access         = NULL;
    bfAccessT            *idx_bfap           = NULL;
    bfAccessT            *remove_bfap        = NULL;
    bfAccessT            *rem_idx_bfap       = NULL;
    bfSetT               *bfSetp             = NULL;
    domainT              *dmnP               = NULL;
    int32_t               idx_mig_trunc_lock = FALSE;
    int32_t               idx_mig_stg_lock   = FALSE;
    int                   error              = 0;
    int                   fail_ftx           = 0;
    int                   reset_rem          = 0;
    int                   reset_par          = 0;
    statusT               ret;
    statusT               dirempty_ret;
    ftxHT                 ftx_handle;
    ni_nameiop_t          saved_op, int_op;


    FILESETSTAT( dvp, advfs_rmdir );

    if (error = advfs_access(dvp, S_IWRITE, cred))
        return (error);

    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp != NULL);
    saved_op = ndp->ni_nameiop;
    

    /*
     * The VFS layer did not give us the vnode of the file to be deleted, so
     * we have to go get it again.  We also want to fill in the nameidata
     * structure, especially the ni_count and ni_offset so we know where 
     * the file name is in the directory.
     *
     * NOTE:  While vn_remove() did do a lookup that filled in all the
     *        nameidata structure information, vns_remove() dropped the
     *        v_count reference on the vnode, so a race condition could have
     *        totally changed the underlying vnode.  Unlikely as it may be,
     *        it could still happen on a system under heavy memory pressure.
     *
     *        A suggested optimization would be to add a new NI_xxx flag to
     *        ni_nameiop that tells advfs_lookup() that we trust the other
     *        nameidata fields, but we need to verify/get the ni_vp value
     *        and if the dnlc has the correct value, please increment
     *        v_count and return the vnode to us, otherwise, please find the
     *        file in the directory and return the vnode with an incremented
     *        v_count.
     *
     *        HOWEVER, if we want to trust the nameidata structure, we MUST
     *        make sure that the caller really did a proper setup of the
     *        nameidata structure, so any optimization should test to see
     *        that the ni_nameiop field is set to NI_DELETE.  If it is not
     *        set to NI_DELETE, then this VOP was called by non-VFS layer
     *        code and we still need to do the full scale advfs_lookup() to
     *        get the correct nameidata information setup.
     *
     * NOTE:  At one time there was code to have advfs_lookup() increment 
     *        v_scount if the request was an NI_DELETE and NI_END_OF_PATH,
     *        then depend on the AdvFS rename, remove, and rmdir VOPs to
     *        decrement the v_scount.  While this sounds good, the problem
     *        is that in the VFS layer there are several error exit paths
     *        after calling VOP_LOOKUP(), that can bypass calling
     *        VOP_RENAME(), VOP_REMOVE(), or VOP_RMDIR().  In situations
     *        like this, no one would decrement v_scount.  
     *
     *        The VFS layer can not be made to deal with this, without
     *        putting a lot of AdvFS specific knowledge into the VFS layer,
     *        since VFS is generic code and does not know that AdvFS took an
     *        _EXTRA_ reference against the file.  If VFS were to call
     *        VN_SOLTRELE() on error exit paths, other file systems would
     *        end up with negative values of v_scount (and if the file
     *        system did not use v_scount, then VN_SOFTRELE() decrements
     *        v_count which would then go negative).  
     *
     *        Plus there is a risk that in the future, another kernel module
     *        developer would look at the VFS code that sets ni_nameiop to
     *        NI_DELETE and attempt to mimic this, but not fully understand
     *        the rules.  They might call VOP_LOOKUP() and then call the
     *        wrong VOP or use the arguments in a way that the VOP_RENAME(),
     *        VOP_REMOVE(), or VOP_RMDIR() do not expect.  The worse case
     *        situation would be that it appears to work for the developer,
     *        but AdvFS can not umount the file system because there is a
     *        vnode (or 2) hanging around with elevated v_scount values.
     *
     *        Any future optimizations must take this into consideration and
     *        should make the actions of VOP_LOOKUP() _NOT_ require house
     *        keeping be done by VOP_RENAME(), VOP_REMOVE(), or VOP_RMDIR().
     */
    ndp->ni_nameiop = NI_DELETE | NI_END_OF_PATH;
    error = advfs_lookup_int(ALI_NO_DNLC, ndp, dvp, name, cred, dvp);
    int_op = ndp->ni_nameiop;    /* record setting of NI_BADDIRSTAMP */
    ndp->ni_nameiop = saved_op;  /* setting outside of vfs, must be restored */
    if (error) {
        return(error);
    }
    rvp = ndp->ni_vp;

    ADVFS_ACCESS_LOGIT( ((struct bfNode*)rvp->v_data)->accessp, "advfs_rmdir back from lookup");
    /*
     * If the vnode we found is not a directory, we raced another rmdir
     * who removed it.  Subsequently, another thread then created a file
     * of the same name in the directory.
     */
    if (rvp->v_type != VDIR) {
        VN_RELE(rvp);
        return ENOTDIR;
    }

    remove_bfap = VTOA( rvp );

    /*
     * the directory being removed will be locked to prevent any changes to it
     * lock the parent directory as well
     */

    par_access = VTOA(dvp);
    bnp = VTOBNP(rvp);
    rem_context = bnp->fsContextp;
    bnp = VTOBNP(dvp);
    par_context = bnp->fsContextp;
    fsnp = VTOFSN(rvp);
    dmnP = fsnp->dmnP;
    bfSetp = fsnp->bfSetp;

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        VN_RELE(rvp);
        return EIO;
    }

    NFS_FREEZE_LOCK(rvp);

    if (remove_bfap < par_access) {
        ADVRWL_FILECONTEXT_WRITE_LOCK(rem_context);
        ADVRWL_FILECONTEXT_WRITE_LOCK(par_context);
    } else {
        ADVRWL_FILECONTEXT_WRITE_LOCK(par_context);
        ADVRWL_FILECONTEXT_WRITE_LOCK(rem_context);
    }

    /*
     * check for an empty directory and read only fileset
     */

    if (fsnp->vfsp && (fsnp->vfsp->vfs_flag & VFS_RDONLY)) {
        error = EROFS;
        goto _error_cleanup;
    }
    dirempty_ret = dir_empty( remove_bfap, rem_context);
    if (dirempty_ret == I_FILE_EXISTS) {
        error = EEXIST;
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
                     fsnp->tagsTag)) {
        if (BS_BFTAG_EQL(par_context->bf_tag,
            fsnp->rootTag)) {

            error = EACCES;
            goto _error_cleanup;
        }
    }

    if (ndp->ni_dirstamp != par_context->dirstamp
                || (int_op & NI_BADDIRSTAMP)) {
        int ret2;
        bfTagT found_bs_tag;
        fs_dir_entry *dir_buffer;
        rbfPgRefHT page_ref;

        ret2 = seq_search(
                         dvp,
                         ndp,
                         name,
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
     * Remove the Directory Name Lookup Cache entry for the removed
     * directory.  If the vnode is in the dnlc, a v_scount will be removed.
     */
    dnlc_purge_vp(rvp);
    MS_SMP_ASSERT( rvp->v_scount >= 0 );

    /*
     * decr the parent directory's link count
     */
    par_context->dir_stats.st_nlink--;
    ADVSMP_FILESTATS_LOCK( &par_context->fsContext_lock );
    par_context->dirty_stats = TRUE;
    ADVSMP_FILESTATS_UNLOCK( &par_context->fsContext_lock );
    reset_par = TRUE;

cow_again:
    /* We need to force a full COW to this file's children if there
     * are any related snapshots.  This also is done before we
     * start the transaction in order to avoid LOG half full errors.
     */
    if (HAS_SNAPSHOT_CHILD(remove_bfap)) {
        ADVFS_SNAPSHOT_CREATE_SYNC( remove_bfap );
        ret = advfs_force_cow_and_unlink( remove_bfap, 
                                          0L,  /* Start at the beginning */
                                          0UL,  /* To the end of the file */
                                          SF_SNAP_NOFLAGS,
                                          FtxNilFtxH);
        ADVFS_SNAPSHOT_DONE_SYNC( remove_bfap );

        /* If advfs_force_cow_and_unlink failed, it marked the chlidren
         * as out of sync. A domain panic may have occured.  Ignore any
         * error if there is NO domain panic. */
        if (remove_bfap->dmnP->dmn_panic) {
            error = EIO;
            goto _error_cleanup;
        }
    }

index_cow_again:
    if (IDX_INDEXING_ENABLED(remove_bfap)) {
        IDX_GET_BFAP(remove_bfap, rem_idx_bfap);
        MS_SMP_ASSERT(rem_idx_bfap != NULL);
        if (HAS_SNAPSHOT_CHILD(rem_idx_bfap)) {
            ADVFS_SNAPSHOT_CREATE_SYNC( rem_idx_bfap );
            ret = advfs_force_cow_and_unlink( rem_idx_bfap,
                                              0L,
                                              0UL,
                                              SF_SNAP_NOFLAGS,
                                              FtxNilFtxH);
            ADVFS_SNAPSHOT_DONE_SYNC( rem_idx_bfap );

            /* Only return an error if there was a domain panic */
            if (remove_bfap->dmnP->dmn_panic) {
                error = EIO;
                goto _error_cleanup;
            }
        }
    }
    
    /* Need to lock the index because of the call chain
     * remove_dir_ent --> idx_directory_insert_space -->
     * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
     * rbf_add_stg
     */
    IDX_GET_BFAP(par_access, idx_bfap); 
    if (idx_bfap != NULL) { 
        ADVRWL_MIGSTG_READ( idx_bfap );
        idx_mig_stg_lock = TRUE;
    }

    /* No need to take the quota locks. rbf_delete will NOT
     * add storage to the quota files. The entry already
     * exists.
     */

    


    ret = FTX_START_XID(FTA_OSF_RMDIR_V1, &ftx_handle, FtxNilFtxH,
                        dmnP, ndp->ni_xid);

    if (ret != EOK) {
        ADVFS_SAD1("Error starting transaction in advfs_rmdir", ret);
    }

    if (HAS_SNAPSHOT_CHILD(remove_bfap) && 
            (remove_bfap->bfaFlags & BFA_SNAP_CHANGE)) 
    {
        /* Looks like a snapset created between the cow and starting
         * the transaction.
         */
        ftx_quit(ftx_handle);
        goto cow_again;
    }

    if (IDX_INDEXING_ENABLED(remove_bfap)) {
        MS_SMP_ASSERT(rem_idx_bfap != NULL);

        if (HAS_SNAPSHOT_CHILD(rem_idx_bfap) && 
                (rem_idx_bfap->bfaFlags & BFA_SNAP_CHANGE)) 
        {
            /* Looks like a snapset created between the cow and starting
             * the transaction.
             */
            ftx_quit(ftx_handle);
            goto index_cow_again;
        }
    }

    /*
     * decr the dir's link count by 2,
     * delete . and .. and mark the file for delete,
     * and delete the entry.  The bitfile delete is done first to
     * avoid potential bmt page pinblock deadlock with the update
     * stats in remove_dir_ent.
     */
    rem_context->dir_stats.st_nlink -= 2;
    reset_rem = TRUE;

    rbf_delete(remove_bfap, ftx_handle );

    ADVSMP_FILESTATS_LOCK( &rem_context->fsContext_lock );
    rem_context->fs_flag |= M_DELETE;
    ADVSMP_FILESTATS_UNLOCK( &rem_context->fsContext_lock );

    /*
     * Remove the index if it is setup. We need to remove it now
     * in case we fail (we will domain panic) and can still call
     * ftx_fail. It isn't actually deleted until the last close.
     */

    if (IDX_INDEXING_ENABLED(remove_bfap))
    {
        ret=idx_remove_index_file(remove_bfap, ftx_handle);
        if (ret != EOK)
        {
            error=ret;
            fail_ftx = TRUE;
            goto _error_cleanup;
        }
    }

    ret = remove_dir_ent(
                         dvp,
                         ndp,
                         name,
                         ftx_handle,
                         FALSE);
    if (ret != EOK) {
        ADVSMP_FILESTATS_LOCK( &rem_context->fsContext_lock );
        rem_context->fs_flag &= ~M_DELETE;
        ADVSMP_FILESTATS_UNLOCK( &rem_context->fsContext_lock );
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

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_29);

    ftx_done_fs(ftx_handle, FTA_OSF_RMDIR_V1);

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_30);


    /* This close will disassociate the index from the directory */


    if (IDX_INDEXING_ENABLED(remove_bfap))
    {
        idx_close_index_file(remove_bfap,IDX_REMOVING_DIR);
    }

    if (idx_bfap != NULL) {
        ADVRWL_MIGSTG_UNLOCK( idx_bfap );
    }

    /*
     * unlock dir and rem_dir
     */
    ADVRWL_FILECONTEXT_UNLOCK(rem_context);
    ADVRWL_FILECONTEXT_UNLOCK(par_context);

    NFS_FREEZE_UNLOCK(rvp);

    VN_RELE(rvp);

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
    if (fail_ftx) {
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_506);
        ftx_fail(ftx_handle);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_507);
    }

    if (idx_mig_stg_lock) {
        if (idx_bfap != NULL) {
            ADVRWL_MIGSTG_UNLOCK( idx_bfap );
        }
    }

    ADVRWL_FILECONTEXT_UNLOCK(rem_context);
    ADVRWL_FILECONTEXT_UNLOCK(par_context);

    NFS_FREEZE_UNLOCK(rvp);

    VN_RELE(rvp);

    return( error );
}

/*
 * advfs_symlink
 *
 * create a symbolic link
 */
advfs_symlink(
    struct vnode *dir_vp,      /* in - directory vnode pointer */
    char         *linkname,    /* in - create this symlink name */
    struct vattr *vap,         /* in - create with these attributes */
    char         *targetname,  /* in - target name for symlink */
    struct ucred *cred)        /* in - credentials */
{
    statusT           ret;
    struct nameidata *ndp;
    ni_nameiop_t      saved_op;
    int               error;

    FILESETSTAT( dir_vp, advfs_symlink );

    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp != NULL);

    saved_op = ndp->ni_nameiop;
    if (!(ndp->ni_nameiop & NI_CREATE)) {
        /*
         * Assumes that the nameidata structure will be properly setup if
         * and only if NI_CREATE is set by the caller (VFS layer), so we can
         * optimize the path.  If we find any other value in ni_nameiop,
         * then do our own lookup to initialize the nameidata structure.
         */
        advfs_lookup_t ali_flags = ALI_NO_FLAGS;
 
        ndp->ni_nameiop = NI_CREATE;
        error = advfs_lookup_int(ali_flags, ndp, 
                                 dir_vp, linkname, cred, dir_vp);
        ndp->ni_nameiop = saved_op;     /* setting outside of vfs, must be
                                           restored */
        if ( error != EOK && error != ENOENT ) {
            return(error);
        }
        if ( ndp->ni_vp != NULL ) {
            /*
             * If the local lookup found a file, we need to release the
             * vnode and report EEXIST.
             */
            VN_RELE(ndp->ni_vp);
            return( EEXIST );
        }
    } else if ( (ndp->ni_vp != NULL) && !(ndp->ni_nameiop & NI_BADDIRSTAMP)) {
        return EEXIST;
    }
    /* N.B.  If NI_CREATE is not set, we cannot just assume that ni_vp is
     * correctly initilaized.  If the file exists, it will be found in
     * fs_create_file and EEXIST will be returned.
     */

    vap->va_type = VLNK;
    vap->va_rdev = 0;

    NFS_FREEZE_LOCK(dir_vp);

    ret = fs_create_file(vap, ndp, dir_vp, linkname, targetname);

    NFS_FREEZE_UNLOCK(dir_vp);
    
    /*
     * fs_create_file() can result in the new symlink vnode having a
     * v_count. Failure to VN_RELE() this vnode results in an extra vnode
     * ref.
     */
    if (ndp->ni_vp != NULL) {
        VN_RELE(ndp->ni_vp);
    }

    return(ret);
}

#define ADVFS_READDIR_MAX (ADVFS_METADATA_PGSZ * 8)
/*
 * advfs_readdir
 *
 * read a buffer full of directory entries
 */
advfs_readdir(
    struct vnode *vp,
    struct uio *uiop,
    struct ucred *cred)
{
    int error = 0;
    bfAccessT *bfap;
    struct bfNode *bnp;
    off_t offset;
    struct iovec t_iovec;
    struct uio t_uio;
    char *readbuf;
    long readlen;
    long bytes_read, bytes2return;
    char *first_new_dp;
    fs_dir_entry *dp, *lastdp;

    FILESETSTAT( vp, advfs_readdir );

    MS_SMP_ASSERT(uiop->uio_iovcnt == 1);

    bnp = VTOBNP(vp);
    if ( (bnp->fsContextp->dir_stats.st_mode & S_IFMT) != S_IFDIR ) {
        return(EINVAL);
    }
    MS_SMP_ASSERT(vp->v_type == VDIR);

    /* 
     * If number of bytes to read is 0 or negative, it is invalid 
     */
    if (uiop->uio_resid < (int64_t)sizeof (fs_dir_entry))
        return EINVAL;

    if ((uiop->uio_offset & (DIRBLKSIZ-1)) == 0 &&
        (uiop->uio_resid & (DIRBLKSIZ-1)) == 0) {
        error = advfs_fs_read(
                       vp,
                       uiop,
                       NULL,
                       0,
                       cred
                       );

        return(error);
    }

    /*
     * See ufs_readdir().  Callers including advfs_readdir2() and
     * advfs_readdir3() depend on a directory entry starting at
     * the beginning of the buffer returned, and that the last
     * directory entry in the buffer is complete.
     */

    offset = uiop->uio_offset & ~(DIRBLKSIZ-1);	
    readlen = ((uiop->uio_offset & (DIRBLKSIZ-1))
               + uiop->uio_resid + (DIRBLKSIZ-1)) & ~(DIRBLKSIZ-1);
    /*
     * Limit size of read - getdentries() limits resid to MAXBSIZE,
     * getdents() is not documented and readdir() uses 8192.  If a
     * program uses getdents() for larger size, ok to read less as
     * long as correct size (uio_resid) and at least 1 entry is 
     * returned.
     */
    readlen = MIN(readlen, ADVFS_READDIR_MAX);
    readbuf = ms_malloc((size_t)readlen);
    t_uio.uio_seg = UIOSEG_KERNEL;
    t_uio.uio_fpflags = 0;
    t_uio.uio_iov = &t_iovec;
    t_uio.uio_iovcnt = 1;

nextblk:
    t_uio.uio_offset = offset;
    t_uio.uio_resid = t_iovec.iov_len = readlen;
    t_iovec.iov_base = readbuf;
    if (error = advfs_fs_read(vp, &t_uio, NULL, 0, cred))
        goto out;
    bytes_read = readlen - t_uio.uio_resid;
    lastdp = dp = (fs_dir_entry *)readbuf;
    first_new_dp = (char *) NULL;

    /*
     * Skip entries before uio_offset.
     */
    while ((char *)dp < (readbuf + bytes_read)
           && (offset < uiop->uio_offset)) {
        MS_SMP_ASSERT(dp->fs_dir_size >= MIN_DIR_ENTRY_SIZE &&
                      (dp->fs_dir_size & 7) == 0);

        offset += dp->fs_dir_size;
        dp = (fs_dir_entry *)((char *)dp + dp->fs_dir_size);
    }

    /*
     * Because we rounded up we have no partial entries.
     */
    if ((char *)dp < (readbuf + bytes_read))
        first_new_dp = (char *) dp;
    else if (offset < bnp->fsContextp->dir_stats.st_size)
            goto nextblk;

    uiop->uio_offset = offset;
    /*
     * See how much we can return.
     */
    while ((char *)dp < (readbuf + bytes_read) &&
           ((long)((char *)dp - first_new_dp + dp->fs_dir_size))
           <= uiop->uio_resid) {
        MS_SMP_ASSERT(dp->fs_dir_size >= MIN_DIR_ENTRY_SIZE &&
                      (dp->fs_dir_size & 7) == 0);
        lastdp = dp;
        dp = (fs_dir_entry *)((char *)dp + dp->fs_dir_size);
    }
    if (first_new_dp){
        bytes2return = (char *) lastdp + lastdp->fs_dir_size
                        - first_new_dp;
        error = uiomove(first_new_dp, (int)bytes2return, UIO_READ, uiop);
    } 
out:
    ms_free(readbuf);
    return error;
}

/*
 * Copied from generic_readdir2()
 */
advfs_readdir2(
    struct vnode *vp,
    struct uio *uiop,
    struct ucred *cred)
{
	struct iovec *iovp;
	fs_dir_entry *idp;
	struct __dirent32 *odp;
	int incount;
	int outcount = 0;
	off_t offset;
	size_t  count, bytes_read;
	struct iovec t_iovec;
	struct uio t_uio;
	char *outbuf;
	char *inbuf;
	size_t bufsize;
	int error = 0;

	/*
	 * Get space to change directory entries into fs independent format.
	 * Also, get space to buffer the data returned by FS_readdir.
	 *
	 * Note that count must be a multiple of 8, because we malloc
	 * a buffer of 2*(count+sizeof struct __dirent32) and then index into
	 * it at the halfway point.  If count is not a multiple of 8, we
	 * will get a trap 18 unaligned data access panic.
	 */
	count = ((uiop->uio_resid + 7) & ~7L);
	bufsize = count + sizeof (struct __dirent32);
        bufsize = MIN(bufsize, ADVFS_READDIR_MAX);
	outbuf =  ms_malloc(bufsize * 2);
	inbuf = (char *)((uintptr_t)outbuf + bufsize);
	iovp = &t_iovec;

	/*
	 * Setup a temporary uio structure to handle the directory information
	 * before reformatting.
	 */
loop:
	odp = (struct __dirent32 *)outbuf;
	iovp->iov_len = count;
	iovp->iov_base = inbuf;
	t_uio.uio_iov = iovp;
	t_uio.uio_iovcnt = 1;
	t_uio.uio_seg = UIOSEG_KERNEL;
	t_uio.uio_offset = uiop->uio_offset;
	t_uio.uio_fpflags = 0;
	t_uio.uio_resid = uiop->uio_resid;

	if (error = advfs_readdir(vp, &t_uio, cred))
		goto out;
	bytes_read = uiop->uio_resid - t_uio.uio_resid;
	offset = t_uio.uio_offset - bytes_read;

	incount = 0;
	idp = (fs_dir_entry *)inbuf;

	/* Transform to file-system independent format */
	while ((size_t)(u_int)incount < bytes_read) {

		/* skip empty entries */
		if (idp->fs_dir_bs_tag_num != 0 && offset >= uiop->uio_offset) {
			odp->__d_ino = (ino32_t)idp->fs_dir_bs_tag_num;
			odp->__d_namlen = idp->fs_dir_namecount;
			(void) strncpy(odp->__d_name, idp->fs_dir_name_string,
                                       (uint64_t)(unsigned short)idp->fs_dir_namecount);
			odp->__d_name[odp->__d_namlen] = '\0';
			odp->__d_reclen = __DIRENT_RECLEN(odp);
			odp->__d_off = offset + idp->fs_dir_size; 
			outcount += odp->__d_reclen;
			/* Got as many bytes as requested, quit */
			if ((size_t)(u_int)outcount > count) {
				outcount -= odp->__d_reclen;
				break;
			}
			odp = (struct __dirent32 *)((uintptr_t)odp + 
							odp->__d_reclen);
		}
		/* verify 8-byte alignment */
                MS_SMP_ASSERT ((idp->fs_dir_size & 7) == 0);
		if (idp->fs_dir_size == 0) {
			error = EINVAL;
			goto out;
		}
		incount += idp->fs_dir_size;
		offset += idp->fs_dir_size;
		idp = (fs_dir_entry *)((intptr_t)idp + idp->fs_dir_size);
	}
	if ((bytes_read > 0) && (outcount == 0)) {
		/*
		 *	Not at EOF so need to get at least 1
		 */
		uiop->uio_offset = offset;
		goto loop;
	}

	if (error = uiomove(outbuf, outcount, UIO_READ, uiop))
		goto out;

	uiop->uio_offset = offset;
out:
	ms_free(outbuf);
	return (error);

}

/*
 * Based on advfs_readdir2()  (generic_readdir2) with modifications
 * to support  __dirent64 output format
 */
advfs_readdir3(
    struct vnode *vp,
    struct uio *uiop,
    struct ucred *cred)
{
	struct iovec *iovp;
	fs_dir_entry *idp;
	struct __dirent64 *odp;
	int incount;
	int outcount = 0;
	off_t offset;
	size_t  count, bytes_read;
	struct iovec t_iovec;
	struct uio t_uio;
	char *outbuf;
	char *inbuf;
	size_t bufsize;
	int error = 0;

	/*
	 * Get space to change directory entries into fs independent format.
	 * Also, get space to buffer the data returned by FS_readdir.
	 *
	 * Note that count must be a multiple of 8, because we malloc
	 * a buffer of 2*(count+sizeof struct __dirent64) and then index into
	 * it at the halfway point.  If count is not a multiple of 8, we
	 * will get a trap 18 unaligned data access panic.
	 */
	count = ((uiop->uio_resid + 7) & ~7L);
	bufsize = count + sizeof (struct __dirent64);
        bufsize = MIN(bufsize, ADVFS_READDIR_MAX);
	outbuf =  ms_malloc(bufsize * 2);
	inbuf = (char *)((uintptr_t)outbuf + bufsize);
	iovp = &t_iovec;

	/*
	 * Setup a temporary uio structure to handle the directory information
	 * before reformatting.
	 */
loop:
	odp = (struct __dirent64 *)outbuf;
	iovp->iov_len = count;
	iovp->iov_base = inbuf;
	t_uio.uio_iov = iovp;
	t_uio.uio_iovcnt = 1;
	t_uio.uio_seg = UIOSEG_KERNEL;
	t_uio.uio_offset = uiop->uio_offset;
	t_uio.uio_fpflags = 0;
	t_uio.uio_resid = uiop->uio_resid;

	if (error = advfs_readdir(vp, &t_uio, cred))
		goto out;
	bytes_read = uiop->uio_resid - t_uio.uio_resid;
	offset = t_uio.uio_offset - bytes_read;

	incount = 0;
	idp = (fs_dir_entry *)inbuf;

	/* Transform to file-system independent format */
	while ((size_t)(u_int)incount < bytes_read) {

		/* skip empty entries */
		if (idp->fs_dir_bs_tag_num != 0 && offset >= uiop->uio_offset) {
			odp->__d_ino = (ino64_t)idp->fs_dir_bs_tag_num;
			odp->__d_namlen = idp->fs_dir_namecount;
			(void) strncpy(odp->__d_name, idp->fs_dir_name_string,
                                       (uint64_t)(unsigned short)idp->fs_dir_namecount);
			odp->__d_name[odp->__d_namlen] = '\0';
			odp->__d_reclen = __DIRENT_RECLEN(odp);
			odp->__d_off = offset + idp->fs_dir_size; 
			outcount += odp->__d_reclen;
			/* Got as many bytes as requested, quit */
			if ((size_t)(u_int)outcount > count) {
				outcount -= odp->__d_reclen;
				break;
			}
			odp = (struct __dirent64 *)((uintptr_t)odp + 
							odp->__d_reclen);
		}
		/*  verify 8-byte alignment */
                MS_SMP_ASSERT((idp->fs_dir_size & 7) == 0);
		if (idp->fs_dir_size == 0) {
			error = EINVAL;
			goto out;
		}
		incount += idp->fs_dir_size;
		offset += idp->fs_dir_size;
		idp = (fs_dir_entry *)((intptr_t)idp + idp->fs_dir_size);
	}
	if ((bytes_read > 0) && (outcount == 0)) {
		/*
		 *	Not at EOF so need to get at least 1
		 */
		uiop->uio_offset = offset;
		goto loop;
	}

	if (error = uiomove(outbuf, outcount, UIO_READ, uiop))
		goto out;

	uiop->uio_offset = offset;
out:
	ms_free(outbuf);
	return (error);

}


/*
 * advfs_readlink
 *
 * read the contents of a symbolic link
 */

advfs_readlink(
    struct vnode *vp,   /* in - vnode pointer of file to read */
    struct uio *uiop,   /* in - uio structure pointer */
    struct ucred *cred) /* in - credentials of caller */
{
    bfAccessT *bfap;
    struct fsContext *context_ptr;
    uint64_t isize;
    int error;
    char *buffer = NULL;
    statusT sts;

    ADVFS_VOP_LOCKSAFE;

    FILESETSTAT( vp, advfs_readlink );

    bfap = VTOA(vp);
    context_ptr = VTOC(vp);

    /*
     * screen domain panic
     */
    if (bfap->dmnP->dmn_panic) {
	ADVFS_VOP_LOCKUNSAFE;
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
                           (uint16_t)isize
                           );

        if (sts != EOK) {
            error = BSERRMAP(sts);
        } else {
            error = uiomove(
                            buffer,
                            (int64_t)isize,
                            UIO_READ, uiop
                            );
        }
        ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );
        context_ptr->dirty_stats = TRUE;
        context_ptr->fs_flag |= MOD_ATIME;
        ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );
        ms_free( buffer );
        goto _exit;
    }

    /* if it is not a fast link, read it from the file */
    if (uiop->uio_resid == 0) {
	ADVFS_VOP_LOCKUNSAFE;
        return(0);
    }

    if ((long) uiop->uio_offset < 0) {
	ADVFS_VOP_LOCKUNSAFE;
        return(EINVAL);
    }

    error = advfs_fs_read(vp, uiop, UIO_READ, 0, cred);

_exit:
    ADVFS_VOP_LOCKUNSAFE;
    return( error );
}


int
advfs_bmap(
    struct vnode  *vp,
    kern_daddr_t   lbn,
    struct vnode **vpp,
    kern_daddr_t  *bnp,
    int            flags)
{
    bfAccessT *bfap;
    bsInMemXtntMapT *xtnt_map;
    off_t offset;
    extent_blk_desc_t *extent_blk_desc;
    domainT *dmnp;
    struct vd *vdp;
    statusT ret;

    bfap = VTOA(vp); 
    MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);

    xtnt_map = bfap->xtnts.xtntMap;
    offset = lbn * DEV_BSIZE; 
    extent_blk_desc = NULL;

    ret = advfs_get_blkmap_in_range( bfap,
                                     xtnt_map,
                                     &offset,
                                     DEV_BSIZE,
                                     &extent_blk_desc,
                                     NULL,
                                     RND_NONE,
                                     EXB_COMPLETE,
                                     XTNT_WAIT_OK);

    if (ret != 0 ) {

        /*
         * advfs_get_blkmap_in_range error. Still try to give the caller
         * usfull info before returning. 
         */

        dmnp = ADVGETDOMAINP(vp->v_vfsp);
        vdp  = VD_HTOP(bfap->primMCId.volume, dmnp);

        if (vpp && !bnp){
            *vpp = vdp->devVp;
        }

        if (vpp && bnp) {
            *vpp = vdp->devVp;
            *bnp = (daddr_t)(0);
        }

        if (!vpp && bnp) {
            *bnp = (daddr_t)(0);
        }
        
        return (0);
    }

    MS_SMP_ASSERT(extent_blk_desc != NULL);
    MS_SMP_ASSERT(extent_blk_desc->ebd_next_desc == NULL);

    if (bnp) {
        *bnp = (daddr_t)extent_blk_desc->ebd_vd_blk;
    }
 
    if (vpp){
        /* get vnode of device */
        dmnp = ADVGETDOMAINP(vp->v_vfsp);
        
        MS_SMP_ASSERT(bnp != NULL);
        
        if (*bnp < 0) {  /* block was a hole, use primary mcell vd. */
            vdp  = VD_HTOP(bfap->primMCId.volume, dmnp);
        }
        else { /* block was not a hole, use vd for that block. */
            vdp  = VD_HTOP(extent_blk_desc->ebd_vd_index, dmnp);
        }
        
        *vpp = vdp->devVp; 
    }

    advfs_free_blkmaps(&extent_blk_desc); 
    MS_SMP_ASSERT(extent_blk_desc == NULL);
    return (0);
}

/*
 * advfs_strategy()
 *
 * Description:
 *
 * Advfs_strategy() will perform a read or write IO request using information
 * supplied from the buf structure.
 * It is assumed that this request is coming from VM swap to an AdvFS swap
 * filesystem and that the buf structure handling requires traditional
 * buffer style IO completion handling.
 *
 * This routine and any called routines must not hold any AdvFS locks 
 * as the VM component's swap filesystem code will hold VM locks that 
 * are lower in the lock hierarchy than AdvFS.
 *
 * The buf structure memory comes from a non-AdvFS component and
 * not from an AdvFS ms_malloc.
 */
int 
advfs_strategy(struct buf *bp)
{
    int retval = 0;
    bfAccessT *bfap;
    bsInMemXtntMapT *xtnt_map;
    extent_blk_desc_t *extent_blk_desc;
    ioanchor_t *ioanchorp;
    statusT error;
    off_t adj_offset;
    vdT *vdp;
   
    /*
     * Make sure we have a vp.
     */
    if (!bp->b_vp) {
        BSETFLAGS(bp,(bufflags_t)B_ERROR);
        bp->b_error = EINVAL;
        biodone(bp);
        return(EINVAL);
    }

    /* Save the original file byte offset. */
    bp->b_foffset = bp->b_offset;
      
    /* Get the file's allocated storage extent block mappings to check
     * for the number of contiguous file offset blocks (FOB's) starting
     * for the given file offset up to the given byte length.
     * The offset passed into the blkmap routine must be aligned to
     * the allocation unit size. 
     * Also increment the requested length by the difference between the
     * file's disk allocation unit size aligned
     * offset and the caller's offset.
     */

    bfap = VTOA(bp->b_vp); 
    MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);
    adj_offset = bp->b_offset;
    extent_blk_desc = NULL;
    xtnt_map = bfap->xtnts.xtntMap;

    error = advfs_get_blkmap_in_range(bfap,
                                      xtnt_map,
                                      &adj_offset,
                                      bp->b_bcount,
                                      &extent_blk_desc,
                                      NULL,
                                      RND_NONE,
                                      EXB_ONLY_STG,
                                      XTNT_WAIT_OK|XTNT_LOCKS_HELD);
    if (error) {
        /* Return size from caller's offset to end of an allocation unit.*/
        BSETFLAGS(bp,(bufflags_t)B_ERROR);
        bp->b_error = BSERRMAP(error);
        biodone(bp);
        return(BSERRMAP(error));
    }
   
    /* Only the first block map entry will be examined. The presumption
     * is the caller predetermined the IO request's length fits within
     * one extent of contiguous disk blocks.
     */
    if ((extent_blk_desc != NULL) &&
        (extent_blk_desc->ebd_vd_blk != XTNT_TERM)){
        /* Assign the logical virtual disk block number to the buf structure.
         * The assumption is that the caller preallocated storage and
         * has predetermined their IO fits within one extent.
         */
        MS_SMP_ASSERT(bp->b_bcount <= extent_blk_desc->ebd_byte_cnt);
        bp->b_blkno = extent_blk_desc->ebd_vd_blk;
        vdp = VD_HTOP(extent_blk_desc->ebd_vd_index, bfap->dmnP);

        /* Issue the IO request. Advfs_iodone() will free the ioanchor
         * memory. Since the caller supplies their own buf structure, set
         * the ioanchor flag so that advfs_iodone() does not free the buf.
         * Also set a flag to indicate to advfs_iodone() that the buf
         * handling must call finish_biodone() to do traditional buffer
         * post IO handling.
         */
        ioanchorp = advfs_bs_get_ioanchor( M_WAITOK );
        ioanchorp->anchr_iocounter = 1;
        ioanchorp->anchr_flags = 
            IOANCHORFLG_KEEP_BUF | IOANCHORFLG_ADVFS_STRATEGY_IO;
        advfs_bs_startio(bp, ioanchorp, NULL, bfap, vdp, 0);
    }
    else {
        /* IO to unmapped (hole) storage is not acceptable through this VOP.*/
        retval = EIO;
    }
    advfs_free_blkmaps(&extent_blk_desc); 
    MS_SMP_ASSERT(extent_blk_desc == NULL);

    if (retval) {
        BSETFLAGS(bp,(bufflags_t)B_ERROR);
        bp->b_error = retval;
        biodone(bp);
    }
    return(retval);
}

/*
 * advfs_aiostrategy()
 *
 * Description:
 *
 * This is the interface between the AIO layer and the AdvFS filesystem.
 * It is assumed that this request is coming from aio_rw() for directIO 
 * handling.  (This avoids the slower AIO mechanism whereby a different
 * thread is used to handle the IO through the normal read/write interface.)
 *
 * Inputs:
 *   bp.b_flags       B_READ or B_WRITE for type of operation
 *   bp.b_vp          vnode for this file
 *   bp.b_rp          struct file * for this file (for DIO only; otherwise NULL)
 *   bp.b_un.b_addr   address of application buffer
 *   bp.b_bcount      size (in bytes) of transfer requested
 *   bp.b_offset      byte offset in file for start of transfer
 *   bp.b_iodone      address of AIO completion routine
 *   bp.b_prio        contains the I/O priority as adjusted by aio_reqprio.
 * Outputs:
 *   bp.b_error       0 if success; else error code
 *   bp.b_resid       # bytes not transferred (out of bp.b_bcount)
 *
 * The value returned in bp.b_error is also returned by the routine.
 *
 * Flags values:
 *   IS_VOP_AIO_SUPPORTED - passed by AIO to see if this filesystem
 *                          supports this VOP.  This is used to prevent
 *                          setting up for the call only to find that the
 *                          filesystem won't handle the call, and that 
 *                          they need to used the threaded implementaion
 *                          anyway.
 */
int 
advfs_aiostrategy(struct buf *bp,
                  int         flags )
{
    struct file *fp;
    struct uio uio;
    struct iovec vec[2];
    struct nameidata * ndp;
    ni_nameiop_t saved_nameiop;
    int retval = 0;

    /* Just return success. bp value may be NULL. */
    if ( flags & IS_VOP_AIO_SUPPORTED ) {
        return 0;
    }

    /*
     * Make sure we have a vp and file structure pointer.
     */
    if ( !bp->b_vp || !bp->b_rp ) {
        MS_SMP_ASSERT(0);       /* We should not get here */
        return EINVAL;
    }

    fp = (struct file *)bp->b_rp;

    vec[0].iov_base = (caddr_t)bp->b_un.b_addr;
    vec[0].iov_len = bp->b_bcount;
    vec[1].iov_base = (caddr_t)bp;
    vec[1].iov_len = 0;

    uio.uio_iov        = vec;
    uio.uio_iovcnt     = 1;             /* 2nd entry is hidden */
    uio.uio_seg        = UIOSEG_USER;   /* user space transfer */
    uio.uio_resid      = bp->b_bcount;  /* # bytes to transfer */
    uio.uio_fpflags    = fp->f_flag;    /* for FREAD/FWRITE */
    uio.uio_offset     = (off_t)bp->b_offset;

    /* Set bp's resid to requested transfer size, and adjust its
     * value in the read/write routines as appropriate.
     */
    bp->b_resid = bp->b_bcount;

    /* Indicate to advfs_rdwr() that this call is via AIO */
    ndp = NAMEIDATA();
    saved_nameiop = ndp->ni_nameiop;
    ndp->ni_nameiop |= NI_AIO;

    /* For directIO calls, dispatch into the mainline AdvFS read/write 
     * routine.
     */
    retval = advfs_rdwr( bp->b_vp,  &uio, 
                         (BGETFLAGS(bp) & B_READ) ? UIO_READ : UIO_WRITE,
                         (fp->f_flag & FAPPEND) ? IO_APPEND : 0,
                         fp->f_cred );

    if ( vec[1].iov_len == 0 ) {   /* No asynchronous I/O's were queued */
        bp->b_error = retval;
        bp->b_resid = uio.uio_resid;

        if ( retval == EOK ) {
            /* call the aio completion routine only if we did the IOs
             * in-line (not asynchronously) and there was no error.
             * AIO does the cleanup if we return an error.
             */
            bp->b_iodone(bp);
        } 
    } else {
        /* We actually started an asynchronous IO, so return success to
         * aio_snd2fs() so it doesn't try to cleanup the aio_bp. That
         * must be done in the IO completion path.
         */
        retval = 0;
    }
    ndp->ni_nameiop = saved_nameiop;    /* undo nameidata changes */
    return ( retval );
}


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
#define MB_TST(_a,_b) (uint64_t)_a<((uint64_t)1L<<(_b-1)) ? _b :
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
 * advfs_pathconf - to handle the _PC_FILESIZEBITS and _PC_ACL_EXTENDED, the 
 * 		    rest gets passed to the common vn_pathconf.
 */
int
advfs_pathconf(
    struct vnode *vp,
    int    name,
    int   *resultp,
    struct ucred *cred)
{
    bfAccessT *bfap;

    switch (name) {

    case _PC_FILESIZEBITS:
        if (vp->v_type != VDIR) {
            return(EINVAL);
        }
        /*
         * _PC_FILESIZEBITS is looking for the minimum number of
         * bits needed to represent the maximum size of a file in a
         * signed varable.  ADVFS_MAX_OFFSET is an offset and
         * MIN_BITS_CONSTANT() needs a maximum file size.  In this
         * case it would be ADVFS_MAX_OFFSET+1, since
         * ADVFS_MAX_OFFSET says that you can lseek to it and still
         * write 1 byte.
         *
         * NOTE:  If ADVFS_MAX_OFFSET is 0x7fffffffffffffff, then 
         *        _PC_FILESIZEBITS is 65 (0x7fffffffffffffff+1 =
         *        0x8000000000000000, or 64 bits plus one more bit
         *        for a sign bit, giving us 65).
         *
         *        If we want to return a value that is 64 bits or
         *        less, then we would need to have ADVFS_MAX_OFFSET 
         *        be equal to 0x3fffffffffffffff, or lower.
         */
        *resultp = MIN_BITS_CONSTANT(ADVFS_MAX_OFFSET+1);
        return(0);

    case _PC_LINK_MAX:
        /*
         * See the comments explaining these asserts in the
         * advfs_link() routine above.
         */
        MS_SMP_ASSERT(MAXLINK == 32767);
        MS_SMP_ASSERT(LINK_MAX == 32767);
        MS_SMP_ASSERT(
            sizeof(((struct vattr *)0)->va_nlink)==sizeof(int16_t)
        );
        MS_SMP_ASSERT(
            sizeof(((struct stat *)0)->st_nlink)==sizeof(int16_t)
        );
        MS_SMP_ASSERT(sizeof(nlink_t) == sizeof(int16_t));

        *resultp = MAXLINK;
        return(0);

    case _PC_NAME_MAX:
        if (vp->v_type != VDIR) {
            return(EINVAL);
        }
        *resultp = NAMEMAX;
        return(0);

    case _PC_PATH_MAX:
        if (vp->v_type != VDIR) {
            return(EINVAL);
        }
        *resultp = (MAXPATHLEN -1);
        return(0);

    case _PC_PIPE_BUF:
        if ((vp->v_type != VDIR) && (vp->v_type != VFIFO)) {
            return (EINVAL);
        }
        *resultp = PIPE_BUF;
        return (0);

    case _PC_CHOWN_RESTRICTED:
        if (wisset(priv_global.priv_mask, PRIV_CHOWN-1)) {
            *resultp = -1; /* chown is not restricted */
        } 
        else {
            *resultp = 1; /* it is restricted */
        }
        return(0);
            
    case _PC_SYNC_IO:
        if ((vp->v_type != VREG) && (vp->v_type != VBLK)) {
            *resultp = -1;
            return (EINVAL);
        }
        *resultp = 1; /* Synchronized IO supported for this file */
        return(0);

    case _PC_NO_TRUNC:
        if (vp->v_type != VDIR) {
            return(EINVAL);
        }
        *resultp = 1; 
        return(0);

    case _PC_MAX_CANON:
    case _PC_MAX_INPUT:
        *resultp = TTYHOG;
        return(0);

    case _PC_VDISABLE:
        *resultp = (int)_POSIX_VDISABLE;
        return(0);

    default:
        *resultp = -1;
        return(EINVAL);
    }

    /*
     * Satisfy the compiler.  Should never be reached.
     */
    MS_SMP_ASSERT(FALSE);
    return(0);
}

/*
 * advfs_fid
 *
 * given a vnode pointer, return a file handle.
 */
advfs_fid(
    struct vnode *vp,  /* in     - vnode pointer */
    struct fid **fhp)  /* in out - pointer to file handle to fill in */
{
    bfNodeT *bnp;

    *fhp = (struct fid *)kmem_alloc(sizeof(struct fid));

    bnp = VTOBNP(vp); 
    ADV_ENCODE_FID(*fhp, &bnp->tag);
    
    return (0);
}

/*
 * advfs_prealloc 
 *
 * Allocates storage in a file for a given size starting from the
 * beginning of the file. 
 * This routine does not zero the storage or any cache pages.
 * Cache pages are invalidated in the file before returning.
 *
 * AdvFS does not implement the concept of minimum free file system blocks.
 * Therefore, ignore_minfree flag is ignored.
 *
 * Assumption:
 * The storage is not guaranteed to be contiguous.
 */
int
advfs_prealloc(
    struct vnode *vp,    /* in: vnode pointer */
    off_t size,          /* in: byte size to preallocate */
    int ignore_minfree,  /* in: True/false flag to ignore minimum
                          *     free filesystem blocks check.
                          *     Does not apply to AdvFS. 
                          */
    int reserved)        /* in: Number of filesystem allocation
                          *     units to reserve for filesystem use
                          *     and not allocate towards swap use.
                          */
{
    bfAccessT * bfap;
    domainT * dmnp;
    struct fsContext *contextp;
    struct advfs_pvt_param priv_param;
    ni_nameiop_t save_orig_nameiop;
    struct nameidata *ndp;
    size_t adjusted_dmn_freeblks;
    size_t bytes_left;
    size_t blks_needed;
    off_t next_offset;
    int error;
    struct vnode *saved_vnode;
    bfap = VTOA(vp);
    dmnp = bfap->dmnP;
    contextp = VTOC(vp);

    /* Adjust the domain's free blocks by the given reserved count.
     * The reserved count is the number of file system allocation units
     * to reserve for filesystem use and not for swap use.
     */   
    adjusted_dmn_freeblks = 
      dmnp->freeBlks - (reserved * bfap->bfPageSz * ADVFS_FOBS_PER_DEV_BSIZE);

    /* Calculate the number of DEV_BSIZE disk blocks the caller wants to
     * preallocate.
     */
    blks_needed = howmany(size, DEV_BSIZE);

    /* Verify the domain has enough free DEV_BSIZE disk blocks for the request.
     * This is only a preliminary check as the domain's freeBlks may 
     * change while performing the storage allocation.
     */
    if (adjusted_dmn_freeblks < blks_needed) {
        return (ENOSPC);
    }

    /* Advfs_getpage() requires the NI_RW flag be set to determine
     * caller is not mmapping and to allow file extentions. 
     * Save the original nameidata value to restore later.
     */
    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp);
    save_orig_nameiop = ndp->ni_nameiop;
    ndp->ni_nameiop = NI_RW;
    saved_vnode = ndp->ni_vp;
    ndp->ni_vp = vp;
 
    /* Allocate the storage starting from the beginning of the file 
     * without creating cache pages or zeroing the data by setting
     * the APP_ADD_STG_NOCACHE getpage optimization flag.
     * Also note that the presence of the private parameter pointer
     * tells getpage this is a non-mmap request.
     */
    bytes_left = size;
    next_offset = 0;
    bzero((char *)&priv_param, sizeof(struct advfs_pvt_param));
    priv_param.app_starting_offset = 0;
    priv_param.app_total_bytes = size;
    priv_param.app_flags = APP_ADDSTG_NOCACHE;
    ADVRWL_FILECONTEXT_WRITE_LOCK( contextp );

    if ( (bfap->bfaFlags & BFA_SNAP_CHANGE) ||
            ( (bfap->bfaFirstChildSnap == NULL) && HAS_SNAPSHOT_CHILD( bfap ) ) ) {
        /*
         * We may have truncated the file out and now are preallocating
         * storage for the file.  Call advfs_force_cow_and_unlink to COW any
         * holes that existed before the prealloc.
         */
        error = advfs_force_cow_and_unlink( bfap,
                                next_offset,
                                bytes_left,
                                SF_NO_UNLINK,
                                FtxNilFtxH);
        if (error != EOK) {
            SNAP_STATS( prealloc_failed_force_cow );
            error = EOK;
        }
    }

    error = advfs_getpage(NULL, &bfap->bfVnode, (off_t *)&next_offset,
                          (size_t *)&bytes_left, FCF_DFLT_WRITE,
                          (uintptr_t)&priv_param, 0 );
    if (error != EOK) {
        goto cleanup;
    }

    /* Update the file size only if the new size increases the file size.
     * This preallocate always starts from the beginning of the file.
     */
    if (size > bfap->file_size)
        bfap->file_size = size;

cleanup:
    ADVRWL_FILECONTEXT_UNLOCK(contextp);

    /* Restore the original value */
    ndp->ni_nameiop = save_orig_nameiop;
    ndp->ni_vp = saved_vnode;
    return(error);
}


/*
 * advfs_swapfs_len
 *
 * This routine returns the byte length of contiguous disk blocks
 * in a file given the file offset and length up to the smaller of either the 
 * byte count (buf.b_bcount) or the length of the contiguous blocks.
 * 
 * The swap code uses this to determine the length of its next
 * vop strategy IO request.
 * 
 * Assumptions:
 * There is an assumption that the caller (swap) preallocated the 
 * the file storage before calling this routine. The swap code 
 * expects a non-zero length return value.
 * 
 * This routine and any called routines must not hold any AdvFS locks
 * as the VM component's swap filesystem code will hold a swap lock that
 * is lower than the AdvFS locks in the lock hierarchy.
 */
int
advfs_swapfs_len(
    struct buf *bp)  /*in */
{
    bfAccessT *bfap;
    bsInMemXtntMapT *xtnt_map;
    extent_blk_desc_t *extent_blk_desc;
    statusT error;
    off_t adj_offset;
    size_t max_size;
    size_t diff_size;
    size_t allocUnitSize;
    size_t rem_length;

    bfap = VTOA(bp->b_vp); 
    MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);

    /* Calculate a default minimum return size of the file's allocation
     * unit minus any non-aligned starting offset into the allocation unit.
     */
    allocUnitSize = bfap->bfPageSz * ADVFS_FOB_SZ;
    rem_length = bp->b_offset % allocUnitSize;
    max_size = (rem_length) ? (allocUnitSize - rem_length) : allocUnitSize;

    /* Get the file's allocated storage extent block mappings to check
     * for the number of contiguous file offset blocks (FOB's) starting
     * for the given file offset up to the given byte length.
     * The offset passed into the blkmap routine must be aligned to
     * the allocation unit size. 
     * Also increment the requested length by the difference between the
     * file's disk allocation unit size aligned
     * offset and the caller's offset.
     */
    adj_offset = rounddown(bp->b_offset, allocUnitSize);
    diff_size = bp->b_offset - adj_offset;
    extent_blk_desc = NULL;
    xtnt_map = bfap->xtnts.xtntMap;

    error = advfs_get_blkmap_in_range(bfap,
                                      xtnt_map,
                                      &adj_offset,
                                      bp->b_bcount + diff_size,
                                      &extent_blk_desc,
                                      NULL,
                                      RND_NONE,
                                      EXB_ONLY_STG,
                                      XTNT_WAIT_OK|XTNT_LOCKS_HELD);
    if (error) {
        /* Return size from caller's offset to end of an allocation unit.*/
        return(max_size);
    }
   
    /* Only the first block map entry will be examined. The presumption
     * is that two block map entries will not typically be contiguous on disk.
     */
    if ((extent_blk_desc != NULL) &&
        (extent_blk_desc->ebd_vd_blk != XTNT_TERM)){
        /* If block mapping was not a hole, determine the smaller byte
         * length of either the contiguous disk blocks or input length.
         * Adjust the length by the difference between the given
         * offset and the allocation unit aligned offset determined earlier.
         */
        max_size = min(bp->b_bcount,extent_blk_desc->ebd_byte_cnt - diff_size);
    }
    advfs_free_blkmaps(&extent_blk_desc); 
    MS_SMP_ASSERT(extent_blk_desc == NULL);
    return(max_size);
}

/*
 * advfs_release
 *
 * This routine exists for swap filesystem support.
 * Swap calls this vop to allow the filesystem to cleanup any resources
 * it may need to release before the swapper releases the paging file.
 *
 * AdvFS does not have any resources to release.
 */
int
advfs_release(struct vnode *vp)
{
    return (0);
}


/*
 * advfs_chown
 *
 * chown operation on a file
 * this is not directly a vn op, but is called from advfs_setattr
 *
 * When quotas are compiled in, we must update the quota stats (in
 * quota files) and the file's uid/gid atomically.  This function
 * does update the quota files transactionally, but all other changes
 * are to in-memory structures.  The dir stats are marked as dirty, but
 * the dir stats can't wait to be updated at close time.  The caller
 * must call fs_update stats.
 */

static int
advfs_chown(
    struct vnode *vp,               /* in - vnode of bitfile */
    uid_t uid,                      /* in - uid to change to */
    gid_t gid,                      /* in - gid to change to */
    struct ucred *cred,             /* in - callers credentials */
    ftxHT parentFtxH)               /* in - parent transaction handle */
{
    int error = 0;
    statusT sts;
    struct fsContext *file_context = VTOC(vp);
    struct bfAccess *bfap;

    if (uid == UID_NO_CHANGE) 
        uid = file_context->dir_stats.st_uid;
    else if (uid < 0 || uid >= MAXUID)
        return EINVAL;
    
    if (gid == GID_NO_CHANGE)
        gid = file_context->dir_stats.st_gid;
    else if (gid < 0 || gid >= MAXUID)
        return EINVAL;

    /*
     * If the caller doesn't own the file, is trying to change the
     * owner of the file, then the caller needs to be superuser.
     */
    if ((cred->cr_uid != file_context->dir_stats.st_uid &&
         !suser() ))   {
        return (EPERM);
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

    ADVSMP_FILESTATS_LOCK( &file_context->fsContext_lock );
    file_context->fs_flag |= MOD_CTIME;
    file_context->dirty_stats = TRUE;
    ADVSMP_FILESTATS_UNLOCK( &file_context->fsContext_lock );

    /* Update the base ACL entries with the new owner and group */
    bfap = VTOA( vp );
    sts = advfs_update_base_acls( parentFtxH, bfap, uid, gid,
				  file_context->dir_stats.st_mode );
    if( sts != EOK )
	return sts;

    return (0);
}

/*
 * advfs_chmod
 *
 * chmod operation on a file
 * this is not directly a vn op, but is called from
 * advfs_setattr
 */
int
advfs_chmod(
    struct vnode *vp,                /* in - vnode of bitfile */
    struct fsContext *file_context,  /* in - file context pointer */
    int mode,                        /* in - mode to change to */
    struct ucred *cred)              /* in - callers credentials */
{
    int error;
    statusT sts;
    bfAccessT *bfap;

    /*
     * lock the in-memory stat structure
     */
    ADVSMP_FILESTATS_LOCK( &file_context->fsContext_lock );

    /*
     * must be owner or suser
     */

    if (cred->cr_uid !=
        file_context->dir_stats.st_uid &&
        ( !suser())) {
        ADVSMP_FILESTATS_UNLOCK( &file_context->fsContext_lock );
        return (EPERM);
    }

    file_context->dir_stats.st_mode &= ~07777;
    if (cred->cr_uid) {
        if ((file_context->dir_stats.st_mode &
             S_IFMT) != S_IFDIR) {
            mode &= ~S_ISVTX;
        }
        if
        (!groupmember(file_context->dir_stats.st_gid)) {
            mode &= ~S_ISGID;
        }
    }

    file_context->dir_stats.st_mode |= mode & 07777;
    file_context->dirty_stats = TRUE;
    ADVSMP_FILESTATS_UNLOCK( &file_context->fsContext_lock );

    /* Update the base ACL entries */
    bfap = VTOA( vp );
    sts = advfs_update_base_acls( FtxNilFtxH,
				  bfap,
				  file_context->dir_stats.st_uid,
				  file_context->dir_stats.st_gid,
				  mode );
    if( sts != EOK )
	return sts;

    return (0);
}

/*
 * attach an undelete directory to a directory
 */
statusT
advfs_attach_undel_dir(
    char *dir_path,
    char *undel_dir_path)
{
    statusT sts = EOK;
    struct nameidata *ndp = NAMEIDATA();
    ni_nameiop_t saved_op;    
    struct vnode *vp = NULL;    /* dir_path vnode for volume */
    struct vnode *rvp = NULL;   /* dir_path vnode that has the ref */
    struct vnode *u_vp = NULL;  /* undel_dir_path vnode for volume */
    struct vnode *u_rvp = NULL; /* undel_dir_path vnode that has the ref */
    struct fileSetNode *fsnp, *u_fsnp;
    struct fsContext *cp, *u_cp;
    bfAccessT *bfap, *u_bfap;
    struct bsUndelDir bmt_rec;
    struct pathname path_name;  /* Pathname for volume */

    pn_alloc(&path_name);

    /* lookup the directory vp for dir_path */
    (void)pn_set(&path_name, dir_path);
    saved_op = ndp->ni_nameiop;

    sts = lookuppn( &path_name, FOLLOW_LINK, NULL, &vp);

    ndp->ni_nameiop = saved_op;
    if (sts) {
	pn_free(&path_name);
	return sts;
    }

    rvp = vp;

    if (clu_is_ready()) {
        CLU_GETPFSVP(rvp, &vp, sts)
	if (sts) {
	    RAISE_EXCEPTION( ENOTSUP );
	}
    }

    /* make sure it's advfs */
    if (vp->v_fstype != VADVFS) {
	RAISE_EXCEPTION( ENOSYS );
    }

    /* make sure it's a directory */
    if (vp->v_type != VDIR) {
	RAISE_EXCEPTION( ENOTDIR );
    }

    /*
     * Make sure this is not a read-only fileset.
     */
    if (vp->v_vfsp->vfs_flag & VFS_RDONLY) {
        RAISE_EXCEPTION( EROFS );
    }

    /* lookup the directory vp for undel_dir_path */
    (void)pn_set(&path_name, undel_dir_path);

    saved_op = ndp->ni_nameiop;
    sts = lookuppn( &path_name, FOLLOW_LINK, NULL, &u_vp); 
    ndp->ni_nameiop = saved_op;
    if (sts) {
	RAISE_EXCEPTION( sts );
    }

    u_rvp = u_vp;

    if (clu_is_ready()) {
        CLU_GETPFSVP(u_rvp, &u_vp, sts)
	if (sts) {
	    RAISE_EXCEPTION( ENOTSUP );
	}
    }

    /* make sure it's advfs */
    if (u_vp->v_fstype != VADVFS) {
	RAISE_EXCEPTION( ENOSYS );
    }

    /* make sure it's a directory */
    if (u_vp->v_type != VDIR) {
	RAISE_EXCEPTION( ENOTDIR );
    }

    /* make sure it's not the same directory */
    if (u_vp == vp) {
	RAISE_EXCEPTION( EDUPLICATE_DIRS );
    }

    /* make sure the two directories are in the same file set */
    fsnp = VTOFSN(vp); u_fsnp = VTOFSN(u_vp);
    if ( fsnp->bfSetp != u_fsnp->bfSetp ) {
	RAISE_EXCEPTION( EDIFF_FILE_SETS );
    }

    /*
     * lock the dir's context area, update the in-mem
     * context area (in case someone else has the dir open)
     * and write the BMTR_UNDEL_DIR record
     */

    bfap = VTOA(vp); u_bfap = VTOA(u_vp);
    cp = VTOC(vp);   u_cp = VTOC(u_vp);

    ADVRWL_FILECONTEXT_WRITE_LOCK(cp);
    cp->undel_dir_tag = u_cp->bf_tag;
    bmt_rec.dir_tag = u_cp->bf_tag;

    sts = bmtr_put_rec(
		       bfap,
                       BMTR_FS_UNDEL_DIR,
                       (void *)&bmt_rec,
                       sizeof(struct bsUndelDir),
                       FtxNilFtxH
                       );
    if (sts != EOK) {
        if (sts == ENO_MORE_MCELLS) {
            cp->undel_dir_tag = NilBfTag;
            ADVRWL_FILECONTEXT_UNLOCK(cp);
	    RAISE_EXCEPTION( ENO_MORE_MCELLS );
        }
        ADVFS_SAD1("bmtr_put_rec error in advfs_attach_undel_dir", sts);
    }

    ADVRWL_FILECONTEXT_UNLOCK(cp);

 HANDLE_EXCEPTION:

    pn_free(&path_name);

    if ( rvp ) VN_RELE( rvp );
    if ( u_rvp ) VN_RELE( u_rvp );

    return (sts);
}

/*
 * detach an undelete directory from a directory
 */
statusT
advfs_detach_undel_dir(
    char *dir_path,
    ftxIdT xid)
{
    statusT sts;
    struct nameidata *ndp = NAMEIDATA();
    ni_nameiop_t saved_op;
    struct vnode *vp = NULL;    /* vnode for volume */
    struct vnode *rvp = NULL;   /* vnode that has the ref */
    struct fsContext *cp;
    bfAccessT *bfap;
    struct bsUndelDir bmt_rec;
    struct pathname path_name;  /* Pathname for volume */

    pn_alloc(&path_name);

    /* lookup the directory vp */
    (void)pn_set(&path_name, dir_path);

    saved_op = ndp->ni_nameiop;
    sts = lookuppn( &path_name, FOLLOW_LINK, NULL, &vp); 
    ndp->ni_nameiop = saved_op; /* setting outside of vfs, must be restored */
    pn_free(&path_name);
    if (sts) {
	return sts;
    }

    rvp = vp;

    if (clu_is_ready()) {
        CLU_GETPFSVP(rvp, &vp, sts)
	if (sts) {
	    RAISE_EXCEPTION( ENOTSUP );
	}
    }

    /* make sure it's advfs */
    if (vp->v_fstype != VADVFS) {
	RAISE_EXCEPTION( ENOSYS );
    }

    /* make sure it's a directory */
    if (vp->v_type != VDIR) {
	RAISE_EXCEPTION( ENOTDIR );
    }

    /*
     * Make sure this is not a read-only fileset.
     */
    if (vp->v_vfsp->vfs_flag & VFS_RDONLY) {
        RAISE_EXCEPTION( EROFS );
    }

    /*
     * lock the dir's context area, update the in-mem
     * context area (in case someone else has the dir open)
     * and re-write the BMTR_UNDEL_DIR record
     */
    bfap = VTOA(vp);
    cp = VTOC(vp);

    ADVRWL_FILECONTEXT_WRITE_LOCK( cp );

    if (BS_BFTAG_EQL( cp->undel_dir_tag, NilBfTag )) {
        ADVRWL_FILECONTEXT_UNLOCK( cp );
        RAISE_EXCEPTION( E_NO_UNDEL_DIR );
    }

    cp->undel_dir_tag = NilBfTag;
    bmt_rec.dir_tag = NilBfTag;
    sts = bmtr_put_rec_xid(
                           bfap,
                           BMTR_FS_UNDEL_DIR,
                           (void *)&bmt_rec,
                           sizeof(struct bsUndelDir),
                           FtxNilFtxH,
                           xid
                           );
    if (sts != EOK) {
        ADVFS_SAD1("bmtr_put_rec error in advfs_detach_undel_dir", sts);
    }
    ADVRWL_FILECONTEXT_UNLOCK( cp );

HANDLE_EXCEPTION:

    if ( rvp ) VN_RELE( rvp );

    return sts;
}

/*
 * get a directory's undelete dir tag
 */
statusT
get_undel_dir(
    char *dir_path,
    bfTagT *undelDirTag)
{
    statusT sts;
    struct nameidata *ndp = NAMEIDATA();
    ni_nameiop_t saved_op;
    struct fsContext *cp;
    struct vnode *vp = NULL;    /* vnode for volume */
    struct vnode *rvp = NULL;   /* vnode that has the ref */
    struct pathname path_name;  /* Pathname for volume */

    pn_alloc(&path_name);

    /* lookup the directory vp */
    (void)pn_set(&path_name, dir_path);
    saved_op = ndp->ni_nameiop;
    sts = lookuppn( &path_name, FOLLOW_LINK, NULL, &vp); 
    ndp->ni_nameiop = saved_op;
    pn_free(&path_name);
    if (sts) {
	return sts;
    }

    rvp = vp;

    if (clu_is_ready()) {
        CLU_GETPFSVP(rvp, &vp, sts)
	if (sts) {
	    RAISE_EXCEPTION( ENOTSUP );
	}
    }

    /* make sure it's advfs */
    if (vp->v_fstype != VADVFS) {
        VN_RELE(rvp);
        return(ENOSYS);
    }

    /* make sure it's a directory */
    if (vp->v_type != VDIR) {
        VN_RELE(rvp);
        return(ENOTDIR);
    }

    /* return the directory's undelete dir tag */
    cp = VTOC(vp);
    *undelDirTag = cp->undel_dir_tag;

 HANDLE_EXCEPTION:

    if ( rvp ) VN_RELE( rvp );

    return( sts );
}

/*
 * Given a tag and a name of a file or directory in the same fileset,
 * return the name of the file and the tag of the parent directory.
 * Returns ENO_NAME if could not find a matching name.
 */
statusT
advfs_get_name(
    char *name,         /* In - Name of file/directory in same fileset */
    bfTagT bf_tag,      /* In - Tag of file */
    char *buffer,       /* In/Out - Buffer addr to store file name string */
    bfTagT *parent_tag) /* Out - Tag of parent diirectory */
{
    statusT sts = ENO_NAME;     /* Let's be pessimistic */
    struct nameidata *ndp = NAMEIDATA();
    ni_nameiop_t saved_op;
    struct vnode *vp = NULL,    /* vnode for "name" */
                 *name_vp =NULL,/* vnode for "name" that has the ref */
                 *bf_tag_vp = NULL,/* vnode for "bf_tag" */
                 *dir_vp = NULL;/* vnode for "bf_tag"'s directory */
    bfAccessT *bf_tag_bfap=NULL,/* Access structure for "bf_tag" */
              *dir_bfap=NULL;   /* Access structure for directory */
    struct fileSetNode *fsnp;   /* Fileset pointer */
    struct pathname path_name;  /* Pathname structure for "name" lookup */
    int found_match = FALSE,    /* TRUE if we found a dir entry for bf_tag */
        current_dir_page;       /* Current page in the scanned directory */
    bs_meta_page_t dir_pages;   /* Number of pages in the directory */
    bf_fob_t dir_page_size;     /* Directory page size in 1k fobs */
    char root_name[2] = "/",    /* Name for root directory */
         *dir_buffer,           /* Buffer to hold directory page */
         *dir_page_end;         /* Pointer to end of directory page in buf */
    bfPageRefHT page_ref;       /* Directory page reference handle */
    fs_dir_entry *dir_p;        /* Pointer to current directory entry */
    bfTagT dir_tag,             /* Tag of "bf_tag"'s directory */
           parent_dir_tag,      /* Tag of "bf_tag"'s parent directory */
           root_tag;            /* Tag for filesystem root directory */
    int close_dir = FALSE;      /* TRUE if we need to close the directory */

    /*
     * Lookup/open the file/directory passed in.
     */
    path_name.pn_buf = name;
    path_name.pn_path = name;
    path_name.pn_pathlen = strlen(name);

    saved_op = ndp->ni_nameiop;

    sts = lookuppn( &path_name, FOLLOW_LINK, NULL, &vp);

    ndp->ni_nameiop = saved_op;
    if (sts) {
	RAISE_EXCEPTION(sts);
    }
    name_vp = vp;

    /*
     * If we're in a cluster, name_vp is the CFS vnode.  Get the AdvFS vnode.
     */
    if (clu_is_ready()) {
        CLU_GETPFSVP(name_vp, &vp, sts)
	if (sts) {
	    RAISE_EXCEPTION(ENOTSUP);
	}
    }

    /* 
     * Make sure it's AdvFS.
     */ 
    if (vp->v_fstype != VADVFS) {
	RAISE_EXCEPTION(ENOSYS);
    }

    /*
     * If the tag passed in is the root tag, this is easy.
     */
    fsnp = VTOFSN(vp);
    root_tag = fsnp->rootTag;
    if (BS_BFTAG_EQL(bf_tag, root_tag)) {
        strncpy(buffer, root_name, NAMEMAX);
        *parent_tag = root_tag;
        RAISE_EXCEPTION(EOK);
    }

    /*
     * Open the file identified by bf_tag.  
     */
    if (BS_BFTAG_REG(bf_tag) && (BS_BFTAG_SEQ(bf_tag) == 0)) {  
        /* Try to find real seq number */
        bfMCIdT bfMCId;

        if (tagdir_lookup(fsnp->bfSetp, &bf_tag, &bfMCId)) {
            RAISE_EXCEPTION(ENO_SUCH_TAG);
        }
    }

    if (bf_get(bf_tag, 0, DONT_UNLOCK, fsnp, &bf_tag_vp, NULL, NULL)) {
        RAISE_EXCEPTION(ENO_SUCH_TAG);
    }
    bf_tag_bfap = VTOA(bf_tag_vp);

    /*
     * If this is a metadata file that's not a directory (an index 
     * file, for example), or a symlink, and not the user quota file, 
     * return ENO_SUCH_TAG.
     * That's because such files don't have stat structures which
     * would tell us the parent directory's tag number.
     */
    if (bf_tag_bfap->dataSafety == BFD_METADATA && 
        bf_tag_vp->v_type != VDIR &&
        bf_tag_vp->v_type != VLNK &&
        bf_tag_bfap != fsnp->qi[USRQUOTA].qiAccessp) { 
        RAISE_EXCEPTION(ENO_SUCH_TAG);
    }

    /* 
     * Grab the directory's tag from the stats in the access structure
     * and close the file.
     */
    dir_tag = bf_tag_bfap->bfFsContext.dir_stats.dir_tag;
    VN_RELE(bf_tag_vp);
    bf_tag_bfap = NULL;
    bf_tag_vp = NULL;

    /*
     * Open the directory, unless it's the root directory, which is already 
     * opened.
     */
    if (!(BS_BFTAG_EQL(dir_tag, root_tag))) {
        if (bs_access(&dir_bfap, dir_tag, fsnp->bfSetp, FtxNilFtxH, 
                      BF_OP_INTERNAL, &dir_vp)) {
            RAISE_EXCEPTION(ENO_NAME);
        }
        close_dir = TRUE;

        /*
         * Make sure it's still a directory, since we're not locking anything.
         */
        if (dir_vp->v_type != VDIR) {
            RAISE_EXCEPTION(ENO_NAME);
        }
    } else {
        dir_bfap = fsnp->rootAccessp;
    }

    /*
     * Lock the directory so it doesn't change while we scan it.
     */
    ADVRWL_FILECONTEXT_READ_LOCK(&dir_bfap->bfFsContext);

    /*
     * Scan the directory pages, looking for an entry which matches bf_tag.
     */
    dir_page_size = dir_bfap->bfPageSz;
    dir_pages = howmany(dir_bfap->bfFsContext.dir_stats.st_size,
                        dir_page_size * ADVFS_FOB_SZ);

    for (current_dir_page = 0; 
         current_dir_page < dir_pages && (!found_match); 
         current_dir_page++) {

        sts = bs_refpg(
                       &page_ref,
                       (void *)&dir_buffer,
                       dir_bfap,
                       current_dir_page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            sts = EIO;
            break;
        }
        dir_page_end = (char *)(dir_buffer + (dir_page_size * ADVFS_FOB_SZ));

        for (dir_p = (fs_dir_entry *)dir_buffer;
             ((char *)dir_p < dir_page_end) && (!found_match);
             dir_p = (fs_dir_entry *)((char *)dir_p + dir_p->fs_dir_size)) {

            /*
             * If we find a corrupt directory entry, take note of it but
             * optimistically move on to the next page of the directory,
             * if there is one, continuing our search, nay, our quest,
             * for the desired file name.
             */
            if (FS_DIR_SIZE_OK(dir_p->fs_dir_size, 
                                ((char *)dir_p - (char *)dir_buffer))) {
                fs_dir_size_notice(dir_bfap, dir_p->fs_dir_size, 
                                   current_dir_page, 
                                   ((char *)dir_p - (char *)dir_buffer));
                break;
            }

            /*
             * If the current directory entry's tag matches bf_tag,
             * copy the name in the directory entry into the caller's
             * buffer and grab the parent directory's tag out of the
             * stats stored in the directory's access structure.
             */
            if (dir_p->fs_dir_bs_tag_num == bf_tag.tag_num) {
                strncpy(buffer, dir_p->fs_dir_name_string, NAMEMAX);
                *parent_tag = dir_tag;
                found_match = TRUE;
                break;
            }
        }

        sts = bs_derefpg(
                         page_ref,
                         BS_NIL
                         );
    }

    ADVRWL_FILECONTEXT_UNLOCK(&dir_bfap->bfFsContext);

    sts = found_match ? EOK : ENO_NAME;

HANDLE_EXCEPTION:

    /*
     * Close the file we opened by tag number.
     */
    if (bf_tag_vp) {
        VN_RELE(bf_tag_vp);
    }

    /*
     * Close the directory we scanned, if it was not the root directory.
     */
    if (close_dir) {
       bs_close(dir_bfap, MSFS_CLOSE_NONE);
    }

    /* 
     * Close the file/directory passed in.
     */
    if (name_vp) {
        VN_RELE(name_vp);
    }

    return(sts);
}

/*
 * advfs_lockctl
 *
 * File locking - uses VFS layers local_lockctl
 */
int
advfs_lockctl(
    struct vnode *vp,
    struct flock *flock,
    int    cmd,
    struct ucred *cred,
    struct file  *fp,
    off_t  LB,
    off_t  UB)
{
    int ret;

    FILESETSTAT( vp, advfs_lockctl );

    ret = local_lockctl(vp, flock, cmd, cred, fp, LB, UB);

    return(ret);
}


/*  
 * advfs_lockf
 *  
 * File locking - uses VFS ;ayers local_lockctl
 */ 
int
advfs_lockf(
    struct vnode *vp,
    int    flag,
    off_t  len,
    struct ucred *cred,
    struct file  *fp,
    off_t  LB,
    off_t  UB)
{
    int ret;
    
    FILESETSTAT( vp, advfs_lockctl );

    ret = local_lockf(vp, flag, len, cred, fp, LB, UB);

    return(ret);
}

int
advfs_shrlock(
    struct vnode *vp,
    int cmd,
    struct shrlock *shr,
    int flag)
{               
    return(vfs_shrlock(vp, cmd, shr, flag));
}

/*
 * advfs_realvp
 *
 *	Returns the real vnode being shadowed by the vnode passed in.  This
 *	is a no-op for AdvFS, where AdvFS vnodes do not shadow other vnodes
 *	(not to be confused with an AdvFS shadow extents, shadow access
 *	structures, and rbmt shadows).
 */
int
advfs_realvp(
    struct vnode *vp,           /* in  - pointer to "false" vp */
    struct vnode **realvpp)     /* out - return the "real" vp */
{
    *realvpp = vp;
    return 0;
}

/* end msfs_vnops.c */
