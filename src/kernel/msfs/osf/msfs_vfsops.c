/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993                      *
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
 *      AdvFS - Advanced File System
 *
 * Abstract:
 *
 *      This module defines the Virtual File System fs interfaces for
 *      the Advanced File System.  All possible VFS fs operations are
 *      defined in this module (whether AdvFS implements them or 
 *      not).
 *
 * Date:
 *
 *      Mon Aug 19 15:39:06 1991
 *
 */
/*
 * HISTORY
 * 
 * 
 * 
 */

#pragma ident "@(#)$RCSfile: msfs_vfsops.c,v $ $Revision: 1.1.827.17 $ (DEC) $Date: 2006/08/08 18:25:09 $"

#include <sys/lock_probe.h>
#include <kern/lock.h>
#include <kern/rad.h>
#include <sys/param.h>
#include <sys/errno.h>
#include <sys/systm.h>
#include <sys/time.h>
#include <sys/user.h>
#include <sys/vnode.h>
#include <sys/specdev.h>
#include <sys/mount.h>
#include <sys/stat.h>
#include <sys/mode.h>
#include <sys/malloc.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_osf.h>
#include <msfs/fs_dir.h>
#include <msfs/fs_dir_routines.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/fs_quota.h>
#include <msfs/ms_assert.h>
#include <msfs/msfs_syscalls.h>
#include <msfs/bs_index.h>
#include <sys/proc.h>
#include <sys/fcntl.h>
#include <sys/buf.h>
#include <sys/ioctl.h>
#include <sys/disklabel.h>
#include <sys/clu.h>
#include <msfs/bs_params.h>
#include <msfs/advfs_evm.h>
#include <sys/syslog_pri.h>
#include <msfs/bs_freeze.h>
#include <msfs/bs_domain.h>

#define ADVFS_MODULE     MSFS_VFSOPS
#define BOGUS_ROOTNAME  "generic#root"
#define BOGUS_BOOTNAME  "not_generic#boot"
#define BOGUS_GLROOTNAME  "cluster_root#root"

/* Relevant ADVFS flags */
#define ADVFS_MOUNT_FLAGS (M_FMOUNT | M_DUAL | M_FAILOVER | M_LOCAL_ROOT | M_GLOBAL_ROOT | M_GLROOT_OTHER)

/*
 * Global names of root/boot mount points and default mount-from name.
 */
extern       char *rootmpname;
extern const char  default_rootname[];  /* "root_device" */
extern       char *bootmpname;
extern const char  default_bootname[];  /* "local_root"  */

extern vm_offset_t     vm_kern_zero_page;    /* A page full of zeroes      */
extern pmap_t          kernel_pmap;          /* pointer to the kernel pmap */

/*
 * msfs vfs operations.
 */
       int  msfs_mount       (struct mount *mp, char *path, caddr_t data, struct nameidata *ndp);
       int  msfs_start       (struct mount *mp, int flags);
       int  msfs_unmount     (struct mount *mp, int mntflags);
       int  msfs_root        (struct mount *mp, struct vnode **vpp);
       int  advfs_quotactl   (struct mount *mp, int cmds, uid_t uid, caddr_t arg);
       int  msfs_statfs      (struct mount *mp);
       int  msfs_sync        (struct mount *mp, int waitfor);
       int  msfs_fhtovp      (struct mount *mp, struct fid *fhp, struct vnode **vpp);
       int  msfs_vptofh      (struct vnode *vp, struct fid *fhp);
       int  msfs_init        (void);
       int  msfs_mountroot   (register struct mount *mp, struct vnode **vpp);
extern int  msfs_noop        ();
       int  msfs_smoothsync  (struct mount *mp, u_int age, u_int smsync_flag);
       int  advfs_freezefs   (struct mount *mp, u_long flags, int timeout);

static int  advfs_mountfs    (struct mount *mp);
static int  chk_unique_fsdev (dev_t curId, int *newId, bfSetT *bfSetp, int failover);
static int  chk_set_fsdev    (struct mount *mp, int fsId, int check_flag);
static void msfs_sync_mmap   (struct mount *mp);
static int  msfs_sync_todr   (struct mount *mp, int waitfor);

/*
 * msfs_vfsops
 *
 * Defines function pointers to AdvFS's VFS fs operations.
 */

struct vfsops msfs_vfsops = {
    msfs_mount,
    msfs_start,
    msfs_unmount,
    msfs_root,
    advfs_quotactl,
    msfs_statfs,
    msfs_sync,
    msfs_fhtovp,
    msfs_vptofh,
    msfs_init,
    msfs_mountroot,
    msfs_noop,
    msfs_smoothsync,
};

struct fileSetNode  *FilesetHead = NULL;
struct vnode       **GlobalRootVpp;

int MountWriteDelay = 0;         /* TRUE if we need to delay writes for mount */
int Advfs_enable_dio_mount = 0;  /* Set to TRUE if mounting of a fileset for
                                  * directIO via "-o directIO" is desired.    */
vm_offset_t virtualZeroRangeP = NULL; /* wc.advfs.014.Zeronoallocbufpg */
extern void advfs_range_init(void);

/*
 * Global mutex and lock to serialize access to fileset (fileSetNode) list.
 */

       lock_data_t  FilesetLock;
struct lockinfo    *ADVFilesetLock_lockinfo;
struct lockinfo    *ADVquotaInfoT_qiLock;

decl_simple_lock_info(,ADVfilesetMutex_lockinfo )
decl_simple_lock_info(,msfs_lockmgrmutex_lockinfo )

/*
 *  msfs_mountroot - AdvFS mount root routine
 *
 */

int
msfs_mountroot(mp, vpp)
    register struct mount *mp;
    struct vnode **vpp;
{
    extern  dev_t   rootdev;
    extern  dev_t   global_rootdev[];
    extern  uint32T global_rootdev_count;
    
    struct  nstatfs      *sbp;
            
            bfAccessT    *root_accessp;
            int           fs_time;
            statusT       sts;
            int           error, count;
            int           now_time;
            bsBfSetAttrT  fsAttr_rec;
            int           fsId = 0, newId = 1, setNewId = 0;
            bfSetT       *bfSetp;
            u_long        flag = mp->m_flag & ADVFS_MOUNT_FLAGS;

    
    MountWriteDelay = 0;
    mp->m_op = &msfs_vfsops;
    /*
     * For CFS global root failover, rootfs is the CFS mount point.
     * mp passed in is for the physical lfs mount point.
     * If mp->m_flag|M_RDONLY is not set, then need to do update.
     */
    if (flag & M_FAILOVER) {
        if (!clu_is_ready()) {
            error = EBUSY;
            goto out;
        }
        if (rootdev == NODEV) {
            panic("msfs_mountroot: rootdev not set for CFS failover");
        }
    } else {
        mp->m_flag |= M_RDONLY; /* Force update mount */
    }
    mp->m_exroot = 0;
    mp->m_uid = 0;
    mp->m_mounth = (struct vnode *)0;

#   ifdef  SER_COMPAT || RT_PREEMPT
        mp->m_funnel = FUNNEL_NULL;
#   endif

    if (flag & M_GLOBAL_ROOT) {
        /* Mounting cluster global root - must get root dev array
         * from global_rootdev and initialize global_rootdev_count
         * vnodes.
         */
        GlobalRootVpp = (struct vnode **)ms_malloc(global_rootdev_count *
                                             sizeof(struct vnode *));
	/* note ms_malloc never fails, it doesn't return until success*/

        for (count = 0; count < global_rootdev_count; count++) {
            if (bdevvp(global_rootdev[count], &(GlobalRootVpp[count])))
                panic("msfs_mountroot: can't setup bdevvp of global rootdev");
        }

        /* Keep caller happy */
        *vpp = GlobalRootVpp[0];
    }
    else {
        mp->m_flag |= M_LOCAL_ROOT;
        flag |= M_LOCAL_ROOT;
        if (bdevvp(rootdev, vpp)) {
            panic("msfs_mountroot: can't setup bdevvp of rootdev");
        }
    }

    UNMOUNT_LOCK_INIT(mp);
    MOUNT_VLIST_LOCK_INIT(mp);
    MOUNT_LOCK_INIT(mp);

    /*
     * Fill f_mntonname and f_mntfromname for advfs_mountfs().  We need to
     * use the bogus root name because advfs_mountfs() calls toke_it() on
     * the f_mntfromname, even though it's ignored subsequently.
     */
    MALLOC_VAR(mp->m_stat.f_mntonname, char *,
        strlen(rootmpname) + 1, M_PATHNAME, M_WAITOK);
    if (!mp->m_stat.f_mntonname) {
        error = ENOMEM;
        goto out;
    }
    strcpy(mp->m_stat.f_mntonname, rootmpname);
    if (clu_is_ready()) {
        if (flag & M_LOCAL_ROOT) {
                MALLOC(mp->m_stat.f_mntfromname, char *, sizeof(BOGUS_BOOTNAME),
                   M_PATHNAME, M_WAITOK);
                strcpy(mp->m_stat.f_mntfromname, BOGUS_BOOTNAME);
        } else {
                MALLOC(mp->m_stat.f_mntfromname, char *, sizeof(BOGUS_GLROOTNAME),
                   M_PATHNAME, M_WAITOK);
                strcpy(mp->m_stat.f_mntfromname, BOGUS_GLROOTNAME);
        }
    }
    else {
        MALLOC(mp->m_stat.f_mntfromname, char *, sizeof(BOGUS_ROOTNAME),
               M_PATHNAME, M_WAITOK);
        strcpy(mp->m_stat.f_mntfromname, BOGUS_ROOTNAME);
    }

    error = advfs_mountfs(mp);
    if (flag & M_GLOBAL_ROOT) {
        ms_free(GlobalRootVpp);
    }
    FREE(mp->m_stat.f_mntonname, M_PATHNAME);
    FREE(mp->m_stat.f_mntfromname, M_PATHNAME);
    if (error)
        goto out;

    mp->m_nxt = mp;
    mp->m_prev = mp;
    mp->m_vnodecovered = (struct vnode *)0;
    /*
     * Now fill in f_mntonname and f_mntfromname with "/" and "root_device"
     * respectively.
     */
    MALLOC_VAR(mp->m_stat.f_mntonname, char *, strlen(rootmpname) + 1,
               M_PATHNAME, M_WAITOK);
    strcpy(mp->m_stat.f_mntonname, rootmpname);
    MALLOC_VAR(mp->m_stat.f_mntfromname, char *,
               strlen((char *) default_rootname) + 1, M_PATHNAME, M_WAITOK);
    if (!mp->m_stat.f_mntfromname) {
        FREE(mp->m_stat.f_mntonname, M_PATHNAME);
        error = ENOMEM;
        goto out;
    }
    strcpy(mp->m_stat.f_mntfromname, (char *) default_rootname);
    /*
     * CFS failover, rootfs already setup.  Skip the time update (since
     * in the case of failover, the time is already setup cluster-wide).
     */
    if ( !(flag & M_FAILOVER)) {
        rootfs = mp;
    
        root_accessp = GETROOTACCESS( mp );
        sts = bmtr_get_rec(root_accessp, BMTR_FS_TIME, &fs_time,
                           sizeof(fs_time));
        if (sts == EOK) {
            inittodr(fs_time);
            /* Update last modify time for initodr/resettodr, msfs_sync
             * typically does this, but we'll do it if we can mount
             * successfully too.
             */
            now_time = time.tv_sec;
            sts = bmtr_put_rec(root_accessp, BMTR_FS_TIME,
                &now_time, sizeof(now_time),
                FtxNilFtxH);
            if (sts != EOK) {
                /* likely i/o error; ignore it; update not critical */
                ms_uprintf("msfs_mountroot: bmtr_put_rec warning %s\n",
                BSERRMSG( sts ) );
            }
        }
    }

    /* Creates a unique fsdev if one has not been assigned to the fileset. */

    bfSetp = GETBFSETP(mp);

    sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                            BSR_BFS_ATTR,
                            &fsAttr_rec,
                            sizeof(bsBfSetAttrT),
                            BMTR_NO_LOCK
                            );
    /*
     * Save the attribute's file set device. If the attribute record was
     * not retrieved, then create a potentially unique file set dev.
     */

    fsId = (sts == EOK) ? fsAttr_rec.fsDev :  0 - (int)time.tv_usec;

    /*
     * If this file set does not have an assigned dev OR current dev is not
     * unique, assign newId to file set dev.
     * newId is guaranteed unique across system.
     */

    /*
     * If this is the failover case:
     *    If the disk value is good, we return it (fsAttr_rec.fsDev).
     *    Otherwise we return newId, below.
     */

    setNewId = chk_unique_fsdev(fsId, &newId, bfSetp, flag & M_FAILOVER ? 1 : 0);

    bfSetp->dev =
            (!(fsAttr_rec.fsDev) || setNewId) ? newId : (dev_t)fsAttr_rec.fsDev;

    sbp = &mp->m_stat;
    sbp->f_fsid.val[0] = bfSetp->dev;


    error = 0;
out:
    return (error);
}


/*
 *  msfs_mount - AdvFS mount routine
 *
 */

static DidInit = 0;

msfs_mount( struct mount *mp,           /* in */
            char *path,                 /* in */
            caddr_t data,               /* in */
            struct nameidata *ndp )     /* in */
{
    extern struct vnodeops msfs_vnodeops;
    
           struct advfs_args args;
           struct mount *pmp;
    
           unsigned    size;
           int         error;
           int         fsId=0;
           int         chk_fsdev_flag=FALSE;
           bfDmnDescT *dmntbl = NULL;
           bfAccessT  *root_accessp;
           char       *setName = NULL;
           char       *dmnName = NULL;
           char       *tmp = NULL;
           statusT     sts;
           int         now_time;
           char       *tempstring = NULL;
           advfs_ev   *advfs_event;
           int         stopFreezefs = 0;

#   ifdef notdef
        if (DidInit == 0) {
            mega_presto_start("/dev/pr0", mp->m_flag & (M_LOCAL_ROOT | M_GLOBAL_ROOT));
        }
#   endif

    DidInit = 1;

    /*
     * Process export requests
     */
    error = copyin( data, (caddr_t)&args, sizeof( struct advfs_args ) );
    if (error != 0) {
        goto cleanup;
    }

    MOUNT_LOCK(mp);
    if ((args.exflags & M_EXPORTED) || (mp->m_flag & M_EXPORTED)) {
        if (args.exflags & M_EXPORTED) {
            mp->m_flag |= M_EXPORTED;
        } else {
            mp->m_flag &= ~M_EXPORTED;
        }
        if (args.exflags & M_EXRDONLY) {
            mp->m_flag |= M_EXRDONLY;
        } else {
            mp->m_flag &= ~M_EXRDONLY;
            mp->m_exroot = args.exroot;
        }
    }

    /* For V5.0 ship, make the -o directIO option hidden and
     * dependent on the Advfs_enable_dio_mount being non-null.
     */
    if ( (mp->m_flag & M_DIRECTIO) && !Advfs_enable_dio_mount ) {
        mp->m_flag &= ~M_DIRECTIO;
    }

#   ifdef  SER_COMPAT || RT_PREEMPT
        mp->m_funnel = FUNNEL_NULL;
#   endif

    MOUNT_UNLOCK(mp);

    /*
     * We allow updating the f_mntfromname and the device
     * names. If this fails, the mount point will be read-write.
     */
    if (mp->m_flag & M_UPDATE) {
        int vdi, vdi2, rdev_match, vdCnt;
        struct vd *vdp;
        vdDescT *new_vdp;
        domainT *dmnP;

        /* We want to update file set dev on disk without verification. */
        MS_SMP_ASSERT(fsId == 0);
        MS_SMP_ASSERT(chk_fsdev_flag == FALSE);

        dmnP = GETDOMAINP(mp);

        /* Sanity check. Don't allow update if the domain passed in
         * does not match the one in the mount stat structure.
         * EXCEPTION:  When booting the machine the root filesystem may have a
         * temporary mntfromname.  The temp name differs depending
         * if the machine is in a cluster. Therefore, allow the change
         * of the mntfromname with this exception. 
         * ASSUMPTION: Only allow differences in mntfromname and args.spec
         * to go through if the mntfromname is either default_rootname ("root_device") 
         * OR default_bootname ("local_root").
         */
        if ( (strcmp(mp->m_stat.f_mntfromname, default_rootname) && 
                        strcmp(mp->m_stat.f_mntfromname, default_bootname)) &&
                        strcmp(mp->m_stat.f_mntfromname, args.fspec) ) {  
                error = EINVAL;
                goto cleanup;
        }
        
        /*
         *  Don't allow mount update if domain is frozen.
         *  Take the dmnFreezeMutex to check status and to
         *  increment the ref count to prevent a new freeze
         *  from starting if we begin the mount -u.
         */
        mutex_lock(&dmnP->dmnFreezeMutex);
        if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            error = EBUSY;
            goto cleanup;
        } else {
           dmnP->dmnFreezeRefCnt++;
           stopFreezefs++;
        }
        mutex_unlock(&dmnP->dmnFreezeMutex);

        /*
         * If a fileset was mounted in read-only mode because
         * a volume appears to be mislabeled, do not allow the log to be
         * flushed or the mount update to succeed.  Instead, force the
         * user to correct this problem.
         */
        if (((struct fileSetNode *) (mp->m_info))->fsFlags & FS_FORCE_RDONLY) {
            error = EROFS;
            goto cleanup;
        }

        /*
         * If this is the root filesystem then you need to update the
         * volume name, domain name, etc.  This code does not always
         * work for non root multi-volume domains.   
         */

        /*
         * here we have stacking issue: mp may not == rootfs, but it can be
         * root, actually it can be global root or local root, should check for
         * both.
         */

        if (clu_is_ready()) {
            extern struct mount *bootfs;
            (void)CC_GETPFSMP(rootfs, &pmp);
            if (mp != pmp)
                (void)CC_GETPFSMP(bootfs, &pmp);
        } else {
            pmp = rootfs;
        }

        if (mp == pmp) {
	    /* ms_malloc never fails, it doesn't return until success*/
            dmntbl = (bfDmnDescT *)ms_malloc( sizeof( bfDmnDescT ) );
            bzero( (char *)dmntbl, sizeof( bfDmnDescT ) );

            dmnName = ms_malloc( MAX_MNT_PATHLEN );

            tmp = ms_malloc( MAX_MNT_PATHLEN );

            error = copyinstr(args.fspec, tmp, MAX_MNT_PATHLEN, (int *)&size);
            if (error != 0) {
                goto cleanup;
            }
            /* Check to see if "root_device" or "local_root" is the name,
             * if so, we need to let it mount R/W. The mount command may have
             * found a missing entry for "/" or the fstab was missing.
             */
            if (((strlen(tmp) == strlen((char *) default_rootname)) &&
                 !strcmp(tmp, (char *) default_rootname) &&
                 (strlen(mp->m_stat.f_mntonname) == strlen(rootmpname)) &&
                 !strcmp(mp->m_stat.f_mntonname, rootmpname)) ||
                (clu_is_ready() &&
                 (strlen(tmp) == strlen((char *) default_bootname)) &&
                 !strcmp(tmp, (char *) default_bootname) &&
                 (strlen(mp->m_stat.f_mntonname) == strlen(bootmpname)) &&
                 !strcmp(mp->m_stat.f_mntonname, bootmpname))) {
                ms_uprintf("msfs_mount: mount device \"%s\" is mounting RW\n",
                           tmp);
                goto cleanup;
            }

            bzero( tmp + size, MAX_MNT_PATHLEN - size );
            /* Extract the set name and the domain name */

            setName = toke_it( tmp, '#', dmnName );

            /*
             * Get the list of disks in this domain into a table.
             */
            sts = get_domain_disks( dmnName, 0, dmntbl );
            if ( sts != EOK ) {
                int special;

                ms_uprintf("msfs_mount: error getting disk list for domain \"%s\"\n", dmnName);
                if ((strlen(mp->m_stat.f_mntonname) == strlen(rootmpname)) &&
                    !strcmp(mp->m_stat.f_mntonname, rootmpname))
                    special = 1;
                else if (clu_is_ready() &&
                         (strlen(mp->m_stat.f_mntonname) ==
                          strlen(bootmpname)) &&
                         !strcmp(mp->m_stat.f_mntonname, bootmpname))
                    special = 2;
                else
                    special = 0;
                if (special) {
                    char *mfname;

                    mfname = (special == 1) ?
                        (char *) default_rootname :
                        (char *) default_bootname;
                    FREE(mp->m_stat.f_mntfromname, M_PATHNAME);
                    MALLOC_VAR(mp->m_stat.f_mntfromname, char *,
                               strlen(mfname)+1,
                               M_PATHNAME,
                               M_WAITOK);
                    strcpy(mp->m_stat.f_mntfromname, mfname);
                    ms_uprintf("msfs_mount: Setting %s device name to \"%s\" RW\n",
                               (special == 1) ? "root" : "boot",
                               mp->m_stat.f_mntfromname);
                }
                else
                    error = sts;
                goto cleanup;
            }

            /*
             * Verify that the number of volumes specified in /etc/fdmns
             * is the same as on-disk volume count. If so, make sure
             * the service classes have been setup ok. If not, return an 
             * error.
             */

            if (dmntbl->vdCount != dmnP->vdCnt) {
                sts = bs_fix_root_dmn(dmntbl, dmnP, dmnName);
                if (sts != EOK) {
                    if (clu_is_ready() && (sts == EAGAIN)) {
                        /*
                         * This is a special cluster only case where
                         * we are unable to update CNX database for
                         * global root devs. Bubble the error up so
                         * that CFS can do the right thing.
                         */
                        error = sts;
                        goto cleanup;
                    } else {
                        ms_uprintf("Volume count mismatch for domain %s.\n", dmnName);
                        ms_uprintf("%s expects %d volumes, /etc/fdmns/%s has %d links.\n",
                                   dmnName, dmnP->vdCnt, dmnName, dmntbl->vdCount);
                        /*
                         * Let root be mounted, so sysadmin can fix it.
                         *   error = BSERRMAP(E_VOLUME_COUNT_MISMATCH);
                         *   goto cleanup;
                         */
                    }
                }
            } else {
                sts = bs_check_root_dmn_sc(dmnP);
                if (sts != EOK) {
                    goto cleanup;
                }
            }

            /*
             * Verify that the specified device is the one that
             * is really being used for the root file system.
             * It's possible for the linkded device to be wrong, so we
             * compare the rdev fields, not the vnodes.
             */

            if(((strlen(mp->m_stat.f_mntonname) == strlen(rootmpname)) &&
                 !strcmp(mp->m_stat.f_mntonname, rootmpname)) ||
                (clu_is_ready() &&
                 (strlen(mp->m_stat.f_mntonname) == strlen(bootmpname)) &&
                 !strcmp(mp->m_stat.f_mntonname, bootmpname)))
            {

                /* Get vnode of each of the devices and compare rdev values */
                for (vdi = 0; vdi < dmntbl->vdCount; vdi++) {
                    ndp->ni_nameiop = LOOKUP | FOLLOW;
                    ndp->ni_segflg = UIO_SYSSPACE;
                    ndp->ni_dirp = dmntbl->vddp[vdi]->vdName;

                    if( error = namei( ndp ) ) {
                        ms_uprintf("msfs_mount: Accessing \"%s\" failed, check device special file\n",
                                dmntbl->vddp[vdi]->vdName);
                        goto cleanup;
                    }

                    /* 
                     * Scan through the domain's vdp table looking for
                     * this device. Note that we have to look through
                     * the whole table as it can be sparse, and the
                     * order is not necessarily the same as the one in
                     * dmntbl. 
                     */
                    rdev_match = FALSE;
                    for (vdi2 = 1; vdi2 <= BS_MAX_VDI; vdi2++) { 
                        if (vdp = vd_htop_if_valid(vdi2, dmnP, FALSE, FALSE)) {
                            if (vdp->devVp->v_rdev == ndp->ni_vp->v_rdev) {
                                rdev_match = TRUE;
                                break;
                            }
                        }
                    }

                    if (!rdev_match) {
                        int isroot;

                        isroot = ((strlen(mp->m_stat.f_mntonname) ==
                                   strlen(rootmpname)) &&
                                  !strcmp(mp->m_stat.f_mntonname, rootmpname)) ?
                                   1 : 0;
                        ms_uprintf("msfs_mount: The mount device does not match the linked device.\n");
                        ms_uprintf("Check linked device in \/etc\/fdmns\/domain\n");
                        strcpy(mp->m_stat.f_mntfromname,
                               (isroot) ?
                               (char *) default_rootname :
                               (char *) default_bootname);
                        ms_uprintf("msfs_mount: Setting %s device name to %s RW\n",
                                   (isroot) ? "root" : "boot",
                                   mp->m_stat.f_mntfromname);
                        vrele( ndp->ni_vp );
                        /*
                         * Let root be mounted, so sysadmin can fix it.
                         * error = BSERRMAP(E_RDEV_MISMATCH);
                         */
                        goto cleanup;
                    }
                    vrele( ndp->ni_vp );
                }
            }

            /*
             * Now we have the mount point's domain structure (virtual disks),
             * and the requested domain/fileset's virtual disks.
             * Update all virtual disk names.
             */
            vdCnt = 0;
            for (vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
                    continue;
                }

                new_vdp = dmntbl->vddp[vdCnt];
                if (new_vdp) {
                    strcpy(vdp->vdName, new_vdp->vdName);
                }
                vd_dec_refcnt( vdp );
                vdCnt++;
            }
            /*
             * Update the domain name
             */
            strcpy(dmnP->domainName, dmnName);

            /*
             * Update mount device name
             */
            FREE(mp->m_stat.f_mntfromname, M_PATHNAME);
            MALLOC_VAR(mp->m_stat.f_mntfromname, char *, strlen(tmp) + 1,
            M_PATHNAME, M_WAITOK);
            if (!mp->m_stat.f_mntfromname) {
                error = ENOMEM;
                goto cleanup;
            }
            strcpy(mp->m_stat.f_mntfromname, tmp);

            /*
             * Flush domain dirty pages if writes have been delayed.
             */
            bs_bfdmn_flush( dmnP );

            /* TODO NOTE:
             * The flush/sync needs a mechanism to track the IO's
             * we flush and sync on. Other IO's can be started
             * after the flush (by bootstrap link kernel) causing
             * sync to return a nonzero count.
             * For now, we ignore the return.
             */
            (void) bs_bfdmn_flush_sync( dmnP );

            MountWriteDelay = 0;    /* Write delay is only for root */

            if ( clu_is_ready() &&
                (mp->m_flag & (M_GLOBAL_ROOT | M_LOCAL_ROOT)) != 0 ) {
                /*
                 * Issue the mount event for the cluster root now (global or
                 * local root), because we have the real domain and fileset
                 * names at this time.  We did not have them earlier when
                 * msfs_mountroot() was called so "generic#root" or
                 * "not_generic#root" was used at that time.  
                 */
                advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
                advfs_event->domain = dmnName;
                advfs_event->fileset = setName;
                advfs_event->directory = mp->m_stat.f_mntonname;
                advfs_post_kernel_event(EVENT_FSET_MOUNT, advfs_event);
                ms_free(advfs_event);
            }
        }

        goto cleanup;
    }

    /*
     * Copy mount on name and mounted from name to stat struct.
     */
    MALLOC(tempstring, char *, MAX_MNT_PATHLEN, M_TEMP, M_WAITOK);
    if (!tempstring) {
        error = ENOMEM;
        goto msfs_mount_exit;
    }
    error = copyinstr(args.fspec, tempstring, MAX_MNT_PATHLEN, (int *)&size);
    if (error != 0) {
        FREE(tempstring, M_TEMP);
        goto msfs_mount_exit;
    }
    MALLOC_VAR(mp->m_stat.f_mntfromname, char *, size, M_PATHNAME, M_WAITOK);
    if (!mp->m_stat.f_mntfromname) {
        error = ENOMEM;
        FREE(tempstring, M_TEMP);
        goto msfs_mount_exit;
    }
    bcopy(tempstring, mp->m_stat.f_mntfromname, size);
    bzero(tempstring, MAX_MNT_PATHLEN);
    error = copyinstr( path, tempstring, MAX_MNT_PATHLEN, (int *)&size );
    if (error != 0) {
        FREE(mp->m_stat.f_mntfromname, M_PATHNAME);
        FREE(tempstring, M_TEMP);
        goto msfs_mount_exit;
    }
    MALLOC_VAR(mp->m_stat.f_mntonname, char *, size, M_PATHNAME, M_WAITOK);
    if (!mp->m_stat.f_mntonname) {
        error = ENOMEM;
        FREE(mp->m_stat.f_mntfromname, M_PATHNAME);
        FREE(tempstring, M_TEMP);
        goto msfs_mount_exit;
    }
    bcopy(tempstring, mp->m_stat.f_mntonname, size);
    FREE(tempstring, M_TEMP);

    error = advfs_mountfs(mp);

    /* Setup to get and set a unique file set device. */
    fsId = args.fsid;
    chk_fsdev_flag = TRUE;

cleanup:
    if (error == 0) {
        domainT *dmnp;
        bfSetT *bfSetp;

        /* Update last modify time for initodr/resettodr, msfs_sync only
         * does this for rootfs, but during install this should be updated
         * too;  otherwise if this becomes the new rootfs the time appears
         * zero.
         */
        root_accessp = GETROOTACCESS( mp );
        now_time = time.tv_sec;
        sts = bmtr_put_rec(root_accessp, BMTR_FS_TIME,
                           &now_time, sizeof(now_time),
                           FtxNilFtxH);
        if (sts != EOK && sts != E_READ_ONLY) {
            /* likely i/o error; ignore it; update not critical */
            ms_uprintf("msfs_mount: bmtr_put_rec warning %s\n",
            BSERRMSG( sts ) );
        }
        /*
         * Possibly update file set dev with a unique random number.
         */
        error = chk_set_fsdev(mp, fsId, chk_fsdev_flag);

        /*
         * Mirror the smoothsync policy flag found in the mount struct
         * into the domain for advfs.  Done this way strictly for 
         * layering.
         */
        dmnp = GETDOMAINP(mp);

	/* If this is a readonly, dual mount,ensure we flush the log record
         * onto disk
         */
	if( (mp->m_flag & M_DUAL) && (mp->m_flag & M_RDONLY) )
                 lgr_flush(dmnp->ftxLogP);

        /*
         * Note that m_flag is now 64bits.  The M_SMSYNC2 position is in
         * the low longword.  So, rather than change the domain struct,
         * just accept the truncation.
         */
        dmnp->smsync_policy = (int)(mp->m_flag & M_SMSYNC2);

        /*
         *  If -u -o extend,
         *      Call extend_vd for each vd. 
         */
        if ( (mp->m_flag & M_UPDATE) &&
             (mp->m_flag & M_EXTENDFS) &&
             !(mp->m_flag & M_RDONLY) ) {
            int vdi, vdCnt;
            struct vd *vdp;
            vdCnt = 0;
            for (vdi = 1; vdCnt < dmnp->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
                if ( !(vdp = vd_htop_if_valid(vdi, dmnp, TRUE, FALSE)) ) {
                    continue;
                }
                sts = vd_extend(vdp);
                if (sts > 0) {
                    ms_uprintf("The attempt to extend the virtual disk %s failed.\n",
                                vdp->vdName);
                    error = sts;
                }
                vd_dec_refcnt( vdp );
                vdCnt++;
            }
        }

        bfSetp = GETFILESETNODE(mp)->bfSetp;

    }

    /*
     *  If freezefs was held off
     *    Allow freezes
     */
    if ( stopFreezefs ) {
        domainT *dmnP;
        dmnP = GETDOMAINP(mp);
        mutex_lock(&dmnP->dmnFreezeMutex);
        dmnP->dmnFreezeRefCnt--;
        if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
            thread_wakeup( (vm_offset_t)&dmnP->dmnFreezeWaiting );
        }
        mutex_unlock(&dmnP->dmnFreezeMutex);
    }

msfs_mount_exit:

    if (dmntbl != NULL) {
        free_vdds( dmntbl );
        ms_free(dmntbl);
    }
    if (dmnName != NULL) {
        ms_free(dmnName);
    }
    if (tmp != NULL) {
        ms_free(tmp);
    }

    return error;
}

static
advfs_mountfs(struct mount *mp)
{
    char *setName = NULL;
    char *dmnName = NULL;
    struct fileSetNode *dn = NULL;
    struct fileSetNode *fsp;
    int dmn_active = FALSE, on_fileset_list = FALSE, domain_open = FALSE,
    locks_init = FALSE, set_open = FALSE, root_open = FALSE,
    fragBfOpen = FALSE;
    bfSetParamsT *bfSetParamsp = NULL;
    bfAccessT *bfap;
    struct vnode *nvp = NULL;
    extern struct vnodeops msfs_vnodeops;
    struct fsContext *root_context;
    struct undel_dir_rec undel_rec;
    quotaInfoT *qip;
    int error;
    statusT sts;
    bfSetT *setp = NULL;
    advfs_ev *advfs_event;
    bsrRsvd17T bsrRsvd17;
    u_long flag = mp->m_flag & ADVFS_MOUNT_FLAGS;
    int noop;
    int first_nonrdonly_mount = TRUE;
    struct timeval new_time;
    int ss_was_activated = FALSE;
    u_long dmnState;

    /*
     * It's not enough to allocate only up to the maximum
     * size of a domain name.  Caller could be passing in
     * a bogus domain name longer than the allowed maximum.
     */
    dmnName = ms_malloc(strlen( mp->m_stat.f_mntfromname) + 1);

    dn = (struct fileSetNode *)ms_malloc( sizeof( struct fileSetNode ) );

    dn->filesetMagic = FSMAGIC;

    bfSetParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));

    /* Extract the set name and the domain name */

    setName = toke_it( mp->m_stat.f_mntfromname, '#', dmnName );
    if (setName == NULL) {
        error = ENOENT;
        goto cleanup;
    }

    /* 
     * activate the bfset (pass in the advfs mount flags) 
     * + special flag used only in cluster
     */

    sts = bs_bfset_activate( dmnName, setName, flag|M_MSFS_MOUNT,
                             &dn->bfSetId );

    if (sts != 0) {
        error = BSERRMAP( sts );
        goto cleanup;
    }

    dmn_active = TRUE;

    dn->bfSetp = NULL;

    /*
     * Disallow mounting a fileset with the same set ID.
     */

    FILESET_WRITE_LOCK(&FilesetLock );

    for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
        if (BS_BFS_EQL(dn->bfSetId, fsp->bfSetId)) {

            FILESET_UNLOCK(&FilesetLock);

            error = EBUSY;
            goto cleanup;
        }
        /* check for non readonly, mounted fset already in this domain */
        if ((BS_UID_EQL(dn->bfSetId.domainId, fsp->bfSetId.domainId)) &&
            (fsp->mountp) && !(fsp->mountp->m_flag & M_RDONLY) ) {
            first_nonrdonly_mount = FALSE;
        }
    }

    /*
     * Link this fileset into the list of mounted filesets.
     */
    if (FilesetHead != NULL) {
        FilesetHead->fsPrev = &dn->fsNext;
    }
    dn->fsNext = FilesetHead;
    dn->fsPrev = &FilesetHead;
    FilesetHead = dn;

    on_fileset_list = TRUE;

    /*
     * Reference the domain so that nobody can remove it
     * while we're using it.
     */
    if( sts = bs_domain_access( &dn->dmnP, dn->bfSetId.domainId, FALSE ) ) {
        FILESET_UNLOCK(&FilesetLock);
        error = BSERRMAP( sts );
        goto cleanup;
    }
    domain_open = TRUE;
    dn->mountp = mp;
    dn->dmnP->mountCnt++;

    /* update the vfast first mount time if first mount, non-readonly */
    mutex_lock( &dn->dmnP->ssDmnInfo.ssDmnLk );
    if((first_nonrdonly_mount == TRUE) &&
        !(dn->mountp->m_flag & M_RDONLY) &&
       (dn->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
        dn->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {

        TIME_READ(new_time);
        dn->dmnP->ssDmnInfo.ssFirstMountTime = new_time.tv_sec;
    }
    mutex_unlock( &dn->dmnP->ssDmnInfo.ssDmnLk );

    FILESET_UNLOCK(&FilesetLock );

    /*
     * Make sure we can read all the necessary information
     * needed to perform the mount.  If we can't, fail the
     * mount.  If we find a volume size inconsistency but
     * we can still read all of the necessary information
     * needed to perform the mount, continue the mount
     * processing but make this a read-only mount.  This will
     * force the user mounting the system to correct the
     * problem before writing any new data and yet will allow
     * him to access the fileset.
     */
    dn->fsFlags = 0;
    sts = check_vd_sizes(dn);
    if (sts < 0) {
        if (sts == -1)
            error = EIO;
        else
            error = -sts;
        goto cleanup;
    }
    else if (sts == 0) {
        mp->m_flag |= M_RDONLY;
    }

    /*
     * Initialize the locks and mutexes in the fileSetNode structure.
     */

    mutex_init3(&dn->filesetMutex, 0, "filesetMutex", ADVfilesetMutex_lockinfo);
    for (qip = &dn->qi[0]; qip < &dn->qi[MAXQUOTAS]; qip++) {
        lock_setup( &(qip->qiLock), ADVquotaInfoT_qiLock, TRUE );
    }
    locks_init = TRUE;

    if( sts = rbf_bfs_access( &dn->bfSetp, dn->bfSetId, FtxNilFtxH ) ) {
        error = BSERRMAP( sts );
        goto cleanup;
    }
    set_open = TRUE;

    sts = bs_fragbf_open( dn->bfSetp );
    if (sts != EOK) {
        error = BSERRMAP( sts );
        goto cleanup;
    }
    fragBfOpen = TRUE;

    if( sts = bs_get_bfset_params( dn->bfSetp, bfSetParamsp, 0 ) ) {
        error = BSERRMAP( sts );
        goto cleanup;
    }

    if (bfSetParamsp->cloneId > 0) {
        dn->fsFlags |= FS_CLONEFSET;
        mp->m_flag |= M_RDONLY;
    }

    BS_BFTAG_IDX(dn->rootTag) = bfSetParamsp->fsContext[0];
    BS_BFTAG_IDX(dn->tagsTag) = bfSetParamsp->fsContext[2];
    BS_BFTAG_SEQ(dn->rootTag) = bfSetParamsp->fsContext[1];
    BS_BFTAG_SEQ(dn->tagsTag) = bfSetParamsp->fsContext[3];

    /*
     * Fill required data into the mount struct.
     */

    mp->m_flag |= M_LOCAL;
    mp->m_info = (qaddr_t)dn;

    /*
     * Fill in the mount entries 'stat' area.
     */

    msfs_statfs( mp );

    /*
     * the rest of msfs_mount used to be in msfs_start...
     */

    /*
     * access the root bf
     */
    if (sts = bs_access(
                        &bfap,
                        dn->rootTag,
                        dn->bfSetp,
                        FtxNilFtxH,
                        BF_OP_GET_VNODE,
                        mp,
                        &nvp
                        ) != EOK) {
        ms_uprintf("error returned from bs_access in msfs_mount %s\n",
                BSERRMSG(sts));
        error = BSERRMAP( sts );
        goto cleanup;
    }

    root_open = TRUE;
    root_context = VTOC( nvp );

    if ( root_context == 0 ) {
        root_context = vnode_fscontext_allocate( nvp );
    }

    FS_FILE_WRITE_LOCK( root_context );

    /*
     * Set up the root's fsContext area
     */

    root_context->bf_tag = dn->rootTag;
#   ifdef ADVFS_DEBUG
        root_context->file_name[0] = '/';
        root_context->file_name[1] = '\0';
#   endif
    root_context->fileSetNode = dn;
    root_context->dirstamp = 0;
    root_context->fs_flag = 0;
    root_context->dirty_stats = 0;
    bfap->fragState = FS_FRAG_NONE;

    sts = bmtr_get_rec(bfap,
                       BMTR_FS_UNDEL_DIR,
                       &undel_rec,
                       sizeof(struct undel_dir_rec)
                       );
    if (sts == EOK) {
        root_context->undel_dir_tag = undel_rec.dir_tag;
    }
    else if (sts == EBMTR_NOT_FOUND) {
        root_context->undel_dir_tag = NilBfTag;
    } else {
        FS_FILE_UNLOCK( root_context );
        ms_uprintf("error returned from bmtr_get_rec for BMTR_FS_UNDEL_DIR in msfs_mount %s\n",
               BSERRMSG(sts));
        error = BSERRMAP( sts );
        goto cleanup;
    }
    /* Check to see if the directory being mounted has an index file
     * associated with it and if so open it now */

    IDX_OPEN_INDEX(bfap,sts);
    if (sts != EOK)
    {
        FS_FILE_UNLOCK( root_context );
        error = BSERRMAP( sts );
        goto cleanup;
    }

    /*
     * Initialize the new vnode
     */

    /* set the vnode type */
    nvp->v_type = VDIR;

    /* say its the root  */
    nvp->v_flag |= VROOT;
    nvp->v_lastr = 0;

    /*
     * Enter new file onto mount queue
     */
    bs_insmntque(FtxNilFtxH, bfap, mp);

    /*
     * get the root stats
     */
    sts = bmtr_get_rec(bfap,
                       BMTR_FS_STAT,
                       &root_context->dir_stats,
                       sizeof(statT)
                       );
    if (sts != EOK) {
        FS_FILE_UNLOCK( root_context );
        aprintf("error returned from bmtr_get_rec for BMTR_FS_STAT in msfs_mount %s\n",
                BSERRMSG(sts));
        error = BSERRMAP( sts );
        goto cleanup;
    }

    bfap->file_size = root_context->dir_stats.st_size;
    bfap->fragId = root_context->dir_stats.fragId;
    bfap->fragPageOffset = root_context->dir_stats.fragPageOffset;

    /*
     * Save the root vnode pointer and access structure pointer
     * in the fileSetNode structure.
     */

    dn->rootAccessp = bfap;
    dn->root_vp = nvp;

    root_context->last_offset = 0;
    root_context->initialized = 1;

    if ((dn->fsFlags & FS_CLONEFSET) == 0) {
        sts = quota_activate(dn);
        if (sts != E_QUOTA_NOT_MAINTAINED && sts != EOK) {
            ms_uprintf("advfs_mount: WARNING: quota activation failed.\n");
        }
    }
    else {
        if (dn->dmnP->dmnVersion >= FIRST_LARGE_QUOTAS_VERSION) {
            dn->quotaStatus |= QSTS_LARGE_LIMITS;
        }
        sts = get_clone_usage_stats(dn);
        if (sts != EOK) {
            ms_uprintf("advfs_mount: WARNING: could not get fileset stats.\n");
        }
    }

    FS_FILE_UNLOCK( root_context );

    /*
     *  Prior to V4.0, AdvFS supported a hierarchical storage manager.
     *  If this HSM product was enabled for this fileset then we must
     *  disallow the mount because HSM shelving is no longer supported.
     */
    setp = dn->bfSetp;
    sts = bmtr_get_rec(setp->dirBfAp,
                       BSR_RSVD17,
                       &bsrRsvd17,
                       sizeof(bsrRsvd17)
                       );
    if ((sts == EOK) && (bsrRsvd17.r7 != 0)) {
        /*
         *  Disallow the mount, shelving not supported.
         */
        ms_uprintf("AdvFS mount - HSM shelving no longer supported!\n");
        sts = EIO;
        error = BSERRMAP(sts);
        goto cleanup;
    }

    mutex_lock(&setp->dmnP->mutex);
    MS_SMP_ASSERT(setp->state == BFS_READY || setp->state == BFS_UNMOUNTED);
    /* Set this so ss_open_file won't start an op during this transistion. */
    setp->state = BFS_BUSY;
    mutex_unlock(&setp->dmnP->mutex);

    if ( setp->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ) {
	/*
         * During failover to avoid deadlock between recovery of cluster_root
         * and recovery of other non root cluster filesystem, cluster_root
         * recovery thread need not acquire DmnTblLock.
         */
        if ( (flag & M_FAILOVER ) && ((strcmp(setp->dmnP->domainName, "cluster_root")) == 0 ) ) {
                dmnState = M_GLOBAL_ROOT;
        }
        else {
                dmnState = 0;
        }

        /* Wait for all vfast threads to finish before mounting the set. */
        ss_change_state(setp->dmnP->domainName, SS_SUSPEND, &dmnState, 0);
        ss_was_activated = TRUE;
    }

    /*
    ** Serialize with hold_cloneset.
    ** Previous callers of hold_cloneset will block this thread here until
    ** they are done.
    */
    mutex_lock(&setp->dmnP->mutex);

    if ( setp->cloneId != BS_BFSET_ORIG ) {
        /*
        ** There may be threads running that do not want the clone fileset
        ** mounted yet. These threads will have incremented  bfsHoldCnt.
        ** Wait for those threads to get done.
        */
        setp->bfsOpPend = 1;

        /* Wait for current holds. */
        while ( setp->bfsHoldCnt > 0 ) {
            /* Only one thread can be waiting on bfsOpWait. */
            MS_SMP_ASSERT(setp->bfsOpWait == 0);
            setp->bfsOpWait = 1;
            thread_sleep( (vm_offset_t)&setp->bfsHoldCnt,
                          &setp->dmnP->mutex.mutex, FALSE );
            mutex_lock( &setp->dmnP->mutex );
        }

        setp->bfsOpPend = 0;
        if ( setp->bfsHoldWait > 0 ) {
            thread_wakeup( (vm_offset_t)&setp->bfsHoldWait );
        }

    }

    /* bitfile set node needs to point to fileset node */
    setp->fsnp = dn;
    
    setp->state = BFS_READY;  /* coordinate with ss_open_file */

    mutex_unlock( &setp->dmnP->mutex );
    
    if ( ss_was_activated == TRUE ) {
	/*
         * During failover to avoid deadlock between recovery of cluster_root
         * and recovery of other non root cluster filesystem, cluster_root
         * recovery thread need not acquire DmnTblLock.
         */
        if ( (flag & M_FAILOVER ) && ((strcmp(setp->dmnP->domainName, "cluster_root")) == 0 ) ) {
                dmnState = M_GLOBAL_ROOT;
        }
        else {
                dmnState = 0;
        }

        /* Tell vfast that it's OK to work on this domain again. */
        ss_change_state(setp->dmnP->domainName, SS_ACTIVATED, &dmnState, 0);
        ss_was_activated = FALSE;
    }

    if (mp->m_flag & M_DIRECTIO) {
        setp->bfSetFlags |= BFS_IM_DIRECTIO;
    } else {
        setp->bfSetFlags &= ~BFS_IM_DIRECTIO;
    }

    if ( clu_is_ready() &&
        (mp->m_flag & (M_GLOBAL_ROOT | M_LOCAL_ROOT)) != 0 ) {
        /*
         * Suppress the event for cluster root (global or local) because at
         * this point in time the domain and fileset names are either
         * "generic#root" or "not_generic#root".  This will not be corrected
         * until a mount update occurs.  This is when the event will be
         * issued for the cluster root.  
         */
        noop = 0;
    }
    else {
        /*
         * Issue a mount event for other mounts which are neither cluster
         * global roots nor cluster local roots.
         */
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = dmnName;
        advfs_event->fileset = setName;
        advfs_event->directory = mp->m_stat.f_mntonname;
        advfs_post_kernel_event(EVENT_FSET_MOUNT, advfs_event);
        ms_free(advfs_event);
    }

    ms_free( dmnName );

    ms_free( bfSetParamsp );

    return (0);

cleanup:

    /*
     * Release resources and return an error code.
     */
    if (root_open) {
        (void) bs_close(bfap, MSFS_INACTIVE_CALL);
    }

    /* if we got to the point of setting up a fileSetNode pointer
     * we have more work to do
     */

    if (dn){
	if (dn->quotaStatus & QSTS_QUOTA_ON) {
        	(void) quota_deactivate( dn );
	}

    	if (fragBfOpen) {
        	(void) bs_fragbf_close( dn->bfSetp );
    	}

    	if (locks_init) {
        	for (qip = &dn->qi[0]; qip < &dn->qi[MAXQUOTAS]; qip++) {
            		lock_terminate( &qip->qiLock );
        	}
        	mutex_destroy(&dn->filesetMutex);
    	}

    	if (on_fileset_list) {

        	FILESET_WRITE_LOCK(&FilesetLock);

        	*dn->fsPrev = dn->fsNext;
        	if (dn->fsNext != NULL) {
            		dn->fsNext->fsPrev = dn->fsPrev;
        	}

        	FILESET_UNLOCK(&FilesetLock);
    	}

    	if (set_open) {
        	bs_bfs_close(dn->bfSetp, FtxNilFtxH, BFS_OP_DEF);
    	}
    
    	if (domain_open) {
        	FILESET_WRITE_LOCK(&FilesetLock);
        	MS_SMP_ASSERT(dn->dmnP->mountCnt > 0);
        	dn->dmnP->mountCnt--;
        	FILESET_UNLOCK(&FilesetLock )
        	bs_domain_close( dn->dmnP );
    	}
    	if (dmn_active) {
        	(void) bs_bfdmn_deactivate( dn->bfSetId.domainId, flag|M_MSFS_MOUNT );
    	}
    } 

    if (dmnName != NULL) {
        ms_free( dmnName );
    }

    if (dn != NULL) {
        ms_free( dn );
    }

    if (bfSetParamsp != NULL) {
        ms_free( bfSetParamsp );
    }
    return error;
}

/*
 * check_vd_sizes - Verify the sizes of the virtual disks.
 *                  Try to check all of the volumes in the domain
 *                  even though just one corrupted volume will
 *                  cause the mount to fail.  That way, the user
 *                  gets all the bad news at once rather than
 *                  incrementally.
 *
 * Return values:  >0: No inconsistency; allow mount to proceed
 *                  0: Non-fatal inconsistency; mount read-only
 *                 <0: Fatal inconsistency or internal error; fail the mount
 */
int
check_vd_sizes(struct fileSetNode *fsnp)
{
    caddr_t         lastBlkp = NULL;            /* Buffer for reading */
    domainT         *dmnP;                      /* This domain */
    struct vd       *vdp;                       /* Current volume */
    DEVGEOMST       *geomstp = NULL;            /* For ioctl() call */
    ulong_t         actual_vd_size;             /* As returned by ioctl() */
    getioinfo_t     *getioinfop;                /* To get disk partition size */
    struct vnode    *dvp;                       /* Device vnode pointer */
    int             sts,                        /* Return status */
                    ret,                        /* Returned by ioctl() */
                    index,                      /* Index of current volume */
                    vols_checked,               /* Volumes checked so far */
                    corrupt_vd_found = FALSE,   /* TRUE if we found a bad one */
                    mislabeled_vd_found = FALSE;/* TRUE if we found bad label */

    MALLOC(getioinfop,void *,sizeof(getioinfo_t), M_DEVBUF,
               M_WAITOK|M_ZERO);
    getioinfop->pi_version = PI_VERSION_0;

    dmnP = fsnp->dmnP;

    /*
     * For every virtual disk in the domain, see if its size matches
     * the size as described in the virtual disk structure.  If it
     * does, great.  If it is greater than that recorded in the
     * virtual disk structure, let the mount proceed but inform the
     * user that the disk is not being fully utilized.  If the size
     * of the volume is smaller than that described in the virtual
     * disk structure, see if we can at least read the last in-use
     * block on the volume.  If so, allow the mount to succeed in
     * read-only mode.  If not, fail the mount.
     */
    for ( vols_checked = 0, index = 1;
         (vols_checked < dmnP->vdCnt) && (index <= BS_MAX_VDI);
         index++) {

        if ( !(vdp = vd_htop_if_valid(index, dmnP, TRUE, FALSE)) ) {
            continue;
        }
        dvp = vdp->devVp;

        VOP_IOCTL(dvp, DIOCGETIOINFO, getioinfop, FREAD, NOCRED, sts, &ret);

        if (sts == EFBIG) {
            getioinfop->pi_un.pi_v0.pi_part.p_size = UINT_MAX;
            ms_uprintf("NOTICE: virtual disk %s is larger than AdvFS maximum\n",
                       vdp->vdName);
            sts = 0;
        }

        if (sts != 0) {
            ms_uprintf("Cannot get size of virtual disk %s.\n",vdp->vdName);
            corrupt_vd_found = TRUE;
        }
        else {
            actual_vd_size = getioinfop->pi_un.pi_v0.pi_part.p_size;
            if ((vdp->vdSize == actual_vd_size) ||
              (vdp->vdSize == (actual_vd_size & ~0x0f))) {
                ;   /* Great; this is what we expected. */
            }
            else if (vdp->vdSize < actual_vd_size) {
                if  ( fsnp->mountp->m_flag & M_EXTENDFS ) {
                    sts = vd_extend(vdp);
                    if (sts > 0) {
                        ms_uprintf("The attempt to extend the virtual disk %s failed.\n",
                                  vdp->vdName);
                    }
                } else {
                    ms_uprintf("NOTICE: Size of virtual disk %s is %ld blocks\n",
                               vdp->vdName,
                               actual_vd_size);
                    ms_uprintf("        AdvFS is only using %ld blocks.\n",
                               vdp->vdSize);
                    ms_uprintf("        Use the '-o extend' mount option to extend the filesystem\n");
                }
            }
            else {
                ms_uprintf("Actual size of virtual disk %s is %ld blocks\n",
                           vdp->vdName, actual_vd_size);
                ms_uprintf("but recorded size is %ld blocks.\n",
                           vdp->vdSize);
                mislabeled_vd_found = TRUE;

                /*
                 * The volume is smaller than we expected.  The
                 * best-case scenario now is that we will mount the
                 * fileset in read-only mode.  See if we can read the
                 * last in-use block on the volume.  We find this block
                 * by scanning the SBM backwards.  If we cannot read this
                 * block or if we can't even read the SBM, fail the mount.
                 */
                {
                    struct uio      *uiop;          /* I/O structure */
                    struct iovec    *iovp;          /* I/O vector */
                    bfPageRefHT     pgref;          /* Handle to page */
                    struct bsStgBm* sbmp;           /* An SBM page */
                    long            page_to_read,   /* Number of page to read */
                                    clust_to_skip;  /* Off. of last in-use cl.*/
                    uint32T         cur_word;       /* Curr. word in SBM page */
                    int             done,           /* TRUE if page is found */
                                    sbm_word,       /* Current word in SBM */
                                    in_use_bits;    /* Bits used in sbm_word */

                    /*
                     * Allocate the bigger structures from heap
                     * to save stack space.
                     */
                    done = FALSE;
                    if (lastBlkp == NULL) {
                        lastBlkp = (caddr_t) ms_malloc(BS_BLKSIZE +
                                                       sizeof(struct uio) +
                                                       sizeof(struct iovec));
                        uiop = (struct uio *)(lastBlkp + BS_BLKSIZE);
                        iovp = (struct iovec *)((caddr_t)uiop +
                                                     sizeof(struct uio));
                    }

                    for (page_to_read = vdp->bitMapPgs - 1;
                         !done && page_to_read >= 0;
                         page_to_read--) {

                        /*
                         * Read the SBM page.
                         */
                        sts = bs_refpg(&pgref,
                                       (void *)&sbmp,
                                       vdp->sbmp,
                                       page_to_read,
                                       BS_NIL);
                        if (sts) {
                            corrupt_vd_found = TRUE;
                            ms_uprintf("Cannot read essential data on %s.\n",
                                        vdp->vdName);
                            break;
                        }

                        /*
                         * Scan backward through the page, trying to find
                         * an in-use bit in the bitmap.
                         */
                        for (sbm_word = SBM_LONGS_PG - 1;
                             (sbm_word >= 0) && (sbmp->mapInt[sbm_word] == 0);
                             sbm_word--);

                        if (sbm_word >= 0) {

                            /*
                             * Calculate the offset of the last 512-byte block
                             * that is currently in use.  We do this by counting
                             * the clusters in the SBM prior to the very last
                             * in-use cluster bit.  This is a three-part
                             * process:
                             *   1. Account for all of the clusters in the
                             *      SBM pages before the page we are currently
                             *      using.
                             *   2. Account for all of the clusters in the
                             *      mapInt[] array of the current page before
                             *      our entry in the array.
                             *   3. Account for the clusters in our mapInt[]
                             *      entry.
                             */
                            clust_to_skip = page_to_read * SBM_BITS_PG;
                            clust_to_skip += sbm_word * SBM_BITS_LONG;
                            for (cur_word = sbmp->mapInt[sbm_word],
                                 in_use_bits = SBM_BITS_LONG;
                                 !(cur_word & 0x80000000) && in_use_bits > 0;
                                 cur_word = cur_word << 1, in_use_bits--);

                            /*
                             * If we couldn't find any bits on in this word,
                             * something is very wrong, probably this search
                             * algorithm.
                             */
                            MS_SMP_ASSERT(in_use_bits > 0);
                            if (in_use_bits <= 0) {
                                bs_derefpg(pgref, BS_RECYCLE_IT);
                                ms_uprintf("Cannot find last in-use block.\n");
                                corrupt_vd_found = TRUE;
                                sts = -1;
                                break;
                            }

                            clust_to_skip += (in_use_bits - 1);
                            done = TRUE;
                        }

                        /*
                         * Release the SBM page.
                         */
                        sts = bs_derefpg(pgref, BS_RECYCLE_IT);
                        MS_SMP_ASSERT(sts == EOK);
                        if (sts != EOK) {
                            ms_uprintf("Could not release SBM page.\n");
                            corrupt_vd_found = TRUE;
                            break;
                        }
                    }

                    /*
                     * If we found the last in-use block, try to read it.
                     */
                    if (sts == EOK) {
                        iovp->iov_base = (caddr_t)lastBlkp;
                        iovp->iov_len = BS_BLKSIZE;
                        uiop->uio_iov = iovp;
                        uiop->uio_iovcnt = 1;
                        uiop->uio_segflg = UIO_SYSSPACE;
                        uiop->uio_rw = UIO_READ;
                        uiop->uio_resid = BS_BLKSIZE;
                        uiop->uio_offset = (clust_to_skip * vdp->stgCluster + 1) *
                                           BS_BLKSIZE;

                        VOP_READ(dvp, uiop, 0, u.u_nd.ni_cred, sts);
                        if (sts || uiop->uio_resid) {
                            ms_uprintf("Cannot read essential data on %s.\n",
                            vdp->vdName);
                            corrupt_vd_found = TRUE;
                        }
                    }
                }
            }
        }
        vols_checked++;
        vd_dec_refcnt( vdp );     /* decrement vdRefCnt bumped above */
    }

    if (corrupt_vd_found) {
        ms_uprintf("Corrupted volume found; failing mount of %s.\n",
                   fsnp->mountp->m_stat.f_mntfromname);
        sts = -1;
    }
    else if (mislabeled_vd_found) {
        fsnp->fsFlags |= FS_FORCE_RDONLY;
        ms_uprintf("Mounting fileset %s in read-only mode.\n",
                   fsnp->mountp->m_stat.f_mntfromname);
        sts = 0;
    }
    else {
        sts = 1;
    }

    FREE(getioinfop, M_DEVBUF);
    if (lastBlkp)
        ms_free(lastBlkp);
    if (geomstp)
        ms_free(geomstp);
    return sts;
}

/*
 * getvp
 *
 */
getvp(  struct vnode **vpp,     /* out */
        caddr_t fname,          /* in */
        struct nameidata *ndp,  /* in */
        enum uio_seg space )    /* in */
{
    int error;
    *vpp = NULL;
    ndp->ni_nameiop = LOOKUP | FOLLOW;
    ndp->ni_segflg = space;
    ndp->ni_dirp = fname;

    if( error = namei( ndp ) ) {
       return( error );
    }

    *vpp = ndp->ni_vp;
    return( 0 );
}


/*
 * msfs_start
 */

msfs_start(
           struct mount *mp, /* in - mount structure pointer */
           int flags         /* in - flags */
           )
{
    return(0);
}

/*
 * msfs_unmount
 *
 * unmount the file system
 */
msfs_unmount(
             struct mount *mp, /* in - the mount structure pointer */
             int mntflags      /* in - flags */
             )
{
    struct fileSetNode *fsnp, *fsp;
    int error = 0;
    statusT sts;
    bfSetT *setp;
    struct fsContext *dir_context_pointer;
    quotaInfoT *qip;
    struct vnode *vp, *rvp;
    extern int advfs_shutting_down;
    char *setName=NULL;
    char dmnName[MNAMELEN];
    advfs_ev advfs_event;
    u_long advfs_flags = mp->m_flag & ADVFS_MOUNT_FLAGS;
    domainT *dmnP;
    int restart_quotas = FALSE, fragBfClosed = FALSE;
    int restart_vfast = FALSE, nonrdonly_mount = FALSE;
    bfsStateT bfs_state;
    u_long cfsState = (advfs_flags & M_FAILOVER) &&
                      (advfs_flags & M_GLOBAL_ROOT) ? M_GLOBAL_ROOT : 0;
    long busy;

    fsnp = GETFILESETNODE(mp);
    setp = fsnp->bfSetp;
    dmnP = setp->dmnP;
    rvp = fsnp->root_vp;

    bfs_state = setp->state;

    if (!advfs_shutting_down) {

        /*  Return EBUSY if domain is frozen.
         *  Take the dmnFreezeMutex to check status and to
         *  increment the ref count to prevent a new freeze
         *  from starting if we begin unmount.
         */
        mutex_lock(&dmnP->dmnFreezeMutex);
        if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            return(EBUSY);
        } else {
            dmnP->dmnFreezeRefCnt++;
            mutex_unlock(&dmnP->dmnFreezeMutex);
        }

    }

    mutex_lock( &dmnP->mutex );
    /* Set this so ss_open_file won't start an op during this transition. */
    setp->state = BFS_BUSY;
    mutex_unlock( &dmnP->mutex );

    if (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) {
        u_long dmnState = cfsState;
        ss_change_state(dmnP->domainName, SS_SUSPEND, &dmnState, 0);
        /* never reactivate vfast during a shutdown */
        restart_vfast = !advfs_shutting_down;
    }

    /*
     * Get rid of all vnodes that have no references, the root
     * and quota files are internally referenced and won't go away.
     * Then flush dirty file stats from closed files without vnodes.
     */
    (void) vflush(mp, rvp, 0);
    bfs_flush_dirty_stats(setp, FtxNilFtxH);

    /*
     * We can only unmount if we have just 3 references remaining,
     * 1 on the root and 1 on each quota file when quotas are active.
     */
    if (rvp->v_usecount != 1) {
        error = EBUSY;
        goto cleanup;
    }
    MOUNT_VLIST_LOCK(mp);
    busy = (fsnp->quotaStatus & QSTS_QUOTA_ON) ? 3 : 1;
    for (vp = mp->m_mounth; vp; vp = vp->v_mountf) {
        busy -= vp->v_usecount;
        if (busy < 0) {
            MOUNT_VLIST_UNLOCK(mp);
            error = EBUSY;
            goto cleanup;
        }
    }
    MOUNT_VLIST_UNLOCK(mp);

    /* close the frag bitfile */
    sts = bs_fragbf_close(setp);
    if (sts != EOK) {
        error = BSERRMAP(sts);
        goto cleanup;
    }

    fragBfClosed = TRUE;

    /* shut off quotas now that no space allocations can change */
    if (fsnp->quotaStatus & QSTS_QUOTA_ON) {
        quota_deactivate( fsnp );
        restart_quotas = TRUE;
    }

    /* eliminate quota file vnodes and any we missed - only root remains */
    error = vflush(mp, rvp, 0);
    if (error)
        goto cleanup;

    /*
     * update the root stats (if needed)
     */
    dir_context_pointer = VTOC( rvp );
    if ((dir_context_pointer != NULL) &&
        (dir_context_pointer->dirty_stats == TRUE)) {
        bfAccessT *root_accessp = GETROOTACCESS(mp);
        sts = fs_update_stats( rvp, root_accessp, FtxNilFtxH, 0 );
        if (sts != EOK) {
            ms_uprintf("Error updating roots stats %s\n", BSERRMSG(sts));
        }
    }

    /* close the root and flush it */
    vrele(rvp);
    error = vflush(mp, NULL, 0);
    if (error) {
        /* assume we failed because root is in use and reref it */
        VREF(rvp);
        goto cleanup;
    }

    /*
     * Remove fileSetNode (fileset) from mounted fileset list.
     * Do this prior to deactivating the bfSet so there won't be
     * a fileSetNode on the list with a bad bfSetp.
     */

    FILESET_WRITE_LOCK(&FilesetLock );

    *fsnp->fsPrev = fsnp->fsNext;
    if (fsnp->fsNext != NULL) {
        fsnp->fsNext->fsPrev = fsnp->fsPrev;
    }
    MS_SMP_ASSERT(dmnP->mountCnt > 0);
    dmnP->mountCnt--;

    /* reset the vfast first mount time if no other mounted filesets writable */
    for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
        /* check for non readonly, mounted fset already in this domain */
        if ((BS_UID_EQL(setp->bfSetId.domainId, fsp->bfSetId.domainId)) &&
            (fsp->mountp) && !(fsp->mountp->m_flag & M_RDONLY) ) {
            nonrdonly_mount = TRUE;
        }
    }
    mutex_lock( &dmnP->ssDmnInfo.ssDmnLk );
    if (nonrdonly_mount == FALSE) {
        setp->dmnP->ssDmnInfo.ssFirstMountTime = 0;
    }
    mutex_unlock( &dmnP->ssDmnInfo.ssDmnLk );

    FILESET_UNLOCK(&FilesetLock );

    /*
     * Close the fileset.
     */
    setp->fsnp = NULL;

    /* coordiate with ss_open_file */
    mutex_lock( &dmnP->mutex );
    MS_SMP_ASSERT(setp->state == BFS_BUSY);
    setp->state = BFS_UNMOUNTED;
    mutex_unlock( &dmnP->mutex );

    if (restart_vfast) {
        /* Tell vfast that it's OK to work on this domain again. */
        u_long dmnState = cfsState;
        ss_change_state(dmnP->domainName, SS_ACTIVATED, &dmnState, 0);
    }

    bs_bfs_close(setp, FtxNilFtxH, BFS_OP_DEF);

    if (!advfs_shutting_down) {
        mutex_lock(&dmnP->dmnFreezeMutex);
        dmnP->dmnFreezeRefCnt--;
        if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
            thread_wakeup( (vm_offset_t)&dmnP->dmnFreezeWaiting );
        }
        mutex_unlock(&dmnP->dmnFreezeMutex);
    }

    /*
     * Close the domain (Should this be inside bs_bfdmn_deactivate?)
     */
    bs_domain_close( dmnP );
    
    /*
     * For cluster, mark deactivate as being in mount/unmount path.
     * Deactivate the domain
     */
    if ((sts = bs_bfdmn_deactivate( fsnp->bfSetId.domainId, advfs_flags|M_MSFS_MOUNT )) != EOK) {
        /*
         * how can this error ever happen?  race with advfs utils?
         */
        aprintf("advfs_unmount: bs_bfdmn_deactivate failed.\n");
    }

    /*
     * Destroy the locks and mutexes in the fileSetNode structure.
     */
    for (qip = &fsnp->qi[0]; qip < &fsnp->qi[MAXQUOTAS]; qip++) {
        lock_terminate( &qip->qiLock );
    }
    mutex_destroy( &fsnp->filesetMutex );

    MS_SMP_ASSERT(fsnp->filesetMagic == FSMAGIC);
    fsnp->filesetMagic |= MAGIC_DEALLOC;
    ms_free( fsnp );

    setName = toke_it( mp->m_stat.f_mntfromname, '#', dmnName );

    bzero(&advfs_event, sizeof(advfs_ev));
    advfs_event.domain = dmnName;
    advfs_event.fileset = setName;
    advfs_event.directory = mp->m_stat.f_mntonname;
    advfs_post_kernel_event(EVENT_FSET_UMOUNT, &advfs_event);

    if (mp->m_stat.f_mntonname)
            FREE(mp->m_stat.f_mntonname, M_PATHNAME);
    if (mp->m_stat.f_mntfromname)
            FREE(mp->m_stat.f_mntfromname, M_PATHNAME);

    MOUNT_LOCK(mp);
    mp->m_flag &= ~M_LOCAL;
    MOUNT_UNLOCK(mp);

    return 0;

cleanup:

    mutex_lock(&dmnP->mutex);
    setp->state = bfs_state;
    mutex_unlock(&dmnP->mutex);

    if (restart_vfast) {
        /* reset flag so smartstore can continue working */
        u_long dmnState = cfsState;
        ss_change_state(dmnP->domainName, SS_ACTIVATED, &dmnState, 0);
    }

    if (fragBfClosed) {
        (void) bs_fragbf_open(setp);
    }

    if (restart_quotas) {
        quota_activate( fsnp );
    }

    if (!advfs_shutting_down) {
        mutex_lock(&dmnP->dmnFreezeMutex);
        dmnP->dmnFreezeRefCnt--;
        if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
            thread_wakeup( (vm_offset_t)&dmnP->dmnFreezeWaiting );
        }
        mutex_unlock(&dmnP->dmnFreezeMutex);
    }

    return error;
}

/*
 * msfs_root
 *
 * return the vnode of the root
 * Note that the root is left accessed for the entire time
 * that the file system is mounted.
 */
msfs_root(
          struct mount *mp,  /* in - the mount structure pointer */
          struct vnode **vpp /* out - the vnode of the root */
          )
{
    /*
     * just read the vp out of the fileSetNode struct
     * and vref it
     */

    *vpp = NULL;
    VREF(GETROOTVNODE(mp));
    *vpp = GETROOTVNODE(mp);
    return(0);
}

/*
 * Do operations associated with quotas
 */
int
advfs_quotactl(
        struct mount *mp,
        int cmds,
        uid_t uid,
        caddr_t arg)
{
    int cmd, type, error, i, kernaddr;
    int root_check = TRUE;  /* Most commands require root user check */
    uid_t ruid;
    struct fileSetNode *dnp = GETFILESETNODE(mp);
    struct ucred *credp = u.u_cred;
    char *setName=NULL;
    char *dmnName=NULL;
    char uid_str[20];
    advfs_ev *advfs_event;

    /*
     * In base system :
     *    Called via the VFS layer on behalf of a quotactl() system
     *    issued from user-level.
     *    In this case, argument addresses will always be user-space
     *    and "command" will be whatever user coded.
     *    Must reject "*_INT" commands since they assume addresses are
     *    kernel-space.
     *
     * In a cluster :
     *    In the case of a quotactl() system call issued from user level,
     *    we will always go thru cfs_quotactl() which will reject all "_INT" 
     *    commands immediately.
     *    
     *    We can get here within the context of a quotactl() system call
     *    if and only if in client-at-server case.
     *    In this case, we will have come thru cfs_quotactl() and
     *    argument addresses will always be user-space and command
     *    will never be "*_INT" (same as base system).
     *
     *    We can also get here if processing a quota request from a remote client
     *    (due to either quotactl() on a client or cfs_dqget() from a client)
     *    or when handling an internally generated (i.e. cfs_dqget) client-at-server 
     *    quota request.
     *    In these cases, cluster code guarantees that command will always be 
     *    a "*_INT" and argument addresses will always be kernel space.
     */

    dnp->mountp = mp;   /* XXX should this be initialized in a
        more general place?  Is a lock needed? */

    /*
     * screen domain panic
     */
    if ((GETDOMAINP(mp))->dmn_panic) {
        return EIO;
    }

    ruid = credp->cr_ruid;

    /*
     * Note: uid could be either user ID or group ID, depending on cmds.
     */
    if (uid == -1)
        uid = ruid;

    cmd = cmds >> SUBCMDSHIFT;
    type = cmds & SUBCMDMASK;

    if (cmd == Q_DQGETQUOTA64) {
            return (EINVAL);
    }

    if (cmd & Q_INT) {
        if (!clu_is_ready()) {
            return (EINVAL);
        } else
            kernaddr = 1;
    } else
        kernaddr = 0;
    
    cmd &= ~Q_INT;

    if (cmd == Q_DQGETQUOTA64) {
            root_check = FALSE;
    }
    else if (cmd == Q_SYNC) {
        if (uid == ruid)
            root_check = FALSE;
    }
    else if ((cmd == Q_GETQUOTA) || (cmd == Q_GETQUOTA64)) {
        if (type == USRQUOTA) {
            if (uid == ruid)
                root_check = FALSE;
        }
        else {  /* type == GRPQUOTA */
            if (groupmember(uid, credp))
                root_check = FALSE;
        }
    }

    if (root_check) {
        if (error = suser(credp, &u.u_acflag)) {
            return (error);
        }
    }

    if ((u_int)type >= MAXQUOTAS) {
        return( EINVAL );
    }

    dmnName = ms_malloc( MAX_MNT_PATHLEN );
    setName = toke_it( mp->m_stat.f_mntfromname, '#', dmnName );

    advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
    advfs_event->domain = dmnName;
    advfs_event->fileset = setName;

    switch (cmd) {

        case Q_QUOTAON:
            error = advfs_enforce_on(dnp, type);
            advfs_post_kernel_event(EVENT_QUOTA_ON, advfs_event);
            break;

        case Q_QUOTAOFF:
            error = advfs_enforce_off(dnp, type, FtxNilFtxH);
            advfs_post_kernel_event(EVENT_QUOTA_OFF, advfs_event);
            break;

        case Q_SETQUOTA:
            error = advfs_set_quota(dnp, uid, type, QUOTA32, arg, kernaddr);
            sprintf(uid_str, "%u", uid);
            if (type == GRPQUOTA) {
                advfs_event->group = uid_str;
                advfs_post_kernel_event(EVENT_QUOTA_SETGRP, advfs_event);
            } else if (type == USRQUOTA) {
                advfs_event->user = uid_str;
                advfs_post_kernel_event(EVENT_QUOTA_SETUSR, advfs_event);
            }
            break;

        case Q_SETUSE:
            error = advfs_set_use(dnp, uid, type, QUOTA32, arg, kernaddr);
            break;

        case Q_GETQUOTA:
            error = advfs_get_quota(dnp, uid, type, QUOTA32, arg, kernaddr);
            break;

        case Q_QUOTAINFO:
            error = advfs_get_quota_info(dnp, arg, kernaddr);
            break;

        case Q_SETQUOTA64:
            error = advfs_set_quota(dnp, uid, type, QUOTA64, arg, kernaddr);
            sprintf(uid_str, "%u", uid);
            if (type == GRPQUOTA) {
                advfs_event->group = uid_str;
                advfs_post_kernel_event(EVENT_QUOTA_SETGRP, advfs_event);
            } else if (type == USRQUOTA) {
                advfs_event->user = uid_str;
                advfs_post_kernel_event(EVENT_QUOTA_SETUSR, advfs_event);
            }
            break;

        case Q_SETUSE64:
            error = advfs_set_use(dnp, uid, type, QUOTA64, arg, kernaddr);
            break;

        case Q_GETQUOTA64:
        case Q_DQGETQUOTA64:
            error = advfs_get_quota(dnp, uid, type, QUOTA64, arg, kernaddr);
            break;

        case Q_SYNC:
            error = advfs_quota_sync(dnp);
            break;

        default:
            error = EINVAL;
            break;
    }

    ms_free(advfs_event);
    ms_free(dmnName);
    return( error );
}


/*
 * msfs_statfs
 *
 * update file system statistics.
 *
 * Synchronization assumptions:
 *      -- File system isn't going anywhere.
 */

msfs_statfs( struct mount *mp )
{
    register struct nstatfs *sbp;
    struct fileSetNode *dn;
    domainT *dmnp;
    bfSetT *bfSetp;
    long freeFiles;
    long filesUsed;
    long freeBlocks;
    long availBlocks;
    long maxFiles;
    long totalBlocks;

    sbp = &mp->m_stat;

    dn = (struct fileSetNode *)mp->m_info;      /* fileset specific info */
    bfSetp = dn->bfSetp;
    dmnp = GETDOMAINP(mp);                      /* domain wide info */

    /*
     * If there are quotas established for this file set, then use them,
     * otherwise use the number of blocks in the domain.
     */
    if ((dn->quotaStatus & QSTS_QUOTA_ON) &&
        (dn->quotaStatus & QSTS_FS_QUOTA_MT)) {
        if (dn->blkSLimit > 0 && dn->blkHLimit > 0) {
                totalBlocks = MIN( dn->blkSLimit, dn->blkHLimit );
        } else if (dn->blkSLimit > 0) {
                totalBlocks = dn->blkSLimit;
        } else if (dn->blkHLimit > 0) {
                totalBlocks = dn->blkHLimit;
        } else {
                totalBlocks = dmnp->totalBlks;  /* no fset limit; use dmn's */
        }
    } else { /* Quotas not turned on */
        totalBlocks = dmnp->totalBlks;  /* no quotas, use domain */
    }

    /*
     * If there are quota values, then use them to figure out the maximum
     * number of files.  If not, then guess at the maximum number by guessing
     * at the maximum number of Mcells that could exist.  This work is done
     * outside of the MOUNT_LOCK() so that we don't panic the system with
     * locking violation in lockmode=4.
     */
    if ((dn->quotaStatus & QSTS_QUOTA_ON) &&
        (dn->quotaStatus & QSTS_FS_QUOTA_MT)) {
        if (dn->fileSLimit > 0 && dn->fileHLimit > 0) {
                maxFiles = MIN( dn->fileSLimit, dn->fileHLimit );
        } else if (dn->fileSLimit > 0) {
                maxFiles = dn->fileSLimit;
        } else if (dn->fileHLimit > 0) {
                maxFiles = dn->fileHLimit;
        } else {
                maxFiles = bs_get_avail_mcells( dmnp );
        }
    } else {
        maxFiles = bs_get_avail_mcells( dmnp );
    }

    /*
     * Do calculations before taking out MOUNT_LOCK. Note,
     * since no lock is being held on the filesetNode and domain
     * their values aren't synchronized, so we handle the possibility
     * that subtractions between their values might go negative
     */
    if ((dn->quotaStatus & QSTS_QUOTA_ON) &&
        (dn->quotaStatus & QSTS_FS_QUOTA_MT))
                filesUsed = dn->filesUsed;
    else
                filesUsed = bfSetp->bfCnt;

    freeFiles = MAX( 0, maxFiles - filesUsed );
    freeBlocks = MAX( 0, totalBlocks - dn->blksUsed );
    /* Note: UFS would allow availBlocks to go negative because UFS has the */
    /*       concept of reserved space that may only be used by root and */
    /*       available blocks reflects the amount of storage that anyone can */
    /*       use.  Allowing UFS available blocks to go negative is how UFS */
    /*       can report greater than 100% of the space used in 'df'.  Advfs */
    /*       does not have this concept, hence the check for a negative value */
    availBlocks = MIN( freeBlocks, dmnp->freeBlks );
    availBlocks = MAX( 0, availBlocks );

    /*
     * set all of the firm values in the mount structure without locking
     */
    sbp->f_type = MOUNT_MSFS;           /* AdvFS */
    sbp->f_fsize = BS_BLKSIZE;          /* basic block size */
    sbp->f_bsize = 8192;                /* optimum xfer size */

    /*
     * NFS uses the f_fsid.val fields to contruct the file system
     * identifier portion of a file handle. The file system id is
     * 64 bits long. Two guarantees must hold true:
     *          Must be persistant across reboots
     *          Must be unique within the system mount table
     *
     * The persistance guarantee is met by using a unique random
     * number (stored in the file set attribute record) for the
     * file set dev (fsid.val[0]). This number is set at mount time,
     * and checked for uniqueness. If the file set dev is not unique,
     * a message is printed, indicating stale file handles will occur,
     * and the file set is assigned an unique dev. Refer to chk_set_fsdev().
     *
     * NOTE: it is assumed that fsid.val[0] is a unique dev for
     * the mount point/fileset. Applications/libraries assume that
     * fsid.val[0] is unique across the system, and files in the
     * mount point/fileset return the same value in st_dev.
     */

    BS_GET_FSID(bfSetp,sbp->f_fsid);

    sbp->mount_info.msfs_args.id.id1 = dn->bfSetId.domainId.tv_sec;
    sbp->mount_info.msfs_args.id.id2 = dn->bfSetId.domainId.tv_usec;
    sbp->mount_info.msfs_args.id.tag = BS_BFTAG_IDX(dn->bfSetId.dirTag);

    /*
     * Lock the mount structure while updating the changing offsets of the
     * mount.m_stat structure so that the update is atomic.  All data
     * collection which may have taken out additional locks were performed
     * first, prior to the MOUNT_LOCK().
     */
    MOUNT_LOCK(mp);

    sbp->f_blocks = totalBlocks;
    sbp->f_bfree = freeBlocks;
    sbp->f_bavail = availBlocks;

    sbp->f_files = maxFiles;
    sbp->f_ffree = freeFiles;

    MOUNT_UNLOCK(mp);

    return (EOK);
}


u_int last_sync;        /* time in seconds of last rootfs sync */
#define SECPERHOUR (60*60)
/*
 * msfs_sync
 *
 */

int AdvfsSyncMmapPages = 1;

msfs_sync(
          struct mount *mp, /* in - mount structure pointer */
          int waitfor       /* in - flags */
          )
{
    struct fileSetNode *fsnp;
    domainT *dmnP;
    extern u_int smsync_period;

    /*
     * if read-only file set just return
     */
    fsnp = (struct fileSetNode *)mp->m_info;
    if (fsnp->fsFlags & FS_CLONEFSET || mp->m_flag & M_RDONLY) {
        goto out;
    }

    /*
     * screen domain panic
     * sync required for unmount, we fake it and return success
     */
    dmnP = GETDOMAINP(mp);
    if (dmnP->dmn_panic) {
        goto out;
    }

    /*
     * If the system is not in a normal running state, just return as
     * we are likely to block on a lock or transaction if it happened
     * in the file system.
     */
    if ( panicstr ) {
        /* Flush all dirty file/dir buffers */
        bs_bfdmn_flush_bfrs( dmnP );
        goto out;
    }

    /*
     * If smoothsync is active, let the smoothsync thread handle these
     * operations.  Also do these if MNT_WAIT is specified.
     */
    if (smsync_period == 0 || waitfor == MNT_WAIT) {
        /*
         *  Don't run if domain is frozen.
         *  Take the dmnFreezeMutex to check status and to
         *  increment the ref count to prevent a new freeze
         *  from starting if we begin sync'ing.
         */
        mutex_lock(&dmnP->dmnFreezeMutex);
        if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            goto out;
        } else {
            dmnP->dmnFreezeRefCnt++;
            mutex_unlock(&dmnP->dmnFreezeMutex);
        }

        /*
         * Update last modify time for initodr/resettodr
         */
        (void)msfs_sync_todr(mp, waitfor);

        /*
         * Flush the buffers.
         */
        bs_bfdmn_flush_bfrs( dmnP );

        /* Sync dirty mmap'ed pages. */
        (void)msfs_sync_mmap(mp);

        /* Move this here from above msfs_sync_todr() to guard against a
         * dangling log record write to disk from the msfs_sync_mmap()/
         * fs_update_stats() path
         */
        lgr_flush( dmnP->ftxLogP );

        mutex_lock(&dmnP->dmnFreezeMutex);
        dmnP->dmnFreezeRefCnt--;

        if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
            thread_wakeup( (vm_offset_t)&dmnP->dmnFreezeWaiting );
        }
        mutex_unlock(&dmnP->dmnFreezeMutex);
    }

out:
    return(0);
}


/*
 * "sync" operations which are not "smooth-sync'ed":
 *    sync dirty mmap'ed pages
 *    update last modify time for initodr/resettodr
 *
 * Called from msfs_sync() and msfs_smoothsync()
 */
static void
msfs_sync_mmap(
              struct mount *mp  /* in - mount structure pointer */
              )
{
    struct vnode *vp = NULL, *nvp = NULL;
    struct fsContext *contextp = NULL;
    bfAccessT *bfap;
    struct bfNode *bnp;
    bfSetT *bfSetp;
    int diskFile = FALSE;
    extern fsFragStateT bs_get_bf_fragState();
    extern bfAccessT *find_bfap();
    struct bsBuf *bp, *bptmp;
    int i;
    MOUNT_VLIST_LOCK(mp);

    for (vp = mp->m_mounth; vp; vp = nvp) {
        /*
         * nvp will hold the vnode pointer to the vnode we're
         * flushing, which could be different from vp, the one
         * that's on the mount vnode list, in the case of VBLK.
         */
        VN_LOCK(vp);

        contextp = VTOC( vp );

        /*
         * Since contextp is freed in msfs_reclaim(), VN_LOCK and following
         * check only guarantee contextp as long as VN_LOCK is held.  Must
         * have a vget() prior to releasing VN_LOCK(vp) if contextp is to be
         * used without VN_LOCK. Also, skip this vnode if it has been opened
         * for directio since that code will flush any pages, if required,
         * before they are invalidated and removed from the cache.
         */
        if ((contextp == NULL) || (vp->v_flag & (VXLOCK | VDIRECTIO))) {
            VN_UNLOCK(vp);
            nvp = vp->v_mountf;
            continue;
        }

        diskFile = vp->v_type == VDIR ||
                   vp->v_type == VREG ||
                   vp->v_type == VLNK;

        if (vp->v_type == VBLK) {
            nvp = shadowvnode(vp);

            if (nvp == NULLVP) {
                VN_UNLOCK(vp);
                nvp = vp->v_mountf;
                continue;
            } else {
                ; /* for break point */
            }
        } else {
            nvp = vp;
        }

        /*
         * Since this is "sync" related processing, if this vnode has no
         * dirty data, then avoid the vget/vrele.  vget/vrele calls on a
         * clean and unreferenced vnode will undesirably update the LRU
         * position of the vnode on the vnode freelist.  This in turn
         * completely thwarts the vnode deallocation logic.
         */
        if ( (!contextp->dirty_stats)                               &&
             ( !VTOA(vp) || (VTOA(vp)->fragState == FS_FRAG_NONE) ) &&
             (vp->v_object)                                         &&
             (vp->v_object->vu_dirtypl == NULL)                     &&
             (vp->v_object->vu_dirtywpl == NULL) ) {

            VN_UNLOCK(vp);
            nvp = vp->v_mountf;
            continue;
        }

        /*
         * Use vget() to grab the vnode to prevent reclaim so contextp
         * will remain valid.
         */
        if (vget_nowait(vp)) {
            /* vnode not ours anymore; skip it */
            VN_UNLOCK(vp);
            nvp = vp->v_mountf;
            continue;
        }

        VN_UNLOCK(vp);
        MOUNT_VLIST_UNLOCK(mp);

        /* get bfap for this vnode */
        if (bfap = VTOA(vp)) {

            /* Take the access struct off the free list and bump
             * its refCnt.
             */
            mutex_lock(&bfap->bfaLock);
            RM_ACC_LIST_COND(bfap);
            bfap->refCnt++;
            mutex_unlock(&bfap->bfaLock);
        } else {
            bnp = (struct bfNode *)&vp->v_data[0];
            bfSetp = bs_bfs_lookup_desc( bnp->bfSetId );
            bfap = find_bfap(bfSetp, bnp->tag, FALSE, NULL);
            if (bfap ) {
                /* Take the access struct off the free list and bump
                 * its refCnt.
                 */
                RM_ACC_LIST_COND(bfap);
                bfap->refCnt++;
                mutex_unlock(&bfap->bfaLock);
            }
        }

        /*
         * Syncing out mmap pages.
         *
         * If the file is valid, not reserved, and has been mmapped, 
         * then call ubc_flush_dirty() to flush any pages on the 
         * object's dirty list.
         *
         * All dirty pages will be on the vm object's dirty list.
         * Check for a non-null dirty list in the object, and call
         * ubc_flush_dirty() to write them out asynchronously.
         * ubc_flush_dirty will call msfs_putpage(), which
         * will place pages on the flushQ or ubcReqQ, but will
         * not block for completion.
         *
         * SMP - still have refcnt bumped for bfap; no bfaLock held.
         */
        if (AdvfsSyncMmapPages && bfap && (vp == bfap->bfVp) &&
            (bfap->bfState == BSRA_VALID) &&
            !BS_BFTAG_RSVD(bfap->tag) &&
            ((clu_is_ready() && (vp->v_flag & VMMAPPED)) ||
             (!clu_is_ready() && bfap->mmapFlush)) &&
            (vp->v_type == VREG || vp->v_type == VNON) &&
            vp->v_object &&
            ((vm_ubc_object_t)(vp->v_object)->vu_dirtypl != NULL ||
            (vm_ubc_object_t)(vp->v_object)->vu_dirtywpl != NULL)) {

            (void)ubc_flush_dirty( (vm_ubc_object_t)vp->v_object, 0);

            /* 
             * Check to see if we can clear the mmapFlush flag; only
             * do this if the file is no longer mmapped.
             *
             * Note that bfap->mmapCnt and bfap->mmapFlush are
             * not maintained in a cluster.  In a cluster, a file 
             * originally mmap'ed on the server of its fileset will
             * be flushed by this routine (if dirty) as long as
             * VMMAPPED is set (cleared at cfs_inactive() time).
             * I.e., VMMAPPED may still be set even after the file has
	     * been unmapped, but only until cfs_inactive().
             */

            if (!clu_is_ready()) {
                mutex_lock(&bfap->bfaLock);
                if ( bfap->mmapCnt == 0 ) {
                    bfap->mmapFlush = 0;
                }
                mutex_unlock(&bfap->bfaLock);
            }
        }

        if ((!contextp->dirty_stats) &&
            (VTOA(vp)) &&
            ((VTOA(vp))->fragState == FS_FRAG_NONE)) {

           /*
            * file stats are not dirty and the file has no frag
            * and no dirty mmap pages (anymore).
            *
            * nothing left to flush for this vnode; move on
            */
            nvp = NULL;
            goto getnext;
        } else {
            VN_LOCK(nvp);
        }

        /* use vget() to grab the shadowvnode so it can't go away */
        if ((vp != nvp) && vget_nowait(nvp)) {
            /* vnode not ours anymore; skip it */
            VN_UNLOCK(nvp);
            nvp = NULL;           /* so we don't touch it later */
            goto getnext;
        }


        if (diskFile || (nvp->v_dirtyblkhd == NULL)) {
            VN_UNLOCK(nvp);
        } else {
            /* device special file needs to be flushed */
            VN_UNLOCK(nvp);
            vflushbuf(nvp, 0);
        }

        if (contextp->dirty_stats) {
            if (bfap && (vp == bfap->bfVp) && (bfap->bfState == BSRA_VALID)) {
                (void) fs_update_stats(vp, bfap, FtxNilFtxH, 0);
            }
        }

getnext:
        /* Before proceeding on to the next vnode, decrement the refCnt
         * on the bfap that we are working with, if any, and vrele its
         * associated vnode.  This order is done explicitly to prevent
         * potential race problems with vnode invalidation and reclamation.
         */
        if (bfap) {
            mutex_lock(&bfap->bfaLock);
            DEC_REFCNT(bfap);
            mutex_unlock(&bfap->bfaLock);
        }
        vrele(vp);
        if (nvp && vp != nvp )
            vrele(nvp);

        MOUNT_VLIST_LOCK(mp);

        if (vp->v_mount == mp) {
            nvp = vp->v_mountf;
        } else {
            nvp = mp->m_mounth;
        }
    }
    MOUNT_VLIST_UNLOCK(mp);
}


/*
 * Update last modify time for initodr/resettodr, only for rootfs
 * or if this is a request for sync-i/o (i.e. MNT_WAIT);
 * this heartbeat provides a sanity-check on the battery-backed clock.
 */
static int
msfs_sync_todr(
               struct mount *mp, /* in - mount structure pointer */
               int waitfor       /* in - flags */
               )
{
    int now_time;
    extern int advfs_shutting_down;             /* global in machdep.c */
    bfAccessT *root_accessp;
    statusT sts = EOK;
    struct mount *pmp;

    if (clu_is_ready()) {
        (void)CC_GETPFSMP(rootfs, &pmp);
        if (mp != pmp)
            (void)CC_GETPFSMP(bootfs, &pmp);
    } else
        pmp = rootfs;

    if ((pmp == mp) || (waitfor == MNT_WAIT)) {
        now_time = time.tv_sec;

        if ( (waitfor == MNT_WAIT) ||                   /* on dismount or  */
             ((pmp == mp) &&
             ((advfs_shutting_down) ||                  /* rootfs on shutdown */
             (now_time >= (last_sync + SECPERHOUR))))){ /*  or hourly basis */
            last_sync = now_time;
            root_accessp = GETROOTACCESS( mp );

            sts = bmtr_put_rec(root_accessp, BMTR_FS_TIME,
                               &now_time, sizeof(now_time),
                               FtxNilFtxH);
            if (sts != EOK) {
                /* likely i/o error; ignore it; update not critical */
                aprintf("msfs_sync: bmtr_put_rec error %s\n",
                BSERRMSG( sts ) );
            }
        }
    } /* (rootfs == mp) || waitfor */


    return(sts);
}



/*
 * msfs_fhtovp
 *
 * Given a file handle, open the file and return a vnode pointer
 */
msfs_fhtovp(
            struct mount *mp,  /* in - mount structure pointer */
            struct fid *fhp,   /* in - pointer to file handle sturcture */
            struct vnode **vpp /* out - vnode pointer of file */
            )
{
    struct vnode *found_vp = NULL;
    statusT ret;
    register struct msfs_fid *i_fid;
    int flag = DONT_UNLOCK|IGNORE_DELETE;

    i_fid = (struct msfs_fid *)fhp;
    *vpp = NULL;
/*    printf(" tag = %d  ", BS_BFTAG_IDX(i_fid->msfs_fid_tag));
    if (BS_BFTAG_IDX(i_fid->msfs_fid_tag) == 256) {
        printf("stop\n");
    }
*/
    /*
     * screen domain panic
     */
    if ((GETDOMAINP(mp))->dmn_panic) {
        return EIO;
    }

    /*
     * If the tag is a pseudo-tag, we are opening a metadata file.
     */
    if (BS_BFTAG_PSEUDO(i_fid->msfs_fid_tag)) {
        flag |= META_OPEN;
    }

    ret = bf_get(
                 i_fid->msfs_fid_tag,
                 0,
                 flag,
                 mp,
                 &found_vp
                 );

    if (ret != EOK) {
        return(BSERRMAP(ret));
    }

    *vpp = found_vp;
    return(0);
}

/*
 * msfs_vptofh
 *
 * given a vnode pointer, return a file handle
 */
msfs_vptofh(
            struct vnode *vp, /* in - vnode pointer */
            struct fid *fhp   /* in out - pointer to file handle to fill in */
            )
{
    register struct msfs_fid *i_fid;
    struct bfNode *bnp;
    struct fsContext *context_ptr;

    i_fid = (struct msfs_fid *)fhp;
    i_fid->msfs_fid_len = sizeof(struct msfs_fid);
    bnp = (struct bfNode *)&vp->v_data[0];
    context_ptr = bnp->fsContextp;
    i_fid->msfs_fid_tag = bnp->tag;

    return (0);
}

int
msfs_init(void)
{
    void bs_kernel_pre_init();

    lock_setup(&FilesetLock, ADVFilesetLock_lockinfo, TRUE);

    bs_kernel_pre_init();
    quota_init();
    advfs_range_init();

    return( 0 );
}

/*
 * advfs_range_init
 *
 * This routine creates a virtual address range.  No physical pages of
 * memory have been allocated and no page table entries have been created.
 *
 * Removing the use of this virtual address range will not save any memory.
 */
void
advfs_range_init(void)
{

    int         pg;
    vm_offset_t phys;

    /*
     *  Set up an AdvFS virtual memory address range.  This will be used
     *  in fs_zero_fill_pages to optimize the zeroing of pages.  If the
     *  value of virtualZeroRangeP is NULL, we will know that the setup has
     *  failed (it never should) and we will not attempt the optimization.
     *
     *  The trick is to allocate some virtual memory page table entries, and
     *  then populate each entry with the _SAME_ physical page (in this case
     *  we will use the vm_kern_zero_page that is used by the /dev/zero
     *  device, so we do not even need to allocate any of our own memory).
     *  The end result will be a few megabytes of virtual address range that
     *  when used as the write buffer in fs_zero_fill_pages() will always
     *  write out zeros.  And the cost is just a few page table entries.
     */
    virtualZeroRangeP = vm_alloc_kva( MAX_VIRT_VM_PAGE_RNG * ADVFS_PGSZ );
        if ( virtualZeroRangeP == NULL ) {
            /* This should never occur.  */
            MS_SMP_ASSERT(0);   /* We should not get here. */
            return;
        } else {
            if (pmap_svatophys(vm_kern_zero_page, &phys) != KERN_SUCCESS) {
                /*
                 * If we did not convert vm_kern_zero_page's virtual address
                 * to a physical address, we will not use a virtual memory
                 * address range.
                 */
                MS_SMP_ASSERT(0);   /* We should not get here. */
                virtualZeroRangeP = NULL;
                return;
            }
        }

    /* Zero each virtial page. */
    for (pg = 0; pg < MAX_VIRT_VM_PAGE_RNG * ADVFS_PGSZ ; pg += PAGE_SIZE) {
        pmap_enter( kernel_pmap,
                    virtualZeroRangeP + pg,
                    phys,
                    VM_PROT_DEFAULT,
                    0,
                    VM_PROT_DEFAULT);
    }

    return;
}

/*
 * Called at system shutdown time with AdvFS root. Note this routine
 * will return the number of outstanding I/O's for the mount points
 * domain.
 */
int
msfs_mntbusybuf(
                struct mount *mp
                )
{
    domainT *dmnP;
    int ind, vols_found;
    struct vd *vdp;
    int numout = 0;
    int i;

    dmnP = GETDOMAINP(mp);

    /*
     * Now we have the mount points domain structure.
     * Loop through it's virtual disks and count number
     * of outstanding I/O's, plus I/O requests still queued
     * up in any of the accessible queues (i.e. not the waitQ).
     */
    for ( vols_found = 0, ind = 1;
         (vols_found < dmnP->vdCnt) && (ind <= BS_MAX_VDI);
         ind++) {

        int num_this_vol = 0;

        /* Only one thread running when this is called, so no need to
         * bump the vdRefCnt.
         */
        if ( vdp = vd_htop_if_valid(ind, dmnP, FALSE, FALSE) ) {
            
            numout += vdp->vdIoOut;

            /* Check to see if any on the higher I/O queues that must
             * also be output.  If so, be sure that they get moved down
             * to the devQ.
             */
            num_this_vol += vdp->waitLazyQ.ioQLen;
            num_this_vol += vdp->readyLazyQ.ioQLen;
            num_this_vol += vdp->blockingQ.ioQLen;
            num_this_vol += vdp->consolQ.ioQLen;
            num_this_vol += vdp->flushQ.ioQLen;
            num_this_vol += vdp->ubcReqQ.ioQLen;
            num_this_vol += vdp->smSyncQ0.ioQLen;
            num_this_vol += vdp->smSyncQ1.ioQLen;
            num_this_vol += vdp->smSyncQ2.ioQLen;
            num_this_vol += vdp->smSyncQ3.ioQLen;
            num_this_vol += vdp->smSyncQ4.ioQLen;
            num_this_vol += vdp->smSyncQ5.ioQLen;
            num_this_vol += vdp->smSyncQ6.ioQLen;
            num_this_vol += vdp->smSyncQ7.ioQLen;
            num_this_vol += vdp->smSyncQ8.ioQLen;
            num_this_vol += vdp->smSyncQ9.ioQLen;
            num_this_vol += vdp->smSyncQ10.ioQLen;
            num_this_vol += vdp->smSyncQ11.ioQLen;
            num_this_vol += vdp->smSyncQ12.ioQLen;
            num_this_vol += vdp->smSyncQ13.ioQLen;
            num_this_vol += vdp->smSyncQ14.ioQLen;
            num_this_vol += vdp->smSyncQ15.ioQLen;
            if ( num_this_vol )
                bs_bflush(vdp);

            numout += num_this_vol;  /* Include these in count for caller */

            vols_found++;
        }
    }
    if (vols_found != dmnP->vdCnt) {
        ms_uprintf("msfs_mntbusybuf:  Found %d disks; expected to find %d.\n",
                vols_found, dmnP->vdCnt);
    }
    return(numout);
}

/*
 * Called by boot() through mntflushbuf() when system has panic()ed.
 * Flush buffers and log without waiting.  (Code for a single domain
 * modeled on bs_bfdmn_flush_all().)
 */
void
msfs_mntflushbuf(
    struct mount *mp
                 )
{
    domainT *dmnP = GETDOMAINP(mp);

    if (dmnP->ftxLogP && dmnP->state == BFD_ACTIVATED) {
        /*
         * Flush the log.  If a log flush would block, lgr_flush_start()
         * will return E_WOULD_BLOCK.  Not checking the return because
         * we don't want to try anything else in that case - panic with
         * busy logger.
         */
        (void) lgr_flush_start( dmnP->ftxLogP, TRUE, nilLSN );
    }

    bs_bfdmn_flush_bfrs( dmnP );
}

/*
 * chk_set_fsdev()
 * Optionally verifies the file set has a unique file set device and
 * creates a new one if not. Then the file set attributes are updated on
 * disk with the new file set device identifier.
 *
 */
static statusT
chk_set_fsdev (
    struct mount *mp,  /* In: mount point to perform fsdev update. */
    int fsId,          /* In: fsdev to use. Set to 0 if check_flag false.*/
    int check_flag     /* In: TRUE, verify and create new fsdev, if needed.*/
                       /*     FALSE, just update the fsdev to disk. */
    )
{
    statusT sts;
    bsBfSetAttrT fsAttr_rec;
    bfSetT *bfSetp;
    ftxHT ftxH;
    int newId = fsId, setNewId = 0;
    int do_update = 0;
    struct nstatfs *sbp;
    int failover = mp->m_flag & M_FAILOVER ? 1 : 0;

    bfSetp = GETBFSETP(mp);

    sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                            BSR_BFS_ATTR,
                            &fsAttr_rec,
                            sizeof(bsBfSetAttrT),
                            BMTR_NO_LOCK
                            );
    if (sts != EOK) {
        ms_uprintf("ADVFS: Update of file set dev record failed for %s\n",
                   mp->m_stat.f_mntfromname);
        ms_uprintf("ADVFS: This will result in stale NFS file handles\n");
        ms_uprintf("ADVFS: when this file set is re-mounted,\n");
        ms_uprintf("ADVFS: forcing NFS clients to re-mount.\n");
        return sts;
    }

    /* Either "check and set" or just "set" the file set device. */

    if (check_flag || failover) {
        /*
         * If the file set does not have an assigned dev OR
         * current dev is not unique, assign newId to file set dev.
         * newId is guaranteed unique across system.
         */

        if (!(fsAttr_rec.fsDev) ||
            chk_unique_fsdev(fsAttr_rec.fsDev, &newId, bfSetp, failover)) {

            do_update = 1;
        }

        /*
         * The only time do_update should be set upon failover
         * is if the on-disk fsid is 0.  This will be the case
         * if the system failed before the new fsid was written
         * to disk, but the in-memory fsid was already updated.
         */

    }
    else {
        if (fsAttr_rec.fsDev != (int)bfSetp->dev)
            do_update = 1;
    }

    /* Update disk version if necessary. */
    if (do_update) {
        /*
         * We have to start the transaction before locking the mcell
         * list for ordering consistency.
         *
         */

        sts = FTX_START_N( FTA_BS_BMT_UPDATE_REC_V1, &ftxH, FtxNilFtxH,
                          bfSetp->dmnP, 1 );
        if (sts != EOK) {
            ADVFS_SAD1( "set_unique_fsdev : ftx start failed", sts );
        }

        sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                                BSR_BFS_ATTR,
                                &fsAttr_rec,
                                sizeof(bsBfSetAttrT),
                                BMTR_LOCK
                                );
        if (sts != EOK) {
            ftx_fail( ftxH );
            ms_uprintf("ADVFS: Update of file set dev record failed for %s\n",
                       mp->m_stat.f_mntfromname);
            ms_uprintf("ADVFS: This will result in stale NFS file handles\n");
            ms_uprintf("ADVFS: when this file set is re-mounted,\n");
            ms_uprintf("ADVFS: forcing NFS clients to re-mount.\n");
            return sts;
        }

        /* Assign the file set device into the attributes record. */

        if (failover && !fsAttr_rec.fsDev) {

            /*
             * This should be the case if the new fsid was not
             * written to disk before the system went down, but
             * the in-memory fsid was already updated.
             */

            fsAttr_rec.fsDev = fsId;
        } else if (check_flag) {
            /* re-check holding the mcell list lock. */
            setNewId = chk_unique_fsdev( fsAttr_rec.fsDev,
                                         &newId,
                                         bfSetp,
                                         failover);

            if ((setNewId) && !(mp->m_flag & M_DUAL)) {
                ms_uprintf("ADVFS: Assigning a unique fileset dev for %s\n",
                    mp->m_stat.f_mntfromname);
                ms_uprintf("ADVFS: This will result in stale NFS file handles\n");
                ms_uprintf("ADVFS: when this file set is re-mounted,\n");
                ms_uprintf("ADVFS: forcing NFS clients to re-mount.\n");
            }
            fsAttr_rec.fsDev = newId;
        } else {
            fsAttr_rec.fsDev = (int)bfSetp->dev;
        }

        /*
         * If bmtr_update_rec_n_unlk() is succesful,
         * the mcell lock is released, but the transaction
         * is not done. Upon failure, we have to fail the
         * transaction, and release the lock.
         */

        sts = bmtr_update_rec_n_unlk(bfSetp->dirBfAp,
                                     BSR_BFS_ATTR,
                                     &fsAttr_rec,
                                     sizeof(bsBfSetAttrT),
                                     ftxH,
                                     BMTR_UNLOCK );
        if (sts != EOK) {
            MCELLIST_UNLOCK( &bfSetp->dirBfAp->mcellList_lk );
            ftx_fail( ftxH );
            ms_uprintf("ADVFS: Update of file set dev record failed for %s\n",
                       mp->m_stat.f_mntfromname);
            ms_uprintf("ADVFS: This will result in stale NFS file handles\n");
            ms_uprintf("ADVFS: when this file set is re-mounted,\n");
            ms_uprintf("ADVFS: forcing NFS clients to re-mount\n");
            return sts;
        } else {
            ftx_done_n( ftxH, FTA_BS_BMT_UPDATE_REC_V1 );
        }
    }

    bfSetp->dev = (dev_t)fsAttr_rec.fsDev;
    sbp = &mp->m_stat;
    sbp->f_fsid.val[0] = bfSetp->dev;

    return EOK;
}

/*
 * Loop through all mounted file sets and verify file set
 * dev is unique. Returns 1 if not unique, zero otherwise.
 * Also ensures newId is unique.
 */
static int
chk_unique_fsdev (
    dev_t curId,        /* in */
    int *newId,         /* in/out */
    bfSetT *setp,       /* in */
    int failover        /* in */
    )
{
    struct fileSetNode *fsp;
    bfSetT *bfSetp;
    int reset = 0;

    /*
     * If the newId passed in is 0, assign a newId.  Zero is not a good 
     * choice for newId since chk_set_fsdev() assumes a 0 means a new
     * fileset.
     */
    if (*newId == 0) {
        *newId += (int)time.tv_usec;
    }

    /*
     * Set high bit to ensure no overlap with existing dev_t's
     */
    if (*newId > 0) {
        *newId = 0 - *newId;
    }

    /*
     * For cfs, the filesystem id must be cluster-wide unique, so we check
     * for mount list which will be cluster wide in WAVE 4, and the CFS fsid
     * will be PFS fsid (first word).
     * TO CHANGE: the problem now is due to the failover requirement, we
     * can't walk through the mount list to ensure the uniqueness. this part
     * of the code will be removed once the new algorithm of
     * cluster-wide fsid is in place.
     */

    if (!clu_is_ready()) {
        FILESET_WRITE_LOCK(&FilesetLock );
    }

loop:
    if (clu_is_ready()) {
        fsid_t fsid;

        fsid.val[1] = MOUNT_MSFS;
        fsid.val[0] = (unsigned int) *newId;
        if (CC_FIND_CLUSTER_FSID(fsid)) {
            *newId += (int)time.tv_usec;
            if (*newId > 0) {
                *newId = 0 - *newId;
            }
            goto loop;
        }
        fsid.val[0] = curId;
        if (CC_FIND_CLUSTER_FSID(fsid)) {
            if (!failover) {
                reset = 1;
            }
        }
    } else {
        for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
            /* if setp is Nil then it is under construction (being mounted) */
            bfSetp = fsp->bfSetp;
            if ( ! BFSET_VALID(bfSetp) )
                continue;

            /*
             * Always keep newId unique
             */
            if (*newId == bfSetp->dev) {
                *newId += (int)time.tv_usec;
                if (*newId > 0) {
                    *newId = 0 - *newId;
                }
                goto loop;
            }

            if (bfSetp->dev &&
                (curId == bfSetp->dev) &&
                (bfSetp != setp)) {

                /*
                 * fs dev is not unique !
                 */
                reset = 1;
                /* Don't break - need unique newId */
            }
        }
        FILESET_UNLOCK(&FilesetLock );
    }
    return(reset);
}

/*
 * this function assumes the mount point is already marked busy, or is
 * being called in the system shutdown path
 */
u_int smsync_period = 0; /* period between smoothsync_thread passes */
u_int smsync_step = 0;
u_int smsync_debug = 0;

int
msfs_smoothsync(mp, sync_age, smsync_flag)
struct mount *mp;
u_int sync_age;
u_int smsync_flag;             /* perform non-smooth'ed operations */
{
    vdT* vdp;
    struct fileSetNode *fsnp;
    domainT *dmnP;
    int vdi;
    int vd_count;
    extern unsigned long startiocalls[];
    extern u_int smsync_age;
    static u_int latch = 0;
    static int freq_cntr = 1;
    static u_int age = 0;
    ubc_cntl_t  uc;

    /*
     * Recalculate advfs smoothsync parameters if smsync_age changes.
     * smsync_period: time interval between advfs smoothsync runs
     * smsync_step: "stride" value to select appropriate smsync-queue
     */
    if (age == 0 || age != smsync_age) {
        age = smsync_age;
        smsync_period = age == 0 ? 0 : age/SMSYNC_NQS+1;
        smsync_step = smsync_period == 0 ? 0 : age/smsync_period;
    }

    /*
     * "smsync_flag&SMSYNC_LATCH" changes on each pass of the smoothsync
     * thread through the set of filesystems.  It remains constant for
     * each filesystem processed during that pass.  This "latch" check
     * allows operations to be done one time per run of filesystems.
     * For advfs, decrement the freq_cntr, which is used to slow down
     * the frequency of flushing the set of advfs filesystems.
     */
    if (latch != (smsync_flag&SMSYNC_LATCH)) {
        latch = smsync_flag&SMSYNC_LATCH;
        if (freq_cntr > 0) freq_cntr--;
        else freq_cntr = smsync_period-1;
    }

    /*
     * The smoothsync thread works at a frequency of 1x/sec.  If that
     * frequency is too high for advfs (based on # sync queues and time
     * to age), slow down the frequency of msfs_smoothsync here.
     */
    if (sync_age && freq_cntr > 0 && !(smsync_flag&SMSYNC_PERIOD)) {
        return(0);
    }

    fsnp = GETFILESETNODE(mp);
    if (fsnp->fsFlags & FS_CLONEFSET) {
        return(0);
    }

    /*
     * Mount incremented the dmnAccCnt; this reference is safe
     * under vfs_busy.  The rootfs (used in the shutdown path)
     * is always safe to access.
     */
    dmnP = GETDOMAINP(mp);
    if (dmnP->dmn_panic) {
        return(0);
    }

    /*
     *  Don't run if domain is frozen.
     *  Take the dmnFreezeMutex to check status and to
     *  increment the ref count to prevent a new freeze
     *  from starting if we begin sync'ing.
     */
    mutex_lock(&dmnP->dmnFreezeMutex);
    if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) {
        mutex_unlock(&dmnP->dmnFreezeMutex);
        return(0);
    } else {
        dmnP->dmnFreezeRefCnt++;
        mutex_unlock(&dmnP->dmnFreezeMutex);
    }

 
    /* Assist UBC in making UBC cache pages available for recycling.
     * If UBC's dirty metadata page count exceeds the threshold,
     * then issue a domain log and metadata flush.
     */
    uc = MID_TO_UC(current_rad_id());       
    if (uc->ubc_dirty_metadata_count >
        ((uc->ubc_pages * uc->ubc_dirty_metadata_pcnt) / 100)) {
        lgr_flush(dmnP->ftxLogP);
        bs_lsnList_flush(dmnP);    
    }

    /*
     * If sync_age is 0, then flush the complete log and buffers.
     * This is the behavior when called from the sync syscall.
     *
     * Otherwise, it's sufficient to flush the log and free up the
     * waitLazyQ I/O's at a more coarse interval.  This interval is
     * smsync_step*smsync_period (as this code gets hit 1x/period),
     * which is the same as smsync_age (less rounding); i.e., the
     * configured smoothsync-age.  Also perform other non-smooth'ed
     * operations at this more coarse interval.  The smoothsync thread
     * will set "smsync_flag" to indicate this should be done.
     */
    if (sync_age == 0 || (smsync_flag&SMSYNC_PERIOD)) {

        SMSYNC_DBG( SMSYNC_DBG_OP,
            aprintf ("smoothsync:    lgr_flush  dmnP = 0x%lx\n", dmnP ));

        (void)msfs_sync_mmap(mp);
        (void)msfs_sync_todr(mp,0);

        /* move this here from before the msfs_sync_mmap() to guard
         * against a dangling log record write to disk from the 
         * msfs_sync_mmap()/fs_update_stats() path
         */
        lgr_flush(dmnP->ftxLogP);
    }

    /*
     * flush aged buffers
     */
    vd_count = 0;
    for (vdi = 1; vd_count < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++ ) {

        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            continue;
        }
        vd_count++;

        /*
         * Now that the log has been flushed, move eligible buffers
         * from the wait to the ready queue.
         */
        if (sync_age == 0 || (smsync_flag&SMSYNC_PERIOD))
            wait_to_readyq(vdp);

        /*
         * flush the smsyncQ to the readyQ
         * if sync(2), flush all queues
         */
        smsync_to_readyq(vdp, sync_age ? IO_SOMEFLUSH : IO_FLUSH);

        /*
         * If there are buffers on the readyQ, move them to the
         * consolQ and start flushing lazy I/Os to the device.
         */
        if (vdp->readyLazyQ.ioQLen) {
            if (ready_to_consolq( vdp )) {
                startiocalls[10]++;
                bs_startio (vdp, IO_FLUSH);
            }
        }

        vd_dec_refcnt( vdp );
    }
    mutex_lock(&dmnP->dmnFreezeMutex);
    dmnP->dmnFreezeRefCnt--;
    if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
        thread_wakeup( (vm_offset_t)&dmnP->dmnFreezeWaiting );
    }
    mutex_unlock(&dmnP->dmnFreezeMutex);

    return(0);
}

/*******************************************************************************
 *
 *  Freeze/thaw a domain
 *
 *  This routine does some preliminary checking for multiple freeze/thaw
 *  requests and then 'sends' a packaged message to the bs_freeze background
 *  thread. It then waits for the background thread to process the message
 *  before returning to the caller.
 *
 */

    extern advfsFreezeMsgT  *AdvfsFreezeMsgs;
    extern mutexT            AdvfsFreezeMsgsLock;

int
advfs_freezefs ( struct mount *mp,
                        u_long flags,
                        int    timeout)
{

    domainT            *dmnP;
    int                 error;
    char                uid_str[20];
    advfsFreezeMsgT    *msg;

    /*
     *  Get the domain pointer via the mount structure
     */
    dmnP = GETDOMAINP(mp);

    /*
     *  Check for domain panic
     */
    if (dmnP->dmn_panic) {
        return EIO;
    }

    /*
     *  If a freeze request, check if freezing or frozen.
     *  If a thaw request, check if not freezing or frozen
     */
    mutex_lock(&dmnP->dmnFreezeMutex);
    if (flags & FS_Q_FREEZE) {
        if (dmnP->dmnFreezeFlags & BFD_FREEZE_IN_PROGRESS) {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            return EINPROGRESS;
        } else if ( dmnP->dmnFreezeFlags & BFD_FROZEN) {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            return EALREADY;
        } else {
            dmnP->dmnFreezeFlags |= BFD_FREEZE_IN_PROGRESS;
        }
    } else if (flags & FS_Q_QUERY) {
        if (dmnP->dmnFreezeFlags & BFD_FROZEN) {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            return -1;
	} else {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            return 0;
	}
    } else {
        if ( !(dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN)) ) {
            mutex_unlock(&dmnP->dmnFreezeMutex);
            return EALREADY;
        }
    }
    mutex_unlock(&dmnP->dmnFreezeMutex);

    /*
     *  Send a message to the background thread
     *  and wait for it to be processed
     */
    msg = (advfsFreezeMsgT *) ms_malloc( sizeof(advfsFreezeMsgT) );
    msg->frzmDomainId    = dmnP->domainId;
    msg->frzmFlags       = flags;
    msg->frzmTimeout     = timeout;
    msg->frzmMP          = mp;
    msg->frzmStatus      = 0;
    mutex_lock(&AdvfsFreezeMsgsLock);
    msg->frzmLink = AdvfsFreezeMsgs;
    AdvfsFreezeMsgs = msg;
    assert_wait_mesg( (vm_offset_t) &msg->frzmStatus, FALSE, "advfs_freeze_vfsop" );
    thread_wakeup( (vm_offset_t) &AdvfsFreezeMsgs );
    mutex_unlock(&AdvfsFreezeMsgsLock);
    thread_block();
    error = msg->frzmStatus;
    ms_free(msg);
    return( error );
}

/* end msfs_vfsops.c */




