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
 *      AdvFS - Advanced File System
 *
 * Abstract:
 *
 *      This module defines the Virtual File System fs interfaces for
 *      the Advanced File System.  All possible VFS fs operations are
 *      defined in this module (whether AdvFS implements them or 
 *      not).
 */

#include <limits.h>
#include <sys/param.h>
#include <sys/errno.h>
#include <sys/systm.h>
#include <sys/time.h>
#include <sys/user.h>
#include <sys/vnode.h>
#include <sys/vfs.h>
#include <sys/mount.h>
#include <sys/diskio.h>
#include <sys/stat.h>
#include <sys/malloc.h>
#include <sys/shutdown.h>
#include <sys/proc.h>
#include <sys/fcntl.h>
#include <sys/buf.h>
#include <sys/ioctl.h>
#include <sys/conf.h>
#include <machine/sys/clock.h>  /* for ms_gettimeofday() */
#include <sys/pathname.h>
#include <h/kthread_access.h>   /* private: for DEVSW(), needs KT_SEMA() */

#include <ms_public.h>
#include <ms_privates.h>
#include <ms_osf.h>
#include <fs_dir.h>
#include <fs_dir_routines.h>
#include <ms_generic_locks.h>
#include <fs_quota.h>
#include <ms_assert.h>
#include <advfs/advfs_syscalls.h>
#include <aclv.h>
#include <advfs_acl.h>
#include <bs_index.h>
#include <tcr/clu.h>
#include <sys/fs/advfs_public.h>
#include <sys/fs/advfs_ioctl.h>
#include <bs_params.h>
#include <bs_access.h>
#include <advfs_evm.h>
#include <bs_freeze.h>
#include <bs_domain.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

#define ADVFS_MODULE       MSFS_VFSOPS
#define BOGUS_ROOTNAME     "generic/root"
#define BOGUS_GLROOTNAME   "cluster_root/root"

extern int advfs_fstype;
extern dev_t clu_global_rootdev[];

/*
 * Global names of root/boot mount points and default mount-from name.
 */
      char *rootmpname         = "/";
const char  advfs_default_rootname[] = "/dev/root";  
      char *bootmpname = NULL;
const char  advfs_default_bootname[] = "boot_device";

/*
 * msfs vfs operations.
 */
int  advfs_mount(struct vfs *vfsp, char *path, smountargs_t *ndp );
int  advfs_unmount(struct vfs *vfsp);
int  advfs_root(struct vfs *vfsp, struct vnode **vpp, char *name);
int  advfs_statvfs(struct vfs *vfsp, struct statvfs *statvfsp);
int  advfs_sync(struct vfs *vfsp, int flags);
int  advfs_vget(struct vfs *vfsp, struct vnode **vpp, struct fid *fhp);
int  advfs_getmount(struct vfs *vfsp, char *, struct mount_data *, char *);
int  advfs_freeze(struct vfs *vfsp, int, int);
int  advfs_thaw(struct vfs *vfsp);
int  advfs_quotactl(struct vfs *vfsp, int cmds, uid_t uid, void *arg);
int  advfs_mountroot(struct vfs *vfsp, int flag);
kern_daddr_t advfs_size(dev_t);     

int advfs_freeze_thaw_common(struct vfs *, int, int);

static int advfs_mountfs(struct vfs *, char *, char *, uint64_t, int32_t);
static int advfs_remount(struct vfs *, char *, uint64_t, advfs_args_t);
static statusT chk_set_fsdev(struct vfs *);
static dev_t adv_get_fsdev(struct vfs*);

/*
 * msfs_vfsops
 *
 * Defines function pointers to AdvFS's VFS fs operations.
 */
struct vfsops msfs_vfsops = {
    advfs_mount,
    advfs_unmount,
    advfs_root,
    advfs_statvfs, 
    advfs_sync,
    advfs_vget,
    advfs_getmount,
    advfs_freeze,
    advfs_thaw,
    advfs_quotactl,
    advfs_mountroot,
    advfs_size,
    NULL             /*advfs_freevfs?*/
};

/*
 * advfs_fstype - this global is used by CFS to test if the passed
 *                vfs pointer is for an AdvFS file system by comparing
 *                advfs_fstype against vfs.vfs_mtype
 */
extern int           advfs_fstype;

struct fileSetNode  *FilesetHead = NULL;
struct vnode       **GlobalRootVpp;

int MountWriteDelay = 0;         /* TRUE if we need to delay writes for mount */

/*
 * Global mutex and lock to serialize access to fileset (fileSetNode) list.
 */
ADVRWL_FILESETLIST_T     FilesetLock;

struct advfs_args_32 {
    ptr32_t fspec;
    int32_t adv_flags;
    int32_t fsid;
};

/*
 *  advfs_mountroot - AdvFS mount root routine
 *
 */

int
advfs_mountroot(
    struct vfs *vfsp,
    int flag)
{
    extern  dev_t   rootdev;
    
    uint64_t      dmn_act_flags = DMNA_MOUNTING;
    bfAccessT    *root_accessp;
    uint32_t      fs_time;
    statusT       sts;
    int           error, count;
    uint32_t      now_time;
    bsBfSetAttrT  fsAttr_rec;
    bfSetT       *bfSetp;

    MountWriteDelay = 0;

    /*
     * For CFS global root failover, rootvfs is the CFS mount point.
     * vfs passed in is for the physical pfs mount point.
     */
    if (flag & MS_FAILOVER) {
        if (!clu_is_ready()) {
            return EBUSY;
        }
        if (rootdev == NODEV) {
            panic("advfs_mountroot: rootdev not set for CFS failover");
        }

        dmn_act_flags |= DMNA_FAILOVER;
    } 

    dmn_act_flags |= (flag & MS_GLOBAL_ROOT) ? DMNA_GLOBAL_ROOT : DMNA_LOCAL_ROOT;

    error = advfs_mountfs(vfsp, NULL, NULL, dmn_act_flags, NULL);

    if (error) {
        return error;
    }

    /* Add the vfsp to the global list as rootvfs */

    /*
     * CFS failover, rootvfs already setup.  Skip the time update (since
     * in the case of failover, the time is already setup cluster-wide).
     */
    if ( !(dmn_act_flags & DMNA_FAILOVER)) {
    
        root_accessp = ADVGETROOTACCESS( vfsp );
        sts = bmtr_get_rec(root_accessp, BMTR_FS_TIME, &fs_time,
                           (uint16_t)sizeof(fs_time));
        if (sts == EOK) {
            /* Ths assumption is that fs_time is the same size as a 
             * time_t value.  If this changes, I am hoping for a compiler
             * error.
             */
            inittodr((time_t) fs_time);
            /* Update last modify time for initodr/resettodr, advfs_sync
             * typically does this, but we'll do it if we can mount
             * successfully too.
             */
            now_time = time.tv_sec;
            sts = bmtr_put_rec(root_accessp, 
                               BMTR_FS_TIME, 
                               &now_time, 
                               (uint16_t)sizeof(now_time), 
                               FtxNilFtxH
                               );
            if (sts != EOK) {
                /* likely i/o error; ignore it; update not critical */
                uprintf("advfs_mountroot: bmtr_put_rec warning %s\n",
                        BSERRMSG( sts ) );
            }
        }
        /*
         *  Since single-volume filesystems can be mounted either by volume
         *  name or by AdvFS name, ensure we're reporting the correct dev_t.
	 *  NOTE: rootdir is already set when mounting /etc/cfs in tcrcfs cluster.
         */
	if((clu_type() != CLU_TYPE_CFS) || 
	   (clu_type() == CLU_TYPE_CFS) && !(dmn_act_flags & DMNA_GLOBAL_ROOT))  {
            sts = VFS_ROOT(rootvfs, &rootdir, "/");
            VN_RELE(rootdir);
	}

        /*  Even if the VFS_ROOT call fails, call chk_set_fsdev().
         *  chk_set_fsdev() will _always_ succeed for root.  If VFS_ROOT
         *  isn't set, it'll use the global rootdev or cluster rootdev.
         */
        (void)chk_set_fsdev(vfsp);
    }

    bfSetp = ADVGETBFSETP(vfsp);

    /*  If mountroot called VFS_RDONLY, vfs_dev *must* equal
     *  rootdev.  This is because it's called very early in the 
     *  boot process.  When this happens, an unmount is performed
     *  via im_preinitrc() using umount_dev(rootdev) which fails
     *  and hangs the boot process if vfs_dev != rootdev.
     */
    if (flag & VFS_RDONLY) {
        vfsp->vfs_dev = rootdev;
    } else {
        vfsp->vfs_dev = bfSetp->bfs_dev;
    }

    vfsp->vfs_mnttime = time.tv_sec;
    vfsp->vfs_fsid[0] = vfsp->vfs_dev;
    vfsp->vfs_fsid[1] = advfs_fstype;
    vfsp->vfs_flag   |= (VFS_LARGEFILES + VFS_MULTIVOL);
    vfsp->vfs_bsize   = (int32_t)(bfSetp->dmnP->userAllocSz * ADVFS_FOB_SZ);

    /* 
     *  Don't do the vfs_add for a cluster mount which is "/"in a SSI cluster
     *  and "/etc/cfs" in a tcrcfs cluster where both will have DMNA_GLOBAL_ROOT set.
     */
    if(!(dmn_act_flags & DMNA_GLOBAL_ROOT))  {
        error = vfs_add((struct vnode *)NULL, vfsp,
                    (flag & VFS_RDONLY)? M_RDONLY: 0);
        if (error) {
            (void) advfs_unmount(vfsp);
        }

        vfs_unlock(vfsp);                       /* locked from vfs_add() */
    }

    return error;
}

static void
advfs_args_32_to_advfs_args(struct advfs_args_32 *args_32, advfs_args_t *kargs) {
    kargs->fspec       = (void *)(args_32->fspec);
    kargs->adv_flags   = args_32->adv_flags;
    kargs->fsid        = args_32->fsid;
}


/*
 * advfs_mount_copy_args
 *
 * The caller if this function is advfs_mount().  This function will
 * malloc memory for the fspec field if necessary.  However,
 * it will not free it if there is an error.  That is left to the caller to 
 * cleanup.
 */

static int
advfs_mount_copy_args(
        caddr_t udataptr, 
        advfs_args_t *adv_args) 
{
    advfs_args_t tmp_args;
    struct advfs_args_32 args_32;
    int error = 0;
    unsigned int len;

    if (ThisSysCallIs32bit()) {
        error = copyin(udataptr, (caddr_t)&args_32, sizeof(struct advfs_args_32));
        if (error) {
            return error;
        }
        advfs_args_32_to_advfs_args(&args_32, &tmp_args);
    } else {
        error = copyin(udataptr, (caddr_t)&tmp_args, sizeof(advfs_args_t));
        if (error) {
            return error;
        }
    }

    adv_args->fspec       = NULL;

    if (tmp_args.fspec != NULL) {
        adv_args->fspec = (char *)ms_malloc(MAXPATHLEN);

        error = copyinstr((caddr_t)tmp_args.fspec, 
                (caddr_t)adv_args->fspec, 
                MAXPATHLEN, 
                &len);
        if (error)
            return error;
    }

    adv_args->adv_flags = tmp_args.adv_flags;
    adv_args->fsid      = tmp_args.fsid;

    return error;
}

/*
 *  advfs_mount - AdvFS mount routine
 */
int
advfs_mount(
    struct vfs   *vfsp,     /* in */
    char         *pathp,    /* in, 'mount on' */
    smountargs_t *argsp )   /* in */
{
    extern struct vnodeops advfs_vnodeops;

    uint32_t           len;
    int                error;
    bfAccessT         *root_accessp;
    statusT            sts;
    int                now_time;
    domainT           *dmnP;
    bfSetT            *bfSetp;
    struct pathname    pn;

    char              *fsnamep = NULL;
    advfs_args_t       adv_args = {0};
    int                stopFreezefs = 0;
    int                pnfailed;
    uint64_t           dmn_act_flags = DMNA_MOUNTING;

    /*
     * Pull the pathname from user space
     * 
     * note that pn_get will return an errno on failure
     * which could be helpful in debugging failures.
     */
    
    pnfailed = pn_get(argsp->spec, UIOSEG_USER, &pn);
    if (!pnfailed) {
        fsnamep = pn.pn_buf;
    } 

    if (argsp->mflag & MS_DATA) {
        error = advfs_mount_copy_args(argsp->dataptr, &adv_args);
        if (error) {
            goto exit;
        }
        if (fsnamep == NULL) {
            fsnamep = adv_args.fspec;
        }
    } 
    if (fsnamep == NULL || pathp == NULL) {
        error = EINVAL;
        goto exit;
    }

    /*
     *  Set domain activation flags
     */
    if (argsp->mflag & MS_GLOBAL_ROOT) {
        dmn_act_flags |= DMNA_GLOBAL_ROOT;
    }

    if (argsp->mflag & MS_FAILOVER) {
        dmn_act_flags |= DMNA_FAILOVER;
    }

    if (adv_args.adv_flags & MS_ADVFS_DIRTY) {
        dmn_act_flags |= DMNA_FMOUNT;
    }

    if (!(argsp->mflag & MS_REMOUNT)) { 
	/*
	 * Initial mount.
	 */
        error = advfs_mountfs( vfsp, 
			       fsnamep, 
			       pathp, 
			       dmn_act_flags,
			       adv_args.adv_flags
			       );

        /*
         * Possibly update file set dev 
         */
        if (!error){
            error = chk_set_fsdev(vfsp);
        }

    } else {
        /*
         *  Remount (mount update).
	 *
         *  Don't allow mount update if domain is frozen.
         *  Take the dmnFreezeMutex to check status and to
         *  increment the ref count to prevent a new freeze
         *  from starting if we begin the mount -u.
         */

        dmnP = ADVGETDOMAINP(vfsp);
        ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
        if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) {
            ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
            error = EBUSY;
            goto exit;
        } else {
           dmnP->dmnFreezeRefCnt++;
           stopFreezefs++;
        }
        ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);

        error = advfs_remount (vfsp,
			       pathp,
			       dmn_act_flags,
			       adv_args
			       );
    }

    if (!error) {
        /*
         *  Update last modify time
         */
        dmnP = ADVGETDOMAINP(vfsp);
        root_accessp = ADVGETROOTACCESS( vfsp );
        now_time = time.tv_sec;
        sts = bmtr_put_rec(root_accessp,
                           BMTR_FS_TIME,
                           &now_time,
                           (uint16_t)sizeof(now_time),
                           FtxNilFtxH);
        if (sts != EOK && sts != E_READ_ONLY) {
            /* likely i/o error; ignore it; update not critical */
            uprintf("advfs_mount: Could not update modified time, %s\n",
                       BSERRMSG( sts ) );
        }

        bfSetp = ADVGETBFSETP(vfsp);
        
        /*  Never change the fsid after initial mount.  It screws
         *  up the VFS hash table.
         */
        if (!(argsp->mflag & MS_REMOUNT)) { 
            vfsp->vfs_dev     = bfSetp->bfs_dev;
            vfsp->vfs_fsid[0] = bfSetp->bfs_dev;
            vfsp->vfs_fsid[1] = advfs_fstype;
        }
        vfsp->vfs_flag   |= (VFS_LARGEFILES + VFS_MULTIVOL);
        vfsp->vfs_bsize   = (int32_t)(dmnP->userAllocSz * ADVFS_FOB_SZ);
    }

    /*
     *  If freezefs was held off
     *    Allow freezes
     */
    if ( stopFreezefs ) {
        domainT *dmnP;
        dmnP = ADVGETDOMAINP(vfsp);
        ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
        dmnP->dmnFreezeRefCnt--;
        if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
            cv_broadcast(&dmnP->dmnFreezeWaitingCV, NULL, CV_NULL_LOCK);
        }
        ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
    }

exit:
    if (adv_args.fspec)
        ms_free(adv_args.fspec);

    if (!pnfailed) {
        pn_free(&pn);
    }

    return error;

}


/* advfs_mountfs()
 *
 * Where the real work to get the filesystem activated in 
 * the kernel happens.
 */
static int advfs_mountfs(
   struct vfs *vfsp,
   char *fsnamep,
   char *pathp,
   uint64_t dmn_act_flags,
   int32_t advflags)
{
    char                *setName = NULL;
    char                *dmnName = NULL;
    struct fileSetNode  *dn = NULL;
    struct fileSetNode  *fsp;
    int                  dmn_active = FALSE,
                         on_fileset_list = FALSE,
                         domain_open = FALSE,
                         locks_init = FALSE,
                         set_open = FALSE,
                         root_open = FALSE;
    bfSetParamsT        *bfSetParamsp = NULL;
    bfAccessT           *bfap;
    struct vnode        *nvp = NULL;
    struct fsContext    *root_context;
    bsUndelDirT undel_rec;
    quotaInfoT          *qip;
    int                  error;
    statusT              sts;
    bfSetT              *setp = NULL;
    advfs_ev            *advfs_event;
    int                  noop;
    int                  first_nonrdonly_mount = TRUE;
    struct timeval       new_time;
    char                *tempstring = NULL;
    int32_t              pathlen = 0;
    int                  blockdevice = 0;

    /*
     *  Allocate and set up the fileSetNode
     */
    dn = (struct fileSetNode *)ms_malloc( sizeof( struct fileSetNode ) );
    dn->filesetMagic = FSMAGIC;
    dn->vfsp         = vfsp;
    /*
     *  Allocate and initialize the mount from and mount on strings.
     *  If not mounting root,
     *    Get mount from and mount on from advfs.args
     */
    if ( !(dmn_act_flags & DMNA_GLOBAL_ROOT) &&
         !(dmn_act_flags & DMNA_LOCAL_ROOT) ) { 
        /*
         *  Regular mount (no update).
         *  Malloc memory for:
         *      f_mntfromname
         *      f_mntonname
         *  Copy mount on name and mounted from name to the fileSetNode.
         */
        if (strlen(fsnamep) > MAXPATHLEN) {
            error = ENAMETOOLONG;
            goto cleanup;
        }

        tempstring = ms_malloc( MAXPATHLEN );

        if ((error = advfs_parsespec(fsnamep, 
                                     tempstring, 
                                     MAXPATHLEN, 
                                     &dmnName, 
                                     &setName, 
                                     &blockdevice)))
        {
            goto cleanup;
        }

        dn->f_mntfromname = ms_malloc( strlen(fsnamep) + 1 );
        strcpy(dn->f_mntfromname,fsnamep);

        if (blockdevice) {
            int i;

            strncpy(vfsp->vfs_name,fsnamep,MAXPATHLEN);
            vfsp->vfs_name[MAXPATHLEN-1] = '\0';

            /* Remove any trailing slashes */
            i = strlen(vfsp->vfs_name) - 1;
            while (i >= 0) {
                if (vfsp->vfs_name[i] != '/') 
                    break;
                vfsp->vfs_name[i--] = '\0';
            }
            if (strlen(vfsp->vfs_name) == 0) {
                error = EINVAL;
                goto cleanup;
            }
        } else {
            strncpy(vfsp->vfs_name,dn->f_mntfromname,(size_t)MAXPATHLEN);
            vfsp->vfs_name[MAXPATHLEN-1] = '\0';
        }

        dn->f_mntonname = ms_malloc( MAXPATHLEN );
        strncpy(dn->f_mntonname,pathp,MAXPATHLEN);
        dn->f_mntonname[MAXPATHLEN-1] = '\0';

    /*
     *  Else, this is root
     *    Create mount from and mount on strings
     */ 
    } else {
        /* 
         *  Set vfs_name to default_rootname "/dev/root"
         */
	if((clu_type() == CLU_TYPE_CFS) && !(dmn_act_flags & DMNA_LOCAL_ROOT)) 
        	strcpy(vfsp->vfs_name, TCRCFSMNTFROM);
	else if((clu_type() == CLU_TYPE_SSI))
		strcpy(vfsp->vfs_name, TCRSSIMNTFROM);
	else
        	strcpy(vfsp->vfs_name,advfs_default_rootname);
        vfsp->vfs_name[MAXPATHLEN-1] = '\0';

        /*
         *  Set domain and fileset name with BOGUS info
         */
        tempstring = ms_malloc( BS_DOMAIN_NAME_SZ );
	if(dmn_act_flags & DMNA_LOCAL_ROOT) {
	   setName = toke_it(BOGUS_ROOTNAME, '/', tempstring );
           dmnName = tempstring;
	} else if (clu_is_ready())  {
           setName = "default"; 
	   dmnName = "cluster_root";
	}

        /*
         * Now fill in f_mntonname and f_mntfromname with "/" and "/dev/root"
         * respectively.
         */
	if((clu_type() == CLU_TYPE_CFS) && !(dmn_act_flags & DMNA_LOCAL_ROOT))  {
            dn->f_mntonname = ms_malloc((uint32_t)(strlen(TCR_ETC_CFS)+1) );
            dn->f_mntfromname = ms_malloc((uint32_t)(strlen(TCRCFSMNTFROM)+1));
            strcpy(dn->f_mntonname, TCR_ETC_CFS);
            strcpy(dn->f_mntfromname, TCRCFSMNTFROM); 
	} else if((clu_type() == CLU_TYPE_SSI)) {
            dn->f_mntonname = ms_malloc( strlen(rootmpname) + 1 );
            dn->f_mntfromname = ms_malloc( strlen(TCRSSIMNTFROM) + 1 );
            strcpy(dn->f_mntonname, rootmpname);
            strcpy(dn->f_mntfromname, TCRSSIMNTFROM);
	} else {
            dn->f_mntonname = ms_malloc( strlen(rootmpname) + 1 );
            dn->f_mntfromname = ms_malloc(strlen(advfs_default_rootname) + 1);
            strcpy(dn->f_mntonname, rootmpname);
            strcpy(dn->f_mntfromname, advfs_default_rootname);
	}
    }

    MS_SMP_ASSERT(setName != NULL);

    bfSetParamsp = (bfSetParamsT *) ms_malloc( sizeof( bfSetParamsT ));

    /* 
     * activate the bfset (pass in the advfs mount flags) 
     * + special flags used only in cluster
     */
    sts = bs_bfset_activate( dmnName,
                             setName,
                             dmn_act_flags,
                             &dn->bfSetId );
    if (sts != 0) {
        error = BSERRMAP( sts );
        goto cleanup;
    }
    dmn_active = TRUE;

    /*
     * Disallow mounting a fileset with the same set ID.
     */
    ADVRWL_FILESETLIST_WRITE(&FilesetLock );

    for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
        if (BS_BFS_EQL(dn->bfSetId, fsp->bfSetId)) {
            ADVRWL_FILESETLIST_UNLOCK(&FilesetLock);
            error = EBUSY;
            goto cleanup;
        }

        /* check for non readonly, mounted fset already in this domain */
        if ((BS_UID_EQL(dn->bfSetId.domainId, fsp->bfSetId.domainId)) &&
            (fsp->f_mntfromname) && !(fsp->vfsp->vfs_flag & VFS_RDONLY)) {
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
        ADVRWL_FILESETLIST_UNLOCK(&FilesetLock);
        error = BSERRMAP( sts );
        goto cleanup;
    }
    domain_open = TRUE;
    dn->dmnP->mountCnt++;

    /* update the vfast first mount time if first mount, non-readonly */
    ADVMTX_SSDOMAIN_LOCK( &dn->dmnP->ssDmnInfo.ssDmnLk );
    if((first_nonrdonly_mount == TRUE) &&
        !(vfsp->vfs_flag & VFS_RDONLY) &&
       (dn->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
        dn->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {

        new_time = get_system_time();
        dn->dmnP->ssDmnInfo.ssFirstMountTime = new_time.tv_sec;
    }
    ADVMTX_SSDOMAIN_UNLOCK( &dn->dmnP->ssDmnInfo.ssDmnLk );

    ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );

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
    sts = advfs_check_vd_sizes(dn);
    if (sts < 0) {
        if (sts == -1) {
            error = EIO;
        } else {
            error = -sts;
        }
        goto cleanup;
    } else if (sts == 0) {
        vfsp->vfs_flag |= VFS_RDONLY;
    }

    /*
     * Initialize the locks and mutexes in the fileSetNode structure.
     */
    ADVSMP_FILESET_INIT(&dn->filesetMutex);
    ADVSMP_SPACE_USED_INIT( &dn->spaceUsed_lock );
    for (qip = &dn->qi[0]; qip < &dn->qi[MAXQUOTAS]; qip++) {
        ADVMTX_QILOCK_INIT(&(qip->qiLock));
    }
    locks_init = TRUE;

    if( sts = bfs_open( &dn->bfSetp, dn->bfSetId, BFS_OP_DEF, FtxNilFtxH ) ) {
        error = BSERRMAP( sts );
        goto cleanup;
    }
    set_open   = TRUE;
    setp       = dn->bfSetp;
    setp->fsnp = (void *) dn;

    if (advflags & MS_ADVFS_DIRECTIO) {
        setp->bfSetFlags |= BFS_IM_DIRECTIO;
    } else {
        setp->bfSetFlags &= ~BFS_IM_DIRECTIO;
    }
    if (advflags & MS_ADVFS_NOATIMES) {
	setp->bfSetFlags |= BFS_IM_NOATIMES;
    } else {
        setp->bfSetFlags &= ~BFS_IM_NOATIMES;
    }
    if (advflags & MS_ADVFS_NOREADAHEAD) {
	setp->bfSetFlags |= BFS_IM_NOREADAHEAD;
    } else {
	setp->bfSetFlags &= ~BFS_IM_NOREADAHEAD;
    }

    if (dmn_act_flags & DMNA_FAILOVER) {
        setp->bfSetFlags |= BFS_IM_FAILOVER;
    }

    if( sts = bs_get_bfset_params( dn->bfSetp, bfSetParamsp, 0 ) ) {
        error = BSERRMAP( sts );
        goto cleanup;
    }

    if (!BS_BFS_EQL(bfSetParamsp->bfspParentSnapSet, nilBfSetId)  && 
            (bfSetParamsp->bfSetFlags & BFS_OD_READ_ONLY)) {
        vfsp->vfs_flag |= VFS_RDONLY;
    }

    dn->rootTag.tag_num = ROOT_FILE_TAG;
    dn->rootTag.tag_seq = 1;
    dn->tagsTag.tag_num = TAGS_FILE_TAG;
    dn->tagsTag.tag_seq = 1;

    /*
     *  Set the fileSetNode pointer in the vfs structure.
     */
    vfsp->vfs_data = (void *)dn;

    /*
     * Must have this set before quota activation so that it can do
     * a bs_refpg
     */
    vfsp->vfs_version = 1;

    /*
     * Must tell the VFS layer the largest file AdvFS supports.  The
     * VFS layer will use vfs_max_large_file in order to constrain file
     * access and file locks to the limits established by AdvFS.
     */
    vfsp->vfs_max_large_file = ADVFS_MAX_OFFSET+1;

    /*
     * Fill in the mount entries 'stat' area.
     */
#   ifdef Tru64_to_HPUX_Port_Comments
    /*
     *                        ----> msfs_statfs() has been replaced by
     *                              advfs_statvfs().  msfs_statfs() would update
     *                              some global values in mount.m_stat, that
     *                              were then used by other parts of AdvFS.  But
     *                              advfs_statvfs() does not do that (at least
     *                              not at the moment).
     *
     *                              The only reason to call advfs_statvfs()
     *                              would be if we decided to maintain the
     *                              mount.m_stat abstraction, then we would have
     *                              advfs_statvfs() also do the global updates.
     *
     *                              Once this is known, then we can decide on
     *                              whether to replace msfs_statfs() with
     *                              advfs_statvfs() or to remove it altogether.
     */
    msfs_statfs( mp );
#   endif /* Tru64_to_HPUX_Port_Comments */

    /*
     * access the root bf
     */
    sts = bs_access(&bfap,
                    dn->rootTag,
                    dn->bfSetp,
                    FtxNilFtxH,
                    BF_OP_NO_FLAGS,
                    &nvp);
    if (sts != EOK) {
        printf("error returned from bs_access in advfs_mount %s\n",
                   BSERRMSG(sts));
        error = BSERRMAP( sts );
        goto cleanup;
    }

    root_open = TRUE;
    root_context = VTOC( nvp );
    MS_SMP_ASSERT( root_context );


    ADVRWL_FILECONTEXT_WRITE_LOCK( root_context );

    /*
     * Set up the root's fsContext area
     */
    root_context->bf_tag      = dn->rootTag;
    root_context->fsc_fsnp    = dn;
    root_context->dirstamp    = 0;
    root_context->fs_flag     = 0;
    root_context->dirty_stats = 0;

    sts = bmtr_get_rec(bfap,
                       BMTR_FS_UNDEL_DIR,
                       &undel_rec,
                       sizeof(bsUndelDirT));
    if (sts == EOK) {
        root_context->undel_dir_tag = undel_rec.dir_tag;
    } else if (sts == EBMTR_NOT_FOUND) {
        root_context->undel_dir_tag = NilBfTag;
    } else {
        ADVRWL_FILECONTEXT_UNLOCK( root_context );
        uprintf("error from bmtr_get_rec for BMTR_FS_UNDEL_DIR in advfs_mount %s\n",
                   BSERRMSG(sts));
        error = BSERRMAP( sts );
        goto cleanup;
    }

    /* Check to see if the directory being mounted has an index file
     * associated with it and if so open it now */

    IDX_OPEN_INDEX(bfap,sts);
    if (sts != EOK) {
        ADVRWL_FILECONTEXT_UNLOCK( root_context );
        error = BSERRMAP( sts );
        goto cleanup;
    }

    /*
     * Initialize the new vnode
     */
    nvp->v_type = VDIR;     /* set the vnode type */ 
    nvp->v_flag |= VROOT;   /* say its the root  */

    /*
     * get the root stats
     */
    sts = bmtr_get_rec(bfap,
                       BMTR_FS_STAT,
                       &root_context->dir_stats,
                       sizeof(statT));
    if (sts != EOK) {
        ADVRWL_FILECONTEXT_UNLOCK( root_context );
        printf("error returned from bmtr_get_rec for BMTR_FS_STAT in advfs_mount %s\n",
                BSERRMSG(sts));
        error = BSERRMAP( sts );
        goto cleanup;
    }

    bfap->file_size      = root_context->dir_stats.st_size;

    /*
     * Save the root vnode pointer and access structure pointer
     * in the fileSetNode structure.
     */
    dn->rootAccessp = bfap;
    dn->root_vp = nvp;

    /* 
     * Set the quota status to INACTIVE so bs_get_bfset_params will read
     * status off disk.
     */
    dn->quotaStatus = QSTS_INACTIVE;

    root_context->last_offset = 0;
    root_context->initialized = 1;

    /*
     * This should work for snapshots and parents alike.  The old Tru64 code
     * dealt this them differently, but group quotas on snapshots are
     * exactly the group quota on the parent.
     */
    sts = advfs_fs_quota_activate(dn);
    if (sts != E_QUOTA_NOT_MAINTAINED && sts != EOK) {
        uprintf("advfs_mount: WARNING: quota activation failed.\n");
        error = EIO;
        goto cleanup;
    }


    ADVRWL_FILECONTEXT_UNLOCK( root_context );

    if ( (clu_type() == CLU_TYPE_SSI) && (dmn_act_flags & DMNA_GLOBAL_ROOT) != 0 ) {
        /*
         * Suppress the event for cluster root (global or local) because at
         * this point in time the domain and fileset names are either
         * "generic#root" or "not_generic#root".  This will not be corrected
         * until a mount update occurs.  This is when the event will be
         * issued for the cluster root.
         */
        noop = 0;
    } else {
        /*
         * Issue a mount event for other mounts which are neither cluster
         * global roots nor cluster local roots.
         */
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = dmnName;
        advfs_event->fileset = setName;
        advfs_event->directory = dn->f_mntonname;
	sprintf (advfs_event->formatMsg,
		 (int)sizeof(advfs_event->formatMsg),
		 "Filesystem %s mounted on %s",
		 dmnName, dn->f_mntonname);
        (void)advfs_post_kernel_event(EVENT_FSET_MOUNT, advfs_event);
         ms_free(advfs_event);
    }

    /*
     * Call advfs_getacl to set the BFA_ACL_NOT_ONDISK flag for newly mounted
     * file system.
     */
    error = advfs_getacl( nvp, 0, NULL, ADVFS_ACL_TYPE );
    if( error != EOK ) {
	uprintf("advfs_mount: WARNING: advfs_getacl failed.\n" );
    }

    ms_free( tempstring );
    ms_free( bfSetParamsp );
    return (0);

cleanup:

    /*
     * Release resources and return an error code.
     */
    if (root_open) {
        (void) bs_close(bfap, MSFS_INACTIVE_CALL);
    }

    if ((dn->quotaStatus & QSTS_USR_QUOTA_MT) ||
        (dn->quotaStatus & QSTS_GRP_QUOTA_MT)) {
        (void) advfs_fs_quota_deactivate( dn );
    }

    if (locks_init) {
        for (qip = &dn->qi[0]; qip < &dn->qi[MAXQUOTAS]; qip++) {
             ADVMTX_QILOCK_DESTROY( &qip->qiLock );
        }
        ADVSMP_FILESET_DESTROY(&dn->filesetMutex);
        ADVSMP_SPACE_USED_DESTROY(&dn->spaceUsed_lock);
    }

    if (on_fileset_list) {
        ADVRWL_FILESETLIST_WRITE(&FilesetLock);
        *dn->fsPrev = dn->fsNext;
        if (dn->fsNext != NULL) {
            dn->fsNext->fsPrev = dn->fsPrev;
        }
        ADVRWL_FILESETLIST_UNLOCK(&FilesetLock);
    }

    if (set_open) {
        (void) bs_bfs_close(dn->bfSetp, FtxNilFtxH, BFS_OP_DEF);
    }
    
    if (domain_open) {
        ADVRWL_FILESETLIST_WRITE(&FilesetLock);
        MS_SMP_ASSERT(dn->dmnP->mountCnt > 0);
        dn->dmnP->mountCnt--;
        ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );
        bs_domain_close( dn->dmnP );
    }
    
    if (dmn_active) {
        (void) bs_bfdmn_deactivate( dn->bfSetId.domainId, dmn_act_flags );
    }

    if (tempstring != NULL) {
        ms_free(tempstring);
    }

    if (dn->f_mntfromname != NULL) {
        ms_free(dn->f_mntfromname);
    }

    if (dn->f_mntonname != NULL) {
        ms_free(dn->f_mntonname);
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
 * advfs_remount
 *
 * equivilent to mount update advfs_mount_update mount -u
 *
 */

static
int
advfs_remount( struct vfs *vfsp,
	       char *path,
	       uint64_t dmn_act_flags,
	       struct advfs_args args )
{

    domainT         *dmnP;
    fileSetNodeT    *fsn;
    bfSetT          *setp;
    struct vfs      *pvfsp;
    bfDmnDescT      *dmntbl;
    char            *dmnName;
    char            *setName;
    char            *tempstring = NULL;
    int              pathlen;
    int              sts;
    int              vdi;
    int              vdi2;
    int              vdCnt;
    struct vd       *vdp;
    struct vdDesc   *new_vdp;
    struct vnode    *vp;
    int              rdev_match;
    advfs_ev        *advfs_event;
    int              blockdevice;

    dmnP       = ADVGETDOMAINP(vfsp);
    fsn        = ADVGETFILESETNODE(vfsp);
    setp = ADVGETBFSETP(vfsp);
    dmntbl     = NULL;
    sts        = EOK;

    /*
     * If a fileset was mounted in read-only mode because
     * a volume appears to be mislabeled, do not allow the log to be
     * flushed or the mount update to succeed.  Instead, force the
     * user to correct this problem.
     */
    if (fsn->fsFlags & FS_FORCE_RDONLY) {
        return EROFS;
    }

    if (clu_type() == CLU_TYPE_SSI) {
        CLU_GETPFSVFSP(rootvfs, &pvfsp, sts);
        /*
         * If we can't get the pvfsp
         *   return success
         */
        if (sts) {
            return EOK;
        }
    } else {
        pvfsp = rootvfs;
    }

    if (pvfsp == vfsp) {
        /* We are updating root 
         * 1. We need to update the vfs_name
         */

        dmntbl = (bfDmnDescT *)ms_malloc( sizeof( bfDmnDescT ) );

        tempstring = ms_malloc( MAXPATHLEN );
        
        if ((sts = advfs_parsespec(args.fspec, 
                                   tempstring,
                                   MAXPATHLEN,
                                   &dmnName,
                                   &setName,
                                   &blockdevice)))
        {
            goto adv_mount_update_exit;
        }

        /* tempstring as returned from advfs_parsespec has the top-level
         * directory in it. 
         */
        pathlen = strlen(tempstring) + strlen(dmnName) +
            strlen(setName) + 3;        /* 3 comes from 2 '/' and '\0' */
        
        if (pathlen > MAXPATHLEN) {
            sts = ENAMETOOLONG;
            goto adv_mount_update_exit;
        }

        /*
         * Get the list of disks in this domain into a table.
         */
        sts = get_domain_disks( dmnName, 0, dmntbl, NULL);
        if ( sts != EOK ) {
            uprintf("advfs_mount: error getting disk list for domain %s\n",
                        dmnName);

            if ((strlen(fsn->f_mntonname) == strlen(rootmpname)) &&
                !strcmp(fsn->f_mntonname, rootmpname)) 
            {
                char *mfname;

                mfname = (char *) advfs_default_rootname;
                ms_free(fsn->f_mntfromname);
                fsn->f_mntfromname = ms_malloc(strlen(mfname)+1);
                strcpy(fsn->f_mntfromname, mfname);
                uprintf("advfs_mount: Setting root device name to \"%s\" RW\n",
                           fsn->f_mntfromname);
            } 
            goto adv_mount_update_exit; 
        }

        /*
         * Verify that the number of volumes specified in /dev/advfs
         * is the same as on-disk volume count. If so, make sure
         * the service classes have been setup ok. If not, return an 
         * error.
         */

        if (dmntbl->vdCount != dmnP->vdCnt) {
            sts = bs_fix_root_dmn(dmntbl, dmnP, dmnName, tempstring);
            if (sts != EOK) {
                if ((clu_type() == CLU_TYPE_SSI) && (sts == EAGAIN)) {
                    /*
                     * This is a special cluster only case where
                     * we are unable to update CNX database for
                     * global root devs. Bubble the error up so
                     * that CFS can do the right thing.
                     */
                    goto adv_mount_update_exit;
                } else {
                    uprintf("Volume count mismatch for domain %s.\n", dmnName);
                    uprintf("%s expects %d volumes, %s/%s has %d links.\n",
                        dmnName, dmnP->vdCnt, tempstring, dmnName, 
                        dmntbl->vdCount);
                }
            }
        } else {
            sts = bs_check_root_dmn_sc(dmnP);
            if (sts != EOK) {
                goto adv_mount_update_exit;
            }
        }

        /*
         * Verify that the specified device is the one that
         * is really being used for the root file system.
         * It's possible for the linked device to be wrong, so we
         * compare the rdev fields, not the vnodes.
         */

        if ((strlen(fsn->f_mntonname) == strlen(rootmpname)) &&
             !strcmp(fsn->f_mntonname, rootmpname))
        {

            /* Get vnode of each of the devices and compare rdev values */
            for (vdi = 0; vdi < dmntbl->vdCount; vdi++) {
                sts = advfs_getvp(&vp, dmntbl->vddp[vdi]->vdName, UIOSEG_KERNEL,
                        FOLLOW_LINK);
                if( sts ) {
                    uprintf("advfs_mount: Accessing \"%s\" failed, check "
                            "device special file\n",
                            dmntbl->vddp[vdi]->vdName);
                    goto adv_mount_update_exit;
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
                        if (vdp->devVp->v_rdev == vp->v_rdev) {
                            rdev_match = TRUE;
                            break;
                        }
                    }
                }

                if (!rdev_match) {
                    uprintf("advfs_mount: The mount device does not match "
                            "the linked device.\n");
                    uprintf("Check linked device in %s/%s\n", tempstring, 
                            dmnName);
                    strcpy(fsn->f_mntfromname, (char *) advfs_default_rootname);
                    uprintf("advfs_mount: Setting root device name %s to RW\n",
                            fsn->f_mntfromname);
                    VN_RELE(vp);
                    /*
                     * Let root be mounted, so sysadmin can fix it.
                     * error = BSERRMAP(E_RDEV_MISMATCH);
                     */
                    sts = EOK;
                    goto adv_mount_update_exit;
                }
                VN_RELE(vp);
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
        ms_free(fsn->f_mntfromname);
        fsn->f_mntfromname = ms_malloc( pathlen );
        sprintf(fsn->f_mntfromname, pathlen, "%s/%s/%s", tempstring, 
            dmnName, setName);

        if (blockdevice) {
            strncpy(vfsp->vfs_name,args.fspec,MAXPATHLEN);
        } else {
            strncpy(vfsp->vfs_name,fsn->f_mntfromname,MAXPATHLEN);
        }

        /*
         * Flush domain dirty pages if writes have been delayed.
         */
        advfs_bs_dmn_flush(dmnP, FLUSH_UPDATE_STATS);

        if ( clu_type() == CLU_TYPE_SSI ) {
            /*
             * Issue the mount event for the cluster root now (global or
             * local root), because we have the real domain and fileset
             * names at this time.  We did not have them earlier when
             * advfs_mountroot() was called so "generic/root" or
             * "not_generic/root" was used at that time.
             */
            advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
            advfs_event->domain = dmnName;
            advfs_event->fileset = setName;
            advfs_event->directory = fsn->f_mntonname;
            (void)advfs_post_kernel_event(EVENT_FSET_MOUNT, advfs_event);
            ms_free(advfs_event);
        }
    }

    if (args.adv_flags & MS_ADVFS_NOREADAHEAD) {
	setp->bfSetFlags |= BFS_IM_NOREADAHEAD;
    }
    else {
	setp->bfSetFlags &= ~BFS_IM_NOREADAHEAD;
    }

adv_mount_update_exit:

    if (dmntbl != NULL) {
        free_vdds( dmntbl );
        ms_free(dmntbl);
    }

    if (tempstring != NULL) {
        ms_free(tempstring);
    }

    return sts;
}


/*
 * advfs_check_vd_sizes - Verify the sizes of the virtual disks.
 *                        Try to check all of the volumes in the domain
 *                        even though just one corrupted volume will
 *                        cause the mount to fail.  That way, the user
 *                        gets all the bad news at once rather than
 *                        incrementally.
 *
 * Return values:  >0: No inconsistency; allow mount to proceed
 *                  0: Non-fatal inconsistency; mount read-only
 *                 <0: Fatal inconsistency or internal error; fail the mount
 */
int
advfs_check_vd_sizes(struct fileSetNode *fsnp)
{
    caddr_t         lastBlkp = NULL;            /* Buffer for reading */
    domainT         *dmnP;                      /* This domain */
    struct vd       *vdp;                       /* Current volume */
    uint64_t         actual_vd_size;            /* As returned by ioctl() */
    int             sts;                        /* Return status */
    vdIndexT        index;                      /* Index of current volume */
    int             vols_checked,               /* Volumes checked so far */
                    corrupt_vd_found = FALSE,   /* TRUE if we found a bad one */
                    mislabeled_vd_found = FALSE;/* TRUE if we found bad label */
    capacity_type   cap;                        /* Capacity in DEV_BSIZE */
    sema_t          *save;                      /* Required by DEVSW() */
    dev_t           char_dev;                   /* VCHR device */

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

        /*
         * The device has been opened via the block dev_t.  We need to
         * have it open via the char dev_t as well to avoid a panic.
         */

        char_dev = block_to_raw(vdp->devVp->v_rdev);

        sts = opend(&char_dev, S_IFCHR, FREAD, 0);

        if (sts == 0) {
            DEVSW(cdevsw, major(char_dev), d_ioctl, 
                  (char_dev, DIOC_CAPACITY, (caddr_t)&cap, FREAD),
                  sts, save);
            (void) closed(char_dev, S_IFCHR, FREAD);
        }

        if (sts != 0) {
            uprintf("Cannot get size of virtual disk %s.\n",vdp->vdName);
            corrupt_vd_found = TRUE;
        }
        else {
            actual_vd_size = cap.lba;
            if ((vdp->vdSize == actual_vd_size) ||
              (vdp->vdSize == (actual_vd_size & ~0x0f))) {
                ;   /* Great; this is what we expected. */
            }
            else if (vdp->vdSize < actual_vd_size) {
		uprintf("NOTICE: Size of virtual disk %s is %ld blocks\n",
			   vdp->vdName,
			   actual_vd_size);
		uprintf("        AdvFS is only using %ld blocks.\n",
			   vdp->vdSize);
            }
            else {
                uprintf("Actual size of virtual disk %s is %ld blocks\n",
                           vdp->vdName, actual_vd_size);
                uprintf("but recorded size is %ld blocks.\n",
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
                    bfPageRefHT     pgref;          /* Handle to page */
                    struct bsStgBm* sbmp;           /* An SBM page */
                    bs_meta_page_t  page_to_read;   /* Number of page to read */
                    bf_vd_blk_t     clust_to_skip;  /* Off. of last in-use cl.*/
                    uint32_t        cur_word;       /* Curr. word in SBM page */
                    int             done,           /* TRUE if page is found */
                                    sbm_word,       /* Current word in SBM */
                                    in_use_bits;    /* Bits used in sbm_word */

                    /*
                     * Allocate the bigger structures from heap
                     * to save stack space.
                     */
                    done = FALSE;
                    if (lastBlkp == NULL) {
                        lastBlkp = (caddr_t) ms_malloc(DEV_BSIZE);
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
                                       FtxNilFtxH,
                                       MF_VERIFY_PAGE);
                        if (sts) {
                            corrupt_vd_found = TRUE;
                            uprintf("Cannot read essential data on %s.\n",
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
                                uprintf("Cannot find last in-use block.\n");
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
                            uprintf("Could not release SBM page.\n");
                            corrupt_vd_found = TRUE;
                            break;
                        }
                    }

                    /*
                     * If we found the last in-use block, try to read it.
                     */
                    if (sts == EOK) {
                        sts = advfs_raw_io(vdp->devVp, 
                                (clust_to_skip * vdp->stgCluster + 1),
                                1,
                                RAW_READ,
                                lastBlkp
                                );
                        if (sts) {
                            uprintf("Cannot read essential data on %s.\n",
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
        uprintf("Corrupted volume found; failing mount of %s.\n",
                   fsnp->f_mntfromname);
        sts = -1;
    }
    else if (mislabeled_vd_found) {
        int32_t ncur;

        ADVFS_ATOMIC_FETCH_BIT_OR( &fsnp->fsFlags, FS_FORCE_RDONLY, &ncur);
        uprintf("Mounting fileset %s in read-only mode.\n",
                   fsnp->f_mntfromname);
        sts = 0;
    }
    else {
        sts = 1;
    }

    if (lastBlkp)
        ms_free(lastBlkp);
    return sts;
}


/*
 * advfs_getvp
 *
 */
int
advfs_getvp(
    struct vnode **vpp,     /* out */
    caddr_t fname,          /* in */
    int32_t space,          /* in */
    int32_t flags )         /* in */
{
    int error;

    *vpp = NULL;
    if (error = lookupname(fname, space, flags, NULL, vpp)) {
        return (error);
    }
    MS_SMP_ASSERT (*vpp != NULL);
    return( 0 );
}


uint32_t advfs_unmount_state_wait = 0;
/*
 * advfs_unmount
 *
 * unmount the file system
 */
int
advfs_unmount(struct vfs *vfsp)   /* in - the vfs structure pointer */
{
    struct fileSetNode   *fsnp,
                         *fsp;
    int                   error = 0;
    statusT               sts;
    struct bfNode        *bnp;
    int                   restart_quotas = FALSE;
    bfSetT               *setp = NULL;
    struct fsContext     *dir_context_pointer;
    bfAccessT            *root_accessp;
    quotaInfoT           *qip;
    struct vnode         *rvp = NULL;
    int                   i;
    bfTagT                userQuotaTag;
    bfTagT                groupQuotaTag;
    bfTagT                rootTag;
    bfTagT                tag;
    struct vnode         *vp;
    char                 *setName=NULL;
    char                 *dmnName=NULL;
    char                 *tmp_mntfrom;
    advfs_ev             *advfs_event;
    domainT              *dmnP = NULL;
    int                   ss_was_activated = FALSE;
    int                   oldState;
    int                   nonrdonly_mount = FALSE;
    lkStatesT             accState;
    bfAccessT            *bfap = NULL;

    fsnp = ADVGETFILESETNODE(vfsp);
    setp = fsnp->bfSetp;
    dmnP = setp->dmnP;
    rvp = fsnp->root_vp;

    /*
     *  Return EBUSY if domain is frozen.
     *  Take the dmnFreezeMutex to check status and to
     *  increment the ref count to prevent a new freeze
     *  from starting if we begin unmount.
     */
    ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
    if ( dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN) ) {
        ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
        return(EBUSY);
    } else {
        dmnP->dmnFreezeRefCnt++;
        ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
    }

    /*
     * Check to see if unmount will fail with EBUSY.  umount_vfs
     * has disabled lookups on this mount queue.  However, reads
     * and writes could still be going on.  vnodes on the mount
     * queue can't gain new references -- they can only lose
     * references.
     *
     */
    rootTag = fsnp->rootTag;

    userQuotaTag = fsnp->qi[USRQUOTA].qiTag;
    groupQuotaTag = fsnp->qi[GRPQUOTA].qiTag;

    if(setp->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) {
        (void)advfs_ss_change_state(setp->dmnP->domainName,
				    SS_SUSPEND, &oldState, 0);
        ss_was_activated = TRUE;
    }

    /*
     * If the system is being shutdown, it is important that we flush all
     * of the access structures to disk.   
     * Therefore, only check for EBUSY failure if not "system is in 
     * shutdown mode".
     * Don't bother checking for EBUSY if the domain has paniced.
     */
    root_accessp = ADVGETROOTACCESS(vfsp);
    if (!IS_SYSTEM_IN_SHUTDOWN_MODE() && !setp->dmnP->dmn_panic) {
again:
        ADVSMP_SETACCESSCHAIN_LOCK(&setp->accessChainLock);
        bfap = setp->accessFwd;
        while (bfap != (bfAccessT *)(&setp->accessFwd)) {
            if (bfap->accMagic == ACCMAGIC_MARKER) {
                bfap = bfap->setFwd;
                continue;
            }

            MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);

            /* grab the bfaLock out of order */
            if (!ADVSMP_BFAP_TRYLOCK(&bfap->bfaLock)) {
                ADVSMP_SETACCESSCHAIN_UNLOCK(&setp->accessChainLock);
                delay(1);
                goto again;
            }

            accState = lk_get_state(bfap->stateLk);
            /*
             * If the bfap is either invalid or on the free or closed list,
             * then it must have no accesses on it, so we can just skip it.
             */
            if ((accState == ACC_INVALID)            ||
                (bfap->freeListState != NOT_ON_LIST)) {
                ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
                bfap = bfap->setFwd;
                continue;
            }
            if ( (accState == ACC_INIT_TRANS) ||
                 (accState == ACC_RECYCLE) ||
                 (accState == ACC_DEALLOC) ) {
                /* This access struct is in a transitive state.
                 * Sleep now and start again from the beginning.
                 */
                ADVSMP_SETACCESSCHAIN_UNLOCK(&setp->accessChainLock);
                advfs_unmount_state_wait++;
                /*
                 * This will drop the bfaLock when it does a 
                 * cv_wait with a no relock flag.
                 */
                lk_wait( &bfap->stateLk, &bfap->bfaLock );
                goto again;
            }

            /*
             * This check makes sure there are no external opens on the
             * file.  An internal reference (a refCnt) should not prevent an
             * unmount. An internal access could occur for a number of
             * reasons including a file being processed on the free/closed
             * lists by the access_mgmt_thread.  
             */
            if ( (!(bfap->bfaFlags & BFA_EXT_OPEN)) &&
                    (bfap->bfVnode.v_count == 0) &&
                    (bfap->bfVnode.v_scount <= 1)) {
                ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
                bfap = bfap->setFwd;
                continue;
            }

            if (!(bfap->bfaFlags & BFA_ROOT_SNAPSHOT) &&
                 (bfap->refCnt == 1) &&
                 (bfap->bfaFlags & BFA_OPENED_BY_PARENT) ) {
                /*
                 * If we are not a root snapshots and we have one access on
                 * bfap and bfap is opened by the parents, then that must be
                 * the reference.   That is a legitimate unmount.
                 */
                MS_SMP_ASSERT( !(bfap->bfaFlags & BFA_EXT_OPEN ) );
                MS_SMP_ASSERT( bfap->bfVnode.v_count == 0 );
                MS_SMP_ASSERT( bfap->bfVnode.v_scount <= 1 );
                ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                bfap = bfap->setFwd;
                continue;
            }

            

            /*
             * If this is the root directory's index file, then ignore it.
             * When we close the root directory below, the index file will
             * be closed as well.
             */
            if ( IDX_INDEXING_ENABLED(bfap) && IDX_FILE_IS_INDEX(bfap) &&
                 root_accessp == ((bsIdxRecT *)bfap->idx_params)->dir_bfap &&
                 bfap->refCnt == 1) {
                ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
                bfap = bfap->setFwd;
                continue;
            }

            tag = bfap->tag;
            if ((BS_BFTAG_EQL(tag, rootTag) ||
                        BS_BFTAG_EQL(tag, userQuotaTag) ||
                        BS_BFTAG_EQL(tag, groupQuotaTag)) &&
                    (bfap->refCnt - (bfap->bfaRefsFromChildSnaps + 
                                      (bfap->bfaFlags & BFA_OPENED_BY_PARENT ?
                                       1 : 0)) == 1)) 
            {
                ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
                bfap = bfap->setFwd;
                continue;
            }

            ADVFS_ACCESS_LOGIT( bfap, "advfs_unmount found busy bfap");
            ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
            ADVSMP_SETACCESSCHAIN_UNLOCK(&setp->accessChainLock);
            error = EBUSY;
            goto cleanup;
        }

        ADVSMP_SETACCESSCHAIN_UNLOCK(&setp->accessChainLock);
    } /* end while */

    advfs_fs_quota_deactivate( fsnp );
    restart_quotas = TRUE;

    /*
     * check the usecount on the root. if unmount can be done, it
     * should be 1. If it is any greater, no unmounting allowed
     * here...
     */

    if (rvp->v_count != 1) {
        error = EBUSY;
        goto cleanup;
    }

    /*
     * write back the roots stats and close the root
     */

    dir_context_pointer = VTOC( rvp );

    /*
     * update the stats (if needed)
     */

    if ((dir_context_pointer != NULL) &&
        (dir_context_pointer->dirty_stats == TRUE)) {

        sts = fs_update_stats( rvp, root_accessp, FtxNilFtxH, 0 );
        if (sts != EOK) {
            uprintf("Error updating roots stats %s\n", BSERRMSG(sts));
        }
    }

    /*
     * close the root
     */

    VN_RELE( rvp );

    if(ss_was_activated == TRUE) {
        (void)advfs_ss_change_state(setp->dmnP->domainName,
		   	            SS_ACTIVATED,
			            &oldState,
			            0);
        ss_was_activated = FALSE;
    }

    /* 
     * Allocate an AdvFS event before decrementing the mountCnt
     * to avoid having to clean up in case of failure.
     */

    advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));

    /*
     * Remove fileSetNode (fileset) from mounted fileset list.
     * Do this prior to deactivating the bfSet so there won't be
     * a fileSetNode on the list with a bad bfSetp.
     */

    ADVRWL_FILESETLIST_WRITE(&FilesetLock );

    *fsnp->fsPrev = fsnp->fsNext;
    if (fsnp->fsNext != NULL) {
        fsnp->fsNext->fsPrev = fsnp->fsPrev;
    }
    MS_SMP_ASSERT(fsnp->dmnP->mountCnt > 0);
    fsnp->dmnP->mountCnt--;

    /* reset the vfast first mount time if no other mounted filesets writable */
    for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
        /* check for non readonly, mounted fset already in this domain */
        if ((BS_UID_EQL(setp->bfSetId.domainId, fsp->bfSetId.domainId)) &&
            (fsp->f_mntfromname) && !(fsp->vfsp->vfs_flag & VFS_RDONLY)) {
            nonrdonly_mount = TRUE;
        }
    }

    ADVMTX_SSDOMAIN_LOCK( &setp->dmnP->ssDmnInfo.ssDmnLk );
    if(nonrdonly_mount == FALSE) {
        setp->dmnP->ssDmnInfo.ssFirstMountTime = 0;
    }
    ADVMTX_SSDOMAIN_UNLOCK( &setp->dmnP->ssDmnInfo.ssDmnLk );

    ADVRWL_FILESETLIST_UNLOCK(&FilesetLock );

    /*
     * Close the fileset.
     */
    dmnP = setp->dmnP;
    setp->fsnp = NULL;
    bs_bfs_close(setp, FtxNilFtxH, BFS_OP_FLUSH_STATS|BFS_OP_UNMOUNT);

    ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
    dmnP->dmnFreezeRefCnt--;
    if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
        cv_broadcast(&dmnP->dmnFreezeWaitingCV, NULL, CV_NULL_LOCK);
    }
    ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);

    /*
     * Flush all dirty metadata in the domain.  The purpose of this flush
     * is to force to disk all BMT, SBM, etc. pages that were modified 
     * by the fileset being unmounted.  If only one fileset in the domain
     * is currently mounted, then this is unnecessary since the call to 
     * bs_bfdmn_deactivate() below will also do this.  But if the fileset
     * being unmounted is one of several filesets mounted in this domain,
     * the call to bs_bfdmn_deactivate() will not perform the domain-wide
     * flush.  The result is that dirty reserved file pages, pages modified
     * on behalf of the fileset currently being unmounted, may still be
     * in cache when the unmount completes.  This opens a window in which if
     * the system crashed right after a successful unmount, metadata changes
     * made on behalf of the filesystem that was successfully unmounted
     * might not appear to have occurred when the fileset is remounted.
     */
    advfs_bs_dmn_flush_meta(dmnP, FLUSH_NOFLAGS);

    /*
     * Close the domain (Should this be inside bs_bfdmn_deactivate?)
     */
    bs_domain_close( dmnP );
    
    /*
     * For cluster, mark deactivate as being in mount/unmount path.
     * Deactivate the domain
     */
    if ((sts = bs_bfdmn_deactivate( fsnp->bfSetId.domainId,
                    DMNA_MOUNTING | DMNA_UNMOUNT_RQST )) != EOK) {
        /*
         * how can this error ever happen?  race with advfs utils?
         */
        printf("advfs_unmount: bs_bfdmn_deactivate failed.\n");
    }

    /*
     * Destroy the locks and mutexes in the fileSetNode structure.
     */
    for (qip = &fsnp->qi[0]; qip < &fsnp->qi[MAXQUOTAS]; qip++) {
        ADVMTX_QILOCK_DESTROY( &qip->qiLock );
    }
    ADVSMP_FILESET_DESTROY( &fsnp->filesetMutex );
    ADVSMP_SPACE_USED_DESTROY(&fsnp->spaceUsed_lock);

    tmp_mntfrom = ms_malloc(MAXPATHLEN);
    strncpy(tmp_mntfrom, fsnp->f_mntfromname, MAXPATHLEN);
    setName = strrchr(tmp_mntfrom, '/');

    MS_SMP_ASSERT(setName != NULL);
    
    *setName = '\0';
    setName++;
    dmnName = strrchr(tmp_mntfrom, '/');

    MS_SMP_ASSERT(dmnName != NULL);
    
    dmnName++;

    advfs_event->domain = dmnName;
    advfs_event->fileset = setName;
    advfs_event->directory = fsnp->f_mntonname;
    sprintf (advfs_event->formatMsg,
	     (int)sizeof(advfs_event->formatMsg),
	     "Filesystem %s unmounted", dmnName);
    (void)advfs_post_kernel_event(EVENT_FSET_UMOUNT, advfs_event);
    ms_free(advfs_event);

    ms_free(tmp_mntfrom);

    if (fsnp->f_mntonname)
            ms_free(fsnp->f_mntonname);
    if (fsnp->f_mntfromname)
            ms_free(fsnp->f_mntfromname);

    MS_SMP_ASSERT(fsnp->filesetMagic == FSMAGIC);
    fsnp->filesetMagic |= MAGIC_DEALLOC;
    ms_free( fsnp );

    return 0;

cleanup:

    if(ss_was_activated) {
        /* reset flag so smartstore can continue working */
        (void)advfs_ss_change_state(setp->dmnP->domainName,
			            SS_ACTIVATED,
			            &oldState,
			            0);
    }

    if (restart_quotas) {
        advfs_fs_quota_activate( fsnp );
    }

    ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
    dmnP->dmnFreezeRefCnt--;
    if ( dmnP->dmnFreezeWaiting && dmnP->dmnFreezeRefCnt == 0) {
        cv_broadcast(&dmnP->dmnFreezeWaitingCV, NULL, CV_NULL_LOCK);
    }
    ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);

    return error;
}

/*
 * advfs_getmount
 *
 * get mount table information
 * store AdvFS specific mount options in opt buffer
 * */
int
advfs_getmount(
    struct vfs *vfsp,
    char *fsmntdir,
    struct mount_data *mdp,
    char *   opts)
{
#define ADVFS_OPTS_STRING_LENGTH 128
    struct fileSetNode *fsnp;
    uint                l;
    char                buf[ADVFS_OPTS_STRING_LENGTH];

    fsnp = ADVGETFILESETNODE(vfsp);
    mdp->md_dev = vfsp->vfs_dev;
    mdp->md_rdev = fsnp->root_vp->v_rdev;

    /*
     * Check mount flags, and set the AdvFS specific mount options in the
     * opt buffer
     */
    buf[0] = '\0';

    /* NOATIMES */
    if( fsnp->bfSetp->bfSetFlags & BFS_IM_NOATIMES ) {
	strcat( buf, "noatimes" );
    }

    /* DIRECTIO */
    if( fsnp->bfSetp->bfSetFlags & BFS_IM_DIRECTIO ) {
	if( buf[0] != '\0' ) {
	    strcat( buf, "," );
	}
	strcat( buf, "directio" );
    }

    /* NOREADAHEAD */
    if( fsnp->bfSetp->bfSetFlags & BFS_IM_NOREADAHEAD ) {
	if( buf[0] != '\0' ) {
	    strcat( buf, "," );
	}
	strcat( buf, "noreadahead" );
    }

    /* Copy buf to opts */
    if( buf[0] != '\0' ) {
	if( opts == (char *)NULL ) {
	    return EINVAL;
	}
	(void)strncpy( opts, buf, (size_t)ADVFS_OPTS_STRING_LENGTH );
    }

    (void)strncpy(fsmntdir, fsnp->f_mntonname, (size_t)MAXPATHLEN);
    return (0);

}


/*
 * advfs_root
 *
 * return the vnode of the root
 * Note that the root is left accessed for the entire time
 * that the file system is mounted.
 */
int
advfs_root(
    struct vfs *vfsp,      /* in - the vfs structure pointer */
    struct vnode **vpp,    /* out - the vnode of the root */
    char   *name)
{
    /*
     * just read the vp out of the fileSetNode struct
     * and vref it
     */
    *vpp = NULL;
    VN_HOLD(ADVGETROOTVNODE(vfsp));
    *vpp = ADVGETROOTVNODE(vfsp);
    return(0);
}

/*
 * Do operations associated with quotas
 */
int
advfs_quotactl(
    struct vfs *vfsp,
    int cmds,
    uid_t uid,
    void *arg)
{
    int type, error, kernaddr;
    int root_check = TRUE;  /* Most commands require root user check */
    uid_t ruid;
    struct fileSetNode *dnp;
    struct ucred *credp = TNC_CRED();

    /*
     * We are processing Q_SYNC up front here because quotacheck calls us with
     * Q_SYNC without providing a vfs pointer.  That means we can't get our
     * fileSetNode because the call to the ADVGETFILESETNODE macro would panic.
     */
    if (cmds == Q_SYNC) {
            return EOK;
    }

    dnp = ADVGETFILESETNODE(vfsp);
    if (dnp->dmnP->dmn_panic) {
        return EIO;
    }

    ruid = credp->cr_ruid;
    if (uid == -1)
        uid = ruid;

    /*
     * We are not exposing group quotas at this time, so we'll simply
     * set type to USRQUOTA.  We may remove all traces of group quotas
     * at a future time.
     */
    type = USRQUOTA;

    if ((vfsp->vfs_flag & VFS_CFSONTOP) && (CLU_QUOTA_KERNEL()))
            kernaddr = 1;
    else
	    kernaddr = 0;

    if ((vfsp->vfs_flag & VFS_CFSONTOP) && (CLU_QUOTA_DQGET()))
            root_check = FALSE;
    else if ((cmds == Q_GETQUOTA) || (cmds == Q_GETQUOTA64)) {
        if (type == USRQUOTA) {
            if (uid == ruid)
                root_check = FALSE;
        }
        else {  /* type == GRPQUOTA */
#if SEC_BASE
            if (groupmember(uid, credp))
#else
            if (groupmember(uid))
#endif
                root_check = FALSE;
        }
    }

    if (root_check) {
        if (!suser()) {
            return (u.u_error);
        }
    }

    if ((uint32_t)type >= MAXQUOTAS) {
        return( EINVAL );
    }

    switch (cmds) {
	    
        case Q_QUOTAON:
            error = advfs_enforce_on(dnp, type);
            break;

        case Q_QUOTAOFF:
            error = advfs_enforce_off(dnp, type, FtxNilFtxH);
            break;

        case Q_SETQLIM:
        case Q_SETQUOTA:
        case Q_SETQLIM64:
        case Q_SETQUOTA64:
            error = advfs_set_quota(dnp, uid, type, cmds, arg, kernaddr);
            break;

        case Q_GETQUOTA:
            error = advfs_get_quota(dnp, uid, type, DQ_32BIT, arg, kernaddr);
            break;

        case Q_QUOTAINFO:
            error = advfs_get_quota_info(dnp, arg, kernaddr);
            break;

        case Q_GETQUOTA64:
            error = advfs_get_quota(dnp, uid, type, DQ_64BIT, arg, kernaddr);
            break;

     /*
      * NOTE: This case is handled above.
      * case Q_SYNC:
      *     error = EOK;
      *     break;
      */

        default:
            error = EINVAL;
            break;
    }

    /*
     * Generate event if needed.
     * Do not generate event for quotaon by recovery thread.
     * Event was already generated when quotas originally enabled.
     * A relocation/recovery should not generate another event.
     * Also, as written, generating an event during recovery could cause
     * a panic due to advfs_parsespec() calling into cfs_readlink()
     * which takes the CFS recovery lock which may already be held.
     */
    if ( (error == 0) && !(CLU_CFS_IS_RECOVERY_THREAD(0)) ) {
	    char *tempstring = NULL;
	    char *setName=NULL;
	    char *dmnName=NULL;
	    char uid_str[20];
	    advfs_ev *advfs_event;
	    int blockdevice = 0;
	    	    
	    tempstring = ms_malloc( MAXPATHLEN );
	    if (error = advfs_parsespec(dnp->f_mntfromname, 
					tempstring, 
					MAXPATHLEN, 
					&dmnName, 
					&setName, 
					&blockdevice)) {
		    ms_free(tempstring);
		    return error;
	    }
	    
	    advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
	    if (advfs_event == NULL) {
		    ms_free(tempstring);
		    return ENOMEM;
	    }
	    advfs_event->domain = dmnName;
	    advfs_event->fileset = setName;
	    
	    switch (cmds) {
		    
	    case Q_QUOTAON:
		    (void)advfs_post_kernel_event(EVENT_QUOTA_ON, advfs_event);
		    break;
		    
	    case Q_QUOTAOFF:
		    (void)advfs_post_kernel_event(EVENT_QUOTA_OFF, advfs_event);
		    break;
		    
	    case Q_SETQLIM:
	    case Q_SETQUOTA:
	    case Q_SETQLIM64:
	    case Q_SETQUOTA64:
		    sprintf(uid_str, sizeof(uid_str), "%u", uid);
		    if (type == GRPQUOTA) {
			    advfs_event->group = uid_str;
			    (void)advfs_post_kernel_event(EVENT_QUOTA_SETGRP,
							  advfs_event);
		    } else if (type == USRQUOTA) {
			    advfs_event->user = uid_str;
			    (void)advfs_post_kernel_event(EVENT_QUOTA_SETUSR,
							  advfs_event);
		    }
		    break;
		    
	    case Q_GETQUOTA:
	    case Q_QUOTAINFO:
	    case Q_GETQUOTA64:
		    break;
		    
	    default:
		    /* should never happen */
		    MS_SMP_ASSERT(0)
		    break;
	    }    
	    
	    ms_free(advfs_event);
	    ms_free(tempstring);
    }
    
    return( error );
}


/*
 * advfs_statvfs
 *
 * Return file system stats.
 *
 * Synchronization assumptions:
 *      -- File system isn't going anywhere.
 *      -- This routine and any called routine must not hold any locks
 *         to avoid lock hierarchy issue with the swap_lock being held
 *         by a swapon() request.
 */
int
advfs_statvfs(
    struct vfs *vfsp,
    struct statvfs *sbp )
{

    struct fileSetNode *dn;
    domainT *dmnp;
    bfSetT *bfSetp;
    long freeFiles;
    long filesUsed;
    long bfree;
    long bavail;
    long maxFiles;
    long totalBlocks;

    dn = ADVGETFILESETNODE(vfsp);               /* fileset specific info */
    bfSetp = ADVGETBFSETP(vfsp);                /* bitfile set info */
    dmnp = ADVGETDOMAINP(vfsp);                 /* domain wide info */

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
     * number of files.  If not, then set the maximum number to the
     * the MAX_ADVFS_TAGS amount. This work is done
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
                maxFiles = bs_get_avail_mcells_nolock(dmnp, dn);
        }
    } else {
        maxFiles = bs_get_avail_mcells_nolock(dmnp, dn);
    }

    /*
     * Do calculations without taking out locks. Note, since no lock is
     * being held on the filesetNode and domain their values aren't
     * synchronized, so we handle the possibility that subtractions between
     * their values might go negative
     */
    if ((dn->quotaStatus & QSTS_QUOTA_ON) &&
        (dn->quotaStatus & QSTS_FS_QUOTA_MT))
                filesUsed = dn->filesUsed;
    else
                filesUsed = bfSetp->bfCnt;

    freeFiles = MAX( 0, maxFiles - filesUsed );
    /* 
     * Note: The X/Open standard says:
     *          f_bfree    total number of free blocks
     *          f_bavail   number of free blocks available to non-privileged
     *                     process
     *
     *       Tru64 UNIX UFS would allow f_bavail to go negative because
     *       UFS has the concept of reserved space that may only be used by
     *       root and available blocks reflects the amount of storage that
     *       anyone can use.  Allowing UFS available blocks to go negative
     *       is how UFS can report greater than 100% of the space used in
     *       'df'.  Advfs does not have this concept, hence the check for a
     *       negative value 
     *
     *       However, AdvFS does not have a reserved blocks concept, so
     *       f_bfree and f_bavail should always be equal on AdvFS.
     */
    MS_SMP_ASSERT(dmnp->freeBlks <= LONG_MAX);
    MS_SMP_ASSERT(dn->blksUsed   <= LONG_MAX);
    bavail = totalBlocks - dn->blksUsed;        /* fileset quota adj free blks*/
    bavail = MIN( bavail, dmnp->freeBlks );     /* use domain free if smaller */
    bavail = MAX( 0, bavail );                  /* But don't go negative */
    bfree = bavail;                   /* AdvFS f_bavail and f_bfree are equal */

    /*
     * Fill in the statvfs structure.
     */

    sbp->f_bsize = dmnp->userAllocSz * ADVFS_FOB_SZ;          
                                        /* Preferred file system block size.
                                         * Other file systems appear to like
                                         * 8192 from looking at the output of
                                         * statvfs() calls 
                                         * AdvFS will return the default
                                         * user data allocation size. */

    sbp->f_frsize = DEV_BSIZE;          /* Fundamental file system block size */

    sbp->f_blocks = totalBlocks;        /* File System total data blocks in
                                         * f_frsize units.  This value on
                                         * HFS/UFS is smaller that the
                                         * f_size value.  The difference
                                         * appears to be the amount of inode
                                         * and cylinder group metadata.
                                         * Since our metadata usage is
                                         * dynamic we will use the same
                                         * value in both f_blocks and
                                         * f_size. */

    sbp->f_bfree = bfree;               /* File System total number of free
                                         * block in f_frsize units. */

    sbp->f_bavail = bavail;             /* Non-superuser available free
                                           blocks in f_frsize units. */

    sbp->f_files = maxFiles;            /* Total files the file system can
                                         * accomodate.  With fileset quotas
                                         * this is a fixed value, otherwise
                                         * we make an estimate based on the
                                         * amount of free space and the
                                         * number of existing files.  The
                                         * estimate is currently capped at
                                         * MAX_ADVFS_TAGS */

    sbp->f_ffree = freeFiles;           /* The number of additional files
                                         * that can be created on this file
                                         * system.  Since our metadata is
                                         * dynamicly allocated, this is a
                                         * bit of a spongy number.  For
                                         * example someone could create a
                                         * single file and use up all the
                                         * free space and then we could not
                                         * grow our metadata, so just a
                                         * moment ago we might have said
                                         * there are a million free inodes,
                                         * and then the next moment say
                                         * there are no free inodes */
    sbp->f_favail = freeFiles;          /* Non-superuser available free file
                                         * nodes. */

    strncpy(&(sbp->f_basetype[0]), "advfs\0", 6);

    sbp->f_namemax = MAXNAMLEN;         /* max.  file name length */

    sbp->f_fstr[32];                    /* This would be the Tru64 UNIX
                                         * f_mntonname (or at least the
                                         * first 32 bytes of it). */

    sbp->f_magic = ADVFS_MAGIC;          /* It would be nice if this value
                                         * was obtained from say the domain
                                         * structure, and then if we wanted
                                         * to change the magic number for
                                         * any reason, the code would
                                         * continue to work.  But to do that
                                         * get_raw_vd_attrs() would need to
                                         * be changed to store the value in
                                         * the domain for the first volume
                                         * mounted, and then compare it
                                         * against the stored value for
                                         * the other volumes of the same
                                         * domain */
 
    sbp->f_type = 0;                    /* type of info, zero for now (that is
                                         * what _statvfs_body.h says; so we
                                         * will set it to zero as well) */
 
    sbp->f_featurebits = 0;             /* File System features.  We will
                                         * start out with 0.  Using HFS as
                                         * and example, it has FSF_LFN (long
                                         * file names), FSF_B1 (B1 security),
                                         * FSF_LARGEFILES, FSF_LARGEUIDS,
                                         * the general idea was to tell the
                                         * calling software if this version
                                         * of the file system supported some
                                         * new feature that a previous
                                         * version did not.  You could also
                                         * get the same information by
                                         * associating a specific Domain
                                         * Version Number with what features
                                         * were added for each DVN change,
                                         * or the f_magic value could be
                                         * changed to also indicate a
                                         * different feature set.  Using
                                         * f_featurebits is a more explicit
                                         * way to specify a new feature. */
 
 /* sbp->f_flag */                      /*  f_flag is the only value that is
                                         *  _NOT_ being set in this routine.
                                         *  cstatvfs() is currently setting
                                         *  this value to ST_RDONLY,
                                         *  ST_NOSUID, ST_EXPORTED,
                                         *  ST_QUOTA, and/or ST_LARGEFILES.
                                         *  The only other xxx_statvfs()
                                         *  routines that set f_flag are the
                                         *  NFS related routines (autofs,
                                         *  nfs, and nfs3.  All the other
                                         *  file systems let cstatvfs() do
                                         *  the work.  However, f_flag will
                                         *  only be valid for the statvfs()
                                         *  syscall, and internal kernel
                                         *  VFS_STATVFS() callers will have
                                         *  to use the vfs structure to get
                                         *  the flag values if they want
                                         *  them */
 
    sbp->f_fsindex = vfsp->vfs_fsid[1]; /* For system use. This field
                                         * contains the vfs_fsid[1] of
                                         * structure vfsp. This is required
                                         * for statfs */
 
    sbp->f_fsid = vfsp->vfs_fsid[0];    /* File system ID */
 
    sbp->f_time = vfsp->vfs_mnttime;    /* Last time HFS Superblock was
                                         * written.  Now what do we do for
                                         * AdvFS.  We only write the
                                         * superblock when the domain is
                                         * created and then never again.
                                         * NFS, CDFS, and MVFS return -1.
                                         * We could also choose to return
                                         * some other date, like the mount
                                         * time, the last log file time, the
                                         * last write of the RBMT, etc... */
 
    sbp->f_cnode = 1;                   /* The statvfs structure comment
                                         * says "cluster node where
                                         * mounted", however, I don't think
                                         * this applies to a TruCluster
                                         * cluster.  Anyway, looking at the
                                         * HP-UX code, everyone sets it to
                                         * 1, so that is what I'm going to
                                         * do for now. */
 
    sbp->f_size = totalBlocks;          /* Size of FS in f_frsize units.
                                         * Because our metadata usage is
                                         * dynamic, we will use the same
                                         * value in both f_blocks and f_size */

    return (EOK);

}


/*
 * advfs_sync
 *
 * Notes:
 *
 *   This get called during initial boot, periodically while running,
 *   from a sync user command and during reboot/shutdown/panic.
 *
 *   For now, it will:
 *     - syncs a given mount point (vfsp exists) or all AdvFS mount
 *       points (vfsp==0) to flush the metadata and/or user data.
 *
 *     - SYNC_META flag:
 *       asynchronously flush all AdvFS filesystem metadata only
 *       periodically on behalf of the syncer threads.
 *       Tsync() calls update() who calls this routine only when running
 *       processor 0 sync thread. (Typically every 30 seconds.)
 *     - SYNC_NOLOCKWAIT flag:
 *       asynchronously flushes AdvFS filesystem metadata and user data
 *       for all AdvFS mount points or passed in mount point.
 *       This sync request will run only when there are no other active
 *       advfs_sync()'s. 
 *       A nowait flag is passed down through the flush so that 
 *       fcache_vn_flush() calls to issue IO do not wait on locks.
 *     - SYNC_REBOOT1 flag:
 *       This flag is not explicitly check, but AdvFS will flush all
 *       user data and metadata asynchronously for all AdvFS mount points
 *       assuming the caller set vfsp=0. Boot() passes this flag in via
 *       update() during reboot shutdown and also waits on the completion
 *       of all asynchronous UFC IO's started here.
 *     - SYNC_REBOOT2 flag:
 *       This is a noop for AdvFS.
 *     - SYNC_PANIC flag:
 *       The flag is not explicitly checked, but AdvFS will flush all
 *       user data and metadata asynchronously given that the SYNC_META flag
 *       is not set. Only panic_boot() passes in this flag and will also wait
 *       on the completion of all asynchronous UFC IO's started here.
 *       Note that a kernel may not have released locks prior to panicing
 *       and may potentially cause this routine to hang or deadlock
 *       panic. Making the attempt to save data to disk when possible is 
 *       the goal.
 *       A nowait flag is passed down through the flush so that 
 *       fcache_vn_flush() calls to issue IO do not wait on locks.
 *     - No flags:
 *       Flush meta data and user data asynchronously for single or
 *       all AdvFS mount points depending upon vfsp value.
 *
 */

/* Number of active advfs_sync() requests global counter. */
uint64_t advfs_sync_cnt = 0;

int
advfs_sync(
    struct vfs *vfsp,   /* in - mount structure pointer */
    int flags)          /* in - flags */
{
    extern domainT *DmnSentinelP;
    domainT *dmnP;
    uint64_t dummy;
    int32_t nowait_flag = FALSE; /* Default is for flush to wait on locks.*/

    /*
     * If there are no active domains then just return.
     * This initial check is done without locking to avoid
     * deadlocks in the panic path when there are no AdvFS filesystems
     * to sync.
     * The SYNC_REBOOT2 flag is a noop for AdvFS so just return.
     */
    if ((DmnSentinelP == NULL) || (flags & SYNC_REBOOT2))
        return (0);

    /* If the caller does not want to wait or the system is panicing then
     * return immediately if there are other AdvFS syncs. Otherwise, setup
     * a flag to pass to down through flush routines so that 
     * fcache_vn_flush() does not wait on locks.
     * Sync() and utssys() are the know callers setting SYNC_NOLOCKWAIT.
     * Those callers do not require a new sync if one is already active.
     */
    if (flags & (SYNC_NOLOCKWAIT | SYNC_PANIC)) {
        if (advfs_sync_cnt) {
            return 1;
        } else {
            /*
             *  temporarily remove until putpage handles NOWAIT better
             */
            /*nowait_flag = TRUE;*/
        }
    }

    /* Count the number of concurrent AdvFS sync requests. */
    ADVFS_ATOMIC_FETCH_INCR(&advfs_sync_cnt, &dummy);

    /*
     * If there is an active domain, then recheck the global while
     * holding the domain hash table lock.
     */
    ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
    if (DmnSentinelP == NULL) {
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        ADVFS_ATOMIC_FETCH_DECR(&advfs_sync_cnt, &dummy);
        return(0);
    }

    /*
     * Sync all AdvFS domains when the mount pointer is NULL.
     * Otherwise, sync just the mount point given.
     * Data is flushed asynchronously.
     * We should only be flushing valid domains.  If recovery is occuring,
     * we don't want to flush.  If the domain is not fully initialized...
     * we don't want to flush.
     */
    dmnP = (!vfsp) ? DmnSentinelP : ADVGETDOMAINP(vfsp);
    MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
    ++dmnP->dmnAccCnt; /* get reference so domain can't dissappear */
    ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );

    do { 
        if (!(dmnP->dmn_panic) && (dmnP->state == BFD_ACTIVATED)) {
            /* Asynchronously flush only dirty metadata if SYNC_META flag.
             * Otherwise, asynchronously flush both metadata and user data 
             * for all other cases (including SYNC_REBOOT1.)
             */
            if (flags & SYNC_META) {
                advfs_bs_dmn_flush_meta(dmnP, 
                        (nowait_flag) ?
                        FLUSH_STATS_ONLY | FLUSH_ASYNC | FLUSH_NOWAIT :
                        FLUSH_STATS_ONLY | FLUSH_ASYNC);
            }
            else
                advfs_bs_dmn_flush(dmnP, 
                        (nowait_flag) ? FLUSH_ASYNC|FLUSH_NOWAIT : FLUSH_ASYNC);
        }
        ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
        --dmnP->dmnAccCnt;
        if (dmnP->dmnAccCnt == 0 && dmnP->dmnRefWaiters) {
            dmnP->dmnRefWaiters = 0;
            cv_broadcast(&dmnP->dmnRefWaitersCV, NULL, CV_NULL_LOCK);
        }
        /* Get next domain to flush only if sync'ing all domains. */
        if (!vfsp) {
            dmnP = dmnP->dmnFwd;
            if (dmnP != DmnSentinelP)
                ++dmnP->dmnAccCnt;     /* add our reference */
            ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        }
        else {
            /* This single mount point flush is done so exit. */
            ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
            break;
        }
    } while (dmnP != DmnSentinelP);

    ADVFS_ATOMIC_FETCH_DECR(&advfs_sync_cnt, &dummy);
    return(0);
}


/*
 * advfs_vget() - Given the file system and a file id (i.e. the inode
 *                number), generate a vnode.  Primarily used by the NFS 3.2
 *                file handle code to look up the vnode for a file handle.
 */
advfs_vget(
    struct vfs *vfsp,          /* in  - VFS structure pointer */
    struct vnode **vpp,        /* out - vnode pointer of file */
    struct fid *fidp)          /* in  - pointer to file handle sturcture */
{
    struct vnode               *found_vp = NULL;
    statusT                     ret = 0;
    int                         flag = DONT_UNLOCK|IGNORE_DELETE;
    bfTagT                     *tagp;

    MS_SMP_ASSERT( sizeof(fidp->fid_data) >= sizeof(bfTagT) );

    *vpp = NULL;

    /* Turn off heirarchy from here on. This needs to come out
     * once we get the VM heirarchy fixed
     */
     ADVFS_VOP_LOCKSAFE;

    /*
     * screen domain panic
     */
    if ((ADVGETDOMAINP(vfsp))->dmn_panic) {
        ADVFS_VOP_LOCKUNSAFE;
        return EIO;
    }

    /*
     * If the tag is a pseudo-tag, we are opening a metadata file.
     */
    tagp = (bfTagT *) ms_malloc(sizeof(bfTagT));
    ADV_DECODE_FID(fidp,tagp,ret);
    if (ret != 0) {
        ms_free(tagp);
        ADVFS_VOP_LOCKUNSAFE;
        return(BSERRMAP(ret));
    }

    if (BS_BFTAG_PSEUDO(*tagp)) {
        flag |= META_OPEN;
    }

    ret = bf_get( *tagp,
                  0,
                  flag,
                  ADVGETFILESETNODE(vfsp),
                  &found_vp,
                  NULL,
                  NULL
                );

    ms_free(tagp);
    ADVFS_VOP_LOCKUNSAFE;

    if (ret != EOK) {
        return(BSERRMAP(ret));
    }
    *vpp = found_vp;
    return(0);
}

/*
 *  Find the dev_t for a fileset (fsDev)
 *
 *  The supplied path should point to an AdvFS filesystem, 
 *  for example:  /dev/advfs/myfs/default, /dev/dsk/c4t0d2, /dev/root
 *
 *  In the /dev/root case, we need to know how the admin intends
 *  this filesystem to be mounted in order to properly set the fsdev.
 *  The only way we can really find out is to read /etc/fstab and find
 *  the specification for the root filesystem.  
 *
 *  Note that clu_global_rootdev may not be meaningful when clusters
 *  support multi-volume root filesystems.  Coordinate with
 *  CFS when this is implemented to ensure we're doing the
 *  "right thing".
 */
static dev_t 
adv_get_fsdev(struct vfs* vfsp)
{
    int           sts;
    struct vnode  *vp = (struct vnode *)NULL;
    struct vnode  *vp2 = (struct vnode *)NULL;    
    dev_t         rdev = -1;
    char*         ppath;
    char          fspath[MAXPATHLEN+1] = {0};
    int           len;

    ppath = vfsp->vfs_name;
    
    if (strcmp(ppath, "/dev/root") == 0) {
        struct iovec iov[1];
        struct uio   uio;
        struct ucred *cred = TNC_CRED();
        char   buf[1024];
        char*  endln = NULL; 
        char*  ptr = NULL;
        char*  pptr = fspath;
        char   mntpath[MAXPATHLEN+1] = {0};
        int    mntfound = 0;
        int    fsfound = 0;
        int    skip_to_next_line = 0;

        /* initialize rdev */
        if (clu_type() != CLU_TYPE_SSI) {
            rdev = rootdev;
        } else {
            /* THIS MUST CHANGE WHEN MULTI-VOLUME ROOT IS SUPPORTED!!! */
            rdev = clu_global_rootdev[0];
        }

        sts = advfs_getvp(&vp, "/etc/fstab", UIOSEG_KERNEL, FOLLOW_LINK);

        if (sts != 0) {
	    if(vp != (struct vnode *)NULL)
            	VN_RELE(vp);
            return(rdev);
        }

        iov[0].iov_base = buf;
        iov[0].iov_len = sizeof(buf);

        uio.uio_iov = iov;
        uio.uio_iovcnt = 1;
        uio.uio_seg = UIOSEG_KERNEL;
        uio.uio_resid = iov[0].iov_len;
        uio.uio_fpflags = 0;
        uio.uio_offset = 0;
        uio.uio_old_offset = 0;

        
        sts = advfs_rdwr(vp, &uio, UIO_READ, 0, cred);

        /* fail if error reading or file is empty */
        if (sts != 0) {
            VN_RELE(vp);
            return(rdev);
        }

        ptr = buf;
        len = uio.uio_offset - uio.uio_old_offset;

        for ( ; len >= 0 ; len--, ptr++ ) {
            if (mntfound) {
                if (strcmp(mntpath, "/") == 0) {
                    break;     /* success! */
                } else {
                    mntfound = 0;
                    mntpath[0] = '\0';
                    skip_to_next_line = 1;
               }
            }

            if (len == 0) {
                /* get a new buffer */
                iov[0].iov_base = buf;
                iov[0].iov_len = sizeof(buf);
                uio.uio_resid = iov[0].iov_len;
                uio.uio_old_offset = uio.uio_offset;
                sts = advfs_rdwr(vp, &uio, UIO_READ, 0, cred);
                if (sts == 0) {
                    len = uio.uio_offset - uio.uio_old_offset;
                    if (len == 0) { /* end of file */
                        fspath[0] = '\0';
                        break;
                    }
                    ptr = buf;
                }
            }

            if (skip_to_next_line) {
                fsfound = 0;
                fspath[0] = '\0';
                pptr = fspath;

                endln = strchr(ptr, '\n');  /* see if there's a newline */
                if (endln == NULL) {  /* not in this buf */
                    len = 1;   /* force fetch of new buffer */
                } else {
                    len -= (endln - ptr);
                    ptr = endln;
                    skip_to_next_line = 0;
                }
                continue;
            }

            if (isspace(*ptr)) {
                if ((!fsfound) && (fspath[0] != '\0')) {
                    *pptr = '\0';
                    fsfound = 1;
                    pptr = mntpath;
                }
                if ((!mntfound) && (mntpath[0] != '\0')) {
                    *pptr = '\0';
                    mntfound = 1;
                }
                continue;
            }

            if (*ptr == '#') {  /* comment line */
                skip_to_next_line = 1;
                continue;
            }

            if (!fsfound || !mntfound) {
                *pptr = *ptr;
                pptr++;
            }
        }

        VN_RELE(vp);

        if (fspath[0] == '\0') {
            return(rdev);
        } 

        /* remove trailing slashes */
        len = strlen(fspath);
        while ((len > 1) && (fspath[len] == '/')) {
            fspath[len] = '\0';
            len--;
        }

        ppath = fspath;
    } 

    /*  On CFS clusters, cluster_root is _always_ minor number 1 with
     *  bit 23 set for TCR.  Move this to advfs_syscalls.h later
     */
    if ((clu_type() == CLU_TYPE_CFS) && (!strcmp(ppath, TCRCFSMNTFROM))) {
        uint32_t     min = (1 | (1 << 22));

        rdev = makedev(ADVFSDEV_MAJOR, min);
        return(rdev);
    }

    sts = advfs_getvp(&vp, ppath, UIOSEG_KERNEL, FOLLOW_LINK);
    
    /*  if a storage domain directory was passed in, try to handle it */
    if ((sts == 0) && (vp->v_type == VDIR)) {  
        char* dmnName;
        char  dmnPath[MAXPATHLEN+1];

        /* set sts to failure, in case it's really a bogus entry */
        sts = EINVAL;

        /* save our original vp */
        vp2 = vp;

        dmnName = strrchr(ppath, '/');
        MS_SMP_ASSERT(dmnName != NULL);
        dmnName++;

        /*  don't depend on strcmp() as there's too many possible
         *  variations on the path.  See if the vnodes match.
         */

        if (clu_type() == CLU_TYPE_CFS) {   
            /* see if it's a shared filesystem */
            sprintf(dmnPath, MAXPATHLEN+1, "%s/%s", ADVFS_TCR_DMN_DIR, dmnName);
            sts = advfs_getvp(&vp, dmnPath, UIOSEG_KERNEL, FOLLOW_LINK);
        }

        if (sts != EOK) {  /* no match yet */
            sprintf(dmnPath, MAXPATHLEN+1, "%s/%s", ADVFS_DMN_DIR, dmnName);
            sts = advfs_getvp(&vp, dmnPath, UIOSEG_KERNEL, FOLLOW_LINK);
        }
        
        if (sts == EOK) {
            if (vp != vp2) {  /* drat! */
                sts = EINVAL;
                VN_RELE(vp);
            }
        }
        VN_RELE(vp2);
    }

    if (sts == 0) {
        rdev = vp->v_rdev;
        VN_RELE(vp);
        if (strcmp(vfsp->vfs_name, "/dev/root") == 0) {
            /*  update root vfs path */
            strncpy(vfsp->vfs_name, ppath, MAXPATHLEN);
            vfsp->vfs_name[MAXPATHLEN-1] = '\0';
        }
    }

    return(rdev);
}

/*
 * chk_set_fsdev()
 * Verifies the file set has the correct file set device and
 * updates it if not. Then the file set attributes are updated on
 * disk with the new file set device identifier.
 *
 */
static
statusT
chk_set_fsdev(struct vfs *vfsp) /* In: mount point to perform fsdev update. */
{
    statusT         sts = EOK;
    bsBfSetAttrT    fsAttr_rec;
    bfSetT         *bfSetp;
    fileSetNodeT   *fsn;
    ftxHT           ftxH;
    int32_t         fsId;
    struct vfs*     ckvfsp = NULL;

    bfSetp = ADVGETBFSETP(vfsp);
    fsn = ADVGETFILESETNODE(vfsp);

    /* Either "check and set" or just "set" the file set device. */

    fsId = adv_get_fsdev(vfsp);

    if (fsId == -1) {
        return(E_BAD_DEV);
    }

    /* Verify that this fsid is not already in use.  This can happen
     * if the filesystem directory was deleted out from under a mounted
     * filesystem.  While this is unlikely, it's not impossible, and 
     * we need to ensure that we don't corrupt VFS by adding a different
     * filesystem with an existing fsid.
     */

    ckvfsp = devtovfs(fsId);
    if (ckvfsp != NULL) {
        /* don't fail if mountfroms are the same */
        if (strcmp(vfsp->vfs_name, ckvfsp->vfs_name)) {
            sts = E_BAD_DEV;
        }
        vfs_rele(ckvfsp);
    }

    if (sts != EOK) {
        return(sts);
    }

    if (bfSetp->bfs_dev == fsId) {
        /* nothing to do here */
        return(EOK);
    }

    /* Update disk version */

    /*
     * We have to start the transaction before locking the mcell
     * list for ordering consistency.
     *
     */
    sts = FTX_START_N( FTA_BS_BMT_UPDATE_REC_V1, &ftxH, FtxNilFtxH,
                          bfSetp->dmnP);
    if (sts != EOK) {
        ADVFS_SAD1( "chk_set_fsdev : ftx start failed", sts );
    }

    sts = bmtr_get_rec_n_lk(bfSetp->dirBfAp,
                                BSR_BFS_ATTR,
                                &fsAttr_rec,
                                sizeof(bsBfSetAttrT),
                                BMTR_LOCK
                                );
    if (sts != EOK) {
        ADVFS_CRASH_RECOVERY_TEST(bfSetp->dmnP, AdvfsCrash_487);
        ftx_fail( ftxH );
        ADVFS_CRASH_RECOVERY_TEST(bfSetp->dmnP, AdvfsCrash_488);
        uprintf("ADVFS: Update of fileset dev record failed for %s\n",
                       fsn->f_mntfromname);
        uprintf("ADVFS: This will result in stale NFS file handles\n");
        uprintf("ADVFS: when this file set is re-mounted,\n");
        uprintf("ADVFS: forcing NFS clients to re-mount.\n");
        return(EOK);  /* this is a non-fatal error */
    }

    /* Assign the file set device into the attributes record. */

    fsAttr_rec.fsDev = fsId;

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
        ADVRWL_MCELL_LIST_UNLOCK( &bfSetp->dirBfAp->mcellList_lk );
        ADVFS_CRASH_RECOVERY_TEST(bfSetp->dmnP, AdvfsCrash_489);
        ftx_fail( ftxH );
        ADVFS_CRASH_RECOVERY_TEST(bfSetp->dmnP, AdvfsCrash_490);
        uprintf("ADVFS: Update of fileset dev record failed for %s\n",
                       fsn->f_mntfromname);
        uprintf("ADVFS: This will result in stale NFS file handles\n");
        uprintf("ADVFS: when this fileset is re-mounted,\n");
        uprintf("ADVFS: forcing NFS clients to re-mount\n");
        return(EOK);  /* this is a non-fatal error */
    } else {
        ADVFS_CRASH_RECOVERY_TEST(bfSetp->dmnP, AdvfsCrash_19);
        ftx_done_n( ftxH, FTA_BS_BMT_UPDATE_REC_V1 );
        ADVFS_CRASH_RECOVERY_TEST(bfSetp->dmnP, AdvfsCrash_20);
    }

    bfSetp->bfs_dev = (dev_t)fsAttr_rec.fsDev;
    
    return(EOK);
}



int
advfs_freeze ( struct vfs *vfsp, int flags, int timeout )
{
	MS_SMP_ASSERT((flags == ADVFS_Q_QUERY) || (flags == ADVFS_Q_FREEZE));
	return (advfs_freeze_thaw_common ( vfsp, flags, timeout ));
}
	
int
advfs_thaw ( struct vfs *vfsp )
{
	return (advfs_freeze_thaw_common ( vfsp, ADVFS_Q_THAW, 0 ));
}

/*
 *
 *  Freeze/thaw a domain
 *
 *  This routine does some preliminary checking for multiple freeze/thaw
 *  requests and then 'sends' a packaged message to the bs_freeze background
 *  thread. It then waits for the background thread to process the message
 *  before returning to the caller.
 *
 */

int
advfs_freeze_thaw_common ( struct vfs *vfsp, int flags, int timeout )
{
    struct fileSetNode *fsnp;
    domainT            *dmnP;
    int                 error;
    char                uid_str[20];
    advfsFreezeMsgT    *msg;

    MS_SMP_ASSERT((flags == ADVFS_Q_QUERY) || (flags == ADVFS_Q_FREEZE) ||
		  (flags == ADVFS_Q_THAW));
    MS_SMP_ASSERT((flags != ADVFS_Q_THAW) || (timeout == 0));
    
    /*
     *  Get the domain pointer
     */
    fsnp = ADVGETFILESETNODE(vfsp);
    dmnP = fsnp->dmnP;

    /*
     *  Check for domain panic
     */
    if ((dmnP->dmn_panic) && (flags == ADVFS_Q_FREEZE)) {
        return EIO;
    }

    /*
     *  If a freeze request, check if freezing or frozen.
     *  If a thaw request, check if not freezing or frozen
     */
    ADVMTX_DOMAINFREEZE_LOCK(&dmnP->dmnFreezeMutex);
    if (flags & ADVFS_Q_FREEZE) {
        if (dmnP->dmnFreezeFlags & BFD_FREEZE_IN_PROGRESS) {
            ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
            return EINPROGRESS;
        } else if ( dmnP->dmnFreezeFlags & BFD_FROZEN) {
            ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
            return EALREADY;
        } else {
            dmnP->dmnFreezeFlags |= BFD_FREEZE_IN_PROGRESS;
        }
    } else if (flags & ADVFS_Q_QUERY) {
        if (dmnP->dmnFreezeFlags & BFD_FROZEN) {
            ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
            return ENOSPC;  /* status of frozen reported as "no space" */
        } else {
            ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
            return 0;  /* status of not frozen reported as "success" */
        }
    } else {
        if ( !(dmnP->dmnFreezeFlags & (BFD_FREEZE_IN_PROGRESS + BFD_FROZEN)) ) {
            ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);
            return EALREADY;
        }
    }
    ADVMTX_DOMAINFREEZE_UNLOCK(&dmnP->dmnFreezeMutex);

    /*
     *  Send a message to the background thread
     *  and wait for it to be processed
     */
    msg = (advfsFreezeMsgT *) ms_malloc( sizeof(advfsFreezeMsgT) );
    msg->frzmDomainId    = dmnP->domainId;
    msg->frzmFlags       = (uint64_t)flags;
    msg->frzmTimeout     = (int64_t)timeout;
    msg->frzmFSNP        = fsnp;
    msg->frzmStatus      = 0;
    cv_init(&msg->frzmStatusCV, "frzmStatusCV", NULL, CV_WAITOK);

    /* protect the list and add this message */
    ADVMTX_FREEZE_MSGS_LOCK(&AdvfsFreezeMsgsLock);
    msg->frzmLink        = AdvfsFreezeMsgs;
    AdvfsFreezeMsgs      = msg;

    /* wake bs_freeze_thread */
    cv_signal( &AdvfsFreezeMsgsCV, NULL, CV_NULL_LOCK );

    /* wait for completion and status for this message only */
    cv_wait(&msg->frzmStatusCV, &AdvfsFreezeMsgsLock, CV_MUTEX, CV_NO_RELOCK);

    error = msg->frzmStatus;
    cv_destroy(&msg->frzmStatusCV);
    ms_free(msg);
    return( error );
}

/*
 * advfs_size() - Return the size of the file system space used on the
 *                specified device.
 *
 *                The primary consumers are dumpconf_device() looking for
 *                dump device, and check_swap_conditions() looking for swap
 *                area.
 *
 *                I borrowed a lot of the code from ufs_size().
 *
 * Input:       dev_t
 * Return:      -1 - Unable to check size (I/O error or looks like AdvFS but
 *                   it was either corrupt, or an on-disk version that this
 *                   code does not understand).
 *               0 - Not part of an AdvFS file system.
 *              >0 - The number of DEV_BSIZE sectors on the specified device
 *                   that are part of the AdvFS file system.
 */

kern_daddr_t
advfs_size(dev_t dev)
{
    struct vnode            *dev_vp;
    char                    *readbuf;
    advfs_fake_superblock_t *superBlk;
    struct bsMPg            *rbmt;
    struct bsVdAttr         *vdAttrp;
    kern_daddr_t             devblks;
    statusT                  sts;
    uint32_t                 readbuf_size;


    if ( opend(&dev, S_IFBLK | IF_MI_DEV, FREAD, 0) ) {
        return -1;
    }
    dev_vp = devtovp(dev);

    /* only return -1 on error or we'll wipe out the filesystem */
    devblks = -1;

    readbuf_size = (ADVFS_SUPER_BLOCK_SZ > ADVFS_METADATA_PGSZ) ?
        ADVFS_SUPER_BLOCK_SZ : ADVFS_METADATA_PGSZ;
    readbuf = ms_malloc( readbuf_size );

    /*
     * Get the AdvFS Fake Superblock to check the Magic number
     */
    superBlk = (advfs_fake_superblock_t *) readbuf;

    sts = advfs_raw_io(dev_vp,
		       ADVFS_FAKE_SB_BLK,
		       ADVFS_SUPER_BLOCK_SZ / DEV_BSIZE,
		       RAW_READ,
		       superBlk);

    if (sts != EOK) {
        goto _error;
    }

    if ( !ADVFS_VALID_FS_MAGIC(superBlk) ) {
        /*
         * Sorry, wrong magic number; this is not an AdvFS volume.
         */
        devblks = 0;
        goto _error;
    }

    /*
     * Fetch the RBMT so that we can get the AdvFS version number and the
     * Volume attributes (which contain the size of the volume).
     */
    rbmt = (struct bsMPg *) readbuf;

    sts = advfs_raw_io( dev_vp, 
			RBMT_BLK_LOC, 
			ADVFS_METADATA_PGSZ_IN_FOBS /
			ADVFS_FOBS_PER_DEV_BSIZE,
			RAW_READ,
			rbmt);

    if (sts != EOK) {
        goto _error;
    }

    if ( rbmt->bmtODSVersion.odv_major > BFD_ODS_LAST_VERSION_MAJOR) {
        /*
         * Sorry, but the on-disk version is too new for this code to
         * properly handle.  Things might not be where we expect them.
         */
        goto _error;
    }


    /*
     * Fetch the volume attributes record from the RBMT. 
     */
    vdAttrp = bmtr_find(&rbmt->bsMCA[BFM_RBMT], BSR_VD_ATTR, NULL);
    if (vdAttrp == NULL) {
        goto _error;
    }

    /*
     * Is this volume index within the valid range for volumes
     */
    if ( TEST_VDI_RANGE(vdAttrp->vdIndex) != EOK ) {
        goto _error;
    }

    /*
     *  grab the volume size
     */
    devblks = (kern_daddr_t) vdAttrp->vdBlkCnt;

_error:
    (void) closed(dev, S_IFBLK, FREAD);
    if (readbuf) {
	ms_free( readbuf );
    }
    return devblks;
}

/* This routine is provided primarily for CFS to determine if a fileset
 * has been mounted with the hidden -o directio option.  It is passed a
 * pointer to the vfs structure, and returns TRUE if the fileset has
 * directIO enabled, and FALSE otherwise.
 */
advfs_vfs_is_mounted_for_dio( struct vfs *vfsp )
{
    struct bfSet *bfSetp = ADVGETBFSETP( vfsp );

    if ( BFSET_VALID( bfSetp ) ) {
        if ( bfSetp->bfSetFlags & BFS_IM_DIRECTIO ) {
            return TRUE;
        }
    }
    return FALSE;
}

/* 
 * Non-locking version of bs_get_avail_mcells(). This version
 * called when locking not permitted (e.g. swap filesystem).
 * Note that calculation is for current fileset only. Initial
 * port to HP-UX supports only one fileset per domain. If this
 * changes then consideration could be given to traversing all
 * filesets in domain (however this is difficult without locking).
 */
int64_t
bs_get_avail_mcells_nolock(
    domainT *dmnP,
    struct fileSetNode *dn
    )
{
    int64_t potentialMcells;
    int64_t availMcells;


    /* Rough estimate of potential number of files based on current
       number of free blocks in domain (currently same as fileset) */
    potentialMcells = ((dmnP->freeBlks / dmnP->metaAllocSz) * BS_TD_TAGS_PG
		       * BSPG_CELLS) / (BS_TD_TAGS_PG + BSPG_CELLS);

    /* Add in number of files used */
    availMcells = dn->filesUsed + potentialMcells;

    /* Make sure we don't report more than the max */
    if (availMcells > MAX_ADVFS_TAGS) {    
        availMcells = MAX_ADVFS_TAGS;
    }

    return availMcells;
}

/* end msfs_vfsops.c */
