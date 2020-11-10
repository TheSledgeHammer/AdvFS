/*****************************************************************************
 **                                                                          *
 **  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991, 1992, 1993                *
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
 * Facility:
 *
 *   AdvFS
 *
 * Abstract:
 *
 *   This module contains routines to create, open, truncate and close
 *   a file, and update a file's stats.
 *
 * Date:
 *
 *   14-May-1990
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fs_create.c,v $ $Revision: 1.1.225.5 $ (DEC) $Date: 2006/03/20 15:11:00 $"

#include <msfs/fs_dir_routines.h>
#include <msfs/ms_public.h>
#include <msfs/fs_dir.h>
#include <sys/stat.h>
#include <msfs/fs_quota.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_access.h>
#include <msfs/fs_file_sets.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <msfs/ms_osf.h>
#include <sys/kernel.h>
#include <sys/vnode.h>
#include <sys/namei.h>
#include <sys/mount.h>
#include <sys/user.h>
#include <sys/ucred.h>
#include <sys/lock_probe.h>
#include <msfs/bs_params.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_index.h>

#define ADVFS_MODULE FS_CREATE

/*
 * SVID III access rules vs BSD.
 * sys_v_mode predates the grpid mount option, but at present we support
 * both.  This affects group ownership of new inodes and preservation of
 * ISGID over modifications.
 */
extern int sys_v_mode;
#define BSD_MODE(vp) (((vp)->v_mount->m_flag & M_GRPID) && !sys_v_mode)

statT NilStats = { 0 };


/*
 *   fs_create_file
 *
 *   Create a new file.
 *   Assumes the caller has already determined that the
 *   file does not already exist.
 *
 */

statusT
fs_create_file(struct vattr *vap,         /* in - vnode attributes pointer */
               struct nameidata *ndp)     /* in - nameidata pointer */
{              
    char        *block_buffer;
    statusT      ret;
    bfTagT       new_file_tag;
    rbfPgRefHT   page_ref;
    bfAccessT   *bfap;
    bfParamsT   *new_bs_paramsp = NULL;
    ftxHT        ftx_handle;
    int          create_symlink;
    int          error = 0;
    int          i;
    long         blks;
    int          mode;
    domainT     *dmnP;
    bfSetT      *bfSetp;
    bfSetIdT     the_bfset_id;
    int          create_directory;
    bfTagT       dir_bs_tag;
    bfAccessT   *dir_bfap;
    bfAccessT   *idx_bfap = NULL;
    u_int        effective_LINK_MAX;
    u_long       ids[MAXQUOTAS];
    boolean_t    quota_mig_trunc_lock[MAXQUOTAS];
    int          qtype;
    
    enum vtype   type;

    struct mount       *mp;
    struct fileSetNode *fsNp;
    struct fsContext   *new_file_cp, *dir_cp;
    struct vnode       *nvp = NULL, *dvp = NULL;
    struct bfNode      *bnp;
    
    extern struct vnodeops msfs_vnodeops;
    extern struct vnodeops spec_bfnodeops;
    extern struct vnodeops fifo_bfnodeops;
    
    extern int    xlate_dev(dev_t, int);


    /*-----------------------------------------------------------------------*/

    new_bs_paramsp = (bfParamsT *) ms_malloc( sizeof( bfParamsT ));
    
    /*
     * get the domain pointer for the to-be-inserted-in directory
     */
    mp = VTOMOUNT(ndp->ni_dvp);
    fsNp = GETFILESETNODE(mp);
    dmnP = GETDOMAINP (mp);

    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        vrele(ndp->ni_dvp);
        ms_free( new_bs_paramsp );
        return EIO;
    }
      
    /*
     * get the access pointer of the directory
     */
    bfSetp = GETBFSETP(mp);
    the_bfset_id = GETBFSET(mp);
    dvp = ndp->ni_dvp;
    dir_bfap = VTOA(dvp);
    dir_cp = VTOC( dvp );

    type = vap->va_type;
    mode = MAKEMODE(vap->va_type, vap->va_mode);

    if ((mode & S_IFMT) == S_IFDIR) {
        create_directory = TRUE;
        /* 
         * Set the value of effective_LINK_MAX:
         *   DVN3 uses signed short st_nlink
         *   DVN4 (or later) uses unsigned short
         */
        if ( dmnP->dmnVersion < FIRST_64K_LINK_MAX_VERSION )
            effective_LINK_MAX = _LINK_MAX_SHORT;
        else
            effective_LINK_MAX = _LINK_MAX_USHORT;

        if ( dir_cp->dir_stats.st_nlink >= effective_LINK_MAX ) {
            vrele(dvp);
            ms_free( new_bs_paramsp );
            return EMLINK;
        }
    } else
        create_directory = FALSE;

    if ((mode & S_IFMT) == S_IFLNK) {
        create_symlink = TRUE;
    } else
        create_symlink = FALSE;
    
    ret = getnewvnode( VT_MSFS, &msfs_vnodeops, &nvp );
    if (ret == EOK) {
        /*
         * Initialize nvp. Note that get_n_setup_vnode called
         * from bs_access will do most of the initialization.
         * Here, we do enough such that any error returns do
         * proper cleanup.
         */
        bnp = (struct bfNode *)&nvp->v_data[0];
        bnp->fsContextp = NULL;
        bnp->accessp = NULL;
    } else {
        error =  ret;
        goto fail_out1;
    }

    FS_FILE_WRITE_LOCK (dir_cp);

    /*
     * start the transaction.  Note: if not running under CFS,
     * ndp->ni_xid must be zero to guarantee uniqueness of
     * ftxId in the transaction log.
     */

    /* Prevent races with migrate code paths */
    /* Lock the directory */
    /* Can't be a clone */
    MS_SMP_ASSERT(dir_bfap->bfSetp->cloneId == BS_BFSET_ORIG);

    /* Need to take the directory lock because of the call chain
     * insert_seq --> rbf_add_stg
     */
    MIGTRUNC_LOCK_READ( &(dir_bfap->xtnts.migTruncLk) );

    /* Need to take the fileset tag directory lock because of
     * the call chain
     * rbf_create --> rbf_int_create --> tagdir_alloc_tag -->
     * init_next_tag_page --> rbf_add_stg
     */
    MIGTRUNC_LOCK_READ( &(bfSetp->dirBfAp->xtnts.migTruncLk) );

    /* Need to lock the index file because of the call chain
     * insert_seq --> idx_directory_get_space-->idx_directory_insert_space -->
     * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
     * rbf_add_stg
     */
    IDX_GET_BFAP(dir_bfap, idx_bfap);
    if (idx_bfap != NULL) { 
        MIGTRUNC_LOCK_READ( &(idx_bfap->xtnts.migTruncLk) );
    }

    /* May need to take the quota locks because of the call chain
     * chk_{blk, bf}_quota --> dqsync --> rbf_add_stg
     */
    /* the group id will most likely be that of the parent directory
     * (which is mapped in the quota file already), except in the
     * case of svid III compatibility, with the set-gid bit on the parent
     * not set.
     */
    ids[USRQUOTA] = (unsigned long) ndp->ni_cred->cr_uid;
    ids[GRPQUOTA] = (unsigned long) ndp->ni_cred->cr_gid;
    for (qtype = 0; qtype < MAXQUOTAS; qtype++) {
        quota_mig_trunc_lock[qtype] = FALSE;
        /* if the type is user and the page is not mapped, take the lock.
         * if the type is group and svid3 compatibility mode is on
         * and the setgid bit on the parent is not set AND the page is
         * not mapped, take the lock.
         * see svid III compatibility below.
         */
        if (((qtype == USRQUOTA) ||
             (qtype == GRPQUOTA && !BSD_MODE(ndp->ni_dvp) && 
             !(dir_cp->dir_stats.st_mode & S_ISGID))) &&
            !quota_page_is_mapped(fsNp->qi[qtype].qiAccessp, ids[qtype])) {
            MIGTRUNC_LOCK_READ( &(fsNp->qi[qtype].qiAccessp->xtnts.migTruncLk) );
            quota_mig_trunc_lock[qtype] = TRUE;
        }
    }

    ret = FTX_START_XID( FTA_FS_CREATE_1, &ftx_handle, FtxNilFtxH,
                         dmnP, 5, ndp->ni_xid );
    if (ret != EOK) {
        domain_panic(dmnP, "Error in ftx_start in fs_create_file. Status is %d",
                     ret);
        error = ret;
        goto fail_out_no_ftx_fail_needed;
    }

    /*
     * Create a new bitfile and access it. If its not a directory
     * create, save the bitfile access handle in the user's fd table.
     * The size (in blocks) is currently a constant from fs_dir.h 
     */

    if (create_directory) {
        new_bs_paramsp->pageSize = ADVFS_PGSZ_IN_BLKS;
        new_bs_paramsp->cl.dataSafety = BFD_FTX_AGENT;
    } else {
        new_bs_paramsp->pageSize = ADVFS_PGSZ_IN_BLKS;
    }

    ret = rbf_create(&new_file_tag, bfSetp, new_bs_paramsp, ftx_handle, TRUE);
    if (ret != EOK) {
        if (ret == E_READ_ONLY) {
            error = EROFS;
        } else {
            error = BSERRMAP( ret );
        }
        goto fail_out;
    }

    ret = bs_access(
                    &bfap,
                    new_file_tag,
                    bfSetp,
                    ftx_handle,
                    BF_OP_HAVE_VNODE,
                    mp,
                    &nvp
                    );
    if (ret != EOK) {
        error = BSERRMAP( ret );
        goto fail_out;
    }

    bnp = (struct bfNode *)&nvp->v_data[0];

    bnp->tag = new_file_tag;
    bnp->bfSetId = the_bfset_id;

    new_file_cp = vnode_fscontext_allocate( nvp );
    new_file_cp->dir_stats = NilStats;
    new_file_cp->undel_dir_tag = NilBfTag;
    new_file_cp->last_offset = 0;

    /*
     * Fill in the uid and gid early for the benefit of the quota system.
     */
    new_file_cp->dir_stats.st_uid = ndp->ni_cred->cr_uid;
    new_file_cp->fs_flag = 0;

    if (BSD_MODE(ndp->ni_dvp)) {
        new_file_cp->dir_stats.st_gid = dir_cp->dir_stats.st_gid;
#if SEC_BASE
        if((new_file_cp->dir_stats.st_mode & S_ISGID) &&
            !groupmember( new_file_cp->dir_stats.st_gid, ndp->ni_cred ) &&
            !privileged(SEC_SETPROCIDENT, 0)) {
#else
        if((new_file_cp->dir_stats.st_mode & S_ISGID) &&
            !groupmember( new_file_cp->dir_stats.st_gid, ndp->ni_cred ) &&
            suser(ndp->ni_cred, NULL)) {
#endif
            new_file_cp->dir_stats.st_mode &= ~S_ISGID;
            mode &= ~S_ISGID;
        }
    } else {
        /* svid III compatibility */
        if (dir_cp->dir_stats.st_mode & S_ISGID) {
            /*
             * Parent directory has the ISGID bit set.  Therefore
             * new nodes' group id becomes the group id of the
             * parent directory.
             */
             new_file_cp->dir_stats.st_gid = dir_cp->dir_stats.st_gid;
             if (create_directory) {
                 /* new directory gets the ISGID bit set */
                 mode |= S_ISGID;
             }
        } else {
            /*
             * Parent directory does NOT have the ISGID bit set.
             * Therefore new nodes' group id becomes the group id (egid)
             * of the calling process.
             */
            new_file_cp->dir_stats.st_gid = ndp->ni_cred->cr_gid;

            if (!(vap->va_mode & S_ISGID)) {
                /*
                 * If we call open & forcibly set the ISGID bit
                 * in the mode field then we WILL NOT fall
                 * here.  If we do fall here, it is because
                 * someone did a 'normal' open call, so we
                 * must turn off the ISGID bit.
                 * - It is not normal to set ISGID with open.
                 */
                new_file_cp->dir_stats.st_mode &= ~S_ISGID;
            }
        }

        if (!groupmember(new_file_cp->dir_stats.st_gid, ndp->ni_cred)) {
            new_file_cp->dir_stats.st_mode &= ~S_ISGID;
        }
    }

    /*
     * Set vnode ops for special files.
     */
    switch (type) {
        case VREG: {
            /*
             * If the cache policy is direct I/O, set the appropriate
             * flag in the vnode.  The exception to this is if the
             * file is going to use data logging.  The only case in
             * which a file might already be marked as a data logging
             * file this early on is if the AdvfsAllDataLogging configurable
             * variable has been set.
             */
            if (bfSetp->bfSetFlags & BFS_IM_DIRECTIO) {
                VN_LOCK(nvp);
                if ((bfap->dataSafety != BFD_FTX_AGENT) &&
                    (bfap->dataSafety != BFD_FTX_AGENT_TEMPORARY)) 
                    nvp->v_flag |= VDIRECTIO;
                else 
                    nvp->v_flag &= ~VDIRECTIO;
                VN_UNLOCK(nvp);
            }
            break;
        }
        case VFIFO:
            nvp->v_op =  &fifo_bfnodeops;
            break;
        case VSOCK:
            nvp->v_socket = (struct socket *) vap->va_socket;
            break;
        case VBLK:
        case VCHR:
            if (((vap->va_rdev >> 16) & 0xffff) == 0) {
                /*
                 * TEMPORARY - XXX (Cribbed from ufs_inode.c)
                 *
                 * We have an old-style dev_t or a new-style dev_t for
                 * major number zero.  We next check for the old-style
                 * major number field (bits 7->15).  If zero, we have
                 * major number zero, of either the old or new format.
                 * If non-zero, we have an old-style dev_t format.
                 *
                 * Note, if an old-style dev_t (16 bit) is detected,
                 * we will convert it to 32 bits and write out the
                 * converted dev_t to disk.
                 */
                if (((vap->va_rdev >> 8) & 0xff) == 0) {
                    /*
                     * NOTE: Major number zero is restricted to an
                     * 8-bit minor so that translation from old to
                     * new format can be performed.
                     */
                    vap->va_rdev = makedev( 0, (vap->va_rdev & 0xff) );
                } else {
                    /*
                     * We have an old format dev_t so call the
                     * translation routine, which will translate
                     * old to new.
                     */
                    vap->va_rdev = xlate_dev(vap->va_rdev, 1);
                }
            }

            if (error = specalloc(nvp, vap->va_rdev)) {
                VN_LOCK(nvp);
                nvp->v_type = VNON;
                VN_UNLOCK(nvp);
                goto fail_out;
            }
            new_file_cp->dir_stats.st_rdev = vap->va_rdev;
            VN_LOCK(nvp);
            nvp->v_op = &spec_bfnodeops;
            VN_UNLOCK(nvp);
            break;
    }
    nvp->v_type = type;

    /*
     * If this file type doesn't use ubc, don't keep unneeded ubc object.
     * Vnode hasn't made it to mount queue yet, bfAccess is on hash queue
     * so must serialize with bs_access_one().
     */
    if ( type == VCHR || type == VBLK || type == VFIFO ) {
        struct vm_ubc_object *obj;
        mutex_lock(&bfap->bfaLock);
        if (obj = bfap->bfObj) {
            bfap->bfObj = NULL;
            VN_LOCK(nvp);
            nvp->v_object = NULL;
            VN_UNLOCK(nvp);
            obj->vu_object.ob_ref_count -= 1;
            mutex_unlock(&bfap->bfaLock);
            ubc_object_free(obj);
        } else {
            mutex_unlock(&bfap->bfaLock);
        }
    }
            
    /*
     * Enter new file onto mount queue
     */
    bs_insmntque(ftx_handle, bfap, mp);

    /*
     * Check that the the total number of bitfiles that the user may
     * own is not exceeded.
     */
    new_file_cp->fileSetNode = fsNp;
    for (i = 0; i < MAXQUOTAS; i++) {
        new_file_cp->diskQuot[i] = NULLDQUOT;
    }
    new_file_cp->quotaInitialized = TRUE;

    /*
     * Pages are allocated to the new file if it is either a directory
     * or a long symbolic link.  Check that the user's block usage
     * is not exceeded.
     */

    blks = 0L;

    if (create_directory) {
        blks = ADVFS_PGSZ_IN_BLKS; /* quota blocks */
    }

    if (create_symlink) {
        int len = strlen(vap->va_symlink);
        if (len > bmtr_max_rec_size()) {
            int npages = (len + ADVFS_PGSZ -1)/ ADVFS_PGSZ;
            blks = npages * ADVFS_PGSZ_IN_BLKS; /* pages to quota blocks */
        }
    }

    error = attach_quota(new_file_cp, ftx_handle);
    if (error)
        goto fail_out;
    error = change_quotas(nvp, 1, blks, NULL, ndp->ni_cred, 0, ftx_handle);
    if (error)
        goto fail_out;

    /*
     * Write the BMTR_FS_STAT mcell record.
     * In del_clean_mcell_list(), we assume that the BMTR_FS_STAT record
     * always resides in the primary mcell. If this is to be changed, change
     * del_clean_mcell_list() accordingly
     */
    ret = bmtr_put_rec(bfap,
                       BMTR_FS_STAT,
                       (void *)&new_file_cp->dir_stats,
                       sizeof(statT),
                       ftx_handle );
    if (ret != EOK) {
        if (ret == ENO_MORE_MCELLS) {
            error = ENOSPC;
        } else {
            error = BSERRMAP(ret);
        }
        goto fail_out;
    }

    /*
     *  For directories, write the BSR_BF_INHERIT_ATTR mcell record
     *  For files, write the BSR_ATTR mcell record
     */ 
    ret = bs_inherit(dir_bfap,        /* parent */
                     bfap,            /* child  */
                     create_directory ? BF_INHERIT_I_TO_I : BF_INHERIT_I_TO_A,
                     ftx_handle );
    if (ret != EOK) {
        if (ret == ENO_MORE_MCELLS) {
            error = ENOSPC;
        } else {
            error = BSERRMAP(ret);
        }
        goto fail_out;
    }

    /*
     * If we are creating a directory then initialize the
     * add a block of storage for it.
     */
    if ( create_directory ) {
        /* tell rbf_add_stg not to check the migtrunc lock: we could
        not take it in this case. Instead, migrate will wait on the
        bitfile state */
        ret = rbf_add_stg(bfap, 0, 1, ftx_handle, FALSE);
        if (ret != EOK) {
            if (ret == ENO_MORE_BLKS) {
                error = ENOSPC;
            } else {
                error = BSERRMAP(ret);
            }
            goto fail_out;
        }
        /* Directories start off with no index regardless of
         * domain versioning. Set a -1 in idx_params to indicate
         * that it is unnecessary to attempt to open the index
         */
        MS_SMP_ASSERT(bfap->idx_params == NULL);
        bfap->idx_params = (void *) -1;

    } /* end of if create_directory */

    /*
     * If we are creating a symbolic link then if not fast link
     * add a page of storage for the link
     */
    if (create_symlink) {
        register int len = strlen(vap->va_symlink);
        register int npages = len / ADVFS_PGSZ;

        if (len <= bmtr_max_rec_size()) {
            ret = bmtr_put_rec(
                               bfap,
                               BMTR_FS_DATA,
                               (void *)vap->va_symlink,
                               (ushort)len,
                               ftx_handle
                               );
            if (ret != EOK) {
                if (ret == ENO_MORE_MCELLS) {
                    error = ENOSPC;
                } else {
                    error = BSERRMAP(ret);
                }
                goto fail_out;
            }
            bfap->file_size = len;
        } else {
            /* create a 'slow' link */
            if (len % ADVFS_PGSZ != 0) {
                npages++;
            }
            /* tell rbf_add_stg not to check the migtrunc lock: we
               could not take it in this case. Instead, migrate will
               wait on the bitfile state */
            ret = rbf_add_stg(bfap, 0, npages, ftx_handle, FALSE);
            if (ret != EOK) {
                if (ret == ENO_MORE_BLKS) {
                    error = ENOSPC;
                } else {
                    error = BSERRMAP(ret);
                }
                goto fail_out;
            }

            bfap->file_size = len;
        }

    } /* end of if create_symlink */

    /*
     * Enter the new file in the directory.  The create_file
     * operation fails if we cannot create a directory entry
     * for the new file, which will only occur if the directory needed
     * to be extended a block, but there was no more space.
     */

    ret = insert_seq (
                      dvp,
                      new_file_tag,
                      nvp,
                      START_FTX,
                      ndp,
                      bfSetp,
                      mp,
                      ftx_handle
                      );
    if ( ret != EOK ) {
        /* this is not really an error - someone created the same file
           that we wanted to */
        if (ret == I_FILE_EXISTS) {
            error = EEXIST;
        } else {
            error = ret;
        }
        goto fail_out;
    }
    /*
     * if creating a directory, init the . and .. entries in the
     * first page (added earlier)
     */
    if (create_directory) {
        ret = rbf_pinpg(
                        &page_ref,
                        (void *)&block_buffer,
                        bfap,
                        0,
                        BS_OVERWRITE,
                        ftx_handle
                        );
        if (ret != EOK) {
            domain_panic(dmnP,
                         "rbf_pinpg error in fs_create_file with status %d",
                         ret);
            error = ret;
            goto fail_out;
        }
        /*
         * pin_rec the whole page, it needs to be bzeroed
         */
        rbf_pin_record(
                       page_ref,
                       block_buffer,
                       ADVFS_PGSZ
                       );
        bzero(block_buffer, ADVFS_PGSZ);

        dir_bs_tag = dir_cp->bf_tag;
        fs_init_directory (
                           block_buffer,
                           new_file_tag,
                           dir_bs_tag,
                           ADVFS_PGSZ_IN_BLKS
                           );
    }

    /*
     * If we are creating a symbolic link then if longer than a BMT
     * record, write it.
     */
    if (create_symlink) {
        register int len = strlen(vap->va_symlink);
        register int npages = len / ADVFS_PGSZ;
        register int i, this_page;

        if (len > bmtr_max_rec_size()) {
            /*
             * useless loop that will always get executed only once
             * (how many symlinks are > 8192 bytes long?)
             */
            if (len % ADVFS_PGSZ != 0) {
                npages++;
            }

            for (i = 0; i < npages; i++) {

                this_page = MIN(len, ADVFS_PGSZ);
                ret = rbf_pinpg(
                                &page_ref,
                                (void *)&block_buffer,
                                bfap,
                                i,
                                BS_NIL,
                                ftx_handle
                                );
                if (ret != EOK) {
                    domain_panic(dmnP,
                              "rbf_pinpg error in fs_create_file. Status is %d",
                              ret);
                    error = ret;
                    goto fail_out;
                }
                bzero(block_buffer, ADVFS_PGSZ);
                rbf_pin_record(
                               page_ref,
                               block_buffer,
                               this_page
                               );
                bcopy(vap->va_symlink, block_buffer, this_page);
                len -= this_page;
            }
        }
    } /* end of if create_symlink */

    /*
     * fill in the time fields and the access handle for the new
     * files's directory. Init its lock.
     * Save the context pointer in the fd table. Note that this must be
     * done after the directory entry is created (bacause the new entry is
     * stored in fsContext but before the directory is unlocked ( so no
     * one can try to open the file before we are ready...)
     */
    /*
     * get file creation date/time
     */
    new_file_cp->fs_flag |= (MOD_ATIME | MOD_MTIME | MOD_CTIME);
    new_file_cp->dir_stats.st_ino = new_file_tag;
    bfap->fragState = FS_FRAG_NONE;
    bfap->fragId = bsNilFragId;
    new_file_cp->dirty_stats = TRUE;
    new_file_cp->dirstamp = 0;

#ifdef ADVFS_DEBUG
    /* copy in the file name. the +1 is to copy the \0 at the string end */
    if (ndp->ni_dent.d_namlen < 29) {
        (void)bcopy(
                       ndp->ni_dent.d_name,
                       new_file_cp->file_name,
                       ndp->ni_dent.d_namlen + 1
                       );
    } else {
        (void)bcopy(ndp->ni_dent.d_name, new_file_cp->file_name, 29);
        new_file_cp->file_name[29] = '\0';
    }
#endif

    if (create_directory) {
        bfap->file_size = ADVFS_PGSZ;
        new_file_cp->dir_stats.st_nlink = 2;
        ++dir_cp->dir_stats.st_nlink;
        if (mode != 0) {
            new_file_cp->dir_stats.st_mode = mode | S_IFDIR;
        } else {
            new_file_cp->dir_stats.st_mode = S_IFDIR | S_IRWXU;
        }
    } else {
        new_file_cp->dir_stats.st_nlink = 1;
        if (!create_symlink) {
            bfap->file_size = 0;
        }
        new_file_cp->dir_stats.st_mode = mode;
    }
    /*
     * update the st_mtime field in the inserted-in directory's stat
     * structure. Also set the stat dirty flag so that the stats get
     * saved on closing the directory, and incr the dirstamp field to
     * indicate that the dir has changed
     */

    mutex_lock( &dir_cp->fsContext_mutex );
    dir_cp->fs_flag |= (MOD_MTIME | MOD_CTIME);
    dir_cp->dirty_stats = TRUE;
    dir_cp->dirstamp++;
    mutex_unlock( &dir_cp->fsContext_mutex );

    ndp->ni_vp = nvp;

    /*
     * set up the back_link stuff in the stats for the new file
     */
    new_file_cp->dir_stats.dir_tag = dir_cp->bf_tag;
     /*
     * put out the stats for the just-created-file.
     * this can't fail.
     */
    ret = fs_update_stats(nvp, bfap, ftx_handle, 0);
    if (ret != EOK) {
        domain_panic(dmnP,
           "error from fs_update_stats of new file in fs_create with status %d",
           ret);
        error = ret;
        goto fail_out;
    }

    /*
     * Note that the file context area is initialized
     */

    new_file_cp->initialized = 1;

    /*
     * update the directory's stats in it's directory entry.
     * This one should never fail.
     */

    ret = fs_update_stats(dvp, dir_bfap, ftx_handle, 0);

    if (ret != EOK) {
        domain_panic(dmnP,
                     "Error fs_updating dirs stats in fs_create with status %d",
                     ret);
        error = ret;
        goto fail_out;
    }

    /*
     * finish the transaction.
     */

    /*
     * Let ftx_done_fs() know whether the fileset is mounted
     * server_only (CFS).
     */

    ftx_done_fs(ftx_handle, FTA_FS_CREATE_1, 
                        (nvp->v_mount->m_flag & M_FSPART));

    MIGTRUNC_UNLOCK( &(dir_bfap->xtnts.migTruncLk) );
    MIGTRUNC_UNLOCK( &(bfSetp->dirBfAp->xtnts.migTruncLk) );
    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }
    /* Unlock the quota files */
    for (qtype = 0; qtype < MAXQUOTAS; qtype++) {
        if (quota_mig_trunc_lock[qtype]) {
            MIGTRUNC_UNLOCK( &(fsNp->qi[qtype].qiAccessp->xtnts.migTruncLk) );
            quota_mig_trunc_lock[qtype] = FALSE;
        }
    }

    /*
     * Inherit the correct security attributes.
     * symlinks shouldn't do ACL inheritance.
     */
    if (type != VLNK) {
        udac_t udac;

        udac.cuid = udac.uid = vap->va_uid;
        udac.cgid = udac.gid = vap->va_gid;
        if (vap->va_rawmode == (u_short)VNOVAL) {
            udac.mode = vap->va_mode & 0777;
        } else {
            udac.mode = vap->va_rawmode & 0777;
        }
        SP_OBJECT_CREATE(ndp->ni_cred, nvp, dvp, SEC_OBJECT, &udac);
    }
     /* finish any directory truncation that might have been started 
     * in insert_seq for the just inserted-into  directory.
     */
    dir_trunc_finish(dvp);

    /*
     * Put an entry for the new file into the namei cache.
     */
    if (ndp->ni_makeentry) {
        ndp->ni_ptr = ndp->ni_dent.d_name;
        cache_enter(ndp);
    }

    /*
     * unlock the directory just inserted-into.
     */
    FS_FILE_UNLOCK( dir_cp);

    /*
     * release the directory
     */
    vrele(dvp);

    /*
     * if symlink release it
     */
    if (create_symlink) {
        vrele(nvp);
    }

    ms_free( new_bs_paramsp );

    if ( !dmnP->dmn_panic ) {
        return (0);
    }

    if ( !create_symlink ) {
        vrele(nvp);
    }
    return EIO;

fail_out:
    ftx_fail(ftx_handle);   /* will delete new bitfile */

    /*
     * Must vrele() after failing the transaction because in
     * a cluster, vrele() of this new bitfile could trigger
     * the start of a new transaction.
     */
fail_out_no_ftx_fail_needed:
    vrele(nvp);     /* will also close new bitfile */
    nvp = NULL;

    MIGTRUNC_UNLOCK( &(dir_bfap->xtnts.migTruncLk) );
    MIGTRUNC_UNLOCK( &(bfSetp->dirBfAp->xtnts.migTruncLk) );
    if (idx_bfap != NULL) {
        MIGTRUNC_UNLOCK( &(idx_bfap->xtnts.migTruncLk) );
    }
    for (qtype = 0; qtype < MAXQUOTAS; qtype++) {
        if (quota_mig_trunc_lock[qtype]) {
            MIGTRUNC_UNLOCK( &(fsNp->qi[qtype].qiAccessp->xtnts.migTruncLk) );
            quota_mig_trunc_lock[qtype] = FALSE;
        }
    }

    FS_FILE_UNLOCK( dir_cp );

fail_out1:
    vrele(dvp);             /* release parent dir */
    ms_free( new_bs_paramsp );
    return BSERRMAP(error);

} /* fs_create_file */


/*
 * fs_update_stats
 *
 * update the on-disk stats for a bitfile in it's BMT recd.
 * Read the system time, if necessary, to update any time fields.
 */
statusT
fs_update_stats(
                struct vnode *vp,             /* in - vnode of bitfile */
                struct bfAccess *bfap,        /* in - access structure */
                ftxHT ftx_handle,             /* in - ftx handle */
                uint32T flags                 /* in - flags */
                )
{
    struct fsContext *context_ptr;
    statusT ret = EOK;
    struct timeval new_time;
    struct mount *mp;
    struct fileSetNode *fsNp;


    context_ptr = VTOC( vp );

    /*
     * When there is no context pointer, there are no stats to update.
     */

    if (!context_ptr) goto EXIT_UPDATE;

    /* TODO this field is not protected with a mutex as fields below are. 
       This routine needs to be fixed in conjunction with  msfsspec_close
       since they would use the same fsContext_mutex to protect the 
       fscontex structure. Any other routine that calls this would have
       to coordinate in the same way if it uses the same mutex in the
       same way.
    */
    context_ptr->dirty_stats = FALSE;

    /*
     * if this was a tag-opened file, just return from here
     */
    if (context_ptr->fs_flag & META_OPEN) {
        goto EXIT_UPDATE;
    }
   
    /*
     * Note that META_OPEN files and files opened internally via
     * bs_access() may be on DEADMOUNT which doesn't have a 
     * fileSetNode pointer in m_data.  Since these accesses are
     * not external filesystem operations do not change the stats.
     */

    mp = VTOMOUNT (vp);
    fsNp = GETFILESETNODE(mp);


    /*
     * if read-only file set or no fileSetNode just return 
     */
    if ( fsNp == NULL || fsNp->fsFlags & FS_CLONEFSET ) {
        goto EXIT_UPDATE;
    }
    if (mp->m_flag & M_RDONLY) {
        goto EXIT_UPDATE;
    }

    /*
     * if any time fields need to be updated, do it now...
     * store the utime field in microsecs instead of the
     * usual nanoseconds for future use.
     */
    mutex_lock( &context_ptr->fsContext_mutex );
    if (context_ptr->fs_flag & (MOD_MTIME | MOD_ATIME | MOD_CTIME)) {
        TIME_READ(new_time);
        if (context_ptr->fs_flag & MOD_MTIME) {
            context_ptr->dir_stats.st_mtime = new_time.tv_sec;
            context_ptr->dir_stats.st_umtime = (new_time.tv_usec * 1000);
        }
        if (context_ptr->fs_flag & MOD_ATIME) {
            context_ptr->dir_stats.st_atime = new_time.tv_sec;
            context_ptr->dir_stats.st_uatime = (new_time.tv_usec * 1000);
        }
        if (context_ptr->fs_flag & MOD_CTIME) {
            context_ptr->dir_stats.st_ctime = new_time.tv_sec;
            context_ptr->dir_stats.st_uctime = (new_time.tv_usec * 1000);
        }
        context_ptr->fs_flag &= ~(MOD_ATIME | MOD_CTIME | MOD_MTIME);
    }
    context_ptr->dir_stats.st_size = bfap->file_size;
    context_ptr->dir_stats.fragId = bfap->fragId;
    context_ptr->dir_stats.fragPageOffset = bfap->fragPageOffset;
    mutex_unlock( &context_ptr->fsContext_mutex );
    ret = bmtr_update_rec(
                          bfap,
                          BMTR_FS_STAT,
                          (void *)&context_ptr->dir_stats,
                          sizeof(statT),
                          ftx_handle,
                          flags
                          );
EXIT_UPDATE:
    return (ret);
}


/*
 * fs_flush_saved_stats
 *
 * Update the on-disk stats for a bitfile in it's BMT recd.
 * This is a special case of fs_update_stats() when there is no longer
 * a vnode because it has already been reclaimed.  This is called only
 * from cleanup_closed_list().  Must be called with no transactions in
 * progress and the bfap->bfaLock held!  The bfap->bfaLock will
 * be dropped and reseized in this routine, so beware!
 */
statusT
fs_flush_saved_stats(
                bfAccessT *bfap,      /* in - access struct */
                uint32T    flags,     /* in - flags */
                ftxHT      ftxH       /* in - transaction handle */
                )
{
    saved_statsT *ssp = bfap->saved_stats;
    statusT ret = EOK;
    struct timeval new_time;

    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfaLock.mutex));

    /*
     * if any time fields need to be updated, do it now...
     * store the utime field in microsecs instead of the
     * usual nanoseconds for future use.
     */
    if (ssp->fs_flag & (MOD_MTIME | MOD_ATIME | MOD_CTIME)) {
        TIME_READ(new_time);
        if (ssp->fs_flag & MOD_MTIME) {
            ssp->dir_stats.st_mtime = new_time.tv_sec;
            ssp->dir_stats.st_umtime = (new_time.tv_usec * 1000);
        }
        if (ssp->fs_flag & MOD_ATIME) {
            ssp->dir_stats.st_atime = new_time.tv_sec;
            ssp->dir_stats.st_uatime = (new_time.tv_usec * 1000);
        }
        if (ssp->fs_flag & MOD_CTIME) {
            ssp->dir_stats.st_ctime = new_time.tv_sec;
            ssp->dir_stats.st_uctime = (new_time.tv_usec * 1000);
        }
        ssp->fs_flag &= ~(MOD_ATIME | MOD_CTIME | MOD_MTIME);
    }

    mutex_unlock(&bfap->bfaLock);
    ret = bmtr_update_rec(bfap,
                          BMTR_FS_STAT,
                          (void *)&ssp->dir_stats,
                          sizeof(statT),
                          ftxH,
                          flags
                          );
    mutex_lock(&bfap->bfaLock);

    /* We don't deallocate the saved_stats structure here in case 
     * bmtr_update_rec() returns a 'cant start a transaction' error.
     * In this case, the whole operation will be retried again at a
     * later time.
     */
    return (ret);
}


/*
 * fs_trunc_test
 *
 * This function determines if the file could be truncated.
 *
 * Pages may be deallocated only at the end of the bitfile.  Allocated pages
 * within a hole of a sparse bitfile are not affected.
 *
 * Quota allocations is adjusted within the transaction tree in which the 
 * storage is being relased.  Thus the BS layer must directly call
 * chk_blk_quota.  This means that the bitfile is in the ACC_INIT_TRANS
 * state when chk_blk_quota is called.  
 */

int
fs_trunc_test(
              struct vnode* vp  /* in */
              )
{
    long nextPageToAlloc;
    long lastPage;
    bfAccessT *bfap;
    bfSetT *bfSetp;
    struct fsContext* fileContext = VTOC( vp );


    if ( fileContext == NULL || fileContext->fs_flag & META_OPEN ) {
        /*
         * This file was opened thru the tag interface.  Its stats
         * area is not initialized so ignore it.
         */
        return 0;
    }

    bfap = VTOA(vp);
    bfSetp = bfap->bfSetp;

    if ( (bfSetp->cloneId != BS_BFSET_ORIG) ||
         (bfap->fragState != FS_FRAG_NONE) ) {
        return 0;
    }

    nextPageToAlloc = bfap->nextPage;

    lastPage = (bfap->file_size + ADVFS_PGSZ - 1L) / ADVFS_PGSZ;

    return ( bfap->trunc = (nextPageToAlloc > lastPage ));
}  /* end fs_truncation_possible */
