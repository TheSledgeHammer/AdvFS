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
 *   This module contains routines to create, open, truncate and close
 *   a file, and update a file's stats.
 */

#include <limits.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <sys/kernel.h>
#include <sys/vnode.h>
#include <sys/dnlc.h>
#include <fs/vfs_ifaces.h>            /* struct nameidata */
#include <sys/cred.h>
#include <sys/mount.h>
#include <sys/user.h>
#include <sys/spinlock.h>             /* for lock_t */

#include <fs_dir_routines.h>
#include <ms_public.h>
#include <fs_dir.h>
#include <fs_quota.h>
#include <ms_privates.h>
#include <bs_access.h>
#include <fs_file_sets.h>
#include <ms_osf.h>
#include <bs_params.h>
#include <ms_assert.h>
#include <bs_index.h>
#include <bs_extents.h>
#include <bs_snapshot.h>
#include <advfs_acl.h>
#include <aclv.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif


#define ADVFS_MODULE FS_CREATE

static statT NilStats = { 0 };



/*
 *   fs_create_file
 *
 *   Create a new file.
 *   Assumes the caller has already determined that the
 *   file does not already exist.
 *
 *   ENTRY CONDITIONS: dvp has an elevated v_count and it is not to be
 *   modified.
 *
 *   EXIT CONDITIONS: On success, ndp->ni_vp has a vnode that represents the
 *   created file and the v_count is elevated.
 *
 */

statusT
fs_create_file(struct vattr     *vap,       /* in - vnode attributes pointer */
               struct nameidata *ndp,       /* in - nameidata pointer */
               struct vnode     *dvp,       /* in - directory vnode pointer */
                      char      *name,      /* in - create this name */
                      char      *targetname /* in - target name for symlink */
              )
{              
    char        *block_buffer;
    statusT      ret, bsRet;
    bfTagT       new_file_tag;
    bfTagFlagsT  tagFlags;
    rbfPgRefHT   page_ref;
    bfAccessT   *bfap;
    bfParamsT   *new_bs_paramsp = NULL;
    ftxHT        ftx_handle;
    int          create_symlink;
    int          error = 0;
    int          err;
    int          i;
    int64_t      blks;
    int          mode;
    domainT     *dmnP;
    bfSetT      *bfSetp;
    bfSetIdT     the_bfset_id;
    int          create_directory;
    bfTagT       dir_bs_tag;
    bfAccessT   *dir_bfap;
    bfAccessT   *idx_bfap = NULL;
    size_t       pgsz_in_fobs;          /* Page size in bytes */
    size_t       pgsz_in_bytes;         
    uint16_t     effective_LINK_MAX;
    off_t        quota_rec_offset[MAXQUOTAS];
    int32_t      quota_mig_stg_lock[MAXQUOTAS];
    int32_t      mig_stg_locks_held = FALSE;
    int          qtype;
    int		 num_tuples;
    
    enum vtype   type;

    struct fileSetNode *fsnp;
    struct fsContext   *new_file_cp, *dir_cp;
    struct vnode       *nvp = NULL;
    struct bfNode      *bnp;
    struct acl	       *acl=NULL;
    
    int locked_vnode = FALSE;      /* for use with VNODE_LOCKX & UNLOCKX */

    lock_t	*sv_lock;
    /*-----------------------------------------------------------------------*/


    new_bs_paramsp = (bfParamsT *) ms_malloc( sizeof( bfParamsT ));
    
    /*
     * get the domain pointer for the to-be-inserted-in directory
     */
    fsnp = VTOFSN(dvp);
    dmnP = fsnp->dmnP;

    /* Clear any stale pointer in the namei structure */
    ndp->ni_vp = NULL;
    /*
     * screen domain panic
     */
    if (dmnP->dmn_panic) {
        ms_free( new_bs_paramsp );
        return EIO;
    }
      
    /*
     * get the access pointer of the directory
     */
    bfSetp = fsnp->bfSetp;
    the_bfset_id = fsnp->bfSetId;
    dir_bfap = VTOA(dvp);

    /*
     * Make sure we can write to the directory
     */
    if ((error = advfs_access(dvp, S_IWRITE, TNC_CRED())) != 0) {
        ms_free( new_bs_paramsp );
        return (error);
    }

    type = vap->va_type;
    mode = MAKEMODE(vap->va_type, vap->va_mode);
    create_directory = FALSE;
    if ((mode & S_IFMT) == S_IFDIR) {
        create_directory = TRUE;
    }

    dir_cp = VTOC( dvp );
    create_symlink = FALSE;
    if ((mode & S_IFMT) == S_IFLNK) {
        create_symlink = TRUE;
    }
    
    /*
     * See the comments explaining these asserts in the advfs_link() routine
     */
    /*
     * Determine the maximum number of links supported.  If we start to
     * support different on-disk versions that have different maximum nlink
     * values, then make this a conditional test based on the domain
     * version.  That is what we did in Tru64 UNIX.
     */
    effective_LINK_MAX = MAXLINK;       /* Max links supported by HFS */

    MS_SMP_ASSERT(MAXLINK == 32767);
    MS_SMP_ASSERT(sizeof(((struct vattr *)0)->va_nlink)==sizeof(short));
    MS_SMP_ASSERT(sizeof(((struct stat *)0)->st_nlink)==sizeof(short));
    MS_SMP_ASSERT(sizeof(nlink_t) == sizeof(short));

    if (create_directory && 
        ( dir_cp->dir_stats.st_nlink >= effective_LINK_MAX))
    {
        ms_free( new_bs_paramsp );
        return EMLINK;
    }

    ADVRWL_FILECONTEXT_WRITE_LOCK (dir_cp);




    /* Need to take the directory lock because of the call chain
     * insert_seq --> rbf_add_stg
     */
    ADVRWL_MIGSTG_READ( dir_bfap );

    /* Need to take the fileset tag directory lock because of
     * the call chain
     * rbf_create --> rbf_int_create --> tagdir_alloc_tag -->
     * init_next_tag_page --> rbf_add_stg
     */
    ADVRWL_MIGSTG_READ( bfSetp->dirBfAp );

    /* Need to lock the index file because of the call chain
     * insert_seq --> idx_directory_get_space-->idx_directory_insert_space -->
     * idx_directory_insert_space_int --> idx_index_get_free_pgs_int -->
     * rbf_add_stg
     */
    IDX_GET_BFAP(dir_bfap, idx_bfap); 
    if (idx_bfap != NULL) { 
        ADVRWL_MIGSTG_READ( idx_bfap );
    }

    /* May need to take the quota locks because of the call chain
     * chk_{blk, bf}_quota --> dqsync --> rbf_add_stg
     */
    /* the group id will most likely be that of the parent directory
     * (which is mapped in the quota file already), except in the
     * case of svid III compatibility, with the set-gid bit on the parent
     * not set.
     */

    /* 
     * The casting here is done because we don't want to sign extend
     * the offset.  Otherwise, negative values become huge offsets
     * which can't be handled by advfs.
     */
    quota_rec_offset[USRQUOTA] = (off_t) 
        ((uint64_t) (uint32_t)TNC_CRED()->cr_uid) * 
                              sizeof(struct dqblk64);
    quota_rec_offset[GRPQUOTA] = (off_t)
        ((uint64_t) (uint32_t)TNC_CRED()->cr_gid) * 
                              sizeof(struct dqblk64);
    for (qtype = 0; qtype < MAXQUOTAS; qtype++) {
        quota_mig_stg_lock[qtype] = FALSE;
        /* if the type is user and the page is not mapped, take the lock.
         * if the type is group and svid3 compatibility mode is on
         * and the setgid bit on the parent is not set AND the page is
         * not mapped, take the lock.
         * see svid III compatibility below.
         */
        if (((qtype == USRQUOTA) ||
             (qtype == GRPQUOTA && !BSD_MODE(dvp) && 
             !(dir_cp->dir_stats.st_mode & S_ISGID))) &&
            !advfs_bs_is_offset_mapped(fsnp->qi[qtype].qiAccessp, 
                                       quota_rec_offset[qtype])) {
            ADVRWL_MIGSTG_READ( fsnp->qi[qtype].qiAccessp );
            quota_mig_stg_lock[qtype] = TRUE;
        }
    }


    mig_stg_locks_held = TRUE;

    /*
     * start the transaction.  Note: if not running under CFS,
     * ndp->ni_xid must be zero to guarantee uniqueness of
     * ftxId in the transaction log.
     */

    ret = FTX_START_XID( FTA_FS_CREATE_1, &ftx_handle, FtxNilFtxH,
                         dmnP, ndp->ni_xid );
    if (ret != EOK) {
        domain_panic(dmnP,"Error in ftx_start in fs_create_file. Status is %d",
                     ret);
        error = ret;
        bsRet = ret;
        goto fail_out_no_ftx_fail_needed;
    }


    /*
     * Create a new bitfile and access it. If it is a directory create or it
     * is a symlink create, set the bfpPageSize and the dataSafety fields
     * for metadata type files.  We are treating symlinks as metadata files
     * due to a change in the AdvFS HPUX ufc integration design that does not
     * allow pinning user file pages under transaction control.  That can
     * only be done to metadata files.  If its not a directory  or symlink
     * create, save the bitfile access handle in the user's fd table.
     */

    if (create_directory || create_symlink) {
        new_bs_paramsp->bfpPageSize = dmnP->metaAllocSz;
        MS_SMP_ASSERT(new_bs_paramsp->bfpPageSize == ADVFS_METADATA_PGSZ_IN_FOBS);
        tagFlags = BFD_METADATA;
    } else {
        new_bs_paramsp->bfpPageSize = dmnP->userAllocSz;
        tagFlags = BFD_USERDATA;
    }

    ret = rbf_create(&new_file_tag,
                     bfSetp,
                     new_bs_paramsp,
                     ftx_handle,
                     CRT_CHK_MIGSTG_LOCK,
                     tagFlags);
    if (ret != EOK) {
        bsRet = ret;
        if (ret == E_READ_ONLY) {
            error = EROFS ;
        } else {
            error = BSERRMAP( ret );
        }
        goto fail_out;
    }

    bsRet = bs_access(
                    &bfap,
                    new_file_tag,
                    bfSetp,
                    ftx_handle,
                    BF_OP_NO_FLAGS,
                    &nvp
                    );

    if (bsRet != EOK) {
        error = BSERRMAP( bsRet );
        goto fail_out;
    }

    ADVFS_ACCESS_LOGIT(bfap, "fs_create_file: first access after create");
    /*
     * We should have a reference on the node at this point (from success in
     * bs_access).  It might be greater than 1 if we are racing someone
     * doing an open of the file we are creating.
     */
    MS_SMP_ASSERT( bfap->bfVnode.v_count > 0);

    ndp->ni_vp = nvp;
    
    bnp = VTOBNP(nvp);

    bnp->tag = new_file_tag;
    bnp->bfSetId = the_bfset_id;

    new_file_cp = &bfap->bfFsContext;
    new_file_cp->dir_stats = NilStats;

    /*
     * Fill in the uid and gid early for the benefit of the quota system.
     */
    new_file_cp->dir_stats.st_uid = TNC_CRED()->cr_uid;
    new_file_cp->fs_flag = 0;

    if (BSD_MODE(dvp)) {
        new_file_cp->dir_stats.st_gid = dir_cp->dir_stats.st_gid;
        if((new_file_cp->dir_stats.st_mode & S_ISGID) &&
            !groupmember( new_file_cp->dir_stats.st_gid ) &&
            suser()) {
            new_file_cp->dir_stats.st_mode &= ~S_ISGID;
            mode &= ~S_ISGID;
        }
    } else {
        /* svid III compatibility. The following code is based on the logic
         * of HP-UX ufs' dirmakeinode() 
         */
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
            } else if ((mode & S_ISGID) && 
                    !groupmember(new_file_cp->dir_stats.st_gid)) {
                mode &= ~S_ISGID;
            }
        } else {
            /*
             * Parent directory does NOT have the ISGID bit set.
             * Therefore new nodes' group id becomes the group id (egid)
             * of the calling process.
             */
            new_file_cp->dir_stats.st_gid = TNC_CRED()->cr_gid; 
            if ((mode & S_ISGID) && 
                    !groupmember(new_file_cp->dir_stats.st_gid)) {
                mode &= ~S_ISGID;
            }
        }
    }

    pgsz_in_fobs = bfap->bfPageSz;
    pgsz_in_bytes = pgsz_in_fobs * ADVFS_FOB_SZ;

    /*
     * Set vnode ops for special files.
     */
    switch (type) {
        case VREG:
            break;
        case VFIFO:
            /*
             * Type mismatch between v_rdev and tag_num is ok here.
             * VFIFO rdev is only used for hashtable lookups and we
             * are only striving for a good hashable value.
             */
            new_file_cp->dir_stats.st_rdev = (dev_t)new_file_tag.tag_num;

            VNODE_LOCKX(nvp, locked_vnode);
            nvp->v_rdev = (dev_t)new_file_tag.tag_num;
            VNODE_UNLOCKX(nvp, locked_vnode);
            break;

        case VBLK:
        case VCHR:
            new_file_cp->dir_stats.st_rdev = vap->va_rdev;

            VNODE_LOCKX(nvp, locked_vnode);
            nvp->v_rdev = vap->va_rdev;
            VNODE_UNLOCKX(nvp, locked_vnode);

            break;
    }
    nvp->v_type = type;

    /*
     * Check that the the total number of bitfiles that the user may
     * own is not exceeded.
     */
    new_file_cp->fsc_fsnp = fsnp;
    for (i = 0; i < MAXQUOTAS; i++) {
        new_file_cp->diskQuot[i] = NULLDQUOT;
    }
    new_file_cp->quotaInitialized = TRUE;

    if ((error = advfs_fs_attach_quota(new_file_cp, ftx_handle)) ||
        (error = chk_bf_quota(nvp, 1, TNC_CRED(), 0, ftx_handle))) {
        goto fail_out;
    }

    /*
     * Pages are allocated to the new file if it is either a directory
     * or a long symbolic link.  Check that the user's block usage
     * is not exceeded.
     */

    blks = 0L;

    if (create_directory) {
        blks = QUOTABLKS(pgsz_in_bytes); /* convert bytes to quota blocks */
    }

    if (create_symlink) {
        MS_SMP_ASSERT(targetname != NULL);
        int len = strlen(targetname);
        if (len > bmtr_max_rec_size()) {
            bf_fob_t nfobs = howmany(len, pgsz_in_bytes) * bfap->bfPageSz;
            blks = QUOTABLKS(nfobs * ADVFS_FOB_SZ); /* bytes to quota blocks */
        }
    }
    
    if (blks && (error = chk_blk_quota( nvp,
                                       blks,
                                       TNC_CRED(),
                                       0,
                                       ftx_handle))) {
        goto fail_out;
    }

    /*
     * Write the BMTR_FS_STAT mcell record.
     * In del_clean_mcell_list(), we assume that the BMTR_FS_STAT record
     * always resides in the primary mcell. If this is to be changed, change
     * del_clean_mcell_list() accordingly
     */

    /*
     *  For directories, write the BSR_BF_INHERIT_ATTR mcell record
     *  For files, write the BSR_ATTR mcell record
     */ 
    ret = bs_stat_n_inherit(
                      dir_bfap,        /* parent */
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
        /* tell rbf_add_stg not to check the migStgLk: we could
        not take it in this case. Instead, migrate will wait on the
        bitfile state */
        ret = rbf_add_stg(bfap, 0, bfap->bfPageSz, ftx_handle, STG_NO_FLAGS);
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
        int len = strlen(targetname);
        bf_fob_t nfobs = howmany(len, pgsz_in_bytes) * bfap->bfPageSz;

        if (len <= bmtr_max_rec_size()) {
            ret = bmtr_put_rec(
                               bfap,
                               BMTR_FS_DATA,
                               (void *)targetname,
                               (uint16_t)len,
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
            /* tell rbf_add_stg not to check the migStgLk: we
               could not take it in this case. Instead, migrate will
               wait on the bitfile state */
            ret = rbf_add_stg(bfap, 0, nfobs, ftx_handle, STG_NO_FLAGS);
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

        ADVFS_ACCESS_LOGIT(bfap, "fs_create_file: create_symlink 1");
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
                      name,
                      START_FTX,
                      ndp,
                      bfSetp,
                      fsnp,
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
     * Optimize performance by telling rbf_pinpg to skip IO to prime
     * new cache pages from disk since this routine is initializing the
     * whole metadata page.
     */
    if (create_directory) {
        ret = rbf_pinpg(
                        &page_ref,
                        (void *)&block_buffer,
                        bfap,
                        0,
                        BS_NIL,
                        ftx_handle,
                        MF_OVERWRITE | MF_NO_VERIFY
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
                       pgsz_in_bytes);


        bzero(block_buffer, pgsz_in_bytes);
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_356);

        dir_bs_tag = dir_cp->bf_tag;
        fs_init_directory (
                           block_buffer,
                           new_file_tag,
                           dir_bs_tag,
                           pgsz_in_fobs
                           );
    }

    /*
     * If we are creating a symbolic link then if longer than a BMT
     * record, write it.
     */
    if (create_symlink) {
        int len = strlen(targetname);

        bs_meta_page_t npages = howmany(len,pgsz_in_bytes);
        int i, this_page;
        char *tnp = targetname;
        
        if (len > bmtr_max_rec_size()) {

            /*
             * useless loop that will always get executed only once
             * (how many symlinks are > 8192 bytes long?)
             */
            for (i = 0; i < npages; i++)  {

                this_page = MIN(len, pgsz_in_bytes);
                ret = rbf_pinpg(
                                &page_ref,
                                (void *)&block_buffer,
                                bfap,
                                i,
                                BS_NIL,
                                ftx_handle,
                                MF_NO_VERIFY
                                );
                if (ret != EOK) {
                    domain_panic(dmnP,
                              "rbf_pinpg error in fs_create_file. Status is %d",
                              ret);
                    error = ret;
                    goto fail_out;
                }
                rbf_pin_record(
                               page_ref,
                               block_buffer,
                               pgsz_in_bytes
                               );
                bzero(block_buffer, pgsz_in_bytes);
                bcopy( tnp, block_buffer, this_page );
                len -= this_page;
                tnp += this_page;
                ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_357);
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
    new_file_cp->dirty_stats = TRUE;
    new_file_cp->dirstamp = 0;

    if (create_directory) {
        bfap->file_size = pgsz_in_bytes;
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

    ADVSMP_FILESTATS_LOCK( &dir_cp->fsContext_lock );
    dir_cp->fs_flag |= (MOD_MTIME | MOD_CTIME);
    dir_cp->dirty_stats = TRUE;
    dir_cp->dirstamp++;
    ADVSMP_FILESTATS_UNLOCK( &dir_cp->fsContext_lock );

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
     * Set the bfaFlags to indicate there is no ACL on disk
     * If there are default ACL entries on the parent directory, then the
     * flag will be updated during advfs_setacl_int
     */
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    bfap->bfaFlags |= BFA_ACL_NOT_ONDISK;
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

    /* Check for Default ACL entries */
    ret = advfs_check_default_acls( dvp,
				    nvp,
				    &acl,
				    &num_tuples,
				    create_directory );
    if( ret != EOK ) {
	error = ret;
	goto fail_out;
    }

    /* Default ACL entries present on parent, set the ACL on the new file */
    if( acl != NULL ) {
	err = advfs_setacl_int( ftx_handle,
				nvp,
				num_tuples,
				acl,
				ADVFS_ACL_TYPE );
	if( err != EOK ) {
	    error = err;
	    goto fail_out;
	}
    }
    ms_free( acl );


    /*
     * finish the transaction.
     */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_1);

    ftx_done_fs(ftx_handle, FTA_FS_CREATE_1);

    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_2);

    ADVRWL_MIGSTG_UNLOCK( dir_bfap );
    ADVRWL_MIGSTG_UNLOCK( bfSetp->dirBfAp );
    if (idx_bfap != NULL) {
        ADVRWL_MIGSTG_UNLOCK( idx_bfap );
    }
    /* Unlock the quota files */
    for (qtype = 0; qtype < MAXQUOTAS; qtype++) {
        if (quota_mig_stg_lock[qtype]) {
            ADVRWL_MIGSTG_UNLOCK( fsnp->qi[qtype].qiAccessp );
            quota_mig_stg_lock[qtype] = FALSE;
        }
    }

     /* finish any directory truncation that might have been started 
     * in insert_seq for the just inserted-into  directory.
     */
    dir_trunc_finish(dvp);

    dnlc_enter(dvp, name, nvp, NOCRED);

    /*
     * unlock the directory just inserted-into.
     */
    ADVRWL_FILECONTEXT_UNLOCK( dir_cp );

    /*
     * We want to return with the vnode referenced.  We haven't
     * touched the v_count of dvp, and nvp is now in the name cache, and we
     * want to keep a v_count on it in a normal create call, vno_close will
     * do the VN_RELE on the vnode we return.  The dnlc_enter may have put
     * dvp into the dnlc cache too.  This means dvp may have a v_scount of 1
     * greater than it was when it entered.
     * */

    ms_free( new_bs_paramsp );

    if ( !dmnP->dmn_panic ) {
        return (0);
    }

    return EIO;

fail_out:
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_358);
    ftx_fail(ftx_handle);   /* will delete new bitfile */
    ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_359);

    /*
     * Must VN_RELE() after failing the transaction because in
     * a cluster, VN_RELE() of this new bitfile could trigger
     * the start of a new transaction.
     */
fail_out_no_ftx_fail_needed:
    /*
     * This doesn't seem right.  If bs_access failed and we jumped down to
     * close the transaction, we might not have ever put the v_count on the
     * vnode.  This should be conditional on the error of bs_access.
     */
    MS_SMP_ASSERT( ( (bsRet==EOK) && nvp ) || (bsRet!=EOK) );
    if (bsRet == EOK) {
        /*
         * An assert in advfs_inactive assumes prev lock state is
         * not ACC_CREATING.
         */
        ADVSMP_BFAP_LOCK   (&bfap->bfaLock);
        lk_set_state( &bfap->stateLk, ACC_INVALID );
        ADVSMP_BFAP_UNLOCK (&bfap->bfaLock);
        VN_RELE(nvp);     /* will also close new bitfile */
    }
    ndp->ni_vp = NULL;

    if (mig_stg_locks_held) {
        ADVRWL_MIGSTG_UNLOCK( dir_bfap );
        ADVRWL_MIGSTG_UNLOCK( bfSetp->dirBfAp );
        if (idx_bfap != NULL) {
            ADVRWL_MIGSTG_UNLOCK( idx_bfap );
        }
        for (qtype = 0; qtype < MAXQUOTAS; qtype++) {
            if (quota_mig_stg_lock[qtype]) {
                ADVRWL_MIGSTG_UNLOCK( fsnp->qi[qtype].qiAccessp );
                quota_mig_stg_lock[qtype] = FALSE;
            }
        }
    }

    ADVRWL_FILECONTEXT_UNLOCK( dir_cp );

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
                uint32_t flags                /* in - flags */
		)
{
    struct fsContext *context_ptr;
    statusT sts, ret = EOK;
    timestruc_t new_time;
    struct fileSetNode *fsnp;
    bfSetT *bfSetp;
    domainT *dmnP;
    ftxHT ftxH;
    vdT *vdp;
    rbfPgRefHT pgref;
    bsMPgT *bmtpgp;
    bsMCT *mcp;
    int ftxStarted = FALSE;
    int finishFtx = FALSE;
    statT *statsP;
    snap_flags_t snap_flags = SF_SNAP_NOFLAGS;
    int32_t close_snap_children = FALSE;
    int32_t mcell_list_locked = FALSE;
    context_ptr = VTOC( vp );

    /*
     * If the finish ftx flag was specified, then we must always
     * either complete or fail the caller's ftx handle that was
     * passed in.
     */

    if( flags & ADVFS_UPDATE_FINISH_FTX ) {
        finishFtx = TRUE;
        ftxH = ftx_handle;
    }

    /*
     * When there is no context pointer, there are no stats to update.
     */

    if (!context_ptr) goto EXIT_UPDATE;

    /* TODO this field is not protected with a mutex as fields below are. 
       This routine needs to be fixed in conjunction with  msfsspec_close
       since they would use the same fsContext_lock to protect the 
       fscontex structure. Any other routine that calls this would have
       to coordinate in the same way if it uses the same mutex in the
       same way.
    */
    context_ptr->dirty_stats = FALSE;

    /* No need to update stats for directory indexes. They
     * do not have stat records.
     */
    if ((IDX_INDEXING_ENABLED(bfap) && IDX_FILE_IS_INDEX(bfap))) {
        goto EXIT_UPDATE;
    }

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

    fsnp = VTOFSN(vp);

    /*
     * if read-only file set or no fileSetNode just return 
     */
    if (fsnp == NULL) {
        goto EXIT_UPDATE;
    }

    if (vp->v_vfsp->vfs_flag & VFS_RDONLY) {
        goto EXIT_UPDATE;
    }

    /*
     * if any time fields need to be updated, do it now...
     * store the utime field in microsecs instead of the
     * usual nanoseconds for future use.
     */
    ADVSMP_FILESTATS_LOCK( &context_ptr->fsContext_lock );
    if (context_ptr->fs_flag & (MOD_MTIME | MOD_ATIME | MOD_CTIME)) {
        ADVFS_GET_SYSTEM_TIME( new_time );
        if (context_ptr->fs_flag & MOD_MTIME) {
            context_ptr->dir_stats.st_mtime = new_time.tv_sec;
            context_ptr->dir_stats.st_nmtime = (int32_t)(new_time.tv_nsec);
        }
        if (context_ptr->fs_flag & MOD_ATIME) {
            context_ptr->dir_stats.st_atime = new_time.tv_sec;
            context_ptr->dir_stats.st_natime = (int32_t)(new_time.tv_nsec);
        }
        if (context_ptr->fs_flag & MOD_CTIME) {
            context_ptr->dir_stats.st_ctime = new_time.tv_sec;
            context_ptr->dir_stats.st_nctime = (int32_t)(new_time.tv_nsec);
        }
        context_ptr->fs_flag &= ~(MOD_ATIME | MOD_CTIME | MOD_MTIME);
    }
    context_ptr->dir_stats.st_size = bfap->file_size;
    ADVSMP_FILESTATS_UNLOCK( &context_ptr->fsContext_lock );

    bfSetp = bfap->bfSetp;


    /*
     * If any snapshots exist, we need to open them so that the updates can
     * be COWed to children.  If a transaction handle was passed in, then it
     * is not safe to close the children since that may trigger storage
     * deallocation (in theory).  However, if a transaction handle was
     * passed in, then the file must be open for data manipulation so the
     * last close will close the chlid.  Otherwise, this may be a flush that
     * won't do a last close so we need to close the children.
     */
    if (HAS_SNAPSHOT_CHILD( bfap )  && !(flags & ADVFS_UPDATE_NO_COW) && 
            (bfap->bfaFirstChildSnap == NULL || (bfap->bfaFlags & BFA_SNAP_CHANGE))) {
        (void)advfs_access_snap_children( bfap, ftx_handle, &snap_flags );
        if (snap_flags & SF_ACCESSED_CHILDREN) {
            if ( FTX_EQ( ftx_handle, FtxNilFtxH ) ) {
                close_snap_children = TRUE;
            } else {
                close_snap_children = FALSE;
            }
        }
        if (bfap->dmnP->dmn_panic) {
            /* Only return an error if the domain paniced from 
             * advfs_access_snap_children.  Any children that failed to 
             * be opened was marked out-of-sync.  Any children that were
             * accessed before the domain_panic will need to be closed.
             */
            ret = E_DOMAIN_PANIC;
            goto EXIT_UPDATE;
        }
    }

    dmnP = bfap->dmnP;
    if (ftx_handle.hndl == FtxNilFtxH.hndl ) {
        sts = FTX_START_N(FTA_BS_BMT_UPDATE_REC_V1, &ftxH, ftx_handle,
                          dmnP);
        if (sts != EOK) {
            domain_panic(dmnP, "fs_update_stats: can't start transaction"
                    "- sts %d", sts);
            ret = E_DOMAIN_PANIC;
            goto EXIT_UPDATE;
        }
        ftxStarted = TRUE;
    } else {
        ftxH = ftx_handle;
    }

    if (!ADVRWL_MCELL_LIST_ISWRLOCKED(&bfap->mcellList_lk)) {
        ADVRWL_MCELL_LIST_WRITE( &bfap->mcellList_lk );
        mcell_list_locked = TRUE;
    }
    vdp = VD_HTOP(bfap->primMCId.volume, dmnP);
    sts = rbf_pinpg(&pgref, (void *) &bmtpgp, vdp->bmtp,
                    bfap->primMCId.page, BS_NIL, ftxH, MF_VERIFY_PAGE);
    if (sts != EOK) {
        domain_panic(dmnP, "fs_update_stats: failed to access bmt pg %ld sts %d",
                     bfap->primMCId.page, sts);
        if (mcell_list_locked == TRUE) {
            ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
        }
        if (ftxStarted) {
            ftx_fail(ftxH);
        }
        ret = E_DOMAIN_PANIC;
        goto EXIT_UPDATE;
    }
    mcp = &bmtpgp->bsMCA[bfap->primMCId.cell];
    statsP = (struct fs_stat *)bmtr_find(mcp, BMTR_FS_STAT, dmnP);
    if (statsP == NULL) { 
        if (mcell_list_locked == TRUE) {
            ADVRWL_MCELL_LIST_UNLOCK( &bfap->mcellList_lk );
        }
        if (ftxStarted) {
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_369);
            ftx_fail(ftxH); /* No records pinned yet. */
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_452);
        }
        ret = bmtr_update_rec( bfap,
                               BMTR_FS_STAT,
                               (void *)&context_ptr->dir_stats,
                               (int16_t)sizeof(statT),
                               ftx_handle,
                               flags );
        if( finishFtx) {
            ftx_done_n( ftxH, FTA_BS_BMT_UPDATE_REC_V1 );
            finishFtx=FALSE;
        }
    } else {
        rbf_pin_record(pgref, statsP, sizeof(statT));
        flmemcpy((char *)&context_ptr->dir_stats, statsP, sizeof(statT));
        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_453);
        if (ftxStarted||finishFtx) {
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_454);
            ftx_done_n( ftxH, FTA_BS_BMT_UPDATE_REC_V1 );
            ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_455);
            finishFtx=FALSE;
        }
        if (mcell_list_locked == TRUE) {
            ADVRWL_MCELL_LIST_UNLOCK( &(bfap->mcellList_lk) );
        }
        dmnP->bmtStat.fStatWrite++;
    }

EXIT_UPDATE:

    if (finishFtx) {
        ftx_fail(ftxH);
    }

    if (close_snap_children == TRUE) {
        ADVRWL_BFAP_SNAP_WRITE_LOCK(bfap);
        (void)advfs_close_snap_children(bfap);
        ADVRWL_BFAP_SNAP_UNLOCK(bfap);
    }

    return (ret);
}


/*
 * fs_trunc_test
 *
 * This function determines if one or more allocation units for
 * the file could be truncated.
 *
 * Allocation units may be deallocated only at the end of the file.  
 * Allocated storage within a hole of a sparse bitfile is not affected.
 *
 * Quota allocations is adjusted within the transaction tree in which the 
 * storage is being relased.  Thus the BS layer must directly call
 * chk_blk_quota.  This means that the bitfile is in the ACC_INIT_TRANS
 * state when chk_blk_quota is called.  
 */

int
fs_trunc_test(struct vnode* vp,
              int64_t allocating_beyond_eof)
{
    uint64_t next_alloc_unit_to_allocate,
             in_use_alloc_units,
             rsvd_alloc_units,
             file_alloc_unit_size;
    bfAccessT *bfap = VTOA(vp);
    bfSetT *bfSetp;
    struct fsContext* fileContext = VTOC(vp);


    if ( (fileContext == NULL) || 
         (fileContext->fs_flag & META_OPEN) || 
         (bfap->dataSafety != BFD_USERDATA) ) {
        /*
         * This file was opened thru the tag interface.  Its stats
         * area is not initialized so ignore it.  Alternately, the file is
         * metadata and file_size is not initialized.
         */
        return 0;
    }

    bfSetp = bfap->bfSetp;

    /*
     * If this file has a small allocation unit and we are being called in
     * the context of allocating new storage beyond the end of the file, 
     * then for the purposes of looking for unused preallocated storage, 
     * consider its allocation unit to be VM_PAGE_SZ bytes.  That way, 
     * we will never truncate the backfilled part of the first page of the 
     * file.  Example:  A file is size 1k and has 1k of storage.  Then an 
     * application writes 4k at offset 100k.  We backfill 1k-4k to prevent a 
     * small hole.  Then we go to add storage at 100k.  We need to make sure 
     * that this call doesn't perceive the 1k-4k storage as being unused and
     * preallocated.
     */
    if (ADVFS_FILE_HAS_SMALL_AU(bfap) && allocating_beyond_eof) {
        file_alloc_unit_size = VM_PAGE_SZ;
        next_alloc_unit_to_allocate = bfap->bfaNextFob / 
                                      (file_alloc_unit_size / ADVFS_FOB_SZ);
    }
    else {
        file_alloc_unit_size = bfap->bfPageSz * ADVFS_FOB_SZ;
        next_alloc_unit_to_allocate = bfap->bfaNextFob / bfap->bfPageSz;
    }


    in_use_alloc_units  = (bfap->file_size + file_alloc_unit_size - 1L) / 
                          file_alloc_unit_size;

    rsvd_alloc_units = (bfap->rsvd_file_size + file_alloc_unit_size - 1L) /
                       file_alloc_unit_size;

    return(bfap->trunc = 
           ( (next_alloc_unit_to_allocate > in_use_alloc_units) &&
             (next_alloc_unit_to_allocate > rsvd_alloc_units) ) );
}
