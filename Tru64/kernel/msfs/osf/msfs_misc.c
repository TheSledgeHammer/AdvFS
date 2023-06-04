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
 * 
 */
#pragma ident "@(#)$RCSfile: msfs_misc.c,v $ $Revision: 1.1.597.13 $ (DEC) $Date: 2006/03/20 15:11:26 $"

#include <sys/lock_probe.h>
#include <sys/param.h>
#include <sys/user.h>
#include <sys/file.h>
#include <sys/vnode.h>
#include <sys/buf.h>
#include <sys/clu.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_osf.h>
#include <msfs/fs_dir.h>
#include <msfs/fs_dir_routines.h>
#include <sys/mode.h>
#include <sys/mount.h>
#include <msfs/ms_assert.h>
#include <vm/vm_mmap.h>
#include <machine/vm_ubc.h>
#include <msfs/bs_index.h>
#include <msfs/bs_params.h>
#include <sys/resource.h>

#define ADVFS_MODULE MSFS_MISC

extern struct vnodeops msfs_vnodeops;


static int DirStampInit;

bfAccessT *
find_bfap(bfSetT *bfSetp,
          bfTagT tag,
          int hold_hashlock,
          ulong *insert_count);

int
spec_page_read(void)
{
    return EOK;
}

int
spec_page_write(void)
{
    return EOK;
}

static
statusT
copy_frag_into_page (
                     bfAccessT *fragBfAccessp,  /* in */
                     bfFragIdT fragId,  /* in */
                     uint32T copyByteOffset,  /* in */
                     uint32T copyByteCnt,  /* in */
                     uint32T bytesPerPage,  /* in */
                     char *bufAddr  /* in */
                     );

struct lockinfo *ADVfsContext_file_lock_info;
decl_simple_lock_info(,ADVfsContext_mutex_lockinfo )


/*
 * osf_fd_to_bfap
 *
 * Converts a file descriptor to an access structure pointer.
 *
 * Returns zero if unsuccessful.
 */

struct bfAccess *
osf_fd_to_bfap(
              int fileDesc,
              struct file **fp
              )
{
    bfAccessT *bfap = NULL;
    struct file *tfp = NULL;
    struct vnode *vp = NULL;
    int error;
    register struct ufile_state *ufp = &u.u_file_state;

    /*
     ** Using the given file desc handle get the file's 'file struct'.
     */

    error = getf( &tfp,
                  fileDesc,
                  FILE_FLAGS_NULL,
                  ufp);

    if (error) {
        return 0;
    }

    /*
     ** Get file's 'vnode struct'.
     */
    vp = (struct vnode *)tfp->f_data;

    if (clu_is_ready()) {
        struct vnode *cvp = vp;
        error = CC_GETPFSVP(cvp, &vp);
        if (error) {
            error = ENOTSUP;
            goto _error;
        }
    }
    /*
     ** Make sure this is a MegaSafe file.
     */
    if (vp->v_tag != VT_MSFS) {
        goto _error;
    }

    *fp = tfp;

    /*
     * Use VTOA macro to get the correct bfap.
     */
    bfap = VTOA(vp);

    return bfap;

_error:
    FP_UNREF_MT(tfp,ufp);
    return 0;
}


/********************************************************************
***********  Init/destroy fsContext locks ***************************
********************************************************************/

/*
 * fscontext_lock_init - initialize fsContext area mutex and locks
 */
void
fscontext_lock_init( struct fsContext* fscp )
{
    mutex_init3( &fscp->fsContext_mutex, 0, "fsContextMutex",
                 ADVfsContext_mutex_lockinfo );
    lock_setup( &fscp->file_lock, ADVfsContext_file_lock_info, TRUE);
}

/*
 * fscontext_lock_destroy - destroy fsContext area mutex and locks
 */
void
fscontext_lock_destroy( struct fsContext* fscp )
{
    /*
     * mutex_destroy will run down all locks
     */
    mutex_destroy( &fscp->fsContext_mutex );
    lock_terminate(&fscp->file_lock);
}

/***************************************************************
************** Vnode based fsContext allocate/deallocate ******
***************************************************************/

/*
 * vnode_fscontext_allocate - allocate an fsContext struct for the
 * vnode and initialize the locks.
 */
struct fsContext*
vnode_fscontext_allocate(
                         struct vnode* vp
                         )
{
    struct fsContext* fscp;
    struct bfNode* bnp = (struct bfNode*)&vp->v_data[0];

    VN_LOCK( vp );

    /*
     * While holding the vnode lock, see if the fsContext structure locks
     * still need to be initialized.  This is the case when a new
     * vnode has been acquired.  Set the pointer in the bfNode
     * structure so that other threads will not initialize the
     * locks also.
     */

    if ( !(fscp = bnp->fsContextp) ) {
        if (vn_maxprivate < MSFS_VN_PRIVATE) {
            /*
             * The fsContext struct will not fit in the vnode.  Allocate it.
             */

            VN_UNLOCK( vp );
            fscp = (struct fsContext*)ms_malloc(sizeof(struct fsContext));
            if ( fscp == 0 ) {
                ADVFS_SAD0("vnode_fscontext_allocate: ms_malloc failed");
            }
            VN_LOCK( vp );

            if ( bnp->fsContextp ) {
                /* another thread beat us to it */
                VN_UNLOCK(vp);
                ms_free( fscp );
                VN_LOCK(vp);
                fscp = bnp->fsContextp;
            } else {
#ifdef ADVFS_DEBUG
                /* for debugging, the LockMgrMutex needs to be locked
                 * which is higher in hierarchy than vnode v_lock
                 */
                VN_UNLOCK( vp );
#endif
                fscontext_lock_init(fscp);
#ifdef ADVFS_DEBUG
                VN_LOCK( vp );
#endif
                bnp->fsContextp = fscp;
                fscp->initialized = 0;
                fscp->quotaInitialized = 0;
                fscp->dirty_alloc = FALSE;
                fscp->dirty_stats = FALSE;
            }
        } else {
            /*
             * The fsContext struct will fit in the vnode.
             */

            fscp = (struct fsContext *)
                   &vp->v_data[roundup( sizeof( struct bfNode ),
                                        sizeof( void * ) ) ];
#ifdef ADVFS_DEBUG
                /* for debugging, the LockMgrMutex needs to be locked
                 * which is higher in hierarchy than vnode v_lock
                 */
                VN_UNLOCK( vp );
#endif
            fscontext_lock_init( fscp );
#ifdef ADVFS_DEBUG
                VN_LOCK( vp );
#endif
            bnp->fsContextp = fscp;
            fscp->initialized = 0;
            fscp->quotaInitialized = 0;
            fscp->dirty_alloc = FALSE;
            fscp->dirty_stats = FALSE;
        }
    }

    VN_UNLOCK( vp );
    return fscp;
}


/*
 * vnode_fscontext_deallocate - deallocate an fsContext struct from a
 * vnode and destroy its locks.
 *
 * The vnode must be locked by the caller or otherwise free from
 * synchronization races with anyone trying to reference the context
 * area.
 */
void
vnode_fscontext_deallocate(
                           struct vnode* vp
                           )
{
    struct bfNode* bnp = (struct bfNode *)&vp->v_data[0];
    struct fsContext* fscp;

    if ( !(fscp = bnp->fsContextp) ) {
        return;   /* never allocated, all done */
    }
    bnp->fsContextp = NULL;

    if ( fscp->quotaInitialized ) {
        (void) detach_quota( fscp );
    }

    fscontext_lock_destroy( fscp );

    if (vn_maxprivate < MSFS_VN_PRIVATE) {
        /* Free the fs context area (for now) */
        ms_free( fscp );
    }
}

/*
 * bf_get
 *
 * Access a bitfile. Analagous to iget operations in other file systems.
 */
statusT
bf_get(
       bfTagT   bf_tag,     /* in - tag of bitfile to access */
       struct fsContext *dir_context, /* in - parent dir context pointer */
       int flag,            /* in - flag - META_OPEN, metadata bf being opened*/
                            /*           - DONT_UNLOCK, don't unlock the dir */
                            /*           - IGNORE_DELETE, ignore BSRA_DELETING*/
       struct mount *mp,    /* in - the mount structure pointer */
       struct vnode **vp    /* out - vnode corresponding to the */
                            /* accessed bitfile  */
       )
{
    return bf_get_l( bf_tag, dir_context, flag, mp, vp, NULL );
}

unsigned long Bf_get_restored_stats = 0;

/*
 * bf_get_l
 *
 * Access a bitfile. Analagous to iget operations in other file systems.
 *
 * Note that the dir_context lock is ALWAYS released in this routine unless
 * the DONT_UNLOCK flag is passed.
 *
 * bf_get open files by tag, and sets up the fsContext area and the
 * vnode for the file if they are not already in existance.
 * The META_OPEN flag means that a metadata bitfile is being opened thru the
 * special .tags directory.
 * The META_OPEN flag is set in the bf's fsContext area so that no stat
 * update occurs when the file is closed.
 *
 * Metadata bitfiles are in the root fileset.  Reserved metadata files have
 * no vnodes while tag directories have vnodes on dead_mount.  To access a
 * metadata file through .tags directory, a vnode is created on the .tags
 * mount point associated with a shadow access structure. The real access
 * structure of the metadata bitfile is recorded in the real_bfap field of
 * the shadow access structure and is used for I/O.  To facilitate searching
 * the shadow access structure, it has a pseudo-tag which has the same tag.num
 * but with a special bit in the tag.seq.  Pseudo-tags exist only in-core and
 * is used to form a unique file handle in the mount point.
 *
 * Note that the dir_context lock is ALWAYS released in this routine unless
 * the DONT_UNLOCK flag is passed.
 */
statusT
bf_get_l(
         bfTagT   bf_tag,      /* in - tag of bitfile to access */
         struct fsContext *dir_context, /* in - parent dir context pointer */
         int flag,             /* in - flag - META_OPEN, metadata bf opening */
                               /*           - DONT_UNLOCK, don't unlock dir */
         struct mount *mp,     /* in - the mount structure pointer */
         struct vnode **vp,    /* out - vnode corresponding to the accessed bf*/
         struct nameidata *ndp /* in - ptr to nameidata data */
       )
{
    int i, error;
    struct vnode *nvp;
    struct fsContext *file_context;
    bfAccessT *bfap;
    struct vm_ubc_object *obj;
    statusT sts=EOK;
    struct undel_dir_rec undel_rec;
    uint32T options;
    extern struct vnodeops spec_bfnodeops;
    extern struct vnodeops fifo_bfnodeops;
    extern int xlate_dev(dev_t, int);
    bfAccessT *real_bfap = NULL;
    int shadow = FALSE, unlockFlag = FALSE;
    struct vnode *nullvp = NULL;
    bfTagT real_tag;
    struct bfNode *bnp;

    /*
     *  Use the bit file set of the mount point.  Even if we are
     *  opening a reserved file.  This will place the vnode for
     *  reserved files opened via the .tags directory on the mount
     *  point list just like any other files.   
     *
     *  If this is for metadata files accessed from .tags, need to
     *  get the shadow bfap.
     */
    if (flag & META_OPEN ) {
        shadow = TRUE;

        /* pseudo-tag for the shadow bfap */
        BS_BFTAG_SEQ(bf_tag) |= BS_PSEUDO_TAG;
    }

    /*
     * If we are trying to open a clone fileset quota file,
     * get a vnode up front to avoid the deadlock described
     */
    if (((GETBFSETP(mp))->cloneId != BS_BFSET_ORIG) &&
        ((bf_tag.num == USER_QUOTA_FILE_TAG) || 
         (bf_tag.num == GROUP_QUOTA_FILE_TAG))) {

        sts = getnewvnode(VT_MSFS, &msfs_vnodeops, &nvp);
        if (sts != EOK) {
            /* Be sure this lock gets released unless requested not to! */
            if (!(flag & DONT_UNLOCK)) {
                FS_FILE_UNLOCK(dir_context);
            }
            return sts;
        }

        /*
         * Initialize nvp. Note that get_n_setup_vnode() called
         * from bs_access_one()  will do most of the initialization.  
         * Here, we do enough such that any error returns do proper cleanup.
         */
        bnp = (struct bfNode *)&nvp->v_data[0];
        bnp->fsContextp = NULL;
        bnp->accessp = NULL;
        options = (flag & IGNORE_DELETE) ?
                  BF_OP_HAVE_VNODE | BF_OP_IGNORE_DEL :
                  BF_OP_HAVE_VNODE;
    }
    else {
        options = (flag & IGNORE_DELETE) ?
                  BF_OP_GET_VNODE | BF_OP_IGNORE_DEL :
                  BF_OP_GET_VNODE;
    }

    /*
     * Access the bitfile associated with the bf_tag
     *
     * NOTE:  When bs_access() succeeds we must not call
     *        bs_close() (this can be done only in msfs_inactive()).
     *        vrele() is the correct way to close the file.
     */
    if (sts = bs_access( &bfap,
                         bf_tag,
                         GETBFSETP(mp),
                         FtxNilFtxH,
                         options,
                         mp,
                         &nvp ) ) {

        /* Be sure this lock gets released unless requested not to! */
        if (!(flag & DONT_UNLOCK)) {
            FS_FILE_UNLOCK(dir_context);
        }
        return sts;
    }

    *vp = nvp;

    /*
     * Setup the real_bfap if accessing metadata from .tags.
     * In this case, DONT_UNLOCK is set because dir_context is not
     * passed in.
     */
    if (shadow) {
        BS_BFTAG_IDX(real_tag) = BS_BFTAG_IDX(bf_tag);
        BS_BFTAG_SEQ(real_tag) = BS_BFTAG_SEQ(bf_tag) & ~BS_PSEUDO_TAG;
        if (bfap->real_bfap) {
            MS_SMP_ASSERT(BS_BFTAG_EQL(bfap->real_bfap->tag, real_tag));
        }
        else {
            /*
             * Need to access the real bitfile in the root bfSet.
             * Shadow is only set for META_OPEN which is set in
             * msfs_lookup() for reserved (negative) tags and
             * .tags/M<tag> which is a tagdir.  Reserved files and
             * tagdirs don't have vnodes.  (META_OPEN is not turned on
             * for regular tags which includes frag file.  The bs_access()
             * above does the open of regular tags and there is no shadow
             * bfap involved in those cases.
             */
            if (sts = bs_access( &real_bfap,
                              real_tag,
                              (GETDOMAINP(mp))->bfSetDirp,
                              FtxNilFtxH,
                              0,
                              NULL,
                              &nullvp )) {
                /*
                 * vrele() will close the bitfile, do not call
                 * bs_close() here which would close the bitfile twice!
                 */
                vrele(nvp);

                return sts;
            }
            bfap->real_bfap = real_bfap;
        }
    }

    file_context = VTOC( nvp );
    if( file_context == 0 ) {
        file_context = vnode_fscontext_allocate( nvp );
    }

    /*
     * unlock the parent dir in case we need to lock the file for
     * initing below (avoid deadlocks)
     */
    if (!(flag & DONT_UNLOCK)) {
        /*
         * Make the directory entry here, if this is a lookup call,
         * before the directory lock is released.
         */
        if ( ndp ) {

            ndp->ni_vp = nvp;

            if (ndp->ni_makeentry) {
                cache_enter(ndp);
            }
        }

        FS_FILE_UNLOCK(dir_context);
    }

    if( file_context->initialized ) {
        /*
         * fsContext has already been initialized.  We're outta here
         * with no locking, which is a good thing, else we would
         * deadlock getting "." from lookup, as well as the "always a
         * good thing to avoid" aspect of it.
         */

        if ((file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR)
        {
            /* don't get the lock if we already obtained it in lookup */
            if ( !lock_holder( &file_context->file_lock ) ) {
                FS_FILE_WRITE_LOCK( file_context );
                unlockFlag = TRUE;
            }

            if(IDX_NEED_INDEX_OPEN(bfap))
            {
                IDX_OPEN_INDEX(bfap,sts);
                
                if (sts != EOK) RAISE_EXCEPTION(sts);
                
            }

            if (unlockFlag)
                FS_FILE_UNLOCK( file_context );
        }

        return sts;
    } else {
        FS_FILE_WRITE_LOCK( file_context );
        if ( file_context->initialized ) {

            if ((file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) 
            {
                if(IDX_NEED_INDEX_OPEN(bfap))
                {
                    IDX_OPEN_INDEX(bfap,sts);

                    if (sts != EOK) RAISE_EXCEPTION(sts);
                }
            }

            FS_FILE_UNLOCK( file_context );
            return sts;
        }
    }

    /******************************************************
    ************  Initialize the fsContext area ***********
    ******************************************************/

    /*
     * Set up the bitfile's fsContext area. The files stats are
     * read in from the bmt record
     */
    file_context->bf_tag = bf_tag;

    /*
     * if it's a metadata open, set META_OPEN so don't write the stats on
     * close.
     */
    if (flag & META_OPEN) {

        file_context->fs_flag = META_OPEN;
        bfap->fragState = FS_FRAG_NONE;
        file_context->dir_stats.st_mode = S_IFREG;
        bfap->file_size = (off_t)((bfap->bfSetp->cloneId && bfap->cloneId) ?
                                   (off_t)bfap->maxClonePgs * ADVFS_PGSZ :
                                   (off_t)bfap->nextPage * ADVFS_PGSZ);

        goto finish_setup;
    }

    /* If there is a bfap->saved_stats structure, then the latest file stats
     * were never flushed to disk before the vnode was reclaimed, so
     * restore the stats from this structure rather than getting the
     * out-of-date stats from disk.  This is most likely to happen when a
     * file is accessed via NFS V3 using asynchronous writes, and there is
     * a long time between the last write and the commit.
     */
    mutex_lock(&bfap->bfaLock);
    if ( bfap->saved_stats ) {
        saved_statsT *ssp = bfap->saved_stats;
        int flush_in_progress = (ssp->op_flags & SS_FLUSH_IN_PROGRESS);

        file_context->fs_flag =
            (ssp->fs_flag & (MOD_CTIME | MOD_ATIME | MOD_MTIME ));
        file_context->dirty_stats = TRUE;

        bcopy( &(ssp->dir_stats),                      /* from */
               &( file_context->dir_stats),            /* to   */
                 sizeof(struct fs_stat) );

        Bf_get_restored_stats++;                       /* stats only */
        if ( !flush_in_progress )
            bfap->saved_stats = NULL;
        else {
            /* mark so that cleanup thread knows that this has been copied
             * back to a context structure.  It uses this info during error
             * recovery when attempting to flush stats to disk.
             */
            ssp->op_flags |= SS_STATS_COPIED_TO_CONTEXT;
        }
        mutex_unlock( &bfap->bfaLock );

        /* Only deallocate if cleanup_closed_list() not processing it! */
        if ( !flush_in_progress )
            ms_free( ssp );

    }  else {
        mutex_unlock( &bfap->bfaLock );

        /*
         * read the stats from the BMT
         */
        sts = bmtr_get_rec( bfap,
                            BMTR_FS_STAT,
                            &file_context->dir_stats,
                            sizeof(statT));
        if (sts == EBMTR_NOT_FOUND) {
            /*
             * Assume this is an open via ".tags" and that the bitfile doesn't
             * have any stats.  For example, the frag file doesn't have any.
             *
             * Treat the open as if it is a metadata open because it doesn't
             * have any stats.  We do this hack so that rmvol can open the
             * bitfile.
             */

            file_context->fs_flag = META_OPEN;
            bfap->fragState = FS_FRAG_NONE;
            bfap->fragId = bsNilFragId;
            /* no need to set fragPageOffset */
            file_context->dir_stats.st_mode = S_IFREG;
            bfap->file_size = (off_t)((bfap->bfSetp->cloneId && bfap->cloneId) ?
                                   (off_t)bfap->maxClonePgs * ADVFS_PGSZ :
                                   (off_t)bfap->nextPage * ADVFS_PGSZ);

            goto finish_setup;
        } else if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        file_context->fs_flag = 0;
        file_context->dirty_stats = 0;
    }

    bfap->file_size = file_context->dir_stats.st_size;

    /*
     * The dirstamp could be initialied to zero, but this has proven
     * handy when debugging.
     */
    file_context->dirstamp = ++DirStampInit;
    file_context->undel_dir_tag = NilBfTag;
    file_context->last_offset = 0;

    /*
     * if the file is a directory, then read it's undel_dir record
     */
    if ((file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
        sts = bmtr_get_rec( bfap,
                            BMTR_FS_UNDEL_DIR,
                            &undel_rec,
                            sizeof(struct undel_dir_rec));
        if (sts == EOK) {
            file_context->undel_dir_tag = undel_rec.dir_tag;
        }
        
        IDX_OPEN_INDEX(bfap,sts);
        if (sts != EOK) RAISE_EXCEPTION(sts); 
    }
    bfap->fragId = file_context->dir_stats.fragId;
    if ((file_context->dir_stats.fragId.frag == bsNilFragId.frag) &&
        (file_context->dir_stats.fragId.type == bsNilFragId.type)) {

        bfap->fragState = FS_FRAG_NONE;
    } else {
        bfap->fragState = FS_FRAG_VALID;
    }
    bfap->fragPageOffset = file_context->dir_stats.fragPageOffset;

    /*
     * Save the bfap, tag and the fsContext pointer in the vnode private
     * area */

finish_setup:

    /*
     * Initialize the new vnode
     */

    nvp->v_lastr = 0;

    file_context->fileSetNode = GETFILESETNODE(mp);

    for (i = 0; i < MAXQUOTAS; i++) {
        file_context->diskQuot[i] = NULLDQUOT;
    }

    file_context->quotaInitialized = 1;

    /* set the vnode type */

    nvp->v_type = TYPTOVT( file_context->dir_stats.st_mode );

    switch (nvp->v_type) {

        /*
         * Set/clear the VDIRECTIO flag of the vnode for VREG files
         * based on the cache policy of the set.  But don't set
         * the VDIRECTIO flag if the file uses data logging.  If the
         * file was opened via .tags, check if the original file is
         * also protected via the dataSafety field to prevent DIRECTIO
         * mode from being set.
         */
        case VREG:
            if (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO) {
                VN_LOCK(nvp);
                if ( (bfap->dataSafety != BFD_FTX_AGENT) &&
                     (bfap->dataSafety != BFD_FTX_AGENT_TEMPORARY) &&
                    !(bfap->real_bfap && 
                      bfap->real_bfap->dataSafety == BFD_FTX_AGENT) )
                    nvp->v_flag |= VDIRECTIO;
                else 
                    nvp->v_flag &= ~VDIRECTIO;
                VN_UNLOCK(nvp);
            }
            break;

        case VDIR:
        case VLNK:
        case VSOCK:
            break;

        case VFIFO:
            nvp->v_op = &fifo_bfnodeops;
            break;

        case VBLK:
        case VCHR:
            if (((file_context->dir_stats.st_rdev >> 16) & 0xffff) == 0) {
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
                if (((file_context->dir_stats.st_rdev >> 8) & 0xff) == 0) {
                    /*
                     * NOTE: Major number zero is restricted to an
                     * 8-bit minor so that translation from old to
                     * new format can be performed.
                     */
                    file_context->dir_stats.st_rdev =
                        makedev( 0,
                                ((file_context->dir_stats.st_rdev) & 0xff) );
                } else {
                    /*
                     * We have an old format dev_t so call the
                     * translation routine, which will translate
                     * old to new.
                     *
                     * Set dirty_stats to TRUE, so the 32 bit dev_t will
                     * be written to disk.
                     */
                    file_context->dir_stats.st_rdev =
                        xlate_dev(file_context->dir_stats.st_rdev, 1);
                    file_context->dirty_stats = TRUE;
                }
            }

            if (error = specalloc( nvp, file_context->dir_stats.st_rdev )) {
                RAISE_EXCEPTION((statusT) error);
            }

            nvp->v_op = &spec_bfnodeops;
            break;

        default:
            RAISE_EXCEPTION(EINVAL);
    }

    /*
     * If this file type doesn't use ubc, don't keep unneeded ubc object.
     * Vnode hasn't made it to mount queue yet, bfAccess is on hash queue
     * so must serialize with bs_access_one().
     */
    if ((nvp->v_type == VCHR || nvp->v_type == VBLK || nvp->v_type == VFIFO)
        && bfap->bfObj ) {
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
    bs_insmntque(FtxNilFtxH, bfap, mp);

    /*
     * Attach file quotas if the file's vnode was inserted onto the file set's
     * mount structure.  If the file's bitfile set is the root tag directory,
     * the vnode is inserted onto a mount queue but quotas should not be
     * turned on.  See bs_access_one() in bs_access.c.
     *
     * Files that are reserved files are the log, the bmts,
     * the sbms and the tag directories.    
     */
    if(!BS_BFTAG_RSVD(bf_tag) && !shadow) {
        /*
         * Attach file quotas.
         */
        (void) attach_quota(file_context, FtxNilFtxH);
    }

    /*
     * Note that the context area is now completely initialized and
     * unlock it.
     */

    file_context->initialized = 1;

    FS_FILE_UNLOCK(file_context);

    return EOK;

HANDLE_EXCEPTION:
    FS_FILE_UNLOCK(file_context);

    nvp->v_type = VNON;
    if (ndp) {
      ndp->ni_vp = NULL;
    }
    vrele( nvp );

    return sts;
}

extern spec_revokeop(), spec_ebadf(), spec_nullop(), spec_print(),
        spec_select_revokeop();
        spec_badop(),
        spec_lockctl_revokeop(),
        msfs_inactive(struct vnode *vp),
        msfsspec_reclaim();

/*
 * The msfsrevoke_vnodeops vector is very similar to revoke_vnodeops,
 * EXCEPT for the inactive, reclaim ptrs.  These must be AdvFS
 * specific to allow applicable cleanup.
 */
struct vnodeops msfsrevoke_vnodeops = {
        spec_revokeop,          /* lookup */
        spec_revokeop,          /* create */
        spec_revokeop,          /* mknod */
        spec_revokeop,          /* open */
        spec_revokeop,          /* close */
        spec_ebadf,             /* access */
        spec_ebadf,             /* getattr */
        spec_ebadf,             /* setattr */
        spec_revokeop,          /* read */
        spec_revokeop,          /* write */
        spec_revokeop,          /* ioctl */
        spec_select_revokeop,   /* select */
        spec_revokeop,          /* mmap */
        spec_nullop,            /* fsync */
        spec_revokeop,          /* seek */
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
        spec_revokeop,          /* bmap */
        spec_revokeop,          /* strategy */
        spec_print,             /* print */
        spec_badop,             /* page_read */
        spec_badop,             /* page_write */
        spec_revokeop,          /* swap */
        spec_revokeop,          /* buffer read */
        spec_revokeop,          /* buffer release */
        spec_lockctl_revokeop,  /* file locking */
        spec_nullop,            /* fsync byte range */
        spec_revokeop,          /* Lock a node */
        spec_revokeop,          /* Unlock a node */
        spec_revokeop,          /* Get extended attributes */
        spec_revokeop,          /* Set extended attributes */
        spec_revokeop,          /* Delete extended attributes */
        spec_revokeop,          /* pathconf */
};

/*
 * msfs_inactive
 *
 * Inactivate a vnode.  Called from vrele when
 * vp->v_usecount == 1.  Since vnode isn't locked
 * when we are called, v_usecount could be up again
 * in which case we just return.
 * taken care of in vfs by use of VINACTIVATING.
 */
msfs_inactive(
              struct vnode *vp /* in - vnode to inactivate */
              )
{
    bfAccessT *tempbfap;
    struct bfNode* bnp;
    bfAccessT *real_bfap = NULL;

    FILESETSTAT( vp, msfs_inactive );

    /*
     * Get the access handle (if there is one) and close the bf.
     * If the bfap is a shadow, need to close the real bf too.
     */
    bnp = (struct bfNode *)&vp->v_data[0];
    tempbfap = bnp->accessp;

    /*
     * If tempbfap != NULL, check if truncation is possible (sets flag in
     * access struct as its side effect if sensible).  This use to be done
     * in msfs_close, but as seen, we may not go through
     * msfs_close() yet we have some preallocated memory that we should give
     * back.  In all cases we must come here, so the fs_trunc_test() is
     * made here.
     */

    if (tempbfap != NULL) {
        fs_trunc_test( vp );
    }

    if (tempbfap != NULL && tempbfap->real_bfap) {
        real_bfap = tempbfap->real_bfap;
        bs_close(real_bfap, MSFS_DO_VRELE);
        tempbfap->real_bfap = NULL;
    }

    if (tempbfap) {
        /*
         * Cleared in bs_close under the BfAccessTblMutex when
         * called from here, to synchronize correctly with the bitfile
         * access path.
         *
         * If there is no access handle, this bitfile was never opened
         * from the file system, only from the bitfile layer, hence
         * the "extra" file system deref of the bitfile is irrelevant.
         */
        /* At this point we need to check if it is a directory being
         * closed and if so, then also close any associated index file.
         */
        if ( IDX_INDEXING_ENABLED(tempbfap) &&
             IDX_FILE_IS_DIRECTORY(tempbfap) )
        {
            /* Close the associated index file and deal
             * with any clone processing.
             */
            IDX_CLOSE_INDEX(tempbfap);
        }

        /* take a ref on the bfap to prevent it from moving to the freelist
         * in bs_close(). This is needed until bfNode.accessp is cleared 
         * below.
         */
        mutex_lock(&tempbfap->bfaLock);
        tempbfap->refCnt++;
        mutex_unlock(&tempbfap->bfaLock);

        /*
         * Advise bs_close that it is being called from
         * msfs_inactive so it doesn't call vrele.
         * */
        (void)bs_close(tempbfap, MSFS_INACTIVE_CALL);
    }

    VN_LOCK(vp);
    /*
     * Dispose of revoked vnodes.
     * Note that it is VERY important that the reclaim op be called before
     * the accessp ptr is cleared below (for revoked vnodes).  Without 
     * this, determining its access struct (bfap) is impossible.
     */
    if (vp->v_flag & VREVOKED)
        (void) vgone(vp, VX_INACTIVE, 0);

    /*
     * Okay to clear out our accessp now. 
     * Note that we MUST hold VN_LOCK, to assure that we don't
     * clear this ptr while the VTOA macro calls (pre-vget) in 
     * msfs_sync_mmap are dereferencing it.
     */
    bnp->accessp = NULL;

    /*
     * Change v_type VREG to VNON.  There may be
     * wired buffers still in the object and we don't want reclaim
     * (vgone, vclean) to try to run them down.
     *
     * This must be done for all access structures, including
     * those not mounted by the file system.
     */
    if (vp->v_type == VREG)
        vp->v_type = VNON;

    VN_UNLOCK(vp);

    /* decrement ref taken on bfap - do this here to avoid lock hierarchy
     * problem with vnode lock
     */
    if (tempbfap) {
        mutex_lock(&tempbfap->bfaLock);
        DEC_REFCNT(tempbfap);
        mutex_unlock(&tempbfap->bfaLock);
    }

    return (0);
}


/* msfs_reclaim_cleanup_failures statistics:
 *      0 = find_bfap() on tag and set from bfNode didn't return access struct
 *      1 = bfap from find_bfap() doesn't point to vp being reclaimed
 */
u_long msfs_reclaim_cleanup_failures[2] = {0L,0L};
u_long msfs_reclaim_saved_stats = 0L;
u_long msfs_reclaim_lost_stats  = 0L;
/*
 * msfs_reclaim
 *
 * reclaim a vnode
 * The VX_LOCK flag is set in the vnode to synchronize
 * with callers of vget.
 */
msfs_reclaim(
             struct vnode *vp, /* in - vnode to reclaim */
             int flags
             )
{
    struct bfNode *bnp;
    bfSetT *bfSetp;
    bfAccessT *bfap = NULL;

    FILESETSTAT( vp, msfs_reclaim );
    bnp = (struct bfNode *)&vp->v_data[0];

    /*
     * Clear vnode pointer out of access struct.
     * It is possible that the access struct does not exist or has
     * been recycled, so check that its vnode is
     * the same before clearing it.
     */

    /* Grab_bsacc will zero out the bfSetId to indicate that we have
     * reached this point because it called vgone and we do not need
     * to do anymore work. Grab_bsacc has taken care of disassociating
     * the vnode and access structure already. We mainly are avoiding
     * the call to bs_bfs_lookup_desc which can be costly if there are
     * a lot of mounted filesets.  */

    if (!BS_BFS_EQL(bnp->bfSetId,nilBfSetId) )
        bfSetp = bs_bfs_lookup_desc( bnp->bfSetId );
    else
        bfSetp = NULL;

    if ( BFSET_VALID(bfSetp) ) {

        /*
         * RACE CONDITION
         *
         * Between the call to bs_bfs_lookup_desc() above and the mutex_lock()
         * below the set handle could have been reused for another bitfile
         * set.  This means that the find_bfap() below could find an
         * access struct belonging to another file.  No problem since
         * must have a different vnode so the vnode check ensures that
         * we ignore the access struct.  Also, note that if the set handle
         * is recycled that we know that there cannot exist any access
         * structs associated with the set that we are currently interested
         * in because we always invalidate all access structs associated
         * with a set that is being "tossed".
         */
        bfap = VTOA(vp);
        if (bfap != NULL) {
            /*
             * If this vnode has been revoked, we can't use
             * find_bfap to get to our bfap, so we'll get there the
             * most direct way...
             */
            mutex_lock(&bfap->bfaLock);
        } else {
            /* returns the requested access struct locked */
            bfap = find_bfap( bfSetp, bnp->tag, FALSE, NULL);
        }

       /* At this point, if there is a bfap, its bfaLock is held */
       if ( bfap && (vp == bfap->bfVp) ) {
            vm_ubc_object_t vop = vp->v_object;

            if((bfap->bfState == BSRA_INVALID ) && vop && vop->vu_dirtypl) {
                /* invalid access struct can't have any dirty ubc pages */
                printf( "msfs_reclaim: dirtypl %d, npages %d, wirecnt %d\n",
                        vop->vu_dirtypl, vop->vu_npages, vop->vu_wirecnt );
                ADVFS_SAD1( "msfs_reclaim: invalid access struct dirty pages",
                            bfap->tag.num );
            }

            /* Remove this access struct from the free list (if on it).
             * Up the refCnt on the access struct to prevent
             * it from being recycled if we sleep on getting
             * a buffer in bs_pinpg.
             */
            RM_ACC_LIST_COND( bfap );
            bfap->refCnt++;

            /*
             * If the vm_object has dirty pages (by mmap)
             * flush them.
             */
            if( vop ) {
                struct vnodeops *vnop_save = vp->v_op;
                boolean_t fragfile, idxfile;

                /* If the access structure is a clone, then
                 * skip invalidating and flushing the pages.
                 * This will not be a problem since clones are
                 * read-only. The only way there could be dirty
                 * pages is on a COW, but that will flush them 
                 * out. Leaving pages on the clean list will get
                 * picked up in grab_bsacc at recycle time.
                 *
                 * We don't get vnodes for internal opens of frag and
                 * index files. If we are in this path, it must have
                 * been an open via .tags. Don't invalidate and flush
                 * pages here - internal close will take care of that.
                 */

                fragfile = (bfap == bfap->bfSetp->fragBfAp);
                idxfile = (IDX_INDEXING_ENABLED(bfap) && (vp->v_type != VDIR));
                if( (((bfSetT *)bfap->bfSetp)->cloneId == BS_BFSET_ORIG) &&
                    !(fragfile) && !(idxfile) )
                {
                    vp->v_op = &msfs_vnodeops;
                    mutex_unlock(&bfap->bfaLock);
                    msfs_flush_and_invalidate(bfap, INVALIDATE_ALL);
                    mutex_lock(&bfap->bfaLock);
                    vp->v_op = vnop_save;
                }
            }

            /*
             * FIX -
             * This call to fs_update_stats() is bogus!  Because
             * the VXLOCK flag is set in the vnode, fs_update_stats()
             * will immediately return and do nothing.  */
            if (VTOC(vp) && VTOC(vp)->dirty_stats) {
                /* We are going to lose the context structure in a moment,
                 * but there are file statistics that need to be flushed to
                 * disk.  The problem is that we may have been called
                 * indirectly from an advfs routine where a transaction has
                 * already been started, but we do not have the transaction
                 * handle to start a subtransaction.  And incorrectly
                 * starting a root transaction is death.  So, we will save
                 * the file statistics from the fsContext into a structure
                 * that will hang off the access structure until: a) the
                 * file is accessed again when we will copy the stats back
                 * into the new fsContext struct, or b) the cleanup thread
                 * processes the acccess structure which will be on the
                 * closed list.  It can easily start a root transaction, flush
                 * the stats to disk, and then move the access struct to the
                 * free list.
                 *
                 * Taking a clue from fs_update_stats(), we only save the
                 * stats if this is NOT a readonly file and the file was not
                 * opened via the .tags directory.
                 */
                struct mount *mp = VTOMOUNT (vp);
                struct fileSetNode *fsNp = GETFILESETNODE(mp);
                struct fsContext *context_ptr = VTOC( vp );
                saved_statsT *ssp;

                if ( !(mp->m_flag & M_RDONLY)            &&
                     !(fsNp->fsFlags & FS_CLONEFSET)     &&
                     !(context_ptr->fs_flag & META_OPEN) ) {

                    /* Because we are lazy about flushing these stats out to
                     * disk, it is possible we are reclaiming a vnode that
                     * still has saved_stats from the previous reclaim.  Not
                     * to worry; we will just update the saved stats that have
                     * already been allocated.
                     */
                    if ( !bfap->saved_stats ) {
                        /* This should be the typical path */
                        mutex_unlock( &bfap->bfaLock );
                        ssp = (saved_statsT *)ms_malloc( sizeof(saved_statsT) );
                        mutex_lock( &bfap->bfaLock );
                    } else
                        ssp = bfap->saved_stats;

                    if ( ssp ) {
                        ssp->op_flags = 0;
                        ssp->fs_flag = context_ptr->fs_flag;
                        bcopy( &(context_ptr->dir_stats),    /* from */
                               &(ssp->dir_stats),            /* to   */
                               sizeof(struct fs_stat) );
                        ssp->dir_stats.st_size        = bfap->file_size;
                        ssp->dir_stats.fragId         = bfap->fragId;
                        ssp->dir_stats.fragPageOffset = bfap->fragPageOffset;

                        /*
                         * make a copy of context_ptr->dirty_alloc.
                         */
                        ssp->dirty_alloc = context_ptr->dirty_alloc;

                        bfap->saved_stats = ssp;
                        msfs_reclaim_saved_stats++;          /* stats only */
                    } else
                        msfs_reclaim_lost_stats++;           /* stats only */
                }
            } else if (VTOC(vp) && !(VTOC(vp)->dirty_stats)) {
                if (bfap->saved_stats) {
                /* Not sure this ever happens, but this is the correct thing
                 * to do:  We are reclaiming a vnode that has no dirty_stats
                 * but there is an old saved_stats structure still hanging
                 * around.  This is obviously out-of-date, so just deallocate
                 * it.  It's presence makes the access structure go onto the
                 * closed list rather than the free list, and we don't want
                 * that in this case.
                 */
                 ms_free(bfap->saved_stats);
                 bfap->saved_stats = NULL;
               }
            }

            bfap->bfVp = NULLVP;
            /*
             * Since the vnode has been recycled, this access
             * structure is a good candidate to be recycled also.
             */
            DEC_REFCNT( bfap );
        } else {
            /*
             * For debugging record times msfs_reclaim() couldn't clean
             * up the access structure.  Note that find_bfap() failures
             * should correspond to access structures being recycled in
             * grab_bsacc().
             */
            if (bfap)
                msfs_reclaim_cleanup_failures[1]++;
            else
                msfs_reclaim_cleanup_failures[0]++;
        }

        bnp->tag = NilBfTag;
        bnp->bfSetId = nilBfSetId;
        if (bfap)
            mutex_unlock(&bfap->bfaLock);
    }

    /*
     * This vnode may or may not be recycled to another file system
     * type, so best to clear out any linkages threaded through the
     * fsContext area.
     */

    vnode_fscontext_deallocate( vp );

    vp->v_object = VM_UBC_OBJECT_NULL;

    return( 0 );
}

int
copy_frag_into_vm_page(
                       vm_ubc_object_t vop,
                       vm_offset_t offset,  /* in */
                       bfAccessT  *fragBfAccessp,  /* in */
                       bfFragIdT fragId,  /* in */
                       uint32T bytesPerFilePage,  /* in */
                       vm_page_t pp
                       )
{
    int error;
    int rflags = 0;
    uint32T copyByteCnt;
    uint32T copyByteOffset;
    uint32T fragByteCnt;
    vm_offset_t kernelAddr;
    uint32T relativePageOffset;
    statusT sts;

    kernelAddr = ubc_load (pp, 0, PAGE_SIZE);

    /*
     * Convert the byte offset into a relative vm page within the file page.
     */

    relativePageOffset = (offset % bytesPerFilePage) / PAGE_SIZE;

    copyByteOffset = relativePageOffset * PAGE_SIZE;
    fragByteCnt = fragId.type * 1024;
    if (copyByteOffset < fragByteCnt) {
        /*
         * The offset and the frag intersect.  Copy the part of the frag
         * that belongs in the vm page into the page.
         */
        copyByteCnt = fragByteCnt - copyByteOffset;
        if (copyByteCnt > PAGE_SIZE) {
            copyByteCnt = PAGE_SIZE;
        }
        sts = copy_frag_into_page (
                                   fragBfAccessp,
                                   fragId,
                                   copyByteOffset,
                                   copyByteCnt,
                                   bytesPerFilePage,
                                   (char *)kernelAddr
                                  );

        if (sts != EOK) {
            ubc_unload (pp, 0, PAGE_SIZE);
            return EIO;
        }

        if (copyByteCnt < PAGE_SIZE) {
            bzero ((void *)(kernelAddr + copyByteCnt), PAGE_SIZE - copyByteCnt);
        }
    } else {
        /*
         * The offset is past the end of the frag.
         */
        bzero ((void *)kernelAddr, PAGE_SIZE);
    }

    ubc_unload (pp, 0, PAGE_SIZE);
    return( 0 );
}

/*
 * Used by mmap() to bump the reference count on an
 * AdvFS vnode.  We also keep a count in the
 * access structure of mmap()'ers.  This is
 * guarded by the bfaLock in the access
 * structure.
 */
msfs_refer(vm_ubc_object_t vop)
{
    struct fsContext *contextp;

    MS_SMP_ASSERT(vop->vu_ap);        /* we have an access structure */
    MS_SMP_ASSERT(vop->vu_ap->bfVp);    /* that has a vnode */
    /* vnode and access point to the same object */
    MS_SMP_ASSERT(vop->vu_ap->bfObj == vop->vu_ap->bfVp->v_object);
    contextp = VTOC(vop->vu_ap->bfVp);
    MS_SMP_ASSERT(contextp != NULL);

    VREF(vop->vu_ap->bfVp);

    mutex_lock( &vop->vu_ap->bfaLock);
    vop->vu_ap->mmapCnt++;
    mutex_unlock( &vop->vu_ap->bfaLock);

}

/*
 * Used by mmap() to decrement the reference count on an
 * AdvFS vnode.  We also decrement the mmap()'er count
 * in the access structure.  The bfaLock in the
 * access structure is seized and released here.
 */
msfs_release(vm_ubc_object_t vop)
{
    struct fsContext *contextp;
    bfAccessT *bfap = vop->vu_ap;

    MS_SMP_ASSERT(bfap);        /* we have an access structure */
    MS_SMP_ASSERT(bfap->bfVp);    /* that has a vnode */
    /* vnode and access point to the same object */
    MS_SMP_ASSERT(bfap->bfObj == bfap->bfVp->v_object);
    contextp = VTOC(bfap->bfVp);
    MS_SMP_ASSERT(contextp != NULL);

    /*
     * The access structure mmapCnt accurately reflects the
     * current number of times the file is mmapped.  The flag
     * VMMAPPED is turned on in msfs_mmap() and does not change
     * on an unmap().  VMMAPPED is only reset when all references
     * to the vnode are gone in close_one_int().  The locking
     * in msfs_mmap(), fs_read(), and fs_write() with regard to
     * the vm_map and file_lock depend on this fact.  Do not
     * turn off VMMAPPED here or deadlocks could result!
     */
    if((bfap->bfVp->v_mount->m_flag & M_ADL)) {
        FS_FILE_WRITE_LOCK(contextp);
        mutex_lock(&bfap->bfaLock);
        bfap->mmapCnt--;
        /*
         * If the mmapCnt is now zero (this was the last mmapper)
         * and the mount point is using temporary data logging
         * (mount -o adl), and the file is using normal asynchronous
         * I/O, then reactivate temporary data logging on this file.
         */
        if ((!bfap->mmapCnt) && (bfap->dataSafety == BFD_NIL)) {
           bfap->dataSafety = BFD_FTX_AGENT_TEMPORARY;
        }
        mutex_unlock(&bfap->bfaLock);
        FS_FILE_UNLOCK(contextp);
    } else {
        mutex_lock(&bfap->bfaLock);
        bfap->mmapCnt--;
        mutex_unlock(&bfap->bfaLock);
    }

    vrele(bfap->bfVp);
}

/*
 * msfs_putpage
 *
 * Assumptions: storage is already allocated,
 * individual pages are not related ie. they are
 * not in file page order.  Should really mark
 * pagelist entry null after page is successfully
 * pushed.  However, we cannot malloc an array
 * here.  Assume bf page size is not less than
 * vm page size.
 */

int
msfs_putpage(
    vm_ubc_object_t vop,
    vm_page_t *pl,
    int pcnt,
    int flags,
    struct ucred *cred )
{
    vm_page_t pp, plp;
    bfPageRefHT page_ref;
    void *page_addr;
    int page;
    vm_offset_t start, offset;
    statusT res;
    int i;
    int pushCnt = 0;
    int error = 0;
    int plcnt;
    unsigned noReadMask = 0;
    int decr_access = 0;
    bfAccessT *bfap = NULL, *tempbfap;
    struct vnode *vp = vop->vu_vp;
    struct fsContext *contextp;
    vdT *vdp;
    int klusterPages;

    bfap = vop->vu_ap;
    MS_SMP_ASSERT(bfap);
    vp = bfap->bfVp;

    /*
     * The UBC may flush a page belonging to a file, such as a
     * clone file, that has no vnode.  So vp may be NULL here.
     */
    if (vp) {
        FILESETSTAT( vp, msfs_putpage ); 

        contextp = VTOC( vp );
        tempbfap = VTOA( vp );

        if( tempbfap == NULL ) {
    
            /*
             * File is inactive, we are flushing dirty buffers after
             * a close.  Get an access handle.  The access struct
             * cannot be recycled since it has dirty pages.
             */

            mutex_lock( &bfap->bfaLock );
            MS_SMP_ASSERT(lk_get_state(bfap->stateLk) != ACC_INVALID);
            MS_SMP_ASSERT(bfap->bfState != BSRA_INVALID);
            decr_access = 1;
            RM_ACC_LIST_COND( bfap );
            bfap->refCnt++;
            mutex_unlock(&bfap->bfaLock);
        }
        else {
            MS_SMP_ASSERT(tempbfap == bfap);
        }
    }

    /*
     * ubc_msync() will call FSOP_PUTPAGE() for each dirty page in the 
     * msync() range, passing flag B_ASYNC for AdvFS to set up the I/O.  
     * Then, it will call a final time with the B_MSYNC flag to tell 
     * AdvFS to set the times and flush the pages set up by the
     * previous call.  
     */

    if (flags & B_MSYNC) {
        if (vp) {
            contextp = VTOC( vp );

            mutex_lock( &contextp->fsContext_mutex );
            /*
             * st_ctime in the file's stat structure
             */
            contextp->fs_flag |= MOD_CTIME | MOD_MTIME;
            contextp->dirty_stats = TRUE;

            mutex_unlock( &contextp->fsContext_mutex );
        }

        /*
         * Check for sync write or msync(MS_ASYNC | MS_INVALIDATE)
         */
        if( !(flags & B_ASYNC) || (flags & B_FREE) ) {
            res = bfflush(bfap,
                          ((vm_offset_t)pl) / ADVFS_PGSZ,
                          (vm_size_t)pcnt,
                          FLUSH_INTERMEDIATE);
            if( res != EOK ) {
                error = EIO;
            }
        }
        if( decr_access ) {
            mutex_lock( &bfap->bfaLock );
            DEC_REFCNT( bfap );
            mutex_unlock( &bfap->bfaLock );
        }
        return( error );
    }

    for(;;) {

        if( pushCnt == pcnt ) {
            break;
        }

        pp = *pl;
        MS_SMP_ASSERT(pp && pp->pg_busy && (pp->pg_reserved & VPP_UBCIO));

        /* Temporary race condition check, previously we purged the page
         * if ((pp->pg_reserved & VPP_INVALID) && !(flags & B_INVAL))
         *  // We want to skip the write if we are called from
             / ubc and the page is being invalidated. Contrarily, we
             / also don't want to skip the write if ubc_invalidate()
             / is calling us to write the page (B_INVAL flag is set).
             / The latter condition is for vclean path.
         * but now we should never be here with VPP_INVALID from other
         * paths.  It should be "no harm, no foul" to either throw the
         * page away or write it, but maybe there is a path I missed... so
         */
        MS_SMP_ASSERT( !(pp->pg_reserved & VPP_INVALID) || (flags & B_INVAL) );

        /*
         * bf page number
         * start is offset at start of bf page.
         */
        page = pp->pg_offset / ADVFS_PGSZ;
        start = (vm_offset_t) page * ADVFS_PGSZ;

        /* 
         * Grab all the vm pages that are dirty and can fit into one I/O.
         * Note that these pages may actually span vdT's so this is
         * not really good clustering of I/Os in some situations.  But
         * most of the time it will give good clustering results.
         */
        vdp = vd_htop_if_valid(((struct bsBuf *)pp->pg_opfs)->ioList.ioDesc->
                               blkDesc.vdIndex, bfap->dmnP, TRUE, FALSE);
        if (vdp) {
            klusterPages = vdp->wrmaxio / ADVFS_PGSZ_IN_BLKS;
            vd_dec_refcnt(vdp);
        }
        else {
            /*
             * Hmmm...vdp is in transition.  Take a reasonable guess
             * as to a maximum I/O size.
             */
            klusterPages = WRMAXIO / ADVFS_PGSZ_IN_BLKS;
        }
        plp = ubc_dirty_kluster(vop, pp, start, 
                                (vm_size_t)ADVFS_PGSZ * klusterPages,
                                flags, FALSE, &plcnt);

        /* 
         * Send these pages to the ubc request queue.
         */
        res = bs_pinpg_put(plp, plcnt, flags);
        if (res != EOK) {
            if (decr_access) {
                mutex_lock(&bfap->bfaLock);
                DEC_REFCNT( bfap );
                mutex_unlock(&bfap->bfaLock);
            }
            return( EIO );
        }

        *pl++ = NULL;
        pushCnt++;
    }

    if( decr_access ) {
        mutex_lock(&bfap->bfaLock);
        DEC_REFCNT( bfap );
        mutex_unlock(&bfap->bfaLock);
    }

    return( error );
}

/*
 * msfs_getpage
 */

int
msfs_getpage(
    vm_ubc_object_t vop,
    vm_offset_t offset,
    vm_size_t len,
    vm_prot_t *protp,
    vm_page_t *pl,
    int plsz,
    vm_policy_t vmp,
    int ubc_flags,
    struct ucred *cred )
{
    struct vnode *vp;
    struct fsContext *contextp;
    bfAccessT *bfap;
    unsigned long fsize;
    int pageCnt;
    int page;
    int error = 0;
    int trunc_xfer_lock = 0;
    int have_write_lock = FALSE;
    int unlock_file_lock = FALSE;
    int writing;
    int prior_read_count;


    bfap = vop->vu_ap;
    vp = bfap->bfVp;
    MS_SMP_ASSERT(vp);
    /*
     * If this file is open for direct I/O, we cannot support VM/UBC
     * based accesses to this file (i.e. NFS).  Returning EOPNOTSUPP
     * indicates to NFS that this operation cannot be allowed and it
     * will "fall back" to VOP_READ based access.
     */
    if (vp->v_flag & VDIRECTIO)
	return EOPNOTSUPP;
    FILESETSTAT( vp, msfs_getpage );

    contextp = VTOC( vp );
    MS_SMP_ASSERT(contextp->initialized);

    writing = !(ubc_flags & B_READ);

    *pl = VM_PAGE_NULL;

    /* screen domain panic */
    if (bfap->dmnP->dmn_panic) {
        error = EIO;
        goto _error;
    }


    page = offset / ADVFS_PGSZ;
    pageCnt = ((offset % ADVFS_PGSZ) + len + ADVFS_PGSZ - 1) / ADVFS_PGSZ;
    if ( !pageCnt )
        pageCnt = 1;

    if ( writing && bfap->bfSetp->cloneSetp != NULL ) {
        bs_cow( bfap, COW_PINPG, page, pageCnt, FtxNilFtxH );
    }

    if (lock_holder(&contextp->file_lock)) {
        /* This is an uncommon, but possible, path. */
        have_write_lock = TRUE;
    } else {
        FS_FILE_READ_LOCK(contextp);
        unlock_file_lock = TRUE;
	/*
	 * We need to check again if we had to acquire the file lock, i.e.
	 * the file could have transitioned between the lock-naked check
	 * above.  If we already had the lock, we're fine because the
	 * transition requires the acquisition of the lock.
	 */
	if (vp->v_flag & VDIRECTIO) {
	    error = EOPNOTSUPP;
	    goto _error;
	}
    }

    /* It is an error to read/write beyond the file's last page,
     * we need to check because some callers don't have the lock.
     * Because of the way nfs works and the problems of racing
     * truncation, we only report an error for vm, other callers
     * get success and fewer (maybe 0) pages.
     */
    fsize = bfap->file_size;

    if (offset >= fsize) {
        /* for mmappers this is an error that becomes sigbus.
         * for nfs this is success and 0 pages returned.
         */
        if (protp)
            error = KERN_INVALID_ADDRESS;
        goto _error;
    }

    if ((offset + len) > round_page (fsize)) {
        if (protp) {
            error = KERN_INVALID_ADDRESS;
            goto _error;
        }

        /* for nfs return the pages up through eof with no error */
        pageCnt = (fsize - 1)/ADVFS_PGSZ - page + 1;
    }

    if (bfap->origAccp) {
        if (writing) {
            /* clones are read-only */
            MS_SMP_ASSERT(0); /* so we can punish the guilty */
            error = KERN_INVALID_ARGUMENT;
            goto _error;
        }
        TRUNC_XFER_LOCK_READ(&bfap->origAccp->trunc_xfer_lk);
        trunc_xfer_lock = 1;
    }

    prior_read_count = u.u_tru.tru_inblock;

    /*
     * Process each page requested, the way the code works today
     * requires there to be exactly 1 advfs page for each vm page.
     */

    if (pageCnt > plsz)
        pageCnt = plsz;

    for ( ; pageCnt; pageCnt--, page++ ) {

        bfPageRefHintT refHint;

        /* Determine read ahead */
        if ( page == vp->v_lastr + 1 ) {
            refHint = BS_SEQ_AHEAD;
        } else {
            refHint = BS_NIL;
        }

        /* get this page in the UBC and bump the pg_hold refcount. */

        if (!writing) {
            error = bs_refpg_get(bfap, page, refHint,
                                 pl, vmp, offset, len, ubc_flags);
            if (error) {
                MS_SMP_ASSERT(error != E_PAGE_NOT_MAPPED);
                break;
            }
            if (protp) {
                *protp = VM_PROT_WRITE;
  	        protp++;
            }
        }
        else {
            error = bs_pinpg_get(bfap, page, refHint,
                                 pl, vmp, offset, len, ubc_flags);
            if (error) {
                /* we could be writing a hole or a frag.  in most cases
                 * on the first unmapped page the call we make below to
                 * fs_write_add_stg() will allocate multiple (we hope all)
                 * pages.  if fs_write_add_stg() didn't allocate all the
                 * pages for any reason, such as a bigpage request larger
                 * than MAX_ALLOC_PAGE_CNT, we will try again when we hit
                 * the next unmapped page.
                 */
                if (error == E_PAGE_NOT_MAPPED) {
                    if (!have_write_lock) {
                        if (FS_FILE_READ_TO_WRITE_LOCK(contextp))
                            FS_FILE_WRITE_LOCK(contextp);
                        unlock_file_lock = have_write_lock = TRUE;

                        /* someone could have truncated the file or added
                         * the storage if we slept getting the write lock.
                         * no special code here for nfs, they do read only.
                         */
                        fsize = bfap->file_size;
                        if (offset >= fsize ||
                            (offset + len) > round_page (fsize) ) {
                            error = KERN_INVALID_ADDRESS;
                            break;
                        }

                        if (page_is_mapped(bfap, page, NULL, TRUE))
                            goto have_storage;
                    }

                    /* add storage for a sparse write or the frag page */
                    error = fs_write_add_stg(bfap, page, 
                                             pageCnt * ADVFS_PGSZ, fsize,
                                             1, /* forces page to be zero'd */
                                             cred, 0, NULL);
                    if (error)
                        break;

                    /* tell sync writers to flush the extent map.  fsync
                     * will also flush if we set the dirty_stats below.
                     * having the write lock is the only protection needed.
                     */
                    contextp->dirty_alloc = TRUE;

have_storage:
                    /* we only retry 1 time and bail out if it fails */
                    error = bs_pinpg_get(bfap, page, refHint,
                                         pl, vmp, offset, len, ubc_flags);
                    if (error)
                        break;
                }
                else break;
            }
            if (protp) {
                *protp = 0;
  	        protp++;
            }
        }

        /* move to next output page array location */
        pl++;
        *pl = VM_PAGE_NULL;

        /* adjust parameters used by ubc for their cache prediction.
         * call to us is not always a page multiple, so ensure length
         * never goes 0 or wraps to a very large ulong.
         */
        offset += ADVFS_PGSZ;
        if (len > ADVFS_PGSZ)
            len -= ADVFS_PGSZ;

        vp->v_lastr = page;

    } /* end of for loop */

    /* when called from vm, update thread page fault statistics to
     * increase page fault read count by number of disk reads we did */
    if (protp)
        u.u_tru.tru_majflt += u.u_tru.tru_inblock - prior_read_count;

_error:

    if (trunc_xfer_lock)
        TRUNC_XFER_UNLOCK(&bfap->origAccp->trunc_xfer_lk);

    if (unlock_file_lock)
        FS_FILE_UNLOCK(contextp);

    /* set flag to update stats if we are writing and returning at least
     * one page, because any successfully pin'd pages will get written.
     */
    if (writing && *pl) {
        mutex_lock(&contextp->fsContext_mutex);
        contextp->fs_flag |= MOD_MTIME | MOD_CTIME;
        contextp->dirty_stats = TRUE;
        mutex_unlock(&contextp->fsContext_mutex);
    }

    return(error);
}

/*
 * msfs_fs_cleanup()
 *     UBC operation callback routine will free the AdvFS-specific
 *     bsBuf structure that the UBC page's filesystem private field 
 *     points at. This callback must only free memory and locks.
 *     No dependencies are permissible.
 */

int
msfs_fs_cleanup(vm_offset_t opfs) /*In:Copy of vm_page's fs private field.*/
{
     struct bsBuf *bp;

     /* If the UBC page contains a bsBuf, then free the bsBuf memory. */
     bp = (struct bsBuf *) opfs;

     if (bp)
     {
         bs_free_bsbuf(bp);
     }
     return(EOK);
}


/* 
 * msfs_fs_replicate()
 *     UBC callback routine to dynamically replicate a bsBuf
 *     whenever UBC is replicating a UBC cache page. UBC replicates
 *     copies of the cache data and header structures in a NUMA
 *     system when a RAD makes a read reference to an existing
 *     cache entry that resides on a different remote RAD.
 *     
 *     Replication happens only on reads. UBC will purge replicated
 *     copies whenever a subsequent modify write cache request
 *     is made. AdvFS will purge replicated bsBuf copies when
 *     UBC calls back to the msfs_fs_cleanup() routine.
 * 
 *     If memory for the bsBuf cannot be obtained without waiting then
 *     a -1 is returned.
 */

int
msfs_fs_replicate(vm_page_t pp) /*In:Copy of vm_page's fs private field.*/
{
    struct bsBuf *bp,*origbp;
    int i;

    origbp = (struct bsBuf *)pp->pg_opfs;

    /* Initialize a new buffer descriptor. */
    bp = bs_get_bsbuf(PAGE_TO_MID(pp), FALSE);
    if (!bp) 
        return(-1);

    bp->ioList = origbp->ioList;

    /* TO DO:  decide if we really need this, or should we always just
     * set up for the 1 ioDesc that is inside the bsBuf.  we really
     * should not need to malloc for the multiple destination case
     * since that only applies to migrate and that should not be here.
     */
    if (bp->ioList.maxCnt > 1) {
        if ((bp->ioList.ioDesc = (ioDescT *)ms_rad_malloc_no_wait(
                                    bp->ioList.maxCnt * sizeof(ioDescT),
                                    M_PREFER, PAGE_TO_MID(pp))) == NULL) {
            bp->ioList.maxCnt = 0; /* can't free what we don't have */
            bs_free_bsbuf(bp);
            return(-1);
        }
        for (i = 0; i < bp->ioList.maxCnt; i++) {
            /* IODESC_CLR(bp, i); cleared by ms_rad_malloc_no_wait */
            bp->ioList.ioDesc[i].bsBuf = bp;
            bp->ioList.ioDesc[i].blkDesc = origbp->ioList.ioDesc[i].blkDesc;
            bp->ioList.ioDesc[i].numBlks = origbp->ioList.ioDesc[i].numBlks;
        }
    }
    else {
        bp->ioList.ioDesc = &bp->ioDesc;
    }
    /* IODESC_CLR(bp, 0); cleared by bs_get_bsbuf */
    bp->ioDesc.bsBuf = bp;
    bp->ioDesc.blkDesc = origbp->ioDesc.blkDesc;
    bp->ioDesc.numBlks = origbp->ioDesc.numBlks;

    /* setting debug flags on original bsBuf without a lock can cause
     * word-tear since there is nobody stopping other threads, but
     * the original should be read-only and unlikely to be undergoing
     * a transition that would affect this field */
    origbp->bufDebug |= BSBUF_REPLICATED;
    bp->bufDebug |= BSBUF_REPLICA;

    bp->ln = SET_LINE_AND_THREAD(__LINE__);
    bp->lock.state = origbp->lock.state;
    MS_SMP_ASSERT(!(bp->lock.state&IO_TRANS));

    bp->bfPgNum = origbp->bfPgNum;
    bp->bfAccess = origbp->bfAccess;
    bp->vmpage = pp;
    pp->pg_opfs = (unsigned long) bp;

    return(EOK);
}

/*
 * Stub routine. This is a ubcops dispatch table
 * entry holder only.
 */

int
msfs_write_check(vm_ubc_object_t vop, vm_page_t pp)
{
    return(1);
}

/*
 * copy_frag_into_page
 *
 * This function copies part or all of the specified frag to the specified
 * location.    The value of "copyByteOffset" is relative to the start
 * of the frag.  The frag offset is relative 0.
 *
 * The caller must acquire (exclusive) the file lock in the context area before
 * calling this function.
 */

static
statusT
copy_frag_into_page (
                     bfAccessT *fragBfAccessp,  /* in */
                     bfFragIdT fragId,  /* in */
                     uint32T copyByteOffset,  /* in */
                     uint32T copyByteCnt,  /* in */
                     uint32T bytesPerPage,  /* in */
                     char *bufAddr  /* in */
                     )
{
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
     * NOTE:  This solution assumes non-sparse files.  The solution handles
     * sparse files but it may not be optimal.
     */

    fragByteCnt = fragId.type * 1024;

    /*
     * Verify that the byte range ends in the frag.
     */
    if ((copyByteOffset + copyByteCnt) > fragByteCnt) {
        ADVFS_SAD0 ("copy_frag_into_page: invalid byte range");
    }

    subFrag1ByteOffset = FRAG2SLOT (fragId.frag) * 1024;
    subFrag1ByteCnt = bytesPerPage - subFrag1ByteOffset;
    if (subFrag1ByteCnt > fragByteCnt) {
        subFrag1ByteCnt = fragByteCnt;
    }

    if (copyByteOffset < subFrag1ByteCnt) {
        /*
         * The requested range starts in the first sub frag.
         */
        sts = bs_refpg( &subFrag1PgRef,
                        (void*)&subFrag1Page,
                        fragBfAccessp,
                        FRAG2PG (fragId.frag),
                        BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        subFrag1Page = subFrag1Page + subFrag1ByteOffset + copyByteOffset;
        subFrag1ByteCnt = subFrag1ByteCnt - copyByteOffset;

        if (copyByteCnt <= subFrag1ByteCnt) {
            /*
             * The requested range ends in the first sub frag.
             */
            bcopy (subFrag1Page, bufAddr, copyByteCnt);
        } else {
            /*
             * The requested range ends in the second sub frag.
             */
            sts = bs_refpg( &subFrag2PgRef,
                            (void*)&subFrag2Page,
                            fragBfAccessp,
                            FRAG2PG (fragId.frag) + 1,
                            BS_NIL);
            if (sts != EOK) {
                RAISE_EXCEPTION (sts);
            }

            bcopy (subFrag1Page, bufAddr, subFrag1ByteCnt);

            bcopy( subFrag2Page,
                   bufAddr + subFrag1ByteCnt,
                   copyByteCnt - subFrag1ByteCnt);

            subFrag2Page = NULL;
            bs_derefpg(subFrag2PgRef, BS_CACHE_IT);
        }

        subFrag1Page = NULL;
        bs_derefpg(subFrag1PgRef, BS_CACHE_IT);
    } else {
        /*
         * The requested range starts and ends in the second sub frag.
         */
        sts = bs_refpg( &subFrag2PgRef,
                        (void*)&subFrag2Page,
                        fragBfAccessp,
                        FRAG2PG (fragId.frag) + 1,
                        BS_NIL);
        if (sts != EOK) {
            RAISE_EXCEPTION (sts);
        }
        subFrag2Page = subFrag2Page + (copyByteOffset - subFrag1ByteCnt);

        bcopy (subFrag2Page, bufAddr, copyByteCnt);

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

}  /* end copy_frag_into_page */


/*
 * msfs_mmap - enable mmapping for a file.  
 *
 * ***********************  NOTE  *********************************
 * cfs_mmap() does *not* call msfs_mmap().
 * cfs_mmap() currently calls msfs_set_mmap() and msfs_set_mmap()
 * to set vnode VMMAPPED state only.  
 * ****************************************************************
 */

msfs_mmap( register struct vnode *vp,
    vm_offset_t offset,
    vm_map_t map,
    vm_offset_t *addrp,
    vm_size_t len,
    vm_prot_t prot,
    vm_prot_t maxprot,
    int flags,
    struct ucred *cred )
{
    struct vp_mmap_args args;
    register struct vp_mmap_args *ap = &args;
    int ret;
    extern kern_return_t u_vp_create();
    struct fsContext *contextp;
    bfAccessT *bfap;
    int locked;
    statusT sts;

    FILESETSTAT( vp, msfs_mmap );

    contextp = VTOC(vp);
    bfap = VTOA(vp);
    MS_SMP_ASSERT(contextp != NULL);
    MS_SMP_ASSERT(bfap != NULL);

restart:
    FS_FILE_WRITE_LOCK(contextp);
    locked = TRUE;

    /*
     * Do not allow a file which uses atomic write data logging to be
     * memory-mapped.  Serialize with data logging activation using the
     * file_lock.
     */

    if (bfap->dataSafety == BFD_FTX_AGENT) {
        ret = ENOTSUP;
    }
    else {

        /*
         * If the file is using temporary data logging (mount -o adl),
         * flush all dirty buffers for the file to fulfill the data
         * logging aspect of previously completed writes.   Then turn off
         * temporary data logging.  Finally, allow the mmap() to continue.
         * This is all done while holding the file lock so callers to
         * bs_set_bf_params() who want to change the dataSafety for the
         * file and callers to fs_write() who want to write to the file
         * are locked out.
         */
        if (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY) {
            sts = bfflush(bfap, 0, 0, FLUSH_INTERMEDIATE);
            if (sts != EOK) {
                ret = BSERRMAP(sts);
                RAISE_EXCEPTION(ret);
            }
            bfap->dataSafety = BFD_NIL;
        }

        /* Increment the mmapCnt under the bfaLock. u_vp_create() below will 
         * increment the mmapCnt also, but no matter. We'll decrement our 
         * increment on mmapCnt after the call to u_vp_create().
         */
        mutex_lock( &bfap->bfaLock );
        bfap->mmapCnt++;
        mutex_unlock( &bfap->bfaLock );

        if (!(vp->v_flag & VMMAPPED)) {
            /*
             * Setting or clearing of VMMAPPED must be done under 
             * protection of the file lock.
             * 
             * Also, the locking in fs_read() and fs_write() depends on 
             * VMMAPPED being turned on while the file lock is held 
             * and not being turned off until all current references 
             * to the file are gone.  If VMMAPPED was turned off 
             * while still in use, it would no longer be safe to 
             * check VMMAPPED without the file lock and take the 
             * vm_map lock.  Deadlocks could result if VMMAPPED is 
             * changed, causing the decision to lock the vm_map lock 
             * first to be the opposite order as in use by a 
             * concurrently executing thread.
             */
            if ( !(vp->v_flag & VDIRECTIO) ) {
                /* DirectIO is not already in effect, just throw the bit.
                 * It won't be modified with the file_lock held by us.
                 */
                VN_LOCK(vp);
                vp->v_flag |= VMMAPPED;
                VN_UNLOCK(vp);
            } else {
                /* Do not allow mmap() to turn off directIO. */
                FS_FILE_UNLOCK(contextp);
                locked = FALSE;
                mutex_lock( &bfap->bfaLock );
                bfap->mmapCnt--;
                mutex_unlock( &bfap->bfaLock );
                ret = ENOTSUP;
                goto _exit;
            }
        }

        ap->a_offset = offset;
        ap->a_vaddr = addrp;
        ap->a_size = len;
        ap->a_prot = prot,
        ap->a_maxprot = maxprot;
        ap->a_flags = flags;

        /* drop the file lock before calling u_vp_create() to avoid a
         * deadlock with a faulting racer
         */
        FS_FILE_UNLOCK(contextp);
        locked = FALSE;

        ret = u_vp_create( map, vp->v_object, (vm_offset_t) ap );

        /*
         * Relock the file lock to protect bfap->dataSafety.
         */
        FS_FILE_WRITE_LOCK(contextp);
        locked = TRUE;
        mutex_lock( &bfap->bfaLock );
        bfap->mmapCnt--;

        /* If we still have this file mmapped (no error), set the
         * field indicating that msfs_sync_mmap() should flush
         * dirty pages for this file every smsync_age seconds.
         */
        if (bfap->mmapCnt) {
            bfap->mmapFlush = 1;
        } else if ((vp->v_mount->m_flag & M_ADL) &&
                   (bfap->dataSafety == BFD_NIL)) {
            /*
             * If the mmapCnt is now zero (only on an error from
             * u_vp_create()) and the mount point is using temporary
             * data logging (mount -o adl), and the file is using normal
             * asynchronous I/O, then reactivate temporary data logging
             * on this file.  It was turned off up above.
             */
            bfap->dataSafety = BFD_FTX_AGENT_TEMPORARY;
        }
        mutex_unlock( &bfap->bfaLock );
    }

HANDLE_EXCEPTION:

    if (locked)
        FS_FILE_UNLOCK(contextp);

    /*
     * Mark the file access time for update to comply with
     * XPG4 Issue 5 and ISO/IEC 9945-1 (ANSI/IEE Std. 1003.1)
     * We mark for update here as the standard only requires 
     * the ATIME be updated on the first fault on the region, 
     * and we don't want to put it in getpage because we 
     * don't want to mark ATIME multiple times which would cause 
     * writes of the stats on each sync() call hurting performance.
     */
    if (ret == 0) {
        mutex_lock( &contextp->fsContext_mutex );
        contextp->fs_flag |= MOD_ATIME;
        if ( !(vp->v_mount->m_flag & (M_NOATIMES | M_RDONLY)) )
            contextp->dirty_stats = TRUE;
        mutex_unlock( &contextp->fsContext_mutex );
    }

_exit:

    return( ret );
}

/*
 * msfs_set_mmap()
 *
 * Called in cfs_mmap() and cfs_cachectl() (i.e. failover).
 * This ensures that if a file is mmap'ed in a cluster, its
 * vnode will properly reflect its mmap'ed status.
 *
 * In a cluster, the VM map lock is always taken before
 * calling PFS read/write, so we actually do not need to
 * take the file lock to set VMMAPPED the way we do in the
 * base.  But let's take it anyway to be consistent with
 * base coding.
 */

msfs_set_mmap(struct vnode *vp)
{
    struct fsContext *contextp;

    contextp = VTOC(vp);
    FS_FILE_WRITE_LOCK(contextp);
    VN_LOCK(vp);
    vp->v_flag |= VMMAPPED;
    VN_UNLOCK(vp);
    FS_FILE_UNLOCK(contextp);
}

/*
 * msfs_unset_mmap()
 *
 * Called in cfs_inactive().  After cnode inactivation, the cnode 
 * goes onto the free list, keeping a reference on the PFS vnode.
 * Only when the cnode is recycled is the PFS vnode inactivated.
 * 
 * Usually VMMAPPED is cleared only at PFS inactivation time
 * (see bs_close_one()), but we allow cfs_inactive() to clear it
 * because PFS vnode inactivation/cnode recycle may not occur for
 * days after cnode inactivate.  During this time, if the file is
 * no longer mmap'ed, we want it to be free for other modes (e.g.
 * direct I/O).  
 *
 * It is also safe to unset VMMAPPED at cfs_inactive() time and
 * not run into deadlocks (see bs_close_one() comments) because
 * the only reference on this file at this point is CFS holding 
 * onto the PFS vnode.
 */

msfs_unset_mmap(struct vnode *vp)
{
    VN_LOCK(vp);	
    vp->v_flag &= ~VMMAPPED;
    VN_UNLOCK(vp);
}


msfs_bread(register struct vnode *vp,
           off_t lbn,
           struct buf **bpp,
           struct ucred *cred)
{
    vm_page_t pl[VP_PAGELIST+1];
    register vm_page_t *pp, lp, fp;
    register struct buf *bp;
    int error;
    struct fsContext *cp;
    struct mount *mp;
    struct nstatfs *sbp;
    struct fileSetNode *dn = GETFILESETNODE( VTOMOUNT( vp ) );

    dn->fileSetStats.msfs_bread++;

    mp = VTOMOUNT( vp );
    dn = GETFILESETNODE( mp );
    sbp = &mp->m_stat;
    cp = VTOC( vp );


    /*
     * Hack Alert:
     * If we are here through the NFS server, bpp would be
     * NULL, in which case we just set the ATIME flag and
     * return. The corresponding pages would already be read
     * through FSOP_GETPAGE call by the NFS thread.
     */

    if (!bpp) {
        mutex_lock(&cp->fsContext_mutex);
        cp->fs_flag |= MOD_ATIME;
        if ( !(vp->v_mount->m_flag & M_NOATIMES) )
            cp->dirty_stats = TRUE;
        mutex_unlock(&cp->fsContext_mutex);
        return 0;
    }


    /*
     * If this read will go past end of file, return error.
     */
    if ((lbn * sbp->f_bsize) + sbp->f_bsize > (VTOA(vp))->file_size) {
        return( EOPNOTSUPP );
    }

    /* Update status bits */
    mutex_lock(&cp->fsContext_mutex);
    cp->fs_flag |= MOD_ATIME;
    if ( !(vp->v_mount->m_flag & M_NOATIMES) )
        cp->dirty_stats = TRUE;
    mutex_unlock(&cp->fsContext_mutex);

    error = msfs_getpage( vp->v_object,
                          lbn * sbp->f_bsize,
                          sbp->f_bsize,
                          NULL,
                          pl,
                          atop(round_page(sbp->f_bsize)),
                          NULL,
                          B_READ,
                          cred);

    if (error) {
        return (error);
    }

    bp = ubc_bufget();
    bp->b_flags = B_READ | B_DONE;
    bp->b_bcount = 0;
    bp->b_resid = 0;

    /*
     * Link pages together
     */
    for (lp = 0, pp = pl; (*pp != VM_PAGE_NULL); pp++) {
        ubc_page_wait( *pp );

        if (lp) {
            lp->pg_pnext = *pp;
        } else {
            fp = *pp;
        }

        lp = *pp;
        bp->b_bcount += PAGE_SIZE;
    }

    lp->pg_pnext = VM_PAGE_NULL;
    fp->pg_pprev = lp;

    /*
     * Call bp_mapin() to allocate virtually contiguous
     * space for buffer to point at.
     */

    bp->b_pagelist = fp;
    bp_mapin( bp );
    *bpp = bp;

    return (0);
}


msfs_brelse(register struct vnode *vp,
            register struct buf *bp)
{
    register vm_page_t pp, cp;

    FILESETSTAT( vp, msfs_brelse );


    pp = bp->b_pagelist;
    ubc_buffree( bp );

    do {
        cp = pp;
        pp = pp->pg_pnext;
        ubc_page_release( cp, B_READ );
    } while (pp != VM_PAGE_NULL);

    return (0);
}

/* msfs_log_and_meta_flush()
 * REVISION 1
 * Flush a domain's log and dirty metadata using the given UBC file object.
 * This gives UBC the ability to flush dirty UBC pages in order
 * to make them eligible for recycling because of low memory
 * conditions.
 * 
 * This routine skips over pinned metadata pages and will not flush them.
 *  
 * Note: This routine will flush the log and the metadata dirty pages
 *       on an advisory not mandatory basis in order to make UBC pages
 *       clean and available for recycling.
 *       In the future, use the pgs_to_flush hint as the number of
 *       metadata pages to flush. 
 *
 * REVISION 2
 * This routine has been changed to flush only the passed in object.
 * This allows for pages that are being migrated (this is generally going
 * to be the frag file) for a object marked NOFLUSH. Since these pages are
 * not under transaction control a log and metadata flush is not enough.
 * We must also flush the object itself.
 *
 * Originally this routine flushed all metadata. Now it will work on a 
 * per object basis. VM may ended up calling in multiple times in order to
 * flush all the metadata but eventually the job will get done. This policy
 * may need to change.
 */


msfs_log_and_meta_flush(vm_ubc_object_t vop,
			int pgs_to_flush)
{
    bfAccessT *bfap;

    bfap = vop->vu_ap;
    MS_SMP_ASSERT(bfap);
    
/* Revision 1*/
    /* Flush the domain's log to disk before flushing metadata.
     * Flush modified metadata pages up to the domain log's
     * highest LSN flushed. This skips flushing pinned metadata pages.
     */
/*
    lgr_flush(bfap->dmnP->ftxLogP);
    bs_lsnList_flush(bfap->dmnP);    
*/

/* Revision 2 */

    /* Flush the passed in object. This will catch case were the
     * metadata file was migrated and the smoothsync queues are full
     * of noflush buffers. These will not be on the domains LSN list
     * so the above flush wouldn't find them.  
     */
    /* 
     * If the object happens to be the log itself we can not use bfflush.
     * Use the special lgr_flush code.
     */

    if (bfap->dataSafety == BFD_NO_NWR)
    {
        /* A new log may be in the process of being setup
         * (switchlog or mkfdmn) lets just be paranoid and
         * not deref NULL.
         */

        if (bfap->dmnP->ftxLogP)
            lgr_flush(bfap->dmnP->ftxLogP);
    }
    else
    {
        bfflush(bfap,0,0,FLUSH_UBC);
    }

}


#ifdef __GNUC__
/*
 * GCC can insert calls to memset into the assembly language
 * output during character array initialization.
 */
void memset(
    void *s,
    int c,
    size_t n
    )
{
    char *p;
    int i;

    p = (char *)s;
    i = n;

    while (i-- > 0) {
        *p = c;
        p++;
    }
}
#endif __GNUCC__
