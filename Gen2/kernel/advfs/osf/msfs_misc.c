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

#define ADVFS_MODULE MSFS_MISC

#include <sys/param.h>
#include <sys/user.h>
#include <sys/file.h>
#include <sys/vnode.h>                /* for struct vnode */
#include <sys/buf.h>
#include <sys/dnlc.h>
#include <sys/pathname.h>
#include <machine/sys/clock.h>
#include <sys/spinlock.h>             /* for lock_t */
#include <sys/mount.h>
#include <sys/resource.h>
#include <sys/proc_iface.h>           /* for p_rlimit() */
#include <fs/vfs_ifaces.h>            /* struct nameidata,  NAMEIDATA macro */
#include <h/proc_private.h>           /* proc->p_shared for IS_XSIG */
#include <sys/signal.h>               /* for SIGXFSZ macro */
#include <sys/privgrp.h>              /* PRIV_MLOCK */
#include <sys/pregion.h>
#include <sys/proc_id.h>              /* proc_pid_self() for proc pid */
#include <tcr/clu.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <ms_osf.h>
#include <fs_dir.h>
#include <fs_dir_routines.h>
#include <bs_public.h>
#include <bs_extents.h>
#include <advfs/advfs_syscalls.h>
#include <aclv.h>
#include <advfs_acl.h>
#include <ms_assert.h>
#include <bs_index.h>
#include <bs_params.h>
#include <sys/fs/advfs_ioctl.h>
#include <bs_snapshot.h>
#include <ftx_privates.h>
#include <vfast.h>

#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

extern struct vnodeops advfs_vnodeops;

statusT
advfs_getmetapage(
    fcache_vminfo_t *fc_vminfo,      
    struct vnode *vp,   	     
    off_t off,               	     
    struct advfs_pvt_param *fs_priv_param,  
    ioanchor_t **ioAnchor_head, 
    fcache_pflags_t pflags,
    extent_blk_desc_t*  snap_maps,   
    ftxHT       parent_ftx);

statusT
advfs_start_blkmap_io(
    fcache_vminfo_t *fc_vminfo,
    struct vnode * vp,         
    off_t     offset,          
    size_t     length,         
    extent_blk_desc_t * primary_blkmap, 
    extent_blk_desc_t ** passed_secondary_blkmap,
    ioanchor_t **ioAnchor_head,       
    page_fsdata_t  ** plist,
    struct bsBuf ** bsBufList,
    fcache_pflags_t pflags,
    int32_t         io_flags              
    );

size_t
advfs_process_bsbufs_for_freeing (
    fcache_vminfo_t *fc_vminfo,
    struct vnode *vp, 
    off_t scan_offset, 
    size_t *scan_size, 
    uint32_t unaligned,
    page_fsdata_t **plist, 
    fcache_pflags_t pflags );

statusT
advfs_adjust_bsbuf_for_domain_panic(
    struct vnode *vp, 
    off_t scan_offset, 
    size_t *scan_size,
    fps_status_t *scan_status);

fps_status_t
parse_meta_list(
    size_t *scan_size,
    page_fsdata_t *plist);

size_t
parse_meta_page(fps_status_t *state, page_fsdata_t **plistp);

int
advfs_bs_zero_fill_pages(
    struct vnode *vp,            /* in - File's vnode structure */
    off_t zero_fill_offset,      /* in - Offset of first byte to zero-fill */
    size_t zero_fill_length,     /* in - Number of bytes to zero-fill */
    int protect,                 /* in - Set write protection on each page */
    page_fsdata_t **plistp,      /* in - Pages from fcache_page_alloc() */
    fcache_vminfo_t *fc_vminfo,  /* in - Pass to fcache_page_alloc(), etc. */
    fcache_ftype_t intent,       /* in - specifies read or write */
    struct advfs_pvt_param *fs_priv_param, /* in - private params for putpage.*/
    int flush                    /* in - flush cache due to BFS_OD_OBJ_SAFETY */
);

#ifdef ADVFS_SMP_ASSERT
int
advfs_verify_unprot_pages(
    struct vnode *vp,		 /* in - File's vnode structure */
    off_t offset,	 /* in - Offset of first byte to zero-fill */
    size_t size,	 /* in - Number of bytes to zero-fill */
    fcache_vminfo_t *fc_vminfo	 /* in - Pass to fcache_page_alloc(), etc. */
);
#endif

int
advfs_getpage(
    fcache_vminfo_t *fc_vminfo,   /* An opaque pointer to a vm data struct*/
    struct vnode * vp,           /* The vnode pointer */
    off_t *off,                   /* The offset in the file of the fault */
    size_t *size,                 /* The size in bytes to fault in */
    fcache_ftype_t ftype,            /* The fault type */
    struct advfs_pvt_param *fs_priv_param, /* File system private parameter */
    fcache_pflags_t pflags       /* Options or modifiers to the function */
);

/*
 * advfs_setext_prealloc()
 *
 * This function is modelled off the VOP advfs_prealloc.
 * However, in order to get preallocation to work without updating the
 * bfAccess structure's file_size, the VOP would need to be overloaded
 * in some manner (or the interface changed).
 */
int
advfs_setext_prealloc( struct vnode *vp, advfs_ext_prealloc_attr_t *prealloc )
{
    bfAccessT * bfap;
    domainT * dmnp;
    struct fsContext *contextp;
    struct advfs_pvt_param priv_param;
    fcache_map_dsc_t *fcmap;        /* UFC mapping for file */
    faf_status_t faultStatus = 0;
    ni_nameiop_t save_orig_nameiop;
    struct nameidata *ndp;
    ftxHT ftxH = { 0 };
    void *delList;
    uint32_t delCnt = 0;
    size_t blks_needed;
    statusT error = 0;
    statusT error2 = 0;
    off_t offset = 0;
    size_t bytes = 0;
    int nozero = 0;             /* do not zero allocated space */
    int reserve_only = 0;       /* do not update the file_size */
    struct vnode *saved_vnode;
    uint64_t rlimit_fsize = 0;
    uint64_t file_size_limit = ADVFS_MAX_OFFSET+1;

    bfap = VTOA(vp);
    dmnp = bfap->dmnP;
    contextp = VTOC(vp);

    /* first thing to do before continuing is to verify we own the file
     * to modify (or are root) */
    if (((kt_cred(u.u_kthreadp))->cr_uid != 0) && 
        ((kt_cred(u.u_kthreadp))->cr_uid != bfap->bfFsContext.dir_stats.st_uid)) {
        return (EPERM);
    }

    /*
     * Determine the maximum file size allowed by current ulimit() settings.
     */
    rlimit_fsize = p_rlimit(u.u_procp)[RLIMIT_FSIZE].rlim_cur;

    /*
     * The maximum file size this thread can write is the minimum of the
     * ulimit() setting for file size and the maximum offset that AdvFS
     * currently supports.
     */
    file_size_limit = MIN(rlimit_fsize, file_size_limit );

    /* For the moment, we preallocate from offset 0 all the time, so
     * we only need to see if the bytes requested exceed the max file
     * size */
    if (prealloc->bytes >= file_size_limit) {
        /*
         * Send the process a SIGXFSZ signal if we need to do so.
         */
        if ((prealloc->bytes > rlimit_fsize) &&
            (IS_XSIG(u.u_procp) || IS_SIGAWARE(u.u_kthreadp, SIGXFSZ))) {
                psignal(u.u_procp, SIGXFSZ);
        }
        return (EFBIG);
    }

    /*
     * Check for the NOZERO flag, and verify that -- if set -- that the caller
     * has root credentials, or has PRIV_MLOCK privileges
     */
    if (prealloc->flags & ADVFS_EXT_PREALLOC_NOZERO) {
        if( !( in_privgrp( PRIV_MLOCK, (struct ucred *)0L ) ) &&
	    ( kt_cred( u.u_kthreadp ) )->cr_uid != 0 ) {
            return (EPERM);
        }
        nozero = 1;
    }

    /* determine if we are reserving space only, or if we are going to update 
     * the file_size
     */
    if (prealloc->flags & ADVFS_EXT_PREALLOC_RESERVE_ONLY) {
        reserve_only = 1;
    }

    /* Calculate the number of DEV_BSIZE disk blocks the caller wants to
     * preallocate.
     */
    blks_needed = howmany(prealloc->bytes, DEV_BSIZE);

    /* If howmany() told us zero blocks were needed but prealloc->bytes != 0,
     * then we probably have an overflow of the uint64_t.  Return EINVAL.
     */
    if (prealloc->bytes != 0 && blks_needed == 0) {
        return (EINVAL);
    }

    /* Verify the domain has enough free DEV_BSIZE disk blocks for the request.
     * This is only a preliminary check as the domain's freeBlks may
     * change while performing the storage allocation.
     */
    if (dmnp->freeBlks < blks_needed) {
        return (ENOSPC);
    }

    /* Advfs_getpage() via the fault requires the NI_RW flag be
     * set to determine caller is not mmapping and to allow file extentions.
     * Save the original nameidata value to restore later.
     */
    ndp = NAMEIDATA();
    MS_SMP_ASSERT(ndp);
    save_orig_nameiop = ndp->ni_nameiop;
    ndp->ni_nameiop = NI_RW;
    saved_vnode = ndp->ni_vp;
    ndp->ni_vp = vp;

    /* initial values for getpage loop variables */
    offset = 0;
    bytes = prealloc->bytes;
    
    /* Pass in the private parameter pointer so the advfs_getpage()
     * doesn't think this is a mmap file.
     */
    bzero((char *)&priv_param, sizeof(struct advfs_pvt_param));
    priv_param.app_starting_offset = offset;
    priv_param.app_total_bytes = bytes;
    priv_param.app_flags = APP_ADDSTG_NOCACHE;

    ADVRWL_FILECONTEXT_WRITE_LOCK( contextp );

    /* If prealloc->bytes is 0, then set the rsvd file size to 0 both
     * in-memory and on-disk in the BSR_ATTR record as this will "turn off"
     * any user reserved preallocated space.  We can accomplish this by
     * setting the two flags in the if block below. */
    if (prealloc->bytes == 0) {
        /* when the file is closed it will be truncated */
        reserve_only = 1; /* follow the reserve_only code path below */
        nozero = 1;  /* no need to zero anything, we are removing rsvd space */
    }

    /* if the file_size or rsvd_file_size is not 0, we return EINVAL as we 
     * can only preallocate for zero-length files.  The exception being if 
     * the bytes to preallocate is 0, which will remove any reserved space */
    else if (bfap->file_size != 0 || bfap->rsvd_file_size != 0) {
        error = EINVAL;
        goto error;
    }

    /* call getpage to add storage.  This call to getpage bypasses
     * the UFC since we have no need to cache the pages we allocate.
     * We make this call in a loop since advfs_getpage() can only allocate
     * 2MB at a time due to log restrictions */
    while( prealloc->bytes > 
           ( (priv_param.stg_desc.app_stg_end_fob + 1) * ADVFS_FOB_SZ) ) {
        error = advfs_getpage( NULL, 
                               &bfap->bfVnode, 
                               (off_t *)&offset,
                               (size_t *)&bytes, 
                               FCF_DFLT_WRITE,
                               &priv_param, 
                               0 );
        if (error != EOK) {
            goto error;
        }
    }

    if (!nozero) {
        char *zeroed_memp = NULL;
        uint64_t extent_count = 0;
        uint32_t last_iosize = 0;
        off_t starting_fob = 0; /* start at beginning of file */
        struct vd *vdp = NULL;
        extent_blk_desc_t *fob_range = NULL, *fr = NULL;

        /* if nozero was NOT specified, we use raw_io to zero the storage.
         * First we need the migStgLk. */
        ADVRWL_MIGSTG_WRITE( bfap ); 

        /* get a list of real extents to zero */
        error = advfs_get_blkmap_in_range(bfap,
                                          bfap->xtnts.xtntMap,
                                          &priv_param.app_starting_offset,
                                          (priv_param.stg_desc.app_stg_end_fob + 1)
                                              * ADVFS_FOB_SZ,
                                          &fob_range,
                                          &extent_count,
                                          RND_NONE,
                                          EXB_ONLY_STG, /* storage only */
                                          XTNT_NO_WAIT);

        if (error != EOK || fob_range==NULL) {
            ADVRWL_MIGSTG_UNLOCK( bfap );
            goto error;
        }

        /* for each extent, call advfs_raw_io to zero it */
        for(fr = fob_range; fr != NULL; fr = fr->ebd_next_desc) {
            bf_vd_blk_t blocks_written = 0;
            bf_vd_blk_t starting_block = fr->ebd_vd_blk;
            bf_vd_blk_t blocks_to_write = fr->ebd_byte_cnt / ADVFS_FOB_SZ;
            vdp = VD_HTOP( fr->ebd_vd_index, bfap->dmnP );

            /* we should use the volume's preferred I/O size since we
             * know it, but we only malloc if the zeroed buffer is a
             * different size than the preferred_iosize -- this avoids
             * needless malloc's */
            if(last_iosize != vdp->preferred_iosize) {
                if(zeroed_memp != NULL) {
                    ms_free(zeroed_memp);
                }
                zeroed_memp = ms_malloc((size_t) vdp->preferred_iosize);
                last_iosize = vdp->preferred_iosize;
            }

            /* we loop and write some fixed amount of zeros each time --
             * this prevents a possible HUGE malloc.  The price is a 
             * performance hit, but that's okay since someone really
             * concerned with performance would choose not to zero
             * this preallocated space */
            while (blocks_to_write > 0) {
                /* only write a MAX of vdp->preferred_iosize */
                if (blocks_to_write > (vdp->preferred_iosize / ADVFS_FOB_SZ)) {
                    blocks_to_write = vdp->preferred_iosize / ADVFS_FOB_SZ;
                }

                error = advfs_raw_io ( vdp->devVp,
                                       starting_block,
                                       blocks_to_write,
                                       RAW_WRITE,
                                       zeroed_memp );

                if (error != EOK) {
                    ADVRWL_MIGSTG_UNLOCK( bfap );
                    error2 = advfs_free_blkmaps ( &fob_range );
                    goto error;
                }

                /* now update the starting_block and blocks_to_write */
                starting_block += blocks_to_write;
                blocks_written += blocks_to_write;
                blocks_to_write = (fr->ebd_byte_cnt / ADVFS_FOB_SZ)
                                  - blocks_written;
            }
        }

        ADVRWL_MIGSTG_UNLOCK( bfap );

        /* free the zeroed memory used for clearing on-disk storage */
        if(zeroed_memp != NULL) {
            ms_free(zeroed_memp);
        }

        /* free the blkmaps */
        error = advfs_free_blkmaps ( &fob_range );
        if (error) {
            goto error;
        }
    }

    if (reserve_only) {
        bsBfAttrT bfAttr = { 0 };

        /* at this point we have allocated on-disk storage but it is not 
         * persistent.  A call to fs_trunc_test will flag this file for 
         * truncation, which is inappropriate if a user has asked for 
         * reserved preallocated space.  So we update the metadata by 
         * setting the BSR_ATTR record to indicate there is reserved 
         * preallocated space. */

        if ( (error = FTX_START_N( FTA_BS_BMT_PUT_REC_V1,
                                   &ftxH, FtxNilFtxH, bfap->dmnP ) ) != EOK ) {
            goto error;
        }

        if ( (error = bmtr_get_rec_n_lk( bfap,
                                         BSR_ATTR,
                                         (bsBfAttrT *)&bfAttr,
                                         sizeof(bfAttr),
                                         TRUE ) ) ) {
            ftx_fail(ftxH);
            goto error;
        }

        bfAttr.bfat_rsvd_file_size = prealloc->bytes;


        if ( (error = bmtr_put_rec_n_unlk( bfap,
                                           BSR_ATTR,
                                           (bsBfAttrT *)&bfAttr,
                                           sizeof(bfAttr),
                                           ftxH,
                                           TRUE,
                                           0 ) ) ) {
            ftx_fail(ftxH);
            goto error;
        }

        /* now end the transaction */
	ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC);
        ftx_done_n( ftxH, FTA_BS_BMT_PUT_REC_V1 );

        /* now we can update the in-memory rsvd_file_size */
        bfap->rsvd_file_size = prealloc->bytes;
    }
    else {

        /*
	 * the user has asked us to preallocate some space and wants the 
         * file_size updated. So we don't flag the tagdir or the bfap, since 
         * the file won't truncate past the file_size
	 *
         * Update the file size to the requested size
         * only if it is larger than the previous file size.
         * Preallocate functionality always starts the preallocation from the
         * beginning of the file.
	 *
	 * set the stat dirty flag and call fs_update_stats() to update the
	 * file size
         */

	if ( (error = FTX_START_N( FTA_PREALLOC, &ftxH, FtxNilFtxH,
				   bfap->dmnP ) ) != EOK ) {
            goto error;
        }

        if (prealloc->bytes > (uint64_t)bfap->file_size) {
            bfap->file_size = prealloc->bytes;
        }

	ADVSMP_FILESTATS_LOCK( &contextp->fsContext_lock );
	contextp->dirty_stats = TRUE;
	ADVSMP_FILESTATS_UNLOCK( &contextp->fsContext_lock );
	if( error = fs_update_stats( vp, bfap, ftxH, 0 ) ) {
	    ftx_fail( ftxH );
	    goto error;
	}

	/* End Transaction */
        ftx_special_done_mode( ftxH, FTXDONE_LOGSYNC);
        ftx_done_n( ftxH, FTA_PREALLOC );
    }

error:

    /*
     * Encountered an error, force a truncation to release the storage we may
     * have allocated.  bf_setup_truncation() requires the migStgLk to be held
     * and a root transaction is required by stg_remove_stg_start() called by
     * bf_setup_truncation().
     */
    if( error ) {
	error2 = FTX_START_N( FTA_FS_WRITE_TRUNC, &ftxH, FtxNilFtxH,
				bfap->dmnP );
	if( error2 == EOK ) {
	    ADVRWL_MIGSTG_READ( bfap );
	    error2 = bf_setup_truncation( bfap, ftxH, 0L,
					  &delList, &delCnt);
	    ADVRWL_MIGSTG_UNLOCK( bfap );
	    ftx_done_n (ftxH, FTA_FS_WRITE_TRUNC);
	}
    }

    /* unlock the file context lock */
    ADVRWL_FILECONTEXT_UNLOCK(contextp);


    /* Restore the original value */
    ndp->ni_nameiop = save_orig_nameiop;
    ndp->ni_vp = saved_vnode;

    return (error2 ? error2 : error);
}


/*
 * This routine registers the prealloc transaction agent
 */
void
advfs_prealloc_register_ftx_agent()
{
    statusT sts;

    sts = ftx_register_agent( FTA_PREALLOC,
                              NULL,
                              NULL );

    if (sts != EOK) {
        ADVFS_SAD1("advfs_prealloc_register_ftx_agent: register failure", sts );
    }

} /* advfs_prealloc_register_ftx_agent */


/*
 * advfs_setext()
 */
int
advfs_setext( struct vnode *vp, advfs_ext_attr_t *attr )
{
    int retval = ESUCCESS;

    switch(attr->type) {
    case ADVFS_EXT_NOOP:
        break;
    case ADVFS_EXT_PREALLOC:
        if (attr->value.prealloc.bytes >= 0) {
            retval = advfs_setext_prealloc(vp, &attr->value.prealloc);
        }
        break;
    default:
        return(ENOTSUP);
    }

    return(retval);
}

/*
 * advfs_getext_prealloc()
 */
int
advfs_getext_prealloc( struct vnode *vp, advfs_ext_prealloc_attr_t *prealloc )
{
/*    return( bs_get_bf_fob_cnt( VTOA( vp ), &(prealloc->bytes) ) ); */
    return ENOTSUP;
}

/*
 * advfs_getext()
 */
int
advfs_getext( struct vnode *vp,  advfs_ext_attr_t *attr )
{
    int retval = ESUCCESS;

    switch(attr->type) {
    case ADVFS_EXT_NOOP:
        break;
    case ADVFS_EXT_PREALLOC:
        retval = advfs_getext_prealloc( vp, &attr->value.prealloc );
        break;
    default:
        return(ENOTSUP);
    }

    return(retval);
}

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
    struct file **fp)
{
    bfAccessT *bfap = NULL;
    struct file *tfp = NULL;
    struct vnode *vp = NULL;
    int error = 0;

    /*
     ** Using the given file desc handle get the file's 'file struct'.
     */
    tfp = getf(fileDesc);
    if ( tfp == NULL ) {
        return 0;
    }

    /*
     ** Get file's 'vnode struct'.
     */
    vp = (struct vnode *)tfp->f_data;

    if (clu_is_ready()) {
        struct vnode *cvp = vp;
        CLU_GETPFSVP(cvp, &vp, error);
        if (error) {
            error = ENOTSUP;
            goto _error;
        }
    }
    /*
     ** Make sure this is a AdvFS file.
     */
    if (vp->v_fstype != VADVFS) {
        goto _error;
    }

    *fp = tfp;

    /*
     * Use VTOA macro to get the correct bfap.
     */
    bfap = VTOA(vp);

    return bfap;

_error:
    PUTF(fileDesc);
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
    ADVSMP_FILESTATS_INIT(&fscp->fsContext_lock);
    ADVRWL_FILECONTEXT_INIT(&fscp->file_lock);
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
    ADVSMP_FILESTATS_DESTROY(&fscp->fsContext_lock);
    ADVRWL_FILECONTEXT_DESTROY(&fscp->file_lock);
}

/*
 * bf_get
 *
 * Access a bitfile.  Analogous to iget operations in other file systems.
 *
 * Note that the dir_context lock is ALWAYS released in this routine unless
 * the DONT_UNLOCK flag is passed.
 *
 * bf_get open files by tag, and sets up the fsContext area and the
 * vnode for the file if they are not already in existence.
 * The META_OPEN flag means that a metadata bitfile is being opened through the
 * special .tags directory.
 * The META_OPEN flag is set in the bf's fsContext area so that no stat
 * update occurs when the file is closed.
 *
 * Metadata bitfiles are in the root fileset.  To access a
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
 * 
 * Note that only advfs_lookup_int() sets the dvp input. All other callers
 * should set dvp=NULL.
 */
statusT
bf_get(
    bfTagT   bf_tag,               /* in - tag of bitfile to access */
    struct fsContext *dir_context, /* in - parent dir context pointer */
    int32_t flag,                  /* in - flag */
                                   /*    META_OPEN, metadata bf being opened */
                                   /*    DONT_UNLOCK, don't unlock the dir */
                                   /*    IGNORE_DELETE: ignore BSRA_DELETING */
    struct fileSetNode *fsnp,      /* in - fileSetNode structure pointer */
    struct vnode **vp,             /* out - vnode corresponding to the */
                                   /*       accessed bitfile  */
    struct vnode *dvp,             /* in - file_name's directory vp or NULL */
    char *file_name)               /* in - if name != NULL, enter in DNCL */
{
    int i, error;
    size_t file_page_size;
    struct vnode *nvp;
    struct fsContext *file_context;
    bfAccessT *bfap;
    statusT sts=EOK;
    bsUndelDirT undel_rec;
    uint32_t options;
    extern int xlate_dev(dev_t, int);
    bfAccessT *real_bfap = NULL;
    int shadow = FALSE;
    struct vnode *nullvp = NULL;
    bfTagT real_tag;
    struct bfNode *bnp;
    lock_t *sv_lock;
    int32_t unlock_file_lock = FALSE; 

    /*
     *  Use the bitfile set of the mount point even if we are
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

    options = (flag & IGNORE_DELETE) ?  BF_OP_IGNORE_DEL : BF_OP_NO_FLAGS;

    /*
     * Access the bitfile associated with the bf_tag
     *
     * NOTE:  When bs_access() succeeds we must not call
     *        bs_close() (this can be done only in advfs_inactive()).
     *        VN_RELE() is the correct way to close the file.  On successful
     *        return from bs_access, v_count has been bumped.
     */
    if (sts = bs_access( &bfap,
                         bf_tag,
                         fsnp->bfSetp,
                         FtxNilFtxH,
                         options,
                         &nvp ) ) {

        /* Be sure this lock gets released unless requested not to! */
        if (!(flag & DONT_UNLOCK)) {
            ADVRWL_FILECONTEXT_UNLOCK(dir_context);
        }
        return sts;
    }

    ADVFS_ACCESS_LOGIT(bfap, "bf_get got bfap from bs_access");


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
             * advfs_lookup() for reserved (negative) tags and
             * .tags/M<tag> which is a tagdir. META_OPEN is not turned on
             * for regular tags.  The bs_access() above does the open 
             * of regular tags and there is no shadow bfap involved 
             * in those cases.
             */
            if (sts = bs_access( &real_bfap,
				 real_tag,
				 fsnp->dmnP->bfSetDirp,
				 FtxNilFtxH,
				 BF_OP_INTERNAL,
				 &nullvp )) {
                VN_RELE(nvp);
                return sts;
            }
            bfap->real_bfap = real_bfap;
        }
    }

    file_context = VTOC( nvp );

    MS_SMP_ASSERT( file_context != NULL );

    /*
     * Unlock the parent dir in case we need to lock the file for
     * initing below (avoid deadlocks)
     */
    if (!(flag & DONT_UNLOCK)) {
        /*
         * Make the dnlc entry here, if this is a lookup call,
         * before the directory lock is released.
         */
        if ( dvp ) {

            if (file_name != NULL) {
                dnlc_enter(dvp, file_name, nvp, NOCRED);
            }
        }

        ADVRWL_FILECONTEXT_UNLOCK(dir_context);
    }

    /*
     * Call advfs_getacl to set up the ACL cache if any optional
     * ACL entries exist for this file.  If this is a reserved file,
     * or if this is a shadow bfap, or a tags directory then skip the call to
     * advfs_getacl and just set bfaFlags with the BFA_ACL_NOT_ONDISK flag.
     * This will prevent trying to access the uninitialized fsContext_lock lock
     * in advfs_getacl_bmt.
     *
     * Trying to filter out index files at ths point may cause a very tight race
     * condition during the IDX_INDEXING_ENABLED check; therefore, we'll only
     * filter then out in advfs_setacl_int.
     *
     * Also, check if either the BFA_ACL_NOT_ONDISK or the ACL cache is set.
     * If it is, then we can skip the call to advfs_getacl all together since
     * the cache/bfaLock was already set up.
     */
    if( BS_BFTAG_RSVD( bf_tag ) ||
	bfap->real_bfap ||
	BS_BFTAG_EQL( bfap->bfSetp->bfSetId.dirTag, staticRootTagDirTag ) ) {

	if( !( bfap->bfaFlags & BFA_ACL_NOT_ONDISK ) ) {
	    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	    bfap->bfaFlags |= BFA_ACL_NOT_ONDISK;
	    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
	}

    } else {
	if( ( bfap->bfa_acl_cache == NULL ) &&
	    !( bfap->bfaFlags & BFA_ACL_NOT_ONDISK ) ) {

	    error = advfs_getacl( nvp, 0, NULL, ADVFS_ACL_TYPE );
	    if( error != EOK ) {
                sts = error;
                goto HANDLE_EXCEPTION;
	    }
	}
    }



    /*
     * If we need to open the index file, do so now. 
     */
    if ((!(flag & META_OPEN)) &&
        (file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
        if(IDX_MAY_NEED_INDEX_OPEN(bfap)) {
            ADVRWL_FILECONTEXT_WRITE_LOCK( file_context );
            unlock_file_lock = TRUE;
            IDX_OPEN_INDEX(bfap,sts);
            if (sts != EOK) {
                goto HANDLE_EXCEPTION;
            }
            ADVRWL_FILECONTEXT_UNLOCK( file_context );
            unlock_file_lock = FALSE;
        }
    }

    /*
     * We used to initialize the fsContext structure here.  Now that is done
     * in bs_map_bf.  There are still, however, a couple fields to deal with
     * up here.   If the initialized flag is not set, then we need to do
     * some more work.
     */
    if ( file_context->initialized ) {
        /* We're done... */
        return sts;
    } else {
        ADVRWL_FILECONTEXT_WRITE_LOCK( file_context );
        unlock_file_lock = TRUE;
        if ( file_context->initialized ) {
            ADVRWL_FILECONTEXT_UNLOCK( file_context );
            return sts;
        }
    }
        

    /* 
     * The file lock is now held for write
     */

    ADVFS_ACCESS_LOGIT(bfap, "bf_get init'ed fsContext");

    /*
     * if it's a metadata open, set META_OPEN so don't write the stats on
     * close.
     */
    if (flag & META_OPEN) {

        file_context->fs_flag = META_OPEN;
        file_context->dir_stats.st_mode = S_IFREG;
        
        /* 
         * file_size and bfa_orig_file_size should have been set up in
         * bs_map_bf if this file could participate in snapshotting.
         */
        MS_SMP_ASSERT( !HAS_SNAPSHOT_PARENT( bfap ) ||
                        ( (bfap->bfa_orig_file_size != -1) ||
                          ( !IS_COW_CANDIDATE( bfap ) ) ) );


        /*
         * Skip the next couple of if's since a meta open wouldnt' have an
         * index or an undel record.
         */
        goto finish_setup;
    }




    /*
     * if the file is a directory, then read it's undel_dir record
     */
    if ((file_context->dir_stats.st_mode & S_IFMT) == S_IFDIR) {
        sts = bmtr_get_rec( bfap,
                            BMTR_FS_UNDEL_DIR,
                            &undel_rec,
                            sizeof(bsUndelDirT));
        if (sts == EOK) {
            file_context->undel_dir_tag = undel_rec.dir_tag;
        }
        
        IDX_OPEN_INDEX(bfap,sts);
        if (sts != EOK) {
            goto HANDLE_EXCEPTION;
        }
    }

    /*
     * Save the bfap, tag and the fsContext pointer in the vnode private
     * area */

finish_setup:

    /*
     * Initialize the new vnode
     */

    file_context->fsc_fsnp = fsnp;

    for (i = 0; i < MAXQUOTAS; i++) {
        file_context->diskQuot[i] = NULLDQUOT;
    }

    file_context->quotaInitialized = 1;

    nvp->v_nodeid = BS_BFTAG_IDX( bf_tag );

    /* set the vnode type */

    nvp->v_type = TYPTOVT( file_context->dir_stats.st_mode );

    if ( nvp->v_type == VBLK  ||  nvp->v_type == VCHR ) {
        nvp->v_rdev = (dev_t)file_context->dir_stats.st_rdev;
    }

    /*
     * Attach file quotas if the file's vnode was inserted onto the fileset's
     * mount structure.  If the file's bitfile set is the root tag directory,
     * the vnode is inserted onto a mount queue but quotas should not be
     * turned on.  See bs_access_one() in bs_access.c.
     */
    if(!BS_BFTAG_RSVD(bf_tag) && !shadow) {
        /*
         * Attach file quotas.
         */
        (void) advfs_fs_attach_quota(file_context, FtxNilFtxH);
    }

    /*
     * Note that the context area is now completely initialized and
     * unlock it.  Note that this is set here for reasons established by the
     * Tru64 code.  The fsContext area is actually mostly setup in bs_map_bf
     * on HPUX.  This flag indicates the remainder of the setup done in this
     * routine has been completed.
     */
    file_context->initialized = 1;

    ADVRWL_FILECONTEXT_UNLOCK(file_context);

    return EOK;

HANDLE_EXCEPTION:

    if( unlock_file_lock ) {
	ADVRWL_FILECONTEXT_UNLOCK( file_context );
    }

    VN_RELE( nvp );

    return sts;
}


#ifdef ADVFS_SMP_ASSERT
/*
 * Ideally, this global wouldn't exist and we could always check
 * small file sizes in debug mode.  But storage preallocation 
 * and ENOSPC errors make this checking unsafe.  
 *
 * If a file preallocates a meg and then sets preallocation to 
 * zero and then the system crashes, when the file is next used, 
 * both file_size and rsvd_file_size will be 0 but there will be 
 * a meg of storage and the small file check will assert, incorrectly.
 *
 * There is also a problem with these checks in ENOSPC scenarios.
 * Consider the following sequence:
 *
 * 1. Write a 2k file.
 * 2. Write at offset 12k for 8k.
 * 3. This will cause us to backfill from 2k-4k to avoid creating
 *    a hole less than VM_PAGE_SZ bytes.
 * 4. Assume the backfill storage allocation succeeds.
 * 5. We then try to allocate 8k at offset 12k.
 * 6. If THIS allocation fails, getpage will return the error and
 *    tell the caller that it brought nothing in.
 * 7. Then, advfs_fs_write() will leave file size unchanged.
 * 8. We end up with 4k of storage for a 2k file.
 *
 * So for now, turn it off by default.  Perhaps this should be
 * a hidden tunable that tests could turn on when not dealing
 * with prealloc or ENOSPC and turn off when dealing with 
 * prealloc and ENOSPC.
 */
static int Advfs_check_small_file_sizes = 0;  /* Default should be 0 */
#endif

/*
 * advfs_inactive
 *
 * Inactivate a vnode.  Called from vrele when
 * vp->v_count == 1.  Since vnode isn't locked
 * when we are called, v_count could be up again
 * in which case we just return.  
 */
advfs_inactive(
               struct vnode *vp,   /* in - vnode to inactivate */
               struct ucred *ucred
              )
{
    bfAccessT *bfap;
    struct bfNode* bnp;
    bfAccessT *real_bfap = NULL;
    lock_t *sv_lock;
    u_long context;
    lkStatesT prevState;
    enum acc_close_flags options;
    uint32_t last_close = FALSE;

    ADVFS_VOP_LOCKSAFE;

    FILESETSTAT( vp, advfs_inactive );

    /* Always return the access pointer contained in the bfNode.
     * Do not use VTOA() because it will return the real bfap if the
     * access structure off of the bfNode is a shadow (a reserved file 
     * via .tags).
     */
    bfap = ((struct bfNode *)vp->v_data)->accessp;

    /* This may be the final close of the file. We need to close the
     * parents before the file goes into state ACC_INIT_TRANS or there
     * is potential deadlock.   If anyone is doing a
     * racing access, they will just open the parents again.
     *
     * advfs_close_snaps may determine that the parents or children don't
     * actually need to be closed because this will not be the last close
     * (if internal accesses are present)
     */
    if (HAS_SNAPSHOT_PARENT( bfap ) ) {
        (void) advfs_close_snaps( bfap, SF_SNAP_NOFLAGS );
    }

    /*
     * We must take this lock to synchronize with modifications to bfaFlags.
     * If we don't hold the lock, then a change to BFA_INT_OPEN and
     * BFA_EXT_OPEN could clobber eachother.  
     */
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );

    /*
     * Decrement the vnode reference count.  If this really was the last
     * reference on the v_count, then also clear the BFA_EXT_OPEN field from
     * the bfap.  We could have raced with a dnlc_lookup and after
     * decrementing, v_count could still be > 0.  The locks here prevent the
     * dnlc_lookup race, otherwise, we could clear the flag in bs_close.
     */
    MP_H_SPINLOCK_USAV(vn_h_sl_pool,vp,&sv_lock,&context);
    if ( --vp->v_count == 0 ) {
        bfap->bfaFlags &= ~BFA_EXT_OPEN;

        /* Put the access structure in ACC_INIT_TRANS now to insure
         * that we are the last close. We do not want to race here.
         * Any openners will block on this state.
         */

        prevState = lk_get_state (bfap->stateLk);
        MS_SMP_ASSERT(prevState != LKW_NONE);
        MS_SMP_ASSERT(prevState != ACC_VALID_EXCLUSIVE);
        MS_SMP_ASSERT(prevState != ACC_INIT_TRANS);
        MS_SMP_ASSERT(prevState != ACC_CREATING);
        MS_SMP_ASSERT(prevState != ACC_RECYCLE);
        MS_SMP_ASSERT(prevState != ACC_DEALLOC);
        (void) lk_set_state (&(bfap->stateLk), ACC_INIT_TRANS);
        last_close = TRUE;
        
    } else {
        /* 
         * Someone raced us doing an open and didn't put a second refCnt on
         * because BFA_EXT_OPEN was already set in advfs_lookup_valid_bfap.
         * We just need to return or we risk deleting the file in bs_close.
         */
        MP_SPINUNLOCK_USAV(sv_lock,context);
        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
        ADVFS_ACCESS_LOGIT(bfap, "advfs_inactive raced");
	ADVFS_VOP_LOCKUNSAFE;
        return 0;
    }
    MP_SPINUNLOCK_USAV(sv_lock,context);

    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

#ifdef ADVFS_SMP_ASSERT
    /* 
     * If this is a file with a small allocation unit and this is the last
     * close of the file, make sure we didn't leave any holes in it that are 
     * less than VM_PAGE_SZ bytes long.  Also, make sure we didn't 
     * overallocate storage.
     */
    if ((last_close) && (ADVFS_FILE_HAS_SMALL_AU(bfap))) {
        statusT sts;
        off_t offset = 0;
        size_t size = bfap->file_size;
        extent_blk_desc_t *hole_blkmap=NULL,
                          *hole=NULL;
        uint64_t need_storage=0;

        sts = x_lock_inmem_xtnt_map(bfap, XTNT_WAIT_OK|X_LOAD_REFERENCE);
        MS_SMP_ASSERT(sts == EOK);

        if (bfap->bfaNextFob) {
            /* 
             * The file has storage.  Check for holes.
             */
            sts = advfs_get_blkmap_in_range(bfap,
                                            bfap->xtnts.xtntMap,
                                            &offset,
                                            size,
                                            &hole_blkmap,
                                            &need_storage,
                                            RND_ALLOC_UNIT,
                                            EXB_ONLY_HOLES,
                                            XTNT_LOCKS_HELD | XTNT_WAIT_OK);
            
            MS_SMP_ASSERT(sts == EOK);

            for (hole = hole_blkmap; hole; hole = hole->ebd_next_desc) {
                /*
                 * Any holes should be at least VM_PAGE_SZ bytes long 
                 * or, if not, they should be at the end of the file.
                 * If a hole is at the end of a file, it's only legitimate
                 * if the hole starts at an offset >= VM_PAGE_SZ, such
                 * as a file with 12k allocated and a file size of 15000.
                 */
                MS_SMP_ASSERT((hole->ebd_byte_cnt >= VM_PAGE_SZ) ||
                              ((hole->ebd_offset + hole->ebd_byte_cnt >= 
                               (size_t)bfap->file_size) &&
                               (hole->ebd_offset >= (off_t)VM_PAGE_SZ)));
            }

            (void)advfs_free_blkmaps(&hole_blkmap);

            /*
             * If this is a small file, make sure we didn't overallocate.
             * Skip this check if preallocation is enabled on the file.
             */
            if ((Advfs_check_small_file_sizes) &&
                (bfap->file_size <= 
                (int64_t)(VM_PAGE_SZ - ((bfap)->bfPageSz * ADVFS_FOB_SZ))) &&
                (bfap->rsvd_file_size == 0)) {
                MS_SMP_ASSERT(bfap->bfaNextFob == 
                              (roundup(bfap->file_size, 
                                      bfap->bfPageSz * ADVFS_FOB_SZ)) /
                              ADVFS_FOB_SZ);
            }
        }
        ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );
    }
#endif

    /*
     * Check if truncation is possible (sets flag in access struct as its
     * side effect if sensible).  This use to be done in advfs_close, but as
     * seen, we may not go through advfs_close() yet we have
     * some preallocated memory that we should give back.  In all cases we
     * must come here, so the fs_trunc_test() is made here.
     */
    (void)fs_trunc_test(vp, FALSE);

    if (bfap->real_bfap) {
        real_bfap = bfap->real_bfap;
        /* We don't need to pass any flags since this real_bfap was 
         * accessed via an internal open 
         */
        (void) bs_close(real_bfap, MSFS_CLOSE_NONE);
        bfap->real_bfap = NULL;
    }

    /* At this point we need to check if it is a directory being
     * closed and if so, then also close any associated index file.
     */
    if ( IDX_INDEXING_ENABLED(bfap) &&
            IDX_FILE_IS_DIRECTORY(bfap) )
    {
        /* Close the associated index file and deal
         * with any clone processing.
         */
        IDX_CLOSE_INDEX(bfap);
    }

    ADVFS_ACCESS_LOGIT(bfap, "advfs_inactive");

    /*
     * Advise bs_close_one that it is being called from
     * advfs_inactive so it doesn't call vrele.
     */
    if (prevState == ACC_INVALID) {
        options = MSFS_INACTIVE_CALL|MSFS_INVALID;
        ADVFS_ACCESS_LOGIT(bfap, "advfs_inactive previous state invalid");
    } else {
        options = MSFS_INACTIVE_CALL|MSFS_VALID;
    }

    (void)bs_close(bfap, options);

    /*
     * return
     */
    ADVFS_VOP_LOCKUNSAFE;
    return (0);
}

extern msgQHT advfs_metaflush_msg_queue;

#ifdef ADVFS_SMP_ASSERT
#define KEEP_LGPG_STATS
#endif

#ifdef KEEP_LGPG_STATS

typedef struct lgpg_statistics {
    long putpage_lgpg_left;
    long putpage_lgpg_right;
    long putpage_inval_release_lgpg_left;
    long putpage_inval_release_lgpg_right;
    long getpage_read_lgpg_left;
    long getpage_read_lgpg_right;
    long getpage_write_lgpg_left;
    long getpage_write_lgpg_right;
    long getpage_lgpg_alignment_demotion;
    long getpage_lgpg_needs_storage; 
    long getpage_lgpg_locking_demotion;
    long getpage_lgpg_lock_upgrade_ok;
    long getpage_lgpg_lock_upgrade_not_needed;
    long getpage_rewrite_due_to_lgpg_left;
    long getpage_reread_due_to_lgpg_left;
}lgpg_statisticsT;

lgpg_statisticsT lgpg_stats;

#endif

#ifdef  GETPUT_LOGGING
int log_metadata_only = 0;
#pragma align SPINLOCK_ALIGN
spin_t   gp_log_lock = {0}; 
gp_log_entryT gp_log[GP_LOG_SIZE];
int gp_log_idx = -1;

void
gp_logevent(gp_caller_t caller,
            struct vnode *vp,
            off_t offset,
            size_t size,
            fcache_pflags_t pflags,
            fcache_ftype_t ftype,
            fpa_status_t cache_status,
            struct bsBuf * bsbufp,
            gp_action_t action
    )
{
    bfAccessT *bfap=VTOA(vp);
    struct timeval t          = time;
    uint64_t       usecs = (t.tv_sec * 1000000LL) + t.tv_usec;
    if ((log_metadata_only) &&
        (bfap->dataSafety == BFD_USERDATA)) return;
    if (gp_log_idx == -1) {
        ADVSMP_GP_LOG_INIT( &gp_log_lock );
        gp_log_idx = 0;
    }
    spin_lock(&gp_log_lock);
    gp_log[gp_log_idx].usecs = usecs;
    gp_log[gp_log_idx].vp = vp;
    if ((caller != IO_COMPLETION_LOG_PAGE) &&
        (caller != IO_COMPLETION_META_PAGE)) {
        gp_log[gp_log_idx].thread = u.u_kthreadp;
    } else {
        gp_log[gp_log_idx].thread = NULL;
    }
    gp_log[gp_log_idx].tag = bfap->tag;
    gp_log[gp_log_idx].offset= offset;
    gp_log[gp_log_idx].size = size;
    gp_log[gp_log_idx].file_size = bfap->file_size;
    gp_log[gp_log_idx].pflags = pflags;
    gp_log[gp_log_idx].ftype = ftype;
    gp_log[gp_log_idx].cache_status = cache_status;
    gp_log[gp_log_idx].caller = caller;
    gp_log[gp_log_idx].action = action;
    gp_log[gp_log_idx].bf_set_flags = bfap->bfSetp->bfSetFlags;
    if (bsbufp != NULL) {
        if (bsbufp->bsb_metafwd != NULL) {
            gp_log[gp_log_idx].on_LSN_list = 1;
        } else {
            gp_log[gp_log_idx].on_LSN_list = 0;
        }            
        gp_log[gp_log_idx].bsb_writeref = bsbufp->bsb_writeref;
        gp_log[gp_log_idx].bsb_flushseq = bsbufp->bsb_flushseq;
        gp_log[gp_log_idx].bsb_flags = bsbufp->bsb_flags;
        gp_log[gp_log_idx].bsb_foffset = bsbufp->bsb_foffset;
        gp_log[gp_log_idx].bsb_pfdatbitmap = bsbufp->bsb_pfdatbitmap;
    }
    gp_log_idx = (gp_log_idx+1)%GP_LOG_SIZE;


    spin_unlock(&gp_log_lock);
}

#endif

#ifdef ADVFS_DEBUG
int32_t advfs_sync_cnt = 0;
#endif

#ifdef ADVFS_SMP_ASSERT
#define KEEP_PPAGE_STATS
#endif 

#ifdef KEEP_PPAGE_STATS
struct putpage_stats_struct {
    uint32_t invalidate_error;
    uint32_t user_data_flush_error;
    uint32_t metaflush_error;
    uint32_t metaflush_error2;
    uint32_t metaflush_and_invalidate_error;
    uint32_t metaflush_and_invalidate_error2;
    uint32_t failed_to_get_ioanchor;
    uint32_t failed_to_get_buf_struct;
    uint32_t blkmap_ewouldblock_failure;
    uint32_t rescanning_for_meta_alignmemt;
    uint32_t encountered_writeref;
    uint32_t domain_panic_marking_dirty;
};

typedef struct putpage_stats_struct putpage_stats_t;
putpage_stats_t putpage_stats;
#endif

#ifdef KEEP_PPAGE_STATS
#define PPAGE_STATS( __stat_name ) putpage_stats.__stat_name++
#else
#define PPAGE_STATS( __stat_name )
#endif

/*
 * advfs_putpage
 *
 * Assumptions: storage is already allocated,
 * individual pages are not related ie. they are
 * not in file page order.  Should really mark
 * pagelist entry null after page is successfully
 * pushed.  However, we cannot malloc an array
 * here.  Assume bf page size is not less than
 * vm page size.
 */

/*
 * NOTE: This needs to be updated to use return size and offset of
 * pages that were scanned in.
 */

int
advfs_putpage(
        fcache_vminfo_t *fc_vminfo,
        struct vnode *vp,
        off_t off,
        size_t size,
        struct advfs_pvt_param *fs_pvt_param,
        fcache_pflags_t pflags )
{
    fps_ptype_t ptype = 0; 
    bfAccessT *bfap = NULL;
    struct fsContext *contextp;
    domainT *dmnp;
    int extent_map_locked = FALSE;
    extent_blk_desc_t *blkmap=NULL, *copy_blkmap=NULL;
    extent_blk_desc_t *blkmap_entry;
    size_t remaining_request_size;
    off_t ending_request_offset;
    off_t original_ending_offset;
    off_t current_request_offset;
    off_t original_request_offset;
    off_t scan_offset;
    off_t meta_scan_offset;
    off_t blkmap_offset;
    off_t copy_blkmap_offset;
    size_t scan_size;
    size_t meta_scan_size;
    size_t blkmap_size;
    size_t copy_blkmap_size;
    page_fsdata_t *plist=NULL;
    fps_status_t scan_status;
    short metadata, nowait=0,migrate_in_progress=0;
    statusT sts = EOK;
    ioanchor_t *ioAnchor_head=NULL;
    multiWaitT *mwp=NULL;
    multiWaitLinkT *mwlp=NULL;
    advfs_metaflush_thread_msg_t *meta_msg;
    int err;
    int return_status = EOK;
    int update_mod_times = 0;
    struct bsBuf *bsbufs_to_clear=NULL;
    uint32_t xtnt_flags = 0;
    uint32_t unaligned = 0;
    int small_au_file = FALSE;
    round_type_t round_type;
    size_t size_to_write;

    int16_t     unlock_snap_lock = FALSE;

#ifdef ADVFS_DEBUG
    if ( pflags & FP_SYNCD ) {
        advfs_sync_cnt++;
    }
#endif
   
    /*
     * Get the bfap and make sure it is valid before continuing.
     */
    bfap = VTOA(vp);
    MS_SMP_ASSERT(bfap);


    /* If the bfap isn't mapped yet then bail out.
     * If the user specifies 0,0 for the flush then
     * we will make sure there is even any work to do
     */
    if ((!(bfap->bfaFlags&BFA_MAPPED)) ||
        (size == 0) &&
        ((bfap->bfaNextFob == 0 && bfap->file_size == 0) &&
         (bfap->bfa_orig_file_size == 0 || 
          bfap->bfa_orig_file_size == ADVFS_ROOT_SNAPSHOT) ) ||
        ((bfap->bfState == BSRA_INVALID) &&
         ((lk_get_state( bfap->stateLk ) == ACC_CREATING) ||
          (lk_get_state( bfap->stateLk ) == ACC_INIT_TRANS))) ||
        (&bfap->bfVnode != vp)) { /* vp is a shadow */
#ifdef  ADVFS_SMP_ASSERT
        fcache_vninfo_t	fcvn_info;
        int ret;
        if (pflags & FP_INVAL) {
            ret = fcache_vn_info(vp,FVINFO_PAGES,&fcvn_info);
            MS_SMP_ASSERT((ret == 0)&&(fcvn_info.fvi_pages == 0));
        }
#endif
        GPLOGIT(PUTPAGE_CALL,vp,off,size,pflags,0,0,0,QUICK_EXIT);
        return EOK;
    }

    MS_SMP_ASSERT( bfap->dataSafety != BFD_NOT_INITIALIZED );


    contextp = VTOC(vp);
    dmnp = bfap->dmnP;
    metadata = (bfap->dataSafety == BFD_LOG) || 
        (bfap->dataSafety == BFD_METADATA);
    current_request_offset = off;
    extern kmem_handle_t advfs_multiWait_arena;
    
    GPLOGIT(PUTPAGE_CALL,vp,off,size,pflags,0,0,0,DEFAULT_ACTION);

    if (metadata && fs_pvt_param &&
        (fs_pvt_param->app_flags&(APP_MARK_READ_ONLY|APP_RESTORE_PG_PROTECTION)) ) {
        /*
         * When dealing with marking pages read only or restoring
         * permissions, we don't care about metadata specifically.
         */
        metadata = FALSE;
    }

    if (size == 0) {
        /* This is a request to flush the entire file (from the passed
         * in offset). Set up the size accordingly. We need to include
         * all possible pages that this file can have in the
         * cache. This would include preallocated pages that are
         * beyond the eof. We also need to account for the case where
         * a file was truncated out beyond the last allocated storage.
         */

        if (bfap->dataSafety == BFD_USERDATA) {
            remaining_request_size = MAX( bfap->file_size,
                                          (off_t)(bfap->bfaNextFob *
                                                  ADVFS_FOB_SZ));
        } else {
/*          if (HAS_SNAPSHOT_PARENT(bfap) && IS_COWABLE_METADATA(bfap)) { */
	    if (    !(bfap->bfaFlags & BFA_ROOT_SNAPSHOT ) 
		 && IS_COWABLE_METADATA(bfap) ) {
                /* A metadata child may have pages in cache for a read
                 * yet not have it's own storage.  Those pages will be for
                 * parent storage that is bounded by bfa_orig_file_size.  It is
                 * important that we get those pages from page_scan for
                 * the FP_INVAL/FP_FREE case.
                 */
                remaining_request_size = MAX( bfap->bfa_orig_file_size,
                        (off_t)(bfap->bfaNextFob * ADVFS_FOB_SZ));
            } else {
                /* File size is not always maintained for metadata file.
                 * and you can't truncate out a metadata file so using the
                 * bfaNextFob is accurate
                 */
                remaining_request_size = bfap->bfaNextFob * ADVFS_FOB_SZ;
            }
        }
        /* Allow a zero size and an offset */
   
        remaining_request_size -= current_request_offset;
        ending_request_offset = current_request_offset + remaining_request_size;

    } else {

        ending_request_offset = current_request_offset + size;
        remaining_request_size = size;
    }

    /* Now we need to do some rounding. Metadata gets rounded to the
     * allocation unit. Userdata gets rounded to the VM page size.
     */
    if (metadata) {
            
        /* We need to work with allocation units for meta-data that
         * is currently fixed at 8K.  We cannot allow just one
         * part of a meta-data page to be flushed if both parts
         * are dirty. We will round the request to encompass
         * allocation units. 
         */
        
        current_request_offset = rounddown(off,bfap->bfPageSz*ADVFS_FOB_SZ);
        ending_request_offset = roundup(ending_request_offset,bfap->bfPageSz
                                        * ADVFS_FOB_SZ);
        remaining_request_size = ending_request_offset - current_request_offset;
    } else {

        original_request_offset = 
        current_request_offset = rounddown(off,VM_PAGE_SZ);
        original_ending_offset = 
        ending_request_offset = roundup(ending_request_offset,VM_PAGE_SZ);
        remaining_request_size = ending_request_offset - current_request_offset;
        small_au_file = ADVFS_FILE_HAS_SMALL_AU(bfap);
    }
    /* Here's what we plan to do */

#ifdef PUTPAGE_RETURN_SIZE_AND_OFFSET
    *off = current_request_offset;
    *size = remaining_request_size;
#endif

    /* Set up the proper arguments to fcache_page_scan based on the passed
     * in flags.
     */
    
    if (pflags & FP_WRITE) {
        ptype = FPS_GET_DIRTY;
    }
    if (pflags & (FP_FREE|FP_INVAL|FP_VHAND)){
        ptype = FPS_GET_DIRTY|FPS_GET_CLEAN;
    }
    if (pflags & (FP_VHAND|FP_SYNCD|FP_NOWAIT)) {
        nowait=1;
    }
    
    if ((fs_pvt_param) && 
        (fs_pvt_param->app_flags & 
        (APP_MIGRATE_FLUSH_ALL|APP_MIGRATE_FLUSH_WAIT|APP_MARK_READ_ONLY|APP_RESTORE_PG_PROTECTION))){
            
        /* The FPS_GET_ALL flag could be used here instead however we
         * are choosing not to since it would cause all of the pages
         * of the migrate range to be brought into the cache at
         * once. For a large migrate this might prove too taxing on
         * VM. 
         */
        MS_SMP_ASSERT(!(pflags&(FP_VHAND|FP_SYNCD|FP_INVAL|FP_FREE)));
        ptype = FPS_GET_DIRTY|FPS_GET_CLEAN;
    }
    if (metadata) {
        /* When scanning for metadata we must bring in all pages
         * at once. We no longer mark both 4K pages dirty when
         * modifying meta-data but we need to treat the entire
         * 8K as dirty. If one page is dirty then the whole 8K
         * is dirty. If we just asked for clean of dirty we could
         * race. i.e we could have the 1st dirty page in flight for
         * i/o, meanwhile the 2nd page is marked dirty and the unpin
         * changes the LSN in the bsBuf, then the 1st page completes
         * and we incorrectly mark the log as flushed upto the 2nd
         * LSN. By always bringing in both pages, this scenario the
         * modification to the 2nd page would page fault since the
         * 2nd page would be inflight.
         */
        if (bfap->dataSafety != BFD_LOG) {
            /* Due to the nature and frequency of log flushes we don't
             * want to scan for all. Log flushes tend to overlap and
             * can be very frequent. This can result in bringing clean
             * pages over and over. The reason we scan for all is because
             * each 4K page of the 8K (currently) meta-data page may not
             * be dirty (see above comment). In the case of the log however
             * we always make a log page "safe" before pushing it to disk
             * this entails writingthe 1st word in each 512 bytes of the
             * page, so we know all of the 8K is dirty.
             * NOTE if we every go to a different meta-data page size
             * this assumption will still hold.
             */

            ptype = FPS_GET_ALL;
        }
        meta_scan_offset=0;
        meta_scan_size=0;
    }

    /* At this point ptype must have been initialized. Lets
     * just make sure.
     */

    MS_SMP_ASSERT(ptype != 0);

    if (nowait || 
            ((fs_pvt_param) && 
            (fs_pvt_param->app_flags & APP_DEBUG_DIRTY))) {
        xtnt_flags = XTNT_NO_WAIT | X_LOAD_REFERENCE;
    }
    else {
        xtnt_flags = XTNT_WAIT_OK | X_LOAD_REFERENCE;
    }
    /*
     * In order to prevent deadlocking with COW when a page lock is
     * released, we will take out the BFA_SNAP_LOCK for WRITE access when
     * flushing data.  This is only necessary when we may block on a page
     * lock since we will be holding the region lock as well.  For VHAND and
     * SYNCD, we don't want to block trying to acquire the BFA_SNAP_LOCK,
     * but since VHAND and SYNCD won't block on the page locks, the deadlock
     * we are trying to avoid is not possible.  So we will just skip getting
     * the lock for those two callers.
     *
     * If we are coming from migrate, the lock is already held for write.
     */
    if ( HAS_SNAPSHOT_CHILD(bfap) && 
         ( !(fs_pvt_param) || ( (fs_pvt_param) && 
           !(fs_pvt_param->app_flags &  (APP_MIGRATE_FLUSH_WAIT|
                                         APP_MIGRATE_FLUSH_ALL|
                                         APP_MIGRATE_FAULT|
                                         APP_SNAP_LOCK_HELD)) ) ) ) {

        if (!(pflags & FP_VHAND) && !(pflags & FP_SYNCD)) {
            /* Take the file lock for write. This blocks out any new COWs in
             * getpage. */
            if (pflags & FP_NOWAIT || 
                    ((fs_pvt_param) && 
                     (fs_pvt_param->app_flags & APP_DEBUG_DIRTY))) {
                if (!ADVRWL_BFAP_SNAP_WRITE_LOCK_TRY( bfap )) {
                    return_status = EWOULDBLOCK;
                    goto putpage_error;
                }
            } else {
                ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
            }
            unlock_snap_lock = TRUE;
        }
    }


    while (current_request_offset <  ending_request_offset) {

        /* We will not break the scan into smaller pieces. This
         * may need to be done later on for performance reasons
         */
        /* We need to hold the extent map lock across the scan pages. This
         * avoids lock heirarchy issues with the page locks.
         */

        if (pflags & FP_VHAND) {
            /* We have a special extent map lock for VHAND.  This
             * insures that any thread obtaining the write lock for
             * the extents will also obtain this VHAND lock for write
             * after obtaining the extent map lock. Otherwise a write
             * locker could block on a read lock waiting on memory
             * which effectively turns the read locker into a write
             * locker.
             */
            
            if (!ADVRWL_VHAND_XTNTMAP_TRY_READ(&bfap->vhand_xtntMap_lk)) {
                /* Just break out of the loop and exit */
                return_status = EWOULDBLOCK;
                break;
            }
        } else {
            sts = x_lock_inmem_xtnt_map(bfap, xtnt_flags);
            if (sts != EOK) {
                /* Just break out of the loop and exit. Potentially
                 * waiting for any i/o's to complete
                 */

                MS_SMP_ASSERT(sts == E_WOULD_BLOCK);
                return_status = EWOULDBLOCK;
                break;
            }
        }
                          
        extent_map_locked = TRUE;

        
        /* Now that we are holding the extent map lock we can check if
         * a migrate is currently in progress and know that this
         * migrate will block until we drop the lock
         */
        
        if (bfap->xtnts.copyXtntMap != NULL) {
            
            migrate_in_progress = 1;
        } else {
            migrate_in_progress = 0;
        }
        /* For metadata files we are using the FPA_GET_ALL flag
         * which returns a list of clean and dirty pages (as opposed
         * to individual lists of just clean or just dirty). We will
         * create sub loop here to process ranges that have a common
         * attribute (clean or dirty).
         */
        scan_offset = current_request_offset;
        /* meta_scan_size refers to any pages brought in by a previous
         * call to page_scan. Since metadata pages are scanned for all
         * at once (clean and dirty) we need to process these one
         * group at a time. Once the meta_scan_size goes to zero we
         * will scan for the next batch of pages.
         */

        if (!(metadata) || (meta_scan_size == 0)){

            /*
             * If the file has an allocation unit smaller than the VM page size,
             * force the call to fcache_page_scan() to return only one VM page
             * if the offset requested is within the first VM page of the file.
             * This is because the first VM page of such a file is handled
             * differently from the rest of such a file.  Specifically, the
             * first VM page may have multiple extents backing it.
             */
            if ((small_au_file) && (scan_offset < (off_t)VM_PAGE_SZ)) {
                scan_size = VM_PAGE_SZ;
            }
            else {
                scan_size = remaining_request_size;
            }

            plist = fcache_page_scan(fc_vminfo, 
                                     vp, 
                                     &scan_offset, 
                                     &scan_size,
                                     &scan_status,
                                     ptype,
                                     pflags);
#ifdef ADVFS_SMP_ASSERT
        /* If the file is opened for Direct I/O, make sure the pages found
         * in the UFC have an active range associated with them, otherwise
         * stale data could be returned or written to the file, causing data
         * corruption or the appearence of corruption.
         */
        if ( plist && bfap->dioCnt ) {
            ASSERT_ACTRANGE_HELD( bfap, scan_offset, scan_size );
        }
#endif /* ADVFS_SMP_ASSERT */

            if (scan_status & FPS_ST_MOD_UPDATE) {
                update_mod_times = 1;
            }

#ifdef ADVFS_SMP_ASSERT
        /* 
         * only worry about these asserts if we don't have
         * a domain panic 
         */
        if (!dmnp->dmn_panic) {
            /* Make sure we don't invalidate a dirty RSVD file except for 
             * the log... when derefing a log page. */
            if (BS_BFTAG_RSVD(bfap->tag) && (bfap->dataSafety != BFD_LOG) 
                    && (pflags & FP_INVAL)) {
                MS_SMP_ASSERT((scan_status & FPS_ST_DIRTY) == 0);
                {
                    page_fsdata_t *dirty_pfdat=plist;
                    while (dirty_pfdat != NULL) {
                        MS_SMP_ASSERT( dirty_pfdat->pfs_info.pi_pg_dirty != 1 );
                        dirty_pfdat = dirty_pfdat->pfs_next;
                    }

                }
            }
        }
#endif
            
            GPLOGIT(PAGE_SCAN,vp,scan_offset,scan_size,pflags,0,scan_status,0,DEFAULT_ACTION);
            if (metadata) {
                meta_scan_size = scan_size;
                unaligned = scan_offset&(ADVFS_METADATA_PGSZ -1);
            }
        }
        if (plist == NULL) {
            /*
             * We didn't get any matching pages.  If this is a small
             * allocation unit file and we were scanning at offset 0
             * and the total request goes beyond VM_PAGE_SZ, reset
             * scan_size to be VM_PAGE_SZ and go to the loop end
             * processing, which will cause us to loop back and 
             * begin rescanning at offset VM_PAGE_SZ.
             */
            if ((small_au_file) &&
                (scan_offset < (off_t)VM_PAGE_SZ) && 
                (ending_request_offset > (off_t)VM_PAGE_SZ)) {
                scan_size = VM_PAGE_SZ;
                goto loop_end;
            }
            else {
                /* We are done for whatever reason. We may have processed
                 * some or no pages. We'll deal with this at the end of the
                 * loop.
                 */
            break;
            }
        }
        if (metadata) {
            /* This is metadata. We need to parse the plist into ranges
             * of clean or dirty pages. 
             */

            if (bfap->dataSafety != BFD_LOG)
            {
                /* We skip this for the log since there is no
                 * parsing to do. See comment above
                 */

                scan_status = parse_meta_list(&scan_size,plist);
                GPLOGIT(PAGE_SCAN,vp,scan_offset,
                        scan_size,pflags,0,scan_status,0,META_LIST_PARSED);
                if ((dmnp->dmn_panic) &&
                    (scan_status == FPS_ST_CLEAN)){
                    /* We need to further adjust the list of metadata
                     * pages in the case of a domain panic and clean
                     * pages. We may have pinned pages without modifying
                     * them.
                     */
                    sts = advfs_adjust_bsbuf_for_domain_panic(vp, 
                                                              scan_offset, 
                                                              &scan_size,
                                                              &scan_status);
                    MS_SMP_ASSERT(sts == EOK);
                }
            }
            meta_scan_size -= scan_size;
        }

#ifdef ADVFS_SMP_ASSERT
        if ((fs_pvt_param) && 
            (fs_pvt_param->app_flags & APP_DEBUG_DIRTY)) {

            MS_SMP_ASSERT(scan_status == FPS_ST_DIRTY);
            err = fcache_page_release(fc_vminfo, 
                                      scan_size,
                                      &plist,
                                      FP_DEFAULT);
            MS_SMP_ASSERT(err == 0);
            
            goto loop_end;
        }

        /* ASSERT if both APP_ASSERT_NO_DIRTY and APP_ASSERT_NO_CLEAN were
         * passed in on an invalidate call since there are either clean or 
         * dirty pages in this range.
         */
        if ( (pflags & FP_INVAL) &&  fs_pvt_param ) { 
            pvt_param_flags_t flags = fs_pvt_param->app_flags & 
                              (APP_ASSERT_NO_DIRTY | APP_ASSERT_NO_CLEAN);
            MS_SMP_ASSERT( flags == (APP_ASSERT_NO_DIRTY | APP_ASSERT_NO_CLEAN)
                           ? FALSE : TRUE );
        }
#endif

        if (scan_offset < current_request_offset) {
            /* We have a large page that extends to the left. We
             * should never see this for a meta-data file or a file with
             * a small allocation unit. We will adjust our range to include 
             * this in the i/o.
             */

            MS_SMP_ASSERT(!metadata);
            MS_SMP_ASSERT(!small_au_file);
            remaining_request_size += current_request_offset - scan_offset;
#ifdef PUTPAGE_RETURN_SIZE_AND_OFFSET
            *size += current_request_offset - scan_offset;
            *off = scan_offset;
#endif
            current_request_offset = scan_offset;
#ifdef KEEP_LGPG_STATS
            lgpg_stats.putpage_lgpg_left++;
#endif
        }
        if ((off_t)(scan_offset + scan_size) > ending_request_offset) {
            /* We have a large page that extends beyond our request. */
                
            MS_SMP_ASSERT(!metadata);
            MS_SMP_ASSERT(!small_au_file);
#ifdef PUTPAGE_RETURN_SIZE_AND_OFFSET
            /* Add the difference onto the size */
            *size += scan_offset + scan_size -
                     ending_request_offset;
#endif
#ifdef KEEP_LGPG_STATS
            lgpg_stats.putpage_lgpg_right++;
#endif
            remaining_request_size = scan_size;
            ending_request_offset = scan_offset + scan_size;
        }

        /* 
         * If the APP_MARK_READ_ONLY flag is set, we need to mark all pages
         * as pi_pg_ro.  Additionally, dirty pages need to be flushed.
         *
         * This is used when creating a snapset in order to mark a bfap's in
         * cache pages as read only.  This will insure that any
         * modifications to the page cause a fault that will allow COW
         * processing to occur.
         */
        if ( (fs_pvt_param) && (fs_pvt_param->app_flags & (APP_MARK_READ_ONLY) ) ) {
            page_fsdata_t* pfdat = plist;
            ADVFS_ACCESS_LOGIT_2( bfap, "advfs_putpage: Marking Read Only",
                    scan_offset, scan_size);
                    
            while (pfdat != NULL) {
                pfdat->pfs_info.pi_pg_ro = 1;
                pfdat = pfdat->pfs_next;
            }
            if (scan_status == FPS_ST_CLEAN) {
                /* There is nothing else to do with these pages, just
                 * release them and continue on.
                 */
                err = fcache_page_release(fc_vminfo, 
                                      scan_size,
                                      &plist,
                                      pflags);
                MS_SMP_ASSERT(err == 0);
                goto loop_end;
            } else {
                /* Dirty pages */
                /* When creating a snapset, we flush the domain in an
                 * exclusive transaction.  That means that if we are marking
                 * metadata pages read only, there should be no dirty pages.
                 */
                MS_SMP_ASSERT( !metadata );
            }
        }

        /* 
         * If the APP_RESTORE_PG_PROTECTION flag is set, we need to mark all pages
         * as pi_pg_ro if they are over a hole and clear pi_pg_ro if they
         * have storage.
         */
        if ( (fs_pvt_param) && 
                (fs_pvt_param->app_flags & (APP_RESTORE_PG_PROTECTION) ) ) {
            page_fsdata_t* pfdat = plist;
            int32_t        stg_found = FALSE;

            GPLOGIT(COW_UNPROTECT,vp,scan_offset,scan_size,pflags,0,scan_status,
                    0,QUICK_EXIT);

            ADVFS_ACCESS_LOGIT_2( bfap, "advfs_putpage: cow unprotect", 
                    scan_offset, scan_size);

            sts = advfs_snap_unprotect_incache_plist( bfap, plist, scan_offset, 
                    scan_size, &stg_found, 0 );
            MS_SMP_ASSERT(sts == EOK);
            /* There is nothing else to do with these pages, just
             * release them and continue on.
             */
            err = fcache_page_release(fc_vminfo, 
                                      scan_size,
                                      &plist,
                                      pflags);
            MS_SMP_ASSERT(err == 0);
            goto loop_end;
        }


        if (((fs_pvt_param) && 
             (fs_pvt_param->app_flags & (APP_MIGRATE_FLUSH_WAIT))) ||
            ((scan_status & FPS_ST_CLEAN) && 
             ((!migrate_in_progress) ||
              (bfap->migrate_starting_offset >= (off_t)(scan_offset +
                                                        scan_size)) ||
              (bfap->migrate_ending_offset <= scan_offset)))){

            size_t size_to_release = scan_size;

            /* If APP_MIGRATE_FLUSH_WAIT this is a fake flush to
             * synchronize any i/o's that may have been started by
             * other threads during the migrate.  By looking up each
             * page in the cache we end up waiting for each page's
             * page lock. This means we wait for any i/o in flight to
             * finish. This will insure that all i/o to the storage
             * that we are about to remove has completed. We will then
             * release any pages that we looked up.
             *
             * If a migrate is not in progress OR this page scan does
             * not overlap the current range of the file being
             * migrated then this will just release the pages with the
             * passed in flags.  This is basically a request to free
             * or invalidate pages.
             */

            if (metadata && (pflags&(FP_INVAL|FP_FREE))) {
                /* We need to clean up bsBufs here. VM will always
                 * call into putpage when invalidating or freeing
                 * clean/diry pages. This is our chance to clean up
                 * any filesystem specific structures.
                 */

                size_to_release = advfs_process_bsbufs_for_freeing (fc_vminfo,
                                                        vp, 
                                                        scan_offset, 
                                                        &scan_size, 
                                                        unaligned,
                                                        &plist, 
                                                        pflags );
            }
            
            if (size_to_release > 0) {
                int32_t        stg_found = FALSE;

                GPLOGIT(FREE_CLEAN,vp,scan_offset,size_to_release,pflags,0,
                        scan_status,0,RELEASED_RANGE);
                
                if (HAS_SNAPSHOT_CHILD(bfap) && (pflags & FP_FREE)) {
                    /* We need to clear the RO bit because these pages
                     * are going on the free list.  If all snapshots
                     * are removed, then we normally clear the RO bit for
                     * all in-cache pages.  However, if the page is on the 
                     * free list, then that bit will not be cleared.  So
                     * we clear it now.  If the page is returned to us 
                     * (without being torn down by VM) then we will set the
                     * RO bit in advfs_getpage() (if snapshots exist)...even
                     * though the page IS NOT datafill.
                     */
                    sts = advfs_snap_unprotect_incache_plist(bfap,
                            plist,
                            scan_offset,
                            scan_size,
                            &stg_found,
                            (nowait)?XTNT_LOCKS_HELD|XTNT_NO_WAIT:
                            XTNT_LOCKS_HELD|XTNT_WAIT_OK);
                    if (sts != EOK) {
                        MS_SMP_ASSERT(sts = EWOULDBLOCK);
                        MS_SMP_ASSERT(nowait);
                        MS_SMP_ASSERT(scan_size > 0);
                        err = fcache_page_release(fc_vminfo, 
                                scan_size,
                                &plist,
                                FP_DEFAULT);

                        MS_SMP_ASSERT(err == 0);
                        return_status = EWOULDBLOCK;
                        break;
                    }
                }
                err = fcache_page_release(fc_vminfo, 
                                          size_to_release,
                                          &plist,
                                          pflags);
                MS_SMP_ASSERT(err == 0);
            }
            goto loop_end;
        }
        if ((scan_status & FPS_ST_DIRTY) &&
            (pflags & FP_WRITE)) {
            /* We have some dirty pages. We will need to push
             * them to disk so get the blkmaps now 
             */
            blkmap=NULL;

            /*
             * If this file has a small allocation unit and we're pushing
             * out part of the first VM_PAGE_SZ bytes of the file, handle
             * it specially, since it may be backed by multiple extents.
             */
            if ((small_au_file) && (scan_offset < (off_t)VM_PAGE_SZ)) {
                blkmap_offset = off;
                blkmap_size = VM_PAGE_SZ - off;
                round_type = RND_ALLOC_UNIT;
            }
            else {
                blkmap_offset = scan_offset;
                blkmap_size = scan_size;
                round_type = RND_VM_PAGE;
            }

            sts = advfs_get_blkmap_in_range(bfap,
                                            bfap->xtnts.xtntMap,
                                            &blkmap_offset,
                                            blkmap_size,
                                            &blkmap,
                                            NULL,
                                            round_type,
                                            EXB_COMPLETE,
                                            (nowait) ?
                                                XTNT_LOCKS_HELD|XTNT_NO_WAIT :
                                                XTNT_LOCKS_HELD|XTNT_WAIT_OK);

            if (sts != EOK) {
                /* The caller can't block so we must bail out and
                 * deal with any i/o started so far.
                 */
                MS_SMP_ASSERT(sts == E_WOULD_BLOCK);
                MS_SMP_ASSERT(nowait);
                MS_SMP_ASSERT(scan_size > 0);
                err = fcache_page_release(fc_vminfo, 
                                          scan_size,
                                          &plist,
                                          FP_DEFAULT);

                MS_SMP_ASSERT(err == 0);
                PPAGE_STATS(blkmap_ewouldblock_failure);
                return_status = EWOULDBLOCK;
                break;
            }

            /*
             * If this is the first page of a small allocation unit file,
             * see if there is a trailing hole less than the size of a
             * VM page.  If so, throw it away.
             */
            if ((small_au_file) && (scan_offset == 0)) {
                extent_blk_desc_t *temp_entry;

                for (temp_entry = blkmap;
                     temp_entry->ebd_next_desc &&
                     temp_entry->ebd_next_desc->ebd_vd_blk != XTNT_TERM &&
                     temp_entry->ebd_next_desc->ebd_vd_blk != COWED_HOLE;
                     temp_entry = temp_entry->ebd_next_desc);

                if (temp_entry->ebd_next_desc) {
                    (void)advfs_free_blkmaps(&temp_entry->ebd_next_desc);
                    temp_entry->ebd_next_desc = NULL;
                }
            }
        }
        if ((migrate_in_progress) &&
            (bfap->migrate_starting_offset < (off_t)(scan_offset + 
                                                     scan_size)) &&
            (bfap->migrate_ending_offset > scan_offset)) {
            /* A migrate is currently in progress and the pages we
             * are about to push out overlap with the migrate
             * range. We will need to push these pages to the copy
             * destination (only for the overlap region).
             *
             * We will request EXB_DO_NOT_INHERIT since we should never be
             * pushing out data to storage not mapped in the file's extent
             * maps.  (A child shouldn't write to its parent).
             */
                    
            copy_blkmap=NULL;
            copy_blkmap_offset = MAX(scan_offset,bfap->migrate_starting_offset);
            copy_blkmap_size   = MIN(bfap->migrate_ending_offset,
                                     (off_t)(scan_offset + scan_size)) 
                               - copy_blkmap_offset;
            sts=advfs_get_blkmap_in_range(bfap,
                                          bfap->xtnts.copyXtntMap,
                                          &copy_blkmap_offset,
                                          copy_blkmap_size,
                                          &copy_blkmap,
                                          NULL,
                                          RND_ALLOC_UNIT,
                                          EXB_COMPLETE|EXB_DO_NOT_INHERIT,
                                          (nowait)?XTNT_LOCKS_HELD|XTNT_NO_WAIT:
                                                XTNT_LOCKS_HELD|XTNT_WAIT_OK);
                    
            if (sts != EOK) {
                /* The caller can't block so we must bail out and
                 * deal with any i/o started so far.
                 */
                MS_SMP_ASSERT(sts == E_WOULD_BLOCK);
                MS_SMP_ASSERT(nowait);
                MS_SMP_ASSERT(scan_size > 0);
                    
                err = fcache_page_release(fc_vminfo, 
                                          scan_size,
                                          &plist,
                                          FP_DEFAULT);
                        
                MS_SMP_ASSERT(err == 0);
                PPAGE_STATS(blkmap_ewouldblock_failure);
                return_status = EWOULDBLOCK;
                break;
            }

            /*
             * Sanity check the copy_blkmap returned.
             */
            MS_SMP_ASSERT(copy_blkmap->ebd_offset <= 
                    MAX(scan_offset,bfap->migrate_starting_offset));

            
            MS_SMP_ASSERT(copy_blkmap_offset == 
                          MAX(scan_offset,bfap->migrate_starting_offset));

            if (scan_status & FPS_ST_CLEAN) {
                /* This is the case of a migrate in progress. We must
                 * flush clean pages to the COPY location on
                 * disk. Here we will make the original extent map the
                 * COPY extent map and the code should work as is.
                 */

                blkmap = copy_blkmap;
                blkmap_offset = copy_blkmap_offset;
                blkmap_size = copy_blkmap_size;
                copy_blkmap = NULL;
            }
        }
        if ((pflags & FP_INVAL) && !(pflags & FP_WRITE)) {
            /* This is the case of an invalidation (no flush)
             */
            MS_SMP_ASSERT(!migrate_in_progress);
            MS_SMP_ASSERT(!(pflags&FP_VHAND));
            MS_SMP_ASSERT(scan_status & FPS_ST_DIRTY);
            if (!metadata) {
                size_t size_to_release;

                /* For user data we can just release the pages since
                 * there is no real clean up to be done at i/o
                 * completion. We already dealt with clean pages at
                 * the top of this loop.  If we got a large page, however,
                 * we must release but not invalidate those pfdats that
                 * fall outside of the caller's request.
                 */

                MS_SMP_ASSERT(scan_size > 0);

                if (scan_offset < original_request_offset) {
                    /*
                     * We got a large page to the left of what the caller
                     * requested.  Release those pfdats that are to the
                     * left of the request.
                     */
                    err = fcache_page_release(fc_vminfo, 
                               original_request_offset - scan_offset,
                                              &plist,
                                              FP_DEFAULT);
                    MS_SMP_ASSERT(err == 0);
#ifdef KEEP_LGPG_STATS
                    lgpg_stats.putpage_inval_release_lgpg_left++;
#endif
                }

                MS_SMP_ASSERT(plist);
 
                if ((off_t)(scan_offset + scan_size) <= original_ending_offset){
                    /*
                     * Normal case or large page to the left.
                     */
                    size_to_release = (scan_offset + scan_size) -
                                      plist->pfs_off;
                }
                else {
                    /*
                     * Large page to the right.
                     */
                    size_to_release = original_ending_offset - plist->pfs_off;
                }

                err = fcache_page_release(fc_vminfo, 
                                          size_to_release,
                                          &plist,
                                          pflags);
                        
                MS_SMP_ASSERT(err == 0);

                if ((off_t)(scan_offset + scan_size) > original_ending_offset) {
                    /*
                     * We got a large page to the right of what the caller
                     * requested.  Release those pfdats that are to the
                     * right of the request.
                     * NOTE: WE DON'T CURRENTLY EXPECT THIS TO BE POSSIBLE.
                     * BUT IT WOULD BE IN THE FUTURE IF WE WERE TO REMOVE
                     * STORAGE FROM THE MIDDLE OF A USER FILE.  ADDING AN
                     * ASSERTION FOR NOW THAT WE DON'T GET HERE.  IF WE HIT
                     * THIS ASSERTION, WE'LL LEARN SOMETHING.
                     */
                    MS_SMP_ASSERT(plist);
                    MS_SMP_ASSERT(0);

                    err = fcache_page_release(fc_vminfo, 
                            (scan_offset + scan_size) - original_ending_offset,
                                              &plist,
                                              FP_DEFAULT);
                    MS_SMP_ASSERT(err == 0);
#ifdef KEEP_LGPG_STATS
                    lgpg_stats.putpage_inval_release_lgpg_right++;
#endif
                }

            } else {
                
                struct bsBuf *bsbuf_list_hd = NULL;
                struct bsBuf *bsbuf_list_tl = NULL;
#ifdef ADVFS_SMP_ASSERT
                page_fsdata_t *pfdat_ptr = plist;
#endif
                int lookup;
                struct bsBuf *bsbuf_ptr;
                off_t pfdat_offset=scan_offset;
                void * cookie;
                /* For meta-data we have some cleanup to do since the
                 * pages are marked dirty. We will do this by faking
                 * the i/o and allowing our completion routines to
                 * perform the cleanup as though these pages were
                 * written.
                 *
                 */

                /* I'm going to make assumptions here that the caller will
                 * insure that any metadata pages being invalidated are not
                 * currently being pinned !!!!!!!.
                 */

                while (pfdat_offset < (off_t)(scan_offset + scan_size)) {
                    /* Dirty metadata PFDATs always come in
                     * ADVFS_METADATA_PGSZ aligned entities
                     * (currently pairs).
                     */
                    MS_SMP_ASSERT((pfdat_ptr->pfs_off & ((ADVFS_METADATA_PGSZ - 1))) == 0);
                    MS_SMP_ASSERT(pfdat_ptr->pfs_vp == vp);
                    
                    lookup = TRUE;
                    /* Should we honor FP_NOWAIT here? */
                    bsbuf_ptr = advfs_obtain_bsbuf(vp,
                                                   pfdat_offset,
                                                   &cookie,
                                                   &lookup);

                    BS_BSBUF_HASH_UNLOCK(bsbuf_ptr->bsb_hashlink.dh_key);

                    MS_SMP_ASSERT(lookup); /* Better have existed */
                    MS_SMP_ASSERT(bsbuf_ptr->bsb_foffset == pfdat_ptr->pfs_off);
                    MS_SMP_ASSERT(!(bsbuf_ptr->bsb_flags & BSBUFFLG_FLUSHNEEDED));
                    MS_SMP_ASSERT(bsbuf_ptr->bsb_writeref == 0);
                    
                    /* Create a list of bsBufs */
                    
                    if (bsbuf_list_hd != NULL) {
                        bsbuf_list_tl->bsb_iolistfwd = bsbuf_ptr;
                    } else {
                        bsbuf_list_hd = bsbuf_ptr;
                    }
                    bsbuf_list_tl = bsbuf_ptr;
                    
                    pfdat_offset += ADVFS_METADATA_PGSZ;
#ifdef ADVFS_SMP_ASSERT
                    pfdat_ptr = pfdat_ptr->pfs_next->pfs_next /* Move over two*/;
#endif
                }
                GPLOGIT(INVAL_DIRTY,vp,scan_offset,scan_size,pflags,0,
                        scan_status,bsbuf_list_hd,FAKING_IO);
                
                /* We have a list of bsBufs corresponding to this range of
                 * PFDATs. Now we can issue the fake I/O to invalidate the
                 * pages.
                 */
                    
                /* We need to clean the page so fake the i/o by passing
                 * NULL blkmaps to the startio routine
                 */
                sts = advfs_start_blkmap_io(fc_vminfo,
                                            vp,
                                            scan_offset,
                                            scan_size,
                                            NULL,
                                            NULL,
                                            &ioAnchor_head,
                                            &plist,
                                            &bsbuf_list_hd,
                                            ((pflags & (~FP_SYNC)) | FP_ASYNC | FP_WRITE),
                                            0);
                if (sts != EOK) {
                    /* set up for error backout */
                    bsbufs_to_clear = bsbuf_list_hd;
                    PPAGE_STATS(invalidate_error);
                    return_status = sts;
                    goto putpage_error;
                }
            }
            goto loop_end;
            
        } /* End of invalidate */

        blkmap_entry = blkmap;
        MS_SMP_ASSERT(blkmap_entry != NULL);

        if ((scan_status & FPS_ST_CLEAN) &&
            (plist->pfs_off < blkmap_entry->ebd_offset)) {
                
            /*
             * Round size_to_release down to VM_PAGE_SZ so that if we have
             * exactly one page but it is the first page of a file with
             * a small allocation unit and a migrate of just part of
             * that page is in progress, we won't release the page.
             */
            size_t size_to_release = 
               rounddown(blkmap_entry->ebd_offset - plist->pfs_off, VM_PAGE_SZ);

            /* The only way we can have clean pages here is if
             * a migrate is in progress otherwise we would have
             * already released the pages. The blkmap may not
             * completely encompass the pages returned from
             * fcache_page_scan. The scan_offset and scan_size
             * may not be rounded to VM PAGE boundaries but
             * the blkmap offset and size will be. This is OK
             * here since we are looking for scans to be outside
             * of the blkmaps.
             */

            MS_SMP_ASSERT(migrate_in_progress);
            MS_SMP_ASSERT(scan_offset < blkmap_entry->ebd_offset);
            
            /* Release the begining of the plist that does
             * not overflap with the range to be
             * migrated 
             */

            if (metadata && (pflags&(FP_INVAL|FP_FREE))) {
                /* We need to clean up bsBufs here. VM will always
                 * call into putpage when invalidating or freeing
                 * clean/diry pages. This is our chance to clean up
                 * any filesystem specific structures.
                 */
                size_t meta_size = size_to_release;

                size_to_release = advfs_process_bsbufs_for_freeing (fc_vminfo,
                                                        vp, 
                                                        scan_offset, 
                                                        &meta_size, 
                                                        unaligned,
                                                        &plist, 
                                                        pflags );
            }
            if (size_to_release > 0) {
                        
                err = fcache_page_release(fc_vminfo, 
                                          size_to_release,
                                          &plist,
                                          pflags);
            
                MS_SMP_ASSERT(err == 0);
            }
        }

        
        /* The next block of code will do some I/O.  This will either
         * be pushing dirty pages to disk or pushing clean pages to
         * the COPY area for migrate. 
         */

        while(blkmap_entry != NULL)
        {

            if ((blkmap_entry->ebd_vd_blk == XTNT_TERM) ||
                    (blkmap_entry->ebd_vd_blk == COWED_HOLE)) {
                /* We are attempting to push out a hole! We should
                 * only encounter this under very special
                 * circumstances.
                 */

                if (scan_status & FPS_ST_CLEAN) {
                    /* A migrate is currently in progress. We
                     * stumbled upon a hole that was read in as
                     * zeros but has no backing store. We simply
                     * want to skip it and release the page.
                     */

                    MS_SMP_ASSERT(migrate_in_progress);

                    /* better be marked read-only. Just assert on
                     * the first one.
                     */
                    MS_SMP_ASSERT(plist->pfs_info.pi_pg_ro || 
                                (bfap->bfaParentSnap != NULL)); 
                    MS_SMP_ASSERT(blkmap_entry->ebd_byte_cnt);

                    err = fcache_page_release(fc_vminfo, 
                            blkmap_entry->ebd_byte_cnt,
                            &plist,
                            pflags);
                    MS_SMP_ASSERT(err == 0);
                } else {
                    /* We should never encounter a dirty hole.
                     */
                    MS_SMP_ASSERT(0);
               
                }
            } else if (!metadata) {

                /* We have been called to flush a range of pages. The
                 * following code will service normal, VHAND (FP_VHAND) and
                 * syncer (FP_SYNC) flushes of user data.  The appropriate
                 * flags have been set up prior to calling fcache_page_scan
                 * above.
                 */

                /* The cache pages have backing store. We will
                 * now push them out to disk.
                 * We can now start the i/o. We will pass the
                 * COPY blkmap pointer to indicate whether a
                 * migrate is in progress (which may have been
                 * set up above) otherwise this is just a
                 * normal i/o.
                 */

                /*
                 * Special processing for small allocation unit files.
                 */
                if ((small_au_file) &&
                    (blkmap_entry->ebd_offset < (off_t)VM_PAGE_SZ)) {
                    extent_blk_desc_t *temp_blkmap_entry;

                    /*
                     * This file may have multiple
                     * on-disk extents within the first VM_PAGE_SZ bytes
                     * of the file.  We will write all of them from the
                     * current VM page.  To avoid releasing the
                     * page in between individual extent I/O's,
                     * we will tell advfs_start_blkmap_io() that
                     * the I/O length is the sum of the lengths of
                     * the blockmap entries.  Doing this will cause
                     * advfs_start_blkmap_io() to treat the possibly
                     * multiple extent I/O's as one logical I/O.
                     */
                    for (size_to_write = 0,
                         temp_blkmap_entry = blkmap_entry;
                         temp_blkmap_entry;
                         size_to_write +=temp_blkmap_entry->ebd_byte_cnt,
                         temp_blkmap_entry = 
                                      temp_blkmap_entry->ebd_next_desc);

                    MS_SMP_ASSERT(size_to_write <= VM_PAGE_SZ);
                }
                else {
                    size_to_write = blkmap_entry->ebd_byte_cnt;
                }
                    
                sts = advfs_start_blkmap_io(fc_vminfo,
                                                vp,
                                                blkmap_entry->ebd_offset,
                                                size_to_write,
                                                blkmap_entry,
                                                &copy_blkmap,
                                                &ioAnchor_head,
                                                &plist,
                                                NULL,
                                                pflags,
                                                0);
                if (sts != EOK) {
                    /* We could not do the i/o. We need to release
                     * these pages and error out.
                     */
                    return_status=sts;
                    PPAGE_STATS(user_data_flush_error);
                    goto putpage_error;
                }
                /* End of User data processing */
            } else {
                /* Start of meta-data processing */

                /* This is a flush request on a meta-data file. Generally
                 * this will be for just an 8K request. We will flush the
                 * page if it is not currently writeRefed or its LSN is
                 * below the log's hiFlushLSN. If the page is writeRefed,
                 * we will set up a multiWait link structure and attach it to
                 * the bsBuf. This will indicate to the unpinner to flush
                 * the page. If the page's LSN is too high we will flush
                 * the log and then retry. We will also service clean
                 * pages in this code path.
                 */

                /* If this is a flush request on behalf of VHAND or
                 * the SYNC demon. We cannot block waiting for locks
                 * or I/O.  The FP_VHAND flag will cause scan_page to
                 * return only dirty pages that have not been
                 * referenced. This is problematic since we absolutely
                 * cannot flush just half of a full dirty meta-data
                 * page.  Similarly the no wait characteristic of the
                 * FP_VHAND or FP_SYNCD flags could cause a single
                 * dirty meta-data PFDAT to be returned if the other
                 * PFDAT is currently locked (not quite sure if this
                 * is possible but we have to deal with it for the
                 * FP_VHAND case anyway) As an optimization we will
                 * flush eligible full dirty meta-data pages only. Any
                 * pages that are not eligible or are not a full 8K
                 * page will be skipped and in the case of VHAND a
                 * message will be sent to a kernel thread to flush
                 * this page on our behalf. 
                 */

                 size_t current_length = blkmap_entry->ebd_byte_cnt; 
                 off_t current_offset = blkmap_entry->ebd_offset; 
                 off_t start_of_write = blkmap_entry->ebd_offset; 
                 struct bsBuf *bsbuf = NULL;
                 struct bsBuf *bsbuf_list_hd = NULL;
                 struct bsBuf *bsbuf_list_tl = NULL;
                 int lookup = 0,reloop = 0;
                 fcache_pflags_t release_flags;
                 size_t current_page_length;
                 void *cookie;
                 int bsbuf_chain_lock_held = FALSE;


                while (current_length > 0)
                {
                    MS_SMP_ASSERT(!(bsbuf_chain_lock_held));
                    if ((scan_status & FPS_ST_DIRTY) ||
                        (pflags & (FP_INVAL | FP_FREE))){
                        /* We only need bsBufs for dirty meta-data or
                         * if we are freeing clean pages and there
                         * might still be some bsBufs left (We would
                         * only be here in that case if this range is
                         * currently within a migrate that is in
                         * progress). We do NOT want to pass a bsBuf
                         * to the completion routine for flushing of
                         * clean pages (this is a migrate in progress
                         * situation).
                         */

                        lookup = TRUE;
                        /* Should we honor FP_NOWAIT here? */
                        bsbuf=advfs_obtain_bsbuf(vp,current_offset,&cookie,&lookup);
                        MS_SMP_ASSERT((scan_status & FPS_ST_CLEAN) || (lookup));

                        /* If we found a bsBuf we are holding the
                         * chain lock! We will either drop this
                         * explicitly or by calling remove_bsbuf.
                         * If we are not going to call remove_bsbuf
                         * we can drop the lock now.
                         */
                        /* We only need to hold the chain lock if this
                         * is a clean metapage, there is a bsBuf and we
                         * are invalidating or freeing the pages.
                         */

                        if (lookup){
                            /* We found a bsBuf in the hashtable */
                            if ( (scan_status == FPS_ST_DIRTY) ||
                                 (!(pflags&(FP_INVAL|FP_FREE)))){
                                /* It is NOT a clean page that is
                                 * being invalidated or freed. Since
                                 * we will not be removing the bsBuf
                                 * from the hash table we can drop the
                                 * lock now.
                                 */
                                BS_BSBUF_HASH_UNLOCK(bsbuf->bsb_hashlink.dh_key);
                            } else {
                                /* This bsBuf needs to be removed from
                                 * the hashtable before starting the
                                 * i/o so we MUST hold onto the chain
                                 * lock
                                 */
                                MS_SMP_ASSERT(bsbuf);
                                bsbuf_chain_lock_held = TRUE;
                            }
                        }
                    }
                    
                    if ((current_offset & (ADVFS_METADATA_PGSZ -1)) ||
                        (current_length < ADVFS_METADATA_PGSZ)) {
                        /* This is a partial meta-data page. Since we
                         * are dealing with a file offset relative
                         * range of pages we know that the only way we
                         * can be in the non-alignement situation is
                         * if the page is at the beginning or end of
                         * the range (or both - a single PFDAT).  We
                         * can get here if easily if it is SYNCD or
                         * VHAND since they will only grab dirty pages
                         * that were least recently reference.  We
                         * could get here in a race with getpage when
                         * the first page is found, marked dirty and
                         * released and then goes to the 2nd page. We
                         * could sneak in and find just the first
                         * page. Unfortunately we really can't assert
                         * on the writeref since the thread could
                         * actually get all the way to unpin by the
                         * time we got here (unlikely but there is no
                         * resource we are holding that could stop
                         * him).
                         */

                        current_page_length = VM_PAGE_SZ;
                    } else {
                        /* This is a full meta-data page */
                        
                        current_page_length = ADVFS_METADATA_PGSZ;
                    }
                    /*For meta-data we will push out as much as we can in
                     * a single I/O.
                     */
                
                    /* We don't need the bufLock to check for a zero
                     * bsb_writeref since the pagelock will keep it
                     * from being bumped. Actually this last statement
                     * is now a lie but the only way it can be bumped
                     * is if we are racing a pinpg in which case the
                     * caller of pinpg will block on these page locks
                     * in getpage. This means the pages themselves are
                     * actually still safe to push out as long as the
                     * writeref was zero at the point of this test
                     * since the caller of pinpg can not actually
                     * modify these pages. It can however go to zero
                     * after this check but see the comments below...
                     */

                    if (((scan_status & FPS_ST_CLEAN) &&
                         ((!bsbuf) || (bsbuf->bsb_writeref == 0))) ||
                        ((bsbuf->bsb_writeref == 0) &&
                         (current_page_length == ADVFS_METADATA_PGSZ) &&
                         ((bfap->dataSafety == BFD_LOG) ||
                          (bsbuf->bsb_metafwd == NULL) ||
                          (LSN_LTE(bsbuf->bsb_currentlogrec.lsn, 
                                   dmnp->ftxLogP->hiFlushLsn))))) {

                        /* This page is eligible for i/o for the
                         * following reasons:
                         *
                         * It is clean - A migrate is in progress. We
                         * need to push this out to the COPY location.
                         * There are no restrictions on doing this.
                         *
                         * The writeref is zero - We can never push
                         * out a page that is currently pinned.
                         *
                         * It is not a partial page(We can not push out just 
                         * a portion of a meta-data page).
                         *
                         * It is a LOG page. Dirty log pages can be pushed
                         * out without checking the LSNs.
                         *
                         * It is not on the LSN dirty list. This page is
                         * being written outside of transaction control. 
                         * Generally this is initialization of a page(i.e.
                         * at domain creation time).
                         *
                         * The LSN is lower than the log's hiFlushLsn. 
                         * The page can be written without first 
                         * flushing the log.
                         */

                        MS_SMP_ASSERT((scan_status & FPS_ST_DIRTY) ||
                                      (migrate_in_progress));

                        if (bsbuf_chain_lock_held) {
                            int removed_bsbuf;
                            /* If the chain is locked then we have
                             * already determined that we need to remove
                             * this bsbuf. So lets get to it.
                             */

                            MS_SMP_ASSERT((scan_status == FPS_ST_CLEAN) &&
                                          (pflags&(FP_INVAL|FP_FREE)) &&
                                          (bsbuf));
                            /* We need to clean up bsBufs here. VM
                             * will always call into putpage when
                             * invalidating or freeing clean/diry
                             * pages. Dirty pages will have their
                             * bsBufs cleanup at io completion
                             * time. This is our chance to clean up
                             * any filesystem specific structures for
                             * clean pages. We can free the bsBuf now
                             * since io completion does not need to
                             * pull it off the LSN list. 
                             */
                            
                            /* Should we honor FP_NOWAIT here? */
                            GPLOGIT(FLUSH_INVALFREE_DIRTY,vp,scan_offset,scan_size,pflags,0,
                                    scan_status,bsbuf,REMOVING_BSBUF);

                            removed_bsbuf = advfs_remove_bsbuf(vp,
                                                               bsbuf->bsb_foffset,
                                                               ADVFS_METADATA_PGSZ,
                                                               cookie);
                            bsbuf_chain_lock_held = FALSE;

                        }
                        /* In the case of a migrate in progress and we
                         * have clean pages that are being freed or
                         * invalidated, we just removed the bsBuf from
                         * the hashtable and released it. Since we are
                         * holding the pages locked any pinpg caller
                         * will block in getpage. After we removed the
                         * bsBuf a new one could have be created and
                         * placed in the hashtable with the writeref
                         * bumped. That is actually ok since the pages
                         * are locked and will be freed at the end of
                         * this call and the pinpg will bring the
                         * pages back into the cache and setup the
                         * bsbuf.
                         * Also note that if we fail to push these clean
                         * pages out to the migrate copy, we will just
                         * have the situation where clean pages are in
                         * the cache without a bsBuf which is fine.
                         */

                        if (scan_status & FPS_ST_DIRTY) {
                            /* We need to keep a list of bsBufs for io
                             * completion only for dirty pages.
                             */
                            if (bsbuf_list_hd != NULL) {
                                bsbuf_list_tl->bsb_iolistfwd = bsbuf;
                            } else {
                                bsbuf_list_hd = bsbuf;
                            }
                            bsbuf_list_tl = bsbuf;
                        }

                        /* We will start accumulating pages until
                         * either we are finished with this blkmap or
                         * we are forced to flush be cause a page in
                         * the middle of the range cannot be flushed.
                         */

                        current_offset      += current_page_length;
                        current_length      -= current_page_length;
                    
                        continue;
                    }
                    /* This metadata page is not eligible for I/O.
                     * This could be a clean or dirty page. We failed
                     * some neccessary condition above. If the page is
                     * clean then we must be in an FP_FREE or FP_INVAL 
                     * situation (its the only case we need to remove
                     * a bsBuf) and its migrate in progress.
                     *
                     * Regardless we need to start io on all the pages
                     * we have accumulated so far that are eligible to
                     * go out. After that we will release this page
                     * back into the cache.
                     */

                    if (bsbuf_chain_lock_held) {

                        MS_SMP_ASSERT((scan_status == FPS_ST_CLEAN) &&
                                      (pflags&(FP_INVAL|FP_FREE)) &&
                                      (bsbuf));
                        /* We are still holding the hashtable chain
                         * locked we need to drop it now.
                         */
                        BS_BSBUF_HASH_UNLOCK(bsbuf->bsb_hashlink.dh_key);
                        bsbuf_chain_lock_held = FALSE;
                    }

                    if (start_of_write < current_offset) {
                        /* We have some bsBufs accumulated so lets
                         * kick off this I/O. We will pass the copy
                         * extent map (which may be NULL) and if a
                         * migrate is in progress the
                         * advfs_start_blkmap_io routine will handle
                         * doing double i/o's
                         */

                        GPLOGIT(STARTED_PREVIOUS_METADATA_WRITE,vp,start_of_write,
                                current_offset - start_of_write,pflags,0,
                                scan_status,bsbuf_list_hd,INELIGIBLE_ENCOUNTERED);

                        sts = advfs_start_blkmap_io(fc_vminfo,
                                                    vp,
                                                    start_of_write,
                                                    current_offset - start_of_write,
                                                    blkmap_entry,
                                                    &copy_blkmap,
                                                    &ioAnchor_head,
                                                    &plist,
                                                    &bsbuf_list_hd,
                                                    pflags,
                                                    0);
                        if (sts != EOK) {
                            PPAGE_STATS(metaflush_error);
                            bsbufs_to_clear = bsbuf_list_hd;
                            return_status = sts;
                            goto putpage_error;
                        }
                    }
                    /* From here on (in this while loop) we will not
                     * be pushing out this page. Set up the release
                     * flags and length once here for all cases.
                     */

                    MS_SMP_ASSERT(plist->pfs_off == current_offset);
                    release_flags=FP_DEFAULT;

                    if (pflags & FP_SYNCD) {
                        /* Put the page back on the syncer queue. */
                        release_flags=pflags|FP_RESYNC;
                    }
                    if (pflags & FP_VHAND) {
                        /* turn off the free flag */
                        release_flags = (pflags&(~FP_FREE))|FP_DEFAULT;
                    }

                    /* Grab the buflock now. We need it
                     * for testing the bsb-writeref 
                     */
                    if (scan_status & FPS_ST_CLEAN) {
                        /* This is the situation where we are freeing
                         * or invalidating a clean page that happens
                         * to also be in the middle of a range that is
                         * being migrated. To compound matters another
                         * caller raced us here and is trying to pin
                         * this page. At this point we are just going
                         * to give up on this page. The pinner is
                         * going to bring this page right back into
                         * the cache anyway. 
                         */

                        MS_SMP_ASSERT(pflags&(FP_INVAL|FP_FREE));
                        MS_SMP_ASSERT(migrate_in_progress);
                        MS_SMP_ASSERT(bsbuf);
                        MS_SMP_ASSERT(bsbuf->bsb_writeref > 0) ;
                        MS_SMP_ASSERT(current_page_length > 0);

                        err = fcache_page_release(fc_vminfo, 
                                                  current_page_length,
                                                  &plist,
                                                  release_flags);
                        MS_SMP_ASSERT(err == 0);
                        goto metaloop_end;

                    }

                    spin_lock(&bsbuf->bsb_lock);
                    if (bsbuf->bsb_writeref > 0) {

                        /* This is a racy test. The bsb_writeref may
                         * have gone to zero since our test above. We
                         * will catch it at the end of the loop and
                         * start over if it has now become
                         * eligible. NOTE if this happens we may have
                         * broken a contiguous i/o into peices. See
                         * more comments below ...
                         */

                        /* Log pages and meta-data pages share the
                         * same flush mechanism. We will set up a
                         * multiWait link structure (if this is a sync
                         * write) to indicate to the unpinner to flush
                         * the page. We must also determine if this is
                     
                         * a dirty 4K page should be very rare but not
                         * impossible. There is a window in get page
                         * where we are bringing in and setting the
                         * first PFDAT dirty before we get to the next
                         * PFDAT (in this case however the writeRef
                         * must/will always be up)
                         */
                    
                        /* Indicate to the unpinner a flush is needed */
                        if (!(pflags & FP_SYNCD)){
                            /* Don't bother for syncd since the page
                             * is recently modified
                             */
                            bsbuf->bsb_flags |= BSBUFFLG_FLUSHNEEDED;

                        }
                        spin_unlock(&bsbuf->bsb_lock);
                        
                        GPLOGIT(BSBUF_FLUSHNEEDED,vp,scan_offset,scan_size,pflags,0,
                                scan_status,bsbuf,DEFAULT_ACTION);

                        /* We can drop the bsBuf lock since we are
                         * holding the pages locked (actually the 1st
                         * page lock is good enough).  Once the
                         * flush_waiting bit is set the caller will
                         * know to flush the page. This flush will
                         * block in putpage waiting for the page lock
                         * until we have setup the multWait link
                         * structure and released the page.
                         */

                        if (pflags & FP_SYNC) {
                            /* We will set up a multWait link
                             * mechanism. This will allow the unpinner
                             * to start the flush of this bsBuf and
                             * then wake us at i/o completion.
                             */
                            if (!mwp) {
                                mwp = kmem_arena_alloc(advfs_multiWait_arena, 
                                                       M_WAITOK);
                                MS_SMP_ASSERT(mwp);
                                mwp->mw_magicid &= ~MAGIC_DEALLOC;
                                mwp->mw_outstandingEvents = 0;
                            }
                        
                            mwlp = (multiWaitLinkT *)
                                ms_malloc(sizeof(multiWaitLinkT));
                            mwlp->mwl_mw = mwp;
                            spin_lock(&mwp->mw_lock);
                            mwp->mw_outstandingEvents++;
                            spin_unlock(&mwp->mw_lock);
                            spin_lock(&bsbuf->bsb_lock);
                            mwlp->mwl_fwd = (struct multiWaitLink *)
                                            bsbuf->bsb_mwllist;
                            bsbuf->bsb_mwllist = mwlp;
                            spin_unlock(&bsbuf->bsb_lock);
                            GPLOGIT(MULTIWAIT_SETUP,vp,scan_offset,scan_size,pflags,0,
                                    scan_status,bsbuf,DEFAULT_ACTION);
                        }
                    
                        /* We could be racing a getpage and see only a
                         * single 4K dirty PFDAT. Getpage could have
                         * found the 1st PFDAT as a new cache page,
                         * marked it dirty and released it. Then it
                         * finds the next PFDAT clean and in the
                         * cache. We could hit in between these
                         * lookups (This can only happen it the
                         * writeRef is up) and can ONLY happen on the
                         * 1st PFDAT of a metadata page and hence the
                         * last page in the blkmap range.  
                         *
                         * The other possibility is that VHAND or
                         * SYNCD was handed a single PFDAT since the
                         * other PFDAT making up the large page
                         * couldn't be locked (or it was referenced
                         * recently - VHAND) For VHAND we'll mark it
                         * to be flushed by the unpinner.
                         */

                        GPLOGIT(INELIGIBLE_METAPAGE,vp,current_offset,
                                current_page_length,pflags,0,
                                scan_status,bsbuf,RARE_EVENT);
                        MS_SMP_ASSERT(current_page_length > 0);

                        err = fcache_page_release(fc_vminfo, 
                                                  current_page_length,
                                                  &plist,
                                                  release_flags);
                        MS_SMP_ASSERT(err == 0);
                        
                    } else if ((current_page_length < ADVFS_METADATA_PGSZ) ||
                               ((bfap->dataSafety != BFD_LOG) &&
                                (bsbuf->bsb_metafwd != NULL) &&
                                (LSN_GT(bsbuf->bsb_currentlogrec.lsn, 
                                        dmnp->ftxLogP->hiFlushLsn)))) {
                        /* The above mentioned racy test comment
                         * continues here. The bsb_writeref may have
                         * gone to zero after we decided not to flush
                         * this bsBuf or the log's hiFlush LSN may now
                         * be high enough. So we basically want to
                         * retest to see if the page is still not
                         * eligible. We want to know that failing this
                         * statement means this page is now eligible.
                         *
                         * If the writeRef went to zero, most likely
                         * the bsBuf's LSN will be too low and we will
                         * fall into this if statment. If not we will
                         * reloop with out advancing the loop counters
                         * causing us to reprocess this page
                         * again. Also the log could have been flushed
                         * by the time we come to this if statement so
                         * here too we will reloop. See more comments
                         * below ...
                         */

                        MS_SMP_ASSERT(bsbuf->bsb_writeref == 0);

                        spin_unlock(&bsbuf->bsb_lock);

                        if (!(pflags & (FP_VHAND|FP_SYNCD))) {
                            size_t size_to_release;
                            /* This is a regular flush. We have an LSN
                             * mismatch. We cannot flush this
                             * meta-data page until the log is
                             * flushed. We need to first release ALL
                             * remaining pages, flush the log and then
                             * start the scanning loop over from where
                             * we left off.
                             */
                            lsnT flushLSN = bsbuf->bsb_currentlogrec.lsn;

                            size_to_release = scan_offset + scan_size - 
                                              current_offset + meta_scan_size;

                            GPLOGIT(INELIGIBLE_METAPAGE,vp,current_offset,
                                    size_to_release,pflags,0,
                                    scan_status,bsbuf,NEED_LOG_FLUSH);
                            MS_SMP_ASSERT(size_to_release > 0);
                            MS_SMP_ASSERT(current_page_length == ADVFS_METADATA_PGSZ);

                            err = fcache_page_release(fc_vminfo, 
                                                      size_to_release, /* all pages left */
                                                      &plist,
                                                      FP_DEFAULT);
                            MS_SMP_ASSERT(err == 0);
                            MS_SMP_ASSERT(plist == NULL);
                    
                            /* We have dropped all page locks. We will
                             * now flush the log in-line and loop back
                             * out to the top of the pagescan loop to
                             * start over at this page. NOTE: This
                             * assumes an FP_ASYNC caller can tolerate
                             * waiting for the log to flush!
                             *
                             * Since we hold no locks we really can't
                             * trust the LSN of the bsbuf that is
                             * causing this log flush. The log could
                             * (unlikely) have moved to two or more
                             * LSN quadrants and this LSN is not
                             * longer valid for comparision. Analysis
                             * of the log flush code shows us that it
                             * is benign.
                             */

                            GPLOGIT(LOGFLUSH_STARTED,vp,scan_offset,scan_size,pflags,0,
                                    scan_status,0,DEFAULT_ACTION);
                            
                            /* Should we honor FP_NOWAIT here? */
                            advfs_lgr_flush(dmnp->ftxLogP, flushLSN, FALSE);
                    
                            /* Make the scan loop look like it stops at
                             * this page so that the outer page scan
                             * loop counter we will start the rescan at
                             * this page.
                             */

                            scan_size = current_offset - scan_offset;
                            meta_scan_size = 0;
                            goto loop_end; /* We need to break out to
                                             * the scan loop */
                        } else {
                            /* This is SYNCD or VHAND. Release the page now. */

                            lsnT flushLSN = bsbuf->bsb_flushseq;

                            GPLOGIT(INELIGIBLE_METAPAGE,vp,current_offset,
                                    current_page_length,pflags,0,
                                    scan_status,bsbuf,INFORM_THREAD);
                            MS_SMP_ASSERT(current_page_length > 0);

                            err = fcache_page_release(fc_vminfo, 
                                                      current_page_length,
                                                      &plist,
                                                      release_flags);
                            MS_SMP_ASSERT(err == 0);

                            if (pflags & FP_VHAND) {
                                /* VHAND can not block waiting to
                                 * flush the log. We will send a
                                 * message to the kernel thread
                                 * instructing it to issue a flush on
                                 * this meta-data page. This will go
                                 * thru the non-vhand path above cause
                                 * a synchronous log flush.
                                 */

                                if (pflags&FP_NOWAIT)
                                {
                                    meta_msg = (advfs_metaflush_thread_msg_t *)
                                        msgq_alloc_msg_nowait(advfs_metaflush_msg_queue);
                                }else {
                                    meta_msg = (advfs_metaflush_thread_msg_t *)
                                        msgq_alloc_msg(advfs_metaflush_msg_queue);
                                }
                                /* If we couldn't get memory we'll just skip this an
                                 * go on.
                                 */
                                if (meta_msg != NULL) {
                                    MS_SMP_ASSERT(meta_msg != NULL);
                                    GPLOGIT(MESSAGE_SENT,vp,rounddown(current_offset, 
                                      bfap->bfPageSz * ADVFS_FOB_SZ),bfap->bfPageSz * ADVFS_FOB_SZ,
                                            pflags,0,scan_status,0,DEFAULT_ACTION);
                                    
                                    meta_msg->amf_msg_type = AMFE_META_FLUSH;
                                    meta_msg->amf_data.amf_meta_flush.amf_meta_tag = 
                                        bfap->tag;
                                    meta_msg->amf_data.amf_meta_flush.amf_bf_set_id = 
                                        bfap->bfSetp->bfSetId;
                                    if (bfap->dataSafety == BFD_METADATA) {
                                        meta_msg->amf_data.amf_meta_flush.amf_start.amf_offset = 
                                            rounddown(current_offset, bfap->bfPageSz * ADVFS_FOB_SZ);
                                    } else {
                                        meta_msg->amf_data.amf_meta_flush.amf_start.amf_lsn = flushLSN;
                                    }                                    
                                    meta_msg->amf_data.amf_meta_flush.amf_size = 
                                        bfap->bfPageSz * ADVFS_FOB_SZ;
                                    msgq_send_msg(advfs_metaflush_msg_queue, meta_msg);
                                } 
                            }
                        }
                    } else {
                        /* The above mentioned racy test comment
                         * finally ends here. We arrive here only if a
                         * race occured and the page is now eligible
                         * for i/o. The bsb_writeref is now zero and
                         * the LSN is eligible. We will not advance
                         * the loop counters causing this page to be
                         * processed again and pushed out! We will
                         * obtain the bsBuf again since we need the
                         * hashchain lock.
                         *
                         * At this point the bsb_writeref could actually
                         * be greater than zero. This is OK because
                         * we know that it went greater than zero
                         * after we locked the pages (called page_scan)
                         * This means the thread is blocked at page_alloc
                         * waiting for these pages and can not and has
                         * not modified them, so it is safe to flush the
                         * page at this point.
                         */

                        spin_unlock(&bsbuf->bsb_lock);
                        MS_SMP_ASSERT(LSN_LTE(bsbuf->bsb_currentlogrec.lsn, 
                                              dmnp->ftxLogP->hiFlushLsn));

                        GPLOGIT(INELIGIBLE_METAPAGE,vp,current_offset,
                                current_page_length,pflags,0,
                                scan_status,bsbuf,BECAME_ELIGIBLE);
                        /* Inidicate where to start doing i/o */
                        bsbuf_list_hd = NULL;
                        bsbuf_list_tl = NULL;
                        start_of_write = current_offset;

                        /* Skip the counter increment */
                        continue;
                    }
                    /* Move along to the next page in the plist */
metaloop_end:                    
                    current_offset      += current_page_length;
                    current_length      -= current_page_length;

                    /* There are no oustanding pages to write */
                    bsbuf_list_hd = NULL;
                    bsbuf_list_tl = NULL;
                    start_of_write = current_offset;
                }

                /* We have finished processing this blkmap
                 * entry. Check to see if we have accumulated ANY
                 * pages that can now be pushed out as a contigouos i/o
                 * before moving on the the next blkmap entry
                 */
                
                if (start_of_write < current_offset) {
                    /* We have some bsBufs accumulated so lets
                     * kick off this I/O. We will pass the copy
                     * extent map (which may be NULL) and if a
                     * migrate is in progress the
                     * advfs_start_blkmap_io routine will handle
                     * doing double i/o's
                     */

                    MS_SMP_ASSERT(((current_offset - start_of_write) & 
                                  (ADVFS_METADATA_PGSZ -1)) == 0);

                    GPLOGIT(STARTED_METADATA_WRITE,vp,start_of_write,
                            current_offset - start_of_write,pflags,0,
                            scan_status,bsbuf_list_hd,DEFAULT_ACTION);

                    sts = advfs_start_blkmap_io(fc_vminfo,
                                                vp,
                                                start_of_write,
                                                current_offset - start_of_write,
                                                blkmap_entry,
                                                &copy_blkmap,
                                                &ioAnchor_head,
                                                &plist,
                                                &bsbuf_list_hd,
                                                pflags,
                                                0);
                    if (sts != EOK) {
                        bsbufs_to_clear = bsbuf_list_hd;
                        PPAGE_STATS(metaflush_error2);
                        return_status = sts;
                        goto putpage_error;
                    }
                }
            }

            blkmap_entry = blkmap_entry->ebd_next_desc;
        }
        if ((plist != NULL) &&
            ((scan_size - (plist->pfs_off - scan_offset) > 0))) {
            /* In the case of clean pages and a migrate in
             * progress we replaced the original blkmap with the
             * copy blkmap in order to push the clean pages to the
             * copy storage. This copy map may have only been a
             * subset of the original blkmap and there could be
             * some pages left over. We want to release these
             * pages as though we are processing whatever request
             * has come in (this will be either a VHAND or a
             * flush and FP_INVAL type request)
             */
            size_t size_to_release = scan_size - (plist->pfs_off - scan_offset);

            MS_SMP_ASSERT(scan_status & FPS_ST_CLEAN);
            MS_SMP_ASSERT(migrate_in_progress);
            MS_SMP_ASSERT(scan_size > blkmap_size);
            
            if (metadata && (pflags&(FP_INVAL|FP_FREE))) {
                /* We need to clean up bsBufs here. VM will always
                 * call into putpage when invalidating or freeing
                 * clean/diry pages. This is our chance to clean up
                 * any filesystem specific structures.
                 */
                size_t meta_size = size_to_release;
                size_t saved_meta_size = meta_size;

                size_to_release = advfs_process_bsbufs_for_freeing (fc_vminfo,
                                                        vp, 
                                                        plist->pfs_off, 
                                                        &meta_size, 
                                                        unaligned,
                                                        &plist, 
                                                        pflags );
                if (saved_meta_size < meta_size) {
                    /* advfs_process_bsbufs_for_freeing found that this list
                     * ended on a non-aligned metapage boundary.
                     * We need to start up the scan back on
                     * an aligned boundary.
                     */

                    scan_size -= (meta_size - saved_meta_size);
                }
            }
            if (size_to_release > 0) {
                err = fcache_page_release(fc_vminfo, 
                                          size_to_release,
                                          &plist,
                                          pflags);
            
                MS_SMP_ASSERT(err == 0);
                MS_SMP_ASSERT(plist == NULL);
            }
        }

    loop_end:
        
        /* only release if we locked it here in putpage */
        if (extent_map_locked) {
            if (pflags & FP_VHAND) {
                ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
            } else {
                ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk ); /* Release the xtntmap lock */
            }
            extent_map_locked = FALSE;
        }
        
        /* Begin the next scan where we left off */
        current_request_offset = scan_offset + scan_size;
        if ((metadata) && 
            (current_request_offset&(ADVFS_METADATA_PGSZ - 1))) {
            /* We could have processed a 4K clean page causing or
             * offset to become unaligned. We can NEVER scan for
             * meta-data in non-allocation units!!! This would allow a
             * portion of a dirty meta-data page to be found and
             * flushed. It is correct to move forward in the file
             * since the processing of a 4K meta-data page means the
             * other 4K was efectively processed.
             */

            current_request_offset += VM_PAGE_SZ;
        }
        remaining_request_size = ending_request_offset - current_request_offset;
        if (blkmap != NULL) {
            (void)advfs_free_blkmaps(&blkmap);
            if (copy_blkmap != NULL) {
                (void)advfs_free_blkmaps(&copy_blkmap);
            }
        }
    }
 putpage_error:
    if (unlock_snap_lock) {
        ADVRWL_BFAP_SNAP_UNLOCK( bfap );
    }

    if (return_status != EOK) {
        if (bsbufs_to_clear != NULL) {
            /* We need to clear out the pointers in the bsbufs.
             * This field is expected to be NULL. We could do
             * this on every write but we'll let the infrequent
             * error path do the work.
             */
            struct bsBuf *tbsbuf;
            tbsbuf=bsbufs_to_clear;
            while(bsbufs_to_clear != NULL) {
                bsbufs_to_clear=tbsbuf->bsb_iolistfwd;
                tbsbuf->bsb_iolistfwd = NULL;
                tbsbuf = bsbufs_to_clear;
            }
        }
        
        /* We could not do the i/o. We need to release
         * these pages and error out.
         */
        if (plist != NULL) {

            err = fcache_page_release(fc_vminfo, 
                                      0UL,
                                      &plist,
                                      FP_DEFAULT);
                    
            MS_SMP_ASSERT(err == 0);
            MS_SMP_ASSERT(plist == NULL);
#ifdef PUTPAGE_RETURN_SIZE_AND_OFFSET
            /* Update the returned size */
            *size -= remaining_request_size;
#endif
        }
        if (blkmap != NULL) {
            (void)advfs_free_blkmaps(&blkmap);
            if (copy_blkmap != NULL) {
                (void)advfs_free_blkmaps(&copy_blkmap);
            }
        }
    }

    if ((update_mod_times) && (contextp)) {
        timestruc_t mod_time;

        ADVFS_GET_SYSTEM_TIME( mod_time );


        /*
         * Update the ctime and mtime in memory.  We will set the
         * dirty_stats so these times go out on next call to 
         * fs_update_stats.  We don't call fs_update_stats here because
         * it starts a transaction. 
         * Posix requires that an MSYNC cause stats to
         * be updated. 
         */
        ADVSMP_FILESTATS_LOCK(&contextp->fsContext_lock);
        contextp->fs_flag &= ~(MOD_CTIME | MOD_MTIME);
        contextp->dir_stats.st_mtime = mod_time.tv_sec;
        contextp->dir_stats.st_nmtime = (int32_t)mod_time.tv_nsec;
        contextp->dir_stats.st_ctime = mod_time.tv_sec;
        contextp->dir_stats.st_nctime = (int32_t)mod_time.tv_nsec;
        contextp->dirty_stats = TRUE;
        ADVSMP_FILESTATS_UNLOCK(&contextp->fsContext_lock);
    }
    
    if (extent_map_locked) {
        if (pflags & FP_VHAND) {
            ADVRWL_VHAND_XTNTMAP_UNLOCK(&bfap->vhand_xtntMap_lk);
        } else {
            ADVRWL_XTNTMAP_UNLOCK(&bfap->xtntMap_lk);
        }
        extent_map_locked = FALSE;
    }

    /* We have processed the flush of the requested range. Last thing
     * to do is wait for any i/os we started if the caller specified
     * the FP_SYNC flag 
     */

    if (pflags & FP_SYNC) {
        if (ioAnchor_head != NULL) {
            /* wait on the buf stored in each IO anchor */
            while (ioAnchor_head != NULL) {
                ioanchor_t *ioAnchor = ioAnchor_head;
                off_t file_offset = ioAnchor_head->anchr_origbuf->b_foffset;
            
                /* This will release the memory for the buf struct */
                err = fcache_buf_iowait(fc_vminfo,
                                        ioAnchor_head->anchr_origbuf,
                                        FBI_DEFAULT);
                if (err != 0) {
                    /* We must continue to wait for buffers even on an
                     * error so squirrel this away. We also need to 
                     * return the range that we succesfully faulted in
                     * so keep track of the minimum file offset that
                     * took an error. NOTE: We could already have an
                     * error such as ENOSPACE or EFAULT but we will
                     * override that with this error.
                     */

#ifdef PUTPAGE_RETURN_SIZE_AND_OFFSET
                    *size = MIN(file_offset - *off,*size);
#endif
                    return_status = err;
                }
                
                ioAnchor_head = ioAnchor_head->anchr_listfwd;
                advfs_bs_free_ioanchor(ioAnchor, M_WAITOK);
            }
        }

        /* Now wait for any meta-data pages that we requested
         * to be flushed
         */
        if (mwp) {
            spin_lock(&mwp->mw_lock);
            while (mwp->mw_outstandingEvents) {
                cv_wait(&mwp->mw_cv, &mwp->mw_lock, CV_SPIN, CV_DFLT_FLG);
            }
            spin_unlock(&mwp->mw_lock);
            mwp->mw_magicid |= MAGIC_DEALLOC;
            kmem_arena_free(mwp, M_WAITOK);
        }
    }
    return( return_status );
}

/* This routine takes a list of pfdats and returns a subset of
 * this list. The subset will be the largest run of pages that
 * have the same state (clean or dirty).
 * The metadata is on 8K boundaries so any dirty page in an
 * 8K range consititutes a dirty 8K page.
 */
fps_status_t
parse_meta_list(
    size_t *scan_size,
    page_fsdata_t *plist)
{
    
    fps_status_t state;
    fps_status_t current_state;
    size_t size;
    MS_SMP_ASSERT(plist != NULL);
    *scan_size = parse_meta_page(&current_state,&plist);
    state = current_state;
    while (plist) {
        size = parse_meta_page(&current_state,&plist);
        if (current_state != state) break;
        *scan_size += size;
    }
    return(state);
}
/* This routine will parse the first metapage on the plist.  It will
 * return the state of the page (clean or dirty), the size of the page
 * (4K or 8K) and it will advance the plist to point to the next page
 * (or NULL) after this metadata page.
 *
 * NOTE: This routine assumes 8K metadata page size. It will
 * need to be modified if we go to a different page size.
 */
size_t
parse_meta_page(fps_status_t *state, page_fsdata_t **plistp)
{
    size_t size = VM_PAGE_SZ;
    *state = (((*plistp)->pfs_info.pi_pg_dirty)
              ?FPS_ST_DIRTY:FPS_ST_CLEAN);
    if ((*plistp)->pfs_off & (ADVFS_METADATA_PGSZ -1)) {
        /* This is the last 4K page of a metapage.
         * Advance the plist and return the state of
         * this page as the state of the metapage
         */
        (*plistp) = (*plistp)->pfs_next;
        return(size);
    }
    if (*state & FPS_ST_DIRTY) {
        /* It only takes one dirty page to call all 8K dirty.
         * We know this is the first page so advance the plist
         * 2 pages if we can.
         */
        (*plistp) = (*plistp)->pfs_next;
        if ((*plistp)) {
            (*plistp) = (*plistp)->pfs_next;
            size = ADVFS_METADATA_PGSZ;
        }
        return(size);
    }
    /* To get here means its a clean 4K page */
    (*plistp) = (*plistp)->pfs_next;
    if ((*plistp) == NULL) {
        return(size);
    } else {
        *state = (((*plistp)->pfs_info.pi_pg_dirty)
                  ?FPS_ST_DIRTY:FPS_ST_CLEAN);
        (*plistp) = (*plistp)->pfs_next;
        size = ADVFS_METADATA_PGSZ;
        return(size);
    }
}

/* This is for the freeing of meta-pages. This routine will process
 * any bsbufs that are associated with the pages being freed. If a
 * race occurs where a bsbuf gets pinned (writeref'ed) the passed in
 * plist * will be modified since pages will need to be released. The
 * number of * pages remaining to be released will be returned. The
 * scan_size which * represents the number of pages that have been
 * processed may be changed * to force the next loop to revisit the
 * last page, if the passed in range * was not meta-page aligned (ie
 * ended on a 4K boundary).
 */ 

size_t
advfs_process_bsbufs_for_freeing (
    fcache_vminfo_t *fc_vminfo,
    struct vnode *vp, 
    off_t scan_offset, 
    size_t *scan_size, 
    uint32_t unaligned,
    page_fsdata_t **plistp, 
    fcache_pflags_t pflags )
{
    int err,removed_buf;
    off_t meta_off = scan_offset;
    size_t meta_size = *scan_size;
    size_t remove_size, size_to_release;
    page_fsdata_t *plist;

    /* We need to clean up bsBufs here. VM will always call into
     * putpage when invalidating or freeing clean/diry pages. This is
     * our chance to clean up any filesystem specific structures.
     */
    plist = *plistp;
    size_to_release = *scan_size;

    while (meta_size > 0) {
        int lookup;
        struct bsBuf *bsbuf;
        void *cookie;
        /* NOTE: There is a dependency here that the metadata page
         * size is 8K (2 4K VM pages).
         */
        if (meta_off & (ADVFS_METADATA_PGSZ - 1)){
            /* This is not aligned. We have a partial page */
            
            remove_size = VM_PAGE_SZ;
        } else {
            remove_size = MIN(meta_size,ADVFS_METADATA_PGSZ);
        }
        /* We could race with bs_pinpg. So we first need to check if
         * there is a bsBuf and if so is the writeref up.  if it is
         * then we will just skip over this page.
         */
        lookup = TRUE;
        bsbuf = advfs_obtain_bsbuf(vp,
                                   meta_off,
                                   &cookie,
                                   &lookup);   /* May return with hashchain locked */
        /* The hashchain lock is only held if a bsbuf was returned to
         * us.
         */

        MS_SMP_ASSERT((!(bsbuf)) ||
                      (bsbuf->bsb_metafwd == NULL) ||
                      (remove_size < ADVFS_METADATA_PGSZ));
        
        if ((bsbuf) && 
            (bsbuf->bsb_writeref == 0) &&
            (bsbuf->bsb_metafwd == NULL)) {

            /* We can only allow a bsbuf to be removed from the hashtable if
             * it is clean. In the case of VHAND or if the pages returned from
             * page alloc get clipped on a non metadata_pgsz boundary we could
             * encounter a clean page that is really paired up with a dirty page.
             * We can not allow this clean page to be stolen since we would be left
             * with a single dirty page that we could never flush.
             */

            GPLOGIT(FREE_CLEAN,vp,meta_off,remove_size,pflags,0,
                    0, bsbuf,REMOVING_BSBUF);

            removed_buf = advfs_remove_bsbuf(vp,
                                             meta_off,
                                             remove_size,
                                             cookie); /* unlocks hashchain */
            
        } else if ((bsbuf) && 
                   ((bsbuf->bsb_writeref > 0) ||
                    (bsbuf->bsb_metafwd != NULL))) {
            /* Someone is in pinpg attempting to access this
             * page. This should not be a request to invalidate the
             * page since it is our assumption here that the caller
             * must be stopping any pinners during this
             * invalidation. A free on the otherhand can come in and
             * find this situtation (VHAND). So in this case lets just
             * ignore this page and move on.  NOTE we are still
             * holding the hash chain lock.
             */
            
            /* Release the hash chain lock */

            BS_BSBUF_HASH_UNLOCK(bsbuf->bsb_hashlink.dh_key);
            MS_SMP_ASSERT(!(pflags&FP_INVAL));
            GPLOGIT(FREE_CLEAN,vp,scan_offset,
                    size_to_release- meta_size,pflags,0,
                    0,bsbuf,ENCOUNTERED_WRITEREF);
            PPAGE_STATS(encountered_writeref);

            /* Release all the pages we have accumulated so far with
             * the passed in flags
             */
            if ( size_to_release > meta_size ) {
                err = fcache_page_release(fc_vminfo, 
                                          size_to_release - meta_size,
                                          &plist,
                                          pflags);
                MS_SMP_ASSERT(err == 0);
                /* Update the release count to reflect the fact that
                 * we processed part of the list
                 */
                size_to_release = meta_size;
            }

            if (pflags & FP_VHAND) {
                bfAccessT *bfap = VTOA(vp);
               /* To be consistent we will inform the pinner that at unpin
                 * time this metapage should be flushed on behalf of VHAND,
                 */
                if (bsbuf->bsb_writeref > 0) {
                    spin_lock(&bsbuf->bsb_lock);
                    bsbuf->bsb_flags |= BSBUFFLG_FLUSHNEEDED;
                    spin_unlock(&bsbuf->bsb_lock);
                } else if (bfap->dataSafety == BFD_METADATA){
                    /* To be consistent we will send of a message to the
                     * kernel thread to flush this page on behalf of
                     * VHAND. This is really a dirty page so as we do in the
                     * dirty path of putpage we will fire off a message.
                     * For the log we will just skip this.
                     */
                    advfs_metaflush_thread_msg_t *meta_msg;
                    if (pflags&FP_NOWAIT)
                    {
                        meta_msg = (advfs_metaflush_thread_msg_t *)
                            msgq_alloc_msg_nowait(advfs_metaflush_msg_queue);
                    }else {
                        meta_msg = (advfs_metaflush_thread_msg_t *)
                            msgq_alloc_msg(advfs_metaflush_msg_queue);
                    }
                    /* If we couldn't get memory we'll just skip this an
                     * go on.
                     */
                    if (meta_msg != NULL) {
                        MS_SMP_ASSERT(meta_msg != NULL);
                        GPLOGIT(MESSAGE_SENT,vp,rounddown(meta_off, 
                                                          bfap->bfPageSz * ADVFS_FOB_SZ),
                                bfap->bfPageSz * ADVFS_FOB_SZ,
                                pflags,0,0,bsbuf,DEFAULT_ACTION);
                        
                        meta_msg->amf_msg_type = AMFE_META_FLUSH;
                        meta_msg->amf_data.amf_meta_flush.amf_meta_tag = 
                            bfap->tag;
                        meta_msg->amf_data.amf_meta_flush.amf_bf_set_id = 
                            bfap->bfSetp->bfSetId;
                        meta_msg->amf_data.amf_meta_flush.amf_start.amf_offset = 
                            rounddown(meta_off, bfap->bfPageSz * ADVFS_FOB_SZ);
                        meta_msg->amf_data.amf_meta_flush.amf_size = 
                            bfap->bfPageSz * ADVFS_FOB_SZ;
                        msgq_send_msg(advfs_metaflush_msg_queue, meta_msg);
                    } 
                }
            }

            /* Release the page with the writeRef with the default
             * flags (don't free it).
             */
            
            MS_SMP_ASSERT(remove_size > 0);
            err = fcache_page_release(fc_vminfo, 
                                      remove_size,
                                      &plist,
                                      FP_DEFAULT);
            MS_SMP_ASSERT(err == 0);

            
            /* Update the release count to reflect the fact that we
             * processed part of the list
             */
            size_to_release -= remove_size;
        }
        
        /* We know that the total range we are scanning for MUST start
         * and end on meta-data allocation unit boudaries. Page_scan
         * could give us back a partial non-aligned range however,
         * since some of the pages in the range may have been stolen
         * by VHAND. Once we are on an aligned boundary page_scan
         * could then clip to range of pages returned to us (currently
         * they do this at 64K). This will cause a meta-page to be
         * split. It is ok for the range we are now processing to
         * start on a non-aligned boundary but it is NOT ok for it to
         * end on a non-aligned boundary without some
         * intervention. This is because we would loop back and start
         * our scan on an unaligned offset.  We must NEVER call
         * page_scan on a non-aligned boundary. Dirty meta-data pages
         * must always be scanned starting on the aligned offset for
         * synchronization with getpage. So we will process this last
         * unaligned page and adjust our loop counters to start over
         * beginning with this page. We will more than likely skip
         * right over this page, but a getpage could sneak in and mark
         * this page dirty in which case it is imperative that we
         * start a scan at the start and not in the middle of the
         * meta-data page.
         */
        if ((meta_size == VM_PAGE_SZ) &&
            (!(meta_off & (ADVFS_METADATA_PGSZ - 1))) &&
            (unaligned)) {
            
            /* If we are at the end of the list and all we have left
             * to process is a 4K page then release it (actually we
             * already did this above) and restart the scan at this
             * page. We only have to do this if the list started
             * unaligned and was more than a single 4K page. If this
             * list was aligned then the reason we end on a 4K page is
             * the next page was not in the cache.  The test above
             * says its a 4K page, the page itself is aligned but the
             * list scanned in was not aligned.
             */
            
            *scan_size -= meta_size;
            
            GPLOGIT(FREE_CLEAN,vp,meta_off,meta_size,pflags,0,
                    0,0,RESCANNING_FOR_META_ALIGNMEMT);
            
            PPAGE_STATS(rescanning_for_meta_alignmemt);
        }
        
        meta_size -= remove_size;
        meta_off += remove_size;
    }
    *plistp = plist;
    return (size_to_release);
}

/* If a domain has paniced we can have clean metadata pages
 * on the domain's LSN list. We need to at this point make 
 * any bsBuf appear dirty so that the correct processing 
 * will take place. This is inefficient but we are domain
 * paniced anyway.
 */

statusT
advfs_adjust_bsbuf_for_domain_panic(
    struct vnode *vp, 
    off_t scan_offset, 
    size_t *scan_size,
    fps_status_t *scan_status)
{
    bfAccessT *bfap = VTOA(vp);
    int err;
    off_t meta_off = scan_offset;
    size_t meta_size = *scan_size;
    size_t page_size;

    /* Only need to call this for clean metadata pages */
    MS_SMP_ASSERT(*scan_status == FPS_ST_CLEAN);
    MS_SMP_ASSERT(bfap->dmnP->dmn_panic);

    while (meta_size > 0) {
        int lookup;
        struct bsBuf *bsbuf;
        void *cookie;
        /* NOTE: There is a dependency here that the metadata page
         * size is 8K (2 4K VM pages).
         */
        if (meta_off & (ADVFS_METADATA_PGSZ - 1)){
            /* This is not aligned. We have a partial page */
            page_size = VM_PAGE_SZ;
        } else {
            page_size = MIN(meta_size,ADVFS_METADATA_PGSZ);
        }
        
        lookup = TRUE;
        bsbuf = advfs_obtain_bsbuf(vp,
                                   meta_off,
                                   &cookie,
                                   &lookup);   /* May return with hashchain locked */
        /* The hashchain lock is only held if a bsbuf was returned to
         * us.
         */
        
        if (bsbuf) {
            BS_BSBUF_HASH_UNLOCK(bsbuf->bsb_hashlink.dh_key);
            if (bsbuf->bsb_metafwd != NULL) {
                
                /* We have a bsBuf on the dirty list and yet the pfdat is
                 * clean. This situation may occur in paniced domains.
                 * Error paths could pin records and then not modify the
                 * page when a domain panic is sensed. Rather than try to
                 * hunt these down and also avoid them in the future, we
                 * will allow for this case and make the pfdats look dirty
                 * here.
                 * If this is the first page passed to this routine then
                 * we will return the size and new status of dirty. If we
                 * have already processed any pages, we will clip the chain
                 * of pfdats here and let putpage process this list as clean
                 * and then call back into this routine for this pfdat.
                 */
                
                PPAGE_STATS(domain_panic_marking_dirty);
                
                if (meta_off == scan_offset) {
                    /* Clip the passed in list to only include this page
                     */
                    
                    MS_SMP_ASSERT(*scan_size == meta_size);
                    *scan_size = page_size;
                    *scan_status = FPS_ST_DIRTY;
                    return(EOK); 
                } else {
                    /* We only want putpage to operate on the pages
                     * upto this page (not including)
                     */
                    
                    *scan_size -= meta_size;
                    return(EOK);
                }
            }
        }
        meta_size -= page_size;
        meta_off += page_size;
    }
    return(EOK);
}
 

/*
 * advfs_getpage:
 *
 * This routine will now be the focal point for reading on-disk data
 * into a UFC buffer. All file data (user and meta) will be brought in
 * to the UFC through this interface. Storage will be allocated by
 * this routine after all page locks are released but prior to
 * returning to the caller.
 *
 *
 * Where the possible fault types are:
 *
 *FCF_DFAULT_READ - This is a data read fault. The caller wants to
 *read the data in the file for the range specified by the off and
 *size parameter.
 *
 *FCF_DFAULT_WRITE - This is a data write fault. The caller wants to
 *write in the file for the range specified by the off and size
 *parameter.
 *
 * And the possible bflags are:
 *
 * FP_READ - Read data from the disk if necessary.
 *
 * FP_CREATE - This is a data write fault but the data does not have
 * to be brought in. In this case the caller will fill the data. The
 * pages will be created in the cache but not read from disk. The
 * pages should remain locked and will be unlocked by the caller after
 * the I/O/. One of FP_READ or FP_CREATE but not both must be set.
 *
 * FP_SYNC - Read data synchronously.
 *
 * FP_ASYNC - Read data asynchronously. One of FP_SYNC or FP_ASYNC
 * must be set but not both.
 *
 * FP_NOWAIT - Do not block for locks or memory or storage. If
 * FP_ASYNC is not set then it will block for i/o.
 *
 * FP_SEQUENTIAL - This specifies that the file will be accessed
 * sequentially. Typically this will happen when the user
 * (application) called the madvise() with the MADV_SEQUENTIAL. This
 * will be used as a hint to do read-ahead on adjacent blocks.
 *
 * Each page in VM is represented by a pfdat structure. Each pfdat
 * structure will contain file system data that can be manipulated by
 * each file system. The file system data is represented by the
 * following structure:
 *
 *struct page_fsdata {
 *		struct vnode		*pfs_vp;
 *		off_t			pfs_off;
 *		uintptr_t			pfs_pvt_data;
 *		pfs_info_t		pfs_info;
 *		page_fsdata_t		*pfs_next;
 *              };
 * Synchronization
 *
 * All callers must obtain the file lock in read mode. This will be
 * true for mmapers and read write system calls. This will allow for
 * those operations that need to add storage to the file to block out
 * threads by obtaining this lock exclusively. Mmapers will have this
 * lock obtained in this routine. Writers and readers will have
 * obtained the lock (generally in shared mode) prior to entering
 * this *routine.
 *
 * UFC pages will be protected from lookups after the call to
 * fcache_page_alloc and until the call to fcache_page _release or
 * biodone.  After the page is looked up VM will protect the page so
 * that any accesses will page fault and block in VOP_GETPAGE or
 * VOP_PUTPAGE. This means that multiple readers and writers may be
 * accessing a page after returning from a call to VOP_GETPAGE.
 *
 * Special synchronization will be provided for meta-data pages. A
 * write reference will be maintained in the bsBuf to synchronize
 * between VOP_GETPAGE and VOP_PUTPAGE operating on the same meta-data
 * page.  If storage needs to be allocated the file lock will be
 * upgraded to write mode.
 *
 * There are a few tricky synchronizations issues that need to be
 * dealt with. We need to upgrade the file lock to write mode when
 * ever we need to allocate storage to the file. We will query the
 * extent maps to see if the request will require any storage to be
 * added and if so immediately upgrade the write lock. If the upgrade
 * fails we will lose the read lock and wait for the write lock and
 * then requery the extent maps. This follows the Tru64 model of
 * holding the file for write the entire request. The alternative of
 * waiting to upgrade until we actually allocate the storage presents
 * very complicated backout conditions. This could be a future
 * enhancement.
 *
 * Large pages present a synchronization issue when trying to
 * determine if storage will need to be allocated. We do not know
 * ahead of time (ie prior to calling fcache_page_alloc) the range
 * that we may need to write to. This means after calling
 * fcache_page_alloc we may find that we now need to allocate storage
 * and therefore must obtain the write lock. This is problematic if we
 * fail to upgrade the lock. We can not hold pages and then attempt to
 * get the FILE lock. A truncation for example may have the FILE lock
 * and be waiting on the pages we have locked! So we have decided that
 * we will make every attempt to do the right thing but when presented
 * with a large page that causes us to need to upgrade the lock and we
 * fail the upgrade, we will release those pages that are outside of
 * our range with the B_ERROR flag (?) which will cause VM to demote
 * the large pages and us to continue.
 *
 * Storage will be added to the file after each call to
 * fcache_page_alloc. All pages brought in from the cache will be
 * released before storage is allocated. Storage will not be allocated
 * in advfs_getpage for meta-data writes. All meta-data storage will
 * be obtained prior to writing the file (i.e. we can assert that we
 * never encounter a hole). After the pages are released, another
 * advfs_getpage will block on the FILE lock thus protecting these
 * pages from being looked up until storage is allocated. A flush
 * could occur on these pages before we allocate storage. This will be
 * handled by skipping these pages in advfs_putpage. If a PFDAT is
 * about to be written to disk and the extent map for this page is
 * found to be a hole, the advfs_putpage routine release the page back
 * to the page cache. In effect treating the page as if we never found
 * it (we raced the storage adder and lost). A debug bit will be added
 * to each PFDAT(check_for_storage) that can be check when
 * advfs_putpage encounters a hole to assert that we are in this
 * situation.
 *
 * An mmaper could have already faulted a page having storage added to
 * it into the page cache. We will protect the page when we release
 * it. After storage has been added we will restore the permissions to
 * the page. Thus if the mmaper touches the page they will page fault
 * and block in advfs_getpage waiting on the FILE lock.
 *
 * The NO_WAIT option will be implemented so that any blocking lock
 * will be obtained in a LOCK TRY manner and if it fails the pages
 * that were processed will be returned as well as the EWOULDBLOCK
 * status. Furthermore if storage needs to be obtained, the call will
 * immediately return EWOULDBLOCK.
 *
 * This function will synchronize with migrate through the page
 * locks. A page can be read during a migration without knowledge that
 * the migrate is currently in progress. The read will use either the
 * original or the copy extent map. Regardless of which extent map is
 * chosen, the data will be present. If the read is started to the
 * original extent map, migrate will not deallocate that storage until
 * this read completes. This is accomplished by issueing a flush after
 * having switched to the new extent map, that will scan for all in
 * cache pages. This has the effect of waiting for any in flight
 * i/o's.
 *
 * The basic flow of the code will be to examine the extent maps to
 * understand the storage requirements of the request, bring in all
 * pages of the request into the cache, initialize or zero the cache
 * pages and release them, allocate storage and further zero any pages
 * allocated that are beyond the request.  advfs_getpage
 *
 * If the APP_ADDSTG_NOCACHE flag is set in fs_priv_param.app_flags, then
 * getpage is being called to merely add storage to a file range.  The
 * pages associated with this file range should not be brought into the
 * cache or zero'd.  When called with this flag, getpage, if storage is
 * successfully added, will set the value of fs_priv_param.app_stg_start_fob
 * to the first fob for which storage was added.  In addition, 
 * fs_priv_param.app_stg_end_fob will be set to the last fob for which 
 * storage was added.  If no storage was added, fs_priv_param.app_stg_end_fob
 * will be set to 0.
 *
 * If the APP_READAHEAD_FAST_PATH flag is set in fs_priv_param.app_flags,
 * then getpage is being called to merely kickoff a planned read-ahead.
 * It will skip the main loop and jump right to kicking off the desired
 * read-ahead.
 */
extern uint64_t advfs_cow_alloc_units;

int
advfs_getpage(
    fcache_vminfo_t *fc_vminfo,   /* An opaque pointer to a vm data struct*/
    struct vnode * vp,           /* The vnode pointer */
    off_t *off,                   /* The offset in the file of the fault */
    size_t *size,                 /* The size in bytes to fault in */
    fcache_ftype_t ftype,            /* The fault type */
    struct advfs_pvt_param *fs_priv_param, /* File system private parameter */
    fcache_pflags_t pflags )     /* Options or modifiers to the function */
{
    struct fsContext *contextp;
    bfAccessT *bfap;

    off_t getpage_req_off_start; /* Starting offset of this getpage request */
    off_t getpage_req_off_end;   /* Ending offset of this getpage request */
    size_t getpage_req_size;     /* Size of this getpage request */

                                 /* The following two variables should only 
                                  * be used by FP_CREATE paths.  They refer
                                  * not to the current getpage request but
                                  * to the full write() request, as
                                  * described by the fs_priv_param. */
    off_t write_req_off_start;   /* Starting offset of the write() request */
    off_t write_req_off_end;     /* Ending offset of the write() request */

    off_t current_loop_offset;
    size_t remaining_loop_size;
    size_t remaining_request_size;

    off_t fp_create_starting_offset; /* Keeps track of what is covered */
    off_t fp_create_ending_offset;   /* by the fp_create flag. Anything 
                                      * outside of this MUST be inited */

     int invalidate_pages=0;
    
    /*
     * If we error out, we need to reset return_offset and return_size to
     * either what was really faulted (if anything) or *off and *size (if
     * nothing was faulted).  In preg_vn_fault VM will ignore errors and
     * look at these values, so make sure we return reasonable values when
     * *off and *size are assigned return_offset and return_size at the end
     * of the routine.
     */
    off_t return_offset;
    size_t return_size;
    statusT return_status = EOK;
    page_fsdata_t *plist;
    fpa_status_t alloc_status;
    off_t alloc_offset;
    size_t alloc_size;
    off_t min_coff;
    off_t max_coff;
    fcache_pflags_t rel_flags=0;
    fcache_pflags_t inv_flags=0;

    int metadata;
    int protect_page;
    int did_io = FALSE;             /* TRUE if a read from disk was done */
    int dummy;

    ioanchor_t *ioAnchor_head=NULL; /* singley linked list of anchors to wait for */

    extent_blk_desc_t *sparseness_blkmap=NULL;
    extent_blk_desc_t *storage_blkmap=NULL;
    off_t sparseness_offset;
    size_t sparseness_size;
    off_t sparseness_end;
    off_t storage_offset;
    size_t storage_size;
    
    int writelock_locked=0;
    int unlock_filelock=FALSE;
    int xtntmap_locked=0;
    int release_pages=0;
    int first_pass = TRUE;
    uint64_t request_needs_storage=0;
    struct advfs_pvt_stg_desc *stg_list;

    off_t  file_size;
    off_t  potential_file_size;

    struct nameidata *ndp;

    statusT sts;
    int err;
    int allow_only_1_dio_obj_safety_allocation = 0;
    int first_check                  = TRUE;
    int addstg_nocache_specified     = FALSE;
    int we_setup_arp                 = FALSE;
    int arp_dropped_lock;
    int mmapped_file                 = FALSE;
    struct actRange *arp             = NULL;
    readahead_flags_t ra_flags       = 0;    /* Flags to read-ahead functions */

    /* Snapshots related fields */
    char        process_snaps = FALSE;
    int16_t     snap_tokens_revoked = FALSE;
    int16_t     check_page_protections = FALSE;
    ftxHT       cow_ftx = FtxNilFtxH;
    ftxHT       parent_ftx;
    off_t       req_start_before_cow;
    off_t       req_end_before_cow;
    extent_blk_desc_t*  snap_maps = NULL;
    extent_blk_desc_t*  bfap_smap_storage_head = NULL;
    extent_blk_desc_t*  bfap_smap_storage_tail = NULL;
    fcache_pflags_t     cow_cleared_pflags; 
    int16_t     forced_cow = FALSE;
    int16_t     bumped_writecnt = FALSE;
    off_t       min_child_stg = 0;
    off_t       max_child_stg = 0;
    int16_t     cleanup_snaps = FALSE;
    int16_t     cow_ftx_started = FALSE;
    int16_t     mig_stg_locked = FALSE;
    off_t       orig_sparseness_offset;
    size_t      orig_sparseness_size;
    uint32_t    delCnt = 0;      /* Used to deferred deletion of prealloc stg */
    void        *delList = NULL; /* Used to deferred deletion of prealloc stg */

    /* vfast related fields */
    struct timeval new_time;
    int32_t wday, totHotCnt;


    ftxHT ftxH;

    int is_a_small_file = FALSE,   
        will_remain_a_small_file = FALSE,
        small_file_already_had_storage = FALSE,
        first_fcache_page_alloc_call = TRUE,
        small_au_file = FALSE;
    size_t size_to_read,
           size_to_zero;

    bfap = VTOA(vp);

    /* We should not be here for a shadow access struct.  A mapping was setup
     * incorrectly.
     */
    MS_SMP_ASSERT(&bfap->bfVnode== vp);

    /* We don't really support this very well here */
    MS_SMP_ASSERT(!(pflags&FP_NOWAIT));
    
    metadata = (bfap->dataSafety == BFD_METADATA) || 
        (bfap->dataSafety == BFD_LOG);


    /* Early fail for out of sync snapshots */
    if (bfap->bfaFlags & BFA_OUT_OF_SYNC) {
        /* 
         * We need to indicate a size of zero so VM knows we had an error.
         * Otherwise, they may ignore our error.
         */
        *size = 0;
        MS_SMP_ASSERT( cow_ftx_started == FALSE );
        return E_OUT_OF_SYNC_SNAPSHOT;
    }

    contextp = VTOC( vp );

    ndp = NAMEIDATA();

    
    /*
     * Setup Private Params passed information
     */
    if (fs_priv_param) {

        /* Setup a list of added storage to pass back */
        stg_list = &fs_priv_param->stg_desc;

        /* snapshot work to be done? */
        if ( (fs_priv_param->app_flags & APP_FORCE_COW)) {
            forced_cow = TRUE;
        }

        /* Is Direct I/O asking to only add storage? */
        addstg_nocache_specified = 
            (fs_priv_param->app_flags & APP_ADDSTG_NOCACHE) ? TRUE : FALSE;
        
        /* Get the Active Range arp pointer; it could be NULL */
        arp = fs_priv_param->arp;

    }

    /* For an mmaper we need to get the lock. The NI_RW is not
     * enough to know that it is NOT an mmaper. A read/write system
     * call could pass us an mmap address as the buffer to read from or
     * write into. So if the NI_RW is set we will then check to
     * see if the ni_vp is the same as the passed in vp. If it is then
     * we are already holding the lock for this file. If not then we
     * need to get the lock
     */
    /* NOTE!!! We have a deadlock scenario that we must solve that
     *         may alleviate the need for the following code.
     */

    if ((!metadata) &&
        (!(ndp->ni_nameiop & NI_RW) || 
         ((ndp->ni_vp != vp)))) {
        /*
         * If this is an mmap writer, bump the bfaWriteCnt and syncrhonize
         * with snapshot creation before acquiring the file lock.
         */
        if (ftype & FCF_DFLT_WRITE) {
            ADVFS_SNAPSHOT_CREATE_SYNC( bfap );
            bumped_writecnt = TRUE;
        }


        mmapped_file = TRUE;

        /* Don't get the file lock for meta-data. Synchronization is
         * provided at a higher level.
         */

        MS_SMP_ASSERT(!ADVRWL_FILECONTEXT_ISWRLOCKED(contextp));

        if (pflags & FP_NOWAIT) {
            if (!ADVRWL_FILECONTEXT_READ_TRY_LOCK(contextp)) {
                return_status = EWOULDBLOCK;
                return_offset = *off;
                return_size = *size;
                goto popsicle_stand;
            }
        } else {
            ADVRWL_FILECONTEXT_READ_LOCK(contextp);
        }
        unlock_filelock = TRUE;
    }
    
    /*
     * See if a parent transaction was passed in.
     */
    parent_ftx = (fs_priv_param != NULL) ? fs_priv_param->app_parent_ftx :
                                                FtxNilFtxH;

start_over:

    if (metadata && (fs_priv_param == NULL) ) {
        /* 
         * This is an implicit fault on metadata.  No COWing should be
         * necessary since this must have been COWed already.  Starting a
         * transaction here could cause a deadlock condition. There could be
         * no snapshots being created because we syncrhonized above and
         * because this is should be under transaction.
         */
        process_snaps = FALSE;
    } 
    else if ( !addstg_nocache_specified ) {
        /* 
         * We may need to process snapshots for both reads and writes (for
         * read locks need to be acquired in the parents.  For writes,
         * additional locking is required.) 
         *
         * NOTE:  APP_ADDSTG_NOCACHE requests (Direct I/O and storage
         *        preallocation) always call advfs_force_cow_and_unlink()
         *        _BEFORE_ calling advfs_getpage(), so any snapshot
         *        copy-on-write work has already been done.  Thus when
         *        APP_ADDSTG_NOCACHE requests occur, the snapshot code does
         *        not need to be executed in this path.
         */
        process_snaps = HAS_RELATED_SNAPSHOT( bfap ) && 
            ( (!metadata) || (IS_COWABLE_METADATA( bfap )) );  

        /*
         * If a parent ftx was passed in but the type is not normal, then we
         * must be doing something like an undo or redo.  If we are doing
         * one of thoese, we should have no COWing to process since it was
         * COWed when the data was first modified under the transaction.  If
         * we allow COWing now, the cow_ftx done routine will panic because
         * the time is not normal and a transaction is being doned
         */
        if (process_snaps && !FTX_EQ(parent_ftx, FtxNilFtxH) &&
            (bfap->dmnP->ftxTbld.tablep[parent_ftx.hndl - 1].type != NORMAL)) {
            process_snaps = FALSE;
        }

        if (process_snaps && 
            fs_priv_param && 
            (fs_priv_param->app_flags & APP_MIGRATE_FAULT)) {
            /* We are trusting migrate to have correctly acquired all locks
             * and to only be faulting locally mapped storage.  
             * There SHOULD be nothing to do for snapshots.
             */
            process_snaps = FALSE;
            MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->xtnts.migStgLk,
                                                          RWL_WRITELOCKED));
        }

    }

    if (!metadata) {
        file_size =  bfap->file_size;
        /*
         * See if the file has an allocation unit less than a VM page size
         * and has less storage allocated to it than a VM page.  If so, we 
         * will consider it to be a "small file".  If it is a small file,
         * then see if it will still be a small file after the current 
         * call to advfs_getpage() completes.  It will remain a small file
         * if this is a read or if it's a write() that keeps the file less
         * than VM_PAGE_SZ bytes long or if it's an mmap write.  An mmap
         * write will call in with offset 0 and size 4096 (VM_PAGE_SZ), but
         * we won't let such an mmap write extend the file.
         */
        small_au_file = ADVFS_FILE_HAS_SMALL_AU(bfap);
        if (is_a_small_file = ADVFS_SMALL_FILE(bfap)) {
            if ((ftype == FCF_DFLT_READ) ||
                ((roundup((*off + *size), bfap->bfPageSz * ADVFS_FOB_SZ) < 
                 VM_PAGE_SZ) || (mmapped_file))) {
                will_remain_a_small_file = TRUE;
            }
            if (bfap->bfaNextFob > 0) {
                small_file_already_had_storage = TRUE;
            }

            /*
             * If this is a write which would cause a hole to exist 
             * in the first VM_PAGE_SZ bytes of the file, reset the 
             * getpage request to include that hole since the rules 
             * of small files state that we will never have a hole 
             * less than VM_PAGE_SZ bytes in size.
             */
            if (ftype & FCF_DFLT_WRITE) {

                /*
                 * If the current write starts at an offset that will
                 * leave a VM_PAGE_SZ aligned hole in the file, 
                 * and the file already has some storage in it,
                 * issue a recursive call to advfs_getpage() to fill in
                 * the hole in the first page.  We could do this for the
                 * case where we are writing into the first or second page,
                 * too, but those cases are handled more efficiently by just
                 * resetting the getpage request to include the adjacent
                 * hole.  
                 *
                 * Do NOT backfill at all if the small file currently has
                 * no storage and we're now writing out at 2*VM_PAGE_SZ
                 * or greater.  This would be unnecessary.
                 */
                if (*off >= (off_t)(2 * VM_PAGE_SZ)) {
                    if (small_file_already_had_storage) {
                        struct advfs_pvt_param temp_priv_param;
                        off_t first_page_hole_offset,
                              saved_offset;
                        size_t first_page_hole_size;
    
                        first_page_hole_offset = saved_offset =
                            ADVFS_FOB_TO_OFFSET(bfap->bfaNextFob);
                        first_page_hole_size = VM_PAGE_SZ - 
                                               first_page_hole_offset;

                        bcopy(fs_priv_param, 
                              &temp_priv_param, 
                              sizeof(struct advfs_pvt_param));
                        temp_priv_param.app_starting_offset = 
                                                         first_page_hole_offset;
                        temp_priv_param.app_total_bytes = first_page_hole_size;
    
                        MS_SMP_ASSERT(rwlock_wrowned(&contextp->file_lock));
            
                        sts = advfs_getpage(fc_vminfo,
                                            vp,
                                            &first_page_hole_offset,
                                            &first_page_hole_size,
                                            ftype,
                                            &temp_priv_param,
                                            pflags);
                        if (sts != EOK) {
                             return_offset = *off;
                             return_size = 0; /* Didn't process 
                                                 intended request */
                             return_status = sts;
                             goto popsicle_stand;
                        }
                        /*
                         * It's not a small file anymore.  
                         */
                        is_a_small_file = FALSE;
    
                        /*
                         * If this call was made for Direct I/O, then 
                         * if object safety is on, return storage added
                         * to the caller so he can zero-fill the storage 
                         * before committing the transaction.  Otherwise, 
                         * include the storage we just added in the total 
                         * storage range returned.
                         */
                        if ( addstg_nocache_specified ) {
                            if (!FTX_EQ(temp_priv_param.app_parent_ftx, 
                                        FtxNilFtxH)) {
                                /*
                                 * Object safety was enabled when the
                                 * storage was added. 
                                 */

                                fs_priv_param->stg_desc.app_stg_start_fob = 
                                temp_priv_param.stg_desc.app_stg_start_fob;
                                fs_priv_param->stg_desc.app_stg_end_fob = 
                                temp_priv_param.stg_desc.app_stg_end_fob;
                                fs_priv_param->app_parent_ftx = 
                                temp_priv_param.app_parent_ftx;
                                return_offset = first_page_hole_offset;
                                return_size = first_page_hole_size;
                                goto popsicle_stand;
                            }
                            else {
                                fs_priv_param->stg_desc.app_stg_start_fob = 
                                    ADVFS_OFFSET_TO_FOB_DOWN(saved_offset);
                                first_check = FALSE;
                            }
                        }
                    }
                }
                else {
                    /*
                     * This is a small file currently and we are writing to
                     * an offset less than 2*VM_PAGE_SZ.  See if we need
                     * to extend the request to the right or the left to
                     * avoid creating a hole smaller than VM_PAGE_SZ.
                     */

                    /*
                     * If we are writing at an offset that would create a 
                     * hole smaller than VM_PAGE_SZ to the left, extend the 
                     * getpage request to the left to include that region.
                     */
                    if (ADVFS_OFFSET_TO_FOB_DOWN(*off) > bfap->bfaNextFob) {

                        size_t adjust_left = *off - 
                                       ADVFS_FOB_TO_OFFSET(bfap->bfaNextFob);

                        *size += adjust_left;
                        *off  -= adjust_left;

                        /*
                         * Adjust the private parameters so that storage
                         * will be added appropriately.
                         */
                        if (fs_priv_param) {
                            fs_priv_param->app_starting_offset -= adjust_left;
                            fs_priv_param->app_total_bytes += adjust_left;
                        }
                    }

                    /*
                     * If we are writing at an offset that would create a hole 
                     * smaller than VM_PAGE_SZ to the right, extend 
                     * the getpage request to the right to include that region.
                     * This happens if the write ends before the file size
                     * and also causes storage to be added.
                     */
                    if  ((!small_file_already_had_storage) &&
                         ((off_t)(roundup((*off + *size), 
                              bfap->bfPageSz * ADVFS_FOB_SZ)) < file_size)) {

                        size_t adjust_right = file_size - (*off + *size);

                        *size += adjust_right;

                        /*
                         * Adjust the private parameters so that storage
                         * will be added appropriately.
                         */
                        if (fs_priv_param) {
                            fs_priv_param->app_total_bytes += adjust_right;
                        }
                    }
                }
            }
        }
    } else {
        file_size = bfap->bfaNextFob * ADVFS_FOB_SZ;
        if (process_snaps && HAS_SNAPSHOT_PARENT( bfap ) ) {
            file_size = max (file_size, bfap->bfa_orig_file_size );
        }
    }

    /*
     * We setup these values here for mmappers but they may be modified
     * later to adjust for snapshots and COWing 
     */
    getpage_req_off_start = *off;
    if ((*size == 0) && (!(metadata))) {
        getpage_req_size = bfap->file_size;
    } else {
        getpage_req_size = *size;
    }
    /* Figure out the ranges that will be covered by the FP_CREATE flag
     * any pages we bring in outside of this range MUST be initialized
     */

    fp_create_starting_offset = roundup(*off,VM_PAGE_SZ);
    fp_create_ending_offset = rounddown((*off+*size),VM_PAGE_SZ) - 1;

    getpage_req_off_end = getpage_req_off_start + getpage_req_size - 1;


    /* This test is a bit confusing. We need to kick out an
     * mmaper that is trying to read/write to an address that
     * is beyond the end of the file. This is straight forward
     * if the caller is just an mmaper. However this gets tricky
     * when the thread faulting on the mmap address is in the
     * context of a read or write system call (ie the address
     * used in the system call is an mmap address of the same
     * or another file).
     */ 
    /* Preserving an old Tru64 comment:
     *
     * It is an error to read/write beyond the file's last page,
     * we need to check because some callers don't have the lock.
     * Because of the way nfs works and the problems of racing
     * truncation, we only report an error for vm, other callers
     * get success and fewer (maybe 0) pages.
     */

     if ((!metadata) &&
         (!(ndp->ni_nameiop & NI_RW) ||
          !(fs_priv_param))) {
         
         /* We know that this is either an mmap fault or a
          * regular page fault. 
          */
         if (!(ndp->ni_nameiop & NI_RW) || 
             (ndp->ni_vp != vp) || 
             (ftype & FCF_DFLT_READ)) {
             /* This is either an mmaper or a page fault for read.
              * In either case we should not be attempting to
              * access beyond the end of the file (rounded to 
              * a VM page boundary.
              */
             if (getpage_req_off_start >= file_size) {
                 /* for mmappers this is an error that becomes sigbus.
                  * NOTE: On Tru64: for NFS this is success and 0 pages
                  * returned.!!!!!!!!!! Does HP-UX need this behavior???
                  * The UFC ERS states that we should always return an
                  * error and VM will do the right thing!
                  */
                 return_offset = *off;
                 return_size = *size;
                 return_status = EFAULT;
                 goto popsicle_stand;
             }
             if (getpage_req_off_end >= (off_t)roundup(file_size,VM_PAGE_SZ)) {
                 /* We know that part of the requested range is within the
                  * file so set up for an error return but bring in what we
                  * can
                  */

                 getpage_req_size = file_size - getpage_req_off_start;
                 return_offset = *off;
                 return_size = *size;
                 return_status = EFAULT; /* remember this for later */
             }

             /*
              * If this is a small file and the caller is an mmapped write,
              * scale back the getpage request size to be the file size 
              * so that this guy doesn't try to add storage up to 
              * VM_PAGE_SZ bytes.  Example:  An mmapper has
              * mmapped a 1000-byte file and is trying to modify the first 
              * byte of the file.  He'll come in here with *off = 0, 
              * *size = 4096, bfap->file_size = 1000, and 
              * bfap->bfaNextFob = 1.  We would want to make sure he doesn't
              * try to allocate 3k of storage at offset 1k.
              */
             if ((is_a_small_file) && (mmapped_file) &&
                 (ftype & FCF_DFLT_WRITE)) {
                 getpage_req_size = file_size;
             }
         }
         else if (ftype & FCF_DFLT_WRITE) {
             /* This is an mmapper or a page fault for write.  if this
              * is an mmaper then it is the case where the mmap
              * address is the target of a read system call to the
              * same file. We can not distinguish between the two
              * here. WE NEED TO FIX THIS once the flag to indicate
              * this is an mmaper is passed to us. In the mean time
              * the best we can do here is to compare against the
              * nextFob instead of the file_size. This will allow an
              * mmaper to write beyond the end of file up to an
              * allocation unit - 4K or more if there is preallocated
              * space. This is necessary because the write fault must
              * be allowed to proceed since it could be extending the
              * file and we have not yet updated the file size.
              */

             if (getpage_req_off_start >= (off_t)(bfap->bfaNextFob *
                                                  ADVFS_FOB_SZ)) {
                 /* for mmappers this is an error that becomes sigbus.
                  * NOTE: On Tru64: for NFS this is success and 0 pages
                  * returned.!!!!!!!!!! Does HP-UX need this behavior???
                  * The UFC ERS states that we should always return an
                  * error and VM will do the right thing!
                  */
                 return_status = EFAULT;
                 return_offset = *off;
                 return_size = *size;
                 goto popsicle_stand;
             }
             if (getpage_req_off_end >= 
                 (off_t)roundup((bfap->bfaNextFob*ADVFS_FOB_SZ), VM_PAGE_SZ)) {
                 /* We know that part of the requested range is within the
                  * file so set up for an error return but bring in what we
                  * can
                  */

                 getpage_req_size = file_size - getpage_req_off_start;
                 return_offset = *off;
                 return_size = *size;
                 return_status = EFAULT; /* remember this for later */
             }

         }
     }


    /* 
     * Acquire the migStgLk before starting a transaction.
     */
    if (!metadata && process_snaps && (ftype & FCF_DFLT_WRITE) ) {
        /*
         * If this is a write to userdata a parent snapshots and storage may be
         * required, get the mig storage lock now before acquiring the
         * bfa snap locks.  
         */
        ADVRWL_MIGSTG_READ( bfap );
        mig_stg_locked = TRUE;
    }


     if (process_snaps && (ftype & FCF_DFLT_WRITE)) {
        

        FTX_START_N( FTA_GETPAGE_COW,
                        &cow_ftx, parent_ftx, bfap->dmnP );
        cow_ftx_started = TRUE;

#ifdef WRITEABLE_SNAPSHOTS
        if (bfap->bfaFlags & BFA_VIRGIN_SNAP) {
            ADVRWL_BFAP_SNAP_WRITE_LOCK( bfap );
            sts = advfs_setup_cow( bfap->bfaParentSnap,
                                        bfap,
                                        SF_SNAP_NOFLAGS,
                                        cow_ftx );
            ADVRWL_BFAP_SNAP_UNLOCK( bfap );
            if (sts != EOK) {
                return_status = sts;
                return_offset = *off;
                return_size = *size;
                goto popsicle_stand;
            }
            
        }
#endif

        /* 
         * We need to open any children snapshots.  The BFA_SNAP_CHANGE 
         * flag will indicate whether or not a new snapset was created 
         * while a file was open.  The second check will see if a snapset was created
         * then a file was opened and has not been written to yet.
         */
        if ( process_snaps && ((bfap->bfaFlags & BFA_SNAP_CHANGE) || 
                 (HAS_SNAPSHOT_CHILD( bfap ) && 
                  (!bfap->bfaFirstChildSnap) ) ) ) {

            sts = advfs_access_snap_children( bfap, cow_ftx, &dummy );
                
            /* 
             * advfs_access_snap_children only fails for a critical
             * error, not if it fails to open a single child.
             * Individual children will just be marked as out of sync.
             */
            if (sts != EOK) {
                return_status = sts;
                return_offset = *off;
                return_size = *size;
                goto popsicle_stand;
            }
        }
    }


    /* 
     * Deal with CFS and snapshots.  We need to try to revoke the CFS tokens
     * for all children.  It is assumed that if the file lock is held for
     * write, this has already been done.  The reason for revoking the
     * tokens is that we are about to add storage to the snapshots.  By
     * revoking the tokens, we make sure that any clients don't have stale
     * extent maps that include storage owned by the parent (which may be
     * modified after this fault!).
     */
     if ( process_snaps && 
          (!metadata) &&
          (bfap->bfVnode.v_vfsp->vfs_flag & VFS_CFSONTOP) && 
          (ftype & FCF_DFLT_WRITE) ) {
        if  (!rwlock_wrowned(&contextp->file_lock) && (!snap_tokens_revoked) ) {
            /* The file lock is held for read.  We can revoke CFS tokens as
             * appropriate */
            sts = advfs_snap_revoke_cfs_tokens( bfap, SF_SNAP_NOFLAGS);
            MS_SMP_ASSERT( sts == EOK );
            snap_tokens_revoked = TRUE;
        } else {
            /* 
             * We cannot drop the file lock and that is required to revoke
             * the CFS tokens (a lock hierarchy issue).  Anyone coming into
             * advfs_getpage with the file lock held should have called
             * advfs_snap_revoke_cfs_tokens before faulting... 
             */
#ifdef SNAPSHOTS
            THIS SHOULD VERIFY CFS DOES NOT HAVE ANY TOKENS.
#endif
        }
    }


    sparseness_offset = getpage_req_off_start;
    sparseness_size = getpage_req_size;
    sparseness_end = sparseness_offset + sparseness_size;

    /* 
     * Check to see if we need to round to COW in ADVFS_COW_ALLOC_UNIT
     * blocks.  This will round the start down and the end up but will limit
     * the end of the COW range to file size (for userdata).
     * We will never try to COW beyond EOF of the current
     * file. We will also skip rounding for metadata although this code
     * would mostly support that if advfs_getmetapage were called in a loop.
     * If this is a forced cow (advfs_force_cow_and_unlink) then we will go
     * ahead and round out even for metadata since we will go through the
     * userdata write path even for metadata...
     */
    if (process_snaps && (ftype & FCF_DFLT_WRITE) && 
            (!metadata || forced_cow) ) {
        off_t   orig_req_end = getpage_req_off_start + getpage_req_size;
        off_t   snap_req_end;

        req_start_before_cow = getpage_req_off_start;
        req_end_before_cow = getpage_req_off_start + getpage_req_size;

        getpage_req_off_start = rounddown( getpage_req_off_start,
                ADVFS_FOB_TO_OFFSET( advfs_cow_alloc_units * bfap->bfPageSz ) );

        /* 
         * Calculate how much we would like to COW
         */
        snap_req_end = roundup( orig_req_end,
                ADVFS_FOB_TO_OFFSET( advfs_cow_alloc_units * bfap->bfPageSz ) );

        /* Bound the desired COW by the file size */
        if (snap_req_end > (bfap->file_size) ) {
            snap_req_end = bfap->file_size;
        }
        /*
         * Storage will be added over this range, so make sure it falls on
         * correct allocation unit boundaries.
         */
        snap_req_end = roundup( max(snap_req_end, orig_req_end),
                                ADVFS_ALLOCATION_SZ(bfap) );

        /*
         * Same as above.  Track meta cow sizes differently 
         */
        getpage_req_size = snap_req_end - getpage_req_off_start;

        GPLOGIT(COW_ADJUSTMENT,vp,getpage_req_off_start,getpage_req_size,
            pflags,ftype,0,0,DEFAULT_ACTION);

    }

    getpage_req_off_end = getpage_req_off_start + getpage_req_size - 1;

    return_size=0;
    return_offset=getpage_req_off_start;


    GPLOGIT(GETPAGE_CALL,vp,getpage_req_off_start,getpage_req_size,
            pflags,ftype,0,0,DEFAULT_ACTION);

    /*
     * If we need to process snapshots, now is the time to acquire all the
     * necessary locks!
     */
    if (process_snaps) {
        /*
         * Once advfs_acquire_snap_locks is called, any error must call
         * advfs_drop_snap_locks UNTIL advfs_add_snap_stg is called. Once
         * advfs_add_snap_stg is called, advfs_cleanup_snaps_and_unlock
         * should be called to drop only the locks for children in the snap
         * maps since advfs_add_snap_stg will drop unneeded locks.!!!
         */
        sts = advfs_acquire_snap_locks( bfap,
                (ftype & FCF_DFLT_WRITE) ? SF_SNAP_WRITE : SF_SNAP_READ,
                cow_ftx );
        if (sts == EAGAIN) {
            /* EAGAIN may be returned if we were racing a migrate.  The
             * migStgLk is only acquired for write by migrate.  Because of
             * lock hierarchy issues and the ftxSlot lock needing to be
             * taken after the migStgLk, we will defer to migrate and try
             * again.  Hopefully there isn't a long queue of migrates
             * waiting to execute on this file's children or this could loop
             * multiple times.
             *
             * We will fail this transaction to limit log traffic.  The
             * operations done in subtransaction on snapshot children were
             * done with "no undo" so they will not be undone.
             */
            ftx_fail( cow_ftx );
            cow_ftx_started = FALSE;

            if (mig_stg_locked) {
                ADVRWL_MIGSTG_UNLOCK( bfap );
                mig_stg_locked = FALSE;
            }

            SNAP_STATS( getpage_eagain_getting_snap_locks );

            goto start_over;

        }
        if (sts != EOK) {
            return_status = sts;
            return_offset = *off;
            return_size = *size;
            goto popsicle_stand;
        }

        /*
         * If we are removing the snapset, we dont' want to continue on and
         * process snapshots and possibly write-protect pages that we bring
         * into cache.  When we remove the child snapshot, it will try to
         * restore the permissions on every in cache page.  NOw that we hold
         * the bfaSnapLock, we will check again to see if we need to process
         * snapshots.  If things have changed, we will drop the locks we
         * have acquired and start over.  This is done to minimize impact on
         * the mainline code path rather than syncrhonize all reads and
         * writes with removing and creating snapshots.
         */
        process_snaps = HAS_RELATED_SNAPSHOT( bfap ) && 
            ( (!metadata) || (IS_COWABLE_METADATA( bfap )) );  

        if (!process_snaps) {
            /* The snapset disappeared.  Drop the locks and start over.
             */
            advfs_drop_snap_locks( bfap, 
                    (ftype & FCF_DFLT_WRITE) ? SF_SNAP_WRITE : SF_SNAP_READ );

            if (mig_stg_locked) {
                ADVRWL_MIGSTG_UNLOCK( bfap );
                mig_stg_locked = FALSE;
            }

            if (cow_ftx_started) {
                ftx_done_n( cow_ftx,
                            FTA_GETPAGE_COW);
                cow_ftx_started = FALSE;
                cow_ftx = FtxNilFtxH;
            }

            cleanup_snaps = FALSE;

            SNAP_STATS( getpage_process_snap_status_changed );

            goto start_over;
        }


        /*
         * Check for out of sync with the bfap lock held.
         */
        if (bfap->bfaFlags & BFA_OUT_OF_SYNC) {
            advfs_drop_snap_locks( bfap,
                    (ftype & FCF_DFLT_WRITE) ? SF_SNAP_WRITE : SF_SNAP_READ );
            return_status = E_OUT_OF_SYNC_SNAPSHOT;
            return_offset = *off;
            return_size = *size;
            goto popsicle_stand;

        }

        cleanup_snaps = TRUE;
    }

    /* 
     * Now check to see if we need to add storage.  This is done before
     * calling into metdata so that we can get storage for snapshot children
     * if necessary
     */
    /* We only need information about the sparseness map in the case
     * of a write since we may need to adjust our beginning request
     * offset to a storage allocation boundary and we will need to
     * upgrade the FILE lock to write mode.
     *
     * For metadata, we should already have storage.  If this is metadata
     * and we are processing snapshots, then we want to fall through this
     * path to add storage to children as needed.
     */

    if ( (ftype & FCF_DFLT_WRITE) && (!(metadata) || (process_snaps)) ) {
        /* We may need to adjust the page range that we bring into the
         * cache to align with the underlying storage allocation unit
         * if the beginning offset falls within a hole. 
         */

        if (!metadata) {

            sts = advfs_get_blkmap_in_range( 
                                        bfap,
                                        bfap->xtnts.xtntMap,
                                        &sparseness_offset,
                                        sparseness_size,
                                        &sparseness_blkmap,
                                        &request_needs_storage,
                                        RND_ALLOC_UNIT,
                                        (EXB_ONLY_HOLES | EXB_DO_NOT_INHERIT), 
                                        ((pflags & FP_NOWAIT)         ?
                                         (XTNT_NO_WAIT | XTNT_NO_MAPS):
                                         (XTNT_WAIT_OK | XTNT_NO_MAPS)) );
            if ((sts != EOK) ||
                    ((pflags&FP_NOWAIT) && (request_needs_storage))){
                /* If the callers cannot wait and we would block or we
                 * need to allocate storage in order to staisfy this write
                 * we will fail the write right now 
                 */
                if ((pflags&FP_NOWAIT) && (request_needs_storage)) {
                    return_status = EWOULDBLOCK;
                } else {
                    return_status = sts;
                }
            
                if (cleanup_snaps) {
                    advfs_drop_snap_locks( bfap,
                      (ftype & FCF_DFLT_WRITE) ? SF_SNAP_WRITE : SF_SNAP_READ );
                    cleanup_snaps = FALSE;
                }

                return_offset = *off;
                return_size = *size;
                goto popsicle_stand;
            }
        } 
        /* request_needs_storage was initialized to 0 for metadata on entry
         * to advfs_getpage */

        arp_dropped_lock = FALSE;
        if (request_needs_storage) {
            /* We need to allocated storage, so get an active range (if the
             * caller wants us to), and upgrade the write lock now to avoid
             * complicated backout scenarios later.
             *
             * If an Active Range arp was passed, and the range
	     * has not been filled in yet (arp->arDebug==0), or it was setup in
	     * a previous pass through the start_over loop, then set it up
	     * using bfap->bfPageSz alignment to cover the allocated pages.
             *
             * Typically, only the Direct I/O write calls pass in an arp,
             * and only IO_APPEND writes are already filled in, all others
             * are filled in here, when we know what storage will or will
             * not be needed.
             *
             * For Direct I/O we _MUST_ either have the file write lock or
             * an active range that covers all of the allocated storage at
             * all times until after the allocated storage has either been
             * initialized or the desired data written.  But getting the
             * active range may cause us to drop the file lock, if for
             * example another Direct I/O read thread is holding just an
             * active range, so we do this dance here before we have
             * allocated the storage, and then once we have the active range
             * and file write lock we make sure we keep the active range
             * until after all file changes have been completed.
             *
             * Assumptions:  advfs_bs_add_stg() never pre-allocates extra
             *               pages for Direct I/O (bfap->dioCnt != 0)
             *               requests.  So rounding the request to
             *               bfap->bfPageSz boundaries will cover the full
             *               range of allocated storage.  There are asserts
             *               after getting storage to make sure we did not
             *               allocate more than the active range is
             *               protecting ("Trust, but Verify").
             */
            if ( we_setup_arp || (arp && arp->arDebug == 0) ) {
                bf_fob_t startFob;
                bf_fob_t endFob;

                MS_SMP_ASSERT(addstg_nocache_specified);
                MS_SMP_ASSERT(bfap->dioCnt);    /* used by Direct I/O today */
                MS_SMP_ASSERT(!process_snaps);  /* Snaps done before call */

                startFob = ADVFS_AU_ROUND_DOWN(bfap,
                                             fs_priv_param->app_starting_offset);
                endFob   = ADVFS_AU_ROUND_UP(bfap,
                                            fs_priv_param->app_starting_offset,
                                            fs_priv_param->app_total_bytes);

                /* See if the file is being extended */
                if ( bfap->file_size < 
                     fs_priv_param->app_starting_offset +
                     fs_priv_param->app_total_bytes ) {
                    if ( bfap->file_size <
                         fs_priv_param->app_starting_offset ) {
                        /* This write is starting beyond the current
                         * end-of-file, so include the current EOF in the
                         * range.  This is done because we may be creating a
                         * sparse hole and pre-allocated storage from a
                         * previous non-Direct I/O may need to be released
                         * in advfs_bs_add_stg().  It is our job to make
                         * sure the active range protects the release of
                         * that pre-allocated storage.
                         */
                        startFob = ADVFS_AU_ROUND_DOWN(bfap, bfap->file_size);
                    }
                    /* Extend range "To Infinity and Beyond!" */
                    endFob   = ~0L;
                }

                if ( we_setup_arp ) {
                    /* If we setup this active range in a previous
                     * start_over pass, we may need to get a new one if the
                     * active range does not cover everything we need this
                     * time.  For example a truncate could have wormed its
                     * way in and changed the end-of-file.  So if the active
                     * range we currently hold is too small, we throw it
                     * away and get a new one.  If ours is too large, we
                     * keep it and declare victory!
                     */
                    if ( startFob < arp->arStartFob || 
                         endFob   > arp->arEndFob ) {
                        /* Our active range is too small, so throw it away.
                         */
                        spin_lock(&bfap->actRangeLock);
                        remove_actRange_from_list(bfap, arp);
                        spin_unlock(&bfap->actRangeLock);
                        arp->arDebug = 0;
                        we_setup_arp = FALSE;
                    }
                }

                /* Do we need to get an active ranage, or did we keep the
                 * range from a previous start_over loop pass?
                 */
                if ( ! we_setup_arp ) {
                    arp->arStartFob = startFob;
                    arp->arEndFob   = endFob;
                    arp->arDebug = SET_LINE_AND_THREAD(__LINE__);
                    arp_dropped_lock = insert_actRange_onto_list(bfap, arp,
                                                                 contextp);
                    we_setup_arp = TRUE;
                }
            }

            /* 
             * We need to allocated storage, so upgrade the write lock
             * now to avoid complicated backout scenarios later
             */
            writelock_locked = ADVRWL_FILECONTEXT_ISWRLOCKED( contextp );

            if (!writelock_locked) {
                /* We can no longer just try an upgrade with snapshots
                 * because a potential deadlock with the upgrade and 
                 * and the point we try to acquire the snap locks in getpage.
                 * We will continue to try the upgrade if there are no
                 * snapshots.
                 */
                if (process_snaps) {
                    /* We will start a new one in a minute when we loop */
                    ftx_done_n( cow_ftx,
                            FTA_GETPAGE_COW);
                    cow_ftx_started = FALSE;
                    cow_ftx = FtxNilFtxH;
                    advfs_drop_snap_locks( bfap,
                            (ftype & FCF_DFLT_WRITE) ? 
                            SF_SNAP_WRITE : SF_SNAP_READ );
                    cleanup_snaps = FALSE;

                    if (!metadata && process_snaps && (ftype & FCF_DFLT_WRITE) ) {
                        ADVRWL_MIGSTG_UNLOCK( bfap );
                        mig_stg_locked = FALSE;
                    }
                }
                /*
                 * If we are processing snapshots we will not even try the
                 * upgrade... the || will be short circuited.
                 */
                if (process_snaps || 
                        ADVRWL_FILECONTEXT_UPGRADE(contextp)!= RWLCK_SUCCESS ) {
                    /* We were unable to upgrade the lock to write mode
                     * Drop the read lock and lock for writing this time
                     * waiting for the lock. Jump back to the top since
                     * we need to revalidate.
                     */
                    
                    ADVRWL_FILECONTEXT_UNLOCK(contextp);
                    ADVRWL_FILECONTEXT_WRITE_LOCK(contextp);
		    writelock_locked = 1;
                    first_pass = FALSE;
             
                    goto start_over;
                }
		writelock_locked = 1;
            }
            if ( arp_dropped_lock ) {
                MS_SMP_ASSERT(!process_snaps);
                first_pass = FALSE;
                goto start_over;
            }

#ifdef ADVFS_SMP_ASSERT
            /* All copy-on-write snapshot operations should have occurred
             * before advfs_getpage() was called.  This is why snapshot code
             * is bypassed when APP_ADDSTG_NOCACHE is set.
             *
             * Now make sure that this is true by asserting this range does
             * not need to be COW'ed!
             */
            if (addstg_nocache_specified) {
                advfs_assert_no_cow(bfap, fs_priv_param->app_starting_offset,
                                          fs_priv_param->app_total_bytes);
            }
#endif /* ADVFS_SMP_ASSERT */
        }

        orig_sparseness_offset = sparseness_offset;
        orig_sparseness_size = sparseness_end - sparseness_offset;


        if (HAS_SNAPSHOT_CHILD( bfap ) && process_snaps) {
            
            if (metadata) {
                /* 
                 * Metadata writes may fault on 4k boundaries if one of the
                 * pages is in cache.  We need to add storage on aligned
                 * boundaries.
                 */
                getpage_req_off_start = rounddown(getpage_req_off_start,
                                          bfap->bfPageSz * ADVFS_FOB_SZ);
                getpage_req_size = getpage_req_off_end - 
                    getpage_req_off_start;
            }

            /*
             * If any children exist, see if we need to add storage for
             * them.  At this point, advfs_cleanup_snaps_and_unlock should
             * be called instead of advfs_drop_snap_locks.
             * advfs_add_snap_stg will drop unneeded locks.  Only those
             * files in the snap_maps need to be unlocked.
             * advfs_cleanup_snaps_and_unlock will be called in the
             * popsicle_stand.
             */
            sts = advfs_add_snap_stg( bfap, 
                                      getpage_req_off_start,
                                      getpage_req_size,
                                      &min_child_stg,
                                      &max_child_stg,
                                      &snap_maps,
                                      ((pflags&FP_NOWAIT)? 
                                       SF_NO_WAIT        :
                                       SF_SNAP_NOFLAGS),
                                      cow_ftx );
            if (sts != EOK) {
                /* 
                 * Errors here should be very rare.  Failing to add storage
                 * to a child shouldn't cause an error unless the child also
                 * could not be marked out of sync.  Failure to access the
                 * extent maps could cause an error, but the extent maps
                 * should already be loaded.
                 *
                 * EWOULDBLOCK will have been returned from
                 * advfs_add_snap_stg if any storage or COWing would have
                 * been required.
                 */
                return_offset = *off;
                return_size = *size;
                goto popsicle_stand;
            }

            /*
             * If min_child_stg is less that sparseness_offset, then round
             * down a bit.   This will make sure the loop control variables
             * are correct.  It won't add extra storage.
             */
            sparseness_offset = min ( min_child_stg, sparseness_offset );

            /*
             * Now that storage has been added to children, we can unround
             * if the COW range was bigger than necessary. 
             */
            if ( (getpage_req_off_start < min_child_stg) ||
                 ((off_t)(getpage_req_off_start + 
                          getpage_req_size) > max_child_stg) ) {
                /* Undo any rounding based on COW boundaries */
                
                if (getpage_req_off_start < min_child_stg) {
                    int64_t req_diff = min( min_child_stg,
                            sparseness_offset ) - getpage_req_off_start;

                    MS_SMP_ASSERT( req_diff >= 0 );
                    getpage_req_off_start += req_diff;                 
                    getpage_req_size -= req_diff;
                }

                if ((off_t)(getpage_req_off_start + 
                            getpage_req_size) > max_child_stg) {
                    /* Trim the size a bit */
                    getpage_req_off_end = max( req_end_before_cow,
                            max_child_stg);
                    getpage_req_size = getpage_req_off_end -
                                       getpage_req_off_start;
                }
                GPLOGIT(SNAP_UNROUND, vp, getpage_req_off_start,
                                          getpage_req_size,
                    pflags, ftype, 0, 0,DEFAULT_ACTION );

            }

        }

        /*
         * If APP_ADDSTG_NOCACHE was specified, all COWing should already
         * have been done prior to adding storage for Direct I/O or
         * advfs_prealloc() by calling advfs_force_cow_and_unlink().
         */
        if ( addstg_nocache_specified && !request_needs_storage ) {

            MS_SMP_ASSERT(addstg_nocache_specified);
            MS_SMP_ASSERT(!process_snaps);  /* Snaps done before call */

            /* We have all the storage we need, thank you.
             *
             * If an Active Range arp was passed, and it has not already
             * been setup, then fill it in using FOB alignment to protect
             * just what we are writing.
             *
             * If we already have an active range, then we do nothing, even
             * if the active range was setup by a previous start_over pass
             * that thought storage was going to be allocated.  While such
             * an active range might be larger than what is needed by a
             * write request to already allocated storage, it is less
             * overhead to keep the larger active range.
             */
            if ( arp &&  arp->arDebug == 0 ) {
                MS_SMP_ASSERT(bfap->dioCnt);    /* used by Direct I/O today */

                arp->arStartFob = ADVFS_OFF_TO_FIRST_FOB_IN_VDBLK(
                                    fs_priv_param->app_starting_offset);
                arp->arEndFob   = ADVFS_OFF_TO_LAST_FOB_IN_VDBLK(
                                    fs_priv_param->app_starting_offset,
                                    fs_priv_param->app_total_bytes);
                arp->arDebug = SET_LINE_AND_THREAD(__LINE__);
                arp_dropped_lock = insert_actRange_onto_list(bfap, arp,
                                                             contextp);
                we_setup_arp = TRUE;
                if ( arp_dropped_lock ) {
                    /* If dropped lock, then trunc could have happened */
                    first_pass = FALSE;
                    goto start_over;
                }
            }

#ifdef ADVFS_SMP_ASSERT
            /* All copy-on-write snapshot operations should have occurred
             * before advfs_getpage() was called.  This is why snapshot code
             * is bypassed when APP_ADDSTG_NOCACHE is set.
             *
             * Now make sure that this is true by asserting this range does
             * not need to be COW'ed!
             */
            if (addstg_nocache_specified) {
                advfs_assert_no_cow(bfap, fs_priv_param->app_starting_offset,
                                          fs_priv_param->app_total_bytes);
            }
#endif /* ADVFS_SMP_ASSERT */

            /* No storage is needed, and this was a call to just
             * add storage, so get out now.
             */
            return_status = EOK;
            fs_priv_param->stg_desc.app_stg_end_fob = 0;   /* no stg added */
            return_offset = *off;
            return_size = *size;
            goto popsicle_stand;
        }
    }


#ifdef ADVFS_SMP_ASSERT
    if (!addstg_nocache_specified) {
        check_page_protections = TRUE;
    }

    if (bfap->dataSafety != BFD_USERDATA) {
        check_page_protections = FALSE;
    }
#endif




    if ((metadata) && (ftype & FCF_DFLT_WRITE)) {
        ftxHT   meta_ftx;
        /* We only need special processing for meta-data
         * writes. Meta-data reads (bs_refpg) can just fall into the
         * code below as a user data read.
         */
        uint32_t xtnt_flags = 0;

        if ( HAS_SNAPSHOT_PARENT( bfap ) && process_snaps ) {
            /*
             * Here we only do a synchronous COW of the single page to be
             * faulted to write.  The rest of the meta-cow range will be
             * dealt with below.
             */
            sts = advfs_sync_cow_metadata( bfap,
                               getpage_req_off_start,
                               getpage_req_size,
                               cow_ftx );
            if (sts != EOK) {
                /* This is serious. If we continue, we will end up with a
                 * metadata page in cache for modification and no storage
                 * under it.  That's bad.
                 */
                return_offset = *off;
                return_size = *size;
                goto popsicle_stand;
            }
        }


        if (pflags & FP_NOWAIT) {
            xtnt_flags = XTNT_NO_WAIT | X_LOAD_REFERENCE;
        }
        else {
            xtnt_flags = XTNT_WAIT_OK | X_LOAD_REFERENCE;
        }

        if (process_snaps) {
            sts = advfs_acquire_xtntMap_locks( bfap, xtnt_flags, 
                    (ftype&FCF_DFLT_READ)?SF_SNAP_READ:SF_SNAP_NOFLAGS );
        } else {
            sts = x_lock_inmem_xtnt_map(bfap, xtnt_flags);
        }

        if (sts != EOK) {
            goto popsicle_stand;
        }

        xtntmap_locked = 1;

        /* This should only be necessary if priv_params is NULL.
         * The above comment is no longer true.  The UFC will try to 
         * optimize faults and only fault on the 4k that isnt' in cache.  
         * At present, advfs_getmetapage is setup to deal with this case 
         * so we need to round down.*/
        getpage_req_off_start = rounddown(getpage_req_off_start,
                                          ADVFS_METADATA_PGSZ);

        if (snap_maps) {
            meta_ftx = cow_ftx;
        } else {
            meta_ftx = FtxNilFtxH;
        }

        
        /*
         * If we are processing snapshots, the we may have rounded out
         * getpage_req_off_start and getpage_req_of_end so COW in larger
         * chunks.  If that is the case, then loop on page boundaries and
         * call advfs_getmetapage.
         */
        sts = advfs_getmetapage(fc_vminfo,
                                vp,
                                getpage_req_off_start,
                                fs_priv_param,
                                &ioAnchor_head,
                                pflags,
                                snap_maps,
                                meta_ftx);

        /* We only fault on metadata sizes */
        return_offset = getpage_req_off_start;
        return_size = ADVFS_METADATA_PGSZ;
        if (sts != EOK)
        {
            return_size = 0;
        }
        goto popsicle_stand;
    }


    if ( (process_snaps) && (snap_maps == NULL) && (ftype == FCF_DFLT_WRITE) ) {
        /*
         * If after all this setup, there is nothing to COW, then just go
         * back to the initial request size so we don't bring more into
         * cache than necessary.
         */
        getpage_req_off_start = req_start_before_cow; 
        getpage_req_size = req_end_before_cow - req_start_before_cow;
        getpage_req_off_end = getpage_req_off_start + getpage_req_size - 1;
        sparseness_offset = orig_sparseness_offset;
        return_size = 0;
        return_offset = getpage_req_off_start;
    } else if ( process_snaps && (ftype == FCF_DFLT_WRITE) ) {
        /* We want to clear these flags or we can't COW. We will set them
         * before returning. */
        cow_cleared_pflags = pflags & (FP_CREATE&FP_ASYNC);
        pflags &= ~(FP_CREATE|FP_ASYNC);
    }

    /* The caller may have passed in (in the case of the write system
     * call) the total size of the write in the private parameters. We
     * need to keep track of how much is being written through each
     * pass of the below loop as well as thru multiple calls to
     * VOP_GETPAGE for the total request. We may have to adjust the
     * passed in request size due to storage allocation requirements
     * or a large/big page was given to us. In order to keep track of
     * this the following local variables will be used.
     *
     * return_offset and return_size: will be return to the
     * caller to indicate the size of the range actually faulted
     * in. This range will only include pages that were retuned by
     * fcache_page_alloc that were beyond the requested range (i.e. a
     * large page that spanned beyond the request). This range will
     * include any adjustments made for allocation unit alignment.
     *
     * remaining_request_size: is the amount of pages left to process
     * for the entire request including any pages that we need to
     * adjust the starting offset by. This variable will be used to
     * determine how much storage we need to add at any time.
     *
     * remaining_loop_size: is the amount of pages remain to
     * process in this VOP_GETPAGE call including any pages that we
     * need to adjust the starting offset by.
     *
     * current_loop_offset is the offset of the first page remaing to
     * be processed in this loop and is adjusted by any storage
     * allocation alignment by advfs_get_blkmaps_in_range.
     */

    if (fs_priv_param != NULL) {
        
        /* What remains to be done. This is the total request + any extra pages
         * we need to bring in due to storage allocation adjustments - any pages
         * we have processed in a prior call to getpage for this request.
         */
        GPLOGIT(GETPAGE_CALL,vp,fs_priv_param->app_starting_offset,
                fs_priv_param->app_total_bytes,
                pflags,ftype,0,0,PRIVATE_PARAMS);

        remaining_request_size = 
            fs_priv_param->app_total_bytes +
            (fs_priv_param->app_starting_offset - 
             MIN(sparseness_offset,fs_priv_param->app_starting_offset)) -
            (getpage_req_off_start - fs_priv_param->app_starting_offset);

        write_req_off_start = fs_priv_param->app_starting_offset;
        write_req_off_end = fs_priv_param->app_starting_offset +
                            fs_priv_param->app_total_bytes - 1;

        /* We may have been adjusted for storage boundaries */

        remaining_loop_size = getpage_req_size + 
            (getpage_req_off_start - MIN(getpage_req_off_start,
                                         sparseness_offset));
    } else {
        /* This is an mmaper, page fault or a meta-data read */

        remaining_request_size = getpage_req_size + 
            (getpage_req_off_start - MIN(getpage_req_off_start, 
                                         sparseness_offset));

        remaining_loop_size = remaining_request_size;
        write_req_off_start = sparseness_offset;
        write_req_off_end   = remaining_request_size + sparseness_offset - 1; 
    }

    current_loop_offset = rounddown(sparseness_offset,VM_PAGE_SZ);
    remaining_loop_size = roundup((remaining_loop_size + 
                                   (sparseness_offset - current_loop_offset))
                                  ,VM_PAGE_SZ);

    remaining_request_size = MAX(remaining_loop_size,
                                 remaining_request_size);

    return_offset = current_loop_offset;
    return_size = remaining_loop_size;

    /*
     * 'potential_file_size' is the larger of the file's highest allocated
     * storage offset, the current file_size (in case there is a sparse hole
     * at the end-of-file), or the write request in the private params.
     * This is then rounded up to encompass a full allocation unit.
     */

    potential_file_size = MAX((off_t)(bfap->bfaNextFob *
                                      ADVFS_FOB_SZ), file_size);

    if ((ftype & FCF_DFLT_WRITE) && (fs_priv_param)) {

        potential_file_size = MAX( potential_file_size,
                                  (fs_priv_param->app_starting_offset + 
                                   fs_priv_param->app_total_bytes)
                                 );
        if (small_au_file && !will_remain_a_small_file) {
            potential_file_size = roundup(potential_file_size, VM_PAGE_SZ);
        } else {
            potential_file_size = roundup(potential_file_size,
                                          bfap->bfPageSz*ADVFS_FOB_SZ);
        }
    }
    else {
        /* In the case of a read or fault for write, we are
         * not increasing the size of the file so we only need
         * to round up to the VM_PAGE_SZ.
         */
        potential_file_size = roundup(potential_file_size,
                                      VM_PAGE_SZ);
    }

    /* Begin the loop to bring in the pages from the cache. */

    while (remaining_loop_size > 0) {

        /* 
         * Skip around the code that brings pages into the cache if
         * we are here merely to add storage.
         */
        if ( addstg_nocache_specified ) {
            MS_SMP_ASSERT( snap_maps == NULL );

            /*
             * If this is a small allocation unit file and we're adding
             * storage starting in the first VM_PAGE_SZ bytes of the
             * file, limit the storage allocation to VM_PAGE_SZ and
             * get the rest, if necessary, on a subsequent allocation.
             * This is done to preserve the small block size in the
             * first VM_PAGE_SZ bytes of the file and the larger
             * (VM_PAGE_SZ) block size after that point in the file.
             */
            alloc_offset = current_loop_offset;
            alloc_size = ((small_au_file) && 
                          (current_loop_offset < (off_t)VM_PAGE_SZ)) ?
                         alloc_size = VM_PAGE_SZ : remaining_loop_size;
            goto just_add_storage;
        }
        
        /* Lock the extent maps for reading. This is necessary to keep
         * the extent map locks above the VM page locks. This also
         * makes sure the extent maps are in memory before getting any
         * page locks. We don't want to page fault bringing in
         * extentmaps while we are holding page locks. We will return
         * with the extent maps read locked.
         */
        if (process_snaps) {
            sts = advfs_acquire_xtntMap_locks( bfap,
                                (pflags&FP_NOWAIT)?
                                    XTNT_NO_WAIT|X_LOAD_REFERENCE:
                                    XTNT_WAIT_OK|X_LOAD_REFERENCE,
                                (ftype&FCF_DFLT_READ)?
                                 SF_SNAP_READ:SF_SNAP_NOFLAGS);
        } else {
            sts = x_lock_inmem_xtnt_map(bfap,
                                    (pflags&FP_NOWAIT)?
                                    XTNT_NO_WAIT|X_LOAD_REFERENCE:
                                    XTNT_WAIT_OK|X_LOAD_REFERENCE);
        }
        if (sts != EOK) {
            return_status = sts;
            goto popsicle_stand;
        }
        
        xtntmap_locked = 1;
        sparseness_blkmap = NULL;
        
        alloc_offset = current_loop_offset;
        alloc_size = remaining_loop_size;
        

        /*
         * If the file has an allocation unit smaller than the VM page size,
         * force the call to fcache_page_alloc() to return only one VM page
         * if the offset requested is within the first VM page of the file.
         * This is because the first VM page of such a file is handled
         * differently from the rest of such a file.  Specifically, the
         * first VM page may have multiple extents backing it.
         */ 
        if ((small_au_file) &&
            (alloc_offset < (off_t)VM_PAGE_SZ) &&
            (first_fcache_page_alloc_call)) {
            min_coff = 0;
            alloc_size = max_coff = VM_PAGE_SZ;  /* NB:alloc_size <= max_coff */
            first_fcache_page_alloc_call = FALSE;
        }
        else if (bfap->dioCnt != 0) {
            /*
             * Direct I/O might be doing a concurrent I/O on an adjacent
             * range, and if this operation brings a page into the cache
             * with data of age x, and a direct I/O writes directly to disk
             * data of age x+1, then the cached age x data gets flushted
             * back to disk, we have corrupted the file with stale data.  So
             * do not allow big pages if the file is open for Direct I/O.
             */
            min_coff = alloc_offset;
            max_coff = alloc_offset + alloc_size;
        }
        else if (process_snaps && (snap_maps != NULL)) {
            /* 
             * If we have to issue IO for COWing to snaps, then we need to make
             * sure that fcache_page_alloc does not try to back up (it does 
             * this to check if it can return a large page).  If it tries to 
             * back up, it can wait on the pfdat lock.  That lock will never 
             * be released unless we get past this fcache_page_alloc loop and 
             * issue the io for the snapshots. 
             */
            min_coff = alloc_offset;
            max_coff = potential_file_size;
        }
        else {
            min_coff = 0;
            max_coff = potential_file_size;
        }

        plist = fcache_page_alloc(fc_vminfo,
                                  vp, 
                                  &alloc_offset,
                                  &alloc_size,
                                  min_coff,
                                  max_coff,
                                  &alloc_status,
                                  FPA_NEW|FPA_PGCACHE,
                                  pflags);
#ifdef ADVFS_SMP_ASSERT
        /* If the file is opened for Direct I/O, make sure the pages being
         * added to the UFC have an active range associated with them,
         * otherwise stale data could be returned or written to the file,
         * causing data corruption or the appearence of corruption.
         */
        if ( plist && bfap->dioCnt ) {
            ASSERT_ACTRANGE_HELD( bfap, alloc_offset, alloc_size );
        }
#endif /* ADVFS_SMP_ASSERT */

        if ((alloc_status == EWOULDBLOCK) ||
            (alloc_status == ENOMEM)) {
            return_status = alloc_status;
            goto popsicle_stand;
        }
        
        if (plist && will_remain_a_small_file) {
            MS_SMP_ASSERT(alloc_offset == 0);
            MS_SMP_ASSERT(alloc_size == VM_PAGE_SZ);
        }
       

        GPLOGIT(PAGE_ALLOC,vp,alloc_offset,alloc_size,pflags,ftype,alloc_status,0,DEFAULT_ACTION);
        
        if (alloc_status == FPA_ST_END) {
            /* This can happen when we have private mmap mappings.
             * When the mmap flag is available assert it here.
             */
            MS_SMP_ASSERT(ftype&FCF_DFLT_READ);
            return_size = alloc_size;
            goto popsicle_stand;
        }
        MS_SMP_ASSERT(alloc_status);

        /*
         * Send cache miss message to autotune/vfast/smartstore if enough have
         * occurred.  Skip reserved files since they can't be migrated anyway.
         */
        if (SS_is_running &&
            (!BS_BFTAG_RSVD (bfap->tag)) &&
            bfap->dmnP->ssDmnInfo.ssDmnSmartPlace &&
            (bfap->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
             bfap->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {
            
            uint64_t dummy;
            new_time = get_system_time();
            GETCURRWDAY (new_time.tv_sec, wday);
            ADVFS_ATOMIC_FETCH_INCR (&bfap->ssHotCnt[wday], &dummy);
            for (totHotCnt = 0, wday = 0; wday < SS_HOT_TRACKING_DAYS; wday++) {
                totHotCnt += bfap->ssHotCnt[wday];
            }
            if (totHotCnt > bfap->dmnP->ssDmnInfo.ssAccessThreshHits) {
                ss_snd_hot (HOT_ADD_UPDATE_MISS, bfap);
            }
        }
        
        if (ftype & FCF_DFLT_READ) {

            if (alloc_status == FPA_ST_DATAFILL) {

                extent_blk_desc_t *blkmap_entry;

                /* PFDATS were not found in the cache. We need to
                 * either read from disk or zero and mark read-only.
                 */
                
                /* We need to see the storage maps in order to
                 * initialize the UFC pages.
                 */

                storage_offset = alloc_offset;
                storage_size = is_a_small_file ?
                       roundup(bfap->file_size, bfap->bfPageSz * ADVFS_FOB_SZ) :
                       alloc_size;

                sts = advfs_get_blkmap_in_range(bfap,
                                                bfap->xtnts.xtntMap,
                                                &storage_offset,
                                                storage_size,
                                                &storage_blkmap,
                                                NULL,
                                                RND_VM_PAGE,
                                                EXB_COMPLETE,
                                                (pflags&FP_NOWAIT)?
                                                XTNT_NO_WAIT|XTNT_LOCKS_HELD:
                                                XTNT_LOCKS_HELD|XTNT_WAIT_OK);
                if (sts != EOK) {
                    /* If the callers cannot wait and we would block
                     * in order to staisfy this request we will fail
                     * right now
                     */

                    MS_SMP_ASSERT(pflags&FP_NOWAIT);
                    release_pages=1;
                    rel_flags = pflags&FP_INVAL&(~FP_DEFAULT);
                    return_status = sts;
                    goto popsicle_stand;
                }
                blkmap_entry = storage_blkmap;
                while (blkmap_entry != NULL) {
                    if ((blkmap_entry->ebd_vd_blk == XTNT_TERM) ||
                        (blkmap_entry->ebd_vd_blk == COWED_HOLE)) {

                        /* We are reading in a hole in the file. We
                         * want to zero-fill the cache pages and write
                         * protect them. If this is a meta-data file
			 * (such as a quota file) don't write protect
			 * in case a meta-data write comes in for the
			 * page.
			 *
			 * Since writes to metadata will alocate storage
			 * before calling getpage, we don't need to write
			 * protect the pages. If we write-protected them
			 * we would have to check/clear write protect on
			 * each metadata write since we wouldn't know it
			 * was a hole.
			 *
			 * If we ever want to allocate storage for
			 * metadata in getpage, we will have to write
			 * protect the pages.
                         */

			if (metadata)
			    protect_page=FALSE;
			else
			    protect_page=TRUE;

                        sts=advfs_bs_zero_fill_pages(vp,
                                                     blkmap_entry->ebd_offset,
                                                     blkmap_entry->ebd_byte_cnt,
                                                     protect_page,
                                                     &plist,
                                                     fc_vminfo,
                                                     ftype,
                                                     NULL,
                                                     FALSE);

                        MS_SMP_ASSERT(sts == EOK);
                        if (fs_priv_param) {
                            fs_priv_param->app_flags |= APP_READ_HOLE;
                        }
                    } else {
                        /* 
                         * We have storage so read data into the pages.
                         * If this file has a small allocation unit and
                         * we're reading the first VM page of the file,
                         * handle it specially since it may be backed
                         * by multiple extents.
                         */
                        if ((small_au_file) &&
                            (blkmap_entry->ebd_offset < (off_t)VM_PAGE_SZ)) {
                            extent_blk_desc_t *temp_entry;

                            /*
                             * This file may have multiple
                             * on-disk extents within the first VM_PAGE_SZ
                             * bytes  of the file.  We will read all of them 
                             * into the current VM page.  To avoid releasing the
                             * page in between individual extent I/O's,
                             * we will tell advfs_start_blkmap_io() that
                             * the I/O length is the sum of the lengths of
                             * the blockmap entries.  Doing this will cause
                             * advfs_start_blkmap_io() to treat the possibly
                             * multiple extent I/O's as one logical I/O.
                             * If the extent map ends in a trailing hole, 
                             * trim it, so that we only pass backed extents
                             * to advfs_start_blkmap_io().
                             */
                            for (size_to_read = 0,
                                 temp_entry = blkmap_entry;
                                 temp_entry;
                                 size_to_read += temp_entry->ebd_byte_cnt,
                                 temp_entry = temp_entry->ebd_next_desc) {

                                 if ((temp_entry->ebd_next_desc != NULL) &&
                                     ((temp_entry->ebd_next_desc->ebd_vd_blk == 
                                                                 XTNT_TERM) ||
                                     (temp_entry->ebd_next_desc->ebd_vd_blk == 
                                                                 COWED_HOLE))) {

                                     (void)advfs_free_blkmaps(
                                             &temp_entry->ebd_next_desc);
                                     temp_entry->ebd_next_desc = NULL;
                                 }
                            }
                            MS_SMP_ASSERT(size_to_read <= VM_PAGE_SZ);

                            /*
                             * If this is a small file, 
                             * advfs_start_blkmap_io() will zero-fill 
                             * the part of the page we won't read from disk.
                             */
                        }
                        else {
                            /*
                             * We will issue one I/O for this blockmap entry.
                             */
                            size_to_read = blkmap_entry->ebd_byte_cnt;
                        }
                        if (process_snaps && HAS_SNAPSHOT_CHILD( bfap )) {
                            /* The pages brought into cache need to be write
                             * protected.  In the future, we could look to
                             * see if this is necessary based on what has
                             * and has not been COWed, but that can come
                             * later.
                             */
                            ADVFS_ACCESS_LOGIT_2( bfap, "advfs_getpage: protecting pages",
                                    alloc_offset, alloc_size);
                            advfs_snap_protect_datafill_plist( bfap, 
                                                        plist,
                                                        blkmap_entry->ebd_offset,
                                                        blkmap_entry->ebd_byte_cnt);
                        }

                        sts = advfs_start_blkmap_io(fc_vminfo,
                                                    vp,
                                                    blkmap_entry->ebd_offset,
                                                    size_to_read,
                                                    blkmap_entry,
                                                    NULL,
                                                    &ioAnchor_head,
                                                    &plist,
                                                    NULL,
                                                    pflags,
                                                    0);
                    
                        did_io = TRUE;
                        MS_SMP_ASSERT(sts == EOK);
                        /*
                         * Let Callers know we did IO.
                         */
                        if (fs_priv_param) {
                            fs_priv_param->app_flags |= APP_IO_PERFORMED;
                        }
                    }
                    /* Move to the next entry in the blkmap */
                    blkmap_entry = blkmap_entry->ebd_next_desc;
                }
                (void)advfs_free_blkmaps(&storage_blkmap);
            } else {
                /* PFDATs found in the cache for reads only need to be
                 * released. They already initialized and in a proper
                 * state.
                 */

                MS_SMP_ASSERT(alloc_status == FPA_ST_EXISTS);
                if (process_snaps && HAS_SNAPSHOT_CHILD(bfap)) {
                    /* If there is a snapshot, then we may need to mark these
                     * pages RO.  This is due to the fact that advfs_putpage
                     * will clear the RO bit in the FP_FREE w/ snapshot case.
                     * VHAND is one caller that may call advfs_putpage and
                     * take that page away from us.  
                     */
                    ADVFS_ACCESS_LOGIT_2( bfap, "advfs_getpage: protecting pages",
                            alloc_offset, alloc_size);
                    advfs_snap_protect_datafill_plist( bfap, 
                            plist,
                            alloc_offset,
                            alloc_size);
                }
                err = fcache_page_release(fc_vminfo, 
                                          alloc_size,
                                          &plist,
                                          pflags);
                MS_SMP_ASSERT(err == 0);
            }
            
            /* We have completed this portion of the read. Update the
             * offset and size that will be returned to the caller if
             * a large page was returned outside of the requests range
             */

            if (alloc_offset < current_loop_offset) {
                /* We were handed a large page to the left.
                 */
                
                if (alloc_offset < return_offset) {
                    /* This large page has caused us to increase the
                     * size of the users request. We need to indicate
                     * to the caller that we processed more than the
                     * request (a large page)
                     */

                    /* Add a big page to the left */
                    return_offset = alloc_offset;
                    return_size += (current_loop_offset - alloc_offset);
                    remaining_request_size += (current_loop_offset - alloc_offset);
                    remaining_loop_size += (current_loop_offset - alloc_offset);
                    current_loop_offset = alloc_offset;

#ifdef KEEP_LGPG_STATS
                    lgpg_stats.getpage_read_lgpg_left++;
#endif
                } else {
                    /* have already processed this page in a previous loop
                     * and VM was able to now return a large page. In this
                     * case we will just update our offset and begin again
                     * on this page.
                     */
                    
                    remaining_request_size += (current_loop_offset - alloc_offset);
                    remaining_loop_size += (current_loop_offset - alloc_offset);
                    current_loop_offset = alloc_offset;
                    
#ifdef KEEP_LGPG_STATS
                    lgpg_stats.getpage_reread_due_to_lgpg_left++;
#endif
                }
            }
            if ((alloc_offset + alloc_size) > (return_size + return_offset)) {
                /* Add a big page to the right */

                return_size = alloc_offset + alloc_size - return_offset;
                remaining_loop_size = alloc_size;
                if (remaining_request_size < remaining_loop_size) {
                    remaining_request_size = remaining_loop_size;
                }
                MS_SMP_ASSERT(return_size > getpage_req_size);
#ifdef KEEP_LGPG_STATS
                lgpg_stats.getpage_read_lgpg_right++;
#endif
            }
        } else {
            /* This begins the processing of the write request.  We
             * will deal with any large pages that cause us to need
             * extra storage, zero, read or release any cache pages,
             * allocate storage if necessary and zero in memory pages
             * that now have storage that are exposed outside of this
             * request.
             */

            /* If we are dealing with snapshots, we don't want to deal with
             * large pages too.  We will demote any large pages we got, then
             * skip processing large pages.  We can only deal with pages to
             * the left at this point but we will setup for large pages to
             * the right to be released later.
             */
            if ((process_snaps) && (snap_maps != NULL)) {
                if ((alloc_offset < return_offset) ||
                    (alloc_offset + alloc_size > return_offset + return_size)) {
                    /* We've got large pages outside of our request range
                     * and snapshots.  Need to release them */

                    if (alloc_status == FPA_ST_DATAFILL) {
                        /* The pages are unitialized and need to be
                         * invalidated. */
                        inv_flags = pflags&FP_INVAL&(~FP_DEFAULT);
                    }
                    
                    if (alloc_offset < return_offset) {
                        err = fcache_page_release(fc_vminfo, 
                                              current_loop_offset - alloc_offset,
                                              &plist,
                                              inv_flags);
                        MS_SMP_ASSERT(err == 0);
                        MS_SMP_ASSERT(plist != NULL);
                    }


                    if (alloc_offset + alloc_size > 
                        remaining_loop_size + current_loop_offset) {
                        /* Tell loop end to invalidate any pages remaining
                         * on the plist. NOTE inv_flags set up above.
                         */
                        invalidate_pages = ((alloc_offset + alloc_size) - 
                                            (remaining_loop_size + current_loop_offset));
                    }

                    /* Make it look like we never got a large page */
                    alloc_size -= current_loop_offset - alloc_offset;
                    alloc_offset = current_loop_offset;
                    alloc_size = MIN(alloc_size, remaining_loop_size);

                }
                goto skip_large_pages;
            }

            /*Deal with any adjustments due to large pages.*/

            if ((alloc_offset < return_offset) ||
                (alloc_offset + alloc_size > return_offset + return_size)) {
                
                uint64_t need_more_storage=0;
                 /* For the moment we should never be handed a large
                  * page that is outside of the bounds of the
                  * page_alloc when write optimiztion is enabled. In
                  * the future this will be an attribute that can be
                  * configured in which case we will decide here to
                  * turn write opt off in favor of large pages.
                  */

#ifdef UFC_WOPT_OPTION3
                pflags &= ~FP_CREATE;
#else
                MS_SMP_ASSERT((alloc_status == FPA_ST_EXISTS) ||
                              !(pflags & FP_CREATE));
#endif

                MS_SMP_ASSERT(alloc_offset <= current_loop_offset);

                /* We need to requery the extent maps to see if the
                 * large page has now caused us to need to aquire more
                 * storage than previously thought.
                 */
                
                sparseness_offset = alloc_offset;
                sparseness_size = alloc_size;
                
                sts = advfs_get_blkmap_in_range(bfap,
                                                bfap->xtnts.xtntMap,
                                                &sparseness_offset,
                                                sparseness_size,
                                                &sparseness_blkmap,
                                                &need_more_storage,
                                                RND_ALLOC_UNIT,
                                                EXB_ONLY_HOLES,
                                                (pflags&FP_NOWAIT)?
                                                XTNT_NO_WAIT|XTNT_LOCKS_HELD|XTNT_NO_MAPS:
                                                XTNT_LOCKS_HELD|XTNT_WAIT_OK|XTNT_NO_MAPS);
                if (sts != EOK) {
                    
                    /* If the callers cannot wait and we would block 
                     * we will fail the write right now 
                     */
                    
                    release_pages = 1;
                    rel_flags = pflags;
                    if (alloc_status == FPA_ST_DATAFILL) {
                        rel_flags = pflags&FP_INVAL&(~FP_DEFAULT);
                    }
                    return_status = sts;
                    goto popsicle_stand;
                }
                
                MS_SMP_ASSERT(sparseness_blkmap == NULL);
                
                if (sparseness_offset < alloc_offset)
                {
                    /* We want to demote the big page since it appears
                     * to not be aligned on our storage allocation
                     * boundary. This could cause a recursive
                     * situation.
                     * We only have to deal with pages to the left
                     * since we can not bring in pages out of order.
                     * We will release any pages that were outside of
                     * the original arguments to page_alloc.
                     * NOTE: We can't release pages out of order so
                     * we will defer releasing pages to the right until
                     * after we have processed all pages in this pass.
                     */
                    
#ifdef KEEP_LGPG_STATS
                    lgpg_stats.getpage_lgpg_alignment_demotion++;
#endif
                    MS_SMP_ASSERT(alloc_offset < current_loop_offset);
                    
                    inv_flags = pflags;
                    
                    if (alloc_status == FPA_ST_DATAFILL) {
                        /* The pages were are about to release are
                         * uninitialized.  We need to invalidate
                         * them
                         */
                        inv_flags = pflags&FP_INVAL&(~FP_DEFAULT);
                    }
                    
                    err = fcache_page_release(fc_vminfo, 
                                              current_loop_offset - alloc_offset,
                                              &plist,
                                              inv_flags);
                    
                    MS_SMP_ASSERT(err == 0);
                    MS_SMP_ASSERT(plist != NULL);
                    
                    if (alloc_offset + alloc_size > 
                        remaining_loop_size + current_loop_offset) {
                        /* We brought in some pages outside of the
                         * request as well. We need to get rid of these
                         * too or loop back up and recheck the blockmaps
                         * to see if we need storage or not (since we can't
                         * distinguish which piece needed storage). Instead
                         * we'll release'em
                         */
                        
                        /* Tell loop end to invalidate any pages remaining
                         * on the plist. NOTE inv_flags set up above.
                         */
                        invalidate_pages = ((alloc_offset + alloc_size) - 
                                            (remaining_loop_size + current_loop_offset));
                    }
                    
                    /* Make it look like we never got a large page */
                    alloc_size -= current_loop_offset - alloc_offset;
                    alloc_offset = current_loop_offset;
                    alloc_size = MIN(alloc_size, remaining_loop_size);
                    need_more_storage=0;
                }
                if ((!request_needs_storage) && (need_more_storage)) {
                    /* The large page caused us to need storage when
                     * didn't already so make sure we are holding the
                     * FILE lock for writing (from a prior call to
                     * GETPAGE if this total write was broken into
                     * multiple calls) or upgrade the lock now.
                     */
                    
#ifdef KEEP_LGPG_STATS
                    lgpg_stats.getpage_lgpg_needs_storage++;
#endif
                    if (pflags&FP_NOWAIT) {
                        /* Sorry guy you lose - what kind of idiot is going to
                         * try to page fault without blocking?
                         */
                        release_pages = 1;
                        inv_flags = pflags;
                        
                        if (alloc_status == FPA_ST_DATAFILL) {
                            /* The pages were are about to release are uninitialized.
                             * We need to invalidate them
                             */
                            inv_flags = pflags&FP_INVAL&(~FP_DEFAULT);
                        }
                        return_status = EWOULDBLOCK;
                        goto popsicle_stand;
                    }
                    if (!writelock_locked) {
                        /* 
                         * We might have called this holding it though,
                         * so double check.
                         */
                        writelock_locked =
			   ADVRWL_FILECONTEXT_ISWRLOCKED(contextp);
                        
                        if (!writelock_locked){
                            /*
                             * We have pfdats locked so we can't sleep
                             * waiting for an upgrade.  If we did, we might
                             * deadlock with another thread holding the file
                             * lock for read and waiting for us to release
                             * the pfdats.
                             */
                            if (ADVRWL_FILECONTEXT_TRYUPGRADE(contextp) != 
                                RWLCK_SUCCESS) {
                                /* We were unable to upgrade the lock to write 
                                 * mode without sleeping.  We continue to hold 
                                 * the lock in read mode so release any pages 
                                 * that are beyond the original request that 
                                 * determined we didn't need storage and go on. 
                                 */
                                inv_flags = pflags;
#ifdef KEEP_LGPG_STATS
                                lgpg_stats.getpage_lgpg_locking_demotion++;
#endif
                                if (alloc_status == FPA_ST_DATAFILL) {
                                    /* The pages were are about to release are 
                                     * uninitialized.  We need to invalidate 
                                     * them.
                                     */
                                    inv_flags = pflags&FP_INVAL&(~FP_DEFAULT);
                                }
                                if ( alloc_offset < current_loop_offset ) {
                                    
                                    err = fcache_page_release(fc_vminfo, 
                                             current_loop_offset - alloc_offset,
                                                              &plist,
                                                              inv_flags);
                                    
                                    MS_SMP_ASSERT(err == 0);
                                    MS_SMP_ASSERT(plist != NULL);
                                }
                                
                                if (alloc_offset + alloc_size > 
                                    remaining_loop_size + current_loop_offset) {
                                    
                                    /* Tell loop end to invalidate any pages 
                                     * remaining on the plist.  NOTE inv_flags 
                                     * set up above.
                                     */
                                    invalidate_pages = 
                                        ((alloc_offset + alloc_size) - 
                                         (remaining_loop_size + 
                                          current_loop_offset));
                                }
                                
                                /* 
                                 * Make it look like we never got a large page.
                                 */
                                alloc_size -= current_loop_offset - 
                                              alloc_offset;
                                alloc_offset = current_loop_offset;
                                alloc_size = MIN(alloc_size,  
                                                 remaining_loop_size);
                                need_more_storage = 0;
                            } else {
#ifdef KEEP_LGPG_STATS
                                lgpg_stats.getpage_lgpg_lock_upgrade_ok++;
#endif
                                writelock_locked = 1;
                            }
                        } 
#ifdef KEEP_LGPG_STATS
                        else {
                            lgpg_stats.getpage_lgpg_lock_upgrade_not_needed++;
                        }
#endif
                    }
                    request_needs_storage = need_more_storage;
                }
            }
            if (alloc_offset < current_loop_offset) {
                /* We were handed a large page to the left.
                 */
#ifdef UFC_WOPT_OPTION3
                pflags &= ~FP_CREATE;
#else
                MS_SMP_ASSERT((alloc_status == FPA_ST_EXISTS) ||
                              !(pflags & FP_CREATE));
#endif
                
                if (alloc_offset < return_offset) {
                    /* This large page has caused us to increase the
                     * size of the users request. We need to indicate
                     * to the caller that we processed more than the
                     * request (a large page)
                     */
                    
                    /* Add a big page to the left */
                    return_offset = alloc_offset;
                    return_size += (current_loop_offset - alloc_offset);
                    remaining_request_size += (current_loop_offset - alloc_offset);
                    remaining_loop_size += (current_loop_offset - alloc_offset);
                    current_loop_offset = alloc_offset;
                    
#ifdef KEEP_LGPG_STATS
                    lgpg_stats.getpage_write_lgpg_left++;
#endif
                } else {
                    /* have already processed this page in a previous loop
                     * and VM was able to now return a large page. In this
                     * case we will just update our offset and begin again
                     * on this page.
                     */
                    
                    remaining_request_size += (current_loop_offset - alloc_offset);
                    remaining_loop_size += (current_loop_offset - alloc_offset);
                    current_loop_offset = alloc_offset;
#ifdef KEEP_LGPG_STATS
                    lgpg_stats.getpage_rewrite_due_to_lgpg_left++;
#endif
                }
            }
            if ((alloc_offset + alloc_size) > (return_size + return_offset)) {
                /* Add a big page to the right */
                
                return_size = alloc_offset + alloc_size - return_offset;
                remaining_loop_size = alloc_size;
                if (remaining_request_size < remaining_loop_size) {
                    remaining_request_size = remaining_loop_size;
                }
                MS_SMP_ASSERT(return_size > getpage_req_size);
#ifdef KEEP_LGPG_STATS
                lgpg_stats.getpage_write_lgpg_right++;
#endif
            }
                
            /* We have finished handling large pages for write
             * intent */
 skip_large_pages:
           
            if (alloc_status == FPA_ST_EXISTS) {
                /* The pages were found in the cache. They are already
                 * initialized with either data or zeroes. In the case
                 * of data our work is done. However ..  In the case
                 * of holes we will leave these pages write protected
                 * and change their protections after storage is
                 * allocated. The write protection will insure that a
                 * racing mmaper can not modify one of these
                 * pages. This is an issue if we are unable to get
                 * storage. We do not want the mmaper to think that
                 * his mofication made it to disk. So by protecting
                 * the pages the mmaper will fault and wait on the
                 * FILE lock until we are done allocating storage.
                 * 
                 * NOTE These pages are not marked dirty since we can
                 * not have a page write protected and dirty at the
                 * same time. Vhand could come along and still one of
                 * these clean pages and we would never have flushed
                 * the zeros to the newly allocated storage. We will
                 * protect against this by looping through each page
                 * after the storage is allocated. We will look for
                 * both cached and data fill pages and fill any
                 * data-fill pages again.
                 *
                 */

#ifdef ADVFS_SMP_ASSERT
                /*
                 * The following debug code asserts that if pages are
                 * found in cache and cover holes, they are write-protected.
                 * If they are found in cache and cover storage, they are
                 * assumed to not be write-protected.  But this code doesn't
                 * work in the case of a small file, whose first and only
                 * VM page could be in either state.
                 */
                if (request_needs_storage && !is_a_small_file) {
                    uint64_t need_storage=0;
                    extent_blk_desc_t *smap_entry;
                    page_fsdata_t *pfdat;

                    pfdat=plist;
                    sparseness_offset = alloc_offset;
                    sparseness_size = alloc_size;
                
                    MS_SMP_ASSERT(current_loop_offset == alloc_offset);
                    MS_SMP_ASSERT(sparseness_offset == alloc_offset);

                    sts = advfs_get_blkmap_in_range(bfap,
                                                    bfap->xtnts.xtntMap,
                                                    &sparseness_offset,
                                                    sparseness_size,
                                                    &sparseness_blkmap,
                                                    &need_storage,
                                                    RND_ALLOC_UNIT,
                                                    EXB_ONLY_HOLES,
                                                    XTNT_LOCKS_HELD|XTNT_WAIT_OK);
            
                    MS_SMP_ASSERT(sts == EOK);
                    /*
                     * In a forced_cow context, we don't add storage to the
                     * bfap... 
                     */
                    MS_SMP_ASSERT((sparseness_offset == alloc_offset) ||
                            forced_cow);
                    
                    smap_entry = sparseness_blkmap;
                    
                    /* Walk thru each entry in the sparseness map that
                     * describes the range that we just processed for
                     * fcache_page_alloc and allocate storage.
                     */
                        
                    while(smap_entry != NULL) {
                        while((pfdat != NULL) &&
                              (pfdat->pfs_off <
                               (off_t)(smap_entry->ebd_offset + 
                                       smap_entry->ebd_byte_cnt))) {
                                
                            if (pfdat->pfs_off >= smap_entry->ebd_offset) {
                                /* This pfdat covers a hole */
                                MS_SMP_ASSERT(pfdat->pfs_info.pi_pg_ro);
                            } else {
                                
                                MS_SMP_ASSERT(!pfdat->pfs_info.pi_pg_ro || process_snaps);
                            }
                            pfdat=pfdat->pfs_next;
                        }
                        smap_entry = smap_entry->ebd_next_desc;
                    }
                    (void)advfs_free_blkmaps(&sparseness_blkmap);
                    sparseness_blkmap = NULL;
                }
#endif

                if (snap_maps == NULL) {
                    /* The pages can just be released. But they may have
                     * been brought into cache as RO preiovusly. If
                     * process_snaps is set, then make sure protections are
                     * correct. */
                    if (process_snaps) {
                        int32_t stg_backed_pages = FALSE;
                        advfs_snap_unprotect_incache_plist( bfap, plist, 
                                alloc_offset, alloc_size, 
                                &stg_backed_pages, 0);
                    }

                    err = fcache_page_release(fc_vminfo, 
                                          alloc_size,
                                          &plist,
                                          pflags);

                    MS_SMP_ASSERT(err == 0);
                    MS_SMP_ASSERT((plist == NULL) || (invalidate_pages));
                } else {
                    /* Some COWing may be required for this page.  This is
                     * where the hoop jumping begins.  For every contiguous
                     * range of the plist that is marked read only, we will
                     * create a fake read and issue writes to any snapshots
                     * that require writes.  For any pages in the plist that
                     * are not read only, they have already been COWed and
                     * can simply be released.  */
                    page_fsdata_t *pfdat;
                    int32_t     is_read_only;
                    int32_t     is_dirty;
                    size_t      contiguous_range;
                    struct buf* bufp;
                    struct buf* snap_buf;
                    
                    while ( (plist != NULL) &&
                            (plist->pfs_off < getpage_req_off_end) ) {
                        pfdat = plist;

                        /* See if we are hunting for contiguous read-only or
                         * writable. */
                        is_read_only = pfdat->pfs_info.pi_pg_ro;
                        is_dirty = pfdat->pfs_info.pi_pg_dirty;
                        contiguous_range = 0;

                        while ( (pfdat != NULL) &&
                                (pfdat->pfs_info.pi_pg_ro == is_read_only) &&
                                (pfdat->pfs_info.pi_pg_dirty == is_dirty) &&
                                (pfdat->pfs_off < getpage_req_off_end) ) {
                            pfdat = pfdat->pfs_next;
                            contiguous_range += VM_PAGE_SZ;
                        }

                        if (!is_read_only) {
                            /* If the pages aren't read-only that it couldn't
                             * need COWing (or something else is screwed up)
                             */
                            err = fcache_page_release(fc_vminfo, 
                                          contiguous_range,
                                          &plist,
                                          pflags);
                            MS_SMP_ASSERT(err == 0);
                        } else {
                            /* Continue the hoop jumping.  We need to create
                             * the fake reads and setup IO anchors then issue
                             * writes to appropriate children snapshots */
                            ioanchor_t* io_anchor = advfs_bs_get_ioanchor( M_WAITOK );
                            int32_t     stg_backed_pages = FALSE;

                            /*
                             * We need to unprotect pages thare are in cache
                             * and not over holes.
                             * advfs_snap_unprotect_incache_plist will return
                             * TRUE for stg_backed_pages if it found any of
                             * the plist pages had storage backing.  If no
                             * pages have storage backing, then there is no
                             * COWing to be done on this plist and it can
                             * just be released.  Otherwise, we will create
                             * a buf associated with the plist and when all
                             * COWing is done, the pages will be released in
                             * advfs_iodone.
                             */
                            advfs_snap_unprotect_incache_plist( bfap, plist, 
                                                        alloc_offset, alloc_size, 
                                                        &stg_backed_pages, 0);
                            GPLOGIT(PAGE_ALLOC,vp,alloc_offset,alloc_size,
                                    pflags,ftype,alloc_status,0,SNAP_UNPROTECT);


                            if (!stg_backed_pages) {
                                /*
                                 * There was no storage under the plist, so
                                 * there couldn't be any COWing to be done.
                                 * The COWed hole was already created by
                                 * advfs_add_snap_stg earlier.  Just release
                                 * the pages.
                                 */
                                err = fcache_page_release(fc_vminfo, 
                                          contiguous_range,
                                          &plist,
                                          pflags);
                                MS_SMP_ASSERT(err == 0);

                            } else {
                                /* 
                                 * There is some COWing that may need to
                                 * be done.  
                                 */
                                io_anchor->anchr_iocounter = 1;
                                io_anchor->anchr_flags = 
                                    IOANCHORFLG_KEEP_ANCHOR|
                                    IOANCHORFLG_WAKEUP_ON_COW_READ|
                                    IOANCHORFLG_CHAIN_ERRORS;
                                bufp = fcache_buf_create( fc_vminfo,
                                                &bfap->bfVnode,
                                                contiguous_range,
                                                0UL, &plist, pflags );
                                if (bufp == NULL) {
                                    /* This could not be an FP_NOWAIT case
                                     * since we would not have added snap
                                     * storage in the no wait case.  So,
                                     * mark the children out of sync and
                                     * carry on with the right.  This
                                     * shouldn't be possible unless the
                                     * fcache_buf_create interface changes.
                                     */
                                    MS_SMP_ASSERT( !(pflags & FP_NOWAIT) );
                                    MS_SMP_ASSERT( 0 ); /* shouldn't get here */

                                    advfs_snap_children_out_of_sync( bfap, cow_ftx );
                                    advfs_bs_free_ioanchor( io_anchor, M_WAITOK );
                                    SNAP_STATS( getpage_buf_create_failure );
                                } else {

                                    /* Continue setting up fake io */
                                    io_anchor->anchr_origbuf = bufp;
                                    snap_buf = (struct buf*)
                                      ms_malloc_no_bzero(sizeof( struct buf ));
                                    bcopy (bufp, snap_buf, sizeof( struct buf ));
                                    io_anchor->anchr_buf_copy = snap_buf;
                                    MS_SMP_ASSERT( !(BGETFLAGS(io_anchor->anchr_buf_copy) & B_DONE) );

                                    if (is_dirty) {
                                        /* 
                                         * This case is not yet supported.  If this
                                         * case does become supported, we must issue
                                         * a write to bfap itself, not to its
                                         * children.  This will syncrhonize with the
                                         * writing of the child snapshots data.
                                         */
                                        MS_SMP_ASSERT( FALSE );
                                    }
                            
                                    sts = advfs_issue_snap_io( io_anchor,
                                                        snap_maps,
                                                        SF_FAKE_ORIG,
                                                        bfap,
                                                        cow_ftx );

                                    /* If advfs_issue_snap_io fails, the children should have
                                     * * been marked out of sync or the domain paniced, there
                                     * * should be nothing to do for an error */
                                    MS_SMP_ASSERT( sts == EOK );
                                }
                            }
                        }
                    }
                }
            } else {
                /* Handle the pages that were not found in the cache. */

                extent_blk_desc_t *blkmap_entry;

                MS_SMP_ASSERT(alloc_status == FPA_ST_DATAFILL);

                storage_offset = alloc_offset;
                if ((is_a_small_file) && 
                    (alloc_offset < (off_t)VM_PAGE_SZ) &&
                    (bfap->file_size)) {
                    /*
                     * This is a small but not zero-length file.  Get
                     * the block map for the entire file rounded up to
                     * allocation unit.
                     */
                    storage_size = roundup(bfap->file_size, 
                                           bfap->bfPageSz * ADVFS_FOB_SZ);
                }
                else {
                    storage_size = alloc_size;
                }

                sts = advfs_get_blkmap_in_range(bfap,
                                                bfap->xtnts.xtntMap,
                                                &storage_offset,
                                                storage_size,
                                                &storage_blkmap,
                                                NULL,
                                                RND_VM_PAGE,
                                                EXB_COMPLETE,
                                                (pflags&FP_NOWAIT)?
                                                XTNT_NO_WAIT|XTNT_LOCKS_HELD:
                                                XTNT_LOCKS_HELD|XTNT_WAIT_OK);
                if (sts != EOK) {
                    /* If the callers cannot wait and we would block or we
                     * need to allocate storage in order to staisfy this write
                     * we will fail the write right now 
                     */
                    /* Since these pages were not initialized we must
                     * invalidate them.
                     */

                    release_pages = 1;
                    rel_flags = pflags&FP_INVAL&(~FP_DEFAULT);
                    return_status = sts;
                    goto popsicle_stand;
                }
                blkmap_entry = storage_blkmap;
                /* Loop through each range to determine if the page
                 * needs to be initialized and, if so, whether to read
                 * from disk or zero in memory. 
                 */
                
                while (blkmap_entry != NULL) {
                    off_t blkmap_entry_end = blkmap_entry->ebd_offset +
                                             blkmap_entry->ebd_byte_cnt - 1;

                    if ((blkmap_entry->ebd_vd_blk != XTNT_TERM) &&
                        (blkmap_entry->ebd_vd_blk != COWED_HOLE)) {

                        /* The pages will need to be initialized if
                         * FP_CREATE is not set or the requested
                         * offset is not page aligned or the requested
                         * ending offset is not page aligned.
                         * If this file has a small allocation unit and
                         * we're writing the first VM page of the file,
                         * handle it specially since it may be backed
                         * by multiple extents.
                         */
                        if ((!(pflags & FP_CREATE)) || 
                            ((small_au_file) &&
                             (blkmap_entry->ebd_offset < (off_t)VM_PAGE_SZ))) {

                            /*
                             * Read all pages. We need to read preallocated
                             * space from disk, since we could be page faulting
                             * in a write system call and have written data
                             * to disk before having updated the file size.
                             * (ie A page is stolen in the middle of a uiomve).
                             */
                            if ((small_au_file) &&
                                (blkmap_entry->ebd_offset < 
                                 (off_t)VM_PAGE_SZ)) {
                                extent_blk_desc_t *temp_blkmap_entry;
                                /*
                                 * This is or was a small file.  It may 
                                 * have multiple on-disk extents within the 
                                 * first VM_PAGE_SZ bytes of the file.  We 
                                 * will read all of them into the current 
                                 * VM page.  To avoid releasing the page 
                                 * in between individual extent I/O's, we 
                                 * will tell advfs_start_blkmap_io() that  
                                 * the I/O length is the sum of the lengths 
                                 * of the blockmap entries.  Doing this 
                                 * will cause advfs_start_blkmap_io() to 
                                 * treat the possibly multiple extent I/O's 
                                 * as one logical I/O.  That which we do 
                                 * not read, we will zero:  Zero, ergo sum.
                                 */
                                for (size_to_read = 0,
                                         temp_blkmap_entry = blkmap_entry;
                                     temp_blkmap_entry;
                                     size_to_read += 
                                         temp_blkmap_entry-> ebd_byte_cnt,
                                         temp_blkmap_entry = 
                                         temp_blkmap_entry->ebd_next_desc);
                                
                                MS_SMP_ASSERT(size_to_read <= VM_PAGE_SZ);

                                /*
                                 * If this is a small file, 
                                 * advfs_start_blkmap_io() will zero-fill 
                                 * the part of the page we won't read from 
                                 * disk.
                                 */
                            } else {

                                size_to_read = blkmap_entry->ebd_byte_cnt;
                            }
                            /* 
                             * If there are snapshots to deal with, pass
                             * the ADVIOFLG_SNAP_READ flag so that the
                             * ioanchor's created will have an iocounter
                             * of 2 and special flags set up so we can
                             * issue writes later.
                             */
                            sts = advfs_start_blkmap_io(fc_vminfo,
                                                        vp,
                                                        blkmap_entry->ebd_offset,
                                                        size_to_read,
                                                        blkmap_entry,
                                                        NULL,
                                                        &ioAnchor_head,
                                                        &plist,
                                                        NULL,
                                                        pflags,
                                                        snap_maps ? 
                                                        ADVIOFLG_SNAP_READ : 0);
                    
                                did_io = TRUE;
                                MS_SMP_ASSERT(sts == EOK);
                        } else {
                            /* 
                             * The FP_CREATE flag is set.  We need to
                             * deal with pages that fall outside of
                             * the range that will be totally
                             * overwritten. This range is determined
                             * by the range passed to getpage. If we
                             * adjusted the range at all it is our
                             * responsibility to initialize any pages
                             * outside of this range.
                             */
 
                            if ((blkmap_entry->ebd_offset < 
                                 fp_create_starting_offset) ||
                                (blkmap_entry_end > fp_create_ending_offset)) {
                                /* 
                                 * We know that the pages are not in cache
                                 * and that they will not be completely
                                 * overwritten by the caller.  We also
                                 * know that there is backing storage for
                                 * them.  In fact, since we're looping
                                 * through extent descriptors, the pages
                                 * that are associated with each extent
                                 * descriptor can all be read in one I/O.
                                 * So rather than looping through the
                                 * pfdats, we should process each extent
                                 * descriptor in three steps:
                                 *
                                 * 1. Initiate I/O on all pages that are
                                 *    to the left of the write request,
                                 *    including the very first page
                                 *    of the request, if it doesn't
                                 *    fall on a page boundary.
                                 *
                                 * 2. Release all pages that will be
                                 *    completely overwritten.
                                 *
                                 * 3. If there is still a plist, it should
                                 *    be for one page at the end of the 
                                 *    request, in the case of write requests
                                 *    that are not page-aligned.  In this
                                 *    case, if the start of the page is
                                 *    beyond EOF, we will just zero-fill it,
                                 *    since it should be zero-filled on
                                 *    disk.  Otherwise, we will read it from
                                 *    disk.
                                 */
                                if (blkmap_entry->ebd_offset < 
                                    fp_create_starting_offset) {
                                    /* Step 1 */
                                    size_to_read = fp_create_starting_offset - 
                                                   blkmap_entry->ebd_offset;
                                    MS_SMP_ASSERT(size_to_read <= 
                                                  blkmap_entry->ebd_byte_cnt);
                                    
                                    sts = advfs_start_blkmap_io(fc_vminfo,
                                                                vp,
                                                       blkmap_entry->ebd_offset,
                                                                size_to_read,
                                                                blkmap_entry,
                                                                NULL,
                                                                &ioAnchor_head,
                                                                &plist,
                                                                NULL,
                                                                pflags,
                                                                0);
                        
                                    did_io = TRUE;
                                    MS_SMP_ASSERT(sts == EOK);
                                }
                                if ((plist) &&           /* Steps 2 & 3 */
                                    (blkmap_entry_end > fp_create_ending_offset)) {
                                    /* This should only be the case when we are
                                     * at the end of the entire write request
                                     * (write_req_off_end).
                                     */
    
                                    MS_SMP_ASSERT(fp_create_ending_offset ==
                                                  rounddown(write_req_off_end,
                                                            VM_PAGE_SZ) - 1);

                                    /*
                                     * Step 2
                                     */
                                        if (plist->pfs_off < 
                                            fp_create_ending_offset){

                                        /* First we need to release any
                                         * pages that we will not be
                                         * servicing since we must always
                                         * work on the beginning of the
                                         * plist.
                                         */
                                        
                                        err = fcache_page_release(fc_vminfo,
                                                                  (fp_create_ending_offset -
                                                                   plist->pfs_off + 1),
                                                                  &plist,
                                                                  pflags);
                                        
                                        MS_SMP_ASSERT(err == 0);
                                    }

                                    /*
                                     * Step 3
                                     */
                                    if ((fp_create_ending_offset + 1) < 
                                        (off_t)file_size) {
                                        /* 
                                         * The last page of the write
                                         * request is only partially covered
                                         * by the write and it is within the
                                         * bounds of the file.  Read it in
                                         * from disk.
                                         */
                                        
                                        MS_SMP_ASSERT(plist != NULL);
    
                                        size_to_read = 
                                            MIN((roundup(file_size, VM_PAGE_SZ) -
                                                 (fp_create_ending_offset + 1)),
                                                (blkmap_entry_end - 
                                                 fp_create_ending_offset));
    
                                        MS_SMP_ASSERT(size_to_read == 
                                                      VM_PAGE_SZ);
                                        
                                        sts = advfs_start_blkmap_io(fc_vminfo,
                                                                    vp,
                                                       (fp_create_ending_offset + 1),
                                                                   size_to_read,
                                                                   blkmap_entry,
                                                                    NULL,
                                                                 &ioAnchor_head,
                                                                    &plist,
                                                                    NULL,
                                                                    pflags,
                                                                    0);
    
                                        did_io = TRUE;
                                        MS_SMP_ASSERT(sts == EOK);
                                        
                                    }
                                    else {

                                        /*
                                         * The last page of the write
                                         * request is only partially covered
                                         * by the write but it is not within
                                         * the bounds of the file.  Rather
                                         * than read in zeroes from disk,
                                         * just zero-fill the last page.
                                         */
                                        MS_SMP_ASSERT(plist != NULL);
                                        MS_SMP_ASSERT(blkmap_entry_end >
                                                      file_size);
                                        MS_SMP_ASSERT((blkmap_entry_end + 1 -
                                                      plist->pfs_off) ==
                                                      VM_PAGE_SZ);
                                        sts=advfs_bs_zero_fill_pages(vp,
                                                    plist->pfs_off,
                                                    (blkmap_entry_end + 1) - 
                                                    plist->pfs_off,
                                                FALSE, /* Don't write protect */
                                                    &plist,
                                                    fc_vminfo,
                                                    ftype,
                                                    NULL,
                                                    FALSE);
                                        
                                        MS_SMP_ASSERT(sts == EOK);
                                        MS_SMP_ASSERT(plist == NULL || 
                                        (blkmap_entry->ebd_next_desc != NULL));
                                        MS_SMP_ASSERT(invalidate_pages == 0);
                                    }
                                }
                            }  /* End of handling pages outside request */

                            if (plist) {
                                /*
                                 * We get here for one of two reasons:
                                 *
                                 * 1.  The pages returned from
                                 *     fcach_page_alloc() do not extend beyond
                                 *     either end of the write request so we're
                                 *     going to completely overwrite these.  In
                                 *     this case, just release them.
                                 *
                                 * 2.  The rest of the pages returned from
                                 *     fcache_page_alloc() are going to be
                                 *     completely overwritten as the write
                                 *     request ends on a page boundary.  In
                                 *     this case, too, we just want to
                                 *     release them.
                                 */
                                size_t size_to_release = 
                                                 (blkmap_entry->ebd_offset + 
                                                  blkmap_entry->ebd_byte_cnt) - 
                                                 plist->pfs_off;

                                MS_SMP_ASSERT(plist->pfs_off >= 
                                              blkmap_entry->ebd_offset);

                                if (size_to_release > 0) {
                                    /* Never call this with a zero size or
                                     * it will release all the pages !
                                     */
                                    err = fcache_page_release(fc_vminfo, 
                                                              size_to_release,
                                                              &plist,
                                                              pflags);
                                        
                                    MS_SMP_ASSERT(err == 0);
                                }
                            }
                        } /* End of FP_CREATE handling */

                    } else {

                        size_t size_to_zero;
                        off_t zero_offset;

                        /* This blkmap entry represents a hole in the file */

                        /* The following should operate on all the
                         * pages returned from fcache_page_alloc that
                         * encompass this range of sparseness.
                         */

                        /*
                         * The request should generally need storage if we
                         * are in this case, but we may be in the case were
                         * we have rounded out to cover snapshots and the
                         * actual write did not require storage but there is
                         * a hole further out in the rounded region.  
                         */
                        MS_SMP_ASSERT(request_needs_storage || 
                                ( blkmap_entry->ebd_offset > 
                                  (off_t)(orig_sparseness_offset +
                                         orig_sparseness_size) ) ||
                                ( (off_t)(blkmap_entry->ebd_offset +
                                          blkmap_entry->ebd_byte_cnt) <
                                  (orig_sparseness_offset) ) );

                        MS_SMP_ASSERT(plist);
                        MS_SMP_ASSERT(plist->pfs_off == 
                                      blkmap_entry->ebd_offset);
                        
                        /* Ideally we wouldn't allocate the pages.  How do
                         * we make VM think we did the fault but really
                         * didn't...  If we release these pages then we get
                         * into an infinite loop case in preg_vn_fault.
                         */

                        if (!(pflags&FP_CREATE) || (will_remain_a_small_file)) {
                            /* 
                             * We need to zero-fill all pages that are
                             * about to receive newly allocated
                             * storage since we do not know if they
                             * will be overwritten.
                             */
                            sts=advfs_bs_zero_fill_pages(vp,
                                                     blkmap_entry->ebd_offset,
                                                     blkmap_entry->ebd_byte_cnt,
                                                     TRUE, /* write protect */
                                                     &plist,
                                                     fc_vminfo,
                                                     ftype,
                                                     NULL,
                                                     FALSE);
                                    
                            MS_SMP_ASSERT(sts == EOK);
                            /* We will, in advfs_fs_zero_fill protect
                             * the pages after zeroing them. Since an
                             * mmaper could find these in the cache
                             * without going thru getpage thus
                             * avoiding the file lock and then msync
                             * the pages and putpage will ignore them
                             */
                        } else {
                            /* We have to zero any pages that are not
                             * protected by FP_CREATE. This will also
                             * protect and dirty these pages.
                             */
                            if (blkmap_entry->ebd_offset < fp_create_starting_offset){
                                
                                size_to_zero = MIN(blkmap_entry->ebd_byte_cnt,
                                                   (fp_create_starting_offset -
                                                    blkmap_entry->ebd_offset));
                                
                                sts=advfs_bs_zero_fill_pages(vp,
                                                       blkmap_entry->ebd_offset,
                                                       size_to_zero,
                                                       TRUE, /* write protect */
                                                       &plist,
                                                       fc_vminfo,
                                                       ftype,
                                                       NULL,
                                                       FALSE);
                                    
                                MS_SMP_ASSERT(sts == EOK);
                            }

                            if ((plist) && 
                                (blkmap_entry_end > fp_create_ending_offset)) {
                                
                                /* This should only be the case when we are
                                 * at the end of the entire write request
                                 * (write_req_off_end).
                                 */
    
                                MS_SMP_ASSERT(fp_create_ending_offset ==
                                              rounddown(write_req_off_end,
                                                        VM_PAGE_SZ) - 1);
                                
                                /* First we need to release any pages
                                 * that we are not going to zero-fill
                                 * since we must always operate on the
                                 * head of the plist.
                                 */
                        
                                size_t size_to_release = 
                                    fp_create_ending_offset -
                                    plist->pfs_off + 1;

                                if (size_to_release > 0) {

                                    err = fcache_page_release(fc_vminfo, 
                                                              size_to_release,
                                                              &plist,
                                                              pflags);
                                    
                                    MS_SMP_ASSERT(err == 0);
                                }

                                zero_offset = fp_create_ending_offset + 1;
                                size_to_zero = (blkmap_entry_end + 1) -
                                               zero_offset;

                                /*
                                 * The following assertion is valid because
                                 * it is our understanding that VM will
                                 * never hand us a large page if FP_CREATE
                                 * is set.  
                                 */
                                MS_SMP_ASSERT(size_to_zero == VM_PAGE_SZ);

                                sts=advfs_bs_zero_fill_pages(vp,
                                                      zero_offset,
                                                      size_to_zero,
                                                      TRUE, /* write protect */
                                                      &plist,
                                                      fc_vminfo,
                                                      ftype,
                                                      NULL,
                                                      FALSE);
                                
                                MS_SMP_ASSERT(sts == EOK);
                            }

                            if (plist) {
                                /*
                                 * If we still have a page list, we can release 
                                 * the remaining pages.  They do not need to
                                 * be zeroed as they will be completely
                                 * overwritten.
                                 */
                                size_t size_to_release = 
                                                 (blkmap_entry->ebd_offset + 
                                                  blkmap_entry->ebd_byte_cnt) - 
                                                 plist->pfs_off;

                                MS_SMP_ASSERT(plist->pfs_off >= blkmap_entry->ebd_offset);

                                if (size_to_release > 0) {
                                    err = fcache_page_release(fc_vminfo, 
                                                              size_to_release,
                                                              &plist,
                                                              pflags);
                                    MS_SMP_ASSERT(err == 0);
                                }
                            }
                        } /* End of FP_CREATE */
                    } /* End of writing a hole loop */


                    blkmap_entry = blkmap_entry->ebd_next_desc;

                } /* End of blkmap loop */
                
                (void)advfs_free_blkmaps(&storage_blkmap);

            } /* End of pages not found in the cache */           

        } /* End of write intent processing */
        
        if (invalidate_pages) {
            /* We can get here for a few reasons. We could have been
             * handed a large page from the UFC and were unable to
             * deal with it for storage reasons or we were unable to
             * upgrade the file lock due to a large page. We continued
             * to process pages that were within the passed in request
             * but now need to invalidate those pages that were
             * outside of the passed in request. NOTE the inv_flags
             * may actually not contain the FP_INVAL flag.
             */

            MS_SMP_ASSERT(plist);

            err = fcache_page_release(fc_vminfo, 
                                      invalidate_pages,
                                      &plist,
                                      inv_flags);
            
            MS_SMP_ASSERT(err == 0);
        }

        MS_SMP_ASSERT(plist == NULL);


        /* 
         * Drop the extent maps lock(s). They are not needed for the
         * remainder of the fcache_page_alloc loop.
         */
        if (process_snaps) {
            advfs_drop_xtntMap_locks( bfap, (ftype&FCF_DFLT_READ)?
                    SF_SNAP_READ : SF_SNAP_NOFLAGS );
        } else {
            ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );
        }
        xtntmap_locked=0;

        /* In the case of a write that encountered a hole we will now
         * allocate storage.  This has been postponed until after all
         * pages are released since we do not want to cause a page
         * fault while we are holding the pages locked. The
         * synchronization of pages in the cache with no storage 
         * is handled by not marking the pages as dirty and write 
         * protecting them. They wont be pushed (clean) and an
         * accessor will page fault and block (write protect)
         * 
         */
 just_add_storage:          
        if (request_needs_storage && !forced_cow) {

            /* We now may need to allocate storage for holes that we
             * zeroed above. We could not allocate storage while we
             * were holding page locks. We took care of zeroing all
             * memory pages (unless the FP_CREATE flag was set in
             * which case they will be overwritten) now we need to
             * allocate the storage
             *
             * NOTE we know that some portion of this request needs
             * storage. The getpage may have broken the request into
             * multiple parts that we are processing so some of these
             * parts may not need storage.
             */

            uint64_t need_storage=0;
            extent_blk_desc_t *smap_entry, *last_entry=NULL;
            off_t ending_storage_offset=0;
            off_t current_storage_offset = 0;

            if (is_a_small_file && alloc_offset < (off_t)VM_PAGE_SZ) {
                sparseness_offset = 0;
                sparseness_size = MIN(roundup(getpage_req_off_end + 1, 
                                              bfap->bfPageSz * ADVFS_FOB_SZ),
                                      VM_PAGE_SZ);
            }
            else {
                sparseness_offset = alloc_offset;
                sparseness_size = alloc_size;
            }

            /* We can only adjust sparseness_offset and sparseness_size
             * based on the orig_sparseness values if we didn't get large
             * pages.  We would have demoted large pages if we were
             * dealing with snapshots and doing a write. We are trying to
             * avoid adding storage to the original because of the fact that
             * we rounded out the write for COWing.
             */
            if ((process_snaps) && (snap_maps) ) {

                /*
                 * If all the storage we would try to add is all outside of
                 * the orig sparseness range, then just skip adding storage
                 * completely.  This is the case where we rounded out for
                 * COWing and are now finding a hole in the parent that is
                 * completely outside of the actual fault request (the
                 * un-COW rounded request).  For instance, lets set we
                 * faulted for 4k at offset 32k in a file.  We may have
                 * rounded out to COW the range [0..64k].  At this point, we
                 * may find that we are about to add storage to offset 56k.
                 * So we will just skip adding the storage and leave the
                 * page in cache as a hole.  
                 */
                if ( (sparseness_offset > 
                      (off_t)(orig_sparseness_offset + orig_sparseness_size)) ||
                     ( (off_t)(sparseness_offset + sparseness_size) <
                       (orig_sparseness_offset) ) ) {
                    goto done_with_add_stg;
                }
                
                /* 
                 * We want to add some storage, but trim the range to be
                 * within the orig sparseness range
                 */
                sparseness_offset = max (orig_sparseness_offset,
                                                sparseness_offset );
                sparseness_size = min( orig_sparseness_offset +
                                        orig_sparseness_size,
                                        alloc_offset + alloc_size ) -
                                    sparseness_offset;

                                       
                /*
                 * We may find we don't really want to add storage to this
                 * range.  In that case, just skip adding storage and don't
                 * unprotect any of the pages as they should already be
                 * setup correctly
                 */
                if (sparseness_size <= 0) {
                    goto done_with_add_stg;
                }
            }
                
            MS_SMP_ASSERT(current_loop_offset == alloc_offset);
            MS_SMP_ASSERT((sparseness_offset == alloc_offset) ||
                          (process_snaps && snap_maps) ||
                          (small_au_file && alloc_offset < (off_t)VM_PAGE_SZ));

            sts = advfs_get_blkmap_in_range(bfap,
                                            bfap->xtnts.xtntMap,
                                            &sparseness_offset,
                                            sparseness_size,
                                            &sparseness_blkmap,
                                            &need_storage,
                                            RND_ALLOC_UNIT,
                                            EXB_ONLY_HOLES,
                                            XTNT_WAIT_OK);
            
            if (sts != EOK) {
                /* If the callers cannot wait and we would block or we
                 * need to allocate storage in order to staisfy this write
                 * we will fail the write right now 
                 */
                /* Since these pages were not initialized we must
                 * invalidate them Release locks, INVALIDATE
                 * pages */
                MS_SMP_ASSERT(sts == E_WOULD_BLOCK);
                return_status = EWOULDBLOCK;
                goto popsicle_stand;
            }

            MS_SMP_ASSERT((sparseness_offset == alloc_offset) ||
                          (process_snaps && snap_maps) ||
                          (small_au_file && alloc_offset < (off_t)VM_PAGE_SZ));


            smap_entry = sparseness_blkmap;

            /* Walk thru each entry in the sparseness map that
             * describes the range that we just processed for
             * fcache_page_alloc and allocate storage.
             */

            while (smap_entry != NULL) {
                off_t current_storage_offset;
                size_t storage_size;
                size_t bytes_to_add;
                off_t  zero_offset;
                off_t  nbpg_offset;
                size_t nbpg_size;
                /*
                 * Storage size represents the amount of storage (identified
                 * by the sparseness map) needed for pages brought into the
                 * cache during this iteration of the page_alloc loop.  
                 *
                 * bytes_to_add will reflect the largest amount of storage
                 * needs to satisfy the entire write request; however,
                 * bytes_to_add is not a hard limit on the amount of storage
                 * to be added.  In other words, preallocation or allocation
                 * unit considerations may cause more that bytes_to_add
                 * to be added.
                 *
                 * bytes_to_add will cover the entire request whereas
                 * storage_size will only cover the first sparse range of
                 * pages in the current plist.  After the call to
                 * advfs_bs_add_stg, bytes_to_add will reflect the actual
                 * number of bytes of storage added.
                 *
                 * The difference between storage_size and bytes_to_add AFTER
                 * advfs_bs_add_stg will indicate the amount of zero-filling
                 * required in the non-FP_CREATE.  In the FP_CREATE
                 * case, only the range outside of the FP_CREATE range needs
                 * to be zero-filled.
                 */

                current_storage_offset = smap_entry->ebd_offset;
                storage_size = smap_entry->ebd_byte_cnt;
                
                if (will_remain_a_small_file) {
                    /*
                     * If this is and will remain a small file, don't
                     * include VM page offsets (alloc_offset and alloc_size)
                     * in the calculation for bytes_to_add.
                     */
                    bytes_to_add = (write_req_off_end + 1)
                                    - current_storage_offset;
                }
                else if (small_au_file && 
                         current_storage_offset < (off_t)VM_PAGE_SZ) {
                    /*
                     * If this is a small AU file but will not
                     * remain a small file, and if we are currently 
                     * backfilling a hole in the first VM_PAGE_SZ bytes
                     * of the file, limit the bytes to add so that 
                     * we only back the first VM_PAGE_SZ bytes of the file
                     * in this pass.
                     */
                    bytes_to_add = VM_PAGE_SZ - current_storage_offset;
                    is_a_small_file = FALSE;
                }
                else {
                    /* 
                     * This will cause the storage allocation code to
                     * allocate storage up to the size of the request
                     * avoiding potential disk fragmentation that would be
                     * caused by allocating only the request size. A large
                     * page could have brought us beyond our request size
                     * so we need to take that into account when adding 
                     * storage as well.
                     */
                    bytes_to_add = MAX((size_t)(write_req_off_end + 1),
                                       (alloc_offset + alloc_size))
                                       - current_storage_offset;
                    
                    /*
                     * If this is a small allocation unit file, round up
                     * the storage request so it will end on a VM_PAGE_SZ
                     * boundary.
                     */
                    if (small_au_file) {
                        MS_SMP_ASSERT(current_storage_offset + bytes_to_add >=
                                      VM_PAGE_SZ);
                        bytes_to_add = 
                            roundup(current_storage_offset + bytes_to_add,
                                    VM_PAGE_SZ) - current_storage_offset;
                    }
                }

                /* All rounding to allocation units will be done
                 * inside the storage allocation routines. We will not
                 * round here. The returned size will be rounded to
                 * allocation units.
                 */

                MS_SMP_ASSERT(bytes_to_add > 0);
                MS_SMP_ASSERT( ADVRWL_FILECONTEXT_ISWRLOCKED(contextp) );

                if (!mig_stg_locked) {
                    /*
                     * If we need to add storage, we need to take the
                     * migStgLk out and hold it until this storage is
                     * initialized.  This will prevent migrate from faulting
                     * in pages that are not initialized and causing them to
                     * be in cache and not read-protected when getpage tries
                     * to unprotect any in-cache pages under which storage
                     * was added.
                     */
                    ADVRWL_MIGSTG_READ( bfap );
                    mig_stg_locked = TRUE;
                }

                if (process_snaps) {
                    fs_priv_param->app_flags |= APP_SNAP_LOCK_HELD;
                }

                if ( allow_only_1_dio_obj_safety_allocation >= 1 ) {
                    /*
                     * Direct I/O with Object Data Safety (blkclear) has
                     * issues.  Object Data Safety causes advfs_bs_add_stg() to
                     * return with an active transaction and expects
                     * advfs_getpage() to zero the storage then do an
                     * ftx_done(). But the call from advfs_fs_write_direct()
                     * bypasses the UFC, so advfs_bs_zero_fill_pages() can not
                     * be called.  Thus advfs_getpage() returns the ftx handle
                     * to advfs_fs_write_direct() and allows the Direct I/O
                     * code do the storage initialization and then ftx_done().
                     * But this means we can not make a second allocation
                     * in advfs_getpage() because we have not ftx_done()'ed the
                     * first transaction yet (multiple allocations can happen
                     * if a write covers multiple sparse holes, or the storage
                     * has to be obtained from multiple volumes).  So we use
                     * the same logic advfs_getpage() would use for ENOSPC, and
                     * let advfs_fs_write_direct() sort it all out.
                     */
                    MS_SMP_ASSERT(allow_only_1_dio_obj_safety_allocation == 1);
                    sts = E_DIO_OBJ_SAFETY_PARTIAL_ALLOC;
                }
                else {
                    /*
                     * Make sure no sneaky mmapper adds storage beyond the
                     * end of the file.
                     */
                    MS_SMP_ASSERT(!((mmapped_file) && 
                                    (current_storage_offset >= file_size))); 

                    sts=advfs_bs_add_stg(bfap, 
                                         current_storage_offset,
                                         &bytes_to_add,
                                         file_size,
                                         TNC_CRED(),
                                         fs_priv_param,
                                         cow_ftx,      /* Parent, probably
                                                          FtxNilFtxH */
                                         &ftxH,
                                         &delCnt,
                                         &delList);
                }
                if (process_snaps) {
                    fs_priv_param->app_flags &= ~APP_SNAP_LOCK_HELD;
                }
                if (sts != EOK) {
                    /* This assumes that the entire call to
                     * advfs_bs_add_stg was failed. None of the
                     * request was satisfied. We will invalidate
                     * all pages from this offset to the last 
                     * page brought in from page_alloc.
                     */
                    if (process_snaps && HAS_SNAPSHOT_PARENT( bfap ) ) {
                        /* We are writing to a child snapshot and it just
                         * failed to get storage.  If the extent was
                         * unmapped (XTNT_TERM) the snapshot is now out of
                         * sync.  If it was a COWED_HOLE then it is still in
                         * sync, but didn't get the storage it wanted.
                         */
                        if (smap_entry->ebd_vd_blk == XTNT_TERM) {
                            /* TODO: shouldn't we check for return values? */
                            (void) advfs_snap_out_of_sync( bfap->tag,
                                                bfap, 
                                                bfap->bfSetp,
                                                cow_ftx );
                            (void) advfs_snap_children_out_of_sync( bfap,
                                                cow_ftx );
                        }
                    }

                    MS_SMP_ASSERT(!(BS_IS_META(bfap)));
                   
                    /*
                     * We are going to flush and invalidate any pages in the
                     * fcache_page_alloc() range that we have not used.  We
                     * flush because it is possible there are pre-existing
                     * dirty pages in the range from some previous writes.
                     */
                    if ( !addstg_nocache_specified ) {
                        uint32_t tmp_flag = 0;

                        if (!fs_priv_param) {
                            /*
                             * Oh, snap!  We need a temporary structure.
                             */
                            fs_priv_param = (struct advfs_pvt_param *)
                                            ms_malloc( 
                                                    sizeof(
                                                        struct advfs_pvt_param) 
                                                    );
                            tmp_flag++;
                        }

                        nbpg_offset = current_storage_offset;
                        nbpg_size   = remaining_loop_size - 
                                (current_storage_offset - current_loop_offset);
                        MS_SMP_ASSERT( nbpg_offset % NBPG == 0 &&
                                       nbpg_size   % NBPG == 0 &&
                                       nbpg_size          != 0 );

                        /*
                         * If snapshots exist and advfs_putpage() gets
                         * invoked, putpage would try to acquire the
                         * bfaSnapLock.  We pass APP_SNAP_LOCK_HELD so that
                         * putpage will skip this step.
                         */
                        fs_priv_param->app_flags |= APP_SNAP_LOCK_HELD;
                        err = fcache_vn_flush(vp,
                                              nbpg_offset,
                                              nbpg_size,
                                              (uintptr_t)fs_priv_param,
                                              FVF_WRITE | FVF_SYNC | FVF_INVAL);
                        fs_priv_param->app_flags &= ~APP_SNAP_LOCK_HELD;

                        if (tmp_flag) {
                            ms_free(fs_priv_param);
                            fs_priv_param = NULL;
                        }
                    
                        MS_SMP_ASSERT(err == 0);
                    }

                    if (last_entry) {
                        /* We satisfied some of this request. We
                         * allocated some storage for prior pages. For
                         * this reason we need to continue on and
                         * unprotect these pages below. We will make
                         * the request look smaller so the unprotect
                         * loop will only operate on pages processed.
                         */
                        
                        /* Clip the list */
                        last_entry->ebd_next_desc = NULL;
                        /* Since there was some storage added we will
                         * make it look like this is where the request
                         * stopped. ending_storage_offset is still set
                         * from the last storage request that was
                         * fulfilled. We just need to adjust what was
                         * brought into the cache.
                         */

                        /* Free all enties from this blkmap on */
                        (void)advfs_free_blkmaps(&smap_entry);
                    } else {
                        /* There was no previous allocations made
                         * so make it look like we never allocated any 
                         * storage
                         */
                        
                        need_storage = 0;
                    }

                    /*
                     * Adjust alloc_size so that it no longer includes
                     * the pages we invalidated.
                     */
                    alloc_size = current_storage_offset - alloc_offset;

                    /*
                     * Adjust return_size so that it doesn't include
                     * bytes beyond current_storage_offset.
                     */
                    return_size -= (current_loop_offset + remaining_loop_size) -
                                   current_storage_offset;

                    return_status = sts;
                    break;
                } 

                ADVFS_ACCESS_LOGIT_2(bfap, "advfs_bs_add_stg added storage",
                        current_storage_offset, bytes_to_add);

                if (is_a_small_file && !will_remain_a_small_file &&
                    current_storage_offset < (off_t)VM_PAGE_SZ) {
                    is_a_small_file = FALSE;
                }
                
                /*
                 * If direct I/O is calling, signal to it that storage
                 * was successfully added.
                 */
                if (addstg_nocache_specified) {
                    fs_priv_param->app_flags |= APP_ADDEDSTG_NOCACHE;
                }

                GPLOGIT(GETPAGE_CALL,vp,current_storage_offset,bytes_to_add,
                        pflags,ftype,0,0,ADDED_STORAGE);
                
                /* Keep track of the highest storage offset added. We
                 * need this for unprotecting any pages that may be in
                 * the cache from a prior read.
                 */

                ending_storage_offset = current_storage_offset + bytes_to_add;
                
                /* Tell sync writers to flush the extent map.  Fsync
                 * will also flush if we set the dirty_stats below.
                 * having the write lock is the only protection needed
                 */

                contextp->dirty_alloc = 1;
                if ((!addstg_nocache_specified) &&
                    (fs_priv_param)) {
                    /* We need to tell the write system call about any
                     * storage we added in case there is an error. The
                     * user code have a bogus source buffer for example
                     * and we will have just allocated storage and not
                     * have initialized the cache pages (FP_CREATE) in
                     * which case we would leave uninitialized storage 
                     * on disk. 
                     * We will build a linked list of all the storage
                     * we've added so that the write can back it out
                     * if necessary
                     */

                    if (stg_list->app_stg_end_fob != 0) {
                        /* We are using this descriptor, we need another
                         */
                        stg_list->next = 
                            (struct advfs_pvt_stg_desc *)ms_malloc_no_bzero(
                                sizeof(struct advfs_pvt_stg_desc));
                        stg_list = stg_list->next;
                        stg_list->next = NULL;
                    }
                    stg_list->app_stg_start_fob = 
                        ADVFS_OFFSET_TO_FOB_DOWN(current_storage_offset);

                    stg_list->app_stg_end_fob = 
                        ADVFS_OFFSET_TO_FOB_DOWN(ending_storage_offset - 1);
                }
                /* storage addition succeeded; if APP_ADDSTG_NOCACHE
                 * we want to incrementally update app_stg_end_fob
                 * since bytes_to_add may be rounded up to account
                 * for storage allocation unit size.  The first time
                 * through we must also check the value for the first 
                 * storage added and update app_stg_start_fob.
                 *
                 * NOTE: May need to modify this code path to handle 
                 * directIO files on filesets mounted with object 
                 * safety option.  
                 */
                if ( addstg_nocache_specified ) {
                    MS_SMP_ASSERT( snap_maps == NULL );

                    if ( first_check ) { 
                        fs_priv_param->stg_desc.app_stg_start_fob = 
                            ADVFS_OFFSET_TO_FOB_DOWN(current_storage_offset);
                        first_check = FALSE;
                    }
                    fs_priv_param->stg_desc.app_stg_end_fob = 
                        ADVFS_OFFSET_TO_FOB_DOWN(ending_storage_offset - 1);
#ifdef ADVFS_SMP_ASSERT
                    /* Make sure we did not allocate more than the active
                     * range is setup to cover.  The active range is what is
                     * protecting us from racing with migrate.  If we drop
                     * it to get a different active range, we open a window
                     * to corruption.  So we have to get it right before
                     * storage is allocated and then keep that active range
                     * until our write is finished and any allocation beyond
                     * our write is initialized to zeros.
                     */
                    if ( arp && arp->arDebug ) {
                        MS_SMP_ASSERT(fs_priv_param->stg_desc.app_stg_start_fob >=
                                      arp->arStartFob);
                        MS_SMP_ASSERT(fs_priv_param->stg_desc.app_stg_end_fob <=
                                      arp->arEndFob);
                    }
#endif
                    /* ZERO FILL?  WE DON'T NEED NO STINKIN' ZERO FILL! */

                    if (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY) {
                        /* Object Data Safety is enabled, so we still have
                         * an active transaction that has to be handled.
                         * Direct I/O will be responsible for either writing
                         * user data or zero'ing, before doing the ftx_done
                         * on the transaction.  And if an error occurs,
                         * calling ftx_fail() if necessary.
                         */
                        MS_SMP_ASSERT( FTX_EQ(fs_priv_param->app_parent_ftx, 
                                              FtxNilFtxH) );
                        fs_priv_param->app_parent_ftx = ftxH;
                        allow_only_1_dio_obj_safety_allocation++;
                    }
                }

                /*
                 * If the fileset supports object safety, then
                 * advfs_bs_add_stg() started a transaction but did
                 * not commit it.  We need to synchronously zero-fill
                 * the storage to disk and then commit the transaction.
                 */
                else if (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY) {
                    /*
                     * If snapshots exist and we call advfs_bs_zero_fill_pages,
                     * advfs_putpage will try to acquire the bfaSnapLock
                     * which is already held.  We will pass a flag to
                     * indicate to putpage that the lock need not be
                     * acquired.
                     */
                    uint32_t tmp_flag = 0;

                    if (!fs_priv_param) {
                        /*
                         * Oh, snap!  We need a temporary structure.
                         */
                        fs_priv_param = (struct advfs_pvt_param *)
                          ms_malloc(sizeof(struct advfs_pvt_param));
                        tmp_flag++;
                    }

                    fs_priv_param->app_flags |= APP_SNAP_LOCK_HELD;

                    sts=advfs_bs_zero_fill_pages(vp,
                                                 current_storage_offset,
                                                 bytes_to_add,
                                                 FALSE,
                                                 &plist, /* plist is NULL */
                                                 fc_vminfo,
                                                 ftype,
                                                 fs_priv_param,
                                                 TRUE);

                    fs_priv_param->app_flags &= ~APP_SNAP_LOCK_HELD;

                    if (tmp_flag) {
                        ms_free(fs_priv_param);
                        fs_priv_param = NULL;
                    }
                                
                    if (process_snaps && (snap_maps != NULL) ) {
                        /* We don't want this to be undone if we are doing
                         * snapshots.
                         */
                        ftx_special_done_mode( ftxH, FTXDONE_SKIP_SUBFTX_UNDO );

                    }
                    ftx_done_n( ftxH, FTA_FS_WRITE_ADD_STG_V1 );

                } else if (((pflags & FP_CREATE) && 
                            (((off_t)(current_storage_offset + bytes_to_add) >
                              (write_req_off_end + 1)) ||
                             (current_storage_offset < write_req_off_start))) ||

                           (!(pflags & FP_CREATE) && 
                            ((current_storage_offset + bytes_to_add) >
                             (alloc_offset + alloc_size)))) {
                    /* 
                     * In the case of FP_CREATE, we need to zero-fill 
                     * any storage that we did not have a cache page 
                     * from above and is not going to be overwritten.
                     * This storage may be before or after the end of 
                     * the write() request.
                     *
                     * We also need to zero-fill storage if this
                     * is an mmapper and the fault (with allocation
                     * alignment) straddled two large pages and we
                     * only find the 1st large page in the cache.
                     */

                    /* In the case where FP_CREATE is set we could
                     * have allocated storage on-disk for pages beyond
                     * the current page_alloc loop. This means that
                     * there is uninitialized storage out on-disk that
                     * may have a page in cache (from a prior read)
                     * that is read protected and clean or there is
                     * are cache pages. In either case these pages can
                     * not be seen by a thread trying to read/write
                     * the file since we are holding the file lock for
                     * write. An mmaper could touch one of these pages
                     * but in either case will page fault and wait for
                     * the lock. Similarly the write or read system
                     * call will wait for the lock. This only leaves
                     * vhand or the syncer daemon. Syncer only deals
                     * with dirty pages so it's fine and vhand may pick
                     * a clean protected page to reclaim but we won't
                     * attempt do do i/o since it is clean so it
                     * should be fine as well. Eventually this thread
                     * is going to overwrite these pages since we know
                     * the total request size (passed in the private
                     * params).
                     */

                    if (pflags & FP_CREATE) {

                        off_t next_byte_to_zero_because_of_storage = 0;

                        /*
                         * First we'll check for pages before the write()
                         * that need to be zeroed.
                         */
                        if (current_storage_offset < write_req_off_start) {
                            /*
                             * In this case, we only have to zero pages if
                             * there is gap in between what we brought into
                             * the cache when we called fcache_page_alloc()
                             * and the beginning of the write().  An example
                             * of this would be a program that seeks out to
                             * 6k and writes one byte.  Up above, 
                             * fcache_page_alloc() would be called at offset
                             * 0 (the allocation unit boundary) and might
                             * return a single 4k page.  In this case, there
                             * is a gap between what fcache_page_alloc()
                             * returned and the beginning of the request at
                             * 6k.  Above we will have zeroed the 4k page 
                             * brought in by fcache_page_alloc() but now we
                             * need to zero the second 4k page, the one that
                             * the actual write() is on.
                             */
                            if ((off_t)(alloc_offset + alloc_size) < 
                                write_req_off_start) {
                                zero_offset = alloc_offset + alloc_size;
                                size_to_zero = roundup(write_req_off_start,
                                                       VM_PAGE_SZ) - 
                                               zero_offset;
                                next_byte_to_zero_because_of_storage = 
                                    zero_offset + size_to_zero;
                                sts=advfs_bs_zero_fill_pages(vp,
                                                             zero_offset,
                                                             size_to_zero,
                                                             FALSE,
                                                             &plist,  /* NULL */
                                                             fc_vminfo,
                                                             ftype,
                                                             NULL,
                                                             FALSE);
                                GPLOGIT(ZERO_FILL,vp,zero_offset,size_to_zero,
                                        pflags,ftype,0,0,FP_CREATE_SET);

                            }
                        }

                        /*
                         * Now we'll check for pages after the write() 
                         * that need to be zeroed.
                         */
                        if ((off_t)(current_storage_offset + bytes_to_add) >
                            (write_req_off_end + 1)) {

                            /*
                             * Storage was added that goes beyond the end of
                             * the write() request.  Any further zeroing to 
                             * be done will start at the greater of the 
                             * pages already allocated and the end of the 
                             * write() request.
                             */
                            zero_offset = MAX(alloc_offset + alloc_size,
                                              rounddown(write_req_off_end + 1,
                                                        VM_PAGE_SZ));

                            /*
                             * Now, make sure we don't re-zero any pages
                             * that we just zeroed.
                             */
                            zero_offset = MAX(zero_offset,
                                          next_byte_to_zero_because_of_storage);

                            /*
                             * Now, see if there is really any zeroing left
                             * to do.
                             */
                            if ( zero_offset < (off_t)(current_storage_offset +
                                                       bytes_to_add) ) {

                                size_to_zero = (current_storage_offset + 
                                                bytes_to_add) - zero_offset;

                                sts=advfs_bs_zero_fill_pages(vp,
                                                             zero_offset,
                                                             size_to_zero, 
                                                             FALSE,
                                                             &plist,  /* NULL */
                                                             fc_vminfo,
                                                             ftype,
                                                             NULL,
                                                             FALSE);
                                GPLOGIT(ZERO_FILL,vp,zero_offset,size_to_zero,
                                        pflags,ftype,0,0,FP_CREATE_SET);
                            }
                        }
                    }
                    else {
                        /* 
                         * FP_CREATE is not set.  We have to zero all 
                         * pages that were not in the cache. 
                         */
                        size_to_zero = (bytes_to_add+current_storage_offset) -
                                       (alloc_offset + alloc_size);

                        sts=advfs_bs_zero_fill_pages(vp,
                                                     (alloc_offset + 
                                                      alloc_size),
                                                     size_to_zero, 
                                                     FALSE,
                                                     &plist, /* plist is NULL */
                                                     fc_vminfo,
                                                     ftype,
                                                     NULL,
                                                     FALSE);
                        GPLOGIT(ZERO_FILL,vp,alloc_offset+alloc_size,
                                size_to_zero,
                                pflags,ftype,0,0,FP_CREATE_NOT_SET);
                    }
                }

#ifdef WRITEABLE_SNAPSHOTS
                if (process_snaps && 
                    HAS_SNAPSHOT_PARENT( bfap ) && 
                    (smap_entry->ebd_vd_blk == XTNT_TERM) ) {
                    /* This is a case of a child snapshot being written to
                     * in a region that has not yet been COWed from its
                     * parent. We need to figure out where the storage that
                     * was just added is at so that we can issue the writes
                     * to that storage that will complete the COW.
                     */
                    extent_blk_desc_t*  new_storage_xtnts;
                    sts = advfs_get_blkmap_in_range( bfap,
                                        bfap->xtnts.xtntMap,
                                        &smap_entry->ebd_offset,
                                        smap_entry->ebd_byte_cnt,
                                        &new_storage_xtnts,
                                        NULL,
                                        RND_NONE,
                                        EXB_ONLY_STG,
                                        0);

                    if (sts != EOK) { 
                        /* The correct error in this case will probably be
                         * to mark the bfap as out of sync to and return an
                         * IO error of some sort.  We can't invalidate the
                         * pages at this juncture since they are probably
                         * locked.
                         */
                        MS_SMP_ASSERT( FALSE );
                    }

                    MS_SMP_ASSERT( new_storage_xtnts != NULL);

                    if (bfap_smap_storage_head == NULL) {
                        bfap_smap_storage_head = new_storage_xtnts;
                    } else {
                        bfap_smap_storage_tail->ebd_next_desc =
                            new_storage_xtnts;
                    }

                    if (bfap_smap_storage_tail == NULL) {
                        bfap_smap_storage_tail = bfap_smap_storage_head;
                    }

                    /* Now set bfap_smap_storage_tail correctly */
                    while (bfap_smap_storage_tail->ebd_next_desc != NULL) {
                        bfap_smap_storage_tail = 
                            bfap_smap_storage_tail->ebd_next_desc;
                    }
                }
#endif
                last_entry = smap_entry;
                smap_entry = smap_entry->ebd_next_desc;

            } /* End of while (smap_entry != NULL) loop */

            /* 
             * If we just added storage, we now need to unprotect all
             * the pages that we allocated storage for that were
             * zeroed and are still in the cache. We also will need to
             * bring in any pages that left the cache and zero them
             * since vhand stole them clean and we need insure that
             * they do not get read in from disk as
             * uninitialized. Another step we need to do is to hunt
             * down any pages that we added storage for that are in
             * the cache from an earlier read and remove the
             * protections (FP_CREATE only).
             * 
             * However, if the fileset supports object safety, then they have
             * already been unprotected by the call to
             * advfs_bs_zero_fill_pages() above.  Skip this block.
             *
             * If called with APP_ADDSTG_NOCACHE, there are no cached
             * pages to unprotect.
             */
            if ( need_storage && 
                 !addstg_nocache_specified &&
                 !(bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY)
               ) {
                size_t size_to_unprotect;
                off_t unprotect_offset;
                off_t start_offset_to_unprotect;
                off_t end_offset_to_unprotect;

                MS_SMP_ASSERT(!(pflags&FP_NOWAIT));
                MS_SMP_ASSERT(ftype&FCF_DFLT_WRITE);
                
                /* Hunt down any pages in the cache from a prior read
                 * that we did not bring into the cache ourselves but
                 * did add storage for and unprotect them now.
                 */
                if ((pflags & FP_CREATE) &&
                    (ending_storage_offset > (off_t)(alloc_offset +
                                                     alloc_size)) &&
                    ((write_req_off_end + 1) > (off_t)(alloc_offset +
                                                       alloc_size))) {
                    /* We only need to look in the range that was covered
                     * by FP_CREATE.
                     */

                    /* TODO: We can limit this by the file_size. There
                     * should be no pages in the cache from a read
                     * beyond eof.
                     */

                    start_offset_to_unprotect =
                        MAX(roundup(write_req_off_start,VM_PAGE_SZ),
                                              alloc_offset + alloc_size);
                    end_offset_to_unprotect = MIN( (ending_storage_offset),
                           (off_t)rounddown(write_req_off_end+1,VM_PAGE_SZ) );

                    if ((start_offset_to_unprotect < end_offset_to_unprotect) &&
                        ((size_t)(end_offset_to_unprotect -
                                  start_offset_to_unprotect) >= VM_PAGE_SZ)){
                        /* There must be atleast a VM page between the
                         * start and end for us to unprotect.
                         */                        
                        size_to_unprotect = end_offset_to_unprotect -
                                            start_offset_to_unprotect;
                        
                        (void) advfs_unprotect_range(fc_vminfo,
                                              vp, 
                                              start_offset_to_unprotect,
                                              size_to_unprotect,
                                              (extent_blk_desc_t *)NULL,
                                              FPA_PGCACHE);
                        GPLOGIT(UNPROTECT,vp,start_offset_to_unprotect,      
                               size_to_unprotect, pflags,ftype,0,0,
                               FP_CREATE_SET);
                    }
                }
    
                /*
                 * If this is a small file that already had storage and the 
                 * sparseness started in the first VM_PAGE_SZ bytes of the 
                 * file, we do not want to unprotect the first
                 * page of the file since it was not protected above.
                 */
                if ((small_file_already_had_storage) &&
                    (sparseness_offset < (off_t)VM_PAGE_SZ)) {
                    /* 
                     * No unprotecting to do for this page.
                     */
                }
                /*
                 * If this is not an FP_CREATE write or the page was found
                 * in cache, and this file does not have a small allocation
                 * unit or it does but the sparseness did not start in the
                 * middle of the first VM_PAGE_SZ bytes of the file,
                 * unprotect the pages for which storage was added.
                 * We can't unprotect these pages if we have any COWing
                 * to do.  The page locks may be held on any of the pages
                 * that were read only and in cache.  The page lock must be
                 * held to allow the writes to complete. 
                 */
                else if (((!(pflags & FP_CREATE)) || (is_a_small_file) || 
                          (alloc_status == FPA_ST_EXISTS)) && 
                    ((snap_maps == NULL) && (bfap_smap_storage_head == NULL))) {
                    /* 
                     * We need to unprotect all pages in the sparseness map.
                     */
                    advfs_unprotect_range(fc_vminfo,
                                          vp, 
                                          sparseness_offset,
                                          sparseness_size,
                                          sparseness_blkmap,
                                          FPA_PGCACHE | FPA_NEW);
                    GPLOGIT(UNPROTECT,vp,sparseness_offset,sparseness_size,
                            pflags,ftype,0,0,FP_CREATE_NOT_SET);
                }
                else if ((snap_maps == NULL) && (bfap_smap_storage_head == NULL)) {
                    /* 
                     * If the pages were not in the cache and FP_CREATE
                     * was passed in, we only zeroed/protected holes that
                     * were outside of the total write request.
                     * So these are the only pages we should unprotect.
                     */
                    off_t next_byte_to_unprotect = 0;
                    
                    /*
                     * If there was a hole before the beginning of the 
                     * write() request, then before allocating storage, we
                     * zeroed and protected any pages that had been allocated 
                     * in that hole.  We'll now unprotect those.  We restrict 
                     * the unprotecting size to the size of the pages allocated 
                     * or the gap between the start of page allocation and the
                     * start of the write(), whichever is smaller.
                     */
                    if (sparseness_offset < write_req_off_start) {
                        size_to_unprotect = MIN(sparseness_size,
                                    (roundup(write_req_off_start, VM_PAGE_SZ) -
                                                 sparseness_offset));
                        unprotect_offset = sparseness_offset;
                        next_byte_to_unprotect = unprotect_offset + 
                                                 size_to_unprotect;
                        advfs_unprotect_range(fc_vminfo,
                                              vp, 
                                              unprotect_offset,
                                              size_to_unprotect,
                                              sparseness_blkmap,
                                              FPA_PGCACHE | FPA_NEW);
                        GPLOGIT(UNPROTECT,vp,unprotect_offset,size_to_unprotect,
                                pflags,ftype,0,0,FP_CREATE_SET);
                    }

                    /*
                     * If there was a hole that extended beyond the end
                     * of the write() request and we zeroed and protected
                     * any pages in that range before allocating storage, 
                     * we need to unprotect those pages now. 
                     */ 
                    if ((off_t)(sparseness_offset + sparseness_size) > 
                        write_req_off_end + 1) {
                        
                        unprotect_offset = MAX(next_byte_to_unprotect,
                           (off_t)rounddown(write_req_off_end + 1, VM_PAGE_SZ));
                        
                        if (unprotect_offset < 
                            (off_t)(sparseness_offset + sparseness_size)) {
                            size_to_unprotect = sparseness_offset +
                                sparseness_size - 
                                unprotect_offset;
                            /*
                             * The following assertion is valid because
                             * it is our understanding that VM will
                             * never hand us a large page if FP_CREATE
                             * is set.  
                             */
                            MS_SMP_ASSERT(size_to_unprotect == VM_PAGE_SZ);
                            
                            advfs_unprotect_range(fc_vminfo,
                                                  vp, 
                                                  unprotect_offset,
                                                  size_to_unprotect,
                                                  sparseness_blkmap,
                                                  FPA_PGCACHE | FPA_NEW);
                            GPLOGIT(UNPROTECT,vp,unprotect_offset,
                                    size_to_unprotect,
                                    pflags,ftype,0,0,FP_CREATE_SET);
                        }
                    }
                }
            }  /* End if need_storage */

            /* We are done with the sparseness map */
            (void)advfs_free_blkmaps(&sparseness_blkmap);

            if (mig_stg_locked && !process_snaps) {
                /*
                 * Any storage added this iteration is now intialized and
                 * protected correctly in cache.  We can drop the migStgLk.
                 * If this is a snapshot, however, hold on to the lock.  It
                 * will be dropped later.
                 */
                ADVRWL_MIGSTG_UNLOCK( bfap );
                mig_stg_locked = FALSE;
            }

        }

done_with_add_stg:


#ifdef WRITEABLE_SNAPSHOTS
        /*
         * If we allocated storage for a snapshot child that was being
         * written, chain that storage into the snap_maps so that it gets
         * written with the other snapshot children.
         */
        if (bfap_smap_storage_head != NULL) {
            bfap_smap_storage_head->ebd_snap_fwd = snap_maps;
            snap_maps = bfap_smap_storage_head;
        }
#endif

        /* We are finally at the bottom of the page_alloc loop. It is now
         * time to update the loop counters and loop back up.
         */

        if (return_status != EOK) {
            /* We hit an error condition that required that we
             * continue the code flow but now it is time to return
             * with the error code. Some of the request may have been
             * completed.  Error paths that get here are responsible
             * for setting return_size appropriately.
             */
            goto popsicle_stand;
        }

#ifdef ADVFS_SMP_ASSERT
        if ( ftype == FCF_DFLT_WRITE && 
                !addstg_nocache_specified && snap_maps == NULL) {
            (void)advfs_verify_unprot_pages(vp,alloc_offset,alloc_size,fc_vminfo);
        }
#endif

        current_loop_offset += alloc_size;
        remaining_loop_size -= alloc_size;
        remaining_request_size -= alloc_size;
        
        MS_SMP_ASSERT(remaining_loop_size <= remaining_request_size);

    }  /* End of while (remaining_loop_size > 0) loop */


popsicle_stand:
    
    /* This is the end of getpage */
    
    if (release_pages) {
        err = fcache_page_release(fc_vminfo, 
                                  alloc_size,
                                  &plist,
                                  rel_flags);
            
        MS_SMP_ASSERT(err == 0);
        MS_SMP_ASSERT(plist == NULL);
    }

    if (xtntmap_locked) {
        if (process_snaps) {
            advfs_drop_xtntMap_locks( bfap, (ftype&FCF_DFLT_READ)?
                    SF_SNAP_READ : SF_SNAP_NOFLAGS  );
        } else {
            ADVRWL_XTNTMAP_UNLOCK( &bfap->xtntMap_lk );
        }
    }


    if (ioAnchor_head != NULL) {
        /* wait on the buf stored in each IO anchor */
        while (ioAnchor_head != NULL) {
            ioanchor_t * ioAnchor = ioAnchor_head;
            off_t file_offset = ioAnchor_head->anchr_origbuf->b_foffset;

            if (snap_maps == NULL) {
                /* For non-snapshot/ non-COW scenarios, this will work just
                 * fine */
                /* This will release the memory for the buf struct */
                err = fcache_buf_iowait(fc_vminfo,
                                    ioAnchor_head->anchr_origbuf,
                                    FBI_DEFAULT);
            
                if (err != 0) {
                /* We must continue to wait for buffers even on an
                 * error so squirrel this away. We also need to 
                 * return the range that we succesfully faulted in
                 * so keep track of the minimum file offset that
                 * took an error. NOTE: We could already have an
                 * error such as ENOSPACE or EFAULT but we will
                 * override that with this error.
                 */

                    return_size = MIN((size_t)(file_offset - return_offset),
                                      return_size);
                    return_status = err;
                }
                
                ioAnchor_head = ioAnchor_head->anchr_listfwd;
                advfs_bs_free_ioanchor(ioAnchor, M_WAITOK);
            } else {
                off_t error_offset = 0;
                sts = advfs_wait_for_snap_io( &ioAnchor_head,
                                        bfap,
                                        &error_offset,
                                        &snap_maps,
                                        cow_ftx );

                /* 
                 * On error, set the return_size to be the amount of data
                 * successfully processed 
                 */
                if (sts != EOK) {
                    return_size = (return_offset + return_size) - 
                        error_offset;
                    return_status = sts;
                }
                MS_SMP_ASSERT( ioAnchor_head == NULL );
                if (sts != EOK) {
                    return_status = sts;
                }
            }
        }
    }


#ifdef ADVFS_SMP_ASSERT
    /*
     * This is a fairly heavy-handed check to make sure we are getting
     * pi_pg_ro protections correct for snapshots.
     */
    if (check_page_protections) {
        if (!cleanup_snaps) {

            /* if clenaup_snaps is false, then the bfap snap lock is not held
             * and this needs to be held to get the extent maps in verify.
             * Also, migrate will already hold the bfaSnapLock so don't
             * acquire it in that case either.
             */ 
            ADVRWL_BFAP_SNAP_READ_LOCK( bfap );
        }
        advfs_verify_snap_page_protections( vp, 
                return_offset, 
                return_size,
                fc_vminfo );
        if (!cleanup_snaps) {
            ADVRWL_BFAP_SNAP_UNLOCK( bfap );
        }
    }
#endif

    /*
     * If we acquired the mig stg lock early, drop it now.
     */
    if (mig_stg_locked) {
        ADVRWL_MIGSTG_UNLOCK( bfap );
    }

        
    /*
     * Generally, only do this on mmap
     */
    if (unlock_filelock) {
        ADVRWL_FILECONTEXT_UNLOCK(contextp);
    }


    /*
     * Cleanup locking and snap_maps if we had to process snapshots 
     */
    if (cleanup_snaps) {
        sts = advfs_snap_cleanup_and_unlock( bfap,
                                &snap_maps,
                                (ftype & FCF_DFLT_WRITE) ?
                                        SF_SNAP_WRITE : SF_SNAP_READ );
        MS_SMP_ASSERT( sts == EOK );

    }

    /*
     * Finish the transaction. All the COW stuff was done with no undo.
     */
    if ( cow_ftx_started ) {

        ftx_done_n( cow_ftx,
                FTA_GETPAGE_COW );
        cow_ftx_started = FALSE;

        /*
         * If we had a COW transaction and called advfs_bs_add_stg, we may
         * have truncated some preallocated storage.  Because of the active
         * cow_ftx, we couldn't call stg_remove_stg_finish which starts a
         * series of root transactions.  Instead, the delCnt and delList was
         * returned.  Now that the cow_ftx is complete, we should be safe to
         * finish the storage removal.
         */
        stg_remove_stg_finish( bfap->dmnP, delCnt, delList );
    }



    /* set flag to update stats if we are writing and returning at least
     * one page, because any successfully pin'd pages will get written.
     */
    
    if ((ftype & FCF_DFLT_WRITE) && 
        (return_size > 0) &&
        (contextp)) {
        ADVSMP_FILESTATS_LOCK(&contextp->fsContext_lock);
        contextp->fs_flag |= MOD_MTIME | MOD_CTIME;
        contextp->dirty_stats = TRUE;
        ADVSMP_FILESTATS_UNLOCK(&contextp->fsContext_lock);
    } else {

        /* This assert will fire the first time an
         * index is written. We need to allow indexes
         * thru.
         */
        MS_SMP_ASSERT (!(ftype & FCF_DFLT_WRITE) ||
                       !(return_size > 0) ||
                       (BS_BFTAG_RSVD(bfap->tag)) || 
                       TRUE );
    }

    if (bumped_writecnt) {
        ADVFS_SNAPSHOT_DONE_SYNC( bfap );
    }
/*
    GPLOGIT(EXIT_CALL,vp,0, 0,
            pflags,ftype,0,DEFAULT_ACTION);
*/


    pflags |= cow_cleared_pflags;

    *size = return_size;
    *off = return_offset;

    MS_SMP_ASSERT( cow_ftx_started == FALSE );
    return(return_status);
}
 
/* 
 * advfs_get metapage
 *
 * This function will be called from advfs_getpage to bring in
 * meta-data pages into the cache. This is being moved to a separate
 * function to ease the complexity of the advfs_getpage code.  These
 * are the MAJOR assumptions being made in this function:
 *
 * 1) All meta-data is 8K. The on-disk allocation unit for meta-data
 * is fixed at 8k (this restriction could be relaxed in the future
 * with a other changes).
 *
 * 2) Storage has previously been allocated. This code does not expect
 * to encounter a sparse hole when attempting to page in
 * meta-data. 
 *
 * 3) We will NEVER find a large page in the cache.
 *
 * 4) This will ONLY be called for a single 8K meta-page.
 *
 * 5) This will only be called for write intent.
 *
 * A meta-data page will consist of 2 PFDATs. In order to bring a
 * meta-data page into the cache both PFDATs must be looked up. This
 * means that one could be in the cache and one could not.
 *
 * This function accept a list of ioAnchors to wait for and return
 * this list to the caller.
 *
 * A pin (write intent) will cause the meta-data PFDATs to be marked
 * dirty. This is necessary for migrate to operate. Migrate can not
 * deal with a clean page that has a non-zero bsb_writeref count in
 * its bsBuf. This causes problems since the page may not be flushed
 * and the waiter not woken up. Also migrate can not atomically flush
 * a clean 4K page followed by its sibling 4K dirty page, since the
 * 1st 4K page could have been redirtied and we can not back up in the
 * page_scan without infinitely looping.
 *
 * Synchronization
 * 
 * The extentmap lock should be held in READ mode before calling this
 * function.
 *
 * The caller (currently ONLY bs_pinpg (calling getpage)) has the
 * responsibility of first looking up the bsbuf in the hashtable and
 * then bumping the writeref and marking BOTH pages as resident.  The
 * bsb_writeref on the bsBuf will protect the PFDATs from being
 * stolen.  All lookups of meta-data will be forced to start on the 8K
 * boundary. Thus racing getpages must wait for the 1st PFDAT to be
 * released before attempting to access the 2nd PFDAT. The PFDATs will
 * also be marked dirty before they are released.
*/

statusT
advfs_getmetapage(
    fcache_vminfo_t *fc_vminfo,      /* An opaque pointer to a vm data struct*/
    struct vnode *vp,   	     /* The access structure pointer */
    off_t off,               	     /* The offset in the file of the fault */
    struct advfs_pvt_param *fs_priv_param,  /* File system private parameter */
    ioanchor_t **ioAnchor_head,      /* list of anchors for wait on */
    fcache_pflags_t pflags, 	     /* Flags passed to VOP_GETPAGE */
    extent_blk_desc_t*  snap_maps,   /* When dealing with snapshots, indicates the
                                      * regions in children snapshots that
                                      * need to be COWed to */
    ftxHT       parent_ftx           /* If a snapshot child needs to be marked out of
                                      * sync, a transaction is required. */
    )
{
    size_t alloc_size;
    size_t loop_size= ADVFS_METADATA_PGSZ;
    off_t alloc_offset = off;
    fpa_status_t alloc_status;
    page_fsdata_t *plist,*pfdat;
    bfAccessT *bfap=VTOA(vp);
    int err;
    statusT sts=EOK;
    loop_size = bfap->bfPageSz*ADVFS_FOB_SZ;
    
    MS_SMP_ASSERT(loop_size == ADVFS_METADATA_PGSZ);
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL(&bfap->xtntMap_lk, RWL_UNLOCKED));  
    MS_SMP_ASSERT(*ioAnchor_head == NULL);
    while (loop_size > 0) {
        alloc_size = loop_size;
        plist = fcache_page_alloc(fc_vminfo,
                                  vp, 
                                  &alloc_offset,
                                  &alloc_size,
                                  0L,
                                  bfap->bfaNextFob * ADVFS_FOB_SZ,
                                  &alloc_status,
                                  FPA_NEW|FPA_PGCACHE,
                                  pflags);
#ifdef ADVFS_SMP_ASSERT
        /* I really do not expect this to ever really happen as metadata is
         * never opened for Direct I/O, but I'd rather be safe than sorry.
         *
         * If the file is opened for Direct I/O, make sure the pages being
         * added to the UFC have an active range associated with them,
         * otherwise stale data could be returned or written to the file,
         * causing data corruption or the appearence of corruption.
         */
        if ( plist && bfap->dioCnt ) {
            ASSERT_ACTRANGE_HELD( bfap, alloc_offset, alloc_size );
        }
#endif /* ADVFS_SMP_ASSERT */
        
        GPLOGIT(PAGE_ALLOC_META,vp,alloc_offset,alloc_size,pflags,FCF_DFLT_WRITE,
                alloc_status,0,DEFAULT_ACTION);

        MS_SMP_ASSERT(alloc_status != FPA_ST_END);
        MS_SMP_ASSERT(alloc_offset == 
                      (off + ((bfap->bfPageSz * ADVFS_FOB_SZ) - loop_size)));
        MS_SMP_ASSERT(alloc_size <= loop_size);
        MS_SMP_ASSERT(plist != NULL);
            
#ifdef ADVFS_SMP_ASSERT
        if (TRUE) {
            off_t storage_offset=alloc_offset;
            size_t storage_size=alloc_size;
            extent_blk_desc_t *storage_blkmap=NULL;
            uint64_t request_needs_storage=0;



            sts = advfs_get_blkmap_in_range(bfap,
                                            bfap->xtnts.xtntMap,
                                            &storage_offset,
                                            storage_size,
                                            &storage_blkmap,
                                            &request_needs_storage,
                                            RND_ALLOC_UNIT,
                                            EXB_ONLY_HOLES, 
                                            XTNT_LOCKS_HELD|XTNT_WAIT_OK|XTNT_NO_MAPS);
            MS_SMP_ASSERT(request_needs_storage == 0);
        }
#endif

        if (alloc_status == FPA_ST_EXISTS) {
            /* We are done with this pfdat(s) */


            if (bfap->bfSetp->bfsFirstChildSnapSet ){
                /* The plist might be read protected if a snapshot child
                 * exists.  So make sure to unprotect before releasing.  
                 * Since this is metadata, we don't need to check for holes.
                 *
                 * We do this unconditionally if a child snapset exist
                 * becuase the child snapset may be in the processes of
                 * being deleted but not yet have un-read protected the
                 * pages.  To complete the snapset removal, the children
                 * need to be closed which may try to start transactions.
                 * Since this is metadata, we may be in a transaction and
                 * need this page to be unprotected.  Therefore, we can't
                 * rely on BFA_NO_SNAP_CHILDREN to optimize this step.
                 */
                pfdat = plist;
                while (pfdat) {
                    pfdat->pfs_info.pi_pg_ro = 0;
                    pfdat = pfdat->pfs_next;
                }
            }

            if (snap_maps == NULL) {
                err = fcache_page_release(fc_vminfo, 
                                      alloc_size,
                                      &plist,
                                      FP_DEFAULT);
            
                MS_SMP_ASSERT(err == 0);
                MS_SMP_ASSERT(plist == NULL);
            } else {
                struct buf*     snap_buf;
                struct buf*     bufp;
                /* Some COWing may be necessary */
                ioanchor_t*     io_anchor;
                io_anchor = advfs_bs_get_ioanchor( M_WAITOK );
                io_anchor->anchr_iocounter = 1;
                io_anchor->anchr_flags = (IOANCHORFLG_KEEP_ANCHOR |
                                          IOANCHORFLG_CHAIN_ERRORS | 
                                          IOANCHORFLG_WAKEUP_ON_LAST_IO);
                snap_buf = (struct buf*)ms_malloc_no_bzero(sizeof(struct buf));
                bufp = fcache_buf_create( fc_vminfo,
                                        &bfap->bfVnode,
                                        alloc_size,
                                        0UL, &plist, pflags );
                if (bufp == NULL) {
                    /* Release the pages before returning. */
                    err = fcache_page_release(fc_vminfo, 
                                      alloc_size,
                                      &plist,
                                      FP_DEFAULT);
            
                    MS_SMP_ASSERT(err == 0);
                    MS_SMP_ASSERT(plist == NULL);

                    /* Is this the correct behavior for snapshots?  What
                     * about IOs already issued? Do we need to mark children
                     * as out of sync? */
                    if (pflags & FP_NOWAIT) {
                        advfs_bs_free_ioanchor( io_anchor, M_WAITOK );
                        /* This is what is returned by advfs_bs_startio.
                         * SHould it be EWOULD_BLOCK? */
                        ms_free(snap_buf);
                        return ENOMEM;
                    }
                    MS_SMP_ASSERT( FALSE );
                }
                /* Setup io_anchor and issue snap IOs as appropriate */
                io_anchor->anchr_origbuf = bufp;

                bcopy( bufp, snap_buf, sizeof( struct buf ) );
                io_anchor->anchr_buf_copy = snap_buf;
                MS_SMP_ASSERT( !(BGETFLAGS(io_anchor->anchr_buf_copy) & B_DONE) );

                sts = advfs_issue_snap_io( io_anchor,
                                           snap_maps,
                                           SF_FAKE_ORIG,
                                           bfap,
                                           parent_ftx );

                /* If advfs_issue_snap_io fails, the children should have
                 * been marked out of sync or the domain paniced, there
                 * should be nothing to do for an error */
                MS_SMP_ASSERT( sts == EOK );

                /* Add the io_anchor to the head of the ioAnchor_head list
                 */
                if (*ioAnchor_head == NULL) {
                    *ioAnchor_head = io_anchor;
                } else {
                    io_anchor->anchr_listfwd = *ioAnchor_head;
                    io_anchor->anchr_listbwd = (*ioAnchor_head)->anchr_listbwd;
                    io_anchor->anchr_listbwd->anchr_listfwd = io_anchor;
                    io_anchor->anchr_listfwd->anchr_listbwd = io_anchor;
                }
            }
        } else {
            /* The FPA_ST_DATAFILL case */
            /* Retrieve the metadata from disk to prime the cache pages
             * unless caller states they are overwriting the metadata page.
             */

            MS_SMP_ASSERT(fs_priv_param);
            
            if (!(fs_priv_param->app_flags & APP_METAPG_OVERWRITE)) {
                off_t storage_offset=alloc_offset;
                size_t storage_size=alloc_size;
                extent_blk_desc_t *storage_blkmap=NULL;
 
                /* We need to read the meta-data page in from disk */
                sts = advfs_get_blkmap_in_range(bfap,
                                                bfap->xtnts.xtntMap,
                                                &storage_offset,
                                                storage_size,
                                                &storage_blkmap,
                                                NULL,
                                                RND_VM_PAGE,
                                                EXB_COMPLETE,
                                                (pflags&FP_NOWAIT)?
                                                XTNT_LOCKS_HELD|XTNT_NO_WAIT:
                                                XTNT_LOCKS_HELD|XTNT_WAIT_OK);

                if (sts != EOK) {
                    /* The caller can't block so we must bail out and
                     * deal with any i/o started so far.
                     */
                    MS_SMP_ASSERT(sts == E_WOULD_BLOCK);
                    MS_SMP_ASSERT(pflags&FP_NOWAIT);
                
                    err = fcache_page_release(fc_vminfo, 
                                              storage_size,
                                              &plist,
                                              FP_INVAL);
                    MS_SMP_ASSERT(err == 0);
                    return(EWOULDBLOCK);
                }

                sts = advfs_start_blkmap_io(fc_vminfo,
                                            vp,
                                            alloc_offset,
                                            alloc_size,
                                            storage_blkmap,
                                            NULL,
                                            ioAnchor_head,
                                            &plist,
                                            NULL,
                                            pflags,
                                            snap_maps ? ADVIOFLG_SNAP_READ : 0);
                if (sts != EOK) {
                    /* This will actually force both pages to be invalidated */
                    err = fcache_page_release(fc_vminfo, 
                                              alloc_size,
                                              &plist,
                                              pflags&FP_INVAL&(~FP_DEFAULT));
                    return(sts);
                }
                /*
                 * We did any IO on this metapage, so pass this back to the
                 * caller.
                 */
                fs_priv_param->app_flags |= APP_IO_PERFORMED;

                MS_SMP_ASSERT(storage_blkmap->ebd_next_desc == NULL);
                (void)advfs_free_blkmaps(&storage_blkmap);
            }
            else {
                /* bs_pinpg() states they are going to overwrite the entire
                 * metadata page and wants to optimize performance by having
                 * getpage skip doing IO to prime the new metadata cache page
                 * from disk. Just setup UFC and call biodone without doing
                 * IO to disk so that UFC clears the FPA_ST_DATAFILL state
                 * in the cache pages.
                 */
                struct buf *bufp;

                bufp = fcache_buf_create(fc_vminfo,
                                         vp,
                                         alloc_size,
                                         0UL,
                                         &plist,
                                         FP_READ|FP_ASYNC);
                MS_SMP_ASSERT(bufp != NULL);
                bufp->b_vp = vp;
                bufp->b_foffset = alloc_offset;
                bufp->b_bcount = alloc_size;           
                biodone(bufp);
            }
        }
        loop_size -= alloc_size;
        alloc_offset += alloc_size;
    }
    return(sts);
}
            
/* Currently this function is tailored to getpage. It assumes that the
 * caller has allocated storage and protected all the pages in the
 * passed in spareseness map. The offset and size reflect that portion
 * of the sparseness map that needs to be unprotected.  As a further
 * example of tailoring this for get page, this routine will bring in
 * both cached and new pages. Cache pages will be unprotected and
 * marked dirty, but new pages will be zeroed and marked dirty.  This
 * is because we know that the page in the range was protected and
 * clean (and zeroes). The only way we wouldn't find it in the cache
 * is if it was stolen by VHAND. Since it was clean the zeroes could not
 * have made it out to disk.
 *
 * A new flavor of this is being added to unprotect pages that were
 * brought in the cache as a result of the read of a hole. Since we
 * are adding storage we need to hunt down these pages and remove
 * the read-only protection.
 *
 */
statusT
advfs_unprotect_range(
    fcache_vminfo_t *fc_vminfo,
    struct vnode * vp, 
    off_t off,
    size_t size,
    extent_blk_desc_t *blkmap,
    fpa_ptype_t ptype
    )
{
    off_t loop_start;
    off_t loop_end;
    extent_blk_desc_t *smap_entry;
    bfAccessT *bfap;
    int err;
    int release_smap_entry = FALSE;
    smap_entry = blkmap;
    bfap = VTOA(vp);
    statusT sts;
    
    ADVFS_ACCESS_LOGIT_2(bfap, "unprotect_range", off, size);
    if (blkmap == NULL) {
        /* Dummy up an smap_entry for the passed in offset and
         * length. This is the situation that the passed in range
         * encompasses a hole so we don't need a sparseness map.
         */
        smap_entry = (extent_blk_desc_t*)ms_malloc(
                sizeof(extent_blk_desc_t));
        smap_entry->ebd_offset = off;
        smap_entry->ebd_byte_cnt = size;
        smap_entry->ebd_vd_blk = XTNT_TERM;
        smap_entry->ebd_vd_index = 0;
        smap_entry->ebd_next_desc = NULL;
        release_smap_entry = TRUE;
    }
    while (smap_entry != NULL) {
        if ((smap_entry->ebd_offset <= (off_t)(off + size)) &&
            ((off_t)(smap_entry->ebd_offset + 
                     smap_entry->ebd_byte_cnt) > off)){
            /* At least a portion of this blkmap needs to
             * have some pages unprotected
             */

            loop_start = MAX(smap_entry->ebd_offset,off);
            loop_start = rounddown(loop_start, VM_PAGE_SZ);
            loop_end = MIN((off + size),(smap_entry->ebd_offset +
                                         smap_entry->ebd_byte_cnt));
            loop_end = roundup(loop_end, VM_PAGE_SZ) - 1;

            while (loop_start < loop_end) {
                fpa_status_t alloc_status;
                page_fsdata_t *pfdat, *plist;
                off_t alloc_offset = loop_start;
            
                size_t alloc_size = loop_end - loop_start + 1;
            
                /* We only need to unprotect those pages still in
                 * the cache. Presumebly they should all still be
                 * there. If a page was stolen by VHAND it was not
                 * pushed to the disk! (clean) so we must bring it
                 * back into the cache now and zero it making it
                 * dirty.
                 */
                /*
                 * NOTE This needs to allow for a large page to be
                 * returned from page_alloc. By forcing max_coff to
                 * alloc_offset + alloc we could break up a large
                 * page. The loop will need to change to release
                 * pages to the left and right of the range to be
                 * uprotected.
                 */
                /* 
                 * NOTE:  If min_coff/max_coff arguments are changed, then
                 *        look at the advfs_getpage() example so that Direct
                 *        I/O is properly taken into consideration.  Because
                 *        Direct I/O can be doing concurrent, I/O bypassing
                 *        the UFC, it needs to know that pages loaded into
                 *        the UFC do _NOT_ cross into Active Ranges used to
                 *        protect Direct I/O operations.  Big pages have the
                 *        potential to leak into an Active Range and can
                 *        cause stale data to be written to disk from the
                 *        UFC after a Direct I/O has written new data
                 *        directly to disk.
                 */

                plist = fcache_page_alloc(fc_vminfo,
                                          vp, 
                                          &alloc_offset,
                                          &alloc_size,
                                          alloc_offset,
                                          alloc_offset + alloc_size,
                                          &alloc_status,
                                          ptype,
                                          FP_DEFAULT);
#ifdef ADVFS_SMP_ASSERT
                /* If the file is opened for Direct I/O, make sure the pages
                 * being added to the UFC have an active range associated
                 * with them, otherwise stale data could be returned or
                 * written to the file, causing data corruption or the
                 * appearence of corruption.
                 */
                if ( plist && bfap->dioCnt ) {
                    ASSERT_ACTRANGE_HELD( bfap, alloc_offset, alloc_size );
                }
#endif /* ADVFS_SMP_ASSERT */

                if ((plist == NULL) ||
                    (alloc_status == FPA_ST_END)) {
                    MS_SMP_ASSERT(ptype != (FPA_PGCACHE|FPA_NEW));
                    break;
                }

                if (alloc_status == FPA_ST_EXISTS) {
                    pfdat=plist;
                    while (pfdat != NULL) {

                        /* We could see a large page here. Lets say
                         * the request landed on 0xd400 for size
                         * 0x2000. This would cause us to bring in
                         * 0xc000 for 0x4000 (allocation rounding). We
                         * could be given a large page and not realize
                         * it in getpage. We would need to unprotect
                         * 0xc000 for 0x2000. The call to page_alloc
                         * would hand us 0x4000. We will unprotect
                         * only the range of interest.
                         */
                        if (pfdat->pfs_off > loop_end) break;
                        if (pfdat->pfs_off >= loop_start){
                            /* Clear the protection */
                            MS_SMP_ASSERT(pfdat->pfs_info.pi_pg_ro);
                            pfdat->pfs_info.pi_pg_ro = 0;
                            /* We want these zeroed pages to get to disk
                             *  eventually 
                             */
                            pfdat->pfs_info.pi_pg_setdirty = 1;
                        }
                        pfdat=pfdat->pfs_next;
                    }

                    /* Release these pages */

                    err = fcache_page_release(fc_vminfo, 
                                              alloc_size,
                                              &plist,
                                              FP_DEFAULT);

                    MS_SMP_ASSERT(err == 0);
                } else {
                    /* A large page should never be created here. */
                    MS_SMP_ASSERT(alloc_status == FPA_ST_DATAFILL);
                    MS_SMP_ASSERT(alloc_offset == loop_start);
                    MS_SMP_ASSERT(alloc_size<=(size_t)(loop_end-loop_start+1));

                    sts = advfs_bs_zero_fill_pages(vp,
                                                 alloc_offset,
                                                 alloc_size,
                                                 FALSE, /* Don't write protect */
                                                 &plist,
                                                 fc_vminfo,
                                                 FCF_DFLT_WRITE,
                                                 NULL,
                                                 FALSE);
                    MS_SMP_ASSERT(sts == EOK);
                }

                /* A large page may be brought in more than once since
                 * it will not necessarily map on the the sparseness
                 * map. Hopefully this will not happen often.
                 */
                loop_start = alloc_offset + alloc_size;
            }
        }
        else if (smap_entry->ebd_offset > (off_t)(off + size))
        {
            /* Early bail out */
            break;
        }
        smap_entry = smap_entry->ebd_next_desc;
    }
    if (release_smap_entry) {
        ms_free(smap_entry);
    }
    return (EOK);
}
                
/* Advfs_start_blkmap_io()
 *
 * This function will initiate i/o for the passed in BLKMAP range. It
 * will take care of creating structures and processing extent maps
 * need to issue an i/o. Multiple destinations (i.e. migrate) will
 * also be handle by passing in an optional secondary (COPY)
 * blkmap. Dirty meta-data i/o will require passing in a pointer to a
 * list of bsBufs.
 *
 * The passed in flags will indicate if this is read/write,
 * sync/async, and other characteristics.
 *
 * The caller should only pass in bsBufs for DIRTY meta-data !!!
 *
 * The offset and length do NOT have to match the primary blockmap,
 * however they must be encompassed by it.
 *
 * The i/o completion routine will release the IO Anchor in the case
 * of asynchronous i/o and will always release the copy buf structs.
 *
 * This routine can handle a secondary map that does NOT have any
 * overlap. This will issue the i/o for the primary and not the
 * secondary.
 *
 * The offset and size arguments are needed for meta-data
 * processing. Since a blkmap range of meta-data needs to be processed
 * a page at a time (some pages might not be eligible for i/o within a
 * given blkmap range). The offset and size allow for a subset of a
 * blkmap range to have i/o done. This routine will modify the
 * secondary blkmap if only a portion of the range is processed.
 *
 * Synchronous callers of this routine expect to have a CIRCULAR list
 * of IO anchors returned for each i/o started on the primary blkmap
 * passed in. Secondary i/o's are implicitly waited for by waiting on
 * the primary i/o.
 *
 */
static int errdbg_insert_ioanchor_error;
static int errdbg_insert_buf_error;
static int errdbg_insert_metadata_ioanchor_error;
static int errdbg_insert_metadata_buf_error;

statusT 
advfs_start_blkmap_io(
    fcache_vminfo_t *fc_vminfo,         /* opaque vm pointer for buf creation */
    struct vnode * vp,                  /* The vnode pointer of the file */
    off_t     offset,                   /* starting offset */
    size_t     length,                  /* length to write */
    extent_blk_desc_t * primary_blkmap, /* The blkmap for the main i/o*/
    extent_blk_desc_t ** passed_secondary_blkmap, /* if multiple destinations */
    ioanchor_t **ioAnchor_head,/* List to add any i/o's started to (SYNC only)*/
    page_fsdata_t  ** plist,            /* list of PFDATS to write */
    struct bsBuf ** bsBufList_ptr,      /* list of bsBufs for meta-data i/o */
    fcache_pflags_t pflags,             /* flags need for buf creation */
    int32_t         io_flags            /* ADVIOFLG_* flags */
    )
{
    off_t current_io_offset;            /* File offset of current I/O */
    ulong_t remaining_io_size;          /* How much I/O remains to be done */
    size_t chunk_size;                  /* If the length requested by the
                                         * caller is too long for the 
                                         * underlying volume(s), the request
                                         * will be broken into chunks of
                                         * this size.
                                         */
    bfAccessT *bfap=VTOA(vp);
    ioanchor_t *ioAnchor;
    struct bsBuf *bsBufList, *bsBuf_iolist;
    struct buf *bufp;
    uint64_t ioflags=0;
    extent_blk_desc_t *secondary_blkmap;
    vdT *vdp=NULL;
    int error=EOK;

    if (bsBufList_ptr == NULL) {
        bsBufList = NULL;
    } else {
        bsBufList = *bsBufList_ptr;
        *bsBufList_ptr = NULL;
    }
    if (passed_secondary_blkmap == NULL) {
        secondary_blkmap = NULL;
    } else {
        secondary_blkmap = *passed_secondary_blkmap;
    }

    MS_SMP_ASSERT((pflags&FP_WRITE) || (secondary_blkmap == NULL));
    current_io_offset = offset;
    remaining_io_size = chunk_size = length;

    /*
     * If this is not a fake I/O, see if we have to break the request apart
     * into chunks.  We need to limit the amount of I/O we do by the 
     * current IO transfer size of the device to which we are writing. 
     * If we are writing to multiple volume, we will use the smallest 
     * transfer size.
     */
    if (primary_blkmap != NULL) {
        extent_blk_desc_t * s_blkmap=secondary_blkmap;

        vdp = VD_HTOP(primary_blkmap->ebd_vd_index,bfap->dmnP);
        chunk_size = MIN(chunk_size,
                      (pflags&FP_WRITE ? vdp->current_iosize_wr :
                       vdp->current_iosize_rd));
        while (s_blkmap != NULL) {
            if (s_blkmap->ebd_vd_index != primary_blkmap->ebd_vd_index) {
                vdT *svdp;
                svdp = VD_HTOP(s_blkmap->ebd_vd_index,bfap->dmnP);
                chunk_size = MIN(chunk_size,
                            (pflags&FP_WRITE ? svdp->current_iosize_wr :
                             svdp->current_iosize_rd));
            }
            s_blkmap = s_blkmap->ebd_next_desc;
        }
        if (bsBufList) {
            /*  We must always push out full meta-data pages (when
             *  dirty) . The passed in size should always be 8K
             *  aligned. However the transfer size might not be? So
             *  make sure if we are throttling this i/o we do not do
             *  it in the middle of a meta-data page.
             */
            chunk_size = rounddown(chunk_size,ADVFS_METADATA_PGSZ);
        }
    }

    /*
     * Loop through the chunks, issuing I/O's.
     */
    while (remaining_io_size > 0) {

        bsBuf_iolist = bsBufList;
        ioAnchor = advfs_bs_get_ioanchor((pflags&FP_NOWAIT?M_NOWAIT:M_WAITOK));
        if (ioAnchor == NULL) {
            PPAGE_STATS(failed_to_get_ioanchor);
            if (bsBufList_ptr){
                *bsBufList_ptr = bsBuf_iolist;
            }
            return (ENOMEM);
        }
        
        ioAnchor->anchr_iocounter++;
        if (io_flags & ADVIOFLG_SNAP_READ) {
            /* This read is for a COW.  Set the iocounter to 2 and set the
             * IOANCHORFLG_WAKEUP_ON_COW_READ and IOANCHORFLG_CHAIN_ERROR so
             * that snapshot processing can be completed correctly */
            ioAnchor->anchr_iocounter = 2;
            ioAnchor->anchr_flags |= 
                IOANCHORFLG_WAKEUP_ON_COW_READ|IOANCHORFLG_CHAIN_ERRORS;
            ioAnchor->anchr_buf_copy = (struct buf*)ms_malloc_no_bzero( 
                    sizeof (struct buf) );
        }
        if (pflags & FP_SYNC) {
            /* The caller chains the anchors together to as a way to
             * get at the buf structs stored in each one for calling
             * bufio_wait.
             */
            ioAnchor->anchr_flags |= IOANCHORFLG_KEEP_ANCHOR;
        }
        bufp = fcache_buf_create(fc_vminfo,
                                 vp, 
                                 MAX(chunk_size, VM_PAGE_SZ), 
                                 0UL, 
                                 plist, 
                                 pflags);
        if (bufp == NULL) {
            /* We couldn't get memory for this so we need to release our
             * resources and return the error to the caller.
             */
            MS_SMP_ASSERT(pflags&FP_NOWAIT);
            /* NOTE if we have done i/o then those pages have been
             * released and the bsBufs taken care of. The caller will
             * release these remaining pages and put the bsBufs back
             * in the hashtable.
             */
            if (ioAnchor->anchr_buf_copy) {
                ms_free( ioAnchor->anchr_buf_copy );
                ioAnchor->anchr_buf_copy = NULL;
            }
            advfs_bs_free_ioanchor(ioAnchor, M_WAITOK);
            if (bsBufList_ptr) {
                *bsBufList_ptr = bsBuf_iolist;
            }
            PPAGE_STATS(failed_to_get_buf_struct);
            return(ENOMEM);
        }

        if (io_flags & ADVIOFLG_SNAP_READ) {
            bcopy( bufp, ioAnchor->anchr_buf_copy, sizeof (struct buf ) );
            MS_SMP_ASSERT( !(BGETFLAGS(ioAnchor->anchr_buf_copy) & B_DONE) );
        }


        MS_SMP_ASSERT(bufp->b_nexthdr == NULL);

        /*
         * If this is a small allocation unit file and we're reading
         * from offset zero and we're asking for less than a full
         * VM_PAGE_SZ bytes, it must be the case that the first 
         * VM_PAGE_SZ bytes of the file are not fully backed by storage.
         * Zero-fill the part of the page that is not backed by storage.
         */
        if (ADVFS_FILE_HAS_SMALL_AU(bfap) && 
            (offset == 0) && (length < VM_PAGE_SZ) && (pflags & FP_READ)) {
            bzero(bufp->b_un.b_addr + length, VM_PAGE_SZ - length);
        }

        if ((bsBuf_iolist) && (chunk_size < remaining_io_size)) {
            /* We have been throttled by the vd max size. We need to
             * clip the passed in bsBufs.
             */
            while (bsBuf_iolist) {
                MS_SMP_ASSERT(bsBuf_iolist->bsb_iolistfwd);
                if((bsBuf_iolist->bsb_iolistfwd->bsb_foffset +
                    ADVFS_METADATA_PGSZ - current_io_offset ) > chunk_size) {
                    struct bsBuf *tmplist;

                    /* bsBufList points to next batch of bsBufs to
                     * process for the next pass thru the loop.
                     * bsBuf_iolist is current batch to process
                     * null terminated.
                     */

                    tmplist=bsBufList;
                    bsBufList=bsBuf_iolist->bsb_iolistfwd;
                    bsBuf_iolist->bsb_iolistfwd = NULL;
                    bsBuf_iolist=tmplist;
                    
                    break;
                }
                bsBuf_iolist = bsBuf_iolist->bsb_iolistfwd;
            }
            MS_SMP_ASSERT(bsBufList);
        }

        bufp->b_vp = vp;
        /* We allow for i/o in the middle of the past in
         * blkmap. Calculate the proper vd blk based on the passed in
         * offset 
         */
        bufp->b_foffset = current_io_offset;
        if (primary_blkmap != NULL) {
            /* This is not a fakeIO */
            MS_SMP_ASSERT(primary_blkmap->ebd_vd_blk != XTNT_TERM);
            bufp->b_blkno = ((primary_blkmap->ebd_vd_blk) + 
                             (current_io_offset - primary_blkmap->ebd_offset)
                             / DEV_BSIZE);
            /*
             * Generally, the b_bcount field will be the size of the chunk.
             * But if each chunk will actually be broken into multiple I/O's
             * because of small allocation units less than a page size,
             * we need to adjust for that.  Assign this first buf the
             * count in the first blockmap entry.
             */
            bufp->b_bcount = MIN(chunk_size, primary_blkmap->ebd_byte_cnt);
        } else {
            /* We are doing a fake i/o */
            ioflags |= ADVIOFLG_FAKEIO;
            bufp->b_bcount = chunk_size;
        }
        ioAnchor->anchr_origbuf = bufp;
        if (pflags&FP_SYNC) {
            /* Enqueue the anchor to the head of the passed in list. */
            if (*ioAnchor_head != NULL) {
                ioAnchor->anchr_listfwd = *ioAnchor_head;
            }
            *ioAnchor_head = ioAnchor;
        }

        /*
         * We have set up the first primary buf structure.  If there is
         * more than one block map entry in the primary block map for
         * a single VM page, launch I/O's for those now.
         */
        if (primary_blkmap && (length > primary_blkmap->ebd_byte_cnt)) {
            extent_blk_desc_t *current_blkmap,
                              *blkmap_to_free;
            struct buf *copy_buf;

            MS_SMP_ASSERT(length <= VM_PAGE_SZ);

            for (current_blkmap = primary_blkmap->ebd_next_desc;
                 current_blkmap;
                 current_blkmap = current_blkmap->ebd_next_desc) {

                /*
                 * Add another I/O to wait for to the I/O anchor.
                 */
                spin_lock(&ioAnchor->anchr_lock);
                ioAnchor->anchr_iocounter++;
                spin_unlock(&ioAnchor->anchr_lock);

                /*
                 * Allocate a "fake" buf.  The I/O completion will by
                 * default free it.
                 *
                 * TODO: Correctly handle FP_NOWAIT.
                 */
                copy_buf = advfs_bs_get_buf(TRUE, FALSE);
                
                /*
                 * Copy the original buf into this "fake" buf.
                 */
                bcopy(bufp,copy_buf,sizeof(struct buf));

                /*
                 * Update fields in the "fake" buf.
                 */
                copy_buf->b_foffset = current_blkmap->ebd_offset;
                copy_buf->b_un.b_addr += 
                                      (daddr_t) (current_blkmap->ebd_offset - 
                                                 current_io_offset);
                MS_SMP_ASSERT(current_blkmap->ebd_vd_blk != XTNT_TERM);
                copy_buf->b_blkno = current_blkmap->ebd_vd_blk;
                copy_buf->b_bcount = current_blkmap->ebd_byte_cnt;

                /* 
                 * Since we have not started the I/O for the primary buf, 
                 * we know that the I/O anchor can not go away.  So it is safe 
                 * to get this I/O started.
                 *
                 * TODO: We need to handle FP_NOWAIT
                 */

                advfs_bs_startio(copy_buf, 
                                 ioAnchor, 
                                 bsBuf_iolist, 
                                 bfap, 
                                 VD_HTOP(current_blkmap->ebd_vd_index, 
                                         bfap->dmnP),
                                 ioflags);

            }

            /* 
             * If multiple block maps cover a single VM page, 
             * free all the primary block maps except the first one,
             * which putpage needs to have stay valid so it can reference
             * it and decide if there is more I/O to do.
             */
            if (length > primary_blkmap->ebd_byte_cnt) {
                (void)advfs_free_blkmaps(&(primary_blkmap->ebd_next_desc));
            }
        }

        /*
         * We're done with the primary block map.
         * Now launch I/O's for any secondary block maps.
         */
        if (secondary_blkmap && 
            (secondary_blkmap->ebd_offset < (current_io_offset + chunk_size))) {
            
            struct buf          *copy_buf;
            extent_blk_desc_t *sblkmap=secondary_blkmap;
            vdIndexT            cur_vd_idx;

            while ((secondary_blkmap) &&
                   (secondary_blkmap->ebd_offset < 
                    (current_io_offset + chunk_size))) {

                if (secondary_blkmap->ebd_offset < current_io_offset) {

                    /* Metadata processing could cause us to come in
                     * here with the seconday blkmap offset starting before
                     * the current_io_offset. This could happen since we may not
                     * have been able to issue i/o on some of the meta-data
                     * pages and are working in the middle of the primary
                     * blkmap. 
                     */
                    /* Clip the blkmaps to start at the loop offset or
                     * just release it and reloop */
                     
                    MS_SMP_ASSERT(bsBufList);
                    
                    if ((secondary_blkmap->ebd_offset +
                         secondary_blkmap->ebd_byte_cnt) < current_io_offset) {
                        
                        extent_blk_desc_t *tblkmap=secondary_blkmap;
                    
                        /* This secondary blkmap does not overlap the
                         *  current write at all. Remove it and move on
                         *  the the next one */
                        secondary_blkmap = secondary_blkmap->ebd_next_desc;
                        tblkmap->ebd_next_desc = NULL;
                        (void)advfs_free_blkmaps(&tblkmap);
                        continue;
                    } else {
                        /* Make the begining match the current_io_offset */
                        MS_SMP_ASSERT(secondary_blkmap->ebd_vd_blk != 
                                      XTNT_TERM);
                        secondary_blkmap->ebd_byte_cnt -= 
                            current_io_offset - secondary_blkmap->ebd_offset;
                        secondary_blkmap->ebd_vd_blk += 
                              (current_io_offset - secondary_blkmap->ebd_offset)
                              / DEV_BSIZE;
                        secondary_blkmap->ebd_offset = current_io_offset;
                    }
                }

                spin_lock(&ioAnchor->anchr_lock);
                ioAnchor->anchr_iocounter++;
                spin_unlock(&ioAnchor->anchr_lock);

                /* Get a copy buf structure (fake) */

                /* This needs to be examined further for the FP_NOWAIT case.
                 * We have lready created a buf struct which will require that
                 * we do a biodone to release the pages on the buf struct. 
                 * If this was metadata then we also need to deal with the
                 * bsBufs associated with the anchor.
                 *
                 * For now I will turn off the NOWAIT for this call.
                 */

                copy_buf = advfs_bs_get_buf(TRUE, FALSE);
                
                /* 
                 * The I/O completion will, by default, free these "fake" bufs.
                 */
                
                bcopy(bufp,copy_buf,sizeof(struct buf));
                /* The secondary may not start on the same offset as
                 * the primary and also may not end on the same
                 * boundary. Remember the secondary will always start
                 * >= the primary offset.
                 */

                copy_buf->b_foffset = secondary_blkmap->ebd_offset;
                copy_buf->b_un.b_addr += 
                                      (daddr_t) (secondary_blkmap->ebd_offset - 
                                                 current_io_offset);
                MS_SMP_ASSERT(secondary_blkmap->ebd_vd_blk != XTNT_TERM);
                copy_buf->b_blkno = secondary_blkmap->ebd_vd_blk;
                copy_buf->b_bcount = MIN(copy_buf->b_bcount - 
                                         (secondary_blkmap->ebd_offset - 
                                          current_io_offset),
                                         secondary_blkmap->ebd_byte_cnt);

                if (!ADVFS_FILE_HAS_SMALL_AU(bfap)) {
                    MS_SMP_ASSERT((copy_buf->b_foffset&(VM_PAGE_SZ-1)) == 0);
                    MS_SMP_ASSERT(((uint64_t)copy_buf->b_un.b_addr &
                                (VM_PAGE_SZ - 1)) == 0);
                    MS_SMP_ASSERT((copy_buf->b_bcount&(VM_PAGE_SZ-1)) == 0);
                }

                /*
                 * We need to advance counters based on the copy_buf, but we
                 * can't touch it after we issue the io since it may be
                 * freed in advfs_iodone.  Therefore, we will keep the
                 * cur_vd_idx and make adjustments before issuing IO
                 */
                cur_vd_idx = secondary_blkmap->ebd_vd_index;

                if ((secondary_blkmap->ebd_offset + 
                     secondary_blkmap->ebd_byte_cnt) >
                    (current_io_offset + chunk_size)) {
                    /* This secondary map extends beyond the i/o we
                     * are doing for the primary. We need to update
                     * this secondary blkmap to indicate the portion
                     * we have processed.
                     */

                    secondary_blkmap->ebd_offset += copy_buf->b_bcount;
                    secondary_blkmap->ebd_byte_cnt -= copy_buf->b_bcount;
                    secondary_blkmap->ebd_vd_blk += 
                        copy_buf->b_bcount / DEV_BSIZE;

                    MS_SMP_ASSERT((secondary_blkmap->ebd_offset&(VM_PAGE_SZ-1)) 
                                  == 0);
                    MS_SMP_ASSERT((secondary_blkmap->ebd_byte_cnt & 
                                   (VM_PAGE_SZ-1)) == 0);
                } else {
                    /* We are done with this secondary blkmap
                     * entry. Remove it so that next loop or call will
                     * avoid having to process it
                     */
                    extent_blk_desc_t *tblkmap=secondary_blkmap;
                    secondary_blkmap = secondary_blkmap->ebd_next_desc;
                    tblkmap->ebd_next_desc = NULL;
                    (void)advfs_free_blkmaps(&tblkmap);
                }

                /* 
                 * Since we have not started I/O for the primary, we know 
                 * the anchor can not go away. So it is safe to get this I/O
                 * started.
                 * TODO: We need to handle FP_NOWAIT
                 */

                advfs_bs_startio(copy_buf, 
                                 ioAnchor, 
                                 bsBuf_iolist, 
                                 bfap, 
                                 VD_HTOP(cur_vd_idx,bfap->dmnP),
                                 ioflags);

            }
        }

        /*
         * Now we can start i/o on the primary blkmap.
         * TODO: We need to handle FP_NOWAIT
         */
        advfs_bs_startio(bufp, 
                         ioAnchor, 
                         bsBuf_iolist,
                         bfap, 
                         vdp,
                         ioflags);

        current_io_offset += chunk_size;
        remaining_io_size -= chunk_size;
        chunk_size = MIN(chunk_size, remaining_io_size);
    }

    if (passed_secondary_blkmap) {
        *passed_secondary_blkmap = secondary_blkmap;
    }
    return(error);
}


/*
 * advfs_bs_zero_fill_pages() 
 * 
 * This function zero-fills pages in memory, optionally writing them
 * to disk synchronously.  It only zero-fills full pages.  This
 * function will write the zero-filled pages to disk synchronously if
 * the fileset supports the "objectsafety" option and there is backing
 * store.  Otherwise, it will only zero-fill the in-memory UFC pages
 * and then return.  It is assumed that a page list will only be
 * passed in for the case that the pages were not found in cache.  It
 * is also assumed that it is acceptable for these pages to be
 * released here.
 *
 * This function takes no locks.  Storage is assumed to be accessible
 * by only this thread since no other threads can access that storage
 * until the current thread releases the file lock.
 * We are holding the extentmap lock for write (we are adding storage).
 *
 * NOTE: When a plist of pages is passed in to be zeroed, it is the
 * responsibility of the caller to mark these pages dirty if so desired.
 * Pages cannot be protected and marked dirty at the same time. 
 */
int 
advfs_bs_zero_fill_pages(
    struct vnode *vp,		 /* in - File's vnode structure */
    off_t zero_fill_offset,	 /* in - Offset of first byte to zero-fill */
    size_t zero_fill_length,	 /* in - Number of bytes to zero-fill */
    int protect,                 /* in - Set write protection on each page */
    page_fsdata_t **plistp,	 /* in - Pages from fcache_page_alloc() */
    fcache_vminfo_t *fc_vminfo,	 /* in - Pass to fcache_page_alloc(), etc. */
    fcache_ftype_t intent,       /* in - specifies read or write */
    struct advfs_pvt_param *fs_priv_param, /* in - private params for putpage.*/
    int flush                    /* in - flush cache due to BFS_OD_OBJ_SAFETY */
)
{
    off_t zstart;
    off_t loop_end,loop_off;
    size_t zsize;
    bfAccessT *bfap = VTOA(vp);
    page_fsdata_t *pfdat, *plist = *plistp;
    fcache_pflags_t pflags;
    int err=0;

    /* Round down zero_fill_offset to the next VM page boundary.
     * Adjust zero_fill_length to account for the change to
     * zero_fill_offset.  Also, adjust zero_fill_length up to the next
     * VM page boundary.  That is, adjust the beginning and end of the
     * passed in zero-fill range to encompass full VM page
     */

    MS_SMP_ASSERT((bfap->dataSafety == BFD_USERDATA) || (protect == FALSE));

    zstart = rounddown(zero_fill_offset,VM_PAGE_SZ);
    zsize = zero_fill_length + (zero_fill_offset - zstart);
    zsize = roundup(zsize,VM_PAGE_SZ);

    if ((plist == NULL) && (intent == FCF_DFLT_WRITE)) {

        /* This is either objectSafety and we need to bring in and zero
         * all pages that storage was allocated for or we allocated storage and
         * ended up rounding out to an allocation unit that was beyond the 
         * request size. 
         */
        
        MS_SMP_ASSERT(!protect); /* not implemented in this case */

        loop_end = zstart + zsize;
        loop_off = zstart;
        while (loop_off <  loop_end) {
            off_t alloc_offset = loop_off;
            size_t alloc_size = loop_end - loop_off;
            fpa_status_t alloc_status;
            
            /* We are going to bag big pages here. We could try to be
             * cute and release pages on either side of a big page but
             * that would probably cause the thing to be demoted
             * anyway. We could still find a big page if it was already
             * in the cache however.
             */
            /* 
             * NOTE:  If min_coff/max_coff arguments are changed, then look
             *        at the advfs_getpage() example so that Direct I/O is
             *        properly taken into consideration.  Because Direct I/O
             *        can be doing concurrent, I/O bypassing the UFC, it
             *        needs to know that pages loaded into the UFC do _NOT_
             *        cross into Active Ranges used to protect Direct I/O
             *        operations.  Big pages have the potential to leak into
             *        an Active Range and can cause stale data to be written
             *        to disk from the UFC after a Direct I/O has written
             *        new data directly to disk.
             */

            plist = fcache_page_alloc(fc_vminfo,
                                      vp, 
                                      &alloc_offset,
                                      &alloc_size,
                                      alloc_offset, /* No big pages */
                                      alloc_offset + alloc_size,/*!big pages */
                                      &alloc_status,
                                      FPA_NEW|FPA_PGCACHE,
                                      FP_DEFAULT);
#ifdef ADVFS_SMP_ASSERT
            /* If the file is opened for Direct I/O, make sure the pages
             * being added to the UFC have an active range associated with
             * them, otherwise stale data could be returned or written to
             * the file, causing data corruption or the appearence of
             * corruption.
             */
            if ( plist && bfap->dioCnt ) {
                ASSERT_ACTRANGE_HELD( bfap, alloc_offset, alloc_size );
            }
#endif /* ADVFS_SMP_ASSERT */

            if (plist == NULL) {
                MS_SMP_ASSERT(FALSE);
            }

            /* We could have pages that are already zeroed in the
             * cache from a read. We need to unprotect these pages.
             * In the cache of ObjectSaftey they will be unportected
             * twice later on in get page but who cares.
             * We also need to set up for pages to be marked dirty
             * after we zero them so that they will be eligible for flushing.
             */
            pfdat = plist;
            while (pfdat) {
                /* There is an assumption here that migrate will
                 * not fault in holes thereby potentially reading
                 * in unallocated storage.
                 */
                if (pfdat->pfs_off > (off_t)(zstart + zsize - 1)) 
                {
                    MS_SMP_ASSERT(alloc_status == FPA_ST_EXISTS);
                    break;
                }
                if (pfdat->pfs_off >= zstart){

                    if (alloc_status == FPA_ST_EXISTS) {
                        pfdat->pfs_info.pi_pg_ro = 0;
                    }
                    pfdat->pfs_info.pi_pg_setdirty = 1;
                } else {
                    MS_SMP_ASSERT(alloc_status == FPA_ST_EXISTS);
                }
                pfdat = pfdat->pfs_next;
            }
            if ((alloc_status == FPA_ST_DATAFILL) ||
                (bfap->bfSetp->bfSetFlags & BFS_OD_OBJ_SAFETY)) {

                /* All objectSafety pages need to be zerofilled. */
                struct buf *bufp;

                pflags = FP_WRITE|FP_ASYNC;
                bufp = fcache_buf_create(fc_vminfo, vp, alloc_size, 0UL, &plist, pflags);
                MS_SMP_ASSERT(bufp != NULL);
                bufp->b_vp = vp;
                BSETFLAGS(bufp, (bufflags_t)B_RESYNC);     /* 
                                                            * This will cause the pages 
                                                            * to be put back on the
                                                            * syncer queue.
                                                            */
                                                  
                bufp->b_foffset = alloc_offset;
                bufp->b_bcount = alloc_size;
                bzero(bufp->b_un.b_addr,alloc_size);
                biodone(bufp);
            } else {
                MS_SMP_ASSERT(alloc_status == FPA_ST_EXISTS);
                /* These pages are already zeroed */

                err = fcache_page_release(fc_vminfo, 
                                          alloc_size,
                                          &plist,
                                          FP_DEFAULT|FP_RESYNC);

                MS_SMP_ASSERT(err == 0);
            }
            loop_off = alloc_offset + alloc_size;
        }
    } else {
        struct buf *bufp;
        
        page_fsdata_t *pfdat=plist;

        if (protect) {
            ADVFS_ACCESS_LOGIT_2( bfap, "zero_fill_pages protecting pages",
                    zero_fill_offset, zero_fill_length );
        }

        
        while (pfdat != NULL) {
            MS_SMP_ASSERT(pfdat->pfs_off >= zstart);
            if (protect) {
                pfdat->pfs_info.pi_pg_ro = 1;
            } else {
                pfdat->pfs_info.pi_pg_ro = 0;
            }

            if ((pfdat->pfs_off + VM_PAGE_SZ) >= (zstart + zsize)) {
                /* We may not be zeroing the entire list of pages */
                
                break;
            }
            pfdat = pfdat->pfs_next;
        }
        /* 
         * Now fake the i/o to release the pages and free the buf struct.
         */
        
        pflags = FP_WRITE|FP_ASYNC;
        bufp = fcache_buf_create(fc_vminfo,vp, zsize, 0UL, &plist, pflags);
        MS_SMP_ASSERT(bufp != NULL);
        bufp->b_vp = vp;
	if(intent == FCF_DFLT_WRITE) {
	    BSETFLAGS(bufp, (bufflags_t)B_RESYNC); /* This will cause the pages to remain dirty */
	}
        bufp->b_foffset = zstart;
        bufp->b_bcount = zsize;
        bzero(bufp->b_un.b_addr, zsize);
        biodone(bufp);

        /* Return the updated plist */
        *plistp = plist;
    }

    if (flush) {
        /* We need to flush these to disk now */
        err = fcache_vn_flush (vp, 
                         zero_fill_offset, 
                         zero_fill_length, 
                         (uintptr_t)fs_priv_param,
                         FVF_WRITE | FVF_SYNC);
    }
    
    return (err);
}

#ifdef ADVFS_SMP_ASSERT

int
advfs_verify_unprot_pages(
    struct vnode *vp,		 /* in - File's vnode structure */
    off_t offset,	 /* in - Offset of first byte to zero-fill */
    size_t size,	 /* in - Number of bytes to zero-fill */
    fcache_vminfo_t *fc_vminfo	 /* in - Pass to fcache_page_alloc(), etc. */
)
{
    page_fsdata_t *pfdat, *plist;
    fcache_pflags_t pflags;
    int err=0;
    size_t alloc_size;
    fpa_status_t alloc_status;
    off_t alloc_offset;
    off_t ending_request_offset;
    bfAccessT *bfap = VTOA(vp);

    /* Loop thru all the pages and make sure there are not pages
     * left in the read-only state.
     */

    alloc_offset = offset;
    alloc_size = size;
    ending_request_offset = offset + size;

    while (alloc_offset < ending_request_offset) {
        /* 
         * NOTE:  If min_coff/max_coff arguments are changed, then look at
         *        the advfs_getpage() example so that Direct I/O is properly
         *        taken into consideration.  Because Direct I/O can be doing
         *        concurrent, I/O bypassing the UFC, it needs to know that
         *        pages loaded into the UFC do _NOT_ cross into Active
         *        Ranges used to protect Direct I/O operations.  Big pages
         *        have the potential to leak into an Active Range and can
         *        cause stale data to be written to disk from the UFC after
         *        a Direct I/O has written new data directly to disk.
         */
        plist = fcache_page_alloc(fc_vminfo,
                                  vp, 
                                  &alloc_offset,
                                  &alloc_size,
                                  alloc_offset, /* No big pages */
                                  ending_request_offset,
                                  &alloc_status,
                                  FPA_PGCACHE,
                                  FP_DEFAULT);
#ifdef ADVFS_SMP_ASSERT
        /* If the file is opened for Direct I/O, make sure the pages being
         * added to the UFC have an active range associated with them,
         * otherwise stale data could be returned or written to the file,
         * causing data corruption or the appearence of corruption.
         */
        if ( plist && VTOA(vp)->dioCnt ) {
            ASSERT_ACTRANGE_HELD( VTOA(vp), alloc_offset, alloc_size );
        }
#endif /* ADVFS_SMP_ASSERT */
        
        if (plist == NULL) {
            break;
        }
        
        /* Check only pages within the caller's range since fcache_page_alloc
         * may have adjusted the return size bigger due to large pages.
         */
        pfdat = plist;
        while (pfdat && (pfdat->pfs_off < ending_request_offset)) {
            /* A snapshot may have been created and pages may have just been
             * marked RO.
             */
            if (pfdat->pfs_off >= offset) {
                MS_SMP_ASSERT(pfdat->pfs_info.pi_pg_ro == 0 || 
                              HAS_RELATED_SNAPSHOT(bfap));
            }
            pfdat = pfdat->pfs_next;
        }

        err = fcache_page_release(fc_vminfo, 
                                  alloc_size,
                                  &plist,
                                  FP_DEFAULT|FP_RESYNC);
        
        MS_SMP_ASSERT(err == 0);
    
        alloc_offset += alloc_size;
        alloc_size = ending_request_offset - alloc_offset;
    }
    return(EOK);
}

#endif

    
/*
 * advfs_mmap - enable mmapping for a file.  
 *
 * ***********************  NOTE  *********************************
 * cfs_mmap() does *not* call advfs_mmap().
 * ****************************************************************
 */

advfs_mmap( struct vnode *vp,           /* in - vnode pointer */
           u_int         off,           /* in - starting file offset */
           u_long        size_bytes,    /* in - number of bytes to map */
           int           access )       /* in - memory access to allow */
{
    int ret = EOK;
    struct fsContext *contextp;
    bfAccessT *bfap;

    FILESETSTAT( vp, advfs_mmap );

    contextp = VTOC(vp);
    bfap = VTOA(vp);

    ADVRWL_BFAPCACHEMODE_READ(bfap);

    /*
     * Do not allow a metadata or log file to be memory-mapped.
     */

    if ((bfap->dataSafety == BFD_METADATA) || (bfap->dataSafety == BFD_LOG)) {
        ret = EINVAL;
    }
    else {
        if ( bfap->dioCnt ) {
            /* Do not allow mmap() to succeed if opened for directIO. */
            ret = EINVAL;
        }
    }

    ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);

    /*
     * Mark the file access time for update to comply with
     * XPG4 Issue 5 and ISO/IEC 9945-1 (ANSI/IEE Std. 1003.1)
     * We mark for update here as the standard only requires 
     * the ATIME be updated on the first fault on the region, 
     * and we don't want to put it in getpage because we 
     * don't want to mark ATIME multiple times which would cause 
     * writes of the stats on each sync() call hurting performance.
     */
    if (ret == EOK) {
        ADVSMP_FILESTATS_LOCK( &contextp->fsContext_lock );
        contextp->fs_flag |= MOD_ATIME;
        if ( !(vp->v_vfsp->vfs_flag & VFS_RDONLY && 
                    (ADVGETBFSETP(vp->v_vfsp)->bfSetFlags & BFS_IM_NOATIMES)) )
        {
            contextp->dirty_stats = TRUE;
        }
        ADVSMP_FILESTATS_UNLOCK( &contextp->fsContext_lock );
    }

    return( ret );
}

/*
 * advfs_munmap() 
 */
advfs_munmap(struct vnode *vp, u_int off, u_long size_bytes, int access)
{
    return(0);
}

advfs_bread(
    struct vnode *vp,
    kern_daddr_t lbn,
    struct buf **bpp, 
    struct vattr *vap,
    struct ucred *cred)
{
    return EIO;
}


advfs_brelse(
    struct vnode *vp,
    struct buf *bp)
{
    /* Keep statistics */
    FILESETSTAT( vp, advfs_brelse );
    return 0;
}

static int
advfs_volinfo(char *vol_path, 
              char *buffer,
              char **vol_dg,
              char **vol_name)
{
    char            *tok;
    struct pathname  pn;
    int              ret;
    struct pathname *pnp;
    char            *temp_str;
    int              error;

    pnp   = &pn;
    error = EOK;

    /* make sure it is an absolute path */
    if (vol_path[0] != '/') {
        return EINVAL;
    }

    tok      = ms_malloc((size_t) MAXPATHLEN);
    temp_str = ms_malloc((size_t) MAXPATHLEN);

    if ((error = pn_get(vol_path, UIOSEG_KERNEL, pnp))) {
        ms_free(tok);
        ms_free(temp_str);
        return error;
    }

    pn_skipslash(pnp);

    if ((error = (*pn_getcomponent)(pnp, tok))) {
        goto error;
    }
    if (strcmp(tok, "dev") || (pn_pathleft(pnp) == 0)) {
        error = EINVAL;
        goto error;
    }
    pn_skipslash(pnp);
    if ((error = (*pn_getcomponent)(pnp, tok))) {
        goto error;
    }
    if (strcmp(tok, "vx") == 0) {
        if (pn_pathleft(pnp) == 0) {
            error = EINVAL;
            goto error;
        }
        pn_skipslash(pnp);
        if ((error = (*pn_getcomponent)(pnp, tok))) {
            goto error;
        }
        if (strcmp(tok, "dsk") || pn_pathleft(pnp) == 0) {
            error = EINVAL;
            goto error;
        }
        pn_skipslash(pnp);
        if ((error = (*pn_getcomponent)(pnp, tok))) {
            goto error;
        }
        *vol_dg = buffer;
        strncpy(*vol_dg, tok, (int64_t)MAXPATHLEN);
        if (pn_pathleft(pnp) == 0) {
            error = EINVAL;
            goto error;
        }
        pn_skipslash(pnp);
        if ((error = (*pn_getcomponent)(pnp, tok))) {
            goto error;
        }
        *vol_name = buffer + strlen(buffer) + 1;
        strncpy(*vol_name, tok, (int64_t)MAXPATHLEN);
    }
    else if (strcmp(tok, "dsk") == 0 || strcmp(tok, "disk") == 0) {
        /* /dev/dsk and /dev/disk are not a valid logical vol pathnames */
        error = EINVAL;
        goto error;
    }
    else {
        struct vnode *vp;

        /* Assume the passed in path belongs to LVM */
        *vol_dg = buffer;
        strncpy(*vol_dg, tok, (int64_t)MAXPATHLEN);

        sprintf(temp_str, MAXPATHLEN, "/dev/%s/group", *vol_dg);
        if ((error = advfs_getvp(&vp, temp_str, UIOSEG_KERNEL, FOLLOW_LINK))) {
            goto error;
        }
        if ((vp->v_type != VCHR) || (pn_pathleft(pnp) == 0)) {
            VN_RELE(vp);
            error = EINVAL;
            goto error;
        }
        pn_skipslash(pnp);
        if ((error = (*pn_getcomponent)(pnp, tok))) {
            VN_RELE(vp);
            goto error;
        }
        *vol_name = buffer + strlen(buffer) + 1;
        strncpy(*vol_name, tok, (int64_t)MAXPATHLEN);
        VN_RELE(vp);
    }
    if (tok)
        ms_free(tok);
    if (temp_str)
        ms_free(temp_str);

    pn_free(pnp);
    return EOK;

error:

    if (tok)
        ms_free(tok);
    if (temp_str)
        ms_free(temp_str);

    pn_free(pnp);
    return error;
}

/*
 *  advfs_devtoname
 *
 *  Given a device path (/dev/disk/xxx, /dev/vgxx/xxxxx) determine 
 *  the domain name.
 */
static char *
advfs_devtoname(char *dev_path, int *error)
{
    char *scratch, *buffer, *leaf1, *leaf2, *ret_name;

    leaf1 = NULL;
    leaf2 = NULL;

    if (dev_path[0] != '/') {
        *error = EINVAL;
        return NULL;
    }
    scratch = ms_malloc((size_t) MAXPATHLEN);
    strncpy(scratch, dev_path, (size_t) MAXPATHLEN);

    leaf2 = strrchr(scratch, '/');
    if ((leaf2 == NULL) || (leaf2 == scratch)) {
        ms_free(scratch);
        *error = EINVAL;
        return NULL;
    }
    *leaf2 = '\0';
    leaf2++;
    leaf1 = strrchr(scratch, '/');
    if (leaf1 == NULL) {
        ms_free(scratch);
        *error = EINVAL;
        return NULL;
    }
    *leaf1 = '\0';
    leaf1++;

    buffer   = ms_malloc((size_t) MAXPATHLEN);
    ret_name = ms_malloc((size_t) MAXPATHLEN);

    if (!strcmp(leaf1, "disk") || !strcmp(leaf1, "dsk")) {
        sprintf(ret_name, MAXPATHLEN, "advfs_%s", leaf2);
    } else if (!strcmp(leaf1, "vol")) {
        sprintf(ret_name, MAXPATHLEN, "advfs_rootvol.%s", leaf2);
    } else if ((*error = 
                advfs_volinfo(dev_path, buffer, &leaf1, &leaf2)) == EOK) {
        sprintf(ret_name, MAXPATHLEN, "advfs_%s.%s", leaf1, leaf2);
    } else {
        ms_free(scratch);
        ms_free(ret_name);
        ms_free(buffer);
        return NULL;
    }
    ms_free(scratch);
    ms_free(buffer);
    return ret_name;
}

/*
 * advfs_parsespec() - Given an AdvFS filesystem path or device path,
 *                     will return the filesystem Domain Name and the 
 *                     fileset name, and whether the passed in path 
 *                     was a block device.  (other than an AdvFS 
 *                     pseudodevice - for example, /dev/disk/c4t1d0).
 *
 *                     The domain directory component may be either under
 *                     ADVFS_TCR_DMN_DIR (for non-SSI cluster shared domains)
 *                     or ADVFS_DMN_DIR.  It will always be under ADVFS_DMN_DIR
 *                     for SSI clusters or standalone systems.
 *
 *                     Note that the returned buffer has null's where
 *                     the slashes ought to be...
 */

int 
advfs_parsespec(const char *fsnamep,  /*  in  - path to be parsed */
                char *buffer,         /*  out - fully qualified AdvFS path */
                int   buflen,         /*  out - length of buffer */
                char **dmnName,       /*  out - filesystem domain  */
                char **setName,       /*  out - fileset (usually 'default') */
                int  *isblockdev)     /*  out - if fsnamep was a block device */
{
    struct vnode *vp = NULL;
    struct vnode *dirvp = NULL;
    int           error;
    int           i;
    int           maj;
    char*         dmnDir = NULL;
    char*         ptr = NULL;
    char          fsname[BS_DOMAIN_NAME_SZ + 1] = {0};
    char          user_set_name[NAMEMAX+1] = {0};

    error         = EOK;
    *dmnName      = NULL;
    *setName      = NULL;
    *isblockdev   = 0;

    if (fsnamep == NULL || buffer == NULL) {
        return EINVAL;
    }
    strncpy(buffer,fsnamep,(int64_t)buflen);

    /* Remove any trailing slashes */
    i = strlen(buffer) - 1;
    while (i >= 0) {
        if (buffer[i] != '/') 
            break;
        buffer[i--] = '\0';
    }
    if (strlen(buffer) == 0) {
        return EINVAL;
    }

    error = advfs_getvp(&vp, fsnamep, UIOSEG_KERNEL, FOLLOW_LINK);

    if (error) {
        goto error;
    }

    if ((vp->v_type != VBLK) && (vp->v_type != VDIR)) {
        error = EINVAL;
        goto error;
    }

    maj = major(vp->v_rdev);

    if ( (vp->v_type == VBLK) && (maj != ADVFSDEV_MAJOR)) {
        /* We have a device.  We need to translate to a domain name */
        char *tmpfsname;
        
        tmpfsname = advfs_devtoname((char*)fsnamep, &error);
        if (tmpfsname == NULL) {
            goto error;
        }

        if (strlen(tmpfsname) > BS_DOMAIN_NAME_SZ) {
            error = EINVAL;
        } else {
            strcpy(fsname, tmpfsname);
        }
        ms_free(tmpfsname);

        if (error) {
            goto error;
        }

        *isblockdev = 1;
    }

    if (maj == ADVFSDEV_MAJOR) {
        /* The passed in buffer is the fileset device. 
         * Try stripping the last component of the pathname to find 
         * the fileset.
         */

        ptr = strrchr(buffer,'/');
        if (ptr != NULL) {
            *ptr = '\0';
            ptr++;
            strncpy(user_set_name, ptr, sizeof(user_set_name)-1);
            user_set_name[sizeof(user_set_name)-1] = '\0';
        } else {
            error = EINVAL;
            goto error;
        }
    } 

    if (*isblockdev) {
        /*  Now find the top-level dir */
        error = EINVAL;
        if (clu_type() == CLU_TYPE_CFS) {
            dmnDir = ADVFS_TCR_DMN_DIR;
            sprintf(buffer, MAXPATHLEN, "%s/%s", dmnDir, fsname);
            error = advfs_getvp(&dirvp, buffer, UIOSEG_KERNEL, FOLLOW_LINK);
        }

        if (error) {
            dmnDir = ADVFS_DMN_DIR;
            sprintf(buffer, MAXPATHLEN, "%s/%s", dmnDir, fsname);
            error = advfs_getvp(&dirvp, buffer, UIOSEG_KERNEL, FOLLOW_LINK);
        }

        if (dirvp) {
            if (dirvp->v_type != VDIR) {
                error = EINVAL;
            }

            VN_RELE(dirvp);
        }
    } else {  
        /* What's in 'buffer' is the domain directory, presumably.
         * Try stripping the last component of the pathname to find 
         * the domain.
         */
        struct vnode*  tmpvp = NULL;

        ptr = strrchr(buffer,'/');
        if (ptr != NULL) {
            *ptr= '\0';
            ptr++;
            if (strlen(ptr) > BS_DOMAIN_NAME_SZ) {
                error = EINVAL;
            } else {
                strcpy(fsname, ptr);
            }
        } else {
            error = EINVAL;
        }

        if (error) {
            goto error;
        }

        /* get the vp for the parent directory as well */
        dmnDir = NULL;

        error = advfs_getvp(&dirvp, buffer, UIOSEG_KERNEL, FOLLOW_LINK);

        if (!error) {
            if (clu_type() == CLU_TYPE_CFS) {
                dmnDir = ADVFS_TCR_DMN_DIR;
                error = advfs_getvp(&tmpvp, dmnDir, UIOSEG_KERNEL, FOLLOW_LINK);
                if (!error) {
                    if (tmpvp != dirvp) {
                        dmnDir = NULL;
                    }
                    VN_RELE(tmpvp);
                }
            }

            if (dmnDir == NULL) {
                dmnDir = ADVFS_DMN_DIR;
                error = advfs_getvp(&tmpvp, dmnDir, UIOSEG_KERNEL, FOLLOW_LINK);
                if (!error) {
                    if (tmpvp != dirvp) {
                        dmnDir = NULL;
                    }
                    VN_RELE(tmpvp);
                }
            }

            if (!dmnDir) {
                error = EINVAL;
            }

            VN_RELE(dirvp);
        }
    }

    if (error != EOK) {
        goto error;
    }

    if (user_set_name[0] == '\0') {
        strncpy(user_set_name, "default", (int64_t)(NAMEMAX+1));
    }

    if (strlen(dmnDir) + strlen(fsname) + strlen(user_set_name) + 3 > 
        (uint)buflen) {

        error = ENAMETOOLONG;
        goto error;
    }

    /*  Return the normalized path, i.e., no CDSLs or /cdev */
    sprintf(buffer, buflen, "%s/%s/%s", dmnDir, fsname, user_set_name);

    /*  Set up the return string pointers */
    *setName = strrchr(buffer, '/');
    **setName = '\0';
    (*setName)++;

    *dmnName = strrchr(buffer, '/');
    **dmnName = '\0';
    (*dmnName)++;

    VN_RELE(vp);
    return EOK;

error:
    if (vp) {
        VN_RELE(vp);
    }
    return error;
}


/* This is a private interface between CFS and AdvFS for turning directIO
 * on or off for a given file.  CFS tracks the number of times a file is
 * opened for directIO, and uses this routine to tell AdvFS when to enable
 * or disable directIO.  Therefore, on a clustered system, the value for
 * bfap->dioCnt will be either 0 (directIO disabled) or 1 (directIO enabled).
 * flag values are: ADVFS_DIRECTIO or ADVFS_CACHED
 * Returns: EOK    if the change was made.
 *          EINVAL if the file mode could not be changed
 */
statusT
advfs_cfs_set_cachepolicy( struct vnode *vp,    /* in: vnode for file */
                           int    flag )        /* in: which mode to set */
{
    struct bfAccess  *bfap = VTOA( vp );
    struct fsContext *cp   = VTOC( vp );
    int    ret             = EOK;
    lock_t *sv_lock;

    /* must be a valid, opened file */
    if ( !bfap || !cp )
        return EINVAL;

    /* Don't change mode for reserved or metadata files */
    if ( bfap->dataSafety != BFD_USERDATA )
        return EINVAL;

    ADVRWL_BFAPCACHEMODE_WRITE(bfap);
    if ( flag == ADVFS_DIRECTIO ) {

        /* Don't allow DIO for non-REG files or memory mapped files */
        if ( (vp->v_type != VREG) || (vp->v_flag & VMMF) ) {
            ret =  EINVAL;
            goto cleanup;
        }

        /* If this file is not already open for DIO, do it */
        if ( bfap->dioCnt == 0 ) {
            bfap->dioCnt++;
            if ( cp->dirty_alloc ) {
                /* Flush any storage allocations to disk.  This is 
                 * necessary because CFS DIO assumes that if the block 
                 * is allocated to this file, it won't be removed on a 
                 * subsequent crash and recovery. 
                 */
                advfs_lgr_flush( bfap->dmnP->ftxLogP, nilLSN, FALSE );
            }
            /* flush and invalidate any pages in cache for this file */
            fcache_vn_flush( &bfap->bfVnode, 0L, 0UL, NULL,
                             FVF_WRITE | FVF_SYNC | FVF_INVAL );
        }
        MS_SMP_ASSERT( bfap->dioCnt == 1 );
    } else {
        MS_SMP_ASSERT( flag == ADVFS_CACHED );
        if ( bfap->dioCnt > 0 ) {
            MS_SMP_ASSERT( bfap->dioCnt == 1 );   /* shouldn't be > 1 */
            bfap->dioCnt--;
        }
    }

cleanup:
    ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);
    return( ret );
}


/* For testing crash-recovery logs - crash the system if crash_event_enum
 * is enabled.
 * By crashing the system at various places, and controling what metadata
 * gets flushed, we can exercise various paths in the crash-recovery code.
 *
 * Events are enabled, and crash_flag set using the user-mode utility, 
 * /usr/sbin/ess.
 *
 * ess_status is set to 1 if crash_event_enum is enabled.
 *
 * ess_crash_flag determines how the system will be brought down.
 * values for ess_crash_flag are:
 *
 * 0: no crash
 *    The system is not crashed. This is used for test development.
 * 
 * 1: domain_panic.
 *    Halts further access to disks and domain.
 *    AdvfsDomainPanicLevel = 0 causes domain_panic to not perform a live dump.
 *    See msfs_io.c for usage of AdvfsDomainPanicLevel.
 * 
 * 2: panic the system.  dmnP->crashTest is used in advfs_bs_dmn_flush_meta()
 *    which is called as a result panic(). If crashTest is set, metadata is
 *    not flushed.
 *
 * ESS callback for handling AdvfsCrash called by ESS_MACRO is AdvfsCrashFunc().
 */
#ifdef OSDEBUG

extern int AdvfsDomainPanicLevel;

void
advfs_crash_recovery_test ( domainT *dmnP, int crash_event_enum )
{
    int   ess_status     = 0;
    int   ess_crash_flag = 1;

    /* If we have already domain paniced, return so we don't trigger additional
     * events.
     */
    if (!dmnP || dmnP->dmn_panic) {
        return;
    }
        
    ESS_MACRO_3 (AdvfsCrash, crash_event_enum, dmnP->domainName, &ess_status, &ess_crash_flag);

    if (ess_status) {
        switch (ess_crash_flag) {
            case 0:             /* no crash */
	        printf ("AdvfsCrash_%d triggered noop\n", crash_event_enum);
                break;

            case 1:             /* domain panic */
	        printf ("AdvfsCrash_%d triggered domain panic\n", crash_event_enum);
                dmnP->crashTest = 1;
                AdvfsDomainPanicLevel = 0;
	        advfs_lgr_flush( dmnP->ftxLogP, nilLSN, FALSE );
                domain_panic (dmnP, "Crash Recovery Test triggered domain panic");
                break;

            case 2:             /* panic */
		printf ("AdvfsCrash_%d triggered panic\n", crash_event_enum);
                dmnP->crashTest = 1;
                panic ("Crash Recovery Test triggered panic");
                break;
        }
    }
}
#endif
