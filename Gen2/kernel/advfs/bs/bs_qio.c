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
 *     AdvFS 
 *
 * Abstract:
 *
 *      bs_qio.c - Various AdvFS I/O and flush routines.
 *
 */

#define ADVFS_MODULE  BS_QIO

#include <limits.h>
#include <sys/file.h>
#include <sys/proc_iface.h>
#include <sys/kdaemon_thread.h>
#include <sys/systm.h>
#include <sys/rtprio.h>
#include <sys/sched.h>
#include <h/kthread_access.h>     /* private: for DEVSW(), needs KT_SEMA() */
#include <sys/buf.h>              /* bsetprio() macro */
#include <sys/conf.h>
#include <sys/spinlock.h>
#include <sys/vm_arena_iface.h>

#include <ms_public.h>
#include <ms_privates.h>
#include <bs_msg_queue.h>
#include <ms_osf.h>
#include <bs_ims.h>
#include <bs_buf.h>


#ifdef ADVFS_SMP_ASSERT
    extern int AdvfsDomainPanicLevel;
#   define ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3  AdvfsDomainPanicLevel=3
#else
#   define ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3
#endif

/*
 * Forward references
 */

extern kmem_handle_t advfs_ioanchor_arena;

/* 
 * Check validity of RBMT page 0
 */
static statusT
advfs_verify_rbmt_page0( bfAccessT * bfap, off_t offset, void* addr) {
    bsMPgT *rbmt_page = (bsMPgT*)addr;
    MS_SMP_ASSERT( offset == 0);

    if ((rbmt_page->bmtPageId != 0) || (rbmt_page->bmtMagicNumber != RBMT_MAGIC)) {
        domain_panic(bfap->dmnP, "Rbmt Page 0 does not represent page 0.  pageId = %d",
                   rbmt_page->bmtPageId);
    }

    return EOK;
}

/* 
 * Check validity of SBM page 
 */
static statusT
advfs_verify_sbm_page( bfAccessT * bfap, off_t offset, void* addr) {
    bsStgBmT *sbm_page = (bsStgBmT*)addr;

    if ((sbm_page->pageNumber != (offset / (bfap->bfPageSz * ADVFS_FOB_SZ)) ) ||
        (sbm_page->magicNumber != SBM_MAGIC) ) {
        domain_panic(bfap->dmnP, "SBM Page number does not match.  Expected %Xd, read %Xd",
                      (offset /(bfap->bfPageSz * ADVFS_FOB_SZ)), 
                      sbm_page->pageNumber);
    }
    /*
     * Later, could compute xor to verify page 
     */

    return EOK;
}

/* 
 * Check validity of tag directory page 
 */
static statusT
advfs_verify_tagdir_page( bfAccessT * bfap, off_t offset, void* addr) {
    bsTDirPgT *tagdir_page = (bsTDirPgT*)addr;
    if ( (tagdir_page->tpPgHdr.currPage != (offset / (bfap->bfPageSz * ADVFS_FOB_SZ))) ||
         ((tagdir_page->tpPgHdr.magicNumber != TAG_MAGIC) && 
          (tagdir_page->tpPgHdr.magicNumber != RTTAG_MAGIC) ) ) {
        domain_panic(bfap->dmnP, "Expected Tag page offset %d, found offset %Xd\n", 
                   offset, 
                   bfap->bfPageSz * ADVFS_FOB_SZ);
    }
    return EOK;
}

/* 
 * Check validity of log page 
 */
static statusT
advfs_verify_log_page( bfAccessT * bfap, off_t offset, void* addr) {
    logPgT * log_page = (logPgT*)addr;
    /*
     * This check may not always be valid after a crach.  Must make sure to
     * pass the NO_VERIFY flag to ref/pinpg so we don't try to check log
     * pages in that case.
     */
    if ( !BS_COOKIE_EQL(log_page->hdr.fsCookie, log_page->trlr.fsCookie) ) {
        domain_panic(bfap->dmnP, "log page header and trailer have id mismatch.\n");
    }
    return EOK;
}

/* 
 * Check validity of BMT page 
 */
static statusT
advfs_verify_bmt_page( bfAccessT * bfap, off_t offset, void* addr) {
    bsMPgT *bmt_page = (bsMPgT*)addr;

    if ( (bmt_page->bmtPageId != (offset / (bfap->bfPageSz * ADVFS_FOB_SZ))) ||
         (bmt_page->bmtMagicNumber != BMT_MAGIC) ) {
        domain_panic(bfap->dmnP, "BMT page at offset %d does not represent correct page.  pageId = %d", 
                   offset, bmt_page->bmtPageId);
    }

    return EOK;
}

/* 
 * Check validity of MISC file page 
 */
static statusT
advfs_verify_misc_page( bfAccessT * bfap, off_t offset, void* addr) {
    /*
     * Nothing to verify 
     */
    return EOK;
}

/* 
 * Check validity of RBMT page (not page 0).
 */
static statusT
advfs_verify_rbmt_ext_page( bfAccessT * bfap, off_t offset, void* addr) {
    bsMPgT *rbmt_page = (bsMPgT*)addr;

    if ( (rbmt_page->bmtPageId != (offset / (bfap->bfPageSz * ADVFS_FOB_SZ))) ||
         (rbmt_page->bmtMagicNumber != RBMT_MAGIC) ) {
        domain_panic(bfap->dmnP, "RBMT page at offset %d does not represent correct page.  pageId = %d", 
                   offset, rbmt_page->bmtPageId);
    }

    return EOK;
}

/*
 * advfs_verify_meta_page 
 *
 * This routine is intended as a framework for verifying metadata after IOs.
 * When faulting in metadata from bs_refpg or bs_pinpg, advfs_getpage or
 * advfs_getmetapage will set flags in the advfs_pvt_params.app_flags field
 * that indicate that IO was performed.  When IO is performed for any part
 * of a metadata page, this routine can be called to do lightweigh validity
 * checking.  
 *
 * Initially, there is only one level of sanity check, but this routine can
 * be extended to provide various levels of checking depending on debug
 * levels.  
 *
 */

void
advfs_verify_meta_page( bfAccessT *bfap, off_t offset, void* addr)    /* in */
{
    
    /* Check to see what type of RSVD'd meta file this is */
    if (BS_IS_TAG_OF_TYPE( bfap->tag, BFM_SBM )) {
        /* Check SBM page validity */
        advfs_verify_sbm_page( bfap, offset, addr);
    } else if (BS_IS_TAG_OF_TYPE( bfap->tag, BFM_BFSDIR )) {
        /* Check root tag dir page validity */
        advfs_verify_tagdir_page( bfap, offset, addr);
    } else if (BS_IS_TAG_OF_TYPE( bfap->tag, BFM_FTXLOG )) {
        /* Check LOG page validity */
        advfs_verify_log_page( bfap, offset, addr);
    } else if (BS_IS_TAG_OF_TYPE( bfap->tag, BFM_BMT )) {
        /* Check BMT page validity */
        advfs_verify_bmt_page( bfap, offset, addr);
    } else if (BS_IS_TAG_OF_TYPE( bfap->tag, BFM_MISC )) {
        /* Check MISC file page validity */
        advfs_verify_misc_page( bfap, offset, addr);
    } else if (BS_IS_TAG_OF_TYPE( bfap->tag, BFM_RBMT)) {
        if (offset == 0) {
            /* check rbmt page 0 validity */
            advfs_verify_rbmt_page0( bfap, offset, addr);
        } else {
            /* Check RBMT page validity (not page 0) */
            advfs_verify_rbmt_ext_page( bfap, offset, addr);
        }
    } 

}

/*
 * advfs_bs_get_adviodesc
 * 
 * Allocates memory for an adviodesc_t structure. 
 * Zeroing of memory is optional.
 * The magic identifier field gets initialized in all cases.
 *
 * Returns a pointer or NULL.
 */
adviodesc_t *
advfs_bs_get_adviodesc(
    int32_t nozero)          /*In:TRUE/FALSE flag to not zero memory*/
{
    adviodesc_t * iodescp;

    /* Zero the memory based upon the caller's request. */
    if (nozero) {
        iodescp=(adviodesc_t *)ms_malloc_no_bzero((uint32_t)sizeof(adviodesc_t));
    } else {
        iodescp=(adviodesc_t *)ms_malloc((uint32_t)sizeof(adviodesc_t));
    }

    iodescp->advio_magicid = ADVIODESCMAGIC;
    return(iodescp);
}

/* advfs_bs_free_adviodesc
 * 
 * Frees the memory for an adviodesc_t structure.
 */
void
advfs_bs_free_adviodesc(adviodesc_t * iodescp) /* in: structure to free */
{
    iodescp->advio_magicid |= MAGIC_DEALLOC;
    ms_free(iodescp);
}

/* advfs_bs_ioanchor_ctor
 *
 * Ioanchor_t structure memory arena constructor. This will allocate
 * the structure's spinlock and condition variable.
 * The kernel memory arena component calls this function.
 */
int
advfs_bs_ioanchor_ctor(void *ioanchorp,
                       size_t ioAnchorSize,
                       int flags)
{
    ioanchor_t *anchorp;

    anchorp = (ioanchor_t *) ioanchorp;
    ADVSMP_IOANCHOR_INIT(&anchorp->anchr_lock,
                       (flags & M_WAITOK) ? TRUE : FALSE);
    MS_SMP_ASSERT( (uintptr_t)&anchorp->anchr_lock > 0x100L );
    cv_init(&anchorp->anchr_cvwait, "ADVFS_IOANCHOR", NULL, CV_WAITOK);
    anchorp->anchr_magicid = (IOANCHORMAGIC | MAGIC_DEALLOC);
    return(EOK);
}

/* advfs_bs_ioanchor_dtor
 *
 * Ioanchor_t structure memory arena destructor. This will deallocate
 * the structure's spinlock and condition variable.
 * The kernel memory arena component calls this function.
 */
void
advfs_bs_ioanchor_dtor(void *ioanchorp,
                       size_t ioanchorSize,
                       int flags)
{
    ioanchor_t *anchorp;

    anchorp = (ioanchor_t *) ioanchorp;
    MS_SMP_ASSERT(anchorp->anchr_magicid == (IOANCHORMAGIC | MAGIC_DEALLOC));
    /*
     * To avoid a race with advfs_iodone() we acquire and release the lock
     * before destroying it.  The situation is that advfs_iodone() can be
     * holding the lock when it calls cv_broadcast(), and before it gets to
     * the unlock, another thread on a different CPU, might wake up and free
     * the ioanchor_t, and then the arena code might decide it needs to
     * reclaim this memory and call here.  So by acquiring and releasing the
     * lock, we make sure that advfs_iodone() is really finished.  And since
     * it should be rare for the arena to reclaim ioanchor_t structures, and
     * because the time window is so short anyway, this acquire/release
     * should not occur often and when it does occur it should be really
     * quick.
     */
    spin_lock(&anchorp->anchr_lock);
    spin_unlock(&anchorp->anchr_lock);
    /*
     * We can safely destroy the lock now.
     */
    spin_destroy(&anchorp->anchr_lock);
    cv_destroy(&anchorp->anchr_cvwait);
}    

/*
 * advfs_bs_get_ioanchor
 * 
 * Allocates an ioanchor_t structure from the memory allocator.
 * 
 */
ioanchor_t *
advfs_bs_get_ioanchor(
    int32_t wait)         /*In: M_WAITOK/M_NOWAIT flag to wait for memory */
{
    ioanchor_t * ioanchorp;

    ioanchorp = (ioanchor_t *) kmem_arena_alloc(advfs_ioanchor_arena, wait);

    /* Initialize the structure if it exists.
     * The anchr_lock and anchr_cvwait synchronization locks were
     * initialized by the memory arena.
     */
    if (ioanchorp) {
        MS_SMP_ASSERT(ioanchorp->anchr_magicid ==
                      (IOANCHORMAGIC | MAGIC_DEALLOC));
        MS_SMP_ASSERT( (uintptr_t)&ioanchorp->anchr_lock > 0x100L );
        ioanchorp->anchr_iocounter = 0;
        ioanchorp->anchr_flags = 0;
        ioanchorp->anchr_origbuf = NULL;
        ioanchorp->anchr_listfwd = NULL;
        ioanchorp->anchr_listbwd = NULL;
        ioanchorp->anchr_aio_bp = NULL;
        ioanchorp->anchr_io_status = 0;
        ioanchorp->anchr_min_req_offset = 0;
        ioanchorp->anchr_min_err_offset = 0;
        ioanchorp->anchr_magicid = IOANCHORMAGIC;
        ioanchorp->anchr_aio_info.anchr_actrangep   = NULL;
        ioanchorp->anchr_aio_info.anchr_lock_key    = NULL;
        ioanchorp->anchr_aio_info.anchr_aio_iosize  = 0;
        ioanchorp->anchr_aio_info.anchr_aio_sid     = 0;
        ioanchorp->anchr_aio_info.anchr_aio_address = NULL;
        ioanchorp->anchr_aio_info.anchr_aio_size    = 0;
        ioanchorp->anchr_aio_info.anchr_aio_flags   = ANCHR_AIO_NOFLAGS;
        ioanchorp->anchr_error_ios = NULL;
        ioanchorp->anchr_buf_copy = NULL;
    }
    return(ioanchorp);
}

/* advfs_bs_free_ioanchor
 * 
 * Frees an ioanchor_t structure back to the memory allocator.
 * Wait flag indicates whether or not caller allows this routine to sleep.
 *
 */
void
advfs_bs_free_ioanchor(
    ioanchor_t * ioanchorp, /* in: ioanchor_t to free */
    int32_t wait)           /* in: M_WAITOK/M_NOWAIT to permit sleeping*/
{
    ioanchorp->anchr_magicid |= MAGIC_DEALLOC;
    kmem_arena_free(ioanchorp, wait);
}


/*
 * advfs_multiWait_wakeup
 *
 * Notify any threads that are flushing this file and are waiting
 * for this buffer to be flushed that this buffer has been flushed.
 *
 * Assumptions:
 * This routine is operates on metadata and log data files only.
 * The caller must hold the bsBuf.bsb_lock.
 */

static void
advfs_multiWait_wakeup(struct bsBuf *bsbufp) /* in - Buffer being released */
{
    MS_SMP_ASSERT(spin_owned(&bsbufp->bsb_lock));

    /* Process multiWaitLinkT's linked to the given bsBuf structure.
     * If one or more threads are waiting for this buffer to complete
     * its I/O, walk the multiWaitLinkT list for this buffer, waking 
     * up any waiters if this is the last I/O (event) they were waiting for.
     * Free each multiWaitLinkT back to its memory arena as we use it.
     */
    if (bsbufp->bsb_mwllist) {
        multiWaitLinkT *mwlp;
        multiWaitT *mwp;

        do {
            mwlp = bsbufp->bsb_mwllist;
            mwp = mwlp->mwl_mw;
            spin_lock(&mwp->mw_lock);
            MS_SMP_ASSERT(mwp->mw_outstandingEvents > 0);

            if (--mwp->mw_outstandingEvents == 0) {
                cv_signal(&mwp->mw_cv, NULL, CV_NULL_LOCK);
            }
            spin_unlock(&mwp->mw_lock);

            bsbufp->bsb_mwllist = (multiWaitLinkT *)mwlp->mwl_fwd;
            ms_free(mwlp);
        } while (bsbufp->bsb_mwllist);
    }
}

/*
 * advfs_bs_startio()
 *
 * Fill balance of struct buf and call strategy routine for disk.
 *
 * Assumptions:
 *   Caller must initialize the following structure fields:
 *
 *      buf.flags: B_READ/B_WRITE, B_ASYNC/B_SYNC and any other flags.
 *      buf.b_vp: File's vnode pointer.
 *      buf.b_un.b_addr: virtual memory buffer address
 *      buf.b_foffset: starting logical file byte offset
 *      buf.b_blkno: starting logical disk block number
 *      buf.b_bcount: byte length of IO
 *      buf.b_spaddr: hardware space address id of buffer
 *
 *    DirectIO must additionally set these fields:
 *      ioanchor.anchr_flags: IOANCHORFLG_DIRECTIO
 *      ioanchor.anchr_aio_bp: Asynchronous IO buf structure or NULL
 *      ioanchor.anchr_actrangep: File's Active Range Lock structure
 *      buf.b_proc: process structure doing the IO.
 */

void
advfs_bs_startio(struct buf * bp,       /*in */
                 ioanchor_t *ioanchorp, /*in: ioanchor setup by caller */
                 struct bsBuf * bsbufp, /*in: Valid for metadata write IO. */
                                        /* Otherwise, set to NULL. */
                 bfAccessT *bfap,       /*in: file access structure */
                 struct vd *vdp,        /*in */
                 uint64_t flags)        /*in: Flags*/
{
    adviodesc_t *iodescp;
    sema_t * save;
    uint64_t dummy;
    int error;

    MS_SMP_ASSERT((vdp != NULL) || (flags & ADVIOFLG_FAKEIO));

    /* For AdvFS IO retry cases, skip the IO setup and go to the disk strategy
     * call. The caller is retrying an IO request that previously completed
     * but reported an IO error. 
     * The presumption is that the caller is passing in a previously
     * initialized buf structure, ioanchor_t, adviodesc_t and any bsBuf.
     * However, certain buf structure fields will be restored here 
     * using info stored in the adviodesc_t by the original IO request
     * specifically for retrying IO requests. The drivers may have changed
     * these buf fields during the IO.
     */

    if (flags & ADVIOFLG_RETRYIO) {
        iodescp = (adviodesc_t *)bp->b_private;
        MS_SMP_ASSERT(iodescp && (iodescp->advio_magicid == ADVIODESCMAGIC));
        bp->b_un.b_addr = iodescp->advio_targetaddr;
        bp->b_offset = (bp->b_blkno<<DEV_BSHIFT); 
        bp->b_error = 0;
        bp->b_eei = 0;
        bp->b_drv_handle = 0;
        /* Set flag to indicate that driver must call filesystem iodone()
         * and not to retry IO failures forever.
         */
        BSETFLAGS(bp,(bufflags_t) (B_CALL | B_NDELAY));
        BRESETFLAGS(bp,(bufflags_t) (B_DONE | B_ERROR));
        goto ADVFS_RETRYIO_LABEL;
    }

    /* Increment the virtual disk's active IO counter that is useful for
     * debugging. Also update other AdvFS IO statistics.
     */
    if (!(flags & ADVIOFLG_FAKEIO) && (vdp)) {
        ADVFS_ATOMIC_FETCH_INCR(&vdp->vdIoOut, &dummy);
        
        /* Keep statistics on number of read and write requests and
         * number of DEV_BSIZE disk blocks read or written.
         */
        if (!(BGETFLAGS(bp) & B_READ) ) {
            ADVFS_ATOMIC_FETCH_INCR(&vdp->dStat.nwrite, &dummy);
            ADVFS_ATOMIC_FETCH_ADD(&vdp->dStat.writeblk,
                             (uint64_t)(bp->b_bcount/DEV_BSIZE),
                             &dummy);
        } else {
            ADVFS_ATOMIC_FETCH_INCR(&vdp->dStat.nread, &dummy);
            ADVFS_ATOMIC_FETCH_ADD(&vdp->dStat.readblk,
                             (uint64_t)(bp->b_bcount/DEV_BSIZE),
                             &dummy);
        }
    }

    /* Allocate and initialize an IO descriptor for this specific
     * IO request that holds information for the IO completion.
     * The allocation call will block waiting for memory and initialize
     * the magic identifier field, but will not zero the remaining structure.
     * Initialize all other IO descriptor fields below.
     */
    iodescp = advfs_bs_get_adviodesc(TRUE);
    MS_SMP_ASSERT(iodescp != NULL);
    iodescp->advio_bfaccess = bfap;

    /* Fake I/O may not have a vdp */
    iodescp->advio_blkdesc.vdIndex = (vdp) ? vdp->vdIndex : 0;     
    iodescp->advio_blkdesc.vdBlk = bp->b_blkno;
    iodescp->advio_bsbuf = bsbufp;
    iodescp->advio_flags = flags;
    iodescp->advio_ioanchor = ioanchorp;
    iodescp->advio_fwd = NULL;

    /* Initialize fields that IO retry uses. The targetaddr is used to reset 
     * the buf's address field. The foffset is the starting file byte offset.
     */
    iodescp->advio_retry_starttime = 0;
    iodescp->advio_ioretrycount = 0;
    iodescp->advio_targetaddr = bp->b_un.b_addr; 
    iodescp->advio_foffset = bp->b_foffset;

    /* Save the adviodesc_t into the buf for retrieval by advfs_iodone().*/
    bp->b_private = iodescp;
    
    /* Assign disk block aligned offset. */
    bp->b_offset = (bp->b_blkno<<DEV_BSHIFT);  
    bp->b_error = 0;
    bp->b_eei = 0;
    bp->b_filevp = bp->b_vp;

    /* Replace the caller's iodone() in the buf with AdvFS's advfs_iodone().
     * Save caller's iodone() in the adviodesc_t only when caller sets
     * the B_CALL flag to allow AdvFS's IO completion to call it. 
     * This allows AdvFS to complete multiple IO buf structure IO requests
     * associated with the same bsBuf structure.
     * UFC callers always have their own iodone but other callers may or
     * may not specify one.
     */
    iodescp->advio_save_iodone = (BGETFLAGS(bp) & B_CALL) ?
                                 bp->b_iodone : NULL;
    bp->b_iodone = (int (*)(struct buf *))advfs_iodone;

    /* Set flag to indicate that driver must call filesystem iodone()
     * and not to retry IO failures forever.
     */
    BSETFLAGS(bp,(bufflags_t)(B_CALL | B_NDELAY));

    /* Setup driver-specific information. */
    if (vdp) {
        bp->b_dev = vdp->devVp->v_rdev;
    }
    /* Priority IO scheduling.
     * Metadata and log file IO are always given the highest
     * priority so that the drivers attempt to process them ahead
     * of previously scheduled user data IO.
     * As a performance optimization, this should be fine as long
     * as the amount of metadata IO remains relatively low.
     *
     * All other IO requests are assigned the calling thread's assigned
     * process priority value.
     */
    if (bfap->dataSafety == BFD_USERDATA) {
        /* Retrieve the process's assigned priority value for all other IO. */
	bsetprio(bp);

        if ( ioanchorp->anchr_aio_bp != NULL ) {
            /* If the AIO request has supplied a lower priority (higher
             * numeric value), then use the AIO's value.  This comes from
             * the aiocb.aio_reqprio which is added to the process'
             * scheduling priority in the AIO code and passed via the AIO's
             * bp.b_prio field to advfs_aiostrategy().
             */
            if ( bp->b_prio < ioanchorp->anchr_aio_bp->b_prio ) {
                bp->b_prio = ioanchorp->anchr_aio_bp->b_prio;
            }
        }
    }
    else {
        /* Set highest priority for log and non-log metadata IO. */
        bp->b2_flags |= B2_FIRST;
        bp->b_prio = 0;
    }

ADVFS_RETRYIO_LABEL:
    /*
     * Call biodone() for domain panic or for a "fake" IO request to bypass
     * calling the disk strategy yet do proper AdvFS IO completion cleanup.
     * Screening domain panic has the following result: 
     *   read  - return failure
     *   write - fake success return without writing to disk.
     *   directIO read/write - return failure
     */
    if ((flags & ADVIOFLG_FAKEIO) || (vdp && vdp->dmnP->dmn_panic)) {
        if ((vdp && vdp->dmnP->dmn_panic &&
             (BGETFLAGS(bp) & (B_READ | B_PHYS)))) {
            /* Setup the IO request for domain panic processing by the IO
             * completion for normal or DirectIO reads.
             * Set the domain panic error flag in the iodesc structure for
             * advfs_iodone() to check and process a domain panic IO error.
             */
            BSETFLAGS(bp,(bufflags_t) B_ERROR);
            bp->b_error = EIO;
            iodescp->advio_flags |= ADVIOFLG_DMN_PANIC_IO;
        }
        biodone(bp);
    } else {
        /* Call the disk strategy routine to issue the IO. */    
        DEVSW(bdevsw, major(bp->b_dev), d_strategy, (bp), error, save);
    }
}

/*
 * advfs_bs_io_complete
 *
 * Perform final I/O completion on a buf structure.
 *
 * This function will unconditionally free the bsBuf structure(s) that
 * exist on the IO descriptor's ioList when the buf structure's
 * B_INVAL or B_FREE flags are set for metadata files writes only.
 *
 * SMP: Locks the bsBuf->bsb_lock and logDescT.dirtyBufLock or domainT.lsnLock
 *      and bfAccess->actRangeLock.
 */
void
advfs_bs_io_complete(struct buf *bufp)    /* in */
{
    adviodesc_t *iodescp;
    ioanchor_t *ioanchorp;
    struct bsBufHdr *lsnp;
    struct bsBuf *bsbufp, *prevbsbufp;
    struct buf *aio_bp;
    bfAccessT *bfap;
    domainT *dmnp;
    extern void resetfirstrec( domainT* dmnP );
    int32_t resetFirstRec;
    int32_t free_act_range = FALSE;

    iodescp = (adviodesc_t *) bufp->b_private;
    MS_SMP_ASSERT(iodescp && iodescp->advio_magicid == ADVIODESCMAGIC);
    ioanchorp = iodescp->advio_ioanchor;
    MS_SMP_ASSERT(ioanchorp && ioanchorp->anchr_magicid == IOANCHORMAGIC);

    /* Extract the bfAccessT pointer from the vnode. */
    bfap = VTOA(bufp->b_vp);

    /* Process write IO completions */
    if ( !(BGETFLAGS(bufp) & B_READ) ) {    
        /* Handle log file write IO. */
        if (bfap->dataSafety == BFD_LOG) {
            logDescT *ldp = bfap->dmnP->ftxLogP;

            /* Process single or multiple bsBuf's linked into a single
             * buf structure IO request. Walk the bsBuf's linked together
             * by the ioListFwd link. 
             */
            bsbufp = iodescp->advio_bsbuf;
            while (bsbufp) {
                /* Do the domain log's dirtyBufList processing only if
                 * the bsBuf is on it which is true when get_clean_pg()
                 * inserts a bsBuf onto the dirtyBufList after calling
                 * bs_pinpg(). There are paths calling bs_pinpg() other than
                 * get_clean_pg(). Those paths purposely do not want the log
                 * dirtyBufList management. 
                 */
                if (bsbufp->bsb_metafwd) {
                    spin_lock(&ldp->dirtyBufLock); 
                    /* Update the log's highest flushed LSN if the current
                     * bsBuf is at the head of the dirtyBufList. This indicates
                     * that all log pages with LSN's lower than this have 
                     * already completed their IO. 
                     */
                    if (bsbufp == ldp->dirtyBufList.bsb_metafwd) {
                        ldp->hiFlushLsn = bsbufp->bsb_flushseq;
                    }
                    else {
                        /* The current bsBuf's LSN gets assigned to the
                         * previous bsBuf on the log's dirtyBuf list because
                         * IO's can complete out of order and we want to
			 * ensure flush synchronization wakeup keeps track
                         * of the potentially highest LSN to wait on.
                         * So if the current bsBuf is the last in the ordered
                         * list a flush will be waiting on, then we want to
                         * wakeup flush waiters only after all of the previous
                         * bsBuf's in the list with lower LSN's are also 
                         * flushed.
                         */
                        MS_SMP_ASSERT(LSN_GT(bsbufp->bsb_flushseq,
                                           bsbufp->bsb_metabwd->bsb_flushseq));
                        bsbufp->bsb_metabwd->bsb_flushseq = 
                                           bsbufp->bsb_flushseq;
                    }
                    /* Remove the bsBuf from the log's dirtyBufList and clear
                     * the bsBuf's flushseq value. 
                     * Have macro decrement the list length.
                     */
                    ADVFS_REM_METABUF_LIST(bsbufp, &ldp->dirtyBufList, TRUE);
                    bsbufp->bsb_flushseq = nilLSN;
                    spin_unlock(&ldp->dirtyBufLock); 
                } 

                spin_lock(&bsbufp->bsb_lock);

                /* Wake up multiWait waiters on this log bsBuf.
                 * The bsBuf's bsb_lock must be held while calling the 
                 * wakeup routine. 
                 */
                advfs_multiWait_wakeup(bsbufp);

                /* Get next bsBuf in the list and remove the previous bsBuf
                 * from the list. The links are protected by the page cache
                 * page locks associated with the current bsBuf.
                 */
                prevbsbufp = bsbufp;
                bsbufp = bsbufp->bsb_iolistfwd;
                prevbsbufp->bsb_iolistfwd = NULL;
                spin_unlock(&prevbsbufp->bsb_lock);

                /* If servicing an invalidation or free request on a
                 * metadata file, then unconditionally free the bsBuf
                 * structure that we previously worked on.
                 */
                if (BGETFLAGS(bufp) & ((bufflags_t)(B_INVAL | B_FREE))) {

                    GPLOGIT(IO_COMPLETION_LOG_PAGE,
                            ATOV(bfap),
                            prevbsbufp->bsb_foffset,
                            bfap->bfPageSz*ADVFS_FOB_SZ,
                            0,
                            0,
                            0,
                            prevbsbufp,
                            REMOVING_BSBUF);

                    (void) advfs_remove_bsbuf(ATOV(bfap),
                                              prevbsbufp->bsb_foffset,
                                              bfap->bfPageSz*ADVFS_FOB_SZ,
                                              NULL);
                } else {
                    GPLOGIT(IO_COMPLETION_LOG_PAGE,
                            ATOV(bfap),
                            prevbsbufp->bsb_foffset,
                            bfap->bfPageSz*ADVFS_FOB_SZ,
                            0,
                            0,
                            0,
                            prevbsbufp,
                            IO_FINISHED);
                }
            } /* End of while bsbuf exists loop */
        } /* end if log file */
        else {
            /* Handle non-log metadata file write IO. */
            if (bfap->dataSafety == BFD_METADATA) {
	        logRecAddrT reclsn;
                struct bsBuf *nxtbsbufp;
                /*  
                 * Process single or multiple bsBuf's linked into a single
                 * buf structure IO request. Walk the bsBuf's linked together
                 * by the ioListFwd link.
                 */
                dmnp =  bfap->dmnP;
                bsbufp = iodescp->advio_bsbuf;
                while (bsbufp) {
                    /* If a metadata bsBuf is under transaction control, 
                     * then do domain lsnList processing.
                     *
                     * advfs_bs_dmn_flush_meta() allows only one lsnList
                     * list marker while a flush is in progress.
                     *
                     * If bsBuf is at the head of domain's lsnList or just
                     * an lsnList bsBuf marker is between the domain lsnList
                     * head and the current bsBuf, then the oldest 
                     * outstanding LSN changed. In this case, remove the bsBuf
                     * from the head of the lsnList and reset the oldest
                     * dirty buffer log address before waking up waiters.
                     * There is a strict dependency that the processing of
                     * of the domain lsnList processing and removal of the
                     * bsBuf from the list happen prior to calling the 
                     * multiWait wakeup.  advfs_bs_dmn_flush_meta() 
                     * requires this ordering to avoid races with inserting 
                     * a new multiWait link onto the bsBuf and this 
                     * IO completion removing the bsBuf and waking waiters.
                     */
                    resetFirstRec = FALSE;

                    if (bsbufp->bsb_metafwd) {
                            spin_lock(&dmnp->lsnLock);                  
                            lsnp = &dmnp->lsnList;
                            if ((lsnp->bsb_metafwd == bsbufp) || 
                                ((lsnp->bsb_metafwd->bsb_flags &
                                  BSBUFFLG_LSNLISTMARKER) &&
                                (lsnp->bsb_metafwd ==
                                 bsbufp->bsb_metabwd))) {
                                /* Set the oldest dirty buffer log record
                                 * address before waking up multiWait sync
                                 * waiters.
                                 * The following skips over any list marker
                                 * on the lsnList following the current bsBuf.
                                 * Note that lsnList length does not include
                                 * list marker bsbuf's.
                                 */
                                reclsn = logNilRecord;
                                nxtbsbufp = bsbufp->bsb_metafwd;
                                if ((lsnp->bsbh_length > 1) &&
                                    (nxtbsbufp != NULL)) {
                                  if (nxtbsbufp->bsb_flags & 
                                      BSBUFFLG_LSNLISTMARKER) {
				    if (nxtbsbufp->bsb_metafwd) {
                                      reclsn = 
                                        nxtbsbufp->bsb_metafwd->bsb_origlogrec;
                                    }
                                  }
                                  else
                                    reclsn = nxtbsbufp->bsb_origlogrec;
				}
                                ftx_set_dirtybufla(dmnp, reclsn);

                                /* The metadata for the oldest log entry just
                                 * got written, so a new oldest log entry
                                 * needs to be recalculated.
                                 */
                                resetFirstRec = TRUE;
                            }
                            /* Remove the bsBuf from the domain's lsnList.
                             * Have macro decrement the list length.
                             * Reset the bsb_flushseq and bsb_firstlsn fields.
                             */

                            MS_SMP_ASSERT(LSN_LTE(
                                            bsbufp->bsb_flushseq,
                                            bfap->dmnP->ftxLogP->hiFlushLsn) ||
                                          bfap->dmnP->dmn_panic);

                            ADVFS_REM_METABUF_LIST(bsbufp, lsnp, TRUE);

                            bsbufp->bsb_flushseq = nilLSN;
                            bsbufp->bsb_firstlsn = nilLSN;                  
          
                            /* Recalculate the new oldest active log entry.
                             * This is only done when servicing IO sent to the
                             * driver and not for a "fake" IO.
                             */
                            if (resetFirstRec &&
                                !(iodescp->advio_flags & ADVIOFLG_FAKEIO))
                                resetfirstrec(dmnp);
                            spin_unlock(&dmnp->lsnLock);
                    }

                    spin_lock(&bsbufp->bsb_lock);

                    /*
                     * Clear addresses of log recs associated with changes
                     * to this buffer. Other code depends on the fact that
                     * bsb_origlogrec does not change while the buffer is on
                     * the lsnList.
                     */
                    bsbufp->bsb_origlogrec.lsn = nilLSN;
                    bsbufp->bsb_currentlogrec.lsn = nilLSN;

                    /* Wake up multiWait waiters on this metadata bsBuf.
                     * The bsBuf's bsb_lock must be held while calling the 
                     * wakeup routine. 
                     */
                    advfs_multiWait_wakeup(bsbufp);

                    /* Get next bsBuf in the list and remove the previous bsBuf
                     * from the list. The links are protected by the page cache
                     * page locks associated with the current bsBuf.
                     */
                    prevbsbufp = bsbufp;
                    bsbufp = bsbufp->bsb_iolistfwd;
                    prevbsbufp->bsb_iolistfwd = NULL;
                    spin_unlock(&prevbsbufp->bsb_lock);

                    /* If servicing an invalidation or free request on
                     * a metadata file, then remove the bsBuf from the
                     * hashtable and free the bsBuf structure we
                     * previously worked on.
                     */
                if (BGETFLAGS(bufp) & ((bufflags_t)(B_INVAL | B_FREE))) {

                        GPLOGIT(IO_COMPLETION_META_PAGE,
                                ATOV(bfap),
                                prevbsbufp->bsb_foffset,
                                bfap->bfPageSz*ADVFS_FOB_SZ,
                                0,
                                0,
                                0,
                                prevbsbufp,
                                REMOVING_BSBUF);

                        (void)advfs_remove_bsbuf(ATOV(bfap),
                                                 prevbsbufp->bsb_foffset,
                                                 bfap->bfPageSz*ADVFS_FOB_SZ,
                                                 NULL);
                        
                    } else {
                        GPLOGIT(IO_COMPLETION_META_PAGE,
                                ATOV(bfap),
                                prevbsbufp->bsb_foffset,
                                bfap->bfPageSz*ADVFS_FOB_SZ,
                                0,
                                0,
                                0,
                                prevbsbufp,
                                IO_FINISHED);
                    }
                        
                } /* End of while bsbuf exists loop */
            } /* end if non-log metadata file */
        } /* end else if */
    } /* end if write IO */

    if ( !(ioanchorp->anchr_flags & IOANCHORFLG_DIRECTIO) ) {
        /* Call the saved iodone routine if it exists. 
         * Pass the original buf pointer (anchr_origbuf) if it exists
         * to the saved iodone. This is typically for UFC IO. However,
         * other callers like advfs_strategy() will have the current
         * buf pointer passed into the saved iodone instead.
         * Also restore the saved iodone back into the buf structure.
         * AdvFS always saves the UFC iodone call for last in order
         * to process our own IO completion routine first.
         * This must be the last operation because UFC iodone may
         * free the buf structure.
         * This must pass the original buf structure saved in the
         * the ioanchor by the advfs_bs_startio() caller.
         * ASSUMPTION:
         * Active range lock processing applies only to DirectIO and
         * is unnecessary on UFC IO.
         */
        if (iodescp->advio_save_iodone) {
            bufp->b_iodone = iodescp->advio_save_iodone;
            if (ioanchorp->anchr_origbuf) 
                (*iodescp->advio_save_iodone)(ioanchorp->anchr_origbuf);
            else
                (*iodescp->advio_save_iodone)(bufp);
        }
        else {
	    /* Process a traditional buffer buf IO request originating
             * from advfs_strategy() only. This executes only when 
             * no advio_save_iodone exists to wakeup waiters.
             */
            if (ioanchorp->anchr_flags & IOANCHORFLG_ADVFS_STRATEGY_IO) {
                finish_biodone(bufp);
            }
        }
    }
    else {
        /* For all DIO completions, figure out whether the IO was
         * successful.  These are all started assuming multi-part IOs.
         */
        off_t err_bytes_transferred = 0;

        if ( ioanchorp->anchr_io_status != 0 ) {
            /* The I/O did not complete successfully. Calculate the
             * number of bytes that were successfully transferred so
             * this can be reported back to the application.
             */
            err_bytes_transferred = ioanchorp->anchr_min_err_offset -
                                    ioanchorp->anchr_min_req_offset;
            ioanchorp->anchr_min_err_offset = err_bytes_transferred;
        }

        /* Cleanup for AIO-generated I/Os for directIO files. */
        aio_bp = ioanchorp->anchr_aio_bp;
        if (aio_bp) {
            anchr_aio_t *aio_infop = &ioanchorp->anchr_aio_info;

            MS_SMP_ASSERT( aio_infop->anchr_actrangep );
            MS_SMP_ASSERT( aio_infop->anchr_lock_key );
            MS_SMP_ASSERT( aio_infop->anchr_aio_address );
            MS_SMP_ASSERT( bfap == iodescp->advio_bfaccess );

            /* Adjust the value of aio_bp->b_resid for the number of bytes
             * transferred for this ioanchor set of IOs.
             */
            if ( ioanchorp->anchr_io_status ) {
                /* An error occurred on at least one bufp */
                if ( err_bytes_transferred == 0 ) {
                    /*
                     * If and only if, we did not transfer any data, do we
                     * return the error detected.
                     */
                    aio_bp->b_error = ioanchorp->anchr_io_status;
                    BSETFLAGS(aio_bp,(bufflags_t)B_ERROR);
                }
                aio_bp->b_resid -= err_bytes_transferred;
            } else {
                aio_bp->b_resid -= aio_infop->anchr_aio_iosize;
            }

            /* Now invoke aio completion routine; may deallocate the aio_bp.
             * Eventually we need to do this conditionally on the last
             * ioanchor generated for this AIO request. This can happen
             * if advfs_fs_write() calls into advfs_fs_write_direct()
             * several times to complete a single IO request.
             */
            aio_bp->b_iodone(aio_bp);

            /* Cleanup the active range */
            spin_lock( &bfap->actRangeLock );
            remove_actRange_from_list( bfap, aio_infop->anchr_actrangep );
            spin_unlock( &bfap->actRangeLock );
            advfs_bs_free_actRange( aio_infop->anchr_actrangep, TRUE );
            aio_infop->anchr_actrangep = NULL;

            /* Claim and release the cacheModeLock */
            ADVRWL_BFAPCACHEMODE_CLAIM( bfap, aio_infop->anchr_lock_key );
            ADVRWL_BFAPCACHEMODE_UNLOCK(bfap);

            /* Now unwire the application pages (if an only if the buffer
             * was in user space; kernel space buffers are not locked).
             */
            if ( aio_infop->anchr_aio_flags & ANCHR_AIO_BUFPIN ) {
                bufunpin( aio_infop->anchr_aio_sid,
                          aio_infop->anchr_aio_address,
                          aio_infop->anchr_aio_size,
                          (BGETFLAGS(bufp) & B_READ) ? B_READ : B_WRITE );
            } 
            else if ( aio_infop->anchr_aio_flags & ANCHR_AIO_VASLOCKPAGES ) {
                vasunlockpages( aio_infop->anchr_aio_address,
                                aio_infop->anchr_aio_size, 0,
                                aio_infop->anchr_aio_sid, NULL,
                                (BGETFLAGS(bufp) & B_READ) ? B_READ : B_WRITE );
            }
        }
    }
}
/*
 * advfs_cfs_flush_and_invalidate
 *
 * This function flushes and optionally invalidates all
 * dirty data for an AdvFS file.  
 * 
 * Note: This routine exists for CFS only.
 *       Callers must guarantee that no racing threads will
 *       create new dirty data since this routine will not
 *       tolerate new dirty UFC pages after the flush.
 */

void
advfs_cfs_flush_and_invalidate(
                          struct vnode *vp,
                          int flags)
{
    struct actRange *arp = NULL;
    bfAccessT *bfap = ((struct bfNode *)(vp->v_data))->accessp;
    int err;
    /*
     * There are two conditions under which we should do no work here:
     *
     * 1. The vp has a shadow access structure.
     * 2. The vp is for a quota file.
     *
     * Here are some more in-depth notes on why these cases are to
     * be handled specially:
     * 
     * When accessing metadata from the .tags directory, a shadow access
     * structure is created and real_bfap points to the access structure
     * in the root bfSet.  If we are in this function with a vnode that
     * has a shadow access structure, it should only be because we are
     * recycling the vnode.  In that case, there is no need to flush
     * or invalidate any pages because the real_bfap continues
     * to be active.
     * 
     * Disallow flush and invalidation on metadata-controlled
     * quota.user and quota.group files.
     * This avoids having CFS directIO open requests call this
     * routine causing memory corruption due to dirty buffer
     * cache being prematurely freed. CFS cannot detect updates
     * to quota files. Those modifications would get lost
     * during the invalidation but AdvFS would still have
     * references to them on various buffer lists.
     */
    if ((BS_BFTAG_PSEUDO(bfap->tag)) || 
        ((bfap->tag.tag_num == USER_QUOTA_FILE_TAG) ||
          (bfap->tag.tag_num == GROUP_QUOTA_FILE_TAG))) {
        return;
    }

    /* 
     * It has been shown that CFS can call here with a migrate running
     * in the kernel so we need to lock them out to prevent corruption.
     * Seize an active range for the whole file.  It is really only the
     * invalidation that is critical to coordinate with migrate, but we
     * might as well prevent any unneccessary flushing too.
     */
    if (!flags & AFI_NO_INVALIDATE) {
        arp = advfs_bs_get_actRange(TRUE);
        MS_SMP_ASSERT(arp);
        arp->arEndFob = ~0L;       /* biggest possible block */
        arp->arState = AR_INVALIDATE;
        arp->arDebug = SET_LINE_AND_THREAD(__LINE__);
        (void)insert_actRange_onto_list( bfap, arp, NULL );
    }

    /* 
     * Flush all dirty data in the file to disk, optionally invalidating
     * the in-memory pages.
     */
    err = fcache_vn_flush(vp, 0, 0, NULL, FVF_WRITE | FVF_SYNC |
                          ((flags & AFI_NO_INVALIDATE) ? 0 : FVF_INVAL));
    MS_SMP_ASSERT(err == 0);

    /* Update file statistics and flush the log. The log flush ensures
     * we flush any storage allocations to disk.  This is necessary because
     * CFS assumes that if the block is allocated to this file, 
     * it won't be removed on a subsequent crash and recovery. 
     */
    if (bfap->bfFsContext.dirty_stats || bfap->bfFsContext.dirty_alloc) {
        (void)fs_update_stats(vp, bfap, FtxNilFtxH, 0);
        advfs_lgr_flush(bfap->dmnP->ftxLogP, nilLSN, FALSE);        
    }

    if (arp) {
        ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( bfap, arp );
        spin_lock(&bfap->actRangeLock);
        remove_actRange_from_list(bfap, arp);   
        spin_unlock(&bfap->actRangeLock);
        advfs_bs_free_actRange(arp, TRUE);
    }
}

    
/*
 * advfs_bs_bfs_flush
 * 
 * Flush all the dirty user data associated with a fileset.
 * Optionally wait for the flush, invalidate the in-memory pages,
 * and update the file statistics.
 *
 * If caller simply wants to start a fileset flush, one pass is
 * made through the chain of access structures for the fileset,
 * kicking off, but not waiting for, a flush on each file.
 *
 * If caller wants to wait for the fileset flush, a second pass
 * is made through the access structures to synchronize on the
 * flushes and, optionally, invalidate the in-memory pages.  This
 * algorithm is used in preference to a one-pass algorithm that
 * would synchronously flush each file because it will probably
 * be faster to launch lots of I/Os and then wait for them rather
 * than launching a small number of I/Os, waiting for them, 
 * launching some more, waiting for them, etc.
 *
 * nowait flag - 
 * indicates whether or not to call the UFC flush routine
 * so that it does not wait on locks held. Only advfs_sync() servicing
 * a panic shutdown should set the flag TRUE. All others set flag to FALSE.
 *
 * Synchronization
 *
 * The caller should make sure that the domain and fileset do not go
 * away.  This is done via the domain's bfSetTblLock and the
 * domain hashtable lock. This routine can support concurrent flushes
 * on the same fileset using list markers.  This routine also synchronizes
 * with the EBUSY check in advfs_unmount() and access_invalidate() via
 * ACCMAGIC_MARKER.
 */

void
advfs_bs_bfs_flush(bfSetT *bfSetp,      /*in - Fileset to be flushed */
                   int32_t invalidate,  /*in - If TRUE, invalidate pages */
                   flushFlagsT flags)   /*in - flags */
{
    bfAccessT  *currbfap,           /* File currently being flushed */
               *bfap_list_marker,
               *start_marker;       /* We use this to keep track of where
                                     * we are in the fileset access list 
                                     */
    fvf_flags_t flush_flags;        /* flags for UFC flush */
    int        first_pass = TRUE;   /* First of possibly two passes */
    int        err;
    ftxHT      ftxH;                /* Transaction handle for flushes */
    int        ftx_started;         /* Flag for transaction slot taken out */
    statusT    sts;

    /*
     * Cannot specify both invalidate and async or invalidate and nowait.
     */
    MS_SMP_ASSERT(!(invalidate && (flags & FLUSH_ASYNC)));
    MS_SMP_ASSERT(!(invalidate && (flags & FLUSH_NOWAIT)));
    MS_SMP_ASSERT(BFSET_VALID(bfSetp));

    /* Make nowait take precedence over synchronous requests. */
    if ((flags & FLUSH_NOWAIT) & !(flags & FLUSH_ASYNC))  {
        flags |= FLUSH_ASYNC;
    }

    /* Make sure that the bfset table lock is held */
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_NEQL(&bfSetp->dmnP->BfSetTblLock.lock.rw,
                RWL_UNLOCKED) || (flags & FLUSH_UPDATE_STATS) || 
            (BS_BFTAG_EQL( bfSetp->dirTag, staticRootTagDirTag) ) );
                
    /* Setup static UFC flush flags. Other flags are set when calling
     * the fcache_vn_flush().
     */
    flush_flags = FVF_WRITE |
                  (invalidate ? FVF_INVAL : 0) |
                  ((flags & FLUSH_NOWAIT) ? FVF_NOWAIT : 0);
    bfap_list_marker = (bfAccessT *)ms_malloc(sizeof(bfAccessT));
    bfap_list_marker->accMagic = ACCMAGIC_MARKER;
    bfap_list_marker->bfSetp = bfSetp;   

    start_marker = (bfAccessT *)ms_malloc(sizeof(bfAccessT));
    start_marker->accMagic = ACCMAGIC_MARKER;
    start_marker->bfSetp = bfSetp;   

    /* 
     * NOTE about markers.  Currently they are access structures, but
     * none of the locks are initialized.  This was done so we could use
     * a smaller marker, in the future, with just the relevant fields for 
     * access chain processing. Any other routine that traverses the 
     * access fileset chain may encounter these markers and must check the 
     * accMagic field before trying to lock the bfaLock.
     */

    /*
     * Add the start marker at the head of the list and insert the 
     * bfap_list_marker after it.
     */
    ADD_ACC_SETLIST(start_marker, TRUE, FALSE);
    INS_ACC_SETLIST(bfap_list_marker, start_marker, TRUE);
    /*
     * Indicate that we don't have a transaction.
     */
    ftx_started = FALSE;
    /*
     * Walk down the chain of access structures for the fileset.
     */
    while (1) {
        /*
         * Start a transaction. We need to do this before looking at
         * the bfap list so that we can take out the flush lock inside
         * the transaction.
         */
        if( (flags & FLUSH_UPDATE_STATS) && (!ftx_started) ) {
            sts = FTX_START_N( FTA_BS_BMT_UPDATE_REC_V1,
                               &ftxH, FtxNilFtxH,
                               bfSetp->dmnP);
            if (sts != EOK) {
                domain_panic(bfSetp->dmnP,
                             "bs_bfs_flush: can't start transaction"
                             "- sts %d", sts);
                break;
            }
            else
                ftx_started = TRUE;
        }
        /*
         * Take out the access chain lock. This protects the next bfap
         * after the marker from going away while we get (attempt to
         * get) the flush lock.
         */
        ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock);
        /*
         * Exit if marker is past the last bfap in the chain.
         */
        if (bfap_list_marker->setFwd == (bfAccessT *)(&bfSetp->accessFwd)) {
            ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);
            break;
        }

        currbfap = bfap_list_marker->setFwd;

        /* 
         * If the currbfap is a list marker, then skip it.
         *
         * If the bfap is a log file, we don't want to flush it through a
         * fileset flush.  The log should be flushed through advfs_lgr_flush
         * and in recovery, flushing the log this way could cause a hang if
         * the last log page still has a writeRef on it.
         */
        if ((currbfap->accMagic == ACCMAGIC_MARKER) ||
            (currbfap->dataSafety == BFD_LOG)) {
            goto _move_marker;
        }

        /*
         * We are ready to flush this access structure. Obtain the
         * flush lock to protect the vnode from going away. This lock
         * will synchronize with the deallocation or recycling of an
         * access structure. The lock will be obtained for write
         * before destroying the vnode.
         */
    
        if (!ADVRWL_BFAP_FLUSH_READ_TRY(currbfap)) {
            /* The only contention for this lock is when the access
             * structure is being deallocatedor recycled. We know
             * therefore that there is no dirty data to flush so we
             * can just skip it.
             */
            ACCESS_STATS( bfs_flush_skipped_due_to_flush_lock_try );
            goto _move_marker;
        }
        ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);

        /*
         * If the caller wanted stats to be updated and there are dirty
         * stats to update, do the update for this file now if this is
         * an asynchronous flush or if it is a synchronous flush and 
         * we're on the second pass.
         */
        if ((flags & FLUSH_UPDATE_STATS) && VTOC(&currbfap->bfVnode) && 
                VTOC(&currbfap->bfVnode)->dirty_stats == TRUE &&
                ((flags & FLUSH_ASYNC) || !first_pass)) {
            (void)fs_update_stats( &currbfap->bfVnode,
                                    currbfap,
                                    ftxH,
                                    ADVFS_UPDATE_FINISH_FTX );
            ftx_started = FALSE; /* fs_update_stats will finish the ftx */
        } else {
            if( ftx_started ) {
                ftx_fail(ftxH);
                ftx_started = FALSE;
            }
        }
        /*
         * Only attempt to flush pages if we are not in a domain panic.  
         */
        if (!bfSetp->dmnP->dmn_panic) {
            /*
             * Flush all dirty data for this file
             * unless we're updating file stats only.
             */
            if (!(flags & FLUSH_STATS_ONLY)) {
                err = fcache_vn_flush(&currbfap->bfVnode, 0, 0, NULL, 
                                  flush_flags |
                                  (first_pass ? FVF_ASYNC : FVF_SYNC));
                MS_SMP_ASSERT((err == 0) || (err == EWOULDBLOCK) || 
                          bfSetp->dmnP->dmn_panic);
            }
        }
        /*
         * Invalidate the pages if we are in a domain panic.
         */
        if (bfSetp->dmnP->dmn_panic) {
            err = fcache_vn_invalidate(&currbfap->bfVnode, 0, 0, NULL, 
                                  (BS_IS_META(currbfap)?FVI_PPAGE:0)|FVI_INVAL);
            MS_SMP_ASSERT(err == 0);
        }

        ADVRWL_BFAP_FLUSH_UNLOCK(currbfap);
        ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock);
_move_marker:
        /* 
         * Move the list marker only if the list marker still points to 
         * currbfap as the next access structure in the list.
         * Otherwise we will use this different access structure for
         * processing in the next iteration of the loop.
         */
        if (bfap_list_marker->setFwd == currbfap) {
            /* First remove the marker from the list */
            RM_ACC_SETLIST(bfap_list_marker, FALSE);

            /* Now add the marker after currbfap (to the right) */
            INS_ACC_SETLIST(bfap_list_marker, currbfap, FALSE);
        }

        /*
         * If we're at the end of the list and we need to make
         * a second pass in order to synchronize on the file flushes, 
         * Move bfap_list_marker to where we initially started.
         */
        if ((bfap_list_marker->setFwd == (bfAccessT *)(&bfSetp->accessFwd)) && 
            (!(flags & FLUSH_ASYNC)) && (first_pass == TRUE)) {
            /* 
             * First remove the marker from the list 
             */
            RM_ACC_SETLIST(bfap_list_marker, FALSE);

            /* Now insert the marker after the start marker */
            INS_ACC_SETLIST(bfap_list_marker, start_marker, FALSE);
            first_pass = FALSE;
        }
        /*
         * We need to release this lock before the call to bs_close() 
         * otherwise we might run into a deadlock if the close needs
         * to flush any dirty data. Our place in the chain is saved by 
         * bfap_list_marker.
         */
        ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);

    }
    if( ftx_started ) ftx_fail(ftxH);

    RM_ACC_SETLIST(bfap_list_marker, TRUE);
    RM_ACC_SETLIST(start_marker, TRUE);
    ms_free(bfap_list_marker);
    ms_free(start_marker);
}

/* 
 * advfs_bs_dmn_flush_meta
 * 
 * This function will flush to disk the metadata buffers on the domain's
 * LSN list.  It will use a multiWaitT to tie together the I/O's for
 * the various metadata files with buffers on the LSN list.  This is
 * a performance optimization over the simpler algorithm of just calling
 * a synchronous flush on each metadata page on the LSN list.
 *
 * NOTE:  If a metadata page has been dirtied in a non-transactional
 *        code path (such as during early domain initialization), it
 *        will not be flushed by this function since this function only
 *        flushes dirty pages on the LSN list.
 *
 * nowait flag - 
 * indicates whether or not to call the UFC flush routine
 * so that it does not wait on locks held. Only advfs_sync() servicing
 * a panic shutdown should set the flag TRUE. All others set flag to FALSE.
 *
 * Synchronization:
 *
 * This function is single-threaded using the domainT.metaFlushLock.
 * The domainT.lsnLock is held when manipulating the domain's LSN list.
 * The bsBuf.bsb_lock is held when attaching multiWaitLinkT structures
 * to the bsBuf.  The bfAccessT.bfaLock is held while manipulating a
 * particular access structure.  And the mw_lock is held while 
 * incrementing the mw_outstandingEvents in the multiWaitT.
 * 
 * It was decided to single-thread this function since there is no known
 * need to make it multi-threaded currently and because all of the potential
 * algorithms for making it multi-threaded suffered from some drawback.
 * Here are some alternatives that were considered:
 *
 * 1.  Make one walk backwards through the list, issuing synchronous calls
 * to fcache_vn_flush() on each dirty bsBuf.  This simple algorithm allows
 * for multi-threading but the single-threaded performance is far from
 * optimal since only one I/O can be queued up at a time.
 *
 * 2.   Use a potentially large amount of malloced memory to make a replica
 * or representation of the lsnList.  Then, unlock the lsnList, allowing it
 * to change, and use the representation, which doesn't need to be locked,
 * to decide what calls to fcache_vn_flush() need to be issued.  This could
 * use a very large amount of memory and is suboptimal since it may cause
 * lots of flush calls to be issued on already-flushed pages.  That is, it
 * becomes out-of-date with the current state of the lsnList.
 *
 * 3.   Allow multiple flush threads to insert list marker bsBufs onto
 * the list.  This allows each thread to be able to keep track of where it
 * was in the list after a call to fcache_vn_flush().  But it causes the I/O
 * completion path to process potentially unbounded chains of contiguous
 * list marker bsBufs in an interrupt context.
 *
 * 4.   Add a new field to the bsBuf, which would represent a hiFlushLsn for
 * metadata in the domain.  A flushing thread would remember the hiFlushLsn
 * of the last call it made to fcache_vn_flush() and would walk the list to
 * find the next bsBuf to flush.  This allows multi-threading without
 * impacting the I/O completion but it is also an O(N^2) algorithm for the
 * flushing thread.
 *
 * The only potential algorithm which would allow multi-threading, one pass
 * through the lsnList, and which would not burden the I/O completion with
 * excessive processing would be to have a flush thread walk the list one
 * time, sending messages to another I/O thread, asking it to do the I/O.
 * The multiWaitT mechanism could still be used for synchronizing
 * on the flush.  It's possible that the other thread could be spontaneously
 * created and destroyed by the metadata flush function.   The overhead of
 * this approach makes it unattractive unless a real need for a
 * multi-threaded domain-wide metadata flush is found.  At the present time,
 * no such need is foreseen.
 *
 * If the domain is in recovery (not BFD_ACTIVATED) then this routine will
 * directly flush the root bitfile set in addition walk the domain lsnList.  
 * This is because during record redo, we pin metadata pages without lsns 
 * so they will not be on the lsnList.
 */

/*
 * Stats for unusual events.
 */

void
advfs_bs_dmn_flush_meta(domainT *dmnp,     /* in - Domain to flush */
                        flushFlagsT flags) /* in - flags */
{
    struct bsBuf *list_marker_bp = NULL, /* Pseudo-bsBuf: marks place in list*/
                 *bp;                    /* Current bsBuf being processed */
    bfAccessT *bfap;                     /* Access structure of current bsBuf*/
    multiWaitT *mwp = NULL;              /* Flush-wide multiWait structure */
    multiWaitLinkT *mwlp = NULL;         /* Current multiWait link */
    int sts;                             /* Status from fcache_vn_flush() */
    extern kmem_handle_t advfs_multiWait_arena;

    /* Make nowait take precedence over synchronous requests. */
    if ((flags & FLUSH_NOWAIT) && !(flags & FLUSH_ASYNC)) 
        flags |= FLUSH_ASYNC;

    if ( dmnp->state != BFD_ACTIVATED ) {
        /* Flush the root bitfile set to get pages without LSNs.*/
        advfs_bs_bfs_flush( dmnp->bfSetDirp, 
                FALSE, flags);

    }

    /*
     * If the caller wanted to perform a synchronous flush, allocate
     * a multiWaitT and the first multiWaitLinkT.
     */
    if (!(flags & FLUSH_ASYNC)) {
        mwp = kmem_arena_alloc(advfs_multiWait_arena, M_WAITOK);
        MS_SMP_ASSERT(mwp);
        mwp->mw_magicid &= ~MAGIC_DEALLOC;
        mwp->mw_outstandingEvents = 0;
        mwlp = (multiWaitLinkT *)ms_malloc(sizeof(multiWaitLinkT));
    }

    ADVMTX_METAFLUSH_LOCK(&dmnp->metaFlushLock);

    /*
     * Get a list marker bsBuf.
     */
    list_marker_bp = advfs_bs_get_bsbuf(TRUE);
    list_marker_bp->bsb_flags |= BSBUFFLG_LSNLISTMARKER;
    list_marker_bp->bsb_bfaccess = NULL; /* Wipe out any old bfap pointer */

    /*
     * Insert the list marker bsBuf at the end of the LSN list.
     */
    spin_lock(&dmnp->lsnLock);
    ADVFS_ADD_TAIL_METABUF_LIST(list_marker_bp, &dmnp->lsnList, FALSE);
    spin_unlock(&dmnp->lsnLock);

    /*
     * Synchronously flush the log so that the metadata flushed below
     * can, hopefully, be flushed without further log flushes.  If the
     * metadata pages to be flushed are modified again after this log
     * flush, then advfs_putpage() will realize this and will issue
     * new calls to advfs_lgr_flush() as necessary.
     */
    advfs_lgr_flush(dmnp->ftxLogP, nilLSN, FALSE);

    spin_lock(&dmnp->lsnLock);

#ifdef OSDEBUG
    /*
     * Don't flush metadata when running a crash/recovery test.
     */
    if (dmnp->crashTest) {
	goto error_exit;
    }
#endif

    /*
     * While there are bsBufs in between the list marker bsBuf and the
     * head of the LSN list, launch an asynchronous flush on each of them.
     */

    while (list_marker_bp->bsb_metabwd != (struct bsBuf *)(&dmnp->lsnList)) {

        bp = list_marker_bp->bsb_metabwd;
        bfap = bp->bsb_bfaccess;

        if (!ADVRWL_BFAP_FLUSH_READ_TRY(bfap) ){
            /* This can't happen. The bsBuf can not be on the dirty
             * buffer list if the access structure is being
             * deallocated or recycled (the only path that takes this
             * lock for write).
             */
            ADVFS_SAD0("advfs_bs_dmn_flush_meta: Unable to obtain flush lock");
        }

        /*
         * Move the list marker bsBuf from its current location in the
         * LSN list to a position just before the bsBuf currently on its
         * "left" in the list.
         */
        ADVFS_REM_METABUF_LIST(list_marker_bp, &dmnp->lsnList, FALSE);
        ADVFS_INSERT_METABUF_LIST(bp->bsb_metabwd, list_marker_bp, 
                                  &dmnp->lsnList, FALSE);

        /*
         * If the caller wants to wait for the flush, attach a 
         * multiWaitLinkT to this bsBuf and to the multiWaitT.
         */
        if (!(flags & FLUSH_ASYNC)) {
            spin_lock(&bp->bsb_lock);
            mwlp->mwl_fwd = (struct multiWaitLink *)bp->bsb_mwllist;
            mwlp->mwl_mw = mwp;
            bp->bsb_mwllist = mwlp;
            spin_lock(&mwp->mw_lock);
            mwp->mw_outstandingEvents++;
            spin_unlock(&mwp->mw_lock);
            spin_unlock(&bp->bsb_lock);
        }

        spin_unlock(&dmnp->lsnLock);

        /*
         * Kick off an asynchronous flush of this metadata buffer.
         * Instruct flush not to wait on locks based on nowait flag.
         */
        if (sts = fcache_vn_flush(&bfap->bfVnode, bp->bsb_foffset, 
                        bfap->bfPageSz * ADVFS_FOB_SZ, 
                        NULL,
                        FVF_WRITE | FVF_ASYNC | 
                        ((flags & FLUSH_NOWAIT) ? FVF_NOWAIT : 0))) 
        {
	    if (!((flags & FLUSH_NOWAIT) && (sts == E_WOULD_BLOCK)))  {
                domain_panic(dmnp,
                   "bs_dm_flush_meta: fcache_vn_flush failed; status = %d", 
                             sts);
                ADVRWL_BFAP_FLUSH_UNLOCK(bfap);
                spin_lock(&dmnp->lsnLock);
                goto error_exit;
            }
        }
        
        ADVRWL_BFAP_FLUSH_UNLOCK(bfap);
 
        if (!(flags & FLUSH_ASYNC)) {
            /*
             * Get a new multiWaitLinkT.  Do this while the lsnLock is
             * not held to minimize the time we keep it locked.
             */
            mwlp = (multiWaitLinkT *)ms_malloc(sizeof(multiWaitLinkT));
        }

        /* 
         * Relock the LSN list.
         */
        spin_lock(&dmnp->lsnLock);

    } /* end for loop */

error_exit:

    if (list_marker_bp) {
        ADVFS_REM_METABUF_LIST(list_marker_bp, &dmnp->lsnList, FALSE);
    }

    spin_unlock( &dmnp->lsnLock );

    /*
     * If caller wanted a synchronous flush, wait for all of the 
     * metadata buffers to be flushed to disk.
     */
    if (!(flags & FLUSH_ASYNC)) {
        spin_lock(&mwp->mw_lock);
        while (mwp->mw_outstandingEvents) {
            cv_wait(&mwp->mw_cv, &mwp->mw_lock, CV_SPIN, CV_DFLT_FLG);
        }
        spin_unlock(&mwp->mw_lock);
    }

    ADVMTX_METAFLUSH_UNLOCK(&dmnp->metaFlushLock);

    /* Free memory resources after releasing locks. */

    if (list_marker_bp) {
        advfs_bs_free_bsbuf(list_marker_bp, TRUE);
    }

    if (mwp) {
        mwp->mw_magicid |= MAGIC_DEALLOC;
        kmem_arena_free(mwp, M_WAITOK);
    }
    if (mwlp) {
        ms_free(mwlp);
    }

    return;
}


/* 
 * This function will flush the metadata first.  This is done before
 * flushing the user data as a precaution against having dirty metadata
 * writes get behind huge numbers of dirty user data buffers in the drivers.  
 * The idea is that we don't want to effectively stop all new transactions
 * for a while just because the log buffers and other critical metadata
 * buffers are waiting at the end of a long line of user data buffers to
 * be flushed.
 *
 * Next, it will loop through all the filesets, flushing each of them.
 * Finally, if the caller wanted the stats updated, it will flush the
 * metadata again since metadata will have been modified by the updates
 * of stats.  This last metadata flush should not contain much data to flush.
 *
 * Callers can specify whether or not they want to wait for the data flushing 
 * to complete, and whether or not they want to have all of the 
 * files' statistics updated.
 *
 * nowait flag - 
 * indicates whether or not flush routines are to call the UFC flush routine
 * so that it does not wait on locks held. Only advfs_sync() servicing
 * a panic shutdown should set the flag TRUE. All others set flag to FALSE.
 *
 * Synchronization
 *
 * The domain's bfSetTblLock is held to prevent the filesets from going away.
 * It is presumed that the caller is making sure that the domain
 * doesn't go away.
 *
 */

void
advfs_bs_dmn_flush(domainT *dmnp,       /* in - Domain to flush */
                   flushFlagsT flags)   /* in - flags */
{

    bfsQueueT *bfSet_queue_entry;     /* Current bfSet being flushed */
    bfSetT *bfSetp;                   /* The corresponding bfSet pointer */
    bfSetT *prevBfSetp;
    statusT sts = EOK;
    bfs_op_flags_t bfs_op_flag = BFS_OP_DEACTIVATED_OK;


    /*
     * If FLUSH_UPDATE_STATS is set, we need to not hold the bfSetTblLock
     * while calling advfs_bs_bfs_flush.  If the FLUSH_NO_TBL_LOCK is set,
     * we can't unlock the bfSetTbl lock... so make sure they aren't both
     * set.
     */
    MS_SMP_ASSERT( !((flags & FLUSH_NO_TBL_LOCK) &&
                    (flags & FLUSH_UPDATE_STATS)) );


    /* Make nowait take precedence over synchronous requests. */
    if ((flags & FLUSH_NOWAIT) && !(flags & FLUSH_ASYNC))  {
        flags |= FLUSH_ASYNC;
    }

    /* 
     * Flush out dirty metadata.
     */
    advfs_bs_dmn_flush_meta(dmnp, flags);

    /*
     * Loop through the filesets in the domain, flushing the
     * dirty user data in each one.
     */
    if (!(flags & FLUSH_NO_TBL_LOCK)) {
        ADVRWL_BFSETTBL_READ(dmnp);
    }

    bfSet_queue_entry = dmnp->bfSetHead.bfsQfwd;
    while ( bfSet_queue_entry != &dmnp->bfSetHead ) {
        
        bfSetp = BFSET_QUEUE_TO_BFSETP(bfSet_queue_entry);

        /*
         * There is a potential lock inversion between holding the bfSetTbl
	 * lock and flushing stats. The original solution to just drop the
	 * lock around the flush is insufficient because the fileset could
	 * disappear and leave us with a stale pointer. We need to ref the
	 * fileset.
         */

        if (flags & FLUSH_UPDATE_STATS) {
            /*
             * If we are going to update stats, we can't hold the bfSetTbl
             * lock since we need to start transactions.  In this case, we
             * will ref the fileset and drop the lock. It's less efficient
             * in some respects but things get done ever so much faster when
             * the system is hung.
             */
            ADVRWL_BFSETTBL_UNLOCK( dmnp );
            sts = bfs_access( &bfSetp,
                bfSetp->bfSetId,
                &bfs_op_flag,
                FtxNilFtxH );
        }
        /*
         * If FLUSH_UPDATE_STATS wasn't set, sts == EOK from initializaion.
         */
        if( sts == EOK ) {
            /*
             * Flush the fileset as long as it isnt' the root bitfile set 
             */
            if (!BS_BFTAG_EQL(bfSetp->dirTag, staticRootTagDirTag)) {
                advfs_bs_bfs_flush(bfSetp, FALSE, flags);
            }

            if (flags & FLUSH_UPDATE_STATS) {
                /* Reacquire the bfSetTbl lock and advance to the next fileset
                 */
                ADVRWL_BFSETTBL_READ( dmnp );
                bfs_close( bfSetp,
                           FtxNilFtxH ); 
            }
            bfSet_queue_entry = bfSet_queue_entry->bfsQfwd;
        }
    }

    if (!(flags & FLUSH_NO_TBL_LOCK)) {
        ADVRWL_BFSETTBL_UNLOCK(dmnp);
    }

    /*
     * If update of stats was requested, flush the now-changed metadata.
     */
    if (flags & FLUSH_UPDATE_STATS) {
        advfs_bs_dmn_flush_meta(dmnp, flags);
    }
}

/* 
 * The advfs_metaflush_msg_queue is defined for use by advfs_putpage when a
 * metaflush is required that would block.  This queue will be monitored by
 * multiple threads all trying to read the same queue.  There will be one
 * thread per CPU blocking on the same queue.  Anyone of the threads can
 * process any message off the queue.
 */
msgQHT advfs_metaflush_msg_queue = NULL;
#define ADVFS_METAFLUSH_QUEUE_SIZE 100

/*
 * BEGIN_IMS
 *
 * advfs_metaflush_thread - handles metadata flushes that could not be
 * completed inline by advfs_putpage. 
 *  
 * Parameters:
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *
 * Algorithm:
 *      Listens for a message on the message queue, then processes the
 *      message by calling fcache_vn_flush on the range of the file to be
 *      flushed.
 *
 * END_IMS
 */
void
advfs_metaflush_thread()
{
    advfs_metaflush_thread_msg_t* msg;
    int32_t done = FALSE;
    int sts = 0;
    bfSetT *bfSetp = NULL;      /* BfSet on which bfap resides */
    bfAccessT *bfap = NULL;     /* bfAccessT to be flushed */

    while (!done) {
        MS_SMP_ASSERT(advfs_metaflush_msg_queue != NULL);

        /*
         * Wait for a message.  There are a number of threads racing to get
         * a message off the msg queue.  Only one thread will get a single
         * message.
         */
        msg = (advfs_metaflush_thread_msg_t*)msgq_recv_msg( advfs_metaflush_msg_queue);

        switch ( msg->amf_msg_type ) {
            case AMFE_META_FLUSH:
                /*
                 * We use bfs_open so we open all dependant filesets.  The
                 * alternative would be bs_access.
                 */
                sts = bfs_open(&bfSetp,
                                   msg->amf_data.amf_meta_flush.amf_bf_set_id,
                                   0,         /* No options */
                                   FtxNilFtxH /* No parent transactions */
                                  );
                if (sts == EOK) {
                    /* 
                     * BFS is in memory.  Access the bfap. Don't bump the
                     * v_count
                     */
                    bfap = advfs_lookup_valid_bfap(bfSetp,
                                 msg->amf_data.amf_meta_flush.amf_meta_tag,
                                 BF_OP_INMEM_ONLY|BF_OP_INTERNAL,
                                                   /* don't bother with bfaps 
                                                    * aren't in memory...
                                                    * they've already been
                                                    * flushed
                                                    */
                                 FtxNilFtxH        /* No parent transactions */
                                );
                    if (bfap != NULL) {
                        int err;

                        if (lk_get_state( bfap->stateLk ) != ACC_VALID) {
                            /*
                             * We got an in memory bfap.  It should be state
                             * ACC_VALID.   The exception to this is that the
                             * file may be in ACC_VALID_EXCLUSIVE or, if a
                             * directory or a long symlink, the file may have a
                             * page to be flushed while it is being created. 
                             * Unless the file is ACC_VALID, just skip it.
                             * Other states are transient and are either
                             * flushing or will be ACC_VALID shortly.
                             */

                            /*
                             * DEC_REFCNT will unconditionally release the
                             * bfaLock 
                             */
                            ACCESS_STATS( metaflush_skipping_bfap); 

                            DEC_REFCNT( bfap, DRC_CLOSED_LIST );

                        } else {

                            ADVFS_ACCESS_LOGIT( bfap, "metaflush_thread found bfap");
                            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                            if (bfap->dataSafety == BFD_METADATA) {
                                err = fcache_vn_flush( &bfap->bfVnode,
                                                   msg->amf_data.amf_meta_flush.amf_start.amf_offset,
                                                   msg->amf_data.amf_meta_flush.amf_size,
                                                   NULL, /* fs_pvt_params */
                                                   FVF_WRITE | FVF_ASYNC);
                                /*
                                 * Make sure we didn't mess up the arguments.
                                 */
                                MS_SMP_ASSERT(err!=EINVAL);
                                MS_SMP_ASSERT(err!=EFAULT);
                            } else {
                                MS_SMP_ASSERT(bfap->dataSafety == BFD_LOG);
                                /* Flush the log upto the passed in lsn */
                            
                                advfs_lgr_flush(bfap->dmnP->ftxLogP,
                                            msg->amf_data.amf_meta_flush.amf_start.amf_lsn,
                                            TRUE);                            
                            }
                            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
                            /* 
                             * DEC_REFCNT will unconditionally release the
                             * bfaLock 
                             */
                            DEC_REFCNT( bfap, DRC_CLOSED_LIST );
                        }
                        
                    } else {
                        /* 
                         * bfap == NULL. Release the bfs from the
                         * bfs_open.
                         */
                        ACCESS_STATS( metaflush_missed_bfap_lookup ); 
                    }

                    /*
                     * Close the bitfileset
                     */
                    bs_bfs_close( bfSetp, 
                                  FtxNilFtxH, 
                                  BFS_OP_DEF ); 

                } else {
                    /*
                     * sts != EOK from bfs_open 
                     */
                    ACCESS_STATS( metaflush_missed_bf_set_open );
                }
                        



                break;
            case AMFE_THREAD_GO_AWAY:
                done = TRUE;
                break;
            default:
                /* We shouldn't get here */
                MS_SMP_ASSERT( FALSE );
                break;
        }
        msgq_free_msg( advfs_metaflush_msg_queue, msg );
    }

}


/*
 * BEGIN_IMS
 *
 * advfs_init_metaflush_thread - creates the advfs_metaflush_thread
 *  
 * Parameters:
 * 
 * Return Values:
 *      If this function fails, it will panic the system.  AdvFS failed to
 *      initialize. 
 *
 * Entry/Exit Conditions:
 *      On exit, at least one thread was created and the msg queue was 
 *      initialized.
 *
 * Algorithm:
 *      Count the cpus on the system and create a new thread for each one.   
 *
 * END_IMS
 */
      
void
advfs_init_metaflush_thread()
{

    extern proc_t *Advfsd;
    tid_t tid;
    statusT sts;
    int32_t ncpus;          /* Total number of cpus == num threads.*/
    int32_t i;              /* loop control */
    int32_t nthreads;       /* Number of threads successfully started */

     /* Try to create a message queue for the metaflush thread */
    sts = msgq_create(  &advfs_metaflush_msg_queue, 
                        ADVFS_METAFLUSH_QUEUE_SIZE,
                        sizeof( advfs_metaflush_thread_msg_t),
                        TRUE, /* Queue can grow larger */
                        0 /* radId */
                     );
    if (sts != EOK) {
        ADVFS_SAD0("advfs_init_metaflush_thread: can't allocate memory for metaflush message queue.");
    }
   
    ncpus = how_many_processors();

    /* 
     * Now loop through and create one thread per cpu.  Count the number of
     * threads we actually create.  As long as we have one at the end we
     * will consider ourselves successful.  If none initialize, then we have
     * a problem.
     */
    nthreads = 0;
    for (i = 0; i < ncpus; i++ ) {
        if (kdaemon_thread_create((void (*)(void *))advfs_metaflush_thread,
                                    NULL,
                                    Advfsd,
                                    &tid,
                                    KDAEMON_THREAD_CREATE_DETACHED) == 0) {
            nthreads++;
        }
    }
    
    /* 
     * If we didn't initialize any threads, fail
     */
    if (nthreads == 0) {
        ADVFS_SAD0("AdvFS metaflush thread was not spawned.\n");
    }

}

/*
 * Constructor and destructor functions for multiWaitT.
 */
int
advfs_bs_multiWait_ctor(void *mwp, size_t mwpsize, arena_flags_t flags)
{
    /* XXX Need to set lock hierarchy values */
    ADVSMP_MULTIWAIT_INIT( &((multiWaitT *)mwp)->mw_lock );

    cv_init(&((multiWaitT *)mwp)->mw_cv, "AdvFS multiWait",
            NULL, CV_WAITOK);
    ((multiWaitT *)mwp)->mw_magicid = MAGIC_DEALLOC | MULTIWAITMAGIC;
    return(EOK);
}

void
advfs_bs_multiWait_dtor(void *mwp, size_t mwpsize, arena_flags_t flags)
{
    MS_SMP_ASSERT(((multiWaitT *)mwp)->mw_magicid == 
                  MAGIC_DEALLOC | MULTIWAITMAGIC);
    spin_destroy(&((multiWaitT *)mwp)->mw_lock);
    cv_destroy(&((multiWaitT *)mwp)->mw_cv);
}
