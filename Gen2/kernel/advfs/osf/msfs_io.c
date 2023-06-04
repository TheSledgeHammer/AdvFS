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
 *      AdvFS 
 *
 * Abstract:
 *
 *      AdvFS System IO related routines.
 *
 */

#define ADVFS_MODULE MSFS_IO

#include <sys/time.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <ms_osf.h>
#include <sys/param.h>
#include <sys/errno.h>
#include <sys/kern_svcs.h>
#include <sys/buf.h>
#include <sys/mount.h>
#include <advfs_evm.h>
#include <stdarg.h>
#include <tcr/clu.h>
#include <sys/spinlock.h>
#include <sys/scsi.h>
#include <advfs/advfs_conf.h>
#include <vfast.h>

extern int advfs_fstype;

int AdvfsDomainPanicLevel=1;
                        /*  Domain Panic Levels (what to do in domain_panic):
                         *  0: do not call live_dump 
                         *  1: call live_dump if domain is mounted (default)
                         *  2: call live_dump even if domain is not mounted 
                         *  3: promote domain panic to system panic
                         */

void
domain_panic(domainT *dmnP, char *format, ...)
{
    va_list valist;
    char buf[DMN_PANIC_MAX_MSG_LEN];
    va_start(valist, format);
    /* 
     * The 3 corresponds to PRF_STRING defined in h/mdep_kernprivate.h
     */

    prf(format, valist, 3, &buf[0], DMN_PANIC_MAX_MSG_LEN);
    va_end(valist);

    _domain_panic(dmnP, buf, DMN_PANIC_GENERIC);
    return;
}
/*
 * domain_panic - halts future access to disks and domain
 * 
 * This routine sets a flag in the domain struct.  It is used
 * to block both in-progress I/O's (advfs_bs_startio) as well as to block
 * new operations from being started.
 */


void
_domain_panic(domainT *dmnP, char *msg, int flags)
{
    advfs_ev advfs_event;
    extern struct vfs *rootvfs;

    MS_SMP_ASSERT(dmnP);

    if (AdvfsDomainPanicLevel == 3) {
        if (flags & DMN_PANIC_IO_CONNECTIVITY) {
            printf("Domain panic appears to be due to a hardware problem\n");
        }
        ADVFS_SAD(
        "domain panic promoted due to AdvfsDomainPanicLevel of 3:  %s", msg);
    }

    /* if the root domain panics, panic the system */

    if (clu_type() == CLU_TYPE_SSI) {
        struct vfs *pvfsp;
        int err = 0;

        /*
         * rootfs:
         *
         * If (err == ENOENT), we know that rootfs is not completely set
         * up yet, but there are no other filesets mounted.
         * So we are safe to assume that the domain to be paniced
         * is the global root, in which case we should panic the system.
         */

        CLU_GETPFSVFSP(rootvfs, &pvfsp, err);
        if ((err == ENOENT) ||
            (!err && ((rootvfs == pvfsp) ||
                      ((pvfsp->vfs_mtype == advfs_fstype) &&
                       (dmnP == ADVGETDOMAINP(pvfsp)))))) {
            if (flags & DMN_PANIC_IO_CONNECTIVITY) {
                printf( "Domain panic appears to be due to a hardware problem\n");
            }
            ADVFS_SAD( "Domain panic promoted because the domain is cluster root: %s",
                       msg);
        }

    } else {
        if ((rootvfs->vfs_mtype == advfs_fstype) && 
            (dmnP == ADVGETDOMAINP(rootvfs))) {
            if (flags & DMN_PANIC_IO_CONNECTIVITY) {
                printf( "Domain panic appears to be due to a hardware problem\n");
            }
            ADVFS_SAD( "Domain panic promoted because the domain is root: %s",
                       msg);
        }
    }

    /* if the domain is already paniced we don't need to do it again */
    if ( dmnP->dmn_panic ) {
        dmnP->dmn_panic |= flags;
        return;
    }

    dmnP->dmn_panic |= flags;

    if (clu_is_ready()) {
        CLU_CFS_DOMAIN_PANIC(dmnP->domainName, dmnP->dmn_panic);
    }

    /*
     * NOTE: advfs_event is a local variable, but the EVM routines
     * called here use malloc (WAITOK).  This will be a problem
     * if _domain_panic() is ever called with simple locks
     * held, which currently doesn't seem to be the case.
     */
    bzero(&advfs_event, sizeof(advfs_ev));
    advfs_event.domain = dmnP->domainName;
    advfs_post_kernel_event(EVENT_FDMN_PANIC, &advfs_event);

    if (!(dmnP->dmn_panic & DMN_PANIC_UNMOUNT)) {

        printf("\n%s\n", msg);
        printf("AdvFS Domain Panic; Domain %s Id 0x%08x.%08x\n", 
               dmnP->domainName,
               dmnP->domainId.id_sec,
               dmnP->domainId.id_usec);
        printf("An AdvFS domain panic has occurred due to either a");
        printf(" metadata write error or an internal inconsistency.");
        printf(" This domain is being rendered inaccessible.\n");

        if (!clu_is_ready()) {
            printf("Please refer to guidelines in AdvFS Guide to File");
            printf(" System Administration regarding what steps to");
            printf(" take to recover this domain.\n");
        }
        if (flags & DMN_PANIC_IO_CONNECTIVITY) {
            printf("Domain panic appears to be due to a hardware problem\n");
        }
    }    

    /*
     * Create a dump file of the running system to aid debugging.  Dump 
     * only mounted domains by default (AdvfsDomainPanicLevel of 1).
     */
    switch (AdvfsDomainPanicLevel) {

        case 1: 
            if (dmnP->mountCnt) {
                livedump(msg, NULL);
            }
            break;

        case 2:
            livedump(msg, NULL);
            break;

        default:
            break;
    }
}

/*
 * advfs_iodone
 *
 * Perform post IO completion processing.
 * The disk driver calls biodone() who calls this AdFS IO completion routine
 * at interrupt level. Advfs_bs_startio() may also call this routine when
 * servicing a "fake" advfs IO request for callers who want AdvFS iodone
 * processing without calling the disk strategy.
 *
 * SMP: 1. This is executed in interrupt context mode and must not block.
 *      2. No locks held when called.
 */

void
advfs_iodone(struct buf *bp)      /* in */
{
    vdT *vdp=NULL;
    fileSetNodeT *fsnp;
    adviodesc_t *iodescp;
    ioanchor_t *ioanchorp; 
    domainT *dmnP;
    statusT sts = EOK;
    int32_t freeioanchor;
    int32_t freebufflag;
    int32_t anchrwakeup;
    int32_t chainerrors;
    int32_t freeiodesc = TRUE;
    uint64_t dummy;
    advfs_handyman_thread_msg_t *msg;
    extern msgQHT advfs_handyman_msg_queue;
    extern uint64_t AdvfsIORetryControl;
    

    /* Retrieve the adviodesc_t from the buf structure. */
    iodescp = (adviodesc_t *)bp->b_private;
    
    dmnP = iodescp->advio_bfaccess->dmnP;
    if (!(iodescp->advio_flags & ADVIOFLG_FAKEIO)){
        vdp = VD_HTOP( iodescp->advio_blkdesc.vdIndex, dmnP );
    }

    /* Process any IO error. */
    if(BGETFLAGS(bp) & B_ERROR) {
        /* Retry IO request when AdvFS IO error retry is enabled (non-zero);
         * the driver indicates the IO is retriable; and there remains
         * time to retry the request.
         */
        if ((AdvfsIORetryControl > 0) && (bp->b_eei & EEI_IO_RETRIABLE_FLG)) {
            struct timeval current_time;

            current_time = get_system_time();
	    /* Setup the retry start time in seconds the first time through. */
	    if (iodescp->advio_retry_starttime == 0) {
                iodescp->advio_retry_starttime = current_time.tv_sec;
            }
            if ((iodescp->advio_retry_starttime + AdvfsIORetryControl)
                > current_time.tv_sec) {
                /* Send a message to the handyman thread to retry the IO */
                msg = (advfs_handyman_thread_msg_t*)
                          msgq_alloc_msg(advfs_handyman_msg_queue );
                if (msg) {
                    msg->ahm_msg_type = AHME_RETRY_IO;
                    msg->ahm_data.ahm_io_retry_bp = bp;
                    msgq_send_msg( advfs_handyman_msg_queue, msg );
                    return;
                }
            }
	}

        printf("AdvFS I/O error:\n");

        /* Process domain panic for read requests setup by advfs_bs_startio. */
        if ((iodescp->advio_flags & ADVIOFLG_DMN_PANIC_IO) &&
            (BGETFLAGS(bp) & B_READ)) {
            printf("    A read failure occurred "
                   "- the AdvFS filesystem is inaccessible (paniced)\n");
        }

        fsnp = iodescp->advio_bfaccess->bfFsContext.fsc_fsnp;
        if (!dmnP->dmn_panic && fsnp && (fsnp->f_mntfromname != NULL) &&
            (fsnp->f_mntonname != NULL)) {
            printf("    AdvFS filesystem: %s\n", fsnp->f_mntfromname);
            printf("    Mounted on: %s\n", fsnp->f_mntonname);
        } 

        MS_SMP_ASSERT(vdp != NULL);
        
        printf("    Volume: %s\n", vdp->vdName);
        printf("    Tag: 0x%lx.%x\n", 
               iodescp->advio_bfaccess->tag.tag_num, 
               iodescp->advio_bfaccess->tag.tag_seq);
        printf("    Page: %lu\n",
          (bp->b_bcount / (iodescp->advio_bfaccess->bfPageSz * ADVFS_FOB_SZ)));
        printf("    Block: %lu\n", iodescp->advio_blkdesc.vdBlk);
        printf("    Block count: %u\n", bp->b_bcount / DEV_BSIZE);
        printf("    Type of operation: %s\n",
               (!(BGETFLAGS(bp) & B_READ) ) ? "Write" : "Read");
        printf("    Error: %d\n", bp->b_error ? bp->b_error : EIO);
        printf("    EEI: 0x%x (%s)\n", bp->b_eei,
                (bp->b_eei & EEI_IO_RETRIABLE_FLG) ? 
                "retries may help" : "Advfs cannot retry this" );
        printf("    AdvFS initiated retries: %d\n",
                  iodescp->advio_ioretrycount);
        printf("    Total AdvFS retries on this volume: %d\n",
               vdp->vdRetryCount );

        /* Suggest filename id help for files that are not reserve metadata.*/
        if (!dmnP->dmn_panic && fsnp && (fsnp->f_mntonname != NULL) && 
            (iodescp->advio_bfaccess->tag.tag_num > 1)) {
            printf("    To obtain the name of the file on which\n");
            printf("    the error occurred, type the command:\n");
            printf("      /usr/sbin/ncheck %s\n", fsnp->f_mntonname);
            printf("    and search for the file tag number %lu.\n",
                   iodescp->advio_bfaccess->tag.tag_num);
        }

        /* If driver indicates IO was retriable, display AdvFS IO retry help
         * only when advfs_io_retries is disabled.
         */
        if (bp->b_eei & EEI_IO_RETRIABLE_FLG) {
            printf("    Current advfs_io_retries time (seconds): %lu\n", 
                   AdvfsIORetryControl);
            if (iodescp->advio_retry_starttime == 0) {
                printf("\n");
                printf("    It is possible that enabling AdvFS I/O\n");
                printf("    retries may have allowed this I/O to succeed.\n");
                printf("\n");
                printf("    To modify the AdvFS IO retry time, adjust the\n");
                printf("    'advfs_io_retries' kernel tunable:\n");
                printf("\n");
                printf("        "
                          "/usr/sbin/kctune advfs_io_retries\n");
                printf("        "
                          "/usr/sbin/kctune advfs_io_retries=nn\n");
                printf("\n");
                printf("    nn == 0 - No AdvFS I/O retries, default\n");
                printf("          1 to %d - total seconds AdvFS will retry I/O\n",
                       ADVFS_IO_RETRIES_MAX);
                printf("\n");
            }
        }

        if( bp->b_error ) {
            sts = (statusT)bp->b_error;
        } else {
            sts = (statusT)EIO;
        }

        /* We are done with the bp->b_eei setting, so clear. */
         bp->b_eei = 0;

        /*
         * If a write to a metadata file fails, call domain_panic().
         * That routine will decide whether to panic the domain or
         * the system.  Directories are considered to be metadata files
         * since they have to correlate with the tag directories.
         */
        if ((sts != EOK) &&
            (!(BGETFLAGS(bp) & B_READ)) &&
            ((iodescp->advio_bfaccess->dataSafety == BFD_LOG) ||
             (iodescp->advio_bfaccess->dataSafety == BFD_METADATA))) {
            /* 
             * Don't domain panic a second time.
             */
            if (!dmnP->dmn_panic) {
                domain_panic_type(dmnP,
                                  "advfs_iodone: metadata write failed",
                                  DMN_PANIC_IO_CONNECTIVITY);
            }
            BRESETFLAGS(bp, (bufflags_t)B_ERROR);
            bp->b_error = 0;
            sts = EOK; /* ignore error */
        }
    } /* end if (bp->b_flags & B_ERROR) */

    /* Call advfs_bs_io_complete() when all IO's associated with the
     * an ioanchor are done.
     * DirectIO or migrate may issue multiple IO's and
     * use the ioanchor to coordinate IO completions. 
     */
    ioanchorp = iodescp->advio_ioanchor;
    spin_lock(&ioanchorp->anchr_lock);

    /* Decide if the current buf structure should be freed based upon the
     * ioanchor's flag. For UFC IO, only allow freeing of the buf
     * if it does not match the original UFC created buf structure stored
     * in the anchor's anchr_origbuf structure. UFC handles freeing the
     * original UFC buf, not AdvFS.
     * Therefore, the default is for advfs_iodone() to free any "copy"
     * buf not matching the original.
     * Callers setting the flag must take responsibility for freeing the
     * "copy" buf structures.
     * The default for DirectIO, is for advfs_iodone() to free all buf
     * structures upon IO completion because they do not use the UFC.
     * DirectIO caller must set the ioanchor's anchr_origbuf = NULL.
     * However, DirectIO can also choose to use this flag if it wants to
     * take responsibility for freeing its own buf structures.
     */
    freebufflag = ((ioanchorp->anchr_flags & IOANCHORFLG_KEEP_BUF) ||
                   (bp == ioanchorp->anchr_origbuf)) ? FALSE : TRUE;  

    /* See if there is a waiter to wake up. There can only be a single
     * waiter since this is a per thread i/o. Also we can not touch
     * the ioanchor again after calling io completion since the
     * anchor could have been released.
     */

    anchrwakeup = ((ioanchorp->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) ||
                   (ioanchorp->anchr_flags & IOANCHORFLG_WAKEUP_ON_LAST_IO));

    /* See if we should chain errors together. We need to save this
     * info now since the ioAnchor may be freed and we can't check for
     * this.
     */

    chainerrors = ioanchorp->anchr_flags & IOANCHORFLG_CHAIN_ERRORS;

    /* Account for I/O error for this buf structure; if doing several
     * I/Os per ioanchor, this will generate the offset at which the
     * logically first error was encountered.  The number of bytes
     * successfully transferred will be set into the ioanchor structure
     * in advfs_bs_io_complete() after the final I/O has completed.
     */
    if (sts != EOK ) {
        if ( bp->b_foffset < ioanchorp->anchr_min_err_offset ) {
            ioanchorp->anchr_io_status = sts;
            ioanchorp->anchr_min_err_offset = bp->b_foffset;
        }
    }
     
    if (--ioanchorp->anchr_iocounter == 0 ) {
        /* Check the ioanchor's flag to decide to free the ioanchor or not.
         * The default is for advfs_iodone() to free the anchor. However,
         * some AdvFS functions may want to handle freeing the anchor. 
         */
        freeioanchor = (ioanchorp->anchr_flags & IOANCHORFLG_KEEP_ANCHOR) ?
                        FALSE : TRUE;

        spin_unlock(&ioanchorp->anchr_lock);

        /* Do the final AdvFS IO completion processing. This may call
         * UFC's iodone(). The ioanchor's lock must not be held.
         */
        advfs_bs_io_complete(bp);

        /* Wakeup any waiters on the ioanchor's condition variable.
         * Waiters must use the IOANCHORFLG_KEEP_ANCHOR and manage the
         * the freeing of the anchor after being woken up.
         *
         * NOTE: Avoid making any modifications to ioanchorp or bp once the
         *       cv_broadcast() is sent.  Especially make sure the
         *       IOANCHORFLG_IODONE flag is set before calling
         *       cv_broadcast(), otherwise a known race in
         *       advfs_dio_unaligned_xfer() could come back.
         *
         *       And in case anyone wants to nit pick, freeing the lock
         *       should not be a problem as ioanchor_t structures are kept
         *       in their own arena with pre-initialized locks, and if the
         *       arena should want to reclaim the structure, the rarely
         *       called advfs_bs_ioanchor_dtor() routine will acquire and
         *       release the lock before allowing the lock to be destroyed.
         *       This will avoid any race with another thread freeing
         *       ioanchorp and the unlock below.  
         *
         *       And for the astute observer, we do _NOT_ want to call
         *       cv_broadcast_unlock() nor do we want to release the lock
         *       before calling cv_broadcast().  If we do that, then if the
         *       broadcast is delayed in occurring (could happen), the
         *       advfs_dio_unaligned_xfer() code could see the
         *       IOANCHORFLG_IODONE flag is set, never call cv_wait(), call
         *       advfs_bs_free_ioanchor(), a other thread could call
         *       advfs_bs_get_ioanchor() getting the just released
         *       ioanchor_t structure, start its own I/O and be sitting on a
         *       cv_wait() with the exact same condition variable address
         *       when the delated broadcast for the previous I/O finally
         *       occurs.  Then we would be back in the same race, only via a
         *       different convoluted path.  
         */

        /* We can ONLY touch the ioAnchor if we are freeing it or
         * there are waiters to be woken up. Otherwise the anchor
         * may have gone away by now.
         */

        if (anchrwakeup) {
            MS_SMP_ASSERT(!freeioanchor);
            spin_lock(&ioanchorp->anchr_lock);
            ioanchorp->anchr_flags |= IOANCHORFLG_IODONE;
            cv_signal(&ioanchorp->anchr_cvwait, 
                      &ioanchorp->anchr_lock, 
                      CV_SPIN);
        }
        /* Free the ioanchor if necessary. */
        if (freeioanchor){
           advfs_bs_free_ioanchor(ioanchorp, M_WAITOK);
        }
    }
    else {
        /* This was not the last IO request associated with the ioanchor's
         * multi-IO set. Check if the ioanchor flag indicates to wakeup a
         * waiter on every IO completion. In this case, the ioanchor structure
         * remains intact and no final IO completion procesing happens yet.
         *
         * NOTE: Avoid making any modifications to ioanchorp or bp once the
         *       cv_broadcast() is sent.
         */
        if (ioanchorp->anchr_flags & IOANCHORFLG_WAKEUP_ON_COW_READ) {
            cv_signal(&ioanchorp->anchr_cvwait, NULL, CV_NULL_LOCK);
        }
        spin_unlock(&ioanchorp->anchr_lock);
    }

    /* If the ioanchor flag IOANCHORFLG_CHAIN_ERRORS is set, then link the
     * adviodesc to the ioanchor and don't free the adviodesc structure. */
        if ((sts != EOK) && (chainerrors)) {
        /* Make sure someone will be watching the ioanchor if we won't be
         * freeing the iodescriptors memory in this routine */
        MS_SMP_ASSERT( ioanchorp->anchr_flags & IOANCHORFLG_KEEP_ANCHOR );
        spin_lock(&ioanchorp->anchr_lock);
        iodescp->advio_fwd = ioanchorp->anchr_error_ios;
        ioanchorp->anchr_error_ios = iodescp;
        freeiodesc = FALSE;
        spin_unlock( &ioanchorp->anchr_lock );
    }


    /* Periodically start vfast. */
    if (SS_is_running &&
       (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) &&
       (dmnP->ssDmnInfo.ssIoInterval) &&
       (vdp) && (vdp->ssVolInfo.ssIOCnt++ >= dmnP->ssDmnInfo.ssIoInterval)) {
        /* occasionally run vfast when system stays busy */
        ss_startios(vdp);
        vdp->ssVolInfo.ssIOCnt=0;
    }

    /* Decrement the virtual disk's active IO request counter. */
    if (vdp) {
        ATOMIC_FETCH_DECR(&vdp->vdIoOut, &dummy);
    }

    /* free the buf structure if necessary. */
    if (freebufflag) 
        advfs_bs_free_buf(bp);

    /* Free the adviodesc_t structure. */
    if (freeiodesc) {
        advfs_bs_free_adviodesc(iodescp);
    }
}

/*
 * BEGIN_IMS
 *
 * advfs_raw_io - This function is a generic raw io routine for advfs.  It
 * handles RAW_READ and RAW_WRITE parameters that determine what type of IO
 * is issued.
 *
 * Parameters:
 *      dev_vnode - The vnode for the device to have IO sent to.
 *      vd_blk - The starting block on dev_vnode (in DEV_BSIZE units) for
 *               the IO.
 *      blk_cnt - The number of blocks (in DEV_BSIZE units) to be issued.
 *                Defines the size of the IO.  data ought to be this size.
 *      raw_type - Either RAW_READ or RAW_WRITE depending on the type of IO
 *                 requested.
 *      data - a pointer to the data buffer for the IO.  This should have
 *             enough buffer space to cover blk_cnt * DEV_BSIZE
 * 
 * Return Values:
 *      EOK - Success;
 *      ENOMEM - A buf structure couldn't be malloced.  
 *      Error status from biowait.
 *
 * Entry/Exit Conditions:
 *      On entry, data must not be NULL.
 *      On entry, data must contain valid data to be written to disk if
 *      RAW_WRITE. The data must be at least blk_cnt * DEV_BSIZE bytes.
 *      On entry, data must point to a buffer to hold blk_cnt * DEV_BISZE
 *      bytes if RAW_READ.
 *      On exit, IO has completed.  If RAW_READ, data holds disk data from
 *      [vd_blk..vd_blk+blk_cnt].
 *
 *
 * Algorithm:
 *      Create a buf structure,
 *      Initialize fields in the buf structure,
 *      Issue an io,
 *      Wait for IO to complete.
 *      Free buf structure,
 *      Return error status.
 *
 * END_IMS
 */
statusT
advfs_raw_io ( struct vnode* dev_vnode,         /* Device vnode for io */
               bf_vd_blk_t vd_blk,              /* start disk blk for IO */
               bf_vd_blk_t blk_cnt,             /* Blk count of DEV_BSIZE
                                                 * blocks for IO. */
               raw_io_type_t raw_type,          /* {RAW_READ,RAW_WRITE} */
               void *data
             )
{
    /*
     * The buf struct to be used to issue the IO.
     */
    struct buf *bp;
    int res = EOK;

    /* 
     * Acquire a new zeroed buf structure.
     */
    bp = advfs_bs_get_buf(TRUE, TRUE);

    /*
     * Initialize buf struct for IO.
     */
    bp->b_un.b_addr = (caddr_t)data;
    bp->b_spaddr = ldsid( bp->b_un.b_addr ); 
    bp->b_blkno = (kern_daddr_t)vd_blk;
    bp->b_bcount = blk_cnt << DEV_BSHIFT; /* Byte count of transfer */
    bp->b_offset = vd_blk <<  DEV_BSHIFT; /* Start byte of IO */
    bp->b_vp = dev_vnode;
    bp->b_dev = dev_vnode->v_rdev;
    bp->b_iodone = NULL;
    bp->b_error = 0;
    bp->b_eei = 0;
    
    if (raw_type == RAW_READ) {
        BSETFLAGS(bp,(bufflags_t)(B_READ | B_SYNC));
    } else {
        MS_SMP_ASSERT (raw_type == RAW_WRITE);
        BSETFLAGS(bp, (bufflags_t)(B_WRITE | B_SYNC));
    }

    /*
     * Set b2_flags to make sure nothing is in there (VOP_STRATEGY checks
     * this).
     */
    bp->b2_flags = 0;

    /*
     * This will call bp->b_vp's strategy routine which was set to dev_vnode
     */
    VOP_STRATEGY(bp);


    /*
     * Wait for the IO to complete.
     */
    res = biowait(bp);

    if( (BGETFLAGS(bp) & B_ERROR) != 0 ) {
        if( bp->b_error ) {
            res = bp->b_error;
        } else {
            res = EIO;
        }
    }
    else if (bp->b_resid) {
        res = EIO;
    }

    /*
     * Return the buf to the arena
     */
    advfs_bs_free_buf( bp);

    return( res );

}

