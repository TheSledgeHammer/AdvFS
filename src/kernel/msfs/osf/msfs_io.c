/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      Routines to interface the MegaSafe System
 *      with the OSF device drivers.
 *
 * Date:
 *
 *      Mon Aug 19 16:50:56 1991
 *
 */
/*
 * HISTORY
 * 
 * 
 * 
 */
#pragma ident "@(#)$RCSfile: msfs_io.c,v $ $Revision: 1.1.262.11 $ (DEC) $Date: 2007/06/26 12:49:42 $"

#define ADVFS_MODULE MSFS_IO

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_osf.h>
#include <sys/param.h>
#include <sys/errno.h>
#include <sys/specdev.h>
#include <sys/buf.h>
#include <sys/radset.h>
#include <sys/ucred.h>  /* only for NOCRED */
#include <sys/mount.h>
#include <sys/lock_probe.h>
#include <kern/event.h>
#include <msfs/advfs_evm.h>
#include <io/common/deveei.h>
#include <stdarg.h>

#include <vm/vm_page.h>
#include <vm/vm_numa.h>
#include <vm/vm_ubc.h>
#include <sys/lwc.h>
#include <sys/syslog_pri.h>
#include <dec/binlog/binlog.h>
#include <sys/clu.h>

#ifdef ADVFS_SMP_ASSERT
static char *smdbg1 =  "bs_startio:    bsBuf 0x%lx  (blockingQ)  vd 0x%lx\n";
static char *smdbg2 =  "bs_startio:    bsBuf 0x%lx  time = %d  delta = %d  %c  vd 0x%lx\n";
#endif /* ADVFS_SMP_ASSERT */

extern unsigned TrFlags;
extern int prattached;
extern char *panicstr;
int MsfsIoOut = 0;

int Advfs_allow_driver_io_feedback=1;  /* For testing, mostly. */
int Advfs_biostats_devq_delta = 8;     /* value for changing devQ.lenLimit */
int Advfs_biostats_low_shift  = 6;     /* 1.5%; acceptable I/O inefficiency */
int Advfs_biostats_high_shift = 2;     /* 25%; unacceptable I/O inefficiency */

int AdvfsDomainPanicLevel=1;
                        /*  Domain Panic Levels (what to do in domain_panic):
                         *  0: do not call live_dump 
                         *  1: call live_dump if domain is mounted (default)
                         *  2: call live_dump even if domain is not mounted 
                         *  3: promote domain panic to system panic
                         */

#ifdef ADVFS_DEBUG
/* Next added for checking queue integrity */
extern unsigned int Verify_Io_Queue;
#endif


void
bs_printf(char *fmt, ...);


/*
 * getosfbuf
 *
 * Allocate a new buf struct for I/O purposes only.
 * They are allocated and filled just before calling
 * the driver's strategy routine.  Clear all fields
 * here so we don't have unexpected operation in the
 * driver from fields we don't initialize and init
 * the b_iocomplete event lock.
 */
static struct buf *
getosfbuf(void)
{
    struct buf *bp;

    RAD_MALLOC(bp, struct buf *, sizeof(*bp), M_ADVFS, 
                (M_WAITOK | M_PREFER), current_execution_radid());

    if( bp == 0 ) {
        ADVFS_SAD0("getosfbuf: MALLOC out of space");
    }

    bzero((char *)bp, sizeof(*bp));
    event_init( &bp->b_iocomplete );

    /* We do not lock_setup(b_lock), this is not used in our paths.
     * A driver could check state is not valid and do the init.
     */
    return( bp );
}

/*
 * freeosfbuf
 *
 * Free a buf struct
 */

void
freeosfbuf( 
    struct buf *bp      /* in */
    )
{
    /* this is paranoia just because we have seen some corruptions
     * in our iodesc structs.  clear the important buf struct fields
     * that we set just so if there is a mistake made by the next
     * allocator, they don't have pointers into our space.
     */
    bp->b_proc = NULL;
    bp->b_dev = NULL;
    bp->b_iodone = NULL;
    bp->b_drv_handle = NULL;
    bp->b_pagelist = NULL;
    bp->b_un.b_addr = NULL;

    /* destroy event to make lock status keeping correct in lockmode=4 */
    event_terminate(&bp->b_iocomplete);

    FREE(bp, M_ADVFS);
}

void
domain_panic(domainT *dmnP, char *format, ...)
{
    va_list valist;
    char buf[DMN_PANIC_MAX_MSG_LEN];

    va_start(valist, format);
    prf((uchar *) &buf[0], (uchar *) &buf[DMN_PANIC_MAX_MSG_LEN], (u_char *)format, valist);
    va_end(valist);

    _domain_panic(dmnP, buf, DMN_PANIC_GENERIC);
    return;
}
/*
 * domain_panic - halts future access to disks and domain
 * 
 * This routine sets a flag in the domain struct.  It is used
 * to block both in-progress I/O's (call_disk) as well as to block
 * new operations from being started.
 */

void
_domain_panic(domainT *dmnP, char *msg, int flags)
{
    char logmsg[1024];
    int errno;
    advfs_ev advfs_event;

    MS_SMP_ASSERT(dmnP);

    if (AdvfsDomainPanicLevel == 3) {
        if (flags & DMN_PANIC_IO_CONNECTIVITY) {
            printf("Domain panic appears to be due to a hardware problem\n");
            printf("Check the binary error log for more information.\n");
        }
        dmnP->dmn_panic |= flags;
        ADVFS_SAD("domain panic promoted due to AdvfsDomainPanicLevel of 3:  %s", msg);
    }

    /* if the root domain panics, panic the system */

    /* for cfs, we have to check global root and local root */
    if (clu_is_ready()) {
        struct mount *pmp;
        extern struct mount *bootfs;
        int err = 0;

        /*
         * rootfs:
         *
         * If (err == ENOENT), we know that rootfs is not completely set
         * up yet, but there are no other filesets mounted.
         * So we are safe to assume that the domain to be panicked
         * is the global root, in which case we should panic the system.
         *
         * (NOTE: We do not use the check
         *
         *     if (!strcmp(dmnP->vdTbl[0]->vdName, default_rootname))
         *
         * for the bootup sequence because it is not always true.)
         *
         */
  
        err = CC_GETPFSMP(rootfs, &pmp);
        if ((err == ENOENT) ||
            (!err && (pmp-> m_stat.f_type == MOUNT_MSFS) &&
             (dmnP == GETDOMAINP(pmp))))  {

            if (flags & DMN_PANIC_IO_CONNECTIVITY) {
                printf("Domain panic appears to be due to a hardware problem\n");
                printf("Check the binary error log for more information.\n");
            }
                dmnP->dmn_panic |= flags;
                ADVFS_SAD("domain panic promoted because the domain is the cluster root:  %s", msg);
        }

        /*
         * for local root (miniroot), it may be ok only panic the domain instead
         * of the system. but now we just panic the system, revisit the code
         * later.
         */  
        err = CC_GETPFSMP(bootfs, &pmp);
        if (!err && (pmp-> m_stat.f_type == MOUNT_MSFS) && 
            (dmnP == GETDOMAINP(pmp))) {
                if (flags & DMN_PANIC_IO_CONNECTIVITY) {
                    printf("Domain panic appears to be due to a hardware problem\n");
                    printf("Check the binary error log for more information.\n");
                }
                dmnP->dmn_panic |= flags;
                ADVFS_SAD("domain panic promoted because the domain is a boot partition:  %s", msg);
        }
    } else {
        if ((rootfs->m_stat.f_type == MOUNT_MSFS) && 
            (dmnP == GETDOMAINP(rootfs))) {
            if (flags & DMN_PANIC_IO_CONNECTIVITY) {
                printf("Domain panic appears to be due to a hardware problem\n");
                printf("Check the binary error log for more information.\n");
            }
            dmnP->dmn_panic |= flags;
            ADVFS_SAD("domain panic promoted because the domain is the root domain:  %s", msg);
        }
    }

    /* if the domain is already paniced we don't need to do it again */
    if ( dmnP->dmn_panic ) {
        dmnP->dmn_panic |= flags;
        return;
    }

    dmnP->dmn_panic |= flags;

    if (clu_is_ready()) {
        CC_CFS_DOMAIN_PANIC(dmnP->domainName, dmnP->dmn_panic);
    }

    if (AT_INTR_LVL()) {
        EvmEventPostImmedVa(NULL,
                       EvmITEM_NAME,
                       EVENT_FDMN_PANIC,
                       EvmITEM_CLUSTER_EVENT, EvmTRUE,
                       EvmITEM_VAR_STRING,"domain", dmnP->domainName,
                       EvmITEM_NONE);
    } else {
        /*
         * NOTE: advfs_event is a local variable, but the EVM routines
         * called here use malloc (WAITOK).  This will be a problem
         * if _domain_panic() is ever called with simple locks
         * held, which currently doesn't seem to be the case. 3/00
         */
        bzero(&advfs_event, sizeof(advfs_ev));
        advfs_event.domain = dmnP->domainName;
        advfs_post_kernel_event(EVENT_FDMN_PANIC, &advfs_event);
    }

    if (!(dmnP->dmn_panic & DMN_PANIC_UNMOUNT)) {

        printf("\n%s\n", msg);
        printf("AdvFS Domain Panic; Domain %s Id 0x%08x.%08x\n", 
               dmnP->domainName,
               dmnP->domainId.tv_sec,
               dmnP->domainId.tv_usec);
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
            printf("Check the binary error log for more information.\n");
        }

        /*
         * Put a domain panic message in the binary error log
         */

        sprintf(logmsg, "\n%s\nAdvFS Domain Panic; Domain %s Id 0x%08x.%08x\n", 
            msg, dmnP->domainName, dmnP->domainId.tv_sec, dmnP->domainId.tv_usec);

        strcat(logmsg, "An AdvFS domain panic has occurred due");
        strcat(logmsg, " to either a metadata write error or an");
        strcat(logmsg, " internal inconsistency. This domain is");
        strcat(logmsg, " being rendered inaccessible.\n");

        if (!clu_is_ready()) {
            strcat(logmsg, "Please refer to guidelines in AdvFS Guide");
            strcat(logmsg, " to File System Administration regarding");
            strcat(logmsg, " what steps to take to recover this domain.\n");
        }
        if (flags & DMN_PANIC_IO_CONNECTIVITY) {
            strcat(logmsg, "Domain panic appears to be due to a hardware problem\n");
        }

        binlog_logmsg(ELSW_ADVFS_DMNPNC, logmsg, strlen(logmsg));
    }    

    /*
     * Create a dump file of the running system to aid debugging.  Dump 
     * only mounted domains by default (AdvfsDomainPanicLevel of 1).
     */

    errno=0;
    switch (AdvfsDomainPanicLevel) {

        case 1: 
            if (dmnP->mountCnt) {
                errno=live_dump(msg);
            }
            break;

        case 2:
            errno=live_dump(msg);
            break;

        default:
            break;
    }
    if (errno != 0 ) {
        printf("\nUnable to create live dump for domain %s\n", dmnP->domainName);
        printf("Error number %d returned from live_dump().\n", errno);
    }  
}

#ifdef ADVFS_DEBUG
statusT
bs_diskerror(
    domainT *dmnP,
    int vd,
    int flags,
    int count )

{
    struct vd *vdp;

    /*
     * Check validity of handle.  Get vdp.
     */

    if(dmnP == NULL){
        return( EBAD_DOMAIN_POINTER );
    }

    /* Bump vdRefCnt on this volume */
    if ( !(vdp = vd_htop_if_valid(vd, dmnP, TRUE, FALSE)) ) {
        return( EBAD_VDI );
    }

    mutex_lock( &vdp->vdIoLock );
    /*
     * Set flags and count
     */
    vdp->errorFlag = flags;
    vdp->errorCount = count;
    vdp->errorRepeat = count;

    mutex_unlock( &vdp->vdIoLock );
    vd_dec_refcnt( vdp );
    return( EOK );
}
#endif /* ADVFS_DEBUG */


/*
 * bs_osf_complete
 *
 * Entry for device completion called by driver interrupt
 * routine from bio_done.
 * Get info from OSF buf struct and return struct to our pool.
 * Call the higher level completion routine for each buffer.
 * Start the device on the next buffer.
 *
 * SMP: 1. This is executed in a lightweight context thread and must not block.
 *      2. No locks held when called.
 *      3. Acquires the vdp->devQ.ioQLock for the virtual disk for which this
 *         IO is associated.  The device queue on this disk is traversed
 *         removing all ioDesc for which this IO is associated.
 *      4. Acquires the bsBuf.bufLock for each buffer passed to bs_io_complete.
 */

int AdvfsIoError = 2;

void
bs_osf_complete( 
    struct buf *bp      /* in */
    )
{
    ioDescT *iop = (ioDescT *)bp->b_pagelist;
    ioDescT *next;
    ioDescT *first_iop, *last_iop;
    struct vd *vdp;
    statusT sts = EOK;
    int pages = 0;
    int len;
    int i;
    bfSetT *setp;
    unsigned char *taddr;
    int fileset_is_mounted = FALSE,
        non_metadata_file = FALSE;
    struct mount *mp = NULL;
    domainT *dmnP;
    int resetfirst = 0;
    extern void resetfirstrec( domainT* dmnP );
    void bp_map_free( vm_offset_t, int ); 
    int dpanic_flags=0;
    int hardware_related_error = FALSE;
    int error_maybe_retriable = FALSE;
    int    b_iolen;
    time_t active_time;
    time_t waiting_time;
    extern long lbolt;
    extern int hz;
    extern int bio_sample_shift; /* 2**bio_sample_shift is sampling period */
    extern int bio_time_shift;
    extern REPLICATED int SS_is_running;
    extern long AdvfsIORetryControl;

    dmnP = iop->bsBuf->bfAccess->dmnP;
    vdp = VD_HTOP( iop->blkDesc.vdIndex, dmnP );

    /*
     * Set up the I/O result field
     */
    if( (bp->b_flags & B_ERROR) == 0 ) {
        sts = (statusT)EOK;
    } else {
        /*
         * Retry I/O that returns any of the following b_eei errors
         * This list may need to be adjusted as more (or less) errors
         * are considered worth retrying.  If there are any questions check
         * with the device driver team (that is where we got this list in
         * the first place).
         *
         * Someday, I/O retry will be handled in the device drivers and this
         * code will be redundant.
         *
         * NOTE:  This error list may be architecture dependent (porting to
         *        to a new platform may change some of these values).
         */
        switch((bp->b_eei & 0x0000ff00)) {
        /*   EEI_BBR_DONE          -- Informational message indicating that
                                      Bad Block Replacement was performed */
        /*   EEI_DEVSOFT_FAILURE   -- Informational message indicating that
                                      a soft error occurred, but the device or
                                      the driver took care of it */
        case EEI_CNTBUSY_FAILURE:  /* HBA/Controller Busy (retriable) */
        case EEI_DEVBUSY_FAILURE:  /* Device is busy (retriable) */
        case EEI_DEVBIO_FAILURE:   /* Device/bus I/O error (retriable) */
        case EEI_ABORTIO_FAILURE:  /* Abort/Timed out I/O occurred */
        case EEI_CNTRLR_FAILURE:   /* HBA/Controller failure (retriable) */
        case EEI_RESERVE_PENDING:  /* Shared Device Re-reserve pending
                                      it might be a transient cluster
                                      condition */
        /*   EEI_DEVPATH_FAILURE:  -- Device not there (powered off or maybe
                                      the brick was pulled from the rack and
                                      even put back in the rack?) this
                                      breaks the path in the driver to the
                                      device and until an outside force does
                                      something like an open/close on the
                                      device, it will continue to return
                                      this error.  One day, if the device
                                      drivers do not provide support for hot
                                      swapping devices, then we could
                                      consider making this retry logic more
                                      complex by doing an open/close on the
                                      device, but for now, that is beyond
                                      the scope of this project. */
        case EEI_DEVPATH_RESET:    /* Device/bus reset occurred */
        case EEI_DEVPATH_CONFLICT: /* Device access denied (path conflict)
                                      it might be a transient cluster
                                      condition */
        case EEI_DEVHARD_FAILURE:  /* Device hardware error (hard error) 
                                      Some hard failures are transient
                                      conditions */
            error_maybe_retriable = TRUE;   /* used by error messages */

            /* 
             * Are we within the I/O retry limits? 
             */
            if ( AdvfsIORetryControl > 0 && 
                 AdvfsIORetryControl > iop->ioRetryCount ) {

                mutex_lock( &vdp->devQ.ioQLock );
                sts = sendtoiothread(vdp,RETRY_IO,bp);
                mutex_unlock( &vdp->devQ.ioQLock );

                if ( sts == 0 ) {
                    /*
                     * The message has been queued for the bs_io_thread
                     * to handle.
                     */
                    return;
                }
            }
        }

        /*
         * This I/O was not retried, so log the I/O failure
         */
        setp = iop->bsBuf->bfAccess->bfSetp;
        if (setp->fsnp != NULL) {
            mp = setp->fsnp->mountp;
            if (mp != &dead_mount)
                fileset_is_mounted = TRUE;
        }
        if (iop->bsBuf->bfAccess->tag.num > 1) {
            non_metadata_file = TRUE;
        }

        /*
         * The following b_eei error codes correspond to a cable pull or
         * a hard device error.
         */

        if ((bp->b_eei == EEI_DEVPATH_FAILURE) ||
            (bp->b_eei == EEI_DEVPATH_RESET)   ||
            (bp->b_eei == EEI_DEVHARD_FAILURE) ||
            (bp->b_eei == EEI_ABORTIO_FAILURE))  {
            hardware_related_error = TRUE;
        }

        bs_printf("AdvFS I/O error:\n");
        if ((bp->b_eei == EEI_ADVFS_DMN_PANIC) && (bp->b_flags & B_READ)){
            bs_printf("    A read failure occurred "
                      "- the AdvFS domain is inaccessible (paniced)\n");
            }
        if (fileset_is_mounted) {
            bs_printf("    Domain#Fileset: %s\n", 
                   mp->m_stat.f_mntfromname);
            bs_printf("    Mounted on: %s\n", mp->m_stat.f_mntonname);
        }
        bs_printf("    Volume: %s\n", vdp->vdName);
        bs_printf("    Tag: 0x%08x.%04x\n", 
               iop->bsBuf->bfAccess->tag.num, 
               iop->bsBuf->bfAccess->tag.seq);
        bs_printf("    Page: %u\n", iop->bsBuf->bfPgNum);
        bs_printf("    Block: %u\n", iop->blkDesc.vdBlk);
        bs_printf("    Block count: %u\n",
               iop->desCnt ? iop->totalBlks : iop->numBlks);
        bs_printf("    Type of operation: %s\n",
               iop->bsBuf->lock.state & WRITING ? "Write" : "Read");
        bs_printf("    Error: %d   (see /usr/include/errno.h)\n",
                  bp->b_error ? bp->b_error : EIO);
        bs_printf("    EEI: 0x%x (%s)\n", bp->b_eei,
                error_maybe_retriable ? 
                "retries may help" : 
                "Advfs cannot retry this" );
        bs_printf("    AdvFS initiated retries: %d\n", iop->ioRetryCount);
        bs_printf("    Total AdvFS retries on this volume: %d\n",
               vdp->vdRetryCount );
        if (hardware_related_error) {
            bs_printf(
                  "    I/O error appears to be due to a hardware problem.\n");
            bs_printf(
                  "    Check the binary error log for details.\n");
        }
        if (fileset_is_mounted && non_metadata_file) {
            bs_printf("    To obtain the name of the file on which\n");
            bs_printf("    the error occurred, type the command:\n");
            bs_printf("      /sbin/advfs/tag2name %s/.tags/%d\n",
                mp->m_stat.f_mntonname,
                iop->bsBuf->bfAccess->tag.num);
        }
        if ( error_maybe_retriable && iop->ioRetryCount == 0 ) {
            bs_printf("\n");
            bs_printf("    It is possible that enabling AdvFS I/O\n");
            bs_printf("    retries may have allowed this I/O to succeed.\n");
        }
        if ( error_maybe_retriable ) {
            bs_printf("\n");
            bs_printf("    To modify AdvFS retries, adjust the\n");
            bs_printf("    'AdvfsIORetryControl' sysconfigtab entry:\n");
            bs_printf("\n");
            bs_printf("        "
                      "/sbin/sysconfig -q advfs AdvfsIORetryControl\n");
            bs_printf("        "
                      "/sbin/sysconfig -r advfs AdvfsIORetryControl=nn\n");
            bs_printf("\n");
            bs_printf("    nn == 0 - No AdvFS retries\n");
            bs_printf("    1 to %2d - AdvFS will retry up to the\n",
                    ADVFS_IO_RETRY_MAX);
            bs_printf("              specified number of times.\n");
            bs_printf("              This is in addition to any device\n");
            bs_printf("              driver retries.\n");
            bs_printf("\n");
            bs_printf("    Current setting of AdvfsIORetryControl: %ld\n",
                   AdvfsIORetryControl);
        }

        if( bp->b_error ) {
            sts = (statusT)bp->b_error;
        } else {
            sts = (statusT)EIO;
        }
    }

    /* We are done with the bp->b_eei setting, so clear. */
    bp->b_eei = 0;

#ifdef ADVFS_DEBUG
    mutex_lock( &vdp->vdIoLock );
    if( sts == EOK && vdp->errorCount ) {
        
        /*
         * Determine whether errorCount should be
         * decremented.
         */
        if( vdp->errorFlag & DEF_READ ) {
            if( iop->bsBuf->lock.state & READING ) {
                if( vdp->errorFlag & DEF_META ){
                    if( iop->bsBuf->bfAccess->dataSafety > BFD_NO_NWR ) {
                        vdp->errorCount--;
                    }
                } else if( vdp->errorFlag & DEF_LOG ) {
                    if( iop->bsBuf->bfAccess->dataSafety == BFD_NO_NWR ) {
                        vdp->errorCount--;
                    }
                } else {
                    if( iop->bsBuf->bfAccess->dataSafety < BFD_NO_NWR ) {
                        vdp->errorCount--;
                    }
                }
            }
        } else if( vdp->errorFlag & DEF_WRITE ) {
            if( iop->bsBuf->lock.state & WRITING ) {
                if( vdp->errorFlag & DEF_META ){
                    if( iop->bsBuf->bfAccess->dataSafety > BFD_NO_NWR ) {
                        vdp->errorCount--;
                    }
                } else if( vdp->errorFlag & DEF_LOG ) {
                    if( iop->bsBuf->bfAccess->dataSafety == BFD_NO_NWR ) {
                        vdp->errorCount--;
                    }
                } else {
                    if( iop->bsBuf->bfAccess->dataSafety < BFD_NO_NWR ) {
                        vdp->errorCount--;
                    }
                }
            }
        }

        /*
         * Generate the error
         */
        if( vdp->errorCount == 0 ) {
            sts = EIO;
            if( vdp->errorFlag & DEF_REPEAT ) {
                vdp->errorCount = vdp->errorRepeat;
            } else {
                vdp->errorFlag = DEF_NONE;
            }
            setp = iop->bsBuf->bfAccess->bfSetp;
            aprintf("advfs induced I/O error: "
                    "setId 0x%08x.%08x.%x.%04x  tag 0x%08x.%04x  page %u\n",
                setp->bfSetId.domainId.tv_sec,
                setp->bfSetId.domainId.tv_usec,
                setp->bfSetId.dirTag.num,
                setp->bfSetId.dirTag.seq,
                iop->bsBuf->bfAccess->tag.num, iop->bsBuf->bfAccess->tag.seq, 
                iop->bsBuf->bfPgNum );
            aprintf( "\tvd %u  blk %u  blkCnt %u\n", 
                iop->blkDesc.vdIndex, iop->blkDesc.vdBlk, 
                iop->desCnt ? iop->totalBlks : iop->numBlks );
            aprintf( "\t%s error = EIO\n", 
                iop->bsBuf->lock.state & WRITING ? "write" : "read" );
        }
    }
    mutex_unlock( &vdp->vdIoLock );
#endif /* ADVFS_DEBUG */
    /*
     * If a write to a metadata file fails, call domain_panic().
     * That routine will decide whether to panic the domain or
     * the system.  Directories are considered to be metadata files
     * since they have to correlate with the tag directories.  I/O
     * errors to user atomic write data logging files will also trigger
     * a domain panic since we can not guarantee the atomicity of the
     * write anymore.
     */
    if ((sts != EOK) &&
        (iop->bsBuf->lock.state & WRITING) &&
        ((iop->bsBuf->bfAccess->dataSafety == BFD_NO_NWR) ||
         (iop->bsBuf->bfAccess->dataSafety == BFD_FTX_AGENT) ||
         (iop->bsBuf->bfAccess->dataSafety == BFD_FTX_AGENT_TEMPORARY))) {

        if (hardware_related_error) {
            dpanic_flags |= DMN_PANIC_IO_CONNECTIVITY;
        } else {
            dpanic_flags |= DMN_PANIC_IO_OTHER;
        }

        domain_panic_type(dmnP,
            "bs_osf_complete: metadata write failed", dpanic_flags);

        sts = EOK; /* ignore error */
    }

    active_time  = (time_t) bp->b_iostats.acttime;
    waiting_time = (time_t) bp->b_iostats.pendtime;
    b_iolen      = bp->b_iostats.devqlen;
 
    /* Need to save starting page address before we release struct buf,
     * which used to be done right here, but now for easier debugging
     * it is now done later down below.
     */
    taddr = bp->b_un.b_addr;

    mutex_lock( &vdp->devQ.ioQLock );
    /*
     * Check if the device is really active.
     */
    if ( vdp->vdIoOut == 0) {
        ADVFS_SAD0("bs_osf_complete: vdIoOut is zero");
    }

    /* If the device driver is supplying feedback on I/O statistics,
     * use it.  Otherwise, we must do our own sampling to determine
     * how fast to feed I/Os to the device.
     */
    if ( active_time && Advfs_allow_driver_io_feedback ) {
        if  ( waiting_time <
              ((active_time + waiting_time) >> Advfs_biostats_low_shift) ) {
            /* Our 'wasted' wait time was less than the threshold,
             * so see if we can increase the potential length of
             * the device queue.
             */
            if ( b_iolen >=  vdp->devQ.lenLimit ) {
                vdp->devQ.lenLimit = b_iolen + Advfs_biostats_devq_delta;
            }
        } else if ( waiting_time >
                  ((active_time + waiting_time) >> Advfs_biostats_high_shift)){
            /* Our wait time is excessive; cut down the size
             * of the device queue.
             */
            if ( b_iolen <=  vdp->devQ.lenLimit ) {
                vdp->devQ.lenLimit = b_iolen - (3 * Advfs_biostats_devq_delta);
            }
            if ( vdp->devQ.lenLimit < (3 * Advfs_biostats_devq_delta) )
                 vdp->devQ.lenLimit = 3 * Advfs_biostats_devq_delta;
        }
        vdp->vd_sample_raw_count = 0;    /* marker for looking at dumps */
    } else if ( (lbolt - vdp->vd_lbolt) >= (hz << bio_sample_shift) ) {
        /* The sampling period has ended. Based on the # of I/Os
         * processed per unit time averaged throughout the sampling
         * period, set vdT.devQ.lenLimit to accommodate requests for
         * y seconds.  The value for y is based on the setting of
         * bio_time_shift.  By default, devQ.lenLimit will accommodate
         * the number of buffers processed per 2 seconds.
         */
        int n = bio_sample_shift-bio_time_shift;
        int nbufs = (iop->desCnt > 0) ? iop->desCnt : 1;
        vdp->vd_lbolt = lbolt;
        if ( (vdp->vd_sample_raw_count >> n) > vdp->devQ.lenLimit ) {
            vdp->devQ.lenLimit = vdp->vd_sample_raw_count >> n;
        }
        vdp->vd_sample_raw_count = (iop->desCnt > 0) ? iop->desCnt : 1;
    } else {
        /* Increment raw count of I/Os completed this sampling period */
        vdp->vd_sample_raw_count += (iop->desCnt > 0) ? iop->desCnt : 1;
    }

    /*
     * First note that bp is the OSF buf struct pointer
     * while iop is the AdvFS I/O descriptor.
     *
     * We use the fwd pointer of the iop to link
     * multiple AdvFS I/O descriptors that may be associated
     * with a single OSF buf struct.  If the correspondence
     * is one to one, the fwd field is NULL.
     */

    if( iop->desCnt > 0 ) {
        /*
         * Deallocate devices' vm
         * The allocation scheme is strictly internal
         * to MegaSafe.
         */
        pages = (ulong_t)iop->totalBlks * BS_BLKSIZE / PAGE_SIZE;
        iop->totalBlks = 0;
        iop->consolidated = 0;
        len = iop->desCnt;
        iop->desCnt = 0;
    } else {
        len = 1;
    }

    /* since the vm page can change between IO operations, we want to
     * make sure the data address isn't reused.  originally I did this
     * clearing in call_disk, but now it is here for debugging.
     * only the first targetAddr is set on cache IO using ubc pages.
     * The check for vmpage (only set for ubc) excludes raw/directIO
     * just for debug ability since they don't use the ubc cache.
     * Up to here we could: MS_SMP_ASSERT(taddr == iop->targetAddr);
     */
    if (iop->bsBuf->vmpage)
        iop->targetAddr = NULL;

    /* Remove ioDesc from devQ; if this is a consolidated I/O, all
     * are removed here and left as a local chain no longer on the
     * devQ for processing after the vdIoLock is dropped.
     */
    first_iop = last_iop = iop;
    for (i=1; i < len; i++ ) {
        last_iop = last_iop->fwd;
    }
    first_iop->bwd->fwd = last_iop->fwd;
    last_iop->fwd->bwd  = first_iop->bwd;
    first_iop->bwd = NULL;
    last_iop->fwd  = NULL;
    vdp->devQ.ioQLen -= len;

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->devQ, TRUE );

    /* If the device queue is less than half full, and we have buffers
     * on the queues that could go out, have the background I/O thread
     * get some more going. Do this under the devQ.ioQLock.
     */
    if ((vdp->devQ.ioQLen <= (vdp->devQ.lenLimit/2)) &&
        !vdp->start_io_posted ) {

        if ((vdp->consolQ.ioQLen || vdp->blockingQ.ioQLen ||
             vdp->flushQ.ioQLen  || vdp->ubcReqQ.ioQLen)) {
            if ( !sendtoiothread(vdp,START_MORE_IO,NULL) ) {
                vdp->start_io_posted=1;
            }
        } else if (!vdp->devQ.ioQLen && SS_is_running) {
            /* no ios on any queues, run vfast. */
            ss_startios(vdp);
            vdp->ssVolInfo.ssIOCnt=0;
        }
    }

    mutex_unlock( &vdp->devQ.ioQLock );

    if (SS_is_running &&
       (dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED) &&
       (dmnP->ssDmnInfo.ssPctIOs) &&
       (vdp->ssVolInfo.ssIOCnt++ >= dmnP->ssDmnInfo.ssPctIOs)) {
        /* occasionally run vfast when system stays busy */
        ss_startios(vdp);
        vdp->ssVolInfo.ssIOCnt=0;
    }

    /* Now, pull a consolidated I/O apart and call bs_io_complete()
     * for each buffer to wake any waiters.
     */
    while( len-- ) {
        next = iop->fwd;
        MS_SMP_ASSERT( iop->ioQ == DEVICE );
        iop->fwd = iop->bwd = NULL;
        iop->consolidated = 0;
        iop->ioQ = NONE;
#ifdef ADVFS_SMP_ASSERT
        iop->bsBuf->ioqLn=SET_LINE_AND_THREAD(__LINE__);
#endif

        if( sts != EOK ) {
            /*
             * Only the first error result is
             * reported back to the bsBuf.
             */
            if( iop->bsBuf->result == EOK ) {
                iop->bsBuf->result = sts;
            }
        }
        /* Call bs_io_complete() when all of the buffer's ioDescT IO's
         * are done. That routine will release the bufLock and make the
         * buffer elible for immediate memory deallocation. So,
         * never reference the buffer after returning from bs_io_complete.
         */
        if( --iop->ioCount == 0 ) {
            mutex_lock(&iop->bsBuf->bufLock);
            if( --iop->bsBuf->ioCount == 0 ) {
                /* IO done integrity checking is done here instead of
                 * bs_io_complete() since we don't want to check on
                 * aborted/fake completions.  The clearing of the
                 * bsBuf->metaCheck is in bs_io_complete() for the
                 * opposite reason.  Checking after a read or write
                 * is controlled by the thread staging the IO.
                 */
                if (iop->bsBuf->metaCheck)
                    bs_io_metacheck(iop->bsBuf);
                bs_io_complete( iop->bsBuf, &resetfirst );
            }
            else {
                mutex_unlock(&iop->bsBuf->bufLock);
            }
        }
        iop = next;
    }

    /* If bs_io_complete set resetfirst then the meta data for the oldest
     * log entry just got written. We can calculate a new oldest log entry.
     * Since the dirtyBufLa value is maintained on a domain-wide basis, 
     * seize the dmnP->lsnLock to be sure that bs_io_complete() running 
     * for another vd doesn't try to modify this at the same time we try 
     * to read it.  Two threads coming thru this code at the same time 
     * for different vd's in the same domain may race to reset this value, 
     * but the worst case is that they both set it to the same value.
     */
    if ( resetfirst ) {
        mutex_lock( &dmnP->lsnLock );
        resetfirstrec(dmnP);
        mutex_unlock( &dmnP->lsnLock );
    }

    /* Mark the disk inactive if this completes all outstanding I/Os.
     * Only do this after we have finished making changes to bfap/dmnP
     * structures in bs_io_complete so flush waiters won't operate on
     * or free the structures before we are done.
     */
    mutex_lock( &vdp->devQ.ioQLock );
    vdp->vdIoOut--;
    if( vdp->gen_active == 0 && vdp->vdIoOut == 0 ) {
        lk_signal( lk_set_state( &vdp->active, INACTIVE_DISK ), &vdp->active );
    }
    mutex_unlock( &vdp->devQ.ioQLock );

    /* release the struct buf, only kept this long for easier debugging */
    freeosfbuf( bp );

    if( pages ) {
        pmap_mmu_unload( (vm_offset_t)taddr, 
                         (vm_size_t)pages << PAGE_SHIFT, 
                         TB_SYNC_ALL );
        /* Return any allocated virtual memory */
        bp_map_free( (vm_offset_t)taddr, pages ); 
    }
}

/*
 * bs_printf - Created for bs_osf_complete() because it used AdvfsIoError to
 *             decide if it should call printf(), aprintf(), or log() to
 *             issue its I/O failure messages.
 *
 *             This routine is an attempt to consolidate all of that into
 *             one routine so that we do not have 3 copies of essentially
 *             the same message text to maintain.
 *
 *             WARNING:  Because this is called by bs_osf_complete(), which
 *                       is a lightweight context thread, this code _MUST_
 *                       not block, and we do not want to use too much stack
 *                       space (as a light weight context thread we are on
 *                       someone else's stack) so PRFMAX is limited to 80
 *                       since the bs_osf_complete() lines are all less than
 *                       80 bytes.
 */
#define PRFMAX 80
void
bs_printf(char *fmt, ...)
{
    va_list valist;
    int n;
    u_char buf[PRFMAX];

    va_start(valist, fmt);
    n = prf(&buf[0], &buf[PRFMAX], (u_char *)fmt, valist);
    va_end(valist);
    if (n <= 0) {
        return;
    }
    buf[n] = '\0';

    /*
     * (0)  - No printf and no error logging of I/O errors
     * (1)  - Call printf (console & error logger)
     * (2)  - Call aprintf (async console & error logging)
     * (3)  - Call log (error logging only)
     */
    switch( AdvfsIoError ) {
    case 1:           printf("%s", buf); break;
    case 2:          aprintf("%s", buf); break;
    case 3: log(LOG_WARNING, "%s", buf); break;
    }
}


void
msfs_iodone( struct buf *bp )
{
    int s;
    struct processor *pr;

    s = splbio();
    pr = current_processor();
    if( pr->MsfsIodoneBuf != BUF_NULL ) {
        binstailfree( bp, pr->MsfsIodoneBuf );
    } else {
        pr->MsfsIodoneBuf = bp;
        bp->av_back = bp->av_forw = bp;
    }
    lwc_cpu_post(LWC_PRI_MSFS_UBC);
    (void) splx(s);
}


void 
msfs_async_iodone_lwc(void)
{
    int sj;
    struct buf *bp;
    struct processor *pr;

    pr = current_processor();
    for( ;; ) {
        sj = splbio();
        /*
         * Only exit when we're out of buffers.
         */
        if( (bp = pr->MsfsIodoneBuf) == BUF_NULL ) {
            (void) splx(sj);
            return;
        }

        /*
         * Grab a buffer from the queue.
         */
        if( bp->av_forw == bp ) {
            pr->MsfsIodoneBuf = BUF_NULL;
        } else  {
            pr->MsfsIodoneBuf = bp->av_forw;
            bremfree( bp );
        }
        (void) splx(sj);

        bs_osf_complete( bp );
    }
}

void 
msfs_io_init(void)
{
    lwc_create( LWC_PRI_MSFS_UBC, &msfs_async_iodone_lwc );
}


/*
 * Take a list of ioDesc.  Map them into a contiguous
 * address space.  Return -1 if the mapping fails
 * otherwise return 0;
 */

int
list_map( ioDescT *ioList, 
          vm_offset_t starting_virtual_address )
{
    ioDescT *iop = ioList;
    u_char *virtual_addr;
    vm_offset_t phys;
    int len = ioList->desCnt;
    int ret;

    virtual_addr = (u_char *)starting_virtual_address;

    while( len-- ) {

        iop->consolidated = 1;
        iop->ioCount = 1;

        /* Map buffer; this code is modeled on bp_map().
         * By this time we must have ref'd the vm page and set
         * the bsbuf->vmpage pointer in prep for I/O.
         */
        phys = page_to_phys( iop->bsBuf->vmpage );
        ret = pmap_mmu_load( (vm_offset_t)virtual_addr,
                             phys, 
                             (vm_size_t)PAGE_SIZE,
                             VM_PROT_READ|VM_PROT_WRITE, 
                             TB_SYNC_NONE );
        if( ret != KERN_SUCCESS ) {
            goto error;
        }
        virtual_addr += PAGE_SIZE;
        iop = iop->fwd;
    }
    return( 0 );

error:
    for( iop = ioList, len = ioList->desCnt; len; len-- ) {
        iop->consolidated = 0;
        iop->ioCount = 0;
    }

    if ( virtual_addr > (u_char *)starting_virtual_address ) {
        pmap_mmu_unload( starting_virtual_address,
                         virtual_addr - (u_char *)starting_virtual_address,
                         TB_SYNC_ALL );
    }

    return( -1 );
}

typedef enum { BLKQ, UBCRQ, FLSHQ, CONSLQ } qtypeT;

/*
 * consecutive_list_io
 *
 * Consolidate a list of buffers from the supplied queue.
 * We assume that the buffers have already been sorted.  
 *
 * SMP: 1. This routine is called with the devQ.ioQLock and the lock for the
 *         incoming queue held.
 *      2. Each ioDesc struct is removed from the I/O queue, and its bsBuf
 *         is marked BUSY.
 *      3. The bp->bufLock is seized while setting the BUSY bit.
 */

static void
consecutive_list_io( 
    ioDescHdrT *qhdr,  /* in */
    struct vd *vdp,    /* in */
    int max_buffers,   /* in */
    ioDescT **ioList,  /* out */
    qtypeT qindex,     /* in */
    int *locks_held    /* in/out */
    )
{
    ioDescT *start = qhdr->fwd;
    int len = qhdr->ioQLen;
    ioDescT *last, *this, *iop;
    int rw;
    int count;
    lbnT blkcnt;
    int maxio;
    int pages;
    vm_offset_t virtual_mem_start;
    vm_offset_t bp_map_alloc( int, boolean_t );
    void        bp_map_free( vm_offset_t, int );
    int         map_status = -1;

    MS_SMP_ASSERT(max_buffers > 1);
 
    MS_VERIFY_IOQUEUE_INTEGRITY( qhdr, TRUE );
     
    MS_SMP_ASSERT(SLOCK_HOLDER(&vdp->devQ.ioQLock.mutex));
    MS_SMP_ASSERT(SLOCK_HOLDER(&qhdr->ioQLock.mutex));
    MS_SMP_ASSERT(locks_held[qindex] != 0);

    /* Position ourselves to point to first ioDesc that can have IO done. */
    if (qhdr == &vdp->consolQ) {
        /* Skip any buffers on consolQ that are marked for removal;
         * try walking this list without locking to speed things up.
         */
retry:
        while ( len && start->bsBuf->lock.state & REMOVE_FROM_IOQ ) {
            start = start->fwd;
            len--;
        } 
        if ( len == 0 ) {
            *ioList = NULL;
            return;
        } else {  
            mutex_lock( &start->bsBuf->bufLock );
            if ( start->bsBuf->lock.state & REMOVE_FROM_IOQ ) {
                /* Whoops, got set while we were locking */
                mutex_unlock( &start->bsBuf->bufLock );
                start= start->fwd;
                len--;
                goto retry;
            }

            /* Setup the UBC page for IO. This obtains the UBC object lock. 
             * Skip this buffer IO if the UBC page's pg_busy flag is already
             * set. Most likely, we lost a race with UBC staging the page
             * for IO prior to calling putpage. We also skip the bsbuf if
             * the vm_page is in an exchange transition, but that is ok
             * since eventually we will see it again on a future pass.
             * RAWRW requests never go on the consolQ.
             */
            MS_SMP_ASSERT(!(start->bsBuf->lock.state & RAWRW));
            if (advfs_page_get(start->bsBuf, UBC_GET_BUSY)) {
                mutex_unlock( &start->bsBuf->bufLock );
                start= start->fwd;
                len--;
                goto retry;
            }
#ifdef ADVFS_SMP_ASSERT
            start->bsBuf->busyLn = SET_LINE_AND_THREAD(__LINE__);
            start->bsBuf->ioqLn = -1;
#endif

            vdp->devQ.queue_cnt++;
            start->bsBuf->lock.state |= BUSY;
            start->bsBuf->bufDebug &= ~BSBUF_WAITQ;
            start->bsBuf->sync_stamp = 0;
            start->ioQ = DEVICE;
            mutex_unlock( &start->bsBuf->bufLock );
        }
    } else {
        /* Just take first one off blocking, flush, or ubcReq queue */
        mutex_lock( &start->bsBuf->bufLock );
        vdp->devQ.queue_cnt++;
        start->bsBuf->lock.state |= BUSY;
        start->bsBuf->bufDebug &= ~BSBUF_WAITQ;
        start->bsBuf->sync_stamp = 0;
        start->ioQ = DEVICE;
        mutex_unlock( &start->bsBuf->bufLock );
    }

    /* Account for first ioDesc now */
    rw = start->bsBuf->lock.state & (READING | WRITING);
    count = 1;
    len--;
    max_buffers--;
    blkcnt = start->numBlks;        /* 512-byte blocks */
    maxio = rw & WRITING ? vdp->wrmaxio : vdp->rdmaxio;

    last = start;
    this = start->fwd;

    /* walk thru remaining ioDesc on this queue checking for appropriateness
     * of consolidating their IOs.  Compare the values of each ioDesc with
     * that of the previous buffer.
     */
    while ( len--             &&      /* more ioDesc's on incoming queue */
            (blkcnt < maxio)  &&      /* have not exceeded max IO size */
            (max_buffers > 0) &&      /* do not exceed requested # bufs */
            (vdp->devQ.ioQLen + count < vdp->devQ.lenLimit) ) {

        ioDescT *prev = this->bwd;
        mutex_lock( &this->bsBuf->bufLock );

        /*
         * We can consolidate I/O if:
         * 1.  next buffer is not in REMOVE_FROM_IOQ state OR on flushq or
         *     blockingQ or ubcReqQ.  (we don't want to do IO for consolQ 
         *     buffers with REMOVE set.)
         * 2.  neither buffer is for raw or directIO
         * 3.  its first block is consecutive to this buffer's last block
         * 4.  it represents I/O in the same direction (reading or writing)
         *     as this buffer (if reading, noReadMask must be clear), and
         * 5.  we have less than the max number of blocks for a single I/O.
         *
         * The IO request must be for a page size multiple number of bytes.
         * Also, for ubc the I/O request must be a request for *all* the vm
         * pages to be read. 
         * If Presto is enabled for this device, then we don't want to 
         * consolidate log or metadata writes since Presto will only cache 
         * 1 to 8 Kb writes.
         * 
         * Note: Files that are of dataSafety type BFD_FTX_AGENT
         *       do not have their I/O consolidated here.  Testing
         *       has shown that consolidating I/O for those files
         *       here will cause "Can't clear a bit twice" errors
         *       reliably in the smpfs mkdir_rmdir_2_exer test.
         */
        if ((((this->bsBuf->lock.state & REMOVE_FROM_IOQ) == 0)  ||
              ((qhdr == &vdp->flushQ)  || 
               (qhdr == &vdp->ubcReqQ) || 
               (qhdr == &vdp->blockingQ))) &&
            (prev->blkDesc.vdBlk + prev->numBlks == this->blkDesc.vdBlk) &&
            ( (!(this->bsBuf->lock.state & RAWRW )) &&
              (!(prev->bsBuf->lock.state & RAWRW )) )                    &&
            (  ((this->bsBuf->lock.state & WRITING) == rw)  ||
              ((this->bsBuf->lock.state & READING) == rw)) &&
            ((this->numBlks * BS_BLKSIZE & (ADVFS_PGSZ - 1)) == 0)       &&

           (!(prattached && prenabled(vdp->devVp->v_rdev)) ||
           (((prev->bsBuf->bfAccess->dataSafety == BFD_NIL) ||
              (prev->bsBuf->bfAccess->dataSafety == BFD_SYNC_WRITE)) &&
            ((this->bsBuf->bfAccess->dataSafety == BFD_NIL) ||
               (this->bsBuf->bfAccess->dataSafety == BFD_SYNC_WRITE))))
           ) {

           if (qhdr == &vdp->consolQ) {
                /* For consolq queue buffers, setup the UBC page for IO.
                 * But skip the buffer IO if these flags are already set.
                 * That means either a racing UBC routine setup the page
                 * for IO but has yet to call the filesystem putpage or
                 * some thread (flush, pin) already set the IO_TRANS flag.
                 * We also skip this buffer if the page is in an exchange
                 * transition, but that is ok since eventually we will see
                 * it again on a future pass.
                 */
                if (advfs_page_get(this->bsBuf, UBC_GET_BUSY)) {
                    /* Next buffer will not be block contiguous so break. */
                    mutex_unlock( &this->bsBuf->bufLock );
                    break;
                }
#ifdef ADVFS_SMP_ASSERT
                this->bsBuf->busyLn = SET_LINE_AND_THREAD(__LINE__);
                this->bsBuf->ioqLn = -1;
#endif
            }

            /* Must mark this buffer BUSY before we drop its lock to
             * check the next buffer.
             */
            vdp->devQ.queue_cnt++;
            this->bsBuf->lock.state |= BUSY;
            this->bsBuf->bufDebug &= ~BSBUF_WAITQ;
            this->bsBuf->sync_stamp = 0;
            this->ioQ = DEVICE;
            mutex_unlock( &this->bsBuf->bufLock );

            count++;                         /* keep track of # to consol */
            max_buffers--;
            blkcnt += this->numBlks;
            last = this;                     /* keep track of last one */
            this = this->fwd;
       }
       else {
            /* can't do any more */
            mutex_unlock( &this->bsBuf->bufLock );
            break;
       }
    }

    /* Remove the ioDesc structs from the queue */
    qhdr->ioQLen -= count;
    start->bwd->fwd = last->fwd;
    last->fwd->bwd = start->bwd;
    start->bwd = last->fwd = NULL;
    MS_VERIFY_IOQUEUE_INTEGRITY( qhdr, TRUE );

    /*
     * If we have consolidated I/O, allocate some contiguous virtual 
     * memory and map these buffers into that address space.
     */
    if ( count > 1 ) {

        start->desCnt = count;

        /* Call bp_map_alloc() to get n pages of contiguous virtual memory
         * from the vm subsystem. If either the allocation or mapping fail,
         * then we have to return one unconsolidated I/O and put the rest
         * of the ioDesc struct onto the blockingQ.
         */
        pages = howmany(blkcnt, ADVFS_PGSZ_IN_BLKS);
        virtual_mem_start = bp_map_alloc(pages,   /* # pages to alloc */
                                         FALSE);  /* don't allow to sleep */ 
        if ( virtual_mem_start ) { 

            /* Map the buffers contiguously starting at virtual_mem_start */
            map_status = list_map( start, virtual_mem_start );
        }

        if ( map_status < 0 ) {

            /* Record the consolidation failure in domain statistics */
            start->bsBuf->bfAccess->dmnP->bcStat.consolAbort++;

            /* If the incoming queue is not the blocking queue then we need to
             * drop the incoming locks so as to get the blocking queue lock in
             * order. Note: This is safe because the ioDescTs in the chain at
             * start are protected from the removal routines because they are
             * marked as being on the devQ and the removal routines do not
             * operate on the device queue. The chain at start has already been
             * removed from the incoming queue so it cannot be moved to another
             * queue while the qhdr->ioQLock is dropped. Racing pins and refs
             * will pend on the BUSY buffer associated with each ioDescT.
             */
            if ( qhdr != &vdp->blockingQ ) {
                mutex_unlock( &qhdr->ioQLock );
                locks_held[qindex] = 0;
                mutex_unlock( &vdp->devQ.ioQLock );
                mutex_lock( &vdp->blockingQ.ioQLock );
                locks_held[BLKQ] = 1;
                mutex_lock( &vdp->devQ.ioQLock );
            }

            /* Put all except first descriptor on the blockingQ.  */
            for( iop = start->fwd; iop != last->fwd; iop = iop->fwd ) {
                iop->ioQ = BLOCKING;
            }
            vdp->blockingQ.fwd->bwd = last;
            last->fwd = vdp->blockingQ.fwd;
            start->fwd->bwd = (ioDescT *)&vdp->blockingQ;
            vdp->blockingQ.fwd = start->fwd;
            vdp->blockingQ.ioQLen += (count - 1);
            count = 1;
            last = start;

            MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->blockingQ, TRUE );
            if ( qhdr != &vdp->blockingQ ) {
                mutex_unlock( &vdp->blockingQ.ioQLock );
                locks_held[BLKQ] = 0;
            }

            /* Return any allocated virtual memory */
            if ( virtual_mem_start ) {
                bp_map_free( virtual_mem_start, start->desCnt ); 
            }

            start->desCnt = 0;
            start->totalBlks = 0;

        } else {
            start->totalBlks = blkcnt;
            /* Pass virtual_mem_start back for call_disk only
             * when we actually combined multiple buffers for one IO.
             */
            start->targetAddr = (unsigned char *)virtual_mem_start;
        }
    }

    *ioList = start;
    start->bwd = last;
    last->fwd = start;

    return;
}

/*
#define CLUSTER_STATS 1
*/

#ifdef CLUSTER_STATS
struct ioclusterstat {
        int cluster_total[10];
        int cluster_read[10];
        int cluster_write[10];
        int reg_cluster_read[10];
        int reg_cluster_write[10];
        int metadata_cluster_read[10];
        int metadata_cluster_write[10];
} advfs_clusterstats;

int disable_advfs_hints = 0;
#endif


/*
 * call_disk
 *
 * Fill balance of struct buf and call strategy
 * routine for disk.
 *
 * SMP: can sleep, no simple locks held on entry
 */

void
call_disk( 
        struct vd *vdp, 
        int ioAmt, 
        lbnT blk,
        ioDescT *ioList
        )
{
    struct buf *bp;
    bfAccessT *bfAccess;
    domainT *dmnP;
#ifdef CLUSTER_STATS
    int desCnt;
#endif

    /* get and clear an osf struct buf */
    bp = getosfbuf();
    ioList->devosfbuf = bp;
    ioList->ioQtime = current_processor()->p_lbolt;
    ioList->ioQln = SET_LINE_AND_THREAD(__LINE__);

    /*
     * Fill OSF buf struct.
     */
    MS_SMP_ASSERT(ioList->targetAddr);
    bp->b_un.b_addr = (caddr_t)ioList->targetAddr;

    if (ioList->bsBuf->directIO) {
        /* Workaround to fix a panic with DirectIO user address
         * requests passed down to a Prestoserve enabled 
         * PCI NVRAM board.  Prestoserve will 
         * reject DirectIO requests and send them directly to
         * the underlying disk driver.
         * A preferred solution is for Prestoserve to convert
         * the user address into a physical KSEG address 
         * before sending the request to the PCI NVRAM board.
         * The hardware only deals with physical addresses.
         * This B_RAW flag can be removed when Prestoserve
         * address conversion is supported.
         *
         * We set B_PHYS for drivers that use the b_proc field.
         *
         * Check to be sure that we do full-sector transfers
         * on directIO transfers.
         */
        bp->b_proc = ioList->bsBuf->procp;
        bp->b_flags = B_RAW | B_PHYS;  
        MS_SMP_ASSERT( !(ioAmt % BS_BLKSIZE) );
    }

    bp->b_bcount = ioAmt;
    bp->b_dev = vdp->devVp->v_rdev;
    bp->b_blkno = blk;
    bp->b_iodone = msfs_iodone;
    bp->b_pagelist = (struct vm_page *)ioList;
    /* bp->b_vp = 0;  No device vnode, to thwart biodone */
    /* Set driver handles out of vnode */
    bp->b_drv_handle = vdp->devVp->v_drv_handle;

    /* set up b_iostats for feedback from the driver */
    bp->b_iostats.devqlen = vdp->devQ.ioQLen;/* # outstanding bufferss */

    bfAccess = ioList->bsBuf->bfAccess;

    if ((bfAccess->dataSafety == BFD_FTX_AGENT) || 
        (bfAccess->dataSafety == BFD_FTX_AGENT_TEMPORARY) ||
        (bfAccess->dataSafety == BFD_NO_NWR)) {
        bp->b_hints = BH_ADVFS_METADATA;
    } else {
        bp->b_hints = BH_REG_DATA;
    }

    /*
     * Bound desCnt to within out array
     */
#ifdef CLUSTER_STATS
    desCnt = ioList->desCnt;
    if (desCnt < 1) {
        desCnt = 1;
    } else if (desCnt > 8) {
        desCnt = 9;
    }
    if ( ioList->ioRetryCount == 0 ) {
        advfs_clusterstats.cluster_total[desCnt]++;
    }
#endif
    /*
     * Keep stats, do trace, set read or write.
     */
    if( ioList->bsBuf->lock.state & WRITING ) {
        bp->b_flags |= B_WRITE;
#ifdef CLUSTER_STATS
        if ( ioList->ioRetryCount == 0 ) {
            advfs_clusterstats.cluster_write[desCnt]++;
            switch(bp->b_hints) {
                    case BH_REG_DATA:
                            advfs_clusterstats.reg_cluster_write[desCnt]++;
                            break;
                    case BH_ADVFS_METADATA:
                            advfs_clusterstats.metadata_cluster_write[desCnt]++;
                    default:
                            break;
            }
        }
#endif

    }
    else if( ioList->bsBuf->lock.state & READING ) {
        bp->b_flags |= B_READ;
#ifdef CLUSTER_STATS
        if ( ioList->ioRetryCount == 0 ) {
            advfs_clusterstats.cluster_read[desCnt]++;
            switch(bp->b_hints) {
                    case BH_REG_DATA:
                            advfs_clusterstats.reg_cluster_read[desCnt]++;
                            break;
                    case BH_ADVFS_METADATA:
                             advfs_clusterstats.metadata_cluster_read[desCnt]++;
                    default:
                            break;
            }
        }
#endif

    } else {
        ADVFS_SAD0("call_disk: ioDesc is neither reading nor writing");
    }

#ifdef CLUSTER_STATS
    if (disable_advfs_hints)
        bp->b_hints = 0;
#endif

    /*
     * screen domain panic: 
     *   directIO read/write - return failure
     *   read  - return failure
     *   write - fake success return without writing
     */
    dmnP = vdp->dmnP;
    if (dmnP->dmn_panic) {
        if ( bp->b_flags & (B_READ | B_PHYS) ) {
            bp->b_flags |= B_ERROR;
            bp->b_error = EIO;
            bp->b_eei = EEI_ADVFS_DMN_PANIC;
        }
        msfs_iodone(bp);
    } else {
        (*(vdp->devVp->v_op->vn_strategy))( bp );
    }

    /*
     * preemption point added to avoid excessively long code paths
     * through advfs that could effect system responsiveness
     */
    if (!current_processor()->first_quantum && !panicstr) {
       thread_t th = current_thread();

       thread_preempt(th, FALSE);
    }
}

typedef enum err {
    RET,
    OK,
    CONT
} errT;

#define handle_error(error, max_buffers, cnt) \
    if (error == RET) \
        return; \
    else if (error == CONT) \
        continue; \
    else \
        max_buffers = (cnt);\

/* This routine is called with devQ.ioQLock held */
errT
get_locks( ioDescHdrT *qhdr, vdT *vdp, qtypeT qindex, int *locks_held )
{
    errT error = OK;

    MS_SMP_ASSERT(SLOCK_HOLDER(&vdp->devQ.ioQLock.mutex));
    if ( !mutex_lock_try(&qhdr->ioQLock.mutex) ) {

        mutex_unlock( &vdp->devQ.ioQLock );
        mutex_lock( &qhdr->ioQLock );
        mutex_lock( &vdp->devQ.ioQLock );

        MS_SMP_ASSERT( vdp->devQ.lenLimit > 0 );
        if ( vdp->devQ.ioQLen >= vdp->devQ.lenLimit ) {  /* no room */
            /* Note: There is no danger that our I/O will be stranded
             * if we bail out here, because the background I/O thread
             * will be sent a message by I/O completion for the current
             * I/O to kick off more I/O.
             */
            error = RET;
            mutex_unlock( &qhdr->ioQLock );
            vdp->gen_active--;
            if( vdp->gen_active == 0 && vdp->vdIoOut == 0 ) {
                lk_signal( lk_set_state( &vdp->active, INACTIVE_DISK ), &vdp->active);
            }
            mutex_unlock( &vdp->devQ.ioQLock );
        } else if ( qhdr->ioQLen <= 0 ) {
            /* a negative queue length is invalid */
            MS_SMP_ASSERT(qhdr->ioQLen == 0);
            error = CONT;
            mutex_unlock( &qhdr->ioQLock );
        } else {
            locks_held[qindex] = 1;
        }
    } else {
        if ( qhdr->ioQLen <= 0 ) {
            /* a negative queue length is invalid */
            MS_SMP_ASSERT(qhdr->ioQLen == 0);
            error = CONT;
            mutex_unlock( &qhdr->ioQLock );
        } else
            locks_held[qindex] = 1;
    }
    return(error);
}


/*
 * bs_startio
 *
 * If there are buffers to complete, do the I/O.  
 *
 * Values of flushFlag:
 *   1. IO_FLUSH means that the caller wants the lazy queues to be
 *      flushed to disk.  This is typically done after moving as
 *      many lazy buffers down the lazy queues to the consolQ as
 *      possible.
 *   2. IO_NOFLUSH means that the caller does not desire the lazy
 *      queues to be explicitly flushed to disk.  Weight the
 *      loading of the blocking and flush queues.
 *   3. IO_SOMEFLUSH - Weight the loading of the blocking and flush
 *      queues, but attempt to load up a proportionately small
 *      percentage from the lazy queue.  This percentage can be
 *      modified on a per-device basis using 'chvol -q' to change
 *      the values for vdT.qtodev.
 *
 * SMP: bp->bufLock will be acquired as needed.
 */

void
bs_startio( 
    struct vd *vdp,      /* in */
    int flushFlag
    )
{
    int ioAmt;
    ioDescT *ioList;
    ioDescT *iop;
    lbnT blks;
    lbnT vdBlk;
    ioDescHdrT *qhdr = NULL, *ptr;
    int devQ_room;          /* amount of room left on devQ */
    int blocking_cnt = 0;   /* # to move from blocking queue */
    int ubcreq_cnt = 0;     /* # to move from ubcreq queue */
    int flush_cnt = 0;      /* # to move from flush queue */
    int lazy_cnt = 0;       /* # to move from lazy queue */
    int max_buffers = 0;    /* temporary counter */
    int temp;               /* temporary counter */
    errT error;
    qtypeT qindex;
    int locks_held[4];
    int i;

    MS_SMP_ASSERT(flushFlag == IO_FLUSH ||
                  flushFlag == IO_NOFLUSH ||
                  flushFlag == IO_SOMEFLUSH);

    /* Calculate the number of buffers to move from each of the 4 incoming
     * queues.  The number that can be output is limited to the room left
     * on the devQ.  Depending on the value of flushFlag, the calculations
     * are slightly different.
     * IO_FLUSH:     Ideally FILL the devQ by taking one-half of the number
     *                 of entries available from the blockingQ, one-half of 
     *                 the now remaining entries from the ubcReqQ,  half of 
     *                 the remaining entries from the flushq, and the rest
     *                 from the lazy queues.  If this calculation would still
     *                 not fill the device queue,  fill in the remainder
     *                 by taking as many as possible from the blocking, 
     *                 ubcReq, and flush queues, in that order.
     * IO_SOMEFLUSH: Same as IO_FLUSH, except that we limit the number
     *                 taken from the lazy queues to a percentage of those
     *                 on the consolQ.  This is done in an attempt to keep
     *                 the lazy I/Os draining, but not do it so fast that
     *                 these buffers cannot be repinned or that we actually
     *                 take up room on the devQ with buffers that do not
     *                 actually need to get flushed now.
     * IO_NOFLUSH:  1. Flush as many as possible from the blockingQ.
     *              2. If there is room left, flush as many as possible from
     *                 the ubcReqQ.
     *              3. If there is still room left, flush as many as possible
     *                 from the flushQ.
     *              4. Do not worry about flushing the lazy queues.
     */

    mutex_lock( &vdp->devQ.ioQLock );

    devQ_room = MAX( 0, vdp->devQ.lenLimit - vdp->devQ.ioQLen );
    if ( !devQ_room ) {
        mutex_unlock( &vdp->devQ.ioQLock );
        return;          /* no room on the device queue right now */
    }

    if ( flushFlag == IO_FLUSH || flushFlag == IO_SOMEFLUSH ) {
        blocking_cnt = MIN( vdp->blockingQ.ioQLen, (devQ_room/2) );
        devQ_room -= blocking_cnt;
        if ( devQ_room ) {
            ubcreq_cnt = MIN( vdp->ubcReqQ.ioQLen, (devQ_room/2) );
            devQ_room -= ubcreq_cnt;
        }
        if ( devQ_room ) {
            flush_cnt = MIN( vdp->flushQ.ioQLen, (devQ_room/2) );
            devQ_room -= flush_cnt;
        }
        if ( devQ_room ) {
            if ( flushFlag == IO_SOMEFLUSH ) {
                /* Pick up items from consolQ even if ioQLen is small */
                temp = (vdp->consolQ.ioQLen <= (1 << vdp->qtodev)) ?
                        vdp->consolQ.ioQLen :
                        vdp->consolQ.ioQLen >> vdp->qtodev;
                lazy_cnt = MIN(temp, devQ_room);
            } else {
                lazy_cnt = MIN( vdp->consolQ.ioQLen, devQ_room );
            }
            devQ_room -= lazy_cnt;
        }

        /* Now go back and see if we can fit in some more from the
         * blocking, ubcreq, or flush queues.  This is mostly needed
         * if one of the incoming queues is relatively empty.
         */
        if ( devQ_room ) {
            temp = MIN( vdp->blockingQ.ioQLen-blocking_cnt, devQ_room );
            if ( temp > 0 ) {
                blocking_cnt += temp;
                devQ_room -= temp;
            }
            temp = MIN( vdp->ubcReqQ.ioQLen-ubcreq_cnt, devQ_room );
            if ( temp > 0 ) {
                ubcreq_cnt += temp;
                devQ_room -= temp;
            }
            temp = MIN( vdp->flushQ.ioQLen-flush_cnt, devQ_room );
            if ( temp > 0 ) {
                flush_cnt += temp;
                devQ_room -= temp;
            }
        }
    } else { /* IO_NOFLUSH */
        blocking_cnt = MIN( vdp->blockingQ.ioQLen, devQ_room );
        devQ_room -= blocking_cnt;
        if ( devQ_room ) {
            ubcreq_cnt = MIN( vdp->ubcReqQ.ioQLen, devQ_room );
            devQ_room -= ubcreq_cnt;
        }
        if ( devQ_room ) {
            flush_cnt = MIN( vdp->flushQ.ioQLen, devQ_room );
            devQ_room -= flush_cnt;
        }
    }

    /* Check for nothing to output */
    if ( !(blocking_cnt || ubcreq_cnt || flush_cnt || lazy_cnt) ) {
        /* nothing to output, nothing on devQ, try vfast */
        if ( !(vdp->devQ.ioQLen) ) {
            ss_startios(vdp);
        }
        mutex_unlock( &vdp->devQ.ioQLock );
        return;
    }

    /* Do not set this as an ACTIVE_DISK until we know that we can
     * actually start some I/Os.
     */
    (void)lk_set_state( &vdp->active, ACTIVE_DISK );
    vdp->gen_active++;

    for (i=0; i < 4; i++)
        locks_held[i] = 0;

    /* Start more I/Os so long as:
     *  1.  The # of buffers on the deviceQ is less than its limit.  AND
     *  2.  There are still more buffers to be moved according to
     *      our calculation above.
     */
    while ( vdp->devQ.ioQLen < vdp->devQ.lenLimit ) {

        if ( vdp->blockingQ.ioQLen && blocking_cnt > 0  ) {
            qhdr = &vdp->blockingQ;
            qindex = BLKQ;
            error = get_locks(qhdr, vdp, qindex, locks_held);
            handle_error(error, max_buffers, blocking_cnt);
        } else if ( vdp->ubcReqQ.ioQLen && ubcreq_cnt > 0 ) {
            qhdr = &vdp->ubcReqQ;
            qindex = UBCRQ;
            error = get_locks(qhdr, vdp, qindex, locks_held);
            handle_error(error, max_buffers, ubcreq_cnt);
        } else if ( vdp->flushQ.ioQLen && flush_cnt > 0 ) {
            qhdr = &vdp->flushQ;
            qindex = FLSHQ;
            error = get_locks(qhdr, vdp, qindex, locks_held);
            handle_error(error, max_buffers, flush_cnt);
        } else if ( vdp->consolQ.ioQLen && lazy_cnt > 0 ) {
            qhdr = &vdp->consolQ;
            qindex = CONSLQ;
            error = get_locks(qhdr, vdp, qindex, locks_held);
            handle_error(error, max_buffers, lazy_cnt);
        } else {
            /* get out if we moved the # of buffers calculated above,
             * even though there is still room on the device queue.
             */
            break;
        }

        if ( vdp->consolidate &&
             (max_buffers > 1) &&
             (vdp->devQ.lenLimit > vdp->devQ.ioQLen + 1) ) {

            /* Call consolidation routine to group I/Os where possible.
             * Avoid calling this routine if there is only one buffer
             * to be added.
             */
            consecutive_list_io( qhdr, vdp, max_buffers, &ioList, qindex, locks_held );
            if ( ioList == NULL )    /* No ioDescs to consolidate */
                break;

        } else {
            /*
             * Set up a single I/O: remove one ioDesc from queue and
             * set up to move to device queue.
             */
            ioList = qhdr->fwd;
            mutex_lock( &ioList->bsBuf->bufLock );
            
            if (qhdr == &vdp->consolQ) {
                /* Skip the buffer if it is coming off the consolQ and 
                 * is marked REMOVE_FROM_IOQ because it is being removed 
                 * from the lazy queues for reuse. Allow ioDesc with 
                 * this bit set on blockingQ to go thru.
                 */
loop:
                while ( ioList->bsBuf->lock.state & REMOVE_FROM_IOQ ) {
                    ioDescT *next = ioList->fwd;
                    mutex_unlock( &ioList->bsBuf->bufLock );
                    ioList = next;
                    if (ioList == (ioDescT *)qhdr )
                        break;
                    else
                        mutex_lock( &ioList->bsBuf->bufLock );
                } 

                /* ran out of ioDesc's on this queue; no buffer locked. */
                if (ioList == (ioDescT *)qhdr )
                    break;

                /* Setup the UBC page for IO.
                 * Raw and Direct I/O do not put buffers on the consolq.
                 */
                MS_SMP_ASSERT(!(ioList->bsBuf->lock.state & RAWRW));
                if (advfs_page_get(ioList->bsBuf, UBC_GET_BUSY)) {
                    /* If the UBC page is already setup for IO (the
                     * pg_busy flag is on), then skip this buffer and
                     * try the next one. UBC might be initiating the IO
                     * and has not yet called AdvFS's putpage.
                     * We also skip this buffer if the page is in an exchange
                     * transition, but that is ok since eventually we will see
                     * it again on a future pass.
                     */
                    mutex_unlock( &ioList->bsBuf->bufLock );
                    ioList = ioList->fwd;
                    if (ioList == (ioDescT *)qhdr )
                        break;
                    else
                        mutex_lock( &ioList->bsBuf->bufLock );
                    goto loop;
                }
#ifdef ADVFS_SMP_ASSERT
                ioList->bsBuf->busyLn = SET_LINE_AND_THREAD(__LINE__);
                ioList->bsBuf->ioqLn = -1;
#endif
            }

            /* mark buffer before dropping its lock */
            ioList->ioQ = DEVICE;
            ioList->bsBuf->lock.state |= BUSY;
            ioList->bsBuf->bufDebug &= ~BSBUF_WAITQ;
            ioList->bsBuf->sync_stamp = 0;
            vdp->devQ.queue_cnt++;

            mutex_unlock( &ioList->bsBuf->bufLock );

            /* Remove this descriptor from the queue */
            ioList->fwd->bwd = (ioDescT *)ioList->bwd; 
            ioList->bwd->fwd = ioList->fwd;
            ioList->fwd = ioList->bwd = ioList;
            qhdr->ioQLen -= 1;
            MS_VERIFY_IOQUEUE_INTEGRITY( qhdr, locks_held[qindex] );
        }

        if (locks_held[qindex]) {
            mutex_unlock(&qhdr->ioQLock);
            locks_held[qindex] = 0;
        }

        if( ioList->desCnt > 0 ) {

            iop = ioList->bwd;  /* iop is the last ioDesc on list */

            /*
             * Add ioDesc list to devQ
             */
            ioList->bwd = vdp->devQ.bwd;
            vdp->devQ.bwd->fwd = ioList;
            iop->fwd = (ioDescT *)&vdp->devQ;
            vdp->devQ.bwd = iop;
            vdp->devQ.ioQLen += ioList->desCnt;

            if( qhdr == &vdp->blockingQ ) {
                blocking_cnt -= ioList->desCnt;
            } else if (qhdr == &vdp->ubcReqQ) {
                ubcreq_cnt -= ioList->desCnt;
            } else if (qhdr == &vdp->flushQ) {
                flush_cnt -= ioList->desCnt;
            } else {
                lazy_cnt -= ioList->desCnt;
            }
            MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->devQ, TRUE );
        } else {

            /*
             * Add ioDescT to devQ
             */
            ioList->bwd = vdp->devQ.bwd;
            vdp->devQ.bwd->fwd = ioList;
            ioList->fwd = (ioDescT *)&vdp->devQ;
            vdp->devQ.bwd = ioList;
            vdp->devQ.ioQLen += 1;
            if( qhdr == &vdp->blockingQ ) {
                blocking_cnt--;
            } else if (qhdr == &vdp->ubcReqQ) {
                ubcreq_cnt--;
            } else if (qhdr == &vdp->flushQ) {
                flush_cnt--;
            } else {
                lazy_cnt--;
            }
            MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->devQ, TRUE );
        }

        /* vdIoOut is the count of all I/O's queued to the device.  */
        vdp->vdIoOut++;

        vdBlk = ioList->blkDesc.vdBlk;

        if( ioList->desCnt != 0 ) {
            
            blks = ioList->totalBlks;
            ioAmt = blks * BS_BLKSIZE;

            if (ioList->bsBuf->lock.state & WRITING) {
                vdp->dStat.wglobBlk += blks;
                vdp->dStat.wglob++;
            } else {
                vdp->dStat.rglobBlk += blks;
                vdp->dStat.rglob++;
            }
        } else {

            /*
             * In the single buffer I/O case, we just
             * use the buffer's virtual address.
             * For non-ubc I/O, the buffer address is already filled in.
             */
            if (!(ioList->bsBuf->lock.state & RAWRW)) {
                ioList->targetAddr = (unsigned char *)
                                     ubc_load(ioList->bsBuf->vmpage, 0, 0);
            }

            ioList->ioCount = 1;

            /*
             * In the case of direct I/O numBlks is really number of bytes.
             */
            if (ioList->bsBuf->directIO) {
                ioAmt = ioList->numBlks;
                blks = howmany(ioAmt, BS_BLKSIZE);

                /* directIO transfers must be full-sectors */
                MS_SMP_ASSERT( !(ioAmt % BS_BLKSIZE) );
            }
            else {
                blks = ioList->numBlks;
                ioAmt = blks * BS_BLKSIZE;
            }
        }

        /*
         * Keep stats, do trace, set read or write.
         */
        if( ioList->bsBuf->lock.state & WRITING ) {
            vdp->dStat.nwrite++;
            vdp->dStat.writeblk += blks;
            vdp->dmnP->bcStat.devWrite++;

            if( TrFlags & trDevWr )
                dev_trace( ioList->blkDesc.vdBlk, 
                           ioList->bsBuf->bfAccess->primVdIndex,
                           ioList->bsBuf->bfAccess->tag, 
                           ioList->bsBuf->bfPgNum, DevWrite );
        } else {
            vdp->dStat.nread++;
            vdp->dStat.readblk += blks;
            vdp->dmnP->bcStat.devRead++;

            if( TrFlags & trDevRd )
                dev_trace( ioList->blkDesc.vdBlk, 
                           ioList->bsBuf->bfAccess->primVdIndex,
                           ioList->bsBuf->bfAccess->tag, 
                           ioList->bsBuf->bfPgNum, DevRead );

        }

        /* 
         * Zero the retry counter.  This will be used to limit the AdvFS
         * retry duration. 
         */
        ioList->ioRetryCount = 0;

        mutex_unlock( &vdp->devQ.ioQLock );
        call_disk( vdp, ioAmt, vdBlk, ioList );
        mutex_lock( &vdp->devQ.ioQLock );
    }

    if (qhdr && locks_held[qindex]) {
        mutex_unlock(&qhdr->ioQLock);
        locks_held[qindex] = 0;
    }

    vdp->gen_active--;
    if( vdp->gen_active == 0 && vdp->vdIoOut == 0 ) {
        lk_signal( lk_set_state( &vdp->active, INACTIVE_DISK ), &vdp->active );
    }

    mutex_unlock( &vdp->devQ.ioQLock );
}


/*
 * bs_raw_complete
 *
 * I/O completion routine for read_raw_bmt_page.
 */

void
bs_raw_complete( 
    struct buf *bp      /* in */
    )
{
    event_post( &bp->b_iocomplete );
}

/*
 * read_raw_bmt_page
 * 
 * This routine reads a BMT page from the specified disk (via its vnode).
 * The read starts at the specified disk block number.
 */

int
read_raw_bmt_page( 
    struct vnode *vp,        /* in - device's vnode */
    lbnT vdblk,              /* in - starting disk block number */
    struct bsMPg *dpg)       /* in - bitfile metadata table page ptr */
{
    return read_raw_page( vp, vdblk, ADVFS_PGSZ_IN_BLKS, dpg );
}

/*
 * read_raw_page
 * 
 * This routine reads a page from the specified disk (via its vnode).
 * The read starts at the specified disk block number.
 */

int
read_raw_page( 
    struct vnode *vp, /* in - device's vnode */
    lbnT vdblk,       /* in - starting disk block number */
    uint blksPerPg,   /* in - number of blocks per page */
    void *pg)         /* in - bitfile page ptr */
{
    struct buf *bp;
    int res = EOK;

    /* get and clear an osf struct buf */
    bp = getosfbuf();

    bp->b_bcount = blksPerPg * BS_BLKSIZE;
    bp->b_dev = vp->v_rdev;

    bp->b_un.b_addr = (caddr_t)pg;

    bp->b_blkno = vdblk;

    bp->b_iodone = bs_raw_complete;

    /* bp->b_vp = 0; No device vnode, to thwart biodone */

    bp->b_flags = B_READ;
    bp->b_drv_handle = vp->v_drv_handle;
    (*(vp->v_op->vn_strategy))( bp );

    /*
     * Wait for read to complete.
     */
    (void)event_wait( &bp->b_iocomplete, FALSE, 0 );

    if( (bp->b_flags & B_ERROR) != 0 ) {
        if( bp->b_error ) {
            res = bp->b_error;
        } else {
            res = EIO;
        }
    }
    else if (bp->b_resid) {
        res = EIO;
    }

    freeosfbuf( bp );
    return( res );
}

/*
 * write_raw_bmt_page
 * 
 * This routine writes a BMT page to the specified disk (via its vnode).
 * The write starts at the specified disk block number.
 */

int
write_raw_bmt_page( 
               struct vnode *vp,        /* in - device's vnode */
               lbnT vdblk,              /* in - starting disk block number */
               struct bsMPg *dpg)       /* in - bitfile metadata table pg ptr */
{
        return write_raw_page( vp, vdblk, ADVFS_PGSZ_IN_BLKS, dpg );
}

/*
 * write_raw_sbm_page
 * 
 * This routine writes a bitmap page to the specified disk (via its vnode).
 * The write starts at the specified disk block number.
 */

int
write_raw_sbm_page( 
               struct vnode *vp,        /* in - device's vnode */
               lbnT vdblk,              /* in - starting disk block number */
               struct bsStgBm* sbmPg)   /* in - bitmap page ptr */
{
        return write_raw_page( vp, vdblk, ADVFS_PGSZ_IN_BLKS, sbmPg );
}

/*
 * write_raw_sbm_page
 * 
 * This routine writes a bitmap page to the specified disk (via its vnode).
 * The write starts at the specified disk block number.
 */

int
write_raw_page( 
    struct vnode *vp, /* in - device's vnode */
    lbnT vdblk,       /* in - starting disk block number */
    uint blksPerPg,   /* in - number of blocks per page */
    void *pg)         /* in - bitfile page ptr */
{
    struct buf *bp;
    int res = EOK;

    /* get and clear an osf struct buf */
    bp = getosfbuf();

    bp->b_bcount = blksPerPg * BS_BLKSIZE;
    bp->b_dev = vp->v_rdev;

    bp->b_un.b_addr = (caddr_t)pg;

    bp->b_blkno = vdblk;

    bp->b_iodone = bs_raw_complete;

    bp->b_drv_handle = vp->v_drv_handle;
    /* bp->b_vp = 0;  No device vnode, to thwart biodone */

    bp->b_flags = B_WRITE;
    (*(vp->v_op->vn_strategy))( bp );

    /*
     * Wait for write to complete.
     */
    (void) event_wait( &bp->b_iocomplete, FALSE, 0 );

    if( (bp->b_flags & B_ERROR) != 0 ) {
        if( bp->b_error ) {
            res = bp->b_error;
        } else {
            res = EIO;
        }
    }
    else if (bp->b_resid) {
        res = EIO;
    }

    freeosfbuf( bp );
    return( res );
}
