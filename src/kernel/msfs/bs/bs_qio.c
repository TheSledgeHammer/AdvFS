/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1989, 1990, 1991                      *
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
 *      bs_qio.c - I/O queueing routines
 *
 * Date:
 *
 *      June, 1990
 *
 */
/*
 * HISTORY
 * 
 */
#pragma ident "@(#)$RCSfile: bs_qio.c,v $ $Revision: 1.1.367.9 $ (DEC) $Date: 2006/07/05 12:23:11 $"

#define ADVFS_MODULE  BS_QIO

#include <sys/file.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_msg_queue.h>
#include <msfs/ms_osf.h>

#include <sys/syslog.h>
#include <mach/std_types.h>
#include <sys/lock_probe.h>
#include <vm/vm_ubc.h>

#define BUF_WRITE_LIM 5
#define DEV_LOCKED 1
#define DEV_UNLOCKED 0

unsigned int Advfs_sort_factor = 4096;

static msgQHT *IoMsgQH;
int PrintList = 0;

unsigned long startiocalls[12] = {0,0,0,0,0,0,0,0,0,0,0,0};
unsigned long notstartiocalls[11] = {0,0,0,0,0,0,0,0,0,0,0};

/* The following macro can be used to update a 16-element histogram.  Each
 * element represents the appropriate power of 2.  In this module, it is
 * used to maintain statistics on the length of the ioDesc chains being moved
 * among the I/O queues.
 */
#ifdef ADVFS_SMP_ASSERT

#define HISTOGRAM_UPDATE( hist, len ) \
{ int i; \
  for ( i = 0; i < 15; i++ ) { \
    if ( (len) <= 1<<i ) \
        break; \
  } \
  (hist)[i]++; \
}

#endif

unsigned long Rdy2ConsolSz[16];
unsigned long Sm2RdySz[16];

/*
 * RdySort[0] = # elements put onto empty readyq.
 * RdySort[1] = # times chain was put onto empty readyq.
 * RdySort[2] = # elements put onto end of readyq as a chain.
 * RdySort[3] = # times a chain was put onto readyq end
 * RdySort[4] = # elements inserted into readyq interior, usually an element at
 *                a time.
 * RdySort[5] = # times chain was sorted onto readyq.
 * RdySort[6] = Longest chain inserted onto readyq interior.
 * RdySort[7] = # times sort_onto_readyq() was called.
 *   Next 3 counters are for code optimized for single-element additions.
 * RdySort[8] = # times a single element was added to an empty readyq.
 * RdySort[9] = # times a single element was added to front or end of readyq.
 * RdySort[10]= # times a single element was added to the interior of readyq.
 *
 * RdySort[0] + [2] + [4] + [8,9,10] = Total # elements put onto readyq
 *
 * RdySort[11] = # times thru special case code for large incoming chains.
 * RdySort[12] = largest chain processed in special case code.
 */
unsigned long RdySort[13] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};


#ifdef ADVFS_SMP_ASSERT
extern int AdvfsDomainPanicLevel;
#define ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3  AdvfsDomainPanicLevel=3
#else
#define ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3
#endif

/*
 * Forward references
 */

void
wait_to_readyq( struct vd *vdp );

void
check_cont_bits( domainT *dmnP, int src );

static
void
lsn_io_list( struct domain *dmnP);          /* in */

void
call_logflush( domainT *dmnP, 
                lsnT lsn, 
                int wait );

void
logflush_cont( struct bfAccess *bfap );     /* in */

void
bs_qtodev( 
    ioDescT *iop        /* in */
    );

void
write_immediate_q( 
    struct vd *vdp,         /* in */
    int devLocked           /* in */
    );

void
sort_onto_readyq(
    struct vd *vdp,             /* in */
    ioDescT *iop,               /* in */
    int cnt,                    /* in */
    mutexT *ioQLock,            /* in */
    tempQMarkerT *tempQ_marker  /* in */
    );

void
add_to_smsync( 
    struct vd *vdp,             /* in */
    ioDescT *ioListp,           /* in */
    u_int len,                  /* in */
    mutexT *ioQLock,            /* in */
    tempQMarkerT *tempQ_marker  /* in */
    );

void
smsync_to_readyq( 
    struct vd *vdp,             /* in */
    int forceFlushFlag          /* in */
    );

void
rm_from_lazyq( struct bsBuf *bp,              /* in */
               ioDescT **ioList,              /* out */
               int *listLen,                  /* out */
               int *noqfnd);                  /* out */

extern char *PrintAction[];


void
check_queue( ioDescHdrT *qhdr )
{
    int cnt      = 0;
    int orig_len;
    ioDescT *iop;

    orig_len = qhdr->ioQLen;
    iop = qhdr->fwd;

    /* scan the queue forwards */

    while( iop != (ioDescT *)qhdr ) {
        cnt++;
        MS_SMP_ASSERT( iop->ioQ == qhdr->ioQ );
        MS_SMP_ASSERT( iop->bsBuf->bufMagic == BUFMAGIC );
        iop = iop->fwd;
        if (cnt >= orig_len) break;
    }

    MS_SMP_ASSERT( orig_len == qhdr->ioQLen );     /* should not change */
    MS_SMP_ASSERT( cnt == qhdr->ioQLen );
    MS_SMP_ASSERT( iop == (ioDescT *)qhdr );

    iop = qhdr->bwd;
    cnt = 0;
    /* scan the queue backwards */
    while( iop != (ioDescT *)qhdr  ) {
        cnt++;
        MS_SMP_ASSERT( iop->ioQ == qhdr->ioQ);
        MS_SMP_ASSERT( iop->bsBuf->bufMagic == BUFMAGIC );
        iop = iop->bwd;
        if (cnt >= orig_len) break;
    }

    MS_SMP_ASSERT( orig_len == qhdr->ioQLen );     /* should not change */
    MS_SMP_ASSERT( cnt == qhdr->ioQLen );
    MS_SMP_ASSERT( iop == (ioDescT *)qhdr );

    return;
}


/*
 * bs_q_blocking
 *
 * If the device is not busy, queue request to device and start it up. 
 * Otherwise, sort io desc into the immediate queue, it will be 
 * picked up by the device's I/O completion.
 *
 * Mutex and Locks:  Since the buffer state is BUSY at this point
 * we can look at any buffer values, we even can write result and vdBlk,
 * however, if we want to change a value others can look at (namely bp->state)
 * we must acquire the bp->bufLock.
 *
 * The list of I/O requests to this routine must all be to the same
 * device.  Using bs_q_list as a filter for this routine assures this.
 * ioListp is a circular, doubly linked list (even if count is one).
 */

void
bs_q_blocking( 
    ioDescT *ioListp,     /* in */
    int count             /* in */
    )
{
    struct vd *vdp;
    int i;
    int write_count = 0;
    int read_count = 0;
    ioDescT *tmp;
    struct bsBuf *bp;

    /*
     * Add the buffer to the blocking queue.  If device is not busy, 
     * start it up.  
     */
    vdp = VD_HTOP(ioListp->blkDesc.vdIndex, ioListp->bsBuf->bfAccess->dmnP);

    /*
     * Mark ioDesc on blocking queue and charge the process
     * for i/o
     */
    tmp = ioListp;
    i = count - 1;
    do {
        bp = tmp->bsBuf;
        tmp->ioQ = BLOCKING;
        if (i > 1) {
            /* The following is a prefetch of bsBuf.  It puts the data
             * structure into cache so that it is ready when needed.
             */
            asm("ldl $31,0(%0)",(tmp->fwd->bsBuf));
        }
        mutex_lock( &bp->bufLock );
        MS_SMP_ASSERT( bp->lock.state & (IO_TRANS | BUSY) );
        if ( !(bp->lock.state & IO_COUNTED) ) {
            if (bp->lock.state & READING)
                read_count++;
            else
                write_count++;

            bp->lock.state |= IO_COUNTED;
        }
        mutex_unlock( &bp->bufLock );

        tmp = tmp->fwd;

        SMSYNC_DBG( SMSYNC_DBG_BUF, 
            aprintf ( "blockingQ:     bsBuf 0x%lx  vd 0x%lx\n", bp, vdp ));
    } while( i-- );

    u.u_tru.tru_inblock += read_count;
    u.u_tru.tru_oublock += write_count;

    mutex_lock( &vdp->blockingQ.ioQLock );
    /*
     * Add list to the end of the blocking queue.
     */
    vdp->blockingQ.ioQLen += count;
    vdp->blockingQ.queue_cnt += count;

    vdp->blockingQ.bwd->fwd = ioListp;
    ioListp->bwd->fwd = (ioDescT *)&vdp->blockingQ;
    tmp = ioListp->bwd;
    ioListp->bwd = vdp->blockingQ.bwd;
    vdp->blockingQ.bwd = tmp;

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->blockingQ, TRUE);

    mutex_unlock( &vdp->blockingQ.ioQLock );

    startiocalls[0]++;
    bs_startio( vdp, IO_NOFLUSH );
}

int Advfs_disable_flushq=0;

void
bs_q_flushq(
    ioDescT *ioListp,     /* in */
    int count             /* in */
    )
{
    struct vd *vdp;
    int i;
    int write_count = 0;
    int read_count = 0;
    ioDescT *tmp;
    struct bsBuf *bp;

    /* This is a 'backdoor' to disable the flushq;  setting
     * Advfs_disable_flushq will force us back to the old
     * algorithm of putting all non-lazy I/Os onto the
     * blocking queue.
     */
    if ( Advfs_disable_flushq ) {
        bs_q_blocking( ioListp, count );
        return;
    }

    vdp = VD_HTOP(ioListp->blkDesc.vdIndex, ioListp->bsBuf->bfAccess->dmnP);

    /* Mark ioDesc on flush queue and charge the process for I/O */
    tmp = ioListp;
    i = count - 1;
    do {
        bp = tmp->bsBuf;
        tmp->ioQ = FLUSHQ;
        if (i > 1) {
            /* The following is a prefetch of bsBuf.  It puts the data
             * structure into cache so that it is ready when needed.
             */
            asm("ldl $31,0(%0)",(tmp->fwd->bsBuf));
        }
        mutex_lock( &bp->bufLock );
        MS_SMP_ASSERT( bp->lock.state & (IO_TRANS | BUSY) );
        if ( !(bp->lock.state & IO_COUNTED) ) {
            if (bp->lock.state & READING)
                read_count++;
            else
                write_count++;

            bp->lock.state |= IO_COUNTED;
        }

        MS_SMP_ASSERT((bp->lock.state & RAWRW) || bp->vmpage->pg_busy);

        mutex_unlock( &bp->bufLock );
        tmp = tmp->fwd;

        SMSYNC_DBG( SMSYNC_DBG_BUF,
            aprintf ( "flushQ:     bsBuf 0x%lx  vd 0x%lx\n", bp, vdp ));
    } while( i-- );

    u.u_tru.tru_inblock += read_count;
    u.u_tru.tru_oublock += write_count;

    /* Add list to the end of the flush queue */
    mutex_lock( &vdp->flushQ.ioQLock );
    vdp->flushQ.ioQLen += count;
    vdp->flushQ.queue_cnt += count;

    vdp->flushQ.bwd->fwd = ioListp;
    ioListp->bwd->fwd = (ioDescT *)&vdp->flushQ;
    tmp = ioListp->bwd;
    ioListp->bwd = vdp->flushQ.bwd;
    vdp->flushQ.bwd = tmp;

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->flushQ, TRUE );
    mutex_unlock( &vdp->flushQ.ioQLock );

    startiocalls[11]++;
    bs_startio( vdp, IO_NOFLUSH );

    return;
}

unsigned int Advfs_disable_ubcreq = 0;

void
bs_q_ubcreq(
    ioDescT *ioListp,     /* in */
    int count             /* in */
    )
{
    struct vd *vdp;
    int i;
    int write_count = 0;
    int read_count = 0;
    ioDescT *tmp;
    struct bsBuf *bp;

    /* This is a 'backdoor' to disable the ubcReqQ;  setting
     * Advfs_disable_ubcreq will force us back to the old
     * algorithm of putting all I/O requests from the UBC for
     * freeing pages onto the blocking queue.
     */
    if ( Advfs_disable_ubcreq ) {
        bs_q_blocking( ioListp, count );
        return;
    }

    vdp = VD_HTOP(ioListp->blkDesc.vdIndex, ioListp->bsBuf->bfAccess->dmnP);

    /* Mark ioDesc on ubcreq queue and charge the process for I/O */
    tmp = ioListp;
    i = count - 1;
    do {
        bp = tmp->bsBuf;
        tmp->ioQ = UBCREQQ;
        if (i > 1) {
            /* The following is a prefetch of bsBuf.  It puts the data
             * structure into cache so that it is ready when needed.
             */
            asm("ldl $31,0(%0)",(tmp->fwd->bsBuf));
        }
        mutex_lock( &bp->bufLock );
        MS_SMP_ASSERT( bp->lock.state & (IO_TRANS | BUSY) );
        if ( !(bp->lock.state & IO_COUNTED) ) {
            if (bp->lock.state & READING)
                read_count++;
            else
                write_count++;

            bp->lock.state |= IO_COUNTED;
        }

        MS_SMP_ASSERT((bp->lock.state & RAWRW) || bp->vmpage->pg_busy);

        mutex_unlock( &bp->bufLock );
        tmp = tmp->fwd;

        SMSYNC_DBG( SMSYNC_DBG_BUF,
            aprintf ( "ubcReqQ:     bsBuf 0x%lx  vd 0x%lx\n", bp, vdp ));
    } while( i-- );

    u.u_tru.tru_inblock += read_count;
    u.u_tru.tru_oublock += write_count;

    /* Add list to the end of the ubcReq queue */
    mutex_lock( &vdp->ubcReqQ.ioQLock );
    vdp->ubcReqQ.ioQLen += count;
    vdp->ubcReqQ.queue_cnt += count;

    vdp->ubcReqQ.bwd->fwd = ioListp;
    ioListp->bwd->fwd = (ioDescT *)&vdp->ubcReqQ;
    tmp = ioListp->bwd;
    ioListp->bwd = vdp->ubcReqQ.bwd;
    vdp->ubcReqQ.bwd = tmp;

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->ubcReqQ, TRUE );

    mutex_unlock( &vdp->ubcReqQ.ioQLock );

    startiocalls[2]++;

    bs_startio( vdp, IO_NOFLUSH );

    return;
}



/*
 * bs_q_list
 *
 * This routine is called as a filter for output routines (bs_q_blocking 
 * or bs_q_lazy) by functions that create lists of ioDesc.
 * From the given list, this routine creates sublists each for a
 * specific vd and calls the supplied routine with each sublist.
 * 
 * SMP: Assumes that bsBuf is marked IO_TRANS so that no other threads
 *      mess with its ioDesc structs.  No locks held.
 */

void
bs_q_list( 
    ioDescT *ioListp,                    /* in */
    int len,                             /* in */
    void (*q_for_io)( ioDescT *, int )   /* in */
    )
{
    ioDescT *iop, *tmp, *siop;
    int i, count;

    /*
     * Make a sublist and call requested enqueuing routine for each
     * individual vd.
     */

    while( len ) {

        siop = ioListp;

        ioListp = siop->fwd;

        iop = ioListp;
        count = 1;

        /*
         * If the next buf is not on the same vd as
         * the starting buffer, sbp, then remove it
         * from the list.  The removed buffer then starts
         * the new buffer list.
         */

        for( i = 0; i < len - 1; i++ ) {

            if ( iop->blkDesc.vdIndex != siop->blkDesc.vdIndex ) {
                /*
                 * Remove buffer from vd's, install
                 * on list headed by ioListp.
                 */
                iop->fwd->bwd = iop->bwd;
                iop->bwd->fwd = iop->fwd;
                
                /*
                 * Check if we are the head
                 * of the list.
                 */
                tmp = iop->fwd;
                if ( iop == ioListp ) {
                    iop->fwd = iop->bwd = iop;
                } else {
                    ioListp->bwd->fwd = iop;
                    iop->bwd = ioListp->bwd;
                    iop->fwd = ioListp;
                    ioListp->bwd = iop;
                }
                iop = tmp;
            } else {
                if ( iop == ioListp ) {
                    ioListp = iop->fwd;
                }
                count++;
                iop = iop->fwd;
            }
        }
        len -= count;
        (*q_for_io)( siop, count );
    }
}

/* bs_aio_write_cleanup():
 * Called when an aio write is finished to decrement the v_numoutput
 * field and wakeup any waiters.  We must decrement the
 * vnode.v_numoutput field if this was a write for a file other than
 * a socket.  This is typically accomplished in biodone(), but that
 * is not called in our I/O completion path.
 */
void
bs_aio_write_cleanup(
    struct vnode *vp    /* in */
    )
{
    int wakeup = 0;
    spl_t s;

    s = splbio();
    VN_OUTPUT_LOCK(vp);
    MS_SMP_ASSERT( vp->v_numoutput > 0 );
    vp->v_numoutput--;
    if ( (vp->v_numoutput == 0) && (vp->v_outflag & VOUTWAIT) ) {
        vp->v_outflag &= ~VOUTWAIT;
        wakeup = 1;
    }
    VN_OUTPUT_UNLOCK(vp);
    splx(s);
    if (wakeup)
        thread_wakeup((vm_offset_t)&(vp->v_numoutput));
}
    

/*
 * bs_io_metacheck
 *
 * Called upon I/O completion before bs_io_complete() when the
 * bsBuf.metaCheck field is non-zero.
 *
 * IO done integrity checking is done here instead of bs_io_complete()
 * because we don't want to check on aborted/fake completions.  Clearing
 * the bsBuf->metaCheck is in bs_io_complete() for the opposite reason.
 * The decision to check after a read or write is controlled by the
 * thread staging the IO.
 *
 * Check routines may set bp->result, bp->lock.state, bfAccess, Bfset,
 * Domain status, etc., or panic (not recommended) as appropriate to
 * the situation.
 *
 * Note: check routines must operate in the context of an I/O LWC
 *
 * SMP: assumes bp->bufLock is held from bs_io_complete(), but not
 *      required unless the check subroutine needs it to be so.
 */

void
bs_io_metacheck( struct bsBuf *bp )    /* in */
{
     switch (bp->metaCheck) {
         case BSBUF_CHK_BMTPG:
             check_BMT_pg(bp);
             break;
         default: /*unknown checks are ignored */
             break;
     }
     bp->bufDebug |= BSBUF_CHECKED;
}

/*
 * bs_io_complete
 *
 * Call upon I/O completion.
 *
 * Responsibilities include:
 * 1. Remove write buffers from the access structure's dirty list.
 * 2. Remove the buffer from the domain's lsnList if it is
 *    therein enqueued.
 * 3. Reset the buffer's state.
 * 4. Wakeup anyone waiting on this buffer becoming valid.
 *
 * SMP: The caller must already hold bp->bufLock for the buffer.
 *      The bp->bufLock will be released.
 */

void
bs_io_complete( 
    struct bsBuf *bp,    /* in */
    int *retestfirst     /* in/out */
    )
{
    struct bsBufHdr *lsnp;
    ioThreadMsgT *msg;
    struct bfAccess *bfap = bp->bfAccess;
    struct domain *dmnP = bfap->dmnP;
    enum contBits contBits = 0;
    int free_bsbuf = FALSE;
    int free_act_range = FALSE;
    int release_ubc_page;
    int radId = 0;
    int ubc_flags;

    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex));

    /*
     * Raw I/O's that do not have UBC pages come through this
     * routine using the status clear and signaling at the end.
     */
    if (bp->lock.state & RAWRW) {
        release_ubc_page = FALSE;
        goto clearSignal;
    }

    /*
     * We normally have a busy ubc page that we must release here.
     * In all cacheio paths, BUSY is set only after vm_page.pg_busy
     * so that when the ubc page is not busy because of an error
     * we can clean up and skip calling ubc_page_release.
     */
    release_ubc_page = (bp->lock.state & BUSY);
 
    /*
     * Remove buf from access list if we have
     * just written a dirty buffer.  These only get
     * put on the list at bs_pinpg.
     * Very special processing for log buffers.
     */

    if ( bp->lock.state & WRITING ) {

        /* be sure that this buffer is on dirty list */
        MS_SMP_ASSERT(bp->accFwd && bp->lock.state & ACC_DIRTY);

        /* Seize file's bfIoLock while moving buffer around on dirty list.*/
        mutex_lock( &bfap->bfIoLock );

        /* Notify any threads which are flushing this file 
         * that this buffer has been flushed. 
         */
        bs_wakeup_flush_threads(bp, (int)(bp->result != EOK));

        /* 
         * If hiFlushLsn has changed, check the domain's
         * vd.waitLazyQ's using the io thread.
         */
        if ((bfap->dirtyBufList.accFwd == bp) &&
            (bp->lock.state & LOG_PAGE)) {

            if ( (dmnP->contBits & CK_WAITQ) == 0 ) {
                contBits |= CK_WAITQ;
            }
        }

        RM_ACCESSLIST( bp, FALSE );
        bp->flushSeq = nilLSN;
        
        /*
         * Signal a waiting migrate thread.
         */
        
        if (bp->lock.state & THROTTLE) {
            --bfap->migPagesPending;
            if (bfap->migWait && bfap->migPagesPending <= bfap->migWait ) { 
                bfap->migWait = 0;
                cond_signal( &bfap->migWait );
            }
        }        
        
        mutex_unlock( &bfap->bfIoLock );
    }

    /*
     * If the buffer is the first entry on the lsn list,
     * the lowest (oldest) outstanding lsn has changed.
     *
     * Remove the buffer from the domain's lsn list.
     */

    if ( bp->lsnFwd != NULL ) {

        /* Seize the domainT.lsnLock while manipulating the lsnList chain. */
        mutex_lock(&dmnP->lsnLock);

        lsnp = &dmnP->lsnList;

        if ( lsnp->lsnFwd == bp ) {

            /*
             * buffer was at front of list
             */

            if ( lsnp->length > 1 ) {
                ftx_set_dirtybufla( dmnP, bp->lsnFwd->origLogRec );
            } else {
                /* There are no more dirty buffers on the lsn list */
                ftx_set_dirtybufla( dmnP, logNilRecord );
            }

            /*
             * Wakeup any waiters on the oldest outstanding
             * lsn changing.
             */
            if ( dmnP->pinBlockWait == 1 ) {
                if (AdvfsLockStats) {
                    AdvfsLockStats->pinBlockSignal++;
                }

                cond_signal( &dmnP->pinBlockCv );

            } else if ( dmnP->pinBlockWait > 1 ) {
                if (AdvfsLockStats) {
                    AdvfsLockStats->pinBlockBroadcast++;
                }

                cond_broadcast( &dmnP->pinBlockCv );
            }

            /* The meta data for the oldest log entry just got written, */
            /* so a new oldest log entry needs to be recalculated. */
            *retestfirst = 1;
        }

        bp->lsnFwd->lsnBwd = bp->lsnBwd;
        bp->lsnBwd->lsnFwd = bp->lsnFwd;
        bp->lsnFwd = bp->lsnBwd = NULL;

        /*
         * Decr length of lsn list
         */
        lsnp->length--;
        mutex_unlock(&dmnP->lsnLock);
    }

    /*
     * Clear addresses of log recs associated with changes to this buffer.
     * Other code depends on the fact that origLogRec does not change
     * while the buffer is on the lsnList.
     */
    bp->origLogRec.lsn = bp->currentLogRec.lsn = nilLSN;

    /*
     * Send a message to the io thread if the log
     * needs to be flushed or pinblock needs to be
     * continued.
     */
    if ( contBits ) {

        dmnP->contBits |= contBits;

        MS_SMP_ASSERT( bfap->dmnP->logVdRadId != -1 );
        /* this means the log has NOT been initialized 
         * if the log isn't initialized, we can't do
         * log I/O...  panic the system
         */
        radId = bfap->dmnP->logVdRadId;
        if ( (IoMsgQH[radId] == NULL) && (bs_io_rad_start(radId) != 0) ){
            int i;

            for(i = 0; i < nrads; i++){
                /* if the RAD isn't valid and we can't start it, find
                 * a valid one!!
                 */
                if (IoMsgQH[i] != NULL){
                    radId = i;
                    break;
                }
            }
            MS_SMP_ASSERT( IoMsgQH[radId] != NULL );
        }

        msg = (ioThreadMsgT *)msgq_alloc_msg( IoMsgQH[radId] );
        if (msg) {
            msg->msgType = LF_PB_CONT;
            msg->u_msg.dmnId = bfap->dmnP->domainId;
            msgq_send_msg( IoMsgQH[radId], msg );
        } else {
            ADVFS_SAD0("bs_io_complete: no io thread msg buf");
        }
    }
clearSignal:

    bp->metaCheck = 0;  /* I/O completion data validity check is now off */

    /* when the state is KEEPDIRTY we do not clear GETPAGE_WRITE and DIRTY
     * because the vm_page was written while still marked pg_dirty.
     */
    bp->lock.state &= (bp->lock.state & KEEPDIRTY) ?
        ~(IO_TRANS | READING | COPY | WRITING | BUSY | IO_COUNTED |
          LOG_PAGE | DIO_REMOVING | KEEPDIRTY | THROTTLE) :
        ~(IO_TRANS | READING | COPY | WRITING | DIRTY | BUSY | IO_COUNTED |
          LOG_PAGE | GETPAGE_WRITE | DIO_REMOVING | THROTTLE);

    if ( bp->lock.waiting > 0 ) {
        
        if ( bp->lock.waiting == 1 ) {
            if (AdvfsLockStats) {
                AdvfsLockStats->usageStats[ bp->lock.hdr.lkUsage ].signal++;
                AdvfsLockStats->bufSignal++;
            }

            cond_signal( &bp->lock.bufCond );
        } else {
            if (AdvfsLockStats) {
                AdvfsLockStats->usageStats[ bp->lock.hdr.lkUsage ].broadcast++;
                AdvfsLockStats->bufBroadcast++;
            }

            cond_broadcast( &bp->lock.bufCond );
        }
    }

    /*
     * If this is an Async direct I/O
     * we have some clean up work to do.
     */
    if (bp->directIO && bp->aio_bp) {

        /* Pass the I/O result to the AIO's done routine
         * if we don't already have an error to report.
         */
        if (bp->aio_bp->b_error == EOK)
            bp->aio_bp->b_error = bp->result;

        if ( !(bp->aio_bp->b_flags & B_READ) && (bp->aio_bp->b_vp) ) {
             bs_aio_write_cleanup( bp->aio_bp->b_vp );
        }

        /* Now invoke aio completion routine; may deallocate bp->aio_bp */
        bp->aio_bp->b_iodone(bp->aio_bp);
    }

    /* The following code was in bs_osf_complete(). It has been moved here
     * because the  call to  ubc_page_release() at the end of bs_io_complete(),
     * may trigger the deallocation of the bsBuf.
     */

    /* Retrieve and reset any ubc_flags for ubc_page_release */
    ubc_flags = B_DONE | bp->ubc_flags;
    bp->ubc_flags = 0;

    if (!bp->actRangep) {
        /* Unlock prior to calling ubc_page release. */
        mutex_unlock(&bp->bufLock);
    }
    else {
        /* There is an active range associated with this buffer.
         * Decrement the # IOs outstanding in the range.
         */
        MS_SMP_ASSERT( bp->actRangep->arIosOutstanding > 0 );
        mutex_lock( &bp->bfAccess->actRangeLock );
        bp->actRangep->arIosOutstanding--;
        if (bp->directIO && bp->aio_bp) {
            /* An aio was completed; we need to release the lock
             * and then free the associated bsBuf structure. Do
             * this check before releasing the bufLock.
             * Remember to clean up bsBuf after we drop locks.
             */
            MS_SMP_ASSERT( bp->lock.waiting == 0 );
            free_bsbuf = 1;

            /* If this completes all I/O on an activeRange, then
             * we must clean up and free the actRangeT in addition
             * to the private bsBuf.
             */
            if ( bp->actRangep->arIosOutstanding == 0 ) {
                remove_actRange_from_list(bp->bfAccess,
                                          bp->actRangep);
                free_act_range = 1;
            }
        }
        mutex_unlock( &bp->bfAccess->actRangeLock );
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
        mutex_unlock(&bp->bufLock);

        /* free this stuff now that locks are dropped. */
        if (free_act_range)
            ms_free(bp->actRangep);
        if (free_bsbuf)
            bs_free_bsbuf(bp);
    }

    /* Release the UBC page except for raw IO requests that
     * do not have a UBC page, or errors paths where the caller 
     * did not make the page busy.  Caller is responsible for 
     * dealing with UBC page contents if an I/O error occurs.
     * After this call to ubc_page_release(), the bsBuf should
     * not be referenced unless the caller is certain that the
     * pg_hold count is up on the vm_page.  This is because
     * if the pg_hold count is not up, ubc_page_release() will
     * put the vm_page onto the UBC's LRU.  Another thread may
     * then recycle the vm_page, which will cause the bsBuf
     * to be deallocated.
     */
    if (release_ubc_page) {
        ubc_page_release(bp->vmpage, ubc_flags);
    }
}

/*
 * check_cont_bits
 *
 * Run by the background I/O thread when requested
 * by a message from bs_io_complete.  Check if a log flush
 * continue or a pin block continue is needed.
 * 
 * SMP: 1. nothing locked when called.
 *
 * The bs_io_thread() checks to see if the domain is NULL.
 */

void
check_cont_bits( domainT *dmnP, int src )
{
    /*
     * Racy test is ok.  If we miss the
     * bits being set, the io thread (guaranteed)
     * or a caller to pin, ref, unpin, deref will
     * see them set.
     */
    if ( ! dmnP->contBits ) {
        return;
    }

    mutex_lock( &dmnP->lsnLock );

    if ( dmnP->contBits & CK_WAITQ ) {

        int vdi = 1;
        int vd_count = 0;
        struct vd *vdp;

        dmnP->contBits &= ~CK_WAITQ;
        mutex_unlock( &dmnP->lsnLock );

        while( vd_count < dmnP->vdCnt && vdi <= BS_MAX_VDI ) {
            
            if ( vdp = vd_htop_if_valid(vdi, dmnP, TRUE, TRUE) ) {
                /* call vd_htop_if_valid with zombie_ok = TRUE since */
                /* this routine is called by bs_io_thread and we want */
                /* to start the IO even if this volume is being removed. */

                wait_to_readyq( vdp );

                /* If there are any buffers on the lazy (consolidate) 
                 * queue, start some I/Os from the lazy queue now.
                 * (readyQ length was checked in wait_to_readyq()).
                 */
                if ( vdp->consolQ.ioQLen ) {
                    startiocalls[1]++;
                    bs_startio( vdp, IO_SOMEFLUSH );
                } else {
                    notstartiocalls[1]++;
                }
                vd_dec_refcnt( vdp );
                vd_count++;
            }
            vdi++;
        }
    } else
        mutex_unlock( &dmnP->lsnLock );
}


/*
 * bs_pinblock - Starts the process for flushing all buffers that have
 *               been modified for files within this domain.  Flushes up
 *               to an lsn passed in, typically the logTrimLsn.  
 * 
 * SMP: seizes the domainT.lsnLock
 */

void
bs_pinblock( 
    domainT *dmnP,        /* in */
    lsnT lsnToWriteTo     /* in */
    )
{
    mutex_lock( &dmnP->lsnLock );

    if ( LSN_GT(lsnToWriteTo, dmnP->writeToLsn) ) {

        dmnP->writeToLsn = lsnToWriteTo;
        lsn_io_list( dmnP);
    }

    mutex_unlock( &dmnP->lsnLock );
}

/*
 * bs_pinblock_sync
 *
 * Allow caller to block to wait for the domain's
 * oldest outstanding lsn to be greater than the given lsn.
 * Returns TRUE if the process blocked waiting for the buffers to
 * be written or if the process would have blocked if noBlock had not 
 * been set. Returns FALSE if it did not block.
 *
 * SMP: Seizes dmnP->lsnLock while checking the progress of the
 *      pinblock, but this will be released if the thread blocks.
 */

int
bs_pinblock_sync(
    domainT *dmnP,        /* in */
    lsnT waitLsn,           /* in */
    int noBlock             /* in */
    )
{
    int blocked = 0,  wait = 0;
    struct processor *myprocessor;        /* used by mark_bio_wait */

    mutex_lock( &dmnP->lsnLock );

    while( dmnP->lsnList.length &&
            LSN_LTE( dmnP->lsnList.lsnFwd->origLogRec.lsn, waitLsn ) ) {

        if (noBlock) {
            /* caller only wants to know if we would block */
            mutex_unlock( &dmnP->lsnLock );
            return 1;
        }

        if (AdvfsLockStats) {
            if (wait) {
                AdvfsLockStats->pinBlockReWait++;
            } else {
                wait = 1;
            }
        
            AdvfsLockStats->pinBlockWait++;
        }

        dmnP->pinBlockWait++;
        mark_bio_wait;
        cond_wait( &dmnP->pinBlockCv, &dmnP->lsnLock );
        unmark_bio_wait;
        dmnP->pinBlockWait--;
        blocked = 1;
    }

    mutex_unlock( &dmnP->lsnLock );

    return blocked;
}

/*
 * lsn_io_list()
 *
 * Create a list of I/O descriptors from the front of 
 * the domain's lsn list, place them onto the blocking queue,
 * and start I/O.
 * 
 * SMP: 1. Caller must hold the dmnP->lsnLock which may be released
 *         and reseized in this routine.
 *      2. Will seize bp->bufLock for buffers that need to be flushed.
 */

static
void
lsn_io_list( struct domain *dmnP) /* in */
{
    struct bsBufHdr *lsnp = &dmnP->lsnList;
    struct bsBuf *bp, *nextbp;
    ioDescT *tmp;
    bfAccessT *logBfap;
    ioDescT *ioListp, *ioListPart;
    int len, listLenPart;
    lsnT origLsn;
    int noqfnd, couldnt_hold;

    MS_SMP_ASSERT(SLOCK_HOLDER(&dmnP->lsnLock.mutex));

    /* we only run one lsn_io_list at a time - before this check was in
     * bs_pinblock, but there was a window where lsn_io_list could end 
     * without picking up the latest writeToLsn, so do the flag setting
     * and checking here now.
     */
    if (dmnP->pinBlockRunning)
        return;

    dmnP->pinBlockRunning = TRUE;

    logBfap = dmnP->logAccessp;

loop:
    /*
     * If the lsn list is empty or the oldest lsn
     * on the list is beyond the writeToLsn, there
     * is nothing to do.
     */
    if ( lsnp->length == 0 || 
        LSN_GT( lsnp->lsnFwd->origLogRec.lsn, 
                dmnP->writeToLsn ) ) {
        dmnP->pinBlockRunning = FALSE;
        return;
    }

    len = 0;
    ioListp = NULL;

    bp = lsnp->lsnFwd;
    MS_SMP_ASSERT( bp != (struct bsBuf *)(NULL) );

    /* bp->origLogRec can't change while on the lsnList; no buffer lock */
    while( bp != (struct bsBuf *)lsnp &&
           LSN_LTE( bp->origLogRec.lsn, dmnP->writeToLsn) ) {
            
        MS_SMP_ASSERT( LSN_LTE(bp->currentLogRec.lsn, logBfap->hiFlushLsn) );

        /* If this buffer is already BUSY, either because this routine already
         * marked it so or because another thread did so, then skip it since
         * it is already on blocking or device queue.  No need to lock the
         * buffer since the BUSY bit won't be cleared in bs_io_complete() until
         * after it gets removed from the lsnList, and we have that lock. 
         */
        if ( bp->lock.state & BUSY ) {
            bp = bp->lsnFwd;
            continue;
        }
        
        couldnt_hold = advfs_page_get(bp, UBC_GET_WRITEHOLD);
        if (couldnt_hold) {
            /* The ubc may be in the process of exchanging the page
             * for numa locality or bigpage consolidation.  We need to
             * wait for it to finish, but we have to drop our locks and
             * there are no guarantees the page will be valid on wake up.
             */
            if (couldnt_hold == UBC_GET_WAIT_EXCHANGE) {
                ubc_fs_page_wait(bp->vmpage, &dmnP->lsnLock.mutex);
                mutex_lock(&dmnP->lsnLock);
                if (ioListp) {
                    /* Point to the last buffer on our accumulated list and
                     * continue on to the next buffer on the lsnlist, which
                     * if we are lucky is this same buffer.  This avoids
                     * having to retraverse the list past these buffers.
                     */
                    bp = ioListp->bwd->bsBuf->lsnFwd;
                    continue;
                } else {
                    goto loop;
                }
            }

            /* it must be busy from another I/O thead and we can skip it */
            bp = bp->lsnFwd;
            continue;
        }

        if ( !mutex_lock_try( &bp->bufLock.mutex )) {
            /* try to reseize in order.  Buffers are added only at the end
             * of the lsnList, so we don't have to worry about buffers 
             * having been inserted in front of this one.  We do have to be
             * concerned about the buffer still being on the lsnList, however.
             */
            mutex_unlock( &dmnP->lsnLock );
            mutex_lock( &bp->bufLock );
            mutex_lock( &dmnP->lsnLock );
            if (!lsnp->lsnFwd ||
                 LSN_GT( bp->currentLogRec.lsn, logBfap->hiFlushLsn ) )
            {
                /* this buffer no longer on lsnList or has changed position,
                 * so just skip it!  Enqueue any buffers we have accumulated 
                 * and restart the traversal of lsnList.
                 */
                mutex_unlock( &bp->bufLock );
                if ( len ) {
                    mutex_unlock( &dmnP->lsnLock );
                    bs_q_list( ioListp, len, bs_q_blocking );
                    mutex_lock( &dmnP->lsnLock );   /* should get this OK */
                }
                /* Drop UBC page (pg_hold count) reference. */
                ubc_page_release(bp->vmpage, B_WRITE);
                goto loop;
            }
        }

        /*
         * Every time this routine drops the lsnLock and bufLock to call
         * bs_q_list(), the I/O could complete for this buffer before
         * locks are acquired again.  If there is no writeRef
         * on this buffer it may have been placed on the freelist and
         * reused for a different page.  Save the origLogRec.lsn to
         * compare whenever locks are reaquired to make sure we have the
         * same buffer prior to changing the buffer's state.
         */
        origLsn = bp->origLogRec.lsn;

        if (bp->lock.state & (FLUSH | IO_TRANS)) {
            /*
             * We will sleep waiting on FLUSH and/or IO_TRANS
             * so write out the buffers and begin again.
             */
            mutex_unlock( &dmnP->lsnLock );
            if ( len ) {
                mutex_unlock( &bp->bufLock );
                bs_q_list( ioListp, len, bs_q_blocking );
                mutex_lock( &bp->bufLock );
                if ( !(bp->lsnFwd) ||
                     !LSN_EQL(bp->origLogRec.lsn, origLsn) ) {
                    mutex_unlock( &bp->bufLock );
                    mutex_lock( &dmnP->lsnLock );
                    /* Drop UBC page (pg_hold count) reference. */
                    ubc_page_release(bp->vmpage, B_WRITE);
                    goto loop;
                }
            }
            wait_state ( bp, FLUSH | IO_TRANS);
            mutex_unlock( &bp->bufLock );
            mutex_lock( &dmnP->lsnLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;

        } else {
            bp->lock.state |= IO_TRANS;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
        }

        /*
         * When we run into the first pinned buffer we're done.
         */
        if ( bp->writeRef != 0 ) {
            ADVFS_SAD0("lsn_io_list: buffer pinned");
        }

        /* Remove any ioDesc's belonging to this buffer that are 
         * currently on lazy I/O queues. Any on blocking or device
         * queues are left alone. Unlock these locks so rm_from_lazyq
         * can lock the vdp->vdIoLock which is higher in the hierarchy.
         */
        mutex_unlock( &dmnP->lsnLock );
        rm_from_lazyq( bp, &ioListPart, &listLenPart, &noqfnd);

        if ( listLenPart ) {
            /* Setup UBC page for IO. Failure should be impossible so
             * just prevent further damage and crash it in development.
             */
            int sts;
            if ((sts = advfs_page_get(bp, UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
                /* must drop and retake bp->bufLock for live dump */
                mutex_unlock(&bp->bufLock);
                ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                domain_panic(dmnP,
                             "lsn_io_list: advfs_page_get(%p) returned %x",
                             bp, sts);
                mutex_lock(&bp->bufLock);

                /* Force advfs io completion to clean up our chains so
                 * we don't just have threads hanging.  The ubc page
                 * remains dirty and since it is metadata probably stays
                 * in cache until the object is reclaimed. 
                 *
                 * Note we have not set BUSY so the page is not released
                 * in bs_io_complete().  We drop our hold, queue up the
                 * previous busy buffer list, and restart since the bsBuf
                 * is pulled off the lsn list in bs_io_complete().
                 */
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                bs_io_complete(bp, &sts);
                ubc_page_release(bp->vmpage, B_WRITE);
                if (len)
                    bs_q_list(ioListp, len, bs_q_blocking);
                mutex_lock(&dmnP->lsnLock);
                goto loop;
            }

            bp->lock.state |= BUSY;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
            bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
            bp->ioqLn = -1;
#endif

            len += listLenPart;
            if ( ioListp == NULL ) {
                ioListp = ioListPart;
            } else {
                /*
                 * Add ioDesc to the end of the list.
                 */
                ioListp->bwd->fwd = ioListPart;
                ioListPart->bwd->fwd = ioListp;
                tmp = ioListPart->bwd;
                ioListPart->bwd = ioListp->bwd;
                ioListp->bwd = tmp;
            }

        } else {
            if ( bp->lsnFwd &&
                 LSN_EQL(bp->origLogRec.lsn, origLsn) ) {
                clear_state ( bp, IO_TRANS );
            }
        }

        /* Because we drop the lsnLock, we can lose our way down the list.
         * If this happens, put any buffers we have accumulated so far onto
         * the blocking queue, and then retraverse the list.  This is fairly
         * fast since any we have moved to the blocking queue already will be 
         * marked BUSY and we will skip them.
         */
        mutex_lock( &dmnP->lsnLock );
        if ( bp->lsnFwd && LSN_EQL(bp->origLogRec.lsn, origLsn) ) {
            nextbp = bp->lsnFwd;
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            bp = nextbp;
        } else {
            /* lost our way!  */
            mutex_unlock( &bp->bufLock );
            if ( len ) {
                mutex_unlock( &dmnP->lsnLock );
                bs_q_list( ioListp, len, bs_q_blocking );
                mutex_lock( &dmnP->lsnLock );
            }
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;
        }
    }

    /* clear the flag for the pinBlockRunning since we are finishing and
     * a new pinblock can change the writeToLsn while we are in bs_q_list().
     */
    dmnP->pinBlockRunning = FALSE;

    /*
     * Write the list we've generated to the blocking queue.
     */
    if ( len ) {
        mutex_unlock( &dmnP->lsnLock );
        bs_q_list( ioListp, len, bs_q_blocking );
        mutex_lock( &dmnP->lsnLock );
    }
}

/*
 * bs_lsnList_flush - Starts flushing dirty metadata cache pages
 *               on a domain's lsnList. Attempts to flush buffers
 *               up to the log's highest flushed LSN passed in.
 *               The caller must call log_flush() prior to calling
 *               this routine so that metadata transactions are
 *               first committed to disk.
 *               This flush routine intentionally skips flushing
 *               any pinned page or page under transition.
 *               
 *               A list of metadata pages to flush are put onto
 *               the I/O blocking queue.
 *
 * Note: This routine flushes on an advisory basis unlike pinblock's
 *       mandatory flushing. No guarantees are made that the
 *       metadata actually gets flushed.
 *               
 * SMP: 1. Seizes and releases the domainT.lsnLock.
 *      2. Will seize bp->bufLock for buffers that need to be flushed.
 */

void
bs_lsnList_flush(struct domain *dmnP)   /* in */
{
    struct bsBufHdr *lsnp = &dmnP->lsnList;
    struct bsBuf *bp, *nextbp;
    ioDescT *tmp;
    bfAccessT *logBfap;
    ioDescT *ioListp, *ioListPart;
    int len, listLenPart;
    lsnT origLsn;
    int noqfnd;

    mutex_lock( &dmnP->lsnLock );

    /* Only run one domain's metadata lsnList flush at a time. 
     * If a pinblock flush is in progress, then skip this flush.
     */
    if (dmnP->lsnListFlushing || dmnP->pinBlockRunning) {
        mutex_unlock( &dmnP->lsnLock );
        return;
    }
    dmnP->lsnListFlushing = TRUE;

    /* Write buffers up to the highest flushed Log LSN. */
    logBfap = dmnP->logAccessp;

loop:
    len = 0;
    ioListp = NULL;

    /* Stop flush if the lsn list is empty. Reset the flush flag. */
    if (!lsnp->length) {
        dmnP->lsnListFlushing = FALSE;
        mutex_unlock( &dmnP->lsnLock );
        return;
    }

    bp = lsnp->lsnFwd;
    MS_SMP_ASSERT( bp != (struct bsBuf *)(NULL) );

    /* bp->origLogRec can't change while on the lsnList; no buffer lock */
    while( bp != (struct bsBuf *)lsnp &&
           LSN_LTE( bp->origLogRec.lsn, logBfap->hiFlushLsn) ) {
        /* If this buffer is already BUSY, either because this routine already
         * marked it so or because another thread did so, then skip it since
         * it is already on blocking or device queue.  No need to lock the
         * buffer since the BUSY bit won't be cleared in bs_io_complete() until
         * after it gets removed from the lsnList, and we have that lock. 
         *
         * Because this is an advisory flush we can also skip transitioning
         * buffers in FLUSH or IO_TRANS without the need to lock them.
         */
        if ( bp->lock.state & (BUSY | FLUSH | IO_TRANS)) {
            bp = bp->lsnFwd;
            continue;
        }
        
        /* this is only an advisory flush so it is ok to just skip pages */
        if (advfs_page_get(bp, UBC_GET_WRITEHOLD)) {
            bp = bp->lsnFwd;
            continue;
        }

        if ( !mutex_lock_try( &bp->bufLock.mutex )) {
            nextbp = bp->lsnFwd;
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            bp = nextbp;
            continue;
        }

        /* Skip currently pinned pages.
         * Also skip repinned pages whose currentLogRec LSN is higher
         * than the highest flushed log LSN. 
         */
        if (bp->writeRef ||
            LSN_GT(bp->currentLogRec.lsn, logBfap->hiFlushLsn)) {
            nextbp = bp->lsnFwd;
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            bp = nextbp;
            continue;
        }
               
        /*
         * Every time this routine drops the lsnLock and bufLock to call
         * bs_q_list(), the I/O could complete for this buffer before
         * locks are acquired again. Save the origLogRec.lsn to compare
         * whenever locks are reaquired to make sure we have the
         * same buffer prior to changing the buffer's state.
         */
         origLsn = bp->origLogRec.lsn;

        /* If the FLUSH and IO_TRANS buffer states are set, then
         * skip the buffer instead of waiting for them to clear.
         * Otherwise, we can just go ahead and set the IO_TRANS flag.
         * The FLUSH flag does not need to be set. 
         */
            
        if ( bp->lock.state & (FLUSH | IO_TRANS)) {
            nextbp = bp->lsnFwd;
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            bp = nextbp;
            continue;
        } else {
            bp->lock.state |= IO_TRANS;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
        }

        /* Remove any ioDesc's belonging to this buffer that are 
         * currently on lazy I/O queues. Any on blocking or device
         * queues are left alone. Unlock these locks so rm_from_lazyq
         * can lock the vdp->vdIoLock which is higher in the hierarchy.
         */
        mutex_unlock( &dmnP->lsnLock );
        rm_from_lazyq( bp, &ioListPart, &listLenPart, &noqfnd);

        if ( listLenPart ) {
            /* Setup UBC page for IO. Failure should be impossible so
             * just prevent further damage and crash it in development.
             */
            int sts;
            if ((sts = advfs_page_get(bp, UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
                /* must drop and retake bp->bufLock for live dump */
                mutex_unlock(&bp->bufLock);
                ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                domain_panic(dmnP,
                             "bs_lsnList_flush: advfs_page_get(%p) returned %x",
                             bp, sts);
                mutex_lock(&bp->bufLock);

                /* Force advfs io completion to clean up our chains so
                 * we don't just have threads hanging.  The ubc page
                 * remains dirty and since it is metadata probably stays
                 * in cache until the object is reclaimed. 
                 *
                 * Note we have not set BUSY so the page is not released
                 * in bs_io_complete().  We drop our hold, queue up the
                 * previous busy buffer list, and restart since the bsBuf
                 * is pulled off the lsn list in bs_io_complete().
                 */
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
                bs_io_complete(bp, &sts);
                ubc_page_release(bp->vmpage, B_WRITE);
                if (len)
                    bs_q_list(ioListp, len, bs_q_blocking);
                mutex_lock(&dmnP->lsnLock);
                goto loop;
            }

            bp->lock.state |= BUSY;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
            bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
            bp->ioqLn = -1;
#endif

            len += listLenPart;
            if ( ioListp == NULL ) {
                ioListp = ioListPart;
            } else {
                /*
                 * Add ioDesc to the end of the list.
                 */
                ioListp->bwd->fwd = ioListPart;
                ioListPart->bwd->fwd = ioListp;
                tmp = ioListPart->bwd;
                ioListPart->bwd = ioListp->bwd;
                ioListp->bwd = tmp;
            }

        } else {
            if ( bp->lsnFwd &&
                 LSN_EQL(bp->origLogRec.lsn, origLsn) ) {
                clear_state ( bp, IO_TRANS );
            }
        }

        /* Because we drop the lsnLock, we can lose our way down the list.
         * If this happens, put any buffers we have accumulated so far onto
         * the blocking queue, and then retraverse the list.  This is fairly
         * fast since any we have moved to the blocking queue already will be 
         * marked BUSY and we will skip them.
         */
        mutex_lock( &dmnP->lsnLock );
        if ( bp->lsnFwd && LSN_EQL(bp->origLogRec.lsn, origLsn) ) {
            nextbp = bp->lsnFwd;
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            bp = nextbp;
        } else {
            /* lost our way!  */
            mutex_unlock( &bp->bufLock );
            if ( len ) {
                mutex_unlock( &dmnP->lsnLock );
                bs_q_list( ioListp, len, bs_q_blocking );
                mutex_lock( &dmnP->lsnLock );
            }
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;
        }
    }

    /* Write the list we've generated to the blocking queue. */
    if ( len ) {
        mutex_unlock( &dmnP->lsnLock );
        bs_q_list( ioListp, len, bs_q_blocking );
        mutex_lock( &dmnP->lsnLock );
    }
 
    /* Reset the flush flag so another metadata flush can start. */
    dmnP->lsnListFlushing = FALSE;
    mutex_unlock( &dmnP->lsnLock );
}


/*
 * bs_q_lazy
 *
 * Called from bs_q_list with a list of ioDesc for a single device.  
 * Place the ioDesc structs onto one of the device's lazy queues; this
 * means that the ioDescs could be put onto the waitQ, the readyQ, or the
 * consolQ.
 *
 * The ioDesc list is currently for only a single bitfile page so 
 * there is usually only one ioDesc on the list.
 *
 * SMP: 1. ioDesc are protected by the IO_TRANS state; no locks held when
 *         this routine is entered.
 *      2. bp->bufLock will be seized while checking and/or modifying the
 *         bsBuf state.
 *
 */

void
bs_q_lazy( 
    ioDescT *ioListp,        /* in */
    int len                  /* in */
    )
{
    struct vd *vdp;
    struct bsBuf *bp;
    ioDescT *iop, *tmp;
    int count,i;
    extern u_int smsync_period;
    domainT *dmnp;

    MS_SMP_ASSERT(ioListp->bsBuf->bfAccess->dmnP->vdpTbl);
    MS_SMP_ASSERT(ioListp->blkDesc.vdIndex <= BS_MAX_VDI);
    vdp = VD_HTOP(ioListp->blkDesc.vdIndex, ioListp->bsBuf->bfAccess->dmnP);
    dmnp = ioListp->bsBuf->bfAccess->dmnP;

    /* 
     * Charge the process for i/o
     * Initialize the sync_stamp
     */
    tmp = ioListp;
    i = len - 1;
    do {
        bp = tmp->bsBuf;
        mutex_lock( &bp->bufLock );
        MS_SMP_ASSERT( bp->lock.state & IO_TRANS );  /* I think this is true */
        if ( !(bp->lock.state & IO_COUNTED) ) {
            if (bp->lock.state & READING)
                u.u_tru.tru_inblock++;
            else
                u.u_tru.tru_oublock++;

            bp->lock.state |= IO_COUNTED;
        }

        /*
         * The default smsync_policy provides the behavior of sync'ing
         * buffers after they age for the specified time interval,
         * regardless of how those buffers get used.  The aging period 
         * begins the first time the buffer gets placed on the lazyQ, and
         * gets reset when the I/O is performed.
         *
         * The M_SMSYNC2 policy provides the behavior of restarting the 
         * clock on a buffer whenever it has a new lazy I/O request placed
         * against it.  This policy decreases the total number of I/O's 
         * performed.  It is appropriate only in those circumstances where
         * intermediate states of the buffer are useless (such as some 
         * databases or build systems).
         */
        if ( bp->sync_stamp == 0 || dmnp->smsync_policy&M_SMSYNC2 )
            bp->sync_stamp = sched_tick;
        mutex_unlock( &bp->bufLock );
        tmp = tmp->fwd;
    } while ( i-- );

    /*
     * Wait until we've acquired some critical mass
     * of buffers before we start the device.
     */
    if ( vdp->consolQ.ioQLen ) {
        MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->consolQ, FALSE );
        startiocalls[3]++;
        bs_startio( vdp, IO_SOMEFLUSH );
    } else {
        notstartiocalls[3]++;
    }

    /* If smoothsync is enabled, call smsync_to_readyq(), otherwise,
     * check to see if we need to start some I/Os.
     */
    if ( smsync_period ) {
        /*
         * Move buffers from aged smoothsync queues to the readyQ.
         */
        smsync_to_readyq( vdp, IO_SOMEFLUSH );

    } else if ( vdp->readyLazyQ.ioQLen >= vdp->readyLazyQ.lenLimit ) {

        /* If the readyLazyQ is above threshold, move its buffers
         * to the consolidate queue and start some I/Os from the
         * lazy (consolidate) queue.
         */
        if ( ready_to_consolq( vdp ) ) {
            startiocalls[10]++;
            bs_startio( vdp, IO_SOMEFLUSH );
        }
    }

    /*
     * If this buffer cannot be written because 
     * the log has not been written, install the
     * ioDesc's on the waitLazyQ for the device.
     * They can be sorted later.
     */
    if ( ! LSN_EQ_NIL( ioListp->bsBuf->origLogRec.lsn ) ) {

        bfAccessT *bfap;

        /*
         * Check if buffer can be written.
         */
        bfap = ioListp->bsBuf->bfAccess->dmnP->logAccessp;

        if ( LSN_GT( ioListp->bsBuf->currentLogRec.lsn,
                    bfap->hiFlushLsn ) ) {

            mutex_lock( &vdp->waitLazyQ.ioQLock );
            for( count = len, iop = ioListp; count; count--, iop = iop->fwd ) {
                iop->ioQ = WAIT_LAZY;
                iop->bsBuf->bufDebug |= BSBUF_WAITQ; /* debug */
                SMSYNC_DBG( SMSYNC_DBG_BUF, 
                    aprintf ( "bs_q_lazy:     bsBuf 0x%lx  added onto waitQ  vd 0x%lx\n",
                    iop->bsBuf, vdp ));
            }

            vdp->waitLazyQ.bwd->fwd = ioListp;
            ioListp->bwd->fwd = (ioDescT *)&vdp->waitLazyQ;
            iop = ioListp->bwd;
            ioListp->bwd = vdp->waitLazyQ.bwd;
            vdp->waitLazyQ.bwd = iop;
            vdp->waitLazyQ.ioQLen += len;
            vdp->waitLazyQ.queue_cnt += len;

            MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->waitLazyQ, TRUE );
            mutex_unlock( &vdp->waitLazyQ.ioQLock );
            return;
        }
    }

    if ( smsync_period ) {
        /* Add the buffers onto the smoothsync queue.  */
        add_to_smsync( vdp, ioListp, (u_int)len, NULL, NULL );
    } else {
        /* Sort ioDesc into readyLazyQ.  */
        sort_onto_readyq( vdp, ioListp, len, NULL, NULL );
        MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->readyLazyQ, FALSE );
    }

}

/*
 * Move all ioDesc possible from the waitLazyQ
 * to either the readyLazyQ or, if smoothsync is
 * enabled, to the smSyncQ.
 * Depends on current lsn of owning buffer.
 *
 */

void
wait_to_readyq( struct vd *vdp )
{
    bfAccessT *bfap;
    ioDescT *iop, *start, *end;
    int count;
    extern u_int smsync_period;
    tempQMarkerT *tempQ_marker = NULL;

    mutex_lock( &vdp->waitLazyQ.ioQLock );

    if ( vdp->waitLazyQ.ioQLen == 0 ) {
        mutex_unlock( &vdp->waitLazyQ.ioQLock );
        return;
    }

    /*
     * Check if buffer can be written.
     */
    iop = vdp->waitLazyQ.fwd;
    bfap = iop->bsBuf->bfAccess->dmnP->logAccessp;

    while ( iop != (ioDescT *)&vdp->waitLazyQ ) {
        start = iop;
        count = 0;

        /* 
         * Gather as many ioDesc together as we can.
         */
        while( (iop != (ioDescT *)&vdp->waitLazyQ)  &&
            (! LSN_GT( iop->bsBuf->currentLogRec.lsn,
                    bfap->hiFlushLsn )) ) {
                        
            count++;
            iop = iop->fwd;
        }

        /*
         * If we found any ioDesc, move them to the
         * readyLazyQ or the smSyncQ
         */
        if ( count ) {

            /* Need special case help for sort_onto_readyQ():
             * If we have a very long list we need to sort it using
             * a tempQ and that requires us to malloc a marker struct.
             * Dropping the lock to do the malloc can not be done after
             * we make the queues inconsistent by removing the entries
             * below while leaving them marked as on the waitLazyQ.
             * Here we process what we found if we can get the struct
             * without dropping our lock, otherwise we have to do it the
             * hard way: drop our lock, wait for memory, and restart.
             */
            if (!tempQ_marker && count >= Advfs_sort_factor) {
                tempQ_marker = (tempQMarkerT *)
                    ms_malloc_no_wait(sizeof(tempQMarkerT));
                if (!tempQ_marker) {
                    mutex_unlock(&vdp->waitLazyQ.ioQLock);
                    tempQ_marker = (tempQMarkerT *)
                        ms_malloc(sizeof(tempQMarkerT));
                    mutex_lock(&vdp->waitLazyQ.ioQLock);
                    iop = vdp->waitLazyQ.fwd;
                    continue;
                }
            }

            /* Back up to the last good thing we looked at.  */
            end = iop->bwd;

            /* Remove from the waitLazyQ */
            start->bwd->fwd = end->fwd;
            end->fwd->bwd = start->bwd;
            MS_SMP_ASSERT(vdp->waitLazyQ.ioQLen >= count);
            vdp->waitLazyQ.ioQLen -= count;

            /* hand doubly-linked list off to be added to correct queue */
            end->fwd = start;
            start->bwd = end;
            if ( smsync_period ) {
                add_to_smsync( vdp, start, (u_int)count, 
                               &vdp->waitLazyQ.ioQLock, tempQ_marker );
            } else {
                sort_onto_readyq( vdp, start, count,
                               &vdp->waitLazyQ.ioQLock, tempQ_marker );
            }
            /* The waitLazy queue lock was dropped in add_to_smsync() or
             * sort_onto_readyq(), so reaquire it and reset the iop.
             */
            mutex_lock( &vdp->waitLazyQ.ioQLock );
            iop = vdp->waitLazyQ.fwd;
        } else {
            iop = iop->fwd;
        }
    }

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->waitLazyQ, TRUE );
    mutex_unlock( &vdp->waitLazyQ.ioQLock );

    if (tempQ_marker)
        ms_free(tempQ_marker);

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->readyLazyQ, FALSE );

    /* If the readyLazyQ goes above the threshold, move some off. */
    if ( vdp->readyLazyQ.ioQLen >= vdp->readyLazyQ.lenLimit ) {
        if ( ready_to_consolq( vdp ) ) {
            startiocalls[4]++;
            bs_startio( vdp, IO_SOMEFLUSH );
        }
    }
}
    

/*
 * ready_to_consolq
 *
 * Move ioDesc from the readyLazyQ to the consolQ.
 * Returns 1 if structs were moved, otherwise returns 0.
 *
 * Does not move any buf with IO_TRANS set.  Callers assume
 * that they must clear IO_TRANS for their lazy I/O requests.
 *
 */

int
ready_to_consolq( struct vd *vdp )
{
    ioDescT *ioList, *iop, *start, *end;
    int ioListLen, len;

    /* Nothing to move; return */
    if ( vdp->readyLazyQ.ioQLen == 0 ) {
        return( 0 );
    }

    ioList = NULL;
    ioListLen = 0;

    mutex_lock( &vdp->readyLazyQ.ioQLock );
    mutex_lock( &vdp->consolQ.ioQLock );
    iop = vdp->readyLazyQ.fwd;

    /*
     * Remove any ioDesc whose buffer is not in the IO_TRANS state.
     */
    while( iop != (ioDescT *)&vdp->readyLazyQ ) {
        
        start = iop;
        len = 0;

        /* We only want to move eligible bsBufs down. If there are mutliple
         * i/o descriptors then skip over this iodesc. Migrate will take
         * care of flushing.
         */


        while( iop != (ioDescT *)&vdp->readyLazyQ &&
               !(iop->bsBuf->lock.state & (IO_TRANS | REMOVE_FROM_IOQ)) &&
               (iop->bsBuf->ioList.writeCnt == 1))  {
            len++;
            iop->ioQ = CONSOL;
            SMSYNC_DBG( SMSYNC_DBG_BUF, 
                aprintf ( "readytocons:   bsBuf 0x%lx  vd 0x%lx\n", iop->bsBuf, vdp ));
            iop = iop->fwd;
        }

        /*
         * If we found something add it to the list.
         */
        if ( len ) {
            end = iop->bwd;  /* move to the last elt of the list */
            ioListLen += len;

            /*
             * Remove from the readyLazyq.
             */
            start->bwd->fwd = end->fwd;
            end->fwd->bwd = start->bwd;
            vdp->readyLazyQ.ioQLen -= len;

            /*
             * Install on local list.
             */
            if ( ioList == NULL ) {
                ioList = start;
                start->bwd = end;
                end->fwd = start;
            } else {
                ioList->bwd->fwd = start;
                start->bwd = ioList->bwd;
                end->fwd = ioList;
                ioList->bwd = end;
            }
        } else {
            iop = iop->fwd;
        }
    }

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->readyLazyQ, TRUE );
    mutex_unlock( &vdp->readyLazyQ.ioQLock );

    if ( ioList == NULL ) {
        mutex_unlock( &vdp->consolQ.ioQLock );
        return( 0 );
    }

    iop = ioList;

    /*
     * Add the list to the end of the consolidate queue.
     */
    vdp->consolQ.bwd->fwd = ioList;
    ioList->bwd->fwd = (ioDescT *)&vdp->consolQ;
    iop = ioList->bwd;
    ioList->bwd = vdp->consolQ.bwd;
    vdp->consolQ.bwd = iop;
    vdp->consolQ.ioQLen += ioListLen;
    vdp->consolQ.queue_cnt += ioListLen;

    /*
     * This updates a histogram of the # of ioDesc structs moved from
     * to ready to the consolidate queue on each pass thru this routine.
     */
#ifdef ADVFS_SMP_ASSERT
    HISTOGRAM_UPDATE( Rdy2ConsolSz, ioListLen );
#endif

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->consolQ, TRUE );

    mutex_unlock( &vdp->consolQ.ioQLock );

    return( 1 );
}


/* sort_to_list
 *
 * Local routine to place ioDesc structs onto a list sorted by disk block.
 * The incoming/outgoing sorted_list is a doubly-linked list that 
 * has a temporary list header.  The ioQueue value is passed here since
 * all list members will be touched, so we can mark each ioDescT with the
 * queue that it will be placed onto.  The calling routine may be able to
 * get away without touching each of the members of this list.
 */
static void
sort_to_list(
    ioDescT *iop,               /* in */
    ioDescT *sorted_list_head,  /* in */
    enum ioQueue ioq            /* in; queue that these will be put on */
    )
{
    ioDescT *prev = sorted_list_head->bwd;

    /* Don't walk down entire list to see that it goes on the front */
    if ( sorted_list_head->fwd == sorted_list_head  ||
         iop->blkDesc.vdBlk < sorted_list_head->fwd->blkDesc.vdBlk ) {
        /* Insert at head of list.  */
        iop->ioQ                   = ioq;
        iop->bwd                   = (ioDescT *)sorted_list_head;
        iop->fwd                   = sorted_list_head->fwd;
        sorted_list_head->fwd->bwd = iop;
        sorted_list_head->fwd      = iop;
        return;
    }

    while( prev != sorted_list_head ) {
        if ( iop->blkDesc.vdBlk >= prev->blkDesc.vdBlk ) {
            /*
             * Insert buffer in list after prev.
             */
            iop->ioQ = ioq;
            iop->fwd = prev->fwd;
            iop->bwd = prev;
            prev->fwd->bwd = iop;
            prev->fwd = iop;
            return;
        }
        prev = prev->bwd;
    }

    /* should never get here */
    MS_SMP_ASSERT( prev != sorted_list_head );
}

/* This routine attempts to keep several temporary lists of buffers on 
 * the temporary queue, separated by thread id. It is called with the
 * lock for the temporary queue held and also the lock for the I/O 
 * queue the ioDescs are currently on.
 */
static void
adjust_temporary_queue_elements( tempQMarkerT *tempQ_marker,
                                 ioDescT *ioListp, int cnt, vdT *vdp )
{
    struct thread *tid;
    tempQMarkerT *local_tmpQ_marker;
    ioDescT *last, *iop, *first;

    /* Walk this list resetting 2 fields so that it is apparent
     * to removal code that each ioDesc is on a temporary queue.
     */
    iop = first = ioListp;
    do {
        iop->ioQ = TEMPORARYQ;
        iop = iop->fwd;
    } while (iop != first);

    tid = current_thread();
    if (vdp->tempQ.cnt == 0) {
        tempQ_marker->mfwd = &vdp->tempQ;
        tempQ_marker->mbwd = &vdp->tempQ;
        vdp->tempQ.mfwd = tempQ_marker;
        vdp->tempQ.mbwd = tempQ_marker;

        tempQ_marker->fwd = ioListp;
        tempQ_marker->bwd = ioListp->bwd;
        ioListp->bwd->fwd = (ioDescT *)tempQ_marker;
        ioListp->bwd = (ioDescT *)tempQ_marker;

        tempQ_marker->thd_id = tid;

        vdp->tempQ.cnt = 1;
    } else {
        /* search for thread id already in the temporary queue */
        local_tmpQ_marker = vdp->tempQ.mfwd;
        while ((local_tmpQ_marker->thd_id != tid) && (local_tmpQ_marker != &vdp->tempQ))
            local_tmpQ_marker = local_tmpQ_marker->mfwd;

        if (local_tmpQ_marker == &vdp->tempQ) {
            /* not found, insert new marker and ioList at end of tempQ */
            tempQ_marker->mfwd = &vdp->tempQ;
            tempQ_marker->mbwd = vdp->tempQ.mbwd;
            vdp->tempQ.mbwd->mfwd = tempQ_marker;
            vdp->tempQ.mbwd = tempQ_marker;

            tempQ_marker->fwd = ioListp;
            tempQ_marker->bwd = ioListp->bwd;
            ioListp->bwd->fwd = (ioDescT *)tempQ_marker;
            ioListp->bwd = (ioDescT *)tempQ_marker;

            tempQ_marker->thd_id = tid;

            vdp->tempQ.cnt++;
        }
    }
}

/*
 * sort_onto_readyq
 *
 * Insert a list of ioDesc structs onto given vd's readyLazy queue 
 * so that the resulting chain is sorted into ascending order according
 * to disk block.  The incoming list of ioDesc structs is assumed to 
 * be a doubly-linked list of ioDesc structs with no ioDescHdr.  It is 
 * also assumed to be out-of-order; it will be sorted and added to the 
 * readyLazyQ with each ioDesc placed in the proper, sorted location.
 *
 * SMP: The incoming_lockp is the lock for the incoming list and it may
 * be held on entry to this routine. It will be released in this routine
 * after the incoming ioDescs have been transitioned to the readyLazy
 * queue. For this reason, callers must be sure that walking of the incoming
 * IO queue must be restarted since there is no guarantee that any ioDesc 
 * structs that were on the queue when this routine is called is still on 
 * the same queue when this routine returns.
 */

void
sort_onto_readyq( 
    struct vd *vdp,             /* in */
    ioDescT *iop,               /* in */
    int cnt,                    /* in */
    mutexT *incoming_lockp,     /* in */
    tempQMarkerT *tempQ_marker  /* in */
    )
{
    ioDescHdrT fake_header;
    ioDescT *sorted_list = (ioDescT *)&fake_header;
    ioDescT *next, *last;
    ioDescT *readyp;
    ioDescT *chain_iop = iop;
    int chain_len;
    int cum_added_here;            /* for statistics */
    int added_this_group;
    
    MS_SMP_ASSERT(iop->bwd->fwd == iop);
    MS_SMP_ASSERT( cnt > 0 );

    if (cnt > RdySort[6] )
        RdySort[6] = cnt;        /* Keep track of largest request made */
    RdySort[7]++;                /* # of times thru this routine */

    /* Let's optimize for cases where the incoming 'chain' has only one
     * element.  The code below is optimized for long chains, so there
     * is too much overhead to efficiently handle single-element chains.
     */
    if ( cnt == 1 ) {
        mutex_lock( &vdp->readyLazyQ.ioQLock );
        iop->ioQ = READY_LAZY;
        if ( vdp->readyLazyQ.ioQLen == 0 ) {
            /* ReadyLazyQ is empty; just add this one in. */
            readyp = (ioDescT *)(vdp->readyLazyQ.fwd);
            MS_SMP_ASSERT( readyp == (ioDescT *)&vdp->readyLazyQ );
            readyp->fwd = iop;
            readyp->bwd = iop;
            iop->fwd    = readyp;
            iop->bwd    = readyp;
            RdySort[8]++;
        } else if ( vdp->readyLazyQ.bwd->blkDesc.vdBlk < iop->blkDesc.vdBlk ) {
            /* The incoming element goes at the end of the readyQ; this
             * seems to happen frequently when writing large files
             * sequentially.
             */
            readyp = (ioDescT *)(&vdp->readyLazyQ);
            MS_SMP_ASSERT( vdp->readyLazyQ.fwd != (ioDescT *)&vdp->readyLazyQ );
            iop->fwd    = readyp;
            iop->bwd    = readyp->bwd;
            readyp->bwd->fwd = iop;
            readyp->bwd = iop;
            RdySort[9]++;
        } else if ( vdp->readyLazyQ.fwd->blkDesc.vdBlk > iop->blkDesc.vdBlk ) {
            /* The incoming element goes at the front of the readyQ; this
             * keeps us from having to walk the whole list.
             */
            readyp = (ioDescT *)(&vdp->readyLazyQ);
            MS_SMP_ASSERT( vdp->readyLazyQ.fwd != (ioDescT *)&vdp->readyLazyQ );
            iop->bwd    = readyp;
            iop->fwd    = readyp->fwd;
            readyp->fwd->bwd = iop;
            readyp->fwd = iop;
            RdySort[9]++;
        } else {
            /* Sadly, we must resort to searching for the place to 
             * insert this element.  We will search backwards to 
             * reduce search time for sequential writes when pressure
             * to move the elements off the readyq is low.
             */
            readyp = (ioDescT *)(vdp->readyLazyQ.bwd);
            MS_SMP_ASSERT( vdp->readyLazyQ.fwd != (ioDescT *)&vdp->readyLazyQ );
            while ( readyp != (ioDescT *)&vdp->readyLazyQ &&
                    readyp->blkDesc.vdBlk >= iop->blkDesc.vdBlk ) {
                readyp = readyp->bwd;
            }
            MS_SMP_ASSERT( readyp != (ioDescT *)&vdp->readyLazyQ );
            iop->bwd    = readyp;
            iop->fwd    = readyp->fwd;
            readyp->fwd->bwd = iop;
            readyp->fwd = iop;
            RdySort[10]++;
        }

        /* Update the readyLazy Queue counts. */
        vdp->readyLazyQ.ioQLen++;
        vdp->readyLazyQ.queue_cnt++;

        if (vdp->readyLazyQ.ioQLen > 0)
            MS_SMP_ASSERT(vdp->readyLazyQ.fwd != (ioDescT *)&vdp->readyLazyQ );

        MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->readyLazyQ, TRUE );
        mutex_unlock( &vdp->readyLazyQ.ioQLock );

        if (incoming_lockp)
            mutex_unlock( incoming_lockp );

        return;
    }

    fake_header.ioQLen = 0xface; /* identifying mark */
    fake_header.lenLimit = 0x0;

    /* Special-case code used to sort a long list of incoming buffers,
     * where a long list is defined by Advfs_sort_factor, usually 4k.
     * 
     * To use this code, the caller must have allocated the tempQMarker
     * struct because we can't safely drop the lock to do it here since
     * the incomming list is in an inconsistent state (i.e. it may be
     * marked as on the waitLazyQ, but it was already removed).  If the
     * caller allocated the structure but now the incomming list is
     * shorter than Advfs_sort_factor, we won't do this code.
     * Long list sort method:
     * 1.  Move any entries already on the readyQ to the consolQ.
     * 2.  Loop doing the following until incoming list is empty:
     *     A. Sort the first Advfs_sort_factor elements of incoming list
     *     B. Put this list onto the readyQ.
     *     C. Call ready_to_consolq() to move these down.
     *     D. Call bs_startio() since we know we moved a bunch to the
     *        consolQ. This also provides us with a preemption point.
     */
    if (tempQ_marker && cnt >= Advfs_sort_factor) {
        /* keep some stats */
        RdySort[11]++;
        if (cnt >  RdySort[12])
            RdySort[12] = cnt;

        /* Move any from readyLazyQ to consolQ to empty readyLazyQ */
        if ( vdp->readyLazyQ.ioQLen )
            ready_to_consolq( vdp );

        /* Since we will drop the per-queue locks in this path, we must mark
         * all the ioDescT on the incoming list so rm_ioq()/rm_from_lazyq()
         * will not try to account for removing the ioDescT from one of the
         * legitimate queues, but will be safely able to remove the ioDesc
         * from the temporary queue.
         */
        mutex_lock( &vdp->tempQ.ioQLock );
        adjust_temporary_queue_elements(tempQ_marker, chain_iop, cnt, vdp);

        if (incoming_lockp)
            mutex_unlock( incoming_lockp );

        while ( chain_iop != (ioDescT *)tempQ_marker ) {

            mutex_lock( &vdp->readyLazyQ.ioQLock );
            chain_len = 0;
            sorted_list->fwd = (ioDescT *)&fake_header;
            sorted_list->bwd = (ioDescT *)&fake_header;

            /* Sort the incoming list into sublists of some multiple
             * of readyLazyQ.lenLimit elements. Then insert each
             * subchain onto the readyQ.
             */
            while ( chain_iop != (ioDescT *)tempQ_marker ) {
                next = chain_iop->bwd->fwd = chain_iop->fwd;
                chain_iop->fwd->bwd = chain_iop->bwd;
                sort_to_list( chain_iop, sorted_list, READY_LAZY );
                chain_iop = next;
                if ( ++chain_len >= Advfs_sort_factor )
                    break;
            }

            /* Insert this chain onto the head of the readyLazyQ */
            readyp = (ioDescT *)&vdp->readyLazyQ;
            sorted_list->fwd->bwd = readyp;
            sorted_list->bwd->fwd = readyp->fwd;
            readyp->fwd->bwd = sorted_list->bwd;
            readyp->fwd = sorted_list->fwd;
            
            vdp->readyLazyQ.ioQLen += chain_len;
            vdp->readyLazyQ.queue_cnt += chain_len;

            if (vdp->readyLazyQ.ioQLen > 0)
                MS_SMP_ASSERT(vdp->readyLazyQ.fwd != (ioDescT *)&vdp->readyLazyQ );

            MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->readyLazyQ, TRUE );
            mutex_unlock( &vdp->readyLazyQ.ioQLock );
            mutex_unlock( &vdp->tempQ.ioQLock );

            /* Now be sure some I/Os get started if necessary.
             * This will allow a preemption point as well.
             */
            ready_to_consolq( vdp );
            bs_startio( vdp, IO_SOMEFLUSH );

            /* Restart back at the front of the chain.  The ioDescT
             * pointed to by chain_iop may have been removed from 
             * this list by rm_ioq() or rm_from_lazyq() when the
             * temporary queue lock was dropped above.
             */
            mutex_lock( &vdp->tempQ.ioQLock );
            chain_iop = tempQ_marker->fwd;
        }

        --vdp->tempQ.cnt;
        tempQ_marker->mbwd->mfwd = tempQ_marker->mfwd;
        tempQ_marker->mfwd->mbwd = tempQ_marker->mbwd;

        mutex_unlock( &vdp->tempQ.ioQLock );

        return;
    }

    /* Processing of shorter incoming chains.  Sort/merge them onto
     * the readyQ without dropping the incoming queue lock.
     */
    iop->bwd->fwd = NULL;                /* terminate incoming list */

    mutex_lock( &vdp->readyLazyQ.ioQLock );
    while ( chain_iop ) {                /* NULL when input chain processed */
        cum_added_here = 0;
        chain_len = 0;
        fake_header.fwd = (ioDescT *)&fake_header;
        fake_header.bwd = (ioDescT *)&fake_header;

        /* Sort the original list; this is a really cheap algorithm and
         * to maximize throughput, we sort sublists of up to 512 elements
         * and then insert each subchain onto the readyQ.  In essence this
         * is now a sort/merge onto the readyQ.
         * The fake header is added to make the sort algorithm easier.
         */
        while ( chain_iop ) {
            SMSYNC_DBG( SMSYNC_DBG_BUF,
                aprintf("sortready: bsBuf 0x%lx  added onto readyQ  vd 0x%lx\n",
                chain_iop->bsBuf, vdp ));
            next = chain_iop->fwd;
            sort_to_list( chain_iop, sorted_list, READY_LAZY );
            chain_iop = next;
            if ( ++chain_len >= 512 )
                break;
        }

        /* Now we have a sorted chain, and we want to add these elements
         * onto the readyLazyQ.  
         */
        readyp = (ioDescT *)(vdp->readyLazyQ.fwd);
        if ( readyp == (ioDescT *)&vdp->readyLazyQ ) {
            /* readyLazyQ is empty; insert entire list in one fell swoop */
            readyp->fwd = sorted_list->fwd;
            readyp->bwd = sorted_list->bwd;
            readyp->bwd->fwd = readyp;
            readyp->fwd->bwd = readyp;

            RdySort[0] += chain_len;          /* # elements added in a group */
            RdySort[1]++;                     /* # times we did this */

            vdp->readyLazyQ.ioQLen += chain_len;
            vdp->readyLazyQ.queue_cnt += chain_len;

            if (vdp->readyLazyQ.ioQLen > 0)
                MS_SMP_ASSERT(vdp->readyLazyQ.fwd != (ioDescT *)&vdp->readyLazyQ );

            MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->readyLazyQ, TRUE );

            continue;
        }

        /* Otherwise, add them on one-at-a-time in the correct locations */
        iop = sorted_list->fwd;
        while ( iop != sorted_list ) {
            next = iop->fwd;

            /* Advance along readyLazyQ until we know next place to insert */
            while ( readyp != (ioDescT *)&vdp->readyLazyQ &&
                    readyp->blkDesc.vdBlk < iop->blkDesc.vdBlk ) {
                readyp = readyp->fwd;
            }
            if ( readyp == (ioDescT *)&vdp->readyLazyQ ) {
                break;      /* get out; add rest of list as a group at end */
            }

            MS_SMP_ASSERT( iop->blkDesc.vdBlk <= readyp->blkDesc.vdBlk );

            /* Insert this entry onto the list. As an optimization, try to
             * place as many entries from the sorted list onto the ready
             * queue as belong at this location.
             */
            added_this_group = 1;
            while ( next != sorted_list &&
                    next->blkDesc.vdBlk <= readyp->blkDesc.vdBlk ) {
                next = next->fwd;
                added_this_group++;       /* # elements added as a group */
            }
            last = next->bwd;       /* may == iop if only 1 to be inserted */

            /* put the group on the readyq */
            last->fwd = readyp;
            iop->bwd  = readyp->bwd;
            readyp->bwd->fwd = iop;
            readyp->bwd      = last;

            /* keep track of some statistics to be sure this is working the way
             * we want.
             */
            RdySort[4] += added_this_group;
            RdySort[5]++;
            cum_added_here += added_this_group; /*total # added for this list*/
        
            iop = next;
        }

        if ( iop != sorted_list ) {
            /* We get here if we still have entries on the sorted list, but 
             * have gotten to the end of the readyLazyQ.  Add the remainder
             * of the list in one fell swoop. readyp is head of queue.
             */
            iop->bwd = readyp->bwd;
            sorted_list->bwd->fwd = readyp;
            readyp->bwd->fwd = iop;
            readyp->bwd = sorted_list->bwd;
            RdySort[2] += (chain_len - cum_added_here);
            RdySort[3]++;
        }

        vdp->readyLazyQ.ioQLen += chain_len;
        vdp->readyLazyQ.queue_cnt += chain_len;

        if (vdp->readyLazyQ.ioQLen > 0)
            MS_SMP_ASSERT(vdp->readyLazyQ.fwd != (ioDescT *)&vdp->readyLazyQ );
    }   /* end of subchain loop */

    MS_VERIFY_IOQUEUE_INTEGRITY( &vdp->readyLazyQ, TRUE );

    mutex_unlock( &vdp->readyLazyQ.ioQLock );

    if (incoming_lockp)
        mutex_unlock( incoming_lockp );

    return;
}


/*
 * cache_logflush
 *
 * Determine the largest lsn on the log's dirty
 * list whose page is not pinned.  Flush the log
 * up to that lsn.  (Common code with
 * call_logflush).  Does not sync on log completion.
 */

void
cache_logflush( domainT *dmnP )
{
    bfAccessT *bfap = dmnP->logAccessp;
    struct bsBuf *tail;
    lsnT logPageLsn;

    mutex_lock( &bfap->bfIoLock );

    if ( bfap->dirtyBufList.length ) {

        /*
         * Backup to find last unpinned page
         */
        tail = bfap->dirtyBufList.accBwd;

        /*
         * Pinned pages do not have their flushSeq field
         * set up yet. It is put in at unpin time. Thus
         * we have to pass a valid lsn to the flush routine
         * so find the first unpinned page
         */


        while( tail != (struct bsBuf *)&bfap->dirtyBufList &&
               tail->writeRef != 0 ) {

            tail = tail->accBwd;
        }

        if ( tail != (struct bsBuf *)&bfap->dirtyBufList ) {

            logPageLsn = tail->flushSeq;
            mutex_unlock( &bfap->bfIoLock );
            (void)bs_logflush_start( bfap, logPageLsn );
            return;
        }
    }
    mutex_unlock( &bfap->bfIoLock );
}

/*
 * call_logflush
 *
 * Check if the log page containing the desired lsn
 * has already been unpinned.  If it has, call
 * bs_logflush_start, otherwise call lgr_flush_start.
 * Do the sync in either case.
 *
 * SMP: ASSERT that caller holds the logBfap->bfIoLock.
 */

void
call_logflush( domainT *dmnP, lsnT lsn, int wait )
{
    bfAccessT *bfap = dmnP->logAccessp;
    statusT sts;
    struct bsBuf *tail;
    lsnT logPageLsn;

    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfIoLock.mutex));
    /*
     * Since pinned pages do not have valid lsns (flushSeq)
     * we can only look at unpinned pages (the flushSeq is 
     * only setup at unpin time). The only time we really
     * need to use the logging routines is if the last log
     * page is in the flush range. In that case we must use
     * the log routines so that the last log page is actively
     * unpinned (otherwise it will not be unpinned until it 
     * is full). Unfortunately though, we can approximate 
     * by looking at the last unpinned page otherwise we 
     * have to assume we might encompass the last log page.
     */

    if ( bfap->dirtyBufList.length )
    {
        /*
         * Backup to find last unpinned page
         */
        tail = bfap->dirtyBufList.accBwd;

        while( tail != (struct bsBuf *)&bfap->dirtyBufList &&
            tail->writeRef != 0 ) 
        {
            tail = tail->accBwd;
        }
        
        if ( tail != (struct bsBuf *)&bfap->dirtyBufList ) 
        {

            logPageLsn = tail->flushSeq;
            if ( LSN_LTE( lsn, logPageLsn ) ) {
                mutex_unlock( &bfap->bfIoLock );
                /*
                 * We can tolerate pinned pages in this range. They will get
                 * flushed at unpin time. What we are really trying to determine
                 * is the lsn. For log pages the flush lsn is only meaningful at
                 * unpin time. While the log page is pinned the lsn can not be
                 * compared against.
                 * So basically what we are trying to do here is to see if the
                 * lsn we are trying to flush does not live on the last page.
                 * if it doesn't we can go ahead and flush right now. Otherwise
                 * we need to call into the logging routine which can identify
                 * the last pages lsn.
                 */
                
                
                bs_logflush_start(bfap, logPageLsn);
                
                /*
                 * Although we flush all unpinned log buffers,
                 * the caller should only have to wait on the
                 * lsn he wants to be written.
                 */
                if ( wait == TRUE ) {
                    sts = bfflush_sync(bfap, lsn);
                }
                mutex_lock( &bfap->bfIoLock );
                return;
            }
        }
    }

    /* The range containing this log page might encompass
     * the last log page. We need to call lgr_flush to determine
     * if the last page is in the range and if so force it to
     * be unpinned.
     */

    mutex_unlock( &bfap->bfIoLock );

    (void)lgr_flush_start( dmnP->ftxLogP, FALSE, lsn );
    if ( wait == TRUE ) {
        (void)lgr_flush_sync( dmnP->ftxLogP, lsn );
    }
    mutex_lock( &bfap->bfIoLock );
}

/*
 * Return TRUE if the given page is on the log's
 * dirty list.  Otherwise, return FALSE.
 *
 * SMP: 1. Assume caller has log descriptor lock held.
 *      2. Seize logbfap->bfIoLock while walking the dirtyBufList, but
 *         I'm not sure that it is strictly necessary with descriptor lock.
 */

int
bs_logpage_dirty( 
    bfAccessT *bfap,        /* in */
    u_long pageNum          /* in */
    )
{
    struct bsBuf *tail;

    mutex_lock( &bfap->bfIoLock );

    /*
     * Determine if the desired logpage 
     * has been unpinned but not yet written.
     */
    if ( bfap->dirtyBufList.length ) {

        /*
         * Backup, looking for the desired page number.
         */

        tail = bfap->dirtyBufList.accBwd;
        
        while( tail != (struct bsBuf *)&bfap->dirtyBufList ) {

            if ( tail->bfPgNum == pageNum ) {
                mutex_unlock( &bfap->bfIoLock );
                return( TRUE );
            }
            tail = tail->accBwd;
        }
    }
    mutex_unlock( &bfap->bfIoLock );
    return( FALSE );
}
    

/*
 * bs_logflush_start
 *
 * Given the access handle for the log, flush all buffers up to the 
 * page containing the given lsn number.
 * 
 * SMP: 1. Enter with no locks held
 *      2. Seizes and releases the bfap->bfIoLock for the log
 *         while walking its dirtyBufList.
 *
 * It is assumed that any caller of this routine has dealt
 * with the last log page if it is within the range being flushed.
 * currently lgr_flush or lgr_flush_start must be called if the
 * the last log page is suspected to be within the flushing range.
 */

statusT
bs_logflush_start( 
    bfAccessT *bfap,            /* in */
    lsnT lsn                    /* in */
    )
{

    struct bsBuf *tail;

    mutex_lock( &bfap->bfIoLock );

    /*
     * Check for empty list.
     */
    if ( bfap->dirtyBufList.length == 0 ) {
        goto exit;
    }

    if ( LSN_GT( lsn, bfap->logWriteTargetLsn ) ) {
        bfap->logWriteTargetLsn = lsn;
    }

    logflush_cont( bfap );

exit:
    mutex_unlock( &bfap->bfIoLock );
    return(EOK);
}

/*
 * logflush_cont
 *
 * Called by either bs_logflush_start to begin the logflush
 * or by check_cont_bits to continue the logflush.  Will try to
 * flush log buffers up to bfap->logWriteTargetLsn.
 *
 * SMP: 1. Caller must hold the bfap->bfIoLock for the log;
 *         it will be dropped and reacquired in this routine.
 *      2. bp->bufLock will be acquired in an out-of-hierarchy
 *         sequence when checking and possibly changing the state
 *         of each buffer.
 */

void
logflush_cont( struct bfAccess *bfap )     /* in - bfap for log */
{
    struct bsBuf *head, *tail, *bp;
    int bufCnt;
    lsnT lsn = bfap->logWriteTargetLsn;
    ioDescT *ioDescp, *ioListp;
    int listLen = 0;
    int first, last;
    int i;
    int couldnt_hold, sts;


    ioListp = NULL;
    bufCnt = 0;

restart:
    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfIoLock.mutex));

    head = bfap->dirtyBufList.accFwd;
    tail = bfap->dirtyBufList.accBwd;

    while( tail != head && 
           LSN_LTE( lsn, tail->accBwd->flushSeq ) ) {

        tail = tail->accBwd;
    }

    /*
     * Scan the list of buffers.  If a buffer is pinned, skip it.
     * If a buffer is BUSY (this means it is already being written 
     * ie. it's on the blocking queue), skip it.  
     *
     * Put the related ioDesc on a linked list, ready for I/O.
     */ 

    for ( bp = head; bp != tail->accFwd;
            bp = bp->accFwd ) {

        /* Peek at writeRef and lock.state before we lock the buffer;
         * should be OK since we are  not modifying these values and we
         * just want to skip the buffers with write Refs or that are BUSY.
         * BUSY may be the result of this routine setting it below, and
         * having to restart the scan.
         * Any pages that have writerefs (pinned) will be flushed
         * at unpin time.
         */
        if ( bp->writeRef || (bp->lock.state & BUSY) ) {

            continue;
        }

        couldnt_hold = advfs_page_get(bp, UBC_GET_WRITEHOLD);
        if (couldnt_hold) {
            /* The ubc may be in the process of exchanging the page
             * for numa locality or bigpage consolidation.  We need to
             * wait for it to finish, but we have to drop our locks and
             * there are no guarantees the page will be valid on wake up.
             */
            if (couldnt_hold == UBC_GET_WAIT_EXCHANGE) {
                ubc_fs_page_wait(bp->vmpage, &bfap->bfIoLock.mutex);
                mutex_lock(&bfap->bfIoLock);
                if (ioListp) {
                    /* Point to the last buffer on our accumulated list and
                     * continue on to the next buffer on the dirtyBufList.
                     * This avoids having to retraverse the list past them.
                     */
                    bp = ioListp->bwd->bsBuf;
                    continue;
                } else {
                    goto restart;
                }
            }

            /* assume busy from another I/O thread and skip it */
            continue;
        }

        /* Try to lock in out-of-hierarchy order since this is simplest.
         * If it doesn't work, we must relock in order and check that
         * the buffer state is still what we expect (on dirty list, not
         * BUSY).
         */
        if (!mutex_lock_try( &bp->bufLock.mutex )) {
            mutex_unlock( &bfap->bfIoLock );
            mutex_lock( &bp->bufLock );
            mutex_lock( &bfap->bfIoLock );
            if (!(bp->lock.state & ACC_DIRTY) ||
                 (bp->lock.state & BUSY) || 
                 (bp->writeRef) ||
                 (bp->bfAccess != bfap) ) {
                 mutex_unlock( &bp->bufLock );
                 /* Drop UBC page (pg_hold count) reference. */
                 ubc_page_release(bp->vmpage, B_WRITE);
 
                 /* we have lost our way down the chain; restart scan
                  * at head of list, but we will skip any buffers we
                  * already put onto our list since they are marked BUSY.
                  */
                 goto restart;
            }
        }

        /* We need to recheck that an unpin did not already start an io
         * on this buffer since were not holding the buf lock above.
         */

        if (!(bp->lock.state & ACC_DIRTY) ||
            (bp->lock.state & BUSY) )
        {
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            continue;
        }

        /* Setup UBC page for IO. Failure should be impossible so
         * just prevent further damage and crash it in development.
         */
        if ((sts = advfs_page_get(bp, UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
            /* must drop and retake bp->bufLock for live dump */
            mutex_unlock(&bp->bufLock);
            mutex_unlock(&bfap->bfIoLock);
            ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
            domain_panic(bfap->dmnP,
                         "logflush_cont: advfs_page_get(%p) returned %x",
                         bp, sts);
            mutex_lock(&bp->bufLock);

            /* Force advfs io completion to clean up our chains so
             * we don't just have threads hanging.  The ubc page
             * remains dirty and since it is metadata probably stays
             * in cache until the domain is unmounted.
             *
             * Note we have not set BUSY so the page is not released
             * in bs_io_complete().  We release our hold and restart
             * since we pulled the bsBuf from the dirtyBufList.
             */
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
            bs_io_complete(bp, &sts);
            ubc_page_release(bp->vmpage, B_WRITE);
            mutex_lock(&bfap->bfIoLock);
            goto restart;
        }
        bp->lock.state |= BUSY;
        bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
        bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
        bp->ioqLn = -1;
#endif

        first = bp->ioList.write;
        last = bp->ioList.write + bp->ioList.writeCnt;

        ioDescp = bp->ioList.ioDesc;

        for( i = first; i < last; i++ ) {

            /*
             * Link ioDesc belonging to the same buffer together.
             */
            if ( i == first ) {
                ioDescp[last - 1].fwd = NULL;
            } else {
                ioDescp[i - 1].fwd = &ioDescp[i];
            }

            if ( i == last - 1 ) {
                ioDescp[first].bwd = NULL;
            } else {
                ioDescp[i + 1].bwd = &ioDescp[i];
            }
        }

        /*
         * Now link all the buffers I/O's
         */
        if ( ioListp == NULL ) {
            ioDescp[first].bwd = &ioDescp[last - 1];
            ioDescp[last - 1].fwd = &ioDescp[first];
            ioListp = &ioDescp[first];
        } else {
            ioListp->bwd->fwd = &ioDescp[first];
            ioDescp[first].bwd = ioListp->bwd;
            ioDescp[last - 1].fwd = ioListp;
            ioListp->bwd = &ioDescp[last - 1];
        }

        bp->ioCount = bp->ioList.writeCnt;
        listLen += bp->ioList.writeCnt;
        bufCnt++;

        mutex_unlock( &bp->bufLock );
        /* Drop UBC page (pg_hold count) reference. */
        ubc_page_release(bp->vmpage, B_WRITE);
    }

    if ( listLen ) {
        /* Enter the buffers onto the blocking queue.  */
        mutex_unlock( &bfap->bfIoLock );
        bs_q_list( ioListp, listLen, bs_q_blocking );
        mutex_lock( &bfap->bfIoLock );
    }

    return;
}


#define MIGRATE 1
#define NOTMIGRATE 0
/*
 * bfflush_start
 *
 * Flush all outstanding buffers for a bit file.
 *
 * Returns seq = nilLSN if no dirty buffers currently queued for i/o.
 *
 *    Normally, set flushPgCnt = 0 to flush all buffers.
 *    Special case: for fs_write()'s sequential write aggressive flushing
 *    support use only, set flushPgCnt to flush only that number of pages
 *    from the beginning of the dirtyBufList.  In this case, the buffers
 *    will be placed on the ubcreq queue instead of the flush queue
 *    and the written pages will go to the head of the ubc clean list.
 *
 * SMP: 1. Entered with no locks held.  
 *      2. Seizes bfap->bfIoLock while walking the dirtyBufList. 
 *      3. Also seizes bp->bufLock in out-of-hierarchy order for each
 *         dirty buffer associated with the file that must be flushed.
 */

void
bfflush_start(
              bfAccessT* bfap, /* in - ptr to access struct */
              lsnT *seq,       /* in */
              int migrate,      /* in */
              unsigned long flushPgCnt /*in: set to 0 to flush all pages or */
                                       /* Special case:  # pages to flush */
    )
{
    struct bsBuf *head, *bp;
    int listLen;
    int listLenPart;
    int end_has_been_determined = 0;
    lsnT finalLsnWeNeedToFlush;
    lsnT hiCurrentLsn;
    ioDescT *ioList, *ioListPart, *save;
    bfAccessT *logBfap;
    lsnT highestSkippedLsn = {0};
    int noqfnd;
    lsnT saveFlushSeq = nilLSN;
    int useFlushCnt;

    *seq = nilLSN;

    mutex_lock( &bfap->bfIoLock );

    /*
     * Init the disk error result of this flush.
     */
    if ( migrate == MIGRATE ) {
        bfap->miDkResult = EOK;
    } else {
        bfap->dkResult = EOK;
    }

    /*
     * Scan the list of buffers creating a list of ioDesc
     * that did not yet make it to the blockingQ (those on the 
     * lazy queues). Queue that list to the blockingQ. 
     *
     * If we must block, either on IO_TRANS or log flush
     * restart the backwards scan.
     */ 

loop:
    /*
     * Check for empty list.
     */
    if ( bfap->dirtyBufList.accFwd == (struct bsBuf *)&bfap->dirtyBufList ) {
        /* Either the accFwd test or the length test would suffice. */
        MS_SMP_ASSERT(bfap->dirtyBufList.length == 0);
        if ( end_has_been_determined ) {
            *seq = finalLsnWeNeedToFlush;
        }
        mutex_unlock( &bfap->bfIoLock );
        return;
    }

    /*
     * If this is a logged file, do a scan of the list we're
     * going to flush to assure that the log has been written.
     */
    if ((bfap->dataSafety == BFD_FTX_AGENT) ||
        (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY)) {
                           
        logBfap = bfap->dmnP->logAccessp;

        /* Scan the list for the greatest LSN value in the log
         * that must be flushed before we can flush the data.
         * Note that we do not lock the buffer to read currentLogRec.lsn
         * to avoid the lock overhead.  If a buffer is updated after we
         * extract this value and call call_logflush(), that will be 
         * caught in the loop below.
         */
        hiCurrentLsn = nilLSN;
        for( bp = bfap->dirtyBufList.accFwd;
            bp != (struct bsBuf *)&bfap->dirtyBufList;
            bp = bp->accFwd ) {
                
            MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);
            if ( LSN_GT( bp->currentLogRec.lsn, hiCurrentLsn ) ) {
                hiCurrentLsn = bp->currentLogRec.lsn;
            }
        }

        if ( LSN_GT( hiCurrentLsn, logBfap->hiFlushLsn ) ) {

            /*
             * We block on the flush.
             * Note that log's bfap->bfIoLock may be surrendered in
             * call_logflush, so we still have to check the current
             * lsn when we attempt to write below.  Also, release the
             * current file's bfIoLock before calling this routine.
             */
            mutex_unlock( &bfap->bfIoLock );
            mutex_lock( &logBfap->bfIoLock );

            call_logflush( bfap->dmnP, hiCurrentLsn, TRUE );

            mutex_unlock( &logBfap->bfIoLock );
            mutex_lock( &bfap->bfIoLock );

            /* since we released the file's bfIoLock, must restart the scan */
            goto loop;
        }
    }

    ioList = NULL;
    listLen = 0;
    head = bfap->dirtyBufList.accFwd;

    /* Only do this the first time thru; this makes the final flushSeq value
     * on entry to this routine the terminator of this loop, not the
     * physical makeup of the dirty list, which can change as we go along.
     * Remember if a caller wants a specified number of pages flushed.
     * In that case, only a specific number of buffers are flushed and
     * and the flushSeq of the last buffer will get passed back to caller.
     */
    if ( !end_has_been_determined ) {
        finalLsnWeNeedToFlush = bfap->dirtyBufList.accBwd->flushSeq;
        useFlushCnt = (flushPgCnt) ? TRUE : FALSE;
        end_has_been_determined = 1;
    }

    /* Flush all pages on the dirtyBufList upto the predetermined
     * end or flush only a specific number of pages the caller wants.
     */
    for( bp = head;
         bp != (struct bsBuf *)&bfap->dirtyBufList &&
         LSN_GTE( finalLsnWeNeedToFlush, bp->flushSeq ) &&
         (!useFlushCnt || (flushPgCnt > 0));
         bp = bp->accFwd) {

         u_long savListSeq = bp->accListSeq;

        /* this is only an advisory flush so it is ok to just skip pages */
        if (advfs_page_get(bp, UBC_GET_WRITEHOLD))
            goto end_of_loop;

        /* Remember buffer's flushSeq for passing back to caller.  */
        saveFlushSeq = bp->flushSeq;
        if (useFlushCnt)
            flushPgCnt--;

        /* Try to lock each buffer out of hierarchy order. If this
         * fails, relock in correct order, but be sure that
         * buffer is still on dirty list after the locks are held.
         */
        if (!mutex_lock_try( &bp->bufLock.mutex )) {
            mutex_unlock( &bfap->bfIoLock );
            mutex_lock( &bp->bufLock );
            mutex_lock( &bfap->bfIoLock );
            if (bp->accListSeq != savListSeq )  /* bp removed from acc list */
            {
                /* buffer was removed from dirty list while we dropped the
                 * chain lock.  Skip this buffer, but must go back and 
                 * restart the scan.
                 */
                mutex_unlock( &bp->bufLock );
                /* Drop UBC page (pg_hold count) reference. */
                ubc_page_release(bp->vmpage, B_WRITE);

                if (ioList) {
                    /* Point to the last buffer on our accumulated list and 
                     * continue on to the next buffer on the dirtyBufList.  
                     * This avoids having to retraverse the list past these 
                     * buffers, as well as waiting for their IO_TRANS state 
                     * to clear.
                     */
                    /* Since these iop are not on any Q the IO for that bp */
                    /* has not completed. Therefore it is still on the bfap */
                    /* dirtyBufList. */
                    bp = ioList->bwd->bsBuf;
                    MS_SMP_ASSERT(bp->bfAccess == bfap);
                    MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);
                    goto end_of_loop;
                } else {
                    goto loop;
                }
            } 
        }

        MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);
        MS_SMP_ASSERT(bp->bfAccess == bfap);
        if ( bp->lock.state & FLUSH ) {

            /*
             * We will sleep waiting on the FLUSH lock
             * so write out the buffers and begin again.
             */
            mutex_unlock( &bfap->bfIoLock );
            if ( listLen ) {
                mutex_unlock( &bp->bufLock );
                bs_q_list( ioList, listLen, 
                           useFlushCnt ? bs_q_ubcreq : bs_q_flushq );
                mutex_lock( &bp->bufLock );
                if (bp->accListSeq != savListSeq )  /* bp removed from list */
                {
                    /* We had to release both locks so make
                     * sure I/O didn't complete and bsBuf
                     * didn't get reused before waiting.
                     */
                    mutex_unlock( &bp->bufLock );
                    mutex_lock( &bfap->bfIoLock );
                    /* Drop UBC page (pg_hold count) reference. */
                    ubc_page_release(bp->vmpage, B_WRITE);
                    goto loop;
                }
            }

            wait_state ( bp, FLUSH );
            mutex_unlock( &bp->bufLock );
            mutex_lock( &bfap->bfIoLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;
        } else {
            bp->lock.state |= FLUSH;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
        }

        /*
         * Keep track of the highest LSN we are waiting for in 
         * order to complete the flush.  bs_unpinpg() will use 
         * this information to decide whether to place the buffer 
         * on the lazy or blocking queue.
         */
        if (SEQ_GT( bp->flushSeq, bfap->hiWaitLsn)) {
            bfap->hiWaitLsn = bp->flushSeq;
        }

        if ( bp->writeRef != 0 ) {
            /*
             * Another thread is modifying this page.  We will
             * skip the bsBuf (not put it onto our ioList) and
             * trust that the other thread will put the buffer
             * onto the blocking queue when it does an unpin
             * of the page.  It should do so because we've
             * already bumped bfap->hiWaitLsn to be at least as
             * high as this bsBuf's flushSeq number.
             */
            if (LSN_GT(bp->flushSeq, highestSkippedLsn)) {
                highestSkippedLsn = bp->flushSeq;
            }
            clear_state( bp, FLUSH );
            mutex_unlock( &bp->bufLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);

            goto end_of_loop;
        }

        /*
         * If bitfile is logged, assure that log
         * has been flushed up to this write.
         */
        if ((bfap->dataSafety == BFD_FTX_AGENT) ||
            (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY)) {

            if ( LSN_GT( bp->currentLogRec.lsn,
                        logBfap->hiFlushLsn ) ) {

                /*
                 * We thought we would do the flush
                 * above, but someone must have been
                 * added to the list.
                 *
                 * Write what we already have since
                 * we will block on the flush.
                 */
                mutex_unlock( &bfap->bfIoLock );
                clear_state( bp, FLUSH );
                mutex_unlock( &bp->bufLock );
                if ( listLen ) {
                    bs_q_list( ioList, listLen,
                               useFlushCnt ? bs_q_ubcreq : bs_q_flushq );
                }
                mutex_lock( &bfap->bfIoLock );
                /* Drop UBC page (pg_hold count) reference. */
                ubc_page_release(bp->vmpage, B_WRITE);
                goto loop;
            }
        }

        if ( bp->lock.state & IO_TRANS ) {
            /*
             * We will sleep waiting on the IO_TRANS lock
             * so write out the buffers and begin again.
             */
            mutex_unlock( &bfap->bfIoLock );
            clear_state( bp, FLUSH );
            if ( listLen ) {
                mutex_unlock( &bp->bufLock );
                bs_q_list( ioList, listLen,
                           useFlushCnt ? bs_q_ubcreq : bs_q_flushq );
                mutex_lock( &bp->bufLock );
                if (bp->accListSeq != savListSeq )  /* bp removed from list */
                {
                    /* We had to release both locks so make
                     * sure I/O didn't complete and bsBuf
                     * didn't get reused before waiting.
                     */
                    mutex_unlock( &bp->bufLock );
                    mutex_lock( &bfap->bfIoLock );
                    /* Drop UBC page (pg_hold count) reference. */
                    ubc_page_release(bp->vmpage, B_WRITE);
                    goto loop;
                }
            }

            wait_state ( bp, IO_TRANS );
            mutex_unlock( &bp->bufLock );
            mutex_lock( &bfap->bfIoLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;
        } else {
            bp->lock.state |= IO_TRANS;
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
        }

        /* Must release these locks for rm_from_lazyq() since it will
         * lock vdp->vdIoLock which is higher in the hierarchy.  Note 
         * that there is no guarantee that this ioDesc is still on 
         * the lazy queues; it may have completed IO by time rm_from_lazyq()
         * returns.  If this is so, then we must determine the identity
         * of the buffer pointed to by 'bp'; it may no longer be a buffer
         * belonging to this file or on this file's dirty list.
         *
         * Because the buffer may no longer belong to this file on return,
         * it is not safe to clear the FLUSH state on return.  Therefore,
         * now that IO_TRANS is set clear the FLUSH state, but don't
         * awaken any threads waiting for FLUSH.  They will be awakened
         * when I/O completes or on return when IO_TRANS is cleared.
         * The other user of FLUSH is the migrate path which sets FLUSH
         * in set_block_map() prior to pinning the page.  It will now
         * be able to set FLUSH once we clear it and release the locks,
         * and proceed to bs_pinpg where it will wait on IO_TRANS for
         * this buffer.
         * Note that the reason we have this problem with FLUSH and not
         * IO_TRANS is that IO_TRANS is cleared by bs_io_complete().  If
         * the buffer is not representing the same dirty page on return
         * then its i/o has completed and bs_io_complete() already
         * cleared the IO_TRANS state.
         */
        bp->lock.state &= ~FLUSH;
        mutex_unlock( &bfap->bfIoLock );
        rm_from_lazyq( bp, &ioListPart, &listLenPart, &noqfnd);

        /* 
         * Enque buffer that was taken off lazy queue or that originates
         * from a getpage write and was not on a queue.
         */
        if (listLenPart || (noqfnd && (bp->lock.state & GETPAGE_WRITE))) {
            int sts;
            MS_SMP_ASSERT(bp->bfAccess == bfap);
            MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);

            /* Setup UBC page for IO.  We can fail here because an
             * mmapped page is wired by an application so we can not
             * change protection to make the page clean.  In that case
             * just write the page while keeping it dirty.  Any other
             * failure should be impossible so just prevent further
             * damage and crash it in development.
             */ 
            if ((sts = advfs_page_get(bp, UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
                if (sts == UBC_GET_VM_PROTECT && bp->bfAccess->bfVp &&
                        (bp->bfAccess->bfVp->v_flag & VMMAPPED)) {
                    sts = advfs_page_get(bp,
                              UBC_GET_BUSY | UBC_GET_HAVEHOLD |
                              UBC_GET_KEEPDIRTY);
                    if (!sts) {
                        bp->lock.state |= (BUSY | KEEPDIRTY);
                        bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                        bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                        bp->ioqLn = -1;
#endif
                    }
                }

                if (sts) {
                    /* must drop and retake bp->bufLock for live dump */
                    mutex_unlock(&bp->bufLock);
                    ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                    domain_panic(bfap->dmnP,
                             "bfflush_start: advfs_page_get(%p) returned %x",
                             bp, sts);
                    mutex_lock(&bp->bufLock);

                    /* Force advfs io completion to clean up our chains so
                     * we don't just have threads hanging.  The ubc page
                     * remains dirty until lru reuse forces it through the
                     * putpage path, where it will be tossed by bs_pinpg_put.
                     *
                     * Note we have not set BUSY so the page is not released
                     * in bs_io_complete().  We release our hold and restart
                     * since we pulled the bsBuf from the dirtyBufList.
                     */
                    bp->ln = SET_LINE_AND_THREAD(__LINE__);
                    bs_io_complete(bp, &sts);
                    ubc_page_release(bp->vmpage, B_WRITE);
                    if (listLen)
                         bs_q_list(ioList, listLen,
                                   useFlushCnt ? bs_q_ubcreq : bs_q_flushq);
                    mutex_lock(&bfap->bfIoLock);
                    goto loop;
                }
            } else {
                bp->lock.state |= BUSY;
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                bp->ioqLn = -1;
#endif
            }

            /* Setup the IO list for GETPAGE_WRITE buffers. */
            if (!listLenPart) {
                link_write_req(bp);
                listLenPart = bp->ioCount;
                ioListPart = bp->ioList.ioDesc;
            }

            listLen += listLenPart;

            if ( ioList == NULL ) {
                ioList = ioListPart;
            } else {
                /*
                 * Add ioDesc to the end of the list.
                 */
                ioList->bwd->fwd = ioListPart;
                ioListPart->bwd->fwd = ioList;
                save = ioList->bwd;
                ioList->bwd = ioListPart->bwd;
                ioListPart->bwd = save;
            }

            if (useFlushCnt)
                bp->ubc_flags = B_AGE;

        } else if (bp->accListSeq != savListSeq ) /* bp removed from list */
        {
            /* we didn't get the buffer off the lazy queues, so check it's
             * identity before continuing.  If changed, put any accumulated
             * ioDesc structs onto the flushq queue and restart the
             * traversal.
             */
            mutex_unlock( &bp->bufLock );
            if ( listLen ) {
                /* Put any buffers found onto the flushq queue before
                 * going back to the top and reinitializing the ioList.
                 */
                bs_q_list( ioList, listLen,
                           useFlushCnt ? bs_q_ubcreq : bs_q_flushq );
            }
            mutex_lock( &bfap->bfIoLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;
        } else {
            clear_state( bp, IO_TRANS );
        }

        mutex_lock( &bfap->bfIoLock );
        mutex_unlock( &bp->bufLock );

        /* Drop UBC page (pg_hold count) reference. */
        ubc_page_release(bp->vmpage, B_WRITE);

end_of_loop:

        /*
         * See if we're about to exit from this loop even though 
         * there are more bsBufs on the dirty list.  If so, do a 
         * sanity check on the value of finalLsnWeNeedToFlush.
         * The bsBuf from which this value was pulled may have had
         * its flushSeq changed while we were traversing the dirty
         * list.  If so, the value of finalLsnWeNeedToFlush does not
         * match the flushSeq number of any of the bsBufs on our ioList.
         * Nor does it match the value of the highest LSN we skipped.
         * If the bsBuf from which we pulled finalLsnWeNeedToFlush
         * had its flushSeq number incremented due to the addition of
         * one or more new bsBufs at the end of the list which have now
         * completed their I/Os and passed on their flushSeq numbers
         * to the bsBuf from which we grabbed finalLsnWeNeedToFlush, 
         * our ioList will not contain the bsBuf from which we pulled
         * finalLsnWeNeedToFlush.  So do a sanity check here.  If we
         * find that the highest LSN flushed so far is less than 
         * finalLsnWeNeedToFlush and there is no bsBuf on the ioList 
         * with a flushSeq value equal to finalLsnWeNeedToFlush, and 
         * finalLsnWeNeedToFlush isn't the same as the highest LSN we 
         * skipped, bump finalLsnWeNeedToFlush to the flushSeq number 
         * of the next bsBuf on the dirty list and do the loop again.  
         * This may cause an unnecessary I/O if another thread has already
         * completed the I/O for finalLsnWeNeedToFlush for us but buffers
         * with smaller LSN's still have not been flushed.
         */
        if ((bp->accFwd != (struct bsBuf *)&bfap->dirtyBufList) &&
            (LSN_GT(bp->accFwd->flushSeq, finalLsnWeNeedToFlush))) {

            if ((LSN_LT(bfap->hiFlushLsn, finalLsnWeNeedToFlush)) &&
                (!LSN_EQL(highestSkippedLsn, finalLsnWeNeedToFlush)) &&
                ((ioList == NULL) ||
                 (!LSN_EQL(ioList->bwd->bsBuf->flushSeq,finalLsnWeNeedToFlush)))){

                /*
                 * The finalLsnWeNeedToFlush doesn't match either the last
                 * bsBuf on our ioList or the highest one we skipped.
                 */
                finalLsnWeNeedToFlush = bp->accFwd->flushSeq;
            }
        }

    }  /* for loop */

    /*
     * It would be nice to be able to put an assertion here that
     * the finalLsnWeNeedToFlush value is sane, that all the I/O's
     * that the bfflush_sync() code will wait for have, indeed,
     * been scheduled.  But we cannot write such an assertion due
     * to the fact that some other thread my have scheduled the
     * I/O's for us.  These I/O's may be on the device or blocking
     * queue or they may have completed.  Let's hope we scheduled
     * all the necessary I/O's correctly...
     */

    /* 
     * Return our logical flush point as the LSN upon which bfflush_sync()
     * will wait.  Note that bs_io_complete() and racing flushers can 
     * bump bfap->hiWaitLsn past the point that we will need to wait in 
     * bfflush_sync(), so do not pass back bfap->hiWaitLsn here.
     */
    *seq = (useFlushCnt) ? saveFlushSeq : finalLsnWeNeedToFlush;

    mutex_unlock( &bfap->bfIoLock );

    if ( listLen ) {
        /* Enter the buffers, if any found, onto the flush queue. */
        bs_q_list( ioList, listLen,
                   useFlushCnt ? bs_q_ubcreq : bs_q_flushq );
    }

    return;
}

/*
 * bfflush_sync
 *
 * Block until the given lsn has been written, return any write error
 * status. 
 *
 * SMP: No locks held on entry; seizes bfap->bfIoLock internally to
 *      guard hiFlushLsn and flushWait.
 */

statusT
bfflush_sync(
                bfAccessT* bfap,        /* in - bf access ptr */
                lsnT waitLsn            /* in - seq to wait for */
             )
{
    statusT sts;
    flushWaiterT * flushWaiter, * curFlushWaiter;
    int deallocWaiter;
    processor_t myprocessor;        /* used by mark_bio_wait */

    sts = EOK;

    /* The flush sync can return if the target LSN is already flushed.
     * Explicit check without the bfap's bfIoLock held for performance. 
     */
     
    if (SEQ_GTE(bfap->hiFlushLsn, waitLsn)) {
        return( bfap->dkResult);
    }
    
    deallocWaiter = FALSE;

    flushWaiter = (flushWaiterT *)ms_malloc(sizeof(flushWaiterT));

    flushWaiter->waitLsn = waitLsn;
    flushWaiter->cnt = 1;

    mutex_lock( &bfap->bfIoLock );
    
    /* Recheck if the target Lsn is already flushed. */

    if (SEQ_LT(bfap->hiFlushLsn, waitLsn)) {

        /*
         * Make sure there's something to wait for.  This check
         * must be done with the bfIoLock held.
         */
        if (bfap->dirtyBufList.length == 0) {
            MS_SMP_ASSERT(0);  /* Get a crash dump in-house... */
            mutex_unlock(&bfap->bfIoLock);
            domain_panic(bfap->dmnP, "bfflush_sync: No dirty buffers");
            ms_free(flushWaiter);
            return E_DOMAIN_PANIC;
        }

        /* Search flushWaiterQ queue backwards in descending Lsn order 
         * for location to insert new entry.
         */ 
        curFlushWaiter = bfap->flushWaiterQ.tail;

        while( curFlushWaiter != (flushWaiterT *) &bfap->flushWaiterQ) {
            /* New waitLsn matches an existing flushWaiter. */
            if (LSN_EQL(curFlushWaiter->waitLsn, waitLsn)) {
                curFlushWaiter->cnt++;
                deallocWaiter = TRUE;
                break;
            } else {
                /* Current flushWaiterQ's LSN is lower than new waitLsn. */
                if (SEQ_LT(curFlushWaiter->waitLsn, waitLsn)) {
                    flushWaiter->bwd = curFlushWaiter;
                    flushWaiter->fwd = curFlushWaiter->fwd;
                    if ( curFlushWaiter->fwd ==
                         (flushWaiterT *)&bfap->flushWaiterQ )
                    {
                        bfap->flushWaiterQ.tail = flushWaiter;
                    } else {
                        curFlushWaiter->fwd->bwd = flushWaiter;
                    }
                    curFlushWaiter->fwd = flushWaiter;
                    curFlushWaiter = flushWaiter;
                    break;
                }
            }
            curFlushWaiter = curFlushWaiter->bwd;
        } 

        /* Insert new flush waiter at the queue head. */
        if (curFlushWaiter == (flushWaiterT *) &bfap->flushWaiterQ) {
            /* The queue is empty. */
            if ( bfap->flushWaiterQ.tail ==
                 (flushWaiterT *) &bfap->flushWaiterQ )
            {
                flushWaiter->fwd = (flushWaiterT *)&bfap->flushWaiterQ;
                flushWaiter->bwd = (flushWaiterT *)&bfap->flushWaiterQ;
                bfap->flushWaiterQ.head = bfap->flushWaiterQ.tail = flushWaiter;
            } else {
                /* The queue has existing entries. Insert new waiter
                 * at the queue head since the new flush waiter's Lsn
                 * is less than all existing waiters.
                 */
                flushWaiter->fwd = bfap->flushWaiterQ.head;
                flushWaiter->bwd = (flushWaiterT *)&bfap->flushWaiterQ;
                bfap->flushWaiterQ.head->bwd = flushWaiter;
                bfap->flushWaiterQ.head = flushWaiter;
            }
            curFlushWaiter = flushWaiter;
        }

        /* Update statistics counters. */
        if (AdvfsLockStats)
            AdvfsLockStats->bfFlushWait++;
        bfap->flushWait++;
        if (bfap->flushWait > bfap->maxFlushWaiters)
            bfap->maxFlushWaiters++;
        if (!deallocWaiter)
            bfap->flushWaiterQ.cnt++;

        /* Block til bs_io_complete determines this IO flush sync is finished.*/
        mark_bio_wait;
        sts = mpsleep((caddr_t)curFlushWaiter,
                      PZERO /*not interruptible*/,
                      "flushWaiter",
                      FALSE /*no timeout*/,
                      simple_lock_addr(bfap->bfIoLock),
                      MS_LOCK_SIMPLE|MS_LOCK_ON_ERROR);
        unmark_bio_wait;

        bfap->flushWait--;
    } else {
        /* The target Lsn is already flushed, so this flush sync is done.  */
        deallocWaiter = TRUE;
    }

    mutex_unlock( &bfap->bfIoLock );

    if (deallocWaiter) {
        ms_free(flushWaiter);
    }

    return( bfap->dkResult );
}

/*
 * bs_bf_flush_nowait
 *
 * Flushes a file and allows the caller to synchronize with the flush
 * but does not wait for pinned buffers to be written.
 *
 * SMP: 1. entered with no locks held.  
 *      2. Acquires bfap->bfIoLock for the file being flushed.
 *      3. Acquires bfap->bfObj lock to wait for I/O completion.
 */

statusT
bs_bf_flush_nowait(
                   bfAccessT* bfap    /* in */
                   )
{
    lsnT waitLsn;
    struct bsBuf *bp, *sbp;
    processor_t myprocessor;        /* used by mark_bio_wait */

    bfflush_start( bfap, &waitLsn, MIGRATE, 0 );

    do {
        mutex_lock(&bfap->bfIoLock);

        /* make this quick check as an optimization to see if
         * the buffer and all those before it have been flushed
         */
        if (SEQ_GTE(bfap->hiFlushLsn, waitLsn)) {
            break;
        }

        /* Search the dirtyBufList to find the buffer with the
         * highest sequence number equal or less than our sequence
         * number, ignoring pinned buffers.
         */

        bp = NULL;

        for (sbp = bfap->dirtyBufList.accFwd;
             sbp != (struct bsBuf *)&bfap->dirtyBufList;
             sbp = sbp->accFwd) {
            
            if (SEQ_GT(sbp->flushSeq, waitLsn))
                break;

            if (sbp->lock.state & BUSY)
                bp = sbp;
        }

        if (bp) {
            /* Wait for I/O to complete by checking vm_page pg_busy
             * which is cleared in bs_io_complete().  Note that on
             * wakeup the vm_page and bsBuf may be gone so we don't
             * reference again and we start over to see if all the
             * buffers are now gone.  The bfIoLock is unlocked in
             * ubc_fs_page_wait() after the pg/obj lock is taken
             * and no locks are held on return.
             */
            mark_bio_wait;
            ubc_fs_page_wait(bp->vmpage, &bfap->bfIoLock.mutex);
            unmark_bio_wait;
        } /* else bp==NULL, while ends with bfIoLock still held */

    } while(bp);

    /* loop always ends with bfIoLock still held */
    mutex_unlock(&bfap->bfIoLock);

    /* No unpinned buffers with lesser sequence numbers are on the list */
    return(bfap->miDkResult);
}

    
/*
 * bs_bflush
 *
 * Flush unpinned dirty buffers to disk.
 *
 * This routine does not block the caller.
 * To block until all buffers are flushed, call bs_bflush_sync.
 *
 */

void
bs_bflush( 
    struct vd *vdp      /* in */
    )
{
    /*
     * Move ioDesc with current lsn <= hiFlushLsn
     * from the waitLazyQ to the readyLazyQ.
     */
    wait_to_readyq( vdp );

    /*
     * Move I/O's from smoothsyncQ to the readyLayzQ
     */
    smsync_to_readyq( vdp, IO_FLUSH );

    /*
     * Consolidate I/O's moving them from the readyLazyQ
     * to the consolidate queue.
     */
    (void)ready_to_consolq( vdp );

    startiocalls[6]++;
    bs_startio( vdp, IO_FLUSH );
}


/*
 * bs_bflush_sync
 *
 * Call after calling bs_bflush to wait for
 * the device to become quiescent.
 * Return the length of the device's lazy queue.
 *
 */

int 
bs_bflush_sync( 
    struct vd *vdp      /* in */
    )
{
    int qlen;
    int i;
    struct processor *myprocessor;        /* used by mark_bio_wait */

    /*
     * to determine that the vd has been sync'ed, the following tests
     * are performed:
     * - wait for the device to become quiescent to indicate that no
     *   I/O is in progres
     * - wait for the consolidate queue to be drained; this is necessary
     *   as bs_bflush/bs_startio can guarantee only that buffers have been
     *   moved as far as the consolQ (due to the AdvfsmaxDevQLen throttle).
     */
     
    do {
        startiocalls[8]++;
        bs_startio( vdp, IO_FLUSH );

        /*
         * Wait for device to become quiescent.
         */
        mutex_lock( &vdp->devQ.ioQLock );
        mark_bio_wait;
        lk_wait_for( &vdp->active, &vdp->devQ.ioQLock, INACTIVE_DISK );
        unmark_bio_wait;
        mutex_unlock( &vdp->devQ.ioQLock );
        qlen = vdp->waitLazyQ.ioQLen;

    } while( vdp->consolQ.ioQLen   ||
             vdp->flushQ.ioQLen    ||
             vdp->ubcReqQ.ioQLen   ||
             vdp->blockingQ.ioQLen ||
             vdp->devQ.ioQLen );

    return( qlen );
}

decl_simple_lock_info(, ADVRangeFlushLock_info )

/*
 * bfflush
 *
 * Flush outstanding buffers in the specified page range.
 * If the first_page and pages_to_flush arguments are both 
 * zero, the caller is requesting a full file flush.
 *
 * The priority can be of 2 values: IMMEDIATE or INTERMEDIATE.
 * Immediate priority flushes are done on behalf of the kernel and
 * the ioDescriptors are placed onto the blocking queue for
 * immediate I/O.  Intermediate-priority flushes have their I/Os
 * placed onto the flushQ which will be processed slower than those
 * on the blockingQ, but before those on the lazy queues.
 *
 * SMP: 1. Entered with no locks held.
 *      2. Locks bfap->bfIoLock while walking the dirtyBufList.
 *      3. Also locks bp->bufLock in out-of-hierarchy order for each
 *         dirty buffer associated with the file that must be flushed.
 *      4. Lock the rangeFlushT.rangeFlushLock when bumping its
 *         outstandingIoCount field.
 *
 */

statusT
bfflush(
        bfAccessT* bfap,        /* in - ptr to access struct */
        bsPageT first_page,     /* in - first page to flush */
        bsPageT pages_to_flush, /* in - number of pages to flush  */
        int priority            /* in - priority of the flush */
    )
{
    struct bsBuf *bp;
    int listLen = 0;
    int listLenPart;
    ioDescT *ioList, *ioListPart, *save;
    lsnT flushTerminatorLsn;
    statusT result;
    bsPageT last_page;
    rangeFlushT *rfp = (rangeFlushT *)NULL;
    rangeFlushLinkT *rflp;
    bfAccessT *logbfap;
    lsnT hiCurrentLsn;
    boolean_t foundTerminator;
    int bsBufsCounted = 0;
    void *q_for_io;
    int noqfnd, couldnt_hold, sts;
    struct processor *myprocessor;        /* used by mark_bio_wait */

    if (priority & FLUSH_IMMEDIATE) q_for_io = bs_q_blocking;
    else if (priority & FLUSH_UBC) q_for_io = bs_q_ubcreq;
    else q_for_io = bs_q_flushq;

    MS_SMP_ASSERT( priority & FLUSH_IMMEDIATE ||
                   priority & FLUSH_INTERMEDIATE ||
                   priority & FLUSH_UBC);

    mutex_lock( &bfap->bfIoLock );

    /*
     * Init the disk error result of this flush.
     */
    bfap->dkResult = EOK;

    /*
     * Check for empty list first since when recycling a bfaccesss
     * other fields like bfsetp and dmnp could be invalid.
     */
    if ( bfap->dirtyBufList.accFwd == (struct bsBuf *)&bfap->dirtyBufList ) {
        result = bfap->dkResult;
        mutex_unlock( &bfap->bfIoLock );
        return result;
    }

    /* If we have dirty buffers we should always have a valid fileset
     * and domain.  The call from grab_bsacc() without dirty buffers is
     * being made with a null bfsetp and dmnp that points to trash, but
     * we believe that is the only invalid (and unneccessary) call.
     * This will catch any real nasty caller.
     */
    MS_SMP_ASSERT(BFSET_VALID(bfap->bfSetp));
    MS_SMP_ASSERT(TEST_DMNP(bfap->dmnP) == EOK);

    /* If both first_page and pages_to_flush are zero, caller wants to
     * do a full file flush for regular files.  This is not normally
     * used for metadata files, but it can get called this way if the
     * metadata file was opened outside the kernel via .tags of VM is
     * requesting an object fluxh due to low memory conditions
     */
    if (!first_page && !pages_to_flush) {

        /*
         * Calculate last_page and pages to flush even for a full-file
         * flush so that we don't flush out extra preallocated pages.
         * Can't use bfap->filesize for metadata files since it is
         * accurate only when on-disk data is copied to the bfaccess
         * structure on first open.  This blanket check of dataSafety
         * could cause preallocated pages of a data logging file to
         * be unnecessarily flushed here.  But until we consistently
         * maintain bfap->file_size for all files, that's a trade-off.
         * If we get the FLUSH_PREALLOCATED_PAGES flag passed in, we 
         * must be sure to flush all pages, even pre-allocated ones.  
         */
        if ( bfap->dataSafety == BFD_FTX_AGENT || 
             priority & FLUSH_PREALLOCATED_PAGES ) {
            pages_to_flush = bfap->nextPage;
            MS_SMP_ASSERT(pages_to_flush);
            last_page = pages_to_flush - 1;
        }
        else {
            last_page = (bfap->file_size > 0) ?
                (((roundup(bfap->file_size, ADVFS_PGSZ))/ADVFS_PGSZ) - 1) : 0;
            pages_to_flush = last_page + 1;
        }
    }
    else {
        MS_SMP_ASSERT(pages_to_flush);
        last_page = first_page + pages_to_flush - 1;
    }

    /* The last dirtyBufList buffer's flushSeq value will be the terminator
     * of this loop, not the physical makeup of the dirty list, which can
     * change as we go along.
     */
    flushTerminatorLsn = bfap->dirtyBufList.accBwd->flushSeq;

    /* 
     * Create a new rangeFlush structure.
     */
    mutex_unlock( &bfap->bfIoLock );
    rfp = (rangeFlushT *)(ms_malloc(sizeof(rangeFlushT)));
    mutex_init3(&rfp->rangeFlushLock, 0, "rangeFlushLock", 
                ADVRangeFlushLock_info);
    rfp->firstPage = first_page;
    rfp->lastPage = last_page;
    rfp->outstandingIoCount = 0;
    mutex_lock( &bfap->bfIoLock );
        
    /*
     * Scan the list of buffers creating a list of ioDesc
     * that did not yet make it to the blockingQ.
     * Queue that list to the blockingQ.
     * If we must block, either on IO_TRANS or FLUSH,
     * restart the scan.
     */

loop:

    /*
     * If this file is logged (i.e., this is either a metadata file
     * or a file that uses atomic write data logging), first flush the
     * log file up to the point that the on-disk log has all of the
     * log records associated with the bsBufs we care about.  This is
     * to preserve the "write the log first" rule.
     */
    if ((bfap->dataSafety == BFD_FTX_AGENT) ||
        (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY)) {
                           
        logbfap = bfap->dmnP->logAccessp;

        /* Scan the list for the greatest LSN value in the log
         * that must be flushed before we can flush the data.
         * Note that we do not lock the buffer to read currentLogRec.lsn
         * to avoid the lock overhead.
         */
        hiCurrentLsn = nilLSN;
        for (bp = bfap->dirtyBufList.accFwd;
             bp != (struct bsBuf *)&bfap->dirtyBufList &&
             LSN_GTE(flushTerminatorLsn, bp->flushSeq);
             bp = bp->accFwd) {
                
            MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);

            /*
             * If this bsBuf represents a page in the range we want to flush,
             * and its associated log record's LSN is the highest we've seen
             * yet, remember it.
             */
            if (((bp->bfPgNum >= rfp->firstPage) && 
                 (bp->bfPgNum <= rfp->lastPage)) &&
                (LSN_GT( bp->currentLogRec.lsn, hiCurrentLsn))) {
                hiCurrentLsn = bp->currentLogRec.lsn;
            }
        }

        /*
         * If necessary, flush the log.
         */
        if (LSN_GT(hiCurrentLsn, logbfap->hiFlushLsn)) {
            mutex_unlock(&bfap->bfIoLock);
            mutex_lock(&logbfap->bfIoLock);

            call_logflush( bfap->dmnP, hiCurrentLsn, TRUE );

            mutex_unlock(&logbfap->bfIoLock);
            mutex_lock(&bfap->bfIoLock);
        }
    }

    ioList = NULL;
    listLen = 0;
    foundTerminator = FALSE;

    for (bp = bfap->dirtyBufList.accFwd;
         bp != (struct bsBuf *)&bfap->dirtyBufList &&
         !foundTerminator && bsBufsCounted < pages_to_flush;
         bp = bp->accFwd) {

        u_long savAccSeq = bp->accListSeq;

        /* When the bsBuf's flushSeq is greater-than or equal-to the
         * flushTerminatorLsn, this is the last one we need to process
         * since we are past the ones that could have applied to this flush.
         * This works even if the flushSeq is reassigned to this bsBuf from
         * a later one since we will process this one if it is in range.
         */
        if (LSN_GTE(bp->flushSeq, flushTerminatorLsn)) {
            foundTerminator = TRUE;
        }

        /*
         * bfPgNum won't change while on dirytBufList and bfIoLock 
         * is held keeping buf on list.  Skip buffers out of range.
         */
        if ((bp->bfPgNum < rfp->firstPage) || (bp->bfPgNum > rfp->lastPage)) {
            continue;
        }

        couldnt_hold = advfs_page_get(bp, UBC_GET_WRITEHOLD);
        if (couldnt_hold) {
            /* The ubc may be in the process of exchanging the page
             * for numa locality or bigpage consolidation.  We need to
             * wait for it to finish, but we have to drop our locks and
             * there are no guarantees the page will be valid on wake up.
             */
            if (couldnt_hold == UBC_GET_WAIT_EXCHANGE) {
                ubc_fs_page_wait(bp->vmpage, &bfap->bfIoLock.mutex);
                mutex_lock(&bfap->bfIoLock);
                if (ioList) {
                    /* Point to the last buffer on our accumulated list and
                     * continue on to the next buffer on the dirtyBufList.
                     * This avoids having to retraverse the list past them.
                     */
                    bp = ioList->bwd->bsBuf;
                    /* make sure page being exchanged gets rechecked */
                    foundTerminator = FALSE;
                    continue;
                } else {
                    goto loop;
                }
            }

            /*
             * The page is busy.  Most likely, the UBC has initiated
             * an I/O on this page.  Since we want to count it as
             * part of this flush, if we have not already marked this 
             * bsBuf as belonging to this range flush, do so now by 
             * chaining onto the bsBuf a rangeFlushLinkT which points to 
             * our rfp.
             *
             * NOTE:  We know that an I/O is occurring on this page
             *        since the page is busy.  We also know that we
             *        failed to bump pg_hold on this page.  So the
             *        only reason we are able to proceed, referencing
             *        this vm_page/bsBuf and attaching a rangeFlushLinkT
             *        is because we are holding the bfap->bfIoLock.
             *        The I/O completion function, bs_io_complete(),
             *        needs to grab this lock in order to complete the
             *        I/O.  After the I/O is completed, the page may
             *        disappear, so it's critical at this place that
             *        the bfap->bfIoLock is held by this thread.
             */
            MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfIoLock.mutex));

            for (rflp = bp->rflList;
                 rflp && rflp->rfp != rfp;
                 rflp = rflp->rflFwd);
            if (!rflp) {
               /*
                * We're holding simple locks so we can't sleep.
                * If we can't allocate a rangeFlushLinkT without
                * sleeping, the system is in a very saturated
                * state.  Give up the CPU and try again.  We
                * expect this path to be taken very rarely.
                */
                rflp = (rangeFlushLinkT *)
                       ms_malloc_no_wait(sizeof(rangeFlushLinkT));
                if (!rflp) {
                    mutex_unlock(&bfap->bfIoLock);
                    if (listLen) {
                        bs_q_list( ioList, listLen, q_for_io );
                    }
                    assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
                    thread_block();
                    mutex_lock(&bfap->bfIoLock);
                    goto loop;
                }
                rflp->rflFwd = bp->rflList;
                rflp->rfp = rfp;
                bp->rflList = rflp;
                mutex_lock(&rfp->rangeFlushLock);
                rfp->outstandingIoCount++;
                bsBufsCounted++;
                
#ifdef ADVFS_SMP_ASSERT
                /*
                 * The following assertion cannot be applied to metadata
                 * files or clones since their bfap->file_size field, from which
                 * rfp->lastPage is derived on a full-file flush, is
                 * not maintained consistently.
                 */
                if (!BS_BFTAG_EQL(bfap->bfSetp->dirTag, staticRootTagDirTag) &&
                    (bfap->bfSetp->cloneId == BS_BFSET_ORIG)) {
                    MS_SMP_ASSERT(rfp->outstandingIoCount <= ((rfp->lastPage - rfp->firstPage) + 1));
                }
#endif
                mutex_unlock(&rfp->rangeFlushLock);
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
            }
            continue;
        }

        /* Try to lock each buffer out of hierarchy order. If this
         * fails, relock in correct order, but be sure that
         * buffer is still on dirty list after the locks are held.
         */
        if (!mutex_lock_try( &bp->bufLock.mutex )) {
            mutex_unlock( &bfap->bfIoLock );
            mutex_lock( &bp->bufLock );
            mutex_lock( &bfap->bfIoLock );
            if (bp->accListSeq != savAccSeq) /* bp removed from list */
            {
                /* buffer was removed from dirty list while we dropped the
                 * chain lock.  Skip this buffer, but must go back and
                 * restart the scan.
                 */
                mutex_unlock( &bp->bufLock );
                /* Drop UBC page (pg_hold count) reference. */
                ubc_page_release(bp->vmpage, B_WRITE);

                if (ioList) {
                    /* Point to the last buffer on our accumulated list and
                     * continue on to the next buffer on the dirtyBufList.
                     * This avoids having to retraverse the list past these
                     * buffers, as well as waiting for their IO_TRANS state
                     * to clear.
                     */
                    /* Since these iop are not on any Q the IO for that bp */
                    /* has not completed. Therefore it is still on the bfap */
                    /* dirtyBufList. */
                    bp = ioList->bwd->bsBuf;
                    MS_SMP_ASSERT(bp->bfAccess == bfap);
                    MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);
                    continue;
                } else {
                    goto loop;
                }
            }
        }

        MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);
        MS_SMP_ASSERT(bp->bfAccess == bfap);

        if (bp->lock.state & (FLUSH | IO_TRANS)) {

            /*
             * We will sleep waiting on the FLUSH and IO_TRANS states
             * so write out the buffers and begin again.
             */
            mutex_unlock( &bfap->bfIoLock );
            if ( listLen ) {
                mutex_unlock( &bp->bufLock );
                bs_q_list( ioList, listLen, q_for_io );
                mutex_lock( &bp->bufLock );
                if (bp->accListSeq != savAccSeq )  /* bp removed from list */
                {
                    /* We had to release both locks so make
                     * sure I/O didn't complete and bsBuf
                     * didn't get reused before waiting.
                     */
                    mutex_unlock( &bp->bufLock );
                    mutex_lock( &bfap->bfIoLock );
                    /* Drop UBC page (pg_hold count) reference. */
                    ubc_page_release(bp->vmpage, B_WRITE);
                    goto loop;
                }
            }

            wait_state(bp, FLUSH | IO_TRANS);
            mutex_unlock( &bp->bufLock );
            mutex_lock( &bfap->bfIoLock );
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;
        } else {
            bp->lock.state |= (FLUSH | IO_TRANS);
            bp->ln = SET_LINE_AND_THREAD(__LINE__);
        }

        /* 
         * If somebody else has this page pinned, mark it.
         * Note that this check must be performed after the
         * IO_TRANS check above because if it is performed
         * first, we might try to mark a temporary buffer
         * placed on the dirty buf by bs_pinpg_one_int()
         * processing which will then be removed when that
         * thread realizes he has lost a race with another
         * bs_pinpg_one_int() thread to initialize the page.
         */
        if (bp->writeRef != 0) {
            /*
             * If we have not already marked this bsBuf as belonging
             * to this range flush, do so now by chaining onto the
             * bsBuf a rangeFlushLinkT which points to our rfp.
             */
            for (rflp = bp->rflList;
                 rflp && rflp->rfp != rfp;
                 rflp = rflp->rflFwd);
            if (!rflp) {
               /*
                * We're holding simple locks so we can't sleep.
                * If we can't allocate a rangeFlushLinkT without
                * sleeping, the system is in a very saturated
                * state.  Give up the CPU and try again.  We
                * expect this path to be taken very rarely.
                */
                rflp = (rangeFlushLinkT *)
                       ms_malloc_no_wait(sizeof(rangeFlushLinkT));
                if (!rflp) {
                    /*
                     * I'm not going to send this bsBuf to the blocking queue
                     * after all.  Clear the flags that I set.
                     */
                    clear_state( bp, FLUSH );
                    clear_state( bp, IO_TRANS );

                    mutex_unlock(&bp->bufLock);
                    mutex_unlock(&bfap->bfIoLock);
                    if (listLen) {
                        bs_q_list( ioList, listLen, q_for_io );
                    }
                    /* Drop UBC page (pg_hold count) reference. */
                    ubc_page_release(bp->vmpage, B_WRITE);
                    assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
                    thread_block();
                    mutex_lock(&bfap->bfIoLock);
                    goto loop;
                }
                rflp->rflFwd = bp->rflList;
                rflp->rfp = rfp;
                bp->rflList = rflp;
                mutex_lock(&rfp->rangeFlushLock);
                rfp->outstandingIoCount++;
                bsBufsCounted++;
                
#ifdef ADVFS_SMP_ASSERT
                /*
                 * The following assertion cannot be applied to metadata
                 * files or clones since their bfap->file_size field, from which
                 * rfp->lastPage is derived on a full-file flush, is
                 * not maintained consistently.
                 */
                if (!BS_BFTAG_EQL(bfap->bfSetp->dirTag, staticRootTagDirTag) &&
                    (bfap->bfSetp->cloneId == BS_BFSET_ORIG)) {
                    MS_SMP_ASSERT(rfp->outstandingIoCount <= ((rfp->lastPage - rfp->firstPage) + 1));
                }
#endif
                mutex_unlock(&rfp->rangeFlushLock);
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
            }

            /*
             * I'm not going to send this bsBuf to the blocking queue
             * after all.  Clear the flags that I set.
             */
            clear_state( bp, FLUSH | IO_TRANS);

            mutex_unlock(&bp->bufLock);
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            continue;
        }

        /*
         * We are about to try to pull this bsBuf's I/O descriptor(s)
         * off of the lazy queues and attach them to our ioList.
         * If this is a data logging or metadata file, we need to
         * recheck that the log has been flushed up to a point such
         * that any log records that describe this update to the
         * bsBuf's page are on disk.  Yes, we already checked that
         * at the top of the loop but another thread in another
         * transaction may have subsequently modified the page again
         * and we can't write out the new copy of the page until its
         * corresponding log record is on disk.  Before we loop
         * back again, send any current ioList off to the blocking
         * queue since we're going to reinitialize ioList.
         */
        if ((bfap->dataSafety == BFD_FTX_AGENT) ||
            (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY)) {
            if (LSN_GT(bp->currentLogRec.lsn, hiCurrentLsn)) {
                mutex_unlock(&bfap->bfIoLock);
                clear_state(bp, FLUSH | IO_TRANS);
                mutex_unlock(&bp->bufLock);
                if (listLen) {
                    bs_q_list(ioList, listLen, q_for_io);
                }
                mutex_lock(&bfap->bfIoLock);
                /* Drop UBC page (pg_hold count) reference. */
                ubc_page_release(bp->vmpage, B_WRITE);
                goto loop;
            }
        }

        /* Must release the bfap->bfIoLock for rm_from_lazyq() since it will
         * lock vdp->vdIoLock which is higher in the hierarchy.  Note
         * bp->bufLock is unlocked and relocked by rm_from_lazyq().  Thus
         * there is no guarantee that this ioDesc is still on the lazy
         * queues; it may have completed IO by the time rm_from_lazyq()
         * returns.  If this is so, then we must see if this buffer was
         * taken off (and maybe put back on) the dirty list; it still
         * belongs to this file since we have the ubc page held.
         *
         * Now that IO_TRANS is set clear the FLUSH state, but don't
         * awaken any threads waiting for FLUSH.  They will be awakened
         * when I/O completes or on return when IO_TRANS is cleared.
         * The other user of FLUSH is the migrate path which sets FLUSH
         * in set_block_map() prior to pinning the page.  It will now
         * be able to set FLUSH once we clear it and release the locks,
         * and proceed to bs_pinpg where it will wait on IO_TRANS for
         * this buffer.
         */
        bp->lock.state &= ~FLUSH;
        mutex_unlock( &bfap->bfIoLock );
        rm_from_lazyq( bp, &ioListPart, &listLenPart, &noqfnd );
        mutex_lock( &bfap->bfIoLock );

        if (listLenPart || (noqfnd && (bp->lock.state & GETPAGE_WRITE))) {
            MS_SMP_ASSERT(bp->bfAccess == bfap);
            MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);

            /*
             * If we have not already marked this bsBuf as belonging
             * to this range flush, do so now by chaining onto the
             * bsBuf a rangeFlushLinkT which points to our rfp.
             */
            for (rflp = bp->rflList;
                 rflp && rflp->rfp != rfp;
                 rflp = rflp->rflFwd);
            if (!rflp) {
               /*
                * We're holding simple locks so we can't sleep.
                * If we can't allocate a rangeFlushLinkT without
                * sleeping, the system is in a very saturated
                * state.  Give up the CPU and try again.  We
                * expect this path to be taken very rarely.
                */
                rflp = (rangeFlushLinkT *)
                       ms_malloc_no_wait(sizeof(rangeFlushLinkT));
                if (!rflp) {
                    /*
                     * We're not going to send this bsBuf to the blocking queue
                     * after all.  Put the ioDescs back onto the lazyq
                     * so we'll find them next time.
                     */
                    bp->ln = SET_LINE_AND_THREAD(__LINE__);
                    mutex_unlock(&bp->bufLock);
                    mutex_unlock(&bfap->bfIoLock);
                    bs_q_list(ioListPart, listLenPart, bs_q_lazy);
                    if (listLen) {
                        bs_q_list( ioList, listLen, q_for_io );
                    }
                    mutex_lock(&bp->bufLock);
                    clear_state( bp, IO_TRANS );
                    mutex_unlock(&bp->bufLock);
                    /* Drop UBC page (pg_hold count) reference. */
                    ubc_page_release(bp->vmpage, B_WRITE);
                    assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
                    thread_block();
                    mutex_lock(&bfap->bfIoLock);
                    goto loop;
                }
                rflp->rflFwd = bp->rflList;
                rflp->rfp = rfp;
                bp->rflList = rflp;
                mutex_lock(&rfp->rangeFlushLock);
                rfp->outstandingIoCount++;
                bsBufsCounted++;

#ifdef ADVFS_SMP_ASSERT
                /*
                 * The following assertion cannot be applied to metadata
                 * files or clones since their bfap->file_size field, from which
                 * rfp->lastPage is derived on a full-file flush, is
                 * not maintained consistently.
                 */
                if (!BS_BFTAG_EQL(bfap->bfSetp->dirTag, staticRootTagDirTag) &&
                    (bfap->bfSetp->cloneId == BS_BFSET_ORIG)) {
                    MS_SMP_ASSERT(rfp->outstandingIoCount <= ((rfp->lastPage - rfp->firstPage) + 1));
                }
#endif
                mutex_unlock(&rfp->rangeFlushLock);
                bp->ln = SET_LINE_AND_THREAD(__LINE__);

            }

            /* Setup UBC page for IO.  We can fail here because an
             * mmapped page is wired by an application so we can not
             * change protection to make the page clean.  In that case
             * just write the page while keeping it dirty.  Any other
             * failure should be impossible so just prevent further
             * damage and crash it in development.
             */ 
            if ((sts = advfs_page_get(bp, UBC_GET_BUSY | UBC_GET_HAVEHOLD))) {
                if (sts == UBC_GET_VM_PROTECT && bp->bfAccess->bfVp &&
                        (bp->bfAccess->bfVp->v_flag & VMMAPPED)) {
                    sts = advfs_page_get(bp,
                              UBC_GET_BUSY | UBC_GET_HAVEHOLD |
                              UBC_GET_KEEPDIRTY);
                    if (!sts) {
                        bp->lock.state |= (BUSY | KEEPDIRTY);
                        bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                        bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                        bp->ioqLn = -1;
#endif
                    }
                }

                if (sts) {
                    /* must drop and retake bp->bufLock for live dump */
                    mutex_unlock(&bp->bufLock);
                    mutex_unlock(&bfap->bfIoLock);
                    ADVFS_SMP_ASSERT_FORCE_DOMAIN_PANIC_LEVEL3;
                    domain_panic(bfap->dmnP,
                                 "bfflush: advfs_page_get(%p) returned %x",
                                 bp, sts);
                    mutex_lock(&bp->bufLock);

                    /* Force advfs io completion to clean up our chains so
                     * we don't just have threads hanging.  The ubc page
                     * remains dirty until lru reuse forces it through the
                     * putpage path, where it will be tossed by bs_pinpg_put.
                     *
                     * Note we have not set BUSY so the page is not released
                     * in bs_io_complete().  We release our hold and restart
                     * since we pulled the bsBuf from the dirtyBufList.
                     */
                    bp->ln = SET_LINE_AND_THREAD(__LINE__);
                    bs_io_complete(bp, &sts);
                    ubc_page_release(bp->vmpage, B_WRITE);
                    if (listLen)
                        bs_q_list(ioList, listLen, q_for_io);
                    mutex_lock(&bfap->bfIoLock);
                    goto loop;
                }
            } else {
                bp->lock.state |= BUSY;
                bp->ln = SET_LINE_AND_THREAD(__LINE__);
#ifdef ADVFS_SMP_ASSERT
                bp->busyLn = SET_LINE_AND_THREAD(__LINE__);
                bp->ioqLn = -1;
#endif
            }

            /* Setup the IO list for GETPAGE_WRITE buffers. */
            if (!listLenPart) {
                link_write_req(bp);
                listLenPart = bp->ioCount;
                ioListPart = bp->ioList.ioDesc;
            }

            /*
             * Add the ioDesc we took off the lazy queue 
             * to the end of the list that we will be
             * sending to the blocking queue.
             */
            listLen += listLenPart;
            if (ioList == NULL) {
                ioList = ioListPart;
            } else {
                ioList->bwd->fwd = ioListPart;
                ioListPart->bwd->fwd = ioList;
                save = ioList->bwd;
                ioList->bwd = ioListPart->bwd;
                ioListPart->bwd = save;
            }

        } else if (bp->accListSeq != savAccSeq) {
            /* bp was removed from dirtyBufList */
            mutex_unlock( &bp->bufLock );
            if ( listLen ) {
                /* Put any buffers found onto the blocking queue before
                 * going back to the top and reinitializing the ioList.
                 */
                mutex_unlock( &bfap->bfIoLock );
                bs_q_list( ioList, listLen, q_for_io );
                mutex_lock( &bfap->bfIoLock );
            }
            /* Drop UBC page (pg_hold count) reference. */
            ubc_page_release(bp->vmpage, B_WRITE);
            goto loop;
        } else {
            /*
             * If the bsBuf was not removed from a lazy queue and I/O
             * is in progress, then we still want to wait for this I/O
             * to complete before returning to the caller so that the
             * changes are surely out on disk.
             */
            if (bp->lock.state & BUSY) {
                /*
                 * If we have not already marked this bsBuf as belonging
                 * to this range flush, do so now by chaining onto the
                 * bsBuf a rangeFlushLinkT which points to our rfp.
                 */
                for (rflp = bp->rflList;
                     rflp && rflp->rfp != rfp;
                     rflp = rflp->rflFwd);
                if (!rflp) {
                   /*
                    * We're holding simple locks so we can't sleep.
                    * If we can't allocate a rangeFlushLinkT without
                    * sleeping, the system is in a very saturated
                    * state.  Give up the CPU and try again.  We
                    * expect this path to be taken very rarely.
                    */
                    rflp = (rangeFlushLinkT *)
                           ms_malloc_no_wait(sizeof(rangeFlushLinkT));
                    if (!rflp) {
                        mutex_unlock(&bp->bufLock);
                        mutex_unlock(&bfap->bfIoLock);
                        if (listLen) {
                            bs_q_list( ioList, listLen, q_for_io );
                        }
                        /* Drop UBC page (pg_hold count) reference. */
                        ubc_page_release(bp->vmpage, B_WRITE);
                        assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
                        thread_block();
                        mutex_lock(&bfap->bfIoLock);
                        goto loop;
                    }
                    rflp->rflFwd = bp->rflList;
                    rflp->rfp = rfp;
                    bp->rflList = rflp;
                    mutex_lock(&rfp->rangeFlushLock);
                    rfp->outstandingIoCount++;
                    bsBufsCounted++;

#ifdef ADVFS_SMP_ASSERT
                    /*
                     * The following assertion cannot be applied to metadata
                     * files or clones since their bfap->file_size field, 
                     * from which rfp->lastPage is derived on a full-file flush,
                     * is not maintained consistently.
                     */
                    if (!BS_BFTAG_EQL(bfap->bfSetp->dirTag, staticRootTagDirTag) &&
                        (bfap->bfSetp->cloneId == BS_BFSET_ORIG)) {
                        MS_SMP_ASSERT(rfp->outstandingIoCount <= ((rfp->lastPage - rfp->firstPage) + 1));
                    }
#endif
                    mutex_unlock(&rfp->rangeFlushLock);
                    bp->ln = SET_LINE_AND_THREAD(__LINE__);
                }
            }
            clear_state( bp, IO_TRANS );
        }

        mutex_unlock( &bp->bufLock );
        /* Drop UBC page (pg_hold count) reference. */
        ubc_page_release(bp->vmpage, B_WRITE);

    } /* End for loop */

    if ( listLen ) {
        /*
         * Enter the buffers onto the blocking or flush queue.
         */
        mutex_unlock( &bfap->bfIoLock );
        bs_q_list( ioList, listLen, q_for_io );
        mutex_lock( &bfap->bfIoLock );
    }

    mutex_lock(&rfp->rangeFlushLock);
    while (rfp->outstandingIoCount) {
        assert_wait_mesg((vm_offset_t)&rfp->outstandingIoCount, FALSE, 
                         "rangeflushwait");
        mutex_unlock(&rfp->rangeFlushLock);
        mutex_unlock( &bfap->bfIoLock );
        mark_bio_wait;
        thread_block();
        unmark_bio_wait;
        mutex_lock( &bfap->bfIoLock );
        mutex_lock(&rfp->rangeFlushLock);
    }
    mutex_unlock(&rfp->rangeFlushLock);

    mutex_destroy(&rfp->rangeFlushLock);
    ms_free(rfp);

    result = bfap->dkResult;
    mutex_unlock( &bfap->bfIoLock );

    return( result );
}

static void
get_ioq( vdT *vdp, ioDescT *iop, ioDescHdrT **qhdr, int *on_lazyq )
{

    switch( iop->ioQ ) {

        case SMSYNC_LAZY0:
            *qhdr = &vdp->smSyncQ0;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY1:
            *qhdr = &vdp->smSyncQ1;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY2:
            *qhdr = &vdp->smSyncQ2;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY3:
            *qhdr = &vdp->smSyncQ3;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY4:
            *qhdr = &vdp->smSyncQ4;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY5:
            *qhdr = &vdp->smSyncQ5;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY6:
            *qhdr = &vdp->smSyncQ6;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY7:
            *qhdr = &vdp->smSyncQ7;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY8:
            *qhdr = &vdp->smSyncQ8;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY9:
            *qhdr = &vdp->smSyncQ9;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY10:
            *qhdr = &vdp->smSyncQ10;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY11:
            *qhdr = &vdp->smSyncQ11;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY12:
            *qhdr = &vdp->smSyncQ12;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY13:
            *qhdr = &vdp->smSyncQ13;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY14:
            *qhdr = &vdp->smSyncQ14;
            *on_lazyq = TRUE;
            break;
        case SMSYNC_LAZY15:
            *qhdr = &vdp->smSyncQ15;
            *on_lazyq = TRUE;
            break;
        case WAIT_LAZY: 
            *qhdr = &vdp->waitLazyQ;
            *on_lazyq = TRUE;
            break;
        case CONSOL:
            *qhdr = &vdp->consolQ;
            *on_lazyq = TRUE;
            break;
        case READY_LAZY:
            *qhdr = &vdp->readyLazyQ;
            *on_lazyq = TRUE;
            break;
        case TEMPORARYQ:
            *qhdr = (ioDescHdrT *)&vdp->tempQ;
            *on_lazyq = TRUE;
            break;
        case FLUSHQ:
            *qhdr = &vdp->flushQ;
            break;
        case UBCREQQ:
            *qhdr = &vdp->ubcReqQ;
            break;
        case BLOCKING:
            *qhdr = &vdp->blockingQ;
            break;
        case DEVICE:
            *qhdr = &vdp->devQ;
            break;
        case NONE:
            *qhdr = NULL;
            break;
        default:
            ADVFS_SAD0("get_ioq: unknown queue enum");
            break;
    }
}

/*
 * rm_from_lazyq
 *
 * Routine removes ioDesc belonging to buffer from lazy queues using
 * following rules:
 *  1.  Descriptors on the waitLazyQ, readyLazyQ, or consolQ are removed
 *      from that queue and returned as a list to the calling routine.
 *  2.  An ioDesc not on a queue or that has already made it onto either
 *      the blockingQ or the devQ is left alone.
 *  3.  The caller must guarantee the buffer does not get recycled.
 *      Since we must release the bfIoLock and the bsBuf.bufLock before
 *      calling this routine, it is possible that by time we seize the
 *      vdIoLock, if the buffer had been on the blocking or device queues,
 *      then the buffer could have completed I/O.
 *
 * SMP  1. This routine enters & leaves holding the bufLock. It drops the lock
 *         during the routine. Caller is responsible for preventing buffer
 *         recycling while the lock is dropped.
 *      2. Buffer state must be set by caller to IO_TRANS to protect ioDesc 
 *         structs from other threads.
 */

void
rm_from_lazyq( struct bsBuf *bp, 
             ioDescT **ioList, 
             int *listLen,
             int *noqfnd)  /* TRUE if all ioDescs were on no queue */

{
    int first, last, i;
    ioDescT *iop;
    ioDescHdrT *qhdr = NULL, *new_qhdr = NULL;
    struct vd *vdp;
    extern u_int smsync_step;
    extern u_int smsync_period;
    u_int period = smsync_period;
    u_int j;
    domainT *dmnp = bp->bfAccess->dmnP;
    u_long savListSeq = bp->accListSeq;
    int noq_cnt = 0, on_lazyq = FALSE, lockflag = FALSE;
    mutexT *lockptr;

    *ioList = NULL;
    *listLen = 0;

    /* The buffer is scheduled for IO. It is on one of the I/O queues
     * or is on a temporary queue because it is being moved between
     * I/O queues.
     */
    MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY);

    /* The ioList is protected by bufLock. */
    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex));
    first = bp->ioList.write;
    last = bp->ioList.write + bp->ioList.writeCnt;

    /* "first" will always be 0, "last" will never be more than 2. */
    /* There are never more than 2 ioDescT per bp, one for the xtntMap */
    /* location on disk, one for the copyXtntMap location. */
    MS_SMP_ASSERT(first == 0);
    MS_SMP_ASSERT(last <= 2);

    for( i = first; i < last; i++ ) {

        iop = &bp->ioList.ioDesc[i];

        /*
         * Find the queue the descriptor is on.
         */
        vdp = VD_HTOP(iop->blkDesc.vdIndex, dmnp);

tryagain:
        on_lazyq = FALSE;
        get_ioq(vdp, iop, &qhdr, &on_lazyq);

        if (qhdr == NULL)
            noq_cnt++;

        /*
         * If iop is on a lazy or temporary queue, remove it.
         */
again:
        if ( on_lazyq ) {

            lockptr = &qhdr->ioQLock;

            if (!mutex_lock_try(&lockptr->mutex)) {

                mutex_unlock( &bp->bufLock );

                /* From this point until the ioQLock is seized, the bp is 
                 * free to change. It can move from queue to queue. One of 
                 * the two possible iop's can complete IO. The bp can even 
                 * have all IO completed and be recycled to another page or 
                 * bfap.
                 */

                mutex_lock( lockptr );
                lockflag = TRUE;

                mutex_lock( &bp->bufLock );

                if ( bp->accListSeq != savListSeq ) {
                    /* IO got done while we waited for the lock. If we had 
                     * removed 1 of the 2 possible iops, the IO could not
                     * have completed. Therefore we must not have removed any
                     * iops.
                     */
                    MS_SMP_ASSERT(*listLen == 0);
                    mutex_unlock( lockptr );
                    lockflag = FALSE;
                    break;
                }

                if ( i >= bp->ioList.write + bp->ioList.writeCnt ) {
                    /* ioList got changed by a call to buf_remap(). This can 
                     * only happen if there were 2 iop and migration got done
                     * then buf_remap() was called.
                     */
                    MS_SMP_ASSERT(i == 1);
                    MS_SMP_ASSERT(bp->ioList.write == 0);
                    MS_SMP_ASSERT(bp->ioList.writeCnt == 1);
                    mutex_unlock( lockptr );
                    lockflag = FALSE;
                    break;
                }

                MS_SMP_ASSERT(&bp->ioList.ioDesc[i] == iop);
            } else 
                lockflag = TRUE;

            /* Recheck the I/O queue the ioDesc is on since it may have
             * moved between the first check and obtaining the I/O queue
             * lock above
             */
            on_lazyq = FALSE;
            get_ioq(vdp, iop, &new_qhdr, &on_lazyq);

            if (new_qhdr != qhdr) {
                mutex_unlock( lockptr );
                lockflag = FALSE;
                qhdr = new_qhdr;
                if (qhdr == NULL)
                    noq_cnt++;
                goto again;
            }

#ifdef ADVFS_SMP_ASSERT
            iop->bsBuf->ioqLn=SET_LINE_AND_THREAD(__LINE__);
#endif

            /* The casting below is needed in case we are removing
             * from vdp->tempQ
             */
            iop->fwd->bwd = (ioDescT *)iop->bwd;
            iop->bwd->fwd = (ioDescT *)iop->fwd;
            iop->ioQ = NONE;

            SMSYNC_DBG( SMSYNC_DBG_BUF,
                aprintf ( "rm_or_mvq:     bsBuf 0x%lx  vd 0x%lx\n", bp, vdp ));

            if ( qhdr != (ioDescHdrT *)&vdp->tempQ) {
                MS_SMP_ASSERT(qhdr->ioQLen > 0);
                qhdr->ioQLen--;
                MS_VERIFY_IOQUEUE_INTEGRITY( qhdr, lockflag );
            }

            /*
             * Build the list that we will return
             */
            if ( *ioList == NULL ) {
                *ioList = iop;
                iop->fwd = iop->bwd = iop;
            } else {
                iop->fwd = *ioList;
                iop->bwd = (*ioList)->bwd;
                (*ioList)->bwd->fwd = iop;
                (*ioList)->bwd = iop;
            }
            (*listLen)++;
        }

        if (lockflag) {
            mutex_unlock( lockptr );
            lockflag = FALSE;
        }
    }

    /*
     * Tell the caller if none of the buffer's IoDesc's are linked
     * to an IO queue. The noqfnd result is used in conjunction with the
     * listLen return value.
     */
    *noqfnd = (noq_cnt == (last - first)) ? TRUE : FALSE;

    return;
}

/*
 * link_write_req
 *
 * Create a circular linked list of the I/O 
 * descriptors to hand off to bs_q_list.
 * Increases the buffers I/O count as required.
 */

void
link_write_req( struct bsBuf *bp )
{
    int last, first, i;
    ioDescT *ioDescp;

    first = bp->ioList.write;
    last = bp->ioList.write + bp->ioList.writeCnt;

    ioDescp = bp->ioList.ioDesc;

    for( i = first; i < last; i++ ) {

        if ( i == first ) {
            ioDescp[last - 1].fwd = &ioDescp[i];
        } else {
            ioDescp[i - 1].fwd = &ioDescp[i];
        }

        if ( i == last - 1 ) {
            ioDescp[first].bwd = &ioDescp[i];
        } else {
            ioDescp[i + 1].bwd = &ioDescp[i];
        }
    }
    bp->ioCount = bp->ioList.writeCnt;
}

/*
 * rm_ioq
 *
 * Remove a buffer from any I/O queue it happens to be on.  
 * Assume the caller has checked for a busy buffer.
 *
 * SMP: 1. Called with no locks held
 *      2. IO_TRANS must be set for the bsBuf to assure that a buffer 
 *         cannot become busy (because it cannot be queued while in IO_TRANS).
 */

rm_ioq( struct bsBuf *bp )
{
    int first, last, i, on_lazyq = FALSE;
    ioDescT *iop;
    ioDescHdrT *qhdr = NULL, *new_qhdr = NULL;
    mutexT *lockptr;
    struct vd *vdp;
    extern u_int smsync_step;
    extern u_int smsync_period;
    u_int period = smsync_period;
    u_int j;

    MS_SMP_ASSERT( bp->lock.state & IO_TRANS );
    first = bp->ioList.write;
    last = bp->ioList.write + bp->ioList.writeCnt;

    for( i = first; i < last; i++ ) {

        iop = &bp->ioList.ioDesc[i];

        /*
         * Find the queue the descriptor is on.
         */
        vdp = VD_HTOP(iop->blkDesc.vdIndex, bp->bfAccess->dmnP);

tryagain:
        on_lazyq = FALSE;
        get_ioq(vdp, iop, &qhdr, &on_lazyq);

        MS_SMP_ASSERT( (qhdr != &vdp->blockingQ) && (qhdr != &vdp->devQ) && (qhdr != &vdp->flushQ) && (qhdr != &vdp->ubcReqQ) );

again:
        if ( qhdr != NULL ) {

            SMSYNC_DBG( SMSYNC_DBG_BUF,
                aprintf ( "rm_ioq:        bsBuf 0x%lx  vd 0x%lx\n", bp, vdp ));

            lockptr = &qhdr->ioQLock;
            mutex_lock( lockptr );

            /* Recheck the I/O queue the ioDesc is on since it may have
             * moved between the first check and obtaining the I/O queue
             * lock above
             */
            on_lazyq = FALSE;
            get_ioq(vdp, iop, &new_qhdr, &on_lazyq);

            MS_SMP_ASSERT( (new_qhdr != &vdp->blockingQ) && (new_qhdr != &vdp->devQ) && (new_qhdr != &vdp->flushQ) && (new_qhdr != &vdp->ubcReqQ) );

            if ( new_qhdr != qhdr ) {
                 mutex_unlock( lockptr );
                 qhdr = new_qhdr;
                 goto again;
            }

            /* Remove ioDesc from queue. The casting is needed if we
             * are removing from the temporary queue.
             */
            iop->fwd->bwd = (ioDescT *)iop->bwd;
            iop->bwd->fwd = (ioDescT *)iop->fwd;
            iop->fwd = iop->bwd = NULL;

#ifdef ADVFS_SMP_ASSERT
            iop->bsBuf->ioqLn=SET_LINE_AND_THREAD(__LINE__);
#endif
            iop->ioQ = NONE;

            if ( qhdr != (ioDescHdrT *)&vdp->tempQ ) {
                MS_SMP_ASSERT(qhdr->ioQLen > 0);
                qhdr->ioQLen--;
                MS_VERIFY_IOQUEUE_INTEGRITY( qhdr, TRUE );
            }
            mutex_unlock( lockptr );
        }
    }
}

/*
 * bs_raw_page
 *
 * Read or write a sequence of blocks bypassing
 * the buffer cache.
 *
 * SMP: 1. No locks held on entry
 */

statusT
bs_raw_page( bfAccessT *bfap,           /* in */
            unsigned vdIndex,           /* in */
            unsigned startBlk,          /* in */
            unsigned blkCnt,            /* in */
            unsigned char *buf,         /* in - out */
            rawModeT rw                 /* in */
            )
{
    struct bsBuf *bp;
    ioDescT *iop;
    int result;
    int wait = 0;
    struct processor *myprocessor;        /* used by mark_bio_wait */

    /* Acquire a raw buffer header. */
    bp = bs_get_bsbuf(CURRENT_MID(), FALSE);
    if (!bp)
        return(ENO_MORE_MEMORY);

    /* Fill in fields of new buffer descriptor. */
    bp->bfAccess = bfap;

    if ( rw == RREAD ) {
        bp->lock.state = IO_TRANS | RAWRW | BUSY | READING;
    } else {
        bp->lock.state = IO_TRANS | RAWRW | BUSY | WRITING;
        bp->writeRef = 1;
    }

    bp->ioCount = 1;

    iop = &bp->ioDesc;
    iop->fwd = iop->bwd = iop;
    iop->blkDesc.vdIndex = vdIndex;
    iop->blkDesc.vdBlk = startBlk;
    iop->ioQ = NONE;
    iop->numBlks = blkCnt;
    /* No vm_page in bsbuf, raw I/O address passed via targetAddr */
    iop->targetAddr = buf;
    iop->bsBuf = bp;
#ifdef ADVFS_SMP_ASSERT
    iop->bsBuf->ioqLn=SET_LINE_AND_THREAD(__LINE__);
#endif
    iop->ioCount = 1;
    bp->ioList.ioDesc = iop;

    bs_q_blocking( iop, 1 );

    /* Wait for the I/O to complete. */
    mutex_lock( &bp->bufLock );
    wait = 0;
    mark_bio_wait;
    while( bp->lock.state & BUSY ) {
        state_block( bp, &wait );
    }
    unmark_bio_wait;
    result = bp->result;
    mutex_unlock( &bp->bufLock );

    bs_free_bsbuf(bp);
    return( result );
}


/*
 * **** Strictly user mode routines ****
 */

/*
 * dev_trace
 *
 * Trace the device action.
 */

void
dev_trace( 
    int vdblk,              /* in */
    int vdi,                /* in */
    bfTagT tag,             /* in */
    u_long bsPage,          /* in */
    TraceActionT type       /* in */
    )
{
    int action;

    for( action = 0; action < sizeof( unsigned ) * 8; action++ ) {
        if ( type & 1 )
            break;
        type >>= 1;
    }

    trace_hdr();

    log( LOG_MEGASAFE | LOG_INFO,
        "%10s: vb%10d %3dvd %10d pag   %10d tag\n",
        PrintAction[ action ], vdblk, vdi, bsPage, BS_BFTAG_IDX(tag) );

}


/*
 * bs_io_thread
 *
 * This is a routine that runs as a background kernel thread.  It monitors
 * a message queue for a particular RAD, waiting for requests for I/O from that
 * RAD.  These requests can be of the following types:
 *     LF_PB_CONT:     Log-flush or pinblock continuation; which is determined
 *                         by the bits set in dmnP->contBits.
 *                         (These are currently not set in the code anywhere!)
 *     START_MORE_IO:  Start some more I/Os on the domain/vd specified.
 *     THREAD_GO_AWAY: A signal that the thread's job is done.
 */

void
bs_io_thread( int radId )
{
    ioThreadMsgT *msg;
    char done = 0;

    /* The ioThreadMsg Queue now uses an internal mutex to protect the
     * queue, so no need to seize external mutex here.
     */
    while (!done) {

        /* if the queue disappears, we've probably lost some I/O */
        MS_SMP_ASSERT(IoMsgQH[radId] != NULL);

        msg = (ioThreadMsgT *)msgq_recv_msg( IoMsgQH[radId] );
        switch( msg->msgType ) {

            case LF_PB_CONT:
            {
                domainT* dmnP;
                statusT sts;

                /*
                 * The call to bs_domain_access() is necessary
                 * in order to get a ref on the domain.  This
                 * prevents a race with dmn_dealloc().
                 *
                 * If the domain no longer exists, that's ok.
                 * Domain deactivation flushes buffers/log.
                 */

                sts = bs_domain_access(&dmnP, msg->u_msg.dmnId, TRUE);
                if (sts == EOK) {
                    check_cont_bits(dmnP, 1);
                    bs_domain_close(dmnP);
                }
                break;
            }

            case START_MORE_IO:
            {
                /* bs_osf_complete() noticed that the devQ is below threshold,
                 * so call bs_startio() to process some more I/O.
                 */

                domainT *dmnP = msg->u_msg.dmnP;
                uint32T vdi = msg->vdi;
                struct vd *vdp = msg->vdp;

                /* Check to see if the domain and the vd are still there.
                 * It should no longer be possible to have the domain
                 * and vd go away while a START_MORE_IO message is queued.
                 * The MS_SMP_ASSERT is here to test this theory and should
                 * eventually be removed (along with the TEST_DMNP).
                 *
                 * For this reason also we do not bump the refCnt on the vd;
                 * the start_io_posted logic should be sufficient.
                 *
                 * We also assume the vdp from the message is valid.
                 * Note that if the volume is being removed, vdp may not
                 * be found in the dmnP->vdpTbl[].
                 */

                MS_SMP_ASSERT(TEST_DMNP(dmnP) == EOK);
                MS_SMP_ASSERT( vdp );
                if ((TEST_DMNP(dmnP) == EOK) && vdp) {

                    startiocalls[9]++;
                    mutex_lock( &vdp->devQ.ioQLock );

                    /*
                     * In the normal case, we're called to refill the devQ,
                     * allow some of the lazy buffers to be flushed,
                     * but don't force them all to the device.  The
                     * # flushed from the lazy queues can be modified
                     * via 'chvol -q' to change vdT.qtodev.
                     *
                     * Reset start_io_posted before bs_startio, because
                     * bs_startio() temporarily drops the devQ.ioQLock
                     * (in call_disk()) and that could cause synchonization
                     * problems with bs_osf_complete().
                     *
                     * If start_io_posted_waiter is set, vd_remove is removing
                     * the vd associated with this message.  The message must
                     * have been posted just before the call to vd_remove.
                     * vd_remove has drained all the IOs already and is waiting
                     * for this message to be processed before freeing the
                     * vd structure and perhaps the domain as well.
                     * So, when start_io_posted_waiter is set, this message can
                     * be treated as a no-op and vd_remove must be unblocked.
                     */

                    MS_SMP_ASSERT( vdp->start_io_posted != 0 );
                    vdp->start_io_posted = 0;
                    if (vdp->start_io_posted_waiter) {
                        vdp->start_io_posted_waiter = 0;
                        MS_SMP_ASSERT( vdp->devQ.ioQLen == 0 );
                        thread_wakeup( (vm_offset_t) &vdp->start_io_posted_waiter );
                        mutex_unlock( &vdp->devQ.ioQLock );
                    } else {
                        mutex_unlock( &vdp->devQ.ioQLock );
                        bs_startio( vdp, IO_SOMEFLUSH );
                    }
                }
                break;
            }
            case THREAD_GO_AWAY: 
            {
                done = 1;
                break;
            }
            case RETRY_IO:
            {
                uint32T     vdBlk;
                int         ioAmt;
                struct buf *bp;
                ioDescT    *iop;
                struct vd  *vdp;
                domainT    *dmnP;

                bp = msg->u_msg.ioRetryBp;
                iop = (ioDescT *)bp->b_pagelist;
                vdBlk = iop->blkDesc.vdBlk;
                dmnP = iop->bsBuf->bfAccess->dmnP;
                vdp = VD_HTOP( iop->blkDesc.vdIndex, dmnP );

                if ( iop->desCnt != 0 ) {
                    ioAmt = iop->totalBlks * BS_BLKSIZE;
                }
                else if (iop->bsBuf->directIO) {
                    /* if Direct I/O numBlks is really number of bytes */
                    ioAmt = iop->numBlks;
                }
                else {
                    ioAmt = iop->numBlks * BS_BLKSIZE;
                }

                freeosfbuf( bp );

                mutex_lock( &vdp->devQ.ioQLock );
                if ( iop->ioRetryCount < USHRT_MAX )
                    iop->ioRetryCount++;
                if ( vdp->vdRetryCount < UINT_MAX )
                    vdp->vdRetryCount++;
                mutex_unlock( &vdp->devQ.ioQLock );

                call_disk( vdp, ioAmt, (daddr_t)vdBlk, iop );

                break;
            }
            default:
                ADVFS_SAD1("bs_io_thread: unknown message type ", msg->msgType);
        }
        msgq_free_msg( IoMsgQH[radId], msg );
    }
}

/*
 * bs_init_io_thread
 *
 * Creates a message queue and the I/O thread for all valid rads.  
 * The message queues are used by the I/O completion routine to send 
 * messages to the I/O threads.
 */
#define INIT_MSG_IO_THREAD 100   /* heuristic value, used to be based */
                                /* on size of domain table           */
void
bs_init_io_thread( void )
{
    int i, num_initialized_rads = 0;

    IoMsgQH = (msgQHT *) ms_malloc( nrads * sizeof(msgQHT) );

    for (i=0;i<nrads;i++){
        IoMsgQH[i] = NULL;  /* all queues should be NULL unless they and accompanying thread */
                            /* have been successfully allocated. */
        if ((rad_id_to_rad(i) != NULL) && (bs_io_rad_start(i) == 0)){ /* checks if rad 'i' is a valid */
            num_initialized_rads++;         /* rad, and if it is, it does a bs_io_rad_start on it. if */
        }                      /* bs_io_rad_start returns 0, then we have an initialized, working rad */
    }
    if (num_initialized_rads == 0){
        /* we have no initialized rads!  Panic the system, because without at least on queue and matching */
        /* thread, the file system will NOT function normally! */
        ADVFS_SAD0( "bs_init_io_thread: can't create at least one rad's msq queue and I/O thread" );
    }
}

/*
 * bs_io_rad_start: sets up the IoMsgQueue and IoThread for a new RAD
 *
 * returns:
 *   0: A-Ok
 *   1: Invalid Arguement (radId<0 || radId>=nrads)
 *   2: Could not create IoMsgQueue on specified RAD (possibly out of memory)
 *   3: Could not spawn kernel thread on specified RAD (possibly no working CPU's)
 */

int bs_io_rad_start(int radId)
{
    thread_t ioThreadH;
    statusT sts;

    if ((radId < 0) || (radId >= nrads)) {
        return 1;
    }

    /*
     * Create a message queue.  It is used to send messages to 
     * the I/O thread.
     */
    sts = msgq_create( &IoMsgQH[radId],         /* returned handle to queue */
                       INIT_MSG_IO_THREAD,       /* max message in queue */
                       sizeof( ioThreadMsgT ),  /* max size of each message */
                       TRUE,                   /* ok to increase msg q size */
                       radId                    /* rad Id the queue should be created on */
                     );
    if (sts != EOK) {
        return 2;
    }
    /*
     * Create and start the I/O thread.
     */
    ioThreadH = rad_kernel_thread( bs_io_thread, (void *) radId, BASEPRI_SYSTEM, radId );
    if (ioThreadH == THREAD_NULL) {
        msgq_destroy( IoMsgQH[radId] );
        return 3;
    }
    return 0;
}

#ifdef THIS_IS_FOR_HOT_SWAPABILITY

/*
 * bs_io_rad_stop: destroys the IoMsgQueue and IoThread for a RAD that is going away
 *
 * returns:
 *   0: A-Ok
 *   1: Invalid Arguement (radId<0 || radId>=nrads || radId was never RAD_STARTED)
 *   2: Could not destroy IoMsgQueue on specified RAD (possibly out of memory)
 *   3: Could not kill kernel thread on specified RAD (possibly no working CPU's)
 */
#define MAX_RAD_DESTROY_ATTEMPTS 1000
#define DELAY_BETWEEN_RAD_DESTROY_ATTEMPTS 100

int bs_io_rad_stop(int radId)
{
    thread_t ioThreadH;
    statusT sts;
    msgQHT qToDestroy;
    ioThreadMsgT *msg;
    int i;

    if ((radId < 0) || (radId >= nrads) || (IoMsgQH[radId] == NULL))
    {
        return 1;
    }

    /* save a pointer to the queue and then set the REAL pointer to NULL so no one
     * else can play with the queue while we're getting rid of it!
     */
    qToDestroy = IoMsgQH[radId];
    IoMsgQH[radId] = NULL;

    msg = (ioThreadMsgT *)msgq_alloc_msg( qToDestroy );
    if (msg) {
        msg->msgType = THREAD_GO_AWAY;
        msgq_send_msg( qToDestroy, msg );
    }
    else {
        IoMsgQH[radId] = qToDestroy;
        return 3;
    }
    for(i = 0;(i < MAX_RAD_DESTROY_ATTEMPTS) && (qToDestroy != NULL); i++){
        msgq_destroy( qToDestroy );
        /* ?does this make sense?
         sleep( DELAY_BETWEEN_RAD_DESTROY_ATTEMPTS );
         */ 
    }
    if (qToDestroy != NULL){
        return 2;
    }
    return 0;
}

#endif

int 
sendtoiothread(struct vd *vdp,                  /* in */
               ioThreadMsgTypeT sendType,       /* in */
               struct buf *bp)                  /* in - NULL if not needed */
{
    ioThreadMsgT *msg;
    int radId = 0, i;
    int rtn = 0;

    radId = PA_TO_MID(vtop(current_processor(), vdp));
    if ( (IoMsgQH[radId] == NULL) && (bs_io_rad_start(radId) != 0) ){
        for(i = 0; i < nrads; i++){
            /* if the RAD isn't valid and we can't start it, find
             * a valid one!!
             */
            if (IoMsgQH[i] != NULL){
                radId = i;
                break;
            }
        }
        MS_SMP_ASSERT( IoMsgQH[radId] != NULL );
    }
    
    msg = (ioThreadMsgT *)msgq_alloc_msg( IoMsgQH[radId] );
    if (msg) {
        if ( sendType == START_MORE_IO ) {
            /*
             * Send a START_MORE_IO message
             */
            msg->u_msg.dmnP = vdp->dmnP;
        }
        else if ( sendType == RETRY_IO ) {
            /*
             * Send a Retry I/O message.
             */
            msg->u_msg.ioRetryBp = bp;
        }
        else {
            /*
             * Unknown send sendType
             */
            MS_SMP_ASSERT(0);
            rtn = 1;
            goto _done;
        }
        msg->msgType = sendType;
        msg->vdp = vdp;
        msg->vdi = vdp->vdIndex;
        msgq_send_msg( IoMsgQH[radId], msg );
    } else {
        /*
         * Could not get message buffer
         */
        rtn = 1;
        goto _done;
    }

_done:
    return(rtn);
}

ioDescHdrT *
get_smsyncq(vdT *vdp, int stampq, int *ioQ)
{
   switch (stampq) {
       case 0:
                *ioQ = SMSYNC_LAZY0;
                return &vdp->smSyncQ0;
       case 1: 
                *ioQ = SMSYNC_LAZY1;
                return &vdp->smSyncQ1;
       case 2: 
                *ioQ = SMSYNC_LAZY2;
                return &vdp->smSyncQ2;
       case 3: 
                *ioQ = SMSYNC_LAZY3;
                return &vdp->smSyncQ3;
       case 4: 
                *ioQ = SMSYNC_LAZY4;
                return &vdp->smSyncQ4;
       case 5: 
                *ioQ = SMSYNC_LAZY5;
                return &vdp->smSyncQ5;
       case 6: 
                *ioQ = SMSYNC_LAZY6;
                return &vdp->smSyncQ6;
       case 7: 
                *ioQ = SMSYNC_LAZY7;
                return &vdp->smSyncQ7;
       case 8: 
                *ioQ = SMSYNC_LAZY8;
                return &vdp->smSyncQ8;
       case 9: 
                *ioQ = SMSYNC_LAZY9;
                return &vdp->smSyncQ9;
       case 10: 
                *ioQ = SMSYNC_LAZY10;
                return &vdp->smSyncQ10;
       case 11: 
                *ioQ = SMSYNC_LAZY11;
                return &vdp->smSyncQ11;
       case 12: 
                *ioQ = SMSYNC_LAZY12;
                return &vdp->smSyncQ12;
       case 13: 
                *ioQ = SMSYNC_LAZY13;
                return &vdp->smSyncQ13;
       case 14:
                *ioQ = SMSYNC_LAZY14;
                return &vdp->smSyncQ14;
       case 15: 
                *ioQ = SMSYNC_LAZY15;
                return &vdp->smSyncQ15;
       default:
                return NULL;
   }
}
       

/*
 * Add ioDesc's onto the appropriate smoothsyncQ for given vd.  If the
 * buffer has aged past the point of the oldest smoothsync queue, put
 * it onto the readyLazyQ.
 */

void
add_to_smsync( 
    struct vd *vdp,             /* in */
    ioDescT *ioListp,           /* in */
    u_int len,                  /* in */
    mutexT *incoming_lockp,     /* in */
    tempQMarkerT *tempQ_marker  /* in */
    )
{
    ioDescT *iop = ioListp;
    ioDescT *next;
    ioDescT *aged_list = (ioDescT *)NULL;
    int aged_cnt = 0;
    ioDescHdrT incoming_header, *smsyncq;
    ioDescT *incoming_hdrp = (ioDescT *)&incoming_header;
    u_int stampq, stamp;
    extern u_int smsync_age;
    extern u_int smsync_step;
    extern u_int smsync_period;
    u_int period = smsync_period;
    int ioQ;

    /* The waitLazy queue lock is held and/or the associated buffers are
     * marked IO_TRANS. Either/both these conditions prevent a racing
     * remove from the list.
     */
    incoming_header.ioQLen = len;
    incoming_header.lenLimit = 0;
    incoming_hdrp->fwd  = ioListp;
    incoming_hdrp->bwd  = ioListp->bwd;
    ioListp->bwd->fwd = incoming_hdrp;
    ioListp->bwd      = incoming_hdrp;

    iop = ioListp;
    while ( iop != incoming_hdrp ) {

        /*
         * Add buffer to the smoothsync queue which will get processed at
         * the time at which the buffer has sufficiently aged.
         */
        if ( sched_tick - iop->bsBuf->sync_stamp < smsync_age && period ) {
            /* 
             * Avoid the cost of the division operation for the
             * common cases of smsync_period value of 1, 2, or 4
             * (i.e. smsync_age value of 15, 30, 60 seconds).
             */
            switch ( period ) {
            case 0:     stamp = 0;
                        break;
            case 1:     stamp = iop->bsBuf->sync_stamp;
                        break;
            case 2:     stamp = iop->bsBuf->sync_stamp >> 1;
                        break;
            case 4:     stamp = iop->bsBuf->sync_stamp >> 2;
                        break;
            default:    stamp = iop->bsBuf->sync_stamp/period;
            }

            stampq = (stamp+smsync_step) % SMSYNC_NQS;
            SMSYNC_DBG( SMSYNC_DBG_BUF,
                aprintf ( "addtosmsync:   bsBuf 0x%lx  added onto smSyncQ %d (time %d, stamp %d)  vd 0x%lx\n",
                iop->bsBuf, stampq, sched_tick, stamp, vdp ));

            /* Remove iop from incoming list and put it onto the appropriate 
             * smoothsync queue. The waitLazy queue lock is held or the
             * associated bsBuf is marked IO_TRANS protecting the ioDesc 
             * from removal. So no need to worry about losing our way down 
             * the list.
             */
            next = iop->fwd;
            iop->bwd->fwd = iop->fwd;
            iop->fwd->bwd = iop->bwd;

            smsyncq = get_smsyncq(vdp, stampq, &ioQ);
            mutex_lock( &smsyncq->ioQLock );
            iop->fwd = (ioDescT *)smsyncq;
            iop->bwd = smsyncq->bwd;
            iop->ioQ = ioQ;
            smsyncq->bwd->fwd = iop;
            smsyncq->bwd = iop;
            smsyncq->ioQLen++;
            smsyncq->queue_cnt++;

            MS_VERIFY_IOQUEUE_INTEGRITY( smsyncq, TRUE );

            mutex_unlock( &smsyncq->ioQLock );
            iop = next;
        } else {
            /* Move aged buffers to the aged_list.  After all have been
             * accumulated, we will call sort_onto_readyq() to put 
             * these buffers onto the readyLazyQ.
             */

            /* Remove from incoming list */
            next = iop->fwd;
            iop->bwd->fwd = iop->fwd;
            iop->fwd->bwd = iop->bwd;

            /* Put onto aged list */
            if ( aged_list ) {
                iop->fwd = aged_list;
                aged_list->bwd->fwd = iop;
                iop->bwd = aged_list->bwd;
                aged_list->bwd = iop;
            } else {
                aged_list = iop;
                iop->fwd = iop->bwd = iop;
            }
            aged_cnt++;
            iop = next;
        }
    }

    if ( aged_list ) {
        /* sort_onto_readyq() will release the incoming (waitLazyQ)
         * lock no matter what path it takes internally.  If it drops
         * it before the chain is completely on the readyq, then it
         * will mark each of the incoming elements on the temporary
         * queue to prevent the 'remove' routines from thinking that
         * they are still on the waitLazyQ.
         */
        sort_onto_readyq( vdp, aged_list, aged_cnt,
                          incoming_lockp, tempQ_marker );
    } else if (incoming_lockp) {
        /* If incoming lock was passed and we didn't release it by
         * calling sort_onto_readyq(), release it now.
         */
        mutex_unlock( incoming_lockp );
    }
}


/*
 * Sort smSyncQ onto readyQ
 */

void
smsync_to_readyq( struct vd *vdp, int flushFlag )
{
    ioDescT *iop;
    extern u_int smsync_period;
    u_int period = smsync_period;
    u_int from;
    u_int to;
    u_int cnt;
    u_int j;
    int qlen, ioQ;
    ioDescHdrT *smsyncq;
    tempQMarkerT *tempQ_marker = NULL;

    /* If the readyLazyQ is already above the threshold, move to
     * consolQ to avoid overhead of sorting onto a long list.
     */
    if ( vdp->readyLazyQ.ioQLen >= vdp->readyLazyQ.lenLimit ) {
        if ( ready_to_consolq( vdp ) ) {
            startiocalls[5]++;
            bs_startio( vdp, flushFlag );
        }
    }
    if ( !current_processor()->first_quantum ) {
        thread_t th = current_thread();
        thread_preempt(th, FALSE);
    }

    from = vdp->syncQIndx;          /* next queue to be processed */

    /*
     * IO_FLUSH:
     *   Flush all the smoothsync queues.
     *   Used in bs_bflush().
     *
     * otherwise (IO_SOMEFLUSH):
     *   Sweep from the oldest queue up to the current queue.
     *   Used in bs_q_lazy(), msfs_smoothsync().
     */
    if ( flushFlag == IO_FLUSH ) {
        cnt = SMSYNC_NQS;
        SMSYNC_DBG( SMSYNC_DBG_OP, 
                    aprintf ( "smsync_rdyq:   all queues  vd 0x%lx\n", vdp ));
    }
    else {
        /* 
         * Avoid the cost of the division operation for the
         * common cases of smsync_period value of 1, 2, or 4
         * (i.e. smsync_age value of 15, 30, 60 seconds).
         */
        switch ( period ) {
        case 0:     j = 0;
                    break;
        case 1:     j = sched_tick;
                    break;
        case 2:     j = sched_tick >> 1;
                    break;
        case 4:     j = sched_tick >> 2;
                    break;
        default:    j = sched_tick/period;
        }
        to = (j+1)%SMSYNC_NQS;
        if ( from <= to )
            cnt = to - from;
        else
            cnt = SMSYNC_NQS - (from-to);
        SMSYNC_DBG( SMSYNC_DBG_OP, 
            aprintf ( "smsync_rdyq:   %d queue(s) starting at %d  vd 0x%lx\n", cnt, from, vdp ));
    }

    for ( ; cnt--; from = (from+1)%SMSYNC_NQS ) {

        smsyncq = get_smsyncq(vdp, from, &ioQ);
        mutex_lock( &smsyncq->ioQLock );
        iop = smsyncq->fwd;
        qlen = smsyncq->ioQLen;

        /* Update this now in case we drop the vdIoLock inside
         * sort_onto_readyq() or bs_startio().  Any racing thread
         * will know to start on the next queue.
         */
        mutex_lock( &vdp->vdIoLock );
        vdp->syncQIndx = (from+1)%SMSYNC_NQS;
        mutex_unlock( &vdp->vdIoLock );

        /* Need special case help for sort_onto_readyQ():
         * If we have a very long list we need to sort it using
         * a tempQ and that requires us to malloc a marker struct.
         * Dropping the lock to do the malloc can not be done after
         * we make the queues inconsistent by removing the entries
         * below while leaving them marked as on a smsyncQ.
         * Here we process what we found if we can get the struct
         * without dropping our lock, otherwise we reset the chain
         * head and length in case it changed.
         */
        if (!tempQ_marker && qlen >= Advfs_sort_factor) {
            tempQ_marker = (tempQMarkerT *)
            ms_malloc_no_wait(sizeof(tempQMarkerT));
            if (!tempQ_marker) {
                mutex_unlock(&smsyncq->ioQLock);
                tempQ_marker = (tempQMarkerT *)
                    ms_malloc(sizeof(tempQMarkerT));
                mutex_lock(&smsyncq->ioQLock);
                iop = smsyncq->fwd;
                qlen = smsyncq->ioQLen;
            }
        }

        if ( iop != ((ioDescT *)smsyncq) ) {

            MS_SMP_ASSERT( qlen != 0 );

            /* Remove entries from the smooth sync queue; but keep 
             * the remaining list as a doubly-linked list.
             */
            iop->bwd = smsyncq->bwd;
            iop->bwd->fwd = iop;

            smsyncq->fwd = 
            smsyncq->bwd = (ioDescT *)smsyncq;
            smsyncq->ioQLen = 0;

            /* Move the list to the readyQ. The smoothsync queue lock will
             * be dropped inside sort_onto_readyq().
             */
            sort_onto_readyq( vdp, iop, qlen,
                              &smsyncq->ioQLock, tempQ_marker );

#ifdef ADVFS_SMP_ASSERT
            HISTOGRAM_UPDATE( Sm2RdySz, qlen );
#endif
        } else {
            MS_SMP_ASSERT( qlen == 0 );
            mutex_unlock( &smsyncq->ioQLock );
        }

        /* If the readyLazyQ goes above the threshold, move some off. */
        if ( vdp->readyLazyQ.ioQLen >= vdp->readyLazyQ.lenLimit ) {
            if ( ready_to_consolq( vdp ) ) {
                startiocalls[5]++;
                bs_startio( vdp, flushFlag );
            }
        }
    }

    if (tempQ_marker)
        ms_free(tempQ_marker);

    /* If we flushed all the smoothsync queues, check to see if we 
     * need to give up the processor for other tasks.
     */
    if ( flushFlag == IO_FLUSH && !current_processor()->first_quantum ) {
       thread_t th = current_thread();
       thread_preempt(th, FALSE);
    }
}

#ifdef ADVFS_IODH_TRACE

void
iodh_trace( ioDescHdrT *ioDH,
            uint16T   module,
            uint16T   line,
            void      *value)
{
    register ioDHTraceElmT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;

    simple_lock(&TraceLock);

    ioDH->trace_ptr = (ioDH->trace_ptr + 1) % IODH_TRACE_HISTORY;
    te = &ioDH->trace_buf[ioDH->trace_ptr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val = value;

    simple_unlock(&TraceLock);
}

#endif
