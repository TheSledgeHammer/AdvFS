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
 * @(#)$RCSfile: bs_buf.h,v $ $Revision: 1.1.132.3 $ (DEC) $Date: 2005/03/04 19:58:03 $
 */

#ifndef _BS_BUF_H_
#define _BS_BUF_H_

/* ifdef'd because sas/sys_advfs.c compilation has no vm_page.h.*/
#ifdef KERNEL
#include <vm/vm_page.h>
#else
typedef void * vm_page_t;  /* so bsBuf stays the same size */
#endif /* KERNEL */

#ifdef ADVFS_BSBUF_TRACE

#define BSBUF_TRACE_HISTORY 100

typedef struct {
  uint32T       seq;
  uint16T       mod;
  uint16T       ln;
  struct thread *thd;
  void          *val;
} bsbufTraceElmtT;

#endif /* ADVFS_BSBUF_TRACE */

/* This macro sets the upper 28 bits with the line number, and the lower
 * 36 bits with the thread id.
 */
#define SET_LINE_AND_THREAD(ln) \
    ((unsigned long)(ln) << 36) +  \
    ((unsigned long)current_thread() & 0xfffffffff)

/*
 * rangeFlushT - describes a range flush
 */
typedef struct rangeFlush {
    unsigned long firstPage;           /* First page in range (static)        */
    unsigned long lastPage;            /* Last page in range (static)         */
    mutexT rangeFlushLock;             /* Guards outstandingIoCount field     */
    unsigned long outstandingIoCount;  /* # of I/O's still to be completed    */
} rangeFlushT;

/*
 * rangeFlushLinkT - links a bsBuf to a rangeFlushT
 * 
 * A single bsBuf could be part of multiple range flushes.
 * Also, a range flush could encompass multiple bsBufs.
 * All fields in the rangeFlushLink are protected by bsBuf.bufLock.
 */
typedef struct rangeFlushLink {
    struct rangeFlushLink *rflFwd;     /* Next rangeFlushLinkT for this bsBuf */
    rangeFlushT *rfp;                  /* The rangeFlushT itself              */
} rangeFlushLinkT;

/*
 * Bitfile buffer descriptor
 */
struct bsBuf {
    struct bsBuf *lsnFwd;         /* lsn list; protected by domainT.lsnLock */
    struct bsBuf *lsnBwd;         
    struct bsBuf *accFwd;         /* bfAccess clean and dirty page list;    */
    struct bsBuf *accBwd;         /*     protected by bfAccessT.bfIoLock    */
    rangeFlushLinkT *rflList;     /* List of connectors to rangeFlushT's    */
                                  /* that include this bsBuf in their range */
                                  /* rflList is protected by bsBuf.bufLock  */
    mutexT bufLock;

    /* Next fields protected by bsBuf.bufLock */
    bufLkT lock;                  /* See state flags below for values */
    long ln;                      /* dbg: line # high 28 bits; tid in low 36*/
    int writeRef;                 /* # of refs on this buffer for writing */
    u_int sync_stamp;             /* used for smoothsync operation */
    struct actRange *actRangep;   /* Active range containing this buffer. */

    /* note this field may be invalid when the vm_page isn't held or busy */
    vm_page_t vmpage;             /* pointer to vm page struct for UBC page */

    u_int bufDebug;               /* see flags and note below - no locking */

    u_int bufMagic;               /* magic number: structure validation */
    u_long accListSeq;            /* bfap list history. protection = bfIoLock */

    logRecAddrT origLogRec;       /* lsn of oldest log record for which  */
                                  /* this buffer contains modifications. */
    logRecAddrT currentLogRec;    /* lsn of log record for most recent   */
                                  /* modification to this buffer.        */
    lsnT flushSeq;                /* sequence number for flushing */

    int directIO;                 /* This buf is being used for directIO */
    struct proc *procp;           /* Proc pointer for directIO */
    struct buf *aio_bp;           /* Asynchronous IO buffer for directIO. */

    u_long bfPgNum;               /* File's page offset. */
    struct bfAccess* bfAccess;    /* to get to disk mapping info */

    int ubc_flags;                /* UBC flags valid during IO request only */
    int unused;                   /* unused for alignment */

    /* Changed under protection of state == IO_TRANS  */
    statusT result;               /* I/O completion status */
    short metaCheck;              /* metadata integrity check on io done */
    short ioCount;                /* number of outstanding I/O's */
    ioListT ioList;               /* buffer's block map info */
    ioDescT ioDesc;               /* if buf has one ioDesc, this is it */

#ifdef ADVFS_SMP_ASSERT
    vm_page_t last_vmpage;        /* previous exchanged vm page struct */
    long busyLn;                  /* Set when advfs_page_get(busy) is called */
    long ioqLn;                   /* cleared(-1) on advfs_page_get(busy) */
                                  /* and set at io completion time */
#endif
#ifdef ADVFS_BSBUF_TRACE
    int trace_ptr;
    bsbufTraceElmtT trace_buf[BSBUF_TRACE_HISTORY];
#endif
};

#ifdef ADVFS_BSBUF_TRACE
void
bsbuf_trace( struct bsBuf  *bp,
             uint16T       module,
             uint16T       line,
             void          *value);

#define BSBUF_TRACE( bp, n1 ) \
    bsbuf_trace((bp), (uint16T)ADVFS_MODULE, (uint16T)__LINE__, (void *)(n1))
#else
#define BSBUF_TRACE( bp, n1 )
#endif

#define BUFIODESC 1     /* How many ioDesc bsBuf contains */

/* flags for bs_startio */
#define IO_NOFLUSH      1
#define IO_FLUSH        2
#define IO_SOMEFLUSH    4

/* flags for bs_bfdmn_flush_all */
#define NORMALFLUSH     0       /* must have same value as IO_NOFLUSH */
#define FORCEFLUSH      1

/*
 * Use this to save some space when
 * we have a buffer list.  Obviously, this and
 * the bsBuf struct must match.
 */
struct bsBufHdr {
    struct bsBuf *lsnFwd;       /* doubly linked lsn list */
    struct bsBuf *lsnBwd;
    struct bsBuf *accFwd;       /* doubly linked off bfAccess */
    struct bsBuf *accBwd;
    int length;                 /* length of queue */
};

/*
 * state bits; values for bsBuf.lock.state
 *
 * The FLUSH and IO_TRANS bits are tied to each other.  If a thread must
 * set both bits, it must first set the FLUSH bit and then the IO_TRANS
 * bit.
 *
 * The REMOVE_FROM_IOQ bit is also a modifier of IO_TRANS.  It is set when
 * a thread intends to remove the ioDesc structs for a certain buffer from
 * the IO queues.  This allows the IO subsystem to avoid moving these ioDesc
 * structs to the device queue, which would force the thread to wait for 
 * BUSY to clear.  Note that buffers with only IO_TRANS set may have their
 * ioDesc structs moved onto the device queue since some threads set this
 * when moving descriptors onto the queues, and either do not clear the bit
 * or wait long enough to clear it that the descriptor may have made it to
 * the device queue before IO_TRANS is cleared.
 */

#define DIRTY           0x01
#define BUSY            0x02   /* I/O in progress */    
#define KEEPDIRTY       0x04   /* page remains dirty after write completes */
#define READING         0x40   /* what kind of I/O in progress */
#define WRITING         0x80   /* what kind of I/O in progress */
#define IO_TRANS        0x400  /* state when moving between I/O queues */   
#define IO_COUNTED      0x800  /* buffer has been counted for i/o stats */
#define RAWRW           0x1000  /* this I/O bypasses buffer cache */
#define UPDATE_LSN      0x8000  /* update highest flushed lsn */
#define LOG_PAGE        0x10000 /* this buffer belongs to the log */
#define DIO_REMOVING    0x20000 /* Directio is in process of removing
                                    this buffer from the AdvFS cache. */
#define GETPAGE_WRITE   0x40000 /* Write request originates from getpage. */
#define UNMAPPED        0x80000 /* zeroed UBC page represents sparse hole.*/
#define ACC_DIRTY       0x1000000 /* this buffer is on the access dirty list */
#define THROTTLE        0x2000000 /* Migrate I/O throttle on this buf */
#define REMOVE_FROM_IOQ 0x10000000 /* removing from IO queues */
#define FLUSH           0x20000000
#define COPY            0x40000000
#define REMAP           0x80000000 /* The REMAP flag indicates that this file
                                      is being migrated.  Furthermore, the
                                      bsBuf with this flag set has had its
                                      ioDesc remapped so that it points to
                                      only the destination extent map of the
                                      migration.  The migrate code sets this
                                      when copying the file's contents to the
                                      destination extent map.  If the REMAP
                                      flag is set and another (non-migrate)
                                      thread pins the page, modifies it, and
                                      unpins it, the unpin code (bs_unpinpg)
                                      will realize that the bsBuf needs to
                                      be remapped once again so that its
                                      ioDesc(s) will point to both the original
                                      extent map for the file and also the
                                      migration destination extent map.  This
                                      is necessary in case the migration fails.
                                      If the migration fails, the destination
                                      storage will be released and the file 
                                      will revert to using only the original
                                      extent map.  If the unpin code did not
                                      remap the bsBuf and later the migration
                                      failed, updates would be lost. */


/* debug state bit values for bsBuf.bufDebug
 *
 * NOTE:  bsBuf.bufDebug isn't being modified under a lock in all cases.
 * This field is currently for debug only so word-tear would not be fatal,
 * just misleading.  With the current paths there should not be a problem.
 */
#define BSBUF_WAITQ         0x01  /* buf was on waitq (smoothsync debug) */
#define BSBUF_DIRECTIO      0x02  /* buf allocated for user direct io */
#define BSBUF_RAWPAGEIO     0x04  /* buf allocated for advfs raw page io */
#define BSBUF_CHECKED       0x08  /* metadata page validated on io complete */
#define BSBUF_REPLICATED    0x10  /* buf was original on vm page replication */
#define BSBUF_REPLICA       0x20  /* buf is a replicated vm page */


/* values for bsBuf.metaCheck field
 * determines how bs_io_metacheck() will validate a (metadata) page.
 */
#define BSBUF_CHK_NONE      0  /* no validation (normal user data) */
#define BSBUF_CHK_BMTPG     1  /* execute check_BMT_pg */


typedef enum { Ref=1, Deref=2, Pin=4, Unpin=8, DevRead=0x10, 
    DevWrite=0x20 }TraceActionT;

/*
 * List manipulation macros
 */

#define IODESC_CLR( bp, i ) \
{ \
    bp->ioList.ioDesc[i].fwd = bp->ioList.ioDesc[i].bwd = NULL; \
    bp->ioList.ioDesc[i].ioQ = NONE; \
    bp->ioList.ioDesc[i].numBlks = 0; \
    bp->ioList.ioDesc[i].targetAddr = 0; \
    bp->ioList.ioDesc[i].bsBuf = bp; \
    bp->ioList.ioDesc[i].ioCount = 0; \
    bp->ioList.ioDesc[i].consolidated = 0; \
    bp->ioList.ioDesc[i].totalBlks = 0; \
    bp->ioList.ioDesc[i].desCnt = 0; \
    bp->ioList.ioDesc[i].data_never_written = 0; \
}

#define ADD_DIRTYACCESSLIST( bp, seize_bfiolock ) \
{ \
    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex)); \
    if (seize_bfiolock) \
        mutex_lock(&bp->bfAccess->bfIoLock); \
    else \
        MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bfAccess->bfIoLock.mutex)); \
    bp->accListSeq++; \
    MS_SMP_ASSERT(!(bp->lock.state & ACC_DIRTY)); \
    bp->lock.state |= ACC_DIRTY; \
    bp->accFwd = (struct bsBuf *)&bp->bfAccess->dirtyBufList; \
    bp->accBwd = bp->bfAccess->dirtyBufList.accBwd; \
    bp->bfAccess->dirtyBufList.accBwd->accFwd = bp; \
    bp->bfAccess->dirtyBufList.accBwd = bp; \
    bp->bfAccess->dirtyBufList.length++; \
    if (seize_bfiolock) \
        mutex_unlock(&bp->bfAccess->bfIoLock); \
}

#define RM_ACCESSLIST( bp, seize_bfiolock ) \
{ \
    MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bufLock.mutex)); \
    if (seize_bfiolock) \
        mutex_lock(&bp->bfAccess->bfIoLock); \
    else \
        MS_SMP_ASSERT(SLOCK_HOLDER(&bp->bfAccess->bfIoLock.mutex)); \
    MS_SMP_ASSERT(bp->lock.state & ACC_DIRTY); \
    bp->bfAccess->dirtyBufList.length--; \
    MS_SMP_ASSERT(bp->bfAccess->dirtyBufList.length >= 0); \
    bp->lock.state &= ~ACC_DIRTY; \
    bp->accListSeq++; \
    bp->accFwd->accBwd = bp->accBwd; \
    bp->accBwd->accFwd = bp->accFwd; \
    bp->accFwd = bp->accBwd = NULL; \
    if (seize_bfiolock) \
        mutex_unlock(&bp->bfAccess->bfIoLock); \
}

#ifdef ADVFS_DEBUG

#define MS_VERIFY_IOQUEUE_INTEGRITY(qhdr,callerLocked) \
{ \
    if ( !(callerLocked) ) { \
        mutex_lock(&(qhdr)->ioQLock); \
    } \
    check_queue( qhdr ); \
    if ( !(callerLocked) ) { \
        mutex_unlock(&(qhdr)->ioQLock); \
    } \
}

#else 

#define MS_VERIFY_IOQUEUE_INTEGRITY(qhdr,callerLocked)

#endif /* ADVFS_DEBUG */

#define PGREF_EQL(x1, x2) \
    ( (x1) == (x2))

#ifndef ADVFS_DEBUG
#define state_block( bp, w ) _state_block( bp, w );

void
_state_block( 
    struct bsBuf *bp,   /* in - buffer on which to block */
    int *wait           /* in/out - waited previously? */
    );

#else
#define state_block( bp, w ) _state_block( bp, w, __LINE__, __FILE__ );

void
_state_block( 
    struct bsBuf *bp,   /* in - buffer on which to block */
    int *wait,          /* in/out - waited previously? */
    int ln,
    char *fn
    );

#endif  /* ADVFS_DEBUG */

statusT
bs_pinpg_ftx( bfPageRefHT *bfPageRefH,       /* out */
              void **bfPageAddr,             /* out */
              struct bfAccess* bfap,         /* in */
              unsigned long bsPage,          /* in */
              bfPageRefHintT refHint,        /* in */
              ftxHT ftxH );                  /* in */

#ifdef KERNEL
statusT
bs_refpg_get( 
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint,        /* in - hint to do read ahead */
         vm_page_t *pp,                 /* out - vm_page struct pointer */
         vm_policy_t policy,            /* in - vm page locality */
         vm_offset_t offset,            /* in - ubc hint */
         vm_size_t len,                 /* in - ubc hint */
         int ubc_flags                  /* in - ubc hint */
         );

statusT
bs_refpg_direct(void *bfPageAddr,              /* in */
                int number_to_read,            /* in */
                struct bfAccess *bfap,         /* in */
                unsigned long bsBlock,         /* in */
                int seg_flag,                  /* in */
                struct buf *aio_bp,            /* in */
                struct actRange *arp,          /* in */
                int *number_read,              /* out */
                int *aio_flag);                /* out */

statusT
bs_pinpg_get(
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in - bf page number */
         bfPageRefHintT refHint,        /* in - hint to do read ahead */
         vm_page_t *pp,                 /* out - vm_page struct pointer */
         vm_policy_t policy,            /* in - vm page locality */
         vm_offset_t offset,            /* in - ubc hint */
         vm_size_t len,                 /* in - ubc hint */
         int ubc_flags                  /* in - ubc hint */
         );

statusT 
bs_pinpg_put(vm_page_t plp,                    /* in */
             int plcnt,                        /* in */
             int ubc_flags);                   /* in */

statusT
bs_pinpg_direct(void *bfPageAddr,              /* in */
                int number_to_write,           /* in */
                struct bfAccess *bfap,         /* in */
                unsigned long bsBlock,         /* in */
                int seg_flag,                  /* in */
                struct buf *aio_bp,            /* in */
                struct actRange *arp,          /* in */
                int *number_written,           /* out */
                int *aio_flag,                 /* out */
                int cowingDone,                /* in */
                struct ucred *cred);           /* in */

#endif /* KERNEL */

void
set_state (
           struct bsBuf *bp,  /* in */
           int ln,  /* in */
           uint32T state  /* in */
           );

void
wait_state (
            struct bsBuf *bp,  /* in */
            uint32T state  /* in */
            );

void
clear_state (
             struct bsBuf *bp,  /* in */
             uint32T state  /* in */
             );

void
bs_wakeup_flush_threads(struct bsBuf *bp,   /* in - Buffer being released */
                       int recordDiskError  /* in - TRUE if got an I/O error */
                      );

struct bsBuf * bs_get_bsbuf(int rad_id,  /* in - preferred rad for struct */
                            int wait);   /* in -TRUE/FALSE wait on malloc.*/

void
bs_free_bsbuf( struct bsBuf *bp ); /*in */

void
bs_io_metacheck( struct bsBuf *bp ); /*in */

/*
 * advfs_page_get - front-end for ubc_fs_page_get()
 * caller must include vm/vm_ubc.h
 *
 * The flags field is divided into 2 16-bit masks, 0xff for ubc definitions
 * and 0xff00 for filesystem specific flags that are ignored by ubc so the
 * wrapper function can pass through the same flags.
 */
int
advfs_page_get(struct bsBuf *bp, /* in - buffer to get current vm_page struct */
               int flags         /* in - what to do - hold, busy, etc */
              );

#define ADVFS_GET_NOCACHE    0x100  /* page not in cache is ok for this call */

#endif /* _BS_BUF_H_ */
