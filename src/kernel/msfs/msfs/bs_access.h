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
 * @(#)$RCSfile: bs_access.h,v $ $Revision: 1.1.237.4 $ (DEC) $Date: 2005/03/04 19:57:59 $
 */

#ifndef _ACCESS_H_
#define _ACCESS_H_

#ifdef KERNEL /* need this so user mode utils (msfsck.c) can compile - ps */
#include <vm/vm_ubc.h>
#endif

#include <sys/vnode.h>
#include <msfs/ms_assert.h>
#ifdef KERNEL
#include <kern/e_dyn_hash.h>
#endif

#define NEW_ACCESS

/*
 * ACCESS_TRACE - used to trace per access activity
 */
#ifdef  ADVFS_ACCESS_TRACE

#define ACCESS_TRACE_HISTORY 200

typedef struct {
    uint32T       seq;
    uint16T       mod;
    uint16T       ln;
    struct thread *thd;
    void          *val;
} AccessTraceElmtT;

#endif /* ADVFS_ACCESS_TRACE */

#define ADVFS_MIN_FREE_ACCESS MIN_FREE_VNODES
#define ADVFSMAXFREEACCESSPERCENT 80
extern ulong ncache_valid_time;
#define BFAP_VALID_TIME ncache_valid_time

/* The following structure is used to cache the file stats from the
 * fsContext structure with the access structure when the vnode is
 * reclaimed and the file stats cannot be flushed to disk.  Before the
 * context struct is deallocated, the flags and dir_stats are copied
 * here and later flushed.
 */
typedef struct {
    long op_flags;                /* flags for state of flushing operation */
    long fs_flag;                 /* file flags - what needs to be flushed */
    struct fs_stat dir_stats;     /* file stats */
    int dirty_alloc;              /* set if stats from an allocating write */
                                  /* are not on disk */
} saved_statsT;

/* Values for saved_statsT.op_flags */
#define SS_FLUSH_IN_PROGRESS       0x0001  /* flushing saved_stats to disk */
#define SS_STATS_COPIED_TO_CONTEXT 0x0002  /* saved_stats have been copied */

extern mutexT BfAccessFreeLock;  /* guards access free & closed lists */

struct bfSet;

/* Dynamic Hashtable Macros for accessing the BfAccessHashTbl */

#define BS_BFAH_INITIAL_SIZE 128
#define BS_BFAH_HASH_CHAIN_LENGTH 30
#define BS_BFAH_ELEMENTS_TO_BUCKETS 5
#define BS_BFAH_USECS_BETWEEN_SPLITS 1000000

#define BS_BFAH_GET_KEY( _s, _t ) \
   ((BFSET_GET_HASH_INPUT(_s)) * (BS_BFTAG_IDX(_t)) + (BS_BFTAG_SEQ(_t)))

#define BS_BFAH_LOCK( _key, _cnt) \
   ( (bfAccessT *) dyn_hash_obtain_chain( BsAccessHashTbl, _key, _cnt) )

#define BS_BFAH_UNLOCK( _key) \
   ( dyn_hash_release_chain( BsAccessHashTbl, _key) )

#define BS_BFAH_REMOVE( _bfap, _laction)                \
{                                                       \
    dyn_hash_remove( BsAccessHashTbl, _bfap, _laction); \
}

#define BS_BFAH_INSERT( _bfap, _laction)                \
{                                                       \
    MS_SMP_ASSERT(SLOCK_HOLDER(&_bfap->bfaLock.mutex)); \
    dyn_hash_insert( BsAccessHashTbl, _bfap, _laction); \
}

/*
 * flushWaiterT - describes a flush synchronization structure including
 * the number of waiters and the highest LSN of the buffers being flushed.
 */
typedef struct flushWaiter {
    struct flushWaiter *fwd;      /* FlushWaiter queue forward link */
    struct flushWaiter *bwd;      /* FlushWaiter queue backward link */
    lsnT waitLsn;                 /* Highest LSN of buffer IO to wait for */
    ulong cnt;                    /* Number of waiters on this LSN */
} flushWaiterT;


/* actRange structure describes a range of blocks in a file that are
 * in use for a variety of reasons, including directIO, migration, and
 * truncation.  This is used as a synchronization mechanism to prevent
 * different threads from manipulating the same range simultaneously.  
 */
typedef struct actRange {
    struct actRange *arFwd;       /* active: pointer to next higher range */
    struct actRange *arBwd;       /* active: pointer to next lower  range */
    struct actRange *arWaitFwd;   /* waiter: pointer to next waiter */
    struct actRange *arWaitBwd;   /* waiter: pointer to previous waiter */
    ulong  arStartBlock;          /* first 512-byte block in this range. */
    ulong  arEndBlock;            /* last  512-byte block in this range.  */
    ulong  arIosOutstanding;      /* # I/Os still outstanding in range */
    ulong  arDebug;               /* line and thread id of last modifier */
    enum {
        AR_UNINITIALIZED = 0,
        AR_DIRECTIO,              /* range in use by direct IO */
        AR_MIGRATE,               /* range in use by migration */
        AR_TRUNCATE,              /* range in use by truncation */
        AR_INVALIDATE,            /* range in use by flush_and_invalidate */
        AR_COW                    /* range in use by cow path (moo!) */
    } arState;
    int   arWaiters;              /* # thds waiting for this range to go away */
} actRangeT;

/* This is the header for active ranges in the access structure.  This
 * header contains two lists of ranges: the active list which includes
 * mutually-exclusive ranges associated with threads that are currently
 * working on their ranges.  These threads should be running or scheduled
 * to run.  The other list is the wait list; these are ranges requested 
 * by threads that currently conflict with ranges on the active list,
 * OR are requests that came in after previous ranges were already
 * put onto the wait list.  This is a fairness algorithm that prevents
 * requests for large ranges from being starved by many small requests.
 * The threads associated with these ranges are typically sleeping.
 * Ranges are removed from the wait list in fifo order (removed from head)
 * and put onto the active list in order (ascending) by block number.
 */
typedef struct actRangeHdr {
    struct actRange *arFwd;       /* active: pointer to next higher range */
    struct actRange *arBwd;       /* active: pointer to next lower  range */
    struct actRange *arWaitFwd;   /* waiters: pointer to next waiter (fifo) */
    struct actRange *arWaitBwd;   /* waiters: pointer to prev waiter (fifo) */
    uint   arCount;               /* # structs on active chain */
    uint   arMaxLen;              /* stats: longest active chain has been */
    uint   arWaitCount;           /* # structs on wait chain */
    uint   arWaitMaxLen;          /* stats: longest wait chain has been */
} actRangeHdrT;

typedef struct bfAccess {
#ifdef KERNEL
    dyn_hashlinks_w_keyT hashlinks; /* dynamic hashtable links */
#endif /*KERNEL*/
                                /* guard next 3 with BfAccessFreeLock */
    struct bfAccess *freeFwd;   /* freelist or closed list */
    struct bfAccess *freeBwd;
    int onFreeList;             /* 1 = on free list, -1 on closed list */
    uint_t accMagic;            /* magic number: structure validation */
                                /* guard next two with bfSetT.accessChainLock */
    struct bfAccess *setFwd;    /* fileset chaining */
    struct bfAccess *setBwd;
    mutexT bfaLock;             /* lock for many of fields in this struct */
                                /* next 3 guarded by bfaLock */
    int accessCnt;              /* number of bitfile accesses */
    int refCnt;                 /* number of access structure references */
    int mmapCnt;                /* number of mmap() calls open on file */
    int mmapFlush;              /* mmapped file could need pages flushed */
    stateLkT stateLk;           /* state field */

    saved_statsT *saved_stats;  /* if non-null; points to struct with
                                 * stats that need to be flushed to disk;
                                 * this file no longer has an fsContext 
                                 * structure (it was deallocated when its
                                 * vnode was reclaimed.)
                                 */

    /* 
     * Following fields modifed in xxx_TRANS state or when
     * holding the bfaLock
     */
    struct vnode *bfVp;
    struct vm_ubc_object *bfObj;

    /*
     * Following fields are protected by bfIoLock
     */
    mutexT bfIoLock;            /* lock for I/O related fields in this struct */
    statusT dkResult;           /* Disk error field - used in flush */
    statusT miDkResult;         /* Disk error field - used in migrate flush */
    struct bsBufHdr dirtyBufList; /* List of modified buffers. */
    uint16T flushWait;          /* Number of threads waiting for an lsn */
    uint16T maxFlushWaiters;    /* Maximum flush waiter threads counter */
    lsnT hiFlushLsn;            /* Highest lsn written (to log) */
    lsnT hiWaitLsn;             /* Highest LSN waited for by file flush */
    lsnT nextFlushSeq;          /* Record seq numbers used */
    lsnT logWriteTargetLsn;     /* Log bitfile's target LSN to flush & sync to.*/
    struct {                    /* Queue of flush synchronizers */
      struct flushWaiter *head;
      struct flushWaiter *tail;
      ulong cnt;
    } flushWaiterQ;
    unsigned long raHitPage;    /* When hit on this page, read ahead */
    unsigned long raStartPage;  /* When read ahead, start read with this page */
    unsigned long seqWritePgCnt; /* Track # of sequentially written pages */
    lock_data_t trunc_xfer_lk;  /* prevent clone reads during orig truncation */
    lock_data_t cow_lk;

    lock_data_t clu_clonextnt_lk; /* clusters only lock protecting clone xtnts */
    unsigned int cloneXtntsRetrieved; /* Indicates cluster client obtained xtnts */
                                      /* Protected by either clonextnt_lk or */
                                      /* the cow_lk */
    struct bfAccess *nextCloneAccp; /* link to next clone's access struct */
    struct bfAccess *origAccp;  /* link to orig bitfile's access struct */
    unsigned long cowPgCount;   /* Protected by the migTruncLk; number of */
                                /* times bs_cow_pg() has added storage */

    /* the following are valid only if mapped == 1 */
    uint32T cloneId;            /* 0 ==> orig; "> 0" ==> clone */
    uint32T cloneCnt;           /* set's clone cnt last time bf changed */
    uint32T maxClonePgs;        /* max pages in clone */
    bfDataSafetyT dataSafety;   /* bitfile's data safety attribute */

    /* flags */
    unsigned noClone;           /* flag true if bitfile has no clone */
    unsigned deleteWithClone;   /* true if bf should be deleted with clone */
    unsigned outOfSyncClone;    /* clone may not be accessed */
    unsigned trunc;             /* truncate bitfile on last bitfile close */

    fsFragStateT fragState;     /* the state of the file's frag */
    bfFragIdT fragId;           /* frag size; 1K, 2K ... 7K */
    uint32T fragPageOffset;


    /* following fields only modified in xxx_TRANS states */
    int bfPageSz;               /* bitfile page size */
    serviceClassT reqServices;  /* required service class */
    serviceClassT optServices;  /* optional service class */
    bfTagT tag;                 /* make a wild guess */
    bfStatesT bfState;          /* bitfile state */
    ftxIdT transitionId;        /* ftx id if CREATING/DELETING */
    /* end of fields only modified in xxx_TRANS states */

    off_t file_size;

    /* The bfSetp can not be modified while the strucuture is
     * in the hashtable */
    bfSetT *bfSetp;             /* ptr to bf set desc */

    struct domain* dmnP;     /* ptr of domain desc */

    ftxLkT mcellList_lk;
    ftxLkT xtntMap_lk;
    int mapped;                 /* flag indicates if bitfile is mapped-in */
    bsPageT nextPage;           /* 1 past high page allocated */
    int extendSize;             /* add'l extend size in blocks */

    bfMCIdT primMCId;           /* primary metadata cell id */
    vdIndexT primVdIndex;       /* vd of primary metadata cell */

    /* Analysis of the code reveals that the following locks
     * seem to be protecting the xtntmap in the following way:
     * 
     * trunc_xfere_lk - used for clone access
     * migTruncLk - Held across add, removing, migrating.
     * mcellList_lk - Protects the on-disk Mcells exclusively but also
     *                allows read access to the xtntMap.
     *
     * The following lock gives exclusive access to the xtntMap
     *
     * xtntMap_lk - Must be held when merging xtntmaps and across any call
     *              to imm_extent_xtnt_map.
     *
     * NOTE: The use of these appears to be fairly inconsistent and
     *       needs to be investigated.
     *
     */
    
    bsInMemXtntT xtnts;         /* extent descriptors */
    void *dirTruncp;            /* possible ptr to dtinfoT struct */

    struct bfAccess *real_bfap; /* the original bfap in the root fileset
                                   this one is shadowing */
#ifdef ADVFS_ACCESS_TRACE
    uint32T trace_ptr;          /* access trace buffer */
    AccessTraceElmtT trace_buf[ACCESS_TRACE_HISTORY];
#endif /* ADVFS_ACCESS_TRACE */

    void *idx_params;           /* Pointer to index file parameters */
    int  idxQuotaBlks;          /* Additional quota credits at dir removal */

    uint32T largest_pl_num;     /* guarded by mcellList_lk */

    /* Active Range structures are used to delineate 'active ranges' of
     * pages that are being modified by directIO or migrate.  This is
     * used to synchronize manipulation of a given file region without 
     * preventing other threads from simultaneously manipulating differing 
     * regions.
     */
    mutexT actRangeLock;                /* protects actRangeList chain */
    struct actRangeHdr actRangeList;    /* chain of actRange structs */
    long bfap_free_time;

    uint32T ssHotCnt[SS_HOT_TRACKING_DAYS]; /* vfast count of IO on hot files */
    uint32T ssMigrating; /* flag to indicate vfast is migrating this file */
    int badDirEntry;            /* set when file is a directroy and it has
                                     an invalid directory entry */
    /* following two migrate Throttle fields are protected by bfIoLock */
    ulong migPagesPending;  /*unpinned migrates to flushQ but not written yet */
    cvT migWait;            /* will sleep until queue reduced to this size */
} bfAccessT;

#ifdef    ADVFS_ACCESS_TRACE

#define ACCESS_TRACE( bfap, n1 ) \
    access_trace((bfap), (uint16T)ADVFS_MODULE, (uint16T)__LINE__, (void*)(n1))

void
access_trace(
             bfAccessT *bfap,
             uint16T   module,
             uint16T   line,
             void      *value
             );

#else  /* ADVFS_ACCESS_TRACE */

#define ACCESS_TRACE( bfap, n1 )

#endif /* ADVFS_ACCESS_TRACE */

#ifdef ADVFS_TAG_TRACE

void tag_trace(uint, uint, uint16T, uint16T, void*);

#define TAG_TRACE(f, s, v) \
    tag_trace((uint)(f), (uint)(s), \
              (uint16T)ADVFS_MODULE, (uint16T)__LINE__, (void*)(v))

#else /* ADVFS_TAG_TRACE */

#define TAG_TRACE(f, s, v)

#endif /* ADVFS_TAG_TRACE */


/* Test the validity of a page number in a file. */
/* "Returns" E_BAD_PAGE_RANGE if the page is beyond the end of the file. */
/* Else "returns" EOK. */
#define TEST_PAGE(pg, bfap) \
    ( (pg) >= (bfap)->nextPage ? E_BAD_PAGE_RANGE : EOK )

/*
 * This struct saves some space when we
 * need a bfAccess compatible list header.
 * It must match perfectly with the start
 * of bfAccess.
 */

struct bfAccessHdr {
#ifdef KERNEL
    dyn_hashlinks_w_keyT hashlinks; /* dynamic hashtable links */
#endif /*KERNEL*/
    struct bfAccess *freeFwd;
    struct bfAccess *freeBwd;
    int len;
    int saved_stats_len;
#ifdef ADVFS_DEBUG
    int max;
    int min;
#endif
};


/*
 * bs_map_bf() options flags.
 */
#define BS_MAP_DEF     0
#define BS_REMAP       2
#define BS_FAILOVER    4
#define BS_ON_DDL      8

statusT
bs_map_bf(
          bfAccessT* bfAp,         /* in/out - ptr to bitfile's access struct */
          uint32T options,         /* in - options flags (see ) */
          struct mount *mp         /* in - mount point */
          );

int
bs_have_clone(
              bfTagT tag,              /* in - bitfile tag */
              bfSetT *cloneSetp,       /* in - clone bitfile set desc ptr */
              ftxHT ftxH               /* in - ftx handle */
              );

statusT
new_clone_mcell(
                bfMCIdT* bfMCIdp,        /* out - ptr to mcell id */
                domainT* dmnp,           /* in - domain ptr */
                ftxHT parFtx,            /* in - parent ftx */
                vdIndexT* vdIndex,       /* out - new vd index */
                bsBfAttrT* bfAttrp,      /* in - bitfile attributes ptr */
                bfSetT *bfSetp,          /* in - bitfile's bf set desc ptr */
                bfTagT newtag,           /* in - tag of new bitfile */
                bsInMemXtntT *oxtntp     /* in - ptr to orig extent map */
                );

/* define values for access_int "options" arg */

#define BF_OP_IGNORE_DEL  0x1
#define BF_OP_GET_VNODE   0x2
#define BF_OP_HAVE_VNODE  0x4
#define BF_OP_INMEM_ONLY  0x8
#define BF_OP_FIND_ON_DDL 0x10

#define NULLMT NULL

statusT
bs_access_one(
              bfAccessT **bfap,    /* out - access structure pointer */
              bfTagT tag,          /* in - tag of bf to access */
              bfSetT *bfSetp,      /* in - BF-set descriptor pointer */
              ftxHT ftxH,          /* in - ftx handle */
              uint32T options,     /* in - options flags */
              struct mount *mp,    /* in - fs mount queue */
              struct vnode **fsvp, /* out - vnode allocated */
              bfAccessT *origBfap  /* in - Orig access (clone open) */
              );

/*
 * bs_close options
 */

#define MSFS_INACTIVE_CALL 0x1
#define MSFS_BFSET_DEL     0x2
#define MSFS_DO_VRELE      0x4
#define MSFS_SS_NOCALL     0x8 /* force vfast to not update vfast lists */

statusT
bs_close_one(
             bfAccessT *bfap,     /* in */
             int options,         /* in */
             ftxHT parentFtxH     /* in */
             );


void access_invalidate( bfSetT *bfSetp );

bfAccessT *
find_bfap(
          bfSetT *bfSetp,       /* in  - bitfile-set descriptor poitner */
          bfTagT tag,           /* in  - bitfile tag */
          int hold_hashlock,    /* in  - TRUE = return with HashLock held 
                                         FALSE = release HashLock on return */
          ulong *insert_count); /* out - hint indicating an insertion
                                         since last time chain was locked */

void
bs_invalidate_rsvd_access_struct (
                                  domainT *domain,      /* in */
                                  bfTagT bfTag,         /* in */
                                  bfAccessT *bfap       /* in */
                                  );

statusT
bs_reclaim_cfs_rsvd_vn (
                        bfAccessT *bfap       /* in */
                        );

void
free_acc_struct(
                bfAccessT* bfap /* in - pointer to access struct */
                );

extern struct bfAccessHdr FreeAcc;
extern struct bfAccessHdr ClosedAcc;

statusT
bmtr_get_rec_ptr(
    bfAccessT *bfap,            /* in - bf access struct ptr */
    ftxHT parentFtxH,           /* in - transaction handle */
    u_short rType,              /* in - type of record */
    u_short bSize,              /* in - size of buffer */
    int pinPg,                  /* in - 1 == pin pg, 0 == ref pg */
    void **bPtr,                /* out - ptr to buffer */
    rbfPgRefHT *pinPgH,         /* out - pg handle (if pinPg == 1) */
    bfPageRefHT *refPgH         /* out - pg handle (if pinPg == 0) */
    );

void bs_init_access_alloc_thread(void);
void bs_access_alloc_thread(void);
void insert_actRange_onto_list( bfAccessT *bfap,
                                actRangeT *arp,
                                struct fsContext *contextp  );
void remove_actRange_from_list( bfAccessT *bfap,
                                actRangeT *arp  );

statusT
limits_of_active_range(
    bfAccessT *bfap,
    bsPageT pg,
    bsPageT *beginpg,
    bsPageT *npgs
    );


/*
 * DEC_REFCNT - macro to decrement access struct refCnt and call
 * routine to add to free list.
 */

#define DEC_REFCNT( bfap ) \
{ \
    MS_SMP_ASSERT(SLOCK_HOLDER(&(bfap)->bfaLock.mutex)); \
    if ( --bfap->refCnt <= 0 ) \
        free_acc_struct( bfap ); \
}

/* Move access structure to the closed list; caller typically has already
 * locked the bfap->bfaLock.  This macro seizes the BfAccessFreeLock
 * while moving the struct onto the closed list.
 */
#define ADD_ACC_CLOSEDLIST( bfap ) \
{ \
    mutex_lock(&BfAccessFreeLock); \
    MS_SMP_ASSERT(bfap->onFreeList == 0); \
    MS_SMP_ASSERT(ClosedAcc.freeBwd); \
    bfap->freeBwd = ClosedAcc.freeBwd; \
    bfap->freeFwd = (bfAccessT *)&ClosedAcc; \
    ClosedAcc.freeBwd->freeFwd = bfap; \
    ClosedAcc.freeBwd = bfap; \
    bfap->onFreeList = -1; \
    ClosedAcc.len++; \
    if (bfap->saved_stats) { \
        ClosedAcc.saved_stats_len++; \
    } \
    ACCMAX(ClosedAcc); \
    mutex_unlock(&BfAccessFreeLock); \
}

/* ADD_ACC_FREELIST adds access structures to the free list.
 * ACC_INVALID access structures are added to the front of the list,
 * others are added to the back.
 * The bfap->bfaLock is typically held by the caller; BfAccessFreeLock
 * will be siezed while the freelist is manipulated.
 *
 * If we're not shutting down and either:
 * 
 *   * More than ADVFSMAXFREEACCESSPERCENT percent of the entire access 
 *     structure pool is on the free list and there are at least 
 *     2*AdvfsMinFreeAccess access structures on the free list 
 *
 *   -or- 
 *
 *   * More than MaxAccess access structures are currently allocated,
 *
 * send a request to the fs_cleanup_thread() to deallocate an 
 * access structure.
 * 
 * Note:  There is code very similar to some of this in init_access().  If
 *        this macro changes, consider whether the same change should
 *        be made to init_access().
 *
 *
 * NOTE: Due to NFS's penchant for keeping few files open, we are also
 *       checking AdvfsMinAccess which is a new hidden tunable due to be
 *       removed in a future release. If AdvfsMinAccess is not set it will
 *       default to zero which will allow the conditions for the following
 *       algorithm to execute as originally designed.  When set, we will
 *       keep many more access structs (and the cached name and file data)
 *       around.
 *
 */
#define ADD_ACC_FREELIST( bfap ) \
{ \
    clupThreadMsgT *msg;        \
    extern msgQHT CleanupMsgQH; \
    extern int advfs_shutting_down; \
                                \
    mutex_lock(&BfAccessFreeLock); \
    MS_SMP_ASSERT(bfap->onFreeList == 0); \
    MS_SMP_ASSERT(bfap->dirtyBufList.length == 0); \
    if ( bfap->stateLk.state == ACC_INVALID ) { \
        MS_SMP_ASSERT(FreeAcc.freeFwd); \
        if (FreeAcc.freeFwd != (bfAccessT *)&FreeAcc) { \
            bfap->bfap_free_time = FreeAcc.freeFwd->bfap_free_time; \
        } else { \
            bfap->bfap_free_time = sched_tick; \
        } \
        bfap->freeBwd = (bfAccessT *)&FreeAcc; \
        bfap->freeFwd = FreeAcc.freeFwd; \
        FreeAcc.freeFwd->freeBwd = bfap; \
        FreeAcc.freeFwd = bfap; \
    } else { \
        MS_SMP_ASSERT(FreeAcc.freeBwd); \
        bfap->freeBwd = FreeAcc.freeBwd; \
        bfap->freeFwd = (bfAccessT *)&FreeAcc; \
        FreeAcc.freeBwd->freeFwd = bfap; \
        FreeAcc.freeBwd = bfap; \
        bfap->bfap_free_time = sched_tick; \
    } \
    bfap->onFreeList = 1; \
    FreeAcc.len++; \
    ACCMAX(FreeAcc); \
    if (!advfs_shutting_down && \
        ((NumAccess > MaxAccess) || \
         ((NumAccess > AdvfsMinAccess)  && \
          (FreeAcc.len > 2*AdvfsMinFreeAccess) && \
         ((FreeAcc.len > (NumAccess * ADVFSMAXFREEACCESSPERCENT)/100) || \
          (FreeAcc.freeFwd->bfap_free_time <  \
                         (long)(sched_tick - BFAP_VALID_TIME)))))) { \
        msg = (clupThreadMsgT *)msgq_alloc_msg(CleanupMsgQH); \
        if (msg) { \
            msg->msgType = DEALLOCATE_BFAPS; \
            msgq_send_msg(CleanupMsgQH, msg); \
        } \
    } \
    mutex_unlock(&BfAccessFreeLock); \
}

/* This does the underlying work for the RM_ACC_LIST macros. Do not
 * call this macro from your code; call only the other ones. This one
 * does no locking or lock verification.
 */
#define RM_ACC_LIST_REAL_WORK( bfap ) \
{ \
    bfap->freeFwd->freeBwd = bfap->freeBwd; \
    bfap->freeBwd->freeFwd = bfap->freeFwd; \
    bfap->freeFwd = bfap->freeBwd = NULL; \
    if ( bfap->onFreeList == 1 ) { \
        MS_SMP_ASSERT(FreeAcc.len > 0); \
        FreeAcc.len--; \
        MS_SMP_ASSERT(bfap->dirtyBufList.length == 0); \
        ACCMIN(FreeAcc); \
    } else { \
        MS_SMP_ASSERT(ClosedAcc.len > 0); \
        ClosedAcc.len--; \
        if (bfap->saved_stats) { \
            ClosedAcc.saved_stats_len--; \
        } \
        ACCMIN(ClosedAcc); \
    } \
    bfap->onFreeList = 0; \
}

/* RM_ACC_LIST removes access structures from the free or closed list.
 * The caller typically holds bfap->bfaLock. BfAccessFreeLock is
 * seized while manipulating the free or closed lists.
 */
#define RM_ACC_LIST( bfap ) \
{ \
    mutex_lock(&BfAccessFreeLock); \
    MS_SMP_ASSERT(bfap->onFreeList == 1 || bfap->onFreeList == -1); \
    MS_SMP_ASSERT(bfap->freeFwd); \
    MS_SMP_ASSERT(bfap->freeBwd); \
    RM_ACC_LIST_REAL_WORK( bfap ); \
    mutex_unlock(&BfAccessFreeLock); \
}

/* RM_ACC_LIST_NOLOCK is like RM_ACC_LIST, except that the BfAccessFreeLock
 * must already be held by the caller instead of seizing and releasing it
 * internally.
 */
#define RM_ACC_LIST_NOLOCK( bfap ) \
{ \
    MS_SMP_ASSERT(SLOCK_HOLDER(&BfAccessFreeLock.mutex)); \
    MS_SMP_ASSERT(bfap->onFreeList == 1 || bfap->onFreeList == -1); \
    MS_SMP_ASSERT(bfap->freeFwd); \
    MS_SMP_ASSERT(bfap->freeBwd); \
    RM_ACC_LIST_REAL_WORK( bfap ); \
}

/* RM_ACC_LIST_COND is like RM_ACC_LIST, except that the state of freeFwd
 * is checked to see if the struct is already on the free or closed list;
 * if it is, then it is removed.  The caller typically has bfap->bfaLock
 * seized.
 */
#define RM_ACC_LIST_COND( bfap ) \
{ \
    mutex_lock(&BfAccessFreeLock); \
    if (bfap->freeFwd) { \
        MS_SMP_ASSERT(bfap->onFreeList == 1 || bfap->onFreeList == -1); \
        MS_SMP_ASSERT(bfap->freeBwd);  \
        RM_ACC_LIST_REAL_WORK( bfap ); \
    } \
    mutex_unlock(&BfAccessFreeLock); \
}

#ifdef ADVFS_DEBUG
#define ACCMAX(accHdr) if ( accHdr.len > accHdr.max ) accHdr.max = accHdr.len
#define ACCMIN(accHdr) if ( accHdr.len < accHdr.min ) accHdr.min = accHdr.len
#else
#define ACCMAX(accHdr)
#define ACCMIN(accHdr)
#endif
#endif /* _ACCESS_H_ */
