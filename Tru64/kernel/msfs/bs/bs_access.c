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
 * 
 */
#pragma ident "@(#)$RCSfile: bs_access.c,v $ $Revision: 1.1.746.17 $ (DEC) $Date: 2006/11/20 08:47:25 $"

#include <sys/param.h>
#include <sys/mount.h>
#include <sys/lock_probe.h>
#include <sys/clu.h>

#include <kern/sched_prim.h>

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/bs_delete.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_stg.h>
#include <msfs/advfs_evm.h> 
#include <msfs/fs_dir_routines.h>
#include <msfs/fs_file_sets.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_index.h>
#include <msfs/ms_osf.h>
#include <sys/buf.h>
#include <sys/user.h>
#include <sys/ucred.h>
#include <sys/fcntl.h>          /* for MAXEND */
#include <vm/vm_ubc.h>
#include <vm/vm_numa.h>
#include <msfs/bs_msg_queue.h>
#include <msfs/bs_access.h>
#include <msfs/fs_dir.h>

static bfAccessT *get_free_acc(int *retry, int forceFlag);

extern struct vnodeops msfs_vnodeops;
int msfs_vget_bad = 0;
long msfs_bsacc_recycles = 0L;

/*
 * Percent of access structures allowed to have saved_stats
 * till we begin flushing stats to disk.
 */

unsigned int MaxSavedStatsAccessPercent = 15;
unsigned int MinSavedStatsAccessPercent = 5;
unsigned int MaxSavedStatsCleanupRetries = 5;

/*
 * Maximum percent of VM managed pages that should be used
 * by AdvFS access structures.
 */
unsigned int AdvfsAccessMaxPercent = 25;      /* 25% default */

/*
 * The desired minimum number of access structures that should be
 * kept on the access structure free list at all times.
 */
unsigned int AdvfsMinFreeAccess = ADVFS_MIN_FREE_ACCESS;

/*
 * The number of access structures in the system.
 */
int NumAccess = 0;

/*
 * The maximum number of access structures that can be allocated
 * under normal conditions.
 */
int MaxAccess = 0;

/*
 * The minimum number of access structures that should be kept
 * if defined via a tunable.
 */
int AdvfsMinAccess = 0;

/*
 * This boolean is set to TRUE when the fs_cleanup_thread is
 * repopulating the free list of access structures.
 */
int BfapAllocInProgress = FALSE;

/* 
 * This boolean is set to TRUE when get_free_acc() returns
 * EHANDLE_OVF and is set back to FALSE when NumAccess 
 * drops below MaxAccess.
 */

int MaxAccessEventPosted = FALSE;

unsigned long cleanup_thread_calls = 0;
unsigned long cleanup_structs_skipped = 0;
unsigned long cleanup_structs_processed = 0;
unsigned long cleanup_saved_stats_processed = 0;
unsigned long cleanup_times_blocked_waiting_for_cleanup = 0;
unsigned long cleanup_acc_unexpected_state_cnt = 0;
unsigned long cleanup_wouldblock_cnt = 0;
unsigned long cleanup_invalid_cnt = 0;
unsigned long cleanup_access_processed = 0;
unsigned long cleanup_free_no_obj = 0;
unsigned long cleanup_list_changed1 = 0;
unsigned long cleanup_pass2_processed = 0;
unsigned long cleanup_pass2_vgone = 0;
unsigned long cleanup_list_changed2 = 0;
unsigned long cleanup_vget_failed = 0;
unsigned long cleanup_list_changed3 = 0;
unsigned long cleanup_retry_all = 0;
unsigned long cleanup_pass2_set = 0;
int CleanupPass2Free = 16;
int SentCleanupMsg = 0;

/*
 * Message queue definitions for the access structure allocation thread.
 */
msgQHT AccAllocMsgQH;

typedef enum {
    ALLOC_BFAP_NORMAL = 1,  /* Respect AdvfsAccessMaxPercent limit */
    ALLOC_BFAP_ROOT,        /* Respect AdvfsAccessMaxPercent + 1% limit */
    ALLOC_BFAP_FORCE        /* Ignore AdvfsAccessMaxPercent limit */
} accAllocMsgTypeT;

typedef struct {
    accAllocMsgTypeT msgType;
} accAllocMsgT;

#define ADVFS_MODULE BS_ACCESS

/* private protos */

static statusT
get_n_setup_new_vnode(
                      bfAccessT *bfap,
                      bfSetT *bfSetp,    /* in - BF-set descriptor pointer */
                      struct vnode **nvp /* in/out - new vnode pointer */
                      );

uint32T
bf_setup_truncation(
                    bfAccessT *bfap,     /* in */
                    ftxHT ftxH,          /* in */
                    void **delList,      /* out */
                    int made_frag
                    );

void
check_mv_bfap_to_free(
                      bfAccessT* bfap    /* in */
                     );

void
test_xtnt(
          bfAccessT *bfap                /* in */
          );

int TestXtntsFlg = 0;

unsigned TrFlags = 0;

mutexT BfAccessFreeLock;  /* guards access free & closed lists */
extern int vm_managed_pages;
extern ulong access_structures_allocated;
extern msgQHT CleanupMsgQH;

struct lockinfo *ADVbfAccessT_xtntMap_lk_info;
struct lockinfo *ADVbfAccessT_mcellList_lk_info;
struct lockinfo *ADV_BMT_bfAccessT_mcellList_lk_info;
struct lockinfo *ADV_BMT_bfAccessT_xtntMap_lk_info;
struct lockinfo *ADVbfAccessT_trunc_xfer_lk_info;
struct lockinfo *ADVbfAccessT_cow_lk_info;
struct lockinfo *ADVbfAccessT_clu_clonextnt_lk_info;
struct lockinfo *ADVbsInMemXtntT_migTruncLk_info;
struct lockinfo *ADVClonebsInMemXtntT_migTruncLk_info;
struct lockinfo *ADVbfAccessT_mss_lk_info;
decl_simple_lock_info(, ADVBfAccessTraceMutex_lockinfo )
decl_simple_lock_info(, ADVBfAccessbfaLock_lockinfo )
decl_simple_lock_info(, ADVBfAccessFreeLock_lockinfo )
decl_simple_lock_info(, ADVBfAccessbfIoLock_lockinfo )
decl_simple_lock_info(, ADVBfactRangeLock_info )

struct bfAccessHdr FreeAcc;
struct bfAccessHdr ClosedAcc;

/* This is the dynamic hashtable for bs access structures */
void * BsAccessHashTbl;
struct lockinfo *ADVBfAccessHashChainLock_lockinfo;

extern int wait_for_vxlock2(struct vnode *  , int , int );

/******************************************************************
***************  Utility routines ********************************
******************************************************************/

#ifdef ADVFS_TAG_TRACE
#define TAG_TRACE_HISTORY 200
/* TAG_TRACE_NUM_BUCKETS must be a power of 2 */
#define TAG_TRACE_NUM_BUCKETS 512
typedef struct {
    uint32T       seq;
    uint16T       mod;
    uint16T       ln;
    struct thread *thd;
    int           hash;
    int           qual;
    void          *val;
} tagTraceElmtT;

typedef struct {
    int           tagTracePtr;
    tagTraceElmtT tagTraceBuf[TAG_TRACE_HISTORY];
} tagTraceT;
tagTraceT TagTrace[TAG_TRACE_NUM_BUCKETS];

/* For tag tracing, hash can be the file tag. qual can be the set tag */
/* For SBM block tracing, hash can be the int index. qual can be the SBM pg. */
void
tag_trace(uint hash, uint qual, uint16T module, uint16T line, void *value)
{
    register tagTraceElmtT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;
    tagTraceT *tt;

    simple_lock(&TraceLock);

    tt = &TagTrace[hash & (TAG_TRACE_NUM_BUCKETS - 1)];
    tt->tagTracePtr = (tt->tagTracePtr + 1) % TAG_TRACE_HISTORY;
    te = &tt->tagTraceBuf[tt->tagTracePtr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->hash = (signed)hash;
    te->qual = (signed)qual;
    te->val = value;

    simple_unlock(&TraceLock);
}
#endif

#ifdef ADVFS_ACCESS_TRACE

/*
 * Ignored for SMP
 */
void
access_trace(
             bfAccessT *bfap,
             uint16T module,
             uint16T line,
             void *value
             )
{
    register AccessTraceElmtT *te;
    extern simple_lock_data_t TraceLock;
    extern int TraceSequence;

    simple_lock(&TraceLock);

    bfap->trace_ptr = (bfap->trace_ptr + 1) % ACCESS_TRACE_HISTORY;
    te = &bfap->trace_buf[bfap->trace_ptr];
    te->thd = (struct thread *)(((long)current_cpu() << 36) |
                                 (long)current_thread() & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->val = value;

    simple_unlock(&TraceLock);
}
#endif /* ADVFS_ACCESS_TRACE */

/*
 * init_access
 *
 * Initialize an access structure, attaching it to the freelist.
 *
 * SMP
 *  Will seize and release BfAccessFreeLock when inserting the
 *  new access structure onto the FreeAcc list.
 */


void
init_access(bfAccessT *bfap)
{
    int wkday;


    mutex_init3(&bfap->bfIoLock,0,"bfIoLock",ADVBfAccessbfIoLock_lockinfo);
    mutex_init3(&bfap->bfaLock, 0, "bfaLock", ADVBfAccessbfaLock_lockinfo);
    mutex_init3(&bfap->actRangeLock, 0, "actRangeLock", ADVBfactRangeLock_info);
    mutex_lock(&bfap->bfaLock);
    lk_init( &bfap->stateLk, &bfap->bfaLock, LKT_STATE, 0, LKU_BF_STATE );
    bfap->stateLk.state = ACC_INVALID;
    ftx_lock_init( &bfap->xtntMap_lk,
                   &bfap->bfaLock,
                   ADVbfAccessT_xtntMap_lk_info);
    lock_setup( &bfap->trunc_xfer_lk, ADVbfAccessT_trunc_xfer_lk_info,TRUE);
    lock_setup( &bfap->cow_lk, ADVbfAccessT_cow_lk_info,TRUE);
    lock_setup( &bfap->clu_clonextnt_lk,
                ADVbfAccessT_clu_clonextnt_lk_info,
                TRUE);
    ftx_lock_init( &bfap->mcellList_lk,
                   &bfap->bfaLock,
                   ADVbfAccessT_mcellList_lk_info);
    lock_setup( &bfap->xtnts.migTruncLk,
                ADVbsInMemXtntT_migTruncLk_info,
                TRUE);
    bfap->mapped = FALSE;
    bfap->freeFwd = bfap->freeBwd = NULL;
    bfap->onFreeList = 0;
    bfap->dirtyBufList.accFwd = (struct bsBuf*)&bfap->dirtyBufList;
    bfap->dirtyBufList.accBwd = (struct bsBuf*)&bfap->dirtyBufList;
    bfap->dirtyBufList.length = 0;
    bfap->flushWait     = 0;
    bfap->maxFlushWaiters = 0;
    bfap->hiFlushLsn    = nilLSN;
    bfap->hiWaitLsn     = nilLSN;
    bfap->nextFlushSeq  = firstLSN;
    bfap->flushWaiterQ.head = (struct flushWaiter *)&bfap->flushWaiterQ;
    bfap->flushWaiterQ.tail = (struct flushWaiter *)&bfap->flushWaiterQ;
    bfap->flushWaiterQ.cnt = 0;
    bfap->seqWritePgCnt = 0;
    bfap->accessCnt = 0;
    bfap->refCnt = 0;
    bfap->cloneXtntsRetrieved = 0;
    bfap->tag = NilBfTag;
    bfap->xtnts.validFlag = 0;
    bfap->xtnts.xtntMap = NULL;
    bfap->xtnts.shadowXtntMap = NULL;
    bfap->xtnts.stripeXtntMap = NULL;
    bfap->xtnts.copyXtntMap = NULL;
    bfap->dirTruncp = NULL;
    bfap->setFwd =
    bfap->setBwd = NULL;
    bfap->accMagic = ACCMAGIC;
    bfap->hashlinks.dh_links.dh_next = NULL;
    bfap->hashlinks.dh_links.dh_prev = NULL;
    bfap->hashlinks.dh_key = 0;
    bfap->saved_stats = NULL;
    bfap->cowPgCount = 0;
    bfap->actRangeList.arFwd     = (struct actRange *)(&bfap->actRangeList);
    bfap->actRangeList.arBwd     = (struct actRange *)(&bfap->actRangeList);
    bfap->actRangeList.arWaitFwd = (struct actRange *)(&bfap->actRangeList);
    bfap->actRangeList.arWaitBwd = (struct actRange *)(&bfap->actRangeList);
    for(wkday=0; wkday<SS_HOT_TRACKING_DAYS; wkday++) {
        bfap->ssHotCnt[wkday] = 0;
    }
    bfap->ssMigrating = SS_MSG_IDLE;
    cv_init( &bfap->migWait );
    bfap->migPagesPending = 0;

    /*
     * Add this struct to beginning of the freelist.  We don't
     * use the ADD_ACC_FREELIST() macro because we also need
     * to increment NumAccess.
     */
    mutex_lock(&BfAccessFreeLock);
    MS_SMP_ASSERT(FreeAcc.freeFwd);
    bfap->freeBwd = (bfAccessT *)&FreeAcc;
    bfap->freeFwd = FreeAcc.freeFwd;
    FreeAcc.freeFwd->freeBwd = bfap;
    FreeAcc.freeFwd = bfap;
    bfap->onFreeList = 1;
    /*
     * Newly allocated access structures do not inherit bfap_free_time
     * from the first access structure on the freelist (as other structures
     * added via ADD_ACC_FREELIST() do), but are instead assigned
     * sched_tick (the current time).  This keeps them from getting
     * aged too soon.
     */
    bfap->bfap_free_time = sched_tick;
    FreeAcc.len++;
    ACCMAX(FreeAcc);
    NumAccess++;
    mutex_unlock(&BfAccessFreeLock);

    mutex_unlock(&bfap->bfaLock);
    return;
}

#define CHECK_ACC_CLEAN(bfap) \
{ \
    MS_DBG_ASSERT(SLOCK_HOLDER(&bfap->bfaLock.mutex)); \
    MS_DBG_ASSERT(bfap->dirtyBufList.length == 0); \
    MS_DBG_ASSERT(bfap->bfObj == NULL || bfap->bfObj->vu_dirtypl == NULL); \
    MS_DBG_ASSERT(bfap->bfObj == NULL || bfap->bfObj->vu_dirtywpl == NULL); \
    MS_DBG_ASSERT(bfap->bfObj == NULL || bfap->bfObj->vu_putpages == 0); \
    MS_DBG_ASSERT(lk_get_state(bfap->stateLk) != ACC_INIT_TRANS); \
    MS_DBG_ASSERT(lk_get_state(bfap->stateLk) != ACC_FTX_TRANS); \
    MS_DBG_ASSERT( !lock_readers( &(bfap->cow_lk) ) ); \
    MS_DBG_ASSERT( !lock_locked( &(bfap->cow_lk) ) ); \
    MS_DBG_ASSERT(!lk_locked(bfap->mcellList_lk)); \
    MS_DBG_ASSERT(!lk_locked(bfap->xtntMap_lk)); \
    MS_DBG_ASSERT(bfap->refCnt == 0); \
    MS_DBG_ASSERT(bfap->saved_stats == NULL); \
}

/*
 * Cleanup the closed list of access structures.
 * NOTE: This algorithm assumes that no access structures are being
 *       deallocated while this is happening.  Currently, that is the
 *       case since this routine is only run by fs_cleanup_thread() and
 *       access structure deallocation is also only done by that thread.
 *
 *       cleanup_closed_list() is invoked via get_free_acc(). 
 *
 *       get_free_acc() passes a NULL domain pointer to allow cleanup
 *       of any (but not necessarily all) structures.
 *
 */
void
cleanup_closed_list(clupClosedListTypeT clean_type)
{
    bfAccessT *bfap;
    bfAccessT *nextp;
    struct fsContext* contextp;
    lkStatesT state;
    statusT   ret;
    unsigned long css;
    unsigned long cic;
    int pass_2 = 0;
    struct vnode *vp;
    unsigned int cleanup_retry_stats_only = 0;
    int ClosedAccCleanInProgress = 0;
    int ClosedAccCleanStatsInProgress = 0;

    mutex_lock(&BfAccessFreeLock);

    switch (clean_type) {
        case CLEANUP_ANY:
            ClosedAccCleanInProgress = 1;
            break;
        case CLEANUP_STATS:
            ClosedAccCleanStatsInProgress = 1;
            break;
        default:
            /* shouldn't be here */
            MS_SMP_ASSERT(0);
            return;
    }

    cleanup_thread_calls++;

_retry:
    css = cleanup_structs_skipped;
    cic = cleanup_invalid_cnt;

    bfap = ClosedAcc.freeFwd;
    while (bfap != (bfAccessT *)(&ClosedAcc)) {
        cleanup_access_processed++;
        
        /* The normal locking order is bfap->bfaLock before FreeLock, so we
         * must do some extra work here to prevent deadlock with other threads.
         * Basically, if we can't get a lock on a given access struct, we
         * skip it.
         */
        if ( !mutex_lock_try(&bfap->bfaLock.mutex) ) {
            bfap = bfap->freeFwd;
            cleanup_structs_skipped++;
            continue;
        }
        /* do not move access struct with pending I/O errors */
        if (bfap->dkResult != EOK)
        {
            mutex_unlock(&bfap->bfaLock);
            bfap = bfap->freeFwd;
            cleanup_structs_skipped++;
            continue;
        }

        state = lk_get_state(bfap->stateLk);
        if (state != ACC_VALID && state != ACC_FTX_TRANS)
            cleanup_acc_unexpected_state_cnt++;   /* Just a sanity check */

        /* No access structure should be on the closed list in the
         * ACC_INVALID state;  some show up occasionally and this will
         * remedy the situation.  If the underlying cause of these invalid
         * structures being on the closed list is fixed, then this paragraph
         * can be removed.
         */
        if ( state == ACC_INVALID ) {
            cleanup_invalid_cnt++;
            if (bfap->bfObj) {
                nextp = bfap->freeFwd;
                /*
                 * Try to lock the bfIoLock out-of-order.  If this fails,
                 * just skip this access structure.
                 */
                if (mutex_lock_try(&bfap->bfIoLock.mutex)) {
                    mutex_unlock(&BfAccessFreeLock);
                    check_mv_bfap_to_free(bfap);
                    mutex_lock(&BfAccessFreeLock);
                    mutex_unlock(&bfap->bfIoLock);
                }
                mutex_unlock(&bfap->bfaLock);
                bfap = nextp;
                /* make sure bfap is still on the list and still valid */
                if ( bfap == (bfAccessT*)&ClosedAcc ||
                     bfap->accMagic != ACCMAGIC ||
                     bfap->onFreeList != -1)
                {
                    break;
                }
                continue;
            } else if ( !bfap->saved_stats ) {
                /* VCHR & VBLK type files don't have objects (see bf_get_l). */
                /* If the state is invalid and there is no object then the */
                /* saved_stats is the only condition that can keep the */
                /* access structure on the closed list. */
                cleanup_free_no_obj++;
                nextp = bfap->freeFwd;
                RM_ACC_LIST_NOLOCK(bfap);
                ADD_ACC_FREELIST(bfap);
                mutex_unlock(&bfap->bfaLock);
                bfap = nextp;
                continue;
            }
        }

        /* Recycle some of the bfacces structs from the ClosedAcc list to
         *     the FreeAcc list.  Do not try to update stats if:
         * 1.  file state is ACC_FTX_TRANS (this is a temporary condition)
         * 2.  There is no vnode associated with this bfap AND there are no
         *         stats saved from the latest reclaim
         * 3.  There is no context pointer
         * 4.  The dirty_stats flag is not set.
         *
         * If the first pass didn't free any access structures, we still
         * don't care about access structures without vnodes because
         * msfs_reclaim() will free up the buffers.
         */
        if ( bfap->bfVp ) contextp = VTOC(bfap->bfVp);
        else contextp = NULL;
        if ( (state == ACC_FTX_TRANS)                                       ||
             ((bfap->bfVp == NULL) && (bfap->saved_stats == NULL))          ||
             (bfap->bfVp && contextp == NULL)                               ||
             (contextp && !contextp->dirty_stats && !pass_2)                ||
             (bfap->dmnP == NULL)) 
        {
            nextp = bfap->freeFwd;
            /* Make sure index file access structs make it
              * to the free list */
            if (mutex_lock_try(&bfap->bfIoLock.mutex)) {
                mutex_unlock(&BfAccessFreeLock);
                check_mv_bfap_to_free(bfap);
                mutex_lock(&BfAccessFreeLock);
                mutex_unlock(&bfap->bfIoLock);
            }

            mutex_unlock(&bfap->bfaLock);
            if (bfap->onFreeList != 1) /* we skipped it */
            {
                cleanup_structs_skipped++;
            }
            bfap = nextp;
            /* make sure bfap is still on the list and still valid */
            if ( bfap == (bfAccessT*)&ClosedAcc ||
                 bfap->accMagic != ACCMAGIC ||
                 bfap->onFreeList != -1)
            {
                break;
            }
            continue;
        }

        /* Find the next entry before we drop the BfAccessFreeLock. */
        nextp  = bfap->freeFwd;

        /* Check for an access structure that has no vnode but has a
         * saved_stats structure.  This calls for special processing.
         * The vnode was reclaimed in msfs_reclaim, but we could not
         * flush the stats to the disk at that time.  Therefore, we
         * saved the stats in the access structure and put it onto the
         * closed list, waiting for this opportunity to flush these stats
         * to disk.  This paragraph is a shortened version of the one that
         * follows for bfap with no vnode.
         */
        if ((bfap->bfVp == NULL) && (bfap->saved_stats != NULL)) {

            /*
             * ClosedAcc.saved_stats_len is incremented or decremented
             * whenever an access structure with saved_stats is
             * added or removed from the closed list (see bs_access.h).
             * Currently the bfap->saved_stats structure is never
             * deallocated while the bfap is on the closed list.
             * Note here that the bfap is removed from the closed list
             * prior to processing.  If it turns out that the saved_stats 
             * structure is not removed, the bfap will be added back to
             * the closed list (ADD_ACC_CLOSEDLIST will increment
             * ClosedAcc.saved_stats_len).
             */

            RM_ACC_LIST_NOLOCK(bfap);        /* take off closed list */
            bfap->refCnt++;

            /* set bit that we are flushing the stats;  a racing bf_get_l()
             * may see this while we are flushing (the lock is dropped inside
             * fs_flush_saved_stats) and load up the stats into a new
             * fsContext, but it will not try to deallocate bfap->saved_stats
             * out from under the fs_flush_saved_stats() routine. Then we must
             * deallocate it after flushing and setting saved_stats to NULL.
             */
            bfap->saved_stats->op_flags |= SS_FLUSH_IN_PROGRESS;

            mutex_unlock(&BfAccessFreeLock);
            ret = fs_flush_saved_stats(bfap, FTX_NOWAIT, FtxNilFtxH);
            if (ret == EWOULDBLOCK) {
                /* All ftx slots used up; we did no processing of the stats
                 * so set it all back to the way it was, stop processing,
                 * and wake waiters */
                bfap->saved_stats->op_flags &= ~SS_FLUSH_IN_PROGRESS;
                if (bfap->saved_stats->op_flags & SS_STATS_COPIED_TO_CONTEXT) {
                    /* Special case; we didn't flush the stats, but they got
                     * copied back to a new fsContext while the lock was
                     * dropped, so latest stats are in the context.  Get rid
                     * of the out-of-date saved_stats.
                     */
                    ms_free( bfap->saved_stats );
                    bfap->saved_stats = NULL;
                }
                cleanup_wouldblock_cnt++;
                DEC_REFCNT(bfap);
                mutex_unlock(&bfap->bfaLock);
                
                  /*
                   *  If on behalf of a domain deactivation,
                   *    Block to allow another thread a chance to clear
                   *    the EWOULDBLOCK (free the ftx slot).  Otherwise, on
                   *    single cpu systems we could hang when we come back
                   *    through the retry loop.
                   */

                if (ClosedAccCleanStatsInProgress) {
                    assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
                    thread_block();
                }
                mutex_lock(&BfAccessFreeLock);
                break;
            } else {
                cleanup_saved_stats_processed++;
                ms_free( bfap->saved_stats );
                bfap->saved_stats = NULL;
                DEC_REFCNT(bfap);
                mutex_unlock(&bfap->bfaLock);
                mutex_lock(&BfAccessFreeLock);
            }

            /* Be careful we don't walk off the list; this bfap may no
             * longer be on the closed list when we return.
             */
            bfap = nextp;
            if ( bfap == (bfAccessT*)&ClosedAcc ||
                 bfap->accMagic != ACCMAGIC )
            {
                break;
            }
            if ( bfap->onFreeList != -1 ) {
                cleanup_list_changed1++;
                break;
            }
            else
                continue;
        } else if (ClosedAccCleanStatsInProgress) {
            mutex_unlock(&bfap->bfaLock);
            bfap = bfap->freeFwd;
            cleanup_structs_skipped++;
            continue;
        }

        vp = bfap->bfVp;
        VN_LOCK(vp);

        /*
         * ClosedAccCleanInProgress only:
         *
         * If we failed to free any access structures on the first pass,
         * force access structure to be freed via msfs_reclaim() which
         * already has code to free up the buffers.
         */
        if (pass_2) {       /* ClosedAccCleanInProgress */
            cleanup_pass2_processed++;
            if (vp->v_usecount == 0 && vp->v_tag == VT_MSFS) {
                cleanup_pass2_vgone++;
                mutex_unlock(&BfAccessFreeLock);
                mutex_unlock(&bfap->bfaLock);
                vgone(vp, VX_NOSLEEP, 0);
                VN_UNLOCK(vp);
                mutex_lock(&BfAccessFreeLock);
            } else {
                mutex_unlock(&bfap->bfaLock);
                VN_UNLOCK(vp);
            }

            bfap = nextp;
            if ( FreeAcc.len > CleanupPass2Free ||
                 bfap == (bfAccessT*)&ClosedAcc ||
                 bfap->accMagic != ACCMAGIC )
            {
                break;
            }
            if ( bfap->onFreeList != -1 ) {
                cleanup_list_changed2++;
                break;
            } else
                continue;
        }

        /* vget() the vnode to prevent it from being reclaimed after we
         * drop the mutex.  If we can't get it, skip to next access struct.
         */
        if (!vget_nowait(vp)) {
            /* Update stats and (hopefully) move to freelist in DEC_REFCNT() */
            VN_UNLOCK(vp);
            RM_ACC_LIST_NOLOCK(bfap);
            bfap->refCnt++;
            mutex_unlock(&BfAccessFreeLock);
            mutex_unlock(&bfap->bfaLock);
            ret = fs_update_stats(vp, bfap, FtxNilFtxH, FTX_NOWAIT);
            /* Don't relock FreeLock until after DEC_REFCNT since it will be
             * seized there when moving bfap to free list.
             */
            mutex_lock(&bfap->bfaLock);
            DEC_REFCNT(bfap);
            mutex_unlock(&bfap->bfaLock);
            vrele(vp);
            mutex_lock(&BfAccessFreeLock);

            if (ret == EWOULDBLOCK) {
                /* All ftx slots used up; stop processing and wake waiters */
                cleanup_wouldblock_cnt++;
                break;
            } else {
                cleanup_structs_processed++;
            }
        } else {
            cleanup_vget_failed++;
            cleanup_structs_skipped++;
            VN_UNLOCK(vp);
            mutex_unlock(&bfap->bfaLock);
        }

        /* The next one on the CloseAcc list may not be on that list
         * any more.  In this case, we have lost our way down that chain
         * and will exit.  We could go back and restart, but I don't want
         * to get caught here in a long loop;  we will be invoked again
         * by get_free_acc() if necessary.
         */
        bfap = nextp;
        if ( bfap == (bfAccessT*)&ClosedAcc ||
             bfap->accMagic != ACCMAGIC )
        {
            break;
        }
        if ( bfap->onFreeList != -1 ) {
            cleanup_list_changed3++;
            break;
        }
    }


    if ( ClosedAccCleanStatsInProgress ) {

        if ((ClosedAcc.saved_stats_len > 
            (NumAccess * MinSavedStatsAccessPercent)/100) &&
            (cleanup_retry_stats_only < MaxSavedStatsCleanupRetries)) { 

            cleanup_retry_stats_only++;

            /*
             * If there are any free BfAccess structs and threads waiting
             * in get_free_acc(), wake them up.  Since we will be doing
             * more work, don't clear the running flag here.
             */
            if (FreeAcc.len && SentCleanupMsg) {
                thread_wakeup((vm_offset_t)&SentCleanupMsg);
            }
            mutex_unlock(&BfAccessFreeLock);
            assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
            thread_block();
            mutex_lock(&BfAccessFreeLock);

            goto _retry;
        }

        ClosedAccCleanStatsInProgress = 0;
    } else {
        if (FreeAcc.len == 0 && !pass_2) {
            cleanup_pass2_set++;
            pass_2 = 1;
            goto _retry;
        }
        ClosedAccCleanInProgress = 0;
    }

    /* Wake up any threads waiting in get_free_acc() */
    if (SentCleanupMsg) {
        SentCleanupMsg = 0;
        thread_wakeup((vm_offset_t)&SentCleanupMsg);
    }
    mutex_unlock(&BfAccessFreeLock);

    switch (clean_type) {
        case CLEANUP_ANY:
            /* Wake up any threads waiting in get_free_acc() */
            thread_wakeup((vm_offset_t)(&ClosedAccCleanInProgress));
            break;
        case CLEANUP_STATS:
            /* Wake up any threads waiting in get_free_acc() */
            thread_wakeup((vm_offset_t)(&ClosedAccCleanStatsInProgress));
            break;
        default:
            /* shouldn't be here */
            break;
    }

    return;
}


/*
 * Cleanup the fileset's access structures list, by flushing stats
 * of structures on the closed list.
 *
 *     This function was derived from cleanup_closed_list, with 
 *     modifucation to wait for blocking conditions rather than
 *     skipping the access structure being blocked. 
 * NOTE: This algorithm assumes that no access structures are being
 *       deallocated while this is happening.  
 */

void
bfs_flush_dirty_stats(bfSetT *bfSetp,
                      ftxHT   ftxH
                      )
{
    bfAccessT *bfap;
    bfAccessT *nextp;
    lkStatesT state;
    statusT   ret;
    struct vnode *vp;
    
    
    mutex_lock(&bfSetp->accessChainLock);

restart:

    bfap = bfSetp->accessFwd;
    
    while (bfap != (bfAccessT *)(&bfSetp->accessFwd)) {

        if (bfap->onFreeList != -1) {
            /*
             * Skip bfaps not on closed list.  This is the most common code path.
             */
            bfap = bfap->setFwd;
            continue;
        }
                           
        mutex_lock(&bfap->bfaLock);
        state = lk_get_state(bfap->stateLk);
        
        MS_SMP_ASSERT(state != ACC_INIT_TRANS);

        nextp  = bfap->setFwd;
        vp = bfap->bfVp;
        mutex_unlock(&bfap->bfaLock);

        if (vp != NULL) {

            /*
             * Get the vnode to prevent it from being reclaimed after we
             * drop the mutex.  If we can't get it, we lost a race and it is
             * gone and will be handled as if it were previously reclaimed.
             */

            mutex_unlock(&bfSetp->accessChainLock);

            if (!vget(vp)) {
                /* Update stats and (hopefully) move to freelist in DEC_REFCNT() */
                RM_ACC_LIST(bfap);
                mutex_lock(&bfap->bfaLock);
                bfap->refCnt++;
                mutex_unlock(&bfap->bfaLock);

                ret = fs_update_stats(vp, bfap, ftxH, 0);

                mutex_lock(&bfap->bfaLock);
                DEC_REFCNT(bfap);
                mutex_unlock(&bfap->bfaLock);
                vrele(vp);
                mutex_lock(&bfSetp->accessChainLock);
                bfap = nextp;
                if ( bfap == (bfAccessT*)&bfSetp->accessFwd ) {
                    break;
                }
                if ( bfap->accMagic != ACCMAGIC || bfap->onFreeList != -1 ) {
                    goto restart;  /* current link changed */
                }
                continue;
            }
            mutex_lock(&bfSetp->accessChainLock);
             
        } 
                
        /* Check for an access structure that has no vnode but has a
         * saved_stats structure.  The vnode was reclaimed in msfs_reclaim, 
         * but we did not flush the stats to the disk at that time.  Therefore,
         * we saved the stats in the access structure and put it onto the
         * closed list, waiting for this opportunity to flush these stats
         * to disk.  
         */
        if ((vp == NULL) && (bfap->saved_stats != NULL)) {

            /*
             * ClosedAcc.saved_stats_len is incremented or decremented
             * whenever an access structure with saved_stats is
             * added or removed from the closed list (see bs_access.h).
             * Currently the bfap->saved_stats structure is never
             * deallocated while the bfap is on the closed list.
             * Note here that the bfap is removed from the closed list
             * prior to processing.  If it turns out that the saved_stats 
             * structure is not removed, the bfap will be added back to
             * the closed list (ADD_ACC_CLOSEDLIST will increment
             * ClosedAcc.saved_stats_len).
             */

            mutex_lock(&bfap->bfaLock);
            RM_ACC_LIST(bfap);        /* take off closed list */
            bfap->refCnt++;

            /* Set stat flush in progress bit;  a racing bf_get_l()
             * may see this while we are flushing (the lock is dropped inside
             * fs_flush_saved_stats) and load up the stats into a new
             * fsContext, but it will not try to deallocate bfap->saved_stats
             * out from under the fs_flush_saved_stats() routine. Then we must
             * deallocate it after flushing and setting saved_stats to NULL.
             */
            bfap->saved_stats->op_flags |= SS_FLUSH_IN_PROGRESS;
            mutex_unlock(&bfSetp->accessChainLock);
            ret = fs_flush_saved_stats(bfap, 0, ftxH);
            mutex_unlock(&bfap->bfaLock);
            ms_free( bfap->saved_stats );
            mutex_lock(&bfap->bfaLock);
            bfap->saved_stats = NULL;
            DEC_REFCNT(bfap);
            mutex_unlock(&bfap->bfaLock);
            mutex_lock(&bfSetp->accessChainLock);
            bfap = nextp;
            if ( bfap == (bfAccessT*)&bfSetp->accessFwd ) {
                break;
            }
            if ( bfap->accMagic != ACCMAGIC || bfap->onFreeList != -1 ) {
                goto restart;  /* current link changed */
            }
            continue;
        }
        bfap = nextp;
    }  /* End of WHILE loop */

    mutex_unlock(&bfSetp->accessChainLock);

    return;
}

/*
 * get_free_acc
 *
 * If the free list is running low on access structures,
 * send a message to the fs_cleanup_thread to repopulate it.
 * Remove an access struct from the freelist if the freelist
 * is not empty.  If there are no access structures on the freelist,
 * wait for the cleanup thread to complete its work.  Then return
 * to the caller without an access structure and with the retry
 * flag set.
 *
 * If we run out of memory the current malloc() operation hangs waiting
 * for some memory to be free'd.
 * 
 * NOTE:  CALLER IS RESPONSIBLE FOR INITIALIZING '*RETRY' PARAMETER!
 *
 * SMP
 *  Called holding no locks
 *  Locks and unlocks the BfAccessFreeLock.
 *  Return access structure holding bfap->bfaLock
 *  If retry is set on return, the value returned 
 *  is NULL and nothing is locked.
 */


static bfAccessT *
get_free_acc(int *retry,        /* In/Out - Retry or error status */
             int forceFlag)     /* In - If TRUE, ignore AdvfsAccessMaxPercent */
{
    bfAccessT *bfap;
    accAllocMsgT *allocmsg;     /* message to send to bfap allocation thread */
    clupThreadMsgT *cleanupmsg; /* message to send to cleanup thread */
    int sent_alloc_msg = FALSE, /* TRUE if we send message to alloc thread */
        have_access_structure = FALSE, /* TRUE when we get one to use */
        not_root = TRUE;         /* FALSE if this is root */
    int cleanup_any=0;


    /*
     * Use the first access structure from the free list if one is available.
     * Note normal locking order is bfap->bfaLock before FreeLock which is
     * being subverted here.  Also, this is the only place in this routine
     * that actually returns a bfap.
     */
    mutex_lock(&BfAccessFreeLock);
    bfap = FreeAcc.freeFwd;
    while( bfap != (bfAccessT *)&FreeAcc ) {

        /* Lock the bfap before proceeding, being careful not to deadlock
         * with threads locking in the conventional order.
         */
        if ( !mutex_lock_try(&bfap->bfaLock.mutex) ) {
            bfap = bfap->freeFwd;
            continue;
        }
        RM_ACC_LIST_NOLOCK(bfap);
        CHECK_ACC_CLEAN(bfap);
        break;
    }

    if (bfap != (bfAccessT *)&FreeAcc) {
        have_access_structure = TRUE;
    }

    /*
     * Try to move access structures from the closed list to the
     * free list if we haven't sent a message to the cleanup thread
     * and one of the following is true:
     * 1. We don't yet have an access structure because the free
     *    list is empty; there are some access structures on the closed
     *    list; and either the maximum allowed number of access 
     *    structures have been allocated, or we are on a retry (e.g.
     *    the allocator might have failed).
     * 2. The number of closed list access structures with saved_stats
     *    is at least MaxSavedStatsAccessPercent of the total number of
     *    access structures.  Such structures should not have buffers
     *    associated with them.  An attempt is made to clean these up
     *    in order to prevent a huge number accumulating on the closed
     *    list, particularly in the face of NFS failures.
     *  
     * NOTE: Currently grab_bsacc() will call get_free_acc() 
     *    infinitely if either the allocator is unable to allocate
     *    more access structures, or the cleanup thread is unable to
     *    make progress on moving access structs from the closed list 
     *    to the free list.  This occurs only if (NumAccess < MaxAccess).
     *
     */
    if (!SentCleanupMsg &&
        ((cleanup_any=(!have_access_structure && ClosedAcc.len > 0 &&
             (NumAccess >= MaxAccess || *retry == TRUE))) ||
         (ClosedAcc.saved_stats_len > 
             (NumAccess * MaxSavedStatsAccessPercent)/100))) {

        cleanupmsg = (clupThreadMsgT *)msgq_alloc_msg(CleanupMsgQH);
        if (cleanupmsg) {
            cleanupmsg->msgType = CLEANUP_CLOSED_LIST;
            cleanupmsg->body.closedListInfo.clean_type =
                cleanup_any ? CLEANUP_ANY : CLEANUP_STATS;
            msgq_send_msg(CleanupMsgQH, cleanupmsg);
            SentCleanupMsg = TRUE;
        }
    }

    /*
     * Try to allocate a new access structure under any one of the 
     * following conditions:
     *
     * 1. There are less than AdvfsMinFreeAccess access structures on
     *    the access structure free list and the access structure allocation
     *    thread is not currently in the process of allocating an 
     *    access structure and the maximum allowed number of access 
     *    structures has not yet been allocated.
     * 2. We do not yet have an access structure and the caller specified
     *    the forceFlag.  This is usually set by an undo or root-done agent.
     * 3. We do not yet have an access structure and this is the root user
     *    and we are less than 1% over the limit specified by 
     *    AdvfsAccessMaxPercent.  That is, the root user can allocate
     *    up to AdvfsAccessMaxPercent + 1 percent of vm_managed_pages for
     *    access structures.  This is helpful in situations where the
     *    system is basically hung because all the access structures are
     *    in use and the root user wants to run ps, kill, sysconfig, etc.
     *    To run any of these commands may require opening a file (for
     *    example, the loader may need to open the executable file to load
     *    it in).  Without this safety valve, the root user couldn't even
     *    run ps or sysconfig on an all-AdvFS system.
     */
    not_root = TRUE;
    if ((FreeAcc.len < AdvfsMinFreeAccess && 
        !BfapAllocInProgress && NumAccess < MaxAccess) ||
        ((!have_access_structure) && 
         ((forceFlag) || 
          ((not_root = suser(u.u_cred, &u.u_acflag)) == FALSE)))) {
        allocmsg = msgq_alloc_msg(AccAllocMsgQH);
        if (allocmsg) {
            allocmsg->msgType = forceFlag ? ALLOC_BFAP_FORCE : 
                                            (!not_root ? 
                                                ALLOC_BFAP_ROOT :
                                                ALLOC_BFAP_NORMAL);
            msgq_send_msg(AccAllocMsgQH, allocmsg);
            sent_alloc_msg = TRUE;
        }
    }

    /*
     * If we found a free access structure, we're done.
     */
    if (have_access_structure) {
        mutex_unlock(&BfAccessFreeLock);
        *retry = FALSE;
        return bfap;
    }

    /*
     * If we sent a message to the allocation thread,
     * wait for him to wake us up.  Otherwise, just
     * have the caller retry the call since we could
     * not lock an access structure or allocation was
     * already in progress.
     */
    if (sent_alloc_msg) {
        cleanup_times_blocked_waiting_for_cleanup++;
        assert_wait((vm_offset_t)(&BfapAllocInProgress), FALSE);
        mutex_unlock(&BfAccessFreeLock);
        thread_block();
        mutex_lock(&BfAccessFreeLock);
    }

    /*
     * If this is a retry and there are still no access structures on the
     * free list and we can't allocate any more, give up.  Otherwise,
     * try again.
     */

    if ((*retry == TRUE) && (FreeAcc.len == 0) && 
        (NumAccess >= MaxAccess) && (!forceFlag)) {
        /*
         * We are done with BfAccessFreeLock.
         * Must remove it before we call ms_malloc anyway, so do it now.
         */
        mutex_unlock(&BfAccessFreeLock);
        *retry = EHANDLE_OVF;
        if (!MaxAccessEventPosted) {
            advfs_ev *advfs_event = NULL;
            ms_printf("Could not create new AdvFS access structure; already at AdvFS memory limit\n");
            ms_printf("as specified by AdvfsAccessMaxPercent value of %d%%.\n", AdvfsAccessMaxPercent);
            advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
            if (advfs_event) {
                advfs_post_kernel_event(EVENT_MAX_ACCESS_STRUCT, advfs_event);
                ms_free(advfs_event);
            }

            MaxAccessEventPosted = TRUE;
        }
    } else {
        *retry = TRUE;

        /*
         * If the cleanup thread is running, do a thread_block() until
         * it finishes.  Then return to the caller with a message to
         * retry.
         */

        if (SentCleanupMsg) {
            assert_wait((vm_offset_t)(&SentCleanupMsg), FALSE);
            mutex_unlock(&BfAccessFreeLock);
            thread_block();
            return (NULL);
        }

        mutex_unlock(&BfAccessFreeLock);

    }
    return (NULL);
}

/*
 * find_bfap
 *
 * Do the hash lookup for the access struct given the bitfile
 * set handle and the tag.  Return a pointer to the access
 * struct if found, NULL otherwise.
 *
 * SMP
 *  Called with no locks held.
 *  If found, return struct with:
 *    1.  bfap->bfaLock held
 *    2.  bfap->stateLk != ACC_RECYCLE
 *
 *  If hold_hashlock is TRUE, BfAccessHashLock is held on return.
 *  Insert count can be used to check if an insertion to the chain
 *  the returned element was on, has occurred between successive
 *  calls.
 */


bfAccessT *
find_bfap(
          bfSetT *bfSetp,         /* in - bitfile-set descriptor poitner */
          bfTagT tag,             /* in - bitfile tag */
          int    hold_hashlock,   /* in - TRUE = return with HashLock held */
                                  /*      FALSE = release HashLock on return */
          ulong  *insert_count)   /* out - hint indicating an insertion
                                           since last time chain was locked */
{
    bfAccessT *findBfap, *last_bfap;
    ulong hash_key;
    thread_t th = current_thread();


    /*
     * Grab the hash table chain's lock to protect the hash lookup
     */

    /* We used to assert that the fileset descriptor was valid.  This
     * assumption is incorrect, since it can go away during the time
     * msfs_reclaim is looking for the bfap of a vnode it is
     * reclaiming. Since nothing prevents it, a fileset could go away
     * in this time frame.
     *
     * It is ok for find_bfap to search the hashtable, since it
     * will not find the bfap and msfs_reclaim will deal with that */

    hash_key = BS_BFAH_GET_KEY( bfSetp, tag);

restart:

    last_bfap = (bfAccessT *) BS_BFAH_LOCK(hash_key,insert_count);
    if (last_bfap == NULL) findBfap=NULL;
    else
    {
        /* We are going to search the chain in the reverse order
         * stopping when we get back to where we started.
         */
        last_bfap = findBfap = last_bfap->hashlinks.dh_links.dh_prev;
    }
    do {
        if (findBfap == NULL) break;

        /* We now attempt to find the bsaccess structure in the
         * hash chain. If the tag.seq is 0 then this is a reserved
         * tag (ie. logfile, BMT, SBM, roottag dir) and the sequence
         * number is not incremented so we must therfore check the
         * bfsetid to insure that it is not an old reserved tag that was
         * left lying around.
         */

        /* We will no longer get the bfa lock each time. We will look
         * at the Setp and tag first and then the lock state.
         *
         * NOTE: If we encounter a BsAccess structure with the same
         * tag and setp but in ACC_RECYCLE we know that it will soon
         * have a different tag and setp but currently we need to wait
         * for it because any i/o associated with it must be flushed
         * before we can access the file.
         *
         * Some day we will be able to skip the wait since there
         * should never be anything to flush since this access
         * structure was just removed from the free list.
         */

        if ( (BS_BFTAG_EQL(findBfap->tag, tag)) &&
             findBfap->bfSetp == bfSetp )
        {
            if ( BS_BFTAG_REG(tag) )
              /*
               * regular files will have only one tag match per hash bucket.
               */
            {
                mutex_lock(&findBfap->bfaLock);
                MS_SMP_ASSERT( (BS_BFTAG_EQL(findBfap->tag, tag)) &&
                               findBfap->bfSetp == bfSetp );
                if (lk_get_state(findBfap->stateLk) == ACC_RECYCLE) {
                    BS_BFAH_UNLOCK(hash_key);
                    lk_wait_while(&findBfap->stateLk, &findBfap->bfaLock,
                                  ACC_RECYCLE);
                    mutex_unlock(&findBfap->bfaLock);
                    goto restart;
                }
                else if (lk_get_state(findBfap->stateLk) == ACC_DEALLOC) {

                    /*
                     * The fs_cleanup_thread() is deallocating this access
                     * structure.  Release all locks so that he can proceed.
                     * Also, call thread_preempt() so that he has a chance of
                     * winning the race to get the hash chain lock.  If we
                     * don't do this and he is on another processor, we will
                     * have an unfair advantage and possibly cause livelock.
                     */
                    mutex_unlock(&findBfap->bfaLock);
                    BS_BFAH_UNLOCK(hash_key);
                    thread_preempt( th, FALSE );
                    goto restart;
                }

                /* We found the one we want;  return it locked */
                goto return_it;
            }
            else
            {
                /* This is a reserved tag. Need further verification
                 * This situation can arise when many filesets are opened
                 * causing the metadata file tags to wrap around. Since the
                 * tag.seq is not incremented we can't tell so we must check
                 * the bfSetId itself.
                 */

                /* There's a chance the fileset could have gone away.
                 * We wont find it anyway so break out
                 */
                if ( ! BFSET_VALID(bfSetp) ) break;
                if (BS_BFS_EQL( bfSetp->bfSetId,
                                 ((bfSetT *)findBfap->bfSetp)->bfSetId )) {
                    mutex_lock(&findBfap->bfaLock);
                    MS_SMP_ASSERT( (BS_BFTAG_EQL(findBfap->tag, tag)) &&
                                   findBfap->bfSetp == bfSetp );
                    if (lk_get_state(findBfap->stateLk) == ACC_RECYCLE) {
                        BS_BFAH_UNLOCK(hash_key);
                        lk_wait_while(&findBfap->stateLk, &findBfap->bfaLock,
                                      ACC_RECYCLE);
                        mutex_unlock(&findBfap->bfaLock);
                        goto restart;
                    }
                    else if (lk_get_state(findBfap->stateLk) == ACC_DEALLOC) {

                        /*
                         * The fs_cleanup_thread() is deallocating this access
                         * structure.  Release all locks so that he can proceed.
                         * Also, call thread_preempt() so that he has a chance
                         * of winning the race to get the hash chain lock. If we
                         * don't do this and he is on another processor, we will
                         * have an unfair advantage and possibly cause livelock.
                         */
                        mutex_unlock(&findBfap->bfaLock);
                        BS_BFAH_UNLOCK(hash_key);
                        thread_preempt( th, FALSE );
                        goto restart;
                }
                    /* We found the one we want; return it locked */
                    goto return_it;
                }
            }
        }

        findBfap = findBfap->hashlinks.dh_links.dh_prev; /* searching in reverse */
    } while ( findBfap != last_bfap );
    findBfap = NULL;

return_it:
    MS_SMP_ASSERT(findBfap ? SLOCK_HOLDER(&findBfap->bfaLock.mutex) : 1);
    MS_SMP_ASSERT(findBfap ? lk_get_state(findBfap->stateLk) != ACC_RECYCLE :1);
    if (!hold_hashlock)
        BS_BFAH_UNLOCK(hash_key);
    return( findBfap );
}


/*
 * access_invalidate
 *
 * Marks all of the access structures for a fileset as invalid,
 * removes them from the access structure hash table, and puts
 * them at the head of the access structure free list since
 * they are prime candidates for reuse.
 */

void
access_invalidate(struct bfSet *bfSetp)
{
    bfAccessT *bfap, *nextbfap;
    bfAccessT *bfap_remove_from_hash_list = NULL;


start:

    mutex_lock(&bfSetp->accessChainLock);
    for (bfap = bfSetp->accessFwd;
         bfap != (bfAccessT *)(&bfSetp->accessFwd);
         bfap = nextbfap) {

        MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);
        /*
         * Lock the access structure.
         */
        mutex_lock(&bfap->bfaLock);
        MS_SMP_ASSERT(bfap->bfSetp == bfSetp);

        /*
         * If fs_cleanup_thread() is deallocating the access structure,
         * release our locks and call thread_block() to give him a chance
         * to take the accessChainLock.  If we don't do this, we will have
         * an unfair advantage and we may cause livelock by continuously
         * getting the accessChainLock, hitting this access structure,
         * and relooping.
         */
        if (lk_get_state(bfap->stateLk) == ACC_DEALLOC) {
            mutex_unlock(&bfap->bfaLock);
            mutex_unlock(&bfSetp->accessChainLock);
            assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
            thread_block();
            goto start;
        }

        /* If the access structure is in the process of being recycled
         * by another thread, wait for it since the recycling process will
         * take it off this fileset's list. This avoids a race with
         * bfs_invalidate destroying the accessChainLock before
         * grab_bsacc tries to take the bfap off the fileset chain.
         *
         * It should be safe to just start over. We will be starting
         * where we left off, since we have been removing access structures.
         */
        if (lk_get_state(bfap->stateLk) == ACC_RECYCLE) {
            mutex_unlock(&bfSetp->accessChainLock);
            lk_wait_while(&bfap->stateLk, &bfap->bfaLock, ACC_RECYCLE);
            mutex_unlock(&bfap->bfaLock);
            goto start;
        }

        /*
         * Make sure access structure isn't in use since
         * we're about to invalidate it.
         */
        MS_SMP_ASSERT(bfap->refCnt == 0);
        MS_SMP_ASSERT(bfap->dirtyBufList.length == 0);

        (void) lk_set_state( &bfap->stateLk, ACC_INVALID );
        bfap->bfVp = NULL;

        /*
         * Remove the access structure from the set's list
         * of access structures.
         */
        nextbfap = bfap->setFwd;
        RM_ACC_SETLIST(bfap, FALSE);

        /*
         * The access structure should be on the free list somewhere.
         * Move it to the head of the free list since it is a prime
         * candidate for reuse.
         */
        RM_ACC_LIST(bfap);

        if( bfap->hashlinks.dh_links.dh_next != NULL )
        {
            /* We can't make this visible yet. First we must
             * take it off the hash table but we can't do that until
             * we give up the accessChainLock so we'll defer this till
             * then.
             */
            bfap->setFwd=bfap_remove_from_hash_list;
            bfap_remove_from_hash_list=bfap;
        }
        else
        {
            /* Since the hashtable relies on the bfSetp for its hash
             * value we can not change this until the access structure
             * is no longer in the hash table
             */

            bfap->bfSetp = NULL;

            ADD_ACC_FREELIST(bfap);
        }
        mutex_unlock(&bfap->bfaLock);
    }
    mutex_unlock(&bfSetp->accessChainLock);
    /*
     * Now we will clean up our work list. It is safe to release the
     * chain lock here since all of the access structures on our work list
     * are no longer reachable from the fileset and are no longer on the
     * free or closed lists.  */

    nextbfap = bfap_remove_from_hash_list;
    while (nextbfap != NULL)
    {
        bfap=nextbfap;
        BS_BFAH_LOCK(bfap->hashlinks.dh_key,NULL);
        mutex_lock(&bfap->bfaLock);
        if( bfap->hashlinks.dh_links.dh_next != NULL ) /* you just never know */
        {
            BS_BFAH_REMOVE(bfap,FALSE);
        }
        else
        {
             BS_BFAH_UNLOCK(bfap->hashlinks.dh_key);
        }
        nextbfap = bfap->setFwd;
        bfap->setFwd=NULL;

        /* Since the hashtable relies on the bfSetp for its hash
         * value we can not change this until the access structure
         * is no longer on the hash table
         */

        bfap->bfSetp = NULL;

        ADD_ACC_FREELIST(bfap);
        mutex_unlock(&bfap->bfaLock);
    }
}


/*
 * bs_invalidate_rsvd_access_struct
 *
 * This function invalidates all the buffers connected to the reserved bitfile,
 * closes the bitfile and sets the state of the access structure to ACC_INVALID.
 * This function is used whenever the caller needs to get rid of all traces of a
 * reserved bitfile in memory: its access structure and its buffers.
 *
 * Removing a volume from the domain needs this function so that the previous
 * reserved bitfile's buffers and access structure are no longer in the system.
 * When we add a volume to the domain and it is assigned the same vdIndex of a
 * previous volume, this prevents any buffer or access structure cache hits.
 *
 * NOTE: All callers of this routine must first call bs_reclaim_cfs_rsvd_vn()
 *       to force cfs to release vnodes(hence bfap) for reserved files that
 *       may be in its cache but not currently used.  This is necessary
 *       because bs_invalidate_rsvd_access_struct() will panic if cfs has not
 *       released the bfap.
 *
 * Switching the root tag directory also needs this function.
 */


void
bs_invalidate_rsvd_access_struct(
                                 domainT *domain,  /* in */
                                 bfTagT bfTag,  /* in */
                                 bfAccessT *bfap  /* in */
                                 )
{
    bfAccessT *tbfap;
    statusT sts;

    /*
     * Kick all the bitfile's buffers out of the buffer cache.
     */
    bs_invalidate_pages (bfap, 0, 0, 0);

    /*
     * XXX - Cleanup access structure lockinfo
     *  See bfm_open_ms()
     */
    lock_terminate( &bfap->mcellList_lk.lock );
    lock_setup( &bfap->mcellList_lk.lock,
                ADVbfAccessT_mcellList_lk_info,
                TRUE );
    lock_terminate( &bfap->xtntMap_lk.lock );
    lock_setup( &bfap->xtntMap_lk.lock,
                ADVbfAccessT_xtntMap_lk_info,
                TRUE );

    /*
     * Close to clean up any access structure stuff.
     */
    sts = bs_close (bfap, MSFS_DO_VRELE);
    if (sts != EOK) {
        ADVFS_SAD1( "bs_invalidate_rsvd_access_struct: bs_close() failed", sts);
    }

    /*
     * Set the access structure's state to ACC_INVALID.
     */
    tbfap = bfap;
    bfap = grab_bsacc (domain->bfSetDirp, bfTag, FALSE, NULL);
    if (bfap != NULL) {
        MS_SMP_ASSERT(bfap->refCnt == 1);
        if ((tbfap == bfap) &&
            (lk_get_state (bfap->stateLk) == ACC_VALID)) {
            MS_SMP_ASSERT(bfap->dirtyBufList.length == 0);
            bfap->stateLk.state = ACC_INVALID;
        }
        DEC_REFCNT (bfap);
        mutex_unlock(&bfap->bfaLock);
    }

    return;
}  /* end bs_invalidate_rsvd_access_struct */


/*
 * bs_reclaim_cfs_rsvd_vn
 *
 * This function forces CFS to reclaim the CFS vnodes for reserved files that
 * are not in use anymore but are still in its vnode cache.  The vgone() call
 * forces CFS into a vnode VOP_RECLAIM.  This CFS vgone() will in turn call vrele
 * VOP_INACTIVATE the Advfs vnode, which will release the reserved files
 * access structure.  This has to be done so that a subsequent call to
 * bs_invalidate_rsvd_access_struct() will not panic on access struct still
 * held by CFS but not accessed by any users anymore.  The refCnt is still up
 * because CFS hasn't let go of the access struct yet.
 *
 * This arises from a previous user running one of our utilities like defragment
 * that accesses one or more of the reserved files to obtain extent information.
 * The utility was closing the file but CFS doesn't let go of the vnode (which
 * contains the access struct) until its cache decides to reclaim the vnode slot.
 * If defragment is followed by rmvol the refCnt>1 paniced the system.
 *
 * Removing a volume from the domain needs this function.
 * Switching the root tag directory also needs this function.
 * Switching the log also needs this function.
 */


statusT
bs_reclaim_cfs_rsvd_vn(
                       bfAccessT *bfap  /* in */
                       )
{
    statusT sts = EOK;
    struct vnode *vp = NULL;
    struct fileSetNode *fsp = NULL;
    char    *fnamep = NULL;
    struct nameidata *ndp = &u.u_nd;
    bfDomainIdT domainId;
    struct mountDirs {
        struct mountDirs *fsNext;
        char *mountpt;
    };
    struct mountDirs *fsMntp=NULL;
    struct mountDirs *fsMntHeadp=NULL;
    struct mountDirs *fsMntNextp=NULL;


    /* check to see if bfap is still on free list but can be reclaimed
     * because no users have it opened.  If this is the case it is still
     * hanging around because CFS doesn't immediately reclaim bfap's.
     * The vgone() below will force CFS to reclaim it though.
     * If the usecount is too high, someone still has it opened so we
     * should error out of here with a E_TOO_MANY_ACCESSORS.
     * The open can occur on one of the reserved files either through
     * mssh utility or a fopen() of a file like "/mnt/.tags/M-13"
     */
    domainId = bfap->dmnP->domainId;

    if( (bfap != NULL) && (bfap->refCnt > 1) ) {

        /* copy the mount points into a temporary list for use below.
         * Do this since if we used the fs list  directly a lock hierarchy violation
         * would occur below because namei() accesses the fsContext.file_lock.
         * Violation would be between fsContext.file_lock and FilesetLock.
         */
        FILESET_READ_LOCK(&FilesetLock );
        for (fsp = FilesetHead; fsp != NULL; fsp = fsp->fsNext) {
            if (BS_UID_EQL(domainId, fsp->dmnP->domainId)) {
                /* create a list of the mounted filesets in the same domain
                 * as volume being removed is in.
                 */
                fsMntp = (struct mountDirs *)ms_malloc(sizeof(struct mountDirs ));
                fsMntp->mountpt = (char *)ms_malloc(strlen(fsp->mountp->m_stat.f_mntonname) + 1);
                strcpy(fsMntp->mountpt, (char *) fsp->mountp->m_stat.f_mntonname);

                /*
                 * Link this fileset into the list of mounted filesets.
                 * Insert always at top of list.
                 */
                if(fsMntHeadp != NULL)
                    fsMntp->fsNext = fsMntHeadp;
                else
                    fsMntp->fsNext = NULL;  /* first time through */
                fsMntHeadp = fsMntp;

            }
        }
        FILESET_UNLOCK(&FilesetLock );

        fnamep = (char *) ms_malloc( MAX_MNT_PATHLEN+15+1 );
        /* directory on which mounted + "/mountpt/.tags/M" + tag */

        for (fsMntp = fsMntHeadp; fsMntp != NULL; fsMntp = fsMntp->fsNext) {
               /* check the mounted filesets in the same domain
                * as volume being removed is in.
                */
               sprintf(fnamep,"%s/.tags/M%d",
                       (char *) fsMntp->mountpt,
                       (int) bfap->tag.num);  /* build a path to this reserved file */

               ndp->ni_nameiop = LOOKUP | FOLLOW;
               ndp->ni_segflg = UIO_SYSSPACE; /* retrieve as from kernel */
               ndp->ni_dirp =  fnamep;
               if (!namei(ndp)) {

                   /* on ENOENT, continue on to next mounted filesystem
                    * in this domain since this fileset is not the one
                    * holding it opened.
                    * - otherwise -
                    * found it!, get the vnode and check the usecount
                    * the usecount tells us that if its 1, we are the only
                    * current accessor and its on the free list because
                    * it hasn't been reclaimed - and may not be for quite a
                    * while. In this case we force the reclaim with a vgone.
                    *
                    * If the usecount is > 1 someone from user mode
                    * has it opened
                    */
                   vp = ndp->ni_vp;
                   VN_LOCK(vp);
                   if (vp->v_usecount == 1) {
                      /* free the vnode/bfap with vgone() because it is
                       * just the namei above that has the v_usecount at 1.
                       * In the process of namei we increment the access struct
                       * and if the vnode is on the free list (but not
                       * currently accessed) we obtain the vnode.  This is
                       * the case of a cluster file system -CFS i.e.hanging
                       * onto the access struct by way of its vnode cache.
                       */

                       if(vp->v_tag == VT_CFS) {
                           vgone(vp, VX_NOSLEEP | VX_INACTIVE, NULL);
                       }

                       VN_UNLOCK(vp);
                       vrele(vp);

                   } else {
                      /*v_usecount indicates someone from user mode is holding it*/
                       VN_UNLOCK(vp);
                       vrele(vp);
                       sts = E_TOO_MANY_ACCESSORS;
                       goto do_cleanup;
                   }
               } /* namei found a vnode for this fileset */
        } /* end for all mounted filesets loop */
    } /* end if refCnt is too high, someone has it opened
       * or has recently closed it
       */


do_cleanup:

    fsMntp = fsMntHeadp;
    while(fsMntp != NULL) {
        fsMntNextp = fsMntp->fsNext;
        ms_free(fsMntp->mountpt);
        ms_free(fsMntp);
        fsMntp = fsMntNextp;
    }

    if(fnamep) ms_free(fnamep);

    return sts;

}  /* end bs_reclaim_cfs_rsvd_vn */


/*
 * bs_init_area
 *
 * Initialize area stuff.
 *
 * No error return.
 *
 * SMP
 *      Eventually called from bs_init(), which has mutual
 *      exclusion.
 */

void
bs_init_area()
{
    int i;
    bfAccessT *new_bfap;

    mutex_init3( &BfAccessFreeLock, 0, "BfAccessFreeLock" ,
                 ADVBfAccessFreeLock_lockinfo );

    /*
     * Initialize the free list and the closed list.
     */
    mutex_lock( &BfAccessFreeLock );
    FreeAcc.freeFwd = FreeAcc.freeBwd = (bfAccessT *)&FreeAcc;
    FreeAcc.len = 0;
    ClosedAcc.freeFwd = ClosedAcc.freeBwd = (bfAccessT *)&ClosedAcc;
    ClosedAcc.len = 0;
    ClosedAcc.saved_stats_len = 0;
    mutex_unlock( &BfAccessFreeLock );

    /*
     * Create some access structures.  We'll start with
     * twice the minimum desired.
     */
    for (i = 0; i < 2 * AdvfsMinFreeAccess; i++) {
        new_bfap = (bfAccessT *)ms_malloc(sizeof(bfAccessT));
        if (new_bfap == NULL)
            ADVFS_SAD0("bs_init_area: can't get space for access structures");
        init_access(new_bfap);
    }

#ifdef ADVFS_DEBUG
    FreeAcc.min = FreeAcc.len;
#endif
    BsAccessHashTbl =dyn_hashtable_init(BS_BFAH_INITIAL_SIZE,
                                        BS_BFAH_HASH_CHAIN_LENGTH,
                                        BS_BFAH_ELEMENTS_TO_BUCKETS,
                                        BS_BFAH_USECS_BETWEEN_SPLITS,
                                        offsetof(bfAccessT,hashlinks),
                                        ADVBfAccessHashChainLock_lockinfo,
                                        NULL);
    if (BsAccessHashTbl == NULL) {
        ADVFS_SAD0("bs_init_area: can't get space for hash table");
    }
}


/*
 * bs_map_bf
 *
 * Create the in-memory mapping structures for the
 * specified bitfile cell/vdi.
 *
 * The caller must have exclusive access to the bitfile's access structure and
 * no other thread can have a handle to the bitfile.
 *
 * Returns EBAD_DOMAIN_POINTER, EBAD_VDI (bad input parameters)
 * Returns E_BAD_MCELL_LINK_SEGMENT, EBMTR_NOT_FOUND, ENO_MORE_MEMORY
 * Returns possible error from bs_pinpg
 * If none of the above returns EOK
 *
 * SMP
 *      Exclusive access to a referenced access structure, no locking
 *      needed.
 */

statusT
bs_map_bf(
          bfAccessT* bfap,         /* in/out - ptr to bitfile's access struct */
          uint32T options,         /* in - options flags (see bs_access.h) */
          struct mount *mp         /* in - mount point */
          )
{
    struct vd* vdp;
    struct domain* dmnP;
    bfPageRefHT pgref;
    statusT sts;
    struct bsMPg* bmtp;
    struct bsMC* mcp;
    bsBfAttrT* bfattrp;
    int pgRefed = FALSE;

    /*-----------------------------------------------------------------------*/

/*    MS_SMP_ASSERT(lk_get_state(bfap->stateLk) == ACC_INIT_TRANS);
      will not work when called by extend_bmt_redo_opx during recovery... */

    dmnP = bfap->dmnP;
    vdp = VD_HTOP(bfap->primVdIndex, dmnP);

    if (bfap->mapped && !(options & BS_REMAP)) {
        /*
         * The bitfile is already mapped into memory and the caller
         * doesn't want us to remap the bitfile.
         */
        return EOK;
    }

    if ( RBMT_THERE(dmnP) ) {
        if (vdp->rbmtp == NULL) {
            /*
             * special case - this is the RBMT we are mapping during
             * initial domain activation (that is why the RBMT bfap is NULL),
             * so use the special buffer it got read into.
             */
            if ((bfap->primMCId.cell != 0) && (bfap->primMCId.page != 0)) {
                ADVFS_SAD0( "bs_map_bf: RBMT not mapped" );
            }

            bmtp = get_bmt_pgptr(dmnP);
            pgref = NilBfPageRefH;

        } else if (BS_BFTAG_RSVD(bfap->tag)) {
            /* access the reserved metadata bitfile page */

            sts = bmt_refpg( &pgref,
                             (void*)&bmtp,
                             vdp->rbmtp,
                             bfap->primMCId.page,
                             BS_NIL );
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }

            pgRefed = TRUE;

        }
        else {
            /* access the metadata bitfile page */

            sts =  bmt_refpg( &pgref,
                              (void*)&bmtp,
                              vdp->bmtp,
                              bfap->primMCId.page,
                              BS_NIL );
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }

            pgRefed = TRUE;
        }
    }
    else {
        if (vdp->bmtp == NULL) {
            /*
             * special case - this is the BMT we are mapping during
             * initial domain activation (that is why the BMT bfap is NULL),
             * so use the special buffer it got read into.
             */

            if ((bfap->primMCId.cell != 0) && (bfap->primMCId.page != 0)) {
                ADVFS_SAD0( "bs_map_bf: BMT not mapped" );
            }

            bmtp = get_bmt_pgptr(dmnP);
            pgref = NilBfPageRefH;

        } else {
            /* access the metadata bitfile page */

            sts = bs_refpg( &pgref,
                            (void*)&bmtp,
                            vdp->bmtp,
                            bfap->primMCId.page,
                            BS_NIL );
            if (sts != EOK) {
                RAISE_EXCEPTION( sts );
            }

            pgRefed = TRUE;
        }
    }

    mcp = &bmtp->bsMCA[bfap->primMCId.cell];
    
    if ( bfap->dmnP->state != BFD_ACTIVATED ) {
        /*
         * During recovery the tag sequence number may not match.
         * We should not domain_panic instead return the error.
         */
        if (!BS_BFTAG_EQL(mcp->tag, bfap->tag))
            RAISE_EXCEPTION( EBAD_TAG );
    } else {
        /* 
         * Once the domain is activated we can do a full check of 
         * the mcell. 
         */
        if ( check_mcell_hdr(mcp, bfap) != EOK )
            RAISE_EXCEPTION(E_BAD_BMT);
    }

    if (mcp->linkSegment != 0) {
        RAISE_EXCEPTION( E_BAD_MCELL_LINK_SEGMENT );
    }

    /* scan the primary mcell for the bitfile's attributes */

    bfattrp = (bsBfAttrT*)bmtr_find( mcp, BSR_ATTR, bfap->dmnP);
    if ( !bfattrp ) {
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    /*
     * In general, if the file's state is invalid or deleting,
     * return an appropriate error.  Exceptions are if the log 
     * isn't fully initialized yet, we're doing a failover
     * or we are in recovery. 
     *
     * If we get here in the cluster 
     * failover case, then we are attempting to reattach the deleting
     * file. In other words when the cluster failed the file was
     * marked for deletion but still open. We need to attatch it 
     * to the new node as though it is still openned and marked
     * for deletion.
     *
     * If we get here with the file marked deleting and we are in 
     * recovery then we do not want to return an error. We are being
     * called from the image redo and need to allow this to occur on 
     * this file, even if it is going to be removed. This is because
     * we may end up failing the transcation and calling the undo routine
     * in which case the file being removed better have the image redo 
     * applied. Or if we will ended up postponing the deletion and 
     * reattatching the file to a new cluster node then once again the
     * image redo must be applied. Note that the only files that should
     * ever arrive here in the BSRA_DELETING and recovery case are either
     * (currently) a directory, and index or a data logging file.
     */    
    if (((bfattrp->state == BSRA_INVALID) &&
         (!BS_BFTAG_EQL(bfap->tag, bfap->dmnP->ftxLogTag))) ||
        ((bfattrp->state == BSRA_DELETING) &&
         !(clu_is_ready() && (options & BS_FAILOVER)) &&
          (bfap->dmnP->state == BFD_ACTIVATED) &&
          !(options & BS_ON_DDL)))
    {
        RAISE_EXCEPTION( ENO_SUCH_TAG );
    }

    bfap->bfPageSz       = bfattrp->bfPgSz;
    bfap->reqServices    = bfattrp->cl.reqServices;
    bfap->optServices    = bfattrp->cl.optServices;
    bfap->bfState        = bfattrp->state;
    bfap->transitionId   = bfattrp->transitionId;
    bfap->cloneId        = bfattrp->cloneId;
    bfap->cloneCnt       = bfattrp->cloneCnt;
    bfap->maxClonePgs    = bfattrp->maxClonePgs;
    bfap->outOfSyncClone = bfattrp->outOfSyncClone;
    bfap->deleteWithClone= bfattrp->deleteWithClone;

    /*
     * If we are opening a file that lives in a fileset that has been
     * mounted with the -o adl option and the file has an on-disk
     * dataSafety value of BFD_NIL, set the in-memory-only dataSafety
     * value to BFD_FTX_AGENT_TEMPORARY.  Otherwise, use the dataSafety
     * value stored in the on-disk metadata.
     */
    if ((mp) && (mp->m_flag & M_ADL) && (bfattrp->cl.dataSafety == BFD_NIL)) {
        bfap->dataSafety = BFD_FTX_AGENT_TEMPORARY;
    }
    else {
        bfap->dataSafety = bfattrp->cl.dataSafety;
    }

    /*
     * If this file is metadata or a data logging file,
     * inform the UBC that its pages should not be flushed.
     */
    if ((bfap->dataSafety == BFD_FTX_AGENT) ||
        (bfap->dataSafety == BFD_FTX_AGENT_TEMPORARY) ||
        (bfap->dataSafety == BFD_NO_NWR)) {
        UBC_OBJECT_PREVENT_FLUSH(bfap->bfObj);
    }

    /*
     * Setup extent map.
     */

    sts = x_create_inmem_xtnt_map( bfap, mcp);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    bfap->mapped = TRUE;

    if (pgRefed) {
        bs_derefpg(pgref, BS_CACHE_IT);
    }

    return EOK;

HANDLE_EXCEPTION:
    if (pgRefed) {
        bs_derefpg(pgref, BS_CACHE_IT);
    }

    return sts;
}


/**********************************************************************
********************  The public routines ****************************
**********************************************************************/

/*
 * bs_insmntque() - inserts AdvFS vnode on mount queue
 *
 * History:     Originally part of bs_access, pulled out to synchronize
 *              with the setting of vnode type & specinfo pointer.
 * Assumptions: No access locks held by caller.  Vm_object is allocated
 *              somewhere else (like bs_access()).
 */

void
bs_insmntque(
             ftxHT ftxH,
             bfAccessT *bfap,
             struct mount *mp
             )
{
    unLkActionT unlkAction = UNLK_NEITHER;
    struct vnode *vp = bfap->bfVp;
    bfSetT *bfSetp = bfap->bfSetp;


    /*
     * Exclude reserved bit files
     */
    if ( (vp->v_mount != mp) &&
          bfSetp != bfSetp->dmnP->bfSetDirp )
    {
        enum vtype tsave;
        lkStatesT bfaccState;

        /*
         * Synchronize with other bs_insmntque's & open/close using
         * ACC_INIT_TRANS (this comment for entire block)
         */
        mutex_lock(&bfap->bfaLock);

        /*
         * Ignore ACC_FTX_TRANS if we are the creator
         */
        if ( (bfap->bfState == BSRA_CREATING) &&
             (ftxH.hndl != 0) &&
             (bfap->transitionId == get_ftx_id(ftxH)) )
        {
            lk_wait_while(&bfap->stateLk, &bfap->bfaLock, ACC_INIT_TRANS);
        } else {
            do {
                lk_wait_while( &bfap->stateLk, &bfap->bfaLock, ACC_FTX_TRANS );
                lk_wait_while( &bfap->stateLk, &bfap->bfaLock, ACC_INIT_TRANS );
            } while ((lk_get_state( bfap->stateLk ) == ACC_FTX_TRANS) ||
                     (lk_get_state( bfap->stateLk ) == ACC_INIT_TRANS));
        }

        bfaccState = lk_get_state( bfap->stateLk);
        (void)lk_set_state( &bfap->stateLk, ACC_INIT_TRANS );
        mutex_unlock( &bfap->bfaLock );

        /*
         * insmntque() gets a vm_object if the type is VREG, so trick it
         */
        tsave = vp->v_type;
        vp->v_type = VNON;
        insmntque( vp, mp, 0 );
        vp->v_type = tsave;

        /*
         * Restore previous state
         */
        mutex_lock( &bfap->bfaLock );
        if ( bfaccState != lk_get_state( bfap->stateLk) )
        {
            unlkAction = lk_set_state( &bfap->stateLk, bfaccState );
            lk_signal( unlkAction, &bfap->stateLk );
        }
        mutex_unlock( &bfap->bfaLock );
    }
}


/*
 * bfm_open_ms - bitfile metadata (bitfile) open routine to open
 * reserved bitfiles.
 */

statusT
bfm_open_ms(
            bfAccessT **outbfap,        /* out - access structure pointer */
            domainT* dmnP,              /* in - domain pointer */
            int bfDDisk,                /* in - domain disk index */
            bfdBfMetaT bfMIndex         /* in - metadata bitfile index */
            )
{
    bfTagT tag;
    statusT sts;
    struct vnode *nullvp = NULL;

    BS_BFTAG_RSVD_INIT(tag, bfDDisk, bfMIndex);

    /*
     *  Open the reserved file without a vnode.  We don't
     *  need one.  If the reserved file is opened via the .tags
     *  directory a new access structure and vnode will be assigned.
     *  This vnode will be associated with the filesets mount point
     *  and will be like a regular file.  If a reserved file is
     *  opened and the fileset is being unmounted the umount call
     *  will fail because the fileset will be busy.  
     */

    sts = bs_access( outbfap, tag, dmnP->bfSetDirp, FtxNilFtxH,
                          0, NULLMT, &nullvp );
    /*
     * The BMT mcellList_lk must be in a lower spot in the hierarchy,
     * since the "normal" progression is bfAccessT.mcellList_lk to
     * vdT->mcell_lk. We have to have a seperate lockinfo for the
     * mcellList_lk for the BMT access structure because a regular
     * mcell may indirectly extend the BMT which requires the BMT
     * access mcellList_lk after getting vdT->mcell_lk.
     */
    if (sts == EOK) {
        if ( RBMT_THERE(dmnP) ) {
            if (bfMIndex == BFM_BMT  ||
                bfMIndex == BFM_RBMT) {
                lock_terminate( &(*outbfap)->mcellList_lk.lock );
                lock_setup( &(*outbfap)->mcellList_lk.lock,
                    ADV_BMT_bfAccessT_mcellList_lk_info,
                    TRUE );
                lock_terminate( &(*outbfap)->xtntMap_lk.lock );
                lock_setup( &(*outbfap)->xtntMap_lk.lock,
                    ADV_BMT_bfAccessT_xtntMap_lk_info,
                    TRUE );
            }
        } else {
            if (bfMIndex == BFM_BMT_V3) {
                lock_terminate( &(*outbfap)->mcellList_lk.lock );
                lock_setup( &(*outbfap)->mcellList_lk.lock,
                    ADV_BMT_bfAccessT_mcellList_lk_info,
                    TRUE );
                lock_terminate( &(*outbfap)->xtntMap_lk.lock );
                lock_setup( &(*outbfap)->xtntMap_lk.lock,
                    ADV_BMT_bfAccessT_xtntMap_lk_info,
                    TRUE );
            }
        }
    }
    return(sts);
}


/*
 * bs_access
 *
 * Routine to open a bitfile and its clone
 *
 * Open the specified bitfile in the specified bitfile set.  All
 * necessary data structures are located and initialized from on-disk
 * structures, if necessary, to allow further page data access and
 * modification functions to be performed.
 *
 * Returns status from bs_access_one routine call.
 */

statusT
bs_access(
          bfAccessT **outbfap,     /* out - access structure pointer */
          bfTagT tag,              /* in - tag of bf to access */
          bfSetT *bfSetp,          /* in - BF-set descriptor pointer */
          ftxHT ftxH,              /* in - ftx handle */
          uint32T options,         /* in - options flags */
          struct mount *mp,        /* in - fs mount pt */
          struct vnode **vp        /* in/out - from bs_access_one */
          )
{
    statusT sts;
    bfAccessT *origbfap;
    struct vnode *nullvp = NULL;

    if (bfSetp->cloneId == BS_BFSET_ORIG) {
        /*
         * We are opening the original bitfile.  Defer opening the
         * clone until we actually need it (like in bs_cow()).
         */

        sts = bs_access_one( outbfap, tag, bfSetp, ftxH, options, mp, 
                             vp, NULL );
        return(sts);
    }

    /*
     * We are opening the clone bitfile. Open the original also.
     */

    sts = bs_access_one( &origbfap, tag, bfSetp->origSetp, ftxH,
                                0, NULLMT, &nullvp, NULL );

    if (sts != EOK) {
        ms_uaprintf( "WARNING: advfs cannot open the original file; %s\n", BSERRMSG(sts) );
        ms_uaprintf( "WARNING: clone fileset is out of sync with the original fileset \n" );
        return sts;
    }

    sts = bs_access_one( outbfap, tag, bfSetp, ftxH,
                              options, mp, vp, origbfap );
    if (sts != EOK) {
        (void) bs_close_one( origbfap, 0, ftxH );
        return sts;
    }

    return sts;
}


/*
 * bs_access_one - Main routine to access a single bitfile.  This
 * is only called from bs_access which deals with clones.
 *
 * Returns ENO_SUCH_TAG, EBAD_DOMAIN_POINTER, EHAND_OVH or EOK,
 * or various status returns from tagdir_lookup and mask_diskbf.
 */

statusT
bs_access_one(
              bfAccessT **outbfap, /* out - access structure pointer */
              bfTagT tag,          /* in - tag of bf to access */
              bfSetT *bfSetp,      /* in - BF-set descriptor pointer */
              ftxHT ftxH,          /* in - ftx handle */
              uint32T options,     /* in - options flags */
              struct mount *mp,    /* in - fs mount queue */
              struct vnode **fsvp, /* in/out - <pre>allocated vnode */
              bfAccessT *origBfap  /* in - Orig access (clone open) */
              )
{
    bfAccessT *bfap;
    bfMCIdT bfMCId;
    vdIndexT vdIndex;
    statusT sts;
    unLkActionT unlkAction = UNLK_NEITHER;
    struct vnode *vp = NULL,
                 *clu_clone_vnode_to_vrele = NULL;
    lkStatesT bfaccState;
    int another_fs_open = FALSE;
    vdT *ddlVd;
    int delFlag;
    domainT *dmnP;
    uint32T flags;
    int got_clu_clone_vnode = FALSE;
    int did_vp_vrele = FALSE;
    extern struct vfs_ubcops msfs_ubcops;
    extern int bypass_dealloc_code;
    struct bfNode *bnp;


retry_access:

    if ( !(options & BF_OP_HAVE_VNODE) ) {
        *fsvp = NULL;
    }
    dmnP = bfSetp->dmnP;

    if (TrFlags & trAccess) {
        trace_hdr();
    }

    if (ftxH.hndl == 0) {
        if (dmnP == NULL) {
            return EBAD_DOMAIN_POINTER;
        }
    } else {
        if (dmnP != ftxH.dmnP) {
            ADVFS_SAD0("bs_access: domain different from ftx domain");
        }
    }

    /* Don't bother looking for tags that cannot possibly exist! */

    if (!BS_BFTAG_RSVD(tag) && !BS_BFTAG_REG(tag)) {
        return ENO_SUCH_TAG;
    }

    /*
     * Check if tag's access entry  is already present in table.
     * If refCnt was zero, we are re-using a cached entry.
     *
     * SMP
     *  grab_bsacc() returns with bfap->bfaLock held
     */

    bfap = grab_bsacc(bfSetp, tag, FALSE, options);
    if (bfap == NULL) {
        return EHANDLE_OVF;

    /* This else case ensures we only return ACC_VALID bfaps
     * via the BF_OP_INMEM_ONLY option.
     */
    } else if ((options & BF_OP_INMEM_ONLY) &&
               (lk_get_state(bfap->stateLk) != ACC_VALID)) {
        return EINVALID_HANDLE;
    }

    vp = NULL;   /* assume we don't get a new one */

retry_clu_clone_access:

    bfaccState = lk_get_state( bfap->stateLk );

    if ((bfaccState == ACC_INIT_TRANS) ||
            (bfap->hashlinks.dh_links.dh_next == NULL)) {
       /*
        * This should only happen if two threads are racing to
        * open the same clone file.  Since we're not sure whether
        * or not the thread we're racing with (the one that set
        * the access structure into state ACC_INIT_TRANS) will
        * succeed in opening the file or not, the safest thing to
        * do is to start over.  That's because it may be that
        * the thread we're racing with will end up exiting with
        * an error, removing the access structure we currently
        * are examining from the hash table.  If the other thread
        * succeeds, and chances are it will, then it will setup
        * a vnode to use so we'll vrele() the one we preallocated.
        */
        MS_SMP_ASSERT(got_clu_clone_vnode == TRUE);
        DEC_REFCNT(bfap);
        mutex_unlock(&bfap->bfaLock);
        vrele(vp);

        got_clu_clone_vnode = FALSE;
        goto retry_access;
    }

    if (bfaccState == ACC_VALID) {

        /*
         * If we are trying to open a clone file in a mounted
         * filesystem (we want a vnode) and the clone file was
         * previously opened, the access structure for the clone 
         * file may still be valid but there may be no vnode 
         * attached to it.  Generally, this isn't a problem.  
         * But in a cluster, we need to allocate a vnode up front 
         * before putting the access structure into the ACC_INIT_TRANS 
         * state in order to avoid deadlock So, in this case, leave
         * the refCnt up on the access structure so it doesn't get
         * recycled, leave it in the hash table, get a vnode, and
         * start over, vnode in hand.  If another thread tries to
         * open the clone file before we start over and finish the
         * job, then we will throw away the newly-allocated vnode.
         * the symmetric situation can also
         * lead to problems: open and then close the clone releases
         * the CFS vnode but the access structures stick around. If
         * we then try to open the original and end up using the just
         * release vnode, we end up deadlocking the thread.
         *
         * Third time is the charm? Another possibility of getting this
         * deadlock is going after the vnode of a directory access while
         * having the access of its index file on hand.
         * 
         * Metadata opens through .tags do not have this problem. The
         * real bfap does not have a vnode, but the shadow one
         * does. In order to be able to go through .tags, a fileset
         * has to be mounted, i.e. the domain has to be activated.The
         * only files that can get us into trouble through this
         * mechanism are reserved files and fileset tag
         * directories. With the exception of the misc file, access
         * structures for the other reserved files are not recycled
         * while the domain is active: the files are kept open. So the
         * only dangerous ones are the misc file and any tag directories
         * for filesets that are not mounted (we can get to them through
         * .tags of a mounted fileset even if they themselves are not
         * mounted). If we could ever open the file corresponding to the
         * real bfap and would want to attach a vnode to it, then the proble
         * could arise. But we never, ever attach a vnode to the real bfap
         * of a reserved or tag directory file.
         *
         * In general, if there is a possibility of having related
         * access structures (in the sense that when we close one, we
         * have to close the other one as well) and one of them has a
         * vnode, while the other does not, there is the possibility
         * of this deadlock: if we go after the file corresponding to
         * the vnode-less bfap, grab_bsacc() will find it and and give
         * it to us. If then we have to go get a vnode for it, we
         * might end up with the CFS vnode of the other bfap, which
         * would cause the thread to deadlock. The solution is to
         * check here and preallocate a vnode, before going after the
         * access structure.  
         */
        if (clu_is_ready() && 
           (
               (bfSetp->cloneId != BS_BFSET_ORIG) ||
               (bfSetp->numClones > 0) ||
               (IDX_INDEXING_ENABLED(bfap) && IDX_FILE_IS_INDEX(bfap))
           ) &&
           (options & BF_OP_GET_VNODE) &&
           (bfap->bfVp == NULL) &&
           (!got_clu_clone_vnode)) {
            mutex_unlock(&bfap->bfaLock);
            sts = getnewvnode(VT_MSFS, &msfs_vnodeops, &vp);
            mutex_lock(&bfap->bfaLock);
            MS_SMP_ASSERT(bfap->refCnt > 0);
            MS_SMP_ASSERT(BS_BFTAG_EQL(tag, bfap->tag));
            MS_SMP_ASSERT(bfSetp == bfap->bfSetp);
            if (sts != EOK) {
                goto err_deref;
            }
            bnp = (struct bfNode *)&vp->v_data[0];
            bnp->fsContextp = NULL;
            bnp->accessp = NULL;
            got_clu_clone_vnode = TRUE;
            goto retry_clu_clone_access;
        }

        if (bfap->bfState == BSRA_DELETING) {
            /*
             * We arrive here when the bitfile has been deleted but one or more
             * threads still has it accessed.  When the last thread closes the
             * bitfile, the close code deallocates the bitfile's storage.
             */
            if ((options & BF_OP_IGNORE_DEL) == 0) {
                /*
                 * This is a normal access.
                 */
                sts = ENO_SUCH_TAG;
                goto err_deref;
            }
            /*
             * A thread is accessing the bitfile so that it can move its
             * metadata to another disk or some other operation.  Just fall
             * thru.  (See callers of bs_close().)
             */
        } else {
            if (bfap->bfState == BSRA_INVALID) {
                /*
                 * We arrive here when the bitfile has been deleted.  The close
                 * code sets the bitfile state from BSRA_DELETING to BSRA_INVALD
                 * after it deallocates the bitfile's storage.
                 *
                 * NOTE: The state is only set in the access structure as the
                 * bitfile's attributes record no longer exists.
                 */
                sts = ENO_SUCH_TAG;
                goto err_deref;
            }
        }

        /*
         * Terrific!!  We've found an existing bitfile access
         * object, it's valid, and there is nothing left to do
         * other than return victorious right now, which is
         * accomplished by dropping through past the rest of these
         * "else ifs" where we will find the bitfile state valid.
         *
         * SMP
         *      Still holding bfap->BfaLock
         */

    } else if (bfaccState == ACC_INVALID) {

        /*
         * The bfAccess struct is invalid.  Initialize it.
         */

        /*
         * If we're in a cluster and we're trying to access a clone
         * file and we don't yet have a vnode, we need to get a 
         * vnode prior to setting the state of the access structure
         * to ACC_INIT_TRANS in order to avoid a deadlock with another
         * thread which may have started a transaction and is trying
         * to open the same clone file and would block on ACC_INIT_TRANS.
         * So, in this case, leave
         * the refCnt up on the access structure so it doesn't get
         * recycled, leave it in the hash table, get a vnode, and
         * start over, vnode in hand.  If another thread tries to
         * open the clone file before we start over and finish the
         * job, then we will throw away the newly-allocated vnode.
         * the symmetric situation can also
         * lead to problems: open and then close the clone releases
         * the CFS vnode but the access structures stick around. If
         * we then try to open the original and end up using the just
         * release vnode, we end up deadlocking the thread.
         *
         * Third time is the charm? Another possibility of getting this
         * deadlock is going after the vnode of a directory access while
         * having the access of its index file on hand.
         * 
         * Metadata opens through .tags do not have this problem. The
         * real bfap does not have a vnode, but the shadow one
         * does. In order to be able to go through .tags, a fileset
         * has to be mounted, i.e. the domain has to be activated.The
         * only files that can get us into trouble through this
         * mechanism are reserved files and fileset tag
         * directories. With the exception of the misc file, access
         * structures for the other reserved files are not recycled
         * while the domain is active: the files are kept open. So the
         * only dangerous ones are the misc file and any tag directories
         * for filesets that are not mounted (we can get to them through
         * .tags of a mounted fileset even if they themselves are not
         * mounted). If we could ever open the file corresponding to the
         * real bfap and would want to attach a vnode to it, then the proble
         * could arise. But we never, ever attach a vnode to the real bfap
         * of a reserved or tag directory file.
         *
         * In general, if there is a possibility of having related
         * access structures (in the sense that when we close one, we
         * have to close the other one as well) and one of them has a
         * vnode, while the other does not, there is the possibility
         * of this deadlock: if we go after the file corresponding to
         * the vnode-less bfap, grab_bsacc() will find it and and give
         * it to us. If then we have to go get a vnode for it, we
         * might end up with the CFS vnode of the other bfap, which
         * would cause the thread to deadlock. The solution is to
         * check here and preallocate a vnode, before going after the
         * access structure.  
         */
        if (clu_is_ready() && 
           (
               (bfSetp->cloneId != BS_BFSET_ORIG) ||
               (bfSetp->numClones > 0) ||
               (IDX_INDEXING_ENABLED(bfap) && IDX_FILE_IS_INDEX(bfap))
           ) &&
           (options & BF_OP_GET_VNODE) &&
           (!got_clu_clone_vnode)) {
            mutex_unlock(&bfap->bfaLock);
            sts = getnewvnode(VT_MSFS, &msfs_vnodeops, &vp);
            mutex_lock(&bfap->bfaLock);
            MS_SMP_ASSERT(bfap->refCnt > 0);
            MS_SMP_ASSERT(BS_BFTAG_EQL(tag, bfap->tag));
            MS_SMP_ASSERT(bfSetp == bfap->bfSetp);
            if (sts != EOK) {
                goto err_deref;
            }
            bnp = (struct bfNode *)&vp->v_data[0];
            bnp->fsContextp = NULL;
            bnp->accessp = NULL;
            got_clu_clone_vnode = TRUE;
            goto retry_clu_clone_access;
        }

        /*
         * Mark entry as "in transition" so other threads will block
         * trying to use it.  This allows us to release the mutex while
         * (potentially) doing disk i/o to access the on-disk
         * metadata.
         */

        unlkAction = lk_set_state( &bfap->stateLk, ACC_INIT_TRANS );
        mutex_unlock(&bfap->bfaLock);

        /*
         * If we don't already have one, get a new vnode; because the access 
         * structure is invalid, there cannot already be one attached to it.  
         * Setting "vp" tells the exit path not to do another vget on 
         * this vnode.
         */
        MS_SMP_ASSERT(bfap->bfObj == NULL);
        /*
         * Don't allocate object for shadow bfap.
         */
        if (!BS_BFTAG_PSEUDO(tag)) {
            bfap->bfObj = ubc_object_allocate((vfs_private_t)&bfap,
                                              &msfs_ubcops,
                                              0);
            MS_SMP_ASSERT(bfap->bfObj);

            /*
             * Give the ref count an extra bump to guarantee no one
             * else can free it.
             */
            bfap->bfObj->vu_object.ob_ref_count += 1;
        }

        /*
         * vp will not be NULL here if got_clu_clone_vnode is TRUE.
         */
        if (vp == NULL) {
            vp = *fsvp;
        }
        if ( options & (BF_OP_GET_VNODE | BF_OP_HAVE_VNODE) ) {
            sts = get_n_setup_new_vnode( bfap, bfSetp, &vp );
            if ( sts != EOK ) {
                /*
                 * Could not get vnode.  Return system error.
                 */
                goto err_setinvalid;
            }

            /*
             * If we preallocated a vnode for a clone file in a cluster,
             * it has now been neatly attached to the access structure
             * and we should follow the normal code paths for any cleanup.
             * Set got_clu_clone_vnode to FALSE so we don't do any
             * ususual cleanup processing.
             */
            got_clu_clone_vnode = FALSE;
        }

        /*
         * Pseudo-tag does not exist on disk.
         */
        if ( BS_BFTAG_PSEUDO(tag)) {
            bfap->primVdIndex   = 0;
            bfap->primMCId.cell = 0;
            bfap->primMCId.page = 0;

            bfap->bfPageSz      = 0;
            bfap->reqServices   = 0;
            bfap->optServices   = 0;
            bfap->nextPage      = 0;
            bfap->bfState       = BSRA_VALID;
        }
        else {

            /* Get tag's mcell info. (potentially blocks for I/O) */

            sts = tagdir_lookup( bfSetp,
                                 &tag,
                                 &bfMCId,
                                 &vdIndex );

            /* During CFS failover, check the DDL for unlinked files */

            if (sts == ENO_SUCH_TAG) {
                if ( clu_is_ready() && mp && (mp->m_flag & M_FAILOVER) ||
                     options & BF_OP_FIND_ON_DDL )
                {
                    sts = find_del_entry (
                                          dmnP,
                                          bfSetp->dirTag,
                                          tag,
                                          &ddlVd,
                                          &bfMCId,
                                          &delFlag
                                          );
                }
                if (sts == EOK) {
                    DDLACTIVE_UNLOCK(&(ddlVd->ddlActiveLk))
                } else goto err_vrele;  /* typically "not found" */
                vdIndex = ddlVd->vdIndex;
            }

            if (vdIndex == 0) {
                /* this normally only happens if a user opens .tags/-2 */
                sts = ENO_SUCH_TAG;
                goto err_vrele;
            }

            if (sts != EOK) {
                goto err_vrele;
            }

            /*
             * Test for trying to access a clone who's fset is being
             * deleted and who has never been cowed.  If so, no need to 
             * bs_map_bf() it since the fileset is going away.
             */

            if ( (origBfap)                                &&
                 (bfSetp->state           == BFS_DELETING) &&
                 (origBfap->primVdIndex   == vdIndex)      &&
                 (origBfap->primMCId.cell == bfMCId.cell)  &&
                 (origBfap->primMCId.page == bfMCId.page)    ) {

                sts = ENO_SUCH_TAG;
                goto err_vrele;
            }
            
            /*
             * No need to acquire the move metadata lock to initialize "primVdIndex"
             * and "primMCId" because no other thread can have a handle to this
             * bitfile.
             */
            bfap->primVdIndex = vdIndex;
            bfap->primMCId    = bfMCId;
            bfap->bfPageSz    = 0;
            bfap->reqServices = 0;
            bfap->optServices = 0;
            bfap->nextPage    = 0;
            bfap->bfState     = BSRA_INVALID;

            flags = BS_MAP_DEF;
            if (mp && (mp->m_flag & M_FAILOVER)) flags |= BS_FAILOVER;
            if (options & BF_OP_FIND_ON_DDL) flags |= BS_ON_DDL;
            if ((sts = bs_map_bf(bfap, flags, mp)) != EOK) {
                goto err_vrele;
            }

            if ( TestXtntsFlg ) {
                test_xtnt(bfap);
            }
        }

        mutex_lock(&bfap->bfaLock);

        /*
         * At this point, the bfAccess structure has been successfully
         * initialized.  After this it will move to some state other
         * than INIT_TRANS.
         */

        /* end of ACC_INVALID code leg */
    } else if ( bfaccState != ACC_FTX_TRANS ) {
        ADVFS_SAD1("bs_access_one: bad access state", bfaccState );
    }

    /*
     * At this point actions related to the access structure state are
     * done.  We must examine the bitfile state to figure out
     * what to do next.
     */

    if (bfap->bfState == BSRA_VALID ) {
        if ( bfaccState == ACC_INVALID ) {
            /*
             * It is currently ACC_INVALID which means we've
             * init'd it and have a vnode.  Set bfaccState to make it
             * ACC_VALID when we drop through.
             */
            bfaccState = ACC_VALID;
        }
    } else if (bfap->bfState == BSRA_DELETING) {
        ;
    } else {
        /* 
         * Must be BSRA_CREATING.
         *
         * The bitfile is neither VALID nor DELETING, therefore it
         * must be in one of the ftx transition states.
         */

        /*
         * Special processing if this is the log file and it is not
         * yet fully initialized.
         */
        if ((ftxH.hndl == FtxNilFtxH.hndl) &&
            (BS_BFTAG_EQL(bfap->tag, bfap->dmnP->ftxLogTag))) {
            bfaccState = ACC_VALID;
        }
        else if ((ftxH.hndl != 0) &&
                 (bfap->transitionId == get_ftx_id(ftxH))) {

            /*
             * The caller has the same ftx id as the ftx that put
             * this bitfile into ftx transition, so allow the
             * caller to see the current state of the bitfile.
             */

            /*
             * This must be the BSRA_CREATING state because the other
             * 3 possible states have all been checked for!
             * It could already be in FTX_TRANS as the result
             * of another thread, also blocked on INIT_TRANS, doing this
             * before us, but that doesn't change the fact that it is
             * still in FTX_TRANS until the create/delete ftx changes it.
             */

            if ( bfaccState == ACC_INVALID ) {
                /*
                 * It is currently ACC_INVALID which means we've
                 * init'd it and have a vnode.
                 */
                bfaccState = ACC_FTX_TRANS;
            }
            /*
             * Do nothing else and drop through!  This bitfile was
             * created by this ftx and it's okay to see it!
             */
        } else {

            /*
             * The caller has a different ftx id than the ftx that
             * put this bitfile into ftx transition.  Block on the
             * condition variable until we exit the ftx transition
             * state.
             *
             * If we were the thread that put this into INIT_TRANS, we
             * must first wake any waiters.  One of the waiters may be
             * the create_rtdn_opx routine that is waiting to change
             * the state to VALID!!
             */

            unlkAction = lk_set_state( &bfap->stateLk, ACC_FTX_TRANS );

            lk_signal( unlkAction, &bfap->stateLk );
            unlkAction = UNLK_NEITHER;

            do {
                lk_wait_while( &bfap->stateLk, &bfap->bfaLock, ACC_FTX_TRANS );
                lk_wait_while( &bfap->stateLk, &bfap->bfaLock, ACC_INIT_TRANS );

            } while ( lk_get_state( bfap->stateLk ) == ACC_FTX_TRANS ||
                      lk_get_state( bfap->stateLk ) == ACC_INIT_TRANS );

            if ( lk_get_state( bfap->stateLk ) == ACC_INVALID) {
                sts = E_TAG_EXISTS;
                if ( vp ) {
                    /*
                     * We got here after setting up a new vnode, so
                     * exit through the error path to release it.
                     */
                    mutex_unlock(&bfap->bfaLock);
                    goto err_vrele;
                } else {
                    goto err_deref;
                }

            } else if ( lk_get_state( bfap->stateLk ) != ACC_VALID) {
                ADVFS_SAD1( "ftbs_access: invalid state", bfaccState );
            }
            bfaccState = ACC_VALID;
        }
    }
    /*
     * Everybody exits through this leg of code.  Deal with the vnode,
     * if necessary, and signal any access structure state change, if
     * necessary.
     */
    if (!vp || got_clu_clone_vnode) {

        /* 
         * Note: if got_clu_clone_vnode is TRUE here, it means
         *       we preallocated a vnode but didn't attach it
         *       to the access structure.  We still might or
         *       we might release it.
         */

        if ( options & (BF_OP_GET_VNODE | BF_OP_HAVE_VNODE) ) {
            if (!bfap->bfVp) {
                (void)lk_set_state( &bfap->stateLk, ACC_INIT_TRANS );
                mutex_unlock(&bfap->bfaLock);

                /*
                 * The access structure has no vnode attached to it.
                 * If we preallocated one for a clone file, it's
                 * already in vp.  If a vnode was preallocated before
                 * calling this routine, it's in fsvp.  Otherwise, we
                 * will allocate a new vnode.
                 */
                if (!got_clu_clone_vnode) {
                    vp = *fsvp;
                }
                sts = get_n_setup_new_vnode( bfap, bfSetp, &vp );
		mutex_lock(&bfap->bfaLock);
                if ( sts != EOK ) {
                    /*
                     * Could not get vnode.  Return system error.
                     */
                    unlkAction = lk_set_state( &bfap->stateLk, bfaccState );
                    goto err_deref;
                }

            } else { /* if (!bfap->bfVp) */
                int vret;
                bfSetIdT bfSetId;
                struct bfNode *bnp;

                /*
                 * If we have a preallocated vnode for a clone but
                 * another thread beat us to it and attached a vnode
                 * to the access structure, we will need to release 
                 * the preallocated vnode.  Don't do the vrele() here,
                 * though, since that would require that we release
                 * the bfaLock.  Releasing the bfaLock would open the
                 * door to an msfs_reclaim() of bfap->bfVp and we don't
                 * want to deal with bfap->bfVp going away right now.
                 * So, remember that we have to do the vrele() of the
                 * preallocated vnode and continue.
                 */
                if (got_clu_clone_vnode) {
                    MS_SMP_ASSERT(vp != bfap->bfVp);
                    clu_clone_vnode_to_vrele = vp;
                    got_clu_clone_vnode = FALSE;
                }
                vp = bfap->bfVp;

                VN_LOCK(vp);
                vret = vget_nowait(vp);

                if ((vret != 0) || (vp->v_type == VBAD)) {
                    if (vp->v_type == VBAD) {
                        /*
                         * This shouldn't happen.  Note instead of panic.
                         */
                        msfs_vget_bad++;
                    } else {
                        /*
                         * Wait for the vnode transition.  Once the locks
                         * are released msfs_reclaim() could clear bfap->bfVp
                         * and the vnode could be deallocated.  Prevent
                         * deallocation by bumping v_cache_lookup_refs so
                         * it is safe to use vget() to wait for the
                         * transition once locks are released.
                         */
                        if (!bypass_dealloc_code) {
                            CACHE_LOOKUP_REF(vp);
                        }
                    }

                    VN_UNLOCK(vp);

                    DEC_REFCNT( bfap );

                    if (bfaccState != lk_get_state(bfap->stateLk)) {
                        unlkAction = lk_set_state( &bfap->stateLk, bfaccState );
                        lk_signal( unlkAction, &bfap->stateLk );
                    }

                    mutex_unlock(&bfap->bfaLock);

                    if ((vret == 0) || (vget_cache(vp) == 0)) {
                        vrele(vp);
                    }

                    /*
                     * If we need to vrele() a preallocated vnode that we
                     * didn't end up using, do that now.
                     */
                    if (clu_clone_vnode_to_vrele) {
                        vrele(clu_clone_vnode_to_vrele);
                        clu_clone_vnode_to_vrele = NULL;
                    }

                    goto retry_access;
                }

                bs_bfs_get_set_id( bfSetp, &bfSetId );
                /*
                 * Sanity check to see if it still belongs to
                 * this bf.
                 */
                bnp = (struct bfNode *)&vp->v_data[0];

                if( (vp->v_tag != VT_MSFS) ||
                    ( ! BS_BFS_EQL( bnp->bfSetId, bfSetId ) ) ||
                    ( ! BS_BFTAG_EQL( bnp->tag, bfap->tag ) ) ) {
                    ADVFS_SAD0( "bs_access_one: vnode check fails" );
                }

                if( vp->v_object && vp->v_object != bfap->bfObj ) {
                    /*
                     * If the vnode has an object it should match the access
                     * structure's object.
                     */
                    ADVFS_SAD2("grab vnode: N1 = vp->v_object, N2 = bfap->bfObj",
                         (long)vp->v_object, (long)bfap->bfObj );
                }

                /*
                 * We change from VREG to VNON in msfs_inactive
                 * to avoid calls into ubc during vclean.  Change
                 * back to VREG here.
                 */
                if( vp->v_type == VNON ) {
                    vp->v_type = VREG;
                }

                VN_UNLOCK(vp);

                if ( options & BF_OP_HAVE_VNODE ) {
                    /*
                     * We didn't need the preallocated vnode after all.
                     * The access structure already had a good vnode
                     * attached to it.  Release the preallocated vnode.
                     * We can safely unlock the bfaLock and relock it
                     * since the refCnt is up on the access structure.
                     * Note: This vrele may cause a new transaction to
                     *       be started so we should not be here if we
                     *       have already started a transaction.
                     */
                    mutex_unlock(&bfap->bfaLock);
                    vrele(*fsvp);
                    mutex_lock(&bfap->bfaLock);
                }
            }
        }
        else {
            vp = bfap->bfVp;
        }
    }

    /*
     * We've got the vnode, it's usecount is up.
     *
     * Check for file system layer specific
     * processing.  Putting the vnode on the file system mount queue
     * (if necessary) is deferred to this point in the code so that
     * error paths will not have to take it off that queue.
     *
     * SMP
     *          bfap->bfaLock is held
     */

    if( mp ) {
        struct bfNode *bnp = (struct bfNode *)&vp->v_data[0];

        if( bnp->accessp != NULL ) {
            /*
             * This file is already open from the
             * file system level.  There is only a
             * count of one refCnt in the access struct
             * for any number of opens from the
             * file system layer.  This is because
             * there is only one call to msfs_inactive
             * that bs_closes the bitfile.
             */
            another_fs_open = TRUE;
            if( bnp->accessp != bfap ) {
                ADVFS_SAD0( "bnp->accessp != bfap" );
            }
            bfap->refCnt--;
            if ( bfap->refCnt <=  0 ) {
                ADVFS_SAD0( "bfap->refCnt <= 0" );
            }

            if (bfSetp->cloneId != BS_BFSET_ORIG) {
                /*
                 * We're opening a clone.  Since we just decremented
                 * its refCnt we must also decrement the original bitfile's
                 * refCnt so that they match.  Since the original
                 * file's access structure is not currently locked
                 * by this thread, we first need to lock it before
                 * modifying the refCnt and accessCnt fields.
                 * NOTE: This is currently the only place where a thread
                 *       locks two access structures simultaneously.
                 *       If more such instances are added, it is important
                 *       that some protocol be followed to prevent deadlock.
                 */
                bfAccessT *orgBfap = bfap->origAccp;

                mutex_lock(&orgBfap->bfaLock);
                orgBfap->refCnt--;
                if ( orgBfap->refCnt <= 0 ) {
                    ADVFS_SAD0( "orgBfap->refCnt <= 0" );
                }
                /*
                 * We need to decrement the original's access count.  It
                 * was opened before this clone open.  (See bs_access()).
                 */
                orgBfap->accessCnt--;
                mutex_unlock(&orgBfap->bfaLock);
            }

        } else {
            /*
             * Set accessp in the bnp indicating that the
             * file is open from the file system
             * level.
             */
            bnp->accessp = bfap;

            /*
             * First access via the file system layer.
             */
            bfap->accessCnt++;

        }

        /*
         * If we are reopening a file that was deleted but still
         * open prior to a cluster failover, set "bfaccState" to 
         * ACC_VALID, the same state that the access structure 
         * would have been in on the other member prior to failover.
         * That way, the code below which compares "bfaccState"
         * to the state in the access structure will reset the
         * state within the access structure to ACC_VALID.
         */
        if ((clu_is_ready()) && (mp->m_flag & M_FAILOVER) && 
            (bfap->bfState == BSRA_DELETING)) {
            bfaccState = ACC_VALID;
        }

    } else {
        /*
         * Another access via the bitfile system layer.
         */
        bfap->accessCnt++;
    }

    if ( bfaccState != lk_get_state( bfap->stateLk ) ) {
        unlkAction = lk_set_state( &bfap->stateLk, bfaccState );
        lk_signal( unlkAction, &bfap->stateLk );
    }

    /*
     * Link the original bitfile's access struct to the clone's
     * access struct.
     */
    if (origBfap) {
        origBfap->nextCloneAccp = bfap;
        bfap->origAccp = origBfap;
    }

    mutex_unlock(&bfap->bfaLock);

    /*
     * If we need to vrele() a preallocated vnode that we
     * didn't end up using, do that now.
     */
    if (clu_clone_vnode_to_vrele) {
        vrele(clu_clone_vnode_to_vrele);
        clu_clone_vnode_to_vrele = NULL;
    }
    *fsvp = vp;
    *outbfap = bfap;
    return( EOK );

    /*
     * Error paths are below here.
     */
err_vrele:
    /*
     * We get here when we've gotten or grabbed a vnode, but are going
     * to decrement the refCnt of the access structure right after
     * this, hence, close will not get called, so do the vrele here.
     */
    if ( !(options & BF_OP_HAVE_VNODE) ) {
        if ( vp ) {
            /*
             * While vnode is still referenced, clear its pointer to object
             * which will be freed below once access structure can't be found.
             * We don't want grab_bsacc() to clean-up the object when state
             * of bfap is ACC_INVALID.
             */
            VN_LOCK(vp);
            vp->v_object = NULL;
            VN_UNLOCK(vp);
            vrele( vp );
            did_vp_vrele = TRUE;
        }
    }

err_setinvalid:

    BS_BFAH_LOCK(bfap->hashlinks.dh_key, NULL);
    mutex_lock(&bfap->bfaLock);
    unlkAction = lk_set_state( &bfap->stateLk, ACC_INVALID );

    /* If entry was on a hash list, remove it.
     * This avoids a race with grab_bsacc waiters who were waiting
     * for the state to transition out of ACC_INIT_TRANS.
     * Since the state is set to ACC_INVALID here, those waiters
     * would all unknowingly race on resetting the entry.
     */
    if( bfap->hashlinks.dh_links.dh_next != NULL )
    {
        BS_BFAH_REMOVE(bfap, FALSE);
    }
    else
    {
        BS_BFAH_UNLOCK(bfap->hashlinks.dh_key);
    }

    /*
     * Once the bfAccess has been removed from the hash list it can't be
     * found by msfs_reclaim() to clear the bfap->bfVp field.  Do that now
     * because the vnode may be deallocated by the time grab_bsacc() gets
     * the bfap off the freelist to recycle it.  Since the access state is
     * invalid free the object also.
     */
    if (bfap->bfObj) {
        bfap->bfObj->vu_object.ob_ref_count -= 1;
        mutex_unlock(&bfap->bfaLock);
        ubc_object_free(bfap->bfObj);
        mutex_lock(&bfap->bfaLock);
        bfap->bfObj = NULL;
    }
    bfap->bfVp = NULL;

err_deref:

    DEC_REFCNT( bfap );

    /*
     * Do the signal before releasing the bfaLock.  After
     * we release the bfaLock, we can't even be assured
     * that 'bfap' still points to an access structure.
     */
    lk_signal( unlkAction, &bfap->stateLk );

    mutex_unlock(&bfap->bfaLock);

    if (got_clu_clone_vnode && !did_vp_vrele) {
        vrele(vp);
    }

    /*
     * If we need to vrele() a preallocated vnode that we
     * didn't end up using, do that now.
     */
    if (clu_clone_vnode_to_vrele) {
        vrele(clu_clone_vnode_to_vrele);
        clu_clone_vnode_to_vrele = NULL;
    }

    *outbfap = NULL;
    return sts;
}

#ifdef DEBUG
int xxx_vnode_cnt = 0;
#endif

/******************************************************************
***************** internal utility routines **********************
******************************************************************/

/*
 * SMP
 * Access struct is in INIT_TRANS
 * Vnode not accessible (not on mount queue)
 */

static statusT
get_n_setup_new_vnode(
                      bfAccessT *bfap,
                      bfSetT *bfSetp,    /* in - BF-set descriptor pointer */
                      struct vnode **nvp /* in/out - new vnode pointer */
                      )
{
    struct vnode *vp = NULL;
    statusT sts;
    struct bfNode *bnp;
    struct fsContext* fscp;
    bfSetIdT bfSetId;

    MS_SMP_ASSERT(lk_get_state( bfap->stateLk ) == ACC_INIT_TRANS);

#ifdef DEBUG
    if (xxx_vnode_cnt) {
        if (xxx_vnode_cnt == 1) {
            *nvp = 0;
            return ENFILE;
        }
        else {
            xxx_vnode_cnt--;
        }
    }
#endif

    vp = *nvp;
    if ( !vp ) {
        sts = getnewvnode( VT_MSFS, &msfs_vnodeops, &vp );
        if (sts != EOK) {
            return sts;
        }
    }
    bfap->bfVp = vp;

    if ( vp->v_object ) {
        ubc_object_free(vp->v_object);
        vp->v_object = VM_UBC_OBJECT_NULL;
    }

    vp->v_type = VREG;

    if ( bfap->bfObj ) {
        vp->v_object = bfap->bfObj;
        if ( vp->v_mount->m_funnel ) {
            vp->v_object->vu_oflags |= OB_FUNNEL;
        }
    }

    bnp = (struct bfNode *)&vp->v_data[0];
#define MSFS_VN_PRIVATE \
    roundup( sizeof( struct bfNode ) + sizeof( struct fsContext ),  \
             sizeof( void * ) )

    if (vn_maxprivate >= MSFS_VN_PRIVATE) {
        /*
         * The fsContext struct will fit in the vnode.
         * so protect that structure by de-initializing it
         */
        fscp = (struct fsContext *)
                &vp->v_data[roundup(sizeof(struct bfNode),sizeof(void *))];
        fscp->initialized = 0;
    }
    /*
     * fsContextp set to zero indicates to caller
     * that this is a new vnode, fsContext must be
     * initialized.
     */
    bnp->fsContextp = NULL;
    bnp->tag = bfap->tag;

    bs_bfs_get_set_id( bfSetp, &bfSetId );
    bnp->bfSetId = bfSetId;
    /*
     * The access structure pointer in the bfNode is also cleared so we
     * know to insert this vnode on the file system mount queue later.
     */
    bnp->accessp = NULL;

    *nvp = vp;
    return EOK;
}


/*
 * grab_bsacc
 *
 * Routine to find/init a bfAccess struct and return a pointer to the
 * locked structure whose refCnt has been incremented.
 *
 * In addition to its use by the public bitfile access routines, this
 * routine is also called by opx routines when access to only the
 * bfAccess structure is needed.  Those callers may not perform i/o to
 * the data pages for the bitfile, as the mapping information (and
 * vnode for ubc) are not set up by this routine.  Those callers must
 * also use the DEC_REFCNT macro to dereference the bfAccess struct.
 *
 * This routine will block while the bfAccess struct is in the
 * INIT_TRANS state, that is, it will only return when the bfAccess
 * struct is not in the INIT_TRANS state.
 *
 * SMP
 *  Returns with bfap->bfaLock held
 */

bfAccessT*
grab_bsacc(
           bfSetT *bfSetp,  /* in - bitfile-set handle */
           bfTagT tag,      /* in - bitfile tag */
           int forceFlag,   /* in - passed to get_free_acc() */
           uint32T options  /* in - options flags */
           )
{
    bfAccessT *bfap, *tbfap;
    u_long saved_v_id;
    bfTagT saved_tag;
    bfSetIdT saved_bfSetId;
    int retry = FALSE;
    struct vnode *vp;
    struct bfNode *bnp;
    ulong insert_count_before,insert_count_after;
    ulong hash_key;

lookup:
    /* Check if access struct with this tag is already in hash table.
     * If so, return it with its bfaLock seized.
     */
    bfap = find_bfap( bfSetp, tag, FALSE, &insert_count_before );

    if( bfap != NULL ) {

    found:
        /*  Found it in the hash table. */
        if ((lk_get_state(bfap->stateLk) == ACC_INVALID) &&
            (bfap->refCnt > 0)) {

            if (bfSetp->cloneId == BS_BFSET_ORIG) {
                /*
                 * We get here if a thread is in fs_create_file() and
                 * is in the process of failing a file creation.  We
                 * find the access structure after the fs_create_file()
                 * thread has called kill_mcell() via ftx_fail() but
                 * before it has had a chance to call vrele() on the
                 * file's vnode.
                 */
                mutex_unlock(&bfap->bfaLock);
                assert_wait_mesg_timo(NULL, FALSE, "AdvFS delay", 1);
                thread_block();
                goto lookup;
            }

            /*
             * We get here if two threads are racing to open the same
             * clone file.  The first one has ref'ed the access structure
             * and has called getnewvnode().  This thread finds the
             * access structure with state ACC_INVALID and the refCnt
             * up.  We just bump the refCnt and return.
             */
            MS_SMP_ASSERT(bfSetp->cloneId != BS_BFSET_ORIG);
            bfap->refCnt++;
            return bfap;
        }
        else if (lk_get_state(bfap->stateLk) != ACC_INVALID ) {

            /* If this bitfile is in transition, then we have to wait */
            lk_wait_while(&bfap->stateLk, &bfap->bfaLock, ACC_INIT_TRANS);
            lk_wait_while(&bfap->stateLk, &bfap->bfaLock, ACC_RECYCLE);

            /* Retry the access entry search if an undesired state is
             * detected or the entry's tag or setid is not as requested
             * by the caller. This prevents a race that can cause the
             * entry's refCnt to go negative.
             */
            switch (lk_get_state( bfap->stateLk )) {
            case ACC_INVALID:
            case ACC_INIT_TRANS:
            case ACC_RECYCLE:
                mutex_unlock(&bfap->bfaLock);
                goto lookup;

            default:
                if ((!BS_BFTAG_EQL(bfap->tag, tag)) ||
                    (!BS_BFS_EQL(bfSetp->bfSetId,
                                 ((bfSetT *)(bfap->bfSetp))->bfSetId))) {
                    mutex_unlock(&bfap->bfaLock);
                    goto lookup;
                }
                break;
            }

            /* Take off free or closed list if on one. */
            RM_ACC_LIST_COND(bfap);

            bfap->refCnt++;

            /* Return to the caller holding the bfaLock.  The caller must
             * test for states other than INIT_TRANS and take appropriate
             * action.
             */
            return bfap;
        } else {
            /* State == ACC_INVALID; take off free or closed list if on one */
            MS_SMP_ASSERT(bfap->refCnt == 0);
            RM_ACC_LIST_COND(bfap);
        }
    } else {

        /*  Not in the hash table, BF_OP_INMEM_ONLY set, so return NULL */
        if (options & BF_OP_INMEM_ONLY) {
            return NULL;
        }

        /*  Not in the hash table, so get a new bfAccess struct (locked) */
        bfap = get_free_acc(&retry, forceFlag);
        if (retry == TRUE) {
            /* nothing returned; no need to unlock bfap */
            goto lookup;
        } 
        else if (retry == EHANDLE_OVF) {
            /*
             * Could not allocate an access structure */
            return NULL;
        }
    }

    (void)lk_set_state(&bfap->stateLk, ACC_RECYCLE);

    /*
     * NOTE: It is necessary that we do the following recycle code
     *       before removing the access structure from the hash table.
     *       If we removed the access structure from the hash table first,
     *       new opens of the file would not find this access structure,
     *       would set up their own, get their own vnode and object,
     *       and start writing to the same disk blocks as this thread.
     */

    /* Initialize the new/recycled bfAccess struct.
     * If the bfAccess is being recycled, its last vnode will not be reused
     * so it should be vgone'd to put it at the front of the vnode freelist.
     * The following object cleanup duplicates some of the processing in
     * msfs_reclaim(), but a call to vgone() at this point would wait for
     * ACC_RECYCLE in find_bfap().  Once the bfAccess is removed from the
     * hash chain for its previous bitfile and bfap->bfaLock is released
     * for a call to vgone(), a new access to the previous bitfile may be
     * found when msfs_reclaim calls find_bfap() which would prevent it from
     * cleaning up the object.  Therefore, the code duplication is necessary.
     */
    vp = bfap->bfVp;
    if (vp) {
        VN_LOCK(vp);
        /* Hold off deallocation via CACHE_LOOKUP_REF instead of using a
         * vget call, as the corresponding vrele could trigger a hierarchy
         * panic in a CFS/Advfs scenario
         */
        CACHE_LOOKUP_REF(vp);
        bnp = (struct bfNode *)&vp->v_data[0];
        saved_v_id = vp->v_id;
        saved_tag = bnp->tag;
        saved_bfSetId = bnp->bfSetId;
        VN_UNLOCK(vp);
    }

    if ( bfap->bfObj ) {
        MS_SMP_ASSERT(bfap->freeFwd == NULL);
        if ( vp ) {
            VN_LOCK(vp);
            /*
             * bfap->bfaLock guarantees that if bfap->bfVp is non-zero
             * we haven't gone through msfs_reclaim(), so vp has not been
             * reused.  Disassociate old vnode from object about to be freed.
             */
            if ( vp->v_object == bfap->bfObj )
                vp->v_object = VM_UBC_OBJECT_NULL;
            VN_UNLOCK(vp);
        }

        mutex_unlock(&bfap->bfaLock);
        MS_SMP_ASSERT(vm_object_type((vm_object_t)bfap->bfObj) == OT_UBC);
        msfs_flush_and_invalidate(bfap, INVALIDATE_ALL | INVALIDATE_QUOTA_FILES);
        bfap->bfObj->vu_object.ob_ref_count -= 1;
        ubc_object_free(bfap->bfObj);
        mutex_lock(&bfap->bfaLock);
        bfap->bfObj = NULL;
        bfap->mapped = FALSE;
    }
    bfap->bfVp = NULL;

    /* If entry was on a hash list, remove it.
     * Check the hashlinks first so we don't lock a bucket unnecessarily.
     * We can for go the bfalock because we have marked the access
     * structure ACC_RECYCLE.
     */
    mutex_unlock(&bfap->bfaLock);
    if( bfap->hashlinks.dh_links.dh_next != NULL )
    {
        BS_BFAH_LOCK(bfap->hashlinks.dh_key,NULL);

        /*
         * We need to recheck the hashlinks because although we are
         * marked ACC_RECYCLE the revoke case in msfs_reclaim doesn't
         * check the state and tries to remove the us from the hashtable.
         * We could race here. This is safe because we are both trying to
         * achieve the same thing.
         */

        if( bfap->hashlinks.dh_links.dh_next != NULL )
        {
            BS_BFAH_REMOVE(bfap,FALSE);
        }
        else
        {
            BS_BFAH_UNLOCK(bfap->hashlinks.dh_key);
        }
    }

    /*
     * If the access structure is on a fileset list, remove it.
     */
    if (BFSET_VALID(bfap->bfSetp)) {

        RM_ACC_SETLIST(bfap, TRUE);

        /* This structure is no longer associated with a file set.
         * This must be cleared out only after it is removed from the
         * hash tbale.
         */

        bfap->bfSetp = NULL;
    }

    /*
     * Set the access structure's state to ACC_INVALID so that
     * vgone() doesn't block on ACC_RECYCLE. This structure
     * has been removed from all lists and is ACC_INVALID. We no
     * longer need the bfalock until we place it back on a list.
     */
    mutex_lock(&bfap->bfaLock);
    lk_signal(lk_set_state(&bfap->stateLk, ACC_INVALID), &bfap->stateLk);
    mutex_unlock(&bfap->bfaLock);

    /*
     * Move the vnode to the head of the free list so it will be chosen
     * first and allow vnodes with cached fscontext info to remain on
     * the free list in hopes that will be re-used. We must also release
     * our reference on the vnode too.
     */

    if (vp) {
        msfs_bsacc_recycles++;
        VN_LOCK(vp);
        if (vp->v_usecount == 0                     && 
            vp->v_id == saved_v_id                  &&
            vp->v_tag == VT_MSFS                    &&
            BS_BFTAG_EQL(bnp->tag, saved_tag)       &&
            BS_BFS_EQL(bnp->bfSetId, saved_bfSetId) ) {

            /*
             * We will zero out the bfSetId in the bfNode in order
             * to inform msfs_reclaim that it does not need to clear
             * out the access structure from the vnode. This avoids
             * a lot of unnecessary work in msfs_reclaim (see code for
             * more comments).
             */

            bnp->bfSetId = nilBfSetId;
            bnp->tag = NilBfTag;

            vgone(vp, VX_NOSLEEP, 0);
            VN_UNLOCK(vp);
        } else {
            VN_UNLOCK(vp);
        }
        CACHE_LOOKUP_RELE(vp);
    }

    /*
     * Get our new hash key and make sure its not already in the hashtable.
     */
    hash_key = BS_BFAH_GET_KEY(bfSetp,tag);
    tbfap = BS_BFAH_LOCK(hash_key,&insert_count_after);

    /* We didn't find any access structure on the chain from the last
     * find bfap call but we noted the insert count at that time. If nothing
     * has been inserted on that chain since then, we can assume find_bfap will
     * fail again.
     * If on the other hand an insertion has taken place to this chain then
     * we better make sure we aren't racing another grab_bsacc with the same
     * tag and bfsetp.
     * In either case the hash chain lock will protect us from an identical
     * access structure going in (we'll be holding it after find_bfap).
     */

    if (insert_count_after != insert_count_before)
    {
        BS_BFAH_UNLOCK(hash_key);

        /* Check to see if this access struct may have been added by another
         * thread while we were messing around in the above code; if so, use it.
         * Otherwise, set up our bfap to use.  Note that in this instance,
         * find_bfap() returns with both bfap->bfaLock and  BfAccessHashLock
         * seized so we can get our new one onto the list without racing any
         * other threads.
         */
        if ( tbfap = find_bfap(bfSetp, tag, TRUE, NULL) )
        {
            BS_BFAH_UNLOCK(hash_key);
            /*
             * bfap->bfaLock has been dropped above. We need to get it before
             * moving the bfap to the free list.
             */
            mutex_lock(&bfap->bfaLock);
            ADD_ACC_FREELIST(bfap);
            mutex_unlock(&bfap->bfaLock);

            bfap = tbfap;
            goto found;
        }
    }

    /* If this bfap was last used by a directory with an index or an
     * index itself, then free up the associated structure */

    if (IDX_INDEXING_ENABLED(bfap))
    {
        ms_free(bfap->idx_params);
    }
    bfap->idx_params=NULL;
    bfap->idxQuotaBlks=0;

    ACCESS_TRACE(bfap, tag.num);

    /*
     * We need to be able to lock a clone file's migTruncLk
     * in a different lock hierarchy order than a non-clone
     * file.  In particular, in bs_cow_pg(), when we are doing
     * a COW to a clone file and adding storage, we need to take
     * the clone file's migTruncLk.  But if the clone file to
     * which storage is being added is the clone fileset's frag
     * file, then the bfSetT.fragLock is already held at that
     * time.  So it is necessary to replace the standard lockinfo
     * structure for the clone file's migTruncLk with a 
     * clone-specific lockinfo structure which is below the 
     * bfSetT.fraglock in the lock hierarchy.  This is similar to
     * the game we play with the BMT in bfm_open_ms().
     */
    MS_SMP_ASSERT(bfap->bfSetp == NULL);
    if (bfSetp->cloneId == BS_BFSET_ORIG) {
        lock_terminate(&(bfap->xtnts.migTruncLk));
        lock_setup(&(bfap->xtnts.migTruncLk), 
                   ADVbsInMemXtntT_migTruncLk_info,
                   TRUE);
    }
    else {
        lock_terminate(&(bfap->xtnts.migTruncLk));
        lock_setup(&(bfap->xtnts.migTruncLk), 
                   ADVClonebsInMemXtntT_migTruncLk_info,
                   TRUE);
    }

    bfap->accessCnt     = 0;
    bfap->refCnt        = 1;
    bfap->mmapCnt       = 0;
    bfap->mmapFlush     = 0;
    bfap->tag           = tag;
    bfap->bfSetp        = bfSetp;
    MS_SMP_ASSERT(TEST_DMNP(bfSetp->dmnP) == EOK);
    bfap->dmnP       = bfSetp->dmnP;
    bfap->mapped        = FALSE;
    bfap->flushWait     = 0;
    bfap->maxFlushWaiters = 0;
    bfap->hiFlushLsn    = nilLSN;
    bfap->hiWaitLsn     = nilLSN;
    bfap->nextFlushSeq  = firstLSN;
    bfap->logWriteTargetLsn = nilLSN;
    bfap->flushWaiterQ.head = (struct flushWaiter *)&bfap->flushWaiterQ;
    bfap->flushWaiterQ.tail = (struct flushWaiter *)&bfap->flushWaiterQ;
    bfap->flushWaiterQ.cnt = 0;
    bfap->raHitPage     = 0;
    bfap->raStartPage   = 0;
    bfap->real_bfap     = NULL;
    bfap->cloneXtntsRetrieved = 0;
    bfap->cloneId       = 0;
    bfap->cloneCnt      = 0;
    bfap->nextCloneAccp = NULL;
    bfap->origAccp      = NULL;
    bfap->noClone       = 0;
    bfap->maxClonePgs   = 0;
    bfap->cowPgCount    = 0;
    bfap->trunc         = 0;
    bfap->outOfSyncClone = 0;
    bfap->deleteWithClone = 0;
    bfap->dirTruncp     = NULL;
    bfap->fragState = FS_FRAG_NONE;

    /*
     * Add the access structure to the fileset's list.
     * of access structures. This returns with the
     * bfalock held.
     */
    ADD_ACC_SETLIST(bfap);
    MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);

    /* Link entry into fileset-tag hash list. We are holding the
     * hash chains lock already and dyn_hash_insert will release it
     */

    bfap->hashlinks.dh_key = hash_key;
    BS_BFAH_INSERT(bfap,FALSE);

    /* Return bfAccess pointer to caller, holding bfaLock. */
    return bfap;
}


/**********************************************************************
************** bitfile close routines ********************************
**********************************************************************/

statusT
bs_close(
         bfAccessT *bfAccessp, /* in */
         int options           /* in */
         )
{
    bfSetT *bfSetp;
    statusT sts;

    if (bfAccessp == NULL) {
        return( EINVALID_HANDLE );
    }

    bfSetp = bfAccessp->bfSetp;

    if ((bfSetp->cloneId != BS_BFSET_ORIG) &&
        (bfAccessp->origAccp != NULL)) {
        /*
         * We are closing the clone.  Close the original too!
         */
        /*
         * If the orig close requires the clone to be updated then we don't
         * want to call bs_access_one again. This will deadlock since
         * it will wait for VINACTIVATING to clear (in vgetm called by
         * grab_vnode called by bs_access_one) and we set VINACTIVATING
         * (vrele calls msfs_inactive calls bs_close).
         * The clone access handle is already in the orig access struct.
         * Set a flag to be used in bs_cow_pg to prevent the call to
         * bs_access_one and use the clone access struct referenced by
         * the orig. This flag is cleared in bs_close_one if the orig is
         * closed for the last time (vrele calls ... calls bs_close_one with
         * MSFS_INACTIVE_CALL set). Otherwise I clear the flag here. The
         * nextCloneAccp == bfAccessp test tells if it is the same access
         * struct (not recycled).
         *
         * NOTE: The above comment/code no longer applies. Now we no longer
         * get a vnode when internally opening the clone (if the close of
         * the orig caused a cow pg (which I'm not sure can happen anymore
         * either)) so it wont deadlock on VINACTIVATING. Also if we ever
         * go back to this the cloneAccHRefd mechanism is flawed since 
         * a racing cow can sneak in an not open the clone.
         */

        (void) bs_close_one(bfAccessp->origAccp, 0, FtxNilFtxH );
    }

    sts = bs_close_one(bfAccessp, options, FtxNilFtxH);
    return(sts);
}

/*
 * bs_close_one
 *
 * Returns EOK, EINVALID_HANDLE
 */

statusT
bs_close_one (
              bfAccessT *bfap,          /* in/out */
              int options,              /* in */
              ftxHT parentFtxH          /* in */
              )
{
    ftxHT ftxH;
    lkStatesT prevState;
    vdT *delVdp;
    statusT sts;
    void *delList;
    uint32T delCnt = 0;
    bfMCIdT delMCId;
    domainT *dmnP;
    boolean_t fragFlag = FALSE;
    boolean_t deleteIt = FALSE;
    boolean_t ftxFlag = FALSE;
    boolean_t madeFrag = FALSE;
    struct vnode *vp;
    boolean_t trunc_xfer_lock = FALSE;
    boolean_t mig_trunc_lock = FALSE;
    boolean_t frag_mig_trunc_lock = FALSE;
    boolean_t is_clone = (bfap->bfSetp->cloneId != BS_BFSET_ORIG);
    int destroy_enabled=0;
    char *attrcopy=NULL;
    int token_flg;
    bfAccessT *cloneap/* = NULL*/;
    bfSetT *cloneSetp;
    int setHeld = FALSE;
    extern REPLICATED int SS_is_running;

    dmnP = bfap->dmnP;

    if (TrFlags & trClose) {
        trace_hdr();
    }

    /* Only test for fragging if this is an external close. External
     * closes will only come from MSFS_INACTIVE_CALL all others are
     * internal closes and shouldn't cause the file to be fragged.
     * By only allowing the last external close to frag the file we
     * insure that the vnode is available, internal opens may not have
     * a vnode on the file.
     */

    if (options & MSFS_INACTIVE_CALL) {
        /* Notify vfast with a FILE_FRAGRATIO_UPDATE that
         * file is being closed by the last non-kernel closer.
         * Update the file's fragmentation stats now.
         * We avoid a hierarchy lock problem between bfaLock and
         * xtntMap_lk used in ss_chk_fragratio by taking the
         * bfaLock after ss_chk_fragratio.
         */
        if((BS_BFTAG_REG(bfap->tag)) &&
           (bfap->ssMigrating != SS_POSTED) &&
           (bfap->xtnts.allocPageCnt > 1) &&
           !(options & MSFS_SS_NOCALL) )  {
                ss_chk_fragratio(bfap);
        }

        mutex_lock(&bfap->bfaLock);
        fragFlag = fs_quick_frag_test (bfap);
    } else
        mutex_lock(&bfap->bfaLock);

    /* We now will allow the file to be fragged or trunced from
     * MSFS_INACTIVE callers even if the access cnt is > 1. This is
     * because the other bumps of the access count are internal opens
     * of the orignal from a clone.  The last close from a clone may
     * not have a vnode for the original and the call to chk_blk_quota
     * will panic. It is safe to frag and trunc now because :
     *
     *      1) Clones are read-only and can not cause the file
     *         to be fragged or truncated.
     *      2) Fragging at this point means the last page of the
     *         file was modified which caused it to COW over to the
     *         clone.
     *      3) Truncation at this point cause the pages to be cowed
     *         over to the clone.
     *
     * An internal open may also be done by clear_bmt() when
     * processing an rmvol.  In this case, the RMVOL_TRUNC_LOCK
     * serialization prior to decrementing bfap->accessCnt allows
     * safe fragging or truncating prior to the last internal close.
     * See comment in clear_bmt() about access used to serialize
     * with the moving of the bitfile.
     */

    if (lk_get_state( bfap->stateLk ) != ACC_INIT_TRANS) {
        if ((!((options & MSFS_INACTIVE_CALL) && ((fragFlag) ||
             (bfap->trunc) || (bfap->bfState == BSRA_DELETING)))
             && ((bfap->accessCnt > 1) ||
              (bfap->bfState != BSRA_DELETING))) ||
            ((bfap->bfState == BSRA_DELETING) &&
             (bfap->dmnP->state != BFD_ACTIVATED)))
        {
            /*
             * The only time that we need to execute the code between
             * here and '_close_it' is, 1) if the state is INIT_TRANS so
             * we have wait for it to become available;  2) or, if the
             * accessCnt == 1 and we need to delete, frag or truncate the
             * bitfile.  If we need to delete, frag or truncate the bitfile we
             * also need to start an ftx which must be done before blocking on
             * INIT_TRANS (the locking protocol is to start an ftx before
             * blocking on the bf state lock).  None of these conditions
             * are true so go straight to jail (er, to "_close_it") and
             * close the bitfile.
             *
             * On the last external close of a file being deleted we must
             * do the quota updates while the fsContext structure is 
             * protected from reclaim (has not gone to vnode freelist yet).  
             * Once the MSFS_INACTIVE_CALL last external close has run, the 
             * vnode can be reclaimed at any time, and chk_blk_quota() and
             * fs_delete_frag() cannot safely check and set fields of the
             * fsContext structure.
             *
             * We need to distinguish if a file is marked for deletion but
             * we are still running recovery. In this case we do not want to
             * remove the file. The recovery code will do the right thing.
             * Either UNDO the remove or ROOTDONE and complete it. It is
             * not our place to do it here.
             */

            --bfap->accessCnt;
            goto _close_it;
        }
    }

    mutex_unlock(&bfap->bfaLock);

    if ( (fragFlag || bfap->trunc) &&
         (bfap->bfState != BSRA_DELETING) &&
         (((bfap->bfSetp->cloneSetp != NULL) &&
         !(options & MSFS_BFSET_DEL)) || 
         ((bfap->accessCnt > 1) && (options & MSFS_INACTIVE_CALL))))
    {
        /*
        ** We can only get here from msfs_inactive=>bs_close (& advfs_mountfs)
        ** since that is the only path that sets MSFS_INACTIVE_CALL and fragFlag
        ** is only set if MSFS_INACTIVE_CALL is set. trunc will only be set by
        ** msfs_inactive=>fs_trunc_test. The other calls to fs_trunc_test clear
        ** trunc immediately. These paths do no have a transaction started.
        ** This is good since the following locks are above root transactions in
        ** the lock hierarchy. Also get_clu_clone_locks is going to start
        ** (and finish) a root transaction. We can't start a second root
        ** transaction when we already have a transaction active.
        */
        MS_SMP_ASSERT(FTX_EQ(parentFtxH, FtxNilFtxH));
        if ( clu_is_ready() && bfap->bfSetp->cloneId == BS_BFSET_ORIG ) {
            token_flg = get_clu_clone_locks( bfap, NULL, &cloneSetp, &cloneap );
            setHeld = TRUE;
        }

        RMVOL_TRUNC_LOCK_READ(bfap->dmnP);

        TRUNC_XFER_LOCK_WRITE(&bfap->trunc_xfer_lk);
        trunc_xfer_lock = TRUE;
    }

    if (FTX_EQ(parentFtxH, FtxNilFtxH) && !is_clone) {
        /* Prevent races with migrate code paths */

        /* Need to take the bfap lock because of the call chain
         * bf_setup_truncation --> stg_remove_stg_start
         */

        MIGTRUNC_LOCK_READ( &(bfap->xtnts.migTruncLk) );
        mig_trunc_lock = TRUE;

        /* Need to take the frag file lock because of the call
         * chain
         * fs_create_frag --> bs_frag_alloc --> frag_list_extend -->
         * rbf_add_stg
         */
        if (fragFlag && (bfap->bfSetp->fragBfAp != NULL)) {
            MIGTRUNC_LOCK_READ(&(bfap->bfSetp->fragBfAp->xtnts.migTruncLk));
            frag_mig_trunc_lock = TRUE;
        }

        /* No need to take the quota locks. Storage is only added
         * to the quota files at file creation, chown or explicit
         * quota setting. The quota entry already exists in the
         * quota file so the chk_blk_quota will NOT add storage.
         * The potential danger path would be
         * chk_blk_quota --> dqsync --> rbf_add_stg
         * but it is tame in this case.
         */
    }

    sts = FTX_START_N(FTA_BS_CLOSE_V1,&ftxH,parentFtxH,bfap->dmnP,1);

    if (sts != EOK) {
        ADVFS_SAD1( "ftx start failed", sts );
    }
    /*
     *There is no need to take the migTruncLk of anything on a clone.
     */
    mutex_lock(&bfap->bfaLock);

    /*
     * Wait until the access struct is out of transition.
     */
    lk_wait_while(&bfap->stateLk, &bfap->bfaLock, ACC_INIT_TRANS);

    if (( --bfap->accessCnt > 0 ) &&
        (!(fragFlag)) &&
        (!(bfap->trunc)) &&
        (bfap->bfState != BSRA_DELETING))
    {
        ftxFlag = TRUE;
        goto _close_it;
    }

    MS_SMP_ASSERT(bfap->accessCnt >= 0);

    /*
     * Note that the access count must be tested while holding the
     * mutex.  Now that the accessCnt has been determined to be 1,
     * set the access structure into transition, as potentially
     * frags, truncation or deletion will need to block for some
     * reason.
     */
    prevState = lk_get_state (bfap->stateLk);
    (void) lk_set_state (&(bfap->stateLk), ACC_INIT_TRANS);

    mutex_unlock(&bfap->bfaLock);

    if ( TestXtntsFlg ) {
        test_xtnt(bfap);
    }

    if ( bfap->bfState == BSRA_DELETING ) {
        /*
         * This bitfile is marked for deletion and this is the
         * final close, so actually clean everything up now.
         * However, if CFS is relocating the domain then don't
         * free the tag and disk storage and don't change quotas.
         */
        /*
         * NOTE: Ignore return status.
         */

        /* notify vfast with a HOT_REMOVE message  */
        if ((SS_is_running == TRUE) &&
             (bfap->dmnP->ssDmnInfo.ssDmnSmartPlace == TRUE) &&
             (bfap->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
              bfap->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {
            ss_snd_hot ( HOT_REMOVE, bfap );
        }

        deleteIt = !clu_is_ready() ||
                   !(bfap->dmnP->dmnFlag & BFD_CFS_RELOC_IN_PROGRESS);

        if ( deleteIt && (options & MSFS_INACTIVE_CALL) ) {
            /*
             * After the vnode is inactivated, the vnode may be reclaimed so do
             * the quota updates now.  If this is also the last close the disk
             * storage will be freed immediately.  If there is also an internal
             * open on the file, the storage is freed on last internal close to
             * serialize with a rmvol clear_bmt().  See comment in clear_bmt().
             */

            if ( (bfap->bfVp) && (VTOC(bfap->bfVp)) ) {
                /*
                 * We can just return the quota allocations right away since
                 * deleting this bitfile can't fail.
                 * NOTE: bfap->idxQuotaBlks will be zero except for directories
                 *  that had an index associated with them. We need to account
                 *  for the space consumed by the index being removed.
                 */
                long totalblks = (long)bfap->xtnts.allocPageCnt *
                                 ADVFS_PGSZ_IN_BLKS + bfap->idxQuotaBlks;

                if (bfap->fragState == FS_FRAG_VALID)
                     totalblks += bfap->fragId.type * 2;

                change_quotas(bfap->bfVp, -1, -totalblks, NULL,
                              u.u_cred, 0, ftxH);

                /* Use bfattrp->state as a flag during recovery. */
                /* If it is BSRA_DELETING then we crashed before we could */
                /* set the quotas. If it is BSRA_INVALID we got the quotas */
                /* set but we may not have deleted the storage yet. */
                reset_ondisk_bf_state( bfap->dmnP,
                                       bfap->primVdIndex,
                                       bfap->primMCId,
                                       BSRA_INVALID,
                                       ftxH );
            }
        }

        if (bfap->accessCnt == 0) {

            /*
             * if !(deleteIt), we need to allow I/O's on dirty pages
             * to proceed and stats to be written out.  We will also
             * leave the file on the DDL, without removing storage.  This
             * is all necessary because the unlinked open file may still
             * be valid after CFS relocation.
             */

            if (deleteIt) {

                /*
                 * The transaction which deleted this bitfile must have 
                 * previously committed (i.e. its done record must appear 
                 * in the in-memory log.)
                 *
                 * This is assured by the fact that the accessCnt is one. (The
                 * accessCnt will not go to zero inside the transaction tree 
                 * that marked the bitfile as deleted.  It must be some 
                 * subsequent close that decrements the accessCnt to zero.)
                 */
                fs_delete_frag (
                                      bfap->bfSetp,
                                      bfap,
                                      ((options & MSFS_INACTIVE_CALL) ? 
                                        bfap->bfVp : NULL),
                                      u.u_cred,
                                      TRUE,
                                      ftxH
                                      );
    
                /*
                 *  Move the tag that is NOT_IN_USE to the free list since
                 *  this is the last accessor.
                 */
    
                tagdir_tag_to_freelist( bfap->bfSetp,
                                        &bfap->tag,
                                        ftxH,
                                        FALSE);


                /*
                 * Purge dirty pages for this bitfile so that cannot be
                 * written to disk after the disk blocks are recycled to
                 * another bitfile.
                 * 
                 * If file doesn't have obj it is device file or fifo and
                 * doesn't do i/o through msfs_vnodeops so no buffers to
                 * clean up.
                 */

                if (bfap->bfObj) {
                    bs_invalidate_pages(bfap, 0, 0, 0);
                }

                /* 
                 * Defer the stg deallocation until after all locks are 
                 * released.  
                 */

                delMCId = bfap->primMCId;
                delVdp = VD_HTOP(bfap->primVdIndex, bfap->dmnP);

                /* 
                 * If we are deleting the file, we don't need to worry 
                 * about flushing the stats to disk. Also we don't want 
                 * this access structure to get on the ClosedAcc list. 
                 * It may be even better to test for BSRA_INVALID in 
                 * free_acc_struct if that guarantees that the access
                 * structure is no longer being used. 
                 */

                if ( bfap->bfVp && VTOC(bfap->bfVp) ) {
                    VTOC(bfap->bfVp)->dirty_stats = NULL;
                }

            } else {
                msfs_flush_and_invalidate(bfap, INVALIDATE_ALL);
                (void) fs_update_stats(bfap->bfVp, bfap, ftxH, 0);
            }

            bfap->bfState = BSRA_INVALID;

        } else {
            /*
             * Wait for last close to remove storage.
             */
            deleteIt = FALSE;
        }
    } else {

        if (fragFlag) {

            /*
             * fs_create_frag will set the "trunc" flag as a side
             * effect if a frag was actually created.
             */
            if ( fs_create_frag(bfap->bfSetp, bfap, u.u_cred, ftxH) == EOK ) {
                madeFrag = TRUE;
            }
        }

        if (bfap->trunc) {
            /*
             * This is the last close.  Must do truncate before the refCnt is
             * decremented to 0 because various routines take an
             * access handle which is only valid if refcnt != 0.
             * The stg dealloc is deferred until all locks are released.
             *
             * Don't bother with this if the file is getting deleted!
             */

            delCnt = bf_setup_truncation(bfap, ftxH, &delList, madeFrag);
            if ( delCnt == 0 && madeFrag ) {
                /* We made a frag but could not truncate the last page.
                 * Delete the frag and leave the last page of the file.
                 * Note quota was not increased so we don't decrease.
                 */
                fs_delete_frag( bfap->bfSetp,
                                      bfap,
                                      bfap->bfVp,
                                      u.u_cred,
                                      TRUE,
                                      ftxH);
            }
            bfap->trunc = 0;

        }
    }

    if ( TestXtntsFlg ) {
        test_xtnt(bfap);
    }

    ftx_done_n(ftxH, FTA_BS_CLOSE_V1);

    if (mig_trunc_lock) {
        if (frag_mig_trunc_lock) {
            MIGTRUNC_UNLOCK(&(bfap->bfSetp->fragBfAp->xtnts.migTruncLk));
            frag_mig_trunc_lock = FALSE;
        }
        MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) );
        mig_trunc_lock = FALSE;
    }

    mutex_lock(&bfap->bfaLock);
    lk_signal( lk_set_state (&(bfap->stateLk), prevState), &bfap->stateLk );
    mutex_unlock(&bfap->bfaLock);

    /*
     * Now that we're out of ACC_INIT_TRANS state, we can
     * delete the storage, which will start more ftxs.
     */

    if (deleteIt) {
        /* delete the file's storage */

        if ((sts = del_dealloc_stg(delMCId, delVdp)) != EOK) {
            domain_panic(dmnP,"bs_close_one: Can't dealloc stg");
        }

        if (destroy_enabled) {
             /*
              * Make sure the destroy transaction is flushed
              * to the log prior to posting the destroy event.
              *
              * There is no need to checkpoint under the unreliable
              * model.  The purpose of checkpointing (subsequent
              * to the post) would be to:
              *         ensure that upon recovery, the
              *         transaction would not be replayed
              *         (causing a new destroy event) after
              *         the destroy event had already been posted.
              * In the unreliable model, if we crash after the
              * event is posted, we are ok--no extra event is
              * posted at recovery time.  If we crash after the
              * the lgr_flush() but before the event post, we
              * lose the event.  If we checkpoint (after posting):
                        - crashing right before the checkpoint
                        is ok, we posted the event.  No event will
                        be posted when the event is replayed at
                        recovery.
                        - crashing after the checkpoint is similar:
                        we posted the event.  The transaction will not
                        be replayed at recovery.
              */
             lgr_flush( bfap->dmnP->ftxLogP );
        }

    } else if ( delCnt ) {
        /* truncate the file's storage */

        stg_remove_stg_finish( dmnP, delCnt, delList );

    }

    /* Update vfast frag list when a file has been truncated
     * or deleted.
     */
    if((BS_BFTAG_REG(bfap->tag)) &&
       (bfap->xtnts.allocPageCnt > 1) &&
       (bfap->ssMigrating != SS_POSTED) &&
      !(options & MSFS_SS_NOCALL) )  {
        ss_chk_fragratio(bfap);
    }

    mutex_lock(&bfap->bfaLock);

_close_it:
    if ( trunc_xfer_lock ) {
        if ( setHeld ) {
            mutex_unlock(&bfap->bfaLock);
            release_clu_clone_locks( bfap, cloneSetp, cloneap, token_flg );
            mutex_lock(&bfap->bfaLock);
        }

        TRUNC_XFER_UNLOCK(&bfap->trunc_xfer_lk);
        RMVOL_TRUNC_UNLOCK(bfap->dmnP);
    }

    if (mig_trunc_lock) {
        if (frag_mig_trunc_lock) {
            MIGTRUNC_UNLOCK(&(bfap->bfSetp->fragBfAp->xtnts.migTruncLk));
            frag_mig_trunc_lock = FALSE;
        }
        MIGTRUNC_UNLOCK( &(bfap->xtnts.migTruncLk) );
        mig_trunc_lock = FALSE;
    }

    vp = bfap->bfVp;
    if (bfap->accessCnt == 0) {

        /*
         * For VREG files set/clear the VDIRECTIO flag of the vnode
         * based on the default cache policy of the set.
         */
        if ((vp != NULL) && (vp->v_type == VREG)) {
            VN_LOCK(vp);
            /*
             * Don't set the direct I/O flag if the file uses data
             * logging.  If the file was opened via .tags, check if 
             * the original file is also protected via the dataSafety 
             * field to prevent DIRECTIO mode from being set.  We must
             * check the tag value in this path since we can get here
             * for a shadow bfap that has already had real_bfap cleared
             * by msfs_inactive.
             */
            if ( (((bfSetT *)bfap->bfSetp)->bfSetFlags & BFS_IM_DIRECTIO) &&
                 (bfap->dataSafety != BFD_FTX_AGENT) &&
                 (bfap->dataSafety != BFD_FTX_AGENT_TEMPORARY) &&
                 !(bfap->real_bfap &&
                   bfap->real_bfap->dataSafety == BFD_FTX_AGENT) &&
                 !(BS_BFTAG_RSVD(bfap->tag) && BS_BFTAG_PSEUDO(bfap->tag)) )
                vp->v_flag |= VDIRECTIO;
            else
                vp->v_flag &= ~VDIRECTIO;

            /*
             * VMMAPPED is cleared so no file can have both VDIRECTIO
             * and VMMAPPED set.  Locking in msfs_mmap(), fs_read(),
             * and fs_write() depends on VMMAPPED only being cleared
             * when the file is no longer referenced.  If cleared
             * earlier deadlocks between those paths could result!
             */
            vp->v_flag &= ~VMMAPPED;
            VN_UNLOCK(vp);
        }
        /* To make certain that access structs do not lie around on the closed
         * list.  Clear dkResult if we are not CFS, NFS or type VREG
         */
        if ((vp != NULL) && ((NFS_SERVER_TSD == 0 &&
           ((clu_is_ready() && CFS_IN_DAEMON()) == 0)) ||
            vp->v_type != VREG))
        {
            bfap->dkResult = EOK;
        }
    }

    /*
     * If this is the last close of the file and this mount point
     * is using temporary data logging (-o adl) and this temporary
     * data logging was deactivated for this file, reactivate it
     * so that the next accessor of the file gets the temporary
     * data logging.  Note that msfs_inactive() bumps the refCnt
     * in the access structure before calling bs_close() so we
     * check for a refCnt of two, not one.
     */
    if ((bfap->refCnt == 2) && (options & MSFS_INACTIVE_CALL) && 
        (bfap->dataSafety == BFD_NIL) && (bfap->bfVp) && 
        (bfap->bfVp->v_mount->m_flag & M_ADL)) {
        bfap->dataSafety = BFD_FTX_AGENT_TEMPORARY;
    }

    DEC_REFCNT( bfap );

    mutex_unlock(&bfap->bfaLock);

    if ( ftxFlag ) {
        ftx_done_n( ftxH, FTA_BS_CLOSE_V1 );
    }
    /*  if we should release the vnode lets check to make sure
     *  there is one.  It is possible to have an access structure
     *  with out a vnode.
     */
    if ( options & MSFS_DO_VRELE && (vp != NULL)) {
        vrele(vp);
    }

    return EOK;
}


/*
 * When the access struct refcnt goes to zero, the access struct is added to
 * the closed list or the free list. Access structures on the closed list still
 * have buffers or dirty pages in their object or are in ACC_FTX_TRANS state.
 * Access structures on the free list are ready for reuse.
 *
 * SMP
 *  Enter and return with bfap->bfaLock held.
 *  Lock the bfap->bfIoLock while examining the clean and dirty bsBuf lists.
 */

void
free_acc_struct(
                bfAccessT* bfap /* in - pointer to access struct */
                )
{
    int added_bfap_to_list = FALSE;

    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfaLock.mutex));

    MS_SMP_ASSERT(bfap->accessCnt == 0);
    if ( bfap->refCnt < 0 ) {
        ADVFS_SAD0("neg refCnt");
    }
    if( bfap->dirTruncp != NULL ) {
        ms_free( bfap->dirTruncp );
        bfap->dirTruncp = NULL;
    }
    bfap->logWriteTargetLsn = nilLSN;
    
    /*
     * Try to lock the bfap->bfIoLock out-of-order.  If we can't,
     * just put the bfap onto the closed list, to be safe.  If
     * we can lock the bfIoLock, look at the dirtyBufList length
     * to see if there are any bsBufs associated
     * with this file.  If so, the access structure needs to go 
     * onto the closed list.   It is essential that we hold the
     * bfIoLock while examining the dirtyBufList
     * fields to avoid races with paths like bs_io_complete() that
     * take that lock, pull a bsBuf off the dirtyBufList
     * and then release the lock.
     */
    if (!mutex_lock_try(&bfap->bfIoLock.mutex)) {
        ADD_ACC_CLOSEDLIST(bfap);
        added_bfap_to_list = TRUE;
    } 
    else {
        if (bfap->dirtyBufList.length ) {
            ADD_ACC_CLOSEDLIST(bfap);
            added_bfap_to_list = TRUE;
        }
        mutex_unlock(&bfap->bfIoLock);
    }
    
    if (!added_bfap_to_list) {
        if ( lk_get_state(bfap->stateLk) == ACC_FTX_TRANS ) {
            ADD_ACC_CLOSEDLIST(bfap);
        } else if (bfap->bfVp && VTOC(bfap->bfVp)&& 
                   VTOC(bfap->bfVp)->dirty_stats){
            ADD_ACC_CLOSEDLIST(bfap);
        } else if (bfap->saved_stats) {
            ADD_ACC_CLOSEDLIST(bfap);
        } else if (bfap->dkResult != EOK) {
            ADD_ACC_CLOSEDLIST(bfap);
        } else if ( bfap->bfObj == NULL ) {
            CHECK_ACC_CLEAN(bfap);
            ADD_ACC_FREELIST(bfap);
        } else {                          /* bfap->bfObj != NULL */
            vm_object_lock(bfap->bfObj);  /* lock while derefing object flds */
            if ( bfap->bfObj->vu_dirtypl == NULL &&
                    bfap->bfObj->vu_dirtywpl == NULL &&
                    bfap->bfObj->vu_putpages == 0) {
                CHECK_ACC_CLEAN(bfap);
                /* unlock to avoid lock violation with BfAccessFreeLock */
                vm_object_unlock(bfap->bfObj);
                ADD_ACC_FREELIST(bfap);
            } else {
                vm_object_unlock(bfap->bfObj);
                ADD_ACC_CLOSEDLIST(bfap);
            }
        }
    }
}


/*
 * Clear_buf and get_freebuf call this when they reduce the number of
 * buffers on the access structure.
 * When an access structure that is on the closed list has no more buffers
 * associated with it, has no dirty or busy pages on the object, and
 * doesn't have dirty stats, then it can move to the free list.
 *
 * SMP
 *  Enter  with bfap->bfaLock and bfap->bfIoLock held.
 *  Return with bfap->bfaLock and bfap->bfIoLock held.
 *  BfAccessFreeLock will be seized/released when manipulating free lists.
 *  Object lock held while examining object fields.
 */

void
check_mv_bfap_to_free(bfAccessT* bfap)
{
    struct vnode *vp = bfap->bfVp;
    struct vm_ubc_object *obj = bfap->bfObj;

    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfaLock.mutex));
    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfIoLock.mutex));

    /* Check following conditions; if any prevail, then do not move to
     * free list.
     *   1. Not on closed list.
     *   2. There are still buffers associated with this access struct
     *   3. The stats are dirty; must stay on the closed list
     *   4. dkResult != EOK (possibly unreported error)
     */
    if ( (bfap->freeFwd == NULL)    ||
         (bfap->dkResult != EOK)    ||
         (bfap->dirtyBufList.length) ||
         (vp && vp->v_tag == VT_MSFS && VTOA(vp) == bfap &&
          VTOC(vp) && VTOC(vp)->dirty_stats)
       ) {
        return;
    }

    /* if the stats are hung off the access structure, must stay on closed
     * list until flushed to disk.
     */
    if (bfap->saved_stats) {
        return;
    }

    /* The bfap is on the closed list (must NOT be on free list yet). */
    MS_SMP_ASSERT(bfap->onFreeList == -1);

    /* if the object has no dirty pages or busy pages then we can move */
    /* the access struct to the free list */
    vm_object_lock(obj);
    if (obj->vu_dirtypl == NULL &&
        obj->vu_dirtywpl == NULL &&
        obj->vu_putpages == 0) {
        vm_object_unlock(obj);
        RM_ACC_LIST(bfap);                 /* take off closed list */
        ADD_ACC_FREELIST(bfap);            /* put on free list     */
    } else
        vm_object_unlock(obj);

}


/*
 * bf_setup_truncation
 *
 * This function truncates the bitfile if it is not a clone and if a truncate
 * was requested.  This function should only be called at the last bitfile
 * close.
 *
 * LOCKS:  The caller usually holds the migTruncLk (this lock must
 *         be locked before starting a root transaction).
 *         This requirement is due to the call to
 *         stg_remove_stg_start().  The exception to the rule
 *         in the path in from bs_close_one() where this thread is
 *         the only accessor of the file.  In that case, there is 
 *         no need to lock the migTruncLk before calling this function
 *         since no migrate thread can access the file while the
 *         access structure's state is ACC_INIT_TRANS, which is what
 *         it will be when coming in here from bs_close_one().
 *         NB: I added asserts in the storage allocation/deallocation
 *              paths to ensure that the miTruncLk is held, so even though
 *              the lock is not strictly necessary, it IS taken (shared).
 */

uint32T
bf_setup_truncation (
                     bfAccessT *bfap,     /* in */
                     ftxHT ftxH,          /* in */
                     void **delList,      /* out */
                     int made_frag
                     )
{
    statusT sts;
    long pagesUsed;
    uint32T delCnt;

    pagesUsed = (bfap->file_size + ADVFS_PGSZ - 1L) / ADVFS_PGSZ;

    if ( bfap->fragState == FS_FRAG_VALID ) {
        --pagesUsed;
    }

    if ( pagesUsed < bfap->nextPage ) {

        sts = stg_remove_stg_start (
                                    bfap,
                                    pagesUsed,
                                    bfap->nextPage - pagesUsed,
                                    made_frag ? 2 : 1, /* do rel quotas */
                                    ftxH,
                                    &delCnt,
                                    delList,
                                    TRUE       /* do COW */
                                    );

        if ( sts == EOK ) {
            return delCnt;
        }
    }

    return 0;
}


/*
 * bs_get_current_tag
 *
 * Given a tag and it's set's handle this routine will determine
 * if the tag exists and it will return the current tag if
 * the given tag's sequence number is zero.
 */

statusT
bs_get_current_tag(
                   bfSetT *bfSetp,       /* in */
                   bfTagT *bfTag         /* in/out */
                   )
{
    statusT sts;
    bfMCIdT bfMCId;
    vdIndexT vdIndex;

    if ((sts = tagdir_lookup( bfSetp,
                              bfTag,
                              &bfMCId,
                              &vdIndex
                              )) != EOK) {
        /* typically "not found" */
        return sts;
    }

    return EOK;
}


void
test_xtnt(bfAccessT *bfap)
{
    int i,j;
    int last_page = 0;

    if ( bfap->mapped == 0 ) {
        return;
    }
    if ( bfap->xtnts.validFlag == 0 ) {
        return;
    }
    if ( bfap->xtnts.xtntMap == NULL ) {
        return;
    }

    for ( i = 0; i < bfap->xtnts.xtntMap->validCnt; i++ ) {
        last_page--;
        /*
         * New subextent could start at the term block from the last subextent.
         */
        for ( j = 0; j < bfap->xtnts.xtntMap->subXtntMap[i].cnt; j++) {
            if ( last_page < (int)bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j].bsPage ) {
                last_page = bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j].bsPage;
                continue;
            }
            printf("Warning: extent error. Tag %d, ", bfap->tag.num);
            printf("last %d, current %d, ", last_page, bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j].bsPage);
            printf("bfap 0x%lx\nsubextent %d at 0x%lx, xtnt %d at 0x%lx\n",
                    bfap, i, &bfap->xtnts.xtntMap->subXtntMap[i],
                    j, &bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j]);
            i = *(int *)(-1);
        }
    }
}


/*
 * Deallocate an access structure.
 */
void
bs_dealloc_access(bfAccessT *bfap)
{

    struct vnode          *vp;

    /*
     * Sanity checks.
     */
    MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);
    MS_SMP_ASSERT(SLOCK_HOLDER(&bfap->bfaLock.mutex));
    MS_SMP_ASSERT(!bfap->onFreeList);

    /*
     * Set the state of the access structure to ACC_DEALLOC.
     * This will cause racing threads to ignore it
     * while we remove it from public lists and tables.
     */
    lk_set_state(&(bfap->stateLk), ACC_DEALLOC);

    /*
     * Do some cleanup.  Note that this code is taken directly
     * from grab_bsacc().
     */

    /*
     * NOTE: It is necessary that we do the following recycle code
     *       before removing the access structure from the hash table.
     *       If we removed the access structure from the hash table first,
     *       new opens of the file would not find this access structure,
     *       would set up their own, get their own vnode and object,
     *       and start writing to the same disk blocks as this thread.
     */

   /*
    * We must put a reference on the vnode before we remove the access
    * structure from the hashtable, and before we release the bfaLock to 
    * call any flushing code.  Once the access structure is removed from 
    * the hash table, msfs_reclaim() will no longer find it in find_bfap(). 
    * So, msfs_reclaim() could be called for this vnode and return allowing 
    * the vnode to be destroyed prior to our use of "vp" below.  If we 
    * reference it, it can not be destroyed.  
    * If the vnode has been revoked, then msfs_inactive() calls vgone() 
    * prior to clearing bfNode.accessp.  In msfs_reclaim() it gets this 
    * bfap with VTOA (rather than the expected find_bfap() which 
    * would recognize ACC_DEALLOC and fail to find our bfap), and then
    * locks the bfaLock.  We need to make sure msfs_reclaim() does not
    * try to lock an already destroyed lock.  Serialized below.
    */

    vp = bfap->bfVp;
    if (vp) {
        VN_LOCK(vp);
        /* Hold off deallocation via CACHE_LOOKUP_REF instead of using a
         * vget call, as the corresponding vrele could trigger a hierarchy
         * panic in a CFS/Advfs scenario
         */
        CACHE_LOOKUP_REF(vp);
        /*
         * Before releasing the vnode, see if it points to the 
         * access structure we're about to deallocate.  If so,
         * break the link since after we deallocate it, the
         * link will be stale.
         */
        if (VTOA(vp) == bfap) {
            struct bfNode *bnp = (struct bfNode *)&vp->v_data[0];
            bnp->accessp = NULL;
        }
        VN_UNLOCK(vp);
    }

    /*
     * Since the access structure is going away, its last vnode will not be
     * reused so it should be vgone'd to put it at the front of the vnode
     * freelist.  The following object cleanup duplicates some of the
     * processing in msfs_reclaim(), but a call to vgone() at this point
     * would wait for ACC_RECYCLE in find_bfap().  Once the bfap is removed
     * from the hash chain and bfap->bfaLock is released for a call to vgone(),
     * a new access to the previous bitfile may be found when msfs_reclaim()
     * calls find_bfap() which would prevent it from cleaning up the object.
     * Therefore, the code duplication is necessary.
     */

    if ( bfap->bfObj ) {
        MS_SMP_ASSERT(bfap->freeFwd == NULL);
        if (vp) {
            VN_LOCK(vp);
            /*
             * bfap->bfaLock guarantees that if bfap->bfVp is non-zero
             * we haven't gone through msfs_reclaim(), so vp has not been
             * reused.  Disassociate old vnode from object about to be freed.
             */
            if (vp->v_object == bfap->bfObj)
                vp->v_object = VM_UBC_OBJECT_NULL;
            VN_UNLOCK(vp);
        }
        mutex_unlock(&bfap->bfaLock);
        MS_SMP_ASSERT(vm_object_type((vm_object_t)bfap->bfObj) == OT_UBC);
        msfs_flush_and_invalidate(bfap, INVALIDATE_ALL | INVALIDATE_QUOTA_FILES);
        bfap->bfObj->vu_object.ob_ref_count -= 1;
        ubc_object_free(bfap->bfObj);
        mutex_lock(&bfap->bfaLock);
        bfap->bfObj = NULL;
        bfap->mapped = FALSE;
    }
    bfap->bfVp = NULL;


    /*
     * Unlock the access structure.  This is necessary to
     * avoid lock hierarchy violations below.
     */
    mutex_unlock(&bfap->bfaLock);

    /*
     * If the access structure is in the access structure
     * hash table, remove it from there.
     */
    if( bfap->hashlinks.dh_links.dh_next != NULL ) {
        BS_BFAH_LOCK(bfap->hashlinks.dh_key, NULL);
        if (bfap->hashlinks.dh_links.dh_next != NULL) {
            BS_BFAH_REMOVE(bfap,FALSE);
        }
        else {
             BS_BFAH_UNLOCK(bfap->hashlinks.dh_key);
        }
    }

    /*
     * If the access structure is associated with a fileset,
     * remove it from the fileset's list of access structures.
     */
    if (bfap->bfSetp) {
        RM_ACC_SETLIST(bfap, TRUE);

        /* This structure is no longer associated with a file set.
         * This must be cleared out only after it is removed from the
         * hash tbale.
         */

        bfap->bfSetp = NULL;
    }

    /*
     * Move the vnode to the head of the free list so it will be chosen
     * first and allow vnodes with cached fscontext info to remain on
     * the free list in hopes that will be re-used. We must also release
     * our reference on the vnode.
     */
    if (vp) {
        msfs_bsacc_recycles++;
        VN_LOCK(vp);
        if (vp->v_flag & VXLOCK) {
            /*
             * VXLOCK indicates we could be racing msfs_reclaim() which
             * could have already set bfap local variable.  We can't
             * destroy bfaLock until we're sure msfs_reclaim() won't
             * try to lock it.
             */
            wait_for_vxlock2(vp, VX_NOLOCK, 0);
        }
        if (vp->v_usecount == 0 && vp->v_tag == VT_MSFS) {
            vgone(vp, VX_NOSLEEP, 0);
            VN_UNLOCK(vp);
        } else {
            VN_UNLOCK(vp);
        }
        CACHE_LOOKUP_RELE(vp);
    }

    /*
     * Destroy all locks in the access structure.
     */

    /*
     * State locks.
     */
    lk_destroy(&bfap->stateLk);

    /*
     * Mutex locks.
     */
    mutex_destroy(&bfap->bfaLock);
    mutex_destroy(&bfap->bfIoLock);
    mutex_destroy(&bfap->actRangeLock);

    /*
     * Complex locks.
     */
    lock_terminate(&bfap->trunc_xfer_lk);
    lock_terminate(&bfap->cow_lk);
    lock_terminate(&bfap->clu_clonextnt_lk);
    lock_terminate(&bfap->xtnts.migTruncLk);

    /*
     * FTX locks.
     */
    lock_terminate(&bfap->mcellList_lk.lock);
    lk_destroy(&bfap->mcellList_lk);
    lock_terminate(&bfap->xtntMap_lk.lock);
    lk_destroy(&bfap->xtntMap_lk);

    /*
     * Deallocate the access structure and all
     * malloc'ed members.
     */
    if (IDX_INDEXING_ENABLED(bfap))
    {
        ms_free(bfap->idx_params);
    }
    bfap->idx_params=NULL;
    bfap->idxQuotaBlks=0;

    ASSERT(bfap->dirTruncp == NULL);
    x_dealloc_extent_maps(&bfap->xtnts);
    bfap->accMagic |= MAGIC_DEALLOC;
    ms_free(bfap);
}


/*
 * Create the thread which will allocate new access structures.
 * Since this thread is critical, its creation cannot fail.
 */
void
bs_init_access_alloc_thread(void)
{
    extern task_t  first_task;
    statusT        sts;

    /*
     * Create a message queue which threads will use to send
     * messages to the access structure allocation thread.
     */
    sts = msgq_create(&AccAllocMsgQH,           /* returned handle to queue */
                      FS_INIT_CLEANUP_Q_ENTRIES, /* initial # messages in queue */
                      sizeof(accAllocMsgT),     /* size of a message */
                      TRUE,                     /* ok to increase msg q size */
                      current_rad_id());        /* RAD to create queue on */
    if (sts != EOK) {
        ADVFS_SAD0("Could not create message queue for bs_access_alloc_thread");
        return;
    }

    /*
     * Create and start the access structure allocation thread.
     */
    if (!kernel_thread( first_task, bs_access_alloc_thread)) {
        ADVFS_SAD0("Could not create bs_access_alloc_thread");
        return;
    }
}


/*
 * This thread handles requests to allocate more access structures.
 * Since this thread is critical, it should never block during
 * an allocation request.  If it ever did, it could hang the system.
 */

void
bs_access_alloc_thread(void)
{
    accAllocMsgT   *msg;          /* Message being processed */
    bfAccessT      *bfap;         /* Pointer to new access structure */
    int            freelockheld;  /* TRUE if holding free list lock */
    unsigned int   maxPercent;    /* max. percent of vm_managed_pages to use */
    int            rad_id = 0;    /* RAD on which to malloc() bfAccessT */

    while (TRUE) {

        /*
         * Wait for the next request to allocate access structures.
         */
        msg = msgq_recv_msg(AccAllocMsgQH);

        mutex_lock(&BfAccessFreeLock);
        BfapAllocInProgress = TRUE;
        freelockheld = TRUE;

        /*
         * If root user is stuck, allow him a little breathing room.
         */
        maxPercent = msg->msgType == ALLOC_BFAP_ROOT ? 
                         AdvfsAccessMaxPercent + 1:
                         AdvfsAccessMaxPercent;

        AdvfsMinFreeAccess = max(ADVFS_MIN_FREE_ACCESS,
                                     NumAccess/10);

        /*
         * While we haven't hit our memory limit and there are less
         * than 2*AdvfsMinFreeAccess access structures on the free
         * list, keep allocating more.  If the caller specified the
         * force flag, ignore AdvfsAccessMaxPercent.  If the caller
         * specified the root flag, allow allocations of up to
         * (AdvfsAccessMaxpercent + 1) percent of vm_managed_pages.
         */
        /*
         * NOTE: Due to Advfs performance problems with heavy NFS usage, we are 
         *       checking AdvfsMinAccess which is a new hidden tunable due to be
         *       removed in a future release.  If AdvfsMinAccess is not set, it 
         *       will default to zero which will allow the conditions for the
         *       following loop to execute as originally designed.
         *
         */        
        while (FreeAcc.len < 2*AdvfsMinFreeAccess || NumAccess < AdvfsMinAccess) {
            if (((long)(NumAccess * sizeof(bfAccessT)) <
                 (long)(vm_managed_pages*PAGE_SIZE*maxPercent)/100) ||
                (msg->msgType == ALLOC_BFAP_FORCE)) {

                /*
                 * Try to allocate memory for an access structure
                 * but don't wait as this could hang the thread
                 * and prevent it from doing other work.
                 * try to allocate access structures in a 
                 * round-robin fashion across all RADs that have memory.  
                 */
                mutex_unlock(&BfAccessFreeLock);
                freelockheld = FALSE;

                /*
                 * Figure out next RAD that has memory attached to it.
                 */
                if (rad_id == nrads) {
                    rad_id = 0;
                }
                while (!MID_TO_MAD(rad_id)) {
                    rad_id++;
                    if (rad_id == nrads) {
                        rad_id = 0;
                    }
                }

                bfap = (bfAccessT*)ms_rad_malloc_no_wait(sizeof(bfAccessT),
                                                         M_PREFER, rad_id++);
                if (bfap == NULL) {
                    if (FreeAcc.len < ADVFS_MIN_FREE_ACCESS) {
                        ms_printf("Could not create new AdvFS access structure; no memory available.\n");
                        ms_printf("Applications may not be able to open more AdvFS files until\n");
                        ms_printf("more memory becomes available.\n");
                    }
                    break;
                }
                else {
                    /*
                     * We got memory for a new access structure.
                     * Initialize it and put it on the free list.
                     * Then go back and check the length of the
                     * free list again.  However, if we have now
                     * allocated an access structure as the caller
                     * desired but are now over the MaxAccess limit,
                     * don't continue to allocate more access structures.
                     */
                    access_structures_allocated++;
                    init_access(bfap);

                    if (NumAccess > MaxAccess) {
                        break;
                    }
                    else {
                        mutex_lock(&BfAccessFreeLock);
                        freelockheld = TRUE;
                    }
                }
            } else {
                /*
                 * At limits and not ALLOC_BFAP_FORCE, wait for next 
                 * request.
                 */
                break;
            }
        }
        BfapAllocInProgress = FALSE;
        if (freelockheld)
            mutex_unlock(&BfAccessFreeLock);

        /*
         * Wakeup any threads waiting on BfapAllocInProgress
         * in get_free_acc().
         */
        thread_wakeup((vm_offset_t)(&BfapAllocInProgress));

        /*
         * Put this message back on the free message queue.
         */
        msgq_free_msg(AccAllocMsgQH, msg);
    }

}

/* search_actRange_list() searches the actRange list passed in to
 * see if the actRange also passed in can be inserted.  If it can
 * the insertion point is returned.  If it cannot, NULL is returned.
 * If NULL is returned, and conflictp was passed in as a non-NULL
 * value, the address of the conflicting range is passed back in
 * the conflictp field.
 *
 * The actRangeLock must be held across this call. It will not be
 * dropped here.
 */
static actRangeT *
search_actRange_list(
    actRangeT    *arp,               /* potential actRange  */
    actRangeHdrT *actListHead,       /* list to check for conflicts */
    actRangeT   **conflictp )        /* return addr of conflicts */
{
    if ( actListHead->arCount == 0 ||    
         arp->arStartBlock > actListHead->arBwd->arEndBlock ) {
        /* Nothing on queue or actRange goes at the end of the list */
        return( (actRangeT *)actListHead );
    } else {
        /* Check if this overlaps with any existing range */
        actRangeT *listp = actListHead->arFwd;
        while ( listp != (actRangeT *)actListHead ) {
            if ( arp->arStartBlock <= listp->arEndBlock ) {
                if  (arp->arEndBlock >= listp->arStartBlock) {
                    /* Range conflict. Return NULL, and load
                     * address of conflicting range if requested.
                     */
                    if ( conflictp )
                        *conflictp = listp;
                    return( (actRangeT *)NULL );
                } else {
                    /* This is insertion point */
                    return ( listp );
                }
            } else {
                /* Keep going into the higher ranges! */
                listp = listp->arFwd;
            }
        }
        MS_SMP_ASSERT( listp != (actRangeT *)actListHead );
        /* NOTREACHED */
    }
    MS_SMP_ASSERT(0);
    return ((actRangeT *)NULL);  /* Keep the compiler from whining. */
}

/* insert_actRange_onto_list
 *
 * This routine places an actRangeT struct onto the chain for a given
 * file.  If this range overlaps one that is already active, then the
 * range will be placed at the end of a wait list and this thread will
 * go to sleep until the range is moved to the active list by the
 * remove_actRange_from_list() routine.  Also, if there are ranges on
 * the wait list when this range is requested, it will be placed onto
 * the wait list in fairness to the previous waiters.  This prevents
 * many small ranges from starving larger range requests.
 *
 * The bfap->actRangeLock will be seized and released inside  this
 * routine.
 *
 * The context pointer can be passed in if the calling routine holds the
 * file lock.  If this routine must sleep, it will release the file
 * lock before sleeping, and reseize it when it awakens.  If the caller
 * does not hold this lock, just pass NULL.
 */

void
insert_actRange_onto_list( bfAccessT *bfap,
                           actRangeT *arp,
                           struct fsContext *contextp  )
{
    actRangeT *listp;
    actRangeT *carp = (actRangeT *)NULL;     /* conflicting active range */
    int        read_lock_held = 0;

    mutex_lock(&bfap->actRangeLock);

    /* If there are actRanges waiting, then we must go to the end
     * of the wait list.  This avoids time-consuming overlap checks
     * of the actRanges on the wait list since they are not kept in
     * sorted order.  Also, known scenarios such as migrate and file
     * truncation tend to take out large active ranges that will 
     * likely conflict with most other ranges.  Well-behaved directIO
     * applications will avoid interthread range conflicts so this 
     * should not be a problem for them.
     */
    if ( bfap->actRangeList.arWaitCount )
        goto put_onto_waitlist;

    /* No waiters in front of us, so let's see if we can get onto
     * the active range list.
     */
    listp = search_actRange_list( arp, &bfap->actRangeList, &carp );
    if ( listp ) {
        /* Insert this actRangeT onto active list before listp */
        arp->arFwd = listp;
        arp->arBwd = listp->arBwd;
        listp->arBwd->arFwd = arp;
        listp->arBwd = arp;

        /* Update count for number of structs on the chain */
        bfap->actRangeList.arCount++;
        if ( bfap->actRangeList.arCount > bfap->actRangeList.arMaxLen )
            bfap->actRangeList.arMaxLen = bfap->actRangeList.arCount;

        mutex_unlock(&bfap->actRangeLock);
        return;
    }

put_onto_waitlist:
    /* Enqueue our actRange at the end of the wait list and go 
     * to sleep.  When the conflicting range has been removed, the 
     * thread that removes the range will put our range onto the 
     * active list and wake us up.
     */
    arp->arWaitFwd = (actRangeT *)(&bfap->actRangeList);
    arp->arWaitBwd = bfap->actRangeList.arWaitBwd;
    arp->arWaitBwd->arWaitFwd = arp;
    bfap->actRangeList.arWaitBwd = arp;
    bfap->actRangeList.arWaitCount++;
    if (bfap->actRangeList.arWaitCount > bfap->actRangeList.arWaitMaxLen) {
        bfap->actRangeList.arWaitMaxLen = bfap->actRangeList.arWaitCount;
    }

    /* If we know the conflicting range, bump its wait count */
    if ( carp )
        carp->arWaiters++;

    if ( contextp ) {
        /* Don't hold file_lock while sleeping; first, check to
         * see whehter it is held exclusively or shared so we
         * can reseize it in similar mode when we wake up.
         */
        read_lock_held = lock_readers( &(contextp->file_lock) );
        if ( read_lock_held ) {
            FS_FILE_UNLOCK_RECURSIVE( contextp );
        } else {
            FS_FILE_UNLOCK( contextp );
        }
    }

    mpsleep((caddr_t)(arp),               /* event addr */
                     PZERO,               /* not interruptible */
                     "actRangeWaiter",
                     FALSE,               /* no timeout */
                     simple_lock_addr(bfap->actRangeLock),
                     MS_LOCK_SIMPLE | MS_LOCK_ON_ERROR );
    
    /* When we wake up, our actRange is on the active range list.  
     * Reseize the file_lock if necessary and return.
     */
    mutex_unlock(&bfap->actRangeLock);
    if (contextp ) {
        if ( read_lock_held ) {
            FS_FILE_READ_LOCK_RECURSIVE( contextp );
        } else {
            FS_FILE_WRITE_LOCK( contextp );
        }
    }

    return;
}

/* remove_actRange_from_list
 *
 * This routine removes an actRangeT struct from the active range list
 * for a given file.  If there are any waiters, their range will be 
 * moved from the head of the wait list to the active list, if possible,
 * and they will be awakened.
 *
 * The bfap->actRangeLock must be held when this routine is called.
 */

void
remove_actRange_from_list( bfAccessT *bfap,
                           actRangeT *arp  )
{
    MS_SMP_ASSERT( arp->arFwd != NULL );
    MS_SMP_ASSERT( arp->arBwd != NULL );

    /* Remove the actRange from the list. */
    arp->arFwd->arBwd = arp->arBwd;
    arp->arBwd->arFwd = arp->arFwd;
    arp->arFwd = arp->arBwd = NULL;

    /* Update count for number of structs on the active chain */
    bfap->actRangeList.arCount--;
            
    /* If any waiters, wake them up.  This involves walking the
     * queue of waiters.  For each entry, if it can now be enqueued,
     * do so and wake the waiting thread.  If it cannot be enqueued
     * because of a conflicting active range, bump arWaiters on the
     * active range and stop walking the list.
     */
    if ( arp->arWaiters ) {
        actRangeT *waitp = bfap->actRangeList.arWaitFwd;
        actRangeT *actp, *nextwaiterp;
        arp->arWaiters = 0;

        MS_SMP_ASSERT( bfap->actRangeList.arWaitFwd != 
                       (actRangeT *)(&bfap->actRangeList) );
        while ( waitp != (actRangeT *)(&bfap->actRangeList) ) {
            actRangeT *carp = (actRangeT *)NULL;  /* conflicting range */

            actp = search_actRange_list( waitp, &bfap->actRangeList, &carp );
            if ( !actp ) {
                /* Can't move this one to active; our work here is done! 
                 * First, bump wait count on range in conflict.
                 */
                carp->arWaiters++;
                break;
            } else {

                 /* remove waitp from wait queue, and insert before actp. */
                 nextwaiterp = waitp->arWaitFwd;
                 waitp->arWaitBwd->arWaitFwd = waitp->arWaitFwd;
                 waitp->arWaitFwd->arWaitBwd = waitp->arWaitBwd;
                 waitp->arWaitFwd = waitp->arWaitBwd = (actRangeT *)(NULL);
                 bfap->actRangeList.arWaitCount--;
    
                 waitp->arFwd = actp;
                 waitp->arBwd = actp->arBwd;
                 waitp->arBwd->arFwd = waitp;
                 actp->arBwd = waitp;
                 bfap->actRangeList.arCount++;

                 /* only one thread ever sleeps on a given range */
                 thread_wakeup_one((vm_offset_t)waitp );
                 waitp = nextwaiterp;
            }
        }
    }

    return;
}


/* Given a bfap and a page number, return the active range that
 * overlaps (completely or partially) the given page. If no active
 * range contains the given page, return NULL.
 *
 * This routine assumes that the active range lock is held by the
 * caller.
 */

static struct actRange *
page_to_active_range(
    bfAccessT *bfap,
    bsPageT pg
    )
{
    actRangeT *listp;
    ulong block_bop = pg * ADVFS_PGSZ_IN_BLKS;
                                        /* block at beginning of page */
    ulong block_eop = ((ulong) pg + 1) * ADVFS_PGSZ_IN_BLKS - 1;
                                        /* block at end of page */

    /* Check for empty range */
    if ( bfap->actRangeList.arCount == 0) {
        return NULL;
    }

    /* Check the active list only */
    listp = bfap->actRangeList.arFwd;
    while ( listp != (actRangeT *)&bfap->actRangeList ) {
        /* check if the block at the beginning of the
         * page is in the active range.
         */
        if ( listp->arEndBlock >= block_bop &&
             listp->arStartBlock <= block_bop ) {
            return listp;
        }
        /* check if the block at the end of the page
         * is in the active range.
         */
        if ( listp->arEndBlock >= block_eop &&
             listp->arStartBlock <= block_eop ) {
            return listp;
        }
        /* check whether the active range is completely contained
         * within this page.
         */
        if ( listp->arEndBlock <= block_eop &&
             listp->arStartBlock >= block_bop ) {
            return listp;
        }
        /* no overlap: keep looking */
        listp = listp->arFwd;
    }

    return NULL;
}

/*
 * See the SET_LINE_AND_THREAD macro in bs_buf.h:
 * It stores  the low 36 bits of the thread id.
 * this macro is the inverse: it gets the 36 bits of
 * thread id out of the stored value.
 */
#define THREAD_ID(x) ((x) & 0xfffffffff)

/* Given a bfap and a page number, return the beginning and the size
 * of the active range that overlaps (completely or partially) the
 * given page. The active range may contain the page in its entirety,
 * but it may also only partially overlap it.
 */
statusT
limits_of_active_range(
    bfAccessT *bfap,
    bsPageT pg,
    bsPageT *beginpg,
    bsPageT *npgs
    )
{
    actRangeT *ar;

    mutex_lock(&bfap->actRangeLock);
    ar = page_to_active_range(bfap, pg);
 
    MS_SMP_ASSERT(ar);

    /* check that this thread is indeed the thread that holds
     * the active range.
     */
    MS_SMP_ASSERT(THREAD_ID(ar->arDebug)  ==
                  THREAD_ID((unsigned long)current_thread()));

    /* catch the impossible even if asserts are turned off */
    if (ar == NULL ||
        THREAD_ID(ar->arDebug) != THREAD_ID((unsigned long)current_thread())) {
        mutex_unlock(&bfap->actRangeLock);
        return E_ACTIVE_RANGE;
    }
      
    *beginpg = ar->arStartBlock / ADVFS_PGSZ_IN_BLKS;
    /* number of pages = endpage - beginpage + 1; */
    *npgs = ar->arEndBlock / ADVFS_PGSZ_IN_BLKS  - ar->arStartBlock / ADVFS_PGSZ_IN_BLKS + 1;
    mutex_unlock(&bfap->actRangeLock);
    return EOK;
}
