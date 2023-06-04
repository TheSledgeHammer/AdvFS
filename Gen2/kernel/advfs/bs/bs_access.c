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

#include <sys/param.h>
#include <sys/mount.h>
#include <sys/proc_iface.h>
#include <sys/kdaemon_thread.h>
#include <sys/time.h>
#include <sys/user.h>
#include <sys/fcntl.h>          /* for MAXEND */
#include <sys/vnode.h>          /* struct vnode */
#include <sys/spinlock.h>       /* for lock_t */

#include <ms_public.h>
#include <ms_privates.h>
#include <bs_public.h>
#include <bs_delete.h>
#include <bs_extents.h>
#include <bs_stg.h>
#include <advfs_evm.h> 
#include <fs_dir_routines.h>
#include <fs_file_sets.h>
#include <ms_assert.h>
#include <bs_index.h>
#include <ms_osf.h>
#include <bs_msg_queue.h>
#include <bs_access.h>
#include <bs_domain.h>
#include <fs_dir.h>
#include <bs_snapshot.h>
#include <tcr/clu.h>
#include <vfast.h>

#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

/*
 * Forward Declarations
 */
struct bsInMemXtntDescId;
struct bsXtntDesc;

extern uint64_t ktval_advfs_open_min;
extern uint64_t ktval_advfs_open_max;
extern struct vnodeops advfs_vnodeops;

extern domainT *DmnSentinelP;

#define ADVFS_MODULE BS_ACCESS

#define ADVFS_CREDIT_FOUND        0x00000001
#define ADVFS_CREDIT_INITWAIT     0x00000100
#define ADVFS_CREDIT_REWAIT       0x00010000
#define ADVFS_CREDIT_FORCED       0x10000000

/* private protos */

static void
test_xtnt(
    bfAccessT *bfap        /* in */
    );


static statusT
bs_map_bf(
    bfAccessT* bfAp,             /* in/out - ptr to bitfile's access struct */
    enum acc_open_flags options, /* in - options flags  */
    bfTagFlagsT  tagFlags        /* in - flags to set various bfap              values */
    );


/* 
 * This function will attemp to find a bfap in the hash table.
 * The state may possibly be ACC_DEALLOC or ACC_RECYCLE.
 */
static bfAccessT *
find_bfap(
    bfSetT *bfSetp,         /* in  - bitfile-set descriptor poitner */
    bfTagT tag,             /* in  - bitfile tag */
    int hold_hashlock,      /* in  - TRUE = return with HashLock held 
                                     FALSE = release HashLock on return */
    uint64_t *insert_count  /* out - hint indicating an insertion
                                     since last time chain was locked */
    );


/*
 * This function will acquire an access structure that is ready to be
 * used by a new file.  The access structure returned by
 * be freshly allocated, from the free list (no dirty data) or from the
 * closed list (potentially dirty data).  On return, all dirty data and
 * vnode data for the bfAccessT will have been cleaned and destroyed and the
 * access structure will be completely ready for reuse
 */
static bfAccessT *
advfs_get_new_access(ftxHT parent_ftx, /* IN - parent transaction handle */
                     int *status       /* OUT - returns status */

                    );


/* 
 * This function initializes a new allocated bfap.  
 * The function should only be called from
 * advfs_get_new_access before returning a new or recycled bfap.
 */

static void
advfs_init_access(bfAccessT* bfap       /* bfAccess to be initialized */
                 );

/* 
 * This function is used to
 * return an access structure to the access structure memory arena.  This
 * function will handle race conditions associated with deallocating access
 * structures.
 */
static statusT
advfs_dealloc_access(
                     bfAccessT *bfap
                    );

/* 
 * This function provides a simple reference mechanism for access
 * structures.  The function will attempt to bump the refCnt field of the
 * access structure but will check to see if the structure is being
 * recycled or deallocated.  If the structure is in transition, the ref will
 * fail and the functin will return a failure status.
 */
static statusT
advfs_ref_bfap(
               bfAccessT* bfap,
               enum acc_open_flags ref_flags
              );

static int32_t test_xtnts_flg = 0;

/*
 * Parameter variables dealing with the advfs_free_acc_list and
 * advfs_closed_acc_list and their management.
 */

/* Time between runs of the advfs_access_mgmt_thread */
struct timespec advfs_acc_mgmt_thread_sleep_time;

/* 
 * This structure is used to track access structure parameters.
 * advfs_get_new_access and advfs_release_access are the primary modifiers
 * of the fields in this structure.  The advfs_access_mgmt_thread uses this
 * structure to make decisions for management 
 */
#pragma align SPINLOCK_ALIGN
advfs_access_ctrl_t advfs_acc_ctrl = {0, 0, 0, 0, 0, 0};

/* Flag for access area initialized */
static int advfs_acc_ctrl_initted = 0;

/* Condition variable to trigger early wakeup of advfs_access_mgmt_thread. */
cv_t advfs_manage_access_cv;

/* Spin lock to protect advfs_manage_access_cv and advfs_vm_mem_request */
#pragma align SPINLOCK_ALIGN
spin_t  advfs_manage_access_cv_lock = {0};

int32_t advfs_vm_mem_request = 0;

/* The advfs free and closed access lists. 
 *
 * The free lists (advfs_free_acc_list[])contains free access structures
 * that are in a ACC_VALID or ACC_CREATING state and could be reopened.
 * Alternately, these access structures can be recycled with ONLY an 
 * invalidate and fcache_vn_destroy.
 *
 * The closed lists (advfs_closed_acc_list[]) contain access structures
 * without any references that are in an ACC_VALID or ACC_CREATING
 * state and could be reopened.  Files on the closed list may or may
 * not have dirty data associated.  To recycle an access structure from
 * the closed list, one must flush, invalidate then fcache_vn_destroy.
 *
 * The lists are sorted so that older structures are at the end of the
 * list.  (Newly released bfaps are put at the front, recycles are
 * taken from the end.)
 *
 * The free lists are protected by locks advfs_access_free_lock[].
 * The closed lists are protected by locks advfs_access_closed_lock[].
 */

#pragma align SPINLOCK_ALIGN
ADVSMP_FREE_LIST_T advfs_access_free_lock[ADVFS_RESOURCE_TABLE_SIZE] = {0};
#pragma align SPINLOCK_ALIGN
ADVSMP_CLOSED_LIST_T advfs_access_closed_lock[ADVFS_RESOURCE_TABLE_SIZE] = {0};
struct bfAccessHdr advfs_free_acc_list[ADVFS_RESOURCE_TABLE_SIZE];
struct bfAccessHdr advfs_closed_acc_list[ADVFS_RESOURCE_TABLE_SIZE];

/* The dynamic hashtable for bs access structures */
void * BsAccessHashTbl;

/* Per CPU indices used to access the free and closed lists */
static int32_t advfs_percpu_access_index[ADVFS_NCPUS_CONFIG] = {0};

/* The access credit table */
static int64_t advfs_access_credits[ADVFS_RESOURCE_TABLE_SIZE] = {0};

/* Initial minimum open countdown */
static int64_t advfs_access_minalloc = 0L;

/* The credit adjustment count */
static int64_t advfs_access_adjustment = 0L;

/* The borrowed credit count */
static int64_t advfs_borrowed_credits = 0L;

/* The override credit count */
static int64_t advfs_override_credits = 0L;


/* Macro to hash bfap addresses into table entries in the above */

#define ACCESS_LIST_HASH(bfab) ((((uint64_t)bfab)>>12) % ADVFS_RESOURCE_TABLE_SIZE)

/******************************************************************
***************  Utility routines ********************************
******************************************************************/

/*
 * Macros for tracking unique access structure conditions 
 */
#ifdef ADVFS_ACCESS_STATS
advfs_access_stats_t advfs_access_stats;
#endif

#ifdef ADVFS_ACCESS_TRACE 

#define ADVFS_ACCESS_LOG_SIZE 1000

bfap_trace_t advfs_access_log[ADVFS_ACCESS_LOG_SIZE];
int32_t advfs_access_cur_index = 0;
#pragma align SPINLOCK_ALIGN
spin_t access_trace_lock = {0};

void
advfs_access_logit(struct bfAccess *bfap, 
                   char* caller,
                   int line,
                   char* file,
                   off_t        offset,
                   size_t       size) 
{
#   define         FILE_Z       (sizeof(advfs_access_log[0].file))
#   define         CALLER_Z     (sizeof(advfs_access_log[0].caller))
    struct timeval t          = time;
    uint64_t       time_usecs = (t.tv_sec * 1000000LL) + t.tv_usec;
    int            strt       = (int)strlen(file) - (int)(FILE_Z + 1);

    /* Need to hold the spin lock accross the stores since
     * it is global and another thread could bump it while
     * we are using it. Same is true for bfa_trace_index.
     */
    spin_lock(&access_trace_lock);
    if (++advfs_access_cur_index >= ADVFS_ACCESS_LOG_SIZE) {
        advfs_access_cur_index = 0;
    }
    advfs_access_log[advfs_access_cur_index].bfap = bfap;
    advfs_access_log[advfs_access_cur_index].time_usecs = time_usecs;
    advfs_access_log[advfs_access_cur_index].tag = bfap->tag.tag_num;
    advfs_access_log[advfs_access_cur_index].seq = bfap->tag.tag_seq;
    advfs_access_log[advfs_access_cur_index].bfState = bfap->bfState;
    advfs_access_log[advfs_access_cur_index].state = bfap->stateLk.state;
    advfs_access_log[advfs_access_cur_index].refCnt = bfap->refCnt;
    advfs_access_log[advfs_access_cur_index].refs_from_snaps_children = 
                                bfap->bfaRefsFromChildSnaps;
    advfs_access_log[advfs_access_cur_index].v_count = bfap->bfVnode.v_count;
    advfs_access_log[advfs_access_cur_index].v_scount = bfap->bfVnode.v_scount;
    advfs_access_log[advfs_access_cur_index].file_size = bfap->file_size;
    advfs_access_log[advfs_access_cur_index].orig_file_size = bfap->bfa_orig_file_size;
    advfs_access_log[advfs_access_cur_index].bfaFlags = bfap->bfaFlags;
    advfs_access_log[advfs_access_cur_index].bfa_next_fob = bfap->bfaNextFob;
    advfs_access_log[advfs_access_cur_index].bfaWriteCnt = bfap->bfaWriteCnt;
    advfs_access_log[advfs_access_cur_index].v_type= bfap->bfVnode.v_type;
    advfs_access_log[advfs_access_cur_index].line = line; 
    advfs_access_log[advfs_access_cur_index].idx_params = bfap->idx_params; 
    advfs_access_log[advfs_access_cur_index].offset= offset; 
    advfs_access_log[advfs_access_cur_index].size= size; 
    strncpy(advfs_access_log[advfs_access_cur_index].caller, caller, CALLER_Z);
    strncpy(advfs_access_log[advfs_access_cur_index].file, &file[strt], FILE_Z);
    advfs_access_log[advfs_access_cur_index].thread = u.u_kthreadp;
    spin_unlock(&access_trace_lock);

    /* Now for this bfap's private log */
    spin_lock( &bfap->bfa_trace_lock);
    if (++bfap->bfa_trace_index >= ADVFS_PER_ACCESS_TRACE) {
        bfap->bfa_trace_index = 0;
    }
    bfap->bfa_trace[bfap->bfa_trace_index].bfap = bfap;
    bfap->bfa_trace[bfap->bfa_trace_index].time_usecs = time_usecs;
    bfap->bfa_trace[bfap->bfa_trace_index].tag = bfap->tag.tag_num;
    bfap->bfa_trace[bfap->bfa_trace_index].seq = bfap->tag.tag_seq;
    bfap->bfa_trace[bfap->bfa_trace_index].bfState = bfap->bfState;
    bfap->bfa_trace[bfap->bfa_trace_index].state = bfap->stateLk.state;
    bfap->bfa_trace[bfap->bfa_trace_index].refCnt = bfap->refCnt;
    bfap->bfa_trace[bfap->bfa_trace_index].refs_from_snaps_children = 
                        bfap->bfaRefsFromChildSnaps;
    bfap->bfa_trace[bfap->bfa_trace_index].v_count = bfap->bfVnode.v_count;
    bfap->bfa_trace[bfap->bfa_trace_index].v_scount = bfap->bfVnode.v_scount;
    bfap->bfa_trace[bfap->bfa_trace_index].file_size = bfap->file_size;
    bfap->bfa_trace[bfap->bfa_trace_index].orig_file_size = bfap->bfa_orig_file_size;
    bfap->bfa_trace[bfap->bfa_trace_index].bfaFlags= bfap->bfaFlags;
    bfap->bfa_trace[bfap->bfa_trace_index].bfa_next_fob = bfap->bfaNextFob;
    bfap->bfa_trace[bfap->bfa_trace_index].v_type= bfap->bfVnode.v_type;
    bfap->bfa_trace[bfap->bfa_trace_index].bfaWriteCnt = bfap->bfaWriteCnt;
    bfap->bfa_trace[bfap->bfa_trace_index].line = line; 
    bfap->bfa_trace[bfap->bfa_trace_index].idx_params = bfap->idx_params; 
    bfap->bfa_trace[bfap->bfa_trace_index].offset = offset; 
    bfap->bfa_trace[bfap->bfa_trace_index].size = size; 

    strncpy(bfap->bfa_trace[bfap->bfa_trace_index].caller, caller, CALLER_Z);
    strncpy(bfap->bfa_trace[bfap->bfa_trace_index].file, &file[strt], FILE_Z);
    bfap->bfa_trace[bfap->bfa_trace_index].thread = u.u_kthreadp;
    spin_unlock( &bfap->bfa_trace_lock);

}
#endif

#ifdef ADVFS_TAG_TRACE
#   define TAG_TRACE_HISTORY 200
#   define TAG_TRACE_NUM_BUCKETS 512 /* must be a power of 2 */
    typedef struct {
        uint32_t        seq;
        uint16_t        mod;
        uint16_t        ln;
        struct kthread *thd;
        int             hash;
        int             qual;
        void           *val;
    } tagTraceElmtT;

    typedef struct {
        int           tagTracePtr;
        tagTraceElmtT tagTraceBuf[TAG_TRACE_HISTORY];
    } tagTraceT;
    tagTraceT TagTrace[TAG_TRACE_NUM_BUCKETS];

    /* For tag tracing, hash can be the file tag. qual can be the set tag */
    /* For SBM block tracing, hash can be the int index. qual can be the SBM page. */
void
tag_trace(uint32_t hash, uint32_t qual, uint16_t module, uint16_t line, void *value)
{
     tagTraceElmtT *te;
    extern mutexT TraceLock;
    extern int TraceSequence;
    tagTraceT *tt;

    ADVMTX_TRACE_LOCK(&TraceLock);

    tt = &TagTrace[hash & (TAG_TRACE_NUM_BUCKETS - 1)];
    tt->tagTracePtr = (tt->tagTracePtr + 1) % TAG_TRACE_HISTORY;
    te = &tt->tagTraceBuf[tt->tagTracePtr];
    te->thd = (struct kthread *)(((int64_t)getprocindex() << 36) |
                             (int64_t)u.u_kthreadp & 0xffffffff);
    te->seq = TraceSequence++;
    te->mod = module;
    te->ln = line;
    te->hash = (signed)hash;
    te->qual = (signed)qual;
    te->val = value;

    ADVMTX_TRACE_UNLOCK(&TraceLock);
}
#endif

typedef enum {
    APAL_INVAL = 0x1,
    APAL_FLUSH_INVAL = 0x2,
} apal_options;

#define AL_NOTLOCKED 0
#define AL_LOCKED    1

/*
 * BEGIN_IMS
 *
 * advfs_process_access_list - This function is designed to process/walk an
 * access structure list (free or closed) and find an available access
 * structure to be recycled.  The list to be used and the lock to protect
 * that list are parameterized, along with the actions necessary to handle
 * the bfap's data.
 *
 * Note: We walk both lists backwards because newer access structures
 * are freed to the front of the list.  We will try to reuse older
 * structures that have had time to flush themselves.
 *
 * Parameters:
 *      list_p - Pointer to the list to be processed.  Should be either
 *       advfs_free_acc_list or advfs_closed_acc_list.
 *      list_lock - pointer to the lock to be used to protect the list
 *       during walking.  Should be either advfs_access_free_lock or 
 *       advfs_access_closed_lock
 *      recycle_action - should specify APAL_INVAL or APAL_FLUSH_INVAL.
 *      lock_flag - whether or not lock is initially held.
 * 
 * Return Values:
 *      On success, the returned bfap pointer is the bfap to be recycled.
 *      On failure, NULL is returned.  Failure indicates the list could not
 *        be used to procure an access structure after multiple retries.
 *
 * Entry/Exit Conditions:
 *      On entry, no locks are held.
 *      During execution, bfaLocks will be taken for each bfap to be
 *        potentially returned.
 *      On exit, the bfaLock is held if the pointer returned in non-NULL.
 *
 * Algorithm:
 *      The basic algorithm is to take the list_lock and walk the list
 *      backwards doing lock tries on each bfap's bfaLock until success.
 *      When a bfaLock is taken, the bfap is set to ACC_RECYCLE to block out
 *      any refCnt's being placed on the bfap (assuming everyone uses either
 *      find_bfap or advfs_ref_bfap).  The bfaLock is then dropped and the
 *      bfap is either invalidated or flushed and invalidated depending on
 *      recycle_action.  The vnode associated with the bfap is then
 *      destroyed from the ufc perspective.  
 *      Before doing the inval/flush-inval, the vnode is purged from the
 *      dnlc and checked for races.  In the event of a race, the bfap is
 *      skipped and left for the other thread to use.  We will then start at
 *      the end of the list and search for another access structure.  It is
 *      assumed that the racer will quickly remove the bfap that failed from
 *      the free/closed list and we will only encounter it a couple times.
 *      We will retry walking the list (always from the end) a number of
 *      times equal to the list length on entry to the function.  If no bfap
 *      is found suitable by then, NULL will be returned.
 *
 * END_IMS
 */

static bfAccessT*
advfs_process_access_list(
        struct bfAccessHdr*   list_p,
        ADVSMP_ACCESS_LIST_T* list_lock_p,
        apal_options          recycle_action,
	advfs_list_state_t    list_state,
        int32_t               lock_flag,
        ftxHT                 parent_ftx)
{

    /* 
     * We will retry the same number of times as the list length on entry.
     * This is purely heuristic.
     */
    int32_t list_iteration = 0;
    int32_t list_length = list_p->len;
    bfAccessT* bfap = NULL;
    lkStatesT previous_state = LKW_NONE;
    int32_t have_access_struct = FALSE;
    int32_t contact_access_mgmt = FALSE;
    
list_search:
    list_iteration++;
    if (list_iteration > list_length) {
        if( lock_flag ) {
            ADVSMP_ACCESS_LIST_UNLOCK ( list_lock_p );
            lock_flag = AL_NOTLOCKED;
        }
	goto empty_handed;
    }

    /* 
     * Attempt to acquire a new bfAccessT from the free/closed list,
     * if we succeed, we must invalidate the pages associated with the bfap. 
     */
    if( !lock_flag ){
        ADVSMP_ACCESS_LIST_LOCK ( list_lock_p );
    }

    /* recheck length under lock */
    if ( list_p->len == 0 ) {
	list_length = list_p->len;
	ADVSMP_ACCESS_LIST_UNLOCK ( list_lock_p );
        lock_flag = AL_NOTLOCKED;
	goto empty_handed;
    }
     
    /* 
     * Walk the free/closed list looking for a good bfap to recycle. 
     */
    bfap = list_p->freeBwd;
    while ( bfap != (bfAccessT*)list_p) {
	/* We need this lock to take the bfap off the free/closed list,
	 * but the usual order is backwards.  If we can't get the lock, 
	 * it must be in use by someone else, so we can't reuse it 
	 * anyways. */
	if ( ADVSMP_BFAP_TRYLOCK( &bfap->bfaLock ) ) {
	    /*
	     * At this point, we hold the bfaLock and the list_lock.
	     * The problem is that someone may have gotten to this bfap
	     * via the access set list or pulled it off the list it is on
	     * (list_p).  That would mean we might
	     * change the state right out from under them.  We need to
	     * check for a refCnt and just skip this if one exists.
	     * Also, files that have dirty statistics to flush to disk
	     * must do so within a transaction. 
	     * Usually, advfs_close() flushes any dirty stats before
	     * the access structure is put onto the closed list.  But
	     * if we are a CFS server, the VOP_CLOSE() may actually
	     * arrive BEFORE the VOP_WRITE() that sets dirty_stats to
	     * TRUE. There is a potential conflict if we were to flush
	     * this file's stats under a transaction for a different
	     * domain than the caller. So skip files with dirty stats
	     * whose domain is different than the caller's parent_ftx
	     * domain. The exception is the caller can flush the file's
	     * stats as long as the caller has no parent transaction as
	     * determined by checking the parent_ftx for a NULL domain 
	     * pointer. The access management thread will eventually 
	     * flush the file's stats.
	     * Additionally, someone may be in the process of creating
	     * this file.  If the bfap is in ACC_CREATING, we shouldn't
	     * recycle it out from under the creator. 
	     */      
	    if ((bfap->refCnt) || 
		((bfap->bfFsContext.dirty_stats) &&
		 ((parent_ftx.dmnP != NULL) && 
		  (parent_ftx.dmnP != bfap->dmnP))) ||
		(lk_get_state( bfap->stateLk ) == ACC_CREATING) ) {
		
		/* Set a flag to wakeup access mgmt thread if any bfap
		 * has dirty stats that we cannot update in-line.
		 */
		if (bfap->bfFsContext.dirty_stats && 
		    ((parent_ftx.dmnP != NULL) &&
		     (parent_ftx.dmnP != bfap->dmnP))) {
		    contact_access_mgmt = TRUE;    
		}
		ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
		ACCESS_STATS( process_access_list_refcnt_or_creating );
		bfap = bfap->freeBwd;
		continue;
	    }
	    
	    /* We got the lock */
	    RM_ACC_LIST_NOLOCK( bfap, list_p, list_lock_p );
	    have_access_struct = TRUE;
	    /* The previous_state should be ACC_VALID.
	     * If we race and find v_count or v_scount up after we set
	     * ACC_RECYCLE, we need to be really careful about setting
	     * the state back to the previous.  ACC_RECYCLE is
	     * sufficient to prevent anyone from bumping the refCnt.
	     * The refCnt must be 0 to get this far. Once ACC_RECYCLE
	     * is set, any racers will block in advfs_lookup_valid_bfap 
	     * on the ACC_RECYCLE state.  However, if they already have
	     * a v_count or v_scount, we should let them win, so we will
	     * set ACC_RECYCLE now, then check for races and potentially
	     * set the state back to the previous state.  
	     */
	    previous_state = lk_get_state( bfap->stateLk );
	    MS_SMP_ASSERT(previous_state == ACC_VALID);
	    (void)lk_set_state ( &bfap->stateLk, ACC_RECYCLE );
	    lk_signal( UNLK_BROADCAST, &bfap->stateLk );
	    /* Unlock the bfap while performing the fcache_vn_invalidate
	     * and the fcache_vn_destroy.  The ACC_RECYCLE state is what
	     * is protecting this bfap from having anyone else touch the
	     * data as we are about to flush and reuse this bfap.  */
	    ADVSMP_BFAP_UNLOCK ( &bfap->bfaLock );
	    break;
	} else {
	    ACCESS_STATS( process_access_list_failed_to_lock );
	    /* Didn't get the lock, go to next bfap on free/closed list 
	     * and continue */
	    bfap = bfap->freeBwd;
	    continue;
	}
    } /* while */

    ADVSMP_ACCESS_LIST_UNLOCK ( list_lock_p );
    lock_flag = AL_NOTLOCKED;
    
    /* 
     * If we got an access structure, process it. Otherwise all done.
     */
    if (!have_access_struct) {
	goto empty_handed;
    }

    /* 
     * Prevent vnode v_count increment race by external ref (dnlc ref).
     * We must purge the name cache to prevent this race. 
     */
        
    /*
     * Internal code should not directly reference the vnode (bump the
     * v_count).  Instead, the code should use advfs_ref_bfap to get a
     * ref count on the bfap.  This will prevent races when the vnode
     * v_count is bumped.  If advfs_ref_bfap fails when a v_count is
     * required, it means that a race was lost and the vnode will be
     * going away.
     */
    dnlc_purge_vp( &bfap->bfVnode );
    
    if ( bfap->bfVnode.v_count || bfap->bfVnode.v_scount ) {
	/* We lost the race.  We wanted to recycle this bfap, but
	 * someone just snuck in and put a reference on the vnode
	 * through the dnlc. We must reset the state to whatever it was
	 * before we moved it to ACC_RECYCLE.  The assumption is that
	 * if we are racing with someone who is going to change the
	 * state from the previous state (ACC_VALID or ACC_CREATING) to
	 * something new, they will be going through the usual
	 * advfs_lookup_valid_bfap or advfs_ref_bfap paths that will
	 * check for ACC_RECYCLE.  This will prevent them from racing
	 * with changing the state back.  
	 */
	have_access_struct = FALSE;
	ACCESS_STATS( process_access_list_recycle_race );
	ADVFS_ACCESS_LOGIT( bfap, "process_acces_list_raced v_*count");
	
	ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	(void)lk_set_state( &bfap->stateLk, previous_state);
	if (list_state == ON_FREE_LIST) {
	    ADVFS_ADD_TO_FREE_LIST( bfap, 
				    list_p,
				    list_lock_p);
	} else if (list_state == ON_CLOSED_LIST) {
	    ADVFS_ADD_TO_CLOSED_LIST( bfap, 
				      list_p,
				      list_lock_p);
	} else {
	    panic("illegal list state");
	}
	
	lk_signal( UNLK_BROADCAST, &bfap->stateLk );
	ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
	
	ACCESS_STATS( process_access_list_search_again );
	goto list_search;
	
    } else {
	/* 
	 * We aren't racing with someone trying to access the vnode 
	 * so we need to invalidate any in-cache pages, and destroy the
	 * vnode 
	 */
	int err;
	if (recycle_action == APAL_INVAL) {
	    /* 
	     * Should be processing the free list... we don't expect
	     * dirty data here.
	     */
	    err=fcache_vn_invalidate( &bfap->bfVnode,
				      (int64_t)0, (uint64_t)0, /*The entire file*/
				      NULL, /* Private params */
				      (BS_IS_META(bfap)?FVI_PPAGE:0)|FVI_INVAL);
	} else {
	    err = fcache_vn_flush( &bfap->bfVnode,
                                   (int64_t)0, (uint64_t)0,
				   /* The entire file */
                                   NULL, /* Private params */
                                   FVF_WRITE | FVF_SYNC | FVF_INVAL);
	    
	    /* Flush any dirty file statistics to disk. */
	    if (bfap->bfFsContext.dirty_stats) {
		(void)fs_update_stats(&bfap->bfVnode, bfap, parent_ftx, 0);
	    }
	}
	
	/*
	 * If the above functions succeeded, try to destroy the vnode.
	 * No different error conditions need to be handled, so if the
	 * above failed, overwrite err with the result from
	 * fcache_vn_destroy.  Handle err != EOK afterwards.
	 */
	
	ADVRWL_BFAP_FLUSH_WRITE(bfap);
	
	if (err == EOK) { 
	    err=fcache_vn_destroy( &bfap->bfVnode,
				   FVD_DEFAULT);
	} 
	
	if (err != EOK) {
	    ADVRWL_BFAP_FLUSH_UNLOCK(bfap);
	    have_access_struct = FALSE;
	    ACCESS_STATS( process_access_list_flush_error );
	    /*
	     * We failed the flush or invalidate and it might not be
	     * safe to recycle this access structure.  We risk data
	     * corruption.  We will screen a domain panic now since if
	     * the bfap was metadata, we could have panic.  In domain
	     * panic, we'll reset the state and return NULL. Otherwise, 
	     * we will try to process the list again for another
	     * available bfap.
	     */
	    
	    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	    (void)lk_set_state( &bfap->stateLk, previous_state);
	    lk_signal( UNLK_BROADCAST, &bfap->stateLk );
	    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
	    
	    if (bfap->dmnP->dmn_panic) {
		return NULL;
	    } else {
		ACCESS_STATS( process_access_list_search_again_2 );
		goto list_search;
	    }
	}
	
	/* 
	 * Remove bfap from hashlinks if on one 
	 */
	if ( bfap->hashlinks.dh_links.dh_next != NULL ) {
	    BS_BFAH_LOCK(bfap->hashlinks.dh_key, NULL);
	    /* 
	     * There is a definite expectation and requirement than
	     * anyone else trying to remove the bfap checks the
	     * ACC_RECYCLE state and blocks on the state.  We will
	     * simply assert than no one else snuck in a removed the
	     * bfap from the hashlist.
	     */
	    if (bfap->hashlinks.dh_links.dh_next != NULL) {
		BS_BFAH_REMOVE(bfap, FALSE);
	    } else {
		MS_SMP_ASSERT( FALSE ); 
	    }
	}
	
	/* 
	 * Remove from bfSet access lists 
	 */
	if ( BFSET_VALID(bfap->bfSetp) ) {
	    RM_ACC_SETLIST(bfap, TRUE);
	    bfap->bfSetp = NULL;
	}
	
	/*
	 * Free the ACL cache if it is not null
	 */
	if( bfap->bfa_acl_cache != NULL ) {
	    ms_free( bfap->bfa_acl_cache );
	    bfap->bfa_acl_cache = NULL;
	    bfap->bfa_acl_cache_len = 0;
	}
	
	/* We have synchronized with the flush routines at this
	 * point. The flush has either skipped this access struct
	 * since it failed the lock_try or blocked on the chain
	 * lock and wont even see it.
	 */
	
	ADVRWL_BFAP_FLUSH_UNLOCK(bfap);
	
	/*
	 * Any directory truncation should be complete by this time, if
	 * not, then just free the message and it will get truncated
	 * some other time.
	 */
	if ( bfap->dirTruncp != NULL ) {
	    ms_free( bfap->dirTruncp );
	    bfap->dirTruncp = NULL;
	}
	
	/*
	 * Free the previous file's in memory extent map.
	 */
	x_dealloc_extent_maps( &bfap->xtnts );
	
	/* 
	 * If this bfap was last used by a directory with an index or an
	 * index itself, then free up the associated structure 
	 */
	if (IDX_INDEXING_ENABLED(bfap)) {
	    ms_free(bfap->idx_params);
	}
	bfap->idx_params=NULL;
	bfap->idxQuotaBlks=0;
	
	if ( bfap->bfFsContext.quotaInitialized ) {
	    (void)advfs_fs_detach_quota( &bfap->bfFsContext);
	    bfap->bfFsContext.quotaInitialized = 0;
	}
	
	/* 
	 * Locking the bfaLock here gives us synchronization with anyone
	 * who found the access struct in the hash table or bfSet access
	 * list before we pulled it off.  Those finders should be
	 * checking stateLk with bfaLock held and will shortly release
	 * the bfaLock and skip this bfap.
	 */
	ADVSMP_BFAP_LOCK( &bfap->bfaLock );
	
	/* Wakeup access mgmt thread to flush closed list files 
	   with dirty stats.*/
	if (contact_access_mgmt)
	    cv_signal( &advfs_manage_access_cv, NULL, CV_NULL_LOCK ); 

	/* 
	 * Everything should be setup like a new bfAccessT from
	 * advfs_init_access.  At least everything important should be.
	 */
	ACCESS_STATS( process_access_list_succeeded ); 
	return bfap;
    }

 empty_handed:
    
    /* Wakeup access mgmt thread to flush closed list files 
       with dirty stats.*/
    if (contact_access_mgmt) {
	cv_signal( &advfs_manage_access_cv, NULL, CV_NULL_LOCK ); 
    }

    if (list_length && list_iteration > list_length) {
	ACCESS_STATS( process_access_list_max_tries_exceeded );
    }

    return NULL;
}

/*
 * advfs_get_access_credit - gets a credit for holding a bfap.
 *
 * Parameters:
 *      open_flags - not zero if override allowed.
 * 
 * Return Values:
 *      On failure, zero is returned.
 *      On success, a non-zero value is returned.
 *         ADVFS_CREDIT_FOUND    A credit was obtained.
 *         ADVFS_CREDIT_INITWAIT The initial malloc should wait.
 *         ADVFS_CREDIT_REWAIT   We should re-wait if a bfap can't be recycled.
 *         ADVFS_CREDIT_FORCED   The credit was forced.
 *
 * Entry/Exit Conditions:
 *
 * Algorithm:
 * *
 */
static int32_t
advfs_get_access_credit( enum acc_open_flags open_flags )
{
    uint32_t cpu;
    uint32_t index;
    uint32_t count;
    int64_t  value64;
    int32_t  ret;
    int32_t  saved_uerror;

    ret = ADVFS_CREDIT_FOUND;

    /* Setup return for if we are below open_min */

    if( advfs_access_minalloc > 0)
    {
        ADVFS_ATOMIC_FETCH_DECR( &advfs_access_minalloc, &value64 );
        if( value64 > 0 ) {
            ret = ADVFS_CREDIT_FOUND|ADVFS_CREDIT_INITWAIT;
        } else ADVFS_ATOMIC_FETCH_INCR( &advfs_access_minalloc, &value64 );
    }

    /* Get cpu number to start */

    cpu = index = how_many_processors() - getprocindex();
    count = ADVFS_RESOURCE_TABLE_SIZE;

    /* Loop looking for a count with something left */

    while( count-- > 0 )
    {
        if( advfs_access_credits[index] > 0 ) {
            ADVFS_ATOMIC_FETCH_DECR( &advfs_access_credits[index], &value64 );
            if( value64 > 0 ) {
#ifdef ADVFS_ACCESS_STATS
                if( index == cpu )
                    ACCESS_STATS( get_credit_got_credit_locally );
                else
                    ACCESS_STATS( get_credit_stole_credit );
#endif                           
                return ret; /* Got a credit, return success and wait flag */
            } else {
            /* We lost a race. The credit count was positive when we
             * checked it but <= 0 when we decremented. Repair and move on.
             */
                ADVFS_ATOMIC_FETCH_INCR( &advfs_access_credits[index], &value64 );
            }
        }
        index = ( index + cpu + 1 ) % ADVFS_RESOURCE_TABLE_SIZE;
    }

   /* Check for recovery thread and allow a credit and bump soft max
    * if it is */

    if ( CLU_CFS_IS_RECOVERY_THREAD( 0 ) ) {
        ADVFS_ATOMIC_FETCH_INCR( &advfs_acc_ctrl.acc_ctrl_soft_max, &value64 );
        ACCESS_STATS( get_credit_bumped_soft_max_for_failover );
        return ret;
    }

   /* Couldn't find a credit. Check for override and return override
    * return forced + re-wait */

    if( open_flags == BF_OP_OVERRIDE_SMAX ) {
        ADVFS_ATOMIC_FETCH_INCR( &advfs_override_credits , &value64 );
        return ret;
    }
   /* Couldn't find a credit. Check for for super user and return 
    * forced + re-wait */
    saved_uerror = u.u_error;
    if ( suser() ) {
        ret |= ADVFS_CREDIT_FORCED|ADVFS_CREDIT_REWAIT;
        ADVFS_ATOMIC_FETCH_INCR( &advfs_borrowed_credits , &value64 );
        ACCESS_STATS( get_credit_forced_credit );
    } else ret = 0;
    u.u_error = saved_uerror;
    return ret;
}

/*
 * advfs_release_access_credit - releases a credit for holding a bfap.
 *
 * Parameters:
 *      None.
 * 
 * Return Values:
 *      None.
 *
 * Entry/Exit Conditions:
 *
 * Algorithm:
 * *
 */
static
void
advfs_release_access_credit()
{
    uint32_t cpu;
    int64_t  value64;
    int32_t  saved_uerror;
    int32_t  i;

   /* If there were override credits, give them back first */

    if( advfs_override_credits ) {
        ADVFS_ATOMIC_FETCH_DECR( &advfs_override_credits , &value64 );
        if( value64 > 0 ) {
            return;
        } else  ADVFS_ATOMIC_FETCH_INCR( &advfs_override_credits , &value64 );
    }
    /* If there are borrowed credits, see if we were eligible to borrow one.
     * if we were give it back instead (it might not be ours, but it doesn't
     * matter. We could always give back borrowed ones first, but that would
     * migrate real ones to privileged components and starve applications.
     */
    
    if( advfs_borrowed_credits ) {
        saved_uerror = u.u_error;
        if ( suser() ) {
            ADVFS_ATOMIC_FETCH_DECR( &advfs_borrowed_credits , &value64 );
            if( value64 > 0 ) {
                u.u_error = saved_uerror;
                return;
            } else  ADVFS_ATOMIC_FETCH_INCR( &advfs_borrowed_credits , &value64 );
        }
        u.u_error = saved_uerror;
    }

    /* Get cpu number to start */

    cpu = how_many_processors() - getprocindex();
    ADVFS_ATOMIC_FETCH_INCR( &advfs_access_credits[cpu], &value64 );
    return;
}
/*
 * BEGIN_IMS
 *
 * advfs_get_new_access - gets a new access structure that is ready for
 * (re)use by a new file.  All race conditions and flushes have been handled
 * for the access structure on return if the access structure is being
 * reused from another file.
 *
 * Parameters:
 *      parent_ftx - Parent transaction handle.
 *      status - a pointer to an int to hold any error status.  Should be
 *      EOK on success.
 *
 * 
 * Return Values:
 *      On error, bfAccess* will be NULL and status will contain the error
 *      code.  Most likely cause of failure is EHANDLE_OVF to indicate that
 *      the maximum number of access structures open has been reached.
 *
 * Entry/Exit Conditions:
 *      On entry, no locks are held.
 *      On successful exit, the bfaLock is held for the bfAccess structure
 *      returned.
 *      The advfs_access_free_lock and the advfs_access_closed_lock may or may
 *      not be taken during this function.
 *      On entry, it is assumed that the purpose of calling this function is
 *      to open an access structure.  If an access structure is returned,
 *      the advfs_acc_ctrl.acc_ctrl_open_cnt will be bumped to account for
 *      this new open.  advfs_release access must be called on this access
 *      structure to decrement the count.
 *      On successful exit, the bfap has been initialized by a call to 
 *      advfs_init_access and is in state ACC_RECYCLE.
 *
 *
 * Algorithm:
 *      The basic algorithm here is to try to malloc WAIT an access
 *      structure if the number of open access structures is below the
 *      acc_ctrl_min value.  If we are above the minimum, the try a malloc
 *      NOWAIT.  If we get an access structure, bump the acc_ctrl_open_cnt
 *      and acc_ctrl_access_cnt (if malloced).  If the malloc didn't
 *      succeed, we will bump the acc_ctrl_open_cnt but decrement the
 *      acc_ctrl_access_cnt as a new access structure was not brought into
 *      existence.  If we have an access structure, we return, otherwise we
 *      try to recycle one first off the free list then off the closed list.
 *      If we are unable to recycle and we are either a CFS recovery thread,
 *      super user or below the acc_ctrl_soft_max value we will do a malloc WAIT to
 *      get the access structure.
 *
 * END_IMS
 */

static bfAccessT*
advfs_get_new_access(ftxHT parent_ftx, /* IN - parent FTX handle */ 
                     int *status)      /* OUT - return status */        

{
    int retry_alloc_with_wait = FALSE;  /* If TRUE, we may want to retry an 
                                         * block kmem_arena_alloc */
    bfAccessT *bfap = NULL;             /* bfap to be returned */

    int32_t    credit;
    int32_t    index;
    uint32_t   cpu;
    uint64_t   value64;
    int32_t    i;
    int32_t    j;
    int32_t    saved_uerror;
 
    *status = EOK;


    /* First we must obtain a credit for holding a bfap.
     */

    if( ( credit = advfs_get_access_credit( BF_OP_NO_FLAGS ) ) == 0 ) {
        *status = EHANDLE_OVF;
        return NULL;
    }

    /* If the number of existing access structures is less than soft max, we'll
     * try an alloc here first. (If we're below min, this should always happen).
     *
     * Should we try the malloc also if there's a borrowed credit? We could
     * potentially have applications fail if they get a credit but a bfap has
     * been recycled on a borrowed credit.
     */

   if(   advfs_acc_ctrl.acc_ctrl_access_cnt
       < advfs_acc_ctrl.acc_ctrl_soft_max ) {
       if( credit & ADVFS_CREDIT_INITWAIT ) {
          bfap = (bfAccessT*)kmem_arena_alloc( advfs_bfaccess_arena, M_WAITOK );
          ACCESS_STATS( get_new_access_malloc_wait );
       } else {
          bfap = (bfAccessT*)kmem_arena_alloc( advfs_bfaccess_arena, M_NOWAIT );
          ACCESS_STATS( get_new_access_malloc_no_wait );
       }
   } else ACCESS_STATS( get_new_access_no_malloc_attempt );

    /* We don't currently do these two things that used to happen here.
     * How do we do them now?
     *
     * Check to see if the access_mgmt_thread might need to be more
     * proactive about dealing with access structures.
     *
     * See if we need to take action on the soft_max because of CFS
     * recovery.  If this is the case, then we will bump soft_cnt here so
     * we will still try to recycle of the free and closed lists.  Bump soft
     * max if we have more than 90% of soft max as open access structures.
     */

    /*
     * Check to see if the malloc was successful.
     */

    if (bfap != NULL) {
        ADVFS_ATOMIC_FETCH_INCR( &advfs_acc_ctrl.acc_ctrl_access_cnt,
                           &value64 );
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
#       ifdef ADVFS_SMP_ASSERT
            /*
             * This is part of a Memory Leak check.  Initializing these fields
             * should not be an issue right here, but we are putting additional
             * asserts in advfs_init_access() to make sure that these fields are
             * initialized this way.  The real worry is when recycling a bfap,
             * not when getting one from the arena.
             */
            bfap->xtnts.validFlag = XVT_INVALID;
            bfap->xtnts.xtntMap = NULL;
            bfap->xtnts.copyXtntMap = NULL;
            bfap->dirTruncp = NULL;
#       endif /* ADVFS_SMP_ASSERT */
        advfs_init_access( bfap );
        (void)lk_set_state( &bfap->stateLk, ACC_RECYCLE ); 
        *status = EOK;
        ADVFS_ACCESS_LOGIT( bfap,
             "advfs_get_new_access: returning malloced bfap");
        return bfap;
    } else {
        ACCESS_STATS( get_new_access_no_initial_bfap_malloc );
    }

    /* 
     * We failed to acquire a bfAccess structure from the arena without
     * having to wait. We will try to grab an access structure from the free
     * lists.
     */

    /* Get cpu number  and number of tries */
    cpu = getprocindex();
    i = ADVFS_RESOURCE_TRIES;
    j = ADVFS_RESOURCE_TABLE_SIZE;

    /* Loop for n tries doing trylocks first, see if we can get one. */
    while( i && j ) {
      index = advfs_percpu_access_index[cpu];
      if( advfs_free_acc_list[index].len ) {
        if(     spin_trylock( &advfs_access_free_lock[index] )
            && ( bfap = advfs_process_access_list(
                             &advfs_free_acc_list[index], 
                             &advfs_access_free_lock[index],
                             APAL_INVAL, ON_FREE_LIST, AL_LOCKED,
                             parent_ftx) ) ) {

#ifdef ADVFS_ACCESS_STATS
            if( i ==  ADVFS_RESOURCE_TRIES )
                ACCESS_STATS( get_new_access_free_direct_hit );
            else
                ACCESS_STATS( get_new_access_free_fast_recycle );
#endif                           
            break;
        } else {
            i--;
        }
        index = (index+cpu+1) % ADVFS_RESOURCE_TABLE_SIZE;
        advfs_percpu_access_index[cpu] = index;
      } else {
        j--;
      }
    }

    /* Loop through whole table doing full locks. see if we can get one. */
    if( !bfap )
    {
        i = ADVFS_RESOURCE_TABLE_SIZE;
        while(i) {
            index = advfs_percpu_access_index[cpu];
            if( ( bfap = advfs_process_access_list(
                             &advfs_free_acc_list[index], 
                             &advfs_access_free_lock[index],
                             APAL_INVAL, ON_FREE_LIST, AL_NOTLOCKED,
                             parent_ftx) ) ) break;

            else
            {
                index = (index+cpu+1) % ADVFS_RESOURCE_TABLE_SIZE;
                advfs_percpu_access_index[cpu] = index;
                i--;
            }
        }
    }

    if (bfap) {
        ADVFS_ACCESS_LOGIT( bfap, "Recycled from free list");
        ACCESS_STATS( get_new_access_free_list_recycle );
        advfs_init_access(bfap);
        return bfap;
    }

    /* 
     * We failed to acquire a bfAccess structure from the arena without
     * having to wait or to acquire one from the free lists. We will try
     * to grab an access structure from the closed lists.
     */

    /* Get cpu number of tries */
    i = ADVFS_RESOURCE_TRIES;
    j = ADVFS_RESOURCE_TABLE_SIZE;

    /* Loop for n tries doing trylocks first, see if we can get one. */
    while( i && j ) {
      index = advfs_percpu_access_index[cpu];
      if( advfs_closed_acc_list[index].len ) {
        if(     spin_trylock( &advfs_access_closed_lock[index] )
            && ( bfap = advfs_process_access_list(
                             &advfs_closed_acc_list[index], 
                             &advfs_access_closed_lock[index],
                             APAL_FLUSH_INVAL,
                             ON_CLOSED_LIST,
                             AL_LOCKED,
                             parent_ftx) ) ) {
#ifdef ADVFS_ACCESS_STATS
            if( i ==  ADVFS_RESOURCE_TRIES )
                ACCESS_STATS( get_new_access_closed_direct_hit );
            else
                ACCESS_STATS( get_new_access_closed_fast_recycle );
#endif                           
            break;
        } else {
            i--;
        }
        index = (index+cpu+1) % ADVFS_RESOURCE_TABLE_SIZE;
        advfs_percpu_access_index[cpu] = index;
      } else {
        j--;
      }
    }

    /* Loop through whole table doing full locks. see if we can get one. */
    if( !bfap )
    {
        i = ADVFS_RESOURCE_TABLE_SIZE;

        while(i) {
            index = advfs_percpu_access_index[cpu];
            if( ( bfap = advfs_process_access_list(
                           &advfs_closed_acc_list[index], 
                           &advfs_access_closed_lock[index],
                           APAL_FLUSH_INVAL,
                           ON_CLOSED_LIST,
                           AL_NOTLOCKED,
                           parent_ftx) ) ) break;

            else
            {
                index = (index+cpu+1) % ADVFS_RESOURCE_TABLE_SIZE;
                advfs_percpu_access_index[cpu] = index;
                i--;
            }
        }
    }

    if (bfap) {
        ADVFS_ACCESS_LOGIT( bfap, "Recycled from closed list");
        ACCESS_STATS( get_new_access_closed_list_recycle );
        advfs_init_access(bfap);
        return bfap;
    }

    /* So far, nothing is working. We may already know that this is
     * super-user if the credit had to be forced. But we may not know
     * if a credit was available, so go check again if we're not sure
     * before doing a malloc with wait.
     */

   if( !( credit & ADVFS_CREDIT_REWAIT ) ) {
       saved_uerror = u.u_error;
       if ( suser() ) credit |= ADVFS_CREDIT_REWAIT;
       u.u_error = saved_uerror;
   }
   if( credit & ADVFS_CREDIT_REWAIT ) {
       bfap = (bfAccessT*)kmem_arena_alloc( advfs_bfaccess_arena, M_WAITOK );
       ACCESS_STATS( get_new_access_malloc_wait );
       ADVFS_ATOMIC_FETCH_INCR( &advfs_acc_ctrl.acc_ctrl_access_cnt,
                          &value64 );
         /*
         * We need to lock the bfap before calling advfs_init_access
         * and we need to return with it locked.
         */
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
#ifdef ADVFS_SMP_ASSERT
        /*
         * This is part of a Memory Leak check.  Initializing these fields
         * should not be an issue right here, but we are putting additional
         * asserts in advfs_init_access() to make sure that these fields are
         * initialized this way.  The real worry is when recycling a bfap,
         * not when getting one from the arena.
         */
        bfap->xtnts.validFlag = XVT_INVALID;
        bfap->xtnts.xtntMap = NULL;
        bfap->xtnts.copyXtntMap = NULL;
        bfap->dirTruncp = NULL;
#endif /* ADVFS_SMP_ASSERT */
        *status = EOK;
    
        /* We got the new access structure, initialize it */
        advfs_init_access(bfap);
        (void)lk_set_state( &bfap->stateLk, ACC_RECYCLE );
        ADVFS_ACCESS_LOGIT( bfap,
              "advfs_get_new_access: got bfap for special user");
        return bfap;
    } 

    /*
     * Unfortunately, this means that we had nothing on the free or closed
     * lists to recycle, and we have too many bfaps open to malloc new ones.
     * Since we aren't CFS or super user, we can't let ourselves exceed our
     * limits, so we need to just return an error for status indicating
     * we've overflowed our limits, and return NULL for the bfap.
     */

    advfs_release_access_credit();
    *status = EHANDLE_OVF;
    return NULL;
}


/*
 * BEGIN_IMS
 *
 * advfs_init_access - Takes an allocated bfAccess structure and initializes
 * fields with basic information.  It also initializes the vnode and bfNode.
 *
 * Parameters:
 *      bfap - pointer to bfAccess to be initialized.
 *
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      On entry, we have exclusive access to this bfap and bfaLock is held. 
 *      On entry, all locks in bfap are initialized and unlocked (except
 *      bfaLock).
 *      Initialization should have been done by the arena.  None should be
 *      locked.   
 *      State lock will not be initialized to any particular value. 
 *
 * Algorithm:
 *      Go through bfAccess field by field initializing as appropriate.
 *
 * END_IMS
 */
static void
advfs_init_access(bfAccessT *bfap)
{
    int wkday;

    MS_SMP_ASSERT( ADVSMP_BFAP_OWNED( &bfap->bfaLock ) == TRUE );

    /* Assert all locks are not held */
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->xtntMap_lk, RWL_UNLOCKED) );
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->flush_lock, RWL_UNLOCKED) );
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->bfaSnapLock, RWL_UNLOCKED) );
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->bfa_acl_lock, RWL_UNLOCKED ) );
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->mcellList_lk.lock.rw,
					   RWL_UNLOCKED ) );
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->xtnts.migStgLk,
					   RWL_UNLOCKED ) );
    MS_SMP_ASSERT( ADVFS_SMP_RW_LOCK_EQL( &bfap->cacheModeLock, RWL_UNLOCKED ));

    /* 
     * Assert that this just came out of the arena! 
     * If this function is called from anywhere other than
     * advfs_get_new_access, rethink this assert 
     */
    MS_SMP_ASSERT( ((lk_get_state( bfap->stateLk) == ACC_DEALLOC) && 
                    (bfap->accMagic == (ACCMAGIC | MAGIC_DEALLOC) ) ) ||
                   ((lk_get_state( bfap->stateLk) == ACC_RECYCLE) && 
                    (bfap->accMagic == (ACCMAGIC) ) ) );
    bfap->accMagic = ACCMAGIC;


    bfap->bfaFlags |= BFA_NO_FLAGS;
    /* Make sure this isn't on a free list */
    /* This assert is based on the advfs_access_constructor setting freeFwd
     * = freeBwd = NULL.  It allows an assert in advfs_access_destructor to
     * verify freeListState == NOT_ON_LIST. */
    MS_SMP_ASSERT( (bfap->freeFwd == NULL) && (bfap->freeBwd == NULL) );
    MS_SMP_ASSERT( (bfap->setFwd == NULL) && (bfap->setBwd == NULL) );
    

    /* 
     * Setup vnode type and basic info.
     * At this point in time, we have exclusive access to this vnode and
     * bfap since it's not on any lists, so don't bother with the vnode
     * lock.
     */
    {
        struct vnode* vp = &bfap->bfVnode;

        vp->v_flag = VSOFTHOLD | VN_MPSAFE;
        vp->v_shlockc = 0;
        vp->v_exlockc = 0;
        vp->v_tcount = 0;
        vp->v_op = &advfs_vnodeops;
        vp->v_socket = NULL;
        vp->v_stream = NULL;
        /*
         * This field need to be set in advfs_lookup_valid_bfap
         */
        vp->v_vfsp = NULL;
        vp->v_writecount = 0;
        vp->v_nodeid = 0;
        /* 
         * If this isnt' a VREG file, bf_get_l will correct it.
         */
        vp->v_type = VREG;
        vp->v_fstype = VADVFS;
        vp->v_count = 0;
        vp->v_scount = 0;
        vp->v_vmdata = NULL;
        vp->v_vfsmountedhere = NULL;
        vp->v_shrlocks = NULL;
        /* The vnode needs to point to the bfnp */
        vp->v_data = (char *) &bfap->bfBnp;
    }

    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

    /* 
     * Allow UFC to setup its VM data structures for this vnode.
     * Do not set the file block size yet. The file block size attribute
     * will be set later in bs_map_bf() where the file's allocation unit
     * size becomes known.
     */
    fcache_vn_create( &bfap->bfVnode, 
                      FVC_DEFAULT,
                      (size_t) 0 /* Do not set file block size at this time. */
                      );
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );


    bfap->refCnt = 1;
    bfap->real_bfap = NULL;
    bfap->trunc         = 0;
    bfap->bfState       = BSRA_INVALID;
    bfap->bfaFlags      = BFA_NO_FLAGS;
    bfap->bfaLastWrittenFob = 0;
    bfap->dioCnt = 0;
    bfap->tag = NilBfTag;
    bfap->bfa_acl_cache = NULL;
    bfap->bfa_acl_cache_len = 0;
    bfap->bfaWriteCnt = 0;

    /* Init snapshot fields */
    bfap->bfaParentSnap = NULL;
    bfap->bfaFirstChildSnap = NULL;
    bfap->bfaNextSiblingSnap = NULL;
    bfap->bfaRefsFromChildSnaps = 0;
    bfap->bfa_orig_file_size = ADVFS_ROOT_SNAPSHOT;

    /*
     * This is part of a memory leak test.  A bfap that is recycled should
     * have been cleaned up before it gets here, so these asserts should not
     * be necessary.   The setting of these fields should only be necessary
     * when a new bfap is allocated from the arena.  When asserts are
     * enabled, there are assert checks that make sure these fields are
     * initialized for arena allocations, so the only way we should hit
     * these asserts is if a recycling path does not clean these fields up
     * before they get here.
     */
    MS_SMP_ASSERT(bfap->xtnts.validFlag == XVT_INVALID);
    MS_SMP_ASSERT(bfap->xtnts.xtntMap == NULL);
    MS_SMP_ASSERT(bfap->xtnts.copyXtntMap == NULL);
    MS_SMP_ASSERT(bfap->dirTruncp == NULL);
    bfap->xtnts.validFlag = XVT_INVALID;
    bfap->xtnts.xtntMap = NULL;
    bfap->xtnts.copyXtntMap = NULL;
    bfap->dirTruncp = NULL;

    bfap->setFwd = NULL;
    bfap->setBwd = NULL;
    bfap->freeFwd = bfap->freeBwd = NULL;
    bfap->freeListState = NOT_ON_LIST;
    bfap->hashlinks.dh_links.dh_next = NULL;
    bfap->hashlinks.dh_links.dh_prev = NULL;
    bfap->hashlinks.dh_key = 0;
    bfap->actRangeList.arFwd     = (struct actRange *)(&bfap->actRangeList);
    bfap->actRangeList.arBwd     = (struct actRange *)(&bfap->actRangeList);
    bfap->actRangeList.arWaitFwd = (struct actRange *)(&bfap->actRangeList);
    bfap->actRangeList.arWaitBwd = (struct actRange *)(&bfap->actRangeList);
    bfap->actRangeList.arMaxLen = 0;
    bfap->actRangeList.arWaitCount = 0;
    bfap->actRangeList.arWaitMaxLen = 0;
    bfap->actRangeList.arCount = 0;
    bfap->dataSafety = BFD_NOT_INITIALIZED;
    for(wkday=0; wkday<SS_HOT_TRACKING_DAYS; wkday++) {
        bfap->ssHotCnt[wkday] = 0;
    }
    bfap->ssMigrating = SS_MSG_IDLE;

    bfap->idx_params=NULL;
    bfap->idxQuotaBlks=0;
    
    bfap->bfSetp = NULL;
    bfap->bfap_free_time = ADVFS_TIMEVAL_TO_HZ(get_system_time());


    /*
     * Make sure our fsContext doesn't look initialized.
     */
    bfap->bfBnp.fsContextp = &bfap->bfFsContext;
    bfap->bfFsContext.initialized = 0;
    bfap->bfFsContext.quotaInitialized = 0;
    bfap->bfFsContext.fsc_fsnp = NULL;
    bfap->bfFsContext.dirty_alloc = FALSE;
    bfap->bfFsContext.dirty_stats = FALSE;
    bfap->bfFsContext.kv = NULL;


    bfap->file_size = -1;
    bfap->rsvd_file_size = 0;

#ifdef ADVFS_SMP_ASSERT
    bzero(&(bfap->bfa_stats), sizeof(bfaStats_t));
#endif
    ADVFS_ACCESS_LOGIT( bfap, "advfs_init_access complete");

    return;
}

/*
 * BEGIN_IMS
 *
 * advfs_dealloc_access - This function deallocates an access structure and
 * returns it to the access structure arena.  The function checks for races
 * with a dnlc lookup and fails if necessary.
 *
 * Parameters:
 *      bfap - the locked bfap to be deallocated and returned to the arena
 *      allocator.
 *
 * 
 * Return Values:
 *      EOK - success.  
 *      E_TOO_MANY_ACCESSORS - We raced with dnlc_lookup and failed.
 *
 * Entry/Exit Conditions:
 *      On entry, the bfap is locked.
 *      On entry, the bfap should not be on the free or closed list.  May be
 *      on the hash list and may be on the bfSet bfaccess list.
 *      On exit, the bfap is unlocked, regardless of success or failure.
 *         Callers must not reference the bfap after this function returns.
 *
 * Algorithm:
 *      Purge dnlc cache
 *      Check refCnt and v_count and v_scount are still 0 after purge.
 *      Fail if someone has referenced anything.
 *      Invalidate and destroy vnode if not referenced.
 *      Free memory.
 *
 * END_IMS
 */
static statusT
advfs_dealloc_access(bfAccessT *bfap)
{


    lkStatesT prior_lk_state = LKW_NONE;        /* Used for asserts */
    int       err;
    uint64_t  value64;

    /* 
     * Check a few assumptions about entry condition
     */
    MS_SMP_ASSERT( bfap->accMagic == ACCMAGIC );
    MS_SMP_ASSERT( bfap->freeListState == NOT_ON_LIST );
    MS_SMP_ASSERT( ADVSMP_BFAP_OWNED( &bfap->bfaLock ) );

    /* 
     * We should only have things being trashed because they are invalid, or
     * being sent back from the free list.  If the state was ACC_INIT_TRANS
     * or ACC_CREATING, it should have been changed to ACC_INVALID prior to
     * this, and we shouldn't be in ACC_RECYCLE while trying to dealloc.
     * Conditionally store off prior_lk_state to check under assert later.
     */
    MS_SMP_ASSERT( ( lk_get_state( bfap->stateLk ) == ACC_INVALID) ||
                   ( lk_get_state( bfap->stateLk ) == ACC_VALID) );
    MS_SMP_ASSERT( ( prior_lk_state = lk_get_state( bfap->stateLk )) ||
                   ( TRUE ) );

    /* 
     * The dnlc_cache must be purged to guarentee no one tries to reference
     * the vnode while we are deallocating it.  We can't hold the bfaLock
     * across this call, so we will set our state to ACC_DEALLOC then drop
     * the lock.  Optimally, we wouldn't set ACC_DEALLOC until after we have
     * already passed the dnlc races conditions, but we could have someone
     * sneak in with an internal open.  Setting ACC_DEALLOC means someone
     * may find an ACC_DEALLOC then find the vnode in ACC_VALID later.  This
     * should be ok as long as ACC_DEALLOC causes a block then a restart
     * when it is found. (The restart would find the ACC_VALID bfap and
     * proceed nicely).
     */


    (void)lk_set_state( &bfap->stateLk, ACC_DEALLOC );
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    dnlc_purge_vp( &bfap->bfVnode );

    /* 
     * If we are racing a dnlc_lookup, a hold has already been placed on the
     * vnode via its v_count or v_scount.  
     * To make sure that internal referecers of the file don't cause this
     * race to be missed, everyone should use the advfs_ref_bfap interface
     * and not directly call VN_HOLD.  advfs_ref_bfap will synchronize with
     * deallocation, VN_HOLD will not and will cause a race here
     */
    if ( (bfap->bfVnode.v_count != 0) ||  
         (bfap->bfVnode.v_scount != 0) ) {
        /* 
         * We lost the race, set state to valid and return.           */ 
        if (prior_lk_state == ACC_VALID) {
	    uint64_t myindex;

	    myindex = ACCESS_LIST_HASH(bfap);

            ADVFS_ACCESS_LOGIT( bfap, "advfs_dealloc_failed race ACC_VALID");
            ACCESS_STATS( dealloc_access_ref_cnt_exists );

            ADVSMP_BFAP_LOCK( &bfap->bfaLock );
            /*
             * If another
             * thread is blocked in advfs_lookup_valid_bfap waiting on
             * ACC_DEALLOC, it will expect the bfap to be on a free/closed
             * list when it finds it.  As a result, we need to put the bfap
             * back on the closed list so it can be taken off by the
             * advfs_ref_bfap call. Before putting it back on the
             * free/closed list, first make it valid. */
            (void)lk_set_state( &bfap->stateLk, ACC_VALID ); 

            ADVFS_ADD_TO_CLOSED_LIST( bfap,
                                        &advfs_closed_acc_list[myindex],
                                        &advfs_access_closed_lock[myindex] );
            
            lk_signal( UNLK_BROADCAST, &bfap->stateLk );

            /* Unlock bfaLock for return. */
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
            return E_TOO_MANY_ACCESSORS;
        } else {
            ADVFS_ACCESS_LOGIT( bfap, "advfs_dealloc_failed race not ACC_VALID");
            ACCESS_STATS( dealloc_access_racing_lookup_on_delete );
            /* We are racing a lookup. The lookup found the vnode in
             * the dnlc and put a softhold on it. They are now blocked
             * on the ACC_DEALLOC state. We will continue on ignorant
             * the softhold (will we zero it here for completeness)
             * and eventually wakeup the lookup(or lookups!) which
             * will get an ENO_SUCH_TAG error.
             */

            /*
             * Don't care if it is in use if we have a domain panic.
             */
            MS_SMP_ASSERT(bfap->bfVnode.v_count == 0 || bfap->dmnP->dmn_panic);
            MS_SMP_ASSERT(bfap->bfState == BSRA_INVALID || bfap->dmnP->dmn_panic);
            bfap->bfVnode.v_scount = 0;
        }
    }

    /* 
     * Bfap is unlocked and in ACC_DEALLOC, ready for final processing.  Let
     * it be freed. 
     */
    /* 
     * Before we get to dealloc, all valid data must already have been
     * flushed.  We will just invalidate and destroy the vnode without doing
     * any flushes.  If the dataSafety state is BFD_NOT_INITIALIZED, it
     * means the bfap is not setup.  We couldn't have data to flush in this
     * case since the bfap was never opened.
     */
  
    if ( bfap->dataSafety != BFD_NOT_INITIALIZED ) {
        err=fcache_vn_invalidate( &bfap->bfVnode, 
                (off_t) 0, 
                (size_t)0, 
                NULL, 
                (BS_IS_META(bfap)?FVI_PPAGE:0)|FVI_INVAL);
        MS_SMP_ASSERT(err==0);
    }

            
    ADVRWL_BFAP_FLUSH_WRITE(bfap);

    err=fcache_vn_destroy( &bfap->bfVnode, FVD_DEFAULT );
            
    MS_SMP_ASSERT(err==0);

    /* 
     * Remove bfap from hashlinks if on one 
     */
    if ( bfap->hashlinks.dh_links.dh_next != NULL ) {
        BS_BFAH_LOCK(bfap->hashlinks.dh_key, NULL);
        /* 
         * We could be racing with someone else trying to remove the
         * bfap (not likely) but doesn't check the ACC_DEALLOC 
         * state.  Just in case, double check that we are still on
         * the hash chain before removing. 
         */
        if (bfap->hashlinks.dh_links.dh_next != NULL) {
            BS_BFAH_REMOVE(bfap, FALSE);
        } else {
            ACCESS_STATS( dealloc_access_shouldnt_happen );
            BS_BFAH_UNLOCK(bfap->hashlinks.dh_key);
        }
    }

    /* 
     * Remove from bfSet access lists 
     */
    if ( BFSET_VALID(bfap->bfSetp) ) {
        RM_ACC_SETLIST(bfap, TRUE);
        bfap->bfSetp = NULL;
    }

    /*
     * Free the ACL cache if it is not null
     */
    if( bfap->bfa_acl_cache != NULL ) {
	ms_free( bfap->bfa_acl_cache );
	bfap->bfa_acl_cache = NULL;
	bfap->bfa_acl_cache_len = 0;
    }

    /* We have synchronized with the flush routines at this
     * point. The flush has either skipped this access struct
     * since it failed the lock_try or blocked on the chain
     * lock and wont even see it.
     */
    
    ADVRWL_BFAP_FLUSH_UNLOCK(bfap);

    MS_SMP_ASSERT( bfap->dirTruncp == NULL );
    x_dealloc_extent_maps( &bfap->xtnts );
    bfap->accMagic |= MAGIC_DEALLOC;

    /* 
     * We need to broadcast to wakeup anyone that might be blocked doing a
     * lookup on the bfap.
     */
    ADVFS_ACCESS_LOGIT( bfap, "advfs_dealloc_access: deallocating");
    
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    lk_signal( UNLK_BROADCAST, &bfap->stateLk );
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

    if ( bfap->bfFsContext.quotaInitialized ) {
        advfs_fs_detach_quota( &bfap->bfFsContext);
        bfap->bfFsContext.quotaInitialized = 0;
    }

   
    kmem_arena_free( bfap, M_WAITOK );

    /*
     * Once we return the access structure to the arena, we must decrement
     * the advfs_acc_access_cnt field. 
     */
     ADVFS_ATOMIC_FETCH_DECR( &advfs_acc_ctrl.acc_ctrl_access_cnt , &value64 );

    return EOK;


} /* End of advfs_dealloc_access */

/*
 * BEGIN_IMS
 *
 * advfs_ref_bfap - This function will attempt to reference a bfAccess
 * structure.  It will check that the bfap is not currently being
 * deallocated, recycled, or invalidated. It is possible to override the
 * soft max of number of ref'ed bfaps on system if the consequences of 
 * failing the ref are grave. The soft max is a guideline and does not 
 * represent hard system limits.
 *
 * Parameters:
 *      bfap - the locked bfap to be referenced. 
 *      ref_flags - input flags
 * 
 * Return Values:
 *      EOK - success.  
 *      EINVALID_HANDLE - Access structure is being deallocated or recycled. 
 *      EHANDLE_OVF - We are at the maximum access structure open limit
 *                    and cannot ref this bfap.
 *
 * Entry/Exit Conditions:
 *      On entry, the bfap is locked if BF_OP_BFA_LOCK_HELD is set.
 *      On entry, bfap is not locked if BF_OP_BFA_LOCK_HELD is not set.
 *      On entry, override acc_ctrl_soft_max if BF_OP_OVERRIDE_SMAX is set.
 *      On entry, honor acc_ctrl_soft_max if BF_OP_OVERRIDE_SMAX is not set.
 *      On exit, if EOK, bfap has the refCnt up and is not on the free list.
 *      On exit, if !EOK, bfap has not been altered.  If bfa_lock_held is
 *      false, then the bfaLock is not held and the bfap may have already
 *      been freed.
 *      On exit, if EHANDLE_OVF, the bfap could not be referenced because of
 *      resource limits being reached.  
 *
 *
 * Algorithm:
 *      Lock bfaLock if necessary.
 *      Check bfap state to see if it is safe to reference the bfap (stateLk
 *      != ACC_INVALID, ACC_DEALLOC, or ACC_RECYCLE.
 *      if bfap can be reference, bump refCnt and remove from free list if
 *      on one.
 *      drop bfaLock if it was taken in this function.
 *
 * END_IMS
 */
static statusT
advfs_ref_bfap(bfAccessT *bfap,         /* Bfap to reference */
	       enum acc_open_flags ref_flags
              )
{
    statusT sts = EOK;
    int32_t owned_list_lock = FALSE;
    int32_t saved_uerror = u.u_error;

    if (!(ref_flags & BF_OP_BFA_LOCK_HELD)) {
        ADVSMP_BFAP_LOCK( &bfap->bfaLock );
    }
    MS_SMP_ASSERT( ADVSMP_BFAP_OWNED( &bfap->bfaLock ) );

    if ( (lk_get_state( bfap->stateLk ) == ACC_RECYCLE) ||
         (lk_get_state( bfap->stateLk ) == ACC_DEALLOC) ||
         (lk_get_state( bfap->stateLk ) == ACC_INVALID) ) {
        /* 
         * The access structure is about to go away or is invalid.  From any
         * of these states, the access structure memory could be returned to
         * the advfs_bfaccess_arena in an error condition.  A refCnt won't
         * stop the freeing so just return an error.
         */
	if (!(ref_flags & BF_OP_BFA_LOCK_HELD)) {
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
        }
        ACCESS_STATS( ref_bfap_failed_ref_bad_state );
        return ENO_SUCH_TAG;
    } else {
        /*
         * The access structure is in a valid state to be referenced.  
         */
        MS_SMP_ASSERT( (lk_get_state( bfap->stateLk) == ACC_VALID) ||
                       (lk_get_state( bfap->stateLk) == ACC_INIT_TRANS) ||
                       (lk_get_state( bfap->stateLk) == ACC_CREATING) ||
                       (lk_get_state( bfap->stateLk) == ACC_VALID_EXCLUSIVE) );
        /*
         * Assert that either this isn't the first ref, or the bfap is on
         * the free list.
         */
        MS_SMP_ASSERT( (bfap->refCnt != 0) || 
                       ( (bfap->refCnt == 0) && 
                         (bfap->freeListState != NOT_ON_LIST) ) );

        /* Attempt to get an access credit, an if we fail return error. */

        if (bfap->refCnt == 0) {
            if( !advfs_get_access_credit( ref_flags ) ){
                ACCESS_STATS( ref_bfap_too_many_open );
                sts = EHANDLE_OVF;
            }
        }
 
        if (sts == EOK) {
            /*
             * If we passed the above checks we can bump the refCnt and
             * remove from free/closed lists.  If we don't have EOK status,
             * something went wrong and we will not bump ref count but will
             * return sts later
             */
            bfap->refCnt++;

            if ( !(ref_flags & BF_OP_IGNORE_CLOSED_LIST) ) {
                /* 
                 * Remove from free/closed list if on one
                 */
                if (bfap->freeListState == ON_FREE_LIST) {
		    uint64_t myindex;

                    myindex = ACCESS_LIST_HASH(bfap);
                    /* 
                     * The bfap is on the free list. We may already hold the
                     * list lock so only conditionally take/release the lock.
                     * The advfs_access_mgmt_thread is the one who might be
                     * holding the lock.  It is inefficient for that thread to
                     * drop the lock.
                     */
                    if(!ADVSMP_FREE_LIST_OWNED( 
                            &advfs_access_free_lock[myindex]) ) {
                        ADVSMP_FREE_LIST_LOCK(
			    &advfs_access_free_lock[myindex] );
                    } else {
                        owned_list_lock = TRUE;
                    }
                    RM_ACC_LIST_NOLOCK( bfap,
					&advfs_free_acc_list[myindex],
					&advfs_access_free_lock[myindex] );
                    if (!owned_list_lock) {
                        ADVSMP_FREE_LIST_UNLOCK(
                              &advfs_access_free_lock[myindex] );
                    }
                } else if (bfap->freeListState == ON_CLOSED_LIST) {
		    uint64_t myindex;

                    myindex = ACCESS_LIST_HASH(bfap);
                    /*
                     * The bfap on the closed list. We may already hold the
                     * list lock so only conditionally take/release the lock.
                     * The advfs_access_mgmt_thread is the one who might be
                     * holding the lock.  It is inefficient for that thread to
                     * drop the lock.
                     */
                    if (!ADVSMP_CLOSED_LIST_OWNED(
			     &advfs_access_closed_lock[myindex] ) ) {
                        ADVSMP_CLOSED_LIST_LOCK(
			    &advfs_access_closed_lock[myindex] );
                    } else {
                        owned_list_lock = TRUE;
                    }
                    RM_ACC_LIST_NOLOCK( bfap,
					&advfs_closed_acc_list[myindex],
					&advfs_access_closed_lock[myindex] );
                    if (!owned_list_lock) {
                        ADVSMP_CLOSED_LIST_UNLOCK(
			    &advfs_access_closed_lock[myindex] );
                    }
                }
            } else {
                ACCESS_STATS( ref_bfap_skipped_list_removal ); 
            }

            /* If someone is sneaking in to ref a bfap that is
             * ACC_VALID_EXCLUSIVE, we need to change the state to
             * ACC_VALID.  There are few callers of advfs_ref_bfap directly,
             * so this isn't a very common case, but may happen from 
             * advfs_bs_bfs_flush.
             */
            if ( lk_get_state( bfap->stateLk ) == ACC_VALID_EXCLUSIVE ) {
                (void)lk_set_state( &bfap->stateLk, ACC_VALID );
            }

        }
       

        /*
         * bfap is refed and off any free lists.  
         */
	if (!(ref_flags & BF_OP_BFA_LOCK_HELD)) {
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
        }
        return sts;
    }
}

/*
 * BEGIN_IMS
 *
 * advfs_release_access - This function will put a valid access (ACC_VALID)
 * structure on the free or closed list and will call advfs_dealloc_access
 * on an invalid (ACC_INVALID) access structure.
 *
 * Parameters:
 *      bfap - the locked bfap to be released to the free/closed list or
 *      deallocated 
 *      action - Defines the action to take on the bfap now that the refCnt
 *      is 0.
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      On entry, the bfap is locked.
 *      On entry, bfap->refCnt == 0
 *      On entry, bfap->freeListState == NOT_ON_LIST
 *      On exit, the bfap is unlocked and the bfap may no longer exist.  This
 *      function may dealloc the bfap if it is ACC_INVALID, so on return,
 *      callers cannot access the pointer to bfap.
 *
 * Algorithm:
 *      Assert refCnt conditions.   
 *      if bfap state is ACC_INVALID, call advfs_dealloc_access.
 *      else put bfap on closed list, unlock and return.
 *
 *
 * END_IMS
 */

void
advfs_release_access(
                     bfAccessT* bfap,         /* in - pointer to access struct */
                     drc_action_type_t action /* in - action to take on bfap */
                    )
{
    statusT sts = EOK;

    MS_SMP_ASSERT(ADVSMP_BFAP_OWNED(&bfap->bfaLock));
    MS_SMP_ASSERT(bfap->refCnt == 0);

    if( bfap->dirTruncp != NULL ) {
        ms_free( bfap->dirTruncp );
        bfap->dirTruncp = NULL;
    }
    

    MS_SMP_ASSERT(bfap->freeListState == NOT_ON_LIST);

    /*
     * Always clear this flag.  If we came through rbf_create for a user
     * create, this will be set and needs clearing before we try to open the
     * file again
     */
    bfap->bfaFlags &= ~BFA_EXT_OPEN;

    /*
     * We must only put ACC_VALID bfaps on the closed or free lists. If the
     * bfap is ACC_INVALID, advfs_dealloc_access it to free the structure.
     * If it is in another state, there is a problem with the access state
     * machine.
     */
    if ( (lk_get_state( bfap->stateLk ) == ACC_INVALID) || 
            (action & DRC_DEALLOC) ) 
    {
        /*
         * If we are going to deallocate, we expect the bfap not to be on the
         * free or closed list.  
         */
        sts = advfs_dealloc_access( bfap );

        if (sts != EOK) {
            ACCESS_STATS( advfs_release_dealloc_failed );
        }
        /* Return our access credit. */
        advfs_release_access_credit();
        return;
    }

    /*
     * ACC_CREATING could occur if the file was created, accessed and
     * closed within a transaction.  The state will not change until the
     * root done is processed.
     */
    MS_SMP_ASSERT( (lk_get_state( bfap->stateLk ) == ACC_VALID) ||
                   (lk_get_state( bfap->stateLk ) == ACC_CREATING) ||
                   (lk_get_state( bfap->stateLk ) == ACC_INIT_TRANS));

    /* 
     * Add bfap to closed list or free list.  Later, use heuristics to put on free list
     * if we KNOW it has no dirty data.  If the bfap is already on one of
     * the lists, we don't need to do anything. 
     */
    if (bfap->freeListState == NOT_ON_LIST) {
	uint64_t myindex;

	myindex = ACCESS_LIST_HASH(bfap);
        if (action & DRC_CLOSED_LIST) {
       
        ADVFS_ADD_TO_CLOSED_LIST( bfap,
                                        &advfs_closed_acc_list[myindex],
                                        &advfs_access_closed_lock[myindex] );
        } else if (action & DRC_FREE_LIST) {
            ADVFS_ADD_TO_FREE_LIST( bfap,
                                        &advfs_free_acc_list[myindex],
                                        &advfs_access_free_lock[myindex] );
        } else {
            printf("Invalid action for advfs_release_access\n");
            MS_SMP_ASSERT( FALSE );
        }
    }

    /*
     * We have no opens of the file at this point, but the BFA_EXT_OPEN flag
     * needed to be cleared in advfs_inactive to prevent a race with
     * dnlc_lookup.  So we will just clear the BFA_INT_OPEN flag.
     */
#ifdef ADVFS_SMP_ASSERT
    bfap->bfaFlags &= ~BFA_INT_OPEN;
#endif

    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );

    /* Return our access credit. */
    advfs_release_access_credit();
 
}


/*
 * BEGIN_IMS
 *
 * advfs_access_constructor - This function is the constructor function for
 * the advfs_bfaccess_arena.  It initializes all the locks in the access
 * structure and sets the ACCMAGIC | MAGIC_DEALLOC as the accMagic field of
 * the access structure.
 *
 * Parameters:
 *      addr - address of bfAccess to initialize.
 *      size - size of bfAccess structure.
 *      flags - M_WAITOK or M_NOWAIT.
 *
 * Return Values:
 *
 * Entry/Exit Conditions:
 *
 *
 * Algorithm:
 *      Initialize all locks and set accMagic.
 *
 * END_IMS
 */
int
advfs_access_constructor(void *addr, size_t size, int flags) 
{
    bfAccessT *bfap = (bfAccessT*)addr;
    /* 
     * Initialize all the locks for the access structure 
     */
    ADVSMP_BFAP_RANGE_INIT( &bfap->actRangeLock );

    ADVSMP_BFAP_INIT( & bfap->bfaLock );

    /* 
     * Init the stateLk.
     */
    if (flags & M_NOWAIT) {
        if (!ADVSMP_BFAP_TRYLOCK( &bfap->bfaLock )) {
            /* We can't wait. We should never have this situation really. */
            return 1;
        }
    } else {
        ADVSMP_BFAP_LOCK(&bfap->bfaLock);
    }
    lk_init( &bfap->stateLk, &bfap->bfaLock, LKT_STATE, 0, LKU_BF_STATE );
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
    bfap->stateLk.state = ACC_DEALLOC;

    ADVRWL_XTNTMAP_INIT( &bfap->xtntMap_lk );
    ADVRWL_VHAND_XTNTMAP_INIT(&bfap->vhand_xtntMap_lk );
    ADVRWL_BFAP_FLUSH_INIT( &bfap->flush_lock );
    ADVFTX_MCELLFTX_INIT( &bfap->mcellList_lk, &bfap->bfaLock );
    ADVRWL_MIGSTG_INIT( bfap );
    ADVRWL_BFAPCACHEMODE_INIT( &bfap->cacheModeLock );
    ADVRWL_BFAP_SNAP_LOCK_INIT( &bfap->bfaSnapLock );
    cv_init( &bfap->bfaSnapCv,
             "advfs_bfap_snap-cv",
             NULL, CV_WAITOK);
    ADVRWL_BFAP_BFA_ACL_LOCK_INIT( &bfap->bfa_acl_lock );

    bfap->bfVnode.v_vmdata = NULL;
    ADVFS_IFOSDEBUG(bfap->bfVnode.v_initialized = 0);
    bfap->bfVnode.v_pfdathd = NULL;
    VN_INIT( &bfap->bfVnode, NULL, VNON, NODEV );


    fscontext_lock_init( &bfap->bfFsContext );

    ADVSMP_BFAP_BFA_SS_MIG_LOCK_INIT( &bfap->bfa_ss_migrate_lock );

#ifdef ADVFS_SMP_ASSERT
    /*
     * This init lets us do checks in advfs_access_destructor that the bfap
     * isn't on any lists... Normally we wouldn't initialize mutable fields
     * in an arena constructor.
     */
    bfap->freeListState = NOT_ON_LIST;
    bfap->freeFwd = bfap->freeBwd = NULL;
    bfap->setFwd = NULL;
    bfap->setBwd = NULL;
#endif

#ifdef ADVFS_ACCESS_TRACE
    ADVSMP_BFAP_TRACE_INIT( &bfap->bfa_trace_lock );
    /* 
     * We zero in the constructor so we can trace the bfap across
     * deallocs...
     */
    bfap->bfa_trace_index = -1;
    {
        int i = 0;
        for (i = 0; i < ADVFS_PER_ACCESS_TRACE; i++) {
            bzero( &bfap->bfa_trace[i], sizeof( bfap_trace_t ) );
        }
    }
#endif


    bfap->accMagic = ACCMAGIC | MAGIC_DEALLOC;

    return 0;

}

/*
 * BEGIN_IMS
 *
 * advfs_access_destructor - This function is the destructor for the
 * advfs_bfaccess_arena.  It destroys all locks and asserts that accMagic ==
 * ACCMAGIC | MAGIC_DEALLOC.
 *
 * Parameters:
 *      addr - address of bfAccess to initialize.
 *      size - size of bfAccess structure.
 *      flags - M_WAITOK or M_NOWAIT.
 *
 * Return Values:
 *
 * Entry/Exit Conditions:
 *
 *
 * Algorithm:
 *      Destroy all locks, and assert accMagic == ACCMAGIC | MAGIC_DEALLOC
 *
 * END_IMS
 */
void
advfs_access_destructor(void *addr, size_t size, int flags) 
{

    bfAccessT *bfap = (bfAccessT*)addr;

    MS_SMP_ASSERT( bfap->accMagic == (ACCMAGIC | MAGIC_DEALLOC));

    MS_SMP_ASSERT( bfap->freeListState == NOT_ON_LIST );
    MS_SMP_ASSERT( bfap->freeFwd == NULL );
    MS_SMP_ASSERT( bfap->freeBwd == NULL );
    
    /* 
     * Destroy all the locks for the access structure 
     */
    spin_destroy( &bfap->actRangeLock );
    ADVSMP_BFAP_DESTROY( &bfap->bfaLock );


    cv_destroy( &bfap->bfaSnapCv );

   /* 
     * Destroy vnode lock
     */
    b_termsema( &bfap->bfVnode.v_lock );

    fscontext_lock_init( &bfap->bfFsContext );

    /* This doesn't do anything! */
    lk_destroy( &bfap->stateLk );

    bfap->stateLk.state = LKW_NONE;


    ADVRWL_XTNTMAP_DESTROY( &bfap->xtntMap_lk );
    ADVRWL_VHAND_XTNTMAP_DESTROY( &bfap->vhand_xtntMap_lk );
    ADVRWL_BFAP_FLUSH_DESTROY( bfap );
    
    ADVRWL_BFAP_SNAP_LOCK_DESTROY( bfap );
    ADVRWL_BFAP_BFA_ACL_LOCK_DESTROY( &bfap->bfa_acl_lock );

    /* There is no destroy for ftx_locks. Just destroy underlying lock.
    ftx_rwlock_init( &bfap->mcellList_lk,
                   &bfap->bfaLock,
                   ADVbfAccessT_mcellList_lk_info);
                   */
    ADVRWL_MCELL_LIST_DESTROY( &bfap->mcellList_lk.lock.rw );
    ADVRWL_MIGSTG_DESTROY( bfap );
    ADVRWL_BFAPCACHEMODE_DESTROY( bfap );
    ADVSMP_BFAP_BFA_SS_MIG_LOCK_DESTROY( &bfap->bfa_ss_migrate_lock );
#ifdef ADVFS_ACCESS_TRACE
    ADVSMP_BFAP_TRACE_DESTROY( &bfap->bfa_trace_lock );
#endif
}

/*
 * BEGIN_IMS
 *
 * advfs_lookup_valid_bfap - This function takes a bfSet* and a bfTag and
 * returns a valid bfap for that file.  The function will either return an
 * access structure from the hashtables, or return a newly initialized
 * bfAccess structure with an initialized vnode.
 *
 * Parameters:
 *      bfSetp* - The bfSet in which the bfTag file exists. Unless
 *      BF_OP_INMEM_ONLY is set in options, this must have
 *      bfSetp->fsnp->vfsp initialized, or this vnode has no vfs structure.
 *      tag - The tag of the file in bfSetp to be opened.
 *      forceFlag - May no longer be necessary.  Used to force allocations
 *      beyond normal limits, but we no longer have access structure limits.
 *      acc_options - options for the function.
 *              BF_OP_INMEM_ONLY - Only return access structures out of the
 *                      cache, do not acquire new ones.
 *      parent_ftx - Parent transaction handle.
 *
 * Return Values:
 *      Returns a valid bfAccessT* or NULL on error (couldn't allocate a
 *      access structure).
 *
 * Entry/Exit Conditions:
 *      On entry, no locks are held.  
 *      On exit, bfap exists, is NOT ACC_DEALLOC or ACC_RECYCLE and has a
 *              non-zero refCnt.
 *      On exit, fcache_vn_create will have been called and the vnode will
 *              have been initialized.
 *      On exit, bfap lock is held if the function succeeded.
 *
 *
 * Algorithm:
 *      The basic algorithm is to lookup the tag in the hash table.  If
 *      found, return.  Otherwise, try to acquire and initialize a new
 *      access structure and its vnode.  If the vnode is recycled or newly
 *      initialized, it is returned in state ACC_INIT_TRANS.
 *
 * END_IMS
 */
struct vfs advfs_meta_vfs;

bfAccessT*
advfs_lookup_valid_bfap(
           bfSetT *bfSetp,  /* in - bitfile-set handle */
           bfTagT tag,      /* in - bitfile tag */
           enum acc_open_flags options, /* in - options flags */
           ftxHT parent_ftx)

{

    bfAccessT *bfap, *tbfap;
    uint64_t insert_count_before,insert_count_after;
    uint64_t hash_key;
    int sts;

lookup:
    /* 
     * Check if access struct with this tag is already in hash table.
     * If so, return it with its bfaLock seized.
     */
    bfap = find_bfap( bfSetp, tag, FALSE, &insert_count_before );

    if( bfap != NULL ) {
        /*  
         * Found it in the hash table. 
         *
         * If we find an access structure in a bad state, we will release
         * the access structure's bfaLock, then sleep and let some other 
         * thread finish processing the state.  We will then be woken up
         * and we will retry.
         */
        switch (lk_get_state( bfap->stateLk )) {
            case ACC_INVALID:
                /*
                 * We can find an access structure in this state still in 
                 * the hash table if we are racing a last close.  The
                 * last close is far enough along at this point that we
                 * will concede the race and let him finish.  He should
                 * still have the refCnt up.  We do not expect to find an
                 * access structure in the hash table with an invalid state
                 * and no thread referencing it.
                 */
                MS_SMP_ASSERT(bfap->refCnt > 0);
            case ACC_INIT_TRANS:
            case ACC_RECYCLE:
            case ACC_DEALLOC:
                /*
                 * Wait on the state.  The lk_wait is safe since it
                 * will not reacquire the bfap->bfaLock when the cv wakes up.
                 */
                ACCESS_STATS( lookup_valid_bfap_bad_state );
                lk_wait( &bfap->stateLk, &bfap->bfaLock );
                goto lookup;
            default:
                if ((!BS_BFTAG_EQL(bfap->tag, tag)) ||
                    (!BS_BFS_EQL(bfSetp->bfSetId,
                                 ((bfSetT *)(bfap->bfSetp))->bfSetId))) {
                    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);
                    ACCESS_STATS( lookup_valid_bfap_tag_mismatch );
                    goto lookup;
                }
                break;
        }
            
        /*
         * An internal open always gets a ref.  External opens get 1 ref
         * for all external opens (represents "VFS" as a single opener).
         * We should never fail to ref the bfap.  The single refCnt for
         * all external opens is decremented when the v_count on the
         * vnode goes to 1 (and will go to 0).  When VN_RELE is called
         * with a v_count of 1, advfs_inactive is called and decrements
         * the last v_count and the last refCnt (by calling bs_close).
         * Assert that we don't
         * fail.  We should always be ACC_VALID or ACC_CREATING, so there 
         * is no reason to fail.  
         */
        if (options & BF_OP_INTERNAL) {
            sts = advfs_ref_bfap( bfap, options | BF_OP_BFA_LOCK_HELD );
            if (sts != EOK) {
                ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                ACCESS_STATS( lookup_valid_bfap_internal_ref_failed );
                return ((bfAccessT*)NULL);
            }
#ifdef ADVFS_SMP_ASSERT
            bfap->bfaFlags |= BFA_INT_OPEN;
#endif
            /* 
             * If this is a ref from a child snapshot, bump
             * bfaRefsFromChildSnaps
             */
            if (options & BF_OP_SNAP_REF) {
                bfap->bfaRefsFromChildSnaps++;
            }
        } else {
            /* 
             * External open, only ref if we haven't already done an
             * external open on this bfap.
             */
            if ( !(bfap->bfaFlags & BFA_EXT_OPEN) ) {
                sts = advfs_ref_bfap( bfap, options | BF_OP_BFA_LOCK_HELD );
                /*
                 * In case this was previously opened internally, reset
                 * vfsp in the vnode
                 */
                bfap->bfVnode.v_vfsp = bfSetp->fsnp->vfsp;
                if( sts != EOK ) {
                    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                    ACCESS_STATS( lookup_valid_bfap_external_ref_failed );
                    return ((bfAccessT*)NULL);
                }
                /* 
                 * If this is a ref from a child snapshot, bump
                 * bfaRefsFromChildSnaps
                 */
                if (options & BF_OP_SNAP_REF) {
                    bfap->bfaRefsFromChildSnaps++;
                }
            }
            bfap->bfaFlags |= BFA_EXT_OPEN;

        }
        MS_SMP_ASSERT( bfap->refCnt > 0 );

        /* Return to the caller holding the bfaLock.  
         * The only state the access structure should be in is ACC_VALID
         * or possible ACC_CREATING.
         */
        MS_SMP_ASSERT( (lk_get_state( bfap->stateLk ) == ACC_VALID) ||
                       (lk_get_state( bfap->stateLk ) == ACC_CREATING) );
        return bfap;
    } else {

        /*  Not in the hash table, BF_OP_INMEM_ONLY set, so return NULL */
        if (options & BF_OP_INMEM_ONLY) {
            ACCESS_STATS( lookup_valid_bfap_inmem_not_found );
            return NULL;
        }

        /*  Not in the hash table, so get a new bfAccess struct (locked) */
        bfap = advfs_get_new_access(parent_ftx, &sts);
        if (sts == EHANDLE_OVF) {
            /*
             * Could not allocate an access structure. 
             * Pay attention if failed_get_access is ever non-zero.  It
             * means we are testing VM at high memory conditions when the
             * page reserve is low and we aren't able to get memory without
             * waiting AND all our access structures are open or highly contended.
             */
            ACCESS_STATS( lookup_valid_bfap_failed_get_access );
            return NULL;
        }
    }
    
    ADVFS_ACCESS_LOGIT( bfap, "advfs_lookup_valid_bfap got new bfap");

    /* 
     * We should have a bfap from advfs_get_new_access and it should be in
     * ACC_RECYCLE from advfs_get_new_access setting the state.
     */
    MS_SMP_ASSERT(bfap != NULL);
    MS_SMP_ASSERT( lk_get_state( bfap->stateLk ) ==  ACC_RECYCLE );

    /*
     * We can't hold the bfaLock if we race with another thread that beats
     * us to adding the access structure.  We need to release this prior to
     * the find_bfap call below.
     */
    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );


    /*
     * Do initialization that couldn't be done by advfs_init_access
     */
    if ( !(options & BF_OP_INTERNAL) ) {
        /* 
         * If this is an external open, we should have a mount point
         */
        MS_SMP_ASSERT(bfSetp->fsnp != NULL); 
        bfap->bfVnode.v_vfsp = bfSetp->fsnp->vfsp; 
    } else {
        /* We are a reserved file and have no vfs. */
        bfap->bfVnode.v_vfsp = &advfs_meta_vfs;
    }
    bfap->bfVnode.v_nodeid = tag.tag_num;

    
    /* 
     * Initialize the bfNode
     */
    {
        struct bfNode *bfnp = &bfap->bfBnp;

        bfnp->fsContextp = &bfap->bfFsContext;
        bfnp->tag = tag;
        bfnp->bfSetId= bfSetp->bfSetId;
        /* 
         * Tru64 didn't init this value this early.  We must have this set
         * for putpage to work (VTOA).  This is no longer used as a flag
         * to indicate the first external open.
         */
        bfnp->accessp = bfap;
    }

    
    MS_SMP_ASSERT(TEST_DMNP(bfSetp->dmnP) == EOK);

    /*
     * Get our new hash key and make sure its not already in the hashtable.
     */
    hash_key = BS_BFAH_GET_KEY(bfSetp,tag);
    tbfap = BS_BFAH_LOCK(hash_key,&insert_count_after);

    /* 
     * We didn't find any access structure on the chain from the last
     * find bfap call but we noted the insert count at that time. If nothing
     * has been inserted on that chain since then, we can assume find_bfap will
     * fail again.
     * If, on the other hand, an insertion has taken place to this chain then
     * we better make sure we aren't racing another advfs_lookup_valid_bfap with the same
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
            ACCESS_STATS( lookup_valid_bfap_racer_created_bfap );
            BS_BFAH_UNLOCK(hash_key);
            /*
             * bfap->bfaLock has been dropped above. We need to get it before
             * moving the bfap to the free list.  Additionally, we need to
             * drop the bfaLock of tbfap.  If we hold tbfap, we will hit a
             * lock hierarchy problem in advfs_dealloc_access trying to call
             * fcache_vn_destroy.  We must return with tbfap locked, so we
             * will just drop the tbfap lock and goto lookup and try again.
             * We should find tbfap again, otherwise, we'll just get a new
             * one again.
             */
            ADVSMP_BFAP_UNLOCK( &tbfap->bfaLock );
            ADVSMP_BFAP_LOCK(&bfap->bfaLock);

            (void)lk_set_state( &bfap->stateLk, ACC_INVALID );

            /* We shouldn't fail for any reason. */
            DEC_REFCNT( bfap, DRC_DEALLOC );
            /* 
             * advfs_dealloc_access drops the lock before freeing the
             * strucutre... obviously 
             */
            goto lookup;
        }
    }

    MS_SMP_ASSERT( bfap->refCnt == 1 );
    
    /* Do more specific init that could be done by advfs_init_access */
    bfap->tag           = tag;
    bfap->bfSetp        = bfSetp;
    bfap->dmnP          = bfSetp->dmnP;
#ifdef ADVFS_SMP_ASSERT
    if ( (options & BF_OP_INTERNAL) ) {
        bfap->bfaFlags |= BFA_INT_OPEN;
    } else {
        bfap->bfaFlags |= BFA_EXT_OPEN;
    }
#else
    if ( !(options & BF_OP_INTERNAL) ) {
        bfap->bfaFlags |= BFA_EXT_OPEN;
    }
#endif
    /* 
     * If this is a ref from a child snapshot, bump
     * bfaRefsFromChildSnaps
     */
    if (options & BF_OP_SNAP_REF) {
        bfap->bfaRefsFromChildSnaps = 1;
    }


    /* Set the state to ACC_INIT_TRANS before placing
     * on the set list so if a racing flush finds it
     * putpage will know not to flush it
     */


    (void)lk_set_state( &bfap->stateLk, ACC_INIT_TRANS );
    /*
     * Add the access structure to the fileset's list.
     * of access structures. This returns with the
     * bfalock held, but expects the lock is not held when called.
     */
    ADD_ACC_SETLIST(bfap, TRUE, TRUE);
    MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);

    /* Link entry into fileset-tag hash list. We are holding the
     * hash chains lock already and dyn_hash_insert will release it
     */

    bfap->hashlinks.dh_key = hash_key;
    /* 
     * We will drop the hash chain lock when we insert 
     */
    BS_BFAH_INSERT(bfap,FALSE);

    /* Return bfAccess pointer to caller, holding bfaLock. */
    return bfap;
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

static bfAccessT *
find_bfap(
          bfSetT *bfSetp,         /* in - bitfile-set descriptor poitner */
          bfTagT tag,             /* in - bitfile tag */
          int    hold_hashlock,   /* in - TRUE = return with HashLock held */
                                  /*      FALSE = release HashLock on return */
          uint64_t  *insert_count)   /* out - hint indicating an insertion
                                           since last time chain was locked */
{
    bfAccessT *findBfap, *last_bfap;
    uint64_t hash_key;
    struct kthread *th = u.u_kthreadp;

    /*
     * Grab the hash table chain's lock to protect the hash lookup
     */

    /* It is ok for find_bfap to search the hashtable, since it
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

/* Revisit - Need to sort out what final behavior
 * should be for performance
 */

/* #ifdef notdef */
        last_bfap = findBfap = last_bfap->hashlinks.dh_links.dh_prev;
/* #endif */
        findBfap = last_bfap;
    }
    do {
        if (findBfap == NULL) break;

        /* We now attempt to find the bsaccess structure in the
         * hash chain. If the tag.tag_seq is 0 then this is a reserved
         * tag (ie. logfile, BMT, SBM, roottag dir) and the sequence
         * number is not incremented so we must therfore check the
         * bfsetid to insure that it is not an old reserved tag that was
         * left lying around.
         */

        /* 
         * We will no longer get the bfa lock each time. We will look
         * at the Setp and tag first and then the lock state.
         *
         * NOTE: If we encounter a BsAccess structure with the same
         * tag and setp but in ACC_RECYCLE we know that it will soon
         * have a different tag and/or setp but currently we need to wait
         * for it because any i/o associated with it must be flushed
         * before we can access the file.
         *
         */

        if ( (BS_BFTAG_EQL(findBfap->tag, tag)) &&
             findBfap->bfSetp == bfSetp )
        {
            if ( BS_BFTAG_REG(tag) )
              /*
               * regular files will have only one tag match per hash bucket.
               */
            {
                ADVSMP_BFAP_LOCK(&findBfap->bfaLock);
                MS_SMP_ASSERT( (BS_BFTAG_EQL(findBfap->tag, tag)) &&
                               findBfap->bfSetp == bfSetp );
                if ( (lk_get_state(findBfap->stateLk) == ACC_RECYCLE) ||
                      lk_get_state(findBfap->stateLk) == ACC_DEALLOC) {

                    BS_BFAH_UNLOCK(hash_key);
                    /* 
                     * It is safe to do a lk_wait on these states
                     * since we will not try to reacquire any locks when the
                     * cv_wait wakes up.  It sould be noted that we may wake
                     * up before the state has actually changed.
                     */
                    lk_wait( &findBfap->stateLk, &findBfap->bfaLock );
                    ACCESS_STATS( find_bfap_restart );
                    goto restart;
                }
                /* We found the one we want;  return it locked */
                goto return_it;
            }
            else
            {
                /* This is a reserved tag. Need further verification
                 * This situation can arise when many domains are opened
                 * causing the metadata file tags to wrap around. Since the
                 * tag.tag_seq is not incremented we can't tell so we must check
                 * the bfSetId itself.
                 */

                /* There's a chance the fileset could have gone away.
                 * We wont find it anyway so break out
                 */
                if ( ! BFSET_VALID(bfSetp) ) break;
                if (BS_BFS_EQL( bfSetp->bfSetId,
                                 ((bfSetT *)findBfap->bfSetp)->bfSetId )) {
                    ADVSMP_BFAP_LOCK(&findBfap->bfaLock);
                    MS_SMP_ASSERT( (BS_BFTAG_EQL(findBfap->tag, tag)) &&
                                   findBfap->bfSetp == bfSetp );
                    if ( (lk_get_state(findBfap->stateLk) == ACC_RECYCLE) ||
                         (lk_get_state(findBfap->stateLk) == ACC_DEALLOC) ) {
                        /* 
                         * We will do a lk_wait until the state changes to
                         * something else (Anything really) or until a
                         * broadcast occurs on the lk cv, then we will
                         * start over and see what happens.
                         */
                        BS_BFAH_UNLOCK(hash_key);
                        lk_wait( &findBfap->stateLk, &findBfap->bfaLock );
                        ACCESS_STATS( find_bfap_meta_restart );
                        goto restart;
                }
                    /* We found the one we want; return it locked */
                    goto return_it;
                }
            }
        }
/* #ifdef notdef */
        findBfap = findBfap->hashlinks.dh_links.dh_prev;
                      /* searching in reverse */
/* #endif */
/*        findBfap = findBfap->hashlinks.dh_links.dh_next; */
                     /* searching in forward */

    } while ( findBfap != last_bfap );
    findBfap = NULL;

return_it:
    MS_SMP_ASSERT(findBfap ? ADVSMP_BFAP_OWNED(&findBfap->bfaLock) : 1);
    MS_SMP_ASSERT(findBfap ? lk_get_state(findBfap->stateLk) != ACC_RECYCLE : 1);
    MS_SMP_ASSERT(findBfap ? lk_get_state(findBfap->stateLk) != ACC_DEALLOC : 1);
    /*
     * If we are racing with the advfs_access_mgmt_thread trying to move
     * this bfap from the closed to the free list, we may have gotten the
     * lock during a flush.  We need to change the state back to ACC_VALID
     * from ACC_VALID_EXCLUSIVE so that the access_mgmt_thread knowns not to
     * put the bfap on the free list.
     */
    if (findBfap) {
        if ( lk_get_state( findBfap->stateLk ) == ACC_VALID_EXCLUSIVE ) {
            ACCESS_STATS( find_bfap_found_valid_exlc );
            (void)lk_set_state( &findBfap->stateLk, ACC_VALID );
        }
    }
    if (!hold_hashlock)
        BS_BFAH_UNLOCK(hash_key);
    return( findBfap );
}

/*
 * BEGIN_IMS
 *
 * advfs_access_arena_callback - This routine is a callback function for VM.
 * When the access arena grows too large, VM will call back requesting some
 * amount of memory be returned.  This callback will setup of parameters
 * based on VMs request, then call wakeup the advfs_access_mgmt_thread to do
 * cleanup.
 *
 * Parameters:
 *      percentage - The target percentage of the arena to free.  
 *
 * 
 * Return Values:
 *     The function is supposed to return how much memory was actually
 *     freed.  This callback will never do any freeing, so the return will
 *     always be 0.  We cannot block in this routine, so we cannot
 *     invalidate access structures.
 *  
 * Entry/Exit Conditions:
 *
 *
 * Algorithm:
 *
 * END_IMS
 */
static int32_t advfs_access_arena_callback_cnt = 0;

int
advfs_access_arena_callback(int percentage)
{

    advfs_access_arena_callback_cnt++;

    spin_lock( &advfs_manage_access_cv_lock );
    /*
     * If the request is not 0, then we are actively processing a
     * request and will just ignore this one.  
     */
    if (advfs_vm_mem_request == 0) {
        advfs_vm_mem_request = percentage;
        cv_signal( &advfs_manage_access_cv, NULL, CV_NULL_LOCK );
    } else {
        ACCESS_STATS( access_arena_callback_ignored );
    }
    spin_unlock( &advfs_manage_access_cv_lock );
    
    return 0;

}

/*
 * BEGIN_IMS
 *
 * advfs_init_access_mgmt_thread - This thread initializes the
 * advfs_access_mgmt_thread and the locks and cv_t structures it uses.
 *
 *
 * Parameters:
 *
 * 
 * Return Values:
 *      On return, the advfs_access_mgmt_thread is started.  If it failed to
 *      start, the system paniced.
 *  
 * Entry/Exit Conditions:
 *
 *
 * Algorithm:
 *      First, initialize locks and cv_t structures, then start a new thread
 *      running advfs_access_mgmt_thread().
 *
 * END_IMS
 */
void
advfs_init_access_mgmt_thread(void)
{
    extern proc_t *Advfsd;
    tid_t tid;
    statusT sts = 0;

    /* Try to initialize locks and cv_t for the thread. */ 

    
    cv_init( &advfs_manage_access_cv,
            "advfs_manage_access_cv",
            NULL, CV_WAITOK);


    ADVSMP_ACCESS_MGMT_INIT( &advfs_manage_access_cv_lock );

    advfs_acc_mgmt_thread_sleep_time.tv_sec = ADVFS_ACC_MGMT_THREAD_SLEEP_TIME_SECS;
    advfs_acc_mgmt_thread_sleep_time.tv_nsec = ADVFS_ACC_MGMT_THREAD_SLEEP_TIME_NSECS;

    /* Try to spawn thread and panic on failure */
    if (kdaemon_thread_create((void (*)(void *))advfs_access_mgmt_thread,
                               NULL,
                               Advfsd,
                               &tid,
                               KDAEMON_THREAD_CREATE_DETACHED) != 0) {
        ADVFS_SAD0("AdvFS access management thread was not spawned.\n");
    }

}


/*
 * BEGIN_IMS
 *
 * advfs_access_mgmt_thread - As the name implies, this thread is
 * responsible for managing the pool of access structures.  It handles aging
 * of bfaps from the closed list to the free list and finally by to the
 * arena allocator for access structs.
 * 
 * Parameters:
 *
 * 
 * Return Values:
 *      If this function returns it is because of an explicit message to
 *      force its termination.  The function will return causing the
 *      detached thread to be cleaned up by its parent process.
 *
 * Entry/Exit Conditions:
 *
 *
 * Algorithm:
 * The algorithm here is as follows:
 * Get the time before sleeping. 
 * Sleep for some target amount of time.
 * Calculate the percentage of time actually slept compared to target sleep
 * time.  This percentage is used to calculate a weighted average for a
 * dynamic cache aging.  It is assumed that more frequent wakeups will occur
 * because advfs_get_new_access is finding that there are too many existing
 * access structures or because VM is calling back to request the memory
 * arena's consumption be reduced.
 *
 *
 * For each domain, recalculate the cache age time for the domain.  This is
 * done by taking a weighted average of 80% of the current dynamic cache age
 * time, 10% of the target (configured) cache age time, and 10% of the
 * current dynamic cache age time multiplied by the percentage difference
 * between actual and expected sleep time calculated above.  On a steady
 * state system, configure age will be greater than dynamic age, and the
 * percentage of actual and target sleep with be 100%, so the dynamic cache
 * age will drift upwards towards the configured cache age.  On a busy
 * system, the differnece between dynamic cache age time * percentage sleep
 * time and dynamic cache age time will be greater than the difference
 * between configured cache age time and dymanic cache age time, causing the
 * age time to drift downwards and become shorter.  This will have the
 * effect of shortening the time bfAccess structures are aged, thus giving
 * them back to the arena sooner.   The next step of this algorithm will be
 * to make the target sleep time also dynamic so that an asymtotic lower
 * bound on dynamic cache time is reached and we don't make the sleep time
 * too short.
 *
 * Once the dynamic sleep time is calculated, the closed list is walked
 * backwards looking for things older than the dynamic cache age time.
 * ANything older is flushed and moved to the head of the free list where
 * it's cache age is reset.   Once any element is found that is newer than
 * the cache age time, the loop is ended.
 *
 * Next, the free list is walked backwards looking for things older than the
 * dynamic cache age time.  Anything older is invalidated and returned to
 * the memory arena.
 *
 *
 *
 * END_IMS
 */

void
advfs_access_mgmt_thread(void)
{
    int i;
    bfAccessT *bfap = NULL;
    domainT *dmnP = NULL;
    bfAccessT *temp_bfap = NULL;

    uint64_t vm_request = 0;    /* Percentage of bfaps requested to be freed */
    int64_t vm_request_obj_cnt;    /* Number of bfaps requested to be freed */
    
    uint64_t expected_sleep_hz = 0;
    uint64_t last_run_hz = 0;
    uint64_t wakeup_time_hz = 0;
    uint64_t actual_sleep_hz= 0;
    uint32_t time_factor;       /* The factor by which ADVFS_ACCESS_AGE_TIME
                                   is adjusted based on system performance */
    int32_t done;               /* Loop control variable */
    int32_t weight_factor = 100;/* This is an indicator of how much weight 
                                 * should be given to the current age time how
                                 * and how much to the configured age time. 
                                 * As system activity goes up, current 
                                 * age time is given more weight. */
    int32_t freed_bfaps_this_list;
    int32_t freed_bfaps;
    int32_t old_freed_bfaps;
    int32_t free_list_locked;

    while (TRUE) {
        /*
         * Do a timed wait until the next time we should try to do some
         * management activity.  As soon as we wake up, grab the value of
         * advfs_vm_mem_request and reset it to 0.  If vm_request is 0, we
         * either timed out or were woken up prematurely by request from
         * advfs (not vm).
         */
        last_run_hz = ADVFS_TIMEVAL_TO_HZ(get_system_time());
        spin_lock( &advfs_manage_access_cv_lock );
        
        /*
         * Set this to 0 so that the callback will respond to new requests 
         */
        advfs_vm_mem_request = 0;

        /*
         * If advfs_vm_mem_request != 0, VM called us AGAIN while we were 
         * processing the last request.  
         * It must need memory badly, so we will just keep going.
         */
        cv_timedwait( &advfs_manage_access_cv,
                        &advfs_manage_access_cv_lock,
                        CV_SPIN, CV_DFLT_FLG,
                        &advfs_acc_mgmt_thread_sleep_time 
                    );

        vm_request = advfs_vm_mem_request;
        spin_unlock( &advfs_manage_access_cv_lock );

        /* Calculate the approximate number of bfaps to free */
        vm_request_obj_cnt =
            (advfs_acc_ctrl.acc_ctrl_access_cnt * vm_request) / 100;
        
        ACCESS_STATS( advfs_mgmt_thread_wakeups );
        if ( vm_request ) {
            /* 
             * We are being woken up by VM to free some bfaps. 
             */
            ACCESS_STATS( advfs_vm_callback_cnt );
        } else {
            /*
             * If we weren't woken up by VM, then we will just adjust the
             * dmnSoftClosedAgeTime.  If we time out, then we aren't in any
             * pressure and will either move dmnSoftClosedAgeTime up or
             * leave it alone (at it's configured value).  If we were woken
             * up early because AdvFS noticed a lot of access structures in
             * use, we will try to make sure things are being actively moved
             * to the free list in preparation for VM calling in and
             * requesting we give back bfaps.
             */
        
            wakeup_time_hz = ADVFS_TIMEVAL_TO_HZ(get_system_time());

            /* Calculate time_factor. */
            expected_sleep_hz =
                 ADVFS_TIMESPEC_TO_HZ(advfs_acc_mgmt_thread_sleep_time);
            actual_sleep_hz = wakeup_time_hz - last_run_hz; 

            if (actual_sleep_hz <= expected_sleep_hz) {
                ACCESS_STATS( advfs_mgmt_thread_early_wakeup );
                /*
                 * Calculate part of a weighted average to contribute to
                 * dmnSoftClosedAgeTime.  This represents the multiplier for the
                 * 10% part of the new softCacheAgeTime. The remaining 90% of
                 * softCacheAgeTime will be from the current value of
                 * softCacheAgeTime.
                 */
                time_factor = (100 * actual_sleep_hz) / expected_sleep_hz;
                weight_factor = weight_factor * time_factor / 100;
            } else {
                /* Give time_factor full weight.  This will start to pull
                 * the age time back up towards the configured value.  */
                time_factor = 100;
                weight_factor = 100;
            }
        }

        /*
         * This code updates the dmnSoftFreeAgeTime and dmnSoftClosedAgeTime 
         * for each domain.  If vm_request, it updates the free age time.
         * Otherwise, it adjusted closed age time.
         */
        ADVMTX_DOMAINHASHTABLE_LOCK( &DmnHashTblLock );
        if (DmnSentinelP != NULL) {
            dmnP = DmnSentinelP;
            do {
                if (vm_request) {
		    int64_t list_length=0;

		    for(i=0; i<ADVFS_RESOURCE_TABLE_SIZE; i++) {
			list_length += advfs_free_acc_list[i].len;
		    }
                    /* 
                     * If we aren't going to have enough elements to free on
                     * the free list to satisfy the vm request, aggressively
                     * cut the dmnSoftClosedAgeTime now. We will cut it by
                     * the percentage that VM requested us to give back.
                     */
			
                    if (list_length < vm_request_obj_cnt) {
                        dmnP->dmnSoftClosedAgeTime = ((100 - vm_request) *
                                         dmnP->dmnSoftClosedAgeTime) / 100;

                    }
                } else {
                    /* 
                     * If this isn't a vm request, we calculate the a weighted 
                     * average for the new dmnSoftClosedAgeTime.  The average
                     * comes from the configured value, the current value,
                     * and the current value adjusted by the amount of time
                     * we woke up early by.
                     *
                     * The dmnSoftFreeAgeTime is adjusted upwards if we
                     * slept the full amount of time we expect to sleep.
                     * Every time we sleep for the full time, we'll bump
                     * dmnCacheFreeAgeTime by 5% up to the configured max.
                     */
                    /* Clarity would have the following expression read:
                     * (((900 - weight_factor) * dmnP->dmnSoftClosedAgeTime) +
                         (weight_factor * dmnP->dmnConfigCacheAgeTime) +
                         (time_factor * dmnP->dmnSoftClosedAgeTime)) 
                         / 1000;
                     * However, it can be simplified to reduce
                     * multiplications.  
                     */
                    dmnP->dmnSoftClosedAgeTime =
                          (((900 - weight_factor + time_factor) * 
                                     dmnP->dmnSoftClosedAgeTime) +
                               (weight_factor * dmnP->dmnConfigCacheAgeTime)) 
                                             / 1000;
                    if (time_factor == 100) {
                        dmnP->dmnSoftFreeAgeTime =
                            (dmnP->dmnSoftFreeAgeTime * 105) / 100;
                        if (dmnP->dmnSoftFreeAgeTime >
                            dmnP->dmnConfigCacheAgeTime) {
                            dmnP->dmnSoftFreeAgeTime =
                            dmnP->dmnConfigCacheAgeTime;
                        }
                    }
                }
                /* Don't let the closed list age less than
                 * ADVFS_MIN_CLOSED_AGE_TIME_HZ.  This value should be at
                 * least the time interval between sync calls. */
                if (dmnP->dmnSoftClosedAgeTime < ADVFS_MIN_CLOSED_AGE_TIME_HZ) {
                    dmnP->dmnSoftClosedAgeTime = ADVFS_MIN_CLOSED_AGE_TIME_HZ;
                }
                dmnP = dmnP->dmnFwd;
            } while (dmnP != DmnSentinelP);
        }
        ADVMTX_DOMAINHASHTABLE_UNLOCK( &DmnHashTblLock );
        
        /* Process the free list.  If vm_request, then we will try to get as
         * many bfaps as possible until we free the requested amount.
         * Otherwise, we will only free things that have aged sufficiently.
         * Even in the vm_request case, things have already aged on the
         * closed list. */

        freed_bfaps = 0;
        old_freed_bfaps = -1;

        while(    ( freed_bfaps <  vm_request_obj_cnt   )
               && ( freed_bfaps != old_freed_bfaps ) ) {
	  for (i=0; i< ADVFS_RESOURCE_TABLE_SIZE; i++) {
            freed_bfaps_this_list = ADVFS_FREE_BFAPS_PER_LIST+1;
            if ( advfs_free_acc_list[i].len == 0 )
		continue; 
            ADVSMP_FREE_LIST_LOCK( &advfs_access_free_lock[i] );
            free_list_locked = 1;
            if ( advfs_free_acc_list[i].len != 0 ) {
              ACCESS_STATS( advfs_mgmt_thread_free_walks );
              /* Start with the last (oldest) bfap on the free list */
              bfap = advfs_free_acc_list[i].freeBwd;
              done = FALSE;

              while ( (bfap != (bfAccessT*)&advfs_free_acc_list[i]) &&
                     (!done) ) {
                if ( ADVSMP_BFAP_TRYLOCK( &bfap->bfaLock ) ) {

                  /*
                   * If we are in ACC_CREATING or have a refCnt, we want
                   * to just skip this bfap.  If we have a refCnt, we
                   * should be actively syncing the bfap and will get to
                   * it next time around.  
                   * If we are in ACC_CREATING, we shouldn't be in 
                   * this state long, but we don't want
                   * to trash the structure in this state as it will screw
                   * up the create_rtdn_opx routine.
                   */
                   if ( (lk_get_state( bfap->stateLk ) == ACC_CREATING) ||
                        (bfap->refCnt) ) {
                       temp_bfap = bfap;
                       bfap = bfap->freeBwd;
                       ADVSMP_BFAP_UNLOCK( &temp_bfap->bfaLock );
                       ACCESS_STATS( advfs_mgmt_thread_skipped_free_bfap );
                       continue;
                   }

                  /* 
                   * Check to see if we need to continue processing the
                   * list.  If either we don't have a vm_request to free
                   * memory and the current bfap is not aged enough to
                   * return, OR we have a vm_request and we have freed
                   * the requested number of bfaps, we will bail out.
                   */
                   if ( (!(vm_request) && 
                          (wakeup_time_hz - bfap->bfap_free_time  <
                           bfap->dmnP->dmnSoftFreeAgeTime)) ||  
                         ((vm_request) && 
                         ( freed_bfaps > vm_request_obj_cnt)) ) {
                     /* Terminate the loop */
                      done = TRUE;
                      ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                      continue;
                   }
                  /* 
                   * If we are executing this then bfap is old enough to
                   * be returned to the arena or vm has requested memory
                   * back.
                   */

                  /* 
                   * We will ref the bfap before invalidating and freeing.  
                   * This means someone else may try to do something with 
                   * the bfap. When we call advfs_ref_bfap, it will be 
                   * pulled off the free list.  We can't call DEC_REFCNT 
                   * directly because it will try to put it back on the closed
                   * list. DEC_REFCNT will be called with DRC_DEALLOC, but
                   * the bfap will only really be deallocated if refCnt
                   * goes to 0, so we will be able to handle a race.
                   */
                   if ( advfs_ref_bfap( bfap, BF_OP_BFA_LOCK_HELD) == 0 ) {
                    /*
                     * We succeeded in refing the bfap. It's off the
                     * free list.  If we failed, then we're racing a
                     * dealloc or recycle so just skip it.
                     */
                     int err;
                     MS_SMP_ASSERT( bfap->freeListState == NOT_ON_LIST );
                     ADVFS_ACCESS_LOGIT(
                          bfap, "advfs_access_mgmt_thread freeing bfap");
                     ADVSMP_FREE_LIST_UNLOCK( &advfs_access_free_lock[i] );
                     free_list_locked = 0;

                    /* 
                     * We don't need to do an invalidate here because
                     * the call to advfs_dealloc_access from DEC_REFCNT
                     * will invalidate all the pages and do it while
                     * handling potential race conditions.  
                     */

                    /*
                     * DEC_REFCNT will call advfs_dealloc access if
                     * refCnt goes to 0.  Unless we just raced with
                     * someone, this will be the case.  DEC_REFCNT will
                     * also drop the bfaLock.
                     */
                     ACCESS_STATS( advfs_mgmt_thread_free_to_arena );
                     DEC_REFCNT( bfap, DRC_DEALLOC);

                    /*
                     * Racing should be rare, but if we did race, we
                     * can't give back the memory anyways, so we will
                     * just count it as freed and continue.
                     */
                     freed_bfaps++;

                    /* Check to see if we've freed enough from this list.
                     * if we have we'll move on.
                     */
                     if( --freed_bfaps_this_list ) break;
                    /* 
                     * We need to reacquire the free list lock to get
                     * the last element 
                     */
                     ADVSMP_FREE_LIST_LOCK( &advfs_access_free_lock[i]);
                     free_list_locked = 1;
                     bfap = (bfAccessT*)advfs_free_acc_list[i].freeBwd;
                                               
                   } else {
                     ACCESS_STATS( advfs_mgmt_thread_failed_free_ref );
                    /* We are racing dealloc or recycle. Just move to the
                     * next bfap */
                     temp_bfap = bfap;
                     bfap = bfap->freeBwd;
                     ADVSMP_BFAP_UNLOCK( &temp_bfap->bfaLock );
                    }

                } else {
                    ACCESS_STATS( advfs_mgmt_thread_failed_free_lock );
                    /* 
                     * Oh well, couldn't get the lock. Someone else is
                     * probably about to use this bfap so we won't worry too
                     * much about the problem.  Just move on to the next
                     * bfap. We hold the list lock so the freeBwd link can't
                     * change on this bfap even though we don't have it
                     * locked.
                     */
                    bfap = bfap->freeBwd;
                } /* End of if trylock */
              } /* End of while loop to scan bfaps */
            } /* End of if free list empty */
            if( free_list_locked )
                ADVSMP_FREE_LIST_UNLOCK( &advfs_access_free_lock[i] );
          } /* End of for loop through free lists */
          old_freed_bfaps = freed_bfaps;
	} /* End of while loop for enough bfaps freed */
        /* End of free list processing */

        /*
         * Examine the closed list and take appropriate actions.
         * When we walk the list, the oldest bfaps are at the end 
         */
	for (i=0; i< ADVFS_RESOURCE_TABLE_SIZE; i++) {
          if ( advfs_closed_acc_list[i].len == 0 )
		continue; 
          ADVSMP_CLOSED_LIST_LOCK( &advfs_access_closed_lock[i] );
          if ( advfs_closed_acc_list[i].len != 0 ) {
            ACCESS_STATS( advfs_mgmt_thread_closed_walks );
            /* Start with the last (oldest) bfap on the closed list */
            bfap = advfs_closed_acc_list[i].freeBwd;
            done = FALSE;

            while ( (bfap != (bfAccessT*)&advfs_closed_acc_list[i]) &&
                    (!done) ) {
                if ( ADVSMP_BFAP_TRYLOCK( &bfap->bfaLock ) ) {

                    /*
                     * If we are in ACC_CREATING or have a refCnt, we want
                     * to just skip this bfap.  If we have a refCnt, we
                     * should be actively syncing the bfap and will get to
                     * it next time around.  
                     * If we are in ACC_CREATING, we shouldn't be in 
                     * this state long, but we don't want
                     * to trash the structure in this state as it will screw
                     * up the create_rtdn_opx routine.
                     */
                    if ( (lk_get_state( bfap->stateLk ) == ACC_CREATING) || 
                         (bfap->refCnt) ) {
                        temp_bfap = bfap;
                        bfap = bfap->freeBwd;
                        ADVSMP_BFAP_UNLOCK( &temp_bfap->bfaLock );
                        ACCESS_STATS( advfs_mgmt_thread_skipped_closed_bfap );
                        continue;
                    }
                    /* 
                     * Check to see if we need to continue processing the
                     * list.  As of now, we stop processing as soon as we
                     * hit the first bfap who is younger than it's domain
                     * age time.  THis means we don't fully respect the age
                     * times domain wide.
                     */
                    if (wakeup_time_hz - bfap->bfap_free_time  <
                        bfap->dmnP->dmnSoftClosedAgeTime) {
                        /* Terminate the loop */
                        done = TRUE;
                        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                        continue;
                    }

                    /*
                     * If we are executing here then bfap is old enough to
                     * be moved to the free list.
                     */

                    
                    /* 
                     * We will ref the bfap before flushing.  This means
                     * someone else may try to do something with the bfap.
                     * When we call advfs_ref_bfap, it will be pulled off
                     * the closed list.  When DEC_REFCNT is called, it will
                     * be passed DRC_FREE_LIST to specify the bfap should go
                     * straight to the free list.
                     */
                    if ( advfs_ref_bfap( bfap, BF_OP_BFA_LOCK_HELD ) == 0 ) {
                        int err;
                        /*
                         * We succeeded in refing the bfap. It's off the
                         * closed list.  If we failed, then we're racing a
                         * dealloc or recycle so just skip it.
                         */
                        MS_SMP_ASSERT( bfap->freeListState == NOT_ON_LIST );
                        ADVFS_ACCESS_LOGIT( bfap, "advfs_access_mgmt_thread moving bfap");


                        /*
                         * We must be certaint that no one comes in an
                         * touches this access structure while we are
                         * flushing.  If they do, they may dirty pages and
                         * we would be in a world of hurt if we then put it
                         * on the free list.  We will set the state to
                         * ACC_VALID_EXCLUSIVE knowing that anyone that
                         * finds it in this state will set it to ACC_VALID
                         * and continue using the bfap.  When we wake up, we
                         * will check that it is still in
                         * ACC_VALID_EXCLUSIVE.  If it is not, then someone
                         * touched it.
                         */
                        MS_SMP_ASSERT( lk_get_state( bfap->stateLk) == ACC_VALID);
                        (void)lk_set_state( &bfap->stateLk, ACC_VALID_EXCLUSIVE ); 

                        /*
                         * Unlock locks for the flush
                         */
                        ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                        ADVSMP_CLOSED_LIST_UNLOCK( &advfs_access_closed_lock[i] );

                        /*
                         * Only attempt to flush pages if we are not in a 
                         * domain panic.
                         */
                        if (!bfap->dmnP->dmn_panic) {
                            /*
                             * This bfap has aged fully on the closed list.
                             * Flush and free it's pages so they can be reused
                             * by vm.
                             */
                            err = fcache_vn_flush( &bfap->bfVnode, 
                                                   (off_t) 0, 
                                                   (size_t) 0,  /* The entire 
                                                                 * file */
                                                   NULL,
                                                   FVF_WRITE | FVF_SYNC | FVF_FREE);
                            /* Flush any dirty file statistics to disk. */
                            if (bfap->bfFsContext.dirty_stats) {
                                (void)fs_update_stats(&bfap->bfVnode, 
                                                      bfap, 
                                                      FtxNilFtxH, 
                                                      0);
			    }
                        } 
                        /*
                         * Invalidate the pages if we are in a domain panic.
                         */
                        if (bfap->dmnP->dmn_panic) {
                            err= fcache_vn_invalidate( &bfap->bfVnode,
                                                      (off_t) 0,
                                                      (size_t)0,
                                                      NULL,
                                                      (BS_IS_META(bfap)?FVI_PPAGE:0)|FVI_INVAL);
                        }
                        MS_SMP_ASSERT(err == 0);

                        ADVSMP_BFAP_LOCK( &bfap->bfaLock );

                        if (lk_get_state( bfap->stateLk ) == ACC_VALID_EXCLUSIVE) {
                            /*
                             * Call DEC_REFCNT specifying that the bfap is clean
                             * (put in on the free list). DEC_REFCNT will drop
                             * the bfaLock.
                             */
                            (void)lk_set_state( &bfap->stateLk, 
                                          ACC_VALID);
                            ACCESS_STATS( advfs_mgmt_thread_closed_to_free );
                            DEC_REFCNT( bfap, DRC_FREE_LIST);
                        } else {
                            /* 
                             * We no longer have exclusive access to this
                             * bfap.  If the refCnt is 1, then someone came
                             * in and left during the flush and we are the
                             * last one to hold a reference on the bfap.
                             * The problem with that is that if they opened
                             * the file and deleted it, the delete will hold
                             * off until last close.  If we just DEC_REFCNT,
                             * that last close will not happen.  As a
                             * result, we will bs_close if refCnt == 1, else
                             * DEC_REFCNT.
                             */
                            if (bfap->refCnt == 1) {
                                /*
                                 * If the state is ACC_INVALID we raced with
                                 * access_invalidate and need to dealloc the
                                 * access structure.
                                 */
                                if (lk_get_state( bfap->stateLk ) == ACC_INVALID) {
                                    ACCESS_STATS( advfs_mgmt_thread_acc_invalid_dealloc );
                                    DEC_REFCNT( bfap, DRC_DEALLOC );
                                } else {
                                    ACCESS_STATS( advfs_mgmt_thread_race_calling_close );
                                    ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
                                    bs_close(bfap, MSFS_CLOSE_NONE);
                                }
                            } else {
                                DEC_REFCNT( bfap, DRC_CLOSED_LIST );
                            }
                        }


                        /* 
                         * Reacquire the closed list lock to continue. We
                         * are processing from the end, so we should be
                         * eliminating older things or racing with things
                         * that should be gone quickly, so we can just start
                         * again at the end.
                         */
                        ADVSMP_CLOSED_LIST_LOCK( &advfs_access_closed_lock[i] );
                        bfap = (bfAccessT*)advfs_closed_acc_list[i].freeBwd;

                    } else {
                        ACCESS_STATS( advfs_mgmt_thread_failed_closed_ref );
                        /* We racing dealloc or recycle. Just move to the next bfap
                         */
                        temp_bfap = bfap;
                        bfap = bfap->freeBwd;
                        ADVSMP_BFAP_UNLOCK( &temp_bfap->bfaLock );
                    }

                } else {
                    ACCESS_STATS( advfs_mgmt_thread_failed_closed_lock );
                    /* 
                     * Oh well, couldn't get the lock. Someone else is
                     * probably about to use this bfap so we won't worry too
                     * much about the problem.  Just move on to the next
                     * bfap. We are holding the list lock, so the thread
                     * that holds the bfa lock on this bfap can't change the
                     * freeBwd link under us. (If it plays fair).
                     */
                    bfap = bfap->freeBwd;
                }
                if (bfap == (bfAccessT*)&advfs_closed_acc_list[i]) {
                    done = TRUE;
                }
            }
          }
          ADVSMP_CLOSED_LIST_UNLOCK( &advfs_access_closed_lock[i] );
        }
        /* End of processing the closed list */

        /* 
         * Do dymanic thread tuning.  Calculate how long to sleep next.
         * Our goal is to get an asymtotic age time.  By reducing the
         * expected sleep time, we slow down the rate at which the 
         * cache age time will be decreased. */
        if (time_factor < 100) {
            /* We woke up earlier than we had planned, so we want to check
             * if the system is heavily burdened and try to wake up earlier
             * next time.  This will give us an assymtotic behavior for
             * our cache aging times.  We will reduce the sleep time by up
             * to 5% of it's currently time each iteration (thus keep 95%).  
             * The 5% will
             * be weighted by the 100 - time_factor (if we slept 80% of the
             * desired time, we will reduce by 5% * 20%).  
             */
            advfs_acc_mgmt_thread_sleep_time.tv_sec = 
                (advfs_acc_mgmt_thread_sleep_time.tv_sec * 
                 (95 + ((5 * (100 - time_factor)) / 100))) / 100;
            if (advfs_acc_mgmt_thread_sleep_time.tv_sec < 1)
                advfs_acc_mgmt_thread_sleep_time.tv_sec = 1;
        } else {
            /* We slept as long as we expected to sleep before we woke up
             * so we should let the sleep time move up towards the
             * configured sleep time if possible.  For now, we'll 
             * try to add 5% of the default sleep time each iteration.
             * We max out at the configured sleep time.
             */
            advfs_acc_mgmt_thread_sleep_time.tv_sec += 
                (ADVFS_ACC_MGMT_THREAD_SLEEP_TIME_SECS * 5) / 100;
            advfs_acc_mgmt_thread_sleep_time.tv_sec = 
                min(advfs_acc_mgmt_thread_sleep_time.tv_sec,
                        ADVFS_ACC_MGMT_THREAD_SLEEP_TIME_SECS);
                   

        }
    }
}


/*
 * BEGIN_IMS
 *
 * access_invalidate - Marks all of the access structures for a fileset as invalid
 * and calls advfs_dealloc_access on them.  

 * Parameters:
 *
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      
 *
 *
 * Algorithm:
 *
 * END_IMS
 */
void
access_invalidate(struct bfSet *bfSetp)
{
    bfAccessT *bfap;

start:
    ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock);

    /* 
     * Lookup until the bfSetp list is empty. As we process the list, we are
     * removing access structures, and no one else should be adding any.
     */
    bfap = bfSetp->accessFwd;
    while (bfap != (bfAccessT *)(&bfSetp->accessFwd) ) {

        MS_SMP_ASSERT(bfap->accMagic == ACCMAGIC);
        /*
         * Lock the access structure.
         */
        ADVSMP_BFAP_LOCK(&bfap->bfaLock);
        MS_SMP_ASSERT(bfap->bfSetp == bfSetp);

        /*
         * If the access structure state is ACC_DEALLOC, ACC_RECYCLE,
         * ACC_INVALID or ACC_INIT_TRANS, it may be deallocated shortly.  If it 
         * is ACC_INIT_TRANS, it may also become valid.  Either way, we will
         * just sleep briefly and let another thread have a chance to finish
         * the work on this access structure.
         */
        if ( (lk_get_state(bfap->stateLk) == ACC_DEALLOC) ||
             (lk_get_state(bfap->stateLk) == ACC_RECYCLE) ||
             (lk_get_state(bfap->stateLk) == ACC_INVALID) ||
             (lk_get_state(bfap->stateLk) == ACC_INIT_TRANS) ) {
            ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);
            lk_wait( &bfap->stateLk, &bfap->bfaLock );
            ACCESS_STATS( access_invalidate_looped );
            goto start;
        }

        /*
         * Make sure access structure isn't in use since
         * we're about to invalidate it.  It could be being moved
         * from the closed to the free list in which case it will have a
         * refCnt of 1 and be in ACC_VALID_EXCLUSIVE.  We will also put a
         * reference on the bfap when moving it from the free list to the
         * arena, but in that case, the bfaLock is never dropped, so we
         * couldn't get into this get and by the time we had acquired the
         * lock, the refCnt would be 0 and the state would be ACC_DEALLOC.   
         */

         /*
          * Don't care if it is in use if we have a domain panic.
          */
         MS_SMP_ASSERT((bfap->refCnt == 0) ||
                       ((bfap->refCnt == 1) &&
                        (lk_get_state(bfap->stateLk) == ACC_VALID_EXCLUSIVE)) ||
                        bfap->dmnP->dmn_panic);
        
        if (lk_get_state( bfap->stateLk ) == ACC_VALID_EXCLUSIVE) {
            /*
             * By setting the state to ACC_INVALID here, we will force the
             * access mgmt thread to just deallocate the access structure.
             * We will be woken up when the state changes out of
             * ACC_DEALLOC.
             */
            (void)lk_set_state( &bfap->stateLk, ACC_INVALID );
            ADVSMP_SETACCESSCHAIN_UNLOCK( &bfSetp->accessChainLock );

            /*
             * This will unlock the bfaLock.
             */
            lk_wait( &bfap->stateLk, &bfap->bfaLock);

            ADVSMP_SETACCESSCHAIN_LOCK( &bfSetp->accessChainLock );
            bfap = bfSetp->accessFwd;
            continue;

        }
        (void)lk_set_state( &bfap->stateLk, ACC_INVALID );


        /*
         * This will cause the access structure to be returned to the
         * advfs_bfaccess_arena since the state is ACC_INVALID. It will also
         * destroy the bfaLock, so we can't unlock it (it will be done in
         * advfs_dealloc_access).  The accessChainLock is dropped so
         * advfs_dealloc_access can acquire it during the deallocation.  
         */
        ADVSMP_SETACCESSCHAIN_UNLOCK( &bfSetp->accessChainLock );

        if (bfap->freeListState == ON_FREE_LIST) {
	    uint64_t myindex;

            myindex = ACCESS_LIST_HASH(bfap);

            ADVSMP_FREE_LIST_LOCK( &advfs_access_free_lock[myindex] );
            RM_ACC_LIST_NOLOCK( bfap,
				&advfs_free_acc_list[myindex],
				&advfs_access_free_lock[myindex] );
            ADVSMP_FREE_LIST_UNLOCK( &advfs_access_free_lock[myindex] );
        } else if ( bfap->freeListState == ON_CLOSED_LIST ) {
	    uint64_t myindex;

            myindex = ACCESS_LIST_HASH(bfap);

            ADVSMP_CLOSED_LIST_LOCK( &advfs_access_closed_lock[myindex] );
            RM_ACC_LIST_NOLOCK( bfap,
				&advfs_closed_acc_list[myindex],
				&advfs_access_closed_lock[myindex] );
            ADVSMP_CLOSED_LIST_UNLOCK( &advfs_access_closed_lock[myindex] );

        }
       
        /*
         * Don't care if it is in use if we have a domain panic.
         */
        MS_SMP_ASSERT( (bfap->bfVnode.v_count == 0) || bfap->dmnP->dmn_panic);

        advfs_dealloc_access( bfap );
        ADVSMP_SETACCESSCHAIN_LOCK( &bfSetp->accessChainLock );
        bfap = bfSetp->accessFwd;
    }
    ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock);

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
    int err;

    /*
     * When we opened the BMT, we reinitialized these two locks to have a
     * different hierarchy order since we need to read the BMT's extents
     * while holding extent locks for other files.  Now we will set them
     * back to the default hierarchy values.
     */
    ADVRWL_MCELL_LIST_DESTROY( &bfap->mcellList_lk.lock.rw );
    ADVRWL_MCELL_LIST_INIT( &bfap->mcellList_lk.lock.rw); /* Lock       */
    ADVRWL_XTNTMAP_DESTROY( &bfap->xtntMap_lk );
    ADVRWL_XTNTMAP_INIT( &bfap->xtntMap_lk); /* Lock       */


    /*
     * Call be closed with the MSFS_CLOSE_DEALLOC flag to tell close to
     * trash the bfap when it does the DEC_REFCNT.
     */
    err = bs_close( bfap, MSFS_CLOSE_DEALLOC );
    if (err != EOK) {
        ADVFS_SAD1( "bs_invalidate_rsvd_access_struct: bs_close() failed", err);
    }

    return;
}  /* end bs_invalidate_rsvd_access_struct */

/*
 * bs_reclaim_cfs_rsvd_vn
 *
 * since CFS will not keep a ref on the PFS vnode/access structure when they
 * don't have a ref anymore, this function is a stub.
 */

statusT
bs_reclaim_cfs_rsvd_vn(
                       bfAccessT *bfap  /* in */
                       )
{
    ACCESS_STATS( bs_reclaim_cfs_rsvd_vn_called );
    return EOK;

}

uint64_t
advfs_open_max_roundup( uint64_t new_open_max )
{
    return (    ((new_open_max+ADVFS_RESOURCE_TABLE_SIZE-1)
                              /ADVFS_RESOURCE_TABLE_SIZE   )
             * ADVFS_RESOURCE_TABLE_SIZE);
}

void
advfs_set_open_min( uint64_t new_open_min )
{
    uint64_t old_open_min;
    uint32_t i;
    uint64_t value64;
    uint64_t difference;
    uint32_t per_entry_adjustment;

    /* If we haven't initialized the access area yet, don't do anything.
     * ktval_advfs_open_min will be set to the value, and we'll pick it
     * up at init time.
     */

    if( !advfs_acc_ctrl_initted ) return;

    spin_lock( &advfs_acc_ctrl.acc_ctrl_lk);
 
    old_open_min = advfs_acc_ctrl.acc_ctrl_min;

    advfs_acc_ctrl.acc_ctrl_min = new_open_min;

    /* If we are adjusting upward to a number greater than the
     * number of existing access structures, increment minalloc
     * to force the number up to open_min.
     */

    if( new_open_min >   advfs_acc_ctrl.acc_ctrl_access_cnt ) {
        difference = new_open_min-advfs_acc_ctrl.acc_ctrl_access_cnt;
        ADVFS_ATOMIC_FETCH_ADD( &advfs_access_minalloc,
                               difference, &value64 );
    }
    /* If adjusting downward, and the new number would decrease
     * minalloc,  change it now.
     */
    else {
        /* Calculate the delta in soft min */
        difference = new_open_min - old_open_min;
        /* If the abs. value of the difference is greater than
           minalloc, use minalloc instead. */
        if( (advfs_access_minalloc - difference) < 0 )
             difference = -advfs_access_minalloc;
        ADVFS_ATOMIC_FETCH_ADD( &advfs_access_minalloc,
                           difference, &value64 );
        /* If we raced and made it negative, fix it */
        while( advfs_access_minalloc < 0 )
        ADVFS_ATOMIC_FETCH_INCR ( &advfs_access_minalloc, &value64 );
    }
    spin_unlock( &advfs_acc_ctrl.acc_ctrl_lk);
}

void
advfs_set_open_max( uint64_t new_open_max )
{
    uint64_t old_soft_max;
    uint64_t new_soft_max;
    uint32_t i;
    uint64_t value64;
    uint64_t difference;
    uint64_t per_entry_adjustment;

    /* If we haven't initialized the access area yet, don't do anything.
     * ktval_advfs_open_max will be set to the value, and we'll pick it
     * up at init time.
     */

    if( !advfs_acc_ctrl_initted ) return;

    spin_lock( &advfs_acc_ctrl.acc_ctrl_lk);
 
    old_soft_max = advfs_acc_ctrl.acc_ctrl_soft_max;

    advfs_acc_ctrl.acc_ctrl_config_max = new_open_max;

    new_soft_max =  
        max(       advfs_acc_ctrl.acc_ctrl_config_max,
             min(   advfs_acc_ctrl.acc_ctrl_soft_max,
		  ((advfs_acc_ctrl.acc_ctrl_open_cnt*11)/10) ) );
    new_soft_max = advfs_open_max_roundup( new_soft_max);
    advfs_acc_ctrl.acc_ctrl_soft_max = new_soft_max;
    spin_unlock( &advfs_acc_ctrl.acc_ctrl_lk);

    /* If we are adjusting upward, distribute the credits. If adjusting
     * downward, set the adjustment count and let the access management
     * thread deal with it.
     */

    if( new_soft_max > old_soft_max ) {
        difference = new_soft_max-old_soft_max;
        per_entry_adjustment = 
            ( new_soft_max-old_soft_max ) /  ADVFS_RESOURCE_TABLE_SIZE;
        for( i=0; i<ADVFS_RESOURCE_TABLE_SIZE; i++ )
            ADVFS_ATOMIC_FETCH_ADD( &advfs_access_credits[i],
                               per_entry_adjustment, &value64 );
    } else if (new_soft_max < old_soft_max ) {
        per_entry_adjustment = ( old_soft_max-new_soft_max );
        ADVFS_ATOMIC_FETCH_ADD( &advfs_access_adjustment,
                           per_entry_adjustment, &value64 );
        spin_lock( &advfs_manage_access_cv_lock );
        cv_signal( &advfs_manage_access_cv,
                   &advfs_manage_access_cv_lock , CV_SPIN );
    }
}

/*
 * advfs_access_area_init 
 *
 * Initialize things that are generally applicable to access structures. 
 *
 * No error return.
 *
 * SMP
 */

void
advfs_access_area_init()
{
    int i;
    bfAccessT *new_bfap;
    
    /*
     * This enables UFC code for files using this vfsp
     */
    advfs_meta_vfs.vfs_version = 1;

    advfs_acc_ctrl.acc_ctrl_open_cnt    = 0;
    advfs_acc_ctrl.acc_ctrl_access_cnt  = 0;
    advfs_vm_mem_request                = 0;
    advfs_borrowed_credits              = 0;
    advfs_override_credits              = 0;
    advfs_access_minalloc               = 0;

    spin_init( &advfs_acc_ctrl.acc_ctrl_lk,
	       "advfs_access_control_lock",
	       NULL,
	       SPIN_WAITOK,
	       ADVFS_LOCK_ORDER_HIGH,
	       NULL);

    /* We first set config and soft max to 0, then call advfs_set_open_max
     * to do the real set. This will distribute the credits. This requires
     * the lock to have been initialized.
     */

    advfs_acc_ctrl.acc_ctrl_min         = 0;
    advfs_acc_ctrl.acc_ctrl_config_max  = 0;
    advfs_acc_ctrl.acc_ctrl_soft_max    = 0;
    advfs_acc_ctrl_initted              = 1;
    advfs_set_open_max( ktval_advfs_open_max );
    advfs_set_open_min( ktval_advfs_open_min );

    for (i=0; i< ADVFS_RESOURCE_TABLE_SIZE; i++) {
        ADVSMP_FREE_LIST_INIT( &advfs_access_free_lock[i] );
        ADVSMP_CLOSED_LIST_INIT( &advfs_access_closed_lock[i] );

        /*
         * Initialize the free list and the closed list.
         */
        advfs_free_acc_list[i].freeFwd = advfs_free_acc_list[i].freeBwd = 
                                  (bfAccessT *)&advfs_free_acc_list[i];
        advfs_free_acc_list[i].len = 0;

        advfs_closed_acc_list[i].freeFwd = advfs_closed_acc_list[i].freeBwd = 
                                    (bfAccessT *)&advfs_closed_acc_list[i];
        advfs_closed_acc_list[i].len = 0;
    }

    BsAccessHashTbl =dyn_hashtable_init(BS_BFAH_INITIAL_SIZE,
                                        BS_BFAH_HASH_CHAIN_LENGTH,
                                        BS_BFAH_ELEMENTS_TO_BUCKETS,
                                        BS_BFAH_USECS_BETWEEN_SPLITS,
                                        advfs_offsetof(bfAccessT,hashlinks),
                                        NULL);
    if (BsAccessHashTbl == NULL) {
        ADVFS_SAD0("bs_init_area: can't get space for hash table");
    }

#ifdef ADVFS_ACCESS_STATS
    bzero( &advfs_access_stats, sizeof( advfs_access_stats_t ) );
#endif

#ifdef ADVFS_ACCESS_TRACE 
    ADVSMP_ACCESS_TRACE_INIT( &access_trace_lock );
    {
        int i = 0;
        for (i = 0; i < ADVFS_ACCESS_LOG_SIZE; i++) {
           advfs_access_log[i].tag = 0;
           advfs_access_log[i].refCnt = 0;
           advfs_access_log[i].v_count= 0;
           advfs_access_log[i].v_scount= 0;
           advfs_access_log[i].line = 0;
           bzero( &advfs_access_log[i].caller, (size_t) 40);
           bzero( &advfs_access_log[i].file, (size_t) 40);
 
           
        }
    }
#endif
    return;
}
/*
 * BEGIN_IMS
 *
 * bs_map_bf - This routine creates the in-memory mapping structures for the
 * specified bitfile.  The caller must have exclusive access to the access
 * structure.  
 *
 * For child snapshots, the data may be read from the a parents primary
 * mcell.  In this case, the parents mcell will not be locked (the
 * mcellList_lk will not be held) but the list could not change since any
 * changes would cause a COW which would block waiting for this child
 * snapshot to be opened.  Thusly, it is safe to just read the parents data. 
 *
 * Parameters:
 *
 * 
 * Return Values:
 * EBAD_DOMAIN_POINTER, EBAD_VDI - invalid input
 * E_BAD_MCELL_LINK_SEGMENT, EBMTR_NOT_FOUND, ENO_MORE_MEMORY - critical
 * errors
 *
 * Other errors from bs_refpg.
 *
 * Entry/Exit Conditions:
 * On entry, must have exclusive access to the bitfile.
 *      
 *
 *
 * Algorithm:
 *
 * END_IMS
 */

static statusT
bs_map_bf(
    bfAccessT* bfap,             /* in/out - ptr to bitfile's access struct */
    enum acc_open_flags options, /* in - options flags (see bs_access.h) */
    bfTagFlagsT  tagFlags        /* in - flags to set various bfap values */ 
          )
{
    struct vd* vdp;
    struct domain* dmnP;
    bfPageRefHT pgref;
    statusT sts;
    struct bsMPg* bmtp;
    struct bsMC* mcp;
    bsBfAttrT* bfattrp;
    fcache_vn_attr_t fcacheAttr;
    int pgRefed = FALSE;
    int32_t     set_file_size = FALSE;
    struct fsContext* fs_context_p;
     

    /*-----------------------------------------------------------------------*/

    MS_SMP_ASSERT( (lk_get_state( bfap->stateLk ) == ACC_INIT_TRANS) ||
                   (lk_get_state( bfap->stateLk ) == ACC_CREATING) );

    dmnP = bfap->dmnP;
    vdp = VD_HTOP((vdIndexT) bfap->primMCId.volume, dmnP);

    /*
     * This routine should not be called with the bfap mapped.
     */
    MS_SMP_ASSERT( !(bfap->bfaFlags & BFA_MAPPED ) );

    if (vdp->rbmtp == NULL) {
        /*
         * special case - this is the RBMT we are mapping during
         * initial domain activation (that is why the RBMT bfap is NULL),
         * so use the special buffer it got read into.
         */
        if ((bfap->primMCId.cell != 0) && (bfap->primMCId.page != 0)) {
            ADVFS_SAD0( "bs_map_bf: RBMT not mapped" );
        }

        bmtp = dmnP->metaPagep;
        pgref = NilBfPageRefH;

    } else if (BS_BFTAG_RSVD(bfap->tag)) {
        /* access the reserved metadata bitfile page */

        sts = bs_refpg(&pgref,
                      (void*)&bmtp,
                      vdp->rbmtp,
                      bfap->primMCId.page,
                      FtxNilFtxH,
                      MF_VERIFY_PAGE);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        pgRefed = TRUE;

    } else {
        /* access the metadata bitfile page */

        sts = bs_refpg(&pgref,
                       (void*)&bmtp,
                       vdp->bmtp,
                       bfap->primMCId.page,
                       FtxNilFtxH,
                       MF_VERIFY_PAGE);
        if (sts != EOK) {
            RAISE_EXCEPTION( sts );
        }

        pgRefed = TRUE;
    }

    mcp = &bmtp->bsMCA[bfap->primMCId.cell];

    if (!BS_BFTAG_EQL(mcp->mcTag, bfap->tag)) {
        RAISE_EXCEPTION( EBAD_TAG );
    }

    if (mcp->mcLinkSegment != 0) {
        RAISE_EXCEPTION( E_BAD_MCELL_LINK_SEGMENT );
    }

    /* scan the primary mcell for the bitfile's attributes */

    bfattrp = (bsBfAttrT*)bmtr_find( mcp, BSR_ATTR, bfap->dmnP);
    if ( !bfattrp ) {
        RAISE_EXCEPTION( EBMTR_NOT_FOUND );
    }

    if ( (tagFlags & BS_TD_ROOT_SNAPSHOT) ||
         (BS_BFTAG_RSVD(bfap->tag)) ) {
        bfap->bfaFlags |= BFA_ROOT_SNAPSHOT;
    }

    if (tagFlags & BS_TD_DEL_WITH_CHILDREN) {
        bfap->bfaFlags |= BFA_DEL_WITH_CHILDREN;
    }

    if (tagFlags & BS_TD_FRAGMENTED) {
        bfap->bfaFlags |= BFA_FRAGMENTED;
    }

    /*
     * Don't set the out of sync flag for any files in the root bitfile
     * set.  The tag dir entries for snapsets may be marked hard 
     * out-of-sync (we ignore it here). We will look for this tag flag in 
     * bfs_access().
     */
    if (tagFlags & BS_TD_OUT_OF_SYNC_SNAP && 
            !BS_BFTAG_EQL(bfap->bfSetp->dirTag, staticRootTagDirTag)) {
        bfap->bfaFlags |= BFA_OUT_OF_SYNC;
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
     * (currently) a directory or index.
     */    
    if (((bfattrp->state == BSRA_INVALID) &&
         (!BS_BFTAG_EQL(bfap->tag, bfap->dmnP->ftxLogTag))) ||
        ((bfattrp->state == BSRA_DELETING) &&
         !(clu_is_ready() && (options & BS_FAILOVER)) &&
          (bfap->dmnP->state == BFD_ACTIVATED) &&
          !(options & BS_ON_DDL)))
    {
        ACCESS_STATS( bs_map_bf_error_no_tag );
        RAISE_EXCEPTION( ENO_SUCH_TAG );
    }

    bfap->bfPageSz       = bfattrp->bfPgSz;
    bfap->reqServices    = bfattrp->reqServices;
    bfap->bfState        = bfattrp->state;
    bfap->transitionId   = bfattrp->transitionId;
    bfap->rsvd_file_size = bfattrp->bfat_rsvd_file_size;
    bfap->dataSafety = tagFlags & BS_DATA_SAFETY_MASK; 

    /*
     * Set up the bitfile's fsContext area. The files stats are
     * read in from the bmt record
     */

    /* Init the fsContext */
    fs_context_p = &bfap->bfFsContext;
    fs_context_p->bf_tag = bfap->tag;

    /*
     * read the stats from the BMT
     */
    sts = bmtr_get_rec( bfap,
                        BMTR_FS_STAT,
                        &fs_context_p->dir_stats,
                        (uint16_t) sizeof(statT));
    if (sts == EBMTR_NOT_FOUND) {
        /* 
         * One of two cases, this could be a metaopen, or this could be a
         * create that has not yet put the BMTR_FS_STAT record on disk.  In
         * either case, we will setup the file_size based on the bfaNextFob
         * later, once we initialize bfaNextFob.  Additionally, we ought to
         * initialize the stats fields.
         */
        bzero(&fs_context_p->dir_stats, sizeof( fs_context_p->dir_stats ) );
        set_file_size = TRUE;
    } else if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    fs_context_p->fs_flag = 0;
    fs_context_p->dirty_stats = 0;

    bfap->file_size = fs_context_p->dir_stats.st_size;

    ADVFS_ACCESS_LOGIT( bfap, "bs_map_bf: just get FS_STAT");

    fs_context_p->dirstamp = 0;
    fs_context_p->undel_dir_tag = NilBfTag;
    fs_context_p->last_offset = 0;
    /*
     * End of init of fsContext area
     */


    MS_SMP_ASSERT( bfap->dataSafety != BFD_NOT_INITIALIZED );

    /* Assign the UFC attributes for this vnode. Metadata and user data vnodes
     * may have different attributes.
     * A UFC restriction is that static attributes must be set prior to
     * the first cache mapping request and can be set only once.
     */
    if (sts = fcache_vn_getattr(&bfap->bfVnode,
                                &fcacheAttr,
                                sizeof(fcache_vn_attr_t))) {
        MS_SMP_ASSERT( (sts == EINVAL) );
        RAISE_EXCEPTION(sts);
    }

    if (bfap->dataSafety == BFD_USERDATA) {

#define UFC_ENABLE_LARGE_PAGES 

#ifdef UFC_ENABLE_LARGE_PAGES 
/*
 * ---> This will turn on large page support with 16K pages.
 *                             
 */
        /*
         * Do not enable large pages for files that have an allocation unit
         * smaller than VM_PAGE_SZ.
         */
        if (!ADVFS_FILE_HAS_SMALL_AU(bfap)) {
            fcacheAttr.fva_block_size = 16*1024;
            fcacheAttr.fva_page_size_hint= 16*1024;
            fcacheAttr.fva_page_size_ceiling = 0;
        }
        else {
            fcacheAttr.fva_block_size = (size_t)(bfap->bfPageSz * ADVFS_FOB_SZ);
            fcacheAttr.fva_page_size_hint= 0;
            fcacheAttr.fva_page_size_ceiling = VM_PAGE_SZ;
        }
#else

        fcacheAttr.fva_block_size = (size_t) (bfap->bfPageSz * ADVFS_FOB_SZ);
        fcacheAttr.fva_page_size_hint= 0;
        fcacheAttr.fva_page_size_ceiling = VM_PAGE_SZ;

#endif
    }
    else {
        /* Metadata attribute assignments.
         * Setup the page size hint to equal the AdvFS metadata page size
         * so that UFC attempts to allocate memory in this size.
         */
        fcacheAttr.fva_block_size = 
                (size_t)(bfap->bfPageSz * ADVFS_FOB_SZ);
        fcacheAttr.fva_page_size_hint=
                (size_t)(bfap->bfPageSz * ADVFS_FOB_SZ);
    }

    /* Set flag so that UFC inserts our dirty pages onto the syncer threads. */
    fcacheAttr.fva_sync_type = FVA_SYNC_RDWR;

    if (sts = fcache_vn_setattr(&bfap->bfVnode,
                                &fcacheAttr,
                                sizeof(fcache_vn_attr_t))) {
        MS_SMP_ASSERT( (sts == EINVAL) );
        RAISE_EXCEPTION(sts);
    }




    /*
     * Setup the bfap's bfa_orig_file_size
     */
    if (!HAS_SNAPSHOT_PARENT( bfap ) ) {
        bfap->bfa_orig_file_size = ADVFS_ROOT_SNAPSHOT;
    } else {
        /* This is a snapshot.  Deal with it */
        if (tagFlags & BS_TD_VIRGIN_SNAP) {
            /* 
             * If the snap does not have its own metadata, then it's
             * file_size was set from the parent.  The orig_file_size is the
             * same as file_size since the parent could not have changed yet
             * (or bfap would have it's own metdata!).
             */
            if ( IS_COWABLE_METADATA( bfap ) ) {
                /* Metadata is based on bfaNextFob, but bfaNextFob is not
                 * correct since the extents have not been loaded and may
                 * not be accurate for snapshots children..
                 * For metadata, set it to 0 and intialize
                 * it later once the parents are opened. */
                bfap->bfa_orig_file_size = 0;
                bfap->bfaFlags |= BFA_VIRGIN_SNAP;
            } else if (bfap->dataSafety == BFD_USERDATA) {
                bfap->bfa_orig_file_size = bfap->file_size;
                bfap->bfaFlags |= BFA_VIRGIN_SNAP;
            } else {
                /* Not a participant in COWing... */
                bfap->bfa_orig_file_size = ADVFS_ROOT_SNAPSHOT;
            }
            /*
             * If the file is a virgin and BFA_ROOT_SNAPSHOT is set, it is
             * from the parent snapshot and needs to be cleared.
             */
            bfap->bfaFlags &= ~BFA_ROOT_SNAPSHOT;
                        
        } else if ( IS_COW_CANDIDATE( bfap ) ) {
            /* 
             * bfap has it's own metadata.  Get the original file size from
             * the BSR_ATTR structure. 
             */
            bfap->bfa_orig_file_size = bfattrp->bfat_orig_file_size;
        }
    }

    /*
     * Setup extent map.
     */

    sts = x_create_inmem_xtnt_map( bfap, mcp);
    if (sts != EOK) {
        RAISE_EXCEPTION( sts );
    }

    /* bfaNextFob is now set (after the call to x_create_inmem_xtnt_map. So
     * we could not deal with the set_file_size case that we couldn't deal
     * with before.
     */
    if (set_file_size) {
        bfap->file_size = bfap->bfaNextFob * ADVFS_FOB_SZ;
    }



    bfap->bfaFlags |= BFA_MAPPED;

    if (pgRefed) {
        (void) bs_derefpg(pgref, BS_CACHE_IT);
    }

    ADVFS_ACCESS_LOGIT( bfap, "bs_map_bf: returning");


    return EOK;

HANDLE_EXCEPTION:
    ACCESS_STATS( bs_map_bf_error_path );
    if (pgRefed) {
        (void) bs_derefpg(pgref, BS_CACHE_IT);
    }

    return sts;
}

/**********************************************************************
********************  The public routines ****************************
**********************************************************************/




/*
 * bfm_open_ms - bitfile metadata (bitfile) open routine to open
 * reserved bitfiles.
 */

statusT
bfm_open_ms(
            bfAccessT **outbfap,        /* out - access structure pointer */
            domainT* dmnP,              /* in - domain pointer */
            vdIndexT bfDDisk,           /* in - domain disk index */
            bfdBfMetaT bfMIndex         /* in - metadata bitfile index */
            )
{
    bfTagT tag;
    statusT sts;
    struct vnode *nullvp = NULL;

    BS_BFTAG_RSVD_INIT(tag, bfDDisk, bfMIndex);

    /*
     * Open the reserved file as an internal open.  This will not put a
     * reference on the vnode and will prevent hangs later in the code
     * trying to deallocate or recycle the access strucutre.
     */

    sts = bs_access( outbfap, tag, dmnP->bfSetDirp, FtxNilFtxH,
                          BF_OP_INTERNAL, &nullvp );
    /*
     * The BMT mcellList_lk must be in a lower spot in the hierarchy,
     * since the "normal" progression is bfAccessT.mcellList_lk to
     * vdT->mcell_lk. We have to have a seperate lockinfo for the
     * mcellList_lk for the BMT access structure because a regular
     * mcell may indirectly extend the BMT which requires the BMT
     * access mcellList_lk after getting vdT->mcell_lk.
     */
    if (sts == EOK) {
        if (bfMIndex == BFM_BMT  ||
            bfMIndex == BFM_RBMT) {
            
            ADVRWL_MCELL_LIST_DESTROY( &(*outbfap)->mcellList_lk.lock.rw );
            ADVRWL_BMT_MCELL_LIST_INIT( &(*outbfap)->mcellList_lk.lock.rw );

            ADVRWL_XTNTMAP_DESTROY( &(*outbfap)->xtntMap_lk );
            ADVRWL_BMT_XTNTMAP_INIT( &(*outbfap)->xtntMap_lk );
        }
    }
    return(sts);
}


/*
 * BEGIN_IMS
 *
 * bs_access - This routine opens an access structure and its parent
 * snapshots.  This routine is the most general mechansim for opening a file
 * and care must be taken when calling bs_access_one directly.  
 *
 * While access a file, all necessary data structures are located on disk
 * and initialize as appropriate.  If the open is happening as an external
 * open (BF_OP_INTERNAL is not set) then the v_count of the access structure
 * will be incremented before returning.
 *
 * This routine may return a file that was in cache and already initialized,
 * or it may access a file from disk and create a new bfAccessT structure
 * for it.
 *
 * Parameters:
 *  
 *  outbfap - the bfAccess structure of the open file (if EOK)
 *  tag - the tag of the file to be accessed
 *  bfSetp - the fileset of the file to be accessed.
 *  ftxHT - the parent transaction.
 *  options - options.
 *  vp - The vnode of the accessed file (if EOK).  This is just
 *      &bfap->bfVnode.
 * 
 * Return Values:
 * Return status from bs_access_one.
 *
 * Entry/Exit Conditions:
 * 
 *
 * Algorithm:
 *
 *
 * END_IMS
 */
statusT
bs_access(
          bfAccessT**   out_bfap,       /* out - access structure pointer */
          bfTagT        tag,            /* in - tag of bf to access */
          bfSetT*       bfSetp,         /* in - BF-set descriptor pointer */
          ftxHT         parent_ftx,           /* in - ftx handle */
          enum acc_open_flags options,  /* in - options flags */
          struct vnode* *vp             /* in/out - &bfap->bfVnode */ 
          )
{

    statusT     sts = EOK;
    statusT     sts2 = EOK;
    bfAccessT*  bfap = NULL;

    sts = bs_access_one( &bfap,
                        tag,
                        bfSetp,
                        parent_ftx,
                        options );
    if (sts != EOK) {
        /* The access failed, return the error */
        *vp = NULL;
        *out_bfap = NULL;
        return sts;
    }

    if ( !(bfap->bfaFlags & BFA_ROOT_SNAPSHOT) ) {
        /* If a parent snapshot may exists, access it */
        sts = advfs_access_snap_parents( bfap,
                                        bfSetp,
                                        parent_ftx );
        if (sts != EOK) {
            advfs_ev advfs_event;
            /* An error occured accessing the parents.  We may not have
             * access to all the necessary extents or metadata, we must fail
             * the open. Try to deallocate the bfap if no one else is refing
             * it. */

            bzero( &advfs_event, sizeof( advfs_ev ) );
            advfs_event.domain = bfap->dmnP->domainName;
            advfs_event.fileset = bfap->bfSetp->bfSetName;
            advfs_post_kernel_event( EVENT_SNAP_BAD_PARENT_ACCESS, &advfs_event );

            ms_uaprintf( "WARNING: AdvFS cannot open the parent snapshot; %s\n", BSERRMSG(sts) );
            ms_uaprintf( "WARNING: The snapshot cannot be accessed.\n" );

            sts2 = bs_close_one( bfap,
                        MSFS_CLOSE_DEALLOC,
                        parent_ftx );
            MS_SMP_ASSERT( sts2 == EOK );


        }
    }


    *vp = &bfap->bfVnode;
    *out_bfap = bfap;

    return sts;
}


/*
 * BEGIN_IMS
 *
 * bs_access_one -  This function access a single bfAccess structure.  On
 * successful exit, the refCnt of the access structure
 * is up.  bs_access should be called to make sure snapshots are correctly
 * dealt with.
 *
 * Parameters:
 *     outbfap - on successful return, the bfap for tag.
 *     tag - the tag of the file to be opened.
 *     bfSetp - the bfSet that tag is in.  This bfSet should have a valid vfsp
 *     in the fileSetNode field (fsnp). (If BF_OP_INMEM_ONLY, then this is
 *     not necessary).
 *     ftxH - Parent transaction to access file under.
 *     options - BF_OP_INMEM_ONLY, BF_OP_FIND_ON_DDL, BF_OP_INTERNAL
 *     mp - if being opened from a higher level, mp must be set.
 * 
 * Return Values:
 *      ENO_SUCH_TAG - The requested tag does not exist.
 *      EBAD_DOMAIN_POINTER - a bad domain pointer.
 *      Status returned from tagdir_lookup or bs_map_bf.
 *      EOK - Success
 *
 * Entry/Exit Conditions:
 *      On entry, bfSetp->fsnp->vfsp is initialized.
 *      On exit, v_count is one higher than on entrance, if this is not an
 *      BF_OP_INTERNAL open.
 *
 * Algorithm:
 *
 * END_IMS
 */
statusT
bs_access_one(
              bfAccessT **outbfap, /* out - access structure pointer */
              bfTagT tag,          /* in - tag of bf to access */
              bfSetT *bfSetp,      /* in - BF-set descriptor pointer */
              ftxHT ftxH,          /* in - ftx handle */
              enum acc_open_flags options) /* in - options flags */
{
    struct vfs *vfsp;
    bfAccessT *bfap;
    bfMCIdT bfMCId;
    bfTagFlagsT tagFlags;
    statusT sts;
    unLkActionT unlkAction = UNLK_NEITHER;
    lkStatesT bfaccState;
    int another_fs_open = FALSE;
    vdT *ddlVd;
    int delFlag;
    domainT *dmnP;
    uint32_t flags;
    int no_deref = FALSE;
    int32_t first_open = FALSE;
    int32_t need_to_flush = FALSE;

    /*----------------------------------------------------------------------*/

retry_access:
 
    vfsp   = ADVGETVFS(bfSetp);
    dmnP = bfSetp->dmnP;

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

    /* Except for reserved tags, the passed-in tag should be fully 
     * qualified with a valid (non-zero) sequence number before getting 
     * here.  All subsequent hash lookups depend on the tag_seq being 
     * properly set.
     */
    if (BS_BFTAG_REG(tag)) {
        MS_SMP_ASSERT(BS_BFTAG_SEQ(tag) != 0);
    }
    
    /*
     * advfs_lookup_valid_bfap will return with a bfap for the file.  If in
     * ACC_INIT_TRANS then the bfap is requires additional
     * setup since it was not found in cache.  If in any other state, 
     * it was found in the hash table and is ready to go.
     * advfs_lookup_valid_bfap() returns with bfap->bfaLock held.  
     * advfs_lookup_valid_bfap will return with a v_count of 0 if the file
     * is newly initialized.  Regardless, the v_count must be bumped if this
     * is not an internal open.  Also, the bfaFlags field is set with
     * BFA_EXT_OPEN or BFA_INT_OPEN flags appropriately after returning from
     * advfs_lookup_valid_bfap
     */
    bfap = advfs_lookup_valid_bfap(bfSetp, 
                                   tag, 
                                   options,
                                   ftxH
                                  );

    if (bfap == NULL) {
        /* 
         * This situation means the access memory arena was empty and memory
         * pressure exists system wide, and the free list and closed lists
         * were either empty or contained highly contended access
         * structures.  Alternately, we could be pushing acc_ctrl_soft_max
         * and be unable to reference additional bfaps.
         */
        ACCESS_STATS( access_one_couldnt_get_bfap );
        return EHANDLE_OVF;
    } else if ((options & BF_OP_INMEM_ONLY) &&
               (lk_get_state(bfap->stateLk) != ACC_VALID)) {
        ADVFS_ACCESS_LOGIT( bfap, "bs_access_one: returning ENO_SUCH_TAG");
        
        if (options & BF_OP_SNAP_REF) {
            /*
             * We bumped the bfaRefsFromChildSnaps counter in
             * advfs_lookup_valid_bfap.  We need to decrement it before we
             * decrement the refCnt.
             */
            bfap->bfaRefsFromChildSnaps--;
        }
        /* 
         * This else case ensures we only return ACC_VALID bfaps
         * via the BF_OP_INMEM_ONLY option.
         */
        DEC_REFCNT( bfap, DRC_CLOSED_LIST );
        ACCESS_STATS( access_one_inmem_and_invalid );
        return ENO_SUCH_TAG;
    }

    if (bfap->bfaFlags & BFA_OUT_OF_SYNC && 
            !(options && BF_OP_IGNORE_OUT_OF_SYNC) &&
            BS_BFTAG_REG( bfSetp->dirTag ) ) 
    {
        ADVFS_ACCESS_LOGIT( bfap, "bs_access_one: returning E_OUT_OF_SYNC_SNAPSHOT");

        if (options & BF_OP_SNAP_REF) {
            /*
             * We bumped the bfaRefsFromChildSnaps counter in
             * advfs_lookup_valid_bfap.  We need to decrement it before we
             * decrement the refCnt.
             */
            bfap->bfaRefsFromChildSnaps--;
        }
        if ((options & BF_OP_INTERNAL) || 
            ((lk_get_state( bfap->stateLk ) != ACC_VALID) ||
             (bfap->bfVnode.v_count == 0))) {
            /*
             * We only want to call DEC_REFCNT if we put the ref on this
             * bfap.  If the state is ACC_VALID then it was already
             * referenced if it was an external ref.  If it is an internal
             * ref or the first external ref, then advfs_lookup_valid_bfap
             * bumped the refCnt.
             */
            DEC_REFCNT( bfap, DRC_DEALLOC );
        } else {
            /* This was an external ref of a file that is already opened
             * externally, just unlock and return.
             */
            ADVSMP_BFAP_UNLOCK( &bfap->bfaLock );
        }
        ACCESS_STATS( access_one_out_of_sync );
        return E_OUT_OF_SYNC_SNAPSHOT;
    }

    bfaccState = lk_get_state( bfap->stateLk );

    if (bfaccState == ACC_VALID) {

        /*
         * External opens bump the v_count a little lower in this
         * routine.  If this is the first external open, v_count
         * will still be zero and we'll want to do the deref on
         * error out, otherwise, no_deref.  We do this because of
         * the ref we get in advfs_lookup_valid_bfap() only if it's
         * the first external open.
         */
        if (!(options & BF_OP_INTERNAL) && (bfap->bfVnode.v_count > 0)) {
            no_deref = TRUE;
        }

        if (bfap->bfState == BSRA_DELETING) {
            ACCESS_STATS( access_one_valid_and_deleting );
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
        }

        need_to_flush = TRUE;

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

    } else if ( (bfaccState == ACC_INIT_TRANS) ||
                (bfaccState == ACC_CREATING) ) {

        /*
         * The bfAccess struct was recycled or is just being created.  
         * It has a vnode, but needs additional initialization that wasn't done in
         * advfs_lookup_valid_bfap.  For the creating state, we just need to 
         * read the info off disk.
         */

        /*
         * The ACC_INIT_TRANS state will prevent others from access this
         * file while we initialize it.
         */

        ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);


        /*
         * Pseudo-tag does not exist on disk.
         */
        if ( BS_BFTAG_PSEUDO(tag)) {
            bfap->primMCId = bsNilMCId;

            bfap->bfPageSz       = ADVFS_METADATA_PGSZ_IN_FOBS;
            bfap->reqServices    = 0;
            bfap->bfaNextFob     = 0;
            bfap->file_size      = 0;
            bfap->rsvd_file_size = 0;
            bfap->bfState        = BSRA_VALID;
            bfap->dataSafety     = BFD_METADATA;

            MS_SMP_ASSERT( bfaccState == ACC_INIT_TRANS ); 
        }
        else {

            /* Get tag's mcell info. (potentially blocks for I/O) */

            sts = tagdir_lookup_full( bfSetp,
                                      &tag,
                                      &bfMCId,
                                      &tagFlags);

            /* 
             * During CFS failover, check the DDL for unlinked files.
             *
             * NOTE:  If the file is on the deferred delete list, then its
             *        tag is not on the free list and all of its tmFlags are
             *        intact (except BS_TD_IN_USE).  tagdir_lookup_full()
             *        will still return tagFlags even if it is also
             *        returning ENO_SUCH_TAG.  If find_del_entry() finds the
             *        file on the deferred delete list, then we will need
             *        tagFlags when we call bs_map_bf() to fill in the
             *        correct bfap->dataSafety values.
             */
            if (sts == ENO_SUCH_TAG) {
                ACCESS_STATS( access_one_tagdir_lookup_no_tag );
                if ( (clu_is_ready() && (bfSetp->bfSetFlags & BFS_IM_FAILOVER))
		     || options & BF_OP_FIND_ON_DDL )
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
                    /*
                     * File is on the deferred delete list.  This means that
                     * tagFlags returned from tagdir_lookup_full() has the
                     * needed tmFlags (like the dataSafety values).
                     */
                    ADVRWL_DDLACTIVE_UNLOCK(&(ddlVd->ddlActiveLk));
                } else {
                    goto err_setinvalid;  /* typically "not found" */
                }
                MS_SMP_ASSERT(FETCH_MC_VOLUME(bfMCId) == ddlVd->vdIndex);
            }
                

            if (bfMCId.volume == 0) {
                ACCESS_STATS( access_one_volume_is_zero );
                /* this normally only happens if a user opens .tags/-2 */
                sts = ENO_SUCH_TAG;
                goto err_setinvalid;
            }

            if (sts == E_OUT_OF_SYNC_SNAPSHOT && 
                    ((options & BF_OP_IGNORE_OUT_OF_SYNC) || 
                    BS_BFTAG_RSVD( bfSetp->dirTag))) 
            {
                /* We just mask the out of sync error code and continue
                 * as usual */
                sts = EOK;
            } else if (sts != EOK) {
                ACCESS_STATS( access_one_error_after_tagdir_lookup );
                goto err_setinvalid;
            }


            /*
             * Test for trying to access a clone who's fset is being
             * deleted and who has never been cowed.  If so, no need to 
             * bs_map_bf() it since the fileset is going away.
             */
            if ( (bfSetp->state == BFS_DELETING) &&
                 ( !(options & BF_OP_IGNORE_BFS_DELETING)) ) {

                sts = ENO_SUCH_TAG;
                goto err_setinvalid;
            }

            
            if ( (bfap->bfState == BSRA_INVALID) ) {
                /*
                 * Only reload the bfap from disk if we are in
                 * ACC_INIT_TRANS (because we are initializing this access
                 * structure for initial use) or are in ACC_CREATING & have
                 * a bfState of BSRA_INVALID.  The latter condition means we
                 * haven't already initialized the structure off disk.
                 */
                /*
                 * No need to acquire the move metadata lock to initialize "primMCId"
                 * because no other thread can have a handle to this bitfile.
                 */
                bfap->primMCId    = bfMCId;
                bfap->bfPageSz    = 0;
                bfap->reqServices = 0;
                bfap->bfaNextFob  = 0;
                /* reset the tag info in the bfap, as the passed in tag
                 * could have incorrect tag_seq information.
                 */
                bfap->tag         = tag;

                /* advfs_lookup_valid_bfap() put the tag into the bfNode,
                 * so that needs to be updated as well.
                 */
                bfap->bfBnp.tag   = tag;

                flags = BS_MAP_DEF;

		if (bfSetp->bfSetFlags & BFS_IM_FAILOVER) {
                    flags |= BS_FAILOVER;
                }

                if (options & BF_OP_FIND_ON_DDL) {
                    flags |= BS_ON_DDL;
                }
                if ((sts = bs_map_bf(bfap, flags, tagFlags)) != EOK) {
                    ACCESS_STATS( access_one_mapping_error );
                    goto err_setinvalid;
                }

                MS_SMP_ASSERT( bfap->bfaFlags & BFA_MAPPED );
            }

            if ( test_xtnts_flg ) {
                test_xtnt(bfap);
            }
        }

        ADVSMP_BFAP_LOCK(&bfap->bfaLock);

        /*
         * At this point, the bfAccess structure has been successfully
         * initialized.  After this it will move to some state other
         * than ACC_INIT_TRANS or ACC_CREATING.
         */
    } 

    /*
     * At this point actions related to the access structure state are
     * done.  We must examine the bitfile state to figure out
     * what to do next.
     */

    if ((bfap->bfState == BSRA_VALID ) || (bfap->bfState == BSRA_DELETING) ) {
        if ( bfaccState == ACC_INIT_TRANS ) {
            ACCESS_STATS( access_one_valid_or_deleting_and_init_trans );
            /*
             * It is currently ACC_INIT_TRANS which means we've
             * init'd it and have a vnode.  Set bfaccState to make it
             * ACC_VALID when we drop through.
             * If BSRA_DELETING, we could be in ACC_INIT_TRANS because we are doing
             * a delete completion from recovery code.  This function cannot
             * return with the bfap in ACC_INIT_TRANS so we will set the state
             * to ACC_VALID (which it really will be by the time we return).
             */
            bfaccState = ACC_VALID;
        }
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
            ACCESS_STATS( access_one_creating_log );
            bfaccState = ACC_VALID;
        } else if ((ftxH.hndl != 0) &&
                   (bfap->transitionId == get_ftx_id(ftxH))) {

            /*
             * The caller has the same ftx id as the ftx that put
             * this bitfile into ftx transition, so allow the
             * caller to see the current state of the bitfile.
             */

            /*
             * This must be the BSRA_CREATING state because the other
             * 3 possible states have all been checked for!
             * It could already be in ACC_CREATING as the result
             * of another thread, also blocked on INIT_TRANS, doing this
             * before us, but that doesn't change the fact that it is
             * still in ACC_CREATING until the create ftx changes it.
             */

            if ( bfaccState == ACC_INIT_TRANS ) {
                ACCESS_STATS( access_one_ftx_match_in_init_trans );
                /*
                 * The access structure is in state ACC_INIT_TRANS so we have
                 * already initialized the bfap and the vnode.  We will set
                 * it to ACC_CREATING during the file creation.
                 */
                bfaccState = ACC_CREATING;
            }
            /*
             * Do nothing else and drop through!  This bitfile was
             * created by this ftx and it's okay to see it!
             */
        } else {

            ACCESS_STATS( access_one_ftx_mismatch );
            /*
             * The caller has a different ftx id than the ftx that
             * put this bitfile into ftx transition.  Ideally, we would just
             * block on the state and continue when the state moved to
             * ACC_VALID.  However, ACC_CREATING can go to ACC_INVALID to
             * ACC_DEALLOC and the structure could be deallocated before we
             * get woken up.  As a result, if we encounter ACC_CREATING or
             * ACC_INIT_TRANS, we sleep then goto retry_access to start
             * again.
             *
             * If we were the thread that put this into INIT_TRANS, we
             * must first wake any waiters.  One of the waiters may be
             * the create_rtdn_opx routine that is waiting to change
             * the state to VALID!!
             */

            unlkAction = lk_set_state( &bfap->stateLk, ACC_CREATING);

            lk_signal( unlkAction, &bfap->stateLk );
            unlkAction = UNLK_NEITHER;

            /* 
             * We've notified any waiters that the state is ACC_CREATING.
             * Now we will wait for a state change or a cv broadcast on the
             * lk.
             */
            lk_wait( &bfap->stateLk, &bfap->bfaLock );
            goto retry_access;

        }
    }

    /* 
     * Our only options for stateLk are ACC_INIT_TRANS (we came through the
     * ACC_RECYCLE code path) or ACC_VALID (the bfap came back from
     * advfs_lookup_valid_bfap ACC_VALID or we waited on ACC_CREATING and
     * are now ACC_VALID).
     */

    /*
     * We've got the vnode from advfs_lookup_valid_bfap, but the v_count is
     * not bumped.  Only do this if we are doing an external open.  *
     * The bfap->bfaLock is still held at this point.  
     */
    MS_SMP_ASSERT( ADVSMP_BFAP_OWNED( &bfap->bfaLock ) );

    if ( !(options & BF_OP_INTERNAL) ) {

        /* 
         * On Tru64, we needed to decrement the refCnt here if this was a
         * second external (VFS layer) open.  Now, that is done in
         * advfs_lookup_valid_bfap when bfaFlags BFA_EXT_OPEN is set.  
         * We just need to bump the v_count (one for EVERY external open).
         * We don't have exclusive access to the vnode, so be careful when 
         * bumping the count.  The refCnt on the bfap prevents it 
         * from being deallocated or recycled.
         */
        MS_SMP_ASSERT( bfap->bfaFlags & BFA_EXT_OPEN );
        MS_SMP_ASSERT( bfap->bfBnp.accessp == bfap );
        VN_HOLD(&bfap->bfVnode);

        /*
         * If we are reopening a file that was deleted but still
         * open prior to a cluster failover, set "bfaccState" to 
         * ACC_VALID, the same state that the access structure 
         * would have been in on the other member prior to failover.
         * That way, the code below which compares "bfaccState"
         * to the state in the access structure will reset the
         * state within the access structure to ACC_VALID.
         */
        if (clu_is_ready() && (bfSetp->bfSetFlags & BFS_IM_FAILOVER) && 
            (bfap->bfState == BSRA_DELETING)) {
            bfaccState = ACC_VALID;
        }

    } 

    /* 
     * We should still be in only ACC_INIT_TRANS or ACC_VALID. We could also
     * be in ACC_CREATING if we are reopening an access structure created
     * in a previous subtransaction of the current transaction.
     */
    MS_SMP_ASSERT( (lk_get_state( bfap->stateLk ) == ACC_VALID) ||
                   (lk_get_state( bfap->stateLk ) == ACC_INIT_TRANS) ||
                   (lk_get_state( bfap->stateLk ) == ACC_CREATING) );
    if ( bfaccState != lk_get_state( bfap->stateLk ) ) {
        unlkAction = lk_set_state( &bfap->stateLk, bfaccState );
        lk_signal( unlkAction, &bfap->stateLk );
    }


    if (bfap->refCnt == 1 && (bfap->bfVnode.v_count <= 1)) {
        /* This is the first open */
        first_open = TRUE;
    }

    /* 
     * bs_access_one generally doesn't deal with snapshots, but if this file
     * is in a snapset, set the bfaFlags appropriately before dropping the
     * bfaLock.
     */
    if (HAS_SNAPSHOT_PARENT( bfap ) ) {
        if (!bfSetp->fsnp || bfSetp->fsnp->vfsp->vfs_flag & VFS_RDONLY) {
            bfap->bfaFlags |= BFA_QUICK_CACHE;
        } else if (bfap->bfaFlags & BFA_QUICK_CACHE) {
            /* Previously, the file was accessed and it was either unmounted
             * or the fileset was mounted read-only.  However, now the fileset
             * is mounted and not read only. So we unset the flag. */
            bfap->bfaFlags &= ~BFA_QUICK_CACHE;
        }
    }

    MS_SMP_ASSERT( bfap->dataSafety != BFD_NOT_INITIALIZED );

    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

    if (HAS_SNAPSHOT_CHILD(bfap) && (first_open == TRUE) && 
            (need_to_flush == TRUE)) {
        /* We need to flush if there is a snapshot and this access structure
         * has been recently used (we got it in ACC_VALID).  This ensures that 
         * we don't mark pages RO that are dirty for snapshots.
         */

        sts = fcache_vn_flush( &bfap->bfVnode,
                (off_t) 0,
                (size_t) 0,
                NULL,
                FVF_WRITE | FVF_SYNC
                );
        if (sts) {
            /* No need to set the state to invalid since we got the access
             * structure in ACC_VALID.
             */
            goto err_deref;
        }
    }

    ADVFS_ACCESS_LOGIT( bfap, "bs_access_one returning success" );


    *outbfap = bfap;
    return( EOK );

    /*
     * Error paths are below here.
     */
err_setinvalid:

    ACCESS_STATS( access_one_err_setinvalid );
    /* 
     * On entry to this error path, the bfap is unlocked 
     */
    (void) BS_BFAH_LOCK(bfap->hashlinks.dh_key, NULL);
    ADVSMP_BFAP_LOCK(&bfap->bfaLock);
    unlkAction = lk_set_state( &bfap->stateLk, ACC_INVALID );

    /* 
     * If entry was on a hash list, remove it.
     * This avoids a race with advfs_lookup_valid_bfap waiters who were waiting
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

err_deref:
    ACCESS_STATS( access_one_err_deref );
    /*
     * Do the signal before calling DEC_REFCNT.  DEC_REFCNT releases the
     * lock since it may call advfs_dealloc_access.  We couldn't be sure the
     * structure exists once the lock was dropped.  Note thta DEC_REFCNT is
     * being called with DRC_CLOSED_LIST but may call advfs_dealloc_bfap if
     * the state is ACC_INVALID.
     */
    lk_signal( unlkAction, &bfap->stateLk );

    /*
     * If no_deref, we still need to do the unlock that DEC_REFCNT does.
     */
    if (no_deref) {
        ACCESS_STATS( access_one_no_deref_path );
        MS_SMP_ASSERT(ADVSMP_BFAP_OWNED(&(bfap)->bfaLock));
        ADVSMP_BFAP_UNLOCK( &(bfap)->bfaLock );
    }
    else {
        if (options & BF_OP_SNAP_REF) {
            /*
             * We bumped the bfaRefsFromChildSnaps counter in
             * advfs_lookup_valid_bfap.  We need to decrement it before we
             * decrement the refCnt.
             */
            bfap->bfaRefsFromChildSnaps--;
#ifndef MULTIPLE_SNAPS
            /*
             * This only works for single snapshots.  Otherwise, assert it's
             * greater than 0
             */
            MS_SMP_ASSERT( bfap->bfaRefsFromChildSnaps == 0 );
#else
            MS_SMP_ASSERT( bfap->bfaRefsFromChildSnaps >= 0 );
#endif
        }

        DEC_REFCNT( bfap, DRC_CLOSED_LIST );
    }

    *outbfap = NULL;
    return sts;
}



/**********************************************************************
************** bitfile close routines ********************************
**********************************************************************/

statusT
bs_close(
         bfAccessT *bfap,               /* in */
         enum acc_close_flags options   /* in */
         )
{
    bfSetT *bfSetp;
    statusT sts;

    if (bfap == NULL) {
        return( EINVALID_HANDLE );
    }

    bfSetp = bfap->bfSetp;

    if (HAS_RELATED_SNAPSHOT(bfap)) {
        /*
         * If we are coming from inactive, we don't want to try to close our
         * parents since this could lead to a deadlock on ACC_INIT_TRANS.
         * If the parent is closing the child and the child is closing the
         * parent, both could block on ACC_INIT_TRANS in bs_close_one if we
         * try to close the parents here
         */
        advfs_close_snaps(bfap, 
                (options & MSFS_INACTIVE_CALL) ? SF_SNAP_SKIP_PARENTS : 
                                                 SF_SNAP_NOFLAGS);
    }

    sts = bs_close_one(bfap, options, FtxNilFtxH);
    return(sts);
}

/*
 * bs_close_one
 *
 * Closes the bfAccessT.  This routine expects refCnt is 1 for the last
 * close.  On last close, the file with be put on the closed or free list if
 * possible.  
 *
 * Returns EOK, EINVALID_HANDLE
 */

statusT
bs_close_one (
              bfAccessT *bfap,                  /* in */
              enum acc_close_flags options,     /* in */
              ftxHT parentFtxH                  /* in */
              )
{
    ftxHT ftxH;
    vdT *delVdp;
    lkStatesT prevState;
    statusT sts;
    void *delList;
    uint32_t delCnt = 0;
    bfMCIdT delMCId;
    domainT *dmnP;
    int32_t deleteIt = FALSE;
    int32_t ftxFlag = FALSE;
    struct vnode *vp;
    int32_t mig_stg_lock = FALSE;
    int32_t rmvol_trunc_lock = FALSE;
    char *attrcopy=NULL;
    int err;
    lock_t *sv_lock;
    int64_t totalblks = 0;

    dmnP = bfap->dmnP;
    ADVFS_ACCESS_LOGIT( bfap, "bs_close_one entered" );

    /*
     * Note that the following check is for vfast's frag ratio as in
     * fragmentation, not frag file.  Don't remove this with the frag file!
     */
    if (options & MSFS_INACTIVE_CALL) {
        /* Notify vfast with a FILE_FRAGRATIO_UPDATE that
         * file is being closed by the last non-kernel closer.
         * Update the file's fragmentation stats now.
         */
        if((BS_BFTAG_REG(bfap->tag)) &&
                (bfap->ssMigrating != SS_POSTED) &&
                (bfap->xtnts.bimxAllocFobCnt > 1) &&
                !(options & MSFS_SS_NOCALL) )  {
            (void)ss_chk_fragratio(bfap);
        }
    }
    /*
     * Take the mutex lock below for all the checks below 
     */
    ADVSMP_BFAP_LOCK( &bfap->bfaLock );


    /* We now will allow the file to be fragged or trunced from
     * MSFS_INACTIVE callers even if the ref cnt is > 1. This is
     * because the other bumps of the ref count are internal opens
     * of the orignal from a clone (most likely).  
     *
     * An internal open may also be done by clear_bmt() when
     * processing an rmvol.  In this case, the ADVRWL_RMVOL_TRUNC
     * serialization prior to decrementing bfap->refCnt allows
     * safe truncating prior to the last internal close.
     * See comment in clear_bmt() about access used to serialize
     * with the moving of the bitfile.
     */

    if ((lk_get_state( bfap->stateLk ) != ACC_INIT_TRANS) ||
        (options & MSFS_INACTIVE_CALL)) {
        if ((!((options & MSFS_INACTIVE_CALL) && 
             ((bfap->trunc) || (bfap->bfState == BSRA_DELETING)))
             && ((bfap->refCnt > 1) ||
              (bfap->bfState != BSRA_DELETING))) ||
            ((bfap->bfState == BSRA_DELETING) &&
             (bfap->dmnP->state != BFD_ACTIVATED)))
        {
            /*
             * The only time that we need to execute the code between
             * here and '_close_it' is, 1) if the state is INIT_TRANS so
             * we have wait for it to become available;  2) or, if the
             * refCnt == 1 and we need to delete, truncate the
             * bitfile.  If we need to delete, truncate the bitfile we
             * also need to start an ftx which must be done before blocking on
             * INIT_TRANS (the locking protocol is to start an ftx before
             * blocking on the bf state lock).  None of these conditions
             * are true so go straight to jail (er, to "_close_it") and
             * close the bitfile.
             *
             * On the last external close of a file being deleted we must
             * do the quota updates using the fsContext structure.             
             *
             * We need to distinguish if a file is marked for deletion but
             * we are still running recovery. In this case we do not want to
             * remove the file. The recovery code will do the right thing.
             * Either UNDO the remove or ROOTDONE and complete it. It is
             * not our place to do it here.
             */

            if (options & MSFS_INACTIVE_CALL) {
                lk_signal(lk_set_state(&(bfap->stateLk), 
                          (options & MSFS_INVALID) ? ACC_INVALID : ACC_VALID),
                          &bfap->stateLk);
            }
            goto _close_it;
        }
    }

    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

    if ( (bfap->trunc) &&
         (bfap->bfState != BSRA_DELETING) &&
         (((BFSET_VALID(bfap->bfSetp->bfsFirstChildSnapSet)) &&
         !(options & MSFS_BFSET_DEL)) || 
         ((bfap->refCnt > 1) && (options & MSFS_INACTIVE_CALL))))
    {
        ADVRWL_RMVOL_TRUNC_READ(bfap->dmnP);
        rmvol_trunc_lock = TRUE;
        
    }

    if (FTX_EQ(parentFtxH, FtxNilFtxH) ) {
        /* Prevent races with migrate code paths */

        /* Need to take the bfap lock because of the call chain
         * bf_setup_truncation --> stg_remove_stg_start
         */

        ADVRWL_MIGSTG_READ( bfap );
        mig_stg_lock = TRUE;


        /* No need to take the quota locks. Storage is only added
         * to the quota files at file creation, chown or explicit
         * quota setting. The quota entry already exists in the
         * quota file so the chk_blk_quota will NOT add storage.
         * The potential danger path would be
         * chk_blk_quota --> dqsync --> rbf_add_stg
         * but it is tame in this case.
         */
    }


    sts = FTX_START_N(FTA_BS_CLOSE_V1,&ftxH,parentFtxH,bfap->dmnP);


    if (sts != EOK) {
        ADVFS_SAD1( "ftx start failed", sts );
    }
    /*
     *There is no need to take the migStgLk of anything on a clone.
     */
    ADVSMP_BFAP_LOCK(&bfap->bfaLock);

    /*
     * We must have an refCnt to insure that we can do a
     * lk_wait_while and the access structure won't go away.  
     */
    MS_SMP_ASSERT( bfap->refCnt );
    /*
     * Wait until the access struct is out of transition.
     */
    if (!(options & MSFS_INACTIVE_CALL)){
        lk_wait_while(&bfap->stateLk, &bfap->bfaLock, ACC_INIT_TRANS);
    } else {
        /* For inactive calls the previous state is indicated in the
         * option flags. This can be only invalid or valid. We'll
         * set up the state now.
         */
        if (options&MSFS_INVALID){
            prevState = ACC_INVALID;
        } else {
            prevState = ACC_VALID;
        }            
    }

    if (( bfap->refCnt > 1 ) &&
        (!(bfap->trunc)) &&
        (bfap->bfState != BSRA_DELETING))
    {
        if (options & MSFS_INACTIVE_CALL){
            lk_signal( lk_set_state (&(bfap->stateLk), prevState), &bfap->stateLk );
        }
            
        ftxFlag = TRUE;
        goto _close_it;
    }

    MS_SMP_ASSERT(bfap->refCnt >= 1);

    /*
     * Note that the access count must be tested while holding the
     * mutex.  Now that the refCnt has been determined to be 1,
     * set the access structure into transition, as potentially
     * frags, truncation or deletion will need to block for some
     * reason.
     */
    if (!(options & MSFS_INACTIVE_CALL)){
        prevState = lk_get_state (bfap->stateLk);
        (void) lk_set_state (&(bfap->stateLk), ACC_INIT_TRANS);
    }

    ADVSMP_BFAP_UNLOCK(&bfap->bfaLock);

    if ( test_xtnts_flg ) {
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
             * Do the quota updates now.  If this is the last close the disk
             * storage will be freed immediately.  If there is also an internal
             * open on the file, the storage is freed on last internal close to
             * serialize with a rmvol clear_bmt().  See comment in clear_bmt().
             */

            if ( (VTOC(&bfap->bfVnode)) ) {
                /*
                 * We can just return the quota allocations right away since
                 * deleting this bitfile can't fail.
                 * NOTE: bfap->idxQuotaBlks will be zero except for directories
                 *  that had an index associated with them. We need to account
                 *  for the space consumed by the index being removed.
                 *  chk_blk_quota will kick out the index file itself.
                 */
                totalblks=(int64_t)QUOTABLKS((uint64_t)bfap->xtnts.bimxAllocFobCnt *
                                             ADVFS_FOB_SZ) + bfap->idxQuotaBlks;

                chk_blk_quota(
                    &bfap->bfVnode,
                    - totalblks,
                    TNC_CRED(),
                    0,
                    ftxH );

                chk_bf_quota( &bfap->bfVnode, -1L, TNC_CRED(), 0, ftxH );

                ADVFS_ACCESS_LOGIT(bfap, "bs_close_one adjusted quotas");
                /* Use bfattrp->state as a flag during recovery. */
                /* If it is BSRA_DELETING then we crashed before we could */
                /* set the quotas. If it is BSRA_INVALID we got the quotas */
                /* set but we may not have deleted the storage yet. */
                reset_ondisk_bf_state( bfap->dmnP,
                                       bfap->primMCId,
                                       BSRA_INVALID,
                                       ftxH );
            }
        }


        if (bfap->refCnt == 1) {

            /*
             * if !(deleteIt), we need to allow I/O's on dirty pages
             * to proceed and stats to be written out.  We will also
             * leave the file on the DDL, without removing storage.  This
             * is all necessary because the unlinked open file may still
             * be valid after CFS relocation.
             */

            if (deleteIt) {

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
                 */

                if (BS_IS_META(bfap)) {
                    err=fcache_vn_invalidate(&bfap->bfVnode,
                                             (off_t) 0,
                                             (size_t) 0,
                                             NULL, 
                                             FVI_PPAGE | FVI_INVAL );
                } else {
                    err=fcache_vn_invalidate(&bfap->bfVnode,
                                             (off_t) 0,
                                             (size_t) 0,
                                             NULL, 
                                             FVI_TRUNC );
                }
                MS_SMP_ASSERT(err==0);

                /* 
                 * Defer the stg deallocation until after all locks are 
                 * released.  
                 */

                delMCId = bfap->primMCId;
                delVdp = VD_HTOP((vdIndexT) bfap->primMCId.volume, bfap->dmnP);

                /* 
                 * If we are deleting the file, we don't need to worry 
                 * about flushing the stats to disk. Also we don't want 
                 * this access structure to get on the advfs_closed_acc_list. 
                 */

                if ( VTOC(&bfap->bfVnode) ) {
                    VTOC(&bfap->bfVnode)->dirty_stats = NULL;
                }

            } else {
                err = fcache_vn_flush( &bfap->bfVnode, 
                                        (off_t) 0, 
                                        (size_t) 0, 
                                        NULL, 
                                        FVF_WRITE | FVF_SYNC | FVF_INVAL
                                      );
                MS_SMP_ASSERT(err == 0);

                (void) fs_update_stats(&bfap->bfVnode, bfap, ftxH, 0);
            }

            bfap->bfState = BSRA_INVALID;

        } else {
            /*
             * Wait for last close to remove storage.
             */
            ACCESS_STATS( close_wasnt_last_closer ); 
            deleteIt = FALSE;
        }
    } else {

        if (bfap->trunc) {
            /*
             * This is the last close.  Must do truncate before the refCnt is
             * decremented to 0 because various routines take an
             * access pointer which is only valid if refcnt != 0.
             * The stg dealloc is deferred until all locks are released.
             *
             * Don't bother with this if the file is getting deleted!
             */

            sts = bf_setup_truncation( bfap, ftxH, 0L, &delList, &delCnt );

            bfap->trunc = 0;

        }
    }

    if ( test_xtnts_flg ) {
        test_xtnt(bfap);
    }

    ftx_done_n(ftxH, FTA_BS_CLOSE_V1);

    if (mig_stg_lock) {
        ADVRWL_MIGSTG_UNLOCK( bfap );
        mig_stg_lock = FALSE;
    }

    if (deleteIt) {

        /*  If domain has a lower threshold set */
        if (bfap->dmnP->dmnThreshold.threshold_lower_limit > 0) {

            /* check to see if we're crossing it. */
            sts = advfs_bs_check_threshold(
                                     bfap,
                                     (totalblks * QUOTA_BLKSIZE) / ADVFS_FOB_SZ,
                                     DOMAIN_LOWER,
                                     FtxNilFtxH
                                     );
       /*
        * Ignore failure in non-debug kernel.  We cannot fail the
        * deletion because of failing threshold checking.
        */
        MS_SMP_ASSERT(sts==EOK);

        }

        /*  If fileset has a lower threshold set and it's mounted */
        if ( (bfap->bfSetp->fsetThreshold.threshold_lower_limit > 0) &&
             (BFSET_VALID(bfap->bfSetp)) ) {

            /* check to see if we're crossing it. */
            sts = advfs_bs_check_threshold(
                                     bfap,
                                     (totalblks * QUOTA_BLKSIZE) / ADVFS_FOB_SZ,
                                     FSET_LOWER,
                                     FtxNilFtxH
                                     );
       /*
        * Ignore failure in non-debug kernel.  We cannot fail the
        * deletion because of failing threshold checking.
        */
        MS_SMP_ASSERT(sts==EOK);

        }

        /* delete the file's storage */

        if ((sts = del_dealloc_stg(delMCId, delVdp)) != EOK) {
            domain_panic(dmnP,"bs_close_one: Can't dealloc stg");
        }

        ADVFS_CRASH_RECOVERY_TEST(dmnP, AdvfsCrash_13);

    } else if ( delCnt ) {
        /* truncate the file's storage */

        stg_remove_stg_finish( dmnP, delCnt, delList );

    }

    /* Update vfast frag list when a file has been truncated
     * or deleted.
     */
    if((BS_BFTAG_REG(bfap->tag)) &&
       (bfap->xtnts.bimxAllocFobCnt > 1) &&
       (bfap->ssMigrating != SS_POSTED) &&
      !(options & MSFS_SS_NOCALL) )  {
        (void)ss_chk_fragratio(bfap);
    }

    ADVSMP_BFAP_LOCK(&bfap->bfaLock);
    if (deleteIt) {
        /* 
         * If we are deleting the storage for the file (below) we don't want
         * to keep it's access structure around.  Since we were previously
         * in ACC_INIT_TRANS when we get here, it is safe to move to
         * ACC_INVALID since blockers on ACC_INIT_TRANS would now find
         * ACC_INVALID and not the file. */
        ADVFS_ACCESS_LOGIT( bfap, "bs_close_one setting state to ACC_INVALID" );
        lk_signal( lk_set_state (&(bfap->stateLk), ACC_INVALID), &bfap->stateLk );
    } else {
        /* Otherwise, if we aren't actually deleting the file, just set it's
         * state to whaterver it was before.
         */
        lk_signal( lk_set_state (&(bfap->stateLk), prevState), &bfap->stateLk );
        ADVFS_ACCESS_LOGIT( bfap, "bs_close_one reset state" );
    }

_close_it:

    if (rmvol_trunc_lock) {
        ADVRWL_RMVOL_TRUNC_UNLOCK(bfap->dmnP);
        rmvol_trunc_lock = FALSE;
    }

    if (mig_stg_lock) {
        ADVRWL_MIGSTG_UNLOCK( bfap );
        mig_stg_lock = FALSE;
    }

    if (options & MSFS_SNAP_PARENT_CLOSE) {
        bfap->bfaFlags &= ~BFA_OPENED_BY_PARENT;
    } else if (options & MSFS_SNAP_DEREF) {
        bfap->bfaRefsFromChildSnaps--;
    }

    if (options & MSFS_CLOSE_DEALLOC) {
        MS_SMP_ASSERT(bfap->refCnt == 1);
        ACCESS_STATS( close_used_drc_dealloc );
        DEC_REFCNT( bfap, DRC_DEALLOC );
    } else {
        DEC_REFCNT( bfap, DRC_CLOSED_LIST );
    }

    if ( ftxFlag ) {
        ftx_done_n( ftxH, FTA_BS_CLOSE_V1 );
    }

    return EOK;
}




/*
 * bf_setup_truncation
 *
 * This function truncates the bitfile if it is not a clone and if a truncate
 * was requested.  This function should only be called at the last bitfile
 * close.
 *
 * LOCKS:  The caller usually holds the migStgLk (this lock must
 *         be locked before starting a root transaction).
 *         This requirement is due to the call to
 *         stg_remove_stg_start().  The exception to the rule
 *         in the path in from bs_close_one() where this thread is
 *         the only accessor of the file.  In that case, there is 
 *         no need to lock the migStgLk before calling this function
 *         since no migrate thread can access the file while the
 *         access structure's state is ACC_INIT_TRANS, which is what
 *         it will be when coming in here from bs_close_one().
 *         NB: I added asserts in the storage allocation/deallocation
 *              paths to ensure that the miTruncLk is held, so even though
 *              the lock is not strictly necessary, it IS taken (shared).
 */

statusT
bf_setup_truncation (
                     bfAccessT *bfap,     /* in */
                     ftxHT ftxH,          /* in */
                     int64_t flags,       /* in */
                     void **delList,      /* out */
                     uint32_t *delCnt     /* out */
                     )
{
    statusT sts = EOK;
    bf_fob_t fobs_used = 0;
    bf_fob_t fobs_rsvd = 0;
    bf_fob_t fobs_to_keep = 0;

    *delCnt = 0;

    fobs_used = (bfap->file_size + ADVFS_FOB_SZ - 1L) / ADVFS_FOB_SZ;
    fobs_rsvd = (bfap->rsvd_file_size + ADVFS_FOB_SZ - 1L) / ADVFS_FOB_SZ;

    /*
     * stg_remove_stg_start expects fobs to be aligned on allocation unit
     * boundaries so we need to round fobs_used up to the next
     * allocation unit boundary.  fobs_rsvd should always be aligned on an
     * allocation unit boundary.  If this is a small allocation unit file
     * and the file size is greater than (VM_PAGE_SZ minus one allocation
     * unit) bytes, consider the allocation unit to be VM_PAGE_SZ bytes.
     */
    if (ADVFS_FILE_HAS_SMALL_AU(bfap) && 
        (bfap->file_size > 
         (int64_t)(VM_PAGE_SZ - ((bfap)->bfPageSz * ADVFS_FOB_SZ)))) {
        fobs_used = roundup( fobs_used, (VM_PAGE_SZ / ADVFS_FOB_SZ));
    }
    else {
        fobs_used = roundup( fobs_used, bfap->bfPageSz );
    }
    fobs_rsvd = roundup( fobs_rsvd, bfap->bfPageSz );
    fobs_to_keep = max( fobs_used, fobs_rsvd );

    if ( fobs_to_keep < bfap->bfaNextFob ) {

        /* We should be truncating in full allocation unit for this file */
        MS_SMP_ASSERT((bfap->bfaNextFob - fobs_to_keep) % bfap->bfPageSz == 0);
        sts = stg_remove_stg_start (
                                    bfap,
                                    fobs_to_keep,
                                    bfap->bfaNextFob - fobs_to_keep,
                                    1,          /* do rel quotas */
                                    ftxH,
                                    delCnt,
                                    delList,
                                    TRUE,      /* do COW */
                                               /* force alloc of mcell in */
                                    TRUE,      /* bmt_alloc_mcell */
                                    flags
                                    );

    }

    return sts;
}

static void
test_xtnt(bfAccessT *bfap)
{
    uint32_t i,j;
    int last_fob= 0;

    if ( !(bfap->bfaFlags & BFA_MAPPED) ) {
        return;
    }
    if ( bfap->xtnts.validFlag == XVT_INVALID ) {
        return;
    }
    if ( bfap->xtnts.xtntMap == NULL ) {
        return;
    }

    for ( i = 0; i < bfap->xtnts.xtntMap->validCnt; i++ ) {
        last_fob--;
        /*
         * New subextent could start at the term block from the last subextent.
         */
        for ( j = 0; j < bfap->xtnts.xtntMap->subXtntMap[i].cnt; j++) {
            if ( last_fob < (int)bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j].bsx_fob_offset) {
                last_fob = bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j].bsx_fob_offset;
                continue;
            }
            printf("AdvFS Warning: extent error. Tag %ld, ", bfap->tag.tag_num);
            printf("last %d, current %d, ", last_fob, bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j].bsx_fob_offset);
            printf("bfap 0x%lx\nsubextent %d at 0x%lx, xtnt %d at 0x%lx\n",
                    bfap, i, &bfap->xtnts.xtntMap->subXtntMap[i],
                    j, &bfap->xtnts.xtntMap->subXtntMap[i].bsXA[j]);
            i = *(int *)(-1);
        }
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
 * dropped here. This is a spin lock because it is held in interrupt
 * context by advfs_bs_io_complete(). So, if the active range list length
 * becomes an issue, then using different locks on the active range may be
 * necessary.
 */
static actRangeT *
search_actRange_list(
    actRangeT    *arp,               /* potential actRange  */
    actRangeHdrT *actListHead,       /* list to check for conflicts */
    actRangeT   **conflictp )        /* return addr of conflicts */
{
    if ( actListHead->arCount == 0 ||    
         arp->arStartFob > actListHead->arBwd->arEndFob ) {
        /* Nothing on queue or actRange goes at the end of the list */
        return( (actRangeT *)actListHead );
    } else {
        /* Check if this overlaps with any existing range */
        actRangeT *listp = actListHead->arFwd;
        while ( listp != (actRangeT *)actListHead ) {
            if ( arp->arStartFob <= listp->arEndFob ) {
                if  (arp->arEndFob >= listp->arStartFob) {
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
 * does not hold this lock, just pass NULL.  It is assumed that if a
 * non-NULL value is passed in for the contextp, the file lock is already
 * held (either for shared or exclusive access).
 *
 * Returns TRUE if this routine had to drop the file lock and sleep while
 * waiting for the active range.  Otherwise, returns FALSE (didn't have
 * to wait for the active range).
 */

int
insert_actRange_onto_list( bfAccessT *bfap,
                           actRangeT *arp,
                           struct fsContext *contextp  )
{
    actRangeT *listp;
    actRangeT *carp = (actRangeT *)NULL;     /* conflicting active range */
    int write_lock_held = 0;

    spin_lock(&bfap->actRangeLock);

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
        bfap->actRangeList.arCount++ ;
        if ( bfap->actRangeList.arCount > bfap->actRangeList.arMaxLen )
            bfap->actRangeList.arMaxLen = bfap->actRangeList.arCount;

        spin_unlock(&bfap->actRangeLock);
        return FALSE;
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
        /* We know the lock is held for either read or write access.
         * See if it is held exclusively.
         */
        if ( ADVRWL_FILECONTEXT_ISWRLOCKED(contextp) )
            write_lock_held = 1;
        ADVRWL_FILECONTEXT_UNLOCK( contextp );
    }

    cv_wait(&arp->arCv, &bfap->actRangeLock, CV_SPIN, CV_NO_RELOCK);

    /* When we wake up, our actRange is on the active range list.  
     * Reseize the file_lock if necessary and return.
     */
    if (contextp ) {
        if (write_lock_held) {
            ADVRWL_FILECONTEXT_WRITE_LOCK( contextp );
        }
        else {
            ADVRWL_FILECONTEXT_READ_LOCK( contextp );
        }
    }
    return TRUE;
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

                 /*
                  * Only one thread ever sleeps on a given range
                  *
                  * This wakeup signal is for the cv_wait() in
                  * insert_actRange_onto_list().
                  */
                 cv_signal(&waitp->arCv, NULL, CV_NULL_LOCK );
                 waitp = nextwaiterp;
            }
        }
    }
    return;
}

/* This routine returns TRUE if there is an active range associated with
 * the file specified that has an active range that includes the specified
 * byte offset or higher.  Otherwise, return FALSE.  Currently this is used 
 * to determine, for an O_APPEND case, whether there is a thread potentially 
 * changing the file size.
 */
int
advfs_bs_actRange_spans( bfAccessT *bfap,
                off_t offset,               /* specified in bytes */
                actRangeT **found)
{
    actRangeT *listp;
    bf_fob_t offset_block = offset / ADVFS_FOB_SZ;

    spin_lock(&bfap->actRangeLock);

    /* Check for empty range */
    if ( bfap->actRangeList.arCount == 0 &&
         bfap->actRangeList.arWaitCount == 0 ) {
        goto _not_found;
    }

    /* Check the active list first */
    listp = bfap->actRangeList.arFwd;
    while ( listp != (actRangeT *)&bfap->actRangeList ) {
        if ( listp->arEndFob >= offset_block &&
             listp->arStartFob <= offset_block ) {
            *found = listp;
            spin_unlock(&bfap->actRangeLock);
            return TRUE;
        }
        listp = listp->arFwd;
    }

    /* Check the waiters as well */
    if ( bfap->actRangeList.arWaitCount == 0 ) {
        goto _not_found;
    } else {
        listp = bfap->actRangeList.arWaitFwd;
        while ( listp != (actRangeT *)&bfap->actRangeList ) {
            if ( listp->arEndFob >= offset_block &&
                 listp->arStartFob <= offset_block ) {
                *found = listp;
                spin_unlock(&bfap->actRangeLock);
                return TRUE;
            }
            listp = listp->arWaitFwd;
        }
    }

_not_found:
    spin_unlock(&bfap->actRangeLock);
    *found = NULL;
    return FALSE;
}


/* _assert_actRange_held() is invoked by the MACRO ASSERT_ACTRANGE_HELD()
 *
 * This assert is called to verify that UFC managed pages in a Direct I/O
 * file are properly protected by an active range.  Migrate (rmvol, defrag,
 * balance, etc...), snapshot, and truncation are examples of code that
 * might populate the UFC when a file is opened for Direct I/O, and they are
 * also responsible for invalidating all the pages they load before dropping
 * the active range, otherwise file corruption can occur.
 */
#ifdef ADVFS_SMP_ASSERT
void
_assert_actRange_held( bfAccessT *bfap,  /* IN - bfap for DIO file */
                      off_t       off,   /* IN - offset to check in actRange */
                      size_t      size ) /* IN - length to check in actRange */
{
    struct actRange  ufc_ar;
    actRangeT       *insertion_point;
    actRangeT       *existing_arp       = NULL;


    if ( !AdvfsEnableAsserts ) {
        return; /* if AdvFS asserts are disabled, get out now */
    }

    if ( bfap->dioCnt == 0 ) {
        return; /* caller should have checked this for us, but what the heck */
    }

    if ( bfap->dataSafety != BFD_USERDATA ) {
        return; /* Direct I/O is for user data, not metadata */
    }

    bzero( &ufc_ar, sizeof(ufc_ar) );
    ufc_ar.arStartFob = ADVFS_OFF_TO_FIRST_FOB_IN_VDBLK(off);
    ufc_ar.arEndFob   = ADVFS_OFF_TO_LAST_FOB_IN_VDBLK(off,size);
    MS_SMP_ASSERT( size > 0 );

    spin_lock(&bfap->actRangeLock);

    insertion_point = search_actRange_list( &ufc_ar, 
                                            &bfap->actRangeList,
                                            &existing_arp );

    /* If an active range exists, we should not be given an insertion point */
    MS_SMP_ASSERT( insertion_point          == NULL );

    /* There had better be an existing active range returned */
    MS_SMP_ASSERT( existing_arp             != NULL );

    /* This active range should not be for a Direct I/O request */
    MS_SMP_ASSERT( existing_arp->arState    != AR_DIRECTIO );

    /* Our request should be totally covered by the existing active range */
    MS_SMP_ASSERT( existing_arp->arStartFob <= ufc_ar.arStartFob );
    MS_SMP_ASSERT( existing_arp->arEndFob   >= ufc_ar.arEndFob   );

    spin_unlock(&bfap->actRangeLock);
}
#endif /* ADVFS_SMP_ASSERT */



/*  ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE()
 * _advfs_assert_no_ufc_pages_in_range()
 *
 *     In a debug environment, test that there are no UFC pages in the range
 *     being read or written by Direct I/O.
 *
 *     Having stale cache pages that are being bypassed by Direct I/O can
 *     result in data corruption, so this test ensures that no other code
 *     path has left pages in the UFC.
 *
 *     The call to fcache_vn_info() tells us if there are any UFC pages
 *     associated with the file, but we cannot tell from that if they are
 *     legitimate pages protected by an active range in another path (like
 *     migrate, truncate, snapshot COW work, etc...).  Legitimate pages will
 *     be invalidated before the owning thread drops its active range
 *     protection.  So, if fcache_vn_info() says we have pages in the UFC,
 *     we use fcache_vn_invalidate() to get us into advfs_putpage() with
 *     flags to ASSERT if pages are found in our active range.
 */

#ifdef ADVFS_SMP_ASSERT
void
_advfs_assert_no_ufc_pages_in_range( 
    bfAccessT       *bfap,      /* IN - file's bfap */
    struct actRange *arp )      /* IN - pointer to active range structure */
{
    struct vnode          *vp;
    off_t                  offset;      /* act range offset in bytes */
    size_t                 size;        /* act range size in bytes */
    off_t                  nbpg_offset; /* offset rounded down to NBPG */
    size_t                 nbpg_size;   /* size rounded up by NBPG */
    fcache_vninfo_t        fc_info;
    struct advfs_pvt_param fs_pvt_param;

    if ( bfap->dioCnt == 0 ) {
        return;                 /* This file is not open for direct I/O */
    }
    if ( !AdvfsEnableAsserts ) {
        return;                 /* AdvFS asserts are not enabled */
    }

    vp = ATOV(bfap);
    fc_info.fvi_pages = 0;
    if ((fcache_vn_info(vp, FVINFO_PAGES, &fc_info) == EINVAL) ||
          fc_info.fvi_pages != 0 ) {

        /*
         * We want to verify that the pages that are in the UFC are NOT
         * inside of our active range.
         */
        fs_pvt_param = Nil_advfs_pvt_param;
        fs_pvt_param.app_flags = APP_ASSERT_NO_DIRTY | APP_ASSERT_NO_CLEAN;

        offset = (                    arp->arStartFob) * ADVFS_FOB_SZ;
        size   = (1 + arp->arEndFob - arp->arStartFob) * ADVFS_FOB_SZ;

        /*
         * fcache_vn_invalidate() has done funny things to us if we do not
         * give it NBPG aligned offset and offset+size values
         */
        nbpg_offset = rounddown(offset, NBPG);
        nbpg_size   = roundup(size+(offset-nbpg_offset), NBPG);

        MS_SMP_ASSERT( (nbpg_offset) % NBPG == 0 &&
                       (nbpg_size)   % NBPG == 0 &&
                       (nbpg_size)          != 0 );
        (void)fcache_vn_invalidate( vp, 
                                    nbpg_offset,
                                    nbpg_size,
                                    (uintptr_t)&fs_pvt_param, 
                                    FVI_INVAL | FVI_PPAGE );
    }
}
#endif /* ADVFS_SMP_ASSERT */


#ifdef ADVFS_SMP_ASSERT

/* March thru the entire access structure hashtable and make sure
 * there are no access structs still in the table that are part of
 * the passed in domain ID.
 */
void bs_access_hashtable_dbug_compare(bfAccessT *bfap, 
                                 bfDomainIdT *domainId)
{
    MS_SMP_ASSERT(!BS_UID_EQL(bfap->bfBnp.bfSetId.domainId, *domainId));
}

#endif



/* advfs_bs_actRange_ctor 
 *
 * Memory arena constructor for actRange structures.
 */
int
advfs_bs_actRange_ctor( void *actRangep,
                        size_t actRangeSize,
                        int flags )
{
    actRangeT *arp;

    arp = (actRangeT *)actRangep;
    cv_init( &arp->arCv, "AdvFS ActRange CV", NULL, CV_WAITOK );
    arp->arMagicId = ACTRANGE_MAGIC | MAGIC_DEALLOC;
    return( EOK );
}

/* advfs_bs_actRange_dtor 
 *
 * Memory arena destructor for actRange structures.
 */
void
advfs_bs_actRange_dtor( void *actRangep,
                        size_t actRangeSize,
                        int flags )
{
    actRangeT *arp;

    arp = (actRangeT *)actRangep;
    MS_SMP_ASSERT( arp->arMagicId == (ACTRANGE_MAGIC | MAGIC_DEALLOC) );
    cv_destroy( &arp->arCv );
}

/*
 * advfs_bs_get_actRange
 *
 * Allocates an actRange structure from the memory allocator.
 *
 */
actRangeT *
advfs_bs_get_actRange(int32_t wait) /*In: TRUE/FALSE flag to wait for memory */
{
    actRangeT *arp = NULL;
    extern kmem_handle_t advfs_actRange_arena;

    arp = (actRangeT *)kmem_arena_alloc( advfs_actRange_arena,
                                         (wait) ? M_WAITOK : M_NOWAIT);

    /* Init the structure; the condition variable was inited in the 
     * constructor.
     */
    if ( arp ) {
        MS_SMP_ASSERT( arp->arMagicId == (ACTRANGE_MAGIC | MAGIC_DEALLOC) );
        arp->arFwd      = NULL;
        arp->arBwd      = NULL;
        arp->arWaitFwd  = NULL;
        arp->arWaitBwd  = NULL;
        arp->arStartFob = 0;
        arp->arEndFob   = 0;
        arp->arDebug    = 0;
        arp->arState    = AR_UNINITIALIZED;
        arp->arWaiters  = 0;
        arp->arMagicId  = ACTRANGE_MAGIC;
    }
    return( arp );
}

/* advfs_bs_free_actRange
 *
 * Frees an actRange structure back to the memory allocator.
 * Wait flag indicates whether or not caller allows this routine to sleep.
 *
 */
void
advfs_bs_free_actRange(actRangeT *actRangep, /* in: active range to free */
                       int32_t wait)     /* in: TRUE/FALSE to permit sleeping*/
{
    actRangep->arMagicId |= MAGIC_DEALLOC;
    kmem_arena_free(actRangep, (wait) ? M_WAITOK : M_NOWAIT);
}

/* end bs_access.c */
