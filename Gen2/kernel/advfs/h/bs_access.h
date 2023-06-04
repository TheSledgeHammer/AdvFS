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

#ifndef _ACCESS_H_
#define _ACCESS_H_

#include <advfs/ms_generic_locks.h>
#include <sys/vm_arena_iface.h>
#include <advfs/e_dyn_hash.h>
#include <sys/vnode.h>
#include <advfs/ms_assert.h>
#include <sys/spin.h>


extern void *BsAccessHashTbl;
extern struct vfs advfs_meta_vfs;

void advfs_set_open_min( uint64_t new_open_min );

extern void advfs_set_open_max(uint64_t);

void advfs_access_area_init(void);

/*
 * bfa_flags_t is early in the file for the access stats
 */

/*
 * Defines the open state of an access structure.  This could be merged with
 * bfState and made into a single field in the access structure.
 */
typedef enum {
    BFA_NO_FLAGS        = 0x0,  /* Not open, not mapped */
    BFA_EXT_OPEN        = 0x1,  /* 1 or more external opens */
    BFA_INT_OPEN        = 0x2,  /* 2 or more internal opens */
    BFA_MAPPED          = 0x4,  /* The bfap is inited from disk */
                                /* same as BSRA_VALID essentially */
    BFA_INACTIVATING    = 0x8,  /* Inactive processing */
    BFA_VIRGIN_SNAP     = 0x10, /* This snapshot has not yet been given its 
                                 * own metadata */
    BFA_OUT_OF_SYNC     = 0x20, /* The bfap is a snapshot and is out of sync
                                 * with its parents.  Access is not allowed */
    BFA_CFS_HAS_XTNTS   = 0x40, /* A CFS client has a cached copy of the extents */
    BFA_XTNTS_IN_USE    = 0x80, /* A COW or a migrate is occuring and CFS
                                 * clients may not get extents */
    BFA_IN_COW_MODE     =0x100, /* Indicates that CFS_COW_MODE_ENTER has been
                                 * called on the bfap */
    BFA_SNAP_CHANGE     =0x200, /* Indicates the children snapshots may have
                                 * changed (new or removed children) */
    BFA_QUICK_CACHE     =0x400, /* Indicates bfap is a RO snapshots */
    BFA_PARENT_SNAP_OPEN=0x800, /* The files parents snapshots are open */ 
    BFA_OPENED_BY_PARENT=0x1000,/* The parent snapshot has opened this child */
    BFA_OPENING_PARENTS =0x2000,/* The bfap is actively opening parent snapshots */
    BFA_ROOT_SNAPSHOT   =0x4000,/* The file is the root of it's snapshot tree */
    BFA_NO_SNAP_CHILD   =0x8000,/* This is an optimization to prevent 
                                 * repeated lookups of children */
    BFA_DEL_WITH_CHILDREN 
                        =0x10000,/* This flags indicates the bfap will be
                                  * deleted when its last child snapshot is 
                                  * removed. */
    BFA_ACL_NOT_ONDISK  =0x20000,/* Set if an ACL does no exist on disk */
    BFA_SWITCH_STG      =0x40000,/* A migrate has started and requires the switching 
                                  * of the copy and the original extent
                                  * maps.  The migStgLk may be dropped
                                  * before the storage is swtiched, so any
                                  * modifiers of the storage must switch the
                                  * storage and clear this flag before
                                  * proceeding.
                                  */
    BFA_SWITCH_STG_ERR  =0x80000,/* An error occured while another thread was performing
                                  * a switch_stg operation for a migrate.
                                  * The migrate thread will deal with the
                                  * error and the thread that encountered
                                  * the error will continue.
                                  */
    BFA_MIG_IN_PROGRESS =0x100000,/* A migrate is in progress.  Migrates may perform
                                    * switch storage operations in other
                                    * threads's contexts, so only one can
                                    * occur on a file at a time.
                                    */
    BFA_FRAGMENTED      =0x200000,/* set if file is fragmented */
} bfa_flags_t;


#ifdef _KERNEL



/**************************************
 * Access stats macros
 *************************************/
#ifdef ADVFS_SMP_ASSERT
#define ADVFS_ACCESS_STATS
#endif 

#ifdef ADVFS_ACCESS_STATS
struct advfs_access_stats_struct {
        /* advfs_bs_bfs_flush */
        uint32_t bs_bfs_flush_skip_bfap;
        /* bs_close_one_stats */
        uint32_t close_wasnt_last_closer;
        uint32_t close_used_drc_dealloc;
        /* bs_access_one_stats */
        uint32_t access_one_couldnt_get_bfap;
        uint32_t access_one_inmem_and_invalid;
        uint32_t access_one_out_of_sync;
        uint32_t access_one_valid_and_deleting;
        uint32_t access_one_valid_and_bsra_invalid;
        uint32_t access_one_tagdir_lookup_no_tag;
        uint32_t access_one_volume_is_zero;
        uint32_t access_one_error_after_tagdir_lookup; 
        uint32_t access_one_mapping_error; 
        uint32_t access_one_valid_or_deleting_and_init_trans;
        uint32_t access_one_creating_log;
        uint32_t access_one_ftx_match_in_init_trans;
        uint32_t access_one_ftx_mismatch;
        uint32_t access_one_err_setinvalid;
        uint32_t access_one_err_deref;
        uint32_t access_one_no_deref_path;
        /* bs_map_bf stats */
        uint32_t bs_map_bf_error_no_tag;
        uint32_t bs_map_bf_error_path;
        /* bs_reclaim_cfs_rsvd_vn */
        uint32_t bs_reclaim_cfs_rsvd_vn_called;
        /* bs_invalidate_rsvd_access_struct */
        /* access_invalidate */
        uint32_t access_invalidate_looped;
        /* access_mgmt_thread */
        uint32_t advfs_mgmt_thread_wakeups;
        uint32_t advfs_mgmt_thread_closed_walks;
        uint32_t advfs_mgmt_thread_free_walks;
        uint32_t advfs_mgmt_thread_closed_to_free;
        uint32_t advfs_mgmt_thread_free_to_arena;
        uint32_t advfs_vm_callback_cnt;
        uint32_t advfs_mgmt_thread_early_wakeup;
        uint32_t advfs_mgmt_thread_skipped_free_bfap;
        uint32_t advfs_mgmt_thread_failed_free_ref;
        uint32_t advfs_mgmt_thread_failed_free_lock;
        uint32_t advfs_mgmt_thread_skipped_closed_bfap;
        uint32_t advfs_mgmt_thread_failed_closed_ref;
        uint32_t advfs_mgmt_thread_failed_closed_lock;
        uint32_t advfs_mgmt_thread_acc_invalid_dealloc;
        uint32_t advfs_mgmt_thread_race_calling_close;
        /* advfs_access_arena_callback */
        uint32_t access_arena_callback_ignored;
        /* find_bfap */
        uint32_t find_bfap_restart;
        uint32_t find_bfap_meta_restart;
        uint32_t find_bfap_found_valid_exlc;
        /* advfs_lookup_valid_bfap */
        uint32_t lookup_valid_bfap_bad_state;
        uint32_t lookup_valid_bfap_tag_mismatch;
        uint32_t lookup_valid_bfap_internal_ref_failed;
        uint32_t lookup_valid_bfap_external_ref_failed;
        uint32_t lookup_valid_bfap_inmem_not_found;
        uint32_t lookup_valid_bfap_racer_created_bfap;
        uint32_t lookup_valid_bfap_failed_get_access;
        /* advfs_ref_bfap */
        uint32_t ref_bfap_failed_ref_bad_state;
        uint32_t ref_bfap_too_many_open;
        uint32_t ref_bfap_skipped_list_removal;
        /* advfs_release_access */
        uint32_t advfs_release_dealloc_failed;
        /* advfs_dealloc_access */
        uint32_t dealloc_access_ref_cnt_exists;
        uint32_t dealloc_access_racing_lookup_on_delete;
        uint32_t dealloc_access_shouldnt_happen;
        /* advfs_get_credit */
        uint32_t get_credit_got_credit_locally;
        uint32_t get_credit_stole_credit;
        uint32_t get_credit_forced_credit;
        uint32_t get_credit_bumped_soft_max_for_failover;
        /* advfs_get_new_access */
        uint32_t get_new_access_malloc_wait;
        uint32_t get_new_access_malloc_no_wait;
        uint32_t get_new_access_no_malloc_attempt;
        uint32_t get_new_access_mgmt_signal;
        uint32_t get_new_access_retry_for_suser_if_needed;
        uint32_t get_new_access_open_max_exceeded_no_retry;
        uint32_t get_new_access_no_initial_bfap_malloc;
        uint32_t get_new_access_free_list_recycle;
        uint32_t get_new_access_free_direct_hit;
        uint32_t get_new_access_free_fast_recycle;
        uint32_t get_new_access_closed_list_recycle;
        uint32_t get_new_access_closed_direct_hit;
        uint32_t get_new_access_closed_fast_recycle;
        uint32_t get_new_access_retry_malloc_ie_recycles_failed;
        /* advfs_process_access_list */
        uint32_t process_access_list_max_tries_exceeded;
        uint32_t process_access_list_succeeded;
        uint32_t process_access_list_failed_to_lock;
        uint32_t process_access_list_recycle_race;
        uint32_t process_access_list_search_again;
        uint32_t process_access_list_search_again_2;
        uint32_t process_access_list_flush_error;
        uint32_t process_access_list_refcnt_or_creating;
        /* advfs_metaflush_thread */
        uint32_t metaflush_missed_bf_set_open;
        uint32_t metaflush_missed_bfap_lookup;
        uint32_t metaflush_skipping_bfap;
        /* advfs_bs_dmn_flush_meta */
        uint32_t bs_dmn_flush_meta_buf_bfap_retry;
        /* advfs_bs_bfs_flush */
        uint32_t bfs_flush_skipped_due_to_flush_lock_try;
        /* rbf_add_stg */
        uint32_t rbf_add_stg_metadata_stg_add_during_snap;
        /* migrate_normal */
        uint32_t migrate_normal_migrate_child_metadata;
        uint32_t migrate_normal_migrate_child_metadata_looped;
        uint32_t migrate_normal_start_over_loop;
        uint32_t migrate_normal_collision;
        uint32_t migrate_normal_eagain_condition;
        uint32_t migrate_normal_bfa_switch_stg_stg;
        uint32_t migrate_normal_wait_for_migStgLk;
        uint32_t migrate_normal_another_thread_did_stg_switch;
        uint32_t migrate_normal_another_thread_did_stg_switch_err;
        uint32_t migrate_normal_doing_more_migrating;
        uint32_t migrate_normal_switched_stg_and_erred;
        /* add_extents */
        uint32_t add_extents_returning_eagain;


};
typedef struct advfs_access_stats_struct advfs_access_stats_t;

extern advfs_access_stats_t advfs_access_stats;
#define ACCESS_STATS( __stat_name ) advfs_access_stats.__stat_name++
#else
#define ACCESS_STATS( __stat_name )
#endif


/***************************************
 * START OF ACCESS TRACE
 **************************************/
/*
 * By default, trace access structures in SMP_ASSERT mode.  
 */
#ifdef ADVFS_SMP_ASSERT
#define ADVFS_ACCESS_TRACE
#endif

#ifdef ADVFS_ACCESS_TRACE 

#define ADVFS_PER_ACCESS_TRACE 100

/* These routines are for tracing access structures and vnode counts */
typedef struct {
    struct bfAccess *bfap;
    uint64_t         time_usecs;
    uint64_t         tag;
    uint64_t         seq;
    bfStatesT        bfState;
    lkStatesT        state;
    uint32_t         refCnt;
    int32_t          refs_from_snaps_children;
    uint32_t         v_count;
    uint32_t         v_scount;
    uint32_t         bfaWriteCnt;
    enum vtype       v_type;
    off_t            file_size;
    off_t            orig_file_size;
    bfa_flags_t      bfaFlags;
    bf_fob_t         bfa_next_fob;
    char             caller[40];
    char             file[40];
    int              line;
    struct kthread  *thread;
    void*            idx_params;
    off_t            offset;
    size_t           size;
} bfap_trace_t;

void
advfs_access_logit(struct bfAccess *bfap, char* caller, int line, char *file, off_t offset, size_t size); 
#define ADVFS_ACCESS_LOGIT(bfap, comment) advfs_access_logit(bfap, comment, __LINE__, __FILE__,(off_t)0,(size_t)0)
#define ADVFS_ACCESS_LOGIT_2(bfap, comment, _off, _size) advfs_access_logit(bfap, comment, __LINE__, __FILE__,_off, _size)
#else
#define ADVFS_ACCESS_LOGIT(bfap, comment)
#define ADVFS_ACCESS_LOGIT_2(bfap, comment, _off, _size) 
#endif

/***************************************
 * END OF ACCESS TRACE
 **************************************/
#endif


struct bfSet;

/* Dynamic Hashtable Macros for accessing the BfAccessHashTbl */

#define BS_BFAH_INITIAL_SIZE             128UL
#define BS_BFAH_HASH_CHAIN_LENGTH         30UL
#define BS_BFAH_ELEMENTS_TO_BUCKETS        5
#define BS_BFAH_USECS_BETWEEN_SPLITS 1000000UL

#ifdef Tru64_to_HPUX_Port_Comments
/*
 * The following comment about bits 0-9 should be reexamined and
 * the comment updated for HP-UX.
 */
#endif
/*
 * This MACRO is used to provide input for hash key calculations involving
 * the bitfile-set descriptor (eg.  bsBuf and bfAccess hash tables).  The
 * address of the bitfile-set descriptor is as unique as anything, so the
 * MACRO returns bits 10-31 of the address.  Because kernel memory is
 * malloc'ed in standard sized buckets it turns out that the bfSetT
 * structure is always 1K aligned, therefore bits 0-9 are always the same.
 * This is why they are shifted out before masking the 22 bits desired.
 */
#define BFSET_GET_HASH_INPUT( _bfSetp ) \
    (((uint64_t)(_bfSetp) >> 10) & 0x3fffff)

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
    MS_SMP_ASSERT(ADVSMP_BFAP_OWNED(&_bfap->bfaLock)); \
    dyn_hash_insert( BsAccessHashTbl, _bfap, _laction); \
}

/* actRange structure describes a range of file offset blocks in a file 
 * that are in use for a variety of reasons, including directIO, migration, 
 * and truncation.  This is used as a synchronization mechanism to prevent
 * different threads from manipulating the same range simultaneously.  
 */
typedef struct actRange {
    struct actRange *arFwd;       /* active: pointer to next higher range */
    struct actRange *arBwd;       /* active: pointer to next lower  range */
    struct actRange *arWaitFwd;   /* waiter: pointer to next waiter */
    struct actRange *arWaitBwd;   /* waiter: pointer to previous waiter */
    bf_fob_t  arStartFob;         /* first FOB in this range */
    bf_fob_t  arEndFob;           /* last  FOB in this range */
    uint64_t  arDebug;            /* line and thread id of last modifier */
    enum {
        AR_UNINITIALIZED = 0,
        AR_DIRECTIO,              /* range in use by direct IO */
        AR_MIGRATE,               /* range in use by migration */
        AR_TRUNCATE,              /* range in use by truncation */
        AR_INVALIDATE,            /* range in use by flush_and_invalidate */
        AR_FORCE_COW              /* range in use by 
                                  *  advfs_force_cow_and_unlink */
    } arState;
    int      arWaiters;           /* # thds waiting for this range to go away */
    cv_t     arCv;                /* thread sync. condition variable */
    uint32_t arMagicId;           /* Unique structure validation identifier */
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
    uint32_t   arCount;           /* # structs on active chain */
    uint32_t   arMaxLen;          /* stats: longest active chain has been */
    uint32_t   arWaitCount;       /* # structs on wait chain */
    uint32_t   arWaitMaxLen;      /* stats: longest wait chain has been */
} actRangeHdrT;


/*
 * Move bfNode declaration from ms_osf.h to bs_access.h. When bfNode is
 * inside of bfAccess, the bfNode declaration was happing too late in the
 * include process.  Re-arranging the include order caused other problems.
 * Since bfNode will eventually go away, it really doesn't matter where it
 * is declared.
 */
/*
 * bfNode is the msfs structure at the end of a vnode
 */
typedef struct bfNode {
    struct bfAccess  *accessp;
    struct fsContext *fsContextp;
    bfTagT            tag;
    bfSetIdT          bfSetId;
} bfNodeT;

/* 
 * Types related to access structures 
 */

/*
 * Defines the state of an access structure relative to the free and closed
 * lists 
 */
typedef enum {
    NOT_ON_LIST,
    ON_FREE_LIST,
    ON_CLOSED_LIST
} advfs_list_state_t;


/*
 * Flags to advfs_setup_readahead and advfs_kickoff_readahead.
 */
typedef enum readahead_flags {
    RA_READ             = 0x1, /* Read() operation in progress */
    RA_WRITE            = 0x2, /* Write() operation in progress */
    RA_MMAP_READ_FAULT  = 0x4, /* Read fault for an mmapper in progress */
    RA_MMAP_WRITE_FAULT = 0x8, /* Write fault for an mmapper in progress */
    RA_READ_RA         = 0x10, /* Read-ahead for a read() in progress */
    RA_WRITE_RA        = 0x20, /* Read-ahead for a write() in progress */
    RA_MMAP_READ_RA    = 0x40, /* Read-ahead for an mmapped read in progress */
    RA_MMAP_WRITE_RA   = 0x80, /* Read-ahead for an mmapped write in progress */
    RA_PREFETCH_RA    = 0x100, /* This is a prefetch, not a speculative RA */
    RA_MARK_READ_ONLY = 0x200  /* This is a read-ahead over a snapshot, 
                                * mark the pages as read-only so they fault
                                * for COW processing */
} readahead_flags_t;

/* 
 * Statistics describing how the file has been used.
 */
typedef struct bfaStats {
    uint64_t bfaReads;
    uint64_t bfaWrites;
    uint64_t bfaMmapReadFaults ;
    uint64_t bfaMmapWriteFaults;
    uint64_t bfaReadReadAheads;
    uint64_t bfaWriteReadAheads;
    uint64_t bfaMmapReadReadAheads;
    uint64_t bfaMmapWriteReadAheads;
    uint64_t bfaPrefetchReadAheads;
    uint64_t bfaSnapReadAhead;
} bfaStats_t;


/*
 * bfAccess structure
 */

#ifdef _KERNEL

typedef struct bfAccess {
    /*
     * The first three fields must match exactly with bfAccessHdr
     * The next three fields need to immediately follow them
     * in order to maintain 16 byte alignment for PA spinlocks.
     * TODO: We will discontinue support for the older PA machines
     *       that require this alignment, when that happens
     *       these locks should move back to where they belong.
     */
    dyn_hashlinks_w_keyT hashlinks; /* dynamic hashtable links */
    /* freeFwd/freeBwd are guarded by the advfs_access_free_lock and
     * advfs_access_closed_lock.  freeListState is guarded by the bfaLock
     * but when putting it on or off a list, both the bfaLock and one of the
     * two list locks must be held. */
    struct bfAccess *freeFwd;     /* Circular linked list of bfAccess       */
    struct bfAccess *freeBwd;     /* structures with a zero refCnt that are
                                   * available for reuse by any AdvFS domain
                                   * or fileset.
                                   *
                                   * The freeListState field specifies if
                                   * these links are chained off of the global
                                   * free list (advfs_free_acc_list[]; clean
                                   * data, no flushing needed), the global
                                   * closed list (advfs_closed_acc_list[];
                                   * dirty data may still need to be flushed),
                                   * or no list if the refCnt is non-zero.
                                   */

    /*
     * TODO: These locks are here temporarily for PA spinlock alignment issues
     */
    uint64_t padding1;
    spin_t actRangeLock;            /* protects actRangeList chain */
    spin_t bfa_ss_migrate_lock;	    /* Protects the file's
				     * autotune/vfast/ss data
				     * (ssMigrating) */
    ADVSMP_BFAP_T bfaLock;          /* lock for many of fields in this
                                     * struct */
#   ifdef ADVFS_ACCESS_TRACE
        spin_t       bfa_trace_lock; 
#   endif

/* NOTE
 *
 * we might want to move this structure further down. It's here to put
 * it adjacent to the other spin locks since it starts with the filestats
 * lock.
 */

    struct fsContext bfFsContext; /* This files fsContext area */

    uint32_t accMagic;            /* magic number: structure validation */

                                  /* guard next 2 with bfSetT.accessChainLock */
    struct bfAccess *setFwd;      /* Circular linked list of the fileset's    */
    struct bfAccess *setBwd;      /* bfAccess structures.  This would include
                                   * open files, as well as internally
                                   * referenced or just cached bfAccess
                                   * structures, and there may be structures
                                   * in create/delete transistion.
                                   */
    int32_t refCnt;               /* number of access structure references */
    int32_t dioCnt;               /* # threads having file open for directIO.
                                   * value = 0 means file open for cached I/O.
                                   * value > 0 means file open for directIO.
                                   */
    advfs_list_state_t freeListState;   /* Determines if the access structure 
                                         * is on the closed list, free list,
                                         * or no list. */
    rwlock_t cacheModeLock;       /* guards dioCnt, cached I/O vs DIO mode,
                                   * and waiting for in-flight I/Os when 
                                   * changing cache mode.
                                   */
    stateLkT stateLk;             /* state field */

    struct vnode bfVnode;         /* The vnode for this file.  */

    struct bfNode    bfBnp;       /* This files bfVnode */

    int32_t     bfaWriteCnt;     /* Count of number of active writes in progress.
                                  * This is used for syncrhonization with
                                  * snapset creation.  It is protected by
                                  * the atomic incr/decr macros.  Waiters
                                  * are notified using the bfaLock and
                                  * bfaSnapCv. */

    rwlock_t bfaSnapLock;        /* Protects snapshot related fields and provides 
                                  * synchronization with bfaSnapCv */
    cv_t     bfaSnapCv;          /* Used for synchronizatin, protected by bfaSnapLock */
    struct bfAccess *bfaParentSnap;  
                                 /* Parent snapshot of this bfap. Null if parent not
                                  * open or doesn't exist */
    struct bfAccess *bfaFirstChildSnap;     
                                 /* First child snapshot of this bafp. 
                                  * NULL if not children exist */
    struct bfAccess *bfaNextSiblingSnap;    
                                 /* Next sibling snapshot on the same level.
                                  * NULL if this is the last bfap on the
                                  * level */
    int32_t     bfaRefsFromChildSnaps;  /* Number of refCnts caused by child
                                  * snapshots accessing this parent */
    off_t       bfa_orig_file_size;     /* Highest byte offset to be COWed in a
                                  * a snapshot.  This is always zero if no
                                  * parent exists.  If a parent exists, this
                                  * is the file_size of the parent at the
                                  * time of the snapshot.  When a snapshot
                                  * has its own metadata, this is stored
                                  * in the bsr_snap_rec structure. */

    rwlock_t flush_lock;         /* Used to synchronize between
                                  * flushing and
                                  * recycling/deallocation.*/
    off_t migrate_starting_offset; /* range being migrated */
    off_t migrate_ending_offset;

    bfDataSafetyT dataSafety;   /* bitfile's data safety attribute */

    uint32_t trunc;             /* truncate bitfile on last bitfile close */

    /* following fields only modified in xxx_TRANS states */
    bf_fob_t bfPageSz;          /* logical page/allocation size (in 1k FOBs) */
    serviceClassT reqServices;  /* required service class */
    bfTagT tag;                 /* make a wild guess */
    bfStatesT bfState;          /* bitfile state */
    ftxIdT transitionId;        /* ftx id if CREATING/DELETING */
    /* end of fields only modified in xxx_TRANS states */

    off_t file_size;
    off_t rsvd_file_size;

    /* The bfSetp can not be modified while the strucuture is
     * in the hashtable */
    bfSetT *bfSetp;             /* ptr to bf set desc */

    struct domain* dmnP;        /* ptr of domain desc */

    ftxLkT mcellList_lk;
    ADVRWL_XTNTMAP_T xtntMap_lk;
    ADVRWL_VHAND_XTNTMAP_T vhand_xtntMap_lk;   /* This lock must be
                                * taken by ALL extentmap WRITE lockers
                                * after obtain the xtntMap_lk for
                                * write.  This guards against block
                                * for a reader waiting for memory and
                                * VHAND not being able to get the read
                                * lock.
                                */
    
    bfa_flags_t bfaFlags;       /* Setting and clearing flags in this field
                                 * is protected by the bfaLocks.  HOWEVER,
                                 * the logic associated with the flags is
                                 * protected by locks specific to that flag.
                                 * For instance, the BFA_EXT_OPEN flag is
                                 * protected by the vnode lock and the
                                 * BFA_SNAP* flags are protected by the
                                 * bfaSnapLock, but the bfaLock must be held
                                 * when actually setting and clearing any
                                 * flags. */
    bf_fob_t bfaNextFob;        /* 1 past the highest allocated */
                                /* file offset block */
    bf_fob_t bfaLastWrittenFob; /* Last FOB written by advfs_fs_write(). */

    bfMCIdT primMCId;           /* primary metadata cell id */
    /* Analysis of the code reveals that the following locks
     * seem to be protecting the xtntmap in the following way:
     * 
     * migStgLk - Held across adding stg, removing stg, migrating stg.
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

    void *idx_params;           /* Pointer to index file parameters */
    int32_t  idxQuotaBlks;      /* Additional quota credits at dir removal */

    rwlock_t bfa_acl_lock;	/* Protects the file's ACL (bfa_acl_cache and
				 * bfa_acl_cache_len). Initialized in
				 * advfs_access_constructor */
    struct advfs_acl *bfa_acl_cache;	/* A cached copy of this file's ACL */
    int32_t bfa_acl_cache_len;	/* length of the bfa_acl_cache */

    /* Active Range structures are used to delineate 'active ranges' of
     * pages that are being modified by directIO or migrate.  This is
     * used to synchronize manipulation of a given file region without 
     * preventing other threads from simultaneously manipulating differing 
     * regions.
     */

    /*
     * TODO: this has been temporarily moved to the start of the structure
     */
#ifdef ADVFS_LATER
    spin_t actRangeLock;                /* protects actRangeList chain */
#endif
    struct actRangeHdr actRangeList;    /* chain of actRange structs */
    uint64_t bfap_free_time;            /* System time when put on closed/free 
                                         * list. Also,
                                         * system time when initialized (if
                                         * not on free/closed list */

    /*
     * TODO: this has been temporarily moved to the start of the structure
     */
#ifdef ADVFS_LATER
    spin_t bfa_ss_migrate_lock;		    /* Protects the file's
					     * autotune/vfast/ss data
					     * (ssMigrating) */
#endif
    uint32_t ssMigrating;                    /* vfast is migrating this file */
    uint32_t ssHotCnt[SS_HOT_TRACKING_DAYS]; /* vfast count of IO on hotfiles */

    int32_t badDirEntry;                     /* set when file is a directory */
                                             /* and it has an invalid */
                                             /* directory entry */
/*
 * If running in ADVFS_ACCESS_TRACE mode, we log all access structure in
 * order plus individual access structure operations.
 */
#ifdef ADVFS_ACCESS_TRACE
    /*
     * TODO: this has been temporarily moved to the start of the structure
     */
#ifdef ADVFS_LATER
    spin_t       bfa_trace_lock;
#endif
    bfap_trace_t bfa_trace[ADVFS_PER_ACCESS_TRACE];
    int32_t      bfa_trace_index;
#endif
#ifdef ADVFS_SMP_ASSERT
    bfaStats_t bfa_stats; 
#endif

} bfAccessT;

#endif /* #ifdef _KERNEL */
/*
 * A "small file" is a file with an allocation unit smaller than the VM
 * page size and which has less than a VM page size amount of storage
 * allocated to it and a file size less than or equal to a VM page.
 */
#define ADVFS_SMALL_FILE(bfap) \
        ((((bfap)->bfPageSz * ADVFS_FOB_SZ) < VM_PAGE_SZ) && \
         (((bfap)->bfaNextFob * ADVFS_FOB_SZ) < VM_PAGE_SZ) && \
         ((uint64_t)((bfap)->file_size) < VM_PAGE_SZ))

/*
 * Macro to return TRUE if file has an allocation unit smaller than
 * the VM page size.
 */
#define ADVFS_FILE_HAS_SMALL_AU(bfap) \
        (((bfap)->bfPageSz * ADVFS_FOB_SZ) < VM_PAGE_SZ)

#ifdef ADVFS_TAG_TRACE
    void tag_trace(uint32_t, uint32_t, uint16_t, uint16_t, void*);
#   define TAG_TRACE(f, s, v) \
        tag_trace((uint32_t)(f), (uint32_t)(s), \
            (uint16_t)ADVFS_MODULE, (uint16_t)__LINE__, (void*)(v))
#else /* ADVFS_TAG_TRACE */
#   define TAG_TRACE(f, s, v)
#endif /* ADVFS_TAG_TRACE */


/* Test the validity of a page number in a metadata file. */
/* "Returns" E_BAD_RANGE if the page is beyond the last allocated fob. */
/* Else "returns" EOK. */
#define TEST_META_PAGE(meta_page, bfap) \
    ( (meta_page * bfap->bfPageSz) >= (bfap)->bfaNextFob? \
      E_BAD_RANGE : EOK )

/*
 * This struct saves some space when we
 * need a bfAccess compatible list header.
 * It must match perfectly with the start
 * of bfAccess.
 */

struct bfAccessHdr {
    dyn_hashlinks_w_keyT hashlinks; /* dynamic hashtable links */
    struct bfAccess *freeFwd;
    struct bfAccess *freeBwd;
    int32_t len;
};


/*
 * bs_map_bf() options flags.
 */
#define BS_MAP_DEF     0x0
#define BS_FAILOVER    0x1
#define BS_ON_DDL      0x2

#ifdef _KERNEL

#define NULLMT NULL

statusT
bs_access_one(
    bfAccessT **bfap,       /* out - access structure pointer */
    bfTagT tag,             /* in - tag of bf to access */
    bfSetT *bfSetp,         /* in - BF-set descriptor pointer */
    ftxHT ftxH,             /* in - ftx handle */
    enum acc_open_flags options       /* in - options flags */
    );


statusT
bs_close_one(
    bfAccessT *bfap,                    /* in */
    enum acc_close_flags options,       /* in */
    ftxHT parentFtxH                    /* in */
    );

void
access_invalidate(
    bfSetT *bfSetp
    );

statusT
bf_setup_truncation(
    bfAccessT *bfap,            /* in */
    ftxHT ftxH,                 /* in */
    int64_t flags,              /* in */
    void **delList,             /* out */
    uint32_t *delCnt            /* out */
    );

/*
 * This enum defines the action to be taken by advfs_release_bfap when
 * called by DEC_REFCNT because refCnt goes to 0. These actions only occur
 * if refCnt is 0.
 */
typedef enum {
    DRC_DEALLOC         = 0x1,  /* call advfs_dealloc_access */
    DRC_FREE_LIST       = 0x2,  /* If bfap in ACC_VALID put on free list (else dealloc) */ 
    DRC_CLOSED_LIST     = 0x4   /* If bfap in ACC_VALID put on closed list (else dealloc) */
} drc_action_type_t;

/*
 * BEGIN_IMS
 * DEC_REFCNT - Decrements the refCnt field of a bfAccess structure and
 * conditionally calls advfs_release_access.    The bfaLock must be held
 * when called and will be unconditionally released on return.  An
 * enumerated type is the second paramter and specifies what action to 
 * take if the refCnt goes to 0.  The type should be from
 * dec_refcnt_action_t
 * This macro will also clear the BFA_INT_OPEN flag if this is the last
 * internal close.
 *
 * Parameters:
 *      bfap - bfAccess with bfaLock held to have it's refCnt decremented. 
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      On entry, the bfaLock is held. 
 *      On entry, refCnt >= 1
 *      On exit, bfaLock is dropped and the bfap may or may not have been
 *      freed back to a memory arena.  Do not access the bfap pointer after
 *      this function is called.
 *
 *
 * Algorithm:
 *      Assert that the access structure meets the entry conditions above
 *      Decrement the refCnt.
 *      If refCnt goes to 0, call advfs_release_access to put the bfap on
 *      the free or closed if (if valid) or to free the bfap back to the
 *      advfs_bfaccess_arena (if invalid).
 *      If the refCnt goes to 1 and the BFA_EXT_OPEN flag is set, then this
 *      must be the last internal open's close, so clear the BFA_INT_OPEN
 *      flag.
 *
 * END_IMS
 */
#define DEC_REFCNT( bfap, action )                                      \
{                                                                       \
    MS_SMP_ASSERT(ADVSMP_BFAP_OWNED(&(bfap)->bfaLock));                \
    MS_SMP_ASSERT( bfap->refCnt >= 1 );                                 \
    if ( --bfap->refCnt <= 0 )                                          \
        advfs_release_access( bfap, action );                           \
    else {                                                              \
        /* This is a safe check because BFA_EXT_OPEN was cleared        \
         * in advfs_inactive, the only path to call DEC_REFCNT for      \
         * external opens (called through bs_close)     */              \
        if ( (bfap->refCnt == 1) &&                                     \
             (bfap->bfaFlags & BFA_EXT_OPEN) ) {                        \
            bfap->bfaFlags &= ~BFA_INT_OPEN;                            \
        }                                                               \
        ADVSMP_BFAP_UNLOCK( &(bfap)->bfaLock );                        \
    }                                                                   \
}
/* 
 * This function will lookup and return a
 * valid bfAccessT * for a given tag. The returned access structure will
 * either have been newly initialized or will have been reused out of the
 * access structure cache.
 */
bfAccessT *
advfs_lookup_valid_bfap(
           bfSetT *bfSetp,      /* in - bitfile-set descriptor pointer */
           bfTagT tag,          /* in - bitfile tag */
           enum acc_open_flags options, /* in - options flags */
           ftxHT parent_ftx     /* IN - parent transaction handle */

           );

/*
 * This function is responsbile for putting an unreferenced 
 * access structure on the free or closed list.
 * If the access structure state is ACC_INVALID it will be immediately freed
 * back to the arena it was created from.
 */
void
advfs_release_access (
           bfAccessT* bfap,           /* in - pointer to access struct */
           drc_action_type_t action   /* in - action to take on bfap */
           );

/* 
 * This function is the constructor for the access structure memory arena.
 * It will initialize locks in the access structure so objects coming out of
 * the arena are ready for fast init.
 */
int
advfs_access_constructor(void *addr, size_t size, int flags); 


/*
 * This function is the destructor for the access structure memory arena.
 * It must undo any initialization done in advfs_access_constructor.
 */
void
advfs_access_destructor(void *addr, size_t size, int flags); 



void
bs_invalidate_rsvd_access_struct (
    struct domain *domain,  /* in */
    bfTagT bfTag,           /* in */
    bfAccessT *bfap         /* in */
    );

statusT
bs_reclaim_cfs_rsvd_vn (
    bfAccessT *bfap         /* in */
    );



statusT
bmtr_get_rec_ptr(
    bfAccessT *bfap,        /* in - bf access struct ptr */
    ftxHT parentFtxH,       /* in - transaction handle */
    uint16_t rType,         /* in - type of record */
    uint16_t bSize,         /* in - size of buffer */
    int32_t pinPg,          /* in - 1 == pin pg, 0 == ref pg */
    void **bPtr,            /* out - ptr to buffer */
    rbfPgRefHT *pinPgH,     /* out - pg handle (if pinPg == 1) */
    bfPageRefHT *refPgH     /* out - pg handle (if pinPg == 0) */
    );



int
insert_actRange_onto_list(
    bfAccessT *bfap,
    actRangeT *arp,
    struct fsContext *contextp
    );

void
remove_actRange_from_list(
    bfAccessT *bfap,
    actRangeT *arp
    );

int
advfs_bs_actRange_spans( bfAccessT *bfap,
        off_t offset,               /* specified in bytes */
        actRangeT **found
        );


#ifdef ADVFS_SMP_ASSERT
/* 
 * ASSERT_ACTRANGE_HELD() checks to make sure that any pages being added to
 * the UFC, for a Direct I/O file, are protected by an Active Range.  The
 * Active Range makes sure that a Direct I/O will not attempt to use this
 * range while there are pages from migrate, truncate, snapshot, etc... in
 * the UFC.
 */
#   define ASSERT_ACTRANGE_HELD( _bfap,   _off,   _size )       \
          _assert_actRange_held((_bfap), (_off), (_size))

    void 
    _assert_actRange_held( 
            bfAccessT *bfap, 
            off_t      off, 
            size_t     size );

/*
 * ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE() checks that the active range does
 * not include any pages in the UFC.  This is called when doing a Direct I/O
 * to make sure the UFC does not contain stale pages, or when releasing an
 * Active Range to make sure all UFC pages in the range have been
 * invalidated.
 */
#   define ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE(_bfap, _arp)      \
        _advfs_assert_no_ufc_pages_in_range((_bfap), (_arp))

    void
    _advfs_assert_no_ufc_pages_in_range( 
            bfAccessT       *bfap, 
            struct actRange *arp );

#else  /* ! ADVFS_SMP_ASSERT */
#   define ASSERT_ACTRANGE_HELD( _bfap,   _off,   _size )
#   define ADVFS_ASSERT_NO_UFC_PAGES_IN_RANGE( _bfap, _arp )
#endif /* ADVFS_SMP_ASSERT */

#endif /* #ifdef _KERNEL */


/*
 * Extern definition of the bfaccess arena.
 */
#ifdef _KERNEL
extern kmem_handle_t advfs_bfaccess_arena;
#endif /* _KERNEL */

extern uint64_t advfs_open_max_roundup(uint64_t);

/*
 * This advfs_access_arena_callback is the routine that handles 
 * setting up for a vm request to free bfaps.  
 */
int
advfs_access_arena_callback( int percentage );

/*
 * The advfs_access_list_mgmt_thread handles moving flushing and
 * invalidating data for bfaps on the closed list, and freeing bfaps on the
 * closed list.  It is intended as a timed, self-tuning mechanism for
 * managing the access structure pool.
 */
void
advfs_access_mgmt_thread(void);

/*
 * This function initializes the advfs_access_mgmt_thread.
 */
void
advfs_init_access_mgmt_thread(void);


/*
 * Parameter variables dealing with the advfs_free_acc_list and
 * advfs_closed_acc_list and their management.
 */

/* Time to sleep between runs of the advfs_access_mgmt_thread */
extern struct timespec advfs_acc_mgmt_thread_sleep_time;


typedef struct {
#ifdef _KERNEL
    spin_t      acc_ctrl_lk;            /* Protects any contended fields of 
                                           structure */
#endif /* _KERNEL */
    uint64_t    acc_ctrl_open_cnt;      /* Number of ref'ed bfaps on system */
    uint64_t    acc_ctrl_access_cnt;    /* Number of access structures 
                                           currently out of arena.
                                           This is free list length + 
                                           closed list length +
                                           acc_ctrl_open_cnt */
    uint64_t    acc_ctrl_min;           /* We will do a WAIT malloc until this 
                                           many access structures are ref'ed */
    uint64_t    acc_ctrl_soft_max;      /* This is the maximum number of open 
                                           access structures allowed
                                           at a single point in time.  This 
                                           may be greater than 
                                           configured max (tunable) if CFS 
                                           failovers cause this system to
                                           maintain a higher than expected load or 
                                           is suser opens beyond acc_ctrl_config_max */
    uint64_t    acc_ctrl_config_max;    /* User tunable maximum number of access 
                                           structures. In stable state cluster 
                                           acc_ctrl_soft_max == acc_ctrl_config_max 
                                           until failovers cause soft_max to go 
                                           up.  Soft max will be pulled back 
                                           down over time.  In base system, 
                                           soft_max == config_max. */
} advfs_access_ctrl_t;

/*
 * Various #defines for defaults
 */
#define ADVFS_ACC_MGMT_THREAD_SLEEP_TIME_SECS 30
#define ADVFS_ACC_MGMT_THREAD_SLEEP_TIME_NSECS 0
#define ADVFS_ACC_CTRL_DEFAULT_MIN 100
#define ADVFS_ACC_CTRL_DEFAULT_SOFT_MAX 5000    /* Just a random guess */
#define ADVFS_ACC_CTRL_DEFAULT_CFG_MAX 5000     /* Just a random guess */

/*
 * Time to age an access structure before recycling.  THis is a base time
 * and may be adjusted based on memory constraints.  Is 10 minutes a good
 * time???
 */
#define ADVFS_ACCESS_AGE_TIME_HZ (hz * 60) * 10 

/* 
 * This is the minimum time to age a bfap on the closed list before moving
 * it to the free list.  This time should be bigger than the sync internal
 * so that things on the closed list have a chance to be flushed before we
 * do the move.  Initially it's 30 seconds (or 30 * hz).  Note that the free
 * list age time is not restricted by this value.
 */
#define ADVFS_MIN_CLOSED_AGE_TIME_HZ (hz * 30)

/* Number of bfaps to try to free per free list on each pass */
#define ADVFS_FREE_BFAPS_PER_LIST 3

/* Condition variable to trigger early wakeup of advfs_access_mgmt_thread */
extern cv_t advfs_manage_access_cv;

/* Spinlock to protect the access structure counters */
#ifdef _KERNEL
extern spin_t advfs_num_access_lock;
#endif /* _KERNEL */

/* The advfs free and closed access lists. See the comments in
 * bs_access.c for details.
 */
 
extern ADVSMP_FREE_LIST_T advfs_access_free_lock[];
extern struct bfAccessHdr advfs_free_acc_list[];
extern ADVSMP_CLOSED_LIST_T advfs_access_closed_lock[];
extern struct bfAccessHdr advfs_closed_acc_list[];


#define ADVFS_TIMEVAL_TO_HZ( tv ) tv.tv_sec * hz + (tv.tv_usec * hz / 1000000)
#define ADVFS_TIMESPEC_TO_HZ( tv ) tv.tv_sec * hz + (tv.tv_nsec * hz / 1000000000)



/*
 * BEGIN_IMS
 * ADVFS_ADD_TO_FREE_OR_CLOSED_LIST - This macro puts a bfap on the front of the
 * list passed in (the "new" end).  It is expected that the list being
 * passed in is either the advfs_free_acc_list or advfs_closed_acc_list.  If
 * the list is the advfs_free_acc_list, then the file should have no dirty
 * data but may have clean data.  If the list passed in is the
 * advfs_closed_acc_list, then the file may have clean or dirty data
 * associated with it.
 *  
 * Parameters:
 *      bfap - bfAccess to be put on the free list.
 *      access_list - list to put the bfap on
 *                      {advfs_free_acc_list, advfs_closed_acc_list}
 *      list_lock - lock to acquire when putting the bfap on access_list
 *
 * 
 * Return Values:
 *
 * Entry/Exit Conditions:
 *      On entry, the bfaLock is held. 
 *      On entry, the bfAccess structure is ACC_VALID or ACC_CREATING
 *      On entry, the bfap->freeListState == NOT_ON_LIST.
 *
 *
 * Algorithm:
 *      Assert that the access structure meets the entry conditions above
 *      Lock the access_list.
 *      Set bfap_free_time and put the bfap on the front of the list. 
 *      unlock the advfs_closed_access_lock.
 *
 * END_IMS
 */
#define ADVFS_ADD_TO_FREE_LIST( _bfap, _access_list_p, _list_lock_p)   \
{                                                                       \
    uint64_t myhz_time;							\
    myhz_time = ADVFS_TIMEVAL_TO_HZ(get_system_time());			\
    ADVSMP_ACCESS_LIST_LOCK( (_list_lock_p) );                      	\
    MS_SMP_ASSERT(_bfap->freeListState == NOT_ON_LIST);                 \
    MS_SMP_ASSERT( (lk_get_state( _bfap->stateLk ) == ACC_VALID) ||     \
                   (lk_get_state( _bfap->stateLk ) == ACC_CREATING ) )  \
    MS_SMP_ASSERT((_access_list_p)->freeBwd);                           \
    _bfap->freeBwd = (bfAccessT *)(_access_list_p);                     \
    _bfap->freeFwd = (_access_list_p)->freeFwd;                         \
    (_access_list_p)->freeFwd->freeBwd = _bfap;                         \
    (_access_list_p)->freeFwd = _bfap;                                  \
    _bfap->bfap_free_time = myhz_time; 					\
    _bfap->freeListState = ON_FREE_LIST;                            \
    (_access_list_p)->len++;                                            \
    ADVSMP_ACCESS_LIST_UNLOCK( (_list_lock_p) );                    	\
}



#define ADVFS_ADD_TO_CLOSED_LIST( _bfap, _access_list_p, _list_lock_p)  \
{                                                                       \
    uint64_t myhz_time;							\
    myhz_time = ADVFS_TIMEVAL_TO_HZ(get_system_time());			\
    ADVSMP_ACCESS_LIST_LOCK( (_list_lock_p) );                      	\
    MS_SMP_ASSERT(_bfap->freeListState == NOT_ON_LIST);                 \
    MS_SMP_ASSERT( (lk_get_state( _bfap->stateLk ) == ACC_VALID) ||     \
                   (lk_get_state( _bfap->stateLk ) == ACC_CREATING ) )  \
    MS_SMP_ASSERT((_access_list_p)->freeBwd);                           \
    _bfap->freeBwd = (bfAccessT *)(_access_list_p);                     \
    _bfap->freeFwd = (_access_list_p)->freeFwd;                         \
    (_access_list_p)->freeFwd->freeBwd = _bfap;                         \
    (_access_list_p)->freeFwd = _bfap;                                  \
    _bfap->bfap_free_time = myhz_time; 					\
    _bfap->freeListState = ON_CLOSED_LIST;                              \
    (_access_list_p)->len++;                                            \
    ADVSMP_ACCESS_LIST_UNLOCK( (_list_lock_p) );                    	\
}

/* RM_ACC_LIST_NOLOCK removes an access structure from the free or closed
 * list.  It is assumed that both the appropriate list lock, and the bfaLock
 * for the bfap are held by the caller.  */
#define RM_ACC_LIST_NOLOCK( bfap, _access_list_p, _access_lock_p ) \
{                                                                  \
    MS_SMP_ASSERT( (bfap->freeListState == ON_FREE_LIST &&         \
                    ADVSMP_FREE_LIST_OWNED(_access_lock_p) ) ||    \
                   (bfap->freeListState == ON_CLOSED_LIST &&       \
                    ADVSMP_CLOSED_LIST_OWNED(_access_lock_p)))     \
    MS_SMP_ASSERT(bfap->freeFwd);                                  \
    MS_SMP_ASSERT(bfap->freeBwd);                                  \
    bfap->freeFwd->freeBwd = bfap->freeBwd;                        \
    bfap->freeBwd->freeFwd = bfap->freeFwd;                        \
    bfap->freeFwd = bfap->freeBwd = NULL;                          \
    (_access_list_p)->len--;                                       \
    bfap->freeListState = NOT_ON_LIST;                             \
}



#endif /* _ACCESS_H_ */


