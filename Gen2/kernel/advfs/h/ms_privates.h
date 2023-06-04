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
 *  AdvFS Storage System
 *
 * Abstract:
 *
 *  This header file defines all AdvFS implementation specific types,
 *  structures (on-disk, memory), and function prototypes.
 *
 */
/*
 *  (C) DIGITAL EQUIPMENT CORPORATION 1989, 1990, 1991
 */

#ifndef MS_PRIVATES
#define MS_PRIVATES


/*
 * Some private definitions
 */


#define MAX_VIRT_VM_PAGE_RNG 256

#ifdef _KERNEL

#include <sys/kernel.h>
#include <sys/kthread_iface.h>  /* private: kt_cred() for TNC_CRED() */

#include <advfs/ms_assert.h>
#define ASSERT_NO_LOCKS()     MS_SMP_ASSERT(current_thread()->lock_count == 0)

#endif /* _KERNEL */

#include <advfs/bs_ods.h>
#include <advfs/bs_ims.h>
enum msfs_setproplist_enum { NO_SET_CTIME=0, SET_CTIME = 1 };

/* 
 * kt_cred() is kernel private; same macro defined in cfs_types.h
 */
#define TNC_CRED()              kt_cred(u.u_kthreadp) 
                                       

/*
 * Page size for VM
 */
#define VM_PAGE_SZ (uint64_t)NBPG

#ifdef OSDEBUG 
#   define ADVFS_IFOSDEBUG(s) s
#else
#   define ADVFS_IFOSDEBUG(s)
#endif

/*
 * Internal macro to get the system time
 *
 * Traditionally, we'd get the time by referencing hrestime. Access to
 * hrestime is non-atomic and should be lock protected. This isn't very
 * efficient. get_system_time provides atomic non-locked access to the
 * time, but provides usec instead of nsec. To get around this without
 * having to copy the time several times, we could provide our own macro
 * to decompose the atomic time.
 *
 * Ideally, we'd do it this way to minimize references:
 *
 * #define ADVFS_GET_SYSTEM_TIME(tsp) \
 * do { \
 *         unsigned long val; \
 *        val = tr_time; \
 *        (tsp)->tv_sec  = (uint32_t)((val) >> TR_SUB_SECOND_BITS); \
 *        (tsp)->tv_nsec =           ((val) & TR_SUB_SECOND_MASK) * 1000; \
 * } while(0)
 *
 * But we can't reference tr_time. This could be optimized. The following
 * is the "safe" way to do it.
 */

#define ADVFS_GET_SYSTEM_TIME(ts)          \
do {                                       \
         struct timeval tv;                \
         tv = get_system_time();           \
         ts.tv_sec = tv.tv_sec;            \
         ts.tv_nsec = tv.tv_usec * 1000;   \
} while(0)

/*
 *  These macros are being defined so that we can
 *  turn off code coverage in places where it makes
 *  sense to do so.
 */

#ifdef _BullseyeCoverage
#   define CCOV_OFF
#   define CCOV_ON
#else
#   define CCOV_OFF
#   define CCOV_ON
#endif

/*
 *  The ATOMIC_FETCH_* macros generate lots of branches
 *  according to the code coverage tool.  Since it's not
 *  our job to force each path of these functions to be
 *  executed, we turn off code coverage for them so as to
 *  get a more accurate determination of how much AdvFS code
 *  is being covered.
 */

#define ADVFS_ATOMIC_FETCH_DECR(a,b) {     \
    CCOV_OFF                               \
    ATOMIC_FETCH_DECR(a,b);                \
    CCOV_ON                                \
}

#define ADVFS_ATOMIC_FETCH_INCR(a,b) {     \
    CCOV_OFF                               \
    ATOMIC_FETCH_INCR(a,b);                \
    CCOV_ON                                \
}

#define ADVFS_ATOMIC_FETCH_ADD(a,b,c) {    \
    CCOV_OFF                               \
    ATOMIC_FETCH_ADD(a,b,c);               \
    CCOV_ON                                \
}     

#define ADVFS_ATOMIC_FETCH_BIT_OR(a,b,c) { \
    CCOV_OFF                               \
    ATOMIC_FETCH_BIT_OR(a,b,c);            \
    CCOV_ON                                \
}

extern void advfs_startup(void);
extern void bs_lock_mgr_init(void);
extern void advfs_access_area_init(void);
extern void advfs_init_bsbuf_hashtable(void);
extern void bs_init_rbmt_thread(void);
extern void bs_bfs_init(void);
extern void bs_domain_init(void);
extern void bs_init_freeze_thread(void);
extern void init_tagdir_opx(void);
extern void init_bscreate_opx(void);
extern void init_bs_delete_opx(void);
extern void init_bs_stg_opx(void);
extern void init_bs_xtnts_opx(void);
extern void init_bs_bitmap_opx(void);
extern void advfs_init_snap_ftx(void);
extern void init_idx_index_opx(void);
extern void init_bs_bmt_util_opx(void);
extern void mig_register_migrate_agent(void);
extern void advfs_acl_register_ftx_agents(void);
extern void advfs_prealloc_register_ftx_agent(void);
extern int fs_init_ftx(void);
extern void advfs_init_access_mgmt_thread(void);
extern void advfs_init_handyman_thread(void);
extern void advfs_init_metaflush_thread(void);
extern void quota_init(void);

#ifdef ADVFS_SMP_ASSERT
#define GETPUT_LOGGING
#endif

#ifdef  GETPUT_LOGGING

#define GP_LOG_SIZE 10000

enum gp_caller {
    GETPAGE_CALL        = 0x1,
    PUTPAGE_CALL        = 0x2,
    PAGE_SCAN           = 0x4,
    PAGE_ALLOC          = 0x8,
    PAGE_ALLOC_META     = 0x10,
    FREE_CLEAN          = 0x20,
    UNPIN_NOMOD         = 0x40,
    INVAL_DIRTY         = 0x80,
    FLUSH_INVALFREE_DIRTY       = 0x100,
    BSBUF_FLUSHNEEDED   = 0x200,
    MULTIWAIT_SETUP     = 0x400,
    LOGFLUSH_STARTED    = 0x800,
    MESSAGE_SENT        = 0x1000,
    STARTED_WRITE       = 0x2000,
    STARTED_METADATA_WRITE      = 0x4000,
    INELIGIBLE_METAPAGE = 0x8000,
    ZERO_FILL           = 0x10000,
    UNPROTECT           = 0x20000,
    COW_ADJUSTMENT      = 0x40000,
    COW_UNPROTECT       = 0x80000,
    STARTED_PREVIOUS_METADATA_WRITE = 0x100000,
    IO_COMPLETION_LOG_PAGE = 0x200000,
    IO_COMPLETION_META_PAGE = 0x400000

};

enum gp_action {
    RAW_CALL            = 0x1,
    DEFAULT_ACTION      = 0x2,
    REMOVED_BSBUF       = 0x4,
    DIDNOT_REMOVE_BSBUF = 0x8,
    NOMOD_RACED         = 0x10,
    QUICK_EXIT          = 0x20,
    RARE_EVENT          = 0x40,
    NEED_LOG_FLUSH      = 0x80,
    INFORM_THREAD       = 0x100,
    INELIGIBLE_ENCOUNTERED      = 0x200,
    ADDED_STORAGE       = 0x400,
    BECAME_ELIGIBLE     = 0x800,
    RESCANNING_FOR_META_ALIGNMEMT       = 0x1000,
    RELEASED_RANGE      = 0x2000,
    FP_CREATE_SET       = 0x4000,
    FP_CREATE_NOT_SET   = 0x8000,
    PRIVATE_PARAMS      = 0x10000,
    ENCOUNTERED_WRITEREF                = 0x20000,
    META_LIST_PARSED    = 0x40000,
    READAHEAD           = 0x80000,
    PREFETCH            = 0x100000,
    SNAP_UNPROTECT      = 0x200000,
    SNAP_UNROUND        = 0x400000,
    EXIT_CALL           = 0x800000,
    FAKING_IO           = 0x1000000,
    REMOVING_BSBUF      = 0x2000000,
    IO_FINISHED         = 0x4000000
};
    

typedef enum gp_caller gp_caller_t;
typedef enum gp_action gp_action_t;

typedef struct gp_log_entry {
    uint64_t usecs;
    gp_caller_t caller;
    struct kthread *thread;
    struct vnode *vp;
    bfTagT tag;
    off_t offset;
    size_t size;
    size_t file_size;
    fcache_pflags_t pflags;
    fcache_ftype_t ftype;
    fpa_status_t cache_status;
    bfs_flags_t bf_set_flags;
    int on_LSN_list;
    uint64_t bsb_writeref;
    lsnT bsb_flushseq;
    uint64_t bsb_flags;
    off_t bsb_foffset;
    bsbuf_pfdatbitmap_t bsb_pfdatbitmap;
    gp_action_t action;
        
}gp_log_entryT;

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
    );

#endif

#ifdef GETPUT_LOGGING
#define GPLOGIT(_caller,_vp,_offset,_size,_pflags,_ftype,_cache_status,_bsbufp,_action)  gp_logevent(_caller,_vp,_offset,_size,_pflags,_ftype,_cache_status,_bsbufp,_action)
#else 
#define GPLOGIT(_caller,_vp,_offset,_size,_pflags,_ftype,_cache_status,_bsbufp,_action) 
#endif

#endif /* MS_PRIVATES */
