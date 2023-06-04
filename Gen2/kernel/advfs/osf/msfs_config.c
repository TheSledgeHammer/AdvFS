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
 *      This module contins AdvFS initialization code, DLKM code
 *      and code to deal with tunables.
 *
 */

#include <sys/types.h>
#include <sys/errno.h>
#include <sys/proc_iface.h>
#include <sys/vm_arena_iface.h>
#include <sys/syscall.h>
#include <sys/spinlock.h>
#include <sys/moddefs.h>
#include <sys/ktune.h>
#include <sys/moddefs.h>
#include <sys/mod_conf.h>
#include <sys/mod_vfs.h>

#include <ms_public.h>
#include <ms_generic_locks.h>
#include <ms_privates.h>
#include <advfs_acl.h>
#include <bs_extents.h>
#include <bs_snapshot.h>
#include <bs_freeze.h> 
#include <bs_access.h>
#include <advfs/advfs_conf.h>
#include <advfs/advfs_syscalls.h>
#include <dyn_hash.h>
#include <ms_osf.h>               /* extern ADVRWL_FILESETLIST_T FilesetLock */

/*
 * Some includes for advfs lock logging.
 */

#ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING
/*
 * These includes are for the lock hierarchy debug
 * code.  They must be commented out for submits
 * as they violate the now enforced header include
 * rules.
 *
 * #include <../h/kthread_private.h> 
 * #include <../svc/ksync/ksync_private.h>
 */
#endif /* #ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING */

/*
 *  Prototypes
 */

void advfsc_link();
int  advfs_shutdown();

static int  advfs_create_proc();
static void advfs_check_asserts();
static void advfs_kernel_init();
static void advfs_create_arenas();
static void advfs_destroy_arenas();
static void advfs_init_locks();
static void advfs_init_panics();
#ifdef _BullseyeCoverage
    static void advfs_ccover_init();
#endif

static int advfs_tunable_debug = 0;  /* set only to debug advfs_init_tunables */
void advfs_init_tunables();
void advfs_init_tunables_int();

/*
 * advfs_fstype is used by CFS to test if the passed
 * vfs pointer is for an AdvFS file system by comparing
 * advfs_fstype against vfs.vfs_mtype
 */
int      advfs_fstype = 0;


mutexT          TraceLock;
int             TraceSequence;

/*
 * Stub entry point, used only to force a load of advfs from the
 * sycall stub.
 */
int
advfs_placebo()
{
    return 1;
}

/*
 *  The initial entry point for AdvFS (defined in advfs.modmeta)
 */
void
advfsc_link(void)
{
    printf("AdvFS: initialization starting ...\n");
    advfs_startup();
    printf("AdvFS: initialization complete.\n");
    return;
}

/*
 * advfs_startup 
 *
 * This routine is called from advfsc_link to
 * call bs_init only once with mutual exclusion.
 */

extern struct vfsops  msfs_vfsops;
extern int (*AdvfsSyscallp)(int,void*,int);
extern void advfs_syscall(void*);
extern int advfs_real_syscall(int,void*,int);

static int advfs_registered = 0;

void
advfs_startup()
{
    static initDone = 0;

    if( initDone == 0 ) {
        if( initDone == 0 ) {
            advfs_kernel_init();
            if( !advfs_registered )
            {
               advfs_fstype = add_vfs_type("advfs", &msfs_vfsops);
               printf("AdvFS: File system registered at index %d.\n", 
                        advfs_fstype);
               advfs_registered = 1;
            }
/*            sysent_link_function(SYS_advfs_syscall, advfs_syscall); */
            AdvfsSyscallp = advfs_real_syscall;
            initDone = 1;
        }
    }
}

/*
 * advfs_kernel_init
 *
 * Do all the reset of AdvFS initialization
 */

struct bsMPg   *Dpgp = NULL;
struct bsMPg   *GRpgp = NULL;

extern void advfs_lklg_anal_init();
extern int advfs_lklg_logging_state;

static
void
advfs_kernel_init()
{
    /* only call from here if debugging, otherwise it is called
       early in boot cycle, before breakpoint honored */
    if (advfs_tunable_debug) advfs_init_tunables_int();
    
    (void)advfs_create_proc();
    advfs_create_arenas();
    advfs_init_panics();
    advfs_check_asserts();
    bs_lock_mgr_init();
    advfs_init_locks();

    advfs_access_area_init();

    /*
     * If we're doing lock logging, init and turn on.
     */

#   ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING
        advfs_lklg_anal_init();
        advfs_lklg_logging_state = 1;
#   endif /* #ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING */

    advfs_init_bsbuf_hashtable();
    bs_init_rbmt_thread();
    bs_bfs_init(); 
    bs_domain_init();
    bs_init_freeze_thread();
    init_tagdir_opx();
    init_bscreate_opx();
    init_bs_delete_opx();
    init_bs_stg_opx();
    init_bs_xtnts_opx();
    init_bs_bitmap_opx();
    advfs_init_snap_ftx();
    init_idx_index_opx();
    init_bs_bmt_util_opx();
    mig_register_migrate_agent();
    advfs_acl_register_ftx_agents();
    advfs_prealloc_register_ftx_agent();
    (void)fs_init_ftx();
    ss_kern_init();
    advfs_init_access_mgmt_thread();
    advfs_init_handyman_thread();
    advfs_init_metaflush_thread();
    quota_init();
#   ifdef _BullseyeCoverage 
        advfs_ccover_init();
#   endif

    if (clu_is_ready()) {
        GRpgp = (struct bsMPg *)ms_malloc( sizeof( struct bsMPg ) );
        if( GRpgp == NULL ) {
            ADVFS_SAD0("advfs_kernel_init: out of memory");
        }
    }
    Dpgp = (struct bsMPg *)ms_malloc( sizeof( struct bsMPg ) );
    if( Dpgp == NULL ) {
        ADVFS_SAD0("advfs_kernel_init: out of memory");
    }
#   ifdef ADVFS_SNAP_STATS
        bzero( (void*)&advfs_snap_stats, sizeof( advfs_snap_stats ) );
#   endif
}

/*
 * Initialize things needed for advfs_sad so that we don't overwrite the
 * stack while trying to panic.  
 */
#define ADVFS_SAD_FORMAT0  "%s"
#define ADVFS_SAD_FORMAT1  "%s\n N1 = %ld"
#define ADVFS_SAD_FORMAT2  "%s\n N1 = %d, N2 = %d"
#define ADVFS_SAD_FORMAT3  "%s\n N1 = %d, N2 = %d, N3 = %d"

char *advfs_sad_format0;
char *advfs_sad_format1;
char *advfs_sad_format2;
char *advfs_sad_format3;

char *advfs_sad_buf1;
char *advfs_sad_buf2;

static
void
advfs_init_panics() {

    advfs_sad_buf1 = ms_malloc(256 * sizeof(char));
    advfs_sad_buf2 = ms_malloc(256 * sizeof(char));

    advfs_sad_format0 = (char*)ms_malloc(strlen(ADVFS_SAD_FORMAT0)+1);
    strcpy(advfs_sad_format0, ADVFS_SAD_FORMAT0);

    advfs_sad_format1 = (char*)ms_malloc(strlen(ADVFS_SAD_FORMAT1)+1);
    strcpy(advfs_sad_format1, ADVFS_SAD_FORMAT1);

    advfs_sad_format2 = (char*)ms_malloc(strlen(ADVFS_SAD_FORMAT2)+1);
    strcpy(advfs_sad_format2, ADVFS_SAD_FORMAT2);

    advfs_sad_format3 = (char*)ms_malloc(strlen(ADVFS_SAD_FORMAT3)+1);
    strcpy(advfs_sad_format3, ADVFS_SAD_FORMAT3);

}

/*
 *  Create the AdvFs proc deamon
 */

proc_t *Advfsd = NULL;              /* advfs proc to hold worker threads */
int     Advfsd_started = FALSE;

static
int
advfs_create_proc()
{
    struct kthread *th = NULL;
    pid_t           pid;

    pid = prepare_newproc( &Advfsd,
                           S_DONTCARE,
                           (pid_t) -1,
                           &th,
                           S_DONTCARE,
                           (tid_t) -1,
                           (uid_t) 0,
                           PGID_NOT_SET,
                           SID_NOT_SET) ;
    if (pid == (pid_t) -1) {
        ADVFS_SAD0("Could not create the AdvFS kernel daemon\n");
    }

    switch (newproc(FORK_DAEMON, Advfsd, th)) {

    case FORKRTN_ERROR:
        ADVFS_SAD0("Could not create the AdvFS kernel daemon\n");

    case FORKRTN_PARENT:
        return 0;

    case FORKRTN_CHILD:
        pstat_cmd(u.u_procp, "advfsd", 1, "advfsd");
#       ifdef Tru64_to_HPUX_Port_Comments
        /*
         * -> Need to add KI_ADVFSD
         */
        u.u_syscall = KI_ADVFSD;     /* defined in ki_calls.h */
#       endif /* Tru64_to_HPUX_Port_Comments */
        set_p_ttyp(Advfsd, NULL);    /* no controlling tty */
        set_p_ttyd(Advfsd, NODEV);   /* no controlling dev */
        Advfsd_started = TRUE;
        while (TRUE) {
            sleep((caddr_t)&Advfsd_started, PZERO-1);  /* -1 sleep non-interruptable */
        }
    }
    ADVFS_SAD0("Could not create the AdvFS kernel daemon\n");
}


/*
 *  Create the various memory arenas
 */

kmem_handle_t advfs_misc_arena = NULL;
kmem_handle_t advfs_bfaccess_arena = NULL;
kmem_handle_t advfs_bsbuf_arena = NULL;
kmem_handle_t advfs_ioanchor_arena = NULL;
kmem_handle_t advfs_multiWait_arena = NULL;
kmem_handle_t advfs_actRange_arena = NULL;
kmem_handle_t advfs_aligned_arena = NULL;

static
void
advfs_create_arenas(void)
{
    kmem_arena_attr_t advfs_kattr;

    /*
     *  This is the miscellaneous areana for AdvFS.
     *  Zeroing is left to the allocation caller.
     */
    kmem_arena_attr_init(&advfs_kattr, sizeof(kmem_arena_attr_t));
    advfs_misc_arena = kmem_arena_create(0UL,
                                         "AdvfsMiscArena",
                                         &advfs_kattr,
                                         M_WAITOK);
    MS_SMP_ASSERT(advfs_misc_arena);

    
    
    /* Create an arena for access structures */
    kmem_arena_attr_init(&advfs_kattr, sizeof(kmem_arena_attr_t));
    advfs_kattr.kat_align = SPINLOCK_ALIGN;
    advfs_kattr.kat_ctor = advfs_access_constructor;
    advfs_kattr.kat_dtor = advfs_access_destructor;
    advfs_kattr.kat_mem_notify = advfs_access_arena_callback;
    advfs_bfaccess_arena = kmem_arena_create(sizeof(bfAccessT), 
                                             "ADVFS_BFACCESS_ARENA",
                                             &advfs_kattr,
                                             M_WAITOK);
    MS_SMP_ASSERT(advfs_bfaccess_arena);

    /* Create the bsBuf structure fixed-size memory arena. */
    kmem_arena_attr_init(&advfs_kattr, sizeof(kmem_arena_attr_t));
    advfs_kattr.kat_align = SPINLOCK_ALIGN;
    advfs_kattr.kat_ctor = advfs_bs_bsbuf_ctor;
    advfs_kattr.kat_dtor = advfs_bs_bsbuf_dtor;
    advfs_bsbuf_arena = kmem_arena_create(sizeof(struct bsBuf),
                                          "ADVFS_BSBUF_ARENA",
                                          &advfs_kattr,
                                          M_WAITOK);
    MS_SMP_ASSERT(advfs_bsbuf_arena);

    /* Create the iooanchor_t structure fixed-size memory arena. */
    kmem_arena_attr_init(&advfs_kattr, sizeof(kmem_arena_attr_t));
    advfs_kattr.kat_align = SPINLOCK_ALIGN;
    advfs_kattr.kat_ctor = advfs_bs_ioanchor_ctor;
    advfs_kattr.kat_dtor = advfs_bs_ioanchor_dtor;
    advfs_ioanchor_arena = kmem_arena_create(sizeof(ioanchor_t),
                                             "ADVFS_IOANCHOR_ARENA",
                                             &advfs_kattr,
                                             M_WAITOK);
    MS_SMP_ASSERT(advfs_ioanchor_arena);

    /*
     * Create fixed-size arena for multiWaitT.
     */
    kmem_arena_attr_init(&advfs_kattr, sizeof(kmem_arena_attr_t));
    advfs_kattr.kat_align = SPINLOCK_ALIGN;
    advfs_kattr.kat_ctor = advfs_bs_multiWait_ctor;
    advfs_kattr.kat_dtor = advfs_bs_multiWait_dtor;
    advfs_multiWait_arena = kmem_arena_create(sizeof(multiWaitT),
                                              "AdvfsMultiWaitArena",
                                              &advfs_kattr, 
                                              M_WAITOK);
    MS_SMP_ASSERT(advfs_multiWait_arena);

    /* Create the actRange structure fixed-size memory arena. */
    kmem_arena_attr_init(&advfs_kattr, sizeof(kmem_arena_attr_t));
    advfs_kattr.kat_ctor = advfs_bs_actRange_ctor;
    advfs_kattr.kat_dtor = advfs_bs_actRange_dtor;
    advfs_actRange_arena = kmem_arena_create(sizeof(struct actRange),
                                             "ADVFS_ACTRANGE_ARENA",
                                             &advfs_kattr,
                                             M_WAITOK);
    MS_SMP_ASSERT(advfs_actRange_arena);

    /*
     * Create an arena that provides variable sized aligned allocations.
     * The primary consumer is advfs_dio_unaligned_xfer() so that it can
     * keep the device drivers from malloc'ing aligned memory and doing
     * additional memory-to-memory copies.  'advfs_misc_arena' can not be
     * used because it sticks a header in front of the allocation that ruins
     * the alignment.
     */
    kmem_arena_attr_init(&advfs_kattr, sizeof(kmem_arena_attr_t));
    advfs_kattr.kat_flags = (KAT_ALIGN_ON_SIZE);
    advfs_aligned_arena   = kmem_arena_create(0UL,
                                              "ADVFS_ALIGNED_ARENA",
                                              &advfs_kattr,
                                              M_WAITOK);
    MS_SMP_ASSERT(advfs_aligned_arena);
}

/*
 *  Destroy the various memory arenas
 */

static
void
advfs_destroy_arenas(void)
{
     (void)kmem_arena_destroy(advfs_misc_arena);
     (void)kmem_arena_destroy(advfs_bfaccess_arena);
     (void)kmem_arena_destroy(advfs_bsbuf_arena);
     (void)kmem_arena_destroy(advfs_ioanchor_arena);
     (void)kmem_arena_destroy(advfs_multiWait_arena);
     (void)kmem_arena_destroy(advfs_actRange_arena);
     (void)kmem_arena_destroy(advfs_aligned_arena);

     
}

/*
 *  Early lock initialization
 */

#ifdef ADVFS_FTX_TRACE_LOG
extern spin_t ftx_trace_lock;
#endif

static
void
advfs_init_locks(void)
{

    ADVMTX_TRACE_INIT(&TraceLock);
    ADVMTX_DYNHASHTHREAD_INIT(&DynHashThreadLock);
    ADVRWL_FILESETLIST_INIT(&FilesetLock);

#ifdef ADVFS_FTX_TRACE_LOG
    spin_init( &ftx_trace_lock,
                "advfs_ftx_trace_log lock",
                NULL,
                SPIN_WAITOK,
                ADVFS_LOCK_ORDER_HIGH,
                NULL );
#endif
}

/*
 *  Check various assertions
 */

/* Below is the spinlock minimum cache line size for PA.  Spinlocks must be
 * aligned on this for PA.  Since we don't test PA that often, I am going to 
 * enforce that even on IA the spinlocks we use should be 16-byte aligned
 */
#define PA_SPIN_ALIGN 16

static
void
advfs_check_asserts()
{
    MS_SMP_ASSERT( sizeof(bsTDirPgT) == ADVFS_METADATA_PGSZ );
    MS_SMP_ASSERT( sizeof(bsStgBmT) == ADVFS_METADATA_PGSZ );
#ifdef Tru64_to_HPUX_port_comments
    /*                           -> This assert has been changed from
     *                              == ADVFS_METADATA PGSZ because alignment
     *                              issues caused the bsMPgT structure
     *                              to be < ADVFS_METADATA_PGSZ.  This is 
     *                              acceptable
     *                              for now, but bsMPgT should equal
     *                              ADVFS_METADATA_PGSZ in the future.
     */
    MS_SMP_ASSERT( sizeof(bsMPgT) == ADVFS_METADATA_PGSZ );
#endif
    MS_SMP_ASSERT( sizeof(bsMPgT) <= ADVFS_METADATA_PGSZ );

    /* The following spinlocks are in the middle of our structures and need
     * to be 16-byte aligned on PA.  I am going to enforce it on IA so that 
     * developers will be aware of this issue even if they only test IA.
     * I am not going to worry about spinlocks at the beginning of structures
     * because when the memory arena allocator returns memory it is already
     * 16-byte aligned.  These checks should be removed when the alignment
     * requirement for PA is no longer valid.
     *
    MS_SMP_ASSERT(((int32_t) advfs_offsetof(domainT, lsnLock) & 
                (SPINLOCK_ARCH_ALIGN - 1)) == NULL);
    MS_SMP_ASSERT((advfs_offsetof(domainT, ftxTblLock) & (SPINLOCK_ARCH_ALIGN - 1)) == NULL);
    MS_SMP_ASSERT((advfs_offsetof(bfAccessT, actRangeLock) & 
                (PA_SPIN_ALIGN - 1)) == NULL);
    MS_SMP_ASSERT((advfs_offsetof(logDescT, dirtyBufLock) & 
                (PA_SPIN_ALIGN - 1)) == NULL);
    MS_SMP_ASSERT((advfs_offsetof(struct bsBuf, bsb_lock) & 
                (PA_SPIN_ALIGN - 1)) == NULL);
    */

    /*
     * Verify uid_t hasn't changed from what we're expecting.  If it has
     * changed, then the on-disk and in-memory structures for AdvFS ACLs will
     * be affected.  See advfs/h/advfs_acl.h and advfs/osf/advfs_acl.c
     */
    MS_SMP_ASSERT( sizeof( uid_t ) == sizeof( int32_t ) );

}

#ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING

/*
 * ADVFS lock logging and hierarchy analysis
 */

#define ADVFS_MAX_DEPENDS_PER_LOCK 120
#define ADVFS_MAX_LOGGED_LOCKS     ADVFS_MAX_DEPENDS_PER_LOCK

/*
 * Structure to keep track of owned locks during
 * locking.
 */

static struct rwl_info
    {
        char  *rwli_name;
        char  *rwli_owned[ADVFS_MAX_DEPENDS_PER_LOCK];
    };

/*
 * Structure to pass lock log info back in syscall
 */

static struct rwl_loginfo
    {
	int   rwli_max_depends_per_lock; /* Maximum held locks       */
					 /*  allowed in table        */
	int   rwli_max_loggged_locks;    /* Maximum number of locks  */
	int   rwli_lock_count;           /* Count of logged locks    */
	int   rwli_depends_count;        /* Count of logged lock     */
					 /*  dependencies            */
	int   rwli_string_size;          /* Total size of names of   */
					 /*  logged locks (needed    */
					 /*  to calculate buffer sz) */
	int   rwli_logging_state;        /* Log state (0/1 off/on)   */
	int   rwli_initted;              /* Log info initted? (0/1)  */
	void *rwli_logbuf;               /* Used in caller for ptr   */
    };

/*
 * Allocate the lock infos and any needed local data
 */

static int   advfs_lklg_logging_state   = 0; /* Turn it off initially */
static int   advfs_lklg_logging_initted = 0; /* and data not initted  */
static int   advfs_lklg_lock_initted    = 0; /* and lock not initted  */
static int   advfs_lklg_lock_count;          /* Count of the number   */
					  /* of locks seen         */
static int   advfs_lklg_ref_total;           /* Count of the total    */
					  /* number of references  */
static int   advfs_lklg_string_size;         /* Count of total needed */
					  /* string size           */

static struct rwl_info advfs_lklg_rwl_infos[ADVFS_MAX_LOGGED_LOCKS];

/*
 * Spin lock to protect the lock logging database
 */

#pragma align SPINLOCK_ALIGN
static spin_t   advfs_lklg_log_lock = {0};

/*
 * Function to initialize the logging table
 */

void
advfs_lklg_anal_init()
{
    int i,j;
    struct rwl_info *Info;

    if( advfs_lklg_logging_initted == 1 ) return;

    if( advfs_lklg_lock_initted == 0 )
    {
        spin_init( &advfs_lklg_log_lock,
                   "Advfs lock log lock",
                   NULL,
                   SPIN_WAITOK,
                   ADVSMP_LOCK_LOG_LOCK_ORDER,
                   NULL);
        advfs_lklg_lock_initted = 1;
    }

    advfs_lklg_lock_count  = 0; /* No locks seen yet */
    advfs_lklg_ref_total   = 0; /* No references */
    advfs_lklg_string_size = 0; /* No names */

    for( i=0; i<ADVFS_MAX_LOGGED_LOCKS; i++ )
    {
        Info = &advfs_lklg_rwl_infos[i];
        Info->rwli_name = ((char *)0);
        for( j=0; j<ADVFS_MAX_DEPENDS_PER_LOCK; j++ )
        {
            Info->rwli_owned[j] = ((char *)0);
        }
    }
    advfs_lklg_logging_initted = 1;
    return;
}

/*
 * Add a lock reference to the Info structure for a lock
 */

void
advfs_lklg_lock_log_owned( struct rwl_info *Info, char *OwnedName )
{
    int i;
    if( Info == ((struct rwl_info *)0) ) return;
    if( Info->rwli_name == OwnedName ) return;
    for( i=0; i<ADVFS_MAX_LOGGED_LOCKS; i++ )
    {
        if( Info->rwli_owned[i] == ((char *)0) )
	{
            Info->rwli_owned[i] = OwnedName;
	    advfs_lklg_ref_total++;
        }
        if( Info->rwli_owned[i] == OwnedName ) break;
    }
    if( i >= ADVFS_MAX_LOGGED_LOCKS )
    {
	ms_uaprintf ("advla: Too many locks\n");
	flush_msgbuf();
    }    
    return;
}

/*
 * Find (or initialize) the lock info structure for a
 * particular lock.
 */

struct rwl_info *
advfs_lklg_get_info( char *LockName )
{
    int i;
    struct rwl_info *Info;
    for( i=0; i<ADVFS_MAX_LOGGED_LOCKS; i++ )
    {
        if( advfs_lklg_rwl_infos[i].rwli_name == ((char *)0) )
	{
            advfs_lklg_rwl_infos[i].rwli_name =  LockName;
	    advfs_lklg_lock_count++;
	    advfs_lklg_string_size+=strlen(LockName)+1;
        }
        if( advfs_lklg_rwl_infos[i].rwli_name == LockName )
        {
            return &advfs_lklg_rwl_infos[i];
        }
    }
    if( i >= ADVFS_MAX_LOGGED_LOCKS ) ms_uaprintf ("advla: Too many locks\n");
    return ((struct rwl_info *)0);
}

/*
 * Function to force references for any external locks or locks only
 * ever taken by try.
 */

void
RwLockForceRefs( void )
{
    int i,j;
    struct rwl_info *Info;
    struct rwl_info *Info2;

    for( i=0; i<ADVFS_MAX_LOGGED_LOCKS; i++ )
    {
        Info = &advfs_lklg_rwl_infos[i];
        if( Info->rwli_name != ((char *)0) )
        {
            for( j=0; j<ADVFS_MAX_LOGGED_LOCKS; j++ )
            {
                if( Info->rwli_owned[j] != ((char *)0) )
                    Info2 = advfs_lklg_get_info( Info->rwli_owned[j] );
            }
        }
    }

    return;
}

/*
 * Log all locks owned when a particular lock is taken out,
 * by traversing the rwlock list in the debug structure, and
 * the list of owned semaphores.
 */

void
advfs_lklg_log_owns(char *LockName)
{
    int i;
    struct rwl_info *Info;

    b_sema_t *Sema;
    rw_dbg_t *RwDbg;

    if( advfs_lklg_logging_state == 0 ) return;

    spin_lock( &advfs_lklg_log_lock );

    Info = advfs_lklg_get_info( LockName );

    for( RwDbg = u.u_kthreadp
		 ->kt_ksync_info
		 ->kt_rw_list;
         RwDbg != ((rw_dbg_t *)0);
         RwDbg  = RwDbg->next )
    {
        advfs_lklg_lock_log_owned( Info, RwDbg->addr->rwl_name );
    }

    for( Sema = u.u_kthreadp
		->kt_ksync_info
		->kt_bsema;
         Sema != ((b_sema_t *)0);
         Sema = Sema->next )
    {
        advfs_lklg_lock_log_owned( Info, Sema->name );
    }
    spin_unlock( &advfs_lklg_log_lock );
    return;
}

/*
 * Syscall function to return table
 */
int
advfs_locklog_syscall( int type, void *buf, int buflen )
{
    int i,j,k;
    int sts;
    int allocsize;
    void *inBuffer;
    char *StrPtr,*csrc;
    char *cin,*cout;
    char c;
    struct rwl_info *InfoOut;
    struct rwl_info *InfoIn;
    struct rwl_info *Info1,*Info2;
    struct rwl_loginfo *LogInfo;

    if( type == OP_LOCKLOGSTART )
    {
        advfs_lklg_anal_init();
	advfs_lklg_logging_state = 1;
	return 0;
    }

    if( type == OP_LOCKLOGSTOP )
    {
	advfs_lklg_logging_state = 0;
	advfs_lklg_logging_initted = 0;
        RwLockForceRefs();
        return 0;
    }

    if( type == OP_LOCKLOGINFO )
        allocsize = sizeof( struct rwl_loginfo );
    else
        allocsize = sizeof(advfs_lklg_rwl_infos) + advfs_lklg_string_size;

    if( buflen < allocsize ) return EINVAL;
    inBuffer = ms_malloc(allocsize);

    /*
     * Return the logging information
     */

    if( type == OP_LOCKLOGINFO )
    {
        LogInfo = ((struct rwl_loginfo *)inBuffer );
	LogInfo->rwli_max_depends_per_lock = ADVFS_MAX_DEPENDS_PER_LOCK;
	LogInfo->rwli_max_loggged_locks    = ADVFS_MAX_LOGGED_LOCKS;
	LogInfo->rwli_lock_count           = advfs_lklg_lock_count;
	LogInfo->rwli_depends_count        = advfs_lklg_ref_total;
	LogInfo->rwli_string_size          = advfs_lklg_string_size;
	LogInfo->rwli_logging_state        = advfs_lklg_logging_state;
	LogInfo->rwli_initted              = advfs_lklg_logging_initted;
    }
    else

    /*
     * Return the actual log table.
     */

    {
        InfoOut = ((struct rwl_info *)inBuffer );
	Info1   = InfoOut;

	/*
	 * First copy the table
	 */

        for( i=0; i <ADVFS_MAX_LOGGED_LOCKS; i++ )
	{
	    Info2 = &advfs_lklg_rwl_infos[i];
	    Info1->rwli_name=Info2->rwli_name;
	    for( j=0; j<ADVFS_MAX_DEPENDS_PER_LOCK; j++ )
	        Info1->rwli_owned[j]=Info2->rwli_owned[j];
	    Info1++;
	}

	/*
	 * Now for each lock, copy its name into the buffer,
	 * and then make all references relative to the
	 * buffer so that a user program can find the names.
	 */

	StrPtr  = ((char *)Info1);
        Info1 = ((struct rwl_info *)inBuffer );
	for( i=0; i <ADVFS_MAX_LOGGED_LOCKS; i++ )
	{
	  if( (csrc=Info1->rwli_name) != ((char *)NULL) )
	  {
	    cin=csrc;
	    cout= ((char *)(
		    ((unsigned long)StrPtr)
		    -((unsigned long)inBuffer) ));

            while(1) if( (*StrPtr++ = *csrc++) == ((char)0) ) break;

	    Info1->rwli_name = cout;

	    Info2 = ((struct rwl_info *)inBuffer );
            for( j=0; j<ADVFS_MAX_LOGGED_LOCKS; j++)
	    {
	       for( k=0; k<ADVFS_MAX_DEPENDS_PER_LOCK; k++ )
	           if( Info2->rwli_owned[k] == cin)
	               Info2->rwli_owned[k] = cout;
               Info2++;
            }
          }
          Info1++;
        }
    }

    /*
     * Return and free the result buffer
     */

    sts = ms_copyout (inBuffer, buf, buflen);
    ms_free( inBuffer );
    return sts;
}

/*
 * Lock routine jackets for logging
 */

void
advfs_mutex_lock( b_sema_t *b_sema )
{
    advfs_lklg_log_owns( b_sema->name );
    return mutex_lock( b_sema );
}

void
advfs_rwlock_wrlock( rwlock_t *rwlock )
{
    advfs_lklg_log_owns( rwlock->rwl_name );
    return rwlock_wrlock( rwlock );
}
void
advfs_rwlock_rdlock( rwlock_t *rwlock )
{
    advfs_lklg_log_owns( rwlock->rwl_name );
    return rwlock_rdlock( rwlock );
}
#endif /* #ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING */

/*
 * DLKM Support
 */

/*
 * Declare forward references
 */

static int advfs_load(void *);
static int advfs_unload(void*);

/*
 * Declare references to data in modmeta
 */

extern struct mod_conf_data advfs_conf_data;
extern struct mod_operations mod_misc_ops;

/*
 * Define local DLKM required structures
 */

static struct mod_type_data advfs_mod_type_data =
    {
	"advfs file system",
	NULL
    };

static struct modlink advfs_mod_link[] =
    {
        {&mod_fs_ops, (void *)&advfs_mod_type_data},
	{(struct mod_operations *)NULL, (void *)NULL }
    };

struct modwrapper advfs_wrapper =
    {
	 MODREV,
         advfs_load,
         advfs_unload,
         NULL,
         (void *)&advfs_conf_data,
         advfs_mod_link
    };

/*
 * DLKM Load routine - nothing to do, always return success.
 */

static int
advfs_load( void *type_specific_data )
{
    fsmod_spec_t *advfs_type_specific_data =
          ((fsmod_spec_t *)type_specific_data);
    if( install_vfs( advfs_type_specific_data->fsname,
                     &msfs_vfsops,
                     advfs_type_specific_data->fs_id )
       == -1 ) return ENOSYS;
    advfs_registered = 1;
    advfsc_link();
    return (0);
}

/*
 * DLKM Unload routine - can't unload, always return failure.
 */

static int
advfs_unload(void *arg)
{
    return (1);
}

/*
 * Kernel Tunables
 */

extern advfs_access_ctrl_t advfs_acc_ctrl;
extern uint64_t AdvfsIORetryControl;
extern uint64_t advfs_quot_undrflo_behav;

extern pgcnt_t phys_mem_pages;

static ktune_id_t ktid_advfs_open_max;
uint64_t   ktval_advfs_open_max;

static ktune_id_t ktid_advfs_open_min;
uint64_t   ktval_advfs_open_min;

static ktune_id_t ktid_advfs_io_retries;
uint64_t   ktval_advfs_io_retries;

static ktune_id_t ktid_advfs_freezefs_timeout;
uint64_t   ktval_advfs_freezefs_timeout;

static ktune_id_t ktid_advfs_quot_undrflo_behav;
uint64_t   ktval_advfs_quot_undrflo_behav;

/*
 * advfs_tn_open_max - Tunable handler
 *
 * Tunable:
 *     advfs_open_max
 *
 * Description:
 *     ADVFS configured maximum number of opens
 *
 * Parameters:
 *    IN    eventid        KEN event ID
 *    IN    reason         KEN reason code
 *    IN    instance       KEN event instance handle
 *    IN    handler_data   tunable handler data pointer
 *    IN    event_data     tunable event data pointer
 *    OUT    result         return status of handler
 *
 * Return Values:
 *    KEN_DONE
 *
 * Entry/Exit Conditions:
 *    Expects to be called by KEN as a result of a tunable event
 *    triggered by the tunable infrastructure.  No locks held on
 *    entry or exit.
 *
 */

int advfs_tn_open_max(ken_id_t       eventid,
                      int            reason,
                      ken_instance_t instance,
                      void          *handler_data,
                      ktune_event_t *event_data,
                      int           *result)
{
    ktune_id_t    tune_id;
    ktune_txnid_t tune_txnid;
    uint64_t      value;
    int ret = 0;

    switch (reason) {
    case KEN_EVENT:

	tune_id    = event_data->kte_tuneid;
	tune_txnid = event_data->kte_txnid;

        switch (event_data->kte_op) {
        case KTOP_CAPABLE:
            *result = 
		KTOP_CAPABLE | 
		KTOP_GETDEFAULT |
		KTOP_PREPARE | 
		KTOP_COMMIT;
            break;

        case KTOP_GETDEFAULT:
	    value = advfs_open_max_roundup( 
                ( ((phys_mem_pages*NBPG)/25)/sizeof(bfAccessT) ) );
	    ktune_savedefault( tune_txnid, tune_id, value );
            break;

        case KTOP_PREPARE:
	    if (ret = ktune_pending( tune_txnid, tune_id, &value)) {
	        value = advfs_open_max_roundup( value );
		*result = ret;
		break;
	    }

	    ktval_advfs_open_max = value;
            break;

        case KTOP_COMMIT:
            advfs_set_open_max(ktval_advfs_open_max);
            break;

        default:
            break;
        }
        break;

    case KEN_BACKOUT:
        switch (event_data->kte_op) {
        case KTOP_PREPARE:
	    /* no need to undo change, only temporary global variable set */
            break;
        default:
            break;
        }
        break;

    default:
         break;
    }
    return KEN_DONE;
}


/*
 * advfs_tn_open_min - Tunable handler
 *
 * Tunable:
 *     advfs_open_min
 *
 * Description:
 *     ADVFS minimum number of guaranteed opens
 *
 * Parameters:
 *    IN    eventid        KEN event ID
 *    IN    reason         KEN reason code
 *    IN    instance       KEN event instance handle
 *    IN    handler_data   tunable handler data pointer
 *    IN    event_data     tunable event data pointer
 *    OUT    result         return status of handler
 *
 * Return Values:
 *    KEN_DONE
 *
 * Entry/Exit Conditions:
 *    Expects to be called by KEN as a result of a tunable event
 *    triggered by the tunable infrastructure.  No locks held on
 *    entry or exit.
 *
 */

int advfs_tn_open_min(ken_id_t        eventid,
                      int             reason,
                      ken_instance_t  instance,
                      void           *handler_data,
                      ktune_event_t  *event_data,
                      int            *result)
{
    ktune_id_t    tune_id;
    ktune_txnid_t tune_txnid;
    uint64_t      value;
    int           ret;

    switch (reason) {
    case KEN_EVENT:

	tune_id    = event_data->kte_tuneid;
	tune_txnid = event_data->kte_txnid;

        switch (event_data->kte_op) {
        case KTOP_CAPABLE:
            *result = 
		KTOP_CAPABLE | 
		KTOP_PREPARE | 
		KTOP_COMMIT;
            break;

        case KTOP_PREPARE:
	    if (ret = ktune_pending( tune_txnid, tune_id, &value)) {
		*result = ret;
		break;
	    }

	    ktval_advfs_open_min = value;
            break;

        case KTOP_COMMIT:
            advfs_set_open_min(ktval_advfs_open_min);
            break;

        default:
            break;
        }
        break;

    case KEN_BACKOUT:
        switch (event_data->kte_op) {
        case KTOP_PREPARE:
	    /* no need to undo change, only temporary global variable set */
            break;
        default:
            break;
        }
        break;

    default:
        break;
    }
    return KEN_DONE;
}

/*
 * advfs_tn_io_retries - Tunable handler
 *
 * Tunable:
 *     advfs_io_retries
 *
 * Description:
 *     ADVFS number of seconds to retry a retriable I/O failure.
 *
 * Parameters:
 *    IN    eventid        KEN event ID
 *    IN    reason         KEN reason code
 *    IN    instance       KEN event instance handle
 *    IN    handler_data   tunable handler data pointer
 *    IN    event_data     tunable event data pointer
 *    OUT   result         return status of handler
 *
 * Return Values:
 *    KEN_DONE
 *
 * Entry/Exit Conditions:
 *    Expects to be called by KEN as a result of a tunable event
 *    triggered by the tunable infrastructure.  No locks held on
 *    entry or exit.
 *
 */

int advfs_tn_io_retries(ken_id_t       eventid,
                        int            reason,
                        ken_instance_t instance,
                        void           *handler_data,
                        ktune_event_t  *event_data,
                        int            *result)
{
    ktune_id_t    tune_id;
    ktune_txnid_t tune_txnid;
    uint64_t      value;
    int           ret;

    switch (reason) {
    case KEN_EVENT:

	tune_id    = event_data->kte_tuneid;
	tune_txnid = event_data->kte_txnid;

        switch (event_data->kte_op) {
        case KTOP_CAPABLE:
            *result = 
		KTOP_CAPABLE | 
		KTOP_PREPARE | 
		KTOP_COMMIT;
            break;

        case KTOP_PREPARE:
	    if (ret = ktune_pending( tune_txnid, tune_id, &value)) {
		*result = ret;
		break;
	    }

	    ktval_advfs_io_retries = value;
            break;

        case KTOP_COMMIT:
            AdvfsIORetryControl = ktval_advfs_io_retries;
            break;

        default:
            break;
        }
        break;

    case KEN_BACKOUT:
        switch (event_data->kte_op) {
        case KTOP_PREPARE:
	    /* no need to undo change, only temporary global variable set */
            break;
        default:
            break;
        }
        break;

    default:
        break;
    }
    return KEN_DONE;
}


/*
 * advfs_tn_freezefs_timeout - Tunable handler
 *
 * Tunable:
 *     advfs_freezefs_timeout
 *
 * Description:
 *     ADVFS number of seconds to remain frozen
 *     after executing advfreezefs with default time
 *
 * Parameters:
 *    IN    eventid        KEN event ID
 *    IN    reason         KEN reason code
 *    IN    instance       KEN event instance handle
 *    IN    handler_data   tunable handler data pointer
 *    IN    event_data     tunable event data pointer
 *    OUT   result         return status of handler
 *
 * Return Values:
 *    KEN_DONE
 *
 * Entry/Exit Conditions:
 *    Expects to be called by KEN as a result of a tunable event
 *    triggered by the tunable infrastructure.  No locks held on
 *    entry or exit.
 *
 */

int advfs_tn_freezefs_timeout(ken_id_t       eventid,
			      int            reason,
			      ken_instance_t instance,
			      void           *handler_data,
			      ktune_event_t  *event_data,
			      int            *result)
{
    ktune_id_t    tune_id;
    ktune_txnid_t tune_txnid;
    uint64_t      value;
    int           ret;

    switch (reason) {
    case KEN_EVENT:

	tune_id    = event_data->kte_tuneid;
	tune_txnid = event_data->kte_txnid;

        switch (event_data->kte_op) {
        case KTOP_CAPABLE:
            *result = 
		KTOP_CAPABLE | 
		KTOP_VALIDATE | 
		KTOP_PREPARE | 
		KTOP_COMMIT;
            break;

        case KTOP_VALIDATE:
	    if (ret = ktune_pending( tune_txnid, tune_id, &value)) {
		*result = ret;
		break;
	    }

	    /* check for legal value - less than or greater than zero */
	    if (value == 0) {
		ktune_error( tune_txnid, 
			     "The value [%ld] of %s is not a legal value",
			     value, ktune_name(tune_id));
		*result = EINVAL;
		break;
	    }
	    
            break;

        case KTOP_PREPARE:
	    /* get the pending value */
	    if (ret = ktune_pending( tune_txnid, tune_id, &value)) {
		*result = ret;
		break;
	    }
	    
	    /* check for legal value in case not previously validated */
	    if (value == 0) {
		ktune_error( tune_txnid, 
			     "The value [%ld] of %s is not a legal value",
			     value, ktune_name(tune_id));
		*result = EINVAL;
		break;
	    }

	    /* save the value off for commit */
	    ktval_advfs_freezefs_timeout = (int64_t)value;

            break;

        case KTOP_COMMIT:
	    /* just set the real value here */
            AdvfsFreezefsTimeout = ktval_advfs_freezefs_timeout;
            break;

        default:
            break;
        }
        break;

    case KEN_BACKOUT:
        switch (event_data->kte_op) {
        case KTOP_PREPARE:
	    /* no need to undo change, only temporary global variable set */
            break;
        default:
            break;
        }
        break;

    default:
        break;
    }
    return KEN_DONE;
}


/*
 * advfs_tn_quot_undrflo_behav - Tunable handler
 *
 * Tunable:
 *     advfs_quot_undrflo_behav
 *
 * Description:
 *     ADVFS behavior on quota underflow
 *
 * Parameters:
 *    IN    eventid        KEN event ID
 *    IN    reason         KEN reason code
 *    IN    instance       KEN event instance handle
 *    IN    handler_data   tunable handler data pointer
 *    IN    event_data     tunable event data pointer
 *    OUT    result         return status of handler
 *
 * Return Values:
 *    KEN_DONE
 *
 * Entry/Exit Conditions:
 *    Expects to be called by KEN as a result of a tunable event
 *    triggered by the tunable infrastructure.  No locks held on
 *    entry or exit.
 *
 */

int advfs_tn_quot_undrflo_behav( ken_id_t        eventid,
                                 int             reason,
                                 ken_instance_t  instance,
                                 void           *handler_data,
                                 ktune_event_t  *event_data,
                                 int            *result)
{
    ktune_id_t    tune_id;
    ktune_txnid_t tune_txnid;
    uint64_t      value;
    int           ret;

    switch (reason) {
    case KEN_EVENT:

	tune_id    = event_data->kte_tuneid;
	tune_txnid = event_data->kte_txnid;

        switch (event_data->kte_op) {
        case KTOP_CAPABLE:
            *result = 
		KTOP_CAPABLE | 
		KTOP_PREPARE | 
		KTOP_COMMIT;
            break;

        case KTOP_PREPARE:
	    if (ret = ktune_pending( tune_txnid, tune_id, &value)) {
		*result = ret;
		break;
	    }

            ktval_advfs_quot_undrflo_behav = value;
            break;

        case KTOP_COMMIT:
	    advfs_quot_undrflo_behav = ktval_advfs_quot_undrflo_behav;
            break;

        default:
            break;
        }
        break;

    case KEN_BACKOUT:
        switch (event_data->kte_op) {
        case KTOP_PREPARE:
	    /* no need to undo change, only temporary global variable set */
            break;
        default:
            break;
        }
        break;

    default:
        break;
    }
    return KEN_DONE;
}

void
advfs_init_tunables()
{
    /* for normal operation fall through */
    if(!advfs_tunable_debug) advfs_init_tunables_int();
}

void
advfs_init_tunables_int()
{
    int status;

/*
 *	Register handler for advfs_open_max
 */

    status = ktune_register_handler(
                KTUNE_VERSION,               /* version      */
                "advfs_open_max",            /* tunable name */
                0,                           /* ken_flags    */
                KEN_UNORDERED,               /* ken_order    */
                "ADVFS configured maximum number of opens",
			                     /* description  */
                advfs_tn_open_max,           /* handler      */
                NULL);                       /* handler_data */

    ktid_advfs_open_max = ktune_id( "advfs_open_max" );

    ktval_advfs_open_max = ktune_get( "advfs_open_max",
				      ADVFS_OPEN_MAX_FAILSAFE);


/*
 *	Register handler for advfs_open_min
 */

    status = ktune_register_handler(
                KTUNE_VERSION,               /* version      */
                "advfs_open_min",            /* tunable name */
                0,                           /* ken_flags    */
                KEN_UNORDERED,               /* ken_order    */
                "ADVFS minimum number of guaranteed opens",
			                     /* description  */
                advfs_tn_open_min,           /* handler      */
                NULL);                       /* handler_data */

    ktid_advfs_open_min = ktune_id( "advfs_open_min" );

    ktval_advfs_open_min = ktune_get( "advfs_open_min",
				       ADVFS_OPEN_MIN_FAILSAFE);

/*
 *	Register handler for advfs_io_retries
 */

    status = ktune_register_handler(
                KTUNE_VERSION,                 /* version      */
                "advfs_io_retries",            /* tunable name */
                0,                             /* ken_flags    */
                KEN_UNORDERED,                 /* ken_order    */
                "ADVFS number of seconds to retry I/O failures",
			                       /* description  */
                advfs_tn_io_retries,           /* handler      */
                NULL);                         /* handler_data */

    ktid_advfs_io_retries = ktune_id( "advfs_io_retries" );

    ktval_advfs_io_retries = ktune_get( "advfs_io_retries",
					  ADVFS_IO_RETRIES_FAILSAFE);

    /* no init routine for this so we'll just do it here */
    AdvfsIORetryControl = ktval_advfs_io_retries;

/*
 *	Register handler for advfs_freezefs_timeout
 */

    status = ktune_register_handler(
                KTUNE_VERSION,                 /* version      */
                "advfs_freezefs_timeout",      /* tunable name */
                0,                             /* ken_flags    */
                KEN_UNORDERED,                 /* ken_order    */
                "ADVFS freezefs number of seconds to remain frozen",
		                               /* description  */
                advfs_tn_freezefs_timeout,     /* handler      */
                NULL);                         /* handler_data */

    ktid_advfs_freezefs_timeout = ktune_id( "advfs_freezefs_timeout" );

    ktval_advfs_freezefs_timeout = ktune_get( "advfs_freezefs_timeout",
					      ADVFS_FREEZEFS_TIMEOUT_FAILSAFE);


/*
 *	Register handler for advfs_quot_undrflo_behav 
 */

    status = ktune_register_handler(
                KTUNE_VERSION,                    /* version      */
                "advfs_quot_undrflo_behav",       /* tunable name */
                0,                                /* ken_flags    */
                KEN_UNORDERED,                    /* ken_order    */
                "ADVFS quota underflow behavior",
			                          /* description  */
                advfs_tn_quot_undrflo_behav,
						  /* handler      */
                NULL);                            /* handler_data */

    ktid_advfs_quot_undrflo_behav =
	ktune_id( "advfs_quot_undrflo_behav" );

    ktval_advfs_quot_undrflo_behav =
	ktune_get( "advfs_quot_undrflo_behav",
                   ADVFS_QUOT_UNDRFLO_FAILSAFE );

}

void
advfs_teardown_tunables()
{
    (void)ktune_unregister_handler(
                KTUNE_VERSION,               /* version      */
                "advfs_open_max",            /* tunable name */
                0,                           /* ken_flags    */
                KEN_UNORDERED,               /* ken_order    */
                advfs_tn_open_max,           /* handler      */
                NULL);                       /* handler_data */

    (void)ktune_unregister_handler(
                KTUNE_VERSION,               /* version      */
                "advfs_open_min",            /* tunable name */
                0,                           /* ken_flags    */
                KEN_UNORDERED,               /* ken_order    */
                advfs_tn_open_min,           /* handler      */
                NULL);                       /* handler_data */

    (void)ktune_unregister_handler(
                KTUNE_VERSION,               /* version      */
                "advfs_io_retries",          /* tunable name */
                0,                           /* ken_flags    */
                KEN_UNORDERED,               /* ken_order    */
                advfs_tn_open_min,           /* handler      */
                NULL);                       /* handler_data */

    (void)ktune_unregister_handler(
                KTUNE_VERSION,               /* version      */
                "advfs_freezefs_timeout",    /* tunable name */
                0,                           /* ken_flags    */
                KEN_UNORDERED,               /* ken_order    */
                advfs_tn_freezefs_timeout,   /* handler      */
                NULL);                       /* handler_data */

    (void)ktune_unregister_handler(
                KTUNE_VERSION,               /* version      */
                "advfs_quot_undrflo_behav",  /* tunable name */
                0,                           /* ken_flags    */
                KEN_UNORDERED,               /* ken_order    */
                advfs_tn_open_min,           /* handler      */
                NULL);                       /* handler_data */

}


#ifdef _BullseyeCoverage 
static
void
advfs_ccover_init() 
{
    int i,k;
    uint64_t j;

/*
 *  Exercise msb()
 */
    for ( i=0,j=1; i<=63; i++ ) {
        k = msb(j);
        MS_SMP_ASSERT(k == i); 
        j = j << 1;
    }
}
#endif
