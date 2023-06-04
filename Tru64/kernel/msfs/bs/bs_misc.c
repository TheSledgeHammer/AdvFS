/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993                      *
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
 *      AdvFS
 *
 * Abstract:
 *
 *      This module contains misc. BS routines
 *
 * Date:
 *
 *      Mon Jul 30 16:21:28 1990
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: bs_misc.c,v $ $Revision: 1.1.442.15 $ (DEC) $Date: 2007/08/10 11:43:53 $"

#include <varargs.h>
#include <sys/user.h>
#include <sys/file.h>
#include <sys/vnode.h>
#include <sys/specdev.h>
#include <sys/audit.h>
#include <sys/lock_probe.h>
#include <sys/clu.h>
#include <sys/dlm.h>

#include <kern/event.h>

#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/ms_assert.h>
#include <msfs/bs_extents.h>
#include <msfs/bs_migrate.h>
#include <msfs/bs_service_classes.h>
#include <msfs/bs_stg.h>
#include <msfs/bs_stripe.h>
#include <msfs/fs_file_sets.h>
#include <msfs/msfs_syscalls.h>
#include <msfs/ms_osf.h>
#include <msfs/bs_ims.h>
#include <msfs/advfs_evm.h>
#include <msfs/bs_index.h>
#include <sys/versw.h>

#ifdef ESS
#include <ess/event_simulation.h>
#endif

static int copyin_domain_and_set_names(char *in_dmnTbl, char *in_setName, char *in_setName2, char **out_dmnTbl, char **out_setName, char **out_setName2);
static statusT open_set(mode_t setMode, mode_t dmnMode, int setOwner, int dmnOwner, bfSetIdT setId, bfSetT **retBfSetp, domainT **dmnPP);
static void validate_syscall_structs(void);
void bs_get_global_stats(opGetGlobalStatsT *stats);

extern struct bsMPg *Dpgp;
extern struct bsMPg *GRpgp;
extern int AdvfsFixUpSBM;
extern int NumAccess;
extern ulong access_structures_allocated;
extern ulong access_structures_deallocated;
extern ulong failed_access_structure_deallocations;
extern ulong cleanup_thread_calls;
extern ulong cleanup_structs_skipped;
extern ulong cleanup_structs_processed;

#ifdef MSFS_CRASHTEST
extern int CrashCnt;
extern ftxAgentIdT CrashAgent;
extern int CrashByAgent;
extern int CrashPrintRtDn;
extern logWriteModeT FtxDoneMode;
extern bfDomainIdT CrashDomain;
#endif

#ifdef FTX_PROFILING
extern ftxProfT AgentProfStats[];
#endif

extern void bs_lock_mgr_init( void );

#define ADVFS_MODULE BS_MISC

lock_data_t InitLock;
event_t MsfsDbgEvent;

struct lockinfo *ADVInitLock_info;

#define INITLOCK_LOCK_WRITE(Lk) \
        lock_write( Lk );

#define INITLOCK_UNLOCK(Lk) \
        lock_done( Lk );

#define SUSER (suser(u.u_cred, &u.u_acflag) == 0)

extern struct nchstats nchstats;


int
find_cfs_server (
        opIndexT         op,            /* in - syscall operation type */
        libParamsT       *buf,          /* in/out - pointer to syscall-specific
                                           data structure */
        ServerSearchKeyT key,           /* in - search key for cfs server */
        int              *do_local,     /* out - flag to show if op should be
                                           carried out locally */
        int              *have_lock,    /* out - flag to cause unlock at exit */
        int              *have_cmsdb_lock, /* out - causes unlock at exit */
        dlm_lkid_t       *domain_lock,  /* in/out - the domain lock */
        dlm_lkid_t       *cmsdb_lock    /* in/out - the cmsdb dlm lockid  */
        );


/*
 * This is a note to future developers who may modify check_privilege().
 *
 * This function currently just checks the dataSafety field in the bfAttr
 * structure.  When this function is called from msfs_setproplist_int(),
 * a dummy bfAttr structure is passed with just the dataSafety field
 * initialized.  If the check_privilege() routine is modified
 * to do any more checking, then you must also modify the initialization
 * of the dummy bfattrp setup in msfs_setproplist_int().
 *
 */
int
check_privilege(
    bfAccessT *bfap,
    mlBfAttributesT *bfattrp
    )
{
    int error, type;
    struct fsContext *contextp = VTOC( ATOV( bfap ) );

    if ((contextp == NULL) || (contextp->fs_flag & META_OPEN)) {
        return ENOT_SUPPORTED;
    }

    /* Is the dataSafety attribute of a quotafile changing? */
    if ((bfDataSafetyT)bfattrp->dataSafety != bfap->dataSafety) {
        for (type = 0; type < MAXQUOTAS; type++) {
            if (contextp == contextp->fileSetNode->qi[type].qiContext) {
                return ENOT_SUPPORTED;
             }
        }
    }

    if (u.u_uid != contextp->dir_stats.st_uid) {
        /* not owner of file */

        error = suser( u.u_cred, &u.u_acflag );
        if (error != 0) {
            /* not superuser */
            return error;
        }
    }

    return 0;
}

static void validate_syscall_structs( void );

int
msfs_real_syscall(
    opTypeT     opType,         /* in - msfs operation to be performed */
    libParamsT  *parmBuf,       /* in - ptr to op-specific parameters buffer;*/
                                /*      contents are modified. */
    int         parmBufLen      /* in - byte length of parmBuf */
    );


void
bs_trace(
    unsigned int *flags,    /* in/out - flags to get, set or clear */
    mlTrActionT action      /* in - 0 = get, 1 = set, 2 = clr */
    )
{
    extern unsigned TrFlags;

    if (action == TRACE_SET) {
        /* SET */
        TrFlags |= *flags;

    } else if (action == TRACE_CLR) {
        /* CLEAR */
        TrFlags &= ~(*flags);
    }

    *flags = TrFlags;
}


/*
 * advfs_sad
 *
 * When the file system detects an internal inconsistency it calls
 * this routine to display some diagnostic information and to abort
 * the system.
 *
 * SAD stands for:
 *
 *      1. "Affected or characterized by sorrow or unhappiness."
 *
 *      2.  System Assurance Defect
 */

void
advfs_sad(
    char *module,
    int line,
    char *fmt,
    char *msg,
    long n1,
    long n2,
    long n3
    )
{
    char buf[256];
    char buf1[256];

    dprintf( "ADVFS EXCEPTION\n" );
    dprintf( "Module = %s, Line = %d\n", module, line );

    /*
     * Most uses of the ADVFS_SADx macros pass arguments that will be formatted
     * by the fmt string passed, as N1 = %d, N2 = %d, etc.  However, there
     * are also many uses of these macros where the msg string passed has
     * %s or %d variable parts.  The following tries to accomodate both types
     * of uses of the ADVFS_SADx macros.  The fmt string is first filled in
     * with the msg passed, and the n1, n2, n3 arguments will go into the fmt
     * "N1 = , N2 =" part.  The second sprintf() of the resulting string is
     * to accomodate uses of %d or %s in the msg string passed.
     */
    if (msg != NULL) {
        sprintf( buf, fmt, msg, n1, n2, n3 );
    } else {
        sprintf( buf, fmt, "", n1, n2, n3 );
    }

    sprintf( buf1, buf, n1, n2, n3 );
    dprintf(buf1);
    dprintf( "\n" );

    panic( buf1 );
}

int
macro_sad(
           char *module,
           int line,
           char *fmt,
           char *msg,
           long n1,
           long n2,
           long n3)
{
    advfs_sad( module, line, fmt, msg, n1, n2, n3);
    return 1;
}


/*
 * write to console and syslog message buffer
 */
void
ms_printf(va_alist)
         va_dcl
{
    aprintf((char *)va_alist);
}

/*
 * Try to do a uprintf().  If at fails do an aprintf().
 */
#define PRFMAX 128
void
ms_uprintf(fmt, va_alist)
    u_char *fmt;
    va_dcl
{
    va_list valist;
    int n;
    char buf[PRFMAX];

    /*
     * Format the args using prf() into a
     * temporary buffer.  If nothing to
     * print, return.
     */
    va_start(valist);
    n = prf(&buf[0], &buf[PRFMAX], fmt, valist);
    va_end(valist);
    if (n <= 0)
        return;

    /*
     * The caller really wanted the message to go to the tty.
     * Try to oblige, if we're not at an interrupt level and
     * we're not holding any simple locks.
     */
    if (AT_INTR_LVL() || SLOCK_COUNT)
        aprintf(buf);
    else if (uprintf(buf))
        aprintf(buf);
}

/*
 * Do both a uprintf() and a aprintf().
 */
void
ms_uaprintf(fmt, va_alist)
    u_char *fmt;
    va_dcl
{
    va_list valist;
    int n;
    char buf[PRFMAX];

    /*
     * Format the args using prf() into a
     * temporary buffer.  If nothing to
     * print, return.
     */
    va_start(valist);
    n = prf(&buf[0], &buf[PRFMAX], fmt, valist);
    va_end(valist);
    if (n <= 0)
        return;

    /*
     * Perform an aprintf().  If we're not at an interrupt
     * level and we're not holding any simple locks, also
     * call uprintf().
     */
    aprintf(buf);
    if (!AT_INTR_LVL() && !SLOCK_COUNT)
        uprintf(buf);
}

/*
 * ms_flush - flush the printf stream correctly for user or kernel
 */
void
ms_pfflush(void)
{
}

/*
 * bs_kernel_pre_init
 *
 * Init the kernel initialization mutex.  Called by msfs_init().
 */

void
bs_kernel_pre_init(void)
{
    extern mlStatusT (* MsfsSyscallp)(
        opTypeT       opType,
        libParamsT    *parmBuf,
        int           parmBufLen
        );
#ifdef ADVFS_DEBUG
extern mutexT LockMgrMutex;
extern struct lockinfo *msfs_lockmgrmutex_lockinfo;
    /*
     * Initialize the Lock Mgr's mutex.
     */
    simple_lock_setup( &LockMgrMutex.mutex, msfs_lockmgrmutex_lockinfo );
#endif

    MsfsSyscallp = msfs_real_syscall;

#ifdef ADVFS_DEBUG
    AdvfsLockStats = (advfsLockStatsT *)ms_malloc( sizeof( advfsLockStatsT ) );
#else
    bs_lock_mgr_init();
#endif

    lock_setup( &InitLock, ADVInitLock_info, TRUE );
    event_init( &MsfsDbgEvent );
}

dbg_event_wait( void )
{
    event_wait( &MsfsDbgEvent, 0, 0 );
}
dbg_event_clear( void )
{
    event_clear( &MsfsDbgEvent );
}
dbg_event_post( void )
{
    event_post( &MsfsDbgEvent );
}
/*
 * bs_kernel_init
 *
 * This routine is called from bs_bfdmn_tbl_activate() to
 * call bs_init only once with mutual exclusion.
 * This routine is also called by bs_dmn_init().
 */

void
bs_kernel_init(int doingRoot)
{
    static initDone = 0;

    if( initDone == 0 ) {
        INITLOCK_LOCK_WRITE( &InitLock )
        if( initDone == 0 ) {
            bs_init(doingRoot);
            initDone = 1;
        }
        INITLOCK_UNLOCK( &InitLock )
    }
}


/*
 * bs_init
 *
 * do all bitfile initialization
 */

/* TraceLock & TraceSequence can move to vfs_init if tracing is added to */
/* vnodes. They can move to vm_mem_init (earier than vfs_init) if object or */
/* vm page tracing is added. */
simple_lock_data_t TraceLock;
struct lockinfo *TraceLock_info;
int TraceSequence;

void
bs_init(int doingRoot)
{

    MS_SMP_ASSERT( sizeof(bsTDirPgT) == ADVFS_PGSZ );
    MS_SMP_ASSERT( sizeof(bsStgBmT) == ADVFS_PGSZ );
    MS_SMP_ASSERT( sizeof(bsMPgT) == ADVFS_PGSZ );
    MS_SMP_ASSERT ((ADVFS_PGSZ_IN_BLKS % BS_CLUSTSIZE) == 0);

    /* We have some code (not enough) to allow our bitfile page size */
    /* (ADVFS_PGSZ) to be a multiple of the UBC page size (PAGE_SIZE). */
    /* We have no code to allow ADVFS_PGSZ to be < PAGE_SIZE. */
    /* See the comment in page_lookup. */
    MS_SMP_ASSERT( ADVFS_PGSZ == PAGE_SIZE );

    /* To maintain compatibility between our concept of a disk block */
    /* and the CAM driver concept. (See comment in bs_public.h) */
    MS_SMP_ASSERT( BS_BLKSIZE == DEV_BSIZE );

    validate_syscall_structs();

    simple_lock_init(&TraceLock);

    msfs_io_init();
    bs_init_io_thread();
    bs_init_area();
    bs_init_rbmt_thread();
    bs_init_access_alloc_thread();
    bs_bfs_init();
    bs_domain_init();
    bs_init_freeze_thread();
    init_tagdir_opx();
    init_bscreate_opx();
    init_bs_delete_opx();
    init_bs_stg_opx();
    init_bs_xtnts_opx();
    init_bs_bitmap_opx();
    init_crmcell_opx();
    init_idx_index_opx();
    init_bs_bmt_util_opx();
    mig_register_migrate_agent();
    str_register_stripe_agent();
    msfs_pl_register_agents();
    fs_init_ftx();
    ftx_init();
    fs_init_cleanup_thread();
    ss_kern_init();

    if (clu_is_ready()) {
        GRpgp = (struct bsMPg *)ms_malloc( sizeof( struct bsMPg ) );
    }
    Dpgp = (struct bsMPg *)ms_malloc( sizeof( struct bsMPg ) );
}


/*
 * bs_owner
 *
 * This routine may be used to determine if an object belongs
 * to the user associated with the current thread.
 */

int
bs_owner(
    uid_t ouid  /* in - object's user id */
    )
{
    struct ucred *cred = u.u_nd.ni_cred;

    if (SUSER || cred->cr_uid == ouid) {
        return TRUE;
    } else {
        return FALSE;
    }
}


/*
 * bs_accessbile
 *
 * This routine may be used to determine if the user associated with
 * the current thread has access to an object.
 */

int
bs_accessible(
    mode_t mode,            /* in - mode wanted */
    mode_t omode,           /* in - object's permissions mode */
    uid_t ouid,             /* in - object's uid */
    gid_t ogid              /* in - object's gid */
    )
{
    struct ucred *cred = u.u_nd.ni_cred;
    gid_t *gp;
    int i;

    if (cred->cr_uid == 0) {
        /* superuser, just return success */
        return TRUE;
    }

    if (cred->cr_uid != ouid) {
        mode >>= 3;

        if (ogid == cred->cr_gid) {
            goto gotit;
        }
        gp = cred->cr_groups;

        for (i = 0;i < cred->cr_ngroups; i++, gp++) {
            if (ogid == *gp) {
                goto gotit;
            }
        }
        mode >>=3;

        gotit:
             ;
    }

    if ((omode & mode) != 0) {
        return TRUE;
    }

    return FALSE;
}


/*
 * copyin_domain_and_set_names
 *
 * Used to copy a domain name and up to two set names from user to
 * kernel space.  Any or all three parameters can be copied.
 * If an 'out' parameter is NULL then no copy is done for that
 * parameter.
 */

static int
copyin_domain_and_set_names(
    char *in_dmnName,           /* in - domain name */
    char *in_setName,           /* in - set name 1 */
    char *in_setName2,          /* in - set name 2 */
    char **out_dmnName,         /* out - domain name */
    char **out_setName,         /* out - set name 1 */
    char **out_setName2         /* out - set name 2 */
    )
{
    int error, size;

    if (out_dmnName != NULL) {
        /* copy domain table name */
        *out_dmnName = ms_malloc( BS_DOMAIN_NAME_SZ );

        error = copyinstr( in_dmnName, *out_dmnName, BS_DOMAIN_NAME_SZ, &size );
        if (error != 0) {
            ms_free( *out_dmnName );
            return error;
        }
    }

    if (out_setName != NULL) {
        /* copy set name 1 */
        *out_setName = ms_malloc( BS_SET_NAME_SZ );

        error = copyinstr( in_setName, *out_setName, BS_SET_NAME_SZ, &size );
        if (error != 0) {
            ms_free( *out_setName );
            ms_free( *out_dmnName );
            return( error );
        }
    }

    if (out_setName2 != NULL) {
        /* copy set name 2 */
        *out_setName2 = ms_malloc( BS_SET_NAME_SZ );

        error = copyinstr( in_setName2, *out_setName2, BS_SET_NAME_SZ, &size );
        if (error != 0) {
            ms_free( *out_setName2 );
            ms_free( *out_setName );
            ms_free( *out_dmnName );
            return( error );
        }
    }

    return 0;
}

/*
 * free_domain_and_set_names
 *
 * Used to free the kernel space allocated by copyin_domain_and_set_names().
 */

static void
free_domain_and_set_names(
    char *dmnTbl,
    char *setName,
    char *setName2
    )
{
    ms_free( dmnTbl );
    ms_free( setName );
    ms_free( setName2 );
}


/*
 * The following may be used when calling open_domain() or open_set()
 * to specify whether the domain/set may be open only by the owner
 * or by anyone.
 */

#define OWNER_ONLY TRUE
#define ANYONE FALSE

/*
 * open_domain
 *
 * Used by the syscalls to open a domain via its ID.
 */

static statusT
open_domain(
    mode_t mode,       /* in - access mode (read, write, etc.) */
    int ownerOnly,     /* in - indicates if only owner is allowed access */
    bfDomainIdT dmnId, /* in - domain's id */
    domainT **dmnPP    /* out - pointer to open domain */
    )
{
    statusT sts;
    bfDmnParamsT *dmnParamsp = NULL;
    int dmnActive = FALSE, dmnOpen = FALSE;

    dmnParamsp = (bfDmnParamsT *)ms_malloc( sizeof( bfDmnParamsT ));

    sts = bs_bfdmn_id_activate( dmnId );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    dmnActive = TRUE;

    sts = bs_domain_access( dmnPP, dmnId, FALSE );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    dmnOpen = TRUE;

    sts = bs_get_dmn_params( *dmnPP, dmnParamsp, 0 );
    if (sts != EOK) {
        goto HANDLE_EXCEPTION;
    }

    if (ownerOnly && !bs_owner( dmnParamsp->uid )) {
        RAISE_EXCEPTION( E_NOT_OWNER );
    }

    if (!bs_accessible( mode, dmnParamsp->mode, dmnParamsp->uid,
                        dmnParamsp->gid )) {
        RAISE_EXCEPTION( E_ACCESS_DENIED );
    }

    if (dmnParamsp != NULL) {
        ms_free( dmnParamsp );
    }

    return EOK;

HANDLE_EXCEPTION:

    if (dmnOpen) {
        bs_domain_close( *dmnPP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( dmnId, 0 );
    }

    if (dmnParamsp != NULL) {
        ms_free( dmnParamsp );
    }

    return sts;
}

/*
 * close_domain
 *
 * Counterpart to open_domain().  Closes the domain opened
 * by open_domain().
 */

static void
close_domain(
    bfDomainIdT dmnId, /* in - domain's id */
    domainT *dmnP,     /* in - domain's pointer */
    uint32T flag       /* in - set in cluster to designate mount path */
    )
{
    bs_domain_close( dmnP );
    (void) bs_bfdmn_deactivate( dmnId, flag );
}


/*
 * open_set
 *
 * Used by the syscalls to open a set via its ID.
 */

static statusT
open_set(
    mode_t setMode,     /* in - access mode (read, write, etc) for the set */
    mode_t dmnMode,     /* in - access mode for the set's domain */
    int setOwner,       /* in - is only the owner allowed access to set */
    int dmnOwner,       /* in - is only the owner allowed access to the dmn */
    bfSetIdT setId,     /* in - set's id */
    bfSetT **retBfSetp, /* out - open set's descriptor pointer */
    domainT **dmnPP     /* out - set's open domain pointer */
    )
{
    statusT sts;
    bfSetParamsT setParams;
    int dmnOpen = FALSE, setOpen = FALSE;

    /* First open the set's domain */

    sts = open_domain( dmnMode, dmnOwner, setId.domainId, dmnPP );
    if (sts != EOK) {
        goto _error;
    }

    dmnOpen = TRUE;

    /* Now open the set */

    sts = rbf_bfs_access( retBfSetp, setId, FtxNilFtxH );
    if (sts != EOK) {
        goto _error;
    }

    setOpen = TRUE;

    sts = bs_get_bfset_params( *retBfSetp, &setParams, 0 );
    if (sts != EOK) {
        goto _error;
    }

    if (setOwner && !bs_owner( setParams.uid )) {
        sts = E_NOT_OWNER;
        goto _error;
    }

    if (!bs_accessible( setMode,
                        setParams.mode, setParams.uid, setParams.gid )) {
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    return EOK;

_error:

    if (dmnOpen) {
        close_domain( setId.domainId, *dmnPP, 0 );
    }

    if (setOpen) {
        bs_bfs_close( *retBfSetp, FtxNilFtxH, BFS_OP_DEF );
    }

    return sts;
}

/*
 * close_set
 *
 * Closes the set that was open via open_set().
 */

static void
close_set(
    bfSetIdT setId,
    bfSetT *bfSetp,
    domainT *dmnP
    )
{
    bs_bfs_close( bfSetp, FtxNilFtxH, BFS_OP_DEF );
    close_domain( setId.domainId, dmnP, 0 );
}


void msfs_audit_syscall( register libParamsT *libBufp,
                         opIndexT opIndex,
                         int error);
/*
 * msfs_real_syscall
 *
 * Implements the AdvFS system call.  Eventually it should be part of
 * the loadable portion of the filesystem.  The routine msfs_syscall()
 * should check to see if AdvFS is loaded before calling this routine.
 *
 * the scope of msfs_real_syscall() is limited strictly to
 * parameter handling.  The routine takes an operation type (opType),
 * a pointer to an operation-specific arguments buffer (parmBuf), and the
 * length of that buffer (parmBufLen).  Based on the operation type, it
 * then performs any or all of the following:
 *
 *    - Copyin the user-space parmBuf to the kernel-space libBufp.
 *    - If there are user-space pointers in libBufp, preserve them
 *        in local variables, and *replace* these pointers in libBufp
 *        with pointers to ms_malloc'ed kernel buffers.  This is very
 *        yucky and should be changed, but for now CFS needs it to
 *        be this way.
 *    - Copyin any user-space data into these new kernel buffers (using
 *        the preserved pointers).
 *    - In a cluster, check to see if the domain is served remotely;
 *        if so, function-ship it. 
 *    - If not in a cluster, or in a cluster but we are the server,
 *      or in a cluster and there is no server:
 *        Call a second-level routine that does the actual work of the
 *        system call.  There is one such routine for each system
 *        call, more or less (well, okay... it's less).
 *    - Copyout any results back into user buffers (using the
 *        preserved pointers).  Free the kernel buffers.
 *    - Restore the preserved pointers back into libBufp.
 *    - Copyout libBufp back into parmBuf.  Free libBufp.
 *    - Return status.
 *
 *
 * In a cluster, a system call can be issued for a domain that is not
 * local to the machine, in which case the parameters are shipped to
 * whatever machine serves that domain.  A server process on that machine
 * directly calls the associated second-level routine, then ships back
 * the results (which are copied out as usual).  The remote machine does
 * not call its own msfs_real_syscall(); in this sense, both
 * msfs_real_syscall() and the CFS syscall server can be considered entry
 * points to the second-level routines.
 * 
 * msfs_real_syscall() returns 0 if successful, AdvFS or system error if not.
 */

int
msfs_real_syscall(
    opTypeT     opIndex,        /* in - msfs operation to be performed */
    libParamsT  *parmBuf,       /* in - ptr to op-specific parameters buffer;*/
                                /*      contents are modified. */
    int         parmBufLen      /* in - byte length of parmBuf */
    )
{
    /* Please note each opType here has a corresponding case in the
       msfs_audit_syscall routine.  When adding, deleting, or changing
       operations here, don't forget to update msfs_audit_syscall, and
       msfs_opcode_to_name (libmsfs).
    */

    register libParamsT *libBufp = NULL;

    statusT sts = EOK;
    int size, error=0;
    int docopyout = 0;
    struct vnode *vp = NULL;
    int which = 0;
    int do_local = 1;
    struct file *fp = NULL;
    bfAccessT *bfap = NULL;
    int dlm_locked = 0;
    int have_domain_lock = 0;
    dlm_lkid_t cfs_domain_lock;
    dlm_lkid_t cms_cmsdb_lock;

#ifdef ESS
    int ess_error = 0;
    int rootdmnmatch = 0;
    domainT *essdmnP;
    bfDomainIdT rootdmnID, localdmnID;
#endif

    /* obtain an exlusive access DLM kernel lock so the mount struct pointers
     * won't get unmounted while we process the syscalls.
     *
     * This lock also serializes the syscalls clusterwide and prevents
     * a planned relocation from happening while a syscall is being 
     * processed.  
     *
     *It is released after exiting the big switch statement.
     */


    /* Allocate arguments buffer and copy in from user space */
    libBufp = (libParamsT *)ms_malloc( sizeof( libParamsT ) );
    if (parmBufLen > sizeof(libParamsT)) {
        RAISE_EXCEPTION( EBAD_PARAMS );
    } else {
        error = ms_copyin( parmBuf, libBufp, parmBufLen );
        if (error) {
            RAISE_EXCEPTION( error );
        }
    }

    if (clu_is_ready()){
        /* 
         * Insure CFS_SENT_INFO allocated for this thread to eliminate
         * potential problems with any future callouts to CFS code.
         * Not necessary to do this callout on every retry but it does 
         * no harm either.
         */
        CC_CFS_CHECK_CSI();
    }

RETRY:  /* CFS may retry ADD_VOL and REMOVE_VOL */
    switch (opIndex) {

        case ADVFS_GET_NAME: {
            char *savedmp, *savedname;

            if (parmBufLen != sizeof(libBufp->getName)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            savedmp = libBufp->getName.mp;
            savedname = libBufp->getName.name;

            /** Copyins **/
            libBufp->getName.mp = ms_malloc(PATH_MAX+1);
            error = copyinstr(savedmp,
                              libBufp->getName.mp,
                              PATH_MAX, &size);
            if (error){
                ms_free(libBufp->getName.mp);
                RAISE_EXCEPTION(error);
            }

            libBufp->getName.name = ms_malloc(PATH_MAX+1);
            error = copyinstr(savedname,
                              libBufp->getName.name,
                              PATH_MAX, &size);
            if (error){
                ms_free(libBufp->getName.mp);
                ms_free(libBufp->getName.name);
                RAISE_EXCEPTION(error);
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_PATH,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_get_name2(libBufp);

            copyout(&libBufp->getName.parentTag,
                    &parmBuf->getName.parentTag, sizeof(mlBfTagT));
            copyoutstr(libBufp->getName.name,
                       savedname, PATH_MAX, &size);

            ms_free(libBufp->getName.mp);
            ms_free(libBufp->getName.name);

            break;
        }

        case ADVFS_UNDEL_GET: {
            char *savedirName;

            if (parmBufLen != sizeof(libBufp->undelGet)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            savedirName = libBufp->undelGet.dirName;

            /** Copyins **/
            libBufp->undelGet.dirName = ms_malloc(PATH_MAX+1);

            error = copyinstr(savedirName,
                              libBufp->undelGet.dirName,
                              PATH_MAX, &size);
            if (error){
                ms_free(libBufp->undelGet.dirName);
                RAISE_EXCEPTION(error);
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_PATH,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_undel_get(libBufp);

            ms_free(libBufp->undelGet.dirName);

            /** Copyouts **/
            copyout(&libBufp->undelGet.undelDirTag,
                    &parmBuf->undelGet.undelDirTag, sizeof(mlBfTagT));

            break;
        }

        case ADVFS_UNDEL_ATTACH: {
            char *saveundelDirname, *savedirName;

            if (parmBufLen != sizeof(libBufp->undelAttach)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            saveundelDirname = libBufp->undelAttach.undelDirName;
            savedirName = libBufp->undelAttach.dirName;

            /** Copyins **/
            libBufp->undelAttach.undelDirName = ms_malloc(PATH_MAX+1);
            error = copyinstr(saveundelDirname,
                              libBufp->undelAttach.undelDirName,
                              PATH_MAX, &size);
            if (error) {
                ms_free(libBufp->undelAttach.undelDirName);
                RAISE_EXCEPTION(error);
            }

            libBufp->undelAttach.dirName = ms_malloc(PATH_MAX+1);
            error = copyinstr(savedirName,
                              libBufp->undelAttach.dirName,
                              PATH_MAX, &size);
            if (error) {
                ms_free(libBufp->undelAttach.dirName);
                ms_free(libBufp->undelAttach.undelDirName);
                RAISE_EXCEPTION(error);
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_PATH,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_undel_attach(libBufp);
     
            ms_free(libBufp->undelAttach.undelDirName);
            ms_free(libBufp->undelAttach.dirName);

            /* no copyout */
            /* reuse OK */
            break;
        }

        case ADVFS_UNDEL_DETACH: {
            char *savedirName;

            if (parmBufLen != sizeof(libBufp->undelDetach)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            savedirName = libBufp->undelDetach.dirName;

            /** Copyins **/
            libBufp->undelDetach.dirName = ms_malloc(PATH_MAX+1);
            error = copyinstr(savedirName,
                              libBufp->undelDetach.dirName,
                              PATH_MAX, &size);
            if (error){
                ms_free(libBufp->undelDetach.dirName);
                RAISE_EXCEPTION(error);
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_PATH,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_undel_detach(libBufp, 0);
            ms_free(libBufp->undelDetach.dirName);


            /* no copyout */
            /* reuse OK */
            break;
        }

        case ADVFS_EVENT: {

            if (parmBufLen != sizeof(libBufp->event)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            sts = msfs_syscall_op_event(libBufp); /* No return value */


            /* no copyout */
            /* reuse OK */
            break;
        }

        case ADVFS_GET_IDX_BF_PARAMS: {

            /* Fall through to ADVFS_GET_BF_PARAMS; based on OpType,   */
            /* msfs_syscall_op_get_bf_params() will know what to do */

        }

        case ADVFS_GET_IDXDIR_BF_PARAMS: {

            /* Fall through to ADVFS_GET_BF_PARAMS; based on OpType,   */
            /* msfs_syscall_op_get_bf_params() will know what to do */

        }

        case ADVFS_GET_BF_PARAMS: {


            if (parmBufLen != sizeof(libBufp->getBfParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->getBfParams.fd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_get_bf_params(libBufp, opIndex, bfap);
            }

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_SET_BF_ATTRIBUTES: {

            if (parmBufLen != sizeof(libBufp->setBfAttributes)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            if ( libBufp->setBfAttributes.bfAttributes.version != 1 ) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->setBfAttributes.fd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_set_bf_attributes(libBufp, bfap, 0);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_GET_BF_IATTRIBUTES: {

            if (parmBufLen != sizeof(libBufp->getBfIAttributes)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->getBfIAttributes.fd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_get_bf_iattributes(libBufp, bfap);
            }

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_SET_BF_IATTRIBUTES: {

            if (parmBufLen != sizeof(libBufp->setBfIAttributes)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            if ( libBufp->setBfIAttributes.bfAttributes.version != 1) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->setBfIAttributes.fd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_set_bf_iattributes(libBufp, bfap);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_MOVE_BF_METADATA: {

            if (parmBufLen != sizeof(libBufp->moveBfMetadata)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->moveBfMetadata.fd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_move_bf_metadata(libBufp, bfap);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_GET_VOL_PARAMS:
        case ADVFS_GET_VOL_PARAMS2: {

            if (parmBufLen > sizeof(libBufp->getVolParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_get_vol_params(libBufp, opIndex);


            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_VOL_BF_DESCS: {
            mlBfDescT *libBfDesc;

            if (parmBufLen != sizeof(libBufp->getVolBfDescs)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libBfDesc = libBufp->getVolBfDescs.bfDesc;
            libBufp->getVolBfDescs.bfDesc = NULL;

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_get_vol_bf_descs(libBufp);

            if (sts == EOK){
                error = ms_copyout (
                       &(libBufp->getVolBfDescs.bfDesc[0]),
                        libBfDesc,
                        libBufp->getVolBfDescs.bfDescCnt * sizeof(bsBfDescT)
                    );
            }
            if (libBufp->getVolBfDescs.bfDesc) {
                    if (do_local)
                            ms_free(libBufp->getVolBfDescs.bfDesc);
                    else
                            free(libBufp->getVolBfDescs.bfDesc, M_NFS);
            }

            if (sts != EOK){
                RAISE_EXCEPTION( sts );
            }

            if (error != 0) {
                RAISE_EXCEPTION( error );
            }

            /** Copyouts **/

            /* Restore user pointer */
            libBufp->getVolBfDescs.bfDesc = libBfDesc;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_SET_VOL_IOQ_PARAMS: {

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->setVolIoQParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_set_vol_ioq_params(libBufp);


            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_GET_BF_XTNT_MAP: {
            vdIndexT volIndex;
            bsExtentDescT *libXtntsArray = NULL;

            if (parmBufLen != sizeof(libBufp->getBfXtntMap)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libXtntsArray = libBufp->getBfXtntMap.xtntsArray;
            libBufp->getBfXtntMap.xtntsArray = NULL;


            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->getBfXtntMap.fd,&fp);
                if (bfap == NULL) {
                    sts = EINVALID_HANDLE;
                }
                else {
                    sts = msfs_syscall_op_get_bf_xtnt_map(libBufp, bfap);
                }
            }

            /** Copyouts **/

            if (sts == EOK) {
                error = ms_copyout( libBufp->getBfXtntMap.xtntsArray,
                                    libXtntsArray,
                                    libBufp->getBfXtntMap.xtntArraySize *
                                        sizeof(bsExtentDescT) );
            }
            if (libBufp->getBfXtntMap.xtntsArray) {
                if (do_local)
                    ms_free( libBufp->getBfXtntMap.xtntsArray );
                else /* xdr allocated this data */
                    free(libBufp->getBfXtntMap.xtntsArray, M_NFS);
            }
            if (sts != EOK){
                RAISE_EXCEPTION(sts);
            }

            if (error != 0) {
                RAISE_EXCEPTION( error );
            }

            /* Restore user pointer */
            libBufp->getBfXtntMap.xtntsArray = libXtntsArray;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_CLUDIO_XTNT_MAP: {
            vdIndexT volIndex;
            bsExtentDescT *libXtntsArray = NULL;

            if (parmBufLen != sizeof(libBufp->getCludioXtntMap)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libXtntsArray = libBufp->getCludioXtntMap.xtntsArray;
            libBufp->getCludioXtntMap.xtntsArray = NULL;


            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->getCludioXtntMap.fd,&fp);
                if (bfap == NULL) {
                    sts = EINVALID_HANDLE;
                }
                else {
                    sts = msfs_syscall_op_get_cludio_xtnt_map(libBufp, bfap);
                }
            }

            /** Copyouts **/

            if (sts == EOK) {
                error = ms_copyout( libBufp->getCludioXtntMap.xtntsArray,
                                    libXtntsArray,
                                    libBufp->getCludioXtntMap.xtntArraySize *
                                        sizeof(bsExtentDescT) );
            }
            if (libBufp->getCludioXtntMap.xtntsArray) {
                if (do_local)
                    ms_free( libBufp->getCludioXtntMap.xtntsArray );
                else /* xdr allocated this data */
                    free(libBufp->getCludioXtntMap.xtntsArray, M_NFS);
            }
            if (sts != EOK){
                RAISE_EXCEPTION(sts);
            }

            if (error != 0) {
                RAISE_EXCEPTION( error );
            }

            /* Restore user pointer */
            libBufp->getCludioXtntMap.xtntsArray = libXtntsArray;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_BKUP_XTNT_MAP: {
            bsExtentDescT *libXtntsArray = NULL;

            if (parmBufLen != sizeof(libBufp->getBkupXtntMap)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libXtntsArray = libBufp->getBkupXtntMap.xtntsArray;
            libBufp->getBkupXtntMap.xtntsArray = NULL;

            /** Operation **/

            /*
             * if we are in a cluster and the operation is executed
             * remotely, the rpc will malloc the memory
             */
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->getBkupXtntMap.fd,&fp);
                if (bfap == NULL) {
                    sts = EINVALID_HANDLE;
                }
                else {
                    sts = msfs_syscall_op_get_bkup_xtnt_map(libBufp, bfap);
                }
            }

            /** Copyouts **/

            if (sts == EOK) {
                error = ms_copyout( libBufp->getBkupXtntMap.xtntsArray,
                                    libXtntsArray,
                                    libBufp->getBkupXtntMap.xtntArraySize *
                                        sizeof(bsExtentDescT) );
            }
            else {
                if (sts == E_NOT_ENOUGH_XTNTS){	
                    /*
                     * Unlike other syscalls, if this one fails
                     * with E_NOT_ENOUGH_XTNTS, it has to return
                     * the new xtntCnt to allow the caller to
                     * correct the arraysize. Unlike v4.x the
                     * returned volIndex has been set up by the
                     * staging routine we called (msfs_syscall_op...)
                     * we pass it back just in case someone needs it
                     */
                    error = ms_copyout (libBufp, parmBuf, parmBufLen);
                    if ( error != 0) {
                        /* most severe error is reported */
                        sts = error;
                    }
                }
            }

            if (libBufp->getBkupXtntMap.xtntsArray) {
                if (do_local)
                        ms_free( libBufp->getBkupXtntMap.xtntsArray );
                else /* allocated by xdr */
                        free( libBufp->getBkupXtntMap.xtntsArray, M_NFS);
            }
            if (sts != 0){
                RAISE_EXCEPTION(sts);
            }
            if (error != 0) {
                RAISE_EXCEPTION( error );
            }

            /* Restore user pointer */
            libBufp->getBkupXtntMap.xtntsArray = libXtntsArray;


            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_LOCK_STATS: {

            if (AdvfsLockStats == NULL) {
                RAISE_EXCEPTION( ENO_SUCH_DOMAIN );
            }

            /** Copyouts **/
            error = ms_copyout((void *)AdvfsLockStats,
                               (void *)parmBuf,
                               MIN(sizeof(advfsLockStatsT), parmBufLen));

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_GET_DMN_PARAMS: {

            if (parmBufLen > sizeof(libBufp->getDmnParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_get_dmn_params(libBufp);


            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_DMNNAME_PARAMS: {
            char *libDomain = NULL;

            if (parmBufLen > sizeof(libBufp->getDmnNameParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->getDmnNameParams.domain;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->getDmnNameParams.domain,
                        NULL,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_get_dmnname_params(libBufp);

 
            free_domain_and_set_names(libBufp->getDmnNameParams.domain,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->getDmnNameParams.domain = libDomain;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_BFSET_PARAMS: {

            if (parmBufLen != sizeof(libBufp->getBfSetParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_get_bfset_params(libBufp);

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_SET_BFSET_PARAMS: {

            if (parmBufLen != sizeof(libBufp->setBfSetParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_set_bfset_params(libBufp);

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SET_BFSET_PARAMS_ACTIVATE: {

            if (parmBufLen != sizeof(libBufp->setBfSetParamsActivate)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                sts = msfs_syscall_op_set_bfset_params_activate(libBufp);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_FSET_GET_INFO: {
            char *libDomain = NULL;

            if (parmBufLen != sizeof(libBufp->setGetInfo)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->setGetInfo.domain;

            error = copyin_domain_and_set_names( libDomain,
                                                 NULL,
                                                 NULL,
                                                 &libBufp->setGetInfo.domain,
                                                 NULL,
                                                 NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_fset_get_info(libBufp);

            free_domain_and_set_names(libBufp->setGetInfo.domain,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->setGetInfo.domain = libDomain;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_FSET_CREATE: {
            char *libSetName = NULL;
            char *libDomain = NULL;

            if (parmBufLen != sizeof(libBufp->setCreate)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->setCreate.domain;
            libSetName = libBufp->setCreate.setName;

            error = copyin_domain_and_set_names( libDomain,
                                                 libSetName,
                                                 NULL,
                                                &libBufp->setCreate.domain,
                                                &libBufp->setCreate.setName,
                                                 NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_fset_create(libBufp, 0, 0);


            free_domain_and_set_names(libBufp->setCreate.domain,
                                      libBufp->setCreate.setName,
                                      NULL);

            /* Restore user pointers */
            libBufp->setCreate.domain = libDomain;
            libBufp->setCreate.setName = libSetName;


            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_FSET_GET_ID: {
            char *libSetName = NULL;
            char *libDomain = NULL;

            if (parmBufLen != sizeof(libBufp->setGetId)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->setGetId.domain;
            libSetName = libBufp->setGetId.setName;

            error = copyin_domain_and_set_names( libDomain,
                                                 libSetName,
                                                 NULL,
                                                 &libBufp->setGetId.domain,
                                                 &libBufp->setGetId.setName,
                                                 NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_fset_get_id(libBufp);

            free_domain_and_set_names(libBufp->setGetId.domain,
                                      libBufp->setGetId.setName,
                                      NULL);

            /* Restore user pointers */
            libBufp->setGetId.domain = libDomain;
            libBufp->setGetId.setName = libSetName;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_FSET_GET_STATS: {
            char *libSetName = NULL;
            char *libDomain = NULL;

            if (parmBufLen > sizeof(libBufp->setGetStats)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->setGetStats.domain;
            libSetName = libBufp->setGetStats.setName;

            error = copyin_domain_and_set_names( libDomain,
                                                 libSetName,
                                                 NULL,
                                                 &libBufp->setGetStats.domain,
                                                 &libBufp->setGetStats.setName,
                                                 NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_fset_get_stats(libBufp);

            free_domain_and_set_names(libBufp->setGetStats.domain,
                                      libBufp->setGetStats.setName,
                                      NULL);

            /* Restore user pointers */
            libBufp->setGetStats.domain = libDomain;
            libBufp->setGetStats.setName = libSetName;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_FSET_DELETE: {
            char *libSetName = NULL;
            char *libDomain = NULL;

            if (parmBufLen != sizeof(libBufp->setDelete)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->setDelete.domain;
            libSetName = libBufp->setDelete.setName;

            error = copyin_domain_and_set_names( libDomain,
                                                 libSetName,
                                                 NULL,
                                                 &libBufp->setDelete.domain,
                                                 &libBufp->setDelete.setName,
                                                 NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_fset_delete(libBufp, 0);

            free_domain_and_set_names(libBufp->setDelete.domain,
                                      libBufp->setDelete.setName,
                                      NULL);

            /* Restore user pointers */
            libBufp->setDelete.domain = libDomain;
            libBufp->setDelete.setName = libSetName;

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_FSET_CLONE: {
            char *libDomain = NULL;
            char *libOrigSetName = NULL;
            char *libCloneSetName = NULL;

            if (parmBufLen != sizeof(libBufp->setClone)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->setClone.domain;
            libOrigSetName = libBufp->setClone.origSetName;
            libCloneSetName = libBufp->setClone.cloneSetName;

            error = copyin_domain_and_set_names(libDomain,
                                                libOrigSetName,
                                                libCloneSetName,
                                               &libBufp->setClone.domain,
                                               &libBufp->setClone.origSetName,
                                               &libBufp->setClone.cloneSetName);
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_fset_clone(libBufp, 0);

            free_domain_and_set_names(libBufp->setClone.domain,
                                      libBufp->setClone.origSetName,
                                      libBufp->setClone.cloneSetName);

            /* Restore user pointers */
            libBufp->setClone.domain = libDomain;
            libBufp->setClone.origSetName = libOrigSetName;
            libBufp->setClone.cloneSetName = libCloneSetName;

            docopyout++;                /* reuse OK */
            break;
       }

       case ADVFS_FSET_RENAME: {
            char *libDomain = NULL;
            char *libOrigSetName = NULL;
            char *libNewSetName = NULL;

            if (parmBufLen != sizeof(libBufp->setRename)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->setRename.domain;
            libOrigSetName = libBufp->setRename.origSetName;
            libNewSetName = libBufp->setRename.newSetName;

            error = copyin_domain_and_set_names( libDomain,
                                                 libOrigSetName,
                                                 libNewSetName,
                                                &libBufp->setRename.domain,
                                                &libBufp->setRename.origSetName,
                                                &libBufp->setRename.newSetName);
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_fset_rename(libBufp, 0);

            free_domain_and_set_names( libBufp->setRename.domain,
                                       libBufp->setRename.origSetName,
                                       libBufp->setRename.newSetName);

            /* Restore user pointers */
            libBufp->setRename.domain = libDomain;
            libBufp->setRename.origSetName = libOrigSetName;
            libBufp->setRename.newSetName = libNewSetName;


            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_DMN_INIT: {
            char *libVolName = NULL;
            char *libDomain = NULL;

            if (!SUSER) {
               RAISE_EXCEPTION( E_ACCESS_DENIED );
            }

            if (parmBufLen != sizeof(libBufp->dmnInit)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->dmnInit.domain;
            libVolName = libBufp->dmnInit.volName;

            error = copyin_domain_and_set_names( libDomain,
                                                 NULL,
                                                 NULL,
                                                &libBufp->dmnInit.domain,
                                                 NULL,
                                                 NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            libBufp->dmnInit.volName = ms_malloc( MAXPATHLEN+1 );

            error = copyinstr( libVolName,
                               libBufp->dmnInit.volName,
                               MAXPATHLEN - 1,
                               &size );
            if (error != 0) {
                ms_free( libBufp->dmnInit.volName );
                free_domain_and_set_names(libBufp->dmnInit.domain, NULL, NULL);
                RAISE_EXCEPTION( error );
            }

            libBufp->dmnInit.volName[size] = '\0';

            /** Operation **/
            sts = msfs_syscall_op_dmn_init(libBufp);

            ms_free( libBufp->dmnInit.volName );
            free_domain_and_set_names( libBufp->dmnInit.domain, NULL, NULL );

            /* Restore user pointers */
            libBufp->dmnInit.domain = libDomain;
            libBufp->dmnInit.volName = libVolName;


            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_DMN_VOL_LIST: {
            uint32T *libVolIndexArray;

            if (parmBufLen != sizeof(libBufp->getDmnVolList)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libVolIndexArray = libBufp->getDmnVolList.volIndexArray;
            libBufp->getDmnVolList.volIndexArray = NULL;


            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts){
                sts = msfs_syscall_op_get_dmn_vol_list(libBufp);
            }

            /** Copyouts **/
            if (sts == EOK) {
                 error = ms_copyout(libBufp->getDmnVolList.volIndexArray,
                               libVolIndexArray,
                               libBufp->getDmnVolList.volIndexArrayLen *
                                   sizeof(uint32T));
            }
            if (libBufp->getDmnVolList.volIndexArray) {
                if (do_local)
                        ms_free( libBufp->getDmnVolList.volIndexArray );
                else {
                    /* if the call was function shipped, then xdr allocated
                     * the memory for us
                     */
                       free( libBufp->getDmnVolList.volIndexArray, M_NFS);
               }
            }

            /* Restore user pointer */
            libBufp->getDmnVolList.volIndexArray = libVolIndexArray;

            if (sts != EOK){
                RAISE_EXCEPTION( sts );
            }
            if (error != 0) {
                RAISE_EXCEPTION( error );
            }

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_MIGRATE: {

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->migrate)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->migrate.fd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_migrate(libBufp, bfap);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_REM_NAME: {
            char *libName = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof( libBufp->removeName )) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libName = libBufp->removeName.name;

            libBufp->removeName.name = ms_malloc( MAXPATHLEN + 1 );

            error = copyinstr( libName,
                               libBufp->removeName.name,
                               MAXPATHLEN - 1,
                               &size );
            if (error != 0) {
                ms_free( libBufp->removeName.name );
                RAISE_EXCEPTION( error );
            }

            libBufp->removeName.name[size] = '\0';

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->removeName.dirFd,&fp);
                if (bfap == NULL) {
                    sts = EINVALID_HANDLE;
                }
                else {
                    sts = msfs_syscall_op_rem_name(libBufp, bfap);
                }
            }

            ms_free( libBufp->removeName.name );

            /* Restore user pointer */
            libBufp->removeName.name = libName;


            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_REM_BF: {

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof( libBufp->removeBf )) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->removeBf.dirFd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_rem_bf(libBufp, bfap);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_TAG_STAT: {
            int bfs_lock = 1;

            if (parmBufLen != sizeof(libBufp->tagStat)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_tag_stat(libBufp);

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_ADD_VOLUME: {
            char *libDomain = NULL;
            char *libVolName = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->addVol)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->addVol.domain;
            libVolName = libBufp->addVol.volName;

            error = copyin_domain_and_set_names(libDomain,
                                                NULL,
                                                NULL,
                                                &libBufp->addVol.domain,
                                                NULL,
                                                NULL);
            if (error != 0) {RAISE_EXCEPTION( error );}

            libBufp->addVol.volName = ms_malloc( MAXPATHLEN+1 );

            error = copyinstr( libVolName,
                               libBufp->addVol.volName,
                               MAXPATHLEN - 1,
                               &size );
            if (error != 0) {
                goto add_vd_exit;
            }
            libBufp->addVol.volName[size] = '\0';


            /** Operation **/

            /* Remote addvol is not supported; this call is to ensure that */
            /* the domain is local, not to function-ship the operation */
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_add_volume(libBufp, opIndex);

        add_vd_exit:
            free_domain_and_set_names(libBufp->addVol.domain, NULL, NULL);

            if (libBufp->addVol.volName != NULL) {
                ms_free( libBufp->addVol.volName );
            }

            /* Restore user pointers */
            libBufp->addVol.domain = libDomain;
            libBufp->addVol.volName = libVolName;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_REM_VOLUME: {

            /*
             * This is a configuration change so the caller must be super user.
             */
            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->remVol)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/

            /* Remote rmvol is not supported; this call is to ensure that */
            /* the domain is local, not to function-ship the operation */
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_rem_volume(libBufp, opIndex);

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SS_SET_LICENSE: {

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->ssSetLicense)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            sts = msfs_syscall_op_ss_set_license(libBufp);

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SS_GET_LICENSE: {

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->ssGetLicense)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            sts = msfs_syscall_op_ss_get_license(libBufp);

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_SS_DMN_OPS: {
            char *libDomain = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->ssDmnOps)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/
            /* Preserve user pointer */
            libDomain = libBufp->ssDmnOps.domainName;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->ssDmnOps.domainName,
                        NULL,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/

            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_ss_dmn_ops(libBufp);

            free_domain_and_set_names(libBufp->ssDmnOps.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->ssDmnOps.domainName = libDomain;

            docopyout++;                 /* reuse OK */
            break;
        }

        case ADVFS_SS_SET_PARAMS: {
            char *libDomain = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->ssSetParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/
            /* Preserve user pointer */
            libDomain = libBufp->ssSetParams.domainName;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->ssSetParams.domainName,
                        NULL,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/

            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_ss_set_params(libBufp);

            free_domain_and_set_names(libBufp->ssSetParams.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->ssSetParams.domainName = libDomain;

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SS_GET_PARAMS: {
            char *libDomain = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->ssGetParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/
            /* Preserve user pointer */
            libDomain = libBufp->ssGetParams.domainName;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->ssGetParams.domainName,
                        NULL,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_ss_get_params(libBufp);

            free_domain_and_set_names(libBufp->ssGetParams.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->ssGetParams.domainName = libDomain;

            docopyout++;
            break;
        }

        case ADVFS_SS_GET_FRAGLIST: {
            mlFragDescT *libFragArray = NULL;
            char *libDomain = NULL;

            if (parmBufLen != sizeof(libBufp->ssGetFraglist)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/
            libFragArray = libBufp->ssGetFraglist.ssFragArray;
            libBufp->ssGetFraglist.ssFragArray = NULL;

            /** Copyins **/
            /* Preserve user pointer */
            libDomain = libBufp->ssGetFraglist.domainName;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->ssGetFraglist.domainName,
                        NULL,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_ss_get_fraglist(libBufp);

            /** Copyouts **/
            if ((sts == EOK) && (libBufp->ssGetFraglist.ssFragCnt > 0)) {
                error = ms_copyout( libBufp->ssGetFraglist.ssFragArray,
                                    libFragArray,
                                    libBufp->ssGetFraglist.ssFragCnt *
                                        sizeof(mlFragDescT) );
            }
            if (libBufp->ssGetFraglist.ssFragArray) {
                if (do_local)
                    ms_free( libBufp->ssGetFraglist.ssFragArray );
                else
                    free(libBufp->ssGetFraglist.ssFragArray, M_NFS);
            }
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }

            if (error != 0) {
                RAISE_EXCEPTION(error);
            }

            free_domain_and_set_names(libBufp->ssGetFraglist.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->ssGetFraglist.domainName = libDomain;
            libBufp->ssGetFraglist.ssFragArray = libFragArray;

            docopyout++;                /* reuse OK */
            break;

        }

        case ADVFS_SS_GET_HOTLIST: {
            mlHotDescT *libHotArray = NULL;

            if (parmBufLen != sizeof(libBufp->ssGetHotlist)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/
            libHotArray = libBufp->ssGetHotlist.ssHotArray;
            libBufp->ssGetHotlist.ssHotArray = NULL;

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_ss_get_hotlist(libBufp);

            /** Copyouts **/
            if ((sts == EOK) &&
                (libBufp->ssGetHotlist.ssHotCnt > 0)) {
                error = ms_copyout( libBufp->ssGetHotlist.ssHotArray,
                                    libHotArray,
                                    libBufp->ssGetHotlist.ssHotCnt *
                                        sizeof(mlHotDescT) );
            }
            if (libBufp->ssGetHotlist.ssHotArray) {
                if (do_local)
                    ms_free( libBufp->ssGetHotlist.ssHotArray );
                else
                    free(libBufp->ssGetHotlist.ssHotArray, M_NFS);
            }
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);
            }

            if (error != 0) {
                RAISE_EXCEPTION(error);
            }

            /* Restore user pointer */
            libBufp->ssGetHotlist.ssHotArray = libHotArray;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_ADD_REM_VOL_SVC_CLASS: {
            char *libDomain = NULL;

            /*
             * This is a configuration change so the caller must be super user.
             */
            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->addRemVolSvcClass)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->addRemVolSvcClass.domainName;

            error = copyin_domain_and_set_names(
                                        libDomain,
                                        NULL,
                                        NULL,
                                        &libBufp->addRemVolSvcClass.domainName,
                                        NULL,
                                        NULL );
            if (error != 0) {RAISE_EXCEPTION( error )};

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_add_rem_vol_svc_class(libBufp);
 
            free_domain_and_set_names(libBufp->addRemVolSvcClass.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->addRemVolSvcClass.domainName = libDomain;

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_ADD_REM_VOL_DONE: {
            char *libDomain = NULL;
            char *libVolName = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->addRemVolDone)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->addRemVolDone.domainName;
            libVolName = libBufp->addRemVolDone.volName;

            error = copyin_domain_and_set_names(
                                            libDomain,
                                            NULL,
                                            NULL,
                                            &libBufp->addRemVolDone.domainName,
                                            NULL,
                                            NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            libBufp->addRemVolDone.volName = ms_malloc( MAXPATHLEN+1 );

            error = copyinstr( libVolName,
                               libBufp->addRemVolDone.volName,
                               MAXPATHLEN - 1,
                               &size );
            if (error != 0) {
                goto add_rem_vol_done_exit;
            }
            libBufp->addRemVolDone.volName[size] = '\0';

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_add_rem_vol_done(libBufp);

add_rem_vol_done_exit:
            free_domain_and_set_names(libBufp->addRemVolDone.domainName,
                                      NULL,
                                      NULL);

            if (libBufp->addRemVolDone.volName != NULL) {
                ms_free( libBufp->addRemVolDone.volName );
            }

            /* Restore user pointers */
            libBufp->addRemVolDone.domainName = libDomain;
            libBufp->addRemVolDone.volName = libVolName;

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SET_BF_NEXT_ALLOC_VOL: {

            if (parmBufLen != sizeof(libBufp->setBfNextAllocVol)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->setBfNextAllocVol.fd,&fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_set_bf_next_alloc_vol(libBufp, bfap);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SWITCH_LOG: {
            char *libDomain = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->switchLog)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->switchLog.domainName;

            error = copyin_domain_and_set_names(libDomain,
                                                NULL,
                                                NULL,
                                                &libBufp->switchLog.domainName,
                                                NULL,
                                                NULL );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_switch_log(libBufp, 0);


            free_domain_and_set_names(libBufp->switchLog.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->switchLog.domainName = libDomain;


            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SWITCH_ROOT_TAGDIR: {
            char *libDomain = NULL;

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->switchRootTagDir)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->switchRootTagDir.domainName;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->switchRootTagDir.domainName,
                        NULL,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_switch_root_tagdir(libBufp);


            free_domain_and_set_names(libBufp->switchRootTagDir.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->switchRootTagDir.domainName = libDomain;


            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_REWRITE_XTNT_MAP: {

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->rewriteXtntMap)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->rewriteXtntMap.fd, &fp);
                if (bfap == NULL) {RAISE_EXCEPTION(EINVALID_HANDLE);}

                sts = msfs_syscall_op_rewrite_xtnt_map(libBufp, bfap, 0);
            }

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_RESET_FREE_SPACE_CACHE: {

            if (!SUSER) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->resetFreeSpaceCache)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_reset_free_space_cache(libBufp);

            /* No copyout */
            break;
        }

        case ADVFS_GET_GLOBAL_STATS: {

            if (parmBufLen != sizeof(libBufp->getGlobalStats)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            sts = msfs_syscall_op_get_global_stats(libBufp);

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_SMSYNC_STATS: {

            if (parmBufLen != sizeof(libBufp->getVolSmsync)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_ID,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = msfs_syscall_op_get_smsync_stats(libBufp);

            if (sts != EOK)
                RAISE_EXCEPTION( sts );

            /** Copyouts **/
            sts = ms_copyout (libBufp, parmBuf, parmBufLen);

            break;
        }


        /*** End of cases ***/

        default: {
            /* no copyout */ /* reuse OK */
            RAISE_EXCEPTION( ENOT_SUPPORTED );
        }

    }  /* end big switch */

    if (sts != EOK) {
        /* ADD and REM VOLUME may have to retry if a node
         * crashes.  Check error code here.  Release any
         * DLM locks if retrying.
         */
        if (clu_is_ready() && (opIndex == ADVFS_ADD_VOLUME || opIndex == ADVFS_REM_VOLUME)
                && sts == EAGAIN)
        {
            if (have_domain_lock) {
                CC_DMN_NAME_DLM_UNLOCK(&cfs_domain_lock);
                have_domain_lock = 0;
            }
            else {
                if (dlm_locked) {
                    CC_CMSDB_DLM_UNLOCK(&cms_cmsdb_lock);
                    dlm_locked = 0;
	        }	
            }

            /* Let's sleep a sec before retrying */
            timeout((void(*)(void *))wakeup, libBufp, hz);
            (void)mpsleep(libBufp, PZERO, (char *)NULL, 0, (void *) NULL, 0);
            goto RETRY;
        }
        RAISE_EXCEPTION( sts );
    }

    if (docopyout)
        error = ms_copyout (libBufp, parmBuf, parmBufLen);

    sts = error;

HANDLE_EXCEPTION:

    if (clu_is_ready()) {
        if (have_domain_lock) {
            CC_DMN_NAME_DLM_UNLOCK(&cfs_domain_lock);
        }
        else {
            if (dlm_locked)
                CC_CMSDB_DLM_UNLOCK(&cms_cmsdb_lock);
        }
    }

    if (bfap != NULL) {
        FP_UNREF_MT (fp, &u.u_file_state);
    }

    if (audswitch)
        msfs_audit_syscall(libBufp, opIndex, sts);

    if (libBufp != NULL) {
        ms_free( libBufp );
    }

    if ( sts == E_DOMAIN_PANIC ) {
        sts = EIO;
    }

    return sts;
}

/***                                                             ***
 *                                                                 *
 *  Little syscall routines.  These functions contain the primary  *
 *  logic of the AdvFS system calls.  They should never be called  *
 *  except by msfs_real_syscall(), and the CFS server code.        * 
 *                                                                 *
 *  Each routine takes an arguments buffer (of type libParamsT, a  *
 *  union of syscall-specific arguments structs).  A few routines  *
 *  also require an operation index (of type opIndexT).            *
 *                                                                 *
 *  In support of CFS, non-idempotent operations will also need a  *
 *  CFS transaction identifier (xid), and operations that require  *
 *  a file descriptor will need the corresponding bfap.            *
 *                                                                 *
 ***                                                             ***/

/* ADVFS_GET_NAME */
mlStatusT
msfs_syscall_op_get_name2(libParamsT *libBufp)
{

    return get_name2(libBufp->getName.mp,
                     libBufp->getName.tag,
                     libBufp->getName.name,
                     &libBufp->getName.parentTag);
}


/* ADVFS_UNDEL_GET */
mlStatusT
msfs_syscall_op_undel_get(libParamsT *libBufp)
{
    return get_undel_dir(libBufp->undelGet.dirName,
                         &libBufp->undelGet.undelDirTag);
}


/* ADVFS_UNDEL_ATTACH */
mlStatusT
msfs_syscall_op_undel_attach(libParamsT *libBufp)
{
    return attach_undel_dir(libBufp->undelAttach.dirName,
                            libBufp->undelAttach.undelDirName);
}


/* ADVFS_UNDEL_DETACH */
mlStatusT
msfs_syscall_op_undel_detach(libParamsT *libBufp, long xid)
{
    return detach_undel_dir(libBufp->undelDetach.dirName, xid);
}


/* ADVFS_EVENT */
mlStatusT
msfs_syscall_op_event(libParamsT *libBufp)
{

    switch (libBufp->event.action) {
        case 0:
            dbg_event_clear();
            break;
        case 1:
            dbg_event_post();
            break;
        default:
            break;
    }

    return EOK;  /* No return value */

}


/* ADVFS_GET_BF_PARAMS, ADVFS_GET_IDX_BF_PARAMS, ADVFS_GET_IDXDIR_BF_PARAMS */
mlStatusT
msfs_syscall_op_get_bf_params(libParamsT *libBufp, 
                              opIndexT opIndex,
                              struct bfAccess *bfap)
{
    bfParamsT *bfparamsp = NULL;
    mlBfAttributesT* bfattrp = &libBufp->getBfParams.bfAttributes;
    mlBfInfoT* bfinfop = &libBufp->getBfParams.bfInfo;
    mlStatusT sts;

    if(opIndex == ADVFS_GET_IDX_BF_PARAMS)
    {
        /* The call is actually to get the bf params for the
         * index file of the passed in directory. Fool this
         * call by replacing the bfap for the directory with
         * the bfap of the index.
         */

        if (IDX_INDEXING_ENABLED(bfap))
        {
            bfap = ((bsDirIdxRecT *)bfap->idx_params)->idx_bfap;
        }
        else
        {
            RAISE_EXCEPTION(EINVALID_HANDLE);  /* Exception */
        }
    }
    else if (opIndex == ADVFS_GET_IDXDIR_BF_PARAMS) {
        /*
         * The call is actually to get the bf params for the
         * directory of the index file passed in.  Fool this
         * call by replacing bfap for the index file with
         * the bfap of the directory.
         */
        if (IDX_INDEXING_ENABLED(bfap) && !IDX_FILE_IS_DIRECTORY(bfap)) {
            bfap = ((bsIdxRecT *)bfap->idx_params)->dir_bfap;
        }
        else {
            RAISE_EXCEPTION(EINVALID_HANDLE);  /* Exception */
        }
    }

    bfparamsp = (bfParamsT *) ms_malloc(sizeof(bfParamsT));

    sts = bs_get_bf_params(bfap, bfparamsp, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    /* bfattrp points to stuff we copied in from the user.
       no real concern on object reuse, since things we don't touch
       retain the user-supplied values upon copyout below. */

    /*
     * copy fields out into respective structures.
     */
    bfattrp->version = 1;
    bfattrp->dataSafety = (mlBfDataSafetyT)bfparamsp->cl.dataSafety;
    bfattrp->reqService = (mlServiceClassT)bfparamsp->cl.reqServices;
    bfattrp->optService = (mlServiceClassT)bfparamsp->cl.optServices;

    if ( bfparamsp->numPages == bfparamsp->nextPage ) {
        bfattrp->initialSize = bfparamsp->pageSize*bfparamsp->numPages;
    } else {
        bfattrp->initialSize = 0;
    }

    bfattrp->extendSize = bfparamsp->cl.extendSize;

    bcopy( bfparamsp->cl.clientArea,
           bfattrp->clientArea,
           sizeof(bfattrp->clientArea) );

    /* Note: the union (bfattrp->attr) is not always completely
       filled in here. */

    switch ( bfparamsp->type ) {
        case BSXMT_APPEND: {
            bfattrp->mapType = XMT_SIMPLE;
            break;
        }
        case BSXMT_STRIPE: {
            bfattrp->mapType = XMT_STRIPE;
            bfattrp->attr.stripe.segmentCnt = bfparamsp->segmentCnt;
            bfattrp->attr.stripe.segmentSize = bfparamsp->segmentSize;
            break;
        }
        default: {
            bfattrp->mapType = XMT_NIL;
        }
    }


    /* acl, rsvd_sec{1,2,3,4}, fill{2} are not filled in here. */

    /* this set covers the entirety of *bfinfop */
    bfinfop->pageSize = bfparamsp->pageSize;
    bfinfop->numPages = bfparamsp->numPages;
    bfinfop->nextPage = bfparamsp->nextPage;
    bfinfop->fragOff  = bfparamsp->fragPageOffset;
    bfinfop->volIndex = bfparamsp->vdIndex;
    bfinfop->tag = *(mlBfTagT*)&bfparamsp->tag;
    bfinfop->cloneId = bfparamsp->cloneId;
    bfinfop->cloneCnt = bfparamsp->cloneCnt;
    bfinfop->fragId.type = bfparamsp->fragId.type;
    bfinfop->fragId.frag = bfparamsp->fragId.frag;

HANDLE_EXCEPTION:

    if (bfparamsp)
       ms_free (bfparamsp);

    return(sts);

}


/* ADVFS_SET_BF_ATTRIBUTES */
mlStatusT
msfs_syscall_op_set_bf_attributes(libParamsT *libBufp,
                                  struct bfAccess *bfap,
                                  long xid)
{
    bfParamsT *bfparamsp = NULL;
    mlBfAttributesT* bfattrp = &libBufp->setBfAttributes.bfAttributes;
    int error;
    mlStatusT sts;


    /* must be file's owner or super user */

    error = check_privilege( bfap, bfattrp );
    if (error != 0) {
        RAISE_EXCEPTION(error);  /* Exception */
    }

    bfparamsp = (bfParamsT *) ms_malloc(sizeof(bfParamsT));

    sts = bs_get_bf_params(bfap, bfparamsp, 0);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    bfparamsp->cl.dataSafety = (bfDataSafetyT)bfattrp->dataSafety;
    bfparamsp->cl.reqServices = (serviceClassT)bfattrp->reqService;
    bfparamsp->cl.optServices = (serviceClassT)bfattrp->optService;
    bfparamsp->cl.extendSize = bfattrp->extendSize;

    sts = bs_set_bf_params(bfap, bfparamsp);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    switch ( bfattrp->mapType ) {
    case XMT_SIMPLE:
        if ( bfparamsp->type != BSXMT_APPEND ) {
            RAISE_EXCEPTION(ENOT_SUPPORTED);  /* Exception */
        }
        break;

    case XMT_STRIPE:
        sts = bs_stripe( bfap,
                         bfattrp->attr.stripe.segmentCnt,
                         bfattrp->attr.stripe.segmentSize,
                         xid );
        break;

    default:
        RAISE_EXCEPTION(EBAD_PARAMS);  /* Exception */
    }

HANDLE_EXCEPTION:

    if (bfparamsp)
       ms_free (bfparamsp);

    return(sts);

}


/* ADVFS_GET_BF_IATTRIBUTES */
mlStatusT
msfs_syscall_op_get_bf_iattributes(libParamsT *libBufp, struct bfAccess *bfap)
{
    bfIParamsT *bfiparamsp = NULL;
    mlBfAttributesT* bfattrp = &libBufp->setBfIAttributes.bfAttributes;
    mlStatusT sts;

    bfiparamsp = (bfIParamsT *) ms_malloc(sizeof(bfIParamsT));

    sts = bs_get_bf_iparams(bfap, bfiparamsp, 0);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    /* bfattrp points to stuff we copied in from the user.
       no real concern on object reuse, since things we don't touch
       retain the user-supplied values upon copyout below. */

    /*
     * copy fields out into respective structures.
     */
    bfattrp->version = 1;
    bfattrp->dataSafety = (mlBfDataSafetyT)bfiparamsp->dataSafety;
    bfattrp->reqService = (mlServiceClassT)bfiparamsp->reqServices;
    bfattrp->optService = (mlServiceClassT)bfiparamsp->optServices;
    bfattrp->initialSize = 0;
    bfattrp->extendSize = bfiparamsp->extendSize;

    bcopy( bfiparamsp->clientArea,
           bfattrp->clientArea,
           sizeof( bfattrp->clientArea ) );

HANDLE_EXCEPTION:

    if (bfiparamsp)
       ms_free (bfiparamsp);

    return(sts);

}


/* ADVFS_SET_BF_IATTRIBUTES */
mlStatusT
msfs_syscall_op_set_bf_iattributes(libParamsT *libBufp, struct bfAccess *bfap)
{
    bfIParamsT *bfiparamsp = NULL;
    mlBfAttributesT* bfattrp = &libBufp->setBfIAttributes.bfAttributes;
    int error;
    mlStatusT sts;


    /* must be file's owner or super user */

    error = check_privilege( bfap, bfattrp );
    if (error != 0) {
        RAISE_EXCEPTION(error);  /* Exception */
    }

    bfiparamsp = (bfIParamsT *) ms_malloc(sizeof(bfIParamsT));

    sts = bs_get_bf_iparams(bfap, bfiparamsp, 0);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    bfiparamsp->dataSafety = (bfDataSafetyT)bfattrp->dataSafety;
    bfiparamsp->reqServices = (serviceClassT)bfattrp->reqService;
    bfiparamsp->optServices = (serviceClassT)bfattrp->optService;
    bfiparamsp->extendSize = bfattrp->extendSize;

    sts = bs_set_bf_iparams(bfap, bfiparamsp);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

HANDLE_EXCEPTION:

    if (bfiparamsp)
       ms_free (bfiparamsp);

    return(sts);

}


/* ADVFS_MOVE_BF_METADATA */
mlStatusT
msfs_syscall_op_move_bf_metadata(libParamsT *libBufp, struct bfAccess *bfap)
{
    mlStatusT sts;

    sts = bs_move_metadata(bfap,NULL);

    return sts;
}


/* ADVFS_GET_VOL_PARAMS, ADVFS_GET_VOL_PARAMS2 */
mlStatusT
msfs_syscall_op_get_vol_params(libParamsT *libBufp, opIndexT opIndex)
{
    domainT *dmnP;
    bsVdParamsT *vdparamsp = NULL;
    int i, dmn_ref=FALSE;
    mlStatusT sts;

    mlVolInfoT* vinfop = &libBufp->getVolParams.volInfo;
    mlVolPropertiesT* vpropp = &libBufp->getVolParams.volProperties;
    mlVolIoQParamsT* vioqp = &libBufp->getVolParams.volIoQParams;
    mlVolCountersT* vcntp = &libBufp->getVolParams.volCounters;
    int *flagp = &libBufp->getVolParams.flag;

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->getVolParams.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmn_ref=TRUE;

    vdparamsp = (bsVdParamsT *) ms_malloc(sizeof(bsVdParamsT));

    /*
     * Fetch ioq stats which allow counting # bufs per interval.
     * ADVFS_GET_VOL_PARAMS returns just a snapshot.
     */
    if (opIndex == ADVFS_GET_VOL_PARAMS2) *flagp |= GETVOLPARAMS2;

    sts = bs_get_vd_params(dmnP,
                           libBufp->getVolParams.volIndex,
                           vdparamsp,
                           (*flagp & (GETVOLPARAMS_NO_BMT |
                                      GETVOLPARAMS2 |
                                      GETVOLPARAMS2_CHVOL_OP_A)) );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    close_domain(libBufp->getVolParams.bfDomainId, dmnP, 0);
    dmn_ref=FALSE;

    /*
     * Copy the fields from the internal format to the library
     * format.
     */

    /* these cover the entirety of *vinfop
       If GETVOLPARAMS_NO_BMT is specified, the bsVdAttr struct
       (vdparamsp) will not get loaded with bmt data.
    */
    if (!(*flagp&GETVOLPARAMS_NO_BMT)) {
        vinfop->vdMntId = vdparamsp->vdMntId;
        vinfop->volIndex = vdparamsp->vdIndex;
        vinfop->volSize = vdparamsp->vdSize;
        vinfop->stgCluster = vdparamsp->stgCluster;
        vinfop->freeClusters = vdparamsp->freeClusters;
        vinfop->totalClusters = vdparamsp->totalClusters;
        vinfop->maxPgSz = vdparamsp->maxPgSz;
    }

    /* these cover the entirety of *vpropp */
    vpropp->serviceClass = vdparamsp->serviceClass;
    vpropp->bmtXtntPgs = vdparamsp->bmtXtntPgs;

    for (i = 0; i < ML_VOL_NAME_SZ; i += 1) {
        vpropp->volName[i] = vdparamsp->vdName[i];
    }

    /* these cover the entirety of *vioq */
    vioqp->rdMaxIo = vdparamsp->rdMaxIo;
    vioqp->wrMaxIo = vdparamsp->wrMaxIo;
    vioqp->qtoDev = vdparamsp->qtoDev;
    vioqp->lazyThresh = vdparamsp->lazyThresh;
    vioqp->consolidate = vdparamsp->consolidate;
    vioqp->blockingFact = vdparamsp->blockingFact;
    vioqp->max_iosize_rd = vdparamsp->max_iosize_rd;
    vioqp->max_iosize_wr = vdparamsp->max_iosize_wr;
    vioqp->preferred_iosize_rd = vdparamsp->preferred_iosize_rd;
    vioqp->preferred_iosize_wr = vdparamsp->preferred_iosize_wr;

    *vcntp = *(mlVolCountersT*)(&vdparamsp->dStat);

HANDLE_EXCEPTION:

    if (dmn_ref)
        close_domain(libBufp->getVolParams.bfDomainId, dmnP, 0);

    if (vdparamsp)
        ms_free (vdparamsp);

    return(sts);
}


/* ADVFS_GET_VOL_BF_DESCS */
mlStatusT
msfs_syscall_op_get_vol_bf_descs(libParamsT *libBufp)
{
    domainT *dmnP;
    mlStatusT sts;
    vdT* vdp;

    libBufp->getVolBfDescs.bfDesc = (mlBfDescT *)ms_malloc(
                    libBufp->getVolBfDescs.bfDescSize * sizeof(bsBfDescT));

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->getVolBfDescs.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    /* Check the disk */
    {
        vdIndexT vdi = libBufp->getVolBfDescs.volIndex;
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            close_domain (libBufp->getVolBfDescs.bfDomainId, dmnP, 0);
            return(EBAD_VDI);  /* Exception */
        }
    }

    sts = bmt_get_vd_bf_descs (dmnP,
                               libBufp->getVolBfDescs.volIndex,
                               libBufp->getVolBfDescs.bfDescSize,
                  (bsBfDescT *)&(libBufp->getVolBfDescs.bfDesc[0]),
                    (bfMCIdT *)&(libBufp->getVolBfDescs.nextBfDescId),
                               &(libBufp->getVolBfDescs.bfDescCnt) );

    /* Decrement the vdRefCnt on the vdT struct */
    vd_dec_refcnt( vdp );

    close_domain (libBufp->getVolBfDescs.bfDomainId, dmnP, 0);

    return(sts);
}


/* ADVFS_SET_VOL_IOQ_PARAMS */
mlStatusT
msfs_syscall_op_set_vol_ioq_params(libParamsT *libBufp)
{
    domainT *dmnP;
    bsVdParamsT *vdparamsp = NULL;
    mlVolIoQParamsT* vioqp = &libBufp->setVolIoQParams.volIoQParams;
    int dmn_ref=FALSE;
    mlStatusT sts;

    sts = open_domain(BS_ACC_WRITE,
                      OWNER_ONLY,
                      libBufp->setVolIoQParams.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmn_ref=TRUE;

    vdparamsp = (bsVdParamsT *) ms_malloc(sizeof(bsVdParamsT));

    sts = bs_get_vd_params(dmnP,
                           libBufp->setVolIoQParams.volIndex,
                           vdparamsp,
                           0);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    /*
     * bs_set_vd_params only modifies the ioq params.
     */

    vdparamsp->rdMaxIo = vioqp->rdMaxIo;
    vdparamsp->wrMaxIo = vioqp->wrMaxIo;
    vdparamsp->qtoDev = vioqp->qtoDev;
    vdparamsp->lazyThresh = vioqp->lazyThresh;
    vdparamsp->consolidate = vioqp->consolidate;
    vdparamsp->blockingFact = vioqp->blockingFact;

    sts = bs_set_vd_params(dmnP,
                           libBufp->setVolIoQParams.volIndex,
                           vdparamsp);

HANDLE_EXCEPTION:

    if (dmn_ref)
        close_domain(libBufp->setVolIoQParams.bfDomainId, dmnP, 0);

    if (vdparamsp)
        ms_free (vdparamsp);

    return(sts);
}


/* ADVFS_GET_BF_XTNT_MAP */
mlStatusT
msfs_syscall_op_get_bf_xtnt_map(libParamsT *libBufp, struct bfAccess *bfap)
{
    vdIndexT volIndex;
    mlStatusT sts;
    struct bfAccess *fromBfap=bfap;

    /*
     * If the caller only wants a count of extents, don't
     * bother allocating an array to hold them.
     */
    if (libBufp->getBfXtntMap.xtntArraySize != 0) {
        libBufp->getBfXtntMap.xtntsArray = (bsExtentDescT *)ms_malloc(
                                            libBufp->getBfXtntMap.xtntArraySize
                                            * sizeof(bsExtentDescT));
    } else {
        libBufp->getBfXtntMap.xtntsArray = NULL;
    }

    if ((bfap->bfSetp->cloneId != BS_BFSET_ORIG) &&
        (bfap->cloneId == 0)){
        
        /* This is a clone that has not had any metadata
         * (or file pages) COWed. At open time the original's
         * extents were loaded into it's (the clone's) in memory extent map.
         * This is basically a bug (to be addressed at some future
         * time). We can not trust these extents since the original
         * could have been migrated (which does not cause a COW).
         * So we need to use the original's bfap not the clone's
         */
         
        fromBfap=bfap->origAccp;
        MS_SMP_ASSERT(fromBfap);
    }

    sts = bs_get_bf_xtnt_map (fromBfap,
                              libBufp->getBfXtntMap.startXtntMap,
                              libBufp->getBfXtntMap.startXtnt,
                              libBufp->getBfXtntMap.xtntArraySize,
                              libBufp->getBfXtntMap.xtntsArray,
                              &libBufp->getBfXtntMap.xtntCnt,
                              &volIndex);

    libBufp->getBfXtntMap.allocVolIndex = volIndex;

    return(sts);
}

/* ADVFS_GET_CLUDIO_XTNT_MAP */
mlStatusT
msfs_syscall_op_get_cludio_xtnt_map(libParamsT *libBufp, struct bfAccess *bfap)
{
    vdIndexT volIndex;
    mlStatusT sts;
    int vdCnt, vdi;
    int pageCnt, options = 0;

    /*
     * If the caller only wants a count of extents, don't
     * bother allocating an array to hold them.
     */
    if (libBufp->getCludioXtntMap.xtntArraySize != 0) {
        libBufp->getCludioXtntMap.xtntsArray = (bsExtentDescT *)
            ms_malloc( libBufp->getCludioXtntMap.xtntArraySize *
                       sizeof(bsExtentDescT) );
    } else {
        libBufp->getCludioXtntMap.xtntsArray = NULL;
    }

    if (libBufp->getCludioXtntMap.xtntsArray == NULL)
        options =  XTNT_XTNTCNT_ONLY;
    else
        options =   XTNT_XTNTMAP;
    
    /* This will do the right thing whether we are operating
     * on the original file or a clone.
     */
    sts = bs_get_bkup_xtnt_map(bfap,
                               libBufp->getCludioXtntMap.startXtntMap,
                               libBufp->getCludioXtntMap.startXtnt,
                               libBufp->getCludioXtntMap.xtntArraySize,
                               options,
                               libBufp->getCludioXtntMap.xtntsArray,
                               &libBufp->getCludioXtntMap.xtntCnt,
                               &volIndex,
                               &pageCnt);
    
    libBufp->getCludioXtntMap.allocVolIndex = volIndex;
    
    /* Up to here, this is a similar operation to ADVFS_GET_BF_XTNT_MAP.
     * However, we have some additional fields to fill in.
     */
    libBufp->getCludioXtntMap.pg_size   = ADVFS_PGSZ;
    libBufp->getCludioXtntMap.file_size = bfap->file_size;
    libBufp->getCludioXtntMap.has_frag  = (bfap->fragState == FS_FRAG_VALID) ?
        TRUE : FALSE;
    /*
     * If we only wanted counts of extents, then 
     * we also only want the max volumes per domain value
     * the actual volume devt list
     */
    
    /*
     * Get count of dev_t's
     */
    mutex_lock( &bfap->dmnP->vdpTblLock );
    vdCnt = 0;
    for ( vdi = 0; vdCnt < bfap->dmnP->vdCnt && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfap->dmnP->vdpTbl[vdi] )
            vdCnt++;
    }
    mutex_unlock( &bfap->dmnP->vdpTblLock );

    libBufp->getCludioXtntMap.volMax = vdi;
    
    if (libBufp->getCludioXtntMap.xtntArraySize == 0) {
        goto _exit;
    }

    libBufp->getCludioXtntMap.devtvec = (bsDevtListT *)
        ms_malloc(sizeof(bsDevtListT));

    /* We need to protect against a volume being removed and
     * referencing bad data in the vd structure.
     * CFS is holding the xtnt map token on the file who's extents we
     * just obtained. If we race rmvol, either
     *
     *  1. rmvol is waiting to
     *     get the xtnt map token so it can migrate this file off the volume,
     *
     *  2. Rmvol already moved this file off the volume so even if
     *     we obtain vd info before rmvol finishes, we know that 
     *     none of the extents in this file live on the vd being
     *     removed.
     *  3. This file does not live on the vd being removed so
     *     neither do its extents.
     *
     * So, the only thing we need to protect against is a bad
     * dereference of the vd structure. The vdpTblLock protects
     * the vdp from going away and the state of BSR_VD_MOUNTED
     * (which can't change while we hold the vdStateLock) insures
     * the vdp->devVp->v_rdev deref is safe.
     */

    mutex_lock( &bfap->dmnP->vdpTblLock );
    vdCnt = 0;
    for ( vdi = 0; vdCnt < bfap->dmnP->vdCnt && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfap->dmnP->vdpTbl[vdi] )
	    libBufp->getCludioXtntMap.volMax = vdi + 1;

	if ( VDI_IS_VALID(vdi+1, bfap->dmnP) ) {
            vdT *vdp = bfap->dmnP->vdpTbl[vdi];
            MS_SMP_ASSERT( vdp->vdMagic == VDMAGIC );
            mutex_lock( &vdp->vdStateLock );
            if (vdp->vdState == BSR_VD_MOUNTED)
            {
                libBufp->getCludioXtntMap.devtvec->device[vdi] = 
                    vdp->devVp->v_rdev;
                vdCnt++;
            } else
                libBufp->getCludioXtntMap.devtvec->device[vdi] = 0;
            mutex_unlock( &vdp->vdStateLock );
        }
    }
    mutex_unlock( &bfap->dmnP->vdpTblLock );
    libBufp->getCludioXtntMap.devtvec->devtCnt = vdCnt;

_exit:
    return(sts);
}


/* ADVFS_GET_BKUP_XTNT_MAP */
mlStatusT
msfs_syscall_op_get_bkup_xtnt_map(libParamsT *libBufp, struct bfAccess *bfap)
{
    vdIndexT volIndex;
    mlStatusT sts;

    /*
     * If the caller only wants a count of extents, don't
     * bother allocating an array to hold them.
     */
    if (libBufp->getBkupXtntMap.xtntArraySize != 0) {
        libBufp->getBkupXtntMap.xtntsArray = (bsExtentDescT *)ms_malloc(
                                          libBufp->getBkupXtntMap.xtntArraySize
                                          * sizeof(bsExtentDescT));
    } else {
        libBufp->getBkupXtntMap.xtntsArray = NULL;
    }

    sts = bs_get_bkup_xtnt_map (bfap,
                                libBufp->getBkupXtntMap.startXtntMap,
                                libBufp->getBkupXtntMap.startXtnt,
                                libBufp->getBkupXtntMap.xtntArraySize,
                                libBufp->getBkupXtntMap.xtntArraySize ?
                                    XTNT_XTNTMAP : XTNT_XTNTCNT_ONLY,
                                libBufp->getBkupXtntMap.xtntsArray,
                                &libBufp->getBkupXtntMap.xtntCnt,
                                &volIndex,
                                &libBufp->getBkupXtntMap.num_pages);

    libBufp->getBkupXtntMap.allocVolIndex = volIndex;

    return(sts);
}


/* ADVFS_GET_DMN_PARAMS */
mlStatusT
msfs_syscall_op_get_dmn_params(libParamsT *libBufp)
{
    domainT *dmnP;
    mlStatusT sts;

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->getDmnParams.bfDomainId,
                      &dmnP);

    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    /* zeros & fills in entirety of dmnParams: */
    sts = bs_get_dmn_params(
              dmnP,
              (bfDmnParamsT *)&libBufp->getDmnParams.dmnParams,
              0
          );

    if ( sts == EOK ) {
        extern statusT getLogStats(domainT *dmnP, logStatT *logStatp);

        sts = getLogStats(dmnP, &libBufp->getDmnParams.dmnParams.mlLogStat);
    }

    close_domain(libBufp->getDmnParams.bfDomainId, dmnP, 0);

    return(sts);
}


/* ADVFS_GET_DMNNAME_PARAMS */
mlStatusT
msfs_syscall_op_get_dmnname_params(libParamsT *libBufp)
{

#ifdef ESS
    int ess_error = 0;
    int rootdmnmatch = 0;
    domainT *essdmnP;
    bfDomainIdT rootdmnID, localdmnID;
    struct mount *pmp = NULL;
#endif
    mlDmnParamsT *mlDmnParamsp = &libBufp->getDmnNameParams.dmnParams;
    int error;
    mlStatusT sts;


    /* zeros & fills in entirety of dmnParams: */
    sts = bs_get_dmntbl_params(libBufp->getDmnNameParams.domain,
                               (bfDmnParamsT *)mlDmnParamsp);

/* ESS event:DomainPanicEss - generate a domain_panic on a non-root fs */
#ifdef ESS
    /* We have to check that the domain we select to panic is not
    ** an advfs root domain. If the root fs is an advfs and its
    ** domain ID matches the domain ID specified in dmnParams, then
    ** we will not execute ESS domain_panic event.
    */
    if (clu_is_ready()) {
        struct mount *pmp = NULL;
        extern struct mount *bootfs;
        error = CC_GETPFSMP(rootfs, &pmp);
        if (!error) {
            if (pmp->m_stat.f_type == MOUNT_MSFS) {
                rootdmnID = ((struct fileSetNode *)pmp->m_info)->bfSetId.domainId;
                localdmnID = mlDmnParamsp->bfDomainId;
                if (BS_UID_EQL(rootdmnID,localdmnID))
                    rootdmnmatch = 1;
            }
        }
        error = CC_GETPFSMP(bootfs, &pmp);
        if (!error) {
            if (pmp->m_stat.f_type == MOUNT_MSFS) {
                rootdmnID = ((struct fileSetNode *)pmp->m_info)->bfSetId.domainId;
                localdmnID = mlDmnParamsp->bfDomainId;
                if (BS_UID_EQL(rootdmnID,localdmnID))
                    rootdmnmatch = 1;
            }
        }
    }

    if (rootfs->m_stat.f_type == MOUNT_MSFS) {
        rootdmnID = ((struct fileSetNode *)rootfs->m_info)->bfSetId.domainId;
        localdmnID = mlDmnParamsp->bfDomainId;
        if (BS_UID_EQL(rootdmnID,localdmnID))
            rootdmnmatch = 1;
    }

    if (!rootdmnmatch) {
        ESS_MACRO_2(AdvfsError,
                    DomainPanicEss,
                    libBufp->getDmnNameParams.domain,
                    &ess_error);
        if (ess_error != 0) {
           DMNTBL_LOCK_WRITE( &DmnTblLock )
           if (essdmnP = domain_name_lookup(libBufp->getDmnNameParams.domain,0))
               domain_panic (essdmnP, "ESS generated domain_panic");
           DMNTBL_UNLOCK( &DmnTblLock )
        }
    }
#endif

    return(sts);
}


/* ADVFS_GET_BFSET_PARAMS */
mlStatusT
msfs_syscall_op_get_bfset_params(libParamsT *libBufp)
{
    domainT *dmnP;
    bfSetT *bfSetp;
    mlStatusT sts;

    sts = open_set( BS_ACC_READ, BS_ACC_READ,
                    ANYONE, ANYONE,
                    libBufp->getBfSetParams.bfSetId,
                    &bfSetp, &dmnP);

    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    /* zeros out & then provides appropriate filling for .bfSetParams: */
    sts = bs_get_bfset_params(bfSetp, &libBufp->getBfSetParams.bfSetParams, 0);

    close_set(libBufp->getBfSetParams.bfSetId, bfSetp, dmnP);

    return(sts);
}


/* ADVFS_SET_BFSET_PARAMS */
mlStatusT
msfs_syscall_op_set_bfset_params(libParamsT *libBufp)
{

    domainT *dmnP;
    bfSetT *bfSetp;
    mlBfSetParamsT *mlBfSetParamsp = &libBufp->setBfSetParams.bfSetParams;
    struct mount *pmp = NULL;
    int clear_flag = 0;
    mlStatusT sts;

    sts = open_set( BS_ACC_WRITE, BS_ACC_READ,
                    ANYONE, OWNER_ONLY,
                    libBufp->setBfSetParams.bfSetId,
                    &bfSetp, &dmnP);

    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    /*
     * inform the cfs server that disk quota of the
     * fileset is going to be decreased.
     */

    if (clu_is_ready()) {

        BFSETTBL_LOCK_READ( bfSetp->dmnP )

        if (bfSetp->fsnp != NULL) {
           struct fileSetNode *fsnp = bfSetp->fsnp;
           if ((fsnp->blkHLimit > mlBfSetParamsp->blkHLimit) ||
               (fsnp->blkSLimit > mlBfSetParamsp->blkSLimit) ||
               ((fsnp->blkSLimit == 0) && (mlBfSetParamsp->blkSLimit > 0)) ||
               ((fsnp->blkHLimit == 0) && (mlBfSetParamsp->blkHLimit > 0)) ) {

                pmp = fsnp->mountp;
                /* To prevent a race with unmount,
                 * try to get unmount lock.  failed
                 * means that umount is happening,
                 * then don't bother flushing.
                 */
                if ((pmp != NULL) &&
                    (UNMOUNT_TRY_READ_NOSLP(pmp))) {
                        BFSETTBL_UNLOCK( bfSetp->dmnP );
                        clear_flag = 1;
                        CC_FSFLUSH(pmp, CHFSET);
                }
            }
        }
        if (!clear_flag)
            BFSETTBL_UNLOCK( bfSetp->dmnP );
    }

    sts = bs_set_bfset_params(bfSetp, mlBfSetParamsp, 0);

    /*
     * Inform the cfs server that quota change is done.
     */
    if (clu_is_ready() && clear_flag) {
        CC_FSFLUSH(pmp, C_DONE);
        UNMOUNT_READ_UNLOCK(pmp);
    }

    close_set(libBufp->setBfSetParams.bfSetId, bfSetp, dmnP);

    return(sts);
}

/* ADVFS_SET_BFSET_PARAMS_ACTIVATE */
mlStatusT
msfs_syscall_op_set_bfset_params_activate(libParamsT *libBufp)
{

    domainT *dmnP;
    bfSetT *bfSetp;
    bfSetIdT bfSetId;
    mlBfSetParamsT *mlBfSetParamsp =
                        &libBufp->setBfSetParamsActivate.bfSetParams;
    struct mount *pmp = NULL;
    int clear_flag = 0;
    mlStatusT sts=0, sts2=0;

    sts = bs_bfset_activate(
                   libBufp->setBfSetParamsActivate.dmnName,
                   libBufp->setBfSetParamsActivate.bfSetParams.setName,
                   0,&bfSetId );

    if (sts != EOK) {
        goto _out;
    }

    sts = rbf_bfs_access( &bfSetp, bfSetId, FtxNilFtxH );
    if (sts != EOK) {
        goto _out;
    }

    /*
     * inform the cfs server that disk quota of the
     * fileset is going to be decreased.
     */

    if (clu_is_ready()) {

        BFSETTBL_LOCK_READ( bfSetp->dmnP )

        if (bfSetp->fsnp != NULL) {
           struct fileSetNode *fsnp = bfSetp->fsnp;
           if ((fsnp->blkHLimit > mlBfSetParamsp->blkHLimit) ||
               (fsnp->blkSLimit > mlBfSetParamsp->blkSLimit) ||
               ((fsnp->blkSLimit == 0) && (mlBfSetParamsp->blkSLimit > 0)) ||
               ((fsnp->blkHLimit == 0) && (mlBfSetParamsp->blkHLimit > 0)) ) {

                pmp = fsnp->mountp;
                /* To prevent a race with unmount,
                 * try to get unmount lock.  failed
                 * means that umount is happening,
                 * then don't bother flushing.
                 */
                if ((pmp != NULL) &&
                    (UNMOUNT_TRY_READ_NOSLP(pmp))) {
                        BFSETTBL_UNLOCK( bfSetp->dmnP );
                        clear_flag = 1;
                        CC_FSFLUSH(pmp, CHFSET);
                }
            }
        }
        if (!clear_flag)
            BFSETTBL_UNLOCK( bfSetp->dmnP );
    }

    sts = bs_set_bfset_params(bfSetp, mlBfSetParamsp, 0);

    /*
     * Inform the cfs server that quota change is done.
     */
    if (clu_is_ready() && clear_flag) {
        CC_FSFLUSH(pmp, C_DONE);
        UNMOUNT_READ_UNLOCK(pmp);
    }

    bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);

    sts2 = bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    if (sts2 != EOK) {
        sts = sts2;
        goto _out;
    }

_out:
    return(sts);
}



/* ADVFS_FSET_GET_INFO */
mlStatusT
msfs_syscall_op_fset_get_info(libParamsT *libBufp)
{
    return fs_fset_get_info(libBufp->setGetInfo.domain,
                            &libBufp->setGetInfo.nextSetIdx,
                            &libBufp->setGetInfo.bfSetParams,
                            &libBufp->setGetInfo.userId,
                            libBufp->setGetInfo.flag);
}


/* ADVFS_FSET_CREATE */
mlStatusT
msfs_syscall_op_fset_create(libParamsT *libBufp,
                            long xid1,
                            long xid2)
{
    advfs_ev *advfs_event;
    mlStatusT sts;

    sts = fs_fset_create(libBufp->setCreate.domain,
                         libBufp->setCreate.setName,
                         libBufp->setCreate.reqServ,
                         libBufp->setCreate.optServ,
                         libBufp->setCreate.fsetOptions,
                         libBufp->setCreate.quotaId,
                         &libBufp->setCreate.bfSetId,
                         xid1,
                         xid2);

    if (sts == EOK) {
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = libBufp->setCreate.domain;
        advfs_event->fileset = libBufp->setCreate.setName;
        advfs_post_kernel_event(EVENT_FSET_MK, advfs_event);
        ms_free(advfs_event);
    }

    return(sts);
}


/* ADVFS_FSET_GET_ID */
mlStatusT
msfs_syscall_op_fset_get_id(libParamsT *libBufp)
{
    return fs_fset_get_id(libBufp->setGetId.domain,
                          libBufp->setGetId.setName,
                          &libBufp->setGetId.bfSetId);
}


/* ADVFS_FSET_GET_STATS */
mlStatusT
msfs_syscall_op_fset_get_stats(libParamsT *libBufp)
{
    return fs_fset_get_stats(
               libBufp->setGetStats.domain,
               libBufp->setGetStats.setName,
               (fileSetStatsT *)&libBufp->setGetStats.fileSetStats
           );
}


/* ADVFS_FSET_DELETE */
mlStatusT
msfs_syscall_op_fset_delete(libParamsT *libBufp, long xid)
{
    mlStatusT sts;

    sts = fs_fset_delete( libBufp->setDelete.domain,
                          libBufp->setDelete.setName,
                          xid );

    return(sts);
}


/* ADVFS_FSET_CLONE */
mlStatusT
msfs_syscall_op_fset_clone(libParamsT *libBufp, long xid)
{
    int size;
    advfs_ev *advfs_event;
    mlStatusT sts;

    /*
     * inform the cfs server that clonefset is happening.
     * the server should ask all its clients to flush
     * data back to keep PFS up to date.
     */

    if (clu_is_ready()) {
        struct mount *pmp;
        bfSetIdT origBfSetId;
        bfSetT *bfSetp;

        sts = bs_bfset_activate(libBufp->setClone.domain,
                                libBufp->setClone.origSetName,
                                0,
                                &origBfSetId);
        if (sts != EOK) {
                return(sts);  /* Exception */
        }

        bfSetp = bs_bfs_lookup_desc(origBfSetId);
        /* if fset not mounted, NULL is returned */
        if (bfSetp != NULL) {
            if (bfSetp->fsnp != NULL) {
                struct fileSetNode *fsnp = bfSetp->fsnp;
                pmp = fsnp->mountp;
                CC_FSFLUSH(pmp, CLONE);
            } else  {
                bs_bfdmn_deactivate( origBfSetId.domainId, 0 );
                return( E_NO_SUCH_BF_SET );  /* Exception */
            }
        }

        bs_bfdmn_deactivate( origBfSetId.domainId, 0 );

    }

    sts = fs_fset_clone(libBufp->setClone.domain,
                        libBufp->setClone.origSetName,
                        libBufp->setClone.cloneSetName,
                        &libBufp->setClone.cloneSetId,
                        xid);

    if (sts == EOK) {
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = libBufp->setClone.domain;
        advfs_event->fileset = libBufp->setClone.origSetName;
        advfs_event->clonefset = libBufp->setClone.cloneSetName;
        advfs_post_kernel_event(EVENT_FSET_CLONE, advfs_event);
        ms_free(advfs_event);
    }

    return(sts);
}


/* ADVFS_FSET_RENAME */
mlStatusT
msfs_syscall_op_fset_rename(libParamsT *libBufp, long xid)
{

    advfs_ev *advfs_event;
    mlStatusT sts;

    sts = fs_fset_name_change(libBufp->setRename.domain,
                              libBufp->setRename.origSetName,
                              libBufp->setRename.newSetName,
                              xid);

    if (sts == EOK) {
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = libBufp->setRename.domain;
        advfs_event->fileset = libBufp->setRename.origSetName;
        advfs_event->renamefset = libBufp->setRename.newSetName;
        advfs_post_kernel_event(EVENT_FSET_RENAME, advfs_event);
        ms_free(advfs_event);
    }

    return(sts);
}


/* ADVFS_DMN_INIT */
mlStatusT
msfs_syscall_op_dmn_init(libParamsT *libBufp)
{

    advfs_ev *advfs_event;
    int error;
    mlStatusT sts;

    sts = bs_dmn_init( libBufp->dmnInit.domain,
                       libBufp->dmnInit.maxVols,
                       libBufp->dmnInit.logPgs,
                       libBufp->dmnInit.logSvc,
                       libBufp->dmnInit.tagSvc,
                       libBufp->dmnInit.volName,
                       libBufp->dmnInit.volSvc,
                       libBufp->dmnInit.volSize,
                       libBufp->dmnInit.bmtXtntPgs,
                       libBufp->dmnInit.bmtPreallocPgs,
                       libBufp->dmnInit.domainVersion,
                       &libBufp->dmnInit.bfDomainId );

    if (sts == EOK) {
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->special = libBufp->dmnInit.volName;
        advfs_event->domain = libBufp->dmnInit.domain;
        advfs_post_kernel_event(EVENT_FDMN_MK, advfs_event);
        ms_free(advfs_event);
    }

    return(sts);
}


/* ADVFS_GET_DMN_VOL_LIST */
mlStatusT
msfs_syscall_op_get_dmn_vol_list(libParamsT *libBufp)
{
    domainT *dmnP;
    mlStatusT sts;

    libBufp->getDmnVolList.volIndexArray = (uint32T *)ms_malloc(
                    libBufp->getDmnVolList.volIndexArrayLen * sizeof(uint32T));

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->getDmnVolList.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    sts = bs_get_dmn_vd_list (dmnP,
                              libBufp->getDmnVolList.volIndexArrayLen,
                              libBufp->getDmnVolList.volIndexArray,
                              &libBufp->getDmnVolList.numVols);

    close_domain(libBufp->getDmnVolList.bfDomainId, dmnP, 0);

    return(sts);
}


/* ADVFS_MIGRATE */
mlStatusT
msfs_syscall_op_migrate(libParamsT *libBufp, struct bfAccess *bfap)
{
    mlStatusT sts;

    sts =  bs_migrate(bfap,
                      libBufp->migrate.srcVolIndex,
                      libBufp->migrate.srcPageOffset,
                      libBufp->migrate.srcPageCnt,
                      libBufp->migrate.dstVolIndex,
                      libBufp->migrate.dstBlkOffset,
                      libBufp->migrate.forceFlag,
                      BS_ALLOC_DEFAULT);

    return sts;
}


/* ADVFS_REM_NAME */
mlStatusT
msfs_syscall_op_rem_name(libParamsT *libBufp, struct bfAccess *bfap)
{
    mlStatusT sts;

    sts = remove_entry( bfap, libBufp->removeName.name );

    return sts;
}


/* ADVFS_REM_BF */
mlStatusT
msfs_syscall_op_rem_bf(libParamsT *libBufp, struct bfAccess *bfap)
{

    ino_t rem_ino;
    mlStatusT sts;

    rem_ino = libBufp->removeBf.ino;
    if (libBufp->removeBf.flag == 1) {
        AdvfsFixUpSBM = 1;
        sts = remove_bf( rem_ino, bfap , 1);
        AdvfsFixUpSBM = 0;
    } else {
        sts = remove_bf( rem_ino, bfap , 0);
    }

    return(sts);
}


/* ADVFS_TAG_STAT */
mlStatusT
msfs_syscall_op_tag_stat(libParamsT *libBufp)
{

    bfTagT tag;
    bfMCIdT mcid;
    bfSetT *bfSetp;
    vdIndexT vdIndex;
    off_t pageCnt;
    int siz;
    statT *fs_statp = NULL;              /* Cheating */
    bsBfAttrT *bs_attrp = NULL;
    int accessed = 0;
    off_t bytes;
    int unmount_read_lock = 0;
    int bfs_lock = 0;
    struct mount *mp;
    struct vnode *nullvp,*index_vp=NULL;
    bfAccessT *bfap;
    mlStatusT sts;

    /*
     * To prevent a race with unmount, the m_unmount_lock
     * is acquired.
     * We must first lock the BfSetTblLock in order to
     * get to the mp needed for the unmount lock.
     *
     */

    bfSetp = bs_bfs_lookup_desc(libBufp->tagStat.setId);
    if (bfSetp == NULL) {
	RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
    }

    BFSETTBL_LOCK_READ( bfSetp->dmnP ) 
    bfs_lock = 1;

    /* Verify the bitfile set is available. */
    if ( !BFSET_VALID(bfSetp) || (bfSetp->fsRefCnt == 0)) {
        RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
    }

    if (bfSetp->fsnp == NULL) {
        RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
    }

    mp = bfSetp->fsnp->mountp;

    if (!UNMOUNT_TRY_READ_NOSLP(mp)) {
        RAISE_EXCEPTION( E_NO_SUCH_BF_SET );
    } else {
        unmount_read_lock = 1;
    }

    BFSETTBL_UNLOCK( bfSetp->dmnP );
    bfs_lock = 0;

    tag = libBufp->tagStat.tag;

    bs_attrp = (bsBfAttrT *) ms_malloc(sizeof(bsBfAttrT));
    fs_statp = (statT *) ms_malloc(sizeof(statT));

    while (1) {
        /*
         * Loop until we find a tag such that:
         *
         *   1. The bitfile has a BMTR_FS_STAT record.
         *   2. The bitfile has a BSR_ATTR record.
         *   3. The bitfile is either a member of a clone fileset
         *      or is not marked 'delete with clone'.
         */
        if ((sts = tagdir_lookup_next(bfSetp, &tag, &mcid, &vdIndex)) != EOK) {
            break;
        }

        nullvp = NULL;
        sts = bs_access(&bfap, tag, bfSetp, FtxNilFtxH, 0, NULLMT, &nullvp);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }
        accessed = 1;

        sts = bmtr_get_rec(bfap, BSR_ATTR, bs_attrp, sizeof(bsBfAttrT));
        if (sts == EBMTR_NOT_FOUND) {
            (void) bs_close(bfap, 0);
            accessed = 0;

            continue;
        } else if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * If the file is marked as "deleteWithClone",
         * then skip it if we are going through the
         * original fileset.  But if we are going
         * through a clone fileset, then count it.
         * That is because the file still exists for
         * the clone fileset.
         */
        if ((bs_attrp->deleteWithClone) &&
            (bfSetp->cloneId == BS_BFSET_ORIG)) {
            (void) bs_close(bfap, 0);
            accessed = 0;

            continue;
        }

        sts = bmtr_get_rec(bfap, BMTR_FS_STAT, fs_statp, sizeof(statT));
        if (sts == EBMTR_NOT_FOUND) {
            (void) bs_close(bfap, 0);
            accessed = 0;

            continue;
        } else if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        /* Need to open the index file if this is a dir so we can
         * add in the allocated block count in bs_get_bf_page_cnt().
         */
        if ((fs_statp->st_mode & S_IFMT) == S_IFDIR) {
        	sts = bf_get_l(tag, NULL, DONT_UNLOCK, mp, &index_vp, NULL);
		if (sts != EOK) {
		    RAISE_EXCEPTION(sts);
                }
	}

        /* Ensure the long pageCnt field is zero because the following
         * routine expects a uint32T size field.
         */

        pageCnt = 0;

        if ((sts = bs_get_bf_page_cnt(bfap, (uint32T *)&pageCnt)) != EOK) {
            break;
        }

        bytes = (off_t)pageCnt * ADVFS_PGSZ;
        bytes += fs_statp->fragId.type * 1024;

        libBufp->tagStat.tag = tag;
        libBufp->tagStat.uid = fs_statp->st_uid;
        libBufp->tagStat.gid = fs_statp->st_gid;
        libBufp->tagStat.atime = fs_statp->st_atime;
        libBufp->tagStat.mode = fs_statp->st_mode;
        libBufp->tagStat.size = bytes;

        break;
    }

HANDLE_EXCEPTION:

    if (accessed) {
	(void) bs_close(bfap,0);
	
	/* release ref on directory & index if we opened it */
	if (index_vp != NULL)
	    vrele(index_vp);
    }

    if (unmount_read_lock) {
        UNMOUNT_READ_UNLOCK(mp);
    }

    if (bfs_lock) {
        BFSETTBL_UNLOCK( bfSetp->dmnP );
    }

    if (bs_attrp) {
        ms_free (bs_attrp);
    }

    if (fs_statp) {
        ms_free (fs_statp);
    }

    return(sts);
}


/* ADVFS_ADD_VOLUME */
mlStatusT
msfs_syscall_op_add_volume(libParamsT *libBufp, opIndexT opIndex)
{

    advfs_ev *advfs_event;
    int size, error;
    mlStatusT sts;
    int flag = 0;

    if (clu_is_ready()) {
        /* set flag if domain is known to cluster */
        if ( CC_DMN_NAME_TO_MP(libBufp->addVol.domain) )
            flag = M_MSFS_MOUNT;

        error = CC_DOMAIN_CHANGE_VOL(&opIndex,
                                     libBufp->addVol.domain,
                                     libBufp->addVol.volName);
        if (error != 0) {
            return(error);  /* Exception */
        }
    }

    sts = bs_vd_add_active(libBufp->addVol.domain,
                           libBufp->addVol.volName,
                           &libBufp->addVol.volSvc,
                           libBufp->addVol.volSize,
                           libBufp->addVol.bmtXtntPgs,
                           libBufp->addVol.bmtPreallocPgs,
                           flag,
                           &libBufp->addVol.bfDomainId,
                           &libBufp->addVol.volIndex);
    if (clu_is_ready()) {
        (void)CC_DOMAIN_CHANGE_VOL_DONE(&opIndex,
                                        libBufp->addVol.domain,
                                        libBufp->addVol.volName,
                                        sts);
    }

    if (sts != EOK) {
        return(sts);  /* Exception */
    } else {
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->special = libBufp->addVol.volName;
        advfs_event->domain = libBufp->addVol.domain;
        advfs_post_kernel_event(EVENT_FDMN_ADDVOL, advfs_event);
        ms_free(advfs_event);
    }

    return(sts);
}


/* ADVFS_REM_VOLUME */
mlStatusT
msfs_syscall_op_rem_volume(libParamsT *libBufp, opIndexT opIndex)
{
    struct mount *pmp = NULL;
    struct domain *dmnP;
    struct vd *vdp;
    int vdRefBumped = 0;
    char *vdName = NULL;
    int error, dmn_ref=FALSE;
    mlStatusT sts;
    int flag = clu_is_ready()?M_MSFS_MOUNT:0;

    sts = open_domain (BS_ACC_WRITE,
                       OWNER_ONLY,
                       libBufp->remVol.bfDomainId,
                       &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    dmn_ref=TRUE;

    vdName = ms_malloc( MAXPATHLEN+1 );

    /* Check the disk; if valid, bump its vdRefCnt */
    {
        vdIndexT vdi = libBufp->remVol.volIndex;
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            ms_free(vdName);
            close_domain (libBufp->remVol.bfDomainId, dmnP, flag);
            RAISE_EXCEPTION( EBAD_VDI );
        } else
            vdRefBumped = 1;
    }

    strcpy(vdName, vdp->vdName);

    /* Decrement the vdRefCnt on the vdT struct */
    vd_dec_refcnt( vdp );
    vdRefBumped = 0;

    if (clu_is_ready()) {
        error = CC_DOMAIN_CHANGE_VOL(&opIndex, dmnP->domainName, vdName);
        if (error != 0) {
            RAISE_EXCEPTION(error);
        }
    }

    sts = bs_vd_remove_active( dmnP,
                               libBufp->remVol.volIndex,
                               libBufp->remVol.forceFlag);

    if (clu_is_ready()) {

        bfSetIdT bfSetId;
        struct fileSetNode *fsnp;
        bfSetParamsT bfsetParams;
        uint32T setIdx = 0;
        uint32T userId;
        statusT sts2=EOK;
        struct mount *pmp = NULL;
        bfSetT* bfSetp;

        /*
         * to inform the cfs server that addvol is done,
         * so it's time to resume block reservation.
         */
        sts2 = bs_bfs_get_info( &setIdx, &bfsetParams, dmnP, &userId );

        MS_SMP_ASSERT(sts2 == EOK);
        if (sts2 == EOK) {
            bfSetId = bfsetParams.bfSetId;
            bfSetp = bs_bfs_lookup_desc(bfSetId);

            if (bfSetp != NULL && bfSetp->fsnp != NULL) {
                fsnp = bfSetp->fsnp;
                pmp = fsnp->mountp;
                CC_FSFLUSH(pmp, R_DONE);
            }
        }
        (void)CC_DOMAIN_CHANGE_VOL_DONE(&opIndex,
            dmnP->domainName, vdName, sts);
    }

HANDLE_EXCEPTION:

    /* Decrement reference count on vdT if we left it bumped. */
    if ( vdRefBumped ) {
        vd_dec_refcnt( vdp );
        vdRefBumped = 0;
    }

    if (vdName)
        ms_free(vdName);

    if (dmn_ref)
        close_domain (libBufp->remVol.bfDomainId, dmnP, flag);

    return(sts);
}


/* ADVFS_SS_DMN_OPS */
mlStatusT
msfs_syscall_op_ss_dmn_ops(libParamsT *libBufp)
{
    mlStatusT sts=EOK;
    ssUIDmnOpsT opType = libBufp->ssDmnOps.type;
    bfDomainIdT domainId;
    domainT *dmnP;
    int dmnActive=FALSE, dmnOpen=FALSE;
    extern REPLICATED int SS_is_running;
    u_long dmnState = 0;

    switch (opType) {
        case SS_UI_RESTART:
            sts = ss_kern_start();
            break;
        case SS_UI_STOP:
            ss_kern_stop();
            break;
        case SS_UI_ACTIVATE:
            sts = ss_change_state(libBufp->ssDmnOps.domainName,
                                  SS_ACTIVATED,&dmnState,1);
            break;
        case SS_UI_DEACTIVATE:
            sts = ss_change_state(libBufp->ssDmnOps.domainName,
                                  SS_DEACTIVATED,&dmnState,1);
            break;
        case SS_UI_SUSPEND:
            sts = ss_change_state(libBufp->ssDmnOps.domainName,
                                  SS_SUSPEND,&dmnState,1);
            break;
        case SS_UI_STATUS:
            sts = bs_bfdmn_tbl_activate( libBufp->ssDmnOps.domainName,
                                         0,
                                         &domainId );
            if (sts != EOK) {
                RAISE_EXCEPTION(sts);  /* Exception */
            }
            dmnActive = TRUE;

            sts = bs_domain_access( &dmnP, domainId, FALSE );

            if (sts != EOK) {
                RAISE_EXCEPTION(sts);  /* Exception */
            }
            dmnOpen = TRUE;

            libBufp->ssDmnOps.ssDmnCurrentState = dmnP->ssDmnInfo.ssDmnState;
            libBufp->ssDmnOps.ssRunningState = SS_is_running;
            break;
        default:
            RAISE_EXCEPTION(EBAD_PARAMS);  /* Exception */
            break;
    }

HANDLE_EXCEPTION:

    if (dmnOpen) {
        bs_domain_close( dmnP );
        dmnOpen = FALSE;
    }
    if (dmnActive) {
        bs_bfdmn_deactivate( domainId, 0 );
        dmnActive = FALSE;
    }

    return(sts);
}


/* ADVFS_SS_SET_PARAMS */
mlStatusT
msfs_syscall_op_ss_set_params(libParamsT *libBufp)
{
    statusT sts=EOK;
    bfDomainIdT domainId;
    mlSSDmnParamsT *ssDmnParamsp = &libBufp->ssSetParams.ssDmnParams;
    domainT *dmnP;
    int dmnActive=FALSE;
    int dmnOpen=FALSE;

    sts = bs_bfdmn_tbl_activate( libBufp->ssSetParams.domainName,
                                 0,
                                 &domainId );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmnOpen = TRUE;

    dmnP->ssDmnInfo.ssDmnDefragment = ssDmnParamsp->ssDmnDefragment;
    dmnP->ssDmnInfo.ssDmnSmartPlace = ssDmnParamsp->ssDmnSmartPlace;
    dmnP->ssDmnInfo.ssDmnBalance = ssDmnParamsp->ssDmnBalance;
    dmnP->ssDmnInfo.ssDmnVerbosity = ssDmnParamsp->ssDmnVerbosity;
    dmnP->ssDmnInfo.ssDmnDirectIo = ssDmnParamsp->ssDmnDirectIo;
    dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy = ssDmnParamsp->ssMaxPercentOfIoWhenBusy;
    if(dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy)
        dmnP->ssDmnInfo.ssPctIOs = 200 / (int) dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    else
        dmnP->ssDmnInfo.ssPctIOs = 0;

    dmnP->ssDmnInfo.ssAccessThreshHits = ssDmnParamsp->ssAccessThreshHits;
    dmnP->ssDmnInfo.ssSteadyState = ssDmnParamsp->ssSteadyState;
    dmnP->ssDmnInfo.ssMinutesInHotList = ssDmnParamsp->ssMinutesInHotList;
    dmnP->ssDmnInfo.ssExtentsConsol = ssDmnParamsp->ssExtentsConsol;
    dmnP->ssDmnInfo.ssPagesConsol = ssDmnParamsp->ssPagesConsol;
    dmnP->ssDmnInfo.ssReserved0 = ssDmnParamsp->ssReserved0;
    dmnP->ssDmnInfo.ssReserved1 = ssDmnParamsp->ssReserved1;
    dmnP->ssDmnInfo.ssReserved2 = ssDmnParamsp->ssReserved2;

    /* write out on-disk smartstore record here. */
    sts = ss_put_rec (dmnP);
    if(sts != EOK) {
        RAISE_EXCEPTION(sts);
    }


HANDLE_EXCEPTION:

    if (dmnOpen) {
        bs_domain_close(dmnP);
        dmnOpen = FALSE;
    }

    if (dmnActive) {
        bs_bfdmn_deactivate(domainId, 0);
        dmnActive = FALSE;
    }

    return(sts);
}

/* ADVFS_SS_GET_PARAMS */
mlStatusT
msfs_syscall_op_ss_get_params(libParamsT *libBufp)
{
    statusT sts=EOK;
    bfDomainIdT domainId;
    bsSSDmnAttrT ssAttr;
    int dmnActive=FALSE;
    int dmnOpen=FALSE;
    domainT *dmnP;
    /*
     * CFS TODO:
     * check if cfs_domain_change_vol() need to be called
     * here.
     */
    sts = bs_bfdmn_tbl_activate( libBufp->ssGetParams.domainName,
                                 0,
                                 &domainId );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmnOpen = TRUE;

    libBufp->ssGetParams.ssDmnParams.ssFirstMountTime =
                             dmnP->ssDmnInfo.ssFirstMountTime;
    libBufp->ssGetParams.ssDmnParams.ssDmnDefragment =
                             dmnP->ssDmnInfo.ssDmnDefragment;

    libBufp->ssGetParams.ssDmnParams.ssDmnSmartPlace =
                             dmnP->ssDmnInfo.ssDmnSmartPlace;
    libBufp->ssGetParams.ssDmnParams.ssDmnBalance =
                             dmnP->ssDmnInfo.ssDmnBalance;
    libBufp->ssGetParams.ssDmnParams.ssDmnVerbosity =
                             dmnP->ssDmnInfo.ssDmnVerbosity;
    libBufp->ssGetParams.ssDmnParams.ssDmnDirectIo =
                             dmnP->ssDmnInfo.ssDmnDirectIo;
    libBufp->ssGetParams.ssDmnParams.ssMaxPercentOfIoWhenBusy =
                             dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    libBufp->ssGetParams.ssDmnParams.ssAccessThreshHits =
                             dmnP->ssDmnInfo.ssAccessThreshHits;
    libBufp->ssGetParams.ssDmnParams.ssSteadyState =
                             dmnP->ssDmnInfo.ssSteadyState;
    libBufp->ssGetParams.ssDmnParams.ssMinutesInHotList =
                             dmnP->ssDmnInfo.ssMinutesInHotList;
    libBufp->ssGetParams.ssDmnParams.ssFilesDefraged =
                             dmnP->ssDmnInfo.ssFilesDefraged;
    libBufp->ssGetParams.ssDmnParams.ssPagesDefraged =
                             dmnP->ssDmnInfo.ssPagesDefraged;
    libBufp->ssGetParams.ssDmnParams.ssPagesBalanced =
                             dmnP->ssDmnInfo.ssPagesBalanced;
    libBufp->ssGetParams.ssDmnParams.ssFilesIOBal =
                             dmnP->ssDmnInfo.ssFilesIOBal;
    libBufp->ssGetParams.ssDmnParams.ssExtentsConsol =
                             dmnP->ssDmnInfo.ssExtentsConsol;
    libBufp->ssGetParams.ssDmnParams.ssPagesConsol =
                             dmnP->ssDmnInfo.ssPagesConsol;
    libBufp->ssGetParams.ssDmnParams.ssReserved0 =
                             dmnP->ssDmnInfo.ssReserved0;
    libBufp->ssGetParams.ssDmnParams.ssReserved1 =
                             dmnP->ssDmnInfo.ssReserved1;
    libBufp->ssGetParams.ssDmnParams.ssReserved2 =
                             dmnP->ssDmnInfo.ssReserved2;

HANDLE_EXCEPTION:

    if (dmnOpen) {
        bs_domain_close( dmnP );
        dmnOpen=FALSE;
    }

    if (dmnActive) {
        bs_bfdmn_deactivate( domainId, 0 );
        dmnActive=FALSE;
    }

    return(sts);
}

/* ADVFS_SS_GET_FRAGLIST */
mlStatusT
msfs_syscall_op_ss_get_fraglist(libParamsT *libBufp)
{
    statusT sts=EOK;
    bfDomainIdT domainId;
    vdIndexT volIndex = 0;
    int vd_refed=FALSE;
    int dmnActive=FALSE;
    int dmnOpen=FALSE;
    domainT *dmnP;
    vdT *vdp;
    ssFragLLT *fp;
    ssFragHdrT *fhp;
    int i=0;

    /*
     * If the caller only wants a count of extents, don't
     * bother allocating an array to hold them.
     */
    if (libBufp->ssGetFraglist.ssFragArraySize != 0) {
        libBufp->ssGetFraglist.ssFragArray = (mlFragDescT *)ms_malloc(
                                 libBufp->ssGetFraglist.ssFragArraySize *
                                 sizeof(mlFragDescT));
    } else {
        libBufp->ssGetFraglist.ssFragArray = NULL;
    }

    sts = bs_bfdmn_tbl_activate( libBufp->ssGetFraglist.domainName,
                                 0,
                                 &domainId );
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmnOpen=TRUE;

    volIndex = libBufp->ssGetFraglist.volIndex;
    MS_SMP_ASSERT(volIndex > 0);
    if ( !(vdp = vd_htop_if_valid(volIndex, dmnP, TRUE, FALSE)) ) {
        /*
         * This can happen when we a disk is removed from service
         * while we are allocating storage.
         */
        RAISE_EXCEPTION(E_VOL_NOT_IN_SVC_CLASS);
    }
    vd_refed = TRUE;

    /*
     * Obtaining fraglist through vdp.
     */
    mutex_lock(&vdp->ssVolInfo.ssFragLk);

    fhp = &vdp->ssVolInfo.ssFragHdr;
    fp = fhp->ssFragRatFwd;  /* entry pointer */
    while (( fp != (ssFragLLT *) fhp ) &&
           (i < libBufp->ssGetFraglist.ssFragCnt)) {
        /*
         * copy fields out into respective fields.
         */
        libBufp->ssGetFraglist.ssFragArray[i].ssFragTag = fp->ssFragTag;
        libBufp->ssGetFraglist.ssFragArray[i].ssFragBfSetId = fp->ssFragBfSetId;
        libBufp->ssGetFraglist.ssFragArray[i].ssFragRatio = fp->ssFragRatio;
        libBufp->ssGetFraglist.ssFragArray[i].ssXtntCnt = fp->ssXtntCnt;

        i++;
        fp = fp->ssFragRatFwd;
    }
    libBufp->ssGetFraglist.ssFragCnt = i;
    mutex_unlock(&vdp->ssVolInfo.ssFragLk);

HANDLE_EXCEPTION:

    if(vd_refed)
        vd_dec_refcnt( vdp );

    if (dmnOpen) {
        bs_domain_close(dmnP);
        dmnOpen=FALSE;
    }

    if (dmnActive) {
        bs_bfdmn_deactivate (domainId, 0);
        dmnActive=FALSE;
    }

    return(sts);
}

/* ADVFS_SS_GET_HOTLIST */
mlStatusT
msfs_syscall_op_ss_get_hotlist(libParamsT *libBufp)
{
    statusT sts = EOK;
    mlBfDomainIdT bfDomainId;
    int dmn_ref=FALSE;
    domainT *dmnP;
    ssHotLLT *hp;
    ssHotHdrT *hhp;
    int i=0, wday;
    struct timeval curr_time;

    /*
     * If the caller only wants a count of hotlist, don't
     * allocate an array to hold them.
     */
    if (libBufp->ssGetHotlist.ssHotArraySize != 0) {
        libBufp->ssGetHotlist.ssHotArray = (mlHotDescT *)ms_malloc(
                              libBufp->ssGetHotlist.ssHotArraySize *
                              sizeof(mlHotDescT));
    } else {
        libBufp->ssGetHotlist.ssHotArray = NULL;
    }

    bfDomainId = libBufp->ssGetHotlist.bfDomainId;

    sts = open_domain (BS_ACC_WRITE,
                       OWNER_ONLY,
                       bfDomainId,
                       &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    dmn_ref=TRUE;

    mutex_lock( &dmnP->ssDmnInfo.ssDmnHotLk );

    /*
     * Obtaining hotlist through dmnP.
     */
    TIME_READ(curr_time);
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    hp = hhp->ssHotFwd;  /* entry pointer */
    while (( hp != (ssHotLLT *) hhp ) &&
           (i < libBufp->ssGetHotlist.ssHotCnt)) {

        if(((curr_time.tv_sec - hp->ssHotEntryTime) <
                  dmnP->ssDmnInfo.ssMinutesInHotList) ) {
            hp = hp->ssHotFwd;
            continue;
        }

        /*
         * copy fields out into respective fields.
         */
        libBufp->ssGetHotlist.ssHotArray[i].ssHotTag = hp->ssHotTag;
        libBufp->ssGetHotlist.ssHotArray[i].ssHotBfSetId = hp->ssHotBfSetId;
        libBufp->ssGetHotlist.ssHotArray[i].ssHotPlaced = hp->ssHotPlaced;
        libBufp->ssGetHotlist.ssHotArray[i].ssHotVdi = hp->ssHotVdi;
        for(wday=0, libBufp->ssGetHotlist.ssHotArray[i].ssHotIOCnt=0;
            wday<SS_HOT_TRACKING_DAYS; wday++) {
            libBufp->ssGetHotlist.ssHotArray[i].ssHotIOCnt += hp->ssHotIOCnt[wday];
        }

        libBufp->ssGetHotlist.ssHotArray[i].ssHotLastUpdTime = hp->ssHotLastUpdTime;
        libBufp->ssGetHotlist.ssHotArray[i].ssHotEntryTime = hp->ssHotEntryTime;
        libBufp->ssGetHotlist.ssHotArray[i].ssHotErrTime = hp->ssHotErrTime;
        i++;
        hp = hp->ssHotFwd;
    }
    libBufp->ssGetHotlist.ssHotCnt = i;

    mutex_unlock( &dmnP->ssDmnInfo.ssDmnHotLk );

HANDLE_EXCEPTION:

    if (dmn_ref)
        close_domain (bfDomainId, dmnP, 0);

    return(sts);
}

/* ADVFS_ADD_REM_VOL_SVC_CLASS */
mlStatusT
msfs_syscall_op_add_rem_vol_svc_class(libParamsT *libBufp)
{
    bfDomainIdT domainId;
    domainT *dmnP;
    struct vd *vdp;
    int vdRefBumped = 0;
    mlStatusT sts;
    int flag = 0;

    /* set flag if domain is known to cluster */
    if (clu_is_ready() && CC_DMN_NAME_TO_MP(libBufp->addRemVolSvcClass.domainName))
        flag = M_MSFS_MOUNT;

    /* CFS TODO: check if cfs_domain_change_vol() needs to be
     * called here.
     */
    sts = bs_bfdmn_tbl_activate( libBufp->addRemVolSvcClass.domainName,
                                 flag,
                                 &domainId );
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
        bs_bfdmn_deactivate( domainId, flag );
        return(sts);  /* Exception */
    }

    /*
     * to inform cfs server that rmvol is going to happen,
     * suspend block reservation for the fileset until
     * rmvol is done. if later rmvol failed, the disk
     * is adding back, we resume block reservation.
     */

    /*
     * Note: The CFS flush needs to occur *before* the volume
     * is removed from the service class, so that writes guaranteed
     * to the user on the basis of block reservation promises can
     * be written to disk.  Note that standards requires that ENOSPC
     * be returned up front for writes that will fail due to no space.
     * (It's ok for the rmvol to later fail if it cannot migrate all
     * data to the remaining volumes.)
     */

    if (clu_is_ready() && (libBufp->addRemVolSvcClass.action)) {
        bfSetIdT bfSetId;
        bfSetParamsT bfsetParams;
        uint32T setIdx = 0;
        uint32T userId;
        statusT sts2=EOK;
        struct mount *pmp = NULL;
        bfSetT *bfSetp;

        /*
         * Note: During a rmvol, all filesets are required to be mounted.
         *
         * bs_bfs_get_info() and CC_FSFLUSH are called only once
         * (i.e. for the first fileset) because CFS marks every
         * fileset in the domain with the appropriate flush flag (see
         * blkrsrv_markflags()).
         */

        sts2 = bs_bfs_get_info( &setIdx, &bfsetParams, dmnP, &userId );

        MS_SMP_ASSERT(sts2 == EOK);
        if (sts2 == EOK) {
            bfSetId = bfsetParams.bfSetId;
            bfSetp = bs_bfs_lookup_desc(bfSetId);

            if (bfSetp != NULL && bfSetp->fsnp != NULL) {
                pmp = bfSetp->fsnp->mountp;
                CC_FSFLUSH(pmp, RMVOL);
            } else {
                /* We should not get here. */
                bs_domain_close( dmnP );
                bs_bfdmn_deactivate( domainId, flag );
                return(EFAIL);  /* Exception */
            }
        }
    }

    /* Check the disk; if valid, bump its vdRefCnt */
    {
        vdIndexT vdi = libBufp->addRemVolSvcClass.volIndex;
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            bs_domain_close( dmnP );
            bs_bfdmn_deactivate( domainId, flag );
            return(EBAD_VDI);  /* Exception */
        } else
            vdRefBumped = 1;
    }

    if (libBufp->addRemVolSvcClass.action == 0) {
        sts = sc_add_vd ( dmnP,
                          libBufp->addRemVolSvcClass.volSvc,
                          libBufp->addRemVolSvcClass.volIndex );
        if(sts == EOK) {
            vdp->serviceClass = libBufp->addRemVolSvcClass.volSvc;
        }

    } else {
        sts = sc_remove_vd ( dmnP,
                             libBufp->addRemVolSvcClass.volSvc,
                             libBufp->addRemVolSvcClass.volIndex );
        if(sts == EOK) {
            vdp->serviceClass = nilServiceClass;
        }
    }

    /* Decrement the vdRefCnt on the vdT struct */
    vd_dec_refcnt( vdp );
    vdRefBumped = 0;

    if (sts != EOK) {
        bs_domain_close( dmnP );
        bs_bfdmn_deactivate( domainId, flag );
        return(sts);  /* Exception */
    }

    bs_domain_close( dmnP );
    bs_bfdmn_deactivate( domainId, flag );

    /* CFS TODO:
     * if cfs_domain_change_vol() is called earlier, should call
     * cfs_domain_change_vol_done() here.
     */

    return(sts);
} 




/* ADVFS_ADD_REM_VOL_DONE */
mlStatusT
msfs_syscall_op_add_rem_vol_done(libParamsT *libBufp)
{
    bfDomainIdT domainId;
    domainT *dmnP;
    mlStatusT sts;
    int flag = 0;
    
    /* set flag if domain is known to cluster */
    if ( clu_is_ready() && CC_DMN_NAME_TO_MP(libBufp->addRemVolDone.domainName) )
        flag = M_MSFS_MOUNT;

    sts = bs_bfdmn_tbl_activate( libBufp->addRemVolDone.domainName,
                                 flag,
                                 &domainId );
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
        bs_bfdmn_deactivate( domainId, flag );
        return(sts);  /* Exception */
    }

    sts = bs_vd_add_rem_vol_done(dmnP, libBufp->addRemVolDone.volName,
                                 libBufp->addRemVolDone.op);

    bs_domain_close( dmnP );
    bs_bfdmn_deactivate( domainId, flag );

    return(sts);
}


/* ADVFS_SET_BF_NEXT_ALLOC_VOL */
mlStatusT
msfs_syscall_op_set_bf_next_alloc_vol(libParamsT *libBufp,
                                      struct bfAccess *bfap)
{
    mlStatusT sts;

    sts = stg_set_alloc_disk(bfap,
                              libBufp->setBfNextAllocVol.curVolIndex,
                              libBufp->setBfNextAllocVol.pageCntNeeded,
                              libBufp->setBfNextAllocVol.forceFlag);

    return sts;
}


/* ADVFS_SWITCH_LOG */
mlStatusT
msfs_syscall_op_switch_log(libParamsT *libBufp, long xid)
{

    bfDomainIdT domainId;
    domainT *dmnP;
    mlStatusT sts;

    /*
     * CFS TODO:
     * check if cfs_domain_change_vol() need to be called
     * here.
     */

    sts = bs_bfdmn_tbl_activate( libBufp->switchLog.domainName,
                                 0,
                                 &domainId );
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
        bs_bfdmn_deactivate( domainId, 0 );
        return(sts);  /* Exception */
    }

    sts = lgr_switch_vol( dmnP->ftxLogP,
                          (vdIndexT) libBufp->switchLog.volIndex,
                          (uint32T) libBufp->switchLog.logPgs,
                          (serviceClassT) libBufp->switchLog.logSvc,
                          xid );

    bs_domain_close( dmnP );
    bs_bfdmn_deactivate( domainId, 0 );

    return(sts);
}


/* ADVFS_SWITCH_ROOT_TAGDIR */
mlStatusT
msfs_syscall_op_switch_root_tagdir(libParamsT *libBufp)
{
    domainT *dmnP;
    bfDomainIdT domainId;
    mlStatusT sts;

    /* CFS TODO:
     * should check if the cfs_domain_change_vol() need to be
     * called here.
     */

    sts = bs_bfdmn_tbl_activate( libBufp->switchRootTagDir.domainName,
                                 0,
                                 &domainId );
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
        bs_bfdmn_deactivate( domainId, 0 );
        return(sts);  /* Exception */
    }

    sts = bs_switch_root_tagdir(
             dmnP,
             (vdIndexT)libBufp->switchRootTagDir.volIndex
          );

    bs_domain_close( dmnP );
    bs_bfdmn_deactivate( domainId, 0 );

    return(sts);
}


/* ADVFS_REWRITE_XTNT_MAP */
mlStatusT
msfs_syscall_op_rewrite_xtnt_map(libParamsT *libBufp,
                                 struct bfAccess *bfap,
                                 long xid)
{
    mlStatusT sts;

    sts = odm_rewrite_xtnt_map(bfap,
                                libBufp->rewriteXtntMap.xtntMapIndex,
                                FtxNilFtxH,
                                xid);

    return sts;
}


/* ADVFS_RESET_FREE_SPACE_CACHE */
mlStatusT
msfs_syscall_op_reset_free_space_cache(libParamsT *libBufp)
{
    domainT *dmnP;
    unLkActionT unlock_action;
    vdT *vdp;
    int vdRefBumped = 0;
    mlStatusT sts;

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->resetFreeSpaceCache.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    /* Check the disk; if valid, bump its vdRefCnt */
    {
        vdIndexT vdi = libBufp->resetFreeSpaceCache.volIndex;
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            close_domain(libBufp->resetFreeSpaceCache.bfDomainId, dmnP, 0);
            return(EBAD_VDI);  /* Exception */
        } else
            vdRefBumped = 1;
    }

    STGMAP_LOCK_WRITE( &(vdp->stgMap_lk) )

    sts = sbm_clear_cache(vdp);

    if (sts == EOK) {
        uint32T temp = libBufp->resetFreeSpaceCache.scanStartCluster;

        /*
         * Make sure scan start clust is set to a cluster
         * within range and on a page (16 block) boundary.
         */
        temp = temp % vdp->vdClusters;
        temp = (temp * vdp->stgCluster) & ~0x0f;  /* blocks */
        vdp->scanStartClust = temp / vdp->stgCluster;
    }

    STGMAP_UNLOCK( &vdp->stgMap_lk )

    /* Decrement the vdRefCnt on the vdT struct */
    vd_dec_refcnt( vdp );
    vdRefBumped = 0;

    close_domain (libBufp->resetFreeSpaceCache.bfDomainId, dmnP, 0);

    return(sts);
}


/* ADVFS_GET_GLOBAL_STATS */
mlStatusT
msfs_syscall_op_get_global_stats(libParamsT *libBufp)
{
    (void)bs_get_global_stats(&(libBufp->getGlobalStats));

    return(EOK);  /* No return value */
}

/* ADVFS_GET_SMSYNC_STATS */
mlStatusT
msfs_syscall_op_get_smsync_stats(libParamsT *libBufp)
{
    domainT *dmnP;
    mlStatusT sts;

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->getVolSmsync.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    sts = bs_get_smsync_stats(dmnP,
                              libBufp->getVolSmsync.volIndex,
                              &libBufp->getVolSmsync.smsyncQ_cnt,
                              libBufp->getVolSmsync.smsync);

    close_domain(libBufp->getVolSmsync.bfDomainId, dmnP, 0);

    return(sts);
}

/**
***
*** End of system call routines.
***
**/

/*
 * find_cfs_server -
 *              finds the CFS server for the specified domain/file.
 * given: 1) a syscall operation index
 *        2) pointer to the syscall data structure
 *        3) a server search key
 *        4) pointer to a flag which shows whether the operation should
 *           be carried out locally.
 * does:  determines whether the operation should be done locally or
 *        remotely. If the latter, function ship the call to the server.
 *
 *
 *  for FILE_PATH case, caller must free allocated memory for path name
 *
 * return:the status code
 *
 * Cluster Performance Notes:  In order to minimize interference between
 *     multiple msfs_real_syscall()'s or with mount/unmount, we want to 
 *     avoid the global CMS dlm lock as much as possible and rely, instead,
 *     upon the domain-specific dlm lock.  This can happen even if no
 *     fileset is currently mounted for a given domain.
 */

mlStatusT
find_cfs_server (
                opIndexT         op,
                libParamsT       *buf,
                ServerSearchKeyT key,
                int              *do_local,
                int              *have_domain_lock,
                int              *have_cmsdb_lock,
                dlm_lkid_t       *sublock,
                dlm_lkid_t       *cmsdb_sublock)
{
    struct  mount *mp = NULL;
    int     sts, fd = -1, len;
    char    *dmn_name = NULL, *file_path,
            name_buf[BS_DOMAIN_NAME_SZ + 1];
    bfDomainIdT dmn_id = {0,0};
    int     get_domain_lock = 1;
    int     get_domain_excl = 0;   /* get domain lock shared by default */
    int     have_cmsdb_excl = 0;
    int     versw_val;
    int     dmn_dlm_lock_timeo=1;

    *have_domain_lock = 0;
    *have_cmsdb_lock = 0;

/* 
 * Since this routine executes before any operation-specific logic,
 * this is where we decide what mode to take the dlm locks with.
 * There are two serialization issues - interference with/from CMS, and
 * disallowing multiple concurrent domain activations in the cluster.
 * In either case, we can use either the domain-specific lock or cmsdb dlm
 * lock.  The domain-specific is preferred for performance reasons.
 *
 * CMS tries to obtain the domain-specific lock in exclusive mode after 
 * having taken the cmsdb lock.  That code has been modifed to be like a
 * "lock_try", releasing the cmsdb lock between retries.  This allows the 
 * advfs syscall paths to obtain the domain-specific lock and then
 * obtain the cmsdb lock in case a CMS proposal must be issued, without
 * encountering a deadlock. However, since the syscall paths must also acquire
 * the cmsdb dlm lock before the domain-specific lock, as the code may not
 * even be able to use the domain-specific lock in a particular case, this 
 * code must also acquire the domain-specific lock via a "lock_try".
 *
 * The general logic is:
 *     Take the cmsdb lock and determine if something is mounted from the
 *       domain in question.
 *     If the operation may require cluster-wide serialization 
 *       due to a CMS proposal being involved, take the domain-specific
 *       lock exclusively if you can (currently, we always can), release
 *       the cmsdb lock, and take it exlusively later if proposal issued
 *     else 
 *       if domain name known (either from syscall params or via a 
 *                             mounted fileset)
 *               take domain-specific lock shared if allowed, and
 *                       exclusive otherwise
 *               release the cmsdb lock
 *       else
 *            keep (re-take if was shared) the cmsdb lock in exclusive mode
 *
 * In compatability mode (VERSW dependent) :
 * (The code will work as before - maintaining the old locking order).
 *     If there may be a CMS proposal issued, take the cmsdb lock in 
 *        exclusive mode.
 *     else 
 *       If we know the domain name, take the domain-specific lock
 *          and release the cmsdb lock
 *       else
 *          keep (re-take if was shared) the cmsdb lock in exclusive mode
 *
 */
retry:
    versw_val = VERSW();

    switch (key) {
        case DMN_NAME:
            switch (op) {
                case ADVFS_FSET_CLONE:
                    dmn_name = buf->setClone.domain;
                    if (versw_val == VERSW_ENABLE) {
                       /* if CMS proposal issued, cmsdb lock will be used then */
                       get_domain_lock = get_domain_excl = 1;
                    } else
                       get_domain_lock = 0;
                    break;
                case ADVFS_FSET_CREATE:
                    dmn_name = buf->setCreate.domain;
                    break;
                case ADVFS_FSET_DELETE:
                    dmn_name = buf->setDelete.domain;
                    if (versw_val == VERSW_ENABLE) {
                       /* if CMS proposal issued, cmsdb lock will be used then */
                       get_domain_lock = get_domain_excl = 1;
                    } else
                       get_domain_lock = 0;
                    break;
                case ADVFS_FSET_GET_ID:
                    dmn_name = buf->setGetId.domain;
                    break;
                case ADVFS_FSET_GET_INFO:
                    dmn_name = buf->setGetInfo.domain;
                    break;
                case ADVFS_FSET_GET_STATS:
                    dmn_name = buf->setGetStats.domain;
                    break;
                case ADVFS_FSET_RENAME:
                    dmn_name = buf->setRename.domain;
                    break;
                case ADVFS_GET_DMNNAME_PARAMS:
                    dmn_name = buf->getDmnNameParams.domain;
                    break;
                case ADVFS_SWITCH_LOG:
                    dmn_name = buf->switchLog.domainName;
                    break;
                case ADVFS_SWITCH_ROOT_TAGDIR:
                    dmn_name = buf->switchRootTagDir.domainName;
                    break;
                case ADVFS_ADD_VOLUME: /* Not supported remotely */
                    dmn_name = buf->addVol.domain;
                    if (versw_val == VERSW_ENABLE) {
                       /* cluster root must use exclusive global dlm lock */
                       if (!strcmp(dmn_name,"cluster_root"))
                           get_domain_lock = 0;
                       else {
                           /* if CMS proposal issued, cmsdb lock will be used then */
                           get_domain_lock = get_domain_excl = 1;
                       }
                    } else
                       get_domain_lock = 0;
                    break;
                case ADVFS_ADD_REM_VOL_SVC_CLASS:
                    dmn_name = buf->addRemVolSvcClass.domainName;
                    break;
                case ADVFS_ADD_REM_VOL_DONE:
                    dmn_name = buf->addRemVolDone.domainName;
                    break;
                case ADVFS_SS_DMN_OPS:
                    dmn_name = buf->ssDmnOps.domainName;
                    break;
                case ADVFS_SS_GET_PARAMS:
                    dmn_name = buf->ssGetParams.domainName;
                    break;
                case ADVFS_SS_SET_PARAMS:
                    dmn_name = buf->ssSetParams.domainName;
                    break;
                case ADVFS_SS_GET_FRAGLIST:
                    dmn_name = buf->ssGetFraglist.domainName;
                    break;
                case ADVFS_SET_BFSET_PARAMS_ACTIVATE:
                    dmn_name = buf->setBfSetParamsActivate.dmnName;
                    break;
                default:
                    MS_SMP_ASSERT(0);
                    get_domain_lock = 0;
                    break;
                }

            CC_CMSDB_DLM_LOCK((get_domain_lock ? DLM_PRMODE : DLM_EXMODE),
                               cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = !get_domain_lock;
            mp = CC_DMN_NAME_TO_MP(dmn_name);

            break;

        case DMN_ID:
                switch (op) {
                case ADVFS_GET_BFSET_PARAMS:     
                    dmn_id = buf->getBfSetParams.bfSetId.domainId;
                    break;
                case ADVFS_GET_DMN_PARAMS:
                    dmn_id = buf->getDmnParams.bfDomainId;
                    break;
                case ADVFS_GET_DMN_VOL_LIST:
                    dmn_id = buf->getDmnVolList.bfDomainId;
                    break;
                case ADVFS_GET_SMSYNC_STATS:
                    dmn_id = buf->getVolSmsync.bfDomainId;
                    break;
                case ADVFS_GET_VOL_BF_DESCS:
                    dmn_id = buf->getVolBfDescs.bfDomainId;
                    break;
                case ADVFS_GET_VOL_PARAMS:
                case ADVFS_GET_VOL_PARAMS2:
                    dmn_id = buf->getVolParams.bfDomainId;
                    break;
                case ADVFS_RESET_FREE_SPACE_CACHE:
                    dmn_id = buf->resetFreeSpaceCache.bfDomainId;
                    break;
                case ADVFS_SET_BFSET_PARAMS:
                    dmn_id = buf->setBfSetParams.bfSetId.domainId;
                    break;
                case ADVFS_SET_VOL_IOQ_PARAMS:
                    dmn_id = buf->setVolIoQParams.bfDomainId;
                    break;
                case ADVFS_TAG_STAT:
                    dmn_id = buf->tagStat.setId.domainId;
                    break;
                case ADVFS_REM_VOLUME: /* Not supported remotely */
                    dmn_id = buf->remVol.bfDomainId;
                    if (versw_val == VERSW_ENABLE) {
                       /* when CMS proposal issued, cmsdb lock will be used then */
                       get_domain_lock = get_domain_excl = 1;
                    } else
                       get_domain_lock = 0;
                    break;
                case ADVFS_SS_GET_HOTLIST:
                    dmn_id = buf->ssGetHotlist.bfDomainId;
                    break;
                default:
                    MS_SMP_ASSERT(0);
                    get_domain_lock = 0;
                    break;
            }
        
            /* 
             * If mp is not found below, we want to take cmsdb lock exclusively.
             * If mp is found below, we will release cmsdb lock soon anyway. 
             * Therefor, we always take it exclusively to avoid having to retry.
             */
            CC_CMSDB_DLM_LOCK(DLM_EXMODE, cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = 1;
            mp = CC_DMN_ID_TO_MP(&dmn_id);

            /* 
             * special handling for ADVFS_REM_VOLUME on cluster root :
             * we want to use exclusive global dlm lock
             */
            if (mp && op == ADVFS_REM_VOLUME) {
                CC_MP_TO_DMN_NAME(mp, name_buf);
                if (!strcmp(name_buf,"cluster_root")) {
                    get_domain_lock = 0;
                }
            }

 
            break;

        case FILE_PATH:
            switch (op) {
                case ADVFS_GET_NAME:
                    file_path = buf->getName.mp;
                    break;
                case ADVFS_UNDEL_ATTACH:
                    file_path = buf->undelAttach.dirName;
                    break;
                case ADVFS_UNDEL_DETACH:
                    file_path = buf->undelDetach.dirName;
                    break;
                case ADVFS_UNDEL_GET:
                    file_path = buf->undelGet.dirName;
                    break;
                default:
                    MS_SMP_ASSERT(0);
                    get_domain_lock = 0;
                    break;
            }

            /* we expect mp below to get set, so try shared mode */
            CC_CMSDB_DLM_LOCK((get_domain_lock ? DLM_PRMODE : DLM_EXMODE),
                               cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = !get_domain_lock;
            mp = CC_PATH_TO_MP(file_path);

            break;

        case FILE_DESC:
            switch (op) {
                case ADVFS_GET_BF_IATTRIBUTES:
                    fd = buf->getBfIAttributes.fd;
                    break;
                case ADVFS_GET_BF_PARAMS:
                    fd = buf->getBfParams.fd;
                    break;
                case ADVFS_GET_BF_XTNT_MAP:
                    fd = buf->getBfXtntMap.fd;
                    break;
                case ADVFS_GET_CLUDIO_XTNT_MAP:
                    fd = buf->getCludioXtntMap.fd;
                    break;
                case ADVFS_GET_BKUP_XTNT_MAP:
                    fd = buf->getBkupXtntMap.fd;
                    break;
                case ADVFS_GET_IDX_BF_PARAMS:
                    fd = buf->getBfParams.fd;
                    break;
                case ADVFS_GET_IDXDIR_BF_PARAMS:
                    fd = buf->getBfParams.fd;
                    break;
                case ADVFS_MIGRATE:
                    fd = buf->migrate.fd;
                    break;
                case ADVFS_MOVE_BF_METADATA:
                    fd = buf->moveBfMetadata.fd;
                    break;
                case ADVFS_REM_BF:
                    fd = buf->removeBf.dirFd;
                    break;
                case ADVFS_REM_NAME:
                    fd = buf->removeName.dirFd;
                    break;
                case ADVFS_REWRITE_XTNT_MAP:
                    fd = buf->rewriteXtntMap.fd;
                    break;
                case ADVFS_SET_BF_ATTRIBUTES:
                    fd = buf->setBfAttributes.fd;
                    break;
                case ADVFS_SET_BF_IATTRIBUTES:
                    fd = buf->setBfIAttributes.fd;
                    break;
                case ADVFS_SET_BF_NEXT_ALLOC_VOL:
                    fd = buf->setBfNextAllocVol.fd;
                    break;
                default:
                    MS_SMP_ASSERT(0);
                    get_domain_lock = 0;
                    break;
            }
           
            /* we expect mp below to get set, so try shared mode */
            CC_CMSDB_DLM_LOCK((get_domain_lock ? DLM_PRMODE : DLM_EXMODE),
                               cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = !get_domain_lock;
            mp = CC_FD_TO_MP(fd);

            break;
        }

        if (mp) /* domain/file is known to cluster, i.e., mounted */
        {
            if (get_domain_lock)
            {
                if (! dmn_name)
                {
                        dmn_name = name_buf;
                        CC_MP_TO_DMN_NAME(mp, name_buf);
                }

                /*
                 * Take domain lock in shared or exclusive mode
                 * Note that this is different from the !mp case.
                 */

                if (*dmn_name)
                {
                        sts = CC_DMN_NAME_DLM_LOCK_TIMEO(dmn_name, 
                                                       get_domain_excl, 
                                                       sublock,
                                                       dmn_dlm_lock_timeo);

                        if ( ! sts)
                        {
                             *have_domain_lock = 1;
                             CC_CMSDB_DLM_UNLOCK(cmsdb_sublock);
                             *have_cmsdb_lock=0;
                        } else {
                            if (sts == EWOULDBLOCK) { /* timed out */
                                CC_CMSDB_DLM_UNLOCK(cmsdb_sublock);
                                *have_cmsdb_lock=0;
                                (void)mpsleep(&dmn_name, PZERO, (char *)NULL, 
                                           1*hz, (void *) NULL, 0);

                                if(dmn_dlm_lock_timeo < 
					CC_MAX_DMN_DLM_LOCK_TIMEO()){
                                    dmn_dlm_lock_timeo++;
                                } else {
                                    dmn_dlm_lock_timeo=1;
                                }

                                goto retry;
                            }
                        }
                }
            }

            /*
             * the cluster callouts can return an mp which refers
             * to a non-cfs filesystem.  don't functionship if this
             * is the case.
             */

            sts = 0;
            if (mp->m_stat.f_type == MOUNT_CFS){
                sts = CC_MSFS_SYSCALL_FSHIP(mp, (int)(op), buf, do_local);
            }
        }
        else   /* not mounted, do op locally */
        {
          if (versw_val == VERSW_ENABLE) {
            if (key == DMN_NAME) {
                   if (dmn_name && *dmn_name) {
                       /* 
                        * Try to take domain lock excl to avoid concurrent
                        * domain activations in the cluster.
                        */
                       sts = CC_DMN_NAME_DLM_LOCK_TIMEO
                                (dmn_name,1,sublock,dmn_dlm_lock_timeo);
                       if ( ! sts) {
                           *have_domain_lock = 1;
                           CC_CMSDB_DLM_UNLOCK(cmsdb_sublock);
                           *have_cmsdb_lock=0;
                       } else {
                           if (sts == EWOULDBLOCK) { /* timed out */

                             CC_CMSDB_DLM_UNLOCK(cmsdb_sublock);
                             *have_cmsdb_lock=0;
                             (void)mpsleep(&dmn_name, PZERO, (char *)NULL, 
                                           1*hz, (void *) NULL, 0);

                             if (dmn_dlm_lock_timeo < 
					CC_MAX_DMN_DLM_LOCK_TIMEO()) {
                                 dmn_dlm_lock_timeo++;
                             } else {
                                 dmn_dlm_lock_timeo=1;
                             }

                             goto retry;
                           } 
			   /* the callout would have paniked for other errors */
                       }
                   }
            } else {
                   /* 
                    * If took cmsdb shared, must release and re-take it 
                    * Not expected to happen since DMN_ID types always
                    * take the lock excl., and other types should find mntpt.
                    */
                   if (*have_cmsdb_lock && !have_cmsdb_excl) {
                        CC_CMSDB_DLM_UNLOCK(cmsdb_sublock);
                        *have_cmsdb_lock=0;
                        /* retry holding cmsdb lock exclusively */
                        get_domain_lock = 0;
                        goto retry;
                   }
            }
          } else { /* VERSW() != ENABLE */
              /* 
               * If took cmsdb shared, must release and re-take it 
               */
              if (*have_cmsdb_lock && !have_cmsdb_excl) {
                   CC_CMSDB_DLM_UNLOCK(cmsdb_sublock);
                   *have_cmsdb_lock=0;
                   /* retry holding cmsdb lock exclusively */
                   get_domain_lock = 0;
                   goto retry;
              }

          }
          /* do_local was initialized to 1 */
          sts = 0;
        }

        return sts;
}



static void
validate_syscall_structs(
    void
    )
{
    MS_DBG_ASSERT( BS_CLIENT_AREA_SZ == ML_CLIENT_AREA_SZ);
    MS_DBG_ASSERT( BFM_RSVD_CELLS == ML_BFM_RSVD_CELLS );
    MS_DBG_ASSERT( sizeof( uint32T ) == sizeof( u32T ) );
    MS_DBG_ASSERT( sizeof( statusT ) == sizeof( mlStatusT ) );
    MS_DBG_ASSERT( sizeof( bfTagT ) == sizeof( mlBfTagT ) );
    MS_DBG_ASSERT( sizeof( bsIdT ) == sizeof( mlIdT ) );
    MS_DBG_ASSERT( sizeof( bfDomainIdT ) == sizeof( mlBfDomainIdT ) );
    MS_DBG_ASSERT( sizeof( serviceClassT ) == sizeof( mlServiceClassT ) );
    MS_DBG_ASSERT( sizeof( struct dStat ) == sizeof( mlVolCountersT ) );
    MS_DBG_ASSERT( BS_FS_CONTEXT_SZ == ML_FS_CONTEXT_SZ );
    MS_DBG_ASSERT( sizeof( bsExtentDescT ) == sizeof( mlExtentDescT ) );
}


/*
 * bs_inherit
 *
 * This routine will copy the inheritable bitfile attributes from one
 * bitfile (the parent) to another (the child).  So, the child bitfile
 * inherits these attributes from the parent.
 *
 * NOTE:  the following bfInheritT types are NOT supported:
 *  BF_INHERIT_A_TO_I,  * copy parent's attributes to childs inheritable *
 *  BF_INHERIT_A_TO_A,  * copy parent's attributes to childs attributes *
 *  BF_INHERIT_A_TO_IA  * copy parent's attributes to childs both *
 */

statusT
bs_inherit(
    bfAccessT *parentbfap,
    bfAccessT *childbfap,
    bfInheritT inherit,
    ftxHT ftxH
    )
{
    statusT sts;
    bsBfAttrT childAttr;
    bsBfInheritAttrT parentInheritAttr;
    extern int AdvfsAllDataLogging;

    sts = bmtr_get_rec(parentbfap,
                       BSR_BF_INHERIT_ATTR,
                       (void *) &parentInheritAttr,
                       sizeof( parentInheritAttr ) );
    if (sts != EOK) {
        domain_panic(parentbfap->dmnP, "bs_inherit - bmtr_get_rec failed, return code = %d", sts);
        return(E_DOMAIN_PANIC);
    }

    if ((inherit == BF_INHERIT_I_TO_A) || (inherit == BF_INHERIT_I_TO_IA)) {
        sts = bmtr_get_rec_n_lk(childbfap,
                                BSR_ATTR,
                                (void *) &childAttr,
                                sizeof( childAttr ),
                                BMTR_LOCK );
        if (sts != EOK) {
            ADVFS_SAD1("bs_inherit - get child attr failed", sts);
        }

        bcopy( &parentInheritAttr, &childAttr.cl, sizeof( childAttr.cl ) );

        sts = bmtr_put_rec_n_unlk( childbfap,
                                   BSR_ATTR,
                                   &childAttr,
                                   sizeof( childAttr ),
                                   ftxH,
                                   BMTR_UNLOCK,
                                   0 );
        if (sts != EOK) {
            ADVFS_SAD1("bs_inherit - set child attr failed", sts);
        }

#ifdef ADVFS_SMP_ASSERT
        /*
         * If AdvfsAllDataLogging is active, then
         * update the access structure dataSafety field if the
         * inherited dataSafety attribute is different from the
         * current dataSafety value in the access structure.
         */
        if (AdvfsAllDataLogging) {
            if (childAttr.cl.dataSafety != childbfap->dataSafety) {
                childbfap->dataSafety = childAttr.cl.dataSafety;
            }
        }
#endif
    }

    if ((inherit == BF_INHERIT_I_TO_I) || (inherit == BF_INHERIT_I_TO_IA)) {
        /*
         * Child also inherits the inheritable attributes.
         */

        sts = bmtr_put_rec_n_unlk( childbfap,
                                   BSR_BF_INHERIT_ATTR,
                                   (void *) &parentInheritAttr,
                                   sizeof( parentInheritAttr ),
                                   ftxH,
                                   BMTR_NO_LOCK,
                                   0 );
        if (sts != EOK) {
            return sts;
        }
    }

    return EOK;
}


statusT
bs_inherit_init(
    bfAccessT *bfap,
    ftxHT ftxH
    )
{
    statusT sts;
    int sz;
    bsBfClAttrT bfClientAttr = { 0 };
    extern int AdvfsAllDataLogging;

#ifdef ADVFS_SMP_ASSERT
    if (AdvfsAllDataLogging) {
        bfClientAttr.dataSafety = BFD_FTX_AGENT;
    }
    else {
        bfClientAttr.dataSafety = BFD_NIL;
    }
#else
    bfClientAttr.dataSafety = BFD_NIL;
#endif

    bfClientAttr.reqServices = defServiceClass;

    sts = bmtr_put_rec_n_unlk( bfap,
                               BSR_BF_INHERIT_ATTR,
                               (void *) &bfClientAttr,
                               sizeof( bfClientAttr ),
                               ftxH,
                               BMTR_NO_LOCK,
                               0 );
    if (sts != EOK) {
        return sts;
    }

    return EOK;
}


/*
 * toke_it
 *
 * Copies the portion of "str" up to the matching "tokchar" into
 * "token", terminating "token" with a NULL and returning a pointer to
 * the character beyond the matching tokchar in str.  If "tokchar" is
 * not found in "str", NULL is returned.
 *
 * "token" must be large enough to contain the token.
 * either "tokchar" must be in "str" or "str" must have a NULL to
 * terminate the loop.
 */

char*
toke_it(
        char* str,      /* in - string to scan */
        char  tokchar,  /* in - token end character */
        char* token)    /* out - token found */
{
    char c;

    if ( !str ) {
        return NULL;
    }

    while ( c = (*token++ = *str++) ) {
        if ( c == tokchar ) {
            *(token - 1) = NULL;
            return str;
        }
    }

    return NULL;
}


/*
 * Give back some global statistics to the caller.
 */
void
bs_get_global_stats(opGetGlobalStatsT *stats)
{
    stats->NumAccess        = NumAccess;
    stats->FreeAccLen       = FreeAcc.len;
    stats->ClosedAccLen     = ClosedAcc.len;
    stats->AccAlloc         = access_structures_allocated;
    stats->AccDealloc       = access_structures_deallocated;
    stats->AccDeallocFail   = failed_access_structure_deallocations;
    stats->CleanupClosed    = cleanup_thread_calls;
    stats->CleanupSkipped   = cleanup_structs_skipped;
    stats->CleanupProcessed = cleanup_structs_processed;
}

int
advfs_post_kernel_event(char *evname, advfs_ev *advfs_event)
{
    int             i, ret;
    EvmEvent_t      ev;
    EvmStatus_t     status;
    EvmVarValue_t   value;

    /*
     * We should not be at interrupt level due to possible sleeps
     * in the event code.  If we are, the path that hits this assertion
     * should be changed to call EvmEventPostImmedVa().
     */
    MS_SMP_ASSERT(!AT_INTR_LVL());

    if ((status = EvmEventCreateVa(&ev, EvmITEM_NAME,evname,
           EvmITEM_NONE)) != EvmERROR_NONE) {
        printf("Failed to create event \"%s\"",evname);
        return -1;
    }

    if (advfs_event->special != NULL) {
        value.STRING = advfs_event->special;
        EvmVarSet(ev, "special", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->domain != NULL) {
        value.STRING = advfs_event->domain;
        EvmVarSet(ev, "domain", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->fileset != NULL) {
        value.STRING = advfs_event->fileset;
        EvmVarSet(ev, "fileset", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->directory != NULL) {
        value.STRING = advfs_event->directory;
        EvmVarSet(ev, "directory", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->clonefset != NULL) {
        value.STRING = advfs_event->clonefset;
        EvmVarSet(ev, "clonefset", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->renamefset != NULL) {
        value.STRING = advfs_event->renamefset;
        EvmVarSet(ev, "renamefset", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->user != NULL) {
        value.STRING = advfs_event->user;
        EvmVarSet(ev, "user", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->group != NULL) {
        value.STRING = advfs_event->group;
        EvmVarSet(ev, "group", EvmTYPE_STRING, value, 0, 0);
    }

    if (advfs_event->dirTag != NULL) {
        value.UINT64 = advfs_event->dirTag;
        EvmVarSet(ev, "dirtag", EvmTYPE_UINT64, value, 0, 0);

        value.UINT64 = advfs_event->dirPg;
        EvmVarSet(ev, "dirPgNm", EvmTYPE_UINT64, value, 0, 0);

        value.UINT64 = advfs_event->pgOffset;
        EvmVarSet(ev, "pgOffset", EvmTYPE_UINT64, value, 0, 0);
        
        /* send mail to root */
        
        EvmItemSetVa(ev, EvmITEM_PRIORITY, 500, EvmITEM_NONE);
    }

    if (advfs_event->frag != NULL) {
        value.UINT64 = advfs_event->frag;
        EvmVarSet(ev, "frag no", EvmTYPE_UINT64, value, 0, 0);
        
        value.UINT64 = advfs_event->fragtype;
        EvmVarSet(ev, "fragtype", EvmTYPE_UINT64, value, 0, 0);

        /* send mail to root */
        
        EvmItemSetVa(ev, EvmITEM_PRIORITY, 500, EvmITEM_NONE);
    }

    EvmItemSet(ev, EvmITEM_CLUSTER_EVENT, EvmTRUE);

    if ((status = EvmEventPost(ev)) != EvmERROR_NONE) {
        printf("Error posting event \"%s\"\n", evname);
        return -1;
    }

    return 0;
}

/* Remove any unlinked files from the DDL that were not reactivated by
 * recovery.  Also for each fileset in the domain, reset the M_FAILOVER
 * flag in mount structure to prohibit further reactivations from the DDL.
 */

int
msfs_process_deferred_delete_list(
                                  char* dmnName, /* in - domain to process */
                                  u_long flag    /* in - mount flags/get DmnTblLock? */
                                 )
{
    domainT *dmnP;
    int vdi, vdcnt;
    struct vd* vdp;
    bfSetT *bfSetp;
    bfsQueueT *entry;
    struct mount *mp;

    if (!(flag & M_GLOBAL_ROOT))
        DMNTBL_LOCK_WRITE( &DmnTblLock )    
    dmnP = domain_name_lookup(dmnName, flag);
    if (TEST_DMNP(dmnP) != EOK) {
        if (!(flag & M_GLOBAL_ROOT))
            DMNTBL_UNLOCK( &DmnTblLock )
        return(EBAD_DOMAIN_POINTER);
    }

    vdcnt = vdi = 1;
    while ((vdi <= BS_MAX_VDI) && (vdcnt <= dmnP->vdCnt)) {
        /* Bump refcnt here to keep the volume from being removed by a
         * racing thread.
         */
        if ( vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE) ) {
            del_clean_mcell_list (vdp, M_FAILOVER);
            vd_dec_refcnt( vdp );
            vdcnt++;
        }
        vdi++;
    }  /* end while */

    entry = dmnP->bfSetHead.bfsQfwd;
    while (entry != &dmnP->bfSetHead) {
        bfSetp = BFSET_QUEUE_TO_BFSETP(entry);

        /* Verify the bitfile set is available and reset M_FAILOVER */
        if (BFSET_VALID(bfSetp) && (bfSetp->fsRefCnt > 0) &&
            (bfSetp->fsnp != NULL)) {
            mp = bfSetp->fsnp->mountp;
            mp->m_flag &= ~M_FAILOVER;
        }
        entry = entry->bfsQfwd;
    }
    if (!(flag & M_GLOBAL_ROOT))
        DMNTBL_UNLOCK( &DmnTblLock )
    return 0;
}


/*  Called when CFS is manually relocating a domain to a new server.
 *  Set domain flag to BFD_CFS_RELOC_IN_PROGRESS.  This flag is never
 *  cleared since the domain will be deallocated following relocation.
 */

statusT
msfs_set_domain_relocating_flag(
                                bfDomainIdT dmnId    /* in */
                               )
{
    statusT sts;
    domainT *dmnp;

    sts = bs_domain_access( &dmnp, dmnId, FALSE );
    if (sts == EOK) {
        dmnp->dmnFlag |= BFD_CFS_RELOC_IN_PROGRESS;
        bs_domain_close( dmnp );
    }
    return sts;
}
