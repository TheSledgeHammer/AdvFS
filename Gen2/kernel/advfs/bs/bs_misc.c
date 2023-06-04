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
 *      This module contains misc. BS routines
 *
 */

#include <stdarg.h>
#include <sys/types.h>
#include <sys/user.h>
#include <sys/file.h>
#include <sys/vnode.h>
#include <sys/kdaemon_thread.h>
#include <sys/proc_iface.h>
#include <sys/kernel.h>		/* kernel global hz */
#include <limits.h>
#include <tcr/clu.h>		/* kern public */

#include <tcr/dlm.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <ms_assert.h>
#include <bs_public_kern.h>
#include <bs_extents.h>
#include <bs_migrate.h>
#include <bs_service_classes.h>
#include <bs_stg.h>
#include <fs_file_sets.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/fs_dir_routines.h>
#include <ms_osf.h>
#include <bs_ims.h>
#include <advfs_evm.h>
#include <bs_index.h>
#include <bs_snapshot.h>
#include <vfast.h>
#include <bs_bitfile_sets.h>
#include <bs_access.h>
#include <bs_domain.h>
#include <bs_public_kern.h>
#include <bs_sbm.h>
#ifdef OSDEBUG
#include <ess/ess_macro.h>
#endif

static int copyin_domain_and_set_names(char *in_dmnTbl, char *in_setName, char *in_setName2, char **out_dmnTbl, char **out_setName, char **out_setName2);
static statusT open_set(mode_t setMode, mode_t dmnMode, int setOwner, int dmnOwner, bfSetIdT setId, bfs_op_flags_t flags, bfSetT **retBfSetp, domainT **dmnPP);
static void bs_get_global_stats(opGetGlobalStatsT *stats);


extern int advfs_fstype;
extern advfs_access_ctrl_t advfs_acc_ctrl;

#define ADVFS_MODULE BS_MISC

static adv_status_t 
find_cfs_server (
        opTypeT          op,            /* in - syscall operation type */
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
 * structure.  When this function is called from advfs_setproplist_int(),
 * a dummy bfAttr structure is passed with just the dataSafety field
 * initialized.  If the check_privilege() routine is modified
 * to do any more checking, then you must also modify the initialization
 * of the dummy bfattrp setup in advfs_setproplist_int().
 *
 */
static int
check_privilege(
    bfAccessT *bfap,
    adv_bf_attr_t *bfattrp
    )
{
#ifdef Tru64_to_HPUX_Port_Comments
/*
 *
 * The user credentials pointer (for uid, gid, etc) is now accessed
 * through the p_cred() accessor for the proc structure and
 * through the kt_cred() accessor (TNC_CRED macro) for the kthread structure.
 *
 */
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
#endif /* Tru64_to_HPUX_Port_Comments */

    return 0;
}

int
advfs_real_syscall(
    opTypeT     opIndex,        /* in - msfs operation to be performed */
    libParamsT  *parmBuf,       /* in - ptr to op-specific parameters buffer;*/
                                /*      contents are modified. */
    int         parmBufLen      /* in - byte length of parmBuf */
    );



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
    int64_t n1,
    int64_t n2,
    int64_t n3
    )
{
    printf( "AdvFS Exception: Module = %s, Line = %d \n", module, line );

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
        sprintf( advfs_sad_buf1, 256 * sizeof(char), fmt, msg, n1, n2, n3 );
    } else {
        sprintf( advfs_sad_buf1, 256 * sizeof(char), fmt, "", n1, n2, n3 );
    }

    sprintf( advfs_sad_buf2, 256 * sizeof(char), advfs_sad_buf1, n1, n2, n3 );
    printf(advfs_sad_buf2);
    printf( "\n" );

    panic( advfs_sad_buf2 );
    return;
}


/*
 * write to console and syslog message buffer
 */
void
ms_printf(char *fmt, ...)
{
    va_list ap;

    va_start(ap,fmt);

    vprintf(fmt, ap);

    va_end(ap);
}

/*
 * Try to do a uprintf().  If at fails do an printf().
 */
#define PRFMAX 128
void
ms_uprintf(const char *fmt, ...)
{
    va_list valist;
    int n;
    char buf[PRFMAX];

    /*
     * Format the args using prf() into a
     * temporary buffer.  If nothing to
     * print, return.
     */
    va_start(valist, fmt);
    /* 
     * The 3 corresponds to PRF_STRING defined in h/mdep_kernprivate.h
     */
    n = prf(fmt, valist, 3, &buf[0], PRFMAX);
    va_end(valist);
    if (n <= 0)
        return;

    uprintf(buf);
}

/*
 * Do both a uprintf() and a printf().
 */
void
ms_uaprintf(const char *fmt, ...)
{
    va_list valist;
    int n;
    char buf[PRFMAX];

    /*
     * Format the args using prf() into a
     * temporary buffer.  If nothing to
     * print, return.
     */
    va_start(valist, fmt);
    /* 
     * The 3 corresponds to PRF_STRING defined in h/mdep_kernprivate.h
     */
    n = prf(fmt, valist, 3, &buf[0], PRFMAX);
    va_end(valist);
    if (n <= 0)
        return;

    printf(buf);
    uprintf(buf);
}

/*
 * bs_owner
 *
 * This routine may be used to determine if an object belongs
 * to the user associated with the current thread.
 */

int
bs_owner(
    adv_uid_t ouid  /* in - object's user id */
    )
{
#ifdef Tru64_to_HPUX_Port_Comments
    /*
     *  ----> u_nd not defined.
     */

    struct ucred *cred = u.u_nd.ni_cred;
    if (suser() || cred->cr_uid == ouid) {
        return TRUE;
    } else {
        return FALSE;
    }

#endif /* Tru64_to_HPUX_Port_Comments */

    return TRUE;

}

/*
 * bs_accessbile
 *
 * This routine may be used to determine if the user associated with
 * the current thread has access to an object.
 */

int
bs_accessible(
    adv_mode_t mode,        /* in - mode wanted */
    adv_mode_t omode,       /* in - object's permissions mode */
    adv_uid_t ouid,         /* in - object's uid */
    adv_gid_t ogid          /* in - object's gid */
    )
{
    struct ucred *cred = TNC_CRED();
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
        gp = CR_GROUPS(cred);

        for (i = 0; i < NGROUPS && *gp != NOGROUP; i++, gp++) {
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
    int error;
    unsigned int size;

    if (out_dmnName != NULL) {
        /* copy domain table name */
        *out_dmnName = ms_malloc( (size_t)BS_DOMAIN_NAME_SZ );

        error = copyinstr( in_dmnName, *out_dmnName, BS_DOMAIN_NAME_SZ, &size );
        if (error != 0) {
            ms_free( *out_dmnName );
            return error;
        }
    }

    if (out_setName != NULL) {
        /* copy set name 1 */
        *out_setName = ms_malloc( (size_t)BS_SET_NAME_SZ );

        error = copyinstr( in_setName, *out_setName, BS_SET_NAME_SZ, &size );
        if (error != 0) {
            ms_free( *out_setName );
            ms_free( *out_dmnName );
            return( error );
        }
    }

    if (out_setName2 != NULL) {
        /* copy set name 2 */
        *out_setName2 = ms_malloc( (size_t)BS_SET_NAME_SZ );

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
    adv_mode_t mode,   /* in - access mode (read, write, etc.) */
    int ownerOnly,     /* in - indicates if only owner is allowed access */
    bfDomainIdT dmnId, /* in - domain's id */
    domainT **dmnPP    /* out - pointer to open domain */
    )
{
    statusT sts;
    bfDmnParamsT *dmnParamsp = NULL;
    int dmnActive = FALSE, dmnOpen = FALSE;

    dmnParamsp = (bfDmnParamsT *)ms_malloc_no_bzero( sizeof( bfDmnParamsT ));

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
    uint32_t flag      /* in - set in cluster to designate mount path */
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
    bfs_op_flags_t flags, /* in - flags for bfs_open */
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

    sts = bfs_open( retBfSetp, setId, flags, FtxNilFtxH );
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
        (void) bs_bfs_close( *retBfSetp, FtxNilFtxH, BFS_OP_DEF );
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
    (void) bs_bfs_close( bfSetp, FtxNilFtxH, BFS_OP_DEF );
    close_domain( setId.domainId, dmnP, 0 );
}


#ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING
extern int advfs_locklog_syscall(int,void*,int);
#endif /* #ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING */

/*
 * advfs_real_syscall
 *
 * Implements the AdvFS system call.  Eventually it should be part of
 * the loadable portion of the filesystem.  The routine advfs_syscall()
 * should check to see if AdvFS is loaded before calling this routine.
 *
 * The scope of advfs_real_syscall() is limited strictly to
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
 * not call its own advfs_real_syscall(); in this sense, both
 * advfs_real_syscall() and the CFS syscall server can be considered entry
 * points to the second-level routines.
 * 
 * advfs_real_syscall() returns 0 if successful, AdvFS or system error if not.
 */


int
advfs_real_syscall(
    opTypeT     opIndex,        /* in - advfs  operation to be performed */
    libParamsT  *parmBuf,       /* in - ptr to op-specific parameters buffer;*/
                                /*      contents are modified. */
    int         parmBufLen      /* in - byte length of parmBuf */
    )
{

     libParamsT *libBufp = NULL;

    statusT sts = EOK;
    unsigned int size;
    int error=0;
    int docopyout = 0;
    struct vnode *vp = NULL;
    int which = 0;
    int do_local = 1;
    int fd = 0;
    struct file *fp = NULL;
    bfAccessT *bfap = NULL;
    int dlm_locked = 0;
    int have_domain_lock = 0 ;
    dlm_lkid_t cfs_domain_lock ;
    dlm_lkid_t cms_cmsdb_lock ;

    /* obtain an exlusive access DLM kernel lock so the mount struct pointers
     * won't get unmounted while we process the syscalls.
     *
     * This lock also serializes the syscalls clusterwide and prevents
     * a planned relocation from happening while a syscall is being 
     * processed.  
     *
     *It is released after exiting the big switch statement.
     */

#ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING
    /*
     * If this is a lock logging syscall, none of the preprocessing
     * for the other syscalls is appropriate. Just direct it off to
     * the lock logging syscall function.
     */

    if(    ( opIndex == OP_LOCKLOGINFO  )
        || ( opIndex == OP_LOCKLOG      )
        || ( opIndex == OP_LOCKLOGSTART )
        || ( opIndex == OP_LOCKLOGSTOP  ) )
	return advfs_locklog_syscall( opIndex, parmBuf, parmBufLen);
#endif /* #ifdef ADVFS_ENABLE_LOCK_HIER_LOGGING */

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

    
#ifdef OSDEBUG
    printf("advfs_real_syscall: opIndex - %d\n", opIndex);
#endif
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
            libBufp->getName.mp = ms_malloc( (size_t)(PATH_MAX+1) );

            error = copyinstr(savedmp,
                              libBufp->getName.mp,
                              PATH_MAX, &size);
            if (error){
                ms_free(libBufp->getName.mp);
                RAISE_EXCEPTION(error);
            }

            libBufp->getName.name = ms_malloc( (size_t)(PATH_MAX+1) );

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
                sts = advfs_syscall_op_get_name2(libBufp);

            copyout(&libBufp->getName.parentTag,
                    &parmBuf->getName.parentTag, sizeof(bfTagT));
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
            libBufp->undelGet.dirName = ms_malloc( (size_t)(PATH_MAX+1) );

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
                sts = advfs_syscall_op_undel_get(libBufp);

            ms_free(libBufp->undelGet.dirName);

            /** Copyouts **/
            copyout(&libBufp->undelGet.undelDirTag,
                    &parmBuf->undelGet.undelDirTag, sizeof(bfTagT));

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
            libBufp->undelAttach.undelDirName = 
                    ms_malloc( (size_t)(PATH_MAX+1) );

            error = copyinstr(saveundelDirname,
                              libBufp->undelAttach.undelDirName,
                              PATH_MAX, &size);
            if (error) {
                ms_free(libBufp->undelAttach.undelDirName);
                RAISE_EXCEPTION(error);
            }

            libBufp->undelAttach.dirName = ms_malloc( (size_t)(PATH_MAX+1) );

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
                sts = advfs_syscall_op_undel_attach(libBufp);
     
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
            libBufp->undelDetach.dirName = ms_malloc( (size_t)(PATH_MAX+1) );
            
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
                sts = advfs_syscall_op_undel_detach(libBufp, 0);
            ms_free(libBufp->undelDetach.dirName);


            /* no copyout */
            /* reuse OK */
            break;
        }

        case ADVFS_GET_IDX_BF_PARAMS: {

            /* Fall through to ADVFS_GET_BF_PARAMS; based on OpType,   */
            /* advfs_syscall_op_get_bf_params() will know what to do */

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

                fd = libBufp->getBfParams.fd;

                sts = advfs_syscall_op_get_bf_params(libBufp, opIndex, bfap);
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

                fd = libBufp->setBfAttributes.fd;

                sts = advfs_syscall_op_set_bf_attributes(libBufp, bfap, 0);
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

                fd = libBufp->getBfIAttributes.fd;

                sts = advfs_syscall_op_get_bf_iattributes(libBufp, bfap);
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

                fd = libBufp->setBfIAttributes.fd;

                sts = advfs_syscall_op_set_bf_iattributes(libBufp, bfap);
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

                fd = libBufp->moveBfMetadata.fd;

                sts = advfs_syscall_op_move_bf_metadata(libBufp, bfap);
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
                sts = advfs_syscall_op_get_vol_params(libBufp, opIndex);


            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_VOL_BF_DESCS: {
            bsBfDescT *libBfDesc;

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
                sts = advfs_syscall_op_get_vol_bf_descs(libBufp);

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
#ifdef Tru64_to_HPUX_Port_Comments
/*
 *                        ----> The following free was using M_NFS,
 *                              need to follow up with CFS/CMS folks.
 *                              There are multiple occurances of this,
 *				all have been changed to M_TEMP
 */
                            free(libBufp->getVolBfDescs.bfDesc, M_TEMP);
#endif /* Tru64_to_HPUX_Port_Comments */
                    ;

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

            if (!suser()) {
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
                sts = advfs_syscall_op_set_vol_ioq_params(libBufp);


            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_GET_XTNT_MAP: {
            /* These two ops are the same and BKUP should be removed.  It is
             * being left for backward command compatability until older ICs
             * are no longer supported.
             */
            bsExtentDescT *libXtntsArray = NULL;

            if (parmBufLen != sizeof(libBufp->getXtntMap)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libXtntsArray = libBufp->getXtntMap.xtntsArray;
            libBufp->getXtntMap.xtntsArray = NULL;


            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, FILE_DESC,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                bfap = osf_fd_to_bfap(libBufp->getXtntMap.fd,&fp);
                if (bfap == NULL) {
                    sts = EINVALID_HANDLE;
                }
                else {
                    sts = advfs_syscall_op_get_xtnt_map(libBufp, bfap); 
                }
                fd = libBufp->getXtntMap.fd;
            }

            /** Copyouts **/

            if (sts == EOK) {
                error = ms_copyout( libBufp->getXtntMap.xtntsArray,
                                    libXtntsArray,
                                    libBufp->getXtntMap.xtntArraySize *
                                        sizeof(bsExtentDescT) );
            }
            if (libBufp->getXtntMap.xtntsArray) {
                if (do_local)
                    ms_free( libBufp->getXtntMap.xtntsArray );
                else { /* xdr allocated this data */
                    kmem_free( libBufp->getXtntMap.xtntsArray,
                            libBufp->getXtntMap.xtntArraySize);

                }
            }
            if (sts != EOK){
                RAISE_EXCEPTION(sts);
            }

            if (error != 0) {
                RAISE_EXCEPTION( error );
            }

            /* Restore user pointer */
            libBufp->getXtntMap.xtntsArray = libXtntsArray;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_CLUDIO_XTNT_MAP: {
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
                    sts = advfs_syscall_op_get_cludio_xtnt_map(libBufp, bfap);
                }
                fd = libBufp->getCludioXtntMap.fd;
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
                else { /* xdr allocated this data */
                    kmem_free(libBufp->getCludioXtntMap.xtntsArray, 
                            libBufp->getCludioXtntMap.xtntArraySize *
                                        sizeof(bsExtentDescT) );
                }

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
                sts = advfs_syscall_op_get_dmn_params(libBufp);


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
                sts = advfs_syscall_op_get_dmnname_params(libBufp);

 
            free_domain_and_set_names(libBufp->getDmnNameParams.domain,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->getDmnNameParams.domain = libDomain;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_SET_DMNNAME_PARAMS: {

            char *libDomain = NULL;

            if (parmBufLen > sizeof(libBufp->setDmnNameParams)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->setDmnNameParams.domain;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->setDmnNameParams.domain,
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
                sts = advfs_syscall_op_set_dmnname_params(libBufp);


            free_domain_and_set_names(libBufp->setDmnNameParams.domain,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->setDmnNameParams.domain = libDomain;

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
                sts = advfs_syscall_op_get_bfset_params(libBufp);

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
                sts = advfs_syscall_op_set_bfset_params(libBufp);

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SET_BFSET_PARAMS_ACTIVATE: {
            char *libDomain = NULL;
            
            if (parmBufLen != sizeof(libBufp->setBfSetParamsActivate)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->setBfSetParamsActivate.dmnName;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->setBfSetParamsActivate.dmnName,
                        NULL,
                        NULL
                    );
            if (error != 0) {
                /* Restore user pointer */
                libBufp->setBfSetParamsActivate.dmnName = libDomain;
                RAISE_EXCEPTION( error );
            }
            
            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                sts = advfs_syscall_op_set_bfset_params_activate(libBufp);
            }

            /* Restore user pointer */
            libBufp->setBfSetParamsActivate.dmnName = libDomain;
            
            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_GET_BFSET_PARAMS_ACT: {
            char *libDomain = NULL;

            if (parmBufLen != sizeof(libBufp->getBfSetParamsAct)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointer */
            libDomain = libBufp->getBfSetParamsAct.dmnName;

            error = copyin_domain_and_set_names(
                    libDomain,
                    NULL,
                    NULL,
                    &libBufp->getBfSetParamsAct.dmnName,
                    NULL,
                    NULL
                    );
            if (error != 0) {
                /* Restore user pointer */
                libBufp->setBfSetParamsActivate.dmnName = libDomain;
                RAISE_EXCEPTION( error );
            }

            /** Operation **/
#ifdef CLU_LATER
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);
#else
            do_local = 1;
            sts = EOK;
#endif

            if (do_local && !sts)
                sts = advfs_syscall_op_get_bfset_params_activate(libBufp);

            /* Restore user pointer */
            libBufp->getBfSetParamsAct.dmnName = libDomain;

            docopyout++;                /* reuse OK */
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
                sts = advfs_syscall_op_fset_get_info(libBufp);

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
                sts = advfs_syscall_op_fset_create(libBufp, 0, 0);


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
                sts = advfs_syscall_op_fset_get_id(libBufp);

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
                sts = advfs_syscall_op_fset_get_stats(libBufp);

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
                sts = advfs_syscall_op_fset_delete(libBufp, 0);

            free_domain_and_set_names(libBufp->setDelete.domain,
                                      libBufp->setDelete.setName,
                                      NULL);

            /* Restore user pointers */
            libBufp->setDelete.domain = libDomain;
            libBufp->setDelete.setName = libSetName;

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SNAPSET_CREATE: {
            char *lib_parent_dmn_name = NULL;
            char *lib_parent_set_name = NULL;
            char *lib_snap_dmn_name = NULL;
            char *lib_snap_set_name = NULL;

            if (parmBufLen != sizeof(libBufp->snapCreate)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            lib_parent_dmn_name = libBufp->snapCreate.parent_dmn_name;
            lib_parent_set_name = libBufp->snapCreate.parent_set_name;
            lib_snap_dmn_name   = libBufp->snapCreate.snap_dmn_name;
            lib_snap_set_name   = libBufp->snapCreate.snap_set_name;

            error = copyin_domain_and_set_names(lib_parent_dmn_name,
                        lib_parent_set_name,
                        NULL,
                        &libBufp->snapCreate.parent_dmn_name,
                        &libBufp->snapCreate.parent_set_name,
                        NULL);
            if (error != 0) {RAISE_EXCEPTION( error );}

            error = copyin_domain_and_set_names(lib_snap_dmn_name,
                        lib_snap_set_name,
                        NULL,
                        &libBufp->snapCreate.snap_dmn_name,
                        &libBufp->snapCreate.snap_set_name,
                        NULL);
            if (error != 0) {
                free_domain_and_set_names(libBufp->snapCreate.parent_dmn_name,
                        libBufp->snapCreate.parent_set_name,
                        NULL);
                libBufp->snapCreate.parent_dmn_name = lib_parent_dmn_name;
                libBufp->snapCreate.parent_set_name = lib_parent_set_name;
                RAISE_EXCEPTION( error );
            }

            /** Operation **/
#if CLU_LATER
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = advfs_syscall_op_create_snapset(libBufp, 0);
#else
                sts = advfs_syscall_op_create_snapset(libBufp, 0);
#endif


            free_domain_and_set_names(libBufp->snapCreate.parent_dmn_name,
                                      libBufp->snapCreate.parent_set_name,
                                      NULL);
            free_domain_and_set_names(libBufp->snapCreate.snap_dmn_name,
                                      libBufp->snapCreate.snap_set_name,
                                      NULL);

            /* Restore user pointers */
            libBufp->snapCreate.parent_dmn_name = lib_parent_dmn_name;
            libBufp->snapCreate.parent_set_name = lib_parent_set_name;
            libBufp->snapCreate.snap_dmn_name = lib_snap_dmn_name;
            libBufp->snapCreate.snap_set_name = lib_snap_set_name;

            docopyout++;                /* reuse OK */
            break;
       }

       case ADVFS_GET_STATVFS: {
	    char *libDomain = NULL;
	    char *libSetName = NULL;

            if ((int32_t)parmBufLen > (int32_t)sizeof(libBufp->getStatvfs)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

	    /* copyins */
	    /* Preserve user pointer */
            libDomain = libBufp->getStatvfs.domainName;
	    libSetName = libBufp->getStatvfs.setName;

            error = copyin_domain_and_set_names(
                        libDomain,
			libSetName,
                        NULL,
                        &libBufp->getStatvfs.domainName,
			&libBufp->getStatvfs.setName,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts) {
                sts = advfs_syscall_op_get_statvfs(libBufp);
	    }

	    free_domain_and_set_names(libBufp->getStatvfs.domainName,
				      libBufp->getStatvfs.setName,
                                      NULL);

	    /* Restore user pointer */
            libBufp->getStatvfs.domainName = libDomain;
	    libBufp->getStatvfs.setName = libSetName;

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
                sts = advfs_syscall_op_fset_rename(libBufp, 0);

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

            if (!suser()) {
               RAISE_EXCEPTION( E_ACCESS_DENIED );
            }

#ifdef OSDEBUG
    printf("advfs_real_syscall: ADVFS_DMN_INIT parmbuflen - %d sizeof dmnInit - %d\n",
            parmBufLen, sizeof(libBufp->dmnInit));
#endif

	    /*
	     * TODO: This is a temporary kludge to 'dance'
	     * around a compatibilty problem with the user
	     * space code introduced when support was added
	     * for specifying 4/8k user file page size.
	     */
            if (parmBufLen < sizeof(libBufp->dmnInit)) {
		libBufp->dmnInit.userAllocFobs = 8;
            } else if (parmBufLen > sizeof(libBufp->dmnInit)) {
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

            libBufp->dmnInit.volName = ms_malloc( (size_t)(MAXPATHLEN+1) );

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

#ifdef OSDEBUG
            printf("advfs_real_syscall: ADVFS_DMN_INIT. dmn name - %s vol name - %s\n",
                    libBufp->dmnInit.domain, libBufp->dmnInit.volName);
#endif
            /** Operation **/
            sts = advfs_syscall_op_dmn_init(libBufp);

            ms_free( libBufp->dmnInit.volName );
            free_domain_and_set_names( libBufp->dmnInit.domain, NULL, NULL );

            /* Restore user pointers */
            libBufp->dmnInit.domain = libDomain;
            libBufp->dmnInit.volName = libVolName;


            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_GET_DMN_VOL_LIST: {
            vdIndexT *libVolIndexArray;

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
                sts = advfs_syscall_op_get_dmn_vol_list(libBufp);
            }

            /** Copyouts **/
            if (sts == EOK) {
                 error = ms_copyout(libBufp->getDmnVolList.volIndexArray,
                               libVolIndexArray,
                               libBufp->getDmnVolList.volIndexArrayLen *
                                   sizeof(uint32_t));
            }
            if (libBufp->getDmnVolList.volIndexArray) {
                if (do_local)
                        ms_free( libBufp->getDmnVolList.volIndexArray );
                else {
                    /* if the call was function shipped, then xdr allocated
                     * the memory for us
                     */
                    kmem_free( libBufp->getDmnVolList.volIndexArray,
                            libBufp->getDmnVolList.volIndexArrayLen * 
                            sizeof( vdIndexT ) );

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

            if (!suser()) {
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

                fd = libBufp->migrate.fd;

                sts = advfs_syscall_op_migrate(libBufp, bfap);
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
                sts = advfs_syscall_op_tag_stat(libBufp);

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_ADD_VOLUME: {
            char *libDomain = NULL;
            char *libVolName = NULL;

            if (!suser()) {
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

            libBufp->addVol.volName = ms_malloc( (size_t)(MAXPATHLEN+1) );

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
                sts = advfs_syscall_op_add_volume(libBufp, opIndex);

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
            if (!suser()) {
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
                sts = advfs_syscall_op_rem_volume(libBufp, opIndex);

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SS_DMN_OPS: {
            char *libDomain = NULL;

            if (!suser()) {
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
                sts = advfs_syscall_op_ss_dmn_ops(libBufp);

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

            if (!suser()) {
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
                sts = advfs_syscall_op_ss_set_params(libBufp);

            free_domain_and_set_names(libBufp->ssSetParams.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->ssSetParams.domainName = libDomain;

            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_SS_RESET_PARAMS: {
            char *libDomain = NULL;

            if (!suser()) {
		sts = E_MUST_BE_ROOT;
		goto HANDLE_EXCEPTION;
            }

            if (parmBufLen != sizeof(libBufp->ssSetParams)) {
		sts = EBAD_PARAMS;
		goto HANDLE_EXCEPTION;
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
            if (error != 0) {
		sts = error;
		goto HANDLE_EXCEPTION;
	    }

            /** Operation **/

            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = advfs_syscall_op_ss_reset_params(libBufp);

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

            if (!suser()) {
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
                sts = advfs_syscall_op_ss_get_params(libBufp);

            free_domain_and_set_names(libBufp->ssGetParams.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->ssGetParams.domainName = libDomain;

            docopyout++;
            break;
        }

        case ADVFS_SS_GET_FRAGLIST: {
            adv_frag_desc_t *libFragArray = NULL;
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
                sts = advfs_syscall_op_ss_get_fraglist(libBufp);

            /** Copyouts **/
            if ((sts == EOK) && (libBufp->ssGetFraglist.ssFragCnt > 0)) {
                error = ms_copyout( libBufp->ssGetFraglist.ssFragArray,
                                    libFragArray,
                                    libBufp->ssGetFraglist.ssFragCnt *
                                        sizeof(adv_frag_desc_t) );
            }
            if (libBufp->ssGetFraglist.ssFragArray) {
                if (do_local)
                    ms_free( libBufp->ssGetFraglist.ssFragArray );
                else {
                    kmem_free( libBufp->ssGetFraglist.ssFragArray,
                            libBufp->ssGetFraglist.ssFragArraySize);
                }

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
            adv_ss_hot_desc_t *libHotArray = NULL;

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
                sts = advfs_syscall_op_ss_get_hotlist(libBufp);

            /** Copyouts **/
            if ((sts == EOK) && (libBufp->ssGetHotlist.ssHotCnt > 0)) {
                error = ms_copyout( libBufp->ssGetHotlist.ssHotArray,
                                    libHotArray,
                                    libBufp->ssGetHotlist.ssHotCnt *
                                        sizeof(adv_ss_hot_desc_t) );
            }
            if (libBufp->ssGetHotlist.ssHotArray) {
                if (do_local)
                    ms_free( libBufp->ssGetHotlist.ssHotArray );
                else {
                    kmem_free( libBufp->ssGetHotlist.ssHotArray,
                            libBufp->ssGetHotlist.ssHotArraySize );

                }
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
            if (!suser()) {
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
            if (error != 0) {RAISE_EXCEPTION( error );}

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = advfs_syscall_op_add_rem_vol_svc_class(libBufp);
 
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

            if (!suser()) {
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

            libBufp->addRemVolDone.volName = ms_malloc( (size_t)(MAXPATHLEN+1));

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
                sts = advfs_syscall_op_add_rem_vol_done(libBufp);

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

        case ADVFS_SWITCH_LOG: {
            char *libDomain = NULL;

            if (!suser()) {
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
                sts = advfs_syscall_op_switch_log(libBufp, 0);


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

            if (!suser()) {
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
                sts = advfs_syscall_op_switch_root_tagdir(libBufp);


            free_domain_and_set_names(libBufp->switchRootTagDir.domainName,
                                      NULL,
                                      NULL);

            /* Restore user pointer */
            libBufp->switchRootTagDir.domainName = libDomain;


            /* no copyout */ /* reuse OK */
            break;
        }

        case ADVFS_RESET_FREE_SPACE_CACHE: {

            if (!suser()) {
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
                sts = advfs_syscall_op_reset_free_space_cache(libBufp);

            /* No copyout */
            break;
        }

        case ADVFS_GET_GLOBAL_STATS: {

            if (parmBufLen != sizeof(libBufp->getGlobalStats)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Operation **/
            sts = advfs_syscall_op_get_global_stats(libBufp);

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_EXTENDFS: {
            char *libDomain = NULL;
            char *libVolName = NULL;

            /*
             * This is a configuration change so the caller must be super user.
             */
            if (!suser()) {
                RAISE_EXCEPTION( E_MUST_BE_ROOT );
            }

            if (parmBufLen != sizeof(libBufp->extendFsParams)) { 
                RAISE_EXCEPTION( EBAD_PARAMS );
            }

            /** Copyins **/

            /* Preserve user pointers */
            libDomain = libBufp->extendFsParams.domain;
            libVolName = libBufp->extendFsParams.blkdev;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->extendFsParams.domain,
                        NULL,
                        NULL
                    );
            if (error != 0) {RAISE_EXCEPTION( error );}

            libBufp->extendFsParams.blkdev = ms_malloc( (size_t)(MAXPATHLEN+1));

            error = copyinstr( libVolName, libBufp->extendFsParams.blkdev,
                               MAXPATHLEN-1, &size );
            if (error != 0) {
                sts = error;
                goto extend_vd_exit;
            }
            libBufp->extendFsParams.blkdev[size] = '\0';

            /** Operation **/
            if (clu_is_ready())
                sts = find_cfs_server (opIndex, libBufp, DMN_NAME,
                                       &do_local, &have_domain_lock,
                                       &dlm_locked, &cfs_domain_lock,
                                       &cms_cmsdb_lock);

            if (do_local && !sts)
                sts = advfs_syscall_op_extendfs(libBufp, 0L, 0);
          
        extend_vd_exit:
            free_domain_and_set_names(libBufp->extendFsParams.domain,
                                      NULL, NULL);
            if (libBufp->extendFsParams.blkdev != NULL) {
                ms_free( libBufp->extendFsParams.blkdev );
            }

            /* Restore user pointers */
            libBufp->extendFsParams.domain = libDomain;
            libBufp->extendFsParams.blkdev = libVolName;

            docopyout++;                /* reuse OK */
            break;
        }

        case ADVFS_DMN_RECREATE: {

            char *libDomain = NULL;

            if (parmBufLen > sizeof(libBufp->dmnRecreate)) {
                RAISE_EXCEPTION( EBAD_PARAMS );
            }


            /** Copyins **/
            libDomain = libBufp->dmnRecreate.domain;

            error = copyin_domain_and_set_names(
                        libDomain,
                        NULL,
                        NULL,
                        &libBufp->dmnRecreate.domain,
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
                sts = advfs_syscall_op_dmn_recreate(libBufp);

            free_domain_and_set_names(libBufp->dmnRecreate.domain,
                                      NULL,
                                      NULL);

            libBufp->dmnRecreate.domain = libDomain;
            
            docopyout++;                /* reuse OK */

            break;
        }

        /*** End of cases ***/

        default: {
            /* no copyout */ /* reuse OK */

#ifdef OSDEBUG
            printf("advfs_real_syscall: unsupported opcode - %d\n", opIndex);
#endif
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
                CLU_DMN_NAME_DLM_UNLOCK(&cfs_domain_lock);
                have_domain_lock = 0;
            }
            else {
                if (dlm_locked) {
                    CLU_CMSDB_DLM_UNLOCK(&cms_cmsdb_lock);
                    dlm_locked = 0;
	        }	
            }

            /* Let's sleep a sec before retrying */
            delay(hz);
            goto RETRY ;
        }
        RAISE_EXCEPTION( sts );
    }

    if (docopyout)
        error = ms_copyout (libBufp, parmBuf, parmBufLen);

    sts = error;

HANDLE_EXCEPTION:

    if (clu_is_ready()) {
        if (have_domain_lock) {
            CLU_DMN_NAME_DLM_UNLOCK(&cfs_domain_lock) ;
        }
        else {
            if (dlm_locked)
                CLU_CMSDB_DLM_UNLOCK(&cms_cmsdb_lock);
        }

    }

    /*
     * A valid bfap implies a successful call to osf_fd_to_bfap()
     * was made.  This requires a PUTF (to match the GETF done
     * in osf_fd_to_bfap() in order to avoid a panic
     * when exiting the system call.
     */
    if (bfap != NULL) {
	PUTF(fd);
    }


#ifdef Tru64_to_HPUX_Port_Comments
/*
 *  ----> Not sure if this is needed in HPUX.
 *                              u_file_state was not defined.
 */
    if (bfap != NULL) {
        FP_UNREF_MT (fp, &u.u_file_state);
    }
#endif /* Tru64_to_HPUX_Port_Comments */



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
 *  except by advfs_real_syscall(), and the CFS server code.       * 
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
adv_status_t
advfs_syscall_op_get_name2(libParamsT *libBufp)
{

    return advfs_get_name(libBufp->getName.mp,
                          libBufp->getName.tag,
                          libBufp->getName.name,
                          &libBufp->getName.parentTag);
}


/* ADVFS_UNDEL_GET */
adv_status_t
advfs_syscall_op_undel_get(libParamsT *libBufp)
{
    return get_undel_dir(libBufp->undelGet.dirName,
                         &libBufp->undelGet.undelDirTag);
}


/* ADVFS_UNDEL_ATTACH */
adv_status_t
advfs_syscall_op_undel_attach(libParamsT *libBufp)
{
    return advfs_attach_undel_dir(libBufp->undelAttach.dirName,
                            libBufp->undelAttach.undelDirName);
}


/* ADVFS_UNDEL_DETACH */
adv_status_t
advfs_syscall_op_undel_detach(libParamsT *libBufp, ftxIdT  xid)
{
    return advfs_detach_undel_dir(libBufp->undelDetach.dirName, xid);
}


/* ADVFS_GET_BF_PARAMS, ADVFS_GET_IDX_BF_PARAMS */
adv_status_t
advfs_syscall_op_get_bf_params(libParamsT *libBufp, 
                              opIndexT opIndex,
                              struct bfAccess *bfap)
{
    bfParamsT *bfparamsp = NULL;
    adv_bf_attr_t* bfattrp = &libBufp->getBfParams.bfAttributes;
    adv_bf_info_t* bfinfop = &libBufp->getBfParams.bfInfo;
    adv_status_t sts;

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
    bfattrp->reqService = (serviceClassT)bfparamsp->cl.reqServices;

    if ( bfparamsp->bfpBlocksAllocated == bfparamsp->bfpNextFob ) {
        bfattrp->initialSize = ADVFS_FOB_SZ * bfparamsp->bfpBlocksAllocated;
    } else {
        bfattrp->initialSize = 0;
    }


    /* Note: the union (bfattrp->attr) is not always completely
       filled in here. */

    /* acl, rsvd_sec{1,2,3,4}, fill{2} are not filled in here. */

    /* this set covers the entirety of *bfinfop */
    bfinfop->dataSafety = bfap->dataSafety;
    bfinfop->pageSize = bfparamsp->bfpPageSize;
    bfinfop->mbfNumFobs = bfparamsp->bfpBlocksAllocated;
    bfinfop->mbfNextFob = bfparamsp->bfpNextFob;

    bfinfop->volIndex = bfparamsp->vdIndex;
    bfinfop->tag = *(bfTagT*)&bfparamsp->tag;
    bfinfop->mbfSnapType = bfparamsp->bfpSnapType;

    bfinfop->mbfRsvdFileSize = bfparamsp->bfpRsvdFileSize;

HANDLE_EXCEPTION:

    if (bfparamsp)
       ms_free (bfparamsp);

    return(sts);

}


/* ADVFS_SET_BF_ATTRIBUTES */
adv_status_t
advfs_syscall_op_set_bf_attributes(libParamsT *libBufp,
                                  struct bfAccess *bfap,
                                  ftxIdT xid)
{
    bfParamsT *bfparamsp = NULL;
    adv_bf_attr_t* bfattrp = &libBufp->setBfAttributes.bfAttributes;
    int error;
    adv_status_t sts;

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

    bfparamsp->cl.reqServices = (serviceClassT)bfattrp->reqService;

    sts = bs_set_bf_params(bfap, bfparamsp);
    if ( sts != EOK ) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }


HANDLE_EXCEPTION:

    if (bfparamsp)
       ms_free (bfparamsp);

    return(sts);

}


/* ADVFS_GET_BF_IATTRIBUTES */
adv_status_t
advfs_syscall_op_get_bf_iattributes(libParamsT *libBufp, struct bfAccess *bfap)
{
    bfIParamsT *bfiparamsp = NULL;
    adv_bf_attr_t* bfattrp = &libBufp->setBfIAttributes.bfAttributes;
    adv_status_t sts;

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
    bfattrp->reqService = (serviceClassT)bfiparamsp->reqServices;
    bfattrp->initialSize = 0;

HANDLE_EXCEPTION:

    if (bfiparamsp)
       ms_free (bfiparamsp);

    return(sts);

}


/* ADVFS_SET_BF_IATTRIBUTES */
adv_status_t
advfs_syscall_op_set_bf_iattributes(libParamsT *libBufp, struct bfAccess *bfap)
{
    bfIParamsT *bfiparamsp = NULL;
    adv_bf_attr_t* bfattrp = &libBufp->setBfIAttributes.bfAttributes;
    int error;
    adv_status_t sts;

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

    bfiparamsp->reqServices = (serviceClassT)bfattrp->reqService;

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
adv_status_t
advfs_syscall_op_move_bf_metadata(libParamsT *libBufp, struct bfAccess *bfap)
{
    adv_status_t sts;

    sts = bs_move_metadata(bfap,NULL);

    return sts;
}


/* ADVFS_GET_VOL_PARAMS, ADVFS_GET_VOL_PARAMS2 */
adv_status_t
advfs_syscall_op_get_vol_params(libParamsT *libBufp, opIndexT opIndex)
{
    domainT *dmnP;
    bsVdParamsT *vdparamsp = NULL;
    int i, dmn_ref=FALSE;
    adv_status_t sts;

    adv_vol_info_t* vinfop = &libBufp->getVolParams.volInfo;
    adv_vol_prop_t* vpropp = &libBufp->getVolParams.volProperties;
    adv_vol_ioq_params_t* vioqp = &libBufp->getVolParams.volIoQParams;
    adv_vol_counters_t* vcntp = &libBufp->getVolParams.volCounters;
    int *flagp = &libBufp->getVolParams.flag;

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->getVolParams.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmn_ref=TRUE;

    vdparamsp = (bsVdParamsT *) ms_malloc_no_bzero(sizeof(bsVdParamsT));

    /*
     * Fetch ioq stats which allow counting # bufs per interval.
     * ADVFS_GET_VOL_PARAMS returns just a clone.
     */
    if (opIndex == ADVFS_GET_VOL_PARAMS2) *flagp |= GETVOLPARAMS2;

    sts = bs_get_vd_params(dmnP,
                           (vdIndexT) libBufp->getVolParams.volIndex,
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
        vinfop->sbmBlksBit = vdparamsp->stgCluster;
        vinfop->freeClusters = vdparamsp->freeClusters;
        vinfop->totalClusters = vdparamsp->totalClusters;
    }

    /* these cover the entirety of *vpropp */
    vpropp->serviceClass = vdparamsp->serviceClass;
    vpropp->bmtXtntFobs = vdparamsp->bmtXtntPgs * ADVFS_METADATA_PGSZ_IN_FOBS;

    for (i = 0; i < BS_VD_NAME_SZ; i += 1) {
        vpropp->volName[i] = vdparamsp->vdName[i];
    }

    /* These cover the entirety of *vioq.
     * The rdMaxIo and wrMaxIo are from disk and returned to caller
     * in terms of device blocks. The max, min and preferred sizes are
     * in terms of bytes.
     */
    vioqp->rdMaxIo = vdparamsp->rdMaxIo;
    vioqp->wrMaxIo = vdparamsp->wrMaxIo;
    vioqp->preferred_iosize = vdparamsp->preferred_iosize;
    vioqp->max_iosize = vdparamsp->max_iosize;
    vioqp->min_iosize = 
        (MAX(dmnP->userAllocSz, dmnP->metaAllocSz) /
         ADVFS_FOBS_PER_DEV_BSIZE) * DEV_BSIZE;
    *vcntp = *(adv_vol_counters_t*)(&vdparamsp->dStat);

HANDLE_EXCEPTION:

    if (dmn_ref)
        close_domain(libBufp->getVolParams.bfDomainId, dmnP, 0);

    if (vdparamsp)
        ms_free (vdparamsp);

    return(sts);
}


/* ADVFS_GET_VOL_BF_DESCS */
adv_status_t
advfs_syscall_op_get_vol_bf_descs(libParamsT *libBufp)
{
    domainT *dmnP;
    adv_status_t sts;
    vdT* vdp;

    libBufp->getVolBfDescs.bfDesc =
                (bsBfDescT *)ms_malloc (
                    libBufp->getVolBfDescs.bfDescSize * sizeof(bsBfDescT)
                );

    sts = open_domain(BS_ACC_READ,
                      ANYONE,
                      libBufp->getVolBfDescs.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    /* Check the disk */
    {
        vdIndexT vdi = (vdIndexT) libBufp->getVolBfDescs.volIndex;
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            close_domain (libBufp->getVolBfDescs.bfDomainId, dmnP, 0);
            return(EBAD_VDI);  /* Exception */
        }
    }

    sts = bmt_get_vd_bf_descs (dmnP,
                               (vdIndexT) libBufp->getVolBfDescs.volIndex,
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
adv_status_t
advfs_syscall_op_set_vol_ioq_params(libParamsT *libBufp)
{
    domainT *dmnP;
    bsVdParamsT *vdparamsp = NULL;
    adv_vol_ioq_params_t* vioqp = &libBufp->setVolIoQParams.volIoQParams;
    int dmn_ref=FALSE;
    adv_status_t sts;

    sts = open_domain(BS_ACC_WRITE,
                      OWNER_ONLY,
                      libBufp->setVolIoQParams.bfDomainId,
                      &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);  /* Exception */
    }
    dmn_ref=TRUE;

    vdparamsp = (bsVdParamsT *) ms_malloc_no_bzero(sizeof(bsVdParamsT));

    sts = bs_get_vd_params(dmnP,
                           (vdIndexT) libBufp->setVolIoQParams.volIndex,
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

    sts = bs_set_vd_params(dmnP,
                           (vdIndexT) libBufp->setVolIoQParams.volIndex,
                           vdparamsp);

HANDLE_EXCEPTION:

    if (dmn_ref)
        close_domain(libBufp->setVolIoQParams.bfDomainId, dmnP, 0);

    if (vdparamsp)
        ms_free (vdparamsp);

    return(sts);
}


/* ADVFS_GET_XTNT_MAP */
adv_status_t
advfs_syscall_op_get_xtnt_map(libParamsT *libBufp, struct bfAccess *bfap)
{
    vdIndexT volIndex;
    adv_status_t sts;
    struct bfAccess *fromBfap=bfap;

    /*
     * If the caller only wants a count of extents, don't
     * bother allocating an array to hold them.
     */
    if (libBufp->getXtntMap.xtntArraySize != 0) {

        libBufp->getXtntMap.xtntsArray =
                (bsExtentDescT *)ms_malloc (
                                            libBufp->getXtntMap.xtntArraySize *
                                            sizeof(bsExtentDescT)
                                            );

    } else {
        libBufp->getXtntMap.xtntsArray = NULL;
    }

    sts = advfs_get_xtnt_map( fromBfap,
                        libBufp->getXtntMap.startXtnt,
                        libBufp->getXtntMap.xtntArraySize,
                        libBufp->getXtntMap.xtntsArray,
                        &libBufp->getXtntMap.xtntCnt,
                        &libBufp->getXtntMap.gxm_fob_cnt,
                        &volIndex,
                        MPF_XTNTMAPS);

    libBufp->getXtntMap.allocVolIndex = volIndex;

    return(sts);
}

/* ADVFS_GET_CLUDIO_XTNT_MAP */
adv_status_t
advfs_syscall_op_get_cludio_xtnt_map(libParamsT *libBufp, struct bfAccess *bfap)
{
    vdIndexT volIndex;
    adv_status_t sts;
    int vdCnt; 
    vdIndexT vdi;
    bf_fob_t fob_cnt = 0;
    map_flags_t map_flags = MPF_NO_FLAGS;

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

    if (libBufp->getCludioXtntMap.xtntsArray == NULL) {
        map_flags =  MPF_XTNT_CNT;
    } else {
        map_flags = MPF_XTNTMAPS;
    }
   
    /* This will do the right thing whether we are operating
     * on the original file or a snapshot.
     */
    sts = advfs_get_xtnt_map( bfap,
                        libBufp->getCludioXtntMap.startXtnt,
                        libBufp->getCludioXtntMap.xtntArraySize,
                        libBufp->getCludioXtntMap.xtntsArray,
                        &libBufp->getCludioXtntMap.xtntCnt,
                        &fob_cnt,
                        &volIndex,
                        map_flags );

                              
    libBufp->getCludioXtntMap.allocVolIndex = volIndex;
    
    /* Up to here, this is a similar operation to ADVFS_GET_XTNT_MAP.
     * However, we have some additional fields to fill in.
     */
    libBufp->getCludioXtntMap.pg_size   = bfap->bfPageSz * ADVFS_FOB_SZ;
    libBufp->getCludioXtntMap.file_size = bfap->file_size;
    /*
     * If we only wanted counts of extents, then 
     * we also only want the max volumes per domain value
     * the actual volume devt list
     */
    
    /*
     * Get count of dev_t's
     */
    ADVMTX_DOMAINVDPTBL_LOCK( &bfap->dmnP->vdpTblLock );
    vdCnt = 0;
    for ( vdi = 0; vdCnt < bfap->dmnP->vdCnt && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfap->dmnP->vdpTbl[vdi] )
            vdCnt++;
    }
    ADVMTX_DOMAINVDPTBL_UNLOCK( &bfap->dmnP->vdpTblLock );

    libBufp->getCludioXtntMap.volMax = vdi;
    
    if (libBufp->getCludioXtntMap.xtntArraySize == 0) {
        goto _exit;
    }

    libBufp->getCludioXtntMap.devtvec = (bsDevtListT *)
        ms_malloc(sizeof(bsDevtListT));

    if (libBufp->getCludioXtntMap.devtvec == NULL){
        sts = ENO_MORE_MEMORY;
        goto _exit;
    }
    
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

    ADVMTX_DOMAINVDPTBL_LOCK( &bfap->dmnP->vdpTblLock );
    vdCnt = 0;
    for ( vdi = 0; vdCnt < bfap->dmnP->vdCnt && vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfap->dmnP->vdpTbl[vdi] )
	    libBufp->getCludioXtntMap.volMax = vdi + 1;

	if ( VDI_IS_VALID(vdi+1, bfap->dmnP) ) {
            vdT *vdp = bfap->dmnP->vdpTbl[vdi];
            MS_SMP_ASSERT( vdp->vdMagic == VDMAGIC );
            ADVMTX_VD_STATE_LOCK( &vdp->vdStateLock );
            if (vdp->vdState == BSR_VD_MOUNTED)
            {
                libBufp->getCludioXtntMap.devtvec->device[vdi] = 
                    vdp->devVp->v_rdev;
                vdCnt++;
            } else
                libBufp->getCludioXtntMap.devtvec->device[vdi] = 0;
            ADVMTX_VD_STATE_UNLOCK( &vdp->vdStateLock );
        }
    }
    ADVMTX_DOMAINVDPTBL_UNLOCK( &bfap->dmnP->vdpTblLock );
    libBufp->getCludioXtntMap.devtvec->devtCnt = vdCnt;

_exit:
    return(sts);
}



/* ADVFS_GET_DMN_PARAMS */
adv_status_t
advfs_syscall_op_get_dmn_params(libParamsT *libBufp)
{
    domainT *dmnP;
    adv_status_t sts;

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

        sts = getLogStats(dmnP, &libBufp->getDmnParams.dmnParams.logStat);
    }

    close_domain(libBufp->getDmnParams.bfDomainId, dmnP, 0);

    return(sts);
}


/* ADVFS_GET_DMNNAME_PARAMS */
adv_status_t
advfs_syscall_op_get_dmnname_params(libParamsT *libBufp)
{
    adv_bf_dmn_params_t *DmnParamsp = &libBufp->getDmnNameParams.dmnParams;
    adv_status_t sts;

    /* zeros & fills in entirety of dmnParams: */
    sts = bs_get_dmntbl_params(libBufp->getDmnNameParams.domain,
                               (bfDmnParamsT *)DmnParamsp);

    return(sts);
}

/* ADVFS_SET_DMNNAME_PARAMS */
adv_status_t
advfs_syscall_op_set_dmnname_params(libParamsT *libBufp)
{
    adv_bf_dmn_params_t *dmnParamsp = &libBufp->setDmnNameParams.dmnParams;
    adv_status_t sts;
    bfDomainIdT dmnId;
    domainT *dmnP;
    int dmnActive = FALSE, dmnOpen = FALSE;

    uint64_t totalDmnBlks, totalDmnBlksUsed;
    uint32_t percentDmnUsed;
    advfs_ev advfs_event;
    struct timeval tv;
    time_t currentTime;

    sts = bs_bfdmn_tbl_activate(libBufp->setDmnNameParams.domain, 0, &dmnId);
    if (sts != EOK) {
        goto _error;
    }

    dmnActive = TRUE;

    sts = bs_domain_access(&dmnP, dmnId, FALSE);
    if (sts != EOK) {
        goto _error;
    }

    dmnOpen = TRUE;

    sts = bs_set_dmn_params(dmnP, (bfDmnParamsT *)dmnParamsp);
    if (sts != EOK) {
        goto _error;
    }

    if (!bs_accessible( BS_ACC_READ,
                        dmnParamsp->mode,
                        dmnParamsp->uid,
                        dmnParamsp->gid )) {
        sts = E_ACCESS_DENIED;
        goto _error;
    }

    bs_domain_close( dmnP );
    (void) bs_bfdmn_deactivate( dmnId, 0 );

    /*
     * Make a preliminary upper threshold crossing check if we 
     * are setting upper limit threshold values and generate
     * an EVM event if appropriate.  We are not checking the
     * lower limit here.  Lower limit event generation at this
     * point is probably just noise to the admin.
     */
    totalDmnBlks     = dmnP->totalBlks;
    totalDmnBlksUsed = totalDmnBlks - dmnP->freeBlks;
    percentDmnUsed   = ((totalDmnBlksUsed * 100) / totalDmnBlks);

    if (dmnP->dmnThreshold.threshold_upper_limit) {

        if (percentDmnUsed >= dmnP->dmnThreshold.threshold_upper_limit) {
            tv = get_system_time();

            /* make sure tv_sec size does not change without us knowing */
            MS_SMP_ASSERT(sizeof(tv.tv_sec) == sizeof(currentTime));
            currentTime = tv.tv_sec;

            bzero(&advfs_event, sizeof(advfs_event));

            advfs_event.domain = dmnP->domainName;
            advfs_event.threshold_domain_upper_limit =
                dmnP->dmnThreshold.threshold_upper_limit;

            sprintf (advfs_event.formatMsg,
                sizeof(advfs_event.formatMsg),
                "Upper Threshold crossed in AdvFS filesystem %s",
                dmnP->domainName);

            advfs_post_kernel_event(EVENT_FILESYSTEM_THRESHOLD_UPPER_CROSSED,
                                    &advfs_event);

            dmnP->dmnThreshold.time_of_last_upper_event = currentTime;
        }
    }

    return EOK;

_error:
    if (dmnOpen) {
        bs_domain_close( dmnP );
    }

    if (dmnActive) {
        (void) bs_bfdmn_deactivate( dmnId, 0 );
    }

    return(sts);
}

/* ADVFS_GET_BFSET_PARAMS */
adv_status_t
advfs_syscall_op_get_bfset_params(libParamsT *libBufp)
{
    domainT *dmnP;
    bfSetT *bfSetp;
    adv_status_t sts;

    sts = open_set( BS_ACC_READ, BS_ACC_READ,
                    ANYONE, ANYONE,
                    libBufp->getBfSetParams.bfSetId, BFS_OP_IGNORE_OUT_OF_SYNC,
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
adv_status_t
advfs_syscall_op_set_bfset_params(libParamsT *libBufp)
{

    domainT *dmnP;
    bfSetT *bfSetp;
    bfSetParamsT *BfSetParamsp = &libBufp->setBfSetParams.bfSetParams;
    int clear_flag = 0;
    adv_status_t sts;

    sts = open_set( BS_ACC_WRITE, BS_ACC_READ,
                    ANYONE, OWNER_ONLY,
                    libBufp->setBfSetParams.bfSetId, BFS_OP_DEF,
                    &bfSetp, &dmnP);

    if (sts != EOK) {
        return(sts);  /* Exception */
    }

    sts = bs_set_bfset_params(bfSetp, BfSetParamsp, 0);
    close_set(libBufp->setBfSetParams.bfSetId, bfSetp, dmnP);
    return(sts);
}

/* ADVFS_SET_BFSET_PARAMS_ACTIVATE */
adv_status_t
advfs_syscall_op_set_bfset_params_activate(libParamsT *libBufp)
{

    domainT *dmnP;
    bfSetT *bfSetp;
    bfSetIdT bfSetId;
    bfSetParamsT *BfSetParamsp =
                        &libBufp->setBfSetParamsActivate.bfSetParams;
    int clear_flag = 0;
    adv_status_t sts=0, sts2=0;

    sts = bs_bfset_activate(
                   libBufp->setBfSetParamsActivate.dmnName,
                   libBufp->setBfSetParamsActivate.bfSetParams.setName,
                   0,&bfSetId );

    if (sts != EOK) {
        goto _out;
    }

    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts != EOK) {
        goto _out;
    }

    sts = bs_set_bfset_params(bfSetp, BfSetParamsp, 0);

    (void) bs_bfs_close(bfSetp, FtxNilFtxH, BFS_OP_DEF);
    sts2 = bs_bfdmn_deactivate( bfSetId.domainId, 0 );
    if (sts2 != EOK) {
        sts = sts2;
    }

_out:
    return(sts);
}


/* ADVFS_GET_BFSET_PARAMS_ACT */
adv_status_t
advfs_syscall_op_get_bfset_params_activate(libParamsT *libBufp)
{
    bfDomainIdT dmn_id;
    bfSetT *bf_set_p;
    statusT sts;

    char *dmn_name = libBufp->getBfSetParamsAct.dmnName;
    int dmn_activated = FALSE, set_open = FALSE;

    sts = bs_bfdmn_tbl_activate( dmn_name, 0, &dmn_id );
    if (sts != EOK) {
        goto _error;
    }
    dmn_activated = TRUE;

    sts = bfs_open(&bf_set_p, libBufp->getBfSetParamsAct.bfSetId, 
            BFS_OP_IGNORE_OUT_OF_SYNC, FtxNilFtxH);
    if (sts != EOK) {
        goto _error;
    }
    set_open = TRUE;

    sts = bs_get_bfset_params(bf_set_p, 
            &libBufp->getBfSetParamsAct.bfSetParams, 0);

    if (sts != EOK) {
        goto _error;
    }

    bs_bfs_close(bf_set_p, FtxNilFtxH, BFS_OP_DEF);
    bs_bfdmn_deactivate( dmn_id, 0 );

    return EOK;

_error:

    if (set_open) {
        bs_bfs_close( bf_set_p, FtxNilFtxH, BFS_OP_DEF );
    }

    if (dmn_activated) {
        bs_bfdmn_deactivate( dmn_id, 0 );
    }

    return sts;
}


/* ADVFS_FSET_GET_INFO */
adv_status_t
advfs_syscall_op_fset_get_info(libParamsT *libBufp)
{
    return fs_fset_get_info(libBufp->setGetInfo.domain,
                            &libBufp->setGetInfo.nextSetIdx,
                            &libBufp->setGetInfo.bfSetParams,
                            &libBufp->setGetInfo.userId,
                            libBufp->setGetInfo.flag);
}


/* ADVFS_FSET_CREATE */
adv_status_t
advfs_syscall_op_fset_create(libParamsT *libBufp,
                            ftxIdT xid1,
                            ftxIdT xid2)
{
    advfs_ev *advfs_event;
    adv_status_t sts;

    sts = fs_fset_create(libBufp->setCreate.domain,
                         libBufp->setCreate.setName,
                         libBufp->setCreate.reqServ,
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
adv_status_t
advfs_syscall_op_fset_get_id(libParamsT *libBufp)
{
    return fs_fset_get_id(libBufp->setGetId.domain,
                          libBufp->setGetId.setName,
                          &libBufp->setGetId.bfSetId);
}


/* ADVFS_FSET_GET_STATS */
adv_status_t
advfs_syscall_op_fset_get_stats(libParamsT *libBufp)
{
    return fs_fset_get_stats(
               libBufp->setGetStats.domain,
               libBufp->setGetStats.setName,
               (fileSetStatsT *)&libBufp->setGetStats.fileSetStats
           );
}


/* ADVFS_FSET_DELETE */
adv_status_t
advfs_syscall_op_fset_delete(libParamsT *libBufp, ftxIdT xid)
{
    adv_status_t sts;

    sts = fs_fset_delete( libBufp->setDelete.domain,
                          libBufp->setDelete.setName,
                          xid );

    return(sts);
}

adv_status_t
advfs_syscall_op_create_snapset(libParamsT *libBufp, ftxIdT cfsXid)
{
    advfs_ev *advfs_event;
    adv_status_t sts;

    snap_flags_t snap_flags = 0;

    snap_flags |= (libBufp->snapCreate.snap_flags & SF_RDONLY) ? 
        SF_SNAP_READ : SF_SNAP_WRITE;

    sts = advfs_create_snapset( libBufp->snapCreate.parent_dmn_name,
            libBufp->snapCreate.parent_set_name,
            libBufp->snapCreate.snap_dmn_name,
            libBufp->snapCreate.snap_set_name,
            &libBufp->snapCreate.snap_set_id,
            snap_flags,
            cfsXid);

    if (sts == EOK) {
        advfs_event = (advfs_ev *)ms_malloc(sizeof(advfs_ev));
        advfs_event->domain = libBufp->snapCreate.parent_dmn_name;
        advfs_event->fileset = libBufp->snapCreate.parent_set_name;
        advfs_event->snapfset = libBufp->snapCreate.snap_set_name;
        advfs_post_kernel_event(EVENT_SNAPSET_CREATE, advfs_event);
        ms_free(advfs_event);
    }

    return(sts);
}



/* ADVFS_FSET_RENAME */
adv_status_t
advfs_syscall_op_fset_rename(libParamsT *libBufp, ftxIdT xid)
{

    advfs_ev *advfs_event;
    adv_status_t sts;

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
adv_status_t
advfs_syscall_op_dmn_init(libParamsT *libBufp)
{

    advfs_ev *advfs_event;
    int error;
    adv_status_t sts;

    sts = bs_dmn_init( libBufp->dmnInit.domain,
                       libBufp->dmnInit.maxVols,
                       libBufp->dmnInit.logFobs,
                       libBufp->dmnInit.userAllocFobs,
                       libBufp->dmnInit.logSvc,
                       libBufp->dmnInit.tagSvc,
                       libBufp->dmnInit.volName,
                       libBufp->dmnInit.volSvc,
                       libBufp->dmnInit.volSize,
                       libBufp->dmnInit.bmtXtntFobs,
                       libBufp->dmnInit.bmtPreallocFobs,
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
adv_status_t
advfs_syscall_op_get_dmn_vol_list(libParamsT *libBufp)
{
    domainT *dmnP;
    adv_status_t sts;

    if (libBufp->getDmnVolList.volIndexArrayLen < 1) {
        return ( EBAD_PARAMS );
    }

    libBufp->getDmnVolList.volIndexArray =
                (uint32_t *)ms_malloc (
                    libBufp->getDmnVolList.volIndexArrayLen * sizeof(uint32_t)
                );

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
adv_status_t
advfs_syscall_op_migrate(libParamsT *libBufp, struct bfAccess *bfap)
{
    adv_status_t sts;

    sts =  bs_migrate(bfap,
                      (vdIndexT) libBufp->migrate.srcVolIndex,
                      libBufp->migrate.srcFobOffset,
                      libBufp->migrate.srcFobCnt,
                      (vdIndexT) libBufp->migrate.dstVolIndex,
                      libBufp->migrate.dstBlkOffset,
                      libBufp->migrate.forceFlag,
                      BS_ALLOC_DEFAULT);

    return sts;
}


/* ADVFS_TAG_STAT */
adv_status_t
advfs_syscall_op_tag_stat(libParamsT *libBufp)
{

    bfTagT tag;                  /* Last tag/current tag */
    bfSetT *bfSetp = NULL;       /* Fileset pointer */
    domainT *dmnP;               /* Domain pointer */
    bf_fob_t fob_cnt;            /* Number of fobs in the file */
    statT *fs_statp = NULL;      /* Stat structure for file */
    struct vnode *tagstatvp;     /* Vnode for file */
    bfAccessT *bfap = NULL;      /* Access structure for file */
    bfMCIdT mcid;                /* Primary mcell for file */
    adv_status_t sts;               /* Return status */

    /*
     * Allocate memory for a stat structure.  Too big for a stack variable.
     */
    fs_statp = (statT *) ms_malloc(sizeof(statT));

    /*
     * Open the fileset.
     */
    sts = open_set(BS_ACC_READ, BS_ACC_READ, ANYONE, ANYONE,
                   libBufp->tagStat.setId, BFS_OP_DEF, &bfSetp, &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }

    /*
     * Make sure it's mounted.
     */
    if (!bfSetp->fsnp) {
        RAISE_EXCEPTION(E_NO_SUCH_BF_SET);
    }

    tag = libBufp->tagStat.tag;

    /*
     * Loop until we find a tag such that:
     *
     *   1. The bitfile has a BMTR_FS_STAT record.
     *   2. The bitfile is either a member of a clone fileset
     *      or is not marked 'delete with clone'.
     */
    while (1) {

        /*
         * See if there is a tag in the fileset after 'tag'.
         */
        sts = tagdir_lookup_next(bfSetp, &tag, &mcid);
        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        /*
         * Open the file.
         */
        tagstatvp = NULL;
        sts = bs_access(&bfap, tag, bfSetp, FtxNilFtxH, BF_OP_INTERNAL,
                        &tagstatvp);
        if (sts) {
            RAISE_EXCEPTION(sts);
        }


        /*
         * If the file is marked to defer deletion until its
         * dependent snapshots are deleted, skip it.
         */
        if ( bfap->bfaFlags & BFA_DEL_WITH_CHILDREN ) {
            (void) bs_close(bfap, MSFS_CLOSE_NONE);
            bfap = NULL;
            continue;
        }

        /*
         * Make sure the file has a BMTR_FS_STAT record.  If not, skip it.
         */
        sts = bmtr_get_rec(bfap, BMTR_FS_STAT, fs_statp, sizeof(statT));
        if (sts == EBMTR_NOT_FOUND) {
            (void) bs_close(bfap, MSFS_CLOSE_NONE);
            bfap = NULL;
            continue;
        } else if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }


        /*
         * Use advfs_get_xtnt_map to count the fobs of storage 
         */
        fob_cnt = 0;
        {
            bsExtentDescT fake_xtnts;
            int32_t       fake_xtnt_cnt;
            vdIndexT      vd_idx;
            sts = advfs_get_xtnt_map( bfap,
                                0,      /* start xtnt */
                                0, 
                                &fake_xtnts,
                                &fake_xtnt_cnt,
                                &fob_cnt,
                                &vd_idx,
                                MPF_FOB_CNT );

        }

        if (sts != EOK) {
            RAISE_EXCEPTION(sts);
        }

        libBufp->tagStat.tag = tag;
        libBufp->tagStat.uid = fs_statp->st_uid;
        libBufp->tagStat.gid = fs_statp->st_gid;
        libBufp->tagStat.atime = fs_statp->st_atime;
        libBufp->tagStat.mode = fs_statp->st_mode;
        libBufp->tagStat.size = fob_cnt * ADVFS_FOB_SZ;

        /*
         * We got one.  We're done.
         */
        break;
    }

HANDLE_EXCEPTION:

    if (bfap) {
        (void) bs_close(bfap, MSFS_CLOSE_NONE);
    }

    if (bfSetp) {
        close_set(libBufp->tagStat.setId, bfSetp, dmnP);
    }

    if (fs_statp) {
        ms_free(fs_statp);
    }

    return(sts);
}


/* ADVFS_ADD_VOLUME */
adv_status_t
advfs_syscall_op_add_volume(libParamsT *libBufp, opIndexT opIndex)
{

    advfs_ev *advfs_event;
    int size, error;
    adv_status_t sts;
    int flag = 0;

    if (clu_is_ready()) {
        /* set flag if domain is known to cluster */
        if ( CLU_DMN_NAME_TO_MP(libBufp->addVol.domain) )  {
            flag = DMNA_MOUNTING;
            error = CLU_DOMAIN_CHANGE_VOL(&opIndex,
                                     libBufp->addVol.domain,
                                     libBufp->addVol.volName);
            if (error != 0) {
            	return(error);  /* Exception */
            }
	}
    }

    sts = bs_vd_add_active(libBufp->addVol.domain,
                           libBufp->addVol.volName,
                           &libBufp->addVol.volSvc,
                           libBufp->addVol.volSize,
                           libBufp->addVol.bmtXtntFobs,
                           libBufp->addVol.bmtPreallocFobs,
                           flag,
                           &libBufp->addVol.bfDomainId,
                           &libBufp->addVol.volIndex);

    if (flag) {
        (void)CLU_DOMAIN_CHANGE_VOL_DONE(&opIndex,
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
adv_status_t
advfs_syscall_op_rem_volume(libParamsT *libBufp, opIndexT opIndex)
{
    struct domain *dmnP;
    struct vd *vdp;
    int vdRefBumped = 0;
    char *vdName = NULL;
    int error, dmn_ref=FALSE;
    adv_status_t sts;
    int flag = 0;

    if(clu_is_ready() && CLU_FIND_CLUSTER_DOMAIN_ID((void *)&libBufp->remVol.bfDomainId))
	flag = DMNA_MOUNTING;

    sts = open_domain (BS_ACC_WRITE,
                       OWNER_ONLY,
                       libBufp->remVol.bfDomainId,
                       &dmnP);
    if (sts != EOK) {
        RAISE_EXCEPTION(sts);
    }
    dmn_ref=TRUE;

    vdName = ms_malloc( (size_t)(MAXPATHLEN+1) );

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

    if(flag & DMNA_MOUNTING) {
        error = CLU_DOMAIN_CHANGE_VOL(&opIndex, dmnP->domainName, vdName);
        if (error != 0) {
            RAISE_EXCEPTION(error);
        }
    }

    sts = bs_vd_remove_active( dmnP,
                               libBufp->remVol.volIndex,
                               libBufp->remVol.forceFlag);

    if(flag & DMNA_MOUNTING) {

        domainT *domain;
        bfSetIdT bfSetId;
        struct fileSetNode *fsnp;
        bfSetParamsT bfsetParams;
        uint64_t setIdx = 0;
        uint32_t userId;
        statusT sts2=EOK;
        struct vfs *vfsp = NULL;
        bfSetT* bfSetp;

        /*
         * to inform the cfs server that addvol is done,
         * so it's time to resume block reservation.
         */
        sts2 = bs_bfs_get_info( &setIdx, &bfsetParams, dmnP, &userId );

        MS_SMP_ASSERT(sts2 == EOK);
        if (sts2 == EOK) {
            bfSetId = bfsetParams.bfSetId;
            bfSetp = bfs_lookup(bfSetId);

            if (bfSetp != NULL && bfSetp->fsnp != NULL) {
                fsnp = bfSetp->fsnp;
                vfsp = fsnp->vfsp;
                CLU_FSFLUSH(vfsp, R_DONE);

            }
        }
        (void)CLU_DOMAIN_CHANGE_VOL_DONE(&opIndex,
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
adv_status_t
advfs_syscall_op_ss_dmn_ops(libParamsT *libBufp)
{
    adv_status_t sts=EOK;
    ssUIDmnOpsT opType = libBufp->ssDmnOps.type;
    bfDomainIdT domainId;
    domainT *dmnP;
    int dmnActive=FALSE, dmnOpen=FALSE;
    int oldState;

    switch (opType) {
        case SS_UI_RESTART:
            sts = ss_kern_start();
            break;
        case SS_UI_STOP:
            ss_kern_stop();
            break;
        case SS_UI_ACTIVATE:
            sts = advfs_ss_change_state(libBufp->ssDmnOps.domainName,
                                  SS_ACTIVATED,&oldState,1);
            break;
        case SS_UI_DEACTIVATE:
            sts = advfs_ss_change_state(libBufp->ssDmnOps.domainName,
                                  SS_DEACTIVATED,&oldState,1);
            break;
        case SS_UI_SUSPEND:
            sts = advfs_ss_change_state(libBufp->ssDmnOps.domainName,
                                  SS_SUSPEND,&oldState,1);
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
adv_status_t
advfs_syscall_op_ss_set_params(libParamsT *libBufp)
{
    statusT sts=EOK;
    bfDomainIdT domainId;
    adv_ss_dmn_params_t *ssDmnParamsp = &libBufp->ssSetParams.ssDmnParams;
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
    dmnP->ssDmnInfo.ssDmnDefragAll  = ssDmnParamsp->ssDmnDefragAll;  
    dmnP->ssDmnInfo.ssDmnSmartPlace = ssDmnParamsp->ssDmnSmartPlace;
    dmnP->ssDmnInfo.ssDmnBalance = ssDmnParamsp->ssDmnBalance;
    dmnP->ssDmnInfo.ssDmnVerbosity = ssDmnParamsp->ssDmnVerbosity;
    dmnP->ssDmnInfo.ssDmnDirectIo = ssDmnParamsp->ssDmnDirectIo;
    dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy = ssDmnParamsp->ssMaxPercentOfIoWhenBusy;
    if(dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy)
        dmnP->ssDmnInfo.ssIoInterval = 100 / (int) dmnP->ssDmnInfo.ssMaxPercentOfIoWhenBusy;
    else
        dmnP->ssDmnInfo.ssIoInterval = 0;

    dmnP->ssDmnInfo.ssAccessThreshHits = ssDmnParamsp->ssAccessThreshHits;
    dmnP->ssDmnInfo.ssSteadyState = ssDmnParamsp->ssSteadyState;
    dmnP->ssDmnInfo.ssMinutesInHotList = ssDmnParamsp->ssMinutesInHotList;
    dmnP->ssDmnInfo.ssExtentsConsol = ssDmnParamsp->ssExtentsConsol;
    dmnP->ssDmnInfo.ssPagesConsol = ssDmnParamsp->ssPagesConsol;
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

/* ADVFS_SS_RESET_PARAMS */
adv_status_t
advfs_syscall_op_ss_reset_params(libParamsT *libBufp)
{
    statusT sts=EOK;
    bfDomainIdT domainId;
    adv_ss_dmn_params_t *ssDmnParamsp = &libBufp->ssSetParams.ssDmnParams;
    domainT *dmnP;
    int dmnActive=FALSE;
    int dmnOpen=FALSE;

    sts = bs_bfdmn_tbl_activate( libBufp->ssSetParams.domainName,
                                 (uint64_t)0,
                                 &domainId );
    if (sts != EOK) {
	goto HANDLE_EXCEPTION;
    }

    dmnActive = TRUE;

    sts = bs_domain_access( &dmnP, domainId, FALSE );

    if (sts != EOK) {
	goto HANDLE_EXCEPTION;
    }
    dmnOpen = TRUE;

    dmnP->ssDmnInfo.ssFilesDefraged = ssDmnParamsp->ssFilesDefraged;
    dmnP->ssDmnInfo.ssFobsDefraged = ssDmnParamsp->ssFobsDefraged;
    dmnP->ssDmnInfo.ssExtentsConsol = ssDmnParamsp->ssExtentsConsol;
    dmnP->ssDmnInfo.ssFilesIOBal = ssDmnParamsp->ssFilesIOBal;
    dmnP->ssDmnInfo.ssFobsBalanced = ssDmnParamsp->ssFobsBalanced;
    dmnP->ssDmnInfo.ssPagesConsol = ssDmnParamsp->ssPagesConsol;

    /* write out on-disk smartstore record here. */
    sts = ss_put_rec (dmnP);
    if(sts != EOK) {
	goto HANDLE_EXCEPTION;
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
adv_status_t
advfs_syscall_op_ss_get_params(libParamsT *libBufp)
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
    libBufp->ssGetParams.ssDmnParams.ssDmnDefragAll =
                             dmnP->ssDmnInfo.ssDmnDefragAll;

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
    libBufp->ssGetParams.ssDmnParams.ssFobsDefraged =
                             dmnP->ssDmnInfo.ssFobsDefraged;
    libBufp->ssGetParams.ssDmnParams.ssFobsBalanced =
                             dmnP->ssDmnInfo.ssFobsBalanced;
    libBufp->ssGetParams.ssDmnParams.ssFilesIOBal =
                             dmnP->ssDmnInfo.ssFilesIOBal;
    libBufp->ssGetParams.ssDmnParams.ssExtentsConsol =
                             dmnP->ssDmnInfo.ssExtentsConsol;
    libBufp->ssGetParams.ssDmnParams.ssPagesConsol =
                             dmnP->ssDmnInfo.ssPagesConsol;
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
adv_status_t
advfs_syscall_op_ss_get_fraglist(libParamsT *libBufp)
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

        libBufp->ssGetFraglist.ssFragArray =
                (adv_frag_desc_t *)ms_malloc (
                                 libBufp->ssGetFraglist.ssFragArraySize *
                                 sizeof(adv_frag_desc_t)
                                         );
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
    ADVMTX_VOLINFO_FRAG_LOCK(&vdp->ssVolInfo.ssFragLk);

    fhp = &vdp->ssVolInfo.ssFragHdr;
    fp = fhp->ssFragRatFwd;  /* entry pointer */
    while ( fp != (ssFragLLT *) fhp ) {
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
    ADVMTX_VOLINFO_FRAG_UNLOCK(&vdp->ssVolInfo.ssFragLk);

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
adv_status_t
advfs_syscall_op_ss_get_hotlist(libParamsT *libBufp)
{
    statusT sts = EOK;
    bfDomainIdT bfDomainId;
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
        libBufp->ssGetHotlist.ssHotArray =
            (adv_ss_hot_desc_t *)ms_malloc(
                              libBufp->ssGetHotlist.ssHotArraySize *
                              sizeof(adv_ss_hot_desc_t)
                                    );
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

    ADVMTX_SSDOMAIN_HOT_LOCK( &dmnP->ssDmnInfo.ssDmnHotLk );

    /*
     * Obtaining hotlist through dmnP.
     */
    curr_time = get_system_time();
    hhp = &dmnP->ssDmnInfo.ssDmnHotHdr;
    hp = hhp->ssHotFwd;  /* entry pointer */
    while ( hp != (ssHotLLT *) hhp ) {

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

    ADVMTX_SSDOMAIN_HOT_UNLOCK( &dmnP->ssDmnInfo.ssDmnHotLk );

HANDLE_EXCEPTION:

    if (dmn_ref)
        close_domain (bfDomainId, dmnP, 0);

    return(sts);
}

/* ADVFS_ADD_REM_VOL_SVC_CLASS */
adv_status_t
advfs_syscall_op_add_rem_vol_svc_class(libParamsT *libBufp)
{
    bfDomainIdT domainId;
    domainT *dmnP;
    struct vd *vdp;
    int vdRefBumped = 0;
    adv_status_t sts;
    int flag = 0;

    /* set flag if domain is known to cluster */
    if (clu_is_ready() && CLU_DMN_NAME_TO_MP(libBufp->addRemVolSvcClass.domainName))
        flag = DMNA_MOUNTING;

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

    if ((flag & DMNA_MOUNTING) && (libBufp->addRemVolSvcClass.action)) {
        bfSetIdT bfSetId;
        bfSetParamsT bfsetParams;
        uint64_t setIdx = 0;
        uint32_t userId;
        statusT sts2=EOK;
        struct vfs *vfsp = NULL;
        bfSetT *bfSetp;

        sts2 = bs_bfs_get_info( &setIdx, &bfsetParams, dmnP, &userId );

        MS_SMP_ASSERT(sts2 == EOK);
        if (sts2 == EOK) {
            bfSetId = bfsetParams.bfSetId;
            bfSetp = bfs_lookup(bfSetId);

            if (bfSetp != NULL && bfSetp->fsnp != NULL) {
                vfsp = bfSetp->fsnp->vfsp;
                MS_SMP_ASSERT(vfsp != NULL);
                CLU_FSFLUSH(vfsp, RMVOL);
            } else {
                /* We should not get here. */
                bs_domain_close( dmnP );
                bs_bfdmn_deactivate( domainId, flag );
                return(E_NO_SUCH_BF_SET);  /* Exception */
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
                          (vdIndexT)libBufp->addRemVolSvcClass.volIndex );
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
    bs_domain_close( dmnP );
    bs_bfdmn_deactivate( domainId, flag );

    /* CFS TODO:
     * if cfs_domain_change_vol() is called earlier, should call
     * cfs_domain_change_vol_done() here.
     */

    return(sts);
} 

/* ADVFS_ADD_REM_VOL_DONE */
adv_status_t
advfs_syscall_op_add_rem_vol_done(libParamsT *libBufp)
{
    bfDomainIdT domainId;
    domainT *dmnP;
    adv_status_t sts;
    int flag = 0;
    
    
    /* set flag if domain is known to cluster */
    if ( clu_is_ready() && CLU_DMN_NAME_TO_MP(libBufp->addRemVolDone.domainName) )
        flag = DMNA_MOUNTING;

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


/* ADVFS_DUMP_LOCKS */
adv_status_t
advfs_syscall_op_dump_locks(libParamsT *libBufp)
{
    bs_dump_locks( libBufp->dumpLocks.locked );

    return(EOK);  /* No return value */
}


/* ADVFS_SWITCH_LOG */
adv_status_t
advfs_syscall_op_switch_log(libParamsT *libBufp, ftxIdT xid)
{

    bfDomainIdT domainId;
    domainT *dmnP;
    adv_status_t sts;

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
                          (uint32_t) libBufp->switchLog.logPgs,
                          (serviceClassT) libBufp->switchLog.logSvc,
                          xid );

    bs_domain_close( dmnP );
    bs_bfdmn_deactivate( domainId, 0 );

    return(sts);
}


/* ADVFS_SWITCH_ROOT_TAGDIR */
adv_status_t
advfs_syscall_op_switch_root_tagdir(libParamsT *libBufp)
{
    domainT *dmnP;
    bfDomainIdT domainId;
    adv_status_t sts;

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


/* ADVFS_RESET_FREE_SPACE_CACHE */
adv_status_t
advfs_syscall_op_reset_free_space_cache(libParamsT *libBufp)
{
    domainT *dmnP;
    unLkActionT unlock_action;
    vdT *vdp;
    int vdRefBumped = 0;
    adv_status_t sts;

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

    ADVFTM_VDT_STGMAP_LOCK( &(vdp->stgMap_lk) );

    sts = sbm_clear_cache(vdp);

    if (sts == EOK) {
        bf_vd_blk_t temp = libBufp->resetFreeSpaceCache.scanStartCluster;

        /*
         * Make sure scan start clust is set to a cluster
         * within range and on a stgCluster boundary.
         */
        temp = temp % vdp->vdClusters;
        temp = (temp * vdp->stgCluster);  /* blocks */
        vdp->scanStartClust = temp / vdp->stgCluster;
    }

    ADVFTM_VDT_STGMAP_UNLOCK( &vdp->stgMap_lk );

    /* Decrement the vdRefCnt on the vdT struct */
    vd_dec_refcnt( vdp );
    vdRefBumped = 0;

    close_domain (libBufp->resetFreeSpaceCache.bfDomainId, dmnP, 0);

    return(sts);
}


/* ADVFS_GET_GLOBAL_STATS */
adv_status_t
advfs_syscall_op_get_global_stats(libParamsT *libBufp)
{
    bs_get_global_stats(&(libBufp->getGlobalStats));

    return(EOK);  /* No return value */
}


/* ADVFS_EXTENDFS */
/* 
 * If fake is set, then the extend was already completed and this call is just
 * to get the new volume size after recovery.
 */
adv_status_t
advfs_syscall_op_extendfs(libParamsT *libBufp, ftxIdT xid, int fake)
{
    int vdi, vdCnt;
    struct vd *vdp;
    vdCnt = 0;
    statusT sts;
    bfDomainIdT domainId;
    domainT *dmnP;
    int i;

    sts = bs_bfdmn_tbl_activate(libBufp->extendFsParams.domain, 0L, &domainId);
    if (sts != EOK) {
        return ( EFAULT );
    }
    sts = bs_domain_access(&dmnP, domainId, FALSE);
    if (sts != EOK) {
        (void) bs_bfdmn_deactivate(domainId, 0L);
        return ( sts );
    }

    for (vdi = 1; vdCnt < dmnP->vdCnt && vdi <= BS_MAX_VDI; vdi++) {
        if ( !(vdp = vd_htop_if_valid(vdi, dmnP, TRUE, FALSE)) ) {
            continue;
        }
        if (strcmp(libBufp->extendFsParams.blkdev, vdp->vdName) == 0)
        {
            if (fake) {  /* See function comment above */
                /* oldBlkSize can't be determined at this point */
                libBufp->extendFsParams.oldBlkSize = 0;
                libBufp->extendFsParams.newBlkSize = vdp->vdSize;
            } else {
                sts = vd_extend(vdp, libBufp->extendFsParams.size,
                                &libBufp->extendFsParams.oldBlkSize,
                                &libBufp->extendFsParams.newBlkSize,
                                xid);
            }
            vd_dec_refcnt( vdp );
            break;
        }
        vd_dec_refcnt( vdp );
        vdCnt++;
    }

    bs_domain_close(dmnP);
    (void) bs_bfdmn_deactivate(domainId, 0L);

    return(sts);
}


/* ADVFS_DMN_RECREATE */
adv_status_t
advfs_syscall_op_dmn_recreate(libParamsT *libBufp)
{
    adv_status_t sts;
    bfDomainIdT dmnId;
    sts = bs_bfdmn_tbl_activate(libBufp->dmnRecreate.domain, 0LL, &dmnId);

    if (sts == EOK) {
        sts = bs_bfdmn_deactivate( dmnId, 0LL );
    }

    return(sts);
}

/* ADVFS_GET_STATVFS */
adv_status_t
advfs_syscall_op_get_statvfs(libParamsT *libBufp)
{
    statusT sts=EOK, sts2=EOK;
    int64_t maxFiles=0;
    bfDomainIdT domainId;
    bfSetIdT bfSetId;
    bfSetT *bfSetp;
    fileSetNodeT *dn;
    domainT *dmnP;
    int32_t domain_activated = FALSE, bfs_is_open = FALSE;

    /* Activate the bitfile set, typically "default" is passed in */
    sts = bs_bfset_activate(
	libBufp->getStatvfs.domainName,
	libBufp->getStatvfs.setName,
	(uint64_t)0, &bfSetId );
    if( sts != EOK ) {
	RAISE_EXCEPTION( sts );
    }

    domain_activated = TRUE;

    sts = bfs_open( &bfSetp, bfSetId, BFS_OP_DEF, FtxNilFtxH );
    if (sts != EOK) {
	RAISE_EXCEPTION( sts );
    }

    bfs_is_open = TRUE;

    dmnP = bfSetp->dmnP;

    /* Dummy up a fileSetNode to pass to bs_get_avail_mcells_nolock*/
    dn = (fileSetNodeT *)ms_malloc( (uint32_t)sizeof( fileSetNodeT ) );
    dn->filesUsed = bfSetp->bfCnt;
    maxFiles = bs_get_avail_mcells_nolock( dmnP, dn );
    ms_free( dn );

    libBufp->getStatvfs.advStatvfs.f_blocks      = dmnP->totalBlks;
    libBufp->getStatvfs.advStatvfs.f_bfree       = dmnP->freeBlks;
    libBufp->getStatvfs.advStatvfs.f_bavail      = dmnP->freeBlks;
    libBufp->getStatvfs.advStatvfs.f_frsize      = DEV_BSIZE;
    libBufp->getStatvfs.advStatvfs.f_files       = maxFiles;
    libBufp->getStatvfs.advStatvfs.f_ffree       = maxFiles - bfSetp->bfCnt;
    libBufp->getStatvfs.advStatvfs.f_type        = 0;
    libBufp->getStatvfs.advStatvfs.f_fsindex     = advfs_fstype;
    libBufp->getStatvfs.advStatvfs.f_magic       = (int32_t)ADVFS_MAGIC;
    libBufp->getStatvfs.advStatvfs.f_featurebits = 0;
    libBufp->getStatvfs.advStatvfs.f_bsize       = ( dmnP->userAllocSz *
						     ADVFS_FOB_SZ );
    libBufp->getStatvfs.advStatvfs.f_favail      = maxFiles - bfSetp->bfCnt;
    libBufp->getStatvfs.advStatvfs.f_namemax     = MAXNAMLEN;
    libBufp->getStatvfs.advStatvfs.f_size        = dmnP->totalBlks;
    libBufp->getStatvfs.advStatvfs.f_time        = dmnP->bfDmnMntId.id_sec;
    libBufp->getStatvfs.advStatvfs.f_flag        = 0;
    /*
     * This will need to be updated to get the fsid from the vfs structure
     * if the file system is mounted because there could be a chance this
     * is pointing to the /dev/advfs device.
     */
    libBufp->getStatvfs.advStatvfs.f_fsid        = bfSetp->bfs_dev;
    libBufp->getStatvfs.advStatvfs.f_cnode       = 1;
    /*
     * This field requires f_mnttoname which is not supported at this time.
     *
     * libBufp->getStatvfs.advStatvfs.f_fstr[32];
     */
    strncpy( &(libBufp->getStatvfs.advStatvfs.f_basetype[0]), "advfs\0", 6 );

HANDLE_EXCEPTION:

    if (bfs_is_open) {
        (void) bs_bfs_close( bfSetp, FtxNilFtxH, BFS_OP_DEF );
    }
    if (domain_activated) {
	sts2 = bs_bfdmn_deactivate( bfSetId.domainId, (uint64_t)0 );
	    if( sts2 != EOK ) {
	    sts = sts2;
	}
    }

    return( sts );
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
 *     multiple advfs_real_syscall()'s or with mount/unmount, we want to 
 *     avoid the global CMS dlm lock as much as possible and rely, instead,
 *     upon the domain-specific dlm lock.  This can happen even if no
 *     fileset is currently mounted for a given domain.
 */

static adv_status_t
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
    struct  vfs *vfsp = NULL;
    int     sts, fd = -1, len;
    char    *dmn_name = NULL, *file_path,
            name_buf[BS_DOMAIN_NAME_SZ + 1] ;
    bfDomainIdT dmn_id = {0,0};
    int     get_domain_lock = 1 ;
    int     get_domain_excl = 0;   /* get domain lock shared by default */
    int     have_cmsdb_excl = 0;
    int     versw_val;
    int     locally_mounted  = 0;  /* assume not a local mount */


    MS_SMP_ASSERT(clu_is_ready());

    *have_domain_lock = 0 ;
    *have_cmsdb_lock = 0 ;

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
 * The general SSI logic is:
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
 *    In a TCRCFS cluster, we may be operating on a locally mounted f.s.
 *    In such a case for DMN_NAME and DMN_ID types of calls, the cluster 
 *    callout will not return a struct vfs *, and so we use 
 *    advfs_is_domain_name_mounted and advfs_is_domain_id_mounted to
 *    determine whether or not a f.s. associated with the domain is mounted.
 *    When called below, these rtns should return TRUE if-and-only-if
 *    there is a fileset from that domain mounted locally (the use of the CMS 
 *    DLM lock here prevents races with cluster-wide mounts/unmounts).  
 *    If a filesystem is locally mounted, we simply avoid using any DLM locks 
 *    since the issue is, in principle, outside of the scope of cluster 
 *    processing.  If a filesystem is neither locally nor cluster-wide
 *    mounted, we take the CMS dlm lock as in SSI.
 *
 *    Note that calls to advfs_is_domain_*_mounted may race with local mounts
 *    and unmounts, however the logic below doesn't care since
 *    a) if these routines return False while racing with a local mount, 
 *       the logic below is conservative and keeps the CMS dlm lock, and
 *    b) if these routines are racing with a local unmount but return True, then
 *       this domain is incapable of being cluster-mounted anyway, and so
 *       DLM locks are not required.
 *
 */
retry:
    switch (key) {
        case DMN_NAME:
            switch (op) {
                case ADVFS_FSET_CREATE:
                    dmn_name = buf->setCreate.domain;
                    break;
                case ADVFS_FSET_DELETE:
                    dmn_name = buf->setDelete.domain;
                    get_domain_lock = get_domain_excl = 1;
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
		case ADVFS_GET_STATVFS:
		    dmn_name = buf->getStatvfs.domainName;
                    break;
                case ADVFS_GET_DMNNAME_PARAMS:
                    dmn_name = buf->getDmnNameParams.domain;
                    break;
                case ADVFS_SET_DMNNAME_PARAMS:
                    dmn_name = buf->setDmnNameParams.domain;
                    break;
                case ADVFS_SWITCH_LOG:
                    dmn_name = buf->switchLog.domainName;
                    break;
                case ADVFS_SWITCH_ROOT_TAGDIR:
                    dmn_name = buf->switchRootTagDir.domainName;
                    break;
                case ADVFS_ADD_VOLUME: /* Not supported remotely */
                    dmn_name = buf->addVol.domain;
                    /* cluster root must use exclusive global dlm lock */
                    if (!strcmp(dmn_name,"cluster_root"))
                        get_domain_lock = 0;
                    else {
                        /* if CMS proposal issued, cmsdb lock will be used then */
                        get_domain_lock = get_domain_excl = 1;
                    }
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
                case ADVFS_SS_RESET_PARAMS:
                    dmn_name = buf->ssSetParams.domainName;
                    break;
                case ADVFS_SS_GET_FRAGLIST:
                    dmn_name = buf->ssGetFraglist.domainName;
                    break;
                case ADVFS_SET_BFSET_PARAMS_ACTIVATE:
                    dmn_name = buf->setBfSetParamsActivate.dmnName;
                    break;
                case ADVFS_EXTENDFS:
                    dmn_name = buf->extendFsParams.domain;
                    break;
                case ADVFS_DMN_RECREATE:
                    dmn_name = buf->dmnRecreate.domain;
                    break;
                default:
                    MS_SMP_ASSERT(0);
                    get_domain_lock = 0;
                    break;
            }

            CLU_CMSDB_DLM_LOCK((get_domain_lock ? DLM_PRMODE : DLM_EXMODE),
                               cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = !get_domain_lock;
	    /* For non stacked server only at server the vfsp will be advfs */
            vfsp = CLU_DMN_NAME_TO_MP(dmn_name);

            /* if not known to the cluster, check for local mount */
            if (vfsp == (struct vfs *)NULL &&
                dmn_name && *dmn_name)
                     locally_mounted = advfs_is_domain_name_mounted(dmn_name);

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
                    /* when CMS proposal issued, cmsdb lock will be used then */
                    get_domain_lock = get_domain_excl = 1;
                    break;
                case ADVFS_SS_GET_HOTLIST:
                    dmn_id = buf->ssGetHotlist.bfDomainId;
                    break;
                default:
                    MS_SMP_ASSERT(0);
                    get_domain_lock = 0 ;
                    break ;
            }
        
            /* 
             * If vfsp is not found below, we want to take cmsdb lock exclusively.
             * If vfsp is found below, we will release cmsdb lock soon anyway. 
             * Therefor, we always take it exclusively to avoid having to retry.
             */
            CLU_CMSDB_DLM_LOCK(DLM_EXMODE, cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = 1;
	    /* For non stacked server only at server the vfsp will be advfs */
            vfsp = CLU_DMN_ID_TO_MP(&dmn_id);

            /* 
             * special handling for ADVFS_REM_VOLUME on cluster root :
             * we want to use exclusive global dlm lock
             */
            if (vfsp && op == ADVFS_REM_VOLUME) {

                if(CLU_MP_TO_DMN_NAME(vfsp, name_buf) == 0) {
                    if (!strcmp(name_buf,"cluster_root")) {
                        get_domain_lock = 0 ;
		    }
                }
            }

            /* if not known to the cluster, check for local mount */
            if ( vfsp == (struct vfs *)NULL &&
                 *(int64_t *)&dmn_id )
                     locally_mounted = advfs_is_domain_id_mounted(dmn_id);
 
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
                    get_domain_lock = 0 ;
                    break ;
            }

            /* we expect p below to get set, so try shared mode */
            CLU_CMSDB_DLM_LOCK((get_domain_lock ? DLM_PRMODE : DLM_EXMODE),
                               cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = !get_domain_lock;
	    /* For non stacked server only at server the vfsp will be advfs */
            vfsp = CLU_PATH_TO_MP(file_path);

            /* validate if known to cluster */
            if (vfsp && vfsp->vfs_mtype != CLU_CFS_VFS_MTYPE(vfsp) &&
                !(vfsp->vfs_flag & VFS_SERVERONLY))
                locally_mounted = 1;

            break;

        case FILE_DESC:
            switch (op) {
                case ADVFS_GET_BF_IATTRIBUTES:
                    fd = buf->getBfIAttributes.fd;
                    break;
                case ADVFS_GET_BF_PARAMS:
                    fd = buf->getBfParams.fd;
                    break;
                case ADVFS_GET_XTNT_MAP:
                    fd = buf->getXtntMap.fd;
                    break;
                case ADVFS_GET_CLUDIO_XTNT_MAP:
                    fd = buf->getCludioXtntMap.fd;
                    break;
                case ADVFS_GET_IDX_BF_PARAMS:
                    fd = buf->getBfParams.fd;
                    break;
                case ADVFS_MIGRATE:
                    fd = buf->migrate.fd;
                    break;
                case ADVFS_MOVE_BF_METADATA:
                    fd = buf->moveBfMetadata.fd;
                    break;
                case ADVFS_SET_BF_ATTRIBUTES:
                    fd = buf->setBfAttributes.fd;
                    break;
                case ADVFS_SET_BF_IATTRIBUTES:
                    fd = buf->setBfIAttributes.fd;
                    break;
                default:
                    MS_SMP_ASSERT(0);
                    get_domain_lock = 0 ;
                    break;
            }
           
            /* we expect vfsp below to get set, so try shared mode */
            CLU_CMSDB_DLM_LOCK((get_domain_lock ? DLM_PRMODE : DLM_EXMODE),
                               cmsdb_sublock);
            *have_cmsdb_lock=1;
            have_cmsdb_excl = !get_domain_lock;
	    /* For non stacked server only at server the vfsp will be advfs */
            vfsp = CLU_FD_TO_MP(fd);

            /* validate if known to cluster */
            if (vfsp && vfsp->vfs_mtype != CLU_CFS_VFS_MTYPE(vfsp) &&
                !(vfsp->vfs_flag & VFS_SERVERONLY))
                locally_mounted = 1;

            break;
    }

    /*
     * For local mount in TCRCFS cluster we release DLM lock
     * and perform operation locally.
     */
    if (locally_mounted) {
            MS_SMP_ASSERT(clu_type() == CLU_TYPE_CFS);
            CLU_CMSDB_DLM_UNLOCK(cmsdb_sublock) ;
            *have_cmsdb_lock=0;
            return (0);
    }

    if (vfsp) {       /* mounted in cluster */

        if (get_domain_lock) {
            if (!dmn_name) {
                dmn_name = name_buf ;
		/* 
		 * CLU_MP_TO_DMN_NAME will fail if the vfs_mtype is not 
		 * cfs which will be the case on the server of non-stacked
		 * server only so we just keep the CMS DLM lock here.
		 */
                if(CLU_MP_TO_DMN_NAME(vfsp, name_buf) != 0)
			dmn_name = NULL;
            }

            /* take domain lock in shared or exclusive mode */
            if (dmn_name && *dmn_name) { 
                sts = CLU_DMN_NAME_DLM_LOCK_TRY(dmn_name, 
                                               get_domain_excl, 
                                               sublock) ;

                if ( ! sts) {
                    *have_domain_lock = 1 ;
                    CLU_CMSDB_DLM_UNLOCK(cmsdb_sublock) ;

                    *have_cmsdb_lock=0;
                } else {
                    if (sts == DLM_NOTQUEUED) {
                        CLU_CMSDB_DLM_UNLOCK(cmsdb_sublock) ;
                        *have_cmsdb_lock=0;
                        delay(1*hz);
                        goto retry;
                    }
                }
            }
        }

        /*
         * the cluster callouts can return an p which refers
         * to a non-cfs filesystem.  don't functionship if this
         * is the case.
         */

        sts = 0;

        if (vfsp->vfs_mtype == CLU_CFS_VFS_MTYPE(vfsp)){
            sts = CLU_MSFS_SYSCALL_FSHIP(vfsp, (int)(op), buf, do_local);
        }

    } else {   /* not mounted at all */

        if (key == DMN_NAME) {
            if (dmn_name && *dmn_name) {
                
                /* 
                 * Try to take domain lock excl to avoid concurrent
                 * domain activations in the cluster.
                 */
                sts = CLU_DMN_NAME_DLM_LOCK_TRY(dmn_name, 1, sublock);
                if ( ! sts) {
                    *have_domain_lock = 1 ;
                    CLU_CMSDB_DLM_UNLOCK(cmsdb_sublock) ;
                    *have_cmsdb_lock=0;
                } else {
                    if (sts == DLM_NOTQUEUED) {
                        CLU_CMSDB_DLM_UNLOCK(cmsdb_sublock) ;
                        *have_cmsdb_lock=0;
                        delay(hz);
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

                CLU_CMSDB_DLM_UNLOCK(cmsdb_sublock) ;

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
