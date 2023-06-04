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
 * Copyright (C) 2004 Hewlett-Packard Company
 */

/*
 *      AdvFS system call interface.
 */


#if     SEC_BASE
#include <sys/security.h>
#endif
#if     SEC_ARCH
#include <sys/secpolicy.h>
#endif

#include <sys/param.h>
#include <sys/systm.h>
#include <sys/kernel.h>
#include <sys/proc.h>
#include <sys/user.h>
#include <sys/syscall.h>
#include <sys/moddefs.h>
#include <advfs_scall.h>

/* system call arg list */
/* the integer types have to be declared as long here
 * otherwise the values will not be passed.  the actual system call
 * definition uses the type int for the interger types.  weird hp-ux
 * system call behavior
 */
typedef struct {
    long op_type;
    void *parm_buf;
    long parm_buf_len;
} advfs_syscall_args_t;

/*
 * DLKM Support
 */

/*
 * Declare forward references
 */

static int advfs_scall_load(void *);
static int advfs_scall_unload(void*);

/*
 * Define local DLKM required structures
 */

static struct mod_type_data advfs_scall_drv_link =
    {
	"advfs syscall module",
	NULL
    };

static struct modlink advfs_scall_mod_link[] =
    {
        {&mod_misc_ops, (void *)&advfs_scall_drv_link},
	{(struct mod_operations *)NULL, (void *)NULL }
    };

struct modwrapper advfs_scall_wrapper =
    {
	 MODREV,
         advfs_scall_load,
         advfs_scall_unload,
         NULL,
         (void *)&advfs_scall_conf_data,
         advfs_scall_mod_link
    };

/*
 * DLKM Load routine - nothing to do, always return success.
 */

static int advfs_scall_load(void *arg)
{
    return (0);
}

/*
 * DLKM Unload routine - can't unload, always return failure.
 */

static int advfs_scall_unload(void *arg)
{
    return (1);
}

/*
 ** AdvfsSyscallp
 *
 * This is a function pointer to the AdvFS system call handler.  It 
 * exists so that we can have advfs_syscall() in the kernel and have no
 * real syscall (as is the case when the customer has not installed
 * AdvFS).  When AdvFS is installed it initializes the syscall
 * function pointer to point to the syscall handler.
 */

int
(* AdvfsSyscallp)( 
    int     opType,
    void    *parmBuf,
    int     parmBufLen
    ) = NULL;

/*
 * advfs_syscall_stub
 *
 * Provides a kernel interface to the AdvFS system call handler.
 *
 * Returns zero if successful, EINVAL if not.  The returned value
 * is placed into the thread's errno by the syscall mechanism.
 *
 * If AdvFS is not installed/initialized then ENOSYS is returned.
 */

void
advfs_syscall_stub(advfs_syscall_args_t *uap)
{
    int placebo_return;

#ifdef OSDEBUG
    printf("advfs_syscall: arg1 - %d arg2 - 0x%lx arg3 - %d\n",
            ((uint32_t)uap->op_type),
            uap->parm_buf,
            ((uint32_t)uap->parm_buf_len));
#endif

    if (      ( AdvfsSyscallp == NULL)
          && !( advfs_placebo() ) ) {
        /*
         ** AdvFS is not installed, and the dlkm could not be loaded.
         */
        u.u_r.r_val1 = -1;
        u.u_error = ENOSYS;
    } else {

        /*
         ** Call the AdvFS syscall handler.  Note that we never set
         ** errno when AdvFS is installed.  The syscall return value
         ** is either zero (EOK) or an errno, or a AdvFS status.  We
         ** don't use errno to indicate an error; it is too limited.
         */
        u.u_r.r_val1 = (long)AdvfsSyscallp(
                                   ((int)uap->op_type), 
                                   uap->parm_buf, 
                                   ((int)uap->parm_buf_len) );
        u.u_error = 0;
    } 
}

void
advfs_scall_link(void)
{
    sysent_link_function(SYS_advfs_syscall, advfs_syscall_stub);
 
    return;
}
