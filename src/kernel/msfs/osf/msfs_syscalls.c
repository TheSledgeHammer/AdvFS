/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      MegaSafe system call interface.
 *
 * Date:
 *
 *      Tue Jul 23 15:32:49 1991
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: msfs_syscalls.c,v $ $Revision: 1.1.14.1 $ (DEC) $Date: 2008/02/12 13:07:13 $"

#include <sys/secdefines.h>
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
#include <kern/task.h>
#include <kern/thread.h>
#include    <sys/lock_probe.h>

/*
 ** msfsSyscallp
 *
 * This is a function pointer to the MegaSafe system call handler.  It 
 * exists so that we can have msfs_syscall() in the kernel and have no
 * real syscall (as is the case when the customer has not installed
 * MegaSafe).  When MegaSafe is installed it initializes the syscall
 * function pointer to point to the syscall handler.
 */

int
(* MsfsSyscallp)( 
    int     opType,
    void    *parmBuf,
    int     parmBufLen
    ) = NULL;

/*
 * msfs_syscall
 *
 * Provides a kernel interface to the MegaSafe system call handler.
 *
 * Returns zero if successful, EINVAL if not.  The returned value
 * is placed into the thread's errno by the syscall mechanism.
 *
 * If MegaSafe is not installed/initialized then ENOSYS is returned.
 */

int
msfs_syscall(
    struct proc *p,     /* in - ?? */
    void *args,         /* in - pointer system call arguments */
    long *retval        /* out - system call return value */
    )
{
    /* 
     * Define a pointer to a struct that contains the user args 
     */
    register struct args {
        int  opType;
        void *parmBuf;
        int  parmBufLen;
    } *uap = (struct args *) args;  /* User Args Pointer */

    if (MsfsSyscallp == NULL) {
        /*
         ** MegaSafe is not installed/initialized.
         */
        *retval = -1;
        return( ENOSYS );
    } else {

        /*
         ** Call the MegaSafe syscall handler.  Note that we never set
         ** errno when MegaSafe is installed.  The syscall return value
         ** is either zero (EOK) or an errno, or a MegaSafe status.  We
         ** don't use errno to indicate an error; it is too limited.
         */
        *retval = (long)MsfsSyscallp( uap->opType, uap->parmBuf, uap->parmBufLen );
        return( 0 );
    }
}
