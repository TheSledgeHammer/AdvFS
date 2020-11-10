/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1990, 1991, 1992, 1993                *
 */
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
 * @(#)$RCSfile: ms_public.h,v $ $Revision: 1.1.16.8 $ (DEC) $Date: 1998/12/04 23:39:59 $
 */

#ifndef MS_PUBLIC
#define MS_PUBLIC

#ifdef USER_MODE

#ifdef PTHREADS_OSF
#include <pthread_osf.h>
#else
#include <pthread.h>
#endif

/*
 * These struct defs are necessary so that we don't have to put a bunch of
 * "#ifdef"s around the definitions of rbf_access_int(), rbf_acces_one_int()
 * and rbf_vfs_access().  The defs must be positioned before the include of
 * ftx_public.h so that ftx_public.h picks up the definition. This is necessary
 * because ftx_public.h defines rbf_vfs_access()'s proto.
 */

#if 0
struct mount {int notused;};
#endif
struct vnode {int notused;};
#endif /* USER_MODE */

#ifndef KERNEL
#define uprintf printf
#endif

#include <msfs/bs_error.h>
#include <msfs/bs_public.h>
/*#include <msfs/mss_common.h>*/
#include <msfs/ms_logger.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/ftx_public.h>

#include <sys/resource.h>

/***************************************************************************
 ***** MCELL mgt routines                                             ******
 ***************************************************************************/

/*
 * File system level bmt record types start with 255 and count backwards.
 */

#define BMTR_FS_STAT 255
#define BMTR_FS_DATA 254
#define BMTR_FS_BACKLINK_obsolete 253
#define BMTR_FS_UNDEL_DIR 252
#define BMTR_FS_TIME 251
#define BMTR_FS_INDEX_FILE 250
#define BMTR_FS_DIR_INDEX_FILE 249

typedef enum { BMTR_NO_LOCK, BMTR_LOCK, BMTR_UNLOCK, BMTR_IGNORE_LOCK } bmtrLkT;

int
bmtr_max_rec_size( void );

#define bmtr_get_rec( bfap, rt, bp, bs ) \
    bmtr_get_rec_n_lk( bfap, rt, bp, bs, BMTR_NO_LOCK )

statusT
bmtr_get_rec_n_lk(
    struct bfAccess *bfap,      /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in/out - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    bmtrLkT lk                  /* in - keep mcell locked? */
    );

#define bmtr_put_rec( bfap, rt, bp, bs, ftxh ) \
    bmtr_put_rec_n_unlk( bfap, rt, bp, bs, ftxh, BMTR_NO_LOCK, 0 )

#define bmtr_put_rec_xid( bfap, rt, bp, bs, ftxh, xid) \
    bmtr_put_rec_n_unlk( bfap, rt, bp, bs, ftxh, BMTR_NO_LOCK, xid )

statusT
bmtr_put_rec_n_unlk(
    struct bfAccess *bfap,      /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,                 /* in - unlock mcell? */
    long xid                    /* in - CFS transaction id */
    );

statusT
bmtr_put_rec_n_unlk_int(
    struct bfAccess *bfap,      /* in - bf access structure */
    u_short rType,              /* in - type of record */
    void *bPtr,                 /* in - ptr to buffer */
    u_short bSize,              /* in - size of buffer */
    ftxHT parentFtxH,           /* in - transaction handle */
    bmtrLkT lk,                 /* in - unlock mcell? */
    long xid                    /* in - CFS transaction id */
    );

#ifdef KERNEL
dev_t bs_bfset_get_dev( bfSetT *bfSetp );
#endif


/* task_getrusage - this routine gets the system time and
 * user time used by the calling task
 */
void
task_getrusage( struct rusage * ru);

/* for advfs smoothsync */
#define SMSYNC_NQS 16   /* best at n*15+1 */

#endif /* MS_PUBLIC */
