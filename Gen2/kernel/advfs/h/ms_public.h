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
 *  AdvFS 
 *
 * Abstract:
 *
 *  This is the AdvFS public header file, included by all users
 *  of AdvFS services.
 *
 */

#ifndef MS_PUBLIC
#define MS_PUBLIC

#include <advfs/bs_error.h>
#include <advfs/bs_public.h>
#include <sys/condvar.h>
#include <advfs/ms_generic_locks.h>
#include <advfs/ms_logger.h>
#include <advfs/ftx_public.h>
#include <sys/time.h>

/***************************************************************************
 ***** MCELL mgt routines                                             ******
 ***************************************************************************/

typedef enum {
    BMTR_NO_LOCK,
    BMTR_LOCK,
    BMTR_UNLOCK,
    BMTR_IGNORE_LOCK
} bmtrLkT;

int
bmtr_max_rec_size( void );

#define bmtr_get_rec( bfap, rt, bp, bs ) \
    bmtr_get_rec_n_lk( bfap, rt, bp, bs, BMTR_NO_LOCK )

statusT
bmtr_get_rec_n_lk(
    struct bfAccess *bfap,     /* in - bf access structure */
    uint16_t rType,            /* in - type of record */
    void *bPtr,                /* in/out - ptr to buffer */
    uint16_t bSize,            /* in - size of buffer */
    bmtrLkT lk                 /* in - keep mcell locked? */
    );

#define bmtr_put_rec( bfap, rt, bp, bs, ftxh ) \
    bmtr_put_rec_n_unlk( bfap, rt, bp, bs, ftxh, BMTR_NO_LOCK, (ftxIdT)0 )

#define bmtr_put_rec_xid( bfap, rt, bp, bs, ftxh, xid) \
    bmtr_put_rec_n_unlk( bfap, rt, bp, bs, ftxh, BMTR_NO_LOCK, xid )

statusT
bmtr_put_rec_n_unlk(
    struct bfAccess *bfap,     /* in - bf access structure */
    uint16_t rType,            /* in - type of record */
    void *bPtr,                /* in - ptr to buffer */
    uint16_t bSize,            /* in - size of buffer */
    ftxHT parentFtxH,          /* in - transaction handle */
    bmtrLkT lk,                /* in - unlock mcell? */
    ftxIdT  xid                /* in - CFS transaction id */
    );


dev_t
bs_bfset_get_dev(
    bfSetT *bfSetp
    );

#define flmemcpy bcopy

#endif /* MS_PUBLIC */

