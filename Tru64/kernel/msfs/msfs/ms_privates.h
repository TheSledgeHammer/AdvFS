/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1989, 1990, 1991                      *
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
 * @(#)$RCSfile: ms_privates.h,v $ $Revision: 1.1.81.2 $ (DEC) $Date: 2003/01/03 18:29:30 $
 */

#ifndef MS_PRIVATES
#define MS_PRIVATES

#define FRAG_LOCK_WRITE( sLk ) \
    lock_write( sLk.lock );
#define FRAG_LOCK_READ( sLk ) \
    lock_read( sLk.lock );
#define FRAG_UNLOCK( sLk ) \
    lock_done( sLk.lock );

#define MCELL_LOCK_WRITE( sLk ) \
    lock_write( sLk.lock );
#define MCELL_UNLOCK( sLk ) \
    lock_done( sLk.lock );

#define STGMAP_LOCK_WRITE( sLk ) \
    lock_write( sLk.lock );
#define STGMAP_UNLOCK( sLk ) \
    lock_done( sLk.lock );

#define MCELLIST_LOCK_WRITE( sLk ) \
    lock_write( sLk.lock );
#define MCELLIST_LOCK_READ( sLk ) \
    lock_read( sLk.lock );
#define MCELLIST_UNLOCK( sLk ) \
    lock_done( sLk.lock );

#define XTNMAP_LOCK_WRITE( sLk ) \
    lock_write( sLk.lock );
#define XTNMAP_LOCK_READ( sLk ) \
    lock_read( sLk.lock );
#define XTNMAP_UNLOCK( sLk ) \
    lock_done( sLk.lock );
#define XTNMAP_LOCK_DOWNGRADE( sLk ) \
    lock_write_to_read( sLk.lock );

#define COW_READ_LOCK_RECURSIVE( sLk ) \
    lock_read_recursive( sLk );
#define COW_READ_UNLOCK_RECURSIVE( sLk ) \
    lock_read_done_recursive(sLk);
#define COW_LOCK_WRITE( sLk ) \
    lock_write( sLk );
#define COW_LOCK_READ( sLk ) \
    lock_read( sLk );
#define COW_UNLOCK( sLk ) \
    lock_done( sLk );

#define CLU_CLXTNT_READ_LOCK_RECURSIVE(sLk) \
    lock_read_recursive(sLk);
#define CLU_CLXTNT_UNLOCK_RECURSIVE(sLk) \
    lock_read_done_recursive(sLk);
#define CLU_CLXTNT_WRITE( sLk) \
    lock_write( sLk );
#define CLU_CLXTNT_READ( sLk ) \
    lock_read( sLk );
#define CLU_CLXTNT_TRY_READ( sLk ) \
    lock_try_read( sLk )
#define CLU_CLXTNT_UNLOCK( sLk) \
    lock_done( sLk );

#define RAWBUFREE_LOCK_WRITE( sLk ) \
    lock_write( sLk );
#define RAWBUFREE_UNLOCK( sLk ) \
    lock_done( sLk );

#define MIGTRUNC_LOCK_READ( sLk ) \
    lock_read( sLk );
#define MIGTRUNC_LOCK_WRITE( sLk ) \
    lock_write( sLk );
#define MIGTRUNC_UNLOCK( sLk ) \
    lock_done( sLk );

#define  DDLACTIVE_LOCK_READ( sLk ) \
    lock_read( sLk );
#define  DDLACTIVE_LOCK_WRITE( sLk ) \
    lock_write( sLk );
#define DDLACTIVE_UNLOCK( sLk ) \
    lock_done( sLk );

#define TRUNC_XFER_READ_LOCK_RECURSIVE( sLk ) \
    lock_read_recursive( sLk );
#define TRUNC_XFER_UNLOCK_RECURSIVE(sLk) \
    lock_read_done_recursive(sLk);
#define TRUNC_XFER_LOCK_READ( sLk ) \
    lock_read( sLk );
#define  TRUNC_XFER_LOCK_WRITE( sLk ) \
    lock_write( sLk );
#define TRUNC_XFER_UNLOCK( sLk ) \
    lock_done( sLk );

/*
 * Some private definitions
 */

#define MSFS_VN_PRIVATE \
    roundup( sizeof( struct bfNode ) + sizeof( struct fsContext ),  \
             sizeof( void * ) )

#define MAX_VIRT_VM_PAGE_RNG 256

#ifdef _KERNEL

#include <kern/thread.h>
#include <msfs/ms_assert.h>
#define ASSERT_NO_LOCKS()     MS_SMP_ASSERT(current_thread()->lock_count == 0)

#endif

/*
 * smoothsync support
 */
#ifdef  _KERNEL
#ifdef ADVFS_SMP_ASSERT
/* only enable smoothsync_debug support if the rest of the AdvFS assert
 * package has been enabled.
 */
#define SMOOTHSYNC_DEBUG 1
#endif /* ADVFS_SMP_ASSERT */

#if SMOOTHSYNC_DEBUG
extern u_int smsync_debug;
#define SMSYNC_DBG_Q    0x1
#define SMSYNC_DBG_OP   0x2
#define SMSYNC_DBG_BUF  0x4
#define SMSYNC_DBG(flag,action) \
{ \
    if (smsync_debug & (flag)) (action); \
}
#else
#define SMSYNC_DBG(flag,action)
#endif /* SMOOTHSYNC_DEBUG */
#endif /* _KERNEL */

#include <msfs/bs_ods.h>
#include <msfs/bs_ims.h>
enum msfs_setproplist_enum { NO_SET_CTIME=0, SET_CTIME = 1 };

#endif /* MS_PRIVATES */
