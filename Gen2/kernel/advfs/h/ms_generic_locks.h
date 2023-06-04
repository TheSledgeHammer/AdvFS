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
 * Facility:
 *
 *     AdvFS 
 *
 * Abstract:
 *
 *      Defines generic locks and routines to manipulate the locks.
 *
 */

#ifndef _GENERIC_LOCKS_
#define _GENERIC_LOCKS_

#include <sys/mutex_impl.h>
#include <sys/rwlock_impl.h>
#include <sys/spin_impl.h>
#include <sys/condvar_impl.h>
#include <sys/mp.h>

extern int getprocindex __((void));
extern int how_many_processors __((void));

/*
 *  Defines to turn various locking options on and off.
 */

/* Turn on to enable hierarchy checking */
/* #define ADVFS_ENABLE_LOCK_HIERARCHY */
/* Turn on to enable lock hierarchy logging */
/* #define ADVFS_ENABLE_LOCK_HIER_LOGGING */
/* Turn on to enable lock usage logging */
/* #define ADVFS_ENABLE_LOCK_USAGE_LOGGING */

/*
 *  The following #define is here for now so that I don't have to
 *  make lots of edits during the port.
 */
#define mutexT       mutex_t

/* Defines to centralize references to smp implementation.
 *
 * Largest number of processors expected, ADVFS_NCPUS_CONFIG is used
 * to dimension per cpu tables. ADVFS_NCPUS_RUN is used to perform
 * operations across all processors that might be running.
 * Resource table size must be greater than ADVFS_NCPUS_CONFIG and
 * must be prime.
 *
 * ADVFS_RESOURCE_TRIES is the number of times an attempt to find a
 * resource in a resource table will be performed with trylock instead
 * of lock.
 */

#define ADVFS_NCPUS_CONFIG MAX_PROCS
#define ADVFS_NCPUS_RUN    how_many_processors()
#define ADVFS_CURRENT_CPU  getprocindex()

#if ((ADVFS_NCPUS_CONFIG-256)>=0)
#define ADVFS_RESOURCE_TABLE_SIZE 257
#elif ((ADVFS_NCPUS_CONFIG-128)>=0)
#define ADVFS_RESOURCE_TABLE_SIZE 131
#elif ((ADVFS_NCPUS_CONFIG-16)>=0)
#define ADVFS_RESOURCE_TABLE_SIZE 19
#endif

#define ADVFS_RESOURCE_TRIES        5


/*
 * Macros to express lock orders in the new extended format
 * (low 4 bits shifted left). We don't currently include the
 * h file where the macros are.
 */

#define LO_EXT_SHIFT (11)
#define ADVLO_PLUS_n(_BASE, _EXT) \
    ((_BASE) + ((_EXT) << LOCK_ORDER_EXT_SHIFT))
#define ADVL(n) ADVLO_PLUS_n(((n)/16),((n)&0xf))

/*
 * Lock order that suppresses order checking
 */

#define ADVFS_NOCHECK_LOCK_ORDER (131 | SEMA_DEADLOCK_SAFE)

/*
 * Lock order of lowest caller (for trust calls)
 */

#define ADVFS_LOWEST_TRUST_LOCK_ORDER 80

/*
 * Define Advfs lock orders. ADVFS_LOCK_ORDER_LOW should always be >= 
 * CLU_CFS_LOW_LOCK_ORDER
 */

#define ADVFS_LOCK_ORDER_LOW      77 
#define ADVFS_LOCK_ORDER_HIGH     131
#define ADVFS_SPIN_BASE_ORDER     131

/*
 * Macros to define the lock orders relative to the low or
 * high range.
 *     ADVLPL defines an order in the lower range,
 *     ADVHPL defines an order in the higher range,
 *     ADVSMP defines a simple lock order.
 *     ADVLPI defines a lock order in the low range that
 *            requires a deadlock safe flag. The macros
 *            take two arguments, a "real" order and an
 *            actual order (used only by ADVLPI) to resolve
 *            conflicts.
 *
 * If hierarchy checking is disabled, all default to the low
 * or high order with deadlock safe, or the spin order.
 */
/* At least until we separate the CFS/ADVFS locks, if checking
 * is enabled bump the order by 1 to differentiate them.
 */
#ifdef ADVFS_ENABLE_LOCK_HIERARCHY
#define ADVLPL(l,n) ADVL((16*ADVFS_LOCK_ORDER_LOW)+l)
#define ADVLPI(l,n) (ADVL((16*ADVFS_LOCK_ORDER_LOW)+n)|SEMA_DEADLOCK_SAFE)
#define ADVHPL(l,n) ADVL((16*ADVFS_LOCK_ORDER_HIGH)+l)
#else
#define ADVLPI(l,n) (ADVL(16*ADVFS_LOCK_ORDER_LOW)|SEMA_DEADLOCK_SAFE)
#define ADVLPL(l,n) (ADVL(16*ADVFS_LOCK_ORDER_LOW)|SEMA_DEADLOCK_SAFE)
#define ADVHPL(l,n) (ADVL(16*ADVFS_LOCK_ORDER_HIGH)|SEMA_DEADLOCK_SAFE)
#endif

/*
 * Can't disable the hierarchy on spin-locks anyway.
 */

#define ADVSMP(l)   ADVL((16*ADVFS_SPIN_BASE_ORDER)+l)


/*
 * Mutex and Read/Write lock orders
 */


/* Tier 0 */

#define ADVRWL_DMNACTIVATION_LOCK_ORDER    ADVLPI( 0, 0)
#define ADVMTX_SSLIST_TPOOL_LOCK_ORDER     ADVLPL( 1, 0)
#define ADVMTX_SSLIST_WORKTPOOL_LOCK_ORDER ADVLPL( 2, 0)

/* Tier 1 */

#define ADVMTX_SS_LOCK_ORDER               ADVLPL( 3, 0)
#define ADVRWL_FILESETLIST_LOCK_ORDER      ADVLPL( 4, 0)
#define ADVMTX_DYNHASHTHREAD_LOCK_ORDER    ADVLPL( 5, 0)

/* Tier 2 */
    /* Trace lock doesn't show in the testing */
#define ADVMTX_TRACE_LOCK_ORDER            ADVLPL( 6, 0)


/* Tier 3 */

#define ADVRWL_RMVOL_TRUNC_LOCK_ORDER      ADVLPL( 7, 0)

/* Tier 4 */
#define ADVRWL_BFAPCACHEMODE_LOCK_ORDER    ADVLPL( 8, 0)
#define ADVRWL_FILECONTEXT_LOCK_ORDER      ADVLPI( 9, 9) /* Must have
						            deadlock safe
							    set */
#define ADVMTX_VOLINFO_FRAG_LOCK_ORDER     ADVLPL(10, 0)
#define ADVMTX_SSDOMAIN_HOT_LOCK_ORDER     ADVLPL(11, 0)

/* Tier 5 */

#define ADVRWL_BFAP_MIGSTG_LOCK_ORDER      ADVLPI(12,12) /* Must have
                                                            deadlock safe
                                                            set */

/* Tier 6 */

#define ADVRWL_DDLACTIVE_LOCK_ORDER        ADVLPL(13, 0)

/* Tier 7 (Forced due to trylock on bfap flush outside of transactions) */

#define ADVRWL_DOMAIN_FTXSLOT_LOCK_ORDER   ADVLPL(14, 0)

/* Tier 8 */

#define ADVRWL_BFAP_BFA_ACL_LOCK_ORDER     ADVLPL(15, 0)
#define ADVRWL_BFSET_TBL_LOCK_ORDER        ADVLPL(16, 0)

/* Tier 9 */

#define ADVFTM_SETTAGDIR_LOCK_ORDER        ADVLPL(17, 0)

/* Tier 10 */

#define ADVRWL_BFAP_FLUSH_LOCK_ORDER       ADVLPI(18,19) /* Temp for Snap */

#define ADVRWL_BFAP_SNAP_LOCK_ORDER        ADVLPI(19,19) /*Must have 
                                                           deadlock safe
                                                           set */
#define ADVMTX_METAFLUSH_LOCK_ORDER        ADVLPI(20,19)
/* Tier 11 */

#define ADVRWL_BFAP_MCELL_LIST_LOCK_ORDER  ADVLPI(20,20) /*Must have 
                                                           deadlock safe
                                                           set */

/* Tier 12 - May conflict with Tier 10 */

#define ADVMTX_DQUOT_LOCK_ORDER            ADVLPI(21,20) /* Temp for quota */
#define ADVMTX_QUOTA_INFO_QI_LOCK_ORDER    ADVLPI(22,20) /* Temp for quota */

/* Tier 13 */

#define ADVFTM_VDT_MCELL_LOCK_ORDER        ADVLPL(23, 0)

/* Tier 14 */

#define ADVRWL_VDT_DELLIST_LOCK_ORDER      ADVLPL(24, 0)
#define ADVRWL_BMT_MCELL_LIST_LOCK_ORDER   ADVLPL(25, 0)
#define ADVRWL_RBMT_MCELL_LOCK_ORDER       ADVLPL(26, 0)

/* Tier 15 */

#define ADVFTM_VDT_STGMAP_LOCK_ORDER       ADVLPL(27, 0)

/* #define ADVMTX_METAFLUSH_LOCK_ORDER        ADVLPL(28, 0) */
/* Tier 16 */

#define ADVRWL_BMT_XTNTMAP_LOCK_ORDER      ADVLPL(29, 0)

/* Tier 17 - Forced */

#define ADVRWL_BFAP_XTNTMAP_LOCK_ORDER     ADVLPI(30,30)
#define ADVRWL_VHAND_XTNTMAP_LOCK_ORDER    ADVLPI(31,30)

/* Tier 18 - Top entry conflicts with Tier 17 */

#define ADVMTX_LOGDESC_LOCK_ORDER          ADVLPI(32,30)

/* Tier 19 - Top entry conflicts with Tier 17 */

#define ADVMTX_LOGDESC_FLUSH_LOCK_ORDER    ADVLPI(33,30)

/* Lowest tier */

#define ADVMTX_DOMAINHASHTABLE_LOCK_ORDER  ADVLPL(34, 0)
#define ADVMTX_SSDOMAIN_LOCK_ORDER         ADVLPL(35, 0)
#define ADVMTX_BFAP_READAHEAD_LOCK_ORDER   ADVLPL(36, 0)
#define ADVMTX_SC_TBL_LOCK_ORDER           ADVLPL(37, 0)
#define ADVMTX_DOMAINVDPTBL_LOCK_ORDER     ADVLPL(38, 0)
#define ADVMTX_VD_STATE_LOCK_ORDER         ADVLPL(39, 0)
#define ADVMTX_SSVDMIG_LOCK_ORDER          ADVLPL(40, 0)
#define ADVMTX_DOMAIN_LOCK_ORDER           ADVLPL(41, 0)
#define ADVMTX_SMARTSTOREGBL_LOCK_ORDER    ADVLPL(42, 0)
#define ADVMTX_DOMAINFREEZE_LOCK_ORDER     ADVLPL(43, 0)
#define ADVMTX_FREEZE_MSGS_LOCK_ORDER      ADVLPL(44, 0)

/* Need to resolve with cluster test */

#define ADVRWL_DMN_XID_RECOVERY_LOCK_ORDER ADVLPL( 5, 0)

/* This lock doesn't really exist yet, but is referenced in
 * code that's conditionalized out in bs_migrate.c. This
 * will need to be finished if multiple snapshots are
 * implemented.
 */

#define ADVRWL_COW_LOCK_ORDER              ADVLPL( 0, 0)

/* These aren't really used. They ought to be converted to
 * spin locks, but are never locked so it doesn't matter
 * for now, and it risks breaking PA alignments.
 */

#define ADVMTX_BFSSET_LOCK_ORDER           ADVLPL( 0, 0)

/*
 * Simple Locks
 */

#define ADVSMP_FILESET_LOCK_ORDER          ADVSMP( 0)
#define ADVSMP_BFSET_LOCK_ORDER            ADVSMP( 1)
#define ADVSMP_DYNHASH_BUCKET_LOCK_ORDER   ADVSMP( 3)
#define ADVSMP_SETACCESSCHAIN_LOCK_ORDER   ADVSMP( 4)
#define ADVSMP_BFAP_LOCK_ORDER             ADVSMP( 5)
#define ADVSMP_FREE_LIST_LOCK_ORDER        ADVSMP( 6)
#define ADVSMP_CLOSED_LIST_LOCK_ORDER      ADVSMP( 7)

#define ADVSMP_BSBUF_LOCK_ORDER            ADVSMP( 8)
#define ADVSMP_LOGDIRTYBUF_LOCK_ORDER      ADVSMP( 8)
#define ADVSMP_IOANCHOR_LOCK_ORDER         ADVSMP( 8)
#define ADVSMP_DOMAINLSN_LOCK_ORDER        ADVSMP( 8)
#define ADVSMP_DOMAINFTXTBL_LOCK_ORDER     ADVSMP( 8)
#define ADVSMP_BFAP_TRACE_LOCK_ORDER       ADVSMP( 8)
#define ADVSMP_ACCESS_TRACE_LOCK_ORDER     ADVSMP( 8)
#define ADVSMP_ACCESS_MGMT_INIT_ORDER      ADVSMP( 8)
#define ADVSMP_BFAP_RANGE_LOCK_ORDER       ADVSMP( 8)
#define ADVSMP_MULTIWAIT_LOCK_ORDER        ADVSMP( 8)
#define ADVSMP_MSGQ_LOCK_ORDER             ADVSMP( 8)
#define ADVSPL_VOLINFO_MSG_LOCK_ORDER      ADVSMP( 8)
#define ADVSMP_ACC_CTRL_STRUC_LOCK_ORDER   ADVSMP( 8)
#define ADVSMP_GP_LOG_LOCK_ORDER           ADVSMP( 8)
#define ADVSMP_LOCK_LOG_LOCK_ORDER         ADVSMP( 8)
#define ADVSMP_BFAP_BFA_SS_MIG_LOCK_ORDER  ADVSMP( 8)
#define ADVSMP_TAG_ARRAY_LOCK_ORDER        ADVSMP( 8)

#define ADVSMP_FILESTATS_LOCK_ORDER        ADVSMP( 9)
#define ADVSMP_SPACE_USED_LOCK_ORDER       ADVSMP( 0)

/*
 * Define lock types for ADVFS. This allows for wholesale
 * replacement of the lock interface in one place
 */

extern void advfs_mutex_lock( b_sema_t *b_sema );
extern void advfs_rwlock_wrlock( rwlock_t *rwlock );
extern void advfs_rwlock_rdlock( rwlock_t *rwlock );

#   define ADVFS_SPIN_T           spin_t
#   define ADVFS_SPIN_INIT        (void)spin_init
#   define ADVFS_SPIN_DESTROY     spin_destroy
#   define ADVFS_SPIN_LOCK        spin_lock
#   define ADVFS_SPIN_UNLOCK      spin_unlock
#   define ADVFS_SPIN_OWNED       spin_owned
#   define ADVFS_SPIN_TRYLOCK     spin_trylock

#   define ADVFS_MUTEX_T          mutex_t
#   define ADVFS_MUTEX_INIT       (void)mutex_init
#   define ADVFS_MUTEX_DESTROY    mutex_destroy
#   define ADVFS_MUTEX_UNLOCK     mutex_unlock
#   define ADVFS_MUTEX_TRYLOCK    mutex_trylock
#   define ADVFS_MUTEX_OWNED      mutex_owned

#ifndef ADVFS_ENABLE_LOCK_HIER_LOGGING
#   define ADVFS_MUTEX_LOCK       mutex_lock
#else
#   define ADVFS_MUTEX_LOCK       advfs_mutex_lock 
#endif /* ADVFS_ENABLE_LOCK_HIER_LOGGING */

/*
 * Lock order push/pop calls for VOP protection
 */

#define ADVFS_VOP_LOCKSAFE push_trust_deadlock_safe( \
 ADVFS_LOWEST_TRUST_LOCK_ORDER )
#define ADVFS_VOP_LOCKUNSAFE pop_trust_deadlock_safe( \
 ADVFS_LOWEST_TRUST_LOCK_ORDER )

#   define ADVFS_RWLOCK_T         rwlock_t
#   define ADVFS_RWLOCK_INIT      (void)rwlock_init
#   define ADVFS_RWLOCK_DESTROY   rwlock_destroy
#   define ADVFS_RWLOCK_UNLOCK    rwlock_unlock
#   define ADVFS_RWLOCK_WROWNED   rwlock_wrowned
#   define ADVFS_RWLOCK_DISOWN    rwlock_disown
#   define ADVFS_RWLOCK_CLAIM     rwlock_claim
#   define ADVFS_RWLOCK_TRYWRITE  rwlock_trywrlock
#   define ADVFS_RWLOCK_TRYREAD   rwlock_tryrdlock
#   define ADVFS_RWLOCK_UPGRADE   rwlock_upgrade
#   define ADVFS_RWLOCK_TRYUPGRADE rwlock_tryupgrade
#   define ADVFS_RWLOCK_DOWNGRADE rwlock_downgrade

#ifndef ADVFS_ENABLE_LOCK_HIER_LOGGING
#   define ADVFS_RWLOCK_WRITE     rwlock_wrlock
#   define ADVFS_RWLOCK_READ      rwlock_rdlock
#else
#   define ADVFS_RWLOCK_WRITE     advfs_rwlock_wrlock 
#   define ADVFS_RWLOCK_READ      advfs_rwlock_rdlock
#endif /* ADVFS_ENABLE_LOCK_HIER_LOGGING */


/*
 * Special - Macros for manipulating locks embedded in an
 *           FTX Lock.
 */

#define ADVFTM_FTXMTX_INIT( sLk, name, order ) \
	ADVFS_MUTEX_INIT( sLk,                 \
                           name,               \
                           NULL,               \
                           MTX_NOWAIT,         \
                           order,              \
                           NULL )
#define ADVFTM_FTXMTX_LOCK( sLk )  ADVFS_MUTEX_LOCK( sLk )

#define ADVFTX_FTXRW_INIT( sLk, name, order ) \
	ADVFS_RWLOCK_INIT( sLk,               \
                           name,              \
                           NULL,              \
                           RWL_NOWAIT,        \
                           order,             \
                           NULL )
#define ADVFTX_FTXRW_READ( sLk )   ADVFS_RWLOCK_READ( sLk )
#define ADVFTX_FTXRW_WRITE( sLk )  ADVFS_RWLOCK_WRITE( sLk )
#define ADVFTX_FTXRW_UNLOCK( sLk ) ADVFS_RWLOCK_UNLOCK( sLk )

/*
 * Complex locks and mutexes - Group 1
 */

/*
 *    Name:     "Advfs domain activation lock"
 *    Formerly: "DmnTblLock"
 */

#   define ADVRWL_DMNACTIVATION_T ADVFS_RWLOCK_T
#   define ADVRWL_DMNACTIVATION_INIT(lock)           \
        ADVFS_RWLOCK_INIT(lock,                      \
                    "Advfs domain activation lock",  \
                    NULL,                            \
                    RWL_WAITOK,                      \
                    ADVRWL_DMNACTIVATION_LOCK_ORDER, \
		    NULL)

#   define ADVRWL_DMNACTIVATION_WRITE(lock)        ADVFS_RWLOCK_WRITE(lock)
#   define ADVRWL_DMNACTIVATION_READ(lock)         ADVFS_RWLOCK_READ(lock)
#   define ADVRWL_DMNACTIVATION_UNLOCK(lock)       ADVFS_RWLOCK_UNLOCK(lock)
#   define ADVRWL_DMNACTIVATION_WRITE_HOLDER(lock) ADVFS_RWLOCK_WROWNED(lock)

/*
 *    Name:     "Advfs bfap cachemode lock"
 */

#   define ADVRWL_BFAPCACHEMODE_INIT(sLk)                             \
    {                                                                 \
        rwlock_attr_t lock_attr;                                      \
        rwlock_attr_init(&lock_attr, sizeof(rwlock_attr_t));          \
        rwlock_attr_setflag(&lock_attr, RWL_ATTR_DISOWNABLE,          \
                             RWL_ATTR_FLAG_ON);                       \
        MS_SMP_ASSERT( lock_attr.attr_flags == RWL_ATTR_DISOWNABLE ); \
        rwlock_init( sLk,                                             \
                     "Advfs bfap cachemode lock",                     \
                     &lock_attr,                                      \
                     RWL_WAITOK,                                      \
                     ADVRWL_BFAPCACHEMODE_LOCK_ORDER,                 \
                     NULL);                                           \
    }

#  define ADVRWL_BFAPCACHEMODE_DESTROY(bfap) \
	  ADVFS_RWLOCK_DESTROY(&((bfap)->cacheModeLock))
#  define ADVRWL_BFAPCACHEMODE_WRITE(bfap) \
	  ADVFS_RWLOCK_WRITE(&((bfap)->cacheModeLock))
#  define ADVRWL_BFAPCACHEMODE_READ(bfap)  \
	  ADVFS_RWLOCK_READ(&((bfap)->cacheModeLock))
#  define ADVRWL_BFAPCACHEMODE_UNLOCK(bfap) \
	  ADVFS_RWLOCK_UNLOCK(&((bfap)->cacheModeLock))
#  define ADVRWL_BFAPCACHEMODE_UPGRADE(bfap) \
	  ADVFS_RWLOCK_UPGRADE(&((bfap)->cacheModeLock))
#  define ADVRWL_BFAPCACHEMODE_HELD_EXCL(bfap) \
	  ADVFS_RWLOCK_WROWNED(&((bfap)->cacheModeLock))
#  define ADVRWL_BFAPCACHEMODE_DISOWN(bfap) \
	  ADVFS_RWLOCK_DISOWN(&((bfap)->cacheModeLock))
#  define ADVRWL_BFAPCACHEMODE_CLAIM(bfap,key) \
	  ADVFS_RWLOCK_CLAIM(&((bfap)->cacheModeLock),(key))

/*
 *    Name:     "Advfs file context lock"
 *    Formerly: "fsContext.file_lock"
 */

#   define ADVRWL_FILECONTEXT_T ADVFS_RWLOCK_T

#   define ADVRWL_FILECONTEXT_INIT( sLk )           \
        ADVFS_RWLOCK_INIT( sLk,                     \
                     "Advfs file context lock",  \
                     NULL,                          \
                     RWL_WAITOK,                    \
                     ADVRWL_FILECONTEXT_LOCK_ORDER, \
		     NULL)

#   define ADVRWL_FILECONTEXT_DESTROY( sLk )  ADVFS_RWLOCK_DESTROY( sLk )

#define ADVRWL_FILECONTEXT_READ_LOCK(cp) \
	ADVFS_RWLOCK_READ(&((cp)->file_lock))
#define ADVRWL_FILECONTEXT_WRITE_LOCK(cp) \
	ADVFS_RWLOCK_WRITE(&((cp)->file_lock))
#define ADVRWL_FILECONTEXT_UNLOCK(cp) \
	ADVFS_RWLOCK_UNLOCK(&((cp)->file_lock))
#define ADVRWL_FILECONTEXT_UPGRADE(cp) \
        ADVFS_RWLOCK_UPGRADE(&((cp)->file_lock))
#define ADVRWL_FILECONTEXT_TRYUPGRADE(cp) \
        ADVFS_RWLOCK_TRYUPGRADE(&((cp)->file_lock))
#define ADVRWL_FILECONTEXT_READ_TRY_LOCK(cp) \
	ADVFS_RWLOCK_TRYREAD(&((cp)->file_lock))
#define ADVRWL_FILECONTEXT_ISWRLOCKED(cp) \
        ADVFS_RWLOCK_WROWNED(&((cp)->file_lock))

/*
 *    Name:     "Advfs metaFlushLock mutex"
 */

#   define ADVMTX_METAFLUSH_LOCK_T ADVFS_MUTEX_T

#   define ADVMTX_METAFLUSH_INIT(lock)           \
        ADVFS_MUTEX_INIT(lock,                   \
                   "Advfs metaflush mutex",      \
                   NULL,                         \
                   MTX_WAITOK,                   \
                   ADVMTX_METAFLUSH_LOCK_ORDER,  \
		   NULL)

#   define ADVMTX_METAFLUSH_DESTROY( sLk) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_METAFLUSH_LOCK( sLk)    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_METAFLUSH_UNLOCK( sLk)  ADVFS_MUTEX_UNLOCK( sLk )

/*
 * Complex locks and mutexes - Group 2
 */

/*
 *    Name:     "Advfs domain rmvol trunc lock"
 *    Formerly: "domainT.rmvolTruncLk"
 */

#   define ADVRWL_RMVOL_TRUNC_T ADVFS_RWLOCK_T

#   define ADVRWL_RMVOL_TRUNC_INIT(dmnp)             \
        ADVFS_RWLOCK_INIT( &(dmnp)->rmvolTruncLk,    \
                    "Advfs domain rmvol trunc lock", \
                    NULL,                            \
                    RWL_WAITOK,                      \
                    ADVRWL_RMVOL_TRUNC_LOCK_ORDER,   \
		    NULL)

#   define ADVRWL_RMVOL_TRUNC_DESTROY(dmnp) \
	   ADVFS_RWLOCK_DESTROY( &(dmnp)->rmvolTruncLk )
#   define ADVRWL_RMVOL_TRUNC_WRITE(dmnp) \
	   ADVFS_RWLOCK_WRITE( &(dmnp)->rmvolTruncLk )
#   define ADVRWL_RMVOL_TRUNC_READ(dmnp) \
	   ADVFS_RWLOCK_READ( &(dmnp)->rmvolTruncLk )
#   define ADVRWL_RMVOL_TRUNC_UNLOCK(dmnp) \
	   ADVFS_RWLOCK_UNLOCK( &(dmnp)->rmvolTruncLk )

/*
 *    Name:     "Advfs bfap flush lock"
 *    Formerly: New for hp/ux
 */

/* The flush lock. This is a complex lock that is used in a very
 * limited scope. It synchronizes the flush path with the deallocation
 * /recycling code. The flush path DEPENDS on NO other code paths
 * taking this lock for write!. This lock is used to keep the vnode
 * from being destroyed. This keeps the vm data structures sane while
 * scanning the cache for pages. DO NOT USE THIS LOCK anywhere else!
*/

#   define ADVRWL_BFAP_FLUSH_INIT(sLk)             \
        ADVFS_RWLOCK_INIT( sLk,                    \
                     "Advfs bfap flush lock",      \
                     NULL,                         \
                     RWL_WAITOK,                   \
                     ADVRWL_BFAP_FLUSH_LOCK_ORDER, \
		     NULL )

#define ADVRWL_BFAP_FLUSH_DESTROY(_bfap) \
        ADVFS_RWLOCK_DESTROY(&((_bfap)->flush_lock))
#define ADVRWL_BFAP_FLUSH_WRITE(_bfap) \
        ADVFS_RWLOCK_WRITE(&((_bfap)->flush_lock))
#define ADVRWL_BFAP_FLUSH_READ_TRY(_bfap) \
        ADVFS_RWLOCK_TRYREAD(&((_bfap)->flush_lock))
#define ADVRWL_BFAP_FLUSH_UNLOCK(_bfap) \
        ADVFS_RWLOCK_UNLOCK(&((_bfap)->flush_lock))

/*
 *    Name:     "Advfs bfap snap lock"
 */
#   define ADVRWL_BFAP_SNAP_LOCK_INIT(sLk)               \
        ADVFS_RWLOCK_INIT(sLk,                           \
                    "Advfs bfap snap lock",              \
                    NULL,                                \
                    RWL_WAITOK,                          \
                    ADVRWL_BFAP_SNAP_LOCK_ORDER,         \
		    NULL )

#define ADVRWL_BFAP_SNAP_WRITE_LOCK( _bfap )             \
        ADVFS_RWLOCK_WRITE( &((_bfap)->bfaSnapLock) )    \

#define ADVRWL_BFAP_SNAP_WRITE_LOCK_TRY( _bfap )         \
        ADVFS_RWLOCK_TRYWRITE( &((_bfap)->bfaSnapLock) ) \

#define ADVRWL_BFAP_SNAP_READ_LOCK( _bfap )              \
        ADVFS_RWLOCK_READ ( &((_bfap)->bfaSnapLock) )    \

#define ADVRWL_BFAP_SNAP_UNLOCK( _bfap )                 \
        ADVFS_RWLOCK_UNLOCK( &((_bfap)->bfaSnapLock) )   \


#   define ADVRWL_BFAP_SNAP_LOCK_DESTROY( _bfap)         \
        ADVFS_RWLOCK_DESTROY( &((_bfap)->bfaSnapLock) );
        

/*
 *    Name:     "Advfs bfap.xtnt migrate/stg lock"
 *    Formerly: "bsInMemXtntT.migStgLk"
 */

#   define ADVRWL_MIGSTG_T ADVFS_RWLOCK_T

#   define ADVRWL_MIGSTG_INIT( __bfap )                  \
        ADVFS_RWLOCK_INIT( &(__bfap)->xtnts.migStgLk,    \
                     "Advfs bfap.xtnt migrate/stg lock", \
                     NULL,                               \
                     RWL_WAITOK,                         \
                     ADVRWL_BFAP_MIGSTG_LOCK_ORDER,      \
		     NULL )

#   define ADVRWL_MIGSTG_DESTROY( __bfap )              \
        ADVFS_RWLOCK_DESTROY( &(__bfap)->xtnts.migStgLk)
#   define ADVRWL_MIGSTG_WRITE( __bfap )                \
        ADVFS_RWLOCK_WRITE( &(__bfap)->xtnts.migStgLk)
#   define ADVRWL_MIGSTG_UNLOCK( __bfap )               \
        ADVFS_RWLOCK_UNLOCK( &(__bfap)->xtnts.migStgLk)
#   define ADVRWL_MIGSTG_UPGRADE( __bfap )              \
        ADVFS_RWLOCK_UPGRADE( &(__bfap)->xtnts.migStgLk)
#   define ADVRWL_MIGSTG_DOWNGRADE( __bfap )            \
        ADVFS_RWLOCK_DOWNGRADE ( &(__bfap)->xtnts.migStgLk)
#   define ADVRWL_MIGSTG_WRITE_TRY( __bfap )            \
        ADVFS_RWLOCK_TRYWRITE( &(__bfap)->xtnts.migStgLk)
#   define ADVRWL_MIGSTG_READ_TRY( __bfap )             \
        ADVFS_RWLOCK_TRYREAD( &(__bfap)->xtnts.migStgLk)

/* 
 * The migStgLk is used to synchronize with migrate.  Typically, migrate is
 * the only one to acquire the lock for write. In the rare case of a migrate
 * of a child snapshot metadata file, migrate must drop the migStgLk in the
 * middle of a migrate to prevent a deadlock with a COW of the parent to the
 * child being migrated.  The deadlock would be between the migStgLk and the
 * ftxSlot lock.  When the deadlock will be resolved by a series of lock
 * tries in migrate and advfs_acquire_snap_locks when this situation arises.
 * However, if another thread acquires the migStgLk of the parent for read,
 * then a deadlock could occur.  In order to prevent the third order
 * deadlocks, whenever the migStgLk is acquired for read, a check will be
 * made to see if the BFA_SWITCH_STG flag is set.  This check will generally
 * be very fast but if it is set, then migrate has dropped the lock and
 * requires a thread that can block starting a transaction while holding the
 * migStg lock to complete. The basic premise is that transaction slots are
 * a scare resource and rather than making migrate block waiting for one,
 * another thread will perform the remainder of migrate in its own
 * transaction context.
 */
#   define ADVRWL_MIGSTG_READ( __bfap )   \
do {                                                    \
        ADVFS_RWLOCK_READ( &(__bfap)->xtnts.migStgLk ); \
        if ( (__bfap)->bfaFlags & BFA_SWITCH_STG) {     \
            /* Upgrade the lock to write or drop and    \
             * acquire. The lock is not dropped on      \
             * failure.  1 is success, 0 if failure */  \
            if (ADVRWL_MIGSTG_UPGRADE( __bfap )) {      \
                (void) advfs_migrate_switch_stg_full(   \
                        (__bfap), FtxNilFtxH );         \
                ADVRWL_MIGSTG_DOWNGRADE( __bfap );      \
                /* That finishes the case of            \
                 * successfully upgrading */            \
            } else {                                    \
                /* We failed to upgrade. */             \
                ADVRWL_MIGSTG_UNLOCK( __bfap );         \
                ADVRWL_MIGSTG_WRITE( __bfap );          \
                if ( (__bfap->bfaFlags & BFA_SWITCH_STG) ) {    \
                    (void) advfs_migrate_switch_stg_full(       \
                            (__bfap), FtxNilFtxH );     \
                     ADVRWL_MIGSTG_DOWNGRADE( __bfap ); \
                } else {                                \
                    /* The migrate thread must have     \
                     * finished while we acquire        \
                     * the write lock */                \
                    ADVRWL_MIGSTG_DOWNGRADE( __bfap );  \
                }                                       \
            }                                           \
        }                                               \
} while(0)





/*
 *    Name:     "Advfs ddl active lock"
 *    Formerly: "ddlActiveLk"
 */

#   define ADVRWL_DDLACTIVE_T ADVFS_RWLOCK_T

#   define ADVRWL_DDLACTIVE_INIT( sLk )          \
        ADVFS_RWLOCK_INIT(sLk,                   \
                    "Advfs ddl active lock",     \
                    NULL,                        \
                    RWL_WAITOK,                  \
                    ADVRWL_DDLACTIVE_LOCK_ORDER, \
		    NULL)

#   define ADVRWL_DDLACTIVE_DESTROY( sLk )  ADVFS_RWLOCK_DESTROY( sLk )
#   define ADVRWL_DDLACTIVE_READ( sLk )     ADVFS_RWLOCK_READ( sLk )
#   define ADVRWL_DDLACTIVE_WRITE( sLk )    ADVFS_RWLOCK_WRITE( sLk )
#   define ADVRWL_DDLACTIVE_UNLOCK( sLk )   ADVFS_RWLOCK_UNLOCK( sLk )

/*
 *    Name:     "Advfs domain ftx slot lock"
 *    Formerly: "domainT.ftxSlotLock"
 */             

#   define ADVRWL_DOMAIN_FTXSLOT_T ADVFS_RWLOCK_T

#   define ADVRWL_DOMAIN_FTXSLOT_INIT( sLk )           \
        ADVFS_RWLOCK_INIT( sLk,                        \
                     "Advfs domain ftx slot lock",     \
                     NULL,                             \
                     RWL_WAITOK,                       \
                     ADVRWL_DOMAIN_FTXSLOT_LOCK_ORDER, \
		     NULL)

#   define ADVRWL_DOMAIN_FTXSLOT_DESTROY( sLk ) ADVFS_RWLOCK_DESTROY( sLk )
#   define ADVRWL_DOMAIN_FTXSLOT_READ( sLk )    ADVFS_RWLOCK_READ( sLk )
#   define ADVRWL_DOMAIN_FTXSLOT_UNLOCK( sLk )  ADVFS_RWLOCK_UNLOCK( sLk )

/*
 *    Name:     "Advfs bf set tbl lock"
 *    Formerly: "BfSetTblLock"
 */

#   define ADVFTX_BFSETTBL_INIT( sLk, sLk2 )       \
        FTX_LOCKINIT(sLk,                          \
                     sLk2,                         \
                     ADVRWL_BFSET_TBL_LOCK_ORDER,  \
                     "Advfs bf set tbl lock" )

#   define ADVRWL_BFSETTBL_WRITE(dmnp) \
           ADVFS_RWLOCK_WRITE( &(dmnp)->BfSetTblLock.lock.rw )
#   define ADVRWL_BFSETTBL_READ(dmnp) \
           ADVFS_RWLOCK_READ( &(dmnp)->BfSetTblLock.lock.rw ) 
#   define ADVRWL_BFSETTBL_UNLOCK(dmnp) \
           ADVFS_RWLOCK_UNLOCK( &(dmnp)->BfSetTblLock.lock.rw ) 
#   define ADVRWL_BFSETTBL_DESTROY(dmnp) \
           ADVFS_RWLOCK_DESTROY( &(dmnp)->BfSetTblLock.lock.rw )
#   define ADVRWL_BFSETTBL_ISWRLOCKED(dmnp) \
           ADVFS_RWLOCK_WROWNED( &(dmnp)->BfSetTblLock.lock.rw )

/*
 *    Name:     "Advfs quota info qi mutex"
 *    Formerly: "quotaInfoT_qiLock"
 */

#define ADVSTR_QI_LOCK "Advfs quota info qi mutex"
#define ADVNAM_QI_LOCK advfs_name_qi_lock
extern char advfs_name_qi_lock[];

#   define ADVMTX_QILOCK_INIT( sLk )                \
        ADVFS_MUTEX_INIT(sLk,                       \
                   advfs_name_qi_lock,              \
                   NULL,                            \
                   MTX_WAITOK,                      \
                   ADVMTX_QUOTA_INFO_QI_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_QILOCK_DESTROY( lock) ADVFS_MUTEX_DESTROY( lock )
#   define ADVMTX_QILOCK_WRITE( lock )  ADVFS_MUTEX_LOCK( lock )
#   define ADVMTX_QILOCK_UNLOCK( lock)  ADVFS_MUTEX_UNLOCK( lock )

/*
 *    Name:     "Advfs set dir lock"
 *    Formerly: "bfSetT.dirLock"
 */

#   define ADVFTM_SETTAGDIR_INIT( sLk, sLk2 )   \
        FTX_MUTEXINIT(sLk,                      \
                     sLk2,                      \
                     ADVFTM_SETTAGDIR_LOCK_ORDER, \
                     "Advfs set dir lock" )

#   define ADVFTM_SETTAGDIR_LOCK( bfSetp, ftxH ) \
    ftx_mutex_lock(&bfSetp->dirLock, ftxH, __LINE__, __FILE__)


/*
 *    Name:     "Advfs bfap mcell list lock"
 *    Formerly: "bfAccessT.mcellList_lk"
 *    Name:     "Advfs BMT bfap mcell list lock"
 *    Formerly: "BMT.bfAccessT.mcellList_lk"
 */

#define ADVSTR_BFAP_MCELL_LIST_LOCK "Advfs bfap mcell list lock"
#define ADVNAM_BFAP_MCELL_LIST_LOCK advfs_name_bfap_mcell_list_lock
extern char ADVNAM_BFAP_MCELL_LIST_LOCK[];

#   define ADVFTX_MCELLFTX_INIT( sLk, sLk2 )            \
        FTX_LOCKINIT(sLk,                               \
                     sLk2,                              \
                     ADVRWL_BFAP_MCELL_LIST_LOCK_ORDER, \
                     ADVNAM_BFAP_MCELL_LIST_LOCK)

#   define ADVRWL_MCELL_LIST_INIT( sLk )               \
        ADVFS_RWLOCK_INIT(sLk,                         \
                    ADVNAM_BFAP_MCELL_LIST_LOCK,       \
                    NULL,                              \
                    RWL_WAITOK,                        \
                    ADVRWL_BFAP_MCELL_LIST_LOCK_ORDER, \
		    NULL )

#   define ADVRWL_BMT_MCELL_LIST_INIT( sLk )           \
        ADVFS_RWLOCK_INIT( sLk,                        \
                     "Advfs BMT bfap mcell list lock", \
                     NULL,                             \
                     RWL_WAITOK,                       \
                     ADVRWL_BMT_MCELL_LIST_LOCK_ORDER, \
		     NULL )

#   define ADVRWL_BMT_MCELL_LIST_LOCK_DESTROY(sLk) ADVFS_RWLOCK_DESTROY( sLk ) 
#   define ADVRWL_BMT_MCELL_LIST_LOCK_READ(sLk)    ADVFS_RWLOCK_READ( sLk )
#   define ADVRWL_BMT_MCELL_LIST_LOCK_UNLOCK(sLk)  ADVFS_RWLOCK_UNLOCK( sLk )


#   define ADVRWL_MCELL_LIST_DESTROY(sLk)   ADVFS_RWLOCK_DESTROY( sLk )
#   define ADVRWL_MCELL_LIST_WRITE(sLk)     ADVFS_RWLOCK_WRITE( sLk.lock.rw )
#   define ADVRWL_MCELL_LIST_TRY_WRITE(sLk) ADVFS_RWLOCK_TRYWRITE( sLk.lock.rw )
#   define ADVRWL_MCELL_LIST_READ(sLk)      ADVFS_RWLOCK_READ( sLk.lock.rw )
#   define ADVRWL_MCELL_LIST_UNLOCK(sLk)    ADVFS_RWLOCK_UNLOCK( sLk.lock.rw )
#   define ADVRWL_MCELL_LIST_ISWRLOCKED(sLk) ADVFS_RWLOCK_WROWNED( sLk.lock.rw )

/* DGA - We need to create an Advfs RBMT bfap xtntmap lock
 * that is below the rbmt_mcell_lk in the vd structure
 */



/*
 *    Name:     "Advfs bfap xtntmap lock"
 *    Formerly: "bfAccessT.xtntMap_lk"
 *    Name:     "Advfs BMT bfap xtntmap lock"
 *    Formerly: "BMT.bfAccessT.xtntMap_lk"
 *
 * The BMT xtntmap lock must have a separate lock hierarchy
 * position (inner to the normal bfap lock), for cases where
 * an extend implicitly extends the BMT.
 */

#define ADVSTR_BFAP_XTNTMAP_LOCK "Advfs bfap xtntmap lock"
#define ADVNAM_BFAP_XTNTMAP_LOCK advfs_name_bfap_xtntmap_lock
extern char advfs_name_bfap_xtntmap_lock[];

#   define ADVRWL_XTNTMAP_T ADVFS_RWLOCK_T

#   define ADVRWL_XTNTMAP_INIT(sLk)                 \
        ADVFS_RWLOCK_INIT(&bfap->xtntMap_lk,        \
                   ADVNAM_BFAP_XTNTMAP_LOCK,        \
                   NULL,                            \
                   RWL_WAITOK,                      \
                   ADVRWL_BFAP_XTNTMAP_LOCK_ORDER,  \
		   NULL )

#   define ADVRWL_BMT_XTNTMAP_INIT( sLk )           \
        ADVFS_RWLOCK_INIT(sLk,                      \
                    "Advfs BMT bfap xtnt map lock", \
                    NULL,                           \
                    RWL_WAITOK,                     \
                    ADVRWL_BMT_XTNTMAP_LOCK_ORDER,  \
		    NULL )

#   define ADVRWL_XTNTMAP_DESTROY( sLk )    ADVFS_RWLOCK_DESTROY( sLk )
#   define ADVRWL_XTNTMAP_WRITE( sLk )      ADVFS_RWLOCK_WRITE( sLk )
#   define ADVRWL_XTNTMAP_READ( sLk )       ADVFS_RWLOCK_READ( sLk )
#   define ADVRWL_XTNTMAP_TRY_READ( sLk)    ADVFS_RWLOCK_TRYREAD( sLk )
#   define ADVRWL_XTNTMAP_TRY_WRITE( sLk)   ADVFS_RWLOCK_TRYWRITE( sLk )
#   define ADVRWL_XTNTMAP_UNLOCK( sLk )     ADVFS_RWLOCK_UNLOCK( sLk )
#   define ADVRWL_XTNTMAP_DOWNGRADE( sLk )  ADVFS_RWLOCK_DOWNGRADE( sLk )
#   define ADVRWL_XTNTMAP_ISWRLOCKED( sLk ) ADVFS_RWLOCK_WROWNED( sLk )

/*
 *    Name:     "Advfs VHAND bfap xtntmap lock"
 *
 * The VHAND extent map lock is used to allow VHAND to 
 * flush pages when the xtntMap_lk is contented. A thread
 * could be holding the xtntMap_lk for read and be waiting
 * for memory while another thread could attempt to obtain
 * the xtntMap_lk for write. No if VHAND tried to get the lock
 * (even in try mode) we could deadlock since the thread
 * waiting for memory may have all the UFC pages and is waiting
 * for VHAND who can't make progress because he needs to get
 * in line behind the thread waiting for write access.
 *
 * We can get around this since no thread holding the xtntMap_lk
 * for write will ever block on memory or UFC pages. This VHAND
 * xtntMap_lk will be obtain after any thread gets the xtntMap_lk
 * for write. VHAND will now only need this lock in putpage instead
 * of the xtntMap_lk for read.
 * 
 */

#   define ADVRWL_VHAND_XTNTMAP_T ADVFS_RWLOCK_T

#   define ADVRWL_VHAND_XTNTMAP_INIT(sLk)               \
        ADVFS_RWLOCK_INIT(&bfap->vhand_xtntMap_lk, \
                   "Advfs VHAND bfap xtntmap lock",     \
                   NULL,                          \
                   RWL_WAITOK,                    \
                   ADVRWL_VHAND_XTNTMAP_LOCK_ORDER,     \
		   NULL )

#   define ADVRWL_VHAND_XTNTMAP_DESTROY( sLk )    ADVFS_RWLOCK_DESTROY( sLk )
#   define ADVRWL_VHAND_XTNTMAP_WRITE( sLk )      ADVFS_RWLOCK_WRITE( sLk )
#   define ADVRWL_VHAND_XTNTMAP_TRY_READ( sLk)    ADVFS_RWLOCK_TRYREAD( sLk )
#   define ADVRWL_VHAND_XTNTMAP_UNLOCK( sLk )     ADVFS_RWLOCK_UNLOCK( sLk )

/*
 *    Name:     "Advfs fileset list lock"
 *    Formerly: "FilesetLock"
 */

#   define ADVRWL_FILESETLIST_T ADVFS_RWLOCK_T

#   define ADVRWL_FILESETLIST_INIT( sLk )          \
        ADVFS_RWLOCK_INIT(sLk,                     \
                    "Advfs fileset list lock",     \
                    NULL,                          \
                    RWL_WAITOK,                    \
                    ADVRWL_FILESETLIST_LOCK_ORDER, \
		    NULL)

#   define ADVRWL_FILESETLIST_WRITE( fsl )  ADVFS_RWLOCK_WRITE( fsl )
#   define ADVRWL_FILESETLIST_READ( fsl )   ADVFS_RWLOCK_READ( fsl )
#   define ADVRWL_FILESETLIST_UNLOCK( fsl ) ADVFS_RWLOCK_UNLOCK( fsl )

/*
 *    Name:     "Advfs vdt mcell lock"
 *    Formerly: "vdT.mcell_lk"
 */

#   define ADVFTM_VDT_MCELL_INIT( sLk, sLk2 )     \
        FTX_MUTEXINIT(sLk,                        \
                     sLk2,                        \
                     ADVFTM_VDT_MCELL_LOCK_ORDER, \
                     "Advfs vdt mcell lock")
#   define ADVFTM_VDT_MCELL_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )

#define ADVFTM_MCELL_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk.lock.mtx )
#define ADVFTM_MCELL_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk.lock.mtx )

/*
 *    Name:     "Advfs vdt del list lock"
 *    Formerly: "vdT.del_list_lk"
 */

#   define ADVFTX_VDT_DELLIST_INIT( sLk, sLk2 )     \
        FTX_LOCKINIT(sLk,                           \
                     sLk2,                          \
                     ADVRWL_VDT_DELLIST_LOCK_ORDER, \
                     "Advfs vdt del list lock")

#   define ADVFTX_VDT_DELLIST_DESTROY( sLk ) ADVFS_RWLOCK_DESTROY( sLk )
#   define ADVFTX_VDT_DELLIST_WRITE( sLk )   ADVFS_RWLOCK_WRITE( sLk.lock.rw )
#   define ADVFTX_VDT_DELLIST_UNLOCK( sLk )  ADVFS_RWLOCK_UNLOCK( sLk.lock.rw )

/*
 *    Name:     "Advfs domain sc mutex"
 *    Formerly: "domainT.scLock"
 */

#   define ADVMTX_SC_TBL_INIT(dmnp)         \
       ADVFS_MUTEX_INIT(&(dmnp)->scLock,    \
                  "Advfs domain sc mutex",  \
                  NULL,                     \
                  MTX_WAITOK,               \
                  ADVMTX_SC_TBL_LOCK_ORDER, \
		  NULL)

#   define ADVMTX_SC_TBL_DESTROY(dmnp) ADVFS_MUTEX_DESTROY( &(dmnp)->scLock )
#   define ADVMTX_SC_TBL_LOCK(dmnp)    ADVFS_MUTEX_LOCK( &(dmnp)->scLock )
#   define ADVMTX_SC_TBL_UNLOCK(dmnp)  ADVFS_MUTEX_UNLOCK( &(dmnp)->scLock )

/*
 *    Name:     "Advfs vdt rbmt mcell lock"
 *    Formerly: "vdT.rbmt_mcell_lk"
 */

#   define ADVFTX_RBMT_MCELL_INIT( sLk, sLk2 )      \
        FTX_LOCKINIT( sLk,                          \
                      sLk2,                         \
                      ADVRWL_RBMT_MCELL_LOCK_ORDER, \
                      "Advfs vdt rbmt mcell lock")

#   define ADVFTX_RBMT_MCELL_DESTROY( sLk ) ADVFS_RWLOCK_DESTROY( sLk )

/*
 *    Name:     "Advfs vdt storage map mutex"
 *    Formerly: "vdT.stgMap_lk"
 */

#   define ADVFTM_VDT_STGMAP_INIT( sLk, sLk2 )     \
        FTX_MUTEXINIT(sLk,                          \
                     sLk2,                         \
                     ADVFTM_VDT_STGMAP_LOCK_ORDER, \
                     "Advfs vdt storage map lock")

#define ADVFTM_VDT_STGMAP_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk.lock.mtx)
#define ADVFTM_VDT_STGMAP_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk.lock.mtx)
#define ADVFTM_VDT_STGMAP_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk.lock.mtx )

/*
 *    Name:     "Advfs domain xid recovery lock"
 *    Formerly: "domainT.xidRecoveryLk"
 */

#   define ADVRWL_XID_RECOVERY_T ADVFS_RWLOCK_T

#   define ADVRWL_XID_RECOVERY_INIT(dmnp)               \
        ADVFS_RWLOCK_INIT( &(dmnp->xidRecoveryLk),      \
                    "Advfs domain xid recovery lock",   \
                    NULL,                               \
                    RWL_WAITOK,                         \
                    ADVRWL_DMN_XID_RECOVERY_LOCK_ORDER, \
		    NULL )

#   define ADVRWL_XID_RECOVERY_WRITE(dmnp) \
	   ADVFS_RWLOCK_WRITE( &(dmnp->xidRecoveryLk) )
#   define ADVRWL_XID_RECOVERY_TRY_WRITE(dmnp) \
	   ADVFS_RWLOCK_TRYWRITE( &(dmnp->xidRecoveryLk) )
#   define ADVRWL_XID_RECOVERY_READ(dmnp) \
	   ADVFS_RWLOCK_READ( &(dmnp->xidRecoveryLk) )
#   define ADVRWL_XID_RECOVERY_UNLOCK(dmnp) \
	   ADVFS_RWLOCK_UNLOCK( &(dmnp->xidRecoveryLk) )
#   define ADVRWL_XID_RECOVERY_DESTROY(dmnp) \
	   ADVFS_RWLOCK_DESTROY( &(dmnp->xidRecoveryLk) )

/*
 * Complex locks and mutexes - Suppressed Hierarchy Group
 */

/*
 *    Name:     "Advfs trace mutex"
 *    Formerly: "TraceLock"
 */

#   define ADVMTX_TRACE_INIT( sLk )         \
        ADVFS_MUTEX_INIT(sLk,               \
                   "Advfs trace mutex",     \
                   NULL,                    \
                   MTX_WAITOK,              \
                   ADVMTX_TRACE_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_TRACE_LOCK( sLk )   ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_TRACE_UNLOCK( sLk ) ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs Dynamic Hash Thread Mutex"
 *    Formerly: This is a new lock
 */

#   define ADVMTX_DYNHASHTHREAD_T ADVFS_MUTEX_T

#   define ADVMTX_DYNHASHTHREAD_INIT( sLk )           \
        ADVFS_MUTEX_INIT(sLk,                         \
                   "Advfs dynamic hash thread mutex", \
                   NULL,                              \
                   MTX_WAITOK,                        \
                   ADVMTX_DYNHASHTHREAD_LOCK_ORDER,   \
		   NULL)

#   define ADVMTX_DYNHASHTHREAD_LOCK( sLk )   ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_DYNHASHTHREAD_UNLOCK( sLk ) ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs dquot mutex"
 *    Formerly: "dQuot.dqLock"
 */

#   define ADVMTX_DQUOT_INIT(dq, dnp)       \
        ADVFS_MUTEX_INIT(&(dq)->dqLock,     \
                   "Advfs dquot mutex",     \
                   NULL,                    \
                   MTX_WAITOK,              \
                   ADVMTX_DQUOT_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_DQUOT_DESTROY(dq) ADVFS_MUTEX_DESTROY(&(dq)->dqLock)
#   define ADVMTX_DQUOT_LOCK(dq)    ADVFS_MUTEX_LOCK(&(dq)->dqLock) 
#   define ADVMTX_DQUOT_UNLOCK(dq)  ADVFS_MUTEX_UNLOCK(&(dq)->dqLock)


/*
 *    Name:     "Advfs log desc mutex"
 *    Formerly: "logDescT.descLock"
 */

#   define ADVMTX_LOGDESC_T   ADVFS_MUTEX_T

#   define ADVMTX_LOGDESC_INIT( sLk )          \
        ADVFS_MUTEX_INIT(sLk,                  \
                    "Advfs log desc mutex",    \
                    NULL,                      \
                    MTX_WAITOK,                \
                    ADVMTX_LOGDESC_LOCK_ORDER, \
		    NULL )
#   define ADVMTX_LOGDESC_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_LOGDESC_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_LOGDESC_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )
#   define ADVMTX_LOGDESC_OWNED( sLk )   ADVFS_MUTEX_OWNED( sLk )

/*
 *    Name:     "Advfs log desc flush mutex"
 *    Formerly: "logDescT.flushLock"
 */

#   define ADVMTX_LOGDESC_FLUSH_T   ADVFS_MUTEX_T

#   define ADVMTX_LOGDESC_FLUSH_INIT( sLk )          \
        ADVFS_MUTEX_INIT(sLk,                        \
                    "Advfs log desc flush mutex",    \
                    NULL,                            \
                    MTX_WAITOK,                      \
                    ADVMTX_LOGDESC_FLUSH_LOCK_ORDER, \
		    NULL )

#   define ADVMTX_LOGDESC_FLUSH_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_LOGDESC_FLUSH_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_LOGDESC_FLUSH_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )
#   define ADVMTX_LOGDESC_FLUSH_OWNED( sLk )   ADVFS_MUTEX_OWNED( sLk )

/* 
 *    Name:     "Advfs SSLock mutex"
 *    Formerly: "SSLock"
 */

#   define ADVMTX_SS_INIT(ssl)           \
        ADVFS_MUTEX_INIT(ssl,            \
                   "Advfs SSLock mutex", \
                   NULL,                 \
                   MTX_WAITOK,           \
                   ADVMTX_SS_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_SS_DESTROY( ssl ) ADVFS_MUTEX_DESTROY( ssl )
#   define ADVMTX_SS_LOCK( ssl )    ADVFS_MUTEX_LOCK( ssl )
#   define ADVMTX_SS_UNLOCK( ssl )  ADVFS_MUTEX_UNLOCK( ssl )

#   define ADVRWL_BFAP_BFA_ACL_LOCK_INIT( sLk )          \
        ADVFS_RWLOCK_INIT(sLk,                           \
                    "Advfs bfap acl lock",               \
                    NULL,                                \
                    RWL_WAITOK,                          \
                    ADVRWL_BFAP_BFA_ACL_LOCK_ORDER,      \
		    NULL )

#define ADVRWL_BFAP_BFA_ACL_LOCK_DESTROY( sLk ) ADVFS_RWLOCK_DESTROY( sLk )
#define ADVRWL_BFAP_BFA_ACL_LOCK_WRITE( sLk )	ADVFS_RWLOCK_WRITE( sLk );
#define ADVRWL_BFAP_BFA_ACL_LOCK_READ( sLk )	ADVFS_RWLOCK_READ( sLk );
#define ADVRWL_BFAP_BFA_ACL_UNLOCK( sLk )	ADVFS_RWLOCK_UNLOCK( sLk );
#define ADVRWL_BFAP_BFA_ACL_DOWNGRADE( sLk )	ADVFS_RWLOCK_DOWNGRADE( sLk );

/*
 * Simple locks (they were in T64)
 */

/*
 *    Name:     "Advfs domain hash table mutex"
 *    Formerly: "DmnTblMutex"
 */

#   define ADVMTX_DOMAINHASHTABLE_INIT( sLk )          \
        ADVFS_MUTEX_INIT( sLk,                         \
                    "Advfs domain hash table mutex",   \
                    NULL,                              \
                    MTX_WAITOK,                        \
                    ADVMTX_DOMAINHASHTABLE_LOCK_ORDER, \
		    NULL)   
                 
#   define ADVMTX_DOMAINHASHTABLE_OWNED(sLk)   ADVFS_MUTEX_OWNED(sLk)
#   define ADVMTX_DOMAINHASHTABLE_LOCK(sLk)    ADVFS_MUTEX_LOCK(sLk)
#   define ADVMTX_DOMAINHASHTABLE_UNLOCK(sLk)  ADVFS_MUTEX_UNLOCK(sLk)
#   define ADVMTX_DOMAINHASHTABLE_DESTROY(sLk) ADVFS_MUTEX_DESTROY(sLk)


/*
 *    Name:     "Advfs domain mutex"
 *    Formerly: "DomainMutex"
 */

#   define ADVMTX_DOMAIN_T ADVFS_MUTEX_T

#   define ADVMTX_DOMAIN_INIT( sLk )          \
        ADVFS_MUTEX_INIT( sLk,                \
                    "Advfs domain mutex",     \
                    NULL,                     \
                    MTX_WAITOK,               \
                    ADVMTX_DOMAIN_LOCK_ORDER, \
		    NULL)

#   define ADVMTX_DOMAIN_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_DOMAIN_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_DOMAIN_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )
#   define ADVMTX_DOMAIN_OWNED( sLk )   ADVFS_MUTEX_OWNED( sLk )

/*
 *    Name:     "Advfs set access chain mutex"
 *    Formerly: "bfSetT.accessChainLock"
 */

#   define ADVSMP_SETACCESSCHAIN_T ADVFS_SPIN_T

#   define ADVSMP_SETACCESSCHAIN_INIT( sLk )         \
        ADVFS_SPIN_INIT(sLk,                         \
                   "Advfs set access chain lock",    \
                   NULL,                             \
                   SPIN_WAITOK,                      \
                   ADVSMP_SETACCESSCHAIN_LOCK_ORDER, \
		   NULL)

#   define ADVSMP_SETACCESSCHAIN_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSMP_SETACCESSCHAIN_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSMP_SETACCESSCHAIN_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK( sLk )
#   define ADVSMP_SETACCESSCHAIN_OWNED( sLk )   ADVFS_SPIN_OWNED( sLk )

/*
 *    Name:     "Advfs bfap mutex"
 *    Formerly: "bfAccessT.bfaLock"
 */

#   define ADVSMP_BFAP_T ADVFS_SPIN_T

#   define ADVSMP_BFAP_INIT(sLk)           \
        ADVFS_SPIN_INIT(sLk,               \
                   "Advfs bfap lock",      \
	           NULL,                   \
                   SPIN_WAITOK,            \
                   ADVSMP_BFAP_LOCK_ORDER, \
		   NULL)

#   define ADVSMP_BFAP_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSMP_BFAP_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSMP_BFAP_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK( sLk )
#   define ADVSMP_BFAP_TRYLOCK( sLk ) ADVFS_SPIN_TRYLOCK( sLk )
#   define ADVSMP_BFAP_OWNED( sLk )   ADVFS_SPIN_OWNED( sLk )

/*
 *    Name:     "Advfs bfap read-ahead mutex"
 */

#   define ADVMTX_BFAP_READAHEAD_INIT(sLk)           \
        ADVFS_MUTEX_INIT(sLk,                        \
                   "Advfs bfap read-ahead mutex",    \
	           NULL,                             \
                   MTX_WAITOK,                       \
                   ADVMTX_BFAP_READAHEAD_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_BFAP_READAHEAD_LOCK( sLk )   ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_BFAP_READAHEAD_UNLOCK( sLk ) ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Because of common code in advfs_process_access_list that is passed
 *    an access list and a lock address, these two lists must have the
 *    same type and operations.
 */

#define ADVSMP_ACCESS_LIST_T             ADVFS_SPIN_T

#define ADVSMP_ACCESS_LIST_INIT          ADVFS_SPIN_INIT
#define ADVSMP_ACCESS_LIST_LOCK( sLk )   ADVFS_SPIN_LOCK( sLk )
#define ADVSMP_ACCESS_LIST_UNLOCK( sLk ) ADVFS_SPIN_UNLOCK( sLk )
#define ADVSMP_ACCESS_LIST_OWNED( sLk )  ADVFS_SPIN_OWNED( sLk )

/*
 *    Name:     "Advfs bfap access free list mutex"
 */

#   define ADVSMP_FREE_LIST_T ADVSMP_ACCESS_LIST_T

#   define ADVSMP_FREE_LIST_INIT(sLk)               \
        ADVSMP_ACCESS_LIST_INIT( sLk,               \
                    "Advfs access free list mutex", \
                    NULL,                           \
                    SPIN_WAITOK,                    \
                    ADVSMP_FREE_LIST_LOCK_ORDER,    \
		    NULL)

#   define ADVSMP_FREE_LIST_LOCK( sLk )   ADVSMP_ACCESS_LIST_LOCK( sLk )
#   define ADVSMP_FREE_LIST_UNLOCK( sLk ) ADVSMP_ACCESS_LIST_UNLOCK( sLk )
#   define ADVSMP_FREE_LIST_OWNED( sLk )  ADVSMP_ACCESS_LIST_OWNED( sLk )

/*
 *    Name:     "Advfs access closed list mutex"
 */


#   define ADVSMP_CLOSED_LIST_T ADVSMP_ACCESS_LIST_T

#   define ADVSMP_CLOSED_LIST_INIT(sLk)               \
        ADVSMP_ACCESS_LIST_INIT( sLk,                 \
                    "Advfs access closed list mutex", \
                    NULL,                             \
                    SPIN_WAITOK,                      \
                    ADVSMP_CLOSED_LIST_LOCK_ORDER,    \
		    NULL)

#   define ADVSMP_CLOSED_LIST_LOCK( sLk )   ADVSMP_ACCESS_LIST_LOCK( sLk )
#   define ADVSMP_CLOSED_LIST_UNLOCK( sLk ) ADVSMP_ACCESS_LIST_UNLOCK( sLk )
#   define ADVSMP_CLOSED_LIST_OWNED( sLk )  ADVSMP_ACCESS_LIST_OWNED( sLk )

/*
 *    Name:     "Advfs domain ss domain mutex"
 *    Formerly: "domainT.ssDmnInfo.ssDmnLk"
 */

#   define ADVMTX_SSDOMAIN_INIT( sLk )              \
        ADVFS_MUTEX_INIT( sLk,                      \
                    "Advfs domain ss domain mutex", \
                    NULL,                           \
                    MTX_WAITOK,                     \
                    ADVMTX_SSDOMAIN_LOCK_ORDER,     \
		    NULL)

#   define ADVMTX_SSDOMAIN_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_SSDOMAIN_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_SSDOMAIN_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs fileset lock"
 *    Formerly: "filesetMutex"
 */


#define ADVSTR_FILESET_LOCK "Advfs fileset lock"
#define ADVNAM_FILESET_LOCK advfs_name_fileset_lock
extern char advfs_name_fileset_lock[];

#   define ADVSMP_FILESET_T ADVFS_SPIN_T

#   define ADVSMP_FILESET_INIT( sLk )          \
        ADVFS_SPIN_INIT( sLk,                  \
                    ADVSTR_FILESET_LOCK,       \
                    NULL,                      \
                    SPIN_WAITOK,               \
                    ADVSMP_FILESET_LOCK_ORDER, \
                    NULL)

#   define ADVSMP_FILESET_DESTROY( sLk )  ADVFS_SPIN_DESTROY( sLk )
#   define ADVSMP_FILESET_LOCK( sLk )     ADVFS_SPIN_LOCK( sLk )
#   define ADVSMP_FILESET_UNLOCK( sLk )   ADVFS_SPIN_UNLOCK( sLk )

/*
 *    Name:     "Advfs file stats lock"
 *    Formerly: "fsContext_mutex"
 */

#   define ADVSMP_FILESTATS_T ADVFS_SPIN_T

#   define ADVSMP_FILESTATS_INIT( sLk )          \
        ADVFS_SPIN_INIT( sLk,                    \
                    "Advfs file stats lock",     \
                    NULL,                        \
                    SPIN_WAITOK,                 \
                    ADVSMP_FILESTATS_LOCK_ORDER, \
		    NULL)

#   define ADVSMP_FILESTATS_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSMP_FILESTATS_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSMP_FILESTATS_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK(sLk )

/*
 *    Name:     "Advfs bfs set mutex"
 *    Formerly: "bfSetT.setMutex"
 */

#   define ADVMTX_BFSSET_INIT( sLk )          \
        ADVFS_MUTEX_INIT( sLk,                \
                    "Advfs bfs set mutex",    \
                    NULL,                     \
                    MTX_WAITOK,               \
                    ADVMTX_BFSSET_LOCK_ORDER, \
		    NULL)
#   define ADVMTX_BFSSET_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )


/*
 *    Name:     "Advfs domain ss domain hot mutex"
 *    Formerly: "domainT.ssDmnInfo.ssDmnHotLk"
 */

#   define ADVMTX_SSDOMAIN_HOT_INIT( sLk )              \
        ADVFS_MUTEX_INIT( sLk,                          \
                    "Advfs domain ss domain hot mutex", \
                    NULL,                               \
                    MTX_WAITOK,                         \
                    ADVMTX_SSDOMAIN_HOT_LOCK_ORDER,     \
		    NULL)

#   define ADVMTX_SSDOMAIN_HOT_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_SSDOMAIN_HOT_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_SSDOMAIN_HOT_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs domain vdp tbl mutex"
 *    Formerly: "domainT.vdpTblLock"
 */

#   define ADVMTX_DOMAINVDPTBL_T ADVFS_MUTEX_T

#   define ADVMTX_DOMAINVDPTBL_INIT( sLk )          \
        ADVFS_MUTEX_INIT( sLk,                      \
                    "Advfs domain vdp tbl mutex",   \
                    NULL,                           \
                    MTX_WAITOK,                     \
                    ADVMTX_DOMAINVDPTBL_LOCK_ORDER, \
		    NULL)

#   define ADVMTX_DOMAINVDPTBL_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_DOMAINVDPTBL_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_DOMAINVDPTBL_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs vd state mutex"
 *    Formerly: "vdT.vdStateLock"
 */

#   define ADVMTX_VD_STATE_INIT( sLk )         \
        ADVFS_MUTEX_INIT(sLk,                  \
                   "Advfs vd state mutex",     \
                   NULL,                       \
                   MTX_WAITOK,                 \
                   ADVMTX_VD_STATE_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_VD_STATE_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_VD_STATE_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )
#   define ADVMTX_VD_STATE_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )

/*    Name:     "Advfs ss vd mig mutex"
 *    Formerly: "vdT.ssVolInfo.ssVdMigLk"
 */

#   define ADVMTX_SSVDMIG_INIT( sLk )         \
        ADVFS_MUTEX_INIT(sLk,                 \
                   "Advfs ss vd mig mutex",   \
                   NULL,                      \
                   MTX_WAITOK,                \
                   ADVMTX_SSVDMIG_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_SSVDMIG_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_SSVDMIG_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_SSVDMIG_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )

/*    Name:     "Advfs ss volinfo frag mutex"
 *    Formerly: "vdT.ssVolInfo.ssFragLk"
 */

#   define ADVMTX_VOLINFO_FRAG_INIT( sLk )         \
        ADVFS_MUTEX_INIT(sLk,                      \
                   "Advfs ss volinfo frag mutex",  \
                   NULL,                           \
                   MTX_WAITOK,                     \
                   ADVMTX_VOLINFO_FRAG_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_VOLINFO_FRAG_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_VOLINFO_FRAG_OWNED( sLk )   ADVFS_MUTEX_OWNED( sLk )
#   define ADVMTX_VOLINFO_FRAG_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_VOLINFO_FRAG_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs ss list tpool mutex"
 *    Formerly: "ssListTpool.plock"
 */

#   define ADVMTX_SSLIST_TPOOL_INIT( sLk)           \
        ADVFS_MUTEX_INIT( sLk,                      \
                    "Advfs ss list tpool mutex",    \
                    NULL,                           \
                    MTX_WAITOK,                     \
                    ADVMTX_SSLIST_TPOOL_LOCK_ORDER, \
		    NULL)

#   define ADVMTX_SSLIST_TPOOL_LOCK( sLk )   ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_SSLIST_TPOOL_UNLOCK( sLk ) ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs ss work tpool mutex"
 *    Formerly: "ssWorkTpool.plock"
 */

#   define ADVMTX_SSLIST_WORKTPOOL_INIT( sLk)           \
        ADVFS_MUTEX_INIT( sLk,                          \
                    "Advfs ss work tpool mutex",        \
                    NULL,                               \
                    MTX_WAITOK,                         \
                    ADVMTX_SSLIST_WORKTPOOL_LOCK_ORDER, \
		    NULL)
#   define ADVMTX_SSLIST_WORKTPOOL_LOCK( sLk )   ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_SSLIST_WORKTPOOL_UNLOCK( sLk ) ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs smartstore global mutex"
 *    Formerly: "ssStoppedMutex"
 */

#   define ADVMTX_SMARTSTOREGBL_INIT( sLk )          \
        ADVFS_MUTEX_INIT( sLk,                       \
                    "Advfs smartstore global mutex", \
                    NULL,                            \
                    MTX_WAITOK,                      \
                    ADVMTX_SMARTSTOREGBL_LOCK_ORDER, \
		    NULL)
#   define ADVMTX_SMARTSTOREGBL_LOCK ADVFS_MUTEX_LOCK

/*
 *    Name:     "Advfs bfset set mutex"
 *    Formerly: "bfSetT.bfSetMutex"
 */

#   define ADVSMP_BFSET_T ADVFS_SPIN_T

#   define ADVSMP_BFSET_INIT( sLk )          \
        ADVFS_SPIN_INIT( sLk,               \
                    "Advfs bfset set lock", \
                    NULL,                    \
                    SPIN_WAITOK,              \
                    ADVSMP_BFSET_LOCK_ORDER, \
		    NULL)
#   define ADVSMP_BFSET_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSMP_BFSET_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSMP_BFSET_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK( sLk )

/*
 *    Name:     "Advfs ss tag array mutex"
 */

#   define ADVSMP_SS_TAG_ARRAY_T ADVFS_SPIN_T

#   define ADVSMP_SS_TAG_ARRAY_INIT( sLk )          \
        ADVFS_SPIN_INIT( sLk,               \
                    "Advfs ss tag array lock", \
                    NULL,                    \
                    SPIN_WAITOK,              \
                    ADVSMP_TAG_ARRAY_LOCK_ORDER, \
		    NULL)
#   define ADVSMP_SS_TAG_ARRAY_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSMP_SS_TAG_ARRAY_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSMP_SS_TAG_ARRAY_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK( sLk )

/*
 *    Name:     "Advfs domain freeze mutex"
 *    Formerly: "dmnFreezeMutex"
 */

#   define ADVMTX_DOMAINFREEZE_T       ADVFS_MUTEX_T

#   define ADVMTX_DOMAINFREEZE_INIT( sLk )          \
        ADVFS_MUTEX_INIT( sLk,                      \
                    "Advfs domain freeze mutex",    \
                    NULL,                           \
                    MTX_WAITOK,                     \
                    ADVMTX_DOMAINFREEZE_LOCK_ORDER, \
		    NULL)

#   define ADVMTX_DOMAINFREEZE_DESTROY( sLk ) ADVFS_MUTEX_DESTROY( sLk )
#   define ADVMTX_DOMAINFREEZE_LOCK( sLk )    ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_DOMAINFREEZE_UNLOCK( sLk )  ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs freeze messages mutex"
 *    Formerly: "AdvfsFreezeMsgsLock"
 */

#   define ADVMTX_FREEZE_MSGS_INIT( sLk )         \
        ADVFS_MUTEX_INIT(sLk,                     \
                   "Advfs freeze messages mutex", \
                   NULL,                          \
                   MTX_WAITOK,                    \
                   ADVMTX_FREEZE_MSGS_LOCK_ORDER, \
		   NULL)

#   define ADVMTX_FREEZE_MSGS_LOCK( sLk )   ADVFS_MUTEX_LOCK( sLk )
#   define ADVMTX_FREEZE_MSGS_UNLOCK( sLk ) ADVFS_MUTEX_UNLOCK( sLk )

/*
 *    Name:     "Advfs dynamic hash bucket mutex"
 *    Formerly: "DynHashbucket"
 */
#define ADVSTR_DYNHASH_BUCKET_LOCK "Advfs dynamic hash bucket lock"
#define ADVNAM_DYNHASH_BUCKET_LOCK advfs_name_dynhash_bucket_lock
extern char ADVNAM_DYNHASH_BUCKET_LOCK[];

#   define ADVSMP_DYNHASH_BUCKET_T ADVFS_SPIN_T

#   define ADVSMP_DYNHASH_BUCKETLOCK_INIT( sLk )      \
        ADVFS_SPIN_INIT( sLk,                         \
                    ADVNAM_DYNHASH_BUCKET_LOCK,       \
                    NULL,                             \
                    SPIN_WAITOK,                       \
                    ADVSMP_DYNHASH_BUCKET_LOCK_ORDER, \
		    NULL)

#   define ADVSMP_DYNHASH_BUCKETLOCK_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSMP_DYNHASH_BUCKETLOCK_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSMP_DYNHASH_BUCKETLOCK_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK( sLk )
#   define ADVSMP_DYNHASH_BUCKETLOCK_OWNED( sLk )   ADVFS_SPIN_OWNED ( sLk )

/*
 * Advfs spin lock definitions
 */

/*
 *    Name:     "Advfs ioanchor lock"
 *    Formerly: ""
 */

#   define ADVSMP_IOANCHOR_INIT( sLk, waitFlag )                     \
        ADVFS_SPIN_INIT( sLk,                                        \
                   "Advfs ioanchor lock",                            \
                   NULL,                                             \
                   ((waitFlag) == TRUE) ? SPIN_WAITOK : SPIN_NOWAIT, \
                   ADVSMP_IOANCHOR_LOCK_ORDER,                       \
                   NULL)

/*
 *    Name:     "Advfs space used lock"
 */

#   define ADVSMP_SPACE_USED_INIT( sLk )                             \
        ADVFS_SPIN_INIT( sLk,                                        \
                   "Advfs space used lock",                          \
                   NULL,                                             \
                   SPIN_WAITOK,                                      \
                   ADVSMP_SPACE_USED_LOCK_ORDER,                     \
                   NULL)

#   define ADVSMP_SPACE_USED_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSPL_SPACE_USED_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSPL_SPACE_USED_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK( sLk )

/*
 *    Name:     "Advfs domain lsn lock"
 *    Formerly: "domainT.lsnLock"
 */

#   define ADVSMP_DOMAINLSN_INIT( sLk )         \
        ADVFS_SPIN_INIT( sLk,                   \
                   "Advfs domain lsn lock",     \
                   NULL,                        \
                   SPIN_WAITOK,                 \
                   ADVSMP_DOMAINLSN_LOCK_ORDER, \
                   NULL)

/*
 *    Name:     "Advfs domain ftx table lock"
 */

#   define ADVSMP_DOMAINFTXTBL_INIT( sLk )         \
        ADVFS_SPIN_INIT( sLk,                            \
                   "Advfs domain ftx table lock",  \
                   NULL,                           \
                   SPIN_WAITOK,                    \
                   ADVSMP_DOMAINFTXTBL_LOCK_ORDER, \
                   NULL)

/*
 *    Name:     "Advfs bfap trace lock"
 */

#   define ADVSMP_BFAP_TRACE_INIT(sLk)           \
        ADVFS_SPIN_INIT(sLk,                           \
                   "Advfs bfap trace lock",      \
	           NULL,                         \
                   SPIN_WAITOK,                  \
                   ADVSMP_BFAP_TRACE_LOCK_ORDER, \
		   NULL)

#   define ADVSMP_BFAP_TRACE_DESTROY(sLk) spin_destroy(sLk) 
/*
 * Name: "Access trace lock"
 */

#   define ADVSMP_ACCESS_TRACE_INIT( sLk )        \
        ADVFS_SPIN_INIT( sLk,                           \
                   "Access trace lock",           \
                   NULL,                          \
                  SPIN_WAITOK,                    \
                  ADVSMP_ACCESS_TRACE_LOCK_ORDER, \
                  NULL)

/*
 *    Name:     "Advfs access mgmt cv lock"
 */

#   define ADVSMP_ACCESS_MGMT_INIT(sLk)            \
        ADVFS_SPIN_INIT( sLk,                            \
                    "Advfs access mgmt cv lock",   \
                    NULL,                          \
                    SPIN_WAITOK,                   \
                    ADVSMP_ACCESS_MGMT_INIT_ORDER, \
		    NULL)

/*
 *    Name:     "Advfs ss volinfo msg lock"
 *    Formerly: "vdT.ssVolInfo.ssVdMsgLk"
 */

#   define ADVSPL_VOLINFO_MSG_INIT( sLk )         \
        ADVFS_SPIN_INIT( sLk,                     \
                   "Advfs ss volinfo msg lock",   \
                   NULL,                          \
                   SPIN_WAITOK,                   \
                   ADVSPL_VOLINFO_MSG_LOCK_ORDER, \
		   NULL)

#   define ADVSPL_VOLINFO_MSG_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#   define ADVSPL_VOLINFO_MSG_LOCK( sLk )    ADVFS_SPIN_LOCK( sLk )
#   define ADVSPL_VOLINFO_MSG_UNLOCK( sLk )  ADVFS_SPIN_UNLOCK( sLk )


/*
 *    Name:     "Advfs bfap active range lock"
 *    Formerly: "bfAccessT.actRangeLock"
 */

#   define ADVSMP_BFAP_RANGE_INIT( sLk )          \
        ADVFS_SPIN_INIT(sLk,                            \
                  "Advfs bfap active range lock", \
                  NULL,                           \
                  SPIN_WAITOK,                    \
                  ADVSMP_BFAP_RANGE_LOCK_ORDER,   \
                  NULL)

/* 
 *    Name:     "Advfs multiwait lock"
 *    Formerly: "rangeFlushT.rangeFlushLock"
 */

#   define ADVSMP_MULTIWAIT_INIT( sLk )         \
        ADVFS_SPIN_INIT( sLk,                         \
                   "Advfs multiwait lock",      \
                   NULL,                        \
                   SPIN_WAITOK,                 \
                   ADVSMP_MULTIWAIT_LOCK_ORDER, \
                   4)

/*
 *    Name:     "Advfs bsBuf lock"
 *    Formerly: "bsBufLock"
 */

#   define ADVSMP_BSBUFLOCK_INIT( sLk )    \
        ADVFS_SPIN_INIT(sLk,                     \
                  "Advfs bsBuf lock",      \
                  NULL,                    \
                  SPIN_WAITOK,             \
                  ADVSMP_BSBUF_LOCK_ORDER, \
                  2)

/*
 *    Name:     "AdvFS log dirty buf list lock"
 *    Formerly: "bfIoLock"
 */

#   define ADVSMP_LOGDIRTYBUF_INIT( sLk )           \
        ADVFS_SPIN_INIT(sLk,                              \
                  "AdvFS log dirty buf list lock",  \
                  NULL,                             \
                  SPIN_WAITOK,                      \
                  ADVSMP_LOGDIRTYBUF_LOCK_ORDER,    \
                  4) /* Don't deadlock with bsBuf lock */

/*
 *    Name:     "Advfs message q lock"
 *    Formerly: "msgQMutex"
 */

#   define ADVSMP_MSGQ_INIT( sLk )          \
        ADVFS_SPIN_INIT( sLk,                     \
                   "Advfs message q lock",  \
                    NULL,                   \
                    SPIN_WAITOK,            \
                    ADVSMP_MSGQ_LOCK_ORDER, \
                    NULL)

#   define ADVSMP_MSGQ(sLk)         spin_lock(sLk)
#   define ADVSMP_MSGQ_UNLOCK(sLk)  spin_unlock(sLk)
#   define ADVSMP_MSGQ_DESTROY(sLk) spin_destroy(sLk)

/*
 *    Name:     "Advfs access control structure lock"
 *    Formerly: New for HP-UX
 */

#   define ADVSMP_ACC_CTRL_INIT( sLk )                    \
        ADVFS_SPIN_INIT( sLk,                             \
                   "Advfs access control structure lock", \
                    NULL,                                 \
                    SPIN_WAITOK,                          \
                    ADVSMP_ACC_CTRL_STRUC_LOCK_ORDER,     \
                    NULL)

/*
 *    Name:     "Advfs GP log lock"
 *    Formerly: New for HP-UX
 */

#   define ADVSMP_GP_LOG_INIT( sLk )          \
        ADVFS_SPIN_INIT( sLk,                 \
                   "Advfs GP log lock",       \
                    NULL,                     \
                    SPIN_WAITOK,              \
                    ADVSMP_GP_LOG_LOCK_ORDER, \
                    NULL) /* 2? */

/*
 *    Name:     "Advfs bfap vfast migrating lock"
 *    Formerly: New for HP-UX
 */

#   define ADVSMP_BFAP_BFA_SS_MIG_LOCK_INIT( sLk )       \
        ADVFS_SPIN_INIT(sLk,                             \
                    "Advfs bfap vfast migrating lock",   \
                    NULL,                                \
                    SPIN_WAITOK,                         \
                    ADVSMP_BFAP_BFA_SS_MIG_LOCK_ORDER,   \
		    NULL )

#define ADVSMP_BFAP_BFA_SS_MIG_LOCK_DESTROY( sLk ) ADVFS_SPIN_DESTROY( sLk )
#define ADVSMP_BFAP_BFA_SS_MIG_LOCK( sLk )         ADVFS_SPIN_LOCK( sLk );
#define ADVSMP_BFAP_BFA_SS_MIG_UNLOCK( sLk )       ADVFS_SPIN_UNLOCK( sLk );

/*
 * Macro to define advfs lock names. Currently this macro just
 * defines strings for locks for which multiple inits occur, so
 * that the name string will have a unique address. This is
 * necessary so that they all look like the same lock type for
 * logging.
 */

#define ADVFS_LOCK_NAME_STRINGS \
char ADVNAM_BFAP_XTNTMAP_LOCK[]=ADVSTR_BFAP_XTNTMAP_LOCK; \
char ADVNAM_DYNHASH_BUCKET_LOCK[]=ADVSTR_DYNHASH_BUCKET_LOCK; \
char ADVNAM_QI_LOCK[]=ADVSTR_QI_LOCK; \
char ADVNAM_FILESET_LOCK[]=ADVSTR_FILESET_LOCK; \
char ADVNAM_BFAP_MCELL_LIST_LOCK[]=ADVSTR_BFAP_MCELL_LIST_LOCK;

/*
 * lkTypeT
 *
 * Lock type.  Enumerates the lock types supported by the lock manager.
 */

typedef enum {
    LKT_INVALID,
    LKT_STATE,          /* state lock */
    LKT_FTXRW,          /* ftx r/w lock */
    LKT_FTXMTX          /* ftx mutex */
} lkTypeT;

/*
 * lkUsageT
 *
 * These are used to associate a notion of the lock's usage with the
 * lock so that when the locks are dumped it is easy to identify the
 * locks.
 *
 * For each lock type there is a comment that indicates the locking order
 * with respect to root transaction starts.  In other words, the comment
 * tells you if the lock can be locked before or after a call to ftx_start()
 * to start a root transaction.
 */

typedef enum {                  /* ##, ftx_start() locking order    */
    LKU_UNKNOWN,                /*  0 */
    LKU_BF_STATE,               /*  3, lock after root ftx_start()  */
    LKU_num_usage_types
} lkUsageT;

/*
 * lkHdrT
 *
 * Common struct header for all locks.
 */

typedef struct lkHdr {
    lkTypeT lkType;

    /*
     * The following are used only for/by ftx locks.
     */
    void *nxtFtxLk;
    spin_t *spin;
    lkUsageT lkUsage;
} lkHdrT;

typedef struct ftxLk {
    lkHdrT hdr;               /* header used by ftx lock routines only */
    union {                   /* The underlying system lock */
        ADVFS_RWLOCK_T rw;    /* R/W lock */
	ADVFS_MUTEX_T  mtx;   /* Mutex */
    } lock;
    cv_t cv;                  /* condition variable */
} ftxLkT;

/*
 * unLkActionT
 *
 * Used to determine whether or not a thread that just unlocked a lock
 * should signal or broadcast to other threads that are waiting on
 * the lock which the thread just unlocked.
 */

typedef enum { 
    UNLK_NEITHER, 
    UNLK_SIGNAL, 
    UNLK_BROADCAST 
} unLkActionT;

typedef enum {
    LKS_UNLOCKED,
    LKS_SHARE,
    LKS_EXCL
} shareExclLkStateT;

/****************************************************************************
 * STATE LOCK SUPPORT
 *
 * State locks provide a general mechanism for changing states and
 * waiting on state changes.
 */

/*
 * lkStatesT
 *
 * Client-defined states for state locks.  We use an enumerated type
 * to make it easier to debug.
 */

typedef enum {

    LKW_NONE,

    /* bfAccessT client states */
    ACC_VALID,          /* represents an accessible bitfile */
    ACC_VALID_EXCLUSIVE,/* Same as ACC_VALID, but used by advfs_access_mgmt_thread
                         * to see if anyone else has touched the bfap.  This
                         * should not be set by anyone other than the 
                         * advfs_acc_mgmt_thread, but can be change to
                         * ACC_VALID whenever encountered. */
    ACC_INVALID,        /* does not represent an accessible bitfile */
    ACC_INIT_TRANS,     /* access struct is being initialized */
    ACC_CREATING,       /* bfap is being created under a transaction. */
    ACC_RECYCLE,        /* access struct is being recycled */
    ACC_DEALLOC         /* access struct is being deallocated */


} lkStatesT;

/*
 * stateLkT
 */

typedef struct stateLk {
    lkHdrT hdr;                 /* header used by ftx lock routines only */
    lkStatesT state;            /* current state */
    lkStatesT pendingState;     /* pending state - ftx lock routines only */
    uint16_t waiters;           /* num threads waiting on the cvp */
    cv_t cv;                    /* condition variable */
} stateLkT;

typedef struct advfsLockUsageStats {
    uint64_t lock;
    uint64_t wait;
    uint64_t reWait;
    uint64_t signal;
    uint64_t broadcast;
} advfsLockUsageStatsT;

typedef struct advfsLockStats {
    uint64_t wait;
    uint64_t signal;
    uint64_t broadcast;
    uint64_t stateLock;
    uint64_t stateWait;
    uint64_t stateReWait;
    uint64_t stateSignal;
    uint64_t stateBroadcast;
    uint64_t msgQWait;
    uint64_t msgQReWait;
    uint64_t msgQSignal;
    uint64_t msgQBroadcast;
    uint64_t ftxSlotWait;
    uint64_t ftxSlotFairWait;
    uint64_t ftxSlotReWait;
    uint64_t ftxSlotSignalFree;
    uint64_t ftxSlotSignalNext;
    uint64_t ftxExcWait;
    uint64_t ftxExcReWait;
    uint64_t ftxExcSignal;
    uint64_t ftxTrimWait;
    uint64_t ftxTrimReWait;
    uint64_t ftxTrimSignal;
    uint64_t ftxTrimBroadcast;
    advfsLockUsageStatsT usageStats[LKU_num_usage_types];
} advfsLockStatsT;

extern advfsLockStatsT *AdvfsLockStats;


#define lk_waiters( lk )        ((lk).waiters)
#define lk_get_state( lk )      ((lk).state)

#define lk_signal( act, lkp ) _lk_signal( act, lkp, __LINE__, NULL )
#define lk_set_state( lkp, state ) \
        _lk_set_state( lkp, state , __LINE__, NULL )
#define lk_wait_for( lkp, mp, state ) \
       _lk_wait_for( lkp, mp, state , __LINE__, NULL )
#define lk_wait_while( lkp, mp, state ) \
       _lk_wait_while( lkp, mp, state , __LINE__, NULL )
#define lk_wait( lkp, mp ) \
       _lk_wait( lkp, mp, __LINE__, NULL )

/*
 ** Prototypes.
 */

void
_lk_signal(
    unLkActionT action,
    void *lk,
    int32_t ln,
    char *fn
    );

void 
trace_hdr( void );

void 
bs_dump_locks(
    int32_t locked
    );

void 
lk_init(
    void *lkp,
    spin_t *spin,
    lkTypeT type,
    int32_t res,
    lkUsageT usage
    );

void 
lk_destroy(
    void *lk
    );


void
_lk_wait_while(
    stateLkT *lk,
    spin_t *lk_spin,
    lkStatesT waitState,
    int32_t ln,
    char *fn
    );

void
_lk_wait(
    stateLkT *lk,
    spin_t *lk_spin,
    int32_t ln,
    char *fn
    );


void
_lk_wait_for(
    stateLkT *lk,
    spin_t *lk_spin,
    lkStatesT waitState,
    int32_t ln,
    char *fn
    );

unLkActionT
_lk_set_state(
    stateLkT *lk,
    lkStatesT newState,
    int32_t ln,
    char *fn
    );

#endif /* _GENERIC_LOCKS_ */

