/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
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
 * @(#)$RCSfile: bs_domain.h,v $ $Revision: 1.1.148.2 $ (DEC) $Date: 2004/12/23 14:24:29 $
 */

#ifndef _DOMAIN_H_
#define _DOMAIN_H_

#ifndef offsetof
#include <stddef.h>
#endif

#include <msfs/vfast.h>
#include <kern/e_dyn_hash.h>

/*
 * Some ftx types that are part of the domain structure, so must be
 * here instead of ftx_privates, which is included after this file.
 */

/* slot states */
typedef enum {
    FTX_SLOT_EXC = 1,           /* slot is in use by an exclusive ftx */
    FTX_SLOT_BUSY = 2,          /* slot is in use by non exclusive ftx */
    FTX_SLOT_AVAIL = 3,
    FTX_SLOT_PENDING = 4,       /* waiting for an ftx struct */
    FTX_SLOT_UNUSED = 5         /* never been used (recovery may use it) */
} ftxSlotStateT;

typedef struct ftxSlot {
    struct ftx* ftxp;     /* ptr to ftx struct */
    ftxSlotStateT state;  /* slot state */
} ftxSlotT;

#define FTX_MAX_FTXH     100
#define FTX_RR_GAP       3
#define FTX_DEF_RR_SLOTS 30      /* default round robin slots */
                                 /* If FTX_DEF_RR_SLOTS changes; be sure to
                                  * change the value of MAX_RD_PGS in
                                  * ms_logger.h since that value is dependent
                                  * on this one.
                                  */

/*
 * ftxTblD - Transaction Table Descriptor
 *
 * The transaction table contains the transaction descriptors
 * for the associated domain (this structure is a field in the domain
 * structure).  During normal system operation (not during recovery),
 * the transaction descriptors are allocated to new root transactions
 * in a round-robin fashion.  However, the allocation policy is done
 * in strict order so if there are are 4 entries for example, they
 * get allocated as follows:  0 1 2 3 0 1 2 3 0 1 ....  This means
 * that it is not possible to allocate an entry if it still busy and
 * it means that an entry cannot be skipped.  So, if we allocated 0 1 2 3
 * and 1 2 3 all finished and were deallocated (deallocation order is
 * not controlled) we must wait for 0 to complete before any new
 * transactions are started.  There is condition variable (cv) in the
 * table which is used for this purpose.  This strict allocation policy
 * is what limits the time between the oldest and newest transaction
 * which helps to prevent log full errors.
 */

typedef struct ftxTblD {
    int rrNextSlot;     /* next round-robin slot to use */
    int rrSlots;        /* number of round-robin slots to use */
    int ftxWaiters;     /* num threads waiting on slot */
    int trimWaiters;    /* num threads waiting for log trim to finish */
    int excWaiters;     /* num threads waiting to start exclusive ftx */
    cvT slotCv;         /* condition variable for slot waiters */
    cvT trimCv;         /* condition variable for trim waiters */
    cvT excCv;          /* condition variable for exc waiters */
    lsnT logTrimLsn;    /* lsn to trim log up to */
    int nextNewSlot;    /* next never-used slot */
    ftxCRLAT oldestFtxLa; /* oldest outstanding ftx logrecaddr */
    ftxIdT lastFtxId;   /* last ftx id used */
    int slotUseCnt;     /* slots in use count */
    int noTrimCnt;      /* reasons not to trim */
    ftxSlotT* tablep;   /* ftx slot table pointer */

    /* stats */
    int oldestSlot;      /* oldest slot whose log contents is not written */
    int totRoots;        /* num root ftxs */
} ftxTblDT;

/*
 * Link list queuing structures and MACROs
 *
 *  An empty list is characterized as:
 *
 *                 domainT:  ____________________________
 *                           \                           \
 *                           / ######################### /
 *                           \                           \
 *                           /                           /
 *                           \                           \
 *                           /                           /
 *                           \                           \
 *            bfsQueueT:     /___________________________/
 *                 *bfsQfwd: | &domainT.bfSetHead        |<<--+
 *                           |___________________________|-->>|
 *                 *bfsQbck: | &domainT.bfSetHead        |-->>+
 *                           |___________________________|
 *                           \                           \
 *                           /                           /
 *                           \                           \
 *                           /___________________________/
 *
 *      Initialize:   dmnp->bfSetHead.bfsQfwd = &dmnp->bfSetHead;
 *                    dmnp->bfSetHead.bfsQbck = &dmnp->bfSetHead;
 *
 *
 *  A chain with entries in it is characterized as:
 *
 *                 domainT:  ____________________________
 *                           \                           \
 *                           / ######################### /
 *                           \                           \
 *                           /                           /
 *                           \                           \
 *                           /                           /
 *                           \                           \
 *             bfsQueueT:    /___________________________/
 *              -->>*bfsQfwd:|&bfSetT_1.bfSetList.bfsQfwd|<<--------
 *             /             |___________________________|---->>    \
 *            /     *bfsQbck:|&bfSetT_3.bfSetList.bfsQfwd|      \    \
 *       <<---|--------------|___________________________|       \   |
 *      /     |              \                           \       |   |
 *     /      |              /                           /       |   |
 *     |      |              \                           \       |   |
 *     |      |              /___________________________/       |   |
 *     |      |                                                  |   |
 *     |      |    bfSetT_1: ____________________________        |   |
 *     |      |              \                           \       |   |
 *     |      |              /                           /       |   |
 *     |      |              \                           \       /   |
 *     |      |bfsQueueT:    /___________________________/      /    |
 *     |    --|--->>*bfsQfwd:|&bfSetT_2.bfSetList.bfsQfwd|<<----     |
 *     |   /  \              |___________________________|---->>     |
 *     |  /    \    *bfsQbck:|&domainT.bfSetList.bfsQfwd |      \    |
 *     |  |     <<-----------|___________________________|       \   |
 *     |  |                  \                           \       |   |
 *     |  |                  /                           /       |   |
 *     |  |                  \                           \       |   |
 *     |  |                  /___________________________/       |   |
 *     |  |                                                      |   |
 *     |  |        bfSetT_2: ____________________________        |   |
 *     |  |                  \                           \       |   |
 *     |  |                  /                           /       |   |
 *     |  |                  \                           \       /   |
 *     |  |   bfsQueueT:     /___________________________/      /    |
 *     |  |     -->>*bfsQfwd:|&bfSetT_3.bfSetList.bfsQfwd|<<----     |
 *     |  \    /             |___________________________|---->>     |
 *     |   \  /     *bfsQbck:|&bfSetT_1.bfSetList.bfsQfwd|      \    |
 *     |    <<|--------------|___________________________|       \   |
 *     |      |              \                           \       |   |
 *     |      |              /                           /       |   |
 *     |      |              \                           \       |   |
 *     |      |              /___________________________/       |   |
 *     |      |                                                  |   |
 *     |      |    bfSetT_3: ____________________________        |   |
 *     |      |              \                           \       |   |
 *     |      |              /                           /       |   |
 *     \      |              \                           \       /   |
 *      \     bfsQueueT:     /___________________________/      /    /
 *       -----|--->>*bfsQfwd:|&domainT.bfSetList.bfsQfwd |<<----    /
 *            \              |___________________________|-------->>
 *             \    *bfsQbck:|&bfSetT_2.bfSetList.bfsQfwd|
 *              <<-----------|___________________________|
 *                           \                           \
 *                           /                           /
 *                           \                           \
 *                           /___________________________/
 *
 *  Insert Head:
 *      domainT *dmnp;
 *      bfSetT *bfSetp;
 *      bfsQueueT *queue = &dmnp->bfSetHead;
 *      bfsQueueT *entry = &bfSetp->bfSetList;
 *      ...
 *      mutex_lock( &dmnp->mutex );
 *      BFSET_DMN_INSQ( dmnp, queue, entry );
 *      mutex_unlock( &dmnp->mutex );
 *
 *  Insert Tail:
 *      domainT *dmnp;
 *      bfSetT *bfSetp;
 *      bfsQueueT *queue = dmnp->bfSetHead.bfsQbck;
 *      bfsQueueT *entry = &bfSetp->bfSetList;
 *      ...
 *      mutex_lock( &dmnp->mutex );
 *      BFSET_DMN_INSQ( dmnp, queue, entry );
 *      mutex_unlock( &dmnp->mutex );
 *
 *  Remove Any:
 *      bfSetT *bfSetp;
 *      bfsQueueT *entry = &bfSetp->bfSetList;
 *      ...
 *      mutex_lock( &dmnp->mutex );
 *      BFSET_DMN_REMQ( dmnp, entry );
 *      mutex_unlock( &dmnp->mutex );
 *
 *      Note:  The design of this queue and the BFSET_DMN_REMQ() MACRO is
 *             such that if an entry that has already been removed from the
 *             queue is passed to BFSET_DMN_REMQ(), the results will be
 *             harmless (a noop).  This feature is taken advantage of by the
 *             bs_bfs_remove_all() and the bfs_invalidate() routines.
 *
 *  Walk Forward:
 *      domainT *dmnp;
 *      bfSetT *bfSetp;
 *      bfsQueueT *entry = dmnp->bfSetHead.bfsQfwd;
 *      ...
 *      mutex_lock( &dmnp->mutex );
 *      while( entry != &dmnp->bfSetHead ) {
 *          bfSetp = BFSET_QUEUE_TO_BFSETP( entry );
 *          ...do not do complex tasks holding the dmnp->mutex...
 *          entry = entry->bfsQfwd;
 *      }
 *      mutex_unlock( &dmnp->mutex );
 *
 *  Walk Backwards:
 *      domainT *dmnp;
 *      bfSetT *bfSetp;
 *      bfsQueueT *entry = dmnp->bfSetHead.bfsQbck;
 *      ...
 *      mutex_lock( &dmnp->mutex );
 *      while( entry != &dmnp->bfSetHead ) {
 *          bfSetp = BFSET_QUEUE_TO_BFSETP( entry );
 *          ...do not do complex tasks holding the dmnp->mutex...
 *          entry = entry->bfsQbck;
 *      }
 *      mutex_unlock( &dmnp->mutex );
 */

typedef struct bfsQueue {
    struct bfsQueue *bfsQfwd;
    struct bfsQueue *bfsQbck;
} bfsQueueT;

#define BFSET_DMN_INSQ( _dmnp, _queue, _entry ) {               \
    MS_SMP_ASSERT(SLOCK_HOLDER(&((_dmnp)->mutex.mutex)));       \
    (_entry)->bfsQfwd = (_queue)->bfsQfwd;                      \
    (_entry)->bfsQbck = (_queue);                               \
    (_queue)->bfsQfwd->bfsQbck = (_entry);                      \
    (_queue)->bfsQfwd = (_entry);                               \
}

#define BFSET_DMN_REMQ( _dmnp, _entry ) {                       \
    MS_SMP_ASSERT(SLOCK_HOLDER(&((_dmnp)->mutex.mutex)));       \
    (_entry)->bfsQfwd->bfsQbck = (_entry)->bfsQbck;             \
    (_entry)->bfsQbck->bfsQfwd = (_entry)->bfsQfwd;             \
    (_entry)->bfsQfwd = (_entry);                               \
    (_entry)->bfsQbck = (_entry);                               \
}

#define BFSET_QUEUE_TO_BFSETP( _link ) \
    ((bfSetT *)((char *)(_link) - offsetof(bfSetT,bfSetList)))


#ifdef ADVFS_DOMAIN_TRACE

#define DOMAIN_TRACE_HISTORY 100

typedef struct {
  uint32T       seq;
  uint16T       mod;
  uint16T       ln;
  struct thread *thd;
  void          *val;
} dmnTraceElmtT;

#endif /* ADVFS_DOMAIN_TRACE */

typedef enum {
    BFD_UNKNOWN,                /* domain is in unknown state */
    BFD_VIRGIN,                 /* domain has never been activated */
    BFD_DEACTIVATED,            /* domain is deactivated */
    BFD_RECOVER_REDO,           /* recovering redo */
/* logging can occur in the following states */
    BFD_RECOVER_FTX,            /* recover partial ftx trees */
    BFD_RECOVER_CONTINUATIONS,  /* finish ftx continuations */
    BFD_ACTIVATED               /* fully activated */
} bfDmnStatesT;

enum contBits { PB_CONT=1, CK_WAITQ=2 };

/* Domain dmnFlag values */


#define BFD_NORMAL                 (0)
#define BFD_RMVOL_IN_PROGRESS      (1<<0)
#define BFD_DUAL_MOUNT_IN_PROGRESS (1<<1)
#define BFD_CFS_RELOC_IN_PROGRESS  (1<<2)
#define BFD_DEACTIVATE_PENDING     (1<<3)
#define BFD_DEACTIVATE_IN_PROGRESS (1<<4)
#define BFD_DOMAIN_FLUSH_IN_PROGRESS (1<<5)

/*
 *  Freeze flags
 */
#define BFD_NO_FREEZE              (0)
#define BFD_FREEZE_IN_PROGRESS     (1<<0)
#define BFD_FROZEN                 (1<<1)

/*
 * domain - this structure describes a bitfile domain, which is
 * mostly a pointer table of all the virtual disks that belong to it.
 */

/* Test the validity of a domain pointer. */
/* "Return" EBAD_DOMAIN_POINTER if the domain is NULL. */
/* "Return" EBAD_DOMAIN_POINTER if the domain structure is corrupt. */
/* Else "return" EOK */
#define TEST_DMNP(_dmnP)                                    \
(                                                           \
        (_dmnP) == NULL ?                                   \
            EBAD_DOMAIN_POINTER :                           \
            (_dmnP)->dmnMagic != DMNMAGIC ?                 \
                EBAD_DOMAIN_POINTER :                       \
                EOK                                         \
)

struct bfAccess;

/*
 * Virtual disk descriptor, filled in from a bf domain record.
 */

typedef struct vdDesc {
    char vdName[MAXPATHLEN+1];  /* block special device name */
    serviceClassT  serviceClass;/* service class */
    dev_t device;               /* dev_t for raw device */
} vdDescT;

/*
 * Bitfile domain descriptor, filled in from bf domain table.
 */

typedef struct bfDmnDesc {
    int vdCount;                /* number of vds described */
    uint32T dmnMajor;           /* fake major number for domain */
    vdDescT* vddp[BS_MAX_VDI];  /* array of vd descriptor ptrs */
} bfDmnDescT;

typedef struct domain {
    mutexT mutex;               /* protects vd.mcell_lk, vd.stgMap_lk, */
                                /* totalBlks, freeBlks */
                                /* vd.ddlActiveWaitMCId */
                                /* bfSetHead, bfSetList */
    uint_t dmnMagic;            /* magic number: structure validation */

    int logVdRadId;             /* the RAD the the log's vd structure */
                                /* lives on. */

    /* all domainTs are on a circular doubly-linked list. */
    struct domain *dmnFwd;       /* Forward pointer of all domainTs */
    struct domain *dmnBwd;      /* Backward pointer of all domainTs */

#ifdef KERNEL
    dyn_hashlinks_w_keyT dmnHashlinks;   /* dyn_hashtable links */
#endif /* KERNEL */

    int dmnVersion;             /* version number: on-disk validation */
    bfDmnStatesT state;         /* domain state */
    bfDomainIdT domainId;       /* unique identifier for domain */
    bfDomainIdT dualMountId;    /* unique on-disk identifier for domain, */
                                /* used only when doing dual-mounts */
    bsIdT bfDmnMntId;           /* last domain activation id */
    int dmnAccCnt;              /* number of domain accesses */
    int dmnRefWaiters;          /* number of waiters on dmnAccCnt */
    int activateCnt;            /* number of domain activations */
    int mountCnt;               /* number of active mounts */
    bfSetT *bfSetDirp;          /* root bitfile-set handle */
    bfTagT bfSetDirTag;         /* tag of root bitfile-set's tag directory */
    ftxLkT BfSetTblLock;        /* protects the filesets in the domain */
    struct bfsQueue bfSetHead;  /* bitfile-sets associated with this domain */
    struct bfAccess *bfSetDirAccp; /* bfAccess of bitfile-set's tag directory */
    bfTagT ftxLogTag;           /* tag of domain ftx log */
    logDescT * ftxLogP;         /* pointer to ftx log for this domain */
    uint32T ftxLogPgs;          /* number of pages in the log */
    struct bfAccess *logAccessp; /* bfAccess pointer for log */
    ftxTblDT ftxTbld;           /* ftx table descriptor */
#ifdef ADVFS_SMP_ASSERT          /* For debugging only, to check for lock */
    lock_data_t ftxSlotLock;     /* hierarchy violations between locks    */
#endif                           /* and starting a root transaction       */
    struct bsBuf *pinBlockBuf;  /* the current pin block buffer, if any */
    char domainName[BS_DOMAIN_NAME_SZ]; /* temp - should be global name */
    uint32T majorNum;           /* domain's device major number (fabricated) */
    uint32T dmnFlag;           /* special domain state indicator */
    uid_t uid;                  /* domain's owner */
    gid_t gid;                  /* domain's group */
    mode_t mode;                /* domain's mode */

    /*
     * Following fields are protected by the domain's lsnLock; seizing
     * lsnLock also guards bsBuf.lsnFwd and .lsnBwd fields of any buffers
     * on this domain's lsnList.
     */
    mutexT lsnLock;
    struct bsBufHdr lsnList; /* Dirty transactional buffers to be written */
    lsnT writeToLsn;         /* pin block until up to this lsn is written */
    uint16T pinBlockWait;
    cvT pinBlockCv;
    int pinBlockRunning;     /* boolean; TRUE if lsn_io_list is running */
    enum contBits contBits;  /* check if log flush or pinblock cont needed */
    int lsnListFlushing;     /* boolean: TRUE if bs_lsnList_flush running */
    ftxCRLAT dirtyBufLa;     /* oldest dirty buffer log address */

    /* This lock protects the storage class table. */
    lock_data_t scLock;
    serviceClassTblT *scTbl;        /* service class table */

    /*
     * These fields used to be protected by the DmnTblLock.  That function
     * of the DmnTblLock has been replaced by the domain-wide lock, 
     * vdpTblLock, so eventually the use of DmnTblLock will be able to 
     * be reduced.
     */
    mutexT     vdpTblLock;          /* protects next 2 fields   */
    int maxVds;                     /* Maximum allowed vds */
    int vdCnt;                      /* number of vd's in vdpTbl */
    struct vd* vdpTbl[BS_MAX_VDI];  /* table of vd ptrs */

    lock_data_t rmvolTruncLk;   /* serializes truncation and rmvol */

    struct bcStat bcStat;       /* per domain buffer cache stats */
    struct bmtStat bmtStat;     /* per domain BMT stats */
    struct logStat logStat;     /* per domain LOG stats */
    /*
     * These fields are protected by the domain mutex.
     */
    uint64T totalBlks;          /* total # of BS_BLKSIZE blocks in domain */
    uint64T freeBlks;           /* total # of free BS_BLKSIZE blocks in dmn */
    int dmn_panic;              /* a boolean for implimenting domain panic */
    xidRecoveryT xidRecovery;   /* Holds CFS xid recovery status information */
    lock_data_t xidRecoveryLk;  /* protects the xidRecovery structure */
    u_int smsync_policy;        /* mirror of M_SMSYNC2 flag */
    struct bsMPg *metaPagep;    /* ptr to page 0 buffer */
    int fs_full_time;           /* last time fs full msg logged */

    mutexT  dmnFreezeMutex;     /* protects dmnFreezeFlags, dmnFreezeWaiting and dmnFreezeRefCnt */
    uint32T dmnFreezeFlags;     /* freezefs status */
    uint32T dmnFreezeWaiting;   /* Freeze thread is waiting to freeze this domain */
    uint32T dmnFreezeRefCnt;    /* Count of threads currently blocking the start of a freeze */

    ssDmnInfoT ssDmnInfo;       /* vfast elements. */
/*>>>>>>> Maintain as last elements of domain structure <<<<<<<<*/ 
#ifdef ADVFS_DOMAIN_TRACE
    uint32T trace_ptr;
    dmnTraceElmtT trace_buf[DOMAIN_TRACE_HISTORY];
#endif /* ADVFS_DOMAIN_TRACE */
} domainT;

extern domainT nilDomain;

extern int DomainCnt;
extern domainT* DomainTbl[];

#ifdef ADVFS_DOMAIN_TRACE

#define DOMAIN_TRACE( dmnp, n1 ) \
    domain_trace((dmnp), (uint16T)ADVFS_MODULE, (uint16T)__LINE__, (void*)(n1))

void
domain_trace( domainT *dmnp,
              uint16T module,
              uint16T line,
              void    *value);

#else  /* ADVFS_DOMAIN_TRACE */

#define DOMAIN_TRACE( dmnp, n1 )

#endif /* ADVFS_DOMAIN_TRACE */

/*
 * Macros for the xidRecoveryLk
 */

#define XID_RECOVERY_LOCK_WRITE( dmnp ) \
    lock_write( &(dmnp->xidRecoveryLk) )

#define XID_RECOVERY_LOCK_TRY_WRITE( dmnp ) \
    lock_try_write( &(dmnp->xidRecoveryLk) )

#define XID_RECOVERY_LOCK_READ( dmnp ) \
    lock_read( &(dmnp->xidRecoveryLk) )

#define XID_RECOVERY_UNLOCK( dmnp ) \
    lock_done( &(dmnp->xidRecoveryLk) )

#define XID_RECOVERY_LOCK_INIT(dmnp) \
    lock_setup( &(dmnp)->xidRecoveryLk, ADVdomainT_xidRecoveryLk_info, TRUE)

#define XID_RECOVERY_LOCK_DESTROY(dmnp) \
    lock_terminate( &(dmnp)->xidRecoveryLk )

/*
 * Macros for the rmvol-truncate lock
 */
#define RMVOL_TRUNC_LOCK_WRITE(dmnp) \
    lock_write( &(dmnp)->rmvolTruncLk )

#define RMVOL_TRUNC_LOCK_READ(dmnp) \
    lock_read( &(dmnp)->rmvolTruncLk )

#define RMVOL_TRUNC_UNLOCK(dmnp) \
    lock_done( &(dmnp)->rmvolTruncLk )

#define RMVOL_TRUNC_LOCK_INIT(dmnp) \
    lock_setup( &(dmnp)->rmvolTruncLk, ADVdomainT_rmvolTruncLk_info, TRUE )

#define RMVOL_TRUNC_LOCK_DESTROY(dmnp) \
    lock_terminate( &(dmnp)->rmvolTruncLk )

/*
 * Macros for the service class table lock.
 */
#define SC_TBL_LOCK(dmnp) \
    lock_write( &(dmnp)->scLock );

#define SC_TBL_UNLOCK(dmnp) \
    lock_done( &(dmnp)->scLock );

/* Lock initialization for scLock. */
#define SC_TBL_LOCK_INIT(dmnp) \
    lock_setup( &(dmnp)->scLock, ADVdomainT_scLock_info, TRUE );

/* Lock destruction for scLock. */
#define SC_TBL_LOCK_DESTROY(dmnp) \
    lock_terminate( &(dmnp)->scLock );

/* Macros for BfSetTblLock  */

#define BFSETTBL_LOCK_WRITE(dmnp) \
    lock_write( &(dmnp)->BfSetTblLock.lock );

#define BFSETTBL_LOCK_READ(dmnp) \
    lock_read( &(dmnp)->BfSetTblLock.lock );

#define BFSETTBL_UNLOCK(dmnp) \
    lock_done( &(dmnp)->BfSetTblLock.lock );

#define BFSETTBL_LOCK_DESTROY(dmnp) \
    lock_terminate( &(dmnp)->BfSetTblLock.lock );

/* Macros for DmnTblLock */

#define DMNTBL_LOCK_WRITE( sLk )  lock_write( sLk );

#define DMNTBL_LOCK_READ( sLk )  lock_read( sLk );

#define DMNTBL_UNLOCK( sLk )  lock_done( sLk );

extern lock_data_t DmnTblLock;

/* Dynamic hash macros */

#define DOMAIN_HASH_INITIAL_SIZE 32
#define DOMAIN_HASH_CHAIN_LENGTH 16
#define DOMAIN_HASH_ELEMENTS_TO_BUCKETS 4
#define DOMAIN_HASH_USECS_BETWEEN_SPLITS 5000000 /* 5 seconds */

#define DOMAIN_GET_HASH_KEY( _domainId ) \
    ( (_domainId).tv_sec)

#define DOMAIN_HASH_LOCK( _key, _cnt ) \
    ( (domainT *)dyn_hash_obtain_chain( DomainHashTbl, _key, _cnt ) )

#define DOMAIN_HASH_UNLOCK( _key ) \
    (void)dyn_hash_release_chain( DomainHashTbl, _key )

#define DOMAIN_HASH_REMOVE( _dmnP, _lock_action ) \
    (void)dyn_hash_remove( DomainHashTbl, _dmnP, _lock_action )

#define DOMAIN_HASH_INSERT( _dmnP, _lock_action ) \
    (void)dyn_hash_insert( DomainHashTbl, _dmnP, _lock_action )


/*****************************************************************
*******************  function prototypes *************************
*****************************************************************/
domainT *
domain_name_lookup(
                   char *dmnName,   /* in - domain to lookup */
                   u_long flag      /* in - mount flags/check DmnTblLock? */
                   );
void
bs_domain_init(
    void
    );

void
bs_bfdmn_flush_bfrs(
                    domainT* dmnp
                    );

void
bs_bfdmn_flush(
               domainT* dmnp
               );

int
bs_bfdmn_flush_sync(
                    domainT* dmnp
                    );

statusT
advfs_enum_domain_devts(
                  char *domain,               /* in */
                  bsDevtListT *DevtList       /* out */
                 );

void
cfs_xid_free_memory(
                    char *arg
                   );
void
cfs_xid_free_memory_int(
                    domainT *dmnp
                   );

statusT
find_del_entry (
                domainT *domain,              /* in */
                bfTagT bfSetTag,              /* in */
                bfTagT bfTag,                 /* in */
                struct vd **delVd,                  /* out */
                bfMCIdT *delMcellId,          /* out */
                int *delFlag                  /* out */
                );

statusT
bs_fix_root_dmn (
                 bfDmnDescT *dmntbl,          /* in */
                 domainT *dmnP,               /* in */
                 char *dmnName                /* in */
                 );

statusT
bs_check_root_dmn_sc (
                 domainT *dmnP                /* in */
                 );

#endif /* _DOMAIN_H_ */
