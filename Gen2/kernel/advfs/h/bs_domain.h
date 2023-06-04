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
 *      Domain descriptor definitions.
 *
 */

#ifndef _DOMAIN_H_
#define _DOMAIN_H_

#include <advfs/ms_generic_locks.h>
#include <advfs/vfast.h>

/*
 * Some ftx types that are part of the domain structure, so must be
 * here instead of ftx_privates, which is included after this file.
 */

/* slot states */
typedef enum {
    FTX_SLOT_EXC = 1,           /* slot is in use by an exclusive ftx */
    FTX_SLOT_BUSY = 2,          /* slot is in use by non exclusive ftx */
    FTX_SLOT_AVAIL = 3          /* slot is available for assignment. */
} ftxSlotStateT;

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
    cv_t slotCv;        /* condition variable for slot waiters */
    cv_t trimCv;        /* condition variable for trim waiters */
    cv_t excCv;         /* condition variable for exc waiters */
    lsnT logTrimLsn;    /* lsn to trim log up to */
    ftxCRLAT oldestFtxLa; /* oldest outstanding ftx logrecaddr */
    ftxIdT lastFtxId;   /* last ftx id used */
    int slotUseCnt;     /* slots in use count */
    int noTrimCnt;      /* reasons not to trim */
    struct ftx* tablep;   /* ftx slot table pointer */
    ftxSlotStateT *tableSltStatep; /* Array of ftx slot states representing */
      	                           /* the respective tablep ftx slot. */
    /* stats */
    int oldestSlot;      /* oldest slot whose log contents is not written */
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
 *      ADVMTX_DOMAIN_LOCK( &dmnp->mutex );
 *      BFSET_DMN_INSQ( dmnp, queue, entry );
 *      ADVMTX_DOMAIN_UNLOCK( &dmnp->mutex );
 *
 *  Insert Tail:
 *      domainT *dmnp;
 *      bfSetT *bfSetp;
 *      bfsQueueT *queue = dmnp->bfSetHead.bfsQbck;
 *      bfsQueueT *entry = &bfSetp->bfSetList;
 *      ...
 *      ADVMTX_DOMAIN_LOCK( &dmnp->mutex );
 *      BFSET_DMN_INSQ( dmnp, queue, entry );
 *      ADVMTX_DOMAIN_UNLOCK( &dmnp->mutex );
 *
 *  Remove Any:
 *      bfSetT *bfSetp;
 *      bfsQueueT *entry = &bfSetp->bfSetList;
 *      ...
 *      ADVMTX_DOMAIN_LOCK( &dmnp->mutex );
 *      BFSET_DMN_REMQ( dmnp, entry );
 *      ADVMTX_DOMAIN_UNLOCK( &dmnp->mutex );
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
 *      ADVMTX_DOMAIN_LOCK( &dmnp->mutex );
 *      while( entry != &dmnp->bfSetHead ) {
 *          bfSetp = BFSET_QUEUE_TO_BFSETP( entry );
 *          ...do not do complex tasks holding the dmnp->mutex...
 *          entry = entry->bfsQfwd;
 *      }
 *      ADVMTX_DOMAIN_UNLOCK( &dmnp->mutex );
 *
 *  Walk Backwards:
 *      domainT *dmnp;
 *      bfSetT *bfSetp;
 *      bfsQueueT *entry = dmnp->bfSetHead.bfsQbck;
 *      ...
 *      ADVMTX_DOMAIN_LOCK( &dmnp->mutex );
 *      while( entry != &dmnp->bfSetHead ) {
 *          bfSetp = BFSET_QUEUE_TO_BFSETP( entry );
 *          ...do not do complex tasks holding the dmnp->mutex...
 *          entry = entry->bfsQbck;
 *      }
 *      ADVMTX_DOMAIN_UNLOCK( &dmnp->mutex );
 */

typedef struct bfsQueue {
    struct bfsQueue *bfsQfwd;
    struct bfsQueue *bfsQbck;
} bfsQueueT;

#define BFSET_DMN_INSQ( _dmnp, _queue, _entry ) {               \
    MS_SMP_ASSERT(ADVMTX_DOMAIN_OWNED(&((_dmnp)->dmnMutex)));     \
    (_entry)->bfsQfwd = (_queue)->bfsQfwd;                      \
    (_entry)->bfsQbck = (_queue);                               \
    (_queue)->bfsQfwd->bfsQbck = (_entry);                      \
    (_queue)->bfsQfwd = (_entry);                               \
}

#define BFSET_DMN_REMQ( _dmnp, _entry ) {                       \
    MS_SMP_ASSERT(ADVMTX_DOMAIN_OWNED(&((_dmnp)->dmnMutex)));     \
    (_entry)->bfsQfwd->bfsQbck = (_entry)->bfsQbck;             \
    (_entry)->bfsQbck->bfsQfwd = (_entry)->bfsQfwd;             \
    (_entry)->bfsQfwd = (_entry);                               \
    (_entry)->bfsQbck = (_entry);                               \
}


#define BFSET_QUEUE_TO_BFSETP( _link ) \
    ((bfSetT *)((char *)(_link) - advfs_offsetof(bfSetT,bfSetList)))

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

/* Domain dmnFlag values */

#define BFD_NORMAL                 (0)
#define BFD_RMVOL_IN_PROGRESS      (1<<0)
#define BFD_DUAL_MOUNT_IN_PROGRESS (1<<1)
#define BFD_CFS_RELOC_IN_PROGRESS  (1<<2)
#define BFD_DEACTIVATE_PENDING     (1<<3)
#define BFD_DEACTIVATE_IN_PROGRESS (1<<4)

/*
 *  Domain activation flags.  Set via the mount path.
 */
#define DMNA_MOUNTING              0x00000001 
#define DMNA_LOCAL_ROOT            0x00000002
#define DMNA_GLOBAL_ROOT           0x00000004 
#define DMNA_FAILOVER              0x00000008 
#define DMNA_CREATING              0x00000010 
#define DMNA_FMOUNT                0x00000020
#define DMNA_UNMOUNT_RQST          0x00000040
#define DMNA_RENAME                0x00000080

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
    vdDescT* vddp[BS_MAX_VDI];  /* array of vd descriptor ptrs */
} bfDmnDescT;

typedef struct domain {
#ifdef _KERNEL
    spin_t ftxTblLock;             /* Protects the ftxTbld */ 
#endif /* _KERNEL */
#ifdef _KERNEL
    spin_t lsnLock;
#endif /* _KERNEL */
    ADVMTX_DOMAIN_T dmnMutex;     /* protects vd.mcell_lk, vd.stgMap_lk, */
                                /* totalBlks, freeBlks */
                                /* vd.ddlActiveWaitMCId */
                                /* bfSetHead, bfSetList */
    uint32_t dmnMagic;          /* magic number: structure validation */

    /* all domainTs are on a circular doubly-linked list. */
    struct domain *dmnFwd;      /* Forward pointer of all domainTs */
    struct domain *dmnBwd;      /* Backward pointer of all domainTs */

#ifdef _KERNEL
    dyn_hashlinks_w_keyT dmnHashlinks;   /* dyn_hashtable links */
#endif /* _KERNEL */

    adv_ondisk_version_t dmnVersion;     /* version number: on-disk validation */
    bfDmnStatesT state;            /* domain state */
    bfDomainIdT domainId;          /* unique identifier for domain */
    bfFsCookieT dmnCookie;         /* domain cookie - must not change */
    bfDomainIdT dualMountId;       /* unique on-disk identifier for domain, */
                                   /* used only when doing dual-mounts */
    bfDomainIdT bfDmnMntId;        /* last domain activation id */
    int dmnAccCnt;                 /* number of domain accesses */
    int dmnRefWaiters;             /* number of waiters on dmnAccCnt */
    cv_t dmnRefWaitersCV;          /* Condition Variable for dmnRefWaiters */
    int activateCnt;               /* number of domain activations */
    int mountCnt;                  /* number of active mounts */
    bfSetT *bfSetDirp;             /* root bitfile-set handle */
    bfTagT bfSetDirTag;            /* tag of root bitfile-set's tag directory */
    ftxLkT BfSetTblLock;           /* protects the filesets in the domain */
    struct bfsQueue bfSetHead;     /* bitfile-sets associated with this 
                                    * domain 
                                    */
    struct bfAccess *bfSetDirAccp; /* bfAccess of bitfile-set's tag directory */
#ifdef _KERNEL
    fcache_as_hdl_t metadataVas;   /* Domain's metadata virtual address space */
    fcache_as_hdl_t userdataVas;   /* Domain's user data virtual address space*/
    bfTagT ftxLogTag;              /* tag of domain ftx log */
    logDescT * ftxLogP;            /* pointer to ftx log for this domain */
#endif /* _KERNEL */
    uint32_t ftxLogPgs;             /* number of pages in the log */
    struct bfAccess *logAccessp;   /* bfAccess pointer for log */
    ftxTblDT ftxTbld;              /* ftx table descriptor */
#   ifdef ADVFS_SMP_ASSERT         /* For debugging only, to check for lock */
    ADVRWL_DOMAIN_FTXSLOT_T ftxSlotLock; /* hierarchy violations */
				      /* between locks    */
#   endif                          /* and starting a root transaction       */
    char domainName[BS_DOMAIN_NAME_SZ]; /* temp - should be global name */
    uint32_t dmnFlag;               /* special domain state indicator */
    uid_t uid;                     /* domain's owner */
    gid_t gid;                     /* domain's group */
    mode_t mode;                   /* domain's mode */

    ADVMTX_METAFLUSH_LOCK_T metaFlushLock; /* Serializes metadata flush calls */

    /*
     * Following fields are protected by the domain's lsnLock; locking the
     * lsnLock also guards the bsBuf.bsb_metafwd and bsBuf.bsb_metabwd 
     * fields of any buffers on this domain's lsnList.
     */
    struct bsBufHdr lsnList; /* Dirty transactional buffers to be written */
    ftxCRLAT dirtyBufLa;     /* oldest dirty buffer log address */

    /* This lock protects the storage class table. */
    mutex_t scLock;
    struct serviceClassTbl *scTbl;        /* service class table */

    /*
     * These fields used to be protected by the DmnTblLock.  That function
     * of the DmnTblLock has been replaced by the domain-wide lock, 
     * vdpTblLock, so eventually the use of DmnTblLock will be able to 
     * be reduced.
     */
    ADVMTX_DOMAINVDPTBL_T    vdpTblLock;   /* protects next 2 fields   */
    int maxVds;                     /* Maximum allowed vds */
    int vdCnt;                      /* number of vd's in vdpTbl */
    struct vd* vdpTbl[BS_MAX_VDI];  /* table of vd ptrs */

    ADVRWL_RMVOL_TRUNC_T rmvolTruncLk; /* serializes truncation and rmvol */

    struct bcStat bcStat;       /* per domain buffer cache stats */
    struct bmtStat bmtStat;     /* per domain BMT stats */
    struct logStat logStat;     /* per domain LOG stats */
    /*
     * These fields are protected by the domain mutex.
     */
    adv_threshold_t dmnThreshold; /* dmn threshold values */
    bf_vd_blk_t totalBlks;      /* total # of DEV_BSIZE blocks in domain */
    bf_vd_blk_t freeBlks;       /* total # of free DEV_BSIZE blocks in dmn */
    bf_fob_t metaAllocSz;       /* Number of fobs in minimum allocation 
                                   unit for metadata */ 
    bf_fob_t userAllocSz;       /* Number of fobs in minimum allocation unit
                                   for user data */
    uint64_t dmnConfigCacheAgeTime;     /* Config time for aging access structure */
    uint64_t dmnSoftClosedAgeTime;      /* Performance adjusted age time */
    uint64_t dmnSoftFreeAgeTime;        /* Memory pressure adjusted age time */
    int dmn_panic;              /* a boolean for implimenting domain panic */
    xidRecoveryT xidRecovery;   /* Holds CFS xid recovery status information */
    ADVRWL_XID_RECOVERY_T xidRecoveryLk; /* protects the xidRecovery structure */
    uint32_t smsync_policy;     /* mirror of M_SMSYNC2 flag */
    struct bsMPg *metaPagep;    /* ptr to page 0 buffer */
    int32_t fs_full_time;       /* last time fs full msg logged */

    ADVMTX_DOMAINFREEZE_T dmnFreezeMutex; /* protects freeze fields (Flags,
				      Waiting, RefCnt */
    uint32_t dmnFreezeFlags;    /* freezefs status */
    uint32_t dmnFreezeWaiting;  /* Freeze thread is waiting to freeze this domain */
    uint32_t dmnFreezeRefCnt;   /* Count of threads blocking the start of a freeze */
    cv_t dmnFreezeWaitingCV;    /* Cond Var for dmnFreezeWaiting */

    ssDmnInfoT ssDmnInfo;       /* vfast elements. */

#ifdef OSDEBUG
    uint32_t crashTest;		/* For testing. Set to one to indicate that
				 * a crash has been triggered for testing
				 * crash/recovery logs.
				 */
#endif

} domainT;

extern domainT* DomainTbl[];

mutex_t   DmnHashTblLock;
ADVRWL_DMNACTIVATION_T DmnActivationLock;

/* Dynamic hash macros */

#define DOMAIN_HASH_INITIAL_SIZE              32
#define DOMAIN_HASH_CHAIN_LENGTH              16
#define DOMAIN_HASH_ELEMENTS_TO_BUCKETS        4
#define DOMAIN_HASH_USECS_BETWEEN_SPLITS 5000000 /* 5 seconds */

#define DOMAIN_GET_HASH_KEY( _domainId ) \
    ( (_domainId).id_sec)

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

int
advfs_volume_mounted(
    char* volume_name
    );
    
void
free_vdds(
    bfDmnDescT* dmnDescp         /* in - ptr to bfdmn desc tbl */
    );

statusT
set_recovery_failed(
    domainT *domain,            /* in */
    uint16_t value              /* in */
    );

statusT
bs_vd_add_rem_vol_done(
    domainT *dmnP,  /* in */
    char *volName,  /* in */
    bsVdOpT op      /* in */
    );

domainT *
domain_name_lookup(
    char *dmnName,     /* in - domain to lookup */
    uint64_t flag      /* in - mount flags/check DmnTblLock? */
);

int
advfs_is_domain_name_mounted(
    char* dmnName      /* in - domain to lookup */
);

int
advfs_is_domain_id_mounted(
    bfDomainIdT bfDomainId     /* in - domain to lookup */
);

void
bs_domain_init(
    void
);

statusT
advfs_enum_domain_devts(
    char *domain,                 /* in */
    bsDevtListT *DevtList         /* out */
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
    struct vd **delVd,            /* out */
    bfMCIdT *delMcellId,          /* out */
    int *delFlag                  /* out */
);

statusT
bs_fix_root_dmn (
    bfDmnDescT *dmntbl,           /* in */
    domainT *dmnP,                /* in */
    char *dmnName,                /* in */
    char *topDmnDir               /* in */
);

statusT
bs_check_root_dmn_sc (
    domainT *dmnP                 /* in */
);

statusT
get_domain_disks(
    char* domain,                /* in - domain name */
    uint64_t doingRoot,          /* in - flag */
    bfDmnDescT* dmnDescp,        /* out - ptr to bfdmn desc */
    char** dmnDir                /* out - if !NULL, return top level dmn dir */
);

#endif /* _DOMAIN_H_ */
