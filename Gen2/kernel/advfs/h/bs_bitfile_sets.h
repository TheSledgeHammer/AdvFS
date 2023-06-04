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
 *      Bitfile-sets function prototypes.
 *
 */
/*
 * (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993
 */

#ifndef _BS_BITFILE_SETS_
#define _BS_BITFILE_SETS_

#include <advfs/e_dyn_hash.h>
#include <advfs/ms_osf.h>

struct domain;



/****************************************************************************
 * bf set mgt
 ****************************************************************************/

#ifdef ADVFS_BFSET_TRACE
#   define BFSET_TRACE_HISTORY 100

    typedef struct {
        uint32_t        seq;
        uint16_t        mod;
        uint16_t        ln;
#ifdef _KERNEL
        struct kthread *thd;
#endif /* _KERNEL */
        void           *val;
    } bfsetTraceElmtT;

#endif /* ADVFS_BFSET_TRACE */

typedef enum {
    /* bitfile set states in bfSetT */
    BFS_INVALID,
    BFS_READY,          /* not special happening with the set */
    BFS_BUSY,           /* something wants to prevent delete */
    BFS_DELETING        /* set is being deleted */
} bfsStateT;

/*
 * bfSetT - Bitfile-set descriptor (in-memory)
 */

#ifdef _KERNEL
#if ! defined(__BFSETT__)
    /*
     * bs_public.h also has a bfSetT typedef, so if bs_public.h happens
     * first, this typedef will be skipped.  If bs_public.h does not happen,
     * then we will declare the typedef here.  This eliminates compiler
     * errors.
     */
    typedef struct bfSet bfSetT;
#define __BFSETT__


#endif /* __BFSETT__ */

/* NOTE WELL
 *
 * The SETACCESSCHAIN and BFSET locks must be carefully positioned in
 * this structure for alignment of the locks.
 */

struct bfSet {
    dyn_hashlinks_w_keyT hashlinks; /* dyn_hashtable links */
    uint32_t bfSetMagic;            /* magic number: structure validation */
    int32_t fsRefCnt;               /* number of bfs_access() accessors */

    /* The above three fields are 32 bytes long in total, aligning */
    /* The next field on a 32 byte boundary. */

    ADVSMP_BFSET_T bfSetMutex; /* protect bfSetFlags */
    ADVSMP_BFSET_T ssTagMutex; /* protects ssTagArray - located here for pa alignment */
    bfs_flags_t bfSetFlags;    /* The high-order 16-bits of this field holds */
                               /* in-memory attributes and the low-order */
                               /* 16-bits holds on-disk flags */
    char bfSetName[BS_SET_NAME_SZ]; /* bitfile-set's name */

    /* Snapshot related fields */
    /* bfsSnapCv, bfsSnapLevel, and bfsSnapRefs are synchronized with 
     * the setMutex where as the other links
     * are protected by the bfSetTbl lock. 
     */
    uint32_t    bfsSnapLevel;           /* 0 = root. Greater than 0 indicates 
                                         * number of parents to root. */
    bfSetT      *bfsParentSnapSet;      /* Parent snapset.  Null if not open */
    bfSetT      *bfsFirstChildSnapSet;  /* First child snap set in snap stree */
    bfSetT      *bfsNextSiblingSnapSet; /* Next snapset on the smae level */
    uint32_t    bfsSnapRefs;            /* Number of refs from other related
                                         * snapsets */
    uint32_t    bfsNumSnapChildren;     /* Number of snapshot children */
    /* End snapshots realted fields */

    /* At this point we are at 104 on PA, 112 on IA */

    bfAccessT *dirBfAp;
    bfAccessT *accessFwd;           /* list of access structures */
    bfAccessT *accessBwd;

    /* PA 128, IA 136 */

    ADVSMP_SETACCESSCHAIN_T  accessChainLock;
				    /* protects the previous two fields */
    bfSetIdT bfSetId;               /* bitfile-set's ID */
    domainT *dmnP;                  /* pointer to BF-set's domain structure */
    bfsQueueT bfSetList;            /* list of bfSetT's in this domain */
    dev_t bfs_dev;                  /* set's dev_t; used for statfs()
				       and stat() */
    bfTagT dirTag;                  /* tag of bitfile-set's tag directory */

    cv_t        bfsSnapCv;          /* Used to synchronize IO with*/
                                    /* making snapset */

    uint32_t infoLoaded;    /* true if correct tagdir info has been loaded */
    mutexT setMutex;        /* protects dirLock & fragLock lock header fields */
    ftxLkT dirLock;         /* tag dir lock */
    bfsStateT state;        /* state */

    /* tagdir info - valid iff infoLoaded == TRUE */
    int32_t bfCnt;             /* number of bitfiles in the bitfile set */
    bs_meta_page_t tagFrLst;   /* page no of head of free list + 1 */
    bs_meta_page_t tagUnInPg;  /* first uninitialized page in tag dir */
    bs_meta_page_t tagUnMpPg;  /* first unmapped page in tag dir */

    adv_threshold_t fsetThreshold;  /* holds bfset threshold information */
    
    fileSetNodeT *fsnp;        /* file set node pointer */

    /* used by autotune to implement defrag_all */
    uint32_t      ssStartTagPg;
    uint32_t      ssTagArraySz;
    ssTagArrayT  *ssTagArray;  /* protected by ssTagMutex */
#ifdef ADVFS_BFSET_TRACE
    uint32_t trace_ptr;
    bfsetTraceElmtT trace_buf[BFSET_TRACE_HISTORY];
#endif /* ADVFS_BFSET_TRACE */
};
#endif /* _KERNEL */

/* Macro to set the filesystem id into an fsid_t */

#define BS_GET_FSID(_setp,_fsid)    \
    {                               \
        _fsid[0] = _setp->bfs_dev;  \
        _fsid[1] = advfs_fstype;    \
    }


#ifdef _KERNEL
/*
 * MACROs for working with the BfSetHashTbl
 */
#define BFSET_HASH_INITIAL_SIZE 32
#define BFSET_HASH_CHAIN_LENGTH 16
#define BFSET_HASH_ELEMENTS_TO_BUCKETS 4
#define BFSET_HASH_USECS_BETWEEN_SPLITS 1000000 /* 1 second */

#define BFSET_GET_HASH_KEY( _bfSetId ) \
    ( (_bfSetId).domainId.id_sec + (_bfSetId).dirTag.tag_num )

#define BFSET_HASH_LOCK( _key, _cnt ) \
    ( (bfSetT *)dyn_hash_obtain_chain( BfSetHashTbl, _key, _cnt ) )

#define BFSET_HASH_UNLOCK( _key ) \
    (void)dyn_hash_release_chain( BfSetHashTbl, _key )

#define BFSET_HASH_REMOVE( _bfSetp, _lock_action ) \
    (void)dyn_hash_remove( BfSetHashTbl, _bfSetp, _lock_action )

#define BFSET_HASH_INSERT( _bfSetp, _lock_action ) \
    (void)dyn_hash_insert( BfSetHashTbl, _bfSetp, _lock_action )


#ifdef ADVFS_BFSET_TRACE

#define BFSET_TRACE( bfsetp, n1 ) \
    bfset_trace((bfsetp), (uint16_t)ADVFS_MODULE, (uint16_t)__LINE__, (void*)(n1))

void
bfset_trace( bfSetT  *bfsetp,
             uint16_t module,
             uint16_t line,
             void    *value);

#else  /* ADVFS_BFSET_TRACE */

#define BFSET_TRACE( bfsetp, n1 )

#endif /* ADVFS_BFSET_TRACE */


/* 
 * ADD_ACC_SETLIST places an access structure onto the head
 * of a fileset structure's list of access structures.  The 
 * fileset's accessChainLock is held if list_lock is TRUE while 
 * that chain is manipulated.  If bfap_lock is TRUE then this macro
 * returns with the BFALOCK_HELD
 */
#define ADD_ACC_SETLIST(bfap, lock_list, lock_bfap) \
{ \
    bfSetT *bfSetp = bfap->bfSetp; \
    MS_SMP_ASSERT(BFSET_VALID(bfSetp)); \
    if (lock_list) \
        ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock); \
    MS_SMP_ASSERT(bfap->setFwd == NULL); \
    MS_SMP_ASSERT(bfap->setBwd == NULL); \
    if (lock_bfap) \
        ADVSMP_BFAP_LOCK(&bfap->bfaLock); \
    bfap->setFwd = bfSetp->accessFwd; \
    bfap->setBwd = (bfAccessT *)(&bfSetp->accessFwd); \
    if (bfSetp->accessBwd == (bfAccessT *)(&bfSetp->accessFwd)) \
        bfSetp->accessBwd = bfap; \
    else \
        bfSetp->accessFwd->setBwd = bfap; \
    bfSetp->accessFwd = bfap; \
    if (lock_list) \
        ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock); \
}

/* 
 * INS_ACC_SETLIST inserts an access structure after currbfap in 
 * a fileset structure's list of access structures.  The fileset's
 * accessChainLock is held while that chain is manipulated.
 * It will be locked and unlocked here if lock_list is TRUE.
 * Otherwise, it is assumed that the caller has locked the
 * chain and will unlock it.
 */
#define INS_ACC_SETLIST(insbfap, currbfap, lock_list) \
{ \
    bfSetT *bfSetp = insbfap->bfSetp; \
    MS_SMP_ASSERT(BFSET_VALID(bfSetp)); \
    if (lock_list) { \
        ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock); \
    } else { \
        MS_SMP_ASSERT(ADVSMP_SETACCESSCHAIN_OWNED(&bfSetp->accessChainLock)); \
    } \
    MS_SMP_ASSERT(insbfap->setFwd == NULL); \
    MS_SMP_ASSERT(insbfap->setBwd == NULL); \
    MS_SMP_ASSERT(currbfap->bfSetp == insbfap->bfSetp); \
    insbfap->setFwd = currbfap->setFwd; \
    if (bfSetp->accessBwd == currbfap) { \
        bfSetp->accessBwd = insbfap; \
    } else { \
        currbfap->setFwd->setBwd = insbfap; \
    } \
    insbfap->setBwd = currbfap; \
    currbfap->setFwd = insbfap; \
    if (lock_list) \
        ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock); \
}

/* 
 * RM_ACC_SETLIST removes an access structure from a fileset
 * structure's list of access structures.  The fileset's
 * accessChainLock is held while that chain is manipulated.
 * It will be locked and unlocked here if lock_list is TRUE.
 * Otherwise, it is assumed that the caller has locked the
 * chain and will unlock it.
 */
#define RM_ACC_SETLIST(bfap, lock_list) \
{ \
    bfSetT *bfSetp = bfap->bfSetp; \
    MS_SMP_ASSERT(BFSET_VALID(bfSetp)); \
    if (lock_list) \
        ADVSMP_SETACCESSCHAIN_LOCK(&bfSetp->accessChainLock); \
    else \
    MS_SMP_ASSERT(ADVSMP_SETACCESSCHAIN_OWNED(&bfSetp->accessChainLock)); \
    MS_SMP_ASSERT(bfap->setFwd != NULL); \
    MS_SMP_ASSERT(bfap->setBwd != NULL); \
    if (bfSetp->accessFwd == bfap) \
        bfSetp->accessFwd = bfap->setFwd; \
    else \
        bfap->setBwd->setFwd = bfap->setFwd; \
    if (bfSetp->accessBwd == bfap) \
        bfSetp->accessBwd = bfap->setBwd; \
    else \
        bfap->setFwd->setBwd = bfap->setBwd; \
    bfap->setFwd = bfap->setBwd = NULL; \
    if (lock_list) \
        ADVSMP_SETACCESSCHAIN_UNLOCK(&bfSetp->accessChainLock); \
}

/* This constant is the number of files that can be freed before a 
 * preemption point will be hit when trying to remove all files in a 
 * fileset 
 */
#define ADVFS_FILES_BEFORE_PREEMPTION_POINT 128

statusT
rbf_bfs_create( 
    struct domain *dmnP,    /* in - domain handle */
    serviceClassT reqServ,  /* in - required service class */
    char *setName,          /* in - set's name */
    uint32_t fsetOptions,   /* in - fileset option flags */
    ftxHT parentFtxH,       /* in - parent transaction handle */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    );


/*
 * Options for bfs_open() and bs_bfs_close().
 */
typedef enum {
    BFS_OP_DEF               = 0x00, /* Default */
    BFS_OP_IGNORE_DEL        = 0x01, /* Ignore the deleting flag */
    BFS_OP_LOCK_TBL_LOCK     = 0x02, /* Lock the table lock if not
                                      *  already locked */
    BFS_OP_TBL_LOCK_LOCKED   = 0x04, /* Indicates the table lock WAS actually 
                                      * locked as requested by the
                                      * BFS_OP_LOCK_TBL_LOCK flag.  If
                                      * the lock was already held and
                                      * BFS_OP_LOCK_TBL_LOCK was set,
                                      * BFS_OP_TBL_LOCK_LOCKED would not
                                      * be set. Used by bfs_access */
   BFS_CL_COND_UNLOCK        = 0x08, /* In only unlock the bfSetTbl lock if
                                      * it was locked by the routine */
   BFS_OP_FLUSH_STATS        = 0x10, /* Flush stats. Used for external opens */
   BFS_OP_UNMOUNT            = 0x20, /* Indicate that an unmount is occuring */
   BFS_OP_IGNORE_OUT_OF_SYNC = 0x40, /* Ignore a fileset marked out of sync */
   BFS_OP_DEACTIVATED_OK     = 0x80  /* Domain deactivated ok in bfs_access */
} bfs_op_flags_t;


statusT
bfs_access(
    bfSetT **retBfSetp,      /* out - pointer to open bitfile-set desc */
    bfSetIdT bfSetId,        /* in - bitfile-set id */
    bfs_op_flags_t*     options,         /* in - options flags */
    ftxHT ftxH);             /* in - transaction handle */

void
bfs_close(
    bfSetT *bfSetp,     /* in - pointer to open bitfile-set desc */
    ftxHT   ftxH        /* in - transaction handle */
    );



statusT
bfs_open(
    bfSetT**    retBfSetp,      /* out - pointer to open bitfile-set */
    bfSetIdT    bfSetId,        /* in - bitfile-set id */
    uint32_t    options,        /* in - options flags */
    ftxHT       ftxH);          /* in - transacton handle */

void
bfs_dealloc(
    bfSetT      *bfSetp, /* in - bitfile set descriptor pointer */
    int         deallocate  /* in - flag indicates to remove the set from existence */
    );

statusT
bs_bfs_close(
    bfSetT*     bfSetp,         /* in - pointer to open bitfile-set */
    ftxHT       ftxH,	        /* in - transaction handle */
    bfs_op_flags_t options      /* in - options flags */
    );


statusT
bs_bfs_delete(
    bfSetIdT bfSetId,      /* in - bitfile set id */
    ftxIdT  xid            /* in - CFS transaction id */
    );

void
bs_bfs_init(
    void
    );

statusT
bs_bfs_add_root( 
    bfSetIdT rootBfSetId,  /* in - bitfile-set id */
    struct domain *dmnP,   /* in - BF-set's domain's pointer */
    bfSetT **retRootBfSetp /* out - pointer to BF-set's descriptor */
    );

void
bs_bfs_switch_root_tagdir ( 
    domainT *domain,       /* in */
    bfTagT newTag          /* in */
    );

void
bfs_delete_pending_list_finish_all(
    domainT *dmnp
    );

bfSetT *
bfs_lookup(bfSetIdT bfSetId);

bfSetT *
bs_bfs_lookup_desc(
    bfSetIdT bfSetId    /* in - bitfile set's ID */
    );


statusT
bfs_create(
    domainT *dmnP,          /* in - domain pointer */
    serviceClassT reqServ,  /* in - required service class */
    char *setName,          /* in - the new set's name */
    uint32_t fsetOptions,   /* in - fileset options */
    ftxHT parentFtxH,       /* in - parent transaction handle */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    );



/*
 * also see bs_public.h for more routines (public bitfile set routines).
 */



#endif /* _KERNEL */
#endif /* _BS_BITFILE_SETS_ */
