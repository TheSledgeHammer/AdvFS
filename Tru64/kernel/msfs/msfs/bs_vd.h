/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993                      *
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
 * @(#)$RCSfile: bs_vd.h,v $ $Revision: 1.1.79.1 $ (DEC) $Date: 2003/07/24 14:13:42 $
 */

#ifndef _VD_H_
#define _VD_H_

#include <sys/param.h>

/*
 * stgDescT - Describes a contiguous set of available (free) vd blocks.
 * These structures are used to maintain a list of free disk space.  There
 * is a free list in each vd structure.  The list is ordered by virtual
 * disk block (it could also be ordered by the size of each contiguous
 * set of blocks in the future).  Refer to the "sbm_" routines in
 * bs_sbm.c.
 */

typedef struct stgDesc {
    uint32T start_clust;        /* vd cluster number of first free cluster */
    uint32T num_clust;          /* number of free clusters */
    struct stgDesc *prevp;
    struct stgDesc *nextp;
} stgDescT;

#ifdef ADVFS_DEBUG
enum deFlags { DEF_NONE=0, DEF_META=1, DEF_DATA=2, 
    DEF_READ=4, DEF_WRITE=8, DEF_REPEAT=16, DEF_LOG=32 };
#endif /* ADVFS_DEBUG */

#ifdef ADVFS_VD_TRACE

#define VD_TRACE_HISTORY 100

typedef struct {
  uint32T       seq;
  uint16T       mod;
  uint16T       ln;
  struct thread *thd;
  void          *val;
} vdTraceElmtT;

#endif /* ADVFS_VD_TRACE */

/* vdp->tempQ is a doubly linked list of tempQMarkers. Each tempQMarker heads a
 * doubly linked list of ioDescs.
 */
typedef struct tempQMarker {
    struct ioDesc *fwd;      /* next ioDesc chained off this marker */
    struct ioDesc *bwd;      /* prev ioDesc chained off this marker */
    mutexT ioQLock;          /* used only in the I/O queue header */
    struct tempQMarker *mfwd; /* next marker in list of tempQMarkers */
    struct tempQMarker *mbwd; /* prev marker in list of tempQMarkers */
    struct thread *thd_id;
    int cnt;                 /* usage varies: in the I/O queue header it is the
                              * total number of markers in the list of tempQMarkers;
                              * otherwise it is currently unused
                              */
} tempQMarkerT;

#ifdef ADVFS_SS_TRACE

#define SS_TRACE_HISTORY 100

typedef struct {
  uint32T       seq;
  uint16T       mod;
  uint16T       ln;
  struct thread *thd;
  long          val1;
  long          val2;
  long          val3;
  long          val4;
} ssTraceElmtT;
#endif /* ADVFS_SS_TRACE */

/*
 * vd - this structure describes a virtual disk, including accessed
 * bitfile references, its size, i/o queues, name, id, and an
 * access handle for the metadata bitfile.
 */

typedef struct vd {
    /*
     ** Static fields (ie - they are set once and never changed).
     */
    uint32T stgCluster;         /* num blks each stg bitmap bit */
    struct vnode *devVp;        /* device access (temp vnode *) */
    uint_t vdMagic;             /* magic number: structure validation */
    bfAccessT *rbmtp;           /* access structure pointer for RBMT */
    bfAccessT *bmtp;            /* access structure pointer for BMT */
    bfAccessT *sbmp;            /* access structure pointer for SBM */
    domainT *dmnP;              /* domain pointer for ds */
    uint32T vdIndex;            /* 1-based virtual disk index */
    uint32T maxPgSz;            /* max possible page size on vd */
    uint32T bmtXtntPgs;         /* number of pages per BMT extent */
    char vdName[BS_VD_NAME_SZ]; /* temp - should be global name */

    /* The following fields are protected by the vdT.vdStateLock mutex */
    bsVdStatesT vdState;        /* vd state */
    struct thread *vdSetupThd;  /* Thread Id of the thread setting up vdT */
    uint32T     vdRefCnt;       /* # threads actively using this volume */
    uint32T     vdRefWaiters;   /* # threads waiting for vdRefCnt to goto 0 */
    mutexT      vdStateLock;    /* lock for above 4 fields */   

    /* 
     * The following fields are protected by the vdScLock semaphore
     * in the domain structure.  This lock is protected by the
     * domain mutex.  Use the macros VD_SC_LOCK and VD_SC_UNLOCK.
     */
    uint32T vdSize;             /* count of vdSectorSize blocks in vd */
    int vdSectorSize;           /* Sector size, in bytes, normally 512 */
    uint32T vdClusters;         /* num clusters in vd */
    serviceClassT serviceClass; /* service class provided */

    ftxLkT mcell_lk;        /* used with domain mutex */
    int nextMcellPg;        /* next available metadata cell's page num */
    uint32T allocClust;     /* clusters attempting to be allocated */
    ftxLkT rbmt_mcell_lk;   /* This lock protects mcell allocation from
                             * the rbmt mcell pool.  This pool is used
                             * to extend reserved bitfiles.
                             */
    int lastRbmtPg;         /* last available reserved mcell's page num */
    int rbmtFlags;          /* protected by rbmt_mcell_lk */

    ftxLkT stgMap_lk;       /* used with domain mutex */
    stgDescT *freeStgLst;   /* ptr to list of free storage descriptors */
    uint32T numFreeDesc;    /* number of free storage descriptors in list */
    uint32T freeClust;      /* total number free clusters */
    uint32T scanStartClust; /* cluster where next bitmap scan will start */
    uint32T bitMapPgs;      /* number of pages in bitmap */
    uint32T spaceReturned;  /* space has been returned */
    stgDescT *fill1;        /* ptr to list of reserved storage descriptors */
    stgDescT *fill3;        /* ptr to list of free, reserved stg descriptors */
    uint32T fill4;          /* # of free, reserved stg descriptors in list */


    ftxLkT del_list_lk;         /* protects global defered delete list */

    lock_data_t ddlActiveLk;   /* Synchs processing of deferred-delete list entries */
                                /* used with domain mutex */
    bfMCIdT ddlActiveWaitMCId;  /* If non-nil, a thread is waiting on this entry */
                                /* Use domain mutex for synchronization */
    cvT ddlActiveWaitCv;        /* Used when waiting for active ddl entry */

    struct dStat dStat;      /* collect device statistics */
    long pad1[6];            /* pad to cache line boundary */

    /*
     * I/O queues; their fields are protected by a lock in the ioDescHdr
     */
    ioDescHdrT blockingQ;     /* Blocking I/O: an immediate-priority queue */
    ioDescHdrT ubcReqQ;       /* A queue for immediate-priority UBC requests */
    ioDescHdrT flushQ;        /* An intermediate-priority queue */
    ioDescHdrT waitLazyQ;     /* Transactional buffers w/ too high lsn */
    ioDescHdrT smSyncQ0;      /* smooth sync queues */
    ioDescHdrT smSyncQ1;      /* smooth sync queues */
    ioDescHdrT smSyncQ2;      /* smooth sync queues */
    ioDescHdrT smSyncQ3;      /* smooth sync queues */
    ioDescHdrT smSyncQ4;      /* smooth sync queues */
    ioDescHdrT smSyncQ5;      /* smooth sync queues */
    ioDescHdrT smSyncQ6;      /* smooth sync queues */
    ioDescHdrT smSyncQ7;      /* smooth sync queues */
    ioDescHdrT smSyncQ8;      /* smooth sync queues */
    ioDescHdrT smSyncQ9;      /* smooth sync queues */
    ioDescHdrT smSyncQ10;     /* smooth sync queues */
    ioDescHdrT smSyncQ11;     /* smooth sync queues */
    ioDescHdrT smSyncQ12;     /* smooth sync queues */
    ioDescHdrT smSyncQ13;     /* smooth sync queues */
    ioDescHdrT smSyncQ14;     /* smooth sync queues */
    ioDescHdrT smSyncQ15;     /* smooth sync queues */
    tempQMarkerT tempQ;       /* temporary queue used in sort_onto_readyq() */
    long pad2;                /* pad to cache line boundary */
    ioDescHdrT readyLazyQ;    /* Sorted, ready for consolidation */ 
    ioDescHdrT consolQ;       /* Consolidated, ready to be written */
    ioDescHdrT devQ;          /* Tracks device */

    /* These fields are protected by the devQ.ioQLock */
    stateLkT active;          /* indicates when disk (or lazy thread) is busy */
    int vdIoOut;              /* There are outstanding I/Os on this vd */
    int gen_active;           /* I/O generation loop active */

    int flushFlags;           /* counters for in-progress device flushes */
    int start_io_posted_waiter;  /* vd_remove waiting for start_io_posted to clear */
    short start_io_posted;         /* 0 = no message yet posted */
                                   /* 1 = message posted but not processed */
    long vd_lbolt;            /* time when I/O completion sampling started */
    u_int vd_sample_raw_count;/* raw counts per sampling period */
    /* end of fields protected by devQ.ioQLock */

#define BLOCKFACT 4
    int blockingFact;         /* keep track of how many times we can take
                                 from blocking q before taking from consol q */
    int rdmaxio;              /* max blocks that can be read/written  */
    int wrmaxio;              /* in a consolidated I/O */

    /* These fields are protected by the vdIoLock */
    mutexT vdIoLock;          /* simple lock for guarding I/O fields */
    u_int syncQIndx;          /* next smsync queue to be processed */
    u_int vdRetryCount;       /* count of AdvFS initiated retries */
#ifdef ADVFS_DEBUG
    enum deFlags errorFlag;
    int errorCount;
    int errorRepeat;
#endif /* ADVFS_DEBUG */
#ifdef ADVFS_SMP_ASSERT
    u_long rmioq_cnt;               /* count of bufs rm_ioq'ed */
    u_long rmormvq_cnt;             /* count of bufs rm_or_moveq'ed */
#endif
    /* end of fields protected by vdIoLock */

    int consolidate;          /* Flag, one indicates disk can take big io's */
    int max_iosize_rd;        /* From device */
    int max_iosize_wr;        /* From device */
    int preferred_iosize_rd;  /* From device */
    int preferred_iosize_wr;  /* From device */
    int qtodev;               /* max number of I/O's to be queued to dev */

    stgDescT freeRsvdStg;     /* desc for free rsvd stg for rsvd files */
    stgDescT freeMigRsvdStg;  /* desc for free rsvd stg for migrating files */
    ssVolInfoT ssVolInfo;     /* smartstore frag and free lists */
#   ifdef ADVFS_VD_TRACE
        uint32T trace_ptr;
        vdTraceElmtT trace_buf[VD_TRACE_HISTORY];
#   endif
#ifdef ADVFS_SS_TRACE
    uint32T ss_trace_ptr;
    ssTraceElmtT ss_trace_buf[SS_TRACE_HISTORY];
#endif
} vdT;

#define IOTHRESHOLD 1024  /* Default # of buffers allowed to accumulate on
                           * the readyLazy queue before they get moved down
                           * to the consolq.
                           */
/*
 * This is the maximum number of 512 byte blocks that can
 * be combined into one I/O.
 */
#define RDMAXIO 128
#define WRMAXIO 128
#define DEF_CONSOL 1

#define QTODEV 5        /* shift value for determining how many lazy buffers
                         * to queue to device during IO_SOMEFLUSH.  Value
                         * of 5 is 3.125% of the buffers on consolQ.
                         */
#ifdef ADVFS_VD_TRACE

#define VD_TRACE( vdp, n1 ) \
    vd_trace((vdp), (uint16T)ADVFS_MODULE, (uint16T)__LINE__, (void*)(n1))

void
vd_trace( vdT     *vdp,
          uint16T module,
          uint16T line,
          void    *value );

#else  /* ADVFS_VD_TRACE */

#define VD_TRACE( vdp, n1 )

#endif /* ADVFS_VD_TRACE */

#ifdef ADVFS_SS_TRACE

#define SS_TRACE( vdp, n1,n2,n3,n4 ) \
    ss_trace((vdp), (uint16T)ADVFS_MODULE, (uint16T)__LINE__, (long)(n1),(long)(n2),(long)(n3),(long)(n4))

void
ss_trace( vdT     *vdp,
          uint16T module,
          uint16T line,
          long    value1,
          long    value2,
          long    value3,
          long    value4
         );
#else  /* ADVFS_SS_TRACE */

#define SS_TRACE( vdp, n1,n2,n3,n4 )

#endif /* ADVFS_SS_TRACE */

/* Values for vdT.nextMcellPg */
#define EXTEND_BMT -1
#define NO_MORE_MCELLS -2

/* Values for vdT.rbmtFlags */
#define RBMT_EXTENSION_IN_PROGRESS 0x1

/* Test validity of a block in an extent. */
/* "Returns" EBAD_PARAMS if the block is not a multiple of blksPerPage. */
/* "Returns" E_RANGE if the block is beyond the end of the volume. */
/* Else "returns" EOK. */
#define TEST_BLOCK(blk, pgsz, vdp)   \
(                                    \
    (blk) >= (vdp)->vdSize ?         \
        E_RANGE :                    \
        (blk) % pgsz != 0 ?          \
            EBAD_PARAMS :            \
            EOK                      \
)

#endif /* _VD_H_ */
