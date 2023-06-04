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
 *      Virtual disk descriptor definitions.
 *
 */
/*
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993
 */

#ifndef _VD_H_
#define _VD_H_

#include <sys/param.h>

#include <advfs/ms_generic_locks.h>

/*
 * stgDescT - Describes a contiguous set of available (free) vd blocks.
 * These structures are used to maintain a list of free disk space.  There
 * is a free list in each vd structure.  The list is ordered by virtual
 * disk block (it could also be ordered by the size of each contiguous
 * set of blocks in the future).  Refer to the "sbm_" routines in
 * bs_sbm.c.
 */

typedef struct stgDesc {
    bf_vd_blk_t start_clust;        /* vd cluster number of first free cluster */
    bf_vd_blk_t num_clust;          /* number of free clusters */
    struct stgDesc *prevp;
    struct stgDesc *nextp;
} stgDescT;

#ifdef ADVFS_VD_TRACE
#   define VD_TRACE_HISTORY 100
    typedef struct {
        uint32_t        seq;
        uint16_t        mod;
        uint16_t        ln;
#ifdef _KERNEL
        struct kthread *thd;
#endif /* _KERNEL */
        void           *val;
    } vdTraceElmtT;
#endif /* ADVFS_VD_TRACE */


#ifdef ADVFS_SS_TRACE
#   define SS_TRACE_HISTORY 100
    typedef struct {
        uint32_t        seq;
        uint16_t        mod;
        uint16_t        ln;
#ifdef _KERNEL
        struct kthread *thd;
#endif /* _KERNEL */
        long            val1;
        long            val2;
        long            val3;
        long            val4;
    } ssTraceElmtT;
#endif /* ADVFS_SS_TRACE */

/*
 * vd - this structure describes a virtual disk, including accessed
 * bitfile references, its size, i/o queues, name, id, and an
 * access handle for the metadata bitfile.
 */

#ifdef _KERNEL

typedef struct vd {
    /* Temporarily move this structure to the beginning to
     * avoid PA alignment issues.  ssVolInfoT has a spin lock
     * as the first element of the structure */
    ssVolInfoT ssVolInfo;      /* smartstore frag and free lists */
    /*
     ** Static fields (ie - they are set once and never changed).
     */
    bf_vd_blk_t stgCluster;     /* num DEV_BSIZE blks each stg bitmap bit */
    struct vnode *devVp;        /* device access (temp vnode *) */
    uint32_t vdMagic;           /* magic number: structure validation */
    bfAccessT *rbmtp;           /* access structure pointer for RBMT */
    bfAccessT *bmtp;            /* access structure pointer for BMT */
    bfAccessT *sbmp;            /* access structure pointer for SBM */
    domainT *dmnP;              /* domain pointer for ds */
    vdIndexT vdIndex;           /* 1-based virtual disk index */
    adv_time_t  vdCookie;       /* unique volumeId */
    bs_meta_page_t bmtXtntPgs;  /* number of pages per BMT extent */
    char vdName[BS_VD_NAME_SZ]; /* temp - should be global name */

    /* The following fields are protected by the vdT.vdStateLock mutex */
    bsVdStatesT vdState;        /* vd state */
/* #ifdef _KERNEL */
    struct kthread *vdSetupThd; /* Thread Id of the thread setting up vdT */
/* #endif  _KERNEL */
    uint32_t     vdRefCnt;      /* # threads actively using this volume */
                                /* Do not use C language ++ or -- to change */
                                /* vdRefCnt. vd_htop_already_valid() uses an */
                                /* atomic increment macro that states not to.*/
    uint32_t     vdRefWaiters;  /* # threads waiting for vdRefCnt to goto 0 */
    cv_t         vdRefWaitersCV;/* Cond Var for vdRefWaiters */
    mutex_t      vdStateLock;   /* lock for above 4 fields */   

    /* 
     * The following fields are protected by the vdScLock semaphore
     * in the domain structure.  This lock is protected by the
     * domain mutex.  Use the macros VD_SC_LOCK and VD_SC_UNLOCK.
     */
    bf_vd_blk_t vdSize;         /* count of vdSectorSize blocks in vd */
    int vdSectorSize;           /* Sector size, in bytes, normally DEV_BSIZE */
    bf_vd_blk_t vdClusters;     /* num clusters in vd (num bits in sbm) */
    serviceClassT serviceClass; /* service class provided */

    ftxLkT mcell_lk;            /* used with domain mutex */
    bs_meta_page_t nextMcellPg; /* next available metadata cell's page num */
    ftxLkT rbmt_mcell_lk;       /* This lock protects mcell allocation from
                                 * the rbmt mcell pool.  This pool is used
                                 * to extend reserved bitfiles.
                                 */
    bs_meta_page_t lastRbmtPg;  /* last available reserved mcell's page num */
    int rbmtFlags;              /* protected by rbmt_mcell_lk */

    ftxLkT stgMap_lk;           /* used with domain mutex */
    stgDescT *freeStgLst;       /* ptr to list of free storage descriptors */
    uint32_t numFreeDesc;       /* number of free storage descriptors in list */
    bf_vd_blk_t freeClust;      /* total num free clusters (free bits in sbm) */
    bf_vd_blk_t scanStartClust; /* cluster where next bitmap scan will start */
    bs_meta_page_t bitMapPgs;   /* number of pages in bitmap */
    uint32_t spaceReturned;     /* space has been returned */
    stgDescT *fill1;            /* ptr to list of reserved storage descriptors */
    stgDescT *fill3;            /* ptr to list of free, reserved stg descriptors */
    uint32_t fill4;             /* # of free, reserved stg descriptors in list */

    ftxLkT del_list_lk;         /* protects global defered delete list */

    ADVRWL_DDLACTIVE_T ddlActiveLk; /* Synchs processing of deferred-delete *
				   * list entries */
                                /* used with domain mutex */

    bfMCIdT ddlActiveWaitMCId;  /* If non-nil, a thread is waiting on this entry */
                                /* Use domain mutex for synchronization */
    cv_t ddlActiveWaitCv;       /* Used when waiting for active ddl entry */

    struct dStat dStat;         /* collect device statistics */
    uint64_t vdIoOut;           /* There are outstanding I/Os on this vd */
    uint32_t vdRetryCount;      /* count of AdvFS initiated retries */
    uint32_t current_iosize_rd; /* Current read IO transfer byte size */
    uint32_t current_iosize_wr; /* Current write IO transfer byte size */
    uint32_t max_iosize;        /* Driver's max rd/wr IO transfer byte size */
    uint32_t preferred_iosize;  /* Driver's preferred r/w IO transfer byte sz*/
    stgDescT freeRsvdStg;       /* desc for free rsvd stg for rsvd files */
    stgDescT freeMigRsvdStg;    /* desc for free rsvd stg for migrating files*/
#   ifdef ADVFS_VD_TRACE
        uint32_t trace_ptr;
        vdTraceElmtT trace_buf[VD_TRACE_HISTORY];
#   endif
#   ifdef ADVFS_SS_TRACE
        uint32_t ss_trace_ptr;
        ssTraceElmtT ss_trace_buf[SS_TRACE_HISTORY];
#   endif
} vdT;
#endif  /* #ifdef _KERNEL */

#ifdef ADVFS_VD_TRACE
#   define VD_TRACE( vdp, n1 )  \
        vd_trace((vdp),         \
        (uint16_t)ADVFS_MODULE, \
        (uint16_t)__LINE__,     \
        (void*)(n1))
    void
    vd_trace(
        vdT     *vdp,
        uint16_t module,
        uint16_t line,
        void    *value
        );

#else 
#   define VD_TRACE( vdp, n1 )
#endif

#ifdef ADVFS_SS_TRACE
#   define SS_TRACE( vdp, n1,n2,n3,n4 ) \
        ss_trace(                       \
            (vdp),                      \
            (uint16_t)ADVFS_MODULE,      \
            (uint16_t)__LINE__,          \
            (int64_t)(n1),              \
            (int64_t)(n2),              \
            (int64_t)(n3),              \
            (int64_t)(n4))

    void
    ss_trace(
        vdT     *vdp,
        uint16_t module,
        uint16_t line,
        int64_t  value1,
        int64_t  value2,
        int64_t  value3,
        int64_t  value4
        );
#else 
#   define SS_TRACE( vdp, n1,n2,n3,n4 )
#endif

/* Values for vdT.nextMcellPg */
#define EXTEND_BMT     -1
#define NO_MORE_MCELLS -2

/* Values for vdT.rbmtFlags */
#define RBMT_EXTENSION_IN_PROGRESS 0x1

#endif /* _VD_H_ */

