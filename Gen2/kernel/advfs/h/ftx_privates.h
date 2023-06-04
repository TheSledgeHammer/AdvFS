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
 *  Typedefs, structs, defines, and function prototypes for ftx routines.
 *
 */

/* bs_public.h must be included prior to this header */

#ifndef _FTX_PRIVATES_
#define _FTX_PRIVATES_

/***********************************************************
 *
 * Common EXTERN declarations
 *
 ***********************************************************/


/***********************************************************
 *
 * Ftx stable record types, structures (on-disk log records)
 *
 ***********************************************************/

/* RECOVERY passes; no longer done in numerical order; rather done in the
 * order:  Meta-meta, meta, data.  Since we need to be able to read
 * older log versions, the values for META_PASS and DATA_PASS can not
 * be changed unless the code first checks the on-disk version. 
 *
 * Because we can't #include this file in ftx_public.h where these values
 * can be used, the #defined values for the recovery passes are duplicated
 * here.  If you change the values here, be sure to change them there as well.
 * The values will only be #defined once.
 */

#ifndef FTX_MAX_RECOVERY_PASS
#   define FINAL_PASS                    0   /* see ftx_public.h for use */
#   define META_META_PASS                3
#   define META_PASS                     1   /* do not change this value */
#   define DATA_PASS                     2   /* do not change this value */
#   define FTX_MAX_RECOVERY_PASS DATA_PASS   /* last recovery pass */
#endif

/* RECOVERY PASS COMPARISON macros:  Since the passes are now done in
 * a sequence that is not increasing numerical order, the comparison of
 * atomicRPass values is complicated slightly.  Direct == comparisons are
 * OK, but any < or > comparison must be done via the following macros:
 */
/* Evaluates to 1 if pass1 < pass2; else 0 */
#define RECOV_PASS_LT( dmnp, pass1, pass2)                                         \
                                                                                   \
    ( (pass1) == META_META_PASS && (pass1) != (pass2) ) ? 1 : (pass1) < (pass2)    \

/* Evaluates to 1 if pass1 <= pass2; else 0 */
#define RECOV_PASS_LTE( dmnp, pass1, pass2)                                        \
                                                                                   \
    ( (pass1) == META_META_PASS) ? 1 : (pass1) <= (pass2) 


typedef enum { ftxNilLR, ftxDoneLR } ftxLRTypeT;

/* TODO: force type fields to be 32 bits?? */
typedef struct {
    ftxLRTypeT type;            /* ftx Log Record Type */
    signed level : 8;           /* ftx node level */
    unsigned atomicRPass : 8;   /* atomic recovery pass number */
    signed member : 16;         /* ftx node member */
    ftxIdT ftxId;               /* ftx tree globally unique id */
    bfDomainIdT bfDmnId;        /* bitfile domain global unique id */
    logRecAddrT crashRedo;      /* crash redo start log address */
    ftxAgentIdT fdl_agentId;    /* ftx opx agent for this ftx node */
    uint32_t contOrUndoRBcnt;   /* continuation/undo opx record byte count */
    uint32_t rootDRBcnt;        /* root done opx record byte count */
    uint32_t opRedoRBcnt;       /* redo opx record byte count */
} ftxDoneLRT;

typedef struct {
    bfTagT bfsTag;              /* bitfile set tag */
    bfTagT tag;                 /* bitfile tag */
    bs_meta_page_t page;        /* bitfile page number */
    uint32_t numXtnts;          /* number of record extents */
} ftxRecRedoT;

typedef struct {
    off_t pgBoff;               /* page relative extent byte offset */
    uint32_t bcnt;              /* record extent byte count */
} ftxRecXT;

/***********************************************************
 *
 * Ftx volatile (memory) record types, structures
 *
 ***********************************************************/

/*
 * The ftxAgent structure describes a specific ftx agent.
 */

typedef struct {
    ftxAgentIdT id;     /* agent identifier */
    opxT *undoOpX;      /* undo opx routine ptr */
    opxT *rootDnOpX;    /* root done opx routine ptr */
    opxT *redoOpX;      /* redo opx routine ptr */
    opxT *contOpX;      /* continuation opx routine ptr */
} ftxAgentT;

/**********************************
 * module specific types, structs
 *********************************/

/*
 * ftxRDHdrT - buffered root done record header
 */
typedef struct {
    uint32_t atomicRPass;   /* recovery pass number */
    int32_t agentId;        /* agent ID */
    int32_t bCnt;           /* byte count of data to follow */
} ftxRDHdrT;

#define FTX_MAX_CONT_REC_BSZ 96

typedef struct {
    int32_t agentId;        /* agent ID */
    int32_t bCnt;           /* byte count of data record */
    int32_t rec[ FTX_MAX_CONT_REC_BSZ/sizeof(int32_t) ];
} ftxContRecT;

#define FTX_MX_PINR 7           /* maximum pinned records per page */

typedef struct {
    int32_t ftxPinS;            /* ftx pin page slot */
    int32_t numXtnts;           /* number of record extents */
    ftxRecXT recX[FTX_MX_PINR]; /* record extent list */
} lvlPinTblT;

/*
 * perlvl struct - contains state for each level of an ftx.  An array
 * of these is in the ftx struct.
 */
#define FTX_MX_PINP 7           /* maximum pinned pages per level. 
                                 * DEL_MAX_PAGES should match this value. */

typedef struct {                /* per level context */
#ifdef ADVFS_DEBUG
    int32_t startLn;            /* line number of ftx_start call; debug */
    char *startFn;              /* file name of ftx_start call; debug */
#endif
#ifdef FTX_PROFILING
    ftxAgentIdT agentId;        /* agent ID */
#endif
    ftxDoneModeT donemode;      /* done mode for this level */
    uint32_t atomicRPass;       /* atomic recovery pass number */
    int32_t member;             /* current member within the level */
    void *lkList;               /* list of ftx locks */
    logRecAddrT skipSubsLink;   /* link to skip this level's subftxs */
    int32_t lastPinS;           /* next slot to use */
    lvlPinTblT lvlPinTbl[FTX_MX_PINP];  /* per level pin table */
} perlvlT;

/*
 * ftxPinTblT struct - contains state associated with a page pinned
 * under ftx control.  An array of these is contained in the perlvl
 * struct.
 */

typedef struct {
    int32_t ftxLvl;             /* transaction level to unpin at */
    bfPageRefHT pgH;            /* ref page handle */
    void* pgAddr;               /* address of pinned page */
    struct bfAccess *pinAccessp;/* bitfile access structure pointer */
    bf_fob_t pgSz;              /* bitfile page size in 1k fobs */
    bsUnpinModeT unpinMode;     /* page unpin mode */
    ftxRecRedoT pgdesc;         /* page descriptor */
} ftxPinTblT;

/*
 * The ftx structure contains state for an active transaction.
 * 
 * The Deepest known transaction tree is 11 (as of Nov 98) and here is the
 * trace
 * 
 *  bmt_alloc_mcell               <== FTA_BS_RBMT_ALLOC_MCELL_V1
 *  stg_alloc_new_mcell
 *  stg_alloc_from_one_disk       <== FTA_BS_STG_ALLOC_MCELL_V1
 *  alloc_append_stg
 *  alloc_stg
 *  add_rsvd_stg
 *  stg_add_stg_no_cow            <== FTA_BS_STG_ADD_V1
 *  stg_add_stg                   
 *  bmt_extend                    <== FTA_BS_BMT_EXTEND_V1 
 *  alloc_mcell
 *  bmt_alloc_mcell               <== FTA_BS_BMT_ALLOC_MCELL_V1
 *  stg_alloc_new_mcell
 *  stg_alloc_from_one_disk       <== FTA_BS_STG_ALLOC_MCELL_V1
 *  stg_alloc_from_svc_class
 *  alloc_append_stg
 *  alloc_stg
 *  add_stg
 *  rbf_add_overlapping_clone_stg  <== FTA_BS_STG_ADD_V1
 *  bs_cow_pg                      <== FTA_BS_COW_PG
 *  bs_cow
 *  bs_pinpg_clone
 *  bs_pinpg_ftx
 *  rbf_pinpg
 *  idx_directory_get_space_int
 *  idx_directory_get_space        <== FTA_IDX_UNDO_V1
 *  insert_seq                     <== FTA_FS_INSERT_V1
 *  fs_create_file                 <== Starts Root Ftx
 *  advfs_create
 *
 *  February '04... Snapshots now produce the deepest transaction tree.
 *
 *  sbm_alloc_bits
 *  sbm_remove_space
 *  stg_alloc_one_xtnt
 *  alloc_from_one_disk
 *  stg_alloc_from_one_disk
 *  alloc_append_stg
 *  alloc_stg
 *  add_rsvd_stg
 *  stg_add_stg
 *  bmt_extend
 *  alloc_mcell
 *  bmt_alloc_link_mcell
 *  advfs_snap_mcell
 *  advfs_snap_copy_mcell_chain
 *  advfs_setup_cow
 *  advfs_access_snap_children
 *  advfs_getpage
 *  preg_vn_fault
 *  fcache_as_fault
 *  bs_pinpg
 *  rbf_pinpg
 *  advfs_fs_dqsync
 *  chk_blk_quota
 *  advfs_bs_add_stg
 *  advfs_getpage
 *  preg_vn_fault
 *  fcache_as_uiomove_wrt
 *  fcache_as_uiomove
 *  advfs_fs_write
 *  advfs_write
 *  advfs_rdwr
 *  ...
 */

#define FTX_MX_NEST     16
#define FTX_TOTAL_PINS  10
#define FTX_RD_BFRSZ    100

typedef enum {
    NORMAL, REC_REDO, OP_REDO, OP_UNDO, ROOTDONE, CONTINUATION
} ftxTypeT;

/*
 * log record descriptor
 */
typedef struct {
    int32_t count;          /* count of buffer elements in use */
    int32_t dataLcnt;       /* total data count in longs */
    logBufVectorT bfrvec[4 + FTX_MX_PINP*(FTX_MX_PINR + 2)];
} lrDescT;

#ifdef ADVFS_FTX_TRACE
#   define FTX_TRACE_HISTORY 100
    typedef struct {
        uint32_t        seq;
        uint16_t        mod;
        uint16_t        ln;
#ifdef _KERNEL
        struct kthread *thd;
#endif /* _KERNEL */
        void           *val;
    } ftxTraceElmtT;
#endif /* ADVFS_FTX_TRACE */

typedef struct ftx {
#ifdef _KERNEL
    struct kthread *thd;
#endif /* _KERNEL */
    logRecAddrT firstLogRecAddr;    /* logaddr of first log rec written */
    logRecAddrT undoBackLink;       /* undo back link log address */
    logRecAddrT lastLogRec;         /* logaddr of last log rec written */
    ftxTypeT type;                  /* current state */
    int32_t currLvl;                /* current ftx level */
    ftxDoneLRT lrh;                 /* log record header */
    int32_t lastFtxPinS;            /* next pin slot to use */
    ftxPinTblT pinTbl[FTX_TOTAL_PINS]; /* pinned page handles */
    perlvlT cLvl[FTX_MX_NEST];      /* per level data structures */
    int32_t rootDnCnt;              /* count of root done records */
    int32_t nextRDoff;              /* next root done record offset (long) */
    int32_t rootDoneRecs[FTX_RD_BFRSZ]; /* buffered root done records */
    ftxContRecT contDesc;           /* continuation record descriptor */
    lrDescT lrdesc;                 /* so we don't malloc at ftx_done */
#ifdef ADVFS_FTX_TRACE
    uint32_t trace_ptr;             /* access trace buffer */
    ftxTraceElmtT trace_buf[FTX_TRACE_HISTORY];
#endif
} ftxStateT;

#ifdef ADVFS_FTX_TRACE
#   define FTX_TRACE( ftxp, n1 ) \
     ftx_trace((ftxp), (uint16_t)ADVFS_MODULE, (uint16_t)__LINE__, (void*)(n1))
    void
    ftx_trace(
        ftxStateT *ftxp,
        uint16_t   module,
        uint16_t   line,
        void      *value
        );
#else
#   define FTX_TRACE( ftxp, n1 )
#endif

/******************************************************
 *******   Function Prototypes ************************
 ******* shared by ftx_routines and ftx_recovery ******
 *****************************************************/

void
addone_cont(
            void* recptr,      /* in - ptr to redo record */
            ftxStateT* ftxp,   /* in/out - ptr to ftx state struct */
            lrDescT* lrdp      /* in/out - ptr to log rec desc */
            );


void
ftx_release_slot(
    int32_t ftxSlot,
    struct ftxTblD *ftxTDp
    );

/*
 * reset_oldest_lsn - check to see if this was the oldest lsn and force
 * update if so.
 *
 * The caller must hold the domainT.ftxTblLock, and already put this ftx
 * back on the free list, except when called during continuations.
 */
void
reset_oldest_lsn(
    ftxStateT* ftxp,         /* in - ptr to ftx state */
    struct domain* dmnp,     /* in/out - ptr to domain */
    int32_t continuation     /* in - true when continuation */
    );
/*
 * addone_recredo - add record image redo elements to log record
 * vector.
 */
void
addone_recredo(
    ftxStateT* ftxp,         /* in/out - ftx state */
    lrDescT* lrdp            /* in/out - log rec desc */
    );

/*
 * log_donerec - call the logger with the ftx done record, and unpin
 * dirty pages associated with it.
 */
statusT
log_donerec_nunpin(
    ftxStateT* ftxp,         /* in/out - ftx state */
    struct domain* dmnp,     /* in - domain state */
    lrDescT* lrdp            /* in/out - done record desc */
    );

/*
 * ftx_fail_2 - this version of fail take the recovery pass as an
 * additional argument for use by recovery.
 */

void
ftx_fail_2(
    ftxHT ftxH,              /* in - leaf (current) ftx handle */
    uint32_t atomicRPass     /* recovery pass */
    );

static
void
ftx_unlock(
    void *lk                 /* in */
    );

void
release_ftx_locks(
    perlvlT *clvlp
    );

void
do_ftx_continuations(
    struct domain* dmnp,     /* in/out - ptr to domain */
    ftxStateT* ftxp,         /* in/out - ftx state */
    ftxHT ftxH,              /* in - ftx handle */
    lrDescT* lrdp            /* in/out - done record desc */
    );

perlvlT *
get_perlvl_p(
    ftxHT ftxH
    );

/*
 * lgr_writev_ftx - Special version of log write called from the
 * transaction manager.  This version serializes the setting of the
 * crash restart log address in the ftx log record header with the
 * writing of the log record.
 *
 * Defined here because of header ordering difficulty defining it in
 * ms_logger.h.
 *
 */
statusT
lgr_writev_ftx(
    ftxStateT* ftxp,        /* in/out - ftx state */
    struct domain* dmnp,    /* in - domain state */
    lrDescT* lrdp,          /* in/out - ftx done record desc */
    logWriteModeT lw_mode   /* in  - write mode (sync, async, ...) */
    );

/*
 * ftx_set_oldestftxla
 *
 * Use the "minimal sync" data structure.
 *
 * The domainT.ftxTblLock must be held by the caller, except for
 * initialization, when there is only one thread active.
 *
 * Note that in order to initialize oldftxLa correctly, the following
 * sequence must be executed:
 *
 *    ftx_set_oldestftxla
 *    get_oldestftxla - gets a null result, throw it away
 *    ftx_set_oldestftxla
 *
 */

void
ftx_set_oldestftxla(
    struct domain* dmnp,
    logRecAddrT oldftxLa
    );

/*
 * ftx_get_oldestftxla - gets the oldest dirty buffer log address
 */

logRecAddrT
ftx_get_oldestftxla(
    struct domain* dmnp
    );

/*
 * ftx_set_firstla - set the first log record address for a particular
 * ftx, as well as the overall oldest ftx log record address, if
 * appropriate.
 *
 * Return the oldest ftx address, as we've got to acquire the
 * domainT.ftxTblLock to set the first log address for this ftx anyway.
 */
logRecAddrT
ftx_set_firstla(
    ftxStateT* ftxp,    /* in/out - ptr to ftx state */
    struct domain* dmnp,/* in/out - ptr to domain */
    logRecAddrT fla     /* in - first log addr */
    );

#endif /* _FTX_PRIVATES_ */

