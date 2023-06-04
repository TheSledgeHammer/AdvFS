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
 * @(#)$RCSfile: msfs_syscalls.h,v $ $Revision: 1.1.132.1 $ (DEC) $Date: 2003/11/11 21:22:19 $
 */

#ifndef _MSFS_SYSCALLS_
#define _MSFS_SYSCALLS_

#include <sys/param.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <msfs/bs_public.h>
#include <msfs/ms_public.h>
#include <msfs/vfast.h>

#define MSFS_DMN_DIR "/etc/fdmns"
#define ADVFS_LOCK_FILE "/etc/fdmns/.advfslock_"
#define ADVFS_LOCK_FILE_FDMNS "/etc/fdmns/.advfslock_fdmns"

typedef int opTypeT;
typedef int opIndexT;

typedef unsigned long u64T;
typedef long i64T;
typedef uint32T u32T;
typedef int32T  i32T;
typedef uint16T u16T;

typedef int mlStatusT;

typedef bfTagT mlBfTagT;
typedef bfFragT mlbfFragT;
typedef bfFragIdT mlbfFragIdT;
typedef fragGrpT mlfragGrpT;
typedef struct timeval mlIdT;
typedef bfDomainIdT mlBfDomainIdT;
typedef bfSetIdT mlBfSetIdT;

typedef enum { 
    ML_NORMAL_METADATA,
    ML_XTNT_METADATA,
    ML_PRIME_MCELL_XTNT_METADATA
} mlMetadataTypeT;

typedef enum {
    ML_VD_NO_OP,
    ML_VD_ADDVOL,
    ML_VD_RMVOL
} mlVdOpT;

typedef struct {
    mlBfTagT bfSetTag;
    mlBfTagT bfTag;
    mlMetadataTypeT metadataType;
} mlBfDescT;

#define ML_BFTAG_RSVD(tag) ((signed)((tag).num) < 0)
#define ML_BFTAG_EQL(tag1, tag2) \
    (((tag1).num == (tag2).num) && ((tag1).seq == (tag2).seq))

#define ML_VOL_NAME_SZ        BS_VD_NAME_SZ
#define ML_DOMAIN_NAME_SZ     BS_DOMAIN_NAME_SZ
#define ML_SET_NAME_SZ        BS_SET_NAME_SZ
#define ML_FS_CONTEXT_SZ      BS_FS_CONTEXT_SZ

#define ML_BFM_RSVD_CELLS     BFM_RSVD_CELLS
#define ML_BFM_RSVD_TAGS      BFM_RSVD_TAGS

#define ML_BFTAG_VDI(tag,version)                                             \
    (-((signed)(tag).num) /                                                   \
          ((version) == 2 ? ML_BFM_RSVD_TAGS :                                \
          ((version) == 3 ? ML_BFM_RSVD_TAGS :                                \
          ((version) == 4 ? ML_BFM_RSVD_TAGS :                                \
          (fprintf(stderr, "Update the ML_BFTAG_VDI macro!\n"),0)             \
    ))))


typedef serviceClassT mlServiceClassT;

/*
 * Tracing stuff
 */
typedef enum { 
    tNone = 0, tAll = 0x7fffffff,
    tAccess = 1, tClose = 2, tCreate = 4,
    tMutex = 0x10, tCond = 0x20, tLock = 0x40,
    tFtx = 0x100, tFtxPP = 0x200, tMem = 0x400,
    tRef = 0x1000, tPin = 0x2000, 
    tDevRd = 0x4000, tDevWr = 0x8000
} mlTrFlagsT;

#define ML_TR_FLAGS 15

typedef enum { TRACE_GET, TRACE_SET, TRACE_CLR } mlTrActionT;

typedef struct {
    int total_calls;
    struct timeval cum_time;
} mlFtxProfT;

/*
 * Kernel disk stats - filled by calls to
 * the table command.
 */
#define UNITNAMESZ 8
typedef struct {
    int alive;      /* == 1 if disk alive, zero otherwise */
    int unit;       /* unit number */
    char unitName[UNITNAMESZ];
    unsigned long xfer;     /* number of transfers */
    unsigned long nreads;   /* number of reads */
    unsigned long nwrites;  /* number of writes */
    unsigned long readblks; /* number of 512 byte blocks read */
    unsigned long writeblks;
} mlKdStatT;

typedef dStatT mlVolCountersT;

enum mlDeFlags { ML_DEF_NONE=0, ML_DEF_META=1, ML_DEF_DATA=2, 
    ML_DEF_READ=4, ML_DEF_WRITE=8, ML_DEF_REPEAT=16, ML_DEF_LOG=32 };

typedef struct mlVolProperties {
    mlServiceClassT serviceClass; /* service class provided */
    u32T bmtXtntPgs;              /* number of pages per BMT extend */
    char volName[ML_VOL_NAME_SZ];
} mlVolPropertiesT;

typedef struct mlVolIoQParams {
    int rdMaxIo;            /* max blocks that can be read/written  */
    int wrMaxIo;            /* in a consolidated I/O */
    int qtoDev;             /* max number of I/O's to be queued to dev */
    int consolidate;        /* whether should consolidate for disk */
    int blockingFact;       /* rm from blockingQ before rm from lazyQ */
    int lazyThresh;         /* min bufs on lazy q before driver called */
    int max_iosize_rd;
    int max_iosize_wr;
    int preferred_iosize_rd;
    int preferred_iosize_wr;
} mlVolIoQParamsT;

typedef struct mlVolInfo {
    mlIdT vdMntId;                /* last mount id */
    u32T volIndex;                /* vol index within file system */
    u32T volSize;                 /* size in blocks */
    u32T stgCluster;              /* number of blocks each bit represents */
    u32T freeClusters;            /* total number of free clusters */
    u32T totalClusters;           /* total number of clusters */
    u32T maxPgSz;                 /* largest possible page size on vol */
} mlVolInfoT;

typedef enum {
    BF_SAFETY_NIL,      /* no bitfile specific safety reqmt */
    BF_SAFETY_NO_NWR,   /* don't maintain NWR */
    BF_SAFETY_AGENT,    /* managed by an ftx agent */
    BF_SYNC_WRITE       /* force all writes to be synchronous */
} mlBfDataSafetyT;

typedef enum {
    XMT_NIL,            /* no extents (allocation) yet */
    XMT_SIMPLE,         /* no special mapping formula */
    XMT_MIRROR_UNSUPPORTED, /* mirrored/shadowed file; currently unsupported */
    XMT_STRIPE          /* consecutive runs on alternate disks */
} mlBfXtntMapTypeT;

typedef struct mlStripeParams {
    i32T segmentCnt;            /* number of stripe segments */
    i32T segmentSize;           /* segment size in blocks */
} mlStripeParamsT;

typedef struct mlMirrorParams {
    i32T mirrorCnt;             /* number of mirrors */
} mlMirrorParamsT;

#define ML_CLIENT_AREA_SZ 4

typedef struct mlBfAttributes {
    i32T version;               /* version of this data structure */
    mlBfDataSafetyT dataSafety; /* bitfile data safety */
    mlServiceClassT reqService; /* required service class */
    mlServiceClassT optService; /* optional service class */
    i32T initialSize;           /* initial allocation in blocks */
    i32T extendSize;            /* add'l extend size in blocks */
    mlBfXtntMapTypeT mapType;   /* storage mapping type */
    union {
        mlStripeParamsT stripe; /* stripe specific parameters */
        mlMirrorParamsT mirror; /* mirror specific parameters */
    } attr;
    i32T bfFlags;
    i32T fill2;
    i32T clientArea[ML_CLIENT_AREA_SZ]; /* user/client-specific area */
    mlBfTagT acl;               /* rsvd for ACL */
    i32T rsvd_sec1;             /* rsvd for security stuff */
    i32T rsvd_sec2;             /* rsvd for security stuff */
    i32T rsvd_sec3;             /* rsvd for security stuff */
    i32T rsvd_sec4;             /* rsvd for security stuff */
    i32T fill3;
    i32T fill4;
} mlBfAttributesT;

typedef struct mlBfInfo {
    u32T pageSize;                /* page size in 512 byte blocks */
    u32T numPages;                /* number of pages allocated */
    u32T nextPage;                /* next page to allocate */
    u32T fragOff;                 /* Page where frag is */
    u32T volIndex;                /* primary mcell vol */
    mlBfTagT tag;                 /* bitfile tag */
    u32T cloneId;                 /* 0 ==> orig; "> 0" ==> clone */
    u32T cloneCnt;                /* set's clone cnt last time bf changed */
    mlbfFragIdT fragId;           /* frag Id */
} mlBfInfoT;


typedef bcStatT mlBcStatT;

/* changed ML_BSR_MAX to BSR_API_MAX from BSR_MAX.
 * This change is required to eliminate the potential for apps
 * who reference ML_BSR_MAX to index bmt records beyond record number 21.
 * In the rare situation of a cluster in mid rolling-upgrade, the potential
 * for two nodes to have different definitions of mlDmnParamsT exists.
 * The structure mlBmtStatT, which is a bmtStatT had the potential to increase!  * in size with the addition of new bmt record types.  This caused user apps
 * to break due to a mismatch in the API structure size.
 */
#define ML_BSR_MAX BSR_API_MAX   /*   !!!!! see above comment  !!!!!   */

typedef bmtStatT mlBmtStatT;

typedef logStatT mlLogStatT;

typedef struct {
    mlBfDomainIdT bfDomainId;           /* domain ID */
    u32T maxVols;                       /* maximum vol index */
    mlBfTagT bfSetDirTag;               /* tag of root bitfile-set tag dir */
    mlBfTagT ftxLogTag;                 /* tag of domain log */
    u32T ftxLogPgs;                     /* number of pages in the log */
    u32T curNumVols;                    /* current number of VOLs */
    uid_t uid;                          /* domain's owner */
    gid_t gid;                          /* domain's group */
    mode_t mode;                        /* domain's access permissions */
    char domainName[ML_DOMAIN_NAME_SZ]; /* domain name */
    mlBcStatT mlBcStat;                 /* buffer cache stats */
    mlBmtStatT mlBmtStat;               /* per domain BMT stats */
    mlLogStatT mlLogStat;               /* per domain LOG stats */
    int dmnVersion;                     /* domain's on-disk version number */
} mlDmnParamsT;

typedef struct {
    mlDmnParamsT *dmnParams;
    u32T *volIndex;
    mlVolCountersT *volCounters;
} mlDmnStatT;

struct dmnStatH {
    int len;
    mlBfDomainIdT *dmnIdp;
    mlDmnStatT **dmnInitStp;
    mlDmnStatT **dmnStp;
};
typedef struct dmnStatH dmnStatHT;

/*
 * This definition serves the same purpose as the
 * definition FIRST_XTNT_IN_PRIM_MCELL but it is
 * for the use of the utilities which typically do
 * not include bs_ods.h, the header file where
 * FIRST_XTNT_IN_PRIM_MCELL is defined.
 */
#define UTILS_FIRST_XTNT_IN_PRIM_MCELL_VERSION 4

/*
 * This macro serves the same purpose as the macro 
 * FIRST_XTNT_IN_PRIM_MCELL but it is for the use of
 * the utilities.  The utilities cannot use the
 * FIRST_XTNT_IN_PRIM_MCELL macro because the values
 * of mlBfXtntMapTypeT (the list of file types used by
 * the utilities) and bsXtntMapType (the list of file
 * types used by the kernel) don't correlate.  For 
 * example, in the kernel, a non-striped file has type
 * BSXMT_APPEND (== 0) but in the utilities space,
 * a non-striped file has type XMT_SIMPLE (== 1).
 * Therefore, we need this macro.
 */
#define UTILS_FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, mapType) \
    ((dmnVersion >= UTILS_FIRST_XTNT_IN_PRIM_MCELL_VERSION) && \
    (((mlBfXtntMapTypeT)(mapType)) == XMT_SIMPLE))

/* quota status */

#define Q_STS_QUOTA_ON           0x0001    /* some quota type is active */
#define Q_STS_QUOTA_SYNC         0x0002    /* set when quota sync in progress */
#define Q_STS_USR_QUOTA_MT       0x0004    /* enable maintaining user quotas */
#define Q_STS_GRP_QUOTA_MT       0x0008    /* enable maintaining group quotas */
#define Q_STS_USR_QUOTA_EN       0x0010    /* enable enforcing user quotas */
#define Q_STS_GRP_QUOTA_EN       0x0020    /* enable enforcing group quotas */
#define Q_STS_FS_USR_QUOTA       0x0040    /* fileset quotas from usr quotas */
#define Q_STS_FS_GRP_QUOTA       0x0080    /* fileset quotas from grp quotas */
#define Q_STS_FS_QUOTA_MT        0x0100    /* track size of fileset */
#define Q_STS_FS_QUOTA_EN        0x0200    /* enforce fileset quotas */
#define Q_STS_LARGE_LIMITS       0x0400    /* set for 8 byte quota fields */

typedef bfSetParamsT mlBfSetParamsT;
typedef bsExtentDescT mlExtentDescT;

#define ML_MAX_LOCKS 100

typedef struct mlAdvfsLockUsageStats {
    unsigned long lock;
    unsigned long wait;
    unsigned long reWait;
    unsigned long signal;
    unsigned long broadcast;
} mlAdvfsLockUsageStatsT;

typedef struct mlAdvfsLockStats {
    unsigned long mutexLock;
    unsigned long mutexUnlock;
    unsigned long wait;
    unsigned long signal;
    unsigned long broadcast;
    unsigned long genLock;
    unsigned long genWait;
    unsigned long genReWait;
    unsigned long genSignal;
    unsigned long genBroadcast;
    unsigned long stateLock;
    unsigned long stateWait;
    unsigned long stateReWait;
    unsigned long stateSignal;
    unsigned long stateBroadcast;
    unsigned long shrLock;
    unsigned long shrWait;
    unsigned long shrReWait;
    unsigned long shrSignal;
    unsigned long excLock;
    unsigned long excWaitExc;
    unsigned long excWaitShr;
    unsigned long excReWaitExc;
    unsigned long excReWaitShr;
    unsigned long excSignal;
    unsigned long excBroadcast;
    unsigned long bufWait;
    unsigned long bufReWait;
    unsigned long bufSignal;
    unsigned long bufBroadcast;
    unsigned long pinBlockWait;
    unsigned long pinBlockReWait;
    unsigned long pinBlockSignal;
    unsigned long pinBlockBroadcast;
    unsigned long bfFlushWait;
    unsigned long bfFlushReWait;
    unsigned long bfFlushSignal;
    unsigned long bfFlushBroadcast;
    unsigned long ftxWait;
    unsigned long ftxReWait;
    unsigned long ftxSignal;
    unsigned long ftxBroadcast;
    unsigned long msgQWait;
    unsigned long msgQReWait;
    unsigned long msgQSignal;
    unsigned long msgQBroadcast;
    mlAdvfsLockUsageStatsT usageStats[ML_MAX_LOCKS];
} mlAdvfsLockStatsT;

typedef struct mlFileSetStats {
    unsigned long    msfs_lookup;

    struct {
        unsigned long hit;
        unsigned long hit_not_found;
        unsigned long miss;
    } lookup;

    unsigned long    msfs_create;
    unsigned long    msfs_mknod;
    unsigned long    msfs_open;
    unsigned long    msfs_close;
    unsigned long    msfs_access;
    unsigned long    msfs_getattr;
    unsigned long    msfs_setattr;
    unsigned long    msfs_read;
    unsigned long    msfs_write;
    unsigned long    msfs_mmap;
    unsigned long    msfs_fsync;
    unsigned long    msfs_seek;
    unsigned long    msfs_remove;
    unsigned long    msfs_link;
    unsigned long    msfs_rename;
    unsigned long    msfs_mkdir;
    unsigned long    msfs_rmdir;
    unsigned long    msfs_symlink;
    unsigned long    msfs_readdir;
    unsigned long    msfs_readlink;
    unsigned long    msfs_inactive;
    unsigned long    msfs_reclaim;
    unsigned long    msfs_page_read;
    unsigned long    msfs_page_write;
    unsigned long    msfs_getpage;
    unsigned long    msfs_putpage;
    unsigned long    msfs_bread;
    unsigned long    msfs_brelse;
    unsigned long    msfs_lockctl;
    unsigned long    msfs_setvlocks;
    unsigned long    msfs_syncdata;
} mlFileSetStatsT;

typedef struct mlFragDesc {
    bfTagT   ssFragTag;
    bfSetIdT ssFragBfSetId;
    uint32T  ssFragRatio;
    uint32T  ssXtntCnt;
} mlFragDescT;

typedef struct mlHotDesc {
    bfTagT   ssHotTag;
    bfSetIdT ssHotBfSetId;
    int      ssHotPlaced;
    int      ssHotVdi;
    uint32T  ssHotIOCnt;
    time_t   ssHotLastUpdTime;
    time_t   ssHotEntryTime;
    time_t   ssHotErrTime;
} mlHotDescT;

typedef struct mlSSDmnParams {
    time_t ssFirstMountTime;
    /* UI visable configurable options */
    uint32T ssDmnDefragment;
    uint32T ssDmnSmartPlace;
    uint32T ssDmnBalance;
    uint32T ssDmnVerbosity;
    uint32T ssDmnDirectIo;
    uint32T ssMaxPercentOfIoWhenBusy;
    uint32T ssFilesDefraged;
    uint32T ssPagesDefraged;
    uint32T ssPagesBalanced;
    uint32T ssFilesIOBal;
    uint32T ssExtentsConsol;
    uint32T ssPagesConsol;
    /* UI hidden configurable options */
    uint32T ssAccessThreshHits;
    uint32T ssSteadyState;
    uint32T ssMinutesInHotList;
    uint32T ssReserved0;
    uint32T ssReserved1;
    uint32T ssReserved2;
} mlSSDmnParamsT;

typedef enum {   /* Important::keep same order as bs_ods.h; ssDmnOpT; */
    ML_SS_DEACTIVATED,
    ML_SS_ACTIVATED,
    ML_SS_SUSPENDED,
    ML_SS_ERROR,
    ML_SS_CFS_RELOC,
    ML_SS_CFS_UMOUNT,
    ML_SS_CFS_MOUNT
} mlSSDmnOpsT;


typedef struct removeName {
    int  dirFd;
    char *name;
} opRemoveName;

typedef struct removeBf {
    ino_t ino;
    int dirFd;
    int flag;
} opRemoveBf;

typedef struct SetRename {
    char *domain;               /* in - domain name */
    char *origSetName;          /* in - set's orig name */
    char *newSetName;           /* in - set's new name */
} opSetRenameT;

typedef struct switchLog {
    char *domainName;           /* in - domain name */
    u32T volIndex;              /* in - volume containing log */
    u32T logPgs;                /* in - new log size, if > 0 */
    mlServiceClassT logSvc;     /* in - new log service class, if != 0 */
} opSwitchLog;

typedef struct switchRootTagDir {
    char *domainName;
    u32T volIndex;
} opSwitchRootTagDirT;

typedef struct _opEvent {
    int action;               /* in - flag: 0 == clear event, 1 == post event */
} opEvent;

typedef struct {
    char *mp;
    char *name;         
    mlBfTagT tag;
    mlBfTagT parentTag;
} opGetName;

typedef struct {
    char *dirName;                 /* in - target directory name */
    mlBfTagT undelDirTag;          /* out - trashcan directory tag */
} opUndelGet;

typedef struct {
    char *dirName;                 /* in - target directory name */
    char *undelDirName;            /* in - trashcan directory name */
} opUndelAttach;

typedef struct {
    char *dirName;                 /* in - target directory name */
} opUndelDetach;

typedef struct {
    char *domain;                  /* in - domain name */
    char *setName;                 /* in - set's name */
    mlServiceClassT reqServ;       /* in - required service class */
    mlServiceClassT optServ;       /* in - optional service class */
    u32T fsetOptions;              /* in - fileset options */
    gid_t quotaId;                 /* in - ID for quota files */
    mlBfSetIdT  bfSetId;           /* out - bitfile set id */
} opSetCreateT;

typedef struct {
    char *domain;                  /* in - domain name */
    char *setName;                 /* in - set's name */
    mlBfSetIdT  bfSetId;           /* out - bitfile set id */
} opSetGetIdT;

typedef struct {
    char *domain;                  /* in - domain name */
    char *setName;                 /* in - set's name */
    mlFileSetStatsT fileSetStats;  /* out */
} opSetGetStatsT;

typedef struct {
    char *domain;                  /* in - domain name */
    char *setName;                 /* in - set's name */
} opSetDeleteT;

typedef struct {
    char *domain;                  /* in - domain name */
    char *origSetName;             /* in - orig set's name */
    char *cloneSetName;            /* in - clone set's name */
    mlBfSetIdT cloneSetId;         /* out - clone's id */
} opSetCloneT;

typedef struct {
    char           *domain;        /* in - domain name */
    u32T           nextSetIdx;     /* in/out */
    int            flag;           /* in - flag, 1 = skip dmn recovery */
    mlBfSetParamsT bfSetParams;    /* out */
    u32T           userId;         /* out */
} opSetGetInfoT;


typedef struct {
    int            fd;             /* in - file descriptor */ 
    mlBfAttributesT bfAttributes;  /* in/out - bitfile attributes */
    mlBfInfoT      bfInfo;         /* out - bitfile info */
} opGetBfParamsT;

typedef struct {
    int            fd;             /* in - file descriptor */
    mlBfAttributesT bfAttributes;  /* in - bitfile attributes */
} opSetBfAttributesT;

typedef struct {
    int            fd;             /* in - file descriptor */
    mlBfAttributesT bfAttributes;  /* in/out - bitfile attributes */
} opGetBfIAttributesT;

typedef struct {
    int            fd;             /* in - file descriptor */
    mlBfAttributesT bfAttributes;  /* in - bitfile attributes */
} opSetBfIAttributesT;

typedef struct {
    int fd;                        /* in - file descriptor */
} opMoveBfMetadataT;
 
typedef struct {
    mlBfDomainIdT bfDomainId;      /* in - domain id */
    u32T         volIndex;         /* in - volume index */
    mlVolInfoT volInfo;            /* out - volume information */
    mlVolPropertiesT volProperties;/* out - volume properties */
    mlVolIoQParamsT volIoQParams;  /* out - volume i/o queue parameters */
    mlVolCountersT volCounters;    /* out - volume stats (dStatT) */
    int flag;                      /* in/out - flags, see below */
} opGetVolParamsT;
/* 
 * Bit definitions for flags field in opGetVolParamsT.
 */
#define GETVOLPARAMS_NO_BMT     0x01    /* don't fetch bmt data   */
#define GETVOLPARAMS2_CHVOL_OP_A 0x02   /* ignore service class */
#define GETVOLPARAMS2           0x04    /* get stats for interval */

typedef struct {
    mlBfDomainIdT bfDomainId;      /* in - domain id */
    u32T volIndex;                 /* in - volume index */
    int bfDescSize;                /* in - size of array */
    mlBfDescT *bfDesc;             /* in/out - array of bitfile descriptors */
    u32T nextBfDescId;             /* in/out - state checkpoint */
    int bfDescCnt;                 /* out - actual # of descriptors returned */
} opGetVolBfDescsT;

typedef struct {
    mlBfDomainIdT bfDomainId;      /* in - domain id */
    u32T volIndex;                 /* in - volume index */
    mlVolIoQParamsT volIoQParams;  /* in - volume i/o queue parameters */
} opSetVolIoQParamsT;

typedef struct {
    mlBfDomainIdT bfDomainId;      /* out - domain id */
    char *domain;                  /* in - bf domain name */
    char *volName;                 /* block special device name */
    mlServiceClassT  volSvc;       /* out - service class */
    u32T volSize;                  /* size of the volume */
    u32T bmtXtntPgs;               /* number of pages per BMT extent */
    u32T bmtPreallocPgs;           /* number of pages to be preallocated for the BMT */
    u32T volIndex;                 /* out - volume index */
} opAddVolT;

typedef struct {
    mlBfDomainIdT bfDomainId;
    u32T          volIndex;
    u32T          forceFlag;
} opRemVolT;

typedef struct {
    char *domainName;              /* in - domain name */
    u32T volIndex;                 /* in - volume index */
    mlServiceClassT volSvc;        /* in - volume service class */
    u32T action;                   /* in - flag: 0 == add, 1/else == remove */
} opAddRemVolSvcClassT;

typedef struct {
    char *domainName;              /* in - domain name */
    char *volName;                 /* in - volume name */
    mlVdOpT op;                    /* in - addvol or rmvol? */
} opAddRemVolDoneT;

typedef struct {
    int fd;                        /* in - file descriptor */
    u32T curVolIndex;              /* in - current volume index */
    u32T pageCntNeeded;            /* in - page count needed */
    int forceFlag;                 /* in - disregard stripe optimization */
} opSetBfNextAllocVolT;

typedef struct {
    int           fd;              /* in - file descriptor */
    int           startXtntMap;    /* in - extent map at which to start */
    int           startXtnt;       /* in - extent at which to start */
    int           xtntArraySize;   /* in - size of extent array */
    mlExtentDescT *xtntsArray;     /* out - array to copy into */
    int           xtntCnt;         /* out - number of extents copied */
    u32T          allocVolIndex;   /* out - disk on which stg is alloc'ed */
} opGetBfXtntMapT;

typedef struct {
    int           fd;              /* in - file descriptor */
    int           startXtntMap;    /* in - extent map at which to start */
    int           startXtnt;       /* in - extent at which to start */
    int           xtntArraySize;   /* in - size of extent array */
    mlExtentDescT *xtntsArray;     /* out - array to copy into */
    int           xtntCnt;         /* out - number of extents copied */
    u32T          allocVolIndex;   /* out - dsk on which stg alloc'ed(stripes)*/
    u32T          pg_size;         /* out - underlying file's page size */
    ulong         file_size;       /* out - file size in bytes */
    int           has_frag;        /* out - TRUE or FALSE if file has a frag */
    int           volMax;          /* out - max volumes per domain */
    bsDevtListT   *devtvec;        /* out - vector of dev_ts  */
} opGetCludioXtntMapT;

typedef struct {
    mlBfDomainIdT bfDomainId;      /* in - domain id */
    mlDmnParamsT dmnParams;        /* out - domain params */
} opGetDmnParamsT;

typedef struct {
    char *domain;                  /* in - domain name */
    mlDmnParamsT dmnParams;        /* out - domain params */
} opGetDmnNameParamsT;

typedef struct {
    mlBfSetIdT     bfSetId;        /* in - fileset id */
    mlBfSetParamsT bfSetParams;    /* out - fileset params */
} opGetBfSetParamsT;

typedef struct {
    mlBfSetIdT     bfSetId;        /* in - fileset id */
    mlBfSetParamsT bfSetParams;    /* in - fileset params */
} opSetBfSetParamsT;

typedef struct {
    char *dmnName;
    mlBfSetParamsT bfSetParams;    /* in - fileset params */
} opSetBfSetParamsActivateT;

typedef struct {
    mlBfDomainIdT bfDomainId;      /* in - domain id */
    int volIndexArrayLen;          /* in - number of ints in array */
    u32T *volIndexArray;           /* out - list of volume indices */
    int numVols;                   /* out - num volumes put in array */
} opGetDmnVolListT;

typedef struct {
    char          *domain;         /* in - bf domain name */
    int maxVols;                   /* in - maximum number of virtual disks */
    u32T logPgs;                   /* in - number of pages in log */
    mlServiceClassT logSvc;        /* in - log service attributes */
    mlServiceClassT tagSvc;        /* in - tag directory service attributes */
    char *volName;                 /* in - block special device name */
    mlServiceClassT volSvc;        /* in - service class */
    u32T volSize;                  /* in - size of the virtual disk */
    u32T bmtXtntPgs;               /* in - number of pages per BMT extent */
    u32T bmtPreallocPgs;           /* in - number of pages to be
                                           preallocated for the BMT */
    u32T domainVersion;            /* in - on-disk version for domain */
    mlBfDomainIdT bfDomainId;      /* out - domain id */
} opDmnInitT;

typedef struct {
    int            fd;             /* in - file descriptor */
    u32T           srcVolIndex;    /* in - source volume of pages to migrate */
    u32T           srcPageOffset;  /* in - page to migrate */
    u32T           srcPageCnt;     /* in - number of pages to migrate */
    u32T           dstVolIndex;    /* in - dest. volume of pages to migrate */
    u32T           dstBlkOffset;   /* in */
    u32T           forceFlag;      /* in */
} opMigrateT;

typedef struct {
    mlBfSetIdT setId;              /* in - bitfile set tag */
    mlBfTagT  tag;                 /* in/out - tag of this bitfile */
    uid_t uid;                     /* out - user ID */
    gid_t gid;                     /* out - group ID */
    off_t size;                    /* out - allocated file size in bytes */
    time_t atime;                  /* out - access time */
    mode_t mode;                   /* out - file mode */
} opTagStatT;

typedef struct {
    int fd;                        /* in - file descriptor */
    int xtntMapIndex;              /* in */
} opRewriteXtntMapT;


typedef struct {
    mlBfDomainIdT bfDomainId;      /* in - domain id */
    u32T volIndex;                 /* in - volume index */
    int scanStartCluster;          /* in - new free space pointer */
} opResetFreeSpaceCacheT;

typedef struct {
    mlBfSetIdT  bfSetId;           /* in - bitfile set id */
    mlBfTagT    bfTag;             /* in - tag value to use */
} opSetNextTagT;

/*
 * A general purpose structure.  Add fields as desired 
 * to get system-wide AdvFS information.
 */
typedef struct {
    ulong NumAccess;               /* access structures in system */
    ulong FreeAccLen;              /* access structures on free list */
    ulong ClosedAccLen;            /* access structures on closed list */
    ulong AccAlloc;                /* number allocated by cleanup thread */
    ulong AccDealloc;              /* number deallocated by cleanup thread */
    ulong AccDeallocFail;          /* number of deallocation failures */
    ulong CleanupClosed;           /* calls to cleanup_closed_list() */
    ulong CleanupSkipped;          /* bfaps skipped and processed */
    ulong CleanupProcessed;        /* in cleanup_closed_list() */        
} opGetGlobalStatsT;

typedef opGetGlobalStatsT mlGetGlobalStatsT;

typedef struct {
    mlBfDomainIdT bfDomainId;         /* in */
    u32T          volIndex;           /* in */
    u32T          smsyncQ_cnt;        /* out - count of bufs added onto queues */
    u32T          smsync[SMSYNC_NQS]; /* out - snapshot of queue lengths */
} opGetVolSmsyncT;

typedef struct {
    char* domainName;                  /* in - domain name */
    int type;                          /* in - operation type */
    mlSSDmnOpsT ssDmnCurrentState;     /* out - domain state */
    int ssRunningState;                /* out - run or stop */
} opSSDmnOpsT;

typedef struct {
    char* domainName;                  /* in - domain id */
    mlSSDmnParamsT ssDmnParams;        /* out - domain params */
} opSSGetParamsT;

typedef struct {
    char* domainName;                  /* in - domain name */
    mlSSDmnParamsT ssDmnParams;        /* in - set the domain params */
} opSSSetParamsT;

typedef struct {
    char*         domainName;          /* in */
    vdIndexT      volIndex;            /* in */
    int           ssStartFraglist;     /* in */
    int           ssFragArraySize;     /* in */
    mlFragDescT   *ssFragArray;        /* out */
    int           ssFragCnt;           /* out */
} opSSGetFraglistT;

typedef struct {
    mlBfDomainIdT bfDomainId;          /* in */
    int           ssStartHotlist;      /* in */
    int           ssHotArraySize;      /* in */
    mlHotDescT    *ssHotArray;         /* out */
    int           ssHotCnt;            /* out */
} opSSGetHotlistT;

typedef struct timeval mlBsIdT;           /* unique identifier, 64 bits */
                                        /* tmp - should be real uuid */
typedef u16T mlVdIndexT;

/****************************************************************/
/**** MSFS_RESERVED_BLKS defines the first block on       *******/
/**** a disk that MegaSafe can use.  Given that the       *******/
/**** first 16 blocks of a disk contain the disk label    *******/
/**** and the UFS boot blocks, this should be set at      *******/
/**** least to 16.                                        *******/
/****                                                     *******/
/**** Blks              Usage                             *******/
/**** ---------------   -------------------------------   *******/
/****  0 to 15          Disk label, UFS boot blocks       *******/
/**** 16 to 19          Fake super block                  *******/
/**** 20 to 31          Unused                            *******/
/**** 64 to 95          Real boot blocks (MISC rsvd mcell *******/
/****************************************************************/

#ifndef MSFS_RESERVED_BLKS
#define MSFS_RESERVED_BLKS 32
#endif

typedef enum { 
    ML_BSR_VD_VIRGIN,      /* The vd has not been mounted */
    ML_BSR_VD_MOUNTED,     /* The vd is mounted or was left mounted */
    ML_BSR_VD_DISMOUNTED,  /* The vd has been dismounted cleanly */
    ML_BSR_VD_DMNT_INPROG, /* Dual mount in progress in this domain */
    ML_BSR_VD_ZOMBIE       /* vd is being removed but is only mostly-dead. */
} mlBsVdStatesT;

/*
 * bfMCIdT - bitfile mcell id.
 *  = BMT page number and cell number (address within a virtual disk)
 */

typedef struct mlBfMCId {
    u32T cell : 5;          /* Cell number within page */
    u32T page : 27;         /* Page number */
} mlBfMCIdT;

/*
 * mlBsMRT - mcell record header
 *
 * Each record in an mcell has preceeded by this record header.
 *
 * Note that records must start on an integer boundary.  The bCnt
 * in each record header is not rounded up but whenever it is used
 * to calculate the location of the next record header then it is
 * rounded up to the next integer boundary.
 */

typedef struct mlBsMR {
    u32T bCnt    : 16;   /* Count of bytes in record */
    u32T type    :  8;   /* Type of structure contained by record */
    u32T version :  8;   /* Version of the record's type */
} mlBsMRT;

#ifndef BSC_R_SZ
#define BSC_R_SZ \
    (292 - sizeof(mlBfMCIdT) - (2*sizeof(mlBfTagT)) - (2*sizeof(u16T)))
#endif

/*
 * mlBsMCT
 *
 * Bitfile metadata cell (mcell).  Used to store bitfile metadata
 * like attributes, extent maps, etc.  The metadata is stored in
 * typed records which are kept in the 'bsMR0' byte array.
 */

typedef struct mlBsMC {
    mlBfMCIdT nextMCId;           /* Link to next mcell */
    u16T nextVdIndex;        /* vd index of next mcell */
    u16T linkSegment;        /* segment in link, starts at zero */
    mlBfTagT tag;                 /* Tag this mcell is assigned to */
    mlBfTagT bfSetTag;            /* tag of this bitfile's bf set dir */
    char bsMR0[BSC_R_SZ];       /* Records */
} mlBsMCT;

#ifndef BSR_VD_ATTR
#define BSR_VD_ATTR 3           /* Vd attributes */
#endif

#ifndef BMT_PG_SZ
#define BMT_PG_SZ 16
#endif

#ifndef BS_BLKSIZE
#define BS_BLKSIZE           512
#endif

#ifndef BSPG_CELLS
#define BSPG_CELLS \
  ((ADVFS_PGSZ - (3 * sizeof(u32T) + sizeof(mlBfMCIdT))) / sizeof(mlBsMCT))
#endif

typedef struct mlBsMPg {
    mlBfMCIdT nextfreeMCId;              /* Next free MCId on the page */
    u32T nextFreePg;                /* Next page in the mcell free list */
    u32T freeMcellCnt;              /* Number of free mcells on this pg */
    u32T pageId : 27;               /* Page number */
    u32T megaVersion: 5;            /* Overall structure version */
    struct mlBsMC mlBsMCA[BSPG_CELLS];     /* Array of Bs Cells */
} mlBsMPgT;

typedef mlAdvfsLockStatsT opGetLockStatsT;
typedef struct {
    int           fd;             /* in - file descriptor */ 
    int           startXtntMap;   /* in - extent map at which to start */
    int           startXtnt;      /* in - extent at which to start */
    int           xtntArraySize;  /* in - size of extent array */
    mlExtentDescT *xtntsArray;    /* out - merged array */
    int           xtntCnt;        /* in/out - num xtnt entries for merged map */
    u32T          allocVolIndex;  /* out - disk on which next stg is alloc'ed */
    int           num_pages;      /* out - count of pages in merged map */
} opGetBkupXtntMapT;

typedef union {
    opGetLockStatsT   getLockStats;

    /* Bitfile Functions */
    opGetBfParamsT    getBfParams;
    opSetBfAttributesT setBfAttributes;
    opGetBfIAttributesT getBfIAttributes;
    opSetBfIAttributesT setBfIAttributes;
    opMoveBfMetadataT moveBfMetadata;
    opGetBfXtntMapT   getBfXtntMap;
    opTagStatT        tagStat;
    opGetBkupXtntMapT getBkupXtntMap;
    opGetCludioXtntMapT getCludioXtntMap;

    /* File Set Functions */
    opSetCreateT      setCreate;
    opSetDeleteT      setDelete;
    opSetCloneT       setClone;
    opSetGetInfoT     setGetInfo;
    opSetGetIdT       setGetId;
    opSetGetStatsT    setGetStats;
    opGetBfSetParamsT getBfSetParams;
    opSetBfSetParamsT setBfSetParams;
    opSetBfSetParamsActivateT setBfSetParamsActivate;

    /* File Domain and Volume Functions */
    opDmnInitT        dmnInit;
    opGetDmnParamsT   getDmnParams;
    opGetDmnVolListT  getDmnVolList;
    opGetDmnNameParamsT   getDmnNameParams;
    opGetVolParamsT   getVolParams;
    opGetVolBfDescsT   getVolBfDescs;
    opSetVolIoQParamsT   setVolIoQParams;
    opAddVolT         addVol;
    opRemVolT         remVol;
    opSwitchLog       switchLog;
    opSwitchRootTagDirT switchRootTagDir;
    opAddRemVolSvcClassT  addRemVolSvcClass;
    opMigrateT        migrate;
    opSetBfNextAllocVolT setBfNextAllocVol;
    opGetVolSmsyncT   getVolSmsync;

    opUndelAttach     undelAttach;
    opUndelDetach     undelDetach;
    opUndelGet        undelGet;
    opGetName         getName;
    opEvent           event;
    opRewriteXtntMapT rewriteXtntMap;
    opResetFreeSpaceCacheT  resetFreeSpaceCache;
    opSetNextTagT     setNextTag;
    opRemoveName      removeName;
    opRemoveBf        removeBf;
    opSetRenameT      setRename;
    opGetGlobalStatsT getGlobalStats;
    opAddRemVolDoneT  addRemVolDone;

    /* vfast Functions */
    opSSDmnOpsT      ssDmnOps;
    opSSGetParamsT   ssGetParams;
    opSSSetParamsT   ssSetParams;
    opSSGetFraglistT ssGetFraglist;
    opSSGetHotlistT  ssGetHotlist;
} libParamsT;

#ifndef KERNEL

static mlBfDomainIdT nilMlBfDomainIdT = { 0 };

static mlTrFlagsT MlTrValues[ML_TR_FLAGS] = {
    tNone, tAll,
    tAccess, tClose, tCreate,
    tMutex, tCond, tLock,
    tFtx, tFtxPP, tMem,
    tRef, tPin, 
    tDevRd, tDevWr
};

static char *MlTrNames[ML_TR_FLAGS] = { 
    "none", "all",
    "access", "close", "create",
    "mutex", "cond", "lock",
    "ftx", "ftxpp", "memory",
    "ref", "pin", 
    "devrd", "devwr"
};

static const mlDmnParamsT NilDmnParams = { 0 };
static const mlBfSetParamsT mlNilBfSetParams;

mlStatusT
msfs_syscall ( 
              opTypeT       opType,
              libParamsT    *parmBuf,
              int           parmBufLen
              );

/*
 * File Set Functions
 */

mlStatusT
msfs_fset_create( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    mlServiceClassT reqServ,  /* in - required service class */
    mlServiceClassT optServ,  /* in - optional service class */
    uint32T fsetOptions,      /* in - fileset options */
    gid_t quotaId,            /* in - group ID for quota files */
    mlBfSetIdT *bfSetId       /* out - bitfile set id */
    );

mlStatusT
msfs_fset_get_id( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    mlBfSetIdT *bfSetId       /* out - bitfile set id */
    );

mlStatusT
msfs_fset_delete( 
    char *domain,             /* in - domain name */
    char *setName             /* in - set's name */
    );

mlStatusT
msfs_fset_clone(
    char *domain,             /* in - domain name */
    char *origSetName,        /* in - set's name */
    char *cloneSetName,       /* in - clone's name */
    mlBfSetIdT *cloneSetId    /* out - clone's id */
    );

mlStatusT
msfs_fset_get_info(
    char           *domain,             /* in */
    u32T           *nextSetIdx,         /* in/out */
    mlBfSetParamsT *bfSetParams,        /* out */
    u32T           *userId,             /* out */
    int            flag                 /* in, 1 = skip dmn recovery */
    );

mlStatusT 
msfs_get_bfset_params(
    mlBfSetIdT bfSetId,
    mlBfSetParamsT *bfSetParams
    );

mlStatusT 
msfs_set_bfset_params(
    mlBfSetIdT bfSetId,
    mlBfSetParamsT *bfSetParams
    );

mlStatusT
msfs_fset_rename(
    char *domain,             /* in - domain name */
    char *origSetName,        /* in - original bfset name */
    char *newSetName          /* in - new bfset name */
    );

/* 
 * File Domain and Volume Functions
 */


mlStatusT
advfs_get_lock_stats( mlAdvfsLockStatsT *lockStats );

mlStatusT
advfs_fset_get_stats( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    mlFileSetStatsT *fileSetStats
 );

mlStatusT
msfs_dmn_init(
    char* domain,               /* in - bf domain name */
    int maxVols,                /* in - maximum number of virtual disks */
    u32T logPgs,                /* in - number of pages in log */
    mlServiceClassT logSvc,     /* in - log service attributes */
    mlServiceClassT tagSvc,     /* in - tag directory service attributes */
    char *volName,              /* in - block special device name */
    mlServiceClassT volSvc,     /* in - service class */
    u32T volSize,               /* in - size of the virtual disk */
    u32T bmtXtntPgs,            /* in - number of pages per BMT extent */
    u32T bmtPreallocPgs,        /* in - number of pages to be preallocated for the BMT */
    u32T domainVersion,         /* in - on-disk version of domain */
    mlBfDomainIdT* bfDomainId   /* out - domain id */
    );

mlStatusT 
msfs_get_dmn_params(
    mlBfDomainIdT bfDomainId,
    mlDmnParamsT *dmnParams
    );

mlStatusT 
msfs_get_dmnname_params(
    char *domain,
    mlDmnParamsT *dmnParams
    );

mlStatusT
msfs_get_dmn_vol_list(
    mlBfDomainIdT bfDomainId,
    int volIndexArrayLen,    /* in - number of ints in array */
    u32T volIndexArray[],    /* out - list of volume indices */
    int *numVols             /* out - num volumes put in array */
    );

mlStatusT 
advfs_get_vol_params (
    mlBfDomainIdT bfDomainId,
    u32T volIndex,
    mlVolInfoT* volInfop,
    mlVolPropertiesT* volPropertiesp,
    mlVolIoQParamsT* volIoQParamsp,
    mlVolCountersT* volCountersp
    );

mlStatusT 
advfs_get_vol_bf_descs (
    mlBfDomainIdT bfDomainId,
    u32T volIndex,
    int bfDescSize,
    mlBfDescT *bfDesc,
    u32T *nextBfDescId,
    int *bfDescCnt
    );

mlStatusT
advfs_set_vol_ioq_params (
    mlBfDomainIdT bfDomainId,
    u32T volIndex,
    mlVolIoQParamsT *volIoQParamsp
    );


mlStatusT 
advfs_get_bf_params (
    int            fd,
    mlBfAttributesT* bfAttributes,
    mlBfInfoT* bfInfo
    ); 

mlStatusT 
advfs_set_bf_attributes (
    int            fd,
    mlBfAttributesT* bfAttributes
    ); 

mlStatusT 
advfs_get_bf_iattributes (
    int            fd,
    mlBfAttributesT* bfAttributes
    ); 

mlStatusT 
advfs_set_bf_iattributes (
    int            fd,
    mlBfAttributesT* bfAttributes
    ); 

mlStatusT 
advfs_tag_stat(
    mlBfTagT       *tag,
    mlBfSetIdT     setId,
    uid_t          *uid,
    gid_t          *gid,
    off_t          *size,
    time_t         *atime,
    mode_t         *mode
    );

mlStatusT
advfs_get_global_stats(
    mlGetGlobalStatsT *stats
    );

mlStatusT 
advfs_remove_name( 
    int dirFd, 
    char *name
    );

mlStatusT
advfs_rem_bf(
    ino_t ino,
    int fd,
    int flag
    );

mlStatusT
advfs_switch_log(    
    char *domainName,
    u32T volIndex,
    u32T logPgs,
    mlServiceClassT logSvc
    );

mlStatusT
advfs_switch_root_tagdir(
    char *domainName,
    u32T volIndex
    );

mlStatusT 
msfs_get_bf_xtnt_map (
    int            fd,
    int startXtntMap,
    int startXtnt,
    int xtntArraySize,
    mlExtentDescT *xtntsArray,
    int *xtntCnt,
    u32T *allocVolIndex
    );

mlStatusT 
advfs_move_bf_metadata (
    int fd
    );

mlStatusT 
advfs_migrate (
    int            fd,
    u32T srcVolIndex,
    u32T srcPageOffset, 
    u32T srcPageCnt, 
    u32T dstVolIndex,
    u32T dstBlkOffset,
    u32T forceFlag
    );

mlStatusT
msfs_add_volume( 
    char *domain,               /* in - domain name */
    char *volName,              /* in - block special device name */
    mlServiceClassT *volSvc,    /* in/out - service class */
    u32T volSize,               /* in - size of the virtual disk */
    u32T bmtXtntPgs,            /* in - number of pages per BMT extent */
    u32T bmtPreallocPgs,        /* in - number of pages to be preallocated for the BMT */
    mlBfDomainIdT *bfDomainId,  /* out - the domain id */
    u32T *volIndex              /* out - vol index */
    );

mlStatusT
advfs_remove_volume( 
    mlBfDomainIdT bfDomainId,   /* in */
    u32T volIndex,  /* in */
    u32T forceFlag  /* in */
    );

mlStatusT
advfs_add_vol_to_svc_class (
    char *domainName,
    u32T volIndex,
    mlServiceClassT volSvc
    );

mlStatusT
advfs_remove_vol_from_svc_class (
    char *domainName,
    u32T volIndex,
    mlServiceClassT volSvc
    );

mlStatusT
msfs_add_vol_done(
    char *domainName,
    char *volName
    );

mlStatusT
msfs_rem_vol_done(
    char *domainName,
    char *volName
    );

mlStatusT
advfs_set_bf_next_alloc_vol (
    int fd,
    u32T curVolIndex,
    u32T pageCntNeeded,
    int forceFlag
    );

mlStatusT 
advfs_rewrite_xtnt_map (
    int fd,
    int xtntMapIndex
    );

mlStatusT 
advfs_get_bkup_xtnt_map (
    int fd,
    int startXtntMap,
    int startXtnt,
    int xtntArraySize,
    mlExtentDescT *xtntsArray,
    int *xtntCnt,
    u32T *allocVolIndex,
    int *num_pages
    );

mlStatusT
advfs_ss_dmn_ops(
    char* domainName,
    int type,
    mlSSDmnOpsT *ssDmnCurrentState,     /* out */
    int *ssRunningState                 /* out */
    );

mlStatusT
advfs_ss_get_params(
    char* domainName,
    mlSSDmnParamsT *ssDmnParams
    );

mlStatusT
advfs_ss_set_params(
    char* domainName,
    mlSSDmnParamsT ssDmnParams
    );

mlStatusT
advfs_ss_get_fraglist(
    char*         domainName,       /* in */
    vdIndexT      volIndex,         /* in */
    int           ssStartFraglist,  /* in */
    int           ssFragArraySize,  /* in */
    mlFragDescT   *ssFragArray,     /* out */
    int           *ssFragCnt        /* out */
    );

mlStatusT
advfs_ss_get_hotlist(
    mlBfDomainIdT bfDomainId,       /* in */
    int           ssStartHotlist,   /* in */
    int           ssHotArraySize,   /* in */
    mlHotDescT    *ssHotArray,      /* out */
    int           *ssHotCnt         /* out */
    );

/*
 * Statistics prototypes
 */

dmnStatHT *
msfs_start_dmnstat();

mlDmnStatT *
msfs_init_dmnstat( mlBfDomainIdT dmnid );

int
msfs_init_dmnstat_list( dmnStatHT *dmnSt );

mlDmnStatT *
msfs_get_dmnstat(
    mlDmnStatT *initStat,       /* in (out too, depending on refresh) */
    mlBfDomainIdT dmnid,        /* in */
    int refresh                 /* in */
    );

mlDmnStatT *
msfs_get_dmnstat2(
    mlDmnStatT *initStat,       /* in (out too, depending on refresh) */
    mlBfDomainIdT dmnid,        /* in */
    int refresh                 /* in */
    );

int
msfs_get_dmnstat_list( 
    dmnStatHT *dmnSt,           /* in/out */
    int refresh );              /* in */

void
msfs_free_dmnstat(
    mlDmnStatT *dmnStat     /* in */
    );

void
print_dmnstat( 
    mlDmnStatT *initStat,       /* in */
    mlBfDomainIdT dmnid         /* in */
    );

void
msfs_print_dmnstat( 
    dmnStatHT *dmnSt,               /* in */
    mlBfDomainIdT dmnid);           /* in */

dmnStatHT *
msfs_end_dmnstat( dmnStatHT *dmnSp );

void
msfs_start_dstat(int *Ndrive);

mlKdStatT *
msfs_init_dstat(int Ndrive);

mlKdStatT *
msfs_get_dstat( mlKdStatT *initp, int Ndrive );

void
msfs_print_dstats( mlKdStatT *initp, int Ndrive );

/*
 * End statistics prototypes
 */


mlStatusT
msfs_undel_attach(
    char *dirName,
    char *undelDirName
    );

mlStatusT
msfs_undel_detach(
    char *dirName
    );

mlStatusT 
msfs_undel_get(
    char *dirName,
    mlBfTagT *undelDirTag
    );

mlStatusT 
msfs_get_name(
    char *mp,           /* in */
    mlBfTagT tag,       /* in */
    char *name,         /* out  */
    mlBfTagT *parentTag /* out */
    );

#endif /* KERNEL */

#define GDSERRMSG( gds_err ) gds_errlst[ (gds_err) ]
extern char *gds_errlst[];

typedef enum {
    GDS_OK = 0,
    GDS_DISKTYPE,
    GDS_ABSPATH,
    GDS_CANTOPEN,
    GDS_BLOCKDEV,
    GDS_CANTFSTAT,
    GDS_NOCHR,
    GDS_LVSIZE,
    GDS_BADPART,
    GDS_ERRLABEL,
    GDS_UNAVAILPART,
    GDS_VOLTOOBIG
} getDevStatusT;

mlStatusT
tag_to_path (
             char *mntOnName,  /* in */
             mlBfTagT bfTag,  /* in */
             int bufSize,  /* in */
             char *buf  /* in, modified */
             );

getDevStatusT
get_dev_size( 
    char *devName,      /* in - device name */
    char *devType,      /* in - optional device type ("rz55", etc or NULL) */
    u32T *devSize       /* out - device size */
    );

int lock_file( 
    char *fileName,     /* in - name of file to lock */
    int lkMode,         /* in - lock mode (fcntl.h) */
    int *error          /* out - errno if return -1 */
);

int lock_file_nb( 
    char *fileName,     /* in - name of file to lock */
    int lkMode,         /* in - lock mode (fcntl.h) */
    int *error          /* out - errno if return -1 */
);

int unlock_file( 
    int lockHndl,       /* in - lock handle (really just the file desc) */
    int *error          /* out - errno if return -1 */
);

int
volume_avail(
    char *prog,         /* in - program name (for error msgs) */
    char *dmnName,      /* in - domain name */
    char *volName       /* in - name of volume to add */
    );

int
volume_in_use( 
    char *prog,                 /* in - program name (for error msgs) */
    char *dmnDir,               /* in - domain dir's full path name */
    struct stat *volStats       /* in - new volume's stats */
    );

void msfs_event( int action );

void *
advfs_xtntmap_open( 
    int fd,
    mlStatusT *status,
    int xtnt_use
    );

void
advfs_xtntmap_close( 
    void *xtntMapD
    );

int
advfs_xtntmap_next_byte( 
    void *xtntMapD,
    long curByte,
    long *nextByte
    );

int
advfs_xtntmap_next_page( 
    void *xtntMapD,
    int curPg,
    int *nextPg
    );

char *
msfs_opcode_to_name(
    opTypeT opcode
    );

/***                                                             ***
 *                                                                 *
 *  Little syscall routines.  These functions contain the primary  *
 *  logic of the AdvFS system calls.  They should never be called  *
 *  except by msfs_real_syscall(), and the CFS server code.        * 
 *                                                                 *
 *  Each routine takes an arguments buffer (of type libParamsT, a  *
 *  union of syscall-specific arguments structs).  A few routines  *
 *  also require an operation index (of type opIndexT).            *
 *                                                                 *
 *  In support of CFS, non-idempotent operations will also need a  *
 *  CFS transaction identifier (xid), and operations that require  *
 *  a file descriptor will need the corresponding bfap.            *
 *                                                                 *
 ***                                                             ***/

mlStatusT
msfs_syscall_op_get_name2(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_undel_get(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_undel_attach(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_undel_detach(
   libParamsT *libBufp,
   long xid
);

mlStatusT
msfs_syscall_op_event(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_get_bf_params(
   libParamsT *libBufp,
   opIndexT opIndex,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_set_bf_attributes(
   libParamsT *libBufp,
   struct bfAccess *bfap,
   long xid
);

mlStatusT
msfs_syscall_op_get_bf_iattributes(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_set_bf_iattributes(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_move_bf_metadata(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_get_vol_params(
   libParamsT *libBufp,
   opIndexT opIndex
);

mlStatusT
msfs_syscall_op_get_vol_bf_descs(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_set_vol_ioq_params(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_get_bf_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_get_cludio_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_get_bkup_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_get_dmn_params(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_get_dmnname_params(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_get_bfset_params(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_set_bfset_params(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_fset_get_info(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_fset_create(
   libParamsT *libBufp,
   long xid1,
   long xid2
);

mlStatusT
msfs_syscall_op_fset_get_id(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_fset_get_stats(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_fset_delete(
   libParamsT *libBufp,
   long xid
);

mlStatusT
msfs_syscall_op_fset_clone(
   libParamsT *libBufp,
   long xid
);

mlStatusT
msfs_syscall_op_fset_rename(
   libParamsT *libBufp,
   long xid
);

mlStatusT
msfs_syscall_op_dmn_init(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_get_dmn_vol_list(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_migrate(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_rem_name(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_rem_bf(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_tag_stat(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_add_volume(
   libParamsT *libBufp,
   opIndexT opIndex
);

mlStatusT
msfs_syscall_op_rem_volume(
   libParamsT *libBufp,
   opIndexT opIndex
);

mlStatusT
msfs_syscall_op_add_rem_vol_svc_class(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_add_rem_vol_done(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_set_bf_next_alloc_vol(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

mlStatusT
msfs_syscall_op_switch_log(
   libParamsT *libBufp,
   long xid
);

mlStatusT
msfs_syscall_op_switch_root_tagdir(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_rewrite_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap,
   long xid
);

mlStatusT
msfs_syscall_op_reset_free_space_cache(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_get_global_stats(
   libParamsT *libBufp
);

mlStatusT
msfs_syscall_op_get_smsync_stats(
   libParamsT *libBufp
);


mlStatusT
msfs_check_on_disk_version(
    int on_disk_version_known_to_caller
    );

mlStatusT
msfs_skip_on_disk_version_check();

/* used in function shipping to locate CFS server */
typedef enum {
        DMN_NAME,
        DMN_ID,
        FILE_PATH,
        FILE_DESC
} ServerSearchKeyT;

#endif /* _MSFS_SYSCALLS_ */
