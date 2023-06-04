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
 *      AdvFS
 *
 * Abstract:
 *
 *      AdvFS library / system call prototypes and types
 *
 *      ***NOTE*** this file contains both the libadvfs interface
 *      used by utilities (cmds) and the advfs_syscall interface
 *      which is only used by libadvfs.  The libadvfs interface
 *      definitions should be moved to a libadvfs.h for utilities.
 */

#ifndef _ADVFS_SYSCALLS_INCLUDED
#define _ADVFS_SYSCALLS_INCLUDED

#include <sys/param.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <advfs/advfs_types.h>
#include <advfs/bs_public.h>
#include <advfs/ms_public.h>
#include <advfs/vfast.h>

#define ADVFS_DMN_DIR            "/dev/advfs"
#define ADVFS_TCR_DMN_DIR        "/dev/advfs_cfs"
#define ADVFS_CDMN_DIR           "/cdev/advfs"
#define ADVFS_TCR_DMN_DIR        "/dev/advfs_cfs"
#define ADVFS_SBIN_DIR           "/sbin/fs/advfs"
#define DOT_ADVFS_LOCK           ".advfslock"
#define DOT_MKFS_LOCK            ".mkdevlock"
#define DOT_ADVFS_STG            ".stg"

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#define ADVFSDEV_MAJOR 47

typedef int32_t adv_status_t;


/*
 * Kernel disk stats - filled by calls to
 * the table command.
 */
#define UNITNAMESZ 8
typedef struct {
    int32_t alive;      /* == 1 if disk alive, zero otherwise */
    int32_t unit;       /* unit number */
    char unitName[UNITNAMESZ];
    uint64_t xfer;     /* number of transfers */
    uint64_t nreads;   /* number of reads */
    uint64_t nwrites;  /* number of writes */
    uint64_t readblks; /* number of DEV_BSIZE byte blocks read */
    uint64_t writeblks;
} mlKdStatT;



/* snapshot flags */
typedef enum {
    SF_NO_FLAGS    = 0x0,  /* No flags */
    SF_RDONLY      = 0x1,  /* Snapset is read-only */
    SF_WRITE       = 0x2,  /* Snapset is writeable */
    SF_FAST_SNAP   = 0x4,  /* Creation flag. Only flush metadata.
                            *  Not supported yet.  
                            */
} bf_snap_flags_t;

typedef dStatT adv_vol_counters_t;

typedef struct adv_vol_prop {
    serviceClassT serviceClass; /* service class provided */
    bf_fob_t bmtXtntFobs;       /* num of 1k file offset blks per BMT extend */ 
    char volName[BS_VD_NAME_SZ];
} adv_vol_prop_t;

typedef struct adv_vol_ioq_params {
    uint32_t rdMaxIo;         /* Current read IO transfer device blocks size */
    uint32_t wrMaxIo;         /* Current write IO transfer device blocks size*/
    uint32_t min_iosize;      /* Minimum read/write IO transfer byte size */
    uint32_t max_iosize;      /* Driver's maximum rd/wr IO transfer byte size*/
    uint32_t preferred_iosize;/* Driver's preferred rd/wr IO transfer byte sz*/
} adv_vol_ioq_params_t;

typedef struct adv_vol_info {
    bsIdT vdMntId;                /* last mount id */
    vdIndexT volIndex;            /* vol index within file system */
    bf_vd_blk_t volSize;          /* size in blocks */
    bf_vd_blk_t sbmBlksBit;       /* number of blocks each bit represents */
    bf_vd_blk_t freeClusters;     /* total number of free clusters */
    bf_vd_blk_t totalClusters;    /* total number of clusters */
} adv_vol_info_t;

typedef struct adv_bf_attr {
    int32_t version;               /* version of this data structure */
    serviceClassT reqService;      /* required service class */
    bf_vd_blk_t initialSize;       /* initial allocation in blocks */
    int32_t bfFlags;
    int32_t fill2;
    int32_t rsvd_sec1;             /* rsvd for security stuff */
    int32_t rsvd_sec2;             /* rsvd for security stuff */
    int32_t rsvd_sec3;             /* rsvd for security stuff */
    int32_t rsvd_sec4;             /* rsvd for security stuff */
    int32_t fill3;
    int32_t fill4;
} adv_bf_attr_t;

typedef struct adv_bf_info {
    bfDataSafetyT dataSafety;	/* bitfile data safety */
    bf_fob_t pageSize;          /* page size in 1k file offset blocks */
    bf_fob_t mbfNumFobs;        /* number of 1K fobs allocated */
    bf_fob_t mbfNextFob;        /* next fob to allocate */
    vdIndexT volIndex;          /* primary mcell vol */
    bfTagT tag;                 /* bitfile tag */
    bf_snap_type_t mbfSnapType; /* snapshot type */
    size_t mbfOrigFileSize;     /* If file is snapshot, then it is the 
                                 * highest byte offset that will be cowed
                                 * from parents.  Otherwise it is 0.
                                 */
    size_t mbfRsvdFileSize;     /* If the file has reserved space, from
                                 * user pre-allocation, then that value
                                 * will be here.  Otherwise it is 0.
                                 */
} adv_bf_info_t;


/* changed ML_BSR_MAX to BSR_API_MAX from BSR_MAX.
 * This change is required to eliminate the potential for apps
 * who reference ML_BSR_MAX to index bmt records beyond record number 21.
 * In the rare situation of a cluster in mid rolling-upgrade, the potential
 * for two nodes to have different definitions of adv_bf_dmn_params_t exists.
 * The structure bmtStatT, which is a bmtStatT had the potential to increase!  
 * in size with the addition of new bmt record types.  This caused user apps
 * to break due to a mismatch in the API structure size.
 */
#define ML_BSR_MAX BSR_API_MAX   /*   !!!!! see above comment  !!!!!   */

typedef struct {
    bfDomainIdT bfDomainId;         /* domain ID */
    uint32_t maxVols;               /* maximum vol index */
    bfTagT bfSetDirTag;             /* tag of root bitfile-set tag dir */
    bfTagT ftxLogTag;               /* tag of domain log */
    bf_fob_t ftxLogFobs;            /* num of 1k file offset blks in the log */
    uint32_t curNumVols;            /* current number of VOLs */
    adv_uid_t uid;                  /* domain's owner */
    adv_gid_t gid;                  /* domain's group */
    adv_mode_t mode;                /* domain's access permissions */
    char domainName[BS_DOMAIN_NAME_SZ]; /* domain name */
    bcStatT bcStat;                 /* buffer cache stats */
    bmtStatT bmtStat;               /* per domain BMT stats */
    logStatT logStat;               /* per domain LOG stats */
    adv_ondisk_version_t dmnVersion; /* domain's on-disk version number */
    bfFsCookieT  dmnCookie;          /* domain cookie - must not change */
    bf_fob_t userAllocSz;       /* allocation unit for user data in 1k fobs */
    bf_fob_t metaAllocSz;       /* allocaiton unit for metadata in 1k fobs */
    adv_threshold_t dmnThreshold;    /* domain threshold values */
} adv_bf_dmn_params_t;

typedef struct {
    adv_bf_dmn_params_t *dmnParams;
    vdIndexT *volIndex;
    adv_vol_counters_t *volCounters;
} adv_dmn_stats_t;


struct dmnStatH {
    int32_t len;
    bfDomainIdT *dmnIdp;
    adv_dmn_stats_t **dmnInitStp;
    adv_dmn_stats_t **dmnStp;
};
typedef struct dmnStatH dmnStatHT;

/* quota status */

#define Q_STS_QUOTA_ON           0x0001    /* some quota type is active */
#define Q_STS_USR_QUOTA_MT       0x0002    /* enable maintaining user quotas */
#define Q_STS_GRP_QUOTA_MT       0x0004    /* enable maintaining group quotas */
#define Q_STS_USR_QUOTA_EN       0x0008    /* enable enforcing user quotas */
#define Q_STS_GRP_QUOTA_EN       0x0010    /* enable enforcing group quotas */
#define Q_STS_FS_GRP_QUOTA       0x0020    /* fileset quotas from grp quotas */
#define Q_STS_FS_QUOTA_MT        0x0040    /* track size of fileset */

#define ADV_MAX_LOCKS 100

typedef struct adv_lock_usage_stats {
    uint64_t lock;
    uint64_t wait;
    uint64_t reWait;
    uint64_t signal;
    uint64_t broadcast;
} adv_lock_usage_stats_t;

typedef struct adv_lock_stats {
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
    adv_lock_usage_stats_t usageStats[ADV_MAX_LOCKS];
} adv_lock_stats_t;

typedef struct adv_file_set_stats {
    uint64_t    advfs_lookup;

    struct {
        uint64_t hit;
        uint64_t hit_not_found;
        uint64_t miss;
    } lookup;

    uint64_t    advfs_create;
    uint64_t    advfs_spare1; /* used to be msfs_mknod on Tru64 UNIX */
    uint64_t    advfs_open;
    uint64_t    advfs_close;
    uint64_t    advfs_access;
    uint64_t    advfs_getattr;
    uint64_t    advfs_setattr;
    uint64_t    advfs_read;
    uint64_t    advfs_write;
    uint64_t    advfs_mmap;
    uint64_t    advfs_fsync;
    uint64_t    advfs_spare2; /* used to be msfs_seek on Tru64 UNIX */
    uint64_t    advfs_remove;
    uint64_t    advfs_link;
    uint64_t    advfs_rename;
    uint64_t    advfs_mkdir;
    uint64_t    advfs_rmdir;
    uint64_t    advfs_symlink;
    uint64_t    advfs_readdir;
    uint64_t    advfs_readlink;
    uint64_t    advfs_inactive;
    uint64_t    advfs_spare3; /* used to be msfs_reclaim on Tru64 UNIX */
    uint64_t    advfs_spare4; /* used to be msfs_page_read on Tru64 UNIX */
    uint64_t    advfs_spare5; /* used to be msfs_page_write on Tru64 UNIX */
    uint64_t    advfs_getpage;
    uint64_t    advfs_putpage;
    uint64_t    advfs_bread;
    uint64_t    advfs_brelse;
    uint64_t    advfs_lockctl;
    uint64_t    advfs_setvlocks;
    uint64_t    advfs_spare6; /* used to be msfs_syncdata on Tru64 UNIX */
} adv_file_set_stats_t;

typedef struct adv_frag_desc {
    bfTagT   ssFragTag;
    bfSetIdT ssFragBfSetId;
    uint32_t ssFragRatio;
    uint32_t ssXtntCnt;
} adv_frag_desc_t;

typedef struct adv_ss_hot_desc {
    bfTagT     ssHotTag;
    bfSetIdT   ssHotBfSetId;
    int32_t    ssHotPlaced;
    vdIndexT ssHotVdi;
    uint32_t   ssHotIOCnt;
    adv_time_t ssHotLastUpdTime;
    adv_time_t ssHotEntryTime;
    adv_time_t ssHotErrTime;
} adv_ss_hot_desc_t;

typedef struct adv_ss_dmn_params {
    adv_time_t ssFirstMountTime;
    /* UI visable configurable options */
    uint16_t ssDmnDefragment;
    uint16_t ssDmnSmartPlace;
    uint16_t ssDmnBalance;
    uint16_t ssDmnVerbosity;
    uint16_t ssDmnDirectIo;
    uint16_t ssMaxPercentOfIoWhenBusy;
    uint64_t ssFilesDefraged;
    uint64_t ssFobsDefraged;             /* Number of 1k file offset blocks */
    uint64_t ssFobsBalanced;             /* Number of 1k file offset blocks */
    uint64_t ssFilesIOBal;
    uint64_t ssExtentsConsol;
    uint64_t ssPagesConsol;
    /* UI hidden configurable options */
    uint16_t ssAccessThreshHits;
    uint16_t ssSteadyState;
    uint16_t ssMinutesInHotList;
    uint16_t ssDmnDefragAll;
    uint32_t ssReserved1;
    uint32_t ssReserved2;
    uint64_t ssReserved3;
    uint64_t ssReserved4;
} adv_ss_dmn_params_t;

typedef struct adv_statvfs {
    uint64_t f_bsize;
    uint64_t f_frsize;
    uint64_t f_blocks;
    uint64_t f_bfree;
    uint64_t f_bavail;
    uint64_t f_files;
    uint64_t f_ffree;
    uint64_t f_favail;
    char f_basetype[16];
    uint64_t f_namemax;
    char f_fstr[32];
    int32_t f_magic;
    int32_t f_type;
    int32_t f_featurebits;
    uint64_t f_flag;
    uint64_t f_fsindex;
    uint64_t f_fsid;
    int64_t f_time;
    uint16_t f_cnode;
    uint64_t f_size;
} adv_statvfs_t;

typedef struct removeName {
    int32_t  dirFd;
    char *name;
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opRemoveName;

typedef struct removeBf {
    ino_t ino;
    int32_t dirFd;
    int32_t flag;
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opRemoveBf;

typedef struct SetRename {
    char *domain;               /* in - domain name */
    char *origSetName;          /* in - set's orig name */
    char *newSetName;           /* in - set's new name */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;
} opSetRenameT;

typedef struct switchLog {
    char *domainName;         /* in - domain name */
    vdIndexT volIndex;        /* in - volume containing log */
    uint32_t logPgs;          /* in - new log size, if > 0 */
    serviceClassT logSvc;     /* in - new log service class, if != 0 */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSwitchLog;

typedef struct switchRootTagDir {
    char *domainName;
    vdIndexT volIndex;
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSwitchRootTagDirT;

typedef struct {
    char *mp;
    char *name;         
    bfTagT tag;
    bfTagT parentTag;
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetName;

typedef struct {
    char *dirName;            /* in - target directory name */
    bfTagT undelDirTag;       /* out - trashcan directory tag */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opUndelGet;

typedef struct {
    char *dirName;                 /* in - target directory name */
    char *undelDirName;            /* in - trashcan directory name */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opUndelAttach;

typedef struct {
    char *dirName;                 /* in - target directory name */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opUndelDetach;

typedef struct {
    char *domain;                /* in - domain name */
    char *setName;               /* in - set's name */
    serviceClassT reqServ;       /* in - required service class */
    uint32_t fsetOptions;        /* in - fileset options */
    adv_gid_t quotaId;           /* in - ID for quota files */
    bfSetIdT  bfSetId;           /* out - bitfile set id */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetCreateT;

typedef struct {
    char *domain;                /* in - domain name */
    char *setName;               /* in - set's name */
    bfSetIdT  bfSetId;           /* out - bitfile set id */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetGetIdT;

typedef struct {
    char *domain;                       /* in - domain name */
    char *setName;                      /* in - set's name */
    adv_file_set_stats_t fileSetStats;  /* out */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetGetStatsT;

typedef struct {
    char *domain;                  /* in - domain name */
    char *setName;                 /* in - set's name */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetDeleteT;

typedef struct {
    char *parent_dmn_name;          /* in - parent domain name */
    char *parent_set_name;          /* in - parent set name */
    char *snap_dmn_name;            /* in - child domain name */
    char *snap_set_name;            /* in - child set name */
    bfSetIdT snap_set_id;           /* out - child set id */
    bf_snap_flags_t snap_flags;     /* in - snap flags */
    uint32_t snap_create_reserved1;
    uint64_t snap_create_reserved2;
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSnapCreateT;

typedef struct {
    char           *domain;        /* in - domain name */
    uint64_t       nextSetIdx;     /* in/out */
    int32_t        flag;           /* in - flag, 1 = skip dmn recovery */
    bfSetParamsT bfSetParams;      /* out */
    uint32_t       userId;         /* out */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetGetInfoT;

typedef struct {
    int32_t          locked;
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opDumpLocksT;

typedef struct {
    int32_t         fd;            /* in - file descriptor */ 
    adv_bf_attr_t bfAttributes;    /* in/out - bitfile attributes */
    adv_bf_info_t      bfInfo;     /* out - bitfile info */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetBfParamsT;

typedef struct {
    int32_t        fd;             /* in - file descriptor */
    adv_bf_attr_t bfAttributes;    /* in - bitfile attributes */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetBfAttributesT;

typedef struct {
    int32_t        fd;             /* in - file descriptor */
    adv_bf_attr_t bfAttributes;    /* in/out - bitfile attributes */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetBfIAttributesT;

typedef struct {
    int32_t        fd;             /* in - file descriptor */
    adv_bf_attr_t bfAttributes;    /* in - bitfile attributes */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetBfIAttributesT;

typedef struct {
    int32_t fd;                    /* in - file descriptor */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;
    uint64_t  rsvd4;    
} opMoveBfMetadataT;
 
typedef struct {
    bfDomainIdT bfDomainId;      /* in - domain id */
    vdIndexT volIndex;           /* in - volume index */
    adv_vol_info_t volInfo;            /* out - volume information */
    adv_vol_prop_t volProperties;/* out - volume properties */
    adv_vol_ioq_params_t volIoQParams;  /* out - volume i/o queue parameters */
    adv_vol_counters_t volCounters;    /* out - volume stats (dStatT) */
    int32_t flag;                  /* in/out - flags, see below */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetVolParamsT;
/* 
 * Bit definitions for flags field in opGetVolParamsT.
 */
#define GETVOLPARAMS_NO_BMT     0x01    /* don't fetch bmt data   */
#define GETVOLPARAMS2_CHVOL_OP_A 0x02   /* ignore service class */
#define GETVOLPARAMS2           0x04    /* get stats for interval */

typedef struct {
    bfDomainIdT bfDomainId;      /* in - domain id */
    vdIndexT volIndex;           /* in - volume index */
    int32_t bfDescSize;            /* in - size of array */
    bsBfDescT *bfDesc;             /* in/out - array of bitfile descriptors */
    bfMCIdT nextBfDescId;         /* in/out - state checkpoint */
    int32_t bfDescCnt;             /* out - actual # of descriptors returned */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetVolBfDescsT;

typedef struct {
    bfDomainIdT bfDomainId;      /* in - domain id */
    vdIndexT volIndex;           /* in - volume index */
    adv_vol_ioq_params_t volIoQParams;  /* in - volume i/o queue parameters */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetVolIoQParamsT;

typedef struct {
    bfDomainIdT bfDomainId;      /* out - domain id */
    char *domain;                  /* in - bf domain name */
    char *volName;                 /* block special device name */
    serviceClassT  volSvc;       /* out - service class */
    bf_vd_blk_t volSize;           /* size of the volume */
    bf_fob_t bmtXtntFobs;          /* number of file offset blocks per BMT extent */
    bf_fob_t bmtPreallocFobs;      /* number of file offset blocks to be preallocated for the BMT */
    vdIndexT volIndex;           /* out - volume index */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opAddVolT;

typedef struct {
    bfDomainIdT bfDomainId;
    vdIndexT    volIndex;
    uint32_t          forceFlag;
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opRemVolT;

typedef struct {
    char *domainName;              /* in - domain name */
    vdIndexT volIndex;           /* in - volume index */
    serviceClassT volSvc;        /* in - volume service class */
    uint32_t action;               /* in - flag: 0 == add, 1/else == remove */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opAddRemVolSvcClassT;

typedef struct {
    char *domainName;              /* in - domain name */
    char *volName;                 /* in - volume name */
    bsVdOpT op;                    /* in - addvol or rmvol? */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opAddRemVolDoneT;

typedef struct {
    int32_t           fd;            /* in - file descriptor */
    int32_t           startXtntMap;  /* in - extent map at which to start */
    int32_t           startXtnt;     /* in - extent at which to start */
    int32_t           xtntArraySize; /* in - size of extent array */
    bsExtentDescT *xtntsArray;       /* out - array to copy into */
    int32_t           xtntCnt;       /* out - number of extents copied */
    vdIndexT    allocVolIndex;     /* out - disk on which stg is alloc'ed */
    uint64_t    gxm_fob_cnt;       /* out - number of fobs in extents */    
    uint32_t  rsvd1;
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetXtntMapT;

typedef struct {
    int32_t           fd;            /* in - file descriptor */
    int32_t           startXtntMap;  /* in - extent map at which to start */
    int32_t           startXtnt;     /* in - extent at which to start */
    int32_t           xtntArraySize; /* in - size of extent array */
    bsExtentDescT *xtntsArray;       /* out - array to copy into */
    int32_t           xtntCnt;       /* out - number of extents copied */
    vdIndexT    allocVolIndex;     /* out - dsk on which stg alloc'ed(stripes)*/
    uint32_t          pg_size;       /* out - underlying file's allocation unit in bytes */
    uint64_t      file_size;         /* out - file size in bytes */
    int32_t           volMax;        /* out - max volumes per domain */
    bsDevtListT   *devtvec;          /* out - vector of dev_ts  */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetCludioXtntMapT;

typedef struct {
    bfDomainIdT bfDomainId;      /* in - domain id */
    adv_bf_dmn_params_t dmnParams;        /* out - domain params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetDmnParamsT;

typedef struct {
    char *domain;                  /* in - domain name */
    adv_bf_dmn_params_t dmnParams;        /* out - domain params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetDmnNameParamsT;

typedef struct {
    char *domain;                  /* in - domain name */
    adv_bf_dmn_params_t dmnParams; /* in - domain params */
}opSetDmnNameParamsT;

typedef struct {
    bfSetIdT     bfSetId;        /* in - fileset id */
    bfSetParamsT bfSetParams;    /* out - fileset params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetBfSetParamsT;

typedef struct {
    bfSetIdT     bfSetId;        /* in - fileset id */
    bfSetParamsT bfSetParams;    /* in - fileset params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetBfSetParamsT;

typedef struct {
    char *dmnName;
    bfSetParamsT bfSetParams;    /* in - fileset params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSetBfSetParamsActivateT;

typedef struct {
    char *dmnName;                 /* in - domain name */
    bfSetIdT     bfSetId;          /* in - fileset id */
    bfSetParamsT bfSetParams;      /* out - fileset params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetBfSetParamsActT;

typedef struct {
    bfDomainIdT bfDomainId;      /* in - domain id */
    int32_t volIndexArrayLen;      /* in - number of ints in array */
    vdIndexT *volIndexArray;     /* out - list of volume indices */
    int32_t numVols;               /* out - num volumes put in array */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetDmnVolListT;

typedef struct {
    char          *domain;         /* in - bf domain name */
    int32_t maxVols;               /* in - maximum number of virtual disks */
    bf_fob_t logFobs;              /* in - number of fobs in log */
    serviceClassT logSvc;        /* in - log service attributes */
    serviceClassT tagSvc;        /* in - tag directory service attributes */
    char *volName;                 /* in - block special device name */
    serviceClassT volSvc;        /* in - service class */
    bf_vd_blk_t volSize;           /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs;          /* in - number of fobs per BMT extent */
    bf_fob_t bmtPreallocFobs;      /* in - number of fobs to be
                                           preallocated for the BMT */
    adv_ondisk_version_t domainVersion;  /* in - on-disk version for domain */
    bfDomainIdT bfDomainId;      /* out - domain id */
    bf_fob_t userAllocFobs;        /* in - number of fobs per user data page */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opDmnInitT;

typedef struct {
     char *domain;                  /* in - domain name */
     uint64_t  rsvd1;
     uint64_t  rsvd2;
     uint64_t  rsvd3;
     uint64_t  rsvd4;
} opDmnRecreateT;

typedef struct {
    char *domainName;		   /* in - domain name */
    char *setName;		   /* in - bitfile set name */
    adv_statvfs_t advStatvfs;      /* out - fill in for statvfsdev() */
    uint64_t  rsvd1;
    uint64_t  rsvd2;
    uint64_t  rsvd3;
    uint64_t  rsvd4;
} opGetStatvfsT;

typedef struct {
    int32_t        fd;             /* in - file descriptor */
    vdIndexT     srcVolIndex;    /* in - source volume of storage to migrate */
    bf_fob_t       srcFobOffset;   /* in - fob to migrate */
    uint64_t       srcFobCnt;      /* in - number of 1k fobs to migrate */
    vdIndexT     dstVolIndex;    /* in - dest. volume of storage to migrate */
    bf_vd_blk_t    dstBlkOffset;   /* in */
    uint32_t       forceFlag;      /* in */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opMigrateT;

typedef struct {
    bfSetIdT setId;              /* in - bitfile set tag */
    bfTagT  tag;                 /* in/out - tag of this bitfile */
    adv_uid_t uid;                 /* out - user ID */
    adv_gid_t gid;                 /* out - group ID */
    adv_off_t size;                /* out - allocated file size in bytes */
    adv_time_t atime;              /* out - access time */
    adv_mode_t mode;               /* out - file mode */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opTagStatT;


typedef struct {
    bfDomainIdT bfDomainId;      /* in - domain id */
    vdIndexT volIndex;           /* in - volume index */
    bf_vd_blk_t scanStartCluster;  /* in - new free space pointer */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opResetFreeSpaceCacheT;

/*
 * Bit definitions for flags field in opExtendFsParamsT
 */
#define EXT_VFLAG 0x01           /* verbose flag */
#define EXT_QFLAG 0x02           /* op is query-only */

typedef struct {
    /* Input */
    char *domain;               /* Domain/Filesystem name */
    char *blkdev;               /* block device to extend */
    uint64_t size;              /* Size in blocks to extend */
    int32_t flags;              /* Op flags */
    /* Output */
    bf_vd_blk_t oldBlkSize;     /* device old block size */
    bf_vd_blk_t newBlkSize;     /* device new block size */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opExtendFsParamsT;

/*
 * A general purpose structure.  Add fields as desired 
 * to get system-wide AdvFS information.
 */
typedef struct {
    uint64_t NumAccess;               /* access structures in system */
    uint64_t FreeAccLen;              /* access structures on free list */
    uint64_t ClosedAccLen;            /* access structures on closed list */
    uint64_t AccAlloc;                /* number allocated by cleanup thread */
    uint64_t AccDealloc;              /* number deallocated by cleanup thread */
    uint64_t AccDeallocFail;          /* number of deallocation failures */
    uint64_t CleanupClosed;           /* calls to cleanup_closed_list() */
    uint64_t CleanupSkipped;          /* bfaps skipped and processed */
    uint64_t CleanupProcessed;        /* in cleanup_closed_list() */        
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opGetGlobalStatsT;

typedef opGetGlobalStatsT GetGlobalStatsT;

typedef struct {
    char* domainName;                  /* in - domain name */
    int32_t type;                      /* in - operation type */
    ssDmnOpT ssDmnCurrentState;     /* out - domain state */
    int32_t ssRunningState;            /* out - run or stop */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSSDmnOpsT;

typedef struct {
    char* domainName;                  /* in - domain id */
    adv_ss_dmn_params_t ssDmnParams;        /* out - domain params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSSGetParamsT;

typedef struct {
    char* domainName;                  /* in - domain name */
    adv_ss_dmn_params_t ssDmnParams;        /* in - set the domain params */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSSSetParamsT;

typedef struct {
    char*         domainName;          /* in */
    vdIndexT    volIndex;            /* in */
    int32_t       ssStartFraglist;     /* in */
    int32_t       ssFragArraySize;     /* in */
    adv_frag_desc_t   *ssFragArray;        /* out */
    int32_t       ssFragCnt;           /* out */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSSGetFraglistT;


typedef struct {
    bfDomainIdT bfDomainId;          /* in */
    int32_t       ssStartHotlist;      /* in */
    int32_t       ssHotArraySize;      /* in */
    adv_ss_hot_desc_t    *ssHotArray;         /* out */
    int32_t       ssHotCnt;            /* out */
    uint64_t  rsvd1;    
    uint64_t  rsvd2;    
    uint64_t  rsvd3;    
    uint64_t  rsvd4;    
} opSSGetHotlistT;



typedef adv_lock_stats_t opGetLockStatsT;


typedef union {
    opDumpLocksT      dumpLocks;
    opGetLockStatsT   getLockStats;

    /* Bitfile Functions */
    opGetBfParamsT    getBfParams;
    opSetBfAttributesT setBfAttributes;
    opGetBfIAttributesT getBfIAttributes;
    opSetBfIAttributesT setBfIAttributes;
    opMoveBfMetadataT moveBfMetadata;
    opGetXtntMapT     getXtntMap;
    opTagStatT        tagStat;
    opGetCludioXtntMapT getCludioXtntMap;

    /* File Set Functions */
    opSetCreateT      setCreate;
    opSetDeleteT      setDelete;
    opSnapCreateT     snapCreate;
    opSetGetInfoT     setGetInfo;
    opSetGetIdT       setGetId;
    opSetGetStatsT    setGetStats;
    opGetBfSetParamsT getBfSetParams;
    opSetBfSetParamsT setBfSetParams;
    opSetBfSetParamsActivateT setBfSetParamsActivate;
    opGetBfSetParamsActT getBfSetParamsAct;

    /* File Domain and Volume Functions */
    opDmnInitT        dmnInit;
    opGetDmnParamsT   getDmnParams;
    opGetDmnVolListT  getDmnVolList;
    opGetDmnNameParamsT   getDmnNameParams;
    opSetDmnNameParamsT   setDmnNameParams;
    opGetVolParamsT   getVolParams;
    opGetVolBfDescsT   getVolBfDescs;
    opSetVolIoQParamsT   setVolIoQParams;
    opAddVolT         addVol;
    opRemVolT         remVol;
    opSwitchLog       switchLog;
    opSwitchRootTagDirT switchRootTagDir;
    opAddRemVolSvcClassT  addRemVolSvcClass;
    opMigrateT        migrate;
    opDmnRecreateT    dmnRecreate;
    opGetStatvfsT     getStatvfs;

    opUndelAttach     undelAttach;
    opUndelDetach     undelDetach;
    opUndelGet        undelGet;
    opGetName         getName;
    opResetFreeSpaceCacheT  resetFreeSpaceCache;
    opRemoveName      removeName;
    opRemoveBf        removeBf;
    opSetRenameT      setRename;
    opGetGlobalStatsT getGlobalStats;
    opAddRemVolDoneT  addRemVolDone;
    opExtendFsParamsT extendFsParams;

    /* vfast Functions */
    opSSDmnOpsT      ssDmnOps;
    opSSGetParamsT   ssGetParams;
    opSSSetParamsT   ssSetParams;
    opSSGetFraglistT ssGetFraglist;
    opSSGetHotlistT  ssGetHotlist;
} libParamsT;

#ifndef _KERNEL

static bfDomainIdT nilMlBfDomainIdT = { 0 };

static bfSetIdT nilMlBfSetIdT = { 0 };

static const adv_bf_dmn_params_t NilDmnParams = { 0 };
static const bfSetParamsT mlNilBfSetParams;

/*
 * File Set Functions
 */

adv_status_t
advfs_fset_create( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    serviceClassT reqServ,  /* in - required service class */
    uint32_t fsetOptions,     /* in - fileset options */
    adv_gid_t quotaId,        /* in - group ID for quota files */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    );

adv_status_t
advfs_fset_get_id( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    bfSetIdT *bfSetId       /* out - bitfile set id */
    );

adv_status_t
advfs_fset_delete( 
    char *domain,             /* in - domain name */
    char *setName             /* in - set's name */
    );

adv_status_t
advfs_lib_create_snapset(
    char *parent_dmn_name,          /* in - Parent fileset's domain name */
    char *parent_set_name,          /* in - Parent fileset name */
    char *snap_dmn_name,            /* in - Snapset's domain name */
    char *snap_set_name,            /* in - Snapset name */
    bf_snap_flags_t snap_flags,     /* in - Snapset creation flags */
    bfSetIdT *snap_set_id           /* out - Snapset's id */
    );


adv_status_t
advfs_fset_get_info(
    char           *domain,             /* in */
    uint64_t       *nextSetIdx,         /* in/out */
    bfSetParamsT *bfSetParams,        /* out */
    uint32_t       *userId,             /* out */
    int32_t         flag                /* in, 1 = skip dmn recovery */
    );

adv_status_t 
advfs_get_bfset_params(
    bfSetIdT bfSetId,
    bfSetParamsT *bfSetParams
    );

adv_status_t 
advfs_set_bfset_params(
    bfSetIdT bfSetId,
    bfSetParamsT *bfSetParams
    );

adv_status_t
advfs_lib_get_bfset_params_activate(
        char *dmnName,
        bfSetIdT bfSetId,
        bfSetParamsT *bfSetParams
        );

adv_status_t
advfs_fset_rename(
    char *domain,             /* in - domain name */
    char *origSetName,        /* in - original bfset name */
    char *newSetName          /* in - new bfset name */
    );

/* 
 * File Domain and Volume Functions
 */


adv_status_t
advfs_get_lock_stats( adv_lock_stats_t *lockStats );

adv_status_t
advfs_fset_get_stats( 
    char *domain,             /* in - domain name */
    char *setName,            /* in - set's name */
    adv_file_set_stats_t *fileSetStats
 );

adv_status_t
advfs_dmn_init(
    char* domain,               /* in - bf domain name */
    int32_t maxVols,            /* in - maximum number of virtual disks */
    bf_fob_t logFobs,           /* in - number of fobs in log */
    bf_fob_t userAllocFobs,     /* in - number of fobs per user data page */
    serviceClassT logSvc,     /* in - log service attributes */
    serviceClassT tagSvc,     /* in - tag directory service attributes */
    char *volName,              /* in - block special device name */
    serviceClassT volSvc,     /* in - service class */
    bf_vd_blk_t volSize,        /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs,       /* in - number of fobs per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - number of fobs to be preallocated for the BMT */
    adv_ondisk_version_t domainVersion,/* in - on-disk version of domain */
    bfDomainIdT* bfDomainId   /* out - domain id */
    );

adv_status_t 
advfs_get_dmn_params(
    bfDomainIdT bfDomainId,
    adv_bf_dmn_params_t *dmnParams
    );

adv_status_t 
advfs_get_dmnname_params(
    char *domain,
    adv_bf_dmn_params_t *dmnParams
    );

adv_status_t
advfs_set_dmnname_params(
    char *domain,
    adv_bf_dmn_params_t *dmnParams
    );
    
adv_status_t
advfs_get_dmn_vol_list(
    bfDomainIdT bfDomainId,
    int32_t volIndexArrayLen,    /* in - number of ints in array */
    vdIndexT volIndexArray[],  /* out - list of volume indices */
    int32_t *numVols             /* out - num volumes put in array */
    );

adv_status_t 
advfs_get_vol_params (
    bfDomainIdT bfDomainId,
    vdIndexT volIndex,
    adv_vol_info_t* volInfop,
    adv_vol_prop_t* volPropertiesp,
    adv_vol_ioq_params_t* volIoQParamsp,
    adv_vol_counters_t* volCountersp
    );

adv_status_t 
advfs_get_vol_bf_descs (
    bfDomainIdT bfDomainId,
    vdIndexT volIndex,
    int32_t bfDescSize,
    bsBfDescT *bfDesc,
    bfMCIdT *nextBfDescId,
    int32_t *bfDescCnt
    );

adv_status_t
advfs_set_vol_ioq_params (
    bfDomainIdT bfDomainId,
    vdIndexT volIndex,
    adv_vol_ioq_params_t *volIoQParamsp
    );

adv_status_t 
advfs_get_bf_params (
    int32_t        fd,
    adv_bf_attr_t* bfAttributes,
    adv_bf_info_t* bfInfo
    ); 

adv_status_t 
advfs_set_bf_attributes (
    int32_t        fd,
    adv_bf_attr_t* bfAttributes
    ); 

adv_status_t 
advfs_get_bf_iattributes (
    int32_t        fd,
    adv_bf_attr_t* bfAttributes
    ); 

adv_status_t 
advfs_set_bf_iattributes (
    int32_t        fd,
    adv_bf_attr_t* bfAttributes
    ); 

adv_status_t 
advfs_tag_stat(
    bfTagT       *tag,
    bfSetIdT     setId,
    adv_uid_t      *uid,
    adv_gid_t      *gid,
    adv_off_t      *size,
    adv_time_t     *atime,
    adv_mode_t     *mode
    );

adv_status_t
advfs_get_global_stats(
    GetGlobalStatsT *stats
    );

adv_status_t 
advfs_remove_name( 
    int32_t dirFd, 
    char *name
    );

adv_status_t
advfs_rem_bf(
    ino_t ino,
    int32_t fd,
    int32_t flag
    );

adv_status_t
advfs_switch_log(    
    char *domainName,
    vdIndexT volIndex,
    bf_fob_t logFobs,
    serviceClassT logSvc
    );

adv_status_t
advfs_switch_root_tagdir(
    char *domainName,
    vdIndexT volIndex
    );

adv_status_t 
advfs_get_bf_xtnt_map (
    int32_t fd,
    int32_t startXtntMap,
    int32_t startXtnt,
    int32_t xtntArraySize,
    bsExtentDescT *xtntsArray,
    int32_t *xtntCnt,
    vdIndexT *allocVolIndex
    );

adv_status_t 
advfs_move_bf_metadata (
    int32_t fd
    );


adv_status_t 
advfs_migrate (
    int32_t    fd,
    vdIndexT srcVolIndex,
    bf_fob_t srcFobOffset, 
    bf_fob_t srcFobCnt, 
    vdIndexT dstVolIndex,
    bf_vd_blk_t dstBlkOffset,
    uint32_t forceFlag
    );

adv_status_t
advfs_add_volume( 
    char *domain,               /* in - domain name */
    char *volName,              /* in - block special device name */
    serviceClassT *volSvc,    /* in/out - service class */
    bf_vd_blk_t volSize,        /* in - size of the virtual disk */
    bf_fob_t bmtXtntFobs,       /* in - number of fobs (1k) per BMT extent */
    bf_fob_t bmtPreallocFobs,   /* in - number of fobs (1k) to be preallocated for the BMT */
    bfDomainIdT *bfDomainId,  /* out - the domain id */
    vdIndexT *volIndex        /* out - vol index */
    );

adv_status_t
advfs_remove_volume( 
    bfDomainIdT bfDomainId,   /* in */
    vdIndexT volIndex,        /* in */
    uint32_t forceFlag          /* in */
    );

adv_status_t
advfs_add_vol_to_svc_class (
    char *domainName,
    vdIndexT volIndex,
    serviceClassT volSvc
    );

adv_status_t
advfs_remove_vol_from_svc_class (
    char *domainName,
    vdIndexT volIndex,
    serviceClassT volSvc
    );

adv_status_t
advfs_add_vol_done(
    char *domainName,
    char *volName
    );

adv_status_t
advfs_rem_vol_done(
    char *domainName,
    char *volName
    );

adv_status_t
advfs_set_bf_next_alloc_vol (
    int32_t fd,
    vdIndexT curVolIndex,
    bf_fob_t FobCntNeeded,          /* Count of 1k fobs needs */
    int32_t forceFlag
    );

adv_status_t 
advfs_get_bkup_xtnt_map (
    int32_t fd,
    int32_t startXtntMap,
    int32_t startXtnt,
    int32_t xtntArraySize,
    bsExtentDescT *xtntsArray,
    int32_t *xtntCnt,
    vdIndexT *allocVolIndex,
    bf_fob_t *num_fobs
    );

adv_status_t
advfs_extendfs(
    char *domain,               /* in - domain/filesystem name */
    char *blkdev,               /* in - block device to extend */
    uint64_t size,              /* in - size in blocks */
    int32_t flags,              /* in - op flags */
    bf_vd_blk_t *oldBlkSize,    /* out - device old block size */
    bf_vd_blk_t *newBlkSize     /* out - device new block size */
    );

adv_status_t
advfs_ss_dmn_ops(
    char* domainName,
    int32_t type,
    ssDmnOpT *ssDmnCurrentState,     /* out */
    int32_t *ssRunningState             /* out */
    );

adv_status_t
advfs_ss_get_params(
    char* domainName,
    adv_ss_dmn_params_t *ssDmnParams
    );

adv_status_t
advfs_ss_set_params(
    char* domainName,
    adv_ss_dmn_params_t ssDmnParams
    );

adv_status_t
advfs_ss_reset_params(
    char* domainName,
    adv_ss_dmn_params_t ssDmnParams
    );

adv_status_t
advfs_ss_get_fraglist(
    char*        domainName,       /* in */
    vdIndexT   volIndex,         /* in */
    int32_t      ssStartFraglist,  /* in */
    int32_t      ssFragArraySize,  /* in */
    adv_frag_desc_t *ssFragArray,      /* out */
    int32_t     *ssFragCnt         /* out */
    );

adv_status_t
advfs_ss_get_hotlist(
    bfDomainIdT bfDomainId,       /* in */
    int32_t       ssStartHotlist,   /* in */
    int32_t       ssHotArraySize,   /* in */
    adv_ss_hot_desc_t   *ssHotArray,       /* out */
    int32_t      *ssHotCnt          /* out */
    );

/*
 * Statistics prototypes
 */

dmnStatHT *
advfs_start_dmnstat();

adv_dmn_stats_t *
advfs_init_dmnstat( bfDomainIdT dmnid );

int
advfs_init_dmnstat_list( dmnStatHT *dmnSt );

adv_dmn_stats_t *
advfs_get_dmnstat(
    adv_dmn_stats_t *initStat,       /* in (out too, depending on refresh) */
    bfDomainIdT dmnid,        /* in */
    int32_t refresh             /* in */
    );

adv_dmn_stats_t *
advfs_get_dmnstat2(
    adv_dmn_stats_t *initStat,       /* in (out too, depending on refresh) */
    bfDomainIdT dmnid,        /* in */
    int32_t refresh             /* in */
    );

int
advfs_get_dmnstat_list( 
    dmnStatHT *dmnSt,           /* in/out */
    int32_t refresh );          /* in */

void
advfs_free_dmnstat(
    adv_dmn_stats_t *dmnStat     /* in */
    );

void
print_dmnstat( 
    adv_dmn_stats_t *initStat,       /* in */
    bfDomainIdT dmnid         /* in */
    );

void
advfs_print_dmnstat( 
    dmnStatHT *dmnSt,               /* in */
    bfDomainIdT dmnid);           /* in */

dmnStatHT *
advfs_end_dmnstat( dmnStatHT *dmnSp );

void
advfs_start_dstat(int32_t *Ndrive);

mlKdStatT *
advfs_init_dstat(int32_t Ndrive);

mlKdStatT *
advfs_get_dstat( mlKdStatT *initp, int32_t Ndrive );

void
advfs_print_dstats( mlKdStatT *initp, int32_t Ndrive );

/*
 * End statistics prototypes
 */


void
advfs_crash( 
    int32_t when,
    int32_t agent,
    int32_t flags,
    char *domain
    );

adv_status_t
advfs_undel_attach(
    char *dirName,
    char *undelDirName
    );

adv_status_t
advfs_undel_detach(
    char *dirName
    );

adv_status_t 
advfs_undel_get(
    char *dirName,
    bfTagT *undelDirTag
    );

adv_status_t 
advfs_get_name(
    char *mp,           /* in */
    bfTagT tag,       /* in */
    char *name,         /* out  */
    bfTagT *parentTag /* out */
    );

#endif /* _KERNEL */

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

adv_status_t
tag_to_path (
    char *mntOnName,  /* in */
    bfTagT bfTag,     /* in */
    int32_t bufSize,  /* in */
    char *buf         /* in, modified */
);

getDevStatusT
get_dev_size( 
    char  *devName,      /* in - device name */
    char  *devType,      /* in - optional device type ("rz55", etc or NULL) */
    off_t *devSize       /* out - device size */
);

int
advfs_checkvol(
    char *vm_path,            /* in */
    char *vm_diskGroup,       /* out */
    char *vm_volName          /* out */
);

int
advfs_is_lvmdev(
    char *device
);


int32_t advfs_global_lock(
    int32_t nowait,     /* in - whether to wait for the lock */
    int32_t *error      /* out - error if return -1 */
);

int32_t advfs_unlock(
    int32_t lock,       /* in - lock file descriptor */
    int32_t *error      /* out - error if return -1 */
);

int32_t advfs_fspath_lock(
    char *fspath,       /* in - path to filesystem to lock */
    int32_t have_global,/* in - whether the global lock is already held */
    int32_t nowait,     /* in - whether to wait for the lock */
    int32_t *error      /* out - error if return -1,-2, or -3 */
);

int32_t advfs_fspath_longrun_lock(
    char *fspath,          /* in - path to filesystem to lock */
    int32_t have_global,   /* in - whether the global lock is already held */
    int32_t nowait,        /* in - whether to wait for the lock */
    int32_t *error         /* out - error if return -1,-2, or -3 */
);

int32_t advfs_fspath_lock_timed(
    char *fspath,        /* in - path to filesystem to lock */
    int32_t have_global, /* in - whether the global lock is already held */
    int32_t seconds,     /* in - how long to wait for the lock */
    int32_t *error       /* out - error if return -1,-2, or -3 */
);

int32_t lock_file( 
    char *fileName,     /* in - name of file to lock */
    int32_t lkMode,     /* in - lock mode (fcntl.h) */
    int32_t *error      /* out - errno if return -1 */
);

int32_t lock_file_nb( 
    char *fileName,     /* in - name of file to lock */
    int32_t lkMode,     /* in - lock mode (fcntl.h) */
    int32_t *error      /* out - errno if return -1 */
);

int32_t unlock_file( 
    int32_t lockHndl,   /* in - lock handle (really just the file desc) */
    int32_t *error      /* out - errno if return -1 */
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


void *
advfs_xtntmap_open( 
    int32_t fd,
    adv_status_t *status,
    int32_t xtnt_use
    );

void
advfs_xtntmap_close( 
    void *xtntMapD
    );

int
advfs_xtntmap_next_byte( 
    void *xtntMapD,
    adv_off_t curByte,
    adv_off_t *nextByte
    );

int
advfs_xtntmap_next_fob( 
    void *xtntMapD,
    bf_fob_t curFob,
    bf_fob_t *nextFob
    );

char *
advfs_fsname(
    char *devpath_arg
    );


/* Argument processing definitions */

typedef enum {
     UKNOWN,       /* unknown arg type */
     ADVFS,        /* arg was advfs fsdir or full fs spec */
     BLKDEV,       /* block device */
     CHRDEV,       /* char/raw device */
     MNTPT         /* mountpoint */
} adv_argtype_t;

typedef enum {
    OK,            /* not corrupt */
    UNKNOWN,       /* unknown reason */
    DEVERR,        /* device is non-existent or could not be read */
    NOFSETDEV,     /* fileset pseudodevice file is missing */
    PARTPROMOTE,   /* FS is partially promoted */
    PARTDEMOTE     /* FS is partially demoted */
} adv_corrupt_t;

typedef struct arg_info {          
    char fsname[MAXPATHLEN];       /* File system name */
    char fspath[MAXPATHLEN];       /* Full file system path */
    char blkfile[MAXPATHLEN];      /* Block device special file */
    char fset[MAXPATHLEN];         /* "default" or snapshot name */
    char stgdir[MAXPATHLEN];       /* Storage subdirectory */
    uint16_t  is_multivol;         /* Is this a multi-volume enabled 
                                    * (i.e., named) filesystem? 
                                    */
    uint16_t  tcr_shared;          /* Is this filesystem TCR clusterwide? */
    adv_argtype_t  arg_type;       /* Argument provided was of this type */
    adv_corrupt_t  corrupt;        /* Is FS corrupt?  If so, enum says how */
    /* Future addition possibilities */
    //char mountpoint[MAXPATHLEN]; /* Where the file system is mounted */
    //uint64_t vdIndex;            /* Volume index in this file system */ 
} arg_infoT;

char *
dev_to_fsname(char *devpath_arg, arg_infoT* fsinfop);

char *
mountpoint_to_fsname(char *raw_mntpoint, arg_infoT* fsinfop);

char *
arg_to_fsname(char *arg, arg_infoT* fsinfop);

arg_infoT *
process_advfs_arg(char *arg);


int 
advfs_extendfs_generic(
    char *Prog, 
    uint64_t size, 
    int32_t flags, 
    int argc, 
    char **argv
);


int
advfs_valid_sblock(
    char *dev_name,
    int32_t *ret_error
);

int advfs_set_multivol(
    char* devspec
);

int advfs_is_multivol(
    char* devspec, 
    int* ret_err
);


int
advfs_is_mounted(
    arg_infoT*  infop,          /* in, filesystem info struct */
    char*       fset,           /* in, optional, fileset name to check */
    char**      mntopts,        /* out, optional, mountopts of filesystem */
    char**      mntdir          /* out, optional, directory mounted on */
);

/*
 * datastructure for returning filesystem path & mntondir
 * used by advfs_get_all_mounted()
 */
typedef struct {
    char    fspath[MAXPATHLEN+1];
    char    mntdir[MAXPATHLEN+1];
} adv_mnton_t;

int
advfs_get_all_mounted(
    arg_infoT*     infop,       /* in, filesystem info struct */
    adv_mnton_t**  mntArray     /* out, optional, filesystem->mntondir array */
);

int
create_advfs_hierarchy(
    void
);

int
advfs_make_blockdev(
    char*  pathname,
    char*  fsetname,
    int    is_tcr,
    int    is_stripe
);


adv_status_t
advfs_check_on_disk_version(
        uint16_t odv_major,
        uint16_t odv_minor
    );

adv_status_t
advfs_skip_on_disk_version_check();

adv_status_t
advfs_get_vol_index(
    char*            fsname,
    char*            volName,
    vdIndexT*        index,
    serviceClassT* volSvcClass,
    adv_vol_ioq_params_t* volIoQ
);

adv_status_t
advfs_get_statvfs(
    char *domainName,
    char *setName,
    struct statvfs *retStatvfs64
    );


/***                                                             ***
 *                                                                 *
 *  Little syscall routines.  These functions contain the primary  *
 *  logic of the AdvFS system calls.  They should never be called  *
 *  except by advfs_real_syscall(), and the CFS server code.        * 
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

adv_status_t
advfs_syscall_op_get_name2(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_undel_get(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_undel_attach(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_undel_detach(
   libParamsT *libBufp,
   ftxIdT xid
);

adv_status_t
advfs_syscall_op_get_bf_params(
   libParamsT *libBufp,
   opIndexT opIndex,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_set_bf_attributes(
   libParamsT *libBufp,
   struct bfAccess *bfap,
   ftxIdT xid
);

adv_status_t
advfs_syscall_op_get_bf_iattributes(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_set_bf_iattributes(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_move_bf_metadata(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_get_vol_params(
   libParamsT *libBufp,
   opIndexT opIndex
);

adv_status_t
advfs_syscall_op_get_vol_bf_descs(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_set_vol_ioq_params(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_get_bf_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_get_cludio_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_get_bkup_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_get_dmn_params(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_get_dmnname_params(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_get_bfset_params(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_set_bfset_params(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_get_bfset_params_activate(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_fset_get_info(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_fset_create(
   libParamsT *libBufp,
   ftxIdT xid1,
   ftxIdT xid2
);

adv_status_t
advfs_syscall_op_fset_get_id(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_fset_get_stats(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_fset_delete(
   libParamsT *libBufp,
   ftxIdT xid
);

adv_status_t
advfs_syscall_op_create_snapset(
   libParamsT *libBufp,
   ftxIdT cfsXid 
);

adv_status_t
advfs_syscall_op_fset_rename(
   libParamsT *libBufp,
   ftxIdT xid
);

adv_status_t
advfs_syscall_op_dmn_init(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_get_dmn_vol_list(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_migrate(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_rem_name(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_rem_bf(
   libParamsT *libBufp,
   struct bfAccess *bfap
);

adv_status_t
advfs_syscall_op_tag_stat(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_add_volume(
   libParamsT *libBufp,
   opIndexT opIndex
);

adv_status_t
advfs_syscall_op_rem_volume(
   libParamsT *libBufp,
   opIndexT opIndex
);

adv_status_t
advfs_syscall_op_add_rem_vol_svc_class(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_add_rem_vol_done(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_switch_log(
   libParamsT *libBufp,
   ftxIdT xid
);

adv_status_t
advfs_syscall_op_switch_root_tagdir(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_rewrite_xtnt_map(
   libParamsT *libBufp,
   struct bfAccess *bfap,
   ftxIdT xid
);

adv_status_t
advfs_syscall_op_reset_free_space_cache(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_get_global_stats(
   libParamsT *libBufp
);

adv_status_t
advfs_syscall_op_extendfs(
   libParamsT *libBufp,
   ftxIdT xid,
   int fake
);

adv_status_t
advfs_syscall_op_dmn_recreate(
    libParamsT *libBufp
);


/* used in function shipping to locate CFS server */
typedef enum {
        DMN_NAME,
        DMN_ID,
        FILE_PATH,
        FILE_DESC
} ServerSearchKeyT;

#endif /* _ADVFS_SYSCALLS_INCLUDED */
