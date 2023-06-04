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
 *      Bitfile on-disk structures.
 */

#ifndef BS_ODS
#define BS_ODS 

/*
 * "Include" section
 */
#include <sys/param.h>
#include <sys/types.h>
#include <advfs/advfs_types.h>
#include <sys/fs/advfs_public.h>
#include <advfs/bs_public.h>

/****************************************************************************/
/****  AdvFS disk layout                                                 ****/
/****                                                                    ****/
/****  Blks (1k)         Usage                                           ****/
/****  ------------      ------------------------------------            ****/
/****    0 thru   7      RESERVED, NEVER used by AdvFS                   ****/
/****                    - HP-UX usage includes LIF/bootloader info, etc ****/
/****    8 thru  15      AdvFS Fake super block                          ****/
/****   16 thru  31      RBMT and BMTpage0                               ****/
/****   32 thru 159      Cluster Private Data Area (CPDA)                ****/
/****  160 thru 175      Real boot blocks (MISC rsvd mcell)              ****/
/****                                                                    ****/
/****************************************************************************/

/*
 * The location of the RBMT should remain at offset 16K and should always be
 * aligned on an 8K boundry to assist recovery tools. 
 *
 * It should NEVER be relocated unless absolutely necessary. 
 * 
 */
#define RBMT_BLK_LOC          16  /* RBMT starting sector (1024 byte sectors)*/
#define ADVFS_RESERVED_BLKS   RBMT_BLK_LOC /* rsvd space, includes fake SB */

#define CPDA_BLK_LOC          32  /* CPDA starting sector (1024 byte sectors)*/
#define CPDA_SIZE_BYTES  0x20000  /* 128K needed today  */
#define CPDA_RESERVED_BLKS     (CPDA_SIZE_BYTES / DEV_BSIZE )


/*
 * Magic numbers for validation of on-disk structures.
 * On-disk magic numbers begin with 0xADF001 to help distinguish from
 * in-memory magic numbers.
 */
#define RBMT_MAGIC       0xADF00101    /* RBMT bsMPgT */
#define BMT_MAGIC        0xADF00102    /* BMT bsMPgT */
#define SBM_MAGIC        0xADF00103    /* bsStgBmT */
#define RTTAG_MAGIC      0xADF00104    /* Root Tag File bsTDirPgHdrT */
#define TAG_MAGIC        0xADF00105    /* Tag File bsTDirPgHdrT */
#define LOG_MAGIC        0xADF00106    /* logPgHdrT */

/****************************************************************/
/****  Structure version constant - change this whenever  *******/
/****  an on-disk structure is modified *************************/
/****                                                     *******/
/****************************************************************/
#define BFD_ODS_LAST_VERSION_MAJOR    5
#define BFD_ODS_LAST_VERSION_MINOR    0

/****************************************************************/
/**** These constants define the allowable on-disk versions  ****/
/**** for newly-created domains.                             ****/
/****************************************************************/
#define FIRST_VALID_FS_VERSION_MAJOR  5
#define FIRST_VALID_FS_VERSION_MINOR  0

#define LAST_VALID_FS_VERSION_MAJOR   5
#define LAST_VALID_FS_VERSION_MINOR   0

/****************************************************************/
/****                Version-dependent macros                ****/
/****                                                        ****/
/****  These macros are used to determine whether certain    ****/
/****  features are in use.  Since these featured are tied   ****/
/****  to certain on-disk changes, the on-disk version is    ****/
/****  used in the determination of whether the feature is   ****/
/****  in use or not.                                        ****/
/****************************************************************/


typedef enum {
    BFM_RBMT =      0,   /* Reserved Bitfile Metadata Table */
    BFM_SBM =       1,   /* Storage BitMap */
    BFM_BFSDIR =    2,   /* BitFileSetDirectory */
    BFM_FTXLOG =    3,   /* FTX Log file */
    BFM_BMT =       4,   /* Bitfile Metadata Table */
    BFM_MISC =      5,   /* Bitfile for fake super block, etc. */
    BFM_RBMT_EXT =  6,   /* Extension mcell of BFM_RBMT */
    BFM_DMN_NAME =  7,   /* mcell for bsDmnNameT record */
    BFM_RUN_TIMES = 8    /* mcell for bsRunTimesRT record */
} bfdBfMetaT;


/* #define BFM_RSVD_CELLS 9 defined in  bs_public.h */


#if defined(HPUXBOOT) | defined(LINT)

#define BSR_NIL           0
#define BSR_XTNTS         1 
#define BSR_ATTR          2
#define BSR_XTRA_XTNTS    5
#define BMTR_FS_DATA    254
#define BMTR_FS_STAT    255

typedef unsigned char bsMcellType;

#else   /* HPUXBOOT | LINT */

typedef unsigned char enum {
    BSR_NIL                    =   0,   /* nil or unused record  */
    BSR_XTNTS                  =   1,   /* bsXtntRT              */
    BSR_ATTR                   =   2,   /* bsBfAttrT             */
    BSR_VD_ATTR                =   3,   /* bsVdAttrT             */
    BSR_DMN_ATTR               =   4,   /* bsDmnAttrT            */
    BSR_XTRA_XTNTS             =   5,   /* bsXtraXtntRT          */
    BSR_MCELL_FREE_LIST        =   6,   /* bsMcellFreeListT      */
    BSR_BFS_ATTR               =   7,   /* bsBfSetAttrT          */
    BSR_VD_IO_PARAMS           =   8,   /* vdIoParamsT           */
    BSR_DEF_DEL_MCELL_LIST     =   9,   /* delLinkRT             */
    BSR_DMN_MATTR              =  10,   /* bsDmnMAttrT           */
    BSR_BF_INHERIT_ATTR        =  11,   /* bsBfInheritAttrT      */
    BSR_BFS_QUOTA_ATTR         =  12,   /* bsQuotaAttrT          */
    BSR_ACL_REC                =  13,   /* bsr_acl_rec_t         */
    BSR_DMN_TRANS_ATTR         =  14,   /* bsDmnTAttrT           */
    BSR_DMN_SS_ATTR            =  15,   /* bsSSDmnAttrT          */
    BSR_DMN_FREEZE_ATTR        =  16,   /* bsDmnFreezeAttrT      */
    BSR_RUN_TIMES              =  17,   /* bsRunTimesRT          */
    BSR_DMN_NAME               =  18,   /* bsDmnNameT            */
    BSR_DMN_UMT_THAW_TIME      =  19,   /* bsDmnUmntThawT        */
/*  BSR_MAX                    =  19,    DEFINED IN bs_public.h  */
    /*
     * File system level bmt record types start with 255 and count backwards.
     */
    BMTR_FS_DIR_INDEX_FILE     = 250,
    BMTR_FS_INDEX_FILE         = 251,
    BMTR_FS_TIME               = 252,
    BMTR_FS_UNDEL_DIR          = 253,
    BMTR_FS_DATA               = 254,   /* symlink record */
    BMTR_FS_STAT               = 255
} bsMcellType;

#endif  /* HPUXBOOT | LINT */

/*
 * bsMRT - mcell record header
 *
 * Each record in an mcell is preceeded by this record header.
 *
 * Note that records must start on an 8 byte boundary due to 8 byte
 * alignment issues in the mcell records.  The bCnt in each record
 * header is not rounded up but whenever it is used to calculate the
 * location of the next record header then it is rounded up to the
 * next 8 byte boundary.
 */

typedef struct bsMR {
    uint16_t     bCnt;
    bsMcellType  type;      /* Type of structure contained by record */
    char         rsvd1;
    uint32_t     padding;   /* Padding to ensure 8 byte mcell alignment */
} bsMRT;

/*
 * All user files require bsBfAttr, bsXtntR, and fs_stat (statT) records.
 * This group of records is often refered to as the Primary Mcell Records
 * and they form the basis for calculating the size (BSC_R_SZ) of a mcell. 
 * Storing all 3 Primary Mcell records into a single mcell help minimize
 * the IO required to process a file. 
 *
 */

typedef enum {
    BSRA_INVALID, BSRA_CREATING, BSRA_DELETING, BSRA_VALID
} bfStatesT;

/*
 * Bitfile attributes
 */
typedef struct bsBfAttr {
    bf_fob_t    bfPgSz;              /* Bitfile area page size in 1k fobs  */
    ftxIdT      transitionId;        /* ftxId when ds state is ambiguous */
    bfStatesT   state;               /* bitfile state of existence */
    serviceClassT reqServices;
    int32_t     bfat_del_child_cnt;  /* Number of children to wait for before
                                        deleting the file. Used to defer delete
                                        of parent snapshots. */
    uint32_t    rsvd1;
    uint64_t    bfat_orig_file_size; /* filesize at time of snapshot creation */
    uint64_t    bfat_rsvd_file_size; /* minimum space to reserve for file */
    uint64_t    rsvd3;
    uint64_t    rsvd4;
} bsBfAttrT;


/*
 * The basic disk extent mapping structure is simply
 * an array of bitfile/logical block number pairs.
 * It is designed for efficient binary searching.
 */
typedef struct bsXtnt {
    bf_fob_t    bsx_fob_offset; /* Starting 1k file offset block */
    bf_vd_blk_t bsx_vd_blk;     /* Starting DEV_BSIZE disk block */
} bsXtntT;


/*
 * An extent with vdBlk set to XTNT_TERM terminates the previous descriptor.
 */
#define XTNT_TERM ((uint64_t)-1)

/*
 * An extent with vdBlk set to COWED_HOLE starts a hole that has been 
 */

/*
 * COWed to a child snapshot.
 */
#define COWED_HOLE ((uint64_t)-2)

/*
 * The boot loader still uses the PERM_HOLE_START #define.
 * - remove once the boot code has been updated
 */
#define PERM_HOLE_START COWED_HOLE


/*
 * Definition of delete link for global deferred deleted list.
 */
typedef struct {
    bfMCIdT  nextMCId;      /* next mcell in the list */
    bfMCIdT  prevMCId;      /* preceding mcell in list */
} delLinkT;

typedef delLinkT delLinkRT;

/*
 * Structure which records the progress of a delete.
 */
typedef struct {
    bfMCIdT  mcid;           /* first unfinished mcell in del pend chain */
    bf_fob_t drtFobOffset;   /* first unprocessed FOB in extent */
    uint64_t drtQuotaBlks;   /* quota blocks already freed */
    uint32_t xtntIndex;      /* index of the first unfinished extent */
    uint32_t rsvd1;          /* padding due to structure alignment */ 
} delRstT;

#define BMT_XTNTS 2             /* num. xtnt descriptors per bsXtntR */

typedef struct bsXtntR {
    bfMCIdT  chainMCId;         /* link to next extent record in the chain */
    delLinkT delLink;           /* doubly linked global deferred delete list */
    delRstT  delRst;            /* large bitfile deletion management */
    uint32_t mcellCnt;          /* total number of mcells with descriptors */
    uint16_t xCnt;              /* Count of elements used in bsXA */
    uint16_t rsvd1;             /* padding caused by alignment issues */
    bsXtntT  bsXA[BMT_XTNTS];   /* Array of disk extent descriptors */
    uint64_t rsvd2;
} bsXtntRT;


struct fs_stat
{
    bfTagT     st_ino;
    adv_mode_t st_mode;
    adv_uid_t  st_uid;
    adv_gid_t  st_gid;
    adv_off_t  st_size;
    int64_t    st_atime;
    adv_dev_t  st_rdev;
    int64_t    st_mtime;
    int64_t    st_ctime;
    int32_t    st_natime;
    int32_t    st_nmtime;
    int32_t    st_nctime;
    uint32_t   st_flags;      /* user defined flags for file */
    bfTagT     dir_tag;       /* tag of parent directory */
    uint64_t   st_nlink;
    uint64_t   st_rsvd1;
    uint64_t   st_rsvd2;
};

typedef struct fs_stat statT;


/*==========================================================================*/

/*  Mcell Header  */
typedef struct bs_mcell_hdr {
    bfMCIdT   nextMCId;     /* Link to next mcell */
    bfTagT    tag;          /* Tag this mcell is assigned to */
    bfTagT    bfSetTag;     /* tag of this bitfile's bf set dir */  
    uint32_t  linkSegment;  /* segment in link, starts at zero */
    uint16_t  numRecords;   /* number of records in mcell */
    uint16_t  rsvd1;        /* Needed for byte alignment */
    uint64_t  rsvd2;
} bs_mcell_hdr_t;

#define mcNextMCId     bs_mcell_hdr.nextMCId
#define mcTag          bs_mcell_hdr.tag
#define mcBfSetTag     bs_mcell_hdr.bfSetTag
#define mcLinkSegment  bs_mcell_hdr.linkSegment
#define mcNumRecords   bs_mcell_hdr.numRecords
#define mcRsvd1        bs_mcell_hdr.rsvd1
#define mcRsvd2        bs_mcell_hdr.rsvd2 

/* 
 * Group the nextFreeMcell and freeMcellCnt into a structure and pass 
 * sizeof(bs_bmt_track_free_mcells_t) when calling rbf_pin_record() since
 * since these 2 values must be updated together. 
 *
 */
typedef struct bs_bmt_track_mcells {
    uint16_t nextFreeMcell; /* Next free mcell on this page */
    uint16_t freeMcellCnt;  /* Number of free mcells on this page */
} bs_bmt_track_mcells_t;

/* BMT Page Header  */
typedef struct bs_bmt_pg_hdr {
    uint32_t              magicNumber;
    union {
        adv_ondisk_version_t ODSVersion; /* RBMT only ! */
        uint32_t             rsvd1;      /* BMT (only) reserved field */
    } bsMPg_data;
    bfFsCookieT           fsCookie;
    bs_meta_page_t        nextFreePg;    /* Next page in the mcell free list */
    bs_meta_page_t        pageId;        /* Page number */
    vdCookieT             vdCookie;      /* unique volumeId */
    vdIndexT              vdIndex;       /* volume index - 1 based */ 
    bs_bmt_track_mcells_t free_mcells;   /* next free mcell & free mcell cnt */
    uint64_t              rsvd2;
} bs_bmt_pg_hdr_t;

#define bmtMagicNumber     bs_bmt_pg_hdr.magicNumber
#define bmtODSVersion      bs_bmt_pg_hdr.bsMPg_data.ODSVersion
#define bmtRsvd1           bs_bmt_pg_hdr.bsMPg_data.rsvd1
#define bmtFsCookie        bs_bmt_pg_hdr.fsCookie
#define bmtPageId          bs_bmt_pg_hdr.pageId
#define bmtNextFreePg      bs_bmt_pg_hdr.nextFreePg
#define bmtVdCookie        bs_bmt_pg_hdr.vdCookie
#define bmtVdIndex         bs_bmt_pg_hdr.vdIndex
#define bmtTrackMcells     bs_bmt_pg_hdr.free_mcells
#define bmtNextFreeMcell   bs_bmt_pg_hdr.free_mcells.nextFreeMcell
#define bmtFreeMcellCnt    bs_bmt_pg_hdr.free_mcells.freeMcellCnt
#define bmtRsvd2           bs_bmt_pg_hdr.rsvd2

/*
 * MIN_MCELL_SIZE defines size of the  smallest Mcell (bsMCT) needed to
 * store a Primary Mcell Record. The Primary Mcell Record consists of the
 * the bsBfAttr, bsXtntR, and statT records. Space is also required to
 * store the Mcell header (bs_mcell_hdr_t) and 4 Mcell Record headers (bsMRT).
 * The 4 record headers include one for each record and a terminator with
 * type BSR_NIL.
 */
#define MIN_MCELL_SIZE  \
    (sizeof(bs_mcell_hdr_t) + (sizeof(bsBfAttrT) + sizeof(bsXtntRT) + \
     sizeof(statT) + 4*sizeof(bsMRT)))

/*
 * BSPG_CELLS defines the number of Mcells (bsMCT) that will fit on a
 * BMT/RBMT page (bsMPgT).
 */
#define BSPG_CELLS  \
    ((ADVFS_METADATA_PGSZ - sizeof(bs_bmt_pg_hdr_t)) / MIN_MCELL_SIZE )

/*
 * BMT_FREE_SPACE defines the amount of space remaining on a BMT/RBMT
 * page after allocating the space required to store as many minimum
 * sized mcells as possible.
 *
 */
#define BMT_FREE_SPACE  \
 ((ADVFS_METADATA_PGSZ - sizeof(bs_bmt_pg_hdr_t)) - (BSPG_CELLS * MIN_MCELL_SIZE))

/*
 * BSC_R_SZ defines the actual Mcell size.  It is calculated by adding any 
 * free BMT space to the minimum mcell size.
 * NOTE:  Free BMT space can must be allocated to mcells in 8-byte units
 *        to ensure 8-byte alignment for mcell records.
 */
#define BSC_R_SZ  \
    (MIN_MCELL_SIZE - sizeof(bs_mcell_hdr_t) + \
     (((BMT_FREE_SPACE / BSPG_CELLS) / sizeof(uint64_t))*sizeof(uint64_t)))

/*
 * BSPG_FILLER defines the amount of free space on a BMT/RBMT page that has
 * not been consumed by the BMT header and mcells. It is used to pad out the
 * BMT/RBMT page to ADVFS_METADATA_PGSZ.
 */
#define BSPG_FILLER  \
    (ADVFS_METADATA_PGSZ - (sizeof(bs_bmt_pg_hdr_t) + BSPG_CELLS*sizeof(bsMCT)))

/*
 * BS_USABLE_MCELL_SPACE defines the largest mcell record possible:
 *
 * BS_USABLE_MCELL_SPACE = (Mcell Size - (header + terminating records))
 */
#define BS_USABLE_MCELL_SPACE   (BSC_R_SZ - (2*sizeof(bsMRT)) )



/******************************************************************************
 * The next section contains structure definitions for the remainder of the   *
 * typed records in the BMT. Basically all the BMT records except for the     * 
 * 3 Primary Mcell records.                                                   * 
 *****************************************************************************/

/* 
 * Bitfile client attributes.  These attributes are freely changeable
 * by clients (users and file system layer).  This struct is also used
 * for the inheritable attributes which the file system layer associates
 * with directories.
 */
typedef struct bsBfClAttr {
    serviceClassT reqServices;    /* required service class */
} bsBfClAttrT;

/*------------------------------------------------------------*/

typedef struct bsVdAttr {
    bsIdT vdMntId;              /* vd mount id */
    bsVdStatesT state;          /* vd state */
    vdIndexT    vdIndex;        /* this vd's index in the domain */
    vdCookieT   vdCookie;       /* unique volumeId */
    bf_vd_blk_t vdBlkCnt;       /* number blocks on this volume */
    bf_vd_blk_t sbmBlksBit;     /* SBM blocks to bit */
    bf_fob_t bmtXtntPgs;        /* number of pages per BMT extent */
    bf_fob_t userAllocSz;       /* allocation unit for user data in 1k fobs */
    bf_fob_t metaAllocSz;       /* allocaiton unit for metadata in 1k fobs */
    uint32_t blkSz;             /* block size - typically DEV_BSIZE */
    serviceClassT serviceClass;
    uint64_t rsvd1;
    uint64_t rsvd2;
} bsVdAttrT;


/*------------------------------------------------------------*/

typedef struct bsDmnAttr {
    bfDomainIdT bfDomainId;   /* id of this bitfile domain */
    bfFsCookieT MasterFsCookie; /* creation time for use as ID, initial bfDomainId */
    bfTagT bfSetDirTag;       /* tag of root bitfile-set tag directory */
    uint32_t maxVds;          /* maximum vd index */
    uint32_t rsvd1;
    uint64_t rsvd2;
} bsDmnAttrT;



/*------------------------------------------------------------*/

#define BMT_XTRA_XTNTS \
    ((BSC_R_SZ - 2*sizeof(bsMRT) - (2*sizeof(uint16_t)) - (2*sizeof(uint32_t))) \
                        / sizeof(bsXtntT))

typedef struct bsXtraXtntR {
    uint16_t xCnt;                  /* Count of elements used in bsXA */
    uint16_t rsvd1;
    uint32_t rsvd2;
    bsXtntT bsXA[BMT_XTRA_XTNTS];  /* Array of disk extent descriptors */
} bsXtraXtntRT;


/*------------------------------------------------------------*/

typedef struct {
    bs_meta_page_t headPg;          /* first page of mcell free list */
    uint64_t rsvd1;
} bsMcellFreeListT;


/*------------------------------------------------------------*/

typedef struct {
    bfSetIdT bfSetId;                   /* bitfile-set's ID */
    bfTagT nxtDelPendingBfSet;          /* next delete pending bf set */
    adv_dev_t fsDev;                    /* Unique ID */
    adv_uid_t uid;                      /* set's owner */
    adv_gid_t gid;                      /* set's group */
    adv_mode_t mode;                    /* set's permissions mode */
    char setName[BS_SET_NAME_SZ];       /* bitfile set's name */
    /* Snapshot related fields */
    bfSetIdT    bfsaParentSnapSet;      /* Parent fileset to this fileset in 
                                         * the snapset tree
                                         */
    bfSetIdT    bfsaFirstChildSnapSet;  /* bfSetId of first snapset child */
    bfSetIdT    bfsaNextSiblingSnap;    /* bfSetId of next sibling snapset */
    uint16_t    bfsaSnapLevel;          /* 0 = root. Greater than 0 indicates 
                                         * number of parents back to root.
                                         */
    uint16_t state;                     /* state of bitfile set (bfSetStateT) */
    uint16_t flags;                     /* The low order 16 bits of bfSet flags
                                         *  that are defined in bfs_flags_t
                                         */
    uint16_t    rsvd1;                  /* Only padding is reserved space */
    adv_time_t  bfsaFilesetCreate;      /* Creation time for the fileset */
    adv_threshold_ods_t fsetThreshold;  /* filesets threshold values */
    uint64_t    rsvd2;
    uint64_t    rsvd3;
    uint64_t    rsvd4;
    uint64_t    rsvd5;
    uint64_t    rsvd6;
} bsBfSetAttrT;


/*
 * Flags for bfSets.  Flags beginning with BFS_OD are on disk flags and the
 * values cannot change without affect on-disk layout.  Flgs beginning with
 * BFS_IM are in memory.  The low order 16  bits are reserved for on disk
 * flags.
 */
typedef enum {
    BFS_OD_OUT_OF_SYNC  =       0x0001, /* Snapshot could not allocate storage for a
                                         * COW.  Some protion of this
                                         * fileset is out of sync */
    BFS_OD_HARD_OUT_OF_SYNC =   0x0002, /* Files are out of sync, but not marked as such. */
    BFS_OD_OBJ_SAFETY   =       0x0010, /* enables/disables forcing zeroed
                                         * newly allocated storage in a file
                                         * to disk before allowing the file
                                         * to have the storage. */
    BFS_OD_READ_ONLY    =       0x0020, /* Indicates the file system is read-only.  
                                         * This is set indicate a RO
                                         * snapshot */
    BFS_OD_ROOT_SNAPSHOT =      0x0040, /* This fileset is a root of a snapshot tree.
                                         * No files have parents */
    BFS_IM_ON_DISK_MASK =  0x0000ffff,  /* Used to select the on-disk flags */
    BFS_IM_DIRECTIO     =  0x00010000,  /* Default direct I/O */
    BFS_IM_NOATIMES     =  0x00020000,  /* Don't update access time on read */
    BFS_IM_GRPID        =  0x00040000,  /* New files get parent dir's group */
    BFS_IM_SNAP_IN_PROGRESS =0x00080000,/* The fielset is currently being snapped,
                                         * all new getpage write requests 
                                         * must synchronize */
    BFS_IM_FAILOVER     =  0x00100000,  /* CFS failover/recovery in progress */
    BFS_IM_NOREADAHEAD  =  0x00200000   /* Don't do read-ahead on this FS */
} bfs_flags_t;

    
/*
 * The migrate mcell list contains a list of mcells used during
 * bitfile migration.  Each mcell describes transition storage
 * used by migrate.
 *
 * A group of related mcells are linked together via the "nextMCId"
 * field in each mcell's header (bsMCT).  The groups are linked together
 * via the "nextMCId" field (bsMigStgT) in the first mcell of each group.
 * The groups are linked to bsMigMcellListT via the "firstMCId" field.
 */

/*------------------------------------------------------------*/

typedef struct vdIoParams {
    uint32_t rdMaxIo;      /* Current read/write IO transfer sizes */
    uint32_t wrMaxIo;      /* in terms of device blocks. */
    int32_t vdiop_reserved1;
    int32_t vdiop_reserved2;
    int32_t vdiop_reserved3;
    int32_t vdiop_reserved4;
} vdIoParamsT;

/*------------------------------------------------------------*/

#define RBMT_END_SHADOW(vdBlkCnt) \
    rounddown((vdBlkCnt),         \
              ( ADVFS_METADATA_PGSZ_IN_FOBS / ADVFS_FOBS_PER_DEV_BSIZE )) \
               - ( ADVFS_METADATA_PGSZ_IN_FOBS / ADVFS_FOBS_PER_DEV_BSIZE )

#define RBMT_MID_SHADOW(vdBlk) \
    rounddown((vdBlk),         \
              ( ADVFS_METADATA_PGSZ_IN_FOBS / ADVFS_FOBS_PER_DEV_BSIZE )) 

#define RBMT_SHADOWS 2

typedef struct sdw_bsRbmtShadowR { 
    uint16_t sdw_pageSize;                    /* page size in 1k fobs */
    uint16_t sdw_seqno;
    uint16_t sdw_actualNumCurPageShadows;
    uint16_t sdw_actualNumNextPageShadows;
    bsXtntT  sdw_curPageShadows[RBMT_SHADOWS];
    bsXtntT  sdw_nextPageShadows[RBMT_SHADOWS];
} bsRbmtShadowRT;

/*------------------------------------------------------------*/

typedef struct bsRunTimesR { 
    adv_time_t rt_active_fsck;
    adv_time_t rt_unmounted_fsck;
    adv_time_t rt_salvage; 
    uint64_t   rt_rsvd1;
    uint64_t   rt_rsvd2;
    uint64_t   rt_rsvd3;
    uint64_t   rt_rsvd4;
    uint64_t   rt_rsvd5;
} bsRunTimesRT;


/*------------------------------------------------------------*/

typedef struct bsDmnName {
       char dmn_name[BS_DOMAIN_NAME_SZ];
} bsDmnNameT;


/*------------------------------------------------------------*/

/* Latest unmount/thaw time used by scan to help differentiate 
 * between original and split-mirror devices.
 */
typedef struct bsDmnUmntThaw {
    bsIdT  dmnUmntThawTime;       /* Latest unmount/thaw time */ 
} bsDmnUmntThawT;

/*------------------------------------------------------------*/

#define    DMNS_RECOVERY_FAILED      0x0001 
#define    DMNS_CLEAR_RECOVERY       0xfffe 
#define    DMNS_DOMAIN_PANIC         0x0002

/*
 * Only one disk contains the most current domain mutable attributes
 * record.  The record with the highest sequence number is
 * the current record.  This record is "moved" only in lgr_switch_vol().
 */
typedef struct bsDmnMAttr {
    adv_ondisk_version_t ODSVersion;  /* Must be first field in record */
    uint32_t seqNum;                  /* sequence number of this record */
    bfTagT delPendingBfSet;           /* head of 'bf set delete pending' list */ 
    adv_uid_t uid;                    /* domain's owner */
    adv_gid_t gid;                    /* domain's group */
    adv_mode_t mode;                  /* domain's access permissions */
    uint16_t vdCnt;                   /* number of disks in the domain */ 
    uint16_t bs_dma_dmn_state;        /* Domain sate  */
    uint32_t ftxLogPgs;               /* number of pages in log */
    bfTagT bfSetDirTag;               /* tag of root bfset tag directory */
    bfTagT ftxLogTag;                 /* tag of domain log */
    adv_threshold_ods_t dmnThreshold; /* domain threshold values */
    adv_timeval_t bs_dma_forced_mnt;  /* time mount forced paniced domain */  
    uint64_t rsvd1;
    uint64_t rsvd2;
} bsDmnMAttrT;


/*------------------------------------------------------------*/

/*
 * Bitfile inheritable attributes.  see bs_inherit().
 */

/*
 * The bsBfAttr record no longer includes the bsBfClAttr record
 * since only the reqServices field remains.  The same change
 * could be made to bsBfInheritAttr BUT there may be a need/use
 * for inheritable attributes that are not stored on disk in
 * bsBfAttr.  For example, attributes could be inherited from
 * a parent to be used when creating an access structure. 
 */
typedef bsBfClAttrT bsBfInheritAttrT;


/*
 * bsQuotaAttrT - Per-fileset quota attributes
 */

typedef struct bsQuotaAttr {
    uint64_t blkHLimit;         /* hard block limit */          
    uint64_t blkSLimit;         /* soft block limit */
    uint64_t fileHLimit;        /* hard file limit */
    uint64_t fileSLimit;        /* soft file limit */
    int64_t  blkTLimit;         /* time limit for excessive disk blk use */    
    int64_t  fileTLimit;        /* time limit for excessive file use */
    uint32_t quotaStatus;       /* quota system status */
    uint32_t rsvd1;
    uint64_t rsvd2;
} bsQuotaAttrT;


typedef enum {
    BSR_VD_NO_OP,
    BSR_VD_ADDVOL,
    BSR_VD_RMVOL
} bsVdOpT;

typedef struct bsDmnTAttr {
    bfMCIdT   chainMCId;
    adv_dev_t dev;
    bsVdOpT   op;
    uint32_t  rsvd1;
} bsDmnTAttrT;

/*------------------------------------------------------------*/

/*
 * vfast on-disk record
 *
 * WARNING: If changes are made to the following vfast enum or structure,
 *          make the same changes to the sbin file vods/print_mcells.c 
 */


typedef enum
{
    SS_DEACTIVATED,/* vfast disabled on domain */
    SS_ACTIVATED,  /* vfast enabled on domain */
    SS_SUSPEND,    /* vfast has work suspended, lists still active */
    SS_ERROR,      /* vfast is disabled on domain due to unknown error */
    SS_CFS_RELOC,  /* CFS is in process of relocating domain to a new server node.
                    * vfast will have this state, if current state was SS_ACTIVATED,
                    * until last umount, when domain deactivation will reset state
                    * to SS_ACTIVATED. Special cluster reloc state that is not to be
                    * written to disk.
                    */
    SS_CFS_MOUNT,
    SS_CFS_UMOUNT
                   /* CFS is in process of (u)mounting. vfast will be temporarily
                    * set to these states, IF current state is SS_ACTIVATED. Otherwise,
                    * vfast state is untouched during the cluster (u)mount.
                    */
} ssDmnOpT;

typedef struct bsSSDmnAttr {
    ssDmnOpT   ssDmnState;        /* processing state that domain is in */
    /* UI visable configurable options */
    uint16_t    ssDmnDefragment;
    uint16_t    ssDmnSmartPlace;
    uint16_t    ssDmnBalance;
    uint16_t    ssDmnVerbosity;
    uint16_t    ssDmnDirectIo;
    uint16_t    ssMaxPercentOfIoWhenBusy;
    /* output only variables */
    uint64_t    ssFilesDefraged;
    uint64_t    ssFobsDefraged;
    uint64_t    ssFobsBalanced;
    uint64_t    ssFilesIOBal;
    uint64_t    ssExtentsConsol;
    uint64_t    ssPagesConsol;
    /* UI invisible configurable options */
    uint16_t    ssAccessThreshHits;
    uint16_t    ssSteadyState;
    uint16_t    ssMinutesInHotList;
    uint16_t    ssDmnDefragAll;
    uint32_t    ssReserved1;
    uint32_t    ssReserved2;
    uint64_t    ssReserved3;
    uint64_t    ssReserved4;
} bsSSDmnAttrT;


/*------------------------------------------------------------*/

typedef struct bsDmnFreezeAttr {
    int64_t  freezeBegin;       /* Time of last dmn freeze */
    int64_t  freezeEnd;         /* Time of last dmn thaw   */
    uint32_t freezeCount;       /* Number of times domain has been frozen */
    uint32_t rsvd1;
    uint32_t rsvd2;
    uint32_t rsvd3;
} bsDmnFreezeAttrT;


/*------------------------------------------------------------*/

/*
 * ACL on disk structures
 */

/* AdvFS version of the aclv structure defined in aclv.h */
typedef struct advfs_acl {
    int32_t  aa_a_type;  /* acl entry type */
    int32_t  aa_a_id;    /* user or group ID */
    int16_t  aa_a_perm;  /* Permissions */
    uint16_t padding;    /* padding due to structure alignment */    
} advfs_acl_t;

/* On-Disk Data Structures */
typedef struct bsr_acl_rec_hdr {
    uint16_t  bar_acl_total_cnt;    /* Num of entries for entire ACL */
    uint16_t  bar_acl_cnt;          /* Num of entries for this record */
    uint16_t  bar_acl_link_seg;     /* Place in ACL mcell list */
    uint16_t  bar_acl_type;         /* SYSV_ACLS */
} bsr_acl_rec_hdr_t;

/*
 * Largest number of ACL entries that can be stored in a BMT mcell record.
 * This is a little over 20 (about 22) as of mid January 2004.
 */
#define ADVFS_ACLS_PER_MCELL ( ( BSC_R_SZ - ( ( 2*sizeof(bsMRT) ) + \
        sizeof(bsr_acl_rec_hdr_t) ) )/sizeof(advfs_acl_t) )

typedef struct bsr_acl_rec {
    bsr_acl_rec_hdr_t  bar_hdr;
    advfs_acl_t        bar_acl[ADVFS_ACLS_PER_MCELL];   /* ACL entry */
} bsr_acl_rec_t;



/*
 * Store the AdvFS Fake Super Block Data in the RBMT (and shadow copies)
 * to help fsck rebuild it if necessary.
 */
typedef struct bsr_advfs_fsb_data {
    advfs_fsb_data_t   advfs_fsb_data;
} bsr_advfs_fsb_data_t;


/******************************************************************************
 * Basic Bitfile M(ap/eta) Table (BMT) structures.
 * The BMT bitfile has a 3 level hierarchical structure:
 *  - BMT pages
 *  - BMT cells
 *  - BMT records
 *
 * Pages are an integral number of blocks with a fixed header
 * containing a log sequence number (LSN) which coordinates updates
 * with log records that describe the update.  Pages are the unit
 * of buffer access/update (read/write).  Pages contain a fixed
 * number of fixed length cells.
 *
 * Cells are the unit of storage assignment within the BMT.  A cell
 * is assigned to serve a specific bitfile.  A cell may be linked
 * to additional cells in a simple singly linked list.  A cell has a
 * fixed header with its assigned tag and a link to additional cells.
 * After the fixed header is a sequence of variable length recorbs.
 *
 * Records are typed, variable length units that contain various
 * structs such as mapping extents and bitfile attributes.
 *
 * A NIL record terminates the record list within a cell.
 *
 * Pages are 8192 = 8 + (31 * 264 byte cells).
 *
 ******************************************************************************/

/*
 * bsMCT
 *
 * Bitfile metadata cell (mcell).  Used to store bitfile metadata
 * like attributes, extent maps, etc.  The metadata is stored in
 * typed records which are kept in the 'bsMR0' byte array.
 *
 * NOTE_1: The tag and bfSetTag structures are padded to 16 bytes and they
 *         must be aligned on an 8 byte boundry within the bsMCT otherwise
 *         the compiler will add additional padding in the bsMCT and casue
 *         an overflow problem when the size exceeds ADVFS_METADATA_PGSZ.
 * 
 * NOTE_2: The bsMR0 array MUST be 8 byte aligned to ensure alignment
 *         within the mcells.
 *       
 */

typedef struct bsMC {
    bs_mcell_hdr_t  bs_mcell_hdr;
    char            bsMR0[BSC_R_SZ]; /* Mcell records
                                      * - MUST BE 64bit ALIGNED!
                                      */
} bsMCT;

/*
 * The number of mcells that fits on a single BMT page.
 *
 * BSPG_CELLS equals 21 !!!
 */

/* Test the validity of a mcell number. */
/* "Return" E_RANGE if the mcell number is invalid. Else "return" EOK. */
/* Proper range test depends on cn being an unsigned number. */
#define TEST_CELL(cn) ( (cn) >= BSPG_CELLS ? E_RANGE : EOK )

/* Last mcell on RBMT page reserved for RBMT itself */
#define RBMT_RSVD_CELL (BSPG_CELLS - 1)

/* 
 * NOTE: The mcell array (bsMCA[]) must be aligned on an 8 byte boundry
 *       to ensure alignment for records within the mcells.
 */
typedef struct bsMPg {
    bs_bmt_pg_hdr_t  bs_bmt_pg_hdr;
    char             filler[BSPG_FILLER]; /* Pad to 8192 bytes
                                           * - placed before mcell array
                                           *   to allow bmt header to grow
                                           *   without impacting basic bmt
                                           *   page layot.
                                           */
    struct bsMC      bsMCA[BSPG_CELLS];   /* Array of Mcells 
                                             - MUST BE 64bit ALIGNED!
                                           */
} bsMPgT;

#define BMT_PG_TERM            ((uint64_t)-1)




/******************************************************************************
 * Storage Bitmap structures.                                                 *
 ******************************************************************************/
/* The storage bitmap is an array of bits describing storage available
 * on a given disk.  The bits are organized into pages.  Each page
 * has an LSN and a checksum (XOR?) field.
 *
 * Bit clear means that block is unassigned.
 *
 * Bitmap pages are contained in the BsBm bitfile.
 *
 */

#define SBM_LONGS_PG  \
     ((ADVFS_METADATA_PGSZ - 3*sizeof(uint32_t)  - sizeof(bfFsCookieT)) \
       / sizeof(uint32_t))
#define SBM_BITS_LONG    (sizeof(uint32_t) * 8 /* bits per byte */)
#define SBM_BITS_PG      (SBM_LONGS_PG * SBM_BITS_LONG)

/*
* In order to extend the BMT the new BMT pages, SBM pages and
* transaction objects corresponding to that operation will be logged.
* If the bmtXtntFobs value is so large that logging this operation will
* overflow the log we have a problem ("Log Half Full Panic").
* We need to monitor this parameter and make the appropriate run time
* decision reagarding its value.  A reasonable relationship between
* the value of bmtXtntFobs and the size of the log has been determined
* as follows:
*
* The number of log pages required to log the SBM pages for a given
* bmtXtntFobs value is approximatley:
*
*                 bmtXtntFobs / SBM_FOBS_PER_PG
*
* This means that when (bmtXtntFobs / SBM_FOBS_PER_PG) is greater then 1/2 the
* number of log pages, we are going to panic with "Log Half Full".
* Given this relationship and the additional overhead of transaction objects
* and other events being logged,  we will take a conservative approach and limit
* this relationship to 25%.
* The following macros serve to implement this strategy:
*/
/* # of AdvFs pages represented by one SBM Page (minus overhead) */
/* clsz = vd->stgCluster */
#define SBM_FOBS_PER_PG(clsz) (SBM_BITS_PG * clsz * ADVFS_FOBS_PER_DEV_BSIZE )


/* Calculate the default bmtXtntFobs value from the target log size.
 * The goal is to allow only as many fobs as would require 25% of the log
 * pages to pin for storage addition. We take the number of log pages times
 * the number of fobs each page of the SBM can represent, and divide by 4.*/
#define BMT_EXT_REQUEST_DEFAULT(lpgs, clsz) (((lpgs) * SBM_FOBS_PER_PG(clsz)) / 4)

/* TRUE if requested the xtntSz (number of fobs of storage to add) will NOT violate 
 * the above relationship */
#define BMT_EXT_REQUEST_OK(xtntSz, lpgs, clsz) (xtntSz <= BMT_EXT_REQUEST_DEFAULT(lpgs, clsz))
/* END OF DUPLICATE CODE */

/* NOTE: bfFsCookieT is based on bfDomainIdT (bsIdT structure) which is
 *       padded to ensure that the structure and any 64-bit fields are
 *       properly aligned. The fsCookie field in the bsStgBm structure
 *       must also be properly aligned or the compiler will add additional
 *       padding and cause the page to overflow the ADVFS_METADATA_PGSZ
 *       page size.
 */ 
typedef struct bsStgBm {
    uint32_t magicNumber;
    uint32_t pageNumber;           /* SBM page number */
    bfFsCookieT fsCookie;
    uint32_t xor;                  /* bit xor */
    uint32_t mapInt[SBM_LONGS_PG]; /* uint32_t array (can't do bit arrays) */
} bsStgBmT;

/******************************************************************************
 * Tag Directory structures.                                                  *
 ******************************************************************************/

typedef struct bsTMap {
    union {
        /*
         * First tagmap struct on page zero only.
         */
        struct {
            bs_meta_page_t freeList;      /* head of free list */
            bs_meta_page_t unInitPg;      /* first uninitialized page */
        } tm_s1;

        /*
         * Tagmap struct on free list.
         */
        struct {
            bfTagFlagsT flags;
            uint32_t seqNo;     /* must retain seqNo value */
            uint32_t nextMap;    /* next free tagmap struct within page */
            uint32_t rsvd;
        } tm_s2;

        /*
         * In use tagmap struct.
         */
        struct {
            bfTagFlagsT flags;
            uint32_t seqNo;
            bfMCIdT  bfMCId;    /* bitfile mcell id */
        } tm_s3;
    } tm_u;
} bsTMapT;

/* Defintions for convenient access to fields of the bsTMapT type. */
#define tmFreeListHead   tm_u.tm_s1.freeList
#define tmUninitPg       tm_u.tm_s1.unInitPg
#define tmNextMap        tm_u.tm_s2.nextMap
#define tmBfMCId         tm_u.tm_s3.bfMCId
#define tmSeqNo          tm_u.tm_s3.seqNo
#define tmFlags          tm_u.tm_s3.flags


/*
 * number of pages to expand the tag dir by when a new extent is needed
 */
#define BS_TD_XPND_PGS 8

/*
 * Structure defined for the convenience of pin_record.
 */
typedef struct bsTDirPgHdr {
    uint32_t magicNumber;           
    uint32_t rsvd1;                   /* placed here for alignment */
    bfFsCookieT fsCookie;
    bs_meta_page_t currPage;          /* page number of this page */
    bs_meta_page_t nextFreePage;      /* next page having free TMaps */
    uint32_t nextFreeMap;             /* index of next free TMap, 1 based */
    uint32_t numAllocTMaps;           /* count of allocated tmaps */
    bfSetIdT bfSetId;                 /* fileset id */
    uint64_t rsvd2;
    uint64_t rsvd3;                   /* additional padding for 8192 bsTDirPgT size */
} bsTDirPgHdrT;


#define BS_TD_TAGS_PG   \
      ((ADVFS_METADATA_PGSZ - (sizeof(bsTDirPgHdrT))) / sizeof(bsTMapT))


typedef struct bsTDirPg {
    struct bsTDirPgHdr tpPgHdr;
    struct bsTMap tMapA[BS_TD_TAGS_PG]; /* array of TMap elements */
} bsTDirPgT;

#define tpCurrPage       tpPgHdr.currPage
#define tpNextFreePage   tpPgHdr.nextFreePage
#define tpNextFreeMap    tpPgHdr.nextFreeMap
#define tpNumAllocTMaps  tpPgHdr.numAllocTMaps
#define tpMagicNumber    tpPgHdr.magicNumber
#define tpFsCookie       tpPgHdr.fsCookie
#define tpBfSetId        tpPgHdr.bfSetId

/*****************************************************************************
 * Logger On-Disk Structures.                                          
 *****************************************************************************/

/*
 * Because the LOG page consists of several disk sectors and the disk
 * sector is the amount that can be written atomically, we must insure
 * for each sector that the data written there is correct and not garbage
 * left over from a previous write. Each sector has an LSN written at
 * the beginning. Since this is written to the disk, to change the
 * sector size as perceived by this code would be an on disk change.
 * So we define the logger's idea of a sector size here and assert that
 * it is still the same as the rest of AdvFS's idea of block size in log_init.
 */

/*
 * logPgHdrT -
 *
 * Header found the beginning of each log page.
 */
typedef struct logPgHdr {
    lsnT        thisPageLSN;          /* LSN of first record in the page */
    uint32_t    magicNumber;
    bfFsCookieT fsCookie;             /* used as a page identifier */
    bfdBfMetaT  pgType;               /* used as a page identifier */
    uint16_t    pgSafe;               /* TRUE if page is in safe format */
    uint16_t    chkBit;
    int16_t     curLastRec;           /* word offset into pg.data */
    int16_t     prevLastRec;          /* word offset into pg.data */
    uint32_t    rsvd1;
    uint64_t    rsvd2;
    logRecAddrT firstLogRec;          /* first log rec addr */
} logPgHdrT;


/*
 * logPgTrlrT -
 *
 * Trailer found at the end of each log page.
 */

typedef struct logPgTrlr {
    lsnT lsnOverwriteVal[ADVFS_LOGBLKS_PER_PAGE - 1];
                            /* The values in each block */   
                            /* (except the first block) that were */
                            /* overwritten by the page's LSN in the routine */
                            /* lgr_make_pg_safe().  lgr_restore_pg() uses   */
                            /* this array to put back the overwritten */
                            /* values.  */
    bfdBfMetaT      pgType;  /* used as a page identifier */
    bfFsCookieT     fsCookie;   /* used as a page identifier */
} logPgTrlrT;

/*
 * Number of data words per log page
 */
#define DATA_WORDS_PG \
    howmany((ADVFS_METADATA_PGSZ - sizeof(logPgHdrT) - sizeof(logPgTrlrT)), \
             sizeof(uint32_t))
/*
 * logPgT -
 *
 * A log page consists of a header followed by log records.  The
 * records are accessed via the 'data' array of longwords.  The last
 * field is a page trailer.
 */
typedef struct logPg {
    logPgHdrT  hdr;                      /* Log page header */
    uint32_t    data[DATA_WORDS_PG];      /* Log records (must type cast) */
    logPgTrlrT trlr;                     /* Log page trailer */
} logPgT;




/*****************************************************************************
 * Prototypes for on-disk structure utility routines.                        *
 *****************************************************************************/

typedef enum {
    RAW_READ,
    RAW_WRITE
} raw_io_type_t;
/*
 * The main raw IO routine for disk initialization and raw access.
 */
statusT
advfs_raw_io ( struct vnode* dev_vnode,         /* Device vnode for io */
               bf_vd_blk_t vd_blk,              /* start disk blk for IO */
               bf_vd_blk_t blk_cnt,             /* Blk count of DEV_BSIZE
                                                 * blocks for IO. */
               raw_io_type_t raw_type,          /* {RAW_READ,RAW_WRITE} */
               void *data
             );


#endif    /* BS_ODS */
