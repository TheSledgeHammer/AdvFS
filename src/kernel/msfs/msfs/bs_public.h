/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1988, 1989, 1990, 1991                *
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
 * @(#)$RCSfile: bs_public.h,v $ $Revision: 1.1.213.2 $ (DEC) $Date: 2006/03/14 19:39:50 $
 */

#ifndef BS_PUBLIC
#define BS_PUBLIC

#include <sys/mount.h>
#include <sys/time.h>       /* tmp - using timeval for bs_uid */
#include <sys/types.h>
#include <sys/malloc.h>

#ifndef REUSABLE_TAGS
#define REUSABLE_TAGS
#endif

#ifdef KERNEL
#include <sys/user.h>
#endif /* KERNEL */

#include <msfs/advfs_modules.h>

#define NULL_STRUCT { 0 }

/*
 * Magic numbers for validation of in-memory structures.
 * These should all begin with 0xADF and should not use
 * the next-highest bit as that is reserved for MAGIC_DEALLOC,
 * which is set when a structure is deallocated.
 */
#define MAGIC_DEALLOC    0x00080000
#define ACCMAGIC         0xADF00001    /* bfAccessT */
#define SETMAGIC         0xADF00002    /* bfSetT */
#define DMNMAGIC         0xADF00003    /* domainT */
#define VDMAGIC          0xADF00004    /* vdT */
#define FSMAGIC          0xADF00005    /* fileSetNodeT */
#define BUFMAGIC         0xADF00006    /* bsBuf */

/**********************************************************************
 * The following set of typedefs are the base set of types for use in
 * "on disk" structure definitions, where the size of the field is
 * constant regardless of cpu architecture.
 **********************************************************************/

typedef unsigned int uint32T;           /* unsigned 32 bit int */
typedef unsigned short uint16T;         /* unsigned 16 bit int */
typedef short int16T;                   /* signed 16 bit int */
typedef int int32T;                     /* signed 32 bit int */
typedef unsigned int bsPageT;           /* bitfile page number, 32 bits */
typedef unsigned int lbnT;              /* logical block number, 32 bits */
typedef struct timeval bsIdT;           /* unique identifier, 64 bits */
                                        /* tmp - should be real uuid */

/***************** end of base "on disk" typedefs ********************/

typedef uint16T vdIndexT;
typedef int statusT;        /* tmp */

#define BS_DOMAIN_NAME_SZ     (NAME_MAX + 1)
#define BS_SET_NAME_SZ        32
#define BS_VD_NAME_SZ         MAXPATHLEN
#define BS_FS_CONTEXT_SZ       8
#define BS_BLKSIZE           512
/*
** Relationship between blocks, clusters and pages.
**
** All AdvFS bitfile pages are 8192 bytes (ADVFS_PGSZ).
** This must be a multiple of the UBC page size PAGE_SIZE.
** There is some code (not enough) to allow ADVFS_PGSZ to be larger than
** PAGE_SIZE. There is no code to allow PAGE_SIZE to be larger than ADVFS_PGSZ.
** We use PAGE_SIZE to interact with the UBC.
** We do not use NBPG which seems to be used for platform specific and
** VM routines.
** We read and write whole AdvFS bitfile pages therefore our buffer size is
** ADVFS_PGSZ.
**
** The disk is divided into blocks. These were historicaly sectors, the amount
** that can be written atomically. We define the block size (BS_BLKSIZE) to be
** 512 independently from the other modules in the kernel.
** We use blocks for 3 purposes:
**
** 1) We describe file location on the disk in terms of blocks.
** These block locations are ultimately passed to a strategy routine.
** cdisk_strategy assumes that b_blkno is in terms of DEV_BSIZE byte blocks.
** DEV_BSIZE is defined in param.h and is platform dependent.
** If DEV_BSIZE was changed AdvFS would not communicate correctly with
** the CAM driver. Most (not all) strategy routines use DEV_BSIZE block size.
** We probably should have put the block size BSR_VD_ATTR record. We still
** could using "jays_new_field".
**
** 2) We recognize that we can't write an 8K LOG page atomiclly. We can write
** a sector atomically. We put special data at the start of each block in a LOG
** page to assist us in determining how much of a LOG page was written if the
** page was not completely written. In this use, our concept of a block must
** remain less than or equal to the disk sector size for our LOG to work.
** Since the LOG on disk has special data every 512 bytes, to change
** this use of block size would be an on disk change with all that implies.
** (See ms_logger.c for comments on LOG_BLKSZ)
**
** 3) Directories historically divide a page up into blocks. None of the
** directory's variable length record cross a block boundary. Most of the
** kernel routines for processing directories (in any filesystem) use
** DIRBLKSIZ which is defined as DEV_BSIZE. Since directories are on disk
** structures, if DEV_BSIZE changed, the various directory processing
** routines could fail. As long as we use a directory block size greater than
** or equal to the largest on disk directory block size, our directory
** processing routines will work.
**
** AdvFS keeps track of (and allocates) the storage on a disk in clusters.
** a cluster is an integral number of blocks. The original design allowed
** for different files types (BMT, SBM, regular files, etc) to have different
** page sizes in multiples of the cluster size which was 1K. In fact all
** AdvFS bitfile pages have always been 8K. In version 4 domains, the cluster
** size is 8K and all AdvFS bitfile pages are 8K.
**
** The cluster size is 2 in all version 3 domains.  This is true regardless 
** of which version of DUNIX the domain was created under.
*/
#define ADVFS_PGSZ             8192
#define ADVFS_PGSZ_IN_BLKS     (ADVFS_PGSZ / BS_BLKSIZE)
#define BS_CLUSTSIZE           ADVFS_PGSZ_IN_BLKS
#define BS_CLUSTSIZE_V3        2
#define BSR_MAX 23 /* increment with every new bmt record addition */

/* In the rare situation of a cluster in mid rolling-upgrade, the potential
 * for two nodes to have different definitions of mlDmnParamsT existed.
 * The structure mlBmtStatT, which is a bmtStatT had the potential to
 * increase in size with the addition of new bmt record types where it's
 * member fields include two arrays  that where defined in term of BSR_MAX,
 * but are now defined in terms of BSR_API_MAX.  BSR_API_MAX cannot change
 * from 21 without user apps breaking due to a mismatch in the API structure
 * size. This change now means that the "advfsstat -B r" and "advfsstat -B w"
 * utility will no longer report stats on bmt record types beyond record 21.
 * If reporting of records beyond number 21 is desired, a new syscall will
 * need to be written.
 */
#define BSR_API_MAX 21 /* !!!! do not change !!!!   (see above comment) */


#ifndef ADVFS_DEBUG
#define BS_MAX_VDI           256
#else
#define BS_MAX_VDI            50
#endif


/* Test the validity of the range of a volume index.  Return 
 * E_RANGE if the vdi is out of range.  This is a test of the
 * validity of the parameters passed in, and should never fail.
 */
#define TEST_VDI_RANGE(vdi) \
    ( ((vdIndexT)(vdi)) == 0 || ((vdIndexT)(vdi)) > BS_MAX_VDI ? E_RANGE : EOK )


/* Test the domainT.vdpTbl[] slot indexed by vdi; return EOK if it
 * is non-NULL (presumably a pointer to a vdT), else return EBAD_VDI.
 * dmnp->vdpTblLock must be held.
 */
#define TEST_VDI_SLOT(vdi, dmnp) \
    ( (dmnp)->vdpTbl[(vdi) - 1] == NULL ?  EBAD_VDI : EOK ) 

/* TEST if the vdi is valid; does not validate the vdp struct.
 * Currently used only by vd_htop_if_valid().
 */
#define VDI_IS_VALID(vdi, dmnp) \
    ( TEST_VDI_RANGE(vdi) == EOK  && \
      TEST_VDI_SLOT(vdi, dmnp) == EOK ) ? TRUE : FALSE

/* prototypes for using VD_HTOP macro and other vdi functions. */
struct domain;
struct vd *vd_htop_if_valid( vdIndexT vdi, 
                             struct domain *dmnp, 
                             int bump_refcnt,
                             int zombie_ok );
struct vd *vd_htop_already_valid( vdIndexT vdi, 
                                  struct domain *dmnp, 
                                  int bump_refcnt );
void vd_dec_refcnt( struct vd *vdp );

/* This definition allows us to use a lot of the existing VD_HTOP()
 * macros.  If the vdi to vdp conversion fails, this routine will
 * panic so long as ADVFS ASSERTs are enabled.
 */
#define VD_HTOP(vdi, dmnp)                       \
    vd_htop_already_valid( (vdi), (dmnp), FALSE )

typedef long bfPageRefHT;

extern bfPageRefHT NilBfPageRefH;

/*
 * bitfile-set descriptor pointer.  This is defined here as well as in
 * bs_public.h to keep the compiler happy.
 */
typedef struct bfSet bfSetT;

#define BFSET_VALID( _bfSetp )                                          \
(                                                                       \
    (_bfSetp) != NULL && (_bfSetp)->bfSetMagic == SETMAGIC              \
)

/*
 * This MACRO is used to provide input for hash key calculations involving
 * the bitfile-set descriptor (eg.  bsBuf and bfAccess hash tables).  The
 * address of the bitfile-set descriptor is as unique as anything, so the
 * MACRO returns bits 10-31 of the address.  Because kernel memory is
 * malloc'ed in standard sized buckets it turns out that the bfSetT
 * structure is always 1K aligned, therefore bits 0-9 are always the same.
 * This is why they are shifted out before masking the 22 bits desired.
 */
#define BFSET_GET_HASH_INPUT( _bfSetp ) \
    (((unsigned long)(_bfSetp) >> 10) & 0x3fffff)

typedef enum {
    BFS_ODS_UNKNONWN,
    BFS_ODS_VALID,
    BFS_ODS_INVALID,
    BFS_ODS_DELETED
} bfSetStateT;

typedef struct fragGrp {
    uint32T firstFreeGrp;
    uint32T lastFreeGrp;
} fragGrpT;

typedef enum {
        BS_ALLOC_DEFAULT,      /* use default algorithm */
        BS_ALLOC_FIT,          /* try to find a fit */
        BS_ALLOC_NFS,           /* use NFS-friendly algorithm */
        BS_ALLOC_RSVD,          /* reservation for reserved files */
        BS_ALLOC_MIG_RSVD,   /* override previous migrate reservation */
        BS_ALLOC_MIG_SINGLEXTNT,  /* attempt to get a single xtnt for blk request */
        BS_ALLOC_MIG_SAMEVD  /* alloc space on same vd as current primary mcell */
} bsAllocHintT;

#define FTX_MAXHNDL 1<<16

typedef struct ftxH {
    struct domain *dmnP;  /* domain pointer */
    signed hndl : 16;   /* handle into active ftx table */
    unsigned level : 8; /* ftx level (root = 0) */
} ftxHT;

#ifdef __arch32__
typedef struct {
    uint32T low;
    uint32T high;
} uint64T;

#define UINT64T_ADD(uint64t_field, addend) \
  if (((uint64t_field).low + (addend)) < (uint64t_field).low) { \
      (uint64t_field).high++; \
  } \
  (uint64t_field).low = (uint64t_field).low + (addend);

#define UINT64T_SUB(uint64t_field, subtrahend) \
  if (((uint64t_field).low - (subtrahend)) > (uint64t_field).low) { \
      (uint64t_field).high--; \
  } \
  (uint64t_field).low = (uint64t_field).low - (subtrahend);

#else /* __arch32__ */

typedef unsigned long uint64T;

#define UINT64T_ADD(uint64t_field, addend) \
  (uint64t_field) = (uint64t_field) + (addend);

#define UINT64T_SUB(uint64t_field, subtrahend) \
  (uint64t_field) = (uint64t_field) - (subtrahend);
#endif /* __arch32__ */

#define BS_UID_EQL(bs_uid1, bs_uid2) \
    ((bs_uid1).tv_sec == (bs_uid2).tv_sec && \
     (bs_uid1).tv_usec == (bs_uid2).tv_usec)
/*
 ** Bitfile Tag -- Unique bitfile identifier.
 */

#define BFM_RSVD_CELLS_V3 6 /* must agree with value in bs_ods.h */
#define BFM_RSVD_CELLS 7 /* must agree with value in bs_ods.h */
#define BFM_RSVD_TAGS  6 /* must agree with value in bs_ods.h */

typedef struct {
    uint32T num;        /* tag number, 1 based */
    uint32T seq;        /* sequence number */
} bfTagT;

extern bfTagT NilBfTag;

#define MAX_ADVFS_TAGS 0x7fffffff /* maximum number of non-reserved tags */

/* Macros for tags. */

#define BS_BFTAG_EQL(tag1, tag2) \
    (((tag1).num == (tag2).num) && ((tag1).seq == (tag2).seq))
#define BS_BFTAG_IDX(tag) ((tag).num)  /* used for hashing on tags */
#define BS_BFTAG_RSVD(tag) ((signed)((tag).num) < 0)
#define BS_BFTAG_REG(tag) ((signed)((tag).num) > 0)
#define BS_BFTAG_SEQ(tag) ((tag).seq)
#define BS_BFTAG_NULL(tag) ((tag.num) == 0)

/*
 * Bit in tag.seq indicating it is a pseudo-tag which exists only in-core.
 * Pseudo-tags are for shadow access structures of metadata files in the root bfSet.
 */
#define BS_PSEUDO_TAG   0x80000000
#define BS_BFTAG_PSEUDO(tag) ((tag).seq & BS_PSEUDO_TAG)

/* Macros for reserved tags. */
#define BS_BFTAG_VDI(tag) (-((signed)((tag).num)) / BFM_RSVD_TAGS)
#define BS_BFTAG_CELL(tag) (-((signed)((tag).num)) % BFM_RSVD_TAGS)
#define BS_BFTAG_RSVD_INIT(tag, vdi, bmtidx) \
    (tag).num = (uint32T) -(((vdi) * BFM_RSVD_TAGS) + (bmtidx)); \
    (tag).seq = 0
#define BS_BFTAG_RBMT(tag) (BS_BFTAG_CELL(tag) == BFM_RBMT)

/* Following macro returns TRUE if tag passed in represents a metadata file
 * of the type passed in metatype.  The second parm must be from the enumerated
 * list in "bfdBfMetaT".
 */
#define BS_IS_TAG_OF_TYPE( tag, metatype ) \
    ( ((((tag).seq)==0) || (BS_BFTAG_PSEUDO(tag)) ) &&  \
      (((-((int)((tag).num))) % BFM_RSVD_TAGS)==(metatype)) ? 1 : 0)

#define TAG_IS_V3BMT( tag, domain )  \
    ( !RBMT_THERE(domain) && BS_IS_TAG_OF_TYPE((tag), BFM_BMT_V3) )

#define BF_IS_V3BMT( bfap )  TAG_IS_V3BMT( (bfap)->tag, (bfap)->dmnP )

#define TAG_IS_V4BMT( tag, domain )  \
    ( RBMT_THERE(domain) && BS_IS_TAG_OF_TYPE((tag), BFM_BMT) )

#define BF_IS_V4BMT( bfap )  TAG_IS_V4BMT( (bfap)->tag, (bfap)->dmnP )

#define TAG_IS_BMT( tag, domain ) \
        ( TAG_IS_V3BMT((tag), (domain)) || TAG_IS_V4BMT((tag), (domain)) )

#define BF_IS_BMT( bfap )  ( BF_IS_V3BMT(bfap) || BF_IS_V4BMT(bfap) )

typedef bsIdT bfDomainIdT;
extern bfDomainIdT nilBfDomainId;

/*
 * Mcell related macros
 */
#define REC_TO_MREC(x) \
  ((bsMRT *)(((char *) (x)) - sizeof(bsMRT)))
#define MREC_TO_REC(x) \
  ((void *)(((char *) (x)) + sizeof(bsMRT)))
#define NEXT_MREC(x) \
  ((bsMRT *)(((char *) (x)) + roundup((x)->bCnt, sizeof(int))))
#define MCID_EQL(x1, x2) \
  (((x1).cell == (x2).cell) && ((x1).page == (x2).page))

/*
 * Returns TRUE if the file is an append (non-striped) file created
 * in a domain that supports extent information in the primary mcell
 * of non-reserved files.
 */
#define FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, mapType)    \
    ((dmnVersion >= FIRST_XTNT_IN_PRIM_MCELL_VERSION) && \
    ((mapType) == BSXMT_APPEND))

/*
 * bfMCIdT - bitfile mcell id.
 *  = BMT page number and cell number (address within a virtual disk)
 */

typedef struct bfMCId {
    uint32T cell : 5;          /* Cell number within page */
    uint32T page : 27;         /* Page number */
} bfMCIdT;

/*
 * bfSetIdT - bitfile set id
 *  = domain id + bitfile directory tag
 */
typedef struct {
    bfDomainIdT domainId;       /* BF-set's domain's ID */
    bfTagT      dirTag;         /* tag of BF-set's tag directory */
} bfSetIdT;

/*
 * bfUTagT - global tag id
 *  = bitfile set id + bitfile tag
 */
typedef struct {
    bfSetIdT bfsid;
    bfTagT tag;
} bfUTagT;

/*
 * mcellUIdT - global mcell id
 *  = mcell id (address) + virtual disk + global tag id
 */
typedef struct {
    bfMCIdT mcell;
    vdIndexT vdIndex;
    bfUTagT ut;
} mcellUIdT;

extern bfSetIdT nilBfSetId;

#define BS_BFS_EQL( bfs_uid1, bfs_uid2 ) \
    ((BS_BFTAG_EQL(bfs_uid1.dirTag, bfs_uid2.dirTag)) && \
    (BS_UID_EQL( bfs_uid1.domainId, bfs_uid2.domainId )))

/*
 * Logical Sequence Number definition and manipulation macros.
 *
 * LSNs are used to uniquely identify log records.  LSNs can
 * wrap, the macros for comparing LSNs take this into account.
 * LSNs are incremented by two (LSN_INCR_AMT) because bit zero
 * is reserved for page validation (see ms_logger.c).
 */

typedef struct {
    uint32T num;
} lsnT;

/* Increment LSNs by 2, effective LSN is 31 bits */
#define LSN_INCR_AMT                    2

#define LSN_MIN                         LSN_INCR_AMT
#define LSN_MAX                         0xfffffffe
#define LSN_SIGNED( lsn )               ((lsn).num & 0x80000000)
#define LSN_EQL( lsn1, lsn2 )           ((lsn1).num == (lsn2).num)
#define LSN_EQ_NIL( lsn )               ((lsn).num == 0)
#define LSN_GT( lsn1, lsn2 )            lgr_seq_gt( lsn1, lsn2 )
#define LSN_GTE( lsn1, lsn2 )           !lgr_seq_lt( lsn1, lsn2 )
#define LSN_LT( lsn1, lsn2 )            lgr_seq_lt( lsn1, lsn2 )
#define LSN_LTE( lsn1, lsn2 )           !lgr_seq_gt( lsn1, lsn2 )
#define SEQ_GT( lsn1, lsn2 )            lgr_seq_gt( lsn1, lsn2 )
#define SEQ_GTE( lsn1, lsn2 )           !lgr_seq_lt( lsn1, lsn2 )
#define SEQ_LT( lsn1, lsn2 )            lgr_seq_lt( lsn1, lsn2 )
#define SEQ_LTE( lsn1, lsn2 )           !lgr_seq_gt( lsn1, lsn2 )

#define LSN_INCR( lsn ) {               \
    (lsn).num += LSN_INCR_AMT;          \
                                        \
    if ((lsn).num == 0) {               \
        /*                              \
         * We are wrapping the lsn so we need to skip (move past) lsn 'zero'. \
         */                             \
        (lsn).num += LSN_INCR_AMT;      \
    }                                   \
}

/*
 * Log Record Address structure
 *
 * A log record can be described by its log page number, the word
 * offset with in the page, and the records LSN.
 */

typedef struct logRecAddr {
    uint16T page;    /* Log page number of the record */
    uint16T offset;  /* Log page offset of the record */
    lsnT   lsn;      /* Logical sequence number of the record */
} logRecAddrT;

#define RECADDR_EQ( rec1, rec2 ) \
     (((rec1).page == (rec2).page) && \
      ((rec1).offset == (rec2).offset) && \
      (LSN_EQL( (rec1).lsn, (rec2).lsn )))

extern logRecAddrT logEndOfRecords;
extern logRecAddrT logNilRecord;

/*
 * frag stuff
 */
#ifndef _BF_FRAG_T_
#define _BF_FRAG_T_
#endif
typedef enum {
    BF_FRAG_ANY = 0,
    BF_FRAG_1K = 1,
    BF_FRAG_2K = 2,
    BF_FRAG_3K = 3,
    BF_FRAG_4K = 4,
    BF_FRAG_5K = 5,
    BF_FRAG_6K = 6,
    BF_FRAG_7K = 7,
    BF_FRAG_MAX = 8,

    BF_FRAG_ALL = 256
} bfFragT;

/*
 * bfFragIdT
 *
 * Uniquely identifies a frag.
 *
 * Notes:
 * - use FRAG2PG( fragId.frag ) to access the frag's page
 * - use FRAG2SLOT( fragId.frag ) access the frag's slot within its page
 * - use slotsPgT to access a frag's slots
 */

typedef struct bfFragId {
    uint32T frag;       /* internally this is simply the starting slot # */
    bfFragT type;       /* frag type */
} bfFragIdT;

extern bfFragIdT bsNilFragId;

/*
 * Service Class stuff
 */

typedef uint32T serviceClassT;

/*
 * service class constants
 */

extern serviceClassT nilServiceClass;
extern serviceClassT defServiceClass;

/*
 * Tracing stuff
 */
typedef enum {
    trNone = 0,
    trAccess = 1, trClose = 2, trCreate = 4,
    trMutex = 0x10, trCond = 0x20, trLock = 0x40,
    trFtx = 0x100, trFtxPP = 0x200, trMem = 0x400,
    trRef = 0x1000, trPin = 0x2000,
    trDevRd = 0x4000, trDevWr = 0x8000
} trFlagsT;

/*
 * Agent profiling.
 */
typedef struct {
   struct timeval start_time;
   struct timeval cum_time;
   int total_calls;
} ftxProfT;

typedef uint32T bsLogSeqT;

#ifndef _VD_STATES_T_
#define _VD_STATES_T_
#endif
/* Note that BSR_VD_VIRGIN and BSR_VD_ZOMBIE are are used in the 
 * vdp->vdState to keep track of when a vdT is allocated, and possibly 
 * in the dmnp->vdpTbl[], but still not valid for general use.
 */
typedef enum {
    BSR_VD_VIRGIN,      /* vd is being set up but is not valid yet */
    BSR_VD_MOUNTED,     /* The vd is mounted or was left mounted */
    BSR_VD_DISMOUNTED,  /* The vd has been dismounted cleanly */
    BSR_VD_DMNT_INPROG, /* Dual mount in progress in this domain */
    BSR_VD_ZOMBIE       /* vd is being removed but is only mostly-dead. */
} bsVdStatesT;

/*
 * Disk stats - part of the disk struct
 */
typedef struct dStat {
    unsigned long nread;   /* number of reads */
    unsigned long nwrite;  /* number of writes */
    unsigned long readblk; /* number of 512 byte blocks read */
    unsigned long writeblk;
    unsigned long flushQ;   /* flushQ statistics */
    unsigned long ubcReqQ;  /* ubcReqQ statistics */
    unsigned long rglobBlk; /* number of consolidated I/O blks */
    unsigned long rglob;    /* number of consolidations */
    unsigned long unused;   /* AVAILABLE FOR USE */
    unsigned long wglobBlk; /* number of consolidated I/O blks */
    unsigned long wglob;    /* number of consolidations */
    unsigned long blockingQ;
    unsigned long waitLazyQ;
    unsigned long readyLazyQ;
    unsigned long consolQ;
    unsigned long devQ;
} dStatT;

typedef struct bsVdParams {
    bsIdT vdMntId;              /* last mount id */
    bsVdStatesT vdState;        /* virtual disk state */
    uint32T vdIndex;            /* vd index within file system */
    uint32T vdSize;             /* size in blocks */
    uint32T stgCluster;         /* number of blocks each bit represents */
    uint32T freeClusters;       /* total number of free clusters */
    uint32T totalClusters;      /* total number of clusters */
    uint32T maxPgSz;            /* largest possible page size on vd */
    uint32T bmtXtntPgs;         /* number of pages per BMT extent */
    serviceClassT serviceClass; /* service class provided */
    char vdName[BS_VD_NAME_SZ];
    struct dStat dStat;         /* device statistics */
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
} bsVdParamsT;

extern bsVdParamsT bsNilVdParams;

/*
*   bsDevtList - returned from advfs_enum_domain_devts()
*                (Wave 4 enabler)
*/
typedef struct bsDevtList {
    int     devtCnt;            /* number of dev_ts in array */
    dev_t   device[BS_MAX_VDI]; /* dev_t's for domain */
} bsDevtListT;

typedef enum {
    BFD_NIL,            /* no bitfile specific safety reqmt */
    BFD_NO_NWR,         /* don't maintain NWR */
    BFD_FTX_AGENT,      /* managed by an ftx agent */
    BFD_SYNC_WRITE,     /* all writes to file are synchronous */
    BFD_FTX_AGENT_TEMPORARY /* Temporary data logging (mount -o adl) */
} bfDataSafetyT;

typedef enum {
    BSXMT_APPEND,    /* Append only bitfile */
    BSXMT_SHADOW_UNSUPPORTED, /* Shadowed file; not supported currently */
    BSXMT_STRIPE     /* Striped bitfile */
} bsXtntMapTypeT;

typedef struct {
    bsPageT bfPage;             /* Bitfile page number */
    uint32T bfPageCnt;          /* Extent page count */
    uint32T volIndex;           /* Disk volume where blocks are stored */
    uint32T volBlk;             /* Disk volume block number */
} bsExtentDescT;

#define BS_CLIENT_AREA_SZ 4

/*
 * bfCParamsT
 *
 * Client changeable params.
 */

typedef struct bfCParams {
    bfDataSafetyT dataSafety;   /* bitfile data safety requirements */
    serviceClassT reqServices;  /* required service class */
    serviceClassT optServices;  /* optional service class */
    int32T extendSize;          /* add'l extend size in blocks */
    int32T clientArea[BS_CLIENT_AREA_SZ]; /* user/client-specific area */
    int32T rsvd1;
    int32T rsvd2;
} bfCParamsT;

/*
 * bfIParamsT
 *
 * Inheritable params.
 */

typedef bfCParamsT bfIParamsT;

/*
 * bfParams
 *
 * Bitfile params.
 */

typedef struct bfParams {
    /* the following are NOT settable by clients/users */
    bfTagT tag;                 /* bitfile tag */
    uint32T pageSize;           /* page size in 512 byte blocks */
    uint32T numPages;           /* number of pages allocated */
    bsPageT nextPage;           /* next page to allocate */
    uint32T vdIndex;            /* primary mcell vd */
    bsXtntMapTypeT type;        /* Type of extent map */
    uint32T cloneId;            /* 0 ==> orig; "> 0" ==> clone */
    uint32T cloneCnt;           /* set's clone cnt last time bf changed */
    bfFragIdT fragId;           /* frag id */
    uint32T fragPageOffset;     /* Page where frag is */
    uint32T segmentCnt;         /* Number of segments per stripe. */
    uint32T segmentSize;        /* The minnumber of pages that are allocated */
                                /* to a stripe segment. */

    /* the following are settable by clients/users */
    bfCParamsT cl;              /* client changeable params */
} bfParamsT;

extern bfParamsT bsNilBsParams; /* old name */
extern bfParamsT bsNilBfParams;

/*
 * Buffer cache statistics - part of the domain struct
 */

typedef struct bcStat {
    uint32T pinHit;
    uint32T pinHitWait;
    uint32T pinRead;
    uint32T refHit;
    uint32T refHitWait;
    uint32T raBuf;     /* read ahead buffers queued */
    uint32T ubcHit;

    struct {
        uint32T lazy;
        uint32T blocking;
        uint32T clean;
        uint32T log;
    } unpinCnt;

    uint32T derefCnt;
    uint32T devRead;
    uint32T devWrite;
    uint32T unconsolidate;
    uint32T consolAbort;

    struct {
        unsigned long meta;
        unsigned long ftx;
    } unpinFileType;

    struct {
        unsigned long meta;
        unsigned long ftx;
    } derefFileType;
} bcStatT;

typedef struct bmtStat {
    unsigned long fStatRead;
    unsigned long fStatWrite;
    unsigned long resv1;
    unsigned long resv2;
    unsigned long bmtRecRead[BSR_API_MAX+1];
    unsigned long bmtRecWrite[BSR_API_MAX+1];
} bmtStatT;

typedef struct logStat {
    ulong_t logWrites;
    ulong_t transactions;
    ulong_t segmentedRecs;
    ulong_t logTrims;
    ulong_t wastedWords;
    uint_t  maxLogPgs;
    uint_t  minLogPgs;
    uint_t  maxFtxWords;
    uint_t  maxFtxAgent;
    uint_t  maxFtxTblSlots;
    uint_t  oldFtxTblAgent;
    ulong_t excSlotWaits;
    ulong_t fullSlotWaits;
    ulong_t rsv1;
    ulong_t rsv2;
    ulong_t rsv3;
    ulong_t rsv4;
} logStatT;

typedef struct bfDmnParams {
    bfDomainIdT bfDomainId;     /* domain id */
    uint32T maxVds;             /* maximum vd index */
    bfTagT bfSetDirTag;         /* tag of tag directory */
    bfTagT ftxLogTag;           /* tag of domain log */
    uint32T ftxLogPgs;          /* number of pages in log */
    uint32T curNumVds;          /* current number of VDs */
    uid_t uid;                  /* domain's owner */
    gid_t gid;                  /* domain's group */
    mode_t mode;                /* domain's access permissions */
    char domainName[BS_DOMAIN_NAME_SZ];
    struct bcStat bcStat;       /* per domain buffer cache stats */
    struct bmtStat bmtStat;     /* per domain BMT stats */
    struct logStat logStat;     /* per domain LOG stats */
    int dmnVersion;             /* domain's on-disk version number */
} bfDmnParamsT;

extern bfDmnParamsT bsNilDmnParams;

typedef struct {
    bfSetIdT bfSetId;
    bfTagT  nextCloneSetTag;             /* tag of next set in clone list */
    bfTagT  origSetTag;                  /* for clones, this is parent set */
    uint32T cloneId;                     /* 0 ==> orig; "> 0" ==> clone */
    uint32T cloneCnt;                    /* times orig has been cloned */
    uint32T numClones;                   /* current number of clones */
    uint32T quotaBlks;                   /* maximum size of fileset */
    uint32T quotaFiles;                  /* maximum bitfiles in fileset */
    uint32T quotaStatus;                 /* same as in fileSetNode struct */
    uid_t uid;                           /* set's owner */
    gid_t gid;                           /* set's group */
    mode_t mode;                         /* set's permissions mode */
    uint32T numBitFiles;                 /* number of bitfiles */
    char setName[BS_SET_NAME_SZ];        /* bitfile set's name */
    uint32T fsContext[BS_FS_CONTEXT_SZ]; /* fs's context */
} bfSetParamsV1T;

typedef struct {
    long blkHLimit;                 /* maximum quota blocks in fileset */
    long blkSLimit;                 /* soft limit for fileset blks */
    long fileHLimit;                /* maximum number of files in fileset */
    long fileSLimit;                /* soft limit for fileset files */
    long blksUsed;                  /* number of quota blocks used */
    long filesUsed;                 /* number of bitfiles used */
    long dmnTotalBlks;              /* number of blocks in set's domain */
    long dmnAvailBlks;              /* num of available blks in set's dmn */
    long dmnAvailFiles;             /* num of available files in set's dmn */
    time_t blkTLimit;               /* time limit for excessive disk blk use */
    time_t fileTLimit;              /* time limit for excessive file use */

    uint32T unused1;
    uint32T unused2;
    uint32T unused3;
    uint32T bfSetFlags;             /* flags for the fileset */
                                    /* uint32T is used as the datatype */
                                    /* to be compatible with bfSetFlags */
                                    /* field in bfSetT */

    uint32T quotaStatus;            /* quota status */

    bfSetIdT bfSetId;
    bfTagT  nextCloneSetTag;             /* tag of next set in clone list */
    bfTagT  origSetTag;                  /* for clones, this is parent set */
    uint32T cloneId;                     /* 0 ==> orig; "> 0" ==> clone */
    uint32T cloneCnt;                    /* times orig has been cloned */
    uint32T numClones;                   /* current number of clones */
    uid_t uid;                           /* set's owner */
    gid_t gid;                           /* set's group */
    mode_t mode;                         /* set's permissions mode */
    char setName[BS_SET_NAME_SZ];        /* bitfile set's name */
    uint32T fsContext[BS_FS_CONTEXT_SZ]; /* fs's context */
    fragGrpT fragGrps[BF_FRAG_MAX];      /* frag list heads */
} bfSetParamsT;

extern bfSetParamsT bsNilBfSetParams;

/*
 * typedefs for bs_inherit()
 */

typedef enum {
    BF_INHERIT_I_TO_I,  /* copy parent's inheritable to childs inheritable */
    BF_INHERIT_I_TO_A,  /* copy parent's inheritable to childs attributes */
    BF_INHERIT_I_TO_IA  /* copy parent's inheritable to childs both */
} bfInheritT;

/*
 * PROTOTYPES
 */

/*****************************/
/****   Bitfile services  ****/
/*****************************/

#include <msfs/bs_params.h>

struct bfAccess;

statusT
bs_inherit(
    struct bfAccess *parentbfap,
    struct bfAccess *childbfap,
    bfInheritT inherit,
    ftxHT ftxH
    );

statusT
bs_domain_access(
                 struct domain **dmnPP,
                 bfDomainIdT bfDomainId, /* in */
                 int deactivated_ok      /* in */
                 );

void
bs_domain_close( struct domain *dmnP);

statusT
bs_access(
          struct bfAccess **outbfap, /* out - access structure pointer */
          bfTagT tag,              /* in - tag of bf to access */
          bfSetT *bfSetp,          /* in - BF-set descriptor pointer */
          ftxHT ftxH,              /* in - ftx handle */
          uint32T options,         /* in - options flags */
          struct mount *mp,        /* in - fs mount pt */
          struct vnode **vp        /* out - vnode pointer */
          );

statusT
bs_close(
         struct bfAccess *bfAccessp, /* in */
         int options                /* in */
         );

statusT
bs_delete(
          struct bfAccess *bfap /* in */
          );

statusT
bs_migrate (
            struct bfAccess *bfap,  /* in */
            vdIndexT srcVdIndex,    /* in */
            bsPageT srcPageOffset,  /* in */
            uint32T srcPageCnt,     /* in */
            vdIndexT dstVdIndex,    /* in */
            uint64T dstBlkOffset,   /* in */
            uint32T forceFlag,      /* in */
            bsAllocHintT alloc_hint /* in */
            );

statusT
bs_stripe (
           struct bfAccess *bfap, /* in */
           uint32T segmentCnt,    /* in */
           uint32T segmentSize,   /* in */
           long xid               /* in */
           );

statusT
bs_add_stg(
           struct bfAccess *bfap,   /* in */
           unsigned long bsPage,    /* in */
           unsigned long bsPageCnt  /* in */
           );

statusT
bs_add_overlapping_stg(
                       struct bfAccess *bfap,   /* in */
                       unsigned long bsPage,    /* in */
                       unsigned long bsPageCnt, /* in */
                       uint32T *allocPageCnt    /* out */
                       );

struct extentmap;
int
advfs_get_extent_map(
                     struct bfAccess *bfap,     /* in */
                     struct extentmap *map      /* in/out */
                     );

typedef enum {
    BS_NIL,             /* default behavior */
    BS_SEQ_AHEAD,       /* sequential access probable */
    BS_OVERWRITE,       /* page will be overwritten */
    BS_BLOCK_UNPINS     /* block unpins of this page */
                        /* currently used by migrate/shadow copy */
} bfPageRefHintT;

statusT
bs_refpg(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in */
         bfPageRefHintT refHint         /* in */
         );

/* Special case flags for bs_pinpg_int buffer cache routine. */
#define PINPG_NOFLAG         0x0  /* Default. no special case pinpg handling.*/
#define PINPG_SET_FLUSH      0x1  /* Wait for and set FLUSH flag in pinpg. 
                                   * For migrate copy routines only.
                                   */
#define PINPG_DIRECTIO       0x2  /* Request came from direct I/O, don't
                                   * allocate a buffer if there isn't one.
                                   */
#define PINPG_DIO_REMOVE     0x10 /* If found, caller will remove this 
                                   * page from the Advfs (and ubc) cache.
                                   * (Set with PINPG_DIRECTIO only).
                                   */

statusT
bs_pinpg(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in */
         bfPageRefHintT refHint         /* in */
         );

statusT
bs_pinpg_int(
         bfPageRefHT *bfPageRefH,       /* out */
         void **bfPageAddr,             /* out */
         struct bfAccess *bfap,         /* in */
         unsigned long bsPage,          /* in */
         bfPageRefHintT refHint,        /* in */
         int flag                       /* in */

         );

typedef enum {
    BS_LOG_NOMOD,       /* log page wasn't modified */
    BS_CACHE_IT,        /* normal cache value to page */
    BS_RECYCLE_IT       /* unlikely to revisit soon */
} bfPageCacheHintT;

statusT
bs_derefpg(
           bfPageRefHT bfPageRefH,      /* in */
           bfPageCacheHintT cacheHint   /* in */
           );

typedef enum {
    BS_NOMOD,           /* page not modified */
    BS_LOG_PAGE,        /* one of a synchronous group */
    BS_MOD_LAZY,        /* modified, deferred write is okay */
    BS_MOD_SYNC,        /* modified, write synchronously */
    BS_MOD_COPY,        /* unpin from the migrate code */
    BS_MOD_DIRECT,      /* modified, directIO cache flush */
    BS_NOMOD_DIRECT     /* not modified, directIO cache flush */
} bfPgRlsModeT;

typedef struct {
    bfPageCacheHintT cacheHint;    /* cache hint */
    bfPgRlsModeT rlsMode;          /* page release mode */
} bsUnpinModeT;

extern bsUnpinModeT BS_DIRTY;
extern bsUnpinModeT BS_WRITETHRU;
extern bsUnpinModeT BS_CLEAN;
extern bsUnpinModeT BS_LOG;

statusT
bs_unpinpg(
           bfPageRefHT bfPageRefH,      /* in */
           logRecAddrT wrtAhdLogAddr,   /* in */
           bsUnpinModeT modeNcache      /* in */
           );

/*
 * Constants for bs_raw
 */
typedef enum {
    RREAD,      /* read from disk */
    RWRITE      /* write to disk */
} rawModeT;

/******************************************************/
/****  Virtual disk and Bitfile Domain management  ****/
/******************************************************/

statusT
bs_disk_init(
           char *diskName,             /* in - disk device name */
           bfDmnParamsT* bfDmnParams,  /* in - domain parameters */
           bsVdParamsT *bsVdParams,    /* in/out - vd parameters */
           serviceClassT tagSvc,       /* in - tag service class */
           serviceClassT logSvc,       /* in - log service class */
           uint32T bmtPreallocPgs,     /* in - num pgs to preallocate for BMT */
           int dmnVersion              /* in - on-disk version number */
           );

statusT
bs_vd_remove_active (
                     struct domain *dmnP,
                     vdIndexT vdIndex, /* in */
                     uint32T forceFlag /* in */
                     );

statusT
bs_vd_add_active(
    char *domain,               /* in - path to domain table */
    char *vdName,               /* in - block special device name */
    serviceClassT *vdSvc,       /* in/out - service class */
    uint32T vdSize,             /* in - size of the virtual disk */
    uint32T bmtXtntPgs,         /* in - number of pages per BMT extent */
    uint32T bmtPreallocPgs,     /* in - number pages to preallocate for BMT */
    uint32T flag,               /* in - M_MSFS_MOUNT may be set */
    bfDomainIdT *dmnId,         /* out - domain Id */
    uint32T *volIndex           /* out - volume index */
    );

statusT
bs_dmn_init(
    char *domain,               /* in - domain name */
    int maxVds,                 /* in - maximum number of virtual disks */
    uint32T logPgs,             /* in - number of pages in log */
    serviceClassT logSvc,       /* in - log service attributes */
    serviceClassT tagSvc,       /* in - tag directory service attributes */
    char *vdName,               /* in - block special device name */
    serviceClassT vdSvc,        /* in - service class */
    uint32T vdSize,             /* in - size of the virtual disk */
    uint32T bmtXtntPgs,         /* in - number of pages per BMT extent */
    uint32T bmtPreallocPgs,     /* in - number pages to preallocate for BMT */
    uint32T domainVersion,      /* in - on-disk version for domain */
    bfDomainIdT *domainId       /* out - unique domain id */
    );

statusT
bs_bfdmn_id_activate(
    bfDomainIdT bfDomainId /* in - domain id */
    );

statusT
bs_bfdmn_tbl_activate(
                      char* bfDmnName,      /* in - bf domain name */
                      u_long flag,          /* in - flag */
                      bfDomainIdT* domainId /* out - domain id */
                      );

typedef enum {
    ANORM,  /* normal whatever */
    ATEST   /* abnormal whatever */
} bfDmnModeT;

statusT
bs_bfdmn_activate(
                  bfDomainIdT domainId,
                  u_long flag
                  );

statusT
bs_bfdmn_deactivate(
                    bfDomainIdT domainId,
                    u_long flag
                    );

statusT
bfflush(
        struct bfAccess *bfap, /* In - access structure */
        bsPageT first_page,    /* In - first page to flush */
        bsPageT pages_to_flush,/* In - number of pages to flush */
        int priority           /* In - priority of the flush */
        );

void
bfflush_start(
              struct bfAccess *bfap, /* in - ptr to access struct */
              lsnT *seq,       /* in */
              int migrate,     /* in */
              unsigned long flushPgCnt /* Set 0 or # of pages to flush */
              );

/* The following can be passed as priority values to bfflush(). */
#define FLUSH_IMMEDIATE          0x01
#define FLUSH_INTERMEDIATE       0x02
#define FLUSH_UBC                0x04
#define FLUSH_PREALLOCATED_PAGES 0x10
        
/* SC_SUBSET( sc1, sc2 ) - returns true if sc1 is a proper subset of sc2 */
#define SC_SUBSET( sc1, sc2 ) (((sc1) & (sc2)) == (sc1))

/* SC_EQL( sc1, sc2 ) - returns true if sc1 and sc2 are equal */
#define SC_EQL( sc1, sc2 ) ((sc1) == (sc2))

/* SC_LT( sc1, sc2 ) - returns true if sc1 is less than sc2 */
#define SC_LT( sc1, sc2 ) ((sc1) < (sc2))

/* SC_GT( sc1, sc2 ) - returns true if sc1 is greater than sc2 */
#define SC_GT( sc1, sc2 ) ((sc1) > (sc2))

char *
_ms_malloc(
           unsigned size,       /* in */
           int ln,              /* in */
           char *fn,            /* in */
           int flag,            /* in */
           int rad_id           /* in */
           );

void
_ms_free(
         void *ptr,
         int ln,              /* in */
         char *fn             /* in */

         );

/* 
 * Allocate some memory from any RAD.  If memory is not immediately
 * available, wait until it is.
 */
#define ms_malloc(size)         _ms_malloc(size,__LINE__,__FILE__,M_WAITOK,0)

/* 
 * Allocate some memory from any RAD but do not wait if no
 * memory is immediately available.
 */
#define ms_malloc_no_wait(size) _ms_malloc(size,__LINE__,__FILE__,M_NOWAIT,0)

/* 
 * Try to allocate some memory from the specified RAD.  If memory is
 * immediately available on that RAD, return it.  Otherwise:
 * 
 * If flag is M_INSIST and no memory
 * is immediately available on the specified RAD, wait until it is.  
 * 
 * If flag is M_PREFER and no memory is immediately available on the specified 
 * RAD, look for memory on other RADs.  If no memory is immediately 
 * available on any RAD, wait for memory to become available on any RAD.
 */
#define ms_rad_malloc(size, flag, rad_id) \
    _ms_malloc(size, __LINE__, __FILE__, flag | M_WAITOK, rad_id)

/* 
 * Try to allocate some memory from the specified RAD.  If memory is
 * immediately available on that RAD, return it.  Otherwise:
 *
 * If flag is M_INSIST and no memory is immediately available on the 
 * specified RAD, return NULL.  
 *
 * If flag is M_PREFER and no memory is immediately available on the 
 * specified RAD, look for memory on other RADs.  If no memory is 
 * immediately available on any RAD, return NULL.
 */
#define ms_rad_malloc_no_wait(size, flag, rad_id) \
    _ms_malloc(size, __LINE__, __FILE__, flag | M_NOWAIT, rad_id)

#define ms_free( ptr )              _ms_free( ptr, __LINE__, __FILE__ )

int
ms_copyin( void *src, void *dest, int len );

int
ms_copyout( void *src, void *dest, int len );

extern char *SadFmt0;
extern char *SadFmt1;
extern char *SadFmt2;
extern char *SadFmt3;

#define ADVFS_SAD( fmt, msg ) \
    advfs_sad( __FILE__, __LINE__, fmt, msg, 0, 0, 0 );
#define ADVFS_SAD0( msg ) \
    advfs_sad( __FILE__, __LINE__, SadFmt0, msg, 0, 0, 0 );
#define ADVFS_SAD1( msg, n1 ) \
    advfs_sad( __FILE__, __LINE__, SadFmt1, msg, (n1), 0, 0 );
#define ADVFS_SAD2( msg, n1, n2 ) \
    advfs_sad( __FILE__, __LINE__, SadFmt2, msg, (n1), (n2), 0 );
#define ADVFS_SAD3( msg, n1, n2, n3 ) \
    advfs_sad( __FILE__, __LINE__, SadFmt3, msg, (n1), (n2), (n3) );

void
advfs_sad(char * module,
          int line,
          char *fmt,
          char *msg,
          long n1,
          long n2,
          long n3);

#define DMN_PANIC_IO_CONNECTIVITY       0x01
#define DMN_PANIC_IO_OTHER              0x02
#define DMN_PANIC_GENERIC               0x04
#define DMN_PANIC_UNMOUNT               0x08

#define DMN_PANIC_MAX_MSG_LEN           128 /* Max length of domain panic message string */
 
void
_domain_panic(struct domain *dmnP,
              char *msg,
              int flags
             );
void
domain_panic( struct domain *dmnP,
              char *format,
              ...
            );

#define domain_panic_type(dmnP, msg, flags) \
    _domain_panic(dmnP, msg, flags)

void
ms_printf(
#ifndef KERNEL
          char *msg, ...
#endif
          );

void
ms_uprintf(
#ifndef KERNEL
          char *msg, ...
#endif
          );

void
ms_uaprintf(
#ifndef KERNEL
          char *msg, ...
#endif
          );

void
ms_pfflush();

/*
 * public bitfile set routines
 */

statusT
bs_bfs_find_set(
    char *setName,             /* in - name of set to find */
    struct domain *dmnP,
    u_long doingRoot,          /* in - flag */
    bfSetParamsT *SetParams    /* out - the bitfile-set's parameters */
    );

statusT
bs_bfs_get_info(
    uint32T *nextSetIdx,       /* in/out - index of set */
    bfSetParamsT *bfSetParams, /* out - the bitfile-set's parameters */
    struct domain *dmnP,
    uint32T *userId            /* out - bfset user id */
    );

void
bs_bfs_get_set_id(
    bfSetT *bfSetp,       /* in - bitfile-set descriptor pointer */
    bfSetIdT *bfSetId     /* out - bitfile set id */
    );

void
bs_bfs_get_clone_info(
    bfSetT *bfSetp,       /* in - bitfile-set descriptor pointer */
    uint32T *cloneId,     /* out - bitfile set clone id */
    uint32T *cloneCnt     /* out - bitfile set clone count */
    );

statusT
bs_bfset_activate(
    char *bfDmnTbl,     /* in - bitfile-set's domain table file name */
    char *bfSetName,    /* in - bitfile-set name */
    u_long flag,        /* flag - used for forced mount */
    bfSetIdT *bfSetId   /* out - bitfile-set's ID */
    );

statusT
bs_bfset_deactivate(
    bfSetIdT bfSetId,   /* in - bitfile-set's ID */
    u_long flag         /* in - advfs mount flags */
    );

/*
 * misc
 */

char*
toke_it(
        char* str,      /* in - string to scan */
        char  tokchar,  /* in - token end character */
        char* token     /* out - token found */
       );

/*
 * The following are to be used for the 'mode' parameter of bs_accessible().
 */

#define BS_ACC_READ     0400
#define BS_ACC_WRITE    0200
#define BS_ACC_EXEC     0100

int
bs_accessible(
    mode_t mode,            /* in - mode wanted */
    mode_t omode,           /* in - object's permissions mode */
    uid_t ouid,             /* in - object's uid */
    gid_t ogid              /* in - object's gid */
    );

int
bs_owner(
    uid_t ouid  /* in - object's user id */
    );

statusT
bs_get_dmntbl_params(
    char *dmnTbl,             /* in - domain table file name */
    bfDmnParamsT *dmnParams
    );

statusT
bs_dmn_change(
    char *dmnTbl,             /* in - domain table file name */
    int newUid,
    uid_t uid,
    int newGid,
    gid_t gid,
    int newMode,
    mode_t mode
    );

statusT
bs_get_current_tag(
    bfSetT *bfSetp,       /* in */
    bfTagT *bfTag         /* in/out */
    );

#endif /* BS_PUBLIC */
