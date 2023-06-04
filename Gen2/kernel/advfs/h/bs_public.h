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
 *  Bitfile public types, structure, defines, and function prototypes.
 *
 */

#ifndef BS_PUBLIC_INCLUDED
#define BS_PUBLIC_INCLUDED

/*
 * TODO: remove the following #ifndef/#endif for HPUXBOOT once
 *       the bootloader code has been updated.
 */
#ifndef HPUXBOOT
#include <sys/malloc.h>
#endif /* HPUXBOOT */

#include <sys/types.h>
#include <sys/time.h>           /* tmp - using timeval for bs_uid */
#include <sys/dirent.h>
#include <sys/param.h>
#include <advfs/advfs_types.h>
#include <sys/fs/advfs_public.h>


#define NULL_STRUCT { 0 }

/* We need this here otherwise we will call a function that
 * expects ints and potentially lose precision
 */

#define min(a,b) ( (a) < (b) ? (a) : (b) )
#define max(a,b) ( (a) < (b) ? (b) : (a) )

/****************************************************************/
/****  Structure version constant - change this whenever  *******/
/****  an on-disk structure is modified *************************/
/****                                                     *******/
/****************************************************************/
#define BFD_ODS_LAST_VERSION_MAJOR    5
#define BFD_ODS_LAST_VERSION_MINOR    0

/*
 * The following is an enumeration for use in the tag flags.  Be very
 * careful when maniuplating anything before 0x00000008 as there the
 * BS_DATA_SAFETY_MASK doesn't represent a bitfield but a value that maps to
 * the BFD_{USER,META}DATA flags.
 */
typedef enum {
    BS_TD_NO_FLAGS              = 0x00000000,
    BS_TD_IN_USE                = 0x00000001,
    BS_DATA_SAFETY_MASK         = 0x00000006,
    BS_TD_OUT_OF_SYNC_SNAP      = 0x00000008,
    BS_TD_VIRGIN_SNAP           = 0x00000010,
    BS_TD_ROOT_SNAPSHOT         = 0x00000020,
    BS_TD_DEL_WITH_CHILDREN     = 0x00000040,
    BS_TD_FRAGMENTED            = 0x00000080,
} bfTagFlagsT;


#define NAMEMAX             255
#define BS_DOMAIN_NAME_SZ   (NAMEMAX + 1) /*Must match CFSMSFS_DOMAIN_NAME_SZ */
#define BS_SET_NAME_SZ      32            /* Must match CSMSFS_BS_SET_NAME_SZ */
#define BS_VD_NAME_SZ       MAXPATHLEN    /* Must match CFSMSFS_VOL_NAME_SZ */ 
#define ADVFS_MIN_DISK_SIZE 8192          /* DEV_BSIZE sized blocks */  

/*
 * Relationships between disk blocks, clusters, file offset blocks and other
 * basic AdvFS units. 
 *
 * The disk is addressed by the drivers in units of DEV_BSIZE sized blocks.
 * When issuing IO to disk, we must be certain that we are using blocks that
 * are DEV_BSIZE large.  At initial port, DEV_BSIZE is 1k, however it may
 * change in the future.  
 *
 * To minimize the impact of DEV_BSIZE changing, AdvFS uses a file offset
 * block that is always 1k.  If the DEV_BSIZE changes, the macro
 * ADVFS_FOBS_PER_DEV_BSIZE must be changed to represent the number of 1k
 * fobs in a DEV_BSIZE block. (DEV_BSIZE / 1024).  When dealing with disk
 * blocks, this macro should be used to convert between fobs (file offset
 * blocks) and disk blocks (DEV_BSIZE blocks).  
 *
 * Cluster size is the number of DEV_BSIZE blocks that each bit in the SBM
 * represents.  The choice between making cluster size be represented in
 * disk blocks of fobs was made primarily because cluster size is a physical
 * measure of disk blocks.  It could have gone either way.  Each storage
 * cluster is 1k, or 1 DEV_BSIZE blocks (until DEV_BSIZE changes).
 * The cluster size was reduced from 4k to 1k to allow for more efficient 
 * storage for small files.
 *
 * The concept of a page only exists in AdvFS for metadata.  User data (from
 * the filesystem perspective) has no fixed structure, and thus no inherent
 * page size.  As a result, it can be defined by the user in terms of
 * allocation units.  Metadata, on the other hand, has fixed, repeating
 * patterns that represent pages.  Metadata is modified as units of
 * ADVFS_METADATA_PGSZ by bs_pinpg.  
 *
 */
#define ADVFS_BS_CLUSTSIZE                    1 
#define BSR_MAX         19  /* increment with every new bmt record addition */

#define ADVFS_FOB_SZ                    1024L
#define ADVFS_METADATA_PGSZ             8192L
#define ADVFS_METADATA_PGSZ_IN_FOBS     ( ADVFS_METADATA_PGSZ / ADVFS_FOB_SZ )
#define ADVFS_FOBS_PER_DEV_BSIZE        ( DEV_BSIZE / ADVFS_FOB_SZ )
#define ADVFS_SUPER_BLOCK_SZ            8192L

/* 
 * These are for index code and need to be removed to make indicies support
 * dynamic page size.   To calculate them dynamically requires log(.) or a
 * bit counting function, so they are just hard-coded for now.
 */
#define ADVFS_METADATA_PAGE_SHIFT       13
#define ADVFS_METADATA_PAGE_MASK        ADVFS_METADATA_PGSZ - 1


#define ADVFS_MAX_OFFSET                0x00003fffffffffffLL

/*
 * ADVFS_MAX_PGSZ_IN_FOBS represents the maximum supported allocation unit for 
 * user data in fobs.  For now, 8k allocation units are the max supported.
 */
#define ADVFS_MAX_ALLOC_SZ_IN_FOBS      8192L

/* These two macros are for convenience.  ADVFS_OFFSET_TO_FOB will return the
 * number of whole fobs required to encompass the given offset.  
 * ADVFS_OFFSET_TO_FOB_DOWN will truncation any extra bits, rounding down. */
#define ADVFS_OFFSET_TO_FOB_UP(offset)    ( ((offset) + ADVFS_FOB_SZ - 1) >> 10 )
#define ADVFS_OFFSET_TO_FOB_DOWN(offset)  ( (offset) >> 10 )
#define ADVFS_FOB_TO_OFFSET(fob)          ( (fob) << 10 )

/* The next 2 macros give the first/last FOB within the vd block
 * that encompasses the input file offset.  They will compensate
 * for changing sizes of DEV_BSIZE with respect to ADVFS_FOB_SZ
 * so long as DEV_BSIZE is always >= ADVFS_FOB_SZ.
 */
#define ADVFS_OFF_TO_FIRST_FOB_IN_VDBLK(offset) \
    (((offset)/DEV_BSIZE) * ADVFS_FOBS_PER_DEV_BSIZE)
#define ADVFS_OFF_TO_LAST_FOB_IN_VDBLK(offset,size)                     \
    (ADVFS_OFF_TO_FIRST_FOB_IN_VDBLK((offset)+(size)-1) +               \
     (ADVFS_FOBS_PER_DEV_BSIZE-1))

/* Used with Active Ranges, where storage allocation is a concern.  Round up
 * or down a file byte offset (and transfer length) into allocation unit
 * (bfap->bfPageSz) aligned FOBs.  Files that have a small (< VM_PAGE_SZ) 
 * allocation unit effectively have two different allocation units,
 * depending on the offset into the file.  If the offset is < VM_PAGE_SZ,
 * the allocation unit is bfap->bfPageSz * ADVFS_FOB_SZ.  But once we get to
 * VM_PAGE_SZ, we start allocating storage in contiguous chunks of size
 * VM_PAGE_SZ bytes.  So these macros have to be aware of this.  In the
 * case of ADVFS_AU_ROUND_DOWN(), if we're in the first VM_PAGE_SZ bytes
 * of the file, we'll round down to zero there because we may need to
 * backfill.  For example, if we have a file with 8k at offset 4k
 * and then we decide to write 1k at offset 1k, we'll backfill from
 * 0k to 1k to avoid creating a hole smaller than VM_PAGE_SZ.
 */
#define ADVFS_AU_ROUND_DOWN(_bfap,_offset)                                     \
    ((ADVFS_FILE_HAS_SMALL_AU(_bfap)) ?                                        \
    (rounddown( (_offset)/(ADVFS_FOB_SZ), (VM_PAGE_SZ/ADVFS_FOB_SZ))) :        \
    (rounddown( (_offset)/(ADVFS_FOB_SZ), (_bfap)->bfPageSz )))       

#define ADVFS_AU_ROUND_UP(_bfap,_offset,_size)                                 \
    ((ADVFS_FILE_HAS_SMALL_AU(_bfap) &&                                        \
      (((_offset) + (_size) >= (off_t)VM_PAGE_SZ) ||                           \
       ((_bfap)->file_size >= (off_t)VM_PAGE_SZ))) ?                           \
    (roundup( ((_offset)+(_size)+(ADVFS_FOB_SZ)-1)/(ADVFS_FOB_SZ),             \
              (VM_PAGE_SZ / ADVFS_FOB_SZ) ) - 1 ) :                            \
    (roundup( ((_offset)+(_size)+(ADVFS_FOB_SZ)-1)/(ADVFS_FOB_SZ),             \
              (_bfap)->bfPageSz ) - 1 ))
/*
 * Used to determine allocation unit size in bytes - for small files
 * the allocation unit size is dependent on the file size.
 */
#define ADVFS_ALLOCATION_SZ(_bfap)                                             \
    ((ADVFS_FILE_HAS_SMALL_AU(_bfap) && _bfap->file_size >= (off_t)VM_PAGE_SZ) ? \
      VM_PAGE_SZ : _bfap->bfPageSz * ADVFS_FOB_SZ)


/* This macro converts a possibly unaligned byte offset to
 * one that is aligned to the lowest byte in its vd block
 */
#define ADVFS_ALIGN_OFFSET_TO_VDBLK_DOWN(offset) \
    (((offset)/DEV_BSIZE) * DEV_BSIZE)

/* This macro converts a number of bytes to the number of vd blocks
 * that span those bytes.
 */
#define ADVFS_BYTES_TO_VDBLKS(bytes) ((bytes)/DEV_BSIZE)

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
#define BSR_API_MAX 40 /* !!!! do not change !!!!   (see above comment) */


#ifndef ADVFS_DEBUG
#define BS_MAX_VDI           254
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

typedef int64_t bfPageRefHT;

/*
 * bitfile-set descriptor pointer.  This is defined here as well as in
 * bs_public.h to keep the compiler happy.
 */
#if ! defined(__BFSETT__)
    /*
     * bs_bitfile_sets.h declares struct bfSet and by rights should be the
     * offical home of bfSetT, but we need it here as well, so this trick
     * eliminates the compiler error for trying to declare bfSetT twice
     * while making sure that it is declared once.
     */
    typedef struct bfSet bfSetT;
#define __BFSETT__
#endif /* __BFSETT__ */

#define BFSET_VALID( _bfSetp )                                          \
(                                                                       \
    (_bfSetp) != NULL && (_bfSetp)->bfSetMagic == SETMAGIC              \
)


typedef enum {
    BFS_ODS_UNKNONWN,
    BFS_ODS_VALID,
    BFS_ODS_INVALID,
    BFS_ODS_DELETED
} bfSetStateT;


typedef enum {
        BS_ALLOC_DEFAULT = 0x1, /* use default algorithm */
        BS_ALLOC_FIT     = 0x2, /* try to find a fit */
        BS_ALLOC_NFS     = 0x4, /* use NFS-friendly algorithm */
        BS_ALLOC_RSVD    = 0x8, /* reservation for reserved files */
        BS_ALLOC_MIG_RSVD= 0x10, /* override previous migrate reservation */
        BS_ALLOC_MIG_SINGLEXTNT = 0x20, /* attempt to get a single xtnt for blk request */
        BS_ALLOC_MIG_SAMEVD     = 0x40, /* alloc space on same vd as current primary mcell */
        BS_ALLOC_ONE_FTX        = 0x80  /* Do not try to finish the transaction and start a new
                              * one, just return what was allocated */
} bsAllocHintT;




typedef struct ftxH {
    struct domain *dmnP;  /* domain pointer */
    int32_t hndl;         /* handle into active ftx table */
    uint32_t level;       /* ftx level (root = 0) */
} ftxHT;

#define UINT64T_ADD(uint64t_field, addend) \
  (uint64t_field) = (uint64t_field) + (addend);

#define UINT64T_SUB(uint64t_field, subtrahend) \
  (uint64t_field) = (uint64t_field) - (subtrahend);

#define BS_UID_EQL(bs_uid1, bs_uid2) \
    ((bs_uid1).id_sec == (bs_uid2).id_sec && \
     (bs_uid1).id_usec == (bs_uid2).id_usec)

#define BS_COOKIE_EQL(bs_cookie1, bs_cookie2) \
    ((bs_cookie1).id_sec == (bs_cookie2).id_sec && \
     (bs_cookie1).id_usec == (bs_cookie2).id_usec)
/*
 ** Bitfile Tag -- Unique bitfile identifier.
 */

#define BFM_RSVD_CELLS 9 /* must agree with value in bs_ods.h */
#define BFM_RSVD_TAGS  6 /* must agree with value in bs_ods.h */

/*
 * Definition of the tag numbers for the two quota files.
 * These are fixed by the code in rbf_bfs_create() and
 * fs_create_file_set() which create the first files in a
 * new fileset in this order:
 * 1 - UNUSED
 * 2 - root directory
 * 3 - .tags directory
 * 4 - user quota file
 * 5 - group quota file
 * Tags from 6 and up are for user-created files and directories.
 * 
 * define tag constants here to prevent compilation problems 
 */
#define ROOT_FILE_TAG 2        /* root ("/") tag number        */  
#define TAGS_FILE_TAG 3        /* tags ("/.tags") tag number   */
#define USER_QUOTA_FILE_TAG  4 /* user  quota ("/quota.user")  */
#define GROUP_QUOTA_FILE_TAG 5 /* group quota ("/quota.group") */
#define LOST_FOUND_DIRECTORY 6 /* "/lost+found" directory      */    

/* max number of non-reserved tags - assigned to uint64_t tag_num but
 * need to allow for tag_num to be less than zero for reserved tags.
 * ADV_ENCODE_TAG and ADV_DECODE_TAG have a dependency that MAX_ADVFS_TAGS
 * is less then or equal to 48 bits of an unsigned value.  For 11.31,
 * we are limiting the total number of advfs files per filesystem to
 * a 32 bit unsigned int, even though the on disk structures maintain 
 * the value in a 64 bit unsigned int.
 */
#define MAX_ADVFS_TAGS 0x00000000FFFFFFFFLL
#define ADVFS_LOWEST_TAG_NUM 2

/* Macros for tags. */

#define BS_BFTAG_EQL(tag1, tag2) \
    (((tag1).tag_num == (tag2).tag_num) && ((tag1).tag_seq == (tag2).tag_seq))
#define BS_BFTAG_IDX(tag) ((tag).tag_num)  /* used for hashing on tags */
#define BS_BFTAG_RSVD(tag) ((int64_t)((tag).tag_num) < 0)
#define BS_BFTAG_REG(tag) ((int64_t)((tag).tag_num) > 0)
#define BS_BFTAG_SEQ(tag) ((tag).tag_seq)
#define BS_BFTAG_NULL(tag) ((tag.tag_num) == 0)

/*
 * Bit in tag.tag_seq indicating it is a pseudo-tag which exists only in-core.
 * Pseudo-tags are for shadow access structures of metadata files in the root bfSet.
 */
#define BS_PSEUDO_TAG   0x80000000
#define BS_BFTAG_PSEUDO(tag) ((tag).tag_seq & BS_PSEUDO_TAG)

/* Macros for reserved tags. */
#define BS_BFTAG_VDI(tag) ((vdIndexT) -((int64_t)((tag).tag_num)) / BFM_RSVD_TAGS)
#define BS_BFTAG_CELL(tag) (-((int64_t)((tag).tag_num)) % BFM_RSVD_TAGS)
#define BS_BFTAG_RSVD_INIT(tag, vdi, bmtidx) \
    (tag).tag_num = (uint64_t) -((((int64_t)vdi) * BFM_RSVD_TAGS) + (bmtidx)); \
    (tag).tag_seq = 0
#define BS_BFTAG_RBMT(tag) (BS_BFTAG_CELL(tag) == BFM_RBMT)

/* Following macro returns TRUE if tag passed in represents a metadata file
 * of the type passed in metatype.  The second parm must be from the enumerated
 * list in "bfdBfMetaT".
 */
#define BS_IS_TAG_OF_TYPE( tag, metatype ) \
    ( ((((tag).tag_seq)==0) || (BS_BFTAG_PSEUDO(tag)) ) &&  \
      (((-((int64_t)((tag).tag_num))) % BFM_RSVD_TAGS)==(metatype)) ? 1 : 0)

#define BS_IS_META( _bfap ) ( (_bfap->dataSafety == BFD_METADATA)|| (_bfap->dataSafety == BFD_LOG) )


typedef  bsIdT bfDomainIdT;
typedef  bfDomainIdT bfFsCookieT;
typedef  adv_time_t vdCookieT;

/*
 * Mcell related macros
 */
#define REC_TO_MREC(x) \
  ((bsMRT *)(((char *) (x)) - sizeof(bsMRT)))
#define MREC_TO_REC(x) \
  ((void *)(((char *) (x)) + sizeof(bsMRT)))
#define NEXT_MREC(x) \
  ((bsMRT *)(((char *) (x)) + roundup((x)->bCnt, sizeof(uint64_t))))
#define MCID_EQL(x1, x2) \
  (((x1).cell == (x2).cell) && ((x1).page == (x2).page) && \
   ((x1).volume == (x2).volume))

/*
 * bfMCIdT - bitfile mcell id.
 *  = BMT page number and cell number (address within a virtual disk)
 */

/*
 * The following macros are used to fetch Mcell ID values for comparison
 * or assignment. They are used to ensure that the TERM (-1) values used
 * to indicate the end of a list or a chain are compared/assigned correctly
 * with/to other variables. The Mcell ID values and most (all?) ot the
 * 
 */

#define VOL_TERM     0xff
#define PAGE_TERM    0xffffffffffff 
#define CELL_TERM    0xff

#define FETCH_MC_VOLUME(mcidp)  \
        (((mcidp).volume) == VOL_TERM  ? (-1) : ((mcidp).volume))

#define FETCH_MC_PAGE(mcidp)    \
        (((mcidp).page)   == PAGE_TERM ? (-1) : ((mcidp).page))

#define FETCH_MC_CELL(mcidp)    \
        (((mcidp).cell)   == CELL_TERM ? (-1) : ((mcidp).cell))


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
 *  = mcell id (address - volume, page, cell) + global tag id
 */
typedef struct {
    bfMCIdT mcell;
    bfUTagT ut;
} mcellUIdT;


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
    uint32_t num;
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
    uint16_t page;    /* Log page number of the record */
    uint16_t offset;  /* Log page offset of the record */
    lsnT   lsn;      /* Logical sequence number of the record */
} logRecAddrT;

#define RECADDR_EQ( rec1, rec2 ) \
     (((rec1).page == (rec2).page) && \
      ((rec1).offset == (rec2).offset) && \
      (LSN_EQL( (rec1).lsn, (rec2).lsn )))


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

typedef uint32_t bsLogSeqT;

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
    uint64_t nread;   /* number of reads */
    uint64_t nwrite;  /* number of writes */
    uint64_t readblk; /* number of DEV_BSIZE byte blocks read */
    uint64_t writeblk;
} dStatT;

typedef struct bsVdParams {
    bsIdT vdMntId;              /* last mount id */
    bsVdStatesT vdState;        /* virtual disk state */
    vdIndexT vdIndex;           /* vd index within file system */
    bf_vd_blk_t vdSize;         /* size in DEV_BSIZE blocks */
    bf_vd_blk_t stgCluster;     /* number of DEV_BSIZE blocks each bit represents */
    bf_vd_blk_t freeClusters;   /* total number of free clusters */
    bf_vd_blk_t totalClusters;  /* total number of clusters */
    bf_fob_t userAllocSz;       /* allocation unit for user data in 1k fobs */
    bf_fob_t metaAllocSz;       /* allocaiton unit for metadata in 1k fobs */
    bf_fob_t bmtXtntPgs;        /* number of pages per BMT extent */
    serviceClassT serviceClass; /* service class provided */
    char vdName[BS_VD_NAME_SZ];
    struct dStat dStat;         /* device statistics */
    uint32_t rdMaxIo;         /* Current read IO transfer device blocks size */
    uint32_t wrMaxIo;         /* Current write IO transfer device blocks size*/
    uint32_t min_iosize;      /* Minimum read/write IO transfer byte size */
    uint32_t max_iosize;      /* Driver's maximum rd/wr IO transfer byte size*/
    uint32_t preferred_iosize;/* Driver's preferred rd/wr IO transfer byte sz*/
} bsVdParamsT;

/*
*   bsDevtList - returned from advfs_enum_domain_devts()
*                (Wave 4 enabler)
*/
typedef struct bsDevtList {
    int       devtCnt;            /* number of dev_ts in array */
    adv_dev_t device[BS_MAX_VDI]; /* dev_t's for domain */
} bsDevtListT;

/* dataSafety values are now stored as bit fields in the tag file
 */
typedef enum {
    BFD_NOT_INITIALIZED = 0x0, /* The field has not be initialized */
    BFD_USERDATA        = 0x2, /* User data file:  No transaction control. */
    BFD_METADATA        = 0x4, /* Metadata file:  Under transaction control. */
    BFD_LOG             = 0x6  /* The log file. */
} bfDataSafetyT;


typedef struct {
    uint64_t   bsed_fob_offset;   /* Offset in 1k units into file */
    uint64_t   bsed_fob_cnt;      /* Count of 1k units */
                                  /* bsed_vol_index must match vdIndexT */
    vdIndexT   bsed_vol_index;    /* Disk volume where blocks are stored */
/*  <size>     padding;   */      /* allow compiler to set in case vdIndexT changes */       
    uint64_t   bsed_vd_blk;       /* Volume block number */ 

   } bsExtentDescT;

#define BS_CLIENT_AREA_SZ 4

/*
 * bfCParamsT
 *
 * Client changeable params.
 */

typedef struct bfCParams {
    serviceClassT reqServices;  /* required service class */
    int32_t rsvd1;
    int32_t rsvd2;
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

typedef enum {
    ST_HAS_PARENT       = 0x1,	/* This file is a snapshot  */
    ST_HAS_CHILD        = 0x2,	/* This file has a snapshot */
    ST_SNAP_OUT_OF_SYNC = 0x4,	/* Snapshot File that is out of sync */
} bf_snap_type_t;
 
 
typedef struct bfParams {
    /* the following are NOT settable by clients/users */
    bfTagT tag;                 /* bitfile tag */
    bf_fob_t bfpPageSize;          /* page size in 1k blocks */
    bf_fob_t bfpBlocksAllocated;   /* number of 1k blocks allocated */
    bf_fob_t bfpNextFob;           /* next 1k file offset block allocate */
    vdIndexT vdIndex;              /* primary mcell vd */
    bf_snap_type_t bfpSnapType;    /* snapshot type */
    uint64_t bfpOrigFileSize;      /* filesize at time of snapset creation.
                                    * Only valid in snapset */
    uint64_t bfpRsvdFileSize;      /* amount of reserved file size; from
                                    * user pre-allocation */
    /* the following are settable by clients/users */
    bfCParamsT cl;              /* client changeable params */
} bfParamsT;

/*
 * Metadata read/modify request statistics - part of the domain structure.
 */

typedef struct bcStat {
    uint64_t refCnt;
    struct {
        uint64_t lazy;
        uint64_t blocking;
        uint64_t clean;
        uint64_t log;
    } unpinCnt;
} bcStatT;

typedef struct bmtStat {
    uint64_t fStatRead;
    uint64_t fStatWrite;
    uint64_t resv1;
    uint64_t resv2;
    uint64_t bmtRecRead[BSR_API_MAX+1];
    uint64_t bmtRecWrite[BSR_API_MAX+1];
} bmtStatT;

typedef struct logStat {
    uint64_t logWrites;
    uint64_t transactions;
    uint64_t segmentedRecs;
    uint64_t logTrims;
    uint64_t wastedWords;
    uint32_t  maxLogPgs;
    uint32_t  minLogPgs;
    uint32_t  maxFtxWords;
    uint32_t  maxFtxAgent;
    uint32_t  maxFtxTblSlots;
    uint32_t  oldFtxTblAgent;
    uint64_t excSlotWaits;
    uint64_t fullSlotWaits;
    uint64_t rsv1;
    uint64_t rsv2;
    uint64_t rsv3;
    uint64_t rsv4;
} logStatT;

/*
 * holds domain and fileset threshold values in memory.
 */
typedef struct {
    uint32_t   threshold_upper_limit;
    uint32_t   threshold_lower_limit;
    uint32_t   upper_event_interval;
    uint32_t   lower_event_interval;
    adv_time_t time_of_last_upper_event;
    adv_time_t time_of_last_lower_event;
} adv_threshold_t;

/*
 * holds domain and fileset threshold values on disk.
 */
typedef struct {
    uint32_t   threshold_upper_limit;
    uint32_t   threshold_lower_limit;
    uint32_t   upper_event_interval;
    uint32_t   lower_event_interval;
} adv_threshold_ods_t;

typedef struct bfDmnParams {
    bfDomainIdT bfDomainId;       /* domain id */
    uint32_t maxVds;              /* maximum vd index */
    bfTagT bfSetDirTag;           /* tag of tag directory */
    bfTagT ftxLogTag;             /* tag of domain log */
    bf_fob_t ftxLogFobs;          /* number of pages in log */
    uint32_t curNumVds;           /* current number of VDs */
    adv_uid_t uid;                /* domain's owner */
    adv_gid_t gid;                /* domain's group */
    adv_mode_t mode;              /* domain's access permissions */
    char domainName[BS_DOMAIN_NAME_SZ];
    struct bcStat bcStat;         /* per domain buffer cache stats */
    struct bmtStat bmtStat;       /* per domain BMT stats */
    struct logStat logStat;       /* per domain LOG stats */
    adv_ondisk_version_t dmnVersion;    /* domain's on-disk version number */
    bfFsCookieT  dmnCookie;       /* domain cookie - must not change */ 
    bf_fob_t userAllocSz;         /* allocation unit for user data in 1k fobs */
    bf_fob_t metaAllocSz;         /* allocaiton unit for metadata in 1k fobs */
    adv_threshold_t dmnThreshold; /* domain threshold values */
} bfDmnParamsT;


typedef struct {
    int64_t blkHLimit;                 /* maximum quota blocks in fileset */
    int64_t blkSLimit;                 /* soft limit for fileset blks */
    int64_t fileHLimit;                /* maximum number of files in fileset */
    int64_t fileSLimit;                /* soft limit for fileset files */
    int64_t blksUsed;                  /* number of quota blocks used */
    int64_t filesUsed;                 /* number of bitfiles used */
    int64_t dmnTotalBlks;              /* number of blocks in set's domain */
    int64_t dmnAvailBlks;              /* num of available blks in set's dmn */
    int64_t dmnAvailFiles;             /* num of available files in set's dmn */
    uint64_t blkTLimit;                /* time limit for excessive disk blk use */
    uint64_t fileTLimit;               /* time limit for excessive file use */

    adv_dev_t fsDev;                   /* fileset's dev_t */
    uint32_t unused2;
    uint32_t unused3;
    uint32_t bfSetFlags;               /* flags for the fileset */

    uint32_t quotaStatus;              /* quota status */

    bfSetIdT bfSetId;
    bfSetIdT bfspParentSnapSet;        /* Parent fileset to this fileset with 
                                        * respect to the snapshot tree */
    bfSetIdT bfspFirstChildSnap;       /* Head of the chain of child snap 
                                        * sets */
    bfSetIdT bfspNextSiblingSnap;      /* Next snapset in the chain on the 
                                        * snap tree */
    uint16_t bfspSnapLevel;            /* 0 = root.  Greater than 0 indicates 
                                        * number of parents back to root. */
    adv_time_t bfspCreateTime;         /* Time that the fileset was created */
    adv_uid_t uid;                     /* set's owner */
    adv_gid_t gid;                     /* set's group */
    adv_mode_t mode;                   /* set's permissions mode */
    char setName[BS_SET_NAME_SZ];      /* bitfile set's name */
} bfSetParamsT;


/*
 * typedefs for bs_inherit()
 */

typedef enum {
    BF_INHERIT_I_TO_I,  /* copy parent's inheritable to childs inheritable */
    BF_INHERIT_I_TO_A,  /* copy parent's inheritable to childs attributes */
    BF_INHERIT_I_TO_IA  /* copy parent's inheritable to childs both */
} bfInheritT;


typedef enum {
    BMT_NORMAL_METADATA,
    BMT_XTNT_METADATA,
    BMT_PRIME_MCELL_XTNT_METADATA
} metadataTypeT;

typedef struct bsBfDesc {
    bfTagT bfSetTag;
    bfTagT bfTag;
    metadataTypeT metadataType;
}  bsBfDescT;

#define advfs_offsetof(s_name, s_member) \
        ((size_t)((char *)&((s_name *)0L)->s_member - (char *)0L))


/*
 * TODO: remove the following #ifndef/#endif for HPUXBOOT once
 *       the bootloader code has been updated
 */
#ifndef HPUXBOOT

char *
advfs_ms_malloc(
           size_t size,         /* in */
           int ln,              /* in */
           char *fn,            /* in */
           int flag,            /* in */
	   int bzero_flag,	/* in */
           int rad_id           /* in */
           );

void
advfs_ms_free(
         void *ptr,
         int ln,              /* in */
         char *fn             /* in */

         );

/* 
 * Allocate some memory from any RAD.  If memory is not immediately
 * available, wait until it is.
 */
#define ms_malloc(size) \
    advfs_ms_malloc(size, __LINE__, __FILE__, M_WAITOK, TRUE, 0)

/* 
 * Allocate some memory from any RAD but do not wait if no
 * memory is immediately available.
 */
#define ms_malloc_no_wait(size) \
    advfs_ms_malloc(size, __LINE__, __FILE__, M_NOWAIT, TRUE, 0)

/* 
 * Allocate some memory but do not wait if memory is unavailable
 * and do not bzero memory.
 */
#define ms_malloc_no_wait_or_bzero(size) \
    advfs_ms_malloc(size, __LINE__, __FILE__, M_NOWAIT, FALSE, 0)

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
    advfs_ms_malloc(size, __LINE__, __FILE__, flag | M_WAITOK, TRUE, 0)

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
    advfs_ms_malloc(size, __LINE__, __FILE__, flag | M_NOWAIT, TRUE, 0)

/*
 * Allocate some memory from any RAD.  If memory is not immediately
 * available, wait until it is.
 *
 * advfs_ms_malloc by default bzero's new memory, for performace reasons,
 * this macro can be called to ask advfs_ms_malloc not to bzero the new memory.
 */
#define ms_malloc_no_bzero(size) \
    advfs_ms_malloc(size, __LINE__, __FILE__, M_WAITOK, FALSE, 0)

#define ms_free( ptr )              advfs_ms_free( ptr, __LINE__, __FILE__ )

int
ms_copyin( void *src, void *dest, int len );

int
ms_copyout( void *src, void *dest, int len );	

#endif /* HPUXBOOT */

#endif /* BS_PUBLIC_INCLUDED */
