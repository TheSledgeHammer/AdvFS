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
 *  AdvFs File System Directory Structure Definition Module
 *
 * Abstract:
 *
 */

#ifndef FS_DIR_KERN_INCLUDED
#define FS_DIR_KERN_INCLUDED

#include <sys/types.h>
#include <sys/dirent.h>
#include <sys/vnode.h>

#include <advfs/advfs_types.h>
#include <advfs/fs_dir.h>
#include <advfs/ms_public.h>
#include <advfs/ms_generic_locks.h>
#include <advfs/fs_quota.h>

#ifdef _KERNEL

struct fsContext {
    ADVSMP_FILESTATS_T fsContext_lock; /* lock on filestats structure */
    int16_t initialized;           /* zero if fsContext is not initialized */
    int16_t quotaInitialized;      /* zero if quota stuff is not initialized */
    bfTagT  undel_dir_tag;         /* tag of undelete directory */
    int64_t fs_flag;               /* flag word - see below */
    int32_t dirty_stats;           /* flag for directories, says update the 
                                      stats in the parent directory entry */
    int32_t dirty_alloc;           /* set if stats from an allocating write
                                      are not on disk (ICHGMETA) */

    ADVRWL_FILECONTEXT_T file_lock;      /* Use an OSF complex lock (read_write_lock) */

    int64_t dirstamp;              /* stamp to determine directory changes */
#ifdef ADVFS_DEBUG
    char file_name[30];            /* first 29 chars of file name */
#endif
    bfTagT bf_tag;                 /* the tag for the file */
    int64_t last_offset;           /* the offset of the last found entry */
    int32_t fsc_has_resid;         /* directory I/O param */
    off_t   fsc_last_dir_fob;      /* end of useful directory contents */
    struct fs_stat dir_stats;      /* stats */
    struct fileSetNode *fsc_fsnp;  /* pointer to per-fileset info */
    struct dQuot *diskQuot[MAXQUOTAS]; /* pointers to quota structs */
};

#endif /* #ifdef _KERNEL */

/* fs_flag bits (in fsContext) */
#define BRENAME        0x0004       /* bitfile is being renamed */
#define MOD_MTIME      0x0020       /* mtime has changed */
#define MOD_ATIME      0x0040       /* atime has changed */
#define MOD_CTIME      0x0100       /* ctime has changed */
#define M_DELETE       0x0200       /* file is marked for delete */
#define META_OPEN      0x0400       /* metadata file was opened by tag */
#define DONT_UNLOCK    0x1000       /* used in calls to bf_get to not unlock the parent dir */
#define IGNORE_DELETE  0x4000       /* used in calls to bf_get to ignore BSRA_DELETING flag */

/* flag bit for fs_update_stats
 *
 * Beware, ADVFS_UPDATE_NO_COW is defined in bs_snapshot.h, and is passed
 * in to fs_update_stats. Other flags such as FTX_NOWAIT could be passed
 * through to bmtr_update_rec so use caution if putting in flags.
 */

#define ADVFS_UPDATE_FINISH_FTX      0x2000

/* flag bits for insert_seq call */
#define HARD_LINK      0x0001       /* inserting a hard link */
#define FAST_SYMLINK   0x0002       /* inserting a fast symlink */
#define START_FTX      0x1000       /* insert under a sub-ftx */
#define DIRECTORY      0x0010       /* inserting a directory */

/* flag bits for seq_search call */
#define PIN_PAGE       0x0001       /* pin the page */
#define REF_PAGE       0x0002       /* ref the page */
#define USE_OFFSET     0x1000       /* use the offset hint */

/* flag bits for glom_dir_entries calls */

#define SKIP_LOST_SPACE_GLOM 0x0001 /* Don't recover lost space if found */
#define UPDATE_LAST_ENTRY    0x0002 /* Last record in page needs updating */
#define LOST_SPACE_FOUND     0x0004 /* Lost space was recovered */
#define SPACE_NEEDS_GLOMMING 0x0008 /* Call the 2nd part of glomming */

/*
 * Macros for dealing with bftag "packing and unpacking" in 
 * a fid struct.  Used in advfs_vget() and advfs_fid() as well as some
 * cfs and nfs routines.
 * 
 * Pack the bftag data because nfs can't handle an opaque value with 
 * more then 10 bytes.  Since we are only copying 48 bits of the tag_num,
 * 48 bits is the upper limit to the number of files that can exist at 
 * one time. The structure which has the limit of 10 bytes used to hold
 * a file handle is struct svcfh.  The encode and decode include 
 * 6 bytes(47 signed bits) of the tag_num and the entire 4 bytes of the 
 * tag_seq for a total of 10 bytes.  If hpux ever supports > 47 bits worth 
 * of files(140 trillion) within a file system, a different solution to 
 * this problem will need to be designed (the negative values are reserved
 * for AdvFS metadata tags).  The following structure is
 * used to avoid any endian issues.
 */

typedef struct {
    uint32_t seq_num;
    uint32_t tag_num_lo; /* least significant bits */
    int16_t  tag_num_hi; /* most  significant bits (signed for metadata tags) */
} adv_fidT;
   
/*
 * Make sure encode length doesn't include any padding.
 */ 
#define ADV_ENCODE_LEN (2*sizeof(uint32_t) + sizeof(uint16_t))

#define ADV_ENCODE_FID(fidp,tagp) {                                      \
    adv_fidT adv_encode;                                                 \
    MS_SMP_ASSERT((int64_t)(tagp)->tag_num <= MAX_ADVFS_TAGS);           \
    MS_SMP_ASSERT(MAX_ADVFS_TAGS <= 0x00007FFFFFFFFFFFLL);/* 48 bit max*/\
    MS_SMP_ASSERT(ADV_ENCODE_LEN <= MAXFIDSZ); /* nfs handle max */      \
    adv_encode.seq_num    = (tagp)->tag_seq;                             \
    adv_encode.tag_num_lo = (uint32_t)(tagp)->tag_num;                   \
    adv_encode.tag_num_hi = (uint16_t)((uint64_t)(tagp)->tag_num >> 32); \
    bcopy(&adv_encode, (fidp)->fid_data, ADV_ENCODE_LEN);                \
    (fidp)->fid_len = ADV_ENCODE_LEN;                                    \
}

#define ADV_DECODE_FID(fidp,tagp,ret) {                                  \
    adv_fidT adv_decode;                                                 \
    MS_SMP_ASSERT( (fidp)->fid_len == ADV_ENCODE_LEN);                   \
    bcopy((fidp)->fid_data, &adv_decode, ADV_ENCODE_LEN);                \
    (tagp)->tag_seq  = adv_decode.seq_num;                               \
    (tagp)->tag_num  = (uint64_t) adv_decode.tag_num_lo;                 \
    (tagp)->tag_num |= ((int64_t) adv_decode.tag_num_hi) << 32;         \
    ret = ((fidp)->fid_len == ADV_ENCODE_LEN) ? 0 : EBAD_TAG;            \
}

/* This struct is used for storing directory truncation info. */
typedef struct {
    uint32_t    magic;                /* to check legal reuse */
    uint32_t    delCnt;
    void       *delList;
    union {
        struct domain *dmnp;                 /* ptr to domain */
	bfDomainIdT    id;                   /* ID of the domain */
    } domain;
} dtinfoT;
#define DT_INFO_MAGIC_NUM  0xfeeb1e


#endif /* FS_DIR_KERN_INCLUDED */
