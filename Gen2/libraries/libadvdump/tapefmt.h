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
 *      Advfs Storage System
 *
 * Abstract:
 *
 *      Backup tape format definitions.
 *
 *      A save-set consists of one or more volumes of backup
 *      media (like tapes).  Each volume consists of a one or
 *      more fixed sized blocks.  Each block consists of a block
 *      header and one or more variable length records.  Each record
 *      consists of a fixed length record header and a variable length
 *      data section.  Records are typed so that restore can tell
 *      what the data in a record means/represents.  For example, there
 *      is a record type for file attributes, and another for file data.
 *
 *      From a logical point of view a save-set consists of a save-set
 *      summary/info section, a file directories section and a files
 *      section.
 *
 *
 */

#ifndef _TAPEFMT_
#define _TAPEFMT_

#include <advfs/advfs_syscalls.h>

#define BLOCK_HEADER_SIZE     256
#define RECORD_HEADER_SIZE     16

#define DATA_PREFIX_SIZE        8
#define SAVE_SET_NAME_SIZE     32

extern uint32_t Blk_size;  /* defined by vbackup.c and vrestore.c */

#define MAX_DATA_SIZE( bsize )   ((bsize) - BLOCK_HEADER_SIZE)
#define MAX_DATA_FREE( bsize )   (MAX_DATA_SIZE( bsize ) - RECORD_HEADER_SIZE)

#define BF_ATTR_REC_SIZE      (sizeof(struct adv_bf_attr_t))
#define FILE_ATTR_REC_SIZE    (sizeof(struct stat)+sizeof(int32_t)+sizeof(ino_t))
#define DEV_ATTR_REC_SIZE     (sizeof(struct stat)+sizeof(int32_t)+sizeof(ino_t))
#define DIR_ATTR_REC_SIZE     (sizeof(struct stat)+sizeof(int32_t)+sizeof(ino_t))
#define HARD_LINK_REC_SIZE    (2*sizeof(int32_t)+3*sizeof(ino_t))
#define SYM_LINK_REC_SIZE     (2*sizeof(int32_t)+2*sizeof(ino_t))
#define UG_QUOTA32_REC_SIZE   (sizeof(int32_t)+4*sizeof(uint32_t)+2*sizeof(time_t))
#define UG_QUOTA64_REC_SIZE   (2*sizeof(int32_t)+2*sizeof(uint64_t)+2*sizeof(uint32_t)+2*sizeof(time_t))
#define F_QUOTA_REC_SIZE      (4*sizeof(int64_t)+2*sizeof(time_t))
#define SUMMARY_PATH_SIZE     MAXPATHLEN + 1
#define SUMMARY_REC_SIZE      (SUMMARY_PATH_SIZE+4*sizeof(int32_t))

#define OLD_FILE_ATTR_REC_SIZE (sizeof(struct old_stat)+sizeof(int32_t)+sizeof(ino_t))
#define OLD_DEV_ATTR_REC_SIZE (sizeof(struct old_stat)+sizeof(int32_t)+sizeof(ino_t))
#define OLD_DIR_ATTR_REC_SIZE (sizeof(struct old_stat)+sizeof(int32_t)+sizeof(ino_t))

/*
 * To handle basic tape loaders, we now attempt to load the tape
 * a number of times before prompting the user.  If after a reasonable 
 * number of failures, we THEN prompt the user.  At this point I am 
 * picking '4' as a reasonable number of failures, my guess is this can 
 * be lowered.
 */
#define TAPE_LOADER_RETRYS 4

/*
 * WARNING: you MUST rebuild BOTH advdump & advrestore when the VERSION changes.
 */
#define VERSION_SIZE           32
#define VERSION_STRING         "6.0" 

/**************************************************************************/
/*  copies of the stat structures from Tru64 UNIX both current (V5.x) and */
/*  obsolete (V4.x).  Required to restore savesets created on Tru64.      */
/**************************************************************************/

#define TRU64__F64_STAT \
        int32_t   st_dev;            /* ID of device containing a directory*/ \
                                     /*   entry for this file.  File serial*/ \
                                     /*   no + device ID + file generation */ \
                                     /*   number uniquely identify the file*/ \
                                     /*   within the system */                \
        int32_t   st_retired1;                                                \
        uint32_t  st_mode;           /* File mode; see #define's in */        \
                                     /*   sys/mode.h */                       \
        uint16_t  st_nlink;          /* Number of links */                    \
        int16_t   st_nlink_reserved; /* DO NOT USE - this may go away */      \
        uint32_t  st_uid;            /* User ID of the file's owner */        \
        uint32_t  st_gid;            /* Group ID of the file's group */       \
        int32_t   st_rdev;           /* ID of device */                       \
                                     /*   This entry is defined only for */   \
                                     /*   character or block special files */ \
        int32_t   st_ldev;           /* Local ID of device:  */               \
                                     /*  conversion of Cluster ID (st_rdev)*/ \
                                     /*  This entry is valid only for */      \
                                     /*  character or block special files */  \
        int64_t   st_size;           /* File size in bytes */                 \
                                                                              \
        int32_t   st_retired2;                                                \
        int32_t   st_uatime;         /* Microseconds of st_atime */           \
        int32_t   st_retired3;                                                \
        int32_t   st_umtime;         /* Microseconds of st_mtime */           \
        int32_t   st_retired4;                                                \
        int32_t   st_uctime;         /* Microseconds of st_ctime */           \
                                                                              \
        int32_t   st_retired5;                                                \
        int32_t   st_retired6;                                                \
                                                                              \
        uint32_t  st_flags;          /* user defined flags for file */        \
        uint32_t  st_gen;            /* file generation number */             \
                                                                              \
        int64_t   st_spare[4];                                                \
                                                                              \
        uint32_t  st_ino;            /* File serial number */                 \
        int32_t   st_ino_reserved;   /* DO NOT USE - this may go away */      \
                                                                              \
                                     /* Times measured in seconds since */    \
                                     /*   00:00:00 GMT, Jan. 1, 1970 */       \
        int32_t   st_atime;          /* Time of last access */                \
        int32_t   st_atime_reserved; /* DO NOT USE - this may go away */      \
        int32_t   st_mtime;          /* Time of last data modification */     \
        int32_t   st_mtime_reserved; /* DO NOT USE - this may go away */      \
        int32_t   st_ctime;          /* Time of last file status change */    \
        int32_t   st_ctime_reserved; /* DO NOT USE - this may go away */      \
                                                                              \
        int64_t   st_blksize;        /* Preferred I/O blk size */             \
        int64_t   st_blocks;         /* blocks allocated for file */

/* stat struct from Tru64 V5.x */
struct t64_v5_stat {
    TRU64__F64_STAT              /* Old V5.0 Compatible stat structure */
};

#define TRU64__PRE_F64_STAT \
        int32_t   st_dev;         /* ID of device containing a directory*/      \
                                /*   entry for this file.  File serial*/      \
                                /*   no + device ID uniquely identify */      \
                                /*   the file within the system */            \
        uint32_t  st_ino;       /* File serial number */                      \
        uint32_t  st_mode;      /* File mode; see #define's in */             \
                                /*   sys/mode.h */                            \
        uint16_t  st_nlink;     /* Number of links */                         \
        uint32_t  st_uid;       /* User ID of the file's owner */             \
        uint32_t  st_gid;       /* Group ID of the file's group */            \
        int32_t   st_rdev;      /* ID of device */                            \
                                /*   This entry is defined only for */        \
                                /*   character or block special files */      \
        int64_t   st_size;      /* File size in bytes */                      \
                                                                              \
                                /* Times measured in seconds since */         \
                                /*   00:00:00 GMT, Jan. 1, 1970 */            \
        int32_t   st_atime;     /* Time of last access */                     \
        int32_t   st_uatime;                                                  \
        int32_t   st_mtime;     /* Time of last data modification */          \
        int32_t   st_umtime;                                                  \
        int32_t   st_ctime;     /* Time of last file status change */         \
        int32_t   st_uctime;                                                  \
        uint32_t  st_blksize;   /* Preferred I/O block size */                \
        int32_t   st_blocks;    /* blocks allocated for file */               \
        uint32_t  st_flags;     /* user defined flags for file */             \
        uint32_t  st_gen;       /* file generation number */


/* stat struct from Tru64 V4.x */
struct old_stat {
    TRU64__PRE_F64_STAT              /* Old V4.0 Compatible stat structure */
};

typedef struct {
    uint32_t    tv_sec;
    uint32_t    tv_usec;
} small_timeval_t;

/* block header */
union block_header_t {
   u_char filler[BLOCK_HEADER_SIZE];
   struct bheader_t {
      uint32_t block_size;                 /* bytes per save-set block */
      uint32_t flags;                      /* block flags (see BF_* #defines) */
      uint16_t vol_set_num;              
      uint16_t vol_sets;
      uint16_t volume_num;               /* block's save-set volume number */
      uint16_t block_num;                /* block's unique number */
      uint16_t header_crc;               /* CRC for block header       */
      uint16_t block_crc;                /* CRC for block (optional)   */
      uint16_t xor_blocks;               /* number of blocks per XOR group */
      uint16_t xor_block_num;            
      uint32_t ss_id;                     /* save-set id (creation time/date */
      char version[VERSION_SIZE];
      small_timeval_t dump_date;         /* date of this dump tape */
   } bheader;
};

/*  
 *  Macros for swapping endian-ness 
 *  Required for restoring savesets created on Tru64 UNIX.
 */

#define SWAP16(x) ((((x)&0xFF)<<8) | (((x)>>8)&0xFF))

#define SWAP32(value) \
 ((( (value) & 0x000000FF) << 24) | \
  (( (value) & 0x0000FF00) << 8) | \
  (( (value) & 0x00FF0000) >> 8) | \
  (( (value) & 0xFF000000) >> 24))


/* 
 * Block Flags definitions 
 */
#define BF_COMPRESSED       0x1
#define BF_XOR_BLOCK        0x2
#define BF_FILES            0x4
#define BF_DIRECTORIES      0x8
#define BF_QUOTA_DATA       0x10
#define BF_USER_QUOTA_DATA  0x20
#define BF_GROUP_QUOTA_DATA 0x40

/* 
 * Record Type definitions 
 * IMPORTANT:  Add new record types to the end so as to preserve
 *             the ability to restore old dumps.
 */
typedef enum { 
   RT_NULL, 
   RT_END_OF_VOL_SET,
   RT_FILLER,
   RT_DIR_HDR, 
   RT_DIR_DATA, 
   RT_DIR_ATTR, 
   RT_BF_DIR_ATTR,
   RT_FILE_ATTR, 
   RT_BF_FILE_ATTR, 
   RT_FILE_DATA, 
   RT_HARD_LINK, 
   RT_DEV_ATTR,
   RT_SYM_LINK, 
   RT_SUMMARY,
   RT_PROPLIST,
   RT_UQUOTA32,
   RT_GQUOTA32,
   RT_FQUOTA,
   RT_UQUOTA64,
   RT_GQUOTA64,
   RT_ACL
} record_type_t;

/*
 * Record Flag definitions
 */
#define RF_NULL                 0
#define RF_DATA_CONTINUED       1
#define RF_SPAN_RECORD          2

/* 
 * record header 
 *
 */
union record_header_t {
   u_char filler[RECORD_HEADER_SIZE];

   struct rheader_t {
      record_type_t type;
      uint32_t d_offset_lo; /* bits 0-31 of rec's offset into file */
      uint16_t size;      /* bytes in rec excluding header */
      uint16_t flags;     /* record flags (see above) */
      uint32_t d_offset_hi; /* bits 32-63 of rec's offset into file */
   } rheader;
};

/* 
 * bitfile and inheritable attributes record
 */

struct bf_attr_rec_t {
    adv_bf_attr_t bfAttr;
};

/* 
 * file attributes record
 *
 * NOTE:  Although it is not shown here, the file's name comes after
 * this record structure.  The file name is 'fname_bytes' long and it
 * is a \0 terminated string.
 */

union file_attr_rec_t {
   u_char filler[FILE_ATTR_REC_SIZE];

   struct fattr_t {
      struct stat file_stat;
      ino_t parent_ino;
      int32_t fname_bytes;
      /* char fname[fname_bytes]; */
   } fattr;
};

/*
 * This is needed due to changes in the stat structure.  We want to
 * be able to read dump files with the old format.
 */
union old_file_attr_rec_t {
   u_char filler[OLD_FILE_ATTR_REC_SIZE];

   struct old_fattr_t {
      struct old_stat file_stat;
      ino_t parent_ino;
      int32_t fname_bytes;
      /* char fname[fname_bytes]; */
   } fattr;
};

/* 
 * special device attributes record
 *
 * NOTE:  Although it is not shown here, the device's name comes after
 * this record structure.  The device name is 'dname_bytes' long and it
 * is a \0 terminated string.
 */

union dev_attr_rec_t {
   u_char filler[DEV_ATTR_REC_SIZE];

   struct dev_attr_t {
      struct stat dev_stat;
      ino_t parent_ino;
      int32_t dname_bytes;
      /* char dname[dname_bytes]; */
   } dattr;
};

/*
 * This is needed due to changes in the stat structure.  We want to
 * be able to read tapes woth the old format.
 */
union old_dev_attr_rec_t {
   u_char filler[OLD_DEV_ATTR_REC_SIZE];

   struct old_dev_attr_t {
      struct old_stat dev_stat;
      ino_t parent_ino;
      int32_t dname_bytes;
      /* char dname[dname_bytes]; */
   } dattr;
};

/* 
 * symbolic link attributes record
 *
 * NOTE:  Although it is not shown here, the link's file name and the 
 * link come after this record structure.  The link name is 
 * 'lname_bytes' long and it is a \0 terminated string.  The link 
 * 'link_bytes' long and it is a \0 terminated string.
 */

union symbolic_link_rec_t {
   u_char filler[SYM_LINK_REC_SIZE];

   struct slink_t {
      ino_t ino;
      ino_t parent_ino;
      int32_t lname_bytes;
      int32_t link_bytes;
      struct stat link_stat;
      /* char lname[fname_bytes]; */
      /* char link[link_bytes]; */
   } slink;
};

/*
 * This is needed due to changes in the stat structure.  We want to
 * be able to read tapes woth the old format.
 */
union old_symbolic_link_rec_t {
   u_char filler[SYM_LINK_REC_SIZE];

   struct old_slink_t {
      ino_t ino;
      ino_t parent_ino;
      int32_t lname_bytes;
      int32_t link_bytes;
      struct old_stat link_stat;
      /* char lname[fname_bytes]; */
      /* char link[link_bytes]; */
   } slink;
};

/* 
 * hard link attributes record
 *
 * NOTE:  Although it is not shown here, the file's name and the file's
 * link's name come after this record structure.  The file name is 
 * 'fname_bytes' long and it is a \0 terminated string.  The link name
 * 'lname_bytes' long and it is a \0 terminated string.
 */

union hard_link_rec_t {
   u_char filler[HARD_LINK_REC_SIZE];

   struct hlink_t {
      ino_t file_ino;
      ino_t file_parent_ino;
      int32_t fname_bytes;
      struct stat file_stat;
      ino_t link_parent_ino;
      int32_t lname_bytes;
      /* char fname[fname_bytes]; */
      /* char lname[lname_bytes]; */
   } hlink;
};

/*
 * This is needed due to changes in the stat structure.  We want to
 * be able to read tapes woth the old format.
 */
union old_hard_link_rec_t {
   u_char filler[HARD_LINK_REC_SIZE];

   struct old_hlink_t {
      ino_t file_ino;
      ino_t file_parent_ino;
      int32_t fname_bytes;
      struct old_stat file_stat;
      ino_t link_parent_ino;
      int32_t lname_bytes;
      /* char fname[fname_bytes]; */
      /* char lname[lname_bytes]; */
   } hlink;
};

/* 
 * directory attributes record
 *
 * NOTE:  Although it is not shown here, the directory's name comes after
 * this record structure.  The directory name is 'dname_bytes' long and it
 * is a '\0' terminated string.
 */

union dir_attr_rec_t {
   u_char filler[DIR_ATTR_REC_SIZE];

   struct dattr_t {
      struct stat dir_stat;
      ino_t parent_ino;
      int32_t dname_bytes;
      /* char dname[dname_bytes]; */
   } dattr;
};

/*
 * This is needed due to changes in the stat structure.  We want to
 * be able to read tapes woth the old format.
 */
union old_dir_attr_rec_t {
   u_char filler[OLD_DIR_ATTR_REC_SIZE];

   struct old_dattr_t {
      struct old_stat dir_stat;
      ino_t parent_ino;
      int32_t dname_bytes;
      /* char dname[dname_bytes]; */
   } dattr;
};

/*
 * User/Group quota record for 8 byte quotas.  Contains selected fields 
 * from struct dQBlk64.
 */
union ug_quota64_rec_t {
   u_char filler[UG_QUOTA64_REC_SIZE];

   struct ugquota64_t {
        int32_t      id;             /* user or group ID */
        int32_t      unused;         /* padding for alignment */
        uint64_t dqb_bhardlimit; /* absolute limit on disk blks alloc */
        uint64_t dqb_bsoftlimit; /* preferred limit on disk blks */
        uint32_t dqb_fhardlimit; /* maximum # allocated inodes + 1 */
        uint32_t dqb_fsoftlimit; /* preferred inode limit */
        time_t   dqb_btimelimit; /* time limit for excessive disk use */
        time_t   dqb_ftimelimit; /* time limit for excessive files */
   } ugquota64;
};

/*
 * Fileset quota record.  Contains selected fields from mlBfSetParamsT.
 */
union f_quota_rec_t {
   u_char filler[F_QUOTA_REC_SIZE];

   struct fquota_t {
        int64_t blkHLimit;         /* maximum quota blocks in fileset */
        int64_t blkSLimit;         /* soft limit for fileset blks */
        int64_t fileHLimit;        /* maximum number of files in fileset */
        int64_t fileSLimit;        /* soft limit for fileset files */
        time_t blkTLimit;       /* time limit for excessive disk blk use */
        time_t fileTLimit;      /* time limit for excessive file use */
   } fquota;
};

/* summary record */
union summary_rec_t {
   u_char filler[SUMMARY_REC_SIZE];

   struct summary_t {
      int32_t pathname_present;
      int32_t not_used2;
      int32_t not_used3;
      int32_t not_used4;
      char source_dir[SUMMARY_PATH_SIZE];
   } summary;
};

/* 
 * blk_t - generic block 
 *
 * Note that blk_t defines the largest possible block.  The data portion
 * will be smaller for smaller blocks.
 */

struct blk_t {
   union block_header_t bhdr;
   char data[MAX_BLK_SZ - BLOCK_HEADER_SIZE];
};

#endif

/* end tapefmt.h */
