/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991                                  *
 */
/* 
 * =======================================================================
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
 *
 *
 * Facility:
 *
 *      MegaSafe Storage System
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
 * Date:
 *
 *      Thu Apr 19 11:41:03 1990
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: tapefmt.h,v $ $Revision: 1.1.23.2 $ (DEC) $Date: 2003/08/27 10:46:09 $
 */
#include <msfs/msfs_syscalls.h>

#ifndef _TAPEFMT_
#define _TAPEFMT_

static char*
rcsid_tapefmt = "tapefmt.h - $Date: 2003/08/27 10:46:09 $";

#define BLOCK_HEADER_SIZE     256
#define OLD_RECORD_HEADER_SIZE	16
#define RECORD_HEADER_SIZE     24

#define DATA_PREFIX_SIZE        8
#define SAVE_SET_NAME_SIZE     32

extern int Blk_size;  /* defined by backup.c and restore.c */

#define MAX_DATA_SIZE( bsize )   ((bsize) - BLOCK_HEADER_SIZE)
#define MAX_DATA_FREE( bsize )   (MAX_DATA_SIZE( bsize ) - RECORD_HEADER_SIZE)

#define BF_ATTR_REC_SIZE      (sizeof(struct mlBfAttributesT))
#define FILE_ATTR_REC_SIZE    (sizeof(struct stat)+sizeof(int)+sizeof(ino_t))
#define DEV_ATTR_REC_SIZE     (sizeof(struct stat)+sizeof(int)+sizeof(ino_t))
#define DIR_ATTR_REC_SIZE     (sizeof(struct stat)+sizeof(int)+sizeof(ino_t))
#define HARD_LINK_REC_SIZE    (2*sizeof(int)+3*sizeof(ino_t))
#define SYM_LINK_REC_SIZE     (2*sizeof(int)+2*sizeof(ino_t))
#define UG_QUOTA32_REC_SIZE   (sizeof(int)+4*sizeof(u_int)+2*sizeof(time_t))
#define UG_QUOTA64_REC_SIZE   (2*sizeof(int)+2*sizeof(u_long)+2*sizeof(u_int)+2*sizeof(time_t))
#define F_QUOTA_REC_SIZE      (4*sizeof(long)+2*sizeof(time_t))
#define SUMMARY_PATH_SIZE     PATH_MAX + 1
#define SUMMARY_REC_SIZE      (SUMMARY_PATH_SIZE+4*sizeof(int))

#define OLD_FILE_ATTR_REC_SIZE (sizeof(struct old_stat)+sizeof(int)+sizeof(ino_t))
#define OLD_DEV_ATTR_REC_SIZE (sizeof(struct old_stat)+sizeof(int)+sizeof(ino_t))
#define OLD_DIR_ATTR_REC_SIZE (sizeof(struct old_stat)+sizeof(int)+sizeof(ino_t))
#define QUOTA32_MAX           0x0ffffffffL

/*
 * To handle basic tape loaders, we now attempt to load the tape
 * a number of times before prompting the user.  If after a reasonable 
 * number of failures, we THEN prompt the user.  At this point I am 
 * picking '4' as a reasonable number of failures, my guess is this can 
 * be lowered.
 */
#define TAPE_LOADER_RETRYS 4

/*
 * WARNING: you MUST rebuild BOTH vdump & vrestore when the VERSION changes.
 */
#define VERSION_SIZE           32
/* #define VERSION_STRING     "$Date: 2003/08/27 10:46:09 $" */ 
#define VERSION_STRING         "5.2"
/* Note: The 5.1 vdump/vrestore introduced the 5.1 archive format
 * which turned out to be problematic because of the lack of forward
 * compatibility in versions existing at the same time and therefore
 * was withdrawn temporarily. We are returning to use of the 5.0
 * archive format (using old_record_header type)
 * and therefore we will no longer embed VERSION_STRING
 * in archives, rather OLD_ARCHIVE_VERSION_STRING
 * The '-V' option continues to report VERSION_STRING
 * When we distribute new versions of vdump and vrestore we can
 * bump the VERSION_STRING, to allow users to distinguish versions
 * but if we find a need to create a new  archive format, we should
 * start it by matching it to the VERSION_STRING
 * at that time. This makes the code meaningful in vrestore when it
 * finds that it isn't smart enough to understand a format so it says
 * "Need vrestore V%s to restore contents" and stuffs it with the
 * Archive version string. So unless that archive version matches 
 * the program Version we would only confuse the user
 */
#define ARCHIVE_VERSION_STRING "5.1"
#define OLD_ARCHIVE_VERSION_STRING "5.0"

/* old stat struct */
struct old_stat {
  __PRE_F64_STAT		/* Old V4.0 Compatible stat structure */
};

/* block header */
union block_header_t {
   u_char filler[BLOCK_HEADER_SIZE];
   struct bheader_t {
      u_int block_size;                 /* bytes per save-set block */
      u_int flags;                      /* block flags (see BF_* #defines) */
      u_short vol_set_num;              
      u_short vol_sets;
      u_short volume_num;               /* block's save-set volume number */
      u_short block_num;                /* block's unique number */
      u_short header_crc;               /* CRC for block header       */
      u_short block_crc;                /* CRC for block (optional)   */
      u_short xor_blocks;               /* number of blocks per XOR group */
      u_short xor_block_num;            
      time_t ss_id;                     /* save-set id (creation time/date */
      char version[VERSION_SIZE];
      struct timeval dump_date;         /* date of this dump tape */
   } bheader;
};

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
   RT_GQUOTA64
} record_type_t;

/*
 * Record Flag definitions
 */
#define RF_NULL                 0
#define RF_DATA_CONTINUED       1
#define RF_SPAN_RECORD          2

/* 
 * old record header 
 *
 */
union old_record_header_t {
   u_char filler[OLD_RECORD_HEADER_SIZE];

   struct old_rheader_t {
      record_type_t type;
      unsigned int d_offset_lo; /* bits 0-31 of rec's offset into file */
      unsigned short size;      
      unsigned short flags;     /* record flags (see above) */
      unsigned int d_offset_hi; /* bits 32-63 of rec's offset into file */
   } rheader;
};

/* 
 * record header 
 *
 */
union record_header_t {
   u_char filler[RECORD_HEADER_SIZE];

   struct rheader_t {
      record_type_t type;
      unsigned int d_offset_lo; /* bits 0-31 of rec's offset into file */

	/* bytes in rec excluding header.Made int from short. */
      unsigned int size;      
      unsigned short flags;     /* record flags (see above) */
      char filler[6];	/* For memory alignment */
      unsigned int d_offset_hi; /* bits 32-63 of rec's offset into file */
   } rheader;
};

/* 
 * bitfile and inheritable attributes record
 */

struct bf_attr_rec_t {
    mlBfAttributesT bfAttr;
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
      int fname_bytes;
      /* char fname[fname_bytes]; */
   } fattr;
};

/*
 * This is needed due to changes in the stat structure.  We want to
 * be able to read tapes woth the old format.
 */
union old_file_attr_rec_t {
   u_char filler[OLD_FILE_ATTR_REC_SIZE];

   struct old_fattr_t {
      struct old_stat file_stat;
      ino_t parent_ino;
      int fname_bytes;
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
      int dname_bytes;
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
      int dname_bytes;
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
      int lname_bytes;
      int link_bytes;
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
      int lname_bytes;
      int link_bytes;
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
      int fname_bytes;
      struct stat file_stat;
      ino_t link_parent_ino;
      int lname_bytes;
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
      int fname_bytes;
      struct old_stat file_stat;
      ino_t link_parent_ino;
      int lname_bytes;
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
      int dname_bytes;
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
      int dname_bytes;
      /* char dname[dname_bytes]; */
   } dattr;
};

/*
 * User/Group quota record for 4 byte quotas.  Contains selected fields 
 * from struct dqblk.
 */
union ug_quota32_rec_t {
   u_char filler[UG_QUOTA32_REC_SIZE];

   struct ugquota32_t {
        int     id;             /* user or group ID */
        u_int   dqb_bhardlimit; /* absolute limit on disk blks alloc */
        u_int   dqb_bsoftlimit; /* preferred limit on disk blks */
        u_int   dqb_ihardlimit; /* maximum # allocated inodes + 1 */
        u_int   dqb_isoftlimit; /* preferred inode limit */
        time_t  dqb_btime;      /* time limit for excessive disk use */
        time_t  dqb_itime;      /* time limit for excessive files */
   } ugquota32;
};

/*
 * User/Group quota record for 8 byte quotas.  Contains selected fields 
 * from struct dQBlk64.
 */
union ug_quota64_rec_t {
   u_char filler[UG_QUOTA64_REC_SIZE];

   struct ugquota64_t {
        int     id;             /* user or group ID */
        int     unused;         /* padding for alignment */
        u_long  dqb_bhardlimit; /* absolute limit on disk blks alloc */
        u_long  dqb_bsoftlimit; /* preferred limit on disk blks */
        u_int   dqb_ihardlimit; /* maximum # allocated inodes + 1 */
        u_int   dqb_isoftlimit; /* preferred inode limit */
        time_t  dqb_btime;      /* time limit for excessive disk use */
        time_t  dqb_itime;      /* time limit for excessive files */
   } ugquota64;
};

/*
 * Fileset quota record.  Contains selected fields from mlBfSetParamsT.
 */
union f_quota_rec_t {
   u_char filler[F_QUOTA_REC_SIZE];

   struct fquota_t {
        long blkHLimit;         /* maximum quota blocks in fileset */
        long blkSLimit;         /* soft limit for fileset blks */
        long fileHLimit;        /* maximum number of files in fileset */
        long fileSLimit;        /* soft limit for fileset files */
        time_t blkTLimit;       /* time limit for excessive disk blk use */
        time_t fileTLimit;      /* time limit for excessive file use */
   } fquota;
};

/* summary record */
union summary_rec_t {
   u_char filler[SUMMARY_REC_SIZE];

   struct summary_t {
      int pathname_present;
      int not_used2;
      int not_used3;
      int not_used4;
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
