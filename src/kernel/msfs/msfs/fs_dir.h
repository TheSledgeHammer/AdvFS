/*
 *****************************************************************************
 **                                                                          *
 **  (C) DIGITAL EQUIPMENT CORPORATION 1990                                  *
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
 * @(#)$RCSfile: fs_dir.h,v $ $Revision: 1.1.135.5 $ (DEC) $Date: 2007/03/22 13:49:17 $
 */

#ifndef FS_DIR
#define FS_DIR

#include <msfs/ms_public.h>
#include <sys/types.h>

#include <kern/lock.h>

#include <sys/vnode.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/fs_quota.h>

#define FS_DIR_MAX_NAME 255
#define FS_MAX_NAME 80

/* largest page number that is supported (0x7fffffff) */
#define max_page 0x7fffffffL

/* largest file offset that is supported (0xfffffffffff) */
#define max_offset (((max_page + 1L) * ADVFS_PGSZ) - 1L)

#define NIL 0
#define TRUE 1
#define FALSE 0
#define isdir 1

typedef enum {
    FS_FRAG_NONE,
    FS_FRAG_VALID
} fsFragStateT;

/*
 * A directory page (size defined by ADVFS_PGSZ_IN_BLKS) consists of a
 * sequence of variable length entries. Each entry is made up of a
 * fixed header, type fs_dir_header, and a variable length name field.
 * The entire entry is of type fs_dir_entry. The entries in a page
 * (actually in the entire directory) are kept in random
 * order. The last word of a  directory page contains the offset in bytes to 
 * the last entry in the page (for quick path traversal).
 */

/* NOTE this is an on-disk structure and all sizes are for tru64 alpha */
struct fs_stat
{
    bfTagT  st_ino;    
    mode_t  st_mode;
    uid_t   st_uid;
    gid_t   st_gid;
    dev_t   st_rdev;
    off_t   st_size;
    time_t  st_atime;
    int     st_uatime;
    time_t  st_mtime;
    int     st_umtime;
    time_t  st_ctime;
    int     st_uctime;
    uint_t  st_flags;          /* user defined flags for file */
				/* 0x10000000 reserved for AUTOFS */
				/* 0x20000000 reserved for AUTOFS */
    bfTagT  dir_tag;           /* tag of parent directory */
    bfFragIdT fragId;
    u_int   st_nlink;
    uint32T fragPageOffset;
    uint32T st_unused_2;
};

typedef struct fs_stat statT;

/* Max values for st_nlink. 
 * _LINK_MAX_UINT is artificially limited to a value that exceeds what
 * the customer specified as their necessary value (20 bits seemed a
 * nice mask point) when we added this support.  The decision to limit
 * the value was only made so we could do reasonable code testing. Test
 * times may expand dramatically as the value is increased
 *
 * The code and on-disk structures can handle 2^32 - 3 = 4294967293.
 */

#define _LINK_MAX_UINT   1048573  
#define _LINK_MAX_USHORT 65533
#define _LINK_MAX_SHORT  32767

/* record at the end of each directory page */

struct dir_rec {
    uint32T lastEntry_offset;         /* offset to last dir entry */
    uint32T largestFreeSpace;         /* size of biggest empty spot on
                                        the page */
    uint32T pageType;                 /* note - this should always be
                                        the last field on the page -
                                        the type of page */
};

typedef struct dir_rec dirRec;

/* for now only one type */
#define SeqType 1;

/* directory header - *** NOTE *** same fields (order and size) as UFS */

struct dir_header {
    uint32T fs_dir_bs_tag_num;        /* tag.num of file */
    uint16T fs_dir_size;       /* size of directory record in bytes */
    uint16T fs_dir_namecount;  /* length in bytes of file name */
};

typedef struct dir_header directory_header;

/*
 * the full tag for a bf is bigger than the UFS inode field, so store
 * the whole tag ('hidden') after the filename string in a dir entry.
 */

struct dir_ent_end {
    bfTagT fs_dir_bs_tag;             /* full bs tag for file */
};

typedef struct dir_ent_end directory_end;

struct directory_entry {
        directory_header fs_dir_header; /* fixed length header */
        char fs_dir_name_string[FS_DIR_MAX_NAME]; /*  file name */
};
typedef struct directory_entry fs_dir_entry;

/*
 * macro to get a pointer to the tag field in a dir entry
 */

#define GETTAGP(dir_ent_p) (((bfTagT *)((char *)dir_ent_p + \
                                sizeof(directory_header) + \
                                dir_ent_p->fs_dir_header.fs_dir_namecount + \
                                (4 - (dir_ent_p->fs_dir_header.fs_dir_namecount % 4)))))
/*
 * Macro to validate fs_dir_size.
 */

#define FS_DIR_SIZE_OK(size, offset) ((size) > DIRBLKSIZ - ((offset)&(DIRBLKSIZ-1)) || (size) & 3 || \
         (size) < (sizeof(directory_header) + sizeof(int)))

struct fsContext {
    short initialized;          /* zero if fsContext is not initialized */
    short quotaInitialized;     /* zero if quota stuff is not initialized */
    bfTagT undel_dir_tag;       /* tag of undelete directory */
    long fs_flag;               /* flag word - see below */
    int dirty_stats;            /* flag for directories, says update the 
                                    stats in the parent directory entry */
    int dirty_alloc;            /* set if stats from an allocating write
                                   are not on disk (ICHGMETA) */

    lock_data_t file_lock;      /* Use an OSF complex lock (read_write_lock) */

    long dirstamp;              /* stamp to determine directory changes */
    mutexT fsContext_mutex;     /* mutex to take out locks on this structure */
#ifdef ADVFS_DEBUG
    char file_name[30];         /* first 29 chars of file name */
#endif
    bfTagT bf_tag;              /* the tag for the file */
    long last_offset;           /* the offset of the last found entry */
    struct fs_stat dir_stats;   /* stats */
    struct fileSetNode *fileSetNode;  /* pointer to per-fileset info */
    struct dQuot *diskQuot[2];  /* pointers to quota structs */
};

typedef struct {
    bfTagT dir_tag;
    bfTagT ins_tag;
    bfSetIdT bfSetId;
    uint32T page;
    uint32T byte;
    ulong old_size;
} undo_headerT;

struct fs_insert_undo_rec {
    undo_headerT undo_header;
};

typedef struct fs_insert_undo_rec insert_undo_rec; 

struct undel_dir_rec {
    bfTagT dir_tag;
};

/* fs_flag bits (in fsContext) */
#define BRENAME        0x0004       /* bitfile is being renamed */
#define MOD_MTIME      0x0020       /* mtime has changed */
#define MOD_ATIME      0x0040       /* atime has changed */
#define MOD_CTIME      0x0100       /* ctime has changed */
#define M_DELETE       0x0200       /* file is marked for delete */
#define META_OPEN      0x0400       /* metadata file was opened by tag */
#define DONT_UNLOCK    0x1000       /* used in calls to bf_get to not unlock the parent dir */

#define IGNORE_DELETE  0x4000       /* used in calls to bf_get to ignore BSRA_DELETING flag */

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
 * Handy macros for locking the fsContext structure. 
 */

#define FS_FILE_READ_LOCK(cp)  lock_read(&((cp)->file_lock))

#define FS_FILE_READ_LOCK_RECURSIVE(cp)  lock_read_recursive(&((cp)->file_lock))

#define FS_FILE_WRITE_LOCK(cp)  lock_write(&((cp)->file_lock))

#define FS_FILE_UNLOCK(cp)  lock_done(&((cp)->file_lock))

#define FS_FILE_UNLOCK_RECURSIVE(cp)  lock_read_done_recursive(&((cp)->file_lock))

#define FS_FILE_READ_TO_WRITE_LOCK(cp)  lock_read_to_write(&((cp)->file_lock))

/*
 * Macros used to acquire the vm map lock in AdvFS routines.
 * Deadlocks were occurring between code paths that started
 * in VM code and acquired vm_map lock and then called the
 * filesystem to do I/O to a file where the filesystem
 * locked the file, such as page faults on an mmapped region;
 * and system calls to the filesystem such as read() and
 * write() where the filesystem locks the file and then
 * faults on uiomove() accessing the user buffer, and the
 * VM code then locks the vm map lock.  Note that the file
 * must be mmapped for all code paths that involve taking
 * the vm map lock first and then coming to the filesystem
 * and locking the file lock.
 * Fix the deadlocks when the file is mmapped by forcing the 
 * order to always be vm_map lock followed by file_lock.
 * The vm map lock is held for read on read()/write() system 
 * calls for mmapped files and for write on mmap() to avoid a 
 * deadlock that might occur while handling a fault in uiomove() 
 * or getting the lock in u_vp_create().  This is a problem 
 * with multi-threaded tasks, therefore lock it only if 
 * needed (indicated by utask_need_to_lock).  Note that a 
 * single threaded task on entry to fs_read(), fs_write(), 
 * msfs_mmap() will remain single threaded to the end 
 * because only that first thread may call thread_create() 
 * and it is executing in AdvFS. The kernel_map (vs a user 
 * map) is avoided because its vm_map lock is at a 
 * different hierarchy and the kernel_map may not be 
 * mmapped so there is no deadlock problem.
 * For directio read() and write() requests, the user's buffer
 * is wired with a call to vm_map_pageable() in
 * bs_refpg_direct() or bs_pinpg_direct().  This path may get
 * the vm map lock for write.  Since no mmapping can occur for
 * the file until the file lock is released, there can be no
 * races with vm_fault().  Therefore, the vm_map lock is not
 * acquired first for non-mmapped files.  This path violates
 * the hierarchy used when the file is mmapped.  To use the
 * same hierarchy in the directio case, the vm map lock would
 * need to be acquired for write in fs_read() and fs_write()
 * which would serialize directio read() and write() requests
 * and hurt performance of multi-threaded database applications.
 */

#define VM_MAP_LOCK_READ_RECURSIVE()                                    \
        if (u.utask->uu_file_state.utask_need_to_lock &&                \
            current_task()->map != kernel_map )                         \
                lock_read_recursive(&(current_task()->map)->vm_lock);

#define VM_MAP_LOCK_READ_RECURSIVE_TRY(serialized)                      \
        if ((!u.utask->uu_file_state.utask_need_to_lock) ||             \
            (current_task()->map == kernel_map))                        \
            serialized = 1;                                             \
        else                                                            \
            serialized = (boolean_t)                                    \
              lock_read_recursive_try(&(current_task()->map)->vm_lock);

#define VM_MAP_LOCK_READ_DONE_RECURSIVE()                               \
        if (u.utask->uu_file_state.utask_need_to_lock &&                \
            current_task()->map != kernel_map )                         \
                lock_read_done_recursive(&(current_task()->map)->vm_lock);

#define VM_MAP_LOCK_WRITE_RECURSIVE()                                   \
        if (u.utask->uu_file_state.utask_need_to_lock &&                \
            current_task()->map != kernel_map ) {                       \
                lock_write(&(current_task()->map)->vm_lock);            \
                lock_set_recursive(&(current_task()->map)->vm_lock);    \
        }

#define VM_MAP_LOCK_WRITE_RECURSIVE_TRY(serialized)                     \
        if ((!u.utask->uu_file_state.utask_need_to_lock) ||             \
            (current_task()->map == kernel_map))                        \
            serialized = 1;                                             \
        else {                                                          \
            if ((boolean_t)serialized =                                 \
                  lock_try_write(&(current_task()->map)->vm_lock))      \
                lock_set_recursive(&(current_task()->map)->vm_lock);    \
        }

#define VM_MAP_LOCK_WRITE_DONE_RECURSIVE()                              \
        if (u.utask->uu_file_state.utask_need_to_lock &&                \
            current_task()->map != kernel_map ) {                       \
                lock_clear_recursive(&(current_task()->map)->vm_lock);  \
                lock_write_done(&(current_task()->map)->vm_lock);       \
        }

/*
 * define the fid structure used by NFS for short-cut opens
 */
struct msfs_fid{
    u_short msfs_fid_len;           /* size of struct */
    bfTagT  msfs_fid_tag;           /* tag of bitfile */
};

extern int dirTruncEnabled;     /* master switch to disable dir trunc */

/* This struct is used for storing directory truncation info. */
typedef struct {
    uint32T        magic;                /* to check legal reuse */
    uint32T        delCnt;
    void          *delList;
    union {
        struct domain *dmnp;                 /* ptr to domain */
	bfDomainIdT    id;                   /* ID of the domain */
    } domain;
} dtinfoT;
#define DT_INFO_MAGIC_NUM  0xfeeb1e

typedef enum {
    CLEANUP_ANY = 1,
    CLEANUP_STATS
} clupClosedListTypeT;

typedef struct {
    clupClosedListTypeT clean_type;
    union {
        struct domain *dmnp;                 /* ptr to domain */
        bfDomainIdT    id;                   /* ID of the domain */
    } domain;
} closedListInfoT;

/* This structure is used for requests to update flagged (bad) frag file
 * group header pages.
 */
typedef struct {
    bfSetIdT bfSetId;
    uint32T badGrpHdrPg;
} badFragGrpT;

/* Next 2 structs used to support fs_cleanup_thread */
#define FS_INIT_CLEANUP_Q_ENTRIES 128

typedef enum {
    FINSH_DIR_TRUNC = 1,         /* finish a directory truncation */
    CLEANUP_CLOSED_LIST,         /* move bfaps from closed to free list */
    ALLOCATE_BFAPS,              /* put access structures onto free list */
    DEALLOCATE_BFAPS,            /* deallocate an access structure */
    UPDATE_BAD_FRAG_GRP_HDR      /* write out flagged bad group header */
} clupThreadMsgTypeT;

typedef struct {
    clupThreadMsgTypeT msgType;
    /* message contents */
    union {
        dtinfoT dtinfo;                  /* directory truncation info */
        closedListInfoT closedListInfo;  /* closed list info */
        badFragGrpT badFragGrp;          /* update flagged frag group header */
    } body;
} clupThreadMsgT;

#endif /*FS_DIR*/
