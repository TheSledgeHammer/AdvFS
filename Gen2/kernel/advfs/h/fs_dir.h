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

#ifndef FS_DIR_INCLUDED
#define FS_DIR_INCLUDED

#include <sys/types.h>
#include <sys/dirent.h>
#include <advfs/advfs_types.h>
#include <advfs/bs_public.h>

#define FS_DIR_MAX_NAME 255
#define FS_MAX_NAME      80

#ifndef TRUE
#   define TRUE  1
#endif
#ifndef FALSE
#   define FALSE 0
#endif

#define NIL   0L


/*
 * A directory page (size defined by ADVFS_METADATA_PGSZ_IN_FOBS) consists
 * of a sequence of variable length entries. Each entry is made up of a
 * fixed header (was fs_dir_header) and a variable length name field.
 * he entire entry is of type fs_dir_entry. The entries in a page (actually
 * in the entire directory) are kept in random order.
 */

/*
 * The directory_header (struct dir_header) type has been deleted 
 * and the fields have been placed directly in the directory_entry
 * structure. This matches the dirent structure AND removes the
 * additional padding added by the compiler to align the 64bit
 * tag_num within a stucture. Removing the padding also saves 4
 * bytes for each directory entry.
 * 
 * The DIR_HEADER_SIZE constant defines the size of the dir_header
 * structure for those functions using sizeof(directory_header) for 
 * various calculations.
 *
 * NOTE: Directory entries (fs_dir_entry) must be 8 byte aligned  
 */

#define DIR_HEADER_SIZE      12

/*
 * the MIN_DIR_ENTRY_SIZE is the sum of:
 * - directory header size (12 bytes)
 * - min 4 bytes for name string to ensure hidden tag_seq alignment
 * - size of tag_seq (tagSeqT) 
 * - 4 bytes needed to ensure the directory entry ends on an 8 byte boundry
 *   to ensure alignment of the fs_dir_bs_tag_num in the following
 *   directory entry
 */
#define MIN_DIR_ENTRY_SIZE  (DIR_HEADER_SIZE + 4 + sizeof(tagSeqT) + 4)

struct directory_entry {
    tagNumT  fs_dir_bs_tag_num;        /* tag.num of file */
    uint16_t fs_dir_size;              /* size of directory record in bytes */
    uint16_t fs_dir_namecount;         /* length in bytes of file name */
    char     fs_dir_name_string[FS_DIR_MAX_NAME]; /*  file name */
};
typedef struct directory_entry fs_dir_entry;

/*
 * macro to get a pointer to the tag field in a dir entry
 */

#define GETSEQP(dir_ent_p)  \
            (((tagSeqT *)((char *)dir_ent_p + \
              DIR_HEADER_SIZE + \
              dir_ent_p->fs_dir_namecount + \
              (4 - (dir_ent_p->fs_dir_namecount % 4)))))

#define GETTAG(tag,dir_ent_p)                             \
            (tag).tag_num = dir_ent_p->fs_dir_bs_tag_num; \
            (tag).tag_seq = *(GETSEQP(dir_ent_p))


/*
 * Macro to validate fs_dir_size.
 */

#define FS_DIR_SIZE_OK(size, offset)                           \
    ((size) > DIRBLKSIZ - ((offset)&(DIRBLKSIZ-1))             \
     || (size) & 7 ||  (size) < (DIR_HEADER_SIZE + sizeof(tagSeqT)))

typedef struct {
    bfTagT   dir_tag;
    bfTagT   ins_tag;
    bfSetIdT bfSetId;
    bs_meta_page_t page;
    uint32_t byte;
    size_t old_size;    /* file_size of dir before change in bytes */
} undo_headerT;

struct fs_insert_undo_rec {
    undo_headerT undo_header;
};

typedef struct fs_insert_undo_rec insert_undo_rec; 

typedef struct bsUndelDir {
    bfTagT  dir_tag;
    uint32_t rsvd1;
    uint32_t rsvd2;
} bsUndelDirT;

#endif /* FS_DIR_INCLUDED */
