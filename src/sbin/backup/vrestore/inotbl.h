/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
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
 *      Inode hash table types and protos
 *
 * Date:
 *
 *      Wed Jan 15 08:48:58 1992
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: inotbl.h,v $ $Revision: 1.1.7.1 $ (DEC) $Date: 2005/11/07 10:23:10 $
 */

typedef enum { FREE_ENT, DIR_ENT, FILE_ENT } entry_type_t;

/*
 * ino_tbl_entry_t
 *
 * Defines and Inode Table Entry which describes a file or directory.
 */

typedef struct ino_tbl_entry {
    entry_type_t type;                  /* free, directory or file */
    ino_t ino;                          /* file's/dir's inode number */
    ino_t parent_ino;                   /* parent dir's inode number */
    char *name;                         /* file's/dir's name */
    struct ino_tbl_entry *nxt;          /* ptr to next entry in hash chain */
    struct ino_tbl_entry *parent;       /* ptr to parent dir's entry */
    struct ino_tbl_entry *sibling;      /* ptr to next sibling's entry */

    /***** begin 32-bit mask *****/
    unsigned restore_me : 1;            /* if true then restore file/dir */

    /* 
     ** The following fields are used for 'type == DIR_ENT' only 
     */
    unsigned                       : 31;
    /***** end 32-bit mask *****/

    unsigned restore_cnt;               /* num descendent entries to restore */
    struct ino_tbl_entry *children;     /* ptr to list of child entries */
    off_t dir_offset;                   /* this dir's offset into dir file */
    int dir_len;                        /* size of this dir (in bytes */
} ino_tbl_entry_t;

static const ino_tbl_entry_t Nil_dir_entry = { DIR_ENT, 0 };

static const ino_tbl_entry_t Nil_file_entry = { FILE_ENT, 0 };

/* Added new structure to save dir attrs to be restored at the end of the
 * vrestore process. */
typedef struct restore_order {
    struct attr_timbuf *dir_times;      /* dir. time stamps.*/
    mode_t st_mode;             /* dir. permission bits. */
    uid_t uid;                  /* dir. user id.*/
    gid_t gid;                  /* dir. group id */
    char* dir_proplist_buffer;       /* proplist for the dir. */
    int proplist_buffer_size;   /* size of proplist buffer*/
    ino_tbl_entry_t *ino_ptr;   /* corresponding ino_tbl entry ptr.*/
    struct restore_order *prev; /* ptr. to the previously restored dir. entry.*/
} restore_order_t;

ino_tbl_entry_t *
ino_tbl_add_dir(
    /* Pass the entire stat str. instead of the inode only. */
    struct stat *dir_stat,
    char *dir_name,
    off_t dir_offset,
    int dir_len,
    ino_t parent_ino
    );

ino_tbl_entry_t *
ino_tbl_add_file(
    ino_t file_ino,
    char *file_name,
    ino_t parent_ino
    );

ino_tbl_entry_t *
ino_tbl_lookup(
    ino_t ino
    );

void
ino_tbl_build_path( 
    ino_tbl_entry_t *ent,
    char *path
    );

void 
ino_tbl_mark_dir(
    ino_tbl_entry_t *ent,
    int mode
    );

void 
ino_tbl_mark_parent_dir(
    ino_tbl_entry_t *ent,
    int mode              
    );

int
ino_tbl_restore_me(
    ino_tbl_entry_t *my_ent,
    ino_tbl_entry_t *my_parent_ent
    );

/* end inotbl.h */
