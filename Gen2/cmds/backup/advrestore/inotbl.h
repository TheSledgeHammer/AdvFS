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
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      Inode hash table types and protos
 *
 */
#ifndef _INOTBL_H_
#define _INOTBL_H_

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
    uint32_t restore_me : 1;            /* if true then restore file/dir */

    /* 
     ** The following fields are used for 'type == DIR_ENT' only 
     */
    uint32_t            : 31;
    /***** end 32-bit mask *****/

    uint32_t restore_cnt;               /* num descendent entries to restore */
    struct ino_tbl_entry *children;     /* ptr to list of child entries */
    off_t dir_offset;                   /* this dir's offset into dir file */
    int32_t dir_len;                    /* size of this dir (in bytes */
} ino_tbl_entry_t;

static const ino_tbl_entry_t Nil_dir_entry = { DIR_ENT, 0 };

static const ino_tbl_entry_t Nil_file_entry = { FILE_ENT, 0 };

ino_tbl_entry_t *
ino_tbl_add_dir(
    ino_t dir_ino,
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

#endif  /* _INOTBL_H_ */

/* end inotbl.h */
