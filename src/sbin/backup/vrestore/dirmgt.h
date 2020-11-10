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
 *      Logical directory management types and protos.
 *
 * Date:
 *
 *      Tue Jan 14 14:22:32 1992
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: dirmgt.h,v $ $Revision: 1.1.6.2 $ (DEC) $Date: 1995/10/20 13:56:28 $
 */

#ifndef _DIRMGT_H_
#define _DIRMGT_H_

/*
 * dir_desc_t
 *
 * A dir descriptor defines a directory (its contents; entries) that
 * has been restored to the logical directory file (accessed via 'dir_fd').
 * It is used when scanning a dir's contents (see dir_read()).
 */

typedef struct {
    int fd;                     /* UNIX file descriptor of director file */
    off_t start_offset;         /* Byte offset of dir in dir file */
    off_t cur_offset;           /* Byte offset of cur buf in dir file */
    int len;                    /* Number of bytes in directory */
    int buf_offset;             /* Current entry's byte offset in dir buf */
    char buf[DIRBLKSIZ];        /* Directory buffer; contains dir entries */
} dir_desc_t;

int
logical_dir_creat(
    char *path
    );

void
logical_dir_close(
    int dir_fd
    );

void
logical_dir_write(
    int dir_fd,
    void *buf,
    int bytes
    );

int
dir_mk(
    int dir_fd,
    char *dir_path,
    struct stat *dir_stat,
    ino_t parent_ino
    );

int
dir_open(
    int dir_fd,
    char *path,
    dir_desc_t *dir_desc,
    ino_t *inop
    );

void
dir_close(
    dir_desc_t *dir_desc
    );

void
dir_rewind(
    dir_desc_t *dir_desc
    );

struct dirent *
dir_read(
    dir_desc_t *dir_desc
    );

int
dir_change_cwd(
    int dir_fd,
    char *dir_path
    );

char *
dir_get_path( 
    ino_t ino,
    char *path
    );

ino_t
dir_lookup(
    int dir_fd,
    char *path,
    ino_t *parent_ino
    );

int
dir_restore_me(
    ino_t my_ino,
    ino_t my_parent_ino
    );

int
dir_add_to_restore( 
    int dir_fd, 
    char *name 
    );

int
dir_del_from_restore(
    int dir_fd, 
    char *name 
    );

extern ino_t Root_ino;
extern ino_t Cwd_ino;
extern char Cwd_name[];

#endif /* _DIRMGT_H_ */

/* end dirmgt.h */
