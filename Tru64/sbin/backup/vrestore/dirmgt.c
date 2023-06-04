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
 *      Defines routines to manage and access a logical directory (such as
 *      the one used by the restore program).
 *
 *      ** THESE ROUTINES ARE NOT CURRENTLY THREAD SAFE **
 *
 *      A logical directory is a UNIX file that contains the contents
 *      of all the directories that have been restored from the directory
 *      section of a save-set.  The directories are restored from the
 *      save-set to the logical directory (also called the 'directory file')
 *      one at a time and are place one after another in the dir file:
 *
 *           dir1 dir2 dir3 .... dirN
 *
 *      As each directory is placed into the dir file we also create
 *      an entry for it in the Inode Hash Table.  The table entry
 *      describe the directory's position within the dir file (we use
 *      the byte offset into the dir file so that we can use lseek()
 *      to locate the start of a directory) and the length of the
 *      directory (refer to inotbl.c for more info).
 *
 * Date:
 *
 *      Tue Jan 14 14:04:02 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: dirmgt.c,v $ $Revision: 1.1.82.1 $ (DEC) $Date: 2005/11/07 10:23:05 $";
#endif

#include "../shared/backup.h"
#include "dirmgt.h"
#include "inotbl.h"
#include <msfs/msfs_syscalls.h>
#include "vrestore_msg.h"

extern nl_catd catd;

ino_t Root_ino;                 /* root directory's inode number */
ino_t Cwd_ino;                  /* current working dir's inode number */
char Cwd_name[MAX_PATH_SZ];     /* current working dir's full path name */

struct dirent *fake_dp = NULL;

#if 0
#define MIN_ENT_SZ (sizeof( fake_dp->d_ino ) + \
                    sizeof( fake_dp->d_reclen ) + \
                    sizeof( fake_dp->d_namlen ) + \
                    4 + \
                    sizeof( mlBfTagT ))
#else
#define MIN_ENT_SZ (sizeof( fake_dp->d_ino ) + sizeof( fake_dp->d_reclen ))
#endif


/*
 * logical_dir_creat
 *
 * Creates a logical directory file.
 *
 * Returns the file's UNIX file handle (descriptor).
 */

int
logical_dir_creat(
    char *dir_path
    )
{
    int dir_fd;
    char Tmp_dname[FILE_NAME_SIZE];

    sprintf( Tmp_dname, "%s.%d", dir_path, getpid() );

    dir_fd = open( Tmp_dname, O_CREAT | O_RDWR | O_EXCL, 0640 );
    if (dir_fd < 0) {
        fprintf( stderr, catgets(catd, S_DIRMGT1, DIRMGT2, "%s: can't create restore dir <%s>; [%d] %s; terminating\n"),
                 Prog, Tmp_dname, errno, ERR_MSG );
        exit( 1 );
    }

    /* 
     * Make the logical dir invisible.  This also has the effect of
     * deleting the dir file when it is closed when we're done with
     * it.
     */
    unlink( Tmp_dname );

    return dir_fd;
}

/*
 * logical_dir_close
 *
 * Closes a logical directory.  Since the dir file is invisible, closing
 * it also deletes it.
 */

void
logical_dir_close(
    int dir_fd          /* in - dir file's UNIX file handle */
    )
{
    close( dir_fd );
}

/*
 * This global dir desc is used to set the dir length.  It is assumed
 * that the caller will first call dir_mk() and write the dir data.
 * The dir len is set to zero in dir_mk() and is increased in 
 * logical_dir_write() by the number of dir data bytes written to the dir.
 * We used to use the stats.st_size in dir_mk() but this was never
 * accurate because mdump saves selected dir entries so this new method
 * is accurate.
 */
ino_tbl_entry_t *Cur_dir_mk = NULL;

/*
 * logical_dir_write
 *
 * Write a buffer to a logical directory file.
 */

void
logical_dir_write(
    int dir_fd,         /* in - logical dir file's handle */
    void *buf,          /* in - buffer to write to dir */
    int bytes           /* in - number of bytes to write */
    )
{
    if (write( dir_fd, buf, bytes ) < bytes) {
        fprintf( stderr, catgets(catd, S_DIRMGT1, DIRMGT3, "%s: dir_write() error; [%d] %s; terminating\n"),
                 Prog, errno, ERR_MSG );
        exit( 1 );
    }

    /* Update the number of bytes in the current dir being made */

    if (Cur_dir_mk != NULL) {
        Cur_dir_mk->dir_len += bytes;
    }
}

/*
 * dir_mk
 *
 * Create a dir in a logical dir file.
 */

int
dir_mk(
    int dir_fd,                 /* in - logical dir file's handle */
    char *dir_path,             /* in - dir's file name */
    struct stat *dir_stat,      /* in - dir's stats */
    ino_t parent_ino            /* in - dir's parent's inode number */
    )
{
#ifndef _OSF_SOURCE
    extern off_t tell( int );
#endif

    if (Root_ino == 0) {
        /*
         * We're creating the root directory.
         */
        Root_ino = dir_stat->st_ino;
        Cwd_ino = Root_ino;
        strcpy( Cwd_name, "/" );
    }

    if (ino_tbl_lookup( dir_stat->st_ino ) != NULL) {
        return EEXIST;
    }

    /*
     * Add the new dir to the Inode Hash Table.  And save new dir's
     * desc in global variable to be used by logical_dir_write() to
     * set the dir's length.
     */
    /* Pass the dir_stat instead of dir_inode. we need to preserve 
     * times, modes and permissions also.
     */

    Cur_dir_mk = ino_tbl_add_dir( dir_stat, 
                                  dir_path, 
                                  tell( dir_fd ), 
                                  0,
                                  parent_ino );
    return OKAY;
}

/*
 * dir_access
 *
 * Open a directory in the logical dir file via its inode number.
 *
 * Returns OKAY if successful, ERROR otherwise.
 */

static int
dir_access(
    int dir_fd,                 /* in - logical dir file handle */
    ino_t ino,                  /* in - inode number of dir to open */
    dir_desc_t *dir_desc        /* in/out - open dir descriptor */
    )
{
    off_t pos;
    ino_tbl_entry_t *dir_ent;

    /*
     * Get the dir's Inode Table Entry.
     */

    dir_ent = ino_tbl_lookup( ino );
    if (dir_ent == NULL) {
        return ERROR; /* inode not found */
    }

    if (dir_ent->type != DIR_ENT) {
        return ERROR; /* inode not a directory */
    }

    /*
     * Initialize the 'open dir descriptor'.
     */

    dir_desc->start_offset = dir_ent->dir_offset;
    dir_desc->cur_offset = dir_ent->dir_offset;
    dir_desc->len = dir_ent->dir_len;
    dir_desc->fd = dir_fd;
    dir_desc->buf_offset = DIRBLKSIZ;   /* this makes the buf appear invalid */

    /*
     * Seek to the start of the dir we are opening.
     */

    pos = lseek( dir_fd, dir_desc->start_offset, SEEK_SET );
    if (pos < 0) {
        fprintf( stderr, 
                 catgets(catd, S_DIRMGT1, DIRMGT4, "%s: dir_access() lseek error; [%d] %s; terminating\n"),
                 Prog, errno, ERR_MSG );
        exit( 1 );
    }

    return OKAY;
}

/*
 * dir_find
 *
 * Find a file name in a directory; given the dir's inode number
 * and the file's name (not its entire path).
 *
 * Returns the file's inode if found, otherwise zero is returned.
 */

ino_t 
dir_find( 
    int dir_fd,         /* in - logical dir file handle */
    ino_t dir_ino,      /* in - inode of dir to search */
    char *name          /* in - name of file to find */
    )
{
    int status;
    struct dirent *dir_ent;
    dir_desc_t dir_desc;

    if (name == NULL) {
        return 0;
    }

    /*
     * Open the dir.
     */

    status = dir_access( dir_fd, dir_ino, &dir_desc );
    if (status != OKAY) {
        return 0;
    }

    /*
     * Search the dir.
     */

    while ((dir_ent = dir_read( &dir_desc )) != NULL) {

        if (strcmp( dir_ent->d_name, name ) == 0) {
            dir_close( &dir_desc );
            return dir_ent->d_ino;
        }
    }

    dir_close( &dir_desc );

    return 0;
}

/*
 * dir_lookup
 *
 * Find a file name in a directory hierarchy; given the file's
 * file's path name (if it start's with a '/' then the search
 * will begin at the root, otherwise it will begin at the current
 * working directory).
 *
 * Returns the file's inode if found, otherwise zero is returned.
 * If the file is found the file's parent dir's inode number is
 * also returned (via a reference parameter).
 */

ino_t
dir_lookup(
    int dir_fd,         /* in - logical dir file handle */
    char *path,         /* in - file path name to lookup */
    ino_t *parent_ino   /* out - file's parent dir's inode number */
    )
{
    ino_t ino, prev_ino = 0;
    char *cur_partp, *slashp, save_ch;

    /*
     * Assume the search will start at the current working dir.
     */
    prev_ino = ino = Cwd_ino;

    /*
     * cur_partp is used to point to the current path component
     * that is being processed (looked up).
     */
    cur_partp = path;

    if (*cur_partp == '/') {
        /*
         * The search will start at the root dir.
         */
        prev_ino = ino = Root_ino;
    }

    /*
     * Special case for AdvFS quota files.
     */
    if (!strcmp(path, USR_QUOTA_FILE) ||
        !strcmp(path, GRP_QUOTA_FILE)) {
        if (ino != Root_ino)
            return 0;
        *parent_ino = Root_ino;
        if (!strcmp(path, USR_QUOTA_FILE))
            return USR_QUOTA_INO;
        else
            return GRP_QUOTA_INO;
    }

    while (TRUE) {
        while ((cur_partp != NULL) && (*cur_partp == '/')) {
            cur_partp++; /* skip '/' */
        }

        if ((cur_partp == NULL) || (*cur_partp == '\0')) {
            /*
             * We've processed the entire path name.
             */

            /*
             * Get the file's parent dir's inode number.
             */

            if (ino == Cwd_ino) {
               *parent_ino = dir_find( dir_fd, ino, ".." );
            } else {
               *parent_ino = prev_ino;
            }

            return ino; /* return file's inode number */
        }

        /* 
         * Move to next path name component.
         */

        slashp = strchr( cur_partp, '/' );

        if (slashp != NULL) {
            save_ch = *slashp;  /* '/' */
            *slashp = '\0';     /* terminate 'cur_partp' */
        }

        prev_ino = ino;

        /*
         * Look up the path name component in prev component dir.
         */

        ino = dir_find( dir_fd, ino, cur_partp );

        if (slashp != NULL) {
            *slashp = save_ch; /* put back '/' */
        }

        if (ino == 0) {
            /* file not found */
            return 0;
        }

        cur_partp = slashp;
    }
}

/*
 * dir_open
 *
 * Open a dir in the logical dir file; given the dir's path name.
 *
 * Returns OKAY if the dir is opened successfully, otherwise ERROR
 * is returned.
 */

int
dir_open(
    int dir_fd,                 /* in - logical dir file handle */
    char *path,                 /* in - path name of dir to open */
    dir_desc_t *dir_desc,       /* in/out - open dir's descriptor */
    ino_t *inop
    )
{
    ino_t parent_ino;
    int status;

    /*
     * Get dir's inode by searching the dir hierarchy.
     */

    *inop = dir_lookup( dir_fd, path, &parent_ino );
    if (*inop == 0) {
        return ERROR; /* path not found */
    }

    /*
     * Open the dir.
     */

    status = dir_access( dir_fd, *inop, dir_desc );

    if (status != OKAY) {
        return ERROR; /* what?! */
    }

    return OKAY;
}

/*
 * dir_close
 *
 * Close a dir.
 */

void
dir_close(
    dir_desc_t *dir_desc        /* in/out - open dir's descriptor */
    )
{
    dir_desc->cur_offset = dir_desc->len;
    dir_desc->buf_offset = DIRBLKSIZ;
}

/*
 * dir_rewind
 *
 * Reset the dir's current offset to the beginning of the dir.
 */

void
dir_rewind(
    dir_desc_t *dir_desc        /* in/out - open dir's descriptor */
    )
{
    dir_desc->cur_offset = dir_desc->start_offset;
    dir_desc->buf_offset = DIRBLKSIZ;
}

/*
 * dir_read
 *
 * Get the next directory entry.
 *
 * Returns a pointer to the next directory entry if successful; otherwise
 * NULL is returned.
 */

struct dirent *
dir_read(
    dir_desc_t *dir_desc        /* in/out - open dir's descriptor */
    )
{
    int rcnt;
    off_t pos;
    struct dirent *dp = NULL;

    while (TRUE) {
        if ((dir_desc->cur_offset + MIN_ENT_SZ) >= 
            (dir_desc->start_offset + dir_desc->len)) {
            return NULL;  /* end of dir */
        }

        if ((dir_desc->buf_offset + MIN_ENT_SZ) >= DIRBLKSIZ) {
            /*
             * The current dir buf is invalid (or we've scanned it completely).
             * Read the next dir buffer.
             */

            dir_desc->cur_offset = roundup( dir_desc->cur_offset, DIRBLKSIZ );

            pos = lseek( dir_desc->fd, dir_desc->cur_offset, SEEK_SET );
            if (pos != dir_desc->cur_offset) {
                fprintf( stderr, 
                         catgets(catd, S_DIRMGT1, DIRMGT5, "%s: dir_read() seek error; [%d] %s; terminating\n"),
                         Prog, errno, ERR_MSG );
                exit( 1 );
            }
        
            rcnt = read( dir_desc->fd, dir_desc->buf, DIRBLKSIZ );
            if (rcnt < DIRBLKSIZ) {
		if (rcnt == 0) {
            		return NULL;  /* end of dir */
		} else {
                	fprintf( stderr, 
                         catgets(catd, S_DIRMGT1, DIRMGT6, "%s: dir_read() read error; [%d] %s; terminating\n"),
                         Prog, errno, ERR_MSG );
                	exit( 1 );
		}
            }
    
            dir_desc->buf_offset = 0;
        }
    
        /* 
         * Get a pointer to the next dir entry.
         */
        dp = (struct dirent *) &dir_desc->buf[ dir_desc->buf_offset ];
        
        /*
         * Update the 'current dir location" offsets to move past
         * the dir entry we are about to return.
         */

        dir_desc->buf_offset += dp->d_reclen;
        dir_desc->cur_offset += dp->d_reclen;
    
        if (dp->d_reclen == 0) {

            /* reached zero-filled end of this block; goto next block */
            dir_desc->buf_offset = DIRBLKSIZ;
            continue;
        }

        if (dp->d_ino == 0) {
            /* empty entry */
            continue;
        }

        return dp;
    }
}

/*
 * dir_change_cwd
 *
 * Sets the current directory to the specified directory's path name.
 *
 * Returns OKAY if successful, otherwise ERROR is returned.
 */

int
dir_change_cwd(
    int dir_fd,         /* in - logical dir file handle */
    char *dir_path      /* in - path name of new current working dir */
    )
{
    ino_tbl_entry_t *dir_ent;
    ino_t dir_ino, parent_ino;
    char temp_name[MAX_PATH_SZ];

    if (dir_path == NULL) {
        Cwd_ino = Root_ino;
        strcpy( Cwd_name, "/" );
        return OKAY;
    }

    /*
     * Get the new cur working dir's inode number.
     */

    dir_ino = dir_lookup( dir_fd, dir_path, &parent_ino );
    if (dir_ino == 0) {
        return ERROR;
    }

    /*
     * Now get its Inode Table Entry.
     */

    dir_ent = ino_tbl_lookup( dir_ino );
    if (dir_ent == NULL) {
        return ERROR; /* inode not found */
    }

    if (dir_ent->type != DIR_ENT) {
        return ERROR; /* inode not a directory */
    }

    /*
     * Get the new cur working dir's full path name; relative to the root.
     */

    temp_name[0] = '\0';

    if (dir_get_path( dir_ino, temp_name ) != NULL) {
        Cwd_name[0] = '\0';
        strcpy( Cwd_name, temp_name );
        Cwd_ino = dir_ino;
    } else {
        return ERROR;
    }
    
    return OKAY;
}

/*
 * dir_get_path
 *
 * Given an inode number return it's full path name relative to the
 * root dir.
 *
 * Returns a pointer to the path name (this address is actually passed
 * to dir_get_path() via the 'path' parameter; in other words, the caller
 * supplies the buffer for the path.  If the routine is not successful then
 * NULL is returned.
 */

char *
dir_get_path( 
    ino_t ino,  /* in - inode for which to generate a path name */
    char *path  /* in/out - buffer for path name */
    )
{
    ino_tbl_entry_t *ent;
    int i = 0;

    ent = ino_tbl_lookup( ino );
    if (ent == NULL) {
        return NULL;
    }

    ino_tbl_build_path( ent, path );

    return path;
}

/*
 * dir_restore_me
 *
 * Logical Dir interface to ino_tbl_restore_me(). 
 *
 * Given an inode number and the corresponding file or dir's parent
 * inode number, this routine will return TRUE if the file or dir
 * is to be restore; otherwise FALSE is returned.
 */

int
dir_restore_me(
    ino_t my_ino,               /* in - inode of file/dir */
    ino_t my_parent_ino         /* in - file/dir's parent's inode */
    )
{
    return ino_tbl_restore_me( ino_tbl_lookup( my_ino ), 
                               ino_tbl_lookup( my_parent_ino ) );
}

/*
 * dir_add_to_restore
 *
 * Add a file/dir to the set of files/dirs to restore.
 *
 * Returns OKAY if successful; ERROR otherwise.
 */

int
dir_add_to_restore( 
    int dir_fd,         /* in - logical dir file handle */
    char *name          /* in - name of file/dir to add */
    )
{
    ino_t ino, parent_ino;
    ino_tbl_entry_t *ent;

    if (name == NULL) {
        return OKAY;
    }

    /*
     * Get file/dir's inode.
     */

    ino = dir_lookup( dir_fd, name, &parent_ino );
    if (ino == 0) {
        fprintf( stderr, catgets(catd, S_DIRMGT1, DIRMGT7, "file <%s> not found; will not extract it\n"), name );
        return ERROR;
    }

    /*
     * Get the file/dir's Inode Table Entry.
     */

    ent = ino_tbl_lookup( ino );

    if ((ent != NULL) && (ent->type == DIR_ENT)) {
        /* adding a directory */

        ino_tbl_mark_dir( ent, TRUE );

    } else {
        /* 
	 * adding a file 
	 * don't fail to add entries on hard link siblings
	 * if there is an entry with a matching inode but
	 * not a perfect match, then it too must be entered
	 * else its path will not be there when it is time
	 * to make the hard link 
	 */

        if (ent == NULL ||
	   !(strcmp( ent->name, name ) == 0) ||
	   !(ent->parent_ino == parent_ino)) {
            	ent = ino_tbl_add_file( ino, name, parent_ino );
        }

        if (!ent->restore_me) {
            ent->restore_me = TRUE;
            ino_tbl_mark_parent_dir( ino_tbl_lookup( parent_ino ), TRUE );
        }
    }

    return OKAY;
}

/*
 * dir_del_to_restore
 *
 * Remove a file/dir from the set of files/dirs to restore.
 *
 * Returns OKAY if successful; ERROR otherwise.
 */

int
dir_del_from_restore(
    int dir_fd,         /* in - logical dir file handle */
    char *name          /* in - name of file/dir to delete */
    )
{
    ino_t ino, parent_ino;
    ino_tbl_entry_t *ent;

    if (name == NULL) {
        return OKAY;
    }

    /*
     * Get file/dir's inode.
     */

    ino = dir_lookup( dir_fd, name, &parent_ino );
    if (ino == 0) {
        fprintf( stderr, catgets(catd, S_DIRMGT1, DIRMGT8, "%s: file <%s> not found\n"), Prog, name );
        return ERROR;
    }

    /*
     * Get the file/dir's Inode Table Entry.
     */

    ent = ino_tbl_lookup( ino );

    if ((ent != NULL) && (ent->type == DIR_ENT)) {
        /* removing a directory */

        ino_tbl_mark_dir( ent, FALSE );

    } else {
        /* removing a file */

        if (ent == NULL) {
            ent = ino_tbl_add_file( ino, name, parent_ino );
        }

        if (ent->restore_me) {
            ent->restore_me = FALSE;
            ino_tbl_mark_parent_dir( ino_tbl_lookup( parent_ino ), FALSE );
        }
    }

    return OKAY;
}


/* end dirmgt.c */
