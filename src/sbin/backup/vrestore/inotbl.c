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
 *      Inode hash table routines.
 *
 *      ** THESE ROUTINES ARE NOT CURRENTLY THREAD SAFE **
 *
 *      The Inode Hash Table (Ino_tbl) is used to keep track of
 *      the files and directories to be restored.  It is a hash table
 *      that uses inode numbers has the hash key.  Each hash bucket
 *      points to a list of table entries whose inode numbers hash
 *      to the same bucket.
 *
 *      All directories on the save-set have an entry in the Ino_tbl.
 *      A file will have an entry only if the user explicitly 
 *      specified that the file should or should not be restored.
 *      Files that are selected for restoration because their parent
 *      directory was specified do not necessarily have an entry
 *      in the table.  This is because, unless specified otherwise,
 *      all descendents of a directory are restored automatically
 *      when a directory is selected for restoration.  Therefore,
 *      there is no need to add an entry for every file into the table.
 *      Examples of when a file would have an entry in the table are:
 * 
 *              1.  The user explicitly specified that the file should
 *                  be restored regardless of whether or not its parent
 *                  directory was specified.
 *
 *              2.  The user has specified that a directory is to be
 *                  restored but has also specified that a file in
 *                  the directory is not to be specified.
 *
 *      So, the Ino_tbl is used to identify all directories and all
 *      file's that were explicity selected (to be restored or not restored).
 *
 *      The Ino_tbl also has the properties of a 'tree'.  Each entry points
 *      to its parent directory's entry.  Each entry is also an element
 *      of a siblings list which identifies all files and directories in
 *      the same directory.  The siblings list is pointed to by it's
 *      parent directory via it's 'children' pointer (in other words, each
 *      directory entry has a 'children' pointer that points to the head
 *      of a list of its children).
 *
 * Date:
 *
 *      Wed Jan 15 08:49:28 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: inotbl.c,v $ $Revision: 1.1.52.5 $ (DEC) $Date: 2007/07/02 12:02:39 $";
#endif

#include "../shared/backup.h"
#include "../shared/util.h"
#include "inotbl.h"
#include "vrestore_msg.h"
/* Added for dir. attribute restoration.*/
#include <sys/fcntl1.h>
#include <sys/proplist.h>

#define INOTBL_SZ 8192
#define INOHASH( ino ) (ino % INOTBL_SZ)

/*
 ** Global variables.
 */
static ino_tbl_entry_t *Ino_tbl[INOTBL_SZ];   /* Inode Hash Table */

/* Added for dir. attribute restoration.*/
restore_order_t* last_order = NULL; /* ptr to the tail of linkedlist */
extern int Preserve_modes;
extern int top_level_dir;


/*
 * ino_tbl_add
 *
 * Add an entry to the table.
 */

void
ino_tbl_add(
    ino_t ino,                  /* in - inode number to hash on */
    ino_tbl_entry_t *entry      /* in - ptr to entry */
    )
{
    int idx = -1;

    idx = INOHASH( ino );       /* get hash bucket index */

    /*
     * Add to entry to hash bucket's linked list 
     */

    entry->nxt = Ino_tbl[idx];
    Ino_tbl[idx] = entry;
}

/*
 * ino_tbl_add_dir
 *
 * Add a directory to the table.
 *
 * If successful, returns pointer to directory's entry.
 * Otherwise, exit() is called 'cause all current errors are fatal.
 */

ino_tbl_entry_t *
ino_tbl_add_dir(
    struct stat *dir_stat,      /* in - dir's stats */
    char *dir_name,     /* in - dir's name (can be full path name) */
    off_t dir_offset,   /* in - dir's offset into dir file */
    int dir_len,        /* in - dir's length (in bytes) */
    ino_t parent_ino    /* in - dir's parent's inode number */
    )
{
    ino_tbl_entry_t *new_ent = NULL;
    ino_tbl_entry_t *parent_ent = NULL;
    restore_order_t *order_ent = NULL;
    char *name;
    struct attr_timbuf tbuf;

    if (dir_stat->st_ino != parent_ino) {
        /* we are not adding the root dir */

        parent_ent = ino_tbl_lookup( parent_ino );
        if (parent_ent == NULL) {
            fprintf(stderr, catgets(catd, S_INOTBL1, INOTBL1, "%s: ino_tbl_add_dir(%d); parent ino not found\n"),
                     Prog, parent_ino );
            exit( 1 );
        }
    
        if (parent_ent->type != DIR_ENT) {
            fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL2, "%s: ino_tbl_add_dir(%d); parent not dir ino\n"), 
                     Prog, parent_ino );
            exit( 1 );
        }
    }

    /*
     * Allocate and initialize an entry.
     */
    new_ent = (ino_tbl_entry_t *) malloc( sizeof( ino_tbl_entry_t ) );
    if (new_ent == NULL) {
        fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL3, "%s: out of memory; terminating\n"), Prog );
        exit( 1 );
    }

    *new_ent = Nil_dir_entry;
    new_ent->ino = dir_stat->st_ino;
    new_ent->parent_ino = parent_ino;

    /* setup the data structure for restoring dir. modes and timestamps. 
     * Fix for dir time restoration. 
     */
    if (Preserve_modes) {

        /* Before setting-up the data structure for the dir. chk. is the user
         * is having permissions to restore them on the restored dir.
         */
        if ((geteuid() == dir_stat->st_uid) || (geteuid() == 0)) {

            /*
             * Allocate and initialize the dir. restore order entry into the
             * linked list.
             */
            order_ent = (restore_order_t*) malloc( sizeof( restore_order_t ) );
            if (order_ent == NULL) {
                fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL3, "%s: out of memory; terminating\n"), Prog );
                exit( 1 );
            }
            order_ent->dir_times = (struct attr_timbuf *) malloc (sizeof (tbuf));
            if (order_ent->dir_times == NULL) {
                fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL3, "%s: out of memory; terminating\n"), Prog );
                exit( 1 );
            }

            order_ent->dir_times->atime.tv_sec = dir_stat->st_atime;
            order_ent->dir_times->atime.tv_usec = 0;

            order_ent->dir_times->mtime.tv_sec = dir_stat->st_mtime;
            order_ent->dir_times->mtime.tv_usec = 0;

            order_ent->dir_times->ctime.tv_sec = dir_stat->st_ctime;
            order_ent->dir_times->ctime.tv_usec = 0;

            order_ent->st_mode = dir_stat->st_mode;

            order_ent->uid = dir_stat->st_uid;
            order_ent->gid = dir_stat->st_gid;

            order_ent->dir_proplist_buffer = NULL;
            order_ent->proplist_buffer_size = 0;

            order_ent->ino_ptr = new_ent;
            order_ent->prev = last_order;
            last_order = order_ent;
        }
    }

    if (dir_stat->st_ino == parent_ino) {
        new_ent->parent = NULL;
        new_ent->sibling = NULL;
        new_ent->children = NULL;
        name = "/";

    } else {
        new_ent->parent = parent_ent;
        new_ent->sibling = parent_ent->children;
        parent_ent->children = new_ent;

        name = get_name( dir_name ); /* strip off any preceding path name */
    }

    /*
     * Allocate a buffer for the name.
     */
    new_ent->name = malloc( strlen( name ) + 1 );
    if (new_ent->name == NULL) {
        fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL3, "%s: out of memory; terminating\n"), Prog );
        exit( 1 );
    }

    strcpy( new_ent->name, name );
    new_ent->dir_offset = dir_offset;
    new_ent->dir_len = dir_len;

    /*
     * Add the entry to the table.
     */
    ino_tbl_add( dir_stat->st_ino, new_ent ); 

    return new_ent;
}

/*
 * ino_tbl_add_file
 *
 * Add a file to the table.
 *
 * If successful, returns pointer to file's entry.
 * Otherwise, exit() is called 'cause all current errors are fatal.
 */

ino_tbl_entry_t *
ino_tbl_add_file(
    ino_t file_ino,     /* in - file's inode number */
    char *file_name,    /* in - file's name (can't be full path name) */
    ino_t parent_ino    /* in - file's parent's inode number */
    )
{
    ino_tbl_entry_t *new_ent = NULL;
    ino_tbl_entry_t *parent_ent = NULL;

    parent_ent = ino_tbl_lookup( parent_ino );
    if (parent_ent == NULL) {
        fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL4, "%s: ino_tbl_add_file(%d); parent ino not found\n"), 
                 Prog, parent_ino );
        exit( 1 );
    }

    if (parent_ent->type != DIR_ENT) {
        fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL5, "%s: ino_tbl_add_file(%d); parent not dir ino\n"), 
                 Prog, parent_ino );
        exit( 1 );
    }

    /*
     * Allocate and initialize an entry.
     */
    new_ent = (ino_tbl_entry_t *) malloc( sizeof( ino_tbl_entry_t ) );
    if (new_ent == NULL) {
        fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL3, "%s: out of memory; terminating\n"), Prog );
        exit( 1 );
    }

    *new_ent = Nil_file_entry;
    new_ent->ino = file_ino;
    new_ent->parent_ino = parent_ino;
    new_ent->parent = parent_ent;
    new_ent->sibling = parent_ent->children;
    parent_ent->children = new_ent;

    new_ent->name = malloc( strlen( file_name ) + 1 );
    if (new_ent->name == NULL) {
        fprintf( stderr, catgets(catd, S_INOTBL1, INOTBL3, "%s: out of memory; terminating\n"), Prog );
        exit( 1 );
    }

    strcpy( new_ent->name, file_name );

    /*
     * Add entry to the table.
     */
    ino_tbl_add( file_ino, new_ent ); 

    return new_ent;
}

/*
 * ino_tbl_lookup
 *
 * Given an inode number this routine will attempt to find the
 * corresponding entry in the table.  
 * 
 * Returns a ponter to the entry if successful, NULL otherwise.
 */

ino_tbl_entry_t *
ino_tbl_lookup(
    ino_t ino           /* in - inode number of entry to find */
    )
{
    int idx = -1;
    ino_tbl_entry_t *cur_ent = NULL;

    idx = INOHASH( ino );       /* get hash bucket's index */

    cur_ent = Ino_tbl[idx];     /* get ptr to first entry in hash bucket */

    /*
     * Search entries in hash bucket's list for a matching inode number.
     */

    while (cur_ent != NULL) {

        if (cur_ent->ino == ino) {
            /*
             * Found:  return ptr to entry.
             */
            return cur_ent;
        }

        cur_ent = cur_ent->nxt;  /* go to next entry in the list */
    }

    /*
     * Not Found:  Return NULL. 
     */
    return NULL;
}

/*
 * ino_tbl_build_path
 *
 * Given a table entry this routine will generate the full path name
 * for the corresponding file.  The path name is generated by traversing
 * the 'parent' list recursively and appending the name of each parent
 * to the path name (in the reverse order that the list is traversed by
 * using recursion).
 */

void
ino_tbl_build_path( 
    ino_tbl_entry_t *ent,
    char *path
    )
{
    if (ent == NULL) {
        return;

    } else if (ent->parent == NULL) {
        /* 
         * Must be the root directory.  Append a "/" and return. 
         */
        strcat( path, "/" );

    } else {
        /*
         * Call self recursively to get parent's path name 
         */
        ino_tbl_build_path( ent->parent, path );

        /*
         * Append current directory's name and return.
         */
        strcat( path, ent->name );  
        strcat( path, "/" );
    }
}

/*
 * ino_tbl_mark_parent_dir
 *
 * Given ptr to a table entry this routine will increment or decrement
 * the restore_cnt of all parents (predecessor) of the given entry.
 *
 * Why is this important you say?!  When an entry is added or removed
 * from the set of files/dirs to restore we need to also know if
 * any of its predecessors need to be added or removed.  For example,
 * if the user added a file then we must also restore all of
 * its predecessors (this will be done if their restore_cnt is greater
 * than zero).  Conversely, if a file is removed then we need to
 * know if any of its predecessors should also be removed (in this case
 * we decrement their restore_cnt and if any fall to zero then they
 * will not be restored).
 */

void 
ino_tbl_mark_parent_dir(
    ino_tbl_entry_t *ent,   /* in - ptr to table entry */
    int inc                 /* in - FALSE to decrement; TRUE to increment */
    )
{
    ino_tbl_entry_t *e;

    e = ent;

    /*
     * Walk up the 'parent' list and increment/decrement all their
     * restore_cnts.
     */

    while (e != NULL) {

        if (inc) {
            e->restore_cnt++;

        } else {
            if (e->restore_cnt == 0) {
                fprintf( stderr, 
                         catgets(catd, S_INOTBL1, INOTBL6, "restore cnt is zero; can't dec; terminating\n") );
                exit( 1 );
            }

            e->restore_cnt--;
        }

        e = e->parent;
    }
}

/*
 * ino_tbl_mark_dir
 *
 * Similar to ino_tbl_mark_parent_dir() except that this routine
 * also marks all descendents if the restore status is changed (ie- if
 * they were not in the 'to be restored' set then we add them (mark them)).
 */

void 
ino_tbl_mark_dir(
    ino_tbl_entry_t *ent,    /* in - ptr to table entry */
    int mode                 /* in - FALSE to not restore; TRUE to restore */
    )
{
    ino_tbl_entry_t *child;

    if (ent->restore_me != mode) {
        ent->restore_me = mode;
        ino_tbl_mark_parent_dir( ent->parent, mode );
    }

    child = ent->children;

    while (child != NULL) {

        if (child->type == DIR_ENT) {
            ino_tbl_mark_dir( child, mode );

        } else {
            if (child->restore_me != mode) {
                child->restore_me = mode;
                ino_tbl_mark_parent_dir( child->parent, mode );
            }
        }

        child = child->sibling;
    }
}

/*
 * ino_tbl_restore_me
 *
 * Returns TRUE if the file or dir represented by a given table entry
 * should be restored; otherwise, FALSE is returned.
 */

int
ino_tbl_restore_me(
    ino_tbl_entry_t *my_ent,
    ino_tbl_entry_t *my_parent_ent
    )
{
    if (my_ent == NULL) {
        /* 
         * Must be a file that we don't have in the table.  Therefore,
         * it should be restored only if it's parent is to be restored.
         */

        return (my_parent_ent->restore_me);
    }

    if (my_ent->restore_me) {
        return TRUE;
    }

    if ((my_ent->type == DIR_ENT) && (my_ent->restore_cnt > 0)) {
        /*
         * This is a directory and it has descendents which need to
         * be restored.  Therefore, we must also restore the directory;
         * however, this doesn't imply that all of it files will be
         * restored.
         */
        return TRUE;
    }

    return FALSE;
}


/*
 * restore_dir_attr
 * 
 * This routine restores all the dir. attributes (including times, modes,
 * permission bits and ACLs (if any) on the restored directories. 
 * This is done after the ordered set has been completely restored on to 
 * the target fileset. The order in which the directory attributes are 
 * restored is goverened by the order in which they were created. This 
 * order is maintained in the restore_order_t structure corresponding to 
 * each dir. entry.
 */

    void 
restore_dir_attr(void)
{
    int fd = -1, informed_once = 0, root_user = 0;
    char path[MAX_PATH_SZ]="";
    char restore_path[MAX_PATH_SZ]="";
    restore_order_t *restore_order = NULL;
    restore_order_t *tmp_ptr = NULL;
    ino_tbl_entry_t *curr_ent = NULL;
    struct proplistname_args all_entries;
    struct stat dir_stat;
    uid_t fuid;
    gid_t fgid;

    /* Get the dir path from ino tbl entry */
    if (getcwd (path, sizeof(path)) == NULL) {
        perror(catgets(catd, S_PATCH1, PATCH_VRESTORE222, "path"));
        exit(1);
    }

    /* Chk. the user type. */
    if ( geteuid() == 0 ) {
        root_user = 1;
    }

    while (last_order) {
        /* Now start restoring the dir. attrs from the last restored dir.*/
        curr_ent = last_order->ino_ptr;

        /* restore_me or restore_cnt for some dirs could be TRUE during 
         * selective restoration.*/
        if ((curr_ent != NULL) && (curr_ent->restore_me || curr_ent->restore_cnt))
        {
            /* build dir path from ino tbl entry */
            strcpy( restore_path, path);
            ino_tbl_build_path( curr_ent->parent, restore_path );
            strcat( restore_path, curr_ent->name );

            /* Chk. if restore_path is a dir. only. In a rare case 
             * this could be a file also.
             */
            if ((stat (restore_path, &dir_stat) == 0) 
                    && (!S_ISDIR(dir_stat.st_mode))) {
                /* This is not the dir. we intended to restore. */    
                fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE227, "%s: WARNING: unable to set directory attributes, not a directory <%s>\n"), Prog, restore_path);
                if (last_order->dir_proplist_buffer)
                    free (last_order->dir_proplist_buffer);

                break;
            }

            /* Do not restore dir attrs on top level dir when selective (-x)
             * or when interactive (-i) restore. */
            if ((strcmp(curr_ent->name, "/") == 0) && (top_level_dir)) {
                /* Do nothing here if the above conditional is true.*/
            }
            /* Restore attributes only if owner */
            else if ((geteuid() != last_order->uid) && !root_user){
                fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE228, "%s: WARNING: insufficient privilege to restore attributes on <%s>\n"), Prog, restore_path);
            }
            else {
		/* change uid & gid only if they differ */
		fuid = (last_order->uid != dir_stat.st_uid)? last_order->uid : -1;
		fgid = (last_order->gid != dir_stat.st_gid)? last_order->gid : -1;
                if (chown(restore_path, fuid, fgid) < 0) {
                    fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE225, "%s: WARNING: insufficient privilege to restore attributes on <%s>; [%d] %s\n"), Prog, restore_path, errno, ERR_MSG);
                }
                else {
                    if (chmod( restore_path, last_order->st_mode & 07777 )) {
                        fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE225, "%s: WARNING: insufficient privilege to restore attributes on <%s>; [%d] %s\n"), Prog, restore_path, errno, ERR_MSG);
                    }
                    /* Restore saved ACLs/Extended attributes now. 
                     * There is still a chance that property list/ACLs are
                     * existing on this dir. If there are more sets of 
                     * property lists on the existing dir., setproplist() 
                     * overwrites only the overlapping sets. In order to 
                     * make sure the restored dir. will only have property 
                     * list in the archived buffer, delete the existing 
                     * property list and then setproplist.
                     */

                    all_entries.pl_mask = PLE_FLAG_ALL;
                    all_entries.pl_numnames = 0;
                    all_entries.pl_names = NULL;

                    if (delproplist(restore_path, 0, &all_entries)) {
                        fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE224,"%s: WARNING: can't delete existing proplist on <%s>; [%d]\n"), Prog, restore_path, errno); 
                    } else {
                        if (last_order->dir_proplist_buffer) {
                            if ((setproplist(restore_path, 0, 
                                            last_order->proplist_buffer_size, 
                                            last_order->dir_proplist_buffer)) < 0) {
                                fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE223, "%s: WARNING: unable to set property list on <%s>; [%d]\n"), Prog, restore_path, errno);
                            }
                        }
                    }

                    /* restore times now.*/
                    if (root_user) {
                        if ((fd = open(restore_path, O_RDONLY, 0)) == -1) {
                            fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE219, "%s: unable to open restored directory: %s; [%d] %s\n"), Prog, restore_path, errno, ERR_MSG); 
                        } 
                        else  {
                            if (fcntl(fd, F_SETTIMES, last_order->dir_times) == -1) {
                                fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE220, "%s: unable to set time stamps on restored directory: %s; [%d] %s\n"), Prog, restore_path, errno, ERR_MSG); 
                            }
                            close (fd);
                        }
                    } else {
                        /* fcntl() fails for a non-root user. 
                         * Restore only the atime and mtime using utimes().  
                         */
                        if (utimes( restore_path, last_order->dir_times )) {
                            fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE220, "%s: unable to set time stamps on restored directory: %s; [%d] %s\n"), Prog, restore_path, errno, ERR_MSG); 
                        }
                        if ( !informed_once ) {
                            fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE221, "%s: As non-root user, only atime and mtime will be set on restored directories.\n"), Prog); 
                            informed_once = 1;
                        }
                    }

                    if (strcmp (curr_ent->name, "/") == 0) {
                        fprintf( stderr, catgets(catd, S_PATCH1, PATCH_VRESTORE226, "%s: WARNING: may have restored directory attributes, from archive onto target directory.\n"), Prog); 
                    }

                }
            }
        }

        if (last_order) {
            if (last_order->dir_proplist_buffer)
                free (last_order->dir_proplist_buffer);
            if (last_order->dir_times)
                free (last_order->dir_times);
            tmp_ptr = last_order;
            last_order = last_order->prev;
            free (curr_ent);
            free (tmp_ptr);
            curr_ent = NULL;
            tmp_ptr = NULL;
        }

    } /* end of while (last_order)*/
}
/* end inotbl.c */
