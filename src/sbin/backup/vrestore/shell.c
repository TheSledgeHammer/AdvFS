
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
 *      Restore program's interactive shell.
 *
 * Date:
 *
 *      Tue Jan 14 13:45:02 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: shell.c,v $ $Revision: 1.1.40.2 $ (DEC) $Date: 1999/11/08 13:25:09 $";
#endif

#include "../shared/backup.h"
#include "../shared/util.h"
#include "dirmgt.h"
#include "inotbl.h"
#include "vrestore_msg.h"

extern nl_catd catd;

extern int Screen_width;        /* defined in vrestore.c */
extern int Quota_data_present;  /* defined in vrestore.c */
extern int Stdout_to_tty;       /* defined in vrestore.c */
extern FILE *Shell_tty;         /* defined in vrestore.c */
extern int Group_Quota_data_present; /* defined in vrestore.c */
extern int User_Quota_data_present;  /* defined in vrestore.c */

#define PROMPT Cwd_name
#define MAX_FILES 1024
#define MODIFIER_SZ 2
#define NAME_SZ (FILE_NAME_SIZE + MODIFIER_SZ)

typedef struct {
    char *name;
    char star;
    char slash;
    /*char _unused_bytes[1];*/
} file_desc_t;

/*
 * prototypes
 */
static void do_sh( char *arg );
static void do_chdir( int dir_fd, char *arg );
int file_name_compare( const void* name1, const void* name2 );
static void sort_and_print_files( int name_cnt, int max_name_sz, 
                                                       file_desc_t files[] );
static void ls( int dir_fd, char *path, int list_all );
static char *get_file_from_list( char *cp, char *buf );
static void do_ls ( int dir_fd, char *arg, int list_all );
static void parse_arg_and_do_fcn( int dir_fd, char *arg, int (*fcn)() );
void do_help( char *arg );
int interactive_shell( int dir_fd, int *verbose );


static void
do_sh( char *arg )
{
    if (arg == NULL) {
        return;
    }

    system( arg );
}

static void
do_chdir( int dir_fd, char *arg )
{
    int ret;
    char buf[MAXPATHLEN];      /* file name to process      */

    (void) get_file_from_list(arg, buf);
    ret = dir_change_cwd( dir_fd, buf[0]=='\0' ? NULL : buf );
    if (ret != OKAY) {
        fprintf( stderr, catgets(catd, S_SHELL1, SHELL1, "directory <%s> not found\n"), arg );
    }
}

int 
file_name_compare(const void* name1 ,const void* name2 )
{
    const file_desc_t *n1 = name1, *n2 = name2;

    return strcmp( n1->name, n2->name ); 
}

static void
sort_and_print_files(
    int name_cnt,
    int max_name_sz,
    file_desc_t files[]
    )
{
    char printf_str[10], *cp;
    int i, cols, c, col_width;

    if (name_cnt <= 0) {
        return;
    }

    qsort( files, name_cnt, sizeof( file_desc_t ), file_name_compare );

    if (max_name_sz > Screen_width) {
        cols = 1;
    } else {
        cols = Screen_width / max_name_sz;
    }

    col_width = MAX( 10, Screen_width / cols );

    sprintf( printf_str, "%%-%ds", col_width );

    i = 0;

    while (i < name_cnt) {

        c = 0;

        while ((i < name_cnt) && (c < cols)) {
            files[i].name[0] = files[i].star;
            /*
             * display non-printable characters in filename as a '?'
             */
            if (Stdout_to_tty) {
                for (cp = files[i].name; *cp; cp++) {
                    if (!isprint(*cp))
                        *cp = '?';
                }
            }
            strncat( files[i].name, &files[i].slash, 1 );

            fprintf( stderr, printf_str, files[i].name );

            free( files[i].name );
            files[i].name = NULL;
            i++;
            c++;
        }

        fprintf( stderr, "\n" );
    }
}

static void
ls( int dir_fd, char *path, int list_all )
{
    int i, num_files, status, max_name_sz = 0;
    struct dirent *dir_entry = NULL;
    dir_desc_t dd;
    ino_tbl_entry_t *dirs_ent = NULL, *ent = NULL;
    char *name;
    file_desc_t *files;
    ino_t ino;

    if (path == NULL) {
        path = ".";
    }

    fprintf( stderr, "\n%s:\n", path );

    status = dir_open( dir_fd, path, &dd, &ino );
    if (status == OKAY){
	num_files = 0;

	/*
	 * Count the number of directory entries, skip . and ..
	 */
	while ((dir_entry = dir_read( &dd )) != NULL) {
	    if (strcmp( dir_entry->d_name, ".") == 0) {
		continue;
	    }
	    if (strcmp( dir_entry->d_name, "..") == 0) {
		continue;
	    }
	    num_files++;
	}

        /*
         * Count quota files.
         */
        if (User_Quota_data_present) 
            num_files += 1;

        if (Group_Quota_data_present) 
            num_files += 1;

	/*
	 * Malloc space for all files.
	 */
	if (num_files != 0) {
	    files = malloc( sizeof(file_desc_t) * num_files);
                
	    if (files == NULL) {
	      fprintf(stderr, catgets(catd, S_SHELL1, SHELL31, "out of memory; can't list files\n"));

	      free(files);
	      dir_close( &dd );
	      return; /* give up; no more memory */
	    }
        } else {
	    dir_close( &dd );
	    return;  /* No files so just return */
	}
       
	dir_close( &dd );
	status = dir_open( dir_fd, path, &dd, &ino );
    }
    else
    {
        /* 'path' is not a directory.  See if it is a file we know about */

        ino_t parent_ino, ino;

        ino = dir_lookup( dir_fd, path, &parent_ino );

        if (ino == 0) {
            fprintf( stderr, catgets(catd, S_SHELL1, SHELL2, "file or directory <%s> does not exist\n"), path );
	    dir_close( &dd );  /* Added 8/18/95 */
            return;
        }

        /* 'path' is a file we know about so list it */

        if (list_all || dir_restore_me( ino, parent_ino )) {
            if (dir_restore_me( ino, parent_ino )) {
                fprintf( stderr, "*" );
            } else {
                fprintf( stderr, " " );
            }

            name = get_name( path );
        }
	dir_close( &dd );  /* Added 8/18/95 */
        return; /* we're all done */
    }
   
    /*
     * If quota files are in the saveset, fill in their
     * entries in the files array manually, using slots 0 and 1.
     */
    i = 0;
    if (User_Quota_data_present && ino == Root_ino) {
        int restore_uquota;

        restore_uquota = dir_restore_me(USR_QUOTA_INO, Root_ino);

        if (list_all || restore_uquota) {
            files[i].name = malloc(strlen(USR_QUOTA_FILE) + MODIFIER_SZ + 1);
            if (files[i].name == NULL) {
                fprintf(stderr, catgets(catd, S_SHELL1, SHELL4, "out of memory; can't list all files\n"));
                free(files);
                return;
            }
            files[i].name[0] = ' ';
            files[i].name[1] = '\0';
            strcat(files[i].name, USR_QUOTA_FILE);
            max_name_sz = MAX( max_name_sz, strlen(USR_QUOTA_FILE) 
                                            + MODIFIER_SZ + 1 );
            files[i].slash = ' ';
            if (restore_uquota)
                files[i].star = '*';
            else
                files[i].star = ' ';
            i++;
        }
    }

    if (Group_Quota_data_present && ino == Root_ino) {
        int restore_gquota;

        restore_gquota = dir_restore_me(GRP_QUOTA_INO, Root_ino);

        if (list_all || restore_gquota) {
            files[i].name = malloc(strlen(GRP_QUOTA_FILE) + MODIFIER_SZ + 1);
            if (files[i].name == NULL) {
                fprintf(stderr, catgets(catd, S_SHELL1, SHELL4, "out of memory; can't list all files\n"));
                free(files);
                return;
            }
            files[i].name[0] = ' ';
            files[i].name[1] = '\0';
            strcat(files[i].name, GRP_QUOTA_FILE);
            max_name_sz = MAX( max_name_sz, strlen(GRP_QUOTA_FILE) 
                                            + MODIFIER_SZ + 1 );
            files[i].slash = ' ';
            if (restore_gquota)
                files[i].star = '*';
            else
                files[i].star = ' ';
            i++;
        }
    }

    /* 'path' is a valid dir so list the dir's files */

    while ((dir_entry = dir_read( &dd )) != NULL) {

        if (strcmp( dir_entry->d_name, ".") == 0) {
            dirs_ent = ino_tbl_lookup( dir_entry->d_ino );
            continue;
        }

        if (strcmp( dir_entry->d_name, "..") == 0) {
            continue;
        }

        ent = ino_tbl_lookup( dir_entry->d_ino );

        if (list_all || ino_tbl_restore_me( ent, dirs_ent )) {
            /* 
             * Allocate space for the file name.  Note that this routine
             * doesn't deallocate the space.  This is done by 
             * sort_and_print_files().
             */
            files[i].name = malloc( strlen( dir_entry->d_name ) 
                                    + MODIFIER_SZ + 1 );
            files[i].star = ' ';
            files[i].slash = ' ';
    
            if (files[i].name == NULL) {
    
                sort_and_print_files( i, max_name_sz, files );
                i = 0;
    
                files[i].name = malloc( strlen( dir_entry->d_name ) 
                                        + MODIFIER_SZ + 1 );

                if (files[i].name == NULL) {
                    fprintf(stderr, catgets(catd, S_SHELL1, SHELL4, "out of memory; can't list all files\n"));

		    free(files);
		    dir_close( &dd );   /* Added 8/18/95 */
                    return; /* give up; no more memory */
                }
            }
    
            files[i].name[0] = ' ';
            files[i].name[1] = '\0';
            strcat( files[i].name, dir_entry->d_name );
            max_name_sz = MAX( max_name_sz, strlen( dir_entry->d_name ) 
                                            + MODIFIER_SZ + 1 );
    
    
            if ((ent != NULL) && (ent->type == DIR_ENT)) {
                files[i].slash = '/';
            } 
    
            if (ino_tbl_restore_me( ent, dirs_ent )) {
                files[i].star = '*';
            }
    
            i++;
        }
    }

    sort_and_print_files( i, max_name_sz, files );

    free( files );
    dir_close( &dd );
}

/*
 * get_file_from_list
 * 
 * Get a file name from the arg list pointed to be 'cp' and copy it
 * to 'buf'.  Handles the following special characters: " ' \
 *
 * Returns address at beginning of next file arg (i.e., skips trailing
 * white space) where left off in 'buf' or NULL if nothing's left.  
 */
static char *
get_file_from_list(char *cp, char *buf)
{
        char delimit_char;    /* delimiter in string   */
        char *tp = buf;       /* temp pointer into buf */

        if (cp != NULL) {
            /*
             * skip white space
             */
            while (*cp == ' ' || *cp =='\t')
                cp++;
            /*
             * determine delimiter and scan to it or the end of the string
             */
            if (*cp == '"')
                delimit_char = *cp++;
            else
                delimit_char = ' ';
            while (*cp != '\0') {
                /*
                 * check if need to skip escape (backslash) character
                 */
                if (*cp == '\\' && *(cp+1) != '\0')
                    cp++;
                *tp++ = *cp++;
                if (*cp == delimit_char) {
                    cp++;
                    while (*cp == ' ' || *cp =='\t')
                        cp++;
                    break;
                }
            }
            if (*cp == '\0')
                cp = NULL;
        }
        *tp = '\0';
        return(cp);
}

static void
do_ls ( int dir_fd, char *arg, int list_all )
{
    char *cp = arg;            /* current pointer into arg  */
    char buf[MAXPATHLEN];      /* file name to process      */

    do {
        cp = get_file_from_list(cp, buf);
        (void) ls( dir_fd, buf[0]=='\0' ? NULL : buf, list_all );
    } while (cp != NULL);
}

static void
parse_arg_and_do_fcn( int dir_fd, char *arg, int (*fcn)() )
{
    char *cp = arg;            /* current pointer into arg  */
    char buf[MAXPATHLEN];      /* file name to process      */

    /*
     * loop while there's more files to process
     */
    while (cp != NULL) {
        cp = get_file_from_list(cp, buf);
        if (buf[0] != '\0')
            (void) (*fcn)(dir_fd, buf);
    }
}

void
do_help( char *arg )
{
    fprintf( stderr, "\n" );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL6, "Options:\n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL7, "   ? | h | help \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL8, "\tDisplay list of interactive commands. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL9, "   ls [<arg>] \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL10, "\tLists all files in the save-set device or file.\n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL11, "   cd | chdir [<arg>] \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL12, "\tChange to the directory specified in arg.\n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL13, "   add <arg> \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL14, "\tAdd a file or directory to the set of files or \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL15, "\tdirectories to restore. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL16, "   verbose | ver \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL17, "\tSpecifies which files have been added for reading. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL18, "   review | rev [<arg>] \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL19, "\tLists current directory or file specified. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL20, "   delete | del | rem | remove <arg> \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL21, "\tRemove a file or directory from the set of files or\n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL15, "\tdirectories to restore. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL22, "   pwd \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL23, "\tPrint the current working directory in the save-set. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL24, "   sh <arg> \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL25, "\tPerform system command outside of shell. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL26, "   extract | restore \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL27, "\tExit the interactive shell and restore files. \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL28, "   exit | e | quit | q \n") );
    fprintf( stderr, catgets(catd, S_SHELL1, SHELL29, "\tExit the interactive shell without restoring any files. \n") );
    fprintf( stderr, "\n" );

} /* end do_help */

int
interactive_shell(
    int dir_fd,
    int *verbose
    )
{
    char s[1024];
    char *sp;
    char *cmd;
    char *arg;
    int done = 0;
    int s_len;
    int cmd_len;
    int abort_restore = FALSE;
    
    fprintf( stderr, "(%s) ", PROMPT );

    while (!done && ((sp = fgets(s, 1024, Shell_tty)) != NULL)) {

        /*
         * skip any white space before cmd
         */
        while (*sp == ' ' || *sp =='\t')
            sp++;
        s_len = strlen( sp );

        /*
         * The code below depends on an absence of a newline
         * character at the end of the string.  If it's there,
         * replace it with a null character.
         */
        if (sp[s_len-1] == '\n') {
            sp[s_len-1] = '\0';
            s_len--;
        }

        if (s_len > 0 && (cmd = strtok( sp, " " )) != NULL) {

            cmd_len = strlen( cmd );

            if ((cmd_len + 1) < s_len) {
                arg = sp + cmd_len + 1;
            } else {
                arg = NULL;
            }
 
            if (strcmp( "ls", cmd ) == 0) {
                do_ls( dir_fd, arg, TRUE );

            } else if ((strcmp( "chdir", cmd ) == 0) ||
                       (strcmp( "cd", cmd ) == 0)) {
                do_chdir( dir_fd, arg );

            } else if (strcmp( "add", cmd ) == 0) {
                parse_arg_and_do_fcn( dir_fd, arg, dir_add_to_restore );

            } else if ((strcmp( "verbose", cmd ) == 0) ||
                       (strcmp( "ver", cmd ) == 0)) {
                *verbose ^= 1;

            } else if ((strcmp( "review", cmd ) == 0) ||
                       (strcmp( "rev", cmd ) == 0)) {
                do_ls( dir_fd, arg, FALSE );

            } else if ((strcmp( "delete", cmd ) == 0) ||
                       (strcmp( "del", cmd ) == 0) ||
                       (strcmp( "rem", cmd ) == 0) ||
                       (strcmp( "remove", cmd ) == 0)) {
                parse_arg_and_do_fcn( dir_fd, arg, dir_del_from_restore );

            } else if (strcmp( "pwd", cmd ) == 0) {
                fprintf( stderr, "%s\n", Cwd_name );

            } else if (strcmp( "sh", cmd ) == 0) {
                do_sh( arg );

            } else if ((strcmp( "help", cmd ) == 0) ||
                       (strcmp( "h", cmd ) == 0) ||
                       (strcmp( "?", cmd ) == 0)) {
                do_help( arg );

            } else if ((strcmp( "extract", cmd ) == 0) ||
                       (strcmp( "restore", cmd ) == 0)) {
                done = 1;

            } else if ((strcmp( "exit", cmd ) == 0) ||
                       (strcmp( "e", cmd ) == 0) ||
                       (strcmp( "quit", cmd ) == 0) ||
                       (strcmp( "q", cmd ) == 0)) {
                abort_restore = TRUE;
                done = 1;

            } else {
                fprintf( stderr, catgets(catd, S_SHELL1, SHELL30, "unknown command\n") );
            }
        }

        fprintf( stderr, "\n" );

        if (!done) {
            fprintf( stderr, "(%s) ", PROMPT );
        }
    }

    return !abort_restore;
}

/* end shell.c */

