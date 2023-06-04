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
 *      Advanced File System (AdvFS)
 *
 * Abstract:
 *
 *      Defines the "AdvFS Create File-set" utility.
 *
 * Date:
 *
 *      Wed Sep 18 13:13:54 1991
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: mkfset.c,v $ $Revision: 1.1.11.3 $ (DEC) $Date: 2001/01/26 14:58:58 $";
#endif

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <grp.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <msfs/bs_error.h>
#include <msfs/ms_public.h>
#include <msfs/bs_ods.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "mkfset_msg.h"

#define TRUE         1
#define FALSE        0
#define ERRMSG( e )  sys_errlist[e]
#define QUOTA_GROUP  catgets(catd, 1, 1, "operator")

static  char        *Prog;
static  char        *fileset;
static  char        *domain;
static  mlBfSetIdT   BfSetId;

        nl_catd      catd;

extern  int          errno;
extern  char        *sys_errlist[];

/*
 *  Local Function Prototypes
 */
 
gid_t
quota_gid(void);

void
add_options(char **old, char *new);

int
set_options (char *options, u32T *fsetOptions);

void
usage( void )
{
    printf( catgets(catd, 1, USAGE,
            "usage: %s [-o arg[,arg] domain fileset\n"), Prog);
    printf( catgets(catd, 1, USAGE2,
            "\t[-o arg[,arg]    - specify fileset options, valid arguments are:\n") );
    printf( catgets(catd, 1, USAGE3,
            "\t\t\t   frag and nofrag.\n") );
}

/*
 *  Start
 */
 
main ( int argc, char *argv[] )
{
    extern int         optind;
    extern char       *optarg;

           int         err;
           int         lkHndl;
           int         dirLocked = 0;
           int         error = 0;
           int         c;
           int         thisDmnLock;
           char       *p;
           char       *cp;
           char       *pp;
           char        dmnPathName[MAXPATHLEN+1]; 
           int         o = 0;
           u32T        fsetOptions = 0;
           char        *options = NULL;

    struct stat        stats;
                     
    mlServiceClassT    reqServices = 0;
    mlServiceClassT    optServices = 0;
    mlStatusT          sts;

    /*------------------------------------------------------------------------*/

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_MKFSET, NL_CAT_LOCALE);

    /*
     *  don't check for valid PAK license because in /sbin
     *
     *  store only the file name part of argv[0]
     */
     
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
    
    /*
     *  check for root
     */
     
    if (geteuid()) {
        fprintf(stderr, catgets(catd,1,20,
				"\nPermission denied - user must be root to run %s.\n\n"),
                                argv[0]);
        usage();
        exit(1);
    }
 
    /*
     *  Get user-specified command line arguments.
     */
     
    while ((c = getopt( argc, argv, "o:" )) != EOF) {
        switch (c) {
            case 'o':
                o++;
                add_options(&options, optarg);
                break;
                
            case '?':
            default:
                usage();
                exit( 1 );
        }
    }

    /*
     *  check for missing required args 
     */
     
    if (optind != (argc - 2)) {
        usage();
        exit( 1 );
    }

    /*
     *  get domain and fileset names
     */
    domain = argv[optind];
    fileset = argv[optind + 1];

    if (strlen( fileset ) == 0) {
        fprintf( stderr, 
                 catgets(catd, 1, 21, "%s: no fileset name provided.\n"),
		 Prog);
        goto _error;
    }

    if (strlen( fileset ) >= ML_SET_NAME_SZ) {
        fprintf( stderr, 
                 catgets(catd, 1, 8, "%s: fileset name '%s' exceeds limit of %d characters\n"),
                 Prog, fileset, ML_SET_NAME_SZ - 1 );
        goto _error;
    }

    if (cp = strpbrk( fileset, "/# :*?\t\n\f\r\v" )) {
        fprintf( stderr, catgets(catd, 1, 9, "%s: invalid character '%c' in fileset name '%s'\n"),
                 Prog, *cp, fileset );
        goto _error;
    }

    /*
     *  If -o options were specified, parse them and set the
     *  appropriate bits in fsetOptions
     */

    if (o) {
    
        if (set_options(options, &fsetOptions) == FALSE) {
            goto _error;
        }
    }

    
    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 10, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        goto _error;
    }

    dirLocked = 1;

    /*
     * Verify that the domain exists and its pathname is in the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, domain);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
        if( errno == ENOENT ) {
            fprintf(stderr, catgets(catd, 1, 11, "%s: domain directory '%s' does not exist\n"),
                     Prog, dmnPathName);
        } else {
            fprintf(stderr, catgets(catd, 1, 12, "%s: error getting '%s' stats; [%d] %s\n"),
                     Prog, dmnPathName, errno, ERRMSG (errno));
        }
        goto _error;
    }

    /*
     * Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnPathName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr,
                 catgets(catd, 1, 13, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 14, "%s: error locking '%s'.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 15, "%s: error = [%d] %s\n"),
                     Prog, err, ERRMSG(err));
        }
        goto _error;
    }

    sts = msfs_fset_create( domain, 
                            fileset, 
                            reqServices, 
                            optServices, 
                            fsetOptions,
                            quota_gid(),
                            &BfSetId);
 
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 16, "%s: can't create fileset '%s' in domain '%s'\n"),
                 Prog, fileset, domain );
        fprintf( stderr, catgets(catd, 1, 17, "%s: error = %s\n"), Prog, BSERRMSG( sts ) );
        goto _error;
    }

    if (unlock_file( lkHndl, &err) == -1) {
        fprintf( stderr, catgets(catd, 1, 18, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        dirLocked = 0;
        goto _error;
    }

    exit( 0 );

_error:

    if (dirLocked) {
        if (unlock_file( lkHndl, &err) == -1) {
            fprintf( stderr, catgets(catd, 1, 18, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        }
    }

    exit( 1 );
}

/*
 * Set the group ID of the quota files to group "operator", if that
 * group exists.
 */
gid_t
quota_gid()
{
    struct group *gr;

    setgrent();
    gr = getgrnam(QUOTA_GROUP);
    endgrent();
 
    if (gr == NULL) {
        fprintf(stderr, 
                catgets(catd, 1, 19, "No group \"operator\", using gid 0 for quota files\n"));
        return (gid_t) 0;
    }
    return gr->gr_gid;
}

/*
 *  Get (or add) options to the option string
 *
 *  This routines takes a pointer to a string of
 *  options (*new) and copies (or appends) the string
 *  to a save area (**old).
 * 
 */

void
add_options(char **old, char *new)
{
    char *p;

    /*
     *  If we already have some options saved away,
     *    Allocate new memory for the combined option strings
     *    Combine the option strings into the save area
     *    Free the old memory
     *    Store the pointer to the new option save area
     */
     
    if (*old) {
        p = (char *) malloc(strlen(*old)+strlen(new)+2);
        if (!p) {
            fprintf( stderr, catgets(catd, 1, 22, "%s: insufficient memory.\n"), Prog );
            exit(1);
        }
        sprintf(p, "%s,%s", *old, new);
        free(*old);
        *old = p;

    /*
     *  Else, no options are already saved,
     *    Allocated memory for the option string
     *    Copy the option string to the save area
     *    Store the pointer to the option save area
     */
     
    } else {
        p = (char *) malloc(strlen(new)+1);
        if (!p) {
            fprintf( stderr, catgets(catd, 1, 22, "%s: insufficient memory.\n"), Prog );
            exit(1);
        }
        strcpy(p, new);
        *old = p;
    }
}

/*
 *  Set the fileset options flag based on the input flags
 *
 *  This routine takes the string of options provided by the user
 *  and sets the appropriate fileset flags.  The options are
 *  processed in the order they were input on the command line
 *  and the last specified option will override previous versions
 *  of itself.
 *
 *  set_options returns TRUE if no problems are encountered, or
 *  FALSE if an invalid option argument is found.
 *
 */
int
set_options (char *options, u32T *fsetOptions)
{
    char *optptr;
    char *optbuf;

    /*
     *  Allocate memory to hold a working copy of the option string
     */    
    optbuf = (char *) malloc(strlen(options)+1);    
    strcpy(optbuf, options);

    /*
     *  Process the "," delimited options one at a time.
     */
        
    for (optptr = strtok(optbuf, ","); optptr; optptr = strtok((char *)NULL, ",")) {
    
        if (!strcmp(optptr, "nofrag")) {
            *fsetOptions |= BFS_OD_NOFRAG;
            
        } else if (!strcmp(optptr, "frag")) {
            *fsetOptions &= ~BFS_OD_NOFRAG;

        /*
         *  invalid option
         */
                         
        } else {
            fprintf( stderr, catgets(catd, 1, 23, "%s: error, unknown -o argument '%s'.\n"),
                                     Prog, optptr );
            free(optbuf);
            return FALSE;             
        }
    }
    free(optbuf);
    return TRUE;
}

