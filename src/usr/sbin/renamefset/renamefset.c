static char* rcsid = "renamefset.c - $Date: 2002/01/11 20:44:57 $";
/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
 *                                                                          *
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
 *      Defines the "MegaSafe Rename File-set" utility.
 *
 * Date:
 *
 *      Wed Jan 19 13:13:54 1994
 *
 */
/*
 * HISTORY
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/errno.h>

#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>

#include <locale.h>
#include "renamefset_msg.h"
nl_catd catd;

extern int errno;
extern char *sys_errlist[];

#define TRUE 1
#define FALSE 0
#define ERRMSG( e ) sys_errlist[e]

static char *Prog = "renamefset";
static char *NewSetName = NULL;
static char *OrigSetName = NULL;
static char *DmnTbl = NULL;

void
usage( void )
{
    printf(catgets(catd, 1, USAGE, "usage: %s domain origSetName NewSetName\n"), Prog);
}

main ( int argc, char *argv[] )
{
    int err, lkHndl, dirLocked = 0, error = 0;
    int thisDmnLock;
    mlStatusT sts;
    mlBfSetIdT bfSetId;
    char *p;
    char dmnName[MAXPATHLEN+1];
    struct stat stats;

    /*-----------------------------------------------------------------------*/

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_RENAMEFSET, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
   /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,11,
				"\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }
 
    /* move past program name */
    argv++; 
    argc--;
 
    if (argc < 3) {
        usage();
        goto _error;
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    if (error) {
        goto _error;
    } else if (argc > 3) {
        fprintf( stderr, catgets(catd, 1, 2, "%s: bad arg <%s>\n"), Prog, *argv );
        goto _error;
    } else if ((argc < 3) || ((*argv)[0] == '-')) {
        usage();
        goto _error;
    }
 
    DmnTbl = *argv;
    argv++;
    OrigSetName = *argv;
    argv++;
    NewSetName = *argv;

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 3, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        goto _error;
    }

    dirLocked = 1;

    /*
     * Make sure the domain exists.
     */

    strcpy( dmnName, MSFS_DMN_DIR );
    strcat( dmnName, "/" );
    strcat( dmnName, DmnTbl );

    if (lstat( dmnName, &stats )) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: domain '%s' doesn't exist\n"), Prog, dmnName );
        goto _error;
    }

    /*
     * Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr,
                     catgets(catd, 1, 5, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 6, "%s: error locking '%s'.\n"),
                     Prog, dmnName);
            fprintf (stderr, catgets(catd, 1, 7, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRMSG(err));
        }
        goto _error;
    }

    sts = msfs_fset_rename( DmnTbl, OrigSetName, NewSetName );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 8, "%s: can't rename file set '%s' to set '%s' in domain '%s'\n"),
                 Prog, OrigSetName, NewSetName, DmnTbl );
        fprintf( stderr, catgets(catd, 1, 9, "%s: error = %s\n"), Prog, BSERRMSG( sts ) );
        goto _error;
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 10, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        dirLocked = 0;
        goto _error;
    }

    return 0;

_error:

    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 10, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        }
    }

    exit( 1 );
}

/* end renamefset.c */
