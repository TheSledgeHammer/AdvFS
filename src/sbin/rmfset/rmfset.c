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
 *      Defines the "MegaSafe Remove File-set" utility.
 *
 * Date:
 *
 *      Tue Apr 14, 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: rmfset.c,v $ $Revision: 1.1.8.5 $ (DEC) $Date: 1999/03/29 14:47:43 $";
#endif

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/file.h>
#include <sys/errno.h>

#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "rmfset_msg.h"
#include <msfs/advfs_evm.h>

nl_catd catd;
extern int errno;
extern char *sys_errlist[];

#define TRUE 1
#define FALSE 0
#define ERRMSG( e ) sys_errlist[e]

static char *Prog = "rmfset";
static char *SetName = NULL;
static char *DmnTbl = NULL;

static advfs_ev advfs_event;
static thisDmnLocked=0;	/* global in case need it outside main */


void
usage( void )
{
    printf(catgets(catd, 1, USAGE, "usage: %s [-f] domain set \n"), Prog);
}

main ( int argc, char *argv[] )
{
    mlStatusT sts;
    int error = 0, err, lkHndl, dirLocked = 0;
    int thisDmnLock;
    int goforit =0, c;
    char *p;
    char dmnPathName[MAXPATHLEN+1];
    struct stat stats;
    extern int optind;
    extern char *optarg;
    char response[20];
    FILE *Local_tty = stdin;

    /*----------------------------------------------------------------------*/

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_RMFSET, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,12,
				"\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }
 
    /*
     ** Get user-specified command line arguments.
     */
    while ((c = getopt( argc, argv, "f" )) != EOF) {
        switch (c) {
            case 'f':
                goforit++;
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if (goforit > 1) {
        usage();
        exit( 1 );
    }

    if ((argc - 2 - optind) != 0) {
        /* missing required args or extra elements */
        usage();
        exit( 1 );
    }

    DmnTbl  = argv[optind++];
    SetName = argv[optind];

    init_event(&advfs_event);
    advfs_event.domain = DmnTbl;
    advfs_event.fileset = SetName;

    /*
     * Make sure the user REALLY means it
     */
    if (!goforit) {
    	fprintf( stderr, catgets(catd, 1, 11, "%s: remove fileset %s? [y/n] "), Prog, SetName);
    	fgets (response, 2, Local_tty);
    	if (strcmp(response, "y") != 0) {
	    goto _error;
    	}
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 2, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        goto _error;
    }

    dirLocked = 1;

    /*
     * Verify that the domain exists and its pathname is in the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, DmnTbl);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
        if( errno == ENOENT ) {
            fprintf(stderr, catgets(catd, 1, 3, "%s: Domain directory '%s' does not exist\n"),
                     Prog, dmnPathName);
        } else {
            fprintf(stderr, catgets(catd, 1, 4, "%s: error getting '%s' stats; [%d] %s\n"),
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
                 catgets(catd, 1, 5, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 6, "%s: error locking '%s'.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 7, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRMSG(err));
        }
        goto _error;
    } else {
        thisDmnLocked = 1;
        advfs_post_user_event(EVENT_FSET_RM_LOCK, advfs_event, Prog);
    }

    /* mark as unlocked now so unlock_file will not be retried if we eventually get
     * to _error.
     */
    dirLocked = 0;			
    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 8, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        goto _error;
    }

    sts = msfs_fset_delete( DmnTbl, SetName );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 9, "%s: can't delete file set '%s' from domain '%s'\n"),
                 Prog, SetName, DmnTbl );
        fprintf( stderr, catgets(catd, 1, 10, "%s: error = %s\n"), Prog, BSERRMSG( sts ) );
        goto _error;
    }

    advfs_post_user_event(EVENT_FSET_RM_UNLOCK, advfs_event, Prog);
    return 0;

_error:

    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 8, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        }
    }

    if (thisDmnLocked) {
        advfs_post_user_event(EVENT_FSET_RM_ERROR, advfs_event, Prog);
    }
    exit( 1 );
}
