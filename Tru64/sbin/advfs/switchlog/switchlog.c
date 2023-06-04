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
 * Facility:
 *
 *      ADVFS
 *
 * Date:
 *
 *      Wed Feb 24 09:22:09 1993
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: switchlog.c,v $ $Revision: 1.1.4.5 $ (DEC) $Date: 1999/03/29 14:47:35 $";
#endif

#include <stdio.h>
#include <strings.h>
#include <signal.h>
#include <unistd.h>
#include <sys/errno.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>

#include <locale.h>
#include "switchlog_msg.h"
nl_catd catd;

extern int errno;
extern char *sys_errlist[];

#define ERRMSG( e ) sys_errlist[e]

char *Prog = "switchlog";

static void
usage( void )
{
printf( 
catgets(catd, 1, USAGE, "usage: %s domain vol-idx\n"), 
Prog );
}

main ( int argc, char *argv[] )
{
    char *domain;
    char dmnName[MAXPATHLEN+1];
    u32T logPgs = 0;
    mlServiceClassT logSvc = 0;
    mlStatusT sts;
    int volIdx, err, c, l = 0;
    char *pp, *cp;
    extern int optind;
    extern char *optarg;
    struct stat stats;
    int dirLocked = 0, thisDmnLock = 0, lkHndl;


    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_SWITCHLOG, NL_CAT_LOCALE);
 /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,7,
				"\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }
    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "l:" )) != EOF) {
        switch (c) {
            case 'l':
                l++;
                logPgs = strtoul( optarg, &pp, 0 );
                break;

            case '?':

            default:
                usage();
                exit( 1 );
        }
    }

    if (l > 1) {
        usage();
        exit( 1 );
    }

    if (optind != (argc - 2)) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    domain = argv[optind];
    volIdx =  strtoul( argv[optind + 1], &pp, 0 );


    /*
     * Lock the domain while you do the switchlog
     */
    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf(stderr, catgets(catd, 1, 3, "%s: error locking '%s'; [%d] %s\n"),
                Prog, MSFS_DMN_DIR, err, BSERRMSG (err) );
        goto _error;
    }
    dirLocked = 1;

    strcpy( dmnName, MSFS_DMN_DIR );
    strcat( dmnName, "/" );
    strcat( dmnName, domain );
    
    if (lstat( dmnName, &stats )) {
        fprintf( stderr, catgets(catd, 1, 6, "%s: domain '%s' doesn't exist\n"),
		Prog, dmnName );
        goto _error;
    }

    /*
     * Lock this domain.
     */
    thisDmnLock = lock_file_nb (dmnName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, 1, 4, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf(stderr, catgets(catd, 1, 3, "%s: error locking '%s'; [%d] %s\n"),
                    Prog, domain, err, BSERRMSG (err) );
        }
        goto _error;
    }

    sts = advfs_switch_log( domain, volIdx, logPgs, logSvc );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 2, "%s: cannot switch log to a new volume; %s\n"),
                 Prog, BSERRMSG( sts ) );
	goto _error;
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf(stderr, catgets(catd, 1, 5,"%s: error unlocking '%s'; [%d] %s\n"),
                Prog, MSFS_DMN_DIR, err, BSERRMSG( err ) );
        dirLocked = 0;
        goto _error;
    }

    return 0;

_error:

    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf(stderr, catgets(catd, 1, 5,"%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, BSERRMSG( err ) );
        }
    }


    exit( 1 );
}
