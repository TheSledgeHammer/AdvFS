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
 *      AdvFs Storage System
 *
 * Abstract:
 *
 *      Defines the "AdvFS Show filesets" utility.
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
static char rcsid[] = "@(#)$RCSfile: showfsets.c,v $ $Revision: 1.1.13.5 $ (DEC) $Date: 2001/01/26 14:59:02 $";
#endif

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <pwd.h>
#include <grp.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/errno.h>

#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "showfsets_msg.h"
#include <msfs/bs_ods.h>


nl_catd catd;
extern int errno;
extern char *sys_errlist[];

#define TRUE 1
#define FALSE 0
#define ERRMSG( e ) sys_errlist[e]

static char *Prog = "showfsets";

#define MODE_STR_SZ 10

/*
 *  Local prototypes
 */

int
show_sets(
    char *dmnName,                /* in */
    int   brief,                  /* in */
    int   k,                      /* in */
    int   quota,                  /* in */
    char *setName);               /* in */

void
timeprt( time_t seconds );

void
usage( void )
{
    printf(catgets(catd, 1, USAGE,
           "usage: %s [-b | -q] [-k] domain [fileset ...], where options are:\n"), Prog);
    printf(catgets(catd, 1, USAGE2,
           "\t[-b]\t - Lists the names of the filesets, without additional detail\n"));
    printf(catgets(catd, 1, USAGE3,
           "\t[-q]\t - Displays the quota limits for filesets in a domain\n"));
    printf( catgets(catd, 1, USAGE4,
           "\t[-k]\t - Displays block information in 1k units instead of 512\n"));
}

/*
 * Start
 */
 
main( int argc, char *argv[] )
{
    extern int      gettimeofday( struct timeval *, struct timezone * );
    extern int      optind;
    extern char    *optarg;

    int             error = 0;
    int             b = 0, k = 0, q = 0;
    int             c, ii;
    char           *p;
    
    static char    *dmnName;
    
    struct timeval  time;
    struct timezone tz;
    
    /*------------------------------------------------------------------------*/

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_SHOWFSETS, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "bkq" )) != EOF) {
        switch (c) {
            case 'b':
                b++;
                break;

            case 'k':
                k++;
                break;

            case 'q':
                q++;
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if (b > 1 || k > 1 || q > 1) {
        usage();
        exit( 1 );
    }

    if (b && q) {
        usage();
        exit( 1 );
    }

    if ((argc - 1 - optind) < 0) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    dmnName = argv[optind];

    if ((optind + 1) >= argc) {

        if( show_sets( dmnName, b, k, q, NULL ) <= 0 ) {
            /* error or no sets in the domain */
            goto _error;
        }

    } else {

        char *setName;

        for (ii = optind + 1; ii < argc; ii++) {
            setName = argv[ii];

            if (show_sets( dmnName, b, k, q, setName ) <= 0) {
                /* error or no sets in the domain */
                goto _error;
            }
        }
    }

    return 0;

_error:

    exit( 1 );
}


void
timeprt( time_t seconds )
{
        time_t hours, minutes;
        static time_t now;

        if (now == 0)
                time(&now);

        if (now > seconds) {
                printf (catgets(catd, 1, 1, "   none"));
                return;
        }

        seconds -= now;
        minutes = (seconds + 30) / 60;
        hours = (minutes + 30) / 60;

        if (hours >= 36) {
                printf( catgets(catd, 1, 2, "%3ddays"), (hours + 12) / 24);
                return;
        }

        if (minutes >= 60) {
                printf( "  %2d:%02d", minutes / 60, minutes % 60);
                return;
        }

        printf( "%7d", minutes);
}

int
show_sets(
    char *dmnName,                /* in */
    int brief,                    /* in */
    int k,                        /* in */
    int quota,                    /* in */
    char *setName                 /* in */
    )
{
    mlStatusT sts;
    int numSets = 0, ret, err, lkHndl, dirLocked = 0;
    u32T setIdx, altSetIdx;
    mlBfSetParamsT setParams, altSetParams;
    char mode_str[MODE_STR_SZ];
    struct passwd *pwd;
    struct group *grp;
    u32T userId, cloneUserId;
    int setFound = FALSE;

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_SH, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 3, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        goto _error;
    }

    if (!brief && quota) {
        printf(catgets(catd, 1, 4,
               "                         Block (%3s) limits               File limits\n"),
               k ? catgets(catd, 1, 5, "1k") : catgets(catd, 1, 6, "512") );
        
        printf(catgets(catd, 1, 7,
               "Fileset         BF    used    soft    hard  grace    used  soft  hard  grace\n"));
    }

    dirLocked = 1;

    setIdx = 0; /* start with first set */

    /*
     * Scan the sets, get their params, and display the params.
     */

    do {
        /*
         * This routine gets the set params for the next set.
         * When 'setIdx' is zero it means to start with the
         * first set.  Each time the routine is called 'setIdx'
         * is updated to 'point' to the next set in the domain (the
         * update is done by msfs_fset_get_info()).
         */

        sts = msfs_fset_get_info( dmnName, &setIdx, &setParams, &userId, 0 );
        if (sts == EOK) {
            numSets++;

            if (setName != NULL) { 
                if (strcmp( setName, setParams.setName )) {
                    continue;
                } else {
                    setFound = TRUE;
                } 
            }

            if (brief || !quota) {
                printf( "%s\n", setParams.setName );
            }

            if (!brief && !quota) {
                printf( catgets(catd, 1, 8, "\tId           : %08x.%08x.%x.%04x\n"), 
                        setParams.bfSetId.domainId.tv_sec,
                        setParams.bfSetId.domainId.tv_usec,
                        setParams.bfSetId.dirTag.num,
                        setParams.bfSetId.dirTag.seq );


                if (setParams.numClones != 0) {
                    altSetIdx = setParams.nextCloneSetTag.num;
                    sts = msfs_fset_get_info( dmnName,
                                              &altSetIdx, 
                                              &altSetParams,
                                              &cloneUserId,
                                              0 );
                    if (sts == EOK) {
                        printf( catgets(catd, 1, 14, "\tClone is     : %s\n"), 
                                altSetParams.setName);
                    }
                }

                if (setParams.cloneId != 0) {
                    altSetIdx = setParams.origSetTag.num;
                    sts = msfs_fset_get_info( dmnName, 
                                              &altSetIdx, 
                                              &altSetParams,
                                              &cloneUserId,
                                              0 );
                    if (sts == EOK) {
                        printf( catgets(catd, 1, 15, "\tClone of     : %s\n"), 
                                altSetParams.setName );
                    }
                    printf( catgets(catd, 1, 16, "\tRevision     : %d\n"), setParams.cloneId ); 
                }

                if (userId != 0) {
                    printf( catgets(catd, 1, 17, "\tUser ID      : 0x%08x\n"), userId );
                }

                if (setParams.cloneId == 0) {
                    printf( catgets(catd, 1, 18, "\tFiles        : %8ld,"), setParams.filesUsed );
                    printf( catgets(catd, 1, 19, "  SLim= %8ld,"), setParams.fileSLimit );
                    printf( catgets(catd, 1, 20, "  HLim= %8ld"), setParams.fileHLimit );

                    if (setParams.fileSLimit && 
                        (setParams.filesUsed >= setParams.fileSLimit)) {
                        printf( catgets(catd, 1, 21, "  grc= ") );
                        timeprt( setParams.fileTLimit );
                    }
                    printf( "\n" );

                    printf( catgets(catd, 1, 22, "\tBlocks %5s :"), 
                            k ? catgets(catd, 1, 23, "(1k)") : catgets(catd, 1, 24, "(512)") );
                    printf( " %8ld,", 
                            k ? setParams.blksUsed / 2 : setParams.blksUsed );
                    printf( catgets(catd, 1, 19, "  SLim= %8ld,"), 
                            k ? setParams.blkSLimit / 2 : setParams.blkSLimit );
                    printf( catgets(catd, 1, 20, "  HLim= %8ld"), 
                            k ? setParams.blkHLimit / 2 : setParams.blkHLimit );

                    if (setParams.blkSLimit && 
                        (setParams.blksUsed >= setParams.blkSLimit)) {
                        printf( catgets(catd, 1, 21, "  grc= ") );
                        timeprt( setParams.blkTLimit );
                    }
                    printf( "\n" );
    
                    printf( catgets(catd, 1, 34, "\tQuota Status : ") );

                    if (setParams.quotaStatus & Q_STS_USR_QUOTA_EN) {
                        printf( catgets(catd, 1, 35, "user=on  ") );
                    } else {
                        printf( catgets(catd, 1, 36, "user=off ") );
                    }

                    if (setParams.quotaStatus & Q_STS_GRP_QUOTA_EN) {
                        printf( catgets(catd, 1, 37, "group=on  ") );
                    } else {
                        printf( catgets(catd, 1, 38, "group=off ") );
                    }

                    printf( "\n" );

                }

                printf( catgets(catd, 1, OBJ1, "\tObject Safety: ") );
                if (setParams.bfSetFlags & BFS_OD_OBJ_SAFETY) {
                    printf( catgets(catd, 1, OBJ2, "on ") );
                }
                else {
                    printf( catgets(catd, 1, OBJ3, "off ") );
                }
                printf( "\n" );

                printf( catgets(catd, 1, FRG1, "\tFragging     : ") );
                if (setParams.bfSetFlags & BFS_OD_NOFRAG) {
                    printf( catgets(catd, 1, OBJ3, "off ") );
                }
                else {
                    printf( catgets(catd, 1, OBJ2, "on ") );
                }
                printf( "\n" );
            }

            if (!brief && quota && setParams.cloneId == 0) {

                printf( "%-16s", setParams.setName );

                if (setParams.blkHLimit &&
                    setParams.blksUsed >= setParams.blkHLimit) {
                    printf( "*" );

                } else if (setParams.blkSLimit &&
                    setParams.blksUsed >= setParams.blkSLimit) {
                    printf( "+" );
                } else {
                    printf( "-" );
                }

                if (setParams.fileHLimit &&
                    setParams.filesUsed >= setParams.fileHLimit) {
                    printf( "*" );

                } else if (setParams.fileSLimit &&
                    setParams.filesUsed >= setParams.fileSLimit) {
                    printf( "+" );

                } else {
                    printf( "-" );
                }

                printf( " %7ld", 
                        k ? setParams.blksUsed / 2 : setParams.blksUsed );
                printf( " %7ld", 
                        k ? setParams.blkSLimit / 2 : setParams.blkSLimit );
                printf( " %7ld", 
                        k ? setParams.blkHLimit / 2 : setParams.blkHLimit );

                if (setParams.blkSLimit && 
                    (setParams.blksUsed >= setParams.blkSLimit)) {
                    timeprt( setParams.blkTLimit );
                } else {
                    printf( "       " );
                }

                printf( "   %5ld", setParams.filesUsed );
                printf( " %5ld", setParams.fileSLimit );
                printf( " %5ld", setParams.fileHLimit );

                if (setParams.fileSLimit && 
                    (setParams.filesUsed >= setParams.fileSLimit)) {
                    timeprt( setParams.fileTLimit );
                }

                printf( "\n" );  

            }
        }
    } while (sts == EOK);

    if (sts != E_NO_MORE_SETS) {
        fprintf( stderr, catgets(catd, 1, 39, "%s: can't show fileset info for domain '%s'\n"),
                 Prog, dmnName );
        fprintf( stderr, catgets(catd, 1, 40, "%s: error = %s\n"), Prog, BSERRMSG( sts ) );
        goto _error;
    }

    if ((setName != NULL) && !setFound) {
        fprintf( stderr, catgets(catd, 1, 41, "%s: fileset '%s' does not exist in domain '%s'\n"),
                 Prog, setName, dmnName );
        goto _error;
    }

    if (numSets == 0) {
        fprintf( stderr, catgets(catd, 1, 42, "%s: domain '%s' has no filesets\n"), Prog, dmnName );
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 43, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        dirLocked = 0;
        goto _error;
    }

    return numSets;

_error:
 
    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 43, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        }
    }

    return -1;
}

/* end showfsets.c */
