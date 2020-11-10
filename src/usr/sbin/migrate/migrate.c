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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      migrate utility.
 *
 * Date:
 *
 *      Wed May 13 17:36:51 1992
 *
 * Revision History:
 *
 *      See end of file.
 */
/*
 * HISTORY
 */

#ifndef lint
static char rcsid[] = "@(#)$RCSfile: migrate.c,v $ $Revision: 1.1.7.1 $ (DEC) $Date: 2001/12/14 16:52:05 $";
#endif

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/file.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/errno.h>
#include <msfs/msfs_syscalls.h>
#include <msfs/vfast.h>

#ifdef _OSF_SOURCE
#include <sys/mount.h>
#endif

#include <msfs/bs_error.h>
#include <msfs/ms_public.h>
#include <msfs/msfs_syscalls.h>

#include <locale.h>
#include "migrate_msg.h"
nl_catd catd;

#ifdef _OSF_SOURCE
#define O_open open
#define O_close close
#else
#include <msfs/fs_syscalls.h>
#endif

char *Prog;

extern int errno;
extern char *sys_errlist[];

#define SYSERRMSG( e ) sys_errlist[e]

static
int
migrate (
         int fd,  /* in */
         unsigned long srcVolIndex,  /* in */
         unsigned long srcPageOffset,  /* in */
         unsigned long srcPageCnt,  /* in */
         unsigned long dstVolIndex,  /* in */
         unsigned long dstBlkOffset  /* in */
         );


void usage( void )
{
    fprintf( stderr, catgets(catd, 1, USAGE, "usage: %s [-p pageoffset] [-n pagecount] "), Prog);
    fprintf( stderr, catgets(catd, 1, 2, "[-s volumeindex] [-d volumeindex] filename\n"));
}

main( int argc, char *argv[] )

{
    unsigned long dstBlkOffset = -1;
    unsigned long dstVolIndex = -1;
    int p = 0, n = 0, s = 0, d = 0, c = 0, b = 0;
    int fd;
    char *path;
    char domain_name[MAXPATHLEN+1];
    char dmnName[MAXPATHLEN+1];
    unsigned long srcPageCnt = -1;
    unsigned long srcPageOffset = -1;
    unsigned long srcVolIndex = -1;
    mlStatusT sts;
    extern int optind;
    extern char *optarg;
    int errorFlag = 0;
    int dirLocked = 0, thisDmnLock = 0, lkHndl, err;
    struct statfs fsStats;
    struct stat   stats;
    mlSSDmnOpsT ssDmnCurrentState=0;
    int ssRunningState=0;
    mlDmnParamsT dmnParams;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_MIGRATE, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,14,
				"\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "p:n:s:d:b:" )) != EOF) {
        switch (c) {
            case 'p':
                p++;
                srcPageOffset = strtoul( optarg, NULL, 0 );
                break;

            case 'n':
                n++;
                srcPageCnt = strtoul( optarg, NULL, 0 );
		if (srcPageCnt < 1) {
		    fprintf(stderr, catgets(catd, 1, 15,
			    "%s: pagecount must be a positive integer.\n"),
			    Prog);
		    usage();
		    exit( 1 );
		}
                break;

            case 's':
                s++;
                srcVolIndex = strtoul( optarg, NULL, 0 );
                break;
  
            case 'd':
                d++;
                dstVolIndex = strtoul( optarg, NULL, 0 );
                break;

            case 'b':
                b++;
                dstBlkOffset = strtoul( optarg, NULL, 0 );
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if (p > 1 || n > 1 || s > 1 || d > 1 ) {
        usage();
        exit( 1 );
    }

    if (optind != (argc - 1)) {
        /* missing required arg */
        usage();
        exit( 1 );
    }

    /* get path name */
    path = argv[optind];

    /*
     * Lock the domain while you do the migrate
     */
    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
	fprintf(stderr, catgets(catd, 1, 7, 
				"%s: error locking '%s'; [%d] %s\n"),
		Prog, MSFS_DMN_DIR, err, SYSERRMSG (err) );
	goto _error;
    }
    dirLocked = 1;

    /*
     *  Compute the domain name.  Then lock it.
     */
    sts = statfs( path, &fsStats, sizeof( fsStats) );
    if (sts < 0) {
	fprintf(stderr, catgets(catd, 1, 9, 
				"%s: statfs of %s failed. [%d] %s\n"),
		Prog, path, errno, SYSERRMSG (errno) );
	goto _error;
    }


    if ( MOUNT_MSFS != fsStats.f_type)
    {
	fprintf(stderr, catgets(catd, 1, 13, 
				"%s: %s not located on an AdvFS domain\n"),
		Prog, path );
	goto _error;


    }
    /*
     *  We now how the mount point which we can get the domain name from.
     */

    strcpy(domain_name, fsStats.f_mntfromname);

    if (NULL == strtok(domain_name,"#")) {
	fprintf(stderr, catgets(catd, 1, 11, 
				"%s: strtok of %s failed. [%d] %s\n"),
		Prog, domain_name, errno, SYSERRMSG (errno) );
	goto _error;
    }
	
    strcpy( dmnName, MSFS_DMN_DIR );
    strcat( dmnName, "/" );
    strcat( dmnName, domain_name );

    if (lstat( dmnName, &stats )) {
        fprintf( stderr, catgets(catd, 1, 10, "%s: domain '%s' doesn't exist\n"),
		Prog, dmnName );
        goto _error;
    }

    if (b > 0) {
        msfs_skip_on_disk_version_check();
        sts = msfs_get_dmnname_params(domain_name, &dmnParams);

        if (sts != EOK) {
            fprintf(stderr,
                    "%s: unable to get info for domain '%s'\n",
                    Prog, domain_name);
            fprintf(stderr,
                    "%s: error = %s\n",
                    Prog, BSERRMSG(sts));
            exit(1);
        }

        sts = advfs_ss_dmn_ops(domain_name, SS_UI_STATUS,
                               &ssDmnCurrentState, &ssRunningState);

        if (sts != EOK) {
            fprintf(stderr,
                    "%s: status not available on domain %s; %s\n",
                    Prog, domain_name, BSERRMSG(sts));
        }
        if (ssDmnCurrentState == 1) {  /* smartstore is activated */
            fprintf(stderr,
                    "%s: vfast is currently activated and is claiming exclusive\n"
                    "use of domain %s, migrate with -b is not allowed.\n",
                    Prog, domain_name);
            exit(1);
        }
    }

    /*
     * Lock this domain.
     */
    thisDmnLock = lock_file_nb (dmnName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, 1, 12, 
		     "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
	    fprintf(stderr, catgets(catd, 1, 7, 
		    "%s: error locking '%s'; [%d] %s\n"),
		    Prog, dmnName, err, SYSERRMSG (err) );
        }
        goto _error;
    }



#ifndef _OSF_SOURCE
    fs_init1( "dmn1", "set1" );
#endif

    fd = O_open( path, O_RDONLY, 0 );
    if (fd < 0) {
        fprintf( stderr, catgets(catd, 1, 3, "%s: open of %s failed. [%d] %s\n"),
                Prog, path, errno, sys_errlist[errno] );
	goto _error;
    }

    if(migrate (fd, srcVolIndex, srcPageOffset, srcPageCnt, dstVolIndex, dstBlkOffset))
        errorFlag = 1;

    if (O_close( fd )) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: close of %s failed. [%d] %s\n"),
                Prog, path, errno, sys_errlist[errno] );
        errorFlag = 1;
    }

#ifndef _OSF_SOURCE
    fs_dmo();
#endif

    if (errorFlag)
        goto _error;


    if (unlock_file( thisDmnLock, &err ) == -1) {
        fprintf(stderr, catgets(catd, 1, 8,"%s: error unlocking '%s'; [%d] %s\n"),
		Prog, dmnName, err, SYSERRMSG( err ) );
        goto _error;
    }

    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf(stderr, catgets(catd, 1, 8,"%s: error unlocking '%s'; [%d] %s\n"),
		Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        dirLocked = 0;
        goto _error;
    }

    return 0;

_error:

    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf(stderr, catgets(catd, 1, 8,"%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        }
    }

    return(-1);

}  /* end main */

static
int
migrate (
         int fd,  /* in */
         unsigned long srcVolIndex,  /* in */
         unsigned long srcPageOffset,  /* in */
         unsigned long srcPageCnt,  /* in */
         unsigned long dstVolIndex,  /* in */
         unsigned long dstBlkOffset  /* in */
         )

{
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    unsigned long pageCnt;
    unsigned long pageOffset;
    mlStatusT sts;
    u32T forceFlag = 0;

    pageOffset = srcPageOffset;
    if (pageOffset == -1) {
        pageOffset = 0;
    }
    pageCnt = srcPageCnt;
    if (pageCnt == -1) {
        sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);
        if (sts != EOK) {
            fprintf( stderr, catgets(catd, 1, 5, "%s: msfs_get_bf_params failed --- %s\n"),
                    Prog, BSERRMSG( sts ));
            return 1;
        }
        pageCnt = bfInfo.nextPage - pageOffset;
    }

	if (pageCnt == 0)
		return 0;

retry_migrate:
    sts = advfs_migrate (
                         fd,
                         srcVolIndex,
                         pageOffset,
                         pageCnt,
                         dstVolIndex,
                         dstBlkOffset,
                         forceFlag
                         );
    if (sts != EOK) {
        if ( (!forceFlag) && (sts == E_INVOLUNTARY_ABORT) ) {
            /* Try again, this time with forceFlag set */
            forceFlag = 1;
            goto retry_migrate;
        }
        else {
            fprintf( stderr, catgets(catd, 1, 6, "%s: advfs_migrate failed --- %s\n"),
                Prog, BSERRMSG( sts ));
            return 1;
        }
    }

    return 0;

}  /* end migrate */
/* end migrate.c */
