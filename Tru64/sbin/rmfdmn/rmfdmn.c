/***********************************************************************
 *                                                                     *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1991, 1992, 1993, 1994           *
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
 *      MegaSafe Filesystem Remove Domain
 *	A substantial amount of this code is pilfered from the mkfdmn
 *	and defragment utilities.
 *
 * Date:
 *
 *      Tue May 17 09:22:09 1994
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: rmfdmn.c,v $ $Revision: 1.1.18.1 $ (DEC) $Date: 2000/10/19 19:36:49 $";
#endif

#include <stdio.h>
#include <strings.h>
#include <dirent.h>
#include <signal.h>
#include <unistd.h>
#include <sys/ioctl.h>
#include <overlap.h>
#include <stdarg.h>
#include <sys/errno.h>
#include <sys/mount.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "rmfdmn_msg.h"
#include <msfs/advfs_evm.h>

#ifdef ESS
#include <ess/event_simulation.h>
#endif

nl_catd catd;

extern int errno;
extern char *sys_errlist[];
extern void sync();

static advfs_ev advfs_event;

#define SYSERRMSG( e ) sys_errlist[e]

#ifndef BBSIZE
#define BBSIZE 8192
#endif

char *Prog = "rmfdmn";

static void
usage( void )
{
printf( catgets(catd, 1, USAGE, "usage: %s [-f] domain\n"), Prog );
}

main ( int argc, char *argv[] )
{
    char lockFile[MAXPATHLEN+8];
    char dmnName[MAXPATHLEN+1];
    char dmnNameTemp[MAXPATHLEN+1];
    char *mountDmnName;
    char *mountBfSetName;
    char *domain;
    mlStatusT sts;
    int dirLocked = 0;
    int goforit = 0, c, i, n, err, lkHndl, mountCnt;
    int thisDmnLock, ret;
    struct statfs *mountTbl = NULL;
    char dev_name[MAXPATHLEN];
    char link_name[MAXPATHLEN];
    extern int optind;
    extern char *optarg;
    struct stat stats;
    char response[20];
    DIR *dirp;
    struct dirent *d;
    FILE *Local_tty = stdin;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_RMFDMN, NL_CAT_LOCALE);

    (void) signal( SIGINT, SIG_IGN ); /* ignore interrupt signals */


    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd, 1, 20,
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

    if ((argc - 1 - optind) < 0) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    domain = argv[optind];
    init_event(&advfs_event);
    advfs_event.domain = domain;

    if (access( MSFS_DMN_DIR, R_OK | X_OK | W_OK )) {

        fprintf( stderr, catgets(catd, 1, 2, "%s: error access dir '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, errno, SYSERRMSG( errno ) );
        goto _error;
    }

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 3, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        goto _error;
    }

    dirLocked = 1;

    strcpy( dmnName, MSFS_DMN_DIR );
    strcat( dmnName, "/" );
    strcat( dmnName, domain );

    if (lstat( dmnName, &stats ) < 0) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: domain '%s' does not exist\n"), 
                     Prog, dmnName );
            goto _error;
    }

    /*
     * Lock this domain.  This does not prevent this utility
     * from deleting the file, and the lock will be released
     * when the utility exits.
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
                     Prog, err, SYSERRMSG(err));
        }
        goto _error;
    } 

    /*
     * determine if any filesets are mounted on this domain before
     * nukeing it.
     */
    mountCnt = getfsstat(0,0, MNT_NOWAIT);
    if (mountCnt < 0) {
        fprintf( stderr, catgets(catd, 1, 8, "%s: getfsstat failed. \n"), Prog);
        goto _error;
    }
    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
	fprintf(stderr, catgets(catd, 1, 9, "%s: Can't allocate file system mount table.\n"),
	    Prog);
	goto _error;
    }
    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        fprintf( stderr, catgets(catd, 1, 8, "%s: getfsstat failed. \n"), Prog);
        fprintf( stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s \n"), 
		Prog, errno, SYSERRMSG (errno)); 
	goto _error;
    }
    /*
     * check the domain name of any mounted advfs filesets. If it's ours,
     * then bomb out.
     */
    for (i=0; i< mountCnt; i++) {
	mountDmnName = (char *) strtok (mountTbl[i].f_mntfromname, "#");
	if ((mountTbl[i].f_type == MOUNT_MSFS) &&
	    (strcmp (domain, mountDmnName) == 0)) {
	/* it's in the domain */
	    mountBfSetName = strtok (0, "");
            fprintf( stderr, catgets(catd, 1, 11, "%s: Fileset %s mounted on %s in domain %s. \n"),
		Prog, mountBfSetName, mountTbl[i].f_mntonname, mountDmnName);
            fprintf( stderr, catgets(catd, 1, 12, "All bitfile sets must be dismounted prior to removal. \n"));
	    goto _error;
	}
    }
    /* 
     * get the list of devices in the domain before getting rid of it
     */
    /*
     * didn't find anything mounted, so make sure the user REALLY means it
     */
    if (!goforit) {
    	fprintf( stderr, catgets(catd, 1, 13, "%s: remove domain %s? [y/n] "), Prog, domain);
    	fgets (response, 2, Local_tty);
    	if (strcmp(response, "y") != 0) {
	    goto _error;
    	}
    }
    /*
     * Rename the domain's dir to something temporary and, ideally,
     * unique.  Die, else.
     */
    
    sprintf(dmnNameTemp, "%s/.rmfdmn.%s.%d", MSFS_DMN_DIR, domain, getpid());
    if (rename(dmnName, dmnNameTemp)) {
        fprintf(stderr, catgets(catd, 1, 21, "%s: Could not rename domain %s\nto temporary name %s.\n"), Prog, dmnName, dmnNameTemp);
        goto _error;
    }


    /*
     * open the dir and read the partition names. Call update_disk_label
     * for each partition to set the disk label to FS_UNUSED. If this 
     * fails, delete the domain anyway.
     */
	
    if ((dirp = opendir(dmnNameTemp)) == NULL) {
        fprintf( stderr, catgets(catd, 1, 14, "%s: can't open %s, domain partitions will not be set unused\n"), Prog, dmnNameTemp);
        goto remove;
    }
    while ((d = readdir(dirp)) != NULL) {
        int cc;  /* used for link_name return length */

	if (strcmp(d->d_name, ".") == 0)
		continue;
	if (strcmp(d->d_name, "..") == 0)
		continue;

	/* Need to get the full link name, as diskgroups are */
	/* not of the format /dev/name but /dev/vol/name */
        strcpy(dev_name, dmnNameTemp);
        strcat(dev_name, "/");
	strcat(dev_name, d->d_name);
	cc = readlink(dev_name, link_name, MAXPATHLEN);
	if (cc >= 0) {
	    ret = set_usage(link_name, FS_UNUSED, 1);
	    if (ret != 0) {
	        fprintf(stderr, catgets(catd, 1, 18, "%s: set_usage error = [%d] while setting disklabel of %s\n"),
			Prog, ret, d->d_name);
	    }
	} else {
	    fprintf(stderr, catgets(catd, 1, 19, "%s: '%s' is not a symbolic link to a device.\n"),
		    Prog, d->d_name);
	}
	unlink(dev_name);
    }
    closedir(dirp);
    /*
     * remove the fdmns directory
     */
remove:
    rmdir(dmnNameTemp); /* What if this fails? */    
    advfs_post_user_event(EVENT_FDMN_RM, advfs_event, Prog);
    printf(catgets(catd, 1, 15, "%s: domain %s removed.\n"), Prog, domain);
    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 16, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        dirLocked = 0;  /* Prevent another attempted (at _error:) unlocking. */
    }

    /*
     * Remove the lock file for the removed domain.
     */
    strcpy(lockFile, ADVFS_LOCK_FILE);
    strcat(lockFile, domain);
    unlink(lockFile);

#ifdef ESS

    /* QA Test Case: perform failure when rmfdmn completed the process */
    ESS_MACRO_0(TCRGenPanicKernel, ADVFS_RMFDMN_PANIC);

#endif

    return 0;

_error:
    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 16, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, SYSERRMSG( err ) );
        }
    }

    fprintf( stderr, catgets(catd, 1, 17, "%s: can't remove domain '%s' \n"), Prog, domain );
    exit( 1 );
}



/****************************************************************************
 *
 */

/*
 */
/* end rmfdmn.c */
