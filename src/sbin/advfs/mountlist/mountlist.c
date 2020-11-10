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
 */
/*
 *
 * Facility:
 *
 *    MegaSafe Storage System
 *
 * Abstract:
 *
 *    mountlist.c
 *    This module implements a utility which lists all mounted megasafe
 *	  file sets.  It is used by the "setld -d" function to remind the user
 *	  if there are any mounted megasafe file sets.
 *
 * Date:
 *
 *    June 1, 1993
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: mountlist.c,v $ $Revision: 1.1.4.2 $ (DEC) $Date: 1997/02/25 01:27:50 $";
#endif

#include <stdio.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/signal.h>
#ifdef _OSF_SOURCE
#include <sys/mount.h>
#endif
#include <msfs/msfs_syscalls.h>
#include <msfs/bs_error.h>

#ifdef _OSF_SOURCE
#define O_open open
#define O_close close
#else
#include <msfs/fs_syscalls.h>
#endif

#include <locale.h>
#include "mountlist_msg.h"

nl_catd catd;

extern int errno;
extern char *sys_errlist[];

#define ERRMSG(err) sys_errlist[err]


static int addVolSvcClassFlag = 0;
static char *dmnName;
static char *Prog;
static mlServiceClassT targetSvcClass;
static u32T targetVolIndex;
static int unlockWorkFlag = 0;
static int unmountFlag = 0;
static char *volName;
#ifdef _OSF_SOURCE
static char *workDir = "/RMVOL";
#else
static char *workDir = "./RMVOL";
#endif
static int workLock;
static char workPathName[MAXPATHLEN+1];

static
void
usage (
       void
       )
{
    fprintf (stderr, catgets(catd, 1, USAGE, "usage: %s [-v]\n"), Prog);
}


/*
 * The main function returns 0 (success) if no megasafe file sets are
 * found mounted (a return value of 1 indicates that an error occurred or
 * that a megasafe set was found mounted; thus, it indicates to setld that
 * there are possibly mounted megasafe file sets).  In the verbose
 * mode (-v) it also prints a list of the mounted file sets.
 */

main (
      int argc,
      char *argv[]
      )

{

    int c;
    int err;
    int err2;
	int i;
	int mountCnt;
	int verboseFlag = 0;
	int megasafeSetFound = 0;
    extern int getopt();
    extern char *optarg;
    extern int optind;
    extern int opterr;
    struct stat stats;
	struct statfs *mountTbl = NULL;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_MOUNTLIST, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = (char *) strrchr (argv[0], '/')) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd, 1, 5,
                "\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }

    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt (argc, argv, "vV")) != EOF) {
        switch (c) {

          case 'v':
		  case 'V':
            verboseFlag = 1;
            break;

          default:
            usage ();
            exit (1);
        }
    }  /* end while */

    mountCnt = getfsstat (0, 0, MNT_NOWAIT);
    if (mountCnt <= 0) {
        fprintf (stderr, catgets(catd, 1, 2, "%s: getfsstat failed.\n"), Prog);
        return 1;
    }

    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
        fprintf (stderr, catgets(catd, 1, 3, "%s: can't malloc file system mount table.\n"), Prog);
        return 1;
    }

    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 2, "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 4, "%s: error = [%d] %s\n"),
                 Prog, errno, ERRMSG (errno));
        return 1;
    }

    for (i = 0; i < mountCnt; i++) {

		if(mountTbl[i].f_type == MOUNT_MSFS) {
			if (verboseFlag) {
				if (!megasafeSetFound)
					printf("\n");
				printf("        %s\n", mountTbl[i].f_mntfromname);
            }
            megasafeSetFound = 1;
        }
    }

	if (verboseFlag && megasafeSetFound)
		printf("\n");

	return megasafeSetFound;

} /* end main */

/* end mountlist.c */
