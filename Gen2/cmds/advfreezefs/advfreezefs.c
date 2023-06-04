/* =======================================================================
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
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 *
 *
 * Facility:
 *
 *      AdvFS File System
 *
 * Abstract:
 *
 *      Implements the freeze / thaw utilities.
 *
 * Date:
 *
 *      6/1/2001
 *
 */
/*
 * HISTORY
 */

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/file.h>
#include <errno.h>
#include <sys/mount.h>
#include <nl_types.h>
#include <locale.h>
#include "advfreezefs_advfs_msg.h"
#include <sys/fs/advfs_ioctl.h>
#include <advfs/advfs_syscalls.h>


#include <sys/versw.h>

#ifndef TRUE
#define TRUE        1
#define FALSE       0
#endif

/* errno extern and macro define */
extern  char       *sys_errlist[];

        nl_catd     catd;
static  char       *Prog;
  
/*******************************************************************************
 *  Local Function Prototypes
 */

void
usage( u_long optionFlags )
{
    printf( catgets(catd,
                    1,
                    USAGE1,
                    "usage: %s mount-point\n"),
                    Prog );
                    
    if (optionFlags & ADVFS_Q_FREEZE) {
        printf( catgets(catd,
                        1,
                        USAGE3,
                        "    or %s -t timeout mount-point\n"),
                        Prog);
        printf( catgets(catd,
                        1,
                        USAGE4,
                        "    or %s -q mount-point\n"),
                        Prog);
    }
}                     

/*******************************************************************************
 *  Start
 */

main( int argc, char *argv[] )
{
    extern int       optind;
    extern char     *optarg;

    char            *pp;
    char            *mountPoint;
    char            *options = NULL;
    int              fd;
    int              error = 0;
    int              c = 0;    
    int              q = 0;    
    int              t = 0, o=0;
    int              sts = -1;
    int              timeout = 0;
    int              frozen = 0;
    u_long           optionFlags=0;
    char             str[PATH_MAX + sizeof("advfreezefs") + 1];	
    arg_infoT*       infop = NULL;
    
    /*------------------------------------------------------------------------*/

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_ADVFREEZEFS_ADVFS, NL_CAT_LOCALE);

    /*
     *  Get the file name part of argv[0]
     *  This executable has two directory entries
     *  advfreezefs and advthawfs, and we determine which
     *  operation to perform by checking the invoking
     *  programs name.  We MUST be run via advfreezefs or
     *  thawfs, otherwise we return an error.
     */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    if ( strcmp(Prog, "advfreezefs") == 0 ) {
        optionFlags |= ADVFS_Q_FREEZE;
    } else if (strcmp(Prog, "advthawfs") == 0 ) {
	optionFlags |= ADVFS_Q_THAW;
    }
    else {
        fprintf( stderr, 
		 catgets(catd,
			 1,
			 NAME,
			 "%s: error, command name must be advfreezefs or advthawfs\n"),
		 Prog );
        exit (1);
    }

    /*
     *  Check for root
     */ 
    if (geteuid() != 0) {
	fprintf( stderr, catgets(catd,
	                         1,
	                         SUSER,
                                 "%s: error, permission denied - must be privileged user\n"),
                                 Prog );
	exit(1);
    }
    /*
     *  Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "t:q" )) != EOF) {
        switch (c) {
            case 't':
                t++;
                timeout = (int) strtol( optarg, &pp, 0 );
                if ((timeout == 0) && (optarg == pp)) {
                    /*
                     * String given for timeout value not a 
                     * valid number.
                     */
                    usage(optionFlags);
                    exit( 1 );
                }
                break;
            case 'q':
		if (optionFlags & ADVFS_Q_FREEZE)
                    q++;
                else {
                    /* Must use "advfreezefs" to do query */
                    usage(optionFlags);
                    exit( 1 );
                }
                break;

            default:
                usage(optionFlags);
                exit( 1 );
        }
    }

    if ((t && q) || (t > 1) || (q > 1)) {
        usage(optionFlags);
        exit( 1 );
    }

    if ((argc - 1 - optind) < 0) {
        /* missing required args */
        usage(optionFlags);
        exit( 1 );
    }

    if (q) {
        optionFlags &= ~ADVFS_Q_FREEZE;
        optionFlags |= ADVFS_Q_QUERY;
    }

    infop = process_advfs_arg(argv[optind]);
    if (infop == NULL) {
        fprintf( stderr, catgets(catd, 1, FSNAME_ERR,
            "%s: error processing specified name %s\n"), Prog, argv[optind]);
        exit(1);
    }

    sts = advfs_is_mounted(infop, NULL, NULL, &mountPoint); 
    if (sts != 1) {
        fprintf( stderr, catgets(catd, 1, NOTMNT,
            "%s: file system is not mounted or not AdvFS\n"), Prog);
        free(infop);
        exit(1);
    }

    free(infop);  /* no longer needed */

    if ( !strcmp(mountPoint,"/")     ||
         !strcmp(mountPoint,"/usr")  ||
         !strcmp(mountPoint,"/var") ) {
         
        fprintf( stderr, catgets(catd,
                                 1,
                                 ROOTUSRVAR,
                                 "%s: error, cannot freeze /, /usr or /var\n"),
                                 Prog );
        free(mountPoint);
        exit( 1 );
    }

    errno = 0;

    /*
     * Open the device.
     */
    if ((fd = open( mountPoint, O_RDONLY )) == -1) {

	fprintf( stderr, catgets(catd,
				 1,
				 MPOPEN,
				 "%s: Cannot open %s\n"),
		 Prog, mountPoint);
        free(mountPoint);
	exit(1);
    }

    /* 
     * do the command and report the results
     */
    
    switch (optionFlags) {
	
    case ADVFS_Q_FREEZE:

	sts = ioctl ( fd, ADVFS_FREEZEFS, &timeout );

	if ( sts == -1 ) {
	    /* Report the error... */
	    fprintf( stderr,
		     catgets(catd,
			     1,
			     FREEZEFAIL,
			     "%s: error performing freeze: [%d]\n"),
		     Prog, errno );
	}
	else {
	    /* Report success... */
	    printf( catgets(catd,
			    1,
			    FREEZESUCCESS,
			    "%s %s: Successful\n"),
		    Prog, mountPoint );
	}

	break;

    case ADVFS_Q_THAW:

	sts = ioctl ( fd, ADVFS_THAWFS, &timeout );

	if ( sts == -1 ) {
	    /* Report the error ... */
	    fprintf( stderr,
		     catgets(catd,
			     1,
			     THAWFAIL,
			     "%s: error performing thaw: [%d]\n"),
		     Prog, errno );
	}
	else {
	    /* Report success... */
	    printf( catgets(catd,
			    1,
			    FREEZESUCCESS,
			    "%s %s: Successful\n"),
		    Prog, mountPoint );
	}

	break;

    case ADVFS_Q_QUERY:
	
	sts = ioctl ( fd, ADVFS_FREEZEQUERY, &frozen );

	if (sts == 0) {
	    if (frozen == 0) {
		printf( catgets(catd, 1, NOTFROZEN, "%s is not frozen\n"),
			mountPoint );
	    } else if (frozen == 1) {
		printf( catgets(catd, 1, FROZEN, "%s is frozen\n"),
			mountPoint );
	    } else {
		/* Bad return value from ioctl - should never happen */
		sts = -1;
		errno = EINVAL;
	    }
	}

	else if ( sts == -1 ) {
	    fprintf( stderr,
		     catgets(catd,
			     1,
			     QUERYFAIL,
			     "%s: error performing query: [%d]\n"),
		     Prog, errno );
	}

	break;
    }

    free(mountPoint);

    /* The close can block if the file system has been modified, and
       is now frozen. Fork a child to do the cleanup, and return */

    switch (fork()) {
    case -1:  /* bad fork */
	sts = ENOEXEC;
	/* fall through to child code */
	
    case 0:  /* child */
	close( fd );
	return( sts );
	break;
	
    default:  /* parent */
	return( sts );
	break;
    }

}

/* end of advfreezefs.c */
