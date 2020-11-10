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
 * Abstract:
 *
 *      Defines the Trashcan Directory Mgt utility.
 *
 * Date:
 *
 *      Wed Sep 18 13:13:54 1991
 *
 * Revision History:
 *
 *      See end of file.
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: mktrashcan.c,v $ $Revision: 1.1.2.4 $ (DEC) $Date: 1998/09/21 16:18:32 $";
#endif

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mount.h>

#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>

extern int errno;
extern char *sys_errlist[];

#define ERRMSG(en) sys_errlist[( en )] 

#ifndef STANDALONE

#include <locale.h>
#include <nl_types.h>
#include "mktrashcan_msg.h"

nl_catd catd;

char *
msg( int num, char *str ) 
{
    char *retStr = 0;

    retStr = catgets( catd, MS_TRASHCAN, num, str );

    if (retStr == 0 || retStr[0] == '\0') {
        fprintf( stderr, "catgets: unknown message [%d : %s]\n", num, str );
        return str;
    }

    return retStr;
}

#ifdef MSG
#undef MSG
#endif
#define MSG msg

#else /* STANDALONE */

#define MSG( num, str) str

#endif /* STANDALONE */

#define TRUE 1
#define FALSE 0

typedef struct _dName {
    char name[256];
    struct _dName *next;
} dNameT;

typedef enum { MKTRASHCAN, RMTRASHCAN, SHTRASHCAN } selfT;

static char *Prog = "mktrashcan";


void
usage( selfT self )
{
    switch (self) {
        case MKTRASHCAN:
            fprintf( stderr, MSG( MKUSAGE, "usage: %s trashcan directory ... \n"), 
                    Prog );
            break;

        case RMTRASHCAN:
            fprintf( stderr, MSG( RMUSAGE, "usage: %s directory ... \n"), Prog );
            break;

        case SHTRASHCAN:
            fprintf( stderr, MSG( SHUSAGE, "usage: %s directory ... \n"), Prog );
            break;

        default:
            break;
    }
}

main ( int argc, char *argv[] )
{
    selfT self;
    int c, a = 0, d = 0, s = 0, error = 0;
    char *p, *dirName, *trashcan;
    mlStatusT sts;
    extern int optind;
    extern char *optarg;
    struct stat dirStats;
    int errorFlag = FALSE;

    /*--------------------------------------------------------------------------*/


#ifndef STANDALONE
    setlocale( LC_ALL, "" );
    catd = catopen( MF_MKTRASHCAN, 0 );
#endif /* STANDALONE */

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    argv++;


    if (strcmp( Prog, "mktrashcan" ) == 0) {
        self = MKTRASHCAN;

	if (argc > 1) {
   	 	if ((argv[0][0] == '-') && (argv[0][1] != '\0')) {
			fprintf(stderr, MSG(ILLEGAL, "%s:  illegal option -- %s\n"),
				Prog, &(argv[0][1]));

			usage( self );
			exit(1);
		}
	}

        if (argc < 3) {
            usage( self );
            exit(1);
        }

        trashcan = *argv;
        argc--;
        argv++;
        a = TRUE;

    } else if (strcmp( Prog, "rmtrashcan" ) == 0) {
        self = RMTRASHCAN;

        if (argc < 2) {
            usage( self );
            exit(1);
        }

 	if ((argv[0][0] == '-') && (argv[0][1] != '\0')) {
		fprintf(stderr, MSG(ILLEGAL, "%s:  illegal option -- %s\n"),
			Prog, &(argv[0][1]));

		usage( self );
		exit(1);
	}

        d = TRUE;

    } else if (strcmp( Prog, "shtrashcan" ) == 0) {
        self = SHTRASHCAN;

        if (argc < 2) {
            usage( self );
            exit(1);
        }

 	if ((argv[0][0] == '-') && (argv[0][1] != '\0')) {
		fprintf(stderr, MSG(ILLEGAL, "%s:  illegal option -- %s\n"),
			Prog, &(argv[0][1]));

		usage( self );
		exit(1);
	}

        s = TRUE;

    } else {
        fprintf( stderr, 
                 MSG( E_PROG, "%s: program name must be one of 'mktrashcan', "
                              "'rmtrashcan' or 'shtrashcan'\n" ), 
                 Prog, errno, ERRMSG( errno ) );
        errorFlag = TRUE;
    }
    
    while ( argc > 1 ) {
        dirName = *argv;

        argc--;
        argv++;

        if (stat( dirName, &dirStats ) < 0) {
            fprintf( stderr, 
                     MSG( E_STAT, "%s: can't access '%s'; %s\n" ), 
                     Prog, dirName, ERRMSG( errno ) );
            errorFlag = TRUE;
            goto _error;
        }

        if (!S_ISDIR( dirStats.st_mode )) {
            /* ignore non-directories */
            goto _error;
        }

        if (a) {
            if (access( trashcan, R_OK | W_OK | X_OK | F_OK ) < 0) {
                fprintf( stderr, 
                         MSG( E_STAT, "%s: can't access '%s'; %s\n" ), 
                         Prog, trashcan, ERRMSG( errno ) );
                errorFlag = TRUE;
                goto _error;
            }

            sts = msfs_undel_attach( dirName, trashcan );
            if (sts != EOK) {
                fprintf( stderr, 
                         MSG( E_ATTACH, 
                              "%s: can't attach trashcan '%s' to '%s'; %s\n" ),
                         Prog, trashcan, dirName, BSERRMSG( sts ) );
                errorFlag = TRUE;
                goto _error;
            }

            printf( MSG( E_ATTACHED1, 
                         "    '%s' attached to '%s'\n" ) , trashcan, dirName);
        }
    
        if (d) {
            mlBfTagT trashcanTag;

            /*
             * Get the tag of the trashcan dir.
             */
    
            sts = msfs_undel_get( dirName, &trashcanTag );
            if ((sts != EOK) || (trashcanTag.num == 0)) {
                fprintf( stderr, 
                         MSG( E_NOTRASHCAN, "%s: '%s' has no trashcan\n" ),
                         Prog,  dirName );
                errorFlag = TRUE;
                goto _error;
            }
    
            sts = msfs_undel_detach( dirName );
            if (sts != EOK) {
                fprintf( stderr, 
                         MSG( E_DETACH,
                              "%s: can't detach trashcan from '%s'; %s\n" ),
                         Prog,  dirName, BSERRMSG( sts ) );
                errorFlag = TRUE;
                goto _error;
            }

            printf( MSG( E_DETACHED, "    '%s' detached\n" ), dirName );
        }
    
        if (s) {
            dNameT *dNames = NULL, *dName;
            mlBfTagT trashcanTag, parentTag, dTag;
            struct statfs fsStats; /* file set's fstats */
            struct stat rootStats; /* file set's root stats */
    
            /* 
             * Use statfs() to get the pathname of the mount point.  The
             * rest of the code gets the path components from the mount
             * point to the trashcan dir name.
             */
    
            if (statfs( dirName, &fsStats ) < 0) {
                fprintf( stderr, 
                         MSG( E_STATFS, "%s: statfs() error; [%d] %s\n" ), 
                         Prog, errno, ERRMSG( errno ) );
                errorFlag = TRUE;
                goto _error;
            }
    
            if (fsStats.f_type != MOUNT_MSFS) {
                fprintf( stderr,
                         MSG( E_ADVFS, "%s: %s is not in a ADVFS filesystem\n"),
                         Prog, dirName );
                errorFlag = TRUE;
                goto _error;
            } 
    
            if (stat( fsStats.f_mntonname, &rootStats ) < 0) {
                fprintf( stderr, 
                         MSG( E_STAT, "%s: can't access '%s'; %s\n" ), 
                         Prog, fsStats.f_mntonname, ERRMSG( errno ) );
                errorFlag = TRUE;
                goto _error;
            }
    
            /*
             * Get the tag of the trashcan dir.
             */
    
            sts = msfs_undel_get( dirName, &trashcanTag );
            if ((sts != EOK) || (trashcanTag.num == 0)) {
                fprintf( stderr, 
                         MSG( E_NOTRASHCAN, "%s: '%s' has no trashcan\n" ),
                         Prog,  dirName );
                errorFlag = TRUE;
                goto _error;
            }
    
            /*
             * The following loop calls msfs_get_name() to get the file/dir name 
             * string for a tag.  msfs_get_name() also returns the tag's parent
             * dir's tag.  So we keep calling msfs_get_name() until we've
             * collected the name strings for all the pathname components
             * of the complete pathname for the trashcan dir.
             *
             * The pathname components are linked together using dNameT
             * structures.  This is singly-linked list and the new elements
             * are inserted at the head.  Since we have to start with the
             * trashcan dir's tag and work our way backwards to the file set
             * root dir, using this list has the effect of reversing the
             * order of the pathname components so that a forward traversal
             * of the list gives us the correct pathname; the components
             * are in the correct forward (left to right) order.
             *
             * The loop is terminated when we reach the file set's root dir.
             */
    
            dTag = trashcanTag;
    
            do {
                /* Get a new pathname component struct */
    
                dName = (dNameT *) malloc( sizeof( dNameT ) );
    
                /* Get the current dir's name its parent's tag */
    
                sts = msfs_get_name( fsStats.f_mntonname, 
                                     dTag, 
                                     dName->name, 
                                     &parentTag );
                if (sts != EOK) {
                    fprintf( stderr, 
                             MSG( E_NOTRASHCAN, "%s: '%s' has no trashcan\n" ),
                             Prog,  dirName );
                    errorFlag = TRUE;
                    goto _error;
                }
    
                /* Link the current dir's pathname component into the list */
    
                dName->next = dNames;
                dNames = dName;
    
                /* The current dir is set to the parent dir; do the parent next */
    
                dTag = parentTag;
    
            } while (dTag.num != rootStats.st_ino);
    
            /*
             * Now we can print the full pathname of the trashcan dir.
             * First we print the mount point pathname and then we
             * print the remaining pathname components by traversing
             * the pathname components linked list.
             */
    
            printf( "    '%s", fsStats.f_mntonname );
    
            while (dNames != NULL) {
                dNameT *tmp;
    
                if (dNames->name[0] != '/') {
                    printf( "/" );
                }
    
                printf( "%s", dNames->name );
                tmp = dNames;
                dNames = dNames->next;
                free( tmp );
            }
    
            printf( MSG( E_ATTACHED2, "' attached to '%s'\n" ), dirName );
        }
_error:
        ;
    }

    if (errorFlag)
        return 1;

    return 0;
}
