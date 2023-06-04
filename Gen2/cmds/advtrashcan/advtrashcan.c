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
 */
/*
 *
 * Facility:
 *
 *      ADVFS
 *
 * Abstract:
 *
 *      Defines the Trashcan Directory Mgt utility.
 *
 */

#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <errno.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <libgen.h>
#include <mntent.h>

/* The following define should be set within the
 * <mntent.h> header.  
 */
#define MNTTYPE_ADVFS "advfs"

#include <advfs/bs_error.h>
#include <advfs/advfs_syscalls.h>

extern int errno;
extern char *sys_errlist[];

#define ERRMSG(en) sys_errlist[( en )] 

#include <locale.h>
#include <nl_types.h>
#include "advtrashcan_advfs_msg.h"

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

#define TRUE 1
#define FALSE 0

typedef struct _dName {
    char name[256];
    struct _dName *next;
} dNameT;

typedef enum { MKTRASHCAN, RMTRASHCAN, SHTRASHCAN } selfT;

static char *Prog = "advtrashcan";

void
usage( )
{
    fprintf(stderr, 
	    MSG(TCUSAGE, "usage: %s [-m trashcan | -r] directory ...\n"), 
	    Prog);
}

main ( int argc, char *argv[] )
{
    selfT option;
    int argindex;
    int errorFlag = FALSE;
    int cmdFailed = FALSE;
    char *p, *dirName, *trashcan;
    adv_status_t sts;
    struct stat dirStats;

    Prog = basename(argv[0]);

    setlocale( LC_ALL, "" );
    catd = catopen( MF_ADVTRASHCAN_ADVFS, 0 );

    if ( !strcmp(argv[1], "-m")) {
        option = MKTRASHCAN;
        if (argc < 4) {
            usage();
            exit(1);
        }
        trashcan = argv[2];
	argindex = 3;

    } else if (!strcmp(argv[1], "-r")) {
        option = RMTRASHCAN;
        if (argc < 3) {
            usage();
            exit(1);
        }
	argindex = 2;

    } else {
	/* check for bogus switches */
	if (!strncmp(argv[1], "-", 1)) {
	    usage();
	    exit(1);
	}

        option = SHTRASHCAN;
        if (argc < 2) {
            usage();
            exit(1);
        }
	argindex = 1;
    }
       
    for ( ; argindex < argc; argindex++ ) {

	if (errorFlag) cmdFailed = TRUE;
	errorFlag = FALSE;

        dirName = argv[argindex];

        if (stat( dirName, &dirStats ) < 0) {
            fprintf( stderr, 
                     MSG( E_STAT, "%s: cannot access '%s'; %s\n" ), 
                     Prog, dirName, ERRMSG( errno ) );
            errorFlag = TRUE;
            continue;
        }

        if (!S_ISDIR( dirStats.st_mode )) {
            /* ignore non-directories */
            continue;
        }

        if ( option == MKTRASHCAN ) {

            if (access( trashcan, R_OK | W_OK | X_OK | F_OK ) < 0) {
                fprintf( stderr, 
                         MSG( E_STAT, "%s: cannot access '%s'; %s\n" ), 
                         Prog, trashcan, ERRMSG( errno ) );
                errorFlag = TRUE;
                continue;
            }

            sts = advfs_undel_attach( dirName, trashcan );
            if (sts != EOK) {
                fprintf( stderr, 
                         MSG( E_ATTACH, 
                              "%s: cannot attach trashcan '%s' to '%s'; %s\n" ),
                         Prog, trashcan, dirName, BSERRMSG( sts ) );
                errorFlag = TRUE;
                continue;
            }

            printf( MSG( E_ATTACHED1, 
                         "    '%s' attached to '%s'\n" ) , trashcan, dirName);

        }  /* end MKTRASHCAN */
    
        if ( option == RMTRASHCAN ) {

            bfTagT trashcanTag;

            /*
             * Get the tag of the trashcan dir.
             */
    
            sts = advfs_undel_get( dirName, &trashcanTag );
            if ((sts != EOK) || (trashcanTag.tag_num == 0)) {
                fprintf( stderr, 
                         MSG( E_NOTRASHCAN, "%s: '%s' has no trashcan\n" ),
                         Prog,  dirName );
                errorFlag = TRUE;
                continue;
            }
    
            sts = advfs_undel_detach( dirName );
            if (sts != EOK) {
                fprintf( stderr, 
                         MSG( E_DETACH,
                              "%s: cannot detach trashcan from '%s'; %s\n" ),
                         Prog,  dirName, BSERRMSG( sts ) );
                errorFlag = TRUE;
                continue;
            }

            printf( MSG( E_DETACHED, "    '%s' detached\n" ), dirName );

        }  /* end RMTRASHCAN */
    
        if ( option == SHTRASHCAN ) {

            dNameT *dNames = NULL, *dName;
            bfTagT trashcanTag, parentTag, dTag;
            struct stat fileStats; /* file stats */
            struct stat rootStats; /* file set's root stats */
            FILE*  mntFile = NULL;
            struct mntent* mntInfo;  
	    int foundit;

            if ( stat(dirName, &fileStats) < 0) {
                fprintf(stderr, 
                    MSG(E_STAT, "%s: cannot access '%s'; %s\n"), 
                    Prog, dirName, errno);
                continue;
            }

            /* 
             * Use getmntent() to get the pathname of the mount point.  The
             * rest of the code gets the path components from the mount
             * point to the trashcan dir name.
             */
            mntFile = setmntent(MOUNTED, "r");
            if (mntFile == NULL) {
                fprintf(stderr, 
                    MSG(E_STAT, "%s: cannot access '%s'; %s\n"), 
                    Prog, MOUNTED, errno);
		endmntent(mntFile);
                continue;
            }

	    foundit = FALSE;
            while (mntInfo = getmntent(mntFile)) {
                if ( stat(mntInfo->mnt_dir, &rootStats) == 0) {
                    if (rootStats.st_dev == fileStats.st_dev) {
                        /*  found a match */
                        if (strcmp(mntInfo->mnt_type, MNTTYPE_ADVFS) != 0) {
                            fprintf(stderr, MSG( E_ADVFS, 
                                "%s: %s is not in an AdvFS filesystem\n"),
                                Prog, dirName );
			    errorFlag = TRUE;
			    endmntent(mntFile);
                        } else {
			    foundit = TRUE;
			}
                        break;
                    }
                }
            }

            endmntent(mntFile);
            if ( !foundit ) {
                continue;
            }

            /*
             * Get the tag of the trashcan dir.
             */
    
            sts = advfs_undel_get( dirName, &trashcanTag );
            if ((sts != EOK) || (trashcanTag.tag_num == 0)) {
                fprintf( stderr, 
                         MSG( E_NOTRASHCAN, "%s: '%s' has no trashcan\n" ),
                         Prog,  dirName );
		errorFlag = TRUE;
		continue;
	    }

            /*
             * The following loop calls advfs_get_name() to get the file/dir name 
             * string for a tag.  advfs_get_name() also returns the tag's parent
             * dir's tag.  So we keep calling advfs_get_name() until we've
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
    
                if ((dName = (dNameT *) malloc( sizeof(dNameT))) == 0 ) {
		    fprintf( stderr, 
			     MSG ( E_NOMEM, "%s: insufficient memory.\n" ),
			     Prog );
		    errorFlag = TRUE;
		    break;
		}
    
                /* Get the current dir's name its parent's tag */
    
                sts = advfs_get_name( mntInfo->mnt_dir, 
                                     dTag, 
                                     dName->name, 
                                     &parentTag );
                if (sts != EOK) {
                    fprintf( stderr, 
                             MSG( E_NOTRASHCAN, "%s: '%s' has no trashcan\n" ),
                             Prog,  dirName );
		    errorFlag = TRUE;
		    break;
                }
    
                /* Link the current dir's pathname component into the list */
    
                dName->next = dNames;
                dNames = dName;
    
                /* The current dir is set to the parent dir; do the parent next */
    
                dTag = parentTag;
    
            } while (dTag.tag_num != rootStats.st_ino);

	    if ( errorFlag ) {
		continue;
	    }

            /*
             * Now we can print the full pathname of the trashcan dir.
             * First we print the mount point pathname and then we
             * print the remaining pathname components by traversing
             * the pathname components linked list.
             */
    
            printf( "    '%s", mntInfo->mnt_dir);
    
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

        }  /* end SHTRASHCAN */

    }  /* end for (all argv) */

    /* Return error for command if any failure detected. Be sure to
       check for final pass through for loop */
    if (errorFlag || cmdFailed)
        return 1;

    return 0;
}

