/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1993                                  *
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
 *      AdvFS Storage System
 *
 * Abstract:
 *
 *      Defines the "AdvFS Change File-set" utility.
 *
 * Date:
 *
 *      Wed Jun 30 13:13:54 1993
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: chfsets.c,v $ $Revision: 1.1.40.2 $ (DEC) $Date: 2001/12/10 14:19:54 $";
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
#include <sys/mount.h>
#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "chfsets_msg.h"
#include <msfs/advfs_evm.h>
#include <msfs/bs_ods.h>
#include <sys/versw.h>

#define TRUE        1
#define FALSE       0
#define ERRMSG( e ) sys_errlist[e]
#define MODE_STR_SZ 10

extern  int         errno;
extern  char       *sys_errlist[];

nl_catd catd;

static  advfs_ev    advfs_event;
static  char       *Prog;
  
/*
 *  Local Function Prototypes
 */

int
change_sets(char *dmnName,                /* in */
            char *setName,                /* in */
            int   b,                      /* in */
            long  blkLimit,               /* in */
            int   B,                      /* in */
            long  blkSLimit,              /* in */
            int   f,                      /* in */
            long  fileLimit,              /* in */
            int   F,                      /* in */
            long  fileSLimit,             /* in */
            int   o,                      /* in */
            int   fsetOptions,            /* in */
            int   fsetOptionsMask);       /* in */

void 
add_options(char **old, char *new);

int
set_options (char *options, u32T *fsetOptions, u32T *fsetOptionsMask, char *domain);

void
usage( void )
{
    printf( catgets(catd, 1, USAGE,
            "usage: %s options domain [fileset ...]\n"), Prog );
    printf( catgets(catd, 1, 30,
            "\t[-f limit]\t - specify fileset's file usage limit (quota)\n") );
    printf( catgets(catd, 1, 31,
            "\t[-F limit]\t - specify fileset's file usage soft limit (quota)\n") );
    printf( catgets(catd, 1, 32,
            "\t[-b limit]\t - specify fileset's block (1k) usage limit (quota)\n") );
    printf( catgets(catd, 1, 33,
            "\t[-B limit]\t - specify fileset's block (1k) usage soft limit (quota)\n") );
    printf( catgets(catd, 1, OPTION1,
            "\t[-o arg[,arg]    - specify fileset options, valid arguments are:\n") );
}

/*
 *  Start
 */

main( int argc, char *argv[] )
{
    extern int       optind;
    extern char     *optarg;

    char             tmp[MAXPATHLEN] = {0};  
    char            *p;
    char            *pp;
    char            *dmnName;
    char            *setName;
    char            *options = NULL;
    int              error = 0;    
    int              f = 0, b = 0, F = 0, B = 0, o = 0;
    int              c;
    int              ii;
    int              sts;
    long             blkLimit = 0;
    long             fileLimit = 0;
    long             blkSLimit = 0;
    long             fileSLimit = 0;
    uint32T          fsetOptions = 0;
    uint32T          fsetOptionsMask = 0;
    struct timeval   time;
    struct timezone  tz;
    
    /*------------------------------------------------------------------------*/

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_CHFSETS, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    if (geteuid() != 0) {
	fprintf( stderr, catgets(catd, 1, 37,
	         "\nPermission denied - user must be root to run %s.\n\n"), Prog);
	usage();
	exit(1);
    }

    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "f:F:b:B:o:" )) != EOF) {
        switch (c) {
            case 'b':
                b++;
                blkLimit = strtoul( optarg, &pp, 0 );
                break;

            case 'f':
                f++;
                fileLimit = strtoul( optarg, &pp, 0 );
                break;

            case 'B':
                B++;
                blkSLimit = strtoul( optarg, &pp, 0 );
                break;

            case 'F':
                F++;
                fileSLimit = strtoul( optarg, &pp, 0 );
                break;

            case 'o':
                o++;
                add_options(&options, optarg); 
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if (b > 1 || f > 1 || B > 1 || F > 1 ) {
        usage();
        exit( 1 );
    }

    if ((argc - 1 - optind) < 0) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    dmnName = argv[optind];

    /*
     *  If the -o options parameter was used, parse the options and
     *  set the option flags and mask.
     */    
    if (o) {
    
        if (set_options(options, &fsetOptions, &fsetOptionsMask, dmnName) == FALSE) {
            goto _error;
        }
    }

    if ((optind + 1) >= argc) {

        /*
         *  If no options to set, just showfsets
         */
        if ((b + f + F + B + o ) == 0) {
            sprintf(tmp, "/sbin/showfsets %s",dmnName);
            system(tmp);

        /*
         *  Change the specified options
         */            
        } else if (change_sets( dmnName, NULL,
                                b, blkLimit, B, blkSLimit,
                                f, fileLimit, F, fileSLimit,
                                o, fsetOptions, fsetOptionsMask ) <= 0) {

            /* error or no sets in the domain */
            goto _error;
        }

    } else {

        for (ii = optind + 1; ii < argc; ii++) {
        
            setName = argv[ii];
            /*
             *  If no options to set, just showfsets
             */
            if ((b + f + F + B + o ) == 0) {
                sprintf(tmp, "/sbin/showfsets %s %s",dmnName, setName);
                system(tmp);

            /*
             *  Change the specified options
             */
            } else {       
                sts = change_sets( dmnName, setName,
                                   b, blkLimit, B, blkSLimit,
                                   f, fileLimit, F, fileSLimit,
                                   o, fsetOptions, fsetOptionsMask );

                if (sts == -2) {
                    error = 1;
                } else if (sts <= 0) {
                    /* error or no sets in the domain */
                    goto _error;
                }
            }
        }

	if (error) {
	    /* One or more of the filesets in the domain was not found. */
	    goto _error;
	}
    }

    return 0;

_error:

    exit( 1 );
}

int
change_sets( char *dmnName,                /* in */
             char *setName,                /* in */
             int b,                        /* in */
             long blkLimit,                /* in */
             int B,                        /* in */
             long blkSLimit,               /* in */
             int f,                        /* in */
             long fileLimit,               /* in */
             int F,                        /* in */
             long fileSLimit,              /* in */
             int o,                        /* in */
             int fsetOptions,              /* in */
             int fsetOptionsMask)          /* in */

{

    int             numSets = 0,
                    ret,
                    err,
                    lkHndl,
                    dirLocked = 0,
                    dmnLocked = 0,
                    thisDmnLock,
                    status = -1,
                    i,
                    mountCnt = 0,
                    found_one,
                    setFound = FALSE;
    char            dmnPathName[MAXPATHLEN+1],
                    optstr[MAXPATHLEN] = {0};
    char           *mountDmnName,
                    tmpName[MAXPATHLEN],
                    *tmpName2;
    u32T            setIdx,
                    altSetIdx,
                    userId,
                    cloneUserId,
                    oldQuotaStatus,
                    oldBfSetFlags;
    mlStatusT       sts;
    mlBfSetParamsT  setParams,
                    altSetParams;
    long            oldFileSLimit,
                    oldFileLimit,
                    oldBlkSLimit,
                    oldBlkLimit;
    struct stat     stats;
    struct passwd  *pwd;
    struct group   *grp;
    struct statfs  *mountTbl = NULL;
    char           *mountSetName;
    int            fsetOptionsMaskOrig=0;


    init_event(&advfs_event);

    lkHndl = lock_file( MSFS_DMN_DIR, LOCK_EX, &err );
    if (lkHndl < 0) {
        fprintf( stderr, catgets(catd, 1, 1, "%s: error locking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        goto _error;
    }

    dirLocked = 1;

    /*
     * Verify that the domain exists and its pathname is in the domain directory.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, dmnName);
    err = lstat (dmnPathName, &stats);
    if( err != 0 ) {
        if( errno == ENOENT ) {
            fprintf(stderr, catgets(catd, 1, 2, "%s: domain directory '%s' does not exist\n"),
                     Prog, dmnPathName);
        } else {
            fprintf(stderr, catgets(catd, 1, 3, "%s: error getting '%s' stats; [%d] %s\n"),
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
                 catgets(catd, 1, 4, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 5, "%s: error locking '%s'.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 6, "%s: error = [%d] %s\n"),
                     Prog, err, ERRMSG(err));
        }
        goto _error;
    }

    dmnLocked = 1;

    mountCnt = getfsstat (0, 0, MNT_NOWAIT);
    if (mountCnt <= 0) {
        fprintf (stderr, catgets(catd, 1, 43, "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 42, "%s: mount count = %d\n"), Prog, mountCnt);
        goto _error;
    }

    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
        fprintf (stderr, catgets(catd, 1, 41, "%s: can't allocate file system mount table.\n"), Prog);
        goto _error;
    }

    /*
     * Because the caller has the domain locked, no more advfs bitfile
     * sets can be mounted for now.  "mountTbl" will be up to date relative to
     * advfs file systems.
     */

    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 40, "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 6, "%s: error = [%d] %s\n"),
                 Prog, errno, ERRMSG (errno));
        goto _error;
    }

    for (i = 0, found_one = 0; i < mountCnt; i++) {

        /*
         * NOTE: strtok() replaces the separator character with a null.
         */
        mountDmnName = (char *) strtok (mountTbl[i].f_mntfromname, "#");
        if ((mountTbl[i].f_type == MOUNT_MSFS) &&
            (strcmp (dmnName, mountDmnName) == 0)) {

            found_one++;
        }
    } /* end for */

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

            if (setParams.cloneId != 0) {
                /* can't change quotas or options for clone filesets */
                fprintf( stderr, catgets(catd, 1, 7, "%s: clone fileset '%s' not changed\n"),
                         Prog, setParams.setName );
                continue;

            } else {

                if (o) {
                    /* Clear all specified flags */
                    setParams.bfSetFlags &= ~fsetOptionsMask;  

                    /* Set the specified flags */
                    setParams.bfSetFlags |= fsetOptions;       

                    advfs_event.domain = dmnName;
                    advfs_event.fileset = setName;
                    if (setParams.bfSetFlags & BFS_OD_NOFRAG) {
                        strcat(optstr,"nofrag ");
                    } else {
                        strcat(optstr,"frag ");
                    }
                    if (setParams.bfSetFlags & BFS_OD_OBJ_SAFETY) {
                        strcat(optstr,"objectsafety ");
                    } else {
                        strcat(optstr,"noobjectsafety ");
                    }                        
                    advfs_event.options = optstr;
                    advfs_post_user_event(EVENT_FSET_OPTIONS, 
                                          advfs_event, Prog);
                    advfs_event.options = NULL;
                }


                if (f) {
                    setParams.fileHLimit = fileLimit;
                    advfs_event.domain = dmnName;
                    advfs_event.fileset = setName;
                    advfs_event.fileHLimit = fileLimit;
                    advfs_post_user_event(EVENT_FSET_QUOTA_HFILE_LIMIT, advfs_event, Prog);
                    advfs_event.fileHLimit = NULL;
                }

                if (b) {
                    setParams.blkHLimit = blkLimit * 2; /* 1k -> 512 */
                    advfs_event.domain = dmnName;
                    advfs_event.fileset = setName;
                    advfs_event.blkHLimit = blkLimit;
                    advfs_post_user_event(EVENT_FSET_QUOTA_HBLK_LIMIT, advfs_event, Prog);
                    advfs_event.blkHLimit = NULL;
                }

                if (F) {
                    setParams.fileSLimit = fileSLimit;
                    advfs_event.domain = dmnName;
                    advfs_event.fileset = setName;
                    advfs_event.fileSLimit = fileSLimit;
                    advfs_post_user_event(EVENT_FSET_QUOTA_SFILE_LIMIT, advfs_event, Prog);
                    advfs_event.fileSLimit = NULL;
                }

                if (B) {
                    setParams.blkSLimit = blkSLimit * 2; /* 1k -> 512 */
                    advfs_event.domain = dmnName;
                    advfs_event.fileset = setName;
                    advfs_event.blkSLimit = blkSLimit;
                    advfs_post_user_event(EVENT_FSET_QUOTA_SBLK_LIMIT, advfs_event, Prog);
                    advfs_event.blkSLimit = NULL;
                }

                sts = msfs_set_bfset_params_activate(dmnName, &setParams);

                if (fsetOptionsMaskOrig) {
                    fsetOptionsMask = fsetOptionsMaskOrig;
                }
            }
        }
    } while (sts == EOK);

    if (sts != E_NO_MORE_SETS) {
        fprintf(stderr, catgets(catd, 1, 24, "%s: can't change fileset info\n"),
                 Prog );
        fprintf(stderr, catgets(catd, 1, 25, "%s: error = %s\n"),
                 Prog, BSERRMSG( sts ) );
        goto _error;
    }

    if ((setName != NULL) && !setFound) {
        fprintf( stderr, catgets(catd, 1, 26, "%s: fileset '%s' does not exist in domain '%s'\n"),
                 Prog, setName, dmnName );
	status = -2;
	goto _error;
    }

    if (numSets == 0) {
        fprintf( stderr, catgets(catd, 1, 27, "%s: domain '%s' has no filesets\n"), Prog, dmnName );
    }

    /* Need to unlock both locks */
    if (unlock_file( thisDmnLock, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 28, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, dmnPathName, err, ERRMSG( err ) );
        dmnLocked = 0;
        goto _error;
    }


    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf( stderr, catgets(catd, 1, 28, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        dirLocked = 0;
        goto _error;
    }

    return numSets;

_error:

    if (dmnLocked) {
        if (unlock_file( thisDmnLock, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 28, "%s: error unlocking '%s'; [%d] %s\n"),
                 Prog, dmnPathName, err, ERRMSG( err ) );
        } else {
        }
    }

    if (dirLocked) {
        if (unlock_file( lkHndl, &err ) == -1) {
            fprintf( stderr, catgets(catd, 1, 28, "%s: error unlocking '%s'; [%d] %s\n"),
                     Prog, MSFS_DMN_DIR, err, ERRMSG( err ) );
        }
    }

    return status;
}

/*
 *  Get (or add) options to the option string
 *
 *  This routines takes a pointer to a string of
 *  options (*new) and copies (or appends) the string
 *  to a save area (**old).
 * 
 */

void
add_options(char **old, char *new)
{
    char *p;

    /*
     *  If we already have some options saved away,
     *    Allocate new memory for the combined option strings
     *    Combine the option strings into the save area
     *    Free the old memory
     *    Store the pointer to the new option save area
     */
     
    if (*old) {
        p = (char *) malloc(strlen(*old)+strlen(new)+2);
        if (!p) {
            fprintf( stderr, catgets(catd, 1, NMALLOC, "%s: insufficient memory.\n"), Prog );
            exit(1);
        }
        sprintf(p, "%s,%s", *old, new);
        free(*old);
        *old = p;

    /*
     *  Else, no options are already saved,
     *    Allocated memory for the option string
     *    Copy the option string to the save area
     *    Store the pointer to the option save area
     */
     
    } else {
        p = (char *) malloc(strlen(new)+1);
        if (!p) {
            fprintf( stderr, catgets(catd, 1, 22, "%s: insufficient memory.\n"), Prog );
            exit(1);
        }
        strcpy(p, new);
        *old = p;
    }
}

/*
 *  Set the fileset options flag based on the input flags, also
 *  set a mask to indicate what flags are being manipulated.
 *
 *  This routine takes the string of options provided by the user
 *  and sets the appropriate fileset flags.  A mask indicating
 *  what flags are being maipulated is created so that only
 *  those bits will be modified.
 *
 *  The options are processed in the order they were input on the
 *  command line and the last specified option will override any
 *  previous versions of itself.
 *
 *  set_options returns TRUE if no problems are encountered, or
 *  FALSE if an invalid option argument is found.
 *
 */

int
set_options (char *options, u32T *fsetOptions, u32T *fsetOptionsMask, char *domain)
{
    char *optptr;
    char *optbuf;

    /*
     *  Allocate memory to hold a working copy of the option string
     */    
    optbuf = (char *) malloc(strlen(options)+1);    
    strcpy(optbuf, options);

    /*
     *  Process the "," delimited options one at a time.
     */
        
    for (optptr = strtok(optbuf, ","); optptr; optptr = strtok((char *)NULL, ",")) {

            
        if (!strcmp(optptr, "nofrag")) {
            *fsetOptions |= BFS_OD_NOFRAG;
            *fsetOptionsMask |= BFS_OD_NOFRAG;
            
        } else if (!strcmp(optptr, "frag")) {
            *fsetOptions &= ~BFS_OD_NOFRAG;
            *fsetOptionsMask |= BFS_OD_NOFRAG;
            
        } else if (!strcmp(optptr, "objectsafety")) {
            *fsetOptions |= BFS_OD_OBJ_SAFETY;
            *fsetOptionsMask |= BFS_OD_OBJ_SAFETY;

        } else if (!strcmp(optptr, "noobjectsafety")) {
            *fsetOptions &= ~BFS_OD_OBJ_SAFETY;
            *fsetOptionsMask |= BFS_OD_OBJ_SAFETY;
        /*
         *  invalid option
         */
                         
        } else {
            fprintf( stderr, catgets(catd, 1, INVALOP, "%s: error, unknown -o argument '%s'.\n"),
                                     Prog, optptr );
            free(optbuf);
            return FALSE;             
        }
    }
    free(optbuf);

    return TRUE;
}


/*
 *  multiple_filesets() returns TRUE if there are more than one fileset in the domain,
 *                      and FALSE if there aren't.
 *
 */
int
multiple_filesets (char *domain)
{
    int             ret = 0,
                    filesetCnt = 0;
    statusT         sts = EOK;
    u32T            setIdx = 0,           /* fileset index, start with first set */
                    userId;
    mlBfSetParamsT  setParams;

    /*
     *  Use  msfs_fset_get_info() to loop through and count the filesets.
     */

    while (sts == EOK) {
        sts = msfs_fset_get_info( domain, &setIdx, &setParams, &userId, 0 );
        if (sts == EOK) {
           if (++filesetCnt > 1) {
               return (TRUE);
           }
        }
    }
    return (FALSE);
}

/* end of chfsets.c */
