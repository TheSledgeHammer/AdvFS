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
 *    MegaSafe Storage System
 *
 * Abstract:
 *
 *    rmvol.c
 *    This module implements the "remove volume" utility.  It removes a
 *    volume from a domain by moving all the metadata and extents to
 *    other disks in the domain.
 *
 *    Note:  If rmvol exits with error 1, an error message must be written to stderr
 *           because of the rsh implementation of cluster support. 
 *
 * Date:
 *
 *    Thu Feb 11 07:34:05 1993
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: rmvol.c,v $ $Revision: 1.1.37.4 $ (DEC) $Date: 2006/08/29 12:56:13 $";
#endif

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/signal.h>
#include <sys/disklabel.h>

#include <io/common/devgetinfo.h>
#include <sys/ioctl.h>
#include <sys/devio.h>

#include <sys/mount.h>
#include <overlap.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "rmvol_msg.h"
#include <msfs/advfs_evm.h>
#include <sys/clu.h>
#include <cfs/cfs_user.h>
#include <clua/clua.h>
#include <syslog.h>

#define BUFSIZE 8192
#define RETRY_MAX 8

nl_catd catd;

extern int errno;
extern char *sys_errlist[];
extern  int clu_adm_lock(int lock_oper);

typedef int boolean;
#define TRUE 1
#define FALSE 0

/* The include file overlap.h defines ERRMSG as 3, so to keep from */
/* having redefinition of ERRMSG during compile time I'm changing  */
/* the local define to ERRORMSG. */

#define ERRORMSG(err) sys_errlist[err]

/*
 * NOTE:  The following two macros were copied from the kernel - bs_stripe.h.
 */

/*
 * This macro converts a bitfile relative page to an extent map
 * relative page.  Before it does the conversion, it must determine
 * which stripe extent map maps the specified bitfile page.
 */
#define BFPAGE_TO_XMPAGE(pageOffset, segmentCnt, segmentSize) \
  ( ( ( (pageOffset) / ((segmentCnt) * (segmentSize)) ) * (segmentSize) ) + \
   ((pageOffset) % (segmentSize)) )

/*
 * This macro converts an extent map relative page to a bitfile relative
 * page.
 */
#define XMPAGE_TO_BFPAGE(mapIndex, pageOffset, segmentCnt, segmentSize) \
  ( ((mapIndex) * (segmentSize)) + \
   ( ((pageOffset) / (segmentSize)) * ((segmentCnt) * (segmentSize)) ) + \
   ((pageOffset) % (segmentSize)) )

typedef struct {
    u32T pageOffset;
    u32T pageCnt;
    u32T holePageCnt;
} xtntDescT;

typedef struct setHdr {
    mlBfTagT setTag;
    int mountedFlag;
    int unmountFlag;
    char *mntOnName;
    char *dotTagsPath;
    int fd;
} setHdrT;

char *sh_cmd="/sbin/sh -c ";

/* Full path name of rmvol.  For cluster use with rsh. */
static char *FullPathProg="/usr/sbin/rmvol";
static char *dotTags = "/.tags";
static char *Prog;

static int forceFlag = 0;                         /* flags */
static int unmountFlag = 0;
static int verboseFlag = 0;
static int veryVerboseFlag = 0;
static int userSpaceCheckFlag = 0;                /* -s indicator */

static int clu_locked = 0;                        /* cluster lock */
static int thisDmnLock = 0;                       /* lock for dmnName */
static int masterDmnLock = 0;                     /* lock for /etc/fdmns */ 
static int unlockThisDmnFlag = 0;                 /* lock for dmnName */
static int unlockMasterDmnFlag = 0;               /* lock for /etc/fdmns */

static char         *dmnName;                     /* domain vars */
static mlDmnParamsT dmnParams;
static char         dmnPathName[MAXPATHLEN+1];

static mlServiceClassT targetSvcClass;            /* sig handler vars */
static u32T            targetVolIndex;
static mlBfDomainIdT   domainId;

static int     setHdrCnt = 0;
static setHdrT *setHdrTbl = NULL;

static advfs_ev advfs_multivol_event;
static advfs_ev advfs_onevol_event;

static struct {
                                                             setHdrT *setHdr;
    mlBfTagT bfTag;
    char pathName[MAXPATHLEN+1];
} currentPath;

static int numVols = 0;                    /* number of vols to be removed */
static char numVolsStr[4];                 /* used with EVM  */
static int numVolsLeftToRem = 0;           /* number of vols in the vector */

static struct volToRemove **volsToRem;     /* vector for all vols to remove */
static struct volToRemove **origVolsToRemPtr;  /* save original vector ptr */
static u32T numFreeClustersAvailable;       /* Available free space */

struct volToRemove {                       /* volume structure */   
    char             *devName;             /* fully qualified device name */
    u32T             volIndex;             /* domain volume number - 
                                              static as vols are removed */
    u32T             volIndexI;            /* vector index in domain - 
                                              dynamic as vols are removed */ 
    mlServiceClassT  serviceClass;         /* service class volume is in */ 
    char             volPathName[MAXPATHLEN+1];  /* full path name of volume */
    char             volLink[MAXPATHLEN+1];      /* used by readlink to check 
                                                    symlink */
    int              addVolSCFlag;         /* if 1, volume must be added 
                                              back into the SC on error  */
};


/*
 * Private prototypes.
 */

static
int
remove_volumes       ();

static
int
do_remote_server_exec(int argc,                     /* in  */
                      char *argv[]);                /* in  */
  
static
int 
get_info_for_all_vols(u32T *volList,                /* out */
                      int *volCnt);                 /* out */

static
int 
remove_one_vol       (struct volToRemove *vol);     /* in */

static
int
translate_volname_to_pathname(char *devName,            /* in */
                              char *retPathName);       /* out */

static
int
switch_root_tagdir  (int volIndexI,                 /* in  */
                     int volIndexCnt,               /* in  */
                     u32T *volIndex);               /* in  */

static
int
switch_log           (int volIndexI,                /* in */
                      int volIndexCnt,              /* in */
                      u32T *volIndex);              /* in */

static
int
move_metadata        (mlBfDomainIdT domainId,       /* in  */
                      u32T volIndex,                /* in  */
                      int *retFileCnt);             /* out */

static
mlStatusT
move_normal_metadata (int fd,                       /* in  */
                      mlBfTagT bfSetTag,            /* in  */
                      mlBfTagT bfTag,               /* in  */
                      u32T volIndex,                /* in  */
                      mlMetadataTypeT mtype,        /* in  */
                      int supports_xtnts_in_prim);  /* in  */

static
mlStatusT
move_stripe_metadata (int fd,                       /* in  */
                      mlBfTagT bfSetTag,            /* in  */
                      mlBfTagT bfTag,               /* in  */
                      u32T targetVolIndex,          /* in  */
                      i32T segmentCnt,              /* in  */
                      i32T segmentSize);            /* in  */

static
u32T
calc_bfpagecnt       (u32T bfPageOffset,            /* in  */
                      u32T xmPageCnt,               /* in  */
                      int xtntMapIndex,             /* in  */
                      i32T segmentCnt,              /* in  */
                      i32T segmentSize);            /* in  */

static
mlStatusT
migrate              (int fd,                       /* in  */
                      u32T srcVolIndex,             /* in  */
                      u32T bfPageOffset,            /* in  */
                      u32T bfPageCnt,               /* in  */
                      u32T dstVolIndex);            /* in  */

static
mlStatusT
create_xtnt_desc_list (int fd,                      /* in  */
                       u32T volIndex,               /* in  */
                       int xtntMapIndex,            /* in  */
                       int *retXtntDescCnt,         /* out */
                       xtntDescT **retXtntDesc      /* out */
                       );

static
mlStatusT
extend_xtnt_desc_list (int *maxCnt,                 /* in/out */
                       xtntDescT **xtntDesc);       /* in/out */

static
void
display_tag           (mlBfTagT bfSetTag,           /* in  */
                       mlBfTagT bfTag);             /* in  */

static
int
mount_bitfile_sets    (char *dmnName,               /* in  */
                       mlBfTagT rootBfSetTag);      /* in  */

static
int
unmount_bitfile_sets  ();
 
static
setHdrT *
insert_set_tag        (mlBfTagT setTag);            /* in  */

static
setHdrT *
find_set_tag          (mlBfTagT setTag);            /* in  */

static
char *
get_current_path_name (void);

/*
 * sigHandler
 *
 * This is the remove volume signal handler.  It gives this program a chance
 * to cleanup before quiting.  Specifically, it unmounts bitfile sets, adds the
 * disks back into the service class and unlocks the domain.
 */
void sigHandler (int signal)                        /* in */   
{    
    int err;
    int i;
    mlStatusT sts;
    
    fprintf (stderr, catgets(catd, 1, 1, "%s: Stopping rmvol operation...\n"), Prog);
    
    switch (signal) {
    case SIGINT:
    case SIGQUIT:
    case SIGPIPE:
    case SIGTERM:
        break;
        
    default:
        fprintf (stderr, catgets(catd, 1, 2, "%s: Unexpected signal: %d\n"), Prog, signal);
        break;
    }  /* end switch */
    
    if (clu_locked) {
        clu_adm_lock(CLU_ADM_UNLOCK);
        clu_locked = 0;
    }
    
    /* Put all vols in vector back into the SC */
    for (i = 0; i < numVolsLeftToRem; i++) {
        if (volsToRem[i]->addVolSCFlag) {
            sts = advfs_add_vol_to_svc_class (
                dmnName,
                volsToRem[i]->volIndex,
                volsToRem[i]->serviceClass
                );
        
            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 3, 
                         "%s: Can't add volume '%s' back into svc class\n"), 
                         Prog, volsToRem[i]->devName);            
                fprintf (stderr, catgets(catd, 1, 102, 
                         "use '/sbin/chvol -A %s %s' to reactivate the \
                          volume\n"), volsToRem[i]->devName, dmnName);
            } else {
                volsToRem[i]->addVolSCFlag = 0;
            }
        }
    }
    
    if (unmountFlag != 0) {
        (void)unmount_bitfile_sets ();
        unmountFlag = 0;
    }
    
    /* Release locks if necessary */
    if (unlockMasterDmnFlag != 0) {
        unlockMasterDmnFlag = 0;

        err = unlock_file (masterDmnLock, &err);
        if (err < 0) {
            fprintf (stderr, catgets(catd, 1, 27, 
                     "%s: Error unlocking '%s'\n"), Prog, MSFS_DMN_DIR);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRORMSG (err));
        }
    }

    if (unlockThisDmnFlag != 0) {
        unlockThisDmnFlag = 0;
        err = unlock_file (thisDmnLock, &err);
        if (err < 0) {
            fprintf (stderr, catgets(catd, 1, 27, 
                     "%s: Error unlocking '%s'\n"),Prog, MSFS_DMN_DIR);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRORMSG (err));
        }
    }

    /* Exit messages and EVM posts */
    fprintf (stdout, catgets(catd, 1, 8, 
             "%s: Can't remove volume '%s' from domain '%s'\n"), 
             Prog, volsToRem[0]->devName, dmnName);
    advfs_post_user_event(EVENT_FDMN_RMVOL_ERROR, advfs_onevol_event, Prog);

    fprintf (stderr, catgets(catd, 1, 107, 
             "%s: Can't remove %d volume(s) from domain '%s'\n"),
             Prog, numVolsLeftToRem, dmnName); 
    advfs_post_user_event(EVENT_FDMN_MULTI_RMVOL_ERROR, advfs_multivol_event, 
                          Prog);
    
    /* free all memory in vector */
    while (numVolsLeftToRem > 0) {
        free (volsToRem[0]->devName);
        free (volsToRem[0]);
        volsToRem++;                    /* move beg of vector */
        numVolsLeftToRem--;
    }
    free (origVolsToRemPtr);

    exit (1);

}  /* end sigHandler */


/*
 *  Describes correct command line usage for rmvol utility.
 */
static
void
usage (void)
{
        fprintf (stderr, catgets(catd, 1, USAGE, 
                 "usage: %s [-f][-v][-s] special... domain\n"), 
                 Prog);
}



/*
 *  main
 *
 *  This function parses the rmvol command line, does preliminary error
 *  checking, initializes the in-memory data structures (volsToRem vector), 
 *  and addresses cluster issues. remove_volumes() is called inside this 
 *  function, which executes the meat of the utility.
 */
main (int argc,
      char *argv[]) 
{
    int c, i;
    int err;
    extern int getopt();
    extern char *optarg;
    extern int optind;
    extern int opterr;
    char specialBlock[MAXPATHLEN];
    char specialChar[MAXPATHLEN];
    char lsm_dev[MAXPATHLEN+1];
    char *temp, *vol, *dg;
    struct stat stats; 

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_RMVOL, NL_CAT_LOCALE);


    /* store only the file name part of argv[0] */
    if ((Prog = (char *) strrchr (argv[0], '/')) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }

    /* check for root */
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,93,
                "\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    err = msfs_check_on_disk_version(BFD_ODS_LAST_VERSION);
    if (err != EOK) {
        fprintf(stderr, catgets(catd, 1, ONDISK,
                "%s: This utility can not process the on-disk structures \
                on this system.\n"), Prog);
        exit(1);
    }

    /*
     * Get user-specified command line arguments.
     */
 
    while ((c = getopt (argc, argv, "fvsV")) != EOF) {
        switch (c) {

        case 'f':
            forceFlag = 1;
            break;

        case 'v':
            verboseFlag = 1;
            break;

        case 's':
            userSpaceCheckFlag = 1;
            break;

        case 'V':
            verboseFlag = 1;
            veryVerboseFlag = 1;
            break;

        default:
            usage ();
            exit (1);
        }
    }  /* end while */


    /*
     * We no longer have a maximum limit on the number of arguments, so
     * check for the minimum number
     */
    if (optind > (argc - 2)) {
        /* we don't have minimum number of args */
        usage ();
        exit (1);
    }

    /* set numVols and allocate space for volsToRem vector */
    numVolsLeftToRem = numVols = (argc - 1) - optind;

    volsToRem = (struct volToRemove **) 
        (malloc (sizeof(struct volToRemove *) * numVols));

    /* save pointer to volsToRem[] for freeing later */
    origVolsToRemPtr = volsToRem;
      
    /* set up each vol struct */
    for (i = 0; optind < (argc - 1); optind++, i++) {
        volsToRem[i] = 
            (struct volToRemove *) malloc (sizeof(struct volToRemove));

        /*
         * Check the block special device name
         * The fsl_to_xx routines provide the functionality whereby
         * the user need not specify the fully qualified device path.
         * For example:  dsk2g instead of /dev/disk/dsk2g
         *
         * fsl_to_raw_name() will convert the input to a character
         * (raw) device as well as fully qualify the pathname if needed.
         * Unfortunaltey, it can return non zero status when it shouldn't.
         *
         * If the user specified just the special file name
         *   Use fsl_to_raw_name to process the input 
         *   Use fsl_to_block_name to convert back to block special
         *   If the fsl_to_block_name returned error
         *     just use the input arg as specified - 
         *     volName points to argv[optind]
         *   else
         *     use input as processed  - volName points to specialBlock
         * else
         *   just pass along the argument as input
         */

        if (strrchr(argv[optind], '/' ) == NULL) {

            bzero(specialChar, MAXPATHLEN);
            bzero(specialBlock, MAXPATHLEN);

            fsl_to_raw_name(argv[optind], specialChar, MAXPATHLEN);

            if (fsl_to_block_name(specialChar, specialBlock, MAXPATHLEN) 
                == NULL) {
              if ((vol = strrchr(argv[optind], '.' )) != NULL) {
                strcpy(lsm_dev, argv[optind]);
                dg = strtok_r(lsm_dev, ".",&temp);
                sprintf(specialBlock, "/dev/vol/%s/%s", dg, vol+1);
                volsToRem[i]->devName = strdup(specialBlock);
              } else { 
                volsToRem[i]->devName = strdup(argv[optind]);
              }
            } else {
                volsToRem[i]->devName = strdup(specialBlock);
            }
        } else { 
            /* Device name entered was a full pathname. 
             * Make sure it exists before continuing.
             */
            err = lstat (argv[optind], &stats);
            if (errno == ENOENT) {
                fprintf (stderr, catgets(catd, 1, 105, 
                         "%s: '%s' does not exist in domain\n"), 
                         Prog, argv[optind]);
                goto _error1;
            }
            volsToRem[i]->devName=strdup(argv[optind]);
        }

        /* initialize other fields */
        volsToRem[i]->volIndex = -1;
        volsToRem[i]->volIndexI = -1;
        volsToRem[i]->addVolSCFlag = 0;
        bzero (volsToRem[i]->volPathName, MAXPATHLEN+1);
        bzero (volsToRem[i]->volLink, MAXPATHLEN+1);
    }


    /* domain is last arg */
    dmnName = argv[argc - 1];

    if (clu_is_member()) {
 
        /*
         * If in a cluster, we need to determine the server
         * and execute either locally or remotely as appropriate.
         *
         * This function will determine which node is the server and 
         * return 0 if the server is local.  Otherwise, the function 
         * will execv a remote rmvol on the remote server, wait for 
         * the remote call to exit, and return 1 if successful.
         * Returns -1 on error.
         */
        err = do_remote_server_exec(argc, argv);
        if (err == -1) {
            /*
             * an error occured: free all memory and exit with error
             */
            goto _error1;
        } else if (err == 1){
            /*
             * remote rmvol successful: free all memory and exit with success
             */
            while (numVolsLeftToRem > 0) {
                free (volsToRem[0]->devName);
                free (volsToRem[0]);
                volsToRem++;                    /* move beg of vector */
                numVolsLeftToRem--;
            }
            free (origVolsToRemPtr);
            return 0;
        } 
    }

    /* Initialize the multi-volume EVM */
    init_event(&advfs_multivol_event);
    sprintf(numVolsStr, "%d", numVols);
    advfs_multivol_event.ev_info_str = numVolsStr;
    advfs_multivol_event.domain = dmnName;
    
    /*
     * Disable interrupts.
     */
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    fprintf (stdout, catgets(catd, 1, 113, 
             "%s: Removing %d volume(s) from domain '%s'\n"), 
             Prog, numVols, dmnName); 
    (void)fflush(stdout);            
    
    advfs_post_user_event(EVENT_FDMN_MULTI_RMVOL_START, advfs_multivol_event, Prog);

    /* Do the meat of the rmvol utility */
    err = remove_volumes();
    if (err != 0) {
        goto _error;
    }
    free (origVolsToRemPtr);

    fprintf (stdout, catgets(catd, 1, 114, 
             "%s: Removed %d volume(s) from domain '%s'\n"), 
             Prog, numVols, dmnName); 
    (void)fflush(stdout);             
       
    advfs_post_user_event(EVENT_FDMN_MULTI_RMVOL_STOP, advfs_multivol_event, Prog);

    return 0;

 _error:

    fprintf (stderr, catgets(catd, 1, 107, 
             "%s: Can't remove %d volume(s) from domain '%s'\n"), 
             Prog, numVolsLeftToRem, dmnName); 
      
    advfs_post_user_event(EVENT_FDMN_MULTI_RMVOL_ERROR, advfs_multivol_event, Prog);

 _error1:    
    /* free all memory in vector */
    while (numVolsLeftToRem > 0) {
        free (volsToRem[0]->devName);
        free (volsToRem[0]);
        volsToRem++;                    /* move beg of vector */
        numVolsLeftToRem--;
    }
    free (origVolsToRemPtr);

    exit (1);

}  /* end main */


/*
 * remove_volumes
 *
 * This function is the meat of the rmvol utility.  Domain information 
 * is gathered, all volume information is gathered using
 * get_info_for_all_vols(), all volumes are removed from the 
 * service class, and remove_one_vol() is called for each requested 
 * volume.  If any subordinate function returns an error, this function 
 * handles all cleanup work.
 *
 * The function returns a zero for success or a one if an error occurs. 
 */

static
int
remove_volumes()
{
    int         foundFlag; 
    int         err; 
    int         err2; 
    int         i;
    int         rmI;
    vdIndexT    vdTagDir, vdLog;
    int         dmnVolCnt;
    u32T        *dmnVolList = NULL;
    mlStatusT   sts;
    struct stat stats;

    /*
     * Lock the master domain directory.  This synchronizes us with domain 
     * and bitfile set creation/deletion.
     */
    masterDmnLock = lock_file (MSFS_DMN_DIR, LOCK_EX, &err);
    if (masterDmnLock < 0) {
        fprintf (stderr, catgets(catd, 1, 9, "%s: error locking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRORMSG (err));
        goto _error;
    }
    unlockMasterDmnFlag = 1;

    /*
     * Verify that the domain exists and its pathname is in the domain 
     * directory.  
     */
    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, dmnName);

    err = lstat (dmnPathName, &stats);
    if (err != 0) {
        if (errno == ENOENT) {
            fprintf (stderr, catgets(catd, 1, 11, "%s: '%s' domain does not exist\n"),
                     Prog, dmnPathName);
        } else {
            fprintf (stderr, catgets(catd, 1, 12, "%s: Error getting '%s' stats.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, errno, ERRORMSG (errno));
        }
        goto _error;
    }

    /*
     * Lock this domain.
     */

    thisDmnLock = lock_file_nb (dmnPathName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, 1, 13, 
                     "%s: cannot execute. Another AdvFS command is currently \
                     claiming exclusive use of the specified domain.\n"), 
                     Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 9, "%s: error locking '%s'.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRORMSG(err));
        }
        goto _error;
    } else {
        unlockThisDmnFlag = 1;
    }

    /* Obtain domain info */
    sts = msfs_get_dmnname_params (dmnName, &dmnParams);

    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 14, 
                 "%s: Can't get domain parameters\n"), Prog);
        if (sts != -1) {
            fprintf (stderr, catgets(catd, 1, 15, 
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        } else {
            fprintf (stderr, catgets(catd, 1, 15, 
                     "%s: Error = %s\n"), Prog, ERRORMSG (errno));
        }
        goto _error;
    }

     
    /* fill volume vector with info for each vol */
    dmnVolList = (u32T *) malloc (dmnParams.curNumVols * sizeof (u32T));
    if (dmnVolList == NULL) {
        fprintf (stderr, catgets (catd, 1, 19, 
                 "%s: Can't allocate memory for volume index array\n"), Prog);
        goto _error;
    }

    if (get_info_for_all_vols(dmnVolList, &dmnVolCnt))
        goto _error;

    /*
     * Mount all bitfile sets in the domain.  The function also opens each 
     * bitfile set's ".tags" directory to ensure that the set stays mounted 
     * while the volume is removed.
     */
    err = mount_bitfile_sets (dmnName, dmnParams.bfSetDirTag);
    if (err != 0) {
        goto _error;
    }
    unmountFlag = 1;

    /*
     * Remove each volume in the vector from the service class so that mcells 
     * or storage can not be allocated for normal bitfiles on these volumes.  
     * We handle striped bitfiles later.
     */
    for (rmI = 0; rmI < numVolsLeftToRem; rmI++) {
        sts = advfs_remove_vol_from_svc_class (dmnName, 
                                               volsToRem[rmI]->volIndex, 
                                               volsToRem[rmI]->serviceClass
            );
        if (sts != EOK) {
            if (sts == EBAD_VDI) {
                fprintf (stderr,
                         catgets(catd, 1, 24, "%s: An earlier rmvol did not completely remove volume %s.\n"),
                         Prog, volsToRem[rmI]->devName);
                fprintf (stderr,
                         catgets(catd, 1, 25, "Use the 'chvol -A' command to activate volume.\n"));
            } else {
                fprintf (stderr, catgets(catd, 1, 115, "%s: Can't remove volume '%s' from service\n"), Prog, volsToRem[rmI]->devName);
                fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            }
            goto _error;
        } else {
            /* the volume was successfully removed from the SC */
            volsToRem[rmI]->addVolSCFlag = 1;
        }
    }
    
    /*
     * Unlock the master domain directory.  Now, domains and
     * bitfile sets can be created/deleted.
     * Plus, we can show the domain.
     */
    unlockMasterDmnFlag = 0;
    err2 = unlock_file (masterDmnLock, &err);
    if (err2 < 0) {
        fprintf (stderr, catgets(catd, 1, 27, "%s: Error unlocking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRORMSG (err));
        goto _error;
    }

    /* Get cluster lock if applicable */
    if (clu_is_member() && (strcmp(dmnName, "cluster_root") == 0)) {
        if (clu_adm_lock(CLU_ADM_LOCK)) {
            fprintf(stderr, catgets(catd, 1, 97, "%s: clu_adm_lock failed to get lock for cluster root\n"), Prog);
            goto _error;
        }
        clu_locked = TRUE;
    }

    /*
     * If the log or root tagdir is on any of the volumes requested for
     * removal, move the log or root tagdir to a persistent volume before
     * moving any other data.
     */
    vdTagDir = BS_BFTAG_VDI(dmnParams.bfSetDirTag);
    vdLog = BS_BFTAG_VDI(dmnParams.ftxLogTag);
    
    for (i = 0; i < numVolsLeftToRem; i++) {
        if (vdTagDir == volsToRem[i]->volIndex)
            if (switch_root_tagdir(volsToRem[i]->volIndexI, dmnVolCnt, dmnVolList))
                goto _error;
        if (vdLog == volsToRem[i]->volIndex)
            if (switch_log(volsToRem[i]->volIndexI, dmnVolCnt, dmnVolList))
                goto _error;
    }

    free(dmnVolList);

    /* Release cluster lock */
    if (clu_locked) {

        /* Run clu_bdmgr to update clu_bdmgr.conf files */
        if(system("/usr/sbin/clu_bdmgr -u") != 0)
            fprintf(stderr, catgets(catd, 1, 103, 
                    "%s: Unable to save cnx partition info, 'clu_bdmgr -u' \
                    failed\n"), Prog);
        clu_adm_lock(CLU_ADM_UNLOCK);
        clu_locked = 0;
    }

    /* Migrate data off all volumes requested for removal
     * by looping thru the vector.
     */
    while (numVolsLeftToRem > 0) { 

        fprintf (stdout, catgets(catd, 1, 6, "%s: Removing volume '%s' from domain '%s'\n"), Prog, volsToRem[0]->devName, dmnName);

        /* Initialize and LOCK the per volume EVM */
        init_event(&advfs_onevol_event);
        advfs_onevol_event.special = volsToRem[0]->devName;
        advfs_onevol_event.domain = dmnName;
        
        advfs_post_user_event(EVENT_FDMN_RMVOL_LOCK, advfs_onevol_event, Prog);


        if (!remove_one_vol(volsToRem[0])) {

            /* Success: Remove vol from the vd vector, 
             * free memory, and post UNLOCK EVM 
             */
            fprintf (stdout, catgets(catd, 1, 7, "%s: Removed volume '%s' from domain '%s'\n"), Prog, volsToRem[0]->devName, dmnName);

            advfs_post_user_event(EVENT_FDMN_RMVOL_UNLOCK, advfs_onevol_event, Prog);

            free (volsToRem[0]->devName);
            free (volsToRem[0]);
            volsToRem++;                        /* move beg of vector */
            numVolsLeftToRem--;
        }
        else {
            fprintf (stdout, catgets(catd, 1, 8, "%s: Can't remove volume '%s' from domain '%s'\n"), Prog, volsToRem[0]->devName, dmnName);
            advfs_post_user_event(EVENT_FDMN_RMVOL_ERROR, advfs_onevol_event, Prog);
            goto _error;
        }
    }

    unmountFlag = 0;
    if (unmount_bitfile_sets())
        goto _error;


    /* Unlock domain dir */
    unlockThisDmnFlag = 0;
    err2 = unlock_file (thisDmnLock, &err);
    if (err2 < 0) {
        fprintf (stderr, catgets(catd, 1, 27, "%s: Error unlocking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRORMSG (err));
        goto _error;
    }
    
    return 0;

 _error:

    if (clu_locked) {
        clu_adm_lock(CLU_ADM_UNLOCK);
        clu_locked = 0;
    }

    /* Put all vols in vector back into the SC */
    for (i = 0; i < numVolsLeftToRem; i++) {
        if (volsToRem[i]->addVolSCFlag) {
            sts = advfs_add_vol_to_svc_class (
                dmnName,
                volsToRem[i]->volIndex,
                volsToRem[i]->serviceClass
                );
            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 3, "%s: Can't add volume '%s' back into svc class\n"), Prog, volsToRem[i]->devName);            
            }
        }
    }
    

    if (unmountFlag != 0) {
        (void)unmount_bitfile_sets ();
        unmountFlag = 0;
    }

    if (unlockMasterDmnFlag != 0) {
        unlockMasterDmnFlag = 0;
        err2 = unlock_file (masterDmnLock, &err);
        if (err2 < 0) {
            fprintf (stderr, catgets(catd, 1, 27, "%s: Error unlocking '%s'.\n"),
                     Prog, MSFS_DMN_DIR);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRORMSG (err));

        }
    }

    if (unlockThisDmnFlag != 0) {
        unlockThisDmnFlag = 0;
        err2 = unlock_file (thisDmnLock, &err);
        if (err2 < 0) {
            fprintf (stderr, catgets(catd, 1, 27, "%s: Error unlocking '%s'.\n"),
                     Prog, MSFS_DMN_DIR);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRORMSG (err));
        }
    }

    free(dmnVolList);

    return 1;
} 
/* end remove_volumes */


/*
 * get_info_for_all_vols
 *
 * This function gathers information needed to completely fill in a 
 * volToRemove struct for each volume.  This function will make sure 
 * all requested volumes are valid and that enough space is available 
 * on the remaining volumes for complete data migration.
 */
static
int 
get_info_for_all_vols(u32T *volList,       /* out */
                      int *volCnt)         /* out */
{

    mlStatusT         sts; 
    mlVolCountersT    volCounters; 
    mlVolInfoT        volInfo; 
    mlVolIoQParamsT   volIoQParams; 
    mlVolPropertiesT  volProp; 
    u32T              num_free_clusters_needed = 0; 
    u32T              free_clusters = 0; 
    int               i, rmI;
    int               avail_for_migration;

    /*
     *  Get the domain's volume list
     */
    sts = msfs_get_dmn_vol_list (
        dmnParams.bfDomainId, 
        dmnParams.curNumVols, 
        volList,
        volCnt
        );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf(stderr, catgets(catd, 1, 94, 
                                "%s: error: All filesets must be mounted\n"), Prog );
        goto _error;
    } else if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 20, 
                                 "%s: Can't get domain's volume list\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 15, 
                                 "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }
   

    /*
     * Translate each volume name into a special device pathname.  Verify that
     * each volume is a valid device name.
     */

    for (i = 0; i < numVolsLeftToRem; i++) {
        
        sts = translate_volname_to_pathname
            (volsToRem[i]->devName, volsToRem[i]->volPathName);
        if (sts)
            goto _error;

        /* Make sure symlink exists */
        sts = readlink (volsToRem[i]->volPathName, volsToRem[i]->volLink, MAXPATHLEN+1);
        if (sts < 0) {
            fprintf (stderr, catgets(catd, 1, 18, 
                                     "%s: Error getting '%s' symbolic link.\n"),
                     Prog, volsToRem[i]->volPathName);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, errno, ERRORMSG (errno));
            goto _error;
        }

    }

    /* We can not remove all vols in the dmn */
    if (numVolsLeftToRem >= *volCnt) {
        fprintf (stderr, catgets(catd, 1, 21, 
                "%s: At least one volume must remain in the domain\n"), Prog);
        goto _error;
    }

    /* Get info for each vol we are removing */
    for (i = 0; i < *volCnt; i++) {
        sts = advfs_get_vol_params (
            dmnParams.bfDomainId,
            volList[i],
            &volInfo,
            &volProp,
            &volIoQParams,
            &volCounters
            );
        if (sts == EBAD_VDI) {
            fprintf (stderr, catgets
                     (catd, 1, 112, "%s: One or more volumes in the domain are unavailable.\n"), 
                     Prog, i);
            fprintf (stderr, catgets
                     (catd, 1, 110, "%s: Use 'showfdmn %s' to see which volumes are unavailable.\n"), Prog, dmnName);
            fprintf (stderr, catgets
                     (catd, 1, 111, "%s: Then use 'chvol -A' to activate each unavailable volume.\n"), 
                     Prog, i);
            goto _error;

        }
        else if (sts != EOK) {
            fprintf (stderr, catgets
                     (catd, 1, 22, "%s: Can't get volume parameters\n"), 
                     Prog);
            fprintf (stderr, catgets
                     (catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            goto _error;
        }

        /* flag to see if volume can be used for migration */
        avail_for_migration = 1;

        /* 
         * Compare each vol in vector to vol we are currently looking 
         * at in the dmn, volList[i].
         */
        for (rmI = 0; rmI < numVolsLeftToRem; rmI++) {

            if (strcmp (volsToRem[rmI]->volLink, volProp.volName) == 0) {
                /* Matching name! */
                volsToRem[rmI]->volIndexI = i;
                volsToRem[rmI]->volIndex = volInfo.volIndex;
                volsToRem[rmI]->serviceClass = volProp.serviceClass;
                num_free_clusters_needed+=(volInfo.totalClusters - 
                                           volInfo.freeClusters);
                avail_for_migration = 0;
                break;  /* out of for */
            } 
        }  /* end rmI for */

        /* Tally available free space for migration */
        if (avail_for_migration) {
            free_clusters+=volInfo.freeClusters;
            numFreeClustersAvailable = free_clusters;
        }

    }  /* end i for */

    /* 
     * Make sure all vols requested for removal found a matching volume
     * name in the dmn list. If any vol requested for removal is not a vol
     * found in the dmn, rmvol fails.
     */
    for (rmI = 0; rmI < numVolsLeftToRem; rmI++) {
        if ((volsToRem[rmI]->volIndexI == -1) || 
            (volsToRem[rmI]->volIndexI >= *volCnt)) {
            /* Vol not in dmn!   */
            fprintf (stderr, catgets(catd, 1, 23, 
                                     "%s: Volume '%s' is not a member of domain '%s'\n"),
                     Prog, volsToRem[rmI]->devName, dmnName);
            goto _error;
        }
    }

    /* 
     * Make sure we have enough space to migrate all volumes 
     * if the user has specified the space check flag 
     */
    if (userSpaceCheckFlag) {
        if (free_clusters < num_free_clusters_needed) {
            fprintf (stderr, catgets(catd, 1, 109, 
                     "%s: Not enough free space for complete migration of all volumes requested for removal.\n"), Prog); 
            fprintf (stderr, catgets(catd, 1, 108, 
                     "    Free space needed:    %dK\n    Free space available: %dK\n"), num_free_clusters_needed*8, free_clusters*8);
            goto _error;
        }
    }

    return 0;

 _error:
    
    return 1;

} /* end get_info_for_all_vols() */


/*
 * remove_one_vol
 *
 * This function will completely remove one volume from the domain.  
 * This includes calling appropriate functions to move all the data and
 * metadata off the volume, delete the symbolic link, relabel the 
 * partition on the disk to UNUSED, and all other associated cleanup work 
 * to completely remove the volume.
 */
static
int 
remove_one_vol(struct volToRemove *vol)       /* in */
{
    u32T forceMigrateFlag = 0;

    mlStatusT         sts; 
    int               err;
    int               fileCnt;

    /*
     * Set values needed by the signal handler and enable interrupts.
     */
    domainId = dmnParams.bfDomainId;
    targetVolIndex = vol->volIndex;
    targetSvcClass = vol->serviceClass;

    signal (SIGINT, sigHandler);
    signal (SIGQUIT, sigHandler);
    signal (SIGPIPE, sigHandler);
    signal (SIGTERM, sigHandler);

    /*
     * Move all normal (non-reserved) metadata and storage off of the
     * disk.
     */
    err = move_metadata (dmnParams.bfDomainId, vol->volIndex, &fileCnt);
    while ((err == 0) && (fileCnt > 0)) {
        /*
         * Even though the disk has been removed from service,
         * the megasafe kernel does not prevent the allocation of mcells
         * on this disk.  It does its best but some allocations slip thru
         * if the system is busy.  So, make multiple passes thru the bmt.
         * This should really be fixed in the kernel so that when the disk
         * is removed from the service class, the caller is guaranteed that
         * no mcells or storage can be allocated on the disk.
         */
        err = move_metadata (dmnParams.bfDomainId, vol->volIndex, &fileCnt);
    }

    /*
     * Disable interrupts.
     */
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    if (err != 0) {
        goto _error;
    }

    /*
     * Get cluster lock if necessary.
     */
    if (clu_is_member() && (strcmp(dmnName, "cluster_root") == 0)) {
        if (clu_adm_lock(CLU_ADM_LOCK)) {
            fprintf(stderr, catgets(catd, 1, 97, "%s: clu_adm_lock failed to get lock for cluster root\n"), Prog);
            goto _error;
        }
        clu_locked = TRUE;
    }

 retry_rem_vol:
    /*
     * We have already moved the root tagdir and log, if necessary.
     * This function will move any remaining reserved metadata off the disk.
     */
    sts = advfs_remove_volume (dmnParams.bfDomainId,
                               vol->volIndex,
                               forceMigrateFlag
        );
    if (sts != EOK) {
        if ( (!forceMigrateFlag) && (sts == E_INVOLUNTARY_ABORT) ) {
            forceMigrateFlag = 1;
            goto retry_rem_vol;
        }
        else {
            fprintf (stderr, catgets(catd, 1, 34, 
                                     "%s: Can't remove the volume\n"), Prog);
            fprintf (stderr, catgets(catd, 1, 15, 
                                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        }
        goto _error;
    }

    /*
     * Release cluster lock if necessary 
     */
    if (clu_locked) {
        /* Run clu_bdmgr to update clu_bdmgr.conf files */
        if(system("/usr/sbin/clu_bdmgr -u") != 0)
            fprintf(stderr, catgets(catd, 1, 103, "%s: Unable to save cnx partition info, 'clu_bdmgr -u' failed\n"), Prog);
        clu_adm_lock(CLU_ADM_UNLOCK);
        clu_locked = 0;
    }

    /*
     * Take the master domain lock.
     */
    masterDmnLock = lock_file (MSFS_DMN_DIR, LOCK_EX, &err);
    if (masterDmnLock < 0) {
        fprintf (stderr, catgets(catd, 1, 35, "%s: Error locking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRORMSG (err));
        goto _error;
    }
    unlockMasterDmnFlag = 1;

    /*
     * Remove the symbolic link to the special device file.
     */
    err = unlink (vol->volPathName);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 36, "%s: Error deleting '%s' symbolic link.\n"),
                 Prog, vol->volPathName);
        fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                 Prog, errno, ERRORMSG (errno));
        goto _error;
    }

    /* 
     * "Commit" the rmvol for this volume on the domain's transient record
     *  on disk.  This transient record is to indicate that an addvol/rmvol
     *  is in progress.  If the system crashes before linking or unlinking 
     *  the /etc/fdmns/<domain>, the discrepancies will be detected and 
     *  corrected during domain activation. 
     */
    (void)msfs_rem_vol_done(dmnName, vol->devName); /* Ignore status */


    /* 
     * Relabel volume type to UNUSED.
     */    
    err = set_usage (vol->devName, FS_UNUSED, 1);
    if (err != 0) 
    {
        fprintf(stderr, catgets(catd, 1, 92 , "Error from set_usage %d setting disklabel on %s\n"), err, vol->devName);
    }

    /* 
     * Release the master domain lock. 
     */
    unlockMasterDmnFlag = 0;
    err = unlock_file (masterDmnLock, &err);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 27, "%s: Error unlocking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRORMSG (err));
        goto _error;
    }

    return 0;
    
 _error:
    /*
     * Release cluster lock if necessary 
     */
    if (clu_locked) {
        /* Run clu_bdmgr to update clu_bdmgr.conf files */
        if(system("/usr/sbin/clu_bdmgr -u") != 0)
            fprintf(stderr, catgets(catd, 1, 103, "%s: Unable to save cnx partition info, 'clu_bdmgr -u' failed\n"), Prog);
        clu_adm_lock(CLU_ADM_UNLOCK);
        clu_locked = 0;
    }
    return 1;

} /* end remove_one_vol() */


/*
 * translate_volname_to_pathname
 * 
 * Translate the devName into a special device pathname.  Verify that
 * it is a valid device name and return the pathname to the caller
 * in retPathName.
 */

static
int
translate_volname_to_pathname(char *devName,       /* in */
                              char *retPathName    /* out */)
{   
    char volDiskGroup[64];
    char lsmName[MAXPATHLEN+1];
    char newDmnPathName[MAXPATHLEN+1];    
    char oldDmnPathName[MAXPATHLEN+1];    
    char *oldVolPathName;                 
    char *volPathName;                     
    char *subs;                           
    char temp[MAXPATHLEN+1];
    struct stat stats; 
    int err; 

    /* copy dmn paths for use in translation */
    strcpy(oldDmnPathName, dmnPathName);
    strcpy(newDmnPathName, dmnPathName);

    /* get the shortcut dev name from the full path */
    subs = (char *) strrchr (devName, '/');
    if (subs == NULL) {
        fprintf(stderr, catgets(catd, 1, 16, 
                "%s: %s is not the fully qualified device pathname\n"), 
                Prog, devName);
        goto _error;
    }

    if (checklsm(devName, volDiskGroup, lsmName)) {
        sprintf(temp, "/%s.%s", volDiskGroup, lsmName);        
        volPathName = (char *) strcat (newDmnPathName, temp);
    } else {
        volPathName = (char *) strcat (newDmnPathName, subs);
    }
    
    err = lstat (volPathName, &stats);

    /* we did not find the symlink */
    if (err != 0) {
        if (errno == ENOENT) {
            /*
             * We now have the situation were it could be the old
             * style symlink. 
             */
            oldVolPathName = (char *) strcat (oldDmnPathName, subs);
            
            err = lstat (oldVolPathName, &stats);
            
            /* oldVolPathName symlink still not found */
            if (err != 0) {
                if (errno == ENOENT) {
                    fprintf (stderr, catgets(catd, 1, 105, 
                            "%s: '%s' does not exist in domain\n"), 
                             Prog, devName);
                /* stat error with oldVolPathName */
                } else {  
                    fprintf (stderr, catgets(catd, 1, 12, 
                             "%s: Error getting '%s' stats.\n"),
                             Prog, oldVolPathName);
                    fprintf (stderr, catgets(catd, 1, 10, 
                             "%s: Error = [%d] %s\n"),
                             Prog, errno, ERRORMSG (errno));
                }
                goto _error;
            } /* end if lstat(oldvolPathName) does not return successfully */

            strcpy(retPathName, oldVolPathName);
        
            /* stat error with volPathName */
        } else { 
            fprintf (stderr, catgets(catd, 1, 12, "%s: Error getting '%s' stats.\n"),
                     Prog, volPathName);
            fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                     Prog, errno, ERRORMSG (errno));
            goto _error;
        }
    } /* end if lstat(volPathName) does not return successfully */

    
    strcpy(retPathName, volPathName);

    return 0;

 _error:
    return 1;
}


/*
 *  do_remote_server_exec
 *
 *  This function will determine which node is the server and return 0
 *  if the server is local.  Otherwise, the function will execv() a 
 *  remote rmvol on the remote server, wait for the remote call to exit,
 *  and return 1 if the remote rmvol was successful.  
 *  Returns -1 on error.
 *
 *  If any changes are made to this logic, please check
 *  addvol's cfs logic also.
 */

static
int
do_remote_server_exec(int argc,                   /* in  */
                      char *argv[]                /* in  */ 
    )
{
    char *newargv[4];
    char our_node[MAXHOSTNAMELEN + 1];
    char server_name[MAXHOSTNAMELEN + 1];   
    char net_name[MAXHOSTNAMELEN + 1];
    char newarg2[ML_DOMAIN_NAME_SZ + MAXPATHLEN + 25];
    struct clu_gen_info *clugenptr;
    int i, sts, proc_sts=0, fork_sts=0, retval=0;
    int cnt, fd;
    int retry_count = 0;
    char buffer[BUFSIZE];
    char tmpName[MAXPATHLEN];


    retval = clu_get_info(&clugenptr);
    if (retval || (clugenptr == NULL)) {
        fprintf(stderr, catgets(catd, 1, 100,
                                "%s: cluster filesystem error \n"), Prog);
        return -1; 
    }

    bzero(our_node, MAXHOSTNAMELEN+1);
    for (i=0; i<=clugenptr->clu_num_of_members-1; i++) {
        if (clugenptr->my_memberid == clugenptr->memblist[i].memberid) {
            if (clugenptr->memblist[i].cnx_nodename != NULL) {
                strcpy(our_node, clugenptr->memblist[i].cnx_nodename);
            }
            break;
        }
    }
    if (our_node[0] == 0) {
        fprintf(stderr, catgets(catd, 1, 100,
                                "%s: cluster filesystem error \n"), Prog);
        return -1;
    }

 _retry:

    /*
     *  Limit retries to RETRY_MAX times for rsh failures
     */
           
    if (retry_count++ > RETRY_MAX) {
        fprintf(stderr, catgets(catd, 1, 99,
                                "%s: rsh failure, check that the /.rhosts file allows cluster alias access.\n"), Prog);
        return -1;
    }

    sts = cfs_server(our_node, dmnName, server_name);
      
    if (sts == -2) {       /* failover/relocation */
        retry_count = 0;   /* no limit for failover retries */
        goto _retry;
            
    } else if (sts == 0) { /* remote server */
        bzero(net_name, MAXHOSTNAMELEN+1);
        for (i=0; i<=clugenptr->clu_num_of_members-1; i++) {
            if (clugenptr->memblist[i].cnx_nodename == NULL) {
                continue;
            }
            if (!strcmp(clugenptr->memblist[i].cnx_nodename, server_name)) {
                strcpy(net_name, clugenptr->memblist[i].cluintername);
                break;
            }
        }
        if (net_name[0] == 0) {
            fprintf(stderr, catgets(catd, 1, 100,
                                    "%s: cluster filesystem error \n"), Prog);
            return -1;
        }

        /*
         *  The rsh command is used (temporarily) to provide cluster support.
         *  Because the exit code of the rmvol rsh'ed to the current server
         *  cannot be determined, we first try to redirect sterr of the rsh'ed
         *  addvol to a temporary file where when can examine it to determine
         *  success or failure.  Since dealing with files can introduce many
         *  additional failure points, we try to create the temp file before
         *  actually doing the rsh.  If we can't create the file (no space, 
         *  etc), stderr is redirected to stout and we just live with the 
         *  fact that the correct exit code cannot be determined 
         *  (0 is returned).  If the temp file can be created, we redirect 
         *  stderr to it and check it when the rsh fork exits. To fix a 
         *  potential security hole, the file is created with root 
         *  privledges but is truncated before being passed as a parameter
         *  to the rsh'ed rmvol command.
         *
         *
         *  The command here is:
         *    /usr/sbin/rsh rhost "/sbin/sh -c '/usr/sbin/rmvol 
         *                                  special domain 2>>/tempfilename'"
         *  or,
         *    /usr/sbin/rsh rhost "/sbin/sh -c '/usr/sbin/rmvol 
         *                                  special domain 2>&1'"
         * 
         *  (Double quotes are not used but using one argument for what's 
         *  inside the double quotes yields the same effect.)
         */

        newargv[0] = "/usr/bin/rsh";
        newargv[1] = net_name;
        sprintf(newarg2, "%s '%s ", sh_cmd, FullPathProg);
        for (i=1; i<argc; i++) {
            strcat(newarg2, argv[i]);
            strcat(newarg2, " ");
        }
        
        /*
         *  Compose the pathname of the temporary file
         */

        sprintf(tmpName, "/cluster/members/member%i/tmp/rmvol.XXXXXX", clugenptr->my_memberid);

        /*
         *  Try and create the temp file now
         */

        retval = 0;
        if ((fd = mkstemp(tmpName)) < 0) {
            retval = fd;
        } else {
            retval = write(fd, buffer, BUFSIZE);
        }            
        /*
         *  If file created,
         *    redirect to the temp file            
         *  Else,
         *    redirect to stdout
         */
              
        if (retval > 0 ) {
            strcat(newarg2, "2>>");
            strcat(newarg2, tmpName);
            strcat(newarg2, "'");
            /* if we are going to be using the file, truncate it */
            ftruncate(fd, 0);
        } else {
            strcat(newarg2, "2>");
            strcat(newarg2, "&1'");
            tmpName[0] = 0;
        }           
            
        /* Close file if the fd is valid */
        if (fd > 0) {
            close(fd);
        }

        newargv[2] = newarg2;
        newargv[3] = NULL;

        fork_sts = fork();

        /* child of fork() */
        if (!fork_sts) {
            /* 
             * close stderr to prevent rsh error msgs 
             * rmvol stderr redirected
             */
            close(2);       
            execv(newargv[0], newargv);

            /* fork error */
        } else if (fork_sts == -1) {
            fprintf(stderr, catgets(catd, 1, 101,
                                    "%s: forking error \n"), Prog);
            return -1;
     
            /* parent of fork(); will wait for child to exit */
        } else {
            wait(&proc_sts);
            if (proc_sts != 0) {        /* bad rsh */
                goto _retry;        
                /*
                 *  rsh succeeded, check results
                 */                  
            } else {

                /*
                 *  If temp file couldn't be created
                 *    set exit code to 0
                 *    (stderr was redirected to stdout)
                 *
                 *  Else If temp file can't be opened
                 *    (the file should have been created,
                 *     even if it's empty)
                 *    output error message
                 *    set exit code to 1
                 *
                 *  Else, file exists
                 *    If non-zero length,
                 *      write contents to stderr
                 *      set exit code to 1
                 *    remove file
                 */

                if (tmpName[0] == 0) {
                    retval = 1;
                } else if ((fd = open(tmpName, O_RDONLY, 0)) < 0) {
                    fprintf(stderr, catgets(catd, 1, 100,
                                            "%s: cluster filesystem error \n"), Prog);
                    retval = -1;
                } else {
                    retval = 1;
                    /* 
                     * An error occured in the remote rmvol if 
                     * the file contains data.  Print data to stderr. 
                     */
                    while ((cnt = read(fd, buffer, BUFSIZE)) > 0) {
                        write(fileno(stderr), buffer, cnt);
                        retval = -1;
                    }
                    close (fd);
                    remove(tmpName);
                }
                return (retval);

            } /* end successful rsh */
        }  /* end fork() parent */
    } /* end remote server */
    
    else if (sts == -1) { /* cluster error */
        fprintf(stderr, catgets(catd, 1, 100,
                                "%s: cluster filesystem error \n"), Prog);
        return -1;
    }
    
    /* sts == 1: Local server */
    return 0;
} /* end do_remote_server_exec() */


/*
 * The maximum is intentionally kept small because we use a sequential
 * search algorithm when searching for an entry in a table of bitfile
 * descriptors.
 */
#define BF_DESC_MAX 10

/*
 * move_metadata
 *
 * This function moves all, non-reserved bitfile metadata off of the specified
 * disk.
 *
 * Metadata can be classified as either non-extent or extent metadata.  Extent
 * metadata describes a bitfile's allocated disk storage and is located on the
 * same disk as the storage.  The storage described by extent metadata must be
 * moved as well as the metadata itself.
 *
 * Non-extent metadata does not describe any storage and can be independently
 * moved.
 */

static
int
move_metadata (
               mlBfDomainIdT domainId,  /* in */
               u32T volIndex,  /* in */
               int *retFileCnt  /* out */
               )

{

    mlBfDescT bfDesc[BF_DESC_MAX];
    mlBfAttributesT bfAttr;
    int bfDescCnt;
    mlBfInfoT bfInfo;
    char dotTagsPathName[MAXPATHLEN+1];
    int duplFlag;
    int err;
    int fd;
    int fileCnt = 0;
    int i;
    int j;
    u32T nextBfDescId;
    setHdrT *setHdr;
    struct stat stats;
    mlStatusT sts;
    u32T totalPageCnt;
    mlBfDescT uniqBfDesc[BF_DESC_MAX];
    int uniqBfDescCnt;

    /*
     * NOTE:  If we successfully moved something, we shouldn't see it
     * again when calling advfs_get_vol_bf_descs().
     */
        
    nextBfDescId = 0;

    sts = advfs_get_vol_bf_descs (
                                  domainId,
                                  volIndex,
                                  BF_DESC_MAX,
                                  &(bfDesc[0]),
                                  &nextBfDescId,
                                  &bfDescCnt
                                  );
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 37, "%s: Can't get volume file descriptors\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        return 1;
    }
    while (bfDescCnt > 0) {

        /*
         * Assume there are duplicate entries.  Copy the unique entries
         * from the original list to the unique list.
         */

        uniqBfDesc[0] = bfDesc[0];
        uniqBfDescCnt = 1;

        for (i = 1; i < bfDescCnt; i++) {

            if (!ML_BFTAG_RSVD (bfDesc[i].bfTag)) {

                duplFlag = 0;

                for (j = 0; j < uniqBfDescCnt; j++) {
                    if ( ML_BFTAG_EQL(bfDesc[i].bfSetTag, uniqBfDesc[j].bfSetTag) &&
                         ML_BFTAG_EQL(bfDesc[i].bfTag, uniqBfDesc[j].bfTag) )
                    {
                        if ( bfDesc[i].metadataType == uniqBfDesc[j].metadataType ) {
                            /* These can't be two ML_PRIME_MCELL_XTNT_METADATA cells, */
                            /* but from rmvol's point of view, it doesn't matter. */
                            duplFlag = 1;
                            break;
                        }

                        if ( uniqBfDesc[j].metadataType == ML_PRIME_MCELL_XTNT_METADATA &&
                             bfDesc[i].metadataType == ML_NORMAL_METADATA )
                        {
                            /* We already came across the prime mcell. */
                            /* When we call advfs_move_bf_metadata for the */
                            /* prime mcell it will  move this mcell. */
                            duplFlag = 1;
                            break;
                        }
                        if ( uniqBfDesc[j].metadataType == ML_NORMAL_METADATA &&
                             bfDesc[i].metadataType == ML_PRIME_MCELL_XTNT_METADATA )
                        {
                            /* We came across a non extent mcell first and */
                            /* now we've found the prime mcell. Move the */
                            /* prime mcell, this  mcell will follow. Replace */
                            /* the uniqBfDesc entry with the prime mcell. */
                            duplFlag = 2;
                            break;
                        }
                    }
                }  /* end for */

                if (duplFlag == 0) {
                    /*
                     * A new entry.
                     */
                    uniqBfDesc[uniqBfDescCnt] = bfDesc[i];
                    uniqBfDescCnt++;
                } else if ( duplFlag == 2 ) {
                    uniqBfDesc[j] = bfDesc[i];
                }
            }
        }  /* end for */

        for (i = 0; i < uniqBfDescCnt; i++) {

            if (ML_BFTAG_RSVD (uniqBfDesc[i].bfSetTag)) {
                /*
                 * This is a bitfile set.  I can't open its tag directory
                 * via ".tags" without making the bitfile unmountable.
                 * So, bitfile sets are moved by the advfs_remove_volume()
                 * syscall.
                 */
                continue;
            }

            /*
             * This is a normal bitfile.
             */

            setHdr = find_set_tag (uniqBfDesc[i].bfSetTag);
            if (setHdr == NULL) {
                /* print out set name?? */
                fprintf (stderr, catgets(catd, 1, 38, "%s: Can't find file set hdr - tag %d.%d\n"),
                         Prog, uniqBfDesc[i].bfSetTag.num, uniqBfDesc[i].bfSetTag.seq);
                return 1;
            }

            currentPath.setHdr = setHdr;
            currentPath.bfTag = uniqBfDesc[i].bfTag;
            currentPath.pathName[0] = '\0';
 
            sprintf (
                     dotTagsPathName,
                     "%s/0x%x.0x%x",
                     setHdr->dotTagsPath,
                     uniqBfDesc[i].bfTag.num,
                     uniqBfDesc[i].bfTag.seq
                     );

            /*
             * If the file is not a directory or a regular file, skip it and
             * let the kernel (advfs_remove_volume()) move its metadata or
             * storage.  We skip the file because we may not be able to open it.
             * The file is a symbolic link, block special, character special,
             * socket or fifo (named pipe).
             */
            err = lstat (dotTagsPathName, &stats);
            if (err == 0) {
                if (!(S_ISDIR (stats.st_mode)) && !(S_ISREG (stats.st_mode))) {
                    continue;
                }
            } else {
                if (errno == ENOENT) {
                    /*
                     * The file was deleted.
                     */
                    continue;
                } else {
                    fprintf (stderr, catgets(catd, 1, 39, "%s: Stat of %s failed.\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                             Prog, errno, ERRORMSG (errno));
                    return 1;
                }
            }

            fd = open (dotTagsPathName, O_RDONLY, 0);
            if (fd < 0) {
                if (errno == ENOENT) {
                    /*
                     * The file was deleted.
                     */
                    continue;
                } else {
                    fprintf (stderr, catgets(catd, 1, 40, "%s: Open of %s failed.\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                             Prog, errno, ERRORMSG (errno));
                    return 1;
                }
            }

            if (uniqBfDesc[i].metadataType == ML_NORMAL_METADATA) {
                /*
                 * Non-extent metadata.  Just move the metadata itself.
                 * This is most likely a BMTR_FS_STAT record for a directory.
                 * Another possibility is a property list mcell.
                 */
                if (verboseFlag != 0) {
                    fprintf (stdout, catgets(catd, 1, 41, "%s: Moving file metadata\n"), Prog);
                    fprintf (stdout, catgets(catd, 1, 42, "    file name: %s\n"), get_current_path_name());
                    if (veryVerboseFlag != 0) {
                        display_tag (
                                     uniqBfDesc[i].bfSetTag,
                                     uniqBfDesc[i].bfTag
                                     );
                    }
		    (void)fflush(stdout);      
                }

                /*
                 * If this is a file which supports extent information in
                 * the primary mcell and there is, in fact, extent information
                 * there at this time, the following call will return with
                 * the error E_STG_ADDED_DURING_MIGRATE_OPER.  This is because
                 * the extent metadata must be moved before the rest of the
                 * metadata can be.  We'll ignore this error since the 
                 * processing of the primary mcell 
                 * (ML_PRIME_MCELL_XTNT_METADATA) will take care of moving
                 * all of the non-extent metadata.
                 */
                sts = advfs_move_bf_metadata (fd);
                if (sts != EOK && sts != E_STG_ADDED_DURING_MIGRATE_OPER) {
                    fprintf (stderr, catgets(catd, 1, 43, "%s: Can't move file %s metadata\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                    (void) close (fd);
                    return 1;
                }
            } 
            else if (uniqBfDesc[i].metadataType == ML_XTNT_METADATA) {
                /*
                 * Extent data not stored in primary mcell.
                 * Move the extents and the extent metadata.
                 */
                sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);  
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 44, "%s: Can't get file %s parameters\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                    (void) close (fd);
                    return 1;
                }
                switch (bfAttr.mapType) {

                  case XMT_SIMPLE:

                    sts = move_normal_metadata (
                            fd,
                            uniqBfDesc[i].bfSetTag,
                            uniqBfDesc[i].bfTag,
                            volIndex,
                            uniqBfDesc[i].metadataType,
                            UTILS_FIRST_XTNT_IN_PRIM_MCELL(dmnParams.dmnVersion,
                                                           bfAttr.mapType) ?
                                TRUE : FALSE
                            );
                    break;

                  case XMT_STRIPE:

                    sts = move_stripe_metadata (
                                                fd,
                                                uniqBfDesc[i].bfSetTag,
                                                uniqBfDesc[i].bfTag,
                                                volIndex,
                                                bfAttr.attr.stripe.segmentCnt,
                                                bfAttr.attr.stripe.segmentSize
                                                );
                    break;

                  default:
                    fprintf (stderr, catgets(catd, 1, 43, "%s: Can't move file %s metadata\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 45, "%s: Unknown file type\n"), Prog);
                    (void) close (fd);
                    return 1;

                }  /* end switch */
                    
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 43, "%s: Can't move file %s metadata\n"),
                             Prog, get_current_path_name());
                    (void) close (fd);
                    return 1;
                }
            }
            else { /* ML_PRIME_MCELL_XTNT_METADATA */
                /*
                 * Looking at metadata from the primary mcell.  If there
                 * are no extents described in the primary mcell, just move
                 * the primary mcell metadata.  Otherwise, first move the
                 * extents described there and then move the primary mcell
                 * metadata.
                 */

                sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 44, "%s: Can't get file %s parameters\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), 
                             Prog, BSERRMSG (sts));
                    (void) close (fd);
                    return 1;
                }
    
                if (UTILS_FIRST_XTNT_IN_PRIM_MCELL(dmnParams.dmnVersion, 
                                                   bfAttr.mapType)) {
                    sts = move_normal_metadata (
                                                fd,
                                                uniqBfDesc[i].bfSetTag,
                                                uniqBfDesc[i].bfTag,
                                                volIndex,
                                                uniqBfDesc[i].metadataType,
                                                TRUE
                                                );

                    if (sts != EOK) {
                        fprintf (stderr, catgets(catd, 1, 43, "%s: Can't move file %s metadata\n"),
                                 Prog, get_current_path_name());
                        (void) close (fd);
                        return 1;
                    }
                }
    
                if (verboseFlag != 0) {
                    fprintf (stdout, catgets(catd, 1, 96, "%s: Moving file prime mcell\n"), Prog);

                    fprintf (stdout, catgets(catd, 1, 42, "    file name: %s\n"), get_current_path_name());
                    if (veryVerboseFlag != 0) {
                        display_tag (
                                     uniqBfDesc[i].bfSetTag,
                                     uniqBfDesc[i].bfTag
                                     );
                    }
                    (void)fflush(stdout);
                }
    
                /*
                 * If another thread added storage to the file and used
                 * the extent descriptor in the primary mcell for the first
                 * time in between our calls to move_normal_metadata() and
                 * advfs_move_bf_metadata(), then we will get back the
                 * return code E_STG_ADDED_DURING_MIGRATE_OPER from this
                 * call.  
                 */
                sts = advfs_move_bf_metadata (fd);
                if (sts != EOK && sts != E_STG_ADDED_DURING_MIGRATE_OPER) {
                    fprintf (stderr, catgets(catd, 1, 43, "%s: Can't move file %s metadata\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), 
                             Prog, BSERRMSG (sts));
                    (void) close (fd);
                    return 1;
                }
                /* Ignore possible E_STG_ADDED_DURING_MIGRATE_OPER error. */
                /* This mcell will be eximined again with the next call to */
                /* move_metadata. */
            }

            if (close (fd)) {
                fprintf( stderr, catgets(catd, 1, 46, "%s: Close of %s failed.\n"),
                        Prog, dotTagsPathName);
                fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRORMSG (errno));
                return 1;
            }

            fileCnt++;

        }  /* end for */

        sts = advfs_get_vol_bf_descs (
                                      domainId,
                                      volIndex,
                                      BF_DESC_MAX,
                                      &(bfDesc[0]),
                                      &nextBfDescId,
                                      &bfDescCnt
                                      );
        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, 37, "%s: Can't get volume file descriptors\n"), Prog);
            fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            return 1;
        }
    }  /* end while */

    *retFileCnt = fileCnt;

    return 0;

}  /* end move_metadata */

/*
 * move_normal_metadata
 *
 * This function moves a normal file's extents off of the specified disk.
 */

static
mlStatusT
move_normal_metadata (
                      int fd,  /* in */
                      mlBfTagT bfSetTag,  /* in */
                      mlBfTagT bfTag,  /* in */
                      u32T volIndex,  /* in */
                      mlMetadataTypeT mtype, /* in */
                      int supports_xtnts_in_prim  /* in */
                      )

{

    int i;
    mlStatusT sts;
    u32T totalPageCnt = 0;
    xtntDescT *xtntDesc = NULL;
    int xtntDescCnt;

 _retry:
    if (verboseFlag != 0) {
        fprintf (stdout, catgets(catd, 1, 47, "%s: Moving file\n"), Prog);
        fprintf (stdout, catgets(catd, 1, 42, "    file name: %s\n"), get_current_path_name());
        if (veryVerboseFlag != 0) {
            display_tag (bfSetTag, bfTag);
        }
	(void)fflush(stdout);	     
    }

    sts = create_xtnt_desc_list (fd, volIndex, 1, &xtntDescCnt, &xtntDesc);
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 48, "%s: Can't create extent desc list\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }

    for (i = 0; i < xtntDescCnt; i++) {

        if (xtntDesc[i].pageCnt > 0) {

            if (verboseFlag != 0) {
                fprintf (stdout, catgets(catd, 1, 49, "    moving pages - "));
                fprintf (stdout, catgets(catd, 1, 50, "page offset: %d, page count: %d\n"),
                         xtntDesc[i].pageOffset, xtntDesc[i].pageCnt);
		(void)fflush(stdout);	       
            }
                
            sts = migrate (
                           fd,
                           -1,  /* srcVolIndex */
                           xtntDesc[i].pageOffset,
                           xtntDesc[i].pageCnt,
                           -1  /* dstVolIndex */
                           );
            if (sts == EOK) {
                totalPageCnt = totalPageCnt + xtntDesc[i].pageCnt;
            } else {
              if (sts == E_PAGE_NOT_MAPPED) {
                
                if (verboseFlag != 0) {
                  fprintf (stdout, catgets(catd, 1, 106, "    extents have changed - reloading extent map for file %s\n"), get_current_path_name());
                  (void)fflush(stdout);             
                }

                if (xtntDesc != NULL) {
                  free (xtntDesc);
                  xtntDesc = NULL;
                }
                xtntDescCnt = 0; 
                totalPageCnt = 0;
                goto _retry;
                
              } else if (sts == E_NO_CLONE_STG) {
                sts = EOK;
              }
              goto _error;
            }
        } else if (supports_xtnts_in_prim &&
                   ((mtype == ML_PRIME_MCELL_XTNT_METADATA) ||
                    (mtype == ML_XTNT_METADATA))) {
            /*
             * If the metadata type is of the extent variety and
             * this file supports extent information in the primary
             * mcell, we will come into this routine for both the
             * ML_PRIME_MCELL_XTNT_METADATA record and for the
             * ML_XTNT_METADATA record.  The first one in will migrate
             * any extents from the volume being removed.  The second
             * will find no extents on this volume and doesn't need to
             * do anything.
             */
             ;
        }
        else {

            /*
             * The only descriptor is from an mcell that doesn't
             * describe any storage.  The mcell has one entry,
             * the termination extent descriptor.
             *
             * FIX - This can happen when a file has no storage and
             * no extent mcells.  If, after the first mcell is selected,
             * storage cannot be allocated from the same disk as the
             * mcell, the mcell is not freed.  It is left on the extent
             * chain and doesn't describe any storage.  This should be
             * fixed in the kernel.
             *
             * FIX - This can also happen when a file is truncated to 0.
             * The last mcell is not deleted.  This should be fixed in the
             * kernel.
             */
            if (verboseFlag != 0) {
                fprintf (stdout, catgets(catd, 1, 51, "    rewriting extent map\n"));
		(void)fflush(stdout);	       
            }
            sts = advfs_rewrite_xtnt_map (fd, 1);
            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 52, "%s: Can't rewrite file %s extent map\n"),
                         Prog, get_current_path_name());
                fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                goto _error;
            }
        }

    }  /* end for  */

    free (xtntDesc);

    if (verboseFlag != 0) {
        if (totalPageCnt == 0) {
            fprintf (stdout, catgets(catd, 1, 53, "    no pages moved\n"));
	    (void)fflush(stdout);	    
        }
    }

    return EOK;

_error:

    if (xtntDesc != NULL) {
        free (xtntDesc);
    }
    return sts;

}  /* end move_normal_metadata */

/*
 * move_stripe_metadata
 *
 * This function moves a stripe file's extents off of the specified disk.
 */

static
mlStatusT
move_stripe_metadata (
                      int fd,  /* in */
                      mlBfTagT bfSetTag,  /* in */
                      mlBfTagT bfTag,  /* in */
                      u32T targetVolIndex,  /* in */
                      i32T segmentCnt,  /* in */
                      i32T segmentSize  /* in */
                      )

{

    u32T bfPageCnt;
    u32T bfPageOffset;
    int i;
    int j;
    u32T pageCnt;
    char *response;
    char responseString[1024];
    mlStatusT sts;
    u32T totalPageCnt = 0;
    int validResponseFlag;
    u32T *volIndex = NULL;
    int xtntDescCnt;
    xtntDescT *xtntDesc = NULL;
    int xtntMapIndex;
    int yesFlag;
    int stripe;

    /*
     * Determine the preferred allocation disk for each extent map.  If
     * the preferred disk matches the target disk, change the preferred
     * allocation disk for the extent map.
     */
    
    volIndex = (u32T *) malloc (sizeof (u32T) * segmentCnt);
    if (volIndex == NULL) {
        fprintf (stderr, catgets(catd, 1, 54, "%s: Can't malloc volume index array.\n"), Prog);
        return ENO_MORE_MEMORY;
    }
    
    for ( stripe = 0;  stripe < segmentCnt;  stripe++ ) {

        sts = msfs_get_bf_xtnt_map (fd, stripe + 1, 0, 0, NULL,
                                    &xtntDescCnt, &(volIndex[stripe]));
        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, 55, "%s: Can't get extent map volume index\n"), Prog);
            fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            goto _error;
        }
        if (targetVolIndex == volIndex[stripe]) {
            xtntMapIndex = stripe;

            /*
             * Select another disk for the extent map's storage allocation.
             */

            /*
             * Calculate the number of pages needed on the new disk.
             */

            sts = create_xtnt_desc_list( fd,
                                         targetVolIndex,
                                         stripe + 1,
                                         &xtntDescCnt,
                                         &xtntDesc);
            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 48,
                         "%s: Can't create extent desc list\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 15,
                         "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                goto _error;
            }

            pageCnt = 0;
            for (i = 0; i < xtntDescCnt; i++) {
                pageCnt = pageCnt + calc_bfpagecnt( xtntDesc[i].pageOffset,
                                                    xtntDesc[i].pageCnt,
                                                    stripe,
                                                    segmentCnt,
                                                    segmentSize);
            }

            free (xtntDesc);
            xtntDesc = NULL;

            if (verboseFlag != 0) {
                fprintf (stdout, catgets(catd, 1, 56,
                       "%s: Setting stripe segment's next allocation volume\n"),
                        Prog);
                fprintf (stdout, catgets(catd, 1, 42,
                         "    file name: %s\n"), get_current_path_name());
                if (veryVerboseFlag != 0) {
                    display_tag (bfSetTag, bfTag);
                }
	        (void)fflush(stdout);	      
            }

            sts = advfs_set_bf_next_alloc_vol( fd,
                                               targetVolIndex,
                                               pageCnt,
                                               0); /* forceFlag */
            if (sts == E_NOT_ENOUGH_DISKS) {
                /*
                 * NOTE:  This message must not change as it is documented in
                 * the rmvol man page.
                 */
                fprintf (stdout, "\n");
                fprintf (stdout, catgets(catd, 1, 57,
                 "This volume contains one stripe segment of %s, which will\n"),
                         get_current_path_name());
                fprintf (stdout, catgets(catd, 1, 58,
               "be moved to another volume in the file domain that already\n"));
                fprintf (stdout, catgets(catd, 1, 59,
                         "contains a stripe segment of %s.\n"),
                         get_current_path_name());
                fprintf (stdout, "\n");
	        (void)fflush(stdout);		

                if (forceFlag == 0) {

                    validResponseFlag = 0;
                    while (validResponseFlag == 0) {
                        /*
                         * NOTE:  This message must not change as it is
                         * documented in the rmvol man page.
                         */
                        fprintf (stdout, catgets(catd, 1, 60,
                                 "Do you want to continue? (y/n):"));
		        (void)fflush(stdout);	
		        response = gets (responseString);
                        if (response != NULL) {
                            if (strlen (response) == 1) {
                                if (response[0] == 'y') {
                                    yesFlag = 1;
                                    validResponseFlag = 1;
                                } else {
                                    if (response[0] == 'n') {
                                        yesFlag = 0;
                                        validResponseFlag = 1;
                                    }
                                }
                            }
                        }
                    }  /* end while */
                    fprintf (stdout, "\n");
		    (void)fflush(stdout);	
                } else {
                     yesFlag = 1;
                }
                if (yesFlag != 0) {
                    fprintf (stdout, catgets(catd, 1, 61,
                             "%s: Stripe segment will be moved.\n"), Prog);
                    fprintf (stdout, catgets(catd, 1, 62,
                             "%s: Striped file will be suboptimal.\n"), Prog);
		    (void)fflush(stdout);	
                    sts = advfs_set_bf_next_alloc_vol( fd,
                                                       targetVolIndex,
                                                       pageCnt,
                                                       1); /* forceFlag */
                    if (sts != EOK) {
                        fprintf (stderr, catgets(catd, 1, 63,
                                "%s: Can't set stripe segment's next storage "),
                                 Prog);
                        fprintf (stderr, catgets(catd, 1, 64,
                                         "allocation volume\n"));
                        fprintf (stderr, catgets(catd, 1, 15,
                                 "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                        goto _error;
                    }
                } else {
                    fprintf (stdout, catgets(catd, 1, 65,
                             "%s: Stripe segment will not be moved.\n"), Prog);
		    (void)fflush(stdout);	
                    sts = E_NOT_ENOUGH_DISKS;
                    goto _error;
                }
            } else {
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 63,
                             "%s: Can't set stripe segment's next storage "),
                             Prog);
                    fprintf (stderr, catgets(catd,1,64, "allocation volume\n"));
                    fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"),
                             Prog, BSERRMSG (sts));
                    goto _error;
                }
            }
            /*
             * We found a matching disk and set its next allocation disk.
             * Determine what the new allocation disk is.
             */
            sts = msfs_get_bf_xtnt_map( fd,
                                        stripe + 1,
                                        0,
                                        0,
                                        NULL,
                                        &xtntDescCnt,
                                        &volIndex[stripe]);
            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 55,
                         "%s: Can't get extent map volume index\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"),
                         Prog, BSERRMSG (sts));
                goto _error;
            }
        }  /* end if stripe matches target */
    }  /* end for each stripe*/

    /*
     * Move stripe pages from the old stripe disk to the new stripe disk.
     *
     * NOTE:  All we know is that some part of the stripe is on the target
     * disk.  To be safe, we spin thru each extent map and remove any storage
     * on the disk. 
     */

    if (verboseFlag != 0) {
        fprintf (stdout, catgets(catd, 1, 47, "%s: Moving file\n"), Prog);
        fprintf (stdout, catgets(catd, 1, 42, "    file name: %s\n"), get_current_path_name());
        if (veryVerboseFlag != 0) {
            display_tag (bfSetTag, bfTag);
        }
	(void)fflush(stdout);		
    }

    for (j = 0; j < segmentCnt; j++) {

        sts = create_xtnt_desc_list (fd, targetVolIndex, j + 1, &xtntDescCnt, &xtntDesc);
        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, 48, "%s: Can't create extent desc list\n"), Prog);
            fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            goto _error;
        }

        for (i = 0; i < xtntDescCnt; i++) {

            if (xtntDesc[i].pageCnt > 0) {

                bfPageOffset = XMPAGE_TO_BFPAGE(j, xtntDesc[i].pageOffset,
                                                segmentCnt, segmentSize);
                bfPageCnt = calc_bfpagecnt (
                                            xtntDesc[i].pageOffset,
                                            xtntDesc[i].pageCnt,
                                            j,  /* 0 based xtntMapIndex */
                                            segmentCnt,
                                            segmentSize
                                            );
                if (verboseFlag != 0) {

                    u32T numPgs;
                    u32T pageOffset;
                        
                    pageOffset = bfPageOffset;

                    numPgs = segmentSize - (pageOffset % segmentSize);
                    if (numPgs > xtntDesc[i].pageCnt) {
                        numPgs = xtntDesc[i].pageCnt;
                    }
                    fprintf (stdout, catgets(catd, 1, 49, "    moving pages - "));
                    fprintf (stdout, catgets(catd, 1, 50, "page offset: %d, page count: %d\n"),
                             pageOffset, numPgs);
                        
                    pageOffset = pageOffset +
                      numPgs + ((segmentCnt - 1) * segmentSize);
                    numPgs = xtntDesc[i].pageCnt - numPgs;
                    while (numPgs > 0) {
                        fprintf (stdout, catgets(catd, 1, 66, "%19spage offset: %d, "), "", pageOffset);
                        if (numPgs > segmentSize) {
                            fprintf (stdout, catgets(catd, 1, 67, "page count: %d\n"), segmentSize);
                            numPgs = numPgs - segmentSize;
                        } else {
                            fprintf (stdout, catgets(catd, 1, 67, "page count: %d\n"), numPgs);
                            numPgs = 0;
                        }
                        pageOffset = pageOffset + (segmentCnt * segmentSize);
                    }
		    (void)fflush(stdout);	
                }
                
                sts = migrate (
                               fd,
                               targetVolIndex,
                               bfPageOffset,
                               bfPageCnt,
                               volIndex[j]
                               );
                if (sts == EOK) {
                    totalPageCnt = totalPageCnt + bfPageCnt;
                } else {
                    if (sts == E_NO_CLONE_STG) {
                        sts = EOK;
                    }
                    goto _error;
                }
            } else {
                /*
                 * The only descriptor is from an mcell that doesn't
                 * describe any storage.  The mcell has one entry,
                 * the termination extent descriptor.
                 *
                 * FIX - This can happen when a file has no storage and
                 * no extent mcells.  If, after the first mcell is selected,
                 * storage cannot be allocated from the same disk as the
                 * mcell, the mcell is not freed.  It is left on the extent
                 * chain and doesn't describe any storage.  This should be
                 * fixed in the kernel.
                 *
                 * FIX - This can also happen when a file is truncated to 0.
                 * The last mcell is not deleted.  This should be fixed in the
                 * kernel.
                 */
                if (verboseFlag != 0) {
                    fprintf (stdout, catgets(catd, 1, 51, "    rewriting extent map\n"));
		    (void)fflush(stdout);	
                }
                sts = advfs_rewrite_xtnt_map (fd, j + 1);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 52, "%s: Can't rewrite file %s extent map\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                    goto _error;
                }
            }

        }  /* end for  */

        if (xtntDesc != NULL) {
            free (xtntDesc);
            xtntDesc = NULL;
        }

    }  /* end for */

    free (volIndex);

    if (verboseFlag != 0) {
        if (totalPageCnt == 0) {
            fprintf (stdout, catgets(catd, 1, 53, "    no pages moved\n"));
	    (void)fflush(stdout);	
        }
    }

    return EOK;

_error:

    if (xtntDesc != NULL) {
        free (xtntDesc);
    }
    if (volIndex != NULL) {
        free (volIndex);
    }
    return sts;

}  /* end move_stripe_metadata */


/*
 * calc_bfpagecnt
 *
 * This function calculates a bitfile relative page count.  Used for striped
 * bitfiles.
 */

static
u32T
calc_bfpagecnt (
                u32T xmPageOffset,  /* in */
                u32T xmPageCnt,  /* in */
                int xtntMapIndex,  /* in, 0 based */
                i32T segmentCnt,  /* in */
                i32T segmentSize  /* in */
                )

{

    u32T endBfPageOffset;
    u32T endXmPageOffset;
    u32T bfPageOffset;

    if (xmPageCnt == 0) {
        return 0;
    }

    bfPageOffset = XMPAGE_TO_BFPAGE( xtntMapIndex,
                                     xmPageOffset,
                                     segmentCnt,
                                     segmentSize);
    endXmPageOffset = (xmPageOffset + xmPageCnt) - 1;
    /*
     * NOTE:  In user mode, the extent map index is relative 1.  In kernel mode,
     * the index is relative zero.
     */
    endBfPageOffset = XMPAGE_TO_BFPAGE( xtntMapIndex,
                                        endXmPageOffset,
                                        segmentCnt,
                                        segmentSize);
    return ((endBfPageOffset - bfPageOffset) + 1);

}  /* end calc_bfpagecnt */

/*
 * migrate
 *
 * This function moves a file's page range off of the specified disk.
 */

static
mlStatusT
migrate (
         int fd,  /* in */
         u32T srcVolIndex,  /* in */
         u32T bfPageOffset,  /* in */
         u32T bfPageCnt,  /* in */
         u32T dstVolIndex  /* in */
         )

{

    mlStatusT sts;
    u32T forceMigrateFlag = 0;

retry_migrate:
    sts = advfs_migrate (
                         fd,
                         srcVolIndex,
                         bfPageOffset,
                         bfPageCnt,
                         dstVolIndex,
                         -1,  /* dstBlkOffset */
                         forceMigrateFlag
                         );
    switch (sts) {

      case EOK:
        numFreeClustersAvailable -=  bfPageCnt;
        break;

      case E_PAGE_NOT_MAPPED:
        /* reload extent map and retry migrate in caller */
        break;

      case E_NO_CLONE_STG:
                        
        fprintf (stderr, catgets(catd, 1, 68, "%s: Can't move file %s pages\n"),
                 Prog, get_current_path_name());
        fprintf (stderr, catgets(catd, 1, 70, "%s: Clone does not have its own storage\n"), Prog);
        break;

      case ENO_MORE_BLKS:

        if ( numFreeClustersAvailable >= bfPageCnt ) 
            /* rmvol failed due to no space on volume to be migrated */
            fprintf(stderr, catgets(catd, 1, 116, "%s: Insufficient space on volume being removed. Can not create metadata. Remove unwanted files and try again\n"), Prog);
        else
            fprintf (stderr, catgets(catd, 1, 104, "%s: Ran out of free space to migrate data off volume\n"), Prog);
        break;

      case E_INVOLUNTARY_ABORT:

        if (!forceMigrateFlag) {
            forceMigrateFlag = 1;
            goto retry_migrate;
        }
        /* Fall through. */

      default:

        fprintf (stderr, catgets(catd, 1, 68, "%s: Can't move file %s pages\n"),
                 Prog, get_current_path_name());
        fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        break;

    }  /* end switch */

    return sts;

}  /* end migrate */

#define XTNT_DESC_MAX 100

/*
 * create_xtnt_desc_list
 *
 * This function creates a list of extents that are located on the
 * specified volume.  The page offset and count in each list entry
 * are bitfile relative.
 */

static
mlStatusT
create_xtnt_desc_list (
                       int fd,  /* in */
                       u32T volIndex,  /* in */
                       int xtntMapIndex,  /* in, 1 based */
                       int *retXtntDescCnt,  /* out */
                       xtntDescT **retXtntDesc  /* out */
                       )

{

    u32T allocVolIndex;
    u32T bfPageCnt;
    u32T bfPageOffset;
    int i;
    mlExtentDescT rawXtntDesc[XTNT_DESC_MAX];
    int rawXtntDescCnt;
    int startXtntDesc;
    mlStatusT sts;
    xtntDescT *xtntDesc = NULL;
    int xtntDescCnt = 0;
    int xtntDescMaxCnt = 0;

    startXtntDesc = 0;
    sts = msfs_get_bf_xtnt_map (
                                fd,
                                xtntMapIndex,
                                startXtntDesc,
                                XTNT_DESC_MAX,
                                &(rawXtntDesc[0]),
                                &rawXtntDescCnt,
                                &allocVolIndex
                                );
    while (sts == EOK) {

        i = 0;

        /*
         * Find the first extent descriptor that describes an extent on
         * the source disk.
         */
        while ((i < rawXtntDescCnt) && (rawXtntDesc[i].volIndex != volIndex)) {
            i++;
        }  /* end while */

        while (i < rawXtntDescCnt) {

            bfPageOffset = rawXtntDesc[i].bfPage;
            bfPageCnt = rawXtntDesc[i].bfPageCnt;
            i++;

            /*
             * Find the next extent descriptor that describes an extent
             * on a disk other than the source disk.
             */
            while ((i < rawXtntDescCnt) && (rawXtntDesc[i].volIndex == volIndex)) {
                bfPageCnt = bfPageCnt + rawXtntDesc[i].bfPageCnt;

                i++;
            }  /* end while */

            if (xtntDescCnt >= xtntDescMaxCnt) {
                sts = extend_xtnt_desc_list (&xtntDescMaxCnt, &xtntDesc);
                if (sts != EOK) {
                    goto _error;
                }
            }
            xtntDesc[xtntDescCnt].pageOffset = bfPageOffset;
            xtntDesc[xtntDescCnt].pageCnt = bfPageCnt;
            xtntDescCnt++;

            /*
             * Find the next extent descriptor that describes an extent on
             * the source disk.
             */
            while ((i < rawXtntDescCnt) && (rawXtntDesc[i].volIndex != volIndex)) {
                i++;
            }  /* end while */

        }  /* end while */

        startXtntDesc = startXtntDesc + rawXtntDescCnt;

        sts = msfs_get_bf_xtnt_map (
                                    fd,
                                    xtntMapIndex,
                                    startXtntDesc,
                                    XTNT_DESC_MAX,
                                    &(rawXtntDesc[0]),
                                    &rawXtntDescCnt,
                                    &allocVolIndex
                                    );

    }  /* end while */

    if (sts != ENO_XTNTS) {
        fprintf (stderr, catgets(catd, 1, 71, "%s: Can't get extent descriptors\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }

    *retXtntDescCnt = xtntDescCnt;
    *retXtntDesc = xtntDesc;

    return EOK;

_error:

    if (xtntDesc != NULL) {
        free (xtntDesc);
    }
    return sts;

}  /* end create_xtnt_desc_list */

/*
 * extend_xtnt_desc_list
 *
 * This function extends the extent list.
 */

static
mlStatusT
extend_xtnt_desc_list (
                       int *maxCnt,  /* in/out */
                       xtntDescT **xtntDesc  /* in/out */
                       )

{

    int i;
    int newMaxCnt;
    xtntDescT *newXtntDesc;
    int oldMaxCnt;
    xtntDescT *oldXtntDesc;

    oldMaxCnt = *maxCnt;
    oldXtntDesc = *xtntDesc;

    newMaxCnt = oldMaxCnt + 100;
    if (oldXtntDesc != NULL) {
        newXtntDesc = (xtntDescT *) malloc (newMaxCnt * sizeof (xtntDescT));
        if (newXtntDesc == NULL) {
            fprintf (stderr, catgets(catd, 1, 72, "%s: Can't allocate memory for expanded xtnt desc table\n"),
                     Prog);
            return ENO_MORE_MEMORY;
        }
        for (i = 0; i < oldMaxCnt; i++) {
            newXtntDesc[i] = oldXtntDesc[i];
        }  /* end for */
        free (oldXtntDesc);
    } else {
        newXtntDesc = (xtntDescT *) malloc (newMaxCnt * sizeof (xtntDescT));
        if (newXtntDesc == NULL) {
            fprintf (stderr, catgets(catd, 1, 73, "%s: Can't allocate memory for new xtnt desc table\n"),
                     Prog);
            return ENO_MORE_MEMORY;
        }
    }

    *maxCnt = newMaxCnt;
    *xtntDesc = newXtntDesc;

    return EOK;

}  /* end extend_xtnt_desc_list */

/*
 * display_tag
 *
 * This function displays the bitfile set tag and bitfile tag.
 */

static
void
display_tag (
             mlBfTagT bfSetTag,  /* in */
             mlBfTagT bfTag  /* in */
             )

{

    fprintf (stdout, catgets(catd, 1, 76, "    set tag: %d.%d (0x%x.0x%x)\n"),
             bfSetTag.num, bfSetTag.seq, bfSetTag.num, bfSetTag.seq);
    fprintf (stdout, catgets(catd, 1, 77, "    tag: %d.%d (0x%x.0x%x)\n"),
             bfTag.num, bfTag.seq, bfTag.num, bfTag.seq);
    (void)fflush(stdout);

}  /* end display_tag */

/*
 * mount_bitfile_sets
 *
 * This function opens ".tags" in each of the domain's bitfile sets so that
 * the bitfile sets cannot be unmounted during the remove volume operation.
 * If the bitfile set is not mounted, this function creates a temporary mount
 * directory and mounts the bitfile set on the directory.  (NOTE:  This is
 * not yet implemented.  Instead, a message is printed that says the bitfile
 * set is not mounted.)
 *
 * This function has the side effect of filling the bitfile set cache.
 *
 * This function assumes the caller has locked the domain.
 */

static
int
mount_bitfile_sets (
                    char *dmnName,  /* in */
                    mlBfTagT rootBfSetTag  /* in */
                    )

{

    setHdrT *setHdr;
    u32T bfSetIndex;
    mlBfSetParamsT bfSetParams;
    int err;
    int i;
    mlBfSetIdT mountBfSetId;
    char *mountBfSetName;
    char *mountDmnName;
    char mountPointPath[MAXPATHLEN+1];
    int mountCnt;
    int setNotMountedFlag = 0;
    struct statfs *mountTbl = NULL;
    mlStatusT sts;
    u32T userId;

    mountCnt = getfsstat (0, 0, MNT_NOWAIT);
    if (mountCnt <= 0) {
        fprintf (stderr, catgets(catd, 1, 78, "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 79, "%s: Mount count = %d\n"), Prog, mountCnt);
        return 1;
    }

    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
        fprintf (stderr, catgets(catd, 1, 80, "%s: Can't malloc file system mount table.\n"), Prog);
        return 1;
    }

    /*
     * Because the caller has the domain locked, no more megasafe bitfile
     * sets can be mounted.  "mountTbl" will be up to date relative to
     * megasafe file systems.
     */

    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 78, "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                 Prog, errno, ERRORMSG (errno));
        return 1;
    }

    /*
     * For all mounted megasafe bitfile sets in the domain, add an entry
     * to the bitfile set cache and set the ".tags" path.
     */

    for (i = 0; i < mountCnt; i++) {

        /*
         * NOTE: strtok() replaces the separator character with a null.
         */
        mountDmnName = (char *) strtok (mountTbl[i].f_mntfromname, "#");

        if ((mountTbl[i].f_type == MOUNT_MSFS) &&
            (strcmp (dmnName, mountDmnName) == 0)) {

            mountBfSetName = (char *) strtok (0, "");

            sts = msfs_fset_get_id (mountDmnName, mountBfSetName, &mountBfSetId);
            if (sts == EOK) {
                /*
                 * The bitfile set is a member of the domain.
                 */
                setHdr = insert_set_tag (mountBfSetId.dirTag);
                if (setHdr != NULL) {
                    setHdr->mountedFlag = 1;
                    if (setHdr->mntOnName == NULL) {
                        setHdr->mntOnName = malloc (
                                                    strlen (mountTbl[i].f_mntonname) + 1
                                                    );
                        if (setHdr->mntOnName == NULL) {
                            fprintf (stderr, catgets(catd, 1, 81, "%s: Can't malloc mount on name memory\n"));
                            return 1;
                        }
                        strcpy (setHdr->mntOnName, mountTbl[i].f_mntonname);
                    }
                    if (setHdr->dotTagsPath == NULL) {
                        /*
                         * New entry.  Set the ".tags" path.
                         */
                        setHdr->dotTagsPath = malloc (
                                                      strlen (mountTbl[i].f_mntonname) +
                                                      strlen (dotTags) + 1
                                                      );
                        if (setHdr->dotTagsPath == NULL) {
                            fprintf (stderr, catgets(catd, 1, 82, "%s: Can't malloc path name memory\n"));
                            return 1;
                        }
                        strcpy (setHdr->dotTagsPath, mountTbl[i].f_mntonname);
                        strcat (setHdr->dotTagsPath, dotTags);
                    }
                } else {
                    fprintf (stderr, catgets(catd, 1, 83, "%s: Can't malloc memory for file set header\n"));
                    return 1;
                }
            } else {
                fprintf (stderr, catgets(catd, 1, 84, "%s: Can't get file set id.\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 85, "%s: Domain name: %s, file set name: %s\n"),
                         Prog, mountBfSetName, mountDmnName);
                fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                return 1;
            }
        }
    }  /* end for */

    bfSetIndex = 0;

    sts = msfs_fset_get_info (dmnName, &bfSetIndex, &bfSetParams, &userId, 0);

    while (sts == EOK) {

        setHdr = insert_set_tag (bfSetParams.bfSetId.dirTag);
        if (setHdr != NULL) {

            if (setHdr->mountedFlag == 0) {

                /*
                 * New bitfile set entry.
                 */

                /*
                 * Mount the bitfile set.
                 */

                fprintf (stderr, catgets(catd, 1, 86, "%s: File set not mounted\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 85, "%s: Domain name: %s, file set name: %s\n"),
                         Prog, dmnName, bfSetParams.setName);
                /*
                 * This flag prevents subsequent ".tags" opens.
                 */
                setNotMountedFlag = 1;
            }
            if ((setHdr->fd < 0) && (setNotMountedFlag == 0)) {
                /*
                 * Open the bitfile set's ".tags" directory so that the bitfile
                 * set cannot be unmounted during the remove volume operation.
                 */
                setHdr->fd = open (setHdr->dotTagsPath, O_RDONLY, 0);
                if (setHdr->fd < 0) {
                    fprintf (stderr, catgets(catd, 1, 40, "%s: Open of %s failed.\n"),
                             Prog, setHdr->dotTagsPath);
                    fprintf (stderr, catgets(catd, 1, 10, "%s: Error = [%d] %s\n"),
                             Prog, errno, ERRORMSG (errno));
                    return 1;
                }
            }

        } else {
            fprintf (stderr, catgets(catd, 1, 83, "%s: Can't malloc memory for file set header\n"));
            return 1;
        }

        sts = msfs_fset_get_info (dmnName, &bfSetIndex,
                                     &bfSetParams, &userId, 0);

    }  /* end while */

    if (sts != E_NO_MORE_SETS) {
        fprintf (stderr, catgets(catd, 1, 87, "%s: Can't get file set info for domain '%s'\n"),
                 Prog, dmnName);
        fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        return 1;
    }

    return setNotMountedFlag;

}  /* end mount_bitfile_sets */

/*
 * unmount_bitfile_sets
 *
 * This function closes ".tags" in each of the domain's bitfile sets so that
 * the bitfile sets can be unmounted.
 */

static
int
unmount_bitfile_sets ()

{

    u32T i;
    int errorFlag = 0;

    if (setHdrTbl == NULL) {
        return 0;
    }

    for (i = 0; i < setHdrCnt; i++) {

        if (setHdrTbl[i].fd != -1) {
            if (close (setHdrTbl[i].fd)) {
                fprintf( stderr, catgets(catd, 1, 88, "%s: Close of .tags file failed. [%d] %s\n"),
                        Prog, errno, sys_errlist[errno] );
                errorFlag = 1;
            }

        }
        if (setHdrTbl[i].mntOnName != NULL) {
            free (setHdrTbl[i].mntOnName);
        }
        if (setHdrTbl[i].dotTagsPath != NULL) {
            free (setHdrTbl[i].dotTagsPath);
        }

    }  /* end for */

    free (setHdrTbl);

    setHdrCnt = 0;
    setHdrTbl = NULL;

    if (errorFlag)
        return 1;

    return 0;

}  /* end unmount_bitfile_sets */

/*
 * insert_set_tag
 *
 * This function inserts a bitfile set tag in the set table.  This function
 * does not check for duplicate entries.
 */

static
setHdrT *
insert_set_tag (
                mlBfTagT setTag  /* in */
                )

{

    int i;
    setHdrT *newSetHdrTbl;
    setHdrT *setHdr;

    setHdr = find_set_tag (setTag);
    if (setHdr != NULL) {
        return setHdr;
    }
        
    if (setHdrTbl != NULL) {
        newSetHdrTbl = (setHdrT *) malloc ((setHdrCnt + 1) * sizeof (setHdrT));
        if (setHdrTbl == NULL) {
            fprintf (stderr, catgets(catd, 1, 89, "%s: Can't allocate memory for expanded set table\n"),
                     Prog);
            return NULL;
        }
        for (i = 0; i < setHdrCnt; i++) {
            newSetHdrTbl[i] = setHdrTbl[i];
        }  /* end for */
        free (setHdrTbl);
        setHdrTbl = newSetHdrTbl;
        setHdrCnt++;
    } else {
        setHdrTbl = (setHdrT *) malloc (sizeof (setHdrT));
        if (setHdrTbl == NULL) {
            fprintf (stderr, catgets(catd, 1, 90, "%s: Can't allocate memory for new set table\n"),
                     Prog);
            return NULL;
        }
        setHdrCnt = 1;
    }

    setHdr = &(setHdrTbl[setHdrCnt - 1]);
    setHdr->setTag = setTag;
    setHdr->mountedFlag = 0;
    setHdr->unmountFlag = 0;
    setHdr->mntOnName = NULL;
    setHdr->dotTagsPath = NULL;
    setHdr->fd = -1;

    return setHdr;

}  /* end insert_set_tag */

/*
 * find_set_tag
 *
 * This function finds a bitfile's set tag in the set table.
 */

static
setHdrT *
find_set_tag (
              mlBfTagT setTag  /* in */
              )

{

    int i;
    setHdrT *setHdr;

    if (setHdrTbl == NULL) {
        return NULL;
    }

    setHdr = NULL;
    for (i = 0; i < setHdrCnt; i++) {
        if (ML_BFTAG_EQL (setTag, setHdrTbl[i].setTag)) {
            return &(setHdrTbl[i]);
        }
    }  /* end for */

    return NULL;

}  /* end find_bitfile_set */

static
char *
get_current_path_name(void)
{
    mlStatusT sts;

    if (!currentPath.pathName[0]) {
        sts = tag_to_path (
                       currentPath.setHdr->mntOnName,
                       currentPath.bfTag,
                       sizeof (currentPath.pathName),
                       &(currentPath.pathName[0])
                       );
        if (sts != EOK) {
            /*
             * Can't convert the tag to a pathname.
             */
            sprintf (
                     currentPath.pathName,
                     catgets(catd, 1, 91, "<unknown> (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))"),
                     currentPath.setHdr->setTag.num,
                     currentPath.setHdr->setTag.seq,
                     currentPath.setHdr->setTag.num,
                     currentPath.setHdr->setTag.seq,
                     currentPath.bfTag.num,
                     currentPath.bfTag.seq,
                     currentPath.bfTag.num,
                     currentPath.bfTag.seq
                     );
        }
    }
    return currentPath.pathName;
} /* end get_current_path_name */


/*
 * switch_root_tagdir
 *
 * Migrate the tagdir to a persistent volume in the domain. 
 */
static
int
switch_root_tagdir(int volIndexI, int volIndexCnt, u32T *volIndex)
{
    int i, j;
    int volAvail;
    mlStatusT sts;

    if (verboseFlag != 0) {
        fprintf (stdout, catgets(catd, 1, 28,
                                 "%s: Switching root tag directory\n"), Prog);
        (void)fflush(stdout);         
    }

    /* Find vol to move tagdir onto and move it */
    for (i = 0; i < volIndexCnt; i++) {
        volAvail = 1;
        if (i != volIndexI) {

            /* do not move tagdir to any vol requested for removal */
            for (j = 0; j < numVolsLeftToRem; j++) {
                if (i == volsToRem[j]->volIndexI) {
                    volAvail = 0;
                    break; /* out of j for */
                }
            }
            
            /* 
             *  This volume is persistent.
             *  Move the tagdir here if enough free space. 
             */
            if (volAvail) {
                sts = advfs_switch_root_tagdir (dmnName, volIndex[i]);
                if ((sts != ENO_MORE_BLKS) && (sts != ENO_MORE_MCELLS)) {
                    break;  /* out of i for */
                }
            }
        }
    }  /* end for */
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 29, "%s: Can't switch root tag directory\n"), Prog);
        if (sts == ENO_MORE_BLKS) {
            fprintf (stderr, catgets(catd, 1, 30, "%s: No one disk has enough available storage\n"),
                     Prog);
        } else {
            if (sts == ENO_MORE_MCELLS) {
                fprintf (stderr, catgets(catd, 1, 31, "%s: No one disk has enough available metadata\n"),
                         Prog);
            } else {
                fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));

            }
        }
        return 1;
    }
    return 0;
}


/*
 * switch_log
 *
 * Migrate the log to a persistent volume in the domain. 
 */
static
int
switch_log(int volIndexI, int volIndexCnt, u32T *volIndex)
{
    int i, j;
    int volAvail;
    mlStatusT sts;

    if (verboseFlag != 0) {
        fprintf (stdout, catgets(catd, 1, 32, "%s: Switching log\n"), Prog);
        (void)fflush(stdout);       
    }

    /* Find vol to move log onto and move it */
    for (i = 0; i < volIndexCnt; i++) {
        volAvail = 1;
        if (i != volIndexI) {

            /* do not move log to any vol requested for removal */
            for (j = 0; j < numVolsLeftToRem; j++) {
                if (i == volsToRem[j]->volIndexI) {
                    volAvail = 0;
                    break; /* out of j for */
                }
            }
            
            /* 
             *  This volume is persistent.
             *  Move the log here if enough free space. 
             */
            if (volAvail) {
                sts = advfs_switch_log (dmnName, volIndex[i], 0, 0);
                if ((sts != ENO_MORE_BLKS) && (sts != ENO_MORE_MCELLS)) {
                    break;  /* out of i for */
                }
            }
        }
    }  /* end for */

    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 33, "%s: Can't switch log\n"), Prog)
            ;
        if (sts == ENO_MORE_BLKS) {
            fprintf (stderr, catgets(catd, 1, 30, "%s: No one disk has enough available storage\n"),
                     Prog);
        } else {
            if (sts == ENO_MORE_MCELLS) {
                fprintf (stderr, catgets(catd, 1, 31, "%s: No one disk has enough available metadata\n"),
                         Prog);
            } else {
                fprintf (stderr, catgets(catd, 1, 15, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            }
        }
        return 1;
    }
    return 0;
}

/* 
 * cfs_server()
 *
 * Any changes to this routine: please see addvol's cfs_server logic
 * also 
 *
 * Return values:
 *      -2: failover/relocation
 *      -1: cfs error
 *       0: remote server
 *       1: local server
 *
 */


cfs_server(const char *node, const char *domain, char *server_name)
{

    cfs_server_info_t info;
    cfg_status_t cfg_status;

    cfg_status = clu_filesystem((char *) domain,
                                CFS_SERVER,
                                CLU_CFS_GET,
                                CFS_DOMAIN_NAME,
                                &info,
                                sizeof(info),
                                NULL);
    if (cfg_status != CFG_SUCCESS) {
        syslog(LOG_ERR, "clu_filesystem error: %d\n", cfg_status);
        return -1;
    }

    if (info.status != CFS_S_OK) {
        switch(info.status) {
        case CFS_S_NOT_MOUNTED:       
            return 1;		/* local host will be server */
        case CFS_S_FAILOVER_IN_PROGRESS:
        case CFS_S_RELOCATION_IN_PROGRESS:
            return -2;  
        default:
            syslog(LOG_ERR, "CLU_CFS_STATUS: %d\n", info.status);
            return -1;
        }
    }

    if (strcmp(node, (strtok(info.name, ".")))) {
        strcpy(server_name, info.name);
        return 0;
    }

    return 1;
}

/* end rmvol.c */
