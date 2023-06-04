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
 * Copyright (c) 2003-2004 Hewlett-Packard Development Company, L.P.
 *
 * Abstract:
 *
 *    balance.c
 *    This module implements the filesystem balancing utility.
 *
 */

#include "common.h"
#include <stdio.h>
#include <math.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/signal.h>
#include <mntent.h>
#include <sys/param.h>
#include <advfs/advfs_evm.h>
#include <locale.h>

#define BALANCE_GOAL 0.002
#define FILE_GROUP_SIZE 5000

#define BF_DESC_MAX 500
#define XTNT_DESC_MAX 500

/*  Typedefs */

typedef int boolean;

typedef struct bfTableEntryT {
    bfTagT                bfSetTag;
    bfTagT                bfTag;
    struct bfTableEntryT *leftChild;
    struct bfTableEntryT *rightChild;
    uint64_t              fileBlockCount;
    char                  noMove;      /* boolean */
    char                  balance;     /* actually, only needs two bits! */
} bfTableEntryT;

typedef struct {
    uint32_t volIndex;
    uint32_t volSubscr;
} xtntDescT;

typedef struct {
    uint32_t volIndex;
    uint64_t volSize;
    uint64_t volFreeSize;
    double   freeSizeFract;
    bfMCIdT  nextBfDescId;
    boolean  noMoreFiles;
} volDataT;

typedef boolean compare_bf_entriesT(
    bfTableEntryT *aEntry,        
    bfTableEntryT *bEntry);       

/*  Globals */
extern char *sys_errlist[];

static bfTableEntryT *bfEntryFreeList;

static arg_infoT     *infop;
static char          *sbmRootPathName = NULL;
static char          *Prog = "fsadm balance";
static int            volIndexCnt;
static int           *volSubscrArray;
static boolean        unmountFlag = FALSE;
static boolean        verboseFlag = FALSE;
static bfTableEntryT *bfTableRoot;

static uint32_t sourceVolSubscr;
static boolean moreFiles;

static boolean reinsertSideFlag;
static uint64_t fsSize;
static uint64_t fsFreeSize;
static double fsFreeSizeFract;
static double fsBalanceDeviation;

static int thisFsLock=-1;
static advfs_ev advfs_event;

/*
 * These two pointers are used to implement the AVL tree balancing algorithm,
 * described in Knuth, vol 3, page 455-7.
 */
static bfTableEntryT *sEntry; /* Entry where re-balancing of tree may 
                               * be necessary. 
                               */
static bfTableEntryT *tEntry; /* Parent of sEntry, or NULL. */

static int64_t *volBlockDistribution;

/*
 * These two pointers are used to implement the AVL tree balancing algorithm,
 * described in Knuth, vol 3, page 455-7.
 */
static bfTableEntryT *sEntry; /* Entry where re-balancing of tree may
                               * be necessary.
                               */
static bfTableEntryT *tEntry; /* Parent of sEntry, or NULL. */

static int64_t *volBlockDistribution;


/*  Macros */

#define MSG(n,s) catgets(catd,MS_FSADM_BALANCE,n,s)

/* The following macros are used to order tags in a binary search tree.  
 * We xor every other bit so as to get a non-monotonic
 * order in the usual case that the tags are encountered in order. 
 */

#define XXOR(x) (x^0x55555555)
#define BS_BFTAG_LESS(tag1, tag2) \
    (((XXOR((tag1).tag_num) < XXOR((tag2).tag_num))) || \
     (((XXOR((tag1).tag_num) == XXOR((tag2).tag_num)) && ((tag1).tag_seq < (tag2).tag_seq))))

/*  Function prototypes */

static bfTableEntryT
*new_bf_entry(void);

static boolean
insert_bf_entry(
    bfTableEntryT *table,          /* in */
    bsBfDescT *datum,              /* in */
    bfTableEntryT **retNewEntry);  /* out */

static void
linearize_bf_table(
    bfTableEntryT *entry);         /* in */

static void
reinsert_bf_entry(
    bfTableEntryT *table,
    bfTableEntryT *entry);

/* These functions return TRUE if the first entry goes to the left
 * of the second entry. 
 */

static boolean
compare_insert(
    bfTableEntryT *aEntry,
    bfTableEntryT *bEntry
);

static boolean
compare_reinsert(
    bfTableEntryT *aEntry,
    bfTableEntryT *bEntry
);

static void
adjust_bf_table(
    bfTableEntryT       *entry,
    compare_bf_entriesT *compare_routine);  /* in */

static int
do_balance(
    void);                       /* in */

static int
make_bf_table (
    bfDomainIdT fsId,            /* in */
    volDataT     *volData);      /* in */

static int
move_files (
    adv_bf_dmn_params_t *fsParams,      /* in */
    volDataT     *volData);      /* in */

static int
update_volume_data (
    adv_bf_dmn_params_t *fsParams,      /* in */
    volDataT *volData);	         /* in/out */

static adv_status_t
get_block_distribution (
    int      fd,                /* in */
    volDataT *volData,          /* in */
    int      pageSize,          /* in */
    boolean  *onSingleVolume);  /* out */

static int
decide_move(
    int       fd,                 /* in */
    volDataT *volData,            /* in */
    int       pageSize,           /* in */
    uint64_t  fileBlockCount,     /* in */
    int      *targetVolumeIndex,  /* out */
    boolean  *moveFlag);          /* out */

static void sigHandler (
    int signal);                  /* in */


static bfTableEntryT
*new_bf_entry(void)
{
    bfTableEntryT *temp;

    if (bfEntryFreeList) {
        temp = bfEntryFreeList; 
        bfEntryFreeList = bfEntryFreeList->rightChild;
        return temp;
    } else {
        return (bfTableEntryT*)malloc(sizeof(bfTableEntryT));
    }
}

/*
 * This is the balance signal handler.  It gives this program a chance
 * to cleanup before quitting.  Specifically, it unmounts and unlocks the 
 * filesystem.
 */

static void sigHandler (
    int signal      /* in */
)
{

    fprintf (stderr, MSG(BAL_STOPPING, 
        "%s: Stopping balance operation...\n"), Prog);

    switch (signal) {

        case SIGINT:
        case SIGQUIT:
        case SIGPIPE:
        case SIGTERM:
            break;

        default:
            fprintf (stderr, CMSG(COMMON_UKNSIG, "%s: Unexpected signal: %d\n"), 
                Prog, signal);
            break;
    }  /* end switch */

    if (unmountFlag) {
        unmountFlag = FALSE;
        (void)close_dot_tags(Prog);
    }

    fprintf (stderr, MSG(BAL_FSNOTBAL, "%s: File system '%s' not balanced\n"), 
        Prog, infop->fsname); 

    if (thisFsLock != -1) {
        advfs_unlock(thisFsLock, NULL);
        advfs_post_user_event(EVENT_FDMN_BAL_ERROR, advfs_event, Prog);
    }
    exit (1);

}  /* end sigHandler */


void
balance_usage(void)
{
    usage(MSG(BAL_USAGE, "%s [-v] fsname\n"), Prog);
    return;
}

int
balance_main(int argc, char **argv)
{

    int                c;
    int                err;
    int                err2;
    struct stat        stats;
    adv_bf_dmn_params_t       fsParams;
    adv_ss_dmn_params_t     ssDmnParams;
    adv_status_t       sts=EOK;
    ssDmnOpT        ssDmnCurrentState=0;
    int                ssRunningState=0;
    int                Vflag = 0;

    extern char       *optarg;
    extern int         optind;
    extern int         opterr;

    check_root(Prog);

    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt (argc, argv, "Vv")) != EOF) {

        switch (c) {
            case 'v':
                verboseFlag++;
                break;

            case 'V':
                Vflag++;
                break;

            default:
                balance_usage();
                return(1);
        }
    }  /* end while */

    if (optind != (argc - 1)) {
        /* missing required args */
        balance_usage();
        return(1);
    }

    if (Vflag) {
        printf(Voption(argc, argv));
        return(0);
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    err = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR, 
        BFD_ODS_LAST_VERSION_MINOR);

    if (err != EOK) {
        fprintf( stderr, MSG(COMMON_ONDISK,
            "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
        return(1);
    }

    infop = process_advfs_arg(argv[optind]);
    if (infop == NULL) {
        fprintf(stderr, MSG(COMMON_FSNAME_ERR,
            "%s: Error processing specified name %s\n"), 
            Prog, argv[optind]);
        return(1);
    }

    init_event(&advfs_event);
    advfs_event.domain = infop->fsname;

    /*
     * If vfast (autotune) is activated on a filesystem AND if vfast defragment OR
     * vfast balance OR vfast topIObalance are enabled, don't run
     * the balance utility.
     */
    sts = advfs_ss_dmn_ops(infop->fsname,
                           SS_UI_STATUS,
                           &ssDmnCurrentState,
                           &ssRunningState);

    if (sts != EOK) {
        fprintf (stderr, CMSG(COMMON_NOSTATUS, 
            "%s: status not available on file system %s; %s\n"),
            Prog, infop->fsname, BSERRMSG(sts));
        return(1);
    }

    sts = advfs_ss_get_params(infop->fsname, &ssDmnParams);
    if (sts != EOK) {
        fprintf(stderr, CMSG(COMMON_NOFSPARAMS, 
            "%s: Cannot get file system parameters for %s\n"),
            Prog, infop->fsname);
    }

    if (((ssDmnParams.ssDmnDefragment == 1) ||
         (ssDmnParams.ssDmnDefragAll  == 1) ||
         (ssDmnParams.ssDmnBalance    == 1) ||
         (ssDmnParams.ssDmnSmartPlace == 1)) &&
         (ssDmnCurrentState == 1)) {
        fprintf(stderr, MSG(BAL_ERRVFAST, "%s: autotune is currently running in the background, cannot run %s on file system '%s'\n"),
                Prog, Prog, infop->fsname);
        return(1);
    }

    /*
     * Disable interrupts.
     */

    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    fprintf(stdout, MSG(BAL_INPROGRESS, "%s: Balancing file system '%s'\n"), 
        Prog, infop->fsname);
    (void)fflush(stdout);               

    err = do_balance();
    if (err != 0) {
        goto _error;
    }

    fprintf(stdout, MSG(BAL_DONE, "%s: Balanced file system '%s'\n"), Prog, 
        infop->fsname);
    (void)fflush(stdout);               

    advfs_post_user_event(EVENT_FDMN_BAL_UNLOCK, advfs_event, Prog);
    return 0;

_error:

    fprintf(stderr, MSG(BAL_CANT, "%s: Cannot balance file system '%s'\n"), Prog, 
        infop->fsname);

    if (thisFsLock != -1) {
        advfs_unlock(thisFsLock, NULL);
        advfs_post_user_event(EVENT_FDMN_BAL_ERROR, advfs_event, Prog);
    }

    return(1);

}  /* end balance_main */

/*
 * do_balance
 *
 * This function balances the files on the specified filesystem.
 */

static int
do_balance(
    void
)
{

    adv_bf_dmn_params_t    fsParams;
    int             err;
    int             err2;
    int             i;
    struct stat     stats;
    adv_status_t    sts;
    uint32_t       *volIndexArray = NULL;
    int             max_vdi;
    volDataT       *volData = NULL;

    int             lastExtentCount;
    int             lastFileWithExtCount;
    int             unimprovedPassCount = 0;

    /*
     * Lock this filesystem.
     */

    thisFsLock = advfs_fspath_longrun_lock (infop->fspath, 
                                            FALSE, 
                                            TRUE, 
                                            &err);
    if (thisFsLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, CMSG(COMMON_FSBUSY, "%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified file system.\n"), Prog);
        } else {
            fprintf(stderr, CMSG(COMMON_ERRLOCK, 
                "%s: Error locking '%s'; [%d] %s\n"),
                     Prog, infop->fspath, err, ERRMSG(err));
        }
        goto _error;
    } else {
        advfs_post_user_event(EVENT_FDMN_BAL_LOCK, advfs_event, Prog);
    }

    /*
     * Get the filesystem's parameters and a list of its volumes.
     */

    sts = advfs_get_dmnname_params (infop->fsname, &fsParams);
    if (sts != EOK) {
        fprintf (stderr, CMSG(COMMON_NOFSPARAMS, 
            "%s: Cannot get file system parameters for %s\n"), Prog, infop->fsname);
        if (sts != -1) {
            fprintf (stderr, CMSG(COMMON_ERR, "%s: Error = %s\n"), 
                Prog, BSERRMSG (sts));
        } else {
            fprintf (stderr, CMSG(COMMON_ERR, "%s: Error = %s\n"), 
                Prog, ERRMSG (errno));
        }
        goto _error;
    }

    volIndexArray = malloc(fsParams.curNumVols * sizeof(uint32_t));
    volData = malloc(fsParams.curNumVols * sizeof(volDataT));

    memset(volData, 0, sizeof(volData));
    
    volBlockDistribution = malloc(fsParams.curNumVols * sizeof(int64_t));

    sts = advfs_get_dmn_vol_list (fsParams.bfDomainId, fsParams.curNumVols, 
              volIndexArray, &volIndexCnt);

    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, CMSG(COMMON_FSMOUNT, 
            "%s: file system must be mounted\n"), Prog);
        goto _error;
    } else if (sts != EOK) {
        fprintf (stderr, CMSG(COMMON_NOFSLIST, 
            "%s: Cannot get file system's volume list\n"), Prog);
        fprintf (stderr, CMSG(COMMON_ERR, "%s: Error = %s\n"), 
            Prog, BSERRMSG (sts));
        goto _error;
    }

    if (volIndexCnt < 2) {
        fprintf (stderr, MSG(BAL_TWOMIN, 
            "%s: File system must have at least two volumes\n"), Prog);
        goto _error;
    }

    for (i = 0; i < volIndexCnt; i++) {
        volData[i].volIndex = volIndexArray[i];
    }

    /* Set up an array that, given a volume index, enables us to find the
     * corresponding subscript into the index array.
     */
    max_vdi = 0;
    for (i = 0; i < volIndexCnt; i++) {
        if (volIndexArray[i] > max_vdi) {
            max_vdi = volIndexArray[i];
        }
    }
    volSubscrArray = malloc((max_vdi + 1) * sizeof(int));
    for (i = 0; i <= max_vdi; i++) {
        volSubscrArray[i] = -1;
    }
    for (i = 0; i < volIndexCnt; i++) {
        volSubscrArray[volIndexArray[i]] = i;
    }

    /*
     * Mount all bitfile sets in the filesystem.  The function also opens each
     * bitfile set's ".tags" directory to ensure that the set stays mounted
     * while the filesystem is balanced.
     */
    err = open_dot_tags(infop, fsParams.bfSetDirTag, NULL, Prog);
    if (err)
        goto _error;

    unmountFlag = TRUE;

    /*
     * Enable interrupts.
     */

    signal (SIGINT, sigHandler);
    signal (SIGQUIT, sigHandler);
    signal (SIGPIPE, sigHandler);
    signal (SIGTERM, sigHandler);

    bfEntryFreeList = NULL;
    sourceVolSubscr = 0;
    moreFiles = TRUE;

    /* Loop to process each group of FILE_GROUP_SIZE files. */
    do {
        /*
         * Get a list of descriptors, one for each file in the filesystem that
         * has any storage.
         */

        err = make_bf_table(fsParams.bfDomainId, volData);
        if (err) {
            goto quit_balance;
        }

        /*
         * Considering each file in turn, migrate it if it would improve
         * the balance deviation of the filesystem.
         */

        err = move_files(&fsParams, volData);
        if (err) {
            goto quit_balance;
        }
    } while (moreFiles);

quit_balance:

    /*
     * Disable interrupts.
     */
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    if (err)
        goto _error;

    unmountFlag = FALSE;
    if (close_dot_tags(Prog))
        goto _error;

    return 0;

_error:

    if (unmountFlag)
        (void)close_dot_tags(Prog);

    return 1;

}  /* end balance_main */

/*
 * make_bf_table
 */

static int
make_bf_table (
    bfDomainIdT fsId,        /* in */
    volDataT *volData        /* in */
)
{

    bsBfDescT         bfDesc[BF_DESC_MAX];
    adv_bf_attr_t   bfAttr;
    int               bfDescCnt;
    adv_bf_info_t         bfInfo;
    char              dotTagsPathName[MAXPATHLEN+1];
    int               fd;
    int               i;
    setInfoT         *setInfo;
    adv_status_t      sts;
    int               err;
    unsigned          fileCount;
    struct stat       stats;
    bfTableEntryT    *thisBfEntry;
    bfTableEntryT    *bfEntry;
    bfTableEntryT    *nextBfEntry;
    unsigned          oldSourceVolSubscr;

    fileCount = 0;

    oldSourceVolSubscr = sourceVolSubscr;

    if (!volData[sourceVolSubscr].noMoreFiles) {

        sts = advfs_get_vol_bf_descs (fsId,
                  volData[sourceVolSubscr].volIndex,
                  BF_DESC_MAX,
                  &(bfDesc[0]),
                  &volData[sourceVolSubscr].nextBfDescId,
                  &bfDescCnt
        );

        if (sts != EOK) {
            fprintf(stderr, MSG(COMMON_ERRBFDESCS, 
                "%s: Cannot get volume file descriptors\n"), Prog);
            fprintf(stderr, CMSG(COMMON_ERR, 
                "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            return 1;
        }
    } else {
        bfDescCnt = 0;
    }

    while (bfDescCnt == 0) {
        volData[sourceVolSubscr].noMoreFiles = TRUE;
        sourceVolSubscr++;
        if (sourceVolSubscr == volIndexCnt) {
            sourceVolSubscr = 0;
        }

        if (sourceVolSubscr == oldSourceVolSubscr) {
            moreFiles = FALSE;
            return 0;
        }

        if (!volData[sourceVolSubscr].noMoreFiles) {

            sts = advfs_get_vol_bf_descs (fsId,
                      volData[sourceVolSubscr].volIndex,
                      BF_DESC_MAX,
                      &(bfDesc[0]),
                      &volData[sourceVolSubscr].nextBfDescId,
                      &bfDescCnt);

            if (sts != EOK) {
                fprintf (stderr, MSG(COMMON_ERRBFDESCS, 
                    "%s: Cannot get volume file descriptors\n"), Prog);
                fprintf(stderr, CMSG(COMMON_ERR, 
                    "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                return 1;
            }
        }
    } /* end while */

    bfTableRoot = NULL;

    while (bfDescCnt > 0) {

        for (i = 0; i < bfDescCnt; i++) {

            if (bfDesc[i].metadataType == BMT_NORMAL_METADATA) {
                /*
                 * This is not extent metadata, which is the only thing
                 * that the balancer migrates.
                 */
                continue;
            }

            if (BS_BFTAG_RSVD (bfDesc[i].bfSetTag)) {
                /*
                 * This is a bitfile set.
                 */
                continue;
            }

            /* Avoid processing duplicate entries. */
            if (bfTableRoot) {
                sEntry = bfTableRoot;
                tEntry = NULL;
                if (!insert_bf_entry(bfTableRoot, &bfDesc[i], &thisBfEntry)) {
                    continue;
                }
            } else {
                bfTableRoot = new_bf_entry();
                if (!bfTableRoot) {
                    fprintf (stderr, CMSG(COMMON_NOMEM, 
                        "%s: insufficient memory.\n"), Prog);
                    return 1;
                }

                bfTableRoot->bfSetTag = bfDesc[i].bfSetTag;
                bfTableRoot->bfTag = bfDesc[i].bfTag;
                bfTableRoot->noMove = FALSE;
                bfTableRoot->leftChild = NULL;
                bfTableRoot->rightChild = NULL;
                bfTableRoot->balance = 0;
                thisBfEntry = bfTableRoot;
            }

            /*
             * This is a normal bitfile.
             */

            setInfo = find_setTag(bfDesc[i].bfSetTag);

            if (setInfo == NULL) {
                /* print out set name */
                fprintf(stderr, CMSG(COMMON_NOTAG, 
                         "%s: Cannot find file set info - tag %d.%d\n"),
                         Prog, bfDesc[i].bfSetTag.tag_num, 
                         bfDesc[i].bfSetTag.tag_seq);
                thisBfEntry->noMove = TRUE;
                continue;
            }

            sprintf(dotTagsPathName, "%s/0x%x.0x%x", setInfo->dotTagsPath,
                     bfDesc[i].bfTag.tag_num, bfDesc[i].bfTag.tag_seq);

            err = lstat (dotTagsPathName, &stats);
            if (err) {
                if (errno == ENOENT) {
                    /* The file was deleted. */
                    thisBfEntry->noMove = TRUE;
                    continue;
                }
                fprintf (stderr, CMSG(COMMON_STATS2, 
                    "%s: Error getting '%s' stats\n"),
                    Prog, dotTagsPathName);
                fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                         "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRMSG (errno));
                thisBfEntry->noMove = TRUE;
                continue;
            }

            if (!S_ISDIR(stats.st_mode) && !S_ISREG(stats.st_mode)) {
                thisBfEntry->noMove = TRUE;
                continue;
            }
            fileCount++;

            fd = open (dotTagsPathName, O_RDONLY, 0);
            if (fd < 0) {
                if (errno == ENOENT) {
                    /*
                     * The file was deleted.
                     */
                    thisBfEntry->noMove = TRUE;
                    continue;
                } else {
                    fprintf(stderr, CMSG(COMMON_NOOPEN2, 
                        "%s: open of %s failed.\n"), Prog,
                        get_tag_path(setInfo, bfDesc[i].bfTag,
                            bfDesc[i].bfSetTag, &err)
                    );
                    fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                        "%s: Error = [%d] %s\n"), Prog, errno, ERRMSG(errno));
                    thisBfEntry->noMove = TRUE;
                    continue;
                }
            }

            /*
             * Get the file's size.
             */
            sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);  
            if (sts != EOK) {
                fprintf (stderr, CMSG(COMMON_ERRBFPARAMS, 
                    "%s: Cannot get file %s parameters\n"), Prog, 
                    get_tag_path(setInfo, bfDesc[i].bfTag,
                        bfDesc[i].bfSetTag, &err)
                );
                fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                    "%s: Error = [%d] %s\n"), Prog, sts, BSERRMSG (sts));
                (void)close(fd);
                thisBfEntry->noMove = TRUE;
                continue;
            }

            if (bfInfo.mbfNumFobs == 0) {
                thisBfEntry->noMove = TRUE;
            } else {
                thisBfEntry->fileBlockCount = bfInfo.mbfNumFobs;
            }

            if (close (fd)) {
                fprintf( stderr, CMSG(COMMON_NOCLOSE2, 
                    "%s: close of %s failed.\n"), Prog, dotTagsPathName);
                fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                    "%s: Error = [%d] %s\n"), Prog, errno, ERRMSG(errno));
                thisBfEntry->noMove = TRUE;
                continue;
            }

        }  /* end for */

        /* Get the next group of bitfile descriptors. */
        if (fileCount >= FILE_GROUP_SIZE) {
            goto linearize_table1;
        }
        sourceVolSubscr++;
        if (sourceVolSubscr == volIndexCnt) {
            sourceVolSubscr = 0;
        }
        oldSourceVolSubscr = sourceVolSubscr;

        if (!volData[sourceVolSubscr].noMoreFiles) {

            sts = advfs_get_vol_bf_descs (fsId,
                      volData[sourceVolSubscr].volIndex,
                      BF_DESC_MAX,
                      &(bfDesc[0]),
                      &volData[sourceVolSubscr].nextBfDescId,
                      &bfDescCnt
            );

            if (sts != EOK) {
                fprintf (stderr, MSG(COMMON_ERRBFDESCS,
                    "%s: Cannot get volume file descriptors\n"), Prog);
                fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                    "%s: Error = [%d] %s\n"), Prog, sts, BSERRMSG (sts));
                return 1;
            }
        } else {
            bfDescCnt = 0;
        }

        while (bfDescCnt == 0) {
            volData[sourceVolSubscr].noMoreFiles = TRUE;
            sourceVolSubscr++;
            if (sourceVolSubscr == volIndexCnt) {
                sourceVolSubscr = 0;
            }

            if (sourceVolSubscr == oldSourceVolSubscr) {
                moreFiles = FALSE;
                goto linearize_table1;
            }

            if (!volData[sourceVolSubscr].noMoreFiles) {

                sts = advfs_get_vol_bf_descs (
                                fsId,
                                volData[sourceVolSubscr].volIndex,
                                BF_DESC_MAX,
                                &(bfDesc[0]),
                                &volData[sourceVolSubscr].nextBfDescId,
                                &bfDescCnt);
                if (sts != EOK) {
                    fprintf (stderr, MSG(COMMON_ERRBFDESCS,
                        "%s: Cannot get volume file descriptors\n"), Prog);
                    fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                        "%s: Error = [%d] %s\n"), Prog, sts, BSERRMSG (sts));
                    return 1;
                }
            }
        } /* end while */

    }  /* end while */

linearize_table1:

    bfEntry = bfTableRoot;
    bfTableRoot = NULL;
    linearize_bf_table(bfEntry);

    /*
     * Now, sort the table by file size.
     */

    if (bfTableRoot == NULL) {
        return 0;
    }

    bfEntry = bfTableRoot->rightChild;
    bfTableRoot->leftChild = NULL;
    bfTableRoot->rightChild = NULL;
    bfTableRoot->balance = 0;
    reinsertSideFlag = FALSE;
    while (bfEntry) {
        nextBfEntry = bfEntry->rightChild;
        sEntry = bfTableRoot;
        tEntry = NULL;
        reinsert_bf_entry(bfTableRoot, bfEntry);
        bfEntry = nextBfEntry;
    }

    if (bfTableRoot == NULL) {
        return 0;
    }

    bfEntry = bfTableRoot;
    bfTableRoot = NULL;
    linearize_bf_table(bfEntry);
    return 0;

}  /* end make_bf_table */

/*
 * move_files
 */
static int
move_files (
    adv_bf_dmn_params_t *fsParams,  /* in */
    volDataT *volData         /* in */
)
{
    adv_bf_attr_t        bfAttr;
    adv_bf_info_t              bfInfo;
    char                   dotTagsPathName[MAXPATHLEN+1];
    int                    fd;
    setInfoT               *setInfo;
    adv_status_t           sts;
    int                    err;
    int                    i;
    struct stat            stats;
    int                    subpassNumber;
    bfTableEntryT         *bfEntry;
    bfTableEntryT         *nextBfEntry;
    int                    fileMoveCount;
    boolean                moveFlag;
    int                    targetVolumeIndex;
    uint64_t               fobsToMove, srcFobOffset;

    fileMoveCount = 0;

    for (bfEntry = bfTableRoot; bfEntry != NULL; bfEntry = nextBfEntry) {
        nextBfEntry = bfEntry->rightChild;
        if (fileMoveCount == 10) {
            fileMoveCount = 0;
        }

        if (fileMoveCount == 0) {
            err = update_volume_data(fsParams, volData);
            if (err) {
                return 1;
            }
        }

        if (fsBalanceDeviation < BALANCE_GOAL * volIndexCnt) {
            moreFiles = FALSE;
            return 0;
        }

        if (bfEntry->noMove) {
            goto move_next_file;
        }

        setInfo = find_setTag(bfEntry->bfSetTag);
        if (setInfo == NULL) {
            fprintf (stderr, CMSG(COMMON_NOHDR, 
                "%s: Cannot find file set info - set tag %d.%d, file tag %d.%d - volume %d\n"),
                Prog, bfEntry->bfSetTag.tag_num, bfEntry->bfSetTag.tag_seq);
            goto move_next_file;
        }

        sprintf(dotTagsPathName, "%s/0x%x.0x%x", setInfo->dotTagsPath,
                 bfEntry->bfTag.tag_num, bfEntry->bfTag.tag_seq);

        fd = open (dotTagsPathName, O_RDONLY, 0);
        if (fd < 0) {
            if (errno == ENOENT) {
                /*
                 * The file was deleted.
                 */
                goto move_next_file;
            } else {
                fprintf(stderr, CMSG(COMMON_NOOPEN2, "%s: open of %s failed.\n"),
                    Prog, 
                    get_tag_path(setInfo, bfEntry->bfTag, 
                        bfEntry->bfSetTag, &err)
                );
                fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                    "%s: Error = [%d] %s\n"), Prog, errno, ERRMSG (errno));
                goto move_next_file;
            }
        }

        /*
         * Move extent metadata and extents.
         */
        sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);  
        if (sts != EOK) {
            fprintf (stderr, MSG(COMMON_ERRBFPARAMS, 
                    "%s: Cannot get file %s parameters\n"), Prog, 
                    get_tag_path(setInfo, bfEntry->bfTag,
                        bfEntry->bfSetTag, &err)
            );
            fprintf (stderr, CMSG(COMMON_ERR, 
                "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            (void) close (fd);
            goto move_next_file;
        }
        err = decide_move(
                    fd,
                    volData,
                    bfInfo.pageSize,
                    bfEntry->fileBlockCount,
                    &targetVolumeIndex,
                    &moveFlag);

        if (!err & moveFlag) {
            uint32_t forceFlag = 0;

            fileMoveCount++;

            if (verboseFlag) {
                fprintf (stdout, MSG(BAL_MOVING, 
                    "%s: Moving file %s to volume %d\n"),
                    Prog,
                    get_tag_path(setInfo, bfEntry->bfTag, 
                        bfEntry->bfSetTag, &err),
                    targetVolumeIndex
                );
		(void)fflush(stdout);
            }

retry_move:

            fobsToMove = bfInfo.mbfNextFob;
            srcFobOffset = 0;
            while (fobsToMove > 0)
            {
                uint64_t chunk;

                chunk = fobsToMove;

                sts = advfs_migrate(
                                    fd,
                                    -1, /* srcVolIndex */
                                    srcFobOffset, /* srcFobOffset */
                                    chunk,
                                    targetVolumeIndex,
                                    -1,  /* dstBlkOffset */
                                    forceFlag);

                
                if (sts == E_INVOLUNTARY_ABORT && !forceFlag) {
                    forceFlag = 1;
                    goto retry_move;
                }

                if (sts != EOK) {
                    if (sts != E_MIGRATE_IN_PROGRESS && sts != E_NO_SNAPSHOT_STG &&
                       sts != E_CANT_MIGRATE_HOLE) {
                        fprintf (stderr, CMSG(COMMON_CANTMOVE,
                            "%s: Cannot move file %s\n"), Prog, 
                            get_tag_path(setInfo, bfEntry->bfTag,
                                bfEntry->bfSetTag, &err)
                        );
                        fprintf (stderr, CMSG(COMMON_ERR,
                                 "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                    }
                    if (sts != ENO_MORE_BLKS) {
                        (void) close (fd);
                        goto move_next_file;
                    }
                }
                fobsToMove -= chunk;
                srcFobOffset += chunk;
            }
        }

        if (close (fd)) {
            fprintf( stderr, CMSG(COMMON_NOCLOSE2, "%s: close of %s failed.\n"),
                Prog, dotTagsPathName);
            fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, 
                "%s: Error = [%d] %s\n"), Prog, errno, ERRMSG (errno));
            goto move_next_file;
        }
move_next_file:
        /* Put the current bf entry on the free list, for reuse in the next call
         * to move_files. */
        bfEntry->rightChild = bfEntryFreeList;
        bfEntryFreeList = bfEntry;
    }

    return 0;

}  /* end move_files */

/* This is a recursive routine to insert an entry into the bf table.
 * It returns TRUE if it is successful. */
static boolean
insert_bf_entry(
    bfTableEntryT *table,         /* in */
    bsBfDescT *datum,             /* in */
    bfTableEntryT **retNewEntry)  /* out */
{
    bfTableEntryT *newEntry;

    /* If this entry's datum is equal to the new datum, then the insert
     * fails. */
    if (BS_BFTAG_EQL(table->bfSetTag, datum->bfSetTag) &&
        BS_BFTAG_EQL(table->bfTag, datum->bfTag)) {
        return FALSE;
    }

    /* If this entry's datum is less than the new datum, the new
     * datum belongs in the right subtree, otherwise the left subtree.
     * If there is already such a subtree, make a recursive call to
     * effect the insertion; otherwise, insert the new entry here.
     * Note that memory allocation failures are simply treated as
     * ordinary failures, and no entry is made, because if this happens,
     * something else will fail soon enough anyway. */
    if (BS_BFTAG_LESS(table->bfSetTag, datum->bfSetTag) ||
        (BS_BFTAG_EQL(table->bfSetTag, datum->bfSetTag) &&
         BS_BFTAG_LESS(table->bfTag, datum->bfTag))) {
        if (table->rightChild) {
            if (table->rightChild->balance != 0) {
                tEntry = table;
                sEntry = table->rightChild;
            }
            return insert_bf_entry(table->rightChild, datum, retNewEntry);
        } else {
            newEntry = new_bf_entry();
            if (!newEntry) {
                return FALSE;
            }
            newEntry->bfSetTag = datum->bfSetTag;
            newEntry->bfTag = datum->bfTag;
            newEntry->noMove = FALSE;
            newEntry->rightChild = NULL;
            newEntry->leftChild = NULL;
            newEntry->balance = 0;
            table->rightChild = newEntry;
            *retNewEntry = newEntry;
            adjust_bf_table(newEntry, compare_insert);
            return TRUE;
        }
    } else {
        if (table->leftChild) {
            if (table->leftChild->balance != 0) {
                tEntry = table;
                sEntry = table->leftChild;
            }
            return insert_bf_entry(table->leftChild, datum, retNewEntry);
        }
        else {
            newEntry = new_bf_entry();
            if (!newEntry) {
                return FALSE;
            }
            newEntry->bfSetTag = datum->bfSetTag;
            newEntry->bfTag = datum->bfTag;
            newEntry->noMove = FALSE;
            newEntry->rightChild = NULL;
            newEntry->leftChild = NULL;
            newEntry->balance = 0;
            table->leftChild = newEntry;
            *retNewEntry = newEntry;
            adjust_bf_table(newEntry, compare_insert);
            return TRUE;
        }
    }
} /* end insert_bf_entry */

/*
 * linearize_bf_table is a recursive routine that transforms the bfTable
 * into a linear list linked by the rightChild pointer.
 */
static void
linearize_bf_table(
    bfTableEntryT *entry
) 
{
    bfTableEntryT *oldLeftChild;

    if (entry->rightChild) {
        linearize_bf_table(entry->rightChild);
    }
    oldLeftChild = entry->leftChild;
    entry->rightChild = bfTableRoot;
    bfTableRoot = entry;

    if (entry->leftChild) {
        linearize_bf_table(oldLeftChild);
    }
} /* end linearize_bf_table */

static boolean
compare_insert(
    bfTableEntryT *aEntry,
    bfTableEntryT *bEntry
)
{
    return (!(BS_BFTAG_LESS(bEntry->bfSetTag, aEntry->bfSetTag) ||
              (BS_BFTAG_EQL(bEntry->bfSetTag, aEntry->bfSetTag) &&
               BS_BFTAG_LESS(bEntry->bfTag, aEntry->bfTag))));
} /* end compare_insert */

static boolean
compare_reinsert(
    bfTableEntryT *aEntry,
    bfTableEntryT *bEntry
)
{
    return(aEntry->fileBlockCount > bEntry->fileBlockCount ||
           (aEntry->fileBlockCount == bEntry->fileBlockCount &&
            reinsertSideFlag));
} /* end compare_reinsert */


/*
 * reinsert_bf_entry
 * This routine inserts the given entry into the table, using the file
 * block count as a sort key.
 */
static void
reinsert_bf_entry(
    bfTableEntryT *table,
    bfTableEntryT *entry
)
{
    if (entry->noMove) {
        entry->rightChild = bfEntryFreeList;
        bfEntryFreeList = entry;
        return;
    }

    entry->leftChild = NULL;
    entry->rightChild = NULL;
    entry->balance = 0;

    if (compare_reinsert(entry, table)) {
        if (table->leftChild) {
            if (table->leftChild->balance != 0) {
                tEntry = table;
                sEntry = table->leftChild;
            }
            reinsert_bf_entry(table->leftChild, entry);
        } else {
            table->leftChild = entry;
            adjust_bf_table(entry, compare_reinsert);
            reinsertSideFlag = !reinsertSideFlag;
        }
    } else {
        if (table->rightChild) {
            if (table->rightChild->balance != 0) {
                tEntry = table;
                sEntry = table->rightChild;
            }
            reinsert_bf_entry(table->rightChild, entry);
        } else {
            table->rightChild = entry;
            adjust_bf_table(entry, compare_reinsert);
            reinsertSideFlag = !reinsertSideFlag;
        }
    }
} /* end reinsert_bf_entry */

/* Adjust the node balance counts of the bf table, and perform a rotation
 * if necessary.
 */
static void
adjust_bf_table(
    bfTableEntryT *entry,                 /* in */
    compare_bf_entriesT *compare_routine) /* in */
{
    int            side;
    bfTableEntryT *pEntry;
    bfTableEntryT *rEntry;

    if (compare_routine(entry, sEntry)) {
        pEntry = sEntry->leftChild;
        side = -1;
    } else {
        pEntry = sEntry->rightChild;
        side = +1;
    }
    rEntry = pEntry;

    while (pEntry != entry) {
        if (compare_routine(entry, pEntry)) {
            pEntry->balance = -1;
            pEntry = pEntry->leftChild;
        } else {
            pEntry->balance = +1;
            pEntry = pEntry->rightChild;
        }
    }

    if (sEntry->balance == 0) {
        sEntry->balance = side;
    } else if (sEntry->balance == -side) {
        sEntry->balance = 0;
    } else /* sEntry->balance == side */ {
        if (rEntry->balance == side) {
            /* Single Rotation */
            pEntry = rEntry;
            if (side == -1) {
                sEntry->leftChild = rEntry->rightChild;
                rEntry->rightChild = sEntry;
            } else /* side == +1 */ {
                sEntry->rightChild = rEntry->leftChild;
                rEntry->leftChild = sEntry;
            }
            sEntry->balance = 0;
            rEntry->balance = 0;
        } else /* rEntry->balance == -side */ {
            /* Double Rotation */
            if (side == -1) {
                pEntry = rEntry->rightChild;
                rEntry->rightChild = pEntry->leftChild;
                pEntry->leftChild = rEntry;
                sEntry->leftChild = pEntry->rightChild;
                pEntry->rightChild = sEntry;
            } else /* side == +1 */ {
                pEntry = rEntry->leftChild;
                rEntry->leftChild = pEntry->rightChild;
                pEntry->rightChild = rEntry;
                sEntry->rightChild = pEntry->leftChild;
                pEntry->leftChild = sEntry;
            }
            if (pEntry->balance == side) {
                sEntry->balance = -side;
                rEntry->balance = 0;
            } else if (pEntry->balance == 0) {
                sEntry->balance = 0;
                rEntry->balance = 0;
            } else /* pEntry->balance == -side */ {
                sEntry->balance = 0;
                rEntry->balance = side;
            }
            pEntry->balance = 0;
        }
        if (tEntry) {
            if (sEntry == tEntry->rightChild) {
                tEntry->rightChild = pEntry;
            } else /* sEntry == tEntry->leftChild */ {
                tEntry->leftChild = pEntry;
            }
        } else {
            bfTableRoot = pEntry;
        }
    }
} /* adjust_bf_table */

/*
 * get_block_distribution
 *
 * Count the number of blocks in a file's storate that are allocated
 * on each volume, and place the results in volBlockDistribution.
 */


static adv_status_t
get_block_distribution (
    int fd,                  /* in */
    volDataT *volData,       /* in */
    int pageSize,            /* in */
    boolean *onSingleVolume  /* out */
)
{

    uint32_t           allocVolIndex;
    unsigned           volIndex;
    int                i;
    bsExtentDescT      rawXtntDesc[XTNT_DESC_MAX];
    int                rawXtntDescCnt;
    int                startXtntDesc;
    adv_status_t       sts;
    int                xtntDescCnt = 0;
    int                xtntDescMaxCnt = 0;
    int                volSubscr;

    for (i = 0; i < volIndexCnt; i++) {
        volBlockDistribution[i] = 0;
    }

    startXtntDesc = 0;
    sts = advfs_get_bf_xtnt_map (
                        fd,
                        1,
                        startXtntDesc,
                        XTNT_DESC_MAX,
                        &(rawXtntDesc[0]),
                        &rawXtntDescCnt,
                        &allocVolIndex
                        );
    while (sts == EOK) {

        for (i = 0; i < rawXtntDescCnt; i++) {
            if ( rawXtntDesc[i].bsed_vd_blk != XTNT_TERM &&
                 rawXtntDesc[i].bsed_vd_blk != COWED_HOLE )
            {
                volBlockDistribution[volSubscrArray[rawXtntDesc[i].bsed_vol_index]] +=
                    rawXtntDesc[i].bsed_fob_cnt * pageSize;
            }
        }

        startXtntDesc = startXtntDesc + rawXtntDescCnt;

        sts = advfs_get_bf_xtnt_map (
                        fd,
                        1,
                        startXtntDesc,
                        XTNT_DESC_MAX,
                        &(rawXtntDesc[0]),
                        &rawXtntDescCnt,
                        &allocVolIndex
                        );

    }  /* end while */

    if (sts != ENO_XTNTS) {
        fprintf (stderr, MSG(COMMON_ERREXTENT, 
            "%s: Cannot get extent descriptors\n"), Prog);
        fprintf (stderr, CMSG(COMMON_ERR, 
            "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        return sts;
    }

    *onSingleVolume = FALSE;
    for (i = 0; i < volIndexCnt; i++) {
        if (volBlockDistribution[i] != 0) {
            if (*onSingleVolume) {
                *onSingleVolume = FALSE;
                break;
            } else {
                *onSingleVolume = TRUE;
            }
        }
    }

    return EOK;

}  /* end get_block_distribution */

static int
update_volume_data(
        adv_bf_dmn_params_t *fsParams,  /* in */
        volDataT *volData)	/* in/out */
{
    adv_vol_counters_t     volCounters;
    adv_vol_info_t         volInfo;
    adv_vol_ioq_params_t    volIoQParams;
    adv_vol_prop_t   volProperties;
    int                i;
    adv_status_t       sts;

    fsSize = 0;
    fsFreeSize = 0;

    /*
     * Determine how much free space there is on each volume.
     */
    for (i = 0; i < volIndexCnt; i++) {

        sts = advfs_get_vol_params(
                fsParams->bfDomainId,
                volData[i].volIndex,
                &volInfo,
                &volProperties,
                &volIoQParams,
                &volCounters);

        /*  the volIndex can have holes in it.  Don't fail
         *  on EBAD_VDI
         */
        if (sts != EOK && sts != EBAD_VDI) {
            fprintf(stderr, CMSG(COMMON_NOVOLPARAMS, 
                "%s: Cannot get volume parameters for %s\n"),
                Prog, volProperties.volName);
            return 1;
        }

        volData[i].volSize = volInfo.volSize;
        volData[i].volFreeSize = volInfo.freeClusters * volInfo.sbmBlksBit;
        volData[i].freeSizeFract = (double)volData[i].volFreeSize /
            volData[i].volSize;

        fsSize += volData[i].volSize;
        fsFreeSize += volData[i].volFreeSize;
    }

    fsFreeSizeFract = (double)fsFreeSize / fsSize;

    /*
     * Calculate the total deviation of the filesystem from perfect balance.
     */
    fsBalanceDeviation = 0.0;
    for (i = 0; i < volIndexCnt; i++) {
        fsBalanceDeviation += fabs(volData[i].freeSizeFract - fsFreeSizeFract);
    }

    return 0;

} /* end update_volume_data */

/*
 * decide_move
 * Decide if moving a file to a new target volume will improve the filesystem's
 * balance.  If it will, update the filesystem's data to reflect the move.
 */
static int
decide_move(
    int       fd,                 /* in */
    volDataT *volData,            /* in */
    int       pageSize,           /* in */
    uint64_t  fileBlockCount,       /* in */
    int      *targetVolumeIndex,  /* out */
    boolean  *moveFlag)           /* out */
{
    int     targetSubscr;
    int     bestTargetSubscr;
    int     volSubscr;
    int     err;
    int64_t newVolFreeSize;
    boolean onSingleVolume;
    double  newBalanceDeviation;
    double  bestNewBalanceDeviation = fsBalanceDeviation + 1.0;
    double  targFreePct, srcFreePct;

    err = get_block_distribution(fd, volData, pageSize, &onSingleVolume);

    for (targetSubscr = 0; targetSubscr < volIndexCnt; targetSubscr++) {
        /* Don't consider moving the file to this volume if either the volume
         * doesn't have enough free space to hold the whole file (the whole
         * file must be allocated afresh before any of the old space occupied
         * by the file can be freed, so the presently occupied space cannot be
         * counted) or the whole file is already on this volume. 
         */

        if ((fileBlockCount >= volData[targetSubscr].volFreeSize) ||
            (onSingleVolume && volBlockDistribution[targetSubscr] > 0)) {
            continue;
        }

        newBalanceDeviation = 0.0;

        for (volSubscr = 0; volSubscr < volIndexCnt; volSubscr++) {
            /* increase the free size of the volume if file is to be moved
             * from this volume.
             */
            newVolFreeSize = volData[volSubscr].volFreeSize +
                volBlockDistribution[volSubscr];

            /* decrement the free size of the target volume */
            if (volSubscr == targetSubscr) {
                newVolFreeSize -= fileBlockCount;
            }

            /* calculate delta for this volume from the filesystem average 
             * free space keep a running total for all volumes in filesystem 
             */

            newBalanceDeviation += 
                fabs((double)newVolFreeSize / volData[volSubscr].volSize -
                    fsFreeSizeFract);
        }

        /* if better than last, set to new best  */
        if (newBalanceDeviation < bestNewBalanceDeviation) {
            bestNewBalanceDeviation = newBalanceDeviation;
            /* this should be the new target volume */
            bestTargetSubscr = targetSubscr;
        }
    }

    /* is new best deviation from all volumes above better
     * the existing filesystem deviation?
     */
    if (bestNewBalanceDeviation < fsBalanceDeviation) {
        /*
         * This code checks to see that the new free area in the target volume
         * is less than the free area that will remain on each of the volumes where
         * the file used to be.  This prevents a very large file from being
         * moved to a small volume, whereby the small volume would then show
         * a possibly very large percentage of its disk suddenly consumed.
         * Think of this as a threshold test where the target is checked against
         * the other volumes' thresholds.
        */
        for (volSubscr = 0; volSubscr < volIndexCnt; volSubscr++) {
            /* skip if volume is the same as the target volume */
            if (volSubscr == targetSubscr) continue;
            /* process only if some blocks on the source volumes */
            if (volBlockDistribution[volSubscr]) {
                targFreePct = (double)(volData[bestTargetSubscr].volFreeSize +
                    volBlockDistribution[bestTargetSubscr] -
                    fileBlockCount) / volData[bestTargetSubscr].volSize;

                srcFreePct = (double) (volData[volSubscr].volFreeSize +
                    volBlockDistribution[volSubscr] )
                    / volData[volSubscr].volSize;

                if ( targFreePct < srcFreePct ) {
                   *moveFlag = FALSE;
                   return 0;
                }
             }
        }

        /* set target volume to the volume that will help  */
        *moveFlag = TRUE;
        *targetVolumeIndex = volData[bestTargetSubscr].volIndex;
        fsBalanceDeviation = bestNewBalanceDeviation;
        /* adjust each volumes parameters - for next call to this routine */
        for (volSubscr = 0; volSubscr < volIndexCnt; volSubscr++) {
            volData[volSubscr].volFreeSize += volBlockDistribution[volSubscr];
            if (volSubscr == bestTargetSubscr) {
                volData[volSubscr].volFreeSize -= fileBlockCount;
            }
            volData[volSubscr].freeSizeFract =
                (double)volData[volSubscr].volFreeSize / volData[volSubscr].volSize;
        }
    } else {
        *moveFlag = FALSE;
    }

    if (err) {
        return 1;
    }

    return 0;
} /* end decide_move */

/* end balance.c */

