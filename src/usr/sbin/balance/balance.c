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
 *    MegaSafe Storage System
 *
 * Abstract:
 *
 *    balance.c
 *    This module implements the domain balancer utility.
 *
 * Date:
 *
 *    December 17, 1993
 *
 * Revision History:
 *
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: balance.c,v $ $Revision: 1.1.21.2 $ (DEC) $Date: 2002/01/03 17:12:54 $";
#endif

#include <stdio.h>
#include <math.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/signal.h>
#include <sys/mount.h>
#include <sys/param.h>
#include <msfs/ms_public.h>
#include <msfs/ms_privates.h>
#include <msfs/msfs_syscalls.h>
#include <locale.h>
#include "balance_msg.h"
#include <msfs/advfs_evm.h>

nl_catd catd;

unsigned long pages_to_migrate = -1;

#define BALANCE_GOAL 0.002
#define FILE_GROUP_SIZE 5000

#define TRUE 1
#define FALSE 0

/* from bs_ods.h */
/* An extent with vdBlk set to XTNT_TERM terminates the previous descriptor. */
#define XTNT_TERM ((uint32T)-1)
/* An extent with vdBlk set to PERM_HOLE_START starts a permanent hole */
/* desriptor in a clone file. */
#define PERM_HOLE_START ((uint32T)-2)

typedef int boolean;

extern int errno;
extern char *sys_errlist[];


/* These are macros, typedefs and routines that are used to implement a table
 * of bitfile descriptors.  The following macros are used to order tags in a
 * binary search tree.  We xor every other bit so as to get a non-monotonic
 * order in the usual case that the tags are encountered in order. */
#define XXOR(x) (x^0x55555555)
#define ML_BFTAG_LESS(tag1, tag2) \
    (((XXOR((tag1).num) < XXOR((tag2).num))) || \
     (((XXOR((tag1).num) == XXOR((tag2).num)) && ((tag1).seq < (tag2).seq))))

typedef struct bfTableEntryT {
    mlBfTagT bfSetTag;
    mlBfTagT bfTag;
    struct bfTableEntryT *leftChild;
    struct bfTableEntryT *rightChild;
    unsigned long fileBlockCount;
    char /*boolean*/ noMove;
    char balance;		/* actually, only needs two bits! */
} bfTableEntryT;

static bfTableEntryT *bfEntryFreeList;

static
bfTableEntryT
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

static
boolean
insert_bf_entry(
            bfTableEntryT *table,  /* in */
            mlBfDescT *datum,  /* in */
            bfTableEntryT **retNewEntry);  /* out */

static
void
linearize_bf_table(
            bfTableEntryT *entry);  /* in */

static
void
reinsert_bf_entry(
            bfTableEntryT *table,
            bfTableEntryT *entry);

/* These functions return TRUE if the first entry goes to the left
 * of the second entry. */
typedef
boolean
compare_bf_entriesT(
            bfTableEntryT *aEntry,  /* in */
            bfTableEntryT *bEntry);  /* in */

static compare_bf_entriesT compare_insert;
static compare_bf_entriesT compare_reinsert;

static
void
adjust_bf_table(
            bfTableEntryT *entry,
            compare_bf_entriesT *compare_routine);  /* in */
 
#define ERRMSG(err) sys_errlist[err]

typedef struct {
    u32T pageOffset;
    u32T pageCnt;
    unsigned volIndex;
    unsigned volSubscr;
} xtntDescT;

typedef struct {
    unsigned volIndex;
    unsigned long volSize;
    unsigned long volStgCluster;
    unsigned long volFreeSize;
    double freeSizeFract;
    u32T nextBfDescId;
    boolean noMoreFiles;
} volDataT;

typedef struct setHdr {
    mlBfTagT setTag;
    int mountedFlag;
    int unmountFlag;
    char *mntOnName;
    char *dotTagsPath;
    int fd;
} setHdrT;

static char *dmnName;
static char *dotTags = "/.tags";
static char *sbmRootPathName = NULL;
static char *Prog;
static int volIndexCnt;
static int *volSubscrArray;
static int setHdrCnt = 0;
static setHdrT *setHdrTbl = NULL;
static boolean unmountFlag = FALSE;
static boolean verboseFlag = FALSE;
static bfTableEntryT *bfTableRoot;

static unsigned sourceVolSubscr;
static boolean moreFiles;

static boolean reinsertSideFlag;
static unsigned long dmnSize;
static unsigned long dmnFreeSize;
static double dmnFreeSizeFract;
static double dmnBalanceDeviation;

static int thisDmnLocked=0;
static advfs_ev advfs_event;

static struct {
        setHdrT *setHdr;
        mlBfTagT bfTag;
        char pathName[MAXPATHLEN+1];
        } currentPath;
/*
 * These two pointers are used to implement the AVL tree balancing algorithm,
 * described in Knuth, vol 3, page 455-7.
 */
static bfTableEntryT *sEntry; /* Entry where re-balancing of tree may be necessary. */
static bfTableEntryT *tEntry; /* Parent of sEntry, or NULL. */

static long *volBlockDistribution;

/*
 * Private protos
 */

static
int
balance_main (
            char *dmnName  /* in */
            );

static
int
make_bf_table (
               mlBfDomainIdT domainId,  /* in */
               volDataT *volData /* in */
               );
static
int
move_files (
               mlDmnParamsT *dmnParams,  /* in */
               volDataT *volData /* in */
               );

static
int
update_volume_data (
        mlDmnParamsT *dmnParams,  /* in */
        volDataT *volData);	/* in/out */

static
mlStatusT
get_block_distribution (
                       int fd,  /* in */
                       volDataT *volData, /* in */
                       int pageSize,  /* in */
                       boolean *onSingleVolume  /* out */
                       );
static
int
mount_bitfile_sets (
                    char *dmnName,  /* in */
                    mlBfTagT rootBfSetTag  /* in */
                    );

static
int
unmount_bitfile_sets (
                      );

static
setHdrT *
insert_set_tag (
                mlBfTagT setTag  /* in */
                );

static
setHdrT *
find_set_tag (
              mlBfTagT setTag  /* in */
              );

static
int
decide_move(
        int fd,  /* in */
        volDataT *volData,  /* in */
        int pageSize,  /* in */
        unsigned long fileBlockCount,  /* in */
        int *targetVolumeIndex,  /* out */
        boolean *moveFlag);  /* out */

static
char *
get_current_path_name(void);


/*
 * This is the balance signal handler.  It gives this program a chance
 * to cleanup before quiting.  Specifically, it unmounts bitfile sets
 * and unlocks the domain.
 */

void sigHandler (
                 int signal  /* in */
                 )

{

    int err;
    mlStatusT sts;

    fprintf (stderr, catgets(catd, 1, 1, "%s: Stopping balance operation...\n"), Prog);

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

    if (unmountFlag) {
        unmountFlag = FALSE;
        (void)unmount_bitfile_sets ();
    }

    fprintf (stderr, catgets(catd, 1, 3, "%s: Domain '%s' not balanced\n"),
             Prog, dmnName); 

    if (thisDmnLocked) {
        advfs_post_user_event(EVENT_FDMN_BAL_ERROR, advfs_event, Prog);
    }
    exit (1);

}  /* end sigHandler */


static
void
usage (
       void
       )
{
    fprintf (stderr, catgets(catd, 1, USAGE, "usage: %s [-v] domain\n"), Prog);
}

main (
      int argc,
      char *argv[]
      )

{

    int c;
    int err;
    int err2;
    extern int getopt();
    extern char *optarg;
    extern int optind;
    extern int opterr;
    struct stat stats;
    mlDmnParamsT dmnParams;
    mlSSDmnParamsT ssDmnParams;
    mlStatusT sts=EOK;
    mlSSDmnOpsT ssDmnCurrentState=0;
    int ssRunningState=0;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_BALANCE, NL_CAT_LOCALE);


    /* store only the file name part of argv[0] */
    if ((Prog = (char *) strrchr (argv[0], '/')) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
    if (geteuid())
    {
        fprintf(stderr, catgets(catd,1,42,
				"\nPermission denied - user must be root to run %s.\n\n"),
                argv[0]);
        usage();
        exit(1);
    }

    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt (argc, argv, "iv")) != EOF) {

        switch (c) {
          case 'i':

              /* The pages_to_migrate size should be a divisor of 8184. There
               * is a flaw in the storage allocation that skips the end of an
               * SBM page and moves to the next when there is not enough space
               * to satisify a Picky request. Since the sbm page has an 8 byte
               * header there are 8184 bits available (1 byte/page in V3 and
               * 1 bit/page in V4).
               */

              pages_to_migrate=341;
              break;
          
          case 'v':
            verboseFlag++;
            break;

          default:
            usage ();
            exit (1);
        }
    }  /* end while */

    if (optind != (argc - 1)) {
        /* missing required args */
        usage();
        exit(1);
    }

    if (verboseFlag > 1) {
        usage();
        exit(1);
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    err = msfs_check_on_disk_version(BFD_ODS_LAST_VERSION);
    if (err != EOK) {
        fprintf( stderr, catgets(catd, 1, ONDISK,
            "%s: This utility can not process the on-disk structures on this system.\n"), Prog);
        exit(1);
    }


    dmnName = argv[optind];

    init_event(&advfs_event);
    advfs_event.domain = dmnName;

    /*
     * If vfast is activated on a domain AND if vfast defragment OR
     * vfast balance OR vfast topIObalance are enabled, don't run
     * the balance utility.
     */
    sts = advfs_ss_dmn_ops(dmnName,
                           SS_UI_STATUS,
                           &ssDmnCurrentState,
                           &ssRunningState);

    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 76,
                 "%s: status not available on domain %s; %s\n"),
                 Prog, dmnName, BSERRMSG(sts));
        exit(1);
    }

    sts = advfs_ss_get_params(dmnName, &ssDmnParams);
    if (sts != EOK) {
        fprintf(stderr, catgets(catd, 1, 13,
                "%s: Can't get domain parameters\n"),
                Prog);
    }

    if (((ssDmnParams.ssDmnDefragment == 1) ||
         (ssDmnParams.ssDmnBalance == 1) ||
         (ssDmnParams.ssDmnSmartPlace == 1)) &&
        (ssDmnCurrentState == 1)) {
        fprintf(stderr, catgets(catd, 1, 44,
                "%s: vfast is currently running in "
                "the background, cannot run %s on domain '%s'\n"),
                Prog, Prog, dmnName);
        exit(1);
    }

    /*
     * Disable interrupts.
     */

    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    fprintf(stdout, catgets(catd, 1, 5, "%s: Balancing domain '%s'\n"), Prog, dmnName);
    (void)fflush(stdout);

    err = balance_main (dmnName);
    if (err != 0) {
        goto _error;
    }


    fprintf(stdout, catgets(catd, 1, 6, "%s: Balanced domain '%s'\n"), Prog, dmnName);
    (void)fflush(stdout);

    advfs_post_user_event(EVENT_FDMN_BAL_UNLOCK, advfs_event, Prog);
    return 0;

_error:

    fprintf(stderr, catgets(catd, 1, 7, "%s: Can't balance domain '%s'\n"), Prog, dmnName);

    if (thisDmnLocked) {
        advfs_post_user_event(EVENT_FDMN_BAL_ERROR, advfs_event, Prog);
    }

    return 1;

}  /* end main */

/*
 * balance_main
 *
 * This function balances the files on the specified domain.
 */

static
int
balance_main (
               char *dmnName  /* in */
               )

{

    int dmnLock;
    int thisDmnLock;
    mlDmnParamsT dmnParams;
    char dmnPathName[MAXPATHLEN+1];
    int err;
    int err2;
    int i;
    struct stat stats;
    mlStatusT sts;
    int unlockDmnFlag = 0;
    u32T *volIndexArray = NULL;
    int max_vdi;
    volDataT *volData = NULL;

    int lastExtentCount;
    int lastFileWithExtCount;
    int unimprovedPassCount = 0;

    /*
     * Lock the master domain directory.  This synchronizes us with domain and
     * bitfile set creation/deletion.
     */

    dmnLock = lock_file (MSFS_DMN_DIR, LOCK_EX, &err);
    if (dmnLock < 0) {
        fprintf (stderr, catgets(catd, 1, 8, "%s: error locking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRMSG(err));
        goto _error;
    }
    unlockDmnFlag = TRUE;

    /*
     * Verify that the domain exists.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, dmnName);
    err = lstat (dmnPathName, &stats);
    if (err) {
        if (errno == ENOENT) {
            fprintf (stderr, catgets(catd, 1, 10, "%s: Domain directory '%s' does not exist\n"),
                     Prog, dmnPathName);
        } else {
            fprintf (stderr, catgets(catd, 1, 11, "%s: Error getting '%s' stats.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                     Prog, errno, ERRMSG (errno));
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
                     catgets(catd, 1, 12, "%s: cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, 1, 8, "%s: error locking '%s'.\n"),
                     Prog, dmnPathName);
            fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRMSG(err));
        }
        goto _error;
    } else {
	thisDmnLocked = 1;
        advfs_post_user_event(EVENT_FDMN_BAL_LOCK, advfs_event, Prog);
    }

    /*
     * Get the domain's parameters and a list of its volumes.
     */

    sts = msfs_get_dmnname_params (dmnName, &dmnParams);
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 13, "%s: Can't get domain parameters\n"), Prog);
        if (sts != -1) {
            fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        } else {
            fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, ERRMSG (errno));
        }
        goto _error;
    }

    volIndexArray = (u32T *)malloc(dmnParams.curNumVols * sizeof(u32T));
    volData = (volDataT *)malloc(dmnParams.curNumVols * sizeof(volDataT));
    for (i = 0; i < dmnParams.curNumVols; i++) {
        volData[i].noMoreFiles = FALSE;
        volData[i].nextBfDescId = 0;
    }
    volBlockDistribution = (long *)malloc(dmnParams.curNumVols * sizeof(long));

    sts = msfs_get_dmn_vol_list (
                                 dmnParams.bfDomainId, 
                                 dmnParams.curNumVols, 
                                 volIndexArray,
                                 &volIndexCnt
                                 );

    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, catgets(catd, 1, 43, "%s: error: All filesets must be mounted\n"), Prog );
        goto _error;
    } else if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 15, "%s: Can't get domain's volume list\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }

    if (volIndexCnt < 2) {
        fprintf (stderr, catgets(catd, 1, 16, "%s: Domain must have at least two volumes\n"), Prog);
        goto _error;
    }

    for (i = 0; i < volIndexCnt; i++) {
        volData[i].volIndex = volIndexArray[i];
    }

    /* Set up an array that enables us to, given a volume index, find the
     * corresponding subscript into the index array. */
    max_vdi = 0;
    for (i = 0; i < volIndexCnt; i++) {
        if (volIndexArray[i] > max_vdi) {
            max_vdi = volIndexArray[i];
        }
    }
    volSubscrArray = (int *)malloc((max_vdi + 1) * sizeof(int));
    for (i = 0; i <= max_vdi; i++) {
        volSubscrArray[i] = -1;
    }
    for (i = 0; i < volIndexCnt; i++) {
        volSubscrArray[volIndexArray[i]] = i;
    }

    /*
     * Mount all bitfile sets in the domain.  The function also opens each
     * bitfile set's ".tags" directory to ensure that the set stays mounted
     * while the domain is balanced.
     */
    err = mount_bitfile_sets (dmnName, dmnParams.bfSetDirTag);
    if (err)
        goto _error;

    unmountFlag = TRUE;

    /*
     * Unlock the master domain directory.  Now, domains and
     * bitfile sets can be created and
     * deleted.  Plus, we can show the domain.
     */
    unlockDmnFlag = FALSE;
    err2 = unlock_file (dmnLock, &err);
    if (err2 < 0) {
        fprintf (stderr, catgets(catd, 1, 17, "%s: Error unlocking '%s'.\n"),
                 Prog, MSFS_DMN_DIR);
        fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                 Prog, err, ERRMSG (err));
        goto _error;
    }

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
         * Get a list of descriptors, one for each file in the domain that
         * has any storage.
         */

        err = make_bf_table(
                dmnParams.bfDomainId,
                volData);
        if (err) {
            goto quit_balance;
        }

        /*
         * Considering each file in turn, migrate it if it would improve
         * the balance deviation of the domain.
         */

        err = move_files(
                &dmnParams,
                volData);
        if (err) {
            goto quit_balance;
        }
    } while (moreFiles);

quit_balance:;

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
    if (unmount_bitfile_sets ())
        goto _error;

    return 0;

_error:

    if (unmountFlag)
        (void)unmount_bitfile_sets ();

    if (unlockDmnFlag) {
        unlockDmnFlag = FALSE;
        err2 = unlock_file (dmnLock, &err);
        if (err2 < 0) {
            fprintf (stderr, catgets(catd, 1, 17, "%s: Error unlocking '%s'.\n"),
                     Prog, MSFS_DMN_DIR);
            fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                     Prog, err, ERRMSG (err));
         }
    }

    return 1;

}  /* end balance_main */


#define BF_DESC_MAX 500

/*
 * make_bf_table
 */

static
int
make_bf_table (
               mlBfDomainIdT domainId,  /* in */
               volDataT *volData /* in */
               )
{

    mlBfDescT bfDesc[BF_DESC_MAX];
    mlBfAttributesT bfAttr;
    int bfDescCnt;
    mlBfInfoT bfInfo;
    char dotTagsPathName[MAXPATHLEN+1];
    int fd;
    int i;
    setHdrT *setHdr;
    mlStatusT sts;
    int err;
    unsigned fileCount;
    struct stat stats;
    bfTableEntryT *thisBfEntry;
    bfTableEntryT *bfEntry;
    bfTableEntryT *nextBfEntry;
    unsigned oldSourceVolSubscr;

    fileCount = 0;

    oldSourceVolSubscr = sourceVolSubscr;

    if (!volData[sourceVolSubscr].noMoreFiles) {

        sts = advfs_get_vol_bf_descs (
                                  domainId,
                                  volData[sourceVolSubscr].volIndex,
                                  BF_DESC_MAX,
                                  &(bfDesc[0]),
                                  &volData[sourceVolSubscr].nextBfDescId,
                                  &bfDescCnt
                                  );

        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, 18, "%s: Can't get volume file descriptors\n"), Prog);
            fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
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

            sts = advfs_get_vol_bf_descs (
                                domainId,
                                volData[sourceVolSubscr].volIndex,
                                BF_DESC_MAX,
                                &(bfDesc[0]),
                                &volData[sourceVolSubscr].nextBfDescId,
                                &bfDescCnt);
            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 18, "%s: Can't get volume file descriptors\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                return 1;
            }
        }
    } /* end while */

    bfTableRoot = NULL;

    while (bfDescCnt > 0) {

        for (i = 0; i < bfDescCnt; i++) {

            if (bfDesc[i].metadataType == ML_NORMAL_METADATA) {
                /*
                 * This is not extent metadata, which is the only thing
                 * that the balancer migrates.
                 */
                continue;
            }

            if (ML_BFTAG_RSVD (bfDesc[i].bfSetTag)) {
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
                    fprintf (stderr, catgets(catd, 1, 19, "%s: Can't allocate memory\n"), Prog);
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

            setHdr = find_set_tag (bfDesc[i].bfSetTag);
            if (setHdr == NULL) {
                /* print out set name?? */
                fprintf (stderr, catgets(catd, 1, 20, "%s: Can't find file set hdr - tag %d.%d\n"),
                         Prog, bfDesc[i].bfSetTag.num, bfDesc[i].bfSetTag.seq);
                thisBfEntry->noMove = TRUE;
                continue;
            }

            sprintf (
                     dotTagsPathName,
                     "%s/0x%x.0x%x",
                     setHdr->dotTagsPath,
                     bfDesc[i].bfTag.num,
                     bfDesc[i].bfTag.seq
                     );

            err = lstat (dotTagsPathName, &stats);
            if (err) {
                if (errno == ENOENT) {
                    /* The file was deleted. */
                    thisBfEntry->noMove = TRUE;
                    continue;
                }
                fprintf (stderr, catgets(catd, 1, 11, "%s: Error getting '%s' stats.\n"),
                         Prog, dotTagsPathName);
                fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRMSG (errno));
                thisBfEntry->noMove = TRUE;
                continue;
            }

            if (!S_ISDIR(stats.st_mode) && !S_ISREG(stats.st_mode)) {
                thisBfEntry->noMove = TRUE;
                continue;
            }
            fileCount++;

            currentPath.setHdr = setHdr;
            currentPath.bfTag = bfDesc[i].bfTag;
            currentPath.pathName[0] = '\0';

            fd = open (dotTagsPathName, O_RDONLY, 0);
            if (fd < 0) {
                if (errno == ENOENT) {
                    /*
                     * The file was deleted.
                     */
                    thisBfEntry->noMove = TRUE;
                    continue;
                } else {
                    fprintf (stderr, catgets(catd, 1, 21, "%s: Open of %s failed.\n"),
                             Prog, get_current_path_name());
                    fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                             Prog, errno, ERRMSG (errno));
                    thisBfEntry->noMove = TRUE;
                    continue;
                }
            }

            /*
             * Get the file's size.
             */
            sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);  
            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 22, "%s: Can't get file %s parameters\n"),
                         Prog, get_current_path_name());
                fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                (void) close (fd);
                thisBfEntry->noMove = TRUE;
                continue;
            }

            if (bfAttr.mapType != XMT_SIMPLE || bfInfo.numPages == 0) {
                thisBfEntry->noMove = TRUE;
            } else {
                thisBfEntry->fileBlockCount = bfInfo.pageSize * bfInfo.numPages;
            }

            if (close (fd)) {
                fprintf( stderr, catgets(catd, 1, 23, "%s: Close of %s failed.\n"),
                        Prog, dotTagsPathName);
                fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRMSG (errno));
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

            sts = advfs_get_vol_bf_descs (
                                  domainId,
                                  volData[sourceVolSubscr].volIndex,
                                  BF_DESC_MAX,
                                  &(bfDesc[0]),
                                  &volData[sourceVolSubscr].nextBfDescId,
                                  &bfDescCnt
                                  );

            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 18, "%s: Can't get volume file descriptors\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
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
                                domainId,
                                volData[sourceVolSubscr].volIndex,
                                BF_DESC_MAX,
                                &(bfDesc[0]),
                                &volData[sourceVolSubscr].nextBfDescId,
                                &bfDescCnt);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 18, "%s: Can't get volume file descriptors\n"), Prog);
                    fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
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
static
int
move_files (
               mlDmnParamsT *dmnParams,  /* in */
               volDataT *volData /* in */
               )
{
    mlBfAttributesT bfAttr;
    mlBfInfoT bfInfo;
    char dotTagsPathName[MAXPATHLEN+1];
    int fd;
    setHdrT *setHdr;
    mlStatusT sts;
    int err;
    int i;
    struct stat stats;
    int subpassNumber;
    bfTableEntryT *bfEntry;
    bfTableEntryT *nextBfEntry;
    int fileMoveCount;
    boolean moveFlag;
    int targetVolumeIndex;
    unsigned long pagesToMove, srcPageOffset;

    fileMoveCount = 0;
    for (bfEntry = bfTableRoot; bfEntry != NULL; bfEntry = nextBfEntry) {
        nextBfEntry = bfEntry->rightChild;
        if (fileMoveCount == 10) {
            fileMoveCount = 0;
        }

        if (fileMoveCount == 0) {
            err = update_volume_data(dmnParams, volData);
            if (err) {
                return 1;
            }
        }

        if (dmnBalanceDeviation < BALANCE_GOAL * volIndexCnt) {
            moreFiles = FALSE;
            return 0;
        }

        if (bfEntry->noMove) {
            goto move_next_file;
        }

        setHdr = find_set_tag (bfEntry->bfSetTag);
        if (setHdr == NULL) {
            /* print out set name?? */
            fprintf (stderr, catgets(catd, 1, 20, "%s: Can't find file set hdr - tag %d.%d\n"),
                     Prog, bfEntry->bfSetTag.num, bfEntry->bfSetTag.seq);
            goto move_next_file;
        }

        currentPath.setHdr = setHdr;
        currentPath.bfTag = bfEntry->bfTag;
        currentPath.pathName[0] = '\0';

        sprintf (
                 dotTagsPathName,
                 "%s/0x%x.0x%x",
                 setHdr->dotTagsPath,
                 bfEntry->bfTag.num,
                 bfEntry->bfTag.seq
                 );

        fd = open (dotTagsPathName, O_RDONLY, 0);
        if (fd < 0) {
            if (errno == ENOENT) {
                /*
                 * The file was deleted.
                 */
                goto move_next_file;
            } else {
                fprintf (stderr, catgets(catd, 1, 21, "%s: Open of %s failed.\n"),
                         Prog, get_current_path_name());
                fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRMSG (errno));
                goto move_next_file;
            }
        }

        /*
         * Move extent metadata and extents.
         */
        sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);  
        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, 22, "%s: Can't get file %s parameters\n"),
                     Prog, get_current_path_name());
            fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
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
            u32T forceFlag = 0;

            fileMoveCount++;

            if (verboseFlag) {
                fprintf (stdout, catgets(catd, 1, 24, "%s: Moving file %s to volume %d\n"),
                         Prog, get_current_path_name(), targetVolumeIndex);
		(void)fflush(stdout);
            }

retry_move:

            pagesToMove = bfInfo.nextPage;
            srcPageOffset = 0;
            while (pagesToMove > 0)
            {
                unsigned long chunk;

                chunk = MIN(pages_to_migrate, pagesToMove);

                sts = advfs_migrate(
                                    fd,
                                    -1, /* srcVolIndex */
                                    srcPageOffset, /* srcPageOffset */
                                    chunk,
                                    targetVolumeIndex,
                                    -1,  /* dstBlkOffset */
                                    forceFlag);

                
                if (sts == E_INVOLUNTARY_ABORT && !forceFlag) {
                    forceFlag = 1;
                    goto retry_move;
                }

                if (sts != EOK) {
                    if (sts != E_MIGRATE_IN_PROGRESS && sts != E_NO_CLONE_STG &&
                       sts != E_PAGE_NOT_MAPPED && sts != E_CANT_MIGRATE_HOLE) {
                        fprintf (stderr, catgets(catd, 1, 25,
                                 "%s: Can't move file %s\n"),
                                 Prog, get_current_path_name());
                        fprintf (stderr, catgets(catd, 1, 14,
                                 "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                    }
                    if (sts != ENO_MORE_BLKS) {
                        (void) close (fd);
                        goto move_next_file;
                    }
                }
                pagesToMove -= chunk;
                srcPageOffset += chunk;
            }
        }

        if (close (fd)) {
            fprintf( stderr, catgets(catd, 1, 23, "%s: Close of %s failed.\n"),
                    Prog, dotTagsPathName);
            fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                     Prog, errno, ERRMSG (errno));
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
static
boolean
insert_bf_entry(
            bfTableEntryT *table,  /* in */
            mlBfDescT *datum,  /* in */
            bfTableEntryT **retNewEntry)  /* out */
{
    bfTableEntryT *newEntry;

    /* If this entry's datum is equal to the new datum, then the insert
     * fails. */
    if (ML_BFTAG_EQL(table->bfSetTag, datum->bfSetTag) &&
        ML_BFTAG_EQL(table->bfTag, datum->bfTag)) {
        return FALSE;
    }

    /* If this entry's datum is less than the new datum, the new
     * datum belongs in the right subtree, otherwise the left subtree.
     * If there is already such a subtree, make a recursive call to
     * effect the insertion; otherwise, insert the new entry here.
     * Note that memory allocation failures are simply treated as
     * ordinary failures, and no entry is made, because if this happens,
     * something else will fail soon enough anyway. */
    if (ML_BFTAG_LESS(table->bfSetTag, datum->bfSetTag) ||
        (ML_BFTAG_EQL(table->bfSetTag, datum->bfSetTag) &&
         ML_BFTAG_LESS(table->bfTag, datum->bfTag))) {
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
static
void
linearize_bf_table(
            bfTableEntryT *entry)  /* in */
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

static
boolean
compare_insert(
            bfTableEntryT *aEntry,
            bfTableEntryT *bEntry)
{
    return (!(ML_BFTAG_LESS(bEntry->bfSetTag, aEntry->bfSetTag) ||
              (ML_BFTAG_EQL(bEntry->bfSetTag, aEntry->bfSetTag) &&
               ML_BFTAG_LESS(bEntry->bfTag, aEntry->bfTag))));
} /* end compare_insert */

static
boolean
compare_reinsert(
            bfTableEntryT *aEntry,
            bfTableEntryT *bEntry)
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
static
void
reinsert_bf_entry(
            bfTableEntryT *table,
            bfTableEntryT *entry)
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
static
void
adjust_bf_table(
        bfTableEntryT *entry, /* in */
        compare_bf_entriesT *compare_routine) /* in */
{
    int side;
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

#define XTNT_DESC_MAX 500

static
mlStatusT
get_block_distribution (
                       int fd,  /* in */
                       volDataT *volData,  /* in */
                       int pageSize,  /* in */
                       boolean *onSingleVolume  /* out */
                       )

{

    u32T allocVolIndex;
    u32T pageCnt;
    u32T pageOffset;
    unsigned volIndex;
    int i;
    mlExtentDescT rawXtntDesc[XTNT_DESC_MAX];
    int rawXtntDescCnt;
    int startXtntDesc;
    mlStatusT sts;
    int xtntDescCnt = 0;
    int xtntDescMaxCnt = 0;
    int volSubscr;

    for (i = 0; i < volIndexCnt; i++) {
        volBlockDistribution[i] = 0;
    }

    startXtntDesc = 0;
    sts = msfs_get_bf_xtnt_map (
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
            if ( rawXtntDesc[i].volBlk != XTNT_TERM &&
                 rawXtntDesc[i].volBlk != PERM_HOLE_START )
            {
                volBlockDistribution[volSubscrArray[rawXtntDesc[i].volIndex]] +=
                    rawXtntDesc[i].bfPageCnt * pageSize;
            }
        }

        startXtntDesc = startXtntDesc + rawXtntDescCnt;

        sts = msfs_get_bf_xtnt_map (
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
        fprintf (stderr, catgets(catd, 1, 26, "%s: Can't get extent descriptors\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
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

_error:

    return sts;

}  /* end get_block_distribution */

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
        fprintf (stderr, catgets(catd, 1, 27, "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 28, "%s: Mount count = %d\n"), Prog, mountCnt);
        return 1;
    }

    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
        fprintf (stderr, catgets(catd, 1, 29, "%s: Can't allocate file system mount table.\n"), Prog);
        return 1;
    }

    /*
     * Because the caller has the domain locked, no more megasafe bitfile
     * sets can be mounted.  "mountTbl" will be up to date relative to
     * megasafe file systems.
     */

    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 27, "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                 Prog, errno, ERRMSG (errno));
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
                            fprintf (stderr, catgets(catd, 1, 30, "%s: Can't allocate mount on name memory\n"));
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
                            fprintf (stderr, catgets(catd, 1, 31, "%s: Can't allocate path name memory\n"));
                            return 1;
                        }
                        strcpy (setHdr->dotTagsPath, mountTbl[i].f_mntonname);
                        strcat (setHdr->dotTagsPath, dotTags);

                        /* Save the first dotTagsPath so that we can later find
                         * all of the sbm files. */
                        if (sbmRootPathName == NULL) {
                            sbmRootPathName = malloc(strlen (setHdr->dotTagsPath));
                            if (sbmRootPathName == NULL) {
                                fprintf (stderr, catgets(catd, 1, 31, "%s: Can't allocate path name memory\n"));
                                return 1;
                            }
                            strcpy (sbmRootPathName, setHdr->dotTagsPath);
                        }
                    }
                } else {
                    fprintf (stderr, catgets(catd, 1, 32, "%s: Can't allocate memory for file set header\n"));
                    return 1;
                }
            } else {
                fprintf (stderr, catgets(catd, 1, 33, "%s: Can't get file set id.\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 34, "%s: Domain name: %s, file set name: %s\n"),
                         Prog, mountBfSetName, mountDmnName);
                fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
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

                fprintf (stderr, catgets(catd, 1, 35, "%s: File set not mounted\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 34, "%s: Domain name: %s, file set name: %s\n"),
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
                    fprintf (stderr, catgets(catd, 1, 21, "%s: Open of %s failed.\n"),
                             Prog, setHdr->dotTagsPath);
                    fprintf (stderr, catgets(catd, 1, 9, "%s: Error = [%d] %s\n"),
                             Prog, errno, ERRMSG (errno));
                    return 1;
                }
            }

        } else {
            fprintf (stderr, catgets(catd, 1, 32, "%s: Can't allocate memory for file set header\n"));
            return 1;
        }

        sts = msfs_fset_get_info (dmnName, &bfSetIndex,
                                     &bfSetParams, &userId, 0);

    }  /* end while */

    if (sts != E_NO_MORE_SETS) {
        fprintf (stderr, catgets(catd, 1, 36, "%s: Can't get file set info for domain '%s'\n"),
                 Prog, dmnName);
        fprintf (stderr, catgets(catd, 1, 14, "%s: Error = %s\n"), Prog, BSERRMSG (sts));
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
unmount_bitfile_sets (
                      )

{

    u32T i;
    int errorFlag = 0;

    if (setHdrTbl == NULL) {
        return 0;
    }

    for (i = 0; i < setHdrCnt; i++) {

        if (setHdrTbl[i].fd != -1) {
            if (close (setHdrTbl[i].fd)) {
                fprintf( stderr, catgets(catd, 1, 37, "%s: Close of .tags file failed. [%d] %s\n"),
                        Prog, errno, sys_errlist[errno] );
                errorFlag = 1;
            }

        }
        if (setHdrTbl[i].unmountFlag != 0) {
            /* unmount */
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
            fprintf (stderr, catgets(catd, 1, 38, "%s: Can't allocate memory for expanded set table\n"),
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
            fprintf (stderr, catgets(catd, 1, 39, "%s: Can't allocate memory for new set table\n"),
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
int
update_volume_data(
        mlDmnParamsT *dmnParams,  /* in */
        volDataT *volData)	/* in/out */
{
    mlVolCountersT volCounters;
    mlVolInfoT volInfo;
    mlVolIoQParamsT volIoQParams;
    mlVolPropertiesT volProperties;
    int i;
    mlStatusT sts;

    dmnSize = 0;
    dmnFreeSize = 0;

    /*
     * Determine how much free space there is on each volume.
     */
    for (i = 0; i < volIndexCnt; i++) {

        sts = advfs_get_vol_params(
                dmnParams->bfDomainId,
                volData[i].volIndex,
                &volInfo,
                &volProperties,
                &volIoQParams,
                &volCounters);

        if (sts != EOK) {
            fprintf(stderr, catgets(catd, 1, 40, "%s: get vol params error %s\n"), Prog, BSERRMSG(sts));
            return 1;
        }

        volData[i].volSize = volInfo.volSize;
        volData[i].volFreeSize = volInfo.freeClusters * volInfo.stgCluster;
        volData[i].volStgCluster = volInfo.stgCluster;
        volData[i].freeSizeFract = (double)volData[i].volFreeSize /
            volData[i].volSize;

        dmnSize += volData[i].volSize;
        dmnFreeSize += volData[i].volFreeSize;
    }

    dmnFreeSizeFract = (double)dmnFreeSize / dmnSize;

    /*
     * Calculate the total deviation of the domain from perfect balance.
     */
    dmnBalanceDeviation = 0.0;
    for (i = 0; i < volIndexCnt; i++) {
        dmnBalanceDeviation += fabs(volData[i].freeSizeFract - dmnFreeSizeFract);
    }

    return 0;

} /* end update_volume_data */

/*
 * decide_move
 * Decide if moving a file to a new target volume will improve the domain's
 * balance.  If it will, update the domain's data to reflect the move.
 */
static
int
decide_move(
        int fd,  /* in */
        volDataT *volData,  /* in */
        int pageSize,  /* in */
        unsigned long fileBlockCount,  /* in */
        int *targetVolumeIndex,  /* out */
        boolean *moveFlag)  /* out */
{
    int targetSubscr;
    int bestTargetSubscr;
    int volSubscr;
    int err;
    long newVolFreeSize;
    boolean onSingleVolume;
    double newBalanceDeviation;
    double bestNewBalanceDeviation = dmnBalanceDeviation + 1.0;
    double targFreePct, srcFreePct;

    err = get_block_distribution(fd, volData, pageSize, &onSingleVolume);

    for (targetSubscr = 0; targetSubscr < volIndexCnt; targetSubscr++) {
        /* Don't consider moving the file to this volume if either the volume
         * doesn't have enough free space to hold the whole file (the whole
         * file must be allocated afresh before any of the old space occupied
         * by the file can be freed, so the presently occupied space cannot be
         * counted) or the whole file is already on this volume. */

        if ((fileBlockCount /* - volBlockDistribution[targetSubscr] */ >=
             volData[targetSubscr].volFreeSize) ||
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

            /* calculate delta for this volume from the domain average free space
             * keep a running total for all volumes in domain
             */

            newBalanceDeviation += 
                fabs((double)newVolFreeSize / volData[volSubscr].volSize -
                    dmnFreeSizeFract);
        }

        /* if better than last, set to new best  */
        if (newBalanceDeviation < bestNewBalanceDeviation) {
            bestNewBalanceDeviation = newBalanceDeviation;
            /* this should be the new target volume */
            bestTargetSubscr = targetSubscr;
        }
    }

    /* is new best deviation from all volumes above better
     * the existing domain deviation?
     */
    if (bestNewBalanceDeviation < dmnBalanceDeviation) {
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
                targFreePct  =  (double) (volData[bestTargetSubscr].volFreeSize +
                                    volBlockDistribution[bestTargetSubscr] -
                                    fileBlockCount) / volData[bestTargetSubscr].volSize;
                srcFreePct =    (double) (volData[volSubscr].volFreeSize +
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
        dmnBalanceDeviation = bestNewBalanceDeviation;
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
                     catgets(catd, 1, 41, "<unknown> (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))"),
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


/* end balance.c */

