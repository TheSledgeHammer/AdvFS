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
 *    Advfs Storage System
 *
 * Abstract:
 *
 *    vfast.c
 *    Command line user interface for vfast(smartstore).
 *
 * Date:
 *
 *    decembar 11, 2001
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: vfast.c,v $ $Revision: 1.1.6.5 $ (DEC) $Date: 2005/10/03 14:27:43 $"

#include <stdarg.h>
#include <sys/file.h>
#include <sys/signal.h>
#include <sys/errno.h>
#include <sys/versw.h>
#include <sys/time.h>
#include <msfs/msfs_syscalls.h>
#include <msfs/vfast.h>
#include <msfs/advfs_evm.h>
#include <locale.h>
#include <nl_types.h>
#include "vfast.h"
#include "vfast_msg.h"
#include "vods.h"
#include <msfs/bs_ods.h>

#define RBMT_VERSION(version) ( version >= FIRST_RBMT_VERSION )

static
void
update_free_space_data (
    int clusterSize,
    int *inHole,
    int *currentHoleSize,
    volFreeSpaceDataT *freeSpaceData
    )
{
    int range;
    unsigned long currentHoleSizeInCluster = *currentHoleSize;

    /* End of the current hole. */
    if (freeSpaceData != NULL) {
        unsigned long currentHoleSizeKb =
                        (currentHoleSizeInCluster *
                         clusterSize * BLOCK_SIZE) / 1024;
        for (range = 0; range < FREE_RANGE_COUNT; range++) {
            if (freeRangeTop[range] == 0 ||
                currentHoleSizeKb < freeRangeTop[range]) {
                freeSpaceData->rangeData[range].holeCount++;
                freeSpaceData->rangeData[range].freeSpace += currentHoleSizeKb;
                freeSpaceData->totalHoleCount++;
                freeSpaceData->totalFreeSpace += currentHoleSizeKb;
                break;
            }
        }
    }
    *inHole = FALSE;
    *currentHoleSize = 0;

} /* end update_free_space_data */

/*
 * retrieve the domain parameter information and mount the bitfiles,
 * set up structures to hold the volume information.
 *
 */
static
int 
open_domain (
    volDataT **retvolData,                     /* out */
    mlDmnParamsT *dmnParams                    /* in/out */
    )
{
    int thisDmnLock;
    int i, j, err;
    char dmnPathName[MAXPATHLEN+1];
    struct stat stats;
    mlStatusT sts = EOK;
    int max_vdi;
    u32T *volIndexArray = NULL;                  /* local use */
    volDataT *volData = NULL;                    /* local use */

    /*
     * Lock the master domain directory.  This synchronizes us with domain and
     * bitfile set creation/deletion.
     */

    DmnLock = lock_file (MSFS_DMN_DIR, LOCK_EX, &err);
    if (DmnLock < 0) {
        abort_prog (catgets(catd, 1, 14, 
                    "%s: error locking '%s'.\n"),
                    Prog, MSFS_DMN_DIR);
    }
    UnlockDmnFlag = TRUE;

    /*
     * Verify that the domain exists.
     */

    strcpy (dmnPathName, MSFS_DMN_DIR);
    strcat (dmnPathName, "/");
    strcat (dmnPathName, dmnName);
    err = lstat (dmnPathName, &stats);
    if (err) {
        if (errno == ENOENT) {
            abort_prog( catgets(catd, 1, 16, 
                        "%s: Domain directory '%s' does not exist\n"),
                        Prog, dmnPathName);
        } else {
            abort_prog( catgets(catd, 1, 17, 
                        "%s: Error getting '%s' stats.\n"),
                        Prog, dmnPathName);
        }
    }

    /* get the number of volumes */

    sts = msfs_get_dmnname_params (dmnName, dmnParams);
    if (sts != EOK) {
        abort_prog ( catgets(catd, 1, 19, 
                     "%s: Can't get domain parameters\n"), Prog);
    }

    volIndexArray = (u32T *)malloc(dmnParams->curNumVols * sizeof(u32T));
    if(volIndexArray == NULL) {
        fprintf (stderr, catgets(catd, 1, 17, 
                 "%s: Can't allocate memory\n"), Prog);
        goto _error;
    }

    volData = (volDataT *)malloc(dmnParams->curNumVols * sizeof(volDataT));
    if(volData == NULL) {
        fprintf (stderr, catgets(catd, 1, 17, 
                 "%s: Can't allocate memory\n"), Prog);
        goto _error;
    }

    /* 
     * initialize
     */

    /* Get the domain's parameters and a list of its volumes. */

    sts = msfs_get_dmn_vol_list (
                                 dmnParams->bfDomainId, 
                                 dmnParams->curNumVols, 
                                 volIndexArray,
                                 &volIndexCnt
                                 );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, catgets(catd, 1, 18, 
                 "%s: error: At least one fileset must be mounted\n"), Prog );
        goto _error;
    } else if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 19, 
                 "%s: Can't get domain's volume list\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 20, 
                  "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }


    /* assign volume indices - without the "L" log designator */

    for (i = 0; i < volIndexCnt; i++) {
        volData[i].volIndex = volIndexArray[i];
        volData[i].volSize = 0;
        volData[i].volStgCluster = 0;
        volData[i].volFreeSize = 0;
        volData[i].freeSizeFract = 0;
        volData[i].totalHoleCount = 0;
        volData[i].extentCount = 0;
        volData[i].fileWithExtCount = 0;
        volData[i].wrMaxIo = 0;
        volData[i].totalIOCount = 0;
        volData[i].totalBestIOCount = 0;
    }

    /* find the maximum volume index number - assign to max_vdi */
    max_vdi = 0;
    for (i = 0; i < volIndexCnt; i++) {
        if (volIndexArray[i] > max_vdi) {
            max_vdi = volIndexArray[i];
        }
    }

    /* Set up an array that enables us to, given a volume index, find the
     * corresponding subscript into the index array. */
    volSubscrArray = (int *)malloc((max_vdi + 1) * sizeof(int));
    if(volSubscrArray == NULL) {
        fprintf (stderr, catgets(catd, 1, 17, 
                 "%s: Can't allocate memory\n"), Prog);
        goto _error;
    }

    /* initialize the array */
    for (i = 0; i <= max_vdi; i++) {
        volSubscrArray[i] = -1;
    }
    for (i = 0; i < volIndexCnt; i++) {
        volSubscrArray[volIndexArray[i]] = i;
    }

    /* assign structures to passed in pointer for use by calling program */
    *retvolData = volData;

    if(volIndexArray)    free(volIndexArray);
    return( dmnParams->curNumVols );

_error:;

    if(volIndexArray)    free(volIndexArray);
    if(volData)          free(volData);
    if(volSubscrArray)   free(volSubscrArray);

    abort_prog( catgets(catd, 1, 38, "%s: Unable to open domain %s\n"), 
                        Prog, dmnPathName);

} /* end open_domain */


/*
 * open_dot_tags_file
 *
 * This function opens ".tags" in each of the domain's bitfile sets so that
 * the bitfile sets cannot be unmounted during the operation.
 *
 * This function has the side effect of filling the bitfile set cache.
 *
 * This function assumes the caller has locked the domain.
 */
static
int
open_dot_tags_file (
    mlBfTagT rootBfSetTag
    )
{

    setHdrT *setHdr = NULL;
    u32T bfSetIndex;
    mlBfSetParamsT bfSetParams;
    int err;
    int i;
    mlBfSetIdT mountBfSetId;
    char *mountBfSetName = NULL;
    char *mountDmnName = NULL;
    char mountPointPath[MAXPATHLEN+1];
    int mountCnt;
    int setNotMountedFlag = 0;
    struct statfs *mountTbl = NULL;
    mlStatusT sts = EOK;
    u32T userId;

    mountCnt = getfsstat(0, 0, MNT_NOWAIT);
    if (mountCnt <= 0) {
        fprintf (stderr, catgets(catd, 1, 31,
                    "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 32,
                    "%s: Mount count = %d\n"), Prog, mountCnt);
        return 1;
    }

    mountTbl = (struct statfs *) malloc (mountCnt * sizeof (struct statfs));
    if (mountTbl == NULL) {
        fprintf (stderr, catgets(catd, 1, 33,
                    "%s: Can't allocate file system mount table.\n"), Prog);
        return 1;
    }

    /*
     * Because the caller has the domain locked, no more megasafe bitfile
     * sets can be mounted.  "mountTbl" will be up to date relative to
     * advfs file systems.
     */

    err = getfsstat(mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        fprintf (stderr, catgets(catd, 1, 31,
                    "%s: getfsstat failed.\n"), Prog);
        fprintf (stderr, catgets(catd, 1, 12,
                    "%s: Error = [%d] %s\n"),
                Prog, errno, sys_errlist[err]);
        return 1;
    }

    /*
     * For all mounted advfs bitfile sets in the domain, add an entry
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

            sts = msfs_fset_get_id (
                      mountDmnName,
                      mountBfSetName,
                      &mountBfSetId );

            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 34,
                         "%s: Can't get file set id.\n"), Prog);
                fprintf (stderr, catgets(catd, 1, 35,
                         "%s: Domain name: %s, file set name: %s\n"),
                         Prog, mountBfSetName, mountDmnName);
                fprintf (stderr, catgets(catd, 1, 20,
                         "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                return 1;
            }

            /*
             * The bitfile set is a member of the domain.
             */
            setHdr = insert_set_tag(mountBfSetId.dirTag);

            if (setHdr == NULL) {
                fprintf (stderr, catgets(catd, 1, 36,
                         "%s: Can't allocate memory for file set header\n"),
                         Prog);
                return 1;
            }

            setHdr->mountedFlag = 1;
            if (setHdr->mntOnName == NULL) {
                setHdr->mntOnName = malloc (
                                  strlen(mountTbl[i].f_mntonname) + 1
                                            );
                if (setHdr->mntOnName == NULL) {
                    fprintf (stderr, catgets(catd, 1, 37,
                             "%s: Can't allocate mount on name memory\n"),
                             Prog);
                    return 1;
                }
                strcpy(setHdr->mntOnName, mountTbl[i].f_mntonname);
            }
            if (setHdr->dotTagsPath == NULL) {
               /*
                * New entry.  Set the ".tags" path.
                */
                setHdr->dotTagsPath = malloc (
                                       strlen(mountTbl[i].f_mntonname) +
                                       strlen(dotTags) + 1
                                              );
                if (setHdr->dotTagsPath == NULL) {
                    fprintf (stderr, catgets(catd, 1, 38,
                             "%s: Can't allocate path name memory\n"), Prog);
                    return 1;
                }
                strcpy(setHdr->dotTagsPath, mountTbl[i].f_mntonname);
                strcat(setHdr->dotTagsPath, dotTags);

                /* Save the first dotTagsPath so that we can later find
                 * all of the metadata (sbm and bmt) files. 
                 */
                if (metadataRootPathName == NULL) {
                   metadataRootPathName = malloc(strlen(setHdr->dotTagsPath)+1);
                    if (metadataRootPathName == NULL) {
                        fprintf (stderr, catgets(catd, 1, 38,
                                 "%s: Can't allocate path name memory\n"),
                                 Prog);
                        return 1;
                    }
                    strcpy (metadataRootPathName, setHdr->dotTagsPath);
                }
            } /* end if dotTagsPath == NULL */

        } /* end if not domain we are looking for */

    }  /* end for all domains */

    bfSetIndex = 0;

    while (sts == EOK) {

        sts = msfs_fset_get_info(dmnName, &bfSetIndex,
                                 &bfSetParams, &userId, 0);

        if ( (sts != E_NO_MORE_SETS) && (sts != EOK) ) {
            fprintf (stderr, catgets(catd, 1, 39,
                     "%s: Can't get file set info for domain '%s'\n"), 
                     Prog, dmnName);
            fprintf (stderr, catgets(catd, 1, 20,
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            return 1;
        }

        if (sts != EOK)
            break;

        setHdr = insert_set_tag(bfSetParams.bfSetId.dirTag);

        if (setHdr == NULL) {
            fprintf (stderr, catgets(catd, 1, 36,
                     "%s: Can't allocate memory for file set header\n"),
                     Prog);
            return 1;
        }

    }  /* end while */

    return setNotMountedFlag;

}  /* end open_dot_tags_file */


/*
 * close_dot_tags_file
 *
 * This function closes ".tags" in each of the domain's bitfile sets so that
 * the bitfile sets can be unmounted.
 */
static
int
close_dot_tags_file ( )
{

    u32T i;
    int errorFlag = 0;

    if (setHdrTbl == NULL) {
        return 0;
    }

    for (i = 0; i < setHdrCnt; i++) {

        if (setHdrTbl[i].fd != -1) {
            if (close(setHdrTbl[i].fd)) {
                fprintf (stderr, catgets(catd, 1, 41,
                         "%s: Close of .tags file failed. [%d] %s\n"),
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
        return -1;

    UnmountFlag = FALSE;

    return 0;

}  /* end close_dot_tags_file */


/*
 * insert_set_tag
 *
 * This function inserts a bitfile set tag in the set table.  This function
 * does not check for duplicate entries.
 */
static
setHdrT *
insert_set_tag (
    mlBfTagT setTag
    )
{
    int i;
    setHdrT *newSetHdrTbl = NULL;
    setHdrT *setHdr = NULL;

    setHdr = find_set_tag (setTag);
    if (setHdr != NULL) {
        return setHdr;
    }
        
    if (setHdrTbl != NULL) {
        newSetHdrTbl = (setHdrT *) malloc ((setHdrCnt + 1) * sizeof (setHdrT));
        if (setHdrTbl == NULL) {
            fprintf (stderr, catgets(catd, 1, 42,
                     "%s: Can't allocate memory for expanded set table\n"),
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
            fprintf (stderr, catgets(catd, 1, 43,
                     "%s: Can't allocate memory for new set table\n"),
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
get_current_path_name (
    mlBfTagT bfTag,
    mlBfTagT bfSetTag,
    int *unknown
    )
{
    mlStatusT sts = EOK;
    struct {
        setHdrT *setHdr;
        mlBfTagT bfTag;
        char pathName[MAXPATHLEN+1];
    } currentPath;

    currentPath.bfTag = bfTag;
    currentPath.pathName[0] = '\0';
    currentPath.setHdr = find_set_tag (bfSetTag);
   
    *unknown= 0;

    if (currentPath.setHdr == NULL) {
            strcpy(currentPath.pathName, " ");
            *unknown = 1;
    }

    if (!currentPath.pathName[0]) {
        sts = tag_to_path (
                       currentPath.setHdr->mntOnName,
                       currentPath.bfTag,
                       sizeof (currentPath.pathName),
                       &(currentPath.pathName[0])
                       );

        if (sts != EOK) {
            if(currentPath.bfTag.num == 0x1) {
                sprintf(
                     currentPath.pathName,
                     "<frag file>");
            }

            else {
                sprintf (
                     currentPath.pathName,
                     "<unknown> (setTag: %d.%d (0x%x.0x%x), "
                     "tag: %d.%d (0x%x.0x%x))",
                     currentPath.setHdr->setTag.num,
                     currentPath.setHdr->setTag.seq,
                     currentPath.setHdr->setTag.num,
                     currentPath.setHdr->setTag.seq,
                     currentPath.bfTag.num,
                     currentPath.bfTag.seq,
                     currentPath.bfTag.num,
                     currentPath.bfTag.seq
                     );
                *unknown = 1;
            }
        }
    }

    return currentPath.pathName;
} /* end get_current_path_name */


/*
 * set and check value of UI Options
 */
static
int
set_ui_params (
    mlSSDmnParamsT *ssDmnParams,
    char *uiParams
    )
{
    char *cp, *nextcp;
    char *nump;
    int num;
    int error=0;
    int j;

    cp = uiParams;
    while (cp != NULL && *cp != '\0') {
        if ((nextcp = index(cp, ',')) != NULL) {
            *nextcp++ = '\0';
        }
        if ((nump = index(cp, '=')) != NULL) {
            *nump++ = '\0';
            num = atoi(nump);
        } else {
            usage();
            exit(1);
        }

        if (!strcmp(cp, "defragment")) {
            if (strcmp(nump, "enable") &&  strcmp(nump, "disable")) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++;
            } else {
                ssDmnParams->ssDmnDefragment = (strcmp(nump, "enable") ? 0 : 1);
                if (ssDmnParams->ssDmnDefragment==1)
                    advfs_post_user_event(EVENT_SS_DEFRAG_ENABLED,
                                          advfs_event,
                                          Prog);
                else 
                    advfs_post_user_event(EVENT_SS_DEFRAG_DISABLED,
                                          advfs_event,
                                          Prog);
            }
        } else if (!strcmp(cp, "topIObalance")) {
            if (strcmp(nump, "enable") &&  strcmp(nump, "disable")) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++;
            } else {
                ssDmnParams->ssDmnSmartPlace = (strcmp(nump, "enable") ? 0 : 1);
                if (ssDmnParams->ssDmnSmartPlace==1)
                    advfs_post_user_event(EVENT_SS_TOPIOBAL_ENABLED,
                                          advfs_event,
                                          Prog);
                else 
                    advfs_post_user_event(EVENT_SS_TOPIOBAL_DISABLED,
                                          advfs_event,
                                          Prog);
            }
        } else if (!strcmp(cp, "balance")) {
            if (strcmp(nump, "enable") &&  strcmp(nump, "disable")) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++;
            } else {
                ssDmnParams->ssDmnBalance = (strcmp(nump, "enable") ? 0 : 1);
                if (ssDmnParams->ssDmnBalance==1)
                    advfs_post_user_event(EVENT_SS_BAL_ENABLED,
                                          advfs_event,
                                          Prog);
                else {
                    advfs_post_user_event(EVENT_SS_BAL_DISABLED,
                                          advfs_event,
                                          Prog);
                }
            }
        } else if (!strcmp(cp, "direct_io")) {
            if (strcmp(nump, "enable") &&  strcmp(nump, "disable")) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++;
            } else {
                ssDmnParams->ssDmnDirectIo = (strcmp(nump, "enable") ? 0 : 1);
            }
        } else if (!strcmp(cp, "percent_ios_when_busy")) {
            for (j=0; nump[j] != '\0'; j++) {
                if (!isdigit(nump[j])) {
                    fprintf (stderr, catgets(catd, 1, 47,
                             "%s: bad value for %s option\n"),
                             Prog, cp);
                    exit(1);
                }
            }
            if ((num > SS_MAX_PCT_IOS_WHEN_BUSY) || (num < SS_MIN_PCT_IOS_WHEN_BUSY)) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++;
            } else {
                ssDmnParams->ssMaxPercentOfIoWhenBusy = num;
            }
        } else if (!strcmp(cp, "ssAccessThreshHits")) {
            if ((num > SS_MAX_FILE_ACC_THRESH) || 
                            (num < SS_MIN_FILE_ACC_THRESH)) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++; 
            } else {
                ssDmnParams->ssAccessThreshHits = num;
            }
        } else if (!strcmp(cp, "ss_steady_state")) {
            for (j=0; nump[j] != '\0'; j++) {
                if (!isdigit(nump[j])) {
                    fprintf (stderr, catgets(catd, 1, 47,
                             "%s: bad value for %s option\n"),
                             Prog, cp);
                    exit(1);
                }
            }
            if ((num > SS_MAX_STEADY_STATE) || 
                            (num < SS_MIN_STEADY_STATE)) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++;
            } else {
                ssDmnParams->ssSteadyState = num;
            }
        } else if (!strcmp(cp, "ssMinutesInHotList")) {
            if ((num > SS_MAX_MINUTES_HOT_LST) || 
                            (num < SS_MIN_MINUTES_HOT_LST)) {
                fprintf (stderr, catgets(catd, 1, 47,
                         "%s: bad value for %s option\n"),
                         Prog, cp);
                error++;
            } else {
                ssDmnParams->ssMinutesInHotList = num;
            }
        } else {
            fprintf (stderr, catgets(catd, 1, 48,
                     "%s: unknown %s option\n"),
                     Prog, cp);
            error++;
        }

        cp = nextcp;
    } /* end while */

    return error; 

} /* end of set_ui_params */


/*
 * Lock this domain.
 */
static
void
lock_domain (
    char *ssDmnName
    )
{
    int dirLocked = 0, thisDmnLock = 0, lkHndl, err;
    char dmnPathName[MAXPATHLEN+1];

    /*
     * Lock the domain
     */
    lkHndl = lock_file(MSFS_DMN_DIR, LOCK_EX, &err);
    if (lkHndl < 0) {
        fprintf (stderr, catgets(catd, 1, 49,
                 "%s: error locking '%s'\n"), Prog, MSFS_DMN_DIR);
        exit(1);
    }
    dirLocked = 1;

    strcpy(dmnPathName, MSFS_DMN_DIR);
    strcat(dmnPathName, "/");
    strcat(dmnPathName, ssDmnName);
    /*
     * Lock this domain.
     */
    thisDmnLock = lock_file_nb (dmnPathName, LOCK_EX, &err);
    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, 1, 50,
                     "%s: cannot execute. Another AdvFS command\n"),Prog);
            fprintf (stderr, catgets(catd, 1, 51,
                     "is currently claiming exclusive use of the\n"));
            fprintf (stderr, catgets(catd, 1, 52,
                     "specified domain.\n"));
        } else {
            fprintf (stderr, catgets(catd, 1, 49,
                     "%s: error locking '%s'\n"),
                     Prog, dmnPathName);
        }
        exit(1);

    } else {
        ThisDmnLocked = 1;
    }

    /*
     * Unlock master lock
     */
    if (unlock_file( lkHndl, &err ) == -1) {
        fprintf (stderr, catgets(catd, 1, 53,
                 "%s: error unlocking '%s'\n"),
                 Prog, MSFS_DMN_DIR);
        dirLocked = 0;
        exit(1);
    }

} /* end of lock_domain */


/*
 * Verify that the domain exists and its 
 * pathname is in the domain directory.
 */
static
void 
verify_domain (
    char *domainName,
    mlDmnParamsT *dmnParams, /* out - domain params */
    int *vols,               /* out - number of volumes */
    int flags                /* in - lock_domain or not */
    )
{
    int err = 0;
    char dmnPathName[MAXPATHLEN+1];
    struct stat stats;
    u32T *volIndexArray;
    mlStatusT sts;

    strcpy(dmnPathName, MSFS_DMN_DIR);
    strcat(dmnPathName, "/");
    strcat(dmnPathName, domainName);

    err = lstat(dmnPathName, &stats);

    if (err != 0) {
        if (errno == ENOENT) {
            fprintf (stderr, catgets(catd, 1, 54,
                     "%s: domain directory '%s' does not exist\n"),
                     Prog, dmnPathName);
        } else {
            fprintf (stderr, catgets(catd, 1, 55,
                     "%s: error getting '%s' stats\n"),
                     Prog, dmnPathName);
        }
        exit(1);
    } /* end of if (err != 0) */

    if(flags == TRUE)
        lock_domain(domainName);

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = msfs_check_on_disk_version(BFD_ODS_LAST_VERSION);
    if (sts != EOK) {
          fprintf( stderr, catgets(catd, 1, ONDISK,
          "%s: This utility can not process the on-disk structures "
          "on this system.\n"), Prog);
          exit(1);
    }
    sts = msfs_get_dmnname_params(domainName, dmnParams);
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 56,
                 "%s: unable to get info for domain '%s'\n"),
                 Prog, domainName);
        fprintf (stderr, catgets(catd, 1, 20,
                 "%s: Error = %s\n"), Prog, BSERRMSG(sts));
    }

    volIndexArray = (u32T*) malloc(dmnParams->curNumVols * sizeof(u32T));
    if (volIndexArray == NULL) {
        fprintf (stderr, catgets(catd, 1, 57, "%s: out of memory\n"), Prog);
        exit(1);
    }

    sts = msfs_get_dmn_vol_list(dmnParams->bfDomainId, 
                                dmnParams->curNumVols, 
                                volIndexArray, 
                                vols);

    if((sts != EOK) && (sts != ENO_SUCH_DOMAIN)) {
        fprintf (stderr, catgets(catd, 1, 58,
                 "%s: get dmn vol list error %s\n"), Prog, BSERRMSG(sts));
        free(volIndexArray);
        exit(1);
    }

} /* end of verify_domain */


/*
 * Verify that the fileset exists.
 */
static
void
verify_fileset (
    char *domainName,
    char *filesetName
    )
{
    mlStatusT sts;
    int setFound = FALSE;
    int numSets = 0;
    u32T setIdx = 0;
    mlBfSetParamsT setParams;
    u32T userId;

    do {
        sts = msfs_fset_get_info(domainName, &setIdx, &setParams, &userId, 0);
        if (sts == EOK) {
            numSets++;

            if (filesetName != NULL) {
                if (strcmp(filesetName, setParams.setName)) {
                    continue;
                } else {
                    setFound = TRUE;
                }
            }
        }
    } while (sts == EOK);

    if (sts != E_NO_MORE_SETS) {
        fprintf (stderr, catgets(catd, 1, 59,
                 "%s: can't show set info for domain '%s'\n"),
                 Prog, domainName);
        exit(1);
    }

    if ((filesetName != NULL) && !setFound) {
        fprintf (stderr, catgets(catd, 1, 60,
                 "%s: file set '%s' does not exist in domain '%s'\n"),
                 Prog, filesetName, domainName);
        exit(1);
    }

    if (numSets == 0) {
        fprintf (stderr, catgets(catd, 1, 61,
                "%s: domain '%s' has no file sets\n"),
                Prog, domainName);
    }
} /* end of verify_fileset */


/*
 * To display a list of the worst performing files in the domain.
 */
static
void
display_extents_list (
    int num_vols,
    char* domainName,
    mlDmnParamsT dmnParams
    )
{
    u32T fraglist_buf_size;
    mlFragDescT *ssFragArray;
    mlBfSetParamsT bfSetParams;
    mlStatusT sts;
    int ssFragCnt, i, j;
    volDataT *volData = NULL;
    mlVolInfoT volInfo;
    mlVolIoQParamsT volIoQParams;
    mlVolPropertiesT volProperties;
    mlVolCountersT volCounters;
    int unknown;
    int err;

    open_domain ( &volData,
                  &dmnParams);

    if(!num_vols) {
        fprintf(stdout, catgets(catd, 1, NO_VOLUMES_FOUND,
                "%s: Unable to open domain.\n"), Prog);
        exit(1);
    }

    fraglist_buf_size = num_vols * SS_INIT_FRAG_LST_SZ;
    ssFragArray = (mlFragDescT *) malloc(
                                         sizeof(mlFragDescT) * 
                                         fraglist_buf_size
                                        );
    if (ssFragArray == NULL) {
        sts = ENO_MORE_MEMORY;
        fprintf (stderr, catgets(catd, 1, 62,
                 "%s: Cannot alloc memory for frag array\n"), Prog);
        exit(1);
    }

    err = open_dot_tags_file (dmnParams.bfSetDirTag);
    if (err) {
        fprintf(stderr, catgets(catd, 1, NOMOUNT,
                "%s: Can't mount bitfile sets\n"), Prog);
         abort_prog(catgets(catd, 1, 38,
                    "%s: Unable to open domain %s\n"), 
                    Prog, dmnName);
    } else
        UnmountFlag = TRUE;

    for (j=0; j<num_vols; j++) {
        sts = advfs_get_vol_params(
                   dmnParams.bfDomainId,
                   volData[j].volIndex,
                   &volInfo,
                   &volProperties,
                   &volIoQParams,
                   &volCounters);

        if(sts == EBAD_VDI)  
            continue;  /* An rmvol is probably running - ignore volume */

        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, 64,
                    "%s: get vol params error %s\n"), Prog, BSERRMSG(sts));
            fflush(stdout);
            exit(1);
        }

        sts = advfs_ss_get_fraglist(domainName,
                              volData[j].volIndex, 
                              0, 
                              fraglist_buf_size, 
                              &(ssFragArray[0]), 
                              &ssFragCnt);

        if (sts != EOK) {
            fprintf (stderr, catgets(catd, 1, 63,
                    "%s: cannot get frag list; %s\n"),
                    Prog, BSERRMSG(sts));
            exit(1);
        }

        fprintf (stdout, catgets(catd, 1, 85,
              "%s: Volume %d\n\n"), domainName, volData[j].volIndex);
        fprintf (stdout, catgets(catd, 1, 86,
              "extent\ncount    fileset/file\n\n"));
        if (ssFragCnt == 0) {
            fprintf (stdout, catgets(catd, 1, 87,"    No extents\n"));
        }

        for (i=0; i<=ssFragCnt; i++) {
            if (BS_BFTAG_REG(ssFragArray[i].ssFragBfSetId.dirTag)) {
                sts = msfs_get_bfset_params(
                                            ssFragArray[i].ssFragBfSetId, 
                                            &bfSetParams
                                           );
                if (sts != EOK) {
                    fprintf(stderr, catgets(catd, 1, 67,
                            "%s: unable to get bf set params\n"), Prog);
                }

                get_current_path_name(
                    ssFragArray[i].ssFragTag, 
                    ssFragArray[i].ssFragBfSetId.dirTag,
                    &unknown);

                fprintf(stdout, "%5d\t", ssFragArray[i].ssXtntCnt);
                fprintf(stdout, "%s: ", bfSetParams.setName);
                fprintf(stdout, "%s\n", 
                                get_current_path_name(
                                    ssFragArray[i].ssFragTag, 
                                    ssFragArray[i].ssFragBfSetId.dirTag,
                                    &unknown)
                                    );
            } /* end if REG files */
        }
        fprintf(stdout, "\n");
    }

    (void)close_dot_tags_file ();

} /* end of display_extents_list */
                        
static
int
process_bmtcell(bsMCT *bsMCp,
             int cn,
             int dmnVersion,
             u64T *volFileWithXtntCnt,
             u64T *volXtntCnt)
{
    bsMRT *bsMRp;
    int rn;
    int i,cnt;
    char *pdata;
    u64T extentCount=0;
    u64T filesWithExtents=0;

    bsMRp = (bsMRT *)bsMCp->bsMR0;

    if ( BS_BFTAG_RSVD(bsMCp->tag) ) {
        /* dont count reserved files */
        return (0);
    }

    rn=0;
    while ( bsMRp->type != 0 && bsMRp < (bsMRT *)&bsMCp->bsMR0[BSC_R_SZ] ) {
        pdata = ((char *) bsMRp) + sizeof(bsMRT);
        switch (bsMRp->type) {
          case BSR_XTNTS:
            if( !BS_BFTAG_RSVD(bsMCp->tag)  &&
                bsMCp->tag.num>1 &&    /* ignore frag file like defragment does */
                FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, ((bsXtntRT *)pdata)->type))
            {
                /* DVN v4 only */
                extentCount += ((bsXtntRT *)pdata)->firstXtnt.xCnt-1;
                if((((bsXtntRT *)pdata)->firstXtnt.xCnt-1 > 0)  ||
                   (((bsXtntRT *)pdata)->firstXtnt.mcellCnt > 1))
                    filesWithExtents += 1;
            }

            break;
          case BSR_XTRA_XTNTS:
            /* DVN v3 or v4 */
            for (i=0,cnt=0; i < ((bsXtraXtntRT *)pdata)->xCnt; i++ ) {
                if(( ((bsXtraXtntRT *)pdata)->bsXA[i].vdBlk != XTNT_TERM ) &&
                   ( ((bsXtraXtntRT *)pdata)->bsXA[i].vdBlk != PERM_HOLE_START))
                    cnt++;
            }
            extentCount += cnt;
            break;
          case BSR_SHADOW_XTNTS:
            if(!BS_BFTAG_RSVD(bsMCp->tag) &&
                bsMCp->tag.num>1 &&    /* ignore frag file like defragment does */
               !FIRST_XTNT_IN_PRIM_MCELL(dmnVersion, ((bsXtntRT *)pdata)->type))
            {
                /* DVN v3 only */
                if((((bsShadowXtntT *)pdata)->xCnt-1 > 0)  ||
                   (((bsShadowXtntT *)pdata)->mcellCnt > 1))
                    filesWithExtents += 1;
            }
            /* DVN v3 or v4 */
            for (i=0,cnt=0; i < ((bsShadowXtntT *)pdata)->xCnt; i++ ) {
                if(( ((bsShadowXtntT *)pdata)->bsXA[i].vdBlk != XTNT_TERM ) &&
                   ( ((bsShadowXtntT *)pdata)->bsXA[i].vdBlk != PERM_HOLE_START))
                    cnt++;
            }
            extentCount += cnt;
            break;
          default:
            /* unrecognizable mcell, skip it */
            break;
        }

        if (( bsMRp->bCnt == 0)  ||
            ( (char *)bsMRp + bsMRp->bCnt > &bsMCp->bsMR0[BSC_R_SZ])) {
            /* Bad mcell record, skip this mcell */
            break;
        }

        rn++;
        bsMRp = (bsMRT*)((char*)bsMRp + roundup(bsMRp->bCnt,sizeof(int)));
    }

    *volFileWithXtntCnt += filesWithExtents;
    *volXtntCnt += extentCount;

    return(0);
}


static
int
process_bmtpage(
    bitFileT *bfp, 
    int pgnum, 
    u64T *volFileWithXtntCnt,
    u64T *volXtntCnt)
{
    int cn;
    bsMPgT *bsMPgp;
    int ret=0;

    if ( read_page(bfp, pgnum) != OK ) {
        return 1;
    }

    bsMPgp = (bsMPgT*)bfp->pgBuf;

    for ( cn = 0; cn < BSPG_CELLS; cn++) {
        ret = process_bmtcell(&bsMPgp->bsMCA[cn], 
                              cn, 
                              bsMPgp->megaVersion,
                              volFileWithXtntCnt,
                              volXtntCnt);
        if(ret != 0) 
            return(ret);
    }

    return(0);
}

/* Make new defines. SBM_LONGS_PG & SBM_BITS_LONG are too misleading. */
/* SBM_INTS_PG is the number of SBM mapInt entries (uint32T) per SBM page. */
#define SBM_INTS_PG SBM_LONGS_PG
/* SBM_BITS_INT is the number of bits in a mapInt entry (32) */
#define SBM_BITS_INT SBM_BITS_LONG

/* blks per SBM bit. 16 in version 4, 2 in version 3 */
#define CLUSTER_SZ (bfp->dmn->dmnVers == 3 ? BS_CLUSTSIZE_V3 : BS_CLUSTSIZE)
#define IS_INUSE(bit, wd) (((wd) & (1 << (bit))))


int
process_sbmpage(bitFileT *bfp, 
                int page,
                uint32T sbmLastPg,
                uint32T sbmlastPgWds,
                uint32T sbmlastPgBits,
                uint32T clusterSize,
                int *inHole,
                int *currentHoleSize, 
                volFreeSpaceDataT *freeSpaceData)
{
    int index;
    bsStgBmT *bsStgBmp=(bsStgBmT*)bfp->pgBuf;
    int nextMapByte;
    uint32T wd,bit,wordCnt,bitCnt;
    uint32T curWord ;

    if ( read_page(bfp, page) ) {
        return (1);
    }

    if (page == sbmLastPg) {
        wordCnt = sbmlastPgWds;
    } else {
        wordCnt = SBM_INTS_PG;
    }

    /*
     * Process all the words in the page.
     */
    bitCnt = SBM_BITS_LONG;
    for (wd = 0; wd < wordCnt; (wd)++) {
        curWord = bsStgBmp->mapInt[wd];

        if((page == sbmLastPg) && (wd == (wordCnt-1))) 
            bitCnt = sbmlastPgBits;

        for (bit = 0; bit < bitCnt; bit++) {

            if((page==0) && (wd==0) && 
               (bit==0) && (!IS_INUSE(bit, curWord))) *inHole = TRUE;

            if (IS_INUSE( bit, curWord )) {
                if(*inHole) {
                    /* end of a hole */
                    update_free_space_data (
                            clusterSize,
                            inHole,
                            currentHoleSize, 
                            freeSpaceData);
                }

            }  else {   
                 /* Add a byte to the current hole (or start a new one). */
                 *inHole = TRUE;
                 (*currentHoleSize)++;
            }
        }
    }

    return 0;
}

/******************************************************************************/
/* Given bfp->pvol, this routine loads the extents for the selected (R)BMT
**
** Input: bfp->pvol - volume on which to find the BMT
** Input: bfp->dmn->vols[] filled in
** Output: fileTag, setTag (vol 1 defaults for FLE & vol == 0)
** OutPut: ppage, pcell
** Return: 0 if all went OK.
** Return: 1 if volume does not exist in domain.
*/
static
int
find_BMT(bitFileT *bfp)
{
    bsVdAttrT *bsVdAttrp;
    vdIndexT vol = bfp->pvol;

    bfp->setTag = -2;
    bfp->ppage = 0;

    if ( RBMT_VERSION(bfp->dmn->dmnVers) ) 
        bfp->pcell = BFM_BMT;
    else
        bfp->pcell = BFM_BMT_V3;

    if ( bfp->pvol == 0 ) {
        /* we haven't selected a volume. default to vdi 1 */
        bfp->fileTag = RSVD_TAGNUM_GEN(1, bfp->pcell);
    } else
        bfp->fileTag = RSVD_TAGNUM_GEN(bfp->pvol, bfp->pcell);

    return(load_xtnts(bfp));
}

/*****************************************************************************/
/* Input: bfp->pvol - volume on which to find the SBM
** Input: bfp->dmn->vols[] filled in
*/
int
find_SBM(bitFileT *bfp)
{
    int i;
    int ret;
    bsVdAttrT *bsVdAttrp;
    bsMCT *mcellp;
    int mc;

    if ( RBMT_VERSION(bfp->dmn->dmnVers) ) 
        mc = BFM_SBM;
    else
        mc = BFM_SBM_V3;

    bfp->ppage = 0;
    bfp->pcell = mc;
    bfp->fileTag = RSVD_TAGNUM_GEN(bfp->pvol, mc);
    bfp->setTag = -2;

    if ( bfp->pvol == 0 ) {
        return (0);
    }

    if ( bfp->dmn->vols[bfp->pvol] ) {
        return load_xtnts(bfp);
    }

    return(1);
}


static
int
display_extents_summary ()
{
    mlDmnParamsT dmnParams;
    volFreeSpaceDataT freeSpaceData;
    u64T volFileWithXtntCnt = 0;
    u64T volXtntCnt = 0;
    int range;
    mlStatusT sts;
    mlVolInfoT volInfo;
    mlVolIoQParamsT volIoQParams;
    mlVolPropertiesT volProperties;
    mlVolCountersT volCounters;
    int err;
    bitFileT bf;
    bitFileT *bfp = &bf;
    int ret=0;
    char *name = NULL;
    uint32T pagenum;
    int vdi;
    u32T clusterSize;
    int currentHoleSize;
    int inHole;
    uint32T numClusters;
    uint32T sbmLastPg,        /* number of whole pages in sbm */
            sbmlastPgWds,     /* number of whole words in last page */
            sbmlastPgBits;    /* number of bits in last word */


    sts = msfs_get_dmnname_params (dmnName, &dmnParams);
    if (sts != EOK) {
        abort_prog ( catgets(catd, 1, 69, 
                     "%s: cannot get domain params on domain %s; %s\n"),
                     Prog, dmnName, BSERRMSG(sts));
    }

    err = open_dot_tags_file (dmnParams.bfSetDirTag);
    if (err) {
        fprintf(stderr, catgets(catd, 1, NOMOUNT,
                "%s: Can't mount bitfile sets\n"), Prog);
        abort_prog(catgets(catd, 1, 38,
                   "%s: Unable to open domain %s\n"), 
                   Prog, dmnName);
    } else
        UnmountFlag = TRUE;

    init_bitfile(bfp);

    /* Set tag to reserved so that if it's a saved file we can check the */
    /* length in resolve_fle. Default to -6. */
    bfp->fileTag = RSVD_TAGNUM_GEN(1, BFM_RBMT);

    name = dmnName;
    bfp->type = DMN;
    ret = resolve_name(bfp, name, 1);
    if ( ret != OK ) {
        fprintf(stderr, catgets(catd, 1, NOACCESS,
              "%s: Unable to access domain %s\n"), Prog, dmnName);
        fflush(stdout);
        return(ret);
    }

    /* initialize array */
    for (range = 0; range < FREE_RANGE_COUNT; range++) {
        freeSpaceData.rangeData[range].freeSpace = 0;
        freeSpaceData.rangeData[range].holeCount = 0;
        freeSpaceData.totalFreeSpace = 0;
        freeSpaceData.totalHoleCount = 0;
    }

    bfp->type |= VOL;
    for ( vdi = 1; vdi < BS_MAX_VDI; vdi++ ) {

        if ( bfp->dmn->vols[vdi] == NULL ) 
            continue;
        else
            bfp->pvol = vdi;

        /* Get the extents info here */
        sts = advfs_get_vol_params(
                   dmnParams.bfDomainId,
                   vdi,
                   &volInfo,
                   &volProperties,
                   &volIoQParams,
                   &volCounters);

        if(sts == EBAD_VDI)
            continue;  /* An rmvol is probably running - ignore volume */

        if (sts != EOK) {
            if (sts == ENO_SUCH_DOMAIN) {
                fprintf (stderr, catgets(catd, 1, 146,
                     "%s: Either there is no such domain or associated fileset\n"),
                     Prog);
            } else {
                fprintf (stderr, catgets(catd, 1, 64,
                     "%s: get vol params error %s\n"), Prog, BSERRMSG(sts));
            }
            fflush(stdout);
            return(sts);
        }

        bfp->fd = bfp->dmn->vols[vdi]->fd;

        if ( find_BMT(bfp) != OK ) {
            fprintf (stderr, catgets(catd, 1, 16,
                "%s: Error reading from bitmap file: %s"), Prog, "BMT");
            fflush(stdout);
            return(ret);
        }

        for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
            ret = process_bmtpage(bfp, 
                                pagenum, 
                                &volFileWithXtntCnt,
                                &volXtntCnt);
            if(ret != 0) {
                fprintf (stderr, catgets(catd, 1, BMT_NOREAD,
                    "%s: Unable to read bmt page %d on volume %d"),
                    Prog, pagenum, vdi);
                fflush(stdout);
                return(ret);
            }
        }

        /* Get the free space info here */

        if ( (ret = find_SBM(bfp)) != 0 ) {
            fprintf (stderr, catgets(catd, 1, 16,
                "%s: Error reading from bitmap file: %s"), Prog, "SBM");
            fflush(stdout);
            return(ret);
        }

        /* get the sbm dimensions */
        numClusters = volInfo.volSize/volInfo.stgCluster;
        sbmLastPg   = numClusters / SBM_BITS_PG;
        sbmlastPgWds  = numClusters / SBM_BITS_LONG - sbmLastPg * SBM_LONGS_PG;
        sbmlastPgBits = numClusters - sbmLastPg * SBM_BITS_PG -
                      sbmlastPgWds * SBM_BITS_LONG;

        currentHoleSize = 0;
        inHole=0;
        for ( pagenum = 0; pagenum < bfp->pages; pagenum++ ) {
            ret = process_sbmpage(bfp, 
                                  pagenum,
                                  sbmLastPg,
                                  sbmlastPgWds,
                                  sbmlastPgBits,
                                  volInfo.stgCluster,
                                  &inHole,
                                  &currentHoleSize, 
                                  &freeSpaceData);
            if(ret != 0) {
                fprintf (stderr, catgets(catd, 1, SBM_NOREAD,
                    "%s: Unable to read sbm page %d on volume %d"),
                    Prog, pagenum, vdi);
                fflush(stdout);
                return(ret);
            }
        }

        if(inHole) {
            /* end of a volume, count this last hole*/
            update_free_space_data (
                        volInfo.stgCluster,
                        &inHole,
                        &currentHoleSize, 
                        &freeSpaceData);
        }
    }

    fprintf(stdout, "%s\n\n", dmnName);
    fprintf(stdout, catgets(catd, 1, 78,
               "    Extents:              %11ld\n"), volXtntCnt);
    fprintf(stdout, catgets(catd, 1, 79,
               "    Files w/extents:      %11ld\n"), volFileWithXtntCnt);

    fprintf(stdout, catgets(catd, 1, 80,
               "    Avg exts per file w/exts: %7.2f\n"), 
                    (volFileWithXtntCnt==0) ? 0.0 :
                    ((double)volXtntCnt / (double)volFileWithXtntCnt));

    fprintf(stdout, catgets(catd, 1, 81,
               "    Free space fragments:      %6d\n"), 
                              freeSpaceData.totalHoleCount);
    fprintf(stdout, "                  ");
    fprintf(stdout, catgets(catd, 1, 82,
               "   <100K     <1M    <10M    >10M\n"));

    fprintf(stdout, catgets(catd, 1, 83,"      Free space:"));
    for (range = 0; range < FREE_RANGE_COUNT; range++) {
        fprintf(stdout, "    %3.0f%%", 
            freeSpaceData.totalFreeSpace ? 
            (100 * freeSpaceData.rangeData[range].freeSpace /
                   (double)freeSpaceData.totalFreeSpace):0.0);
    }

    fprintf(stdout, catgets(catd, 1, 84,"\n      Fragments: "));
    for (range = 0; range < FREE_RANGE_COUNT; range++) {
        fprintf(stdout, " %7d", freeSpaceData.rangeData[range].holeCount );
    }
    fprintf(stdout, "\n");

    (void)close_dot_tags_file ();

    exit(ret);

} /* end of display_extents_summary */


static
void
display_hotfiles_list (
    mlDmnParamsT dmnParams
    )
{
    u32T hotlist_buf_size;
    int ssHotCnt=0;
    int i,j;
    mlHotDescT *ssHotArray;
    mlBfSetParamsT bfSetParams;
    mlStatusT sts;
    int unknown;
    int err;
    volDataT *volData = NULL;
    u32T partial_list_cnt;
    int num_vols=0;

    num_vols = open_domain( &volData,
                            &dmnParams);

    if(!num_vols) {
        fprintf(stdout, catgets(catd, 1, NO_VOLUMES_FOUND,
                "%s: Unable to open domain.\n"), Prog);
        exit(1);
    }

    hotlist_buf_size = num_vols * SS_INIT_HOT_LST_GROW_SZ;
    ssHotArray = (mlHotDescT *) malloc (sizeof(mlHotDescT) * 
                                        hotlist_buf_size
                                       );

    if (ssHotArray == NULL) {
        sts = ENO_MORE_MEMORY;
        fprintf (stderr, catgets(catd, 1, 65,
                 "%s: Cannot alloc memory for hot array\n"), Prog);
        exit(1);
    }

    sts = advfs_ss_get_hotlist (
              dmnParams.bfDomainId, 
              0, 
              hotlist_buf_size, 
              &(ssHotArray[0]), 
              &ssHotCnt
              );
    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 66,
                 "%s: cannot get hot list; %s\n"),
                 Prog, BSERRMSG(sts));
        exit(1);
    }

    if (ssHotCnt == 0) {
       fprintf (stdout, catgets(catd, 1, 130,
                       "    No hot files in list for this domain.\n"));
       exit(0);
    }

    err = open_dot_tags_file (dmnParams.bfSetDirTag);
    if (err) {
        fprintf(stderr, catgets(catd, 1, NOMOUNT,
                "%s: Can't mount bitfile sets\n"), Prog);
        abort_prog(catgets(catd, 1, 38,
                   "%s: Unable to open domain %s\n"), 
                   Prog, dmnName);
    } else
        UnmountFlag = TRUE;

    partial_list_cnt = ssHotCnt * SS_HOT_LST_CROSS_PCT / 100;
    if (partial_list_cnt < num_vols) {
        partial_list_cnt = num_vols;
    }

    fprintf(stdout, catgets(catd, 1, 88, "\n Past Week\n"));
    fprintf(stdout, catgets(catd, 1, 89, 
           "  IO Count     Volume    File\n\n"));
    for (i=0,j=0; i<ssHotCnt; i++) {

        /* Put a line after SS_HOT_LST_CROSS_PCT percent of entries */
        if ((j== partial_list_cnt) && (j!=0)) {
            printf("   -----------------------------------------"
                   "----------------------\n");
        }

        j++;

        if (BS_BFTAG_REG(ssHotArray[i].ssHotBfSetId.dirTag)) {
            get_current_path_name(
                ssHotArray[i].ssHotTag, 
                ssHotArray[i].ssHotBfSetId.dirTag,
                &unknown);
            
            if (unknown) {
                fprintf(stdout, "%11d", ssHotArray[i].ssHotIOCnt);
                fprintf(stdout, "    %5d",
                        (ssHotArray[i].ssHotPlaced!=0) ?
                            (signed short)ssHotArray[i].ssHotPlaced :
                            (signed short)ssHotArray[i].ssHotVdi);
                fprintf(stdout, "  %s  ",
                        (ssHotArray[i].ssHotPlaced>0) ? "(x)" : "   ");
                fprintf(stdout, catgets(catd, 1, 129, 
                        "<unknown> setTag: %d.%d (0x%x.0x%x), "\
                        "tag: %d.%d (0x%x.0x%x)\n"),
                        ssHotArray[i].ssHotBfSetId.dirTag.num,
                        ssHotArray[i].ssHotBfSetId.dirTag.seq,
                        ssHotArray[i].ssHotBfSetId.dirTag.num,
                        ssHotArray[i].ssHotBfSetId.dirTag.seq,
                        ssHotArray[i].ssHotTag.num,
                        ssHotArray[i].ssHotTag.seq,
                        ssHotArray[i].ssHotTag.num,
                        ssHotArray[i].ssHotTag.seq);
            } else if (!unknown) {
                sts = msfs_get_bfset_params(ssHotArray[i].ssHotBfSetId, 
                                            &bfSetParams);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 67,
                             "%s: unable to get bf set params\n"), Prog);
                }

                fprintf(stdout, "%11d", ssHotArray[i].ssHotIOCnt);
                fprintf(stdout, "    %5d",
                        (ssHotArray[i].ssHotPlaced!=0) ?
                            (signed short)ssHotArray[i].ssHotPlaced :
                            (signed short)ssHotArray[i].ssHotVdi);
                fprintf(stdout, "  %s  ",  /* placed or not? */
                        (ssHotArray[i].ssHotPlaced>0) ? "(x)" : "   ");

                switch(ssHotArray[i].ssHotTag.num) {
                    case 1:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, 1, 90, "tag = "));
                        fprintf(stdout, "%d, %d, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.num,
                                    ssHotArray[i].ssHotTag.num);
                        fprintf(stdout, catgets(catd, 1, 91, "FRAG\n"));
                        continue;
                    case 2:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, 1, 90, "tag = "));
                        fprintf(stdout, "%d, %d, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.num,
                                    ssHotArray[i].ssHotTag.num);
                        fprintf(stdout, catgets(catd, 1, 92,"ROOT DIRECTORY\n"));
                        continue;
                    case 3:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, 1, 90, "tag = "));
                        fprintf(stdout, "%d, %d, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.num,
                                    ssHotArray[i].ssHotTag.num);
                        fprintf(stdout, catgets(catd, 1, 94,".tags\n"));
                        continue;
                    case 4:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, 1, 90, "tag = "));
                        fprintf(stdout, "%d, %d, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.num,
                                    ssHotArray[i].ssHotTag.num);
                        fprintf(stdout, catgets(catd, 1, 95,"quota.user\n"));
                        continue;
                    case 5:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, catgets(catd, 1, 90, "tag = "));
                        fprintf(stdout, "%d, %d, ",
                                    ssHotArray[i].ssHotBfSetId.dirTag.num,
                                    ssHotArray[i].ssHotTag.num);
                        fprintf(stdout, catgets(catd, 1, 95,"quota.group\n"));
                        continue;
                    default:
                        fprintf(stdout, "%s: ", bfSetParams.setName);
                        fprintf(stdout, "%s\n",
                                        get_current_path_name(
                                            ssHotArray[i].ssHotTag, 
                                            ssHotArray[i].ssHotBfSetId.dirTag,
                                            &unknown));
                }
            }

        } else if (ML_BFTAG_RSVD(ssHotArray[i].ssHotBfSetId.dirTag)) {
            fprintf(stdout, "%11d", ssHotArray[i].ssHotIOCnt);
            fprintf(stdout, "    %5d %s  ",
                    (ssHotArray[i].ssHotPlaced==0) ?
                        (signed short)ssHotArray[i].ssHotVdi :
                        (signed short)ssHotArray[i].ssHotPlaced,
                    (ssHotArray[i].ssHotPlaced==0) ?
                        "   " : "(x)");

            if (!ML_BFTAG_RSVD(ssHotArray[i].ssHotTag))
                fprintf(stdout, " %s: ", bfSetParams.setName);

            fprintf(stdout, catgets(catd, 1, 90, 
                         "*** a reserved file, tag = "));
            fprintf(stdout, "%d, %d, ", 
                        ssHotArray[i].ssHotBfSetId.dirTag.num,
                        ssHotArray[i].ssHotTag.num);

            if (ML_BFTAG_RSVD(ssHotArray[i].ssHotTag)) {
                fprintf(stdout, "%s\n",
                       (abs(ssHotArray[i].ssHotTag.num) % 6 == 0) ? "RBMT" :
                       (abs(ssHotArray[i].ssHotTag.num) % 6 == 1) ? "SBM" :
                       (abs(ssHotArray[i].ssHotTag.num) % 6 == 2) ? "TAG" :
                       (abs(ssHotArray[i].ssHotTag.num) % 6 == 3) ? "LOG" :
                       (abs(ssHotArray[i].ssHotTag.num) % 6 == 4) ? "BMT" :
                       " ");
            } else {
                fprintf(stdout, "TAGDIR\n");
            }
        }

    } /* end of loop on ssHotCnt */

    (void)close_dot_tags_file ();

} /* end of display_hotfiles_list */


static
void
display_hotfiles_summary (
    mlDmnParamsT dmnParams
    )
{
    u32T hotlist_buf_size;
    int ssHotCnt=0;
    int i, j, k;
    mlHotDescT *ssHotArray;
    mlStatusT sts;
    u32T totalHotCnt=0;
    float max, min, iopcnt;
    volDataT *volData = NULL;
    u32T partial_list_cnt;
    float partial_list_percentage = 0.0;
    int num_vols=0;
    typedef struct {
        int vdCnt;
        vdIndexT vdi;
    } hotDistT;
    hotDistT *hotDist;

    num_vols = open_domain ( &volData,
                             &dmnParams);

    if(!num_vols) {
        fprintf(stdout, catgets(catd, 1, NO_VOLUMES_FOUND,
                "%s: Unable to open domain.\n"), Prog);
        exit(1);
    }

    hotDist = (hotDistT *) malloc(sizeof(hotDistT) * num_vols);
    if (hotDist == NULL) {
        fprintf (stderr, catgets(catd, 1, 68,
                 "%s, Cannot alloc memory for hot file count array\n"),
                 Prog);
        exit(1);
    }

    hotlist_buf_size = num_vols * SS_INIT_HOT_LST_GROW_SZ;
    ssHotArray = (mlHotDescT *) malloc (sizeof(mlHotDescT) * 
                                        hotlist_buf_size
                                       );

    if (ssHotArray == NULL) {
        sts = ENO_MORE_MEMORY;
        fprintf (stderr, catgets(catd, 1, 65,
                 "%s: Cannot alloc memory for hot array\n"), Prog);
        exit(1);
    }

    sts = advfs_ss_get_hotlist(dmnParams.bfDomainId, 
                         0, 
                         hotlist_buf_size, 
                         &(ssHotArray[0]), 
                         &ssHotCnt);

    if (sts != EOK) {
        fprintf (stderr, catgets(catd, 1, 66,
                "%s: cannot get hot list; %s\n"),
                Prog, BSERRMSG(sts));
        exit(1);
    }

    if ((partial_list_cnt=ssHotCnt*SS_HOT_LST_CROSS_PCT/100) < num_vols) {
        partial_list_cnt = num_vols;
    }

    if (ssHotCnt != 0)
        partial_list_percentage = partial_list_cnt * 100.0 / ssHotCnt;

    /* initalize the array */
    for (i=0; i<=num_vols; i++) {
        hotDist[i].vdCnt = 0;
        hotDist[i].vdi = volData[i].volIndex;
    }

    for (i=0; i<partial_list_cnt; i++) {
        for (k=0; k<num_vols; k++) {
            if ((signed short)ssHotArray[i].ssHotVdi > 0) {
                if (hotDist[k].vdi == ssHotArray[i].ssHotVdi) {
                    hotDist[k].vdCnt++;
                }
            }
        }
        totalHotCnt++;
    }

    if (totalHotCnt != 0) {
        max = (min = 100.0 * hotDist[0].vdCnt / totalHotCnt);
    } else {
        exit(1);
    }

    fprintf (stdout, catgets(catd, 1, 96,
             "\ndomain:  %s"), dmnParams.domainName);
    fprintf (stdout, catgets(catd, 1, 97,
             "\n\n         Top %2.0f%% of Hot Files\n"), partial_list_percentage);
    fprintf (stdout, catgets(catd, 1, 98,
             "Volume   Distribution by Domain Volume\n"));
    for (j=0; j<num_vols; j++) {
        iopcnt = 100.0 * hotDist[j].vdCnt / totalHotCnt;
        fprintf(stdout,
                "%6d         %4.2f%%\n",
                volData[j].volIndex,
                iopcnt);

        max = (max > iopcnt) ? max : iopcnt;
        min = (min <= iopcnt) ? min : iopcnt;
    }

    if (num_vols > 1) {
        fprintf (stdout, catgets(catd, 1, 99, 
              "\n         greatest difference "
              "between storage devices: %4.2f%%\n"), (max-min));
    }

} /* end of display_hotfiles_summary */


usage ()
{
    fprintf(stdout, catgets(catd, 1, 5, "Usage:\n"));
    fprintf(stdout, catgets(catd, 1, 6,
        "       vfast activate|deactivate|suspend|status domain_name\n"));
    fprintf(stdout, catgets(catd, 1, 7,
        "       vfast -l|-L extents|hotfiles domain_name\n"));
    fprintf(stdout, catgets(catd, 1, 8,
        "       vfast -o option_list domain_name\n\n"));
    fprintf(stdout, catgets(catd, 1, USAGE9,
        "       option_list is a comma separated list of options:\n"));
    fprintf(stdout, catgets(catd, 1, USAGE10,
        "           defragment=enable|disable\n"));
    fprintf(stdout, catgets(catd, 1, USAGE11,
        "           balance=enable|disable\n"));
    fprintf(stdout, catgets(catd, 1, USAGE12,
        "           topIObalance=enable|disable\n"));
    fprintf(stdout, catgets(catd, 1, USAGE13,
        "           direct_io=enable|disable\n"));
    fprintf(stdout, catgets(catd, 1, USAGE14,
        "           percent_ios_when_busy=percent\n"));
    fprintf(stdout, catgets(catd, 1, USAGE15,
        "           ss_steady_state=hours\n\n"));

    exit(1);
}


/* Command types for vfast */
static
int 
request_type (
    const char *requestString
    )
{
    static struct {
        char *str;
        ssUIDmnOpsT opType;
    } request_types[] = {
        { "restart"     , SS_UI_RESTART    },
        { "stop"        , SS_UI_STOP       },
        { "activate"    , SS_UI_ACTIVATE   },
        { "deactivate"  , SS_UI_DEACTIVATE },
        { "suspend"     , SS_UI_SUSPEND    },
        { "status"      , SS_UI_STATUS     },
        { "showhidden"  , SS_UI_SHOWHIDDEN }
    };
    int i;

    for(i=sizeof(request_types)/sizeof(request_types[0]); --i >= 0; ) {
        if(strcmp(request_types[i].str,requestString) == 0)
            return(request_types[i].opType);
    }

    /* illegal request types */
    usage();
}


static
void
abort_prog(
    char *msg, ...
          )
{
  int err;
  va_list ap;

    if (UnmountFlag)
        if(close_dot_tags_file ())
            abort_prog("%s: Can't unmount bitfile sets\n", Prog);

    if (UnlockDmnFlag) {
        UnlockDmnFlag = FALSE;
        if( unlock_file (DmnLock, &err) ) {
            fprintf (stderr, catgets(catd, 1, 21,
                     "%s: Error unlocking '%s'.\n"),
                     Prog, MSFS_DMN_DIR);
            fprintf (stderr, catgets(catd, 1, 3,
                     "%s: Error = [%d] %s\n"),
                     Prog, err, ERRMSG (err));
         }
    }

    va_start( ap, msg );
    vfprintf( stderr, msg, ap );
    va_end( ap );

    fflush( stderr );
    program_exit_status = 3;
    exit(program_exit_status);
}

main(int argc,char *argv[])
{
    char *q;
    ssUIDmnOpsT opType;
    char domain_name[ML_SET_NAME_SZ];
    char fileset_name[ML_SET_NAME_SZ];
    mlStatusT sts;
    char exclusion_filename[255];
    mlDmnParamsT dmnParams;
    mlSSDmnParamsT ssDmnParams;
    char ui_params_list[255];
    int vols;
    int err;
    struct timeval curr_time;
    u_int time_remaining;
   

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_VFAST, NL_CAT_LOCALE);

    /* check for root */
    if (geteuid()) {
        fprintf (stderr, catgets(catd, 1, NOROOT,
                "\nPermission denied - user must be root to run %s.\n\n"),
                Prog);
        exit(1);
    }


    if (clu_is_ready()) {
        if (VERSW() != VERSW_ENABLE) {
            fprintf(stderr, catgets(catd, 1, 127,
                    "Entire cluster not up to same version: vfast is not supported\n"));
            exit(1);
        }
    }

    for (argv++; --argc > 0 && (*argv)[0]=='-'; argv++) {
        if(strcmp(argv[0],"--") == 0)
            break;

        for(q=argv[0]; *q; q++) {

            if (*q == 'o') {

                if (--argc <= 1)
                    usage();
                argv++;
                strcpy(ui_params_list, argv[0]);
                strcpy(domain_name, argv[1]);
                verify_domain(domain_name, &dmnParams, &vols, FALSE);

                init_event(&advfs_event);
                advfs_event.domain = domain_name;

                sts = advfs_ss_get_params(domain_name, &ssDmnParams);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 69,
                            "%s: cannot get domain params on domain %s; %s\n"),
                            Prog, domain_name, BSERRMSG(sts));
                    exit(1);
                }
                sts = set_ui_params(&ssDmnParams, ui_params_list);
                if (sts != EOK) {
                    exit(1);
                }
                sts = advfs_ss_set_params(domain_name, ssDmnParams);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 70,
                            "%s: cannot set %s on domain %s; %s\n"),
                            Prog, ui_params_list, domain_name, BSERRMSG(sts));
                    exit(1);
                }
                
                goto _finish;
                
            } else if ((*q == 'l') || (*q == 'L')) {

                if (--argc <= 1) 
                    usage();
                argv++;
                strcpy(domain_name, argv[1]);
                verify_domain(domain_name, &dmnParams, &vols, FALSE);

                dmnName = argv[1];
                sts = advfs_ss_get_params(domain_name, &ssDmnParams);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 69,
                            "%s: cannot get domain params on domain %s; %s\n"),
                            Prog, domain_name, BSERRMSG(sts));
                    exit(1);
                }

                if (strcmp(argv[0],"extents") == 0) {

                    if (*q == 'l') {
                        display_extents_list(vols, domain_name, dmnParams);
                        goto _finish;

                    } else if (*q == 'L') {
                        display_extents_summary();
                        goto _finish;

                    } else {
                        usage();
                    }
                } else if (strcmp(argv[0], "hotfiles") == 0) {
                    if (*q == 'l') {
                        display_hotfiles_list(dmnParams);
                        goto _finish;

                    } else if (*q == 'L') {
                        display_hotfiles_summary(dmnParams);
                        goto _finish;

                    } else {
                        usage();
                    }
                } else { /* other than extents and hotfiles */
                    usage();
                }
            } /* end if ((*q == 'l') || (*q == 'L')) */
        }     /* end for (q=argv[0]; *q; q++) */
    }         /* end for (argv++; --argvc > 0 && argv[0][0]=='-'; argv++) */

    if(argc <= 0 || argc > 2) {
        usage();
    }

    opType = request_type(argv[0]);
    if ((opType==SS_UI_ACTIVATE) || 
        (opType==SS_UI_DEACTIVATE) || 
        (opType==SS_UI_SUSPEND) || 
        (opType==SS_UI_STATUS)) {
            if (argv[1] == '\0')
                usage();
            else {
                strcpy(domain_name, argv[1]);
                if (opType==SS_UI_ACTIVATE) 
                    verify_domain(domain_name, &dmnParams, &vols, TRUE);
                else
                    verify_domain(domain_name, &dmnParams, &vols, FALSE);
                advfs_event.domain = domain_name;
            }
    } else if (opType==SS_UI_SHOWHIDDEN ) {
        if (argv[1] == '\0') {
            fprintf(stderr, "Usage: %s showhidden domain_name\n", Prog);
            exit(1);
        } else {
            strcpy(domain_name, argv[1]);
            verify_domain(domain_name, &dmnParams, &vols,FALSE);
        }
    } else if ((opType==SS_UI_RESTART) || 
               (opType==SS_UI_STOP)) {
        if (argc != 1)
            usage();
    }

    switch (opType) {
        mlSSDmnOpsT ssDmnCurrentState=0;
        int ssRunningState=0;

        case SS_UI_RESTART:
            sts = advfs_ss_dmn_ops (
                                    domain_name,
                                    opType,
                                    &ssDmnCurrentState,
                                    &ssRunningState );

            if (sts != EOK) {
                if (sts == E_DOMAIN_NOT_ACTIVATED) 
                    fprintf (stderr, catgets(catd, 1, NOTINITED,
                         "%s: At least one domain must have been "
                         "previously mounted to start vfast.\n"),
                         Prog);
                else
                    fprintf (stderr, catgets(catd, 1, 71,
                         "%s: cannot restart on domain %s; %s\n"),
                         Prog, domain_name, BSERRMSG(sts));
                exit(1);
            }
            break;

        case SS_UI_STOP:
            sts = advfs_ss_dmn_ops (
                                    domain_name,
                                    opType,
                                    &ssDmnCurrentState,
                                    &ssRunningState );

            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 72,
                         "%s: cannot stop on domain %s; %s\n"),
                         Prog, domain_name, BSERRMSG(sts));
                exit(1);
            }
            break;

        case SS_UI_ACTIVATE:
            sts = advfs_ss_dmn_ops (
                                    domain_name,
                                    opType,
                                    &ssDmnCurrentState,
                                    &ssRunningState );

            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 73,
                         "%s: cannot activate on domain %s; %s\n"),
                         Prog, domain_name, BSERRMSG(sts));
                exit(1);
            }
            advfs_post_user_event(EVENT_SS_ACTIVATED, advfs_event, Prog);
            break;

        case SS_UI_DEACTIVATE:
            sts = advfs_ss_dmn_ops (
                                    domain_name,
                                    opType,
                                    &ssDmnCurrentState,
                                    &ssRunningState );

            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 74,
                         "%s: cannot deactivate on domain %s; %s\n"),
                         Prog, domain_name, BSERRMSG(sts));
                exit(1);
            }
            advfs_post_user_event(EVENT_SS_DEACTIVATED, advfs_event, Prog);
            break;

        case SS_UI_SUSPEND:
            sts = advfs_ss_dmn_ops (
                                    domain_name,
                                    opType,
                                    &ssDmnCurrentState,
                                    &ssRunningState );

            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 75,
                        "%s: cannot suspend on domain %s; %s\n"),
                        Prog, domain_name, BSERRMSG(sts));
                exit(1);
            }
            advfs_post_user_event(EVENT_SS_SUSPENDED, advfs_event, Prog);
            break;

        case SS_UI_STATUS:
            sts = advfs_ss_dmn_ops (
                                    domain_name,
                                    opType,
                                    &ssDmnCurrentState,
                                    &ssRunningState );

            if (sts != EOK) {
                fprintf (stderr, catgets(catd, 1, 76,
                        "%s: status not available on domain %s; %s\n"),
                        Prog, domain_name, BSERRMSG(sts));
                exit(1);
            } else {
                sts = advfs_ss_get_params(domain_name, &ssDmnParams);
                if (sts != EOK) {
                    fprintf (stderr, catgets(catd, 1, 77,
                        "%s: domain params not available on domain %s; %s\n"),
                        Prog, domain_name, BSERRMSG(sts));
                    exit(1);
                }

                if(ssRunningState == 0) {
                    fprintf (stdout, catgets(catd, 1, 100,
                                "\n%s currently stopped\n"), Prog);
                } else {
                    fprintf (stdout, catgets(catd, 1, 101,
                                "\n%s is currently running\n"), Prog);
                }

                if(ssDmnCurrentState == 0)
                    fprintf (stdout, catgets(catd, 1, 102,
                             "%s is deactivated on %s\n"), Prog, domain_name);
                else if(ssDmnCurrentState == 1)
                    fprintf (stdout, catgets(catd, 1, 103,
                             "%s is activated on %s\n"), Prog, domain_name);
                else if(ssDmnCurrentState == 2)
                    fprintf (stdout, catgets(catd, 1, 104,
                             "%s is suspended on %s\n"), Prog, domain_name);
                else
                    fprintf (stdout, catgets(catd, 1, 105,
                             "%s is unknown on %s\n"), Prog, domain_name);

                if(ssDmnParams.ssDmnDefragment==1)
                    fprintf (stdout, catgets(catd, 1, 106,
                            "vfast defragment:     enabled\n"));
                else
                    fprintf (stdout, catgets(catd, 1, 107,
                            "vfast defragment:     disabled\n"));

                if(ssDmnParams.ssDmnBalance==1)
                    fprintf (stdout, catgets(catd, 1, 108,
                            "vfast balance:        enabled\n"));
                else
                    fprintf (stdout, catgets(catd, 1, 109,
                            "vfast balance:        disabled\n"));

                if(ssDmnParams.ssDmnSmartPlace==1)
                    fprintf (stdout, catgets(catd, 1, 110,
                            "vfast top IO balance: enabled\n"));
                else
                    fprintf (stdout, catgets(catd, 1, 111,
                            "vfast top IO balance: disabled\n"));

                fprintf (stdout, catgets(catd, 1, 112, "\nOptions:\n"));

                if(ssDmnParams.ssDmnDirectIo==1)
                    fprintf (stdout, catgets(catd, 1, 113,
                            "  Direct IO File Processing: enabled\n"));
                else
                    fprintf (stdout, catgets(catd, 1, 114,
                            "  Direct IO File Processing: disabled\n"));

                fprintf (stdout, catgets(catd, 1, 115,
                    "  Percent IOs Allocated to %s When System Busy: %d%%\n"),
                    Prog, ssDmnParams.ssMaxPercentOfIoWhenBusy);

                gettimeofday(&curr_time, NULL);
                time_remaining = ssDmnParams.ssSteadyState -
                    ((curr_time.tv_sec - ssDmnParams.ssFirstMountTime) / 3600);

                if (ssDmnParams.ssFirstMountTime!=0) {
                    fprintf (stdout, catgets(catd, 1, 116,
                        "  Default Hours Until Steady State: %d; Hours remaining: "),
                        ssDmnParams.ssSteadyState);
                    fprintf (stdout, "%d\n",
                           ((signed)time_remaining<0) ? 0 : time_remaining);
                } else {
                    fprintf (stdout, catgets(catd, 1, 147,
                        "  Default Hours Until Steady State: %d\n"),
                        ssDmnParams.ssSteadyState);
                }

                fprintf (stdout, catgets(catd, 1, 117,
                    "  Total Files Defragmented:  %d\n"), 
                                ssDmnParams.ssFilesDefraged);
                fprintf (stdout, catgets(catd, 1, 118,
                    "  Total Pages Moved for Defragment:  %d\n"), 
                                ssDmnParams.ssPagesDefraged);
                fprintf (stdout, catgets(catd, 1, 119,
                    "  Total Extents Combined for Defragment:  %d\n"),
                                ssDmnParams.ssExtentsConsol);
                fprintf (stdout, catgets(catd, 1, 120,
                    "  Total Pages Moved for Balance:  %d\n"), 
                                ssDmnParams.ssPagesBalanced);
                fprintf (stdout, catgets(catd, 1, 121,
                    "  Total Files Moved for Volume IO Balance:  %d\n"), 
                                ssDmnParams.ssFilesIOBal);
                fprintf (stdout, catgets(catd, 1, 122,
                  "  Total Pages Moved for Volume Free Space Consolidation:  %d\n"), 
                                ssDmnParams.ssPagesConsol);
                (void)fflush(stdout);
            }
            break;

        case SS_UI_SHOWHIDDEN:
            sts = advfs_ss_get_params(domain_name, &ssDmnParams);
            if (sts != EOK) {
                fprintf (stdout, catgets(catd, 1, 123,
                        "%s: cannot get params; %s\n"),
                        Prog, BSERRMSG(sts));
                exit(1);
            }
            fprintf (stdout, catgets(catd, 1, 124,
                    "ssAccessThreshHits = %d\n"),
                    ssDmnParams.ssAccessThreshHits);
            fprintf (stdout, catgets(catd, 1, 125,
                    "ssSteadyState = %d\n"),
                    ssDmnParams.ssSteadyState);
            fprintf (stdout, catgets(catd, 1, 126,
                    "ssMinutesInHotList = %d\n"),
                    ssDmnParams.ssMinutesInHotList);
            break;
    }

_finish:
    return 0;

} /* end main */
