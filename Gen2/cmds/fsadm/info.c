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
 * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
 */

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <sys/errno.h>

#include <advfs/advfs_syscalls.h>
#include <advfs/fs_quota.h>           /* Needed for USER_QUOTA_FILE_TAG */
#include <advfs/bs_bitfile_sets.h>
#include "common.h"

extern int errno;
extern char *sys_errlist[];

static char *Prog = "fsadm info";

#define BUF_SZ 15
#define DEFAULT_FILESET "default"
#define FIELD_NM "%20s"

/* PROTOTYPES */
struct arg_info *check_info_args( int argc, char *argv[], int *V, int *m );
void info_usage( void );
int show_vols( adv_bf_dmn_params_t dmnParams );
int get_dmn_metadata_size( struct arg_info *infop,
			   adv_bf_dmn_params_t *dmnParams,
			   uint32_t *dmnMetaTotal );

int
info_main(int argc, char *argv[])
{
    adv_status_t sts;
    adv_bf_dmn_params_t dmnParams;
    int err;
    int V=0, m=0;
    uint32_t dmnMetaTotal = 0;
    bfSetParamsT setParams;
    uint32_t userId;
    uint64_t setIdx;
    int setFound     = FALSE;
    int exit_status  = 0;
    arg_infoT *infop = NULL;

    infop = check_info_args(argc, argv, &V, &m);
    if (infop == NULL) {
	/* There's no hope for what the user passed in, give up */
	info_usage();
	return(1);
    }

    if (V) {
	printf("%s", Voption(argc, argv));
	exit_status = 0;
	goto _exit;
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */

    sts = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR,
                                     BFD_ODS_LAST_VERSION_MINOR);
    if (sts != EOK) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ONDISK,
            "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
        return(1);
    }

#ifdef DEBUG
    printf("fspath: %s\tfileset: %s\n", 
            infop->fspath, infop->fset);
#endif /* DEBUG */

    /* process_advfs_arg routine stats the file system to make sure it exists */
    sts = advfs_get_dmnname_params(infop->fsname, &dmnParams);
    if (sts != EOK) {
	if( sts == EACCES ) {
	    fprintf( stderr, catgets( catd, MS_FSADM_INFO, INFO_NOTROOT,
		"%s: User must be privileged to run %s on an unmounted file system.\n" ),
		Prog, Prog );
	    exit_status = 1;
	    goto _exit;
	}
	fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOFSPARAMS,
	    "%s: Cannot get file system parameters for %s\n"),
	    Prog, infop->fsname);
	fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR, 
	    "%s: Error = %s\n"), Prog, BSERRMSG(sts));
	exit_status = 1;
	goto _exit;
    }

    if (m) {
        /*
         * Domain metadata size request
         */

	if (get_dmn_metadata_size(infop, &dmnParams, &dmnMetaTotal) !=  0) {
	    /* error or no sets in the file system */
	    exit_status = 1;
	    goto _exit;
	}

        printf(catgets(catd, MS_FSADM_INFO, INFO_METADATA,
                    "File system: %s, Metadata Size = %d Blocks (1K)\n"),
                infop->fsname, dmnMetaTotal);     
    } else {        
        char s[32];
        time_t tv;

        tv = (time_t)dmnParams.bfDomainId.id_sec;

        sprintf(s, "%s", ctime(&tv));
        s[strlen(s) - 1] = '\0';

        setIdx = 0;            /* start with first file set */

        /* Get the fileset params */
        do {
            sts = advfs_fset_get_info(infop->fsname, &setIdx, 
                    &setParams, &userId, 0);
            if (sts == EOK) {
                if (strcmp(infop->fset, setParams.setName)) {
                    continue;
                } else {
                    setFound = TRUE;
                    break;
                }
            }
        } while (sts == EOK);

        if (sts != E_NO_MORE_SETS && setFound == FALSE) {
            printf(catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
                        "%s: Error = %s\n"), 
                    Prog, BSERRMSG(sts));
            exit_status = 1;
            goto _exit;
        } 

        /* Print out the info */

        /* The following info is from the file system */
        printf("\n");
        printf(FIELD_NM" : %s\n", 
                catgets(catd, MS_FSADM_INFO, INFO_NAME, "Name"), 
                infop->is_multivol ? infop->fsname : infop->blkfile);
        printf(FIELD_NM" : %016x.%016x\n", 
                catgets(catd, MS_FSADM_INFO, INFO_ID, "Id"), 
                dmnParams.bfDomainId.id_sec,
                dmnParams.bfDomainId.id_usec);
        printf(FIELD_NM" : %d.%d\n", 
                catgets(catd, MS_FSADM_INFO, INFO_VERSION, "Version"), 
                dmnParams.dmnVersion.odv_major, dmnParams.dmnVersion.odv_minor);
        printf(FIELD_NM" : %d\n", 
                catgets(catd, MS_FSADM_INFO, INFO_LOGBLKS, "Log Blocks"), 
                dmnParams.ftxLogFobs);

        printf(FIELD_NM" : %d\n", 
		catgets(catd, MS_FSADM_INFO, INFO_USER_BLK, "User Block Size (1K)"),	
                dmnParams.userAllocSz);

        printf(FIELD_NM" : %d\n",
		catgets(catd, MS_FSADM_INFO, INFO_META_BLK, "Meta Block Size (1K)"),
		dmnParams.metaAllocSz);

        printf(FIELD_NM" : ",
                catgets(catd, MS_FSADM_INFO, INFO_THRESHOLD_UPPER, "Upper Threshold"));
        printf("%5s = %3d ",
                catgets(catd, MS_FSADM_INFO, INFO_THRESHOLD_LIMIT, "Limit"),
                dmnParams.dmnThreshold.threshold_upper_limit);
        printf("%8s = %3d\n",
                catgets(catd, MS_FSADM_INFO, INFO_THRESHOLD_INTERVAL, "Interval"),
                dmnParams.dmnThreshold.upper_event_interval);

        printf(FIELD_NM" : ",
                catgets(catd, MS_FSADM_INFO, INFO_THRESHOLD_LOWER, "Lower Threshold"));
        printf("%5s = %3d ",
                catgets(catd, MS_FSADM_INFO, INFO_THRESHOLD_LIMIT, "Limit"),
                dmnParams.dmnThreshold.threshold_lower_limit);
        printf("%8s = %3d\n",
                catgets(catd, MS_FSADM_INFO, INFO_THRESHOLD_INTERVAL, "Interval"),
                dmnParams.dmnThreshold.lower_event_interval);

        /* The following info is from the fileset */
        if (setFound == TRUE) {
            time_t set_tv;
            char set_time_str[32];

            set_tv = (time_t)setParams.bfspCreateTime;
            sprintf(set_time_str, "%s", ctime(&set_tv));
            set_time_str[strlen(set_time_str) - 1] = '\0';

	    printf(FIELD_NM" : %s\n", 
            	catgets( catd, MS_FSADM_INFO, INFO_SETNAME, "Set Name" ), 
		    setParams.setName);
            printf(FIELD_NM" : %08x.%08x\n",
	 	catgets( catd, MS_FSADM_INFO, INFO_SETTAG, "Set Tag" ),
                    setParams.bfSetId.dirTag.tag_num,
                    setParams.bfSetId.dirTag.tag_seq);
            printf(FIELD_NM" : %s\n",
                    catgets(catd, MS_FSADM_INFO, INFO_DATE, "Date Created"),
                    set_time_str);
            printf(FIELD_NM" : %lld\n", 
                    catgets(catd, MS_FSADM_INFO, INFO_TOTBLKS, "Total Blocks"), 
                    setParams.dmnTotalBlks);
            printf(FIELD_NM" : %lld\n", 
                    catgets(catd, MS_FSADM_INFO, INFO_FREEBLKS, "Free Blocks"),
                    setParams.dmnAvailBlks);
            printf(FIELD_NM" : %ld\n", 
                    catgets(catd, MS_FSADM_INFO, INFO_FILES, "Files"),
                    setParams.filesUsed);
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_QSTATUS,
                        "Quota Status"));
            if (setParams.quotaStatus & Q_STS_USR_QUOTA_EN) {
                printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_ON, "on"));
            } else {
                printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_OFF, "off"));
            }
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_BLKCLEAR,
                        "Blkclear"));
            if (setParams.bfSetFlags & BFS_OD_OBJ_SAFETY) {
                printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_ON, "on"));
            } else {
                printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_OFF, "off"));
            }
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_COMMON, COMMON_ISSNAP, "Is snapshot?"));
            if (!BS_BFS_EQL(setParams.bfspParentSnapSet,nilMlBfSetIdT)) {
                if (setParams.bfSetFlags & BFS_OD_OUT_OF_SYNC) {
                    printf("%s", catgets(catd, MS_FSADM_INFO, INFO_YESNOSYNC, 
                                "Yes (out-of-sync)"));
                } else {
                    printf("%s", catgets(catd, MS_FSADM_COMMON, COMMON_YES, 
                                "Yes"));
                }
                if (setParams.bfSetFlags & BFS_OD_HARD_OUT_OF_SYNC) {
                    printf(" %s", catgets(catd, MS_FSADM_INFO, INFO_HARDNOSYNC, 
                                "(unavailable)"));
                }
                printf("\n");
            } else {
                printf("%s\n", catgets(catd, MS_FSADM_COMMON, COMMON_NO, "No"));
            }
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_COMMON, COMMON_HASSNAP, "Has snapshot?"));
            if (!BS_BFS_EQL(setParams.bfspFirstChildSnap,nilMlBfSetIdT)) {
                printf("%s\n", catgets(catd, MS_FSADM_COMMON, COMMON_YES, "Yes"));
            } else {
                printf("%s\n", catgets(catd, MS_FSADM_COMMON, COMMON_NO, "No"));
            }
        } else {
            /* Error getting fileset info */
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_SETNAME, "Set Name"));
            printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : %016x.%08x\n", catgets(catd, MS_FSADM_INFO, INFO_SETTAG, "Set Tag"), 0, 0);
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_DATE, "Date Created"));
            printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_TOTBLKS,
                        "Total Blocks"));
            printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_FREEBLKS,
                        "Free Blocks"));
            printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_FILES,
                        "Files"));
            printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_QSTATUS,
                        "Quota Status"));
            printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : ", catgets(catd, MS_FSADM_INFO, INFO_BLKCLEAR,
                        "Blkclear"));
            printf("%s\n", catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : %s\n", catgets(catd, MS_FSADM_COMMON, COMMON_ISSNAP,
                    "Is snapshot?"),
                    catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : %s\n", catgets(catd, MS_FSADM_COMMON, COMMON_HASSNAP,
                    "Has snapshot?"),
                    catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : %s\n", catgets(catd, MS_FSADM_COMMON, COMMON_ISSNAP,
                    "Is snapshot?"),
                    catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
            printf(FIELD_NM" : %s\n", catgets(catd, MS_FSADM_COMMON, COMMON_HASSNAP,
                    "Has snapshot?"),
                    catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL,
                        "-data unavailable-"));
        }

        if (show_vols(dmnParams)) {
            exit_status = 1;
            goto _exit;
        }
    }

_exit:

    if (infop) {
        free(infop);
    }

    return exit_status;
}

void
info_usage(void)
{
    usage(catgets(catd, MS_FSADM_INFO, INFO_USAGE,
	"%s [-V] [-m] fsname\n"), Prog);
}


int show_vols( 
    adv_bf_dmn_params_t dmnParams
    )
{
    adv_status_t sts;
    char *tok;
    int vol, vols, volIndex, logVolIndex;
    adv_vol_info_t volInfo;
    adv_vol_prop_t volProperties;
    adv_vol_ioq_params_t volIoQParams;
    adv_vol_counters_t volCounters;
    uint32_t *volIndexArray;
    long totalSpace = 0L, totalFree = 0L;
    int volError;
    int total = 1;

    logVolIndex = BS_BFTAG_VDI(dmnParams.ftxLogTag);

    volIndexArray = (uint32_t *) malloc(dmnParams.curNumVols * sizeof(uint32_t));
    if (volIndexArray == NULL) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM, 
                    "%s: insufficient memory.\n"), Prog);
        return 1;
    }

    sts = advfs_get_dmn_vol_list(dmnParams.bfDomainId, 
                                 dmnParams.curNumVols, 
                                 volIndexArray, 
                                 &vols);
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf(stdout, catgets(catd, MS_FSADM_COMMON, COMMON_NOVOLINFO, 
                    "%s: unable to display volume info; file system not active\n"), Prog);
        return 0;

    } else if (sts != EOK) {
        fprintf(stderr, catgets(catd, MS_FSADM_INFO, INFO_NOVOLLIST, 
                    "%s: get file system vol list error %s\n"), 
                 Prog, BSERRMSG(sts));
        free(volIndexArray);
        return 1;
    }

    printf("%s", catgets(catd, MS_FSADM_INFO, INFO_BANNER,
                "\n    Vol    1K-Blks        Free  % Used  Rblks  Wblks  Vol Name\n"));

    volError = FALSE;

    for (vol = 0; vol < vols; vol++) {
        sts = advfs_get_vol_params(dmnParams.bfDomainId, 
                                   volIndexArray[vol], 
                                   &volInfo, &volProperties,
                                   &volIoQParams, &volCounters);

        if (sts != EOK) {
            if (sts != EBAD_VDI) {
                fprintf(stderr, 
                        catgets(catd, MS_FSADM_COMMON, COMMON_VOLPARAMS_ERR, 
                            "%s: get vol params error %s\n"), 
                        Prog, BSERRMSG(sts));
                volError = TRUE;
            }
            else {
                /* Try again to get info which doesn't require reading
                   the bmt.  If the vol is being removed (so the bmt is
                   unavailable), the service class will be nil.
                 */
                sts = advfs_get_vol_params(dmnParams.bfDomainId, 
                                           volIndexArray[vol], 
                                           0, &volProperties,
                                           &volIoQParams, &volCounters);
                if (sts == EOK && volProperties.serviceClass == 0) {
                    fprintf(stderr, "%38s", 
                            catgets(catd, MS_FSADM_INFO, INFO_UNAVAIL, 
                                "-data unavailable-"));
                    printf("%5d  ", volIoQParams.rdMaxIo);
                    printf("%5d", volIoQParams.wrMaxIo);
                    printf("  %-s\n", volProperties.volName);
                    total = 0;
                }
                else if (sts != EBAD_VDI) {
                    fprintf(stderr, 
                            catgets(catd, MS_FSADM_COMMON, COMMON_VOLPARAMS_ERR, 
                                "%s: get vol params error %s\n"), 
                            Prog, BSERRMSG(sts));
                    volError = TRUE;
                }
            }
        } else {
            if (volInfo.volIndex == logVolIndex) {
                printf("%6uL ", volInfo.volIndex);
            } else {
                printf("%6u  ", volInfo.volIndex);
            }
            printf("%10u  ", volInfo.volSize);
            printf("%10u  ", volInfo.freeClusters * volInfo.sbmBlksBit);
            printf("%5.0f%%  ", 100.0 - 
                    ((double) (volInfo.freeClusters * volInfo.sbmBlksBit) / 
                     (double) volInfo.volSize * 100.0));
            printf("%5d  ", volIoQParams.rdMaxIo);
            printf("%5d", volIoQParams.wrMaxIo);
            printf("  %-s\n", volProperties.volName);
            totalSpace += volInfo.volSize;
            totalFree += (volInfo.freeClusters * volInfo.sbmBlksBit);
        }
    }

    if (vols > 1 && total && (volError == FALSE)) {
        printf("%18s  ", "----------");
        printf("%10s  ", "----------");
        printf("%6s\n", "------");
        printf("%18lu  ", totalSpace);
        printf("%10lu  ", totalFree);
        printf("%5.0f%%\n", 100.0 - 
                ((double) totalFree / (double) totalSpace * 100.0));
    }

    if (volError)
        return 1;
    else
        return 0;
}

int
get_dmn_metadata_size(
        arg_infoT *infop, 	      /* in */
        adv_bf_dmn_params_t *dmnParams,      /* in */
        uint32_t *dmnMetaTotal        /* out */    
        )
{
    uint32_t setIdx;  
    bfSetIdT bfSetId;
    bfSetParamsT setParams;
    adv_status_t sts; 
    int err, logVolIndex, vols;   
    int unmounted = 0, ii; 
    struct stat stBuf; 
    char strBuf[MAXPATHLEN], dirName[MAXPATHLEN]; 
    char buf[BUF_SZ]; 
    uint32_t fsMetaTotal = 0, *volIndexArray; 
    adv_vol_info_t volInfo; 
    adv_vol_prop_t volProperties; 
    adv_vol_ioq_params_t volIoQParams; 
    adv_vol_counters_t volCounters; 
    char* mntdirname = NULL;

    setIdx = 0;            /* start with first file set */

    logVolIndex = BS_BFTAG_VDI(dmnParams->ftxLogTag);    
    volIndexArray = (uint32_t *) malloc(dmnParams->curNumVols * 
            sizeof(uint32_t));
    if (volIndexArray == NULL) {
        fprintf(stderr, 
                catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM, 
                    "%s: insufficient memory.\n"), 
                Prog);
        return 1;
    }

    sts = advfs_get_dmn_vol_list(dmnParams->bfDomainId, 
            dmnParams->curNumVols, 
            volIndexArray, 
            &vols);

    if (sts != EOK) {
        fprintf(stderr, 
                catgets(catd, MS_FSADM_INFO, INFO_NOVOLLIST, 
                    "%s: get file system vol list error %s\n"), 
                Prog, BSERRMSG(sts));
        free(volIndexArray);
        return 1;
    }

    if (advfs_is_mounted(infop, infop->fset, NULL, &mntdirname) != 1) {
        fprintf(stderr, 
                catgets(catd, MS_FSADM_INFO, INFO_MOUNTFS,
                    "%s: Error looking for mounted fs '%s'\n"),
                Prog, infop->fsname);
        return 1;
    }

    sprintf(dirName, "%s/%s", mntdirname, "/.tags/M-");

    /*
     * Get stats for each reserved file on each volume
     */

#ifdef DEBUG
    printf("%s: statting all reserved files for each volume\n", Prog);
#endif
    for (ii = 0; ii < dmnParams->curNumVols; ii++) {

        sts = advfs_get_vol_params(dmnParams->bfDomainId, 
                volIndexArray[ii], 
                &volInfo, &volProperties,
                &volIoQParams, &volCounters);

        if (sts != EOK) {                   
            fprintf(stderr, 
                    catgets(catd, MS_FSADM_COMMON, COMMON_VOLPARAMS_ERR, 
                        "%s: get vol params error %s\n"), 
                    Prog, BSERRMSG(sts));
            return 1;
        }

        strcpy(strBuf, dirName);
        snprintf(buf, sizeof(buf), "%d",
                volIndexArray[ii] * 6 + BFM_RBMT);
        strcat(strBuf, buf);

        if (stat(strBuf, &stBuf) != 0) {
            perror(Prog);
            return 1;
        }
        *dmnMetaTotal += stBuf.st_blocks;

        strcpy(strBuf, dirName);
        snprintf(buf, sizeof(buf), "%d",
                volIndexArray[ii] * 6 + BFM_SBM);
        strcat(strBuf, buf);

        if (stat(strBuf, &stBuf) != 0) {
            perror(Prog);
            return 1;
        }
        *dmnMetaTotal += stBuf.st_blocks;

        /*
         * Skip transaction log size, and root tag size.  This is consistent 
         * with old vdf accounting, it is actually wrong if the log file has 
         * been moved.  The old log and tag files are not removed.
         */

        if (volInfo.volIndex == logVolIndex) {

            strcpy(strBuf, dirName);
            snprintf(buf, sizeof(buf), "%d",
                    volIndexArray[ii] * 6 + BFM_FTXLOG);
            strcat(strBuf, buf);

            if (stat(strBuf, &stBuf) != 0) {
                perror(Prog);
                return 1;
            }
            *dmnMetaTotal += stBuf.st_blocks;

            strcpy(strBuf, dirName);
            snprintf(buf, sizeof(buf), "%d",
                    volIndexArray[ii] * 6 + BFM_BFSDIR);
            strcat(strBuf, buf);

            if (stat(strBuf, &stBuf) != 0) {
                perror(Prog);
                return 1;
            }
            *dmnMetaTotal += stBuf.st_blocks;

        } /* endif log file volume */

        strcpy(strBuf, dirName);
        snprintf(buf, sizeof(buf), "%d",
                volIndexArray[ii] * 6 + BFM_BMT);
        strcat(strBuf, buf);

        if (stat(strBuf, &stBuf) != 0) {
            perror(Prog);
            return 1;
        }
        *dmnMetaTotal += stBuf.st_blocks;

        strcpy(strBuf, dirName);
        snprintf(buf, sizeof(buf), "%d",
                volIndexArray[ii] * 6 + BFM_MISC);
        strcat(strBuf, buf);

        if (stat(strBuf, &stBuf) != 0) {
            perror(Prog);
            return 1;
        }
        *dmnMetaTotal += stBuf.st_blocks;

    }  /* end volume loop */                   

#ifdef DEBUG
    printf("%s: done statting\n", Prog);
#endif

    /* reset dirName */
    strcpy(dirName, mntdirname);

    /* get fileset info */
    sts = advfs_fset_get_id(infop->fsname, infop->fset, &bfSetId);
    if(sts != EOK) {
        fprintf(stderr, "%s: get fset id error: %s\n",
                Prog, BSERRMSG(sts));
        return 1;
    }

    sts = advfs_get_bfset_params(bfSetId, &setParams);

#ifdef DEBUG
    printf("%s: got bfset params for %s\n", Prog, setParams.setName);
#endif
    if (sts == EOK) {

        if (BS_BFS_EQL(setParams.bfspParentSnapSet, nilMlBfSetIdT)) {
            /*
             * This is a real fileset, not a clone
             *     Skipping this for clones is wrong, but is done
             *     for consistency with original VDF output, this 
             *     should be corrected when it is better understood
             */

            /*
             * Get fileset's quota.user file size
             */

            snprintf(strBuf, sizeof(strBuf), "%s/.tags/%d", dirName, 
                    USER_QUOTA_FILE_TAG);
            if (stat(strBuf, &stBuf) != 0) {
                perror(Prog);
#ifdef DEBUG
                fprintf(stderr, "error on stat for user quota file\n");
                fprintf(stderr, "%s/.tags/%d\n", dirName, USER_QUOTA_FILE_TAG);
#endif
                return 1;
            }
            fsMetaTotal += stBuf.st_blocks;

            /*
             * Get fileset's quota.group file size
             */                        

            snprintf(strBuf, sizeof(strBuf), "%s/.tags/%d", dirName, 
                    GROUP_QUOTA_FILE_TAG);      
            if (stat(strBuf, &stBuf) != 0) {
                perror(Prog);
#ifdef DEBUG
                fprintf(stderr, "error on stat for group quota file\n");
#endif
                return 1;
            }
            fsMetaTotal += stBuf.st_blocks;

        }  /* endif for not a clone */

        /*
         * Get fileset's tag directory size
         */

        snprintf(strBuf, sizeof(strBuf), "%s/.tags/M%d", dirName,
                setParams.bfSetId.dirTag.tag_num);
        if (stat(strBuf, &stBuf) != 0) {
            perror(Prog);
#ifdef DEBUG
            fprintf(stderr, "error on stat for tag dir file\n");
#endif
            return 1;
        }
        fsMetaTotal += stBuf.st_blocks;
        unmounted = 0;

        *dmnMetaTotal += fsMetaTotal;

    }  else {
        fprintf(stderr, catgets(catd, MS_FSADM_INFO, INFO_NOSETINFO, 
                    "%s: Cannot show set info for file system '%s'\n"), 
                Prog, infop->fsname);
        fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR, 
                    "%s: Error = %s\n"), 
                Prog, BSERRMSG (sts));
        return -1;
    }

    return 0;
}

struct
arg_info *
check_info_args( int argc, char *argv[], int *V, int *m )
{
    int c=0;
    extern int optind;

    optind = 1;

    /*
     ** Get user-specified command line arguments.
     */
     while ((c = getopt(argc, argv, "Vm")) != EOF) {
	switch (c) {
	    case 'V':
		(*V)++;
		break;
	    case 'm':
		(*m)++;
		break;
	    default:
		return NULL;
	}
    }

    if ((*m > 1) || (*V > 1) || (argc - 1 - optind) != 0) {
	return NULL;
    }

    return (process_advfs_arg(argv[optind]));
}
