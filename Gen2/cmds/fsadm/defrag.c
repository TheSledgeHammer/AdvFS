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
 *
 * Abstract:
 *
 *    defrag.c
 *    This module implements the "defragment" utility.
 *
 */
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <math.h>
#include <stdarg.h>
#include <libgen.h>
#include <sys/errno.h>
#include <sys/file.h>
#include <sys/signal.h>
#include <sys/param.h>
#include <sys/time.h>
#include <pwd.h>
#include <advfs/ms_public.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/bs_error.h>
#include <locale.h>
#include <pthread.h>
#include "fsadm_advfs_msg.h"
#include "defrag.h"

/* note to maintainer
howmany(x, y) (((x)+((y)-1))/(y))   from param.h
*/

/*
 * Global Variables
 */
static char *Prog = "fsadm defrag";
static arg_infoT *infop;
static int displaySummary = 0;
static boolean displayFreeSpaceFlag = FALSE; /* Debug setting */
static int program_exit_status=0;

adv_bf_dmn_params_t gdmnParams;
static advfs_ev advfs_event;
static int thisDmnLock;
static int num_vols;

static char *metadataRootPathName = NULL;
static int volIndexCnt;
static int *volSubscrArray = NULL;
static boolean ignoreErrorsFlag = FALSE;
static boolean on_last_volume = FALSE;
static boolean hasTimeLimit = FALSE;
static boolean hasStrictTimeLimit = FALSE;
static int64_t allowedTimeInterval;
static int limitTime;

static uint64_t largeFileThreshhold;

/* Tops of the ranges into which the free space holes will be clasified, in
 * 1K units.  The last range has a top of zero, which means no top.  Ranges
 * over and including 1000 must be in multiples of 1000. */
static const int freeRangeTop[FREE_RANGE_COUNT] = {100,1000,10000,0};


/* Create mutex to synchronize access among working volume threads.
 *
 * Allvol_mutex - used for access to volume storage totals for the whole 
 * domain.  
 * volPassStat_mutex - used for keeping track of the volume thread passes, 
 *                     records the passes done on each volume and is used 
 *                     by print thread and main thread pool routine to 
 *                     figure out which volume to defragment next.
 * passCheck_cv - (condition variable ) awakens print thread so that 
 *                it can check to see if 
 *                all threads have completed the current pass number.  If 
 *                they have then spit out the verbage for the pass.  If 
 *                not, then continue to wait for this momentous event.
 */
static pthread_mutex_t Allvol_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_mutex_t volPassStat_mutex = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  passCheck_cv = PTHREAD_COND_INITIALIZER;


/*
 * defrag main -
 *
 * Parse command args (argv[]) and start defragment
 */

defrag_main (
      int argc,
      char *argv[]
      )

{

    int c;
    int Vflg =0;
    int whole_domain = 1;
    char defrag_this_file[MAXPATHLEN+1] = "";
    int multi_threaded =1;

    int err;
    int num_threads=0;
    volDataT *volData = NULL;  /* array - volume data for each volume */
    volFreeSpaceDataT *volFreeSpaceData; /* an array that holds free space */
                                         /* info for each volume */ 
    adv_status_t sts=EOK;
    adv_ss_dmn_params_t ssDmnParams;
    ssDmnOpT ssDmnCurrentState=0;
    int ssRunningState=0;


    extern char *optarg;
    extern int optind;

    struct rlimit rlimit;

    curPassNumber=0;

    /* Must be privileged to run */
    check_root( Prog );

    /*
     * Need to make sure we have access to all the memory we
     * can get access to.  This is important for filesets with
     * large number of files.
     */
    rlimit.rlim_max = RLIM_INFINITY;
    rlimit.rlim_cur = RLIM_INFINITY;

    err = setrlimit( RLIMIT_DATA, &rlimit );
    if( err != 0 ) {
	fprintf( stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n" ),
	    Prog, errno, ERRMSG( errno ) );
    }

    /*
     ** Get user-specified command line arguments.
     */
    while ((c = getopt (argc, argv, "Ve:Lw:s")) != EOF) {
        switch (c) {

          case 'V':
            Vflg++;
            break;

          case 'e':
            whole_domain=0;
            sprintf(defrag_this_file, "%s", optarg);
            break;

          case 'w':
            hasTimeLimit++;
            allowedTimeInterval = 60 * strtoul(optarg, NULL, 0);
            break;

          case 'L':
            displaySummary++;
            break;

          case 's':
            /* Hidden debug mode */
            multi_threaded =0;
            break;

          default:
            defrag_usage ();
            return(1);
        }
    }  /* end while */

    if (optind != (argc - 1)) {
        /* missing required args */
        defrag_usage();
        return(1);
    }

    if (hasTimeLimit && allowedTimeInterval < 1) {
        defrag_usage();
        return(1);
    }

    /* print Voption if -V was specified */
    if(Vflg) {
        /* print verified command line */
        printf("%s", Voption(argc, argv));
        return(0);
    }

    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    err = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR,
                                     BFD_ODS_LAST_VERSION_MINOR);
    if (err != EOK) {
        fprintf( stderr, CMSG(COMMON_ONDISK,
            "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
        return(1);
    }

    /*
     * Get the device special and fsname 
     */
    infop = process_advfs_arg( argv[optind] );
    if( infop == NULL ) {
        defrag_usage();
        return(1);
    }


    init_event(&advfs_event);
    advfs_event.domain = infop->fsname;

    /*
     * If autotune is activated on a domain AND if autotune defragment OR
     * autotune balance OR autotune topIObalance are enabled, don't run
     * the defragment utility.
     */
    sts = advfs_ss_dmn_ops(infop->fsname,
                          SS_UI_STATUS,
                          &ssDmnCurrentState,
                          &ssRunningState);

    if (sts != EOK) {
        fprintf (stderr, MSG(DF_NOSTAT,
                 "%s: status not available on domain %s; %s\n"),
                 Prog, infop->fsname, BSERRMSG(sts));
        return(1);
    }

    sts = advfs_ss_get_params(infop->fsname, &ssDmnParams);
    if (sts != EOK) {
        fprintf(stderr, MSG(DF_NOPARMS,
                "%s: Cannot get domain params on domain %s; %s\n"),
                Prog, infop->fsname, BSERRMSG(sts));
    }

    if (((ssDmnParams.ssDmnDefragment == 1) ||
         (ssDmnParams.ssDmnDefragAll  == 1) ||
         (ssDmnParams.ssDmnBalance    == 1) ||
         (ssDmnParams.ssDmnSmartPlace == 1)) &&
        (ssDmnCurrentState == 1)) {
        fprintf(stderr, MSG(DF_ERRVRUN,
                "%s: autotune is currently running in the background, cannot run %s on domain '%s'\n"),
                Prog, Prog, infop->fsname);
        return(1);
    }

    if (hasStrictTimeLimit) {
        hasTimeLimit = TRUE;
    }

    if (hasTimeLimit) {
        err = get_system_time(&limitTime);
        if (err) {
            abort_prog(MSG(DF_BADTIME, 
                      "%s: unable to set a time limit for defragment\n"), Prog);
        }
        limitTime += allowedTimeInterval;
        if (hasStrictTimeLimit) {
            limitTime = limitTime - 10; /* allow 10 minutes to be sure 
                                         * it completes any ongoing work */
        }
    }

    /*
     * Disable interrupts until mounting of bitfile sets is completed
     */
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    /* open the domain and load each volume's parameters */

    num_vols = open_domain( &volData,
                            &volFreeSpaceData,
                            &gdmnParams,
                            &volPassStat) ;

    if(!num_vols) {
        fprintf(stdout, MSG(DF_NOVOLSFOUND, 
	    "%s: Unable to open storage domain %s.\n"), Prog, infop->fsname);
	(void)fflush(stdout);
        return(1);
    }
    
    /*
     * Enable interrupts.
     */
    signal (SIGINT, sigHandler);
    signal (SIGQUIT, sigHandler);
    signal (SIGPIPE, sigHandler);
    signal (SIGTERM, sigHandler);

    if((num_threads <= 0) || (num_threads > MAX_THREADS)) {
        int out_bounds = 0;
	/*
	 * Check to see if out of bounds, 0 is the default case.
	 */
        if (num_threads != 0) {
	    out_bounds = 1;
	}

	/*
	 * Set threads to a reasonable number.
	 */
        if(num_vols <= MAX_THREADS) {
            num_threads = num_vols;  /* default is number of threads == 
                                      * num of vols */
        } else {
            num_threads = MAX_THREADS;    /* if too many vols, set it to 20 */
        }
	
	/*
	 * If number out of bounds, print message.
	 */
	if (out_bounds == 1) {
	    fprintf(stdout, MSG(DF_INVALTHREADS, 
		    "%s: The number of threads specified is out of bounds, setting to %d\n"),
		    Prog, num_threads);
	    (void)fflush(stdout);
	}
    }

    /*
     * Whole domain and multi-threaded is the norm.  Single threaded is
     * a debug mode of whole domain. If the user requests a specific file 
     * then we are single-threaded by default.
     */
    if (whole_domain) {
        if (multi_threaded) {
            /* Normal path */
            tpool_init(tpoolp,
                    num_threads,
                    &gdmnParams,   
                    volData,                
                    volFreeSpaceData);
        } else { 
            /* Single threaded debug mode */
            package_t *p;
            int vol;

            fprintf( stdout, MSG(DF_DBGMODE,
                "%s: Running in single threaded debug mode.\n"), Prog);
            if ((p = (package_t *)malloc(sizeof(package_t))) == NULL) {
                fprintf( stderr, MSG(DF_NOMEM,
                    "%s: Cannot allocate memory\n"), Prog);
            }
        
            /* loop through volumes */
            for(vol=0; vol<num_vols ; vol++) {
                p->volume = vol;
                p->dmnParams = &gdmnParams;
                p->volData = &volData[vol];
                p->volFreeSpaceData = &volFreeSpaceData[vol];
                defragment_main(p); /* Do work on this volume */
            }
            free(p);
        }
    } else { /* defrag a specific file */
        int fd=-1;
        int errcode;
        arg_infoT *tmp_infop;

        fd = open(defrag_this_file, O_RDONLY, 0);
        if (fd < 0) {
            fprintf( stderr, MSG(DF_FILEOPEN,
                "%s: Could not access file %s\n"), Prog, defrag_this_file);
            program_exit_status=1;
            goto cleanup_single_file;
        }

        if ((get_fsinfo_from_file(defrag_this_file, &tmp_infop, &errcode) != 0) ||
            (strcmp(tmp_infop->fsname, infop->fsname) != 0)) {
            fprintf( stderr, MSG(DF_NOTINFS,
                "%s: File %s is not in filesystem %s\n"), 
                     Prog, defrag_this_file, infop->fsname);
            program_exit_status=1;
            goto cleanup_single_file;
        }

        sts = file_migrate(Prog, fd,-1,-1,-1,-1,-1);
        if ( sts ) {
            fprintf( stderr, MSG(DF_FILEFAIL,
                "%s: Single file defragment failed, status %d\n"), Prog, sts);
            program_exit_status=1;
            goto cleanup_single_file;
        }

cleanup_single_file:

        if (fd >=0) {
            close(fd);
        }
    }

    (void) close_dot_tags (Prog);

    if(volPassStat)      free(volPassStat);
    if(volData)          free(volData);
    if(volFreeSpaceData) free(volFreeSpaceData);
    if(volSubscrArray)   free(volSubscrArray);
    if(AllvolFreeSpaceData) free(AllvolFreeSpaceData);

    /*
     * Disable interrupts.
     */
    signal (SIGINT, SIG_IGN);
    signal (SIGQUIT, SIG_IGN);
    signal (SIGPIPE, SIG_IGN);
    signal (SIGTERM, SIG_IGN);

    defragment_exit();

}  /* end main */


/*
 * defragment_main
 *
 * This function defragments the files on the specified domain.
 */

static
void
defragment_main (
               package_t *p  /* in */
               )

{
    int err, i;
    int ret=0;
    int err_find=0;
    adv_status_t sts = EOK;
    boolean savedReverseFlag;
    bfListEntryT *bfEntry = NULL;
    bfListEntryT *nextBfEntry = NULL;
    boolean reinsertSideFlag;

    /*
     * For each pass/volume (if volumes count matches thread count):
     *
     *  1) Select an area to be cleared out that is some fraction
     *     of the total free space on the volume.
     *  2) Migrate all extents out of this area.
     *  3) Migrate whole files into the area (defragmenting them
     *     in the process).
     */

    /* 
     * set time limit for next pass 
     */
    if (hasTimeLimit) {
        err = set_time_limit(p->volData->passNumber, 
                             &p->volData->startPassTime, 
                             &p->volData->endPassTime,
                             &p->volData->clearLimitTime,
                             p->volData );
        if (err) {
            goto reset_vol_ptr;
        }
    }


    /* 
     * select the clearing strategy
     */
    select_clear_strategy(p->volData->passNumber, 
                          p->volData);

    /*
     * Determine how much free space is left on this volume
     */
    err = update_volume_data(p->dmnParams, 
                                 p->volData);
    if (err) {
        goto reset_vol_ptr;
    }

    if ((p->volData->dmnFreeSizeFract < MINIMUM_FREE_SIZE_FRACTION) &&
        (p->volData->dmnFreeSize < MINIMUM_VOLUME_FREE_BLOCKS) &&
        !p->volData->stopDefragment) {

        fprintf(stderr, MSG(DF_NOSPACE,
               "%s: Insufficient free space for defragmenting volume %d.\n"),
                Prog, p->volume+1);

        pthread_mutex_lock(&volPassStat_mutex );
        volPassStat[p->volume].not_enough_space = TRUE;
        pthread_mutex_unlock(&volPassStat_mutex );

        err = NOT_ENOUGH_SPACE;
        goto reset_vol_ptr;

    }

    largeFileThreshhold = p->volData->dmnFreeSize / (volIndexCnt * LARGE_FILE_PCT_OF_DMN);

    /*
     * Determine where clearest part of disk is. We will clear it later.
     * Set the point to move extents to on the current volume to after the
     * clearest part of disk.
     */

    err = reset_clear_point(  p->volData,
                              p->volFreeSpaceData, 
                              &p->volData->areaPopulation,
                              p->volData->passNumber,
                              p->volData->unimprovedScore,
                              p->dmnParams,
                              p->volData->averageExtentCount) ;
    if (err) {
        goto reset_vol_ptr;
    }

    /* Clear out an area on this volume, unless it already is clear.
     * Even then, it's necessary to call move_files_clear to gather
     * a list of files for the fill phase later on. 
     */

    p->volData->extentCount = 0;
    p->volData->fileWithExtCount = 0;
    p->volData->totalIOCount = 0;
    p->volData->totalBestIOCount = 0;
    p->volData->shortenedClearPass = FALSE;

    /* 
     * PART 1
     * move the extents in the sparsest area on the volume out of the area 
     * to be cleared.  Later fill in this cleared area with extents from 
     * fragmented files, thus defragmenting them in the process.
     */

    /* 
     * for this volume, move the file extents out of the area to be cleared 
     * onto same disk in other areas.
     */

    err = move_files_clear(
                            p->dmnParams->bfDomainId, 
                            p->volData,
                            p->volume,
                            p->volData->clearLimitTime,
                            &reinsertSideFlag
                          );
    if (err) {
        goto reset_vol_ptr;
    }

    /* remove all noFillMove==TRUE */
    bf_clean_list( bf_criteria1_list_start[p->volume],
                   bf_criteria1_list_last[p->volume],
                   p->volume,
                   1
                 );

    /* It is possible that, during the clear, we ran out of space on
     * this volume.  Message if so
     */

    if (!p->volData->doFill)  {
        fprintf(stderr, MSG(DF_NOSPACE,
               "%s: Insufficient free space for defragmenting volume %d.\n"),
                Prog, p->volume+1);

        pthread_mutex_lock(&volPassStat_mutex );
        volPassStat[p->volume].not_enough_space = TRUE;
        pthread_mutex_unlock(&volPassStat_mutex );

        err = NOT_ENOUGH_SPACE;
        goto reset_vol_ptr;
    }

    /* update the volumes with the new free space and performance data */
    update_stats(p->volFreeSpaceData, 
                 p->volData->passNumber,
                 p->volData,
                 p->volume);

    /* if verbal only then skip the actual file move operation */
    if (p->volData->stopDefragment) {
        goto reset_vol_ptr;
    }

    /* First set the free space cache to point to the area that was just 
     * cleared.  Then move the worst fragmented files into the cleared area.
     */

    if (p->volData->doFill) {
        sts = advfs_reset_free_space_cache(
                          p->dmnParams->bfDomainId,
                          p->volData->bsed_vol_index,
                          p->volData->lowerBlkClearBound / 
                                          p->volData->volStgCluster);
        if (sts != EOK) {
            fprintf(stderr, MSG(DF_PASSERR,
                  "%s: Error occurred during pass %d on volume %d.   Continuing...\n"),
                   Prog, p->volData->passNumber, p->volume+1);
            goto reset_vol_ptr;
        }
    }

    /* 
     * PART 2
     * move the worst fragmented files into the cleared area until it is full 
     */

    /* Determine how much free space there is on this volume.*/

    err = update_volume_data(p->dmnParams,
                                 p->volData);
    if (err) {
        goto reset_vol_ptr;
    }

    /* adjust the remaining area to be cleared for the filling phase */

    /* 
     * After clearing, we need to set the remainingClearAreaSize to the 
     * lessor of the actual cleared size, the planned to-be-cleared size, 
     * or 85% of the volumes current free size.
     * This is necessary because it is possible that during the clear phase, 
     * a disk got more filled up so that there isn't as much free space 
     * left as the area we cleared.
     *
     * remainingClearAreaSize will become the goal for amount of filling
     * to do.
     */
    if (p->volData->actualClearedAreaSize < p->volData->clearAreaSize) {
        p->volData->remainingClearAreaSize = p->volData->actualClearedAreaSize;
    } else {
        p->volData->remainingClearAreaSize = p->volData->clearAreaSize;
    }

    if (p->volData->remainingClearAreaSize > p->volData->volFreeSize) {
        p->volData->remainingClearAreaSize = p->volData->volFreeSize *
                                                        CLEAR_SIZE_FRACTION;
    }

    /* Test the eligibility of the volume. */
    if (p->volData->doFill) {
        p->volData->isEligibleTarget = p->volData->doFill;
    }

    /* PASS 1 - move files based on "filePerf" < "threshholdPerf" */

    if(bf_criteria1_list_start[p->volume]) {  /* something on list */
        err = move_files_fill(
                    bf_criteria1_list_start[p->volume],
                    p->dmnParams,
                    p->volData,
                    1,
                    p->volume);
        /* also updates file performance stats on files */

        /* destroy list 1, it will be recreated during next pass  */
        bfEntry = bf_criteria1_list_start[p->volume];
        while (bfEntry->next) {
            nextBfEntry = bfEntry->next;
            free(bfEntry);
            bfEntry = nextBfEntry;
        }
        if(bfEntry) free(bfEntry);

        bf_criteria1_list_start[p->volume] = NULL;
        bf_criteria1_list_last[p->volume] = NULL;

        if (err) {
            goto reset_vol_ptr;
        }

        if (p->volData->shortenedClearPass) {
            goto reset_vol_ptr;
        }

    }

    /* Falling through here means that all of
     * the files have been visited without filling up all of the available
     * space in the cleared areas.
     */

    if (bf_criteria2_list_start[p->volume] == NULL) {
        goto quit_defrag;
    }

    /* now remove noFillMove==TRUE or fileMovePayoff==-1 from list of files */
    bf_clean_list( bf_criteria2_list_start[p->volume],
                   bf_criteria2_list_last[p->volume],
                   p->volume,
                   2
                 );

    /* PASS 2 - if still room in clear area get files based on "worst" */
    if(bf_criteria2_list_start[p->volume]) {  /* something on list */
        err = move_files_fill(
                    bf_criteria2_list_start[p->volume],
                    p->dmnParams,
                    p->volData,
                    2,
                    p->volume);
        /* destroy list 1, it will be recreated during next pass  */
        bfEntry = bf_criteria2_list_start[p->volume];
        while (bfEntry->next) {
            nextBfEntry = bfEntry->next;
            free(bfEntry);
            bfEntry = nextBfEntry;
        }
        if(bfEntry) free(bfEntry);

        bf_criteria2_list_start[p->volume] = NULL;
        bf_criteria2_list_last[p->volume] = NULL;

        if (err) {
            goto reset_vol_ptr;
        }
            
        if (p->volData->shortenedClearPass) {
            goto reset_vol_ptr;
        }
    }

quit_defrag:;

    /*
     * stop processing if improvement over last pass was not good enough
     */

    ret = stop_pass_check(p->volData);

    if (ret==VOLDONE) {

reset_vol_ptr:;

        /* We are done with this volume forever */
        pthread_mutex_lock(&volPassStat_mutex );
        p->volData->passNumber++;
        volPassStat[p->volume].volDone = TRUE;
        save_vols_end_stats(p->volData, p->volFreeSpaceData, p->volume);
        pthread_mutex_unlock(&volPassStat_mutex );

        if(err) {
            fprintf(stderr, MSG(DF_PASSERR,
                  "%s: Error occurred during pass %d on volume %d.   Continuing...\n"),
                   Prog, p->volData->passNumber-1, p->volume+1);
            (void)fflush(stdout);
            if(err == NOT_ENOUGH_SPACE) {
                program_exit_status = 1;
                /* 
                 * don't reset allocation pointer below since error on
                 * this volume is NOT_ENOUGH_SPACE(this error will possibly not
                 * allow the following reset routines to run correctly), 
                 * just return to pool and wait for other threads on other 
                 * volumes to finish
                 */
                return ;
            }
        }

        /*
         * Set up each volume now so that the next allocation will
         * begin at the start of the largest free area.
         */
        err_find = find_sparsest_area(
                            p->volData->bsed_vol_index,
                            p->volData->volSize,
                            p->volData->volStgCluster,
                            TRUE,
                            0,
                            0,
                            clearLargestHole,
                            p->dmnParams,
                            &p->volData->clearAreaSize,
                            &p->volData->areaPopulation,
                            &p->volData->lowerBlkClearBound,
                            &p->volData->alreadyClear,
                            NULL,
                            p->volData);
        if (err_find)
            abort_prog(MSG(DF_PASSERR,
                  "%s: Error occurred during pass %d on volume %d.   Continuing...\n"),
                   Prog, p->volData->passNumber-1, p->volume+1);

        sts = advfs_reset_free_space_cache(
                        p->dmnParams->bfDomainId,
                        p->volData->bsed_vol_index,
                    p->volData->lowerBlkClearBound / p->volData->volStgCluster);
            if (sts != EOK) {
                abort_prog(MSG(DF_PASSERR,
                      "%s: Error occurred during pass %d on volume %d.   Continuing...\n"),
                       Prog, p->volData->passNumber-1, p->volume+1);
            }

    } else {
        p->volData->passNumber++;
    }

    (void)fflush(stdout);

    if((err) || (err_find)) program_exit_status = 1;

}  /* end defragment_main */


/*
 * move_files_clear
 *
 * move file extents out of the area to clear.  Move them to the area
 * already determined to be sparsest.
 *
 * This function builds a table of all files on this volume.  This 
 * table is used in file migration later in the code.
 *
 * The function also selects files that 
 * meet the criteria for files which may have extents in the zone to be 
 * cleared.  If a file meets the criteria a call is made to 
 * move_normal_file_clear() where a check is made for any extents that
 * fall into the zone that needs to be cleared.  If there are extents to
 * be moved they are also moved in move_normal_file_clear().
 */
static
int
move_files_clear (
               bfDomainIdT domainId,  /* in */
               volDataT *volData,       /* in */
               int volume,              /* in */
               int clearLimitTime,      /* in */
               boolean *reinsertSideFlag
               )
{
    bsBfDescT bfDesc[BF_DESC_MAX];
    bsBfDescT sorted_bfDesc[BF_DESC_MAX];
    int bfDescCnt;
    uint64_t i,j,val,k;
    bfMCIdT nextBfDescId = {0,0,0};
    adv_status_t sts = EOK;
    adv_status_t ret = EOK;
    int err;
    bfListEntryT *thisBfEntry=NULL;
    int fileStartTime;
    int fileEndTime;
    bfListEntryT *newEntry = NULL;
    bfListEntryT *newEntry2 = NULL;
    time_t atime;
    bfSetIdT bfsetid;
    bfSetParamsT setParams;
    boolean skip_it;

    bf_criteria1_list_start[volume] = NULL;
    bf_criteria1_list_last[volume] = NULL;
    bf_criteria2_list_start[volume] = NULL;
    bf_criteria2_list_last[volume] = NULL;


    /* while some files left to process on this volume*/
    while ( 1 ) {

        /*
         * Get the next group(BF_DESC_MAX=500) of bitfile descriptors.
         *
         * This function reads this volumes' bmt and returns an array
         *(BF_DESC_MAX size) of bitfile descriptors (bfSetTag/bfTag pairs) until
         * no more are found on this volume.
         *
         */

        sts = advfs_get_vol_bf_descs (
                                      domainId,
                                      volData->bsed_vol_index,
                                      BF_DESC_MAX,
                                      &(bfDesc[0]),
                                      &nextBfDescId,
                                      &bfDescCnt);
        if (sts != EOK) {
                fprintf (stderr, CMSG(COMMON_ERRBFDESCS,
                         "%s: Cannot get volume file descriptors\n"), Prog);
                fprintf (stderr, CMSG(COMMON_ERR,
                         "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                return 1;
        }

        /* no more found on this volume - exit cleanly */
        if( bfDescCnt <= 0 ) {
                nextBfDescId.volume=0;
                nextBfDescId.page=0;
                nextBfDescId.cell=0;
                break;
        }

        /* found some */

        /* 
         * Reorder the tags so that the PRIME metadata types come befor the 
         * XTNTS types.  The prime types are garanteed to be unique for all
         * tags in the domain.  This will ensure that the PRIME metatypes are 
         * used for the statistics gathering BEFORE the XTNT types are processed
         * as part of a clear move.  If we didn't do this then the stats come 
         * out funny because the PRIME types are processed after the XTNT 
         * ones and the XTNT ones had already caused a move to happen if the 
         * tag had extents in the clear zone.  In other words we would have 
         * moved the files extents before we counted the old extents.
         */
        /* first copy the PRIME metadata types into the sorted array */
        for(i=0,j=0; i < bfDescCnt; i++) {
            if(bfDesc[i].metadataType == BMT_PRIME_MCELL_XTNT_METADATA) {
                sorted_bfDesc[j].bfSetTag.tag_num = bfDesc[i].bfSetTag.tag_num;
                sorted_bfDesc[j].bfSetTag.tag_seq = bfDesc[i].bfSetTag.tag_seq;
                sorted_bfDesc[j].bfTag.tag_num = bfDesc[i].bfTag.tag_num;
                sorted_bfDesc[j].bfTag.tag_seq = bfDesc[i].bfTag.tag_seq;
                sorted_bfDesc[j].metadataType = bfDesc[i].metadataType;
                j++;
            }
        }
        /* now copy the XTNTS metadata types into the sorted array */
        for(i=0, k=0, skip_it=FALSE; i < bfDescCnt; i++, skip_it=FALSE) {
            /*
             * defragment only migrates regular xtnt metadata, not
             * reserved files as specified in the NORMAL types.
             */
            if(bfDesc[i].metadataType == BMT_XTNT_METADATA) {
                for(k=0; k<j; k++) {
                    /* see if its in the list already */
                    /* if it is then skip it or we would process it multiple times */
                    if ( (sorted_bfDesc[k].bfSetTag.tag_num == bfDesc[i].bfSetTag.tag_num) &&
                         (sorted_bfDesc[k].bfSetTag.tag_seq == bfDesc[i].bfSetTag.tag_seq)  &&
                         (sorted_bfDesc[k].bfTag.tag_num == bfDesc[i].bfTag.tag_num)        &&
                         (sorted_bfDesc[k].bfTag.tag_seq == bfDesc[i].bfTag.tag_seq)        &&
                         (sorted_bfDesc[k].metadataType == BMT_XTNT_METADATA) ) {
                        skip_it=TRUE; /* skip this one */
                    }
                }
                if(!skip_it) {
                    sorted_bfDesc[j].bfSetTag.tag_num = bfDesc[i].bfSetTag.tag_num;
                    sorted_bfDesc[j].bfSetTag.tag_seq = bfDesc[i].bfSetTag.tag_seq;
                    sorted_bfDesc[j].bfTag.tag_num = bfDesc[i].bfTag.tag_num;
                    sorted_bfDesc[j].bfTag.tag_seq = bfDesc[i].bfTag.tag_seq;
                    sorted_bfDesc[j].metadataType = bfDesc[i].metadataType;
                    j++;
                }
            }
        }

        /* process the list of file tags */
        for(i=0;  i < j; i++) {

            if (BS_BFTAG_RSVD (sorted_bfDesc[i].bfSetTag)) {
                /*
                 * This is a bitfile set.  Skip it.
                 */
                continue;
            }

            if (BS_BFTAG_RSVD (sorted_bfDesc[i].bfTag)) continue;

            newEntry = (bfListEntryT *)malloc(sizeof(bfListEntryT));
            if (!newEntry) {
                return FALSE;
            }

            newEntry->bfSetTag.tag_num = sorted_bfDesc[i].bfSetTag.tag_num;
            newEntry->bfSetTag.tag_seq = sorted_bfDesc[i].bfSetTag.tag_seq;
            newEntry->bfTag.tag_num = sorted_bfDesc[i].bfTag.tag_num;
            newEntry->bfTag.tag_seq = sorted_bfDesc[i].bfTag.tag_seq;
            newEntry->noFillMove = FALSE;
            newEntry->hasValidData = FALSE;
            newEntry->fileMovePayoff = -1.0;
            newEntry->next = NULL;
            newEntry->prior = NULL;
            newEntry->balance = 0;
            newEntry->singleVolumeIndex = 0;
            newEntry->filePerf =0;
            newEntry->fileMovePayoff =0;
            newEntry->filesize =0;
            newEntry->fileExtentCount =0;
            newEntry->wasCleared = FALSE;
            newEntry->isSparse =0;

            /*
             * If the source volume doesn't need clearing (i.e., it
             * is either not to be filled or is already clear), there
             * is no need to move this file.
             * We *do* need to process the volume's entries in this loop
             * anyway, so that they can be entered into the bfTable so
             * that the fill phase can find them.
             */
            if (!volData->doFill || volData->alreadyClear) {
                free(newEntry);
                continue;
            }

            /* 
             * check to see if file extents need moved, if they do, move them
             */
            err = chk_move_file (volData,
                                 &sorted_bfDesc[i],
                                 newEntry,
                                 &fileStartTime,
                                 clearLimitTime);
            if (err == ERROR) {
                free(newEntry);
                return 1;
            } else if (err == CONTINUE)  {
                free(newEntry);
                continue;
            }

           if (newEntry->noFillMove == FALSE) {
                /* and file is eligible to be moved */

                 val = copy_newEntry(newEntry,&newEntry2);
                 if(val == FALSE) {  /* unable to add  because of no memory */
                     free(newEntry);
                     return 1;
                 }
                 val = insert_bf_entry_criteria1(  
                                      newEntry, /* list entry to add to list */
                                      volume,
                                      (uint64_t) volData->clearAreaSize + 
                                            (volData->clearAreaSize * .10),
                                          /* add 10% to be absolutely sure 
                                           * we have enough files to 
                                           * fill the cleared zone 
                                           */
                                      FALSE
                                      ) ;
                 if(val == FALSE) {  
                     /* not added to list because it is not bad enough 
                      * to fit on the list of bad tags 
                      */
                     free(newEntry);
                 }

                 /* try the other list */
                 val = insert_bf_entry_criteria2(  
                                  newEntry2, /* list entry to add to list */
                                  volume,
                                  (uint64_t) volData->clearAreaSize + 
                                        (volData->clearAreaSize * .10),
                                      /* added 10% to be absolutely sure 
                                       * we have enough files to 
                                       * fill the cleared zone 
                                       */
                                  FALSE
                                  ) ;
                 if(val == FALSE) {  
                     /* 
                      * not added to list because it is not bad enough 
                      * to fit on the list of bad tags 
                      */
                     free(newEntry2);
                 }
            } else {   
                /* 
                 * file has been determined to not be a candidate for any of a
                 * number of reasons. 
                 */
                free(newEntry);
            }
        
            /* If there is a strict time limit, see if there is enough time
             * for another file to be moved (assuming it takes the same time
             * as this one did); if not, exit. */
             if (hasStrictTimeLimit) {
                err = get_system_time(&fileEndTime);
                if (err) {
                    return 1;
                }
                if(fileEndTime + (fileEndTime-fileStartTime) > clearLimitTime) {
                    volData->shortenedClearPass = TRUE;
                    return 0;
                }
             }

        }
    } 

return 0;

}  /* end move_files_clear */


/*
 * chk_move_file
 *
 * check file for correct type and call move_normal_file_clear()
 * to see if file has extents that are in zone to be cleared.
 *
 * return BREAK, ABORT, CONTINUE, or SUCCESS
 */
static
int
chk_move_file (
               volDataT *volData,          /* in */
               bsBfDescT *bfDesc,          /* in */
               bfListEntryT *retBfEntry, /* in/out */
               int *fileStartTime,         /* out */
               int clearLimitTime
               )
{
    setInfoT *setInfo;
    char dotTagsPathName[MAXPATHLEN+1];
    struct stat stats;
    int err;
    int fd;
    int unknown;
    adv_status_t sts;
    adv_bf_attr_t bfAttr;
    adv_bf_info_t bfInfo;


        /*
         * This is a normal bitfile.
         */
        setInfo = find_setTag (bfDesc->bfSetTag);

        if (setInfo == NULL) {
            fprintf (stderr, MSG(DF_NOHDR1,
                     "%s: Cannot find file set hdr - tag %d.%d - volume %d\n"),
                     Prog, bfDesc->bfSetTag.tag_num, bfDesc->bfSetTag.tag_seq,volData->bsed_vol_index);
            if (ignoreErrorsFlag) {
                retBfEntry->noFillMove = TRUE;
                return CONTINUE;
            } else {
                return ERROR;
            }
        }

        sprintf (
                 dotTagsPathName,
                 "%s/0x%x.0x%x",
                 setInfo->dotTagsPath,
                 bfDesc->bfTag.tag_num,
                 bfDesc->bfTag.tag_seq
                 );

        err = lstat (dotTagsPathName, &stats);
        if (err) {
            if (errno == ENOENT) {
                    /* The file was deleted. */
                    retBfEntry->noFillMove = TRUE;
                    return CONTINUE;
            }
            fprintf (stderr, CMSG(COMMON_STATS2, 
                     "%s: Error getting '%s' stats\n"),
                     Prog, dotTagsPathName);
            fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRMSG (errno));
            if (ignoreErrorsFlag) {
                retBfEntry->noFillMove = TRUE;
                return CONTINUE;
            } else {
                return ERROR;
            }
        }

        if (!S_ISDIR(stats.st_mode) && !S_ISREG(stats.st_mode)) {
            retBfEntry->noFillMove = TRUE;
            return CONTINUE;
        }

        /* If there is a strict time limit, check the time now; if we're
         * over, then exit, otherwise keep track of it so that we'll know
         * how long it took to move this file. */
        if (hasStrictTimeLimit) {
            err = get_system_time(fileStartTime);
            if (err) {
                return ERROR;
            }
            if (*fileStartTime > clearLimitTime) {
                volData->shortenedClearPass = TRUE;
                return BREAK;
            }
        }

        retBfEntry->filesize = stats.st_size;

        /*
         * open the file 
         */
        fd = open(dotTagsPathName, O_RDONLY, 0);
        if (fd < 0) {
            if (errno == ENOENT) {
                /*
                 * The file was deleted.
                 */
                retBfEntry->noFillMove = TRUE;
                return CONTINUE;
            } else {
                fprintf (stderr, CMSG(COMMON_NOOPEN2, 
                         "%s: open of %s failed.\n"),
                         Prog, get_tag_path(NULL, bfDesc->bfTag, bfDesc->bfSetTag, &unknown));
                fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRMSG (errno));
                if (ignoreErrorsFlag) {
                    retBfEntry->noFillMove = TRUE;
                    return CONTINUE;
                } else {
                    return ERROR;
                }
            }
        }

        /*
         * Get extent metadata (extents).
         */
        sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);  
        if (sts != EOK) {
            fprintf (stderr, CMSG(COMMON_ERRBFPARAMS, 
                     "%s: Cannot get file %s parameters\n"),
                     Prog, get_tag_path(NULL, bfDesc->bfTag, bfDesc->bfSetTag, &unknown));
            fprintf (stderr, CMSG(COMMON_ERR, 
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            (void) close (fd);
            if (ignoreErrorsFlag) {
                retBfEntry->noFillMove = TRUE;
                return CONTINUE;
            } else {
                return ERROR;
            }
        }
        if (bfInfo.mbfNumFobs == 0) {
            retBfEntry->noFillMove = TRUE;
            sts = EOK;
        } else {

            /*
            * Move the extents clear of zone IF any extents in zone.
            */
            sts = move_normal_file_clear (
                                        fd,
                                        bfDesc->bfTag,
                                        bfDesc->metadataType,
                                        bfInfo.pageSize,
                                        volData,
                                        retBfEntry,
                                        clearLimitTime,
                                        &bfAttr
                                        );
        }
                    
        if (sts != EOK) {
            (void) close (fd);
            if (ignoreErrorsFlag) {
                return CONTINUE;
            } else {
                fprintf (stderr, CMSG(COMMON_CANTMOVE, 
                     "%s: Cannot move file %s\n"),
                     Prog, get_tag_path(NULL, bfDesc->bfTag, bfDesc->bfSetTag, &unknown));
                fprintf (stderr, CMSG(COMMON_ERR, 
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                return ERROR;
            }
        }

        /* close the file */

        if (close (fd)) {
            fprintf( stderr, CMSG(COMMON_NOCLOSE2, "%s: close of %s failed.\n"),
                     Prog, dotTagsPathName);
            fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                     Prog, errno, ERRMSG (errno));
            if (ignoreErrorsFlag) {
                return CONTINUE;
            } else {
                return ERROR;
            }
        }


return(0);

} /* end chk_move_file */


/*
 * move_files_fill
 *
 * moves file extents back into the area previously cleared.
 *
 * This subroutine is called twice for each major PASS.  First pass, to move the
 * files based on the most desirable defragmenting gains, "perf".  Second pass,
 * to  move any remaining files based on "worst" criteria.  Between passes the
 * calling program will resort the files based on these two criterias.
 *
 */
static
int
move_files_fill (
               bfListEntryT *list_start,  /* in */
               adv_bf_dmn_params_t *dmnParams,  /* in */
               volDataT *volData,        /* in */
               int subpassNumber,        /* in */
               int volume                /* in */
               )
{
    adv_bf_attr_t bfAttr;
    adv_bf_info_t bfInfo;
    char dotTagsPathName[MAXPATHLEN+1];
    int fd;
    setInfoT *setInfo = NULL;
    adv_status_t sts = EOK;
    int unknown;
    int err;
    bfListEntryT *bfEntry = NULL;
    bfListEntryT *nextBfEntry;
    struct stat stats;
    boolean doMove;
    boolean selectFillSuccess = TRUE;
    int fileStartTime;
    int fileEndTime;
    int moveFillCount = 0;	
                                /* This is used only as a guide for how often
                                   to update the volume data; it actually
                                   erroneously counts some files as moved that
                                   are not. */



    for (bfEntry = list_start; bfEntry != NULL; bfEntry = bfEntry->next) {
        /*
         * This is a normal bitfile.
         */

        /* This function finds a bitfile's set tag in the set table. */
        setInfo = find_setTag (bfEntry->bfSetTag);
        if (setInfo == NULL) {
            fprintf (stderr, MSG(DF_NOSETHDR, 
                     "%s: move_files_fill()::Cannot find file set hdr - tag %d.%d - volume %d\n"),
                     Prog, bfEntry->bfSetTag.tag_num, bfEntry->bfSetTag.tag_seq,volData->bsed_vol_index);
            fflush(stdout);
            if (ignoreErrorsFlag) {
                continue;
            } else {
                return 1;
            }
        }

        /* set up file pathname */
        sprintf (
                 dotTagsPathName,
                 "%s/0x%x.0x%x",
                 setInfo->dotTagsPath,
                 bfEntry->bfTag.tag_num,
                 bfEntry->bfTag.tag_seq
                 );

        /* If there is a strict time limit, check the time now; if we're
         * over, then exit, otherwise keep track of it so that we'll know
         * how long it took to move this file. */
        if (hasStrictTimeLimit) {
            err = get_system_time(&fileStartTime);
            if (err) {
                return 1;
            }
            if ((fileStartTime > limitTime)) {
                break;  /* leave now */
            }
        }
            /* also updates file performance stats on files */

        /* open the file */
        fd = open(dotTagsPathName, O_RDONLY, 0);
        if (fd < 0) {
            if (errno == ENOENT) {
                /*
                 * The file was deleted.
                 */
                continue;
            } else {
                fprintf (stderr, CMSG(COMMON_NOOPEN2, 
                         "%s: open of %s failed.\n"),
                         Prog, get_tag_path(NULL, bfEntry->bfTag, bfEntry->bfSetTag, &unknown));
                fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                         Prog, errno, ERRMSG (errno));
                if (ignoreErrorsFlag) {
                    continue;
                } else {
                    return 1;
                }
            }
        }

        /*
         * Get extent metadata (extents).
         */
        sts = advfs_get_bf_params (fd, &bfAttr, &bfInfo);  
        if (sts != EOK) {
            fprintf (stderr, CMSG(COMMON_ERRBFPARAMS,   
                     "%s: Cannot get file %s parameters\n"),
                     Prog, get_tag_path(NULL, bfEntry->bfTag, bfEntry->bfSetTag, &unknown));
            fprintf (stderr, CMSG(COMMON_ERR, 
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            (void) close(fd);
            if (ignoreErrorsFlag) {
                continue;
            } else {
                return 1;
            }
        }

        /*
         *  Target volume must be the same as the source. - No balancing!
         */

        /* update volume data and adjust the remaining clear area size */
        if (moveFillCount == 20) {
            if (update_volume_data(dmnParams, volData)) {
                return 1;
            }
            if (volData->remainingClearAreaSize > volData->volFreeSize) {
                    volData->remainingClearAreaSize = 
                           volData->volFreeSize * CLEAR_SIZE_FRACTION;
            }
            moveFillCount = 0;
        }

        if(((subpassNumber==1) && 
            (bfEntry->filePerf < volData->threshholdPerf)) || 
                                                (subpassNumber == 2)) {

            sts = move_normal_file_fill(
                    fd,
                    bfEntry->bfTag,
                    bfEntry->bfSetTag,
                    volData->bsed_vol_index,
                    &bfInfo,
                    bfEntry->fileExtentCount,
                    &volData->remainingClearAreaSize,
                    volume,
                    bfEntry->singleVolumeIndex,
                    &bfAttr);
        } else {
            sts = EOK;
            break;
        }

        if (sts == EOK) {
            if(subpassNumber == 1) {
            /* if this entry exists in the list for subpassnumber == 2 
                * then delete it since we don't want to move it again!
                */
                delete_bf_entry(
                        volume,
                        bfEntry,/* linked list entry to delete in list 2 */
                        2
                                );
            }
            moveFillCount++;
        }

        if (sts != EOK) {
            if (sts == ENO_MORE_BLKS) {
                /* Having insufficient space to migrate here means that the
                 * remaining clear area on that volume has been consumed
                 * by someone else. */
                volData->isEligibleTarget = FALSE;
            } else {
                (void) close (fd);
                if (ignoreErrorsFlag) {
                    continue;
                } else {
                    fprintf (stderr, CMSG(COMMON_CANTMOVE, 
                         "%s: Cannot move file %s\n"),
                         Prog, get_tag_path(NULL, bfEntry->bfTag, bfEntry->bfSetTag, &unknown));
                    fprintf (stderr, CMSG(COMMON_ERR, 
                         "%s: Error = %s\n"), Prog, BSERRMSG (sts));
                    return 1;
                }
            }
        }

        /* Done for now - close the file */

        if (close (fd)) {
            fprintf( stderr, CMSG(COMMON_NOCLOSE2, "%s: close of %s failed.\n"),
                    Prog, dotTagsPathName);
            fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                     Prog, errno, ERRMSG (errno));
            if (ignoreErrorsFlag) {
                continue;
            } else {
                return 1;
            }
        }

        /* If there is a strict time limit, see if there is enough time
         * for another file to be moved (assuming it takes
         * slightly longer than this one did for filling,
         * because the filling limit must be stricter than the clearing
         * limit); if not, exit. 
         */
        if (hasStrictTimeLimit) {
            err = get_system_time(&fileEndTime);
            if (err) {
                return 1;
            }
            if (fileEndTime + 1.1 * (fileEndTime-fileStartTime) > limitTime) {
                break;     /* leave now */
            }
        }

        /* See if the current target volume just got its clear area
           filled up, and is thus no longer eligible for filling.
         */

        if (volData->remainingClearAreaSize <= 0) {
            volData->isEligibleTarget = FALSE;
        }

        if(!volData->isEligibleTarget) {
            break;
        }

    }  /* end for */

    return 0;

}  /* end move_files_fill */


static
void
bf_clean_list( bfListEntryT *list_start,  /* in/out */
               bfListEntryT *list_last,   /* in/out */
               int volume,                /* in */
               int stage                  /* in */
             )
{
    bfListEntryT *p,*previous, *next;

    p=list_start;
    while(p) {
       if( ( (p->noFillMove == TRUE)&&(stage==1) ) || 
           ( ( (p->noFillMove == TRUE) || (p->fileMovePayoff == -1) ) && 
             (stage==2) ) )  {
           if(p->next == 0 && p->prior == 0) {
               /* no other entries other than this one */
               free(p);
               list_last = NULL;
               list_start = NULL;
               return;
           }
           if(p->next == 0) {
               /* new last entry in list */
               previous = p->prior;
               previous->next = 0;
               list_last = previous;
               free(p);
               return;
           }   
           if(p->prior == 0) {
               /* new first entry in list */
               next = p->next;
               next->prior = 0;
               list_start = next;
               free(p);
               p = next;
               continue;
           }
           /* remove from middle */
           next = p->next;
           previous = p->prior;
           next->prior = previous;
           previous->next = next;
           free(p);
           p = next; 
           continue;
       }
       p = p->next;
    }

}


/* this function is for debugging the linked lists.  keep it for future */
static void
bear_list(
          bfListEntryT *list_start,
          int volume)
{
 bfListEntryT *p,*old;
 int i;

    fprintf(stderr,"START PRINTOUT\n");

    p=list_start;

    for(i=0; p != NULL;i++) {
        if(p->bfTag.tag_num != 0) {
            fprintf(stderr,"vol=%d, i=%d, num=%d.%x, fileMovePayoff=%f, \
fileExtentCount=%d, filePerf=%f, noFillMove=%d, size(in pages)=%d\n",
                volume,i,p->bfTag.tag_num,p->bfTag.tag_seq,
                p->fileMovePayoff, p->fileExtentCount,
                p->filePerf,p->noFillMove,p->filesize/8192);
            old = p;
            p = p->next;
        } else {
            fprintf(stderr,"vol=%d::Entry i=%d: p->bfTag.tag_num is 0 --> exiting\n",i,volume);
            exit(0);
        }
    }


    if(bf_criteria1_list_start[volume] != NULL) 
        fprintf(stderr,"criteria1_list_start tag = %d.0x%x\n",
                    bf_criteria1_list_start[volume]->bfTag.tag_num,
                    bf_criteria1_list_start[volume]->bfTag.tag_num);
    else
        fprintf(stderr,"criteria1_list_start  == NULL\n");

    if(bf_criteria1_list_last[volume] != NULL) 
        fprintf(stderr,"criteria1_list_last tag = %d.0x%x\n",
                    bf_criteria1_list_last[volume]->bfTag.tag_num,
                    bf_criteria1_list_last[volume]->bfTag.tag_num);
    else
        fprintf(stderr,"criteria1_list_last  == NULL\n");

    if(bf_criteria2_list_start[volume] != NULL) 
        fprintf(stderr,"criteria2_list_start tag = %d.0x%x\n",
                    bf_criteria2_list_start[volume]->bfTag.tag_num,
                    bf_criteria2_list_start[volume]->bfTag.tag_num);
    else
        fprintf(stderr,"criteria2_list_start  == NULL\n");

    if(bf_criteria2_list_last[volume] != NULL) 
        fprintf(stderr,"criteria2_list_last tag = %d.0x%x\n",
                    bf_criteria2_list_last[volume]->bfTag.tag_num,
                    bf_criteria2_list_last[volume]->bfTag.tag_num);
    else
        fprintf(stderr,"criteria2_list_last  == NULL\n");

    fprintf(stderr,"END PRINTOUT\n");

}




/*
 * copy_newEntry ()
 * copies one structure to another, identical structure.  First one
 * is used in criteria list 1 and the second structure is used in th
 * criteria list 2.  Both lists are used as the list of files to be moved
 * during the fill phase.
 */
static
int
copy_newEntry(bfListEntryT *newEntry,
              bfListEntryT **retnewEntry) 
{

    bfListEntryT *p=NULL;

    if (!newEntry) {
         fprintf (stderr, MSG(DF_INVALBIT,
                 "%s: Invalid bitfile entry\n"), Prog);
         return FALSE;
    }

    p = (bfListEntryT *)malloc(sizeof(bfListEntryT));
    if (!p) {
         fprintf (stderr, MSG(DF_NOMEMLIST, 
                "%s: Cannot allocate memory for linked list of files.\n"), Prog);
         return FALSE;
    }

    p->bfSetTag.tag_num = newEntry->bfSetTag.tag_num;
    p->bfSetTag.tag_seq = newEntry->bfSetTag.tag_seq;
    p->bfTag.tag_num = newEntry->bfTag.tag_num;
    p->bfTag.tag_seq = newEntry->bfTag.tag_seq;
    p->filePerf = newEntry->filePerf;
    p->fileExtentCount = newEntry->fileExtentCount;
    p->fileMovePayoff = newEntry->fileMovePayoff;
    p->noFillMove = newEntry->noFillMove;
    p->hasValidData = newEntry->hasValidData;
    p->balance = newEntry->balance;
    p->wasCleared = newEntry->wasCleared;
    p->isSparse = newEntry->isSparse;
    p->prior = NULL;
    p->next = NULL;
    p->singleVolumeIndex = newEntry->singleVolumeIndex;
    p->filesize = newEntry->filesize;

    *retnewEntry = p;

return TRUE;
}


static
int
delete_bf_entry(
                int volume,
                bfListEntryT *Entry, /* linked list entry to delete */
                int list  /* list 1 or 2 */
               ) 
{
    bfListEntryT *p=NULL;


    if (list==1) {
        p = bf_criteria1_list_start[volume];  /* start at top of list */
        while(p) {
    
           if( (p->bfTag.tag_num == Entry->bfTag.tag_num ) &&
               (p->bfTag.tag_seq == Entry->bfTag.tag_seq ) &&
               (p->bfSetTag.tag_num == Entry->bfSetTag.tag_num ) &&
               (p->bfSetTag.tag_seq == Entry->bfSetTag.tag_seq ) ) {
                if (p==bf_criteria1_list_start[volume]) {
                    /* item found at start of list */
                    if(p->next) {
                        bf_criteria1_list_start[volume] = p->next;
                        p->next->prior = 0;
                    } else {
                        /* only one item on list -delete it*/
                        bf_criteria1_list_start[volume] = NULL;
                        bf_criteria1_list_last[volume] = NULL;
                    }
                } else if(p==bf_criteria1_list_last[volume]) {
                    /* item found on end of list */
                    if(p->prior) {
                        /* there is other entries */
                        bf_criteria1_list_last[volume] = p->prior;
                        p->prior->next = 0;
                    } else {
                        /* only one item on list -delete it*/
                        bf_criteria1_list_start[volume] = NULL;
                        bf_criteria1_list_last[volume] = NULL;
                    }
                } else {
                    /* item found in middle of list */
                    p->prior->next = p->next;
                    p->next->prior = p->prior;
                }

                free(p);
                return TRUE;
            }

            p = p->next;
        }

        return FALSE;  /* item not found in list */

    } else if(list==2){

        p = bf_criteria2_list_start[volume];  /* start at top of list */
        while(p) {
    
           if( (p->bfTag.tag_num == Entry->bfTag.tag_num ) &&
               (p->bfTag.tag_seq == Entry->bfTag.tag_seq ) &&
               (p->bfSetTag.tag_num == Entry->bfSetTag.tag_num ) &&
               (p->bfSetTag.tag_seq == Entry->bfSetTag.tag_seq ) ) {
                if (p==bf_criteria2_list_start[volume]) {
                    /* item found at start of list */
                    if(p->next) {
                        bf_criteria2_list_start[volume] = p->next;
                        p->next->prior = 0;
                    } else {
                        /* only one item on list -delete it*/
                        bf_criteria2_list_start[volume] = NULL;
                        bf_criteria2_list_last[volume] = NULL;
                    }
                } else if(p==bf_criteria2_list_last[volume]) {
                    /* item found on end of list */
                    if(p->prior) {
                        bf_criteria2_list_last[volume] = p->prior;
                        p->prior->next = 0;
                    } else {
                        /* only one item on list -delete it*/
                        bf_criteria2_list_start[volume] = NULL;
                        bf_criteria2_list_last[volume] = NULL;
                    }
                } else {
                    /* item found in middle of list */
                    p->prior->next = p->next;
                    p->next->prior = p->prior;
                }

                free(p);
                return TRUE;
            }

            p = p->next;
        }

    return FALSE;  /* item not found in list */

    }

}


static
int
insert_bf_entry_criteria1(
                     bfListEntryT *newEntry, /* table entry to create */
                     int volume,
                     uint64_t clearSize,
                     int assert_flag   /* flag to perform list checks */
                                         ) 
{

    bfListEntryT *p,*old;
    int insert_anyways=0;
    int64_t TotalFileSizes=0;

    p = bf_criteria1_list_start[volume];  

    /* start at top of list- tally up current size of all files */
    while(p) {
        TotalFileSizes += p->filesize/BLOCK_SIZE;
        p = p->next;
    }

    if(bf_criteria1_list_last[volume]) {
        if(((clearSize < TotalFileSizes) && 
           (newEntry->filePerf > bf_criteria1_list_last[volume]->filePerf)) || 
           (newEntry->filePerf == 0) || (newEntry->filePerf == 100) ) {
           /* 
            * Enough files to fill cleared area in list already, and 
            * newEntry is better file than last on list, so don't add 
            * this one on list
            */
            return FALSE;
        }
    }

    if(bf_criteria1_list_last[volume]==0)  {
        /* first element on list */
        newEntry->next = NULL;
        newEntry->prior = NULL;
        bf_criteria1_list_last[volume] = newEntry;
        bf_criteria1_list_start[volume] = newEntry;
        return TRUE;
    }

    /* first check list for a duplicate
     * if one is found update it by re-inserting it
     */
    p = bf_criteria1_list_start[volume];  /* start at top of list */
    while(p) {

       if( (p->bfTag.tag_num == newEntry->bfTag.tag_num ) && 
           (p->bfTag.tag_seq == newEntry->bfTag.tag_seq ) && 
           (p->bfSetTag.tag_num == newEntry->bfSetTag.tag_num ) &&
           (p->bfSetTag.tag_seq == newEntry->bfSetTag.tag_seq ) ) {

           if(assert_flag == TRUE) {
               abort_prog(MSG(DF_BADLIST1, 
              "%s: internal defragment error: insert_bf_entry_criteria1() list corruption\n (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))\n"),
               Prog,
               newEntry->bfSetTag.tag_num,newEntry->bfSetTag.tag_seq,
               newEntry->bfSetTag.tag_num,newEntry->bfSetTag.tag_seq,
               newEntry->bfTag.tag_num,newEntry->bfTag.tag_seq,
               newEntry->bfTag.tag_num,newEntry->bfTag.tag_seq);
           }
           /* update the values with the newer values
            * by first deleting this entry and then adding
            * it back in.
            */

            /* delete the old entry */
            delete_bf_entry(
                            volume,
                            newEntry,
                            1
                           );

            /* add back in the new one */
            insert_bf_entry_criteria1(
                     newEntry, /* table entry to create */
                     volume,
                     clearSize,
                     TRUE);

            return TRUE;
       } /* end if a match is found */

       p = p->next;

    } /* end while list entries */


    /* Its not already in the list so 
     * Now see where this newEntry belongs in list - if it does
     */
    p = bf_criteria1_list_start[volume];  /* start at top of list */
    old = 0;
    while(p) {
        if(newEntry->filePerf > p->filePerf) {
            old = p;            /* get next on list */
            p = p->next;
        } else {
            if(p->prior) {          /* put in middle of list */
                p->prior->next = newEntry;
                newEntry->next = p;
                newEntry->prior = p->prior;
                p->prior = newEntry;
                if(clearSize < TotalFileSizes) {
                    /* 
                     * let one fall off the bottom , since we are at max we 
                     * want to keep and we added one 
                     */
                    delete_bf_entry(
                            volume,
                            bf_criteria1_list_last[volume],
                            1
                                   );
                }
                return TRUE;
            }
            newEntry->next = p; /* new first element on list */
            newEntry->prior = 0;
            p->prior = newEntry;
            bf_criteria1_list_start[volume] = newEntry;
            if(clearSize < TotalFileSizes) {
                /* 
                 * let one fall off the bottom , since we are at max we 
                 * want to keep and we added one 
                 */
                    delete_bf_entry(
                            volume,
                            bf_criteria1_list_last[volume],
                            1
                                   );
            }
            return TRUE;
        }
    }
    
    old->next=newEntry;  /* put on end of list */
    newEntry->next = 0;
    newEntry->prior = old;
    bf_criteria1_list_last[volume] = newEntry;
    return TRUE;

} /* end of insert_bf_entry_criteria1 */


static
int
insert_bf_entry_criteria2(
                     bfListEntryT *newEntry, /* table entry to create */
                     int volume,
                     uint64_t clearSize,
                     int assert_flag   /* flag to perform list checks */
                                         ) 
{

    bfListEntryT *p = NULL;
    bfListEntryT *old = NULL;
    int64_t TotalFileSizes=0;

    p = bf_criteria2_list_start[volume];

    /* start at top of list- tally up current size of all files */
    while(p) {
        TotalFileSizes += p->filesize/BLOCK_SIZE;
        p = p->next;
    }

    if(bf_criteria2_list_last[volume]) {
        if( (clearSize < TotalFileSizes) && 
            compare_insert(bf_criteria2_list_last[volume],newEntry))  {
            /* 
             * Enough files to fill cleared area and newEntry is better 
             * file than last on list, so don't add this one on list
             */

            return FALSE;
        }
    }

    if(bf_criteria2_list_last[volume]==0) {
        /* only element on list */
        newEntry->next = NULL;
        newEntry->prior = NULL;
        bf_criteria2_list_last[volume] = newEntry;
        bf_criteria2_list_start[volume] = newEntry;
        return TRUE;
    }

    /* first check list for a duplicate
     * if one is found update it by re-inserting it
     */
    p = bf_criteria2_list_start[volume];  /* start at top of list */
    while(p) {

       if( (p->bfTag.tag_num == newEntry->bfTag.tag_num ) && 
           (p->bfTag.tag_seq == newEntry->bfTag.tag_seq ) && 
           (p->bfSetTag.tag_num == newEntry->bfSetTag.tag_num ) &&
           (p->bfSetTag.tag_seq == newEntry->bfSetTag.tag_seq ) ) {

           if(assert_flag == TRUE) {
               abort_prog(MSG(DF_BADLIST2, 
               "%s: internal defragment error: insert_bf_entry_criteria2() list corruption\n (setTag: %d.%d (0x%x.0x%x), tag: %d.%d (0x%x.0x%x))\n"),
               Prog,
               newEntry->bfSetTag.tag_num,newEntry->bfSetTag.tag_seq,
               newEntry->bfSetTag.tag_num,newEntry->bfSetTag.tag_seq,
               newEntry->bfTag.tag_num,newEntry->bfTag.tag_seq,
               newEntry->bfTag.tag_num,newEntry->bfTag.tag_seq);
           }
           /* update the values with the newer values
            * by first deleting this entry and then adding
            * it back in.
            */

            /* delete the old entry */
            delete_bf_entry(
                            volume,
                            newEntry,
                            2
                           );

            /* add back in the new one */
            insert_bf_entry_criteria2(
                     newEntry, /* table entry to create */
                     volume,
                     clearSize,
                     TRUE);

            return TRUE;
       } /* end if a match is found */

       p = p->next;

    } /* end while list entries */

    p = bf_criteria2_list_start[volume];  /* start at top of list */
    old = 0;
    while(p) {

        if(compare_insert(p,newEntry)) {
            old = p;            /* get next on list */
            p = p->next;
        } else {
            if(p->prior) {          /* put in middle of list */
                p->prior->next = newEntry;
                newEntry->next = p;
                newEntry->prior = p->prior;
                p->prior = newEntry;
                if(clearSize < TotalFileSizes) {
                    /* 
                     * let one fall off the bottom , since we are at max we 
                     * want to keep and we added one 
                     */
                    delete_bf_entry(
                            volume,
                            bf_criteria2_list_last[volume],
                            2
                                   );
                }
                return TRUE;
            }
            newEntry->next = p; /* new first element on list */
            newEntry->prior = 0;
            p->prior = newEntry;
            bf_criteria2_list_start[volume] = newEntry;
            if(clearSize < TotalFileSizes) {
                /* 
                 * let one fall off the bottom , since we are at max we 
                 * want to keep and we added one 
                 */
                    delete_bf_entry(
                            volume,
                            bf_criteria2_list_last[volume],
                            2
                                   );
            }
            return TRUE;
        }
    }
    
    old->next=newEntry;  /* put on end of list */
    newEntry->next = 0;
    newEntry->prior = old;
    bf_criteria2_list_last[volume] = newEntry;

    return TRUE;

}



static
boolean
compare_insert(
            bfListEntryT *aEntry,
            bfListEntryT *bEntry)
{
    return (

            aEntry->fileMovePayoff > bEntry->fileMovePayoff
            ||
            (
             aEntry->fileMovePayoff == bEntry->fileMovePayoff && 
             aEntry->fileExtentCount > bEntry->fileExtentCount
            ) 
            ||
            (
             aEntry->fileMovePayoff == bEntry->fileMovePayoff && 
             aEntry->fileExtentCount == bEntry->fileExtentCount && 
             aEntry->filePerf <= bEntry->filePerf 
            )
           );
} /* end compare_insert */


/*
 * move_normal_file_clear
 *
 * This function checks a file for extents in the zone to be cleared.  
 * If an extent falls into the zone, it moves the normal file's extents 
 * off of the specified volume to a new location on the volume.  The 
 * location is dependant on where the pointer was set to in 
 * advfs_reset_free_space_cache(). 
 *
 * First, it creates a list of all the file's extents that are located in
 * the zone to be cleared.  Next, it moves each of the file's extent(s)
 * out of the zone.
 * 
 */

static
adv_status_t
move_normal_file_clear (
                      int fd,  /* in */
                      bfTagT bfTag,  /* in */
                      metadataTypeT metadataType,  /* in */
                      int pageSize,  /* in */
                      volDataT *volData,  /* in */
                      bfListEntryT *fileEntry, /* in/out */
                      int clearLimitTime,
                      adv_bf_attr_t *bfAttr 
                      )

{

    int i;
    adv_status_t sts = EOK;
    xtntDescT *xtntDesc = NULL;
    int xtntDescCnt=0;
    int xtntDescMaxCnt=0;
    int checkTime;
    uint64_t fobOffset=0;
    uint64_t pgsMigratedsofar=0;
    uint64_t pgstoMigrate=0;
    uint64_t remainderPgs=0;

    /* create a list of the extents that are in the zone to be cleared */
    sts = create_xtnt_desc_list (
                                 fd,
                                 1,
                                 volData,
                                 pageSize,
                                 fileEntry,
                                 &xtntDescCnt,
                                 &xtntDesc,
                                 bfAttr,
                                 metadataType,
                                 1);

    if (volData->stopDefragment) {
        if(sts == EOK) {
            if (xtntDesc != NULL) {
                free (xtntDesc);
            }
        }
        return EOK;
    }

    if (sts != EOK) {
        fprintf (stderr, CMSG(COMMON_ERREXTLIST,
                 "%s: Cannot create extent desc list\n"), Prog);
        fprintf (stderr, CMSG(COMMON_ERR, 
                 "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }

    /* migrate any extents of this file that are found in the area to be 
     * cleared into a new area.
     */
    for (i = 0; i < xtntDescCnt; i++) {

        if (xtntDesc[i].fobCnt > 0) {

            /* migrate the extent out of zone to another place on same disk  */
            fobOffset          = xtntDesc[i].fobOffset;
            pgsMigratedsofar    = 0;
            pgstoMigrate        = 0;

            while(pgsMigratedsofar < xtntDesc[i].fobCnt) {
            if ((xtntDesc[i].fobCnt*pageSize) > MAX_MIGRATE_FILL_SIZE_INBLKS) {
                /* extent is greater than MAX_MIGRATE_FILL_SIZE_INBLKS 
                 * we need to do more than one migrate of size 
                 * MAX_MIGRATE_FILL_SIZE_INBLKS 
                 */

                fobOffset = fobOffset +  pgstoMigrate;
                remainderPgs = xtntDesc[i].fobCnt - pgsMigratedsofar;

                if((remainderPgs*pageSize) > MAX_MIGRATE_FILL_SIZE_INBLKS)
                    pgstoMigrate = (MAX_MIGRATE_FILL_SIZE_INBLKS / pageSize);
                else
                    pgstoMigrate = remainderPgs;

                pgsMigratedsofar += pgstoMigrate;

            } else {
                /* extent is less than MAX_MIGRATE_FILL_SIZE_INBLKS */
                fobOffset       = xtntDesc[i].fobOffset;
                pgstoMigrate     = xtntDesc[i].fobCnt;  /* migrate extent as is */
                pgsMigratedsofar = xtntDesc[i].fobCnt;
                }

                sts = migrate (
                               fd,
                               xtntDesc[i].bsed_vol_index,  /* source */
                               fobOffset,
                               pgstoMigrate,
                               xtntDesc[i].bsed_vol_index,  /* destination */
                               fileEntry->bfTag,
                               fileEntry->bfSetTag
                               );

                if (sts == EOK) {
                    /* keep stats */
                    volData->actualClearedAreaSize += (pageSize * pgstoMigrate);
                } else if (sts == ENO_MORE_BLKS) {
                    /* There isn't enough free space on this volume to do this
                     * migration.  Since we're only migrating small pieces at
                     * a time in the clear phase, this means this volume has gotten
                     * too full to be worth trying to do a clear/fill on it. */
                    volData->doFill = FALSE;
                    sts = EOK;
                } else {
                    /* someone else is migrating?? - catch it next time */
                    if ((sts == E_MIGRATE_IN_PROGRESS) || (sts == E_NO_SNAPSHOT_STG) ||
                        (sts == E_RANGE_NOT_MAPPED) || (sts == E_CANT_MIGRATE_HOLE)||
                        (sts == E_INVOLUNTARY_ABORT)) {
                        sts = EOK;
                    }
                    goto _error;
                }
            } /* end while migrating */
            fileEntry->wasCleared = TRUE;
        } else {
            /* 
             * TODO: remove this once we are sure we aren't hiting this
             * location 
             */
            printf("BAD: we should not be here. Tru64 V3 domains only\n");
            fileEntry->wasCleared = TRUE;
        }

        if (hasStrictTimeLimit) {
            (void)get_system_time(&checkTime);
            if (checkTime > clearLimitTime) {
                break;
            }
        }

    }  /* end for  */


    if(fileEntry->wasCleared == TRUE) {

    /* 
     * fetch the stats again since the file was cleared(moved), this way  
     * we can migrate it back if its really fragmented due to the clearing. 
     */

        /* free the ones just used */
        if (xtntDesc != NULL) {
            free (xtntDesc);
        }

        /* get a new list and the perforamnce stats */
        sts = create_xtnt_desc_list (
                                 fd,
                                 1,
                                 volData,
                                 pageSize,
                                 fileEntry,
                                 &xtntDescCnt,
                                 &xtntDesc,
                                 bfAttr,
                                 metadataType,
                                 2);

        if(sts == EOK) {
          /* we don't need these - shouldn't be any anyways since we just moved 
             the extents out of cleared zone */
            if (xtntDesc != NULL) {
                free (xtntDesc);
            }
        }
        if (volData->stopDefragment) {
            return EOK;
        }

        if (sts != EOK) {
            fprintf (stderr, CMSG(COMMON_ERREXTLIST, 
                     "%s: Cannot create extent desc list\n"), Prog);
            fprintf (stderr, CMSG(COMMON_ERR, 
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            goto _error;
        }
    } else {
        if (xtntDesc != NULL) {
            free (xtntDesc);
        }
    }

    return EOK;

_error:

    if (xtntDesc != NULL) {
        free (xtntDesc);
    }
    return sts;

}  /* end move_normal_file_clear */

/*
 * move_normal_file_fill
 *
 * This function moves an entire file for the process of filling a
 * previously cleared area, and thus defragmenting the file.
 */

static
adv_status_t
move_normal_file_fill (
                      int fd,  /* in */
                      bfTagT bfTag,  /* in */
                      bfTagT bfSetTag,  /* in */
                      uint32_t targetVolIndex,  /* in */
                      adv_bf_info_t *bfInfo,  /* in */
                      uint64_t fileExtentCount,  /* in */
                      int64_t *remainingClearAreaSize, /* in/out */
                      int volume,
                      int singleVolumeIndex,
                      adv_bf_attr_t *bfAttr
                      )
{
    adv_status_t sts = EOK;
    int xtntCount;
    int migrateBlkCount;
    uint64_t fileSizeBlks=(bfInfo->mbfNumFobs * bfInfo->pageSize);
    uint64_t fobOffset=0;
    uint64_t maxNumPgstoMigrate=0;
    uint64_t pgstoMigrate=0;

    /* 
     * Attempt to migrate the whole file into the remaining clear area.
     * If the file's size exceeds the remaining clear area size, then
     * look for a run of at least two extents that can be migrated into
     * the remaining clear area size.  
     *
     * Force files that are bigger than MAX_MIGRATE_FILL_SIZE_INBLKS
     * into find_xtnt_run extent combining routine below.
     */

    if( (fileSizeBlks > *remainingClearAreaSize) ||
        (singleVolumeIndex == -1 ) || 
        (fileSizeBlks >= MAX_MIGRATE_FILL_SIZE_INBLKS) ) {
        boolean found;
        xtntRunDescT *xtntRun;
        xtntRunDescT *nextxtnt;

        /*
           Don't migrate only a piece of a small file.  Small files
           can be migrated in their entirety in the fill phase of a
           later pass; a partial migrate would only have to be re-done,
           leaving an extra hole.  Only migrate pieces of files if the file
           has multiple extents that can be combined, thus reducing the number
           of extents(fragments).
         */

        if ((fileExtentCount <= 1)
            && (singleVolumeIndex != -1) ) {
            return EOK;
        }

        migrateBlkCount = 0;

        /*
         * Go take a look at the extents and logically combine the ones
         * that are both adjacent and on the same volume. Put these into
         * a local array called xtntRun.
         *
         * Then migrate the newly formed logical extents onto the
         * same volume thus physically combining the storage and reducing
         * fragmentation.
         *
         * find_xtnt_run() will only produce extents less than
         * maxNumPgstoMigrate size but not equal to or larger!
         */

        if (*remainingClearAreaSize > MAX_MIGRATE_FILL_SIZE_INBLKS)
            maxNumPgstoMigrate = (MAX_MIGRATE_FILL_SIZE_INBLKS / bfInfo->pageSize);
        else
            maxNumPgstoMigrate = (*remainingClearAreaSize / bfInfo->pageSize);

        /* find_xtnt_run will throw away extents >= maxNumPgstoMigrate */
        sts = find_xtnt_run (
                    fd,
                    maxNumPgstoMigrate,  /* max migrate size in any one extent */
                    bfInfo->pageSize,
                    fileExtentCount,
                    &found,
                    &xtntRun,
                    targetVolIndex,
                    bfTag,
                    bfAttr);
        if (sts != EOK) {
            goto _error;
        }
        if (found) {

            /* loop through the newly combined extents and migrate
             * them.
             */
            while (xtntRun) {
                migrateBlkCount += 
                          xtntRun->allocatedFobCount * bfInfo->pageSize;

                sts = migrate (
                            fd,
                            targetVolIndex,  /* srcVolIndex */
                            xtntRun->fobOffset,
                            xtntRun->nominalFobCount,
                            targetVolIndex,
                            bfTag,
                            bfSetTag);

                nextxtnt = xtntRun->next;
                free(xtntRun);    /* free up the newly created logical extent */
                xtntRun = nextxtnt;
            }
        }
    } else {

        /*
         * migrate the whole file because
         *  1. file is located on only one volume
         *  2. clear space (created or found) is large enough for whole file
         *  3. file type is not striped
         *  4. file is smaller than MAX_MIGRATE_FILL_SIZE_INBLKS
         */

        fobOffset = 0;
        migrateBlkCount = fileSizeBlks;
        pgstoMigrate = bfInfo->mbfNextFob;  /* no limit (last page)  */

        sts = migrate (
                        fd,
                        targetVolIndex,            /* srcVolIndex */
                        fobOffset,                /* fobOffset */
                        pgstoMigrate,              /* FobCount */
                        targetVolIndex,            /* dstVolIndex */
                        bfTag,
                        bfSetTag
                        );

    } /* end else */

    if (sts != EOK) {
        if ((sts == E_MIGRATE_IN_PROGRESS) || (sts == E_NO_SNAPSHOT_STG) ||
            (sts == E_RANGE_NOT_MAPPED) || (sts == E_CANT_MIGRATE_HOLE) ||
            (sts == E_INVOLUNTARY_ABORT)) {
            sts = EOK;
        }
        goto _error;
    }

    *remainingClearAreaSize -= migrateBlkCount;
    return EOK;

_error:

    return sts;

}  /* end move_normal_file_fill */

/*
 * migrate
 *
 * This function is used for migrating during both clear and fill phases.
 */

static
adv_status_t
migrate (
         int fd,  /* in */
         uint32_t srcVolIndex,  /* in */
         uint64_t bsed_fob_offset,  /* in */
         uint64_t bsed_fob_cnt,  /* in */
         uint32_t dstVolIndex,  /* in */
         bfTagT bfTag,
         bfTagT bfSetTag
         )

{

    adv_status_t sts = EOK;
    int unknown;
    uint32_t forceFlag = 0;

retry_migrate:
    sts = advfs_migrate (
                     fd,
                     srcVolIndex,
                     bsed_fob_offset,
                     bsed_fob_cnt,
                     dstVolIndex,
                     -1,/* dstBlkOffset */
                     forceFlag
                     );
    switch (sts) {

      case EOK:

        break;

      case E_MIGRATE_IN_PROGRESS:

        fprintf (stderr, CMSG(COMMON_CANTMOVE, "%s: Cannot move file %s\n"),
                 Prog, get_tag_path(NULL, bfTag, bfSetTag, &unknown));
        fprintf (stderr, CMSG(COMMON_MIGACTIVE, 
                 "%s: Migrate already active\n"), Prog);
        break;

      case E_NO_SNAPSHOT_STG:
                        
        fprintf (stderr, CMSG(COMMON_CANTMOVE, "%s: Cannot move file %s\n"),
                 Prog, get_tag_path(NULL, bfTag, bfSetTag, &unknown));
        fprintf (stderr, MSG(DF_ECLONE, 
                 "%s: Snapshot does not have its own storage\n"), Prog);
        break;

      case E_INVOLUNTARY_ABORT:

        if (!forceFlag) {
            forceFlag = 1;
            goto retry_migrate;
        }
        /* Fall through. */

      default:

        /* There is no need to print a message here, since move_files_fill
         * will report the error when it encounters the bad status. */
        break;

    }  /* end switch */

    return sts;

}  /* end migrate */

/*
 * get_bf_xtnt_map()
 *
 * This routine is like advfs_get_bf_xtnt_map, except that it does not
 * return extents that represent holes(sparse files), and returns one 
 * extent if two are found that are in fact together.
 *
 * get_bf_xtnt_map can also return the actual raw extents that were
 * obtained from advfs_get_bf_xtnt_map, in case the caller needs both
 * the raw ones and the filtered ones.
 *
 * Note that all extents of striped files are read in at the same time.
 *
 * Stripe files:  Both raw and filtered extents are xtnt-map relative.
 * Callers of get_bf_xtnt_map() need to convert these to bitfile
 * relative in order to pass appropriate parameters to bs_migrate().
 *
 * The stripeArray[] reads in stripe information for use with the
 * rawXtntsArrayParam.  The filteredXtntDescT has a stripe field
 * to hold stripe information.  This is useful as sometimes stripes
 * are not always on separate volumes.
 */
adv_status_t
get_bf_xtnt_map (
    int fd,  /* in */
    int startXtntMap,  /* in */
    int startXtnt,  /* in */
    int pageSize,  /* in */
    int xtntsArraySize,  /* in */
    filteredXtntDescT **filterxtntsArray,  /* out */
    bsExtentDescT **rawXtntsArrayParam,  /* out */
    int *filterxtntCnt, /* out */
    int *rawXtntCnt, /* out */
    adv_bf_attr_t *bfAttr,
    int **stripeArray
    )
{
    bsExtentDescT *srcXtntsArrayLocal=NULL;
    bsExtentDescT tmpXtntsArrayLocal[XTNT_DESC_MAX];
    bsExtentDescT **rawXtntsArray;
    bsExtentDescT *rawXtnts;
    int *stripe;
    filteredXtntDescT *filterXtnts;
    adv_status_t sts = EOK;
    int src, targ;
    unsigned holeFobCount;
    unsigned holeFobOffset;
    int xtntMapCnt = 0;
    int XtntCnt = 0;
    int i,x;
    unsigned allocVolIndex;
    int dst,totalXtntCnt;

    *rawXtntCnt = 0;

    if (rawXtntsArrayParam == NULL) {
        rawXtntsArray = &srcXtntsArrayLocal;
    } else {
        rawXtntsArray = rawXtntsArrayParam;
        if (*rawXtntsArray != NULL) {
            free(*rawXtntsArray);
            *rawXtntsArray = NULL;
        }
    }

    if (*filterxtntsArray != NULL) {
        free(*filterxtntsArray);
        *filterxtntsArray = NULL;
    }

    if (*stripeArray != NULL) {
        free(*stripeArray);
        *stripeArray = NULL;
    }

    /* fetch the extent map from kernel -
     * number of extents is limited by XTNT_DESC_MAX - which this call 
     * will not exceed.
     */
    rawXtnts = *rawXtntsArray = (bsExtentDescT *)
            malloc (sizeof(bsExtentDescT) * xtntsArraySize);

    if (rawXtnts == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

    sts = advfs_get_bf_xtnt_map(
                                fd,
                                startXtntMap,
                                startXtnt,
                                xtntsArraySize,
                                rawXtnts,
                                rawXtntCnt,
                                &allocVolIndex);

    if (sts != EOK) {
        goto _error;
    }

    targ = 0;
    holeFobCount = 0;

    filterXtnts = *filterxtntsArray = (filteredXtntDescT *)
        malloc (sizeof(filteredXtntDescT) * *rawXtntCnt);

    if (filterXtnts == NULL) {
        sts = ENO_MORE_MEMORY;
        goto _error;
    }

    for (src = 0; src < *rawXtntCnt; src++) {
        if ( rawXtnts[src].bsed_vd_blk == XTNT_TERM ||
             rawXtnts[src].bsed_vd_blk == PERM_HOLE_START )
        {
            /* if xtnt is a hole, save it for processing of next extent */
            holeFobCount = rawXtnts[src].bsed_fob_cnt;
            holeFobOffset = rawXtnts[src].bsed_fob_offset;
            continue;
        }
        if (targ > 0 && holeFobCount &&
            rawXtnts[src].bsed_vol_index == filterXtnts[targ-1].bsed_vol_index &&
            rawXtnts[src].bsed_vd_blk ==
                filterXtnts[targ-1].bsed_vd_blk +
                    filterXtnts[targ-1].allocatedFobCnt * pageSize) {

         /* extent is adjacent to last extent, so make last include this one */
            filterXtnts[targ-1].allocatedFobCnt += 
                                           rawXtnts[src].bsed_fob_cnt;

            filterXtnts[targ-1].nominalFobCnt += 
                                           rawXtnts[src].bsed_fob_cnt;

            filterXtnts[targ-1].nominalFobCnt += holeFobCount;
        } else {

          /* normal extent(not adjacent to next),
           * copy extent to new extent map 
           */
            filterXtnts[targ].fobOffset = rawXtnts[src].bsed_fob_offset;

            filterXtnts[targ].nominalFobCnt = 
                                              rawXtnts[src].bsed_fob_cnt;

            filterXtnts[targ].allocatedFobCnt = 
                                              rawXtnts[src].bsed_fob_cnt;

            filterXtnts[targ].bsed_vol_index = rawXtnts[src].bsed_vol_index;
            filterXtnts[targ].bsed_vd_blk = rawXtnts[src].bsed_vd_blk;
            if (holeFobCount > 0) {
                /* add the previous hole into the current extent */
                /* this is where we get rid of holes and do not show them */
                filterXtnts[targ].fobOffset = holeFobOffset;
                filterXtnts[targ].nominalFobCnt += holeFobCount;
            }
            targ++;
        }
        holeFobCount = 0;
    }

    if (rawXtntsArrayParam == NULL) {
        if (srcXtntsArrayLocal != NULL) {
            free(srcXtntsArrayLocal);
        }
    }

    *filterxtntCnt = targ;
    return EOK;

_error:

    if (rawXtntsArrayParam == NULL) {
        if (srcXtntsArrayLocal != NULL) {
            free(srcXtntsArrayLocal);
        }
    }

    if (sts != ENO_XTNTS) {
        if (rawXtntsArrayParam != NULL) {
            if (*rawXtntsArray != NULL) {
                free(*rawXtntsArray);
                *rawXtntsArray = NULL;
            }
        }

        if (*filterxtntsArray != NULL) {
            free(*filterxtntsArray);
            *filterxtntsArray = NULL;
        }

        if (*stripeArray != NULL) {
            free(*stripeArray);
            *stripeArray = NULL;
        }
    }

    return sts;

} /* end get_bf_xtnt_map */

/*
 * create_xtnt_desc_list
 *
 * This function creates a list of extents for the specified file
 * that are located on the specified volume and at which also 
 * begin in the range to be cleared (zone).
 * The page offset and count in each list entry
 * are bitfile relative.
 *
 * Successive extents, if they both meet the selection criteria,
 * are described by only one descriptor in the list, except that
 * successive extents are never combined into an extent that is
 * larger than LARGEST_MIGRATE_SIZE (that is, if a returned descriptor
 * in the list describes an extent larger than LARGEST_MIGRATE_SIZE,
 * then it really is a single extent).
 *
 * Do not return out of this function directly; instead, set sts
 * and goto _end in order to free memory.
 *
 */

static
adv_status_t
create_xtnt_desc_list (
                       int fd,                    /* in */
                       int xtntMapIndex,          /* in */
                       volDataT *volData,         /* in */
                       int pageSize,              /* in */
                       bfListEntryT *fileEntry,  /* in */
                       int *retXtntDescCnt,       /* out */
                       xtntDescT **retXtntDesc,   /* out */
                       adv_bf_attr_t *bfAttr,
                       metadataTypeT metadataType, /* in */
                       int phase
                       )

{
    uint64_t fobCnt;
    uint64_t fobOffset;
    uint64_t bsed_vol_index;
    int i;
    filteredXtntDescT *filteredXtntDesc=NULL;
    bsExtentDescT *rawXtntDesc=NULL;
    int rawXtntDescCnt=0;
    int filteredXtntDescCnt=0;
    int startXtntDesc=0;
    adv_status_t sts = EOK;
    xtntDescT *xtntDesc = NULL;
    int xtntDescCnt = 0;
    uint64_t xtntDescMaxCnt = 0;
    int volSubscr;
    int thisExtentCount = 0;
    int thisIOCount = 0;
    int xtntBlks = 0;
    int wrMaxIo;
    int largestXtntPages = 0;
    int smallestXtntPages = INT_MAX;
    int thisVolumeIndex = -1;
    int blksInRarifyArea = 0;
    int xtntsOnThisVol=0;
    int *stripeArray=NULL;
    int stripe=-1;
    int xtntsCount = 0;

    fileEntry->singleVolumeIndex = -1;
    fileEntry->isSparse = FALSE;

    for(sts = EOK, startXtntDesc=0;
        sts != ENO_XTNTS && sts != MAPDONE;
        startXtntDesc += rawXtntDescCnt) {
    
        /* get the file's extent map at offset startXtntDesc */


    /*
     * Lists Processed:
     *    fileEntry -> entry for this file in file list that was created 
     *                 previously by 
     *                 move_files_clear()::bfTableRoot.
     *    xtntDesc/retXtntDesc - List of extents that need to be cleared on 
     *                           this volume
     *
     * Operation: 
     * 1. Get all file extents for this file, regardless of volume
     * 2. Calculate some variables that will determine how much value in moving 
     *    this file later on during the fill phase.
     *
     *    IOCount - degree of fragmentation
     *    blksInRarifyArea - double payoff because blks are in an area that 
     *                       will be our next cleared area.  
     *    etc.
     *
     * 3. Put the extents that are in the areas to be cleared into the 
     *    descriptor list that will be returned to the caller
     *
     * 4. Lastly, IF file has no extents that need to be cleared, mark file in 
     *    list of files with performance parameters for use later on in fill 
     *    phase.
     *
     * Use the PRIME for counting stats and the XTNTS descriptors for 
     * processing.
     * By processing it is meant the xtnts need to be evaluated for movement if
     * they are in the clear zone.
     */
        sts = get_bf_xtnt_map (
                        fd,
                        xtntMapIndex,
                        startXtntDesc,           /* in */
                        pageSize,
                        XTNT_DESC_MAX,
                        &filteredXtntDesc,  /* out */
                        &rawXtntDesc,       /* out */
                        &filteredXtntDescCnt,    /* out */
                        &rawXtntDescCnt,         /* out */
                        bfAttr,
                        &stripeArray
                        );

        if (( sts == ENO_XTNTS ) ||
            (rawXtntDescCnt == 0) && (sts != ENO_MORE_MEMORY)) {
            break;
        }

        if ( sts != EOK && sts != MAPDONE ) {
            fprintf (stderr, CMSG(COMMON_ERREXTENT, 
                     "%s: Cannot get extent descriptors\n"), Prog);
            fprintf (stderr, CMSG(COMMON_ERR, 
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            goto _end;
        }

        /* Now, count the extents and add in this file's contribution
         * to the total I/O count.  Use the filtered extents for 
         * counting (since we want to count adjacent extents as one) and 
         * the raw extents to create the actual extent list that will be 
         * used to move the files in clearing.  Also, keep track of 
         * whether all of this file's storage
         * is on a single volume. 
         */
        xtntsCount += filteredXtntDescCnt;
        for (i = 0, xtntsOnThisVol=0; i < filteredXtntDescCnt; i++) {
            if (thisVolumeIndex == -1) {
              /* first time through - save it */
                /* save first volume occurance */
                thisVolumeIndex = filteredXtntDesc[i].bsed_vol_index;
                fileEntry->singleVolumeIndex = thisVolumeIndex;
            } else if (thisVolumeIndex != filteredXtntDesc[i].bsed_vol_index) {
                /* file has extents on multiple volumes */
                fileEntry->singleVolumeIndex = -1;
                /* still need to migrate clear of clear area, 
                 * if required! so drop through */
            }


            if(filteredXtntDesc[i].bsed_vol_index == volData->bsed_vol_index) {

                xtntBlks += filteredXtntDesc[i].allocatedFobCnt * pageSize;
                wrMaxIo = volData->wrMaxIo; /* will be same for all volumes 
                                             * - based on dev drvr 
                                             */
                thisIOCount +=
                    howmany(filteredXtntDesc[i].allocatedFobCnt * pageSize, 
                            wrMaxIo);  /* see comment in header */

                /* determine if file is sparse */
                if (filteredXtntDesc[i].allocatedFobCnt !=
                    filteredXtntDesc[i].nominalFobCnt) {
                    fileEntry->isSparse = TRUE;
                }
    
                /* find largest block of pages contained in a single extent */

                if (filteredXtntDesc[i].allocatedFobCnt > largestXtntPages) {
                    largestXtntPages = filteredXtntDesc[i].allocatedFobCnt;
                }

                /* 
                 * find smallest block of pages contained in a single extent
                 */
                if (filteredXtntDesc[i].allocatedFobCnt < smallestXtntPages &&
                    filteredXtntDesc[i].allocatedFobCnt != 0) {
                    smallestXtntPages = filteredXtntDesc[i].allocatedFobCnt;
                }
                /*
                 * is extent contained in the area to be filled?
                 * used later to calculate fileMovePayoff
                 */
                blksInRarifyArea += included_count(
                                        volData->lowerBlkRarifyBound,
                                        volData->upperBlkRarifyBound,
                                        filteredXtntDesc[i].bsed_vd_blk,
                                        filteredXtntDesc[i].bsed_vd_blk - 1 +
                                            filteredXtntDesc[i].allocatedFobCnt
                                            * pageSize);

                xtntsOnThisVol++;

            }  /* end if this extent is not on this threads volume */
        } /* end for all filtered extents */

        /* keep track of the total number of extents for this file */
        thisExtentCount += xtntsOnThisVol;

        /* 
         * look at ALL the raw extent descriptors and find ranges that
         * are successive to each other or stand alone.  When you find one, 
         * add it to the list IF it is in the area to be cleared on this volume
         *
         * compresses list of extents without regard for disk block
         *
         * End up with a list of extents that contain new ranges that 
         * are in areas of volumes that are to be cleared.
         */
        i = 0;
        while ((i < rawXtntDescCnt) && ( metadataType != BMT_PRIME_MCELL_XTNT_METADATA )) {
            /* skip xtnts on other vols  */
            if (rawXtntDesc[i].bsed_vol_index == volData->bsed_vol_index)    {
                /*
                 * Walk through all raw extents on this volume...
                 * Find an extent descriptor that describes an extent
                 * (that isn't too big) for a volume's given clear range
                 * as determined by volData->lowBlkClearBound and 
                 * volData->highBlkClearBound
                 */
                while (i < rawXtntDescCnt &&
                       !xtnt_in_range(   /* is extent in area to be cleared? */
                           volData,
                           rawXtntDesc[i].bsed_vol_index,
                           rawXtntDesc[i].bsed_vd_blk,
                           rawXtntDesc[i].bsed_fob_cnt * pageSize)) {
                    i++;  /* no?, keep going, get next extent in list */
                }  /* end while */

                /* if none found then jump to next extent map */

                if(i >= rawXtntDescCnt){
                    break;
                }

                /* 
                 * Found an extent in range specified in volData->lowBound! 
                 * save the start of this extent 
                 */
                fobOffset = rawXtntDesc[i].bsed_fob_offset;
                fobCnt = rawXtntDesc[i].bsed_fob_cnt;
                bsed_vol_index = rawXtntDesc[i].bsed_vol_index;
                volSubscr = volSubscrArray[bsed_vol_index];

                i++;  /* go to next extent */

                /* An extent belonging to another stripe will not
                 * be added to the current extent
                 */

                /*
                 * Find the successive extent descriptors (if any) that 
                 * describe an extent which is on the *same* source disk 
                 * in the given range.  Keep track of the page count to use 
                 * in the create of a single extent
                 * below.  This is how we combine adjacent extents. 
                 */


                while (i < rawXtntDescCnt 			&&
                       xtnt_in_range(   /* is extent in area to be cleared? */
                           volData,
                           rawXtntDesc[i].bsed_vol_index,
                           rawXtntDesc[i].bsed_vd_blk,
                           rawXtntDesc[i].bsed_fob_cnt * pageSize) &&
                       volSubscrArray[rawXtntDesc[i].bsed_vol_index] == volSubscr) {

                    /* As long as extents lie in the range to be cleared,
                     * keep adding them to the list.  They will be migrated
                     * as smaller pieces later on in migrate(), if
                     * necessary.
                     */

                    fobCnt += rawXtntDesc[i].bsed_fob_cnt;

                    i++;
                }  /* end while */

                /* extend the list we are building if the list doesn't
                 * have enough slots.
                 */
                if (xtntDescCnt >= xtntDescMaxCnt) {
                    sts = extend_xtnt_desc_list (&xtntDescMaxCnt, &xtntDesc);
                    if (sts != EOK) {
                        goto _end;
                    }
                }

                /* Add this entry to the list we are creating for clearing */

                xtntDesc[xtntDescCnt].fobOffset = fobOffset;
                xtntDesc[xtntDescCnt].fobCnt = fobCnt;

                xtntDesc[xtntDescCnt].bsed_vol_index = bsed_vol_index;
                xtntDesc[xtntDescCnt].volSubscr = volSubscr;
                xtntDescCnt++;

            } else {
                i++;
            }
        }  /* end while */
    }  /* end for all extent maps offsets */

    /* 
     * Add this file's contribution to the total I/O count and
     * total best I/O count. 
     */
    volData->totalIOCount += thisIOCount;

    if (xtntBlks > 0) {
        volData->totalBestIOCount += howmany(xtntBlks, wrMaxIo);
    }

    if((phase == 1) && ( metadataType == BMT_PRIME_MCELL_XTNT_METADATA )) {
       /* Add this file's contributions to the total extent count */
        volData->extentCount += xtntsCount;
       /* keep track of the number of files processed */
        volData->fileWithExtCount++;
    }

    /* 
     * If it had only zero extents, mark it as not needing to be 
     * moved in the fill phase.
     */
    if ( (thisExtentCount < 1) && (metadataType == BMT_XTNT_METADATA )) {
        fileEntry->noFillMove = TRUE;
    }

    /* If there were no extents found that represent extents to be
     * cleared, then we can assume that the data gathered for this
     * file represents what should be analyzed in the fill phase.
     * (But note that files that have only one extent, and no clearing
     * required will not need to be considered in the fill phase, since
     * they can be assumed to still have only one extent.) */


    if (thisExtentCount > 1) {
        fileEntry->hasValidData = TRUE;
        if (thisIOCount <= 1) {
                fileEntry->filePerf = 100.0;
        } else {
                fileEntry->filePerf =
                        (double)howmany(xtntBlks, wrMaxIo) / 
                        (double)thisIOCount * 100.0;
        }
        fileEntry->fileExtentCount = thisExtentCount;
        fileEntry->fileMovePayoff = (double)(thisExtentCount-1) /
                                        (double)xtntBlks;

        if (xtntBlks >= largeFileThreshhold) {
                /*
                 * For large files, boost the payoff of files that have a large 
                 * difference in largest and smallest extent size, especially 
                 * if they have many small ones.
                 */
                 fileEntry->fileMovePayoff *= non_uniform_payoff_boost(
                                                largestXtntPages * pageSize,
                                                smallestXtntPages * pageSize,
                                                xtntBlks / thisExtentCount,
                                                FALSE);
        }
        fileEntry->fileMovePayoff *= 1.0 +
                    (double)blksInRarifyArea / 
                    (double)xtntBlks * (MAX_RARIFY_BOOST - 1.0);

    } else {
        /* file is defragmented to 1 extent already and doesn't need cleared */
        fileEntry->noFillMove = TRUE;
    }

    if (xtntDescCnt == 0) {   /* number of extents to be cleared */
        fileEntry->wasCleared = FALSE;
    } else {
        fileEntry->wasCleared = TRUE;
    }


    *retXtntDescCnt = xtntDescCnt;
    *retXtntDesc = xtntDesc;

    sts = EOK;

_end:

    if ((sts != EOK) && (xtntDesc != NULL)) {
        free (xtntDesc);
    }

    if (rawXtntDesc != NULL) {
        free(rawXtntDesc);
    }

    if (filteredXtntDesc != NULL) {
        free(filteredXtntDesc);
    }

    if (stripeArray != NULL) {
        free(stripeArray);
    }

    return sts;

}  /* end create_xtnt_desc_list */



/*
 * extend_xtnt_desc_list
 *
 * This function extends the extent list to allow more extent entries
 * to be added.
 */

static
adv_status_t
extend_xtnt_desc_list (
                       uint64_t *maxCnt,  /* in/out */
                       xtntDescT **xtntDesc  /* in/out */
                       )

{

    int i;
    uint64_t newMaxCnt=0;
    xtntDescT *newXtntDesc=NULL;
    uint64_t oldMaxCnt=0;
    xtntDescT *oldXtntDesc=NULL;

    oldMaxCnt = *maxCnt;
    oldXtntDesc = *xtntDesc;

    /* get arbitrarily 100 more extent slots */
    newMaxCnt = oldMaxCnt + 100;

    if (oldMaxCnt != 0) {
        newXtntDesc = (xtntDescT *) malloc (newMaxCnt * sizeof (xtntDescT));
        if (newXtntDesc == NULL) {
            fprintf (stderr, CMSG(COMMON_EXPMEMEXT, 
                    "%s: Cannot allocate memory for expanded xtnt desc table\n"),
                    Prog);
            return ENO_MORE_MEMORY;
        }
        /* copy the old list to the new, extended list */
        for (i = 0; i < oldMaxCnt; i++) {
            newXtntDesc[i] = oldXtntDesc[i];
        }  /* end for */
    } else {
        newXtntDesc = (xtntDescT *) malloc (newMaxCnt * sizeof (xtntDescT));
        if (newXtntDesc == NULL) {
            fprintf (stderr, CMSG(COMMON_MEMXD, 
                     "%s: Cannot allocate memory for new xtnt desc table\n"),
                     Prog);
            return ENO_MORE_MEMORY;
        }
    }

    /* set the new count of the number of slots */
    *maxCnt = newMaxCnt;
    *xtntDesc = newXtntDesc;

    return EOK;

}  /* end extend_xtnt_desc_list */


static
xtntRunDescT *
get_run_desc(
        int nominalFobCount,
        int allocatedFobCount,
        int fobOffset,
        int volume)
{
    xtntRunDescT *newDesc;

    newDesc = (xtntRunDescT *)malloc(sizeof(xtntRunDescT));

    if (newDesc) {
        newDesc->nominalFobCount = nominalFobCount;
        newDesc->allocatedFobCount = allocatedFobCount;
        newDesc->fobOffset = fobOffset;
        newDesc->next = NULL;
    } else {
        fprintf (stderr, MSG(DF_MEMRD, 
                 "%s: Cannot allocate memory for run descriptors.\n"), Prog);
        return NULL;
    }
    return newDesc;
} /* end get_run_desc */


/*
 * Find a suitable run of at least two consecutive extents in the given file
 * that has a total page size of less than or equal to the given maximum and
 * also where the extents exist on the same volume.
 *
 * If possible, find more than one run, as long as the total size of the
 * runs is less than the given maximum.
 *
 * Do not return out of this function directly; instead, set retval
 * and goto _end in order to free memory.
 *
 */
static
adv_status_t
find_xtnt_run (
           int fd,  /* in */
           int maxFobCount,  /* in */
           int pageSize,  /* in */
           uint64_t fileExtentCount,  /* in */
           boolean *found,  /* out */
           xtntRunDescT **retXtntRuns, /* out */
           int volume,
           bfTagT bfTag, /* in */
           adv_bf_attr_t *bfAttr
           )
{
    uint64_t nominalFobCnt;
    uint64_t allocatedFobCnt;
    uint64_t fobOffset,prevfobOffset,prevnominalFobCnt;
    int i, j=0;
    filteredXtntDescT *filteredXtntDesc=NULL;
    int filteredXtntDescCnt;
    int rawXtntDescCnt;
    int startXtntDesc;
    adv_status_t sts = EOK;
    int runSize;
    int averageExtentSize;
    int threshholdExtentSize;
    xtntRunDescT **nextDescTarget = retXtntRuns;
    xtntRunDescT *newRunDesc = NULL;
    int nextExpectedOffset=0;
    int StripeCnt=0;
    int retval=EOK;
    int *stripeArray=NULL;
    int stripe=-1;

    if (fileExtentCount == 0) {
        *found = FALSE;
        return EOK;
    }

    /* Calculate the threshhold (size or smaller of an extent that
     * a run must start with).  This is based on the
     * second smallest extent's size (second smallest because then
     * we're guaranteed there's at least one that is not the last
     * extent)
     */

    /* Exempt extents that start a run that are very large.  This
     * should have the effect of combining the smaller extents in a
     * run into larger extents.  Remember that all this applies to
     * only those files that cannot be migrated in their entirety due
     * to clear area space limitations.
     * We are factoring in maxFobCount( space remaining) here.
     */
    threshholdExtentSize = maxFobCount * SINGLE_XTNT_OF_CLEAR_SIZE_FRAC;

    /* Now, look at all of the extents to find a suitable run. */

    rawXtntDescCnt = 0;
    startXtntDesc = 0;

    *found = FALSE;
    sts = EOK;
    for (i=0,runSize=0; sts == EOK; i=0) {

        startXtntDesc += rawXtntDescCnt;
        /* fetch the extents */
        sts = get_bf_xtnt_map (
                        fd,
                        1,
                        startXtntDesc,
                        pageSize,
                        XTNT_DESC_MAX,
                        &filteredXtntDesc,
                        NULL,
                        &filteredXtntDescCnt,
                        &rawXtntDescCnt,
                        bfAttr,
                        &stripeArray
                        );

        if ( (sts != ENO_XTNTS) && (sts != EOK) && (sts != MAPDONE) ) {
            fprintf (stderr, CMSG(COMMON_ERREXTENT,
                     "%s: Cannot get extent descriptors\n"), Prog);
            fprintf (stderr, CMSG(COMMON_ERR, 
                     "%s: Error = %s\n"), Prog, BSERRMSG (sts));
            retval = sts;
            goto _end;
        }

        if(sts != EOK && sts != MAPDONE) 
            break;

        while( i < filteredXtntDescCnt) {

            /*
             * At this point, we either just obtained a fresh batch of
             * extents or else had encountered an extent which turned out
             * not to be the start of a suitable run (possibly both are true).
             * We either need to look for the start of a run or look for
             * further extents in a run.
             */
look_for_next_start:

            if (runSize == 0) {
                /* Find the first extent descriptor that describes an extent
                 * which is smaller than the space left for it to be moved into,
                 * and is also smaller than the threshholdExtentSize for this file.
                 * and is on this volume 
                 *
                 * If an extent is already in the largest size allowed skip it.
                 *
                 */
                while (i < filteredXtntDescCnt) {
                    if (filteredXtntDesc[i].allocatedFobCnt < maxFobCount &&
                        filteredXtntDesc[i].allocatedFobCnt <= 
                                                       threshholdExtentSize  &&
                        filteredXtntDesc[i].bsed_vol_index == volume ) {
                        runSize = 1;
                        nominalFobCnt = filteredXtntDesc[i].nominalFobCnt;
                        allocatedFobCnt = filteredXtntDesc[i].allocatedFobCnt;
                        fobOffset = filteredXtntDesc[i].fobOffset;
                        prevfobOffset = filteredXtntDesc[i].fobOffset;
                        prevnominalFobCnt = filteredXtntDesc[i].nominalFobCnt;
                        stripe = filteredXtntDesc[i].stripe;
                        i++;
                        break;
                    }
                    i++;
                }  /* end while */
                if (runSize == 0) {
                    break;    /* get more extents - none found */
                }
            }

            /*
             * Now, see if the first extent just found is the start of
             * a run of two or more extents whose total size is smaller
             * than the maximum AND also whose extents are contiguous(no hole)
             * AND on the same volume as the first extent.
             */

            while(i < filteredXtntDescCnt) {

                int newFobCount = 
                    allocatedFobCnt + filteredXtntDesc[i].allocatedFobCnt;

                nextExpectedOffset = prevfobOffset + prevnominalFobCnt;

                /* small enough to fit in clear zone */
                /* adjacent to the last one */
                /* on same vol */
                if ((newFobCount < maxFobCount) &&
                    (filteredXtntDesc[i].fobOffset == nextExpectedOffset) &&
                    (filteredXtntDesc[i].bsed_vol_index == volume) )  {
                    /* keep going - we are on a roll */
                    runSize++;
                    allocatedFobCnt = newFobCount;
                    prevfobOffset = filteredXtntDesc[i].fobOffset;
                    prevnominalFobCnt = filteredXtntDesc[i].nominalFobCnt;
                    nominalFobCnt += filteredXtntDesc[i].nominalFobCnt;
                    i++;
                } else {
                    if (runSize > 1) {

                        /* runSize > 1
                         * end of a run  - get descriptor to describe the run
                         */

                        *found = TRUE;

                        newRunDesc = get_run_desc(
                                        nominalFobCnt,
                                        allocatedFobCnt,
                                        fobOffset,
                                        volume);
                        if (newRunDesc) {
                            /* set callers returning pointer(*nextDescTarget) to point
                             * to structure just created
                             */
                            *nextDescTarget = newRunDesc;
                            /* link next one (if created later, otherwise NULL) into
                             * the list
                             */
                            nextDescTarget = &newRunDesc->next;
                        } else {
                            fprintf (stderr, MSG(DF_GETRUN, 
                             "%s: Error occurred getting extent descriptors\n"),
                             Prog);
                            retval = ENO_MORE_MEMORY;
                            goto _end;
    
                        }
                        maxFobCount -= allocatedFobCnt;
    
                        if (maxFobCount * pageSize <= 0) {
                            retval = EOK;
                            goto _end;
                        }

                    } /* end if runSize > 1*/

                   runSize = 0;
                   goto look_for_next_start;

                } /* end else if run is over and we need to create a logical xtnt entry */

            }  /* end while checking for end of run */

        }  /* end while still xtnts to be processed in this map fetch */

    }  /* end for still extents to be gotten in file */

    /* Falling through means we have looked at all of the extents, and
     * not come to the termination of a run.  Either we are in a run or
     * not at this point.  If we are currently in a run we need to create
     * the logical xtnt entry at this point and return.  If we are not
     * currently in a run then we just need to return.
     */
    if (runSize > 1) {
        /* we are currently in the middle of a run */
        *found = TRUE;

        newRunDesc = get_run_desc(nominalFobCnt, 
                                  allocatedFobCnt, 
                                  fobOffset, 
                                  volume);
        if (newRunDesc) {
            /* set callers returning pointer(*nextDescTarget) to point
             * to structure just created
             */
             *nextDescTarget = newRunDesc;
            /* link next one (if created later, otherwise NULL) into
             * the list
             */
            nextDescTarget = &newRunDesc->next;
        } else {
            fprintf (stderr, MSG(DF_GETRUN, 
                    "%s: Error occurred getting extent descriptors\n"),
                     Prog);
            retval = ENO_MORE_MEMORY;
            goto _end;
        }
    }

    retval = EOK;

_end:

    if (filteredXtntDesc != NULL) {
        free(filteredXtntDesc);
    }

    if (stripeArray != NULL) {
        free(stripeArray);
    }

    return retval;

}  /* end find_xtnt_run */

/* 
 * find_next_hole
 *
 * If the byte we're about to read will cause the last byte
 * in the buffer to overwrite the first, find the next hole.
 * If there are no more holes in the buffer, continue to search
 * ahead until another hole is found.
 */
int
find_next_hole(
             char *window,
             int     *currentWindowHolePopulation,
             int     *currentWindowFreePopulation,
             int     *currentWindowFirst,
             int     *currentWindowLast,
             int     *currentWindowSize, 
             int     *maxWindowSize,
             int     *lowerExcludeMapPosition, 
             int     *upperExcludeMapPosition,
             int     *sparsestMapFreePopulation,
             int     *sparsestMapPosition,
             int     *sparsestMapSize,
             boolean *sparsestAreaWasFound,
             int     *greatestHoleMapPopulation,
             int     *greatestHoleMapPosition,
             int     *greatestHoleMapSize,
             boolean *greatestHoleAreaWasFound,
             int     *mapFreePopulationAtGreatestHoles,
             findSparsestAreaRootT *root, 
             boolean *byteWasRead, 
             char    *nextMapByte,
             unsigned clusterSize,
             int     *currentMapPosition,
             boolean *inHole,
             int     *currentHoleSize, 
             int     *largestHoleSize,
             volFreeSpaceDataT *freeSpaceData,  
             int     *largestHoleMapPosition,
             int     *nextBmtXtntIndex,
             bsExtentDescT *bmtXtntArray,
             int     *blksRepPerMapByte,
             int     *bmtXtntCount
            )
{
    boolean lookingForZero = FALSE;
    int oldCurrentMapPosition = *currentMapPosition;

    (*currentWindowHolePopulation)--;

    /* look for holes as we walk the first window pointer through the 
     * buffer
     */

    while (TRUE) {
        if (!window[*currentWindowFirst]) {
            (*currentWindowFreePopulation)--;
        }
        (*currentWindowFirst)++;
        if (*currentWindowFirst == *maxWindowSize) {
            *currentWindowFirst = 0;
        }
        (*currentWindowSize)--;

        if (*currentWindowSize > 0) {
           /* if we haven't overran the buffer */
            (*currentMapPosition)++;
            if (lookingForZero && !window[*currentWindowFirst]) {
                /* Found the start of next hole; done. */
                return OKAY;
            }
            if (!lookingForZero && window[*currentWindowFirst]) {
                /* Found the end of the hole; look for start of next. */
                lookingForZero = TRUE;
            }
        } else {
            /* The first byte has caught up with the last byte 
             * (i.e. the buffer is empty).  This means that the buffer did
             * not contain the start of another hole.  In this case,
             * just keep looking for the next hole; when its start is
             * found (if ever), it will just be put at the start of the
             * buffer, which will be re-initialized. 
             */
            if (!lookingForZero) {
                /* The buffer is all zeros (a part of a very large
                 * hole).  See if this represents the most free
                 * space or the most holes seen so far. 
                 */
                if (!included_count(
                          *lowerExcludeMapPosition, *upperExcludeMapPosition,
                          oldCurrentMapPosition,
                          oldCurrentMapPosition + *maxWindowSize - 1)) {

                   if (*maxWindowSize > *sparsestMapFreePopulation) {
                        *sparsestMapFreePopulation = *maxWindowSize;
                        *sparsestMapPosition = oldCurrentMapPosition;
                        *sparsestMapSize = *maxWindowSize;
                        *sparsestAreaWasFound = TRUE;
                    }

                    if (1 > *greatestHoleMapPopulation ||
                       (1 == *greatestHoleMapPopulation && 
                        *currentWindowFreePopulation > 
                        *mapFreePopulationAtGreatestHoles)) {

                        *greatestHoleMapPopulation = 1;
                        *mapFreePopulationAtGreatestHoles = *maxWindowSize - 1;
                        *greatestHoleMapPosition = oldCurrentMapPosition;
                        *greatestHoleMapSize = *maxWindowSize;
                        *greatestHoleAreaWasFound = TRUE;
                    }
                } /* end if !included_count */
            }

            /* find next hole */

            while (TRUE) {

                if (get_next_map_byte(root, byteWasRead, nextMapByte)) {
                    return ERROR;
                }

                if (!*byteWasRead) {
                    /* no more map entries - exit */
                    return MAPDONE;
                }

                /* if nextMapByte is a hole - 
                 * record the fact that we are starting in a hole.  if
                 * nextMapByte is not a hole, and not coming off a
                 * hole, then do nothing.
                 */

                update_free_space_data(
                    *nextMapByte, clusterSize,
                    *currentMapPosition, inHole,
                    currentHoleSize, largestHoleSize,
                    freeSpaceData, largestHoleMapPosition);

                (*currentMapPosition)++;

                if (lookingForZero && !*nextMapByte) {
                    /* found a new hole! update window and exit */
                    *currentWindowFreePopulation = 0;
                    *currentWindowHolePopulation = 0;
                    *currentWindowFirst = 0;
                    *currentWindowLast = 0;
                    window[0] = 0;
                    *currentWindowSize = 1;

                    /* Find index of next bmt extent that we will encounter. */
                    while (bmtXtntArray[*nextBmtXtntIndex].bsed_vd_blk <
                           *currentMapPosition * *blksRepPerMapByte &&
                           *nextBmtXtntIndex < *bmtXtntCount) {
                                (*nextBmtXtntIndex)++;
                    }

                    return OKAY;
                }

                if (!lookingForZero && *nextMapByte) {
                    lookingForZero = TRUE;
                }

            } /* end while */
        }  /* end else if currentWindowSize has reached zero */

   } /* end while */

} /* end find_next_hole */


/* update_free_space_data()
 * 
 * updates the structure that keeps track of the amounts of free space
 * within each free space size range (1-4) for this volume.
 * It also keeps track of the largest hole found on this volume so far.
 *
 * If nextMapByte is already allocated, then check to see if this is the
 * first allocated byte after a hole.  If it is not then exit, if it is
 * the end of a hole then do the updates.
 *
 * If nextMapByte is not allocated, keep going and note the fact that
 * we are in a hole.
 */
static
void
update_free_space_data(
        int nextMapByte,  /* in */
        int clusterSize,  /* in */
        int nextBytePosition,  /* in */
        boolean *inHole,  /* in/out */
        int *currentHoleSize,  /* in/out */
        int *largestHoleSize,  /* in/out */
        volFreeSpaceDataT *freeSpaceData,  /* in/out */
        int *largestHoleMapPosition)  /* out */
{
    int range;
    uint64_t currentHoleSizeInCluster = *currentHoleSize;

    if (nextMapByte) {
     /* allocated 8 clusters - since 1 byte == 8 clusters */
        if (*inHole) {
            /* End of the current hole. - record free space and hole counts */
            if (freeSpaceData != NULL) {
                for (range = 0; range < FREE_RANGE_COUNT; range++) {
                    /* determine the hole size */
                    uint64_t currentHoleSizeKb = (currentHoleSizeInCluster *
                            8 * clusterSize * BLOCK_SIZE) / 1024;
                    if (freeRangeTop[range] == 0 ||
                        currentHoleSizeKb < freeRangeTop[range]) {
                        /* record this under the correct hole size 
                         * (free space entry)
                         */
                        freeSpaceData->rangeData[range].holeCount++;
                        freeSpaceData->rangeData[range].freeSpace += 
                                                       currentHoleSizeKb;

                        freeSpaceData->totalHoleCount++;
                        freeSpaceData->totalFreeSpace += currentHoleSizeKb;
                        break;
                    }
                }
            }
            if (*currentHoleSize > *largestHoleSize) {
              /* record the largest hole found thus far on this volume */
                *largestHoleSize = *currentHoleSize;
                *largestHoleMapPosition = nextBytePosition - *currentHoleSize;
            }
            /* not in a hole anymore */
            *inHole = FALSE;
            *currentHoleSize = 0;
        }
    } else {
     /* unallocated byte == 8 clusters */
        /* In a hole or starting a hole */
        /* Add a byte to the current hole (or start a new one). */
        *inHole = TRUE;
        (*currentHoleSize)++;
    }

} /* end update_free_space_data */

/*
 * find_sparsest_area
 *
 * Search the storage bitmap and find the area of the desired size that
 * has either the smallest fraction of blocks allocated or (if reduceHoleFlag
 * is TRUE) has the greatest number of holes.
 *
 * This routine also has the effect of gathering data on free space holes of
 * the filesystem; sometimes, it is called when that is the only effect of interest.
 *
 * This algorithm assumes that each of the bytes in the bit map is either
 * all zeroes or all ones.  This means that it does its work based on 8 clusters
 * at a time since a cluster is represented by a single bit and there are 8 bits
 * in an int.
 */
static
int
find_sparsest_area (
            uint64_t bsed_vol_index,  /* in */
            unsigned volSize,  /* in */
            unsigned clusterSize,  /* in */
            boolean inhibitDisplay,  /* in */
            int lowerExcludeBound,  /* in */
            int upperExcludeBound,  /* in */
            clearStrategyT clearStrategy,  /* in */
            adv_bf_dmn_params_t *dmnParams,  /* in */
            uint64_t *areaSize,  /* in/out */
                             /*in- area to find, out-area actually found */
            uint64_t *areaPopulation,  /* in/out */
            uint64_t *lowerBound,  /* out */
            boolean *alreadyClear,  /* out */
            volFreeSpaceDataT *freeSpaceData, /* out */
            volDataT *volData
            )

{
    findSparsestAreaRootT root;
    boolean mapNameIsAllocated;
    boolean bmtNameIsAllocated;
    boolean windowIsAllocated;
    boolean bufferIsAllocated;
    boolean mapFileIsOpen;
    boolean errorFlag;

    int fd;
    adv_status_t sts = EOK;
    adv_bf_attr_t bfAttrs;
    adv_bf_info_t bfInfo;
    char *bmtFileName;
    int err;

    char *window;
    int maxWindowSize;
    int currentWindowFirst;
    int currentWindowLast;
    int currentWindowSize;
    int currentWindowFreePopulation;
    int currentWindowHolePopulation;
    int currentMapPosition;
    char fileNumber[10];
    char nextMapByte;
    boolean byteWasRead;
    boolean hasFreeArea;
    boolean sparsestAreaWasFound;
    boolean greatestHoleAreaWasFound;
    boolean isFirstTry;
    int blksRepPerMapByte;

    int sparsestMapPosition;
    int sparsestMapFreePopulation;
    int sparsestMapSize;

    int greatestHoleMapPosition;
    int greatestHoleMapPopulation;
    int greatestHoleMapSize;	/* i.e., size of map w/greatest holes */
    int mapFreePopulationAtGreatestHoles;

    int largestHoleSize;
    int largestHoleMapPosition;

    float filledFraction = 0.0;

    boolean inHole;
    int currentHoleSize;
    int range;

    int lowerExcludeMapPosition;
    int upperExcludeMapPosition;

    struct {
        float filledFraction;
        int sparsestMapPosition;
        int sparsestMapFreePopulation;
        int sparsestMapSize;
        int maxWindowSize;
    } first;
        
    bsExtentDescT bmtXtntArray[MAX_BMT_EXTENTS];
    int bmtXtntCount;
    int nextBmtXtntIndex;
    unsigned allocVolIndex;

    if (*areaSize > MAXIMUM_CLEAR_AREA_SIZE) {
        *areaSize = MAXIMUM_CLEAR_AREA_SIZE;
    } else if (*areaSize == 0) { 
        /* can happen if rtn is called to only gather data */
        *areaSize = clusterSize * 8; /* to accomodate any cluster size */
    }

    blksRepPerMapByte = clusterSize * 8;
    lowerExcludeMapPosition = lowerExcludeBound / blksRepPerMapByte;
    upperExcludeMapPosition = upperExcludeBound / blksRepPerMapByte;
    errorFlag = FALSE;
    mapNameIsAllocated = FALSE;
    bmtNameIsAllocated = FALSE;
    bufferIsAllocated = FALSE;
    windowIsAllocated = FALSE;
    mapFileIsOpen = FALSE;
    isFirstTry = TRUE;

/*************** Open the storage bitmap file and get its params **************/

    /* Determine the name of the map file. */
    root.mapFileName = malloc(strlen(metadataRootPathName) + 10);
    if (root.mapFileName == NULL) {
        fprintf(stderr, MSG(DF_MEMMFN, 
                "%s: Cannot allocate memory for map file name\n"),
                Prog);
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }
    mapNameIsAllocated = TRUE;
    strcpy(root.mapFileName, metadataRootPathName);
    strcat(root.mapFileName, "/-");
    sprintf(fileNumber, "%d", bsed_vol_index * 6 + 1);
    strcat(root.mapFileName, fileNumber);

    fd = open(root.mapFileName, O_RDONLY, 0);
    if (fd < 0) {
        fprintf (stderr, CMSG(COMMON_NOOPEN2, "%s: open of %s failed.\n"),
                 Prog, root.mapFileName);
        fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                 Prog, errno, ERRMSG (errno));
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }

    sts = advfs_get_bf_params(fd, &bfAttrs, &bfInfo);

    if (sts != EOK) {
        fprintf (stderr, CMSG(COMMON_ERRBFP, 
                 "%s: advfs_get_bf_params failed --- %s\n"),
                 Prog, BSERRMSG( sts ));
        (void)close(fd);
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }
    (void)close(fd);

/***************** Get the bmt extents and load into an array **************/

    /* Find out where the pieces of the bmt file are, so that
     * we can avoid clearing and filling an area that spans
     * such a piece. */

    /* Determine the name of the bmt file. */
    bmtFileName = malloc(strlen(metadataRootPathName) + 10);
    if (bmtFileName == NULL) {
        fprintf(stderr, MSG(DF_MEMBFN, 
                "%s: Cannot allocate memory for bmt file name\n"),
                Prog);
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }
    bmtNameIsAllocated = TRUE;
    strcpy(bmtFileName, metadataRootPathName);
    strcat(bmtFileName, "/-");
    sprintf(fileNumber, "%d", bsed_vol_index * 6);
    strcat(bmtFileName, fileNumber);

    fd = open(bmtFileName, O_RDONLY, 0);
    if (fd < 0) {
        fprintf (stderr, CMSG(COMMON_NOOPEN2, "%s: open of %s failed.\n"),
                 Prog, bmtFileName);
        fprintf (stderr, CMSG(COMMON_GEN_NUM_STR_ERR, "%s: Error = [%d] %s\n"),
                 Prog, errno, ERRMSG (errno));
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }

    sts = advfs_get_bf_xtnt_map(
            fd,
            1,
            0,
            MAX_BMT_EXTENTS,
            bmtXtntArray,
            &bmtXtntCount,
            &allocVolIndex);

    if (sts != EOK) {
        fprintf (stderr, MSG(DF_EBFEXT, 
                 "%s: Cannot get extents of bmt %s --- %s\n"),
                 Prog, bmtFileName, BSERRMSG( sts ));
        (void)close(fd);
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }
    (void)close(fd);

    /* Sort the bmt extents by volume block. */
    {
        int i, j;
        bsExtentDescT tempXtnt;

        for (j = 1; j < bmtXtntCount; j++) {
            for (i = 0; i < j; i++) {
                if (bmtXtntArray[i].bsed_vd_blk > bmtXtntArray[j].bsed_vd_blk) {
                    tempXtnt = bmtXtntArray[j];
                    bmtXtntArray[j] = bmtXtntArray[i];
                    bmtXtntArray[i] = tempXtnt;
                }
            }
        }
    }

/****************************** Allocate a buffer ***********************/

    root.buffer_size = bfInfo.pageSize * BLOCK_SIZE;
    root.buffer = malloc(root.buffer_size);
    if (root.buffer == NULL) {
        fprintf(stderr, MSG(DF_MEMMFB, 
                "%s: Cannot allocate memory for map file buffer\n"),
                Prog);
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }
    bufferIsAllocated = TRUE;

/****************************** Allocate a window ***********************/

    /* Allocate a window big enough to hold enough of the bitmap to represent
     * an area of the size we're looking for. */
    maxWindowSize = *areaSize / blksRepPerMapByte;
    window = malloc(maxWindowSize);
    if (window == NULL) {
        fprintf(stderr, MSG(DF_MEMSBM, 
                "%s: Cannot allocate memory for analyzing storage bit map\n"),
                Prog);
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }
    windowIsAllocated = TRUE;

/*************** Open storage map , Initialize structures ***************/

try_smaller_window:
    root.mapFile = fopen(root.mapFileName, "r");
    if (root.mapFile == NULL) {
        fprintf(stderr, MSG(DF_EBITMAP, 
                "%s: Error opening bitmap file %s\n"),
                Prog, root.mapFileName);
        errorFlag = TRUE;
        goto quit_find_sparsest;
    }
    mapFileIsOpen = TRUE;
    inHole = FALSE;
    currentHoleSize = 0;
    if (freeSpaceData != NULL) {
        freeSpaceData->totalFreeSpace = 0;
        freeSpaceData->totalHoleCount = 0;
        for (range = 0; range < FREE_RANGE_COUNT; range++) {
            freeSpaceData->rangeData[range].holeCount = 0;
            freeSpaceData->rangeData[range].freeSpace = 0;
        }
    }

    /* Initialize to begin reading map bytes. */
    root.remainingBytesToRead = volSize / blksRepPerMapByte;
    root.remainingBytesToRead +=
        (root.remainingBytesToRead + root.buffer_size - HEADER_SIZE - 1) /
            (root.buffer_size - HEADER_SIZE) * HEADER_SIZE;
    root.bytesInBuffer = root.buffer_size;
    root.bufferPosition = root.buffer_size;

    nextBmtXtntIndex = 0;
    currentMapPosition = 0;		/* Corresponds to currentWindowFirst. */
    hasFreeArea = FALSE;
    sparsestAreaWasFound = FALSE;
    greatestHoleAreaWasFound = FALSE;
    sparsestMapFreePopulation = 0;
    greatestHoleMapPopulation = 0;
    largestHoleSize = 0;

/****************************** Find start of first hole ********************/
skip_bmt_extent:

    /* Find the START of the first hole, and initialize the window
     * to begin there.  We also come here if, in the course of reading
     * the storage bitmap, we encounter a bmt extent, as we then want to start
     * with the window at the beginning of the next hole. */

     /* loop through each byte in storage bitmap, until START of first hole */
    while (TRUE) {
        /* get the next storage bitmap byte in line */
        if (get_next_map_byte(&root, &byteWasRead, &nextMapByte)) {
            errorFlag = TRUE;
            goto quit_find_sparsest;
        }
        if (!byteWasRead) {
            /* No more bytes were found.  - end of bitmap file */
            goto end_of_map_already_processed;
        }
        /* if nextMapByte is a hole - record the fact that we are starting 
         * in a hole.  if nextMapByte is not a hole, and not coming off a
         * hole, then do nothing.
         */
        update_free_space_data(
            nextMapByte, clusterSize,
            currentMapPosition, &inHole,
            &currentHoleSize, &largestHoleSize,
            freeSpaceData, &largestHoleMapPosition);

        if (!nextMapByte) {

            /* Found a hole! */
            /* mark start of window */
            /* get next bmt extent */
            /* exit */

            hasFreeArea = TRUE;
            currentWindowFirst = 0;
            currentWindowLast = 0;
            window[0] = 0;

            /* We don't actually count this byte until the next one is read. */
            currentWindowFreePopulation = 0;
            currentWindowHolePopulation = 0;
            currentWindowSize = 1;

            /* Find the index of the next bmt extent that we will encounter. */
            while (bmtXtntArray[nextBmtXtntIndex].bsed_vd_blk <
                    currentMapPosition * blksRepPerMapByte &&
                   nextBmtXtntIndex < bmtXtntCount) {
                nextBmtXtntIndex++;
            }

            /* exit loop */

            break;
        }

        /* keep going - go get next byte! */
        currentMapPosition++;

    } /* end while TRUE */

/**********  until done with map.                                *************/

    /* Loop to read each map byte searching for the next hole. */

    while (TRUE) {

        /* If the byte we're about to read will cause the last byte
         * in the buffer to overwrite the first, find the next hole.
         * If there are no more holes in the buffer, continue to search
         * ahead until another hole is found.
         */

        if (currentWindowLast + 1 == currentWindowFirst ||
            (currentWindowLast == maxWindowSize - 1 &&
             currentWindowFirst == 0)) {

            err = find_next_hole(
                                window,
                                &currentWindowHolePopulation,
                                &currentWindowFreePopulation,
                                &currentWindowFirst,
                                &currentWindowLast,
                                &currentWindowSize,
                                &maxWindowSize,
                                &lowerExcludeMapPosition,
                                &upperExcludeMapPosition,
                                &sparsestMapFreePopulation,
                                &sparsestMapPosition,
                                &sparsestMapSize,
                                &sparsestAreaWasFound,
                                &greatestHoleMapPopulation,
                                &greatestHoleMapPosition,
                                &greatestHoleMapSize,
                                &greatestHoleAreaWasFound,
                                &mapFreePopulationAtGreatestHoles,
                                &root,
                                &byteWasRead,
                                &nextMapByte,
                                clusterSize,
                                &currentMapPosition,
                                &inHole,
                                &currentHoleSize,
                                &largestHoleSize,
                                freeSpaceData,
                                &largestHoleMapPosition,
                                &nextBmtXtntIndex,
                                bmtXtntArray,
                                &blksRepPerMapByte,
                                &bmtXtntCount
                               );
            if(err == ERROR) {
                errorFlag = TRUE;
                goto quit_find_sparsest;
            } else if(err == MAPDONE)
                goto end_of_map_already_processed;

        }


        if (get_next_map_byte(&root, &byteWasRead, &nextMapByte)) {
            errorFlag = TRUE;
            goto quit_find_sparsest;
        }
        if (!byteWasRead) {
            break;
        }

        update_free_space_data(
            nextMapByte, clusterSize,
            currentMapPosition + currentWindowSize, &inHole,
            &currentHoleSize, &largestHoleSize,
            freeSpaceData, &largestHoleMapPosition);

        /* If the previous byte is free, count it, and see if it is the
         * end of a hole.  If it is, count the hole, and see if the current
         * window represents either the most holes, or the most free space
         * we have seen so far. */
        if (!window[currentWindowLast]) {
            currentWindowFreePopulation++;
            if (nextMapByte) {
                currentWindowHolePopulation++;
                if (!included_count(
                        lowerExcludeMapPosition, 
                        upperExcludeMapPosition,
                        currentMapPosition, 
                        currentMapPosition + currentWindowSize-1)) {

                    if (currentWindowFreePopulation > 
                                           sparsestMapFreePopulation) {

                               /* most free space - update sparsest params */
                        sparsestMapFreePopulation = currentWindowFreePopulation;
                        sparsestMapPosition = currentMapPosition;
                        sparsestMapSize = currentWindowSize;
                        sparsestAreaWasFound = TRUE;
                    }
                    if (currentWindowHolePopulation > 
                         greatestHoleMapPopulation ||
                         (currentWindowHolePopulation == 
                         greatestHoleMapPopulation &&
                         currentWindowHolePopulation == 1 &&
                         currentWindowFreePopulation >
                            mapFreePopulationAtGreatestHoles)) {

                               /* most holes - update holes params */
                        greatestHoleMapPopulation = currentWindowHolePopulation;
                        mapFreePopulationAtGreatestHoles = 
                                           currentWindowFreePopulation;
                        greatestHoleMapPosition = currentMapPosition;
                        greatestHoleMapSize = currentWindowSize;
                        greatestHoleAreaWasFound = TRUE;
                    }
                }
            }
        }

        /* If the byte just read is the start of a bmt extent, go back
         * and start the window at the next hole.
         * We can't migrate out bmt extents so find another suitable window
         * area.
         */
        if (nextBmtXtntIndex < bmtXtntCount &&
            (currentMapPosition + currentWindowSize) * blksRepPerMapByte ==
                bmtXtntArray[nextBmtXtntIndex].bsed_vd_blk) {
            /* Count the byte that's about to be read. */
            currentMapPosition += currentWindowSize + 1;
            goto skip_bmt_extent;
        }

        currentWindowSize++;
        currentWindowLast++;
        if (currentWindowLast == maxWindowSize) {
            currentWindowLast = 0;
        }
        window[currentWindowLast] = nextMapByte;

    }  /* end while still some bytes */


/******************  final update of params ******************************/

    /* Process the final end of the disk; it might be the end of a hole. */

    if (!window[currentWindowLast]) {
        currentWindowFreePopulation++;
        currentWindowHolePopulation++;
        if (!included_count(
                lowerExcludeMapPosition, upperExcludeMapPosition,
                currentMapPosition, currentMapPosition + currentWindowSize - 1)) {
            if (currentWindowFreePopulation > sparsestMapFreePopulation) {
                 /* update free space params */
                sparsestMapFreePopulation = currentWindowFreePopulation;
                sparsestMapPosition = currentMapPosition;
                sparsestMapSize = currentWindowSize;
                sparsestAreaWasFound = TRUE;
            }
            if (currentWindowHolePopulation > greatestHoleMapPopulation ||
                (currentWindowHolePopulation == greatestHoleMapPopulation &&
                 currentWindowHolePopulation == 1 &&
                 currentWindowFreePopulation > 
                                           mapFreePopulationAtGreatestHoles)) {
                 /* update hole params */
                greatestHoleMapPopulation = currentWindowHolePopulation;
                mapFreePopulationAtGreatestHoles = currentWindowFreePopulation;
                greatestHoleMapPosition = currentMapPosition;
                greatestHoleMapSize = currentWindowSize;
                greatestHoleAreaWasFound = TRUE;
            }
        }
    }

end_of_map_already_processed:


    update_free_space_data(
        TRUE, clusterSize,
        currentMapPosition + currentWindowSize, &inHole,
        &currentHoleSize, &largestHoleSize,
        freeSpaceData, &largestHoleMapPosition);

/************** lets see how well we did!   Update filledFraction ************/

    if (hasFreeArea &&
        ((clearStrategy == clearMostHoles && greatestHoleAreaWasFound) ||
         (clearStrategy == clearSparsestArea && sparsestAreaWasFound) ||
         clearStrategy == clearLargestHole)) {
        switch (clearStrategy) {

          case clearMostHoles:
            filledFraction = 1.0 -
                (float)mapFreePopulationAtGreatestHoles / greatestHoleMapSize;
            break;

          case clearSparsestArea:

            filledFraction = 1.0 - 
                         (float)sparsestMapFreePopulation / sparsestMapSize;

            /* See if it would be worthwhile to try a smaller clear area size */


            if (isFirstTry) {
                if (filledFraction > 0.40) {
                    first.filledFraction = filledFraction;
                    first.sparsestMapPosition = sparsestMapPosition;
                    first.sparsestMapFreePopulation = sparsestMapFreePopulation;
                    first.sparsestMapSize = sparsestMapSize;
                    first.maxWindowSize = maxWindowSize;

                    maxWindowSize = maxWindowSize * 2 / 3;
                    fclose(root.mapFile);
                    mapFileIsOpen = FALSE;
                    isFirstTry = FALSE;
                    goto try_smaller_window;
                }

            } else {
                /* second pass
                 * Decide whether to use the small or large (first) window. 
                 * which one worked better?
                 */
                if (filledFraction + 0.20 >= first.filledFraction) {
                    filledFraction = first.filledFraction;
                    sparsestMapPosition = first.sparsestMapPosition;
                    sparsestMapFreePopulation = first.sparsestMapFreePopulation;
                    sparsestMapSize = first.sparsestMapSize;
                    maxWindowSize = first.maxWindowSize;
                }
            }
            break;

          case clearLargestHole:
            filledFraction = 0.0;
            break;
        }
    }


quit_find_sparsest:

/************************************* clean up ************************/
    if (mapFileIsOpen) {
        fclose(root.mapFile);
    }
    if (bufferIsAllocated) {
        free(root.buffer);
    }
    if (windowIsAllocated) {
        free(window);
    }
    if (mapNameIsAllocated) {
        free(root.mapFileName);
    }
    if (bmtNameIsAllocated) {
        free(bmtFileName);
    }

/************************ update strategy params ******************************/

    if (hasFreeArea &&
        ((clearStrategy == clearMostHoles && greatestHoleAreaWasFound) ||
         (clearStrategy == clearSparsestArea && sparsestAreaWasFound) ||
         clearStrategy == clearLargestHole)) {

        switch (clearStrategy) {
          case clearMostHoles:
            *areaSize = greatestHoleMapSize * blksRepPerMapByte;
            *areaPopulation = *areaSize -
                mapFreePopulationAtGreatestHoles * blksRepPerMapByte;
            *lowerBound = greatestHoleMapPosition * blksRepPerMapByte;
            *alreadyClear = 
                    (mapFreePopulationAtGreatestHoles == greatestHoleMapSize);
            break;

          case clearSparsestArea:
            *areaSize = sparsestMapSize * blksRepPerMapByte;
            *areaPopulation = *areaSize -
                sparsestMapFreePopulation * blksRepPerMapByte;
            *lowerBound = sparsestMapPosition * blksRepPerMapByte;
            *alreadyClear = (sparsestMapFreePopulation == sparsestMapSize);
            break;

          case clearLargestHole:
            *areaSize = largestHoleSize * blksRepPerMapByte;
            *areaPopulation = *areaSize;
            *lowerBound = largestHoleMapPosition * blksRepPerMapByte;
            *alreadyClear = TRUE;
            break;
        }

    } else {
        *areaSize = 0;
        *areaPopulation = 0;
        *lowerBound = 0;
        *alreadyClear = FALSE;
    }


    if (errorFlag) {
        return 1;
    } else {
        return 0;
    }

} /* end find_sparsest_area */

/*
 * get_next_map_byte()
 *
 * Reads the next byte in the buffer.  If the buffer has been completely
 * read then get the next page of the storage bitmap for reading.
 *
 * return  byteWasRead = TRUE if we were able to read the next byte.
 *         nextMapByte points to the next byte gotten.
 */
static
int
get_next_map_byte(
            findSparsestAreaRootT *root_ptr,
            boolean *byteWasRead,
            char *nextMapByte)
{
    int attemptedReadCount;
    int actualReadCount;

    if (root_ptr->bufferPosition >= root_ptr->bytesInBuffer)
    {

     /* need another pages worth of the storage bitmap read into memory */

        if (root_ptr->remainingBytesToRead == 0) {
            /* no more bytes in storage map - end of bitmap - exit */
            *byteWasRead = FALSE;
            return 0;
        }

        /* if not a whole page left get whats remaining  */
        if (root_ptr->remainingBytesToRead < root_ptr->buffer_size)
            attemptedReadCount = root_ptr->remainingBytesToRead;
        else
            attemptedReadCount = root_ptr->buffer_size;

        /* keep track of how many are left to read */
        root_ptr->remainingBytesToRead -= attemptedReadCount;

        /* read them in */
        actualReadCount = fread(
                   root_ptr->buffer, 1, attemptedReadCount, root_ptr->mapFile);

        if (actualReadCount != attemptedReadCount)
        {
          /* whoops - didn't get what we wanted */
            fprintf (stderr, CMSG(COMMON_READERR1, 
                     "%s: Error reading from bitmap file: %s\n"),
                     Prog, root_ptr->mapFileName);
            return 1;
        }
        /* position to the end of the new page's header */
        root_ptr->bufferPosition = HEADER_SIZE;
        root_ptr->bytesInBuffer = actualReadCount;
    }

    /* get the next byte, keep track of where we are */
    *byteWasRead = TRUE;
    *nextMapByte = root_ptr->buffer[root_ptr->bufferPosition];
    root_ptr->bufferPosition++;  /* walk through one integer at a time. */
    return 0;

} /* end get_next_map_byte */


/*
 * xtnt_in_range
 *
 * Return TRUE if the indicated extent is in one of the ranges specified
 * to be cleared in volData.
 * NOTE - return FALSE for extents that are over 3/4 of the size of the
 * area to be cleared; moving such an extent in the clear phase might
 * split it up so that it could not be made better again in the fill
 * phase.
 */
static
boolean
xtnt_in_range(
        volDataT *volData, /* in */
        uint64_t bsed_vol_index, /* in */
        uint64_t bsed_vd_blk, /* in */
        int xtntBlkCount  /* in */
        )
{
    return (bsed_vol_index == volData->bsed_vol_index &&
            bsed_vd_blk >= volData->lowerBlkClearBound &&
            bsed_vd_blk <= volData->upperBlkClearBound &&
            volData->doFill &&
            xtntBlkCount <= volData->clearAreaSize * 3 / 4);
} /* end xtnt_in_range */

/* Return the amount of the minor interval that is also contained in the major
 * interval. 
 */
static
int
included_count(
        int majorLower,  /* in */
        int majorUpper,  /* in */
        int minorLower,  /* in */
        int minorUpper   /* in */
        )
{
    int lower, upper;

    if (minorLower >= majorUpper || minorUpper <= majorLower) {
        /* No intersection. */
        return 0;
    }

    if (minorLower > majorLower) {
        lower = minorLower;
    } else {
        lower = majorLower;
    }

    if (minorUpper < majorUpper) {
        upper = minorUpper;
    } else {
        upper = majorUpper;
    }

    return upper - lower + 1;
 
} /* end included_count */

static
int
get_system_time(
        int *result  /* out */
        )
{
    struct timeval tempTime;
    struct timezone tempZone;
    int err;

    err = gettimeofday(&tempTime, &tempZone);
    if (err) {
        fprintf(stderr, MSG(DF_BADSYSTIME, 
                "%s: unable to get system time\n"), Prog);
        return ERROR;
    } 

    *result = tempTime.tv_sec;
    return OKAY;

} /* end get_system_time */


/*
 * Determine how much free space,volume size there is on each volume.
 */
static
int
update_volume_data(
        adv_bf_dmn_params_t *dmnParams,  /* in */
        volDataT *volData)      /* in/out */
{
    adv_vol_counters_t volCounters;
    adv_vol_info_t volInfo;
    adv_vol_ioq_params_t volIoQParams;
    adv_vol_prop_t volProperties;
    int i;
    adv_status_t sts = EOK;

    /*
     * Determine how much free space there is on this volume.
     */

    sts = advfs_get_vol_params(
                dmnParams->bfDomainId,
                volData->bsed_vol_index, 
                &volInfo,
                &volProperties,
                &volIoQParams,
                &volCounters);

    if (sts != EOK) {
            fprintf(stderr, CMSG(COMMON_VOLPARAMS_ERR, 
                    "%s: get vol params error %s\n"), Prog, BSERRMSG(sts));
            fflush(stdout);
            return 1;
    }

    volData->volSize = volInfo.volSize;
    volData->volFreeSize = volInfo.freeClusters * volInfo.sbmBlksBit;
    volData->volStgCluster = volInfo.sbmBlksBit;
    volData->freeSizeFract = (float)volData->volFreeSize /
            volData->volSize;
    volData->wrMaxIo = volIoQParams.wrMaxIo;

    volData->dmnFreeSize = volData->volFreeSize;
    volData->dmnFreeSizeFract = (float)volData->dmnFreeSize / volData->volSize;

    return OKAY;

} /* end update_volume_data */


/*
 * non_uniform_payoff_boost
 *
 * This routine, which should be called only for large files, returns a factor
 * by which the fileMovePayoff of a "non-uniform" file should be increased.
 * The idea is that if a file has small extents and large ones, we can probably
 * realize a greater payoff by migrating a part of it in the fill phase (see
 * find_xtnt_run) than the payoff would lead us to believe.  So, we want to 
 * boost the payoff of files that have a large difference in largest and 
 * smallest extent size, especially if they have many small ones.
 */
static
float
non_uniform_payoff_boost(
        int largestXtnt,  /* in */
        int smallestXtnt,  /* in */
        int averageXtnt,  /* in */
        boolean wasCleared)  /* in */
{
    float sizeRatio = largestXtnt / smallestXtnt;

    if (sizeRatio < 10) {
        return 1.0;
    } else {
        float boost;
        float midrangeSize = (largestXtnt + smallestXtnt) / 2;

        boost = sizeRatio / 8.0;
        if (boost > 3.0) {
            boost = 3.0;
        }
        /* This has the effect of multiplying by 1.0 if average==largest;
         * and by 3.0 if average==smallest. */
        boost *= 2.0 - 
                 2.0 * (averageXtnt-midrangeSize) / (largestXtnt-smallestXtnt);

        /* Further boost the payoff if the file was just moved in the clear 
         * phase.  If it was moved, chances are its extent count just 
         * increased, so we want to give the defragmenter a chance to re-lower
         * its extent count. 
         */
        if (wasCleared) {
            boost *= 2.0;
        }
        return boost;
    }
    
} /* end non_uniform_payoff_boost */

/*
 * This routine is used to print the histogram ranges for the
 * free space table.
 */
static
void
range_print (FILE *f, char *format_in, int range_in)
{
    char s[100];
    char format[100];
    int i;
    int range;

    strcpy(format, format_in);

    if (abs(range_in) >= 1000) {
        range = range_in / 1000;
        for (i = 0; format[i] != '\0'; i++) {
            if (format[i] == 'K') {
                format[i] = 'M';
                break;
            }
        }
    } else {
        range = range_in;
    }

    sprintf(s, format, range);

    for (i = 0; s[i] != '\0'; i++) {
        if (s[i] == '-') {
            s[i] = '<';
            break;
        } else if (s[i] == '+') {
            s[i] = '>';
            break;
        }
    }
    fprintf(f, "%s", s);
    (void)fflush(f);

} /* end range_print */	



/*
 * This is the defragment signal handler.  It gives this program a chance
 * to cleanup before quiting.  Specifically, it unmounts bitfile sets
 * and unlocks the filesystem.
 */

static
void 
sigHandler (
                 int signal  /* in */
                 )

{

    int err;
    adv_status_t sts = EOK;

    fprintf (stderr, MSG(DF_ERRSTOP, 
             "%s: Stopping defragment operation...\n"), Prog);

    switch (signal) {

      case SIGINT:
      case SIGQUIT:
      case SIGPIPE:
      case SIGTERM:
        break;

      default:
        fprintf (stderr, CMSG(COMMON_UKNSIG, 
                           "%s: Unexpected signal: %d\n"), Prog, signal);
        break;
    }  /* end switch */

    (void) close_dot_tags (Prog);

    fprintf (stderr, MSG(DF_NOTDONE, 
        "%s: Domain '%s' not defragmented\n"),
        Prog, infop->fsname); 

    program_exit_status = 1;
    defragment_exit();

}  /* end sigHandler */


void
defrag_usage (void)
{
    usage (catgets(catd,
                   MS_FSADM_DEFRAG,
                   DF_USAGE,
                   "%s [-V] [-L] [-e filename] [-w time] {special|fsname}\n"), Prog);
}


/*
 * retrieve the filesystem parameter information and mount the bitfiles,
 * set up structures to hold the volume information.
 *
 */
static
int 
open_domain (
    volDataT **retvolData,                     /* out */
    volFreeSpaceDataT **retvolFreeSpaceData,   /* out */
    adv_bf_dmn_params_t *dmnParams,                /* in/out */
    volPassStatT **retvolPassStat           /* out */
)
{
    int i, j, err;
    struct stat stats;
    adv_status_t sts = EOK;
    int max_vdi;
    uint32_t *volIndexArray = NULL;              /* local use */
    volDataT *volData = NULL;                    /* local use */
    volPassStatT *volPassStat = NULL;            /* local use */
    volFreeSpaceDataT *volFreeSpaceData = NULL;  /* local use */

    /*
     * Lock this domain.
     */

    thisDmnLock = advfs_fspath_longrun_lock (infop->fspath,
                                            FALSE,
                                            TRUE,
                                            &err);

    if (thisDmnLock < 0) {
        if (err == EWOULDBLOCK) {
            abort_prog( MSG(DF_EEXEC, 
                        "%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified domain.\n"), Prog);
        } else {
            abort_prog( CMSG(COMMON_ERRLOCK2, 
                        "%s: Error locking '%s'\n"),
                        Prog, infop->fspath);
        }
    }
    advfs_post_user_event(EVENT_FDMN_DEFRAG_LOCK, advfs_event, Prog);

    /* get the number of volumes */

    sts = advfs_get_dmnname_params (infop->fsname, dmnParams);
    if (sts != EOK) {
        abort_prog ( MSG(DF_DOMP, 
                     "%s: Cannot get storage domain parameters\n"), Prog);
    }

    /* set up global structures */
    bf_criteria1_list_start = (bfListEntryT **) malloc(dmnParams->curNumVols * 
                                                 sizeof(bfListEntryT *));
    if(bf_criteria1_list_start == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    bf_criteria1_list_last = (bfListEntryT **) malloc(dmnParams->curNumVols * 
                                                 sizeof(bfListEntryT *));
    if(bf_criteria1_list_last == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    bf_criteria2_list_start = (bfListEntryT **) malloc(dmnParams->curNumVols * 
                                                 sizeof(bfListEntryT *));
    if(bf_criteria2_list_start == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    bf_criteria2_list_last = 
             (bfListEntryT **) malloc(dmnParams->curNumVols * 
                                                 sizeof(bfListEntryT *));
    if(bf_criteria2_list_last == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    volIndexArray = (uint32_t*)malloc(dmnParams->curNumVols * sizeof(uint32_t));
    if(volIndexArray == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    volData = (volDataT *)malloc(dmnParams->curNumVols * sizeof(volDataT));
    if(volData == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    volFreeSpaceData = 
        (volFreeSpaceDataT *)malloc((dmnParams->curNumVols) *
                                                    sizeof(volFreeSpaceDataT));
    if(volFreeSpaceData == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    AllvolFreeSpaceData = 
        (volFreeSpaceDataT *)malloc(sizeof(volFreeSpaceDataT));
    if(AllvolFreeSpaceData == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    /* 
     * initialize
     */

    for (j=0; j<FREE_RANGE_COUNT; j++) {
        AllvolFreeSpaceData->rangeData[j].holeCount=0;
        AllvolFreeSpaceData->rangeData[j].freeSpace=0;
    }
    AllvolFreeSpaceData->totalHoleCount = 0;
    AllvolFreeSpaceData->totalFreeSpace = 0;

    AllvolData = (AllvolDataT *)malloc(sizeof(AllvolDataT));
    if(AllvolData == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    volPassStat = 
       (volPassStatT *)malloc(dmnParams->curNumVols * sizeof(volPassStatT));
    if(volPassStat == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }
    /* Get the domain's parameters and a list of its volumes. */

    sts = advfs_get_dmn_vol_list (
                                 dmnParams->bfDomainId, 
                                 dmnParams->curNumVols, 
                                 volIndexArray,
                                 &volIndexCnt
                                 );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, MSG(DF_EUNMNT, 
                 "%s: Error: All filesets must be mounted\n"), Prog );
        goto _error;
    } else if (sts != EOK) {
        fprintf (stderr, MSG(DF_EDVL, 
                 "%s: Cannot get domain's volume list\n"), Prog);
        fprintf (stderr, CMSG(COMMON_ERR, 
                  "%s: Error = %s\n"), Prog, BSERRMSG (sts));
        goto _error;
    }


    /* assign volume indices - without the "L" log designator */

    for (i = 0; i < volIndexCnt; i++) {
        volData[i].bsed_vol_index = volIndexArray[i];
        volData[i].passNumber = 1;
        volData[i].volSize = 0;
        volData[i].volStgCluster = 0;           /* number of blocks each bit represents */
        volData[i].volFreeSize = 0;             /*  amount of free space left on this volume */
        volData[i].freeSizeFract = 0;          /* percentage of free space left on volume */
        volData[i].lowerBlkClearBound = 0;      /* lower boundary (in blks) of area to be cleared */
        volData[i].upperBlkClearBound = 0;      /* upper boundary (in blks) of area to be cleared */
        volData[i].clearAreaSize = 0;           /* initial to-be-cleared area setting */
        volData[i].actualClearedAreaSize = 0;   /* actual cleared area setting after clearing*/
        volData[i].alreadyClear = 0;         /* flag to indicate that volume is not to be cleared */
        volData[i].doFill = 0;               /* flag to indicate that volume is too full to be */
                                        /* worth trying to do a clear/fill on it. */
        volData[i].remainingClearAreaSize = 0;  /* keeps track of the amount remaining that needs */
                                       /* to be filled still.                          */
        volData[i].isEligibleTarget = 0;     /* once clear area is filled up, this is the flag */
                                        /* that indicates that no more filling is to be done*/
        volData[i].wrMaxIo = 0;  /* used in calculating the total IO rating for this volume. */
                                    /* max blocks that can be read/written in a consolidated I/O */
        volData[i].lowerBlkRarifyBound = 0; /* lower boundary of the area to be used in filling in */
                                    /* anticipation of the next pass using this area for clearing. */
        volData[i].upperBlkRarifyBound = 0; /* upper boundary of the area to be used in filling in */
                                    /* anticipation of the next pass using this area for clearing. */
        volData[i].startPassTime = 0;
        volData[i].endPassTime = 0;
        volData[i].averageExtentCount = 0;
        volData[i].lastAverageExtentCount = 0;
        volData[i].bestAverageExtentCount = 0;
        volData[i].totalHoleCount = 0;
        volData[i].unimprovedScore = 0;
        volData[i].areaPopulation = 0;
        volData[i].clearLimitTime = 0;
        volData[i].extentCount = 0;
        volData[i].fileWithExtCount = 0;
        volData[i].totalIOCount = 0;
        volData[i].totalBestIOCount = 0;
        volData[i].threshholdPerf = 0;
        volData[i].basicClearStrategy = 0;
        volData[i].shortenedClearPass = 0;
        volData[i].aggregatePerf = 0;
        volData[i].stopDefragment = 0;
        volData[i].dmnFreeSize = 0;
        volData[i].dmnFreeSizeFract = 0;

        for (j=0; j<FREE_RANGE_COUNT; j++) {
            volFreeSpaceData[i].rangeData[j].holeCount=0;
            volFreeSpaceData[i].rangeData[j].freeSpace=0;
        }
        volFreeSpaceData[i].totalHoleCount = 0;
        volFreeSpaceData[i].totalFreeSpace = 0;

    }

    AllvolData->averageExtentCount = 0;
    AllvolData->extentCount = 0;
    AllvolData->fileWithExtCount = 0;
    AllvolData->shortenedClearPass = 0;
    AllvolData->aggregatePerf = 0;
    AllvolData->totalIOCount = 0;
    AllvolData->totalBestIOCount = 0;
    AllvolData->Othervols_extentCount = 0;
    AllvolData->Othervols_fileWithExtCount = 0;
    AllvolData->Othervols_totalIOCount = 0;
    AllvolData->Othervols_totalBestIOCount = 0;

    for (j=0; j<FREE_RANGE_COUNT; j++) {
        AllvolData->Othervols_freeSpace.rangeData[j].holeCount=0;
        AllvolData->Othervols_freeSpace.rangeData[j].freeSpace=0;
    }
    AllvolData->Othervols_freeSpace.totalHoleCount=0;
    AllvolData->Othervols_freeSpace.totalFreeSpace=0;

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
        fprintf (stderr, MSG(DF_NOMEM, 
                 "%s: Cannot allocate memory\n"), Prog);
        goto _error;
    }

    /* initialize the array */
    for (i = 0; i <= max_vdi; i++) {
        volSubscrArray[i] = -1;
    }
    for (i = 0; i < volIndexCnt; i++) {
        volSubscrArray[volIndexArray[i]] = i;
    }

    /*
     * Mount all bitfile sets in the domain.  The function also opens each
     * bitfile set's ".tags" directory to ensure that the set stays mounted
     * while the domain is defragmented.
     */

    err = open_dot_tags (infop, dmnParams->bfSetDirTag,
                         &metadataRootPathName, Prog);
    if (err) {
        fprintf (stderr, MSG(DF_NOMOUNT,
                    "%s: File system mount failure\n"), Prog);
        goto _error;
    }

    /* assign structures to passed in pointer for use by calling program */
    *retvolData = volData;
    *retvolPassStat = volPassStat;
    *retvolFreeSpaceData = volFreeSpaceData; 

    if(volIndexArray)    free(volIndexArray);
    return( dmnParams->curNumVols );

_error:;

    if(volPassStat)    free(volPassStat);
    if(bf_criteria2_list_start)    free(bf_criteria2_list_start);
    if(bf_criteria2_list_last)    free(bf_criteria2_list_last);
    if(volIndexArray)    free(volIndexArray);
    if(volData)          free(volData);
    if(volFreeSpaceData) free(volFreeSpaceData);
    if(AllvolFreeSpaceData) free(AllvolFreeSpaceData);
    if(volSubscrArray)   free(volSubscrArray);

    abort_prog( MSG(DF_NOVOLSFOUND, "%s: Unable to open storage domain %s.\n"), 
                        Prog, infop->fspath);

} /* end open_domain */


static
void
abort_prog(
          char *msg, ...
          )
{
  int err;
  va_list ap;

    (void) close_dot_tags (Prog);

    fprintf(stderr, MSG(DF_CANTDEF, 
        "%s: Cannot defragment domain '%s'\n"),
        Prog, infop->fsname);

    va_start( ap, msg );
    fprintf( stderr, MSG(DF_UTIL1, 
             "\n\n************* PROGRAM ABORT **************\n\n") );
    vfprintf( stderr, msg, ap );
    fprintf( stderr, "\n\n" );
    va_end( ap );

    fflush( stderr );
    program_exit_status = 3;
    defragment_exit();
}



static
boolean
stop_pass_check(
        volDataT *volData
         )
{

    /* Terminate the defragmentation if there have been more than a
     * fixed number of passes that did not improve fragmentation. */
    if (volData->unimprovedScore > MAXIMUM_UNIMPROVED_SCORE_HIGH ||
        (volData->passNumber > 1 &&
         volData->averageExtentCount < AVERAGE_EXTENT_COUNT_GOAL &&
         volData->unimprovedScore > MAXIMUM_UNIMPROVED_SCORE_LOW)) {

        return(VOLDONE);
    }

    volData->lastAverageExtentCount = volData->averageExtentCount;

return OKAY;
}  /* end stop_pass_check */


static
boolean
set_time_limit(int passNumber,
               int *startPassTime,
               int *endPassTime,
               int *clearLimitTime,
               volDataT *volData
              )
{
    int err;


    /* Get a time for the end of the last pass/start of this pass. */
    err = get_system_time(endPassTime);
    if (err) {
        return(ERROR);
    }

    /* Assuming that this pass takes as much time as the last pass if
     * there's a strict time limit, or slightly less if there isn't,
     * see if there is enough time left. */

    if(passNumber > 1) {

        float f = (hasStrictTimeLimit ? 1.0 : 0.80);

        if (*endPassTime + f * (*endPassTime - *startPassTime) > limitTime) {
            fprintf(stderr, MSG(DF_OUTOFTIME,
	        "%s: unable to complete pass %d on volume %d in remaining time.\n"),
                Prog, passNumber, volData->bsed_vol_index);
	    volData->stopDefragment = TRUE;
        }
    }


    if( passNumber > 0) {

       *startPassTime = *endPassTime;

       /* If there is a strict time limit, impose a limit on the clear
        * phase so that it leaves 1/3 of the remaining time for filling. */

       if (hasStrictTimeLimit) {
           *clearLimitTime = limitTime - (limitTime - *startPassTime) / 3;
        }
    } else {    /* passNumber == 0 */
       *clearLimitTime = limitTime;
    }

return OKAY;

} /* end set_time_limit */


/* select a clear strategy - (performed once each loop )
 *
 * choices are:
 *  a. clearMostHoles -  clear the area that has the greatest number of
 *                       free space fragments.  The advantage of this 
 *                       strategy is that it helps the defragmentor 
 *                       achieve its secondary goal of consolidating the
 *                       free space fragments into a contiguous free area.
 *
 *  b. clearSparsestArea - Clear the area that is the most sparsely populated.
 *                         The advantage is that an area can be cleared quickly
 *                         since the number of extents that need to be migrated
 *                         are minimal.
 */
static
void
select_clear_strategy(int passNumber, 
                      volDataT *volData
                      )
{

    /* first pass always try to consolidate the free space fragments */
    if (passNumber == 1) {
        volData->basicClearStrategy = clearMostHoles;
    } else {

        /* if the average extents per file is less than defined value OR
         * defragmenter may be towards end of the passes consolidate the 
         * free space.  This is done for performance reasons.
         */
        if (volData->averageExtentCount < BEGIN_REDUCE_HOLES_RATIO || 
            volData->unimprovedScore > 0) {
            volData->basicClearStrategy = clearMostHoles;
        } else {

          /* if average extents per file is still high then alternate 
           * strategies on each pass. 
           */
            if (volData->basicClearStrategy == clearMostHoles) {
                volData->basicClearStrategy = clearSparsestArea;
            } else {
                volData->basicClearStrategy = clearMostHoles;
            }
        }

    } /* end if passNumber == 1 */

return ;

} /* end select_clear_strategy */



/* For each volume, determine if it needs clearing and filling
 * (subsequently, we may discover that it doesn't really need clearing,
 * because there may be a big enough area that is already clear).  If it does,
 * determine where this area should be and then reset its free space
 * cache to start immediately after the area to be cleared.
 */

static
boolean
reset_clear_point( volDataT *volData,
                   volFreeSpaceDataT *volFreeSpaceData,
                   uint64_t *areaPopulation,                   /* in/out */
                   int passNumber,
                   int unimprovedScore,
                   adv_bf_dmn_params_t *dmnParams,
                   double averageExtentCount
                 )
{
 int i;
 int err;
 adv_status_t sts = EOK;

    /* for this volume - is there enough free space? */
    if (volData->freeSizeFract < volData->dmnFreeSizeFract - 0.80 ||
        (volData->freeSizeFract < 0.20 * MINIMUM_FREE_SIZE_FRACTION &&
         volData->volFreeSize < 0.20 * MINIMUM_VOLUME_FREE_BLOCKS)) {
        volData->doFill = FALSE;
        /* The following is needed by find_sparsest_area, even if
         * no clearing is to be done. 
         */
        if (volData->volFreeSize < volData->volStgCluster * 8) {
            fprintf(stderr, MSG(DF_NOSPACE,
                       "%s: Insufficient free space for defragmenting volume %d.\n"),
                       Prog, volData->bsed_vol_index);
            exit(1);
        } else {
            volData->clearAreaSize = volData->volFreeSize;
        }
    } else {
        volData->doFill = TRUE;
        volData->clearAreaSize = volData->volFreeSize * CLEAR_SIZE_FRACTION;
    }

    if (volData->doFill || displayFreeSpaceFlag) {
        uint64_t rarifyAreaSize = volData->clearAreaSize * RARIFY_AREA_RATIO;

        err = find_sparsest_area(
                            volData->bsed_vol_index,
                            volData->volSize,
                            volData->volStgCluster,
                            !volData->doFill,
                            0,
                            0,
                            volData->basicClearStrategy,
                            dmnParams,
                            &volData->clearAreaSize,
                            areaPopulation,
                            &volData->lowerBlkClearBound,
                            &volData->alreadyClear,
                            volFreeSpaceData,
                            volData);
        if (err)
            return(ERROR);

        volData->upperBlkClearBound =
                    volData->lowerBlkClearBound + volData->clearAreaSize - 1;

        if (volData->upperBlkClearBound > volData->volSize - 1) {

            volData->upperBlkClearBound = volData->volSize - 1;
            volData->clearAreaSize = 1 +
                     volData->upperBlkClearBound - volData->lowerBlkClearBound;
        }

        volData->lowerBlkRarifyBound = 0;
        volData->upperBlkRarifyBound = 0;

        if (rarifyAreaSize <=
           (volData->volSize - volData->clearAreaSize) / 2 &&
            (passNumber == 1 ||
            (averageExtentCount >= BEGIN_REDUCE_HOLES_RATIO && 
             unimprovedScore == 0))) {

            uint64_t dummy1;
            boolean dummy2;

            err = find_sparsest_area(
                                volData->bsed_vol_index,
                                volData->volSize,
                                volData->volStgCluster,
                                TRUE,
                                volData->lowerBlkClearBound,
                                volData->upperBlkClearBound,
                                clearMostHoles,
                                dmnParams,
                                &rarifyAreaSize,
                                &dummy1,
                                &volData->lowerBlkRarifyBound,
                                &dummy2,
                                NULL,
                                volData);
            if (err)
                return(ERROR);

            volData->upperBlkRarifyBound = 
                       volData->lowerBlkRarifyBound + rarifyAreaSize;

        }

        /* If the volume doesn't need clearing, then the actual cleared
         * size is the same as the to-be-cleared size.  If it does need
         * clearing, we need to keep track of the actual cleared size,
         * in the process of clearing, so initialize it here to the
         * count of already cleared blocks. 
         */
        if (volData->alreadyClear) {
            volData->actualClearedAreaSize = volData->clearAreaSize;
        } else {
            volData->actualClearedAreaSize = 
                       volData->clearAreaSize - *areaPopulation;
        }

        if (volData->upperBlkClearBound > volData->volSize - 1) {
            sts = advfs_reset_free_space_cache(
                            dmnParams->bfDomainId,
                            volData->bsed_vol_index,
                            0);
        } else {
            sts = advfs_reset_free_space_cache(
                            dmnParams->bfDomainId,
                            volData->bsed_vol_index,
                            (volData->upperBlkClearBound + 1) /
                                volData->volStgCluster);
        }

        if (sts != EOK) {
            return(ERROR);
        }

    } /* end if doFill==TRUE or display the free space for this volume */

return OKAY;

} /* end reset_clear_point */


/* update_stats
 *
 * update the volumes free space and hole data.  
 * Determine if another pass is warranted.
 */
static
void 
update_stats(volFreeSpaceDataT *volFreeSpaceData, 
             int passNumber,
             volDataT *volData,
             int volume
             )
{
    int i;
    int range;
    double lastAverageExtentCount;
    uint64_t lastTotalHoleCount;

        lastAverageExtentCount = volData->averageExtentCount;
        if (volData->fileWithExtCount == 0) {
            volData->averageExtentCount = 1.0;
        } else {
            volData->averageExtentCount = (double)volData->extentCount / 
                                          (double)volData->fileWithExtCount;
        }
        if (volData->totalIOCount == 0) {
            volData->aggregatePerf = 100.0;
        } else {
            volData->aggregatePerf = (double)volData->totalBestIOCount / 
                                     (double)volData->totalIOCount * 100.0;
        }
        if (volData->averageExtentCount < 1.0) { /* shouldn't ever happen */
            volData->averageExtentCount = 1.0;
        }
        volData->threshholdPerf = pow(volData->aggregatePerf, 0.70);
        if (volData->threshholdPerf > volData->aggregatePerf) {
            volData->threshholdPerf = volData->aggregatePerf;
        }

        /* update the global data here */

        pthread_mutex_lock(&Allvol_mutex );

        if(volData->shortenedClearPass) AllvolData->shortenedClearPass = TRUE;

        AllvolData->extentCount      += volData->extentCount;
        AllvolData->fileWithExtCount += volData->fileWithExtCount;
        AllvolData->totalIOCount     += volData->totalIOCount;
        AllvolData->totalBestIOCount += volData->totalBestIOCount;

        /* add this volumes values to the total */
        for (range = 0; range < FREE_RANGE_COUNT; range++) {
            AllvolFreeSpaceData->rangeData[range].freeSpace +=
                        volFreeSpaceData->rangeData[range].freeSpace;
            AllvolFreeSpaceData->rangeData[range].holeCount +=
                        volFreeSpaceData->rangeData[range].holeCount;
            AllvolFreeSpaceData->totalFreeSpace +=
                        volFreeSpaceData->rangeData[range].freeSpace;
            AllvolFreeSpaceData->totalHoleCount +=
                        volFreeSpaceData->rangeData[range].holeCount;
        }

        lastTotalHoleCount = AllvolFreeSpaceData->totalHoleCount;
        volData->totalHoleCount = AllvolFreeSpaceData->totalHoleCount;

        pthread_mutex_unlock(&Allvol_mutex );

        pthread_mutex_lock(&volPassStat_mutex );
        volPassStat[volume].curPass +=1;
        pthread_mutex_unlock(&volPassStat_mutex );

        /* end global data update */

        /* Determine if this pass reduced the fragmentation. */
        if (passNumber != 1) {
            if (volData->averageExtentCount >= lastAverageExtentCount) {
                /* average extent count is worse or equal to last pass */
                (volData->unimprovedScore) += 2;
                if (volData->totalHoleCount >= lastTotalHoleCount) {
                  /* total free fragment count is worse or equal to last pass */
                    volData->unimprovedScore++;
                }
            } else 
             if (volData->averageExtentCount * 1.0002 >= 
                                              lastAverageExtentCount) {
                /* average extent count has not improved much over last pass */
                volData->unimprovedScore++;
            }
            if (volData->averageExtentCount < volData->bestAverageExtentCount) {
                /* average extent count has is better than ever */
                volData->bestAverageExtentCount = volData->averageExtentCount;
                volData->unimprovedScore /= 2;
            }
        } else { /* passNumber == 1 */
            /* the best average extent count needs initialized */
            volData->bestAverageExtentCount = volData->averageExtentCount;
        }


    return ;
} /* end update_stats */


/* save_vols_end_stats ()
 *
 * Thread has detected that volume needs no more defragmentation.
 * Update the counters for report(verbal) output.  The two counters are
 * the active volumes counters and the completed volumes counters.
 *
 */
static
void 
save_vols_end_stats(volDataT *volData,
                    volFreeSpaceDataT *volFreeSpaceData,
                    int volume)
{
    int range;

    pthread_mutex_lock(&Allvol_mutex );

    /*
     * Update the counters for the cummulative values for all volumes
     * that have completed.  These values are retained until program
     * completion. They are only updated as volumes complete their
     * defragmentation for good.
     */
    AllvolData->Othervols_extentCount += volData->extentCount;
    AllvolData->Othervols_fileWithExtCount += volData->fileWithExtCount;
    AllvolData->Othervols_totalIOCount += volData->totalIOCount;
    AllvolData->Othervols_totalBestIOCount += volData->totalBestIOCount;
    for(range = 0; range < FREE_RANGE_COUNT; range++) {
        AllvolData->Othervols_freeSpace.rangeData[range].holeCount +=
                            volFreeSpaceData->rangeData[range].holeCount;
        AllvolData->Othervols_freeSpace.rangeData[range].freeSpace +=
                            volFreeSpaceData->rangeData[range].freeSpace;
        AllvolData->Othervols_freeSpace.totalFreeSpace +=
                            volFreeSpaceData->rangeData[range].freeSpace;
        AllvolData->Othervols_freeSpace.totalHoleCount +=
                            volFreeSpaceData->rangeData[range].holeCount;
    }

    /*
     * now subtract the counts from the global active counters
     * Active counters keep track of the cumulative values for volumes
     * that have not completed.  It is true that the volume calling
     * this procedure has completed.
     *
     * These counters are decremented since for this current pass the
     * counters were already incremented.  Since we just added them to
     * the completed counters directly above we need to subtract them
     * from the active counters.
     */
    AllvolData->extentCount -= volData->extentCount;
    AllvolData->fileWithExtCount -= volData->fileWithExtCount;
    AllvolData->totalIOCount -= volData->totalIOCount;
    AllvolData->totalBestIOCount -= volData->totalBestIOCount;

    for (range = 0; range < FREE_RANGE_COUNT; range++) {
        AllvolFreeSpaceData->rangeData[range].freeSpace = 0;
        AllvolFreeSpaceData->rangeData[range].holeCount = 0;
        AllvolFreeSpaceData->totalFreeSpace = 0;
        AllvolFreeSpaceData->totalHoleCount = 0;
    }

    pthread_mutex_unlock(&Allvol_mutex );

}


static
void
print_stats(tpool_t *tpoolp)
{
    int i,j,k;
    int range;
    boolean somebody_did_some_work;


    pthread_mutex_lock(&volPassStat_mutex );

wait_more:

    somebody_did_some_work = FALSE;

    for(j=0; j<num_vols; j++) {

        if( (volPassStat[j].volDone == FALSE) ||
	    !somebody_did_some_work) {
            /* if there are any threads still active, we need to keep going */

            tpoolp->print_stats_thread_sleeping = TRUE;
            pthread_cond_wait(&passCheck_cv,&volPassStat_mutex);
            tpoolp->print_stats_thread_sleeping = FALSE;
            tpoolp->print_stats_signal_sent = FALSE;

            /* loop through threads and see if we need to print pass results */
            for(i=0; i<num_vols; i++) {
                if(((volPassStat[i].work_in_progress == FALSE) && 
                    (curPassNumber+1 == volPassStat[i].curPass)) ||

                    (curPassNumber+1 < volPassStat[i].curPass) ||

                    (volPassStat[i].volDone == TRUE) ) {

                     /* volume is ready to print - check next volume */
                     continue;
                } else {
                     /* this volume is not done with current pass yet */
                     goto wait_more ;
                }

            }

            /* fell through - means all volumes are ready to print */

            /* loop through threads and see if we need to print pass verbals */
            for(i=0; i<num_vols; i++) {
                if(volPassStat[i].not_enough_space == FALSE)
                     somebody_did_some_work = TRUE;
            }

            pthread_mutex_lock(&Allvol_mutex );

            if(somebody_did_some_work == TRUE && displaySummary) {
                if (AllvolData->shortenedClearPass) {
                    fprintf(stdout, MSG(DF_NOTIME, 
                           "  There was insufficient time to gather data.\n"));
                    (void)fflush(stdout);
                }

                fprintf(stdout, MSG(DF_PSTAT1, 
                    "  Domain data as of the start of this pass:\n"));
                (void)fflush(stdout);
    
                /* calculate averages  - no divide by 0 */

                if ((AllvolData->fileWithExtCount + 
                     AllvolData->Othervols_fileWithExtCount) == 0) {
                    AllvolData->averageExtentCount = 1.0;
                } else {
                    AllvolData->averageExtentCount = 
                              ((double)AllvolData->extentCount + 
                               (double)AllvolData->Othervols_extentCount)   / 
                              ((double)AllvolData->fileWithExtCount + 
                               (double)AllvolData->Othervols_fileWithExtCount);
                }

                if (AllvolData->totalIOCount + 
                                  AllvolData->Othervols_totalIOCount == 0) {

                    AllvolData->aggregatePerf = 100.0;

                } else {

                    AllvolData->aggregatePerf = 
                              (((double)AllvolData->totalBestIOCount +
                               (double)AllvolData->Othervols_totalBestIOCount) / 
                              ((double)AllvolData->totalIOCount + 
                               (double)AllvolData->Othervols_totalIOCount))
                               * 100.0;
                }


                /* print verbose pass stuff */
                fprintf(stdout, MSG(DF_PSTATEX, 
                        "    Extents:               %8d\n"), 
                        AllvolData->extentCount + 
                               AllvolData->Othervols_extentCount);

                fprintf(stdout, MSG(DF_PSTATFEX, 
                        "    Files w/extents:       %8d\n"), 
                        AllvolData->fileWithExtCount + 
                               AllvolData->Othervols_fileWithExtCount);

                fprintf(stdout, MSG(DF_AVGEXT, 
                        "    Avg exts per file w/exts: %5.2f\n"),
                        AllvolData->averageExtentCount);

                fprintf(stdout, MSG(DF_AGGIO, 
                        "    Aggregate I/O perf:        %3.0f%%\n"),
                        AllvolData->aggregatePerf);
		(void)fflush(stdout);

                if (displayFreeSpaceFlag) {
                    fprintf(stdout, MSG(DF_FSF,
                            "    Free space fragments:    %6d\n"),
                            AllvolFreeSpaceData->totalHoleCount +
                            AllvolData->Othervols_freeSpace.totalHoleCount);

                    fprintf(stdout, "                  ");
                    for (range = 0; range < FREE_RANGE_COUNT; range++) {
                        if (range == FREE_RANGE_COUNT - 1) {
                           range_print(stdout, " %+6dK\n", freeRangeTop[range - 1]);
                        } else {
                            range_print(stdout, " %+6dK", -freeRangeTop[range]);
                        }
                    }
                    fprintf(stdout, MSG(DF_PFS, "      Free space:"));
                    for (range = 0; range < FREE_RANGE_COUNT; range++) {
                        fprintf(stdout, "    %3.0f%%",
                            (AllvolFreeSpaceData->totalFreeSpace +
                             AllvolData->Othervols_freeSpace.totalFreeSpace) ?
                              100.0 *
                                (AllvolFreeSpaceData->rangeData[range].freeSpace +
                                AllvolData->Othervols_freeSpace.rangeData[range].freeSpace) /
                                  ((double) AllvolFreeSpaceData->totalFreeSpace +
                                   (double) AllvolData->Othervols_freeSpace.totalFreeSpace) :
                              0.0);
                    }
                    fprintf(stdout, MSG(DF_PFRAG, "\n      Fragments: "));
                    for (range = 0; range < FREE_RANGE_COUNT; range++) {
                        fprintf(stdout, " %7d",
                        AllvolFreeSpaceData->rangeData[range].holeCount +
                        AllvolData->Othervols_freeSpace.rangeData[range].holeCount);
                    }
                    fprintf(stdout, "\n\n");
                    (void)fflush(stdout);
                } /* end if display freespace flag is set */
            } /* end if somebody did some work */

            /* reset the fields */
            AllvolData->extentCount = 0;
            AllvolData->fileWithExtCount = 0;
            AllvolFreeSpaceData->totalHoleCount = 0;
            AllvolFreeSpaceData->totalFreeSpace = 0;
            /* reinitialize the slots for all volumes */
            for (range = 0; range < FREE_RANGE_COUNT; range++) {
                AllvolFreeSpaceData->rangeData[range].freeSpace = 0;
                AllvolFreeSpaceData->rangeData[range].holeCount = 0;
            }

            pthread_mutex_unlock(&Allvol_mutex );

            /* if there are any volumes still not done, we need to keep going */
            curPassNumber++;
            for(k=0; k<num_vols; k++) {
                if (volPassStat[k].volDone == FALSE) {
                    goto wait_more ;  /* this volume is not done yet */

                }

            }  /* end for */


            /* fell through - means all volumes volDone is TRUE, so notify pool */
            tpoolp->shutdown = TRUE;
            pthread_mutex_unlock(&volPassStat_mutex );

            /* no volumes need work so terminate this reporting thread */
            pthread_exit(NULL) ;

        } /* end if any more threads active*/
    }/* end for any more vols */

/* should never get here ... but ... */
pthread_mutex_unlock(&volPassStat_mutex );
} /* end print_stats */

void
tpool_init(tpool_t *tpoolp,
           int num_worker_threads,
           adv_bf_dmn_params_t *dmnParams,   
           volDataT *volData,                
           volFreeSpaceDataT *volFreeSpaceData
)
{
    pthread_attr_t pthread_custom_attr;
    int i;
    uint64_t thread_stack_size;

    /* Allocate the pool data structure */
    tpoolp = (tpool_t *) malloc(sizeof(tpool_t));
    if(tpoolp == NULL) {
        fprintf (stderr, MSG(DF_NOMEM, 
                            "%s: Cannot allocate memory\n"), Prog);
    }

    /* Initialize the pool structure fields */
    tpoolp->num_threads = num_worker_threads;
    tpoolp->threads = (pthread_t *)malloc((num_worker_threads+1) 
                                                 * sizeof(pthread_t));
    tpoolp->shutdown = 0;
    tpoolp->print_stats_thread_sleeping = FALSE;
    tpoolp->print_stats_signal_sent = FALSE;
    tpoolp->dmnParams = dmnParams;
    tpoolp->volData = volData;
    tpoolp->volFreeSpaceData = volFreeSpaceData;

    /* initialize volume statistic fields */
    for(i=0; i<num_vols; i++) {
        volPassStat[i].volDone = FALSE;
        volPassStat[i].curPass = 0;
        volPassStat[i].work_in_progress = FALSE;
        volPassStat[i].not_enough_space = FALSE;
    }

    /* create threads */
   
    /* One thread for global data printing of statistics */
    /* no need for this thread to have a large stack size */
    pthread_attr_init(&pthread_custom_attr);

    pthread_create(&tpoolp->threads[0],
                       &pthread_custom_attr,
                       (void *) print_stats,
                       (void *) tpoolp);

    /* stack size needs changed because default stack size is not enough */
    (void)pthread_attr_getstacksize(&pthread_custom_attr, &thread_stack_size);
    if (thread_stack_size < MIN_REQ_SSIZE) {
        pthread_attr_setstacksize(&pthread_custom_attr, (int64_t)MIN_REQ_SSIZE);
    }

    for(i=1; i<num_worker_threads+1; i++) {
        pthread_create(&tpoolp->threads[i],
                       &pthread_custom_attr,
                       (void *) tpool_thread,
                       (void *) tpoolp);
    
    }

    /* Synchronize on the shutdown */
    for (i = 0; i < num_worker_threads+1; i++) {
      pthread_join(tpoolp->threads[i], (void *) NULL);
    }

    free(tpoolp);
}     /* end tpool_init */ 


void
tpool_thread(tpool_t *tpoolp)
{
package_t *p;
int vol, vol2;
pthread_t tid;
    tid = pthread_self( );

    for(;!tpoolp->shutdown;) {

        /* loop through volumes and see if any need servicing */
        for(vol=0; vol<num_vols ; vol++) {

           pthread_mutex_lock(&volPassStat_mutex );
           if( (volPassStat[vol].curPass <= curPassNumber) && 
               (volPassStat[vol].volDone == FALSE) &&
               (volPassStat[vol].work_in_progress == FALSE) )  {
/*****************************************************************************/
               /* a volume needs serviced - work on this volume */
               volPassStat[vol].work_in_progress = TRUE;
               pthread_mutex_unlock(&volPassStat_mutex );

               p = (package_t *)malloc(sizeof(package_t));
               if(p == NULL) {
                   fprintf (stderr, MSG(DF_NOMEM, 
                                "%s: Cannot allocate memory\n"), Prog);
               }
               p->volume = vol; 
               p->dmnParams = tpoolp->dmnParams;
               p->volData = &tpoolp->volData[vol];
               p->volFreeSpaceData = &tpoolp->volFreeSpaceData[vol];

               /*************** do the work on the volume **********/
               defragment_main(p);
               /*************** do the work on the volume **********/

               free(p);
               pthread_mutex_lock(&volPassStat_mutex );
               volPassStat[vol].work_in_progress = FALSE;
               pthread_mutex_unlock(&volPassStat_mutex );

               pthread_cond_signal(&passCheck_cv);
               break;
/*****************************************************************************/
           } 
           else if ((tpoolp->print_stats_thread_sleeping) &&
                    (!tpoolp->print_stats_signal_sent)) {
               /*
                * There's no work to do right now for this
                * volume.  But if the print_stats() thread is asleep
                * on the passCheck_cv and no other worker thread
                * has already sent him a signal, see if there is any work
                * in progress on any volume.  If not, wake up the 
                * print_stats() thread.  This wakeup is generally done 
                * up above, after the call to defragment_main().  
                * But due to thread timings, it's possible for the 
                * print_stats() thread to miss that wakeup call.  
                */
               for(vol2 = 0; 
                   (vol2 < num_vols) && (!volPassStat[vol2].work_in_progress);
                   vol2++);
               if (vol2 == num_vols) {
                   pthread_cond_signal(&passCheck_cv);
                   tpoolp->print_stats_signal_sent = TRUE;
               }
           }
           pthread_mutex_unlock(&volPassStat_mutex );
       } /* end for each volume */
       sleep(3);
    } /* end forever loop */

} /* end tpool_thread */


static void
defragment_exit()
{

    if (thisDmnLock != -1) {
        if (program_exit_status == 0) {
            advfs_post_user_event(EVENT_FDMN_DEFRAG_UNLOCK, advfs_event, Prog);
        } else {
            advfs_post_user_event(EVENT_FDMN_DEFRAG_ERROR, advfs_event, Prog);
        }
    }

    exit(program_exit_status);
}

/* end defragment.c */

