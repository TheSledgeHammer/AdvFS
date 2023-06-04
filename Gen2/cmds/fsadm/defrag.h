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
 *    defragment.h
 *    This module implements the "defragment" utility.
 */
#include <errno.h>
#include <advfs/advfs_evm.h>
#include <advfs/ms_public.h>
#include "common.h"

#define MSG(n,s) catgets(catd,MS_FSADM_DEFRAG,n,s)
nl_catd catd;

#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#define ERROR       -1
#define OKAY         0
#define CONTINUE     1
#define BREAK        2
#define MAPDONE	     3
#define VOLDONE	     4
#define NOT_ENOUGH_SPACE  5

#define BLOCK_SIZE 512
#define MIN_REQ_SSIZE 512000 /* default of 21K is not enough for this program */
                             /* this program has recursive calls which add */
                             /* dramatically to the stack size */

/* The following two limits are applied such that at least one must be met
 * for defragmentation to proceed. */
#define MINIMUM_FREE_SIZE_FRACTION 0.02 /* must be at least 2% free for a volume to be worked */
#define MINIMUM_VOLUME_FREE_BLOCKS 9760	/* i.e., 5 megabytes per volume */
#define MAX_THREADS 20   /* optimum number for now  - from some testing */
#define CLEAR_SIZE_FRACTION 0.85
#define SINGLE_XTNT_OF_CLEAR_SIZE_FRAC 0.75

#define AVERAGE_EXTENT_COUNT_GOAL 1.004
#define AVERAGE_EXTENT_GOAL .01

/* This determines when the defragmenter will quit if the average extent
 * count has not yet reached the goal. */
#define MAXIMUM_UNIMPROVED_SCORE_HIGH 5

/* This determines when the defragmenter will quit if the average extent
 * count has reached the goal. */
#define MAXIMUM_UNIMPROVED_SCORE_LOW 2

#define RARIFY_AREA_RATIO 1
#define MAX_RARIFY_BOOST 4

#define BEGIN_REDUCE_HOLES_RATIO 1.5

#define MAX_MIGRATE_FILL_SIZE_INBLKS 2097152  /* (2GB) don't migrate more than this */

#define BF_DESC_MAX 500  /* limit the number files fetched per fetch to this amount */
#define XTNT_DESC_MAX 500  /* limit the number extents fetched per fetch to this amount */
#define MAXIMUM_CLEAR_AREA_SIZE 64000000  /* size limit on clear area (initially!) */
                                          /* WARN:this value can actually be exceeded greatly */
#define LARGE_FILE_PCT_OF_DMN   10        /* 10% of a volume constitutes a "large" file */
#define HEADER_SIZE 2 * sizeof(uint32_t)
#define MAX_BMT_EXTENTS 1024

typedef int32_t boolean;

typedef enum {
    clearSparsestArea, 
    clearMostHoles, 
    clearLargestHole
} clearStrategyT;

extern char *sys_errlist[];

typedef struct bfListEntryT {
    bfTagT bfSetTag;
    bfTagT bfTag;
    float filePerf;
    uint64_t fileExtentCount;
    float fileMovePayoff;	/* Initial value of -1 means "never set".*/
    int32_t noFillMove;
    int32_t hasValidData;  /* FilePerf and fileExtentCount. */
    int32_t balance;  /* Actually, only needs two bits! */
    int32_t wasCleared;
    /* Normally, files that have one extent and are not cleared may have their
     * bf table entries deleted in the clear phase to save memory.  But, sparse
     * files may not, since such a file may really have more than one bf desc
     * because it really has more than one extent.  Deleting its bf entry in
     * the clear phase would then result in double-counting the file. */
    int32_t isSparse;
    struct bfListEntryT *prior;
    struct bfListEntryT *next;
    int32_t singleVolumeIndex;	/* -1 if file is on > 1 volume */
    uint64_t filesize;           /* used to add up total filesizes needed for filling */
} bfListEntryT;

static bfListEntryT **bf_criteria1_list_start;
static bfListEntryT **bf_criteria1_list_last;
static bfListEntryT **bf_criteria2_list_start;
static bfListEntryT **bf_criteria2_list_last;

typedef struct {
    uint64_t fobOffset;
    uint64_t nominalFobCnt;
    uint32_t allocatedFobCnt;
    uint64_t bsed_vd_blk;
    uint64_t bsed_vol_index;
    int32_t stripe;
} filteredXtntDescT;

typedef struct {
    uint64_t fobOffset;
    uint64_t fobCnt;
    uint64_t bsed_vol_index;
    uint32_t volSubscr;
} xtntDescT;

typedef struct xtntRunDescT {
    uint64_t fobOffset;
    uint64_t allocatedFobCount;
    uint64_t nominalFobCount;
    struct xtntRunDescT *next;
} xtntRunDescT ;

typedef struct {
  uint64_t bsed_vol_index;             /* index number of this volume, WARNING- may not be sequential */
  uint64_t volSize;		/* volume's gross size */
  uint64_t volStgCluster;	/* number of blocks each bit represents */ 
  uint64_t volFreeSize;		/*  amount of free space left on this volume */
  float freeSizeFract;		/* percentage of free space left on volume */
  uint64_t lowerBlkClearBound;  /* lower boundary (in blks) of area to be cleared */
  uint64_t upperBlkClearBound;  /* upper boundary (in blks) of area to be cleared */
  uint64_t clearAreaSize;       /* initial to-be-cleared area setting */
  uint64_t actualClearedAreaSize;   /* actual cleared area setting after clearing*/
  boolean alreadyClear;         /* flag to indicate that volume is not to be cleared */
  boolean doFill;               /* flag to indicate that volume is too full to be */
                                /* worth trying to do a clear/fill on it. */
  int64_t remainingClearAreaSize;	/* keeps track of the amount remaining that needs */
                                /* to be filled still.				*/
  boolean isEligibleTarget;     /* once clear area is filled up, this is the flag */
                                /* that indicates that no more filling is to be done*/
  int32_t wrMaxIo;              /* used in calculating the total IO rating for this volume. */
                            /* max blocks that can be read/written in a consolidated I/O */
  uint64_t lowerBlkRarifyBound; /* lower boundary of the area to be used in filling in */
                            /* anticipation of the next pass using this area for clearing. */
  uint64_t upperBlkRarifyBound; /* upper boundary of the area to be used in filling in */
                            /* anticipation of the next pass using this area for clearing. */
  int32_t startPassTime;
  int32_t endPassTime;
  double averageExtentCount;
  double lastAverageExtentCount;
  double bestAverageExtentCount;
  uint64_t totalHoleCount;
  int32_t unimprovedScore;
  uint64_t areaPopulation;
  int32_t passNumber;
  int32_t clearLimitTime;
  uint64_t extentCount;
  uint64_t fileWithExtCount;
  uint64_t totalIOCount;
  uint64_t totalBestIOCount;
  double threshholdPerf;
  clearStrategyT basicClearStrategy;
  boolean shortenedClearPass;
  double aggregatePerf;
  boolean stopDefragment;
  uint64_t dmnFreeSize;
  float dmnFreeSizeFract;
} volDataT;

#define FREE_RANGE_COUNT 4
typedef struct {
            struct {
                uint64_t holeCount;
                uint64_t freeSpace;
            } rangeData[FREE_RANGE_COUNT];
            uint64_t totalHoleCount;
            uint64_t totalFreeSpace;
} volFreeSpaceDataT;

static volFreeSpaceDataT *AllvolFreeSpaceData;  /* holds free space infor for all vols. */

typedef struct {
/* these fields contain the total for all vols for the pass just completed */
  double averageExtentCount;
  uint64_t extentCount;
  uint64_t fileWithExtCount;
  boolean shortenedClearPass;
  double aggregatePerf;
  uint64_t totalIOCount;
  uint64_t totalBestIOCount;
/* these fields contain the last known data for vols that are finished */
/* used in the pass verbal output calculations */
  uint64_t Othervols_extentCount;
  uint64_t Othervols_fileWithExtCount;
  uint64_t Othervols_totalIOCount;
  uint64_t Othervols_totalBestIOCount;
  volFreeSpaceDataT Othervols_freeSpace;
} AllvolDataT;

static AllvolDataT *AllvolData;

typedef struct {
            int32_t remainingBytesToRead;
            int32_t bytesInBuffer;
            int32_t bufferPosition;
            char *mapFileName;
            FILE *mapFile;
            int32_t buffer_size;
            char *buffer;
} findSparsestAreaRootT;


/**** THREAD RELATED ****/
/* thread based structures and mutexes and condition variables */

/* 
 * structure for holding individual thread parameters  
 * - used for each thread/volume combination 
 */

typedef struct {
  int32_t       volume;                 /* sequential volume index number */
  adv_bf_dmn_params_t *dmnParams;       /* pointer to domain's params */
  volDataT *volData;                    /* ptr to structure holding this threads work vars */
  volFreeSpaceDataT *volFreeSpaceData;  /* ptr to structure holding this threads free space vars */ 
} package_t;

/* 
 * structure that contains the current status of a volume
 * - GLOBAL, mutex protected  - used in thread pass synchronization
 * each volume will have one of these until program termination
 * used separate structure from volData for performance reasons
 */

typedef struct {
  int32_t       curPass;
  int32_t       volDone;              /* T or F, has this volume completed its defrag? */
  int32_t       work_in_progress;     /* T or F, is this volume currently being defragmented? */
  boolean   not_enough_space;         /* T or F, does this volume have enough space this pass? */
} volPassStatT;

static volPassStatT *volPassStat;  /* ptr holding pass status info for all vols/thread combos. */

static int curPassNumber;          /* lets the reporting thread know which pass its trying
                                      to syncronize */

/* thread pool stuff */

typedef struct {
     int32_t      num_threads;
     pthread_t    *threads;
     int32_t      shutdown;
     int32_t      print_stats_thread_sleeping; 
     int32_t      print_stats_signal_sent; 
     adv_bf_dmn_params_t *dmnParams;              /* pointer to domain params */
     volDataT     *volData;                /* ptr to structure holding all threads volume data */
     volFreeSpaceDataT *volFreeSpaceData;  /* ptr to structure holding all threads free space data */
} tpool_t;

static tpool_t      *tpoolp; 

void
tpool_init(tpool_t *tpoolp,
           int     num_worker_threads,
           adv_bf_dmn_params_t *dmnParams,   
           volDataT *volData,          
           volFreeSpaceDataT *volFreeSpaceData);

void
tpool_thread(tpool_t *tpoolp);


/*
 * Private protos
 */

static
void
defragment_main (
            package_t *p ); /* in */

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
            adv_bf_dmn_params_t *dmnVersion,  /* in */
            uint64_t *areaSize,  /* in/out */
            uint64_t *areaPopulation,  /* in/out */
            uint64_t *lowerBound,  /* out */
            boolean *alreadyClear,  /* out */
            volFreeSpaceDataT *freeSpaceData, /* out */
            volDataT *volData);

static
int
get_system_time (
            int *result);  /* out */

static
int
move_files_clear (
               bfDomainIdT domainId,  /* in */
               volDataT *volData,       /* in */
               int volume,/* in */
               int clearLimitTime,/* in */
               boolean *reinsertSideFlag);
static
int
chk_move_file (
               volDataT *volData,          /* in */
               bsBfDescT *bfDesc,          /* in */
               bfListEntryT *retBfEntry,   /* in */
               int *fileStartTime,         /* out */
               int clearLimitTime);        /* in */
static
adv_status_t
move_normal_file_clear (
                      int fd,  /* in */
                      bfTagT bfTag,  /* in */
                      metadataTypeT metadataType,  /* in */
                      int pageSize,  /* in */
                      volDataT *volData,  /* in */
                      bfListEntryT *fileEntry, /* in/out */
                      int clearLimitTime, /* in */
                      adv_bf_attr_t *bfAttr);
static
int
move_files_fill (
               bfListEntryT *list_start,  /* in */
               adv_bf_dmn_params_t *dmnParams,  /* in */
               volDataT *volData,        /* in */
               int subpassNumber,        /* in */
               int volume);              /* in */

static
adv_status_t
move_normal_file_fill (
                           int fd,
                           bfTagT bfTag, 
                           bfTagT bfSetTag, 
                           uint32_t targetVolIndex,
                           adv_bf_info_t *bfInfo,
                           uint64_t fileExtentCount,
                           int64_t *remainingClearAreaSize,
                           int volume,
                           int singlevolume,
                           adv_bf_attr_t *bfAttr);

static
adv_status_t
migrate (
         int fd,  /* in */
         uint32_t srcVolIndex,  /* in */
         uint64_t bsed_fob_offset,  /* in */
         uint64_t bsed_fob_cnt,  /* in */
         uint32_t dstVolIndex, /* in */
         bfTagT bfTag,   /* in */
         bfTagT bfSetTag);   /* in */

adv_status_t
get_bf_xtnt_map (
    int            fd,  /* in */
    int startXtntMap,  /* in */
    int startXtnt,  /* in */
    int pageSize,  /* in */
    int xtntsArraySize,  /* in */
    filteredXtntDescT **xtntsArray,  /* out */
    bsExtentDescT **srcXtntsArrayParam,  /* in/out */
    int *xtntCnt, /* out */
    int *srcXtntCnt, /* out */
    adv_bf_attr_t *bfAttr,
    int **stripeArray);

static
adv_status_t
create_xtnt_desc_list (
                       int fd,  /* in */
                       int xtntMapIndex,  /* in */
                       volDataT *volData, /* in */
                       int pageSize,  /* in */
                       bfListEntryT *fileEntry,  /* in/out */
                       int *retXtntDescCnt,  /* out */
                       xtntDescT **retXtntDesc,  /* out */
                       adv_bf_attr_t *bfAttr,
                       metadataTypeT metadataType, /* in */
                       int phase);  /* in */

static
boolean
xtnt_in_range(
        volDataT *volData, /* in */
        uint64_t bsed_vol_index, /* in */
        uint64_t bsed_vd_blk, /* in */
        int xtntBlkCount);  /* in */

static
int
included_count(
        int majorLower,  /* in */
        int majorUpper,  /* in */
        int minorLower,  /* in */
        int minorUpper); /* in */

static
adv_status_t
extend_xtnt_desc_list (
                       uint64_t *maxCnt,  /* in/out */
                       xtntDescT **xtntDesc);  /* in/out */

static
adv_status_t
find_xtnt_run (
               int fd,  /* in */
               int maxFobCountIn,  /* in */
               int pageSize,  /* in */
               uint64_t fileExtentCount,  /* in */
               boolean *found, /* out */
               xtntRunDescT **retXtntRuns, /* out */
               int volume,
               bfTagT bfTag,
               adv_bf_attr_t *bfAttr);

static
void
select_fill_target(
        int fd,  /* in */
        volDataT *volData,  /* in */
        int srcVolumeIndex,  /* in */
        uint64_t fileBlockCount,  /* in */
        unsigned *targetVolumeSubscr,  /* in/out */
        boolean *found);  /* out */

static
float
non_uniform_payoff_boost(
        int largestXtnt,      /* in */
        int smallestXtnt,     /* in */
        int averageXtnt,      /* in */
        boolean wasCleared);  /* in */

static
void
range_print (FILE *f, char *format_in, int range_in);

void
defrag_usage(void);

static
void
sigHandler ( 
            int signal);  /* in */

static
void
abort_prog(
           char *msg, ... );

static
int
open_domain(
    volDataT **volData,     /* in/out */
    volFreeSpaceDataT **volFreeSpaceData,
    adv_bf_dmn_params_t *dmnParams,
    volPassStatT **retvolPassStat);  /* in/out */

static
void
print_stats (tpool_t *tpoolp);

static
boolean
stop_pass_check(
        volDataT *volData);

static
boolean
set_time_limit(int passNumber,
               int *startPassTime,
               int *endPassTime,
               int *clearLimitTime,/* in */
               volDataT *volData);

static
void
select_clear_strategy(int passNumber,
                      volDataT *volData);

static
boolean
reset_clear_point( volDataT *volData,
                   volFreeSpaceDataT *volFreeSpaceData,
                   uint64_t *areaPopulation,                   /* in/out */
                   int passNumber,
                   int unimprovedScore,
                   adv_bf_dmn_params_t *dmnParams,
                   double averageExtentCount);

static
void 
update_stats(volFreeSpaceDataT *volFreeSpaceData, 
             int passNumber,
             volDataT *volData,
             int volume);    /* in */


static
int
get_next_map_byte(
            findSparsestAreaRootT *root_ptr,
            boolean *byteWasRead,
            char *nextMapByte);

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
        int *largestHoleMapPosition); /* out */

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
             int     *bmtXtntCount);

/* These functions return TRUE if the first entry goes to the left
 * of the second entry. */
static
boolean
compare_insert(
            bfListEntryT *aEntry,  /* in */
            bfListEntryT *bEntry);

static
int
update_volume_data(
        adv_bf_dmn_params_t *dmnParams,  /* in */
        volDataT *volData);     /* in/out */

static
xtntRunDescT *
get_run_desc(
        int nominalFobCount,
        int allocatedFobCount,
        int fobOffset,
        int volume);

static
void
save_vols_end_stats(volDataT *volData,
                    volFreeSpaceDataT *volFreeSpaceData,
                    int volume);

static 
int
insert_bf_entry_criteria1(
                     bfListEntryT *newEntry, /* table entry to create */
                     int volume,
                     uint64_t clearAreasize,
                     int assert_flag); /* flag to perform list checks */

static 
int
insert_bf_entry_criteria2(
                     bfListEntryT *newEntry, /* table entry to create */
                     int volume,
                     uint64_t clearAreasize,
                     int assert_flag);   /* flag to perform list checks */

static
void
bf_clean_list(
              bfListEntryT *list_start,
              bfListEntryT *list_last,
              int volume,
              int stage);

static
int
copy_newEntry(bfListEntryT *newEntry,
              bfListEntryT **retnewEntry);

static
int
delete_bf_entry(
                int volume,
                bfListEntryT *Entry,/* linked list entry to delete */
                int list);  /* list number to delete from */

static 
void
defragment_exit();

