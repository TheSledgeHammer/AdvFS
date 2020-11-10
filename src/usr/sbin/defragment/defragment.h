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
 *    defragment.h
 *    This module implements the "defragment" utility.
 *
 * Date:
 *
 *    Feb 20, 1997
 *
 * Revision History:
 *
 */
/*
 * HISTORY
 */
/*
 * @(#)$RCSfile: defragment.h,v $ $Revision: 1.1.13.1 $ (DEC) $Date: 2001/12/14 16:52:00 $
 */

#include <msfs/advfs_evm.h>
#include "thread_interfaces.h"
#include <msfs/ms_public.h>

nl_catd catd;
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#define LARGE_MIGRATE_GOVERNOR 1

#define O_open open
#define O_close close

#define ERROR       -1
#define OKAY         0
#define CONTINUE     1
#define BREAK        2
#define MAPDONE	     3
#define VOLDONE	     4
#define NOT_ENOUGH_SPACE  5

#define BLOCK_SIZE 512
#define MIN_REQ_SSIZE 512000     /* default of 21K is not enough for this program */
                                 /* this program has recursive calls which add dramatically */
                                 /* to the stack size */

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

#ifdef LARGE_MIGRATE_GOVERNOR
#define MAX_MIGRATE_FILL_SIZE_INBLKS 2097152  /* (1GB)  don't ever migrate more than this */
                                            /* until "log half full" problem is fixed */
/* #define MAX_MIGRATE_FILL_SIZE_INBLKS 20000  (10MB)   - used for testing */
#endif

#define BF_DESC_MAX 500  /* limit the number files fetched per fetch to this amount */
#define XTNT_DESC_MAX 500  /* limit the number extents fetched per fetch to this amount */
#define MAXIMUM_CLEAR_AREA_SIZE 64000000  /* size limit on clear area (initially!) */
                                          /* WARN:this value can actually be exceeded greatly */
#define LARGE_FILE_PCT_OF_DMN   10   /* 10% of a volume constitutes a "large" file */
#define HEADER_SIZE 2 * sizeof(u32T)
#define MAX_BMT_EXTENTS 1024

typedef int boolean;

typedef enum {clearSparsestArea, clearMostHoles, clearLargestHole} clearStrategyT;

typedef unsigned long u64T;

extern int errno;
extern char *sys_errlist[];

typedef struct bfListEntryT {
    mlBfTagT bfSetTag;
    mlBfTagT bfTag;
    float filePerf;
    u64T fileExtentCount;
    float fileMovePayoff;	/* Initial value of -1 means "never set".*/
    int noFillMove;
    int hasValidData;  /* FilePerf and fileExtentCount. */
    int balance;  /* Actually, only needs two bits! */
    int wasCleared;
    /* Normally, files that have one extent and are not cleared may have their
     * bf table entries deleted in the clear phase to save memory.  But, sparse
     * files may not, since such a file may really have more than one bf desc
     * because it really has more than one extent.  Deleting its bf entry in
     * the clear phase would then result in double-counting the file. */
    int isSparse;
    struct bfListEntryT *prior;
    struct bfListEntryT *next;
    int singleVolumeIndex;	/* -1 if file is on > 1 volume */
    u64T filesize;           /* used to add up total filesizes needed for filling */
} bfListEntryT;

static bfListEntryT **bf_criteria1_list_start;
static bfListEntryT **bf_criteria1_list_last;
static bfListEntryT **bf_criteria2_list_start;
static bfListEntryT **bf_criteria2_list_last;
static int cnt;

#define ERRMSG(err) sys_errlist[err]

typedef struct {
    u32T pageOffset;
    u32T nominalPageCnt;
    u32T allocatedPageCnt;
    u32T volBlk;
    int volIndex;
    int stripe;
} filteredXtntDescT;

typedef struct {
    u32T pageOffset;
    u32T pageCnt;
    int volIndex;
    unsigned volSubscr;
} xtntDescT;

typedef struct xtntRunDescT {
    int pageOffset;
    int allocatedPageCount;
    int nominalPageCount;
    struct xtntRunDescT *next;
} xtntRunDescT ;

typedef struct {
  int volIndex;                /* index number of this volume, WARNING- may not be sequential */
  u64T volSize;			/* volume's gross size */
  u64T volStgCluster;		/* number of blocks each bit represents */ 
  u64T volFreeSize;		/*  amount of free space left on this volume */
  float freeSizeFract;		/* percentage of free space left on volume */
  u64T lowerBlkClearBound;      /* lower boundary (in blks) of area to be cleared */
  u64T upperBlkClearBound;      /* upper boundary (in blks) of area to be cleared */
  u64T clearAreaSize;           /* initial to-be-cleared area setting */
  u64T actualClearedAreaSize;   /* actual cleared area setting after clearing*/
  boolean alreadyClear;         /* flag to indicate that volume is not to be cleared */
  boolean doFill;               /* flag to indicate that volume is too full to be */
                                /* worth trying to do a clear/fill on it. */
  long remainingClearAreaSize;	/* keeps track of the amount remaining that needs */
                                /* to be filled still.				*/
  boolean isEligibleTarget;     /* once clear area is filled up, this is the flag */
                                /* that indicates that no more filling is to be done*/
  int wrMaxIo;              /* used in calculating the total IO rating for this volume. */
                            /* max blocks that can be read/written in a consolidated I/O */
  u64T lowerBlkRarifyBound; /* lower boundary of the area to be used in filling in */
                            /* anticipation of the next pass using this area for clearing. */
  u64T upperBlkRarifyBound; /* upper boundary of the area to be used in filling in */
                            /* anticipation of the next pass using this area for clearing. */
  int startPassTime;
  int endPassTime;
  double averageExtentCount;
  double lastAverageExtentCount;
  double bestAverageExtentCount;
  u64T totalHoleCount;
  int unimprovedScore;
  u64T areaPopulation;
  int passNumber;
  int clearLimitTime;
  u64T extentCount;
  u64T fileWithExtCount;
  u64T totalIOCount;
  u64T totalBestIOCount;
  double threshholdPerf;
  clearStrategyT basicClearStrategy;
  boolean shortenedClearPass;
  double aggregatePerf;
  boolean stopDefragment;
  u64T dmnFreeSize;
  float dmnFreeSizeFract;
} volDataT;

#define FREE_RANGE_COUNT 4
typedef struct {
            struct {
                u64T holeCount;
                u64T freeSpace;
            } rangeData[FREE_RANGE_COUNT];
            u64T totalHoleCount;
            u64T totalFreeSpace;
} volFreeSpaceDataT;

static volFreeSpaceDataT *AllvolFreeSpaceData;  /* holds free space infor for all vols. */

typedef struct {
/* these fields contain the total for all vols for the pass just completed */
  double averageExtentCount;
  u64T extentCount;
  u64T fileWithExtCount;
  boolean shortenedClearPass;
  double aggregatePerf;
  u64T totalIOCount;
  u64T totalBestIOCount;
/* these fields contain the last known data for vols that are finished */
/* used in the pass verbal output calculations */
  u64T Othervols_extentCount;
  u64T Othervols_fileWithExtCount;
  u64T Othervols_totalIOCount;
  u64T Othervols_totalBestIOCount;
  volFreeSpaceDataT Othervols_freeSpace;
} AllvolDataT;

static AllvolDataT *AllvolData;

typedef struct setHdr {
    mlBfTagT setTag;
    int mountedFlag;
    int unmountFlag;
    char *mntOnName;
    char *dotTagsPath;
    int fd;
} setHdrT;

static advfs_ev advfs_event;
static int thisDmnLocked=0;
static int dmnLock;
static int unlockDmnFlag;
static int unmountFlag;
static int num_vols;
static int program_exit_status=0;

static char *dmnName;
static char *dotTags = "/.tags";
static char *metadataRootPathName = NULL;
static char *Prog;
static int volIndexCnt;
static int *volSubscrArray = NULL;
static int setHdrCnt = 0;
static setHdrT *setHdrTbl = NULL;
static boolean unmountFlag = FALSE;
static boolean ignoreErrorsFlag = FALSE;
static boolean on_last_volume = FALSE;
static boolean noDefragmentFlag = FALSE;
static boolean hasTimeLimit = FALSE;
static boolean hasStrictTimeLimit = FALSE;
static boolean verboseFlag = FALSE;
static boolean veryVerboseFlag = FALSE;
static boolean displayFreeSpaceFlag = FALSE;
static boolean stripeMessagePrinted = FALSE;
static int passCount = 1000000;
#ifdef LARGE_MIGRATE_GOVERNOR
static boolean OverRideFlag = FALSE;
#endif

static int allowedTimeInterval;
static int limitTime;

static u64T largeFileThreshhold;

/* Tops of the ranges into which the free space holes will be clasified, in
 * 1K units.  The last range has a top of zero, which means no top.  Ranges
 * over and including 1000 must be in multiples of 1000. */
static const int freeRangeTop[FREE_RANGE_COUNT] = {100,1000,10000,0};

typedef struct {
            int remainingBytesToRead;
            int bytesInBuffer;
            int bufferPosition;
            char *mapFileName;
            FILE *mapFile;
            int buffer_size;
            char *buffer;
} findSparsestAreaRootT;


/**** THREAD RELATED ****/
/* thread based structures and mutexes and condition variables */

/* 
 * structure for holding individual thread parameters  
 * - used for each thread/volume combination 
 */

typedef struct {
  int       volume;                     /* sequential volume index number */
  mlDmnParamsT *dmnParams;              /* pointer to domain's params */
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
  int       curPass;
  int       volDone;              /* T or F, has this volume completed its defrag? */
  int       work_in_progress;     /* T or F, is this volume currently being defragmented? */
  boolean   not_enough_space;     /* T or F, does this volume have enough space this pass? */
} volPassStatT;
static volPassStatT *volPassStat;  /* ptr holding pass status info for all vols/thread combos. */

static int curPassNumber;          /* lets the reporting thread know which pass its trying
                                      to syncronize */

/* Global mutual exclusion (mutex) for threaded program operation */
t_mutex  Allvol_mutex;             /* Global */
t_mutex  volPassStat_mutex;                     /* Global */
/* synchronize print output after each pass of all volume threads */
t_cond   passCheck_cv;                    /* Global */


/* thread pool stuff */

typedef struct {
     int    num_threads;
     pthread_t    *threads;
     int          shutdown;
     int          print_stats_thread_sleeping; 
     int          print_stats_signal_sent; 
     mlDmnParamsT *dmnParams;              /* pointer to domain params */
     volDataT *volData;                    /* ptr to structure holding all threads volume data */
     volFreeSpaceDataT *volFreeSpaceData;  /* ptr to structure holding all threads free space data */
} tpool_t;

static tpool_t      *tpoolp; 

void
tpool_init(tpool_t *tpoolp,
           int     num_worker_threads,
           mlDmnParamsT *dmnParams,   
           volDataT *volData,          
           volFreeSpaceDataT *volFreeSpaceData
);

void
tpool_thread(tpool_t *tpoolp);


/*
 * Private protos
 */

static
void
defragment_main (
            package_t *p /* in */
            );

static
int
find_sparsest_area (
            int volIndex,  /* in */
            unsigned volSize,  /* in */
            unsigned clusterSize,  /* in */
            boolean inhibitDisplay,  /* in */
            int lowerExcludeBound,  /* in */
            int upperExcludeBound,  /* in */
            clearStrategyT clearStrategy,  /* in */
            mlDmnParamsT *dmnVersion,  /* in */
            u64T *areaSize,  /* in/out */
            u64T *areaPopulation,  /* in/out */
            u64T *lowerBound,  /* out */
            boolean *alreadyClear,  /* out */
            volFreeSpaceDataT *freeSpaceData, /* out */
            volDataT *volData
            );

static
int
get_system_time (
            int *result  /* out */
            );

static
int
move_files_clear (
               mlBfDomainIdT domainId,  /* in */
               volDataT *volData,       /* in */
               int volume,/* in */
               int clearLimitTime,/* in */
               boolean *reinsertSideFlag
               );
static
int
chk_move_file (
               volDataT *volData,          /* in */
               mlBfDescT *bfDesc,          /* in */
               bfListEntryT *retBfEntry, /* in */
               int *fileStartTime,         /* out */
               int clearLimitTime /* in */
               );
static
mlStatusT
move_normal_file_clear (
                      int fd,  /* in */
                      mlBfTagT bfTag,  /* in */
                      mlMetadataTypeT metadataType,  /* in */
                      int pageSize,  /* in */
                      volDataT *volData,  /* in */
                      bfListEntryT *fileEntry, /* in/out */
                      int clearLimitTime, /* in */
                      mlBfAttributesT *bfAttr
                      );
static
int
move_files_fill (
               bfListEntryT *list_start,  /* in */
               mlDmnParamsT *dmnParams,  /* in */
               volDataT *volData,        /* in */
               int subpassNumber,        /* in */
               int volume              /* in */
               );

static
mlStatusT
move_normal_file_fill (
                           int fd,
                           mlBfTagT bfTag, 
                           mlBfTagT bfSetTag, 
                           u32T targetVolIndex,
                           mlBfInfoT *bfInfo,
                           u64T fileExtentCount,
                           long *remainingClearAreaSize,
                           int volume,
                           int singlevolume,
                           mlBfAttributesT *bfAttr
                           );

static
mlStatusT
migrate (
         int fd,  /* in */
         u32T srcVolIndex,  /* in */
         u32T bfPageOffset,  /* in */
         u32T bfPageCnt,  /* in */
         u32T dstVolIndex, /* in */
         mlBfTagT bfTag,   /* in */
         mlBfTagT bfSetTag   /* in */
         );

mlStatusT
get_bf_xtnt_map (
    int            fd,  /* in */
    int startXtntMap,  /* in */
    int startXtnt,  /* in */
    int pageSize,  /* in */
    int xtntsArraySize,  /* in */
    filteredXtntDescT **xtntsArray,  /* out */
    mlExtentDescT **srcXtntsArrayParam,  /* in/out */
    int *xtntCnt, /* out */
    int *srcXtntCnt, /* out */
    mlBfAttributesT *bfAttr,
    int **stripeArray
    );

static
mlStatusT
create_xtnt_desc_list (
                       int fd,  /* in */
                       int xtntMapIndex,  /* in */
                       volDataT *volData, /* in */
                       int pageSize,  /* in */
                       bfListEntryT *fileEntry,  /* in/out */
                       int *retXtntDescCnt,  /* out */
                       xtntDescT **retXtntDesc,  /* out */
                       mlBfAttributesT *bfAttr,
                       mlMetadataTypeT metadataType, /* in */
                       int phase  /* in */
                       );

static
boolean
xtnt_in_range(
        volDataT *volData, /* in */
        int volIndex, /* in */
        int volBlk, /* in */
        int xtntBlkCount  /* in */
        );

static
int
included_count(
        int majorLower,  /* in */
        int majorUpper,  /* in */
        int minorLower,  /* in */
        int minorUpper   /* in */
        );

static
mlStatusT
extend_xtnt_desc_list (
                       u64T *maxCnt,  /* in/out */
                       xtntDescT **xtntDesc  /* in/out */
                       );

static
mlStatusT
find_xtnt_run (
               int fd,  /* in */
               int maxPageCountIn,  /* in */
               int pageSize,  /* in */
               u64T fileExtentCount,  /* in */
               boolean *found, /* out */
               xtntRunDescT **retXtntRuns, /* out */
               int volume,
               mlBfTagT bfTag,
               mlBfAttributesT *bfAttr
               );

static
int
mount_bitfile_sets (
                    mlBfTagT rootBfSetTag  /* in */
                    );

int
unmount_bitfile_sets (
                      );

static
setHdrT *
insert_set_tag (
                mlBfTagT setTag  /* in */
                );

static
void
select_fill_target(
        int fd,  /* in */
        volDataT *volData,  /* in */
        int srcVolumeIndex,  /* in */
        u64T fileBlockCount,  /* in */
        unsigned *targetVolumeSubscr,  /* in/out */
        boolean *found  /* out */
        );

static
setHdrT *
find_set_tag (
              mlBfTagT setTag  /* in */
              );
static
char *
get_current_path_name(
         mlBfTagT bfTag,   /* in */
         mlBfTagT bfSetTag   /* in */
         );

static
float
non_uniform_payoff_boost(
        int largestXtnt,  /* in */
        int smallestXtnt,  /* in */
        int averageXtnt,  /* in */
        boolean wasCleared);  /* in */

static
void
range_print (FILE *f, char *format_in, int range_in);

static
void
usage();

static
void
sigHandler ( 
            int signal  /* in */
           );

static
void
abort_prog(
           char *msg, ...
          );

static
int
open_domain(
    volDataT **volData,     /* in/out */
    volFreeSpaceDataT **volFreeSpaceData,
    mlDmnParamsT *dmnParams,
    volPassStatT **retvolPassStat           /* in/out */
);

static
void
print_stats (tpool_t *tpoolp);

static
boolean
stop_pass_check(
        volDataT *volData
);

static
boolean
set_time_limit(int passNumber,
               int *startPassTime,
               int *endPassTime,
               int *clearLimitTime,/* in */
               volDataT *volData
);

static
void
select_clear_strategy(int passNumber,
                      volDataT *volData
);

static
boolean
reset_clear_point( volDataT *volData,
                   volFreeSpaceDataT *volFreeSpaceData,
                   u64T *areaPopulation,                   /* in/out */
                   int passNumber,
                   int unimprovedScore,
                   mlDmnParamsT *dmnParams,
                   double averageExtentCount
);

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
             mlExtentDescT *bmtXtntArray,
             int     *blksRepPerMapByte,
             int     *bmtXtntCount
            );

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
        mlDmnParamsT *dmnParams,  /* in */
        volDataT *volData);     /* in/out */

static
xtntRunDescT *
get_run_desc(
        int nominalPageCount,
        int allocatedPageCount,
        int pageOffset,
        int volume
);
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
                     u64T clearAreasize,
                     int assert_flag   /* flag to perform list checks */
);

static 
int
insert_bf_entry_criteria2(
                     bfListEntryT *newEntry, /* table entry to create */
                     int volume,
                     u64T clearAreasize,
                     int assert_flag   /* flag to perform list checks */
);

static
void
bf_clean_list(
              bfListEntryT *list_start,
              bfListEntryT *list_last,
              int volume,
              int stage
);

static
int
copy_newEntry(bfListEntryT *newEntry,
              bfListEntryT **retnewEntry)
;

static
int
delete_bf_entry(
                int volume,
                bfListEntryT *Entry,/* linked list entry to delete */
                int list  /* list number to delete from */
               )
;
