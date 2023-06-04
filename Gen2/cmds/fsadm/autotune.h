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

#define FREE_RANGE_COUNT 4
#define NL_AUTOTUNE MS_FSADM_AUTOTUNE
#define IS_INUSE(bit, wd) (((wd) & (1 << (bit))))

static int program_exit_status=0;
static advfs_ev advfs_event;
static const int freeRangeTop[FREE_RANGE_COUNT] = {100,1000,10000,0};

static char *metadataRootPathName = NULL;
static arg_infoT *infop;

/* Flags for command line switches */
typedef enum {
    SWITCH_NONE		= 0x0,	/* No switches set */
    SWITCH_V		= 0x1,	/* V option */
    SWITCH_LXTNT1	= 0x2,	/* l extents option */
    SWITCH_LHOT1	= 0x4,	/* l hotfiles option */
    SWITCH_LXTNT2	= 0x8,	/* L extents option */
    SWITCH_LHOT2	= 0x10,	/* L hotfiles option */
    SWITCH_O		= 0x20	/* O option */
} switch_flag_t;

typedef struct {
    int      volIndex;  /* index number of this volume, may not be sequential */
    uint64_t volSize;          /* volume's gross size */
    uint64_t volStgCluster;    /* number of blocks each bit represents */ 
    uint64_t volFreeSize;      /*  amount of free space left on this volume */
    float    freeSizeFract;    /* percentage of free space left on volume */
    uint64_t totalHoleCount;   /* total number of holes in free space */
    uint64_t extentCount;      /* number of extents on a volume */
    uint64_t fileWithExtCount; /* number of files with extents */
    int      wrMaxIo;
    uint64_t totalIOCount;
    uint64_t totalBestIOCount;
} volDataT;

typedef struct {
            struct {
                uint64_t holeCount;
                uint64_t freeSpace;
            } rangeData[FREE_RANGE_COUNT];
            uint64_t totalHoleCount;
            uint64_t totalFreeSpace;
} volFreeSpaceDataT;

static int FSLock;
static int UnmountFlag;

static int volIndexCnt;
static int *volSubscrArray = NULL;


/*
 * local module prototypes
 */

static void
update_free_space_data (
    int, int*, int*, volFreeSpaceDataT*);

static int
open_fs (
    volDataT**, adv_bf_dmn_params_t * );

static int
set_ui_params (
    adv_ss_dmn_params_t*, char* );

static void
lock_fs (
    void );

static void
prep_filesys (
    adv_bf_dmn_params_t*, int*, int );

static void
display_extents_list (
    adv_bf_dmn_params_t );

static void
process_bmtcell(
    bsMCT*, int, uint64_t*, uint64_t* );

static int
display_extents_summary ();

static void
display_hotfiles_list (
    adv_bf_dmn_params_t );

static void
display_hotfiles_summary ( adv_bf_dmn_params_t );

static int 
request_type (
    const char* );

static void 
abort_prog (
    char*, ...);

void
autotune_usage(
    void );

static int
switches(
    switch_flag_t, char* );

static int
verify_options(
    char* );
