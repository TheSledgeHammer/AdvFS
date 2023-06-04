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
 *    vfast.h
 *
 * Date:
 *
 *    December 11, 2001
 */
/*
 * HISTORY
 */ 
#pragma ident "@(#)$RCSfile: vfast.h,v $ $Revision: 1.1.4.1 $ (DEC) $Date: 2001/12/14 16:52:09 $"

static char *Prog = "vfast";

#define BLOCK_SIZE 512
#define FREE_RANGE_COUNT 4

static int program_exit_status=0;
static advfs_ev advfs_event;
static const int freeRangeTop[FREE_RANGE_COUNT] = {100,1000,10000,0};

extern int errno;
static char *dmnName;
static char *metadataRootPathName = NULL;

typedef struct setHdr {
    mlBfTagT setTag;
    int mountedFlag;
    int unmountFlag;
    char *mntOnName;
    char *dotTagsPath;
    int fd;
} setHdrT;

static char *dotTags = "/.tags";
static int setHdrCnt = 0;
static setHdrT *setHdrTbl = NULL;

nl_catd catd;
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

typedef unsigned long u64T;
extern int errno;
extern char *sys_errlist[];
#define ERRMSG(err) sys_errlist[err]

typedef struct {
    int volIndex;      /* index number of this volume, may not be sequential */
    u64T volSize;      /* volume's gross size */
    u64T volStgCluster;/* number of blocks each bit represents */ 
    u64T volFreeSize;  /*  amount of free space left on this volume */
    float freeSizeFract; /* percentage of free space left on volume */
    u64T totalHoleCount; /* total number of holes in free space */
    u64T extentCount;    /* number of extents on a volume */
    u64T fileWithExtCount; /* number of files with extents */
    int  wrMaxIo;
    u64T totalIOCount;
    u64T totalBestIOCount;
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

static int ThisDmnLocked=0;
static int DmnLock;
static int UnlockDmnFlag;
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
open_domain (
    volDataT**, mlDmnParamsT * );

static void
get_vol_xtnt_cnt (
    mlDmnParamsT*, volDataT*, u64T*, u64T*, u64T*, u64T* );

static int
open_dot_tags_file (
    mlBfTagT );

static int
close_dot_tags_file ( );

static setHdrT *
insert_set_tag (
    mlBfTagT );

static setHdrT *
find_set_tag (
    mlBfTagT );

static char *
get_current_path_name (
    mlBfTagT, mlBfTagT, int* );

static int
set_ui_params (
    mlSSDmnParamsT*, char* );

static void
lock_domain (
    char* );

static void
verify_domain (
    char*, mlDmnParamsT*, int*, int );

static void 
verify_fileset (
    char*, char* );

static void
display_extents_list (
    int, char*, mlDmnParamsT );

static int
display_extents_summary ();

static void
display_hotfiles_list (
    mlDmnParamsT );

static void
display_hotfiles_summary ( mlDmnParamsT );

static int 
requestType (
    const char* );

static void 
abort_prog (
    char*, ...);
