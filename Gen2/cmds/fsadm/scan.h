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

#define NL_SCAN MS_FSADM_SCAN

#define Mounted 1
#define Dismounted 2
#define Virgin 3
#define Rmvoled 4

/* Defines for scan_advfs() flag */
#define SCAN_COUNT          1
#define SCAN_MATCH          2
#define SCAN_ADDDEVS        3

#define FS_ADVFS "advfs"

/* TYPEDEFS */
typedef struct devs_i {
	char dev_path[MAXPATHLEN];	/* path of this device */
	char dev_name[FS_DIR_MAX_NAME];	/* name of this device */
        char dev_fs_name[BS_DOMAIN_NAME_SZ]; /* file system name from RBMT */
        bsIdT vdMntId;                  /* from vdAttr rec, split-mirror help */
        bsVdStatesT state;              /* from vdAttr rec, split-mirror help */
        vdIndexT vdIndex;               /* from vdAttr rec, split-mirror help */
        bsIdT umnt_thaw;                /* Time file system was last unmounted
                                         * or thawed.  Used to differentiate
                                         * disks with split-mirror copies.
                                         */
	int state_flag;			/* state the device is in */
	int correspond_flag;		/* correspond flag */
	int wrong_dir;			/* Non-zero means device is not in
					 * correct /dev/advfs/FS location. */
	char wrong_dir_name[MAXPATHLEN];
} device_info;

typedef struct filesys {
	bsIdT fs_id;		/* File system id */
	bsIdT mnt_id;		/* Time file system was last mounted */
        bsIdT umnt_thaw;        /* Time file system was last unmounted or
                                 * thawed.  Used to differentiate disks
                                 * with split-mirror copies.
                                 */
	int fs_dev_num;		/* number of disks in the file system */
	int device_count;	/* working count of devices in file system */
	int num_links;
	int version_maj;	/* version of this file system */
	int version_min;	/* version of this file system */
        uint16_t dma_dmn_state; /* from  bsDmnMAttr rec, split-mirror help */
        char dev_fs_name[BS_DOMAIN_NAME_SZ]; /* file system name from RBMT */
	char *filesys_name;	/* name of file system */
	device_info *device[256];	/* info on each device in  this fs */
} filesys_t;


typedef struct disk_tab {
        char *disk_name[FS_DIR_MAX_NAME];	/* name of device */
} disk_table_entry;


extern char *optarg;
extern int optind;

/* GLOBALS */
static advfs_ev advfs_event;
bsIdT fs;
uint32_t blkcnt;
char *state = NULL;
struct dirent *devs;
struct dirent *devss;
char *fix_fs = NULL;
char devname[MAXPATHLEN];
int diskCnt = 0;
int fs_cnt = 0;
int state_flag;
int print_geom = 0;
bsIdT mnt_id;
bsIdT umnt_thaw;
char dev_fs_name[BS_DOMAIN_NAME_SZ];

/* STATE GLOBALS */
static char never[14] = "never mounted";
static char mounted[8] = "mounted";
static char rmvoled[8] = "rmvoled";
static char dism[11] = "dismounted";

/*
 * fs_table
 * allow for finding 512 file systems on the disks to be scanned
 * create an entry for each one. This table is static except for
 * the parts_info (device table for the file system) and the filesys_name
 * which are malloc'ed
 */
filesys_t fs_table[512] = {0};

/*
 * disk_table_pt is a malloc'ed table of disk_table_entry's. It is
 * malloced for n entries where n is the number of disks entered on
 * the command line plus the number of disks found in /dev/advfs/*
 * only disks with the scan_flag set are actually scanned although
 * entries are made for all disks found in /dev/advfs (for later
 * /dev/advfs correlation with what is found on the disks)
 */
disk_table_entry *disk_table_pt;

/* LOCAL PROTOTYPES */
static statusT
scan_advfs(
    int flag, int *number_devs );

static int
parse_options(
    char *options, int *recreate, int *fix, int *undo );

static void
gather(
    bsMRT *precord );

static void
scan_cell(
    bsMCT *pdata );

static void
read_page(
    int fd, bsMPgT *ppage, int lbn );

static void
process(
    int fd );

void
scan_usage(
    void );
