/****************************************************************************
 *                                                                          *
 *  (C) DIGITAL EQUIPMENT CORPORATION 1992                                  *
 */
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
 *
 * Facility:
 *
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      Disk partition scavenger (note - code is stolen from vods)
 *
 * Date:
 *
 *      Wed Sep 14 1994
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: advscan.c,v $ $Revision: 1.1.48.1 $ (DEC) $Date: 2002/09/26 17:14:54 $";
#endif

#include <stdio.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <ufs/fs.h>
#include <sys/ioctl.h>
#include <sys/errno.h>
#include <dirent.h>
#include <sys/disklabel.h>
#include <overlap.h>

#include <msfs/bs_public.h>
#include <msfs/ms_generic_locks.h>
#include <msfs/ftx_public.h>
#include <msfs/bs_ods.h>
#include <msfs/fs_dir.h>

#include <overlap.h>
#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>
#include <msfs/advfs_evm.h>

#include <locale.h>
#include "advscan_msg.h"


nl_catd catd;

#define BB_LBNS         32      /* ADVFS - Length of primary bootstrap */
#define BB_START_LBN    64      /* Starting lbn of primary bootstrap   */
#define BB_QUAD_LENGTH  64      /* Number of quadwords in boot block    */
#define BB_SUM_LENGTH   63      /* Checksum done over first 63 quads    */
#define BB_VAX_BYTES    0x88    /* Used by VAX booting mechinism        */
#define BB_ALPHA_START  0x1e0   /* First offset used by alpha           */
#define BB_EXPANSION    (BB_ALPHA_START - BB_VAX_BYTES) /* Alpha will add */
                                /* any entensions fromhigh to low address */
#ifndef BBSIZE
#define BBSIZE  8192                    /* size of boot area, with label */
#endif


/*
 * Description of the alpha boot block.  The definition comes from the alpha
 * srm V2.1.
 *
 * For non-alpha systems declare twice as many longs to get the propper
 * structure size.
 */
typedef struct bootblock {
        union {
                struct {
                        char vax_boot_block[BB_VAX_BYTES];
                        char expansion_reserve[BB_EXPANSION];
                        long count;
                        long starting_lbn;
                        long flags;
                        long checksum;
                } bb_fields;
                struct {
                        long bb_quadwords[BB_QUAD_LENGTH];
                } bb_quads;
        } bb_def;
} BOOTBLOCK;

typedef struct parts_i {
        char part_path[MAXPATHLEN];
	char part_name[FS_DIR_MAX_NAME];
	int state_flag;
	int correspond_flag;
        int wrong_dir;  /* Non-zero means partition is not in correct
                         * /etc/fdmns/DOMAIN location. */
        char wrong_dir_name[MAXPATHLEN];
} parts_info;

typedef struct dom {
	bsIdT domain_id;
	bsIdT mnt_id;
	int disk_count;
	int number_parts;
	int num_links;
	uint32T found_parts[8];
	int version;
	char *domain_name;
	parts_info *parts[256];
} domm;

typedef struct disk_tab {
	char *disk_name[FS_DIR_MAX_NAME];
} disk_table_entry;

#define Mounted 1
#define Dismounted 2
#define Virgin 3
#define Rmvoled 4

extern char *optarg;
extern int optind;

static domm nil_domain = {0};
static advfs_ev advfs_event;
bsIdT domain;
uint32T blkcnt;
char state[14] = " ";
struct dirent *devs;
struct dirent *devss;
char *fix_domain = NULL;
char devname[MAXPATHLEN] = "/dev/";
char lsmname[MAXPATHLEN];
char lsmpath[MAXPATHLEN];
char temprawlsmname[MAXPATHLEN] = "/dev/rvol/";
char templsmname[MAXPATHLEN] = "/dev/vol/";
char devicename[MAXPATHLEN] = "/dev/r";
char fdmns_name[MAXPATHLEN] = "/etc/fdmns/";
char dom[6] = "domain";
int diskCnt = 0;
int domain_cnt = 0;
int state_flag;
int print_geom = 0;
bsIdT mnt_id;
/*
 * domain_table
 * allow for finding 512 domains on the disks to be scanned
 * create an entry for each one. This table is static except for
 * the parts_info (partition table for the domain) and the domain_name
 * which are malloc'ed
 */
domm domain_table[512];
/*
 * disk_table_pt is a malloc'ed table of disk_table_entry's. It is
 * malloced for n entries where n is the number of disks entered on
 * the command line plus the number of disks found in /etc/fdmns/*
 * only disks with the scan_flag set are actually scanned although
 * entries are made for all disks found in /etc/fdmns (for later
 * /etc/fdmns correlation with what is found on the disks)
 */
disk_table_entry *disk_table_pt;

gather(precord)
bsMRT *precord;
{
  char *pdata;
  bsDmnAttrT *ddata;
  bsVdAttrT *vdata;
  bsDmnMAttrT *mdata;
  char never[14] =   "never mounted";
  char mounted[8] = "mounted";
  char rmvoled[8] = "rmvoled";
  char dism[11] =    "dismounted";

  pdata = ((char *) precord) + sizeof(bsMRT);

  switch (precord->type) {
  case BSR_VD_ATTR: /* 3 */
	vdata = (bsVdAttrT *)pdata;
	mnt_id = vdata->vdMntId;
	if (vdata->state == BSR_VD_VIRGIN) {
		state_flag = Virgin;
		strcpy(state, &never[0]);
	}
	if (vdata->state == BSR_VD_MOUNTED) {
		state_flag = Mounted;
		strcpy(state, &mounted[0]);
	}
	if (vdata->state == BSR_VD_DISMOUNTED) {
		/*
		 * the vdMntId is cleared in vd_remove when a volume
		 * is rmvol'ed from a domain
		 */
		if ((vdata->vdMntId.tv_sec == 0) &&
			(vdata->vdMntId.tv_usec == 0)) {
			strcpy(state, &rmvoled[0]);
			state_flag = Rmvoled;
		} else {
			strcpy(state, &dism[0]);
			state_flag = Dismounted;
		}
	}
	blkcnt = vdata->vdBlkCnt;
	break;
  
  case BSR_DMN_ATTR: /* 4 */
  	ddata = (bsDmnAttrT *) pdata;
	domain.tv_sec = ddata->bfDomainId.tv_sec;
	domain.tv_usec = ddata->bfDomainId.tv_usec;
        break;

  case BSR_DMN_MATTR: /* 15 */
	mdata = (bsDmnMAttrT *)pdata; 
	diskCnt = mdata->vdCnt;
	break;

  default:
    break;
  }
}
  
/*
 * scan the partitions page 0 mcells for relevant AdvFS stuff
 */
scan_cell(pdata)
bsMCT *pdata;
{
  bsMRT *precord;

  precord = (bsMRT *) pdata->bsMR0;
  while (
	 (precord < ((bsMRT *) &(pdata->bsMR0[BSC_R_SZ]))) &&
	 (precord->type != 0)
	 ) {
    gather(precord);
    precord = (bsMRT *) (((char *)precord) + 
			 roundup(precord->bCnt,sizeof(int)));
  }
}

read_page(fd, ppage, lbn)
int fd;
bsMPgT *ppage;
int lbn;
{
  lseek(fd, ((unsigned long)lbn) * DEV_BSIZE, L_SET);
  read(fd, ppage, sizeof(bsMPgT));
} 

/*
 * process a partition 
 */
void process(fd,cnt,lp)
int fd;			/* File descriptor of partition to be processed */
int cnt;		/* Which partition (0 == a, 1 == b, ... , 7 == h) */
struct disklabel *lp;	/* Duh. */
{
    uint32T *superBlk;
    bsMPgT page;
    BOOTBLOCK *bb;
    long checksum;
    int i, boot = FALSE, cn, vers, found_one, part_num, domain_slot;
    char *j;
    parts_info *part;
    struct partition *parts;

    superBlk = (uint32T *) malloc(ADVFS_PGSZ);
    if (superBlk == NULL) {
        printf(catgets(catd, 1, 1, "no mem\n"));
        return;
    }

    /*
     * this read reads the msfs fake superblock, page 16,
     * to check for the msfs magic number
     */
    read_page(fd, superBlk, MSFS_FAKE_SB_BLK);
    if (superBlk[MSFS_MAGIC_OFFSET / sizeof( uint32T )] != MSFS_MAGIC) {
	return;
    }

    /*
     * this read reads page 0 of the device
     */
    read_page(fd, &page, 0);
    bb = (BOOTBLOCK *)&page;
    /*
     * figure out if it has an advfs boot block in it
     */
    checksum = 0;
    for (i=0; i < BB_SUM_LENGTH; i++) {
      	checksum +=
      	bb->bb_def.bb_quads.bb_quadwords[i];
    }
    if (checksum == bb->bb_def.bb_fields.checksum) {
        /*
         * The code in disklabel.c sets starting_lbn to 1 for non-bootable
         * disks. If the disk is made bootable, then the primary bootblock
         * is loaded from bootxx.advfs, and is placed at lbn 64 for advfs.
         * disklabel.c, when called with '-t advfs', checks bootxx.advfs to
         * get its size and set the appropriate value in the bootblock. The
         * check is for a size LESS THAN 33 for AdvFS (because there are only
         * 2 pages allowed on the disk for this), and is probably always going
         * to be GREATER THAN 1.
         */
    	if ((bb->bb_def.bb_fields.count <= BB_LBNS) &&
	   (bb->bb_def.bb_fields.count > 1) &&
           (bb->bb_def.bb_fields.starting_lbn == BB_START_LBN)) {
	    boot = TRUE;
        }
    }
    /*
     * now read the first page of the BMT
     */
    read_page(fd, &page, MSFS_RESERVED_BLKS);
    vers = page.megaVersion;

    diskCnt = 0;
    for (cn=0; cn<BSPG_CELLS; cn++) scan_cell(&(page.bsMCA[cn]));

    /*
     * If this disk does not have a disklabel, then it is an lsm volume, 
     * just print the info
     */
    if (lp == NULL) {

        found_one = FALSE;
        for (i = 0; i < domain_cnt + 1; i++) {
	    if ((domain_table[i].domain_id.tv_sec == domain.tv_sec) &&
	        (domain_table[i].domain_id.tv_usec == domain.tv_usec)) {
		found_one = TRUE;
		domain_slot = i;
	    }
        }
        if (!found_one) {
	    domain_table[domain_cnt].domain_id.tv_sec = domain.tv_sec;
	    domain_table[domain_cnt].domain_id.tv_usec = domain.tv_usec;
	    domain_slot = domain_cnt;
	    domain_cnt++;
        }
	part_num = domain_table[domain_slot].number_parts;
	part = (parts_info *)malloc(sizeof(parts_info));
	domain_table[domain_slot].parts[part_num] = part;

	/*
	 * Need to load diskgroup.volume and path to the device file.
	 */
	strcpy(part->part_name, lsmname);
	strcpy(part->part_path, templsmname);
	strcat(part->part_path, lsmpath);

	part->state_flag = state_flag;
	part->correspond_flag = 0; /* 0 means found partition on disk */
	domain_table[domain_slot].number_parts++;
	domain_table[domain_slot].version = vers;
	domain_table[domain_slot].mnt_id = mnt_id;
	if (diskCnt != 0) {
	    domain_table[domain_slot].disk_count = diskCnt;
	}
	
	if (print_geom)
	{
	    printf("\n%s",lsmname);
	    printf("\t%08x.%08x", domain.tv_sec, domain.tv_usec);
    	    printf(catgets(catd, 1, 7, "\tV%d, %s"), vers, state);
	    if (boot) printf(catgets(catd, 1, 3, ", bootable"));
	    printf("\n");

	    if (diskCnt != 0) {
		if (diskCnt == 1) {
		    printf(catgets(catd, 1, 4, "\t\t\t\t\t%d volume in domain\n"), diskCnt);
		} else {
		    printf(catgets(catd, 1, 5, "\t\t\t\t\t%d volumes in domain\n"), diskCnt);
		}
	    }

    	    printf(catgets(catd, 1, 8, "\n\t\tCreated\t\t\t%s "), ctime(&domain));
	    if (domain_table[domain_slot].mnt_id.tv_sec != 0) {
	    	printf(catgets(catd, 1, 9, "\t\tLast mount\t\t%s\n"),
		       ctime(&mnt_id));
	    }
	}
	return;
    }

    /*
     * find the disk partition info for this device and make sure
     * the size matches the size in the BSR_VD_ATTR record. If
     * they don't match, then this is probably an overlapping
     * partition and we will skip it.
     */
    j = (char *)&lp->d_partitions[0];
    j += (sizeof(struct partition)) * cnt;
    parts = (struct partition *) j;

    if (blkcnt == parts->p_size) {
	char part_path[MAXPATHLEN];
	char *orig_part_path;
	char *orig_part_name;
	char part_let;
	struct partition *orig_parts;
	/*
	 * look for the domain in the domain_table. Could already
	 * be there; or this might be the first partition found
	 * for this domain
	 */
	if (!print_geom && (state_flag != Rmvoled)) {
        found_one = FALSE;
        for (i = 0; i < domain_cnt + 1; i++) {
	    if ((domain_table[i].domain_id.tv_sec == domain.tv_sec) &&
	        (domain_table[i].domain_id.tv_usec == domain.tv_usec)) {
		found_one = TRUE;
		domain_slot = i;
	    }
        }
        if (!found_one) {
	    domain_table[domain_cnt].domain_id.tv_sec = domain.tv_sec;
	    domain_table[domain_cnt].domain_id.tv_usec = domain.tv_usec;
	    domain_slot = domain_cnt;
	    domain_cnt++;
        }

	/* 
	 * Check to see if this partition exactly (size & start block)
	 * overlaps another partition already in this domain's table.
	 */
	fsl_to_block_name(devname, part_path, MAXPATHLEN);
	for (i = 0 ; i < domain_table[domain_slot].number_parts ; i++) {
	    orig_part_path = domain_table[domain_slot].parts[i]->part_path;
	    orig_part_name = domain_table[domain_slot].parts[i]->part_name;
	    if (!strncmp(part_path, orig_part_path, strlen(part_path) - 1)) {
		/* These volumes are on the same device. See if they start
		 * at the same block. */
		part_let = orig_part_path[strlen(orig_part_path) - 1];
		j = (char *)&lp->d_partitions[0];
		j += (sizeof(struct partition)) * (part_let - 'a');
		orig_parts = (struct partition *) j;
		if (parts->p_offset == orig_parts->p_offset) {
		    /*
		     * This partition overlaps one we've already found.
		     * Choose one and only one.
		     */
		    if (FS_ADVFS == parts->p_fstype &&
			FS_ADVFS != orig_parts->p_fstype) {
			/* 
			 * The second partition found was marked AdvFS,
			 * while the first one wasn't.  Replace the first
			 * partition with the second.
			 */
			orig_part_path[strlen(orig_part_path) - 1] =
			    part_path[strlen(part_path) - 1];
			orig_part_name[strlen(orig_part_name) - 1] =
			    part_path[strlen(part_path) - 1];
		    }
		    return;
		}
	    }
	}

	part_num = domain_table[domain_slot].number_parts;
	part = (parts_info *)malloc(sizeof(parts_info));
	domain_table[domain_slot].parts[part_num] = part;
	strcpy(part->part_name, (strrchr(devname, '/') + 1));
	if (!strncmp(devname, "/dev/rr", 7)) {	/* unraw for old dev names */
	    strcpy(part->part_name, (strrchr(devname, '/') + 2));
	}
	strcpy(part->part_path, part_path);
	part->state_flag = state_flag;
	part->correspond_flag = 0;
	domain_table[domain_slot].number_parts++;
	if (diskCnt != 0) {
	    domain_table[domain_slot].disk_count = diskCnt;
	}
	domain_table[domain_slot].version = vers;
	domain_table[domain_slot].mnt_id = mnt_id;
	/*
 	 * if -g was specified, then print the advfs info found in this
	 * partition
	 */
	} 
	if (print_geom) {
    	    printf("\n%s", devname);
    	    printf("\t%08x.%08x",
	    	domain.tv_sec, domain.tv_usec);
    	    printf(catgets(catd, 1, 7, "\tV%d, %s"), vers, state);
    	    if (boot) printf(catgets(catd, 1, 3, ", bootable"));
    	    printf("\n");
	    if (diskCnt != 0) {
	    	if (diskCnt == 1) {
	            printf(catgets(catd, 1, 4, "\t\t\t\t\t%d volume in domain\n"), diskCnt);
	    	} else {
	            printf(catgets(catd, 1, 5, "\t\t\t\t\t%d volumes in domain\n"), diskCnt);
	    	}
	    }
    	    printf(catgets(catd, 1, 8, "\n\t\tCreated\t\t\t%s "), ctime(&domain));
	    if (mnt_id.tv_sec != 0) {
	    	printf(catgets(catd, 1, 9, "\t\tLast mount\t\t%s\n"),
		    ctime(&mnt_id));
	    }
	}
    }
}
void
usage(prog)
char *prog;
{

  printf(catgets(catd, 1, USAGE, "usage: %s [-argq] [-f domain] <disk list> <lsm diskgroup list>\n"),
	  prog);
  printf(catgets(catd, 1, 11, "Ex: %s rz10 rz11 rz12 rootdg acctdg\n"), prog);
  printf(catgets(catd, 1, 13, "Ex: %s -a              (Scans all disks found in /etc/fdmns)\n"), prog);
  printf(catgets(catd, 1, 14, "Ex: %s -a -r           (Recreates anything missing from /etc/fdmns)\n"),
	prog);
  printf(catgets(catd, 1, 15, "Ex: %s -a -f my_domain (Fixes links for my_domain)\n"), prog);
  printf(catgets(catd, 1, 16, "Ex: %s -a -g           (Prints advfs partitions by disk)\n"), prog);
}

main(argc,argv)
int argc;
char *argv[];
{
    int fd, i, j, k, n, size, disks = 0, diskgroups = 0;
    int have_label = FALSE, c, strsiz, all_devs = 0, num_opts = 0;
    int recreate = 0, no_fdmns = 0, quiet = 0, namlen = 0, fix = 0;
    DIR *FD;
    char partition[2] = " ";
    char *p, *pp;
    struct  disklabel lab;
    struct disklabel *lp;
    int max_devs = 0, partts = 0, devs_checked = 0;
    statusT sts;
    parts_info *part;
    char rawdevname[MAXPATHLEN];
    /*
     ** Get user-specified command line arguments.
     */

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_ADVSCAN, NL_CAT_LOCALE);

	/* check for root */
	if (geteuid())
	{
		fprintf(stderr, catgets(catd,1,62,
				"\nPermission denied - user must be root to run %s.\n\n"),
				argv[0]);
    	usage(argv[0]);
		exit(1);
	}

    while ((c = getopt( argc, argv, "agrqf:" )) != EOF) {
        switch (c) {

	    case 'a':
		all_devs++;
		break;

	    case 'g':
		print_geom++;
		break;

	    case 'r':
		recreate++;
		break;

	    case 'q':
		quiet++;
		break;

	    case 'f':
		fix++;
		fix_domain = optarg;
		break;

            default:
                usage(argv[0]);
                exit( 1 );
        }
    }

    num_opts = optind-1;

    if (argc < 2) {
    	usage(argv[0]);
    	exit(1);
    }

    if ((print_geom) && (fix || recreate)) {
	usage(argv[0]);
	exit(1);
    }

    for (i = 0; i < 512; i++) {
	domain_table[i] = nil_domain;
    }

    /*
     * processing a list of disks & diskgroups
     */
    disks = argc - 1 - num_opts;
    if ((disks < 1) && !all_devs) {
	usage(argv[0]);	
	exit (1);
    }
    /*
     * figure out how many disks there might be if option -a
     * was entered. Then malloc a table of the argv and /etc/fdmns disks.
     * note that scanning fdmns will give us more than the number of
     * disks, it will give us the number of partitions in use. But
     * this is the best we can do for now.
     */
    max_devs = 0;
    if (all_devs) {
	sts = scan_fdmns(1, &max_devs);

	if (sts != EOK) {
	    no_fdmns = 1;
	    all_devs = 0;
	    if (!recreate) {
	        printf(catgets(catd, 1, 20, "Can't read /etc/fdmns. You should rerun \n"));
	        printf(catgets(catd, 1, 21, "advscan with -r option (recreate fdmns)\n"));
	    }
	}
    }
    /*
     * if no disks were entered and no /etc/fdmns either then not much
     * to do
     */
    if((max_devs + disks) == 0) {
	printf(catgets(catd, 1, 22, "No devices to scan, rerun advscan with\n"));
	printf(catgets(catd, 1, 23, "devices specified\n"));
        exit (1);
    }
    /*
     * malloc and initialize the table
     */
    disk_table_pt = malloc(sizeof (disk_table_entry) * (max_devs + disks)); 
    p = (char *)disk_table_pt;
    for (i = 0; i < (sizeof (disk_table_entry) * (max_devs + disks)); i++) {
	p[i] = ' ';
    }
    /*
     * move the argv devices (if any) into the table
     */
    p = (char *)disk_table_pt;
    for (j = 0; j < disks; j++) {
	
	strcpy(&temprawlsmname[10], argv[j + num_opts + 1]);

	FD = opendir(temprawlsmname);
	if (FD == NULL) {
	    /* Not a diskgroup so it must be a device */
	    strcpy(rawdevname, argv[j + num_opts + 1]);
	    strcat(rawdevname, "a");	/* Append a partition name */
	    fsl_find_raw_name(rawdevname, p, FS_DIR_MAX_NAME);
	    p[strlen(p) - 1] = '\0';	/* Remove the partition name */
	} else {
	    /* This is a diskgroup */
	    strcpy(p, temprawlsmname);
	}
	closedir(FD);

	p += sizeof(disk_table_entry);
    }
    max_devs = disks;
    /*
     * now move the device names from /etc/fdmns into the malloc'ed table 
     */
    if (all_devs) {
    	sts = scan_fdmns(5, &max_devs);
    } 
    p = (char *)disk_table_pt;
    printf(catgets(catd, 1, 24, "\nScanning devices\t")); 
    for (i = 0; i < max_devs; i++) {
    	printf("%s ", p);
	if ((i + 1)%5 == 0) printf("\n\t\t");
	p += sizeof(disk_table_entry); 
    }
    printf("\n");
    if (print_geom) {
	printf(catgets(catd, 1, 25, "\n Partition		Domain Id\n"));
    }
    p = (char *)disk_table_pt;

    for (j = 0; j < max_devs; j++) {
	int n;
	char volPath[MAXPATHLEN+1];
	char tempPath[MAXPATHLEN+1];
	char *tempDiskGroup;
	char *temp;

	n = strncmp(p,"/dev/rvol/",10);

	if (n == 0) {
	    DIR *FD;
	    struct dirent *diskgroup;

	    /*
	     * Check all volumes in this diskgroup
	     */

	    FD = opendir(p);
	    if (FD == NULL) {
		printf(catgets(catd, 1, 18, "Can't open %s\n"),p);
		exit (1);
	    }
	    while ((diskgroup = readdir(FD)) != NULL) {
		if (diskgroup->d_name[0] == '.') {
		    continue;
		}
		strcpy(volPath, p);
		strcat(volPath,"/");
		strcat(volPath, diskgroup->d_name);

		fd = open(volPath, O_RDONLY);
		if (fd < 0) {
		    printf(catgets(catd, 1, 19, "Can't open device %s\n"), volPath);
		    break;
		}

		/*
		 *  we need to figure out the
		 *  diskgroup.  the string in p  will be in one of the two
		 *  formats:
		 *
		 *  1) /dev/vol/vol01
		 *  2) /dev/vol/diskgroup/vol01
		 *
		 *  In case 1, we know that group is rootdg.
		 *  In case 2, it is 'diskgroup'.
		 *
		 *  WARNING: We are going to use strtok to get the diskgroup.
		 *  If this code is ever made multi-threaded this will need to
		 *  be changed.
		 */
		strcpy(tempPath, volPath);

		tempDiskGroup  = strtok(tempPath, "/");
		tempDiskGroup  = strtok(NULL,"/");
		tempDiskGroup  = strtok(NULL,"/");
		temp           = strtok(NULL,"/"); 
	    
		if (NULL == temp){
		    /*
		     * In this case we have no listed diskgroup
		     * Therefor we know it is rootdg.
		     */
		    strcpy(lsmname, "rootdg.");
		    strcat(lsmname, tempDiskGroup);
		    strcpy(lsmpath, "rootdg/");
		    strcat(lsmpath, tempDiskGroup);
		} else {
		    /*
		     * We have a diskgroup so handle it.
		     */
		    strcpy(lsmname, tempDiskGroup);
		    strcat(lsmname, ".");
		    strcat(lsmname, temp);
		    strcpy(lsmpath, tempDiskGroup);
		    strcat(lsmpath, "/");
		    strcat(lsmpath, temp);
		}

		lp = NULL;

		if (fd > 0) {
		    process(fd,i,lp);
		    devs_checked++;
		    close(fd);
		}
	    }
	} else {
	    strcpy(devname, p);
	    size = strlen(devname);
	    for (i = 0; i < 8; i++) {
		partition[0] = 97 + i;
		devname[size] = partition[0]; 
		devname[size + 1] = '\0';
  		fd = open(devname, O_RDONLY);
		/*
		 * if first time thru for this volume, read block
		 * 0 for the disklabel
		 */
		if (i == 0) {
		    if (fd < 0) {
			printf(catgets(catd, 1, 19, "Can't open device %s\n"), devname);
			break;
                    }
               	    lp = &lab;
                    if (ioctl(fd, DIOCGDINFO, lp) < 0) {
			printf(catgets(catd, 1, 26, "Error reading disklabel for %s\n"),
			    devname);
		    } else {
		    	have_label = TRUE;
		    }
		}    
  		if (fd > 0) {
  		    process(fd,i,lp);
		    devs_checked++;
  		    close(fd);
		}
	    }
	}
	p += sizeof(disk_table_entry);
    }

    if (0 == devs_checked) exit( 1 );
    if (print_geom) exit( 0 );

/*
 * match up domain_table with /etc/fdmns 
 */
    sts = scan_fdmns(4, &partts);
/*
 * print out the domain table
 */
    if (!quiet && !fix) {
    printf(catgets(catd, 1, 27, "\nFound domains:\n\n"));
    for (i = 0; i < domain_cnt; i++) {
	if (domain_table[i].domain_name == NULL) {
	   printf(catgets(catd, 1, 28, "*unknown*\n"));
	} else {
	   printf("%s\n", domain_table[i].domain_name);
	}
	printf(catgets(catd, 1, 29, "\t\tDomain Id\t%08x.%08x\n"),
		domain_table[i].domain_id.tv_sec,
		domain_table[i].domain_id.tv_usec);
	printf(catgets(catd, 1, 30, "\t\tCreated\t\t%s\n"),
	    ctime(&domain_table[i].domain_id));
	printf(catgets(catd, 1, 31, "\t\tDomain volumes\t\t%d\n"),
	    domain_table[i].disk_count);
	printf(catgets(catd, 1, 32, "\t\t/etc/fdmns links\t%d\n"),
	    domain_table[i].num_links);
	printf(catgets(catd, 1, 33, "\n\t\tActual partitions found:\n"));
	for (j = 0; j < domain_table[i].number_parts; j++) {
	    part = domain_table[i].parts[j];
            if ( part->wrong_dir != 1 ) {   /* Part in correct directory. */
                printf(catgets(catd, 1, 63, "\t\t\t\t\t%s"), part->part_name);
            } else {
                printf(catgets(catd, 1, 64,
                 "\t\t\t\t\t%s   *** Incorrectly located in ***\n\t\t\t\t\t        /etc/fdmns/%s"),
                 part->part_name, part->wrong_dir_name);
            }
	    if (part->correspond_flag == 0) {
		printf("*  ");
	    }
	    if (part->state_flag == Rmvoled) {
		printf(catgets(catd, 1, 34, "  Rmvoled"));
	    }
	    printf("\n");
	}
    }
    }
/*
 * if -r, re-create anything missing from /etc/fdmns
 */
    if (recreate) {
	if (no_fdmns) {
	    if ((mkdir("/etc/fdmns", 0755) < 0) && (errno != EEXIST)) { 
		printf(catgets(catd, 1, 35, "Unable to create /etc/fdmns; [%d] %s\n"),
		    errno, BSERRMSG( errno ));
		    exit (1);
	    }
	}
    	strcpy(&fdmns_name[11], dom);
    	for (i = 0; i < domain_cnt; i++) {
	    if (domain_table[i].domain_name == NULL) {
	    /*
	     * make up a domain name
	     */
    		k = 17;
	    	for (j = 0; j < domain_table[i].number_parts; j++) {
		    if (k > MAXPATHLEN) {
		        break;
		    }
	            part = domain_table[i].parts[j];
		    if (part->state_flag == Rmvoled) continue;
		    fdmns_name[k] = '_';
	            strcpy(&fdmns_name[k + 1], part->part_name);
		    k += strlen(part->part_name) + 1;
	    	}
		fdmns_name[k] = '/';
		fdmns_name[k + 1] = '\0';
		namlen = strlen(fdmns_name);
		/*
		 * make the /etc/fdmns directory for the domain
		 */
	   	printf(catgets(catd, 1, 36, "\nCreating %s\n"), fdmns_name);
            	if ((mkdir(fdmns_name, 0755) < 0) 
			&& (errno != EEXIST)) {
                    printf(catgets(catd, 1, 37, "Unable to create %s; [%d] %s\n"),
			fdmns_name, errno, BSERRMSG(errno));
		}

		/*
		 * make the symbolic links for the domain
		 */
        	for (j = 0; j < domain_table[i].number_parts; j++) {

            	    part = domain_table[i].parts[j];
            	    printf(catgets(catd, 1, 38, "\tlinking %s\n"), part->part_name);

		    if (part->state_flag != Rmvoled) {
			strcpy(&fdmns_name[namlen], part->part_name);
			if (symlink(part->part_path, fdmns_name) < 0) {
			    printf("%s\n", part->part_path);
			    printf(catgets(catd, 1, 39, "Could not link %s in %s; [%d] %s\n"),
				   part->part_name, fdmns_name, errno,
				   BSERRMSG(errno));
			    
			}
			set_usage(part->part_path, FS_ADVFS, 1);
		    }
		}
        init_event(&advfs_event);
        advfs_event.domain = fdmns_name;
        advfs_post_user_event(EVENT_ADVSCAN_RECREATE, advfs_event, argv[0]);
	    }
	}
    }
/*
 * if -f, fix the domain specified
 */
    if (fix) {
        for (i = 0; i < domain_cnt; i++) {

	    if (domain_table[i].domain_name == NULL) continue;

	    n = strcmp(fix_domain, domain_table[i].domain_name);
	    if (n == 0) {
		printf(catgets(catd, 1, 40, "\nAttempting to fix link/dev_count for domain \n\n\t%s\n\n"),
	    fix_domain);
	    } else {
		continue;
	    }
	    /* 
	     * if don't have the domain count, don't do anything.
	     */
	    if (domain_table[i].disk_count == 0) {
		printf(catgets(catd, 1, 41, "Partition containing volume count is missing.\n"));
		printf(catgets(catd, 1, 42, "Can't fix.\n"));
		break;
	    }
	    /*
	     * num of links and domain count the same
	     */
	    if (domain_table[i].num_links == domain_table[i].disk_count) {
		/* more parts than links and count. Should be OK. */
		if (domain_table[i].number_parts > domain_table[i].disk_count) {
		    printf(catgets(catd, 1, 44, "Domain OK; will mount with %d partitions\n"),
			domain_table[i].number_parts);
		    break;
		}
		/* if they are all equal, where's the beef? */
		if (domain_table[i].number_parts == domain_table[i].disk_count) {
		    printf(catgets(catd, 1, 45, "Nothing to fix\n"));
		    break;
		}
		/* less parts than links and domain count - can't fix */
	        if (domain_table[i].number_parts < domain_table[i].disk_count) {
		    printf(catgets(catd, 1, 47, "It appears that a partition is missing from the\n"));
		    printf(catgets(catd, 1, 48, "domain. Can't fix.\n"));
		}
	    	break;
	    }
	    /*
	     * num of partitions and domain count the same
	     */
	    if (domain_table[i].number_parts == domain_table[i].disk_count) {
		if (domain_table[i].num_links > domain_table[i].number_parts) {
		    printf(catgets(catd, 1, 47, "It appears that a partition is missing from the\n"));
	  	    printf(catgets(catd, 1, 48, "domain. Can't fix.\n"));
		}
	    	if (domain_table[i].num_links < domain_table[i].number_parts) {
		    /* add the missing link(s) */
		    for (j = 0; j < domain_table[i].number_parts; j++) {
			part = domain_table[i].parts[j];
			if (part->correspond_flag == 0) {
    		    	    strcpy(&fdmns_name[11], domain_table[i].domain_name);
			    strcat(fdmns_name, "/");
		    	    strcat(fdmns_name, part->part_name);	 
			    strcpy(devname, part->part_path);
		    	    printf(catgets(catd, 1, 51, "Adding link %s for domain %s\n"),
				devname, domain_table[i].domain_name);
			    if (symlink(devname, fdmns_name) < 0) {
			    	printf(catgets(catd, 1, 39, "Could not link %s in %s; [%d] %s\n"),
				    part->part_name, fdmns_name, errno,
				    BSERRMSG(errno));
			    }
			    set_usage(part->part_path, FS_ADVFS, 1);
			}
		    }
		}
		break;
	    }
	    /*
	     * num of partitions and links the same
	     */
	    if (domain_table[i].number_parts == domain_table[i].num_links) {
	 	if (domain_table[i].number_parts < domain_table[i].disk_count) {
		/* domain thinks it has more partitions...*/
		    printf(catgets(catd, 1, 53, "The domain thinks it has more partitions than\n"));
		    printf(catgets(catd, 1, 54, "were found. Can't fix.\n"));
		}
		if (domain_table[i].number_parts > domain_table[i].disk_count) {
		    printf(catgets(catd, 1, 56, "The domain thinks it has less partitions than\n"));
		    printf(catgets(catd, 1, 54, "were found. Can't fix.\n"));
		}
	   	break;
	    }
	} /* for loop */

    	if (i == domain_cnt) {
	    printf(catgets(catd, 1, 57, "\nNo domain by name %s found, can't fix\n"), fix_domain);
	    exit (1);
    	}
    } /* if fix */

    exit (0);
}
/*
 * scan /etc/fdmns. 
 * If flag = 1, count up the disks. 
 * If flag = 3, put the non-lsm disks in the device_table.
 * if flag = 4, match up domain_table with /etc/fdmns
 * if flag = 5, put lsm and non-lsm disks in device table, new format
 */
statusT
scan_fdmns(
	  int flag,
  	  int *number_devs
	  )
{
    DIR *FD, *FDD;
    int fd, n, i, j, got_it, got_domain, domain_num = 0;
    char *tab_ptr, *p;
    char save_disk[FS_DIR_MAX_NAME];
    parts_info *part;
    char domain_part_path[MAXPATHLEN];
    char dev_link_path[MAXPATHLEN];

    FD = opendir("/etc/fdmns");
    if (FD == NULL) {
	printf(catgets(catd, 1, 58, "Can't open /etc/fdmns\n"));
  	return (-1);
    }
    while ((devs = readdir(FD)) != NULL) {
	if (devs->d_name[0] == '.') {
	    continue;
	}
	strncpy(&fdmns_name[11], devs->d_name, devs->d_namlen);
	fdmns_name[11 + devs->d_namlen] = '\0';
  	FDD = opendir(fdmns_name);
    	if (FDD == NULL) {
	    printf(catgets(catd, 1, 59, "Can't open domain dir %s\n"), devs->d_name);
            continue;
	}
	got_domain = FALSE;
	while ((devss = readdir(FDD)) != NULL) {
	    if (devss->d_name[0] == '.') {
	        continue;
	    }

	    /*
	     * Determine the device path which the
	     * domain's partition is linked to.
	     */
	    strcpy(domain_part_path, fdmns_name);
	    strcat(domain_part_path, "/");
	    strcat(domain_part_path, devss->d_name);
	    if (readlink(domain_part_path, dev_link_path, MAXPATHLEN) < 0) {
		printf(catgets(catd, 1, 60, "Can't readlink %s\n"),
		       domain_part_path);
		continue;
	    }

	    if (flag == 3) {
		/* move each 'unique' disk into disk_table */
	 	p = (char *)disk_table_pt;
		strncpy(&save_disk[0], devss->d_name, devss->d_namlen - 1);
		save_disk[devss->d_namlen - 1] = '\0';
		got_it = FALSE;
		for (i = 0; i < *number_devs; i++) {
		    n = strcmp(save_disk, p);
		    /* if n = 0, it's the same disk */
		    if (n == 0) {
			got_it = TRUE;
			continue;		    
		    }
		    p += sizeof(disk_table_entry);
		}
		if (got_it) continue;
	    	tab_ptr = (char *)disk_table_pt + (sizeof(disk_table_entry) * (*number_devs) );
	    	strncpy(tab_ptr, devss->d_name, devss->d_namlen - 1);
		tab_ptr += devss->d_namlen - 1;
		tab_ptr[0] = '\0';
	    }
	    if (flag == 4) {
		got_it = FALSE;
		/* scan the domain table (all partitions in all domains)
		 *  for this partition
		 */
		if (got_domain == FALSE) {
		    for (i = 0; i < domain_cnt; i++) {
		        for (j = 0; j < domain_table[i].number_parts; j++) {
			    part = domain_table[i].parts[j];
			    n = strcmp(devss->d_name, part->part_name);

			    if (n == 0) {
			    	got_it = TRUE;
			    	part->correspond_flag = 1;
				++domain_table[i].num_links;
                                if ( domain_table[i].domain_name == NULL ) {
                                    domain_table[i].domain_name = malloc(devs->d_namlen);
                                    strcpy(domain_table[i].domain_name, devs->d_name);
                                    got_domain = TRUE;
                                    domain_num = i;
                                } else if ( strcmp(devs->d_name, domain_table[i].domain_name) != 0 ) {
                                    /* Name should match.  If not, a partition
                                     * resides in another directory.
                                     * Warn the user later on that the mount of
                                     * this domain will fail.
                                     */
                                    part->wrong_dir = 1;
                                    strcpy(part->wrong_dir_name,devs->d_name);
                                }
			    }
		    	    if (got_it) goto x;
		        }
		    }
		    /* already in the domain, just look for the partition */
		} else {
		    ++domain_table[domain_num].num_links;
		    for (j == 0; j < domain_table[domain_num].number_parts; j++) {
                        part = domain_table[domain_num].parts[j];
                        n = strcmp(devss->d_name, part->part_name);
                        if (n == 0) {
                            got_it = TRUE;
                            part->correspond_flag = 1;
			goto x;
                        }
		    }
		}
	    }
	    if (flag == 5) {
		int err;
		char volPathName[MAXPATHLEN+1];
		char volLinkName[MAXPATHLEN+1];
		struct stat stats;    
		char rawVolName[MAXPATHLEN+1];
		char *raw;
		unsigned long lsmSize;
		int  linkSize;

		strcpy(volPathName,fdmns_name);
		strcat(volPathName,"/");
		strcat(volPathName,devss->d_name);

		err = readlink(volPathName, volLinkName, MAXPATHLEN+1);
		if (err < 0) {
		    printf(catgets(catd, 1, 60, "Can't readlink %s\n"),
			   volLinkName);
		}

		/* move each 'unique' disk into disk_table */
	 	p = (char *)disk_table_pt;

		raw = fsl_to_raw_name(volLinkName, rawVolName, MAXPATHLEN);
		if (!raw) {
		    printf(catgets(catd, 1, 61, "Could not get raw device name for %s\n"),
			   volLinkName);
		}

		linkSize = strlen(rawVolName);

		if (islsm64(raw, &lsmSize)) {
		    char tempPath[MAXPATHLEN+1];
		    char *tempDiskGroup;
		    char *temp;
		    
		    strcpy(save_disk,"/dev/rvol/");

		    /*
		     *  diskgroup.  the string in raw  will be in one of the 
		     *  two formats:
		     *
		     *  1) /dev/rvol/vol01
		     *  2) /dev/rvol/diskgroup/vol01
		     *
		     *  In case 1, we know that group is rootdg.
		     *  In case 2, it is 'diskgroup'.
		     *
		     *  WARNING: We are going to use strtok to get the 
		     *  diskgroup.  If this code is ever made multi-threaded
		     *  this will need to be changed.
		     */
		    strcpy(tempPath, raw);

		    tempDiskGroup  = strtok(tempPath, "/");
		    tempDiskGroup  = strtok(NULL,"/");
		    tempDiskGroup  = strtok(NULL,"/");
		    temp           = strtok(NULL,"/"); 
	    
		    if (NULL == temp){
		      /*
		       * In this case we have no listed diskgroup
		       * Therefor we know it is rootdg.
		       */
		      strcat(save_disk, "rootdg");
		    } else {
		      /*
		       * We have a diskgroup so handle it.
		       */
		      strcat(save_disk, tempDiskGroup);
		    }
		    strcat(save_disk,"\0");
		} else {
		    /* 
		     * Now we know it is NOT, so convert to
		     * partition a
		     */
		    strncpy(save_disk, rawVolName, linkSize - 1);
		    save_disk[linkSize - 1] = '\0';
		}

		got_it = FALSE;
		for (i = 0; i < *number_devs; i++) {
		    n = strcmp(save_disk, p);
		    /* if n = 0, it's the same disk */
		    if (n == 0) {
			got_it = TRUE;
			continue;		    
		    }
		    p += sizeof(disk_table_entry);
		}
		if (got_it) continue;

	    	tab_ptr = (char *)disk_table_pt + (sizeof(disk_table_entry) * (*number_devs) );
	    	strncpy(tab_ptr, save_disk, strlen(save_disk));
		tab_ptr += strlen(save_disk);
		tab_ptr[0] = '\0';
	    }

x:
	    (*number_devs)++;
	}
	closedir (FDD);
    }
    closedir(FD);
    return (EOK);
}
