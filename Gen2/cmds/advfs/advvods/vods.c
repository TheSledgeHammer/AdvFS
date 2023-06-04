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

/*  Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P. */

#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stropts.h>
#include <assert.h>
#include <dirent.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/file.h>
#include <sys/mount.h>
#include <sys/fs.h>
#include <sys/diskio.h>
#include <sys/ioctl.h>
#include <advfs/bs_public.h>
#include <advfs/fs_dir.h>
#include <advfs/ms_privates.h>
#include <advfs/advfs_syscalls.h>

#include "vods.h"

extern *sys_errlist;

bfTagT staticRootTagDirTag = {-BFM_BFSDIR, 0};

#define PRINT_BMT_OR_RBMT ( BS_BFTAG_RBMT(bfp->fileTag) ? "RBMT" : "BMT")

bfTagT NilBfTag = { 0, 0 };


static int getopts(int, char* const*, char const*);
static int init_dmn(bitFileT*);
static int setup_src(bitFileT *bfp, int argc, char **argv, 
		     int *optindp, int rawflg);
static int open_dmn(arg_infoT*, bitFileT*, char *);
static void check_vol(bitFileT*, vdIndexT, bfDomainIdT*, int*);
static int open_vol(bitFileT*, char*, vdIndexT*);
static int test_advfs_magic_number(int, char*);
static void get_vol_blocks(volT*, char*);
static int get_vol_size(volT *volp);
static int find_vdi(bitFileT*, bsMPgT*, char*, vdIndexT*);
static int load_rbmt_xtnts(bitFileT*);
static int read_vol_at_blk(bitFileT*);
static int load_clone_xtnts(bitFileT*);
static void merge_clone_xtnts(bitFileT*, bitFileT*);
static int get_bsr_xtnts(bitFileT*, int*, bfMCIdT*, int*);
static int load_bsr_xtnts(bitFileT*, int*, bsMCT*, bfMCIdT*, int*);
static int get_xtra_xtnts(bitFileT*, int, bfMCIdT*, int, int);
static int load_xtra_xtnts(bitFileT*, int*, bsMCT*, bfMCIdT*, int);
static void get_xtnts(bitFileT*, int*, vdIndexT, bsXtntT*, int);
static int resolve_vol(bitFileT*, char*);
static int resolve_fle(bitFileT*, char*, struct stat*);
static int find_saveset_TAG(bitFileT*);
static int find_set_by_name(bitFileT*, char*, bitFileT*);
static int get_orig_set(bitFileT*, uint64_t, bitFileT*);
static int get_prim_from_tagdir(bitFileT*, bfTagT*, bitFileT*);
static int find_file_by_name(bitFileT*, char*, bitFileT*);
static int find_prim_from_dot_tags(char*, bitFileT*, bitFileT*, bitFileT*);
static int find_rsrv_prim_from_tag(bitFileT*, bfTagT);
static int search_dir(bitFileT*, bitFileT*, char*);
static int get_savset_file(bitFileT*, char*);
static int test_bmt_page(bitFileT*, char*, int);
static int test_mcell(bitFileT*, char*, bfMCIdT, bsMCT*);
static int test_mcid(bitFileT*, char*, bfMCIdT);
static int test_vdi(bitFileT*, char*, vdIndexT);
static int test_blk(bitFileT*, vdIndexT, uint64_t, bf_fob_t);

/*****************************************************************************/
/*
** Input: bitfile pointer
** Output: bitfile is initialized
**         pointers to malloced space are nulled (dmn, xmap.xtnt)
**         type set to allow all types
**         other fields set to illegal values
** Errors: no errors
** Return: none
*/
void
init_bitfile(bitFileT *bfp)
{
    assert(bfp);

    bfp->type = FLE | DMN | VOL;
    
    bfp->fileName[0] = '\0';
    bfp->setName[0] = '\0';
    bfp->pmcell.volume = 0;
    bfp->pmcell.page = PAGE_TERM;
    bfp->pmcell.cell = CELL_TERM;
    bfp->pages = BMT_PG_TERM;
    bfp->fileTag = NilBfTag; 
    bfp->setTag  = NilBfTag;
    bfp->setp = NULL;
    bfp->origSetp = NULL;
    bfp->dmn = NULL;
    bfp->xmap.cnt = 0;
    bfp->xmap.xtnt = NULL;
    bfp->fd = -1;
    bfp->pgLbn = LBN_BLK_TERM;
    bfp->bfPgSz = ADVFS_METADATA_PGSZ_IN_FOBS;  /* need to CHANGE to meta/user */
    bfp->pgNum = BMT_PG_TERM;
    bfp->pgVol = 0;
    bfp->pgBuf = NULL;
    bfp->next = NULL;
}

/*****************************************************************************/
/*
** Copies some of the contents of a bitFile to another bitFile.
**
** Input: srcBfp is set up. type, dmn & setTag have been set up
** Output: destBfp gets srcBfp type, dmn & setTag. other values are initialized
** Errors: no errors
** Return: none
*/
void
newbf(bitFileT *srcBfp, bitFileT *destBfp)
{
    assert(srcBfp);
    assert(destBfp);
    assert(srcBfp->type != 0);
    assert(srcBfp->dmn);

    init_bitfile(destBfp);

    destBfp->type = srcBfp->type;
    destBfp->dmn = srcBfp->dmn;
    destBfp->setTag = srcBfp->setTag;
}

/******************************************************************************/
/*
** resolve_src...
**
** Looks at the first few option flags and sets cooresponding bits in
** the passed in flag reference "flagp".  Also figures out what the
** source of metadata information to this command is (dump file,
** filesystem, volume) and fills in the appropriate values in the
** passed in "bfp" structure..
**
** Input: bfp - initialized
**        argv, argc - command line arguments
**        optindp - pointer to argv index
**        optstr - string of accepted options
** Output: bfp->type - DMN, VOL, FLE
**         bfp->domain - filled in if source is a filesystem
**         flagp - set for options flags found  VFLG, CCFLG, TTFLG
** Errors: prints and exist for format errors
**         prints error for all errors
** Return: OK - proper structure for domain, volume, file
**         ENOENT - no such domain, volume, file
**         EACCES - can't access "name" due to permissions
**         EBUSY - block device is mounted
**         BAD_DISK_VALUE - bad magic or other values not as expected
*/
int
resolve_src(int argc, char **argv, int *optindp, char *optstr, bitFileT *bfp,
            int *flagp)
{
    int ret;
    char c;
    int typeFlg = 0;
    int rawFlg = 0;
    char *name = NULL;
    int64_t longnum;

    while ( (c = getopts( argc, argv, optstr)) != EOF ) {
        switch ( c ) {

          case 'f':   /* next argument is a dump file */
            typeFlg = FLE;
            if ( optarg == NULL ) {
                fprintf(stderr, "Missing file name\n");
                usage();
            }

            name = optarg;
            bfp->type = FLE;

	    /* go get src info.  Use "raw flag" cuz this is a dump
	       file so we don't care if the FS is mounted. */

	    (*optindp)--;
            ret = setup_src(bfp, argc, argv, optindp, 1);
            if ( ret != OK && ret != BAD_DISK_VALUE ) {
                exit(ret);
            }
            break;

          case 'v':		/* verbose */
            *flagp |= VFLG;	/* print all information in the mcells */
            break;

          case 'r':		/* raw */
            rawFlg = 1;
            break;

          case 'T':
            *flagp |= TTFLG;
            break;

          case 'C':
            *flagp |= CCFLG;
            break;

          default:
            usage();
        }
    }

    if ( name == NULL ) {
        if ( *optindp >= argc ) {
            fprintf(stderr, "Domain, volume or file name required.\n");
            usage();
        }

        if ( argv[*optindp][0] == '-' ) {
            if ( strlen(argv[*optindp]) == 2 ) {
                fprintf(stderr, "Unknown option \"%s\"\n", argv[*optindp]);
            } else {
                fprintf(stderr, "Unknown option flag '%c' in \"%s\"\n",
                  optopt, argv[*optindp]);
            }
            usage();
        }

	ret = setup_src(bfp, argc, argv, optindp, rawFlg);
    }

    /* Check "ret" from setup_src() */

    if ( ret != OK && ret != BAD_DISK_VALUE ) {
        exit(ret);
    }

    /* Again check ret from setup_src().  At this point if the
       volume has serious AdvFS format errors, bail. */
    if ( ret != OK ) {
        exit(ret);
    }

    return OK;

} /* resolve_src */

/*****************************************************************************/
/*
** Match as many options as possible then return EOF
**
** Input: argv, argc - command line options
**        optstring - string of valid options
**        global optind - index to argv
** Output: global optarg - points to required option
**         global optind - may be incremented
**         global opopt - option character found
** Error: no printf
** Return: the option found
**         EOF - no more recognized options
*/
static int
getopts( int argc, char * const *argv, char const *optstring)
{
    static int stringind = 1;
    int c;
    char *cp;

    /* if no more args or no more - options or dash w/o option then EOF */
    if ( stringind == 1 ) {
        if ( optind >= argc ||
             argv[optind] == NULL ||
             argv[optind][0] != '-' ||
             argv[optind][1] == '\0')
        {
            return EOF;
        }
    }

    optopt = c = argv[optind][stringind];
    if ( (cp = strchr(optstring, c)) == NULL ) {
        /* check for unrecognized options */
        stringind = 1;
        return EOF;
    }

    if ( *++cp == ':' ) {
        /* Option requires a parameter.  There may or may not be spaces */
        /* between the option and the parameter. */
        if ( argv[optind][stringind+1] != '\0' )
            /* no blanks separate option and parameter */
            optarg = (char*)&argv[optind++][stringind+1];
        else if ( ++optind >= argc ) {
            /* No option but one is required. Set optarg to null. */
            optarg = NULL;
        } else {
            optarg = (char*)argv[optind++];
        }
        stringind = 1;
    } else if ( *cp == ';' ) {
        /* Option may have an optional parameter. */
        /* Parameter (if it exists) follows option with no spaces. */
        if ( argv[optind][stringind+1] != '\0' ) {
            optarg = (char*)&argv[optind][stringind+1];
        } else {
            optarg = NULL;
        }
        stringind = 1;
        optind++;
    } else {                        /* parameter not needed */
        /* if c is the last option update optind */
        if (argv[optind][++stringind] == '\0') {
            stringind = 1;
            optind++;
        }
        optarg = NULL;
    }

    return c;
}

/*****************************************************************************/
/*
** Malloc and init the dmn element of a bitFileT structure.
**
** Input: bfp = the bitFileT in which to init the dmn element
** Output: bfp->dmn malloced and its elements initialized
** Errors: diagnostic output to stderr for all errors
** Return: OK - all went well
**         ENOMEM - malloc failed
*/
static int
init_dmn(bitFileT *bfp)
{
    int i;

    assert(bfp->dmn == NULL);

    bfp->dmn = malloc(sizeof(DmnT));
    if ( bfp->dmn == NULL ) {
        perror("init_dmn: malloc failed");
        return ENOMEM;
    }

    bfp->dmn->dmnVers.odv_major = bfp->dmn->dmnVers.odv_minor = 0;
    bfp->dmn->dmnName[0] = '\0';
    for ( i = 0; i < BS_MAX_VDI; i++ ) {
        bfp->dmn->vols[i] = NULL;
    }

    return OK;
}

/*****************************************************************************/
/*
** resolve_name...
**
** The command line provides for a metadata source which can be a
** volume, a domain name, or a dump file.  This function determines
** which it is and sets the bfp->type field to reflect that.
**
** If the name is a domain, we malloc and fill in bfp->dmn, set
** bfp->type to DMN, and malloc/init bfp->dmn->vols[] for all volumes.
** The rbmt and rbmt extents (if present) are filled in for all
** volumes.
**
** If the name is a volume we malloc and fill in bfp->dmn and
** bfp->dmn->vols[] for the one volume.  The index will be the vdi
** read from the volume.  If the volume isn't a good AdvFS volume, the
** vdi will be 0.  bfp->type will be set to VOL.
**
** If the name is a file, resolve_name does not malloc bfp->dmn.
** The fd is set to the fd of the file.
**
** The calling sequence is:
**     resolve_name => open_dmn => open_vol
**     resolve_name => resolve_vol => open_vol
**     resolve_name => resolve_fle
**
** Input: bfp->type == FLE if the user explicitly flagged a dump file
**        "name" is domain, volume, file or directory
**        rawflg - open volumes in domain as character devices
**
** Output: bfp->type
**         if DMN, malloc bfp->dmn. malloc bfp->dmn->vols[] for all volumes
**                 open all volumes. read vdi from BS_VD_ATTR
**                 bfp->dmn->vols[]->fd, blocks, 
**                 malloc & init bfp->dmn->vols[]->rbmt
**                 malloc and fill bfp->dmn->vols[]->rbmt->xmap
**         if VOL, malloc bfp->dmn. malloc bfp->dmn->vols[] for one volume
**                 open volume. read vdi from BS_VD_ATTR
**                 bfp->dmn->vols[vdi]->fd, blocks, 
**         if FLE, bfp->fd, pages
**
** Errors: diagnostic output to stderr for all errors
**
** Return: OK
**         ENOENT - no such domain, volume, file
**         EACCES - can't access "name" due to permissions
**         EBUSY - block device is mounted
**         BAD_DISK_VALUE - bad magic or other values not as expected
*/
int
resolve_name(bitFileT *bfp, char *name, int rawflg)
{
    int stat_ret;
    int status;
    struct stat  statbuf;
    arg_infoT *fsInfo;
    adv_mnton_t *mntArray;

    if (bfp->type == FLE) {

	/* the source is a file name (user spcified "-f") */

	stat_ret = stat(name, &statbuf);
	
	if (stat_ret != -1 && S_ISREG(statbuf.st_mode)) {
	    return resolve_fle(bfp, name, &statbuf);
	}
	
	return ENOENT;
    }

    /* 
     * The user did not explicitly specifiy the source as a dump
     * file by using the '-f' option.  (normally done if there's a
     * name conflict between the dump file and a domain name).
     * The source is a storage domain name or full path, a volume
     * name, or a dump file without the '-f' option.
     */
	
    fsInfo = process_advfs_arg(name);

    if (fsInfo) {

	/*
	 * if any part of the domain is mounted then the user must
	 * specify "-r", unless the source is a dump file.
	 */

	if (!rawflg) {
	    int i;

	    status = advfs_get_all_mounted(fsInfo, &mntArray);

	    if (status == -1) {
		printf ("'%s' mount check failed.\n", fsInfo->fsname);
		exit(1);
	    }
	
	    if (status > 0) {
		printf("\n\tStorage domain '%s' has %d mounted file %s:\n\n",
		       fsInfo->fsname, status, 
		       status > 1 ? "systems" : "system");
    
		for (i = 0; i < status; i++) {
		    printf("\t%s mounted on %s\n", 
			   mntArray[i].fspath, mntArray[i].mntdir);
		}

		printf("\n\tUse the -r option for a mounted file system.\n");
		printf("\tNOTE: metadata dumped from a mounted file system\n");
		printf("\tmay be inconsistent.\n\n");
		exit(1);
	    }
	}

	/* 
	 * The user entered either a domain or volume name. We stat
	 * that string and if it is a block device but not the AdvFS
	 * block device (e.g.: /dev/advfs/domain/default), then the
	 * user entered a volume device.
	 */

	stat_ret = stat(name, &statbuf);
	
	if (stat_ret != -1 && S_ISBLK(statbuf.st_mode) &&
	    major(statbuf.st_dev) != ADVFSDEV_MAJOR) {
		bfp->type = VOL;
		return resolve_vol(bfp, name);
	}

	/* the user entered a domain name, open it... */

	bfp->type = DMN;
	return open_dmn(fsInfo, bfp, name);
    }

    /* If process_advfs_arg returns a NULL, the source is neither a
     * volume path nor FS, but it might be a file... */

    stat_ret = stat(name, &statbuf);
	
    if (stat_ret != -1 && S_ISREG(statbuf.st_mode)) {
	bfp->type = FLE;
	return resolve_fle(bfp, name, &statbuf);
    }

    fprintf(stderr, "Invalid metadata source: \"%s\"\n", name);
    return ENOENT;

} /* end: resolve_name */

/*****************************************************************************/
/*
** setup_src...
**
** HISTORY: this function used to be resolve_name.  We wanted to
** extend that function so that it could handle the volume spec
** "domain_name vol_index" more consistently for advvods in addition
** to volume special file paths.  That forced us to change the call
** interface.  Since resolve_name() is called from outside advvods
** (ncheck), we left that function alone and created this one to hold
** our changes.
**
** ABSTRACT: The command line provides for a metadata source which can
** be a volume special file path, a domain name, a domain name with a
** volume index, or a dump file.  This function determines which it is
** and loads the bfp structures accordingly.
**
** If the name is a domain, we malloc and fill in bfp->dmn, set
** bfp->type to DMN, and malloc/init bfp->dmn->vols[] for ALL volumes.
** The rbmt and rbmt extents (if present) are filled in for ALL
** volumes.
**
** If the name is a volume special file path OR a domain with an index
** number, we malloc and fill in bfp->dmn and bfp->dmn->vols[] for
** that ONE volume.  The index will be the vdi read from the volume.
** If the volume isn't a good AdvFS volume, the vdi will be 0.
** bfp->type will be set to VOL.
**
** If the name is a file (dump file), resolve_name does not malloc
** bfp->dmn.  The fd is set to the fd of the file.
**
** The calling sequence is:
**     setup_src => open_dmn => open_vol
**     setup_src => resolve_vol => open_vol
**     setup_src => resolve_fle
**
** Input: bfp->type == FLE if the user explicitly flagged a dump file
**        "name" is domain, volume, file or directory
**        rawflg - open volumes in domain as character devices
**
** Output: bfp->type
**         if DMN, malloc bfp->dmn. malloc bfp->dmn->vols[] for all volumes
**                 open all volumes. read vdi from BS_VD_ATTR
**                 bfp->dmn->vols[]->fd, blocks, 
**                 malloc & init bfp->dmn->vols[]->rbmt
**                 malloc and fill bfp->dmn->vols[]->rbmt->xmap
**         if VOL, malloc bfp->dmn. malloc bfp->dmn->vols[] for one volume
**                 open volume. read vdi from BS_VD_ATTR
**                 bfp->dmn->vols[vdi]->fd, blocks, 
**         if FLE, bfp->fd, pages
**
** Errors: diagnostic output to stderr for all errors
**
** Return: OK
**         ENOENT - no such domain, volume, file
**         EACCES - can't access "name" due to permissions
**         EBUSY - block device is mounted
**         BAD_DISK_VALUE - bad magic or other values not as expected
*/

static int
setup_src(bitFileT *bfp, int argc, char **argv, int *optindp, int rawflg)
{
    int stat_ret;
    int status;
    char *source;
    char *devname;
    struct stat  statbuf;
    arg_infoT *fsInfo;
    adv_mnton_t *mntArray;
    int64_t longnum;

    source = argv[*optindp];
    (*optindp)++;

    if (bfp->type == FLE) {

	/* the source is a file name (user spcified "-f") */

	stat_ret = stat(source, &statbuf);
	
	if (stat_ret == -1) {
	    printf("Can not stat '%s': %s\n", source, sys_errlist[errno]);
	    return ENOENT;
	}

	if (!(S_ISREG(statbuf.st_mode))) {
	    printf("'%s' is not a regular file.\n", source);
	    return ENOENT;
	}

	return resolve_fle(bfp, source, &statbuf);
    }

    /* 
     * The user did not explicitly specifiy the source as a dump
     * file by using the '-f' option.  (normally done if there's a
     * name conflict between the dump file and a domain name).
     * The source is a storage domain name or full path, a volume
     * name, or a dump file without the '-f' option.
     */
	
    fsInfo = process_advfs_arg(source);

    if (fsInfo) {

	/*
	 * if any part of the domain is mounted then the user must
	 * specify "-r", unless the source is a dump file.
	 */

	if (!rawflg) {

	    int i;

	    status = advfs_get_all_mounted(fsInfo, &mntArray);

	    if (status == -1) {
		printf ("'%s' mount check failed.\n", fsInfo->fsname);
		exit(1);
	    }
	
	    if (status > 0) {
		printf("\nStorage domain '%s' has %d mounted file %s:\n",
		       fsInfo->fsname, status, 
		       status > 1 ? "systems" : "system");
    
		for (i = 0; i < status; i++) {
		    printf("  %s mounted on %s\n", 
			   mntArray[i].fspath, mntArray[i].mntdir);
		}

		printf("\nUse the -r option for a storage domain with mounted file system(s).\n");
		printf("NOTE: Metadata from a storage domain with mounted file systems\n");
		printf("      may appear inconsistent.\n\n");
		exit(1);
	    }
	}

	/* 
	 * The user entered either 1) storage domain name or 2) volume
	 * spec. A volume spec is either a special device file full
	 * path name, or a domain name followed by a number, which is
	 * the volume's index number.  
	 *
	 * We stat the string and first see if it's a device but NOT
	 * the AdvFS device (e.g.: /dev/advfs/dmn/default).
	 */

	stat_ret = stat(source, &statbuf);
	
	if (stat_ret != -1 && 
	    (S_ISBLK(statbuf.st_mode) || S_ISCHR(statbuf.st_mode)) &&
	    major(statbuf.st_rdev) != ADVFSDEV_MAJOR) {
		bfp->type = VOL;
		return resolve_vol(bfp, source);
	}

	/* the user has entered a domain name.  It could be a domain
	   name standing alone, or a domain name plus volume index,
	   which we'll need to resolve to a volume.  In the latter
	   case, we gather load the domain info (open_dmn) so we can
	   see the volume configuration, and if the volume index is
	   valid we wipe out the domain info, and resolve the target
	   volume only.  This is done in order to exactly match the
	   case where a user specified a volume special file */

	bfp->type = DMN;

	if (open_dmn(fsInfo, bfp, source) != OK) {
	    exit(BAD_PARAM);
	}

	if (getnum(argv[*optindp], &longnum) == OK ) {

	    bfp->pmcell.volume = longnum;
	    (*optindp)++;

	    if (test_vdi(bfp, NULL, (vdIndexT) longnum) != OK) {
		exit(BAD_PARAM);
	    }

	    devname = strdup(bfp->dmn->vols[longnum]->volName);
	    free(bfp->dmn);
	    bfp->dmn = NULL;
	    bfp->type = VOL;
	    return resolve_vol(bfp, devname);
	}

	/* the user must have enetered a storage domain name without a
	   volume index.  We've already fetched the domain info so
	   we're done. */

	return OK;
    }

    /* If process_advfs_arg() call above returned a NULL, the source
     * is not a domain name and also not a volume associated with a
     * domain name.  It might still be a dumpfile name entered without
     * the "-f" switch or a volume that is not part of any
     * domain... */

    stat_ret = stat(source, &statbuf);
	
    if (stat_ret != -1 && S_ISREG(statbuf.st_mode)) {
	/* dump file */
	bfp->type = FLE;
	return resolve_fle(bfp, source, &statbuf);

    } else if (stat_ret != -1 && 
	       (S_ISBLK(statbuf.st_mode) || S_ISCHR(statbuf.st_mode))) {
	/* a volume without a domain */
	bfp->type = VOL;
	return resolve_vol(bfp, source);
    }

    fprintf(stderr, "Invalid metadata source: \"%s\"\n", source);
    return ENOENT;

} /* end: setup_src */

/*****************************************************************************/
/*
** Open the volumes named in the in the domain storage dir.
** Malloc the bfp->dmn structure. For each block device, open the volume
** and fill in bfp->dmn->vols[vdi]. If the rwflg is set open the character
** device for each volume instead.
** open_dmn is called by resolve_name and calls open_vol.
**
** Input: fsInfo - contains the FS name and directory info
** Input: bfp - bitfile pointer for the domain info
** Input: name - the name of the domain or volume from the command line
** Output: bfp->fd set to -1			WHY
**         malloc bfp->dmn
**         malloc and fill in bfp->dmn->vols[]  for each volume
**         fill in bfp->dmn->dmnName
** Errors: diagnostic output to stderr for all errors
** Return: OK - if all goes well
**         EBUSY - block device is mounted
**         ENOENT - no such block device
**         ENOMEM - if malloc fails
**         BAD_DISK_VALUE - bad magic or other unexpected value
*/
static int
open_dmn(arg_infoT *fsInfo, bitFileT *bfp, char *name)
{
    DIR *stgDir;
    struct dirent *dp;
    char *cp;
    char errstr[80];
    char dirname[MAXPATHLEN];   /* used to make /dev/advfs/<dmn>/<vol> path */
    char volName[MAXPATHLEN];   /* used to make domain volume blk dev name */
    vdIndexT vdi;
    struct stat statBuf;
    bfDomainIdT dmnId;
    int volCnt;
    int volsOpened;
    int ret;

    errstr[0] = '\0';       /* just in case sprintf fails */
    dmnId.id_sec = 0;
    dmnId.id_usec = 0;
    volCnt = 0;
    volsOpened = 0;

    bfp->fd = -1;
    ret = init_dmn(bfp);
    if ( ret != OK ) {      /* init_dmn can fail with ENOMEM */
        return ret;
    }

    strcpy(bfp->dmn->dmnName, fsInfo->fsname); 
    assert(bfp->dmn->dmnName);

    stgDir = opendir(fsInfo->stgdir);
    if ( stgDir == NULL ) {
        sprintf(errstr, "open_dmn: opendir \"%s\" failed", fsInfo->stgdir);
        perror(errstr);
        return ENOENT;
    }

    /* Read the contents of the domain storage directory. */
    for ( dp = readdir(stgDir); dp != NULL; dp = readdir(stgDir) ) {

        /* Ignore "." & "..", just keep reading. */

        if ( strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0 ) {
            continue;
        }

        /* Construct the full path path name of each domain volume. */

        strcpy(dirname, fsInfo->stgdir);
        strcat(dirname, "/");
        cp = &dirname[strlen(dirname)];
        *cp = '\0';
        strcat(dirname, dp->d_name);


	/* poke the file to see if it's a block device */

        if (stat(dirname, &statBuf) < 0) {
            sprintf(errstr, "open_dmn: stat failed for %s", dirname);
            perror(errstr);
            continue;
        }

        /*
         * Skip if not block device 
         */
        if (!S_ISBLK(statBuf.st_mode)) {
            continue;
        }

	/* get the real volume path name (it may be a link) */

	if (realpath(dirname, volName) == NULL) {
            sprintf(errstr, "realpath() failed on '%s'", dirname);
            perror(errstr);
	    continue;
        }

        ret = open_vol(bfp, volName, &vdi);
        if ( ret != OK ) {
	    continue;
        }

        volsOpened++;
        check_vol(bfp, vdi, &dmnId, &volCnt);
    }

    if ( volCnt == 0 ) {
        fprintf(stderr, "WARNING: Domain vdCnt not found\n");
    } else if ( volsOpened != volCnt ) {
        fprintf(stderr, "WARNING: Domain should have %d volumes. ", volCnt);
        fprintf(stderr, "%d opened. ", volsOpened);
    }

    closedir(stgDir);
    return OK;
}

/*****************************************************************************/
/*
** Read the BSR_DMN_ATTR and BSR_DMN_MATTR records to get the
** vdCnt and bfDomainId. Compare bfDomainId to previous volumes.
**
** Input:  bfp->dmn->vols[vdi] filled in
**         dmnIdp - domain version from previous call to check_vol
**         volCntp - number of volumes in domain from previous call to check_vol
** Output: dmnIdp - domain version if not previously set
**         volCntp - number of volumes in domain if not previously set
** Errors: diagnostic printf for all errors
** Return: no error returns
*/
static void
check_vol(bitFileT *bfp, vdIndexT vdi, bfDomainIdT *dmnIdp, int *volCntp)
{
    int ret;
    bitFileT *mdBfp;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    uint16_t mc;
    bsDmnAttrT *bsDmnAttrp;
    bsDmnMAttrT *bsDmnMAttrp;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[vdi]);

    mdBfp = bfp->dmn->vols[vdi]->rbmt;
    mc = BFM_RBMT_EXT;      /* mcell 6 in RBMT page 0 */

    assert(mdBfp);
    ret = read_page(mdBfp, 0);
    if ( ret != OK ) {
        fprintf(stderr,
          "check_vol: Read volume %d (%s) RBMT page 0 (lbn %ld) failed.\n",
          mdBfp->pgVol, bfp->dmn->vols[vdi]->volName, mdBfp->pgLbn);
        return;
    }

    bsMPgp = (bsMPgT*)mdBfp->pgBuf;
    bsMCp = &bsMPgp->bsMCA[mc];
    ret = find_bmtr(bsMCp, BSR_DMN_ATTR, (char**)&bsDmnAttrp);
    if ( ret != OK ) {
        /* Every volume should have a BSR_DMN_ATTR record. */
        fprintf(stderr,
    "check_vol: RBMT vol,page,mcell %d 0 %d - No BSR_DMN_ATTR record found.\n",
          mdBfp->pgVol, mc);
    }

    if ( dmnIdp->id_sec == 0 && dmnIdp->id_usec == 0 ) {
        /* Domain Id not yet set. Set it now. */
        *dmnIdp = bsDmnAttrp->bfDomainId;
    } else if ( dmnIdp->id_sec != bsDmnAttrp->bfDomainId.id_sec ||
                dmnIdp->id_usec != bsDmnAttrp->bfDomainId.id_usec )
    {
        fprintf(stderr,
          "Volume %d has a different filesystem ID than the previous volume.\n",
          vdi);
        fprintf(stderr, "Filesystem ID on volume %d is 0x%lx.%lx.\n", vdi,
          bsDmnAttrp->bfDomainId.id_sec, bsDmnAttrp->bfDomainId.id_usec);
        fprintf(stderr, "The previous filesystem ID was 0x%lx.%lx.\n",
          dmnIdp->id_sec, dmnIdp->id_usec);
    }

    ret = find_bmtr(bsMCp, BSR_DMN_MATTR, (char**)&bsDmnMAttrp);
    if ( ret == OK && bsDmnMAttrp->seqNum) {
        *volCntp = bsDmnMAttrp->vdCnt;
    }
}

/******************************************************************************/
/* Open the volume and fill in the volume specific parts of bfp.
** Malloc bfp->dmn->vols[0]. Setup bfp->dmn->vols[0] until the volume index
** can be determined then move vols[0] to vols[vdi]. 
** Setup vols[0]->volName & vols[0]->fd. Fill in the vols[0]->blocks, the
** size of the volume. Malloc and fill in vols[0]->rbmt.
** Read the page at block RBMT_BLK_LOC. Find vdi and the domain version number.
** Setup vols[vdi]->bmt.
** Test the volume AdvFS magic number but just issue a warning if not OK.
**
** This is a central routine during the setup if reading a domain or volume.
** It is called by resolve_name => open_dmn or resolve_name => resolve_vol.
** FIX. not OK => return BAD_DISK_VALUE. This is ok to read a raw block
**
** Input:  devname = name of special block (character) device to open
**         bfp->dmn is malloced
** OutPut: malloc and fill in bfp->dmn->vols[vol] (volName, index, fd)
**         malloc and fill in vols[vol]->rbmt {type, blocks, pvol, ppage, 
**                                             pcell, xtnts, dmn}
**         malloc and fill in vols[vol]->bmt
**         set pvol. NO, open_dmn doesn't want pvol set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT - no such volume
**         EBUSY - block device is mounted
**         EACCES - no permissions to access volume
**         ENOMEM - malloc failed
**         BAD_DISK_VALUE - bad magic or other unexpected disk contents
*/
static int
open_vol(bitFileT *bfp, char *devname, vdIndexT *vdip)
{
    int volFd;
    char errstr[80];
    int ret;
    bsVdAttrT *bsVdAttrp;
    bsMPgT *bsMPgp;
    bitFileT *rbmt;

    errstr[0] = '\0';       /* just in case sprintf fails */

    assert(bfp->dmn);

    volFd = open(devname, O_RDONLY);
    if ( volFd == -1 ) {
        sprintf(errstr, "open_vol: open for volume \"%s\" failed", devname);
        perror(errstr);
        if ( errno == EACCES || errno == EBUSY || errno == ENOENT ) {
            return errno;
        } else {
            return ENOENT;
        }
    }

    /* Setup vdi 0 for now. We'll change it after we find the real vdi. */
    bfp->dmn->vols[0] = malloc(sizeof(volT));
    if ( bfp->dmn->vols[0] == NULL ) {
        perror("open_vol: malloc vols[0] failed");
        return ENOMEM;
    }

    strcpy(bfp->dmn->vols[0]->volName, devname);
    bfp->dmn->vols[0]->fd = volFd;
    get_vol_blocks(bfp->dmn->vols[0], devname);

    /* TODO. use different test for OK volume based just on BMT page 0 data. */
    if ( (ret = test_advfs_magic_number(volFd, devname)) != OK ) {
        /* Return ENOMEM or BAD_DISK_VALUE */
        /* BAD_DISK_VALUE is OK if we are looking at a block on the disk */
        /* We have not set up vols[0]->rbmt */
        return ret;
    }

    bfp->dmn->vols[0]->rbmt = malloc(sizeof(bitFileT));
    if ( bfp->dmn->vols[0]->rbmt == NULL ) {
        perror("open_vol: malloc rbmt failed");
        return ENOMEM;
    }

    rbmt = bfp->dmn->vols[0]->rbmt;

    /* init rbmt (including pvol,pgVol = 0) & */
    /* set rbmt->type, rbmt->dmn, rbmt->setTag */
    newbf(bfp, rbmt);
    rbmt->setTag = staticRootTagDirTag;

    /* read RBMT at pgVol (= 0) and pgLbn */
    rbmt->pgLbn = RBMT_BLK_LOC;              /* block 16 */
    if ( read_vol_at_blk(rbmt) != OK ) {
        return BAD_DISK_VALUE;
    }

    bsMPgp = (bsMPgT*)rbmt->pgBuf;
    rbmt->pgNum = 0;
    (void)test_bmt_page(rbmt, "open_vol", TRUE);

    /* set filesystem On Disk Structure version number */ 
    if ( bfp->dmn->dmnVers.odv_major == 0 ) {
        /* This is the first volume opened. Set dmnVers now. */
        bfp->dmn->dmnVers = bsMPgp->bmtODSVersion;
    } else {
        /* Volumes already opened. dmnVers already set. */
        if ( bfp->dmn->dmnVers.odv_major != bsMPgp->bmtODSVersion.odv_major ) {
            fprintf(stderr,
   "Filesystem version (%d.%d) on volume \"%s\" does not match previously read version (%d.%d)\n",
              bsMPgp->bmtODSVersion.odv_major,
              bsMPgp->bmtODSVersion.odv_minor,
              devname, bfp->dmn->dmnVers.odv_major,
              bfp->dmn->dmnVers.odv_minor);
        }
    }

    /* Read BSR_VD_ATTR record to find the volume index of this disk. */
    ret = find_vdi(bfp, bsMPgp, devname, vdip);
    if ( ret != OK ) {
         return ret;
    }

    /* now that we know the volume index, mv vols[0] there. */
    bfp->dmn->vols[*vdip] = bfp->dmn->vols[0];
    bfp->dmn->vols[0] = NULL;

    bfp->dmn->vols[*vdip]->rbmt->pmcell.volume = *vdip;
    bfp->dmn->vols[*vdip]->rbmt->pmcell.page = 0;
    bfp->dmn->vols[*vdip]->rbmt->pmcell.cell = BFM_RBMT;
    BS_BFTAG_RSVD_INIT(bfp->dmn->vols[*vdip]->rbmt->fileTag, *vdip, BFM_RBMT);

    bfp->dmn->vols[*vdip]->rbmt->dmn = bfp->dmn;

    ret = load_rbmt_xtnts(bfp->dmn->vols[*vdip]->rbmt);
    if ( ret != OK ) {
        return ret;
    }

    bfp->dmn->vols[*vdip]->bmt = malloc(sizeof(bitFileT));

    if ( bfp->dmn->vols[*vdip]->bmt == NULL ) {
        sprintf(errstr, "open_vol: malloc failed");
	perror(errstr);
	return ENOMEM;
    }

    newbf(bfp->dmn->vols[*vdip]->rbmt, bfp->dmn->vols[*vdip]->bmt);
    bfp->dmn->vols[*vdip]->bmt->pmcell.volume = *vdip;
    bfp->dmn->vols[*vdip]->bmt->pmcell.page = 0;
    bfp->dmn->vols[*vdip]->bmt->pmcell.cell = BFM_BMT;
    bfp->dmn->vols[*vdip]->bmt->fd = volFd;
    BS_BFTAG_RSVD_INIT(bfp->dmn->vols[*vdip]->bmt->fileTag, *vdip, BFM_BMT);
    ret = load_xtnts(bfp->dmn->vols[*vdip]->bmt);
    if ( ret != OK ) {
        return ret;
    }

    bfp->dmn->vols[*vdip]->bmt->dmn = bfp->dmn;

    return OK;
}

/*****************************************************************************/
/* Read the super block and check the AdvFS magic number. This routine does
** not need or use extent maps. 
**
** Input: fd - lseek and read this file
** Input: devname used only to print warning
** Output: none
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM - if malloc of buffer to read page failed
**         BAD_DISK_VALUE - bad magic number
*/
static int
test_advfs_magic_number(int fd, char *devname)
{
    ssize_t read_ret;
    advfs_fake_superblock_t *superBlk;

    superBlk = (advfs_fake_superblock_t *)malloc(ADVFS_SUPER_BLOCK_SZ);
    if (superBlk == NULL) {
        return ENOMEM;
    }

    if ( lseek(fd, (off_t)ADVFS_FAKE_SB_BLK * DEV_BSIZE, SEEK_SET) == -1 ) {
        perror("test_advfs_magic_number: lseek failed");
        free(superBlk);
        return BAD_DISK_VALUE;
    }

    read_ret = read(fd, superBlk, (size_t)ADVFS_SUPER_BLOCK_SZ);
    if ( read_ret != (ssize_t)ADVFS_SUPER_BLOCK_SZ ) {
        free(superBlk);
        if ( read_ret == (ssize_t)-1 ) {
            perror("test_advfs_magic_number: read superblock failed");
        } else {
            fprintf(stderr,
                "read superblock failed: read %ld bytes. wanted %d\n",
                read_ret, ADVFS_SUPER_BLOCK_SZ);
        }
        return BAD_DISK_VALUE;
    }

    if ( !ADVFS_VALID_FS_MAGIC(superBlk) ) { 
        fprintf(stderr, "Bad AdvFS magic number on %s.\n", devname);
        free(superBlk);
        return BAD_DISK_VALUE;
    }

    free(superBlk);
    return OK;
}

/*****************************************************************************/
/*
** fill in the volume size value (in blocks), and verify that the
** volume out there really is that size.
**
** Input: vol->volName (path of vol in the domain's .stg directory)
**        vol->volFd is open
** Outout: vol->blocks - set to partition size or 0
** Errors: diagnostic output to stderr for all errors
** Return: none
*/
static void
get_vol_blocks(volT *volp, char *volName)
{
    off_t seek_off;
    void *buf;
    char errstr[MAXPATHLEN + 80];
    char rawName[MAXPATHLEN];
    int fd, ret;
    capacity_type size;

    /* convert the block device name to the character device name for
       use with the below ioctl() */

    if (__blocktochar(volName, rawName) == NULL) {
	printf("get_vol_blocks: can't convert %s to character device name\n",
	       volName);
	return;
    }

    /* open the volume char device and size the volume */

    fd = open(rawName, O_RDONLY);
    if ( fd == -1 ) {
        sprintf(errstr, "get_vol_blocks: open for volume \"%s\" failed", 
		rawName);
        perror(errstr);
	return;
    }

    size.lba = 0;

    if (ioctl(fd, DIOC_CAPACITY, &size) < 0) {
        perror("get_vol_blocks: Can't get volume size");
	return;
    }

    close(fd);

    if (!size.lba) {
        /* If we failed to get the volume size, c'est la vie. */
        return;
    }

    volp->blocks = size.lba;

    /* test that the last block is really there */
    buf = (void *)malloc(DEV_BSIZE);
    if ( buf == NULL ) {
        perror("get_vol_blocks: malloc fail");
        return;
    }

    seek_off = (off_t)(volp->blocks - 1) * DEV_BSIZE;
    if ( pread(volp->fd, buf, (size_t)DEV_BSIZE, seek_off) != DEV_BSIZE ) {
        perror("get_vol_blocks: pread fail");
    }

    free(buf);

    return;
}

/*****************************************************************************/
/* Given page 0 of the RBMT, look at the mcell with the vdIndex.
** Check the tag and set tag of the mcell. Look for BSR_VD_ATTR record.
** Check the vdIndex.
**
** Input: bmtpgp - pointer to (R)BMT page 0
**        bfp->dmn->vols[] - filled in. used to check vol previously found
** Output: vdip - returns volume index
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE for bad disk contents or BSR_VD_ATTR not found
*/
static int
find_vdi(bitFileT *bfp, bsMPgT *bmtpgp, char *devname, vdIndexT *vdip)
{
    bsVdAttrT *bsVdAttrp;
    bsMCT *mcellp;
    uint16_t mc;
    vdIndexT vdi;
    int ret;

    mc = BFM_RBMT;      /* mcell 0 in RBMT page 0 */
    mcellp = &bmtpgp->bsMCA[mc];

    /* Check for correct set tag */
    if ( !BS_BFTAG_EQL(mcellp->mcBfSetTag, staticRootTagDirTag) )
    {
        fprintf(stderr, "find_vdi: Bad fileset tag (%ld,%d) in mcell %d. ",
          mcellp->mcBfSetTag.tag_num, mcellp->mcBfSetTag.tag_seq, mc);
        fprintf(stderr, "Should be %ld,%d\n", staticRootTagDirTag.tag_num,
                staticRootTagDirTag.tag_seq);
    }

    /* Check for correct file tag */
    if ( !BS_IS_TAG_OF_TYPE(mcellp->mcTag, BFM_RBMT) ) 
    {
        fprintf(stderr, "find_vdi: Bad file tag (%ld,%d) in mcell %d.\n",
          mcellp->mcTag.tag_num, mcellp->mcTag.tag_seq, mc);
        fprintf(stderr, "File tag should be negative, and divisible by %d. ",
                BFM_RSVD_TAGS);
        fprintf(stderr, " Sequence number should be 0\n");
    }

    ret = find_bmtr(mcellp, BSR_VD_ATTR, (char**)&bsVdAttrp);
    if ( ret != OK ) {    /* can fail with BAD_DISK_VALUE or ENOENT */
        fprintf(stderr,
          "find_vdi: Could not find BSR_VD_ATTR record in cell %d.\n", mc);
        return BAD_DISK_VALUE;
    }

    vdi = bsVdAttrp->vdIndex;

    if ( vdi >= BS_MAX_VDI ) {
        fprintf(stderr,
          "find_vdi: Bad vdi (%d) read from BSR_VD_ATTR record. ", vdi);
        fprintf(stderr, "Must be <= %d.\n", BS_MAX_VDI);
        return BAD_DISK_VALUE;
    }

    if ( bfp->dmn->vols[vdi] ) {
        fprintf(stderr, "find_vdi: vdi %d already found. ", vdi);
        fprintf(stderr, "Skipping %s\n", devname);
        return OK;
    }

    *vdip = vdi;

    return OK;
}

/*****************************************************************************/
/* Load the RBMT extent map, following the chain of RBMT pages.
** Each page of the RBMT file contains, in last mcell, a BSR_XTNT
** record whose extent describes the next page of the RBMT. The
** chain and next also point to the next RBMT page.
** Called from open_vol.
**
** Input: bfp->dmn->vols[]
**        bfp->pmcell.cell set to 0
**        bfp->pgBuf already read, page 0 of RBMT
** Output: bfp->xmap - filled in
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM - malloc fails
**         BAD_DISK_VALUE - lseek or read fails or unexpected disk values
*/

static int
load_rbmt_xtnts(bitFileT *bfp)
{
    int ret;
    bsXtntRT *rbmtXtntRp;
    int  idx;                     /* index of next xmap.xtnt[] to be filled */
    bsXtntRT  *bsXtntRp;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    bfMCIdT bfMCId;
    int mcCnt;
    int mcellCnt;
    int savCnt;

    assert(bfp->pmcell.cell == BFM_RBMT);
    assert(bfp->fileTag.tag_num == RSVD_TAGNUM_GEN(bfp->pmcell.volume, BFM_RBMT));
    assert(bfp->pgBuf);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->pmcell.page == 0);

    bsMPgp = (bsMPgT*)bfp->pgBuf;
    bsMCp = &bsMPgp->bsMCA[bfp->pmcell.cell];
    ret = load_bsr_xtnts(bfp, &idx, bsMCp, &bfMCId, &mcCnt);
    if ( ret != OK ) {
       return ret;
    }

    if ( bfMCId.page != 0 ) {
        fprintf(stderr,"RBMT first BSR_XTRA_XTNTS cell is not on page 0 (%ld)\n",
          bfMCId.page);
        return BAD_DISK_VALUE;
    }

    mcellCnt = 1;   /* count the primary mcell */

    /* vdi from the chain mcell will be zero if there are no BSR_XTRA_XTNTS */
    /* extents. But the extent chain ends with a cell with no record. */
    /* The last BSR_XTRA_XTNTS cell's next points to an empty cell. */
    while ( bfMCId.volume > 0 ) {

        /* test additional restriction on vdi */
        if ( bfMCId.volume != bfp->pmcell.volume ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr, "RBMT file extents change volume.\n");
            return BAD_DISK_VALUE;
        }

        bsMCp = &bsMPgp->bsMCA[bfMCId.cell];

        ret = load_xtra_xtnts(bfp, &idx, bsMCp, &bfMCId, TRUE);
        if ( ret == ENOENT ) {
            /* No BSR_XTRA_XTNTS record in this cell. End of the chain. */
            break;

        }
        if ( ret != OK ) {
            return ret;
        }

        /* Test additional restriction on page. On addtional mcell per page. */
        if ( bfMCId.page != bfp->pmcell.page ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr, "RBMT BSR_XTRA_XTNTS cell is not on page %ld (%ld)\n",
              bfp->pmcell.page, bfMCId.page);
            return BAD_DISK_VALUE;
        }

        mcellCnt++;

        if ( mcellCnt % 100 == 0 ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              " reading %d mcells, %d mcells read\r", mcCnt, mcellCnt);
        }

        /* Only allow read_page to look at mcellCnt extents. */
        savCnt = bfp->xmap.cnt;
        bfp->xmap.cnt = mcellCnt;

        if ( ret = read_page(bfp, (bs_meta_page_t)FETCH_MC_PAGE(bfMCId)) ) {
            bfp->xmap.cnt = savCnt;
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              "load_rbmt_xtnts: Read RBMT page %ld on volume %d failed.\n",
              bfMCId.page, bfMCId.volume);
            return ret;
        }

        /* Restore actual number of extents allocated. */
        bfp->xmap.cnt = savCnt;

        bsMPgp = (bsMPgT*)bfp->pgBuf;
        (void)test_bmt_page(bfp, "load_rbmt_xtnts", TRUE);

        bsMCp = &bsMPgp->bsMCA[bfMCId.cell];
        (void)test_mcell(bfp, "load_rbmt_xtnts", bfMCId, bsMCp);
    }

    fprintf(stderr, CLEAR_LINE);

    if ( mcellCnt != mcCnt ) {
        fprintf(stderr,
          "load_rbmt_xtnts: Bad mcellCnt. Expected %d, actual %d\n",
          mcCnt, mcellCnt);
    }

    /* needed for page_mapped and read_page */
    bfp->xmap.xtnt[idx].fob = 0;
    bfp->pages = FOB2PAGE(bfp->xmap.xtnt[idx - 1].fob); /* last FOB # */
    bfp->xmap.cnt = idx;

    return OK;
}

/*****************************************************************************/
/* Read a page into pgBuf at the specified block from the specified volume.
**
** Input: bfp->pgVol, bfp->pgLbn - volume and block to read
**        dmn->vols[pgVol]->fd - file descriptor of opened volume
** Output: bfp->pgBuf may be malloc'd
**         bfp->pgBuf filled
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM if malloc failed
**         BAD_DISK_VALUE - lseek or read fails
*/
static int
read_vol_at_blk(bitFileT *bfp)
{
    int fd, ret;
    ssize_t read_ret;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->pgLbn % (ADVFS_METADATA_PGSZ / DEV_BSIZE) == 0);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->dmn->vols[bfp->pgVol]);
    assert(bfp->dmn->vols[bfp->pgVol]->fd > 2);

    fd = bfp->dmn->vols[bfp->pgVol]->fd;

    ret = test_blk(bfp, bfp->pgVol, bfp->pgLbn, bfp->bfPgSz);
    if (ret == OUT_OF_RANGE) {
        fprintf(stderr, "WARNING: Block %d is too large for volume %d\n",
                bfp->pgLbn, bfp->pgVol);
    } else {
        if (ret == BAD_DISK_VALUE) {
            fprintf(stderr, "WARNING: Invalid alignment for Block %d on volume %d\n",
                    bfp->pgLbn, bfp->pgVol);
        }
    }
        
    /* RBMT start at lbn 16 (RBMT_BLK_LOC) on each volume */
    if ( lseek(fd, (off_t)bfp->pgLbn * DEV_BSIZE, SEEK_SET) == (off_t)-1 ) {
        perror("read_vol_at_blk: lseek failed");
        return BAD_DISK_VALUE;
    }

    if ( bfp->pgBuf == NULL ) {
        bfp->pgBuf = malloc(ADVFS_METADATA_PGSZ);
        if ( bfp->pgBuf == NULL ) {
            perror("read_vol_at_blk: malloc failed");
            return ENOMEM;
        }
    }

    read_ret = read(fd, bfp->pgBuf, (size_t)ADVFS_METADATA_PGSZ);
    if ( read_ret != (ssize_t)ADVFS_METADATA_PGSZ ) {
        if ( read_ret == (ssize_t)-1 ) {
            perror("read_vol_at_blk: read failed");
        } else {
            fprintf(stderr,
                "read_vol_at_blk: read failed: read %ld bytes. wanted %d\n",
                read_ret, ADVFS_METADATA_PGSZ);
        }
        return BAD_DISK_VALUE;
    }

    return OK;
}
/*****************************************************************************/
/* Follow the chain of mcells and load all the extents in bfp.
**
** Reserved files and V4 non reserved, non striped files have one extent in
** BSR_XTNTS. The next extents are in BSR_XTRA_XTNTS.
**
** For RBMT and BMT just copy already existing extents in
** bfp->dmn->vols[]->(r)bmt.
**
** Input: bfp->fileTag - use RBMT vs BMT for mcells
**        bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - primary mcell in (R)BMT
** Output: bfp->xmap.xtnts loaded
**         bfp->pages set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOMEM if malloc of xtnts fails
**         BAD_DISK_VALUE - extents or mcells not as expected
** TODO: Rename to load_bf_xtnts()? */
int
load_xtnts(bitFileT *bfp)
{
    int ret;
    int idx;
    bfMCIdT bfMCId;
    int segSz;
    int mcCnt;

    assert(bfp);
    assert(bfp->fileTag.tag_num);
    assert(bfp->setTag.tag_num);
    assert(bfp->pmcell.volume != 0);
    assert(bfp->pmcell.page != PAGE_TERM);
    assert(bfp->pmcell.cell != CELL_TERM);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pmcell.volume]);

    /* special case for BMT & RBMT. use preloaded extents */
    if ( bfp->fileTag.tag_num == RSVD_TAGNUM_GEN(bfp->pmcell.volume, BFM_RBMT) )
    {
        /* XXX - do we need to service RBMT here, or is that now done
           in load_rbmt_xtnts() ? */
        /* special case for RBMT. Just "link" to RBMT in dmn. */
        assert(bfp->dmn->vols[bfp->pmcell.volume]->rbmt->xmap.xtnt);
        bfp->xmap.xtnt = bfp->dmn->vols[bfp->pmcell.volume]->rbmt->xmap.xtnt;
        bfp->xmap.cnt = bfp->dmn->vols[bfp->pmcell.volume]->rbmt->xmap.cnt;
        bfp->pages = bfp->dmn->vols[bfp->pmcell.volume]->rbmt->pages;
        return OK;
    }

    if ( bfp->fileTag.tag_num == RSVD_TAGNUM_GEN(bfp->pmcell.volume, BFM_BMT) &&
         bfp->dmn->vols[bfp->pmcell.volume]->bmt->xmap.xtnt )
    {
        /* special case for BMT. Just "link" to BMT in dmn. */
        bfp->xmap.xtnt = bfp->dmn->vols[bfp->pmcell.volume]->bmt->xmap.xtnt;
        bfp->xmap.cnt = bfp->dmn->vols[bfp->pmcell.volume]->bmt->xmap.cnt;
        bfp->pages = bfp->dmn->vols[bfp->pmcell.volume]->bmt->pages;
        return OK;
    }

    ret = get_bsr_xtnts(bfp, &idx, &bfMCId, &mcCnt);
    if ( ret != OK ) {
        return ret;
    }

    if ( bfMCId.volume == 0 ) {
        bfp->xmap.cnt = idx;
        return OK;
    }

    ret = get_xtra_xtnts(bfp, idx, &bfMCId, mcCnt, FALSE);
    if ( ret != OK ) {
        return ret;
    }

    return OK;
}

/*****************************************************************************/
/*
** If this file is a snapShot, find the original file and merge the
** snapShot extents with the original extents.
**
** Input:  bfp->setp - zero for reserved fliles, otherwise setup
**         bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell
**         bfp->dmn->vols[bfp->pmcell.volume] set up
** Output: bfp->pages, bfp->xmap.xtnt
** Errors: diagnostic printf for errors
** Return: OK if all went well
**         ENOMEM if malloc fails
*/
static int
load_clone_xtnts(bitFileT *bfp)
{
    int ret;
    bitFileT origBfp;
    bsBfAttrT *bsBfAttrp;
    bitFileT *mdBfp;
    bsMPgT *bmtpgp;
    bsMCT *mcellp;

    ret = load_xtnts(bfp);
    if ( ret != OK ) {
        return ret;
    }

    if ( bfp->setp == NULL ) {
        /* This is a reserved file. It is not a clone. */
        assert(BS_BFTAG_EQL(bfp->setTag, staticRootTagDirTag));
        return OK;
    }

    if ( bfp->setp->origSetp == NULL ) {
        /* This file is not in a clone fileset. Return w/ orig extents */
        return OK;
    }

    ret = find_rec(bfp, BSR_ATTR, (char**)&bsBfAttrp);
    if ( ret != OK ) {
        if ( ret != BAD_DISK_VALUE ) {
            fprintf(stderr,
         "Primary mcell at vol %d, page %ld, cell %d: can't find BSR_ATTR rec\n",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
        }
        return OK;
    }

    if ( bfp->pages == 0 ) {
        return OK;
    }

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pmcell.volume]);
    assert (BS_BFTAG_REG(bfp->fileTag));  /* no clone reserved files */

    if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
        /* reserved files use the rbmt for their extents */
        mdBfp = bfp->dmn->vols[bfp->pmcell.volume]->rbmt;
    } else {
        mdBfp = bfp->dmn->vols[bfp->pmcell.volume]->bmt;
    }
    assert(mdBfp);

    ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(bfp->pmcell));
    if ( ret != OK ) {
        fprintf(stderr, "load_clone_xtnts: Read volume %d %s page %ld failed.\n",
          bfp->pmcell.volume, BS_BFTAG_RSVD(bfp->fileTag)  ? "RBMT" : "BMT",
          bfp->pmcell.page);
        return ret;
    }

    bmtpgp = (bsMPgT*)mdBfp->pgBuf;
    mcellp = &bmtpgp->bsMCA[bfp->pmcell.cell];
    if ( mcellp->mcBfSetTag.tag_num != bfp->setTag.tag_num ) {
        /* This primary mcell belongs to the original file. */
        /* The clone file has no metadata of its own. */
        bfp->pmcell.volume = 0;
        bfp->pmcell.page = PAGE_TERM;
        bfp->pmcell.cell = CELL_TERM;
        if ( mcellp->mcBfSetTag.tag_num != bfp->setp->origSetp->fileTag.tag_num ) {
            fprintf(stderr,
              "load_clone_xtnts: Bad %s mcell (vol,page,cell) %d %ld %d.\n",
              BS_BFTAG_RSVD(bfp->fileTag) ? "RBMT" : "BMT",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
            fprintf(stderr,
             "Bad mcell set tag (%ld). Clone fileset %ld. Original fileset %ld.\n",
              mcellp->mcBfSetTag.tag_num, bfp->setTag.tag_num, bfp->setp->origSetp->fileTag.tag_num);
        }
    }

    newbf(bfp, &origBfp);

    ret = get_file_by_tag(bfp->setp->origSetp, bfp->fileTag.tag_num, &origBfp);
    if ( ret == ENOENT ) {
        goto done;
    }
    if ( ret != OK ) {
        fprintf(stderr,
         "load_clone_xtnts: Error in retrieving orig extents for file tag %ld\n",
          bfp->fileTag.tag_num);
        goto done;
    }

    merge_clone_xtnts(bfp, &origBfp);

done:
    free(origBfp.xmap.xtnt);

    return OK;
}

/*****************************************************************************/
/*
** Merge the original file's extents into the clone extents.
** If the clone maps a page (including permanent holes) use the clone's extent.
** Else if the original maps the page, use the original's extent.
** Else leave a hole in the extent map.
**
** Input:  cloneBfp->xmap.xtnt, origBfp->xmap.xtnt, cloneBfp->pages
** Output: cloneBfp->xmap.xtnt modified
** Errors: none
** Return: OK if all went well
**         ENOMEM - if no mem while extending the clone's xmap.xtnt
*/
static void
merge_clone_xtnts(bitFileT *cloneBfp, bitFileT *origBfp)
{
    bf_fob_t fob = 0;
    int cloneXi = 0;
    int origXi = 0;
    int mergeXi = 0;
    extentT *xtnts;
    bf_fob_t nextCloneFob;
    bf_fob_t nextOrigFob;
    int xtntCnt = cloneBfp->xmap.cnt + origBfp->xmap.cnt;

    xtnts = malloc(xtntCnt * sizeof(extentT));
    if ( xtnts == NULL ) {
        perror("merge_clone_xtnts: malloc failed");
        exit(1);
    }

    while ( fob < (bf_fob_t) PAGE2FOB(cloneBfp->pages) ) {
        if ( cloneXi < cloneBfp->xmap.cnt &&
             cloneBfp->xmap.xtnt[cloneXi].blk != XTNT_TERM )
        {
            /* We have a clone extent (maybe a perm hole). */
            /* Use the clone extent. We use the orig extent only where the */
            /* clone has a hole. */
            xtnts[mergeXi] = cloneBfp->xmap.xtnt[cloneXi];
            cloneXi++;
            assert(cloneXi < cloneBfp->xmap.cnt);
            mergeXi++;
            assert(mergeXi < xtntCnt);
            fob = cloneBfp->xmap.xtnt[cloneXi].fob;
            continue;
        }

        /* The clone extent at cloneXi is a hole or the end of clone extents. */
        assert(cloneBfp->xmap.xtnt[cloneXi].blk == XTNT_TERM);
        assert(cloneBfp->xmap.xtnt[cloneXi].fob <= fob);

        if ( origXi >= origBfp->xmap.cnt - 1 ) {
            /* There are no more extents in the original file. */
            /* Copy the rest of the clone's extents and we're done. */
            while ( cloneXi < cloneBfp->xmap.cnt - 1 ) {
                xtnts[mergeXi] = cloneBfp->xmap.xtnt[cloneXi];
                if ( cloneBfp->xmap.xtnt[cloneXi].fob < fob ) {
                    xtnts[mergeXi].fob = fob;
                }
                cloneXi++;
                mergeXi++;
                assert(mergeXi < xtntCnt);
            }

            fob = cloneBfp->xmap.xtnt[cloneXi].fob;
            break;
        }

        /* The original file still has more extents to look at. */
        if ( origBfp->xmap.xtnt[origXi + 1].fob <= fob ) {
            /* The orig does not overlap the clone hole. It is completely */
            /* before the hole. Move the orig extent pointer forward. */
            /* This orig extent maps pages already merged into clone. */
            origXi++;
            assert(origXi < origBfp->xmap.cnt);
            continue;
        }

        /* The original extent at origXi covers "fob" */
        assert(origBfp->xmap.xtnt[origXi].fob <= fob);
        assert(origBfp->xmap.xtnt[origXi + 1].fob > fob);

        xtnts[mergeXi].vol = origBfp->xmap.xtnt[origXi].vol;
        xtnts[mergeXi].fob = fob;
        if ( origBfp->xmap.xtnt[origXi].blk == XTNT_TERM ) {
            xtnts[mergeXi].blk = XTNT_TERM;
        } else {
            xtnts[mergeXi].blk = origBfp->xmap.xtnt[origXi].blk +
              ((fob - origBfp->xmap.xtnt[origXi].fob) * ADVFS_FOBS_PER_DEV_BSIZE);
        }
        mergeXi++;
        assert(mergeXi < xtntCnt);

        if ( cloneXi < cloneBfp->xmap.cnt - 1 ) {
            nextCloneFob = cloneBfp->xmap.xtnt[cloneXi + 1].fob;
        } else {
            nextCloneFob = PAGE2FOB(cloneBfp->pages);
        }
        assert(origXi + 1 < origBfp->xmap.cnt);
        nextOrigFob = origBfp->xmap.xtnt[origXi + 1].fob;

        if ( nextOrigFob < nextCloneFob ) {
            /* We used the entire orig extent. Move on to the next. */
            origXi++;
            assert(origXi < origBfp->xmap.cnt);
            fob = nextOrigFob;
        } else if ( nextOrigFob > nextCloneFob ) {
            /* We passed the entire clone hole. Move on to the next extent. */
            cloneXi++;
            fob = nextCloneFob;
        } else {
            /* The orig extent is used up, the clone hole is passed. Move on. */
            cloneXi++;
            origXi++;
            assert(origXi < origBfp->xmap.cnt);
            fob = nextOrigFob;
        }
    }

    /* Write a terminator in the merged extents. */
    assert(mergeXi > 0);
    xtnts[mergeXi].vol = xtnts[mergeXi - 1].vol;
    xtnts[mergeXi].fob = fob;
    xtnts[mergeXi].blk = XTNT_TERM;
    mergeXi++;
    assert(mergeXi < xtntCnt);

    xtnts = realloc( xtnts, mergeXi * sizeof(extentT) );
    free( cloneBfp->xmap.xtnt );
    cloneBfp->xmap.xtnt = xtnts;
    cloneBfp->xmap.cnt = mergeXi;
}

/*****************************************************************************/
/* Read the (R)BMT page with the prim mcell. Find the prim mcell and
** the BSR_XTNTS record.  In files that have extents in the primary
** mcell the BSR_XTNTS record tells how many mcells have extents for
** that file.  This is used to malloc the extent array in bfp.
**
** must have tag (file or set) since ppage==0 could be reg file in V4
** Input: bfp->fileTag - use RBMT for negative fileTag, else use BMT
**        bfp->dmn, bfp->dmn->vols[pvol]->(r)bmt - extents of (R)BMT
**        bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - location of prim mcell
** Output: bfp->xmap.xtnts[0] filled in
**         bfp->pages set.
**         cvdi, cmcid - location of chain mcell
**         idxp - next location in bfp->xmap.xtnts[] to fill
**         mcCntp - number of extent mcells in the chain
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE - extents or mcells not as expected
**         ENOMEM - malloc failed
*/
static int
get_bsr_xtnts(bitFileT *bfp, int *idxp, bfMCIdT *cmcid, int *mcCntp)
{
    int ret;
    bitFileT *mdBfp;
    bsMPgT *bmtpgp;
    bsMCT *mcellp;
    bfMCIdT bfMCId;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfp->pmcell.volume]);

    if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
        /* reserved files use the rbmt for their extents */
        mdBfp = bfp->dmn->vols[bfp->pmcell.volume]->rbmt;
    } else {
        mdBfp = bfp->dmn->vols[bfp->pmcell.volume]->bmt;
    }
    assert(mdBfp);

    ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(bfp->pmcell));
    if ( ret != OK ) {
        fprintf(stderr, "get_bsr_xtnts: read_page (tag %ld, page %ld) failed.\n",
          mdBfp->fileTag.tag_num, bfp->pmcell.page);
        return ret;
    }

    bmtpgp = (bsMPgT*)mdBfp->pgBuf;
    (void)test_bmt_page(mdBfp, "get_bsr_xtnts", BS_BFTAG_RSVD(bfp->fileTag));

    mcellp = &bmtpgp->bsMCA[bfp->pmcell.cell];
    bfMCId = bfp->pmcell;
    (void)test_mcell(bfp, "get_bsr_xtnts", bfMCId, mcellp);

    ret = load_bsr_xtnts(bfp, idxp, mcellp, cmcid, mcCntp);
    return ret;
}

/*****************************************************************************/
/* Given an mcell, find the BSR_XTNTS record and load the extent into
** bfp->xmap.xtnts[0]. Set idxp to point to next (1) array entry.
**
** Input: mcellp - mcell contents with BSR_XTNTS record
** Output: bfp->xmap.xtnts[0] filled in
**         bfp->pages set.
**         idxp - set to 1 if BSR_XTNTS had one extent
**         cvdi, cmcid - pointer to chain mcell on disk
**         mcCntp - number of extent mcells in the chain
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if bad mcell contents.
**         ENOMEM - malloc failed
*/
static int
load_bsr_xtnts(bitFileT *bfp, int *idxp, bsMCT *mcellp, bfMCIdT *cmcid,
               int *mcCntp)
{
    bsXtntRT *bsXtntRp;
    int ret;
    int idx;

    ret = find_bmtr(mcellp, BSR_XTNTS, (char**)&bsXtntRp);
    if ( ret != OK ) {
        fprintf(stderr, "load_bsr_xtnts: Bad mcell RBMT vol %d page %ld cell %d: ",
          bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
        fprintf(stderr, "No BSR_XTNTS record in primary mcell.\n");
        return BAD_DISK_VALUE;
    }

    /* xCnt can be 1 or 2 */
    if ( bsXtntRp->xCnt > BMT_XTNTS ||
         bsXtntRp->xCnt == 0) {
        fprintf(stderr, 
		"load_bsr_xtnts: Bad mcell RBMT vol %d page %ld cell %d: ",
		bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
        fprintf(stderr, "%d extents in primary mcell.\n",
          bsXtntRp->xCnt);
        return BAD_DISK_VALUE;
    }

    *mcCntp = bsXtntRp->mcellCnt;

    bfp->xmap.cnt = BMT_XTNTS;
    if ( bsXtntRp->mcellCnt == 0 ) {
        bfp->xmap.cnt += BMT_XTRA_XTNTS;
    } else {
        bfp->xmap.cnt += (bsXtntRp->mcellCnt - 1) * BMT_XTRA_XTNTS;
    }

    /* get one more than we need. see read_page */
    bfp->xmap.xtnt = malloc((bfp->xmap.cnt + 1) * sizeof(extentT));
    if ( bfp->xmap.xtnt == NULL ) {
        perror("load_bsr_xtnts: malloc failed");
        return ENOMEM;
    }

    /* load extents from primary mcell */
    idx = 0;
    get_xtnts(bfp, &idx, bfp->pmcell.volume,
              bsXtntRp->bsXA, bsXtntRp->xCnt);

    *idxp = idx;
    *cmcid = bsXtntRp->chainMCId;
    if ( cmcid->volume != 0 ) {
        if ( test_vdi(bfp, "load_bsr_xtnts", cmcid->volume) != OK )
            return BAD_DISK_VALUE;
        if ( test_mcid(bfp, "load_bsr_xtnts", *cmcid) != OK ) {
            fprintf(stderr,
              "load_bsr_xtnts: Bad mcell RBMT vol %d page %ld cell %d: ",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
            return BAD_DISK_VALUE;
        }
    }

    return OK;
}

/*****************************************************************************/
/* Read each (R)BMT page that contains an mcell in the list of xtra mcell.
** Find the xtra mcell and the BSR_XTRA_XTNTS record. Load the extents
** into bfp->xmap.xtnts.
** Since some files can have many extra mcells, a progress report is
** printed every 100 mcells read.
**
** Input: idx = index in bfp->xmap to start filling at
**        vol, pn, cn = first xtra mcell
**        mcCnt - number of extent mcells in the chain
**        stflg - TRUE = expect non-XTRA cell after chain of XTRA cells
** Output: bfp->xmap.xtnts filled in
**         bfp->pages set.
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if mcell contents are bad
**         ENOMEM - malloc failed
*/
static int
get_xtra_xtnts(bitFileT *bfp, int idx, bfMCIdT *bfMCIdp, int mcCnt, int stflg)
{
    int ret = OK;
    bitFileT *mdBfp;
    uint32_t mcellCnt;
    bsMPgT *bsMPgp;
    bsMCT *bsMCp;
    uint32_t bsXACnt;
    bsXtntT *bsXA;
    uint32_t origMcCnt = mcCnt;
    bsXtraXtntRT  *xtraXtntRecp;

    assert(bfp);
    assert(bfp->fileTag.tag_num);
    assert(bfp->type & DMN || bfp->type & VOL);
    assert(bfp->dmn);

    /* TODO. assert idx OK */
    assert(test_vdi(bfp, NULL, bfMCIdp->volume) == OK);
    assert(test_mcid(bfp, NULL, *bfMCIdp) == OK);

    mcellCnt = 1;            /* count the BSR_XTNTS mcell */

    while (bfMCIdp->volume > 0) {

        if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
            /* reserved files use the rbmt for their extents */
            mdBfp = bfp->dmn->vols[bfMCIdp->volume]->rbmt;
        } else {
            /* normal files use the bmt for their extents */
            mdBfp = bfp->dmn->vols[bfMCIdp->volume]->bmt;
        }
        assert(mdBfp);

        if ( mcellCnt >= mcCnt ) {
            /*
            ** The chain of mcells is longer than expected from the
            ** mcellCnt in the BSR_XTNTS record.  For a BSXMT_APPEND
            ** file, this means the short mcellCnt wrapped and we need
            ** to get more extent entrys in our array.  
	    */
            if ( ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(*bfMCIdp)) ) {
                fprintf(stderr, CLEAR_LINE);
                fprintf(stderr,
                  "get_xtra_xtnts: Read BMT page %ld on volume %d failed.\n",
                  bfMCIdp->page, bfMCIdp->volume);
                return ret;
            }

            bsMPgp = (bsMPgT*)mdBfp->pgBuf;

            bsMCp = &bsMPgp->bsMCA[bfMCIdp->cell];

            ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)&xtraXtntRecp);
            if ( ret != OK ) {
                return (ENOENT);
            }

            /* This means the on disk ushort mcellCnt wrapped. */
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              "get_xtra_xtnts: mcellCnt wrapped. Adding room for 64K more.\n");
            bfp->xmap.cnt += 0x10000 * BMT_XTRA_XTNTS;
            bfp->xmap.xtnt = realloc(bfp->xmap.xtnt,
                bfp->xmap.cnt * sizeof(extentT));
            mcCnt += 0x10000;
        }

        if ( mcellCnt % 100 == 0 ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              " reading %d mcells, %d mcells read\r", mcCnt, mcellCnt);
        }

        if ( ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(*bfMCIdp)) ) {
            fprintf(stderr, CLEAR_LINE);
            fprintf(stderr,
              "get_xtra_xtnts: Read BMT page %ld on volume %d failed.\n",
              bfMCIdp->page, bfMCIdp->volume);
            return ret;
        }

        bsMPgp = (bsMPgT*)mdBfp->pgBuf;
        (void)test_bmt_page(mdBfp, "get_xtra_xtnts", BS_BFTAG_RSVD(bfp->fileTag)); 
        bsMCp = &bsMPgp->bsMCA[bfMCIdp->cell];
        (void)test_mcell(bfp, "get_xtra_xtnts", *bfMCIdp, bsMCp);

        ret = load_xtra_xtnts(bfp, &idx, bsMCp, bfMCIdp, stflg);
        if ( ret != OK ) {
            break;
        }

        mcellCnt++;
    }

    fprintf(stderr, CLEAR_LINE);
    bfp->xmap.cnt = idx;

    if ( mcellCnt != origMcCnt ) {
        fprintf(stderr,
          "get_xtra_xtnts: Bad mcellCnt. Expected %d, actual %d\n",
          origMcCnt, mcellCnt);
    }

    return ret;
}

/*****************************************************************************/
/* Given a mcell, find the BSR_XTRA_XTNTS record and load the extents
** into bfp->xmap.xtnts. Test the mcell contents. Set bfp->pages.  If
** we didn't malloc enough when the BSR_XTNTS record was read, get
** some more now. This only happens if the mcell count in the
** BSR_XTNTS record is bad.
**
** Input: idxp - index in extent array to start loading
**        bsMCp - mcell contents
**        stflg - TRUE = expect non-XTRA cell after chain of XTRA cells
** Output: bfp->xmap.xtnts filled in
**         bfp->pages set
**         idxp - incremented
**         vnp, bfMCIdp - next xtra mcell pointer
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT if no BSR_XTRA_XTNTS record found. Used for stripes
**         BAD_DISK_VALUE if mcell contents bad
**         ENOMEM if malloc fails
*/
static int
load_xtra_xtnts(bitFileT *bfp, int *idxp, bsMCT *bsMCp,
                bfMCIdT *bfMCIdp, int stflg)
{
    bsXtraXtntRT  *xtraXtntRecp;
    uint32_t bsXACnt;
    bsXtntT *bsXA;
    
    int ret;

    ret = find_bmtr(bsMCp, BSR_XTRA_XTNTS, (char**)&xtraXtntRecp);
    if ( ret != OK ) {
        if ( stflg ) {
            return ENOENT;
        }

        fprintf(stderr, "Bad %s mcell at volume, page, cell: %d %ld %d\n",
          PRINT_BMT_OR_RBMT, bfMCIdp->volume, bfMCIdp->page, bfMCIdp->cell);
        fprintf(stderr, " Can't find BSR_XTRA_XTNTS record.\n");
        return BAD_DISK_VALUE;
    }

    bsXACnt = xtraXtntRecp->xCnt;
    bsXA = xtraXtntRecp->bsXA;
    if ( xtraXtntRecp->xCnt > BMT_XTRA_XTNTS ) {
        fprintf(stderr, "load_xtra_xtnts: %d extents in an xtra extent cell\n",
          xtraXtntRecp->xCnt);
        return BAD_DISK_VALUE;
    }

    /* TODO. Use realloc */
    if ( *idxp + bsXACnt > bfp->xmap.cnt ) {
        extentT *tmp;
        fprintf(stderr,
          "load_xtra_xtnts: more extents found (%d) than expected (%d).\n",
          *idxp + bsXACnt, bfp->xmap.cnt);
        tmp = malloc((*idxp + bsXACnt) * sizeof(extentT));
        if ( tmp == NULL ) {
            perror("load_xtra_xtnts: malloc failed");
            return ENOMEM;
        }

        bcopy(bfp->xmap.xtnt, tmp, bfp->xmap.cnt * sizeof(extentT));
        free(bfp->xmap.xtnt);
        bfp->xmap.xtnt = tmp;
        bfp->xmap.cnt = *idxp + bsXACnt;
    }
      
    get_xtnts(bfp, idxp, FETCH_MC_VOLUME(*bfMCIdp), bsXA, bsXACnt);

    *bfMCIdp = bsMCp->mcNextMCId;
    if ( bfMCIdp->volume != 0 ) {
        if ( test_vdi(bfp, "load_xtra_xtnts", bfMCIdp->volume) != OK )
            return BAD_DISK_VALUE;
        if ( test_mcid(bfp, "load_xtra_xtnts", *bfMCIdp) != OK )
            return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/* Given a disk extent array and a count, load the bfp->xmap.xtnts array.
** Test the extent contents.
**
** Input: bsXA = array of extents from disk
**        cnt = number of entries in bsXA
**        *idxp = start filling bfp->xmap here
** Output: bfp->xmap.xtnts (partially) filled in
**         bfp->pages - one page past last page (so far).
**         *idxp = one page past last page filled.
** Errors: diagnostic output to stderr for all errors
** Return: none
*/ 
/* TODO: rename to load_xtnts() only after load_xtnts is renamed */
static void
get_xtnts(bitFileT *bfp, int *idxp, vdIndexT vdi, bsXtntT *bsXA, int cnt)
{
    int i;
    bf_fob_t lastFob;

    /* TODO?     check cnt + *idxp <= bfp->xmap.cnt */
    if ( *idxp == 0 ) {
        lastFob = 0;
        if ( bsXA[0].bsx_fob_offset != 0 ) {
            fprintf(stderr, "Bad extent map: doesn't start at page 0\n");
        }
    } else {
        lastFob = bfp->xmap.xtnt[*idxp - 1].fob;
        if ( bsXA[0].bsx_fob_offset == lastFob ) {
            if ( bfp->xmap.xtnt[*idxp - 1].blk == XTNT_TERM ) {
                /* Collapse the two identical entries at the end of one */
                /* extent map and the beginning of the next extent map. */
                (*idxp)--;
                /* fool the test in the for loop */
                lastFob--;
            } else {
                fprintf(stderr,
          "Bad extent map: extent in previous map at FOB %ld not terminated.\n",
			lastFob);
            }
        } else {
            fprintf(stderr, "Bad extent map: fileset tag %ld.%d, file tag %ld.%d, ",
		    bfp->setTag.tag_num, bfp->setTag.tag_seq,
                    bfp->fileTag.tag_num, bfp->fileTag.tag_seq);
            fprintf(stderr, "has a gap between extents after page %ld.\n",
		    FOB2PAGE(lastFob));
        }
    }

    for ( i = 0; i < cnt; i++, (*idxp)++ ) {
        bfp->xmap.xtnt[*idxp].vol = vdi;
        bfp->xmap.xtnt[*idxp].fob = bsXA[i].bsx_fob_offset;
        bfp->xmap.xtnt[*idxp].blk = bsXA[i].bsx_vd_blk;

        if ( *idxp != 0 && bsXA[i].bsx_fob_offset <= lastFob ) {
            fprintf(stderr,
      "Bad extent map: fileset tag %ld.%d, file tag %ld.%d, page %ld follows page %ld.\n",
		    bfp->setTag.tag_num, bfp->setTag.tag_seq,
                    bfp->fileTag.tag_num, bfp->fileTag.tag_seq, 
		    FOB2PAGE(bfp->xmap.xtnt[*idxp].fob), 
		    FOB2PAGE(lastFob));
        }

        lastFob = bsXA[i].bsx_fob_offset;

        if ( test_blk(bfp, vdi, bsXA[i].bsx_vd_blk, bfp->bfPgSz ) != OK ) {
            fprintf(stderr, "Bad extent map: bad block (%ld) at FOB %ld\n",
              bfp->xmap.xtnt[*idxp].blk, bsXA[i].bsx_fob_offset);
        }
    }

    bfp->pages = FOB2PAGE(lastFob);
}

/*****************************************************************************/
/* If this is the first volume opened, malloc and init bfp->dmn.
** Malloc and setup bfp->dmn->vols[vdi].
**
** Input: devname - device to open
** Output: bfp->type set to VOL
**         bfp->dmn - may be malloced
**         bfp->fd - set
**         vdi - volume index found by reading volume
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT - volume doesn't exist
**         EBUSY - block device is mounted
**         EACCES - insufficient permissions
**         BAD_DISK_VALUE - bad magic or unexpected disk value
**         ENOMEM - malloc failed
*/
static int
resolve_vol(bitFileT *bfp, char *devname)
{
    bsDmnMAttrT *dmattr;
    int ret;
    vdIndexT vdi;

    bfp->type = VOL;
    if ( bfp->dmn == NULL ) {
        ret = init_dmn(bfp);
        if ( ret != OK ) {
            return ret;
        }
    }

    /* After this call, we know the Version of the on-disk structures */
    ret = open_vol(bfp, devname, &vdi);
    bfp->pmcell.volume = vdi;
    
    if ( bfp->dmn->vols[bfp->pmcell.volume] != NULL )
        bfp->fd = bfp->dmn->vols[bfp->pmcell.volume]->fd;

    return ret;
}

/*****************************************************************************/
/* Given a file name, open the file and test its length.
** All saved reserved files are multiples of 8K. 
**
** Input: name - file pathname to open
**        statp - stats of file
**        bfp->fileTag - used to test length
** Output: bfp->type set to FLE
**         bfp->fd set to fd of file
**         bfp->pages set to number to 8K pages in file
**         bfp->fileName set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         ENOENT - no such file or bad size
**         EACCES - can't access "name" due to permissions
*/
static int
resolve_fle(bitFileT *bfp, char *name, struct stat *statp)
{
    int  fd;
    char errstr[80];

    errstr[0] = '\0';       /* just in case sprintf fails */

    fd = open(name, O_RDONLY);
    if ( fd == -1 ) {
        sprintf(errstr, "open %s failed", name);
        perror(errstr);
        if ( errno == EACCES ) {
            return EACCES;
        } else {
            return ENOENT;
        }
    }

    bfp->type = FLE;
    strcpy(bfp->fileName, name);        /* check for NULL reseults */
    bfp->fd = fd;

    if ( statp->st_size == 0 ) {
            fprintf(stderr, "Bad saved file. File is 0 bytes long.\n");
            return ENOENT;
    }

    bfp->pages = statp->st_size / ADVFS_METADATA_PGSZ;
    if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
        if ( bfp->pages * ADVFS_METADATA_PGSZ != statp->st_size ) {
            fprintf(stderr, "Bad saved file. ");
            fprintf(stderr, "File is not a multiple of 8192 bytes long.\n");
            return ENOENT;
        }
    }

    return OK;
}

/*****************************************************************************/
/*
** Dump bitfile "bfp" to file.
**
** Input: bfp->xmap - list of all pages of file
**        bfp->dmn->vols[] - open volumes of domain on which pages reside
**        dfile = pathname of file to dump bitfile to
** Output: write file
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         EACCES - any error in creat, lseek, read or write
**         BAD_DISK_VALUE - bad vdIndex or block number in exetnts
*/
/* todo: if dfile is a dir, put file under that */
int
dumpBitFile(bitFileT *bfp, char *dfile)
{
    int dumpFd;
    int fd = bfp->fd;
    int i;
    bf_fob_t fobs;
    bf_fob_t totalFobs = 0;
    int ret;
    char fob[ADVFS_FOB_SZ];
    char errstr[80];
    int64_t fileSize = 0;
    statT fs_stat;
    statT *statp;
    size_t writeSz = ADVFS_FOB_SZ;

    errstr[0] = '\0';       /* just in case sprintf fails */

    if (bfp->type == FLE) {
        fprintf(stderr, "dumpBitFile: can't dump a dump file\n");
	return EIO;
    }

    dumpFd = creat(dfile, 0644);
    if ( dumpFd == -1 ) {
        sprintf(errstr, "dumpBitFile: creat \"%s\" failed", dfile);
        perror(errstr);
        return EACCES;
    }

    if ( BS_BFTAG_REG(bfp->setTag) &&
         find_rec(bfp, BMTR_FS_STAT, (char**)&statp) == OK )
    {
        /* statp is unaligned! */
        bcopy(statp, &fs_stat, sizeof(fs_stat));

        statp = &fs_stat;
        fileSize = statp->st_size;
        printf("fileSize %ld\n", fileSize);
        if (bfp->xmap.xtnt[bfp->xmap.cnt - 1].fob > 
	    (fileSize % ADVFS_FOB_SZ ? fileSize / ADVFS_FOB_SZ + 1 :
	     fileSize / ADVFS_FOB_SZ))
        {
            fprintf(stderr, 
		    "extents for %ld FOBS, stat fileSize of %ld FOBS, dump all fobs\n",
		    bfp->xmap.xtnt[bfp->xmap.cnt - 1].fob,
		    (fileSize % ADVFS_FOB_SZ ? fileSize / ADVFS_FOB_SZ + 1 :
		     fileSize / ADVFS_FOB_SZ));
            fileSize = 0;
        }
    }

    for ( i = 0; i < bfp->xmap.cnt; i++ ) {
        off_t seek_off;

	/* check for extent terminator */
        if ( (int32_t)bfp->xmap.xtnt[i].blk == XTNT_TERM ) {
            continue;
        }

        if ( bfp->type & DMN ) {
            if ( bfp->dmn->vols[bfp->xmap.xtnt[i].vol] == NULL ) {
                if ( bfp->type & DMN )
                    printf("No volume %d in filesystem %s\n",
                           bfp->xmap.xtnt[i].vol, bfp->dmn->dmnName);
                else
                    printf("No information about volume %d\n",
                           bfp->xmap.xtnt[i].vol);
                return BAD_DISK_VALUE;
            }
            fd = bfp->dmn->vols[bfp->xmap.xtnt[i].vol]->fd;
        }

        seek_off = (off_t)bfp->xmap.xtnt[i].blk * DEV_BSIZE;
        if ( lseek(fd, seek_off, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, 
		    "dumpBitFile: lseek of %ld blocks into source file failed",
                    bfp->xmap.xtnt[i].blk);
            perror(errstr);
            if ( errno == EACCES ) {
                return EACCES;
            } else {
                return BAD_DISK_VALUE;
            }
        }

        seek_off = (off_t)bfp->xmap.xtnt[i].fob * ADVFS_FOB_SZ;
        if ( lseek(dumpFd, seek_off, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "dumpBitFile: lseek of %ld fobs into dump file failed",
                    bfp->xmap.xtnt[i].fob);
            perror(errstr);
            return EACCES;
        }

        fobs = bfp->xmap.xtnt[i+1].fob - bfp->xmap.xtnt[i].fob;
        for ( ; fobs; fobs-- )
        {
            ssize_t io_ret;

            if ( (io_ret = read(fd, fob, (size_t)ADVFS_FOB_SZ)) != ADVFS_FOB_SZ ) {
                fprintf(stderr, 
			"read number %ld from source failed\n",
                        bfp->xmap.xtnt[i+1].fob - fobs);

                if ( io_ret == -1 ) {
                    perror("dumpBitFile: read failed");
                } else {
                    fprintf(stderr,
			    "dumpBitFile: read %d bytes, expected %d bytes\n",
			    io_ret, ADVFS_FOB_SZ);
                }
                return EACCES;
            }

            if ( fileSize != 0 && fobs == 1 && i == bfp->xmap.cnt - 2 &&
                 fileSize < 
		 ADVFS_FOB_SZ * bfp->xmap.xtnt[bfp->xmap.cnt - 1].fob )
            {
                /* last page. partial write */
                writeSz = fileSize % ADVFS_FOB_SZ;
            }

            if ( (io_ret = write(dumpFd, fob, writeSz)) != writeSz ) {
                fprintf(stderr, "write number %ld to file failed\n",
                        bfp->xmap.xtnt[i+1].fob - fobs);
                if ( io_ret == -1 ) {
                    perror("dumpBitFile: write failed");
                } else {
                    fprintf(stderr,
			    "dumpBitFile: wrote %d bytes, expected %d bytes\n",
			    io_ret, writeSz);
                }
                return EACCES;
            }

            totalFobs++;
            if ( totalFobs % 400 == 0 ) {
                fprintf(stderr, CLEAR_LINE);
                fprintf(stderr, " dumping %ld fobs, %ld fobs dumped\r",
                  PAGE2FOB(bfp->pages), totalFobs);
            }
        }
    }

    fprintf(stderr, CLEAR_LINE);
    if ( writeSz == ADVFS_FOB_SZ ) {
        printf("%ld fobs dumped.\n", totalFobs);
    } else {
        printf("%ld fobs + %d bytes dumped.\n", totalFobs - 1, writeSz);
    }

    if ( close(dumpFd) == -1 ) {
        perror("dumpBitFile: close failed");
    }

    return OK;
}

/*****************************************************************************/

/* The number of the volume containing the current root tag file is
** stored in the BSR_DMN_MATTR record.  There may be BSR_DMN_MATTR
** records on more than one volume, which happens when the log is
** moved to a new volume.  A valid BSR_DMN_MATTR record, has a
** non-=zero sequence number, so we take the root tag directory's
** volume number from that one.
**
** Read page 0 of RBMT on each volume and find the active
** BSR_DNM_MATTR record, which has the tag/volume number of the root
** tag directory.  Fetch the rtag dir's extents into the input bfp
** arg, and return.
**
** Input: bfp->dmn->vols[]
** Output: bfp->xmap.xtnts loaded
**         bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - set to prim mcell of TAG file
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if TAG file not found
*/
int
find_TAG(bitFileT *bfp)
{
    bitFileT *mdBfp;
    bsXtntRT *xtntrp;
    bsMCT *mcellp;
    bsDmnMAttrT dmnMAttr;
    bsDmnMAttrT *bufDmnMAttrp;
    int recFound = 0;
    vdIndexT vdi;
    int rtagVDI;
    int mattrVDI;
    int maxSeqNum = 0;

    assert(bfp);
    if ( bfp->type & FLE ) {
        bfp->setTag = staticRootTagDirTag;
        bfp->pmcell.page = 0;
        BS_BFTAG_RSVD_INIT(bfp->fileTag, 1, BFM_BFSDIR);
        bfp->pmcell.cell = BFM_BFSDIR;
        return OK;
    }

    assert(bfp->dmn);

    /* loop through the volumes looking for the active BSR_DMN_MATTR
       record */

    for ( vdi = 0; vdi < BS_MAX_VDI; vdi++ ) {
        if ( bfp->dmn->vols[vdi] == NULL ) {
            continue;
        }

        mdBfp = bfp->dmn->vols[vdi]->rbmt;
        assert(mdBfp);

        if ( read_page(mdBfp, 0) != OK ) {
            fprintf(stderr,
             "find_TAG: Warning. read_page 0 failed for RBMT on volume %d.\n",
              vdi);
            continue;
        }

        /* the mcell holding the BSR_DMN_MATTR record */
        mcellp = &((bsMPgT *)PAGE(mdBfp))->bsMCA[BFM_RBMT_EXT];

	if (find_bmtr(mcellp, BSR_DMN_MATTR, (char**)&bufDmnMAttrp) != OK ||
	    bufDmnMAttrp->seqNum == 0){
	    /* No (valid) BSR_DMN_MATTR on this volume. */
	    continue;
	}

	/* Save the BSR_DMN_MATTR record. */

	if (bufDmnMAttrp->seqNum > maxSeqNum) {
	    maxSeqNum = bufDmnMAttrp->seqNum;
	    dmnMAttr = *bufDmnMAttrp;
	    mattrVDI = vdi;
	}
    }

    if ( maxSeqNum == 0 ) {
        fprintf(stderr, "*** Corruption: No BSR_DMN_MATTR record found in domain!\n");
	exit(1);
    }

    rtagVDI = BS_BFTAG_VDI(dmnMAttr.bfSetDirTag);

    if ( bfp->dmn->vols[rtagVDI] == NULL ) {
	fprintf(stderr, "WARNING: BSR_DMN_MATTR says root TAG is on vol %d which does not exist.\n", 
		rtagVDI);
	fprintf(stderr, "We'll try looking on BSR_DMN_MATTR rec's vol (%d).\n", vdi);
	rtagVDI = mattrVDI;

    }

    /* go fetch the extents for the root tag file */
 
    bfp->pmcell.cell = BFM_BFSDIR;    /* root tag dir's mcell in RBMT */
    bfp->pmcell.volume = rtagVDI;     /* volume holding the rtag file */
    bfp->pmcell.page = 0;             /* RBMT page 0 */
    bfp->fileTag = dmnMAttr.bfSetDirTag; /* rtag dir's tag number */
    bfp->setTag = staticRootTagDirTag; 

    return load_xtnts(bfp); 
}

/*****************************************************************************/
static int
find_saveset_TAG(bitFileT *bfp)
{
    DIR *stgDir;
    char dirname[MAXPATHLEN];
    struct dirent *dp;
    char *cp;
    int fd;
    int ret;
    char errstr[80];
    struct stat statBuf;

    errstr[0] = '\0';       /* just in case sprintf fails */

    assert(bfp->dmn->dmnName != NULL);
    stgDir = opendir(bfp->dmn->dmnName);
    if ( stgDir == NULL ) {
        fprintf(stderr, "bad filesystem name %s\n", bfp->dmn->dmnName);
        return EACCES;
    }

    for ( dp = readdir(stgDir); dp != NULL; dp = readdir(stgDir) ) {

        if ( strcmp(dp->d_name, "tag") == 0 ) {
            closedir(stgDir);
            strcpy(dirname, bfp->dmn->dmnName);
            strcat(dirname, "/");
            cp = &dirname[strlen(dirname)];
            *cp = '\0';
            strcat(dirname, dp->d_name);
            if ( stat(dirname, &statBuf) == -1 ) {
                sprintf(errstr, "find_saveset_TAG: stat failed for volume %s",
                  dirname);
                perror(errstr);
                return BAD_DISK_VALUE;
            }

            if ( !S_ISREG(statBuf.st_mode) ) {
                printf("bmt is not a regular file\n");
                return BAD_DISK_VALUE;
            }

            ret = resolve_fle(bfp, dirname, &statBuf);

            return ret;
        }
    }

    closedir(stgDir);

    return ENOENT;
}

/*****************************************************************************/
/* Find a the primary extents and load the extent for a file in a fileset.
** The fileset and/or the file can be specified by tag.
**
** Input: argv, argc, optindp - filset name/tag and file name/tag
** Output: bfp->xmap.xtnts filled in
**         bfp->pages filed in
**         bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - set to prim mcell of file
**         optindp points to next input arg
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_SYNTAX - too many args, args not the right kind (int vs string)
**         EINVAL - bad arg range
**         ENOENT - no such set or file
**         BAD_DISK_VALUE - bad extents or tag file or other unexpected value
**         ENOMEM - malloc failed
*/
int
get_set_n_file(bitFileT *bfp, char *argv[], int argc, int *optindp)
{
    bitFileT *setBfp;
    bitFileT *rootBfp;
    int ret;

    assert(bfp->dmn);

    setBfp = malloc(sizeof(bitFileT));
    if ( setBfp == NULL ) {
        return ENOMEM;
    }
    rootBfp = malloc(sizeof(bitFileT));
    if ( rootBfp == NULL ) {
	free(setBfp);
        return ENOMEM;
    }

    newbf(bfp, setBfp);
    newbf(bfp, rootBfp);

    assert(argc > *optindp);

    /* load xtnts of root TAG file */
    if ( (ret = find_TAG(rootBfp)) != OK ) {
        fprintf(stderr, "Couldn't find the root TAG file\n");
	free(setBfp);
	free(rootBfp);
        return ret;
    }

    if ( (ret = get_set(rootBfp, argv, argc, optindp, setBfp)) != OK ) {
	free(setBfp);
	free(rootBfp);
        return ret;
    }

    if ( (ret = get_file(setBfp, argv, argc, optindp, bfp)) != OK ) {
	free(setBfp);
	free(rootBfp);
        return ret;
    }

    free(setBfp);
    free(rootBfp);
    return OK;
}


/*****************************************************************************/
/* Given the definition of the root TAG file and the name or tag of a file set,
** find the primary mcell and load the extents of the fileset.
** The fileset can be specified by name or tag.
** "bfp" is filled in to represet the fileset's TAG file.
**
** Input: argv, argc, optindp - fileset name or tag
**        rootTag - definition of root TAG file
** Output: bfp->xmap.xtnts - filled in for fileset
**         bfp->pages - set
**         bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - primary mcell of fileset
**         bfp->setTag, bfp->fileTag - set
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_SYNTAX - command line syntax errors
**         EINVAL - out of range numbers
**         ENOENT - no such set
**         BAD_DISK_VALUE - bad extents or tag file or other unexpected value
**         ENOMEM - malloc failed
*/
int
get_set(bitFileT *rootTag, char *argv[], int argc, int *optindp, bitFileT *bfp)
{
    bfTagT tag;
    int64_t longnum;
    int ret;
    bsBfSetAttrT *bsBfSetAttrp;

    bfp->setTag = staticRootTagDirTag; 

    /* if the user specifies as snapshot on the command line (via tag
       id), then use that as the fileset */

    if ( strcmp(argv[*optindp], "-t") == 0 ) {
        (*optindp)++;
        if ( argc == *optindp ) {
            fprintf(stderr, "Missing snapshot tag number\n");
            return BAD_SYNTAX;
        }

        /* get set tag */
        if ( getnum(argv[*optindp], &longnum) != OK ) {
            fprintf(stderr, "Badly formed snapshot tag number: \"%s\"\n",
              argv[*optindp]);
            (*optindp)++;
            return BAD_SYNTAX;
        }

        (*optindp)++;

        if ( longnum < 1 || longnum > MAXTAG ) {
            fprintf(stderr, "Invalid snapshot tag number (%ld). ", longnum);
            fprintf(stderr, "Valid range is 1 through %ld\n", MAXTAG);
            return EINVAL;
        }

        ret = get_file_by_tag(rootTag, (uint64_t)longnum, bfp);
        if ( ret == ENOENT ) {
            fprintf(stderr, "Snapshot tag %ld is not in use.\n", longnum);
            return ret;
        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "Snapshot tag %ld has corruption in it's mcells.\n", longnum);
            return ret;
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding snapshot tag %ld.\n", longnum);
            return ret;
        }

        /* get_file_by_tag clobbers setTag */
        bfp->setTag = staticRootTagDirTag; 

        ret = find_rec(bfp, BSR_BFS_ATTR, (char**)&bsBfSetAttrp);
        if ( ret != OK ) {
            /* FIX. let find_rec print for BAD_DISK. print here for ENOENT */
            fprintf(stderr,
    "Primary mcell at vol %d, page %ld, cell %d: can't find BSR_BFS_ATTR rec\n",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
            return ret;
        }

        strcpy(bfp->setName, bsBfSetAttrp->setName);

	/* if this fileset is a snapshot (i.e. it has a parent)... */
        if (bsBfSetAttrp->bfsaParentSnapSet.dirTag.tag_num != 0 ) {
            bfp->origSetp = malloc(sizeof(bitFileT));
            if ( bfp->origSetp == NULL ) {
                perror("malloc origSet");
                goto done;
            }

            newbf(bfp, bfp->origSetp);
            ret = get_file_by_tag(rootTag, bsBfSetAttrp->bfsaParentSnapSet.dirTag.tag_num, bfp->origSetp);
            if ( ret == ENOENT ) {
                fprintf(stderr, "Fileset tag %ld is not in use.\n", longnum);
                free(bfp->origSetp);
                bfp->origSetp = NULL;
                goto done;
            } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
                  "Fileset tag %ld has corruption in it's mcells.\n", longnum);
                free(bfp->origSetp);
                bfp->origSetp = NULL;
                goto done;
            } else if ( ret != OK ) {
                fprintf(stderr, "Error in finding fileset tag %ld.\n", longnum);
                free(bfp->origSetp);
                bfp->origSetp = NULL;
                goto done;
            }
            /* get_file_by_tag clobbers this */
            bfp->origSetp->setTag = staticRootTagDirTag;
        }

done:
        return OK;

    } else if ( bfp->type == FLE ) {
        printf("Can't get snapshot by name from saved tag file.\n");
        return ENOENT;

    }
    /* If the user specifies as snapshot name on the command line,
       use that as the fileset */ 
    else if ( strcmp(argv[*optindp], "-S") == 0 ) {

	(*optindp)++;
	if ( argc == *optindp ) {
	    fprintf(stderr, "Missing snapshot name\n");
	    return BAD_SYNTAX;
	}

        /* replace root TAG def in bfp with def of set tag bf */
        ret = find_set_by_name(rootTag, argv[*optindp], bfp);
        if ( ret == ENOENT ) {
            /* get fileset tag */
            if ( getnum(argv[*optindp], &longnum) != OK ) {
                fprintf(stderr, "No such snapshot: \"%s\"\n", argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            if ( longnum < 1 || longnum > MAXTAG ) {
                fprintf(stderr, "No such snapshot tag number or name (%s).\n",
                  argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            ret = get_file_by_tag(rootTag, (uint64_t)longnum, bfp);
            if ( ret == ENOENT ) {
                fprintf(stderr,
                  "There is no snapshot with name or tag %s in this filesystem.\n",
                  argv[*optindp]);
                  (*optindp)++;
                  return ret;
            } else if ( ret == BAD_DISK_VALUE ) {
                fprintf(stderr,
                  "Snapshot tag %ld has corruption in it's mcells.\n",
                  longnum);
                (*optindp)++;
                return ret;
            } else if ( ret != OK ) {
                fprintf(stderr, "Error in finding snapshot tag %ld.\n",
                  longnum);
                (*optindp)++;
                return ret;
            }

            /* get_file_by_tag clobbers this */
            bfp->setTag = staticRootTagDirTag; 

            ret = find_rec(bfp, BSR_BFS_ATTR, (char**)&bsBfSetAttrp);
            if ( ret != OK ) {
                /* find_rec prints error. don't do here */
                fprintf(stderr,
    "Primary mcell at vol %d, page %ld, cell %d: can't find BSR_BFS_ATTR rec\n",
                  bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
                (*optindp)++;
                return ret;
            }

            strcpy(bfp->setName, bsBfSetAttrp->setName);

	    /* if this fileset is a snapshot (i.e. it has a parent)... */
	    if (bsBfSetAttrp->bfsaParentSnapSet.dirTag.tag_num != 0 ) {
                bfp->origSetp = malloc(sizeof(bitFileT));
                if ( bfp->origSetp == NULL ) {
                    perror("malloc origSet");
                    goto done1;
                }

                newbf(bfp, bfp->origSetp);
                ret = get_file_by_tag(rootTag, 
			   bsBfSetAttrp->bfsaParentSnapSet.dirTag.tag_num,
                           bfp->origSetp);
                if ( ret == ENOENT ) {
                    fprintf(stderr, "Snapshot tag %ld is not in use.\n",
                      longnum);
                    free(bfp->origSetp);
                    bfp->origSetp = NULL;
                    goto done1;
                } else if ( ret == BAD_DISK_VALUE ) {
                    fprintf(stderr,
                      "Snapshot tag %ld has corruption in it's mcells.\n",
                      longnum);
                    free(bfp->origSetp);
                    bfp->origSetp = NULL;
                    goto done1;
                } else if ( ret != OK ) {
                    fprintf(stderr, "Error in finding snapshot tag %ld.\n",
                      longnum);
                    free(bfp->origSetp);
                    bfp->origSetp = NULL;
                    goto done1;
                }
                /* get_file_by_tag clobbers this */
                bfp->origSetp->setTag = staticRootTagDirTag;
            }

        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "Snapshot %s has corruption in it's mcells.\n", argv[*optindp]);
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding snapshot %s.\n", argv[*optindp]);
        }

done1:
        /* FIX. move up */
        (*optindp)++;   /* count the fileset name */

    }
     /* else, a snapshot has not been specified on the command line,
	just use the default fileset */
    else {

	bfp->setTag = staticRootTagDirTag; 

	/* replace root TAG def in bfp with def of set tag bf */

	ret = find_set_by_name(rootTag, "default", bfp);

	if ( ret == ENOENT ) {
	    fprintf(stderr, "No default fileset!\n");
	} else if ( ret == BAD_DISK_VALUE ) {
	    fprintf(stderr,
		    "Fileset has corruption in it's mcells.\n");
	} else if ( ret != OK ) {
	    fprintf(stderr, "Error looking for default fileset.\n");
	}
    }

    return ret;

} /* get_set */


/*****************************************************************************/
/* Read each root TAG page. For each fileset, read the (R)BMT page of the
** primary mcell and find the BSR_BFS_ATTR record. Compare the given name
** with the fileset name in the BSR_BFS_ATTR record. If it matches load
** the fileset extents.
**
** Input: name = fileset name
**        rootBfp - root TAG file
** Output: bfp->xmap.xtnts, bfp->pages - set
**         bfp->setName - set
**         bfp->pmcell.volume, ppage, pcell - set to primary mcell of fileset TAG file
**         bfp->setTag - set to root set tag (-2)
**         bfp->fileTag - set to fileset tag in root TAG file
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went OK
**         ENOENT if no such fileset
**         BAD_DISK_VALUE - bad extents or tag file or directory
**         ENOMEM - malloc failed
*/
static int
find_set_by_name(bitFileT *rootBfp, char *name, bitFileT *bfp)
{
    int j;
    int vi;
    bs_meta_page_t pg;
    bsMPgT *page;
    bsBfSetAttrT *bsBfSetAttrp;
    bfMCIdT bfMCId;
    int i;
    bsTDirPgT *pdata;
    bsXtntRT *bsXtntRp;
    int xtntOff;
    bsMRT *recordp;
    int ret;
    bfTagT tag;

    ret = get_max_tag_pages(rootBfp, &pg);
    if ( ret != OK ) {
        return ret;
    }

    tag.tag_seq = 0;
    /* for each tag (fileset) in the root tag file */
    for ( tag.tag_num = 1; tag.tag_num < pg * BS_TD_TAGS_PG; tag.tag_num++ ) {
        ret = get_prim_from_tagdir(rootBfp, &tag, bfp);
        if ( ret != OK ) {
            continue;
        }

        if ( find_rec(bfp, BSR_BFS_ATTR, (char**)&bsBfSetAttrp) != OK ) {
            fprintf(stderr,
    "Primary mcell at vol %d, page %ld, cell %d: can't find BSR_BFS_ATTR rec\n",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
            continue;
        }

        /*  cmp name to setName */
        if ( strcmp(bsBfSetAttrp->setName, name) != 0 ) {
            continue;
        }

        strcpy(bfp->setName, name);

        if ( bsBfSetAttrp->bfsaParentSnapSet.dirTag.tag_num != 0 ) {
            if (bsBfSetAttrp->bfsaParentSnapSet.dirTag.tag_num > MAXTAG ) {
                fprintf(stderr, "bad set number\n");
            } else {
                (void)get_orig_set(rootBfp, 
			    bsBfSetAttrp->bfsaParentSnapSet.dirTag.tag_num, 
			    bfp);
            }
        }

	ret = load_xtnts(bfp);
	if ( ret != OK ) {
	    fprintf(stderr,
		    "find_set_by_name: load_xtnts_from_prim failed\n");
	    return ret;
	}
        return OK;
    }

    return ENOENT;
}

/*****************************************************************************/
/*
** Find the number of good tag pages. When a tag file is expanded it grows
** by 8 pages at a time. However the pages are not all initialized.
** The first entry on the first page tells how many pages are initialized
** ie howmany valid pages there are in the file.
**
** Input: tagBfp is the tag (root or fileset) tag file
** Output: pgs is the returned number of valid pages in the tag file.
** Errors: diagnostic output to stderr for bad values
** Return: OK if all is well
**         BAD_DISK_VALUE if tag page header has a bad page number
**         ENOMEM if malloc fails
*/
int
get_max_tag_pages(bitFileT *tagBfp, bs_meta_page_t *pgs)
{
    bsTDirPgT *tagDirPg;
    int ret;

    ret = read_tag_page(tagBfp, 0, &tagDirPg);
    if ( ret != OK ) {
        return ret;
    }

    if ( tagDirPg->tMapA[0].tm_u.tm_s1.unInitPg > tagBfp->pages ) {
        fprintf(stderr, "get_max_tag_pages: Bad unInitPg %d. Must be < %d\n",
          tagDirPg->tMapA[0].tm_u.tm_s1.unInitPg, tagBfp->pages);

        *pgs = tagBfp->pages;
    }

    *pgs = tagDirPg->tMapA[0].tm_u.tm_s1.unInitPg;

    return OK;
}

/*****************************************************************************/
/*
** Read a page in the tag file and check the page header page number
**
** Input: tagBfp is the tag (root or fileset) tag file
**        pg is the page number to read.
** Output: retBsTDirPg is the returned tag page
** Errors: diagnostic output to stderr for bad values
** Return: OK if all is well
**         BAD_DISK_VALUE if tag page header has a bad page number
**         ENOMEM if malloc fails
*/
int
read_tag_page(bitFileT *tagBfp, bs_meta_page_t pg, bsTDirPgT **retBsTDirPg)
{
    bsTDirPgT *tagDirPg;
    int ret;

    ret = read_page(tagBfp, pg);
    if ( ret != OK ) {
        fprintf(stderr,
          "read_tag_page: can't read page %ld of the tag file\n", pg);
        return ret;
    }

    tagDirPg = (bsTDirPgT*)tagBfp->pgBuf;
    *retBsTDirPg = tagDirPg;

    if ( tagDirPg->tpPgHdr.currPage != pg ) {
        fprintf(stderr, "Bad tag page. Read page %ld, currPage is %ld.\n",
          pg, tagDirPg->tpPgHdr.currPage);
        return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/* Given the tag of the original set for the clone set, find the original
** set and attach it to the clone set bitFile structure.
**
** Input:  rootBfp - root TAG file
**         tagnum - tag number of original set
**         bfp - clone set
** Output: bfp->origSetp - setup
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went OK
**         ENOENT if no such fileset
**         BAD_DISK_VALUE - bad extents or tag file or directory
**         ENOMEM - malloc failed
*/
static int
get_orig_set(bitFileT *rootBfp, uint64_t tagnum, bitFileT *bfp)
{
    int ret;

    bfp->origSetp = malloc(sizeof(bitFileT));
    if ( bfp->origSetp == NULL ) {
        perror("malloc origSet");
        return ENOMEM;
    }
    newbf(bfp, bfp->origSetp);

    ret = get_file_by_tag( rootBfp, tagnum, bfp->origSetp);
    /* get_file_by_tag clobbers this */
    bfp->origSetp->setTag = staticRootTagDirTag; 

    if ( ret == ENOENT ) {
        fprintf(stderr, "Fileset tag %ld is not in use.\n", tagnum);
        free(bfp->origSetp);
        bfp->origSetp = NULL;
        return ENOENT;
    } else if ( ret == BAD_DISK_VALUE ) {
        fprintf(stderr, "Fileset tag %ld has corruption in it's mcells.\n",
             tagnum);
        free(bfp->origSetp);
        bfp->origSetp = NULL;
        return ENOENT;
    } else if ( ret != OK ) {
        fprintf(stderr, "Error in finding fileset tag %ld.\n", tagnum);
        free(bfp->origSetp);
        bfp->origSetp = NULL;
        return ENOENT;
    }

    return OK;
}

/******************************************************************************/
/* Find the primary mcell and load the extents of a file given the fileset TAG
** file and a file path name or a file tag. The path name is relative to the
** root of the fileset. Ie it does not begin with a /, nor does it include
** the mount point (especially since the fileset is probably not mounted).
**
** Input: argv, argc, optindp - file pathname or file tag number
**        setBfp is the fileset TAG file
** Output: bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - set to primary mcell of file
**         bfp->xtnts - filled in for file in fileset
** Errors: diagnostic output to stderr for all errors
** Return: Ok if all went well
**         BAD_SYNTAX for cammand line syntax errors
**         EINVAL for out of range numbers
**         ENOENT - no such file
**         BAD_DISK_VALUE - bad extents, tag file, directory, etc
**         EINVAL - tag number out of range
**         ENOMEM - malloc failed
*/
/* TODO: Do we need -f <name> ? else how to handle file named "-t" */
int
get_file(bitFileT *setBfp, char *argv[], int argc, int *optindp,
               bitFileT *bfp)
{
    int64_t longnum;
    bfTagT tag;
    int ret;

    /* get file by tag */
    if ( strcmp(argv[*optindp], "-t") == 0 ) {
        (*optindp)++;   /* count the -t */

        if ( argc == *optindp ) {
            fprintf(stderr, "Missing file tag number.\n");
            return BAD_SYNTAX;
        }

        /* get file tag number */
        if ( getnum(argv[*optindp], &longnum) != OK ) {
            fprintf(stderr, "Badly formed set tag number: \"%s\"\n",
              argv[*optindp]);
            (*optindp)++;   /* count the tag number */
            return BAD_SYNTAX;
        }

        (*optindp)++;   /* count the tag number */

        if ( longnum < 1 || longnum > MAXTAG ) {
            fprintf(stderr, "Invalid file tag number (%ld). ", longnum);
            fprintf(stderr, "Valid range is 1 through %ld\n", MAXTAG);
            return EINVAL;
        }

        ret = get_file_by_tag(setBfp, longnum, bfp);
        if ( ret == ENOENT ) {
            fprintf(stderr, "File tag %ld is not in use.\n", longnum);
        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "File tag %ld has corruption in it's mcells.\n", longnum);
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding file tag %ld.\n", longnum);
        }
    } else { /* get file by name */
        if ( bfp->type & FLE ) {
            printf("don't have dir to lookup file\n");
            return EINVAL;
        }

        ret = find_file_by_name(setBfp, argv[*optindp], bfp);
        if ( ret == ENOENT ) {
            /* Couldn't find a file called 123. Look for tag 123. */
            if ( getnum(argv[*optindp], &longnum) != OK ) {
                fprintf(stderr, "No such file: \"%s\"\n", argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            if ( longnum < 1 || longnum > MAXTAG ) {
                fprintf(stderr, "No such file tag number or name (%s).\n",
                  argv[*optindp]);
                (*optindp)++;
                return ENOENT;
            }

            ret = get_file_by_tag(setBfp, longnum, bfp);
            if ( ret == ENOENT ) {
                fprintf(stderr,
                  "There is no file with name or tag %s in this fileset.\n",
                  argv[*optindp]);
            } else if ( ret == BAD_DISK_VALUE ) {
                fprintf(stderr,
                  "File tag %ld has corruption in it's mcells.\n", longnum);
            } else if ( ret != OK ) {
                fprintf(stderr, "Error in finding file tag %ld.\n",
                  longnum);
            }
        } else if ( ret == BAD_DISK_VALUE ) {
            fprintf(stderr,
              "Found corrupted metadata while searching for \"%s\".\n",
              argv[*optindp]);
        } else if ( ret != OK ) {
            fprintf(stderr, "Error in finding file %s.\n", argv[*optindp]);
        }

        (*optindp)++;   /* count the file name */
    }

    return ret;
}

/*****************************************************************************/
/* Given a root or fileset TAG file and a tag, fill in bfp and load the extents.
**
** Input: tagBfp - TAG file
**        tagnum - tag of file in the TAG file
** Output: bfp - file to init
** Errors: status return only. No diagnostic printf
** Return: OK if all goes well
**         ENOENT if tag not in use in TAG file
**         BAD_DISK_VALUE - bad extents
**         ENOMEM - malloc failed
*/
int
get_file_by_tag(bitFileT *tagBfp, uint64_t tagnum, bitFileT *bfp)
{
    bfTagT tag;
    int ret;

    tag.tag_num = tagnum;
    tag.tag_seq = 0;

    bfp->setTag = tagBfp->fileTag;
    bfp->setp = tagBfp;

    /* get the prim mcell of the file/set from the given TAG file */
    if ( get_prim_from_tagdir(tagBfp, &tag, bfp) != OK ) {
        return ENOENT;
    }

    return load_clone_xtnts(bfp);
}

/*****************************************************************************/
/*
** Find the prime mcell given the fileset tag and the file tag.
** Reads the fileset tag file to obtain the prime mcell information.
**
** Input: tagBfp is the tag (root or fileset) tag file
**        tag is the tag to look up in the tag file
**        tag.tag_seq - if 0, just match num. else match seq also
** Output: bfp->pmcell.volume, ppage, pcell - set to the prim mcell
**         bfp->fileTag
** Errors: diagnostic output to stderr for all errors
** Return: OK if all goes well
**         BAD_DISK_VALUE if some value read from disk is out of bounds
**         ENOENT if tag is not in use
**         ENOMEM malloc failed
*/
static int
get_prim_from_tagdir(bitFileT *tagBfp, bfTagT *tag, bitFileT *bfp)
{
    bsTDirPgT *tagDirPg;
    int ret;
    bs_meta_page_t pgs;

    /* sanity check the tag we're supposed to look up */

    if ( get_max_tag_pages(tagBfp, &pgs) == OK && TAGTOPG(tag) >= pgs ) {
        fprintf(stderr, "Tag %ld is beyond the valid pages in the tag file.\n",
          tag->tag_num);
        return ENOENT;
    }

    ret = read_tag_page(tagBfp, TAGTOPG(tag), &tagDirPg);
    if ( ret != OK ) {
        return ret;
    }

    if ( !(tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.flags & BS_TD_IN_USE) )
        return ENOENT;

    bfp->pmcell = tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.bfMCId;
    bfp->fileTag.tag_num = tag->tag_num;
    bfp->fileTag.tag_seq = tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.seqNo;
    
    if ( tag->tag_seq &&
         tag->tag_seq != tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.seqNo )
    {
        fprintf(stderr,
          "file tag %ld: dir seq number %d does not match tag file seqNo %d.\n",
          tag->tag_num, tag->tag_seq, tagDirPg->tMapA[TAGTOSLOT(tag)].tm_u.tm_s3.seqNo);
    }

    /* test primary mcell vdi */
    if ( test_vdi(bfp, NULL, bfp->pmcell.volume) != OK ) {
        fprintf(stderr, "get_prim_from_tagdir: bad vdi (%d)\n", bfp->pmcell.volume);
        return BAD_DISK_VALUE;
    }

    /* FIX. use test_mcid */
    /* test primary mcell page */
    if ( bfp->pmcell.page >= tagBfp->dmn->vols[bfp->pmcell.volume]->bmt->pages ) {
        fprintf(stderr, "Fileset %ld tag file, file %ld. ",
          tagBfp->fileTag.tag_num, tag->tag_num);
        fprintf(stderr, "Bad prim mcell page %ld. ", bfp->pmcell.page);
        fprintf(stderr, "Valid range: 0 - %ld.\n",
           tagBfp->dmn->vols[bfp->pmcell.volume]->bmt->pages);
        return BAD_DISK_VALUE;
    }

    /* test primary mcell number */
    if ( bfp->pmcell.cell >= BSPG_CELLS ) {
        fprintf(stderr, "Tag file read for tag %ld:\n", tag->tag_num);
        fprintf(stderr, "primary mcell number (%d) is too large.\n",
                bfp->pmcell.cell);
        return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/*
** TODO: rename to get_file_prim_from_set() OR add load_xtnts()
*/
/* Find the primary mcell given a set tag and a fileset relative path name.
** Reads the fileset tag file to find the root dir prime mcell.
** Loads the root tag extents and reads the directory.
** For each component of the path: find the tag in the directory, find
** the prime mcell in the fileset tag file and load the extents.
** At the last component return the prime mcell. Does not load extents of
** found file.
**
** Input: tagBfp is tag file
**        path is fileset relative path
** Output: bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - primary mcell of found file
** Errors: diagnostic output to stderr for all errors
** Return: OK if file found in set
**         ENOENT - file does not exist
**         BAD_DISK_VALUE - bad extents, tag file or directory
**         EINVAL - badly formed .tags pathname
**         ENOMEM - malloc failure
*/
#define TAG3 98

static int
find_file_by_name(bitFileT *tagBfp, char *path, bitFileT *bfp)
{
    bfTagT tag;
    bitFileT dirBf;
    char *component;
    char *component_end;
    char *path_end;
    int ret;
    bitFileT *dirBfp = &dirBf;    /* Used for each dir in the path */

    bfp->setTag = tagBfp->fileTag;
    bfp->setp = tagBfp;

    newbf(bfp, &dirBf);
    dirBf.setp = bfp->setp;
    /* tag 2 is the fileset root directory */
    tag.tag_num = ROOT_FILE_TAG;
    tag.tag_seq = 0;
    ret = get_prim_from_tagdir(tagBfp, &tag, &dirBf);
    if ( ret != OK ) {
        return ret;
    }

    component = path;
    path_end = &path[strlen(path)];
    component_end = strchr(path, (int)'/');
    if ( component_end ) {
        *component_end = '\0';
    }

    while(1) {
        ret = load_clone_xtnts(&dirBf);
        if ( ret != OK ) {
            return ret;
        }

        ret = search_dir(tagBfp, dirBfp, component);
        if ( ret == TAG3 ) {
            ret = find_prim_from_dot_tags(component, dirBfp, tagBfp, bfp);
        }
        if ( ret != OK ) {
            return ret;
        }

        if ( &component[strlen(component)] == path_end ) {
            *bfp = *dirBfp;
            break;
        }

        component = &component[strlen(component)];
        *component = '/';
        component++;
        component_end = strchr(component, (int)'/');
        if ( component_end ) {
            *component_end = '\0';
        } else {
            /* last component in path */
            if ( ret == TAG3 )
                return ret;
        }
    }

    return load_clone_xtnts(bfp);
}

/*****************************************************************************/
/* The next path component in not in the directory.
** It is in the .tags directory. It is a tag.
** Called from find_file_by_name
**
** Input: component - path name in .tags dir
**        dirBfp - ???
**        tagBfp - tag file description
** Output: bfp - primary mcell filled in
** Errors: diagnostic output to stderr for all errors
** Return: OK - component found and resolved
**         ENOENT - no such component
**         BAD_SYNTAX - not a number in .tags dir
**         EINVAL - invalid number in .tags
*/
static int
find_prim_from_dot_tags(char *component, bitFileT *dirBfp, bitFileT *tagBfp,
                       bitFileT *bfp)
{
    bfTagT tag;
    int64_t longNum;

    /* this component is a tag. ie 3 or M-6 */
    /* get the prim mcell from the fileset tag file */
    if ( component[0] == 'M' ) {
        component++;
    }

    if ( getnum(component, &longNum) != OK ) {
        fprintf(stderr, "Badly formed set tag number: \"%s\"\n", component);
        return BAD_SYNTAX;
    }

    if ( longNum == 0 ) {
        fprintf(stderr, "Invalid tag number (0).\n");
        return EINVAL;
    }

    tag.tag_num = (uint64_t)longNum;
    tag.tag_seq = 0;
    if ( BS_BFTAG_RSVD(tag) ) {
        if ( find_rsrv_prim_from_tag(dirBfp, tag) != OK ) {
            fprintf(stderr, "tag %ld does not exist\n", tag.tag_num);
            return ENOENT;
        }

        *bfp = *dirBfp;
        return OK;
    }

    if ( get_prim_from_tagdir(tagBfp, &tag, dirBfp) != OK ) {
        fprintf(stderr, "tag %ld does not exist\n", tag.tag_num);
        return ENOENT;
    }

    *bfp = *dirBfp;
    return OK;
}

/*****************************************************************************/
/* Given the tag of a reserved file, find the primary mcell.
** The tag gives the volume and the reserved file.
** Reads every volume and lloks for BSR_VD_ATTR record with matching vdIndex.
** Called from find_prim_from_dot_tags.
**
** Input: bfp->dmn->vols[]->(r)bmt - (R)BMT files on volumes in domain
**        tag - tag of reserved file
** Output: bfp->fd - fd of found volume
**         bfp->pmcell.volume, bfp->pmcell.page, bfp-pcell = primary mcell of reserved file
**         bfp->xmap setup
** Errors: diagnostic output to stderr for all errors
** Return: OK if reserved file found
**         EINVAL for bad tag number
**         ENOENT for no such reserved file tag found
**         errors from read_vol_at_blk
*/
static int
find_rsrv_prim_from_tag(bitFileT *bfp,  bfTagT tag)
{
    int vol = -(int64_t)tag.tag_num / BFM_RSVD_TAGS;
    int cell = -(int64_t)tag.tag_num % BFM_RSVD_TAGS;
    int i;
    int attr_mcell = BFM_RBMT;
    int ret;
    bsMPgT *bsMPgp;
    bsMCT *mcellp;
    bsVdAttrT *bsVdAttrp;
    bitFileT *mdBfp;

    assert(bfp);
    assert(bfp->dmn);
    assert(BS_BFTAG_RSVD(tag));

    if ( cell == BFM_RBMT_EXT )
    {
        printf("No reserved file with tag %ld\n", -(cell) );
        return EINVAL;
    }

    for ( i = 0; i < BS_MAX_VDI; i++ ) {
        if ( bfp->dmn->vols[i] ) {

	  /* reserved files use the rbmt for their extents */
	    mdBfp = bfp->dmn->vols[i]->rbmt;

            mdBfp->pgLbn = RBMT_BLK_LOC;  /* block 16 */
            ret = read_vol_at_blk(mdBfp);
            if ( ret != OK ) {
                return ret;
            }
            bsMPgp = (bsMPgT*)mdBfp->pgBuf;

            mcellp = &bsMPgp->bsMCA[attr_mcell];
            ret = find_bmtr(mcellp, BSR_VD_ATTR, (char**)&bsVdAttrp);
            if ( ret != OK ) {
                fprintf(stderr,
     "find_rsrv_prim_from_tag: Could not find BSR_VD_ATTR record in cell %d.\n",
                  attr_mcell);
                return ret;
            }

            if ( vol != bsVdAttrp->vdIndex ) {
                continue;
            }

            bfp->fd = bfp->dmn->vols[i]->fd;
            bfp->pmcell.page = 0;
            bfp->pmcell.cell = cell;
            bfp->pmcell.volume = i;
            /* FIX. notice this only works for BMT, not any other rsv file. */
            bfp->xmap = bfp->dmn->vols[bfp->pmcell.volume]->rbmt->xmap;
            bfp->pages = bfp->dmn->vols[bfp->pmcell.volume]->rbmt->pages;

            return OK;
        }
    }

    printf("no volume %d corresponding to reserved tag %ld\n", vol, tag.tag_num);

    return ENOENT;
}

/******************************************************************************/
/* Given an mcell ID (vol, page, mcell), read the page, find the
** mcell, find and test the chain pointer.
**
** Input: bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - point to the primary mcell
** Output: vol, mcidp - point to the chain mcell
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE for bad mcell contents
*/
/* TODO: set setTag
**       if filetag == 0 set it
*/
int
get_chain_mcell(bitFileT *bfp, bfMCIdT *mcidp)
{
    bsMPgT    *mcpage;
    bsMRT     *recordp;
    int       xtntOff;
    bsXtntRT  *bsXtntRp;
    int ret;
    bitFileT *mdBfp;

    assert(bfp);
    if ( !(bfp->type & FLE) ) {
        assert(bfp->dmn);
        assert(bfp->pmcell.volume < BS_MAX_VDI);
        assert(bfp->dmn->vols[bfp->pmcell.volume]);

        if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
            mdBfp = bfp->dmn->vols[bfp->pmcell.volume]->rbmt;
        } else {
            mdBfp = bfp->dmn->vols[bfp->pmcell.volume]->bmt;
        }
        assert(mdBfp);
        assert(bfp->pmcell.page < mdBfp->pages);
        assert(bfp->pmcell.cell < BSPG_CELLS);

        /* read primary mcell from (R)BMT */
        if ( ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(bfp->pmcell)) ) {
            fprintf(stderr, "get_chain_mcell: read_page fail\n");
            return ret;
        }

        mcpage = (bsMPgT*)mdBfp->pgBuf;
    } else {
        /* read  page specified from the dump file */
        if ( ret = read_page(bfp, (bs_meta_page_t)FETCH_MC_PAGE(bfp->pmcell)) ) {
            fprintf(stderr, "get_chain_mcell: read_page fail\n");
            return ret;
        }

        mcpage = (bsMPgT*)bfp->pgBuf;
    }

    ret = find_bmtr(&mcpage->bsMCA[bfp->pmcell.cell], BSR_XTNTS, (char**)&bsXtntRp);
    if ( ret != OK ) {
	if (ret == ENOENT) {
	    return NOT_FOUND;
	}
        return BAD_DISK_VALUE;
    }

    *mcidp = bsXtntRp->chainMCId;

    if ( mcidp->volume == 0 ) {
        if ( mcidp->page != 0 ) {
            fprintf(stderr,
              "%s vol %d page %ld cell %d: bad chainMCId.page (%ld) for vdi=0\n",
              BS_BFTAG_RSVD(bfp->fileTag) ? "RBMT" : "BMT",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell, mcidp->page);
            return BAD_DISK_VALUE;
        }
        if ( mcidp->cell != 0 ) {
            fprintf(stderr,
              "%s vol %d page %ld cell %d: bad chainMCId.cell (%d) for vdi=0\n",
              BS_BFTAG_RSVD(bfp->fileTag)  ? "RBMT" : "BMT",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell, mcidp->page);
            return BAD_DISK_VALUE;
        }
        return OK;
    }

    if ( !(bfp->type & FLE) ) {
        if ( test_vdi(bfp, NULL, bsXtntRp->chainMCId.volume) != OK ) {
            fprintf(stderr,
              "%s vol %d page %ld cell %d: bad nextVdIndex %d\n",
              BS_BFTAG_RSVD(bfp->fileTag) ? "RBMT" : "BMT",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell, bsXtntRp->chainMCId.volume);
            return BAD_DISK_VALUE;
        }

        if ( test_mcid(bfp, "get_chain_mcell", *mcidp) != OK ) {
            fprintf(stderr,
             "%s vol %d page %ld cell %d: bad chainMCId.\n",
              BS_BFTAG_RSVD(bfp->fileTag) ? "RBMT" : "BMT",
              bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell);
            return BAD_DISK_VALUE;
        }
    } else {
        if ( bsXtntRp->chainMCId.volume != bfp->pmcell.volume ) {
            fprintf(stderr,
    "%s vol %d page %ld cell %d: chainVdIndex (%d) indicates a different file. ",
                  BS_BFTAG_RSVD(bfp->fileTag) ? "RBMT" : "BMT",
                  bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell, bsXtntRp->chainMCId.volume);
        }
    }
 
    return OK;
}

/******************************************************************************/
/* Given a mcell pointer (vdi, pg, cn) read the cell and get the next cell
** pointer. Test the next cell pointer.
**
** Input: bfp->type - saved file or disk
**        bfp->fileTag - for disk, select RBMT or BMT for mcells
**        bfp->dmn->vols[*vdi]->(r)bmt - pointers to BMT file
**        vdip, mcidp - point to current mcell
** Output: bfp->pgBuf - filled with next mcell page
**         vdip, mcidp - point to next mcell
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if cell has bad contents
*/
/* TODO: why read next page here? */
int
get_next_mcell(bitFileT *bfp, bfMCIdT *mcidp)
{
    int ret;
    bsMCT *cellp;
    bfMCIdT saveMcell;
    bitFileT *mdBfp;

    if ( bfp->type & FLE ) {
        mdBfp = bfp;
    } else {
        assert(mcidp->volume != 0);
        assert(mcidp->volume < BS_MAX_VDI);
        assert(mcidp->cell < BSPG_CELLS);
        assert(bfp->dmn);
        assert(bfp->dmn->vols[mcidp->volume]);
        assert(bfp->fileTag.tag_num != 0);

        if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
            mdBfp = bfp->dmn->vols[mcidp->volume]->rbmt;
        } else {
            mdBfp = bfp->dmn->vols[mcidp->volume]->bmt;
        }
        assert(mdBfp);
        assert(mcidp->page < mdBfp->pages);
    }

    ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(*mcidp));
    if ( ret != OK ) {
        return ret;
    }

    cellp = &((bsMPgT*)mdBfp->pgBuf)->bsMCA[mcidp->cell];

#ifdef NOTYET        /* bmt doesn't set tag. tag = 0 */
    if ( cellp->tag.tag_num != bfp->fileTag.tag_num ) {
        fprintf(stderr, "Vdi %d, page %ld, mcell %d: bad tag %ld, expected %ld\n",
          mcidp->volume, mcidp->page, mcidp->cell,
          (int64_t)cellp->tag.tag_num, (int64_t)bfp->fileTag.tag_num);
        return BAD_DISK_VALUE;
    }
#endif

    /* test next vdi */
    if ( cellp->mcNextMCId.volume >= BS_MAX_VDI ) {
        fprintf(stderr, "Vdi %d, page %ld, mcell %d: bad nextVdIndex %d\n",
          mcidp->volume, mcidp->page, mcidp->cell, cellp->mcNextMCId.volume);
        return BAD_DISK_VALUE;
    }

    if ( !(bfp->type & FLE) ) {
        /* test next page */
        if ( cellp->mcNextMCId.page >= mdBfp->pages ) {
            fprintf(stderr, "Vdi %d, page %ld, mcell %d: bad nextMCId.page %ld\n",
             mcidp->volume, mcidp->page, mcidp->cell, cellp->mcNextMCId.page);
            return BAD_DISK_VALUE;
        }
    }

    /* test next mcell number */
    if ( cellp->mcNextMCId.cell >= BSPG_CELLS ) {
        fprintf(stderr, "Vdi %d, page %ld, mcell %d: bad nextMCId.cell %d\n",
          mcidp->volume, mcidp->page, mcidp->cell, cellp->mcNextMCId.cell);
        return BAD_DISK_VALUE;
    }

    /* If the next mcell is in another page, read in that page */
    if ( cellp->mcNextMCId.volume && (cellp->mcNextMCId.page != mcidp->page) ) {

        /* save these values; cellp points to new page after read_page */
        saveMcell = cellp->mcNextMCId;

        ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(cellp->mcNextMCId));
        if ( ret != OK ) {
            return ret;
        } else {
            *mcidp = saveMcell;
            return OK;
        }
    }

    *mcidp = cellp->mcNextMCId;

    return OK;
}

/*****************************************************************************/
/* Given the tag file for the fileset and a directory in the fileset and
** a path conponent (ie directory entry), find the tag of the component.
** On by one, read the directory pages and for each dir entry, test the
** component name. If found, return the tag of the component in bfp->fileTag.
**
** Input: dirbfp is set up for the dir to search
**        component - file or next dir to find in dirbfp
** Output: dirbfp->fileTag -  component tag
** Errors: diagnostic output to stderr for all errors
** Return: OK if component found
**         TAG3 if found conponent is tag 3 (.tags)
**         ENOENT if component does not exist
**         BAD_DISK_VALUE for mangled directory entries
*/
static int
search_dir(bitFileT *tagBfp, bitFileT *dirbfp, char *component)
{
    vdIndexT v;
    uint32_t p,c;
    bs_meta_page_t i;
    int ret;
    bfTagT tag;

    /* read file set root dir looking for file name */
    for ( i = 0; i < dirbfp->pages; i++ ) {
        fs_dir_entry  *dir_ent_p;

        /* read dir page */
        ret = read_page(dirbfp, (bs_meta_page_t)i);
        if ( ret != OK ) {
            fprintf(stderr,
              "search_dir: read directory (tag %ld set tag %ld) page %ld failed\n",
              dirbfp->fileTag.tag_num, dirbfp->setTag.tag_num, i);
            return ret;
        }
        dir_ent_p = (fs_dir_entry*)(dirbfp->pgBuf);

        /* scan dir page looking for file  */
        while ( (char*)dir_ent_p < &dirbfp->pgBuf[ADVFS_METADATA_PGSZ] )
        {
            if ( dir_ent_p->fs_dir_namecount >= FS_DIR_MAX_NAME )
            {
                printf("dir err\n");
                return BAD_DISK_VALUE;
            }
            dir_ent_p->fs_dir_name_string[dir_ent_p->fs_dir_namecount] = '\0';

            /* look for file */

            if ( strcmp(component, dir_ent_p->fs_dir_name_string) == 0 ) {
                if ( dir_ent_p->fs_dir_bs_tag_num == 0 ) 
                {
                     fprintf(stderr,
          "fs_dir_bs_tag_num (%ld) doesn't match GETTAG(dir_ent_p)->tag_num (%ld)\n",
                       dir_ent_p->fs_dir_bs_tag_num,
                       dir_ent_p->fs_dir_bs_tag_num);
                }

                GETTAG(tag, dir_ent_p);
                ret = get_prim_from_tagdir(tagBfp, &tag, dirbfp);
                if ( ret != OK ) {
                    return ret;
                }

                /* if tag == 3 => .tags */
                /* next component is a tag */
                if ( dir_ent_p->fs_dir_bs_tag_num == TAGS_FILE_TAG) {
                    return TAG3;
                }

                dirbfp->fileTag.tag_num = dir_ent_p->fs_dir_bs_tag_num;
                return OK;
            }
            dir_ent_p =
       (fs_dir_entry*)((char*)dir_ent_p + dir_ent_p->fs_dir_size);
        }
    }

    return ENOENT;
}

/*****************************************************************************/
/* Reads primary mcell (bfp->pmcell.volume, bfp->pmcell.page,
** bfp->pmcell.cell) and looks for the record type. If not found, read
** the next mcell and look there.
**
** Input: bfp->dmn - defines the domain and BMT extents
**        bfp->fileTag - negative tag uses RBMT
**        bfp->pmcell.volume, bfp->pmcell.page, bfp->pmcell.cell - primary mcell
**        recpa - address of pointer to found record
** Output: recpa - pointer to the found record. NULL is not found
** Errors: diagnostic output to stderr for all errors
** Return: OK if record found
**         BAD_DISK_VALUE for bad mcell contents
**         ENOENT if record is not found
**         errors from find_bmtr
*/
/* FIX. pass in mcid to id bad cell in fprintf */
int
find_rec(bitFileT *bfp, int type, char **recpa)
{
    bitFileT *mdBfp;
    bsMCT *mcp;
    bfTagT setTag;
    bfTagT fileTag;
    int first = 1;
    int ret;
    bfMCIdT mcid;

    assert(bfp != NULL);
    assert(bfp->fileTag.tag_num != 0);
    assert(bfp->dmn);

    assert(bfp->dmn->vols[bfp->pmcell.volume] != NULL);

    mcid = bfp->pmcell;
    *recpa = NULL;

    if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
        /* reserverved file: look in rbmt for mcells */
        mdBfp = bfp->dmn->vols[mcid.volume]->rbmt;
    } else {
        /* non reserved file: look in bmt for mcells */
        mdBfp = bfp->dmn->vols[mcid.volume]->bmt;
    }
    assert(mdBfp);

    while ( mcid.volume ) {
        ret = read_page(mdBfp, (bs_meta_page_t)FETCH_MC_PAGE(mcid));
        if ( ret != OK ) {
            fprintf(stderr, "read_page fail\n");
            return ret;
        }

        mcp = &((bsMPgT*)mdBfp->pgBuf)->bsMCA[mcid.cell];

        if ( first ) {
            setTag = mcp->mcBfSetTag;
            if ( bfp->setTag.tag_num != setTag.tag_num ) {
                if ( bfp->setp == NULL || bfp->setp->origSetp == NULL ||
                     bfp->setp->origSetp->fileTag.tag_num != setTag.tag_num )
                {
                    fprintf(stderr, "wrong set tag\n");
                    return BAD_DISK_VALUE;
                }
            }

            fileTag = mcp->mcTag;
            if ( bfp->fileTag.tag_num != fileTag.tag_num ) {
                fprintf(stderr, "wrong tag\n");
                return BAD_DISK_VALUE;
            }

            first = 0;
        } else {
            if ( !BS_BFTAG_EQL(mcp->mcBfSetTag, setTag) ) {
                fprintf(stderr, "wrong set tag\n");
                return BAD_DISK_VALUE;
            }

            if ( !BS_BFTAG_EQL(mcp->mcTag, fileTag) ) {
                fprintf(stderr, "wrong tag\n");
                return BAD_DISK_VALUE;
            }
        }

        ret = find_bmtr(mcp, type, recpa);
        if ( ret == OK ) {
            return OK;
        }
        if ( ret != ENOENT ) {
            return ret;
        }

        ret = get_next_mcell(bfp, &mcid);
        if ( ret != OK ) {
            return ret;
        }
    }

    return ENOENT;
}

/*****************************************************************************/
/*
** Find record type in given mcell.
**
** Input: bsMCp - pointer to mcell in memory
**        rtype - record type for which to search
**        recpa - address of pointer to found record
** Output: recpa - pointer to found record. NULL is record not found
** Errors: diagnostic output to stderr for all errors
** Return: OK if record found
**         BAD_DISK_VALUE for bad mcell contents
**         ENOENT if record was not found
*/
int
find_bmtr(bsMCT *bsMCp, int rtype, char **recpa)
{
    bsMRT *rp;
    int rn = 0;

    *recpa = NULL;

    /* scan cell for desired record type.  all cells contain a */
    /* NIL type record as a "stopper".  */

    rp = (bsMRT *)bsMCp->bsMR0;

    while ( rp->type != BSR_NIL ) {
        if ( (char *)rp + rp->bCnt > &bsMCp->bsMR0[BSC_R_SZ] ) {
            printf("Bad mcell record %d: bCnt too large %d\n", rn, rp->bCnt);
            return BAD_DISK_VALUE;
        }

        /* This check prevents infinite loop on a bogus record */
        if ( rp->bCnt == 0 ) {
            printf("Bad mcell record %d: bCnt == 0\n", rn);
            return BAD_DISK_VALUE;
        }

        if ( rp->type == rtype ) {
            *recpa = (char *)rp + sizeof(bsMRT);
            return OK;
        }

        rp = (bsMRT *)((char *)rp + roundup(rp->bCnt, sizeof(uint64_t)));
    }

    return ENOENT;
}

/*****************************************************************************/
/* Change the number in the character string to a long.
** Accept octal, hex or decimal. If the string has any bad numbers, return
** an error without outputing a number.
**
** Input: character string cp with octal, decimal or hexadecimal number.
** Output: long at "num" iff conversion is OK. 
** Errors: status return only. No diagnostic printf
** Return: OK if converstion went well
**         EINVAL for badly formed number
*/
int
getnum(char *cp, int64_t *num)
{
    char *endptr;
    int base = 10;
    int64_t number;

    if (cp == NULL) {
	return (EINVAL);
    }

    if ( cp[0] == '0' ) {
        if ( cp[1] == 'x' || cp[1] == 'X' ) {
            base = 16;
        } else {
            base = 8;
        }
    }

    number = strtol(cp, &endptr, base);

    if ( endptr != cp + strlen(cp) ) {
        return EINVAL;
    }

    *num = number;
    return OK;
}

/*****************************************************************************/
#define PRINT_NAME(bfp) {                                               \
    if ( BS_BFTAG_RSVD(bfp->fileTag) ) {                                           \
        switch ( -bfp->fileTag.tag_num % BFM_RSVD_TAGS ) {                      \
            case BFM_RBMT:                                              \
                printf("RBMT ");                                        \
                break;                                                  \
            case BFM_SBM:                                               \
                printf("SBM ");                                         \
                break;                                                  \
            case BFM_BFSDIR:                                            \
                printf("root TAG ");                                    \
                break;                                                  \
            case BFM_FTXLOG:                                            \
                printf("LOG ");                                         \
                break;                                                  \
            case BFM_BMT:                                               \
                printf("BMT ");                                         \
                break;                                                  \
            default:                                                    \
                break;                                                  \
        }                                                               \
    } else if (BS_BFTAG_REG(bfp->fileTag) && BS_BFTAG_EQL(bfp->setTag, staticRootTagDirTag)) {      \
        if ( bfp->setName[0] != '\0' )                                  \
            printf("\"%s\" TAG ", bfp->setName);                        \
        else                                                            \
            printf("set tag %ld TAG ", bfp->setTag.tag_num);                     \
    }                                                                   \
}

/*****************************************************************************/
/* Prints a header for the page of data displayed.
** Prints a one line description of the source of the formated data.
** For a domain it prints the domain name, the volume and the block number.
** Then it prints the bitfile name (BMT, SBM, etc) and the bitfile page number.
** For a volume it prints the volume name, block, bitfile name and page.
** For a saved file it prints the file name and the page number.
** If no page number is specified (block on disk is specified) then just
** print the given block number.
** A double line "====" precedes the description and a single line follows it.
**
** DOMAIN "---" VDI # ("<vol>") LBN #  <file> PAGE #
** VOLUME "---" (VDI #) LBN #  <file> PAGE #
** FILE "---"  PAGE #
**
** <file> = BMT, SBM, LOG, ROOT TAG, "file_set" TAG, "file"
**
** DOMAIN "---"  VDI # ("<vol>")  LBN #
** VOLUME "---" (VDI #)  LBN #
** VOLUME "---"  LBN #
**
** SAVE SET "---"  VDI #  SAVE FILE "---"  PAGE #
**
** Input: bfp->type = DMN, VOL or FLE
**        bfp->fileTag - selects file name
**        bfp->pgVol - if DMN or VOL then VDI to print
**        bfp->dmn->vols[bfp->pgVol]->volName - if DMN or VOL then volume name
**        bfp->pgLbn - if != -1 then LBN to print
**        bfp->pgNum - if != -1 then file page number to print
** Output: prints volume page header
** Errors: no errors
** Return: none
*/
void
print_header(bitFileT *bfp)
{
    char line[80];
    int  line_len = 0;
    char *volume;
    line[79] = '\0';        /* in case sprintf fails */

    assert(bfp);
    assert(bfp->type & (DMN | VOL | FLE));

    printf(DOUBLE_LINE);
    if ( bfp->type & DMN ) {
        assert(bfp->dmn);
        printf("DOMAIN \"%s\"  ", bfp->dmn->dmnName);
        if ( bfp->pgVol ) {
            assert(bfp->dmn->vols[bfp->pgVol]);
            volume = bfp->dmn->vols[bfp->pgVol]->volName;
            printf("VDI %d (%s)  ", bfp->pgVol, volume);
            if ( bfp->pgLbn != LBN_BLK_TERM )
                printf("lbn %ld   ", bfp->pgLbn);
            if ( bfp->pgNum != BMT_PG_TERM ) {
                PRINT_NAME(bfp);
                printf("page %ld", bfp->pgNum);
            }
        }
    } else if ( bfp->type & VOL ) {
        assert(bfp->dmn);
        volume = bfp->dmn->vols[bfp->pgVol]->volName;
        printf("VOLUME \"%s\" ", volume);
        if ( bfp->pgVol ) {
            printf("(VDI %d) ", bfp->pgVol);
        }

        if ( bfp->pgLbn != LBN_BLK_TERM) {
            printf(" lbn %ld   ", bfp->pgLbn);
        }

        if ( bfp->pgNum != BMT_PG_TERM ) {
            PRINT_NAME(bfp);
            printf("page %ld", bfp->pgNum);
        }
    } else if ( bfp->type & FLE ) {
        printf("FILE \"%s\"   ", bfp->fileName);
        if ( bfp->pgNum != BMT_PG_TERM ) {
            PRINT_NAME(bfp);
            printf("page %ld", bfp->pgNum);
        }
    }

    printf("\n");
    printf(SINGLE_LINE);
}

/*****************************************************************************/
/* Prints a header for the FOB of data displayed.
** Prints a one line description of the source of the formated data.
** For a filesystem it prints the FS name, the volume and the block number.
** Then it prints the bitfile name (BMT, SBM, etc) and the bitfile FOB number.
** For a volume it prints the volume name, block, bitfile name and FOB.
** For a saved file it prints the file name and the page number.
** If no FOB number is specified (block on disk is specified) then just
** print the given block number.
** A double line "====" precedes the description and a single line follows it.
**
** DOMAIN "---" VDI # ("<vol>") LBN #  <file> PAGE #
** VOLUME "---" (VDI #) LBN #  <file> PAGE #
** FILE "---"  PAGE #
**
** <file> = BMT, SBM, LOG, ROOT TAG, "file_set" TAG, "file"
**
** DOMAIN "---"  VDI # ("<vol>")  LBN #
** VOLUME "---" (VDI #)  LBN #
** VOLUME "---"  LBN #
**
** SAVE SET "---"  VDI #  SAVE FILE "---"  PAGE #
**
** Input: bfp->type = DMN, VOL or FLE
**        bfp->fileTag - selects file name
**        bfp->pgVol - if DMN or VOL then VDI to print
**        bfp->dmn->vols[bfp->pgVol]->volName - if DMN or VOL then volume name
**        bfp->pgLbn - if != -1 then LBN to print
**        bfp->pgNum - if != -1 then file page number to print
** Output: prints volume FOB header
** Errors: no errors
** Return: none
*/
void
print_fob_header(bitFileT *bfp, bf_fob_t fobnum)
{
    char line[80];
    int  line_len = 0;
    char *volume;
    line[79] = '\0';        /* in case sprintf fails */

    assert(bfp);
    assert(bfp->type & (DMN | VOL | FLE));

    printf(DOUBLE_LINE);
    if ( bfp->type & DMN ) {
        assert(bfp->dmn);
        printf("DOMAIN \"%s\"  ", bfp->dmn->dmnName);
        if ( bfp->pgVol ) {
            assert(bfp->dmn->vols[bfp->pgVol]);
            volume = bfp->dmn->vols[bfp->pgVol]->volName;
            printf("VDI %d (%s)  ", bfp->pgVol, volume);
            if ( bfp->pgLbn != LBN_BLK_TERM )
                printf("lbn %ld   ", bfp->pgLbn);
            if ( bfp->pgNum != BMT_PG_TERM ) {
                PRINT_NAME(bfp);
                printf("fob %ld", fobnum);
            }
        }
    } else if ( bfp->type & VOL ) {
        assert(bfp->dmn);
        volume = bfp->dmn->vols[bfp->pgVol]->volName;
        printf("VOLUME \"%s\" ", volume);
        if ( bfp->pgVol ) {
            printf("(VDI %d) ", bfp->pgVol);
        }

        if ( bfp->pgLbn != LBN_BLK_TERM ) {
            printf(" lbn %ld   ", bfp->pgLbn);
        }

        if ( bfp->pgNum != BMT_PG_TERM ) {
            PRINT_NAME(bfp);
	    printf("fob %ld", fobnum);
        }
    } else if ( bfp->type & FLE ) {
        printf("FILE \"%s\"   ", bfp->fileName);
        if ( bfp->pgNum != BMT_PG_TERM ) {
            PRINT_NAME(bfp);
	    printf("fob %ld", fobnum);
        }
    }

    printf("\n");
    printf(SINGLE_LINE);
}

/*****************************************************************************/
/* Prints groups of 4 bytes as an int until < 4 bytes left to print,
** then prints bytes, all in hex.
**
** Input: pdata - data to be displayed. No alignment required
**        size - number of bytes to displayed
** Output: print to stdout
** Errors: no errors
** Return: none
*/

#define INTSZ 4   /* sizeof(uint32_t) */
#define BPL 16    /* bytes of data per line */

void
print_unknown( char *pdata, int size )
{
    int lines = size / BPL;
    int ln;
    u_char *line;
    u_char *cp;
    uint32_t x;

    for ( ln = 0; ln < lines; ln++ )
    {
        line = (u_char *)&pdata[ln * BPL];
        printf("        ");
        for ( cp = line; cp < (u_char*)&pdata[ln * BPL + BPL]; cp += INTSZ )
        {
            bcopy(cp, (char *)&x, sizeof(uint32_t));
            printf("%08x ", x);
        }
        printf("\n");
    }

    line = (u_char*)&pdata[ln * BPL];
    if ( line != (u_char*)&pdata[size] )
    {
        printf("        ");
        for ( cp = line; cp < (u_char*)&pdata[size - (INTSZ-1)]; cp += INTSZ )
        {
            bcopy(cp, (char *)&x, sizeof(uint32_t));
            printf("%08x ", x);
        }

        if ( cp != (u_char*)&pdata[size] )
        {
            for ( ; cp < (u_char*)&pdata[size]; cp++ )
            {
                printf("%02x ", *cp);
            }
        }
        printf("\n");
    }
}

/*****************************************************************************/
/* Test whether "page" exists in "bfp".
**
** Input: bfp->type 
**        bfp->pages - if bfp->type == FLE
**        bfp->xmap - if bfp->type == VOL or DMN
** Output: none
** Errors: return status only. no errors
** Return: TRUE if page is mapped otherwise FALSE
*/
int
page_mapped(bitFileT *bfp, int page)
{
    bf_fob_t fob;
    int low;
    int high;
    int idx;
    int lastidx;

    if ( bfp->type & FLE )
    {
        if ( page < bfp->pages )
            return TRUE;
        else
            return FALSE;
    }

    fob = PAGE2FOB(page);

    if ( bfp->xmap.cnt == 0 )
        return FALSE;

    low = 0;
    high = bfp->xmap.cnt;
    idx = high / 2;

    while ( (idx > 0) &&
            ((fob < bfp->xmap.xtnt[idx].fob) ||
             ((idx < bfp->xmap.cnt - 1) &&
              (fob >= bfp->xmap.xtnt[idx + 1].fob))) )
    {

        lastidx = idx;
        if ( fob < bfp->xmap.xtnt[idx].fob ) {
            high = idx;
            idx = low + (idx - low) / 2;
        } else {
            low = idx;
            idx = idx + (high - idx) / 2;
        }

        assert(idx != lastidx);
    }
    assert(idx >= 0);
    assert(idx < bfp->xmap.cnt);

    if ( bfp->xmap.xtnt[idx].blk < 0 ) {
        /* page not mapped */
        return FALSE;
    }

    return TRUE;
}

/*****************************************************************************/
/* Read a page from the bitFile into the bitFile page buffer.
** Maintain a simple one page cache. If pgVol & pgLbn as calculated from page
** and xmap are aready set in bfp then pgBuf already contains the page.
**
** input: bfp->xmap hold extents for file
**        bfp->type determines whether to use xtnts or fd
**        bfp->fd - read from this fd if type is FLE or VOL
**        bfp->pgVol, pgNum, pgLbn - if same as requested, don't read again
**        page is bitFile page to read
** output: bfp->pgBuf - a page of data is loade here
**         bfp->pgBuf - if NULL, malloc a page buffer
**         bfp->pgVol, pgNum, pgLbn - set to reflect where pgBuf read from
** Errors: diagnostic output to stderr for all errors
** return OK - if read succeeds
**        BAD_DISK_VALUE - block is not mapped or volume does not exist
**        ENOMEM - malloc failed
*/
/* TODO: use read_vol_at_blk. */
int
read_page(bitFileT *bfp, bs_meta_page_t page)
{
    uint64_t lbn;
    bf_fob_t fob;
    int vol = 0;
    int fd, low = 0, high = bfp->xmap.cnt, idx = high / 2;
    int lastidx;
    ssize_t read_ret;
    char errstr[80];

    errstr[0] = '\0';       /* just in case sprintf fails */

    if ( page >= bfp->pages ) {
        fprintf(stderr,
		"read_page: Requested page (%ld) is beyond last page (%ld).\n",
		page, bfp->pages - 1);
    }

    fob = PAGE2FOB(page);

    if ( bfp->type & FLE ) {
        /* working with save file;  page # is real offset into the file */
        if ( bfp->pgBuf && page * ADVFS_METADATA_PGSZ_IN_BLKS == bfp->pgLbn ) {
            /* simple cache. if lbn matches, just return. */
            return OK;
        }

        fd = bfp->fd;
        if ( lseek(fd, (off_t)page * ADVFS_METADATA_PGSZ, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "read_page: lseek to page %ld failed", page);
            perror(errstr);
            return BAD_DISK_VALUE;
        }
    } else {
        assert ( bfp->xmap.cnt != 0 );

	/* find the extent whose range includes the target page */

        while ( (idx > 0) &&
                ((fob < bfp->xmap.xtnt[idx].fob) ||
                 ((idx < bfp->xmap.cnt - 1) &&
                  (fob >= bfp->xmap.xtnt[idx + 1].fob))) ) {

            lastidx = idx;
            if (fob < bfp->xmap.xtnt[idx].fob) {
                high = idx;
                idx = low + (idx - low) / 2;
            } else {
                low = idx;
                idx = idx + (high - idx) / 2;
            }

            /* is this a test that the page is off the end? return err */
            assert(idx != lastidx);
        }
        assert(idx >= 0);

        if (bfp->xmap.xtnt[idx].blk < 0) {
            /* page not mapped */
            return BAD_DISK_VALUE;
        }

        if ( !(bfp->type & FLE) ) {
            vol = bfp->xmap.xtnt[idx].vol;
            if ( bfp->dmn->vols[vol] == NULL ) {
                fprintf(stderr,
                  "Bad volume (%d) for extent %d of set tag %ld, file tag %ld\n",
                  vol, idx, bfp->setTag.tag_num, bfp->fileTag.tag_num);
                fprintf(stderr,
                  "Fob %ld (page %ld) defined in extent %d is on volume index %d.\n",
                   fob, page, idx, bfp->xmap.xtnt[idx].vol);
                fprintf(stderr, "Domain \"%s\" doesn't have this volume.\n",
                   bfp->dmn->dmnName);
                return BAD_DISK_VALUE;
            }
            fd = bfp->dmn->vols[vol]->fd;
        } else {
            /* this is a BMT. It is self contained on this volume */
            fd = bfp->fd;
        }

	/* compute the block number where the target page starts */

        lbn = bfp->xmap.xtnt[idx].blk +
	       ((fob - bfp->xmap.xtnt[idx].fob) / ADVFS_FOBS_PER_DEV_BSIZE);
        if ( bfp->pgBuf && lbn == bfp->pgLbn && bfp->pgVol == vol ) {
            /* simple cache. if lbn matches, just return. */
            return OK;
        }

        bfp->pgVol = vol;
        bfp->pgLbn = lbn;
        if ( lseek(fd, lbn * DEV_BSIZE, SEEK_SET) == (off_t)-1 ) {
            sprintf(errstr, "read_page: lseek to block %ld failed", lbn);
            perror(errstr);
            return BAD_DISK_VALUE;
        }
    }

    if ( bfp->pgBuf == NULL ) {
        bfp->pgBuf = malloc(ADVFS_METADATA_PGSZ);
        if ( bfp->pgBuf == NULL ) {
            perror("read_page: malloc failed");
            return ENOMEM;
        }
    }

    read_ret = read(fd, bfp->pgBuf, (size_t)ADVFS_METADATA_PGSZ);
    if ( read_ret != (ssize_t)ADVFS_METADATA_PGSZ ) {
        if ( read_ret == (ssize_t)-1 ) {
            sprintf(errstr, "read_page: Read page %ld failed.", page);
            perror(errstr);
        } else {
            fprintf(stderr,
              "read_page: Truncated read on page %ld, expected %d, got %ld\n",
              page, ADVFS_METADATA_PGSZ, read_ret);
        }
        return BAD_DISK_VALUE;
    }

    bfp->pgNum = page;

    return OK;
}

/*****************************************************************************/
/* Test the BMT page.
** Test nextfreeMCId, nextFreePg, freeMcellCnt, pageId
** Example message:
** open_vol: Corrupted RBMT page 5 (lbn 564) on volume 2 (/dev/disk/dsk8c).
** Bad pageId (23).
**
** Input:  bfp->pgBuf - page to check
**         bfp->pgNum - page number
**         bfp->pages - number of pages in the (R)BMT if known
** Output: none
** Errors: diagnostic output to stderr for all errors
** Return: OK if all went well
**         BAD_DISK_VALUE if a value is bogus
*/
static int
test_bmt_page(bitFileT *bfp, char *func_name, int rbmtflg)
{
    bsMPgT *bsMPgp;
    int ret = OK;

    /* if not FLE then pgLbn set up */
    assert(bfp->type & FLE || bfp->pgLbn != LBN_BLK_TERM);
    /* if not FLE the dmn, vols & volName filled in */
    assert(bfp->type & FLE || bfp->dmn);
    assert(bfp->type & FLE || bfp->dmn->vols[bfp->pgVol]);
    assert(bfp->type & FLE || bfp->dmn->vols[bfp->pgVol]->volName);

    bsMPgp = (bsMPgT*)bfp->pgBuf;
    if ( bsMPgp->bmtPageId != bfp->pgNum ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Corrupted %s page %ld ",
              func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
            if ( !(bfp->type & FLE) ) {
                assert(bfp->pgLbn != LBN_BLK_TERM);
                fprintf(stderr, "(lbn %ld) ", bfp->pgLbn);
                fprintf(stderr, "on volume %d (%s).\n",
                  bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
            } else {
                fprintf(stderr, ".\n");
            }
            fprintf(stderr, "Bad pageId (%ld).\n", bsMPgp->bmtPageId);
        }
        ret = BAD_DISK_VALUE;
    }

    if (rbmtflg && bsMPgp->bmtODSVersion.odv_major > BFD_ODS_LAST_VERSION_MAJOR)
    {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Corrupted %s page %ld ",
              func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
            if ( !(bfp->type & FLE) ) {
                assert(bfp->pgLbn != LBN_BLK_TERM);
                fprintf(stderr, "(lbn %ld) ", bfp->pgLbn);
                fprintf(stderr, "on volume %d (%s).\n",
                  bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
            } else {
                fprintf(stderr, ".\n");
            }
            fprintf(stderr, "Bad ODSVersion (%d.%d).\n", bsMPgp->bmtODSVersion.odv_major, bsMPgp->bmtODSVersion.odv_minor);
        }
        ret = BAD_DISK_VALUE;
    }

    if ( bsMPgp->bmtFreeMcellCnt >= BSPG_CELLS ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Corrupted %s page %ld ",
              func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
            if ( !(bfp->type & FLE) ) {
                assert(bfp->pgLbn != LBN_BLK_TERM);
                fprintf(stderr, "(lbn %ld) ", bfp->pgLbn);
                fprintf(stderr, "on volume %d (%s).\n",
                  bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
            } else {
                fprintf(stderr, ".\n");
            }
            fprintf(stderr, "Bad freeMcellCnt (%d).\n", bsMPgp->bmtFreeMcellCnt);
        }
        ret = BAD_DISK_VALUE;
    } 

    if ( bsMPgp->bmtFreeMcellCnt > 0 ) {
        /* BMT freelist terminate with nextFreePg = BMT_PG_TERM (-1) */
        /* RBMT nextFreePg always = 0 */
        if ( bfp->pages &&
             ( (!rbmtflg && bsMPgp->bmtNextFreePg != BMT_PG_TERM) ||
               (rbmtflg && bsMPgp->bmtNextFreePg != 0) ) &&
             bsMPgp->bmtNextFreePg >= bfp->pages )
        {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %ld ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != LBN_BLK_TERM);
                    fprintf(stderr, "(lbn %ld) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad nextFreePg (%ld).\n", bsMPgp->bmtNextFreePg);
            }
            ret = BAD_DISK_VALUE;
        }

        if ( bsMPgp->bmtNextFreeMcell >= BSPG_CELLS ) {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %ld ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != LBN_BLK_TERM);
                    fprintf(stderr, "(lbn %ld) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad bmtNextFreeMcell (%d).\n",
                  bsMPgp->bmtNextFreeMcell);
            }
            ret = BAD_DISK_VALUE;
        }
    } else {  /* no free mcells on this page */
        if ( (rbmtflg && bsMPgp->bmtNextFreePg != 0) ||
             (!rbmtflg && bsMPgp->bmtNextFreePg != BMT_PG_TERM) )
        {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %ld ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != LBN_BLK_TERM);
                    fprintf(stderr, "(lbn %ld) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr,
                  "Bad nextFreePg (%ld) on page with no free mcells.\n",
                        bsMPgp->bmtNextFreePg);
            }
            ret = BAD_DISK_VALUE;
        }

        if ( bsMPgp->bmtNextFreeMcell != 0 ) {
            if ( func_name != NULL ) {
                fprintf(stderr, "%s: Corrupted %s page %ld ",
                  func_name, rbmtflg ? "RBMT" : "BMT", bfp->pgNum);
                if ( !(bfp->type & FLE) ) {
                    assert(bfp->pgLbn != LBN_BLK_TERM);
                    fprintf(stderr, "(lbn %ld) ", bfp->pgLbn);
                    fprintf(stderr, "on volume %d (%s).\n",
                      bfp->pgVol, bfp->dmn->vols[bfp->pgVol]->volName);
                } else {
                    fprintf(stderr, ".\n");
                }
                fprintf(stderr, "Bad bmtNextFreeMcell (%d).\n",
                  bsMPgp->bmtNextFreeMcell);
            }
            ret = BAD_DISK_VALUE;
        }
    }

    return ret;
}

/*****************************************************************************/
/* Test fields in an mcell.
** tag and setTag in mcell must match fields bitFileT.
**
** Input:  bfp->fileTag, bfp->setTag
**         bsMCp - pointer to mcell contents
**         func_name - routine that called. If NULL then no fprintf.
**         vdi, bfMCId - used to ID mcell in diag printf
** Output: none
** Errors: diagnostic output to stderr for all errors if func_name not NULL
** Return: OK if all went well
**         BAD_DISK_VALUE if the mcell has errors
*/
static int
test_mcell(bitFileT *bfp, char *func_name, bfMCIdT bfMCId, bsMCT *bsMCp)
{
    int ret = OK;

    if ( !BS_BFTAG_EQL(bsMCp->mcTag,bfp->fileTag) ) {
        if ( func_name != NULL ) {
            fprintf(stderr,
              "%s: Bad tag (%ld.%d) in mcell %d %ld %d, expected %ld.%d\n",
              func_name, bsMCp->mcTag.tag_num, bsMCp->mcTag.tag_seq,
              bfMCId.volume, bfMCId.page, bfMCId.cell,bfp->fileTag.tag_num,
              bfp->fileTag.tag_seq);
        }
        ret = BAD_DISK_VALUE;
    }

    if ( !BS_BFTAG_EQL(bsMCp->mcBfSetTag,bfp->setTag) &&
         (bfp->setp == NULL ||
          bfp->setp->origSetp &&
          !BS_BFTAG_EQL(bsMCp->mcBfSetTag,bfp->setp->origSetp->fileTag)) )
    {
        if ( func_name != NULL ) {
            fprintf(stderr,
              "%s: Bad set tag (%ld.%d) in mcell %d o%ld %d, expected %ld.%d\n",
              func_name, bsMCp->mcBfSetTag.tag_num, bsMCp->mcBfSetTag.tag_seq, 
              bfMCId.volume, bfMCId.page, bfMCId.cell, bfp->setTag.tag_num,
              bfp->setTag.tag_seq);
        }
        ret = BAD_DISK_VALUE;
    }

    return ret;
}

/*****************************************************************************/
/* Test mcell address. page and cell must be within range for BMT.
**
** Input:  bfMCId.page, bfMCId.cell - tested
**         vdi - volume mcell lives on
**         bfp->dmn->vols[vdi]->[r]bmt - test page agaisnt this BMT
**         func_name - used in diag printf. if NULL, no printf
** Output: none
** Errors: diagnostic output to stderr for all errors if func_name not NULL
** Return: OK if all went well
**         BAD_DISK_VALUE if the mcell id is out of range
*/
static int
test_mcid(bitFileT *bfp, char *func_name, bfMCIdT bfMCId)
{
    bitFileT *mdBfp;
    int ret = OK;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[bfMCId.volume]);

    if ( BS_BFTAG_RSVD(bfp->fileTag) ) {
        mdBfp = bfp->dmn->vols[bfMCId.volume]->rbmt;
    } else {
        mdBfp = bfp->dmn->vols[bfMCId.volume]->bmt;
    }
    assert(mdBfp);

    if ( bfMCId.page >= mdBfp->pages ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Bad mcell ID page (%ld).  ",
              func_name, bfMCId.page);
            fprintf(stderr, "%ld %s in %s.\n", mdBfp->pages,
              mdBfp->pages == 1 ? "page" : "pages",
              BS_BFTAG_RSVD(bfp->fileTag) ? "RBMT" : "BMT");
        }
        ret = BAD_DISK_VALUE;
    }

    if ( bfMCId.cell >= BSPG_CELLS ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Bad mcell ID cell (%d).\n",
              func_name, bfMCId.cell);
        }
        ret = BAD_DISK_VALUE;
    }

    return ret;
}

/*****************************************************************************/
/* Test the specified volume index.
**
** Input:  bfp->dmn->vols[] - array of valid volumes in domain
**         vdi - volume index to test
**         func_name - used in diag printf. no printf if NULL
** Output: none
** Errors: diagnostic printf if func_name not NULL
** Return: OK if vdi passes all tests
**         OUT_OF_RANGE if vdi is too large
**         BAD_DISK_VALUE if vdi is not in this domain
*/
static int
test_vdi(bitFileT *bfp, char *func_name, vdIndexT vdi)
{
    if ( vdi >= BS_MAX_VDI || vdi == 0) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: volume index (%d) out of range.\n", func_name, vdi);
        } else {
            fprintf(stderr, "volume index (%d) out of range.\n", vdi);
	}
        return OUT_OF_RANGE;
    }

    assert(bfp->type != FLE);
    assert(bfp->dmn != NULL);
    if ( bfp->dmn->vols[vdi] == NULL ) {
        if ( func_name != NULL ) {
            fprintf(stderr, "%s: Invalid volume index (%d). No such volume.\n", func_name, vdi);
        } else {
            fprintf(stderr, "Invalid volume index (%d). No such volume.\n", vdi);
	}
	
        return BAD_DISK_VALUE;
    }

    return OK;
}

/*****************************************************************************/
/* Test the specified block. Compare the block number to the number of
** blocks in the volume. Test to see if the block is on a "page" boundary.
** The number of blocks in a volume may not be set up. If not
** it will be zero. In order to test extents, blk -1 & -2 are "OK".
**
** Input: bfp->dmn->vols[vdi]->blocks - blocks in the volume
**        vdi - volume number on which to test block
**        blk - block to test
** Output: none
** Errors: status return only. No diagnostic printf
** Return: OK if block passes all tests
**         BAD_DISK_VALUE if block is not a multiple of 16
**         OUT_OF_RANGE if block is too large for the volume
*/
static int
test_blk(bitFileT *bfp, vdIndexT vdi, uint64_t blk, bf_fob_t pgsize)
{

    if ((blk == XTNT_TERM) || (blk == COWED_HOLE))
        return OK;

    assert(bfp);
    assert(bfp->dmn);
    assert(bfp->dmn->vols[vdi]);

    if ( bfp->dmn->vols[vdi]->blocks &&
         blk >= bfp->dmn->vols[vdi]->blocks )
        return OUT_OF_RANGE;

    return OK;
}
