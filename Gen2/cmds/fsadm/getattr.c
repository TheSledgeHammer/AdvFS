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
 * Facility:
 *
 *    AdvFS - Advanced File System
 *
 * Abstract:
 *
 *    getattr.c - Display attributes for an AdvFS file.
 *
 */

/* includes */
#include        <stdio.h>
#include        <stdlib.h>
#include        <sys/stat.h>
#include        <sys/mount.h>
#include	<advfs/advfs_syscalls.h>
#include	<advfs/bs_error.h>
#include	<strings.h>
#include	<pwd.h>
#include	<grp.h>
#include	<ctype.h>
#include	<sys/file.h>
#include	<sys/errno.h>
#include 	<sys/param.h>
#include	<mntent.h>

#include	<locale.h>
#include        "common.h"

extern int errno;

#define NL_GETATTR MS_FSADM_GETATTR

static char *Prog = "fsadm getattr";

typedef struct volInfo {
    int volCnt;
    adv_vol_ioq_params_t params[1024];
} volInfoT;

/* Local Prototypes */
static
int 
get_vols( 
    adv_bf_dmn_params_t fsParams,	/* in */
    uint32_t vol_index,		/* in */
    volInfoT *volInfo,		/* out */
    char *volName		/* out */
    );

static
int
show_file (
    char *path,			/* in */
    int xflg,			/* in */
    int hflg,			/* in */
    int Iflg,			/* in */
    int verbose			/* in */
    );

static
adv_status_t
display_xtnt_map (
    int fd,			/* in */
    uint32_t allocUnitCnt,	/* in */
    int actualXtntCnt,		/* in */
    int bfXtntMapCnt,		/* in */
    int bfXtntCnt,		/* in */
    bsExtentDescT *xtntsArray,  /* in */
    int hflg			/* in */
    );

void getattr_usage(void)
{
    usage( catgets(catd, NL_GETATTR, GETATTR_USAGE,
                   "%s [-V] [-v] [-I] [-h | -x] filename ...\n" ),
           Prog);
}

/*
 *	NAME:
 *		main_getattr()
 *
 *	DESCRIPTION:
 *		The switches to fsadm getattr are as follows:
 *
 *		<none>	Display statistics about named file or directory
 *		<-x>	Display extent map info for named file or directory.
 *		<-h>	Display extent map including holes.
 *		<-i>	Display statistics about an index file associated with 
 *			the named directory
 *		<-ix>	Display extent map information for the index of named
 *			directory
 *
 *		If a directory has an index file and <-i> switch is not present
 *		the directory name will be followed by (index)
 *
 *		If <-i> switch is used and then the word index will be printed
 *		followed by (<directory name>).
 *
 *	ARGUMENTS:
 *		argc		number of command line arguments
 *		argv		command line arguments
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 */

int
getattr_main( int argc, char **argv )
{
    char *fileName = NULL;
    int c, Iflg, ii, xflg, hflg, Vflg, verbose, fd, fileOpen = FALSE;
    char *p;
    adv_status_t sts;
    extern int optind;
    extern char *optarg;
    int errorFlag;
    uid_t ruid, euid;

    c = Iflg = ii = xflg = hflg = Vflg = verbose = 0;

    ruid = getuid();
    euid = geteuid();
    if (ruid != euid) {
        if (setuid(ruid) == -1) {
            fprintf(stderr, catgets(catd, NL_GETATTR, GETATTR_SETID,
		"%s: Error setting user id\n"), Prog);
            return( 1 );
        }
    }

    /*
     ** Get user-specified command line arguments.
     */
    while ((c = getopt( argc, argv, "VvIxh" )) != EOF) {
        switch (c) {
            case 'x':
                xflg++;
		break;

            case 'I':
                Iflg++;
                break;

            case 'h':
                hflg++;
                break;

            case 'V':
		Vflg++;
		break;

	    case 'v':
		verbose++;
		break;

            default:
                getattr_usage();
                return( 1 );
        }
    }

    if (xflg > 1 || Iflg > 1 || hflg > 1 || verbose > 1 || (xflg && hflg)) {
        getattr_usage();
        return( 1 );
    }

    if ((argc - 1 - optind) < 0) {
        /* missing required args */
        getattr_usage();
        return( 1 );
    }
    /*
     * Check to make sure this utility understands the on-disk
     * structures on this system.
     */
    sts = advfs_check_on_disk_version(BFD_ODS_LAST_VERSION_MAJOR,
                                     BFD_ODS_LAST_VERSION_MINOR);
    if (sts != EOK) {
	fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ONDISK,
	    "%s: This utility cannot process the on-disk structures on this system.\n"), Prog);
	return( 1 );
    }

    /*
     *  If -V option was specified, print out verified command line and exit
     */
    if( Vflg ) {
        printf( "%s", Voption( argc, argv ) );
        return 0;
    }

    errorFlag = FALSE;
    for (ii = optind; ii < argc; ii++) {
        fileName = argv[ii];

        if( show_file( fileName, xflg, hflg, Iflg, verbose ) ) {
            errorFlag = TRUE;
	}
    }

    if( errorFlag ) {
        return( 1 );
    }

    return 0;

}

/*
 *	NAME:
 *		show_file()
 *
 *	DESCRIPTION:
 *		displays AdvFS information about specified file
 *
 *	ARGUMENTS:
 *		path	(in)	path for file to display info on
 *		xflg	(in)	-x option was specified
 *		hflg	(in)	-h option was specified
 *		Iflg	(in)	-I option was specified
 *		verbose	(in)	-v option was specified
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 */

#define XTNTARRAYSIZE 500

static
int
show_file (
    char *path,	/* in */
    int xflg,	/* in */
    int hflg,	/* in */
    int Iflg,	/* in */
    int verbose	/* in */
    )
{
    uid_t ruid=0;
    int fd = -1;
    int err = 0;
    int i, actualXtntCnt=0;
    char fullPath[PATH_MAX];
    char cwd[PATH_MAX];
    char *IDX;
    char idx_tag_path[PATH_MAX];
    char out_txt[PATH_MAX];
    char *mntFsname=NULL;
    char mntOn[PATH_MAX];
    adv_status_t sts;
    int xtntCnt;                            /* number of extents to print */
    int xtntMapCnt;
    static int banner = 0;                  /* Has a banner been displayed? */
    char volName[BS_VD_NAME_SZ];
    volInfoT volInfo;
    vdIndexT metaIndex=0;
    serviceClassT volSvcClass;
    adv_vol_ioq_params_t volIoQ;
    adv_bf_dmn_params_t fsParams;
    struct statvfs vfsStats;
    adv_bf_attr_t bfAttrs;
    adv_bf_info_t bfInfo;
    struct stat fStats;
    mntlist_t *mntlist = NULL, *ptr = NULL, *match = NULL;
    bsExtentDescT fakeXtnts, *xtntsArray;
    vdIndexT volIndex;
    int errorFlag = FALSE;
    int span=0, spantmp=0;
    int sXtntCnt = 0;
    int sMcellCnt = 0;
    arg_infoT *infop = NULL;

    IDX = (char *) malloc(strlen(catgets(catd, NL_GETATTR, GETATTR_INDEX,
	"index")));

    if( IDX == NULL ) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM,
		"%s: insufficient memory.\n"), Prog );
        errorFlag=TRUE;
        goto _finish;
    }
    
    strcpy(IDX,catgets(catd, NL_GETATTR, GETATTR_INDEX, "index"));

    if( realpath( path, fullPath ) == NULL ) {
	fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_STATS,
	    "%s: Error getting '%s' stats; [%d] %s\n"),
	    Prog, path, errno, BSERRMSG( errno ) );
	errorFlag=TRUE;
	goto _finish;
    }

    strcpy( out_txt, fullPath );

    sts = statvfs( fullPath, &vfsStats );
    if (sts < 0) {
	char path_dot[4] = "..";
	if (path[0] == 'M') {
	    /* this path points to one of the meta data files in .tags */
	    sts = statvfs( path_dot, &vfsStats );
	} else {
	    printf( catgets(catd, NL_GETATTR, GETATTR_NOADVFS,
		"%s: The specified file %s is not an AdvFS file\n"),
		Prog, path );
	}
    }

    if( strcmp( vfsStats.f_basetype, "advfs" ) ) {
	printf( catgets(catd, NL_GETATTR, GETATTR_NOADVFS,
	    "%s: The specified file %s is not an AdvFS file\n"), Prog, path );
	errorFlag=TRUE;
	goto _finish;
    }

    /*
     *  load and loop through the mount table looking for the mount on
     *  dir that is closest to our file path.  Save the mount from (fsname)
     *  name as well.
     */
    mntlist = mntl_load( &err );
    if( mntlist == NULL ) {
	printf( catgets(catd,MS_FSADM_COMMON,COMMON_MNTLLOAD,
	    "%s: mntl_load error\n" ), Prog );
	errorFlag=TRUE;
	goto _finish;
    }

    ptr = mntlist;

    while( ptr != NULL ) {

	if( strncmp( ptr->mntl_ondir, fullPath,
	    strlen(ptr->mntl_ondir ) ) == 0 ) {
		spantmp = strspn( fullPath, ptr->mntl_ondir );
		if( spantmp > span ) {
		    /* this entry is closer to what we want */
		    span = spantmp;
                    match = ptr;
		}
	}
	ptr = ptr->mntl_next_p;
    }

    infop = process_advfs_arg( match->mntl_fromname );
    if (infop == NULL) {
        /* There's no hope for what the user passed in, give up */
        fprintf(stderr, catgets(catd,MS_FSADM_COMMON,COMMON_FSNAME_ERR,
                    "%s: Error processing specified name %s\n"),
                Prog, match->mntl_fromname);
        mntl_free( mntlist );
        errorFlag = TRUE;
        goto _finish;
    }
    strcpy( mntOn, match->mntl_ondir );
    mntFsname = strdup( infop->fsname );

    free( infop );
    mntl_free( mntlist );

    sts = lstat( fullPath, &fStats );
    if (sts < 0) {
	fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_STATS,
	    "%s: Error getting '%s' stats; [%d] %s\n"),
	    Prog, path, errno, BSERRMSG( errno ) );
	errorFlag=TRUE;
	goto _finish;
    }

    fd = open( path, O_RDONLY, 0 );
    
    if (fd >= 0) {
        if (Iflg)
        {
            if (S_ISDIR( fStats.st_mode ))
            {
                sts = advfs_get_idx_bf_params(fd, &bfAttrs, &bfInfo );
                if (sts == EINVALID_HANDLE)
                {
                    printf( catgets(catd, NL_GETATTR, GETATTR_NOINDEX,
			"%s: %s does not have an index file\n"),
                            Prog, path );
                    errorFlag=TRUE;
                    goto _finish;
                }
                if (sts != EOK)
                {
                    errorFlag=TRUE;
                    goto _finish;
                }

		sprintf(idx_tag_path, "%s/.tags/0x%08x",
			mntOn, bfInfo.tag.tag_num);

                if (close( fd )) {
                    fprintf(stderr,catgets(catd,MS_FSADM_COMMON,COMMON_NOCLOSE,
			"%s: close of %s failed. [%d] %s\n"),
                             Prog, path, errno, sys_errlist[errno] );
                    errorFlag = TRUE;
                    goto _finish;
                }

                fd = open( idx_tag_path, O_RDONLY, 0 );

                if (getuid()) {
                    if (setuid(ruid) == -1) {
                        fprintf(stderr, catgets(catd, NL_GETATTR, GETATTR_SETID,
				"%s: Error setting user id\n"), Prog);
			errorFlag=TRUE;
			goto _finish;
                    }
                }

                if (fd < 0) {
                    errorFlag=TRUE;
                    goto _finish;
                }
                sprintf(out_txt,"%s (%s)",IDX,fullPath);
                free(IDX);
            }
            else
            {
                fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOTDIR,
			"%s: %s is not a directory\n"),
                         Prog, path );
                errorFlag=TRUE;
                goto _finish;
            }
        }
        else
        {

            if (S_ISDIR( fStats.st_mode ))
            {
                sts = advfs_get_idx_bf_params(fd, &bfAttrs, &bfInfo );
                if (sts == EOK)
                {
                    sprintf(out_txt,"%s (%s)",fullPath,IDX);
                    free(IDX);
                }
                else if (sts != EINVALID_HANDLE)
                {
                    errorFlag=TRUE;
                    goto _finish;
                }
            }

            sts = advfs_get_bf_params (fd, &bfAttrs, &bfInfo );  
	    if( sts != EOK ) {
		fprintf( stderr, catgets( catd, MS_FSADM_COMMON,
			 COMMON_ERRBFPARAMS,
			 "%s: Cannot get file %s parameters\n" ),
			 Prog, out_txt );
		errorFlag=TRUE;
		goto _finish;
	    }
        }
    } else {

	/* fd was not valid */
	errorFlag = TRUE;
	goto _finish;
    }

    /* Get file system params and a list of volumes */
    sts = advfs_get_dmnname_params( mntFsname, &fsParams );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOFSPARAMS,
              "%s: cannot get file system parameters for %s\n"),
                 Prog, mntFsname );
	errorFlag=TRUE;
	goto _finish;
    }
    if (get_vols( fsParams, bfInfo.volIndex, &volInfo, volName )) {
        errorFlag = TRUE;
	goto _finish;
    }

    printf( "\n" );

    /* Print full file path */
    printf( catgets( catd, NL_GETATTR, GETATTR_FILE,
		     "                 File: %s\n" ), out_txt );

    /* Get metadata volume index and print vol name and index */
    sts = advfs_get_vol_index( mntFsname, volName, &metaIndex,
			       &volSvcClass, &volIoQ );
    if( sts != EOK ) {
	fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_VOLNOTMEM,
	         "%s: volume '%s' is not a member of file system '%s'\n" ),
	         Prog, volName, mntFsname );
	errorFlag = TRUE;
	goto _finish;
    }
    strcpy( volName, strrchr( volName, '/' ) + 1 );
    printf( catgets( catd, NL_GETATTR, GETATTR_METAVOL,
		     "              MetaVol: %s (%d)\n" ), volName, metaIndex );

    /* If verbose, print snapshot information */
    if( ( bfInfo.mbfSnapType & ST_HAS_PARENT ) || verbose ) {
	printf( "%22s ", "Is a snapshot?");
	printf( "%s", (bfInfo.mbfSnapType & ST_HAS_PARENT) ? 
	        catgets(catd, MS_FSADM_COMMON, COMMON_YES, "Yes") :
	        catgets(catd, MS_FSADM_COMMON, COMMON_NO, "No"));
	printf( " %s\n", (bfInfo.mbfSnapType & ST_SNAP_OUT_OF_SYNC) ? 
            catgets(catd, NL_GETATTR, GETATTR_NOSYNC, "(OUT_OF_SYNC)") : "");

	printf( "%22s ", "Has a snapshot?");
	printf( "%s\n", (bfInfo.mbfSnapType & ST_HAS_CHILD) ? 
	        catgets(catd, MS_FSADM_COMMON, COMMON_YES, "Yes") :
	        catgets(catd, MS_FSADM_COMMON, COMMON_NO, "No"));

	printf( "%21s: ", catgets(catd, NL_GETATTR, GETATTR_ORIGFILESIZE,
	    "Original file size") );
	if( bfInfo.mbfSnapType & ST_HAS_PARENT ) {
	    printf( "%d\n", bfInfo.mbfOrigFileSize );
	} else {
	    printf( catgets( catd, NL_GETATTR, GETATTR_NA, "N/A\n" ) );
	}
    }

    /* If verbose, print reserved file size */
    if( verbose ) {
	printf( "%21s: %d\n", catgets(catd, NL_GETATTR, GETATTR_RSVDFILESIZE, 
		"Reserved file size"), bfInfo.mbfRsvdFileSize );
    }

    printf( catgets( catd, NL_GETATTR, GETATTR_BANNER1,
		     "\n        Id      Alloc Unit    Alloc Unit Cnt   File Class\n") );

    printf( "%5ld.%04ld", bfInfo.tag.tag_num, bfInfo.tag.tag_seq );
    printf( "%12d", bfInfo.pageSize );
    printf( "%15ld", ( bfInfo.mbfNumFobs / bfInfo.pageSize ) );

    if( bfInfo.dataSafety == BFD_USERDATA )
	printf( catgets( catd, NL_GETATTR, GETATTR_USER, "\t\t  user    " ) );
    else if( bfInfo.dataSafety == BFD_METADATA )
	printf( catgets( catd, NL_GETATTR, GETATTR_META, "\t\t  meta    " ) );
    else
	printf( "\t\t **" );

    /* Get the total extent count and malloc space for the extent map */
    xtntMapCnt = 1;
    sts = advfs_get_bf_xtnt_map( fd,
				 xtntMapCnt,
				 0, 0,
				 &fakeXtnts,
				 &xtntCnt,
				 &volIndex );
    if( sts != EOK ) {
	fprintf( stderr, catgets(catd, NL_GETATTR, GETATTR_XTNTERR,
		"%s: Error loading extent map; %s\n"),
		Prog, BSERRMSG( sts ) );
	errorFlag = TRUE;
	goto _finish;
    }

    xtntsArray = (bsExtentDescT *) malloc( xtntCnt * sizeof( bsExtentDescT ) );
    if( xtntsArray == NULL ) {
	fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM,
	         "%s: insufficient memory.\n" ), Prog );
	errorFlag = TRUE;
	goto _finish;
    }

    sts = advfs_get_bf_xtnt_map( fd,
				 xtntMapCnt,
				 0,
				 xtntCnt,
				 xtntsArray,
				 &xtntCnt,
				 &volIndex );
    if (sts == EOK) {
        for( i=0; i<xtntCnt; i++ )
            if( ( xtntsArray[i].bsed_vd_blk != XTNT_TERM ) &&
                    ( xtntsArray[i].bsed_vd_blk != COWED_HOLE ) )
                actualXtntCnt++;
    } else {
        fprintf( stderr, catgets(catd, NL_GETATTR, GETATTR_XTNTERR,
		"%s: Error loading extent map; %s\n"),
                    Prog, BSERRMSG( sts ) ); 
        errorFlag = TRUE;
        goto _finish;
    }
    
    printf( "\n" );

    if (!xflg && !hflg) {
        goto _finish;
    }

    sts = display_xtnt_map( fd,
			    (bfInfo.mbfNumFobs / bfInfo.pageSize),
			    actualXtntCnt,
			    xtntMapCnt,
			    xtntCnt,
			    xtntsArray,
			    hflg );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, NL_GETATTR, GETATTR_MAPFAIL,
		"%s: Error displaying extent map\n"), Prog );
        errorFlag = TRUE;
        goto _finish;
    }

_finish:

    if (fd >= 0) {
        if (close( fd )) {
            fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOCLOSE,
		"%s: close of %s failed. [%d] %s\n"),
                    Prog, path, errno, sys_errlist[errno] );
            errorFlag = TRUE;
        }
    }

    if (mntFsname) 
        free( mntFsname );

    if( xtntsArray )
	free( xtntsArray );

    if( errorFlag ) {
        return 1;
    }

    return 0;

} /* end show_file */

/*
 *	NAME:
 *		get_vols()
 *
 *	DESCRIPTION:
 *		returns a list of volumes in file system
 *		Also returns the volume name that matches the volume
 *		index of the specified file.
 *
 *	ARGUMENTS:
 *		fsParams	(in)	file system parameters
 *		vol_index	(in)	index for volume containing this file
 *		volInfo		(out)	volInfoT structure
 *		volName		(out)	Volume associated with index number
 *
 *	RETURN VAULES:
 *		success			0
 *		failure			1
 */

static
int
get_vols( 
    adv_bf_dmn_params_t fsParams,
    uint32_t vol_index,
    volInfoT *volInfo,
    char *volName
    )
{
    adv_status_t sts;
    int ret=0;
    char *tok;
    vdIndexT vol, volIndex;
    int vols;
    adv_vol_info_t volInfop;
    adv_vol_prop_t volPropertiesp;
    adv_vol_counters_t volCountersp;
    uint32_t *volIndexArray;
    long totalSpace = 0L, totalFree = 0L;

    volIndexArray = (uint32_t*) malloc( fsParams.curNumVols * sizeof(uint32_t) );
    if (volIndexArray == NULL) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM,
		"%s: insufficient memory.\n"), Prog );
        return 1;
    }

    sts = advfs_get_dmn_vol_list( fsParams.bfDomainId, 
                                 fsParams.curNumVols, 
                                 volIndexArray, 
                                 &vols );
    if (sts == ENO_SUCH_DOMAIN) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOVOLINFO, 
	   "%s: unable to display volume info; file system not active\n"),
		Prog );
	ret = 1;
	goto _finish;

    } else if (sts != EOK) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOFSLIST,
		"%s: cannot get file system's volume list\n"), Prog );
	ret = 1;
	goto _finish;
    }

    for (vol = 0; vol < vols; vol++) {


        sts = advfs_get_vol_params( fsParams.bfDomainId, 
                                    volIndexArray[vol], 
                                    &volInfop, 
                                    &volPropertiesp,
                                    &volInfo->params[ volIndexArray[ vol ] ],
                                    &volCountersp );

        /*  the volIndex can have holes in it.  Don't fail
         *  on EBAD_VDI
         */
        if (sts != EOK && sts != EBAD_VDI) {
            fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_VOLPARAMS_ERR,
		"%s: get vol params error %s\n"), 
                     Prog, BSERRMSG( sts ) );
	    ret = 1;
	    goto _finish;
        }

	/* Set the volume name for the passed in volume index */
	if( (volInfop.volIndex) == vol_index) {
	    strcpy( volName, volPropertiesp.volName);
	}
    }

_finish:
    free( volIndexArray );
    return ret;
}

/*
 *	NAME:
 *		display_xtnt_map()
 *
 *	DESCRIPTION:
 *		Displays extenet map for non-stripe files
 *
 *	ARGUMENTS:
 *		fd		(in)	file descriptor for file we're getting
 *					attributes for
 *		allocUnitCnt	(in)	total number of Allocation Units
 *		actualXtntCnt	(in)	Not including HOLEs and TERMs
 *		bfXtntMapCnt	(in)	extenet map count
 *		bfXtntCnt	(in)	extent count
 *		xtntsArray	(in)	extent map
 *		hflg		(in)	-h option was specified
 *
 *	RETURN VALUES:
 *		success			0 (EOK)
 *		failure			non-zero
 *
 */

static
adv_status_t
display_xtnt_map (
                  int fd,                    /* in */
                  uint32_t allocUnitCnt,     /* in */
		  int actualXtntCnt,	     /* in */
                  int bfXtntMapCnt,          /* in */
                  int bfXtntCnt,             /* in */
		  bsExtentDescT *xtntsArray, /* in */
                  int hflg                   /* in */
                  )

{

    int x;

    printf (catgets(catd, NL_GETATTR, GETATTR_XTNTMAP,
	"\n      extentMap: %d\n\n"), bfXtntMapCnt);
    printf (catgets(catd, NL_GETATTR, GETATTR_BANNER2,
	"\n        AllocUnitOff    AllocUnitCnt    vol     volBlock        volBlockCnt\n"));

    for( x=0; x<bfXtntCnt; x++ ) {
	if( hflg || ( xtntsArray[x].bsed_vd_blk != XTNT_TERM ) &&
	    ( xtntsArray[x].bsed_vd_blk != COWED_HOLE ) ) {

	    printf ("\t%u", xtntsArray[x].bsed_fob_offset );
	    printf ("\t\t%u", xtntsArray[x].bsed_fob_cnt);
	    printf ("\t\t%d", xtntsArray[x].bsed_vol_index);
	    if( ( xtntsArray[x].bsed_vd_blk != XTNT_TERM ) &&
		( xtntsArray[x].bsed_vd_blk != COWED_HOLE ) ) {

		printf("\t%u", xtntsArray[x].bsed_vd_blk);
		printf("\t\t%u", xtntsArray[x].bsed_fob_cnt /
			ADVFS_FOBS_PER_DEV_BSIZE );
	    } else if ( xtntsArray[x].bsed_vd_blk == COWED_HOLE) {
		printf( catgets( catd, NL_GETATTR, GETATTR_COWHOLE,
			"       COWHOLE" ) );
	    } else if ( ( xtntsArray[x].bsed_vd_blk == XTNT_TERM ) &&
			( xtntsArray[x].bsed_fob_cnt != 0 ) ) {
		printf( catgets( catd, NL_GETATTR, GETATTR_HOLE,
			"       HOLE" ) );
	    } else {
		printf( catgets( catd, NL_GETATTR, GETATTR_TERM,
			"       TERM" ) );
	    }
	    printf( "\n" );
	}
    }

    /* Number of extents shown in this map */
    printf (catgets(catd, NL_GETATTR, GETATTR_STORE,
	"\n      Number of storage extents:              %d"),
	actualXtntCnt );

    /* Average extent size */
    printf( catgets(catd, NL_GETATTR, GETATTR_AVERAGE,
	"\n      Average extent size (Allocation Units): %d\n\n" ),
	(allocUnitCnt / actualXtntCnt) );

    return EOK;

}  /* end display_xtnt_map */
