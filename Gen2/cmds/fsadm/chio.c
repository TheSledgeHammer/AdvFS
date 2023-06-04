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

/* includes */
#include        <stdio.h>
#include        <string.h>
#include        <sys/file.h>
#include        <advfs/advfs_syscalls.h>
#include        <sys/errno.h>
#include        <locale.h>

#include        "common.h"

extern int errno;

#define NL_CHIO	MS_FSADM_CHIO

/* options list set flags */
#define READ_BLOCKS     0x0001  /* max 1 KB blocks to read in an I/O request */
#define WRITE_BLOCKS    0x0002  /* max 1 KB blocks to write in an I/O request */
#define ACTIVATE_VOL    0x0004  /* Activate a volume after incomplete fsadm
                                 * addvol or fsadm rmvol */
static char *Prog = "fsadm chio";
static int32_t fs_lock;

/* Prototypes */
static
int
parse_options(
    char *options,			/* in */
    unsigned int *chioOptions,		/* out */
    int *rb,				/* out */
    int *wb				/* out */
    );

static
int
getvdparams(
    arg_infoT* infop,			/* in  */
    char *volName,			/* in  */
    uint32_t flag,			/* in  */
    bfDomainIdT *fsId,  		/* out */
    vdIndexT *vdi,				/* out */
    adv_vol_ioq_params_t *volp,		/* out */
    serviceClassT *volSvcClass  	/* out */
    );

void
chio_usage();

/*
 *	NAME:
 *		chio_main()
 *
 *	DESCRIPTION:
 *		main routine for fsadm chio command
 *
 *	ARGUMENTS:
 *		argc		number of command line arguments
 *		argv		command line arguments
 *
 *	RETURN VALUES:
 *		success		0
 *		failure		non-zero
 *
 */

int
chio_main(int argc, char **argv)
{
    int rb, wb;
    int l, o, V;
    arg_infoT* infop;
    char *volName = NULL;
    char *fsname = NULL;
    char *options = NULL;
    int ch, res;
    int32_t err;
    bfDomainIdT fsId;
    vdIndexT vdi;
    adv_vol_ioq_params_t volIoQParams;
    serviceClassT volSvcClass;
    adv_status_t sts;
    extern int getopt();
    extern char *optarg;
    extern int optind;
    extern int opterr;
    int retval=0;
    uint32_t iobmax, iobmin, iobpref;
    uint32_t chioOptions = 0;

    l = o = V = rb = wb = 0;

    check_root( Prog );

    /*
     * Get user specified command line arguments.
     * No arguments displays the default I/O transfer sizes
     */
    while( (ch = getopt( argc, argv, "Vlo:" )) != EOF ) {
        switch( ch ) {

            case 'l':	/* Display range of I/O transfer sizes */
                l++;
                break;

            case 'o':	/* options list */
                o++;
                options = (char *) malloc(strlen(optarg)+1);
                if (!options) {
                        fprintf( stderr,
				catgets( catd, MS_FSADM_COMMON, COMMON_NOMEM,
				"%s: insufficient memory.\n"), Prog );
                        exit(1);
                }
                strcpy( options, optarg);
                break;

	    case 'V':
		V++;
		break;

	    case '?':

            default:
		chio_usage();
                exit( 1 );
        }
    }

    /* Check arguments */
    if( ( l > 1 ) || ( o > 1 ) || ( V > 1 ) ) {
	chio_usage();
	exit( 1 );
    }
    if( ( argc - 1 - optind ) < 0 ) {
        /* missing required args */
	chio_usage();
        exit( 1 );
    }

    /*
     * Get the device special and fsname 
     */
    infop = process_advfs_arg( argv[optind] );
    if( infop == NULL ) {
	chio_usage();
	exit( 1 );
    }

    /*
     * This is a multi volume file system, and the user did not
     * specify a volume
     */
    if( strcmp( infop->blkfile, "" ) == 0 ) {
	chio_usage();
	retval = 1;
	goto _finish;
    } else
	volName = infop->blkfile;

    if (argv[optind + 1] != NULL) {
        /* User specified a blockfile and a fsname.
         * Check to make sure they belong to the same filesystem 
         */
        fsname = arg_to_fsname(argv[optind + 1], NULL);
        if (fsname == NULL) {
	    fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NO_FS,
		"%s: file system '%s' does not exist\n" ),
		Prog, argv[optind + 1] );
	    chio_usage();
	    retval = 1;
	    goto _finish;
	}
        
        if (strcmp(fsname, infop->fsname) != 0) {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, 
                COMMON_FSNAME_DEV_ERR,   
                "%s: file system and device mismatch: %s, %s\n"),
                Prog, infop->fsname, fsname);
            retval = 1;
            free(fsname);
            goto _finish;
        }
    }


    /*
     * If -o option was used, parse the options and set the options flag
     */
    if( o ) {
        if( parse_options( options, &chioOptions, &rb, &wb ) == FALSE ) {
	    retval = 1;
	    goto _finish;
	}
    }

    /*
     *  If -V option was specified, print out verified command line and exit
     */
    if( V ) {
	printf( "%s", Voption( argc, argv ) );
	retval = 0;
	goto _finish;
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
	retval = 1;
	goto _finish;
    }
    res = getvdparams(
	infop,
        volName,
	chioOptions,
        &fsId,
        &vdi,
        &volIoQParams,
        &volSvcClass
        );
    if( res < 0 ) {
	retval = 1;
	goto _finish;
    }

    /*
     * Modify the required params.
     */
    iobmax = volIoQParams.max_iosize/DEV_BSIZE;
    iobpref = volIoQParams.preferred_iosize/DEV_BSIZE;
    iobmin = volIoQParams.min_iosize/DEV_BSIZE;

    /* Check the read range */
    if( chioOptions & READ_BLOCKS ) {
        if( rb >= iobmin && rb <= iobmax ) {
            volIoQParams.rdMaxIo = rb;
        } else {
            fprintf(stderr, catgets(catd, NL_CHIO, CHIO_READRANGE,
		"%s: read blocks not within range %d-%d\n"),
		Prog, iobmin, iobmax);
            retval = 1;
            goto _end;
        }
    }

    /* Check the write range */
    if( chioOptions & WRITE_BLOCKS ) {
        if( wb >= iobmin && wb <= iobmax ) {
            volIoQParams.wrMaxIo = wb;
        } else {
            fprintf(stderr, catgets(catd, NL_CHIO, CHIO_WRITERANGE,
		"%s: write blocks not within range %d-%d\n"),
		Prog, iobmin, iobmax);
            retval = 1;
            goto _end;
        }
    }

    /*
     * Print the current parameters if nothing was changed.
     */
    if( !( o || l ) ) {
        fprintf( stdout, catgets(catd, NL_CHIO, CHIO_PARAMS,
		"rblks = %d  wblks = %d\n"),
		volIoQParams.rdMaxIo,
	    volIoQParams.wrMaxIo );
	retval = 0;
	goto _finish;
    }


    if( (chioOptions & READ_BLOCKS) || (chioOptions & WRITE_BLOCKS) ){
        /*
         * Stuff the new values
         */
        sts = advfs_set_vol_ioq_params( fsId, vdi, &volIoQParams );

        if (sts != EOK) {
            fprintf(stderr, catgets(catd, NL_CHIO, CHIO_NOPARAMS,
		"%s: Cannot set I/O parameters\n"), Prog);
            if( sts != -1 ) {
                fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
			"%s: Error = %s\n"), Prog, BSERRMSG (sts));
            } else {
                fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
			"%s: Error = %s\n"), Prog, ERRMSG (errno));
            }
            retval = 1;
            goto _end;
        }
        fprintf( stdout, catgets(catd, NL_CHIO, CHIO_PARAMS,
	    "rblks = %d  wblks = %d\n"),
	    volIoQParams.rdMaxIo, volIoQParams.wrMaxIo );
    }

_end:

    if( chioOptions & ACTIVATE_VOL ) {
     /*
      * Re-add the volume to the service class.  Here, it is first necessary
      * to remove the volume, in case it is still in the service class, because
      * adding it to the service class does not check to see if it is already
      * there.
      */
        sts = advfs_remove_vol_from_svc_class(infop->fsname, vdi, volSvcClass );
        if (sts != EOK && sts != EBAD_VDI) {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
		"%s: Error = %s\n"), Prog, BSERRMSG (sts));
            retval = 1;
        }

        sts = advfs_add_vol_to_svc_class(infop->fsname, vdi, volSvcClass );
        if (sts != EOK) {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
		"%s: Error = %s\n"), Prog, BSERRMSG (sts));
            retval = 1;
        }
    }

    if( l ) {
        fprintf( stdout, catgets(catd, NL_CHIO, CHIO_TRANSRATES,
	    "rblks: min = %d  max = %d  pref = %d\nwblks: min = %d  max = %d  pref = %d\n"), iobmin, iobmax, iobpref, iobmin, iobmax, iobpref);
    }

_finish:

    /* unlock the filesystem */
    if( advfs_unlock( fs_lock, &err ) == -1 ) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRUNLOCK,
	    "%s: Error unlocking '%s'; [%d] %s\n"),
	    Prog, infop->fspath, errno, ERRMSG(errno));
	retval = 1;
    }

    free( infop );
    return( retval );
}

/*
 *	NAME:
 *		parse_options()
 *
 *	DESCRIPTION:
 *		parse_options takes the string of options provided by the user
 *		and sets the appropriate fsadm chio flags.  It then parses the
 *		read and write block sizes and stores then in rb and wb
 * 
 *		parse_options returns TRUE for success and FALSE if an invalid
 *		argument is specified.
 *
 *	ARGUMENTS:
 *		options		(in)	options list from chio_main()
 *		chioOptions	(out)	bit flag for user specified options
 *		rb		(out)	read block transfer rate
 *		wb		(out)	Write block transfer rate
 *
 *	RETURN VALUES:
 *		success			TRUE
 *		Failure			FALSE
 *
 */

static
int
parse_options(
    char *options,		/* in */
    unsigned int *chioOptions,	/* out */
    int *rb,			/* out */
    int *wb			/* out */
    )
{
    char *optptr;
    char *optbuf;
    char *pp = 0;

    *wb = *rb = 0;

    optbuf = (char *) malloc(strlen(options)+1);
    if (!optbuf) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM,
		"%s: insufficient memory.\n"), Prog );
        return FALSE;
    }
    strcpy(optbuf, options);

    for( optptr = strtok( optbuf, "," ); optptr;
	optptr = strtok( (char *)NULL, "," ) ) {

        if( strncmp(optptr, "read=", strlen( "read=" ) ) == 0 ) {
            *chioOptions |= READ_BLOCKS;
            optptr = strchr( optptr, '=' );
            *rb = atoi( optptr+1 );

        } else if (strncmp(optptr, "write=", strlen( "write=" ) ) == 0 ) {
            *chioOptions |= WRITE_BLOCKS;
            optptr = strchr( optptr, '=' );
            *wb = atoi( optptr+1 );

        } else if (!strcmp(optptr, "activate")) {
            *chioOptions |= ACTIVATE_VOL;

        } else {
            fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_BADOARG,
		"%s: unknown -o argument '%s'.\n"), Prog, optptr );
            free(optbuf);
            return FALSE;
        }
    }

    free(optbuf);

    return TRUE;
}

/*
 *	NAME:
 *		getvdparams()
 *
 *	DESCRIPTION:
 *		get the file system id, the vd index, and the vd I/O params.
 *
 *	ARGUMENTS:
 *		infop 		(in)	file system information
 *		volName		(in)	volume name
 *		flag		(in)	options flag
 *		fsId		(out)	file system ID
 *		vdi		(out)
 *              volp 		(out)   volume params
 *		volSvcClass	(out)	volume service class
 *
 *	RETURN VALUES:
 *		sucess			0
 *		failure			-1
 */

static
int
getvdparams(
    arg_infoT* infop,			/* in */
    char *volName,			/* in */
    uint32_t  flag,			/* in */
    bfDomainIdT *fsId,	        	/* out */
    vdIndexT *vdi,				/* out */
    adv_vol_ioq_params_t *volp,		/* out */
    serviceClassT *volSvcClass  	/* out */
    )

{
    int err;
    struct stat stats;
    adv_bf_dmn_params_t fsParams;
    adv_status_t sts;
    char volPathName[MAXPATHLEN];
    char *subs;
    char volLink[MAXPATHLEN+1];
    int i;
    char lvmVolGroup[64];
    char lvmVolName[MAXPATHLEN+1];

    /*
     * Lock the global AdvFS file system directory and the file system itself.
     * This synchronizes us with file system and bitfile set creation/deletion.
     */
    fs_lock = advfs_fspath_lock( infop->fspath, TRUE, FALSE, &err );
    if( fs_lock < 0 ) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_FSBUSY,
		"%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified file system.\n"), Prog);
        } else {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRLOCK,
		"%s: Error locking '%s'; [%d] %s\n"),
		Prog, infop->fspath, err, ERRMSG( err ) );
        }
        goto _error;
    }

    sts = advfs_get_dmnname_params(infop->fsname, &fsParams );
    if( sts != EOK ) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOFSPARAMS,
		"%s: Cannot get file system parameters for %s\n"),
		Prog, infop->fsname);
        if( sts != -1 ) {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR,
		"%s: Error = %s\n"), Prog, BSERRMSG (sts));
        } else {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERR, 
		"%s: Error = %s\n"), Prog, ERRMSG (errno));
        }
        goto _error;
    }

    /*
     * Build the symlink for the volume
     */
    if( !advfs_checkvol( volName, lvmVolGroup, lvmVolName ) ) { 

	/* This is a regular device */
        subs = (char *) strrchr (volName, '/');
        if( subs == NULL ) {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_SLASH,
		"%s: Error looking for '/' in %s\n"), Prog, volName );
            goto _error;
        }
	snprintf( volPathName, MAXPATHLEN, "%s/%s", infop->stgdir, subs );

    } else {

        /* This is a volume manager volume */
	snprintf( volPathName, MAXPATHLEN-strlen(volPathName), "%s/%s.%s",
	    infop->stgdir, lvmVolGroup, lvmVolName );
    }

    err = lstat( volPathName, &stats );
    if( err != 0 ) {
        if( errno == ENOENT ) {
	    fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_VOLNOTMEM,
		"%s: volume '%s' is not a member of file system '%s'\n"),
		Prog, volName, infop->fsname);
        } else {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_STATS,
		"%s: Error getting '%s' stats; [%d] %s\n"),
		Prog, volPathName, errno, ERRMSG (errno));
        }
        goto _error;
    }

    err = readlink( volPathName, volLink, MAXPATHLEN+1 );
    if( err < 0 ) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRSYMLINK,
		"%s: Error getting '%s' symbolic link; [%d] %s\n"),
		Prog, volPathName, errno, ERRMSG (errno));
        goto _error;
    }

    sts = advfs_get_vol_index(infop->fsname, volLink, vdi, volSvcClass, volp);

    if (sts != EOK) {
        fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_VOLNOTMEM,
		"%s: volume '%s' is not a member of file system '%s'\n"),
		Prog, volName, infop->fsname);
        goto _error;
    }

    *fsId =fsParams.bfDomainId;

    return( 0 );

_error:

    return( -1 );
}

void
chio_usage( void )
{
    usage( catgets( catd, NL_CHIO, CHIO_USAGE,
                    "%s [-V] [-l] [-o option_list] special [fsname]\n" ),
           Prog);
}
