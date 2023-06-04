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
#include	<signal.h>
#include        <unistd.h>
#include        <sys/stat.h>
#include        <strings.h>
#include        <locale.h>
#include        <sys/time.h>
#include        <sys/file.h>
#include        <sys/param.h>
#include        <advfs/bs_error.h>
#include        <advfs/advfs_syscalls.h>

#include        "common.h"

#define NL_MVLOG MS_FSADM_MVLOG

static char *Prog = "fsadm mvlog";

/* Prototypes */
static long get_block_size( const char *size_str );
void mvlog_usage( void );

/*
 *	NAME:
 *		mvlog_main()
 *
 *	DESCRIPTION:
 *		main routine for fsadm mvlog command
 *		This command moves a file system transaction log to
 *		a specified volume.  The log size may also be changed with
 *		the -l option.  The size must be spcified in K (kilobytes),
 *		M (megabytes), or G (gigabytes).
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
mvlog_main(int argc, char **argv)
{
    char *fsname=NULL, *volName=NULL, *fspath=NULL;
    arg_infoT *infop;
    bf_fob_t logFobs = 0;
    serviceClassT logSvc = 0;
    adv_status_t sts;
    vdIndexT volIdx = 0;
    int retval=0, err, i, c, V = 0, l = 0, thisFSLock = -1;
    extern int optind;
    extern char *optarg;

    check_root( Prog );

    /*
     ** Get user-specified command line arguments.
     */
    while ((c = getopt( argc, argv, "Vl:" )) != EOF) {
        switch (c) {
            case 'l':
                l++;
                logFobs = get_block_size( optarg );
		if( logFobs == -1 ) {
		    fprintf(stderr, catgets( catd, NL_MVLOG, MVLOG_LOGFOBS,
			"%s: Invalid logsize argument\n" ), Prog );
		    return( 1 );
		}
                break;

            case 'V':
		V++;
		break;

            case '?':

            default:
		mvlog_usage();
                return( 1 );
        }
    }

    if (l > 1) {
	mvlog_usage();
        return( 1 );
    }

    if (optind != (argc - 2)) {
        /* missing required args */
	mvlog_usage();
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

    /* zero will fail, but that's okay since volumes should start at 1 */
    volIdx = (vdIndexT)atoi( argv[optind] );

    /* user may have specified device path instead of volume index */
    if( !volIdx ) {
        fsname = dev_to_fsname(argv[optind], NULL);
	if( fsname == NULL ) {
	    /* Input makes no sense, croak */
	    mvlog_usage();
	    return( 1 );
	}

	/* volume path */
	volName = argv[optind];
    } /* end !volIdx */

    /* what file system name are we working with */
    infop = process_advfs_arg( argv[optind+1] );
    if( infop == NULL ) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NO_FS,
            "%s: file system '%s' does not exist\n" ),
             Prog, argv[optind + 1] );
        retval=1;
        goto _finish;
    }

    if (fsname != NULL) {
        /* volume specified and storage domain specified must match */
        if (strcmp(fsname, infop->fsname) != 0) {
            fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_VOLNOTMEM,
                     "%s: volume '%s' is not a member of file system '%s'\n" ),
                     Prog, volName, infop->fsname );
            retval=1;
            goto _finish;
        }

        /* done with this now */
        free(fsname);
        fsname = NULL;

        /* get the volume index */
        sts = advfs_get_vol_index(infop->fsname, volName, &volIdx, NULL, NULL);

        /* if index is not set, then the disk was not in this volume set */
        if( !volIdx ) {
            fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_VOLNOTMEM,
                "%s: volume '%s' is not a member of file system '%s'\n" ),
                 Prog, volName, infop->fsname );
            retval=1;
            goto _finish;
        }
    }

    /* If -V option was specified, print out verified command line and return */
    if( V ) {
	printf( "%s", Voption( argc, argv ) );
	retval=0;
	goto _finish;
    }

    /*
     * Lock the file system while you do the mvlog
     */
    thisFSLock = advfs_fspath_lock( infop->fspath, FALSE, FALSE, &err );
    if (thisFSLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr, catgets(catd, MS_FSADM_COMMON, COMMON_FSBUSY,
		"%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified file system.\n"), Prog);
        } else {
            fprintf(stderr, catgets(catd, MS_FSADM_COMMON, COMMON_ERRLOCK,
		"%s: Error locking '%s'; [%d] %s\n"),
		Prog, fsname, err, BSERRMSG (err) );
        }
	retval=1;
        goto _finish;
    }

    sts = advfs_switch_log( infop->fsname, volIdx, logFobs, logSvc );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, NL_MVLOG, MVLOG_NOSWITCH,
		"%s: Cannot switch log to a new volume; %s\n"),
                 Prog, BSERRMSG( sts ) );
	retval=1;
        goto _finish;
    }

_finish:

    if (thisFSLock != -1) {
        advfs_unlock(thisFSLock, &err);
    }

    if( infop )
	free( infop );
    if( fsname )
	free( fsname );
    return( retval );
}

/*
 *	NAME:
 *		get_block_size()
 *
 *	DESCRIPTION:
 *		Take a string in one of the following formats and convert
 *		it to a size in 1K blocks: K, MB, GB
 *
 *	ARGUMENTS:
 *		blk_str		(in)	string representing the size
 *
 *	RETURN VALUES:
 *		success		returns block size in 1k blocks
 *		failure		-1
 *
 */

static long get_block_size( const char *size_str ) {
    char *p = NULL;
    long size = 0;
    long block_size = 0;

    size = strtoul( size_str, &p, 10 );
    if( p == NULL || strlen( p ) > 1 ) {
	return -1;
    }
    switch( (int)p[0] ) {
	case 'K':
	    block_size = size;
	    break;
	case 'M':
	    block_size = size * 1024;
	    break;
	case 'G':
	    block_size = size * 1024 * 1024;
	    break;
	case '\0':
	default:
	    return -1;
    }

    return block_size;
}

void
mvlog_usage( void )
{
    usage( catgets(catd, NL_MVLOG, MVLOG_USAGE,
                   "%s [-V] special fsname\n" ),
           Prog );
}
