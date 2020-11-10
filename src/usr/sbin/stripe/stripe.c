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
 *      MegaSafe Storage System
 *
 * Abstract:
 *
 *      stripe utility.
 *
 * Date:
 *
 *      Wed July 08 09:11:50 1992
 *
 * Revision History:
 *
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: stripe.c,v $ $Revision: 1.1.7.2 $ (DEC) $Date: 2005/10/03 14:03:42 $";
#endif

#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <sys/file.h>
#ifdef _OSF_SOURCE
#include <sys/mount.h>
#endif

#include <msfs/bs_error.h>
#include <msfs/msfs_syscalls.h>

#include <locale.h>
#include "stripe_msg.h"
nl_catd catd;

#ifdef _OSF_SOURCE
#define O_open open
#define O_close close
#else
#include <msfs/fs_syscalls.h>
#endif

static char *Prog = "stripe";

extern int errno;
extern char *sys_errlist[];

#ifndef _OSF_SOURCE
static char *BfSetName = "set1";
#endif

void usage( void )
{
    fprintf( stderr, catgets(catd, 1, USAGE, "usage: %s -n volume_count filename\n"), Prog);
}

main( int argc, char *argv[] )

{

    int c, n = 0;
    int fd;
    char *path;
    int segmentCnt = -1;
    mlStatusT sts;
    extern int optind;
    extern char *optarg;
    mlBfAttributesT bfAttrs;
    mlBfInfoT bfInfo;
    int errorFlag = 0;
    struct stat Stats;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_STRIPE, NL_CAT_LOCALE);


    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "n:" )) != EOF) {
        switch (c) {

            case 'n':
                n++;
                segmentCnt = atoi( optarg );
                break;

            default:
                usage();
                exit( 1 );
        }
    }

    if (n != 1) {
        /* must specify -n once and only once */
        usage();
        exit( 1 );
    }

    if (optind != (argc - 1)) {
        /* missing required arg */
        usage();
        exit( 1 );
    }

    if (segmentCnt <= 1) {
        fprintf( stderr, catgets(catd, 1, 2, "%s: Illegal volume count.\n"), Prog);
        exit( 1 );
    }

    /* get path name */
    path = argv[optind];

 
    if (clu_is_member()) {

        /*
         * The following stat() is necessary in a cluster.
         * This will cause a cluster-wide flush of the file so
         * that any outstanding dirty data will be flushed to the
         * server and requested new allocations will take effect
         * prior to the execution of stripe.
         *
         */

        sts = stat( path, &Stats ); 
        if (sts < 0) {
           fprintf(stderr,
                   catgets(catd,1,7,"\n%s: stat failed for file '%s'; %s\n"),
                   Prog, path, BSERRMSG( errno ) );
            return 1;
        }
    }

#ifndef _OSF_SOURCE
    fs_init1( "dmn1", "set1" );
#endif

    fd = O_open( path, O_RDONLY, 0 );
    if (fd < 0) {
        fprintf( stderr, catgets(catd, 1, 3, "%s: open of %s failed. [%d] %s\n"),
                Prog, path, errno, sys_errlist[errno] );
        return 1;
    }

    sts = advfs_get_bf_params( fd, &bfAttrs, &bfInfo );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 4, "%s: advfs_get_bf_params failed --- %s\n"),
                Prog, BSERRMSG( sts ));
        errorFlag = 1;
    }

    bfAttrs.mapType = XMT_STRIPE;
    bfAttrs.attr.stripe.segmentCnt = segmentCnt;
    bfAttrs.attr.stripe.segmentSize = 8;  /* FIX - reasonable default for now */

    sts = advfs_set_bf_attributes( fd, &bfAttrs );
    if (sts != EOK) {
        fprintf( stderr, catgets(catd, 1, 5, "%s: advfs_set_bf_attributes failed --- %s\n"),
                Prog, BSERRMSG( sts ));
        errorFlag = 1;
    }

    if (O_close( fd )) {
        fprintf( stderr, catgets(catd, 1, 6, "%s: close of %s failed. [%d] %s\n"),
                Prog, path, errno, sys_errlist[errno] );
        errorFlag = 1;
    }

#ifndef _OSF_SOURCE
    fs_dmo();
#endif

    if (errorFlag)
        return 1;

    return 0;

}  /* end main */

/* end stripe.c */
