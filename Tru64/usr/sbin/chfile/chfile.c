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
 *      'change file' utility.
 *
 * Date:
 *
 *      Wed Dec  9 11:07:33 1992
 *
 */
/*
 * HISTORY
 */
#ifndef lint
static char rcsid[] = "@(#)$RCSfile: chfile.c,v $ $Revision: 1.1.57.1 $ (DEC) $Date: 2002/12/18 21:24:22 $";
#endif

#include <msfs/msfs_syscalls.h>
#include <msfs/bs_error.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>
#include <pwd.h>
#include <grp.h>
#include <ctype.h>
#include <fcntl.h>
#include <sys/types.h>
#include <unistd.h>
#include <sys/file.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <sys/param.h>
#include <sys/mode.h>
#include <locale.h>
#include "chfile_msg.h"

nl_catd catd;
extern int errno;
extern char *sys_errlist[];

#define TRUE 1
#define FALSE 0
#define ERRMSG( e ) sys_errlist[e]

char *Prog;

void usage( void )
{
    fprintf( stderr, catgets(catd, 1, USAGE, "usage: %s {[-l on | off] | [-L on | off]} filename ...\n"), Prog );
}

main( int argc, char *argv[] )
{
    char *fileName = NULL;
    int c, i, l_flag = 0, L_flag = 0,
        fd, fileOpen = FALSE;
    char *p;
    char *slashp;
    mlStatusT sts;
    extern int optind;
    extern char *optarg;
    mlBfInfoT bfInfo;
    struct stat stats;
    char *onOff;
    advfs_opT advfs_op;
    int force_sync_writes = 0,
        atomic_write_data_logging = 0,
        saved_errno;
    int on_disk_io_type, in_memory_io_type;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_CHFILE, NL_CAT_LOCALE);

    /* store only the file name part of argv[0] */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) {
        Prog = argv[0];
    } else {
        Prog++;
    }
 
    /*
     ** Get user-specified command line arguments.
     */
 
    while ((c = getopt( argc, argv, "l:L:" )) != EOF) {
        switch (c) {
            case 'l':
                l_flag++;
                onOff = optarg;

                if (strcmp( onOff, "on") == 0) {
                    force_sync_writes = TRUE;
                } else if (strcmp( onOff, "off") == 0) {
                    force_sync_writes = FALSE;
                } else {
                    usage();
                    exit( 1 );
                }
                break;

            case 'L':
                L_flag++;
                onOff = optarg;

                if (strcmp( onOff, "on") == 0) {
                    atomic_write_data_logging = TRUE;
                } else if (strcmp( onOff, "off") == 0) {
                    atomic_write_data_logging = FALSE;
                } else {
                    usage();
                    exit( 1 );
                }
                break;

            case '?':

            default:
                usage();
                exit( 1 );
        }
    }

    if ((l_flag > 1 || L_flag > 1) ||
        (l_flag && L_flag)) {
        usage();
        exit( 1 );
    }

    if ((argc - 1 - optind) < 0) {
        /* missing required args */
        usage();
        exit( 1 );
    }

    for (i = optind; i < argc; i++) {
        fileName = argv[i];

        if (lstat( fileName, &stats ) < 0) {
            fprintf( stderr, catgets(catd, 1, 2, "%s: error getting file stats for '%s'; %s\n"), 
                     Prog, fileName, ERRMSG( errno ) );
            goto _error;
        }

        if (!S_ISREG( stats.st_mode )) {
            fprintf( stderr, catgets(catd, 1, 3, "%s: '%s' must be a regular file\n"), 
                     Prog, fileName );
            goto _error;
        }

        fd = open( fileName, O_RDONLY, 0 );
        if (fd < 0) {
            fprintf( stderr, catgets(catd, 1, 4, "%s: open of %s failed; %s\n"),
                    Prog, fileName, ERRMSG( errno ) );
            goto _error;
        }

        fileOpen = TRUE;
        advfs_op.version = 1;
    
        /*
         * Get the on-disk I/O mode for the l_flag and L_flag tests below.
         *
         * Note: setting operation to ADVFS_AW_DATA_LOGGING is somewhat
         * arbitrary.  ADVFS_SYNC_WRITE would work as well.  Both will
         * return the current I/O mode setting.
         */
        advfs_op.operation = ADVFS_AW_DATA_LOGGING;
        advfs_op.action    = ADVFS_GET_INFO;
        advfs_op.info_buf  = &on_disk_io_type;
        advfs_op.info_buf_size = sizeof(int);

        sts = fcntl(fd, F_ADVFS_OP, &advfs_op);
        if (sts != EOK) {
            saved_errno = errno;
            fprintf(stderr, catgets(catd, 1, 19,
                "%s: error getting file attributes for '%s'; %s\n"),
                Prog, fileName, ERRMSG(errno));
            if (saved_errno == ENOTSUP) {
                fprintf(stderr, catgets(catd, 1, 15,
                    "Are you trying to get information about a non-AdvFS file?\n"));
            }
            goto _error;
        }

        /*
         * No flags specified.  Show current I/O mode setting.
         */
        if (!L_flag && !l_flag) {
            /*
             * Usually, a file's in-memory I/O mode (dataSafety) is
             * the same as its on-disk I/O mode.  The only exception
             * is when the "-o adl" mount option is used.  In that
             * case, the in-memory I/O mode is set to be "data logging"
             * if the on-disk I/O mode is "normal, asynchronous writes".
             * So we query both the on-disk and in-memory I/O modes
             * and report a difference, if there is one.
             */

            /*
             * So, query the in-memory I/O mode, stored in the dataSafety
             * field of the access structure.
             */
            advfs_op.action    = ADVFS_GET_IN_MEM_INFO;
            advfs_op.info_buf  = &in_memory_io_type;
            sts = fcntl(fd, F_ADVFS_OP, &advfs_op);
            if (sts != EOK) {
                saved_errno = errno;
                fprintf(stderr, catgets(catd, 1, 26,
                    "%s: error getting in-memory file attributes for '%s'; %s\n"),
                    Prog, fileName, ERRMSG(errno));
                goto _error;
            }

            printf(catgets(catd, 1, 16, "I/O mode = ")); 
            switch (on_disk_io_type) {
                case ADVFS_ASYNC_IO:
                    if (on_disk_io_type == in_memory_io_type) {
                        printf(catgets(catd, 1, 20, 
                               "normal asynchronous writes\n") );
                    }
                    else {
                        printf(catgets(catd, 1, 23,
                               "normal asynchronous writes (permanent)\n") );
                        switch (in_memory_io_type) {
                            case ADVFS_TEMPORARY_DATA_LOGGING_IO:
                                printf(catgets(catd, 1, 24, 
                                       "           atomic write data logging (temporarily active)\n") );
                                break;
                            default:
                                printf(catgets(catd, 1, 25, "unexpected in-memory I/O mode: %d\n"), in_memory_io_type);
                        }
                    }
                    break;
                case ADVFS_DATA_LOGGING_IO:
                    printf(catgets(catd, 1, 10, 
                           "atomic write data logging\n") );
                    break;
                case ADVFS_SYNC_IO:
                    printf(catgets(catd, 1, 11, 
                           "forced synchronous writes\n") );
                    break;
                default:
                    printf(catgets(catd, 1, 12, "unknown\n") );
            }
        }

        /*
         * Forced synchronous writes request.
         */
        if (l_flag) {
            if (on_disk_io_type == ADVFS_SYNC_IO && force_sync_writes) {
                fprintf(stderr, catgets(catd, 1, 21,
                   "Forced synchronous writes has already been activated.\n"));
                goto _error;
            }
            advfs_op.operation = ADVFS_SYNC_WRITE;
            advfs_op.action = force_sync_writes ? ADVFS_ACTIVATE :
                                                  ADVFS_DEACTIVATE;
            sts = fcntl(fd, F_ADVFS_OP, &advfs_op);
            if (sts != EOK) {
                saved_errno = errno;
                fprintf(stderr, catgets(catd, 1, 17, 
                        "%s: error setting file attributes for '%s'; %s\n"), 
                        Prog, fileName, ERRMSG(errno));
                if (!force_sync_writes) {
                    fprintf(stderr, catgets(catd, 1, 18,
                            "Are you trying to turn off a file attribute that is not turned on?\n"));
                }
                else if (saved_errno == ENOTSUP) {
                    fprintf(stderr, catgets(catd, 1, 14, 
                            "Are you trying to activate forced synchronous writes on a non-AdvFS file?\n"));
                }
                goto _error;
            }
        }

        /*
         * Atomic write data logging request.
         */
        if (L_flag) {
            if (on_disk_io_type == ADVFS_DATA_LOGGING_IO && 
                atomic_write_data_logging) {
                fprintf(stderr, catgets(catd, 1, 22,
                   "Atomic write data logging has already been activated.\n"));
                goto _error;
            }
            advfs_op.operation = ADVFS_AW_DATA_LOGGING;
            advfs_op.action = atomic_write_data_logging ? ADVFS_ACTIVATE :
                                                          ADVFS_DEACTIVATE;
            sts = fcntl(fd, F_ADVFS_OP, &advfs_op);
            if (sts != EOK) {
                saved_errno = errno;
                fprintf(stderr, catgets(catd, 1, 17, 
                        "%s: error setting file attributes for '%s'; %s\n"), 
                        Prog, fileName, ERRMSG(errno));
                if (!atomic_write_data_logging) {
                    fprintf(stderr, catgets(catd, 1, 18,
                            "Are you trying to turn off a file attribute that is not turned on?\n"));
                }
                else if (saved_errno == ENOTSUP) {
                    /* See if the cache policy is FDIRECTIO (=1). */
                    int temp;
                    sts = fcntl(fd, F_GETCACHEPOLICY, &temp);
                    if (sts == 1) {
                        /* FDIRECTIO is the cache policy. */
                        fprintf(stderr, catgets(catd, 1, 27,
                            "Atomic write data logging cannot be enabled with direct I/O\n"));
                    } else if (sts != -1) {
                        fprintf(stderr, catgets(catd, 1, 13,
                            "Are you trying to activate atomic write data logging on a non-AdvFS file or\non an AdvFS file which has been memory-mapped via mmap() or which has a frag?\n"));
                    } else {
                        fprintf(stderr, catgets(catd, 1, 28,
                            "Are you trying to activate atomic write data logging on a non-AdvFS file, or\non an AdvFS file which has direct I/O enabled, or on an AdvFS file which has\nbeen memory-mapped via mmap() or which has a frag?\n"));
                    }
                }
                goto _error;
            }
        }

        if (fileOpen) {
            close( fd );
            fileOpen = FALSE;
        }
    }

    return 0;

_error:

    if (fileOpen) {
        close( fd );
        fileOpen = FALSE;
    }

    return 1;
}

/* end chfile.c */
