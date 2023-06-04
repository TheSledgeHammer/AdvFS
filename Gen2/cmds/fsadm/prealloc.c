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
 *
 * Facility:
 *
 *    AdvFS - Advanced File System
 *
 * Abstract:
 *
 *    prealloc.c - preallocate file storage for a new file.
 *
 */
#include <string.h>
#include <fcntl.h>
#include <stropts.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <sys/fs/advfs_ioctl.h>

#include "common.h"

static char *Prog = "fsadm prealloc";


/*
 *  NAME:
 *    parse_options()
 *
 *  DESCRIPTION:
 *    parse_options takes the string of options provided by the user
 *    and sets the appropriate prealloc flags.  
 *
 *    parse_options returns TRUE for success and FALSE if an invalid
 *    argument is specified.
 *
 *  ARGUMENTS:
 *    options         (in)  options list from prealloc_main()
 *    preallocOptions (out) bit flag for user specified options
 *
 *  RETURN VALUES:
 *    success     TRUE
 *    Failure     FALSE
 *
 */

static int
parse_options(
    char *options,    /* in */
    uint64_t *preallocOptions /* out */
    )
{
    char *optptr;
    char *optbuf;

    optbuf = (char *) malloc(strlen(options)+1);
    if (!optbuf) {
        fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_NOMEM,
    "%s: insufficient memory.\n"), Prog );
        return FALSE;
    }
    strcpy(optbuf, options);

    for( optptr = strtok( optbuf, "," ); optptr;
         optptr = strtok( (char *)NULL, "," ) ) {

        if (!strcmp(optptr, "nozero")) {
	    /*
	     * user must be privileged to use nozero option
	     * This check is done in the kernel in advfs_setext_prealloc()
	     */
            *preallocOptions |= ADVFS_EXT_PREALLOC_NOZERO;
        }
        else if (!strcmp(optptr, "reserveonly")) {
            *preallocOptions |= ADVFS_EXT_PREALLOC_RESERVE_ONLY;
        }
        else {
            fprintf( stderr, catgets(catd, MS_FSADM_COMMON, COMMON_BADOARG,
                                     "%s: unknown -o argument '%s'.\n"), Prog, optptr );
            free(optbuf);
            return FALSE;
        }
    }

    free(optbuf);

    return TRUE;
}

void
prealloc_usage(void)
{
    usage(catgets(catd,
                  MS_FSADM_PREALLOC,
                  PREALLOC_USAGE,
                  "%s [-o option_list] file size\n"),
          Prog);
    return;
}

static uint64_t
str_to_bytes(char *usr_str)
{
    char *next_ptr = NULL;
    long user_size = 0;
    long num_bytes = 0;

    if (usr_str == NULL) {
        /* early exit if argument was NULL */
        return -1;
    }
    user_size = strtol(usr_str, &next_ptr, 10);
    if (next_ptr == NULL || strlen(next_ptr) > 1) {
        return -1;
    }
    switch((int)next_ptr[0]) {
    case '\0':
        num_bytes = user_size;
        break;
    case 'K':
    case 'k':
        num_bytes = user_size * 1024;
        break;
    case 'M':
    case 'm':
        num_bytes = user_size * 1024 * 1024;
        break;
    case 'G':
    case 'g':
        num_bytes = user_size * 1024 * 1024 * 1024;
        break;
    default:
        return -1;
    }

    return num_bytes;
}

int
prealloc_main(int argc, char **argv)
{
    int fd = 0;
    int err = 0;
    int c = 0;
    int Vflg = 0;
    int oflg = 0;
    char *options = NULL;
    advfs_ext_attr_t attr = { 0 };
    extern int optind;
    extern char *optarg;

    attr.type = ADVFS_EXT_PREALLOC;

    while((c = getopt( argc, argv, "Vo:" )) != EOF) {
        switch(c) {
        case 'V':
            Vflg++;
            break;
        case 'o':
            oflg++;
            options = strdup(optarg);
            break;
        default:
            prealloc_usage();
            exit(1);
        }
    }

    if( (argc - optind) != 2) {
        prealloc_usage();
        exit(1);
    }

    if( oflg ) {
        if((parse_options( options, &(attr.value.prealloc.flags) )) == FALSE) {
            prealloc_usage();
            exit(1);
        }
    }

    attr.value.prealloc.bytes = str_to_bytes(argv[optind + 1]);
    if(attr.value.prealloc.bytes < 0) {
        prealloc_usage();
        exit(1);
    }

    if((fd = open(argv[optind], O_RDWR | O_CREAT, 0666)) < 0) {
        perror(Prog);
        prealloc_usage();
        exit(1);
    }

    if((err = ioctl(fd, ADVFS_SETEXT, &attr)) != 0) {
        perror(Prog);
        close(fd);
        prealloc_usage();
        exit(1);
    }

    close(fd);

    return 0;
}

