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

/* INCLUDES */
#include "common.h"   /* This pulls in everything we need */

/* GLOBALS */
static char *Prog = "fsadm extend";

/* PROTOTYPES */
void extend_usage(void);


/*
 *      NAME:
 *              extend_main()
 *
 *      DESCRIPTION:
 *              main routine for fsadm extend command
 *              This utility will extend the size of volume(s) in an Advfs
 *              filesystem.
 *
 *      ARGUMENTS:
 *              argc            number of command line arguments
 *              argv            command line arguments
 *
 *      RETURN VALUES: 
 *              success         0
 *              failure         non-zero
 */
int
extend_main(int argc, char **argv)
{
    extern char *optarg;
    extern int optind;
    uint64_t size = -1;
    int Vflg = 0, nflg = 0;
    int ch = 0;

    check_root(Prog);

    while ((ch = getopt(argc, argv, "Vn:")) != EOF) {
        switch((char)ch) {
        case 'V':
            Vflg++;
            break;
        case 'n':
            nflg++;
             /* Force negative values to 0 to trigger error */
            size = atol(optarg)>0 ? atol(optarg) : 0;
            if(size == 0) {
                extend_usage();
                return(1);
            }
            break;
        default:
            extend_usage();
            return(1);
        }
    }

    /* skip the processed options */
    argc -= optind;
    argv += optind;

    if (argc == 0 || Vflg > 1 || nflg > 1 || argc > 2) {
        extend_usage();
        return(1);
    }

    /* print Voption if -V was specified */
    if(Vflg) {
        /* reset argc and argv */
        argc += optind;
        argv -= optind;

        /* print verified command line */
        printf("%s", Voption(argc, argv));
        return(0);
    }

    /* 
     * flags is 0; only when called from extendfs does this get 
     * called with additional flags (query and verbose).
     */
    return ( advfs_extendfs_generic(Prog, size, 0, argc, argv) );
}

void
extend_usage(void)
{
    usage(catgets(catd,
                  MS_FSADM_EXTEND,
                  EXTEND_USAGE,
                  "%s [-V] [-n size] special\n\t%s [-V] [-n size] [special] fsname\n"), Prog, Prog);
}
