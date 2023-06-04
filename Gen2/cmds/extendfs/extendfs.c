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
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <mntent.h>
#include <locale.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <advfs/advfs_syscalls.h>

#include "extendfs_advfs_msg.h" 

nl_catd catd;
#define MSGSTR(n,s) catgets(catd,MS_EXTENDFS,n,s)

static char *Prog = "extendfs";

int	qflg = 0;
int	sflg = 0;
int	Fflg = 0;
int	vflg = 0;

/* local prototypes */
void usage(void);


int
main(int argc, char **argv)
{
    extern char *optarg;
    extern int optind;
    uint64_t size = -1;
    int flags = 0;
    int ch = 0;

    setlocale( LC_ALL, "" );
    catd = catopen(MF_EXTENDFS_ADVFS,NL_CAT_LOCALE);

#define NL_SETN 2
    /* verify user is root */
    if (geteuid() != 0) {
        fprintf( stderr,
                 MSGSTR(BADUSER,
                 "%s: error, permission denied - privileged user required\n"),
                Prog);
        exit(1);
    }
        
    while ((ch = getopt(argc, argv, "qvs:F:")) != EOF)
        switch((char)ch) {
        case 'q':
            qflg++;
            flags |= EXT_QFLAG;
            break;
        case 'v':
            vflg++;
            flags |= EXT_VFLAG;
            break;
        case 's':
            sflg++;
            /* Force negative values to 0 to trigger error */
            size = atol(optarg)>0 ? atol(optarg) : 0;
            if(size == 0) {
                perror(Prog);
                usage();
                exit(1);
            }
            break;
        case 'F':
            if (strcmp(optarg, MNTTYPE_ADVFS)) {
                fprintf(stderr,
                        MSGSTR(BADFS,
                               "%s: %s is not a valid file system type\n"),
                        Prog, optarg);
                usage();
                exit(1);
            }
            Fflg++;
            break;
        default:
            usage();
            exit(1);
        }
  
    /* skip the processed options */
    argc -= optind;
    argv += optind;
	
    if (argc == 0 || sflg > 1 || vflg > 1 || qflg > 1 || Fflg > 1 ||
        (qflg && sflg)) {
        usage();
        exit(1);
    }

    return (advfs_extendfs_generic(Prog, size, flags, argc, argv));
}

void
usage(void)
{
    fprintf(stderr,
            MSGSTR(USAGE,
                   "usage: %s [-F advfs] [-q | -s size] [-v] {special | fsname}...\n"), 
            Prog);
}

