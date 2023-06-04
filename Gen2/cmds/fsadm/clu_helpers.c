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
 * Copyright (c) 2004 Hewlett-Packard Development Company, L.P.
 *
 * Facility:
 *
 *    AdvFS - Advanced File System
 *
 * Abstract:
 *
 *    clu_helpers.c - Helper functions for use during clu_create
 *                    or by other non-AdvFS utilities.  The intent is
 *                    to keep 'internals' knowledge of /dev/advfs[_cfs]
 *                    and other AdvFS-isms with AdvFS so as to prevent
 *                    these other utilities from having to change in 
 *                    lock-step with AdvFS.
 *
 *
 */

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <errno.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <locale.h>
#include <syslog.h>
#include <libgen.h>
#include <stdlib.h>
#include <dirent.h>
#include <tcr/clu.h>
#include <advfs/advfs_syscalls.h>

#include "common.h"

static char*    Prog = "fsadm clu_helpers";

static char *myopts[] = {
#define UNIQUE          0
                        "unique",
                        NULL
};


/* Function prototypes */
static int 
check_unique(
    int      memId
);

/*********************************************************************/

void
helper_usage(void)
{
    usage("%s -m <memberid> -o <options>\n", Prog);
    return;
}

/*
 *  Options:
 *
 *     -m <memberno> 	Cluster member id
 *     -o unique	Verifies that member-specific filesystems for
 *            		a given member being added to a cluster do not
 *                      conflict with any cluster_shared filesystems.
 *
 */
int
helper_main(int argc, char **argv)
{
    int                  st = 0;
    char                 c;
    long                 memId = -1;
    char*                badchars = NULL;
    char*                suboptp = NULL;
    char*                optval = NULL;
    int                  dounique = 0;

    extern int           optind;
    extern int           opterr;
    extern char*         optarg;

    /* Must be privileged to run */
    check_root( Prog );

    /*
     *   Get user-specified command line arguments.
     */

    while ((c = getopt( argc, argv, "o:m:" )) != EOF) {
        switch (c) {

        case 'o':
            suboptp = optarg;
            while (*suboptp != '\0') {
                switch(getsubopt(&suboptp, myopts, &optval)) {
                    case UNIQUE:
                        dounique++;
                        break;
                    default:
                        break;
                }
            }
            break;
        case 'm':
            memId = strtol(optarg, &badchars, 10);
            if (((memId == 0) && (errno != 0)) || (*badchars != '\0')) {
                helper_usage();
                return(1);
            }
            break;
        default:
            helper_usage();
            return 1;
        }
    }

    if (dounique && (memId != -1)) {
        st = check_unique(memId);
    }

    return st;
}

/*
 *  Verifies that there are no conflicts between member-specific fsnames
 *  and cluster-shared fsnames.  Most errors are ignored to keep scripting
 *  simple.
 */
static int 
check_unique(
    int      memId
)
{
    int                  st;
    int                  globalLock = -1;
    struct dirent*       fsdir;
    DIR*                 dirp = NULL;
    char                 tmppath[MAXPATHLEN+1];
    struct stat          statBuf;
    int                  found = 0;
    
    /* Only valid in a TCR CFS cluster */
    if (clu_type() != CLU_TYPE_CFS) {
        return 0;
    }

    /* take out the global - not an error if we can't get it, just 
     * a precaution
     */
    globalLock = advfs_global_lock(FALSE, &st);

    dirp = opendir(ADVFS_TCR_DMN_DIR);

    if (dirp == NULL) {
        if (globalLock != -1) {
            advfs_unlock(globalLock, &st);
        }
        return 0;
    }

    while ((fsdir = readdir(dirp)) != NULL) {
        if (fsdir->d_name[0] == '.') {
            continue;
        }
      
        sprintf(tmppath, ADVFS_CLU_MEMBER_PATH"/%s", memId, fsdir->d_name);

        st = stat(tmppath, &statBuf);
        if (!st) {
            printf("%s\n", fsdir->d_name);
            found++;
        }
    }

    closedir(dirp);

    /* release the global lock */
    if (globalLock != -1) {
        advfs_unlock(globalLock, &st);
    }

    return found;
}

