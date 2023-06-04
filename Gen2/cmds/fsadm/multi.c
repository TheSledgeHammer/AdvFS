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
 *    multi.c - convert a single-volume filesystem to a multi-volume
 *              capable filesystem.
 *              "rename" is a synonym for multi, insofar as the code
 *              paths are identical.
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
#include <advfs/advfs_syscalls.h>

#include "common.h"

#define MSG(n,s) catgets(catd,MS_FSADM_MULTI,n,s)

extern char     *sys_errlist[];
static char*    Prog;
static char*    multi_prog = "fsadm multi";
static char*    rename_prog = "fsadm rename";

/* Function prototypes */
static int
real_main(int argc, char **argv);

static void
dousage(void);

/*********************************************************************/

static void
dousage(void)
{
    usage(MSG(MULTI_USAGE, "%s [-V] special sdname\n"), Prog);
    return;
}

void
multi_usage(void)
{

    Prog = multi_prog;
    dousage();
    return;
}

void
rename_usage(void)
{
    Prog = rename_prog;
    dousage();
    return;
}

int
multi_main(int argc, char **argv)
{
    int    ret;

    Prog = multi_prog;
    
    ret = real_main(argc, argv);

    return ret;
}

int
rename_main(int argc, char **argv)
{
    int    ret;

    Prog = rename_prog;

    ret = real_main(argc, argv);

    return ret;
}

static int
real_main(int argc, char **argv)
{
    char*                devspec = NULL;
    char*                sdName = NULL;
    char*                ptr;
    struct stat          stats;
    arg_infoT*           infop = NULL;
    arg_infoT*           new_infop = NULL;
    adv_mnton_t*         mntArray = NULL;
    int                  ret = 0;
    int                  lockFd;
    int                  globalLockFd;
    char                 lkPath[MAXPATHLEN+1] = {0};
    char                 sdPath[MAXPATHLEN+1] = {0};
    char                 devnam[MAXPATHLEN+1];
    char                 tmpnam[MAXPATHLEN+1];
    DIR*                 dirp = NULL;
    struct dirent*       d;
    char                 c;
    int                  Vflg = 0;

    extern int           optind;
    extern int           opterr;
    extern char*         optarg;

    /* Must be privileged to run */
    check_root( Prog );

    /*
     *   Get user-specified command line arguments.
     */

    while ((c = getopt( argc, argv, "V" )) != EOF) {
        switch (c) {

        case 'V':
            Vflg++;
            break;
        default:
            dousage();
            return(1);
        }
    }

    if ((argc - optind) < 2) {
        dousage();
        return(1);
    }

    /* if we were passed the -V flag, print completed command line
     * and exit */
    if(Vflg) {
        printf("%s", Voption(argc, argv));
        return(0);
    }

    devspec = argv[optind];
    sdName = basename(argv[optind+1]);

    /*  Don't allow fsnames to begin with a "." */

    if (*sdName == '.') {
        fprintf(stderr, CMSG(COMMON_NODOT,
            "%s: Storage domain name must not begin with a \".\"\n"),
            Prog);
        return(1);
    }

    if (ptr = strpbrk(sdName, "/# :*?\t\n\f\r\v")) {
        fprintf(stderr, CMSG(COMMON_BADCHAR,
            "%s: invalid character '%c' in storage domain name '%s'\n"),
            Prog, *ptr, sdName);
        return(1);
    }

    infop = process_advfs_arg(devspec);
    
    if (infop == NULL) {
        fprintf(stderr, CMSG(COMMON_NOTADVFSSD,
            "%s: %s is not a device or AdvFS storage domain.\n"),
            Prog, devspec);
        return(1);
    }

    /* take out the global lock */
    globalLockFd = advfs_global_lock(FALSE, &ret);
    if (globalLockFd < 0) {
        fprintf (stderr, CMSG(COMMON_ERRGLOBAL,
            "%s: Error locking AdvFS global lock\n"),
            Prog);
        free(infop);
        return(1);
    }

    lockFd = advfs_fspath_lock(infop->fspath, TRUE, FALSE, &ret);
    if (lockFd < 0) {
        fprintf (stderr, CMSG(COMMON_ERRLOCK,
            "%s: Error locking '%s'; [%d] %s\n"),
            Prog, infop->fspath, ret, ERRMSG(ret));
        (void)advfs_unlock(globalLockFd, &ret);
        free(infop);
        return(1);
    }

    /* save away the lockfile name */
    snprintf(tmpnam, MAXPATHLEN+1, "%s/../%s_%s", infop->fspath, 
        DOT_ADVFS_LOCK, infop->fsname);
    ptr = realpath(tmpnam, lkPath);
    if (ptr == NULL) {
        lkPath[0] = '\0';
    }

    /*  check to make sure nothing from this storage domain is mounted */
    ret = advfs_get_all_mounted(infop, &mntArray);
    if (ret > 0) {
        /* print out list of mounted filesystems */
        int i;

        for (i = 0 ; i < ret ; i++) {
            fprintf(stderr, CMSG(COMMON_DISM, 
              "%s: File system(s) must be dismounted prior to modification: \n"), Prog);
            fprintf(stderr, CMSG(COMMON_FSMOUNTED, "\t%s is mounted on %s\n"),
                mntArray[i].fspath, mntArray[i].mntdir);
        }
        (void)advfs_unlock(globalLockFd, &ret);
        (void)advfs_unlock(lockFd, &ret);
        free(infop);
        free(mntArray);
        return(1);
    }

    new_infop = process_advfs_arg(sdName);
    if (new_infop != NULL) {
        /*  Whoops, this filesystem name is already in use */
        fprintf(stderr, MSG(MULTI_DUP,
            "%s: Storage domain name %s already in use.\n"), Prog, sdName);
        advfs_unlock(globalLockFd, &ret);
        advfs_unlock(lockFd, &ret);
        free(infop);
        return(1);
    }

    /*  See if we're running on a TCR/CFS cluster.  If we are, 
     *  we need to check additional directories to ensure uniqueness
     *  for a TCR-shared domain.
     */

    if (infop->tcr_shared) {
        ret = tcr_fsname_in_use(sdName);
        if (ret > 0) {
            fprintf(stderr, CMSG(COMMON_DUPTCR, 
                "%s: Storage domain name %s already in use on member %d.\n"), 
                Prog, sdName, ret);
            advfs_unlock(globalLockFd, &ret);
            advfs_unlock(lockFd, &ret);
            free(infop);
            return(1);
        }
    }

    strcpy(sdPath, infop->fspath);
    ptr = strrchr(sdPath, '/');
    *(++ptr) = '\0';
    strcat(sdPath, sdName);

    ret = rename(infop->fspath, sdPath);
    if (ret != 0) {
        fprintf(stderr, MSG(MULTI_BADRENAME, 
            "%s: Could not rename AdvFS storage domain %s to %s; [%d] %s\n"),
            Prog, infop->fsname, sdName, errno, sys_errlist[errno]);
        advfs_unlock(globalLockFd, &ret);
        advfs_unlock(lockFd, &ret);
        free(infop);
        return(1);
    }
 
    /*  Release lock on the old filesystem */
    advfs_unlock(lockFd, &ret);

    /*  Remove the old lock file */
    if (lkPath[0] != '\0') {
        unlink(lkPath);
    }

    free(infop);

    infop = process_advfs_arg(sdName);
    if (infop == NULL) {
        /* eh?  what happened? */
        fprintf(stderr, MSG(COMMON_NO_SD, 
            "%s: storage domain '%s' does not exist\n"),
            Prog, sdName);
        return(1);
    }

    /*  Take out lock on the new filesystem */
    lockFd = advfs_fspath_lock(infop->fspath, TRUE, FALSE, &ret);
    if (lockFd < 0) {
        fprintf (stderr, CMSG(COMMON_ERRLOCK,
            "%s: Error locking '%s'; [%d] %s\n"),
            Prog, infop->fspath, ret, ERRMSG(ret));
        return(1);
    }

    /* release the global lock */
    advfs_unlock(globalLockFd, &ret);

    /*  Set the multi-enabled bit in each device's superblock */
    dirp = opendir(infop->stgdir);
    if (dirp == NULL) {
        fprintf(stderr, MSG(RF_BADOPEN, "%s: Could not open %s.\n"),
            Prog, infop->stgdir);
        advfs_unlock(lockFd, &ret);
        return(1);
    }

    while ((d = readdir(dirp)) != NULL) {
        if ( (strcmp(d->d_name, ".") == 0) ||
             (strcmp(d->d_name, "..") == 0) ) {
                continue;
        }

        snprintf(tmpnam, MAXPATHLEN, "%s/%s", infop->stgdir, d->d_name);

        ptr = realpath(tmpnam, devnam);
        if (ptr == NULL) {
            continue;
        }

        ret = stat(devnam, &stats);

        if ((ret == 0) && S_ISBLK(stats.st_mode)) {
            ret = advfs_set_multivol(devnam);

            if (ret) {
                fprintf(stderr, MSG(MULTI_EMULTI,
                    "%s: Could not set file system %s/%s to multivolume, error writing %s [%d]\n"), Prog, infop->fsname, infop->fset, devnam);
            }
            break;
        }
    }

    closedir(dirp);

    advfs_unlock(lockFd, &ret);
    free(infop);

    return 0;
}
