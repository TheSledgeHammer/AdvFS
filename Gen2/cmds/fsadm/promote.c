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
 *    promote.c - For TCR clusters only.  Makes a member-specific 
 *                filesystem available cluster-wide.
 *
 *                If the environment variable CLU_SPECIAL_PROMOTE=1,
 *                we're being called from clu_create, so we skip the
 *                clu_type check to allow cluster-wide filesystems to
 *                be promoted.
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
#include <advfs/advfs_syscalls.h>
#include <tcr/clu.h>

#include "common.h"

#define MSG(n,s) catgets(catd,MS_FSADM_PROMOTE,n,s)

extern char     *sys_errlist[];
static char*    Prog;
static char*    promote_prog = "fsadm promote";
static char*    demote_prog = "fsadm demote";
static int      ispromote;
static char*    target_top_dir;

/* Function prototypes */
static int
dopromote(int argc, char **argv);

static void
dousage(void);

/*********************************************************************/

static void
dousage(void)
{
    usage(MSG(PROMOTE_USAGE, "%s [-V] sdname\n"), Prog);
    return;
}

void
promote_usage(void)
{
    Prog = promote_prog;
    dousage();
    return;
}

void
demote_usage(void)
{
    Prog = demote_prog;
    dousage();
    return;
}

int
promote_main(int argc, char **argv)
{
    int    ret;

    ispromote = 1;
    target_top_dir = ADVFS_TCR_DMN_DIR;

    Prog = promote_prog;
    
    ret = dopromote(argc, argv);

    return ret;
}

int
demote_main(int argc, char **argv)
{
    int    ret;

    ispromote = 0;
    target_top_dir = ADVFS_DMN_DIR;

    Prog = demote_prog;

    ret = dopromote(argc, argv);

    return ret;
}

static int
dopromote(int argc, char **argv)
{
    char*                sdName = NULL;
    char*                ptr;
    struct stat          stats;
    arg_infoT*           infop = NULL;
    adv_mnton_t*         mntArray = NULL;
    int                  ret = 0;
    int                  globalLockFd;
    char                 lkPath[MAXPATHLEN+1] = {0};
    char                 sdPath[MAXPATHLEN+1] = {0};
    char                 devnam[MAXPATHLEN+1];
    char                 tmpnam[MAXPATHLEN+1];
    DIR*                 dirp = NULL;
    struct dirent*       d;
    char                 c;
    int                  Vflg = 0;
    clu_type_t           clutype = CLU_TYPE_UNKNOWN;
    char*                rmCmd = "/bin/rm -rf";
    char                 dormCmd[MAXPATHLEN+12];
    char*                special;

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

    if ((argc - optind) != 1) {
        dousage();
        return(1);
    }

    /* check for special clu_create envvar */
    special = getenv("CLU_SPECIAL_PROMOTE");

    if ((special == NULL) || (strcmp(special, "1"))) {
        clutype = clu_type();

        if (clutype != CLU_TYPE_CFS) {
            fprintf(stderr, MSG(PROMOTE_NOTTCR, 
                "%s:  This system is not a cluster member\n"), Prog);
            return(1);
        }
    }

    sdName = argv[optind]; 

    infop = process_advfs_arg(sdName);
    
    if (infop == NULL) {
        fprintf(stderr, CMSG(COMMON_NOTADVFSSD,
            "%s: %s is not a device or AdvFS storage domain.\n"),
            Prog, sdName);
        return(1);
    }

    /* if we were passed the -V flag, print completed command line
     * and exit */
    if(Vflg) {
        printf("%s", Voption(argc, argv));
        return(0);
    }

    if ((ispromote) && (infop->tcr_shared)) {
        fprintf(stderr, MSG(PROMOTE_ISSHARED,
            "%s:  Storage domain %s is already cluster-shared.\n"),
            Prog, sdName);
        free(infop);
        return(1);
    }

    if ((!ispromote) && (!infop->tcr_shared)) {
        fprintf(stderr, MSG(PROMOTE_ISSPECIFIC,
            "%s:  Storage domain %s is already member-specific.\n"),
            Prog, sdName);
        free(infop);
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

    /*  check to make sure nothing from this storage domain is mounted */
    if ((special == NULL) || (strcmp(special, "1"))) {
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
            free(infop);
            free(mntArray);
            return(1);
        }
    }

    /*  If promoting, we need to check additional directories to ensure 
     *  name uniqueness for a TCR-shared domain.
     */

    if (ispromote) {
        ret = tcr_fsname_in_use(infop->fsname);
        if (ret > 0) {
            fprintf(stderr, CMSG(COMMON_DUPTCR, 
                "%s: Storage domain name %s already in use on member %d.\n"), 
                Prog, sdName, ret);
            advfs_unlock(globalLockFd, &ret);
            free(infop);
            return(1);
        }
    }

    /* save away the lockfile name */
    snprintf(tmpnam, MAXPATHLEN+1, "%s/../%s_%s", infop->fspath,
        DOT_ADVFS_LOCK, infop->fsname);
    ptr = realpath(tmpnam, lkPath);
    if (ptr == NULL) {
        lkPath[0] = '\0';
    }


    /* Ok, let's create the [un]promoted version */
    snprintf(sdPath, MAXPATHLEN+1, "%s/%s", target_top_dir, infop->fsname);
    snprintf(dormCmd, sizeof(dormCmd), "%s %s", rmCmd, sdPath);

    if (mkdir(sdPath, 0755)) {
        fprintf(stderr, MSG(PROMOTE_ERRMKDIR, 
            "%s:  cannot create dir '%s'; [%d] %s\n"), 
            Prog, sdPath, errno, strerror(errno));
        advfs_unlock(globalLockFd, &ret);
        free(infop);
        return(1);
    }

    /* need to make new advfsdevs for each fileset */
    dirp = opendir(infop->fspath);
    if (dirp == NULL) {
        fprintf(stderr, CMSG(COMMON_NOOPEN, 
            "%s: open of %s failed. [%d] %s\n"),
            Prog, infop->fspath, errno, strerror(errno));
        unlink(sdPath);
        advfs_unlock(globalLockFd, &ret);
        free(infop);
        return(1);
    }

    while ((d = readdir(dirp)) != NULL) {
        /* filesets cannot start with "." */
        if (d->d_name[0] == '.') {
            /* continue */
        }

        snprintf(tmpnam, MAXPATHLEN, "%s/%s", infop->fspath, d->d_name);

        ret = stat(tmpnam, &stats);
        if (ret != 0) {
            continue;
        }

        if (major(stats.st_rdev) == ADVFSDEV_MAJOR) {
            ret = advfs_make_blockdev(sdPath, d->d_name, ispromote, 0);

            if (ret != 0) {
                fprintf(stderr, MSG(PROMOTE_ERRDEV,
                    "%s:  cannot create AdvFS device %s/%s\n"),
                    Prog, sdPath, d->d_name);
            
                closedir(dirp);
                system(dormCmd);
                advfs_unlock(globalLockFd, &ret);
                free(infop);
                return(1);
            }
        }
    }
    closedir(dirp);

    /*  Now move the storage */
    snprintf(sdPath, MAXPATHLEN+1, "%s/%s/%s", target_top_dir, infop->fsname, 
        DOT_ADVFS_STG);

    if (mkdir(sdPath, 0755)) {
        fprintf(stderr, MSG(PROMOTE_ERRMKDIR, 
            "%s:  cannot create dir '%s'; [%d] %s\n"), 
            Prog, sdPath, errno, strerror(errno));
        system(dormCmd);
        advfs_unlock(globalLockFd, &ret);
        free(infop);
        return(1);
    }

    dirp = opendir(infop->stgdir);
    if (dirp == NULL) {
        fprintf(stderr, CMSG(COMMON_NOOPEN, 
            "%s: open of %s failed. [%d] %s\n"),
            Prog, infop->stgdir, errno, strerror(errno));
        system(dormCmd);
        advfs_unlock(globalLockFd, &ret);
        free(infop);
        return(1);
    }

    while ((d = readdir(dirp)) != NULL) {
        if (d->d_name[0] == '.') {
            continue;
        }

        snprintf(tmpnam, MAXPATHLEN, "%s/%s", infop->stgdir, d->d_name);

        ptr = realpath(tmpnam, devnam);
        if (ptr == NULL) {
            /* bad link, most likely */
            continue;
        }

        /* LVM devices can't be cluster shared at this time */
        if (ispromote && (advfs_is_lvmdev(devnam))) {
            fprintf(stderr, MSG(COMMON_TCRLVM, 
                "%s:  Volume manager device found (%s).\nVolume manager devices are not permitted in a cluster shared filesystem.\n"),
                Prog, devnam);

            system(dormCmd);
            advfs_unlock(globalLockFd, &ret);
            free(infop);
            return(1);
        }
        
        snprintf(tmpnam, MAXPATHLEN, "%s/%s", sdPath, d->d_name);

        if (symlink(devnam, tmpnam)) {
            fprintf(stderr, MSG(PROMOTE_ERRLINK, 
                "%s:  cannot create symlink '%s @--> %s'; [%d] %s\n"),
                Prog, tmpnam, devnam, errno, strerror(errno));
            system(dormCmd);
            advfs_unlock(globalLockFd, &ret);
            free(infop);
            return(1);
        }
    }
    closedir(dirp);

    /*  That's it.  Get rid of the old version */
    snprintf(dormCmd, sizeof(dormCmd), "%s %s", rmCmd, infop->fspath);
    system(dormCmd);

    /*  and the old lock file */
    if (lkPath[0] != '\0') {
        unlink(lkPath);
    }

    free(infop);

    /* release the global lock */
    advfs_unlock(globalLockFd, &ret);

    return 0;
}
