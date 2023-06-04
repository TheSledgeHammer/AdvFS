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
 *
 * Abstract:
 *
 *      Advfs File system - Remove File system
 *
 */
#include <stdio.h>
#include <libgen.h>
#include <fcntl.h>
#include <signal.h>
#include <unistd.h>
#include <stdarg.h>
#include <errno.h>
#include <advfs/bs_error.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/advfs_evm.h>
#include <sys/fs.h>               /* used by valid_sblock() */
#include <string.h>

#include <locale.h>
#include "fsadm_advfs_msg.h"

#ifdef ESS
#include <ess/event_simulation.h>
#endif

nl_catd catd;
#define MSG_RF(n,s) catgets(catd,MS_FSADM_RMFS,n,s)
#define MSG_CM(n,s) catgets(catd,MS_FSADM_COMMON,n,s)

static advfs_ev advfs_event;

static char *Prog = "fsadm rmfs";

/* function prototypes */
static adv_status_t
rmfs_delete_recursive(
        arg_infoT*  infop,
        bfSetIdT    parent_set_id,
        bfSetIdT    first_child_set_id
);

static int
cleanup_dir (
    char*   dirPath,
    int     markSuperblock
);

/**************************************************************************/
void
rmfs_usage( void )
{
    usage(MSG_RF(RF_USAGE, "%s [-V] [-f] [-Rr] {special | fsname}\n"), 
            Prog );
}

int
rmfs_main ( int argc, char *argv[] )
{
    char            tmplockFile[MAXPATHLEN+1];
    char            lockFile[MAXPATHLEN+1];
    char*           lptr = NULL;
    char*           mntdir;
    int             thisFsLock = -1;
    int             do_not_ask = 0, c, i, n, err, mountCnt;
    int             just_do_it = 0;
    int             recurse = 0;
    int             ret, dirty;
    char            file_name[MAXPATHLEN+1];
    char            link_name[MAXPATHLEN+1];
    struct stat     stats;
    char            response[2];
    adv_status_t    sts = EOK;
    arg_infoT*      infop = NULL;
    bfSetIdT        set_id;
    bfSetParamsT    set_params, child_set_params;
    int             Vflag = 0;
    int             rem_stg = 0;

    extern int      optind;
    extern char*    optarg;

    /*  only a privileged user can run this utility */
    check_root(Prog);

    (void) signal( SIGINT, SIG_IGN ); /* ignore interrupt signals */

    /*
     ** Get user-specified command line arguments.
     */

    while ((c = getopt( argc, argv, "VfrR" )) != EOF) {
        switch (c) {
            case 'f':
                do_not_ask++;
                break;

            case 'V':
                Vflag++;
                break;

            case 'R':
            case 'r':
                recurse++;
                break;

            default:
                rmfs_usage();
                return 1;
        }
    }

    if (do_not_ask > 1 || recurse > 1 || Vflag > 1) {
        rmfs_usage();
        return 1;
    }

    if ((argc - optind) != 1) {
        /* missing required args or has too many */
        rmfs_usage();
        return 1;
    }

    infop = process_advfs_arg(argv[optind]);

    if (infop == NULL) {
        fprintf(stderr, 
                MSG_CM(COMMON_NO_SD, "%s: storage domain '%s' does not exist\n"), 
                Prog, argv[optind]);
        return 1;
    }

    if (Vflag) {
        printf("%s\n", Voption(argc, argv));
        return 0;
    }
    
    init_event(&advfs_event);
    advfs_event.domain = infop->fsname;

    /*
     * Lock this file system.  This does not prevent this utility
     * from deleting the file, and the lock will be released
     * when the utility exits.
     */

    thisFsLock = advfs_fspath_longrun_lock(infop->fspath, 
                                           FALSE, 
                                           FALSE, 
                                           &err);
    if (thisFsLock < 0) {
        if (err == EWOULDBLOCK) {
            fprintf (stderr,
                     MSG_CM(COMMON_SDBUSY, 
                         "%s: Cannot execute. Another AdvFS command is currently claiming exclusive use of the specified storage domain.\n"), 
                     Prog);
        } else {
            fprintf( stderr, 
                    MSG_CM(COMMON_ERRLOCK, "%s: Error locking '%s'; [%d] %s\n"),
                    Prog, infop->fspath, err, BSERRMSG( err ) );
        }
        goto _error;
    } 

    /*  First, check if anything's mounted.  No need to go any further
     *  if there is.
     */

    if (recurse && (strcmp(infop->fset, "default") == 0)) {
        just_do_it = 1;
    }

    if (just_do_it) {
        /*  check for all mounted */
        adv_mnton_t*   mntArray;

        ret = advfs_get_all_mounted(infop, &mntArray);

        if (ret > 0)  {
            for (i = 0 ; i < ret ; i++) {
                fprintf(stderr, MSG_RF(RF_ISMNT2, 
                        "%s: File system %s is mounted on %s. \n"),
                        Prog, mntArray[i].fspath, mntArray[i].mntdir);
            }
            fprintf( stderr, MSG_RF(RF_DISM, 
                "File system(s) must be dismounted prior to removal. \n"));
            free(mntArray);
            goto _error;
        }        
    } else {
        ret = advfs_is_mounted(infop, NULL, NULL, &mntdir);

        if (ret > 0) {
            fprintf(stderr, MSG_RF(RF_ISMNT, 
                        "%s: File system %s/%s is mounted on %s. \n"),
                    Prog, infop->fsname, infop->fset, mntdir);
            fprintf( stderr, MSG_RF(RF_DISM, 
                "File system(s) must be dismounted prior to removal. \n"));
            free(mntdir);
            goto _error;
        } 
    } 
		    
    /*  We need to determine if this set has snapshots.  However, if
     *  the fileset is invalid, we still need to clean up the rest 
     *  of the directory structure.  We can do this at will, as 
     *  an invalid bfset cannot have children.
     *
     *  If this is a recursive delete on "default", don't bother 
     *  calling into the kernel. 
     */

    if (!just_do_it) {
        sts = advfs_fset_get_id(infop->fsname, infop->fset, &set_id);
        if (sts && (sts != E_NO_SUCH_BF_SET) && (sts != ENO_SUCH_DOMAIN) && 
               (sts != E_BAD_MAGIC) && (sts != ENOENT) && (sts != EIO)) {
            fprintf (stderr, MSG_CM(COMMON_BADFSID,
                        "%s: Cannot get file set id.\n"), Prog);
            fprintf (stderr, MSG_CM(COMMON_FSFSNM,
                        "%s: File system name: %s, file set name: %s\n"),
                    Prog, infop->fsname, infop->fset);
            fprintf (stderr, MSG_CM(COMMON_ERR,
                        "%s: Error = %s\n"), Prog, BSERRMSG(sts));
            goto _error;
        }
    
        if (sts == EOK) {
            sts = advfs_lib_get_bfset_params_activate(infop->fsname, set_id, 
                    &set_params);
            if (sts) {
                fprintf(stderr, 
                    MSG_CM(COMMON_ERRFSINFO, 
                    "%s: Cannot get file set info for file system '%s'\n"),
                    Prog, infop->fsname);
                goto _error;
            }
        }

        /* Only check for snapshot if not a recursive delete */
        if (sts == EOK && !recurse && 
            !BS_BFS_EQL(set_params.bfspFirstChildSnap, nilMlBfSetIdT)) {
                fprintf(stderr, MSG_RF(RF_HASSNAP, 
                "%s: file system %s/%s has a snapshot.\n"),
                Prog, infop->fsname, infop->fset);
            goto _error;
        }
    }

    if (!do_not_ask) {
        if (strcmp(infop->fset, "default") != 0) {
            fprintf( stderr, MSG_RF(RF_CONFIRM, 
                "%s: remove file system %s/%s? [y/n] "), 
                Prog, infop->fsname, infop->fset);
        } else {
            fprintf( stderr, MSG_RF(RF_CONFIRMSD, 
                "%s: remove storage domain %s? [y/n] "), 
                Prog, infop->fsname);
        }    
        fgets (response, 2, stdin);
        if (strncasecmp(response, "y", 1) != 0) {
            goto _error;
        }
    }

    /*  Don't bother removing snapshots if we're removing the whole 
     *  storage domain.
     */
    if (sts == EOK && recurse && (strcmp(infop->fset, "default") != 0)) {
        /* recursive delete */
        sts = rmfs_delete_recursive(infop,
                set_params.bfSetId,
                set_params.bfspFirstChildSnap);
        if (sts) {
            goto _error;
        }
    }

    /* We are not going to delete the metadata for the 'default' fileset.  
     * The reasoning is if a user mistakenly calls rmfs then there exists some 
     * path of recovery for the file system.
     */
    if (strcmp(infop->fset, "default") != 0) {
        /* Delete the fileset metadata */
        sts = advfs_fset_delete(infop->fsname, infop->fset);
        if (sts) {
            fprintf(stderr, 
                    MSG_RF(RF_ERRDEL, "%s: Error deleting %s. %s\n"), 
                    Prog, infop->fsname, BSERRMSG(sts));
            goto _error;
        }
        snprintf(file_name, MAXPATHLEN+1, "%s/%s", infop->fspath, infop->fset);
        ret = unlink(file_name);
    } else {
        /* we're taking out the default fileset, wipe out everything */
        snprintf(tmplockFile, MAXPATHLEN+1, "%s/../%s_%s", infop->fspath, 
            DOT_ADVFS_LOCK, infop->fsname);
        lptr = realpath(tmplockFile, lockFile);

        /* get the lockfile name before we start removing anything */

        dirty = cleanup_dir(infop->fspath, 0);

        if (dirty) {
            printf(MSG_RF(RF_NOTEMPTY, 
               "%s: Non-AdvFS files found in %s, please inspect then \nmanually remove (rm -rf) this directory.\n"), 
               Prog, infop->fspath);
            goto _error;
        } else {
            unlink(infop->fspath);
        }

        /*
         * Remove the lock file for the removed file system.
         */
        if (lptr != NULL) {
            unlink(lockFile);
        }
    }

    advfs_post_user_event(EVENT_FDMN_RM, advfs_event, Prog);
    if (strcmp(infop->fset, "default") != 0) {
        printf(MSG_RF(RF_GOODRM, "%s: file system %s/%s removed.\n"), Prog, 
            infop->fsname, infop->fset);
    } else {
        printf(MSG_RF(RF_GOODRMSD, "%s: storage domain %s removed.\n"), Prog, 
            infop->fsname);
    }


#ifdef ESS

    /* QA Test Case: perform failure when rmfs completed the process */
    ESS_MACRO_0(TCRGenPanicKernel, ADVFS_RMFDMN_PANIC);

#endif

    free(infop);
    return 0;

_error:
    if (infop) {
        free(infop);
    }

    if (thisFsLock != -1) {
        (void)advfs_unlock(thisFsLock, &err);
    }

    if (strcmp(infop->fset, "default") != 0) {
        fprintf( stderr, MSG_RF(RF_BADRM, 
            "%s: Cannot remove file system '%s/%s' \n"), 
            Prog, infop->fsname, infop->fset);
    } else {
        fprintf( stderr, MSG_RF(RF_BADRMSD, 
            "%s: Cannot remove storage domain '%s' \n"),
            Prog, infop->fsname);
    }

    return 1;
} 

static adv_status_t
rmfs_delete_recursive(
        arg_infoT*  infop,
        bfSetIdT    parent_set_id,
        bfSetIdT    first_child_set_id
)
{
    bfSetParamsT    cur_set_params;
    adv_status_t    sts;
    bfSetIdT        next_set_id;
    bfSetIdT        cur_set_id = first_child_set_id;
    int             ret = 0;
    char            unlink_path[MAXPATHLEN+1];
    char*           mntdir;

    while (!BS_BFS_EQL(cur_set_id, nilMlBfSetIdT)) {
        if (sts = advfs_lib_get_bfset_params_activate(infop->fsname, 
                cur_set_id, &cur_set_params)) {
            fprintf(stderr, 
                    MSG_RF(RF_BADSNAPINFO, 
                        "%s: Error getting file system info for snapshot\n"),
                    Prog);
            return sts;
        }

        if (advfs_is_mounted(infop, cur_set_params.setName, NULL, &mntdir) > 0) {
            fprintf(stderr, MSG_RF(RF_ISMNT, 
                        "%s: File system %s/%s is mounted on %s. \n"),
                    Prog, infop->fsname, cur_set_params.setName, mntdir);
            fprintf( stderr, MSG_RF(RF_DISM, 
                "File system(s) must be dismounted prior to removal. \n"));
            free(mntdir);
            return E_ACCESS_DENIED;
        }

        next_set_id = cur_set_params.bfspNextSiblingSnap;
        if ( sts = rmfs_delete_recursive(infop, cur_set_id, 
                cur_set_params.bfspFirstChildSnap)) {
            return sts;
        }
        cur_set_id = next_set_id;
        if (sts = advfs_fset_delete(infop->fsname, cur_set_params.setName)) {
            fprintf(stderr, MSG_RF(RF_ERRDELSNAP, 
                        "%s: Error deleting snapshot %s. %s\n"),
                    Prog, cur_set_params.setName, BSERRMSG(sts));
            return sts;
        }
        snprintf(unlink_path, MAXPATHLEN+1, "%s/%s", infop->fspath, 
                cur_set_params.setName);
        if (ret = unlink(unlink_path)) {
            return ret;
        }
    }
    return EOK;
}

/*
 *  cleanup_dir
 *
 *  Recursively go through the specified directory, unlinking everything.
 *  If markSuperblock is 1, invalidate the superblock of any block devices
 *  found in the directory.
 */
static int
cleanup_dir (
    char*   dirPath,
    int     markSuperblock
)
{
    DIR*           dirp = NULL;
    struct dirent* d;
    struct stat    statBuf;
    char           path[MAXPATHLEN+1];
    int            ret;  

    dirp = opendir(dirPath);
    if (dirp == NULL) {
        fprintf(stderr, MSG_RF(RF_BADOPEN, "%s: Could not open %s.\n"), 
            Prog, dirPath);
        return 1;
    }

    while ((d = readdir(dirp)) != NULL) {

        if ((strcmp(d->d_name, ".") == 0) ||
            (strcmp(d->d_name, "..") == 0)) {
            continue;
        }

        snprintf(path, MAXPATHLEN+1, "%s/%s", dirPath, d->d_name);

        ret = stat(path, &statBuf);
        if (ret != 0) {
            /*  probably a link to nowhere.  Get rid of it. */
            unlink(path);
            continue;
        }

        if (S_ISDIR(statBuf.st_mode)) {
            if (strcmp(d->d_name, DOT_ADVFS_STG) == 0) {
                ret = cleanup_dir(path, 1);
            } else {
                ret = cleanup_dir(path, 0);
            }
            if (ret != 0) {
                closedir(dirp);
                return 1;
            }
        }

        if (S_ISBLK(statBuf.st_mode) && markSuperblock) {
            /* Mark the Advfs superblock token to show that there
             * used to be an Advfs file system on this volume 
             */
            (void)invalidate_superblock(path);
        }

        /*  lastly, unlink the file */
        unlink(path);
    }

    closedir(dirp);

    return 0;
}

/* end rmfs.c */
