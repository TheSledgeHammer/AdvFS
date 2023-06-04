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
 *    advchdsk  - Utility to modify the disk which is part of 
 *                an AdvFS storage domain when that disk's name
 *                has been changed.  This can occur when doing
 *                a clu_add_member in a TCR CFS cluster as device
 *                naming needs to be normalized between the existing
 *                cluster member(s) and the member being added.
 * 
 *                This utility is intended to be run as part of 
 *                initial boot of the newly added member, i.e., 
 *                before filesystems have been mounted.  Note that 
 *                if the root filesystem has been modified, the 
 *                system should immediately be rebooted before attempting
 *                to mount other filesystems to prevent any possible
 *                confusion with VFS.
 *
 *                Because this utility may be run on the root filesystem,
 *                and because it may not be able to check that the specified
 *                devices actually exist (if a SCRAM database has been 
 *                newly installed, for example), sanity checking is lax.
 *                This tool should be used with care as it could render your
 *                system unusable if incorrect devices are specified.
 *
 *                Takes as arguments 2 block device paths.  The first 
 *                is the original block device.  The second is the new name
 *                of that device.  
 *         
 *                Output is the old fsname and the new fsname.  This is to
 *                inform the caller a) if the fsname changed and b) what the
 *                new name is.  Note that name changes should only occur on
 *                "unnamed" filesystems - i.e., using the default constructed
 *                name.
 *
 *                Exit status:
 *                     0 = Success
 *                     1 = Device spec provided did not belong to AdvFS.
 *                         No action taken.
 *                     2 = Device is part of an AdvFS filesystem, but an
 *                         error occurred when trying to update.
 */
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <stdlib.h>
#include <advfs/advfs_syscalls.h>
#include <tcr/clu.h>

static char*    Prog = "advchdsk";

/*********************************************************************/

int
main(int argc, char *argv[])
{
    int           ret;
    char*         oldDev = NULL;
    char*         newDev = NULL;
    arg_infoT*    infop = NULL;
    char*         newfsname = NULL;
    int           globalLockFd = -1;
    char*         ptr = NULL;
    char          newfsdir[MAXPATHLEN+1] = {0};
    char          tmpstr[MAXPATHLEN+1] = {0};
    char          vm_diskgroup[MAXPATHLEN+1] = {0}; 
    char          vm_volname[MAXPATHLEN+1] = {0};
    char          origfsname[MAXPATHLEN+1] = {0};

    /* Must be privileged to run */
    if (geteuid()) {
        fprintf(stderr, "%s:  Permission denied - user must be privileged.\n",
            Prog);
        exit(2);
    }

    /*  as we'll be renaming filesystems, potentially, take out the
     *  global lock 
     */
    globalLockFd = advfs_global_lock(FALSE, &ret);
    if (globalLockFd < 0) {
        fprintf(stderr, "%s:  Could not obtain global lock\n", Prog); 
        exit(2);
    }

    /*  must have 2 arguments provided */
    if (argc != 3) {      
        fprintf(stderr, "Usage:  %s <old devspec> <new devspec>\n", Prog);
        exit(2);
    }

    oldDev = argv[1];
    newDev = argv[2];

    /*  1st argument is the old device spec.  Ensure that it's part of 
     *  an existing filesystem 
     */
    
    infop = process_advfs_arg(oldDev);

    /*  device specified is not in use by AdvFS */
    if (infop == NULL) {
        exit(1);
    }

    newfsname = arg_to_fsname(newDev, NULL);
    if (newfsname != NULL) {    /* hey, new device already in use! */
        fprintf(stderr, "%s:  New device in use by filesystem %s\n",
            Prog, newfsname);
        free(infop);
        free(newfsname);
        advfs_unlock(globalLockFd, &ret);
        exit(2);
    }

    strcpy(origfsname, infop->fsname);

    /*  if it's unnamed, gotta rename the filesystem */
    if (!infop->is_multivol) {
        /*  Generate the new fsname */
        newfsname = advfs_fsname(newDev);
        if (newfsname == NULL) {
            fprintf(stderr, "%s: Error processing specified device name %s\n",
                Prog, newDev);
            free(infop);
            advfs_unlock(globalLockFd, &ret);
            exit(2);
        }

        /* remove the old lockfile */
        snprintf(tmpstr, MAXPATHLEN+1, "%s/../%s_%s", infop->fspath,
            DOT_ADVFS_LOCK, infop->fsname);
        unlink(tmpstr);

        /* ok, set up the new directory */
        strcpy(newfsdir, infop->fspath);
        ptr = strrchr(newfsdir, '/');
        if (ptr == NULL) {  /* should never happen */
            fprintf(stderr, "%s: Error processing specified device name %s\n",
                Prog, newDev);
            free(infop);
            free(newfsname);
            advfs_unlock(globalLockFd, &ret);
            exit(2);
        }

        *++ptr = '\0';
        strcat(newfsdir, newfsname);

        ret = rename(infop->fspath, newfsdir);

        if (ret) {
            fprintf(stderr, 
                "%s:  Could not create new filesystem directory %s\n", 
                Prog, newfsdir);
            free(infop);
            free(newfsname);
            advfs_unlock(globalLockFd, &ret);
            exit(2);
        }

        free(infop);

        infop = process_advfs_arg(newfsname);

        free(newfsname);

        if (infop == NULL) {  /* should never happen */
            fprintf(stderr, 
                "%s:  Error processing specified device name %s\n",
                Prog, newDev);
            advfs_unlock(globalLockFd, &ret);
            exit(2);
        }
    }

    /*  Create the new link name */
    if (advfs_checkvol(newDev, vm_diskgroup, vm_volname)) {
        snprintf(tmpstr, MAXPATHLEN+1, "%s/%s.%s", infop->fspath, 
            vm_diskgroup, vm_volname);
    } else {
        ptr = strrchr(newDev, '/');
        if (ptr == NULL) {     /* should never happen */
            ptr = newDev;
        } else {
            ptr++;
        }

        snprintf(tmpstr, MAXPATHLEN+1, "%s/%s", infop->stgdir, ptr);
    }

    if (symlink(newDev, tmpstr)) {
        fprintf(stderr, "%s: cannot create symlink '%s @--> %s'; [%d]\n",
            Prog, newDev, tmpstr, errno);
        free(infop);
        advfs_unlock(globalLockFd, &ret);
        exit(2);
    }

    /*  remove the old link */
    if (advfs_checkvol(oldDev, vm_diskgroup, vm_volname)) {
        snprintf(tmpstr, MAXPATHLEN+1, "%s/%s.%s", infop->fspath, 
            vm_diskgroup, vm_volname);
    } else {
        ptr = strrchr(oldDev, '/');
        if (ptr == NULL) {     /* should never happen */
            ptr = oldDev;
        } else {
            ptr++;
        }

        snprintf(tmpstr, MAXPATHLEN+1, "%s/%s", infop->stgdir, ptr);
    }

    unlink(tmpstr);

    /*  Lastly, report the original and new fsnames to the caller */
    fprintf(stdout, "%s\t%s\n", origfsname, infop->fsname);

    free(infop);

    /* release the global lock */
    advfs_unlock(globalLockFd, &ret);

    return 0;
}
