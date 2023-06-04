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
 */

/* includes */
#include <sys/types.h>
#include <sys/stat.h>
#include <string.h>
#include <time.h>
#include        "common.h" /* includes other includes that we need */

/* GLOBALS */
static char *Prog = "fsadm list";

/* PROTOTYPES */
void list_usage( void );
static void print_fs_devices(char* fs_name, int character);
static void print_snap_children( char *, bfSetIdT, bfSetIdT, int );


/*
 *      NAME:
 *              list_main()
 *
 *      DESCRIPTION:
 *              main routine for fsadm list command
 *              This utility will list AdvFS filesystems.
 *
 *      ARGUMENTS:
 *              argc            number of command line arguments
 *              argv            command line arguments
 *
 *      RETURN VALUES: 
 *              success         0
 *              failure         non-zero
 *
 */ 
int
list_main(int argc, char **argv)
{
    int c, block=0, character=0, snapshot=0;
    DIR *FD, *FDD;
    struct dirent *devs, *devss;
    char fs_name[MAXPATHLEN];
    char * req_fsname =NULL;
    struct stat  statBuf;
    arg_infoT*   infop = NULL;
    bfSetIdT set_id;
    statusT sts = EOK;
    bfSetParamsT set_params;
    int Vflag = 0;
    int ret = 0;
    int i;

    extern char*  advfs_global_dirs[];

    /*
     * Get user-specified command line arguments.
     */
    while ((c = getopt( argc, argv, "bcsV" )) != EOF) {
        switch (c) {

            case 'b':           /* list block devices */
                block++;
                break;

            case 'c':           /* list character devices */
                character++;
                break;

            case 's':           /* list snapshot tree */
                snapshot++;
                break;

            case 'V':
                Vflag++;
                break;

            default:
                list_usage();
                return 1;
        }
    }

    if ((character && block) || (character > 1) || (block > 1)) {
        list_usage();
        return 1;
    }

    if ((character && snapshot) || (block && snapshot) || (snapshot > 1)) {
        list_usage();
        return 1;
    }

    if ( Vflag > 1 ) {
        list_usage();
        return 1;
    }
    
    if (Vflag) {
        printf( "%s\n", Voption(argc, argv));
        return 0;
    }

    /* skip the processed options */
    argc -= optind;
    argv += optind;

    if (argc) {
        /* only doing the one filesystem */
        infop = process_advfs_arg(*argv);
        if (!infop) {
            printf(catgets(catd, MS_FSADM_LIST, LIST_NO_FS,
                    "%s: Could not find file system %s\n"), Prog, *argv);
            return 1;
        }

        printf("%s\n", (infop->is_multivol) ? infop->fspath : infop->blkfile);
        if (snapshot) {

            sts = advfs_fset_get_id(infop->fsname, infop->fset, &set_id);
            if (sts) {
                printf(catgets(catd, MS_FSADM_COMMON, COMMON_BADFSID, 
                            "%s: Cannot get file set id.\n"),
                        Prog);
                free(infop);
                return 1;
            }
            sts = advfs_lib_get_bfset_params_activate(infop->fsname, set_id, 
                    &set_params);
            if (sts) {
                printf(catgets(catd, MS_FSADM_COMMON, COMMON_ERRFSINFO, 
                            "%s: Cannot get file set info for file system '%s'\n"), 
                        Prog, *argv);

                free(infop);
                return 1;
            }
            print_snap_children(infop->fsname, set_id,
                    set_params.bfspFirstChildSnap, 1);

        } else if (block || character) {
            print_fs_devices(infop->stgdir, character);
        }

        free(infop);
        return(0);
    }

    /* open top-level AdvFS directories */
    for (i = 0 ; advfs_global_dirs[i] != NULL ; i++) {
        FD = opendir(advfs_global_dirs[i]);
        if (FD == NULL) {
            continue;
        }

        /* Open each file system in the AdvFS directory */
        while ((devs = readdir(FD)) != NULL) {
            int   st = 0;

            if (devs->d_name[0] == '.') {
                continue;
            }

            snprintf(fs_name, MAXPATHLEN, "%s/%s", advfs_global_dirs[i], 
                devs->d_name);

            st = stat(fs_name, &statBuf);
            if (st != 0 || !S_ISDIR(statBuf.st_mode)) {
                /*  not a directory, skip it */
                continue;
            }

            /* use the fully-qualified (dir+fsname) name here in case
             * of name collision between TCR shared filesystems and 
             * system-specific ones.  Theoretically we'll be able to 
             * prevent the name collisions, but... 
             */
            infop = process_advfs_arg(fs_name);

            if (!infop) {
                continue;  /* guess it wasn't an advfs filesystem */
            }

            printf("%s\n", (infop->is_multivol) ? 
                infop->fspath : infop->blkfile);

            /* open the file system directory */
            if (snapshot) {

                sts = advfs_fset_get_id(infop->fsname, infop->fset, &set_id);
                if (sts) {
                    printf(catgets(catd, MS_FSADM_COMMON, COMMON_BADFSID, 
                                "%s: Cannot get file set id.\n"),
                            Prog);
                    free(infop);
                    closedir(FD);
                    return 1;
                }
                sts = advfs_lib_get_bfset_params_activate(infop->fsname, 
                        set_id, &set_params);
                if (sts) {
                    printf(catgets(catd, MS_FSADM_COMMON, COMMON_ERRFSINFO, 
                        "%s: Cannot get file set info for file system '%s'\n"), 
                        Prog, *argv);
    
                    free(infop);
                    closedir(FD);
                    return 1;
                }
                print_snap_children(infop->fsname, set_id,
                        set_params.bfspFirstChildSnap, 1);

            } else if (block || character) {
                print_fs_devices(infop->stgdir, character);
            }

            free(infop);
            infop = NULL;
        }

        closedir(FD);
    }

    return ret;
}

static void
print_fs_devices(char* fs_name, int character)
{
    DIR*          FDD = NULL;
    struct dirent *devs;
    char          tmp_path[MAXPATHLEN+1];
    char          dev_link_path[MAXPATHLEN+1];

    FDD = opendir(fs_name);
    if (FDD == NULL) {
        printf(catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN,
                    "%s: open of %s failed. [%d] %s\n"), 
                    Prog, fs_name, errno, sys_errlist[errno]);
    }

    /* Find a matching device link in the file system directory */
    while ((devs = readdir(FDD)) != NULL) {
        if (devs->d_name[0] == '.') {
            continue;
        }

        snprintf(tmp_path, MAXPATHLEN, "%s/%s", fs_name, devs->d_name);
        /* Get the path to this device */
        if (readlink(tmp_path, dev_link_path, MAXPATHLEN) < 0) {
            printf(catgets(catd, MS_FSADM_COMMON, COMMON_NOREADLINK,
                            "%s: readlink of %s failed. [%d] %s\n"), 
                           Prog, tmp_path, errno, sys_errlist[errno]);
            continue;
        }
        if (character) {
            strcpy (tmp_path, dev_link_path);
            if (__blocktochar(tmp_path, dev_link_path) == NULL) {
                printf(catgets(catd, MS_FSADM_LIST, LIST_NO_CHAR, 
                            "%s: unable to resolve character device special file from %s.\n"), Prog, tmp_path);
                continue;
            }
        }
        printf("    %s\n", dev_link_path);
    }
    closedir(FDD);

    return;
}


void
list_usage( void )
{
    usage( catgets( catd, MS_FSADM_LIST, LIST_USAGE,
                    "%s [-V] [-b | -c | -s] [file_system]\n" ),
           Prog );
}

static void
print_snap_children(
        char *dmn_name,                 /* in - domain name */
        bfSetIdT set_id,              /* in - set id for parent */
        bfSetIdT first_child_set_id,  /* in - first child of parent */
        int level)                      /* in - level of recursion */
{
    bfSetParamsT set_params;
    bfSetIdT child_set_id;
    time_t tv;
    statusT sts = EOK;
    char time_str[32];

    child_set_id = first_child_set_id;
    while (!BS_BFS_EQL(child_set_id, nilMlBfSetIdT)) {
        sts = advfs_lib_get_bfset_params_activate(dmn_name,
                child_set_id, &set_params);
        if (sts) {
            printf(catgets(catd, MS_FSADM_LIST, LIST_NO_INFO, 
                        "<unable to get snapshot info>\n"));
            break;
        }

        tv = (time_t)set_params.bfspCreateTime;
        sprintf(time_str, "%s", ctime(&tv));
        time_str[strlen(time_str) - 1] = '\0';

        printf("%*s%s (%s %s)\n", (level*2), "", set_params.setName,
                catgets(catd, MS_FSADM_LIST, LIST_CREATE, "Created"), 
                time_str);
        print_snap_children(dmn_name, child_set_id, 
                set_params.bfspFirstChildSnap, (level+1));
        child_set_id = set_params.bfspNextSiblingSnap;
    }
}
        
