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
#include        <stdio.h>
#include        <sys/stat.h>
#include        <sys/time.h>
#include        <sys/file.h>
#include        <sys/ioctl.h>
#include        <sys/errno.h>
#include	<sys/condvar.h>
#include        <dirent.h>
#include        <advfs/bs_public.h>
#include        <advfs/ms_generic_locks.h>
#include        <advfs/ftx_public.h>
#include        <advfs/ms_logger.h>
#include        <advfs/bs_ods.h>
#include        <advfs/fs_dir.h>
#include        <advfs/bs_error.h>
#include        <advfs/advfs_syscalls.h>
#include        <advfs/advfs_evm.h>
#include        <sys/fs.h>
#include        <string.h>
#include        <libgen.h>

#include        "common.h"
#include	"scan.h"
#include        <locale.h>

static char *Prog = "fsadm scan";

/*
 *      NAME:
 *              scan_main()
 *
 *      DESCRIPTION:
 *              main routine for fsadm scan command
 *              This utility will locate AdvFS volumes on disk devices.
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
scan_main(int argc, char **argv)
{
    int fd, i, j, n, disks = 0;
    int c, all_devs = 0, num_opts = 0;
    int V, o, recreate, no_advfs, quiet, namlen, fix, undo;
    char create_path[MAXPATHLEN+1];
    char rawdevname[MAXPATHLEN+1];
    char fs_name[BS_DOMAIN_NAME_SZ];
    char *options = NULL;
    char *p, *ptr;
    struct stat rawstats;
    int max_devs = 0, num_devices = 0, devs_checked = 0;
    statusT sts;
    device_info *thisDevice;
    bfSetParamsT set_params;                   /* for advfs_fset_get_info */
    uint64_t set_idx = 0;                      /* for advfs_fset_get_info */
    uint32_t user_id;                          /* for advfs_fset_get_info */
    char *tstfsname = NULL;

    V = o = recreate = no_advfs = quiet = namlen = fix = undo = 0;

    check_root( Prog );

    /*
     ** Get user-specified command line arguments.
     */
    while ((c = getopt( argc, argv, "Vagqo:" )) != EOF) {
        switch (c) {

            case 'a':           /* list all AdvFS volumes */
                all_devs++;
                break;

            case 'g':           /* list AdvFS volumes */
                print_geom++;
                break;

            case 'o':           /* options list */
                o++;
                options = (char *) malloc(strlen(optarg)+1);
                if (!options) {
                        fprintf( stderr,
                                catgets( catd, MS_FSADM_COMMON, COMMON_NOMEM,
                                "%s: insufficient memory.\n"), Prog );
                        exit(1);
                }
                strcpy( options, optarg);
                break;

            case 'q':           /* quiet */
                quiet++;
                break;

            case 'V':           /* print verified command line */
                V++;
                break;

            case '?':

            default:
		scan_usage();
                exit( 1 );
        }
    }

    num_opts = optind-1;

    if (argc < 2) {
	scan_usage();
        exit( 1 );
    }

    /* Check for redundant arguments */
    if( ( all_devs > 1 ) || ( print_geom > 1 ) || ( o > 1 ) ||
	( quiet > 1 ) || ( V > 1 ) ) {
	scan_usage();
	exit( 1 );
    }

    /*
     * If -o option was used, parse the options and set the options flag
     */
    if( o ) {
        if( parse_options( options, &recreate, &fix, &undo ) == -1 ) {
                exit( 1 );
        }
    }

    if ((print_geom) && (fix || recreate)) {
	scan_usage();
        exit(1);
    }

    /*
     * Use of the "name=< fs_name >" option in combination with the "recreate"
     * option is limited to 1 fs_name and no "undo" options to prevent any
     * ambiguities with the disks targeted for the filesystem being recreated.
     */
    if (recreate && ((fix > 1) || ((fix == 1) && (undo != 0)))) {
        printf(catgets(catd, NL_SCAN, SCAN_BADRECROPT,
               "%s: recreate accepts a maximum of one fsname option with "
               "no undo option\n"),
               Prog );
        scan_usage();
        exit(1);
    }

    /*
     * processing a list of devices
     */
    disks = argc - 1 - num_opts;
    if ((disks < 1) && !all_devs) {
	scan_usage();
        exit (1);
    }

    /*
     *  If -V option was specified, print out verified command line and exit
     */
    if( V ) {
        printf( "%s", Voption( argc, argv ) );
        return 0;
    }

    /* If -a option specified, get the number of devices found in all AdvFS 
     * filesystems
     */
    if (all_devs) {
        sts = scan_advfs( SCAN_COUNT, &max_devs );
        if (sts != EOK) {
            no_advfs = 1;
            all_devs = 0;
            if (!recreate) {
                /*  TODO:  What should these messages say?  */
                printf(catgets(catd, NL_SCAN, SCAN_NOREAD,
                        "Cannot read %s. You should rerun \n"),
                                ADVFS_DMN_DIR);
                printf(catgets(catd, NL_SCAN, SCAN_NOREAD2,
                        "fsadm scan with -r option (recreate /dev/advfs)\n"));
            }
        }
    } /* end: if all_dev */

    /* Nothing to do */
    if((max_devs + disks) == 0) {
        printf(catgets(catd, NL_SCAN, SCAN_NODEV,
            "No devices to scan, rerun fsadm scan with devices specified\n"));
        exit( 1 );
    }

    /* Initialize the disk table */
    disk_table_pt = calloc( max_devs + disks, sizeof (disk_table_entry));
    if( disk_table_pt == NULL ) {
	fprintf( stderr, catgets( catd, MS_FSADM_COMMON, COMMON_NOMEM,
	    "%s: insufficient memory.\n"), Prog );
	exit(1);
    }

    /* put the user specified devices in the disk table */
    p = (char *)disk_table_pt;
    for (j = 0; j < disks; j++) {

        /*
         *  Check if this is a block device, if so, convert to raw device
         *  and add it to the disk table
         */
        strcpy( rawdevname, argv[j+num_opts+1] );
        if( stat( rawdevname, &rawstats ) < 0 ) {
            printf( catgets(catd, MS_FSADM_COMMON, COMMON_STATS,
                "%s: Error getting '%s' stats; [%d] %s\n" ),
                Prog, rawdevname, errno, ERRMSG( errno ) );
            exit( 1 );
        }
        if( S_ISBLK( rawstats.st_mode ) ) {
            ptr = __blocktochar( rawdevname, p );
            if( ptr == NULL) {
                printf(catgets(catd, NL_SCAN, SCAN_BLK2CHAR,
                    "%s: __blocktochar() failed\n"), Prog );
                exit( 1 );
            }
        } else if( S_ISCHR( rawstats.st_mode ) ) {
	    strcpy( p, rawdevname );
        } else {
            printf( catgets(catd, MS_FSADM_COMMON, COMMON_NOT_BLKDEV,
                "%s: %s is not a valid block device\n" ), Prog, rawdevname );
            exit( 1 );
        }

        p += sizeof(disk_table_entry);
    }
    max_devs = disks;

    /* if -a, put the devices from all of AdvFS in the table too */
    if (all_devs) {
        sts = scan_advfs( SCAN_ADDDEVS, &max_devs );
    }

    /* Let's do some scanning! */
    p = (char *)disk_table_pt;
    printf(catgets(catd, NL_SCAN, SCAN_SCANDEV, "\nScanning devices\t"));
    for (i = 0; i < max_devs; i++) {
        printf("%s ", p);
        if ((i + 1)%3 == 0) printf("\n\t\t\t");
        p += sizeof(disk_table_entry);
    }
    printf("\n");

    /* If -g, print banner */
    if (print_geom) {
        printf(catgets(catd, NL_SCAN, SCAN_BANNER,
                "\nDevice                  File System Id\n"));
    }

    /* Process each device and add it to the fs_table */
    p = (char *)disk_table_pt;
    for (j = 0; j < max_devs; j++) {

        strcpy(devname, p);
        fd = open(devname, O_RDONLY);
        if (fd <= 0) {
            printf(catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN2,
                "%s: open of %s failed.\n"), Prog, devname);
            break;
        }

        if (fd > 0) {
	    int tmperr = 0;
	    if( undo ) {
		sts = revalidate_superblock( devname );
		if( sts != EOK ) {
		    printf( catgets( catd, MS_FSADM_COMMON, COMMON_RE_MAGIC,
			"%s: could not restore an AdvFS file system on %s; [%d] %s\n" ),
			Prog, devname, errno, ERRMSG( errno ) );
		    break;
		}
	    }
            process(fd);
            devs_checked++;
            close(fd);
        }
        p += sizeof(disk_table_entry);
    }

    /* if we processed no devices, or -g, then we're all done */
    if (0 == devs_checked) exit( 1 );
    if (print_geom) exit( 0 );

    /* match up fs_table with AdvFS filesystems, num_devices is not 
     * really used 
     */
    sts = scan_advfs( SCAN_MATCH, &num_devices );

    /*
     * print out the fs_table
     */
    if (!quiet && !fix) {
	printf(catgets(catd, NL_SCAN, SCAN_FOUNDFS,
		"\nFound file systems:\n\n"));
	for (i = 0; i < fs_cnt; i++) {
	    if (fs_table[i].filesys_name == NULL) { 
                printf(catgets(catd, NL_SCAN, SCAN_UNKNOWN, "*unknown*\n"));
                /*
                 * The disks listed for this filesystem were supplied
                 * by the user and are not listed the .stg directory of
                 * any existing filesystem.
                 *
                 * These disks may be:
                 *  - some or all of the disks from of a split-mirror 
                 *    that has not been recreated
                 *  - orphan disks from an existing filesystem that has
                 *    missing disks
                 *  - some or all of the disks from a filesystem that
                 *    no longer exists
                 *  - some or all of the disks from a filesystem that
                 *    was copied from another filesystem system
                 */
                
                /* TODO: review new messages with Kristen - BobB */
                if (fs_table[i].dev_fs_name[0] != '\0') {
                    printf("*** was/is part of filesystem %s\n",
                            fs_table[i].dev_fs_name);
                }
	    } else {
		printf("%s\n", fs_table[i].filesys_name);
	    }
	    printf(catgets(catd, NL_SCAN, SCAN_FSID,
		"\t\tFile system Id\t%08x.%08x\n"),
		fs_table[i].fs_id.id_sec, fs_table[i].fs_id.id_usec);
	    printf(catgets(catd, NL_SCAN, SCAN_CREATED, "\t\tCreated\t\t%s\n"),
		ctime( (time_t *)&fs_table[i].fs_id ) );
	    printf(catgets(catd, NL_SCAN, SCAN_FSVOLS,
		"\t\tFile system volumes\t\t%d\n"), fs_table[i].fs_dev_num);
            /* TODO:  Need to change this message too */
	    printf(catgets(catd, NL_SCAN, SCAN_LINKS,
		"\t\t/dev/advfs links\t\t%d\n"), fs_table[i].num_links);
	    printf(catgets(catd, NL_SCAN, SCAN_DEVFND,
		"\n\t\tActual devices found:\n"));

	    for (j = 0; j < fs_table[i].device_count; j++) {
		thisDevice = fs_table[i].device[j];
		if ( thisDevice->wrong_dir != 1 ) {
		    /* Part in correct directory. */
		    printf(catgets(catd, NL_SCAN, SCAN_PART,
			"\t\t\t\t\t%s"), thisDevice->dev_name);
		} else {
                    /* TODO:  Need to change this message too */
		    printf(catgets(catd, NL_SCAN, SCAN_INCORRECT,
			"\t\t\t\t\t%s   *** Incorrectly located in ***\n\t\t\t\t\t /dev/advfs/%s"),
			thisDevice->dev_name, thisDevice->wrong_dir_name);
		}
		if( thisDevice->correspond_flag == 0) {
			printf("*  ");
		}
		if( thisDevice->state_flag == Rmvoled) {
			printf(catgets(catd, NL_SCAN, SCAN_RMVOL, "  Rmvoled"));
		}
		printf("\n");
	    }
	}
    } /* end if not quiet/not fix */

    /*
     * if -o recreate, re-create anything missing 
     */
    if( recreate || undo ) {

	/* Create the AdvFS directory[ies] if missing */
        if (create_advfs_hierarchy()) {
            /* errors will be printed from create_advfs_hierarchy() */
            exit( 1 );
        }
        for (i = 0; i < fs_cnt; i++) {
            /* TODO:  This is ok for now and the foreseeable future.  
             *        scan will recreate storage domains in ADVFS_DMN_DIR.
             *        Once DRD/device promotion exists, scan may need to
             *        be updated to use the TCR/CFS top level dir too.
             */
            snprintf(fs_name, MAXPATHLEN+1, "%s/%s", ADVFS_DMN_DIR, FS_ADVFS);

            if (fs_table[i].filesys_name == NULL) {
                int valid_fsname = FALSE;
                if (fix) {
                    /*
                     * Use name supplied by user if it is not currently
                     * in use.  Process entire list filesystem names so
                     * new name can be added to list.
                     */
                    /*
                     * TODO:  review error message and
                     *        add to fsadm_advfs.msg file
                     */
                    if (fix_fs == NULL) {
                        printf("A valid filesystem name must be entered\n");
                        scan_usage();
                        exit( 1 );
                    }
                    tstfsname = arg_to_fsname(fix_fs, NULL);
                    if (tstfsname == NULL) {
                        strcpy(fs_name, fix_fs);
                        valid_fsname = TRUE;
                    } else {
                        free(tstfsname);
                        printf("Filesystem Name %s already in use\n",
                                tstfsname);
                        scan_usage();
                        exit( 1 );
                    }
                }

		/*
                 * Use the filesystem name from the dev_fs_name if valid
                 * and not currently in use. 
                 */
                if ( !valid_fsname && (fs_table[i].dev_fs_name[0] != '\0')) {
                    tstfsname = arg_to_fsname(fs_table[i].dev_fs_name, NULL);
                    if (tstfsname == NULL) {
                        strcpy(fs_name, fs_table[i].dev_fs_name);
                        valid_fsname = TRUE;
                    } else {
                        free(tstfsname);
                    }
                }

                if (!valid_fsname) {
                    /*
                     * Make up a file system name
                     */
                    int rmvols = 0;
                    for (j = 0; j < fs_table[i].device_count; j++) {
                        thisDevice = fs_table[i].device[j];
                        if( thisDevice->state_flag == Rmvoled) {
                            rmvols++;
                            continue;
                        }
                        tstfsname = arg_to_fsname(thisDevice->dev_name, NULL);
                        if (tstfsname == NULL) {
                            snprintf(fs_name, BS_DOMAIN_NAME_SZ, "%s_%s",
                                    FS_ADVFS, thisDevice->dev_name);
                            valid_fsname = TRUE;
                            break;
                        } else {
                            free(tstfsname);
                        }
                    }
                    if (rmvols == fs_table[i].device_count) {
                        printf("No valid AdvFS devices found\n");
                        scan_usage();
                        exit( 1 );
                    }
                    
                    if (!valid_fsname) {
                        printf("Filesystem Name %s already in use\n",
                                fs_name);
                        scan_usage();
                        exit( 1 );
                    }
                }

                fs_table[i].filesys_name = malloc(strlen(fs_name) + 1);
                strcpy(fs_table[i].filesys_name, fs_name);

                snprintf(create_path, MAXPATHLEN+1, "%s/%s/", ADVFS_DMN_DIR, 
                        fs_name);
                /* create the file system directory */
                printf(catgets(catd, NL_SCAN, SCAN_CREATING,
                        "\nCreating %s\n"), create_path);
                if ((mkdir(create_path, 0755) < 0)
                        && (errno != EEXIST)) {
                    printf(catgets(catd, NL_SCAN, SCAN_ERRCREATE,
                        "Unable to create %s; [%d] %s\n"),
                        create_path, errno, BSERRMSG(errno));
                    continue;     /* bail out for this domain */
                }

                /* create the storage subdirectory */
                strcat(create_path, DOT_ADVFS_STG);
		strcat(create_path, "/");
                if ((mkdir(create_path, 0755) < 0)
                        && (errno != EEXIST)) {
                    printf(catgets(catd, NL_SCAN, SCAN_ERRCREATE,
                        "Unable to create %s; [%d] %s\n"),
                        create_path, errno, BSERRMSG(errno));
                    continue;     /* bail out for this domain */
                }

                /* make the symbolic links for the file system */
		namlen = strlen( create_path );
                for (j = 0; j < fs_table[i].device_count; j++) {

                    thisDevice = fs_table[i].device[j];
                    printf(catgets(catd, NL_SCAN, SCAN_LINKING,
                        "\tlinking %s\n"), thisDevice->dev_name );
                    if( thisDevice->state_flag != Rmvoled ) {
			strcpy( &create_path[namlen], thisDevice->dev_name );
                        if( symlink( thisDevice->dev_path, create_path ) < 0 ) {
                            printf( "%s\n", thisDevice->dev_path );
                            printf(catgets(catd, NL_SCAN, SCAN_LINKERR,
                                "Could not link %s in %s; [%d] %s\n"),
                                   thisDevice->dev_name, create_path, errno,
                                   BSERRMSG(errno));

                        }
                    }
                }

                /* create_path should now be the top-level dir for the file
                 * system 
                 */
                snprintf(create_path, MAXPATHLEN+1, "%s/%s", ADVFS_DMN_DIR, 
                        fs_name);
                /* create the block devices (including for snapshots) */
                do {
                    sts = advfs_fset_get_info( fs_name, &set_idx,
                            &set_params, &user_id, 0);
                    if (sts == EOK) {
                        if (advfs_make_blockdev(create_path, 
                                    set_params.setName, 0, 0)) 
                        {
                            printf(catgets(catd, NL_SCAN, SCAN_ERRDEV,
                                "Unable to create Advfs device %s/%s\n"),
                                fs_name, set_params.setName);
                        }
                    }
                } while (sts == EOK);

                if (sts != E_NO_MORE_SETS) {
                    printf(catgets(catd, MS_FSADM_COMMON, COMMON_ERRFSINFO, 
                        "%s: Cannot get file set info for file system '%s'\n"), 
                        Prog, fs_name);
                }
		init_event( &advfs_event );
		advfs_event.domain = fs_name;
		advfs_post_user_event(EVENT_ADVSCAN_RECREATE, advfs_event, 
                    argv[0]);

            } /* end if filesys_name is NULL */
        } /* end for loop */
    } /* end if recreate */

    /*
     * if -o name=<file system>, fix the file system specified
     */
    if( fix ) {

        for (i = 0; i < fs_cnt; i++) {

            if (fs_table[i].filesys_name == NULL) continue;

            n = strcmp( fix_fs, fs_table[i].filesys_name );
            if( n == 0 ) {
                printf(catgets(catd, NL_SCAN, SCAN_FIXLINK,
                    "\nAttempting to fix link/dev_count for file system\n\n\t%s\n\n"), fix_fs);
            } else {
                continue;
            }

            /* if we don't have the file system count, don't do anything. */
            if( fs_table[i].fs_dev_num == 0 ) {
                printf(catgets(catd, NL_SCAN, SCAN_MISS,
                        "Device containing volume count is missing.\n"));
                printf(catgets(catd, NL_SCAN, SCAN_NOFIX, "Cannot fix.\n"));
                break;
            }

            /* num of links and file system device count the same */
            if( fs_table[i].num_links == fs_table[i].fs_dev_num ) {
                /*
		 * More devices than links and file system device count.
		 * Should be OK.
		 */
                if( fs_table[i].device_count > fs_table[i].fs_dev_num ) {
                    printf(catgets(catd, NL_SCAN, SCAN_FSOK,
                        "File system OK; will mount with %d devices\n"),
                        fs_table[i].device_count );
                    break;
                }

                /* if they are all equal, where's the beef? */
                if( fs_table[i].device_count == fs_table[i].fs_dev_num ){
                    printf(catgets(catd, NL_SCAN, SCAN_FIXNOTH,
                        "Nothing to fix\n"));
                    break;
                }

                /* less devices than links and file system count - can't fix */
                if (fs_table[i].device_count < fs_table[i].fs_dev_num ) {
                    printf(catgets(catd, NL_SCAN, SCAN_FSMISS,
                        "It appears that a device is missing from the file system. Cannot fix.\n"));
                }
                break;
            }

            /* num of devices and file system count the same */
            if( fs_table[i].device_count == fs_table[i].fs_dev_num ) {
                if( fs_table[i].num_links > fs_table[i].device_count ) {
                    printf(catgets(catd, NL_SCAN, SCAN_FSMISS,
                        "It appears that a device is missing from the file system. Cannot fix.\n"));
                }
                if( fs_table[i].num_links < fs_table[i].device_count ) {
                    /* add the missing link(s) */
                    for (j = 0; j < fs_table[i].device_count; j++ ) {
                        thisDevice = fs_table[i].device[j];
                        if( thisDevice->correspond_flag == 0 ) {
                            snprintf(fs_name, MAXPATHLEN+1, "%s/%s/%s/",
                                ADVFS_DMN_DIR, fs_table[i].filesys_name, 
                                DOT_ADVFS_STG);
                     
                            strcat( fs_name, thisDevice->dev_name );
                            printf(catgets(catd, NL_SCAN, SCAN_ADDLINK,
                                "Adding link %s for file system %s\n"),
                                thisDevice->dev_path, fs_table[i].filesys_name);
                            if( symlink( thisDevice->dev_path, fs_name ) < 0 ) {
                                printf(catgets(catd, NL_SCAN, SCAN_LINKERR,
                                        "Could not link %s in %s; [%d] %s\n"),
                                        thisDevice->dev_name, fs_name, errno,
                                        BSERRMSG(errno));
                            }
                        }
                    }
                }
                break;
            }
            /*
             * num of devices and links the same
             */
            if( fs_table[i].device_count == fs_table[i].num_links ) {
                if( fs_table[i].device_count < fs_table[i].fs_dev_num ) {
		    /* file system thinks it has more devices...*/
                    printf(catgets(catd, NL_SCAN, SCAN_MORE,
                        "The file system expects more devices than were found. Cannot fix.\n"));
                }
                if( fs_table[i].device_count > fs_table[i].fs_dev_num ) {
                    printf(catgets(catd, NL_SCAN, SCAN_LESS,
                        "The file system expects fewer devices than were found. Cannot fix.\n"));
                }
                break;
            }
        } /* for loop */

        if( i == fs_cnt ) {
            printf(catgets(catd, NL_SCAN, SCAN_NOFS,
                "\nNo file system by name %s found on the devices specified, cannot fix\n"), fix_fs);
            exit( 1 );
        }
    } /* if fix */

    exit( 0 );
}

/*
 *      NAME:
 *              parse_options()
 *
 *      DESCRIPTION:
 *              takes the string of options from option_list and sets the
 *              appropriate options flags as well as the file system name
 *              in fix_fs.
 *
 *      ARGUMENTS:
 *              options         (in)    options_list
 *              recreate        (out)   if the recreate option was specified
 *              fix             (out)   if a file system was specified to fix
 *
 *      RETURN VALUES:
 *              success                 0
 *              failure                 -1
 */

static
int
parse_options( char *options, int *recreate, int *fix, int *undo )
{
    char *optptr;

    for( optptr = strtok( options, "," ); optptr;
	optptr = strtok( (char *) NULL, "," ) ) {

        if( strncmp(optptr, "name=", strlen( "name=" ) ) == 0 ) {
            (*fix)++;
            optptr = strchr( optptr, '=' );
            fix_fs = optptr+1;

        } else if (!strcmp(optptr, "recreate")) {
            (*recreate)++;

	} else if( !strcmp(optptr, "undo")) {
	    (*undo)++;

        } else {
            fprintf( stderr, catgets( catd, MS_FSADM_COMMON, COMMON_BADOARG,
                "%s: unknown -o argument '%s'.\n"), Prog, optptr );
            return -1;
        }
    }

    /* Make sure undo was specified by itself */
    if( *undo && ( *recreate || *fix ) ) {
	scan_usage();
	return -1;
    }

    return 0;
}

/*
 *      NAME:
 *              scan_advfs()
 *
 *      DESCRIPTION:
 *              scan AdvFS directories
 *
 *              If flag = SCAN_COUNT, count up the disks.
 *              if flag = SCAN_ADDDEVS, put devices in device table
 *              if flag = SCAN_MATCH, match up fs_table with filesystem dirs
 *
 *      ARGUMENTS:
 *              flag            (in)    see above
 *              number_devs     (out)   total number of devices found
 *
 *      RETURN VALUES:
 *              success                 0 (EOK)
 *              failure                 -1
 */

static
statusT
scan_advfs(
          int flag,
          int *number_devs
          )
{
    DIR *FD, *FDD;
    int n, i, j, got_it, got_fs, fs_num = 0;
    int k;
    char *tab_ptr, *p;
    char *tab_p;
    char save_disk[FS_DIR_MAX_NAME];
    device_info *thisDevice;
    char fs_dev_path[MAXPATHLEN+1];
    char dev_link_path[MAXPATHLEN+1];
    arg_infoT*  infop = NULL;
    
    extern char* advfs_global_dirs[];

    for (k = 0 ; advfs_global_dirs[k] != NULL ; k++) {
        /* open AdvFS directory */
        FD = opendir(advfs_global_dirs[k]);
        if (FD == NULL) {
            continue;
        }

        /* Scan the AdvFS directory and open each file system */
        while ((devs = readdir(FD)) != NULL) {
            if (devs->d_name[0] == '.') {
                /* filesystem names cannot start with "." */
                continue;
            }

            infop = process_advfs_arg(devs->d_name);

            if (infop == NULL) {
                /*  not an AdvFS filesystem */
                continue;
            }

            /* open the file system directory */
            FDD = opendir(infop->stgdir);
            if (FDD == NULL) {
                printf(catgets(catd, MS_FSADM_COMMON, COMMON_NOOPEN2,
                    "%s: open of %s failed.\n" ), Prog, infop->stgdir);
                continue;
            }

            /* read each device in the file system directory */
            got_fs = FALSE;
            while ((devss = readdir(FDD)) != NULL) {

                if (devss->d_name[0] == '.') {
                    continue;
                }

                snprintf(fs_dev_path, MAXPATHLEN+1, "%s/%s", infop->stgdir, 
                    devss->d_name);
                /* Get the path to this device */
                if (readlink(fs_dev_path, dev_link_path, MAXPATHLEN) < 0) {
                    printf(catgets(catd, NL_SCAN, SCAN_NOREADLINK,
                        "Cannot readlink %s\n"), fs_dev_path);
                    continue;
                }

                /* SCAN_COUNT is done automatically anytime scan_advfs 
                 * is called 
                 */

                if( flag == SCAN_MATCH ) {
                    got_it = FALSE;
                    /* scan the fs_table (all devices in all file systems)
                     *  for this device
                     */
                    if (got_fs == FALSE) {
                        for (i = 0; i < fs_cnt; i++) {
                            for (j = 0; j < fs_table[i].device_count; j++) {
                                thisDevice = fs_table[i].device[j];
                                n = strcmp(devss->d_name, thisDevice->dev_name);
                                if (n == 0) {
                                    got_it = TRUE;
                                    thisDevice->correspond_flag = 1;
                                    ++fs_table[i].num_links;
                                    if ( fs_table[i].filesys_name == NULL ) {
                                        fs_table[i].filesys_name =
                                            malloc(devs->d_namlen+1);
                                        strcpy(fs_table[i].filesys_name,
                                            devs->d_name);
                                        got_fs = TRUE;
                                        fs_num = i;
                                    } else if ( strcmp(devs->d_name,
                                        fs_table[i].filesys_name) != 0 ) {
                                        /* Name should match.  If not, a device
                                         * resides in another directory.
                                         * Warn the user later on that the 
                                         * mount of this file system will fail.
                                         */
                                        thisDevice->wrong_dir = 1;
                                        strcpy(thisDevice->wrong_dir_name,
                                            devs->d_name);
                                    }
                                }
                                if (got_it) goto x;
                            }
                        }
                    /* already in the file system, just look for the device */
                    } else {
                        ++fs_table[fs_num].num_links;
                        for (j == 0; j < fs_table[fs_num].device_count; j++) {
                            thisDevice = fs_table[fs_num].device[j];
                            n = strcmp(devss->d_name, thisDevice->dev_name);
                            if (n == 0) {
                                got_it = TRUE;
                                thisDevice->correspond_flag = 1;
                                goto x;
                            }
                        }
                    }
                }

                if( flag == SCAN_ADDDEVS ) {

                    p = __blocktochar( dev_link_path, save_disk );
                    if( p == NULL ) {
                        printf(catgets(catd, NL_SCAN, SCAN_NORAWNAME,
                            "Could not get raw device name for %s\n"),
                            dev_link_path );
                        continue;
                    }
    
                    got_it = FALSE;
                    p = (char *)disk_table_pt;
                    for (i = 0; i < *number_devs; i++) {
                        n = strcmp(save_disk, p);
                        /* if n = 0, it's the same disk */
                        if (n == 0) {
                            got_it = TRUE;
                            continue;
                        }
                        p += sizeof(disk_table_entry);
                    }
                    if (got_it) continue;

                    tab_ptr = (char *)disk_table_pt +
                        (sizeof(disk_table_entry) * (* number_devs) );
                    strcpy(tab_ptr, save_disk);
                }

x:
                (*number_devs)++;
            }
            closedir (FDD);
            free(infop);
        }
        closedir(FD);
    }
    return (EOK);

} /* end scan_advfs */

/*
 *      NAME:
 *              gather()
 *
 *      DESCRIPTION:
 *
 *      ARGUMENTS:
 *              precord (in)    mcell record header
 *
 *      RETURN VALUES:
 *              none
 *
 */
static
void
gather( bsMRT *precord )
{
  char *pdata;
  bsDmnAttrT *ddata;
  bsVdAttrT *vdata;
  bsDmnMAttrT *mdata;
  bsDmnNameT *nmdata;
  bsDmnUmntThawT *umtdata;

  pdata = ((char *) precord) + sizeof(bsMRT);

  switch (precord->type) {
  case BSR_VD_ATTR: /* 3 */
        vdata = (bsVdAttrT *)pdata;
        mnt_id = vdata->vdMntId;
        if (vdata->state == BSR_VD_VIRGIN) {
                state_flag = Virgin;
                state = never;
        }
        if (vdata->state == BSR_VD_MOUNTED) {
                state_flag = Mounted;
                state = mounted;
        }
        if (vdata->state == BSR_VD_DISMOUNTED) {
                /*
                 * the vdMntId is cleared in vd_remove when a volume
                 * is rmvol'ed from a file system
                 */
                if ((vdata->vdMntId.id_sec == 0) &&
                        (vdata->vdMntId.id_usec == 0)) {
                        state = rmvoled;
                        state_flag = Rmvoled;
                } else {
                        state = dism;
                        state_flag = Dismounted;
                }
        }
        blkcnt = vdata->vdBlkCnt;
        break;
 
  case BSR_DMN_ATTR: /* 4 */
        ddata = (bsDmnAttrT *) pdata;
        fs.id_sec = ddata->bfDomainId.id_sec;
        fs.id_usec = ddata->bfDomainId.id_usec;
        break;

  case BSR_DMN_MATTR: /* 15 */
        mdata = (bsDmnMAttrT *)pdata;
        if (mdata->seqNum != 0) {
            diskCnt = mdata->vdCnt;
        }
        break;

  case BSR_DMN_NAME: /* 18 */
        nmdata = (bsDmnNameT *)pdata;
        strcpy(dev_fs_name, nmdata->dmn_name);
        break;

  case BSR_DMN_UMT_THAW_TIME: /* 19 */
        umtdata = (bsDmnUmntThawT *)pdata;
        umnt_thaw = umtdata->dmnUmntThawTime;
        break;

  default:
    break;
  }
} /* end gather */


/*
 *      NAME:
 *              scan_cell()
 *
 *      DESCRIPTION:
 *              scan the devices page 0 mcells for relevant AdvFS stuff
 *
 *      ARGUMENTS:
 *              pdata
 *
 *      RETURN VALUES:
 *              none
 */

static
void
scan_cell( bsMCT *pdata )
{
  bsMRT *precord;

  precord = (bsMRT *) pdata->bsMR0;
  while ((precord < ((bsMRT *) &(pdata->bsMR0[BSC_R_SZ]))) &&
         (precord->type != BSR_NIL) && (precord->bCnt != 0)) {
      gather(precord);
      precord = (bsMRT *) (((char *)precord) +
              roundup(precord->bCnt,sizeof(uint64_t)));
  }
} /* end scan_cell */


/*
 *      NAME:
 *              read_page()
 *
 *      DESCRIPTION:
 *              reads the page
 *
 *      ARGUMENTS:
 *              fd
 *              ppage
 *              lbn
 *
 *      RETURN VALUES:
 *              none
 */

static
void
read_page( int fd, bsMPgT *ppage, int lbn )
{
  lseek(fd, ((unsigned long)lbn) * DEV_BSIZE, L_SET);
  read(fd, ppage, sizeof(bsMPgT));
} /* end read_page */


/*
 *      NAME:
 *              process()
 *
 *      DESCRIPTION:
 *              if -g was specified, then just print out some info
 *		we gather about this device.  Otherwise, collect
 *		the information about this device in the fs_table
 *
 *      ARGUMENTS:
 *              fd      file descriptor of device to be processed
 *
 *      RETURN VALUES:
 *              none
 */

static
void
process( int fd )
{
    advfs_fake_superblock_t *superBlk;
    bsMPgT page;
    int i, boot = FALSE, cn, vers_maj, vers_min, found_one, dev_num, fs_slot=0;
    device_info *thisDevice;
    char diskGroup[MAXPATHLEN];
    char volName[MAXPATHLEN];
    char* ptr;

    superBlk = (advfs_fake_superblock_t *) malloc(ADVFS_SUPER_BLOCK_SZ);
    if (superBlk == NULL) {
        printf( catgets( catd, MS_FSADM_COMMON, COMMON_NOMEM,
                "%s: insufficient memory.\n" ) );
        return;
    }

    /*
     * this read reads the advfs fake superblock, page 8,
     * to check for the advfs magic number
     */
    read_page(fd, (bsMPgT *)superBlk, ADVFS_FAKE_SB_BLK);
    if ( !ADVFS_VALID_FS_MAGIC(superBlk) ) {
        free( superBlk );
        return;
    }
    free( superBlk );

    /*
     * now read the first page of the RBMT
     */
    read_page(fd, &page, RBMT_BLK_LOC);
    vers_maj = page.bmtODSVersion.odv_major;
    vers_min = page.bmtODSVersion.odv_minor;

    /*
     *  Once the logistics are worked out, code will be added to determine
     *  if this is a boot block
     */

    diskCnt = 0;
    dev_fs_name[0] = '\0';
    umnt_thaw.id_sec = umnt_thaw.id_usec = 0;
    /*
     * The LAST mcell on RBMT pages is reserved for an extra extent record
     * to support RBMT extentions so terminate loop at mcell[last-1]. 
     */
    for (cn=0; cn < (BSPG_CELLS-1); cn++) scan_cell(&(page.bsMCA[cn]));

    /* if -g, print out info found on this device */
    if( print_geom ) {
        printf("\n%s", devname);
        printf("\t%08x.%08x", fs.id_sec, fs.id_usec);
        printf(catgets(catd, NL_SCAN, SCAN_MSG1, "\tV%d.%d, %s"), 
            vers_maj, vers_min, state);
        if (boot) {
            printf(catgets(catd, NL_SCAN, SCAN_BOOTABLE, ", bootable"));
        }
        printf("\n");
        if (diskCnt != 0) {
            printf(catgets(catd, NL_SCAN, SCAN_VOLINFS,
                "\t\t\t\t\t%d volume(s) in file system\n"), diskCnt);
        }
        printf(catgets(catd, NL_SCAN, SCAN_TIME,
            "\n\t\tCreated\t\t\t%s "), ctime( (time_t *)&fs ) );
        if (fs_table[fs_slot].mnt_id.id_sec != 0) {
            printf(catgets(catd, NL_SCAN, SCAN_LSTMNT,
	    "\t\tLast mount\t\t%s\n"), ctime( (time_t *) &mnt_id ) );
        }

        printf("\n *** Device Name = %s\n, *** UmtThawTime = \t%08x.%08x",
                dev_fs_name, umnt_thaw.id_sec, umnt_thaw.id_usec);

    /* Fill out the fs_table entry */
    } else {
	char dev_path[MAXPATHLEN];

	/* Scan the fs_table looking for this file system */
	found_one = FALSE;
	for (i = 0; i < fs_cnt + 1; i++) {
	    if ((fs_table[i].fs_id.id_sec      == fs.id_sec) &&
		(fs_table[i].fs_id.id_usec     == fs.id_usec) &&
                (fs_table[i].umnt_thaw.id_sec  == umnt_thaw.id_sec) &&
                (fs_table[i].umnt_thaw.id_usec == umnt_thaw.id_usec)) {
		found_one = TRUE;
		fs_slot = i;
	    }
	}

	/* File system not found, so let's make an entry */
	if (!found_one) {
	    fs_table[fs_cnt].fs_id.id_sec = fs.id_sec;
	    fs_table[fs_cnt].fs_id.id_usec = fs.id_usec;
            fs_table[fs_cnt].umnt_thaw.id_sec = umnt_thaw.id_sec;
            fs_table[fs_cnt].umnt_thaw.id_sec = umnt_thaw.id_sec;
            strcpy(fs_table[fs_cnt].dev_fs_name, dev_fs_name);
	    fs_slot = fs_cnt;
	    fs_cnt++;
	}

	/* Add this device to the file system entry */
	dev_num = fs_table[fs_slot].device_count;

	thisDevice = (device_info *)malloc(sizeof(device_info));
	if( thisDevice == NULL ) {
	    printf( catgets( catd, MS_FSADM_COMMON, COMMON_NOMEM,
		"%s: insufficient memory.\n" ), Prog );
	    return;
	}
	fs_table[fs_slot].device[dev_num] = thisDevice;

	thisDevice->state_flag = state_flag;
	thisDevice->correspond_flag = 0; /* 0 means found device on disk */
        strcpy(thisDevice->dev_fs_name, dev_fs_name);
        thisDevice->umnt_thaw.id_sec   == umnt_thaw.id_sec;
        thisDevice->umnt_thaw.id_usec  == umnt_thaw.id_usec;

	fs_table[fs_slot].device_count++;

	fs_table[fs_slot].version_maj = vers_maj;
	fs_table[fs_slot].version_min = vers_min;
	fs_table[fs_slot].mnt_id = mnt_id;
	if (diskCnt != 0) {
	    fs_table[fs_slot].fs_dev_num = diskCnt;
	}
	ptr = __chartoblock( devname, dev_path );
	if( ptr == NULL ) {
            printf(catgets(catd, NL_SCAN, SCAN_CHAR2BLK,
                "%s: __chartoblock() failed\n"), Prog );
            exit( 1 );
        }
	strcpy( thisDevice->dev_path, dev_path );

	/* If this is a volume manager's volume, parse the name */
	if( advfs_checkvol( dev_path, diskGroup, volName ) ) {
	    strcat( diskGroup, "." );
	    strcat( diskGroup, volName );
	    strcpy( thisDevice->dev_name, diskGroup );
	} else
	    strcpy( thisDevice->dev_name, basename( devname ) );
    }
} /* end process */

void
scan_usage( void )
{
    usage( catgets( catd, NL_SCAN, SCAN_USAGE,
                    "%s [-V] [-g] [-a] [-o option_list] special ...\n" ),
           Prog );
}
