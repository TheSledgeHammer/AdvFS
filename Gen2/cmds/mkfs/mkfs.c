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

#include <stdio.h>
#include <strings.h>              /* stdup(), strcat(), etc ... */
#include <signal.h>
#include <mntent.h>
#include <sys/stat.h>             /* used for stat() */
#include <fcntl.h>                /* used for open, fcntl */
#include <sys/ioctl.h>
#include <sys/ioctl.h>
#include <sys/file.h>
#include <sys/statvfs.h>
#include <advfs/advfs_syscalls.h>
#include <advfs/bs_error.h>       /* needed for BSERRMSG */
#include <advfs/bs_public.h>      /* for BS_MAX_VDI */
#include <errno.h>                /* for errno and messages */
#include <locale.h>
#include <ustat.h>                /* needed for is_mounted() check */
#include "mkfs_advfs_msg.h"

/* #define statements */
#define MAXLEN 256

/* for errno messages */
extern char *sys_errlist[];
#define SYSERRMSG( e ) sys_errlist[e]

extern char *__chartoblock(char *charp, char *blockp);

#undef DEBUGMSG


/* Global variables */
nl_catd catd;
static char *prog = "mkfs";
static char *myopts[] = {         /* Valid values for getsubopt           */ 
#define LOGSIZE 0                 /* in command line parsing of -o flag   */ 
    "logsize",
#define BLKSIZE 1
    "blksize",
#define ROOTFS 2
    "rootfs",
    NULL
};

/* Local function prototypes */
char        *Vcheck_str(char *, int, char **, char *);
long        size_str_to_fobs(const char *); 
int         is_mounted(char *volume, struct stat *vol_stat);
int         process_mflag(arg_infoT* infop);

static void
usage(void)
{
    if (strcmp(prog, "newfs") == 0) {      /* newfs, no -f in usage */

        printf(catgets(catd, 1, USAGE, 
            "usage: %s %s"), prog, "[-F advfs] [-rV] [-B|-R size] [-o logsize=size]\n"
            "            [-o blksize=size] [-o rootfs] [-s size] [-n name] special\n");
    } else {
        printf(catgets(catd, 1, USAGE, 
            "usage: %s %s"), prog, "[-F advfs] [-frV] [-B|-R size] [-o logsize=size]\n"
            "            [-o blksize=size] [-o rootfs] [-s size] [-n name] special\n");
    }

    printf(catgets(catd, 1, USAGE2, 
        "       %s %s"), prog, "[-F advfs] [-mV] special\n");
}

main(int argc, char *argv[])
{
    char *echo_str;           /* copy of command string */
    int   cflag;              /* command flag parsed from getopt */
    int   m_flag = 0;         /* indicates if the "how was this made" flag */
                              /* was used */
    int   s_flag = 0;         /* indicates if the size flag was used */
    int   globalLock = 0;     /* to prevent racing mkfs/newfs */
    int   err = 0;            
    off_t fs_size = 0;        /* stores user specified fs size in blocks */

/* NOTE: - vol_size should be changed from int to bf_vd_blk_t and renamed to
 *         vol_blks(?). Additional changes are needed - including handling
 *         of negative values with bf_vd_blk_t(uint64_t). 
 */
    off_t vol_size = 0;       /* Size of created file system in blocks */
    int   o_flag = 0;         /* indicates if the -o flag was used */
    char *suboptp;            /* pointer used by getsubopt */
    char *subopt_valp;        /* pointer to value returned by getsubopt */
    int   l_flag = 0;         /* indicates if logsize option was used */
/* NOTE: - log_blks should be changed from int to bf_fob_t and renamed
 *         to log_fobs. Additional changes are needed - including handling
 *         of negative values with bf_fob_t(uint64_t). 
 */
    int   log_blks = 0;       /* contains user specified log size */
    int   blksize_flag = 0;   /* indicates userdata pagesize options was used */
    bf_fob_t blksize_fobs = 8;/* userdata pagesize */
    int   isroot = 0;         /* did the user specify root filesystem? */
    int   n_flag = 0;         /* indicates if the user specified fs name */
    char *fsname = NULL;      /* specified file system name */
    char *tstfsname = NULL;   /* to check if fsname already used */
    arg_infoT fsinfo = {0};   /* to check if fsname already used */
    int   V_flag = 0;         /* indicates '-V' user flag */
    int   force_flag = 0;     /* user specified force flag */
    int   exitcode = 0;       /* a value to exit() with if not 1 or 0 */
    int   swap_size = 0;      /* user specified swap size, in megabytes */
    int   boot_reserve = 0;   /* user specifies space left on dev for boot */
    int   D_flag = 0;         /* user specified reuse of existing /dev/advfs
                               * entry
                               */
    char *vol_name = NULL;    /* stores the user specified device */
    struct stat stat_info;    /* stores info returned from stat() */
    char *cp;                 /* used to look for invalid chars in user provided
                               * file system name 
                               */
    int   c;                  /* used for user input */
    char  fs_pathname[MAXPATHLEN+1]; /* used to construct file system 
                                      * directory */
    getDevStatusT dev_size_error;   /* return value from get_dev_size */
    char link_name[MAXPATHLEN];     /* volume link name for ADVFS_DMN_DIR */
    serviceClassT dmn_svc = 0;      /* used for advfs_dmn_init call */
    bf_fob_t bmt_xtnt_fobs = 0;     /* used for advfs_dmn_init call */
    bf_fob_t bmt_prealloc_fobs = 0; /* used for advfs_dmn_init call */
    adv_ondisk_version_t domain_version;  /* used for advfs_dmn_init call */
    bfDomainIdT bf_domain_id;       /* filled in by advfs_dmn_init */
    adv_status_t sts;               /* Status returned by libadvfs calls */
    char *fileset_name = "default"; /* default fileset name */
    uint32_t fset_options = 0;      /* used for advfs_fset_create */
    serviceClassT fset_svc = 0;     /* used for advfs_fset_create */
    bfSetIdT bfset_id;              /* filled in by advfs_fset_create */
    struct statvfs statdev;         /* used for statvfsdev */
    char vm_diskgroup[MAXPATHLEN];  /* Used for advfs_checkvol() */
    char vm_volname[MAXPATHLEN];    /* Used for advfs_checkvol() */
    int dir_exists = 0;             /* Used for overwriting /dev/advfs entry */
    char vol_buf[MAXPATHLEN];
    char fs_devpath[MAXPATHLEN+1];  /* used to construct device pathname */
    char rmCmd[MAXPATHLEN+12] = "/bin/rm -rf "; /* to destruct failed fs */

    
    domain_version.odv_major = 0;
    domain_version.odv_minor = 0;

    /* Grab the file name part of argv[0] */
    if ((prog = strrchr(argv[0], '/')) == NULL) {
        prog = argv[0];
    }
    else {
        prog++;
    }

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_MKFS_ADVFS, NL_CAT_LOCALE);

    /* Only a user with root privileges should run */
    if (geteuid()) {
        fprintf(stderr,
            catgets(catd, 1, EROOT, 
            "Permission denied - user must be privileged to run %s.\n"),
            prog, prog);
        usage();
        exit(1);
    }

    /* ignore interrupt signals */
    if (signal(SIGINT, SIG_IGN) == SIG_ERR) {
        fprintf(stderr,
            catgets(catd, 1, ESIG, "%s: Error modifying signal handler.\n"), 
            prog);
        exit(1);
    }

    /* ignore bad system call signal */
    if (signal(SIGSYS, SIG_IGN) == SIG_ERR) {
        fprintf(stderr,
            catgets(catd, 1, ESIG, "%s: Error modifying signal handler.\n"), 
            prog);
        exit(1);
    }
    /* Save the command string for the -V option.  The getsubopt routine
     * called in the case of -o option will destroy the original string.
     */

    echo_str = Vcheck_str(prog, argc, argv, "mx:o:rF:fs:n:BR:V");

    /* Parse the command line options */

    while ((cflag = getopt(argc, argv, "mx:o:rF:fDs:n:b:BR:V")) != EOF) {
        switch (cflag) {
            case 'b':
                if (blksize_flag) {
                    usage();
                    goto error_1;
                }
                blksize_flag++;
                /* the assumed block size from this function is 1K */
                blksize_fobs = size_str_to_fobs(optarg);
                if (blksize_fobs == -1) {
#ifdef DEBUGMSG
                    printf("invalid blksize argument %s\n", optarg);
#endif
                    usage();
                    goto error_1;
                }
                break;
            case 'm':
                /* we just flag that -m was seen, we must verify it is the
                 * only specified flag (excepting -F) after getopt parsing
                 * or ERR */
                m_flag++;
                break;
            case 'F':
                if (strcmp(optarg, MNTTYPE_ADVFS)) {
                    usage();
                    goto error_1;
                }
                break;
            case 's':
                s_flag++;
                fs_size = (off_t) size_str_to_fobs(optarg);
                if (fs_size == -1) {
#ifdef DEBUGMSG
                    printf("invalid fs_size argument %s\n", optarg);
#endif
                    usage();
                    goto error_1;
                }
                break;
            case 'o':
                o_flag++;
                suboptp = optarg;
                while (*suboptp != '\0') {
                    switch (getsubopt(&suboptp, myopts, &subopt_valp)) {
                        case LOGSIZE:
                            if (l_flag || subopt_valp == NULL) {
                                usage();
                                goto error_1;
                            }
                            l_flag++;
                            log_blks = size_str_to_fobs(subopt_valp);
                            if (log_blks == -1) {
#ifdef DEBUGMSG
                                printf("invalid logsize argument: '%s'\n", subopt_valp);
#endif
                                usage();
                                goto error_1;
                            }
                            break;
                        case BLKSIZE:
                            if (blksize_flag || subopt_valp == NULL) {
                                usage();
                                goto error_1;
                            }
                            blksize_flag++;
                            /* the assumed block size from this function is 1K */
                            blksize_fobs = size_str_to_fobs(subopt_valp);
                            if (blksize_fobs == -1) {
#ifdef DEBUGMSG
                                printf("invalid blksize argument: '%s'\n", subopt_valp);
#endif
                                usage();
                                goto error_1;
                            }
                            break;
                        case ROOTFS:
                            isroot++;
                            break;
                        default:
                            usage();
                            goto error_1;
                    }  /* end switch(getsubopt...) */
                }  /* end while (*suboptp = ...) */
                break;
            case 'D':
                D_flag++;
                break;
            case 'r':
                isroot++;
                break;
            case 'n':
                n_flag++;
                fsname = strdup(optarg);
                break;
            case 'f':
                force_flag++;
                D_flag++; /* causes fs domain dir to be blown away */
                break;
            case 'B':
                boot_reserve++;
                break;
            case 'R':
                /* This size is in MB */
                if (swap_size) {
                    /* duplicate option */
                    usage();
                    goto error_1;
                }
                swap_size = atoi(optarg);
                break;
            case 'V':
                V_flag++;
                break;
            default:
                usage();
                goto error_1;
        }  /* end switch(cflag) */
    }   /* end while(cflag = ...) */

    /* 
     * Currently we only support user data page sizes of 1k, 2k, 4k or 8k.
     * When that changes this can be removed or altered. 
     */
    if (blksize_fobs != 1 && blksize_fobs != 2 &&
        blksize_fobs != 4 && blksize_fobs != 8) {
        usage();
        goto error_1;
    }

    /* the -m flag is exclusive and cannot be used with other flags */
    if (m_flag && (o_flag || s_flag || n_flag || isroot ||
                  D_flag || boot_reserve)) {
        usage();
        goto error_1;
    }
    
    /* duplicate options are not allowed */
    if (m_flag > 1 || s_flag > 1 || n_flag > 1 ||
        V_flag > 1 || isroot > 1 || D_flag > 1 || boot_reserve > 1) {
        usage();
        goto error_1;
    }

    /* Check for missing arguments - the volume */
    if (optind != (argc - 1)) {
        usage();
        goto error_1;
    }

    vol_name = argv[optind];

    /* at this point, if the -m flag was specified, we print out the command
     * line used to build this filesystem
     */
    if(m_flag) {
        /* process_mflag() contains the logic to determine how the filesystem
         * was created */

        arg_infoT*    infop = NULL;;

        infop = process_advfs_arg(vol_name);
        if (infop == NULL) {
            fprintf(stderr, catgets(catd, 1, NOTOURS, 
                "%s: %s is not an AdvFS file system.\n"),
                prog, vol_name);
            exit(1);
        }
 
        if ((infop->is_multivol) && (infop->blkfile[0] == '\0')) {
            fprintf(stderr, catgets(catd, 1, NOMMULTI,
                "%s:  The -m option is not supported for multi-volume filesystems.\n"),
                prog);
            free(infop);
            exit(1);
        }

        if (V_flag) {
            printf("%s", echo_str);
            free(infop);
            exit(0);
        }

        err = process_mflag(infop);
        free(infop);
        exit(err);
    }
    
    if (vol_name[0] != '/') {
        fprintf(stderr, catgets(catd, 1, EPATH, 
            "%s: special must be a complete path\n"), prog);
        goto error_1;
    }

    /* Make sure user supplied device name is valid.
     * stat vol_name and check if it is a block device 
     */

    if (stat(vol_name, &stat_info) < 0) {
        fprintf(stderr, catgets(catd, 1, NOACC, 
            "%s: Cannot access specified device %s\n"), prog, vol_name);
        goto error_1;
    }

    if (S_ISCHR(stat_info.st_mode)) {
        if ((__chartoblock(vol_name,vol_buf)) == NULL) {
            fprintf(stderr, catgets(catd, 1, NOACC, 
                "%s: Cannot access specified device %s\n"), prog, vol_name);
            goto error_1;
        }

        if (stat(vol_buf,&stat_info) < 0) {
            fprintf(stderr, catgets(catd, 1, NOACC, 
                "%s: Cannot access specified device %s\n"), prog, vol_name);
            goto error_1;
        }
        vol_name = vol_buf;
    }

    if (!S_ISBLK(stat_info.st_mode)) {
        fprintf(stderr, catgets(catd, 1, EBLK, 
            "%s: Specified device %s is not a block special device\n"),
            prog, vol_name);
        goto error_1;
    }
    
    if (is_mounted(vol_name, &stat_info)) {
        fprintf(stderr,
            catgets(catd, 1, EMNT, "%s: device %s already mounted\n"), 
            prog, vol_name);
        goto error_1;
    }

    /* If no fsname was provided on the command line, then create one to 
     * use internally.
     */
    if (!fsname) {
        fsname = advfs_fsname(vol_name);
        if (fsname == NULL) {
            fprintf(stderr, catgets(catd, 1, EDEV, 
                "%s: Error processing specified device name %s\n"), 
	        prog, vol_name);
            goto error_1;
        }
    }
    else {
        if (strlen(fsname) >= BS_DOMAIN_NAME_SZ) {
            fprintf(stderr, catgets(catd, 1, ELIMIT, 
                "%s: file system name '%s' exceeds limit of %d characters\n"),
                prog, fsname, (BS_DOMAIN_NAME_SZ - 1));
            goto error_1;
        }

        /*  Don't allow fsnames to begin with a "." */

        if (*fsname == '.') {  
            fprintf(stderr, catgets(catd, 1, BADFSNAME, 
                "%s:  file system name must not begin with a \".\"\n"),
                 prog);
            goto error_1;
        }

        if (cp = strpbrk(fsname, "/# :*?\t\n\f\r\v")) {
            fprintf(stderr, catgets(catd, 1, BADCHAR, 
                "%s: invalid character '%c' in file system name '%s'\n"),
                prog, *cp, fsname);
            goto error_1;
        }
    }

    /* Checks if the user specified device belongs to a logical volume. */
    if (!force_flag && (advfs_is_lvmdev(vol_name) == 1)) {
        fprintf(stderr, catgets(catd, 1, EVOLM, 
            "%s: %s belongs to a volume manager. Please use the appropriate volume name.\n"), 
            prog, vol_name);
        exit(1);
    }

    if (V_flag) {
        printf("%s", echo_str);
        exit(0);
    }

    /* check if the super block has a valid magic number.  
     * valid magic numbers known are the ones supported by statvfsdev()
     * and advfs_valid_sblock() (AdvFS).  If the force flag
     * was set, then the check is skipped.
     */
    if (!force_flag && (!(statvfsdev(vol_name, &statdev)) || 
                (advfs_valid_sblock(vol_name, &err) == 1))) {
        fprintf(stdout, catgets(catd, 1, OVERWR, 
            "%s: There appears to be a valid file system on %s.  If you continue, the current file system will be overwritten.\n"), 
            prog, vol_name);
        do {
            fprintf(stdout, catgets(catd, 1, CONT, "CONTINUE? [y/n] "));
            (void) fflush(stdout);
            c = getc(stdin);
            while (c != '\n' && getc(stdin) != '\n') {
                if (feof(stdin))
                    exitcode = 2;
                    goto error_1;
            }
            if (c == 'n' || c == 'N') {
                exitcode = 2;
                goto error_1;
            }
        } while (c != 'y' && c != 'Y');
    }

    /* Create the /dev/advfs directory if it's missing */
    if (create_advfs_hierarchy()) {
        goto error_1;
    }

    
    /* we need to take out the big lock for mkfs, since we are initializing
     * a device.  It could be possible that two mkfs processes are asked to
     * create different filesystems on the SAME device.  Just locking the
     * filesystem directory won't block this */
    globalLock = advfs_global_lock(FALSE, &err);
    if (globalLock < 0) {
        fprintf( stderr, catgets(catd, 1, EGLOB, 
            "%s: error locking AdvFS global lock; [%d] %s\n"),
            prog, err, SYSERRMSG( err ) );
        goto error_1;
    }
    
    /* need to prevent name collisions with TCR shared and make sure
     * nothing from this filesystem is mounted if it already exists 
     */
    tstfsname = arg_to_fsname(fsname, &fsinfo);
    if (tstfsname != NULL) {
        adv_mnton_t*   mntArray = NULL;
        int            i;

        if (fsinfo.tcr_shared) {
            fprintf(stderr, catgets(catd, 1, EXISTS, 
                "%s: file system '%s' already exists\n"),
                prog, fsname);
        }
       
        err = advfs_get_all_mounted(&fsinfo, &mntArray);
        if (err > 0) {
            fprintf(stderr,
                catgets(catd, 1, EMNT, "%s: device %s already mounted\n"), 
                prog, vol_name);
            free(mntArray);
        }
        
        free(tstfsname);
        goto error_1;
    }

    snprintf(fs_pathname, MAXPATHLEN, "%s/%s", ADVFS_DMN_DIR, fsname);

    if (!lstat(fs_pathname, &stat_info)) {
        if (D_flag) {
            dir_exists++;
        } else {
            fprintf(stderr, catgets(catd, 1, EXISTS, 
                "%s: file system '%s' already exists\n"),
                prog, fs_pathname);
            goto error_1;
        }
    }

    /* Make sure the volume is not in in use by another AdvFS file system */
    if (!volume_avail(prog, fsname, vol_name)) {
        goto error_1;
    }

    /* get the size (in DEV_BSIZE blocks) of the specified device */
    dev_size_error = get_dev_size(vol_name, (char *)NULL, &vol_size);
    if (dev_size_error != GDS_OK) {
        fprintf(stderr, catgets(catd, 1, ESIZE, 
            "%s: cannot get device size; '%s'\n"), 
            prog, GDSERRMSG(dev_size_error));
/* uhhh, one suspects we ought to handle this error and not just
 * start making things up...
 */
    }

    /* Subtract any requested swap or boot space out of file system size */
    if (swap_size) {
        vol_size -= (off_t) ((off_t)swap_size * 
                (off_t)((1024LL * 1024LL) / (off_t)DEV_BSIZE));
    }
    if (boot_reserve) {
#define BOOT_VOL "/usr/lib/uxbootlf"

        struct stat  bst;

        if (stat(BOOT_VOL, &bst) == 0) {
            vol_size -= (off_t) ((bst.st_size + (DEV_BSIZE - 1)) / DEV_BSIZE);
        } else {
            /* Taken from hfs/mkfs.c */
            vol_size -= (off_t) ((691 * 1024) / DEV_BSIZE);
        }
    }

    if (vol_size <= 0) {
        fprintf(stderr, catgets(catd, 1, ESPACE, 
            "%s: Insufficient space for boot/swap reservation\n"), 
            prog);
        goto error_1;
    }

    /* If a file system size was specified, update the vol_size accordingly */
    if (fs_size) {
        if (fs_size > vol_size) {
            fprintf(stderr, catgets(catd, 1, SMALL, 
                "%s: invalid file system size.  Specified size %ld is greater than device capacity %ld (blocks)\n"),
                prog, fs_size, vol_size);
            goto error_1;
        }
        vol_size = fs_size;
    }
    
    /* set up the rm command to use if failure */
    strcat(rmCmd, fs_pathname);

    /* Create file system directory for volume info */
    if (dir_exists && D_flag) {
        /* blow away the old directory and it's contents */
        system(rmCmd);
    }

    if (mkdir(fs_pathname, 0755)) {
        fprintf(stderr, catgets(catd, 1, EMKDIR, 
            "%s: cannot create dir '%s'; [%d] %s\n"), 
	    prog, fs_pathname, errno, strerror(errno));
        goto error_1;
    }

    /* TODO: Add check for specified log blocks versus bmt extend size 
     */
    sts = advfs_dmn_init(fsname,
                        (isroot ? 1 : BS_MAX_VDI),
                        (bf_fob_t)log_blks,   /* cast OK since non-neg here */
                        blksize_fobs,
                        dmn_svc,              /* log services. 0 for now */ 
                        dmn_svc,              /* tag services. 0 for now */
                        vol_name,
                        dmn_svc,              /* services vol provides. 0 for now */
                        (bf_vd_blk_t)vol_size,/* cast OK since non-neg here */
                        bmt_xtnt_fobs,
                        bmt_prealloc_fobs,
                        domain_version,
                        &bf_domain_id);

    if (sts != EOK) {
        fprintf(stderr, catgets(catd, 1, EINIT, 
            "%s: file system init error %d : '%s'\n"), 
            prog, sts, BSERRMSG(sts));
        goto error_2;
    }

    /*  create the AdvFS block device */
    err = advfs_make_blockdev(fs_pathname, fileset_name, 0, 0);

    if (err != 0) {
        fprintf(stderr, catgets(catd, 1, BADDEV, 
            "%s: cannot create device '%s/%s'\n"),
            prog, fs_pathname, fileset_name);

        goto error_2;
    }

    snprintf(fs_devpath, MAXPATHLEN, "%s/%s", fs_pathname, ".stg");

    if (mkdir(fs_devpath, 0755)) {
        fprintf(stderr, catgets(catd, 1, EMKDIR, 
            "%s: cannot create dir '%s'; [%d] %s\n"), 
	    prog, fs_devpath, errno, strerror(errno));
        goto error_2;
    }

    /* create link for volume in file system directory 
     * If device is logical volume crete symlink name that has disk 
     * group and logical volume name 
     */
    link_name[0] = '\0';
    strcpy(link_name, fs_devpath);
    if (advfs_checkvol(vol_name, vm_diskgroup, vm_volname)) {
        char temp_str[MAXPATHLEN];
        
        snprintf(temp_str, MAXPATHLEN, "/%s.%s", vm_diskgroup, vm_volname);
        strcat(link_name, temp_str);
    }
    else {
        strcat(link_name, strrchr(vol_name, '/'));
    }
#ifdef DEBUGMSG
    printf("creating a link at %s pointing to  %s\n", link_name, vol_name);
#endif
    if (symlink(vol_name, link_name)) {
        fprintf(stderr, catgets(catd, 1, ELINK, 
            "%s: cannot create symlink '%s @--> %s'; [%d] %s\n"),
            prog, link_name, vol_name, errno, strerror(errno));
        goto error_2;
    }

#ifdef DEBUGMSG
    printf("calling advfs_fset_create with domain name %s\n", fsname);
#endif
    sts = advfs_fset_create(fsname, 
                           fileset_name,
                           fset_svc,       /* req services. 0 for now */
                           fset_options,
                           (adv_gid_t) 0,      /* group id for quota files */
                           &bfset_id);
    if (sts != EOK) {
        fprintf(stderr, catgets(catd, 1, ERRCR, 
            "%s: file system creation error %d : '%s'\n"), 
	    prog, sts, BSERRMSG(sts));
        goto error_2;
    }

    /* set the multi-volume flag if this is a named storage domain */
    if (n_flag) {
        (void)advfs_set_multivol(vol_name);
    }
    
    /* unlock the global directory */
    advfs_unlock( globalLock, &err );

    /* fsname was malloced either in command line processing or when
     * a name was generated.  The memory should be freed now. 
     */
    free(fsname);
    exit(0);

error_2:
    err = system(rmCmd);
    if ( (err == -1) || (WEXITSTATUS(err) != 0) ) {
        fprintf(stderr, "Warning: unable to rmdir %s [%d] %s", 
                fs_pathname, errno, strerror(errno));
    }
    
error_1:
    /* unlock the global directory */
    if(globalLock != -1) {
        advfs_unlock( globalLock, &err );
    }

    if (fsname)
        free(fsname);

    /* this allows us to exit with a non-standard error if need be -- 
     * i.e. if the user chose "N" to the question about overwriting 
     * the device */
    if (exitcode) {
        exit(exitcode);
    }

    exit(1);
}

/* NAME: Vcheck_str(cmd, argc, argv, optstr)
 *
 * DESCRIPTION:
 *   This routine is for parsing the command string for -V option.
 *   Parses all options including suboptions such as -o. 
 *   Automatically adds -F hfs in the resulting string.
 *   Do not include -V in the resulting string.  
 *   
 * INPUT:
 *   cmd        : command name to be included in the string
 *   argc       : argument count
 *   argv       : argument vector
 *   optstr     : options recognized by the command             
 * 
 * RETURN VALUES:       
 *   cmd_str    : the resulting command string 
 */

char *
Vcheck_str(char *cmd, int argc, char **argv, char *optstr)
{
    int             c, save_optind;  /* stores option flag and index */
    extern char     *optarg;         /* pointer to option flag for getopt */
    extern int      optind, opterr;
    char            workstr[MAXLEN];
    static char     cmd_str[MAXLEN];
    int             save_opterr;

    save_optind = optind;
    save_opterr = opterr;
    opterr = 0;
    optind = 1;
    sprintf(cmd_str, "%s -F advfs ", cmd);
    while ((c = getopt(argc, argv, optstr)) != EOF) {
            switch (c) {
            case 'V':
                    break;
            case 'F':
                    break;
            default:
                    sprintf(workstr, "-%c ", c);
                    strcat(cmd_str, workstr);
                    if (optarg) {
                            sprintf(workstr, "%s ", optarg);
                            strcat(cmd_str, workstr);
                    }
                    break;
            }
    }
    while (optind < argc) {
            sprintf(workstr, "%s ", argv[optind++]);
            strcat(cmd_str, workstr);
    }
    strcat(cmd_str, "\n");
    optind = save_optind;
    opterr = save_opterr;

    return((char *) cmd_str);
}

/* NAME: size_str_to_fobs(char *)
 *
 * DESCRIPTION: 
 *
 *     This function takes a string passed in and converts it
 *     to 1K fobs.
 *
 * INPUT: 
 *     usr_str : user specified size string.
 *        VALID format of string -
 *        %d - Assumes user entered size in FOBs
 *        %dK - size in kilobytes
 *        %dM - size in megabytes
 *        %dG - size in gigabytes
 *
 * RETURN VALUES: 
 *     Returns size in 1K blocks.
 *     Returns -1 if the size string is not valid
 *
 */

long
size_str_to_fobs(const char *usr_str) {
    char *next_ptr = NULL;
    uint64_t user_size = 0;
    uint64_t num_fobs = 0;

    user_size = strtol(usr_str, &next_ptr, 10);
    if (next_ptr == NULL || strlen(next_ptr) > 1) {
        return -1;
    }
    switch((int)next_ptr[0]) {
        case '\0':
        case 'K':
        case 'k':
            num_fobs = user_size;
            break;
        case 'M':
        case 'm':
            num_fobs = user_size * 1024; 
            break;
        case 'G':
        case 'g':
            num_fobs = user_size * 1024 * 1024;
            break;
        case 'T':
        case 't':
            num_fobs = user_size * 1024 * 1024 * 1024;
            break;
        default:
            return -1;
    }

    return num_fobs;
}

/* 
 * is_mounted(): 
 *              check whether or not the specified device is a mounted
 *              file system. This function uses ustat() 
 *              to figure out it's mounted or not.
 *
 *              Very similar to the is_mounted routine in libfs.
 */

int         
is_mounted(char *volume, struct stat *vol_stat)
{
   	struct ustat ust;

	if (ustat(vol_stat->st_rdev, &ust) >= 0)
	  	return(1);

        return(0);
}

/*
 *  FUNCTION: process_mflag
 *  NOTES: tries to intelligently determine how an AdvFS filesystem
 *         was created
 * 
 *  return value : none
 */

int
process_mflag(arg_infoT* infop)
{
    int is_root = 0;
    int err;
    char *root_fsname = NULL;
    char command_line[MAXPATHLEN+1] = { 0 };
    getDevStatusT dev_size_error = 0; /* return value from get_dev_size */
    off_t vol_blocks = 0;
    struct statvfs statvfsbuf = { 0 };
    adv_bf_dmn_params_t dmnParams = { 0 };

    if (advfs_valid_sblock(infop->blkfile, &err) != 1) {
        fprintf(stderr, catgets(catd, 1, NOTOURS, 
            "%s: %s is not an AdvFS file system.\n"),
            prog, infop->blkfile);
        return(1);
    }
    
    snprintf(command_line, MAXPATHLEN, "%s -F advfs", prog);  

    if (infop->is_multivol) {
        strncat(command_line, " -n ", MAXPATHLEN);
        strncat(command_line, infop->fsname, MAXPATHLEN);
    }

    /* determine if we are querying root */
    root_fsname = mountpoint_to_fsname("/", NULL);

    if ((root_fsname != NULL) && (!strcmp(root_fsname, infop->fsname))) {
        is_root = 1;
        free(root_fsname);
    }
    
    /* get the size (in DEV_BSIZE blocks) of the specified device */
    dev_size_error = get_dev_size(infop->blkfile, (char *)NULL, &vol_blocks);
    if (dev_size_error != GDS_OK) {
        fprintf(stderr, catgets(catd, 1, ESIZE, 
            "%s: cannot get device size; '%s'\n"), 
            prog, GDSERRMSG(dev_size_error));
        return(1);
    }
    
    /* now we query the kernel for the logsize, blksize, and root flag */
    advfs_skip_on_disk_version_check();
    if(advfs_get_dmnname_params( infop->fsname, &dmnParams ) != EOK) {
        /* we might be querying root that hasn't undergone a mount update */
        if(!is_root ||
           advfs_get_dmnname_params( "generic", &dmnParams) != EOK) {
            fprintf(stderr, catgets(catd, 1, EFSPAR,
                "%s: error reading file system parameters for '%s'\n"),
                prog, infop->fsname);
            return(1);
        }
    }

    /* now we statvfsdev() the device to get the vfs size */
    if (statvfsdev(infop->blkfile, &statvfsbuf) < 0) {
        fprintf(stderr, catgets(catd, 1, EFSPAR,
            "%s: error reading file system parameters for '%s'\n"),
            prog, infop->fsname);
        return(1);
    }

    if(vol_blocks > statvfsbuf.f_blocks) {
        strncat(command_line, " -s ", MAXPATHLEN);
        strncat(command_line, ultostr(statvfsbuf.f_blocks, 10), MAXPATHLEN);
    }
    
    if(dmnParams.maxVols == 1) {
        strncat(command_line, " -r", MAXPATHLEN);
    }
    strncat(command_line, " -o logsize=", MAXPATHLEN);
    strncat(command_line, ultostr(dmnParams.ftxLogFobs, 10), MAXPATHLEN);
    
    strncat(command_line, " -o blksize=", MAXPATHLEN);
    strncat(command_line, ultostr(dmnParams.userAllocSz, 10), MAXPATHLEN);

    fprintf(stdout, "%s %s\n", command_line, infop->blkfile);

    return(0);
}

/* end mkfs.c */

