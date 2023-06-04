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

#ifndef lint
#endif

#include <sys/param.h>
#include <sys/file.h>
#include <sys/time.h>
#include <sys/wait.h>
#include <sys/quota.h>
#include <errno.h>
#include <stdio.h>
#include <signal.h>
#include <string.h>
#include <sys/mount.h>                /* base and CFS mount flags */
#include <sys/fs/advfs_public.h>       /* AdvFS and generic mount flags */
#include <mntent.h>                   /* needed for mount options */
#include <advfs/advfs_syscalls.h>
#include <advfs/bs_error.h>
#include <tcr/clu.h>                  /* for clu_is_member() */
#include <dirent.h>

#include <locale.h>
#include "mount_advfs_msg.h"
nl_catd catd;


extern char *sys_errlist[];

#define ERRMSG( e ) sys_errlist[e]

#define OPTIONS "ero:F:V"  /* used to parse command line w/ getopt */

#define RW_OPT(type) (strcasecmp(type, MNTOPT_RW)==0)
#define MAXLEN 256

static void parse_std_options(char *);
#ifdef DEBUG
static void print_flags(void);
#endif
char *Vcheck_str(char *, int, char**, char *);
static void usage(void);
int advfs_valid_sblock(char *, int *);

/* TODO: Change string literals to MNTOPT macros where possible.  This requires
 * modififying mntent.h. Any option that is valid in /etc/fstab and 
 * /etc/mnttab needs a corresponding MNTOPT macro in mntent.h
 */
char *myopts[] = {
#define OPT_RO 0
    MNTOPT_RO,
#define OPT_RW 1
    MNTOPT_RW,
#define OPT_SUID 2
    MNTOPT_SUID,
#define OPT_NOSUID 3
    MNTOPT_NOSUID,
#define OPT_REMOUNT 4
    MNTOPT_REMOUNT,
#define OPT_DIRTY 5
    "dirty",
#define OPT_ATIMES 6
    MNTOPT_ATIMES,
#define OPT_NOATIMES 7
    MNTOPT_NOATIMES,
#define OPT_QUOTA 8
    MNTOPT_QUOTA,
#define OPT_SERVER_ONLY 9
    MNTOPT_SERVERONLY,   /* Trucluster-only, defined in mount.h */
#define OPT_SERVERNAME 10
    MNTOPT_SERVER,       /* Trucluster-only, defined in mount.h */
#define OPT_BOOTFS 11
    MNTOPT_BOOTFS,       /* Trucluster-only, defined in mount.h */
#define OPT_DIRECTIO 12
    "directio",
#define OPT_READAHEAD 13
    "readahead",
#define OPT_NOREADAHEAD 14
    "noreadahead",
#define OPT_NOAUTO 15
    MNTOPT_DEFAULTS,
#define OPT_DEFAULTS 16
    MNTOPT_NOAUTO,
    NULL
};

u_long gen_mntflg = 0;  /* stores generic mount options */
u_long adv_mntflg = 0;  /* stores advfs specific mount options */
char *cfs_servername = NULL;
int verbose = 0;

int
main(int argc, char **argv)
{
    extern char *optarg;
    extern int optind;
    register int cnt;
    int ch;           /* holds flag parsed from command string */
    int ret;          /* return value from mountfs() */
    int Vflag = 0;    /* Indicates if user passed in -V flag */
    char* remaining, *fsopts = NULL, *echo_str;

    (void) setlocale(LC_ALL, "");
    catd = catopen(MF_MOUNT_ADVFS, NL_CAT_LOCALE);

    /* check for root */
    if (geteuid()) {
        fprintf(stderr, catgets(catd, 1, ERR_PERM,
                    "mount: Permission denied - must be privileged user to mount an AdvFS file system.\n"));
        exit(1);
    }

    /* Copy the command string for the Voption */
    echo_str = Vcheck_str(argv[0], argc, argv, OPTIONS);

    while ((ch = getopt(argc, argv, OPTIONS)) != EOF ) {
        switch((char)ch) {
            case 'F':
                if (strcmp(optarg, MNTTYPE_ADVFS)) {
                    fprintf(stderr, catgets(catd, 1, ERR_NOT_ADVFS,
                                "mount: file system type not 'advfs'\n"));
                    usage();
                    /* NOT REACHED */
                }
                break;
            case 'e':
                verbose++;
                break;
            case 'o':
                fsopts = strdup(optarg);
                parse_std_options(fsopts);
                break;
            case 'r':
                gen_mntflg |= MS_RDONLY;
                break;
            case 'V':
                Vflag++;
                break;
            case '?':
            default:
                usage();
                /* NOT REACHED */
        }
    }

    if (verbose > 1 || Vflag > 1)
        usage();

    if (argc - optind != 2) {
        usage();
    }
    argc -= optind;
    argv += optind;

    if (Vflag) {
        printf("%s", echo_str);
        if (fsopts)
            free(fsopts);
        exit(0);
    }
#ifdef DEBUG
    print_flags();
#endif
    ret = mountfs(argv[0], argv[1]);
    if (fsopts)
        free(fsopts);
    if (cfs_servername)
        free(cfs_servername);
    exit (ret);
}

int
mountfs(char *spec, char *mntdir)
{
    int status;
    pid_t pid;
    int err, argc, i, lkHndl = -1, seed;
    int createLock = 0;
    struct advfs_args args;
    struct cfs_args cfsargs;
    char *argp;
    struct timeval tv;
    struct timezone *tz;
    char volpath[MAXPATHLEN+1];
    char vol_link_path[MAXPATHLEN+1];
    char symlink_path[MAXPATHLEN+1];
    DIR *dmndir;
    struct dirent *vol;
    struct stat st;
    int datalen = 0;              /* used for mount syscall */
    char *fstype;                 /* used for mount syscall */
    char quotafile[MAXPATHLEN];   
    arg_infoT* arginfo;

    /* Convert the passed in filespec to the AdvFS filesystem.  Accepts
     * filesystem name, /dev/advfs/<x>, /cdev/advfs/<x> or single device
     * spec (i.e., /dev/disk/xxx).
     */

    arginfo = process_advfs_arg(spec);

    if (arginfo == NULL) {
        fprintf(stderr, catgets(catd, 1, ERR_INVALID_DEV, 
            "mount: ERROR - %s is an invalid device or cannot be opened.\n"));
        return(1);
    }

    /*  Disallow mounting a named filesystem by its component devices */
    if (arginfo->is_multivol && (arginfo->arg_type != ADVFS)) {
        fprintf(stderr, catgets(catd, 1, ERR_INVALID_DEV, 
            "mount: ERROR - %s is an invalid device or cannot be opened.\n"));
        return(1);
    }

    /* Make sure the mount directory is valid */
    if (mntdir[0] != '/') {
        fprintf(stderr, catgets(catd, 1, ERR_ABSPATH, 
		"mount: ERROR - absolute path name is required for %s\n"), mntdir);
        return(1);
    }
    if (stat(mntdir,&st)) {
        fprintf(stderr,"mount: ");
        perror(mntdir);
        return(errno);
    } else if ( !(S_ISDIR(st.st_mode)) ) {
        fprintf(stderr, catgets(catd, 1, ERR_INVALID_MNTDIR, 
		"mount: ERROR - %s must be a directory\n"), mntdir);
        return(1);
    }

#ifdef DEBUG
    printf("mount (DEBUG) passed in spec: %s\tfspath: %s\n", spec, arginfo->fspath);
#endif

    /* lock the filesystem directory so no changes can be made to it
     * while we're in the process of mounting 
     */
    lkHndl = advfs_fspath_lock(arginfo->fspath, FALSE, FALSE, &err);
    if (lkHndl < 0) {
        /*
         * If the filesystem holding /dev/advfs is a read-only file system 
         * and the lock file has not been created yet, we are going to
         * run into a chicken and egg problem.  So don't fail if 
         * errno = EROFS.
         */
        if (EROFS != err) {
            fprintf(stderr, catgets(catd, 1, ERR_LOCK, 
                "mount: error locking '%s'; [%d] %s\n"),
                arginfo->fspath, err, ERRMSG(err));
            return(1);
        }
    }
#ifdef MOUNT_SYMLINKS
    /* If the spec passed in is a symlink, read the link and continue 
     * processing */
    if(!lstat(spec, &st) && S_ISLNK(st.st_mode)) {
        if (readlink(spec, symlink_path, MAXPATHLEN) < 0) {
            perror("readlink");
        }
        /* point the spec argument at the link's contents.
         * The old spec is not saved since we don't need it */ 
        spec = symlink_path;
    }
#endif /* MOUNT_SYMLINKS */


    /*
     * does not need to check ADVFS_DMN_DIR for fs config info if read-only
     * or on a remount.
     */
    if (!(gen_mntflg & MS_RDONLY) && !(gen_mntflg & MS_REMOUNT)) {
        dmndir = opendir(arginfo->stgdir);
        if (dmndir == NULL) {
            fprintf(stderr, catgets(catd, 1, ERR_INVALID_CONFIG_1, 
                        "mount: Error opening %s for %s. This\n"
                        "directory stores configuration information needed "
                        "by the AdvFS file system.\n"),
                    arginfo->stgdir, spec);
            fprintf(stderr, catgets(catd, 1, ERR_INVALID_CONFIG_2,
                        "mount: Either the specified file system does not exist "
                        "or the\ndirectory has become corrupt. Please see fsadm "
			"for a possible fix.\n"));
            fprintf(stderr, catgets(catd, 1, ERR_FSADM_SCAN, 
			"mount: Please use fsadm to scan the file system config for errors\n"));
            goto _return;
        }

        /* Make sure that the volumes in the file system config directory
         * refer to valid block special devices.
         */
        while ((vol = readdir(dmndir)) != NULL) {
            if (vol->d_name[0] == '.') {
                continue;
            }
            snprintf(volpath, MAXPATHLEN, "%s/%s", dmndir, vol->d_name);
            if (readlink(volpath, vol_link_path, MAXPATHLEN) < 0) {
                continue;
            }

            if (stat(vol_link_path, &st) < 0 ||
                    !S_ISBLK(st.st_mode)) {
                /* If stat() fails, execute this code.  If          */
                /* stat() succeeds, then S_ISBLK check is made,     */
                /* and again this error message will need to happen */
                fprintf(stderr, catgets(catd, 1, ERR_NOT_BLOCKDEV, 
                            "mount: ERROR - %s is not a block device.\n"), 
                        vol_link_path);
                goto _return;
            }
            /* Check for the AdvFS magic number in the superblock */
            status = advfs_valid_sblock(vol_link_path, &err);
            if (status == -1) {
                fprintf(stderr, catgets(catd, 1, ERR_SYSCALL, 
                            "mount: ERROR - %s"), strerror(err));
                closedir(dmndir);
                goto _return;
            }
            else if (status == 0) {
                /* AdvFS magic number not found */
                fprintf(stderr, catgets(catd, 1, ERR_MAGIC_1, 
                            "mount: ERROR - %s does not contain the valid "
                            "AdvFS identifier.\n"), vol_link_path);
                fprintf(stderr, catgets(catd, 1, ERR_MAGIC_2, "mount: Either "
                            "the device does not contain a valid AdvFS file "
                            "system\nor the AdvFS identifier has become "
                            "corrupt.\n"));
                fprintf(stderr, catgets(catd, 1, ERR_FSADM_SCAN, 
			    "mount: Please use fsadm to scan the file system config for errors\n"));
                fprintf(stderr, catgets(catd, 1, ERR_FSCK, 
			    "mount: Please use fsck to check the file system\n"));
                closedir(dmndir);
                goto _return;
            }
        }
        closedir(dmndir);
    }

    if (arginfo->is_multivol) {
        args.fspec = malloc(strlen(arginfo->fspath) + strlen(arginfo->fset) + 2);
    } else {
        args.fspec = malloc(strlen(arginfo->blkfile) + 1);
    }

    if (args.fspec == NULL) {
        fprintf(stderr, catgets(catd, 1, OUTOFMEM, 
                    "mount: ERROR - could not allocate memory.\n"));
        return(1);
    }

    if (arginfo->is_multivol) {
        sprintf(args.fspec, "%s/%s", arginfo->fspath, arginfo->fset);
    } else {
        strcpy(args.fspec, arginfo->blkfile);
    }

    /* Set the flags in the private advfs structure and add the
     * MS_DATA flag to the generic flags field.  This indicates to
     * the mount system call that we are passing a file system specific
     * private data structure.
     */
    args.fsid = 0;
    args.adv_flags = adv_mntflg;
    gen_mntflg |= MS_DATA;

#ifdef DEBUG
    printf("mount (DEBUG): args.fspec = %s\n"
           "mount (DEBUG): "
           "args.fsid = %d\n", args.fspec, args.fsid);
#endif
    /* If the server= option was specified, then we will need to pass a 
     * cfs_args structure to mount. The cfs_args structure will contain 
     * a pointer to the PFS advfs_args structure.
     */
    if (cfs_servername) {
        cfsargs.servername = cfs_servername;
        cfsargs.fstype = MNTTYPE_ADVFS;
        cfsargs.dataptr = (caddr_t)&args;
        cfsargs.datalen = sizeof(struct advfs_args);
        argp = (caddr_t)&cfsargs;
        datalen = sizeof(struct cfs_args);
        fstype = MNTTYPE_CFS;
    }
    else {
        argp = (caddr_t)&args;
        datalen = sizeof(struct advfs_args);
        fstype = MNTTYPE_ADVFS;
    }

    if (mount(args.fspec, mntdir, gen_mntflg, fstype, argp, datalen))
    {
        fprintf(stderr, catgets(catd, 1, REPORT, "%s on %s: \n"), spec, mntdir);

        switch (errno) {
            case EMFILE:
                fprintf(stderr, 
                        catgets(catd, 1, ERR_TBL_FULL, 
			"mount: ERROR - file system or mount table full\n"));
                break;
            case EINVAL:
                if (gen_mntflg & MS_REMOUNT) {
                    fprintf(stderr, catgets(catd, 1, ERR_NO_MATCH, 
			"mount: ERROR - Specified device does not match\nmounted device\n"));
                } else {
                    fprintf(stderr, catgets(catd, 1, ERR_INVALID_FS, 
			"mount: ERROR - Device does not contain a valid\nAdvFS file system\n"));
                }
                break;
            case ENOENT:
                fprintf(stderr, 
                        catgets(catd, 1, ERR_NO_FS, 
			"mount: ERROR - No such file system or mount directory\n"));
                break;
            case EOPNOTSUPP:
                fprintf(stderr, 
                        catgets(catd, 1, ERR_NOT_SUPP, 
			"mount: ERROR - Operation not supported\n"));
                break;
            case ENODEV:
                fprintf(stderr, catgets(catd, 1, ERR_EQUALS_EITHER, "mount: ERROR = %s or %s\n"), 
                        BSERRMSG(E_BAD_DEV), BSERRMSG(E_ADVFS_NOT_INSTALLED));
                break;
            default:
                perror((char *)NULL);
                break;
        }
        goto _return;
    }

    (void) snprintf(quotafile, MAXPATHLEN, "%s/%s", mntdir, "quotas");

    if ( (gen_mntflg & MS_QUOTA) &&
         (quotactl(Q_QUOTAON, args.fspec, 0, quotafile) == -1)) {
            (void) fprintf(stderr,
                "mount: WARNING: could not enable quotas on %s", mntdir);
            perror(" ");
    } 

    if (lkHndl != -1) {
        advfs_unlock( lkHndl, &err );
    }

    /* cleanup mount argument stack */

    return(0);

_return:
    /* No reason to call advfs_unlock if lock_file failed */
    if (lkHndl != -1) {
        advfs_unlock( lkHndl, &err );
    }
    return(1);
}

/* NAME: parse_std_options(char *)
 *
 * DESCRIPTION: 
 *   Parse -o options from the command line and update the mntflg variable 
 *   accordingly.  This function will modify the string passed in.  If some
 *   of the options are not processed, then the utility will exit.
 *
 * INPUT:
 *   fsopts : command line option string passed in with '-o' flag 
 *
 * RETURN VALUES:
 *   NONE.  The global mntflg variable is altered.
 */
static void
parse_std_options(char *fsopts) 
{
    char *tok = NULL, *type = NULL, *subopt_val;
    char *options = strdup(fsopts);   /* Make a copy for strtok */
    char *subopt = options;
    unsigned long mntflg = 0;

    /* verify we aren't in low memory conditions */
    if(subopt == NULL) {
        fprintf(stderr, catgets(catd, 1, OUTOFMEM,
                        "mount: ERROR - could not allocate memory.\n"));
        exit(1);
    }

    while (*subopt != '\0') {
        switch (getsubopt(&subopt, myopts, &subopt_val)) {
            case OPT_RO:
                if (type && RW_OPT(type)) {
                    fprintf(stderr, catgets(catd, 1, ERR_RW_RO, 
                                "mount: Conflicting ro/rw options.\n"));
                    usage();
                    /* NOTREACHED */
                } else
                    type = MNTOPT_RO;
                gen_mntflg |= MS_RDONLY;
                break;
            case OPT_RW:
                if (type && !RW_OPT(type)) {
                    fprintf(stderr, catgets(catd, 1, ERR_RW_RO, 
                                "mount: Conflicting ro/rw options.\n"));
                    usage();
                    /* NOTREACHED */
                } else
                    type = MNTOPT_RW;
                gen_mntflg &= ~MS_RDONLY;
                break;
            case OPT_SUID:
                gen_mntflg &= ~MS_NOSUID;
                break;
            case OPT_NOSUID:
                gen_mntflg |= MS_NOSUID;
                break;
            case OPT_REMOUNT:
                gen_mntflg |= MS_REMOUNT;
                break;
            case OPT_DIRTY:
                adv_mntflg |= MS_ADVFS_DIRTY;
                break;
            case OPT_ATIMES:
                adv_mntflg &= ~MS_ADVFS_NOATIMES;
                break;
            case OPT_NOATIMES:
                adv_mntflg |= MS_ADVFS_NOATIMES;
                break;
            case OPT_QUOTA:
                gen_mntflg |= MS_QUOTA;
                break;
            case OPT_DIRECTIO:
                adv_mntflg |= MS_ADVFS_DIRECTIO;
                break;
            case OPT_SERVER_ONLY:
                if (!clu_is_member())
                    usage();
                gen_mntflg |= MS_SERVERONLY;
                break;
            case OPT_SERVERNAME:
                if (!clu_is_member())
                    usage();
                if (subopt_val)
                    cfs_servername = strdup(subopt_val);
                else
                    usage();
                break;
            case OPT_BOOTFS:
                if (!clu_is_member())
                    usage();
                gen_mntflg |= MS_BOOTFS;
                break;
            case OPT_READAHEAD:
                adv_mntflg &= ~MS_ADVFS_NOREADAHEAD;
                break;
            case OPT_NOREADAHEAD:
                adv_mntflg |= MS_ADVFS_NOREADAHEAD;
                break;
            case OPT_DEFAULTS:
                /* There is no need to explicitly specify defaults 
                 * for the AdvFS mount.  This just does a break;
                 * and continues processing */
                break;
            case OPT_NOAUTO:
                /* We don't actually do anything here, but we
                 * need to accept the NOAUTO option because running 
                 * 'mount /mntpoint' (which looks in /etc/fstab) may
                 * pass in this option and we don't want to error */
                break;
            default:
                usage();
        }
    }

    if (options) free(options);
}

/* NAME: print_flags(void)
 *
 * DESCRIPTION: 
 *   Prints the '-o' options that were processed.  Defined only when DEBUG 
 *   is defined.
 *
 * INPUTS:
 *   flag : bit flag where mount options are stored
 *
 * RETURN VALUES:  None
 *   
 */
   
#ifdef DEBUG
static void
print_flags(void) 
{
    int f = 0, first = 1;
    unsigned long gen_opt[] = {MS_RDONLY, MS_NOSUID, MS_REMOUNT, MS_SERVERONLY, MS_BOOTFS, NULL};
    char *gen_opt_str[] = {"ro", "nosuid", "remount", "server_only", "bootfs", NULL};
    unsigned long adv_opt[] = {MS_ADVFS_NOATIMES, MS_ADVFS_DIRTY, 
                               MS_ADVFS_DIRECTIO, MS_ADVFS_NOREADAHEAD, NULL};
    char *adv_opt_str[] = {"noatimes", "dirty", "directio", "noreadahead",  
                           NULL};

    printf("mount (DEBUG) gen_flags: 0x%x : ", gen_mntflg);
    for(f; gen_opt_str[f] != NULL; f++) {
        if (gen_mntflg & gen_opt[f]) {
            if (first) {
                printf("%s", gen_opt_str[f]);
                first = 0;
            }
            else
                printf(",%s", gen_opt_str[f]);
        }
    }
    printf("\n");
    printf("mount (DEBUG) adv_flags: 0x%x : ", adv_mntflg);
    for(f=0, first=1; adv_opt_str[f] != NULL; f++) {
        if (adv_mntflg & adv_opt[f]) {
            if (first) {
                printf("%s", adv_opt_str[f]);
                first = 0;
            }
            else
                printf(",%s", adv_opt_str[f]);
        }
    }
    printf("\n");
    if (cfs_servername) 
        printf("mount (DEBUG) server = %s\n", cfs_servername);
}
#endif

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
    int     c, save_optind;
    extern char     *optarg;
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

static void
usage(void)
{
        fprintf(stderr, catgets(catd, 1, USAGE, "usage:\n  mount %s "), 
                "[-F advfs] [-r] [-e] [-o option_list] [-V] special " 
                "directory\n");
        exit(1);
}
