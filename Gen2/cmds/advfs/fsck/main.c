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

/*  Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P. */

#include "fsck.h"

/*
 * Global
 */
nl_catd _m_catd;
cacheT *cache;

/* private prototypes */

static void usage(void);
static int checkFS(domainT *domain, arg_infoT *fsInfo);
static int create_domain(domainT **domain);
static int free_domain(domainT *domain);
static int check_dir(char *path, char *opt);

static void prepare_signal_handlers(void);
static void signal_handler(int sigNum);
static int want_abort(void);

char cmdLine[MAXPATHLEN+128];


static void 
usage( void )
{
    char   *funcName = "usage";

    /*
     *  SV_CONT indents by about 8 spaces.
     */

    writemsg(SV_ERR | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_1, 
		     "usage: %s [-F advfs] [-pmPVyYnN]\n"),
	     Prog);
    writemsg(SV_ERR | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_2, 
		     "       [-o option[,option...]] [ storage_domain | special ... ]\n"));

}  /* end usage */


/*
 * Function Name:  main
 *
 * Description:
 *     This is the main function of the fsck program. It is responsible
 *     for getting fsck setup and to execute the correct functions.
 *
 * Input parameters:
 *     argc - Argument Count.
 *     argv - Array of arguments.
 *
 * Exit values: EXIT_SUCCESS, EXIT_FAILED or EXIT_CORRUPT.
 */
main(int  argc, char *argv[])
{
    char	   *funcName = "main";
    char           *option;
    extern int	   getopt();
    extern int	   optind;
    int            status;
    int            i;
    int		   getoptValue;
    int		   bomb;    /* Used to exit on illegal options */
    struct rlimit  rlimit;
    domainT	   *domain;
    struct stat	   statBuf;
    char	   *pp;  /* Used for character to int conversion */
    char	   prompt[10];
    char	   fsname[PATH_MAX];
    char 	   *command; /* used for option types strtok */
    char	   selectedSetName[BS_SET_NAME_SZ+1];
    arg_infoT      *fsInfo;

    /*
     * store only the file name part of argv[0] 
     */
    Prog = strrchr( argv[0], '/' );
    if (Prog == NULL) {
	Prog = argv[0];
    } else {
	Prog++;
    }

    /*
     * Setup fsck message catalog.
     */
    (void) setlocale(LC_ALL, "");
    _m_catd = catopen(MF_FSCK, NL_CAT_LOCALE);

    /* 
     * check for root 
     */
    if (geteuid()) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_5, 
			 "Permission denied - user must be root to run.\n"));
	failed_exit();
    }

    /*
     * Need to make sure we have access to all the memory we
     * can get access to.  This is important for filesets with
     * large number of files.
     */
    rlimit.rlim_max = (uint32_t) RLIM_INFINITY;
    rlimit.rlim_cur = (uint32_t) RLIM_INFINITY;

    if (setrlimit(RLIMIT_DATA, &rlimit)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_6, 
			 "setrlimit() failed to change memory usage limits.\n"));
	perror(Prog);
	failed_exit();
    }

    /*
     * Initialize Variables
     */ 

    create_domain(&domain);

    bomb = 0;

    /*
     * Set Options defaults.
     */

    Options.echoCmdline = FALSE;
    Options.advfs = FALSE;
    Options.verboseOutput = SV_NORMAL;
    Options.full = FALSE;
    Options.nolog = FALSE;
    Options.sanityCheck = FALSE;
    Options.yes = FALSE;
    Options.no = FALSE;
    Options.activate = FALSE;
    Options.nochange = FALSE;
    Options.preen = FALSE;
    Options.sync = FALSE;
    Options.undo = FALSE;
    Options.msglog = FALSE;
    Options.msglogHndl = NULL;
    Options.msglogFile = NULL;
    Options.dirPath = ffd_malloc(MAXPATHLEN);

    /*
     * Open a local /dev/tty for prompting the user during an error.
     * If the fopen () fails, Localtty will be NULL.
     */

    Localtty = fopen ("/dev/tty", "r");

    /*
     * Prepare to get any signal interrupts.
     */
    prepare_signal_handlers ();

    /* 
     * Seed random number generator - done only once.  Used for skiplists.
     */
    srandom(1);

    /* 
     * Get user-specified command line arguments. 
     */
    while ((getoptValue = getopt( argc, argv, "F:mPpVyYnNso:")) != EOF) {

	switch (getoptValue) {

	case 'F':   /* must be advfs filesys type */

	    if (strcmp(optarg, MNTTYPE_ADVFS)) {

		writemsg(SV_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_971, 
				 "'%s' file system type unknown to AdvFS\n\n"),
			 optarg);
		    bomb++;
	    } else {
		Options.advfs = TRUE;
	    }
	    break;

	case 'P':
	case 'm':
	    Options.sanityCheck = TRUE;
	    break;

	case 'V':  /* echo the command line */
	    Options.echoCmdline = TRUE;
	    break;

	case 'p':   /* preen mode (exits if interaction needed) */
	    Options.preen = TRUE;
	    Options.full = TRUE;
	    break;

	case 'y':  /* answer prompts with 'yes' */
	case 'Y':
	    Options.yes = TRUE;
	    break;

	case 'n':  /* answer prompts with 'no' */
	case 'N':		
	    Options.no = TRUE;
	    break;

	case 's':   /* compatability only, used to suppress sync's */
	    break;

	case 'o':   /* command options follow */

	    option = strdup(optarg);

	    command = strtok(option, ",");
	    while (command != NULL) {

		if (strcmp(command, "activate") == 0) {
		    Options.activate = 1;

		} else if (strcmp(command,"quiet") == 0) {

		    if (Options.verboseOutput != SV_NORMAL) {
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FSCK_1, FSCK_972, 
					 "Specify only one message output level.\n"));
			bomb++;
		    } else {
			Options.verboseOutput = SV_ALL;
		    }

		} else if (strcmp(command,"verbose") == 0) {

		    if (Options.verboseOutput != SV_NORMAL) {
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FSCK_1, FSCK_972, 
					 "Specify only one message output level.\n"));
			bomb++;
		    } else {
			Options.verboseOutput = SV_VERBOSE;
		    }

		} else if (strcmp(command,"debug") == 0) {

		    if (Options.verboseOutput != SV_NORMAL) {
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FSCK_1, FSCK_972, 
					 "Specify only one message output level.\n"));
			bomb++;
		    } else {
			Options.verboseOutput = SV_DEBUG;
		    }

		} else if (strcmp(command, "verify") == 0) {
		    Options.nochange = TRUE;

		} else if (strcmp(command,"sync") == 0) {
		    Options.sync = TRUE;

		} else if (strcmp(command,"full") == 0) {
		    Options.full = TRUE;

		} else if (strcmp(command,"nolog") == 0) {
		    Options.nolog = TRUE;

		} else if (strncmp(command,"msglog=", 7) == 0) {

		    strcpy(Options.dirPath, &command[7]);

		    if (check_dir(Options.dirPath, "msglog") != SUCCESS) {
			bomb++;
		    } else {
			Options.msglog = TRUE;
		    }

		} else if (strncmp(command,"undo=", 5) == 0) {

		    strcpy(Options.dirPath, &command[5]);
		    
		    if (check_dir(Options.dirPath, "undo") != SUCCESS) {
			bomb++;
		    } else {
			Options.undo = TRUE;
		    }

		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_969, 
				     "Illegal option '%s' specified with '-o'.\n"),
			     command);
		    bomb++;
		}
		command = strtok((char *)NULL, ",");
	    }

	    free(option);
	    break;

	default:
	    bomb++;
	} /* end switch */
    } /* end while */
    
    /* 
     * We're done fetching all the options from the command line, we
     * now look for illegal combinations.  Some of the combination
     * groups checked below are redundant, but they're expressed this
     * way for clarity.
     */

    if ((Options.full && (Options.nochange ||
			  Options.sync ||
			  Options.undo))  
	||
	(Options.sync && (Options.full ||
			  Options.nochange ||
			  Options.undo))
	||
	(Options.nochange && (Options.full ||
			      Options.sync ||
			      Options.activate ||
			      Options.nolog ||
			      Options.undo))
	||
	(Options.undo && (Options.activate ||
			  Options.nochange ||
			  Options.sync ||
			  Options.full ||
			  Options.msglog ||
			  Options.preen ||
			  Options.sanityCheck))
	||
	(Options.sanityCheck && (Options.full ||
				 Options.sync ||
				 Options.nochange))
	||
	(Options.yes && Options.no)) {

	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_23, 
			     "invalid option combination.\n"));
	    bomb++;
    }

    /* 
     * All argument handling should now be done. 
     */

    if (bomb > 0) {
	usage();
	exit(EXIT_FAILED);
    }

    /* echo the command line if the -V option was used.  This option
       is typically used by scripts to sanity check the command line.
       If we made it this far, the comand line is OK, so print the
       command line (less the -V arg) and the script executes
       that. Note that the fs_wrapper program that calls fsck strips
       off the "-F advfs" specifier before calling us, so we put it
       back here if it's missing.  Exit with code 0 (no error). */

    if (Options.echoCmdline) {

	strcpy(cmdLine, Prog);
	strcat(cmdLine, " ");

	if (Options.advfs == FALSE) {
	    strcat(cmdLine, "-F advfs "); /* fs_wrapper strips off -F opt */
	}	

	for (i = 1 ; i < argc ; i++) {
	    if (strcmp(argv[i], "-V") != 0) {
		strcat(cmdLine, argv[i]);
		strcat(cmdLine, " ");
	    }
	}

	fprintf(stdout, "%s\n", cmdLine);
	exit(0);
    }

    /* We now begin the work of checking the storage domain(s). */

    if (!argv[optind]) {

	/*  If no file system was specified on the command line, we
	    check all the AdvFS file systems named in fstab. */

        struct mntent *fs;
        FILE *fstab;

        fstab = setmntent(MNT_CHECKLIST, "r");

        if (fstab == NULL) {
	    writemsg (SV_ERR | SV_LOG_ERR, 
		      catgets(_m_catd, S_FSCK_1, FSCK_13,
			      "Can not read '%s'.\n"), MNT_CHECKLIST);
	    exit(EXIT_FAILED);
        }

        fsInfo = NULL;

        while (fs = getmntent(fstab)) {

            if (strcmp(fs->mnt_type, MNTTYPE_ADVFS)) {
		continue;
	    }

	    /* special case: in clusters, the boot device file path begins
	       with "/dev/bootdev".  The advfs command parsing routines
	       do not understand this odd case, so we check for the case
	       here and convert it to a normal FS specification (which it
	       is linked to), then call advfs. */
	    
	    if (!(strcmp(fs->mnt_fsname, "/dev/bootdev"))) {
		if (realpath(fs->mnt_fsname, fsname) == NULL) {
		    writemsg (SV_ERR | SV_LOG_ERR, 
			      catgets(_m_catd, S_FSCK_1, FSCK_3,
				      "Can not resolve real path for '%s'.\n"),
			      fs->mnt_fsname);
		    exit(EXIT_FAILED);
		}
	    } else {
		strcpy(fsname, fs->mnt_fsname);
	    }

	    if ((fsInfo = process_advfs_arg(fsname)) == 0) {
		writemsg (SV_ERR | SV_LOG_ERR, 
			  catgets(_m_catd, S_FSCK_1, FSCK_20,
				  "Can not obtain AdvFS info for '%s'.\n"),
			  fsname);
		exit(EXIT_FAILED);
	    }

	    domain->dmnName = fsInfo->fsname;

	    status = checkFS(domain, fsInfo);

	    if (status == FAILURE) {
		failed_exit();
	    } else if (status == EXIT_FAILED) {
		exit(EXIT_FAILED);
	    } else if (status == SUCCESS) {
		finalize_fs(domain);
	    } else if (status == DONE) { 
		/* skip writing undo files or flushing page cache (ie:
		   do nothing) */
	    }

	    /* tear down and rebuild the domain structure */

	    free_domain(domain);
	    create_domain(&domain);

	} /* end: while more fs entries in fstab file */

	endmntent(fstab);

	if (fsInfo == NULL) {
	    writemsg (SV_ERR | SV_LOG_ERR, 
		      catgets(_m_catd, S_FSCK_1, FSCK_22,
			      "'%s' file contains no AdvFS file systems.\n"), 
		      MNT_CHECKLIST);
	    exit(EXIT_FAILED);
	}

	exit (EXIT_SUCCESS);

    } /* end: if no FS name on cmd line, use fstab file */

    /* check the storage domain(s) found on the command line */

    while (argv[optind]) {

	/* special case: in clusters, the boot device file path begins
	   with "/dev/bootdev".  The advfs command parsing routines
	   do not understand this odd case, so we check for the case
	   here and convert it to a normal FS specification (which it
	   is linked to), then call advfs. */

	if (!(strcmp(argv[optind], "/dev/bootdev"))) {
	    if (realpath(argv[optind], fsname) == NULL) {
		writemsg (SV_ERR | SV_LOG_ERR, 
			  catgets(_m_catd, S_FSCK_1, FSCK_3,
				  "Can not resolve real path for '%s'.\n"),
			  argv[optind]);
		exit(EXIT_FAILED);
	    }
	} else {
	    strcpy(fsname, argv[optind]);
	}

	/* Various command line formats can be used to specify an FS,
	   such as special file, etc. Parse that out and start the
	   checks. */

	if ((fsInfo = process_advfs_arg(fsname)) == 0) {
	    writemsg (SV_ERR | SV_LOG_ERR, 
		      catgets(_m_catd, S_FSCK_1, FSCK_20,
			      "Can not obtain AdvFS info for '%s'.\n"),
		      fsname);
	    exit(EXIT_FAILED);
	}

	domain->dmnName = fsInfo->fsname;

	status = checkFS(domain, fsInfo);

	if (status == SUCCESS) {
	    finalize_fs(domain);
	} else if (status == EXIT_FAILED) {
	    exit(EXIT_FAILED);
	} else if (status == FAILURE) {
	    failed_exit();
	} else if (status == DONE) { 
	    /* skip writing undo files or flushing page cache (ie:
	       do nothing) */
	}

	optind++;
	
	/* tear down and rebuild the domain structure */

	free_domain(domain);
	create_domain(&domain);
    }

    exit (EXIT_SUCCESS);

}  /* end main */

/*
 * Function Name:  checkFS...
 *
 * Description:
 *     
 *     After all the command line parsing is done, the main function
 *     calls here to do the work of checking the file system.
 *
 * Input parameters:
 *     argc - Argument Count.
 *     argv - Array of arguments.
 *
 * Exit values: EXIT_SUCCESS, EXIT_FAILED or EXIT_CORRUPT.
 */

static int
checkFS(domainT *domain,
	arg_infoT *fsInfo)
{
    char *funcName = "checkFS";
    int status;
    int fixed;
    int logfd;
    filesetT *FS = NULL;
    filesetT *priorFS;
    filesetT *nextFS;

    if (!Options.undo && !Options.sanityCheck) {

	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_4, 
			 "Beginning checks for '%s'\n"), fsInfo->fsname);

	/* if this is not an undo operation, build the log file
	   directory path now */

	/* If no log path specified use the CWD */

	if (Options.msglog == 0) {
	    strcpy(Options.dirPath, ".");
	    writemsg(SV_VERBOSE | SV_LOG_INFO,
		     catgets(_m_catd, S_FSCK_1, FSCK_28, 
			     "No '-o msglog=<dir>' specified, using current working directory to\n"));
	    writemsg(SV_VERBOSE | SV_LOG_INFO | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_29, 
			     "save the fsck log and undo files.\n"));
	} 

	/*
	 * Open information log file.
	 */

	Options.msglogFile = (char *)ffd_malloc(strlen(Options.dirPath) +
					     strlen("/fsck.") +
					     strlen(domain->dmnName) +
					     strlen(".log") + 1);
	if (Options.msglogFile == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_10, 
			     "Can't allocate memory for %s.\n"), 
		     catgets(_m_catd, S_FSCK_1, FSCK_30, 
			     "log file name"));
	    return (FAILURE);
	}

	sprintf(Options.msglogFile, "%s/fsck.%s.log", Options.dirPath,
		domain->dmnName);
	logfd = open(Options.msglogFile, O_RDWR | O_CREAT | O_APPEND, 0600);
	if (logfd == -1) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_31, 
			     "Can't open log file %s.\n"), Options.msglogFile);
	    perror(Prog);
	    return (FAILURE);
	}
	Options.msglogHndl = fdopen(logfd, "a+");
	if (Options.msglogHndl == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_32, 
			     "Can't open log file handle.\n"));
	    perror(Prog);
	    return (FAILURE);
	}
	fprintf(Options.msglogHndl, "\n******** %s\n", cmdLine);
    }

    /*
     * get the number of volumes and their device names.  This must be
     * done before anything else because this code verifies that the
     * target FS is not mounted.
     */

    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_33, 
		     "Gathering volume information.\n"));
    status = get_domain_volumes (fsInfo, domain);

    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_34, 
			 "Error obtaining volume information.\n"));
	return (FAILURE);
    }

    /* 
     * Get file descriptors for each volume and check the magic
     * number.  This code should be run before we get any further
     * because it verifies that the target is indeed an advfs file
     * system.
     */

    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_35, 
		     "Opening the volumes.\n"));

    status = open_domain_volumes (domain);

    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_36, 
			 "Volume open error.\n"));

	return (FAILURE);
    }

    /* if we're restoring from undo, do it now */

    if (Options.undo) {

	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_37, 
			 "Restoring file system from the undo files.\n"));

	status = restore_undo_files(domain);

	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_38, 
			     "Error restoring undo files.\n"));
	    return (FAILURE);
	}

	/* 
	 * Don't return SUCCESS - it will write undo files and
	 * activate dmn.
	 */

	writemsg(SV_NORMAL | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_39, 
			 "Restore from undo files completed.\n"));
	return (DONE);
    }

    /* if the user entered '-m' we do some general sanity checks, most
       notably that we're working with a bonafile AdvFS file
       system. If we aren't we wouldn't have gotten past the above
       open calls so if we're still here, we must be using an advfs
       FS.  Return before we do any real FS work since -m is otherwise
       an effective no-op. */

    if (Options.sanityCheck) {
	return (DONE);
    }

    /* 
     * The fsck clears the transaction log during a full check, which
     * could destroy valid data in the log.  Therefore we pre-activate
     * the domain, which plays back the transaction log and processes
     * the DDL.  This protects a naive user who might ask for a "full"
     * check on an FS following a crash but before mounting (mount
     * automatically activates a domain).
     *
     * The "verify" option, which does not alter on-disk metadata,
     * skips the pre-activation.
     *
     * The automatic activation can be manually bypassed using the
     * command line option "nolog".
     */

    if (Options.nolog == FALSE && Options.nochange == FALSE) {

	writemsg(SV_VERBOSE,
		 catgets(_m_catd, S_FSCK_1, FSCK_8, 
			 "pre-activating '%s'.\n"), domain->dmnName);

	status = pre_activate_domain(domain);

	if (status != SUCCESS) {

	    /* if pre-activation failed and the "y" option was
	       sepcified, we go ahead and do a full domain check. If
	       pre-activation failed and the "n" option was specified,
	       we do a full check but do not write the metadata. */
	    if (Options.yes) {
		Options.full = TRUE;
	    } else if (Options.no) {
		Options.nochange = TRUE;
		Options.full = TRUE;
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_979, 
				 "Run '%s -o full,nolog' to attempt repair of this file system.\n"), 
			 Prog);
		return (EXIT_FAILED);
	    }
	}

	if (Options.full == FALSE && Options.sync == FALSE) {
	    /* if neither "full" nor "sync" options used (we already
	       know "verify" was not used), we're done cuz fsck w/o
	       those options results in activation only */
	    return (DONE);
	}
    }

    /* This version of fsck doesn't fix metadata if the '-n' option is
       specified.  If we change fsck to do "trivial can't-miss" sorts
       of fixes later on, even if the user entered '-n', then we won't
       set 'nochange' here.  Until then, we roadblock writes of the
       metadata by setting 'nochange'. */

    if (Options.no) {
	Options.nochange = TRUE;
    }

    /* if we're doing a 'verify', then also set 'full' and 'nolog' to
       control verify behavior.  Do this only after checking for illegal
       comand line combinations above. */ 

    if (Options.nochange) {
	Options.full = TRUE;
	Options.nolog = TRUE;
    }

    /* 
     * Check/correct domain and volume attributes and sync them across
     * volumes.  Must be called before load_subsystem_extents.
     */

    writemsg(SV_NORMAL | SV_LOG_INFO,
	     catgets(_m_catd, S_FSCK_1, FSCK_40, 
		     "Checking the RBMT(s).\n"));

    status = correct_domain_rbmt(domain);

    if (status != SUCCESS) {

	if (status == FIXED) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_41,
			     "Corrected problems in the RBMT.\n"));
	    prompt_user_to_continue(domain, 
				    catgets(_m_catd, S_FSCK_1, FSCK_42, 
					    "Fixed errors in the RBMT.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_43, 
			     "Error correcting the RBMT.\n"));
	    return (FAILURE);
	}
    }

    /*
     * Can exit at this point if only doing SYNC.
     */

    if (Options.sync) {
	return (SUCCESS);
    }
    /*
     * Get the LBN arrays for the metadata structures stored in
     * the RBMT.
     */
    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_44, 
		     "Loading subsystem extents.\n"));

    status = load_subsystem_extents(domain);

    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_45, 
			 "Error collecting subsystem extents.\n"));
	return (FAILURE);
    }

    /*
     * make a best guess estimate for the amount of memory we will
     * need, and see if it can be malloc'ed.
     */
    status = predict_memory_usage(domain);

    if (status != SUCCESS) {
	/*
	 * predict_memory_usage will always print something if it
	 * errors out.
	 */
	return (FAILURE);
    }
	    
    /*
     * The following routine is called to check log extents and set up
     * the log to be empty.
     */

    writemsg(SV_NORMAL | SV_LOG_INFO,
	     catgets(_m_catd, S_FSCK_1, FSCK_46, 
		     "Clearing the log on volume %s.\n"),
	     domain->volumes[domain->logVol].volName);

    status = clear_log(domain);

    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_47, 
			 "Error clearing the log.\n"));
	return (FAILURE);
    }

    /* 
     * Check BMT extent records, create the mcell list nodes, check BMT
     * page sequence, create BMT page status array, and check the free
     * mcells and free mcell list.
     *
     * This routine then calls correct_free_mcell_list and then frees
     * the BMT page status array. correct_free_mcell_list will loop
     * through the list and remove any full pages, mark pages with free
     * cells in the BMT page status array, then step through the BMT
     * page status array and add pages to the list which are not full
     * and not already in the list.  
     */

    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_48, 
		     "Checking the BMT mcell data.\n"));

    status = correct_bmt_mcell_data(domain);

    if (status != SUCCESS) {

	if (status == FIXED) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_49, 
			     "Corrected BMT mcell data.\n"));
	    prompt_user_to_continue(domain, 
				    catgets(_m_catd, S_FSCK_1, FSCK_50, 
					    "Fixed errors in BMT mcell data.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_51, 
			     "Error correcting BMT mcell data.\n"));
	    return (FAILURE);
	}
    }

    /*
     * Need to clear out the deferred delete list.
     */

    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_52, 
		     "Checking the deferred delete list.\n"));

    status = clear_deferred_delete_list(domain);

    if (status != SUCCESS) {
	if (status == FIXED) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_53, 
			     "Cleared the deferred delete list.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_54, 
			     "Error clearing the deferred delete list.\n"));
	    return (FAILURE);
	}
    }

    /*
     * Correct the root tag file. (-2 means root tag file).
     * This is needed to continue.
     */

    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_55, 
		     "Checking the root tag file.\n"));

    status = correct_tag_file(domain, -2);

    if (status != SUCCESS) {
	if (status == FIXED) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_56, 
			     "Corrected the root tag file.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_57, 
			     "Error correcting the root tag file.\n"));
	    return (FAILURE);
	}
    }

    if (domain->filesets == NULL) {   /* set above by correct_tag_file() */
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_58, 
			 "Unable to continue, no file system information was found.\n"));
	return (FAILURE);
    }

    fixed = FALSE;
    if (Options.verboseOutput < SV_VERBOSE) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_64, 
			 "Checking the tag file(s).\n"));
    }

    /*
     * Check/correct each FS's tag directory file.
     */
    
    for (FS = domain->filesets; FS != NULL; FS = FS->next) {

	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_65, 
			 "Checking the '%s' file system tag file.\n"),
		 FS->fsName);

	status = correct_tag_file(domain, 
				  FS->filesetId.dirTag.tag_num);
	if (status != SUCCESS) {
	    if (status == FIXED) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FSCK_1, FSCK_66, 
				 "Corrected '%s' file system tag file.\n"),
			 FS->fsName);
		fixed = TRUE;
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_67,
				 "Error correcting '%s' file system tag file.\n"),
			 FS->fsName);
		return (FAILURE);
	    }
	}
    } /* end file system tag dir check/correct loop */

    if (fixed == TRUE) {
	prompt_user_to_continue(domain, 
				catgets(_m_catd, S_FSCK_1, FSCK_68, 
					"Fixed errors in tag files.\n"));
    }

    /*  
     * Call correct nodes to find loops in mcell chains, collects SBM
     * info and adds pages to the number found from extent records, and
     * saves the original stats in the tag list.  
     */
    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_69, 
		     "Checking the mcell nodes.\n"));
    status = correct_nodes(domain);
    if (status != SUCCESS) {
	if (status == FIXED) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED,
		     catgets(_m_catd, S_FSCK_1, FSCK_70, 
			     "Corrected the mcell nodes.\n"));
	    prompt_user_to_continue(domain, 
				    catgets(_m_catd, S_FSCK_1, FSCK_71, 
					    "Fixed errors in mcell nodes.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_72, 
			     "Error correcting mcell nodes.\n"));
	    return (FAILURE);
	}
    }

    /* 
     * check and correct BMT chains for each FS
     */

    fixed = FALSE;

    if (Options.verboseOutput < SV_VERBOSE) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_73, 
			 "Checking the BMT chains.\n"));
    }

    /*
     * check/correct the BMT chain for each FS.
     */

    for (FS = domain->filesets; FS != NULL; FS = FS->next) {

	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_75, 
			 "Checking '%s' file system BMT chains.\n"), 
		 FS->fsName);
	/*  
	 * To save reads, check the tags while page is loaded into
	 * memory.  This is a VERY slow operation as we will be
	 * following chains, which could mean loading some pages as
	 * many times as it has mcells.  We have already looked for
	 * chain loops, so we don't need to worry about that at this
	 * point. 
	 */
	status = correct_bmt_chains(FS);
	if (status != SUCCESS) {
	    if (status == FIXED) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FSCK_1, FSCK_76, 
				 "Corrected the BMT chains on '%s' file system.\n"),
			 FS->fsName);
		fixed = TRUE;
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_77, 
				 "Error correcting the BMT chains on '%s' file system.\n"),
			 FS->fsName);
		return (FAILURE);
	    }
	}
    } /* end check/correct BMT chain loop */

    if (fixed == TRUE) {
	prompt_user_to_continue(domain, 
				catgets(_m_catd, S_FSCK_1, FSCK_78, 
					"Fixed errors in BMT chains.\n"));
    }
    
    /*
     * If the ddl had snap files on it we need to handle them.
     */

    if (domain->ddlSnapList != NULL) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_84, 
			 "Checking for snapshot files on the deferred delete list.\n"));

	status = correct_snap_files_on_ddl(domain);

	if (status != SUCCESS) {
	    if (status == FIXED) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED,
			 catgets(_m_catd, S_FSCK_1, FSCK_85, 
				 "Corrected snapshot files on the deferred delete list.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_86, 
				 "Error correcting snapshot files on the deferred delete list.\n"));
		return (FAILURE);
	    }
	}
    } /* end if we have ddl snapshot files */

    /*
     * We have now cleaned up all the mcell related structures.
     * We need to check if we still have any mcell which are orphans.
     */
    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_87, 
		     "Checking for BMT mcell orphans.\n"));
    status = correct_orphans(domain);
    if (status != SUCCESS) {
	if (status == FIXED) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED,
		     catgets(_m_catd, S_FSCK_1, FSCK_88, 
			     "Cleared BMT mcell orphans.\n"));	
	    prompt_user_to_continue(domain, 
				    catgets(_m_catd, S_FSCK_1, FSCK_89, 
					    "Fixed BMT mcell orphans.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_90, 
			     "Error correcting BMT mcell orphans.\n"));
	    return (FAILURE);
	}
    }

    /* 
     * Call correct_sbm_overlaps to deal with any overlaps the SBM
     * has detected.
     */
	
    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_91, 
		     "Checking for file overlaps.\n"));
    status = correct_sbm_overlaps(domain);
    if (status != SUCCESS) {
	if (status == FIXED) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_92, 
			     "Corrected file overlaps.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_93, 
			     "Error correcting file overlaps.\n"));
	    return (FAILURE);
	}
    }

    /*
     * Check and correct fileset directories for each FS
     */

    if (Options.verboseOutput < SV_VERBOSE) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_94, 
			 "Checking the directories.\n"));
    }

    for (FS = domain->filesets; FS != NULL; FS = FS->next) {

	writemsg(SV_VERBOSE | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_95, 
			 "Checking '%s' file system directories.\n"), 
		 FS->fsName);

	status = correct_all_dirs(FS);
	if (status != SUCCESS) {
	    if (status == FIXED) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FSCK_1, FSCK_96, 
				 "Corrected '%s' file system directories.\n"),
			 FS->fsName);
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_97, 
				 "Error correcting '%s' file system directories.\n"),
			 FS->fsName);
		return (FAILURE);
	    }
	}
    } /* end fileset loop */

    /*
     * After this point, delete_tag should never be called.
     *
     * Check/correct each fileset's quota files.
     */

    if (Options.verboseOutput < SV_VERBOSE) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_103, 
			 "Checking the quota files.\n"));
    }

    for (FS = domain->filesets; FS != NULL; FS = FS->next) {

	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_104, 
			 "Checking '%s' file system quota file.\n"), 
		 FS->fsName);

	status = correct_quota_files(FS);
	if (status != SUCCESS) {
	    if (status == FIXED) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FSCK_1, FSCK_105, 
				 "Corrected '%s' file system quota files.\n"),
			 FS->fsName);

	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_106, 
				 "Error correcting '%s' file system quota files.\n"),
			 FS->fsName);
		return (FAILURE);
	    }
	}
    } /* end fileset loop */

    /*
     * Very last thing do is correct the SBM.
     */

    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_799,
		     "Checking the SBM(s).\n"));
    status = correct_sbm(domain);
    if (status != SUCCESS) {
	if (FIXED == status) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED,
		     catgets(_m_catd, S_FSCK_1, FSCK_800,
			     "Corrected the SBM.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_801,
			     "Error correcting the SBM.\n"));
	    return (FAILURE);
	}
    }

    /* made it! */

    return (SUCCESS);

}  /* end checkFS */


/*
 * Function Name: create_domain
 *
 * Description: initialize a domain structure
 *
 * Input: *domain - pntr to domain struct to be initialized
 *
 * Output: Domain and page cache are initialized.
 */

static int
create_domain(domainT **domain)
{
    int        status;
    mcellNodeT *maxMcell;
    domainT    *dmn;

    dmn = (domainT *)ffd_malloc(sizeof(domainT));

    if (dmn == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FSCK_1, FSCK_11, 
			 "mcell list tail"));
	failed_exit();
    }

    dmn->dmnName        = NULL;
    dmn->dmnId.id_sec   = 0;
    dmn->dmnId.id_usec  = 0;
    dmn->ODSVersion.odv_major = 0;
    dmn->ODSVersion.odv_minor = 0;
    dmn->filesets       = NULL;
    dmn->numFilesets    = 0;
    dmn->volumes        = NULL;
    dmn->numVols        = 0;
    dmn->highVolNum     = 0;
    dmn->mcellList      = NULL;
    dmn->logVol         = 0;
    dmn->rootTagVol     = 0;
    dmn->rootTagLBN     = NULL;
    dmn->rootTagLBNSize = 0;
    dmn->errorCount     = 0;
    dmn->filesetQuota   = NULL;
    dmn->status         = 0;
    DMN_CLEAR_SBM_LOADED(dmn->status);

    /*
     * Malloc and initialize the page cache.
     */

    status = create_cache(dmn);
    if (status != SUCCESS) {
	writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FSCK_1, FSCK_9,
			  "Error creating the data cache.\n"));
	failed_exit();
    }

    /* We need maxMcell to set up the mcell list.  Maxmcell becomes
       the tail of the skip list and so it is not freed here. */

    maxMcell = (mcellNodeT *)ffd_malloc(sizeof(mcellNodeT));
    if (maxMcell == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FSCK_1, FSCK_11, 
			 "mcell list tail"));
	failed_exit();
    }

    /*
     * Create the mcell list.  The key (mcid) needs to be set, the
     * rest of the maxMcell structure can be garbage - this is the
     * tail node of the skiplist.
     */

    maxMcell->mcid.volume = MAX_VOLUMES;
    maxMcell->mcid.page = MAXINT;
    maxMcell->mcid.cell = BSPG_CELLS + 1;

    status = create_list(&compare_mcell_nodes, &(dmn->mcellList), maxMcell);
    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_12, 
			 "Error creating the mcell list structures.\n"));
	failed_exit();
    }

    *domain = dmn;
    return (SUCCESS);

} /* end create_domain */


/*
 * Function Name: teardown_domain
 *
 * Description: free a domain structure
 *
 * Input: domain - pntr to domain struct to be freed
 *
 * Output: Domain, page cache, and mcell list are freed
 */

static int
free_domain(domainT *domain)
{
    void     *data;
    nodeT    *pNode;
    filesetT *pFileset;
    volNumT  volNum;

    free_cache();

    while (find_first_node(domain->mcellList, &data, &pNode) == SUCCESS) {
	delete_node(domain->mcellList, &data);
	free(data);
    }

    for (pFileset = domain->filesets; pFileset; pFileset = pFileset->next) {

	while (find_first_node(pFileset->tagList, &data, &pNode) == SUCCESS) {
	    delete_node(pFileset->tagList, &data);
	    free(data);
	}
    }

    /* free per volume structs, especially the SBM mirrors, which
       could be very large */

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

	if (domain->volumes[volNum].volFD == -1) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}

	free(domain->volumes[volNum].volSbmInfo->sbm);
	free(domain->volumes[volNum].volSbmInfo->overlapBitMap);
    }

    free(domain->mcellList);
    free(domain->filesets);
    free(domain);

    return (SUCCESS);

} /* end teardown_domain */


/*
 * Function Name: check_dir
 *
 * Description: Check the validity of a log/undo directory path
 *
 * Input: path - the directory path to be checked
 *        opt - the option keyword this path is associated with
 *
 * Output: If the directory path ends with a slash, we remove it 
 *         here as a convenience.
 */

static int
check_dir(char *path,
	  char *opt)
{
    struct stat statBuf;
    size_t length;

    if (strcmp(opt, "undo")) {
	length = strlen(path) + strlen("undoidx") + BS_DOMAIN_NAME_SZ +
	    strlen("0") + 1;
    } else {
	length = strlen(path) + strlen("/fsck.") + BS_DOMAIN_NAME_SZ +
	         strlen(".log") + 1;
    }

    if (path == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_14, 
			 "No directory path specified with '%s' option.\n"), 
		 opt);
	return (FAILURE);
    }

    if (length >= MAXPATHLEN) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_15, 
			 "directory path '%s' is too long.\n"),
		 path);
	return (FAILURE);
    }

    /*
     * Check to see if the directory exists.
     */

    if (stat(path, &statBuf) == -1) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_17,
			 "stat() failed on directory path '%s'.\n"), path);
	perror(Prog);
	return (FAILURE);
    } else {
	/* 
	 * Verify that the path given is a directory.
	 */
	if (S_ISDIR(statBuf.st_mode) == FALSE) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_18, 
			     "directory path '%s' is not a directory.\n"), 
		     path);
	    return (FAILURE);
	}
    }

    return (SUCCESS);

} /* check_dir */

/*
 * Function Name: prepare_signal_handlers (3.2)
 *
 * Description:	  This routine sets up the signal handling for the SIGINT, 
 *		  SIGQUIT, and SIGTERM signals.
 *
 * Input parameters: N/A
 *
 * Output parameters: N/A
 *
 * Return values: N/A
 */
static void 
prepare_signal_handlers (void)
{
    char	     *funcName = "prepare_signal_handlers";
    sigset_t	     newset;
    sigset_t	     *newsetp;
    struct sigaction intAction;	  /* action structure for signals */

    /*
     * Initialize Variables
     */
    newsetp = &newset;
    
    /* 
     * Set up intAction to identify signal_handler as the signal handler 
     * for interrupts.
     */
    intAction.sa_handler = signal_handler;
    memset(&intAction.sa_mask, 0, sizeof(sigset_t));
    intAction.sa_flags	 = SA_RESETHAND;
    
    /*
     * Call sigaction using intAction for SIGINT, SIGQUIT, and SIGTERM.
     */

    sigaction (SIGINT, &intAction, NULL);
    sigaction (SIGQUIT, &intAction, NULL);
    sigaction (SIGTERM, &intAction, NULL);
    sigaction (SIGBUS, &intAction, NULL);
    sigaction (SIGSEGV, &intAction, NULL);

    /*
     * Call sigemptyset to set up newset as the signal mask.
     */
    sigemptyset (newsetp);
    
    /*
     * Call sigaddset to add signals to the signal mask.
     */
    sigaddset (newsetp, SIGINT);
    sigaddset (newsetp, SIGQUIT);
    sigaddset (newsetp, SIGTERM);
    sigaddset (newsetp, SIGBUS);
    sigaddset (newsetp, SIGSEGV);
    
    /*
     *Call sigprocmask with newset to unblock signals specified in signal mask.
     */
    sigprocmask (SIG_UNBLOCK, newsetp, NULL);
    
} /* end prepare_signal_handlers */


/*
 * Function Name: signal_handler (3.3)
 *
 * Description: This routine is called by the system for local signal 
 *              handling.  It asks the user if they want to abort fsck
 *              when a SIGINT or SIGQUIT interrupt is received. If so,
 *              we exit, otherwise, we reset the signal handler and
 *              return. If we receive a SIGTERM interrupt, then we
 *              exit.
 *
 *              We also catch the SIGSEGV fault and SIGBUS signals so
 *              that we can print a message indicating that the domain
 *              remains unmodified.  We need to stop catching these
 *              once we start to write the disk.
 *
 * Input parameters:
 *    sigNum: Number of the signal received.
 *
 * Output parameters: N/A
 *
 * Return values: If the user wants to abort, exit with EXIT_FAILED.
 */
static void 
signal_handler(int sigNum)
{
    char   *funcName = "signal_handler";
    char   *buf = NULL;
    struct sigaction newInt;  /* new interrupt action structure	    */
    struct sigaction oldInt;  /* old interrupt action structure	    */
    
    switch (sigNum) {

    case SIGINT:
    case SIGQUIT:

	/*
	 * Set up newInt to ignore signals, then tell system to ignore
	 * these signals so that we won't be bothered by a reoccurance
	 * of them while we are handling this occurance.
	 */
	newInt.sa_handler = SIG_IGN;
	memset(&newInt.sa_mask, 0, sizeof(sigset_t));
	newInt.sa_flags   = 0;

	sigaction (SIGINT, &newInt, &oldInt);
	sigaction (SIGQUIT, &newInt, &oldInt);

	/* see if the user wants to abort fsck */

	if (want_abort() == FALSE) {
	    sigaction (SIGQUIT, &oldInt, NULL);
	    sigaction (SIGINT, &oldInt, NULL);
	    return;
	}
	break;

    case SIGSEGV:
	writemsg(SV_ERR | SV_CONT, "\n");
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_102,
			 "Segmentation Fault, exiting. "));

	if (!(cache->domain->status & DMN_META_WRITTEN)) {
	    writemsg(SV_ERR | SV_CONT, "\n");
	    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		     catgets(_m_catd, S_FSCK_1, FSCK_831,
			     "Storage domain '%s' not modified. "),
		     cache->domain->dmnName);
	}
	abort();
	break;

    case SIGBUS:
	writemsg(SV_ERR | SV_CONT, "\n");
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_100,
			 "Bus Error, exiting. " ));

	if (!(cache->domain->status & DMN_META_WRITTEN)) {
	    writemsg(SV_ERR | SV_CONT, "\n");
	    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		     catgets(_m_catd, S_FSCK_1, FSCK_831,
			     "Storage domain '%s' not modified. "),
		     cache->domain->dmnName);
	}
	abort();
	break;

    case SIGTERM:
	writemsg(SV_ERR | SV_CONT, "\n");
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_99,
			 "Interrupted by software termination signal."));
	break;

    default:
	writemsg(SV_ERR | SV_CONT, "\n");
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_101,
			 "Caught signal %d, storage domain not modified."),
		 sigNum);
	break;
    }
  
    failed_exit();

} /* end signal_handler */


/*
 * Function Name:  want_abort
 *
 * Description:	 This routine prompts the user to see if they really want
 *		 to abort fsck.  If stdin is being used for input, or 
 *		 the user cannot respond, then this routine will return TRUE.
 *		 This routine may be called from threads other than the 
 *		 signal_thread to prompt user for abort.
 *
 * Input parameters:
 *
 * Output parameters:
 *
 * Return values: TRUE (abort wanted), FALSE (don't abort).
 */
static int 
want_abort(void)
{
    char *funcName = "want_abort";
    char abortAnswer[10];  /* response to prompt */

    if (Options.preen)
	return (TRUE);

    /*
     * Prompt the user if they want to abort.
     */
    writemsg(SV_ERR | SV_CONT, "\n");
    writemsg(SV_ERR, catgets(_m_catd, S_FSCK_1, FSCK_107,
			     "Do you want to abort? (y/n)  "));
	
    if (prompt_user_yes_no(abortAnswer, NULL) != SUCCESS) {
	return(TRUE);
    }
	
    if (abortAnswer[0] == 'n') {
	return(FALSE);
    }
	
    return(TRUE);
    
} /* end want_abort */

/* end main.c */


