/* 
 * =======================================================================
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
 * 
 * Facility:
 * 
 *      Advanced File System
 * 
 * Abstract:
 * 
 *      On-disk structure fixer.
 * 
 * Date:
 *      Mon Jan 10 13:20:44 PST 2000
 */
/* 
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fixfdmn.c,v $ $Revision: 1.1.30.5 $ (DEC) $Date: 2006/06/19 11:53:46 $"

#include "fixfdmn.h"

/*
 * Global
 */
nl_catd _m_catd;
cacheT *cache;


void usage( void )
{
    char   *funcName = "usage";
    
    /*
     * FUTURE: left align this - the SV_CONT will indent by about 8 spaces.
     */
    writemsg(SV_ERR | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_1, 
		     "usage: %s [-m type[,type...]] [-d directory] [-v number]\n"),
	     Prog);
    writemsg(SV_ERR | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_2, 
		     "       [-a[-c]|-n] [-s {y|n}] domain [fileset]\n"));
    writemsg(SV_ERR | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_3, 
		     "       %s -u directory domain\n"), Prog);
}  /* end usage */


/*
 * Function Name:  main (3.1)
 *
 * Description:
 *     This is the main function of the fixfdmn program. It is responsible
 *     for getting fixfdmn setup and to execute the correct functions.
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
    extern int	   getopt();
    extern int	   optind;
    int            status;
    int		   getoptValue;
    int		   bomb;    /* Used to exit on illegal options */
    int            m, d, v, a, c, n, s, u; /* option flags */
    struct rlimit  rlimit;
    mcellNodeT	   *maxMcell;
    domainT	   domain;
    filesetT	   *fileset,*orig;
    filesetT	   *priorFileset;
    filesetT	   *nextFileset;
    struct stat	   statBuf;
    char	   *path;
    char	   undoDirPath[MAXPATHLEN];
    char	   *pp;  /* Used for character to int conversion */
    char	   prompt[10];
    char 	   *command; /* used for -m option types strtok */
    int	           logfd;
    int            fixed;
    char	   selectedSetName[BS_SET_NAME_SZ+1];
    ulong_t	   soflags;  /* Used for safe_open */
    int            loop;

    /*
     * store only the file name part of argv[0] 
     */
    Prog = strrchr( argv[0], '/' );
    if (NULL == Prog) {
	Prog = argv[0];
    } else {
	Prog++;
    }

    /*
     * Setup fixfdmn message catalog.
     */
    (void) setlocale(LC_ALL, "");
    _m_catd = catopen(MF_FIXFDMN, NL_CAT_LOCALE);

#ifdef DEBUG
    printf(catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_4, "Debug is on.\n"));
#endif

    /* 
     * check for root 
     */
    if (geteuid()) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_5, 
			 "Permission denied - user must be root to run.\n"));
	failed_exit();
    }

    /*
     * Need to make sure we have access to all the memory we
     * can get access to.  This is important for filesets with
     * large number of files.
     */
    rlimit.rlim_max = RLIM_INFINITY;
    rlimit.rlim_cur = RLIM_INFINITY;

    if (setrlimit(RLIMIT_DATA, &rlimit)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_6, 
			 "setrlimit() failed to change memory usage limits.\n"));
	perror(Prog);
	failed_exit();
    }

    /*
     * Initialize Variables
     */ 
    domain.dmnName        = NULL;
    domain.dmnId.tv_sec   = 0;
    domain.dmnId.tv_usec  = 0;
    domain.dmnVersion     = 0;
    domain.filesets       = NULL;
    domain.numFilesets    = 0;
    domain.volumes        = NULL;
    domain.numVols        = 0;
    domain.highVolNum     = 0;
    domain.mcellList      = NULL;
    domain.logVol         = 0;
    domain.rootTagVol     = 0;
    domain.rootTagLBN     = NULL;
    domain.rootTagLBNSize = 0;
    domain.errorCount     = 0;
    domain.filesetQuota   = NULL;
    domain.ddlClone       = NULL;
    domain.status         = 0;
    DMN_CLEAR_SBM_LOADED(domain.status);
    DMN_CLEAR_SM_LOADED(domain.status);

    /*
     *  Indicate no overlaps found.
     */
    domain.countOverlaps = FALSE;

    bomb = 0;
    m = 0;
    d = 0;
    v = 0;
    a = 0;
    c = 0;
    n = 0;
    s = 0;
    u = 0;
    fileset = NULL;

    /*
     * Set Options defaults.
     */
    Options.clearClones = 0;
    Options.type = NULL;
    Options.typeStatus = 0;
#ifdef DEBUG
    Options.verboseOutput = SV_DEBUG;
#else
    Options.verboseOutput = SV_NORMAL;
#endif
    Options.activate = 0;
    Options.nochange = 0;
    Options.answer = 0;
    Options.undo = 0;
    Options.undoDir = NULL;
    Options.logHndl = NULL;
    Options.logFile = NULL;
    Options.filesetName = NULL;

    /*
     * Open a local /dev/tty for prompting the user during an error.
     * If the fopen () fails, Localtty will be NULL.
     */
    Localtty = fopen ("/dev/tty", "r");

#ifdef DEBUG
    /*
     * Field test warning message.
     */
    writemsg(SV_DEBUG | SV_LOG_WARN | SV_CONT, "\n");
    writemsg(SV_DEBUG | SV_LOG_WARN | SV_CONT,
	     "WARNING:  This is an early beta version of fixfdmn.  It is\n");
    writemsg(SV_DEBUG | SV_LOG_WARN | SV_CONT,
	     "recommended that you run fixfdmn using the '-n' flag, this\n");
    writemsg(SV_DEBUG | SV_LOG_WARN | SV_CONT,
	     "reports errors that are found on the domain, but will not\n");
    writemsg(SV_DEBUG | SV_LOG_WARN | SV_CONT,
	     "modify the domain.  Prior to running fixfdmn without this\n");
    writemsg(SV_DEBUG | SV_LOG_WARN | SV_CONT,
	     "flag it is recommended that you 'dd' a copy of your domain\n");
    writemsg(SV_DEBUG | SV_LOG_WARN | SV_CONT,
	     "in case of problems.\n\n");

    writemsg(SV_DEBUG | SV_CONT, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_7, 
		     "Do you want to continue? ([y]/n)  "));

    status = prompt_user_yes_no(prompt, "y");
    if (SUCCESS != status) {
	prompt[0] = 'n';
    }
	
    writemsg(SV_DEBUG | SV_CONT, "\n");

    if ('n' == prompt[0]) {
	exit(EXIT_SUCCESS);
    }
#endif

    /*
     * Prepare to get any signal interrupts.
     */
    prepare_signal_handlers ();

    /* 
     * Seed random number generator - done only once.  Used for skiplists.
     */
    srandom(1);

    /*
     * Malloc and initialize the cache and Mcell list.
     */
    status = create_cache(&domain);
    if (SUCCESS != status) {
	writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_9,
			  "Error creating the data cache.\n"));
	failed_exit();
    }

    maxMcell = (mcellNodeT *)ffd_malloc(sizeof(mcellNodeT));
    if (NULL == maxMcell) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_11, 
			 "mcell list tail"));
	failed_exit();
    }
    /*
     * The key (mcellId) needs to be set, the rest of the maxMcell
     * structure can be garbage - this is the tail node of the skiplist.
     */
    maxMcell->mcellId.vol = MAX_VOLUMES;
    maxMcell->mcellId.page = MAXINT;
    maxMcell->mcellId.cell = BSPG_CELLS + 1;

    status = create_list(&compare_mcells, &(domain.mcellList), maxMcell);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_12, 
			 "Error creating the mcell list structures.\n"));
	failed_exit();
    }

    /* 
     * Get user-specified command line arguments. 
     */
    while ((getoptValue = getopt( argc, argv, "m:d:v:acns:u:")) != EOF) {
	switch (getoptValue) {
	    case 'm':
		m++;
		Options.type = optarg;
		/*
		 * Parse the types setting the typeStatus bits.
		 */
		command = strtok (Options.type, ",");
		while (command != NULL) {
		    if (0 == strcmp(command,"log")) {
			OT_SET_LOG(Options.typeStatus);
		    } else if (0 == strcmp(command,"sbm")) {
			OT_SET_SBM(Options.typeStatus);
		    } else if (0 == strcmp(command,"bmt")) {
			OT_SET_BMT(Options.typeStatus);
		    } else if (0 == strcmp(command,"frag")) {
			OT_SET_FRAG(Options.typeStatus);
		    } else if (0 == strcmp(command,"quota")) {
			OT_SET_QUOTA(Options.typeStatus);
		    } else if (0 == strcmp(command,"files")) {
			OT_SET_FILES(Options.typeStatus);
		    } else if (0 == strcmp(command,"sync")) {
			OT_SET_SYNC(Options.typeStatus);
		    } else {
			/*
			 * unknown option type
			 */
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_13, 
					 "Invalid argument '%s' specified with the -m option.\n"),
				 command);
			bomb++;
		    }
		    command = strtok ((char *)NULL, ",");
		}

		break;

	    case 'd':		
		/* 
		 * Place log and undo files in this location 
		 */
		d++;
		path = optarg;

		if (NULL == path) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_14, 
				     "No undo directory path specified with -d option.\n"));
		    bomb++;
		    break;
		}

		if ((strlen(path) + strlen("/fixfdmn.") + ML_DOMAIN_NAME_SZ +
		     strlen(".log")) >= MAXPATHLEN) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_15, 
				     "Undo directory path '%s' is too long.\n"),
			     path);
		    bomb++;
		    break;
		}
		strcpy(undoDirPath, path);
		
		/*
		 * Check to see if the directory exists.
		 */
		if (stat(undoDirPath, &statBuf) == -1) {
		    if (ENOENT == errno) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_16,
					 "Undo directory path '%s' doesn't exist.\n"),
				 undoDirPath);
			bomb++;
		    }
		    else {
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_17,
					 "stat() failed on undo directory path '%s'.\n"),
				 undoDirPath);
			perror(Prog);
			bomb++;
		    }
		}
		else {
		    /* 
		     * Verify that the path given is a directory.
		     */
		    if (FALSE == S_ISDIR(statBuf.st_mode)) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_18, 
					 "Undo directory path '%s' is not a directory.\n"),
				 undoDirPath);
			bomb++;
		    }
		}
		break;

	    case 'v':	
		/* 
		 * Verbose output 
		 */
		v++;
		Options.verboseOutput = strtoul( optarg, &pp, 0 );
		if ((Options.verboseOutput < 0) || (Options.verboseOutput > 3))
		{
		    /*
		     * Verbose option level '3' is hidden, but valid.
		     */
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_19, 
				     "Verbose option specified is outside of valid range of 0 to 2.\n"));
		    bomb++;
		}
		break;
		
	    case 'a':		
		/* 
		 * Activate the domain 
		 */
		a++;
		Options.activate = 1;
		break;

	    case 'c':		
		/* 
		 * Clear the clones in the domain 
		 */
		c++;
		Options.clearClones = 1;
		break;

	    case 'n':	
		/*
		 * Do not do any repairs 
		 */
		n++;
		Options.nochange = 1;
		break;

	    case 's':		
		/* 
		 * Default answer to questions for scripts 
		 */
		s++;
		strncpy(prompt, optarg, 2);
		if ('y' == prompt[0] || 'Y' == prompt[0]) {
		    Options.answer = 1;
		}
		else if ('n' == prompt[0] || 'N' == prompt[0]) {
		    Options.answer = -1;
		}
		else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_20, 
				     "The -s option requires a 'y' or 'n'.\n"));
		    bomb++;
		}
		break;

	    case 'u':
		u++;
		Options.undo = 1;

		path = optarg;

		if (strlen(path) >= MAXPATHLEN) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_15, 
				     "Undo directory path '%s' is too long.\n"),
			     path);
		    bomb++;
		    break;
		}

		strcpy(undoDirPath, path);
		
		/*
		 * Check to see if the directory exists.
		 */
		if (stat(undoDirPath, &statBuf) == -1) {
		    if (ENOENT == errno) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_16,
					 "Undo directory path '%s' doesn't exist.\n"),
				 undoDirPath);
			bomb++;
		    }
		    else {
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_17,
					 "stat() failed on undo directory path '%s'.\n"),
				 undoDirPath);
			perror(Prog);
			bomb++;
		    }
		}
		else {
		    /* 
		     * Verify that the path given is a directory.
		     */
		    if (FALSE == S_ISDIR(statBuf.st_mode)) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_18,
					 "Undo directory path '%s' is not a directory.\n"),
				 undoDirPath);
			bomb++;
		    }
		}
		break;

	    default:
		bomb++;
	} /* end switch */
    } /* end while */
    
    /* 
     * These options are only allowed once in the command line. 
     */
    if (m > 1 || d > 1 || v > 1 || a > 1 || 
	c > 1 || n > 1 || s > 1 || u > 1) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_21, 
			 "A duplicate option was used.\n"));
	bomb++;
    }

    /* 
     * These options cannot used together. 
     */
    if (a > 0 && n > 0) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_22, 
			 "The -n option may not be used with the -a option.\n"));
	bomb++;
    }
    if (u > 0 &&
	(m > 0 || d > 0 || v > 0 || a > 0 || n > 0 || s > 0) ) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_23, 
			 "The -u option may not be used with any other options.\n"));
	bomb++;
    }

    /*
     * This option can only be used if another option is also set.
     */
    if ((c > 0) && (0 == a)) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_24, 
			 "The -c option can only be used if the -a option is also used.\n"));
	bomb++;

    }

    /*
     * Get the domain name from the command line.
     */
    domain.dmnName = argv[optind];
    if (NULL == domain.dmnName) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_25, 
			 "Missing domain name argument.\n"));
	bomb++;
    } else {
	/*
	 * Validate domain name length
	 */
	if (strlen(domain.dmnName) + strlen("/etc/fdmns/") > MAXPATHLEN) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_26, 
			     "Domain name '%s' is too long.\n"), 
		     domain.dmnName);
	    bomb++;
	}
    }

    /*
     * Get fileset name if there is one.
     */
    if (argc > optind + 1) 
    {
	Options.filesetName = argv[optind + 1];
	
	/*
	 * if fileset is selected the following -m arguments are
	 * invalid: sync log sbm bmt
	 */
	if ((OT_IS_SYNC(Options.typeStatus)) ||
	    (OT_IS_LOG(Options.typeStatus)) ||
	    (OT_IS_SBM(Options.typeStatus)) ||
	    (OT_IS_BMT(Options.typeStatus))) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_27, 
			     "Invalid -m argument(s) when specifying fileset.\n"));
	    bomb++;
	}
    }

    /* 
     * All argument handling should now be done. 
     */
    if (bomb > 0) {
	usage();
	failed_exit();
    }

    /* 
     * If d option is not used, and it's not restoring the undo files,
     * then we will use CWD for undo files.
     */
    if (0 == d && 0 == u) {
	strcpy(undoDirPath, ".");
	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_28, 
			 "No '-d' directory specified, using current working directory to\n"));
	writemsg(SV_VERBOSE | SV_LOG_INFO | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_29, 
			 "save the fixfdmn log and undo files.\n"));
    } 
    Options.undoDir = undoDirPath;
    /*
     * If undoDir ends in a slash, remove the trailing slash.
     */
    if (Options.undoDir[strlen(Options.undoDir) - 1] == '/') {
	Options.undoDir[strlen(Options.undoDir) - 1] = '\0';
    }

    /*
     * Start main loop here.
     */

    /*
     * Open information log file.
     */
    Options.logFile = (char *)ffd_malloc(strlen(Options.undoDir) +
					 strlen("/fixfdmn.") +
					 strlen(domain.dmnName) +
					 strlen(".log") + 1);
    if (NULL == Options.logFile) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_30, 
			 "log file name"));
	failed_exit();
    }
    sprintf(Options.logFile, "%s/fixfdmn.%s.log", Options.undoDir,
	    domain.dmnName);

    /*
     * Use safe_open to close a possible security hole.
     * 
     * The log file could be create:
     *    in /tmp so need to allow sticky bit. 
     *    on a NFS filesystem
     *    on a relative path (ie ../logfile)
     *    on a path with sym-links owned by other than UID
     *    in directories which are writeable to a group ID
     *    were the current directory is writeable to group/world
     *    with a different owner (NFS root becomes nobody)
     *
     * safe_open sets mode to 0600.
     */
    soflags = (OPN_TRUST_STICKY_BIT | OPN_FSTYPE_REMOTE | OPN_RELATIVE | 
	       OPN_TRUST_DIR_OWNERS | OPN_TRUST_GROUP | 
	       OPN_TRUST_STARTING_DIRS | OPN_UNOWNED);
    loop = 3;
    do {
	logfd = safe_open(Options.logFile,
			  O_RDWR | O_CREAT | O_APPEND, soflags);
    } while (-1 == logfd && ENOENT == errno && --loop);

    if (-1 == logfd) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_31, 
			 "Can't open log file %s.\n"), Options.logFile);
	perror(Prog);
	failed_exit();
    }

    Options.logHndl = fdopen(logfd, "a+");
    if (NULL == Options.logHndl) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_32, 
			 "Can't open log file handle.\n"));
	perror(Prog);
	failed_exit();
    }	

    /*
     * Parse /etc/fdmns and get number of volumes and device names.
     */
    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_33, 
		     "Gathering volume information.\n"));
    status = get_domain_volumes (domain.dmnName, &domain);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_34, 
			 "Error obtaining volume information.\n"));
	failed_exit();
    }

    /* 
     * Get file descriptors for each volume and check the 
     * magic number. 
     */
    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_35, 
		     "Opening the volumes.\n"));
    status = open_domain_volumes (&domain);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_36, 
			 "Error opening the volumes.\n"));
	failed_exit();
    }

    if (TRUE == Options.undo) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_37, 
			 "Restoring domain from the undo files.\n"));
	status = restore_undo_files(&domain);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_38, 
			     "Error restoring undo files.\n"));
	    failed_exit();
	}
	/* 
	 * Don't use clean_exit - it will write undo files and 
	 * activate dmn. 
	 */
	writemsg(SV_NORMAL | SV_LOG_INFO,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_39, 
			 "Restore from undo files completed.\n"));
	exit(EXIT_SUCCESS);
    } /* end option types */

    /* 
     * The next routine is used to synchronize values across all
     * the volumes in the domain. A command line option will cause only
     * this routine to be executed.  
     */

    /* 
     * Check domain/volume attributes and sync them across volumes. 
     * This needs to be called before load_subsystem_extents.
     */
    writemsg(SV_NORMAL | SV_LOG_INFO,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_40, 
		     "Checking the RBMT.\n"));
    status = correct_domain_rbmt (&domain);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_41,
			     "Corrected problems in the RBMT.\n"));
	    prompt_user_to_continue(&domain, 
				    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_42, 
					    "Fixed errors in the RBMT.\n"));
	} else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_43, 
			     "Error correcting the RBMT.\n"));
	    failed_exit();
	}
    }

    /*
     * Can exit at this point if only doing SYNC.
     */
    if (OT_SYNC == Options.typeStatus) {
	clean_exit(&domain);
    } /* end option types */
    
    /*
     * Get the LBN arrays for the metadata structures stored in
     * the RBMT/BMT0.
     */
    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_44, 
		     "Loading subsystem extents.\n"));
    status = load_subsystem_extents(&domain);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_45, 
			 "Error collecting subsystem extents.\n"));
	failed_exit();
    }

    /*
     * Now that we have the subsystems, make a best guess estimate for
     * the amount of memory we will need, and see if it can be malloc'ed.
     */
    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_QUOTA(Options.typeStatus)) ||
	(OT_IS_FRAG(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus))) {
	status = predict_memory_usage(&domain);
	if (SUCCESS != status) {
	    /*
	     * predict_memory_usage will always print something if it
	     * errors out.
	     */
	    failed_exit();
	}
    } /* end option types */
	    
    /*
     * The following routine is called if the log has been selected,
     * to check log extents and set up the log to be empty.
     */
    if ((NULL == Options.type) ||
	(OT_IS_LOG(Options.typeStatus))) {
	writemsg(SV_NORMAL | SV_LOG_INFO,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_46, 
			 "Clearing the log on volume %s.\n"),
		 domain.volumes[domain.logVol].volName);
	status = clear_log(&domain);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_47, 
			     "Error clearing the log.\n"));
	    failed_exit();
	}
    } /* end option types */

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
    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_QUOTA(Options.typeStatus)) ||
	(OT_IS_FRAG(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus))) {

	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_48, 
			 "Checking the BMT mcell data.\n"));
	status = correct_bmt_mcell_data(&domain);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_49, 
				 "Corrected BMT mcell data.\n"));
		prompt_user_to_continue(&domain, 
					catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_50, 
						"Fixed errors in BMT mcell data.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_51, 
				 "Error correcting BMT mcell data.\n"));
		failed_exit();
	    }
	}

	/*
	 * Need to clear out the deferred delete list.
	 */
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_52, 
			 "Checking the deferred delete list.\n"));
	status = clear_deferred_delete_list(&domain);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_53, 
				 "Cleared the deferred delete list.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_54, 
				 "Error clearing the deferred delete list.\n"));
		failed_exit();
	    }
	}
    } /* end option types */

    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_QUOTA(Options.typeStatus)) ||
	(OT_IS_FRAG(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus))) {
	/*
	 * Correct the root tag file. (-2 means root tag file).
	 * This is needed to continue.
	 */

	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_55, 
			 "Checking the root tag file.\n"));
	status = correct_tag_file(&domain, -2);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_56, 
				 "Corrected the root tag file.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_57, 
				 "Error correcting the root tag file.\n"));
		failed_exit();
	    }
	}

	if (NULL == domain.filesets) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_58, 
			     "Unable to continue, no fileset information was found.\n"));
	    failed_exit();
	}

	if (NULL != Options.filesetName) {
	    int fsCount;
	    int fsNameLen;
	    int haveListedFilesets;
	    int lookingForFileset;

	    fsCount = 0;
	    fsNameLen = 0;
	    haveListedFilesets = FALSE;
	    lookingForFileset  = TRUE;

	    while (TRUE == lookingForFileset) {
		fileset = domain.filesets;
		while (NULL != fileset) {
		    /*
		     * We only want to work on specified setName.
		     */
		    if (0 == strcmp(Options.filesetName, fileset->fsName)) {
			lookingForFileset = FALSE;
			break;
		    }
		    fileset = fileset->next;
		}

		if (NULL == fileset) {
		    /*
		     * Unable to find the fileset they requested to be checked.
		     */
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_59, 
				     "Unable to find the requested fileset '%s'.\n"),
			     Options.filesetName);

		    if (FALSE == haveListedFilesets) {
			/*
			 * List out the names of all of the found filesets.
			 */
			fsCount = 0;
			fileset = domain.filesets;

			writemsg (SV_ERR | SV_LOG_INFO,
				  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_60,
					  "The following filesets were found in this domain:\n"));
			while (NULL != fileset) {
			    fsCount++;
			    writemsg (SV_ERR | SV_CONT | SV_LOG_INFO,
				      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_61, 
					      "     %s\n"), 
				      fileset->fsName);
			    fileset = fileset->next;
			}
			writemsg (SV_ERR | SV_CONT, "\n");
			haveListedFilesets = TRUE;
		    } /* end if (FALSE == haveListedFilesets) */

		    writemsg(SV_ERR | SV_CONT, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_7, 
				     "Do you want to continue? ([y]/n)  "));

		    status = prompt_user_yes_no(prompt, "y");
		    if (SUCCESS != status) {
			prompt[0] = 'n';
		    }
	
		    writemsg(SV_ERR | SV_CONT, "\n");

		    if ('n' == prompt[0]) {
			failed_exit();
		    }

		    /*
		     * The user wants to continue.  Ask if all filesets
		     * should be processed.
		     */
		    writemsg (SV_ERR | SV_LOG_ERR,
			      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_62, 
				      "Do you want %s to process all filesets? ([y]/n)  "),
			      Prog);

		    status = prompt_user_yes_no(prompt, "y");
		    if (SUCCESS != status) {
			prompt[0] = 'n';
		    }
	
		    writemsg(SV_ERR | SV_CONT, "\n");

		    if ('y' == prompt[0]) {
			Options.filesetName = NULL;
			lookingForFileset   = FALSE;
		    } else {
			writemsg (SV_ERR | SV_LOG_ERR,
				  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_63,
					  "Please enter the corrected fileset name:  "));
			status = prompt_user (selectedSetName, BS_SET_NAME_SZ);
			if (SUCCESS != status) {
			    return status;
			}
			fsNameLen = strlen(selectedSetName) - 1;
			selectedSetName[fsNameLen] = '\0';
			Options.filesetName = selectedSetName;
		    } /* end if ('y' == prompt[0]) */
		} /* end if unable to find fileset */
	    } /* end while (TRUE == lookingForFileset) */
	} /* end if (NULL != Options.filesetName) */
    }  /* end option types */

    /*
     * Sort the filesets such that all the clones are at the end of the list.
     */
    priorFileset = NULL;
    fileset = domain.filesets;

    while (NULL != fileset) {
	nextFileset = fileset->next;
	if (FALSE == FS_IS_CLONE(fileset->status) && (NULL != priorFileset)) {
	    priorFileset->next = nextFileset;
	    fileset->next = domain.filesets;
	    domain.filesets = fileset;
	} else {
	    priorFileset = fileset;
	}
	fileset = nextFileset;
    }

    if ((NULL == Options.type) ||
	(OT_IS_FRAG(Options.typeStatus)) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_QUOTA(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus))) {

	fixed = FALSE;
	if (Options.verboseOutput < SV_VERBOSE) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_64, 
			     "Checking the tag file(s).\n"));
	}

	/*
	 * Loop through the filesets.
	 */
	fileset = domain.filesets;
	while (NULL != fileset) {
	    if (NULL != Options.filesetName) {
		/*
		 * We only want to work on specified setName.
		 */
		if (0 != strcmp(Options.filesetName, fileset->fsName)) {
		    fileset = fileset->next;
		    continue;
		}
	    }
	    
	    writemsg(SV_VERBOSE | SV_LOG_INFO,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_65, 
			     "Checking fileset %s's tag file.\n"),
		     fileset->fsName);
	    /*
	     * If only a clonefileset has been specified on the command line,we
             * need to ensure we load the taglist of the original fileset
             * as well
             */
	     if ( ( FS_IS_CLONE(fileset->status) == TRUE ) && ( Options.filesetName != NULL ) ) {
			orig = fileset->origClonePtr;
			status = correct_tag_file(&domain,orig->filesetId.dirTag.num);
			if (SUCCESS != status) {
				if (FIXED == status) {	
					writemsg(SV_VERBOSE | SV_LOG_FIXED,
						 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_66,
							 "Corrected fileset %s's tag file.\n"),
							  fileset->fsName);
					fixed = TRUE;
				} else {
					writemsg(SV_ERR | SV_LOG_ERR,
						 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_67,
							 "Error correcting fileset %s's tag file.\n"),fileset->fsName);
						failed_exit();
				}
			}
	   } 							
					
								
	    status = correct_tag_file(&domain, 
				      fileset->filesetId.dirTag.num);
	    if (SUCCESS != status) {
		if (FIXED == status) {
		    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_66, 
				     "Corrected fileset %s's tag file.\n"),
			     fileset->fsName);
		    fixed = TRUE;
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_67,
				     "Error correcting fileset %s's tag file.\n"),
			     fileset->fsName);
		    failed_exit();
		}
	    }
	    fileset = fileset->next;
	} /* end fileset loop */
	if (TRUE == fixed) {
	    status = prompt_user_to_continue(&domain, 
					     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_68, 
						     "Fixed errors in tag files.\n"));
	}
    } /* end option types */

    /*  
     * Call correct nodes to find loops in mcell chains, collects SBM
     * info and adds pages to the number found from extent records, and
     * saves the original stats and frag information in the tag list.  
     */
    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_QUOTA(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_FRAG(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus))) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_69, 
			 "Checking the mcell nodes.\n"));
	status = correct_nodes(&domain);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_70, 
				 "Corrected the mcell nodes.\n"));
		status = prompt_user_to_continue(&domain, 
						 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_71, 
							 "Fixed errors in mcell nodes.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_72, 
				 "Error correcting mcell nodes.\n"));
		failed_exit();
	    }
	}
    } /* end option types */


    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus)) ||
	(OT_IS_FRAG(Options.typeStatus))) {
	fixed = FALSE;

	if (Options.verboseOutput < SV_VERBOSE) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_73, 
			     "Checking the BMT chains.\n"));
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_74, 
			     "Checking the frag file group headers.\n"));
	}

	fileset = domain.filesets;
	while (NULL != fileset) {
	    if (NULL != Options.filesetName) {
		/*
		 * We only want to work on specified setName.
		 */
		if (0 != strcmp(Options.filesetName, fileset->fsName)) {
		    fileset = fileset->next;
		    continue;
		}
	    }

	    writemsg(SV_VERBOSE | SV_LOG_INFO,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_75, 
			     "Checking fileset %s's BMT chains.\n"), 
		     fileset->fsName);
	    /*  
	     * To save reads check the tags while page is loaded into
	     * memory.  This is a VERY slow operation as we will be
	     * following chains, which could mean loading some pages as
	     * many times as it has mcells.  We have already looked for
	     * chain loops, so we don't need to worry about that at this
	     * point. 
	     */
	    status = correct_bmt_chains(fileset);
	    if (SUCCESS != status) {
		if (FIXED == status) {
		    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_76, 
				     "Corrected the BMT chains on fileset %s.\n"),
			     fileset->fsName);
		    fixed = TRUE;
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_77, 
				     "Error correcting the BMT chains on fileset %s.\n"),
			     fileset->fsName);
		    failed_exit();
		}
	    }
	    fileset = fileset->next;
	} /* end fileset loop */
	if (TRUE == fixed) {
	    status = prompt_user_to_continue(&domain, 
					     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_78, 
						     "Fixed errors in BMT chains.\n"));
	}
    } /* end option types */

    if ((NULL == Options.type) ||
	(OT_IS_FRAG(Options.typeStatus))) {

	fixed = FALSE;
	if (Options.verboseOutput < SV_VERBOSE) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_79, 
			     "Checking for frag overlaps.\n"));
	}

	fileset = domain.filesets;
	while (NULL != fileset) {
	    if (NULL != Options.filesetName) {
		/*
		 * We only want to work on specified setName.
		 */
		if (0 != strcmp(Options.filesetName, fileset->fsName)) {
		    fileset = fileset->next;
		    continue;
		}
	    }

	    writemsg(SV_VERBOSE | SV_LOG_INFO,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_80, 
			     "Checking for frag overlaps in fileset %s.\n"),
		     fileset->fsName);

	    status = correct_frag_overlaps(fileset, &domain);
	    if (SUCCESS != status) {
	        if (FIXED == status) {
	            writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_81, 
				     "Corrected frag overlaps in fileset %s.\n"),
	                     fileset->fsName);
		    fixed = TRUE;
	        }
	        else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_82, 
				     "Error correcting frag overlaps in fileset %s.\n"),
			     fileset->fsName);
		    failed_exit();
		}
	    }
	    fileset = fileset->next;
	} /* end fileset loop */
	if (TRUE == fixed) {
	    prompt_user_to_continue(&domain, 
				    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_83, 
					    "Fixed frag file overlaps.\n"));
	}
    }  /* end option types */
    
    /*
     * If the ddl had clone files on it we need to handle them.
     */
    if (NULL != domain.ddlClone) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_84, 
			 "Checking for clone files on the deferred delete list.\n"));
	status = correct_clone_files_on_ddl(&domain);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_85, 
				 "Corrected clone files on the deferred delete list.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_86, 
				 "Error correcting clone files on the deferred delete list.\n"));
		failed_exit();
	    }
	}
    } /* end if we have ddl clone files */

    /*
     * We have now cleaned up all the mcell related structures.
     * We need to check if we still have any mcell which are orphans.
     */
    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_QUOTA(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus))) {
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_87, 
			 "Checking for BMT mcell orphans.\n"));
	status = correct_orphans(&domain);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_88, 
				 "Cleared BMT mcell orphans.\n"));	
		prompt_user_to_continue(&domain, 
					catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_89, 
						"Fixed BMT mcell orphans.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_90, 
				 "Error correcting BMT mcell orphans.\n"));
		failed_exit();
	    }
	}
    } /* end option types */

    /* 
     * Call correct_sbm_overlaps to deal with any overlaps the SBM
     * has detected.
     */
    if ((NULL == Options.type) ||
	(OT_IS_SBM(Options.typeStatus))) {
	
	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_91, 
			 "Checking for file overlaps.\n"));
	status = correct_sbm_overlaps(&domain);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_92, 
				 "Corrected file overlaps.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_93, 
				 "Error correcting file overlaps.\n"));
		failed_exit();
	    }
	}
    } /* end option types */

    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus))) {
	
	if (Options.verboseOutput < SV_VERBOSE) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_94, 
			     "Checking the directories.\n"));
	}

	fileset = domain.filesets;
	while (NULL != fileset) {
	    if (NULL != Options.filesetName) {
		/*
		 * We only want to work on specified setName.
		 */
		if (0 != strcmp(Options.filesetName, fileset->fsName)) {
		    fileset = fileset->next;
		    continue;
		}
	    }

	    writemsg(SV_VERBOSE | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_95, 
			     "Checking fileset %s's directories.\n"), 
		     fileset->fsName);

	    status = correct_all_dirs(fileset);
	    if (SUCCESS != status) {
		if (FIXED == status) {
		    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_96, 
				     "Corrected fileset %s's directories.\n"),
			     fileset->fsName);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_97, 
				     "Error correcting fileset %s's directories.\n"),
			     fileset->fsName);
		    failed_exit();
		}
	    }
	    fileset = fileset->next;
	} /* end fileset loop */
    } /* end option types */

    if ((NULL == Options.type) ||
	(OT_IS_FRAG(Options.typeStatus))) {

	fixed = FALSE;
	if (Options.verboseOutput < SV_VERBOSE) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_98, 
			     "Checking the frag file(s).\n"));
	}

	fileset = domain.filesets;
	while (NULL != fileset) {
	    if (NULL != Options.filesetName) {
		/*
		 * We only want to work on specified setName.
		 */
		if (0 != strcmp(Options.filesetName, fileset->fsName)) {
		    fileset = fileset->next;
		    continue;
		}
	    }

	    writemsg(SV_VERBOSE | SV_LOG_INFO,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_99, 
			     "Checking fileset %s's frag file.\n"),
		     fileset->fsName);

	    status = correct_frag_file(fileset, &domain);
	    if (SUCCESS != status) {
	        if (FIXED == status) {
	            writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_100,
				     "Corrected fileset %s's frag file.\n"),
	                     fileset->fsName);
		    fixed = TRUE;
	        }
	        else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_101,
				     "Error correcting fileset %s's frag file.\n"),
			     fileset->fsName);
		    failed_exit();
		}
	    }
	    fileset = fileset->next;
	} /* end fileset loop */
	if (TRUE == fixed) {
	    prompt_user_to_continue(&domain,
				    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_102, 
					    "Fixed errors in the frag file.\n"));
	}
    }  /* end option types */

    /*
     * After this point, delete_tag should never be called.
     */
    if ((NULL == Options.type) ||
	(OT_IS_QUOTA(Options.typeStatus))) {

	if (Options.verboseOutput < SV_VERBOSE) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_103, 
			     "Checking the quota files.\n"));
	}

	fileset = domain.filesets;
	while (NULL != fileset) {
	    if (NULL != Options.filesetName) {
		/*
		 * We only want to work on specified setName.
		 */
		if (0 != strcmp(Options.filesetName, fileset->fsName)) {
		    fileset = fileset->next;
		    continue;
		}
	    }

	    writemsg(SV_VERBOSE | SV_LOG_INFO,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_104, 
			     "Checking fileset %s's quota file.\n"), 
		     fileset->fsName);

	    status = correct_quota_files(fileset);
	    if (SUCCESS != status) {
		if (FIXED == status) {
		    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_105, 
				     "Corrected fileset %s's quota files.\n"),
			     fileset->fsName);

		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_106, 
				     "Error correcting fileset %s's quota files.\n"),
			     fileset->fsName);
		    failed_exit();
		}
	    }
	    fileset = fileset->next;
	} /* end fileset loop */
    } /* end option types */

    /* 
     * correct_sbm has been moved to clean_exit.
     */

    clean_exit(&domain);
}  /* end main */


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
void prepare_signal_handlers (void)
{
    char	     *funcName = "prepare_signal_handlers";
    sigset_t	     newset;
    sigset_t	     *newsetp;
    struct sigaction intAction;	  /* action structure for signals */
    struct sigaction emptyAction; /* empty action structure used in call */

    /*
     * Initialize Variables
     */
    newsetp = &newset;
    
    /* 
     * Set up intAction to identify signal_handler as the signal handler 
     * for interrupts.
     */
    intAction.sa_handler = signal_handler;
    intAction.sa_mask	 = 0;
    intAction.sa_flags	 = 0;
    
    /*
     * Call sigaction using intAction for SIGINT, SIGQUIT, and SIGTERM.
     */
    sigaction (SIGINT, &intAction, &emptyAction);
    sigaction (SIGQUIT, &intAction, &emptyAction);
    sigaction (SIGTERM, &intAction, &emptyAction);
    
    /*
     * Call sigemptyset to set up newset as the signal mask.
     */
    sigemptyset (newsetp);
    
    /*
     * Call sigaddset to add SIGINT, SIGQUIT, and SIGTERM to the signal mask.
     */
    sigaddset (newsetp, SIGINT);
    sigaddset (newsetp, SIGQUIT);
    sigaddset (newsetp, SIGTERM);
    
    /*
     *Call sigprocmask with newset to unblock signals specified in signal mask.
     */
    sigprocmask (SIG_UNBLOCK, newsetp, NULL);
    
} /* end prepare_signal_handlers */


/*
 * Function Name: signal_handler (3.3)
 *
 * Description: This routine asks the user if they want to abort fixfdmn when
 *    a SIGINT or SIGQUIT interrupt is received. If so, we exit, otherwise, we
 *    reset the signal handler and return. If we receive a SIGTERM interrupt,
 *    then we exit.
 *
 * Input parameters:
 *    sigNum: Number of the signal received.
 *
 * Output parameters: N/A
 *
 * Return values: If the user wants to abort, exit with EXIT_FAILED.
 */
void signal_handler(int sigNum)
{
    char   *funcName = "signal_handler";
    int	   abort;
    struct sigaction newInt;  /* new interrupt action structure	    */
    struct sigaction oldInt;  /* old interrupt action structure	    */
    
    /*
     * Initialize Variables
     */
    abort = FALSE; /* flag indicating if abort is wanted */

    /*
     * Set up newInt to ignore signals.
     */
    newInt.sa_handler = SIG_IGN;
    newInt.sa_mask    = 0;
    newInt.sa_flags   = 0;

    if ((SIGINT == sigNum) || (SIGQUIT == sigNum)) {
	/*
	 * The following 2 calls save the current action structure in 
	 * oldInt and use the new structure in newInt.
	 */

	/*
	 * Call sigaction for SIGINT to block new interrupts.
	 */
	sigaction (SIGINT, &newInt, &oldInt);

	/* 
	 * Call sigaction for SIGQUIT to block new interrupts.
	 */
	sigaction (SIGQUIT, &newInt, &oldInt);

	/*
	 * Call want_abort to see if user wants to abort fixfdmn.
	 */
	abort = want_abort ();
	
	/*
	 * Call sigaction to reset signal action for SIGINT.
	 */
	sigaction (SIGQUIT, &oldInt, NULL);
	
	/*
	 * Call sigaction to reset signal action for SIGQUIT.
	 */
	sigaction (SIGINT, &oldInt, NULL);
	
    } else {
	/*
	 * Set abort.
	 */
	abort = TRUE;
    }
  
    if (TRUE == abort) {
	failed_exit();
    }
} /* end signal_handler */


/*
 * Function Name:  want_abort (3.4)
 *
 * Description:	 This routine prompts the user to see if they really want
 *		 to abort fixfdmn.  If stdin is being used for input, or 
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
int want_abort (void)
{
    char *funcName = "want_abort";
    char abortAnswer[10];  /* response to prompt */
    int	 abort;
    int  status;
    
    /*
     * Prompt the user if they want to abort.
     */
    writemsg(SV_ERR | SV_CONT, "\n");
    writemsg(SV_ERR, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_107,
			     "Do you want to abort? (y/n)  "));
	
    status = prompt_user_yes_no(abortAnswer, NULL);
    if (SUCCESS != status) {
	abortAnswer[0] = 'y';
    }
	
    if ('y' == abortAnswer[0]) {
	abort = TRUE;
    } else {
	abort = FALSE;
    }
    
    return (abort);
} /* end want_abort */

/* end fixfdmn.c */
