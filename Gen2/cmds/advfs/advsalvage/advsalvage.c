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
 * Advanced File System On-disk structure Salvager
 *
 */

#ifndef lint
#endif

#include "advsalvage.h"

/*
 * Global
 */
nl_catd mcat;
int64_t nodeCounter;
int64_t maxNodes;

/*
 * Private protos
 */
const char salvage_optstring[] = "VPSxplv:d:L:D:o:s:M:T:U:u:Q:";



void usage( void )
{
    char   *funcName = "usage";
    
    /*
     * Do not use writemsg for this one, we don't want salvage preappended.
     */
    fprintf(stderr, catgets(mcat, S_SALVAGE_1, SALVAGE_1, "usage: %s [-V] [-x|-p] [-l] [-S] [-v num] [-d time] [-D dir] [-L path] [-o option] [-U format [-u archive]] {-s special [-s special]... | fsname} [snap_id [path]]\n"), Prog);
    fflush( stderr );
}  /* end usage */


/*
 * Function Name: main
 *
 * Description:
 *  This is the main routine for the salvage utility.
 *
 * Input parameters:
 *  argc: Argument Count 
 *  argv: Array of arguments
 *
 * Exit values: EXIT_SUCCESS, EXIT_FAILED or EXIT_PARTIAL.
 */
main(int  argc, 
     char *argv[])
{
    char          *funcName = "main";
    extern int    getopt();
    extern int    optind;
    int           status;
    int           c;
    int           V = 0;
    int           x = 0, p = 0, l = 0;
    int           v = 0, d = 0, L = 0;
    int           D = 0, o = 0, s = 0;
    int           M = 0, T = 0, Pass3 = 0;
    int	          U = 0, u = 0;
    int           P = 0, Q = 0;
    int           euid;
    int           SoftLimit;
    int           bomb = 0;
    int           vol;
    domainInfoT   domain;
    struct stat   statBuf;
    struct rlimit rlimit;
    char          restorePath[MAXPATHLEN];
    char          restoreLogPath[MAXPATHLEN];
    char          archiveName[MAXPATHLEN];
    char          qtestPath[MAXPATHLEN];
    char          *pp;  /* Used for character to int conversion */
    char          *dmnName     = NULL;
    char          *setName     = NULL;
    char          *pathName    = NULL;
    char          *logPath     = NULL;
    char          *dirPath     = NULL;
    char          *owrite      = NULL;
    char          *logMode = "a"; /* mode for opening log file */
    filesetLLT    *fileset;       /* pointer used to loop thru filesets */

#ifdef PROFILE
    {
	int i;
	for (i = 0 ; i < FUNC_NUMBER_FUNCTIONS ; i++) {
	    prof_timeused[i] = 0;
	    prof_currcalls[i] = 0;
	    prof_numcalls[i] = 0;
	}
    }

    PROF_START(func);
#endif /* PROFILE */

    /* 
     * store only the file name part of argv[0] 
     */
    if ((Prog = strrchr( argv[0], '/' )) == NULL) 
    {
        Prog = argv[0];
    } 
    else 
    {
        Prog++;
    }

    /*
     * Setup salvage message catalog.  Pax message catalog is set up in
     * setup_tar().
     */
#if defined(NLS) || defined(NLS16)
    if (!setlocale(LC_ALL, "")) {
        fputs(_errlocale(name), stderr);
        putenv("LANG=");
    }
    mcat = catopen(MF_ADVSALVAGE_ADVFS, NL_CAT_LOCALE);
#endif

    /* 
     * Check to see if -Q option is passed in, if so skip superuser
     * activities and use regular files instead of volumes.
     */
    Options.qtest=0;
    while ((c = getopt( argc, argv, salvage_optstring)) != EOF) 
    {
        switch (c) 
	{
	    case 'Q':
		Q++;
		if (strlen(optarg) >= MAXPATHLEN)
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1,SALVAGE_295, 
			     "Quick test path '%s' is too long\n"),
			     optarg);
		    bomb++;
                    break;
		}
                strcpy(qtestPath,optarg);
            
                /*
                 *  Check to see if this is a directory
                 */
                if (stat(qtestPath, &statBuf) == -1) 
                {
                    writemsg(SV_ERR,catgets(mcat, S_SALVAGE_1,SALVAGE_5,
                                "stat() failed on '%s'\n"), qtestPath);
                    perror(Prog);
                    bomb++;
                    break;
                }

                if (! S_ISDIR(statBuf.st_mode)) 
                {
                    writemsg(SV_ERR,catgets(mcat, S_SALVAGE_1,SALVAGE_294,
                                "Quick test path '%s' is not a directory\n"), qtestPath);
                    bomb++;
                    break;
                }
                Options.qtest =1;
                Options.qtestPath = qtestPath;
		break;

	    default:
		break;
	} /* end switch */
    } /* end while */
    
    if (bomb > 0)
    {
	usage();
	exit(EXIT_FAILED);
    }

    /* Reset the getopt counter for later when we do real parameter parsing */
    optind = 1;

    /* 
     * If not run as root, then we must be in test mode, otherwise die.
     */
    if ((euid =geteuid()) && (!Options.qtest)) 
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_2, 
		 "Permission denied - must be a privileged user to run\n"));
	exit(EXIT_FAILED);
    }

    /*
     * Need to make sure we have access to all the memory we
     * can get access to.  This is important for filesets with
     * large number of files.
     */
    rlimit.rlim_max = RLIM_INFINITY;
    rlimit.rlim_cur = RLIM_INFINITY;

    if (euid ==0) {
	/* 
	if (setrlimit(RLIMIT_DATA, &rlimit))
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_3, 
		   "setrlimit() failed - Unable to change memory usage limits\n"));
	    perror(Prog);
	    exit(EXIT_FAILED);
	}
	*/
    }

    /*
     * Initialize Variables
     */ 
    domain.dmnId.id_sec = 0;
    domain.dmnId.id_usec = 0;
    domain.fsCookie.id_sec =0;
    domain.fsCookie.id_usec =0;
    domain.recoverType = 0;
    domain.filesets = NULL;
    domain.volumes = NULL;
    domain.numberVolumes = 0;

    D_SET_RESTORE(domain.status);
    D_SET_PASS1_NEEDED(domain.status);
    D_SET_PASS2_DEFAULT(domain.status);
    D_SET_PASS3_DEFAULT(domain.status);
    D_SET_VOL_SETUP_NEEDED(domain.status);

    Options.recPartial = 1;     /* default to recover partial files */
    Options.overwrite = OW_YES; /* default to overwrite files */
    Options.tagSoftLimit = TAGS_SOFTLIMIT; /* default to tags softlimit */

    Options.tagHardLimit = MAX_ADVFS_TAGS; 

    Options.outputFormat = F_DISK;	/* default to use disk format */
    archiveName[0] = '\0';
    Options.archiveName = archiveName;
    Options.stdoutArchive = 0;
    Options.progressReport = 0;

    maxNodes = 0;

    /*
     * Open a local /dev/tty for prompting the user during an error.
     * If the fopen () fails, Localtty will be NULL.
     */
    Localtty = fopen ("/dev/tty", "r");

    /*
     * Prepare to get any signal interrupts.
     */
    prepare_signal_handlers ();

    if (SUCCESS != create_volumes(&domain.volumes))
    {
        exit(EXIT_FAILED);
    }

    /* 
     * Get user-specified command line arguments. 
     */
    while ((c = getopt( argc, argv, salvage_optstring)) != EOF) 
    {
        switch (c) 
	{
	    case 'V':  /* Print out verfied command line */
	        V++;
		break;

	    case 'x':  /* Don't recover partial files */
	        x++;
		Options.recPartial = 0;
		break;

	    case 'p':  /* Add suffix to partial files */
		p++;
		Options.addSuffix++;
		break;

	    case 'l':  /* Verbose logging */
		l++;
		Options.verboseLog++;
		break;

	    case 'v':  /* Verbose output */
		v++;
		Options.verboseOutput = strtoul( optarg, &pp, 0 );
		if ((Options.verboseOutput < 0) || (Options.verboseOutput > 3))
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_4, 
                             "Verbose flag outside of valid range\n"));
		    bomb++;
		}
		break;
		
	    case 'd':  /* Recover files which are older than this date */
		d++;
		Options.recoverDate.tv_sec = convert_date(optarg);

		if (-1 == Options.recoverDate.tv_sec)
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1,SALVAGE_298, 
			     "Illegal time value '%s'\n"), optarg);
		    bomb++;
		}
		break;

	    case 'L':  /* Place log in this location */
		L++;
		logPath = optarg;

		if (strlen(logPath) >= MAXPATHLEN)
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1,SALVAGE_253, 
			     "Recover log path '%s' is too long\n"),
			     logPath);
		    bomb++;
		}
		else
		{
		    strcpy(restoreLogPath,logPath);
		
		    /*
		     *  Check to see if we need to append the log name.
		     */
		    if (stat(restoreLogPath, &statBuf) == -1) 
		    {
			if (errno == ENOENT)
			{
			    /*
			     * Requested file does not exist.
			     */
			    break;
			}

			writemsg(SV_ERR,catgets(mcat, S_SALVAGE_1,SALVAGE_5,
				 "stat() failed on '%s'\n"), restoreLogPath);
			perror(Prog);
			bomb++;
			break;
		    }

		    /* 
		     * If a directory append the log name.
		     */
		    if (S_ISDIR(statBuf.st_mode)) 
		    {
			if ((strlen(restoreLogPath) + 
			     strlen(catgets(mcat, S_SALVAGE_1, SALVAGE_6, 
					    "/advsalvage.log"))) >= MAXPATHLEN)
			{
			    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1,
				     SALVAGE_252, 
				     "Recover log path '%s%s' is too long\n"),
				     restoreLogPath,
				     catgets(mcat, S_SALVAGE_1, SALVAGE_6,
				     "/advsalvage.log"));
			    bomb++;
			}
			else
			{
			    strcat(restoreLogPath,catgets(mcat, S_SALVAGE_1,
				   SALVAGE_6, "/advsalvage.log"));
			}
		    }
		}
		break;

	    case 'D':  /* Place recovered files in the directroy */
		D++;
		dirPath = optarg;
		if (strlen(dirPath) >= PATH_MAX)
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_251,
			     "Recover directory path '%s' is too long\n"),
			     dirPath);
		    bomb++;
		}
		else 
		{
		    strcpy(restorePath, dirPath);
		}
		break;

	    case 'o':  /* Specify what to do if file already exists */
		o++;
		owrite = optarg;

		if ((strcmp(owrite, catgets(mcat, S_SALVAGE_1, SALVAGE_7, 
                                            "yes")) == 0) ||
		    (strcmp(owrite, catgets(mcat, S_SALVAGE_1, SALVAGE_8, 
					    "y")) == 0)) 
		{
		    Options.overwrite = OW_YES;
		} 
		else if ((strcmp(owrite, catgets(mcat, S_SALVAGE_1, 
						 SALVAGE_9, "no")) == 0) ||
			 (strcmp(owrite, catgets(mcat, S_SALVAGE_1, 
						 SALVAGE_10, "n")) == 0)) 
		{
		    Options.overwrite = OW_NO;
		}
		else if (strcmp(owrite, catgets(mcat, S_SALVAGE_1, 
						SALVAGE_11, "ask")) == 0) 
		{
		    Options.overwrite = OW_ASK;
		} 
		else 
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_12,
			     "Invalid parameter for option '-o'\n"));
		    bomb++;
		}
		break;

	    case 's':  /* Specify which volumes to checks */
		if (s >= MAX_VOLUMES) 
		{
		    /* 
		     * Max number of volumes exceeded   
		     * Need to print message then exit 
		     */
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_13,
		             "Too many volumes selected - '%s' puts us over the limit\n"), 
			     domain.volumes[s].volName);
		    bomb++;
		    break;
		}
		domain.volumes[s].volName = optarg;
		
		if (stat(domain.volumes[s].volName, &statBuf) == -1) 
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_14,
			     "stat() failed for volume '%s'\n"), 
			     domain.volumes[s].volName);
		    perror(Prog);
		    bomb++;
		    break;
		}

		/* 
		 * Skip if not block device 
		 */
		if (!S_ISBLK(statBuf.st_mode)) 
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_15,
			     "Volume '%s' is not a block device\n"),
			     domain.volumes[s].volName);
		    bomb++;
		    break;
		}

		s++;
		break;


	    case 'S': /* Sequential search. AKA Pass3. */
		/*
		 * Skip pass 1 and 2, and do pass 3 instead.
		 */
		D_SET_PASS1_COMPLETED(domain.status);
		D_SET_PASS2_COMPLETED(domain.status);
		D_SET_PASS3_NEEDED(domain.status);
		D_SET_VOL_SETUP_PASS3(domain.status);
		Pass3++;

		break;

	    case 'U':
		U++;
		if (0 == strcmp(optarg, "tar") ||
		    0 == strcmp(optarg, "TAR"))
		{
		    Options.outputFormat = F_TAR;
		}
		else if (0 == strcmp(optarg, "vdump") ||
			 0 == strcmp(optarg, "VDUMP"))
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_261,
			     "Vdump output format is not yet supported.\n"));
		    bomb++;
		}
		else
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1,
			     SALVAGE_259, "Invalid output format '%s'\n"),
			     optarg);
		    bomb++;
		}
		break;

	    case 'u':
		u++;
		if (strlen(optarg) >= MAXPATHLEN)
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_256,
			     "Archive name '%s' is too long\n"),
			     optarg);
		    bomb++;
		}
		else 
		{
		    strcpy(archiveName, optarg);
		}
		break;

	    /*
	     * The following are HIDDEN flags.
             */
            /*
             * The fsCookie format is: SEC.USEC
             *
	     */
	    case 'M':
		M++;
                double ck;
		ck = strtod( optarg, NULL );
                /*
                 * If epoch requested is less than year 1980, then it's 
                 * probably incorrect input.
                 */
                if (ck < 315576000.0) {
                    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_16, 
                            "The fsCookie value %f requested is prior to the year 1980.\n"),
                             ck);
                    if (want_abort()) {
                        exit(EXIT_FAILED);
                    }
                }
		domain.fsCookie.id_sec = (uint64_t)ck;
		domain.fsCookie.id_usec = (ck-(double)(uint64_t)ck)*1000000.0+0.1;
		break;
	    
	    case 'T':
		T++;
		SoftLimit = strtoul( optarg, &pp, 0 );
		if ((SoftLimit < 1022) || (SoftLimit > MAX_ADVFS_TAGS))
	        {
		  writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_17, 
                          "Tag Softlimit flag outside of valid range\n"));
		  bomb++;
		}
		Options.tagSoftLimit = SoftLimit;
		break;

	    case 'P':
		P++;
		Options.progressReport++;
		break;

	    case 'Q':
		/* This has already been processed above */
		break;

	    default:
		bomb++;
	} /* end switch */
    } /* end while */
    
    /* 
     * These flags are only allowed once in the command line. 
     */
    if (V > 1 || x > 1 || p > 1 || l > 1 || v > 1 || d > 1 || L > 1 || D > 1 ||
	o > 1 || U > 1 || u > 1 || M > 1 || T > 1 || Pass3 > 1 || P > 1)

    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_18, 
                 "A duplicate flag was used\n"));
        usage();
	exit(EXIT_FAILED);
    }

    if (bomb > 0)
    {
	usage();
	exit(EXIT_FAILED);
    }

    if (V)
    {
        writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_291, 
                 "%s version %s\n"), Prog, salvage_version);
        exit(EXIT_SUCCESS);
    }

    /* 
     * These flags can not used together. 
     */
    if ((1 == x) && (1 == p)) 
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_19, 
                 "The flags x and p can not be used together\n"));
        usage();
	exit(EXIT_FAILED);
    }

    if ((1 == s) && (1 == Q)) 
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_292, 
                 "The flags s and Q can not be used together\n"));
        usage();
	exit(EXIT_FAILED);
    }
    
    /*
     * The u flag requires the U flag.
     */
    if ((0 == U) && (1 == u))
    {
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_260,
		 "The u flag requires the U flag to be used\n"));
	usage();
	exit(EXIT_FAILED);
    }

    /*
     * Set defaults for when the U flag is used.
     */
    if (U)
    {
	if (0 == u)
	{
	    strcpy(archiveName, DEFTAPE); /* defined in <sys/mtio.h> */
	}

	if (0 == strcmp(archiveName, "-"))
	{
	    Options.stdoutArchive = 1;
	}

	/* setup_tar can exit the program here, but cannot return failure. */
	setup_tar();
    }

    /* 
     * If D flag is not used then we will use CWD for restore 
     */
    if (0 == D) 
    {
	strcpy(restorePath, ".");
    } 

    if ('/' != restorePath[0]) {
	/*
	 * If the restorePath doesn't start at "/", then the recover
	 * code won't work properly if it has to chdir down then back
	 * up when files to be recovered are longer than PATH_MAX.
	 */
	char cwd[MAXPATHLEN];
	char fullPath[MAXPATHLEN];
	if (NULL == getcwd(cwd, MAXPATHLEN)) {
	    /* getcwd failed - leave restorePath alone */
	}
	else if (strlen(cwd) + strlen(restorePath) >= PATH_MAX) {
	    /* path too long - leave restorePath alone */
	}
	else {
	    strcpy(fullPath, cwd);
	    if (0 != strcmp(restorePath, ".")) {
		strcat(fullPath, "/");
		strcat(fullPath, restorePath);
	    }
	    strcpy(restorePath, fullPath);
	}
    }

    Options.recoverDir = restorePath;
  
    /* 
     * If L flag not given then append the log name to restorePath 
     */
    if (0 == L) 
    {
        if ((strlen(restorePath) + 
	    strlen(catgets(mcat, S_SALVAGE_1, SALVAGE_6, "/advsalvage.log")))
	    >= MAXPATHLEN)
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_252, 
				     "Recover log path '%s%s' is too long\n"),
		     restorePath,
		     catgets(mcat, S_SALVAGE_1, SALVAGE_6, "/advsalvage.log"));
	    exit(EXIT_FAILED);
	}
        strcpy(restoreLogPath, restorePath);
	strcat(restoreLogPath,catgets(mcat, S_SALVAGE_1, SALVAGE_6, 
	       "/advsalvage.log"));
    }

    /* 
     * If s == 0 then we need to pull the domain name
     */
    if (0 == s) 
    {
        if (argc > optind + 3) 
	{
	    /* 
	     * At most we can have three additional arguments 
	     */
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_20, 
                     "Too many arguments on the command line\n"));
	    usage();
	    exit(EXIT_FAILED);
	}

	dmnName  = argv[optind];
	if (NULL == dmnName) 
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_21, 
                     "Missing fsname\n"));
	    usage();
	    exit(EXIT_FAILED);
	}

	/* 
	 * We need to figure out which volumes are part of this domain 
	 */
	status = get_domain_volumes(dmnName, &s, domain.volumes);
	if (SUCCESS != status)
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_22, 
                     "Not able to determine which volumes are in '%s'\n"),
		     dmnName);
	    exit(EXIT_FAILED);
	}

	/* 
	 * We have no volumes to work on 
	 */
	if (0 == s) 
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_23, 
                     "No valid volumes found\n"));
	    exit(EXIT_FAILED);
	}

	if (argc > optind + 1) 
	{
	    setName = argv[optind + 1];
	}

	if (argc == optind + 3) 
	{
	    pathName = argv[optind + 2];
	} 
    } 
    else 
    {
        if (argc > optind + 2) 
	{
	    /* 
	     * At most we can have two additional arguments 
	     */
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_20, 
                     "Too many arguments on the command line\n"));
	    usage();
	    exit(EXIT_FAILED);
	}

	if (argc > optind) 
	{
	    setName  = argv[optind];
	}

	if (argc == optind +2) 
	{
	    pathName = argv[optind + 1];
	} 
    }
    Options.setName  = setName;

    if ( pathName != NULL && 
         pathName[strlen(pathName)-1] == '/' )  /* trunc trailing "/" if any */
    {
        pathName[strlen(pathName)-1] = '\0';
    }
    Options.pathName = pathName;

    /*
     * Change directory to the restore path to make sure we can get 
     * there before we try to recover anything.
     */
    if (-1 == chdir (restorePath))
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_24, 
                 "Cannot change directory to '%s'\n"), restorePath);
        perror (Prog);
        exit(EXIT_FAILED);
    }

    /* 
     * Open the log file - we need to use fopen to get a descriptor of
     * type FILE returned because fprintf requires a type FILE to describe
     * the stream it writes to.
     */
    Options.logFD = fopen (restoreLogPath, logMode);
    
    /*
     * If we can't open the log file, then exit with error.
     */
    if (NULL == Options.logFD)
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_25, 
                 "Cannot open log file '%s'\n"), restoreLogPath);
        perror (Prog);
        exit(EXIT_FAILED);
    }

    if (NULL != dmnName) 
    {
        writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_26, 
                 "About to recover '%s'\n"), dmnName);
    }

    if (NULL != Options.setName) 
    {
        writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_27, 
                 "Snapshot (or file system) to be recovered '%s'\n"), Options.setName);
    }
  
    if (NULL != pathName) 
    {
        writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_28, 
                 "Path to be recovered '%s'\n"), pathName);
    }

    writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_29, 
             "Volume(s) to be used "));
    for (x = 0; x < V; x++) 
    {
        /*
	 * Must use fprintf not writemsg
	 */
	if (0 == Options.stdoutArchive)
	{
	    fprintf(stdout, "'%s' ", domain.volumes[x].volName);
	}
	else
	{
	    fprintf(stderr, "'%s' ", domain.volumes[x].volName);
	}
    } /* end for */
    if (0 == Options.stdoutArchive)
    {
	fprintf(stdout, "\n");
    }
    else
    {
	fprintf(stderr, "\n");
    }

    switch (Options.outputFormat)
    {
	case F_DISK:
	    writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_30, 
		     "Files will be restored to '%s'\n"), restorePath);
	    break;
	case F_TAR:
	    if (1 == Options.stdoutArchive)
	    {
		writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_257,
			 "Files will be archived to stdout in TAR format\n"));
	    }
	    else
	    {
		writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_258,
			 "Files will be archived to '%s' in TAR format\n"),
			 archiveName);
	    }
	    break;
	default:
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_262,
		     "Invalid output format '%d'.\n"), Options.outputFormat);
	    break;
    }
    writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_31, 
             "Logfile will be placed in '%s'\n"), restoreLogPath);

    /* 
     * Open all volumes & Verify volumes are AdvFS 
     */
    status = open_domain_volumes(&domain);
    if (SUCCESS != status)
    {
        exit(EXIT_FAILED);
    }

    if (SUCCESS != get_domain_cookie(&domain))
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_266,
		"Unable to determine the cookie number of the storage domain.\n"));
        exit(EXIT_FAILED);
    }

    /* 
     * Open BMT0 load info into structures 
     */
    if (D_IS_VOL_SETUP_NEEDED(domain.status))
    {
        int counter;

	if (SUCCESS != setup_volume(&domain))
	{
	    exit(EXIT_FAILED);
	}

	for (counter = 1; counter <= MAX_VOLUMES; counter ++) 
	{
	    int64_t bytesRead;
	    bsMPgT buffer;   
	    
	    /*
	     * Check to see if the current volume has a valid filedescriptor.
	     */
	    if (-1 == domain.volumes[counter].volFd) 
	    {
		continue;
	    }

	    /* 
	     * Read the RBMT/BMT0 page from the disk, from the known location 
	     */
	    status = read_page_by_lbn(domain.volumes[counter].volFd,
				      &buffer, 
				      RBMT_BLK_LOC, 
				      &bytesRead);
	    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
	    {
		/* 
		 * Function only returns FAILURE on read or lseek errors 
		 */
	        D_SET_PASS2_NEEDED(domain.status);
	        continue;
	    }

	    /* 
	     * Check Mcell 2 for root tag file 
	     */
	    status = load_root_tag_file(&buffer, &domain, counter);
	    if (SUCCESS != status)
	    {
		D_SET_PASS2_NEEDED(domain.status);
		writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_32, 
                         "Could not read root tag from '%s'\n"),
			 domain.volumes[counter].volName);
		if (NO_MEMORY == status)
		{
		    exit(EXIT_FAILED);
		}
	    }
	}
    }

    if (D_IS_VOL_SETUP_PASS3(domain.status))
    {
	if (SUCCESS != p3_setup_volume(&domain)) 
	{
	    exit(EXIT_FAILED);
	}
    }

    if (D_IS_PASS1_NEEDED(domain.status)) 
    {
	/* 
	 * PASS 1
	 * 
	 * Go through all filesets which are to be recovered 
	 */
	if (NULL != domain.filesets)
	{
	    writemsg(SV_ALL | SV_DATE, catgets(mcat, S_SALVAGE_1, 
                     SALVAGE_33, "Starting file system/snapshot search method:\n"));
	    for (fileset = domain.filesets; 
		 fileset != NULL; 
		 fileset = fileset->next) 
	    {
		if ((FS_IS_PASS1_NEEDED(fileset->status)) &&
		    (FS_IS_RESTORE(fileset->status)))
		{
		    int volId;

		    /* 
		     * Build Dir Tree & Fill in tag array 
		     */
		    writemsg(SV_NORMAL, catgets(mcat, S_SALVAGE_1, 
			     SALVAGE_34, "Starting search of '%s'\n"),
			     fileset->fsName);

		    /*
		     * Set all the volumes default file descriptors to
		     * the block device.  build_tree is the one routine that
		     * is considerably faster with the block than with
		     * the raw.
		     */
		    for (volId = 1 ; volId <= MAX_VOLUMES ; volId ++) {
			domain.volumes[volId].volFd =
			    domain.volumes[volId].volBlockFd;
		    }

		    status = build_tree(&domain,fileset);
		    if (SUCCESS != status)
		    {
		        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, 
                                 SALVAGE_35, 
                                 "Failed search of '%s'\n"),
				 fileset->fsName);
			D_SET_PASS2_NEEDED(domain.status);
			if (NO_MEMORY == status)
			{
			    exit(EXIT_FAILED);
			}
		    }

		    /*
		     * Set all the volumes default file descriptors back
		     * to the raw device before exiting.  build_tree is the
		     * only routine that is considerably faster with the
		     * block than with the raw.
		     */
		    for (volId = 1 ; volId <= MAX_VOLUMES ; volId++) {
			domain.volumes[volId].volFd =
			    domain.volumes[volId].volRawFd;
		    }

		    FS_SET_PASS1_COMPLETED(fileset->status);
		    
		    /*
		     * Pass 2 is based on entire domain, so if fileset
		     * needs pass2 set domain flag
		     */
		    if (FS_IS_PASS2_NEEDED(fileset->status))
		    {
			D_SET_PASS2_NEEDED(domain.status);
		    }
		}
		else
		{
		    writemsg(SV_VERBOSE, catgets(mcat, S_SALVAGE_1, 
                             SALVAGE_36, "Skipping search of '%s'\n"),
			     fileset->fsName);
		} /* end if PASS1 needed on fileset*/
	    } /* end for */
	}
	else
	{
	    writemsg(SV_ALL, catgets(mcat, S_SALVAGE_1, SALVAGE_37, 
                     "Nothing found using file system/snapshot search method\n"));
	    D_SET_PASS2_NEEDED(domain.status);
	}

	D_SET_PASS1_COMPLETED(domain.status);
    } /* end if PASS1 need on domain */
    
    if (D_IS_PASS2_NEEDED(domain.status))
    {
        int counter;
 
	writemsg(SV_ALL | SV_DATE, catgets(mcat, S_SALVAGE_1, SALVAGE_38, 
                 "Starting volume search method:\n"));
	for (counter = 1; counter <= MAX_VOLUMES; counter ++) 
        {
	    /* 
	     * Verify that this a valid volume 
	     */
	    if (domain.volumes[counter].volFd != -1) 
	    {
		/* 
		 * Build Dir Tree & Fill in tag array 
		 */
	        writemsg(SV_NORMAL, catgets(mcat, S_SALVAGE_1, SALVAGE_39, 
                         "Starting search of volume '%s'\n"),
			 domain.volumes[counter].volName);
		status = p2_build_tree(&domain, counter);
		if (SUCCESS != status)
		{
		    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, 
                             SALVAGE_40, "Failed search of volume '%s'\n"),
			     domain.volumes[counter].volName);
		    if (NO_MEMORY == status)
		    {
		        exit(EXIT_FAILED);
		    }
		}
	    }
	} /* end for loop */

	status = build_cleanup(&domain);
	if (SUCCESS == status)
	{
	    D_SET_PASS2_COMPLETED(domain.status);
	}
	else
	{
	    exit(EXIT_FAILED);
	}
    } /* PASS2 needed */

    if (D_IS_PASS3_NEEDED(domain.status))
    {
        int counter;

	writemsg(SV_ALL | SV_DATE, catgets(mcat, S_SALVAGE_1, SALVAGE_41, 
                 "Starting sequential search method:\n"));
	for (counter = 1; counter <= MAX_VOLUMES; counter ++) 
        {
	    /* 
	     * Verify that this a valid volume 
	     */
	    if (domain.volumes[counter].volFd != -1) 
	    {
		/* 
		 * Build Dir Tree & Fill in tag array 
		 */
	        writemsg(SV_NORMAL, catgets(mcat, S_SALVAGE_1, SALVAGE_42, 
                         "Starting sequential search of volume '%s'\n"),
			 domain.volumes[counter].volName);
		status = p3_build_tree(&domain, counter);
		if (SUCCESS != status)
		{
		    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, 
                             SALVAGE_43, 
                             "Failed on sequential search of volume '%s'\n"),
			     domain.volumes[counter].volName);
		    if (NO_MEMORY == status)
		    {
		        exit(EXIT_FAILED);
		    }
		}
	    }
	} /* end for loop */

	status = build_cleanup(&domain);
	if (SUCCESS != status)
	{
	    exit(EXIT_FAILED);
	}

	D_SET_PASS3_COMPLETED(domain.status);

    } /* PASS3 needed */

    /*
     * Free the extent-to-lbn xreference tables - we don't need them
     * anymore.
     */
    for ( vol = 1 ; vol <= MAX_VOLUMES ; vol++ )
    {
        if ( domain.volumes[vol].volFd != -1 )
        {
	    if (domain.volumes->extentArraySize != 0)
	    {
		free(domain.volumes[vol].extentArray);
		domain.volumes[vol].extentArray = NULL;
		domain.volumes->extentArraySize = 0;
	    }
        }
    }

    for (fileset = domain.filesets; fileset != NULL; fileset = fileset->next)
    {
	/*
	 * Verify that the fileset has been named.
	 */
	status = validate_fileset(fileset);
	if (SUCCESS != status)
	{
            exit(EXIT_FAILED);
	}
    }

    /*
     * For each fileset, sort the extents lists for the tags, then insert the
     * filenames for the nodes in the tree. Then validate the tree 
     * structure, by scanning the tree, looking for and correcting certain 
     * inconsistencies.
     */
    writemsg(SV_ALL | SV_DATE, catgets(mcat, S_SALVAGE_1, SALVAGE_44, 
             "Starting file name loading:\n"));
    for (fileset = domain.filesets; fileset != NULL; fileset = fileset->next)
    {
        int reqPathFound;
        tagNumT tagNum;
        int status;
        filesetTagT *tag = NULL;

	if (FS_IS_NOT_RESTORE(fileset->status))
	{
	    continue;
	}

	writemsg(SV_NORMAL, catgets(mcat, S_SALVAGE_1, SALVAGE_45, 
                 "Loading file names from '%s'\n"), fileset->fsName);
        for (tagNum = 2; tagNum < fileset->tagArraySize; tagNum++)
        {
            tag = fileset->tagArray[tagNum];
        
            if ( tag == NULL || tag == IGNORE || tag == DEAD ) 
            {
                continue;
            }
            status = sort_and_fill_extents (tag);
        }
        
        status = insert_filenames (fileset, &reqPathFound);
        if ( status != SUCCESS )
        {
            writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_46, 
                     "Unable to name files from '%s'\n"),
		     fileset->fsName);
            exit(EXIT_FAILED);
        }

        if ( reqPathFound == REQUESTED_PATH_NOT_FOUND )
        {
            writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_47, 
		     "File(s) in requested pathname '%s' not found\n"),
		     Options.pathName);
            exit(EXIT_FAILED);
        }

        status = validate_tree( fileset );
        if ( status != SUCCESS )
        {
            writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_48, 
                     "Unable to validate '%s'\n"), fileset->fsName);
            exit(EXIT_FAILED);
        }
    }

    /*
     * We have now built up the tree of information.
     * It is now time to dump to disk (or tape) by fileset.
     */
    writemsg(SV_ALL | SV_DATE, catgets(mcat, S_SALVAGE_1, SALVAGE_49, 
             "Starting file system/snapshot recovery:\n"));
    for (fileset = domain.filesets; fileset != NULL; fileset = fileset->next) 
    {
	if (FS_IS_NOT_RESTORE(fileset->status))
	{
	    continue;
	}
		    
	writemsg(SV_NORMAL, catgets(mcat, S_SALVAGE_1, SALVAGE_50, 
                 "Starting recovery of '%s'\n"), fileset->fsName);
        if (SUCCESS != recover_fileset (&domain, fileset))
        {
            break;
        }
    }

    if (0 != Options.progressReport) {
	/*
	 * Add a line feed to the progress messages.
	 */
	writemsg(SV_ALL | SV_CONT, "\n");
    }
    
    switch (Options.outputFormat)
    {
	case F_DISK:
	    break;
	case F_TAR:
            write_eot();
	    close_archive();
	    break;
	default:
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_262,
		     "Invalid output format '%d'.\n"), Options.outputFormat);
	    break;
    }

#ifdef PROFILE
    PROF_RETURN(func);

    {
	int i, numprinted, curmax;
	for (i = 0 ; i < FUNC_NUMBER_FUNCTIONS ; i++) {
	    if (prof_currcalls[i] != 0) {
		writemsg(SV_ERR, "Bad profiling of %s - currcalls = %d - i = %d.\n",
			 prof_funcnames[i], prof_currcalls[i], i);
	    }
	    prof_endtime[i].tv_sec = prof_timeused[i] / 1000000;
	    prof_endtime[i].tv_usec = prof_timeused[i] % 1000000;
	}

	numprinted = 0;
	while (numprinted < FUNC_NUMBER_FUNCTIONS) {
	    curmax = 0;
	    for (i = 0 ; i < FUNC_NUMBER_FUNCTIONS ; i++) {
		if (prof_timeused[i] > prof_timeused[curmax]) {
		    curmax = i;
		}
	    }
	    writemsg(SV_ERR,	/* Don't worry about catgets on this */
		     "%7d.%06d seconds elapsed within %s\n",
		     prof_endtime[curmax].tv_sec,
		     prof_endtime[curmax].tv_usec,
		     prof_funcnames[curmax]);
	    prof_timeused[curmax] = -1;
	    numprinted++;
	}
    }
#endif /* PROFILE */

    /*
     * Close the log file
     */
    fclose (Options.logFD);

    exit(EXIT_SUCCESS);
}  /* end main */

/* end salvage.c */

