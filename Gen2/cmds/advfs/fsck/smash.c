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
 * *****************************************************************
 * *                                                               *
 * *  Copyright (c) 2002 Hewlett-Packard Development Company, L.P. *
 * *                                                               *
 * *****************************************************************
 * 
 * Facility:
 * 
 *      Advanced File System
 * 
 * Abstract:
 * 
 *      Help test fsck by "smashing" metadata on-disk structures to
 *      see if fsck can find and fix the smashed data.
 * 
 * Date:
 *      11-nov-03
 */

#include "fsck.h"

/* global protos for rec.c */

/* RBMT */

void smash_rbmt_header(bsMPgT *pRBMT);

void smash_attr(bsMPgT *pRBMT);
void smash_vd_attr(bsMPgT *pRBMT);
void smash_mattr(bsMPgT *pRBMT);
void smash_dmn_attr(bsMPgT *pRBMT);
void smash_dmn_trans_attr(bsMPgT *pRBMT);
void smash_dmn_freeze_attr(bsMPgT *pRBMT);
void smash_dmn_ss_attr(bsMPgT *pRBMT);
void smash_vd_io_params(bsMPgT *pRBMT);
void smash_def_del_mcell_list(bsMPgT *pRBMT);

void smash_xtnts(bsMPgT *pBMT, int testType);
void smash_xtra_xtnts(bsMPgT *pBMT, int testType);

/* BMT */

void smash_bmt_header(bsMPgT *pBMT);
void smash_bfs_attr(bsMPgT *pBMT);
void smash_bf_inherit_attr(bsMPgT *pBMT);
void smash_bfs_quota_attr(bsMPgT *pBMT);
void smash_bsr_proplist_head(bsMPgT *pBMT);
void smash_bsr_proplist_data(bsMPgT *pBMT);
void smash_bmtr_fs_dir_index_file(bsMPgT *pBMT);
void smash_bmtr_fs_index_file(bsMPgT *pBMT);
void smash_bmtr_fs_time(bsMPgT *pBMT);
void smash_bmtr_fs_undel_dir(bsMPgT *pBMT);
void smash_bmtr_fs_data(bsMPgT *pBMT);
void smash_bmtr_fs_stat(bsMPgT *pBMT);
void smash_bsr_mcell_free_list(bsMPgT *pBMT);
void smash_bsr_def_del_mcell_list(bsMPgT *pBMT);
void smash_bsr_acl(bsMPgT *pBMT);

/* local prototypes */

static void usage(void);
static int insert_errors();

static int superErr();
static int rbmtErr();
static int bmtErr();
#ifdef NOTYET
static int rootTagErr();
static int tagErr();
static int logErr();
static int sbmErr();
#endif

static int check_undo_dir(char *path, int makeDir);
static int update_disk(domainT *domain);

static void prepare_signal_handlers(void);
static void signal_handler(int sigNum);
static int want_abort(void);


/* command line option support */

struct _opts {
    int   super;          /* -t superblock */
    int   rbmt;           /* -t rbmt */
    int   bmt;            /* -t bmt */
    int   rootTag;        /* -t root */
    int   tag;            /* -t tag */
    int   sbm;            /* -t sbm */
    int   log;            /* -t log */
    int   verbose;        /* -v */
    int   dir;            /* -d */
    int   undo;           /* -u */
    char  undoDir[MAXPATHLEN+1];   /* undo file dir for undo/restore */
    int   volNum;
} opts;

#define OPTS "t:o:"

/*
 * Global
 */
nl_catd _m_catd;
cacheT *cache;

static void 
usage( void )
{
    char   *funcName = "usage";
    
    fprintf(stderr, 
	    "usage: %s [-o option[,option,...]] [-t test[,test...]] dmnName [volNum]\n",
	    Prog);
    fprintf(stderr, "\t     Options: verbose,debug,[msglog=<dir>|undo=<dir>]\n");
    fprintf(stderr, "\t     Tests: superblock,rbmt,bmt,root,tag,sbm\n");
    exit(1);

}  /* end usage */


/*
 * Function Name:  main (3.1)
 *
 * Description:
 *     This is the main function of the smash program.  It reads the
 *     command line and dispatches to routines that smash the various 
 *     ODS elements.
 *
 * Input parameters:
 *     argc - Argument Count.
 *     argv - Array of arguments.
 *
 */
main(int  argc, char *argv[])
{
    extern int	   getopt();
    extern int	   optind;
    int            status;
    int            i, t;
    char	   *keyword;
    int		   getoptValue;
    mcellNodeT	   *maxMcell;
    struct rlimit  rlimit;
    domainT	   domain;
    arg_infoT *fsInfo;

    /* store only the file name part of argv[0] */

    Prog = strrchr( argv[0], '/' );
    if (Prog == NULL) {
	Prog = argv[0];
    } else {
	Prog++;
    }

    if (argc < 2)
	usage();

    /*
     * Setup fsck message catalog.
     */
    (void) setlocale(LC_ALL, "");
    _m_catd = catopen(MF_FSCK, NL_CAT_LOCALE);

    /* 
     * check for root 
     */
    if (geteuid()) {
	writemsg(SV_ERR, "Permission denied - user must be root to run.\n");
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
	writemsg(SV_ERR, 
		 "setrlimit() failed to change memory usage limits.\n");
	perror(Prog);
	failed_exit();
    }

    /*
     * Initialize Variables
     */ 
    domain.dmnName        = NULL;
    domain.dmnId.id_sec   = 0;
    domain.dmnId.id_usec  = 0;
    domain.ODSVersion.odv_major = 0;
    domain.ODSVersion.odv_minor = 0;
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
#ifdef SNAPSHOTS
    domain.ddlClone       = NULL;
#endif /* SNAPSHOTS */
    domain.status         = 0;

    DMN_CLEAR_SBM_LOADED(domain.status);

    /* init our option flags */

    opts.super = 0;
    opts.rbmt = 0;
    opts.bmt = 0;
    opts.rootTag = 0;
    opts.tag = 0;
    opts.sbm = 0;
    opts.log = 0;
    opts.verbose = 0;
    opts.dir = 0;
    opts.undo = 0;
    opts.undoDir[0] = '\0';
    opts.volNum = 0;

    t = 0;

    /* init the fsck options.  We don't explicitly use these in this
       program (we have our own options), but some of the fsck
       utilities routines we link with require that they are set up,
       so we do that here. */

    Options.echoCmdline = FALSE;
    Options.verboseOutput = SV_NORMAL;
    Options.full = FALSE;
    Options.nolog = FALSE;
    Options.yes = FALSE;
    Options.no = FALSE;
    Options.activate = FALSE;
    Options.preActivate = FALSE;
    Options.nochange = FALSE;
    Options.preen = FALSE;
    Options.sync = FALSE;
    Options.undo = FALSE;
    Options.msglog = FALSE;
    Options.msglogHndl = NULL;
    Options.msglogFile = NULL;
    Options.filesetName = NULL;
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
     * Malloc and initialize the cache and Mcell list.
     */

    status = create_cache(&domain);
    if (status != SUCCESS) {
	writemsg(SV_ERR, "Error creating the data cache.\n");
	failed_exit();
    }

    maxMcell = (mcellNodeT *)ffd_malloc(sizeof(mcellNodeT));
    if (maxMcell == NULL) {
	writemsg(SV_ERR, "Can't allocate memory for maxMcell.\n");
	failed_exit();
    }

    /*
     * The key (mcellId) needs to be set, the rest of the maxMcell
     * structure can be garbage - this is the tail node of the skiplist.
     */

    maxMcell->mcellId.volume = MAX_VOLUMES;
    maxMcell->mcellId.page = MAXINT;
    maxMcell->mcellId.cell = BSPG_CELLS + 1;

    status = create_list(&compare_mcell_nodes, &(domain.mcellList), maxMcell);
    if (status != SUCCESS) {
	writemsg(SV_ERR, "Error creating the mcell list structures.\n");
	failed_exit();
    }

    /* 
     * Get user-specified command line arguments. 
     */

    while ((getoptValue = getopt( argc, argv, OPTS)) != EOF) {
	switch (getoptValue) {

	    case 't':

		t++;

		/*
		 * Parse the types
		 */
		keyword = strtok(optarg, ",");

		while (keyword != NULL) {
		    if (!strcmp(keyword,"superblock")) {
			opts.super++;
		    } else if (!strcmp(keyword,"super")) {
			opts.super++;
		    } else if (!strcmp(keyword,"rbmt")) {
			opts.rbmt++;
		    } else if (!strcmp(keyword,"bmt")) {
			opts.bmt++;
		    } else if (!strcmp(keyword,"root")) {
			opts.rootTag++;
		    } else if (!strcmp(keyword,"tag")) {
			opts.tag++;
		    } else if (!strcmp(keyword,"log")) {
			opts.log++;
		    } else if (!strcmp(keyword,"sbm")) {
			opts.sbm++;
		    } else {
			writemsg(SV_ERR, 
		      "Invalid argument '%s' specified with the -t option.\n",
				 keyword);
			usage();
		    }
		    keyword = strtok ((char *)NULL, ",");
		}

		break;

	    case 'o':

		/*
		 * Parse the types
		 */
		keyword = strtok(optarg, ",");

		while (keyword != NULL) {
		    if (!strcmp(keyword,"verbose")) {
			Options.verboseOutput = SV_VERBOSE;
		    } else if (!strcmp(keyword,"debug")) {
			Options.verboseOutput = SV_DEBUG;
		    } else if (!strncmp(keyword,"msglog=", 7)) {

			/* Place undo files in this location */
			opts.dir = TRUE;
			strcpy(opts.undoDir, &keyword[4]);

			if (opts.undoDir[strlen(opts.undoDir) - 1] == '/') {
			    opts.undoDir[strlen(opts.undoDir) - 1] = '\0';
			}

			strcat(opts.undoDir, "/smash_undo");

			writemsg(SV_VERBOSE, "undo files to go in %d\n", opts.undoDir);

			if (check_undo_dir(opts.undoDir, TRUE) != SUCCESS) {
			    usage();
			}

			Options.dirPath = opts.undoDir;

		    } else if (!strncmp(keyword,"undo=", 5)) {

			opts.undo = TRUE;
			strcpy(opts.undoDir, &keyword[5]);

			if (opts.undoDir[strlen(opts.undoDir) - 1] == '/') {
			    opts.undoDir[strlen(opts.undoDir) - 1] = '\0';
			}

			strcat(opts.undoDir, "/smash_undo");

			writemsg(SV_VERBOSE, "fetching undo files from %d\n", opts.undoDir);

			if (check_undo_dir(opts.undoDir, FALSE) != SUCCESS) {
			    usage();
			}

			Options.undo = TRUE;
			Options.dirPath = opts.undoDir;

		    } else {
			writemsg(SV_ERR, 
		      "Invalid argument '%s' specified with the -o option.\n",
				 keyword);
			usage();
		    }
		    keyword = strtok ((char *)NULL, ",");
		}

		break;

	default:
	    writemsg(SV_ERR, "unknown command arg '%s'\n", getoptValue);
	    usage();

	} /* end switch */
    } /* end while */

    if (t == 0 && opts.undo == FALSE) {
	writemsg(SV_ERR, "Missing '-t test' argument.\n");
	usage();
    }

    /* We now fetch the file system name and related volume info.  An
       FS can be specified by FS name, FS path, or special file
       (volume) path. */

    if (argv[optind] == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_25, 
			 "Missing file system name argument.\n"));
	usage();
    }


    if ((fsInfo = process_advfs_arg(argv[optind])) == 0) {
	writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FSCK_1, FSCK_20,
			  "Can't obtain file system info for '%s'.\n"),
		  argv[optind]);
	exit(EXIT_FAILED);
    }

    domain.dmnName = fsInfo->fsname;

    if (argv[++optind]) {
	if (strcmp(argv[optind-1], fsInfo->fsname) == 0) {
	    opts.volNum = atoi(argv[optind]);
	} else {
	    writemsg(SV_ERR, "invalid volume number.\n");
	    usage();
	}
    }

    /* check command line options for legal combinations, etc. */

    if (opts.undo && opts.dir) {
	writemsg(SV_ERR, "Can't specify to both 'log' and 'undo' directories.\n");
	usage();
    }

    /* 
     * If the directory option is not specified, we use the current
     * working directory for undo file creation.
     */

    if (!opts.dir && !opts.undo) {
	opts.dir = TRUE;
	strcpy(opts.undoDir, "./smash_undo");
	Options.dirPath = opts.undoDir;
	writemsg(SV_VERBOSE, 
		 "No '-o log=<directory>' specified, logging to \"%s\"\n",
		 opts.undoDir);

	if (check_undo_dir(opts.undoDir, TRUE) != SUCCESS) {
	    failed_exit();
	}
    } 

    /*
     * Parse /dev/advfs and get number of volumes and device names.
     */

    writemsg(SV_VERBOSE, "Gathering volume information.\n");

    status = get_domain_volumes(fsInfo, &domain);
    if (status != SUCCESS) {
	writemsg(SV_ERR, "Error obtaining volume information.\n");
	failed_exit();
    }

    /* 
     * Open the domain volumes and do restore if applicable.
     */

    writemsg(SV_VERBOSE, "Opening the volumes.\n");

    Options.undo = TRUE; /* skip superBlk correction in open_domain_volumes */

    if (open_domain_volumes(&domain) != SUCCESS) {
	writemsg(SV_ERR, "Error opening the volumes.\n");
	failed_exit();
    }

    Options.undo = opts.undo;

    if (opts.undo == TRUE) {
	writemsg(SV_VERBOSE, "Restoring domain from the undo files.\n");

	if (restore_undo_files(&domain) != SUCCESS) {
	    writemsg(SV_ERR, "Error restoring undo files.\n");
	    failed_exit();
	}

	writemsg(SV_VERBOSE, "Restore from undo files completed.\n");

	/* 
	 * Don't use clean_exit - it will write undo files and 
	 * activate dmn. 
	 */
	exit(EXIT_SUCCESS);
    }

    /* 
     * Check domain/volume attributes and sync them across volumes. 
     * This needs to be called before load_subsystem_extents.
     */

    writemsg(SV_VERBOSE,
	     catgets(_m_catd, S_FSCK_1, FSCK_40, 
		     "Checking the RBMT(s).\n"));
    status = correct_domain_rbmt (&domain);
    if (status != SUCCESS) {
	if (status == FIXED) {
	    writemsg(SV_VERBOSE, 
		     catgets(_m_catd, S_FSCK_1, FSCK_41,
			     "Corrected problems in the RBMT.\n"));
	    prompt_user_to_continue(&domain, 
				    catgets(_m_catd, S_FSCK_1, FSCK_42, 
					    "Fixed errors in the RBMT.\n"));
	} else {
	    writemsg(SV_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_43, 
			     "Error correcting the RBMT.\n"));
	    failed_exit();
	}
    }

    /*
     * load the extent to LBN mapping arrays for the metadata
     * structures stored in the RBMT.
     */

    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_44, 
		     "Loading subsystem extents.\n"));
    status = load_subsystem_extents(&domain);
    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_45, 
			 "Error collecting subsystem extents.\n"));
	failed_exit();
    }

    /* smash the metadata... */

    if (insert_errors(&domain) != SUCCESS) {
	writemsg(SV_ERR, "failed to smash, aborting...\n");
	failed_exit();
    }

    /* save the existing metadata and writes the cache to disk */

    exit(update_disk(&domain));

}  /* end main */

/*
 * Function Name: insert_errors
 *
 * Description: This routine inserts errors in the AdvFS Metadata ODS
 *              as specified by the command line.
 *
 */

static int 
insert_errors(domainT *domain)
{
    if (opts.super) {
	if (superErr(domain) != SUCCESS) {
	    return (FAILURE);
	}
    }

    if (opts.rbmt) {
	if (rbmtErr(domain) != SUCCESS) {
	    return (FAILURE);
	}
    }

    if (opts.bmt) {
	if (bmtErr(domain) != SUCCESS) {
	    return (FAILURE);
	}
    }

#ifdef NOTYET
    if (opts.rootTagDir) {
	if (rootTagErr(domain) != SUCCESS) {
	    return (FAILURE);
	}
    }

    if (opts.tagDir) {
	if (tagErr(domain) != SUCCESS) {
	    return (FAILURE);
	}
    }
    if (opts.log) {
	if (logErr(domain) != SUCCESS) {
	    return (FAILURE);
	}
    }

    if (opts.sbm) {
	if (sbmErr(domain) != SUCCESS) {
	    return (FAILURE);
	}
    }
#endif /* NOTYET */

    return (SUCCESS);

} /* insert_errors */

/*
 * Function Name: superErr
 *
 * Description: corrupt the fake superblock
 *
 */

static int
superErr(domainT *domain)
{
    int	     status;
    pageT    *page;
    volNumT  volNum;
    advfs_fake_superblock_t *superBlk;

    writemsg(SV_VERBOSE, "Smashing the fake superBlock\n");

    for (opts.volNum ? volNum = opts.volNum : volNum = 1; 
	 volNum <= domain->highVolNum; 
	 volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	/* Smash the fake superblock */

	status = read_page_from_cache(volNum, ADVFS_FAKE_SB_BLK, &page);

	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_326, 
			     "Failed to read the super-block on volume %s.\n"), 
		     domain->volumes[volNum].volName);
	    return FAILURE;
	}

	superBlk = (advfs_fake_superblock_t *) page->pageBuf;
	superBlk->adv_fs_magic = 12345678;
	superBlk->adv_ods_version.odv_major = 1;
	superBlk->adv_ods_version.odv_minor = 5;

	/* release the fixed superblock back to the cache */

	if (release_page_to_cache(volNum, ADVFS_FAKE_SB_BLK, page, 
				  TRUE, FALSE) != SUCCESS) {
	    writemsg(SV_ERR, "Failed to release doctored superblock\n");
	    return(FAILURE);
	}

	if (opts.volNum)
	    break;
    }

    return(SUCCESS);

} /* superErr */

/*
 * Function Name: rbmtErr
 *
 * Description: corrupt the RBMT
 *
 */

static int
rbmtErr(domainT *domain)
{
    int	     status;
    pageT    *page;
    volNumT  volNum;
    bsMPgT   *pRBMT;

    writemsg(SV_VERBOSE, "Smashing the RBMT\n");

    /* if the user specifies a volume, do that volume only, othewise,
       do 'em all. */

    for (opts.volNum ? volNum = opts.volNum : volNum = 1; 
	 volNum <= domain->highVolNum; 
	 volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	if (opts.volNum && opts.volNum != volNum) {
	    continue;
	}

	status = read_page_from_cache(volNum, RBMT_START_BLK, &page);

	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_327, 
			     "Failed to read RBMT on volume %s.\n"), 
		     domain->volumes[volNum].volName);
	    return FAILURE;
	}

	pRBMT = (bsMPgT *) page->pageBuf;

	smash_rbmt_header(pRBMT);

	/* 
	 * Smash the various RBMT records.  We'd like to smash the
	 * mcell headers too but the only thing they contain is the
	 * type and bCnt and we need both of those things for the
	 * record smashing routines to work.
	 */

	smash_attr(pRBMT);
	smash_vd_attr(pRBMT);
	smash_dmn_attr(pRBMT);
#ifdef NOTNOW
	smash_dmn_mattr(pRBMT);
	smash_dmn_trans_attr(pRBMT);
	smash_dmn_freeze_attr(pRBMT);
	smash_dmn_ss_attr(pRBMT);
	smash_vd_io_params(pRBMT);
	smash_def_del_mcell_list(pRBMT);
	smash_xtnts(pRBMT, 3);
	smash_xtra_xtnts(pRBMT, 3);
#endif /* NOTNOW */

	/* release the smashed RBMT back to the cache */

	if (release_page_to_cache(volNum, RBMT_START_BLK, page, 
				  TRUE, FALSE) != SUCCESS) {
	    writemsg(SV_ERR, "Failed to release doctored RBMT\n");
	    return(FAILURE);
	}

	if (opts.volNum)
	    break;
    }

    return(SUCCESS);

} /* rbmtErr */

/*
 * Function Name: bmtErr
 *
 * Description: corrupt the BMT
 *
 */

static int
bmtErr(domainT *domain)
{
    int	     status;
    pageT    *page;
    volNumT  volNum;
    bsMPgT   *pRBMT, *pBMT;
    fobtoLBNT *extents;
    xtntNumT  numExtents;
    xtntNumT  extentCnt;
    volNumT   currVol;
    lbnNumT   currLBN;
    pageNumT  pageNum;

    writemsg(SV_VERBOSE, "Smashing the BMT\n");

    /* loop through the volumes */

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

        if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	/* if the user specified a volume, skip all but that one */

	if (opts.volNum && opts.volNum != volNum) {
	    continue;
	}

	/* 
	 * loop through all the pages in this volume's BMT 
	 */

	extents = domain->volumes[volNum].volRbmt->bmtLBN;
	numExtents  = domain->volumes[volNum].volRbmt->bmtLBNSize;

	for (extentCnt = 0; extentCnt < numExtents; extentCnt++) {

	    /* loop through each page in the current extent */

	    for (pageNum = FOB2PAGE(extents[extentCnt].fob);
		 pageNum < FOB2PAGE(extents[extentCnt+1].fob);
		 pageNum++) {

		/* get the page's starting LBN */

		status = convert_page_to_lbn(extents, numExtents, pageNum,
					     &currLBN, &currVol);

		if (status != SUCCESS) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FSCK_1, FSCK_128, 
				     "Can't convert BMT page %ld to LBN.\n"), 
			     pageNum);
		    return FAILURE;
		}

		status = read_page_from_cache(currVol, currLBN, &page);

		if (status != SUCCESS) {
		    writemsg(SV_ERR,
			     "Failed to read BMT page %ld from volume %s.\n", 
			     pageNum, domain->volumes[volNum].volName);
		    return FAILURE;
		}

		pBMT = (bsMPgT *) page->pageBuf;

		/* smash the BMT */

#ifdef NOTNOW
		smash_bmt_header(pBMT);

		/* 
		 * Smash the various BMT records.  We'd sorta like to smash
		 * the mcell headers too but the only thing they contain is
		 * the type and bCnt and we need both of those things for the
		 * record smashing routines to work.
		 */

		smash_attr(pBMT);
		smash_bfs_attr(pBMT);
		smash_bf_inherit_attr(pBMT);
		smash_bfs_quota_attr(pBMT);
		smash_bmtr_fs_dir_index_file(pBMT);
		smash_bmtr_fs_index_file(pBMT);
		smash_bmtr_fs_time(pBMT);
		smash_bmtr_fs_undel_dir(pBMT);
		smash_bmtr_fs_data(pBMT);
		smash_bmtr_fs_stat(pBMT);
		smash_bsr_mcell_free_list(pBMT);
		smash_bsr_def_del_mcell_list(pBMT);

		smash_xtra_xtnts(pBMT, 3);

		/* corrupt a small subset of pages */

		if (pageNum == 1 || !(pageNum % 12)) {
		    smash_xtnts(pBMT, 1);
		}

#endif /* NOTNOW */
		smash_bsr_acl(pBMT);

		/* release the smashed BMT back to the cache */

		if (release_page_to_cache(currVol, currLBN, page, 
					  TRUE, FALSE) != SUCCESS) {
		    writemsg(SV_ERR, 
			     "Failed to release doctored BMT page %ld on volume %s\n",
			     pageNum, domain->volumes[volNum].volName);
		    return(FAILURE);
		}

		writemsg(SV_DEBUG, "Done: BMT page %ld, vol %d\n", 
			 pageNum, volNum);

	    } /* end loop: pages within this extent */
	} /* end loop: extents on this volume */
    } /* end loop: volumes in this cdomain */

    return(SUCCESS);

} /* bmtErr */


/*
 * Function Name: check_undo_dir
 *
 * Description: Look to see if a directory exists, and if it doesn't, 
 *              create it.
 *
 */

static int
check_undo_dir(char *path,
	       int makeDir)
{
    struct stat	   statBuf;

    if ((strlen(path) + strlen(Prog) + BS_DOMAIN_NAME_SZ +
	 strlen(".undo")) >= MAXPATHLEN) {

	writemsg(SV_ERR, "Undo directory path '%s' is too long.\n", path);
	return (FAILURE);
    }

    /*
     * If the directory doesn't exist, create it.
     */

    if (stat(path, &statBuf) == -1) {
	if (errno == ENOENT) {
	    if (makeDir) {
		writemsg(SV_VERBOSE,
			 "directory '%s' doesn't exist, creating it.\n", 
			 path);

		if (mkdir(path, S_IRWXU)) {
		    writemsg(SV_ERR, "Can't create '%s': ", path);
		    perror(Prog);
		    return (FAILURE);
		}
	    } else {		
		writemsg(SV_ERR, "directory '%s' does not exist.\n", path);
		return (FAILURE);
	    }
	} else {
	    writemsg(SV_ERR, "stat() failed on directory path '%s'.\n", 
		     path);
	    perror(Prog);
	    return (FAILURE);
	}
    } else {
	/* 
	 * Verify that the path given is a directory.
	 */
	if (S_ISDIR(statBuf.st_mode) == FALSE) {
	    writemsg(SV_ERR, "Undo directory path '%s' is not a directory.\n",
		     path);
	    return (FAILURE);
	}
    }

    return (SUCCESS);

} /* check_undo_dir */


/*
 * Function Name: update_disk
 *
 * Description: save the untouched metadata to the side in the undo
 *              directory and flush the cache to disk.
 *
 */

static int
update_disk(domainT *domain)
{

    if (create_undo_files(domain) != SUCCESS) {
 	writemsg(SV_ERR, 
		 "Undo files were not created, aborting...\n");
	return (FAILURE);
    }

    if (write_cache_to_disk() != SUCCESS) {
 	writemsg(SV_ERR, 
		 "Can't flush cache; restoring Undo files, aborting...\n");
	restore_undo_files(domain);
	return (FAILURE);
    }

    return (SUCCESS);

} /* update_disk */


/********************************************************************
 ********************************************************************
 ********************************************************************
 *
 * Signal Handler Routines
 *
 ********************************************************************
 ********************************************************************
 *******************************************************************/

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
    intAction.sa_flags	 = 0;
    
    /*
     * Call sigaction using intAction for SIGINT, SIGQUIT, and SIGTERM.
     */
    sigaction (SIGINT, &intAction, NULL);
    sigaction (SIGQUIT, &intAction, NULL);
    sigaction (SIGTERM, &intAction, NULL);
    
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
 * Description: This routine asks the user if they want to fsck when
 *    a SIGINT or SIGQUIT interrupt is received. If so, we exit,
 *    otherwise, we reset the signal handler and return. If we receive
 *    a SIGTERM interrupt, then we exit.
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
    int	   abort;
    struct sigaction newInt;  /* new interrupt action structure	    */
    struct sigaction oldInt;  /* old interrupt action structure	    */
    
    /*
     * Set up newInt to ignore signals.
     */
    newInt.sa_handler = SIG_IGN;
    memset(&newInt.sa_mask, 0, sizeof(sigset_t));
    newInt.sa_flags   = 0;

    if ((sigNum == SIGINT) || (sigNum == SIGQUIT)) {
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
	 * Call want_abort to see if user wants to abort.
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
  
    if (abort == TRUE) {
	failed_exit();
    }
} /* end signal_handler */


/*
 * Function Name:  want_abort (3.4)
 *
 * Description:	 This routine prompts the user to see if they really want
 *		 to abort.  If stdin is being used for input, or the
 *		 user cannot respond, then this routine will return
 *		 TRUE.  This routine may be called from threads other
 *		 than the signal_thread to prompt user for abort.
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

/* end smash.c */
