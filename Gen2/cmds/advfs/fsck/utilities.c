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
 * Global Variables
 */
extern cacheT *cache;
extern nl_catd _m_catd;


static int32_t dmnLock = 0;  /* lock for specific domain */

/*
 * Local static prototypes
 */

static int get_volume_info(domainT *domain, 
			   volNumT index,
			   volNumT *volNumber,
			   lbnNumT *volSize);

static int merge_snap_extents(fobtoLBNT *origXtntArray, 
			      xtntNumT  origXtntArraySize,
			      fobtoLBNT *snapXtntArray, 
			      xtntNumT  snapXtntArraySize,
			      fobtoLBNT **newXtntArray, 
			      xtntNumT  *newXtntArraySize);

static void writelog_real(int  severity, 
			  char *fmt);


/*
 * writemsg
 *
 * Description
 *  This function writes a message to stdout, using vfprintf (i.e., variable
 *  argument list), according to a specified "severity level". The message 
 *  is formatted to include a leading program name, and an optional datetime 
 *  string if requested.
 *
 * Input parameters:
 *  severity: Code determining whether message will be printed under current
 *            verbosity level.
 *  fmt: Format string for message, in "printf" style.
 *  ... : Variable length list of arguments, correspoding to format string.
 *
 */
void 
writemsg(int severity, 
	 char *fmt, 
	 ... )
{
    char *funcName = "writemsg";
    va_list args;
    char dateStr[32];
    char msg[MAXERRMSG];
    char printStr[MAXERRMSG * 2];
    int printDate;
    int isStderr;
    int isCont;
    int isLog;
    int i;

    /*
     * Initialize variables.
     */
    printDate = (severity & SV_DATE) > 0 ? TRUE : FALSE;
    isStderr  = (severity & SV_ERR)  > 0 ? TRUE : FALSE;
    isCont    = (severity & SV_CONT) > 0 ? TRUE : FALSE;
    isLog     = (severity & SV_LOG_MASK) > 0 ? TRUE : FALSE;

    /* format the arg list before messing with format */

    va_start(args, fmt);

    /*
     * Does this message also go to the log?
     *
     * NOTE: each new call to cagets() re-uses the catgets() string
     * buffer.  If the fmt arg passed in above was returned from
     * catgets (which it normally is), it is pointing to a catgets()
     * internal buffer.  Calls to catgets() in writelog_real, signal
     * handling, etc, will destroy the fmt string passed in so we
     * process the string now with vsprint and save it aside for later
     * printing.
     *
     * This appears to not be a problem for the IA architecture, which
     * has apparently implemented catgets() differently.
     */

    vsprintf(msg, fmt, args);

    if (isLog == TRUE) {
	/* call writelog_real instead of writelog cuz we already have
	   the args formatted */ 
	writelog_real(severity, msg);
    }

    /*
     * If the severity code indicates this message should not be printed,
     * get out.
     */
    if ((isStderr == FALSE) &&  
	((severity & SV_MASK) > Options.verboseOutput)) {
	va_end(args);
	return;
    }

   /*
     * If date was requested, get it, and increment the required length of
     * the new format length.
     */
    if (TRUE == printDate) {
        get_timestamp( dateStr, sizeof(dateStr) );
    }

    /*
     * If appropriate, put the Prog name at the beginning.
     */

    if (isCont == FALSE) {
        strcpy(printStr, Prog);
        strcat(printStr, ": ");
    } else {
	strcpy(printStr, "  ");
	for (i = 0 ; i < strlen(Prog) ; i++) {
	    strcat(printStr, " ");
	}
    }

    /*
     * If date was requested, add it on.
     */
    if (printDate == TRUE) {
        if ('\n' == printStr[strlen(printStr)-1]) {
	    if ('.' == printStr[strlen(printStr)-2]) {
		printStr[strlen(printStr)-2] = '\0'; /* end of string */
		strcat( printStr, catgets(_m_catd, S_FSCK_1, FSCK_722,
					" at ") );
		strcat( printStr, dateStr );
		strcat( printStr, ".\n" );
	    } else {
		printStr[strlen(printStr)-1] = '\0'; /* end of string */
		strcat( printStr, catgets(_m_catd, S_FSCK_1, FSCK_722,
					" at ") );
		strcat( printStr, dateStr );
		strcat( printStr, "\n" );
	    }
        } else {
	    if ('.' == printStr[strlen(printStr)-1]) {
		printStr[strlen(printStr)-1] = '\0'; /* end of string */
		strcat( printStr, catgets(_m_catd, S_FSCK_1, FSCK_722,
					" at ") );
		strcat( printStr, dateStr );
		strcat( printStr, "." );
	    } else {
		strcat( printStr, catgets(_m_catd, S_FSCK_1, FSCK_722,
					" at ") );
		strcat( printStr, dateStr );
	    }
        }
    }

    /*
     * Add in the old format.
     */

    strcat(printStr, msg);

    /*
     * Print the message.
     */

    if (isStderr == FALSE) {
        vfprintf(stdout, printStr, args);
    } else {
        vfprintf(stderr, printStr, args);
    }

    va_end(args);

} /* end writemsg */


/*
 * writelog
 *
 * Description
 *  This is a wrapper function to the call 'writelog_real'.  It was 
 *  needed so we could call this function from writemsg, it converts
 *  a variable number of arguments into a va_list struct.
 *
 * Input parameters:
 *  severity: Code determining what type of log message it is.
 *  fmt: Format string for message, in "printf" style.
 *  ... : Variable length list of arguments, correspoding to format string.
 *
 */
void writelog(int severity, char *fmt, ...)
{
    char *funcName = "writelog";
    char msg[MAXERRMSG * 2];
    va_list args;

    va_start(args, fmt);
    vsprintf(msg, fmt, args);
    writelog_real(severity, msg);
    va_end(args);

} /* end writelog */


/*
 * writelog_real
 *
 * Description
 *  This function writes a message to the log file, using vfprintf 
 *  (i.e., variable argument list).  The message is formatted to 
 *  include the time and "severity" level.
 *
 * Input parameters:
 *  severity: Code determining what type of log message it is.
 *  fmt: Format string for message, in "printf" style.
 *  args: Variable length list of arguments, correspoding to format string.
 *
 */
static void 
writelog_real(int severity, 
	      char *msg)
{
    char *funcName = "writelog_real";
    int  messageType;
    char dateStr[32];
    FILE *logHandle;

    if (Options.msglogHndl == NULL) {
	/*
	 * The log is not open so, just return.
	 */
	return;
    }

    /*
     * Compute the message type.
     */
    messageType = severity & SV_LOG_MASK;

    /*
     * Only output SV_LOG_DEBUG messages if the verbosity is set to debug
     */
    if (messageType == SV_LOG_DEBUG) {
	if (SV_DEBUG != Options.verboseOutput) {
	    return;
	}
    }

    /*
     * Initialize variables.
     */
    logHandle = Options.msglogHndl;

    /*
     * Output the time.
     */
    if (get_time(dateStr, sizeof(dateStr)) == NULL) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_723, 
			 "Can't get the time to put in the log.\n"));
	fprintf(logHandle, catgets(_m_catd, S_FSCK_1, FSCK_724,
				   "00:00:00"));
    } else {
	fprintf(logHandle, dateStr);
    }
    fprintf(logHandle, " | ");

    /*
     * Output the message type.
     */
    switch (messageType) {
	case (SV_LOG_INFO):
	    fprintf(logHandle, catgets(_m_catd, S_FSCK_1, FSCK_725, 
				       "INFO"));
	    break;	

	case (SV_LOG_SUM):
	    fprintf(logHandle, catgets(_m_catd, S_FSCK_1, FSCK_726,
				       "SUMMARY"));
	    break;
	    
	case (SV_LOG_FOUND):
	    fprintf(logHandle, catgets(_m_catd, S_FSCK_1, FSCK_727, 
				       "FOUND"));
	    break;

	case (SV_LOG_FIXED):
	    if (FALSE == Options.nochange) {		  
		fprintf(logHandle, 
			catgets(_m_catd, S_FSCK_1, FSCK_728, 
				"FIXED"));
	    } else {
		fprintf(logHandle, 
			catgets(_m_catd, S_FSCK_1, FSCK_729,
				"DISABLED"));
	    }
	    break;

	case (SV_LOG_WARN):
	    fprintf(logHandle, catgets(_m_catd, S_FSCK_1, FSCK_730,
				       "WARNING"));
	    break;

	case (SV_LOG_ERR):
	    fprintf(logHandle, catgets(_m_catd, S_FSCK_1, FSCK_731,
				       "ERROR"));
	    break;

	case (SV_LOG_DEBUG):
	    fprintf(logHandle,  catgets(_m_catd, S_FSCK_1, FSCK_910,
					"DEBUG"));
	    break;

	default:
	    fprintf(logHandle, catgets(_m_catd, S_FSCK_1, FSCK_732,
				       "UNKNOWN"));
	    break;
    } /* end switch */

    fprintf(logHandle, " | ");

    /*
     * Print out the message
     */

    fprintf(logHandle, msg);

} /* end writelog_real */


/*
 * get_timestamp
 *
 * Description
 *  This function returns an ASCII date string representing the current
 *  date-time.
 *  NOTE:
 *    Currently this version is a quick placeholder, with no
 *    internationalization implemented.
 *
 * Input parameters:
 *  dateStr: Input date character string.
 *  maxLen: Maximum length for dateStr buffer.
 *
 * Returns:
 *  Pointer to dateStr, or NULL if error in strftime().
 */
char * get_timestamp(char *dateStr, size_t maxLen)
{
    char *funcName = "get_timestamp";
    time_t t;
    struct tm dateTime;
    size_t retChars;
        
    t = time(0);
    localtime_r(&t, &dateTime); /* Thread Safe */
    /*    dateTime = localtime_r(&t, &result); */ /* Thread Safe */
    retChars = strftime ( dateStr, maxLen, 
			  catgets(_m_catd, S_FSCK_1, FSCK_733, 
				  "%d-%b-%Y %T"), &dateTime);

    if ( retChars > 0 ) {
        return dateStr;
    } else {
        return NULL;
    }
} /* end get_timestamp */


/*
 * get_time
 *
 * Description
 *  This function returns an ASCII string representing the current time.
 *  NOTE:
 *    Currently this version is a quick placeholder, with no
 *    internationalization implemented.
 *
 * Input parameters:
 *  dateStr: Input date character string.
 *  maxLen: Maximum length for dateStr buffer.
 *
 * Returns:
 *  Pointer to dateStr, or NULL if error in strftime().
 */
char * 
get_time(char *dateStr, 
	 size_t maxLen)
{
    char *funcName = "get_time";
    time_t t;
    struct tm dateTime;
    size_t retChars;
        
    t = time(0);
    localtime_r(&t, &dateTime); /* Thread Safe */
    retChars = strftime ( dateStr, maxLen, "%T", &dateTime );

    if ( retChars > 0 ) {
        return dateStr;
    } else {
        return NULL;
    }
} /* end get_time */


/*
 * Function Name: prompt_user
 *
 * Description: A generic call which prompts the user for input
 *              and returns the input to the caller.  User must
 *              pass in a buffer to store the message
 *
 * Input parameters:
 *     size   : The size of the buffer.
 *
 * Output parameters: N/A
 *     answer : The string entered by the user.
 *
 * Returns: SUCCESS, FAILURE
 *
 */
int 
prompt_user(char *answer, 
	    int size)
{
    char *funcName = "prompt_user";
    int  valid;

    valid = 0;

    if (!Localtty) { 
        /* can't prompt user; no local tty */
	writemsg(SV_ERR | SV_CONT, "\n");
	writemsg(SV_ERR, catgets(_m_catd, S_FSCK_1, FSCK_734, 
				 "Can't prompt user for answer.\n"));
        return FAILURE;
    }

    while (0 == valid ) {
        /*
	 * We need an answer from the user.  If there is no way to
	 * get one (Localtty is NULL), then we should have returned above.  
	 */
        if (NULL != fgets (answer, size, Localtty)) {
	    valid = 1;
	}
    } 

    return SUCCESS;
} /* end prompt_user */


/*
 * Function Name: prompt_user_yes_no
 *
 * Description: A generic call which prompts the user for a yes
 *              or no and returns the input to the caller.  User must
 *              pass in a buffer to store the message.  The string
 *		returned will be either "y" or "n".
 *
 * Input parameters:
 *     defaultAns : The default answer if unable to obtain one from the 
 *                  user.  If NULL, this routine will keep prompting until
 *                  a valid answer is given.
 *
 * Output parameters: N/A
 *     answer : The string entered by the user.
 *
 * Returns: SUCCESS, FAILURE
 *
 */
int 
prompt_user_yes_no(char *answer, 
		   char *defaultAns)
{
    char *funcName = "prompt_user_yes_no";
    char buffer[10];
    int  valid;
    int  status;

    valid = FALSE;

    /* check for user specified default answers */

    if (Options.yes || Options.no) {
	if (Options.yes ==  1) {
	    strcpy(answer, "y");
	} else {
	    strcpy(answer, "n");
	}
	writemsg(SV_NORMAL | SV_CONT | SV_LOG_INFO, "\n");
	writemsg(SV_NORMAL | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_735, 
			 "Using default answer of '%s' as specified on the command line.\n"), 
		 answer);
	return SUCCESS;
    }

    do {
	status = prompt_user(buffer, 10);
	if (status != SUCCESS) {
	    if (NULL != defaultAns) {
		answer[0] = defaultAns[0];
		valid = TRUE;
	    }
	    else {
		return status;
	    }
	}
	if ('y' == buffer[0] || 'n' == buffer[0] ||
	    'Y' == buffer[0] || 'N' == buffer[0]) {
	    answer[0] = tolower(buffer[0]);
	    valid = TRUE;
	}
	else if ('\n' == buffer[0] && defaultAns != NULL) {
	    answer[0] = defaultAns[0];
	    valid = TRUE;
	}
	else {
	    writemsg(SV_ERR | SV_CONT, 
		     catgets(_m_catd, S_FSCK_1, FSCK_736, 
			     "Please enter 'y' or 'n':  "));
	}
    } while (FALSE == valid);

    return SUCCESS;

} /* end prompt_user_yes_no */


/*
 * Function Name: prompt_user_to_continue
 *
 * Description: This prompt is called from main.c after a new round of
 *              checking finds corruption.  This is handy n the event
 *              that ongoing checks cause an fsck crash.  In that
 *              case, the user runs fsck again and bails at the prompt
 *              before the crash to save whatever changes we have.
 *
 * Input parameters:
 *     message: The message presented to user
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS
 *
 */
int 
prompt_user_to_continue (domainT *domain, 
			 char *message)
{
    char *funcName = "prompt_user_to_continue";
    int  cont;
    int  status;
    char answer[10];

     /*
      * Due to the extremely high probablity of cascading corruptions,
      * it has been decided to remove this prompt and effectively make
      * this a NULL function.  The idea is that not continuing can get
      * the user into more trouble than continuing onward to try to
      * fix the cascading corruptions.  It's not definitively clear
      * whether this is the right thing to do, so we'll keep the code
      * around in case we get severe customer complaint about our not
      * giving them enough rope to hang themselves and their horses
      * with.
      */
     return SUCCESS;
 
#ifdef NOT_ANY_MORE
    cont = FALSE;
    if (NULL != message) {
	writemsg(SV_ERR, message);
    }

    /* -p (preen) option: we've been told to fix minor problems
        without user interaction, so queries to continue are
        skipped. */

    if (Options.preen) {
	writemsg(SV_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_975, 
			 "looking for more corruption (-p option skips user prompt)\n"));

	return (SUCCESS);
    }

    /* if the user has specified an explicit 'yes' or 'no' on the
       command line, we are running non-interactive so we skip the
       "press onward?" prompt issued below. Also, if the user is
       running a verify pass we skip the prompt since we're not going
       to be saving the metadata anyway. */ 

    if (Options.nochange || Options.yes || Options.no) {
	return (SUCCESS);
    }

    /*
     * Perform a check to see if user wishes to continue.
     */
    writemsg(SV_ERR, 
	     catgets(_m_catd, S_FSCK_1, FSCK_737, 
		     "Do you want %s to keep looking for more corruption? ([y]/n)  "),
	     Prog);

    status = prompt_user_yes_no(answer, "y");
    if (status != SUCCESS) {
	writemsg(SV_ERR, catgets(_m_catd, S_FSCK_1, FSCK_738,
				 "Continuing %s.\n"), Prog);
	cont = TRUE;
    }

    if ('y' == answer[0]) {
	cont = TRUE;
    }

    /* 
     * If the user wants to terminate fsck:
     */
    if (cont == FALSE) {
	/*
	 * Call clean_exit to cleanly exit the program.
	 */
	finalize_fs(domain);
	exit (EXIT_SUCCESS);
    }

    return SUCCESS;

#endif /* NOT_ANY_MORE */

} /* end prompt_user_to_continue */


/*
 * Function Name: create_volumes (3.n)
 *
 * Description:
 *  Malloc and initialize volumes array 
 *
 * Output parameters:
 *  volumes: A pointer to a malloced volumeT structure. 
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY.
 */
int 
create_volumes(volumeT **volumes)
{
    char  *funcName = "create_volumes";
    volNumT counter;

    /* 
     * Malloc new structs 
     */
    *volumes = (volumeT *)ffd_calloc(MAX_VOLUMES, sizeof(volumeT));
    if (*volumes == NULL) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_740, 
			 "volume array"));
        return NO_MEMORY;
    }

    /* 
     * Initialize structure 
     */
    for (counter = 0; counter < MAX_VOLUMES; counter++) {
        (*volumes)[counter].volFD            = -1;
        (*volumes)[counter].volName          = NULL;
        (*volumes)[counter].volSize          = 0;
        (*volumes)[counter].volRbmt          = NULL;
        (*volumes)[counter].mountId.id_sec   = 0;
        (*volumes)[counter].mountId.id_usec  = 0;
        (*volumes)[counter].mntStatus        = 0;
        (*volumes)[counter].rootTag          = 0;
	(*volumes)[counter].volSbmInfo	     = NULL;
        (*volumes)[counter].vdCookie         = 0;
	memset(&((*volumes)[counter].super), NULL, 
	       sizeof(advfs_fake_superblock_t));
    } /* end for loop */

    return SUCCESS;
} /* end create_volumes */


/*
 * Function Name: get_domain_volumes
 *
 * Description: Parse /dev/advfs/domain_name to collect all the volumes in 
 *              the domain. There can be only one thread running when this
 *              routine is called.  This routine also locks the domain to
 *		prevent other AdvFS utilities from modifying it.
 *
 * Input parameters:
 *    domainName: The name of the domain from which to find volumes.
 *
 * Output parameters:
 *    domain: Used to store information about the domain's volume(s).
 *
 * Returns: SUCCESS, FAILURE
 *
 */
int 
get_domain_volumes(arg_infoT *fsInfo, 
		   domainT *domain)
{
    char *funcName = "get_domain_volumes";
    DIR           *stgDir;
    struct dirent *dp;
    volumeT       *volumes;
    volNumT       counter;
    int           err;
    int           status;
    struct stat   statBuf;
    char          dirName[MAXPATHLEN+1];
    char          volName[MAXPATHLEN+1];
    char          *devType;
    char          *mountDir;
    off_t         volSize;
    getDevStatusT devRet;
    char          fsetPath[MAXPATHLEN+1];
    adv_mnton_t   *mntArray;
    int i;

    /*
     * Init variables
     */
    counter = 0;
    devType = NULL;

    /*
     * Grab the lock for this domain. It will be released when fsck
     * exits.
     */   

    dmnLock = advfs_fspath_lock(fsInfo->fspath, FALSE, TRUE, &err);

    if (dmnLock < 0) {
        if (EWOULDBLOCK == err) {
            writemsg (SV_ERR | SV_LOG_ERR, 
		      catgets(_m_catd, S_FSCK_1, FSCK_744, 
			      "Cannot execute. Another AdvFS command is currently claiming\n"));
	    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
		      catgets(_m_catd, S_FSCK_1, FSCK_745,
			      "exclusive use of the specified domain.\n"));
        } else {
            writemsg (SV_ERR | SV_LOG_ERR, 
		      catgets(_m_catd, S_FSCK_1, FSCK_746, 
			      "Can't obtain AdvFS domain lock on '%s'.\n"),
		      fsInfo->fspath);
	    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT, 
		      catgets(_m_catd, S_FSCK_1, FSCK_743, 
			      "Error = [%d] %s\n"), err, ERRMSG(err));
        }
        return FAILURE;
    } 
    
    /*
     * Advfs should not be mounted; bail if it is.
     */

    status = advfs_get_all_mounted(fsInfo, &mntArray);

    if (status == -1) {
	writemsg (SV_ERR | SV_LOG_ERR | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_993, 
			  "'%s' mount check failed.\n"));
	return FAILURE;
    }

    if (status > 0) {
	writemsg (SV_ERR | SV_LOG_ERR,
		  catgets(_m_catd, S_FSCK_1, FSCK_750, 
			  "Storage domain '%s' has %d mounted file system(s):\n"),
		  fsInfo->fsname, status);
    
	for (i = 0; i < status; i++) {
	    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
		      catgets(_m_catd, S_FSCK_1, FSCK_928, 
			      "%s mounted on %s\n"), 
			      mntArray[i].fspath, mntArray[i].mntdir);
	}

	writemsg (SV_ERR | SV_LOG_ERR | SV_CONT, 
		  catgets(_m_catd, S_FSCK_1, FSCK_751, 
			  "Can not operate on storage domains with mounted file systems.\n"));
	free(mntArray);
	return FAILURE;
    }
    /*
     * Create the volumes array to store the device names in.
     */
    status = create_volumes (&volumes);
    if (status != SUCCESS) {
        return status;
    }
    domain->volumes = volumes;
    
    /*
     * Open the domain's storage directory to get the volumes.
     */

    writemsg(SV_DEBUG, catgets(_m_catd, S_FSCK_1, FSCK_741, 
			       "Opening directory '%s'.\n"), fsInfo->stgdir);

    stgDir = opendir(fsInfo->stgdir);
    if (stgDir == NULL) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_752, 
			 "Open failed on '%s'\n"), fsInfo->stgdir);
        perror(Prog);
        return FAILURE;
    }
    
    /*
     * Loop through all the volumes in the domain. Eventually, there is
     * to be a way to get a list of device paths given an fsname, and
     * we can get rid of the code that now fetches the volume names
     * via realpath() below.
     */
    while (dp = readdir(stgDir)) {

	if (*dp->d_name == '.') { /* not a volume */
	    continue;
	}
        strcpy(dirName, fsInfo->stgdir);
        strcat(dirName, "/");
    
        strcat(dirName, dp->d_name);

        if (stat(dirName, &statBuf) < 0) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_753, 
			     "Can't obtain stat information for volume '%s'.\n"), 
		     dirName);
            perror(Prog);
            continue;
        }

        /*
         * Skip if not block device 
         */
        if (!S_ISBLK(statBuf.st_mode)) {
            continue;
        }

	/* get the real volume path name for the volume link */

	if (realpath(dirName, volName) == NULL) {
            writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_754, 
			     "realpath() failed on '%s'.\n"), dirName);
            perror(Prog);
            closedir(stgDir);
            return FAILURE;
        }

        volumes[counter].volName = (char *)ffd_malloc(strlen(volName) + 1);

        if (volumes[counter].volName == NULL) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FSCK_1, FSCK_755,
			     "volume name"));
            closedir(stgDir);
            return NO_MEMORY;
        }

        strcpy(volumes[counter].volName, volName);
        
        counter++;

        if (counter >= MAX_VOLUMES) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_757,
			     "More than %d volumes found.\n"), 
		     (MAX_VOLUMES - 1));
            closedir(stgDir);
            return FAILURE;
        }
    } /* end while loop */

    domain->numVols = counter;

    closedir(stgDir);
    return SUCCESS;

} /* end get_domain_volumes*/


/*
 * Function Name: open_domain_volumes
 *
 * Description: This opens all the volumes in the domain.
 *
 * Input parameters:
 *     domain: The domain structure which contains the volumes to open.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, CORRUPT, NO_MEMORY, NON_ADVFS
 *
 */
int 
open_domain_volumes(domainT *domain)
{
    char      *funcName = "open_domain_volumes";
    int       fixed; /* Local variable to see if any volume has been fixed. */
    volNumT   counter;
    int       status;
    volNumT   volCounter;
    volNumT   volNumber;
    lbnNumT   volSize;
    uint64_t  sizeOfSbm;
    uint32_t  *sbm;  /* Pointer to SBM array.   */
    uint32_t  *overlapBitMap; /* Pointer to overlapping SBM array.  */
    volumeT   *oldVolumes;
    volumeT   *newVolumes;
    rbmtInfoT *volRbmt;

    fixed = FALSE;
    oldVolumes = domain->volumes;
    domain->dmnId.id_sec = NULL;
    domain->dmnId.id_usec = NULL;
    domain->highVolNum = 0;
    volCounter = 0;

    /*
     * Open all the volumes
     */
    for (counter = 0; counter < MAX_VOLUMES; counter++)  {
        /* 
         * If the volume name is not NULL, open it. 
         */
        if (oldVolumes[counter].volName == NULL) {
            continue;
        }

        /* 
         * Found a valid block device, so open it and store the
         * file descriptor.
         */
        
        oldVolumes[counter].volFD = open(oldVolumes[counter].volName, 
					 O_RDWR | O_NONBLOCK);

        if (oldVolumes[counter].volFD == -1) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_758,
			     "Can't open device '%s'.\n"),
                     oldVolumes[counter].volName);
            perror(Prog);
            continue;
        }

	if (is_advfs_volume(&oldVolumes[counter]) == FALSE) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_175,
			     "'%s' may not be an AdvFS volume.\n"),
		     oldVolumes[counter].volName);
	    return FAILURE;
	}
		
	/*
	 * Note that it is always possible to open the raw device,
	 * so there is no point in trying to open it to prevent others
	 * from using it.
	 */
   }

    /* 
     * Reallocate volumes based on actual volume number. We need to
     * get a new array to put them in.
     */
    status = create_volumes (&newVolumes);
    if (status != SUCCESS) {
        return status;
    } 
   
    /* 
     * Loop through all the volumes in the old volumes array.  
     */
    for (counter = 0; counter < MAX_VOLUMES; counter++) {
        /* 
         * Check to see if the current volume has a valid file descriptor.
         */
        if (oldVolumes[counter].volFD == -1) {
            continue;
        }

        /*
         * Get the volume number and size from the RBMT's VD_ATTR record. 
         */
	status = get_volume_info(domain, counter, &volNumber, &volSize);
        if (status != SUCCESS) {
            /* 
             * Get volume number failed.  Very bad...
             */
            return status;
	}

        /*
         * Check for duplicate volume numbers.
         */
        if (newVolumes[volNumber].volFD != -1) {
            /*
             * We have two or more volumes which have the same volume ID.
	     * At this point we do not know which is the correct volume,
	     * so we will need to corrupt exit.
	     *
	     * FUTURE: We could check more places on disk to see if we
	     * can figure out which one is in error.
             */
            writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_759,
			     "Two volumes, %s and %s, have the same volume ID %d.\n"),
		     newVolumes[volNumber].volName,
		     oldVolumes[counter].volName,
		     volNumber);
            close(newVolumes[volNumber].volFD);
            newVolumes[volNumber].volFD = -1;

	    /*
	     * Inform the user to remove one of the volumes and then
	     * re-run fixfdmn.  Then exit corrupt.
	     */
	    writemsg(SV_ERR | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_760, 
			     "If you know which volume does not belong to this domain,\n"));
	    writemsg(SV_ERR | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_761,
			     "remove it and then rerun %s.\n"), Prog);
	    corrupt_exit(NULL);
        }
        
        if (volNumber > domain->highVolNum) {
            domain->highVolNum = volNumber;
        }

        /* 
         * Copy all the info for this volume into the correct slot in the
         * new volume array. 
         */
        volCounter++;

	*(&newVolumes[volNumber]) = *(&oldVolumes[counter]);

        newVolumes[volNumber].volSize = volSize;

	volRbmt = (rbmtInfoT *)ffd_malloc(sizeof(struct rbmtInfo));
	if (volRbmt == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FSCK_1, FSCK_762,
			     "volume RBMT structure"));
	    return NO_MEMORY;
	}
	volRbmt->sbmLBNSize = 0;
	volRbmt->bmtLBNSize = 0;

        newVolumes[volNumber].volRbmt = volRbmt;

	newVolumes[volNumber].volSbmInfo =
	    (sbmInfoT *)ffd_malloc(sizeof(sbmInfoT));
	if (newVolumes[volNumber].volSbmInfo == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_10,
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FSCK_1, FSCK_763,
			     "volume sbmInfo structure"));
	    return NO_MEMORY;
	}

	/* compute the number of 32 bit words in the SBM and allocate it */

	sizeOfSbm = LBN2CLUST(newVolumes[volNumber].volSize) / 
	            SBM_BITS_LONG + 1;

	sbm = (uint32_t *)ffd_calloc(sizeOfSbm, sizeof(uint32_t));
	if (sbm == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_10,
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FSCK_1, FSCK_764,
			     "storage bit map"));
	    return NO_MEMORY;
	}

	overlapBitMap = (uint32_t *)ffd_calloc(sizeOfSbm, sizeof(uint32_t));
	if (overlapBitMap == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FSCK_1, FSCK_765, 
			     "overlap bit map"));
	    return NO_MEMORY;
	}

	newVolumes[volNumber].volSbmInfo->sbm = sbm;
	newVolumes[volNumber].volSbmInfo->overlapBitMap = overlapBitMap;
	newVolumes[volNumber].volSbmInfo->overlapListByFile = NULL;
	newVolumes[volNumber].volSbmInfo->overlapListByLbn = NULL;

        writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_766, 
			 "Volume '%s' is volume number %d.\n"),
                 newVolumes[volNumber].volName, volNumber);

    } /* End loop of volumes. */

    domain->volumes = newVolumes;
    /* 
     * Have moved all volumes into the correct position in newVolumes 
     */
    free(oldVolumes);

    if (volCounter == 0) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_767, 
			 "No valid volumes found in domain '%s'.\n"),
		 domain->dmnName);
        corrupt_exit(NULL);
    }

    /*
     * If what they want to do is restore from undo we can return.
     * This way we don't try to correct the magic number for a restore.
     */
    if (Options.undo == TRUE) {
	return SUCCESS;
    }

    /*
     * Loop through the volumes checking the magic number.
     */
    for (counter = 1; counter <= domain->highVolNum; counter ++) {

        if (newVolumes[counter].volFD == -1) {
            continue;
        }
        
        /* 
         * Volume is now open - fix the FS magic number if we're not
         * restoring the undo files.
         */

	status = correct_magic_number(domain, counter);

	if (status != SUCCESS) {
	    if (status == FIXED) {
		fixed = TRUE;
	    } else if (status == NON_ADVFS) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_774,
				 "'%s' is not an AdvFS volume.\n"),
			 newVolumes[counter].volName);
		return status;
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_775,
				 "Can't fix AdvFS magic number on volume %s.\n"),
			 newVolumes[counter].volName);
		return status;
	    }
	}
    } /* end for loop */

    return SUCCESS;

} /* end open_domain_volumes */


/*
 * Function Name: get_volume_number
 *
 * Description:
 *  Follow the next mcell pointer from RBMT page 0 mcell 0, get
 *  the volume number from the next pointer and domain attributes. 
 *
 * Input parameters:
 *  domain: A pointer to the domain structure.
 *  index: An index into the volumes array.
 *  volNumber: The volume we are working on.
 *  volSize: The size of the volume
 *
 * Output parameters:
 *  domain: dmnId is non-NULL if we found one in the RBMT domain attrs
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, CORRUPT
 */
static int 
get_volume_info(domainT *domain, 
		volNumT index,
		volNumT *volNumber,
		lbnNumT *volSize)
{
    char          *funcName = "get_volume_number";
    pageBufT       pageBuf;
    bsMPgT        *pBMTPage;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsVdAttrT     *pVdAttr;
    bsDmnAttrT    *pDmnAttr;   
    ssize_t       bytes;
    volNumT       volId1;
    volNumT       volId2;
    volNumT       volId3;

    /*
     * Init volIds to illegal value.
     */
    volId1 = 0;
    volId2 = 0;
    volId3 = 0;

    /*
     * Read the RBMT page 0.
     */

    if (lseek(domain->volumes[index].volFD,
                           RBMT_START_BLK * (int64_t)DEV_BSIZE,
			   SEEK_SET) == (off_t)-1) {
        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_192,
			 "Can't lseek to block %ld on '%s'.\n"),
		 RBMT_START_BLK, domain->volumes[index].volName); 
        perror(Prog);
        return FAILURE;
    }
    bytes = read(domain->volumes[index].volFD, &pageBuf, ADVFS_METADATA_PGSZ);

    if (bytes == -1) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_193, 
			 "Can't read page at block %ld on '%s'.\n"), 
		 RBMT_START_BLK, domain->volumes[index].volName);
        perror(Prog);
        return FAILURE;
    } else if (bytes != ADVFS_METADATA_PGSZ) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_194,
			 "Can't read page at block %ld on '%s', only read %ld of %d bytes.\n"),
		 RBMT_START_BLK, domain->volumes[index].volName,
                 bytes, ADVFS_METADATA_PGSZ);
        return FAILURE;
    }
      
    pBMTPage = (bsMPgT *)pageBuf;

    /* 
     * Locate mcell 0 in BMT page. 
     */
    pMcell = &(pBMTPage->bsMCA[0]);

    /* 
     * Now we need to step through this mcell looking for the volume
     * attributes record.
     */
    pRecord = (bsMRT *)pMcell->bsMR0;

    while ((pRecord->type != BSR_NIL) && (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	if (pRecord->type == BSR_VD_ATTR) {

	    /* Get the volume number from the record. */

	    pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));
	    volId1 = pVdAttr->vdIndex;
	}
            
	pRecord = NEXT_MREC(pRecord);

    } /* end while loop */

    /*
     * Get the volume number from the NEXT pointer of mcell 0.
     */
    volId2 = pMcell->mcNextMCId.volume;

    /*
     * Follow the NEXT pointer to find the mcell we need.
     */
     
    pMcell = &(pBMTPage->bsMCA[pMcell->mcNextMCId.cell]);

    /*
     * Compute the volume number from this mcell's BMT tag number.
     */
    volId3 = (-1 * pMcell->mcTag.tag_num / BFM_RSVD_TAGS);
    
    /*
     * Now see if all of the volume numbers obtained actually match.
     */
    if ((volId1 == volId2) && (volId2 == volId3)) {
	/*
	 * All three match - No corruption.
	 */
	*volNumber = volId1;
    } else if ((volId1 == volId2) || (volId1 == volId3)) {
	/*
	 * v2 or v3 match v1, volId1 not corrupt.
	 */
	*volNumber = volId1;
    } else if (volId2 == volId3) {
	/*
	 * v2 and v3 three match each other but mismatch v1, volId1 corrupt.
	 */
	*volNumber = volId2;
    } else {
	/*
	 * No match - Have chosen to use volId1.
	 */
	*volNumber = volId1;
    }

    /*
     * The volume size info is stored in the RBMT's vd_attr.  We used
     * to get the 'real world' volume size from the driver to cross
     * check the info in the size vd_attr record, but here on HP/UX
     * the file system can be smaller and one device and so the volume
     * size can also be smaller than the device.  (on Tru64 the file
     * system was always the same size as a partition).  This means we
     * can no longer get the volume size from the driver, so cross
     * checking of the vd_attr volume size value is no longer
     * possible.  We just believe the BSR_VD_ATTR record.
     */

    *volSize = pVdAttr->vdBlkCnt;

    /*
     * Make sure the volume number we have falls within the
     * proper range.
     */
    if ((*volNumber > MAX_VOLUMES) || (*volNumber < 1)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_780, 
			 "'%s' volume number %d invalid, may not be an AdvFS volume.\n"),
		 domain->volumes[index].volName, volNumber);
        return CORRUPT;
    }

    return SUCCESS;

} /* end get_volume_number */


/*
 * Function Name: load_subsystem_extents
 *
 * Description: Collect the extents from the RBMT for all the 
 *              important subsystems.
 *
 *		Note that the RBMT has already been checked for
 *		internal consistency so this routine can trust that
 *		information.
 *
 * Input parameters:
 *     domain: The domain being checked.
 * 
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY
 *
 */
int 
load_subsystem_extents(domainT *domain)
{
    char 	*funcName = "load_subsystem_extents";
    int		status;
    volNumT 	volNum;
    volumeT	*volumes;
    mcidT	mcell;

    volumes = domain->volumes;

    for (volNum = 1 ; volNum <= domain->highVolNum ; volNum++) {
        if (volumes[volNum].volFD == -1) {
	    continue;
	}

	/*
	 * Get the RBMT extents
	 */
	status = collect_rbmt_extents(domain, volNum, TRUE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Set defaults for the following
	 */
	mcell.volume = volNum;
	mcell.page = 0;

	/*
	 * Get the SBM extents
	 */
	mcell.cell = BFM_SBM;
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->sbmLBN),
				 &(volumes[volNum].volRbmt->sbmLBNSize), TRUE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Get the root tag extents
	 */
	if (volNum == domain->rootTagVol) {
	    mcell.cell = BFM_BFSDIR;
	    status = collect_extents(domain, NULL, &mcell, TRUE,
				     &(domain->rootTagLBN),
				     &(domain->rootTagLBNSize), TRUE);
	    if (status != SUCCESS) {
		return status;
	    }
	}

	/*
	 * Get the transaction log extents.
	 * NOTE: All volumes CAN have a tran log.
	 */
	mcell.cell = BFM_FTXLOG;
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->logLBN),
				 &(volumes[volNum].volRbmt->logLBNSize),
				 TRUE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Get the BMT extents
	 */

	mcell.cell = BFM_BMT;
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->bmtLBN),
				 &(volumes[volNum].volRbmt->bmtLBNSize), 
				 TRUE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Get the MISC bitfile extents
	 */
	mcell.cell = BFM_MISC;
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->miscLBN),
				 &(volumes[volNum].volRbmt->miscLBNSize), 
				 TRUE);


	if (status != SUCCESS) {
	    return status;
	}
    }

    return SUCCESS;
} /* end load_subsystem_extents */


/*
 * Function Name: collect_extents
 *
 * Description: This routine collects all the extents for a file.  The
 *		calling function is responsible for freeing the extent array
 *		that is malloc'ed by this routine. Also, the extents have
 *		already been checked before this function is called. There
 *		is no need to do validation of the extent array.
 *
 * Implementation Notes & mcell linkage primer: 
 *
 * Any AdvFS file (including metadata files) is represented by a
 * linked list of mcells, (the list head is the "primary mcell").  The
 * linkage reference is an mcidT struct, rather than a memory address.
 * The mcidT struct references the next mcell using that mcell's
 * volume/page/cell address, which is unique across all domain
 * volumes.  An mcell list is forward linked using the mcell header's
 * mcell->nextMCId entry.
 * 
 * This function is passed the mcidT of a file's primary mcell, and it
 * then walks the list of the mcells in search of the mcell that
 * contains the "primary extent record" (BSR_XTNT).  The primary
 * extent record lists the starting block address of that file's
 * storage.  If the file uses more storage than can be shown in the
 * primary extent record, then the primary extent record links to an
 * extra extent record using the extentRecord->chainMCId entry. This
 * begins a new linked list that is distinct from the mcell linked
 * list begun from the primary mcell.
 * 
 * Additional extent information linked to by the primary extent
 * record is found in the linked-to mcell whose sole record is the
 * "extra extents record" (BSR_XTRA_XTNTS).  If the extra extents
 * record can not hold all the file's needed additional storage, it
 * links to another extra extents record.  This linkage is continued
 * using the mcell header's mcell->nextMCId link.  Additional extra
 * extent records are added to this list using the mcell->nextMCId
 * link as needed.
 *
 * So in summary, the file's mcell linked list is headed by the
 * prinary mcell, and is forward linked using the mcell header's
 * mcell->nextMCId entry.  That mcell list contains a "primnary extent
 * record" which may be the head of a forward linked list of
 * additional extent data, using the extent record's extent->chainMCId
 * entry.  Additional extent data elements in that list are forward
 * linked using the mcell header's mcell->nextMCId link.
 *
 * In general, this function walks the mcell list to find the primnary
 * extent, then walks the extent list headed by the primary extent.
 * As it walks the extent list it aggregates extent information into a
 * newly created "extent array", which is returned to the caller.
 *
 *
 * Input parameters:
 *     domain: A pointer to the domain being worked on.
 *     fileset: A pointer to the fileset (if any) this file belongs to.
 *     pPrimMcell: A pointer to the primary mcell for the file.
 *     addToSbm: A flag whether to call collect_sbm_info or not.
 *     isRBMT: A flag indicating whether extents should be collected
 *		from the BMT or from the RBMT.
 * 
 * Output parameters:
 *     extents: A pointer to an array of the extents in this file.
 *     arraySize: Size of extent array.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 *
 */
int 
collect_extents(domainT   *domain, 
		filesetT  *fileset, 
		mcidT     *pPrimMCId,
		int       addToSbm, 
		fobtoLBNT **extents,
		xtntNumT  *arraySize, 
		int       isRBMT)
{
    char *funcName = "collect_extents";

    mcidT	currMCID;	/* The mcell currently being worked on */
    mcidT	nextMCID;	/* a forward link in the mcell list */
    mcidT	chainMCId;	/* a forward link in the extent list */
    volumeT	*volume;
    pageT	*currPage;
    fobtoLBNT   *tmpExtents;
    fobtoLBNT   extentArray[BMT_XTRA_XTNTS];
    bsMPgT	*bmtPage;
    bsMCT	*pMcell;
    bsMRT	*pRecord;
    bsXtntRT	*pXtnt;
    bsXtraXtntRT *pXtraXtnt;
    xtntNumT	numExtents;
    xtntNumT	currExtent;
    lbnNumT	currLbn;
    volNumT	currVol;
    int		i;
    xtntNumT    xcnt;
    int         found;
    int		status;
    tagNumT	filetag;
    tagNumT	settag;

    numExtents = 0;
    currMCID = *pPrimMCId;
    ZERO_MCID(chainMCId);

    /*
     * Count the extents claimed by this mcell chain so we know how
     * much memory to alloc.  Starting with the primary mcell (input
     * arg), walk the "next" linked mcells until a BSR_XTNT record is
     * found.
     */

    found = FALSE;

    while ((currMCID.volume != 0) && (found == FALSE)) {

	volume = &(domain->volumes[currMCID.volume]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	}
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_783, 
			     "Can't convert [R]BMT page %ld to LBN.\n"),
		     currMCID.page);
	    return status;
	}

	/* fetch the RBMT or BMT page that maps the file whose extent
	   info we're collecting. */

	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (status != SUCCESS) {
	    return status;
	}

	bmtPage = (bsMPgT *)currPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCID.cell]);
	filetag  = pMcell->mcTag.tag_num;
	settag  = pMcell->mcBfSetTag.tag_num;

	nextMCID = pMcell->mcNextMCId;

	/*
	 * Now loop through the mcell's records in search of the
	 * BSR_XTNTS record.
	 */

	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BSR_XTNTS) {

		/* found the extent record. Add to the extent count
		   and save aside any chain reference to extra
		   extents.  We'll track those below. */ 

	        pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		numExtents += pXtnt->xCnt - 1;
		chainMCId = pXtnt->chainMCId;
		found = TRUE;
		break;
	    }

	    /*
	     * Point to the next record in the current mcell.
	     */

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, currPage, FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currMCID = nextMCID;

    } /* end loop: primary extent search */

    /*
     * We've now found the primary extent record, now walk down the
     * extent chain (if any).
     */
    currMCID = chainMCId;

    while (currMCID.volume != 0) {
	volume = &(domain->volumes[currMCID.volume]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	}
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_783, 
			     "Can't convert [R]BMT page %ld to LBN.\n"),
		     currMCID.page);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (status != SUCCESS) {
	    return status;
	}

	bmtPage = (bsMPgT *)currPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCID.cell]);
	nextMCID = pMcell->mcNextMCId;

	/*
	 * find the extra extents record in this mcell.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BSR_XTRA_XTNTS) {

		/* found an extra extents record, update the extent
		   count. */

		pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord +
					     sizeof(bsMRT));
		numExtents += pXtraXtnt->xCnt - 1;
	    }

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, currPage, FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currMCID = nextMCID;
    } /* end chain loop */

    /*
     * Now we have found and counted all the extents.  malloc the
     * extent array and loop through the extents again, this time
     * loading the extent array.  Call collect_sbm_info if caller
     * directed us to.
     *
     * arraySize is the number of actual extents, which does not
     * include the trailing LBN -1 "extent" entry - therefore calloc
     * arraysize + 1 elements.
     */

    *arraySize = numExtents;
    tmpExtents = (fobtoLBNT *)ffd_calloc(*arraySize + 1, sizeof(fobtoLBNT));
    if (tmpExtents == NULL) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_417, 
			 "extent array"));
        return NO_MEMORY;
    }

    *extents = tmpExtents;

    /*
     * Reset pointers to loop through the mcells again.
     */

    currExtent = 0;
    currMCID = *pPrimMCId;
    ZERO_MCID(chainMCId);

    /* Starting with the primary mcell (input arg), walk the "next"
       linked mcells until a BSR_XTNT record is found. */

    found = FALSE;

    while ((currMCID.volume != 0) && (found == FALSE)) {

	volume = &(domain->volumes[currMCID.volume]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	}
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_783,
			     "Can't convert [R]BMT page %ld to LBN.\n"),
		     currMCID.page);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (status != SUCCESS) {
	    return status;
	}

	bmtPage = (bsMPgT *)currPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCID.cell]);

	nextMCID = pMcell->mcNextMCId;

	/*
	 * Now loop through the mcell records in search of the
	 * BSR_XTNTS record.
	 */

	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BSR_XTNTS) {
	    
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		/* we found the primary extent record.  Fill in the
		   extent array passed in by the caller (referenced
		   via tmpExtents) */

		for (i = 0; i < pXtnt->xCnt && i < (BMT_XTNTS + 1); i++) {

		    tmpExtents[currExtent].volume = currMCID.volume;
		    tmpExtents[currExtent].fob = pXtnt->bsXA[i].bsx_fob_offset;
		    tmpExtents[currExtent].lbn = pXtnt->bsXA[i].bsx_vd_blk;
		    tmpExtents[currExtent].isOrig = FALSE;
		    extentArray[i].volume = currMCID.volume;
		    extentArray[i].fob = pXtnt->bsXA[i].bsx_fob_offset;
		    extentArray[i].lbn = pXtnt->bsXA[i].bsx_vd_blk;
		    currExtent++;
		}

		if (addToSbm == TRUE) {
		    status = collect_sbm_info(domain, extentArray,
					      pXtnt->xCnt,
					      filetag, settag, 
					      &currMCID, 0);
		    if (status != SUCCESS) {
			return status;
		    }
		}

		/*
		 * Save aside the chain linkage pointer.  We'll follow
		 * the chain and load the extra extents next.
		 */
		chainMCId = pXtnt->chainMCId;
		found = TRUE;
		break;
	    } /* End if BSR_XTNTS record */

	    /*
	     * Point to the next record.
	     */

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, currPage, 
				       FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currMCID = nextMCID;

    } /* end loop to fetch primary extents */

    /*
     * We found the primary extent record and loaded those values.
     * Now track down the extra extents, if any.  The first extra
     * extents record lives in an mcell pointed to by the primary
     * extents record's chainMCId entry.  If there is more than one
     * extra extents mcell, then each extra extents mcell's nextMCId
     * pointer points to the next extra extents mcell in the chain.
     */

    currMCID = chainMCId;

    while (currMCID.volume != 0) {
	volume = &(domain->volumes[currMCID.volume]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currMCID.page,
					 &currLbn, &currVol);
	}
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_783, 
			     "Can't convert [R]BMT page %ld to LBN.\n"),
		     currMCID.page);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (status != SUCCESS) {
	    return status;
	}

	bmtPage = (bsMPgT *)currPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCID.cell]);

	nextMCID = pMcell->mcNextMCId;

	/*
	 * Now loop through chain records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BSR_XTRA_XTNTS) {
		pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord +
					     sizeof(bsMRT));
		/*
		 * Decrement currExtent to deal with the final extent
		 * of the previous record.
		 */
		if (currExtent > 0) {
                    currExtent--;
                }

		/*
		 * Sanity check - The first extent of this record should
		 * be the same page as the last extent of the previous
		 * record.  This should have already been fixed earlier
		 * in fcsk.
		 */
		assert(tmpExtents[currExtent].fob ==
		       pXtraXtnt->bsXA[0].bsx_fob_offset);

		/*
		 * Load all extents
		 */
		for (i = 0 ; i < pXtraXtnt->xCnt && i < BMT_XTRA_XTNTS ; i++) {

		    tmpExtents[currExtent].volume = currMCID.volume;
		    tmpExtents[currExtent].fob = 
		                           pXtraXtnt->bsXA[i].bsx_fob_offset;
		    tmpExtents[currExtent].lbn = pXtraXtnt->bsXA[i].bsx_vd_blk;
		    tmpExtents[currExtent].isOrig = FALSE;
		    extentArray[i].volume = currMCID.volume;
		    extentArray[i].fob = pXtraXtnt->bsXA[i].bsx_fob_offset;
		    extentArray[i].lbn = pXtraXtnt->bsXA[i].bsx_vd_blk;
		    currExtent++;
		}
		if (addToSbm == TRUE) {

		    /* set this file's allocation bits in the in-memory sbm */

		    status = collect_sbm_info(domain, extentArray,
					      pXtraXtnt->xCnt,
					      filetag, settag,
					      &currMCID, 0);
		    if (status != SUCCESS) {
			return status;
		    }
		}
	    } /* End if pRecord->type points to BSR_XTRA */
	    
	    /*
	     * Point to the next record.
	     */

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, currPage, FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currMCID = nextMCID;

    } /* end while: walk the extra extents loop */

    /*
     * If this is a snapshot fileset, merge in the extents from the
     * original
     *
     * TODO WRITEABLE SNAPSHOTS: need to determine is origSnapPtr
     * means the original fileset, or just the parent of this snap
     * (which may itself be a snap).
     */

    if (fileset != NULL && FS_IS_SNAP(fileset->status)) {
	filesetT  *origFileset;
	fobtoLBNT *origXtntArray;
	xtntNumT  origXtntArraySize;
	fobtoLBNT *snapXtntArray;
	xtntNumT  snapXtntArraySize;
	fobtoLBNT *newXtntArray;
        xtntNumT  newXtntArraySize;
	tagNodeT  tmpTag;
	tagNodeT  *tagNode;
	tagNodeT  *origTagNode;
	nodeT	  *node;

	/* 
	 * Initialize variables
	 */

	origFileset = fileset->origSnapPtr;
	snapXtntArray = tmpExtents;
	snapXtntArraySize = numExtents;
	tmpTag.tagSeq.tag_num = filetag;
	tagNode = &tmpTag;

	status = find_node(fileset->tagList, (void **)&tagNode, &node);
	if (status != SUCCESS) {
	    return status;
	}

	if (origFileset == NULL) {
	    /*
	     * This might be an Internal Error, but we can continue, so
	     * do so.
	     */
	    return SUCCESS;
	}

	origTagNode = &tmpTag;
	status = find_node(origFileset->tagList, (void **)&origTagNode, &node);
	if (status == NOT_FOUND) {
	    /*
	     * Another Internal Error we can continue from.
	     */
	    return SUCCESS;
	} else if (status != SUCCESS) {
	    return status;
	}

	if (T_IS_SNAP_ORIG_PMCELL(tagNode->status)) {
	    /*
	     * We just collected the extents for the original file.
	     * Set isOrig to TRUE for all extents.
	     */
	    for (xcnt = 0 ; xcnt <= numExtents ; xcnt++) {
		tmpExtents[xcnt].isOrig = TRUE;
	    }
	} else {
	    /*
	     * We just collected the snap specific extents.  Now go
	     * get the original file's extents and merge them.
	     */
	    status = collect_extents(domain, origFileset,
				     &(origTagNode->tagMCId), FALSE,
				     &origXtntArray, &origXtntArraySize,
				     FALSE);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = merge_snap_extents(origXtntArray, origXtntArraySize,
					 snapXtntArray, snapXtntArraySize,
					 &newXtntArray, &newXtntArraySize);
	    if (status != SUCCESS) {
		return status;
	    }
	    free(origXtntArray);
	    free(snapXtntArray);

	    *extents = newXtntArray;
	    *arraySize = newXtntArraySize;
	}

	if (tagNode->size <
	    ((*extents)[*arraySize].fob - 1) * ADVFS_FOB_SZ) {
	    /*
	     * This situation can happen when a file is extended after
	     * the snap is made.  The snap file's filesize is the only
	     * clue that we've reached the end of the file, so we
	     * should truncate the extents.
	     */
	    bf_fob_t lastFob;

	    if (tagNode->size % ADVFS_FOB_SZ == 0) {
		lastFob = (tagNode->size / ADVFS_FOB_SZ);
	    } else {
		lastFob = (tagNode->size / ADVFS_FOB_SZ) + 1;
	    }

	    for (xcnt = *arraySize ; xcnt > 0 ; xcnt--) {
		if ((*extents)[xcnt - 1].fob < lastFob) {
		    if ((*extents)[xcnt].fob >= lastFob) {
			(*extents)[xcnt].fob = lastFob;
			(*extents)[xcnt].lbn = -1;
			*arraySize = xcnt;
		    }
		    break; /* out of for loop */
		}
	    }
	} /* End if: extents beyond filesize */
    } /* End if: snapshot */

    return SUCCESS;
} /* end collect_extents */

/*
 * Function Name: merge_snap_extents
 *
 * Description: This routine merges the original and snap extent
 *		maps to form a complete extent map for a snap file.
 *
 * Note that the general algorithm is taken from imm_merge_xtnt_map in
 * kernel/common/fs/advfs/bs/bs_inmem_map.c.
 *
 * The merged extent map describes fobs mapped by both the parent and
 * the child extent maps.  The fobs from the parent extent map are
 * those that do not intersect with real fobs described by the child
 * extent map.  The fobs from the child extent map are those real fobs
 * that do intersect with real fobs described by the parent extent
 * map.
 *

 * Input parameters:
 *     origXtntArray: The array of the original (parent) file's extents.
 *     origXtntArraySize: Size of the original file's extent array.
 *     snapXtntArray: The array of the snap's (child's) extents.
 *     snapXtntArraySize: Size of the snap's extent array.
 * 
 * Output parameters:
 *     newXtntArray: A pointer to an array of the merged extents in this file.
 *     newXtntArraySize: Size of the merged extent array.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 *
 */
static int 
merge_snap_extents(fobtoLBNT *origXtntArray, 
		   xtntNumT  origXtntArraySize,
		   fobtoLBNT *snapXtntArray, 
		   xtntNumT  snapXtntArraySize,
		   fobtoLBNT **newXtntArray, 
		   xtntNumT  *newXtntArraySize)
{
    char *funcName = "merge_snap_extents";
    xtntNumT  origXtnt;
    xtntNumT  snapXtnt;
    xtntNumT  newXtnt;
    xtntNumT  copyXtnt;
    fobtoLBNT *copyXtntArray;
    xtntNumT  copyXtntArraySize;
    uint64_t  tmpSize;
    int       isOrig;

    /*
     * Initialize variables
     */
    origXtnt = 0;
    snapXtnt = 0;
    newXtnt = 0;
    copyXtntArray = NULL;

    assert(origXtntArray != NULL && snapXtntArray != NULL);

    /*
     * If either extent map is empty, return a copy of the other one.
     * A copy is needed because both originals are going to be freed.
     */
    if (origXtntArraySize == 0) {
	copyXtntArray = snapXtntArray;
	copyXtntArraySize = snapXtntArraySize;
	isOrig = FALSE;
    } else if ((snapXtntArraySize == 0) || snapXtnt > snapXtntArraySize) {
	copyXtntArray = origXtntArray;
	copyXtntArraySize = origXtntArraySize;
	isOrig = TRUE;
    }

    if (copyXtntArray != NULL) {
	/*
	 * arraySize is the number of actual extents, which does not include
	 * the trailing LBN -1 "extent" entry - therefore calloc arraysize + 1
	 * elements.
	 */
	*newXtntArraySize = copyXtntArraySize;
	*newXtntArray = (fobtoLBNT *)ffd_calloc(*newXtntArraySize + 1,
						 sizeof(fobtoLBNT));
	if (*newXtntArray == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FSCK_1, FSCK_417, 
			     "extent array"));
	    return NO_MEMORY;
	}
	memcpy(*newXtntArray, copyXtntArray,
	       ((*newXtntArraySize + 1) * sizeof(fobtoLBNT)));

	for (newXtnt = 0 ; newXtnt <= *newXtntArraySize ; newXtnt++) {
	    (*newXtntArray)[newXtnt].isOrig = isOrig;
	}

	return SUCCESS;
    }

    /*
     * Both extent maps have extents.  Have to do a merge.
     */

    /*
     * Malloc new extent array to be the size of both original and snap
     * arrays added together.  This is probably overkill, but not by a
     * significant amount, and shouldn't have a significant memory impact.
     * There is no way for this to be too small, and it cuts the amount of
     * processing and code for this routine almost in half.
     *
     * newXtntArraySize will be reset after the actual number of extents
     * has been calculated.
     */
    *newXtntArraySize = (origXtntArraySize + 1) + (snapXtntArraySize + 1);
    *newXtntArray = (fobtoLBNT *)ffd_calloc(*newXtntArraySize + 1,
					     sizeof(fobtoLBNT));
    if (*newXtntArray == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_417, 
			 "extent array"));
	return NO_MEMORY;
    }

    origXtnt = 0;
    snapXtnt = 0;

    while (origXtnt < origXtntArraySize && snapXtnt < snapXtntArraySize) {
	/* Get the first or next non-hole snap extent. */
	while ((snapXtnt < snapXtntArraySize) &&
	       (snapXtntArray[snapXtnt].lbn == XTNT_TERM)) {
	    snapXtnt++;
	}

	assert(snapXtnt == snapXtntArraySize ||
	       snapXtntArray[snapXtnt].lbn != XTNT_TERM);

	/*
	 * Step 1: Copy original extents into new extent map until we
	 * find an extent that overlaps the next real snap extent's
	 * fob range.
	 */
	while (origXtntArray[origXtnt + 1].fob <=
	       snapXtntArray[snapXtnt].fob) {

	    (*newXtntArray)[newXtnt].volume = origXtntArray[origXtnt].volume;
	    (*newXtntArray)[newXtnt].fob = origXtntArray[origXtnt].fob;
	    (*newXtntArray)[newXtnt].lbn = origXtntArray[origXtnt].lbn;
	    (*newXtntArray)[newXtnt].isOrig = TRUE;
	    newXtnt++;
	    origXtnt++;
	}

	/*
	 * Step 2: If the current original extent's fob is less than
	 * the current snap extent's fob, split the orig extent and
	 * add that split into the new extent map.
	 */
	if (origXtntArray[origXtnt].fob < snapXtntArray[snapXtnt].fob) {
	    (*newXtntArray)[newXtnt].volume = origXtntArray[origXtnt].volume;
	    (*newXtntArray)[newXtnt].fob = origXtntArray[origXtnt].fob;
	    (*newXtntArray)[newXtnt].lbn = origXtntArray[origXtnt].lbn;
	    (*newXtntArray)[newXtnt].isOrig = TRUE;
	    newXtnt++;
	}

	/*
	 * Step 3: Copy snap extents to the new extent map until we
	 * find a hole or run out of extents.
	 */
	while ((snapXtnt < snapXtntArraySize) &&
	       (snapXtntArray[snapXtnt].lbn) != XTNT_TERM) {
	    (*newXtntArray)[newXtnt].volume = snapXtntArray[snapXtnt].volume;
	    (*newXtntArray)[newXtnt].fob = snapXtntArray[snapXtnt].fob;
	    (*newXtntArray)[newXtnt].lbn = snapXtntArray[snapXtnt].lbn;
	    (*newXtntArray)[newXtnt].isOrig = FALSE;
	    newXtnt++;
	    snapXtnt++;
	}

	/*
	 * Increment origXtnt until the last page of that extent is at
	 * least the first page of the next hole in the snap extentmap.
	 */
	while ((origXtnt < origXtntArraySize) &&
	       (origXtntArray[origXtnt + 1].fob <=
		snapXtntArray[snapXtnt].fob)) {
	    origXtnt++;
	}

	/*
	 * Split the original extent if it overlaps the last used snap
	 * extent.
	 */
	if (origXtntArray[origXtnt].fob < snapXtntArray[snapXtnt].fob) {
	    tmpSize = (snapXtntArray[snapXtnt].fob -
		       origXtntArray[origXtnt].fob);
	    if (origXtntArray[origXtnt].lbn != XTNT_TERM) {
	        origXtntArray[origXtnt].lbn += tmpSize * 
		                               ADVFS_FOBS_PER_DEV_BSIZE;
	    }
	    origXtntArray[origXtnt].fob = snapXtntArray[snapXtnt].fob;
	}
    } /* End while both extentmaps have unprocessed extents */

    assert(origXtnt == origXtntArraySize || snapXtnt == snapXtntArraySize);

    /*
     * Step 4: Copy remaining extents from whichever (if either) extent
     * map still has unprocessed extents.
     */
    if (origXtnt == origXtntArraySize && snapXtnt == snapXtntArraySize) {

	if (origXtntArray[origXtnt].fob > snapXtntArray[snapXtnt].fob) {
	    copyXtntArray = origXtntArray;
	    copyXtntArraySize = origXtntArraySize;
	    copyXtnt = origXtnt;
	    isOrig = TRUE;
	} else {
	    copyXtntArray = snapXtntArray;
	    copyXtntArraySize = snapXtntArraySize;
	    copyXtnt = snapXtnt;
	    isOrig = FALSE;
	}
    } else if (origXtnt < origXtntArraySize) {
	copyXtntArray = origXtntArray;
	copyXtntArraySize = origXtntArraySize;
	copyXtnt = origXtnt;
	isOrig = TRUE;
    } else if (snapXtnt < snapXtntArraySize) {
	copyXtntArray = snapXtntArray;
	copyXtntArraySize = snapXtntArraySize;
	copyXtnt = snapXtnt;
	isOrig = FALSE;
    } else {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_786, 
			 "Internal Error: Both origXtnt and snapXtnt greater than array size.\n"));
	return FAILURE;
    }
    if (copyXtntArray != NULL) {
	while (copyXtnt <= copyXtntArraySize) {
	    (*newXtntArray)[newXtnt].volume = copyXtntArray[copyXtnt].volume;
	    (*newXtntArray)[newXtnt].fob = copyXtntArray[copyXtnt].fob;
	    (*newXtntArray)[newXtnt].lbn = copyXtntArray[copyXtnt].lbn;
	    (*newXtntArray)[newXtnt].isOrig = isOrig;
	    newXtnt++;
	    copyXtnt++;
	}
    }

    /*
     * Step 5: Reset *newXtntArraySize to the actual number of extents.
     */
    *newXtntArraySize = newXtnt - 1;

    return SUCCESS;
} /* end merge_snap_extents */

/*
 * Function Name: save_fileset_info
 *
 * Description: This is used to load the fileset information from the BMT.
 *
 * Input parameters:
 *     domain: The domain being worked on.
 *     pPage: A root tag file page.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, CORRUPT or NO_MEMORY
 *
 */
int 
save_fileset_info(domainT *domain, 
		  pageT *pPage)
{
    char         *funcName = "save_fileset_info";
    bsTDirPgT    *pTagPage;
    pageNumT     tagPageNum;
    pageT        *pBmtPage;
    bsMPgT       *bmtPage;
    volumeT      *volume;
    bsMCT        *pMcell;
    bsMRT        *pRecord;
    bsBfSetAttrT *pFsAttr;
    filesetT     *pFileset;
    filesetT     *pFS;
    mcidT        *pFsPmcell;
    fobtoLBNT    *pFsTagLBN;
    xtntNumT     fsTagLBNSize;
    int          fsAttrFound;
    char         *pFsName;
    tagNodeT     *maxTag;
    int          count;
    int          tagStart;
    mcidT        currMCId;
    lbnNumT      pageLBN;
    volNumT      pageVol;
    int          status;

    pTagPage = (bsTDirPgT *)pPage->pageBuf;
    tagPageNum = pTagPage->tpPgHdr.currPage;

    /*
     * We do not use tag 0 on page 0.
     */
    if (tagPageNum == 0) {
        tagStart = 1;
    } else {
        tagStart = 0;
    }

    /*
     * Loop through the tag entries in the root tag file and save the
     * fileset information for the active entries.
     */
    for (count = tagStart; count < BS_TD_TAGS_PG; count++) {
        if (pTagPage->tMapA[count].tmFlags & BS_TD_IN_USE) {
	  
	    /*	
	     * Reset for each fileset
	     */
	    fsAttrFound = FALSE;

            /*
             * Get space for the fileset structure.
             */
            pFileset = (filesetT *)ffd_malloc(sizeof(filesetT));
            if (pFileset == NULL) {
	        writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_10, 
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FSCK_1, FSCK_787,
				 "file system structure"));
                return NO_MEMORY;
            }

            /* 
	     * Init the fileset struct and save the fileset tag file
	     * primary mcell
	     */

            pFileset->domain = domain;
	    pFileset->filesetId.dirTag.tag_num = 0;
	    pFileset->filesetId.dirTag.tag_seq = 0;
	    pFileset->tagLBN = NULL;
	    pFileset->tagLBNSize = 0;
	    pFileset->tagList = NULL;

            pFileset->parentSetTag.tag_num = 0;
            pFileset->parentSetTag.tag_seq = 0;
            pFileset->childSetTag.tag_num = 0;
            pFileset->childSetTag.tag_seq = 0;
            pFileset->sibSetTag.tag_num = 0;
            pFileset->sibSetTag.tag_seq = 0;
            pFileset->snapLevel = 0;
	    pFileset->origSnapPtr = NULL;
            pFileset->linkFixed = FALSE;

	    pFileset->status = 0;
	    pFileset->errorCount = 0;
	    pFileset->next = NULL;

            pFileset->primMCId = pTagPage->tMapA[count].tmBfMCId;

            /* 
             * Get the extents for the fileset tag file.  Do NOT save
             * the extents into the sbm, that will be done in BMT
             * code.  Pretend there is no fileset associated with this
             * mcell, since there is no snap/original relationship
             * yet.
             */

            status = collect_extents(domain, NULL, &pFileset->primMCId, FALSE,
                                     &pFsTagLBN, &fsTagLBNSize, FALSE);
            
            if (status != SUCCESS) {
                return status;
            }
            pFileset->tagLBN = pFsTagLBN;
            pFileset->tagLBNSize = fsTagLBNSize;
            
	    /* get the primary mcell ID and volume struct for the
	       current fileset tag directory.  We'll use this to read
	       in the BMT page that contains the fileset tag files
	       attribute records. */

	    currMCId = pFileset->primMCId;
            volume = &(domain->volumes[currMCId.volume]);
            
            /*
             * Loop through the fileset tag file mcells looking for
             * the fileset's BSR_BFS_ATTR record.
             */

            while ((fsAttrFound == FALSE) && (currMCId.volume != 0)) {

                status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
                                             volume->volRbmt->bmtLBNSize,
                                             currMCId.page, &pageLBN, &pageVol);
                if (status != SUCCESS) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FSCK_1, FSCK_128, 
				     "Can't convert BMT page %ld to LBN.\n"),
			     currMCId.page);
                    return status;
                }

                status = read_page_from_cache(pageVol, pageLBN, &pBmtPage);

                if (status != SUCCESS) {
                    return status;
                }
            
                bmtPage = (bsMPgT *)pBmtPage->pageBuf;
                pMcell = &(bmtPage->bsMCA[currMCId.cell]);
                pRecord = (bsMRT *)pMcell->bsMR0;

                /*
                 * Loop through this mcell's the records looking for
                 * type BSR_BFS_ATTR, then grab the fileset ID and
                 * fileset name from it.
                 */
                while ((fsAttrFound == FALSE) &&
                       (pRecord->type != BSR_NIL) &&
                       (pRecord->bCnt != 0) &&
                       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

                    if (pRecord->type == BSR_BFS_ATTR) {
                        pFsAttr = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));
                        pFileset->filesetId = pFsAttr->bfSetId;
                        
                        /*
                         * Get memory for the fileset name and copy
                         * it from the record.
                         */
                        pFsName = (char *)ffd_malloc(BS_SET_NAME_SZ);
                        if (pFsName == NULL) {
			    writemsg(SV_ERR | SV_LOG_ERR,
				     catgets(_m_catd, S_FSCK_1, FSCK_10, 
					     "Can't allocate memory for %s.\n"),
				     catgets(_m_catd, S_FSCK_1, FSCK_789,
					     "file system name"));
                            return NO_MEMORY;
                        }
                        strncpy(pFsName, pFsAttr->setName, BS_SET_NAME_SZ);
                        pFileset->fsName = pFsName;

                        if (pFsAttr->bfsaSnapLevel)
			    FS_SET_SNAP(pFileset->status);

			pFileset->snapLevel = pFsAttr->bfsaSnapLevel;
			pFileset->parentSetTag = 
			    pFsAttr->bfsaParentSnapSet.dirTag;
			pFileset->childSetTag = 
			    pFsAttr->bfsaFirstChildSnapSet.dirTag;
			pFileset->sibSetTag = 
			    pFsAttr->bfsaNextSiblingSnap.dirTag;

			/* TODO WRITEABLE SNAPSHOTS: Do we need to
			   just find the parent or the first parent
			   (parent of all parents)? */

			for (pFS = domain->filesets; pFS; pFS = pFS->next) {
			    if (BS_BFTAG_EQL(pFileset->parentSetTag, 
					     pFS->filesetId.dirTag)) {
				pFileset->origSnapPtr = pFS;
				pFS->origSnapPtr = pFileset;
				break;
			    }
			}

                        fsAttrFound = TRUE;

                    } /* end if record type is BSR_BFS_ATTR */
                    
		    pRecord = NEXT_MREC(pRecord);

                } /* end while looping through records */
                
                status = release_page_to_cache(pageVol, pageLBN, pBmtPage,
                                               FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}

		currMCId = pMcell->mcNextMCId;
		volume = &(domain->volumes[currMCId.volume]);
                
            } /* end while attrs not found and page is not 0 */
            
            if (fsAttrFound == FALSE) {
                writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_790,
				 "Can't find FS attributes for set tag %ld.\n"),
                         (pTagPage->tpCurrPage * BS_TD_TAGS_PG) + count);
                return CORRUPT;
            }
	    /*
	     * Create the skip list for this fileset's tags.
	     */
	    maxTag = (tagNodeT *)ffd_malloc(sizeof(tagNodeT));
	    if (maxTag == NULL) {
	        writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_10,
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FSCK_1, FSCK_791,
				 "tag list tail"));
		return NO_MEMORY;
	    }

	    maxTag->tagSeq.tag_num = MAX_ADVFS_TAGS;
	    maxTag->tagSeq.tag_seq = BS_TD_MAX_SEQ;

	    status = create_list(&compare_tags, &(pFileset->tagList), maxTag);
	    if (status != SUCCESS) {
		return status;
	    }

	    /*
	     * The filesets are ordered in the root tagfile in the
	     * order in which they were created (parents before their
	     * snapshots, if any).  We want to preserve that order in
	     * the fileset list attached to domain to assure that we
	     * check parents before their snapshots, so we add the new
	     * fileset to the end of the list.
	     *
	     * TODO WRITEABLE SNAPSHOTS: may need a more complex
	     * ordering here...
	     */

	    if (domain->filesets == NULL) {
		domain->filesets = pFileset;
	    } else {
		for (pFS = domain->filesets; pFS->next; pFS = pFS->next)
		    ;
		pFS->next = pFileset;
	    }
        } /* end if tag is active */
    } /* end for each tag on the page */

   return SUCCESS;

} /* end save_fileset_info */


/*
 * Function Name: corrupt_exit
 *
 * Description: This function is used to exit when we find corruption
 *              the tool can't fix. This include printing a message to
 *              the user letting them know what their options are.
 *
 * Input parameters:
 *     message: The message presented to user.
 *
 * Output parameters: N/A
 *
 * Exit Value: 1 (EXIT_CORRUPT)
 * 
 */
void 
corrupt_exit(char *message)
{
    char *funcName = "corrupt_exit";
    if (message != NULL) {
	/*    
	 * Print the message. 
	 */
	writemsg(SV_ERR | SV_CONT, "\n");
        writemsg(SV_ERR | SV_LOG_ERR, message);
    } else {
	/*
	 * Print the default message letting user know their
	 * options at this point.
	 */
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, "\n");
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_795, 
			 "%s found errors on disk it could not fix.\n"),
		 Prog);
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_796, 
			 "This domain should be restored from backup\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_797,
			 "tapes, or you can use the salvage tool to\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FSCK_1, FSCK_798,
			 "pull what files it can find off of this domain.\n\n"));
    }

    if (dmnLock)
	unlock_file(dmnLock, &errno); 

    /*
     * Close the information log file.
     */
    fclose(Options.msglogHndl);

    exit (EXIT_CORRUPT);
} /* end corrupt_exit */


/*
 * Function Name: finalize_fs
 *
 * Description: This function creates the undo files, flushes
 *              metadata changes out to disk, and activates the domain
 *              if the user asked for that.
 *
 * Input parameters:
 *
 * Output parameters:
 *
 * Exit Value:0 (Success), 1 (Corrupt), or 2 (Failure).
 * 
 */
void 
finalize_fs(domainT *domain)
{
    char *funcName = "finalize_fs";
    filesetT *fileset;
    int status;
    int modified = FALSE;
    char answer[10];
    int volNum;
    int noRecordChanges = FALSE;
    pageT	*pPage;
    nodeT	*pNode;
    advfs_fake_superblock_t *superBlk;

#ifdef MOUNT_BIT_XXX
    /* if we're here, we assume that we've fixed up the disk now, so
       we clear the "do not mount" bit from each volume's superblock
       and timestamp it */

    for (volNum = 0 ; volNum <= domain->highVolNum ; volNum++) {

	if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	if (read_page_from_cache(volNum, MSFS_FAKE_SB_BLK, &page) != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_326, 
			     "Failed to read the super-block on volume %s.\n"), 
		     domain->volumes[volNum].volName);
	    failed_exit();
	}

	superBlk = (advfs_fake_superblock_t *) page->pageBuf;

	if (superBlk->doNotMount) {
	    superBlk->doNotMount = NULL;
	    superBlk->fsckTime = time(0);
	    modified = TRUE;
	}

	/* release the fixed superblock back to the cache */

	if (release_page_to_cache(volNum, MSFS_FAKE_SB_BLK, page, 
				  modified, FALSE) != SUCCESS) {
	    /* No writemsg here - release_page_to_cache wrote one. */
	}

	modified = FALSE;
    }
#endif /* MOUNT_BIT_XXX */

    /* create the undo files before we write the new metadata */

    writemsg(SV_DEBUG | SV_LOG_SUM, 
	     catgets(_m_catd, S_FSCK_1, FSCK_802, 
		     "Cache stats:  %d writehits, %d readhits, %d misses\n"),
	     cache->writeHits, cache->readHits, cache->misses);

    /*
     * If the write cache is non-empty then we create undo files.
     */

    status = find_first_node(cache->writeCache, (void *)&pPage, &pNode);

    if (status == NOT_FOUND) {
	noRecordChanges = TRUE;
    } else if (status != SUCCESS) {
	failed_exit();
    } else {  /* there's data in the write cache, create undo files... */

	status = create_undo_files(domain);
	if (status != SUCCESS) {

	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_803, 
			     "Undo files were not created.\n"));

	    if (Options.preen) {
		preen_exit();
	    }

	    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_804, 
			     "If you continue you will not be able to be undo the changes.\n"));
	    writemsg(SV_ERR | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_805,
			     "Do you want %s to write the changes to disk? (y/[n])  "),
		     Prog);
	    status = prompt_user_yes_no(answer, "n");
	    if (status != SUCCESS) {
		failed_exit();
	    }
	    /*
	     * answer[0] must be 'y' or 'n'.
	     */
	    if ('n' == answer[0]) {
		failed_exit();
	    }
	}

    /* now write the changed metadata out to the disk */

	if (Options.nochange == FALSE) {
	    status = write_cache_to_disk();
	    if (status != SUCCESS) {
		restore_undo_files(domain);
		failed_exit();
	    }
	}
    } /* end if: write cache contains data */

    close_domain_volumes(domain);

    /*
     * If the user explicitly wants to activate the domain after
     * writing the corrected metadata, do that here.
     * 
     * Future: print summary statistics to log and tty
     */

    if (Options.activate == TRUE) {
	status = activate_domain(domain);
	if (status != SUCCESS && noRecordChanges == FALSE) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_806,
			     "Activation of the domain failed.\n"));
	    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		     catgets(_m_catd, S_FSCK_1, FSCK_807, 
			     "Restoring the domain's undo files.\n"));
	    for (volNum = 0 ; volNum <= domain->highVolNum ; volNum++) {
		/*
		 * In this error case we must re-open the volume(s) in
		 * order to restore the metadata from undo files.
		 */
		if (domain->volumes[volNum].volName == NULL) {
		    continue;
		}
		domain->volumes[volNum].volFD =
		    open(domain->volumes[volNum].volName, O_RDWR | O_NONBLOCK);

		if (domain->volumes[volNum].volFD == -1) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_758,
				     "Can't open device '%s'.\n"),
			     domain->volumes[volNum].volName);
		    writemsg(SV_ERR| SV_LOG_ERR | SV_CONT, 
			     catgets(_m_catd, S_FSCK_1, FSCK_808, 
				     "Can't restore from undo files.\n"));
		    perror(Prog);

		    failed_exit();
		}
	    }
	    
	    status = restore_undo_files(domain);
	    if (status != SUCCESS) {
		failed_exit();
	    } else {
		corrupt_exit(NULL);
	    }
	}
    } /* end if: need to activate the domain */

    writemsg(SV_VERBOSE | SV_LOG_INFO, 
	     catgets(_m_catd, S_FSCK_1, FSCK_813, 
		     "Checks for '%s' finalized.\n"), domain->dmnName);

    if (!(domain->status & DMN_META_WRITTEN)) {
	writemsg(SV_VERBOSE | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_831,
			 "Storage domain '%s' was not modified.\n\n"),
		 domain->dmnName);
    }

    /*
     * Close the information log file.
     */
    fclose(Options.msglogHndl);

    if (dmnLock)
	unlock_file(dmnLock, &errno); 

    return;

} /* end finalize_fs */


/*
 * Function Name: failed_exit 
 *
 * Description: This function is used to exit when we have a failure which
 *              we can not work around. 
 *
 * Input parameters:
 *
 *
 * Output parameters: N/A
 *
 * Exit Value: 1 (EXIT_FAILED)
 * 
 */
void 
failed_exit(void)
{
    char *funcName = "failed_exit";    
    domainT *domain = cache->domain;
    /*
     * Print a message letting the use know we are exiting.
     */
    writemsg(SV_ERR | SV_CONT, "\n");
    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
	     catgets(_m_catd, S_FSCK_1, FSCK_814, 
		     "Unable to continue, %sexiting.\n"),
	     domain->status & DMN_META_WRITTEN ? "" : "storage domain not modified, ");

    if (dmnLock)
	unlock_file(dmnLock, &errno); 

    /*
     * Close the information log file.
     */
    fclose(Options.msglogHndl);

    exit (EXIT_FAILED);

} /* end failed_exit */


/*
 * Function Name: preen_exit 
 *
 * Description: In "preen" mode (-p on cmd line) if user interaction is 
 *              needed then we simply exit without updating the
 *              metadata.  This function does that.
 *
 * Input parameters:
 *
 *
 * Output parameters: N/A
 *
 * Exit Value: 1 (EXIT_FAILED)
 * 
 */
void 
preen_exit(void)
{
    char *funcName = "failed_exit";    
    /*
     * Print a message letting the user know we are exiting.
     */
    writemsg(SV_ERR | SV_CONT, "\n");
    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
	     catgets(_m_catd, S_FSCK_1, FSCK_973, 
		     "%s has detected a problem that requires user interaction.\n."), 
	     Prog);
    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
	     catgets(_m_catd, S_FSCK_1, FSCK_974, 
		     "No file system changes made. Please rerun %s without the -p option.\n"),
	     Prog);

    if (dmnLock)
	unlock_file(dmnLock, &errno); 

    /*
     * Close the information log file.
     */
    fclose(Options.msglogHndl);

    exit (EXIT_FAILED);

} /* end preen_exit */


/*
 *      crc_32 from POSIX.2 Draft 11.
 *
 *	It is used in create_undo_files and restore_undo_files to generate
 *	a checksum to verify that the pages written to the undo file are
 *	correct.
 *
 * NAME: crc_32
 *
 * FUNCTION:
 *     crc32 generates a 32 bit "classic" CRC using the following
 *     CRC polynomial:
 *
 *   g(x) = x^32 + x^26 + x^23 + x^22 + x^16 + x^12 + x^11 + x^10 + x^8
 *               + x^7  + x^5  + x^4  + x^2  + x^1  + x^0
 *
 *   e.g. g(x) = 1 04c1 1db7
 *
*/

uint64_t
crc_32(b, n, s)
       unsigned char *b;	/* byte sequence to checksum */	
       int 	      n;	/* length of sequence */
       uint64_t       s;	/* initial checksum value */
{

	register int i, aux;

uint64_t crctab[] = {
0x7fffffff,
0x77073096, 0xee0e612c, 0x990951ba, 0x076dc419, 0x706af48f,
0xe963a535, 0x9e6495a3, 0x0edb8832, 0x79dcb8a4, 0xe0d5e91e,
0x97d2d988, 0x09b64c2b, 0x7eb17cbd, 0xe7b82d07, 0x90bf1d91,
0x1db71064, 0x6ab020f2, 0xf3b97148, 0x84be41de, 0x1adad47d,
0x6ddde4eb, 0xf4d4b551, 0x83d385c7, 0x136c9856, 0x646ba8c0,
0xfd62f97a, 0x8a65c9ec, 0x14015c4f, 0x63066cd9, 0xfa0f3d63,
0x8d080df5, 0x3b6e20c8, 0x4c69105e, 0xd56041e4, 0xa2677172,
0x3c03e4d1, 0x4b04d447, 0xd20d85fd, 0xa50ab56b, 0x35b5a8fa,
0x42b2986c, 0xdbbbc9d6, 0xacbcf940, 0x32d86ce3, 0x45df5c75,
0xdcd60dcf, 0xabd13d59, 0x26d930ac, 0x51de003a, 0xc8d75180, 
0xbfd06116, 0x21b4f4b5, 0x56b3c423, 0xcfba9599, 0xb8bda50f,
0x2802b89e, 0x5f058808, 0xc60cd9b2, 0xb10be924, 0x2f6f7c87,
0x58684c11, 0xc1611dab, 0xb6662d3d, 0x76dc4190, 0x01db7106,
0x98d220bc, 0xefd5102a, 0x71b18589, 0x06b6b51f, 0x9fbfe4a5,
0xe8b8e433, 0x7807c9a2, 0x0f00f934, 0x9609a88e, 0xe10e9818,
0x7f6a0dbb, 0x086d3d2d, 0x91646c97, 0xe6635c01, 0x6b6b51f4,
0x1c6c6162, 0x856530d8, 0xf262004e, 0x6c0695ed, 0x1b01a57b,
0x8208f4c1, 0xf50fc457, 0x65b0d9c6, 0x12b7e950, 0x8bbeb8ea,
0xfcb9887c, 0x62dd1ddf, 0x15da2d49, 0x8cd37cf3, 0xfbd44c65,
0x4db26158, 0x3ab551ce, 0xa3bc0074, 0xd4bb30e2, 0x4adfa541,
0x3dd895d7, 0xa4d1c46d, 0xd3d6f4fb, 0x4369e96a, 0x346ed9fc,
0xad678846, 0xda60b8d0, 0x44042d73, 0x33031de5, 0xaa0a4c5f,
0xdd0d7cc9, 0x5005713c, 0x270241aa, 0xbe0b1010, 0xc90c2086,
0x5768b525, 0x206f85b3, 0xb966d409, 0xce61e49f, 0x5edef90e,
0x29d9c998, 0xb0d09822, 0xc7d7a8b4, 0x59b33d17, 0x2eb40d81,
0xb7bd5c3b, 0xc0ba6cad, 0xedb88320, 0x9abfb3b6, 0x03b6e20c,
0x74b1d29a, 0xead54739, 0x9dd277af, 0x04db2615, 0x73dc1683,
0xe3630b12, 0x94643b84, 0x0d6d6a3e, 0x7a6a5aa8, 0xe40ecf0b,
0x9309ff9d, 0x0a00ae27, 0x7d079eb1, 0xf00f9344, 0x8708a3d2, 
0x1e01f268, 0x6906c2fe, 0xf762575d, 0x806567cb, 0x196c3671,
0x6e6b06e7, 0xfed41b76, 0x89d32be0, 0x10da7a5a, 0x67dd4acc,
0xf9b9df6f, 0x8ebeeff9, 0x17b7be43, 0x60b08ed5, 0xd6d6a3e8, 
0xa1d1937e, 0x38d8c2c4, 0x4fdff252, 0xd1bb67f1, 0xa6bc5767,
0x3fb506dd, 0x48b2364b, 0xd80d2bda, 0xaf0a1b4c, 0x36034af6,
0x41047a60, 0xdf60efc3, 0xa867df55, 0x316e8eef, 0x4669be79,
0xcb61b38c, 0xbc66831a, 0x256fd2a0, 0x5268e236, 0xcc0c7795,
0xbb0b4703, 0x220216b9, 0x5505262f, 0xc5ba3bbe, 0xb2bd0b28,
0x2bb45a92, 0x5cb36a04, 0xc2d7ffa7, 0xb5d0cf31, 0x2cd99e8b,
0x5bdeae1d, 0x9b64c2b0, 0xec63f226, 0x756aa39c, 0x026d930a,
0x9c0906a9, 0xeb0e363f, 0x72076785, 0x05005713, 0x95bf4a82,
0xe2b87a14, 0x7bb12bae, 0x0cb61b38, 0x92d28e9b, 0xe5d5be0d, 
0x7cdcefb7, 0x0bdbdf21, 0x86d3d2d4, 0xf1d4e242, 0x68ddb3f8,
0x1fda836e, 0x81be16cd, 0xf6b9265b, 0x6fb077e1, 0x18b74777,
0x88085ae6, 0xff0f6a70, 0x66063bca, 0x11010b5c, 0x8f659eff,
0xf862ae69, 0x616bffd3, 0x166ccf45, 0xa00ae278, 0xd70dd2ee, 
0x4e048354, 0x3903b3c2, 0xa7672661, 0xd06016f7, 0x4969474d,
0x3e6e77db, 0xaed16a4a, 0xd9d65adc, 0x40df0b66, 0x37d83bf0,
0xa9bcae53, 0xdebb9ec5, 0x47b2cf7f, 0x30b5ffe9, 0xbdbdf21c,
0xcabac28a, 0x53b39330, 0x24b4a3a6, 0xbad03605, 0xcdd70693,
0x54de5729, 0x23d967bf, 0xb3667a2e, 0xc4614ab8, 0x5d681b02,
0x2a6f2b94, 0xb40bbe37, 0xc30c8ea1, 0x5a05df1b, 0x2d02ef8d 
};


	aux = 0;
	while (n-- > 0) {
		/* compute the index to the crc table */
		i = (s >> 24) ^ ((unsigned int)(*b++));

		if (0 == i) {
			/* replace an intermediate zero with the
			   next value from the sequence */
			i = aux++;
			if (aux >= sizeof(crctab)/sizeof(crctab[0]))
				aux = 0;
		}

		/* new checksum value */
		s = ((s << 8) & 0xffffffffUL) ^ crctab[i];
	}
	return s;

}


/*
 * Function Name: create_undo_files
 *
 * Description: This routine creates the undo files needed should the
 *              user wish to undo the changes made by fsck.
 *
 * There are two undo files created, a data file and an index file.
 * For each modified page in the write cache, the corresponding
 * original page is read in from the disk, then written out to the
 * undo data file.  For each entry written to the undo data file, a
 * corresponding entry is written to the undo index file.  Each index
 * entry contains the an undo page number, the volume number, volume
 * LBN number where the page starts, and a crc32 for the saved undo
 * page.  This continues until a page and index entry are written for
 * each modified page in the write cache.
 *
 * Input parameters:
 *     domain: The domain structure for which undo files need to be created.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or FAILURE.
 * 
 */
int 
create_undo_files(domainT *domain)
{
    char	*funcName = "create_undo_files";
    int		version;
    char	undofile[MAXPATHLEN];
    char	indexfile[MAXPATHLEN];
    char	tmpUndo[MAXPATHLEN];
    char	tmpIndex[MAXPATHLEN];
    int		status;
    int		undofd;
    int		indexfd;
    int         writeDone;
    pageT	*pPage;
    nodeT	*pNode;
    messageT	*pMessage;
    messageT	*prevMessage;
    pageBufT	origPageBuf;
    off_t	bytes;
    unsigned int crc;
    char	tmpBuf[MAXPATHLEN];
    uint64_t   	undopage;
    struct stat	statbuf;
    char        answer[10];

    undofd  = -1;
    indexfd = -1;
    undopage = 0;

    /*
     * If there is nothing in the write cache we're done.
     */
    status = find_first_node(cache->writeCache, (void *)&pPage, &pNode);
    if (status != SUCCESS) {
	if (status == NOT_FOUND) {
	    return SUCCESS;
	} else {
	    return status;
	}
    }	

    if (Options.nochange == FALSE) {
	/* 
	 * The undo file naming convention has the version number as
	 * the last character of the filename.  The versions go from
	 * 0-9 and then wrap.  Nine version files can co-exist and
	 * there is always one file missing in the sequence.  The last
	 * file before the missing number is the most recent
	 * version. (ex: if file versions 0-3 and 5-9 exist, version 3
	 * is the latest version.)
	 */

	/*
	 * Find the version number gap.
	 */

	for (version = 0 ; version <= 9 ; version++) {
	    sprintf(undofile, "%s/undo.%s.%01d", Options.dirPath,
		    domain->dmnName, version);
	    sprintf(indexfile, "%s/undoidx.%s.%01d", Options.dirPath,
		    domain->dmnName, version);

	    if (stat(undofile, &statbuf) != 0 ||
		stat(indexfile, &statbuf) != 0) 
	    {
		break;
	    }
	}

	/* 
	 * Make sure that neither of the files we're about to create
	 * already exist. */	

	if (stat(undofile, &statbuf) == 0) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_991, 
			     "File '%s' already exists.\n"), 
		     undofile);
	    return FAILURE;
	}

	if (stat(indexfile, &statbuf) == 0) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_991, 
			     "File '%s' already exists.\n"), 
		     indexfile);
	    return FAILURE;
	}

	/*
	 * Now delete the version file following the one we're about
	 * to create.
	 */

	sprintf(tmpUndo, "%s/undo.%s.%01d", Options.dirPath,
		domain->dmnName, (version+1) % 10);
	sprintf(tmpIndex, "%s/undoidx.%s.%01d", Options.dirPath,
		domain->dmnName, (version+1) % 10);

	writemsg(SV_DEBUG, catgets(_m_catd, S_FSCK_1, FSCK_815,
				   "Unlinking files '%s' and '%s'\n"),
		 tmpUndo,tmpIndex);

	/*
	 * Ignore return code - it will fail if the file doesn't exist.
	 */
	unlink(tmpUndo);
	unlink(tmpIndex);

	/* Create the new undo files. */

	undofd = open(undofile, O_RDWR | O_CREAT | O_EXCL, 0600);
	if (undofd == -1) {
	    /* 
	     * Already checked for file existing - Unknown error. 
	     */
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_816, 
			     "Can't open file '%s'.\n"), 
		     undofile);
	    perror(Prog);
	    return FAILURE;
	}

	indexfd = open(indexfile, O_RDWR | O_CREAT | O_EXCL, 0600);
	if (indexfd == -1) {
	    /* 
	     * Already checked for file existing - Unknown error. 
	     */
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_816,
			     "Can't open file '%s'.\n"), 
		     indexfile);
	    perror(Prog);
	    close(undofd);
	    undofd = -1;
	    return FAILURE;
	}	

	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_817, 
			 "Created undo files '%s' and '%s'\n"),
		          undofile, indexfile);
    } /* if nochange == FALSE */

    status = find_first_node(cache->writeCache, (void *)&pPage, &pNode);
    if (status != SUCCESS) {
	/*
	 * Not found is caught above
	 */
	return status;
    }

    /*
     * loop through all the pages in the write cache
     */

    while (pPage != NULL) {

	if (P_IS_MODIFIED(pPage->status) == FALSE) {
	    status = find_next_node(cache->writeCache, (void *)&pPage, &pNode);
	    if (status == FAILURE) {
		close(undofd);
		close(indexfd);
		return status;
	    }
	    continue;
	}
	
	/*
	 * Each page might have messages attached to it,
	 * print them to the log.
	 */
	pMessage = pPage->messages;
	while (pMessage != NULL) {
	    if (pMessage->message != NULL) {
		char diskArea[10];
		
		switch (pMessage->diskArea) {
		    case(RBMT):
			strcpy(diskArea, 
			       catgets(_m_catd, S_FSCK_1, FSCK_818,
				       "RBMT"));
			break;
		    case(BMT):
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_819,
				       "BMT"));
			break;
		    case(SBM):
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_820,
				       "SBM"));
			break;
		    case(log):
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_821,
				       "LOG"));
			break;
		    case(tagFile):
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_823,
				       "TAG FILE"));
			break;
		    case(dir):
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_824, 
				       "DIR"));
			break;
		    case(tag):
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_825,
				       "TAG"));
			break;
		    case(misc):
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_826,
				       "MISC"));
			break;
		    default:
			strcpy(diskArea,
			       catgets(_m_catd, S_FSCK_1, FSCK_732,
				       "UNKNOWN"));
			break;
		} /* end of switch */

		writelog(SV_LOG_FIXED, 
			 "%s | %s:%d | %s\0",
			 diskArea, domain->volumes[pPage->vol].volName,
			 pPage->pageLBN, pMessage->message);
		free(pMessage->message);
	    } /* message existed */
	    prevMessage = pMessage;
	    pMessage = pMessage->next;
	    free(prevMessage);
	} /* while more message exist */
	pPage->messages = NULL;

	if (Options.nochange == TRUE) {		  
	    /*
	     * No reason do the next section, so just continue.
	     */
	    status = find_next_node(cache->writeCache, 
				    (void *)&pPage, &pNode);
	    if (status == FAILURE) {
		return status;
	    }
	    continue;
	}

	/*
	 * Read the original page from disk.
	 */
	if ((off_t)-1 == lseek(domain->volumes[pPage->vol].volFD,
			       pPage->pageLBN * (int64_t)DEV_BSIZE,
			       SEEK_SET)) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_192, 
			     "Can't lseek to block %ld on '%s'.\n"),
		     pPage->pageLBN, 
		     domain->volumes[pPage->vol].volName);
	    perror(Prog);
	    corrupt_exit(NULL);
	}

	bytes = read(cache->domain->volumes[pPage->vol].volFD,
		     origPageBuf, ADVFS_METADATA_PGSZ);
	    
	if ((off_t)-1 == bytes) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_193,
			     "Can't read page at block %ld on '%s'.\n"), 
		     pPage->pageLBN, 
		     domain->volumes[pPage->vol].volName);
	    perror(Prog);
	    corrupt_exit(NULL);
	} else if (bytes != ADVFS_METADATA_PGSZ) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_194, 
			     "Can't read page at block %ld on '%s', only read %ld of %d bytes.\n"),
		     pPage->pageLBN, domain->volumes[pPage->vol].volName,
		     bytes, ADVFS_METADATA_PGSZ);
	    corrupt_exit(NULL);
	}

	/* Calculate a validation checksum for this page. */
	crc = crc_32((unsigned char *)origPageBuf, ADVFS_METADATA_PGSZ, 0);

	sprintf(tmpBuf, "%ld\t%d\t%ld\t%08x\n", undopage++,
		pPage->vol, pPage->pageLBN, crc);

	/*
	 * write the undo index file.
	 */
	writeDone = FALSE;
	while (writeDone == FALSE) {
	    bytes = write(indexfd, tmpBuf, strlen(tmpBuf));
	    if (strlen(tmpBuf) == bytes) {
		writeDone = TRUE;
	    } else if ((bytes == -1) && (ENOSPC != errno)) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_827,
				 "Can't write to '%s'.\n"), indexfile);
		perror(Prog);
		close(undofd);
		close(indexfd);
		return FAILURE;
	    } else {
		if (bytes == -1) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_827,
				     "Can't write to '%s'.\n"), indexfile);
		    perror(Prog);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_828, 
				     "Can't write to '%s', only wrote %d of %d bytes.\n"), 
			     indexfile, bytes, strlen(tmpBuf));
		}
		writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
			 catgets(_m_catd, S_FSCK_1, FSCK_829, 
				 "This could be because the domain is out of space.\n"));
		if (Options.preen) {
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}

		writemsg(SV_ERR | SV_CONT,
			 catgets(_m_catd, S_FSCK_1, FSCK_830,
				 "Do you want to add more space and continue? (y/[n])  "));

		status = prompt_user_yes_no(answer, "n");
		if (status != SUCCESS) {
		    failed_exit();
		}
		/*
		 * answer[0] must be 'y' or 'n'.
		 */
		if (answer[0] == 'n') {
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}
		
		/*	
		 * lseek backwards over the partial write.
		 */
		if ((off_t)-1 == lseek(indexfd, (-1 * bytes), SEEK_SET)) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FSCK_1, FSCK_192,
				     "Can't lseek to block %ld on '%s'.\n"),
			     pPage->pageLBN, 
			     domain->volumes[pPage->vol].volName);
		    perror(Prog);
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}
	    } /* end if bytes not equal to strlen */
	} /* end while writeDone */

	/*
	 * write the undo file entry
	 */
	writeDone = FALSE;
	while (writeDone == FALSE) {
	    bytes = write(undofd, origPageBuf, ADVFS_METADATA_PGSZ);
	    if (bytes == ADVFS_METADATA_PGSZ) {
		writeDone = TRUE;
	    } else if ((bytes == -1) && (ENOSPC != errno)) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_827,
				 "Can't write to '%s'.\n"), undofile);
		perror(Prog);
		close(undofd);
		close(indexfd);
		return FAILURE;
	    } else {
		if (bytes == -1) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_827,
				     "Can't write to '%s'.\n"), undofile);
		    perror(Prog);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_828, 
				     "Can't write to '%s', only wrote %d of %d bytes.\n"), 
			     undofile, bytes, strlen(tmpBuf));
		}
		writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
			 catgets(_m_catd, S_FSCK_1, FSCK_829,
				 "This could be because the domain is out of space.\n"));
		if (Options.preen) {
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}

		writemsg(SV_ERR | SV_CONT,
			 catgets(_m_catd, S_FSCK_1, FSCK_830,
				 "Do you want to add more space and continue? (y/[n])  "));

		status = prompt_user_yes_no(answer, "n");
		if (status != SUCCESS) {
		    failed_exit();
		}
		/*
		 * answer[0] must be 'y' or 'n'.
		 */
		if (answer[0] == 'n') {
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}
		
		/*
		 * lseek backwards over the partial write.
		 */
		if ((off_t)-1 == lseek(indexfd, (-1 * bytes), SEEK_SET)) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FSCK_1, FSCK_192,
				     "Can't lseek to block %ld on '%s'.\n"),
			     pPage->pageLBN, 
			     domain->volumes[pPage->vol].volName);
		    perror(Prog);
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}
	    } /* end if bytes not equal to strlen */
	} /* end while writeDone */

	status = find_next_node(cache->writeCache, (void *)&pPage, &pNode);
	if (status == FAILURE) {
	    close(undofd);
	    close(indexfd);
	    return status;
	}
    } /* End of loop on all pages in write cache */

    if (Options.nochange == FALSE) {		  
	fsync(undofd);
	fsync(indexfd);
	close(undofd);
	close(indexfd);
    }

    /* FUTURE: Read the undofile and verify checksums? */

    return SUCCESS;
} /* end create_undo_files */


/* 
 * Function Name: write_cache_to_disk
 *
 * Description: This routine flushes the write cache to disk. It is
 *              assumed to be called only after the undo files have
 *              been completely written.
 *
 * Input parameters: N/A
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or FAILURE.
 *
 */
int 
write_cache_to_disk(void)
{
    char	*funcName = "write_cache_to_disk";
    int		status;
    pageT	*pPage;
    nodeT	*pNode;
    domainT     *domain;
    ssize_t	bytes;
    int		vol;
    struct sigaction ignAction;
    struct sigaction saveAction;
    
    domain = cache->domain;

    status = find_first_node(cache->writeCache, (void *)&pPage, &pNode);
    if (status != SUCCESS) {
	if (NOT_FOUND == status) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FSCK_1, FSCK_831,
			     "Storage domain '%s' was not modified.\n"));
	    return SUCCESS;
	} else {
	    return status;
	}
    }

    /* ignore signals so that we can't be interrupted while writing
       the new metadata */

    ignAction.sa_handler = SIG_IGN;
    memset(&ignAction.sa_mask, 0, sizeof(sigset_t));
    ignAction.sa_flags   = 0;

    sigaction(SIGINT, &ignAction, &saveAction);
    sigaction(SIGQUIT, &ignAction, &saveAction);

    domain->status |= DMN_META_WRITTEN;

    /* write out the new metadata pages */

    while (pPage != NULL) {
	if (P_IS_MODIFIED(pPage->status) == FALSE) {
	    status = find_next_node(cache->writeCache, (void *)&pPage, 
				    &pNode);
	    if (status == FAILURE) {
		goto err;
	    }
	    continue;
	}

	/*
	 * Lseek to the correct page on disk.
	 */
	if ((off_t)-1 == lseek(domain->volumes[pPage->vol].volFD,
			       pPage->pageLBN * (int64_t)DEV_BSIZE,
			       SEEK_SET)) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_192,
			     "Can't lseek to block %ld on '%s'.\n"),
		     pPage->pageLBN,
		     domain->volumes[pPage->vol].volName);
	    perror(Prog);
	    status = FAILURE;
	    goto err;
	}

	/*
	 * Write the new page to disk.
	 */
	bytes = write(domain->volumes[pPage->vol].volFD,
		      pPage->pageBuf, ADVFS_METADATA_PGSZ);
	if (bytes == -1) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_827,
			     "Can't write to '%s'.\n"), 
		     domain->volumes[pPage->vol].volName);
	    perror(Prog);
	    goto err;
	} else if (bytes != ADVFS_METADATA_PGSZ) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_828,
			     "Can't write to '%s', only wrote %d of %d bytes.\n"), 
		     domain->volumes[pPage->vol].volName,
		     bytes, ADVFS_METADATA_PGSZ);
	    goto err;
	}

	status = find_next_node(cache->writeCache, (void *)&pPage, &pNode);
	if (FAILURE == status) {
	    goto err;
	}
    }
    for (vol = 1 ; vol <= domain->highVolNum ; vol++) {
	if (domain->volumes[vol].volFD != -1) {
	    fsync(domain->volumes[vol].volFD);
	}
    }

    sigaction(SIGINT, &saveAction, NULL);
    sigaction(SIGQUIT, &saveAction, NULL);
    return SUCCESS;

err:
    sigaction(SIGINT, &saveAction, NULL);
    sigaction(SIGQUIT, &saveAction, NULL);

    return status; 

} /* end write_cache_to_disk */


/*
 * Function Name: close_domain_volumes
 *
 * Description: This closes all the open volumes in the domain.
 *
 * Input parameters:
 *     domain: The domain structure which contains the volumes to close.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS
 *
 */
int close_domain_volumes(domainT *domain)
{
    char    *funcName = "close_domain_volumes";
    int     volNum;   /* loop counter */
    volumeT *volumes;
  
    volumes = domain->volumes;

    /*
     * Loop on all valid entries in domain.volumes[].
     */
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        /* 
         * If we have a file descriptor for the volume, then close it. 
         */
        
        if (volumes[volNum].volFD != -1) {
            close(volumes[volNum].volFD);
	    /*
	     * Do not free the volName because it may be needed later.
	     */
            volumes[volNum].volFD = -1;
        }
    }
    return SUCCESS;

} /* end close_domain_volumes */


/*
 * Function Name: activate_domain (3.17)
 *
 * Description: This routine activates the domain and open the
 *              non-snap filesets.  This serves as a test to 
 *              see if the fixup work we did was successful.
 *
 * Input parameters:
 *     domain: The domain to activate.
 *
 * Output parameters: N/A
 *
 * Side Effects:
 *	This routine could possibly cause a system or domain panic 
 *      if activation is not possible.
 *
 * Returns: SUCCESS or FAILURE
 *
 */
int 
activate_domain(domainT *domain)
{
    char	    *funcName = "activate_domain";
    char            answer[80];
    bfSetIdT        setID;
    bfSetParamsT    setParams;
    uint32_t        userId;
    uint64_t        setIdx;
    int             status;

    writemsg(SV_ERR, "\n");
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT, 
	     catgets(_m_catd, S_FSCK_1, FSCK_836, 
		     "*** WARNING! ***\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT, "\n");
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_837, 
		     "%s is about to activate this domain to ensure it has\n"), 
	     Prog);
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_838, 
		     "been correctly fixed.  There is a small chance this could\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_839, 
		     "cause a system panic, taking the system down.  It could\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_840, 
		     "also cause a domain panic, which would only affect this\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_841,
		     "domain, but not the rest of the system.  For more\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FSCK_1, FSCK_842, 
		     "information, see the %s(8) man page.\n"), Prog);
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT, "\n");

    if (!Options.preen) {

	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_843, 
			 "Do you want %s to activate this domain now? (y/[n])  "), 
		 Prog);

	status = prompt_user_yes_no(answer, "n");
	if ((status != SUCCESS) || (answer[0] == 'n')) {
	    writemsg(SV_VERBOSE | SV_LOG_WARN, 
		     catgets(_m_catd, S_FSCK_1, FSCK_844, 
			     "Not activating the domain.\n"));
	    return SUCCESS;
	}
    }
#ifdef DEBUG
    writemsg(SV_ERR | SV_LOG_ERR, 
	     catgets(_m_catd, S_FSCK_1, FSCK_847, 
		     "About to activate domain, if you want to continue,\n"));
    writemsg(SV_ERR | SV_CONT, 
	     catgets(_m_catd, S_FSCK_1, FSCK_833, 
		     "you must type in \"yes write it\" to do so: "));
    status = prompt_user(answer, 80);
    if (status != SUCCESS) {
	answer[0] = '\0';
    }
#endif

    /* 
     * Fetch info on the domain for each fileset and get the fileset
     * IDs.  This will cause the kernel to activate the domain and
     * open each fileset as a side effect. 
     */

    setIdx = 0;            /* start with first file set */

    while ((status = advfs_fset_get_info(domain->dmnName, &setIdx,
					&setParams, &userId, 0)) 
	   != EOK) {

        if ((advfs_fset_get_id(domain->dmnName, setParams.setName, &setID))
	    != EOK) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_915, 
			     "domain activation error = %s\n"), 
		     BSERRMSG(status));
	    return FAILURE;
	}
    }

    if (status != E_NO_MORE_SETS) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_915, 
			 "domain activation error = %s\n"), 
		 BSERRMSG(status));
	return FAILURE;
    }

    writemsg(SV_NORMAL | SV_LOG_INFO,
	     catgets(_m_catd, S_FSCK_1, FSCK_855, 
		     "Successfully activated the domain.\n"));

    return SUCCESS;
} /* end activate_domain */

/*
 * Function Name: pre_activate_domain
 *
 * Description: This routine is called in order to trigger a log 
 *              playback and for the deferred deletes to be processed.
 *              The purpose is to make the domain ready to mount.
 *              AdvFS doesn't "mark clean" an FS, but
 *              we can make it clean by playing back the log and
 *              deferred delete processing.  This is done here by
 *              triggering domain activation.
 *
 * Input parameters:
 *     domain: The domain to activate.
 *
 * Output parameters: N/A
 *
 * Side Effects:
 *	This routine could possibly cause a system or domain panic 
 *      if activation is not possible.
 *
 * Returns: SUCCESS or FAILURE
 *
 */
int 
pre_activate_domain(domainT *domain)
{
    char	    *funcName = "pre_activate_domain";
    bfSetParamsT    setParams;
    uint32_t        userId;
    uint64_t        setIdx;
    int             status;

    /* 
     * Fetch info on the domain for each fileset and get the fileset
     * IDs.  This will cause the kernel to activate the domain and
     * open each fileset as a side effect. 
     */

    setIdx = 0;            /* default fileset */

    if ((status = advfs_fset_get_info(domain->dmnName, 
				     &setIdx,
				     &setParams, 
				     &userId, 0)) != EOK) {
	
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_970, 
			 "File system pre-activation error '%s' on '%s'\n"),
		 BSERRMSG(status), domain->dmnName);

	return FAILURE;
    }

    return SUCCESS;

} /* end pre_activate_domain */


/*
 * Function Name: restore_undo_files
 *
 * Description:This routine restores a domain to its state prior to a
 *             previous run of fcsk by reading that run's undo files.
 *
 * Input parameters:
 *     undo_file: The path to the undo file.
 *     undo_index_file: The path to the undo index file.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or FAILURE.
 * */
int 
restore_undo_files(domainT *domain)
{
    char	*funcName = "restore_undo_files";
    char	answer[10];
    int		version;
    char	undofile[MAXPATHLEN];
    char	indexfile[MAXPATHLEN];
    char	tmpUndo[MAXPATHLEN];
    char	tmpIndex[MAXPATHLEN];
    FILE	*idx;
    int		count;
    int		undofd;
    int		indexfd;
    pageBufT	origPageBuf;
    off_t	bytes;
    unsigned int crc; 
    unsigned int savedcrc;
    uint64_t   	undopage;
    int         vol;
    lbnNumT     lbn;
    struct stat	statbuf;
    int		status;

    undofd = -1;
    indexfd = -1;
    undopage = 0;

    for (version = 0 ; version <= 9 ; version++) {
	sprintf(undofile, "%s/undo.%s.%01d", Options.dirPath,
		domain->dmnName, version);
	sprintf(indexfile, "%s/undoidx.%s.%01d", Options.dirPath,
		domain->dmnName, version);

	if ((stat(undofile, &statbuf) == -1) ||
	    (stat(indexfile, &statbuf) == -1))
	{
	    /*
	     * This version doesn't exist, go on to the next.
	     */
	    continue;
	}

	/*
	 * Check if next one exists.
	 */
	if (version == 9) {
	    sprintf(tmpUndo, "%s/undo.%s.%01d", Options.dirPath,
		    domain->dmnName, 0);
	    sprintf(tmpIndex, "%s/undoidx.%s.%01d", Options.dirPath,
		    domain->dmnName, 0);
	} else {
	    sprintf(tmpUndo, "%s/undo.%s.%01d", Options.dirPath,
		    domain->dmnName, version + 1);
	    sprintf(tmpIndex, "%s/undoidx.%s.%01d", Options.dirPath,
		    domain->dmnName, version + 1);
	}

	if ((stat(tmpUndo, &statbuf) == -1) &&
	    (stat(tmpIndex, &statbuf) == -1))
	{
	    /*
	     * Next version doesn't exist.  Open this one, which does.
	     */
	    undofd = open(undofile, O_RDONLY, 0600);
	    if (undofd == -1) {
		/* 
		 * Already checked for file existing - Unknown error. 
		 */
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_816, 
				 "Can't open file '%s'.\n"), undofile);
		perror(Prog);
		return FAILURE;
	    }
	    indexfd = open(indexfile, O_RDONLY, 0600);
	    if (indexfd == -1) {
		/* 
		 * Already checked for file existing - Unknown error.
		 */
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_816,
				 "Can't open file '%s'.\n"), indexfile);
		perror(Prog);
		close(undofd);
		undofd = -1;
		return FAILURE;
	    }	
	    break;
	}
    }

    if (undofd == -1 || indexfd == -1) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_858,
			 "Can't find any undo files for '%s' in '%s'.\n"),
		 domain->dmnName, Options.dirPath);
	return FAILURE;
    } else {
	writemsg(SV_NORMAL |SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_859, 
			 "Restoring domain '%s' from files '%s' and '%s'.\n"),
		 domain->dmnName, undofile, indexfile);
    }
    /*
     * Do an fdopen on the index file descriptor so that fscanf will work.
     */
    idx = fdopen(indexfd, "r");

    /*
     * FUTURE: Check if domain mounted more recently than undo files.
     */

    count = fscanf(idx, "%ld\t%d\t%ld\t%08x\n",
		    &undopage, &vol, &lbn, &savedcrc);
    while (count == 4) {
	bytes = read(undofd, origPageBuf, ADVFS_METADATA_PGSZ);
	if (bytes == (off_t)-1) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_860, 
			     "Can't read page at byte %ld of '%s'.\n"), 
		     (undopage * ADVFS_METADATA_PGSZ), undofile);
	    perror(Prog);
	    return FAILURE;
	} else if (bytes != ADVFS_METADATA_PGSZ) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_861, 
			     "Can't read page at byte %ld of '%s', %d of %d bytes read.\n"),
		     (undopage * ADVFS_METADATA_PGSZ), undofile, 
		     bytes, ADVFS_METADATA_PGSZ);
	    return FAILURE;
	}

	crc = crc_32((uchar_t *)origPageBuf, ADVFS_METADATA_PGSZ, 0);

	if (crc != savedcrc) {
	    writemsg(SV_ERR | SV_LOG_WARN, 
		     catgets(_m_catd, S_FSCK_1, FSCK_862, 
			     "WARNING: checksum failed for undopage %ld: disk volume %s, LBN %ld.\n"),
		     undopage, domain->volumes[vol].volName, lbn);
	    writemsg(SV_ERR | SV_CONT, "\n");

	    if (Options.preen)
		return FAILURE;

	    writemsg(SV_ERR | SV_CONT, 
		     catgets(_m_catd, S_FSCK_1, FSCK_863, 
			     "Do you want to continue restoring the undo file? (y/[n])  "));
	    status = prompt_user_yes_no(answer, "n");
	    if (status != SUCCESS) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_864,
				 "Stopping restore of undo file.\n"));
		return status;
	    }
	    if ('n' == answer[0]) {
		return FAILURE;
	    }
	}

	/*
	 * Lseek to spot to write to.
	 */
	if ((off_t)-1 == lseek(domain->volumes[vol].volFD,
			       lbn * (int64_t)DEV_BSIZE,
			       SEEK_SET)) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_192, 
			     "Can't lseek to block %ld on '%s'.\n"),
		     lbn, domain->volumes[vol].volName);
	    perror(Prog);
	    return FAILURE;
	}
	/*
	 * Blast the original page back to disk.
	 */
	bytes = write(domain->volumes[vol].volFD,
		      origPageBuf, ADVFS_METADATA_PGSZ);
	if (bytes == (ssize_t)-1) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_827, 
			     "Can't write to '%s'.\n"), 
		     domain->volumes[vol].volName);
	    perror(Prog);
	    return FAILURE;
	} else if (bytes != ADVFS_METADATA_PGSZ) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_828, 
			     "Can't write to '%s', only wrote %d of %d bytes.\n"), 
		     domain->volumes[vol].volName, bytes, ADVFS_METADATA_PGSZ);
	    return FAILURE;
	}

	count = fscanf(idx, "%ld\t%d\t%ld\t%08x\n",
			&undopage, &vol, &lbn, &savedcrc);
    }
    if (count != EOF) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_865, 
			 "Can't scan file '%s' for data.\n"),
		 indexfile);
	perror(Prog);
	return FAILURE;
    }
    close(undofd);
    close(indexfd);
    for (vol = 1 ; vol <= domain->highVolNum ; vol++) {
	if (domain->volumes[vol].volFD != -1) {
	    fsync(domain->volumes[vol].volFD);
	}
    }

    return SUCCESS;

} /* end restore_undo_files */


/*
 * Function Name: convert_page_to_lbn
 *
 * Description: 
 * This function converts a page location within a METADATA
 * file/buffer to an addressable volume/logical block location.
 *
 * Input parameters:
 *  extentArray: Array of extent entries for the file/buffer.
 *  extentArraySize: Number of extent recs in extentArray.
 *  page: The page number for which we are looking.
 *
 * Output parameters:
 *  lbn: The logical block number on the volume where the page is located.
 *  vol: The volume number on which the page is located.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

int 
convert_page_to_lbn(fobtoLBNT *extentArray,
		    xtntNumT  extentArraySize,
		    pageNumT  page,
		    lbnNumT   *lbn,
		    volNumT   *vol)
{
    char   *funcName = "convert_page_to_lbn";
    uint64_t min;
    uint64_t max;
    uint64_t current;
    uint64_t fobDifference;
    bf_fob_t fob;
    int      found;

    *lbn = -1;
    *vol = -1;

    /*
     * Initialize our search bounds.
     */
    min = 0;
    max = extentArraySize;
    current = (min + max) / 2;

    fob = PAGE2FOB(page);

    if ((fob >= extentArray[max].fob) || (fob == XTNT_TERM)) {
        /*
	 * FOB is out of bounds
	 */
         writemsg(SV_DEBUG | SV_LOG_INFO, 
                  catgets(_m_catd, S_FSCK_1, FSCK_866,
			  "Can't convert FOB %ld to an LBN, valid range is 0 to %d.\n"),
		  fob, extentArray[max].fob);
         return FAILURE;
    }

    /*
     * Step 1 : Locate extent which contains page.
     */
    found = FALSE;
    while (found == FALSE) {
        if (min == max) {
	    /*
	     * Check if min = max : Found FOB
	     */
	     current = min;
	     found = TRUE;
	} else if (fob >= extentArray[current].fob &&
		   fob < extentArray[current+1].fob) {
	    /*
	     * Check if FOB falls between current and current+1 : Found FOB
	     */
	     found = TRUE;
	} else if (fob > extentArray[current].fob) {
	    /*
	     * Cut our search area in 1/2 for next pass, trim lower half.
	     */
	    min = current;	
	    current = (min + max) / 2;	    
	} else {
	    /*
	     * Cut our search area in 1/2 for next pass, trim upper half
	     */
	    max = current;
	    current = (min + max) / 2;	    
	}
    } /* end while */

    /*
     * Step 2 : Calculate lbn of page from extent
     */
    if (extentArray[current].lbn != XTNT_TERM)  {
        fobDifference = fob - extentArray[current].fob;
	*lbn = extentArray[current].lbn + 
	       (fobDifference * ADVFS_FOBS_PER_DEV_BSIZE);
    } else {
        /*
	 * Sparse Hole, or Missing extent which has been filled.
	 */
	*lbn = extentArray[current].lbn;
    }
    *vol = extentArray[current].volume;

    return SUCCESS;
} /* end convert_page_to_lbn */


/*
 * Function Name: predict_memory_usage
 *
 * Description:
 *  This function attempts to malloc all the memory that will be
 *  needed by fsck, called early on so that fsck can exit early
 *  if not enough memory/swap/kernel parameters are set.
 *
 *  mcellList needs the number of mcells in the BMT, plus allocation
 *    for the skiplist node structure.  20.5 is the average for all
 *    levels of the dynamically-sized skiplist.
 *  tagLBN needs 24 bytes per extent of each tag file.
 *  tagList needs 116.5 bytes per tag, 96 for the tagNode, plus 20.5 for the 
 *  	skiplist node.
 *  
 *  The number of tags can be estimated by dividing the number of
 *  mcells by 2.
 *  
 *  This routine should be able to prevent 99% of memory problems a
 *  user will encounter.  However, it's entirely possible another
 *  process could chew up memory while fsck is running after it has
 *  made its prediction, thus allowing malloc to fail after fsck has
 *  been running for a long time.
 *  
 *  Fsck will call its own wrapper function instead of malloc, which
 *  will check for a failure.  If malloc fails, this wrapper will
 *  prompt the user to add swap space and/or kill off large memory
 *  usage processes other than fsck, and/or some other memory saving
 *  procedure, then make another attempt to malloc the needed memory.
 *  This can repeat until the user decides to tell fsck not to retry.
 *  
 *
 * Input parameters:
 *  domain: The domain to predict memory usage of.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

int 
predict_memory_usage(domainT *domain)
{
    char	*funcName = "predict_memory_usage";
    int64_t	predictedMemUsage;
    tagNumT	numTags;
    cellNumT	numMcells;
    volNumT	volNum;
    int		status;
    char	answer[10];

    predictedMemUsage = 0;
    numMcells = 0;

    /*
     * Calculate maximum number of mcells in this domain.
     */
    for (volNum = 0 ; volNum <= domain->highVolNum ; volNum++) {

	if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}

	numMcells += FOB2PAGE(domain->volumes[volNum].volRbmt->bmtLBN[domain->volumes[volNum].volRbmt->bmtLBNSize].fob) * BSPG_CELLS;
    }

    /*
     * Calculate best guess of number of tags in this domain.
     * See function description above for how the numbers 2 and 1 were
     * determined.
     */
    numTags = numMcells / 2;

    /*
     * Get mcell usage.
     *
     * 64 is the smallest malloc bucket size that fits sizeof(mcellNodeT),
     * which is currently 48.
     *
     * 20.5 is the average number of bytes per node needed for all levels of
     * the skiplist structure.
     *
     * 8 is overhead used by malloc.
     */
    predictedMemUsage += numMcells * (64 + 20.5 + 8);
    /*
     * Get tag usage
     *
     * 128 is the smallest malloc bucket size that fits sizeof(tagNodeT),
     * which is currently 96.
     *
     * 20.5 is the average number of bytes per node needed for all levels of
     * the skiplist structure.
     *
     * 8 is overhead used by malloc.
     */
    predictedMemUsage += numTags * (128 + 20.5 + 8);

    /*
     * Now that we have a memory prediction, malloc it.
     */
    if (predictedMemUsage > 0) {
	char *pMemory;

	/*
	 * Use regular malloc instead of ffd_malloc, because failures are
	 * handled differently here.
	 */
	pMemory = malloc(predictedMemUsage);

	if (pMemory == NULL) {
	    writemsg(SV_ERR | SV_LOG_WARN,
		     catgets(_m_catd, S_FSCK_1, FSCK_867,
			     "Failed to malloc predicted memory usage of %ld bytes.\n"),
		     predictedMemUsage);

	    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_874, 
			     "Check for other processes (besides %s) which are\n"), 
		     Prog);
	    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_875, 
			     "using large amounts of memory, and kill them if\n"));
	    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_876, 
			     "possible, or increase swap space.\n"));

	    if (Options.preen)
		return FAILURE;

	    writemsg(SV_ERR | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_871, 
			     "\nDo you want to continue running %s anyway? (y/[n])  "), Prog);

	    status = prompt_user_yes_no(answer, "n");
	    if (status != SUCCESS || 'n' == answer[0]) {
		return FAILURE;
	    }
	} else {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FSCK_1, FSCK_872, 
			     "Predicted bytes needed: %d. Allocation test was successful.\n"),
		     predictedMemUsage);

	    free(pMemory);
	}
    }

    return SUCCESS;
} /* end predict_memory_usage */


/*
 * Function Name: ffd_malloc (3.nn)
 *
 * Description:
 *  This function is a malloc wrapper that will prompt the user if the
 *  malloc call fails, and will reattempt the malloc assuming the user
 *  can either kill off other memory intensive processes or add swap
 *  space if one of those is the problem preventing the malloc.
 *
 *
 * Input parameters:
 *  size: The number of bytes to malloc.
 *
 * Returns:
 *  A pointer to the malloc'ed memory, or NULL if malloc failed.
 */

void *ffd_malloc(size_t size)
{
    char	*funcName = "ffd_malloc";
    void	*ptr;
    int		fail;
    int		status;
    char	answer[10];

    fail = FALSE;

    ptr = malloc(size);

    while (ptr == NULL && fail == FALSE) {
	writemsg(SV_ERR | SV_LOG_WARN,
		 catgets(_m_catd, S_FSCK_1, FSCK_873, 
			 "Can't allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_874, 
			 "Check for other processes (besides %s) which are\n"), 
		 Prog);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_875, 
			 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_876, 
			 "possible, or increase swap space.\n"));

	if (Options.preen) {
	    break;
	}

	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_880, 
			 "\nDo you want %s to try this allocation again? ([y]/n)  "), 
		 Prog);

	status = prompt_user_yes_no(answer, "y");
	if (status != SUCCESS || 'n' == answer[0]) {
	    fail = TRUE;
	}
	else {
	    ptr = malloc(size);
	}
    } /* End while loop */

    return ptr;
} /* end ffd_malloc */


/*
 * Function Name: ffd_calloc (3.nn)
 *
 * Description:
 *  This function is a calloc wrapper that will prompt the user if the
 *  calloc call fails, and will reattempt the calloc assuming the user
 *  can either kill off other memory intensive processes or add swap
 *  space if one of those is the problem preventing the calloc.
 *
 *
 * Input parameters:
 *  numElts: The number of elements to calloc.
 *  eltSize: The size of each element to calloc.
 *
 * Returns:
 *  A pointer to the calloc'ed memory, or NULL if calloc failed.
 */

void *ffd_calloc(size_t numElts, size_t eltSize)
{
    char	*funcName = "ffd_calloc";
    void	*ptr;
    int		fail;
    int		status;
    char	answer[10];

    fail = FALSE;

    ptr = calloc(numElts, eltSize);

    while (ptr == NULL && fail == FALSE) {
	writemsg(SV_ERR | SV_LOG_WARN,
		 catgets(_m_catd, S_FSCK_1, FSCK_873, 
			 "Can't allocate %ld bytes.\n"), 
		 numElts * eltSize);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_874, 
			 "Check for other processes (besides %s) which are\n"),
		 Prog);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_875, 
			 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_876, 
			 "possible, or increase swap space.\n"));

	if (Options.preen) {
	    break;
	}

	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_880, 
			 "\nDo you want %s to try this allocation again? ([y]/n)  "), 
		 Prog);

	status = prompt_user_yes_no(answer, "y");
	if (status != SUCCESS || 'n' == answer[0]) {
	    fail = TRUE;
	}
	else {
	    ptr = calloc(numElts, eltSize);
	}
    } /* End while loop */

    return ptr;
} /* end ffd_calloc */


/*
 * Function Name: ffd_realloc (3.nn)
 *
 * Description:
 *  This function is a realloc wrapper that will prompt the user if the
 *  realloc call fails, and will reattempt the realloc assuming the user
 *  can either kill off other memory intensive processes or add swap
 *  space if one of those is the problem preventing the realloc.
 *
 *
 * Input parameters:
 *  pointer: The originally malloc'ed pointer.
 *  size: The number of bytes to realloc.
 *
 * Returns:
 *  A pointer to the realloc'ed memory, or NULL if realloc failed.
 */

void *ffd_realloc(void *pointer, size_t size)
{
    char	*funcName = "ffd_realloc";
    void	*ptr;
    int		fail;
    int		status;
    char	answer[10];

    fail = FALSE;

    ptr = realloc(pointer, size);

    while (ptr == NULL && fail == FALSE) {
	writemsg(SV_ERR | SV_LOG_WARN,
		 catgets(_m_catd, S_FSCK_1, FSCK_873,
			 "Can't allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_874, 
			 "Check for other processes (besides %s) which are\n"),
		 Prog);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_875,
			 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_876,
			 "possible, or increase swap space.\n"));

	if (Options.preen) {
	    break;
	}

	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_880, 
			 "\nDo you want %s to try this allocation again? ([y]/n)  "), 
		 Prog);

	status = prompt_user_yes_no(answer, "y");
	if (status != SUCCESS || 'n' == answer[0]) {
	    fail = TRUE;
	}
	else {
	    ptr = realloc(pointer, size);
	}
    } /* End while loop */

    return ptr;
} /* end ffd_realloc */


/*
 * Function Name: add_fixed_message
 *
 * Description:
 *
 * Input parameters:
 *  pMessage: The pointer to a message structure.
 *  diskArea: location type of message
 *  fmt: The message to be added
 *  ...: variable arguments.
 *
 * Returns:
 * 	SUCCESS or NO_MEMORY
 */
int add_fixed_message(messageT **pMessage, locationT diskArea, 
		      char *fmt, ...)
{
    char *funcName = "add_fixed_message";
    va_list  args;
    messageT *msg;
    int      msgLen;
    char     *msgString;
    char newFmt[MAXERRMSG * 2]; /* larger than we would ever use */

    /*
     * Compute size of msgString and arguments.
     * As parsing variable arguments of different types is
     * a massive amount of work the easy way to do this is
     * put it in a temp area, then strlen the temp area.
     */
    va_start(args, fmt);
    vsprintf(newFmt, fmt, args);
    va_end(args);

    msgLen = strlen(newFmt);

    /*
     * Malloc space.
     */
    msg = (messageT *)ffd_malloc(sizeof(messageT));
    if (msg == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_881,
			 "log message"));
	return NO_MEMORY;
    }

    msgString = (char *)ffd_malloc(msgLen + 1);
    if (msgString == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_881, 
			 "log message"));
	return NO_MEMORY;
    }

    /*
     * Load the string with the variable arguments.
     */
    va_start(args, fmt);
    vsprintf(msgString, fmt, args);
    va_end(args);
    
    /*
     * Attach the message to the pMessage.
     */
    msg->diskArea = diskArea;
    msg->message = msgString;

#if 1 /*MSGORDER*/
    if (*pMessage == NULL) {
	msg->next = NULL;
    } else { 
	msg->next = *pMessage;
    }
    *pMessage = msg;
#else /*MSGORDER*/

    /*
     * Write the message immediately to the log.
     */
    {
	char diskArea[10];

	switch (msg->diskArea) {
	    case(RBMT):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_818, "RBMT"));
		break;
	    case(BMT):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_819, "BMT"));
		break;
	    case(SBM):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_820, "SBM"));
		break;
	    case(log):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_821, "LOG"));
		break;
	    case(tagFile):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_823, "TAG FILE"));
		break;
	    case(dir):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_824, "DIR"));
		break;
	    case(tag):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_825, "TAG"));
		break;
	    case(misc):
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_826, "MISC"));
		break;
	    default:
		strcpy(diskArea,
		       catgets(_m_catd, S_FSCK_1, FSCK_732, "UNKNOWN"));
		break;
	} /* end of switch */
	
	writelog(SV_LOG_FIXED, "%s | %s:%d | %s", diskArea, 
		 catgets(_m_catd, S_FSCK_1, FSCK_882, "volX"), 
		 12345, msg->message);
    }
#endif /*MSGORDER*/

    return SUCCESS;
} /* end add_fixed_message */


/*
 * Function Name: collect_rbmt_extents (3.n)
 *
 * Description: Collect the extents that map all RBMT pages.
 *
 *        NOTE: The calling function is responsible for freeing the
 *              extent array that is malloc'ed by this routine. 
 *
 * Input parameters:
 *     domain: The domain being loaded.
 *     volNum: The volume being loaded.
 *     collectSbm: Flag to let us know if we should add extents to SBM
 * 
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY
 *
 */
int 
collect_rbmt_extents(domainT *domain, 
		     volNumT volNum, 
		     int collectSbm)
{
    char     *funcName = "collect_rbmt_extents";
    int	     status;
    lbnNumT  currPageLBN;
    lbnNumT  nextPageLBN;
    tagNumT  tag;
    tagNumT  setTag;
    pageNumT pageNum;
    mcidT    mcell;
    volumeT  *volume;
    pageT    *pPage;
    bsMPgT   *pBMTPage;
    bsMCT    *pMcell;
    bsMRT    *pRecord;
    bsXtraXtntRT *pXtra;
    fobtoLBNT extentArray[2];
    
    /*
     * Init variables
     */
    volume = &(domain->volumes[volNum]);

    currPageLBN = RBMT_START_BLK;
    nextPageLBN = -1;
    volume->volRbmt->rbmtLBNSize = 1;

    /*
     * The last mcell in a an RBMT page (bsMCA[RBMT_RSVD_CELL]) contains
     * extra extents (if any) for the RBMT over and above what's in
     * Cell 0.  If there is another RBMT page, the last mcell points
     * to that page with it's nextMCId header entry.  That page would
     * contain more extra extents records. We count the pages in the
     * RBMT here in order to know how large an extent array to
     * allocate.
     */
    while (currPageLBN != XTNT_TERM) {
	status = read_page_from_cache(volNum, currPageLBN, &pPage);
	if (status != SUCCESS) {
	    return status;
	}
	pBMTPage = (bsMPgT *)pPage->pageBuf;
	pMcell   = &(pBMTPage->bsMCA[RBMT_RSVD_CELL]);
	pRecord  = (bsMRT *)pMcell->bsMR0;
	pXtra    = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));

	if ((pMcell->mcNextMCId.page != 0) ||
	    (pMcell->mcNextMCId.cell != 0)) {
	    nextPageLBN = pXtra->bsXA[0].bsx_vd_blk;
	    volume->volRbmt->rbmtLBNSize++;
	} else {
	    nextPageLBN = -1;
	}

	status = release_page_to_cache(volNum, currPageLBN,
				       pPage, FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	currPageLBN = nextPageLBN;
	nextPageLBN = -1;
    }

    /*
     * Finished counting pages.  Time to malloc the array.
     */
    volume->volRbmt->rbmtLBN =
	(fobtoLBNT *)ffd_calloc(volume->volRbmt->rbmtLBNSize+1,
				 sizeof(fobtoLBNT));
    if (volume->volRbmt->rbmtLBN == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_782,
			 "RBMT extents"));
	return NO_MEMORY;
    }

    /*
     * Now that the array is malloc'ed, fill it in.
     */
    currPageLBN = RBMT_START_BLK;
    nextPageLBN = -1;
    pageNum = 0;

    while (currPageLBN != XTNT_TERM) {
	status = read_page_from_cache(volNum, currPageLBN, &pPage);
	if (status != SUCCESS) {
	    return status;
	}
	    
	pBMTPage = (bsMPgT *)pPage->pageBuf;
	pMcell   = &(pBMTPage->bsMCA[RBMT_RSVD_CELL]);
	pRecord  = (bsMRT *)pMcell->bsMR0;
	pXtra    = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));

	volume->volRbmt->rbmtLBN[pageNum].volume = volNum;
	volume->volRbmt->rbmtLBN[pageNum].fob = PAGE2FOB(pageNum);
	volume->volRbmt->rbmtLBN[pageNum].lbn = currPageLBN;

	if (collectSbm == TRUE) {
	    tag        = pMcell->mcTag.tag_num;
	    setTag     = pMcell->mcBfSetTag.tag_num;
	    mcell.volume = volNum;
	    mcell.page = pageNum;
	    mcell.cell = 0;

	    /*
	     * Build up a dummy extent array to represent this RBMT
	     * page and get the corresponding bits set in our SBM map.
	     */
	    extentArray[0].volume = volNum;
	    extentArray[0].lbn    = currPageLBN;
	    extentArray[0].fob   = 0;
	    extentArray[1].volume = volNum;
	    extentArray[1].lbn    = -1;
	    extentArray[1].fob   = ADVFS_METADATA_PGSZ_IN_FOBS;

	    status = collect_sbm_info(domain, extentArray, 2, 
				      tag, setTag, &mcell, 0);
	    if (status != SUCCESS) {
		return status;
	    }
	}

	pageNum++;

	if ((pMcell->mcNextMCId.page != 0) ||
	    (pMcell->mcNextMCId.cell != 0)) {
	    nextPageLBN = pXtra->bsXA[0].bsx_vd_blk;
	} else {
	    nextPageLBN = -1;
	}
	    
	status = release_page_to_cache(volNum, currPageLBN,
				       pPage, FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	currPageLBN = nextPageLBN;
	nextPageLBN = -1;
    } /* End filling in array loop */

    volume->volRbmt->rbmtLBN[pageNum].volume = volNum;
    volume->volRbmt->rbmtLBN[pageNum].fob = PAGE2FOB(pageNum);
    volume->volRbmt->rbmtLBN[pageNum].lbn = -1;
    
    return SUCCESS;
} /* collect_rbmt_extents */


/*
 * Function Name: log_lsn_gt
 *
 * Description: Compare to two lsn values.  The problem with lsn is that
 *              they wrap so we need to take that into account. There
 *              is a maximum range that the lsn's can be apart from 
 *              one another so we can use this piece of knowledge to
 *              help solve the wrapping problem (see ms_logger.c).
 *
 * Input parameters:
 *      lsn1 
 *      lsn2
 * 
 * Output parameters: N/A
 *
 * Returns: TRUE or FALSE
 */

int 
log_lsn_gt(lsnT lsn1, 
	   lsnT lsn2)
{
    uint32_t breakPoint;

    if (lsn1.num == NULL) {
	return FALSE;
    }

    if (lsn2.num == NULL) {
	return TRUE;
    }

    /*
     * This version uses the 'magic lsn range' to determine whether 
     * signed or unsigned lsn compares are appropriate.
     */

    breakPoint = 1<<29;

    if ((lsn1.num < breakPoint) || (lsn1.num > -breakPoint)) {
        /* Must use signed compares */
        return( (int) lsn1.num > (int) lsn2.num );
    } else {
        return( lsn1.num > lsn2.num );
    }
}
