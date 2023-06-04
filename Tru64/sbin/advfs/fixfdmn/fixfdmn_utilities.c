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
 *
 *      Mon Jan 10 13:48:14 PST 2000
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fixfdmn_utilities.c,v $ $Revision: 1.1.37.7 $ (DEC) $Date: 2007/07/27 04:03:57 $"

#include "fixfdmn.h"

/*
 * Global Variables
 */
extern cacheT *cache;
extern nl_catd _m_catd;

/*
 * Local static prototypes
 */

/*
 * Design Specification 3.*.* writemsg
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
void writemsg( int severity, char *fmt, ... )
{
    char *funcName = "writemsg";
    va_list args;
    char newFmt[MAXERRMSG * 2]; /* larger than we would ever use */
    int newFmtLen;
    char dateStr[32];
    int printDate;
    int isStderr;
    int isCont;
    int isLog;
    int i;

    /*
     * Initialize variables.
     */
    newFmtLen = 0;
    printDate = (severity & SV_DATE) > 0 ? TRUE : FALSE;
    isStderr  = (severity & SV_ERR)  > 0 ? TRUE : FALSE;
    isCont    = (severity & SV_CONT) > 0 ? TRUE : FALSE;
    isLog     = (severity & SV_LOG_MASK) > 0 ? TRUE : FALSE;
    
    /*
     * Does this message also go to the log?
     */
    va_start(args, fmt);
    if (TRUE == isLog) {
	writelog_real(severity, fmt, args);
    }
    va_end(args);

    /*
     * If the severity code indicates this message should not be printed,
     * get out.
     */
    if ((FALSE == isStderr) &&  
	((severity & SV_MASK) > Options.verboseOutput)) {
	return;
    }

    /*
     * Begin calculations for the required length for the expanded format
     * string.
     */
    newFmtLen = strlen(Prog) + 2 + strlen(fmt) + 1;

    /*
     * If date was requested, get it, and increment the required length of
     * the new format length.
     */
    if (TRUE == printDate) {
        get_timestamp( dateStr, sizeof(dateStr) );
        newFmtLen += strlen(dateStr) + 5;
    }

    /*
     * Make sure the message is not larger than our newFmt buffer.
     */
    if ((MAXERRMSG * 2) < newFmtLen) {
	int len;
	int tooLong;
	/*
	 * truncate the passed in message to make sure it fits.
	 */
	len = strlen(fmt);
	tooLong = newFmtLen - (MAXERRMSG * 2) - 1;
	fmt[len - tooLong] = '\0';
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_721, 
			 "Message to be printed is too long, truncating it.\n"));
    }

    /*
     * If appropriate, put the Prog name at the beginning.
     */
    if (FALSE == isCont) {
        strcpy(newFmt, Prog);
        strcat(newFmt, ": ");
    } else {
	strcpy(newFmt, "  ");
	for (i = 0 ; i < strlen(Prog) ; i++) {
	    strcat(newFmt, " ");
	}
    }

    /*
     * Add in the old format. Then set the old fmt pointer to the new
     * string (the compiler doesn't like a local variable being passed to
     * the "va" call).
     */
    strcat( newFmt, fmt );

    /*
     * If date was requested, add it on.
     */
    if (TRUE == printDate) {
        if ('\n' == newFmt[strlen(newFmt)-1]) {
	    if ('.' == newFmt[strlen(newFmt)-2]) {
		newFmt[strlen(newFmt)-2] = '\0'; /* end of string */
		strcat( newFmt, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_722,
					" at ") );
		strcat( newFmt, dateStr );
		strcat( newFmt, ".\n" );
	    } else {
		newFmt[strlen(newFmt)-1] = '\0'; /* end of string */
		strcat( newFmt, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_722,
					" at ") );
		strcat( newFmt, dateStr );
		strcat( newFmt, "\n" );
	    }
        } else {
	    if ('.' == newFmt[strlen(newFmt)-1]) {
		newFmt[strlen(newFmt)-1] = '\0'; /* end of string */
		strcat( newFmt, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_722,
					" at ") );
		strcat( newFmt, dateStr );
		strcat( newFmt, "." );
	    } else {
		strcat( newFmt, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_722,
					" at ") );
		strcat( newFmt, dateStr );
	    }
        }
    }


    /*
     * Print the message.
     */
    va_start(args, fmt);
    if (FALSE == isStderr) {
        vfprintf(stdout, newFmt, args);
    } else {
        vfprintf(stderr, newFmt, args);
    }
    va_end(args);
} /* end writemsg */


/*
 * Design Specification 3.*.* writelog
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
    va_list args;

    va_start(args, fmt);
    writelog_real(severity, fmt, args);
    va_end(args);
} /* end writelog */


/*
 * Design Specification 3.*.* writelog_real
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
void writelog_real(int severity, char *fmt, va_list args)
{
    char *funcName = "writelog_real";
    int  messageType;
    char dateStr[32];
    FILE *logHandle;

    if (NULL == Options.logHndl) {
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
    logHandle = Options.logHndl;

    /*
     * Output the time.
     */
    if (NULL == get_time(dateStr, sizeof(dateStr))) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_723, 
			 "Can't get the time to put in the log.\n"));
	fprintf(logHandle, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_724,
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
	    fprintf(logHandle, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_725, 
				       "INFO"));
	    break;	

	case (SV_LOG_SUM):
	    fprintf(logHandle, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_726,
				       "SUMMARY"));
	    break;
	    
	case (SV_LOG_FOUND):
	    fprintf(logHandle, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_727, 
				       "FOUND"));
	    break;

	case (SV_LOG_FIXED):
	    if (FALSE == Options.nochange) {		  
		fprintf(logHandle, 
			catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_728, 
				"FIXED"));
	    } else {
		fprintf(logHandle, 
			catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_729,
				"DISABLED"));
	    }
	    break;

	case (SV_LOG_WARN):
	    fprintf(logHandle, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_730,
				       "WARNING"));
	    break;

	case (SV_LOG_ERR):
	    fprintf(logHandle, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_731,
				       "ERROR"));
	    break;

	case (SV_LOG_DEBUG):
	    fprintf(logHandle,  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_910,
					"DEBUG"));
	    break;

	default:
	    fprintf(logHandle, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_732,
				       "UNKNOWN"));
	    break;
    } /* end switch */

    fprintf(logHandle, " | ");

    /*
     * Print out the message
     */
    vfprintf(logHandle, fmt, args);
} /* end writelog_real */


/*
 * Design Specification 3.*.* get_timestamp
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
    struct tm *dateTime;
    struct tm result;
    size_t retChars;
        
    t = time(0);
    dateTime = localtime_r(&t, &result); /* Thread Safe */
    retChars = strftime ( dateStr, maxLen, 
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_733, 
				  "%d-%b-%Y %T"), dateTime );

    if ( retChars > 0 ) {
        return dateStr;
    } else {
        return NULL;
    }
} /* end get_timestamp */




/*
 * Design Specification 3.*.* get_time
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
char * get_time(char *dateStr, size_t maxLen)
{
    char *funcName = "get_time";
    time_t t;
    struct tm *dateTime;
    struct tm result;
    size_t retChars;
        
    t = time(0);
    dateTime = localtime_r(&t, &result); /* Thread Safe */
    retChars = strftime ( dateStr, maxLen, "%T", dateTime );

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
int prompt_user(char *answer, int size)
{
    char *funcName = "prompt_user";

    if (!Localtty) { 
        /* can't prompt user; no local tty */
	writemsg(SV_ERR | SV_CONT, "\n");
	writemsg(SV_ERR, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_734, 
				 "Can't prompt user for answer.\n"));
        return FAILURE;
    }

    /*
     * We need an answer from the user.  If there is no way to
     * get one (Localtty is NULL), then we should have returned above.  
     */

    if (NULL == fgets (answer, size, Localtty)) {
	perror("prompt_user: fgets:");
	return FAILURE;
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
int prompt_user_yes_no(char *answer, char *defaultAns)
{
    char *funcName = "prompt_user_yes_no";
    char buffer[10];
    int  valid;
    int  status;

    valid = FALSE;

    /* 
     * Only possible values for Options.answer are 1,0,-1.
     */
    if (0 != Options.answer) {
	if (1 == Options.answer) {
	    strcpy(answer, "y");
	} else if (-1 == Options.answer) {
	    strcpy(answer, "n");
	}
	writemsg(SV_NORMAL | SV_CONT | SV_LOG_INFO, "\n");
	writemsg(SV_NORMAL | SV_LOG_INFO,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_735, 
			 "Using default answer of '%s' as specified on the command line.\n"), 
		 answer);
	return SUCCESS;
    }

    do {
	status = prompt_user(buffer, 10);
	if (SUCCESS != status) {
	    if ( errno == ENOMEM ) {
                perror("\n Unable to allocate memory in fgets");
                return status;
            }
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
	else if ('\n' == buffer[0]) {
	    answer[0] = defaultAns[0];
	    valid = TRUE;
	}
	else {
	    writemsg(SV_ERR | SV_CONT, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_736, 
			     "Please enter 'y' or 'n':  "));
	}
    } while (FALSE == valid);

    return SUCCESS;
} /* end prompt_user_yes_no */


/*
 * Function Name: prompt_user_to_continue (3.5)
 *
 * Description: After errors have been found and fixed, check if the user 
 *              wishes to continue looking for further errors.
 *
 * Input parameters:
 *     message: The message presented to user
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS
 *
 */
int prompt_user_to_continue (domainT *domain, char *message)
{
    char *funcName = "prompt_user_to_continue";
    int  cont;
    int  status;
    char answer[10];

    /*
     * Due to the extremely high probablity of cascading corruptions,
     * it has been decided to remove this prompt.  The code is still
     * here, however, in case we get severe customer complaint about
     * our not giving them enough rope to hang themselves and their
     * horses with.
     */
    return SUCCESS;

#if 0
    cont = FALSE;
    if (NULL != message) {
	writemsg(SV_ERR, message);
    }

    /*
     * Perform a check to see if user wishes to continue.
     */
    writemsg(SV_ERR, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_737, 
		     "Do you want %s to keep looking for more corruptions? ([y]/n)  "),
	     Prog);

    status = prompt_user_yes_no(answer, "y");
    if (SUCCESS != status) {
	writemsg(SV_ERR, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_738,
				 "Continuing %s.\n"), Prog);
	cont = TRUE;
    }

    if ('y' == answer[0]) {
	cont = TRUE;
    }

    /* 
     * If the user wants to terminate fixfdmn:
     */
    if (FALSE == cont) {
	/*
	 * Call clean_exit to cleanly exit the program.
	 */
	clean_exit(domain);
    }
    return SUCCESS;
#endif
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
int create_volumes(volumeT **volumes)
{
    char  *funcName = "create_volumes";
    int   counter;

    /* 
     * Malloc new structs 
     */
    *volumes = (volumeT *)ffd_calloc(MAX_VOLUMES, sizeof(volumeT));
    if (NULL == *volumes) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_740, 
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
        (*volumes)[counter].domainID.tv_sec  = 0;
        (*volumes)[counter].domainID.tv_usec = 0;
        (*volumes)[counter].dmnVersion       = 0;
        (*volumes)[counter].mountId.tv_sec   = 0;
        (*volumes)[counter].mountId.tv_usec  = 0;
        (*volumes)[counter].mntStatus        = 0;
        (*volumes)[counter].rootTag          = 0;
	(*volumes)[counter].volSbmInfo	     = NULL;
    } /* end for loop */

    return SUCCESS;
} /* end create_volumes */


/*
 * Function Name: get_domain_volumes (3.6)
 *
 * Description: Parse /etc/fdmns/domain_name to collect all the volumes in 
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
int get_domain_volumes(char *domainName, domainT *domain)
{
    char *funcName = "get_domain_volumes";
    DIR           *dmnDir;
    char          *dmnName;
    struct statfs *mountTbl;
    char          pathName[MAXPATHLEN];
    char          *mountDmnName;
    char          *mountBfSetName;
    struct dirent *dp;
    volumeT       *volumes;
    int           counter;
    int           i;
    int           lkHndl;   /* /etc/fdmns directory lock */
    int           dmnLock;  /* lock for specific domain */
    int           err;
    int           mountCnt;
    int           status;
    struct stat   statBuf;
    char          dirName[MAXPATHLEN];
    char          volName[MAXPATHLEN];
    char          rawName[MAXPATHLEN];
    char          *devType;
    unsigned int  volSize;
    int volBigFlag = 0;
    /*
     * Init variables
     */
    counter = 0;
    i = 0;
    mountTbl = NULL;
    devType = NULL;
    strcpy(pathName, "/etc/fdmns/");

    /*
     * Validate domain name, has been done prior to this call
     * so asserts are acceptable.
     */
    assert(NULL != domainName);
    assert(strlen(domainName) + strlen(pathName) <= MAXPATHLEN);

    /*
     * Append domainName to pathName.
     */
    strcat (pathName, domainName);
    writemsg(SV_DEBUG, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_741, 
			       "Opening directory '%s'.\n"), pathName);
    
    /*   
     * Grab the lock for the /etc/fdmns directory.
     */
    lkHndl = lock_file (MSFS_DMN_DIR, LOCK_EX, &err);
    if (lkHndl < 0) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_742,
			  "Can't obtain AdvFS global lock on '%s'.\n"),
		  MSFS_DMN_DIR);
	writemsg (SV_ERR | SV_LOG_ERR | SV_CONT, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_743, 
			  "Error = [%d] %s\n"), err, ERRMSG(err));
        return FAILURE;
    }

    /*
     * Grab AdvFS domain-specific lock. It will be released automatically
     * when fixfdmn exits.
     */   
    dmnLock = lock_file_nb (pathName, LOCK_EX, &err);
    if (dmnLock < 0) {
        if (EWOULDBLOCK == err) {
            writemsg (SV_ERR | SV_LOG_ERR, 
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_744, 
			      "Cannot execute. Another AdvFS command is currently claiming\n"));
	    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_745,
			      "exclusive use of the specified domain.\n"));
        } else {
            writemsg (SV_ERR | SV_LOG_ERR, 
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_746, 
			      "Can't obtain AdvFS domain lock on '%s'.\n"),
		      pathName);
	    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT, 
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_743, 
			      "Error = [%d] %s\n"), err, ERRMSG(err));
        }
        return FAILURE;
    } 

    /*
     * Release the /etc/fdmns lock.
     */
    if (unlock_file (lkHndl, &err) == -1) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_747, 
			  "Can't release AdvFS global lock on '%s'.\n"),
		  MSFS_DMN_DIR);
	writemsg (SV_ERR | SV_LOG_ERR | SV_CONT, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_743, 
			  "Error = [%d] %s\n"), err, ERRMSG(err));
	return FAILURE;
    }

    /*
     * Determine if any filesets are mounted on this domain.
     */
    mountCnt = getfsstat (0, 0, MNT_NOWAIT);
    if (mountCnt < 0) {
        writemsg (SV_ERR | SV_LOG_ERR , 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_748, 
			  "Can't obtain information on mounted filesystems via the getfsstat call.\n"));
        perror (Prog);
        return FAILURE;
    }
    mountTbl = (struct statfs *)ffd_malloc(mountCnt * sizeof(struct statfs));
    if (NULL == mountTbl) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			  "Can't allocate memory for %s.\n"),
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_749, 
			  "file system mount table"));
        return NO_MEMORY;
    }
    
    err = getfsstat (mountTbl, mountCnt * sizeof (struct statfs), MNT_NOWAIT);
    if (err < 0) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_748, 
			  "Can't obtain information on mounted filesystems via the getfsstat call.\n"));
        perror (Prog);
        return FAILURE;
    }
    
    /*
     * Check the domain name of any mounted AdvFS filesets. If it's ours,
     * then return failure.  This is threadsafe as we use strtok_r().
     */
    status = SUCCESS;
    for (i = 0 ; i < mountCnt ; i++) {
	char *pSave;

        mountDmnName = (char *) strtok_r (mountTbl[i].f_mntfromname, 
					  "#", &pSave);
        if ((MOUNT_MSFS == mountTbl[i].f_type) &&
            (0 == strcmp (domainName, mountDmnName))) {
            /*
	     * This is the domain we want to look at. 
	     */
            mountBfSetName = strtok_r (NULL, "", &pSave);
            writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_750, 
			      "Fileset %s mounted on %s in domain %s.\n"),
                      mountBfSetName, mountTbl[i].f_mntonname, 
		      mountDmnName);
	    status = FAILURE;
        }
    }
    
    if (status == FAILURE) {
	writemsg (SV_ERR | SV_LOG_ERR | SV_CONT, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_751, 
			  "Can't fix domain with mounted filesets.\n"));
	return FAILURE;
    }

    /*
     * Open the domain directory to get the volumes.
     */

    dmnDir = opendir(pathName);
    if (NULL == dmnDir) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_752, 
			 "Invalid domain name '%s'.\n"), domain->dmnName);
        perror(Prog);
        return FAILURE;
    }
    
    /*
     * Create the volumes array to store the device names in.
     */
    status = create_volumes (&volumes);
    if (SUCCESS != status) {
	closedir(dmnDir);
        return status;
    }
    domain->volumes = volumes;
    
    /*
     * Loop through all the file entries in /etc/fdmns/<domain>/
     */
    while (NULL != (dp = readdir(dmnDir))) {
        strcpy(dirName, pathName);
        strcat(dirName, "/");
    
        strcat(dirName, dp->d_name);

        if (-1 == stat(dirName, &statBuf)) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_753, 
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

        /*
         * Read the symbolic link to find the real device name.
         */
        status = readlink(dirName, volName, MAXPATHLEN -1);
        if (-1 == status) {
            writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_754, 
			     "Can't readlink on '%s'.\n"), dirName);
            perror(Prog);
            closedir(dmnDir);
            return FAILURE;
        } else if (MAXPATHLEN == status) {
            volName[MAXPATHLEN -1] = '\0';
        }

        volumes[counter].volName = (char *)ffd_malloc(strlen(volName) + 1);

        if (NULL == volumes[counter].volName) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_755,
			     "volume name"));
            closedir(dmnDir);
            return NO_MEMORY;
        }

        strcpy (volumes[counter].volName, volName);
        
        /*
         * Get the volume size and save it.
         */
        status = get_dev_size(volName, devType, &volSize);
	if ( status == GDS_VOLTOOBIG ) {
	   /*--too big, let us use the default size in BSR_VD_ATTR--*/
		printf("\n lsm volume is too big, assuming a size of 2TB \n");
		volumes[counter].volSize = MAXUINT;    
		volBigFlag = 1;	
		status = GDS_OK;
	}
        if (GDS_OK != status) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_756,
			     "Can't get size for volume '%s'.\n"), 
		     volName);
            closedir(dmnDir);
            return FAILURE;
        }
	if(!volBigFlag)
        	volumes[counter].volSize = volSize;

        counter++;

        if (counter >= MAX_VOLUMES) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_757,
			     "More than %d volumes found.\n"), 
		     (MAX_VOLUMES - 1));
            closedir(dmnDir);
            return FAILURE;
        }
    } /* end while loop */

    domain->numVols = counter;

    free(mountTbl);
    closedir(dmnDir);
    return SUCCESS;

} /* end get_domain_volumes*/


/*
 * Function Name: open_domain_volumes (3.7)
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
int open_domain_volumes(domainT *domain)
{
    char *funcName = "open_domain_volumes";
    int  fixed; /* Local variable to see if any volume has been fixed. */
    int  counter;
    int  status;
    int  volCounter;
    int  volNumber;
    int  dmnInfo;
    ulong_t  soflags; /* Used for safe_open */
    volumeT  *oldVolumes;
    volumeT  *newVolumes;
    rbmtInfoT *volRbmt;
    extern errno;

    fixed = FALSE;
    dmnInfo = FALSE; /* Set to TRUE in get_volume_number */
    oldVolumes = domain->volumes;
    domain->highVolNum = 0;
    volCounter = 0;

    /*
     * Loop on all entries in domain.volumes[].
     */
    for (counter = 0; counter < MAX_VOLUMES; counter++)  {
        /* 
         * If the volume name is not NULL, open it. 
         */
        if (NULL == oldVolumes[counter].volName) {
            continue;
        }

        /* 
         * Found a valid block device, so open it and store the
         * file descriptor.
         */
        
	/*
	 * Safe_open is used to close a possible security problem
	 *
	 * volName is a Block device.
	 */
	soflags = (OPN_TYPE_BLK);

	oldVolumes[counter].volFD = 
	    safe_open(oldVolumes[counter].volName, 
		      O_RDWR | O_SYNC , soflags);
        
        if (-1 == oldVolumes[counter].volFD) {
            writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_758,
			     "Can't open device '%s', error=%d.\n"),
                     oldVolumes[counter].volName, errno);
            continue;
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
    if (SUCCESS != status) {
        return status;
    } 
   
    /* 
     * Loop through all the volumes in the old volumes array.  
     */
    for (counter = 0; counter < MAX_VOLUMES; counter++) {
        /* 
         * Check to see if the current volume has a valid file descriptor.
         */
        if (-1 == oldVolumes[counter].volFD) {
            continue;
        }

        /*
         * Get the volume number from mcell 4 and return it. 
         */
	status = get_volume_number(domain, counter, &volNumber, &dmnInfo);
        if (SUCCESS != status) {
            /* 
             * Get volume number failed.
	     * FUTURE: We may have been able to figure out which 
	     * volNumber it should be on, WHAT SHOULD WE DO?
             */
            return status;
	}

        /*
         * Check for duplicate volume numbers.
         */
        if (-1 != newVolumes[volNumber].volFD) {
            /*
             * We have two or more volumes which have the same volume ID.
	     * At this point we do not know which is the correct volume,
	     * so we will need to corrupt exit.
	     *
	     * FUTURE: We could check more places on disk to see if we
	     * can figure out which one is in error.
             */
            writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_759,
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
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_760, 
			     "If you know which volume does not belong to this domain,\n"));
	    writemsg(SV_ERR | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_761,
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
        newVolumes[volNumber].volFD       = oldVolumes[counter].volFD;
        newVolumes[volNumber].volName     = oldVolumes[counter].volName;
        newVolumes[volNumber].volSize     = oldVolumes[counter].volSize;
        newVolumes[volNumber].volRbmt     = oldVolumes[counter].volRbmt;
        newVolumes[volNumber].domainID.tv_sec  = 
	    oldVolumes[counter].domainID.tv_sec;
        newVolumes[volNumber].domainID.tv_usec = 
	    oldVolumes[counter].domainID.tv_usec;
        newVolumes[volNumber].dmnVersion  = oldVolumes[counter].dmnVersion;
        newVolumes[volNumber].mountId.tv_sec   = 
	    oldVolumes[counter].mountId.tv_sec;
        newVolumes[volNumber].mountId.tv_usec  = 
	    oldVolumes[counter].mountId.tv_usec;
        newVolumes[volNumber].mntStatus   = oldVolumes[counter].mntStatus;
        newVolumes[volNumber].rootTag     = oldVolumes[counter].rootTag;
	newVolumes[volNumber].volSbmInfo  = oldVolumes[counter].volSbmInfo;

	volRbmt = (rbmtInfoT *)ffd_malloc(sizeof(struct rbmtInfo));
	if (NULL == volRbmt) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_762,
			     "volume RBMT structure"));
	    return NO_MEMORY;
	}
	volRbmt->sbmLBNSize = 0;
	volRbmt->bmtLBNSize = 0;
	volRbmt->maxBmtPage = 0;
	volRbmt->maxRbmtPage = 0;

        newVolumes[volNumber].volRbmt = volRbmt;

        status = init_sbm_structures (&newVolumes[volNumber]);
	if (status != SUCCESS) {
	    return status;
	}

        writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_766, 
			 "Setting volume '%s' to volume number %d.\n"),
                 newVolumes[volNumber].volName, volNumber);

    } /* End loop of volumes. */

    domain->volumes = newVolumes;
    /* 
     * Have moved all volumes into the correct position in newVolumes 
     */
    free(oldVolumes);

    if (0 == volCounter) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_767, 
			 "No valid volumes found in domain '%s'.\n"),
		 domain->dmnName);
        corrupt_exit(NULL);
    }
    
    if (FALSE == dmnInfo) {
        /*
         * We couldn't find a valid domain version on any of the
         * volumes, so prompt the user for one.
         * FUTURE: Look at more pages in each volume for a valid
         * domain version.
         */
        char errorMsg[MAXERRMSG];
        char answer[10];
        int size = 10;
        int  dmnVersion = 0;  /* The version the user wishes to change to */
        
        writemsg(SV_ERR | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_768, 
			 "No valid domain version found for domain '%s'.\n"),
		 domain->dmnName);
        writemsg(SV_ERR | SV_CONT, 
                  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_490, 
			  "Do you want to correct the domain version? (y/[n])  "));

        status = prompt_user_yes_no(answer, "n");
        if (SUCCESS != status) {
            sprintf(errorMsg, 
		    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_770,
			    "Unable to prompt user for input.\n"));
            corrupt_exit(errorMsg);
        }

        if ('y' == answer[0]) {
            /*
             * Loop until valid domain version is entered
             */
            while (0 == dmnVersion) {
                writemsg(SV_ERR | SV_CONT, 
                         catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_771,
				 "Please enter the domain version to use (3 or 4):  "));
                status = prompt_user(answer, size);
                if (SUCCESS != status) {
                    sprintf(errorMsg,
			    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_770,
				    "Unable to prompt user for input.\n"));
                    corrupt_exit(errorMsg);
                }

                /*
                 * convert string to int.
                 */
                dmnVersion = atoi(answer);

                if ((dmnVersion < 3) || (dmnVersion > 4)) {
                    writemsg (SV_ERR | SV_CONT, 
			      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_772,
				      "Invalid domain version.\n\n"));
                    dmnVersion = 0;
                }
            } /* end while loop */
            
            domain->dmnVersion = dmnVersion;
        } else {
            /*
             * User does not want to continue.
             */
            sprintf(errorMsg,
                    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_773,
			    "Unable to continue, verify the correct volumes are in the domain.\n"));
            corrupt_exit(errorMsg);
        }
    } /* endif no domain version */

    /*
     * If what they want to do is restore from undo we can return.
     * This way we don't try to correct the magic number for a restore.
     */
    if (TRUE == Options.undo) {
	return SUCCESS;
    }

    /*
     * Loop through the volumes checking the magic number.
     */
    for (counter = 1; counter <= domain->highVolNum; counter ++) {
        if (-1 == newVolumes[counter].volFD) {
            continue;
        }
        
        /* 
         * Volume is now open - fix the magic number if we're
	 * not restoring the undo files.
         */
	status = correct_magic_number(domain, counter);

	if (SUCCESS != status) {
	    if (FIXED == status) {
		fixed = TRUE;
	    } else if (NON_ADVFS == status) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_774,
				 "'%s' is not an AdvFS volume.\n"),
			 newVolumes[counter].volName);
		return status;
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_775,
				 "Can't fix AdvFS magic number on volume %s.\n"),
			 newVolumes[counter].volName);
		return status;
	    }
	}
    } /* end for loop */

    /*
     * If we fixed a magic number, then see if the user wants to continue
     * looking for corruptions or stop now.
     */
    if (TRUE == fixed) {
        prompt_user_to_continue(domain, 
				catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_776,
					"Magic number fixed.\n"));
    }            

    return SUCCESS;

} /* end open_domain_volumes */


/*
 * Function Name: get_volume_number (3.n)
 *
 * Description:
 *  Follow the next mcell pointer from BMT/RBMT page 0 mcell 0, get 
 *  the volume number from the next pointer and domain attributes. Also
 *  same the domain ID and version number in the domain structure if we 
 *  don't have them already.
 *
 * Input parameters:
 *  domain: A pointer to the domain structure.
 *  index: An index into the volumes array.
 *  volNumber: The volume we are working on.
 *  dmnInfo: A flag indicating if we have same the domain ID and version. 
 *
 * Output parameters:
 *  dmnInfo: Set to true when we have saved the domain ID and version.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, CORRUPT
 */
int get_volume_number(domainT *domain, int  index,
		      int  *volNumber, int  *dmnInfo)
{
    char          *funcName = "get_volume_number";
    pageBufT       pageBuf;
    bsMPgT        *pBMTPage;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsVdAttrT     *pVdAttr;
    bsDmnAttrT    *pDmnAttr;   
    ssize_t       bytes;
    int           status;
    int           volId1;
    int           volId2;
    int           volId3;

    /*
     * Init volIds to illegal value.
     */
    volId1 = 0;
    volId2 = 0;
    volId3 = 0;

    /*
     * Read the RBMT/BMT0 page. Don't use the cache.
     */
    if ((off_t)-1 == lseek(domain->volumes[index].volFD,
                           RBMT_PAGE0 * (long)DEV_BSIZE,
			   SEEK_SET)) {
        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_192,
			 "Can't lseek to block %d on '%s'.\n"),
		 RBMT_PAGE0, domain->volumes[index].volName); 
        perror(Prog);
        return FAILURE;
    }
    bytes = read(domain->volumes[index].volFD, &pageBuf, PAGESIZE);

    if (-1 == bytes) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_193, 
			 "Can't read page at block %d on '%s'.\n"), 
		 RBMT_PAGE0, domain->volumes[index].volName);
        perror(Prog);
        return FAILURE;
    } else if (PAGESIZE != bytes) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_194,
			 "Can't read page at block %d on '%s', %d of %d bytes read.\n"),
		 RBMT_PAGE0, domain->volumes[index].volName,
                 bytes, PAGESIZE);
        return FAILURE;
    }
      
    pBMTPage = (bsMPgT *)pageBuf;
    
    /*
     * Save the domain version number for this volume.
     */
    domain->volumes[index].dmnVersion = pBMTPage->megaVersion;
    
    /* 
     * Locate mcell 0 in BMT page. 
     */
    pMcell = &(pBMTPage->bsMCA[0]);

    /*
     * Get the volume number from the NEXT pointer of mcell 0.
     */
    volId1 = pMcell->nextVdIndex;

    /*
     * Follow the NEXT pointer to find the mcell we need.
     * ODS Version 3 : this will always be 4.
     * ODS Version 4 : this will always be 6.
     * Make sure the version and next mcell are not corrupt before
     * using them.
     */
     
    if ((3 == pBMTPage->megaVersion && 4 == pMcell->nextMCId.cell) ||
        (4 == pBMTPage->megaVersion && 6 == pMcell->nextMCId.cell)) {
        pMcell = &(pBMTPage->bsMCA[pMcell->nextMCId.cell]);

        /*
         * Compute the volume number from this mcell's BMT tag number.
         */
        volId2 = (-1 * pMcell->tag.num / 6);
    
        /* 
         * Now we need to step through this record
         */
        pRecord = (bsMRT *)pMcell->bsMR0;

        while ((0 != pRecord->type) &&
               (0 != pRecord->bCnt) &&
               (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
            if (BSR_VD_ATTR == pRecord->type) {
                /* 
                 * Get the volume number from the record. 
                 */
                pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));
                volId3 = pVdAttr->vdIndex;
            }
            
            /*
             * If we haven't already stored the domain ID and domain 
             * version number in the domain structure, then do that here
             * and set dmnInfo to TRUE indicating that we now have the
             * information.
             */
            if ((BSR_DMN_ATTR == pRecord->type) && (FALSE == *dmnInfo)) {
                domain->dmnVersion = pBMTPage->megaVersion;
                pDmnAttr = (bsDmnAttrT *)((char *)pRecord + sizeof(bsMRT));
                domain->dmnId.tv_sec = pDmnAttr->bfDomainId.tv_sec;
                domain->dmnId.tv_usec = pDmnAttr->bfDomainId.tv_usec;
                *dmnInfo = TRUE;
            }
            pRecord = (bsMRT *) (((char *)pRecord) + 
                             roundup(pRecord->bCnt, sizeof(int))); 
        } /* end while loop */

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
             * Two of the three match, volId1 not corrupt.
             */
            *volNumber = volId1;
        } else if (volId2 == volId3) {
            /*
             * Two of the three match, volId1 corrupt.
             */
            *volNumber = volId2;
        } else {
            /*
             * No match - Have chosen to use volId1.
             */
            *volNumber = volId1;
        }
    } else {
        /*
         * We had a corruption which prevented us from following
         * the next pointer, so just use volId1.
         */
        *volNumber = volId1;
    } /* end if version and next mcell values match */
    
    /*
     * Make sure the volume number we have falls within the
     * proper range.
     */
    if ((*volNumber > MAX_VOLUMES) || (*volNumber < 1)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_780, 
			 "Volume '%s' has an invalid volume number of %d.\n"),
		 domain->volumes[index].volName, volNumber);
        return CORRUPT;
    }

    return SUCCESS;
} /* end get_volume_number */


/*
 * Function Name: load_subsystem_extents (3.8)
 *
 * Description: Collect the extents from the RBMT/BMT0 for all the 
 *              important subsystems.
 *
 *		Note that the RBMT/BMT0 has already been checked for
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
int load_subsystem_extents(domainT *domain)
{
    char 	*funcName = "load_subsystem_extents";
    int		status;
    int		volNum;
    int         tag;
    int         setTag;
    volumeT	*volumes;
    mcellT	mcell;
    pageT	*pPage;
    bsMPgT	*pBMTPage;
    bsMCT	*pMcell;
    bsMRT	*pRecord;
    bsXtraXtntRT *pXtra;
    pagetoLBNT  extentArray[2];
    LBNT	currPageLBN;
    LBNT	nextPageLBN;
    long	pageNum;

    volumes = domain->volumes;

    for (volNum = 1 ; volNum <= domain->highVolNum ; volNum++) {
        if (-1 == volumes[volNum].volFD) {
	    continue;
	}

	/*
	 * Get the RBMT / BMT0 extents
	 */
	status = collect_rbmt_extents(domain, volNum, TRUE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Set defaults for the following
	 */
	mcell.vol = volNum;
	mcell.page = 0;

	/*
	 * Get the SBM extents
	 */
	mcell.cell = 1;
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->sbmLBN),
				 &(volumes[volNum].volRbmt->sbmLBNSize), TRUE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Get the root tag extents
	 */
	if (volNum == domain->rootTagVol) {
	    mcell.cell = 2;
	    status = collect_extents(domain, NULL, &mcell, TRUE,
				     &(domain->rootTagLBN),
				     &(domain->rootTagLBNSize), TRUE);
	    if (SUCCESS != status) {
		return status;
	    }
	}

	/*
	 * Get the transaction log extents.
	 * NOTE: All volumes CAN have a tran log.
	 */
	mcell.cell = 3;
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->logLBN),
				 &(volumes[volNum].volRbmt->logLBNSize),
				 TRUE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Get the BMT extents
	 */
	if (3 == volumes[volNum].dmnVersion) {
	    /*
	     * Start on mcell 0 for old domains.
	     */
	    mcell.cell = 0;
	} else {
	    mcell.cell = 4;
	}
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->bmtLBN),
				 &(volumes[volNum].volRbmt->bmtLBNSize), 
				 TRUE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Get the MISC extents
	 */
	mcell.cell = 5;
	status = collect_extents(domain, NULL, &mcell, TRUE,
				 &(volumes[volNum].volRbmt->miscLBN),
				 &(volumes[volNum].volRbmt->miscLBNSize), 
				 TRUE);


	if (SUCCESS != status) {
	    return status;
	}
    }

    return SUCCESS;
} /* end load_subsystem_extents */


/*
 * Function Name: collect_extents (3.9)
 *
 * Description: This routine collects all the extents for a file.  The
 *		calling function is responsible for freeing the extent array
 *		that is malloc'ed by this routine. Also, the extents have
 *		already been checked before this function is called. There
 *		is no need to do validation of the extent array.
 *
 * Input parameters:
 *     domain: A pointer to the domain being worked on.
 *     fileset: A pointer to the fileset (if any) this file belongs to.
 *     pPrimMcell: A pointer to the primary mcell for the file.
 *     addToSbm: A flag whether to call collect_sbm_info or not.
 *     isRBMT: A flag indicating whether extents should be collected
 *		from the BMT or from the RBMT/BMT0.
 * 
 * Output parameters:
 *     extents: A pointer to an array of the extents in this file.
 *     arraySize: Size of extent array.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 *
 */
int collect_extents(domainT *domain, filesetT *fileset, mcellT *pPrimMcell,
		    int addToSbm, pagetoLBNT **extents,
		    int *arraySize, int isRBMT)
{
    char *funcName = "collect_extents";

    mcellT	currentMcell;	/* The mcell currently being worked on */
    mcellT	nextMcell;	/* The next mcell to be worked on */
    mcellT	chainMcell;	/* The first mcell on the extent chain. */
    volumeT	*volume;
    pageT	*page;
    pagetoLBNT  *tmpExtents;
    pagetoLBNT  extentArray[BMT_XTRA_XTNTS];
    bsMPgT	*bmtPage;
    bsMCT	*pMcell;
    bsMRT	*pRecord;
    bsXtntRT	*pXtnt;
    bsXtraXtntRT *pXtraXtnt;
    bsShadowXtntT *pShadowXtnt;
    long	numExtents;
    long	currExtent;
    LBNT	currLbn;
    int		currVol;
    int		i;
    int         found;
    int		status;
    LBNT	lbn;
    int		extentSize;
    int		tagnum;
    int		tagseq;
    int		settag;

    numExtents = 0;

    currentMcell.vol  = pPrimMcell->vol;
    currentMcell.page = pPrimMcell->page;
    currentMcell.cell = pPrimMcell->cell;

    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    /*
     * Count how many extents need to be malloced
     */

    found = FALSE;
    /*
     * Loop through primary next list until BSR_XTNT is found.
     */
    while ((0 != currentMcell.vol) && (FALSE == found)) {
	volume = &(domain->volumes[currentMcell.vol]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	}
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_783, 
			     "Can't convert [R]BMT page %d to LBN.\n"),
		     currentMcell.page);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &page);
	if (SUCCESS != status) {
	    return status;
	}

	bmtPage = (bsMPgT *)page->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);
	tagnum  = pMcell->tag.num;
	tagseq  = pMcell->tag.seq;
	settag  = pMcell->bfSetTag.num;

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Now loop through primary next records.
	 */

	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * If it's not a BSR_XTNTS record, we don't need it.
	     */
	    if (BSR_XTNTS == pRecord->type) {

	        pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		if ((TRUE == isRBMT) ||
		    (domain->dmnVersion >= 4)) {
		    /*	
		     * Only collect extents on V4 or greater domains 
		     * or from BMT0 mcells.
		     */
		    numExtents += pXtnt->firstXtnt.xCnt - 1;
		}

		/*
		 * Set chainMcell to follow the extent chain later.
		 */
		chainMcell.vol  = pXtnt->chainVdIndex;
		chainMcell.page = pXtnt->chainMCId.page;
		chainMcell.cell = pXtnt->chainMCId.cell;
		found = TRUE;
		break;
	    }

	    /*
	     * Point to the next record.
	     */
	    pRecord = (bsMRT *)(((char *)pRecord) +
				roundup(pRecord->bCnt, sizeof(int)));
	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, page, FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currentMcell.vol  = nextMcell.vol;
	currentMcell.page = nextMcell.page;
	currentMcell.cell = nextMcell.cell;
    } /* end loop on primary next list */

    /*
     * Now start looking through the extent mcell chain.
     */
    currentMcell.vol  = chainMcell.vol;
    currentMcell.page = chainMcell.page;
    currentMcell.cell = chainMcell.cell;

    while (0 != currentMcell.vol) {
	volume = &(domain->volumes[currentMcell.vol]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	}
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_783, 
			     "Can't convert [R]BMT page %d to LBN.\n"),
		     currentMcell.page);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &page);
	if (SUCCESS != status) {
	    return status;
	}

	bmtPage = (bsMPgT *)page->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Now loop through chain records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (BSR_XTRA_XTNTS == pRecord->type) {
		pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord +
					     sizeof(bsMRT));
		numExtents += pXtraXtnt->xCnt - 1;
	    } else if (BSR_SHADOW_XTNTS == pRecord->type) {
		pShadowXtnt = (bsShadowXtntT *)((char *)pRecord +
						sizeof(bsMRT));
		numExtents += pShadowXtnt->xCnt - 1;
	    }

	    /*
	     * Point to the next record.
	     */
	    pRecord = (bsMRT *)(((char *)pRecord) +
				roundup(pRecord->bCnt, sizeof(int)));
	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, page, FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currentMcell.vol  = nextMcell.vol;
	currentMcell.page = nextMcell.page;
	currentMcell.cell = nextMcell.cell;
    } /* end chain loop */

    /*
     * Now that we know how many extents there are,
     * malloc the array and fill it, calling collect_sbm_info
     * if needed.
     */

    /*
     * arraySize is the number of actual extents, which does not include
     * the trailing LBN -1 "extent" entry - therefore calloc arraysize + 1
     * elements.
     */
    *arraySize = numExtents;
    tmpExtents = (pagetoLBNT *)ffd_calloc(*arraySize + 1, sizeof(pagetoLBNT));
    if (NULL == tmpExtents) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_417, 
			 "extent array"));
        return NO_MEMORY;
    }
    *extents = tmpExtents;

    /*
     * Reset pointers to loop through the mcells again.
     */
    currExtent = 0;

    currentMcell.vol  = pPrimMcell->vol;
    currentMcell.page = pPrimMcell->page;
    currentMcell.cell = pPrimMcell->cell;

    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    /*
     * Loop through primary next list until BST_XTNTs is found
     */
    found = FALSE;
    while ((0 != currentMcell.vol) && (FALSE == found)) {
	volume = &(domain->volumes[currentMcell.vol]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	}
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_783,
			     "Can't convert [R]BMT page %d to LBN.\n"),
		     currentMcell.page);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &page);
	if (SUCCESS != status) {
	    return status;
	}

	bmtPage = (bsMPgT *)page->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Now loop through primary next records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    /*
	     * If it's not a BSR_XTNTS record, we don't care about it.
	     */
	    if (BSR_XTNTS == pRecord->type) {
	    
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		if ((TRUE == isRBMT) || (domain->dmnVersion >= 4)) {
		    /*
		     * Loop on all extents
		     */
		    for (i = 0; 
			 i < pXtnt->firstXtnt.xCnt && i < (BMT_XTNTS + 1);
			 i++) {
			tmpExtents[currExtent].volume = currentMcell.vol;
			tmpExtents[currExtent].page   = pXtnt->firstXtnt.bsXA[i].bsPage;
			tmpExtents[currExtent].lbn    = pXtnt->firstXtnt.bsXA[i].vdBlk;
			tmpExtents[currExtent].isOrig = FALSE;
			extentArray[i].volume = currentMcell.vol;
			extentArray[i].page   = pXtnt->firstXtnt.bsXA[i].bsPage;
			extentArray[i].lbn    = pXtnt->firstXtnt.bsXA[i].vdBlk;
			currExtent++;
		    }
		    if (TRUE == addToSbm) {
			status = collect_sbm_info(domain, extentArray,
						  pXtnt->firstXtnt.xCnt,
						  tagnum, settag, 
						  &currentMcell, 0);
			if (SUCCESS != status) {
			    return status;
			}
		    }
		}

		/*
		 * Set chainMcell to follow the extent chain later.
		 */
		chainMcell.vol  = pXtnt->chainVdIndex;
		chainMcell.page = pXtnt->chainMCId.page;
		chainMcell.cell = pXtnt->chainMCId.cell;
		found = TRUE;
		break;
	    } /* End if BSR_XTNTS record */

	    /*
	     * Point to the next record.
	     */
	    pRecord = (bsMRT *)(((char *)pRecord) +
				roundup(pRecord->bCnt, sizeof(int)));
	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, page, 
				       FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currentMcell.vol  = nextMcell.vol;
	currentMcell.page = nextMcell.page;
	currentMcell.cell = nextMcell.cell;
    } /* end loop on primary next list */

    /*
     * Now start looking through the extent mcell chain.
     */
    currentMcell.vol  = chainMcell.vol;
    currentMcell.page = chainMcell.page;
    currentMcell.cell = chainMcell.cell;

    while (0 != currentMcell.vol) {
	volume = &(domain->volumes[currentMcell.vol]);
	if (isRBMT) {
	    status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,
					 volume->volRbmt->rbmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	} else {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currentMcell.page,
					 &currLbn, &currVol);
	}
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_783, 
			     "Can't convert [R]BMT page %d to LBN.\n"),
		     currentMcell.page);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &page);
	if (SUCCESS != status) {
	    return status;
	}

	bmtPage = (bsMPgT *)page->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Now loop through chain records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (BSR_XTRA_XTNTS == pRecord->type) {
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
		 * in fixfdmn.
		 */
		assert(tmpExtents[currExtent].page ==
		       pXtraXtnt->bsXA[0].bsPage);

		/*
		 * Loop on all extents
		 */
		for (i = 0 ; i < pXtraXtnt->xCnt && i < BMT_XTRA_XTNTS ; i++) {
		    tmpExtents[currExtent].volume = currentMcell.vol;
		    tmpExtents[currExtent].page   = pXtraXtnt->bsXA[i].bsPage;
		    tmpExtents[currExtent].lbn    = pXtraXtnt->bsXA[i].vdBlk;
		    tmpExtents[currExtent].isOrig = FALSE;
		    extentArray[i].volume = currentMcell.vol;
		    extentArray[i].page   = pXtraXtnt->bsXA[i].bsPage;
		    extentArray[i].lbn    = pXtraXtnt->bsXA[i].vdBlk;
		    currExtent++;
		}
		if (TRUE == addToSbm) {
		    status = collect_sbm_info(domain, extentArray,
					      pXtraXtnt->xCnt,
					      tagnum, settag,
					      &currentMcell, 0);
		    if (SUCCESS != status) {
			return status;
		    }
		}
	    } else if (BSR_SHADOW_XTNTS == pRecord->type) {
		pShadowXtnt = (bsShadowXtntT *)((char *)pRecord +
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
		 * in fixfdmn.
		 */
		assert(tmpExtents[currExtent].page ==
		       pShadowXtnt->bsXA[0].bsPage);

		/*
		 * Loop on all extents
		 */
		for (i = 0 ; i < pShadowXtnt->xCnt && i < BMT_SHADOW_XTNTS ; i++) {
		    tmpExtents[currExtent].volume = currentMcell.vol;
		    tmpExtents[currExtent].page   = pShadowXtnt->bsXA[i].bsPage;
		    tmpExtents[currExtent].lbn    = pShadowXtnt->bsXA[i].vdBlk;
		    tmpExtents[currExtent].isOrig = FALSE;
		    extentArray[i].volume = currentMcell.vol;
		    extentArray[i].page   = pShadowXtnt->bsXA[i].bsPage;
		    extentArray[i].lbn    = pShadowXtnt->bsXA[i].vdBlk;
		    currExtent++;
		}
		if (TRUE == addToSbm) {
		    status = collect_sbm_info(domain, extentArray,
					      pShadowXtnt->xCnt,
					      tagnum, settag,
					      &currentMcell, 0);
		    if (SUCCESS != status) {
			return status;
		    }
		}
	    } /* End if pRecord->type points to BSR_XTRA or BSR_SHADOW */
	    
	    /*
	     * Point to the next record.
	     */
	    pRecord = (bsMRT *)(((char *)pRecord) +
				roundup(pRecord->bCnt, sizeof(int)));
	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, page, FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Reset pointers for the next pass.
	 */
	currentMcell.vol  = nextMcell.vol;
	currentMcell.page = nextMcell.page;
	currentMcell.cell = nextMcell.cell;
    } /* end chain loop */

    /*
     * If this is a clone, and NOT the frag file, merge in the extents
     * from the original
     */
    if (NULL != fileset &&
	FS_IS_CLONE(fileset->status) &&
	1 != tagnum)
    {
	filesetT   *origFileset;
	pagetoLBNT *origXtntArray;
	int        origXtntArraySize;
	pagetoLBNT *cloneXtntArray;
	int        cloneXtntArraySize;
	pagetoLBNT *newXtntArray;
	int        newXtntArraySize;
	tagNodeT   tmpTag;
	tagNodeT   *tagNode;
	tagNodeT   *origTagNode;
	nodeT	   *node;

	/*
	 * Initialize variables
	 */
	origFileset = fileset->origClonePtr;
	cloneXtntArray = tmpExtents;
	cloneXtntArraySize = numExtents;
	tmpTag.tagSeq.num = tagnum;
	tagNode = &tmpTag;

	status = find_node(fileset->tagList, (void **)&tagNode, &node);
	if (SUCCESS != status) {
	    return status;
	}

	if (NULL == origFileset) {
	    /*
	     * This might be an Internal Error, but we can continue, so
	     * do so.
	     */
	    /*
	     * TODO:  writemsg / writelog here?
	     */
	    return SUCCESS;
	}

	origTagNode = &tmpTag;
	status = find_node(origFileset->tagList, (void **)&origTagNode, &node);
	if (NOT_FOUND == status) {
	    /*
	     * Another Internal Error we can continue from.
	     */
	    return SUCCESS;
	} else if (SUCCESS != status) {
	    return status;
	}

	if (T_IS_CLONE_ORIG_PMCELL(tagNode->status)) {
	    /*
	     * We just collected the extents for the original file.
	     * Set isOrig to TRUE for all extents.
	     */
	    for (i = 0 ; i <= numExtents ; i++) {
		tmpExtents[i].isOrig = TRUE;
	    }
	} else {
	    /*
	     * We just collected the clone specific extents.  Now go
	     * get the original file's extents and merge them.
	     */
	    status = collect_extents(domain, origFileset,
				     &(origTagNode->tagPmcell), FALSE,
				     &origXtntArray, &origXtntArraySize,
				     FALSE);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = merge_clone_extents(origXtntArray, origXtntArraySize,
					 cloneXtntArray, cloneXtntArraySize,
					 &newXtntArray, &newXtntArraySize);
	    if (SUCCESS != status) {
		return status;
	    }
	    free(origXtntArray);
	    free(cloneXtntArray);

	    *extents = newXtntArray;
	    *arraySize = newXtntArraySize;
	}

	if (tagNode->size <
	    ((*extents)[*arraySize].page - 1) * PAGESIZE) {
	    /*
	     * This situation can happen when a file is extended
	     * after the clone is made.  The clone's filesize is the
	     * only clue that we've reached the end of the file, so
	     * we should truncate the extents.
	     */
	    int lastPage;

	    if (BF_FRAG_ANY != tagNode->fragSize) {
		lastPage = tagNode->fragPage;
	    } else if (0 == tagNode->size % PAGESIZE) {
		lastPage = (tagNode->size / PAGESIZE);
	    } else {
		lastPage = (tagNode->size / PAGESIZE) + 1;
	    }

	    for (i = *arraySize ; i > 0 ; i--) {
		if ((*extents)[i - 1].page < lastPage) {
		    if ((*extents)[i].page >= lastPage) {
			(*extents)[i].page = lastPage;
			(*extents)[i].lbn = (LBNT) -1;
			*arraySize = i;
		    }
		    break; /* out of for loop */
		}
	    }
	} /* End if extents beyond filesize */
    } /* End if clone */

    return SUCCESS;
} /* end collect_extents */


/*
 * Function Name: merge_clone_extents (3.9)
 *
 * Description: This routine merges the original and clone extent
 *		maps to form a complete extent map for a clone file.
 *
 *		Note that the general algorithm is taken from
 *		imm_merge_xtnt_map in kernel/msfs/bs/bs_inmem_map.c.
 *
 * Input parameters:
 *     origXtntArray: The array of the original file's extents.
 *     origXtntArraySize: Size of the original file's extent array.
 *     cloneXtntArray: The array of the clone's extents.
 *     cloneXtntArraySize: Size of the clone's extent array.
 * 
 * Output parameters:
 *     newXtntArray: A pointer to an array of the merged extents in this file.
 *     newXtntArraySize: Size of the merged extent array.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 *
 */
int merge_clone_extents(pagetoLBNT *origXtntArray, int origXtntArraySize,
			pagetoLBNT *cloneXtntArray, int cloneXtntArraySize,
			pagetoLBNT **newXtntArray, int *newXtntArraySize)
{
    char *funcName = "merge_clone_extents";

    int numExtents;
    int origXtnt;
    int cloneXtnt;
    int newXtnt;
    int copyXtnt;
    pagetoLBNT *copyXtntArray;
    int copyXtntArraySize;
    int tmpSize;
    int isOrig;

    /*
     * Initialize variables
     */
    origXtnt = 0;
    cloneXtnt = 0;
    newXtnt = 0;
    copyXtntArray = NULL;

    assert(NULL != origXtntArray && NULL != cloneXtntArray);

    /*
     * If either extent map is empty, return a copy of the other one.
     * A copy is needed because both originals are going to be freed.
     */
    if (0 == origXtntArraySize) {
	copyXtntArray = cloneXtntArray;
	copyXtntArraySize = cloneXtntArraySize;
	isOrig = FALSE;
    } else if ((0 == cloneXtntArraySize) ||
	       cloneXtnt > cloneXtntArraySize) {
	copyXtntArray = origXtntArray;
	copyXtntArraySize = origXtntArraySize;
	isOrig = TRUE;
    }

    if (NULL != copyXtntArray) {
	/*
	 * arraySize is the number of actual extents, which does not include
	 * the trailing LBN -1 "extent" entry - therefore calloc arraysize + 1
	 * elements.
	 */
	*newXtntArraySize = copyXtntArraySize;
	*newXtntArray = (pagetoLBNT *)ffd_calloc(*newXtntArraySize + 1,
						 sizeof(pagetoLBNT));
	if (NULL == *newXtntArray) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_417, 
			     "extent array"));
	    return NO_MEMORY;
	}
	memcpy(*newXtntArray, copyXtntArray,
	       ((*newXtntArraySize + 1) * sizeof(pagetoLBNT)));

	for (newXtnt = 0 ; newXtnt <= *newXtntArraySize ; newXtnt++) {
	    (*newXtntArray)[newXtnt].isOrig = isOrig;
	}

	return SUCCESS;
    }

    /*
     * Both extent maps have extents.  Have to do a merge.
     */

    /*
     * Malloc new extent array to be the size of both original and clone
     * arrays added together.  This is probably overkill, but not by a
     * significant amount, and shouldn't have a significant memory impact.
     * There is no way for this to be too small, and it cuts the amount of
     * processing and code for this routine almost in half.
     *
     * newXtntArraySize will be reset after the actual number of extents
     * has been calculated.
     */
    *newXtntArraySize = (origXtntArraySize + 1) + (cloneXtntArraySize + 1);
    *newXtntArray = (pagetoLBNT *)ffd_calloc(*newXtntArraySize + 1,
					     sizeof(pagetoLBNT));
    if (NULL == *newXtntArray) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_417, 
			 "extent array"));
	return NO_MEMORY;
    }

    origXtnt = 0;
    cloneXtnt = 0;

    while (origXtnt < origXtntArraySize && cloneXtnt < cloneXtntArraySize) {
	/* Get the first or next non-hole clone extent. */
	while ((cloneXtnt < cloneXtntArraySize) &&
	       ( (LBNT) -1 == cloneXtntArray[cloneXtnt].lbn)) {
	    cloneXtnt++;
	}

	assert(cloneXtnt == cloneXtntArraySize ||
	      ( (LBNT) -1 != cloneXtntArray[cloneXtnt].lbn) );

	/*
	 * Step 1: Copy original extents into new extent map until we
	 * find an extent that overlaps the next real clone extent's
	 * page range.
	 */
	while (origXtntArray[origXtnt + 1].page <=
	       cloneXtntArray[cloneXtnt].page) {
	    (*newXtntArray)[newXtnt].volume = origXtntArray[origXtnt].volume;
	    (*newXtntArray)[newXtnt].page = origXtntArray[origXtnt].page;
	    (*newXtntArray)[newXtnt].lbn = origXtntArray[origXtnt].lbn;
	    (*newXtntArray)[newXtnt].isOrig = TRUE;
	    newXtnt++;
	    origXtnt++;
	}

	/*
	 * Step 2: If the current original extent's page is less than
	 * the current clone extent's page, split the orig extent and
	 * add that split into the new extent map.
	 */
	if (origXtntArray[origXtnt].page < cloneXtntArray[cloneXtnt].page) {
	    (*newXtntArray)[newXtnt].volume = origXtntArray[origXtnt].volume;
	    (*newXtntArray)[newXtnt].page = origXtntArray[origXtnt].page;
	    (*newXtntArray)[newXtnt].lbn = origXtntArray[origXtnt].lbn;
	    (*newXtntArray)[newXtnt].isOrig = TRUE;
	    newXtnt++;
	}

	/*
	 * Step 3: Copy clone extents to the new extent map until we
	 * find a hole or run out of extents.
	 */
	while ((cloneXtnt < cloneXtntArraySize) &&
	       ( (LBNT) -1 != cloneXtntArray[cloneXtnt].lbn)) {
	    (*newXtntArray)[newXtnt].volume = cloneXtntArray[cloneXtnt].volume;
	    (*newXtntArray)[newXtnt].page = cloneXtntArray[cloneXtnt].page;
	    (*newXtntArray)[newXtnt].lbn = cloneXtntArray[cloneXtnt].lbn;
	    (*newXtntArray)[newXtnt].isOrig = FALSE;
	    newXtnt++;
	    cloneXtnt++;
	}

	/*
	 * Increment origXtnt until the last page of that extent is at
	 * least the first page of the next hole in the clone extentmap.
	 */
	while ((origXtnt < origXtntArraySize) &&
	       (origXtntArray[origXtnt + 1].page <=
		cloneXtntArray[cloneXtnt].page)) {
	    origXtnt++;
	}

	/*
	 * Split the original extent if it overlaps the last used clone
	 * extent.
	 */
	if (origXtntArray[origXtnt].page < cloneXtntArray[cloneXtnt].page) {
	    tmpSize = (cloneXtntArray[cloneXtnt].page -
		       origXtntArray[origXtnt].page);
	    if ( (LBNT) -1 != origXtntArray[origXtnt].lbn) {
		origXtntArray[origXtnt].lbn += tmpSize * BLOCKS_PER_PAGE;
	    }
	    origXtntArray[origXtnt].page = cloneXtntArray[cloneXtnt].page;
	}
    } /* End while both extentmaps have unprocessed extents */

    assert(origXtnt == origXtntArraySize || cloneXtnt == cloneXtntArraySize);

    /*
     * Step 4: Copy remaining extents from whichever (if either) extent
     * map still has unprocessed extents.
     */
    if (origXtnt == origXtntArraySize && cloneXtnt == cloneXtntArraySize) {
	if (origXtntArray[origXtnt].page > cloneXtntArray[cloneXtnt].page) {
	    copyXtntArray = origXtntArray;
	    copyXtntArraySize = origXtntArraySize;
	    copyXtnt = origXtnt;
	    isOrig = TRUE;
	} else {
	    copyXtntArray = cloneXtntArray;
	    copyXtntArraySize = cloneXtntArraySize;
	    copyXtnt = cloneXtnt;
	    isOrig = FALSE;
	}
    } else if (origXtnt < origXtntArraySize) {
	copyXtntArray = origXtntArray;
	copyXtntArraySize = origXtntArraySize;
	copyXtnt = origXtnt;
	isOrig = TRUE;
    } else if (cloneXtnt < cloneXtntArraySize) {
	copyXtntArray = cloneXtntArray;
	copyXtntArraySize = cloneXtntArraySize;
	copyXtnt = cloneXtnt;
	isOrig = FALSE;
    } else {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_786, 
			 "Internal Error: Both origXtnt and cloneXtnt greater than array size.\n"));
	return FAILURE;
    }
    if (NULL != copyXtntArray) {
	while (copyXtnt <= copyXtntArraySize) {
	    (*newXtntArray)[newXtnt].volume = copyXtntArray[copyXtnt].volume;
	    (*newXtntArray)[newXtnt].page = copyXtntArray[copyXtnt].page;
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
} /* end merge_clone_extents */


/*
 * Function Name: save_fileset_info (3.10)
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
int save_fileset_info(domainT *domain, pageT *pPage)
{
    char       *funcName = "save_fileset_info";
    bsTDirPgT  *pTagPage;
    pageT      *pBmtPage;
    bsMPgT     *bmtPage;
    volumeT    *volume;
    bsMCT      *pMcell;
    bsMRT      *pRecord;
    bsBfSetAttrT *pFsAttr;
    filesetT   *pFileset;
    filesetT   *fsPtr;
    mcellT     *pFsPmcell;
    pagetoLBNT *pFsTagLBN;
    char       *pFsName;
    tagNodeT   *maxTag;
    int fsTagLBNSize;
    int count;
    int pageNum;
    LBNT pageLBN;
    int pageVol;
    int mcellNum;
    int nextPageNum;
    int nextPageVol;
    int nextMcellNum;
    int fsAttrFound;
    int status;
    int tagPageNum;
    int tagStart;
    
    pTagPage = (bsTDirPgT *)pPage->pageBuf;
    tagPageNum = pTagPage->tpPgHdr.currPage;

    /*
     * We do not use tag 0 on page 0.
     */
    if (0 == tagPageNum) {
        tagStart = 1;
    } else {
        tagStart = 0;
    }

    /*
     * Loop through the tag entries on the page and save
     * the fileset information for the active entries.
     */
    for (count = tagStart; count < BS_TD_TAGS_PG; count++) {
        if ((0 != (pTagPage->tMapA[count].tmSeqNo & BS_TD_IN_USE))) {
	  
	    /*	
	     * Reset for each fileset
	     */
	    fsAttrFound = FALSE;                        

            /*
             * Get space for the fileset structure.
             */
            pFileset = (filesetT *)ffd_malloc(sizeof(filesetT));
            if (NULL == pFileset) {
	        writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_787,
				 "fileset structure"));
                return NO_MEMORY;
            }

            /* 
	     * Save the fileset tag file primary mcell and initialize
	     * fields that won't be set from data in this routine.
	     */
            pFileset->domain = domain;
	    pFileset->filesetId.dirTag.num = 0;
	    pFileset->filesetId.dirTag.seq = 0;
            pFileset->fsPmcell.vol = pTagPage->tMapA[count].tmVdIndex;
            pFileset->fsPmcell.page = pTagPage->tMapA[count].tmBfMCId.page;
            pFileset->fsPmcell.cell = pTagPage->tMapA[count].tmBfMCId.cell;
	    pFileset->fragLBN = NULL;
	    pFileset->fragLBNSize = 0;
	    pFileset->tagLBN = NULL;
	    pFileset->tagLBNSize = 0;
	    pFileset->tagList = NULL;
	    pFileset->fragOverlapHead = NULL;
	    pFileset->fragSlotArray = NULL;
            pFileset->origCloneTag.num = 0;
            pFileset->origCloneTag.seq = 0;
            pFileset->origClonePtr = NULL;
	    pFileset->groupUse = NULL;
	    pFileset->status = 0;
	    pFileset->errorCount = 0;
	    pFileset->next = NULL;
            
            /*
             * Get the extents for the fileset tag file.
	     * do NOT save the extents into the sbm, will be done in BMT.
	     * Pretend there is no fileset associated with this mcell,
	     * since there is no clone/original relationship yet.
             */
            pFsPmcell = &(pFileset->fsPmcell);
            status = collect_extents(domain, NULL, pFsPmcell, FALSE,
                                     &pFsTagLBN, &fsTagLBNSize, FALSE);
            
            if (SUCCESS != status) {
                return status;
            }
            pFileset->tagLBN = pFsTagLBN;
            pFileset->tagLBNSize = fsTagLBNSize;
            
            /*
             * Loop through the mcells and get the fileset ID and
             * the fileset name. Start with the primary mcell and
             * follow the next mcells if necessary.
             */
            volume = &(domain->volumes[pFileset->fsPmcell.vol]);
            pageNum = pFileset->fsPmcell.page;
	    mcellNum = pFileset->fsPmcell.cell;

	    /*
	     * Set to pageVol so we get into the loop, it will be reset
	     * once we do the convert_page_to_lbn call.
	     */
	    pageVol = pFileset->fsPmcell.vol;
            
            while ((FALSE == fsAttrFound) && (0 != pageVol)) {
                status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
                                             volume->volRbmt->bmtLBNSize,
                                             pageNum, &pageLBN, &pageVol);
                if (SUCCESS != status) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
				     "Can't convert BMT page %d to LBN.\n"),
			     pageNum);
                    return status;
                }
                status = read_page_from_cache(pageVol, pageLBN, &pBmtPage);
                if (SUCCESS != status) {
                    return status;
                }
            
                bmtPage = (bsMPgT *)pBmtPage->pageBuf;
                pMcell = &(bmtPage->bsMCA[mcellNum]);
                pRecord = (bsMRT *)pMcell->bsMR0;
                nextPageNum = pMcell->nextMCId.page;
		nextPageVol = pMcell->nextVdIndex;
		nextMcellNum = pMcell->nextMCId.cell;
 
                /*
                 * Loop through the records looking for type BSR_BFS_ATTR
                 * and get the fileset ID and fileset name from it.
                 */
                while ((FALSE == fsAttrFound) &&
                       (0 != pRecord->type) &&
                       (0 != pRecord->bCnt) &&
                       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
                    if (BSR_BFS_ATTR == pRecord->type) {
                        pFsAttr = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));
                        pFileset->filesetId = pFsAttr->bfSetId;
                        
                        /*
                         * Get memory for the fileset name and copy
                         * it from the record.
                         */
                        pFsName = (char *)ffd_malloc(BS_SET_NAME_SZ);
                        if (NULL == pFsName) {
			    writemsg(SV_ERR | SV_LOG_ERR,
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
					     "Can't allocate memory for %s.\n"),
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_789,
					     "fileset name"));
                            return NO_MEMORY;
                        }
                        strncpy(pFsName, pFsAttr->setName, BS_SET_NAME_SZ);
                        pFileset->fsName = pFsName;
                        
                        /*
                         * If this is a clone fileset, then set the
                         * clone status and save the original fs tag
                         * in the fileset structure.
                         */
                        if (pFsAttr->cloneId > 0) {
                            FS_SET_CLONE(pFileset->status);
                            pFileset->origCloneTag.num = pFsAttr->origSetTag.num;
                            pFileset->origCloneTag.seq = pFsAttr->origSetTag.seq;
                        }
                        
                        /*
                         * If this fileset has a clone, then set the
                         * status and save the clone tag in the fileset
                         * structure.
                         */
                        if (pFsAttr->numClones > 0) {
                            FS_SET_ORIGFS(pFileset->status);
                            pFileset->origCloneTag.num = pFsAttr->nextCloneSetTag.num;
                            pFileset->origCloneTag.seq = pFsAttr->nextCloneSetTag.seq;                            
                        }
                        fsAttrFound = TRUE;
                    } /* end if record type is BSR_BFS_ATTR */
                    
                    pRecord = (bsMRT *) (((char *)pRecord) + 
                                        roundup(pRecord->bCnt, sizeof(int))); 
                } /* end while looping through records */
                
                status = release_page_to_cache(pageVol, pageLBN, pBmtPage,
                                               FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}

                volume = &(domain->volumes[nextPageVol]);
                pageNum  = nextPageNum;
		pageVol  = nextPageVol;
		mcellNum = nextMcellNum;
                
            } /* end while attrs not found and page is not 0 */
            
            if (FALSE == fsAttrFound) {
                writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_790,
				 "Can't find fileset attributes for set tag %d.\n"),
                         (pTagPage->tpCurrPage * BS_TD_TAGS_PG) + count);
                return CORRUPT;
            }
	    /*
	     * Create the skip list for the tags.
	     */
	    maxTag = (tagNodeT *)ffd_malloc(sizeof(tagNodeT));
	    if (NULL == maxTag) {
	        writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_791,
				 "tag list tail"));
		return NO_MEMORY;
	    }

	    maxTag->tagSeq.num = MAXINT;
	    maxTag->tagSeq.seq = MAXINT;
	      
	    status = create_list(&compare_tags, &(pFileset->tagList), maxTag);
	    if (SUCCESS != status) {
		return status;
	    }

            /*
             * If the fileset is a clone or has a clone, then see if we
             * have the corresponding clone/original fileset already in
             * the list. If so, set them to point to each other.
             */
            
            if (FS_IS_CLONE(pFileset->status) || FS_IS_ORIGFS(pFileset->status)) {
                fsPtr = domain->filesets;
                while (fsPtr != NULL) {
                    if ((pFileset->origCloneTag.num == fsPtr->filesetId.dirTag.num) &&
                        (pFileset->origCloneTag.seq == fsPtr->filesetId.dirTag.seq)) {
                        /*
                         * We found the fileset that should be the clone/original
                         * for this one. Make sure it has this fileset's tag
                         * number saved in it. If not, this should be an error.
                         * Need to figure out what to do in this case.
                         */
                        if ((fsPtr->origCloneTag.num == pFileset->filesetId.dirTag.num) &&
                            (fsPtr->origCloneTag.seq == pFileset->filesetId.dirTag.seq)) {
                            pFileset->origClonePtr = fsPtr;
                            fsPtr->origClonePtr = pFileset;
                            break;
                        } else {
                            /*
                             * One of the fileset attribute records is wrong.
                             * FUTURE: Figure out which one is wrong and fix it.
                             */
                            if (FS_IS_CLONE(pFileset->status)) {
                                writemsg(SV_ERR | SV_LOG_ERR,
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_792,
						 "Mismatch between clone fileset %s and original %s.\n"),
					 pFileset->fsName, fsPtr->fsName);
                            } else {
                                writemsg(SV_ERR | SV_LOG_ERR,
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_792, 
						 "Mismatch between clone fileset %s and original %s.\n"),
					 fsPtr->fsName, pFileset->fsName);
                            }
                        } /* end if clone/original points back */
                    } /* end if clone/original tag matches */
                    
                    fsPtr = fsPtr->next;
                } /* end while not at end of fileset list */
            } /* end if fileset is a clone or original w/clone */
	    /*
	     * Add this fileset to the front of the fileset linked list.
	     */
	    pFileset->next = domain->filesets;
	    domain->filesets = pFileset;
        } /* end if tag is active */
    } /* end for each tag on the page */

    /*
     * FUTURE: check that we have found the corresponding clone/original
     * fileset for every fileset that is/has a clone. Need to decide what
     * to do if we can't find the corresponding fileset (probably only a
     * problem if we have a clone and not the original). For now just print
     * a message if we haven't found the corresponding fileset.
     */
    fsPtr = domain->filesets;
    while (fsPtr != NULL) {
        if (FS_IS_CLONE(fsPtr->status) && (NULL == fsPtr->origClonePtr)) {
            writemsg(SV_ERR | SV_LOG_ERR, 
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_793,
			     "Original fileset not found for clone %s.\n"),
                     fsPtr->fsName);
        }
        if (FS_IS_ORIGFS(fsPtr->status) && (NULL == fsPtr->origClonePtr)) {
            writemsg(SV_ERR | SV_LOG_ERR, 
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_794,
			     "Clone fileset not found for fileset %s.\n"),
                     fsPtr->fsName);
        }
        fsPtr = fsPtr->next;
    }
    return SUCCESS;
} /* end save_fileset_info */


/*
 * Function Name: corrupt_exit (3.12)
 *
 * Description: This function is used to exit when we find corruptions
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
void corrupt_exit(char *message)
{
    char *funcName = "corrupt_exit";
    if (NULL != message) {
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
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_795, 
			 "%s found errors on disk it could not fix.\n"),
		 Prog);
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_796, 
			 "This domain should be restored from backup\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_797,
			 "tapes, or you can use the salvage tool to\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_798,
			 "pull what files it can find off of this domain.\n\n"));
    }

    exit (EXIT_CORRUPT);
} /* end corrupt_exit */


/*
 * Function Name: clean_exit (3.13)
 *
 * Description:This function is used to exit cleanly. This include
 *             flushing changes out to the disk, and creating the undo
 *             files. Note that nothing has been modified on disk
 *             prior to this call. Make sure to restore_undo_files if
 *             this fails.
 *
 * Input parameters:
 *
 * Output parameters:
 *
 * Exit Value:0 (Success), 1 (Corrupt), or 2 (Failure).
 * 
 */
void clean_exit(domainT *domain)
{
    char *funcName = "clean_exit";
    filesetT *fileset;
    int status;
    char answer[10];
    int volNum;
    ulong_t soflags; /* Used by safe_open */
    extern errno;

    /*
     * Very last thing do before we exit is correct the SBM.
     */
    if ((NULL == Options.type) ||
	(OT_IS_FILES(Options.typeStatus)) ||
	(OT_IS_BMT(Options.typeStatus)) ||
	(OT_IS_SBM(Options.typeStatus))) {

	writemsg(SV_NORMAL | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_799,
			 "Checking the SBM.\n"));
	status = correct_sbm(domain);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		writemsg(SV_VERBOSE | SV_LOG_FIXED,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_800,
				 "Corrected the SBM.\n"));
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_801,
				 "Error correcting the SBM.\n"));
		failed_exit();
	    }
	}
    } /* end of Option.Types */

    writemsg(SV_DEBUG | SV_LOG_SUM, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_802, 
		     "Cache stats:  %d writehits, %d readhits, %d misses\n"),
	     cache->writeHits, cache->readHits, cache->misses);

    status = create_undo_files(domain);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_803, 
			 "Undo files were not created.\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_804, 
			 "If you continue you will not be able to be undo the changes.\n"));
	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_805,
			 "Do you want %s to write its changes to disk? (y/[n])  "),
		 Prog);
	status = prompt_user_yes_no(answer, "n");
	if (SUCCESS != status) {
	    failed_exit();
	}
	/*
	 * answer[0] must be 'y' or 'n'.
	 */
	if ('n' == answer[0]) {
	    failed_exit();
	}
    }

    if (FALSE == Options.nochange) {
	status = write_cache_to_disk();
	if (SUCCESS != status) {
	    restore_undo_files(domain);
	    failed_exit();
	}
    }

    close_domain_volumes(domain);

    /*
     * Future: print summary statistics to log and tty
     */

    if (TRUE == Options.activate) {
	status = activate_domain(domain);

	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_806,
			     "Activation of the domain failed.\n"));
	    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_807, 
			     "Restoring the domain's undo files.\n"));
	    for (volNum = 0 ; volNum <= domain->highVolNum ; volNum++) {
		/*
		 * Must use volName instead of volFD because the FDs have
		 * been closed.
		 */
		if (NULL == domain->volumes[volNum].volName) {
		    continue;
		}

		/*
		 * Safe_open is used to close a possible security problem
		 *
		 * volName is a Block device
		 */
		soflags = (OPN_TYPE_BLK);

		domain->volumes[volNum].volFD =
		    safe_open(domain->volumes[volNum].volName, 
			      O_RDWR | O_SYNC, soflags);

		if (-1 == domain->volumes[volNum].volFD) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_758,
				     "Can't open device '%s', error=%d.\n"),
			     domain->volumes[volNum].volName, errno);
		    writemsg(SV_ERR| SV_LOG_ERR | SV_CONT, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_808, 
				     "Can't restore from undo files.\n"));

		    failed_exit();
		}
	    }
	    
	    status = restore_undo_files(domain);
	    if (SUCCESS != status) {
		failed_exit();
	    } else {
		corrupt_exit(NULL);
	    }
	}
    }

    if (TRUE == Options.clearClones) {
	fileset = domain->filesets;
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_809, 
			 "If you clear the clones from this domain, you will\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_810,
			 "not be able to restore from the undo files.\n"));
	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_811, 
			 "Do you want to remove the clones? ([y]/n)  "));

	status = prompt_user_yes_no(answer, "y");
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_812, 
			     "Not removing the clones.\n"));
	    answer[0] = 'n';
	}
	if ('y' == answer[0]) {
	    while (NULL != fileset) {
		if (NULL != Options.filesetName) {
		    /*
		     * We only want to work on specified setName.
		     */
		    if (0 != strcmp(Options.filesetName, 
				    fileset->fsName)) {
			fileset = fileset->next;
			continue;
		    }
		}
		    
		if (FS_IS_CLONE(fileset->status)) {
		    status = remove_clone_from_domain(domain->dmnName, 
						      fileset->fsName);
		    if (SUCCESS != status) {
			/*
			 * FUTURE: Not sure what to do here, can we 
			 * continue or is our best bet to exit corrupt.
			 */
			corrupt_exit(NULL);
		    }
		}
		fileset = fileset->next;
	    } /* end while filesystems exist */
	} /* end do they really want us to clear clones */
    } /* if clearclones is on */


    writemsg(SV_NORMAL | SV_LOG_INFO, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_813, "Completed.\n"));
    /*
     * Close the information log file.
     */
    fclose(Options.logHndl);

    exit (EXIT_SUCCESS);
} /* end clean_exit */


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
void failed_exit(void)
{
    char *funcName = "failed_exit";    
    /*
     * Print a message letting the use know we are exiting.
     */
    writemsg(SV_ERR | SV_CONT, "\n");
    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_814, 
		     "%s is not able to continue, no changes made to domain, exiting.\n"), 
	     Prog);
    exit (EXIT_FAILED);

} /* end failed_exit */


/*
 *	used in create_undo_files and restore_undo_files to generate
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

/*
 * code straight out of POSIX.2 Draft 11
 */

unsigned long
crc_32(b, n, s)
register unsigned char *b;	/* byte sequence to checksum */	
register int 		n;	/* length of sequence */
register ulong 		s;	/* initial checksum value */
{

	register int i, aux;

unsigned long crctab[] = {
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
 * Function Name: create_undo_files (3.14)
 *
 * Description: This routine creates the undo files needed should the
 *              user wish to undo the changes made by fixfdmn.
 *
 * Input parameters:
 *     domain: The domain structure for which undo files need to be created.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or FAILURE.
 * 
 */
int create_undo_files(domainT *domain)
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
    int		undopage;
    struct stat	statbuf;
    char        answer[10];
    int         loop;
    ulong_t     soflags;/* Used by safe_open */

    undofd  = -1;
    indexfd = -1;
    undopage = 0;

    /*
     * If there is nothing on the write cache we can return SUCCESS.
     */
    status = find_first_node(cache->writeCache, (void *)&pPage, &pNode);
    if (SUCCESS != status) {
	if (NOT_FOUND == status) {
	    return SUCCESS;
	} else {
	    return status;
	}
    }	

    if (FALSE == Options.nochange) {
	/*
	 * Rotate undofile version starting at .0, incrementing to .9.
	 * Ensure that the undofile following the one we're writing is
	 * deleted so restore undo file knows where to start.
	 */
	for (version = 0 ; version <= 9 ; version++) {
	    sprintf(undofile, "%s/undo.%s.%01d", Options.undoDir,
		    domain->dmnName, version);
	    sprintf(indexfile, "%s/undoidx.%s.%01d", Options.undoDir,
		    domain->dmnName, version);

	    if ((0 == stat(undofile, &statbuf)) ||
		(0 == stat(indexfile, &statbuf))) 
	    {
		continue;
	    }
	    break;
	}

	/*
	 * Unlink next one.
	 */
	if (9 == version) {
	    sprintf(tmpUndo, "%s/undo.%s.%01d", Options.undoDir,
		    domain->dmnName, 0);
	    sprintf(tmpIndex, "%s/undoidx.%s.%01d", Options.undoDir,
		    domain->dmnName, 0);
	} else {
	    sprintf(tmpUndo, "%s/undo.%s.%01d", Options.undoDir,
		    domain->dmnName, version + 1);
	    sprintf(tmpIndex, "%s/undoidx.%s.%01d", Options.undoDir,
		    domain->dmnName, version + 1);
	}
	writemsg(SV_DEBUG, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_815,
				   "Unlinking files '%s' and '%s'.\n"),
		 tmpUndo,tmpIndex);

	/*
	 * Ignore return code - it will fail if the file doesn't exist.
	 */
	unlink(tmpUndo);
	unlink(tmpIndex);

	/*
	 * Safe_open is used to close a possible security problem
	 *
	 * Now safe_open the undo files.
	 *     
	 * Undo files could be created:
	 *   in /tmp so need to allow sticky bit. 
	 *   on a NFS filesystem
	 *   in a relative path (ie ../logfile)
	 *   on a path with sym-links owned by other than UID
	 *   in directories which are writeable to group
	 *   were the current directory is writeable to group/world
	 *   with a different owner (NFS root becomes nobody)
	 *
	 * safe_open sets mode to 0600.
	 */
	soflags = (OPN_TRUST_STICKY_BIT | OPN_FSTYPE_REMOTE | 
		   OPN_RELATIVE | OPN_TRUST_DIR_OWNERS | OPN_TRUST_GROUP |
		   OPN_TRUST_STARTING_DIRS | OPN_UNOWNED);
	loop = 3;
	do {
	    undofd = safe_open(undofile, O_RDWR | O_CREAT | O_EXCL, 
			       soflags);
	} while (-1 == undofd && ENOENT == errno && --loop);

	if (-1 == undofd) {
	    /* 
	     * Already checked for file existing - Unknown error. 
	     */
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_816, 
			     "Can't open file '%s'.\n"), 
		     undofile);
	    perror(Prog);
	    return FAILURE;
	}

	loop = 3;
	do {
	    indexfd = safe_open(indexfile, O_RDWR | O_CREAT | O_EXCL, 
				soflags);
	} while (-1 == indexfd && ENOENT == errno && --loop);

	if (-1 == indexfd) {
	    /* 
	     * Already checked for file existing - Unknown error. 
	     */
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_816,
			     "Can't open file '%s'.\n"), 
		     indexfile);
	    perror(Prog);
	    close(undofd);
	    undofd = -1;
	    return FAILURE;
	}	

	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_817, 
			 "Creating undo files for domain %s.\n"),
		 domain->dmnName);
    } /* if nochange */

    status = find_first_node(cache->writeCache, (void *)&pPage, &pNode);
    if (SUCCESS != status) {
	/*
	 * Not found is caught above
	 */
	return status;
    }

    while (NULL != pPage) {
	if (FALSE == P_IS_MODIFIED(pPage->status)) {
	    status = find_next_node(cache->writeCache, (void *)&pPage, &pNode);
	    if (FAILURE == status) {
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
	while (NULL != pMessage) {
	    if (NULL != pMessage->message) {
		char diskArea[10];
		
		switch (pMessage->diskArea) {
		    case(RBMT):
			strcpy(diskArea, 
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_818,
				       "RBMT"));
			break;
		    case(BMT):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_819,
				       "BMT"));
			break;
		    case(SBM):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_820,
				       "SBM"));
			break;
		    case(log):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_821,
				       "LOG"));
			break;
		    case(frag):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_822, 
				       "FRAG"));
			break;
		    case(tagFile):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_823,
				       "TAG FILE"));
			break;
		    case(dir):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_824, 
				       "DIR"));
			break;
		    case(tag):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_825,
				       "TAG"));
			break;
		    case(misc):
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_826,
				       "MISC"));
			break;
		    default:
			strcpy(diskArea,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_732,
				       "UNKNOWN"));
			break;
		} /* end of switch */

		writelog(SV_LOG_FIXED, 
			 "%s | %s:%u | %s\0",
			 diskArea, domain->volumes[pPage->vol].volName,
			 pPage->pageLBN, pMessage->message);
		free(pMessage->message);
	    } /* message existed */
	    prevMessage = pMessage;
	    pMessage = pMessage->next;
	    free(prevMessage);
	} /* while more message exist */
	pPage->messages = NULL;

	if (TRUE == Options.nochange) {		  
	    /*
	     * No reason do the next section, so just continue.
	     */
	    status = find_next_node(cache->writeCache, 
				    (void *)&pPage, &pNode);
	    if (FAILURE == status) {
		return status;
	    }
	    continue;
	}

	/*
	 * Read the original page from disk.
	 */
	if ((off_t)-1 == lseek(domain->volumes[pPage->vol].volFD,
			       pPage->pageLBN * (long)DEV_BSIZE,
			       SEEK_SET)) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_192, 
			     "Can't lseek to block %u on '%s'.\n"),
		     pPage->pageLBN, 
		     domain->volumes[pPage->vol].volName);
	    perror(Prog);
	    corrupt_exit(NULL);
	}

	bytes = read(cache->domain->volumes[pPage->vol].volFD,
		     origPageBuf, PAGESIZE);
	    
	if ((off_t)-1 == bytes) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_193,
			     "Can't read page at block %u on '%s'.\n"), 
		     pPage->pageLBN, 
		     domain->volumes[pPage->vol].volName);
	    perror(Prog);
	    corrupt_exit(NULL);
	} else if (PAGESIZE != bytes) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_194, 
			     "Can't read page at block %u on '%s', %d of %d bytes read.\n"),
		     pPage->pageLBN, domain->volumes[pPage->vol].volName,
		     bytes, PAGESIZE);
	    corrupt_exit(NULL);
	}

	/* Calculate a validation checksum for this page. */
	crc = crc_32(origPageBuf, PAGESIZE, 0);

	sprintf(tmpBuf, "%d\t%d\t%u\t%08x\n", undopage++,
		pPage->vol, pPage->pageLBN, crc);

	/*
	 * write the undo index file.
	 */
	writeDone = FALSE;
	while (FALSE == writeDone) {
	    bytes = write(indexfd, tmpBuf, strlen(tmpBuf));
	    if (strlen(tmpBuf) == bytes) {
		writeDone = TRUE;
	    } else if ((-1 == bytes) && (ENOSPC != errno)) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_827,
				 "Can't write to '%s'.\n"), indexfile);
		perror(Prog);
		close(undofd);
		close(indexfd);
		return FAILURE;
	    } else {
		if (-1 == bytes) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_827,
				     "Can't write to '%s'.\n"), indexfile);
		    perror(Prog);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_828, 
				     "Can't write to '%s', only wrote %d of %d bytes.\n"), 
			     indexfile, bytes, strlen(tmpBuf));
		}
		writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_829, 
				 "This could be because the filesystem is out of space.\n"));
		writemsg(SV_ERR | SV_CONT,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_830,
				 "Do you want to add more space and continue? (y/[n])  "));

		status = prompt_user_yes_no(answer, "n");
		if (SUCCESS != status) {
		    failed_exit();
		}
		/*
		 * answer[0] must be 'y' or 'n'.
		 */
		if ('n' == answer[0]) {
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}
		
		/*	
		 * lseek backwards over the partial write.
		 */
		if ((off_t)-1 == lseek(indexfd, (-1 * bytes), SEEK_SET)) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_192,
				     "Can't lseek to block %u on '%s'.\n"),
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
	while (FALSE == writeDone) {
	    bytes = write(undofd, origPageBuf, PAGESIZE);
	    if (PAGESIZE == bytes) {
		writeDone = TRUE;
	    } else if ((-1 == bytes) && (ENOSPC != errno)) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_827,
				 "Can't write to '%s'.\n"), undofile);
		perror(Prog);
		close(undofd);
		close(indexfd);
		return FAILURE;
	    } else {
		if (-1 == bytes) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_827,
				     "Can't write to '%s'.\n"), undofile);
		    perror(Prog);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_828, 
				     "Can't write to '%s', only wrote %d of %d bytes.\n"), 
			     undofile, bytes, strlen(tmpBuf));
		}
		writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_829,
				 "This could be because the filesystem is out of space.\n"));
		writemsg(SV_ERR | SV_CONT,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_830,
				 "Do you want to add more space and continue (y/[n])  "));

		status = prompt_user_yes_no(answer, "n");
		if (SUCCESS != status) {
		    failed_exit();
		}
		/*
		 * answer[0] must be 'y' or 'n'.
		 */
		if ('n' == answer[0]) {
		    close(undofd);
		    close(indexfd);
		    return FAILURE;
		}
		
		/*
		 * lseek backwards over the partial write.
		 */
		if ((off_t)-1 == lseek(indexfd, (-1 * bytes), SEEK_SET)) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_192,
				     "Can't lseek to block %u on '%s'.\n"),
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
	if (FAILURE == status) {
	    close(undofd);
	    close(indexfd);
	    return status;
	}
    } /* End of loop on all pages in write cache */

    if (FALSE == Options.nochange) {		  
	fsync(undofd);
	fsync(indexfd);
	close(undofd);
	close(indexfd);
    }

    /* FUTURE: Read the undofile and verify checksums? */

    return SUCCESS;
} /* end create_undo_files */


/* 
 * Function Name: write_cache_to_disk (3.15) 
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
int write_cache_to_disk(void)
{
    char	*funcName = "write_cache_to_disk";
    int		status;
    pageT	*pPage;
    nodeT	*pNode;
    domainT     *domain;
    ssize_t	bytes;
    int		vol;
#ifdef DEBUG
    char	answer[80];
#endif
    
    domain = cache->domain;

    status = find_first_node(cache->writeCache, (void *)&pPage, &pNode);
    if (SUCCESS != status) {
	if (NOT_FOUND == status) {
	    writemsg(SV_NORMAL | SV_LOG_INFO, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_831,
			     "No pages on the domain have been modified.\n"));
	    return SUCCESS;
	} else {
	    return status;
	}
    }

#ifdef DEBUG
    writemsg(SV_ERR, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_832, 
		     "About to modify domain--if you want to continue,\n"));
    writemsg(SV_ERR | SV_CONT, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_833, 
		     "you must type in \"yes write it\" to do so: "));
    status = prompt_user(answer, 80);
    if (SUCCESS != status) {
	answer[0] = '\0';
    }
#endif

    while (NULL != pPage) {
	if (FALSE == P_IS_MODIFIED(pPage->status)) {
	    status = find_next_node(cache->writeCache, (void *)&pPage, 
				    &pNode);
	    if (FAILURE == status) {
		return status;
	    }
	    continue;
	}

	/*
	 * Lseek to the correct page on disk.
	 */
	if ((off_t)-1 == lseek(domain->volumes[pPage->vol].volFD,
			       pPage->pageLBN * (long)DEV_BSIZE,
			       SEEK_SET)) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_192,
			     "Can't lseek to block %u on '%s'.\n"),
		     pPage->pageLBN,
		     domain->volumes[pPage->vol].volName);
	    perror(Prog);
	    return FAILURE;
	}

#ifdef DEBUG
	if (0 == strcmp(catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_834, 
				"yes write it\n"), answer)) {
#endif
	    /*
	     * Write the new page to disk.
	     */
	    bytes = write(domain->volumes[pPage->vol].volFD,
			  pPage->pageBuf, PAGESIZE);
	    if (-1 == bytes) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_827,
				 "Can't write to '%s'.\n"), 
			 domain->volumes[pPage->vol].volName);
		perror(Prog);
		return FAILURE;
	    } else if (PAGESIZE != bytes) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_828,
				 "Can't write to '%s', only wrote %d of %d bytes.\n"), 
			 domain->volumes[pPage->vol].volName,
			 bytes, PAGESIZE);
		return FAILURE;
	    }
#ifdef DEBUG
	}
	else {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_835, 
			     "NOT writing new volume %d LBN %u to disk.\n"),
		     pPage->vol, pPage->pageLBN);
	}
#endif

	status = find_next_node(cache->writeCache, (void *)&pPage, &pNode);
	if (FAILURE == status) {
	    return status;
	}
    }
    for (vol = 1 ; vol <= domain->highVolNum ; vol++) {
	if (-1 != domain->volumes[vol].volFD) {
	    fsync(domain->volumes[vol].volFD);
	}
    }

    return SUCCESS;
} /* end write_cache_to_disk */


/*
 * Function Name: close_domain_volumes (3.16)
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
        
        if (-1 != volumes[volNum].volFD) {
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
 * Description: This routine verifies that the domain is mountable,
 *              but only after checking with the user to ensure s/he
 *              wants to. It does this by mounting every non-clone
 *              fileset.
 *
 * Input parameters:
 *     domain: The domain to activate.
 *
 * Output parameters: N/A
 *
 * Side Effects:
 *	This routine could possibly cause a system panic.
 *
 * Returns: SUCCESS or FAILURE
 *
 */
int activate_domain(domainT *domain)
{
    char		*funcName = "activate_domain";
    char		answer[80];
    time_t		randomnum;
    char		topdir[MAXPATHLEN];
    char                *dp;
    filesetT		*fileset;
    char		*mntdir;
    char		mntdir1[MAXPATHLEN];
    char		mntdir2[MAXPATHLEN];
    char		dmnfset[MAXPATHLEN];
    struct advfs_args	args;
    char		*argp;
    struct timeval	tv;
    struct timezone	tz;
    int			err;
    int                 lkHndl;
    int                 seed;
    int			failed;
    filesetT		*firstfileset;
    int			status;

    failed = FALSE;
    writemsg(SV_ERR, "\n");
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_836, 
		     "*** WARNING! ***\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT, "\n");
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_837, 
		     "%s is about to activate this domain to ensure it has\n"), 
	     Prog);
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_838, 
		     "correctly fixed the domain.  There is a small chance this\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_839, 
		     "could cause a system panic, taking the system down.  It\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_840, 
		     "could also cause a domain panic, which would only affect\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_841,
		     "this file domain, but not the rest of the system.  For more\n"));
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_842, 
		     "information, see the %s(8) man page.\n"), Prog);
    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT, "\n");
    writemsg(SV_ERR | SV_CONT,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_843, 
		     "Do you want %s to activate this domain now? (y/[n])  "), 
	     Prog);

    status = prompt_user_yes_no(answer, "n");
    if ((SUCCESS != status) || ('n' == answer[0])) {
	writemsg(SV_VERBOSE | SV_LOG_WARN, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_844, 
			 "Not activating the domain.\n"));
	return SUCCESS;
    }

    /*
     * Create a unique file name to mount the directory on
     *
     * mkdtemp creates a unique name and creates the directory with
     * permission of 0700.  This helps close a possible security hole.
     */
    strcpy(topdir, "/tmp/.fixfdmn_XXXXXXXX");
    if (NULL == (dp = mkdtemp(topdir))) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_845, 
			 "Can't create unique mount directory.\n"));
	return FAILURE;
    }

    sprintf(mntdir1, "%s/mnt1", dp);
    if (0 != mkdir(mntdir1, 0700)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_846, 
			 "Can't make directory %s.\n"), mntdir1);
	perror(Prog);
	return FAILURE;
    }

    sprintf(mntdir2, "%s/mnt2", dp);
    if (0 != mkdir(mntdir2, 0700)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_846, 
			 "Can't make directory %s.\n"), mntdir2);
	perror(Prog);
	return FAILURE;
    }

    /*
     * Mount the first fileset, and leave it mounted until the end.
     * Then mount and unmount each subsequent fileset.  This allows
     * a maximum of two filesets to be mounted at a time, preventing
     * an overflow of the max number of simultaneous mounts, but still
     * prevents activation and deactivation of the domain more than once.
     */

    fileset = domain->filesets;
    firstfileset = fileset;
#ifdef DEBUG
    writemsg(SV_ERR | SV_LOG_ERR, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_847, 
		     "About to activate domain, if you want to continue,\n"));
    writemsg(SV_ERR | SV_CONT, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_848, 
		     "you must type in \"yes write it\" to do so:  "));
    status = prompt_user(answer, 80);
    if (SUCCESS != status) {
	answer[0] = '\0';
    }
#endif

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

	/*
	 * This code is taken from mount_advfs.c.
	 */
	sprintf(dmnfset, "%s#%s", domain->dmnName, fileset->fsName);
	args.fspec = dmnfset;
	args.exroot = -2;
	args.exflags = M_EXRDONLY;
	if (0 != gettimeofday(&tv, &tz)) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_849, 
			     "Can't get the time of day.\n"));
	    failed = TRUE;
	    break;
	}
	seed = fileset->filesetId.dirTag.num;
	seed |= tv.tv_usec;
	srandom(seed);
	args.fsid = random();
	argp = (caddr_t)&args;

	lkHndl = lock_file(MSFS_DMN_DIR, LOCK_SH, &err);
	/* Ignore errors from lock_file because mount_advfs also does. */

	if (firstfileset == fileset) {
	    mntdir = mntdir1;
	} else {
	    mntdir = mntdir2;
	}

#ifdef DEBUG
	if (0 == strcmp(catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_834, 
				"yes write it\n"), answer)) {
#endif
	    if (0 != mount(MOUNT_MSFS, mntdir, 0, argp)) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_850,
				 "Can't mount '%s' on '%s'.\n"),
			 dmnfset, mntdir);
		perror(Prog);
		if (lkHndl >= 0) {
		    unlock_file(lkHndl, &err);
		}
		failed = TRUE;
		break;
	    }
#ifdef DEBUG
	}
	else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_851,
			     "Didn't actually mount %s on %s.\n"),
		     dmnfset, mntdir);
	}
#endif

	if (lkHndl >= 0) {
	    unlock_file(lkHndl, &err);
 	}

	if (firstfileset != fileset) {
#ifdef DEBUG	    
	    if (0 == strcmp(catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_834, 
				    "yes write it\n"), answer)) {
#endif
		if (0 != umount(mntdir, 0)) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_852,
				     "Can't unmount '%s'.\n"), mntdir);
		    perror(Prog);
		    failed = TRUE;
		    break;
		}
#ifdef DEBUG
	    }
	    else {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_853, 
				 "Didn't actually unmount %s.\n"), mntdir);
	    }
#endif
	}

	fileset = fileset->next;
    } /* End loop on filesets */

    if ( NULL != firstfileset && ( Options.filesetName == NULL || ( strcmp(Options.filesetName,firstfileset->fsName) == 0 ) ) ) {
#ifdef DEBUG
	if (0 == strcmp(catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_834,
				"yes write it\n"), answer)) {
#endif
	    if (0 != umount(mntdir1, 0)) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_852,
				 "Can't unmount '%s'.\n"), mntdir1);
		perror(Prog);
		failed = TRUE;
	    }
#ifdef DEBUG
	}
	else {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_853, 
			     "Didn't actually unmount %s.\n"), mntdir1);
	}
#endif
    }

    if (0 != rmdir(mntdir1)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_854, 
			 "Can't remove directory '%s'.\n"), mntdir1);
	perror(Prog);
	failed = TRUE;
    }	    
    if (0 != rmdir(mntdir2)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_854, 
			 "Can't remove directory '%s'.\n"), mntdir2);
	perror(Prog);
	failed = TRUE;
    }	    
    if (0 != rmdir(topdir)) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_854, 
			 "Can't remove directory '%s'.\n"), topdir);
	perror(Prog);
	failed = TRUE;
    }

    if (TRUE == failed) {
	return FAILURE;
    }

    writemsg(SV_NORMAL | SV_LOG_INFO,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_855, 
		     "Successfully activated the domain.\n"));

    return SUCCESS;
} /* end activate_domain */


/*
 * Function Name: remove_clone_from_domain (3.18)
 *
 * Description: This routine removes a clone from the domain. It does
 *              not remove that clone's entry (if there is one) from
 *              /etc/fstab as there is a good chance the user will
 *              want to re-create the clone.
 *
 * Input parameters:
 *     domainName: The domain to remove the clone from.
 *     cloneName: The clone to remove.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or FAILURE.
 * */
int remove_clone_from_domain(char *domainName, char *cloneName)
{
    char	*funcName = "remove_clone_from_domain";
    mlStatusT	status;
#ifdef DEBUG
    char	answer[80];

    writemsg(SV_ERR, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_832,
		     "About to modify domain--if you want to continue,\n"));
    writemsg(SV_ERR | SV_CONT, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_833,
		     "you must type in \"yes write it\" to do so: "));
    status = prompt_user(answer, 80);
    if (SUCCESS != status) {
	answer[0] = '\0';
    }
#endif

#ifdef DEBUG
    if (0 == strcmp(catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_834, 
			    "yes write it\n"), answer)) {
#endif
	status = msfs_fset_delete(domainName, cloneName);
#ifdef DEBUG
    }
    else {
	status = EOK;
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_856,
			 "Didn't actually delete clone %s from domain %s.\n"),
		 cloneName, domainName);
    }
#endif

    if (EOK != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_857, 
			 "Can't delete clone '%s'. Error [%d] = %s\n"),
		 cloneName, status, BSERRMSG(status) );
    }

    return SUCCESS;
} /* end remove_clone_from_domain */


/*
 * Function Name: restore_undo_files (3.19)
 *
 * Description:This routine restores a domain to its state prior to a
 *             previous run of fixfdmn by reading that run's undo files.
 *
 * Input parameters:
 *     undo_file: The path to the undo file.
 *     undo_index_file: The path to the undo index file.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or FAILURE.
 * */
int restore_undo_files(domainT *domain)
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
    int		undopage;
    int         vol;
    LBNT         lbn;
    struct stat	statbuf;
    int		status;
    ulong_t     soflags; /* Used by safe_open */

    undofd = -1;
    indexfd = -1;
    undopage = 0;

    for (version = 0 ; version <= 9 ; version++) {
	sprintf(undofile, "%s/undo.%s.%01d", Options.undoDir,
		domain->dmnName, version);
	sprintf(indexfile, "%s/undoidx.%s.%01d", Options.undoDir,
		domain->dmnName, version);

	if ((-1 == stat(undofile, &statbuf)) ||
	    (-1 == stat(indexfile, &statbuf)))
	{
	    /*
	     * This version doesn't exist, go on to the next.
	     */
	    continue;
	}

	/*
	 * Check if next one exists.
	 */
	if (9 == version) {
	    sprintf(tmpUndo, "%s/undo.%s.%01d", Options.undoDir,
		    domain->dmnName, 0);
	    sprintf(tmpIndex, "%s/undoidx.%s.%01d", Options.undoDir,
		    domain->dmnName, 0);
	} else {
	    sprintf(tmpUndo, "%s/undo.%s.%01d", Options.undoDir,
		    domain->dmnName, version + 1);
	    sprintf(tmpIndex, "%s/undoidx.%s.%01d", Options.undoDir,
		    domain->dmnName, version + 1);
	}

	if ((-1 == stat(tmpUndo, &statbuf)) &&
	    (-1 == stat(tmpIndex, &statbuf)))
	{
	    /*
	     * Next version doesn't exist.  Open this one, which does.
	     *
	     * Safe_open is used to close a possible security problem
	     *
	     * Undo files could be created: 
	     *   in /tmp so need to allow sticky bit. 
	     *   on a NFS filesystem
	     *   in a relative path (ie ../logfile)
	     *   on a path with sym-links owned by other than UID
	     *   in directories which are writeable to group
	     *   were current directory is writeable to group/world
	     *   with a different owner (NFS root becomes nobody)
	     */
	    soflags = (OPN_TRUST_STICKY_BIT | OPN_FSTYPE_REMOTE | 
		       OPN_RELATIVE | OPN_TRUST_DIR_OWNERS | 
		       OPN_TRUST_GROUP | OPN_TRUST_STARTING_DIRS | 
		       OPN_UNOWNED);
	    undofd = safe_open(undofile, O_RDONLY, soflags);

	    if (-1 == undofd) {
		/* 
		 * Already checked for file existing - Unknown error. 
		 */
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_816, 
				 "Can't open file '%s'.\n"), undofile);
		perror(Prog);
		return FAILURE;
	    }

	    indexfd = safe_open(indexfile, O_RDONLY, soflags);

	    if (-1 == indexfd) {
		/* 
		 * Already checked for file existing - Unknown error.
		 */
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_816,
				 "Can't open file '%s'.\n"), indexfile);
		perror(Prog);
		close(undofd);
		undofd = -1;
		return FAILURE;
	    }	
	    break;
	}
    }

    if (-1 == undofd || -1 == indexfd) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_858,
			 "Can't find any undo files for '%s' in '%s'.\n"),
		 domain->dmnName, Options.undoDir);
	return FAILURE;
    } else {
	writemsg(SV_NORMAL |SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_859, 
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

    count = fscanf(idx, "%d\t%d\t%u\t%08x\n",
		    &undopage, &vol, &lbn, &savedcrc);
    while (4 == count) {
	bytes = read(undofd, origPageBuf, PAGESIZE);
	if ((off_t)-1 == bytes) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_860, 
			     "Can't read page at byte %d of '%s'.\n"), 
		     (undopage * PAGESIZE), undofile);
	    perror(Prog);
	    return FAILURE;
	} else if (PAGESIZE != bytes) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_861, 
			     "Can't read page at byte %d of '%s', %d of %d bytes read.\n"),
		     (undopage * PAGESIZE), undofile, bytes, PAGESIZE);
	    return FAILURE;
	}

	crc = crc_32(origPageBuf, PAGESIZE, 0);

	if (crc != savedcrc) {
	    writemsg(SV_ERR | SV_LOG_WARN, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_862, 
			     "WARNING: checksum failed for undopage %d: disk volume %s, LBN %u.\n"),
		     undopage, domain->volumes[vol].volName, lbn);
	    writemsg(SV_ERR | SV_CONT, "\n");
	    writemsg(SV_ERR | SV_CONT, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_863, 
			     "Do you want to continue restoring the undo file? (y/[n])  "));
	    status = prompt_user_yes_no(answer, "n");
	    if (SUCCESS != status) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_864,
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
			       lbn * (long)DEV_BSIZE,
			       SEEK_SET)) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_192, 
			     "Can't lseek to block %u on '%s'.\n"),
		     lbn, domain->volumes[vol].volName);
	    perror(Prog);
	    return FAILURE;
	}
	/*
	 * Blast the original page back to disk.
	 */
	bytes = write(domain->volumes[vol].volFD,
		      origPageBuf, PAGESIZE);
	if ((ssize_t)-1 == bytes) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_827, 
			     "Can't write to '%s'.\n"), 
		     domain->volumes[vol].volName);
	    perror(Prog);
	    return FAILURE;
	} else if (PAGESIZE != bytes) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_828, 
			     "Can't write to '%s', only wrote %d of %d bytes.\n"), 
		     domain->volumes[vol].volName, bytes, PAGESIZE);
	    return FAILURE;
	}

	count = fscanf(idx, "%d\t%d\t%u\t%08x\n",
			&undopage, &vol, &lbn, &savedcrc);
    }
    if (EOF != count) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_865, 
			 "Can't scan file '%s' for data.\n"),
		 indexfile);
	perror(Prog);
	return FAILURE;
    }
    close(undofd);
    close(indexfd);
    for (vol = 1 ; vol <= domain->highVolNum ; vol++) {
	if (-1 != domain->volumes[vol].volFD) {
	    fsync(domain->volumes[vol].volFD);
	}
    }

    return SUCCESS;

} /* end restore_undo_files */


/*
 * Function Name: convert_page_to_lbn (3.nn)
 *
 * Description:
 *  This function converts a page location within a file/buffer to an
 *  addressable volume/logical block location.
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

int convert_page_to_lbn(pagetoLBNT *extentArray,
			int        extentArraySize,
			int        page,
			LBNT        *lbn,
			int        *vol)
{
    char   *funcName = "convert_page_to_lbn";
    int    min;
    int    max;
    int    current;
    int    found;
    int    pageDifference;

    *lbn = (LBNT) -1;
    *vol = -1;

    /*
     * Initialize our search bounds.
     */
    min = 0;
    max = extentArraySize;
    current = (min + max) / 2;

    if ((page >= extentArray[max].page) ||
	(page < 0)) {
        /*
	 * Page is out of bounds
	 */
         writemsg(SV_DEBUG | SV_LOG_INFO, 
                  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_866,
			  "Can't convert page %d to an LBN, valid range is 0 to %d.\n"),
		  page, extentArray[max].page);
         return FAILURE;
    }

    /*
     * Step 1 : Locate extent which contains page.
     */
    found = FALSE;
    while (FALSE == found) {
        if (min == max) {
	    /*
	     * Check if min = max : Found Page
	     */
	     current = min;
	     found = TRUE;
	} else if ((page >= extentArray[current].page) &&
		   (page < extentArray[current+1].page)) {
	    /*
	     * Check if page falls between current and current+1 : Found Page
	     */
	     found = TRUE;
	} else if (page > extentArray[current].page) {
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
    if ( (extentArray[current].lbn != (LBNT) -1 ) && (extentArray[current].lbn != (LBNT) -2 ) ) {
        pageDifference = page - extentArray[current].page;
	*lbn = (extentArray[current].lbn + pageDifference * BLOCKS_PER_PAGE);
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
 * Function Name: predict_memory_usage (3.nn)
 *
 * Description:
 *  This function attempts to malloc all the memory that will be
 *  needed by fixfdmn, called early on so that fixfdmn can exit early
 *  if not enough memory/swap/kernel parameters are set.
 *
 *  At this point in time, the larger memory structures which have not yet
 *  been malloc'ed include:
 *  
 *  	domain->filesets
 *  	domain->mcellList
 *  	fileset->fragLBN
 *  	fileset->tagLBN
 *  	fileset->tagList
 *  	fileset->fragSlots
 *	fileset->fragGroupUse
 *  
 *  filesets needs 128 bytes per fileset in the domain.
 *  mcellList needs 52.5 bytes per mcell.  32 bytes for mcellNode structure,
 *  	plus 20.5 bytes for the skiplist node structure.  20.5 is the
 *  	average for all levels of the dynamically-sized skiplist.
 *  fragLBN needs 12 bytes per extent of each frag file.
 *  tagLBN needs 12 bytes per extent of each tag file.
 *  tagList needs 116.5 bytes per tag, 96 for the tagNode, plus 20.5 for the 
 *  	skiplist node.
 *  fragSlots needs 3 bits per 1kb size of each frag file.
 *  fragGroupUse needs 1 byte per 128kb size of each frag file.
 *  
 *  To give an idea of the relative sizes of these structures, here are a
 *  few examples:
 *  
 *  For a V4ODS 100gb domain, 10% BMT, 50% frag file, 16 pgs per frag file
 *  extent, this estimate is:
 *  
 *  10gb / 8kb = 1.25 * 2^20 pages * 28 = 36,700,160 mcells (18,350,080 tags)
 *  numFragExtents = 50gb / 8kb / 16 = 409,600
 *  
 *  mcellNodeT = (32 + nodeT) * nummcells   = 1,926,758,400 bytes
 *  tagNodeT = (96 + nodeT) * numtags       = 2,137,784,320 bytes
 *  fragSlots = 50gb / 1kb * 3/8            =    19,660,800 bytes
 *  fragGroupUse = 50gb / 128kb             =       409,600 bytes
 *  fragLBN = 12 bytes * numFragExtents     =     4,915,200 bytes
 *  filesetT = 128 bytes * 1022 fsets       =       130,816 bytes
 *  
 *  
 *  Worst case scenarios for least impact of mcellList + tagList:
 *  For a V4ODS 108gb domain, 100gb frag file, all 7kb frags, 16 pgs per
 *  frag file extent, this estimate is:
 *  
 *  100gb / 7kb = 14,979,657 files (tags) = 14,979,657 mcells
 *  numFragExtents = 100gb / 8kb / 128 = 819,200
 *  
 *  mcellNodeT = (32 + nodeT) * nummcells   =   786,431,993 bytes
 *  tagNodeT = (96 + nodeT) * numtags       = 1,745,130,041 bytes
 *  fragSlots = 100gb / 1kb * 3/8           =    39,321,600 bytes
 *  fragGroupUse = 100gb / 128kb            =       819,200 bytes
 *  fragLBN = 12 bytes * numFragExtents     =     9,830,400 bytes
 *  filesetT = 128 bytes * 1022 fsets       =       130,816 bytes
 *  
 *  For a V4ODS 100gb domain, 100gb single file with only 1 extent, this
 *  estimate is:
 *  
 *  Single, BMT page, 28 mcells, therefore 14 possible tags
 *  mcellNodeT = (32 + nodeT) * nummcells   =         1,470 bytes
 *  tagNodeT = (96 + nodeT) * numtags       =         1,631 bytes
 *  fragSlots = 0gb / 1kb * 3/8             =             0 bytes
 *  fragGroupUse = 0gb / 128kb              =             0 bytes
 *  fragLBN = 12 bytes * numFragExtents     =             0 bytes
 *  filesetT = 128 bytes * 1022 fsets       =       130,816 bytes
 *  
 *  Because large frag files require even larger numbers of tags and
 *  therefore mcells, the frag memory usage will only be a very small
 *  percentage of total fixfdmn memory usage.  If filesetT becomes the
 *  largest memory factor, at 128kb, and this is a problem on the system
 *  fixfdmn is being run on, the system has far more problems than
 *  fixfdmn can handle.
 *  
 *  Based on the above numbers, it looks like the only things we need
 *  to determine up front are the number of mcells and the number of tags
 *  (files).
 *  
 *  As of load_subsystem_extents, a maximum of the number of mcells could be
 *  determined by the number of pages in the BMT.
 *  
 *  The number of tags can be estimated by dividing the number of mcells by
 *  2 for a V4ODS domain, and dividing by 3 for a V3ODS domain.  In theory,
 *  there could be one mcell per V4ODS domain, and two mcells per V3ODS
 *  domain, if almost every file was less than 7kb.  Is this a frequent
 *  enough occurrence that we need to worry about it?
 *  
 *  This routine should be able to prevent 99% of memory problems a user
 *  will encounter.  However, it's entirely possible another process could
 *  chew up memory while fixfdmn is running after it has made its
 *  prediction, thus allowing malloc to fail after fixfdmn has been running
 *  for a long time.
 *  
 *  One possible solution is to have fixfdmn checkpoint where it is when the 
 *  malloc fails, then have the user re-run fixfdmn from that point.  This
 *  is a good idea for a future version, but is not very feasible for the
 *  first release.
 *  
 *  Instead, for the first release, fixfdmn will call its own wrapper
 *  function instead of malloc, which will check for a failure.  If malloc
 *  fails, this wrapper will prompt the user to add swap space and/or kill
 *  off large memory usage processes other than fixfdmn, and/or some other
 *  memory saving procedure, then make another attempt to malloc the needed
 *  memory.  This can repeat until the user decides to tell fixfdmn not to
 *  retry.
 *  
 *
 * Input parameters:
 *  domain: The domain to predict memory usage of.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

int predict_memory_usage(domainT *domain)
{
    char	*funcName = "predict_memory_usage";
    long	predictedMemUsage;
    long	numMcells;
    long	numTags;
    int		volNum;
    int		status;
    char	answer[10];

    predictedMemUsage = 0;
    numMcells = 0;

    /*
     * Calculate maximum number of mcells in this domain.
     */
    for (volNum = 0 ; volNum <= domain->highVolNum ; volNum++) {
	if (-1 == domain->volumes[volNum].volFD) {
	    continue;
	}
	numMcells += domain->volumes[volNum].volRbmt->
	    bmtLBN[domain->volumes[volNum].volRbmt->bmtLBNSize].page *
	    BSPG_CELLS;
    }

    /*
     * Calculate best guess of number of tags in this domain.
     * See function description above for how the numbers 2 and 1 were
     * determined.
     */
    if (3 == domain->dmnVersion) {
	numTags = numMcells / 2;
    } else {
	numTags = numMcells;
    }

    /*
     * Get mcell usage.
     *
     * 32 is the smallest malloc bucket size that fits sizeof(mcellNodeT),
     * which is currently 28.
     *
     * 20.5 is the average number of bytes per node needed for all levels of
     * the skiplist structure.
     *
     * 8 is overhead used by malloc.
     */
    predictedMemUsage += numMcells * (32 + 20.5 + 8);
    /*
     * Get tag usage
     *
     * 96 is the smallest malloc bucket size that fits sizeof(mcellNodeT),
     * which is currently 76.
     *
     * 20.5 is the average number of bytes per node needed for all levels of
     * the skiplist structure.
     *
     * 8 is overhead used by malloc.
     */
    predictedMemUsage += numTags * (96 + 20.5 + 8);

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

	if (NULL == pMemory) {
	    writemsg(SV_ERR | SV_LOG_WARN,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_867,
			     "Failed to malloc predicted memory usage of %ld bytes.\n"),
		     predictedMemUsage);
	    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_868, 
			     "Increase max_per_proc_data_size, possibly vm_maxvas\n"));
	    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_869,
			     "and max_per_proc_address_space, check swap space,\n"));
	    writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_870,
			     "and other running processes using lots of memory.\n"));
	    writemsg(SV_ERR | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_871, 
			     "\nDo you want to continue running %s anyway? (y/[n])  "), Prog);

	    status = prompt_user_yes_no(answer, "n");
	    if (SUCCESS != status || 'n' == answer[0]) {
		return FAILURE;
	    }
	} else {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_872, 
			     "Successfully malloced predicted %ld bytes.\n"),
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

    while (NULL == ptr && FALSE == fail) {
	writemsg(SV_ERR | SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_873, 
			 "Can't allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_874, 
			 "Check for other processes (besides %s) which are\n"), 
		 Prog);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_875, 
			 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_876, 
			 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_877, 
			 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_878, 
			 "to increase max_per_proc_data_size, vm_maxvas,\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_879, 
			 "and max_per_proc_address_space.\n"));
	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_880, 
			 "\nDo you want %s to try this allocation again? ([y]/n)  "), 
		 Prog);

	status = prompt_user_yes_no(answer, "y");
	if (SUCCESS != status || 'n' == answer[0]) {
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

    while (NULL == ptr && FALSE == fail) {
	writemsg(SV_ERR | SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_873, 
			 "Can't allocate %ld bytes.\n"), 
		 numElts * eltSize);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_874, 
			 "Check for other processes (besides %s) which are\n"),
		 Prog);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_875, 
			 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_876, 
			 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_877,
			 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_878,
			 "to increase max_per_proc_data_size, vm_maxvas,\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_879,
			 "and max_per_proc_address_space.\n"));
	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_880, 
			 "\nDo you want %s to try this allocation again? ([y]/n)  "), 
		 Prog);

	status = prompt_user_yes_no(answer, "y");
	if (SUCCESS != status || 'n' == answer[0]) {
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

    while (NULL == ptr && FALSE == fail) {
	writemsg(SV_ERR | SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_873,
			 "Can't allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_874, 
			 "Check for other processes (besides %s) which are\n"),
		 Prog);
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_875,
			 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_876,
			 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_877, 
			 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_878, 
			 "to increase max_per_proc_data_size, vm_maxvas,\n"));
	writemsg(SV_ERR | SV_LOG_WARN | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_879,
			 "and max_per_proc_address_space.\n"));
	writemsg(SV_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_880, 
			 "\nDo you want %s to try this allocation again? ([y]/n)  "), 
		 Prog);

	status = prompt_user_yes_no(answer, "y");
	if (SUCCESS != status || 'n' == answer[0]) {
	    fail = TRUE;
	}
	else {
	    ptr = realloc(pointer, size);
	}
    } /* End while loop */

    return ptr;
} /* end ffd_realloc */


/*
 * Function Name: add_fixed_message (3.nn)
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
    if (NULL == msg) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_881,
			 "log message"));
	return NO_MEMORY;
    }

    msgString = (char *)ffd_malloc(msgLen + 1);
    if (NULL == msgString) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_881, 
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
    if (NULL == *pMessage) {
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
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_818, "RBMT"));
		break;
	    case(BMT):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_819, "BMT"));
		break;
	    case(SBM):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_820, "SBM"));
		break;
	    case(log):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_821, "LOG"));
		break;
	    case(frag):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_822, "FRAG"));
		break;
	    case(tagFile):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_823, "TAG FILE"));
		break;
	    case(dir):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_824, "DIR"));
		break;
	    case(tag):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_825, "TAG"));
		break;
	    case(misc):
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_826, "MISC"));
		break;
	    default:
		strcpy(diskArea,
		       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_732, "UNKNOWN"));
		break;
	} /* end of switch */
	
	writelog(SV_LOG_FIXED, "%s | %s:%d | %s", diskArea, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_882, "volX"), 
		 12345, msg->message);
    }
#endif /*MSGORDER*/

    return SUCCESS;
} /* end add_fixed_message */


/*
 * Function Name: collect_rbmt_extents (3.n)
 *
 * Description: Collect the extents from the RBMT.
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
int collect_rbmt_extents(domainT *domain, int volNum, int collectSbm)
{
    char    *funcName = "collect_rbmt_extents";
    int	    status;
    LBNT    currPageLBN;
    LBNT    nextPageLBN;
    int     tag;
    int     setTag;
    long    pageNum;
    mcellT  mcell;
    volumeT *volume;
    pageT   *pPage;
    bsMPgT  *pBMTPage;
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    bsXtraXtntRT *pXtra;
    pagetoLBNT   extentArray[2];
    
    /*
     * Init variables
     */
    volume = &(domain->volumes[volNum]);

    if (3 == domain->dmnVersion) {
	/*
	 * This domain does not have an RBMT - fill in BMT0 info.
	 */
	volume->volRbmt->rbmtLBNSize = 1;
	volume->volRbmt->rbmtLBN =
	    (pagetoLBNT *)ffd_calloc(volume->volRbmt->rbmtLBNSize+1,
				     sizeof(pagetoLBNT));
	if (NULL == volume->volRbmt->rbmtLBN) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_781,
			     "BMT0 extent"));
	    return NO_MEMORY;
	}
	volume->volRbmt->rbmtLBN[0].volume = volNum;
	volume->volRbmt->rbmtLBN[0].page = 0;
	volume->volRbmt->rbmtLBN[0].lbn = RBMT_PAGE0;
	volume->volRbmt->rbmtLBN[1].volume = volNum;
	volume->volRbmt->rbmtLBN[1].page = 1;
	volume->volRbmt->rbmtLBN[1].lbn = (LBNT) -1;
    } else {
	/* 
	 * This domain has an RBMT.  Gotta follow the links....
	 */
	currPageLBN = RBMT_PAGE0;
	nextPageLBN = (LBNT) -1;
	volume->volRbmt->rbmtLBNSize = 1;   /* count the BSR_XTNTS mcell */

        /*
         * Read the primary mcell BSR_XTNTS record to see if there is a chain.
         */
        status = read_page_from_cache(volNum, currPageLBN, &pPage);
        if (SUCCESS != status) {
            return status;
        }

        pBMTPage = (bsMPgT *)pPage->pageBuf;
        pMcell   = &(pBMTPage->bsMCA[0]);   /* prime mcell of rbmt */
        pRecord  = (bsMRT *)pMcell->bsMR0;
        while ((0 != pRecord->type) &&
               (0 != pRecord->bCnt) &&
               (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))))
        {
	    if (BSR_XTNTS == pRecord->type) {   /* find the BSR_XTNTS record */
                bsXtntRT *pXtnt;
	        pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
                if ( pXtnt->chainVdIndex != 0 ) {   /* there is a chain cell */
                    nextPageLBN = currPageLBN;
                }
		break;
	    }

	    pRecord = (bsMRT *)(((char *)pRecord) +
				roundup(pRecord->bCnt, sizeof(int)));
	} /* end record loop */

        status = release_page_to_cache(volNum, currPageLBN,
					   pPage, FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

        currPageLBN = nextPageLBN;   /* if chain cell exists, follow chain */
        nextPageLBN = (LBNT) -1;

	/*
	 * Start counting pages for how large an array to malloc.
	 */
	while ( (LBNT) -1  != currPageLBN ) {
	    status = read_page_from_cache(volNum, currPageLBN, &pPage);
	    if (SUCCESS != status) {
		return status;
	    }
	    pBMTPage = (bsMPgT *)pPage->pageBuf;
	    pMcell   = &(pBMTPage->bsMCA[RBMT_RSVD_CELL]);
	    pRecord  = (bsMRT *)pMcell->bsMR0;

	    if ( BSR_XTRA_XTNTS == pRecord->type ) {
                pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		nextPageLBN = pXtra->bsXA[0].vdBlk;
		volume->volRbmt->rbmtLBNSize++;   /* count next page mcell */
	    } else {
		nextPageLBN = (LBNT) -1;
	    }

	    status = release_page_to_cache(volNum, currPageLBN,
					   pPage, FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }

	    currPageLBN = nextPageLBN;
	    nextPageLBN = (LBNT) -1;
	}

	/*
	 * Finished counting pages.  Time to malloc the array.
	 */
	volume->volRbmt->rbmtLBN =
	    (pagetoLBNT *)ffd_calloc(volume->volRbmt->rbmtLBNSize+1,
				     sizeof(pagetoLBNT));
	if (NULL == volume->volRbmt->rbmtLBN) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_782,
			     "RBMT extents"));
	    return NO_MEMORY;
	}

	/*
	 * Now that the array is malloc'ed, fill it in.
	 */
	currPageLBN = RBMT_PAGE0;
	nextPageLBN = (LBNT) -1;
	pageNum = 0;

        volume->volRbmt->rbmtLBN[0].volume = volNum;
        volume->volRbmt->rbmtLBN[0].page = 0;
        volume->volRbmt->rbmtLBN[0].lbn = currPageLBN;

        status = read_page_from_cache(volNum, currPageLBN, &pPage);
        if (SUCCESS != status) {
            return status;
        }

        pBMTPage = (bsMPgT *)pPage->pageBuf;
        pMcell   = &(pBMTPage->bsMCA[0]);   /* prime mcell of rbmt */
        pRecord  = (bsMRT *)pMcell->bsMR0;
        while ((0 != pRecord->type) &&
               (0 != pRecord->bCnt) &&
               (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))))
        {
            if (BSR_XTNTS == pRecord->type) {   /* find the BSR_XTNTS record */
                bsXtntRT *pXtnt;
                pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
                if ( pXtnt->chainVdIndex != 0 ) {   /* there is a chain cell */
                    nextPageLBN = currPageLBN;
                }
                break;
            }

            pRecord = (bsMRT *)(((char *)pRecord) +
                                roundup(pRecord->bCnt, sizeof(int)));
        } /* end record loop */

        if ( TRUE == collectSbm ) {
            tag        = pMcell->tag.num;
            setTag     = pMcell->bfSetTag.num;
            mcell.vol  = volNum;
            mcell.page = 0;
            mcell.cell = 0;

            /*
             * Fake an extent Array to handle the extent.
             */
            extentArray[0].volume = volNum;
            extentArray[0].lbn    = currPageLBN;
            extentArray[0].page   = 0;
            extentArray[1].volume = volNum;
            extentArray[1].lbn    = (LBNT) -1;
            extentArray[1].page   = 1;

            status = collect_sbm_info( domain, extentArray, 2, 
                                       tag, setTag, &mcell, 0 );
            if (SUCCESS != status) {
                return status;
            }
        }

        status = release_page_to_cache( volNum, currPageLBN,
                                         pPage, FALSE, FALSE );
        if ( SUCCESS != status ) {
            return status;
        }

        pageNum++;
        currPageLBN = nextPageLBN;   /* if chain cell exists, follow chain */
        nextPageLBN = (LBNT) -1;
 
	while ( (LBNT) -1 != currPageLBN) {
	    status = read_page_from_cache(volNum, currPageLBN, &pPage);
	    if (SUCCESS != status) {
		return status;
	    }
	    
	    pBMTPage = (bsMPgT *)pPage->pageBuf;
	    pMcell   = &(pBMTPage->bsMCA[RBMT_RSVD_CELL]);
	    pRecord  = (bsMRT *)pMcell->bsMR0;

            if ( BSR_XTRA_XTNTS != pRecord->type ) {
                nextPageLBN = (LBNT) -1;
                goto done;
            }
	    pXtra    = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));

	    volume->volRbmt->rbmtLBN[pageNum].volume = volNum;
	    volume->volRbmt->rbmtLBN[pageNum].page = pXtra->bsXA[0].bsPage;
	    volume->volRbmt->rbmtLBN[pageNum].lbn = pXtra->bsXA[0].vdBlk;

	    if (TRUE == collectSbm) {
		tag        = pMcell->tag.num;
		setTag     = pMcell->bfSetTag.num;
		mcell.vol  = volNum;
		mcell.page = pageNum;
		mcell.cell = RBMT_RSVD_CELL;

		/*
		 * Fake an extent Array to handle the extent.
		 */
		extentArray[0].volume = volNum;
		extentArray[0].lbn    = pXtra->bsXA[0].vdBlk;
		extentArray[0].page   = pXtra->bsXA[0].bsPage;
		extentArray[1].volume = volNum;
		extentArray[1].lbn    = (LBNT) -1;
		extentArray[1].page   = pXtra->bsXA[0].bsPage + 1;

		status = collect_sbm_info(domain, extentArray, 2, 
					  tag, setTag, &mcell, 0);
		if (SUCCESS != status) {
		    return status;
		}
	    }

            nextPageLBN = pXtra->bsXA[0].vdBlk;
	    pageNum++;

done:
	    status = release_page_to_cache(volNum, currPageLBN,
					   pPage, FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }

	    currPageLBN = nextPageLBN;
	    nextPageLBN = (LBNT) -1;
	} /* End filling in array loop */
	volume->volRbmt->rbmtLBN[pageNum].volume = volNum;
	volume->volRbmt->rbmtLBN[pageNum].page = pageNum;
	volume->volRbmt->rbmtLBN[pageNum].lbn = (LBNT) -1;
    } /* End if dmnVersion not 3 */
    
    return SUCCESS;
} /* collect_rbmt_extents */


/*
 * Function Name: log_lsn_gt (3.n)
 *
 * Description: Compare to two lsn.  The problem with lsn is that
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

int log_lsn_gt(lsnT lsn1, lsnT lsn2)
{
    int maxLSNs;

    /*
     * This version uses the 'magic lsn range' to determine whether 
     * signed or unsigned lsn compares are appropriate.
     */
    maxLSNs = 1<<29;

    if ((lsn1.num < maxLSNs) || (lsn1.num > -maxLSNs)) {
        /* Must use signed compares */
        return( (int) lsn1.num > (int) lsn2.num );
    } else {
        return( lsn1.num > lsn2.num );
    }
}

/* end fixfdmn_utilities.c */

