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
 *      Advance File System
 *
 * Abstract:
 *
 *      On-disk structure salvager.
 *
 * Date:
 *
 *      Wed Feb 05 15:00:00 1997
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: utility.c,v $ $Revision: 1.1.40.2 $ (DEC) $Date: 2006/04/06 03:21:39 $"

#include <overlap.h>
#include "salvage.h"

/*
 * Global Variables
 */
extern nl_catd _m_catd;

/*
 * Local static prototypes
 */
static int add_delimiters(int  yearAndCenturyFlag, 
			  char *tmp, 
			  char **newBuf);


/*
 * Design Specification : 3.2.1 get_domain_volumes
 *
 * Description:
 *   We have a domain name given in the command line. Look in /etc/fdmns
 *   for the domain and get all the volumes specified there. Store the
 *   special device names in the volume structure.  
 *
 * Input parameters: 
 *    domain: The name of the domain.
 *
 * Output parameters:
 *    numVolumes: The number of volumes. 
 *    volumes: An Array of volumes. 
 *
 * Returns:
 * Status value - SUCCESS, FAILURE or NO_MEMORY
 */
int get_domain_volumes(char    *domain, 
		       int     *numVolumes, 
		       volMapT volumes[])
{
    char          *funcName = "get_domain_volumes";
    DIR           *dmnDir;
    char          dmnName[ MAXPATHLEN ] = "/etc/fdmns/";
    struct dirent *dp;
    int           counter = 0;

    *numVolumes = 0;

    /*
     * Validate domain name
     */
    if (NULL == domain) 
    {
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_228, 
				   "Domain name is NULL\n"));
	return FAILURE;
    }

    if (strlen(domain) + strlen(dmnName) > MAXPATHLEN) 
    {
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_229, 
				   "Domain name '%s' is too long\n"), domain);
	return FAILURE;
    }

    strcat (dmnName, domain);

    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_230,
			       "Opening directory '%s'\n"), dmnName);
    
    /*
     * Open domain directory to get access to all volumes 
     */
    dmnDir = opendir(dmnName);
    if (dmnDir == NULL) 
    {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_231, 
				 "Invalid domain name '%s'\n"), domain);
	perror(Prog);
	return FAILURE;
    }


    /*
     * Loop through all the file entries in /etc/fdmns/<domain>/
     */
    while (NULL != (dp = readdir(dmnDir)))
    {
	struct stat statBuf;
	char        dirName[MAXPATHLEN];
	char        volName[MAXPATHLEN];
	int         status;

	strcpy(dirName, dmnName);
	strcat(dirName, "/");
    
	strcat(dirName, dp->d_name);

	if (stat(dirName, &statBuf) == -1) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_232, 
		     "stat() failed for volume '%s'\n"), dirName);
	    perror(Prog);
	    continue;
	}

	/*
	 * Skip if not block device 
	 */
	if (!S_ISBLK(statBuf.st_mode)) 
	{
	    continue;
	}
	
	/*
	 * Read the symbolic link to find the real device name.
	 */
	status = readlink(dirName, volName, MAXPATHLEN -1);
	if (status == -1) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_233, 
				     "readlink() failed on '%s'\n"), dirName);
	    perror(Prog);
	    closedir(dmnDir);
	    return FAILURE;
	} 
	else if (status == MAXPATHLEN) 
	{
	    volName[MAXPATHLEN -1] = '\0';
	}

	volumes[counter].volName = (char *) salvage_malloc(strlen(volName) +1);

	if (NULL == volumes[counter].volName) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_234, 
				     "malloc() failed\n"));
	    closedir(dmnDir);
	    return NO_MEMORY;
	}

	strcpy (volumes[counter].volName, volName);
	counter++;

	if (counter >= MAX_VOLUMES) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_235, 
		     "Greater than %d volumes found\n"), MAX_VOLUMES);
	    closedir(dmnDir);
	    return FAILURE;
	}
    } /* end for loop */

    *numVolumes = counter;
    
    closedir(dmnDir);
    return SUCCESS;
} /* end domain_get_volume */


/*
 * Design Specification : 3.2.2 open_domain_volumes
 *
 * Description:
 *  This routine opens each volume and saves the file descriptor for
 *  future use. It also calls check_advfs_magic_number to check if each
 *  volume is an AdvFS volume. 
 *
 * Input parameters:
 *  domain: The domain with the array of volumes to open.
 *
 * Side Effects:
 *  domain->volumes: The file descriptor field in this array will be changed. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, NO_MEMORY
 */
int open_domain_volumes(domainInfoT *domain)
{
    char *funcName = "open_domain_volumes";
    int  status;
    int  return_status = FAILURE;
    int  counter;
    int  isAdvfs;
    volMapT   *volumes;
    char lsmVol[MAXPATHLEN];
    char *temp;
	
    volumes = domain->volumes;

    /* 
     * For each valid volume in the volume map 
     */
    for ( counter = 0; counter < MAX_VOLUMES; counter ++) 
    {
	char        rawName[MAXPATHLEN];

        /*
	 * If we don't have a device name set fd to -1.
	 */
        if (NULL == volumes[counter].volName) 
	{
	    volumes[counter].volFd = -1;
	    volumes[counter].volBlockFd = -1;
	    volumes[counter].volRawFd = -1;
	    continue;
	}

	/* 
	 * Found a valid block device, so open it. 
	 */
	volumes[counter].volBlockFd = open(volumes[counter].volName, 
					   O_RDONLY, O_NONBLOCK | O_NDELAY);
	if ( volumes[counter].volBlockFd == -1 ) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_236, 
		     "open() failed for Device '%s'\n"),
		     volumes[counter].volName);
	    perror(Prog);
	    continue;
	}
	
        strcpy(lsmVol, volumes[counter].volName);
        temp = strtok( lsmVol, "/" );
        temp = strtok( NULL, "/" );
        if ( strcmp(temp,"vol") == 0 ) 
                volumes[counter].lsm = 1;
        else
                volumes[counter].lsm = 0;

	/* 
	 * Now open the raw device.
	 */
#ifdef OLD_ODS
	fsl_rawname(volumes[counter].volName, rawName);
#else
	fsl_to_raw_name(volumes[counter].volName, rawName, MAXPATHLEN);
#endif

	volumes[counter].volRawFd = open(rawName,
					 O_RDONLY, O_NONBLOCK | O_NDELAY);
	if ( volumes[counter].volRawFd == -1 ) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_236, 
		     "open() failed for Device '%s'\n"),
		     rawName);
	    perror(Prog);
	    continue;
	}
	
	/*
	 * And set the standard FD to the raw device, since most
	 * routines are faster using that than the block device.
	 * 
	 * If the volume is a diskgroup, use the block device.
	 * The driver only accepts sector-aligned I/O requests.
	 */
	if (volumes[counter].lsm)
		volumes[counter].volFd = volumes[counter].volBlockFd;
	else
		volumes[counter].volFd = volumes[counter].volRawFd;

	/*
	 * Have succesfuly opened at least one volume.
	 */
	return_status = SUCCESS;

	/* 
	 * Volume is now open - Verify that this is an AdvFS volume. 
	 * This is done by checking the magic number.                
	 */
	status = check_advfs_magic_number(volumes[counter].volFd, &isAdvfs);
	if (SUCCESS != status)
	{
	    return status;
	}

	/*
	 * print a warning that the volume does not have a AdvFS magic number.
	 */
	if (TRUE != isAdvfs) 
	{
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_237, 
		     "'%s' does not have an AdvFS magic number\n"),
		     volumes[counter].volName);
	}
    } /* end for loop */
    return return_status;
} /* end open_domain_volumes */


/*
 * Design Specification 3.2.3 check_advfs_magic_number
 *
 * Description:
 *  This routine reads the AdvFS magic number to verify that the
 *  volume has been initialized for AdvFS usage. The call sets isAdvfs
 *  to TRUE or FALSE, depending on the result.
 *
 * Input parameters:
 *  volFD: A file descriptor for an open volume. 
 *
 * Output parameters:
 *  isAdvfs: Set to TRUE if magic number is valid. 
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY. 
 *
 */
int check_advfs_magic_number(int volFd,
			     int *isAdvfs)
{
    char    *funcName = "check_advfs_magic_number";
    uint32T *superBlk;
    bsMPgT  page;
    long    bytesRead;
    int     status;

    *isAdvfs = FALSE;
   
    /* 
     * Malloc Space to store super block 
     */
    superBlk = (uint32T *) salvage_malloc(PAGESIZE);
    if (NULL == superBlk) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_234, 
				 "malloc() failed\n"));
	return NO_MEMORY;
    }

    /* 
     * this read reads the msfs fake superblock, page 16, 
     * to check for the msfs magic number                
     */
    status = read_page_by_lbn(volFd, superBlk, MSFS_FAKE_SB_BLK, &bytesRead);
    if ((SUCCESS != status) || (PAGESIZE != bytesRead))
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_238, 
				   "Failure to read the superblock\n"));
	free(superBlk);
	return SUCCESS;
    }

    /*
     * Verify AdvFS magic number
     */
    if (superBlk[MSFS_MAGIC_OFFSET / sizeof( uint32T )] != MSFS_MAGIC) 
    {
	free(superBlk);
	return SUCCESS;
    }

    *isAdvfs = TRUE;
    free(superBlk);
    return SUCCESS;
} /* end check_advfs_magic_number */



/*
 * Design Specification 3.2.4 read_page_by_lbn
 *
 * Description
 *  This could be converted to a macro which calls read_bytes_by_lbn to 
 *  read a page from disk starting at a passed logical block number.
 *
 * Input parameters:
 *  fd:  The file descriptor to read from. 
 *  lbn: The logical block number to read. 
 *
 *  The caller must verify the FD is valid.
 *
 * Output parameters:
 *  ppage: A pointer to a page of memory which is read into. 
 *
 *  The caller is responsible to have malloced a one page buffer.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE or PARTIAL  
 */
int read_page_by_lbn(int    fd, 
		     void   *ppage, 
		     LBNT   lbn,
		     long   *bytesRead)
{
    char  *funcName = "read_page_by_lbn";
    int   status;

    status = read_bytes_by_lbn(fd, PAGESIZE, ppage, lbn, bytesRead);

    return status;
} /* end read_page_by_lbn */



/*
 * Design Specification 3.2.5 read_bytes_by_lbn
 *
 * Description
 *  This routine reads a number of bytes from disk starting at a passed
 *  logical block number.
 *
 * Input parameters:
 *  fd: The file descriptor to read from. 
 *  numBytes: The number of bytes to read. 
 *  lbn: The logical block number to read. 
 *
 *  The caller must verify the FD is valid.
 *
 * Output parameters:
 *  pbuffer: A pointer to a buffer which is read into. 
 *
 *  The caller is responsible to have malloced the correct size buffer.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE or PARTIAL.
 */
int read_bytes_by_lbn (int  fd, 
		       unsigned long numBytes, 
		       void *pbuffer, 
		       LBNT  lbn,
		       long *bytesRead)
{
    char    *funcName = "read_bytes_by_lbn";
    ssize_t bytes;

    if (-1 == lseek(fd, ((unsigned long)lbn) * DEV_BSIZE, SEEK_SET)) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_239, 
				 "lseek() failed\n"));
	perror(Prog);
	return FAILURE;
    }
    bytes = read(fd, pbuffer, numBytes);
    if (NULL != bytesRead)
    {
	*bytesRead = bytes;
    }

    if (-1 == bytes) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_240, 
				 "read() failed\n"));
	perror(Prog);
	return FAILURE;
    }
    else if (bytes != numBytes)
    {
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_241, 
		 "Did not read all bytes. %d of %d at LBN %u\n"), 
		 bytes, numBytes, lbn);
	return PARTIAL;
    }
    return SUCCESS;
}/* end read_bytes_by_lbn */


/*
 * Design Specification 3.2.6 writemsg
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

    /*
     * Initialize variables.
     */
    newFmtLen = 0;
    printDate = (severity & SV_DATE) > 0 ? 1 : 0;
    isStderr  = (severity & SV_ERR)  > 0 ? 1 : 0;
    isCont    = (severity & SV_CONT) > 0 ? 1 : 0;
    

    /*
     * If the severity code indicates this message should not be printed,
     * get out.
     */
    if (SV_ERR != severity  &&  (severity & 0x3) > Options.verboseOutput )
    {
        return;
    }

    /*
     * Begin calculations for the required length for the expanded format
     * string.
     */
    newFmtLen = strlen(fmt) + 1;

    /*
     * If the program name header is included, add it in.
     */
    if (0 == isCont)
    {
        newFmtLen += strlen(Prog)+2 + 1;
    }

    /*
     * If date was requested, get it, and increment the required length of
     * the new format length.
     */
    if (1 == printDate)
    {
        get_timestamp( dateStr, sizeof(dateStr) );
        newFmtLen += strlen(dateStr) + 2;
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
	tooLong = newFmtLen - (MAXERRMSG * 2) + 1;
	fmt[len - tooLong] = '\0';
	writemsg(SV_ERR, 
                 catgets(_m_catd, S_SALVAGE_1, SALVAGE_271,
                         "Message to be printed is too long, truncating it.\n"));
    }

    /*
     * If appropriate, put the Prog name at the beginning.
     */
    if (0 == isCont)
    {
        strcpy( newFmt, Prog );
        strcat( newFmt, ": " );
    }
    else
    {
        newFmt[0] = '\0';
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
    if (1 == printDate) {
        if ('\n' == newFmt[strlen(newFmt)-1])
        {
            newFmt[strlen(newFmt)-1] = ' ';
            strcat( newFmt, " " );
            strcat( newFmt, dateStr );
            strcat( newFmt, "\n" );
        }
        else
        {
            strcat( newFmt, "  " );
            strcat( newFmt, dateStr );
        }
    }


    /*
     * Print the message.
     */
    va_start(args, fmt);

    if ( 0 == isStderr && 0 == Options.stdoutArchive) {
        vfprintf(stdout, newFmt, args);
    } else {
        vfprintf(stderr, newFmt, args);
    }

    fflush(NULL);
    va_end(args);
}


/*
 * Design Specification 3.2.7 check_overwrite
 *
 * Description:
 *
 * Given the name of the file to restore when the file already exists,
 * check_overwrite checks the Overwrite flag and returns -1 if we shouldn't
 * overwrite the existing file and 0 if we should overwrite the existing 
 * file.
 *
 * Input parameters:
 *  filename: pointer to the name of the file to be overwritten
 *
 * Returns:
 *  Status value - -1 (don't overwrite) or 0 (overwrite)
 */

int
check_overwrite (char *fileName)
{
    char *funcName = "check_overwrite";
    char owAnswer[10];          /* contains overwrite answer */
    int notValid = 1;           /* assume overwrite answer isn't valid */

    if (Options.overwrite == OW_YES) 
    {
        return 0;
    } 
    else if (Options.overwrite == OW_NO) 
    {
        writemsg(SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_245, 
                 "'%s' not restored; file already exists\n"), fileName);
        return -1;
    } 
    else 
    {                        /* ask user to overwrite or not */
        
        while (notValid) 
        {         /* answer must be yes or no */
        
            writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_246, 
                     "'%s' already exists; overwrite? "), fileName);
            /*
             * We need an answer from the user.  If there is no way to
             * get one (Localtty is NULL), then default to overwrite.  
             */
            if (Localtty) 
            {
                if (fgets (owAnswer, 6, Localtty) == NULL) 
                {
                    return 0;
                }
            }
            else 
            {
                Options.overwrite = OW_YES;
                return 0;
            }
            
            if ((owAnswer[0] == 'y') || (owAnswer[0] == 'Y'))
            {
                return 0;
            } 
            else if ((owAnswer[0] == 'n') || (owAnswer[0] == 'N'))
            {
                return -1;

            } 
            else 
            {
	        /*
		 * Don't use writemsg here.
		 */
	        fprintf(stderr, "\n");
            }
        }
    }

}  /* end check_overwrite */


/*
 * Design Specification 3.2.8 prepare_signal_handlers
 *
 * Description:
 *
 * Set up the signal handler routine for SIGINT, SIGQUIT, and SIGTERM.
 *
 * Input parameters:
 *  none
 *
 * Returns:
 *  none
 */
void prepare_signal_handlers (void)
{
    int      status;
    sigset_t newset;
    sigset_t *newsetp = &newset;
    struct sigaction intAction;   /* action structure for signals */
    struct sigaction emptyAction; /* empty action structure used in call */
    
    /*
     * Set up action structures for signals and call sigaction.
     */
    
    intAction.sa_handler = signal_handler;
    intAction.sa_mask    = 0;
    intAction.sa_flags   = 0;
    
    sigaction (SIGINT, &intAction, &emptyAction);
    sigaction (SIGQUIT, &intAction, &emptyAction);
    sigaction (SIGTERM, &intAction, &emptyAction);

    /*
     * Set up signal mask for the signals we want to catch.
     */
    
    sigemptyset (newsetp);
    sigaddset (newsetp, SIGINT);
    sigaddset (newsetp, SIGQUIT);
    sigaddset (newsetp, SIGTERM);
    
    sigprocmask (SIG_UNBLOCK, newsetp, NULL);
    
} /* end prepare_signal_handlers */


/*
 * Design Specification 3.2.9 signal_handler
 *
 * Description:
 *
 * Ask the user if they want to abort when we get SIGINT or SIGQUIT. If so, 
 * exit with partial status, otherwise reset the handler and return. If
 * we get a SIGTERM, then exit.
 *
 * Input parameters:
 *  arg - dummy argument
 *
 * Returns:
 *  none
 */
void signal_handler (int sigNum)
{
    int              abort = 0; /* 1 = abort */
    struct sigaction newInt;
    struct sigaction oldInt;

    newInt.sa_handler = SIG_IGN;
    newInt.sa_mask    = 0;
    newInt.sa_flags   = 0;

    if (sigNum == SIGINT || sigNum == SIGQUIT) 
    {
        /*
         * Ignore any new asynch signals that come in while
         * we do an interactive query to abort the process.
         */

        sigaction (SIGINT, &newInt, &oldInt);
        sigaction (SIGQUIT, &newInt, &oldInt);

        abort = want_abort ();
        
        /*
         * In a future release we may want to check for abort here
         * and checkpoint what we've done so far so the user can
         * restart later.
         */
        
        sigaction (SIGQUIT, &oldInt, NULL);
        sigaction (SIGINT, &oldInt, NULL);
    } 
    else 
    {
        /* non-interactive or unblocked signal received; terminate */
        abort = 1;
    }

    /*
     * Check to see if we should abort, and do it. We should check
     * statistics when available to determine exit status, but for
     * now exit with a status of partial.
     */ 
    if (1 == abort)
    {     
        exit (EXIT_PARTIAL);
    }
    
    return;
} /* end signal_handler */


/*
 * Design Specification 3.2.10 want_abort
 *
 * Description:
 *
 * Prompts the user to see if they really want to abort salvage.
 *
 * Input parameters:
 *  filename: pointer to the name of the file to be overwritten
 *
 * Returns:
 *  Status value - -1 or 0
 */

/* Interactive query to abort; return TRUE if abort desired, else FALSE.
 * If stdin is being used for input, or the user cannot respond, then this 
 * routine will return TRUE. This routine may be called from threads other 
 * than the signal_thread to prompt user for abort.
 */
int want_abort ( )
{
    char     abortAnswer[10];          /* contains abort answer */
    int valid = 0;
    int abort = 0;

    if (!Localtty) 
    { 
        /* can't prompt user; no local tty */
        return (1);
    }

    while (0 == valid ) 
    {
          
        /* answer must be yes or no */
        fprintf(stderr, "\n");
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_247, 
				 "Do you want to abort? (yes or no) "));

        /*
         * We need an answer from the user.  If there is no way to
         * get one (Localtty is NULL), then we should have returned above.  
         */
        if (fgets (abortAnswer, 6, Localtty) == NULL) 
        {
            abortAnswer[0] = 'y';
        }

        if ((abortAnswer[0] == 'y') || (abortAnswer[0] == 'Y')) 
        {
            valid = 1;
            abort = 1;
        } 
        else if ((abortAnswer[0] == 'n') || (abortAnswer[0] == 'N')) 
        {
            valid = 1;
        } 
        else 
        {
            fprintf(stderr, "\n");
        } 
    } 
    return (abort);
}
/* end want_abort */


/*
 * Design Specification 3.2.11 convert_date
 *
 * Description
 *  This function converts an input date/time string into a time_t integer.
 *  NOTE:
 *    A large portion of this function was taken from the 'at' command.
 *    
 *    The time can be inputted in the following format(s):
 *
 *    [[cc]yy]MMddhhmm[.ss]
 *
 * Input parameters:
 *  dateStr: Input date character string.
 *
 * Returns:
 *  The number of seconds since 1970 or -1.
 */
int convert_date(char       *timeString,
		 const char *dateFormat[])
{
    char       *funcName = "convert_date";
    struct tm  dateTime;
    char       *pTime = timeString;
    char       *pLocation;
    char       *newArgp;
    char       *fmt;
    time_t     timbuf;
    int        seconds = -1;
    int        status;
    int        currentMonth;     /* Used to see if we need to reset year */
    int        currentYear;
    int	       currentDay;	

    for(fmt = (char *)*dateFormat++; 
	fmt; 
	fmt = (char *)*dateFormat++)
    {
        /*
	 * Set default values, such as YEAR and TIMEZONE.
	 */
        timbuf   = time(NULL); 
	dateTime = *localtime(&timbuf);  /* Resets to right now */
	dateTime.tm_sec = 0;             /* Reset seconds to 0  */
	currentMonth = dateTime.tm_mon;
	currentYear  = dateTime.tm_year;
        currentDay   = dateTime.tm_mday;

	/* 
	 * if the YEAR should contain the Century, then set the 
	 * appropriate flag on the argument to add the delimiters in
	 * the right place.
	 */
	if (strchr(fmt, 'Y'))
	{
	    status = add_delimiters(1, timeString, &newArgp);
	}
	else
	{
	    status = add_delimiters(0, timeString, &newArgp);
	}

	if (SUCCESS != status)
	{
	    return seconds;
	}

	pTime = newArgp;
	pLocation = strptime(pTime, fmt, &dateTime); /* Trial conversion */

	if (!pLocation) 
	{
	  /* 
	   * Did not convert correctly
	   */
            bzero(&dateTime,sizeof(dateTime));
	    continue;		 
	}

	if (*pLocation == ' ') 
	{
	    pLocation++;
	}

	if (*pLocation == '.')			
	{
	   /* 
	    * Expecting seconds 
	    */
	    pLocation = strptime(pLocation, ".%S", &dateTime);
	}

	if (!pLocation || *pLocation)
	{
	    /* 
	     * Format error or extra chars 
	     */
            bzero(&dateTime,sizeof(dateTime));
	    continue;
	}
	
	/*
	 * Special case - The time asked for is in the future.
	 * future.  Assume prior year.
	 */
	if ((currentYear < dateTime.tm_year) || \
          ((currentYear == dateTime.tm_year)&&(currentMonth < dateTime.tm_mon))\
          || ((currentYear == dateTime.tm_year)&& \
          (currentMonth == dateTime.tm_mon)&&(currentDay < dateTime.tm_mday)))
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_242, 
                     "Date given is in the future\n"));
	}

	/*
	 * The user has entered a year prior to Epoch.
	 */
	if (dateTime.tm_year < 70)
	{
	    /*
	     * Pre-Epoch - invalid date.
	     */
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_243, 
                     "Entered a date prior to Epoch\n"));
	    return -1;
	}

	break;
    }

    seconds = mktime( &dateTime );
    if ( seconds == -1 )
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_290, 
                 "Entered an invalid time\n"));
    }

    return seconds;
} /* end convert_date */


/*
 * Design Specification 3.2.12 get_timestamp
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

char * get_timestamp(char *dateStr, 
		     size_t maxLen)
{
    char *funcName = "get_timestamp";
    time_t t;
    struct tm *dateTime;
    size_t retChars = 0;

    t = time(0);
    dateTime = localtime(&t);           /* Not thread safe */
    retChars = strftime ( dateStr, maxLen, "%d-%b-%Y %T", dateTime );

    if ( retChars > 0 )
    {
        return dateStr;
    }
    else
    {
        return NULL;
    }
} /* end get_timestamp */



/*
 * Design Specification 3.2.13 add_delimiters
 *
 * Description
 *  This function converts an input date/time string into a format
 *  which strptime can understand.
 *  NOTE:
 *    A large portion of this function was taken from the 'at' command.
 *    
 *    The time can be inputted in the following format(s):
 *
 *    [[cc]yy]MMddhhmm[.ss]
 *
 * Input parameters:
 *    yearAndCenturyFlag : Are we converting for year and century.
 *    tmp                : The input time string.
 *
 * Output parameters:
 *    newBuf             : The modified time string.
 *
 * Returns:
 *  SUCCESS, FAILURE or NO_MEMORY
 */
static int add_delimiters(int  yearAndCenturyFlag, 
			  char *tmp, 
			  char **newBuf)
{
    char *funcName = "add_delimiters";
    char *delim;
    char *tempBuf;
    int  i, len, strLen;

    strLen = strlen(tmp);

    if ((tempBuf = (char*) salvage_malloc(strLen + 8)) == NULL)
    {
        return NO_MEMORY;
    }

    /* 
     *  tmpBuf is explicitly null terminated for it can contain junk
     */

    tempBuf[0]='\0';

    if (yearAndCenturyFlag) 
    {
        strncpy(tempBuf, tmp, 4);
        tempBuf[4]='\0';          
	strcat(tempBuf, " ");
	tmp += 4;
	strLen -= 4;
    }

    for(i = 0; i < strLen; i += len) 
    {
	if (*tmp == '.') 
	{
	    len = 3;
	    delim = "";
	}
	else 
	{
	    len = 2;
	    delim = " ";
	}

	strncat(tempBuf, tmp, len);
	strcat(tempBuf, delim);
	tmp += len;
    }
    *newBuf = tempBuf;

    return SUCCESS;
}
/* end add_delimiters */


/*
 * Design Specification 3.2.14 check_tag_array_szie
 *
 * Description:
 *
 * While enlarging the tag array we have found a tag which
 * is greater than the current 'softlimit' on number of tags.
 *
 * We give the user the chance to raise the softlimit, but with the 
 * possibility that the realloc will fail.  
 *
 * Input parameters:
 *  old :  The old maximum tags
 *  new :  The possible new number of tags
 *
 * Returns:
 *  Status value : TRUE (increase softlimit), 
 *                 FALSE (continue with current softlimit)
 */

int check_tag_array_size (int old,
			  int new)
{
    char *funcName = "check_tag_array_size";
    char answer[10];  /* contains users answer */

    /* 
     * ask user to increase tag array size.
     */
        
    while (TRUE) 
    {         
        /* 
	 * answer must be yes or no
	 */
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_248, 
		 "Current softlimit on tag array %d; increase size to %d? "), 
		 old,new);
	/*
	 * We need an answer from the user.  If there is no way to
	 * get one (Localtty is NULL), then default to NO.  
	 */
	if (Localtty) 
	{
	    if (fgets (answer, 6, Localtty) == NULL) 
	    {
		return FALSE;
	    }
	}
	else 
	{
	    return FALSE;
	}
        
	if ((answer[0] == 'y') || (answer[0] == 'Y'))
	{
	    return TRUE;
        } 
	else if ((answer[0] == 'n') || (answer[0] == 'N'))
	{
	    return FALSE;
	} 
	else 
	{
	    /*
	     * Don't use writemsg here.
	     */
	    fprintf(stderr, "\n");
	}
    }
}  /* end check_tag_array_size */

/*
 * Function Name: salvage_malloc
 *
 * Description:
 *  This function is a malloc wrapper that will prompt the user if the
 *  malloc call fails, and will reattempt the malloc assuming the user
 *  can either kill off other memory intensive processes or add swap
 *  space if one of those is the problem preventing the malloc.
 *
 *	NOTE:	This routine was borrowed from fixfdmn.  Changes
 *		made here should probably also be made in the
 *		fixfdmn code as well.
 *
 * Input parameters:
 *  size: The number of bytes to malloc.
 *
 * Returns:
 *  A pointer to the malloc'ed memory, or it exits.
 */

void *salvage_malloc(size_t size)
{
    char	*funcName = "salvage_malloc";
    void	*ptr;
    int		fail;
    int		status;
    char	answer[10];

    fail = FALSE;

    ptr = malloc(size);

    while (NULL == ptr && FALSE == fail) {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_272,
		 "Can't allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_273,
		 "Check for other processes (besides %s) which are\n"), Prog);
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_274,
		 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_275,
		 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_276,
		 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_277,
		 "to increase max_per_proc_data_size, vm_maxvas,\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_278,
		 "and max_per_proc_address_space.\n"));

	if (!Localtty) {
	    return ptr;
	}

	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_279,
		 "\nDo you want %s to try this allocation again? ([y]/n)  "));

	if (fgets (answer, 6, Localtty) == NULL)
	{
	    answer[0] = 'n';
	}

	if ('N' == answer[0] || 'n' == answer[0]) {
	    fail = TRUE;
	}
	else {
	    ptr = malloc(size);
	}
    } /* End while loop */

    return ptr;
} /* end salvage_malloc */

/*
 * Function Name: salvage_calloc
 *
 * Description:
 *  This function is a calloc wrapper that will prompt the user if the
 *  malloc call fails, and will reattempt the malloc assuming the user
 *  can either kill off other memory intensive processes or add swap
 *  space if one of those is the problem preventing the malloc.
 *
 * Input parameters:
 *  numElts: The number of elements to calloc.
 *  eltSize: The size of each element to calloc.
 *
 * Returns:
 *  A pointer to the calloc'ed memory, or it exits.
 */

void *salvage_calloc(size_t numElts, size_t eltSize)
{
    char	*funcName = "salvage_calloc";
    void	*ptr;
    int		fail;
    int		status;
    char	answer[10];

    fail = FALSE;

    ptr = calloc(numElts, eltSize);

    while (NULL == ptr && FALSE == fail) {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_272,
		 "Can't allocate %ld bytes.\n"), numElts * eltSize);
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_273,
		 "Check for other processes (besides %s) which are\n"), Prog);
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_274,
		 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_275,
		 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_276,
		 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_277,
		 "to increase max_per_proc_data_size, vm_maxvas,\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_278,
		 "and max_per_proc_address_space.\n"));

	if (!Localtty) {
	    return ptr;
	}

	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_279,
		 "\nDo you want %s to try this allocation again? ([y]/n)  "));

	if (fgets (answer, 6, Localtty) == NULL)
	{
	    answer[0] = 'n';
	}

	if ('N' == answer[0] || 'n' == answer[0]) {
	    fail = TRUE;
	}
	else {
	    ptr = calloc(numElts, eltSize);
	}
    } /* End while loop */

    return ptr;
} /* end salvage_calloc */

/*
 * Function Name: salvage_realloc
 *
 * Description:
 *  This function is a realloc wrapper that will prompt the user if the
 *  malloc call fails, and will reattempt the malloc assuming the user
 *  can either kill off other memory intensive processes or add swap
 *  space if one of those is the problem preventing the malloc.
 *
 * Input parameters:
 *  pointer: The originally malloc'ed pointer.
 *  size: The number of bytes to realloc.
 *
 * Returns:
 *  A pointer to the realloc'ed memory, or it exits.
 */

void *salvage_realloc(void *pointer, size_t size)
{
    char	*funcName = "salvage_realloc";
    void	*ptr;
    int		fail;
    int		status;
    char	answer[10];

    fail = FALSE;

    ptr = realloc(pointer, size);

    while (NULL == ptr && FALSE == fail) {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_272,
		 "Can't allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_273,
		 "Check for other processes (besides %s) which are\n"), Prog);
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_274,
		 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_275,
		 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_276,
		 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_277,
		 "to increase max_per_proc_data_size, vm_maxvas,\n"));
	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_278,
		 "and max_per_proc_address_space.\n"));

	if (!Localtty) {
	    return ptr;
	}

	writemsg(SV_ERR | SV_CONT, catgets(_m_catd, S_SALVAGE_1, SALVAGE_279,
		 "\nDo you want %s to try this allocation again? ([y]/n)  "));

	if (fgets (answer, 6, Localtty) == NULL)
	{
	    answer[0] = 'n';
	}

	if ('N' == answer[0] || 'n' == answer[0]) {
	    fail = TRUE;
	}
	else {
	    ptr = realloc(pointer, size);
	}
    } /* End while loop */

    return ptr;
} /* end salvage_realloc */
/* end utility.c */
