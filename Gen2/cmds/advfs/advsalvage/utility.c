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

#include "advsalvage.h"

/*
 * Global Variables
 */

extern char *__blocktochar(char *blockp, char *charp);

/*
 * Local static prototypes
 */
static int add_delimiters(int  yearAndCenturyFlag, 
			  char *tmp, 
			  char **newBuf);


/*
 * Function Name: get_domain_volumes
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
    char          dmnName[ MAXPATHLEN + 1 ] = "/dev/advfs/";
    struct dirent *dp;
    int           counter = 0;

    *numVolumes = 0;

    /*
     * Look for the list of "volumes" at the qtest path if qtest option is
     * specified
     */
    if (Options.qtest) {
        strcpy(dmnName, Options.qtestPath);
        strcat(dmnName, "/");
    }

    /*
     * Validate domain name
     */
    if (NULL == domain) 
    {
	writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_228, 
				   "fsname is NULL\n"));
	return FAILURE;
    }

    if (strlen(domain) + strlen(dmnName) > MAXPATHLEN) 
    {
	writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_229, 
				   "fsname '%s' is too long\n"), domain);
	return FAILURE;
    }

    strcat (dmnName, domain);
    strcat (dmnName, "/.stg");

    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_230,
			       "Opening storage directory '%s'\n"), dmnName);
    
    /*
     * Open domain directory to get access to all volumes 
     */
    dmnDir = opendir(dmnName);
    if (dmnDir == NULL) 
    {
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_231, 
				 "Invalid fsname '%s'\n"), domain);
	perror(Prog);
	return FAILURE;
    }


    /*
     * Loop through all the file entries in /dev/advfs/<storage_domain>/.stg
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
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_14, 
		     "stat() failed for volume '%s'\n"), dirName);
	    perror(Prog);
	    continue;
	}

        /*
         * We treat the list of "volumes" differently depending on 
	 * whether we are in test mode.
         */
        if (!Options.qtest) {
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
                writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_233, 
                                        "readlink() failed on '%s'\n"), dirName);
                perror(Prog);
                closedir(dmnDir);
                return FAILURE;
            } 
            else if (status == MAXPATHLEN) 
            {
                volName[MAXPATHLEN -1] = '\0';
            }
        } else {
            /*
             * Skip if not regular file.
             */
            if (!S_ISREG(statBuf.st_mode)) {
                continue;
            }
            strcpy(volName, dirName);
        }
	

	volumes[counter].volName = (char *) salvage_malloc(strlen(volName) +1);

	if (NULL == volumes[counter].volName) 
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_68, 
				     "malloc() failed\n"));
	    closedir(dmnDir);
	    return NO_MEMORY;
	}

	strcpy (volumes[counter].volName, volName);
	counter++;

	if (counter >= MAX_VOLUMES) 
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_235, 
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
 * Function Name: open_domain_volumes
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
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_236, 
		     "open() failed for Device '%s'\n"),
		     volumes[counter].volName);
	    perror(Prog);
	    continue;
	}
	
	/* 
	 * Now open the raw device.
	 */
        if (!Options.qtest) {
            if ((__blocktochar(volumes[counter].volName, rawName)) == NULL) {
                continue;
            }
        } else {
            strcpy(rawName, volumes[counter].volName);
        }

	volumes[counter].volRawFd = open(rawName,
					 O_RDONLY, O_NONBLOCK | O_NDELAY);
	if ( volumes[counter].volRawFd == -1 ) 
	{
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_236, 
		     "open() failed for Device '%s'\n"),
		     rawName);
	    perror(Prog);
	    continue;
	}
	
	/*
	 * And set the standard FD to the raw device, since most
	 * routines are faster using that than the block device.
	 */
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
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_237, 
		     "'%s' does not have an AdvFS magic number\n"),
		     volumes[counter].volName);
	}
    } /* end for loop */
    return return_status;
} /* end open_domain_volumes */


/*
 * Function Name: check_advfs_magic_number
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
    char     *funcName = "check_advfs_magic_number";
    advfs_fake_superblock_t *superBlk;
    bsMPgT   page;
    int      status;
    int64_t bytesRead;

    *isAdvfs = FALSE;
   
    /* 
     * Malloc Space to store super block 
     */
    superBlk = (advfs_fake_superblock_t *) salvage_malloc(ADVFS_METADATA_PGSZ);
    if (NULL == superBlk) 
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_68, 
				 "malloc() failed\n"));
	return NO_MEMORY;
    }

    /* 
     * this read reads the advfs fake superblock, page 16, 
     * to check for the advfs magic number                
     */
    status = read_page_by_lbn(volFd, superBlk, ADVFS_FAKE_SB_BLK, &bytesRead);
    if ((SUCCESS != status) || (ADVFS_METADATA_PGSZ != bytesRead))
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_238, 
				   "Failure to read the superblock\n"));
	free(superBlk);
	return SUCCESS;
    }

    /*
     * Verify AdvFS magic number
     */
    if ( !ADVFS_VALID_FS_MAGIC(superBlk) )
    {
	free(superBlk);
	return SUCCESS;
    }

    *isAdvfs = TRUE;
    free(superBlk);
    return SUCCESS;
} /* end check_advfs_magic_number */



/*
 * Function Name: read_page_by_lbn
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
int read_page_by_lbn(int     fd, 
		     void    *ppage, 
		     bf_vd_blk_t lbn,
		     int64_t *bytesRead)
{
    char  *funcName = "read_page_by_lbn";
    int   status;

    status = read_bytes_by_lbn(fd, ADVFS_METADATA_PGSZ, ppage, lbn, bytesRead);

    return status;
} /* end read_page_by_lbn */



/*
 * Function Name: read_bytes_by_lbn
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
 *  The caller must use numBytes that is divisible by DEV_BSIZE
 *
 * Output parameters:
 *  pbuffer: A pointer to a buffer which is read into. 
 *
 *  The caller is responsible to have malloced the correct size buffer.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE or PARTIAL.
 */
int read_bytes_by_lbn (int      fd, 
		       uint64_t numBytes, 
		       void     *pbuffer, 
		       bf_vd_blk_t lbn,
		       int64_t  *bytesRead)
{
    char    *funcName = "read_bytes_by_lbn";
    ssize_t bytes;

    if (numBytes % DEV_BSIZE !=0) {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_290, 
                 "read %d bytes does not end on block boundary\n"),numBytes);
        return EINVAL;
    }

    if (-1 == lseek(fd, ((uint64_t)lbn) * DEV_BSIZE, SEEK_SET)) 
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_239, 
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
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_240, 
				 "read() failed\n"));
	perror(Prog);
	return FAILURE;
    }
    else if (bytes != numBytes)
    {
	writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_241, 
		 "Did not read all bytes. %d of %d at LBN %d\n"), 
		 bytes, numBytes, lbn);
	return PARTIAL;
    }
      
    return SUCCESS;
}/* end read_bytes_by_lbn */


/*
 * Function Name: writemsg
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
    int  newFmtLen;
    char *fmtcopy;
    char dateStr[32];
    int  printDate;
    int  isStderr;
    int  isCont;

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
        fmtcopy = strdup(fmt); /* Preserve fmt from stomping on PA arch */
	writemsg(SV_ERR, 
                 catgets(mcat, S_SALVAGE_1, SALVAGE_271,
                         "Message to be printed is too long, truncating it.\n"));
        strcpy(fmt,fmtcopy);
        free(fmtcopy);
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
 * Function Name: check_overwrite
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
        writemsg(SV_VERBOSE, catgets(mcat, S_SALVAGE_1, SALVAGE_245, 
                 "'%s' not restored; file already exists\n"), fileName);
        return -1;
    } 
    else 
    {                        /* ask user to overwrite or not */
        
        while (notValid) 
        {         /* answer must be yes or no */
        
            writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_246, 
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
 * Function Name: prepare_signal_handlers
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
    int      status,i;
    sigset_t newset;
    sigset_t *newsetp = &newset;
    struct sigaction intAction;   /* action structure for signals */
    struct sigaction emptyAction; /* empty action structure used in call */
    
    /*
     * Set up action structures for signals and call sigaction.
     */
    
    intAction.sa_handler = signal_handler;
    for (i=0; i<8; i++)
	intAction.sa_mask.sigset[i] =0;
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
 * Function Name: signal_handler
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
    int i;
    struct sigaction newInt;
    struct sigaction oldInt;

    newInt.sa_handler = SIG_IGN;
    for (i=0; i<8; i++)
	newInt.sa_mask.sigset[i] =0;
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
 * Function Name: want_abort
 *
 * Description:
 *
 * Prompts the user to see if they really want to abort salvage.
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
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_247, 
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
 * Function Name: convert_date
 *
 * Description
 *  This function converts an input date/time string into a time_t integer.
 *  NOTE:
 *    A large portion of this function was taken from the 'at' command. At
 *    some point this could be made a shared library.
 *    
 *    The time can be inputted in the following format(s):
 *
 *    [[CC]YY]MMddhhmm[.SS]
 *
 * Input parameters:
 *  timestr: Input date strings. 
 *
 * Returns:
 *  Returns -1 if the time cannot be calculated because any of the
 *   components are out of range or if the calculated time is earlier 
 *   than the Epoch, (with appropriate allowances made for timezone.
 *  Returns the number of seconds since 1970 if the date string is valid.
 */
int convert_date(char       *timestr_param)
{
        long    nt;
        char    *time_end;
        char    timestr[16];
        int     cent=0;
        int     inlength,i;
        int64_t seconds = -1;
        struct tm t = {0, 0, 0, 0, 0, 100, 0, 0, 0};

        if ((timestr_param == NULL) || (strlen(timestr_param)>15)) {
            return(-1);
        }
        /* Copy the parameter so we can trash it as we like */
        strncpy (timestr, timestr_param, 16);

        /* Valid date string means all characters are 0..9 or '.' */
        for(i=0; i<strlen(timestr); i++) {
            if (((timestr[i] < '0') || (timestr[i] > '9')) &&
                (timestr[i] != '.')) {
                return (-1);
            }
        }

        /* set a pointer to the end of the string, then back up 2 */
        inlength = strlen(timestr);
        if (inlength < 2) return(-1);
        time_end = timestr + inlength;
        time_end -= 2;

        /* The last two chars are seconds if they follow a '.' */
        if(*(time_end - 1) == '.') {
                time_end--;
                *time_end       = 0;
                t.tm_sec      = atoi(time_end + 1);
                if (t.tm_sec < 0 || t.tm_sec > 61)
                        return(-1);
                time_end -= 2;
        }

        if (time_end < timestr)
                return(-1);

        /* Get the minutes. */
        t.tm_min      = atoi(time_end);
        if (t.tm_min < 0 || t.tm_min > 59)
            return(-1);
        *time_end = 0;
        time_end -=2;

        if (time_end < timestr) return(-1);

        t.tm_hour     = atoi(time_end);
        if (t.tm_hour < 0 || t.tm_hour > 23)
            return(-1);
        *time_end = 0;
        time_end -= 2;

        if (time_end < timestr) return(-1);

        t.tm_mday     += atoi(time_end);
        if (t.tm_mday < 1 || t.tm_mday > 31)
            return(-1);
        *time_end       = 0;
        time_end        -= 2;

        if (time_end < timestr) return(-1);

        t.tm_mon      = atoi(time_end);
        if (t.tm_mon < 1 || t.tm_mon > 12)
            return(-1);
        --(t.tm_mon); /* Month is rep as 0-11 (see /usr/include/sys/time.h) */

        /* If there are more chars, the next two back are the year */
        if (time_end > timestr) {
            *time_end   = 0;
            time_end -= 2;
            t.tm_year = atoi(time_end);
            /*  If there are more chars yet, they are the century */
            if (time_end > timestr) {
                *time_end = NULL;
                time_end -= 2;
                cent = atoi(time_end);
                if (cent < 19)
                    return(-1);
            }
        }
        /* if not back to the beginning, something is wrong */
        if (time_end != timestr)
            return(-1);

        /*
         * Used to test for !tm_year. yrs==0 is actaully
         * valid as in the year 2000. The default tm_year
         * is now 100.
         */
        if (t.tm_year == 100) {
            (void) time(&nt);
            t.tm_year = localtime(&nt)->tm_year;
        }

        /*
         * The following year range has been modified to
         * be consistant with the standard. Please refer
         * to date(1) manpage for more details              
         */
        if(!cent)       {
                if(t.tm_year >= 70)  /*  If no century given w/ -t, key off */
                        cent = 19;     /*  year; if earlier than '70, assume  */
                else                   /*  roll-around to 21st century        */
                        cent = 20;
        }

        t.tm_year     = t.tm_year + cent*100;
        t.tm_year     -= 1900;        /* Year since 1900 */

        /*
         * There is a upperbound on the year because, mktime(3C)
         * accepts only years withing 137 years since 1900. It 
         * returns a -1 for later years. 
         * The upperbound can be 137. ( 2037 AD )
         */
        if (t.tm_year > 137)
                return(-1);

        /* leap second(s) */

        if(t.tm_sec > 59)     {
                t.tm_min++;
                t.tm_sec      = 0;
        }

        /* hours of 24 allowed in old syntax, not in new */
        if(t.tm_hour == 24)   {
                t.tm_mday++;
                t.tm_hour     = 0;
        }
        
	/* The user has entered a year prior to Epoch. */
	if (t.tm_year < 70)
	{
	    /* Pre-Epoch - invalid date. */
	    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_243, 
                     "Entered a date prior to Epoch\n"));
	    return(-1);
	}

        /* Force mktime to figure out daylight savings */
        t.tm_isdst = -1;

        seconds = mktime( &t );
        if ( seconds == -1 )
        {
            writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_244, 
                    "mktime() failed\n"));
        }
	
        /* 
         * Notify if the time asked for is in the future (fudge of 1 hour
         * in order to account for daylight savings.
         */
	if (seconds > time(NULL))
	{
	    writemsg(SV_NORMAL, catgets(mcat, S_SALVAGE_1, SALVAGE_242, 
                     "Date given is in the future\n"));
	}

        return seconds;
}
/* end convert_date */


/*
 * Function Name: get_timestamp
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
 * Function Name: add_delimiters
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
 * Function Name: check_tag_array_size
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

int check_tag_array_size (int64_t old,
			  int64_t new)
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
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_248, 
		 "Current soft-limit on tag array %d; increase size to %d? "), 
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
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_272,
		 "Cannot allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_273,
		 "Check for other processes (besides %s) which are\n"), Prog);
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_274,
		 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_275,
		 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_276,
		 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_277,
		 "to increase system settings which limit memory usage.\n"));

	if (!Localtty) {
	    return ptr;
	}

	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_293,
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
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_272,
		 "Cannot allocate %ld bytes.\n"), numElts * eltSize);
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_273,
		 "Check for other processes (besides %s) which are\n"), Prog);
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_274,
		 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_275,
		 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_276,
		 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_277,
		 "to increase system settings which limit memory usage.\n"));

	if (!Localtty) {
	    return ptr;
	}

	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_293,
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
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_272,
		 "Cannot allocate %ld bytes.\n"), size);
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_273,
		 "Check for other processes (besides %s) which are\n"), Prog);
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_274,
		 "using large amounts of memory, and kill them if\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_275,
		 "possible, or increase swap space.\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_276,
		 "If neither of those options are available, you may need\n"));
	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_277,
		 "to increase system settings which limit memory usage.\n"));

	if (!Localtty) {
	    return ptr;
	}

	writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_293,
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
