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
#pragma ident "@(#)$RCSfile: salvage_init.c,v $ $Revision: 1.1.49.1 $ (DEC) $Date: 2006/04/06 03:21:33 $"

#include "salvage.h"

/*
 * Global Variables
 */
extern nl_catd _m_catd;


/*
 * Function Name: setup_volume 3.3.1
 *
 * Description
 *  Load the old volume structure with data from BMT0/RBMT, then sort this data
 *  based on the AdvFS volume id.
 *
 * Input parameters:
 *  domain: The domain to be setup. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY. 
 */
int setup_volume(domainInfoT *domain)
{
    char    *funcName = "setup_volume";
    int     status;
    volMapT *oldVolumes; 
    volMapT *newVolumes; 
    int     volNumber; 
    int     counter; 
    int     volCounter = 0;  /* Number of Valid volumes */
    bsMPgT  buffer;    

    /*
     * Keep a pointer to the passed in volumes structure.
     */
    oldVolumes = domain->volumes;
  
    /* 
     * Initialize new volume struct 
     */
    status = create_volumes(&newVolumes);
    if (SUCCESS != status)
    {
        return status;
    }

    domain->volumes = newVolumes;
   
    /* 
     * Loop through all the volumes in oldVolumes.
     *
     * Note that the loop starts at 0 and ends before MAX_VOLUMES in this
     * special case before the volumes have been put at their correct
     * volume numbers.
     */
    for (counter = 0; counter < MAX_VOLUMES; counter ++) 
    {
        /* 
	 * Check to see if the current volume has a valid file descriptor.
	 */
        if (-1 == oldVolumes[counter].volFd) 
	{
	    continue;
        }

	/*
	 * Call setup_rbmt and setup_bmt to read BMT0/RBMT and 
         * setup structures.
	 */
	status = setup_rbmt(domain, &oldVolumes[counter], &buffer);
	if (FAILURE == status)
	{
	    /* 
	     *   Could not read rbmt from this volume - PASS 2 needed.
	     *   
	     *   Can not work with this BMT continue onto next volume.
	     */
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_131, 
		     "Unable to read page 0 of RBMT for volume '%s'\n"),
		     oldVolumes[counter].volName);
	    
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_132, 
                     "Setting search by volume\n"));
	    D_SET_PASS2_NEEDED(domain->status);
	}
	else if (NO_MEMORY == status)
	{
	    return status;
	}

	status = setup_bmt(domain, &oldVolumes[counter], &buffer);
	if (FAILURE == status)
	{
	    if (NULL == oldVolumes[counter].extentArray) {
		writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_285,
		       "Volume '%s' is too corrupt to salvage data from it\n"),
			 oldVolumes[counter].volName);
		writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_286,
		       "normally.  %s with the '-S' (sequential mode) flag\n"),
			 Prog);
		writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_287,
		   "might be able to retrieve some data from this volume.\n"));

		/*
		 * Set volFd and volName to look like this volume doesn't
		 * exist.
		 */
		oldVolumes[counter].volFd = -1;
		oldVolumes[counter].volName = NULL;
	    }
	    else {
		/* 
		 *   Could not read bmt0 or RBMT from this volume - PASS 2
		 *	needed.
		 *   
		 *   Can not work with this BMT continue onto next volume.
		 */
		writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_133, 
			 "Unable to read page 0 of BMT for volume '%s'\n"),
			 oldVolumes[counter].volName);
	    
		writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_132, 
					   "Setting search by volume\n"));
		D_SET_PASS2_NEEDED(domain->status);
	    } 

	    continue;
	}
	else if (PARTIAL == status) 
	{
	    /* 
	     * Could not read all of BMT0or an RBMT page - PASS 2 needed 
	     * We have found some of what we are looking for.
	     */
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_134, 
                     "Setting search by volume - Partial BMT0\n"));
	    D_SET_PASS2_NEEDED(domain->status);
	}
	else if (NO_MEMORY == status)
	{
	    return status;
	}

	/*
	 * Check Mcell 4 for Domain & Volume attributes : return volNumber 
	 */
	if (SUCCESS != load_domain_attributes(&buffer, domain, &volNumber)) 
	{
	    /* 
	     * Load domain attributes failed, but we may have been able to
	     * figure out which volNumber it should be on.
	     */
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_135, 
                     "Unable to read domains attributes from volume '%s'\n"),
		     oldVolumes[counter].volName);
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_132, 
                     "Setting search by volume\n"));
	    D_SET_PASS2_NEEDED(domain->status);
	}

	if (volNumber < 1 || volNumber > MAX_VOLUMES)
	{
	    /*
	     * We were not able to figure out where this volume goes.
	     * So continue to next volume.
	     */
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_136, 
                     "Unable to find volume id for volume '%s'\n"),
		     oldVolumes[counter].volName);
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_132, 
                     "Setting search by volume\n"));
	    D_SET_PASS2_NEEDED(domain->status);
	    continue;
	}

	/*
	 * Check for duplicate volume IDs.
	 */
	if (NULL != domain->volumes[volNumber].volName) 
	{
	    /*
	     * We have two or more volumes which have the same volume ID.
	     * This is an error, close second volume.
	     */
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_137, 
                     "Two volumes, %s and %s, have the same volume ID %d.\n"),
		     domain->volumes[volNumber].volName,
		     oldVolumes[counter].volName, volNumber);
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_279,
		    "Closing volume %s and not salvaging anything from it.\n"),
		     oldVolumes[counter].volName);
	    close(oldVolumes[counter].volBlockFd);
	    close(oldVolumes[counter].volRawFd);
	    /* Don't close volFd because it's really either Block or Raw. */
	    oldVolumes[counter].volBlockFd = -1;
	    oldVolumes[counter].volRawFd = -1;
	    oldVolumes[counter].volFd = -1;
	    oldVolumes[counter].lsm = 0;	
	    continue;
	}

	/* 
	 * Copy all the info for this volume into the correct slot in the
	 * new volume array. 
	 */
	volCounter++;
	domain->volumes[volNumber].volBlockFd = oldVolumes[counter].volBlockFd;
	domain->volumes[volNumber].volRawFd   = oldVolumes[counter].volRawFd;
	domain->volumes[volNumber].volFd      = oldVolumes[counter].volFd;
	domain->volumes[volNumber].lsm	= oldVolumes[counter].lsm;
	domain->volumes[volNumber].volName    = oldVolumes[counter].volName;
	domain->volumes[volNumber].extentArray = oldVolumes[counter].extentArray;
	domain->volumes[volNumber].extentArraySize = oldVolumes[counter].extentArraySize;
	domain->volumes[volNumber].rbmtArray = oldVolumes[counter].rbmtArray;
	domain->volumes[volNumber].rbmtArraySize = oldVolumes[counter].rbmtArraySize;

	/*
	 * Set fd to indicate this volume has been copied to the new array,
	 * and also make sure badVolNum is false.
	 */
	domain->volumes[volNumber].badVolNum  = FALSE;
	oldVolumes[counter].volFd = -1;

	writemsg(SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_138, 
                 "Setting volume '%s' to volume id %d\n"),
		 domain->volumes[volNumber].volName, volNumber);
    } /* End loop of volumes. */

    /* 
     * Loop through all the volumes in oldVolumes a second time to make
     * sure all of them have been copied into the new volume array.
     *
     * Note that the loop starts at 0 and ends before MAX_VOLUMES in this
     * special case before the volumes have been put at their correct
     * volume numbers.
     */
    for (counter = 0; counter < MAX_VOLUMES; counter ++) 
    {
	if (-1 == oldVolumes[counter].volFd)
	{
	    continue;
	}

	/*
	 * Start placing volumes at the end of the array, so that the
	 * chances of following a link to this volume are very very slim.
	 */
	for (volNumber = MAX_VOLUMES ; volNumber >= 1 ; volNumber--)
	{
	    if (NULL == domain->volumes[volNumber].volName) {
		break;
	    }
	}

	assert(volNumber > 0);

	/*
	 * Found an unused slot in the new array - copy the data
	 * from the old volume array.
	 */
	volCounter++;
	domain->volumes[volNumber].volBlockFd = oldVolumes[counter].volBlockFd;
	domain->volumes[volNumber].volRawFd   = oldVolumes[counter].volRawFd;
	domain->volumes[volNumber].volFd      = oldVolumes[counter].volFd;
	domain->volumes[volNumber].lsm	      = oldVolumes[counter].lsm;
	domain->volumes[volNumber].volName    = oldVolumes[counter].volName;
	domain->volumes[volNumber].extentArray = oldVolumes[counter].extentArray;
	domain->volumes[volNumber].extentArraySize = oldVolumes[counter].extentArraySize;
	domain->volumes[volNumber].rbmtArray = oldVolumes[counter].rbmtArray;
	domain->volumes[volNumber].rbmtArraySize = oldVolumes[counter].rbmtArraySize;

	/*
	 * Set the bad volume number flag.
	 */
	domain->volumes[volNumber].badVolNum = TRUE;
    }

    /* 
     * Have moved all Volumes into the correct position in newVolumes 
     */
    free(oldVolumes);
    D_SET_VOL_SETUP_COMPLETED(domain->status);

    if (0 == volCounter)
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_139, 
                 "No valid volumes found\n"));
        return FAILURE;
    }

    return SUCCESS;
} /* end setup_volume */


/*
 * Function Name: p3_setup_volume 3.3.2
 *
 * Description
 *  Move the volumes into the correct position in the Array.  This is called
 *  when the main setup_volume can not be used (ie pass3).
 *
 * Input parameters:
 *  domain: The domain to be setup. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY.
 */
int p3_setup_volume(domainInfoT *domain)
{
    char    *funcName = "p3_setup_volume";
    int     status;
    int     mcell;
    int     volNumber;
    int     counter;
    int     found;
    int     logicalPageNumber;
    int     fd;
    long    bytesRead;
    volMapT *oldVolumes;
    volMapT *newVolumes;
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    bsMPgT  BMTbuffer;
    int     volCounter = 0;
    int     ttyCounter = 1;
    int     volIdArray[MAX_VOLUMES +1];
    int     x;

    /*
     * Keep a pointer to the passed in volumes structure.
     */
    oldVolumes = domain->volumes;
  
    /* 
     * Initialize new volume struct 
     */
    status = create_volumes(&newVolumes);
    if (SUCCESS != status)
    {
        return status;
    }

    domain->volumes = newVolumes;
   
    /* 
     * Loop through all the volumes in oldVolumes.  
     *
     * Note that the loop starts at 0 and ends before MAX_VOLUMES in this
     * special case before the volumes have been put at their correct
     * volume numbers.
     */
    for (counter = 0; counter < MAX_VOLUMES; counter ++) 
    {
        /* 
	 * Check to see if the current volume has a valid file descriptor.
	 */
        if (-1 == oldVolumes[counter].volFd) 
	{
	    continue;
        }

        /*
	 *  Initialize array for each volume pass.
	 */
        for (x = 0; x <= MAX_VOLUMES; x++)
	{
	    volIdArray[x] = 0;
	}

	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_140, 
                 "Attempting to locate volume id for '%s'\n"),
		 oldVolumes[counter].volName);

	/*
	 * Loop through all PAGES on disk until we find a BMT page
	 * which we can validate the volume number.
	 */
	fd                = oldVolumes[counter].volFd;
	found             = FALSE;
	logicalPageNumber = 6;     /* SKIP BMT Page 0 & 1, start on block 96 */
	volNumber         = 0;

	status = read_page_by_lbn(fd, &BMTbuffer, 
				  (LBNT)logicalPageNumber * BLOCKS_PER_PAGE,
				  &bytesRead);

	while ((found == FALSE) &&
	       (SUCCESS == status) &&
	       (bytesRead == PAGESIZE))
	{
	    int validPage;

	    validPage = validate_page(BMTbuffer);
	    
	    /*
	     * Check if page matches the domains version number.
	     */

	    if (BMTbuffer.megaVersion != domain->version) 
	    {
	        /*
		 * INVALID PAGE
		 */
	        validPage = FALSE;
	    }
	    
	    if (TRUE == validPage)
	    {
	        long pageId      = BMTbuffer.pageId;
		long currentTag;
		long nextTag;
		long chainTag;

		pMcell = (struct bsMC *)BMTbuffer.bsMCA;

		/*
		 * Loop through all mcells on page looking for a
		 * next pointer which falls on the same page.
		 */
		for (mcell = 0; mcell < BSPG_CELLS; mcell++)
		{
		    long nextPage;
		    long nextCell;

		    pRecord    = (bsMRT *)pMcell[mcell].bsMR0;
		    currentTag = pMcell[mcell].tag.num;
		    nextPage   = pMcell[mcell].nextMCId.page;
		    nextCell   = pMcell[mcell].nextMCId.cell;
		    
		    /*
		     * If nextVdIndex is not valid then skip this mcell
		     */
		    if ((pMcell[mcell].nextVdIndex < 1) ||
			(pMcell[mcell].nextVdIndex > MAX_VOLUMES))
		    {
			continue;
		    }

		    /*
		     * Collect count of vdIndex.
		     */
		    volIdArray[pMcell[mcell].nextVdIndex]++;
		    
		    /*
		     * Are these mcells on the SAME page.
		     */
		    if (pageId == nextPage)
		    {
			nextTag = pMcell[nextCell].tag.num;

			/*
			 * Do they both have the same tag number.
			 */
			if (currentTag == nextTag)
			{
			    /*
			     * We found a VALID VolID.
			     */
			    volNumber = pMcell[mcell].nextVdIndex;
			    found = TRUE;
			    break;
			}
		    }
		} /* end for mcell loop */

		if (FALSE == found)
		{
		    /*
		     * We have a valid page but no NEXT pointers
		     * which work.  Now we are goign to try the 
		     * same basic check, but on CHAIN pointers.
		     */
		    pMcell = (struct bsMC *)BMTbuffer.bsMCA;

		    for (mcell = 0; mcell < BSPG_CELLS; mcell++)
		    {
			currentTag  = pMcell[mcell].tag.num;
			pRecord = (bsMRT *)pMcell[mcell].bsMR0;

			/*
			 * Loop through all records in current MCELL.
			 */
			while ((pRecord->type != 0) && 
			       (pRecord->bCnt != 0) &&
			       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
			{
			    if (BSR_XTNTS == pRecord->type)
			    {
			        bsXtntRT     *pXtnt;
				int chainPage;
				int chainCell;
				int chainVol;

			        pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
				chainVol  = pXtnt->chainVdIndex;
				chainCell = pXtnt->chainMCId.cell;
				chainPage = pXtnt->chainMCId.page;

				/*
				 * Have a volume ID.
				 */
				if ((chainVol >= 1) &&
				    (chainVol <= MAX_VOLUMES))
				{
				    /*
				     * Collect count of vdIndex.
				     */
				    volIdArray[chainVol]++;
		    
				    /*
				     * page numbers are the same.
				     */
				    if (pageId == chainPage)
				    {
					chainTag = pMcell[chainCell].tag.num;

					/*
					 * Do they both have the same tag 
					 * number.
					 */
					if (currentTag == chainTag)
					{
					    /*
					     * We found a VALID VolID.
					     */
					    volNumber = chainVol;
					    found = TRUE;
					    break;
					}
				    }
				}
			    } /* End if */
			    pRecord = (bsMRT *) (((char *)pRecord) + 
						 roundup(pRecord->bCnt, sizeof(int))); 
			} /* end while */
		    } /* end for mcell loop */
		}
	    } /* End if valid page */

	    /*
	     * Read the next page from the volume.
	     */
	    logicalPageNumber++;
	    status = read_page_by_lbn(fd, &BMTbuffer, 
				      (LBNT)logicalPageNumber * BLOCKS_PER_PAGE, 
				      &bytesRead);
	}

	if (FALSE == found)
	{
	    int max = 0;
	   
	    /*
	     * use the best bet, the volume with the highest number
	     * of refernces.
	     */
	    for (x = 1; x <= MAX_VOLUMES; x++)
	    {
		if (max < volIdArray[x])
		{
		    volNumber = x;
		    max = volIdArray[x];
		}
	    }
	}
	
	/*
	 * Check for an UNLOCATED volume ID of 0.
	 */
	if ((0 == volNumber) || (NULL != domain->volumes[volNumber].volName))
	{
	    char numAnswer[20];
	    int  notValid = TRUE;
	    int  temp;

	    while (notValid)
	    {
#ifdef NOT_USED_CURRENTLY
/*
 * I'm commenting out the code which prompts a user for a 
 * volume ID in pass3.  The reason is we do not follow 
 * pointers in pass3, so we can assign any 'unique' number
 * we want for pass3.  If we ever decide to rerun pass1 
 * after pass3, then we will need to reactivate this code.
 */
	        /*
		 * prompt user for volume ID:
		 */
	        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_141, 
                        "Unable to find a unique volume id for volume '%s'\n"),
			 oldVolumes[counter].volName);

		writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_142, 
			 "Please enter a unique volume id. [1 - %d]? "),
			 MAX_VOLUMES);

		/*
		 * We need an answer from the user.  If there is no way to
		 * get one (Localtty is NULL), then default is to pick one at
		 * random.
		 */
		if (Localtty) 
		{
		    if (fgets (numAnswer, 20, Localtty) == NULL) 
		    {
		        return FAILURE;
		    }

		    /*
		     * Don't use writemsg here.
		     */
		    fprintf(stderr, "\n");

		    temp = atoi(numAnswer);
		
		    if ((temp > 0) && (temp <= MAX_VOLUMES))
		    {
			volNumber = temp;

			if (NULL != domain->volumes[volNumber].volName)
			{
			    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, 
				     SALVAGE_143, 
				  "Another volume has the same volume ID.\n"));
			    notValid = TRUE;
			}
			else
			{
			    notValid  = FALSE;
			}
		    } 
		}
		else 
		{
		    while (notValid)		    
		    {
			volNumber = ttyCounter++;
			if (NULL == domain->volumes[volNumber].volName)
			{
			    notValid  = FALSE;
			}
		    }
		}
#else
		while (notValid)		    
		{
		    volNumber = ttyCounter++;
		    if (NULL == domain->volumes[volNumber].volName)
		    {
			notValid = FALSE;
		    }
		}
#endif
	    }
	}
	
	/* 
	 * Copy all the info for this volume into the correct slot in the
	 * new volume array. 
	 */
	volCounter++;
	domain->volumes[volNumber].volBlockFd = oldVolumes[counter].volBlockFd;
	domain->volumes[volNumber].volRawFd   = oldVolumes[counter].volRawFd;
	domain->volumes[volNumber].volFd      = oldVolumes[counter].volFd;
	domain->volumes[volNumber].lsm        = oldVolumes[counter].lsm;
	domain->volumes[volNumber].volName    = oldVolumes[counter].volName;
	domain->volumes[volNumber].extentArray = oldVolumes[counter].extentArray;
	domain->volumes[volNumber].extentArraySize = oldVolumes[counter].extentArraySize;
	domain->volumes[volNumber].rbmtArray = oldVolumes[counter].rbmtArray;
	domain->volumes[volNumber].rbmtArraySize = oldVolumes[counter].rbmtArraySize;

	writemsg(SV_VERBOSE, catgets(_m_catd, S_SALVAGE_1, SALVAGE_138, 
		 "Setting volume '%s' to volume id %d\n"),
		 domain->volumes[volNumber].volName, volNumber);
    }

    /* 
     * Have moved all Volumes into the correct position in newVolumes 
     */
    free(oldVolumes);

    if (0 == volCounter)
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_139, 
                 "No valid volumes found\n"));
	return FAILURE;
    }

    return SUCCESS;
} /* end p3_setup_volume */


/*
 * Function Name: setup_bmt (3.3.3)
 *
 * Description :
 *  Setup the BMT (for Old ODS this is only the BMT0) for this volume, 
 *  and create extent Array.
 *
 * Input parameters:
 *  domain: The domain we are workin on.
 *  volume: The volume to work on.
 *
 * Output parameters:
 *  BMTbuffer: A buffer with BMT page read into it. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, NO_MEMORY or PARTIAL
 *
 * Side Effect:
 *  volumes->extentArray is malloced. 
 */
int setup_bmt(domainInfoT *domain,
	      volMapT     *volume,
	      bsMPgT      *BMTbuffer)
{
    char         *funcName = "setup_bmt";
    int          status;
    unsigned int lastExtent;
    long         bytesRead;
    extentT      *extentArray = NULL;

    /* 
     * Read the BMT0 page from the disk, from the known location 
     */
    status = read_page_by_lbn(volume->volFd, BMTbuffer, 
			      MSFS_RESERVED_BLKS, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != PAGESIZE))
    {
        /* 
	 * Function only returns FAILURE on read or lseek errors 
	 */
        return FAILURE;
    }

    status = find_last_bmt_extent(domain, volume, &lastExtent, BMTbuffer);
    if (FAILURE == status)
    {
	/* 
	 * Do NOT return FAILURE on PARTIAL, instead return PARTIAL
	 * after we have called load_bmt_lbn_array.  The way this is
	 * done is that we return 'status' at the end of the function.
	 */
	return status;
    }

    /* 
     * Calloc space for extentArray - Need lastExtent 
     */
    extentArray = (extentT *) salvage_calloc(lastExtent + 1, sizeof(extentT));
    if (NULL == extentArray) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_144, 
                 "calloc() failed\n"));
        return NO_MEMORY;
    }
    
    /* 
     * Set the pointer to table to volume 
     */
    volume->extentArray      = extentArray;
    volume->extentArraySize  = lastExtent;

    /* 
     * Load extent Array with valid data 
     */
    if (SUCCESS != load_bmt_lbn_array(domain, volume, BMTbuffer)) 
    {
        /* 
	 * We don't want to return FAILURE, as we may have some data we can
         * work with.  So return PARTIAL instead.
	 */
        return PARTIAL;
    }
    
    /*
     * This status is from find_last_bmt_extent()
     */
    return status;
} /* end setup_bmt */


/*
 * Function Name: setup_rbmt (3.3.4)
 *
 * Description :
 *  Read RBMT from this volume, and create Array.
 *
 * Input parameters:
 *  domain: The domain we are workin on.
 *  volume: The volume to work on.
 *
 * Output parameters:
 *  buffer: A buffer with RBMT0 read into it. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, NO_MEMORY or PARTIAL
 *
 * Side Effect:
 *  volume->rbmtArray is malloced. 
 */
int setup_rbmt(domainInfoT *domain,
	       volMapT     *volume,
	       bsMPgT      *buffer)
{
    char         *funcName = "setup_rbmt";
    int          fd;
    int          status;
    extentT      *extentArray = NULL;
    unsigned int lastExtent;
    long         bytesRead;

    fd = volume->volFd;

    /* 
     * Read the RBMT page 0 from the disk, from the known location 
     */
    status = read_page_by_lbn(fd, buffer, MSFS_RESERVED_BLKS, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != PAGESIZE))
    {
        /* 
	 * Function only returns FAILURE on read or lseek errors 
	 */
        return FAILURE;
    }

    /*
     * Load RBMT array.
     */
    if (domain->version < 4)
    {
	/*
	 * ODS V3 did not have RBMT they only had a single page
	 * of reserved BMT data, this was known as BMT0.
	 *
	 * So we are going to create an RBMT array with 1 extent
	 * which is 1 page long.  This will be the BMT0.
	 */
	lastExtent = 1;
    }
    else
    {
	/*
	 * Starting with ODS V4 we can now have multiple pages
	 * of 'Reserved' BMT data.  So we need to create an
	 * Array of these extents.  We then will need to access
	 * all of these pages as needed.
	 */

	status = find_last_rbmt_extent(domain, volume, &lastExtent, buffer);
	if (FAILURE == status)
	{
	    /* 
	     * Do NOT return FAILURE on PARTIAL, instead return PARTIAL
	     * after we have called load_bmt_lbn_array.  The way this is
	     * done is that we return 'status' at the end of the function.
	     */
	    return status;
	}
    }

    /* 
     * Calloc space for extentArray - Need lastExtent 
     */
    extentArray = (extentT *) salvage_calloc(lastExtent + 1, sizeof(extentT));
    if (NULL == extentArray) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_144, 
                 "calloc() failed\n"));
        return NO_MEMORY;
    }

    /* 
     * Set the pointer to table to volume 
     */
    volume->rbmtArray      = extentArray;
    volume->rbmtArraySize  = lastExtent;

    /* 
     * Load extent Array with valid data 
     */
    if (SUCCESS != load_rbmt_lbn_array(domain, volume, buffer)) 
    {
        /* 
	 * We don't want to return FAILURE, as we may have some data we can
         * work with.  So return PARTIAL instead.
	 */
        return PARTIAL;
    }

    /*
     * This status is from find_last_rbmt_extent()
     */
    return status;
} /* end setup_rbmt */


/*
 * Function Name: setup_fileset (3.3.5)
 *
 * Description:
 *
 *  This function creates two different structures, one of which is
 *  an array of extents which make up the fileset tag file. The second
 *  structure is an array which has an entry for each tag in the
 *  filesystem. Both of these structures are returned loaded with as
 *  much data as is available.
 *  
 * Input parameters:
 *  domain: The domain we are working on. 
 *  fileset: The fileset we are working on. 
 *
 * Output parameters:
 *  fsTagFileBuffer: A pointer to a buffer which contains the extents of 
 *                   the fileset tag file. 
 *  fsTagFileExtents : Number of extents in the buffer.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY
 *
 * Side Effect:
 *  fsTagFileBuffer is calloced. 
 *  fileset.tagArray is calloced.  
 */
int setup_fileset (domainInfoT *domain,
		   filesetLLT  *fileset,
		   extentT     **fsTagFileBuffer,
		   int         *fsTagFileExtents)
{
    char   *funcName = "setup_fileset";
    long   numTags;
    long   numExtents;
    int    status;

    *fsTagFileExtents = -1;

    /* 
     * Get number of Tags used, get number of Extents in fsTagDir 
     */
    status = find_number_tags(domain, fileset, &numTags, &numExtents);
    if ((FAILURE == status) || (-1 == numExtents) || (-1 == numTags))
    {
        /*
	 * If we have a PARTIAL, then continue with what we have.
	 */
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_145, 
                 "Setting search by volume - Failure on num tags\n"));
        D_SET_PASS2_NEEDED(domain->status);
        return FAILURE;
    }
    else if (PARTIAL == status)
    { 
        D_SET_PASS2_NEEDED(domain->status);
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_146, 
                 "Partial on find num tags : %d %d\n"), numExtents, numTags);
    }
    
    *fsTagFileExtents = numExtents;

    /*
     * We now know the how large the buffer should be, calloc it 
     */
    *fsTagFileBuffer = (extentT *) salvage_calloc(numExtents + 1, sizeof(extentT));
    if (NULL == *fsTagFileBuffer) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_144, 
                 "calloc() failed\n"));
	return NO_MEMORY;
    }

    /* 
     * Load fileset tag extents.
     */
    status = load_fileset_tag_extents(domain, fileset->tagFileMcell,
				      *fsTagFileBuffer);
    if (SUCCESS != status)
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_147, 
                 "Setting search by volume - fileset tag extents\n"));
        FS_SET_PASS2_NEEDED(fileset->status);
    }

    /* 
     * We now know the size of the tag array, calloc it 
     */
    if (numTags + 1 > fileset->maxSize)
    {
        if (numTags >= Options.tagHardLimit)
	{
	    /*
	     * The user has requested we no longer enlarge the array.
	     */
	    return FAILURE;
	}

        /*
	 * This tag is too large, we can either increment fileset->maxSize
	 * and call calloc (which could fail).  Or not handle tags this large.
	 */
        if (check_tag_array_size(fileset->maxSize, numTags + 1))
	{
	    fileset->maxSize = numTags + 1;
	}
	else
	{
	    Options.tagHardLimit = numTags + 1;
	    return FAILURE;
	}
    }

    fileset->tagArray = (filesetTagT **) salvage_calloc(numTags + 1, 
						sizeof(filesetTagT *));
    if (NULL == fileset->tagArray) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_144, 
                 "calloc() failed\n"));
	return NO_MEMORY;
    }
    
    status = load_frag_array(domain, fileset, *fsTagFileBuffer);
    if (SUCCESS != status)
    {
        if (NO_MEMORY == status)
	{
	    return status;
	}
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_148, 
                 "Setting search by volume - load frag array\n"));
        FS_SET_PASS2_NEEDED(fileset->status);
    }

    fileset->tagArraySize = numTags;

    return SUCCESS;
} /* end setup_fileset */


/*
 * Function Name: setup_tag_2 (3.3.6)
 *
 * Description
 *  Read the tag page entry for a fileset's tag 2, setup its extent data.
 *
 * Input parameters:
 *  fileset: The current fileset. 
 *  extentBuf: Array of extent entries for the fileset tag file.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */
int setup_tag_2(filesetLLT *fileset,
                extentT    *extentBuf)
{
    char *funcName = "setup_tag_2";
    int status = 0;
    int volFd = 0;
    LBNT lbn;
    LBNT extentLbn;
    int page = 0;
    bsTDirPgT tagPage;
    mcellT currMcell;
    int parentTagNum = 0;
    long bytesRead;

    /*
     * initial sanity check - this prevents read_page_by_lbn from
     * throwing up a read() error.
     */
    if (XTNT_TERM == extentBuf[0].lbn) {
	return FAILURE;
    }

    /*
     * Get the volume fd and starting lbn for the first extent, and read
     * the page (the first page of the fileset tag file) into the tag page
     * struct.
     */
    volFd = fileset->domain->volumes[extentBuf[0].volume].volFd;
    extentLbn = extentBuf[0].lbn;
    lbn = extentLbn + (page  * BLOCKS_PER_PAGE);

    status = read_page_by_lbn(volFd, &tagPage, lbn, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != PAGESIZE))
    {
        return FAILURE;
    }

    /*
     * Do sanity check on the tag page header.
     */
    if ( ( 0 != tagPage.tpPgHdr.currPage)      ||
         (-1 == tagPage.tpPgHdr.nextFreePage)  ||
         (-1 == tagPage.tpPgHdr.nextFreeMap)   ||
         (tagPage.tpPgHdr.numAllocTMaps > BS_TD_TAGS_PG)||
         (-1 == tagPage.tpPgHdr.numAllocTMaps) ||
         (tagPage.tpPgHdr.numDeadTMaps > BS_TD_TAGS_PG) ||
         (-1 == tagPage.tpPgHdr.numDeadTMaps)  ||
         (-1 == tagPage.tpPgHdr.padding) )
    {
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_149, 
                 "Tag page header corrupt\n"));
        return FAILURE;
    }

    /*
     * Map ourselves to the primary mcell for tag 2.
     */
    currMcell.vol  = tagPage.tMapA[2].tm_u.tm_s3.vdIndex;
    currMcell.page = tagPage.tMapA[2].tm_u.tm_s3.bfMCId.page;
    currMcell.cell = tagPage.tMapA[2].tm_u.tm_s3.bfMCId.cell;

    /*
     * Do sanity check on the mcell.
     */
    if ( (currMcell.cell >= BSPG_CELLS) ||
         (currMcell.cell <  0)          ||
         (currMcell.vol  <  0)          ||
         (currMcell.vol  > MAX_VOLUMES) )
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_150, 
                 "mcell corrupt\n"));
        return FAILURE;
    }

    /*
    * Create and load the element for tag 2 in our array
    */
    status = load_tag_from_mcell( fileset->domain, fileset, 2, currMcell,
                                  &parentTagNum );
    if ( SUCCESS != status )
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_151, 
                 "load_tag_from_mcell failed %d\n"), status);
        return FAILURE;
    }
    return SUCCESS;
} /* end setup_tag_2 */



/*
 * Function Name: setup_fs_tree (3.3.7)
 *
 * Description
 *  Create an entry for tag 2 (head of fileset) in both array and tree.
 *  Create an entry for lost+found in the fs tree.
 *
 * Input parameters:
 *  fileset: The fileset we are working on. 
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY.
 */
int setup_fs_tree(filesetLLT *fileset)
{
    char             *funcName = "setup_fs_tree";
    int              counter;
    int              status;
    filesetTreeNodeT *lost_found;
    statT            lost_found_stat;
    struct timeval   lost_found_time;
    struct timezone  lost_found_timezone;

    /*
     * As the only way this calls fails is memory problems, the caller
     * will exit when this fails.  So we do not cleanup these creates.
     */

    /* 
     * Create the entry for tag 2 
     */
    status = create_tag_array_element(&(fileset->tagArray[2]));
    if (SUCCESS != status)
    {
        return status;
    }

    /* 
     * Create the entry for tag 0, cheat for lost+found 
     */
    status = create_tag_array_element(&(fileset->tagArray[0]));
    if (SUCCESS != status)
    {
        return status;
    }

    /* 
     * Create the head of the fileset dir Tree 
     */
    status = create_unnamed_tree_node(&(fileset->dirTreeHead)); 
    if (SUCCESS != status)
    {
        return status;
    }
    fileset->activeNodes++;

    /* 
     * Create the lost+found entry of the fileset dir Tree 
     */ 
    status = create_unnamed_tree_node(&lost_found);
    if (SUCCESS != status)
    {
        return status;
    }
    fileset->activeNodes++;
    
    if (NULL != fileset->fsName) 
    {
	fileset->dirTreeHead->name = salvage_malloc(strlen(fileset->fsName) + 1);
	lost_found->name = salvage_malloc(strlen(fileset->fsName) + 
				          strlen(catgets(_m_catd, S_SALVAGE_1, 
					         SALVAGE_152, ".lost+found")) + 1);
	if ((NULL == fileset->dirTreeHead->name) ||
	    (NULL == lost_found->name))
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                     "malloc() failed\n"));
	    return NO_MEMORY;
	}

	strcpy(fileset->dirTreeHead->name,fileset->fsName);
	strcpy(lost_found->name,fileset->fsName);
	strcat(lost_found->name,catgets(_m_catd, S_SALVAGE_1, SALVAGE_152, 
               ".lost+found"));
    }
    else
    {
	char tmpBuf[256];
        /*
	 * We might not have found the name of the fileset.
	 * Create a temporary name in the format "fileset_settag_###".
	 */
	sprintf(tmpBuf, catgets(_m_catd, S_SALVAGE_1, SALVAGE_155, 
                "fileset_settag_%d"), fileset->filesetId.dirTag.num);

	fileset->dirTreeHead->name = salvage_malloc(strlen(tmpBuf) + 1);
	lost_found->name = salvage_malloc(strlen(tmpBuf) + 
				          strlen(catgets(_m_catd, S_SALVAGE_1, 
                                                 SALVAGE_152, ".lost+found")) + 1);
	if ((NULL == fileset->dirTreeHead->name) ||
	    (NULL == lost_found->name))
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
		     "malloc() failed\n"));
	    return NO_MEMORY;
	}

	strcpy(fileset->dirTreeHead->name,tmpBuf);
	strcpy(lost_found->name,tmpBuf);
	strcat(lost_found->name,catgets(_m_catd, S_SALVAGE_1, SALVAGE_152, 
               ".lost+found"));
    }

    /*
     * Load Head of tree (tag 2) with values
     */
    fileset->dirTreeHead->fileType      = FT_DIRECTORY;
    fileset->tagArray[2]->fileType      = FT_DIRECTORY;
    fileset->dirTreeHead->parentTagNum  = -2;
    fileset->dirTreeHead->tagNum        = 2;
    fileset->tagArray[2]->linksFound    = 1;
    fileset->tagArray[2]->firstInstance = fileset->dirTreeHead;

    /*
     * Get current time.  If that fails, set time to zero (Jan 1, 1970)
     */
    if (0 != gettimeofday(&lost_found_time, &lost_found_timezone))
    {
	lost_found_time.tv_sec = 0;
	lost_found_time.tv_usec = 0;
    }

    /*
     * Load default stat information
     */
    lost_found_stat.st_mode = S_IFDIR | S_IRWXU|S_IRGRP|S_IXGRP;
    lost_found_stat.st_nlink = 1;
    lost_found_stat.st_size = 0;
    lost_found_stat.st_uid = 0;
    lost_found_stat.st_gid = 0;
    lost_found_stat.st_mtime = lost_found_time.tv_sec;
    lost_found_stat.st_umtime = lost_found_time.tv_usec;
    lost_found_stat.st_atime = lost_found_time.tv_sec;
    lost_found_stat.st_uatime = lost_found_time.tv_usec;
    lost_found_stat.st_ctime = lost_found_time.tv_sec;
    lost_found_stat.st_uctime = lost_found_time.tv_usec;

    /*
     * Load lost+found (tag 0) with values
     */
    lost_found->fileType                = FT_DIRECTORY;
    fileset->tagArray[0]->fileType      = FT_DIRECTORY;
    lost_found->parentTagNum            = -2;
    lost_found->tagNum                  = 0;
    fileset->tagArray[0]->linksFound    = 1;
    fileset->tagArray[0]->firstInstance = lost_found;
    fileset->dirTreeHead->nextSibling   = lost_found;
    S_SET_COMPLETE (fileset->tagArray[0]->status);

    if (NO_MEMORY == create_tag_attr(&(fileset->tagArray[0]->attrs),
				     &lost_found_stat))
    {
	return NO_MEMORY;
    }

    return SUCCESS;
} /* end setup_fs_tree */


/*
 * Function Name: create_volumes (3.3.8)
 *
 * Description:
 *  Malloc and initialize volumes array 
 *
 * Output parameters:
 *  volumes: A pointer to a malloced volMapT structure. 
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY.
 */
int create_volumes(volMapT  **volumes)
{
    char  *funcName = "create_volumes";
    int   counter;

    /* 
     * Malloc new structs 
     */
    *volumes = (volMapT *) salvage_calloc(MAX_VOLUMES + 1, sizeof(volMapT));
    if (NULL == *volumes) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_144, 
                 "calloc() failed\n"));
	return NO_MEMORY;
    }

    /* 
     * Initialize Variables 
     *
     * Need to hit 0 through 256 because that's every possible usage
     * both before and after the real volume numbers have been determined.
     */
    for ( counter = 0; counter <= MAX_VOLUMES; counter++) 
    {
        (*volumes)[counter].volBlockFd      = -1;
        (*volumes)[counter].volRawFd        = -1;
        (*volumes)[counter].volFd           = -1;
	(*volumes)[counter].lsm             = 0;
        (*volumes)[counter].badVolNum       = FALSE;
	(*volumes)[counter].volName         = NULL;
	(*volumes)[counter].extentArray     = NULL;
	(*volumes)[counter].extentArraySize = 0;
    } /* end for loop */

    return SUCCESS;
} /* end create_volumes */


/*
 * Function Name: create_tag_array_element (3.3.9)
 *
 * Description:
 *  Malloc and initialize an element of type filesetTagT. 
 *
 * Output parameters:
 *  tag: A pointer to a malloced fileset tag. 
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY. 
 */
int create_tag_array_element(filesetTagT  **tag)
{
    char  *funcName = "create_tag_array_element";

    *tag = (filesetTagT *) salvage_malloc(sizeof(filesetTagT));
    if (NULL == *tag) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	return NO_MEMORY;
    }

    /*
     * Initialize variables
     */
    S_SET_DEFAULT((*tag)->status);
    (*tag)->fileType      = FT_DEFAULT;
    (*tag)->linksFound    = 0;
    (*tag)->seqNum        = 0x8001;
    (*tag)->bytesFound    = 0;
    (*tag)->extentHead    = NULL;
    (*tag)->extentTail    = NULL;
    (*tag)->firstInstance = NULL;
    (*tag)->attrs         = NULL;
    (*tag)->props         = NULL;
    (*tag)->addAttrs      = NULL;

    return SUCCESS;
} /* end create_tag_array_element */


/*
 * Function Name: create_unnamed_tree_node (3.3.10)
 *
 * Description:
 *  Malloc and initialize an element of type filesetTreeNodeT. 
 *
 * Output parameters:
 *  node: A pointer to a malloced a fileset tree node. 
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY. 
 */
int create_unnamed_tree_node(filesetTreeNodeT **node)
{
    char  *funcName = "create_unnamed_tree_node";
    extern long maxNodes;

    /* 
     * Create the a node of the fileset dir Tree 
     */
    *node = (filesetTreeNodeT *) salvage_malloc(sizeof(filesetTreeNodeT));
    if (NULL == *node) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	return NO_MEMORY;
    }

    /*
     * Initialize variables
     */
    (*node)->name         = NULL;   
    (*node)->fileType     = 0;      
    (*node)->parentTagNum = 0;
    (*node)->parent       = NULL;
    (*node)->children     = NULL;
    (*node)->nextSibling  = NULL; 
    (*node)->prevSibling  = NULL; 
    (*node)->tagNum       = 0;

    maxNodes++;
    return SUCCESS;
} /* end create_unnamed_tree_node */


/*
 * Function Name: create_extent_element (3.3.11)
 *
 * Description:
 *  Create a structure of type extentLLT and assign the passed in
 *  values into the structure.
 *
 * Input parameters: 
 *  volume: The volume the extent is on.
 *  diskBlock: The disk block the extent starts at.
 *  size: The size of the extent in bytes.  
 *  fileLocation: The starting byte in the file for the extent.
 *
 *  Caller is responsible to pass in valid data.
 *
 * Output parameters:
 *  extent: A pointer to a malloced extent record. 
 *
 * Returns:
 *  Status value - SUCCESS, INVALID or NO_MEMORY.
 */
int create_extent_element(extentLLT  **extent,
			  int        volume,
			  LBNT       diskBlock,
			  long       size,
			  long       fileLocation)
{
    char  *funcName = "create_extent_element";

    /* 
     * Create an extent element 
     */
    *extent = (extentLLT *) salvage_malloc(sizeof(extentLLT));
    if (NULL == *extent) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	return NO_MEMORY;
    }

    if (!((volume != 0) &&
	(volume < MAX_VOLUMES) &&
	(volume >= -2) &&
	( (diskBlock == PERM_HOLE_START )|| (diskBlock == XTNT_TERM )|| (diskBlock > 0) ) &&
	(size >= 0) &&
	(fileLocation >= 0)))
    {
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_156, 
                 "Corrupt extent - Vol %d LBN %u - Skipping\n"), volume, 
		 diskBlock);
	/*
	 * This is a corrupt extent, don't create the extent.
	 */
	free(*extent);
	*extent = NULL;
	return INVALID;
    }

    /*
     * Initialize variables
     */
    (*extent)->volume     = volume;
    (*extent)->diskBlock  = diskBlock;
    (*extent)->extentSize = size;
    (*extent)->byteInFile = fileLocation;
    (*extent)->next       = NULL;

    return SUCCESS;
} /* end create_extent_element */


/*
 * Function Name: create_tag_attr (3.3.12)
 *
 * Description
 *  Create and Load a tagAttribute structure. 
 *
 * Input parameters:
 *  pdata: A pointer to a data buffer, used to load values into
 *         tagAttr.
 *
 * Output parameters:
 *  tagAttr: A pointer to a malloced structure. 
 *
 * Returns:
 *  Status value - SUCCESS, NO_MEMORY or INVALID.
 */
int create_tag_attr(tagAttributeT  **tagAttr,
		    statT          *pdata)
{
    char  *funcName = "create_tag_attr";
    statT tmpStat;

    /*
     * WARNING: you MUST copy pdata into a local variable.
     * The reason for this is pdata is not 'aligned' on long
     * word boundaries due to the way we read it from disk.
     */
    memcpy(&tmpStat, pdata, sizeof(statT));

    /* 
     * Create an tagAttr structure 
     */
    *tagAttr = (tagAttributeT *) salvage_malloc(sizeof(tagAttributeT));
    if (NULL == *tagAttr) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
		 "malloc() failed\n"));
	return NO_MEMORY;
    }

    /* 
     * Initialize values 
     */
    (*tagAttr)->mode          = tmpStat.st_mode;
    (*tagAttr)->numLinks      = tmpStat.st_nlink;
    (*tagAttr)->size          = tmpStat.st_size;
    (*tagAttr)->uid           = tmpStat.st_uid;
    (*tagAttr)->gid           = tmpStat.st_gid;
    (*tagAttr)->mtime.tv_sec  = tmpStat.st_mtime;
    (*tagAttr)->mtime.tv_usec = tmpStat.st_umtime;
    (*tagAttr)->atime.tv_sec  = tmpStat.st_atime;
    (*tagAttr)->atime.tv_usec = tmpStat.st_uatime;
    (*tagAttr)->ctime.tv_sec  = tmpStat.st_ctime;
    (*tagAttr)->ctime.tv_usec = tmpStat.st_uctime;

    /*
     *  Validate the data to the best of our ability.
     *  The following may be EXTREME overkill.
     */
    if (((tmpStat.st_nlink == 0) || 
	(tmpStat.st_nlink == -1) || (tmpStat.st_nlink == -2)) || 
	(!((S_ISREG(tmpStat.st_mode)) ||
	   (S_ISDIR(tmpStat.st_mode)) ||
	   (S_ISFIFO(tmpStat.st_mode)) ||
	   (S_ISCHR(tmpStat.st_mode)) ||
	   (S_ISBLK(tmpStat.st_mode))||
	   (S_ISLNK(tmpStat.st_mode)) ||
	   (S_ISSOCK(tmpStat.st_mode)))))
    {
        /*
	 * The rest of the sttribute could be correct, but at least
	 * warn the caller something is bad.
	 */
	return INVALID;
    }
   
    return SUCCESS;
} /* end create_tag_attr */


/*
 * Function Name: create_tag_prop_list (3.3.13)
 *
 * Description
 *  Given a buffer which contains property list info, create a
 *  structure of type tagPropListT and load it with the correct values.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  pdata: A pointer to a data buffer, used to load values into tagPropList.
 *  bCnt:  Number of bytes in pdata.
 *
 * Output parameters:
 *  tagPropList: A pointer to a malloced structure. 
 *
 * Returns:
 *  Status value - SUCCESS, NO_MEMORY or INVALID.
 */
int create_tag_prop_list (domainInfoT  *domain,
			  tagPropListT **tagPropList,
			  void         *pdata,
			  int           bCnt)
{
    char  *funcName = "create_tag_prop_list";
#ifndef OLD_ODS
    bsPropListHeadT_v3 tmpPropList_old;
    bsPropListHeadT    tmpPropList;
#else
    typedef bsPropListHeadT bsPropListHeadT_v3;
    bsPropListHeadT_v3 tmpPropList_old;
#endif
    char  *nBuffer;
    char  *vBuffer;
    int   entrySize;
    int   nameSize;
    int   tail;

#ifndef OLD_ODS
    /*
     * we need to be able to handle BOTH version of the 
     * ODS type.
     */
    if (domain->version >= 4)
    {
        /*
	 * WARNING: you MUST copy pdata into a local variable.
	 * The reason for this is pdata is not 'aligned' on long
	 * word boundaries due to the way we read it from disk.
	 */
	memcpy(&tmpPropList, pdata, sizeof(bsPropListHeadT));

	nameSize  = ALIGN(tmpPropList.namelen);

	/* 
	 * Create a tagPropListT structure
	 */
	*tagPropList = (tagPropListT *) salvage_malloc(sizeof(tagPropListT));
	if (NULL == *tagPropList) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                     "malloc() failed\n"));
	    return NO_MEMORY;
	}

	/* 
	 * Create a buffer for the nameBuffer 
	 */
	nBuffer = (char *) salvage_malloc(tmpPropList.namelen + 1);
	if (NULL == nBuffer) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                     "malloc() failed\n"));
	    free(tagPropList);
	    return NO_MEMORY;
	}
    
	/* 
	 * Create a buffer for the valBuffer 
	 */
	vBuffer = (char *) salvage_malloc(tmpPropList.valuelen + 1);
	if (NULL == vBuffer) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                     "malloc() failed\n"));
	    free(tagPropList);
	    free(nBuffer);
	    return NO_MEMORY;
	}

	bzero (nBuffer, tmpPropList.namelen + 1);
	bzero (vBuffer, tmpPropList.valuelen + 1);

	switch (tmpPropList.flags &  BSR_PL_RESERVED) 
	{
	  case 0 :
	    /*
	     * Record is completely in this mcell.
	     */
	    memcpy(nBuffer, ((bsPropListHeadT *)pdata)->buffer, 
		   tmpPropList.namelen);
	    memcpy(vBuffer, &(((bsPropListHeadT *)pdata)->buffer[nameSize]), 
		   tmpPropList.valuelen);
	  
	    tail = tmpPropList.valuelen;
	    break;

	  case BSR_PL_LARGE :
	    /*
	     * Record CAN have additional records of type 20.
	     *
	     * Buffers are padded with 4 null bytes.
	     */
	    tail = MIN((bCnt - BSR_PROPLIST_HEAD_SIZE - nameSize - 4),
		       tmpPropList.valuelen); 

	    memcpy(nBuffer, ((bsPropListHeadT *)pdata)->buffer, 
		   tmpPropList.namelen);
	    memcpy(vBuffer, &(((bsPropListHeadT *)pdata)->buffer[nameSize]), 
		   tail);
	    break;

	  case BSR_PL_DELETED :
	    /*
	     * Proplist has been deleted.
	     */
	    free(nBuffer);
	    free(vBuffer);
	    free(tagPropList);
	    *tagPropList = NULL;
	    return SUCCESS;
	    break;

	  case BSR_PL_PAGE :
	    /*
	     * Not sure how to handle this type.
	     */

	  default :
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_157, 
                     "Invalid proplist flag %d\n"), tmpPropList.flags);

	    free(nBuffer);
	    free(vBuffer);
	    free(tagPropList);
	    *tagPropList = NULL;
	    return INVALID;
	    break;
	}

	/* 
	 * Initialize values
	 */
	(*tagPropList)->plType                          = PL_COMPLETE;
	(*tagPropList)->pl_num                          = tmpPropList.pl_num;
	(*tagPropList)->pl_union.pl_complete.flag       = tmpPropList.flags;
	(*tagPropList)->pl_union.pl_complete.nameLen    = tmpPropList.namelen;
	(*tagPropList)->pl_union.pl_complete.nameBuffer = nBuffer;
	(*tagPropList)->pl_union.pl_complete.valueLen   = tmpPropList.valuelen;
	(*tagPropList)->pl_union.pl_complete.valBuffer  = vBuffer;
	(*tagPropList)->pl_union.pl_complete.tail       = tail;
	(*tagPropList)->next                            = NULL;
    }
#else /* OLD_ODS */
    /*
     * We only need to handle the OLD ODS type.
     */
    if (domain->version >= 4)
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_250, 
		 "New ODS detected. This version does not support them.\n"));
	return INVALID;
    }
#endif /* OLD_ODS */
    else
    {
        /*
	 * WARNING: you MUST copy pdata into a local variable.
	 * The reason for this is pdata is not 'aligned' on long
	 * word boundaries due to the way we read it from disk.
	 */

	memcpy(&tmpPropList_old, pdata, sizeof(bsPropListHeadT_v3));

	nameSize  = ALIGN(tmpPropList_old.namelen);

	/* 
	 * Create a tagPropListT structure
	 */
	*tagPropList = (tagPropListT *) salvage_malloc(sizeof(tagPropListT));
	if (NULL == *tagPropList) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                     "malloc() failed\n"));
	    return NO_MEMORY;
	}

	/* 
	 * Create a buffer for the nameBuffer 
	 */
	nBuffer = (char *) salvage_malloc(tmpPropList_old.namelen + 1);
	if (NULL == nBuffer) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                     "malloc() failed\n"));
	    free(tagPropList);
	    return NO_MEMORY;
	}
    
	/* 
	 * Create a buffer for the valBuffer 
	 */
	vBuffer = (char *) salvage_malloc(tmpPropList_old.valuelen + 1);
	if (NULL == vBuffer) 
	{
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                     "malloc() failed\n"));
	    free(tagPropList);
	    free(nBuffer);
	    return NO_MEMORY;
	}

	bzero (nBuffer, tmpPropList_old.namelen + 1);
	bzero (vBuffer, tmpPropList_old.valuelen + 1);

	switch (tmpPropList_old.flags &  BSR_PL_RESERVED) 
	{
	  case 0 :
	    /*
	     * Record is completely in this mcell.
	     */
	    memcpy(nBuffer, ((bsPropListHeadT_v3 *)pdata)->buffer, 
                   tmpPropList_old.namelen);
	    memcpy(vBuffer, &(((bsPropListHeadT_v3 *)pdata)->buffer[nameSize]),
		   tmpPropList_old.valuelen);
	  
	    tail = tmpPropList_old.valuelen;
	    break;

	  case BSR_PL_LARGE :
	    /*
	     * Record CAN have additional records of type 20.
	     *
	     * Buffers are padded with 4 null bytes.
	     */
	    tail = MIN((bCnt - BSR_PROPLIST_HEAD_SIZE - nameSize - 4),
		       tmpPropList_old.valuelen);

	    memcpy(nBuffer, ((bsPropListHeadT_v3 *)pdata)->buffer, 
		   tmpPropList_old.namelen);
	    memcpy(vBuffer, &(((bsPropListHeadT_v3 *)pdata)->buffer[nameSize]),
		   tail);
	    break;

	  case BSR_PL_DELETED :
	    /*
	     * Proplist has been deleted.
	     */
	    free(nBuffer);
	    free(vBuffer);
	    free(tagPropList);
	    *tagPropList = NULL;
	    return SUCCESS;
	    break;

	  case BSR_PL_PAGE :
	    /*
	     * Not sure how to handle this type.
	     */

	  default :
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_157, 
                     "Invalid proplist flag %d\n"), tmpPropList_old.flags);

	    free(nBuffer);
	    free(vBuffer);
	    free(tagPropList);
	    *tagPropList = NULL;
	    return INVALID;
	    break;
	}

	/* 
	 * Initialize values
	 */
	(*tagPropList)->plType                          = PL_COMPLETE;

	(*tagPropList)->pl_union.pl_complete.flag       = tmpPropList_old.flags;
	(*tagPropList)->pl_union.pl_complete.nameLen    = tmpPropList_old.namelen;
	(*tagPropList)->pl_union.pl_complete.nameBuffer = nBuffer;
	(*tagPropList)->pl_union.pl_complete.valueLen   = tmpPropList_old.valuelen;
	(*tagPropList)->pl_union.pl_complete.valBuffer  = vBuffer;
	(*tagPropList)->pl_union.pl_complete.tail       = tail;
	(*tagPropList)->next                            = NULL;
    }

    return SUCCESS;
} /* end create_tag_prop_list */


/*
 * Function Name: load_tag_prop_data (3.3.14)
 *
 * Description
 *  Given a buffer which contains property list info, append this data 
 *  to the tail of the property list buffer.
 *
 * Input parameters:
 *  pdata: A pointer to a data buffer, used to load values into tagPropList.
 *  bCnt:  Number of bytes in pdata.
 *
 * Output parameters:
 *  tagPropList: A pointer to a malloced structure. 
 *
 * Returns:
 *  Status value - SUCCESS.
 */
int load_tag_prop_data (tagPropListT    *tagPropList,
			bsPropListPageT *pdata,
			int             bCnt)
{
    char *funcName = "load_tag_prop_data";
    int  numChar; 

    numChar = tagPropList->pl_union.pl_complete.valueLen - 
              tagPropList->pl_union.pl_complete.tail;

    if (numChar > bCnt)
    {
	numChar = bCnt;
    }

    memcpy(&(tagPropList->pl_union.pl_complete.valBuffer[tagPropList->pl_union.pl_complete.tail]),
	   pdata->buffer, numChar);

    /*
     * Buffers are padded with 4 null bytes.
     */
    tagPropList->pl_union.pl_complete.tail += numChar - 4;

    return SUCCESS;
}


/*
 * Function Name: p2_create_tag_prop_list (3.3.15)
 *
 * Description
 *
 *  This is the pass 2 version of create_tag_prop_list. The major
 *  difference between the two version is this one needs to check if
 *  this prop list has already been found, and if so free it.  Given a
 *  buffer which contains property list info, create a structure of type
 *  tagPropListT and load it with the correct values.
 *
 * Input parameters:
 *  pdata: A pointer to a data buffer, used to load values into tagPropList. 
 *  bCnt:  Number of bytes in pdata.
 *
 * Output parameters:
 *  tagPropList: A pointer to a malloced structure. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, NO_MEMORY or INVALID.
 */
int p2_create_tag_prop_list (domainInfoT  *domain,
			     tagPropListT **tagPropList,
			     void         *pdata,
			     int           bCnt)
{
    char         *funcName = "p2_create_tag_prop_list";
    int          status;
    tagPropListT *tempPropList;

    /*
     * Create a prop list entry
     */
    status = create_tag_prop_list(domain,&tempPropList, pdata, bCnt);
    if (SUCCESS != status) 
    {
        return status;
    }

    /*
     * Found a DELETED proplist entry
     */
    if (NULL == tempPropList)
    {
        return SUCCESS;
    }

    if (NULL == *tagPropList) 
    {
        /*
         * At the head of the tagPropList, so assign the newly created record.
	 */
        *tagPropList = tempPropList;
    } 
    else 
    {
        tagPropListT *current;
        tagPropListT *last;

        /*
	 * Search through all the tagPropLists untill at the end of the list.
	 * If you find a match then free newly created record.
	 */
        last = NULL;
	current = *tagPropList;

	while (NULL != current) 
        {
	    if ((current->pl_union.pl_complete.nameLen == 
		 tempPropList->pl_union.pl_complete.nameLen) &&

		(current->pl_union.pl_complete.valueLen == 
		 tempPropList->pl_union.pl_complete.valueLen) &&

		(current->pl_union.pl_complete.flag == 
		 tempPropList->pl_union.pl_complete.flag) &&

		(0 == strcmp(current->pl_union.pl_complete.nameBuffer,
			     tempPropList->pl_union.pl_complete.nameBuffer)) &&

		(0 == strcmp(current->pl_union.pl_complete.valBuffer,
			     tempPropList->pl_union.pl_complete.valBuffer)))
            {
	        /*
		 * We have found identical buffers, so free this one.
		 */
	        free(tempPropList->pl_union.pl_complete.nameBuffer);
	        free(tempPropList->pl_union.pl_complete.valBuffer);
	        free(tempPropList);
		return SUCCESS;
	    }
	    last = current;
	    current = current->next;
	} /* end while */
	last->next = tempPropList;
    }
    return SUCCESS;
} /* end p2_create_tag_prop_list */


/*
 * Function Name: create_fileset_element (3.3.16)
 *
 * Description
 *  Create a structure of type filesetLLT and initialize it. 
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *
 * Output parameters:
 *  fileset: A pointer to a malloced filesetLLT buffer. 
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY. 
 */
int create_fileset_element(filesetLLT  **fileset, 
			   domainInfoT *domain ) 
{
    char  *funcName = "create_fileset_element";

    /* 
     * Malloc space to save fileset 
     */
    *fileset = (filesetLLT *) salvage_malloc(sizeof(filesetLLT));
    if (NULL == *fileset) 
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
		 "malloc() failed\n"));
	return NO_MEMORY;
    }

    (*fileset)->statistics = salvage_malloc( sizeof(filesetStatsT) );
    if (NULL == (*fileset)->statistics ) 
    {
        free(fileset);
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	return NO_MEMORY;
    }
    bzero( (void *)((*fileset)->statistics), sizeof(filesetStatsT) );
	    
    /* 
     * load fileset with default values 
     */
    (*fileset)->status                     = 0;

    FS_SET_RESTORE((*fileset)->status);
    FS_SET_NO_CLONE((*fileset)->status);
    FS_SET_PASS1_NEEDED((*fileset)->status);

    (*fileset)->filesetId.domainId.tv_sec  = 0;
    (*fileset)->filesetId.domainId.tv_usec = 0;
    (*fileset)->filesetId.dirTag.num       = 0;
    (*fileset)->filesetId.dirTag.seq       = 0;
    (*fileset)->domain                     = domain;
    (*fileset)->fsName                     = NULL;
    (*fileset)->tagArraySize               = 0;
    (*fileset)->maxSize                    = Options.tagSoftLimit;
    (*fileset)->activeNodes                = 0;
    (*fileset)->tagArray                   = NULL;
    (*fileset)->dirTreeHead                = NULL;
    (*fileset)->fragLbnSize                = 0;
    (*fileset)->fragLbnArray               = NULL;
    (*fileset)->tagFileMcell               = NULL;
    (*fileset)->hardLinks                  = NULL;
    (*fileset)->quota                      = NULL;
    (*fileset)->next                       = NULL;

    return SUCCESS;
} /* end create_fileset_element */


/*
 * Function Name: find_number_tags (3.3.17)
 *
 * Description:
 *  Used to compute how many tags (and how many extents) are in
 *  the fileset tag file.
 *
 * Input parameters:
 *  domain: The domain we are working on. 
 *  fileset: The fileset we are working on. 
 *
 * Output parameters:
 *  tagsFound: The number of valid tags found. 
 *  extentsFound: The number of valid extents found. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or PARTIAL  
 */
int find_number_tags(domainInfoT *domain,
		     filesetLLT  *fileset,
		     long        *tagsFound,
		     long        *extentsFound)
{
    char          *funcName = "find_number_tags";
    int           extentCounter = 0;
    LBNT          lbn;
    int           fd;
    mcellT        current;
    int           lastPage = 0;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsShadowXtntT *pShadow;
    bsXtraXtntRT  *pXtraXtnt;
    bsXtntRT      *pXtnt;
    bsMPgT        mcellPage;
    long          bytesRead;
    int           status;
    int           followChain = FALSE;

    *tagsFound    = -1;
    *extentsFound = -1;

    /* 
     * We have not found the primary mcell for this fileset.  There is
     * no way for us to collect the number of tags with out this info.
     */
    if (NULL == fileset->tagFileMcell) {
	return FAILURE;
    }

    /*
     * Set current to the primary mcell of the tag file.
     */
    current.vol  = fileset->tagFileMcell->vol;
    current.page = fileset->tagFileMcell->page;
    current.cell = fileset->tagFileMcell->cell;

    /*
     * Get LBN and FD
     */
    if (SUCCESS != bmt_page_to_lbn(domain, current.vol, 
				   current.page, &lbn, &fd)) 
    {
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_158, 
                 "Can't convert BMT vol %d page %d to lbn\n"),
		 current.vol, current.page);
        return FAILURE;
    }

    /* 
     * load page pointed at by fileset.tagFileMcell.page into mcellPage buffer.
     */
    status = read_page_by_lbn(fd, &mcellPage, lbn, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != PAGESIZE))
    {
        return FAILURE;
    }

    /* 
     * Locate the fileset.tagFileMcell.cell we want in mcellPage. 
     */
    pMcell = &(mcellPage.bsMCA[current.cell]);
  
    /*
     * Loop until mcell is at end of CHAIN:
     */
    while (pMcell != NULL) 
    {
        /*
	 * Need to typecast because bsMR0 is char buffer full of bsMRTs
	 */
        pRecord = (bsMRT *)pMcell->bsMR0;
    
	/*
	 * Loop through all records in current MCELL.
	 */
	while ((pRecord->type != 0) && 
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	{
	    switch (pRecord->type)
	    {
#ifndef OLD_ODS
	      case BSR_XTNTS: 
		if (domain->version >=4)
		{
		    followChain = TRUE;
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    lastPage = 
		      pXtnt->firstXtnt.bsXA[pXtnt->firstXtnt.xCnt - 1].bsPage;
		    extentCounter += pXtnt->firstXtnt.xCnt;
		}
		break;
#endif
	      case BSR_SHADOW_XTNTS:
	        pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));
		lastPage = pShadow->bsXA[pShadow->xCnt - 1].bsPage;
		extentCounter += pShadow->xCnt;
		break;

	      case BSR_XTRA_XTNTS:
 	        pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		lastPage = pXtraXtnt->bsXA[pXtraXtnt->xCnt - 1].bsPage;
		extentCounter += pXtraXtnt->xCnt;
		break;

	      default:
		/*
		 * This is not the record type we are ignoring.
		 */
		break;
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int)));

	} /* end while */

	if (SUCCESS != find_next_mcell(domain, &current,
				       &pMcell, &mcellPage, followChain)) 
	{
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_159, 
                     "Broken mcell chain in fileset '%s'\n"), fileset->fsName);

	    *tagsFound    = lastPage * BS_TD_TAGS_PG;
	    *extentsFound = extentCounter;

	    return PARTIAL;
	}
	followChain = FALSE;
    } /* end while */

    /*
     * Did not find any pages
     */
     if (0 == lastPage)
     {
         writemsg(SV_DEBUG,catgets(_m_catd, S_SALVAGE_1, SALVAGE_160, 
                  "No tag pages found\n"));
	 return FAILURE;
     }

    /* 
     * Use last page to figure out number of Tags that have space allocated.
     */
    *tagsFound    = lastPage * BS_TD_TAGS_PG;
    *extentsFound = extentCounter;
    
    return SUCCESS;
} /* end find_number_tags */


/*
 * Function Name: find_last_bmt_extent (3.3.18)
 *
 * Description:
 *  Search through the the extent map for the BMT and compute the last extent.
 *
 * Input parameters:
 *  domain: The domain we are workin on.
 *  volume: The volume we are workin on.
 *  BMTbuffer: A pointer to the BMT buffer. 
 *
 * Output parameters:
 *  lastExtent: Returns the number of bmt extents. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, or PARTIAL  
 */
int find_last_bmt_extent(domainInfoT  *domain,
			 volMapT      *volume,
			 unsigned int *lastExtent,
			 bsMPgT       *BMTbuffer)
{
    char         *funcName = "find_last_bmt_extent";
    int          status = 0;
    int          numExtents;
    int          fd;
    int          currentCell;
    long         currentPage = 0;
    long         lbn;
    long         bytesRead;
    bsMCT        *pMcell;
    bsMRT        *pRecord;
    bsXtntRT     *pXtntData;
    bsXtraXtntRT *pXtraData;
    bsMPgT       buffer;

    fd = volume->volFd;

    /* 
     * BMT mcell has 1 extent 
     */
    numExtents = 1;
    *lastExtent = numExtents;

    status = find_bmt_chain_ptr(domain, BMTbuffer, &currentCell);
    if (status != SUCCESS) 
    {
        return status;
    }

    /*
     * For ODS version 3 :
     *   currentCell == 0 means: We have found a newly created domain with no
     *                           filesets, no BMT pages have been created.
     * For ODS version 4 : 
     *   currentCell == 0 means: We have only the single extent in mcell 4.
     */

    if (0 == currentCell)
    {
	return SUCCESS;
    }

    /*
     * point to the First BMT page.
     */
    pMcell = &(BMTbuffer->bsMCA[currentCell]);
   
    /*
     * Follow NEXT until last page of extents.
     */
    while (NULL != pMcell) 
    {
        /*
	 * Verify that we don't have an invalid mcell NEXT pointer.
	 */
        if ((pMcell->nextMCId.cell < 0) ||
	    (pMcell->nextMCId.cell >= BSPG_CELLS))
	{
	    return PARTIAL;
	}

	/*
	 * Verify that the record is of the correct type
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	/*
	 * Now loop through records.
	 */
	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	{
	    if (BSR_XTRA_XTNTS != pRecord->type) 
	    {
		/* 
		 *  We have found a BOGUS type, increment extents by 1.
		 *  This will leave a hole in the array which we will use.
		 */
		numExtents += 1;
	    } 
	    else 
	    {
		pXtraData  = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		numExtents += pXtraData->xCnt - 1;
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int)));
 
	} /* end while */

	currentCell = pMcell->nextMCId.cell;

	/*
	 * For ODS V4 we might need to move to a new RBMT page
	 */

	if (!((pMcell->nextVdIndex == 0) &&
	      (pMcell->nextMCId.page == 0) &&
	      (pMcell->nextMCId.cell == 0)))
	{
	    if (domain->version < 4)
	    {
		pMcell = &(BMTbuffer->bsMCA[currentCell]);
	    }
	    else
	      {
		/*
		 * Do we move to the next page
		 */
		if (pMcell->nextMCId.page != currentPage)
		{
		    currentPage = pMcell->nextMCId.page;

		    /*
		     * Set next page
		     */
		    lbn = volume->rbmtArray[currentPage].lbn;

		    /*
		     * Load the next page.
		     */
		    status = read_page_by_lbn(fd, &buffer, lbn, &bytesRead);
		    if ((SUCCESS != status) || (bytesRead != PAGESIZE))
		    {
			return FAILURE;
		    }

		    pMcell = &(buffer.bsMCA[currentCell]);
		}
		else
		{
		  
		    if (0 == currentPage)
		    {
			pMcell = &(BMTbuffer->bsMCA[pMcell->nextMCId.cell]);
		    }
		    else
		    {
		    
			pMcell = &(buffer.bsMCA[pMcell->nextMCId.cell]);
		   
		    }
		   
		}
	    }
	} 
	else 
	{
	    pMcell = NULL;
	}
    }

    *lastExtent = numExtents;
    return SUCCESS;
} /* end find_last_bmt_extent */


/*
 * Function Name: load_root_tag_file (3.19)
 *
 * Description:
 *  Given a RBMT/BMT0 buffer for a volume, determine if the root tag file
 *  is on this volume. If it is then load the filesets into the domain
 *  structures.
 *
 * Input parameters:
 *  mcellPage: A pointer to the BMT0 buffer. 
 *  domain: The domain we are working on.
 *  volume: The volume we are working on
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, NO_MEMORY or PARTIAL. 
 * 
 * Side Effects: 
 *  For each fileset found, add it to the domain structure.
 */
int load_root_tag_file(bsMPgT      *mcellPage,
		       domainInfoT *domain,
		       int         volume)
{
    char         *funcName = "load_root_tag_file";
    mcellT       current;
    bsMCT        *pMcell;
    bsMRT        *pRecord;
    bsXtntRT     *pXtnt;
    bsXtraXtntRT *pXtra;
    long         bytesRead;
    int          status;

    /*
     * Check to see if this is the PRIMARY root tag mcell
     * This can be done TWO different ways             
     * 1) Does Mcell 2 have extents - YES it is Primary
     * 2) Does Mcell 4 have a Mattr record?            
     *    YES) Get the tag from pXtnt->bfSetDirTag.num 
     */

    /* 
     * At this time we have choosen to use option 1       
     */
    current.cell = 2;
    current.vol  = volume;
    current.page = 0;

    pMcell = &(mcellPage->bsMCA[current.cell]);

    /* 
     * Now on correct cell we need to step through this record 
     */
    pRecord = (bsMRT *)pMcell->bsMR0;

    /*
     * Now loop through records.
     */
    while ((pRecord->type != 0) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
    {
        /*
	 * Do work only on BSR_XTNTS record type, ignore other types
	 */
        if (pRecord->type == BSR_XTNTS) 
	{
	    /* 
	     * Set up data buffer 
	     */
	    pXtnt =  (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
#ifdef OLD_ODS
	    if (pXtnt->xu.x.xCnt > 1) 
#else
	    if (pXtnt->firstXtnt.xCnt > 1) 
#endif
	    {
	        /* 
		 * Get list of filesets from root tag 
		 */
	        status = load_filesets(mcellPage, pXtnt, domain, 
				       current.vol, BSR_XTNTS); 
	        if (SUCCESS != status)
		{
		    return status;
		}
	    }

	    /*
	     * load CHAIN chain, in case we have more than a single record with
	     * extents.
	     */
	    current.vol  = pXtnt->chainVdIndex; 
	    current.page = pXtnt->chainMCId.page;
	    current.cell = pXtnt->chainMCId.cell;
	}
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 
    } /* end while */

    /*
     * Need to check the very rare case were we have more than
     * single page of filesets.
     */
    if ((current.vol != 0) ||
	(current.page != 0))
    {
        bsMPgT mcellPage;
	LBNT   lbn;
	int    fd;
	
	/*
	 * Get New LBN and FD
	 */
	if (SUCCESS != bmt_page_to_lbn(domain, current.vol,
				       current.page, &lbn, &fd)) 
	{
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_158, 
                     "Can't convert BMT vol %d page %d to lbn\n"),
		     current.vol, current.page);
	    return PARTIAL;
	}

	/* 
	 * load new page into mcellPage buffer.
	 */
	status = read_page_by_lbn(fd, &mcellPage, lbn, &bytesRead);
	if ((SUCCESS != status) || (bytesRead != PAGESIZE))
	{
	    return PARTIAL;
	}

	/* 
	 * Locate the cell we want in mcellPage. 
	 */
	pMcell = &(mcellPage.bsMCA[current.cell]);

	while (NULL != pMcell) 
	{
	    /* 
	     * Now on correct cell we need to step through this record 
	     */
	    pRecord = (bsMRT *)pMcell->bsMR0;

	    /*
	     * Now loop through records.
	     */
	    while ((pRecord->type != 0) &&
		   (pRecord->bCnt != 0) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	    {

	        if (pRecord->type == BSR_XTRA_XTNTS) 
		{
	            pXtra =  (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    if (pXtra->xCnt > 1) 
		    {
		        /* 
			 * Get list of filesets from root tag 
			 */
		        if (SUCCESS != load_filesets(&mcellPage, pXtra, domain,
						     current.vol,
						     BSR_XTRA_XTNTS))
			{
			    return PARTIAL;
			}
		    }
		}
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } /* end while */

	    /* 
	     * Find next mcell
	     */
	    if (SUCCESS != find_next_mcell(domain, &current, 
					   &pMcell, &mcellPage, FALSE)) 
	    {
	        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_161, 
                         "Broken mcell chain - root tag file\n"));
	        return PARTIAL;
	    }
	} /* end while */
    }
    return SUCCESS;
} /* end load_root_tag_file */


/*
 * Function Name: find_bmt_chain_ptr (3.3.20)
 *
 * Description:
 *  In ODS version 3 : 
 *     Follow chain pointer in BSR_XTNTS record in BMT Page 0, cell 0.
 *     This normally leads to cell 6.
 *
 *  In ODS version 4 :
 *     Follow chain pointer in BSR_XTNTS record in RBMT Page 0, cell 4.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  BMTbuffer: A pointer to the BMT buffer. 
 *
 * Output parameters:
 *  cellNumber: A pointer to an integer cell number.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.  
 */
int find_bmt_chain_ptr(domainInfoT  *domain,
		       bsMPgT *BMTbuffer,
		       int    *cellNumber)
{
    char         *funcName = "find_bmt_chain_ptr";
    bsMCT        *pMcell;
    bsMRT        *pRecord;
    bsXtntRT     *pXtntData;

    *cellNumber = -1;

    /*
     * For ODS version 3 : 
     *   Start with cell 0 of BMT page 0, follow the "chain pointer" 
     *   normally will be mcell 6, but could also be 0.
     *
     * For ODS version 4 : 
     *   Start with cell 4 of RBMT page 0, follow the "chain pointer" 
     *   normally will be mcell 7, but could also be 0.
     */

    if (domain->version < 4)
    {
	pMcell  = &(BMTbuffer->bsMCA[0]);
    }
    else
    {
	pMcell  = &(BMTbuffer->bsMCA[4]);
    }

    pRecord = (bsMRT *)pMcell->bsMR0;

    while ((pRecord->type != 0) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))) &&
	   (*cellNumber == -1)) 
    {
        if (pRecord->type == BSR_XTNTS) 
	{
	    pXtntData   = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
            *cellNumber = pXtntData->chainMCId.cell;
        }
        pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int)));
    } /* end while */

    if (*cellNumber == -1) 
    {
       /* 
	* Didn't find BSR_XTNTS record - corrupt 
	*/
        return FAILURE;
    }

    return SUCCESS;
} /* find_bmt_chain_ptr */


/*
 * Function Name: bmt_page_to_lbn (3.3.21)
 *
 * Description:
 *  Given a page convert bmt page to the lbn
 *
 * Input parameters:
 *  domain: The domain which is being worked on.
 *  volume: The volume which the page is on.
 *  page: The page number we are looking for.
 *
 * Output parameters:
 *  lbn: The logical block number of the page.
 *  fd:  The file descriptor of the volume which the page is on.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or INVALID.
 */
int bmt_page_to_lbn(domainInfoT *domain,
		    int         volume,
		    int         page,
		    LBNT        *lbn,
		    int         *fd)
{
    char   *funcName = "bmt_page_to_lbn";
    int    min;
    int    max;
    int    current;
    int    found;
    int    vol;
    int    status;

    *lbn = XTNT_TERM;
    *fd  = -1;

    /*
     * error check on volume.
     */
    if ((0 >= volume) || 
	(volume > MAX_VOLUMES))
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_162, 
                 "Invalid volume ID %d\n"), volume);
        return FAILURE;
    }

    /*
     * A volume which is currently not in use.
     */
    if (NULL == domain->volumes[volume].volName)
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_163, 
                 "Volume ID %d not in use\n"), volume);
        return INVALID;
    }

    /* 
     *Convert volume into fd 
     */
    *fd = domain->volumes[volume].volFd;    

    /*
     * Do the conversion. In this case, the "vol" arg is "dont care".
     */
    status = convert_page_to_lbn( domain->volumes[volume].extentArray, 
                                  domain->volumes[volume].extentArraySize, 
                                  page, lbn, &vol );
    return status;
} /* end bmt_page_to_lbn */


/*
 * Function Name: frag_page_to_lbn (3.3.22)
 *
 * Description:
 *  Given a page convert frag page to the lbn
 *
 * Input parameters:
 *  fileset: The fileset we are working on. 
 *  page: The page number we are lookin
 *
 * Output parameters:
 *  lbn: The logical block number of the page.
 *  vol: The volume the frag is on.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.  
 */
int frag_page_to_lbn(filesetLLT  *fileset,
		     int         page,
		     LBNT        *lbn,
		     int         *vol)
{
    char   *funcName = "frag_page_to_lbn";
    int    min;
    int    max;
    int    current;
    int    found;
    int    status;

    *lbn = XTNT_TERM;
    *vol = -1;

    if (NULL == fileset->fragLbnArray)
    {
        return FAILURE;
    }

    status = convert_page_to_lbn( fileset->fragLbnArray, fileset->fragLbnSize,
                                  page, lbn, vol );
    return status;
} /* end frag_page_to_lbn */


/*
 * Function Name: convert_page_to_lbn (3.3.23)
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

int convert_page_to_lbn( extentT *extentArray,
                         int     extentArraySize,
                         int     page,
                         LBNT    *lbn,
                         int     *vol)
{
    char   *funcName = "convert_page_to_lbn";
    int    min;
    int    max;
    int    current;
    int    found;
    int    pageDifference;

    *lbn = XTNT_TERM;
    *vol = -1;

    /*
     * Initialize our search bounds.
     */
    min = 0;
    max = extentArraySize;
    current = (min + max) / 2;

    if ((page >= extentArray[max].page) ||
	(page < 0))
    {
        /*
	 * Page is out of bounds
	 */
         writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_164, 
                  "Page %d is out of bounds. Valid range 0 to %d\n"),
		  page,extentArray[max].page);
         return FAILURE;
    }

    /*
     * Step 1 : Locate extent which contains page.
     */
    found = FALSE;
    while (FALSE == found) 
    {
        if (min == max) 
	{
	    /*
	     * Check if min = max : Found Page
	     */
	     current = min;
	     found = TRUE;
	}
	/*
	 * Change second test to page <= extentArray page so it will
	 * handle entries with the same page number. Otherwise we can
	 * get into an infinite loop.
	 *
	 * The change to <= causes us to read the wrong page in the
	 * following case:
	 *
	 *     0           xxxx
	 *     1           yyyy
	 *     2           zzzz
	 *     3           -1
	 *
	 * When we ask for page 2 we get the LBN for page 1.
	 *
	 * The error case we were trying to block was:
	 *     0           xxxx
	 *     1           yyyy
	 *     2           zzzz
	 *     2           qqqq
	 *     3           -1
	 *
	 * This causes us to jump back and forth from extent 2 and 1.
	 *
	 */ 
	else if ((page >= extentArray[current].page) &&
		 (page < extentArray[current+1].page))
	{
	    /*
	     * Check if page falls between current and current+1 : 
	     *   Found Page
	     */
	     found = TRUE;
	} else if ((page == extentArray[current].page) &&
		   (page == extentArray[current+1].page)) {
	    /*
	     * Both pages extent entires match, this causes an infinite
	     * loop.  So we need need to pick one or the other.  Either
	     * one of them could be correct so we need to pick one
	     * of them.  Might as well pick this one.
	     */
	    found = TRUE;
	}
	else if (page > extentArray[current].page) 
	{
	    /*
	     * Cut our search area in 1/2 for next pass, trim lower half.
	     */
	    min = current;	
	    current = (min + max) / 2;	    
	} 
	else 
	{
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
    if ( (extentArray[current].lbn !=XTNT_TERM ) && (extentArray[current].lbn != PERM_HOLE_START) )  
    {
	pageDifference = page - extentArray[current].page;
	*lbn = (extentArray[current].lbn + (LBNT)pageDifference *BLOCKS_PER_PAGE);
    } 
    else
    {
        /*
	 * Sparse Hole, or Missing extent which has been filled.
	 */
	*lbn = extentArray[current].lbn;

    }
    *vol = extentArray[current].volume;
    return SUCCESS;
} /* end convert_page_to_lbn */


/*
 * Function Name: find_next_mcell (3.3.24)
 *
 * Description:
 *   Given the current mcell, and the next mcell, load page as needed.
 *
 * Input parameters:
 *  domain: The current domain
 *  current: The current mcell.
 *  pMcell: A pointer to the current mcell.
 *  pPage: A pointer to the current page.
 *  followChain : If set to TRUE, follow chain instead of next.
 *
 * Output parameters:
 *  current: The next mcell.
 *  pMcell: A pointer to the next mcell.
 *  pPage: A pointer to the page which mcell is on.
 *
 * Returns:
 *  Status value - SUCCESS, PARTIAL
 */
int find_next_mcell(domainInfoT *domain,
		    mcellT      *current,
		    bsMCT       **pMcell,
		    bsMPgT      *pPage,
		    int         followChain)
{
    char     *funcName = "find_next_mcell";
    int      fd;
    LBNT     lbn;
    long     bytesRead;
    int      status;
    mcellT   chain;
    bsMRT    *pRecord;
    bsXtntRT *pXtnt;

    /*
     * Initialize variables
     */
    chain.vol  = 0;	/* If no BSR_XTNTS record, chain won't get set. */
    chain.page = 0;	/* If no BSR_XTNTS record, chain won't get set. */
    chain.cell = 0;	/* If no BSR_XTNTS record, chain won't get set. */

    if (TRUE == followChain)
    {
	/*
	 * through mcell looking for chain.
	 */
	pRecord = (bsMRT *)(*pMcell)->bsMR0;

	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &((*pMcell)->bsMR0[BSC_R_SZ]))))
	{
	    if (pRecord->type == BSR_XTNTS) 
	    {
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		chain.vol  = pXtnt->chainVdIndex; 
		chain.page = pXtnt->chainMCId.page;
		chain.cell = pXtnt->chainMCId.cell;
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int)));
	} /* end while */
    }
    else
    {
	chain.vol  = (*pMcell)->nextVdIndex;
	chain.page = (*pMcell)->nextMCId.page;
	chain.cell = (*pMcell)->nextMCId.cell;
    }

    if ((0 == chain.vol) &&
	(0 == chain.page) &&
	(0 == chain.cell))
    {
	/* 
	 * End of chain 
	 */
	*pMcell = NULL;
    }
    else if ((chain.vol <= 0) ||
	     (chain.page < 0) ||    /* Changed for ODS V4 */
	     (chain.cell < 0) ||
	     (chain.cell >= BSPG_CELLS) ||
	     (chain.vol > MAX_VOLUMES))
    {
        /* 
	 * End of chain 
	 */
        *pMcell = NULL;
    }
    else if (TRUE == domain->volumes[chain.vol].badVolNum)
    {
	/*
	 * Found chain, but points to bad volume.
	 */
	return PARTIAL;
    }

    else if ((current->vol  == chain.vol) &&
	     (current->page == chain.page) &&
	     (current->cell == chain.cell)) 
    {
        /*
         * points to itself.  End chain, otherwise loop.
	 */
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_165, 
                 "Mcell chain %d,%d,%d points to itself\n"),
		 chain.vol, chain.page, chain.cell);
        *pMcell = NULL;
    }
    else if ((current->vol  == chain.vol) &&
	     (current->page == chain.page)) 
    {
        /*
	 * Next mcell is on the same vol/page.
	 */
        current->cell = chain.cell;
	*pMcell = (&pPage->bsMCA[current->cell]);
    } 
    else 
    {
        /* 
	 * Mcell is on a different page or volume 
	 */
        current->vol  = chain.vol;
	current->page = chain.page;
	current->cell = chain.cell;

	/*
	 * Get New LBN and FD
	 */
	if (SUCCESS != bmt_page_to_lbn(domain, current->vol,
				       current->page, &lbn, &fd)) 
	{
	    /*
	     * Found chain but unable to locate it on disk
	     */
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_158, 
                     "Can't convert BMT vol %d page %d to lbn\n"),
		     current->vol, current->page);
	    return PARTIAL;
	}
  
	/* 
	 * load new page into mcellPage buffer.
	 */
	status = read_page_by_lbn(fd, pPage, lbn, &bytesRead);
	if ((SUCCESS != status) || (bytesRead != PAGESIZE))
	{
	    /*
	     * Found chain but unable to locate it on disk
	     */
	    return PARTIAL;
	}

	/* 
	 * Locate the cell we want in mcellPage. 
	 */
	*pMcell = &(pPage->bsMCA[current->cell]);
    }

    return SUCCESS;
} /* end find_next_mcell */


/*
 * Function Name: get_domain_version (3.3.25)
 *
 * Description
 *  
 *  Check the domain to see which version of ODS it uses.
 *
 * Input parameters:
 *  domain: The domain to check 
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 *
 * Side effect:
 *  domain->version is set.
 */
int get_domain_version(domainInfoT *domain)
{
    char    *funcName = "get_domain_version";
    int     status;
    int     version1 = 0;
    int     version2 = 0;
    int     volCounter;
    int     fd;
    int     found;
    int     logicalPageNumber;
    long    bytesRead;
    bsMPgT  BMTbuffer;

    if (domain->version != 0)
    {
        /*
	 * Preset by the hidden flag.
	 */
        return SUCCESS;
    }

    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_166, 
             "Attempting to locate domain version\n"));

    /*
     * Loop through volumes until we have a valid one.
     *
     * Note that the loop starts at 0 and ends before MAX_VOLUMES in this
     * special case before the volumes have been put at their correct
     * volume numbers.
     */
    for (volCounter = 0; volCounter < MAX_VOLUMES; volCounter++) 
    {
        /* 
	 * Check to see if the current volume has a valid filedescriptor.
	 */
        if (-1 != domain->volumes[volCounter].volFd) 
	{
	    break;
        }
    }

    /*
     * Only the first volume found is checked because there's not much
     * salvage can do if the domain version differs between different
     * volumes in this domain.  Multiple places on this volume are checked
     * so that a single error won't give us a bad domain version.
     * Also note that about the only way to have volumes with differing
     * versions is if either the user mucked about in /etc/fdmns, or
     * passed in the wrong volumes as arguments to salvage.
     */

    /*
     * Loop through all pages on disk until we find a BMT page.
     */
    fd    = domain->volumes[volCounter].volFd;
    found = FALSE;

    /*
     * Has the user request us to do a pass3?  If so we want to skip over 
     * BMT Page 0 & 1 as they contain information which could have come 
     * from a new mkfdmn
     */
    if (D_IS_PASS3_NEEDED(domain->status))
    {
        logicalPageNumber = 6;     /* Start at block 96 - Root Tag Dir */
    }
    else
    { 
        logicalPageNumber = 2;     /* Start at block 32 - BMT Page 0 */
    }
    

    status = read_page_by_lbn(fd, &BMTbuffer, 
			      logicalPageNumber * BLOCKS_PER_PAGE,
			      &bytesRead);

    while ((found == FALSE) &&
	   (SUCCESS == status) &&
	   (bytesRead == PAGESIZE))
    {
	int validPage;

	validPage = validate_page(BMTbuffer);

	if (TRUE == validPage)
	{
	    version1 = BMTbuffer.megaVersion;
	    found    = TRUE;
	} /* End if valid page */

	/*
	 * Read the next page from the volume.
	 */
	logicalPageNumber++;
	status = read_page_by_lbn(fd, &BMTbuffer, 
				  logicalPageNumber * BLOCKS_PER_PAGE, 
				  &bytesRead);
    }

    /*
     * Read pages until we have a second BMT page.
     */
    found = FALSE;

    while ((found == FALSE) &&
	   (SUCCESS == status) &&
	   (bytesRead == PAGESIZE))
    {
	int validPage;

	validPage = validate_page(BMTbuffer);

	if (TRUE == validPage)
	{
	    version2 = BMTbuffer.megaVersion;
	    found    = TRUE;
	} /* End if valid page */

	/*
	 * Read the next page from the volume.
	 */
	logicalPageNumber++;
	status = read_page_by_lbn(fd, &BMTbuffer, 
				  logicalPageNumber * BLOCKS_PER_PAGE, 
				  &bytesRead);
    }

    /*
     * compare versions - if different return Failure.
     */
    if (version1 != version2)
    {
        return FAILURE;
    }

    /*
     * Check if in valid range.
     */
#ifdef OLD_ODS
    if (version1 != 3)
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_250, 
		 "New ODS detected. This version does not support them.\n"));
        return FAILURE;
    }
#else
    if ((version1 < 3) || (version1 > 4))
    {
	return FAILURE;
    }
#endif

    /*
     * assign value
     */
    domain->version = version1;
    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_265,
			       "Located domain version: %d\n"), version1);
    return SUCCESS;
}


/*
 * Function Name: validate_page (3.3.26)
 *
 * Description
 *  
 *  Check a page to see if it a valid BMT page.
 *
 * Input parameters:
 *  page : The page to check.
 *
 * Returns:
 *  Status value - TRUE or FALSE. 
 */
int validate_page(bsMPgT  page)
{
    char    *funcName = "validate_page";
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    int     mcell;

    /*
     * NOTE: This check should be for all ODS versions that are out
     * there.  E.g., when ODS V5 is released, it needs to be made valid
     * in this check for all older support pools, including the ones
     * that do not support ODS V5.  get_domain_version should deal
     * with ODS versions that are not supported in that release, and
     * all other calls to validate page make checks to ensure that
     * the page megaVersion matches the domain megaVersion which was
     * determined by get_domain_version.
     */
    if ((page.megaVersion < 3) || (page.megaVersion > 4))
    {
	/*
	 * INVALID PAGE
	 */
	return FALSE;
    }
    else
    {
	/*
	 * Possibly a BMT page.
	 */
	if ((page.freeMcellCnt == 0) && 
	    (page.nextfreeMCId.page == 0) && 
	    (page.nextfreeMCId.cell == 0))
	{
	    /*
	     * This most likely is a Full Page
	     */
	    pMcell = (struct bsMC *)page.bsMCA;

	    /* 
	     * Due to random garbage lets do more checks.  Check if 
	     * tag.num and bfSetTag.num are == 0, for each mcell.
	     * BMT1 mcell 0 is special case, so check any other mcell.
	     * There is a small chance that a few BAD BMT pages might 
	     * slip by these tests.
	     */
	    for (mcell = 1; mcell < BSPG_CELLS; mcell++)
	    {
		if ((pMcell[mcell].tag.num == 0) && 
		    (pMcell[mcell].bfSetTag.num == 0))
		{
		    /*
		     * BOGUS CELL - INVALID PAGE
		     */
		    return FALSE;
		}
		
		/* 
		 * To increase the odds of a good page check record 
		 * Header.  If we need more checks, then check all 
		 * records in the mcell not just the first one.
		 */
		pRecord = (bsMRT *)pMcell[mcell].bsMR0;
	      
		if ((pRecord->type < 0) || (pRecord->type > 256) || 
		    (pRecord->bCnt < 0) || (pRecord->bCnt > 268) || 
		    (pRecord->version != 0)) 
		{
		    /*
		     * BOGUS RECORD - INVALID PAGE
		     */
		    return  FALSE;
		}
	    }
	}
	else if ((page.freeMcellCnt >= 0) && 
		 (page.freeMcellCnt <= 28) &&
		 (page.nextfreeMCId.page == page.pageId))
	{
	    /*
	     * This is a PARTIAL BMT PAGE.
	     */
	    pMcell = (struct bsMC *)page.bsMCA;

	    /* 
	     * Due to random garbage lets do more checks.  Check if 
	     * tag.num and bfSetTag.num are == 0, for each mcell.
	     * BMT1 mcell 0 is special case, so check any other mcell.
	     * It is know that a few BAD BMT pages slip by this test. 
	     */
	    for (mcell = 1; mcell < BSPG_CELLS; mcell++)
	    {
		if ((pMcell[mcell].tag.num == 0) && 
		    (pMcell[mcell].bfSetTag.num == 0))
		{
		    /*
		     * This should be a free mcell on chain of mcells.
		     */
		    if (page.pageId == pMcell[mcell].nextMCId.page)
		    {
			/*
			 * Valid Mcell.
			 */
		    }
		    /*
		     * This should be the last free mcell on this page.
		     */
		    else if ((pMcell[mcell].tag.seq == 0) &&
			     (pMcell[mcell].bfSetTag.seq == 0) &&
			     (pMcell[mcell].nextVdIndex == 0) && 
			     (pMcell[mcell].nextMCId.page == 0) &&
			     (pMcell[mcell].nextMCId.cell == 0) &&
			     (pMcell[mcell].linkSegment == 0))
		    {
			/*
			 * Valid Mcell.
			 */
		    }
		    else 
		    {
			/*
			 * BOGUS CELL
			 */
			return FALSE;
		    }
		}
			
		/*
		 * This should a valid mcell, so check it out.
		 */
		pRecord = (bsMRT *)pMcell[mcell].bsMR0;
		
		if ((pRecord->type < 0) || (pRecord->type > 256) || 
		    (pRecord->bCnt < 0) || (pRecord->bCnt > 268) || 
		    (pRecord->version != 0)) 
		{
		    return FALSE;
		}
	    } /* end for mcell loop */
	}
	else
	{
	    /*
	     * INVALID PAGE
	     */
	    return FALSE;
	}
    }
    return TRUE;
}


/*
 * Function Name: p2_create_tag_prop_head (3.3.27)
 *
 * Description
 *   For pass2/pass3 with new ODS create a prop list head for this tag.
 *
 * Input parameters:
 *  pdata: A pointer to a data buffer, used to load values into tagPropList. 
 *  bCnt:  Number of bytes in pdata.
 *
 * Output parameters:
 *  tagPropList: A pointer to a malloced structure. 
 *
 * Returns:
 *  Status value - SUCCESS, NO_MEMORY, or INVALID.
 */
int p2_create_tag_prop_head (tagPropListT    **tagPropList,
			     bsPropListHeadT *pdata,
			     int             bCnt)
{
    char            *funcName = "p2_create_tag_prop_head";
    bsPropListHeadT tmpPropList;
    char            *nBuffer;
    char            *vBuffer;
    unsigned int    entrySize;
    unsigned int    nameSize;
    unsigned int    tail;

    /*
     * This function should only be called for version 4
     * of the ODS proplists.
     */
#ifndef OLD_ODS

    /*
     * WARNING: you MUST copy pdata into a local variable.
     * The reason for this is pdata is not 'aligned' on long
     * word boundaries due to the way we read it from disk.
     */
    memcpy(&tmpPropList, pdata, sizeof(bsPropListHeadT));

    nameSize  = ALIGN(tmpPropList.namelen);

    /* 
     * Create a tagPropListT structure
     */
    *tagPropList = (tagPropListT *) salvage_malloc(sizeof(tagPropListT));
    if (NULL == *tagPropList) 
    {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	return NO_MEMORY;
    }

    /* 
     * Create a buffer for the nameBuffer 
     */
    nBuffer = (char *) salvage_malloc(tmpPropList.namelen + 1);
    if (NULL == nBuffer) 
    {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	free(tagPropList);
	return NO_MEMORY;
    }
    
    /* 
     * Create a buffer for the valBuffer 
     */
    vBuffer = (char *) salvage_malloc(tmpPropList.valuelen + 1);
    if (NULL == vBuffer) 
    {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	free(tagPropList);
	free(nBuffer);
	return NO_MEMORY;
    }

    bzero (nBuffer, tmpPropList.namelen + 1);
    bzero (vBuffer, tmpPropList.valuelen + 1);

    switch (tmpPropList.flags &  BSR_PL_RESERVED) 
    {
      case 0 :
	/*
	 * Record is completely in this mcell.
	 */
	memcpy(nBuffer, ((bsPropListHeadT *)pdata)->buffer, 
	       tmpPropList.namelen);
	memcpy(vBuffer, &(((bsPropListHeadT *)pdata)->buffer[nameSize]), 
	       tmpPropList.valuelen);
	tail = tmpPropList.valuelen;
	/* 
	 * Initialize values
	 */
	(*tagPropList)->plType                          = PL_COMPLETE;
	(*tagPropList)->pl_num				= tmpPropList.pl_num;
	(*tagPropList)->pl_union.pl_complete.flag       = tmpPropList.flags;
	(*tagPropList)->pl_union.pl_complete.nameLen    = tmpPropList.namelen;
	(*tagPropList)->pl_union.pl_complete.valueLen   = tmpPropList.valuelen;
	(*tagPropList)->pl_union.pl_complete.nameBuffer = nBuffer;
	(*tagPropList)->pl_union.pl_complete.valBuffer  = vBuffer;
	(*tagPropList)->pl_union.pl_complete.tail       = tail;
	(*tagPropList)->next                            = NULL;

	break;

      case BSR_PL_LARGE :
	/*
	 * Record CAN have additional records of type 20.
	 *
	 * Buffers are padded with 4 null bytes.
	 */
	tail = MIN((bCnt - BSR_PROPLIST_HEAD_SIZE - nameSize - 4),
		   tmpPropList.valuelen);

	memcpy(nBuffer, ((bsPropListHeadT *)pdata)->buffer, 
	       tmpPropList.namelen);
	memcpy(vBuffer, &(((bsPropListHeadT *)pdata)->buffer[nameSize]), 
	       tail);

	/* 
	 * Initialize values
	 */
	if (tail == tmpPropList.valuelen) {
	    /*
	     * Rare case where the record fits completely in this mcell
	     */
	    (*tagPropList)->plType                      = PL_COMPLETE;
	} else {
	    (*tagPropList)->plType                      = PL_HEAD;
	}
	(*tagPropList)->pl_num			    = tmpPropList.pl_num;
	(*tagPropList)->pl_union.pl_head.flag       = tmpPropList.flags;
	(*tagPropList)->pl_union.pl_head.nameLen    = tmpPropList.namelen;
	(*tagPropList)->pl_union.pl_head.valueLen   = tmpPropList.valuelen;
	(*tagPropList)->pl_union.pl_head.nameBuffer = nBuffer;
	(*tagPropList)->pl_union.pl_head.valBuffer  = vBuffer;
	(*tagPropList)->pl_union.pl_head.valSize    = tail;
	(*tagPropList)->next                        = NULL;
	break;

      case BSR_PL_DELETED :
	/*
	 * Proplist has been deleted.
	 */
	free(nBuffer);
	free(vBuffer);
	free(tagPropList);
	*tagPropList = NULL;
	break;

      case BSR_PL_PAGE :
	/*
	 * Not sure how to handle this type.
	 */

      default :
	writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_157, 
                 "Invalid proplist flag %d\n"), tmpPropList.flags);

	free(nBuffer);
	free(vBuffer);
	free(tagPropList);
	*tagPropList = NULL;
	return INVALID;
	break;
      }

#endif /* OLD_ODS */

    return SUCCESS;
} /* end p2_create_tag_prop_head */


/*
 * Function Name: p2_create_tag_prop_data (3.3.28)
 *
 * Description
 *  For pass2/pass3 with new ODS create a prop list data for this tag.
 *
 * Input parameters:
 *  pdata: A pointer to a data buffer, used to load values into tagPropList. 
 *  bCnt:  Number of bytes in pdata.
 *
 * Output parameters:
 *  tagPropList: A pointer to a malloced structure. 
 *
 * Returns:
 *  Status value - SUCCESS, NO_MEMORY.
 */
int p2_create_tag_prop_data (tagPropListT    **tagPropList,
			     bsPropListPageT *pdata,
			     int             bCnt)
{
    char            *funcName = "p2_create_tag_prop_data";
    int		    pl_num;
    int		    pl_seg;
    bsPropListPageT tmpPropList;
    char            *buffer;
    unsigned int    bufSize;

    /*
     * This function should only be called for version 4
     * of the ODS proplists.
     */
#ifndef OLD_ODS

    /*
     * size of buffer, minus header.
     */
    bufSize = bCnt - NUM_SEG_SIZE;

    /*
     * WARNING: you MUST copy pdata into a local variable.
     * The reason for this is pdata is not 'aligned' on long
     * word boundaries due to the way we read it from disk.
     */
    memcpy(&tmpPropList, pdata, sizeof(bsPropListPageT));

    pl_num = tmpPropList.pl_num;
    pl_seg = tmpPropList.pl_seg;

    /* 
     * Create a tagPropListT structure
     */
    *tagPropList = (tagPropListT *) salvage_malloc(sizeof(tagPropListT));
    if (NULL == *tagPropList) 
    {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	return NO_MEMORY;
    }

    /* 
     * Create a buffer for the valBuffer 
     */
    buffer = (char *) salvage_malloc(bufSize + 1);
    if (NULL == buffer) 
    {
	writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_153, 
                 "malloc() failed\n"));
	free(tagPropList);
	return NO_MEMORY;
    }

    memcpy(buffer, pdata->buffer, bufSize);

    (*tagPropList)->plType                      = PL_DATA;
    (*tagPropList)->pl_num                      = pl_num;
    (*tagPropList)->pl_union.pl_data.buffer     = buffer;
    (*tagPropList)->pl_union.pl_data.bufferSize = bufSize;
    (*tagPropList)->pl_union.pl_data.pl_seg     = pl_seg;
    (*tagPropList)->next                        = NULL;

#endif /* OLD_ODS */
    return SUCCESS;
} /* end p2_create_tag_prop_data */



/*
 * Function Name: p2_create_tag_prop_full (3.3.29)
 *
 * Description: This is used to recombine the different internal
 *  proplists strutures into the correct format. This only works for
 *  ODS V4.0 or greater.
 *
 * Input parameters:
 *  fileset: The fileset we are working on.
 *
 * Returns:
 *  Status value - SUCCESS
 */
int p2_create_tag_prop_full (filesetLLT  *fileset)
{
    char               *funcName = "p2_create_tag_prop_full";
    int                status;
    int                tagNum;
    int                counter;
    int                processed;
    int                plNum;
    int                tail;
    struct filesetTag  *tag;
    struct tagPropList *headProp;
    struct tagPropList *prevProp;
    struct tagPropList *tmpProp;
    struct tagPropList *prevTmpProp;
    struct tagPropList *newProp;
    
    if (fileset->domain->version < 4)
    {
        /*
	 * We only need to do this for version 4 (or greater)
	 * of AdvFS.
	 */
        return SUCCESS;
    }

    /*
     * Loop through all tags.
     */
    for (tagNum = 2; tagNum < fileset->tagArraySize; tagNum++)
    {
	/*
	 * Loop through all the tags in the tag array.
	 * Skipping those that don't interest us.
	 */
	tag = fileset->tagArray[tagNum];

	/*
	 * Does this tag exist.
	 */
	if ( tag == NULL || tag == IGNORE || tag == DEAD )
	{
	    continue;
	}

	/* 
	 * Do we have any proplist records for this tag
	 */
	if (NULL == tag->props)
	{
	    continue;
	}

	/*
	 * Step through the link list of proprecords.
	 */
	headProp  = tag->props;
   	prevProp  = NULL;
	     
	while (headProp != NULL)
	{
	    switch (headProp->plType)
	    {
	      case PL_COMPLETE:
	      case PL_INCOMPLETE:
		/*
		 * No need to handle these.
		 */
		break;

	      case PL_HEAD:
		/*
		 * Set to complete.  The data in head and complete
		 * are in the same format, just different names.
		 */
		headProp->plType = PL_COMPLETE;
		tail = headProp->pl_union.pl_head.valSize;

		/*
		 * Loop through rest of records counting number of 
		 * data records which belong to this proplist.
		 */
		counter   = 0;
		plNum     = headProp->pl_num;
		tmpProp   = headProp->next;

		while (tmpProp != NULL)
		{
		    if (tmpProp->pl_num == plNum)
		    {
			counter++;
		    }
		    tmpProp = tmpProp->next;
		}

		if (0 == counter)
		{
		    /*
		     * No data records exist for this head
		     */
		    headProp->plType = PL_INCOMPLETE;
		    break;
		}

		/*
		 * Loop through records to find the data records.
		 */
		processed   = 0;
		tmpProp     = headProp->next;
		prevTmpProp = headProp;

		while (tmpProp != NULL)
		{
		    if ((tmpProp->pl_num == plNum) &&
			(tmpProp->plType == PL_DATA) &&
			(tmpProp->pl_union.pl_data.pl_seg == processed + 1))
		    {
			/*
			 * Found the next in seq data record.
			 * Append its data to the current record.
			 */
			memcpy(&(headProp->pl_union.pl_complete.valBuffer[tail]), 
			       tmpProp->pl_union.pl_data.buffer, 
			       tmpProp->pl_union.pl_data.bufferSize);

			tail = tail + tmpProp->pl_union.pl_data.bufferSize;
			headProp->pl_union.pl_complete.tail = tail;

			/*
			 * Free the data record, fixing LL chain.
			 */
			prevTmpProp->next = tmpProp->next;
			free (tmpProp->pl_union.pl_data.buffer);
			free (tmpProp);
			tmpProp  = prevTmpProp->next;
			processed++;
		    }
		    else
		    {
		        /*
			 * Skip this one.
			 */
			prevTmpProp = tmpProp;
			tmpProp  = tmpProp->next;
		    }
		} /* END tmpProp while loop*/

		if (processed != counter)
		{
		    /* 
		     * Missing one or more entries
		     */
		    headProp->plType = PL_INCOMPLETE;
		}

		break;
	      case PL_DATA:
		/*
		 * Check if HEAD record exists, and how many data record.
		 */
		newProp = NULL;
		tmpProp = headProp->next;
		plNum   = headProp->pl_num;
		counter = 1;

		while (tmpProp != NULL)
		{
		    if (tmpProp->plType == PL_HEAD)
		    {
		        /*
			 * Found the PL HEAD.
			 */
		        newProp = tmpProp;
		    }
		    else if (tmpProp->pl_num == plNum)
		    {
		        /* 
			 * Keep track of number of DATA records 
			 */
			counter++;
		    }
		    tmpProp = tmpProp->next;
		}

		/*
		 * No PL Head found.
		 */
		if (NULL == newProp)
		{
		    headProp->plType = PL_INCOMPLETE;
		
		    /*
		     * Free data records attached to this pl_num
		     */
		    counter--;
		    tmpProp     = headProp->next;    
		    prevTmpProp = headProp;
		   
		    while (0 != counter) 
		    {
			if (tmpProp->plType == PL_DATA)
			{
			    /*
			     * Free the data record, fixing LL chain.
			     */
			    prevTmpProp->next = tmpProp->next;
			    free (tmpProp->pl_union.pl_data.buffer);
			    free (tmpProp);
			    tmpProp  = prevTmpProp->next;
			    counter--;
			}
			else
			{
			    prevTmpProp = tmpProp;
			    tmpProp  = tmpProp->next;
			}
		    } /* End of freeing records. */
		    break;
		} /* end of no head */

		/*
		 * We have found a HEAD for this proplist, so sort them 
		 * load the buffer.
		 */
		newProp->plType  = PL_COMPLETE;
		tail             = newProp->pl_union.pl_head.valSize;
		tmpProp          = headProp;
		prevTmpProp      = prevProp;
		processed        = 0;

		while (tmpProp != NULL)
		{
		    /*
		     * Find the next data record of this proplist.
		     */
		    if ((tmpProp->pl_num == plNum) &&
			(tmpProp->plType == PL_DATA) &&
			(tmpProp->pl_union.pl_data.pl_seg == processed + 1))
		    {
			/*
			 * Found the next in seq data record.
			 * Append its data to the current record.
			 */
			memcpy(&(newProp->pl_union.pl_complete.valBuffer[tail]), 
			       tmpProp->pl_union.pl_data.buffer, 
			       tmpProp->pl_union.pl_data.bufferSize);

			tail = tail + tmpProp->pl_union.pl_data.bufferSize;
			newProp->pl_union.pl_complete.tail = tail;

			/*
			 * Free the data record, fixing LL chain.
			 */
			if (prevTmpProp == NULL)
			{
			    tag->props = tmpProp->next;
			    free (tmpProp->pl_union.pl_data.buffer);
			    free (tmpProp);
			    tmpProp = tag->props;
			}
			else
			{
			    prevTmpProp->next = tmpProp->next;
			    free (tmpProp->pl_union.pl_data.buffer);
			    free (tmpProp);
			    tmpProp = prevTmpProp->next;
			}
			processed++;
		    }
		    else
		    {
		        /*
			 * Skip this one.
			 */
			prevTmpProp = tmpProp;
			tmpProp  = tmpProp->next;
		    }
		    tmpProp = tmpProp->next;
		}

		if (processed != counter)
		{
		    /* 
		     * Missing one or more entries
		     */
		    newProp->plType = PL_INCOMPLETE;
		}

		break;
	    } /* End switch */

	    headProp = headProp->next;
	    prevProp = headProp;

	} /* End while loop of headProp */
    } /* End for loop of all tags */
    return SUCCESS;

} /* end p2_create_tag_prop_full */


/*
 * Function Name: find_last_rbmt_extent (3.3.30)
 *
 * Description:
 *
 *   The RBMT follows a unique way of identifing itself.  The last
 *   mcell (27) on each RBMT page, describes the next RBMT page.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  volume: The volume we are working on.
 *  BMTbuffer: A pointer to the BMT buffer. 
 *
 * Output parameters:
 *  lastExtent: Returns the number of rbmt extents. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, or PARTIAL  
 */
int find_last_rbmt_extent(domainInfoT  *domain,
			  volMapT      *volume,
			  unsigned int *lastExtent,
			  bsMPgT       *BMTbuffer)
{
    char     *funcName = "find_last_rbmt_extent";
    int      status;
    int      numExtents;
    int      lastCell = BSPG_CELLS - 1;   /* Currently 27 */
    int      fd;
    LBNT     lbn;
    long     bytesRead;
    bsMPgT   buffer;
    bsMCT    *pMcell;
    bsMRT    *pRecord;
    bsXtraXtntRT *pXtraXtnt;

    fd = volume->volFd;

    /* 
     * RBMT by default has 1 extent 
     */
    numExtents = 1;
    *lastExtent = numExtents;

    /*
     * Check last mcell on RBMT0 to see if it points to the next page.
     */
    pMcell = &(BMTbuffer->bsMCA[lastCell]);

    /*
     * Follow NEXT until last page of extents.
     */
    while (NULL != pMcell) 
    {
        mcellT nextChain;
	nextChain.cell = 0;

	/*
	 * Find the correct record.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	/*
	 * Now loop through records.
	 */
	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	{
	    if (BSR_XTRA_XTNTS == pRecord->type) 
	    {
		pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		nextChain.vol  = pMcell->nextVdIndex; 
		nextChain.page = pMcell->nextMCId.page;
		nextChain.cell = pMcell->nextMCId.cell;
		lbn = pXtraXtnt->bsXA[0].vdBlk;

		numExtents += 1;
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int)));
	} /* end pRecord while */

	if (0 != nextChain.cell)
	{
	    /*
	     * lbn is set above.
	     */

	    /*
	     * Load the new buffer if it exists.
	     */
	    status = read_page_by_lbn(fd, &buffer, lbn, &bytesRead);
	    if ((SUCCESS != status) || (bytesRead != PAGESIZE))
	    {
		*lastExtent = numExtents;
		return PARTIAL;
	    }

	    pMcell = &(buffer.bsMCA[lastCell]);
	}
	else 
	{
	    pMcell = NULL;
	}
    } /* end pMcell while */

    *lastExtent = numExtents;
    return SUCCESS;
} /* end find_last_rbmt_extent */


/*
 * Function Name: load_rbmt_lbn_array (3.3.31)
 *
 * Description: 
 *  Given a BMT page 0 buffer and the number of rbmt extents, 
 *  create and load the array with RBMT extent info.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  volume: The volume we are working on.
 *  BMTbuffer: A pointer to the BMT page 0 buffer. 
 *
 * Returns:
 *  Status value - SUCCESS or PARTIAL. 
 */
int load_rbmt_lbn_array(domainInfoT *domain,
			volMapT     *volume,
			bsMPgT      *BMTbuffer)
{
    char     *funcName = "load_rbmt_lbn_array";
    int      status;
    int      counter;
    int      lastCell = BSPG_CELLS - 1;    /* Currently 27 */
    int      extent = 0;
    int      fd;
    int      lastExtent;
    LBNT     lbn;
    long     bytesRead;
    mcellT   nextChain;
    bsMPgT   buffer;
    bsMCT    *pMcell;
    bsMRT    *pRecord;
    bsXtraXtntRT  *pXtraXtnt;
    extentT  *rbmt2Lbn;

    rbmt2Lbn   = volume->rbmtArray;
    lastExtent = volume->rbmtArraySize;
    fd         = volume->volFd;

    nextChain.vol  = 0;
    nextChain.page = 0;
    nextChain.cell = 0;

    /*
     * Fill in entries for RBMT extent 0, these are hard coded to always 
     * be page 0 and LBN 32.
     */
    rbmt2Lbn[extent].page = 0; 
    rbmt2Lbn[extent].lbn  = MSFS_RESERVED_BLKS;             /* 32 */
    extent++;

    /*
     * If first record type is corrupted we need to make sure that we have
     * saved the size of the extent.  To do this we preload the extent with
     * a default value.
     */
    rbmt2Lbn[extent].page = 1;   
    rbmt2Lbn[extent].lbn  = XTNT_TERM;   

    if (domain->version < 4)
    {
        return SUCCESS;
    }

#ifndef OLD_ODS
    /*
     * Handle the extents in BMT cell 27, these are NOT hard coded
     */

    pMcell  = &(BMTbuffer->bsMCA[lastCell]);
    pRecord = (bsMRT *)pMcell->bsMR0;

    while (NULL != pMcell)
    {
	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))))
	{
	    if (pRecord->type == BSR_XTRA_XTNTS) 
	    {
		pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		nextChain.vol  = pMcell->nextVdIndex;
		nextChain.page = pMcell->nextMCId.page;
		nextChain.cell = pMcell->nextMCId.cell;
		lbn = pXtraXtnt->bsXA[0].vdBlk;

		for (counter = 0; 
		     counter < pXtraXtnt->xCnt && counter < BMT_XTRA_XTNTS;
		     counter++) 
		{
		    rbmt2Lbn[extent].page = 
		      pXtraXtnt->bsXA[counter].bsPage;
		    
		    /*
		     * As this is stored as a unsigned long, not a signed
		     * long we need to handle it this way.
		     */
		    if (pXtraXtnt->bsXA[counter].vdBlk != XTNT_TERM &&
			pXtraXtnt->bsXA[counter].vdBlk != PERM_HOLE_START)
		    {
			rbmt2Lbn[extent].lbn = 
			  pXtraXtnt->bsXA[counter].vdBlk;
			extent++;
		    } 
		    else 
		    {
			rbmt2Lbn[extent].lbn =  XTNT_TERM;
		    }
		}
	    } 	    
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* End pRecord While loop */

	if ((0 != nextChain.cell) || (0 != nextChain.vol) || (0 != nextChain.page))
	{
	    if (nextChain.cell != lastCell) {
		writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_268, 
                "RBMT next cell points to %d instead of the last mcell.  Using last mcell instead.\n"), nextChain.cell); 
	    }

	    /*
	     * We're always staying on the same volume, so there's no
	     * reason to check nextChain.vol.  lbn is set above.
	     */

	    /*
	     * Load the new buffer if it exists.
	     */
	    status = read_page_by_lbn(fd, &buffer, lbn, &bytesRead);
	    if ((SUCCESS != status) || (bytesRead != PAGESIZE))
	    {
		return PARTIAL;
	    }
	    
	    pMcell = &(buffer.bsMCA[lastCell]);
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    
	    /*
	     * Set to nextChain to 0.
	     */
	    nextChain.vol  = 0;
	    nextChain.page = 0;
	    nextChain.cell = 0;
	}
	else 
	{
	    pMcell = NULL;
	}

    } /* end pMcell While loop */
#endif
    return SUCCESS;
} /* end load_rbmt_lbn_array */

/*
 * Function Name: setup_tar (3.3.x)
 *
 * Description
 *  Sets all the necessary global variables for writing a tar format
 *  archive and calls open_archive to ensure that the specified output
 *  device is available.
 *
 *  A large portion of this code is taken straight from ./usr/bin/pax.
 *
 *  Note:
 *     Most of the variables referenced in this routine are defined in
 *     pax.h.
 *
 * Input parameters:
 *  None.
 *
 * Side Effects:
 *  This routine can exit salvage if either open_archive or buf_allocate
 *  fail.
 */
void setup_tar (void)
{
    char	*funcName = "setup_tar";
    char	*buf_ptr;
    int		status;

    setlocale(LC_ALL, "");
    catd = catopen(MF_PAX, NL_CAT_LOCALE);

    loc_rec_form = nl_langinfo(_M_D_RECENT);
    if (loc_rec_form == NULL || !*loc_rec_form)
        loc_rec_form = rec_form_posix;

    loc_old_form = nl_langinfo(_M_D_OLD);
    if (loc_old_form == NULL || !*loc_old_form)
        loc_old_form = old_form_posix;

    /*
     * myname is set to tar to fool pax code which assumes that the only
     * valid values for myname are "tar", "pax", and "cpio".
     */
    myname = "tar";

    uidMax = getuidmax();
    gidMax = uidMax;
    mask = umask(0);

    uid = getuid();
    gid = getgid();
    now = time((time_t *) 0);

    /* open terminal for interactive queries */
    ttyf = open_tty();

    ar_file = Options.archiveName;

    ar_format = XPAX;
    ar_interface = TAR;
    blocking = DEF_BLOCKING;
    blocksize = blocking * BLOCKSIZE;
    f_raise_sig = 0;
    f_binary = 0;
    f_eommsg = 0;
    f_extended_header = 0;
    f_extract = 0;
    f_list = 0;
    f_unconditional = 0;
    xflag = 0;
    Sflag = 0;
    rptape = 0;
    rplhead = NULL;

    f_except = 1;
    f_chdir = 1;
    f_autosize = 1;
    f_mtime = 1;
    f_dir_create = 1;
    msgfile = stderr;
    f_follow_links = 0;
    f_omode = 0;
    f_newer = 0;
    f_eat_eof = 0;
    f_create = 1;
    f_extract_access_time = 1;
    f_owner = 1;
    f_mode = 1;
    f_noattrbs = 0;

    /*
     * The buf_allocate routine will not call the salvage_malloc
     * routine.  So, to give the user a chance to recover from a
     * "nearly out of memory" condition before failing, try to
     * malloc the needed space before calling the buf_allocate
     * routine.  Promptly free the memory so that buf_allocate
     * can malloc it.
     */
    buf_ptr = (char *) salvage_malloc(blocksize + 1);
    if (NULL != buf_ptr) {
	free(buf_ptr);
    }

    /*
     *
     */
    buf_allocate((OFFSET) blocksize);

    status = open_archive(AR_WRITE);
    if (0 == status) {
	return;
    }

    writemsg(SV_ERR,
	     catgets(_m_catd, S_SALVAGE_1, SALVAGE_270,
		     "Unable to create archive file.  Exiting.\n")); 
    exit(EXIT_FAILED);

} /* end setup_tar */

/* end salvage_init.c */
