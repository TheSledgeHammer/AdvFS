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
 *      Mon Feb 14 10:28:41 PST 2000
 */
/*
 * HISTORY
 * 
 */
#pragma ident "@(#)$RCSfile: fixfdmn_rbmt.c,v $ $Revision: 1.1.41.9 $ (DEC) $Date: 2006/06/19 11:53:49 $"

#include "fixfdmn.h"

/*
 * Global
 */
extern nl_catd _m_catd;


/*
 * Function Name: correct_domain_rbmt (3.21)
 *
 * Description: This is used to check and fix the BMT0/RBMT.
 *
 * Input parameters:
 *     domain: The domain being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, FAILURE or NO_MEMORY
 */
int correct_domain_rbmt(domainT *domain)
{
    char   *funcName ="correct_domain_rbmt";
    char   errorMsg[MAXERRMSG];
    int    status;
    int    fixedPages;      /* Flag to see if fixes have been done */
    int    foundVolCorrupt; /* Used to locate volume corruption */
    int    mountStatus;     /* The volumes mount status */
    int    volNum;          /* Counter for the volume loop */
    pageT  *pPage;
    bsMPgT *pBMTPage;       /* The current RBMT page we are working on */
    bsMCT  *pMcell;         /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;        /* The Current mcell record we are working on */
    bsVdAttrT  *pVdAttr;    /* Pointer to the BSR_VD_ATTR record */
    bsDmnAttrT *pDmnAttr;   /* Pointer to the BSR_DMN_ATTR record */
    struct timeval mountId;
    
    /*
     * Init variables.
     */
    fixedPages = FALSE;

    /*	
     * Loop on all entries in domain->volumes[]:
     */
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (-1 == domain->volumes[volNum].volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}
	/*
	 * read RBMT page 0.
	 */
	status = read_page_from_cache(volNum, RBMT_PAGE0, &pPage);
	if (SUCCESS != status) {
	    return status;
	}
      
	status = validate_bmt_page(domain, pPage, 0, TRUE);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_487, 
			     "RBMT/BMT0 on volume %s is too corrupt to fix.\n"), 
		     domain->volumes[volNum].volName);
	    corrupt_exit(NULL);
	}

	status = release_page_to_cache(volNum, RBMT_PAGE0, pPage,
				       FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
    } /* end volume loop */

    /*
     * Now that fixfdmn has a list of volume's metadata, 
     * synchronize the metadata across the volumes.
     */

    /*
     * Loop through all the volumes checking domain version.
     */
    foundVolCorrupt = FALSE;
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (-1 != domain->volumes[volNum].volFD) {
	    if (domain->dmnVersion != domain->volumes[volNum].dmnVersion) {
	        foundVolCorrupt = TRUE;
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_488,
				 "The domain version is corrupt on volume %s.\n"),
			 domain->volumes[volNum].volName);
	    }
	}
    } /* end volume loop */

    if (TRUE == foundVolCorrupt) {
        char errorMsg[MAXERRMSG];
	char answer[10];  /* buffer for user input */
	int  size;        /* buffer size */
	int  dmnVersion;  /* The version the user wishes to change to */

	/*
	 * Init variables.
	 */
	size = 10; 
	dmnVersion = 0;

	writemsg (SV_ERR | SV_LOG_FOUND, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_489, 
			  "The domain versions of the volumes do not match.\n"));
	writemsg (SV_ERR | SV_CONT, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_490, 
			  "Do you want to correct the domain version? (y/[n])  "));

	status = prompt_user_yes_no(answer, "n");
	if (SUCCESS != status) {
	    return status;
	}


	if ('y' != answer[0]) {
	    /*
	     * User does not want to continue.
	     */
	    sprintf(errorMsg, 
		    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_491, 
			    "Verify the correct volumes are in the domain.\n"));
	    corrupt_exit(errorMsg);
	} else {
	    /*
	     * Loop until valid domain version is entered
	     */
	    while (0 == dmnVersion) {
	        /*		
		 * Found corruption on at least one volume, print a message 
		 * to the user asking them which domain version should be used.
		 */
	        writemsg(SV_ERR | SV_CONT, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_771, 
				 "Please enter the domain version to use (3 or 4):  "));
		status = prompt_user(answer, size);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * convert string to int.
		 */
		dmnVersion = atoi(answer);

		if ((dmnVersion < 3) || (dmnVersion > 4)) {
		    writemsg (SV_ERR | SV_CONT, 
			      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_493,
				      "%d is an invalid domain version.\n"),
			      dmnVersion);
		    dmnVersion = 0;
		}
	    } /* end while loop */
	} /* end prompt response */

	/* 	
	 * Loop through all the volumes
	 */
	for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	    if (-1 == domain->volumes[volNum].volFD) {
		/*
		 * Not a valid volume, continue on to next volume
		 */
		continue;
	    }
	    if (dmnVersion != domain->volumes[volNum].dmnVersion) {
		domain->volumes[volNum].dmnVersion = dmnVersion;
		/* 
		 * Read this volume's RBMT page 0.
		 */
		status = read_page_from_cache(volNum, RBMT_PAGE0, &pPage);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * Modify page so it has the correct version.
		 */
		pBMTPage = (bsMPgT *)pPage->pageBuf;
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_494, 
						   "Modified domain version from %d to %d.\n"),
					   pBMTPage->megaVersion, 
					   dmnVersion);
		if (SUCCESS != status) {
		    return status;
		}

		pBMTPage->megaVersion = dmnVersion;
		
		/*
		 * Release the modified page.
		 */
		status = release_page_to_cache(volNum, RBMT_PAGE0, 
					       pPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		fixedPages = TRUE;
	    } /* end if domain version don't match */
	} /* end volume loop */
    } /* end domain Version Fix */

    /*
     * Call correct_rbmt_pages to check/fix rbmt metadata.
     */
    status = correct_rbmt_pages(domain);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    fixedPages = TRUE;
	} else {
	    return status;
	}
    }

    /*
     * Loop through all the volumes checking domain ids.
     */
    foundVolCorrupt = FALSE;
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (-1 == domain->volumes[volNum].volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}
	    
	if (domain->dmnId.tv_sec != domain->volumes[volNum].domainID.tv_sec ||
	    domain->dmnId.tv_usec != domain->volumes[volNum].domainID.tv_usec)
	{
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_495,
			     "The domain ID is corrupt on volume %s.\n"),
		     domain->volumes[volNum].volName);
	    foundVolCorrupt = TRUE;
	}
    }

    if (TRUE == foundVolCorrupt) {
        /*	
	 * Found corruption on at least one volume.	
	 */
	char answer[10]; /* buffer for user input */
	int  size;       /* buffer size */
	int  dmnId;      /* The version the user wishes to change to */

	size = 10; 
	dmnId = 0; 

	writemsg (SV_ERR | SV_LOG_FOUND, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_496, 
			  "The domain IDs of all the volumes do not match.\n"));
	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_497, 
			  "It is possible that you have a volume from a different\n"));

	writemsg (SV_ERR | SV_LOG_FOUND | SV_CONT, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_498, 
			  "domain listed as part of this domain.\n"));

	/*		
	 * Found corruption on at least one volume, print a message to the 
	 * user asking them if they wish to reset the ids.  They might have
	 * an incorrect volume mounted, which is why we ask.
	 */
	writemsg (SV_ERR | SV_CONT, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_499, 
			  "Set all volumes to the same domain ID? (y/[n])  "));

	status = prompt_user_yes_no(answer, "n");
	if (SUCCESS != status) {
	    return status;
	}

	if ('n' == answer[0]) {
	    /*
	     * User does not want to continue.
	     */
	    sprintf(errorMsg, 
		    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_491, 
			    "Verify the correct volumes are in the domain.\n"));
	    corrupt_exit(errorMsg);
	} 

	/* 	
	 * Loop through all the volumes
	 */
	for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	    if (-1 == domain->volumes[volNum].volFD) {
		/*
		 * Not a valid volume, continue on to next volume
		 */
		continue;
	    }
	    if ((domain->dmnId.tv_sec != domain->volumes[volNum].domainID.tv_sec) ||
		(domain->dmnId.tv_usec != domain->volumes[volNum].domainID.tv_usec)) {
		int foundFixed;

		foundFixed = FALSE;
		domain->volumes[volNum].domainID.tv_sec = 
		    domain->dmnId.tv_sec;
		domain->volumes[volNum].domainID.tv_usec = 
		    domain->dmnId.tv_usec;
		/* 
		 * Read this volumes RBMT page 0.
		 */
		status = read_page_from_cache(volNum, RBMT_PAGE0, &pPage);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * Modify page so it has the correct domain Id.
		 */
		pBMTPage = (bsMPgT *)pPage->pageBuf;
		    
		/*
		 * Locate the correct place depending on domain version.
		 */
		if (3 == domain->volumes[volNum].dmnVersion) {
		    pMcell = &(pBMTPage->bsMCA[4]);
		} else if (4 == domain->volumes[volNum].dmnVersion) {
		    pMcell = &(pBMTPage->bsMCA[6]);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_500,
				     "Internal Error: Invalid domain version on volume %s.\n"),
			     domain->volumes[volNum].volName);
		    failed_exit();
		}

		pRecord = (bsMRT *)pMcell->bsMR0;
		/*
		 * Now loop through all records in this mcell.
		 */
		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		    if (BSR_DMN_ATTR == pRecord->type) {
			pDmnAttr = (bsDmnAttrT *)((char *)pRecord + sizeof(bsMRT));
			writemsg(SV_VERBOSE | SV_LOG_FOUND, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_501, 
					 "Domain ID on volume %s is incorrect.\n"),
				 domain->volumes[volNum].volName);

			status = add_fixed_message(&(pPage->messages), 
						   RBMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_502, 
							   "Modified domain ID from %x.%x to %x.%x.\n"),
						   pDmnAttr->bfDomainId.tv_sec, 
						   pDmnAttr->bfDomainId.tv_usec,
						   domain->dmnId.tv_sec,
						   domain->dmnId.tv_usec);
			if (SUCCESS != status) {
			    return status;
			}
			pDmnAttr->bfDomainId.tv_sec = domain->dmnId.tv_sec;
			pDmnAttr->bfDomainId.tv_usec = domain->dmnId.tv_usec;
			foundFixed = TRUE;
		    }
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, 
						 sizeof(int))); 
		} /* end record loop */
		
		if (FALSE == foundFixed) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_503,
				     "Internal Error - Can't fix domain ID on %s.\n"),
			     domain->volumes[volNum].volName);
		    failed_exit();
		}
		
		/*
		 * Release the modified page.
		 */
		status = release_page_to_cache(volNum, RBMT_PAGE0, 
					       pPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		fixedPages = TRUE;
	    } /* end if domain Ids don't match */
	} /* end volume loop */
    } /* end if found volume corruption */

    /*	
     *	 Loop through all the volumes checking mount status.
     */
    foundVolCorrupt = FALSE;
    mountStatus     = -1;
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (-1 == domain->volumes[volNum].volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}
	if (mountStatus != domain->volumes[volNum].mntStatus) {
	    if (-1 == mountStatus) {
		/*
		 *	First Volume found, uses its value.
		 */
		mountStatus = domain->volumes[volNum].mntStatus;
	    } else {
		writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_504,
				 "Volume %s's mntStatus is %d expecting %d.\n"),
			 domain->volumes[volNum].volName, 
			 domain->volumes[volNum].mntStatus, mountStatus);
		foundVolCorrupt = TRUE;
	    }
	}
    }

    if (TRUE == foundVolCorrupt) {
        /*	
	 * Found corruption on at least one volume.	
	 */

	/* 	
	 * Loop through all the volumes
	 */
	for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	    if (-1 == domain->volumes[volNum].volFD) {
		/*
		 * Not a valid volume, continue on to next volume
		 */
		continue;
	    }
	    if (mountStatus != domain->volumes[volNum].mntStatus) {
		int foundFixed;

		foundFixed = FALSE;
		domain->volumes[volNum].mntStatus = mountStatus;

		/* 
		 * Read this volumes RBMT/BMT page 0.
		 */
		status = read_page_from_cache(volNum, RBMT_PAGE0, &pPage);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * Modify page so it has the correct mount status.
		 */
		pBMTPage = (bsMPgT *)pPage->pageBuf;
		    
		if (3 == domain->volumes[volNum].dmnVersion) {
		    pMcell = &(pBMTPage->bsMCA[4]);
		} else if (4 == domain->volumes[volNum].dmnVersion) {
		    pMcell = &(pBMTPage->bsMCA[6]);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_500,
				     "Internal Error: Invalid domain version on volume %s.\n"),
			     domain->volumes[volNum].volName);
		    failed_exit();
		}

		pRecord = (bsMRT *)pMcell->bsMR0;
		/*
		 * Now loop through all records in this mcell.
		 */
		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		    if (BSR_VD_ATTR == pRecord->type) {
			pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_505, 
							   "Modified volume mount status from %d to %d.\n"),
						   pVdAttr->state,
						   mountStatus);
			if (SUCCESS != status) {
			    return status;
			}
			pVdAttr->state = mountStatus;
			foundFixed = TRUE;
		    }
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt,
						 sizeof(int))); 
		} /* end while record loop */

		assert(TRUE == foundFixed);
		
		/*
		 * Release the modified page.
		 */
		status = release_page_to_cache(volNum, RBMT_PAGE0, 
					       pPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		fixedPages = TRUE;
	    } /* end if mount status corrupt */
	} /* end volume loop */
    } /* end if found corruption */


    /*
     * Loop through all the volumes mount Id.
     * It doesn't matter what the value is as long as they are the same
     * so we are just going to pick the first one we find and set all
     * of them to that value.
     */
    foundVolCorrupt = FALSE;
    mountId.tv_sec = -1;
    mountId.tv_usec = -1;
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (-1 == domain->volumes[volNum].volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}

	if ((mountId.tv_sec != domain->volumes[volNum].mountId.tv_sec) ||
	    (mountId.tv_usec != domain->volumes[volNum].mountId.tv_usec)) {
	    if (-1 == mountId.tv_sec) {
		/*
		 *	First Volume found, uses its value.
		 */
		mountId.tv_sec  = domain->volumes[volNum].mountId.tv_sec;
		mountId.tv_usec = domain->volumes[volNum].mountId.tv_usec;
	    } else {
		writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_506,
				 "Volume %s's mntId is %x.%x expecting %x.%x.\n"),
			 domain->volumes[volNum].volName, 
			 domain->volumes[volNum].mountId.tv_sec, 
			 domain->volumes[volNum].mountId.tv_usec,
			 mountId.tv_sec,mountId.tv_usec);
		foundVolCorrupt = TRUE;
	    }
	} 
    } /* end volume loop */

    if (TRUE == foundVolCorrupt) {
        /*	
	 * Found corruption on at least one volume.	
	 */

	/* 	
	 * Loop through all the volumes
	 */
	for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	    if (-1 == domain->volumes[volNum].volFD) {
		/*
		 * Not a valid volume, continue on to next volume
		 */
		continue;
	    }

	    if ((mountId.tv_sec != domain->volumes[volNum].mountId.tv_sec) ||
		(mountId.tv_usec != domain->volumes[volNum].mountId.tv_usec)) {
		int foundFixed;

		foundFixed = FALSE;
		domain->volumes[volNum].mountId.tv_sec = mountId.tv_sec; 
		domain->volumes[volNum].mountId.tv_usec = mountId.tv_usec;

		/* 
		 * Read this volumes RBMT/BMT page 0.
		 */
		status = read_page_from_cache(volNum, RBMT_PAGE0, &pPage);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * Modify page so it has the correct mount status.
		 */
		pBMTPage = (bsMPgT *)pPage->pageBuf;
		    
		if (3 == domain->volumes[volNum].dmnVersion) {
		    pMcell = &(pBMTPage->bsMCA[4]);
		} else if (4 == domain->volumes[volNum].dmnVersion) {
		    pMcell = &(pBMTPage->bsMCA[6]);
		} else {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_500,
				     "Internal Error: Invalid domain version on volume %s.\n"),
			     domain->volumes[volNum].volName);
		    failed_exit();
		}

		pRecord = (bsMRT *)pMcell->bsMR0;
		/*
		 * Now loop through all records in this mcell.
		 */
		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		    if (BSR_VD_ATTR == pRecord->type) {
			pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_507, 
							   "Modified volume mntId from %x.%x to %x.%x.\n"),
						   pVdAttr->vdMntId.tv_sec,
 						   pVdAttr->vdMntId.tv_usec,
						   mountId.tv_sec,
						   mountId.tv_usec);
			if (SUCCESS != status) {
			    return status;
			}
			pVdAttr->vdMntId.tv_sec = mountId.tv_sec;
			pVdAttr->vdMntId.tv_usec = mountId.tv_usec;
			foundFixed = TRUE;
		    }
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt,
						 sizeof(int))); 
		} /* end while record loop */

		assert(TRUE == foundFixed);
		
		/*
		 * Release the modified page.
		 */
		status = release_page_to_cache(volNum, RBMT_PAGE0, 
					       pPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		fixedPages = TRUE;
	    } /* end if mount status corrupt */
	} /* end volume loop */
    } /* end if found corruption */

    if (TRUE == fixedPages) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_domain_rbmt*/



/*
 * Function Name: correct_rbmt_pages (3.22)
 *
 * Description: This function will check and fix RBMT/BMT0 pages. The
 *              RBMT (or BMT0) will have certain mcell record types
 *              that must be in known locations. The tool will use
 *              that information to perform detailed checks and fixes
 *              of the RBMT.
 *
 * Input parameters:
 *     domain: The domain to perform the check and fixes on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE.
 */
int correct_rbmt_pages(domainT *domain)
{
    char    *funcName ="correct_rbmt_pages";
    int     status;
    LBNT    currPageLBN;
    LBNT    nextPageLBN;    /* LBN of next RBMT page to check */
    int     modified;       /* Flag to let us know if page has been modified */
    int     pageNumber;     /* The page number we are working on */
    int     isOdsV4;        /* Is this a RBMT page or BMT0 */
    int     freeCounter;    /* How many free mcells on page */
    int     rootTagVolume;  /* Which volume has the root Tag file */
    int     mattrVol;       /* Which volume has current mattr */
    int     mattrSeq;       /* Current sequence number of mattr */
    int     mattrLogVolume; /* The volume which has the log */
    int     bfSetDirVol = 0;
    int     volNum;         /* Loop counters */
    int     page;           /* Loop counters */
    int     mcell;          /* Loop counters */
    int     extent;         /* Loop counters */
    int     priorMcell;     /* Previous mcell */
    int     found;
    int     numExtents;
    int     cnt;
    int	    mcellCnt[6];    /* Used for mcellCnt of mcell 0-5 */
    char    errorMsg[MAXERRMSG];
    pageT   *pPage;         /* The current cache page we are working on */
    bsMPgT  *pBMTPage;      /* The current RBMT page we are working on */
    bsMCT   *pMcell;        /* The current RBMT mcell we are working on */
    bsMRT   *pRecord;       /* The Current mcell record we are working on */
    volumeT *volume;        /* Short cut to access the volume */
    logVolT *logVolume;     /* link list of volumes which have log extents. */
    logVolT *headLogVolume; /* Head of link list */
    logVolT *tmpLogVolume;  /* pointer to link list */
    mcellUsage  freeArray[BSPG_CELLS];  /* which mcells are free */
    bsDmnMAttrT *pDmnMAttr;
    bsXtntRT	*pXtnt;
    pagetoLBNT  *pExtents; 
    int     lastValidMcell; /* 27 (LAST_MCELL) for ODS v3, 26 for ODS v4 */

    /*
     * Initialize once per domain.
     */
    logVolume = NULL;
    mattrVol  = 0;
    mattrSeq  = 0;

    /*
     * Loop through all the volumes.
     */
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (-1 == domain->volumes[volNum].volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}
	currPageLBN = RBMT_PAGE0;
	  
	/* 
	 * Read this volumes RBMT page 0.
	 */
	status = read_page_from_cache(volNum, currPageLBN, &pPage);
	if (SUCCESS != status) {
	    return status;
	}
	  
	/*	
	 * Initialize once per volume
	 */
	modified      = FALSE;
	pageNumber    = 0;
	rootTagVolume = FALSE;

	for (cnt = 0; cnt < 6; cnt++) {
	    mcellCnt[cnt] = 0;
	}
	  
	pBMTPage = (bsMPgT *)pPage->pageBuf;
	
	if (4 == pBMTPage->megaVersion) {
	    isOdsV4 = TRUE;
	    lastValidMcell = LAST_MCELL - 1;
	} else {
	    isOdsV4 = FALSE;
	    lastValidMcell = LAST_MCELL;
	}
	
	while (NULL != pPage) {
	    /*
	     * Initialize nextPageLBN to default value.
	     */
	    nextPageLBN = (LBNT) -1;
	    
	    /*
	     * Fix the page header
	     */
	    status = correct_bmt_page_header(pageNumber,
					     volNum,
					     FALSE,/* Single page extent */
					     -1,   /* Single page extent */
					     pPage,
					     domain,
					     TRUE); /* rbmtFlag */
	    if (SUCCESS != status) {
		if (FIXED == status) {
		    modified = TRUE;
		} else {
		    return status;
		}
	    }

	    /*
	     * The nextFreePg field is checked within correct_bmt_page_header,
	     * so it doesn't need to be checked here.
	     */

	    /*
	     * Check the rbmt extent chain.
	     */
	    if (TRUE == isOdsV4) {
		status = correct_rbmt_next_page_mcell(pPage, 
						      volNum,
						      domain,
						      pageNumber,
						      &nextPageLBN);
		if (SUCCESS != status) {
		    if (FIXED == status) {
			modified = TRUE;
		    } else {
			return status;
		    }
		}
		
		/*
		 * If an used RBMT mcell
		 */
                if ( ( (LBNT) -1 ) != nextPageLBN ) {
		    mcellCnt[0]++;
		}
	    }

	    if (TRUE == modified) {
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_517, 
						   "Modified RBMT/BMT0 page.\n"));
		if (SUCCESS != status) {
		    return status;
		}
	    }
	    
	    status = release_page_to_cache(volNum, currPageLBN, pPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;
	    
	    if ( ( (LBNT)-1 )!= nextPageLBN) {
		currPageLBN = nextPageLBN;
		status = read_page_from_cache(volNum, currPageLBN, &pPage);
		if (SUCCESS != status) {
		    return status;
		}
		pageNumber++;
		/*
		 * Need to reset pBMTPage each time through the loop
		 */
		pBMTPage = (bsMPgT *)pPage->pageBuf;
	    } else {
		pPage = NULL;
	    }
	} /* End loop of pages */

	/*
	 * Now load the extents for the RBMT, do not add them to the SBM.
	 */
	status = collect_rbmt_extents(domain, volNum, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	
	/*
	 * Now we need to loop through the pages a second time
	 */
	volume = &(domain->volumes[volNum]);
	pExtents = volume->volRbmt->rbmtLBN;
	numExtents = volume->volRbmt->rbmtLBNSize;
	pageNumber = 0;

	for (extent = 0; extent < numExtents; extent++) {
	    /*
	     * This is a special case extent array.  We know that
	     * each entry is for a single page.
	     */
	    currPageLBN = pExtents[extent].lbn;

	    status = read_page_from_cache(volNum, currPageLBN, &pPage);
	    if (SUCCESS != status) {
		return status;
	    }

	    pBMTPage = (bsMPgT *)pPage->pageBuf;

	    /*
	     * Certain mcells on page 0 must be of a set type.
	     * So we will check them here.
	     */
	     if (0 == pageNumber) {
		/* Check mcell 0 - BMT */
		status = correct_rbmt_bmt_mcell(pPage,
					    isOdsV4,
					    volNum);
		mcellCnt[0]++;
		if (SUCCESS != status) {
               		if (FIXED == status) {
                       		modified = TRUE;
                    	} else {
                        	return status;
                    	}
		}

	   	/* Check vd attr out out order. Check vol size before SBM. */
            	/*Check BMT0 mcell 4 or 6 - Vol/Dmn Attributes */
	
		status = correct_rbmt_vol_dom_mcell(pPage, isOdsV4, volNum,
                                                    domain, &mattrVol,
                                                    &mattrSeq, &bfSetDirVol); 	 
		if (SUCCESS != status) {
                	if (FIXED == status) {
                       		modified = TRUE;
                    	} else {
                       		return status;
                        }
                }
	    
		/* Check mcell 1 - SBM */
		status =  correct_rbmt_sbm_mcell(pPage,
						 domain,
						 volNum);
		mcellCnt[1]++;
		if (SUCCESS != status) {
        		if (FIXED == status) {
                	        modified = TRUE;
			} else {
                       		return status;
                    	}
                }
 
		/* Check mcell 2 - Root Tag Dir */
		status = correct_rbmt_root_tag_mcell(pPage,
						     domain,
						     volNum,
						     &rootTagVolume);
		domain->volumes[volNum].rootTag =  rootTagVolume;
		mcellCnt[2]++;
		if (SUCCESS != status) {
                	if (FIXED == status) {
                        	modified = TRUE;
                    	} else {
                        	return status;
                    	}
                }
 
		/* Check mcell 3 - Log */
		status = correct_rbmt_log_mcell(pPage,
					        volNum,
						domain,
						&logVolume);
		mcellCnt[3]++;
		if (SUCCESS != status) {
                	if (FIXED == status) {
                        	modified = TRUE;
                    	} else {
                        	return status;
                    	}
                }
 
		if (TRUE == isOdsV4) {
			/* Check RBMT mcell 4 - BMT */
			status = correct_rbmt_primary_bmt(pPage, 
							  volNum);
			mcellCnt[4]++;
			if (SUCCESS != status) {
                        	if (FIXED == status) {
                            		modified = TRUE;
                        	} else {
                            		return status;
                        	}
                    	}

		} else {
			/* Vol/Dmn Attributes already checked */
		}
			    
		/* Check mcell 5 - Misc */
		status = correct_rbmt_misc_mcell(pPage,
		   			         volNum,
						 pageNumber,
						 domain);
		mcellCnt[5]++;
		if (SUCCESS != status) {
			if (FIXED == status) {
				modified = TRUE;
                        } else {
				return status;
			}
		}

		/* Check mcell 6 - RBMT */
		if (TRUE == isOdsV4) {
			/* Vol/Dmn Attributes checked above. */
		} else {
			mcell = 6;
			pMcell = &(pBMTPage->bsMCA[mcell]);
			status = correct_rbmt_other_mcell(pPage,
							  mcell,
							  volNum,
							  domain);
			if (pMcell->tag.num != 0) {
				uint idx = -pMcell->tag.num - volNum * 6;
				assert(idx < 6);
				mcellCnt[idx]++;
			}
			if (SUCCESS != status) {
                        	if (FIXED == status) {
					modified = TRUE;
                        	} else {
					return status;
                        	}
                    	}

		}
			    
		/* check mcell 27 */
		if (TRUE == isOdsV4) {
			/*
			 * Already checked
			 */
			status = SUCCESS;
		} else {
			mcell = 27;
                        pMcell = &(pBMTPage->bsMCA[mcell]);
			status = correct_rbmt_other_mcell(pPage,
							  mcell,
							  volNum,
							  domain);
			if (pMcell->tag.num != 0) {
				uint idx = -pMcell->tag.num - volNum * 6;
				assert(idx < 6);
				mcellCnt[idx]++;
			}
			if (SUCCESS != status) {
        	                if (FIXED == status) {
					modified = TRUE;
                        	} else {
					return status;
                        	}
			}
		}
			   
                for (mcell = 7; mcell < BSPG_CELLS-1; mcell++) {

			pMcell = &(pBMTPage->bsMCA[mcell]);
	 
			/*
			 * All other mcells on BMT0/RBMT are either
			 * BSR_XTRA_XTNTS  or are empty mcells.
			 */
			status = correct_rbmt_other_mcell(pPage,
							  mcell,
							  volNum,
							  domain);
			if (pMcell->tag.num != 0) {
				uint idx = -pMcell->tag.num - volNum * 6;
                                assert(idx < 6);
                                mcellCnt[idx]++;
			}
			    
			if (SUCCESS != status) {
				if (FIXED == status) {
			    		modified = TRUE;
				} else {
			    		return status;
				}
		    	}
	 	} /* End of mcell loop */	
	    } else {
		/* 
		 * Some page other than 0.  RBMT only
		 */

		for (mcell = 0; mcell < BSPG_CELLS-1; mcell++) {
			pMcell = &(pBMTPage->bsMCA[mcell]);
	 
			assert (TRUE == isOdsV4);
			status = correct_rbmt_other_mcell(pPage,
							  mcell,
							  volNum,
							  domain);
			if (pMcell->tag.num != 0) {
                       		uint idx = -pMcell->tag.num - volNum * 6;
                            	assert(idx < 6);
                            	mcellCnt[idx]++;
			}
			if (SUCCESS != status) {
				if (FIXED == status) {
					modified = TRUE;
				} else {
					return status;
				}
			}
	    	} /* End of mcell loop */
	    } /* end if page == 0 */  

	    /*
	     * Start check of free mcell list
	     */

	    /*
	     * Check of all the mcell headers for this RBMT/BMT0 page.
	     */
	    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
		/*
		 * point to the current BMT mcell.
		 */
		pMcell = &(pBMTPage->bsMCA[mcell]);
		
		if ((0 == pMcell->tag.num) &&
		    (0 == pMcell->bfSetTag.num)) {
		    /*
		     * mcell is empty 
		     */
		    freeArray[mcell] = FREE;
		    
		    /*
		     * linkSegment must be zero.
		     * nextVdIndex must be zero.
		     * nextMCId.cell must be less than 27 (LAST_MCELL)
		     *   on ODS v4, or <= 27 on ODS v3.
		     * nextMCId.page must equal the current page number,
		     *   unless this is the tail of the list (page 0 cell 0).
		     */
		    if ((0 != pMcell->linkSegment) ||
			(0 != pMcell->nextVdIndex) ||
			(pMcell->nextMCId.cell > lastValidMcell) ||
			((pageNumber != pMcell->nextMCId.page) &&
			 ((0 != pMcell->nextMCId.page) ||
			  (0 != pMcell->nextMCId.cell)))) {

			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_510,
					 "Mcell (%d,%d,%d) is corrupt.\n"),
				 volNum, pageNumber, mcell);
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_511,
							   "Modified mcell (%d,%d,%d) to be marked as empty.\n"),
						   volNum,
						   pageNumber,
						   mcell);
			if (SUCCESS != status) {
			    return status;
			}

			pMcell->nextVdIndex = 0;
			pMcell->nextMCId.page = 0;
			pMcell->nextMCId.cell = 0;
			pMcell->linkSegment = 0;
			modified = TRUE;
		    }
		} else {
		    /*
		     * Mcell is used.
		     */
		    freeArray[mcell] = USED;
		    
		    if ((0 != pMcell->nextVdIndex) &&
			(volNum != pMcell->nextVdIndex)) {
			if ((0 == pMcell->nextMCId.page) &&
			    (0 == pMcell->nextMCId.cell)) {
			    pMcell->nextVdIndex = 0;
			} else {
			    pMcell->nextVdIndex = volNum;
			}

			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_510,
					 "Mcell (%d,%d,%d) is corrupt.\n"),
				 volNum, pageNumber, mcell);
			status = add_fixed_message(&(pPage->messages),
						   RBMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_512, 
							   "Modified mcell (%d,%d,%d)'s nextVdIndex to %d.\n"),
						   volNum, pageNumber, mcell,
						   pMcell->nextVdIndex);
			if (SUCCESS != status) {
			    return status;
			}
			modified = TRUE;
		    }
		    
		    if (-2 != pMcell->bfSetTag.num) {
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_510,
					 "Mcell (%d,%d,%d) is corrupt.\n"),
				 volNum, pageNumber, mcell);
			status = add_fixed_message(&(pPage->messages), RBMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_513, 
							   "Modified mcell (%d,%d,%d)'s set tag from %d to %d.\n"),
						   volNum, pageNumber, mcell,
						   pMcell->bfSetTag.num, -2);
			if (SUCCESS != status) {
			    return status;
			}
			pMcell->bfSetTag.num = -2;
			modified = TRUE;
		    }

		    /*
		     * Already checked pMcell->tag in correct_rbmt_*_mcell
		     * routines.
		     */

		} /* end if free/used */
	    } /* end loop of mcells */
	    
	    /*
	     * Check the free list chain mcell headers for RBMT/BMT0
	     */
	    freeCounter = 0;
	    priorMcell  = -1;
	    page = pBMTPage->nextfreeMCId.page;
	    mcell = pBMTPage->nextfreeMCId.cell;
	    
	    while ((0 != page) || (0 != mcell)) {
		pMcell = &(pBMTPage->bsMCA[mcell]);
		if (FREE == freeArray[mcell]) {
		    /*
		     * The value 'ONLIST' means on free list.
		     */
		    freeArray[mcell] = ONLIST;
		    freeCounter++;
		} else {
		    /*
		     * Active cell on free list OR Free list has a loop.
		     */
		    if (-1 == priorMcell) {
			/*
			 * First mcell on list, break chain;
			 */
			pBMTPage->nextfreeMCId.page = 0;
			pBMTPage->nextfreeMCId.cell = 0;
		    } else {
			pMcell = &(pBMTPage->bsMCA[priorMcell]);
			pMcell->nextMCId.page = 0;
			pMcell->nextMCId.cell = 0;
		    }
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_510,
				     "Mcell (%d,%d,%d) is corrupt.\n"),
			     volNum, pageNumber, mcell);
		    status = add_fixed_message(&(pPage->messages),
					       RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_514, 
						       "Modified RBMT/BMT0 mcell free list.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		    
		    /*
		     * Get out of loop.
		     */
		    modified = TRUE;
		    page = 0;
		    mcell = 0;
		    continue;
		} /* end if free */
		priorMcell = mcell;
		page = pMcell->nextMCId.page;
		mcell = pMcell->nextMCId.cell;
	    } /* end while loop, free mcell chain  */
	    
	    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
		if (FREE == freeArray[mcell]) {
		    /*
		     * Free mcell not on list, so add it.
		     */
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_515, 
				     "Mcell (%d,%d,%d) should be on free list.\n"),
			     volNum, pageNumber, mcell);
		    status = add_fixed_message(&(pPage->messages),
					       RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_514, 
						       "Modified RBMT/BMT0 mcell free list.\n"));
		    if (SUCCESS != status) {
			return status;
		    }

		    pMcell = &(pBMTPage->bsMCA[mcell]);
		    pMcell->nextMCId.page =  pBMTPage->nextfreeMCId.page;
		    pMcell->nextMCId.cell =  pBMTPage->nextfreeMCId.cell;
		    pBMTPage->nextfreeMCId.page = pageNumber;
		    pBMTPage->nextfreeMCId.cell = mcell;
		    modified = TRUE;
		    freeCounter++;
		}
	    } /* end mcell loop */
	    
	    if (freeCounter != pBMTPage->freeMcellCnt) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_516, 
				 "Mcell free list incorrect on page %d.\n"),
			 pageNumber);
		status = add_fixed_message(&(pPage->messages),
					   RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_514, 
						   "Modified RBMT/BMT0 mcell free list.\n"));
		if (SUCCESS != status) {
		    return status;
		}
		pBMTPage->freeMcellCnt = freeCounter;
		modified = TRUE;
	    }

	    /*
	     * End check of free list
	     */
	    
	    if (TRUE == modified) {
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_517, 
						   "Modified RBMT/BMT0 page.\n"));
		if (SUCCESS != status) {
		    return status;
		}
	    }
	    
	    status = release_page_to_cache(volNum, currPageLBN, pPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;
	    pageNumber++;
	} /* End loop of extents */

	/*
	 * Re-read RBMT page 0.
	 */
	currPageLBN   = RBMT_PAGE0;
	modified      = FALSE;
	  
	/* 
	 * Read this volumes RBMT page 0.
	 */
	status = read_page_from_cache(volNum, currPageLBN, &pPage);
	if (SUCCESS != status) {
	    return status;
	}

	pBMTPage = (bsMPgT *)pPage->pageBuf;

	for (mcell = 0; mcell <= 5; mcell++) {
	    if ((TRUE != isOdsV4) && (4 == mcell)) {
		/*
		 * Mcell 4 on ODSv3 is part of the BMT chain.
		 */
		continue;
	    }

	    /*
	     * Check Mcell
	     */
	    pMcell = &(pBMTPage->bsMCA[mcell]);
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    found = FALSE;

	    while ((0 != pRecord->type) &&
		   (0 != pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))) &&
		   (FALSE == found)) {
		if (BSR_XTNTS == pRecord->type) {
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    found = TRUE;
		    break;
		}	    
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } /* end record loop */

	    /*
	     * We should ALWAYS find the Xtnt record.
	     */
	    assert(TRUE == found);

	    if (5 == mcell) {
		/* 
		 * Misc mcell (mcell 5) was a
		 * special case, it had always been set to 0, even
		 * though the value should have been 1.
		 *
		 * this value is now set correctly.
		 *
		 * If the computed mcellCnt is equal to 1, then
		 * either 0 or 1 are valid values for mcell 5. Other
		 * values for mcellCnt handled in the general case.
		 */
		if (1 == mcellCnt[mcell]) {
		    if (0 == pXtnt->firstXtnt.mcellCnt) {
			continue;
		    }
		}
	    } /* end special case for mcell 5 */

	    /*
	     * Check chain vs mcellCnt
	     */
	    if ((0 == pXtnt->chainVdIndex) &&
		(0 == pXtnt->chainMCId.page) &&
		(0 == pXtnt->chainMCId.cell)) {
		if (1 != pXtnt->firstXtnt.mcellCnt) {
		    /*
		     * Chain is empty, mcellCnt should be 1.
		     */
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_510,
				     "Mcell (%d,%d,%d) is corrupt.\n"),
			     volNum, 0, mcell);

		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_142,
						       "Modified mcell (%d,%d,%d)'s mcellCnt from %d to %d.\n"),
					       volNum, 0, mcell,
					       pXtnt->firstXtnt.mcellCnt,
					       1);
		    pXtnt->firstXtnt.mcellCnt = 1;
		    modified = TRUE;
		}
	    } else if (mcellCnt[mcell] != pXtnt->firstXtnt.mcellCnt) {
		/*
		 * Chain is not (0,0,0) and mcellCnts are different.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_510,
				 "Mcell (%d,%d,%d) is corrupt.\n"),
			 volNum, 0, mcell);

		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_142,
						   "Modified mcell (%d,%d,%d)'s mcellCnt from %d to %d.\n"),
						   volNum, 0, mcell,
						   pXtnt->firstXtnt.mcellCnt,
						   mcellCnt[mcell]);
		pXtnt->firstXtnt.mcellCnt = mcellCnt[mcell];
		modified = TRUE;
	    } /* end mcellCnts are different */
	} /* end mcell loop */

	status = release_page_to_cache(volNum, currPageLBN, pPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	modified = FALSE;

    } /* end volume loop */

    /* 
     * Did we find a root tag volume?
     */
    rootTagVolume = 0;

    if (bfSetDirVol && domain->volumes[bfSetDirVol].rootTag) {
        rootTagVolume = bfSetDirVol;
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		"rootTagVolume via bfSetDirVol = %d\n", rootTagVolume);
    } else
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
        if (-1 == domain->volumes[volNum].volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}
	if (FALSE != domain->volumes[volNum].rootTag) {
	    writemsg(SV_ERR | SV_LOG_ERR,
	             "rootTagVolume via search = %d\n", volNum);
	    if (0 == rootTagVolume) {
		rootTagVolume = volNum;
	    } else {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_518,
				 "Unable to continue, more than one root tag file.\n"));
		corrupt_exit(NULL);
	    }
	} /* end if root tag volume found */
    } /* end volume loop */

    if (0 == rootTagVolume) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_519,
			 "Unable to continue, not able to find a volume with a root tag file.\n")); 
	corrupt_exit(NULL);
    } else {
        domain->rootTagVol = rootTagVolume;
    }

    if (NULL == logVolume) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_520,
			 "Unable to continue, not able to find a volume with the transaction log.\n"));
	corrupt_exit(NULL);
    }

    /*
     * The mattr must be on the same volume as the log.
     */
    if (0 == mattrVol) {
        /*
	 * Future: What if we found the logVolume, should we create the
	 *         mattr record?  More research is needed for this.
	 */
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_521,
			 "Unable to continue, can't find a volume with domain attributes.\n"));
	corrupt_exit(NULL);
    } else {
        found = FALSE;
	headLogVolume = logVolume;

	while ((TRUE != found) && (NULL != headLogVolume)) {
	    /*
	     *  Walk the logVolume chain looking for match to mattrVol.
	     */
	    if (headLogVolume->volume == mattrVol) {
	        domain->logVol = mattrVol;
		found = TRUE;
	    } else {
	        headLogVolume = headLogVolume->next;
	    }
	}
	
	if (FALSE == found) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_522,
			     "Unable to continue, can't find domain attributes on the log volume.\n")); 
	    corrupt_exit(NULL);
	}
    }

    /*
     * free log volume struture.
     */
    headLogVolume = logVolume;
    while (NULL != headLogVolume) {
        tmpLogVolume = headLogVolume;
	headLogVolume = headLogVolume->next;
	free(tmpLogVolume);
    }
    
    /*
     * Now that we know which volume the mattr is on we need to update 
     * the fields which we couldn't do earlier.
     */
    currPageLBN = RBMT_PAGE0;
    status = read_page_from_cache(mattrVol, currPageLBN, &pPage);
    if (SUCCESS != status) {
        return status;
    }

    pBMTPage = (bsMPgT *)pPage->pageBuf;

    /*
     * This mcell is in different locations based on ODS version.
     */
    if (TRUE == isOdsV4) {
        pMcell = &(pBMTPage->bsMCA[6]);
    } else {
        pMcell = &(pBMTPage->bsMCA[4]);
    }

    pRecord = (bsMRT *)pMcell->bsMR0;
    found = FALSE;

    while ((0 != pRecord->type) &&
	   (0 != pRecord->bCnt) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))) &&
	   (FALSE == found)) {
        if (BSR_DMN_MATTR == pRecord->type) {
	    pDmnMAttr = (bsDmnMAttrT *)((char *)pRecord + sizeof(bsMRT));
	    found = TRUE;
	    break;
	}	    
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 
    }

    /*
     * Based on earlier checks we should ALWAYS find the mattr record.
     */
    assert(TRUE == found);

    if (pDmnMAttr->bfSetDirTag.num !=  -(2 + (6 * rootTagVolume))) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_523,
					   "Modified set dir tag's number from %d to %d.\n"),
				   pDmnMAttr->bfSetDirTag.num,
				   -(2 + (6 * rootTagVolume)));
	if (SUCCESS != status) {
	    return status;
	}
        pDmnMAttr->bfSetDirTag.num = -(2 + (6 * rootTagVolume));
        modified = TRUE;
    }

    if (pDmnMAttr->ftxLogTag.num != -(3 + (6 * mattrVol))) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_524,
					   "Modified ftx log tag's number from %d to %d.\n"),
				   pDmnMAttr->ftxLogTag.num, 
				   -(3 + (6 * mattrVol)));
	if (SUCCESS != status) {
	    return status;
	}
        pDmnMAttr->ftxLogTag.num = -(3 + (6 * mattrVol));
        modified = TRUE;
    }

    if (pDmnMAttr->vdCnt != domain->numVols) {
        char answer[10];  /* buffer for user input */
	char errorMsg[MAXERRMSG];
	int  status;

	writemsg(SV_ERR | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_525, 
			 "The domain attributes on volume %d has a different number of\n"), 
		  volNum);
	writemsg(SV_ERR | SV_LOG_FOUND | SV_CONT, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_526, 
			 "volumes compared to the number of volumes in /etc/fdmns.\n"));

	writemsg(SV_ERR | SV_CONT, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_527, 
			 "Set the number of volumes to %d? (y/[n])  "),
		 domain->numVols);
		      
	status = prompt_user_yes_no(answer, "n");
	if (SUCCESS != status) {
	    return status;
	}

	if ('n' == answer[0]) {
	    /*
	     * User does not want to continue.
	     */
	    sprintf(errorMsg, 
		    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_528, 
			    "Exit requested, try running 'advscan' to find all volumes.\n"));
	    corrupt_exit(errorMsg);
	}
	pDmnMAttr->vdCnt = domain->numVols;
	modified = TRUE;
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_529, 
			 "Corruption in domain mattr record.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_530, 
					   "Modified RBMT/BMT0 domain mattr record.\n"));
	if (SUCCESS != status) {
	    return status;
	}
    }
    
    status = release_page_to_cache(mattrVol, currPageLBN, pPage,
				   modified, FALSE);
    if (SUCCESS != status) {
        return status;
    }

    /*
     * Free the RBMT extent array as it will be collected later, and
     * at that point we will need to collect SBM information.
     */
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	volume = &(domain->volumes[volNum]);
        if (-1 == volume->volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}

	free (volume->volRbmt->rbmtLBN);
	volume->volRbmt->rbmtLBN = NULL;
	volume->volRbmt->rbmtLBNSize = 0;
    } /* end of volume loop */

    return SUCCESS;
} /* end correct_rbmt_pages */


/*
 * Function Name: correct_rbmt_bmt_mcell (3.23)
 *
 * Description: This function will check mcell 0 of the RBMT. It will
 *              fix and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     isRBMT: Flag to let us know if this is RBMT or BMT0.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
int correct_rbmt_bmt_mcell(pageT *pPage, int isRBMT, int volNum)
{
    char   *funcName = "correct_rbmt_bmt_mcell";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;   /* The Current mcell record we are working */
    int    modified;   /* Flag to let us know if the page has been modified. */
    int    foundDDLRecord;
    int    foundSSRecord;
    int    foundVdRecord;
    int    foundFreezeRecord;
    int    x;
    int    mcellNum;
    int    tagNum;
    int    status;
    bsBfAttrT   *pBSRAttr;
    bsXtntRT    *pXtnt;
    vdIoParamsT *pVdIoParams;
    delLinkRT   *delLink;
    mcellT      nextDelMcell;

    /*
     * Init variables.
     */
    modified = FALSE;
    mcellNum = 0;
    tagNum   = -(6 * volNum);
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);

    if (pMcell->tag.num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_531,
					   "Modified RBMT/BMT0 BMT mcell's tag number from %d to %d.\n"),
				   pMcell->tag.num, tagNum);
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = tagNum;
	modified = TRUE;
    }

    if (FALSE == isRBMT) {
        /*
	 * Working on an old domain: BMT0
	 */
        if ((0 != pMcell->nextMCId.page) || 
	    (4 != pMcell->nextMCId.cell)) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_532, 
					       "Modified RBMT/BMT0 BMT mcell's next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
				       volNum, 
				       pMcell->nextMCId.page,
				       pMcell->nextMCId.cell,
				       volNum, 0, 4);
	    if (SUCCESS != status) {
		return status;
	    }
	    pMcell->nextMCId.page = 0;
	    pMcell->nextMCId.cell = 4;
	    modified = TRUE;
	} 
    } else {
        /*
	 * Working on a NEW domain: RBMT
	 */
        if ((0 != pMcell->nextMCId.page) || 
	    (6 != pMcell->nextMCId.cell)) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_532, 
					       "Modified RBMT/BMT0 BMT mcell's next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
				       volNum, 
				       pMcell->nextMCId.page,
				       pMcell->nextMCId.cell,
				       volNum, 0, 6);
	    if (SUCCESS != status) {
		return status;
	    }
	    pMcell->nextMCId.page = 0;
	    pMcell->nextMCId.cell = 6;
	    modified = TRUE;
	} 
    }

    pRecord = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_bsr_attr_record (pPage, pRecord, mcellNum, 2, 0);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_bsr_xtnts_default_values (pPage, pRecord, 
					       mcellNum, 80);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */
    if (2 != pXtnt->firstXtnt.xCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_543, 
					   "Modified RBMT/BMT0 BMT mcell's xCnt from %d to %d.\n"),
				   pXtnt->firstXtnt.xCnt, 2);
	if (SUCCESS != status) {
	    return status;
	}
        pXtnt->firstXtnt.xCnt = 2;
	modified = TRUE;
    }

    if (TRUE == isRBMT) {
        /*
	 * RBMT unique values
	 *
	 * Can only have a single extent which starts at BLK 32.
	 */
        if ((0  != pXtnt->firstXtnt.bsXA[0].bsPage) ||
	    (32 != pXtnt->firstXtnt.bsXA[0].vdBlk)  ||
	    (1  != pXtnt->firstXtnt.bsXA[1].bsPage) ||
	    (-1 != pXtnt->firstXtnt.bsXA[1].vdBlk)) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_544, 
					       "Modified RBMT/BMT0 BMT mcell's extents.\n"));
	    if (SUCCESS != status) {
		return status;
	    }
	    pXtnt->firstXtnt.bsXA[0].bsPage = 0;
	    pXtnt->firstXtnt.bsXA[0].vdBlk  = 32;
	    pXtnt->firstXtnt.bsXA[1].bsPage = 1;
	    pXtnt->firstXtnt.bsXA[1].vdBlk  = -1;
	    modified = TRUE;
	}

	/*
	 * Do we have addional RBMT pages?
	 */
	if (1 != pXtnt->firstXtnt.mcellCnt) {
	    /*	
	     * more than 1 mcell in chain, chain better point to 
	     * (volNum,0,27).
	     */
	    if ((pXtnt->chainVdIndex != volNum) ||
		(0 != pXtnt->chainMCId.page) ||
		(27 != pXtnt->chainMCId.cell)) {
	        bsMCT  *pTempMcell;    
		bsMRT  *pTempRecord;   

		pTempMcell  = &(pBMTPage->bsMCA[27]);
		pTempRecord = (bsMRT *)pTempMcell->bsMR0;

		/*
		 * Is chain pointer incorrect, or is mcellCnt incorrect?
		 *
		 * If mcell 27 is of type BSR_XTRA_XTNTS we will assume the
		 * chain is incorrect, otherwise mcellCnt is incorrect.
		 */
		if (BSR_XTRA_XTNTS == pTempRecord->type) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_545,
						       "Modified RBMT/BMT0 BMT mcell's chain pointer from (%d,%d,%d) to (%d,%d,%d).\n"),
					       pXtnt->chainVdIndex,
					       pXtnt->chainMCId.page,
					       pXtnt->chainMCId.cell,
					       volNum, 0, 27);
		    if (SUCCESS != status) {
			return status;
		    }
		    pXtnt->chainVdIndex   = volNum;
		    pXtnt->chainMCId.page = 0;
		    pXtnt->chainMCId.cell = 27;
		} else {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_546, 
						       "Modified RBMT/BMT0 BMT mcell's mcellCnt from %d to %d.\n"),
					       pXtnt->firstXtnt.mcellCnt, 1);
		    if (SUCCESS != status) {
			return status;
		    }
		    pXtnt->firstXtnt.mcellCnt = 1;
		}
		modified = TRUE;
	    } /* end if chain pointer not correct */
	} /* end if more than 1 mcell in chain */
    } else {
        /*
	 * BMT0 unique values. 
	 */
	/*
	 * Can only have a 2 page extent which starts at BLK 32.
	 */
        if ((0  != pXtnt->firstXtnt.bsXA[0].bsPage) ||
	    (32 != pXtnt->firstXtnt.bsXA[0].vdBlk)  ||
	    (2  != pXtnt->firstXtnt.bsXA[1].bsPage) ||
	    (-1 != pXtnt->firstXtnt.bsXA[1].vdBlk)) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_544, 
					       "Modified RBMT/BMT0 BMT mcell's extents.\n"));
	    if (SUCCESS != status) {
		return status;
	    }
	    pXtnt->firstXtnt.bsXA[0].bsPage = 0;
	    pXtnt->firstXtnt.bsXA[0].vdBlk  = 32;
	    pXtnt->firstXtnt.bsXA[1].bsPage = 2;
	    pXtnt->firstXtnt.bsXA[1].vdBlk  = -1;
	    modified = TRUE;
	}

	/*
	 * Do we have addional BMT pages?
	 */
	if (1 != pXtnt->firstXtnt.mcellCnt) {
	    /*	
	     * more than 1 mcell in chain, chain better point to volNum,0,6.
	     */
	    if ((pXtnt->chainVdIndex != volNum) ||
		(0 != pXtnt->chainMCId.page) ||
		(6 != pXtnt->chainMCId.cell)) {
	        bsMCT  *pTempMcell;    
		bsMRT  *pTempRecord;   

		pTempMcell  = &(pBMTPage->bsMCA[6]);
		pTempRecord = (bsMRT *)pTempMcell->bsMR0;

		/*
		 * Is chain pointer incorrect, or is mcellCnt incorrect?
		 *
		 * If mcell 6 is of type BSR_XTRA_XTNTS we will assume the
		 * chain is incorrect, otherwise mcellCnt is incorrect.
		 */
		if (BSR_XTRA_XTNTS == pTempRecord->type) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_545, 
						       "Modified RBMT/BMT0 BMT mcell's chain pointer from (%d,%d,%d) to (%d,%d,%d).\n"), 
					       pXtnt->chainVdIndex,
					       pXtnt->chainMCId.page,
					       pXtnt->chainMCId.cell,
					       volNum, 0, 6);
		    if (SUCCESS != status) {
			return status;
		    }
		    pXtnt->chainVdIndex   = volNum;
		    pXtnt->chainMCId.page = 0;
		    pXtnt->chainMCId.cell = 6;
		} else {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_546, 
						       "Modified RBMT/BMT0 BMT mcell's mcellCnt from %d to %d.\n"), 
					       pXtnt->firstXtnt.mcellCnt, 1);
		    if (SUCCESS != status) {
			return status;
		    }
		    pXtnt->firstXtnt.mcellCnt = 1;
		}
		modified = TRUE;
	    }
	} /* end if mcellCnt != 1 */
    } /* end rbmt/bmt if/else */

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 

    /*
     * At this point, Multiple record types may appear in any order:
     *		BSR_DEF_DEL_MCELL_LIST
     *		BSR_DMN_SS_ATTR
     *		BSR_VD_IO_PARAMS
     *          BSR_DMN_FREEZE_ATTR
     */
    foundDDLRecord = FALSE;
    foundSSRecord  = FALSE;
    foundVdRecord  = FALSE;
    foundFreezeRecord = FALSE;

    while ((0 != pRecord->type) &&
	   ((FALSE == foundSSRecord) ||
	    (FALSE == foundVdRecord) ||
	    (FALSE == foundDDLRecord) ||
	    (FALSE == foundFreezeRecord))) {

	switch (pRecord->type) {
	    case (BSR_DEF_DEL_MCELL_LIST):
		if (TRUE == foundDDLRecord) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundDDLRecord) */
		foundDDLRecord = TRUE;

		/* (sizeof(bsMRT) + sizeof(delLinkRT)) == 12 */
		if ((sizeof(bsMRT) + sizeof(delLinkRT)) != pRecord->bCnt) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_541, 
						       "Modified RBMT/BMT0 BMT mcell's record bCnt from %d to %d.\n"),
					       pRecord->bCnt, 
					       (sizeof(bsMRT) + sizeof(delLinkRT)));
		    if (SUCCESS != status) {
			return status;
		    }
		    pRecord->bCnt = (sizeof(bsMRT) + sizeof(delLinkRT));
		    modified = TRUE;
		}

		/*
		 * Zero out the RBMT DDL.
		 */
		delLink = (delLinkRT *)((char *)pRecord + sizeof(bsMRT));

		/*
		 * Save the next record.
		 */
		nextDelMcell.page = delLink->nextMCId.page;
		nextDelMcell.cell = delLink->nextMCId.cell;

		if ((0 != nextDelMcell.page) || (0 != nextDelMcell.cell)) {
		    /*
		     * Non-empty DDL - Set head of deferred delete list
		     * to represent an empty list.
		     */
		    delLink->nextMCId.page = 0;
		    delLink->nextMCId.cell = 0;
		    delLink->prevMCId.page = 0;
		    delLink->prevMCId.cell = 0;

		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_908, 
						       "Cleared the RBMT deferred delete list.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		    modified = TRUE;
		} /* end if DDL not empty */
		break;

	    case (BSR_DMN_SS_ATTR):
		if (TRUE == foundSSRecord) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundSSRecord) */

		foundSSRecord = TRUE;
		status = correct_smart_store_record(pPage, pRecord, 
						    mcellNum);
		if (FIXED == status) {
		    modified = TRUE;
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_883,
						       "Corrected vfast record.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		}
		break;

	    case (BSR_VD_IO_PARAMS):
		if (TRUE == foundVdRecord) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundVdRecord) */
		foundVdRecord = TRUE;

		/* (sizeof(bsMRT) + sizeof(vdIoParamsT)) == 28 */
		if ((sizeof(bsMRT) + sizeof(vdIoParamsT)) != pRecord->bCnt) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_541, 
						       "Modified RBMT/BMT0 BMT mcell's record bCnt from %d to %d.\n"),
					       pRecord->bCnt, 
					       (sizeof(bsMRT) + sizeof(vdIoParamsT)));
		    if (SUCCESS != status) {
			return status;
		    }
		    pRecord->bCnt = (sizeof(bsMRT) + sizeof(vdIoParamsT));
		    modified = TRUE;
		}

		/*
		 * The values in VdIoParams can be safely ignored.
		 */
		pVdIoParams = (vdIoParamsT *)((char *)pRecord + sizeof(bsMRT));
		break;

	    case (BSR_DMN_FREEZE_ATTR):
		if (TRUE == foundFreezeRecord) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundFreezeRecord) */

		foundFreezeRecord = TRUE;
		status = correct_freeze_record (pPage, pRecord, mcellNum);
		if (FIXED == status) {
		    modified = TRUE;
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_911,
						       "Corrected domain freeze record.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		}
		break;

	    default:
		/*
		 * Found an unsupported record.  Delete it.
		 * This may not be the best thing to do.  Perhaps we
		 * should consider moving all later records "forward".
		 */
		pRecord->type = 0;
		pRecord->bCnt = 0;
		modified = TRUE;
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_549, 
						   "Deleted records from RBMT/BMT0 BMT mcell.\n"));
		if (SUCCESS != status) {
		    return status;
		}
		break;
	} /* end switch */

	/*
	 * Set pRecord to the next record of pMcell.
	 */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 
    } /* End while ((0 != pRecord->type) && ... */

    /*
     * We have found all the record we are looking for. So this mcell 
     * should not have any more records.
     */
    if (0 != pRecord->type) {
        pRecord->type = 0;
	pRecord->bCnt = 0;
	modified = TRUE;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_549, 
					   "Deleted records from RBMT/BMT0 BMT mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_550, 
			 "Found corruptions in RBMT/BMT0 BMT mcell.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_551, 
					   "Modified RBMT/BMT0 BMT mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_bmt_mcell */

/*
 * Function Name: find_first_sbm_page 
 *
 * Description: Try & find hte first page of the SBM.
 *
 * Input parameters:
 *     bsXA: The extent array.
 *     vdBlkCnt: Total size of the Volume.
 *     odsVersion: The domain version
 *     sbmBlock: The sbmBlock which we tried to read initially.
 * 
 * Output parameters: N/A
 *
 * Returns: TRUE,FAILURE 
 */

int find_first_sbm_page(bsXtntRT *pXtnt,unsigned int vdBlkCnt, int volNum, int odsVersion, LBNT sbmBlock )
{
	
	
	int sbmFixed,status;
	pageT *pTempPage;
	LBNT sbmFirstBlk;		
	sbmFixed = FALSE;
	/* 
	 * Try to find the sbm on BLK 96 or 112 or 40% of VolSize.
	 */


        if ((96 != pXtnt->firstXtnt.bsXA[0].vdBlk) &&
	    (112 != pXtnt->firstXtnt.bsXA[0].vdBlk)) {
		
		/*
             	 * Check to see if page 96 is part of the SBM.
             	 */
            	status = read_page_from_cache(volNum, 96, &pTempPage);
            	if (SUCCESS != status) {
               		return status;
            	}
            	status = validate_sbm_page(pTempPage, odsVersion,0);
            	if (SUCCESS == status) {
                    /*
                     * The SBM starts on page 96.
                     */
                     pXtnt->firstXtnt.bsXA[0].vdBlk = 96;
                     sbmFixed = TRUE;
             	}
		
		status = release_page_to_cache(volNum, 96, pTempPage,FALSE,FALSE);
	    	if ( SUCCESS == status && sbmFixed == TRUE ) {	
			return sbmFixed;
	    	}
				
		if (TRUE != sbmFixed) {
                	/*
                 	 * Check to see if page 112 is part of the SBM.
                 	 */
                	status = read_page_from_cache(volNum, 112, &pTempPage);
                	if (SUCCESS != status) {
		    		return status;
                	}

                	status = validate_sbm_page(pTempPage, odsVersion,0);
                	if (SUCCESS == status) {
                    		/*
                     	 	 * The SBM starts on page 112.
                     	 	 */
                    		pXtnt->firstXtnt.bsXA[0].vdBlk = 112;
                    		sbmFixed = TRUE;
                	}

                	status = release_page_to_cache(volNum, 112, pTempPage,
                                               FALSE, FALSE);
            		if (SUCCESS == status && sbmFixed == TRUE) {
                	    		return sbmFixed;
                	}
		}

		/*--DVN3 can have SBM starting at 96 or 112 only--*/

		if (TRUE != sbmFixed && odsVersion <= 3) {
                	writemsg(SV_ERR | SV_LOG_ERR,
                        	 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_562,
                                	 "Unable to find the starting block of the SBM.\n"));
                	corrupt_exit(NULL);
		}
		
            	if (TRUE != sbmFixed) {
            		/*
                	 * Check to see if page sbmFirstBlk is part of the SBM.
                 	 */
			sbmFirstBlk = roundup((vdBlkCnt / 100) * 40, BS_CLUSTSIZE);
 			if (sbmBlock == sbmFirstBlk )
			{
				/*-Try to continue, as this will be corrected later in sbm_correct-*/
				sbmFixed = TRUE;
				return (sbmFixed);	 	
			
			}
			else {
	       
	        		status = read_page_from_cache(volNum, sbmFirstBlk, &pTempPage);
                		if (SUCCESS != status) {
                    			return status;
                		}
		
				status = validate_sbm_page(pTempPage, odsVersion,0);
                		if (SUCCESS == status) {
                    			/*
                     	 	 	 * The SBM start on block sbmFirstBlk.
                     	 	 	 */
                    			pXtnt->firstXtnt.bsXA[0].vdBlk = sbmFirstBlk;
                    			sbmFixed = TRUE;
                		}
		
				status = release_page_to_cache(volNum, sbmFirstBlk,
                                               	pTempPage, FALSE, FALSE);
                		if (SUCCESS == status && sbmFixed == TRUE) {
                    			return sbmFixed;
		  		}
			}
		}
	}
	if (TRUE != sbmFixed) {
               	writemsg(SV_ERR | SV_LOG_ERR,
                       	 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_562,
                               	 "Unable to find the starting block of the SBM.\n"));
               	corrupt_exit(NULL);
        }

} /* end  find_first_sbm_page */	

/*
 * Function Name: test_sbm_xtnt_array 
 *
 * Description: This function will check the extent array of a given extent.
 *              It also tries to validate each of the SBM pages. 
 *
 * Input parameters:
 *     sCnt: Size of the extent descriptor array	
 *     bsXA: The extent array to be checked	
 *     prevPage: The last SBM page of the previous extent.
 *     pPage: The RBMT page containing the extent map.
 *     odsVersion: The domain version		
 *     domain: The domain we are working on.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters:
 * modified : TRUE to indicate if the RBMT page information was changed. 
 *
 * Returns: SUCCESS, FAILURE
 */

int test_sbm_xtnt_array( int sCnt, bsXtntT *bsXA, int volNum,int prevPage, int *modified,pageT *pPage, volumeT *volume )
{

	LBNT vdblk;
	pageT *pTempPage;
	int status,i,j;
	int numPages;
	int odsVersion = volume->dmnVersion;
	
	if ( (bsXA[0].bsPage != prevPage) || ((LBNT) -1 != bsXA[sCnt-1].vdBlk) ) {

		status = add_fixed_message(&(pPage->messages), RBMT,
       				catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_564,
               		"Modified SBM extent records from [%d,%d],[%d,%d] to [%d,%d],[%d,%d].\n"),
			bsXA[0].bsPage,
                        bsXA[0].vdBlk,
                        bsXA[sCnt-1].bsPage,
                        bsXA[sCnt-1].vdBlk,
                        0, bsXA[0].vdBlk,
                        bsXA[sCnt-1].bsPage, -1);
		if (SUCCESS != status) {
				return status;
        	}

		bsXA[0].bsPage = prevPage;
        	bsXA[sCnt-1].vdBlk  = -1;
        	*modified = TRUE;
    	}

	for(i=1;i<sCnt;i++)
	{
		/*--check no holes in SBM & extents are in ascending order--*/		
		if ( bsXA[i].bsPage <= prevPage ) {	
			/*--need to do more here--*/
			return FAILURE;
		}
		prevPage = bsXA[i].bsPage;
		if ( bsXA[i].vdBlk == (LBNT) -1 && i != (sCnt -1) ) {
	
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_935,
					"Can't convert SBM page %d to LBN.\n"),bsXA[i].bsPage);		
                        return FAILURE;
                }
	}

	for( i = 0 ; i < ( sCnt - 2 ) ; i++ )
	{

		vdblk = bsXA[i].vdBlk;	
		numPages = bsXA[i+1].bsPage - bsXA[i].bsPage;

		for (j=0;j<=(numPages-1);j++)
		{
			status = read_page_from_cache(volNum,vdblk,&pTempPage);
			if (SUCCESS != status) {
                		return status;
        		}
			status = validate_sbm_page(pTempPage, odsVersion, j);
			if (SUCCESS != status) {
				writemsg (SV_VERBOSE | SV_LOG_FOUND,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_689,
                                              "Mismatch in SBM data on volume %s, SBM page %d.\n"),
                                      volume->volName, j+bsXA[i].bsPage);
			}
			status = release_page_to_cache(volNum,vdblk,pTempPage,FALSE,FALSE);
			if (SUCCESS != status) {
                                return status;
                        }
			vdblk = vdblk + 16;
		}
	
	}
	return SUCCESS;
	
}
/*
 * Function Name: correct_rbmt_sbm_mcell (3.24)
 *
 * Description: This function will check mcell 1 of the RBMT.
 *              It will fix and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     domain: The domain we are working on.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FIXED, CORRUPT
 */
int correct_rbmt_sbm_mcell(pageT *pPage, domainT *domain, int volNum)
{
    char   *funcName = "correct_rbmt_sbm_mcell";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell,prevMcell;    /* The current RBMT mcell we are working on */
    bsMRT  *pRecord,*sRecord;   /* The current mcell record we are working on */
    int    modified,pageZeroModified;   /* Flag to let us know if the page has been modified. */
    int    status;
    int    mcellNum;
    int    tagNum;
    int    x;
    int    odsVersion;
    int    numPages,totalSbmPages; /* number of pages which should be in the SBM */
    int    volPages;   /* number of 8K pages */
    div_t  temp;       /* temporary for division to determine SBM pages */
    char   errorMsg[MAXERRMSG];
    bsBfAttrT   *pBSRAttr;
    bsXtntRT    *pXtnt;
    bsXtraXtntRT *sXtnt;	
    int xCnt,sCnt;
    LBNT lbn;
    int vol;		
    pageT *tempPage; 
    volumeT *volume;	
    bsMCT *Mcell;
    LBNT rbmtLbn;
    int vdi,page_to_read;	
    LBNT sbmFirstBlk,vdblk;
    unsigned int vdBlkCnt;
    int   sbmFixed;
    pageT *pTempPage;
    int i,j,prevPage =0;
    int next = 0;
    
    /*
     * Init variables
     */

    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = 1;
    tagNum   = -(mcellNum + (6 * volNum));
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    volPages = domain->volumes[volNum].volSize/BLOCKS_PER_PAGE;
    odsVersion = domain->dmnVersion;
    volume = &(domain->volumes[volNum]);	
	 
    if (pMcell->tag.num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_552,
			 "Modified RBMT/BMT0 SBM mcell's tag number from %d to %d.\n"),pMcell->tag.num, tagNum);
	
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = tagNum;
	pageZeroModified = TRUE;
    }

    pRecord = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_bsr_attr_record (pPage, pRecord, mcellNum, 2, 0);
    if (FIXED == status) {
	pageZeroModified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_bsr_xtnts_default_values (pPage, pRecord, 
					       mcellNum, 80);
    if (FIXED == status) {
	pageZeroModified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }
   
    /*
     * Unique BSR_XTNTS values to this mcell
     */
    if (2 != pXtnt->firstXtnt.xCnt) {
        status = add_fixed_message(&(pPage->messages), RBMT,
                                   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_543,
                                           "Modified RBMT/BMT0 BMT mcell's xCnt from %d to %d.\n"),
                                   pXtnt->firstXtnt.xCnt, 2);
        if (SUCCESS != status) {
            return status;
        }
        pXtnt->firstXtnt.xCnt = 2;
        pageZeroModified = TRUE;
    }
	
    assert (0 != domain->volumes[volNum].volSize);


    vdBlkCnt = domain->volumes[volNum].volSize;
    sbmFirstBlk = pXtnt->firstXtnt.bsXA[0].vdBlk;
		
    /*
     * RBMT unique values:
     *
     * Can only have a single extent which can start at the following 
     * possible places:
     * BLK 96 if originally the root tag file was NOT on this domain.
     *      BLK 112 if root tag file was originally on this domain.
     *      a BLK at the 40% mark of the original size of the volume.
     */
	 
    /*
     * we atlaset have a +ve logilcal block number,now check further.
     */
    
    if (odsVersion <= 3)
    {
	/*--DVN3 doesnot always take care of the XOR, hence just validate
	 *-- the SBM starting block is 96 0r 112--
	 */
	if (sbmFirstBlk != 96 && sbmFirstBlk != 112) {
		status = find_first_sbm_page(pXtnt,vdBlkCnt,volNum,odsVersion,sbmFirstBlk);
		if ( status == TRUE ) {
			status = add_fixed_message(&(pPage->messages), RBMT,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_563,
                                              "Modified the block that the SBM starts on to %u.\n"),
                                      pXtnt->firstXtnt.bsXA[0].vdBlk);
			if (SUCCESS != status) {
         			return status;
         		}
            		pageZeroModified = TRUE;
	
		}
		else {
			/*--should never reach here --*/ 
			corrupt_exit(NULL);
		}
	}
    }
    else {		
	/*
	 * validate SBM page 0
	 */
	
	status = read_page_from_cache(volNum,sbmFirstBlk,&pTempPage); 
	if (SUCCESS != status) {
        	return status;
        }

	status = validate_sbm_page( pTempPage, odsVersion, 0 );
	if (SUCCESS != status) {
		status = release_page_to_cache(volNum,sbmFirstBlk,pTempPage,modified,FALSE);
                if (SUCCESS != status) {
                	return status;
                }

		/*--need to enhance this --may be try to find the location--*/
		/*--lets try the old method--*/
		status = find_first_sbm_page(pXtnt,vdBlkCnt,volNum,odsVersion,sbmFirstBlk);
		if ( status == TRUE )
		{
			if ( sbmFirstBlk != pXtnt->firstXtnt.bsXA[0].vdBlk ) {
				status = add_fixed_message(&(pPage->messages), RBMT,
        			                           catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_563,
                                			     	   "Modified the block that the SBM starts on to %u.\n"),
							   pXtnt->firstXtnt.bsXA[0].vdBlk);
				if (SUCCESS != status) {
        	       			 return status;
            			}
            			pageZeroModified = TRUE;
			}
		}
		else
		{
			/*--should never reach here --*/ 
			corrupt_exit(NULL);
		}
	}
	else {
		
		status = release_page_to_cache(volNum,sbmFirstBlk,pTempPage,modified,FALSE);
		if (SUCCESS != status) {
       	        	return status;
        	}
	}
    }

    /*-validate 1)extent count & pages 2)each of the sbm pages */
    /*--check the SBM extents */
	
    /*
     * Compute the number of pages.
     */
    if (odsVersion > 3)
    {
	temp = div(volPages, SBM_ADVFS_PGS(16));
	numPages = temp.quot;
	if (0 != temp.rem) {
		numPages++;
       	}
    }
    else {
	temp = div(volPages, SBM_ADVFS_PGS(2));
	numPages = temp.quot;
        if (0 != temp.rem) {
        	numPages++;
        }
   }

   totalSbmPages = numPages;
   xCnt = pXtnt->firstXtnt.xCnt;

   /*--Check if first page is 0 & last extent descriptor lbn is -1 --*/

   status = test_sbm_xtnt_array(xCnt,pXtnt->firstXtnt.bsXA,volNum,0,&modified,pPage,volume); 
   if ( status != SUCCESS)
   {
	writemsg(SV_ERR | SV_LOG_ERR,
        	 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_936,
                 	 "Corrupt SBM extent.\n"));
	return FAILURE;
   }
   vdi = pXtnt->chainVdIndex;

   if ( (vdi == 0) && (pXtnt->chainMCId.page == 0) && (pXtnt->chainMCId.cell == 0) ) {
	if (pXtnt->firstXtnt.mcellCnt != 1) {
		status = add_fixed_message(&(pPage->messages),
                                                   BMT,
                                                   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_431,
                                                           "Modified mcell (%d,%d,%d)'s xtnt mcell count from %d to %d.\n"),
                                                   vdi,
                                                   0,
                                                   mcellNum,
                                                   pXtnt->firstXtnt.mcellCnt,
                                                   1);
                        if (SUCCESS != status) {
                            return status;
                        }
	
		pXtnt->firstXtnt.mcellCnt = 1;
		pageZeroModified = TRUE;
	}
	return SUCCESS;
    }
    else {
	if (vdi != volNum)
	{
		status = add_fixed_message( &pPage->messages, RBMT,
                       			    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_938,
						    "Modified extent's next volume Index from %d to %d.\n"),
						     vdi,volNum);	
		vdi = volNum;
		pXtnt->chainVdIndex = vdi;
		pageZeroModified = TRUE;
	}
	page_to_read = pXtnt->chainMCId.page;
	mcellNum = pXtnt->chainMCId.cell;
	prevPage = pXtnt->firstXtnt.bsXA[xCnt-1].bsPage; 
    }	
       
    while(vdi)
    {
	if ( page_to_read !=0 ) {
		if(odsVersion <= 3) {
			/*--Should always be 0 for DVN3--*/
			page_to_read = 0;
			modified = TRUE;
		}
		else {
			status = convert_page_to_lbn(volume->volRbmt->rbmtLBN,volume->volRbmt->rbmtLBNSize,page_to_read,&rbmtLbn,&vdi);
			if (SUCCESS != status) {
                        writemsg(SV_ERR | SV_LOG_ERR,
                                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_598,
                                         "Can't convert RBMT page %d to LBN\n"),
                                 pXtnt->chainMCId.page);
                        return FAILURE;
                    }
 
		}
	}
	if ( page_to_read == 0)
	{
		tempPage = pPage;
	}		
 	else {
		status = read_page_from_cache(vdi,rbmtLbn,&tempPage);
		if (SUCCESS != status) {
               		return status;
		}
	}	
	/*--check if the mcell record is BSR_XTRA_XTNTS--*/
	pBMTPage = (bsMPgT *)tempPage->pageBuf;
	Mcell = &(pBMTPage->bsMCA[mcellNum]); 
	if( Mcell->tag.num !=  tagNum) {
		status = add_fixed_message(&(tempPage->messages), RBMT,
                                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_552,
                                         "Modified RBMT/BMT0 SBM mcell's tag number from %d to %d.\n"),
                                 Mcell->tag.num, tagNum);
     		if (SUCCESS != status) {
            		return status;
        	}
        	Mcell->tag.num = tagNum;
        	modified = TRUE;
    	}
	sRecord = (bsMRT *)Mcell->bsMR0;
	sXtnt = (bsXtraXtntRT *) ((char *)sRecord + sizeof(bsMRT));
	sCnt = sXtnt->xCnt;

	/*-- call correct_bsr_xtra_xtnts_record for the basic checks --*/
				

	status = correct_bsr_xtra_xtnts_record(domain,volNum,tempPage,mcellNum,sRecord);
	if ( status == FIXED )
		modified = TRUE;
	else if( status != SUCCESS)
	{
		writemsg(SV_ERR | SV_LOG_ERR,
                         catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_801,
                                 "Error correcting the SBM.\n"));
		return status;
	}
		
	status = test_sbm_xtnt_array(sCnt,sXtnt->bsXA,vdi,prevPage,&modified,tempPage,volume );
		
	if ( status == FAILURE )
	{
		/* Something more needs to be done here */ 
		/*-release page-*/
		status = release_page_to_cache(vdi,rbmtLbn,tempPage,modified,FALSE);		
		if (status != SUCCESS) {
                       	return status;
                }
		return FAILURE;
	}
			
	prevPage = sXtnt->bsXA[sCnt-1].bsPage;
	if (page_to_read != 0)
	{
		/*--dont release page 0 as we didnot lock it--*/
		status = release_page_to_cache(vdi,rbmtLbn,tempPage,modified,FALSE);
		if (status != SUCCESS) {
			return status;
		}
	}
	if ( Mcell->nextVdIndex != 0 )
	{
			vdi =  Mcell->nextVdIndex;
			if ( vdi != volNum)
        		{
                
				status = add_fixed_message( &tempPage->messages, RBMT,
                                   			    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_938,
								    "Modified extent's next volume Index from %d to %d.\n"),		
			 					     vdi,volNum);	
				vdi = volNum;
				Mcell->nextVdIndex = vdi;
				if( page_to_read == 0 )
					pageZeroModified = TRUE;
				else
					modified = TRUE;	
				
        		}

			mcellNum = Mcell->nextMCId.cell;
			page_to_read = Mcell->nextMCId.page;
	}		
	
	else {
			vdi = 0;
			/*--check if the last xtnt is totalSbmPages--*/
			if(sXtnt->bsXA[sCnt-1].bsPage != totalSbmPages) {
				status = add_fixed_message( &tempPage->messages, RBMT,
                                   			    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_937,
                    							"Modified total number of SBM pages from %d to %d.\n"),
                                   			   	    sXtnt->bsXA[sCnt-1].bsPage,
                                  				    totalSbmPages);
        			if ( SUCCESS != status ) {
            				return status;
        			}
				sXtnt->bsXA[sCnt-1].bsPage = totalSbmPages; 
				modified = TRUE;
			}
	}

    } 

    if ( pageZeroModified == TRUE || modified == TRUE ) {
		return FIXED;
    }

    return SUCCESS;
}

/*
 * Function Name: correct_rbmt_root_tag_mcell (3.25)
 *
 * Description: This function will check mcell 2 of the RBMT. It will fix 
 *              and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     domain: The domain we are working on.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters:
 *     rootTagVolume: Does this volume have a root tag file?
 *
 * Returns: SUCCESS, FIXED, CORRUPT
 */
int correct_rbmt_root_tag_mcell(pageT *pPage, domainT *domain,
				int volNum, int *rootTagVolume)
{
    char   *funcName = "correct_rbmt_root_tag_mcell";
    int    status;
    int    modified;   /* Has the page has been modified. */
    int    x;
    int    mcellNum;
    int    tagNum;
    int    reqServices;
    LBNT   lbn;
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;   /* The current mcell record we are working on */
    bsBfAttrT   *pBSRAttr;
    bsXtntRT    *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = 2;
    tagNum   = -(mcellNum + (6 * volNum));
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));
    *rootTagVolume = FALSE;
    reqServices = 0;

    if (pMcell->tag.num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_570, 
					   "Modified RBMT/BMT0 root tag mcell's tag number from %d to %d.\n"),
				   pMcell->tag.num, tagNum);
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = tagNum;
	modified = TRUE;
    }

    /*
     * this field is now set to 0 or DEF_SVC_CLASS.
     * It is set to '0' if the volume was added via 'mkfdmn', and set to
     * DEF_SVC_CLASS if added via 'addvol'.  We have no way to know which
     * way the volume was added. We keep the value as DEF_SVC_CLASS if set
     * to that, otherwise we set it to 0.
     * 
     * In prior releases it was always set to 0. 
     *          logIsoV3_nowrite has this set as "1", this is a ODSv3
     *          domain.  This means that older release can also have 
     *          multiple value:
     */
    if (DEF_SVC_CLASS == pBSRAttr->cl.reqServices) {
	reqServices = DEF_SVC_CLASS;
    }

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_bsr_attr_record (pPage, pRecord, 
					   mcellNum, 2, reqServices);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_bsr_xtnts_default_values (pPage, pRecord, 
					       mcellNum, 80);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */

    /*	
     * If xCnt = 1, then roottag not on volume.
     */
    if (2 != pXtnt->firstXtnt.xCnt) {
        *rootTagVolume = FALSE;

        if ((0 != pXtnt->firstXtnt.bsXA[0].bsPage) ||
	    (-1 != pXtnt->firstXtnt.bsXA[0].vdBlk)) {
	    pageT *pTempPage;

	    lbn = pXtnt->firstXtnt.bsXA[0].vdBlk;
	    /*
	     * We have an extent record, but xCnt says we shouldn't.
	     */
	    status = read_page_from_cache(volNum, lbn, &pTempPage);
	    if (SUCCESS != status) {
	        return status;
	    }

	    status = validate_tag_file_page(pTempPage, 0);
	    if (SUCCESS != status) {
	        /*
		 * This is not a tag file page, extent is corrupt..
		 */
		pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		pXtnt->firstXtnt.bsXA[0].vdBlk  = -1;
	    } else {
	        /*
		 * xCnt is corrupt.
		 */
	        pXtnt->firstXtnt.xCnt = 2;
		*rootTagVolume = TRUE;
	    }
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_580, 
					       "Modified the root tag extent map.\n"));
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = TRUE;	

	    status = release_page_to_cache(volNum, lbn,
					   pTempPage, FALSE, FALSE);
	    if (SUCCESS != status) {
	        return status;
	    }
        }
    } else {
	pageT *pTempPage;

        *rootTagVolume = TRUE;

	lbn = pXtnt->firstXtnt.bsXA[0].vdBlk;
	/*
	 * Check to see if the page is actually a tag page.
	 */
	status = read_page_from_cache(volNum, lbn, &pTempPage);
	if (SUCCESS != status) {
	    return status;
	}

	status = validate_tag_file_page(pTempPage, 0);
	if (SUCCESS != status) {
	    /*
	     * This is not a tag file page, xCnt is corrupt..
	     */
	    *rootTagVolume = FALSE;
	    pXtnt->firstXtnt.xCnt = 1;

	    pXtnt->firstXtnt.bsXA[0].bsPage = 0;
	    pXtnt->firstXtnt.bsXA[0].vdBlk  = -1;
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_580, 
					       "Modified the root tag extent map.\n"));
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = TRUE;
	}

	status = release_page_to_cache(volNum, lbn,
				       pTempPage, FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
    } 

    /*
     * Chain will be checked later in the program.
     */

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 

    /*
     * This mcell should have no more records.
     */
    if (0 != pRecord->type) {
        pRecord->type = 0;
	pRecord->bCnt = 0;
	modified = TRUE;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_582, 
					   "Deleted records from RBMT/BMT0 root tag mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_583,
			 "Found corruptions in RBMT/BMT0 root tag mcell.\n"));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_584, 
					   "Modified RBMT/BMT0 root tag mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_root_tag_mcell */


/*	
 * Function Name: correct_rbmt_log_mcell (3.26)
 *
 * Description: This function will check mcell 3 of the RBMT. It will fix
 *              and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *     logVolume: A link list of volumes with log extents.
 *
 * Output parameters: 
 *     logVolume: A link list of volumes with log extents.
 *
 * Returns: SUCCESS, FIXED, CORRUPT
 */
int correct_rbmt_log_mcell(pageT *pPage, int volNum, 
			   domainT *domain, logVolT **logVolume)
{
    char   *funcName = "correct_rbmt_log_mcell";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;   /* The current mcell record we are working on */
    int    modified;   /* Flag to let us know if the page has been modified. */
    int    x;
    int    mcellNum;
    int    tagNum;
    int    status;
    int    logOnVolume;
    int    reqServices;
    LBNT   lbn;
    bsBfAttrT *pBSRAttr;
    bsXtntRT  *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    logOnVolume = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = 3;
    tagNum   = -(mcellNum + (6 * volNum));
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    if (pMcell->tag.num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_585,
					   "Modified RBMT/BMT0 log mcell's tag number from %d to %d.\n"),
				   pMcell->tag.num, tagNum);
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = tagNum;
	modified = TRUE;
    }

    /*
     * LOG_SVC_CLASS, in prior releases it was always set to 0.  
     *
     *          logIsoV3_nowrite has this set as "2", this is a ODSv3
     *          domain.  This means that older release can also have 
     *          multiple value:
     */
    reqServices = 0;
    if (LOG_SVC_CLASS == pBSRAttr->cl.reqServices) {
	reqServices = LOG_SVC_CLASS;
    }
   
    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_bsr_attr_record (pPage, pRecord, 
					   mcellNum, 1, reqServices);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_bsr_xtnts_default_values (pPage, pRecord, 
					       mcellNum, 80);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */
    logOnVolume = TRUE;

    if (1 == pXtnt->firstXtnt.xCnt) {
        logOnVolume = FALSE;

        /*
         * verify we don't have an extent.
         */
        if ((0 != pXtnt->firstXtnt.bsXA[0].bsPage) ||
	    (-1 != pXtnt->firstXtnt.bsXA[0].vdBlk)) {
	    pageT  *pTempPage;
	    bsMPgT *pTempBMTPage;

	    lbn = pXtnt->firstXtnt.bsXA[0].vdBlk;
	    /* 
	     * We have an extent record, but xCnt says we shouldn't.	
	     * Either xCnt is wrong or the extent is wrong.
	     * So lets check the page the xtnt points to and see if it 
	     * is a log page.	
	     */
	    status = read_page_from_cache(volNum, lbn, &pTempPage);
	    if (SUCCESS != status) {
	        return status;
	    }

	    pTempBMTPage = (bsMPgT *)pPage->pageBuf;

	    status = validate_log_file_page(domain, pTempPage);
	    if (SUCCESS != status) {
	        /*
		 * This is not a log file page, extent is corrupt..
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_595, 
						   "Truncated the log file extent.\n"));
		if (SUCCESS != status) {
		    return status;
		}
		pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		pXtnt->firstXtnt.bsXA[0].vdBlk  = -1;
	    } else {
	        /*
		 * xCnt is corrupt.
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_596, 
						   "Modified the log file extent count from %d to %d.\n"),
					   pXtnt->firstXtnt.xCnt, 2);
		if (SUCCESS != status) {
		    return status;
		}
	        pXtnt->firstXtnt.xCnt = 2;
		logOnVolume = TRUE;
	    }
	    modified = TRUE;	

	    status = release_page_to_cache(volNum, lbn, 
					   pTempPage, FALSE, FALSE);
	    if (SUCCESS != status) {
	        return status;
	    }
	} /* end if we have an extent */
    } /* end if pXtnt->firstXtnt.xCnt == 1 */

    if (1 == pXtnt->firstXtnt.xCnt) {
        /*
	 * we could have had it change in the last if block, so check again.
	 */
        logOnVolume = FALSE;

        if ((1 != pXtnt->firstXtnt.mcellCnt) ||
	    (0 != pXtnt->chainVdIndex)      ||
	    (0 != pXtnt->chainMCId.page)    ||
	    (0 != pXtnt->chainMCId.cell)) {
	    /*	
	     * As we don't have any extents we shouldn't have a chain.
	     */
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_597, 
					       "Modified RBMT/BMT0 log mcell's chain from (%d,%d,%d) to (%d,%d,%d).\n"),
				       pXtnt->chainVdIndex,
				       pXtnt->chainMCId.page,
				       pXtnt->chainMCId.cell,
				       0, 0, 0);
	    if (SUCCESS != status) {
		return status;
	    }

	    pXtnt->chainVdIndex   = 0;
	    pXtnt->chainMCId.page = 0;
	    pXtnt->chainMCId.cell = 0;
	    pXtnt->firstXtnt.mcellCnt = 1;
	    modified = TRUE;
	}
    } else if ((1 != pXtnt->firstXtnt.xCnt) &&  
	       (1 == pXtnt->firstXtnt.mcellCnt)) {
        /*
	 * We have a log file with a single extent.
	 */

        if ((0 != pXtnt->chainVdIndex)   ||
	    (0 != pXtnt->chainMCId.page) ||
	    (0 != pXtnt->chainMCId.cell)) {
	    /*	
	     * Either the chain is incorrect or mcellCnt is.
	     * As we don't have the RBMT extents yet we can only check
	     * the chain if it points to this page.  We can check if the
	     * volNum is correct.
	     */
	    if ((volNum != pXtnt->chainVdIndex) ||
		(pXtnt->chainMCId.cell > BSPG_CELLS)) {
	        /*
		 * We have a corruption in the chain, so assume chain is
		 * the corruption.
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_597, 
						   "Modified RBMT/BMT0 log mcell's chain from (%d,%d,%d) to (%d,%d,%d).\n"),
					   pXtnt->chainVdIndex,
					   pXtnt->chainMCId.page,
					   pXtnt->chainMCId.cell,
					   0, 0, 0);
		if (SUCCESS != status) {
		    return status;
		}
	        pXtnt->chainVdIndex   = 0;
		pXtnt->chainMCId.page = 0;
		pXtnt->chainMCId.cell = 0;
		modified = TRUE;
	    } else {
	        bsMCT  *pTempMcell;
		pageT  *pTempPage;
		bsMPgT *pTempBMTPage;
		LBNT   tmpLbn;
		int    tmpVol;

		if (0 == pXtnt->chainMCId.page) {
		    /*
		     * The mcell falls on this page so lets check it.
		     */
		    pTempMcell = &(pBMTPage->bsMCA[pXtnt->chainMCId.cell]);
		} else {
		    pagetoLBNT *rbmtLbn;
		    int        size;

		    /*
		     * mcell falls on another RBMT page.
		     */
		    rbmtLbn = domain->volumes[volNum].volRbmt->rbmtLBN;
		    size    = domain->volumes[volNum].volRbmt->rbmtLBNSize;

		    status = convert_page_to_lbn(rbmtLbn,
						 size,
						 pXtnt->chainMCId.page,
						 &tmpLbn, 
						 &tmpVol);
		    if (SUCCESS != status) {
		        writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_598,
					 "Can't convert RBMT page %d to LBN\n"), 
				 pXtnt->chainMCId.page);
			return FAILURE;
		    }
	
		    status = read_page_from_cache(tmpVol, tmpLbn, &pTempPage);
		    if (SUCCESS != status) {
		        return status;
		    }
		    pTempBMTPage = (bsMPgT *)pPage->pageBuf;
		    pTempMcell   = &(pTempBMTPage->bsMCA[pXtnt->chainMCId.cell]);		
		} /* end if on a different page */

		if (pTempMcell->tag.num != tagNum) {
		    /*
		     * mcell does not belong to log file, So chain is corrupt.
		     */
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_597, 
						       "Modified RBMT/BMT0 log mcell's chain from (%d,%d,%d) to (%d,%d,%d).\n"),
					       pXtnt->chainVdIndex,
					       pXtnt->chainMCId.page,
					       pXtnt->chainMCId.cell,
					       0, 0, 0);
		    if (SUCCESS != status) {
			return status;
		    }
		    pXtnt->chainVdIndex   = 0;
		    pXtnt->chainMCId.page = 0;
		    pXtnt->chainMCId.cell = 0;
		    modified = TRUE;
		} else {
		    /*
		     * The mcell pointed at by the chain is part of the log.
		     * Set it to 2.  Later when we follow the chain we will
		     * verify this value.
		     */
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_599, 
						       "Modified RBMT/BMT0 log mcell's mcellCnt from %d to %d.\n"),
					       pXtnt->firstXtnt.mcellCnt,
					       2);
		    if (SUCCESS != status) {
			return status;
		    }
		    pXtnt->firstXtnt.mcellCnt = 2;
		    modified = TRUE;
		} /* end if tag.num incorrect */
		
		if (0 != pXtnt->chainMCId.page) {
		    status = release_page_to_cache(tmpVol, tmpLbn,
						   pTempPage, FALSE, FALSE);
		    if (SUCCESS != status) {
		        return status;
		    }
		} /* end if page != 0 */
	    } /* end if chain falls on this page */
	} /* end if valid chain */
    } /* if/else xcnt */

    /*
     * If a log does exist on this volume then add an entry to our list.
     */
    if (TRUE == logOnVolume) {
        logVolT *tmp;
        logVolT *head;

        writemsg(SV_DEBUG | SV_LOG_INFO, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_601, 
						 "Found the transaction log on volume %s.\n"),
		 domain->volumes[volNum].volName);

	tmp = (logVolT *)ffd_malloc(sizeof(logVolT));
	tmp->volume = volNum;
	tmp->next = NULL;

	if (NULL == *logVolume) {
	    *logVolume = tmp;
	} else {
	    head = *logVolume;
	    while (NULL != head->next) {
	        head = head->next;
	    }
	    head->next = tmp;
	}
    } else {
        writemsg(SV_DEBUG, catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_602,
				   "Unable to find the transaction log on volume %s.\n"),
		 domain->volumes[volNum].volName);
    } /* end if log exists on volume */
    
    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 
    /*
     * This mcell should have no more records.
     */
    if (0 != pRecord->type) {
        pRecord->type = 0;
	pRecord->bCnt = 0;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_603, 
					   "Deleted records from RBMT/BMT0 log mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_604, 
			 "Found corruptions in RBMT/BMT0 log mcell.\n"));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_605, 
					   "Modified RBMT/BMT0 log mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_log_mcell */


/*
 * Function Name: correct_rbmt_primary_bmt (3.27)
 *
 * Description: This function will check mcell 4 of the RBMT which is the 
 *              second mcell.  Will only be called on V4 ODS domains.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
int correct_rbmt_primary_bmt(pageT *pPage, int volNum)
{
    char   *funcName = "correct_rbmt_primary_bmt";
    bsMPgT *pBMTPage;/* The current RBMT page we are working on */
    bsMCT  *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT  *pRecord; /* The Current mcell record we are working on */
    int    modified; /* Flag to let us know if the page has been modified. */
    int    status;
    int    mcellNum;
    int    tagNum;
    int    x;
    bsBfAttrT   *pBSRAttr;
    bsXtntRT    *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = 4;
    tagNum   = -(mcellNum + (6 * volNum));
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    if (pMcell->tag.num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_606,
					   "Modified RBMT/BMT0 primary BMT mcell's tag number from %d to %d.\n"),
				   pMcell->tag.num, tagNum);
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = tagNum;
	modified = TRUE;
    }

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_bsr_attr_record (pPage, pRecord,
					   mcellNum, 2, 0);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    status = correct_bsr_xtnts_default_values (pPage, pRecord, 
					       mcellNum, 80);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */
    if (2 != pXtnt->firstXtnt.xCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_617, 
					   "Modified RBMT/BMT0 primary BMT mcell's xCnt from %d to %d.\n"),
				   pXtnt->firstXtnt.xCnt, 2);
	if (SUCCESS != status) {
	    return status;
	}
        pXtnt->firstXtnt.xCnt = 2;
	modified = TRUE;
    }

    /*
     * Newly created domains will not have additional BMT pages
     * so the chain will be 0.  Once the mcells in the first page
     * of the bmt is filled the chain will point to mcell 7.  We 
     * could not come up with any exceptions.
     */
    if ((volNum != pXtnt->chainVdIndex) ||
	(0 != pXtnt->chainMCId.page)    ||
	((7 != pXtnt->chainMCId.cell)   && 
	 (0 != pXtnt->chainMCId.cell))) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_618, 
					   "Modified RBMT/BMT0 primary BMT mcell's chain from (%d,%d,%d) to (%d,%d,%d).\n"),
				   pXtnt->chainVdIndex,
				   pXtnt->chainMCId.page,
				   pXtnt->chainMCId.cell,
				   volNum, 0, 7);
	if (SUCCESS != status) {
	    return status;
	}
        pXtnt->chainVdIndex   = volNum;
	pXtnt->chainMCId.page = 0;
	pXtnt->chainMCId.cell = 7;
	modified = TRUE;
    }

    /*
     * Can only have a 1 page extent which starts at BLK 48.
     */
    if ((0 != pXtnt->firstXtnt.bsXA[0].bsPage) ||
	(48 != pXtnt->firstXtnt.bsXA[0].vdBlk) ||
	(1 != pXtnt->firstXtnt.bsXA[1].bsPage) ||
	(-1 != pXtnt->firstXtnt.bsXA[1].vdBlk)) {
        pXtnt->firstXtnt.bsXA[0].bsPage = 0;
	pXtnt->firstXtnt.bsXA[0].vdBlk  = 48;
	pXtnt->firstXtnt.bsXA[1].bsPage = 1;
	pXtnt->firstXtnt.bsXA[1].vdBlk  = -1;
	status = add_fixed_message(&(pPage->messages), RBMT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_619, 
			 "Modified RBMT/BMT0 primary BMT mcell's extents.\n"));
	if (SUCCESS != status) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 

    /*
     * This mcell should have no more records.
     */
    if (0 != pRecord->type) {
        pRecord->type = 0;
	pRecord->bCnt = 0;
	modified = TRUE;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_621, 
					   "Deleted records from RBMT/BMT0 primary BMT mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_622, 
			 "Found corruptions in RBMT/BMT0 primary BMT mcell.\n"));

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_623,
					   "Modified RBMT/BMT0 primary BMT mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_primary_BMT */


/*
 * Function Name: correct_rbmt_vol_dom_mcell (3.28)
 *
 * Description: This function will check mcell 6 of the RBMT, or mcell 4 of 
 *              the BMT0.  It will fix and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     isRBMT: Flag to let us know if this is RBMT or BMT0.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *     mattrVol: Which volume has current mattr
 *     mattrSeq: Current sequence number of mattr
 *
 * Output parameters: 
 *     mattrVol: Which volume has current mattr
 *     mattrSeq: Current sequence number of mattr
 *     bfSetDirVol: Which volume has current root tag file
 *
 * Returns: SUCCESS, FIXED
 */
int correct_rbmt_vol_dom_mcell(pageT *pPage, int isRBMT, int volNum, 
			       domainT *domain, int *mattrVol, int *mattrSeq,
			       int *bfSetDirVol)
{
    char    *funcName = "correct_rbmt_vol_dom_mcell";
    bsMPgT  *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT   *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT   *pRecord; /* The Current mcell record we are working on */
    int     foundDmnTransAttr;
    int     foundDmnMAttr;
    int     foundSSRecord;
    int     foundVdRecord;
    int     foundFreezeRecord;
    int     modified; /* Flag to let us know if the page has been modified.*/
    int     mcellNum;
    int     tagNum;
    int     status;
    volumeT *volume;
    struct div_t logPages;
    bsVdAttrT     *pVdAttr;
    bsDmnAttrT    *pDmnAttr;
    bsDmnMAttrT   *pDmnMAttr;
    char   errorMsg[MAXERRMSG];

    /*
     * Init variables
     */
    volume = &(domain->volumes[volNum]);
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;

    if (TRUE == isRBMT) {
	mcellNum = 6;
    } else {
        mcellNum = 4;
    }
    tagNum = -(6 * volNum);
    pMcell = &(pBMTPage->bsMCA[mcellNum]);

    /*
     * In this mcell, we expect to find the following records:
     *		BSR_VD_ATTR (Required)
     *		BSR_DMN_ATTR (Required)
     *		BSR_DMN_MATTR (Optional)
     *		BSR_DMN_TRANS_ATTR (Optional)
     *
     * NOTE: BSR_DMN_SS_ATTR, BSR_VD_IO_PARAMS, BSR_DMN_FREEZE_ATTR
     *       are records that are in this chain.  They should ALL be
     *       in the first mcell in the chain and not located here.
     *       
     *       If the record is both mcells it will ignore the second one
     *       so no reason not to check if the data is valid.
     */
    foundDmnMAttr     = FALSE;
    foundDmnTransAttr = FALSE;
    foundSSRecord     = FALSE;
    foundVdRecord     = FALSE;
    foundFreezeRecord = FALSE;

    if (pMcell->tag.num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_624, 
					   "Modified RBMT/BMT0 vol/dom mcell's tag number from %d to %d.\n"),
				   pMcell->tag.num, tagNum);
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = tagNum;
	modified = TRUE;
    }

    /*
     * This record must be of type BSR_VD_ATTR
     */
    pRecord  = (bsMRT *)pMcell->bsMR0;

    if (BSR_VD_ATTR != pRecord->type) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_625, 
					   "Modified RBMT/BMT0 vol/dom mcell's record type from %d to BSR_VD_ATTR.\n"),
				   pRecord->type);
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->type = BSR_VD_ATTR;
	modified = TRUE;
    }

    /* (sizeof(bsMRT) + sizeof(bsVdAttrT)) == 40 */
    if ((sizeof(bsMRT) + sizeof(bsVdAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_626, 
					   "Modified RBMT/BMT0 vol/dom mcell's record bCnt from %d to %d.\n"),
				   pRecord->bCnt,
				   (sizeof(bsMRT) + sizeof(bsVdAttrT)));
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsVdAttrT));
	modified = TRUE;
    }
    pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check fields that have known values.
     */
    if ((pVdAttr->state < BSR_VD_VIRGIN) || (pVdAttr->state > BSR_VD_ZOMBIE)) {
        /*
	 * Value is out side of valid range, set to the mounted state.
	 */
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_627, 
					   "Modified RBMT/BMT0 vol/dom mcell's state from %d to BSR_VD_MOUNTED.\n"),
				   pVdAttr->state);
	if (SUCCESS != status) {
	    return status;
	}
	pVdAttr->state = BSR_VD_MOUNTED;
	modified = TRUE;
    }
	
    if (pVdAttr->vdIndex != volNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_628,
					   "Modified RBMT/BMT0 vol/dom mcell's vdIndex from %d to %d.\n"),
				   pVdAttr->vdIndex, volNum);
	if (SUCCESS != status) {
	    return status;
	}
        pVdAttr->vdIndex = volNum;
	modified = TRUE;
    }

    if (BLOCKS_PER_PAGE != pVdAttr->maxPgSz) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_629, 
					   "Modified RBMT/BMT0 vol/dom mcell's maxPgSz from %d to %d.\n"),
				   pVdAttr->maxPgSz, BLOCKS_PER_PAGE);
	if (SUCCESS != status) {
	    return status;
	}
        pVdAttr->maxPgSz = BLOCKS_PER_PAGE;
	modified = TRUE;
    }

    /*
     * Service class was 1 before.  Now it can be 2 or 3.
     *
     * If you run log isolation on either a ODSv3/v4 domain it will change 
     * the value to 2.
     */
    if (2 == pVdAttr->serviceClass) {
	int foundLogIsolation;
	int foundMiscMcellRecord;
	int newServiceClass;
	bsMCT *pMiscMcell;  /* The misc mcell  */
	bsMRT *pMiscRecord; /* The mcell record we are working on */
	bsXtntRT *pXtnt;

	/*
	 * Special case. The log MAY have been isolated on this
	 * volume.  What we do know is that if log isolation has
	 * been done on this domain then you will have a 'misc'
	 * mcell chain.  So we can look to see if the chain exists.
	 *
	 * For ODSv4 we have already processed this mcell, so we know
	 * we will find it in a correct state.
	 *
	 * For ODSv3 we have not processed this mcell, so if we do
	 * not find the BSR_XTNT treat it as this log has NOT been
	 * isolated.  As when we recreate it we will not be setting
	 * it up isolated.
	 */
	foundLogIsolation = FALSE;
	pMiscMcell = &(pBMTPage->bsMCA[5]);
	pMiscRecord  = (bsMRT *)pMiscMcell->bsMR0;

	while (0 != pMiscRecord->type) {
	    if (BSR_XTNTS == pMiscRecord->type) {
		pXtnt = (bsXtntRT *)((char *)pMiscRecord + sizeof(bsMRT));
		if (0 != pXtnt->chainVdIndex) {
		    /*
		     * Has a chain, so assume it has been isolated
		     */
		    foundLogIsolation = TRUE;
		}
	    } /* If the record we want */
	    /*
	     * Set pRecord to next record of pMcell.
	     */
	    pMiscRecord = (bsMRT *) (((char *)pMiscRecord) + 
				     roundup(pMiscRecord->bCnt,
					     sizeof(int))); 
	} /* End while ((0 != pMiscRecord->type) && ... */
	
	if (FALSE == foundLogIsolation) {
	    /*
	     * This volume does NOT have log isolation, so serviceClass
	     * is incorrect.  Set it to the value that makes the most
	     * sense on its domain version.
	     *
	     * logiso3 found newServiceClass set to 3 on ODSv3
	     */
	    if (3 == domain->dmnVersion) {
		newServiceClass = 1;
	    } else {
		newServiceClass = 3;
	    }

	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_630, 
					       "Modified RBMT/BMT0 vol/dom mcell's serviceClass from %d to %d.\n"),
				       pVdAttr->serviceClass, 
				       newServiceClass);
	    if (SUCCESS != status) {
		return status;
	    }
	    pVdAttr->serviceClass = newServiceClass;
	    modified = TRUE;
	}
    } else {
	/*
	 * we need a way to decide what the value of the
	 * service class should be.  NOTE: 2 has already been handled.
	 *
	 * This effects ODSv3 domains as well.
	 */
	if ((1 != pVdAttr->serviceClass) &&
	    (3 != pVdAttr->serviceClass)) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_630, 
					       "Modified RBMT/BMT0 vol/dom mcell's serviceClass from %d to %d.\n"),
				       pVdAttr->serviceClass, 3);
	    if (SUCCESS != status) {
		return status;
	    }
	    /*
	     * Switching to '3' will not cause problems on a
	     * domain, but we only change it to 3 if the value is not
	     * 1, 2 or 3
	     */
	    pVdAttr->serviceClass = 3;
	    modified = TRUE;
	}
    }

    if (TRUE == isRBMT) {
        if (16 != pVdAttr->stgCluster) {
	    /*
	     * Domains created in a BETA V5.0 release could have 
	     * stgCluster equal to 2. This is a very RARE case.
	     */
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_631,
					       "Modified RBMT/BMT0 vol/dom mcell's stgCluster from %d to %d.\n"),
				       pVdAttr->stgCluster, 16);
	    if (SUCCESS != status) {
		return status;
	    }
	    pVdAttr->stgCluster = 16;
	    modified = TRUE;
	}
    } else {
        if (2 != pVdAttr->stgCluster) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_631, 
					       "Modified RBMT/BMT0 vol/dom mcell's stgCluster from %d to %d.\n"),
				       pVdAttr->stgCluster, 2);
	    if (SUCCESS != status) {
		return status;
	    }
	    pVdAttr->stgCluster = 2;
	    modified = TRUE;
	}
    }

    assert (0 != volume->volSize);
    if( ( pVdAttr->vdBlkCnt != volume->volSize) && ( volume->volSize == MAXUINT ) ) {
	writemsg(SV_ERR |SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_943,"Partition size is too large;More than the max Advfs volume size\n"));
	if ( pVdAttr->vdBlkCnt < volume->volSize ) {
		volume->volSize = pVdAttr->vdBlkCnt;	
	}
    }			
    else if ( pVdAttr->vdBlkCnt < volume->volSize ) {
	writemsg(SV_ERR |SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_940,"The advfs volume size is less than the volume/disk partition size.\n"));
	writemsg(SV_ERR |SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_941,"Please use mount -u -o extend command to extend the AdvFS volume.\n"));
	writemsg(SV_ERR |SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_942,"Currently using the Advfs Volume size of %u\n"),pVdAttr->vdBlkCnt);	
	volume->volSize = pVdAttr->vdBlkCnt;	
    }
    else if (pVdAttr->vdBlkCnt >  volume->volSize) {
        char answer[10];  /* buffer for user input */

	writemsg(SV_ERR | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_944,
			 "The %s AdvFs volume size of %u does not match its partition/volume size of %u.\n"),
		 volume->volName, pVdAttr->vdBlkCnt,volume->volSize);
	writemsg(SV_ERR | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_945,
			 "If the partition/volume size is incorrect, you must exit %s now and use the disklabel utility or volassist command to change %s's size to %u.\n"),
		Prog, volume->volName, pVdAttr->vdBlkCnt);
	writemsg(SV_ERR | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_939,
			 "If lsm volume was extended,in order to extend the AdvFs volume, use mount -u -o extend command.\n"));	
	writemsg(SV_ERR | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_919,
			 "Do you wish to exit %s now? ([y]/n)  "), Prog);

	status = prompt_user_yes_no(answer, "y");
	if (SUCCESS != status) {
	    return status;
	}

	if ('y' == answer[0]) {
	    /*
	     * User wants to exit.
	     */
	    sprintf(errorMsg,
		    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_920,
			    "Exit requested, try using 'disklabel' to change %s's size to %u.\n"),
		    volume->volName, volume->volSize);
	    corrupt_exit(errorMsg);
	}

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_632, 
					   "Modified RBMT/BMT0 vol/dom mcell's volSize from %u to %u.\n"),
				   pVdAttr->vdBlkCnt, volume->volSize);
	if (SUCCESS != status) {
	    return status;
	}
        pVdAttr->vdBlkCnt = volume->volSize;
	modified = TRUE;
    }

    if (0 == pVdAttr->bmtXtntPgs) {
        /*
	 * Only value which is a problem.
         */
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_633, 
					   "Modified RBMT/BMT0 vol/dom mcell's bmtXtntPgs from %d to %d.\n"),
				   pVdAttr->bmtXtntPgs, 128);
	if (SUCCESS != status) {
	    return status;
	}
        pVdAttr->bmtXtntPgs = 128; /* default value */
	modified = TRUE;
    }

    volume->mountId.tv_sec = pVdAttr->vdMntId.tv_sec;
    volume->mountId.tv_usec = pVdAttr->vdMntId.tv_usec;
    volume->mntStatus = pVdAttr->state;

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 

    /*
     * This record must be of type BSR_DMN_ATTR
     */
    if (BSR_DMN_ATTR != pRecord->type) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_634,
					   "Modified RBMT/BMT0 vol/dom mcell's record type from %d to BSR_DMN_ATTR.\n"),
				   pRecord->type);
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->type = BSR_DMN_ATTR;
	modified = TRUE;
    }

    /* (sizeof(bsMRT) + sizeof(bsDmnAttrT)) == 24 */
    if ((sizeof(bsMRT) + sizeof(bsDmnAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_626, 
					   "Modified RBMT/BMT0 vol/dom mcell's record bCnt from %d to %d.\n"),
				   pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsDmnAttrT)));
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnAttrT));
	modified = TRUE;
    }

    pDmnAttr = (bsDmnAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This is an ODS so use BS_MAX_VDI not MAX_VOLUMES
     */
    if (!((1 == pDmnAttr->maxVds) || (BS_MAX_VDI == pDmnAttr->maxVds))){
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_635, 
					   "Modified RBMT/BMT0 vol/dom mcell's record maxVds from %d to BS_MAX_VDI.\n"),
				   pDmnAttr->maxVds);
	if (SUCCESS != status) {
	    return status;
	}
	pDmnAttr->maxVds = BS_MAX_VDI;
	modified = TRUE;
    }

    volume->domainID.tv_sec = pDmnAttr->bfDomainId.tv_sec;
    volume->domainID.tv_usec = pDmnAttr->bfDomainId.tv_usec;

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int)));

    /*
     * At this point we have a bunch of optional records that can be
     * in any order.
     */
    while (0 != pRecord->type) {
	switch (pRecord->type) {
	    case (BSR_DMN_MATTR):
		if (TRUE == foundDmnMAttr) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundDmnMAttr) */

		/* (sizeof(bsMRT) + sizeof(bsDmnMAttrT)) == 52 */
		if ((sizeof(bsMRT) + sizeof(bsDmnMAttrT)) != pRecord->bCnt) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_626, 
						       "Modified RBMT/BMT0 vol/dom mcell's record bCnt from %d to %d.\n"),
					       pRecord->bCnt, (sizeof(bsMRT) + sizeof(bsDmnMAttrT)));
		    if (SUCCESS != status) {
			return status;
		    }
		    pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnMAttrT));
		    modified = TRUE;
		}

		pDmnMAttr = (bsDmnMAttrT *)((char *)pRecord + sizeof(bsMRT));

		if (0 == pDmnMAttr->seqNum) {
		    /*
		     * No way to know the correct number, so at least set
		     * it to a valid number.  We may end up using an old
		     * mattr, but there is nothing we can do about that.
		     *
		     * We handle duplicate seqNum elsewere/
		     */
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_637,
						       "Modified RBMT/BMT0 vol/dom mcell's seqNum from %d to %d.\n"),
					       pDmnMAttr->seqNum, 1);
		    if (SUCCESS != status) {
			return status;
		    }
		    pDmnMAttr->seqNum = 1;
		    modified = TRUE;
		}
	
		if (0 != pDmnMAttr->recoveryFailed) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_638, 
						       "Modified RBMT/BMT0 vol/dom mcell's recoveryFailed from %d to %d.\n"),
					       pDmnMAttr->recoveryFailed, 0);
		    if (SUCCESS != status) {
			return status;
		    }
		    pDmnMAttr->recoveryFailed = 0;
		    modified = TRUE;
		}

		/*
		 * we have a fileset that was either being deleted, or
		 * corruption saying a fileset is being deleted.  It will
		 * not hurt to set to 0, and let the user reissue the rmfset
		 * command after the domain is repaired.
		 */
		if (0 != pDmnMAttr->delPendingBfSet.num) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_639, 
						       "Modified RBMT/BMT0 vol/dom mcell's delPendingBfSet from %d.%x to %d.%x.\n"),
					       pDmnMAttr->delPendingBfSet.num,
					       pDmnMAttr->delPendingBfSet.seq,
					       0, 0);
		    if (SUCCESS != status) {
			return status;
		    }
		    pDmnMAttr->delPendingBfSet.num = 0;
		    pDmnMAttr->delPendingBfSet.seq = 0;
		    modified = TRUE;
		}

		if (pDmnMAttr->ftxLogPgs < 512) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_640, 
						       "Modified RBMT/BMT0 vol/dom mcell's ftxLogPgs from %d to %d.\n"),
					       pDmnMAttr->ftxLogPgs, 512);
		    if (SUCCESS != status) {
			return status;
		    }
		    pDmnMAttr->ftxLogPgs = 512;
		    modified = TRUE;
		}

		logPages = div(pDmnMAttr->ftxLogPgs, 4);

		if (0 != logPages.rem) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_640, 
						       "Modified RBMT/BMT0 vol/dom mcell's ftxLogPgs from %d to %d.\n"),
					       pDmnMAttr->ftxLogPgs, 512);
		    if (SUCCESS != status) {
			return status;
		    }
		    pDmnMAttr->ftxLogPgs = 512;
		    modified = TRUE;
		}

		/* 
		 * The next two will be checked/set later:
		 *
		 * bfSetDirTag = -(2 + (6 * volume number on which located))
		 * ftxLogTag = -(3 + (6 * volume number on which located))
		 *
		 * The following can not be verified, but will not 
		 * cause problems:
		 *
		 *  uid - domain's owner
		 *  gid - domain's group
		 *  mode - domain's access permissions
		 */
	
		if (pDmnMAttr->seqNum > *mattrSeq) {
		    *mattrSeq = pDmnMAttr->seqNum;
		    /*
		     * need to convert ftxLogTag.num to vol num.
		     */ 
		    *mattrVol = ((pDmnMAttr->ftxLogTag.num * -1) - 3) / 6;
		    *bfSetDirVol = ((pDmnMAttr->bfSetDirTag.num * -1) -2) / 6;
		}
		break;

	    case(BSR_DMN_TRANS_ATTR):
		/* 
		 * On newer domains that have added a new record type to
		 * this mcell.  This record is BSR_DMN_TRANS_ATTR.  It
		 * is used to handle the addvol/rmvols which could be in
		 * process when the domain crashed.
		 * 
		 * FUTURE: We may want to set values in this record to 
		 * NULL/-1.  More research is needed on this. 
		 */
		if (TRUE == foundDmnTransAttr) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundDmnTransAttr) */
		foundDmnTransAttr = TRUE;

		/* (sizeof(bsMRT) + sizeof(bsDmnTAttrT)) == 20 */
		if ((sizeof(bsMRT) + sizeof(bsDmnTAttrT)) != pRecord->bCnt) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_626, 
						       "Modified RBMT/BMT0 vol/dom mcell's record bCnt from %d to %d.\n"),
					       pRecord->bCnt, (sizeof(bsMRT) + sizeof(bsDmnTAttrT)));
		    if (SUCCESS != status) {
			return status;
		    }
		    pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnTAttrT));
		    modified = TRUE;
		}
		break;

	    case (BSR_DMN_SS_ATTR) :
		if (TRUE == foundSSRecord) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundSSRecord) */

		foundSSRecord = TRUE;
		status = correct_smart_store_record (pPage, pRecord,
						     mcellNum);
		if (FIXED == status) {
		    modified = TRUE;
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_883,
						       "Corrected vfast record.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		}
		break;

	    case (BSR_VD_IO_PARAMS):
		if (TRUE == foundVdRecord) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundVdRecord) */
		foundVdRecord = TRUE;

		/* (sizeof(bsMRT) + sizeof(vdIoParamsT)) == 28 */
		if ((sizeof(bsMRT) + sizeof(vdIoParamsT)) != pRecord->bCnt) {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_541, 
						       "Modified RBMT/BMT0 BMT mcell's record bCnt from %d to %d.\n"),
					       pRecord->bCnt, (sizeof(bsMRT) + sizeof(vdIoParamsT)));
		    if (SUCCESS != status) {
			return status;
		    }
		    pRecord->bCnt = (sizeof(bsMRT) + sizeof(vdIoParamsT));
		    modified = TRUE;
		}

		/*
		 * The values in VdIoParams can be safely ignored.
		 */
		break;

	    case (BSR_DMN_FREEZE_ATTR) :
		if (TRUE == foundFreezeRecord) {
		    /*
		     * We have a duplicate record.  At this time the
		     * code will simply skip over the record.  Later,
		     * however, it may be desirable to delete the record.
		     */
		    break;
		} /* End if (TRUE == foundFreezeRecord) */
		foundFreezeRecord = TRUE;
		status = correct_freeze_record (pPage, pRecord, mcellNum);
		if (FIXED == status) {
		    modified = TRUE;
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_911,
						       "Corrected domain freeze record.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		}
		break;

	    default:
		/*
		 * Found an unsupported record.  Delete it.
		 * This may not be the best thing to do.  Perhaps we
		 * should consider moving all later records "forward".
		 */
		pRecord->type = 0;
		pRecord->bCnt = 0;
		modified = TRUE;
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_549, 
						   "Deleted records from RBMT/BMT0 BMT mcell.\n"));
		if (SUCCESS != status) {
		    return status;
		}
		break;
	} /* end switch */

	/*
	 * Set pRecord to next record of pMcell.
	 */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 
    } /* End while ((0 != pRecord->type) && ... */

    /*
     * This mcell should have no more records.
     */
    if (0 != pRecord->type) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_642,
					   "Deleted records from RBMT/BMT0 vol/dom mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->type = 0;
	pRecord->bCnt = 0;
	modified = TRUE;
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_643, 
			 "Found corruptions in RBMT/BMT0 vol/dom mcell.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_644, 
					   "Modified RBMT/BMT0 vol/dom mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        return FIXED;
    }

  return SUCCESS;
} /* end correct_rbmt_vol_dom_mcell */


/*
 * Function Name:correct_rbmt_misc_mcell(3.29)
 *
 * Description: This function will check mcell 5 of the RBMT. It will fix
 *              and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *     pageNum: The page number in the RBMT this belongs to.
 *     domain: The domain we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
int correct_rbmt_misc_mcell(pageT *pPage, int volNum,
			    int pageNum, domainT *domain)
{
    char   *funcName = "correct_rbmt_misc_mcell";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT  *pRecord; /* The Current mcell record we are working on */
    int    modified; /* Flag to let us know if the page has been modified. */
    int    mcellNum;
    int    tagNum;
    int    status;
    int    foundCorruption;
    int    chainVol;
    int    chainPage;
    int    chainMcell;
    int    x;
    bsBfAttrT   *pBSRAttr;
    bsXtntRT    *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    mcellNum = 5;
    tagNum   = -(mcellNum + (6 * volNum));
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    if (pMcell->tag.num != tagNum) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_645,
					   "Modified RBMT/BMT0 misc mcell's tag number from %d to %d.\n"),
				   pMcell->tag.num, tagNum);
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = tagNum;
	modified = TRUE;
    }

    /*
     * This record must be of type BSR_ATTR
     */
    status = correct_rbmt_bsr_attr_record (pPage, pRecord, mcellNum, 2, 0);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     *
     * NOTE : Special case on this mcell's record size. 
     */
    status = correct_bsr_xtnts_default_values (pPage, pRecord, 
					       mcellNum, 160);
    if (FIXED == status) {
	modified = TRUE;
    } else if (SUCCESS != status){
	return status;
    }

    /*
     * Unique BSR_XTNTS values to this mcell
     */
    if (BLOCKS_PER_PAGE != pXtnt->blksPerPage) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_655,
					   "Modified RBMT/BMT0 misc mcell's blksPerPage from %d to %d.\n"),
				   pXtnt->blksPerPage, BLOCKS_PER_PAGE);
	if (SUCCESS != status) {
	    return status;
	}
        pXtnt->blksPerPage = BLOCKS_PER_PAGE;
	modified = TRUE;
    }

    if (3 != pXtnt->firstXtnt.xCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_656, 
					   "Modified RBMT/BMT0 misc mcell's xCnt from %d to %d.\n"),
				   pXtnt->firstXtnt.xCnt, 3);
	if (SUCCESS != status) {
	    return status;
	}
        pXtnt->firstXtnt.xCnt = 3;
	modified = TRUE;
    }

    /*
     * with log isolation we can now have mcells
     * in the chain.  If the chain is valid then the mcells in it 
     * should be of tagNum 'misc'.  This mcell could be on a different
     * page so we might need to load a second page.
     */
    foundCorruption = FALSE;
    chainVol   = 0; /* Set to empty chain value */
    chainPage  = 0; /* Set to empty chain value */
    chainMcell = 0; /* Set to empty chain value */

    if ((0 == pXtnt->chainMCId.page) &&	(0 == pXtnt->chainMCId.cell)) {
	if (0 != pXtnt->chainVdIndex) {
	    /*
	     * page and cell both are 0.  This means chainVd must be zero.
	     */
	    foundCorruption = TRUE;
	}
    } else {
	/*
	 * Else page or cell is set to something other than 0
	 */
	int size;
	LBNT tmpLbn;
	int tmpVol;
	int chainTagNum;
	pagetoLBNT *rbmtLbn;
	bsMCT  *pTempMcell;
	pageT  *pTempPage;
	bsMPgT *pTempBMTPage;

	/*
	 * Check to see if the cell is in the valid range.
	 */
	if ((pXtnt->chainMCId.cell < 0) || 
	    (pXtnt->chainMCId.cell >= RBMT_RSVD_CELL)) {
	    /*
	     * mcell chain is invalid.
	     */
	    foundCorruption = TRUE;
	} 

	if (pageNum != pXtnt->chainMCId.page) {
	    /*
	     * Now check to see if the page is valid.
	     */
	    rbmtLbn = domain->volumes[volNum].volRbmt->rbmtLBN;
	    size    = domain->volumes[volNum].volRbmt->rbmtLBNSize;
	    
	    status = convert_page_to_lbn(rbmtLbn, size, pXtnt->chainMCId.page,
					 &tmpLbn, &tmpVol);
	    if (SUCCESS != status) {
		/*
		 * Page is corrupt as it doesn't fall in the RBMT.
		 * If this happens an info/debug message is printed
		 * by convert_page_to_lbn
		 */
		foundCorruption = TRUE;
	    }
	}
	    
	/*
	 * Now if page and cell are valid, check if mcell belongs
	 * to the 'misc' chain.
	 */
	if (FALSE == foundCorruption) {
	    if (pageNum == pXtnt->chainMCId.page) {
		/*
		 * The mcell falls on this page so lets check it.
		 */
		pTempPage = pPage;
	    } else {
		/*
		 * mcell falls on another RBMT page.
		 */
		status = read_page_from_cache(tmpVol, tmpLbn, &pTempPage);
		if (SUCCESS != status) {
		    return status;
		}
	    } /* end if/else status not SUCCESS */

	    pTempBMTPage = (bsMPgT *)pTempPage->pageBuf;
	    pTempMcell   = &(pTempBMTPage->bsMCA[pXtnt->chainMCId.cell]);
	    chainTagNum  = pTempMcell->tag.num;

	    if (tagNum != chainTagNum) {
		/*
		 * The chainTag does not match this tag.
		 * Two possible corruptions:
		 *  A) The chain 
		 *  B) the chainTagNum.
		 *
		 * For now we will truncate the chain, FUTURE we might
		 * want to see if we can check/fix the chainTagNum.
		 */
		foundCorruption = TRUE;
	    } else {
		if (volNum != pXtnt->chainVdIndex) {
		    /*
		     * The chainVd is corrupt, it MUST be on this 
		     * volume.  Make sure to set page and cell.
		     */
		    chainVol   = volNum;
		    chainPage  = pXtnt->chainMCId.page;
		    chainMcell = pXtnt->chainMCId.cell;
		    foundCorruption = TRUE;
		} /* end if chainVdIndex is corrupt */
	    } /* end if/else tagNum doesn't match */

	    if (pageNum != pXtnt->chainMCId.page) {
		/*
		 * Need to release the second page.
		 */
		status = release_page_to_cache(tmpVol, tmpLbn,
					       pTempPage, FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    } /* end if on different pages */
	} /* end if not corrupt */
    } /* end if/else page and cell equal 0 */

    if (TRUE == foundCorruption) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_657, 
					   "Modified RBMT/BMT0 misc mcell's chain from (%d,%d,%d) to (%d,%d,%d).\n"),
				   pXtnt->chainVdIndex,
				   pXtnt->chainMCId.page,
				   pXtnt->chainMCId.cell,
				   chainVol, chainPage, chainMcell);
	if (SUCCESS != status) {
	    return status;
	}
	pXtnt->chainVdIndex   = chainVol;
	pXtnt->chainMCId.page = chainPage;
	pXtnt->chainMCId.cell = chainMcell;
	modified = TRUE;
	foundCorruption = FALSE;
    }

    /*
     * Can only have a 2 extents with set locations.
     */
    if ((0 != pXtnt->firstXtnt.bsXA[0].bsPage) ||
	(0 != pXtnt->firstXtnt.bsXA[0].vdBlk)  ||
	(2 != pXtnt->firstXtnt.bsXA[1].bsPage) ||
	(64 != pXtnt->firstXtnt.bsXA[1].vdBlk) ||
	(4 != pXtnt->firstXtnt.bsXA[2].bsPage) ||
	(-1 != pXtnt->firstXtnt.bsXA[2].vdBlk)) {
        pXtnt->firstXtnt.bsXA[0].bsPage = 0;
	pXtnt->firstXtnt.bsXA[0].vdBlk  = 0;
	pXtnt->firstXtnt.bsXA[1].bsPage = 2;
	pXtnt->firstXtnt.bsXA[1].vdBlk  = 64;
	pXtnt->firstXtnt.bsXA[2].bsPage = 4;
	pXtnt->firstXtnt.bsXA[2].vdBlk  = -1;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_658,
					   "Modified RBMT/BMT0 misc mcell's extents.\n"));
	if (SUCCESS != status) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * Set pRecord to next record  of pMcell.
     */
    pRecord = (bsMRT *) (((char *)pRecord) + 
			 roundup(pRecord->bCnt, sizeof(int))); 

    /*
     * This mcell should have no more records.
     */
    if (0 != pRecord->type) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_660, 
					   "Deleted records from RBMT/BMT0 misc mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->type = 0;
	pRecord->bCnt = 0;
	modified = TRUE;
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_661, 
			 "Found corruptions in RBMT/BMT0 misc mcell.\n"));
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_662, 
					   "Modified RBMT/BMT0 misc mcell.\n"));
	if (SUCCESS != status) {
	    return status;
	}
        return FIXED;
    }

  return SUCCESS;
} /* end correct_rbmt_misc_mcell */


/*
 * Function Name:correct_rbmt_other_mcell(3.30)
 *
 * Description: This function will check the rest of the mcell in the RBMT. 
 *              It will fix and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     mcellNum: Which mcell to work on.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED
 */
int correct_rbmt_other_mcell(pageT *pPage, int mcellNum, 
			     int volNum, domainT *domain)
{
    char   *funcName = "correct_rbmt_other_mcell";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;  /* The current RBMT mcell we are working on */
    bsMRT  *pRecord; /* The current mcell record we are working on */
    bsXtraXtntRT *pXtra;
    int    modified; /* Flag to let us know if the page has been modified. */
    int    status;
    int    x;
    int    numXtnt;
    int    tagNum;
    char   errorMsg[MAXERRMSG];

    /*
     * Init variables
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * setTag and Tag should be either (0,0) or
     * (-2, [-(volnum * 6) to -((volnum * 6) + 5)]).
     *
     * If setTag is -2 and tag is out of range, set tag to belong to
     * the BMT chain.
     *
     * If tag is within legal range and setTag is not -2, set it to -2.
     *
     * Else, make sure tag and setTag are both set to zero.
     */
    if ((-2 == pMcell->bfSetTag.num) &&
	((pMcell->tag.num > -(volNum * 6)) ||
	 (pMcell->tag.num < -((volNum * 6) + 5)))) {
	/*
	 * Tag number is out of range for this mcell based on 
	 * the volNum.  In almost all cases the tag should 
	 * belong to the BMT chain.  
	 *
	 * FUTURE: walk the chains in the RBMT to verify tagNum.
	 */
	if (3 == domain->dmnVersion) {
	    tagNum = -(volNum * 6);
	} else {
	    tagNum = -((volNum * 6) + 4);
	}

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_676,
					   "Modified RBMT/BMT0 mcell %d's tag number from %d to %d.\n"),
				   mcellNum, pMcell->tag.num, tagNum);
	if (SUCCESS != status) {
	    return status;
	}
	pMcell->tag.num = tagNum;
	modified = TRUE;
    } /* end if setTag is -2 */
    else if (((pMcell->tag.num <= -(volNum * 6)) &&
	      (pMcell->tag.num >= -((volNum * 6) + 5))) &&
	     (-2 != pMcell->bfSetTag.num)) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_664, 
					   "Modified RBMT/BMT0 mcell %d's set tag number from %d.%x to %d.%x.\n"),
				   mcellNum,
				   pMcell->bfSetTag.num, 
				   pMcell->bfSetTag.seq,
				   -2,
				   pMcell->bfSetTag.seq);
	if (SUCCESS != status) {
	    return status;
	}
	pMcell->bfSetTag.num = -2;
	modified = TRUE;
    } /* End if tag is in valid range */
    else if (!((0 == pMcell->tag.num) && (0 == pMcell->bfSetTag.num)) &&
	     !((-2 == pMcell->bfSetTag.num) &&
	       (pMcell->tag.num <= -(volNum * 6)) &&
	       (pMcell->tag.num >= -((volNum * 6) + 5))) ) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_663, 
					   "Modified RBMT/BMT0 mcell %d's tag number from %d.%x to %d.%x.\n"),
				   mcellNum, pMcell->tag.num, 
				   pMcell->tag.seq,
				   0, 0);
	if (SUCCESS != status) {
	    return status;
	}

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_664, 
					   "Modified RBMT/BMT0 mcell %d's set tag number from %d.%x to %d.%x.\n"),
				   mcellNum,
				   pMcell->bfSetTag.num, 
				   pMcell->bfSetTag.seq,
				   0, 0);
	if (SUCCESS != status) {
	    return status;
	}
        pMcell->tag.num = 0;
        pMcell->tag.seq = 0;
	pMcell->bfSetTag.num = 0;
	pMcell->bfSetTag.seq = 0;
	/*
	 * Clear the mcell record header.
	 */
	pRecord->bCnt = 0;
	pRecord->type = 0;
	pRecord->version = 0;

	modified = TRUE;
    }
    else {
	/* (setTag, tag) are not corrupt. */
    }
	
    if ((0 != pMcell->tag.num) && (0 != pMcell->bfSetTag.num)) {
        /*
	 * Non empty mcell.
	 */
        pRecord = (bsMRT *)pMcell->bsMR0;

	/*
	 * This record must be of type BSR_XTRA_XTNTS
	 */
	if (BSR_XTRA_XTNTS != pRecord->type) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_665, 
					       "Modified RBMT/BMT0 mcell %d's record type from %d to BSR_XTRA_XTNTS.\n"),
				       mcellNum, pRecord->type);
	    if (SUCCESS != status) {
		return status;
	    }
	    pRecord->type = BSR_XTRA_XTNTS;
	    modified = TRUE;
	}

	/* (sizeof(bsMRT) + sizeof(bsXtraXtntRT)) == 264 */
	if ((sizeof(bsMRT) + sizeof(bsXtraXtntRT)) != pRecord->bCnt) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_666, 
					       "Modified RBMT/BMT0 mcell %d's record bCnt from %d to %d.\n"),
				       mcellNum, pRecord->bCnt, 
				       (sizeof(bsMRT) + sizeof(bsXtraXtntRT)));
	    if (SUCCESS != status) {
		return status;
	    }
	    pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsXtraXtntRT));
	    modified = TRUE;
	}

	if (BLOCKS_PER_PAGE != pXtra->blksPerPage) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_667, 
					       "Modified RBMT/BMT0 mcell %d's blksPerPage from %d to %d.\n"),
				       mcellNum, pXtra->blksPerPage, BLOCKS_PER_PAGE);
	    if (SUCCESS != status) {
		return status;
	    }
	    pXtra->blksPerPage = BLOCKS_PER_PAGE;
	    modified = TRUE;
	}

	/*
	 * Need to make sure the xCnt is with in valid range.
	 */
	if ((pXtra->xCnt < 1) || (pXtra->xCnt > BMT_XTRA_XTNTS)) {
	    /*
	     * As we will be be setting the xCnt to the actual number in 
	     * the following block ofcode, all we need to do is set xCnt 
	     * to BMT_XTRA_XTNTS and set the last vdBlk to 0.
	     */
	    pXtra->xCnt = BMT_XTRA_XTNTS;

	    /* 
	     * The only valid values for the last vdblk are -1 and 0,
	     * but garbage has been known to show up here when the entry
	     * in the array is not in use.
	     *
	     * If vdBlk should not be 0 but instead -1, it will be set
	     * correctly in the next block of code.  
	     */
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_669, 
					       "Modified RBMT/BMT0 mcell %1$d's xCnt from %2$d to %3$d.\n"),
				       mcellNum, pXtra->xCnt, -1);
	    if (SUCCESS != status) {
		return status;
	    }
	    pXtra->bsXA[pXtra->xCnt - 1].vdBlk = 0;
	    modified = TRUE;
	}

	/*
	 * Check the number of xtnts in record.
	 */
	numXtnt = pXtra->xCnt;

	if (-1 != pXtra->bsXA[numXtnt - 1].vdBlk) {
	    int x;
	    int endExtent;
	    int found;
	    
	    /*
	     * Init variables
	     */
	    found = 0;
	    endExtent = 0;
	
	    /*
	     * One of two possible error conditions:
	     *
	     * A) xCnt is wrong
	     * B) the extent pointed at by xCnt - 1 is wrong.
	     *
	     * The tool is going to assume that there is only a single
	     * corruption.
	     */
      
	    /*
	     * Loop through all possible extents looking for an 'end' case.
	     *
	     * A) two extents in a row with vdblk -1 (hole on end).
	     * B) A bogus page number
	     * C) A bogus vdBlk
	     * 
	     * If we can not find one of the above, assume xCnt is correct
	     * and extent is wrong.
	     */

	    /*
	     * loop through extents.
	     */
	    for (x = 0; (x < BMT_XTRA_XTNTS && FALSE == found); x++) {
	        /*
		 * Order of 'if's  is important
		 */
	        if ((3 == domain->dmnVersion) &&
		    (pXtra->bsXA[x].bsPage == pXtra->bsXA[x + 1].bsPage) &&
		    (-1 == pXtra->bsXA[x].vdBlk) &&
		    (-1 == pXtra->bsXA[x + 1].vdBlk)) {
		    /*
		     * NOTE In V3 domains it is possible to have two extents
		     * with the same page, IF vdBlk is -1. - not an end case.
		     * believe this only happens in the frag file.
		     */
		} else if (pXtra->bsXA[x].bsPage >= pXtra->bsXA[x + 1].bsPage) {
		    /* 
		     * page number does not increment. - valid end case 
		     */
		    endExtent = x;
		    found = TRUE;
		} else if (-1 == pXtra->bsXA[x].vdBlk) {
		    if (-1 == pXtra->bsXA[x + 1].vdBlk) {
		        /* 
			 * Hole at end - valid end case 
			 */
		        endExtent = x + 1;
			found = TRUE;
		    }
		} else if ((((pXtra->bsXA[x+1].bsPage - pXtra->bsXA[x].bsPage) * BLOCKS_PER_PAGE) + pXtra->bsXA[x].vdBlk) > domain->volumes[volNum].volSize) {
		    /*
		     * location on disk falls off end of domain 
		     */
		    endExtent = x;
		    found = TRUE;
		} else {
		    /*
		     * Any other??
		     */
		}
	    } /* end for loop */

	    if (FALSE == found) {
	        /*
		 * Did not find an end, assume xCnt correct.
		 */
		status = add_fixed_message(&(pPage->messages), RBMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_668, 
						   "Modified RBMT/BMT0 mcell %d's vdBlk from %d to %d.\n"),
					   mcellNum,
					   pXtra->bsXA[pXtra->xCnt - 1].vdBlk,
					   -1);
		if (SUCCESS != status) {
		    return status;
		}

	        pXtra->bsXA[pXtra->xCnt - 1].vdBlk = -1;
		modified = TRUE;
	    } else {
	        /* 
		 * Found an end point 
		 */
	        if (-1 != pXtra->bsXA[endExtent].vdBlk) {
		    if (endExtent + 1  < pXtra->xCnt) {
		        /*
			 * Double corruption.
			 */
			status = add_fixed_message(&(pPage->messages), 
						   RBMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_669, 
							   "Modified RBMT/BMT0 mcell %d's xCnt from %d to %d.\n"),
						   mcellNum, pXtra->xCnt,
						   endExtent + 1);
			if (SUCCESS != status) {
			    return status;
			}
		        pXtra->xCnt = endExtent + 1;
		    }

		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_668, 
						       "Modified RBMT/BMT0 mcell %d's vdBlk from %d to %d.\n"),
					       mcellNum,
					       pXtra->bsXA[pXtra->xCnt - 1].vdBlk,
					       -1);
		    if (SUCCESS != status) {
			return status;
		    }
		    pXtra->bsXA[pXtra->xCnt - 1].vdBlk = -1;
		} else {
		    status = add_fixed_message(&(pPage->messages), RBMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_669,
						       "Modified RBMT/BMT0 mcell %d's xCnt from %d to %d.\n"),
					       mcellNum,
					       pXtra->xCnt, endExtent + 1);

		    if (SUCCESS != status) {
			return status;
		    }
		    pXtra->xCnt = endExtent + 1;
		}
		modified = TRUE;
	    }
	} /* end if bad last Xtnt */

	numXtnt = pXtra->xCnt;
	/*
	 * loop through extents 0 to (numXtnt - 2).
	 */
	for (x = 0; x <= (numXtnt - 2); x++) {
	    if (-1 == pXtra->bsXA[x].vdBlk) {
	        writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_670,
				 "RBMT/BMT0 xtnt map has a hole.\n"));
		corrupt_exit(NULL);
	    }

	    /*
	     * vdBlk must be even divisable by BLOCKS_PER_PAGE(16).
	     * No way for us to currently determine the correct vdBlk
	     * so we have no option but to corrupt_exit.
	     */
	    if (0 != pXtra->bsXA[x].vdBlk % BLOCKS_PER_PAGE) {
	        writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_922,
				 "RBMT/BMT0 xtnt map has a LBN %d which is not on a page boundary.\n"),
			 pXtra->bsXA[x].vdBlk);
		corrupt_exit(NULL);
		
	    }

	    if ((((pXtra->bsXA[x+1].bsPage - pXtra->bsXA[x].bsPage) * BLOCKS_PER_PAGE) + pXtra->bsXA[x].vdBlk) > domain->volumes[volNum].volSize) {
	        writemsg(SV_ERR | SV_LOG_ERR, 
			catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_671, 
				"Extent size %d is larger than device size %d.\n"),
			(((pXtra->bsXA[x+1].bsPage - pXtra->bsXA[x].bsPage) 
			  * BLOCKS_PER_PAGE) + pXtra->bsXA[x].vdBlk),
			domain->volumes[volNum].volSize);
		corrupt_exit(NULL);
	    }

	    if (pXtra->bsXA[x].bsPage > pXtra->bsXA[x + 1].bsPage) {
	        writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_672, 
				 "RBMT/BMT0 xtnt pages out of order.\n"));
		corrupt_exit(NULL);
	    }
	}

        /*
	 * Set pRecord to next record  of pMcell.
	 */
        pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 

	/*
	 * This mcell should have no more records.
	 */
	if (0 != pRecord->type) {
	    pRecord->type = 0;
	    pRecord->bCnt = 0;
	    modified = TRUE;
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_673, 
					       "Deleted records from RBMT/BMT0 mcell %d.\n"),
				       mcellNum);
	    if (SUCCESS != status) {
		return status;
	    }
	}
    } else {
        /*
	 * Empty mcell
	 */
    }

    /*
     * We modified this page.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_674,
			 "Found corruptions in RBMT/BMT0 mcell %d.\n"),
		 mcellNum);

	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_675, 
					   "Modified RBMT/BMT0 mcell %d.\n"),
				   mcellNum);
	if (SUCCESS != status) {
	    return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_other_mcell */


/*
 * Function Name:correct_rbmt_next_page_mcell (3.31)
 *
 * Description: This function will check mcell 27 of the RBMT. It will fix 
 *              and correct any errors that it finds.
 *
 * Input parameters:
 *     pPage: The RBMT page to check.
 *     volNum: The volume number that is being worked on.
 *     domain: The domain we are working on.
 *     pageNum: The page number that is being worked on.
 *
 * Output parameters: 
 *     nextPageLBN: LBN of next RBMT page to check
 *
 * Returns:  SUCCESS, FIXED
 */
int correct_rbmt_next_page_mcell(pageT *pPage, int volNum, domainT *domain, 
				 int pageNum, LBNT *nextPageLBN)
{
    char   *funcName = "correct_rbmt_next_page_mcell";
    int    modified;   /* Flag to let us know if the page has been modified. */
    int    status;
    int    tagNum;
    char   errorMsg[MAXERRMSG];
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* The current RBMT mcell we are working on */
    bsMRT  *pRecord;   /* The Current mcell record we are working on */
    bsXtraXtntRT *pXtra;

    /*
     * Init variables
     */
    modified = FALSE;
    tagNum   = -(6 * volNum);
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &pBMTPage->bsMCA[RBMT_RSVD_CELL];
    pRecord  = (bsMRT *)pMcell->bsMR0;
    *nextPageLBN = (LBNT) -1;

    /*
     * On a version 4 domain, mcell 27 on all pages of the RBMT are marked
     * with the set tag and file tag of the RBMT. Mcell 27 on the last page
     * of the RBMT will not have any records, but still belongs to the RBMT.
     */
    if ( pMcell->tag.num != tagNum ) {
        status = add_fixed_message( &pPage->messages, RBMT,
                                    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_676,
                   "Modified RBMT/BMT0 mcell %d's tag number from %d to %d.\n"),
                                    RBMT_RSVD_CELL, pMcell->tag.num, tagNum );
        if ( SUCCESS != status ) {
            return status;
        }
        pMcell->tag.num = tagNum;
        modified = TRUE;
    }

    /*
     * Check to see if this is the last mcell in the RBMT extent chain.
     * We will determine this based on the presence or absence of
     * a BSR_XTRA_XTNT or nil record. We will look at 2 pieces of data
     * to determine if the mcell has a BSR_XTRA_XTNT  or nil record.
     * If the record type is 0 and the bCnt is correct for a nil record
     * then assume that it is an empty mcell and fix anything that makes
     * it look not empty. Correct bCnt is 0 or 4 (another small change).
     * If the record type is not BSR_XTRA_XTNT and the bCnt is not consistant
     * with a BSR_XTRA_XTNT record then something is really wrong. Make it
     * an empty mcell.
     * If the type is BSR_XTRA_XTNT and/or the bCnt is consistant with a
     * BSR_XTRA_XTNT record then fix it up as a BSR_XTRA_XTNT record.
     * we know many of the values. The vdBlk value has to be checked.
     * If it is not OK then we can not fix it.
     *
     * RBMT extent records change.  Before the chain
     * of mcells (nextVdi & nextMCId) extended all the way to the last
     * RBMT page. Mcell 27 on the second to the last RBMT page pointed
     * to the empty mcell 27 on the last page of the RBMT.
     * After the change, the "next" pointer only extended to the
     * second to the last RBMT page. Mcell 27 in the second to the last page
     * had a BSR_XTRA_XTNT record with the extent of the last page. The next
     * pointer in that mcell was nil.
     * The old way causes problems when we extend the RBMT
     * so we will fix it here.
     */

    if ( 0 == pRecord->type && sizeof(bsMRT) == pRecord->bCnt ||
         0 == pRecord->type && 0 == pRecord->bCnt )
    {
        /* This looks like a empty mcell. Fix it up and be done with it. */
        if ( 0 != pMcell->nextVdIndex ||
             0 != pMcell->nextMCId.page || 
             0 != pMcell->nextMCId.cell )
        {
            status = add_fixed_message( &pPage->messages, RBMT,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_532,
  "Modified RBMT/BMT0 BMT mcell's next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
                                        pMcell->nextVdIndex,
                                        pMcell->nextMCId.page,
                                        pMcell->nextMCId.cell,
                                        0, 0, 0);
            if ( SUCCESS != status ) {
                return status;
            }
            pMcell->nextVdIndex = 0;
            pMcell->nextMCId.page = 0;
            pMcell->nextMCId.cell = 0;
            modified = TRUE;
        }
    } else if ( BSR_XTRA_XTNTS != pRecord->type &&
                sizeof(bsMRT) + sizeof(bsXtraXtntRT) != pRecord->bCnt )
    {
        /* This looks like an unextepected record. */
        /* Let's make it a nil record and be done with it. */
        status = add_fixed_message( &pPage->messages, RBMT,
                                    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_679,
                              "Deleted records from next RBMT page mcell.\n") );
        if ( SUCCESS != status ) {
            return status;
        }
        pRecord->type = 0;
        pRecord->bCnt = sizeof(bsMRT);
        modified = TRUE;

        if ( 0 != pMcell->nextVdIndex ||
             0 != pMcell->nextMCId.page ||
             0 != pMcell->nextMCId.cell )
        {
            status = add_fixed_message( &pPage->messages, RBMT,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_532,
  "Modified RBMT/BMT0 BMT mcell's next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
                                        pMcell->nextVdIndex,
                                        pMcell->nextMCId.page,
                                        pMcell->nextMCId.cell,
                                        0, 0, 0);
            if ( SUCCESS != status ) {
                return status;
            }
            pMcell->nextVdIndex = 0;
            pMcell->nextMCId.page = 0;
            pMcell->nextMCId.cell = 0;
            modified = TRUE;
        }
    } else if ( BSR_XTRA_XTNTS != pRecord->type ) {
        /* Maybe only the type is wrong. Lets fix it and see if that works. */
        status = add_fixed_message( &(pPage->messages), RBMT,
                                    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_677,
      "Modified RBMT/BMT0 mcell %d's record type from %d to BSR_XTRA_XTNTS.\n"),
                                     RBMT_RSVD_CELL, pRecord->type);
        if ( SUCCESS != status ) {
            return status;
        }
        pRecord->type = BSR_XTRA_XTNTS;
        modified = TRUE;
    } else if ( sizeof(bsMRT) + sizeof(bsXtraXtntRT) != pRecord->bCnt ) {
        /* Maybe only the bCnt is wrong. Lets fix it and see if that works. */
        status = add_fixed_message( &pPage->messages, RBMT,
                                    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_666,
                  "Modified RBMT/BMT0 mcell %d's record bCnt from %d to %d.\n"),
                                    RBMT_RSVD_CELL, pRecord->bCnt,
                                    sizeof(bsMRT) + sizeof(bsXtraXtntRT) );
        if ( SUCCESS != status ) {
            return status;
        }
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsXtraXtntRT));
        modified = TRUE;
    }

    /*
     * Now the mcell has a nil record or a BSR_XTRA_XTNTS record type.
     * If it is BSR_XTRA_XTNTS then check it and fix it.
     */
    if ( BSR_XTRA_XTNTS == pRecord->type ) {
        pageT *pNextPg;
        bsMPgT *pNextBMTPage;
        bsMCT *pNextMcell;
        bsMRT *pNextRecord;
        int secondLast = FALSE;

        pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
        /*
         * First decide if the vdBlk is any good.
         */
        if ( 0 != pXtra->bsXA[0].vdBlk % BLOCKS_PER_PAGE ) {
            /* vdBlk must be evenly divisable by BLOCKS_PER_PAGE(16)  */
            writemsg(SV_ERR | SV_LOG_ERR,
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_923,
       "RBMT/BMT0 'next' xtnt has a LBN %d which is not on a page boundary.\n"),
                     pXtra->bsXA[0].vdBlk);
            corrupt_exit(NULL);
        }

        if (pXtra->bsXA[0].vdBlk > domain->volumes[volNum].volSize) {
            /* vdBlk must be within the volume */
            writemsg(SV_ERR | SV_LOG_ERR,
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_678,
                  "RBMT/BMT0 'next' xtnt LBN is off the end of the volume.\n"));
            corrupt_exit(NULL);
        }

        *nextPageLBN = pXtra->bsXA[0].vdBlk;

        /*
         * Now that the vdBlk for the next RBMT page is reasonable,
         * fix the rest of the mcell.
         *
         * First check and fix the next pointer.
         * The second to the last RBMT page may have a nil pointer or
         * it may point to mcell 27 on the last RBMT page.
         * All other pages need to point to the next page.
         * We wont know if this is the second to the last page until
         * we read the next page and see if mcell 27 is empty or not.
         * So read the next RBMT page.
         * We are not trying to fix the next page here, we are only trying
         * to determine if we will recognize it as the last page when we
         * get to it. We will recognize it as the last page if it has a
         * good empty mcell 27 (nil record type and good bCnt). If it has
         * an unextpected type and bCnt then we know that when we correct
         * that page we will make mcell 27 empty so assume it is empty now,
         * and therefore this page is the second to the last page.
         */
        status = read_page_from_cache( volNum, *nextPageLBN, &pNextPg );
        if ( SUCCESS != status ) {
            return status;
        }

        pNextBMTPage = (bsMPgT *)pNextPg->pageBuf;
        pNextMcell = &pNextBMTPage->bsMCA[RBMT_RSVD_CELL];
        pNextRecord = (bsMRT *)pNextMcell->bsMR0;

        if ( 0 == pNextRecord->type && sizeof(bsMRT) == pNextRecord->bCnt ||
	     0 == pNextRecord->type && 0 == pNextRecord->bCnt )
	{		
            secondLast = TRUE;
        } else if ( BSR_XTRA_XTNTS != pNextRecord->type &&
                    sizeof(bsMRT) + sizeof(bsXtraXtntRT) != pNextRecord->bCnt )
        {
            secondLast = TRUE;
        }

        status = release_page_to_cache( volNum, *nextPageLBN, pNextPg, 
                                        FALSE, FALSE);
        if ( SUCCESS != status ) {
            return status;
        }

        if ( FALSE == secondLast ) {
            /* Next pointer must point to mcell 27 on the next RBMT page. */
            if ( pMcell->nextVdIndex != volNum ||
                 pMcell->nextMCId.page != pageNum + 1 ||
                 pMcell->nextMCId.cell != 27 )
            {
                status = add_fixed_message(&pPage->messages, RBMT,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_532,
  "Modified RBMT/BMT0 BMT mcell's next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
                                           pMcell->nextVdIndex,
                                           pMcell->nextMCId.page,
                                           pMcell->nextMCId.cell,
                                           volNum, (pageNum + 1), 27 );
                if (SUCCESS != status) {
                    return status;
                }
                pMcell->nextVdIndex = volNum;
                pMcell->nextMCId.page = (pageNum + 1);
                pMcell->nextMCId.cell = 27;
                modified = TRUE;

            }
        } else {
            /*
             * Mcell 27 on the second to last RBMT page must be a nil pointer.
             * If it is not a valid pointer to the next page then set it to nil.
             */
            if ( !(pMcell->nextVdIndex == 0 &&
                   pMcell->nextMCId.page == 0 &&
                   pMcell->nextMCId.cell == 0) )
            {
                status = add_fixed_message( &pPage->messages, RBMT,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_532,
  "Modified RBMT/BMT0 BMT mcell's next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
                                            pMcell->nextVdIndex,
                                            pMcell->nextMCId.page,
                                            pMcell->nextMCId.cell,
                                            0, 0, 0);
                if ( SUCCESS != status ) {
                    return status;
                }
                pMcell->nextVdIndex = 0;
                pMcell->nextMCId.page = 0;
                pMcell->nextMCId.cell = 0;
                modified = TRUE;
            }
        }

	if ( BLOCKS_PER_PAGE != pXtra->blksPerPage ) {
            status = add_fixed_message( &pPage->messages, RBMT,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_667,
                  "Modified RBMT/BMT0 mcell %d's blksPerPage from %d to %d.\n"),
                          RBMT_RSVD_CELL, pXtra->blksPerPage, BLOCKS_PER_PAGE );
            if ( SUCCESS != status ) {
                return status;
            }
            pXtra->blksPerPage = BLOCKS_PER_PAGE;
            modified = TRUE;
        }

        /*
         * Check the number of xtnts in record, should always be 2.
         */
        if ( 2 != pXtra->xCnt ) {
            pXtra->xCnt = 2;
            modified = TRUE;
        }

        if ( (LBNT) -1 != pXtra->bsXA[1].vdBlk ) {
            pXtra->bsXA[1].vdBlk = -1;
            modified = TRUE;
        }

        if ( pXtra->bsXA[0].bsPage != pageNum + 1 ) {
            pXtra->bsXA[0].bsPage = pageNum + 1;
            modified = TRUE;
        }

        if ( pXtra->bsXA[1].bsPage != pageNum + 2 ) {
            pXtra->bsXA[1].bsPage = pageNum + 2;
            modified = TRUE;
        }

        /*
         * Set pRecord to next record  of pMcell.
         */
        pRecord = (bsMRT *) (((char *)pRecord) + 
                             roundup(pRecord->bCnt, sizeof(int))); 

        /*
         * This mcell should have no more records.
         */
        if ( 0 != pRecord->type ) {
            pRecord->type = 0;
            pRecord->bCnt = 0;
            modified = TRUE;
            status = add_fixed_message( &pPage->messages, RBMT,
                                      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_679,
                              "Deleted records from next RBMT page mcell.\n") );
            if ( SUCCESS != status ) {
                return status;
            }
        }
    }

    if ( pageNum + 2 != pMcell->linkSegment ) {
        status = add_fixed_message( &pPage->messages, RBMT,
                                   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_929,
                    "Modified mcell (%d,%d,%d)'s linkSegment from %d to %d.\n"),
                                   volNum,
                                   pageNum,
                                   27,
                                   pMcell->linkSegment,
                                   pageNum + 2);
        if ( SUCCESS != status ) {
            return status;
        }
        pMcell->linkSegment = pageNum + 2;
        modified = TRUE;
    }

    /*
     * We modified this page.
     */
    if ( TRUE == modified ) {
        writemsg(SV_VERBOSE | SV_LOG_FOUND,
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_680,
                         "Found corruptions in next RBMT page mcell.\n"));

        status = add_fixed_message( &pPage->messages, RBMT,
                                    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_681, 
                                          "Modified next RBMT page mcell.\n") );
        if ( SUCCESS != status ) {
            return status;
	}
        return FIXED;
    }

    return SUCCESS;
} /* end correct_rbmt_next_page_mcell */


/*
 * Function Name: correct_smart_store_record (3.xx)
 * 
 * Description: This function only checks the bCnt of the record.  The
 *              data in the record is not checked.
 *
 * Input parameters:
 *     pPage:	The page on which the record is found.
 *     pRecord:	The Smart Store record to check.
 *     mcellNum: The mcell number of the record we are checking.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, or FIXED
 */
int correct_smart_store_record (pageT *pPage, bsMRT *pRecord, int mcellNum)
{
    char *funcName = "correct_smart_store_record";
    int  modified;   /* Has the record has been modified. */
    int  status;

    /*
     * Init variables
     */
    modified = FALSE;

    /* (sizeof(bsMRT) + sizeof(bsSSDmnAttrT)) == 60 */
    if ((sizeof(bsMRT) + sizeof(bsSSDmnAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_900, 
					   "Modified RBMT/BMT0 mcell %1$d's record byte count from %2$d to %3$d.\n"),
				   mcellNum, pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsSSDmnAttrT)));
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsSSDmnAttrT));
	modified = TRUE;
    }

    /*
     * Nothing to check in the record.
     */
    if (TRUE == modified) {
	return FIXED;
    } else {
	return SUCCESS;
    }
} /* end correct_smart_store_record */


/*
 * Function Name: correct_freeze_record (3.xx)
 *
 * Description: This function only checks the bCnt of the record.  The
 *              data in the record is not checked.
 *
 * Input parameters:
 *     pPage:	The page on which the record is found.
 *     pRecord:	The freeze record to check.
 *     mcellNum: The mcell number of the record we are checking.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, or FIXED
 */
int correct_freeze_record (pageT *pPage, bsMRT *pRecord, int mcellNum)
{
    char   *funcName = "correct_freeze_record";
    int  modified;   /* Has the record has been modified. */
    int  status;

    /*
     * Init variables
     */
    modified = FALSE;

    /* (sizeof(bsMRT) + sizeof(bsDmnFreezeAttrT)) == 32 */
    if ((sizeof(bsMRT) + sizeof(bsDmnFreezeAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_900, 
					   "Modified RBMT/BMT0 mcell %1$d's record byte count from %2$d to %3$d.\n"),
				   mcellNum, pRecord->bCnt, 
				   (sizeof(bsMRT) +
				    sizeof(bsDmnFreezeAttrT)));
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsDmnFreezeAttrT));
	modified = TRUE;
    }

    /*
     * Nothing to check in the record.
     */
    if (TRUE == modified) {
	return FIXED;
    } else {
	return SUCCESS;
    }
} /* end correct_freeze_record */


/*
 * Function Name: correct_rbmt_bsr_attr_record (3.xx)
 *
 * Description: This function will check a RBMT BSR_ATTR record.
 *              It will correct any errors found.  
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *     pRecord:	 The Smart Store record to check.
 *     mcellNum: Which mcell has this record.
 *     dataSafety : What value should this field have.
 *     reqServices: What value should this field have.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED or NO_MEMORY.
 */
int correct_rbmt_bsr_attr_record (pageT *pPage, bsMRT *pRecord, 
				  int mcellNum, int dataSafety,
				  int reqServices)
{
    char *funcName = "correct_rbmt_bsr_attr_record";
    int  modified;   /* Has the record has been modified. */
    int  x;
    int  status;
    bsBfAttrT   *pBSRAttr;

    /*
     * Init variables
     */
    modified = FALSE;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_ATTR
     */
    if (BSR_ATTR != pRecord->type) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_899, 
					   "Modified RBMT/BMT0 mcell %1$d's record type from %2$d to BSR_ATTR.\n"),
				   mcellNum, pRecord->type);
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->type = BSR_ATTR;
	modified = TRUE;
    }
    /* (sizeof(bsMRT) + sizeof(bsBfAttrT)) == 92 */
    if ((sizeof(bsMRT) + sizeof(bsBfAttrT)) != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_900, 
					   "Modified RBMT/BMT0 mcell %1$d's record byte count from %2$d to %3$d.\n"),
				   mcellNum, pRecord->bCnt, 
				   (sizeof(bsMRT) + sizeof(bsBfAttrT)));
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->bCnt = (sizeof(bsMRT) + sizeof(bsBfAttrT));
	modified = TRUE;
    }

    /*
     * Check each field in BSR_ATTR looking for corruptions:
     */
    if (BSRA_VALID != pBSRAttr->state) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_901, 
					   "Modified RBMT/BMT0 mcell %1$d's state from %2$d to %3$d.\n"),
				   mcellNum, pBSRAttr->state, BSRA_VALID);
	if (SUCCESS != status) {
	    return status;
	}
        pBSRAttr->state = BSRA_VALID;
	modified = TRUE;
    }

    if (BLOCKS_PER_PAGE != pBSRAttr->bfPgSz) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_902, 
					   "Modified RBMT/BMT0 mcell %1$d's bfPgSz from %2$d to %3$d.\n"), 
				   mcellNum, pBSRAttr->bfPgSz, BLOCKS_PER_PAGE);
	if (SUCCESS != status) {
	    return status;
	}
        pBSRAttr->bfPgSz = BLOCKS_PER_PAGE;
	modified = TRUE;
    }

    if (dataSafety != pBSRAttr->cl.dataSafety) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_903, 
					   "Modified RBMT/BMT0 mcell %1$d's dataSafety from %2$d to %3$d.\n"),
				   mcellNum, pBSRAttr->cl.dataSafety, dataSafety);
	if (SUCCESS != status) {
	    return status;
	}
	pBSRAttr->cl.dataSafety = dataSafety;
	modified = TRUE;
    }
    
    if (reqServices != pBSRAttr->cl.reqServices) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_898, 
					   "Modified RBMT/BMT0 mcell %1$d's reqServices from %2$d to %3$d.\n"),
				   mcellNum, pBSRAttr->cl.reqServices, reqServices);
	if (SUCCESS != status) {
	    return status;
	}
        pBSRAttr->cl.reqServices = reqServices;
	modified = TRUE;
    }

    if ((0 != pBSRAttr->transitionId)    ||
	(0 != pBSRAttr->cloneId)         ||
	(0 != pBSRAttr->cloneCnt)        ||
	(0 != pBSRAttr->maxClonePgs)     ||
	(0 != pBSRAttr->deleteWithClone) ||
	(0 != pBSRAttr->outOfSyncClone)  ||
	(0 != pBSRAttr->cl.optServices)  ||
	(0 != pBSRAttr->cl.extendSize)   ||
	(0 != pBSRAttr->cl.rsvd1)        ||
	(0 != pBSRAttr->cl.rsvd2)        ||
	(0 != pBSRAttr->cl.acl.num)      || 
	(0 != pBSRAttr->cl.acl.seq)      ||
	(0 != pBSRAttr->cl.rsvd_sec1)    ||
	(0 != pBSRAttr->cl.rsvd_sec2)    ||
	(0 != pBSRAttr->cl.rsvd_sec3)) {
        /*
	 * These values must be 0.
	 */
        pBSRAttr->transitionId = 0;
        pBSRAttr->cloneId = 0;
        pBSRAttr->cloneCnt = 0;
        pBSRAttr->maxClonePgs = 0;
        pBSRAttr->deleteWithClone = 0;
        pBSRAttr->outOfSyncClone = 0;
        pBSRAttr->cl.optServices = 0;
        pBSRAttr->cl.extendSize = 0;
        pBSRAttr->cl.rsvd1 = 0;
        pBSRAttr->cl.rsvd2 = 0;
        pBSRAttr->cl.acl.num = 0;
        pBSRAttr->cl.acl.seq = 0;
        pBSRAttr->cl.rsvd_sec1 = 0;
        pBSRAttr->cl.rsvd_sec2 = 0;
        pBSRAttr->cl.rsvd_sec3 = 0;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_904, 
					   "Modified RBMT/BMT0 mcell %d, reset empty fields.\n"),
				   mcellNum);
	if (SUCCESS != status) {
	    return status;
	}
	modified = TRUE;
    }

    for (x = 0; x < BS_CLIENT_AREA_SZ; x++) {
        if (0 != pBSRAttr->cl.clientArea[x]) {
	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_905, 
					       "Modified RBMT/BMT0 mcell %1$d's client area [%2$d] from %3$d to %4$d.\n"),
				       mcellNum, x, pBSRAttr->cl.clientArea[x], 0);
	    if (SUCCESS != status) {
		return status;
	    }
	    pBSRAttr->cl.clientArea[x] = 0;
	    modified = TRUE;
	}
    }

    if (TRUE == modified) {
	return FIXED;
    } else {
	return SUCCESS;
    }
} /* end correct_rbmt_bsr_attr_record */


/*
 * Function Name: correct_bsr_xtnts_default_values (3.xx)
 *
 * Description: This function will check a RBMT BSR_XTNT record.
 *              It will correct any errors found.  
 *
 * Input parameters:
 *     pPage:	 The page on which the record is found.
 *     pRecord:	 The Smart Store record to check.
 *     mcellNum: Which mcell has this record.
 *     byteCount: What is the size of the record (to handle special case)
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED or NO_MEMORY.
 */
int correct_bsr_xtnts_default_values (pageT *pPage, bsMRT *pRecord, 
				      int mcellNum, int byteCount)
{
    char *funcName = "correct_bsr_xnts_default_values";
    int  modified;   /* Has the record has been modified. */
    int  x;
    int  status;
    bsXtntRT *pXtnt;

    /*
     * Init variables
     */
    modified = FALSE;
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * This record must be of type BSR_XTNTS
     */
    if (BSR_XTNTS != pRecord->type) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_906,
					   "Modified RBMT/BMT0 mcell %1$d's record type from %2$d to BSR_XTNTS.\n"),
				   mcellNum, pRecord->type);
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->type = BSR_XTNTS;
	modified = TRUE;
    }

    if (byteCount != pRecord->bCnt) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_900, 
					   "Modified RBMT/BMT0 mcell %1$d's record byte Count from %2$d to %3$d.\n"),
				   mcellNum, pRecord->bCnt, byteCount);
	if (SUCCESS != status) {
	    return status;
	}
        pRecord->bCnt = byteCount;
	modified = TRUE;
    }

    if (BLOCKS_PER_PAGE != pXtnt->blksPerPage) {
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_667, 
					   "Modified RBMT/BMT0 mcell %1$d's blksPerPage from %2$d to %3$d.\n"),
				   mcellNum, pXtnt->blksPerPage, BLOCKS_PER_PAGE);
	if (SUCCESS != status) {
	    return status;
	}
        pXtnt->blksPerPage = BLOCKS_PER_PAGE;
	modified = TRUE;
    }

    /*
     * Rest of record will be 0.
     */
    if ((0 != pXtnt->type)       ||
	(0 != pXtnt->rsvd1)      ||
	(0 != pXtnt->rsvd2.page) ||
	(0 != pXtnt->rsvd2.cell)) {
        pXtnt->type = 0;
	pXtnt->rsvd1 = 0;
	pXtnt->rsvd2.page = 0;
	pXtnt->rsvd2.cell = 0;
	pXtnt->segmentSize = 0; /* Not checked as can contain garbage */
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_904,
					   "Modified RBMT/BMT0 mcell %d, reset empty fields.\n"),
				   mcellNum);
	if (SUCCESS != status) {
	    return status;
	}
	modified = TRUE;
    }
	  
    /*
     * The deferred delete links in the rbmt will always be empty.
     */
    if ((0 != pXtnt->delLink.nextMCId.page) ||
	(0 != pXtnt->delLink.nextMCId.cell) ||
	(0 != pXtnt->delLink.prevMCId.page) ||
	(0 != pXtnt->delLink.prevMCId.cell) ||
	(0 != pXtnt->delRst.mcid.page)      ||
	(0 != pXtnt->delRst.mcid.cell)      ||
	(0 != pXtnt->delRst.vdIndex)        ||
	(0 != pXtnt->delRst.xtntIndex)      ||
	(0 != pXtnt->delRst.offset)         ||
	(0 != pXtnt->delRst.blocks)) {
        pXtnt->delLink.nextMCId.page = 0; 
	pXtnt->delLink.nextMCId.cell = 0;
	pXtnt->delLink.prevMCId.page = 0;
	pXtnt->delLink.prevMCId.cell = 0;
	pXtnt->delRst.mcid.page = 0;      
	pXtnt->delRst.mcid.cell = 0;      
	pXtnt->delRst.vdIndex = 0;       
	pXtnt->delRst.xtntIndex = 0;     
	pXtnt->delRst.offset = 0;        
	pXtnt->delRst.blocks = 0;
	status = add_fixed_message(&(pPage->messages), RBMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_907, 
					   "Modified RBMT/BMT0 mcell %d, reset deferred delete links.\n"),
				   mcellNum);
	if (SUCCESS != status) {
	    return status;
	}
	modified = TRUE;
    }

    /*
     * vdBlk must either be -1, even divisable by BLOCKS_PER_PAGE(16).
     *
     * Set vdBlk to "-1" hole if not.
     */
    for (x = 0; 
	 x < (pXtnt->firstXtnt.xCnt - 1) && x < (BMT_XTNTS - 1);
	 x++) { 
	if ((-1 != pXtnt->firstXtnt.bsXA[x].vdBlk) &&
	    (0 != pXtnt->firstXtnt.bsXA[x].vdBlk % BLOCKS_PER_PAGE)) {

	    status = add_fixed_message(&(pPage->messages), RBMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_668, 
					       "Modified RBMT/BMT0 mcell %d's vdBlk from %d to %d.\n"),
				       mcellNum,
				       pXtnt->firstXtnt.bsXA[x].vdBlk, 
				       -1);
	    if (SUCCESS != status) {
		return status;
	    }

	    pXtnt->firstXtnt.bsXA[x].vdBlk = -1;
        }
    } /* end extent loop */

    if (TRUE == modified) {
	return FIXED;
    } else {
	return SUCCESS;
    }
} /* correct_bsr_xtnts_default_values */

/* end fixfdmn_rbmt.c */
