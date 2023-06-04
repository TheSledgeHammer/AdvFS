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
 * 
 */
#pragma ident "@(#)$RCSfile: fixfdmn_bmt.c,v $ $Revision: 1.1.44.6 $ (DEC) $Date: 2006/03/17 03:03:55 $"

#include "fixfdmn.h"

/*
 * Global
 */
extern nl_catd _m_catd;


/*
 * Function Name: correct_bmt_mcell_data (3.38)
 *
 * Description: This routine checks and fixes the mcell data in the
 *              BMT. This routine is called before the root tag file
 *              and tag file are read so it cannot validate set tags
 *              or tags.
 *
 * Input parameters:
 *     domain: The domain being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, FAILURE, or NO_MEMORY 
*/
int correct_bmt_mcell_data (domainT *domain)
{
    char *funcName = "correct_bmt_mcell_data";
    int  fixedBMT; /* Flag set if we fixed anything in the BMT. */
    LBNT nextLBN;  /* LBN of the start of the next BMT page. */
    LBNT currLBN;  /* Logical block number of the start of the BMT page. */
    int  inExtent; /* Indicates that the page is not the first in an extent. */
    int  *bmtPageStatus; /* Array for each BMT page which indicate if the page
			    is full and if it is on the free list.*/
    mcellUsage freeMcell[BSPG_CELLS]; /* Array which indicate if the mcell is 
					 FREE, USED, and then used to see if a
					 link to it was used (ONLIST). */
    int  freeListCount; /* number of mcells in the free mcell list*/
    int  linksReset; /* Have we reset the links in the mcell free list*/
    int  pageNumber; /* The number of the BMT page being processed. */
    int  volNum;
    int  bmtXtnt;
    int  modified;
    int  vol;
    int  page;
    int  mcell;
    int  status;
    int  bmtLBNSize;
    int  isPrimary;
    int  isFsstat;
    int  dmnVersion;
    pageT  *pPage;  /* The current cache page we are working on */
    bsMPgT *bmtPage; /* The current BMT page we are working on */
    bsMCT  *pMcell;  /* The current BMT mcell we are working on */
    pagetoLBNT *bmtLBNArray;
    volumeT *volume;

    /*
     * Init variables
     */
    dmnVersion = domain->dmnVersion;

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	if (-1 == domain->volumes[volNum].volFD) {
	    continue;
	}
	
	/*
	 * Init variables.
	 */
	fixedBMT = FALSE;
	pageNumber = 0;
	volume = &(domain->volumes[volNum]);
	bmtLBNArray = volume->volRbmt->bmtLBN;
	bmtLBNSize  = volume->volRbmt->bmtLBNSize;
	
	bmtPageStatus = (int *)ffd_calloc(bmtLBNArray[bmtLBNSize].page, 
					  sizeof(int));
	if (NULL == bmtPageStatus) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_109, 
			     "BMT page status"));
	    return NO_MEMORY;
	}

	for (bmtXtnt = 0; bmtXtnt < bmtLBNSize; bmtXtnt++) {
	    inExtent = FALSE;
	    for (pageNumber = bmtLBNArray[bmtXtnt].page;
		 pageNumber < bmtLBNArray[bmtXtnt+1].page;
		 pageNumber++) {
		if ((0 == pageNumber) && (3 == dmnVersion)) {
		    /*
		     * Skip BMT page 0 since it was already processed.
		     */
		    continue;
		}
		currLBN = (bmtLBNArray[bmtXtnt].lbn + 
			   (BLOCKS_PER_PAGE * (pageNumber - bmtLBNArray[bmtXtnt].page)));

		if (pageNumber == bmtLBNArray[bmtXtnt + 1].page - 1) {
		    nextLBN = bmtLBNArray[bmtXtnt + 1].lbn;
		} else {
		    nextLBN = currLBN + BLOCKS_PER_PAGE;
		}

		modified = FALSE;
		status = read_page_from_cache(bmtLBNArray[bmtXtnt].volume, 
					      currLBN, &pPage);
		if (SUCCESS != status) {
		    return status;
		} 
		bmtPage = (bsMPgT *)pPage->pageBuf;

		status = validate_bmt_page(domain, pPage, pageNumber, FALSE);
		if (SUCCESS != status) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_110,
				     "Invalid BMT page on volume %s page %d, reinitializing it.\n"),
			     volume->volName, pageNumber);
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_111, 
						       "Reinitialized BMT page %d.\n"),
					       pageNumber);
		    if (SUCCESS != status) {
			return status;
		    }

		    /*
		     * Initialize this page to an empty page.
		     */
		    bzero(bmtPage, sizeof(bsMPgT));
		    bmtPage->pageId       = pageNumber;
		    bmtPage->megaVersion  = dmnVersion;
		    bmtPage->nextFreePg   = 0;
		    bmtPage->nextfreeMCId.page = pageNumber;
		    bmtPage->nextfreeMCId.cell = 0;
		    bmtPage->freeMcellCnt = BSPG_CELLS;

		    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
			pMcell = &(bmtPage->bsMCA[mcell]);
			pMcell->nextMCId.page = pageNumber;
			pMcell->nextMCId.cell = mcell + 1;
		    }

		    pMcell = &(bmtPage->bsMCA[LAST_MCELL]);
		    pMcell->nextMCId.page = 0;
		    pMcell->nextMCId.cell = 0;

		    fixedBMT = TRUE;

		    status = release_page_to_cache(bmtLBNArray[bmtXtnt].volume,
						   currLBN, pPage, 
						   TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		    continue;
		} /* end if status on validate_page */

		/*
		 * Now look for errors and fix them.
		 */
		status = correct_bmt_page_header(pageNumber, volNum,
						 inExtent, nextLBN,
						 pPage, domain,
						 FALSE);
		if (SUCCESS != status) {
		    if (FIXED == status) {
			modified = TRUE;
		    } else {
			return status;
		    }
		}

		/*
		 * NOTE: PL_PAGE type might cause some problems
		 *       It is believed we will reinit the page, but
		 *       this will require further testing.
		 */
		/*
		 * Now walk the mcells.
		 */
		for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
		    status = correct_mcell_header(pPage, volNum,
						  pageNumber, 
						  mcell, domain);
		    if (SUCCESS != status) {
			if (FIXED == status) {
			    modified = TRUE;
			} else {
			    return status;
			}
		    }
		    
		    pMcell = &(bmtPage->bsMCA[mcell]);

		    /*
		     * Check to see if the mcell is USED or FREE.
		     *
		     * There is a special case.  The tag looks like
		     * it is 'Free' based on the tagNum and setNum
		     * but it is actually 'Used'.
		     */
		    if (((0 == pMcell->tag.num) &&
			(0 == pMcell->bfSetTag.num)) &&
			(!((0 == mcell) &&
			 (((3 == dmnVersion) && (1 == pageNumber)) ||
			  ((dmnVersion > 3) && (0 == pageNumber)))))) {
			freeMcell[mcell] = FREE;
		    } else {
			mcellNodeT *pMcellNode;

			freeMcell[mcell] = USED;
			status = correct_mcell_records(domain, pPage, 
						       volNum, pageNumber,
						       mcell, &isPrimary,
						       &isFsstat);
			if (SUCCESS != status) {
			    if (FIXED == status) {
				modified = TRUE;
			    } else if (DELETED == status) {
				freeMcell[mcell] = FREE;
			    } else {
				return status;
			    }
			}

			/*
			 * Insert this mcell into the skip list.
			 */
			pMcellNode =
			    (mcellNodeT *)ffd_malloc(sizeof(mcellNodeT));
			if (NULL == pMcellNode) {
			    writemsg(SV_ERR | SV_LOG_ERR, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
					     "Can't allocate memory for %s.\n"),
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_112, 
					     "mcell node"));
			    return NO_MEMORY;
			}

			pMcellNode->mcellId.vol  = volNum;
			pMcellNode->mcellId.page = pageNumber;
			pMcellNode->mcellId.cell = mcell;
			pMcellNode->status       = 0;
			pMcellNode->tagSeq.num   = pMcell->tag.num;
			pMcellNode->tagSeq.seq   = pMcell->tag.seq;
			pMcellNode->setTag.num   = pMcell->bfSetTag.num;
			pMcellNode->setTag.seq   = pMcell->bfSetTag.seq;
			if (TRUE == isPrimary) {
			    MC_SET_PRIMARY(pMcellNode->status);
			}

			if (TRUE == isFsstat) {
			    MC_SET_FSSTAT(pMcellNode->status);
			}

			if ((0 == mcell) &&
			    (((3 == dmnVersion) && (1 == pageNumber)) ||
			     ((dmnVersion > 3) && (0 == pageNumber)))) {
			    /*
			     * Special case no one pointes to this mcell.
			     */
			    MC_SET_FOUND_PTR(pMcellNode->status);
			}


			status = insert_node(domain->mcellList, 
					     pMcellNode);
			if (SUCCESS != status) {
			    return status;
			}
		    } /* end if free or full */
		} /* end mcell loop */

		/*
		 * Now we are going to check the free mcell list.
		 */
		freeListCount = 0;
		linksReset    = FALSE;
		page          = bmtPage->nextfreeMCId.page;
		mcell         = bmtPage->nextfreeMCId.cell;

		/*
		 * Loop until the end of the list (0,0)
		 */
		while ((0 != page) || (0 != mcell)) {
		    if (FREE == freeMcell[mcell]) {
			freeMcell[mcell] = ONLIST;
			freeListCount++;
			pMcell = &(bmtPage->bsMCA[mcell]);
			page  = pMcell->nextMCId.page;
			mcell = pMcell->nextMCId.cell;
		    } else {
			/*
			 * There is a free mcell pointing to an active
			 * one or there is a loop in the free mcell 
			 * list, so reset the links on the free mcells.
			 * Active mcells pointing to free ones will be
			 * caught in correct_nodes.
			 */
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_113, 
					 "Invalid free mcell links on volume %s BMT page %d.\n"),
				 volume->volName, pageNumber);
			status = reset_free_mcell_links(pPage, freeMcell);
			if (SUCCESS != status) {
			    return status;
			}
			    
			status = add_fixed_message(&(pPage->messages),
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_114, 
							   "Reset free mcell links on BMT page %d.\n"),
						   pageNumber);
			if (SUCCESS != status) {
			    return status;
			}

			linksReset = TRUE;
			modified   = TRUE;
			mcell      = 0;
			page       = 0;
		    } /* end of if on FREE list */
		} /* end of while loop on free list */

		if (FALSE == linksReset) {
		    if (freeListCount != bmtPage->freeMcellCnt) {
			writemsg(SV_VERBOSE | SV_LOG_FOUND, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_113,
					 "Invalid free mcell links on volume %s BMT page %d.\n"),
				 volume->volName, pageNumber);
			/*
			 * Not all of the free mcells were in the free 
			 * list, so reset all of the links.
			 */
			status = reset_free_mcell_links(pPage, freeMcell);
			if (SUCCESS != status) {
			    return status;
			}
			linksReset = TRUE;
			modified   = TRUE;
			status = add_fixed_message(&(pPage->messages),
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_114, 
							   "Reset free mcell links on BMT page %d.\n"),
						   pageNumber);
			if (SUCCESS != status) {
			    return status;
			}
		    }
		}

		/*
		 * RBMT does not use this.
		 */
		if (NULL != bmtPageStatus) {
		    if (0 == bmtPage->freeMcellCnt) {
			bmtPageStatus[pageNumber] = BMTPAGE_FULL;
		    } else {
			bmtPageStatus[pageNumber] = BMTPAGE_FREE;
		    }
		}

		status = release_page_to_cache(bmtLBNArray[bmtXtnt].volume,
					       currLBN, pPage, 
					       modified, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		if (TRUE == modified) {
		    fixedBMT = TRUE;
		}
		modified = FALSE;
		inExtent = TRUE;
	    } /* End page number loop */
	} /* End extent loop */

	status = correct_free_mcell_list(volume, bmtPageStatus);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		modified = TRUE;
		fixedBMT = TRUE;
	    } else {
		return status;
	    }
	}
	free (bmtPageStatus);
    } /* End active volume */
    
    if (TRUE == fixedBMT) {
	writelog(SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_50,
			 "Fixed errors in BMT mcell data.\n"));
	return FIXED;
    }
    return SUCCESS;
}/* end correct_bmt_mcell_data */


/*
 * Function Name: correct_bmt_page_header (3.39)
 *
 * Description: This routine checks and fixes a BMT page header.
 *
 * Input parameters:
 *     pageNumber: The BMT page number that this page should be.
 *     volNum: The volume we are working on.
 *     inExtent: Indicating that we are not at the first block of an extent.
 *     nextLBN: The LBN of the next page to be processed.
 *     page: The BMT page to process.
 *     domain: The domain being worked on.
 *     rbmtFlag: Is this bmt page in the RBMT/BMT0
 *
 * Output parameters: N/A 
 *
 * Returns: FIXED, SUCCESS, or NO_MEMORY
 */
int correct_bmt_page_header(int pageNumber, int volNum, int inExtent, 
			    LBNT nextLBN, pageT *pPage,
			    domainT *domain, int rbmtFlag)
{
    char   *funcName = "correct_bmt_page_header";
    int    freeCount; /* Count of free mcells on page. */
    bsMPgT *bmtPage;  /* The current BMT page we are working on */
    bsMCT  *pMcell;   /* The current BMT mcell we are working on */
    char   errorMsg[MAXERRMSG];
    int    modified;
    int    status;
    int    mcell;
    int    dmnVersion;
    signed int terminator;
    int    maxBmtPage;
    volumeT *volume;
    int    lastValidMcell;

    /*
     * Init Variables.
     */
    modified = FALSE;
    bmtPage = (bsMPgT *)pPage->pageBuf;
    dmnVersion = domain->dmnVersion;
    volume = &(domain->volumes[volNum]);

    if (TRUE != rbmtFlag) {
	maxBmtPage = volume->volRbmt->bmtLBN[volume->volRbmt->bmtLBNSize].page;
    }

    if (3 == dmnVersion) {
	terminator = 0;
    } else {
	terminator = -1;
    }

    if (TRUE == rbmtFlag && 4 == dmnVersion) {
	lastValidMcell = 26;
    } else {
	lastValidMcell = 27;
    }

    if (bmtPage->megaVersion != dmnVersion) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_116, 
			 "Invalid domain version on volume %s BMT page %d.\n"),
		 volume->volName, pageNumber);

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_117,
					   "Modified BMT page %d domain version from %d to %d.\n"),
				   pageNumber, bmtPage->megaVersion, 
				   domain->dmnVersion);
	if (SUCCESS != status) {
	    return status;
	}
        bmtPage->megaVersion = domain->dmnVersion;
	modified = TRUE;
    }

    if (bmtPage->pageId != pageNumber) { 
       /*
	* if the page is within an extent, then number is corrupt, so fix it.
	*/
        if (TRUE == inExtent) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_118, 
			     "Invalid page number on volume %s's BMT page %d.\n"),
		     volume->volName, pageNumber);
	    
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_119, 
					       "Modified BMT page %d's number from %d to %d.\n"),
				       pageNumber, bmtPage->pageId,
				       pageNumber);
	    if (SUCCESS != status) {
		return status;
	    }

	    bmtPage->pageId = pageNumber;
	    modified = TRUE;
	} else if ( (LBNT) -1 == nextLBN) {
	    /*
	     * If we have a 1 page extent as the last page of the 
	     * BMT we don't want to read the nextLBN as it will be 
	     * '-1'.  So we have this special check for that case.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_118,
			     "Invalid page number on volume %s's BMT page %d.\n"),
		     volume->volName, pageNumber);

	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_119, 
					       "Modified BMT page %d's number from %d to %d.\n"),
				       pageNumber, bmtPage->pageId, 
				       pageNumber);
	    if (SUCCESS != status) {
		return status;
	    }
	    bmtPage->pageId = pageNumber;
	    modified = TRUE;
	} else {
	    pageT *pTempPage;
	    bsMPgT *bmtTempPage; 
	    /*
	     * If the page is at the start of an extent, there could be 
	     * corruption in the extent, so see if the next page has the next 
	     * page number in sequence. if so, then set the page number on 
	     * this page.
	     */

	    status = read_page_from_cache(volNum, nextLBN, &pTempPage);
	    if (SUCCESS != status) {
	        return status;
	    } 

	    bmtTempPage = (bsMPgT *)pTempPage->pageBuf;

	    if (bmtTempPage->pageId == pageNumber + 1) { 
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_118, 
				 "Invalid page number on volume %s's BMT page %d.\n"),
			 volume->volName, pageNumber);
		
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_119, 
						   "Modified BMT page %d's number from %d to %d.\n"),
					   pageNumber, bmtPage->pageId,
					   pageNumber);
		if (SUCCESS != status) {
		    return status;
		}
	        bmtPage->pageId = pageNumber;
		modified = TRUE;
	    } else { 

	        /* 
		 * At this point, we can't tell if we just have
		 * corrupted page numbers on multiple pages, or if there
		 * are actually missing BMT pages. In either case, we
		 * just exit here. In a future release we may want to
		 * search the disk looking for missing BMT pages or
		 * allocate some empty pages to fill in the missing
		 * ones.  
		 */
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_121, 
				 "Volume %s BMT page %d header's page ID is not fixable.\n"), 
			 volume->volName, pageNumber);

	        corrupt_exit(NULL);
	    }
	    status = release_page_to_cache(volNum, nextLBN, pTempPage,
					   FALSE, FALSE);
	    if (SUCCESS != status) {
	        return status;
	    }
	}/* end if start of extent or middle */
    } /* end if page number is incorrect */


    /*
     * Check if nextFreePg is out of bounds.
     */
    if (TRUE == rbmtFlag) {
	if (bmtPage->nextFreePg != 0) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_122, 
					       "Modified RBMT page %d's nextFreePg from %d to %d.\n"),
				       pageNumber, bmtPage->nextFreePg, 0);
	    if (SUCCESS != status) {
		return status;
	    }
	    bmtPage->nextFreePg = 0;
	    modified = TRUE;
	}
    } else {
	if (((signed int)(bmtPage->nextFreePg) < terminator) ||
	    ((signed int)(bmtPage->nextFreePg) > maxBmtPage)) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_123, 
					       "Modified BMT page %d's nextFreePg from %d to %d.\n"),
				       pageNumber, bmtPage->nextFreePg,
				       terminator);
	    if (SUCCESS != status) {
		return status;
	    }
	    bmtPage->nextFreePg = terminator;
	    modified = TRUE;
	}
    }

    /*
     * Check if nextfreeMCId is out of bounds.
     */
    if ((bmtPage->nextfreeMCId.cell > lastValidMcell) ||
	((pageNumber != bmtPage->nextfreeMCId.page) &&
	 ((0 != bmtPage->nextfreeMCId.page) ||
	  (0 != bmtPage->nextfreeMCId.cell)))) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_928,
					   "Modified BMT page %d's nextfreeMCId from (%d,%d) to (%d,%d).\n"),
				   pageNumber, bmtPage->nextfreeMCId.page,
				   bmtPage->nextfreeMCId.cell, 0, 0);
	if (SUCCESS != status) {
	    return status;
	}
	bmtPage->nextfreeMCId.page = 0;
	bmtPage->nextfreeMCId.cell = 0;
	modified = TRUE;
    }

    /*
     * Check if the free mcell count in the header is correct.
     *
     * Future: We may need to special case PL_PAGE.
     */
    freeCount = 0;

    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
        pMcell = &(bmtPage->bsMCA[mcell]);
	if ((0 == pMcell->tag.num) && (0 == pMcell->bfSetTag.num)) {
	    freeCount++;
	}
    }

    /*
     * Special case BMT page 0 (ODSV4) or page 1 (ODSV3).
     * Count will be off by one due to mcell 0.  Make sure
     * we don't do this on the rbmt page
     */
    if (FALSE == rbmtFlag) {
        if (3 == domain->dmnVersion) {
	    if (1 == pageNumber) {
	        freeCount--;
	    }
	} else {
	    if (0 == pageNumber) {
	        freeCount--;
	    }
	}
    }

    if (bmtPage->freeMcellCnt != freeCount) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_124, 
					   "Modified BMT page %d's freeCount from %d to %d.\n"),
				   pageNumber, bmtPage->freeMcellCnt,
				   freeCount);
	if (SUCCESS != status) {
	    return status;
	}
        bmtPage->freeMcellCnt = freeCount;
	modified = TRUE;
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_125, 
			 "Found problems on volume %s BMT page %d's header.\n"),
		 volume->volName, pageNumber);
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bmt_page_header */


/*
 * Function Name: correct_mcell_header (3.40) 
 *
 * Description: This function checks and fixes mcell headers.
 *
 *	Normal Cases	tag		SetTag
 *      File		> 0              > 0
 *      Empty             0                0
 *      fileset         > 0               -2
 *
 *      Following will be fixed when we follow chains:
 *
 *                       0               Any but 0
 *                       Any but 0       0
 *                      > 0              < 0 (but not -2)
 *
 * Input parameters:
 *     pPage: The page to check the header on.
 *     volNum: The volume the mcell is on.
 *     pageNum: The page the mcell is on.
 *     mcellId: Which mcell on the page to check.
 *     domain: The domain being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS or FIXED.
 */
int correct_mcell_header(pageT *pPage, int volNum, int pageNum, 
			 int mcellId, domainT *domain)
{
    char   *funcName = "correct_mcell_header";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* The mcell we are working on. */
    int    modified;   /* Flag letting us know if we have made changes. */
    int    nextVolNum;
    int    status;
    volumeT *volume;

    /*
     * Init Variables.
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellId]);
    volume = &(domain->volumes[volNum]);

    if ((0 == pMcell->tag.num) && (0 == pMcell->bfSetTag.num)) {
        /* 
	 * Will handle case of one but not both being '0' when
	 * we follow chains, as we could find the correct tag 
	 * and or settag at that point.
	 */

        if (0 != pMcell->nextVdIndex) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_126, 
					       "Modified mcell (%d,%d,%d)'s nextVdIndex from %d to %d.\n"),
				       volNum, pageNum, mcellId,
				       pMcell->nextVdIndex,
				       0);
	    if (SUCCESS != status) {
		return status;
	    }
	    pMcell->nextVdIndex = 0;
	    modified = TRUE;
	}
	/*
	 * make sure nextMCId is on this page (free mcell).
	 */
	if (pMcell->nextMCId.page != pageNum) {
	    if ((0 == pMcell->nextMCId.page) && 
		(0 == pMcell->nextMCId.cell)) {
	        /*
		 * special case - tail of list.
		 */
	    } else {
	        /*
		 * will be readded to free list later.
		 */
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_127, 
						   "Modified mcell (%d,%d,%d)'s nextMCId %d,%d to %d,%d.\n"),
					   volNum, pageNum, mcellId,
					   pMcell->nextMCId.page,
					   pMcell->nextMCId.cell,
					   0, 0);
		if (SUCCESS != status) {
		    return status;
		}
		pMcell->nextMCId.page = 0;
		pMcell->nextMCId.cell = 0;
		modified = TRUE;
	    }
	}
    } else {
	/*
	 * Else tag and set tag are not zero.
	 */

        /* 
	 * now we check to see if tag and seq are valid.
	 *
	 * pMcell->linkSegment is checked and fixed when we follow NEXT chains.
	 */
        if ((-2 == pMcell->bfSetTag.num) && (pMcell->tag.num > 0)) {
	    pageT  *pTempPage;
	    bsMPgT *pTempBMTPage;  
	    bsMCT  *pTempMcell;
	    bsMRT  *pRecord;
	    int    otherPage;
	    LBNT   tmpLbn;
	    int    tmpVol;

	    otherPage = FALSE;

	    /*
	     * This could be the head of the primary chain of a fileset.
	     */
	    if (0 == pMcell->nextVdIndex) {
	        /*
		 * The tail of the chain, trust the header.
		 */
	    } else {
	        /*
		 * Could be the head, or in the middle of the chain.
		 */
	        if ((pMcell->nextVdIndex == volNum) &&
		    (pMcell->nextMCId.page == pageNum)) {
		    /*
		     * Next mcell on the same BMT page
		     */
		    pTempMcell = &(pBMTPage->bsMCA[pMcell->nextMCId.cell]);
		} else {
		    pagetoLBNT *bmtLbn;
		    int        size;

		    /*
		     * mcell falls on another BMT page.
		     */
		    otherPage  = TRUE;
		    nextVolNum = pMcell->nextVdIndex;
		    bmtLbn     = domain->volumes[nextVolNum].volRbmt->bmtLBN;
		    size       = domain->volumes[nextVolNum].volRbmt->bmtLBNSize;

		    status = convert_page_to_lbn(bmtLbn,
						 size,
						 pMcell->nextMCId.page,
						 &tmpLbn, 
						 &tmpVol);
		    if (SUCCESS != status) {
		        writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
					 "Can't convert BMT page %d to LBN.\n"), 
				 pMcell->nextMCId.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(tmpVol, tmpLbn, &pTempPage);
		    if (SUCCESS != status) {
		        return status;
		    } 

		    pTempBMTPage = (bsMPgT *)pTempPage->pageBuf;
		    pTempMcell   = &(pTempBMTPage->bsMCA[pMcell->nextMCId.cell]);
		} /* end if on a different page */

		/*
		 * We now have the next mcell in the Next chain.
		 * If it is the primary mcell the next will have a BFS_ATTR
		 * record, if it is the head of the extent list then it
		 * should be a XTRA_XTNTS record.
		 */
		pRecord = (bsMRT *)pTempMcell->bsMR0;

		if ((BSR_BFS_ATTR != pRecord->type) &&
		    (BSR_XTRA_XTNTS != pRecord->type)) {
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_129, 
						       "Freed mcell %d on BMT page %d, the next chain was corrupt.\n"),
					       mcellId, pageNum);
		    if (SUCCESS != status) {
			return status;
		    }

		    /*
		     * Free Original mcell, not the next 
		     */
		    status = free_mcell(domain, volNum, pPage, mcellId);
		    if (SUCCESS != status) {
		        writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_130,
					 "Can't free mcell %d on volume %s BMT page %d.\n"), 
				 mcellId, volume->volName, pageNum);
			return FAILURE;
		    }
		    modified = TRUE;
		}

		if (TRUE == otherPage) {
		   status = release_page_to_cache(tmpVol, tmpLbn, pTempPage, 
						  FALSE, FALSE);
		   if (SUCCESS != status) {
		       return status;
		   }
		}
	    } /* end if not head of fileset primary chain */
	} /* end if a fileset mcell */ 
	else if (pMcell->bfSetTag.num < 0) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_131,
					       "Freed mcell %d on BMT page %d, the setTag was negative.\n"),
				       mcellId, pageNum);
	    if (SUCCESS != status) {
		return status;
	    }

	    /*
	     * Only negitive for primary fileset mcells.
	     */
	    status = free_mcell(domain, volNum, pPage, mcellId);
	    if (SUCCESS != status) {
	        writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_130,
				 "Can't free mcell %d on volume %s BMT page %d.\n"), 
			 mcellId, volume->volName, pageNum);
		return FAILURE;
	    }
	    modified = TRUE;
	} /* end if bfSetTag is negative */
    } /* end if empty/valid mcell */
    
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_133,
			 "Found corruption in mcell (%d,%d,%d)'s header.\n"),
		 volNum, pageNum, mcellId);
        return FIXED;
    }

    return SUCCESS;
} /* end correct_mcell_header */


/*
 * Function Name:correct_mcell_records (3.41)
 *
 * Description: This routine checks and fixes mcell records in the
 *              BMT. It is called from correct_bmt_mcell_data.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     page: The page which mcell is on.
 *     volNum: The volume we are working on.
 *     pageNum: The page number of the page we are working on.
 *     mcellNum: The mcell to be checked and fixed
 *     isPrimary: This mcell is the primary mcell.
 *     isFsstat: This mcell contains the fsstat record.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, CORRUPT, FIXED.  
 */
int correct_mcell_records(domainT *domain, pageT *pPage, 
			  int volNum, int pageNum, int mcellNum,
			  int *isPrimary, int *isFsstat)
{
    char   *funcName = "correct_mcell_record";
    bsMPgT *pBMTPage; /* The current RBMT page we are working on */
    bsMCT  *pMcell;   /* Pointer to current mcell. */
    bsMRT  *pRecord;  /* Pointer to current mcell record. */
    int    modified;  /* Let the tool know if page has been modified. */
    int    status;
    int    tag;
    int    recordType;
    int    sizeUsed;

    /*
     * Init variables
     */
    modified = FALSE;
    sizeUsed = 0;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    tag      = pMcell->tag.num;
    *isPrimary = FALSE;
    *isFsstat  = FALSE;
    
    /*
     * Now loop through all records in this mcell.
     */
    while ((0 != pRecord->type) &&
	   (0 != pRecord->bCnt) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	status = correct_mcell_record_bcnt(domain, pPage, mcellNum, 
					   pRecord, sizeUsed);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		modified = TRUE;
	    } else if (CORRUPT == status) {
		/*
		 * This record would overwrite the next mcell.  So we are 
		 * just going to truncate the mcell.
		 */
		pRecord->bCnt = 0;
		pRecord->type = 0;
		pRecord->version = 0;
		modified = TRUE;
		break; /* out of while loop */
	    } else {
		return status;
	    }
	}

        switch (pRecord->type) {
	    case (BSR_XTNTS):
		status = correct_bsr_xtnts_record(pPage, mcellNum, 
						  pRecord);    
		break;

	    case (BSR_ATTR):
		status = correct_bsr_attr_record(pPage, mcellNum,
						 pRecord, FALSE);
		if ((SUCCESS == status) || (FIXED == status)) {
		    *isPrimary = TRUE;
		} /* end if success or fixed */
		break;

	    case (BSR_XTRA_XTNTS):
		status = correct_bsr_xtra_xtnts_record(domain, volNum, 
						       pPage, mcellNum,
						       pRecord);
		break;

	    case (BSR_SHADOW_XTNTS):
		status = correct_bsr_shadow_xtnts_record(domain, volNum, 
							 pPage, mcellNum,
							 pRecord);
		break;

	    case (BSR_BFS_ATTR):
		status = correct_bsr_bfs_attr_record(domain, pPage, 
						     mcellNum, pRecord);
		break;

	    case (BSR_BF_INHERIT_ATTR):
		status = correct_bsr_bf_inherit_attr_record(pPage, 
							    mcellNum, 
							    pRecord,
							    FALSE);
		break;

	    case (BSR_BFS_QUOTA_ATTR):
		status = correct_bsr_bfs_quota_attr_record(domain, pPage, 
							   mcellNum, 
							   pRecord);
		break;

	    case (BSR_PROPLIST_HEAD):
		status = correct_bsr_proplist_head_record(domain, pPage, 
							  mcellNum,
							  pRecord);
		break;

	    case (BSR_PROPLIST_DATA):
		status = correct_bsr_proplist_data_record(pPage, mcellNum, 
							  pRecord);
		break;

	    case (BMTR_FS_DIR_INDEX_FILE):
		status = correct_bmtr_fs_dir_index_file(pPage, mcellNum,
							pRecord);
		break;

	    case (BMTR_FS_INDEX_FILE):
		status = correct_bmtr_fs_index_file_record(pPage, mcellNum,
							   pRecord);
		break;

	    case (BMTR_FS_TIME):
		status = correct_bmtr_fs_time_record(domain, pPage, 
						     mcellNum, pRecord, 
						     tag);
		break;

	    case (BMTR_FS_UNDEL_DIR):
		status = correct_bmtr_fs_undel_dir_record(pPage, mcellNum, 
							  pRecord);
		break;

	    case (BMTR_FS_DATA):
		status = correct_bmtr_fs_data_record(pPage, mcellNum, 
						     pRecord);
		break;

	    case (BMTR_FS_STAT):
		status = correct_bmtr_fs_stat_record(pPage, mcellNum, 
						     pRecord);
		if ((SUCCESS == status) || (FIXED == status)) {
		    *isFsstat = TRUE;
		} /* end if success or fixed */
		break;

		/*
		 * Special cases follow.
		 */

	    case (BSR_MCELL_FREE_LIST):
		status = correct_bsr_mcell_free_list_record(domain, pPage, 
							    mcellNum, 
							    pRecord);
		break;

	    case (BSR_DEF_DEL_MCELL_LIST):
		status = correct_bsr_def_del_mcell_list_record(domain, 
							       pPage,
							       mcellNum,
							       pRecord);
		break;

	    case (BSR_RSVD17):
		status = correct_bsr_rsvd17_record(pPage, mcellNum, 
						   pRecord);
		break;

	    /*
	     * This group of records are only found in RBMT/BMT0, 
	     * so they are invalid record.
	     */
	    case (BSR_VD_ATTR):
	    case (BSR_DMN_ATTR):
	    case (BSR_VD_IO_PARAMS):
	    case (BSR_DMN_MATTR):
	    case (BSR_DMN_TRANS_ATTR):

	    /* 
	     * This group of records should never be found.
	     */
	    case (BSR_FREE_2):
	    case (BSR_RSVD11):
	    case (BSR_RSVD12):
	    case (BSR_RSVD13):
	    case (BMTR_FS_BACKLINK_obsolete):

	    /*
	     * Found an unknown mcell record type
	     */
	    default:
		/*
		 * We have an unknown record type which we can not 
		 * match to a known type.  This could happen if 
		 * AdvFS adds a record type without adding support 
		 * to fixfdmn.  It was decided not to delete this
		 * record, as we could lose some valid data.
		 */
		writemsg(SV_DEBUG | SV_LOG_WARN, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_135, 
				 "Unknown BMT record %d in mcell (%d,%d,%d).\n"),
			 pRecord->type, volNum, pageNum, mcellNum);
		break;
	} /* End Switch */

	if (SUCCESS != status) {
	    if (FIXED == status) {
	        modified = TRUE;
	    } else {
	        return status;
	    }
	}

	/*
	 * Increment sizeUsed by record size rounded up.
	 */
	sizeUsed += roundup(pRecord->bCnt, sizeof(int));
	
	/*
	 * Set pRecord to next record  of pMcell.
	 */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 
    } /* end pRecord loop */

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_136, 
			 "Found corruption in mcell (%d,%d,%d).\n"),
		 volNum, pageNum, mcellNum);
        return FIXED;
    }

    return SUCCESS;
} /* end correct_mcell_records */


/*
 * Function Name:match_record_byte_count(3.42)
 *
 * Description: This function will take a look at a buffer and try and
 *              figure out what mcell record type it is. Make sure
 *              that V4 ODS do not end up on V3 domains.
 *
 * Input parameters:
 *     pRecord: Pointer to a buffer containing a mcell record.
 *     pageNum: The page number in the BMT/RBMT this record is on.
 *     mcellNum: The mcell number that this record belongs to.
 *     rbmtFlag: Set to TRUE if checking RBMT/BMT0, FALSE if BMT.
 *
 * Output parameters: 
 *     recordType: The best guess on what the record type is.
 *
 * Returns: SUCCESS
 */
int match_record_byte_count(bsMRT *pRecord, int pageNum, int mcellNum,
                            int rbmtFlag, int *recordType)
{
    char *funcName = "match_record_byte_count";
    int  bCnt;    /* Size of the record (from header) */
    int  type;    /* Type of the record (from header) */
    int  version; /* Version of the record (from header) */
    bsBfClAttrT  *pBsBfClAttr;
    bsBfAttrT    *pBsBfAttr;


    type = pRecord->type;
    bCnt = pRecord->bCnt;
    version = pRecord->version;
    *recordType = 0;

    switch (bCnt) {
      case (8):
	  /*
	   * This could be either BSR_MCELL_FREE_LIST or BMTR_FS_TIME.
	   *
	   * NOTE: Special case for BSR_MCELL_FREE_LIST, as we have no
	   * way at this level to know the type of domain, and this is a
	   * very RARE record type we are going to accept the very very
	   * small chance that this record is corrupt on page 1 of V4
	   * domains.  
	   */
	  if (((1 == pageNum ) || (0 == pageNum)) && (0 == mcellNum)) {
	      *recordType = BSR_MCELL_FREE_LIST;
	  } else {
	      *recordType = BMTR_FS_TIME;
	  }
	break;

      case (12):
	  /*
	   * This can be BMTR_FS_UNDEL_DIR, BMTR_FS_DIR_INDEX_FILE or 
	   * BSR_DEF_DEL_MCELL_LIST.
	   *
	   * NOTE: Special case for BSR_DEF_DEL_MCELL_LIST, as we have
	   * no way at this level to know the type of domain, and this
	   * is a very RARE record type we are going to accept the very
	   * very small chance that this record is corrupt on page 1 of
	   * V4 domains.
	   *
	   * For the other two record types it is impossible to figure 
	   * out what it should be with out more data, as the data for 
	   * both of them is so close.  
	   */
	  if (((1 == pageNum ) || (0 == pageNum)) && (0 == mcellNum)) {
	      *recordType = BSR_DEF_DEL_MCELL_LIST;
	  }
	break;

      case (20):
	  *recordType = BMTR_FS_INDEX_FILE;
	  break;

      case (24):
          if (TRUE == rbmtFlag) {
	      *recordType = BSR_DMN_ATTR;
	  }
	  break;

      case (28):
          if (TRUE == rbmtFlag) {
	      *recordType = BSR_VD_IO_PARAMS;
	  }
	  break;

      case (32):
          if (TRUE == rbmtFlag) {
	      *recordType = BSR_DMN_FREEZE_ATTR;
	  }
	  break;

      case (36):
	  *recordType = BSR_RSVD17;
	  break;

      case (40):
          if (TRUE == rbmtFlag) {
	      *recordType = BSR_VD_ATTR; 
	  }
	  break;

      case (52):
          if (TRUE == rbmtFlag) {
	      *recordType = BSR_DMN_MATTR;
	  }
	  break;

      case (60):
          if (TRUE == rbmtFlag) {
	      *recordType = BSR_DMN_SS_ATTR;
	  }
	  break;

      case (64):
	  /*
	   * We have two structures which are different, compare the fields 
	   * to the known defaults and see if either of them stands out as 
	   * the correct record.
	   */

	  pBsBfClAttr = (bsBfClAttrT *)((char *)pRecord + sizeof(bsMRT));
	  
	  if ((1 == pBsBfClAttr->reqServices) &&
	      (pBsBfClAttr->dataSafety >= 0) &&
	      (pBsBfClAttr->dataSafety < 4)) {
	      /*
	       * There is a very SMALL chance this is set in the
	       * quota record.  It would mean that blkHLimitHi is
	       * set to '1'.  Also the quota record only exists
	       * once per fileset, while this exists for all directories.
	       */
	      *recordType = BSR_BF_INHERIT_ATTR;
	  } else {
              *recordType = BSR_BFS_QUOTA_ATTR;
	  }
	  break;

      case (80):
	  *recordType = BSR_XTNTS;
	  break;

      case (92):
          /*
	   * We have two structures which are different, compare the fields 
	   * to the known defaults and see if either of them stands out as 
	   * the correct record.
	   */
	  pBsBfAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

	  if (BLOCKS_PER_PAGE == pBsBfAttr->bfPgSz) {
	      /*
	       * This is one of the only fields in BSR_ATTR that
	       * we know should be a fixed value.  This variable
	       * maps to ino.seq of the stat record, which should
	       * never be equal to BLOCKS_PER_PAGE.
	       */
	      *recordType = BSR_ATTR;
	  } else {
	      *recordType = BMTR_FS_STAT;
	  }
	  break;

      case (220):
          *recordType = BSR_BFS_ATTR;
	  break;

      case (260):
          *recordType = BSR_SHADOW_XTNTS;
          break;

      case (264):
	  /*
	   * Note that BSR_PROPLIST_DATA is probably also this size, but
	   * it is so rare compared to XTRA_XTNTS that we're better off
	   * assuming it's an XTRA_XTNTS record.
	   */
          *recordType = BSR_XTRA_XTNTS;
	  break;

      default:
	  /*
	   * These are variable length records, or records which we don't care
	   * if they are deleted.
	   *
	   *  The size of BSR_PROPLIST_HEAD is NEW_ODS 24 + variable bufSize, 
	   *                                   OLD_ODS 16 + variable bufSize.
	   *  The size of BSR_PROPLIST_DATA is variable.
	   *  The size of BMTR_FS_DATA will be between 5 and 264.
	   */
	  break;
    } /* End Switch */

    return SUCCESS;
} /* end match_record_byte_count */


/*
 * Function Name: correct_bsr_xtnts_record (3.43)
 *
 * Description: This routine checks and fixes mcell records of type BSR_XTNTS.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.
 */
int correct_bsr_xtnts_record(pageT *pPage, int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_xtnts_record";
    bsMPgT *pBMTPage;  /* The current BMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    odsVersion;
    int    pageNum;
    int    volNum;
    int    status;
    bsXtntRT  *pXtnt;

    /*
     * Init Variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    odsVersion = pBMTPage->megaVersion;
    pageNum = pBMTPage->pageId;
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Look for known errors.
     */
    if ((0 != pXtnt->type) && (BSXMT_STRIPE != pXtnt->type)) {
        int oldType;
	oldType = pXtnt->type;

	if (8 == pXtnt->segmentSize) {
	    pXtnt->type = BSXMT_STRIPE; /* Striped */
	} else {
	  /*
	   * Most common is type 0.
	   */
	    pXtnt->type = 0;
	    pXtnt->segmentSize = 0;
	}
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_139, 
					   "Modified mcell (%d,%d,%d)'s xtnt type from %d to %d.\n"),
				   volNum, pageNum, mcellNum, oldType, 
				   pXtnt->type);
			if (SUCCESS != status) {
			    return status;
			}
	modified = TRUE;
    }

    /*
     * It was found while testing that segmentSize is not initialized
     * in all cases.  So we are not going to fix it UNLESS type is 2.
     *
     *  This was the old check:
     *    if (pXtnt->type == 0) && (pXtnt->segmentSize != 0)
     */

    if ((2 == pXtnt->type) && (8 != pXtnt->segmentSize)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_140, 
					   "Modified mcell (%d,%d,%d)'s xtnt segmentSize from %d to %d.\n"),
				   volNum, pageNum, mcellNum,
				   pXtnt->segmentSize, 8);
	if (SUCCESS != status) {
	    return status;
	}

        pXtnt->segmentSize = 8;
	modified = TRUE;
    }

    if (BLOCKS_PER_PAGE != pXtnt->blksPerPage) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_141, 
					   "Modified mcell (%d,%d,%d)'s xtnt blksPerPage from %d to %d.\n"),
				   volNum, pageNum, mcellNum, 
				   pXtnt->blksPerPage, BLOCKS_PER_PAGE);
	if (SUCCESS != status) {
	    return status;
	}

        pXtnt->blksPerPage = BLOCKS_PER_PAGE;
	modified = TRUE;
    }

    if ((3 == odsVersion) || (BSXMT_STRIPE == pXtnt->type)) {
        /*
	 * For ODSV3 and earlier these fields are only used for 'reserved'
	 * tags, and those are all in BMT0 so we don't have to worry about
	 * them here.
	 *
	 * For all versions of stripes, the extent data in this record
	 * can be random garbage.
	 */
    } else {
        /*
	 * For ODSV4 and later: mcellCnt should be 1 if chain is 0,0.
	 * Greater than 1 if chain is valid, so set to 2 for now
	 * we will set the correct value when we follow the chains.
	 *
	 * This is true UNLESS the file is striped.
	 */
        if (0 == pXtnt->firstXtnt.mcellCnt) {
	    if ((0 == pXtnt->chainMCId.page) && 
		(0 == pXtnt->chainMCId.cell)) {
	        pXtnt->firstXtnt.mcellCnt = 1;
	    } else {
	        pXtnt->firstXtnt.mcellCnt = 2;
	    }
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_142, 
					       "Modified mcell (%d,%d,%d)'s mcellCnt from %d to %d.\n"),
				       volNum, pageNum, mcellNum, 0,
				       pXtnt->firstXtnt.mcellCnt);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	} else if (1 == pXtnt->firstXtnt.mcellCnt) {
	    /*
	     * Should not have a chain.
	     */
	    if ((0 != pXtnt->chainMCId.page) ||
		(0 != pXtnt->chainMCId.cell) ||
		(0 != pXtnt->chainVdIndex)) {
		pXtnt->chainMCId.page = 0;
		pXtnt->chainMCId.cell = 0;
		pXtnt->chainVdIndex = 0;
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_143,
						   "Modified mcell (%d,%d,%d)'s chain to (%d,%d,%d), as mcellCnt was %d.\n"),
					   volNum, pageNum, mcellNum,
					   0, 0, 0, 1);
		if (SUCCESS != status) {
		    return status;
		}
		
		modified = TRUE;
	    }
	} else {
	    /*
	     * Chain better be set, if not mcellCnt is wrong.
	     */
	    if ((0 == pXtnt->chainMCId.page) &&
		(0 == pXtnt->chainMCId.cell) &&
		(0 == pXtnt->chainVdIndex)) {
	        pXtnt->firstXtnt.mcellCnt = 1;
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_144, 
						   "Modified mcell (%d,%d,%d)'s mcellCnt to %d, because chain is empty.\n"),
					   volNum, pageNum, mcellNum, 1);
		if (SUCCESS != status) {
		    return status;
		}

		modified = TRUE; 
	    }  
	} /* end mcellCnt check */

        /*
	 * Two cases:
	 *
	 * A) file with out extents : xcnt = 1, 0 -1 in extent
	 *
	 * B) file with extents     : xcnt = 2, 0  X
	 *                                      Y -1
	 *
	 * The LBN "X" will be checked later.
	 */
	if ((2 == pXtnt->firstXtnt.xCnt) &&
	    (0 == pXtnt->firstXtnt.bsXA[0].bsPage) &&
	    ((LBNT)-1 == pXtnt->firstXtnt.bsXA[1].vdBlk)) {
	    /*
	     * This is valid for a file with an extent.
	     */
	} else if ((1 == pXtnt->firstXtnt.xCnt) &&
		   (0 == pXtnt->firstXtnt.bsXA[0].bsPage) &&
		   ((LBNT)-1 == pXtnt->firstXtnt.bsXA[0].vdBlk)) {
	    /*
	     * This is valid for a file with NO extents.
	     */
	    /*
	     * NOTE : bsXA[1] is not initialized.
	     */
	} else {
	    /*
	     * Now handle the error cases.
	     */
	    if ((2 == pXtnt->firstXtnt.xCnt) &&
		(0 == pXtnt->firstXtnt.bsXA[1].bsPage)) {
		/*
		 * Error case
		 *                    xcnt = 2, 0  X
		 *                              0 -1
		 * Fix
		 *
		 *                    xcnt = 1, 0  -1
		 */
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_142, 
						   "Modified mcell (%d,%d,%d)'s mcellCnt from %d to %d.\n"),
					   volNum, pageNum, mcellNum,
					   pXtnt->firstXtnt.mcellCnt, 1);
		if (SUCCESS != status) {
		    return status;
		}

	        pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		pXtnt->firstXtnt.bsXA[0].vdBlk  = (LBNT)-1;
		pXtnt->firstXtnt.mcellCnt       = 1;
		modified = TRUE;
	    } else if ((1 == pXtnt->firstXtnt.xCnt) &&
		((0 != pXtnt->firstXtnt.bsXA[0].bsPage) || 
		 ((LBNT)-1 != pXtnt->firstXtnt.bsXA[0].vdBlk))) {
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_145, 
						   "Modified BMT page %d mcell %d, xCnt is %d, but no extents in record.\n"),
					   pageNum, mcellNum, 1);
		if (SUCCESS != status) {
		    return status;
		}

	        pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		pXtnt->firstXtnt.bsXA[0].vdBlk  = (LBNT)-1;
		modified = TRUE;
	    } else if ((2 == pXtnt->firstXtnt.xCnt) &&
		       ((0 != pXtnt->firstXtnt.bsXA[0].bsPage) || 
			((LBNT)-1 != pXtnt->firstXtnt.bsXA[1].vdBlk))) {
		pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		pXtnt->firstXtnt.bsXA[1].vdBlk  = (LBNT)-1;
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_146, 
						   "Modified BMT page %d mcell %d, extent record was not terminated.\n"),
					   pageNum, mcellNum);
		if (SUCCESS != status) {
		    return status;
		}
		modified = TRUE;
	    } else {
		if (((LBNT)-1 != pXtnt->firstXtnt.bsXA[0].vdBlk) ||
		    ((LBNT)-1 == pXtnt->firstXtnt.bsXA[1].vdBlk)) {
		    /*
		     * Has a valid LBN or has a "hole".
		     */
		    pXtnt->firstXtnt.xCnt = 2;
		    pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		    pXtnt->firstXtnt.bsXA[1].vdBlk  = (LBNT) -1;
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_147, 
						       "Modified BMT page %d mcell %d, xCnt set to %d.\n"),
					       pageNum, mcellNum, 2);
		    if (SUCCESS != status) {
			return status;
		    }
		    modified = TRUE;
		} else {
		    /*
		     * A single extent
		     */
		    pXtnt->firstXtnt.xCnt = 1;
		    pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		    pXtnt->firstXtnt.bsXA[0].vdBlk  = (LBNT) -1;
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_147,
						       "Modified BMT page %d mcell %d, xCnt set to %d.\n"),
					       pageNum, mcellNum, 1);
		    if (SUCCESS != status) {
			return status;
		    }
		    modified = TRUE;
		} /* end if single vs two extents */
	    } /* end if different extent corruption types */
	} /* end if/else extent checks */

	/*
	 * The first extent's vdBlk must either "-1", "-2" or be evenly 
	 * divisable by BLOCKS_PER_PAGE.
	 */
	if (((LBNT)-1 != pXtnt->firstXtnt.bsXA[0].vdBlk) &&
	    ((LBNT)-2 != pXtnt->firstXtnt.bsXA[0].vdBlk) &&
	    (0 != pXtnt->firstXtnt.bsXA[0].vdBlk % BLOCKS_PER_PAGE)) {
	    /*
	     * LBN not on page boundary, make it a hole (-1).
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_460,
					       "Modified mcell (%d,%d,%d) xtnt entry %d's LBN from %u to %d.\n"),
				       volNum, pageNum, mcellNum,
				       0, 
				       pXtnt->firstXtnt.bsXA[0].vdBlk, -1);
	    if (SUCCESS != status) {
		return status;
	    }
	    pXtnt->firstXtnt.bsXA[0].vdBlk = (LBNT)-1;
	    modified = TRUE;
	}
    } /* end else V4 ODS */

    /*
     * Don't need to check the delLink or delRst at this point.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_XTNTS");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_xtnts_record */


/*
 * Function Name:correct_bsr_attr_record(3.44)
 *
 * Description: This routine checks and fixes mcell records of
 *              type BSR_ATTR.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *     RBMTflag: Set to TRUE if checking RBMT/BMT0, FALSE if BMT.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, DELETED.  
 */
int correct_bsr_attr_record(pageT *pPage, int mcellNum, 
			    bsMRT *pRecord, int RBMTflag)
{
    char   *funcName = "correct_bsr_attr_record";
    bsMPgT *pBMTPage; /* The current RBMT page we are working on */
    bsMCT  *pMcell;   /* Pointer to current mcell. */
    int    modified;  /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    int    x;
    bsBfAttrT   *pBSRAttr;
   
    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check each field in BSR_ATTR looking for corruptions:
     */
    if ((pBSRAttr->state < BSRA_INVALID) || 
	(pBSRAttr->state > BSRA_VALID)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_149, 
					   "Modified mcell (%d,%d,%d)'s bsrAttr state from %d to BSRA_VALID.\n"),
				   volNum, pageNum, mcellNum, 
				   pBSRAttr->state);
	if (SUCCESS != status) {
	    return status;
	}

        pBSRAttr->state = BSRA_VALID;
	modified = TRUE;
    }

    if (BLOCKS_PER_PAGE != pBSRAttr->bfPgSz) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_150, 
					   "Modified mcell (%d,%d,%d)'s bsrAttr bfPgSz from %d to %d.\n"),
				   volNum, pageNum, mcellNum, 
				   pBSRAttr->bfPgSz, BLOCKS_PER_PAGE);
	if (SUCCESS != status) {
	    return status;
	}

        pBSRAttr->bfPgSz = BLOCKS_PER_PAGE;
	modified = TRUE;
    }

    if (1 != pBSRAttr->cl.reqServices) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_151, 
					   "Modified mcell (%d,%d,%d)'s bsrAttr reqServices from %d to %d.\n"),
				   volNum, pageNum, mcellNum, 
				   pBSRAttr->cl.reqServices, 1);
	if (SUCCESS != status) {
	    return status;
	}

        pBSRAttr->cl.reqServices = 1;
	modified = TRUE;
    }

#if 0
    /*
     * The deleteWithClone flag cannot be checked at this stage, as there
     * is the possibility of this fileset having a clone without the
     * BSR_ATTR->cloneCnt flag having been set.  This flag is checked
     * later when we follow chains.
     */
    if (0 != pBSRAttr->deleteWithClone) {
	if ((1 == pBSRAttr->deleteWithClone) &&
	    (0 != pBSRAttr->cloneCnt)) {
	    /*
	     * This file has a clone, and the original has been deleted.
	     */
	} else {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_152, 
					       "Modified mcell (%d,%d,%d)'s bsrAttr deleteWithClone from %d to %d.\n"),
				       volNum, pageNum, mcellNum,
				       pBSRAttr->deleteWithClone,
				       0);
	    if (SUCCESS != status) {
		return status;
	    }

	    pBSRAttr->deleteWithClone = 0;
	    modified = TRUE;
	}
    }
#endif

    /*
     * dataSafety -  Should be 0 for non RBMT/BMT0 mcells, exceptions being
     * directories and files which have been set to use 'logging'.
     */
        
    /*
     * following fields should be zero.
     */
    if ((0 != pBSRAttr->cl.optServices) ||
	(0 != pBSRAttr->cl.rsvd1)       ||
	(0 != pBSRAttr->cl.rsvd2)       ||
	(0 != pBSRAttr->cl.acl.num)     || 
	(0 != pBSRAttr->cl.acl.seq)     ||
	(0 != pBSRAttr->cl.rsvd_sec1)   ||
	(0 != pBSRAttr->cl.rsvd_sec2)   ||
	(0 != pBSRAttr->cl.rsvd_sec3)) {
        /*
	 * These values must be 0.
	 */
        pBSRAttr->cl.optServices = 0;
        pBSRAttr->cl.rsvd1 = 0;
        pBSRAttr->cl.rsvd2 = 0;
        pBSRAttr->cl.acl.num = 0;
        pBSRAttr->cl.acl.seq = 0;
        pBSRAttr->cl.rsvd_sec1 = 0;
        pBSRAttr->cl.rsvd_sec2 = 0;
        pBSRAttr->cl.rsvd_sec3 = 0;
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_153, 
					   "Cleared mcell (%d,%d,%d) bsrAttr's cl structure.\n"),
				   volNum, pageNum, mcellNum);
	if (SUCCESS != status) {
	    return status;
	}

	modified = TRUE;
    }

    for (x = 0; x < BS_CLIENT_AREA_SZ; x++) {
        if (0 != pBSRAttr->cl.clientArea[x]) {
	    pBSRAttr->cl.clientArea[x] = 0;
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_154,
					       "Modified mcell (%d,%d,%d)'s bsrAttr clientArea [%d] to %d.\n"),
				       volNum, pageNum, mcellNum, x, 0);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	}
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_attr_record */


/*
 * Function Name: correct_bsr_xtra_xtnts_record (3.45)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BSR_XTRA_XTNTS.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     volNum: The volume number we are working on.
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum:  the mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bsr_xtra_xtnts_record(domainT *domain, int volNum, pageT *pPage, 
				  int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_xtra_xtnts_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    extent;
    volumeT *volume;
    bsXtraXtntRT *pXtra;

    /*
     * Init variables.
     */
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;
    pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
    volume = &(domain->volumes[volNum]);

    /*
     * Check each field in BSR_XTRA_XTNTS looking for corruptions:
     */
    if (BLOCKS_PER_PAGE != pXtra->blksPerPage) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_155, 
					   "Modified mcell (%d,%d,%d)'s xtra blksPerPage from %d to %d.\n"),
				   volNum, pageNum, mcellNum, 
				   pXtra->blksPerPage, BLOCKS_PER_PAGE);
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
	 * The only valid values for the last vdblk are -1 and 0, but
	 * garbage has been known to show up here when the entry in the
	 * array is not in use.
	 *
	 * If vdBlk should not be 0 but instead -1, it will be set
	 * correctly in the next block of code.  
	 */
	pXtra->bsXA[pXtra->xCnt - 1].vdBlk = 0;
	modified = TRUE;

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_156, 
					   "Modified mcell (%1$d,%2$d,%3$d)'s xCnt to %4$d.\n"),
				   volNum, pageNum, mcellNum, 
				   pXtra->xCnt);
	if (SUCCESS != status) {
	    return status;
	}
    }

    if ((LBNT)-1 != pXtra->bsXA[pXtra->xCnt - 1].vdBlk) {
        int x;
	int endExtent;
	int found;
	
	endExtent = 0;
	found = FALSE;

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
	 * loop through extents 0 to (BMT_XTRA_XTNTS - 2).
	 */
	for (x = 0; (x <= (BMT_XTRA_XTNTS - 2) && FALSE == found); x++) {
	   /*
	    * Order of 'if's  is important
	    */
	    if ((3 == domain->dmnVersion) &&
		(pXtra->bsXA[x].bsPage == pXtra->bsXA[x + 1].bsPage) &&
		((LBNT)-1 == pXtra->bsXA[x].vdBlk) &&
		((LBNT)-1 == pXtra->bsXA[x + 1].vdBlk)) {
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
	    } else if ((LBNT)-1 == pXtra->bsXA[x].vdBlk) {
	        if ((LBNT)-1 == pXtra->bsXA[x + 1].vdBlk) {
		    /* 
		     * Hole at end - valid end case 
		     */
		    endExtent = x + 1;
		    found = TRUE;
		}
	    } else if ((((pXtra->bsXA[x+1].bsPage - pXtra->bsXA[x].bsPage) * BLOCKS_PER_PAGE) + pXtra->bsXA[x].vdBlk) >
		       volume->volSize) {
	        /*
		 * location on disk falls off end of volume.
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
	    pXtra->bsXA[pXtra->xCnt - 1].vdBlk = (LBNT)-1;
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_162, 
					       "Modified mcell (%d,%d,%d)'s vdBlk to %d.\n"),
				       volNum, pageNum, mcellNum, -1);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	} else {
	    /* 
	     * Found an end point 
	     */
	    if ((LBNT)-1 != pXtra->bsXA[endExtent].vdBlk) {
	        if (endExtent + 1  < pXtra->xCnt) {
		    /*
		     * Double corruption.
		     */
		    pXtra->xCnt = endExtent + 1;
		}
		pXtra->bsXA[pXtra->xCnt - 1].vdBlk = (LBNT)-1;
	    } else {
		pXtra->xCnt = endExtent + 1;
	    }
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_156, 
					       "Modified mcell (%d,%d,%d)'s xCnt to %d.\n"),
				       volNum, pageNum, mcellNum, 
				       pXtra->xCnt);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	}
    } /* end if bad last Xtnt */

    /*
     * Need to check the consistance of the extents PRIOR to checking
     * the chains as we will have already added them to the SBM and
     * it needs to at least know the page numbers are correct.
     */
    
    /*
     * check first two pages, other checks require this to be already done.
     *
     * Error case #1 (First 2 extents in mcell)
     *   page0 > page1 then set page0 to page1 and LBN (of 0) to -1
     */
    if (pXtra->bsXA[0].bsPage > pXtra->bsXA[1].bsPage) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_158, 
					   "Modified mcell (%d,%d,%d) extent %d's page from %d to %d.\n"),
				   volNum, pageNum, mcellNum,
				   0,
				   pXtra->bsXA[0].bsPage, 
				   pXtra->bsXA[1].bsPage);
	if (SUCCESS != status) {
	    return status;
	}

	pXtra->bsXA[0].bsPage = pXtra->bsXA[1].bsPage;
	pXtra->bsXA[0].vdBlk = (LBNT)-1;
	modified = TRUE;
    }

    /*
     * Now that we can loop through the rest of the extents in the mcell.
     * If this mcell has only 2 extents we don't enter the loop
     *
     * Error case #2 
     *   if pageC < pageA then set PageC to PageB
     *
     * Error case #3
     *   if PageC < PageB then set PageB to PageC
     */
    for (extent = 2; extent < pXtra->xCnt; extent++) {
	int pageA, pageB, pageC;

	pageA = pXtra->bsXA[extent -2].bsPage;
	pageB = pXtra->bsXA[extent -1].bsPage;
	pageC = pXtra->bsXA[extent].bsPage;

	if (pageC < pageA) {
	    /*
	     * PageC set to value of PageB.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_159, 
					       "Modified mcell (%d,%d,%d)'s extent page range out of order.\n"),
				       volNum, pageNum, mcellNum);
	    if (SUCCESS != status) {
		return status;
	    }

	    pXtra->bsXA[extent].bsPage = pXtra->bsXA[extent - 1].bsPage;
	    pXtra->bsXA[extent].vdBlk = (LBNT)-1;
	    pXtra->bsXA[extent - 1].vdBlk = (LBNT)-1;
	    modified = TRUE;
	} else if (pageC < pageB) {
	    /*
	     * PageB set to value of PageC.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_159,
					       "Modified mcell (%d,%d,%d)'s extent page range out of order.\n"),
				       volNum, pageNum, mcellNum);
	    if (SUCCESS != status) {
		return status;
	    }

	    pXtra->bsXA[extent - 1].bsPage = pXtra->bsXA[extent].bsPage;
	    pXtra->bsXA[extent].vdBlk = (LBNT)-1;
	    pXtra->bsXA[extent - 1].vdBlk = (LBNT)-1;
	    modified = TRUE;
	}
    } /* end of extents in mcell */

    /*
     * Check that vdBlk is either -1, -2 or evenly divisable
     * by BLOCKS_PER_PAGE(16).
     */
    for (extent = 0; extent < pXtra->xCnt - 1; extent++) {
	if (((LBNT)-1 != pXtra->bsXA[extent].vdBlk) &&
	    ((LBNT)-2 != pXtra->bsXA[extent].vdBlk) &&
	    (0 != pXtra->bsXA[extent].vdBlk % BLOCKS_PER_PAGE)) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_460,
					       "Modified mcell (%d,%d,%d) xtnt entry %d's LBN from %u to %d.\n"),
				       volNum, pageNum, mcellNum, extent,
				       pXtra->bsXA[extent].vdBlk, -1);
	    if (SUCCESS != status) {
		return status;
	    }
	    pXtra->bsXA[extent].vdBlk = (LBNT)-1;
	    modified = TRUE;
	}
    } /* end extent loop */

    /*
     * Can not have consecutive extents with the same bsPage.
     * This is new for ODSv4, but we will fix it for both ODSs.
     *
     * The solution is to move all the following extents up.
     * 
     *   Original              Fixed
     *   xCnt = 4              xCnt=3
     *   page  lbn             page  lbn
     *     10    X               10    X    
     *     15    P               15   -1
     *     15    Q               20    Y
     *     20    Y
     */
    for (extent = 0; extent < pXtra->xCnt - 1; extent++) {
	if (pXtra->bsXA[extent].bsPage == pXtra->bsXA[extent+1].bsPage) {
	    int localExtent;
	    /*
	     * Shift all the records up one.  
	     */
	    for (localExtent = extent; 
		 localExtent < pXtra->xCnt - 1; 
		 localExtent++) {
		pXtra->bsXA[localExtent].bsPage = pXtra->bsXA[localExtent +1].bsPage;
		pXtra->bsXA[localExtent].vdBlk  = pXtra->bsXA[localExtent +1].vdBlk;
	    }
	    
	    /*
	     * Set current extent LBN to -1, as we can't tell which
	     * LBN is correct.
	     */
	    pXtra->bsXA[extent].vdBlk = (LBNT)-1;
	    
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_932,
					       "Modified mcell (%d,%d,%d) removed duplicate xtnt entry %d.\n"),
				       volNum, pageNum, mcellNum, extent);

	    if (SUCCESS != status) {
		return status;
	    }
	    
	    /*
	     * Correct the xCnt.
	     */
	    pXtra->xCnt--;

	    /*
	     * Reset extent and continue original loop.
	     */
	    extent--;

	    modified = TRUE;
	} /* end if consecutive extents */
    } /* end extent loop */

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148,
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_XTRA_XTNTS");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bsr_xtra_xtnts_record */


/*
 * Function Name:correct_bsr_shadow_xtnts_record (3.46)
 *
 * Description: This routine checks and fixes mcell records of type
 *               BSR_SHADOW_XTNTS.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     volNum: The volume number we are working on.
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bsr_shadow_xtnts_record(domainT *domain, int volNum, pageT *pPage,
				    int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_shadow_xtnts_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    dmnVersion;
    int    extent;
    bsShadowXtntT *pShadow;
    volumeT *volume;
    
    /*
     * Init variables.
     */
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;
    pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));
    dmnVersion = domain->dmnVersion;
    volume = &(domain->volumes[volNum]);

    /*
     * Check each field in BSR_SHADOW_XTNTS looking for corruptions:
     */
    if (BLOCKS_PER_PAGE != pShadow->blksPerPage) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_160, 
					   "Modified BMT page %d mcell %d's shadow blksPerPage from %d to %d.\n"),
				   pageNum, mcellNum, pShadow->blksPerPage,
				   BLOCKS_PER_PAGE);
	if (SUCCESS != status) {
	    return status;
	}

        pShadow->blksPerPage = BLOCKS_PER_PAGE;
	modified = TRUE;
    }
    
    /* 
     * mcellCnt will be checked when we follow chains.
     */

    /*
     * Check vdIndex. For ODS V3 '-1' means non striped.
     */
    if (3 == dmnVersion) {
        if ((ushort)-1 != pShadow->allocVdIndex) {
	    /*
	     * This should be a striped file.
	     */
	    if ((pShadow->allocVdIndex > domain->highVolNum) ||
		(-1 == domain->volumes[pShadow->allocVdIndex].volFD)) {
	        /*
		 * Not a valid volume.  Set to -1, when we follow
		 * chains we will need to find the correct volume 
		 * if it is striped.
		 */
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_161, 
						   "Modified mcell (%d,%d,%d)'s allocVdIndex from %d to %d.\n"),
					   volNum, pageNum, mcellNum, 
					   pShadow->allocVdIndex, -1);
		if (SUCCESS != status) {
		    return status;
		}

	        pShadow->allocVdIndex = (ushort)-1;
		modified = TRUE;
	    }
        } 
    } else {
        /*
	 * Only striped files use shadow extents on V4 ODS domains.
	 */
	if (-1 == domain->volumes[pShadow->allocVdIndex].volFD) {
	    /*
	     * Found corruption which we can not fix at this point.
	     * we will fix it when we follow chains.
	     */
	}
    }

    /*
     * Need to make sure the xCnt is with in valid range.
     */
    if ((pShadow->xCnt < 1) || (pShadow->xCnt > BMT_SHADOW_XTNTS)) {
	/*
	 * As we will be be fixing the xCnt in the following block of
	 * code, all we need to do is set xCnt to BMT_SHADOW_XTNTS and
	 * set the last vdBlk to 0.
	 */
	pShadow->xCnt = BMT_SHADOW_XTNTS;

	/* 
	 * The only valid values for the last vdblk are -1 and 0, but
	 * garbage has been known to show up here when the entry in the
	 * array is not in use.
	 *
	 * If vdBlk should not be 0 but instead -1, it will be set
	 * correctly in the next block of code.  
	 */
	pShadow->bsXA[pShadow->xCnt - 1].vdBlk = 0;
	modified = TRUE;

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_156, 
					   "Modified mcell (%1$d,%2$d,%3$d)'s xCnt to %4$d.\n"),
				   volNum, pageNum, mcellNum, 
				   pShadow->xCnt);
	if (SUCCESS != status) {
	    return status;
	}
    }

    if ((LBNT)-1 != pShadow->bsXA[pShadow->xCnt - 1].vdBlk) {
        int x;
	int endExtent = 0;
	int found = FALSE;
	
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
	 * loop through extents 0 to (BMT_SHADOW_XTNTS - 2).
	 */
	for (x = 0; (x <= (BMT_SHADOW_XTNTS - 2) && FALSE == found); x++) {
	   /*
	    * Order of 'if's  is important
	    */
	    if ((3 == domain->dmnVersion) &&
		(pShadow->bsXA[x].bsPage == pShadow->bsXA[x + 1].bsPage) &&
		((LBNT)-1 == pShadow->bsXA[x].vdBlk) &&
		((LBNT)-1 == pShadow->bsXA[x + 1].vdBlk)) {
	        /*
		 * NOTE In V3 domains it is possible to have two extents
		 * with the same page, If vdBlk is -1. - not an end case.
		 * believe this only happens in the frag file.
		 */
	    } else if (pShadow->bsXA[x].bsPage >= pShadow->bsXA[x + 1].bsPage) {
	        /* 
		 * page number does not increment. - valid end case 
		 */
	        endExtent = x;
	        found = TRUE;
	    } else if ((LBNT)-1 == pShadow->bsXA[x].vdBlk) {
	        if ((LBNT)-1 == pShadow->bsXA[x + 1].vdBlk) {
		    /* 
		     * Hole at end - valid end case 
		     */
		    endExtent = x + 1;
		    found = TRUE;
		}
	    } else if ((((pShadow->bsXA[x+1].bsPage - pShadow->bsXA[x].bsPage) * BLOCKS_PER_PAGE) + pShadow->bsXA[x].vdBlk) >
		       volume->volSize) {
	        /*
		 * location on disk falls off end of volume
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
	    pShadow->bsXA[pShadow->xCnt - 1].vdBlk = (LBNT)-1;
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_162, 
					       "Modified mcell (%d,%d,%d)'s vdBlk to %d.\n"),
				       volNum, pageNum, mcellNum, -1);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	} else {
	    /* 
	     * Found an end point 
	     */
	    if (-1 != pShadow->bsXA[endExtent].vdBlk) {
	        if (endExtent + 1  < pShadow->xCnt) {
		    /*
		     * Double corruption.
		     */
		    pShadow->xCnt = endExtent + 1;
		}
		pShadow->bsXA[pShadow->xCnt - 1].vdBlk = (LBNT)-1;
	    } else {
		pShadow->xCnt = endExtent + 1;
	    }
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_156,
					       "Modified mcell (%d,%d,%d)'s xCnt to %d.\n"),
				       volNum, pageNum, mcellNum,
				       pShadow->xCnt);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	}
    } /* end if bad last Xtnt */


    /*
     * Need to check the consistance of the extents PRIOR to checking
     * the chains as we will have already added them to the SBM and
     * it needs to at least know the page numbers are correct.
     */
    
    /*
     * check first two pages, other checks require this to be already done.
     *
     * Error case #1 (First 2 extents in mcell)
     *   page0 > page1 then set page0 to page1 and LBN (of 0) to -1
     */
    if (pShadow->bsXA[0].bsPage > pShadow->bsXA[1].bsPage) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_158, 
					   "Modified mcell (%d,%d,%d) extent %d's page from %d to %d.\n"),
				   volNum, pageNum, mcellNum, 
				   0,
				   pShadow->bsXA[0].bsPage,
				   pShadow->bsXA[1].bsPage);
	if (SUCCESS != status) {
	    return status;
	}

	pShadow->bsXA[0].bsPage = pShadow->bsXA[1].bsPage;
	pShadow->bsXA[0].vdBlk = (LBNT) -1;
	modified = TRUE;
    }

    /*
     * Now that we can loop through the rest of the extents in the mcell.
     * If this mcell has only 2 extents we don't enter the loop
     *
     * Error case #2 
     *   if pageC < pageA then set PageC to PageB
     *
     * Error case #3
     *   if PageC < PageB then set PageB to PageC
     */
    for (extent = 2; extent < pShadow->xCnt; extent++) {
	int pageA, pageB, pageC;

	pageA = pShadow->bsXA[extent -2].bsPage;
	pageB = pShadow->bsXA[extent -1].bsPage;
	pageC = pShadow->bsXA[extent].bsPage;

	if (pageC < pageA) {
	    /*
	     * PageC set to value of PageB.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_159, 
					       "Modified mcell (%d,%d,%d)'s extent page range out of order.\n"),
				       volNum, pageNum, mcellNum);
	    if (SUCCESS != status) {
		return status;
	    }

	    pShadow->bsXA[extent].bsPage = pShadow->bsXA[extent - 1].bsPage;
	    pShadow->bsXA[extent].vdBlk = (LBNT)-1;
	    pShadow->bsXA[extent - 1].vdBlk = (LBNT)-1;
	    modified = TRUE;
	} else if (pageC < pageB) {
	    /*
	     * PageB set to value of PageC.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_159, 
					       "Modified mcell (%d,%d,%d)'s extent page range out of order.\n"),
				       volNum, pageNum, mcellNum);
	    if (SUCCESS != status) {
		return status;
	    }

	    pShadow->bsXA[extent - 1].bsPage = pShadow->bsXA[extent].bsPage;
	    pShadow->bsXA[extent].vdBlk = (LBNT)-1;
	    pShadow->bsXA[extent - 1].vdBlk = (LBNT)-1;
	    modified = TRUE;
	}
    } /* end of extents in mcell */

    /*
     * Check that vdBlk is either -1, -2 or evenly divisable
     * by BLOCKS_PER_PAGE(16).
     */
    for (extent = 0; extent < pShadow->xCnt - 1; extent++) {
	if (((LBNT)-1 != pShadow->bsXA[extent].vdBlk) &&
	    ((LBNT)-2 != pShadow->bsXA[extent].vdBlk) &&
	    (0 != pShadow->bsXA[extent].vdBlk % BLOCKS_PER_PAGE)) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_460,
					       "Modified mcell (%d,%d,%d) xtnt entry %d's LBN from %u to %d.\n"),
				       volNum, pageNum, mcellNum, extent,
				       pShadow->bsXA[extent].vdBlk, -1);
	    if (SUCCESS != status) {
		return status;
	    }
	    pShadow->bsXA[extent].vdBlk = (LBNT)-1;
	    modified = TRUE;
	}
    } /* end extent loop */


    /*
     * Can not have consecutive extents with the same bsPage.
     * This is new for ODSv4, but we will fix it for both ODSs.
     *
     * The solution is to move all follwing extents up.
     * 
     *   Original              Fixed
     *   xCnt = 4              xCnt=3
     *   page  lbn             page  lbn
     *     10    X               10    X    
     *     15    P               15   -1
     *     15    Q               20    Y
     *     20    Y
     */
    for (extent = 0; extent < pShadow->xCnt - 1; extent++) {
	if (pShadow->bsXA[extent].bsPage == pShadow->bsXA[extent+1].bsPage) {
	    int localExtent;
	    /*
	     * Shift all the records up one.  
	     */
	    for (localExtent = extent; 
		 localExtent < pShadow->xCnt - 1; 
		 localExtent++) {
		pShadow->bsXA[localExtent].bsPage = pShadow->bsXA[localExtent +1].bsPage;
		pShadow->bsXA[localExtent].vdBlk  = pShadow->bsXA[localExtent +1].vdBlk;
	    }
	    
	    /*
	     * Set current extent LBN to -1, as we can't tell which
	     * LBN is correct.
	     */
	    pShadow->bsXA[extent].vdBlk = (LBNT) -1;
	    
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_932,
					       "Modified mcell (%d,%d,%d) removed duplicate xtnt entry %d.\n"),
				       volNum, pageNum, mcellNum, extent);

	    if (SUCCESS != status) {
		return status;
	    }
	    
	    /*
	     * Correct the xCnt.
	     */
	    pShadow->xCnt--;

	    /*
	     * Reset extent and continue original loop.
	     */
	    extent--;

	    modified = TRUE;
	} /* end if consecutive extents */
    } /* end extent loop */

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148,
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_SHADOW_XTNTS");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bsr_shadow_xtnts_record */


/*
 * Function Name:correct_bsr_mcell_free_list_record (3.47)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BSR_MCELL_FREE_LIST.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bsr_mcell_free_list_record(domainT *domain, pageT *pPage, 
				       int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_mcell_free_list_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    dmnVersion;
    int    currPage;
    int    status;
    int    pageNum;
    int    volNum;
    int    maxBmtPage;
    signed int firstPage;

    bsMcellFreeListT *pMcellFreeList;
    volumeT *volume;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;
    pMcellFreeList = (bsMcellFreeListT *)((char *)pRecord + sizeof(bsMRT));
    currPage = pBMTPage->pageId;
    dmnVersion = domain->dmnVersion;
    volume = &(domain->volumes[volNum]);
    maxBmtPage = volume->volRbmt->bmtLBN[volume->volRbmt->bmtLBNSize].page;

    /*
     * Check each field in BSR_MCELL_FREE_LIST looking for corruptions:
     */
    if  ((0 != mcellNum) ||
	 ((3 == dmnVersion) && (1 != currPage)) ||
	 ((dmnVersion > 3) && (0 != currPage))) {
        /*
	 * This record should only appear in mcell 0 on the first BMT 
	 * page.  if it appears elsewhere it is invalid type.
	 * Delete the record.
	 */
        status = delete_mcell_record(domain, pPage, mcellNum, pRecord);
	if (SUCCESS != status) {
	    return status;
	}
	/* 
	 * We currently just truncate the record. In the future we may 
	 * need to delete the entire mcell, possibly the entire chain
	 * depending on the situation.  If needed delete mcell from mcell 
	 * list.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_163, 
					   "Modified BMT page %d mcell %d - Deleted invalid record %d.\n"),
				   pageNum, mcellNum, pRecord->type);
	if (SUCCESS != status) {
	    return status;
	}

	modified = TRUE;
    }

    if (3 == dmnVersion) {
	firstPage = 0;
    } else {
	firstPage = -1;
    }

    /*
     * pMcellFreeList.headPg will be recalculated in free_mcell_list.
     */
    if (((signed int)(pMcellFreeList->headPg) < firstPage) ||
	((signed int)(pMcellFreeList->headPg) > maxBmtPage)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_164, 
					   "Modified mcell (%d,%d,%d)'s headPg from %d to %d.\n"),
				   volNum, pageNum, mcellNum,
				   pMcellFreeList->headPg,
				   firstPage);
	if (SUCCESS != status) {
	    return status;
	}
	pMcellFreeList->headPg = firstPage;
	modified = TRUE;
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_MCELL_FREE_LIST");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_mcell_free_list_record */


/*
 * Function Name:correct_bsr_bfs_attr_record (3.48)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BSR_BFS_ATTR.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED. 
 */
int correct_bsr_bfs_attr_record(domainT *domain, pageT *pPage, 
				int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_bfs_attr_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    bsBfSetAttrT *pAttr;

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->pageId;
    pAttr    = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check each field in BSR_BFS_ATTR looking for corruptions:
     */
    if ((pAttr->bfSetId.domainId.tv_sec != domain->dmnId.tv_sec) ||
	(pAttr->bfSetId.domainId.tv_usec != domain->dmnId.tv_usec)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_165,
					   "Modified mcell (%d,%d,%d)'s domain ID from %x.%x to %x.%x.\n"),
				   volNum, pageNum, mcellNum,
				   pAttr->bfSetId.domainId.tv_sec, 
				   pAttr->bfSetId.domainId.tv_usec,
				   domain->dmnId.tv_sec, 
				   domain->dmnId.tv_usec);
	if (SUCCESS != status) {
	    return status;
	}

        pAttr->bfSetId.domainId.tv_sec = domain->dmnId.tv_sec;
	pAttr->bfSetId.domainId.tv_usec = domain->dmnId.tv_usec;
	modified = TRUE;
    }

    if ((pAttr->bfSetId.dirTag.num !=  pMcell->tag.num) ||
	(pAttr->bfSetId.dirTag.seq !=  pMcell->tag.seq)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_166, 
					   "Modified mcell (%d,%d,%d)'s fileset ID from %d.%x to %d.%x.\n"),
				   volNum, pageNum, mcellNum,
				   pAttr->bfSetId.dirTag.num, 
				   pAttr->bfSetId.dirTag.seq, 
				   pMcell->tag.num, pMcell->tag.seq);
	if (SUCCESS != status) {
	    return status;
	}

        pAttr->bfSetId.dirTag.num =  pMcell->tag.num;
	pAttr->bfSetId.dirTag.seq =  pMcell->tag.seq;
	modified = TRUE;
    }

    /*
     * When setting to a hardcoded seqNumber it is in easier to use HEX.
     */
    if ((1 != pAttr->fragBfTag.num) || (0x8001 != pAttr->fragBfTag.seq)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_167, 
					   "Modified mcell (%d,%d,%d)'s fragBfTag from %d.%x to %d.%x.\n"),
				   volNum, pageNum, mcellNum,
				   pAttr->fragBfTag.num, 
				   pAttr->fragBfTag.seq,
				   1, 0x8001);
	if (SUCCESS != status) {
	    return status;
	}

        pAttr->fragBfTag.num = 1;
	pAttr->fragBfTag.seq = 0x8001;
	modified = TRUE;
    }

    status = validate_fileset_name(pAttr->setName);
    if (SUCCESS != status) {
        char   setName[32];
	/*
	 * Create a new name for the fileset.
	 */
	sprintf(setName, "fixfdmn_fileset_%d", pAttr->bfSetId.dirTag.num);
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_168, 
					   "Modified invalid fileset name to '%s'.\n"),
				   setName);
	if (SUCCESS != status) {
	    return status;
	}

	strncpy(pAttr->setName, setName, 32);
	modified = TRUE;
    }

    /*
     * The next struct is an array which always points
     * to a known group of tag.seq of special reserver files.
     */
    if ((2 != pAttr->fsContext[0]) || (0x8001 != pAttr->fsContext[1]) ||
	(3 != pAttr->fsContext[2]) || (0x8001 != pAttr->fsContext[3]) ||
	(4 != pAttr->fsContext[4]) || (0x8001 != pAttr->fsContext[5]) ||
	(5 != pAttr->fsContext[6]) || (0x8001 != pAttr->fsContext[7])) {
        pAttr->fsContext[0] = 2;
	pAttr->fsContext[1] = 0x8001;
	pAttr->fsContext[2] = 3;
	pAttr->fsContext[3] = 0x8001;
	pAttr->fsContext[4] = 4;
	pAttr->fsContext[5] = 0x8001;
	pAttr->fsContext[6] = 5;
	pAttr->fsContext[7] = 0x8001;
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_169,
					   "Modified mcell (%d,%d,%d)'s fsContext to defaults.\n"),
				   volNum, pageNum, mcellNum);
	if (SUCCESS != status) {
	    return status;
	}

	modified = TRUE;
    }

    /*
     * Frag info will be corrected in correct_frag_file
     */

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_BFS_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_bfs_attr_record */


/*
 * Function Name: correct_bsr_def_del_mcell_list_record (3.49)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BSR_DEF_DEL_MCELL_LIST.  The tool will be resetting
 *              the deferred delete list in clear_def_del_list, so the
 *              only thing we really need to check is the location of
 *              the record.  if it is located anywhere else, first see
 *              if the type is wrong otherwise delete it.
 *
 * Input parameters:
 *     domain: The domain we are eorking on.
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bsr_def_del_mcell_list_record(domainT *domain, pageT *pPage, 
					  int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_def_del_mcell_list_record";
    bsMPgT *pBMTPage; /* The current RBMT page we are working on */
    bsMCT  *pMcell;   /* Pointer to current mcell. */
    int    modified;  /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    int    dmnVersion;
    delLinkRT *pDelLink;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;
    pDelLink = (delLinkRT *)((char *)pRecord + sizeof(bsMRT));
    dmnVersion = domain->dmnVersion;

    /*
     * Check each field in BSR_DEF_DEL_MCELL_LIST looking for corruptions:
     */
    if ((0 != mcellNum) ||
	((3 == dmnVersion) && (1 != pageNum)) ||
	((dmnVersion > 3) && (0 != pageNum))) {
        /*
	 * This record should only appear on the mcell 0 on the first BMT 
	 * page.  if it appears elsewhere it is invalid type.
	 * Delete the record.
	 */
        status = delete_mcell_record(domain, pPage, mcellNum, pRecord);
	if (SUCCESS != status) {
	    return status;
	}
	/* 
	 * We currently just truncate the record. In the future we may 
	 * need to delete the entire mcell, possibly the entire chain
	 * depending on the situation.  If needed delete mcell from mcell 
	 * list.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_163, 
					   "Modified BMT page %d mcell %d - Deleted invalid record %d.\n"),
				   pageNum, mcellNum, pRecord->type);
	if (SUCCESS != status) {
	    return status;
	}

	modified = TRUE;
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_DEF_DEL_MCELL_LIST");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_def_del_mcell_list_record */


/*
 * Function Name:correct_bsr_bf_inherit_attr_record (3.50)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BSR_BF_INHERIT_ATTR.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *     RBMTflag: Set to TRUE if checking RBMT, FALSE if BMT.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bsr_bf_inherit_attr_record(pageT *pPage, int mcellNum, 
                                       bsMRT *pRecord, int RBMTflag)
{
    char   *funcName = "correct_bsr_bf_inherit_attr_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    bsBfInheritAttrT *pInherit;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->pageId;
    pInherit = (bsBfInheritAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check each field in BSR_BF_INHERIT_ATTR looking for corruptions:
     */
    if ((1 != pInherit->reqServices) ||
	(0 != pInherit->dataSafety) ||
	(0 != pInherit->extendSize)) {
        pInherit->reqServices = 1;
	pInherit->dataSafety  = 0;
	pInherit->extendSize  = 0;
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_170, 
					   "Cleared mcell (%d,%d,%d)'s inheritable attributes.\n"),
				   volNum, pageNum, mcellNum);
	if (SUCCESS != status) {
	    return status;
	}

	modified = TRUE;
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_BF_INHERIT_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_bf_inherit_attr_record */


/*
 * Function Name: correct_bsr_rsvd17_record (3.51)
 *
 * Description: This routine checks and fixes mcell records of type 
 *              BSR_RSVD17.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.
 */
int correct_bsr_rsvd17_record(pageT *pPage, int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_rsvd17_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    bsrRsvd17T *pRsvd17;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;
    pRsvd17 = (bsrRsvd17T *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Can safely ignore this mcell, although some values are set.
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148,
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_RSVD17");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_rsvd17_record */


/*
 * Function Name: correct_bsr_bfs_quota_attr_record (3.52)
 *
 * Description: This routine checks and fixes mcell records of type 
 *              BSR_BFS_QUOTA_ATTR.  The quota info will be saved in
 * 		a link list which will be attached to the domain
 * 		structure.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 * 
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, NO_MEMORY.
 */
int correct_bsr_bfs_quota_attr_record(domainT *domain, pageT *pPage, 
				      int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_bfs_quota_attr_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    long   hardBlockLimit;   /* hard block limit */
    long   softBlockLimit;   /* soft block limit */
    long   hardFileLimit;    /* hard file limit */
    long   softFileLimit;    /* soft file limit */
    time_t blockTimeLimit;   /* time limit for excessive disk blk use */
    time_t fileTimeLimit;    /* time limit for excessive file use */
    bsQuotaAttrT *pQuotaAttr;
    fsQuotaT *pFsQuota;      /* Used to store the quotas */

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;
    pQuotaAttr = (bsQuotaAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Quota values are stored as 2 ints, but in reality they are longs.
     */
    hardBlockLimit =
      ((long)pQuotaAttr->blkHLimitHi << 32) | pQuotaAttr->blkHLimitLo;

    softBlockLimit = 
      ((long)pQuotaAttr->blkSLimitHi << 32) | pQuotaAttr->blkSLimitLo;

    hardFileLimit =
      ((long)pQuotaAttr->fileHLimitHi << 32) | pQuotaAttr->fileHLimitLo;

    softFileLimit = 
      ((long)pQuotaAttr->fileSLimitHi << 32) |pQuotaAttr->fileSLimitLo;

    blockTimeLimit = pQuotaAttr->blkTLimit;
    fileTimeLimit = pQuotaAttr->fileTLimit;

    /*
     * There are no values in this record which will affect the boot/use 
     * of the system. Save the info for later, then we can print a message
     * letting the user know what the current quota values are.  After the
     * domain is fixed, they then can change them  using the normal quota
     * tools.  We do not have the fileset records at this point so we will
     * instead attach it to the domain record.
     */
    pFsQuota = (fsQuotaT *)ffd_malloc(sizeof(fsQuotaT));
    if (NULL == pFsQuota) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_171,
			 "fileset quotas"));
	return NO_MEMORY;
    }

    pFsQuota->filesetTag.num = pMcell->tag.num;
    pFsQuota->filesetTag.seq = pMcell->tag.seq;
    pFsQuota->hardBlockLimit = hardBlockLimit;
    pFsQuota->softBlockLimit = softBlockLimit;
    pFsQuota->hardFileLimit = hardFileLimit;
    pFsQuota->softFileLimit = softFileLimit;
    pFsQuota->blockTimeLimit = pQuotaAttr->blkTLimit;
    pFsQuota->fileTimeLimit = pQuotaAttr->fileTLimit;
    pFsQuota->quotaStatus = pQuotaAttr->quotaStatus;
    pFsQuota->next = domain->filesetQuota;
    domain->filesetQuota = pFsQuota;

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_BFS_QUOTA_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_bfs_quota_attr_record */


/*
 * Function Name: correct_bsr_proplist_head_record (3.53)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BSR_PROPLIST_HEAD.  A detailed check will need to be
 *              done during a later stage, because we will need to
 *              follow the chain from each proplist head and make sure
 *              the whole proplist is there.
 *
 *              When this ported to V4.0 we will need to make some
 *              changes to the types.  They have a different name under
 *              the old OS.
 *
 *              The spec says we can 'delete' proplist if we need to.
 *
 * Input parameters:
 *     domain: The domain we are using.
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bsr_proplist_head_record(domainT *domain, pageT *pPage, 
				     int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_proplist_head_record";
    bsMPgT *pBMTPage;	  /* The current RBMT page we are working on */
    bsMCT  *pMcell;	  /* Pointer to current mcell. */
    uint64T alignedFlags; /* Gets rid of unaligned access errors. */
    int    modified;	  /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    int    dmnVersion;
    bsPropListHeadT    *pPropList;
    bsPropListHeadT_v3 *pPropListV3;

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->pageId;
    dmnVersion = domain->dmnVersion;

    if (3 != dmnVersion) {
        pPropList = (bsPropListHeadT *)((char *)pRecord + sizeof(bsMRT));

	FLAGS_READ(alignedFlags, pPropList->flags);

	if (alignedFlags & BSR_PL_DELETED) {
	    /*
	     * Ignore this record, it's been deleted.
	     */
	} else if (alignedFlags & BSR_PL_PAGE) {
	    /*
	     * This is only created on on OLD_ODS domains, but the kernel
	     * can still handle them.  I'm putting this code here in case
	     * an update domain structure tool is written and these get
	     * carried over to a ODSV4 domain.
	     * 
	     * header in mcell 0 and entire page is used for proplist.
	     */
	    if (0 != mcellNum) {
	        /*
		 * Must start in the first mcell.
		 */
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_177, 
						   "Deleting mcell (%d,%d,%d) proplist record, must be in cell 0.\n"),
					   volNum, pageNum, mcellNum);
		if (SUCCESS != status) {
		    return status;
		}

		status = delete_mcell_record(domain, pPage, 
					     mcellNum, pRecord);
		if (SUCCESS != status) {
		    return status;
		}
		modified = TRUE;
	    } else {
		if (0 != pBMTPage->freeMcellCnt) {
		    /*
		     * Uses the whole page, make sure other mcells 
		     * can't be used.
		     */
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_174, 
						       "Modified BMT page %d's freeMcellCnt to %d, proplist of type 'PAGE' had free mcells.\n"),
					       pageNum, 0);
		    if (SUCCESS != status) {
			return status;
		    }

		    pBMTPage->freeMcellCnt = 0;
		    modified = TRUE;
		}

		if (BSR_PROPLIST_PAGE_SIZE != pRecord->bCnt) {
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_172, 
						       "Modified mcell (%d,%d,%d)'s proplist size from %d to %d.\n"),
					       volNum, pageNum, mcellNum,
					       pRecord->bCnt, 
					       BSR_PROPLIST_PAGE_SIZE);
		    if (SUCCESS != status) {
			return status;
		    }

		    pRecord->bCnt = BSR_PROPLIST_PAGE_SIZE;
		    modified = TRUE;
		}  /* end if bCnt is invalid */
	    }
	} else {
	    /*
	     * Proplist fits in this mcell.
	     */
	    if (pRecord->bCnt >= sizeof(bsMCT)) {
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_172, 
						   "Modified mcell (%d,%d,%d)'s proplist size from %d to %d.\n"),
					   volNum, pageNum, mcellNum,
					   pRecord->bCnt, sizeof(bsMCT));
		if (SUCCESS != status) {
		    return status;
		}

		pRecord->bCnt = sizeof(bsMCT);
		modified = TRUE;
	    }  /* end if bCnt is invalid */
	}
    } else {
	/*
	 * Starting ODSV3 section.
	 */
        pPropListV3 = (bsPropListHeadT_v3 *)((char *)pRecord + sizeof(bsMRT));

	FLAGS_READ(alignedFlags, pPropListV3->flags);

	if (alignedFlags & BSR_PL_DELETED) {
	    /*
	     * Ignore this record, it's been deleted.
	     */
	} else if (alignedFlags & BSR_PL_PAGE) {
	    /*
	     * This is only used on OLD_ODS domains
	     * 
	     * header in mcell 0 and entire page is used for proplist.
	     */
	    if (0 != mcellNum) {
	        /*
		 * Must start in the first mcell.
		 */
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_177, 
						   "Deleting mcell (%d,%d,%d) proplist record, must be in cell 0.\n"),
					   volNum, pageNum, mcellNum);
		if (SUCCESS != status) {
		    return status;
		}

		status = delete_mcell_record(domain, pPage, 
					     mcellNum, pRecord);
		if (SUCCESS != status) {
		    return status;
		}
		modified = TRUE;
	    } else {
		if (0 != pBMTPage->freeMcellCnt) {
		    /*
		     * Uses the whole page, make sure other mcells 
		     * can't be used.
		     */
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_174, 
						       "Modified BMT page %d's freeMcellCnt to %d, proplist of type 'PAGE' had free mcells.\n"),
					       pageNum, 0);
		    if (SUCCESS != status) {
			return status;
		    }

		    pBMTPage->freeMcellCnt = 0;
		    modified = TRUE;
		}

		if (BSR_PROPLIST_PAGE_SIZE != pRecord->bCnt) {
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_176, 
						       "Modified mcell (%d,%d,%d) proplist's size from %d to %d.\n"),
					       volNum, pageNum, mcellNum,
					       pRecord->bCnt,
					       BSR_PROPLIST_PAGE_SIZE);
		    if (SUCCESS != status) {
			return status;
		    }

		    pRecord->bCnt = BSR_PROPLIST_PAGE_SIZE;
		    modified = TRUE;
		} /* end if bCnt is invalid */
	    }
	} else {
	    /*
	     * Proplist fits in this mcell.
	     */
	    if (pRecord->bCnt >= sizeof(bsMCT)) {
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_176, 
						   "Modified mcell (%d,%d,%d) proplist's size from %d to %d.\n"),
					   volNum, pageNum, mcellNum,
					   pRecord->bCnt, sizeof(bsMCT));
		if (SUCCESS != status) {
		    return status;
		}

		pRecord->bCnt = sizeof(bsMCT);
		modified = TRUE;
	    }  /* end if bCnt is invalid */
	}
    }
    
    
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_PROPLIST_HEAD");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_proplist_head_record */


/*
 * Function Name: correct_bsr_proplist_data_record (3.54)
 *
 * Description: This routine does absolutely nothing, as we can not
 *              verify the data or the record until we follow the
 *              chain in a later phase.  It is here for completeness.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS
 */
int correct_bsr_proplist_data_record(pageT *pPage, int mcellNum, 
				     bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_proplist_data_record";

    /*
     * for OLD_ODS domains there is no data fields we can check, we need 
     * to assume that this record is correct.  As the size of the mcell 
     * record is variable, we have to assume it is correct as well.
     *
     * for NEW ODS domains, Two new fields were added but they can only
     * be checked when we follow the chain.]
     */

    return SUCCESS;
} /* end correct_bsr_proplist_data_record */


/*
 * Function Name: correct_bmtr_fs_dir_index_file (3.55)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BMTR_FS_DIR_INDEX_FILE.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bmtr_fs_dir_index_file(pageT *pPage, int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_dir_index_file";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;

    /*
     * The field tag, could be 0 in very rare case.  This means the best we 
     * can do is check the high order bit.  if we know the number of extents 
     * in the tag file we can compute a high-water mark (page * 1022).
     * We can check this when we follow chains.
     */
    
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_DIR_INDEX_FILE");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_dir_index_file */


/*
 * Function Name: correct_bmtr_fs_index_file_record (3.56)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BMTR_FS_INDEX_FILE.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bmtr_fs_index_file_record(pageT *pPage, int mcellNum, 
				      bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_index_file_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->pageId;

    /*	
     * fname_page   = 1 or more, but no larger than max extent page.
     * ffree_page   = 1 or more, but no larger than max extent page.
     * fname_levels = Max of 4, see bs_index.h for more details.
     * ffree_levels = Max of 4, see bs_index.h for more details.
     */

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_INDEX_FILE");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_index_file_record */


/*
 * Function Name: correct_bmtr_fs_time_record (3.57)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BMTR_FS_TIME, if needed we can can verify this record
 *              even better during the phase which we follow the
 *              chain.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *     tag: The tag this record belong to.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bmtr_fs_time_record(domainT *domain, pageT *pPage, 
				int mcellNum, bsMRT *pRecord, int tag)
{
    char   *funcName = "correct_bmtr_fs_time_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->pageId;

    if (2 != pMcell->tag.num) {
        /*
	 * record type only appears for tag 2 of each fileset.
	 * As 2 records have this bCnt we might delete a dir index
	 * or a trashcan, but those are not critical files.
	 */
        status = delete_mcell_record(domain, pPage, mcellNum, pRecord);
	if (SUCCESS != status) {
	    return status;
	}
	/* 
	 * We currently just truncate the record. In the future we may 
	 * need to delete the entire mcell, possibly the entire chain
	 * depending on the situation.  If needed delete mcell from mcell 
	 * list.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_178, 
					   "Deleted BMT page %d mcell %d record, invalid location.\n"),
				   pageNum, mcellNum);
	if (SUCCESS != status) {
	    return status;
	}

	modified = TRUE;
    }

    /*
     * Field in record can not be incorrect.
     */

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148,
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_TIME");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_time_record */


/*
 * Function Name:correct_bmtr_fs_undel_dir_record (3.58)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BMTR_FS_UNDEL_DIR.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bmtr_fs_undel_dir_record(pageT *pPage, int mcellNum, 
				     bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_undel_dir_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->pageId;

    /*
     * dirTag  = Tag number of a Valid directory, could check the high bit
     *
     * At this point there is no tag list, a better place to do the 
     * detailed check would be during the chain check.
     */

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_UNDEL_DIR");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_undel_dir_record */


/*
 * Function Name:correct_bmtr_fs_data_record (3.59)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BMTR_FS_DATA.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.
*/
int correct_bmtr_fs_data_record(pageT *pPage, int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_data_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    recordType;
    int    status;
    int    pageNum;
    int    volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->pageId;

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148, 
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_DATA");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_data_record */


/*
 * Function Name: correct_bmtr_fs_stat_record (3.60)
 *
 * Description: This routine checks and fixes mcell records of type
 *              BMTR_FS_STAT. FUTURE Add a flag which would cause us
 *              to read in the passwd and group file and verify that
 *              the uid and gid are valid.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: the mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED.  
 */
int correct_bmtr_fs_stat_record(pageT *pPage, int mcellNum, bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_stat_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;  /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    int    pageNum;
    int    volNum;
    statT  *pStat;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->pageId;
    pStat    = (statT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Special case: we will not correct st_ino record in the fsstat
     * record until we get to correct_bmt_chains.  If we were to set it
     * here we could be wrong 50% of the time, once we are the chains
     * we have a much higher (maybe 100%?) chance of setting the correct
     * value.
     * 
     * We also check the frag data in chains
     */
    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_148,
			 "Found corruption in mcell (%d,%d,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_STAT");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_stat_record */


/*
 * Function Name: free_mcell (3.61)
 *
 * Description: This function removes (frees) a mcell from a BMT page.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     volNum: The volume number we are working on.
 *     pPage : The page containing the mcell to be freed.
 *     mcellNum : Which mcell to free.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE.
 */
int free_mcell(domainT *domain, int volNum, pageT *pPage, int mcellNum)
{
    char    *funcName = "free_mcell";
    bsMPgT  *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT   *pMcell;    /* The mcell we are working on. */
    bsMRT   *pRecord;   /* The record we are working on. */
    volumeT *pVolume;
    int     status;
    int     x; 
    LBNT    lbn;
    int	    extentSize;
    int	    tag;
    int	    setTag;
    int     pageNum;
    int     dmnVersion;
    LBNT     pageLbn;
    LBNT     rbmtLbn;
    int     rbmtVol;
    int     numExtents;
    int     rbmtFlag;
    pagetoLBNT    *pExtents;
    bsXtntRT      *pXtnt;
    bsXtraXtntRT  *pXtra;
    bsShadowXtntT *pShadow;
    mcellT	  mcellId;
    mcellNodeT    *mcell;
    mcellNodeT    tempMcell;
    nodeT         *node;

    /*
     * Init variables.
     */
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pageNum  = pBMTPage->pageId;
    pageLbn  = pPage->pageLBN;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pVolume  = &(domain->volumes[volNum]);
    tag      = pMcell->tag.num;
    setTag   = pMcell->bfSetTag.num;
    dmnVersion = domain->dmnVersion;

    /*
     * Save the mcell ID for use within the loop.
     */
    mcellId.vol  = volNum;
    mcellId.page = pageNum;
    mcellId.cell = mcellNum;

    /*
     * Delete mcell node from skip list.
     */
    tempMcell.mcellId.vol = volNum;
    tempMcell.mcellId.page = pageNum;
    tempMcell.mcellId.cell = mcellNum;
    mcell = &tempMcell;

    status = delete_node(domain->mcellList, (void *)&mcell);
    if (SUCCESS == status) {
	free(mcell);
    } else if (NOT_FOUND != status) {
	return status;
    }

    if ((0 == pMcell->tag.num) && (0 == pMcell->bfSetTag.num)) {
	/*
	 * This mcell is already free, don't need to do anything.
	 */
	return SUCCESS;
    }

    /*
     * Check the next chain if it points to a valid mcell.
     * If it does then clear the found pointer.
     */
    tempMcell.mcellId.vol  = pMcell->nextVdIndex;
    tempMcell.mcellId.page = pMcell->nextMCId.page;
    tempMcell.mcellId.cell = pMcell->nextMCId.cell;
    mcell = &tempMcell;

    status = find_node(domain->mcellList, (void **)&mcell, &node);
    if (SUCCESS == status) {
	MC_CLEAR_FOUND_PTR(mcell->status);
    } else if (NOT_FOUND != status) {
	return status;
    }

    /*
     * Now free the extents from the in-memory SBM (assuming it exists).
     */
    if (NULL != pVolume->volSbmInfo) {
        while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    switch (pRecord->type) {
	      case (BSR_XTNTS):
		  pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		  
		  /*
		   * For both ODS types check if the chain points to a 
		   * valid mcell.  If it does the clear the found
		   * pointer.
		   */
		  tempMcell.mcellId.vol  = pXtnt->chainVdIndex;
		  tempMcell.mcellId.page = pXtnt->chainMCId.page;
		  tempMcell.mcellId.cell = pXtnt->chainMCId.cell;
		  mcell = &tempMcell;

		  status = find_node(domain->mcellList, 
				     (void **)&mcell, &node);
		  if (SUCCESS == status) {
		      MC_CLEAR_FOUND_PTR(mcell->status);
		  } else if (NOT_FOUND != status) {
		      return status;
		  }

		  if (3 == dmnVersion) {
		      /*
		       * On V3 ODS systems the extents are not used. 
		       * Execept for BMT0/RBMT mcells.
		       */
		  } else {
		      /*
		       * Loop through all the extents in this record.
		       */
		      for (x = 0; 
			   x < (pXtnt->firstXtnt.xCnt - 1) && x < (BMT_XTNTS - 1);
			   x++) {
			  lbn = pXtnt->firstXtnt.bsXA[x].vdBlk;
			  extentSize = (pXtnt->firstXtnt.bsXA[x + 1].bsPage -
					pXtnt->firstXtnt.bsXA[x].bsPage);

			  status = remove_extent_from_sbm(domain, volNum, lbn, 
							  extentSize, tag,
							  setTag,
							  &mcellId, x);
			  if (SUCCESS != status) {
			      return status;
			  }
		      }
		  }

		break;

	      case (BSR_SHADOW_XTNTS): 
		  pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));

		  /*
		   * Loop on all extents
		   */
		  for (x = 0; 
		       x < (pShadow->xCnt - 1) && x < (BMT_SHADOW_XTNTS - 1);
		       x++) {
		      lbn = pShadow->bsXA[x].vdBlk;
		      extentSize = (pShadow->bsXA[x + 1].bsPage -
				    pShadow->bsXA[x].bsPage);

		      status = remove_extent_from_sbm(domain, volNum, lbn, 
						      extentSize, tag, setTag,
						      &mcellId, x);
		      if (SUCCESS != status) {
			  return status;
		      }
		  }
		  break;

	      case (BSR_XTRA_XTNTS):
		  pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		  /*
		   * Loop on all extents
		   */
		  for (x = 0; 
		       x < (pXtra->xCnt - 1) && x < (BMT_XTRA_XTNTS - 1);
		       x++) {
		      lbn = pXtra->bsXA[x].vdBlk;
		      extentSize = (pXtra->bsXA[x + 1].bsPage -
				    pXtra->bsXA[x].bsPage);

		      status = remove_extent_from_sbm(domain, volNum, lbn, 
						      extentSize, tag, setTag,
						      &mcellId, x);
		      if (SUCCESS != status) {
			  return status;
		      }
		  }
		break;

	      default:
		  /*
		   * Non extent based record.
		   */
		break;
	    }
	    
	    /*
	     * Set pRecord to next record  of pMcell.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end while record exist */
    } /* end if sbm array exists */

    /*
     * Mark the mcell as free.
     */
    pMcell->tag.num = 0;
    pMcell->bfSetTag.num = 0;
    pMcell->tag.seq = 0;
    pMcell->bfSetTag.seq = 0;
    pMcell->linkSegment = 0;

    /*
     * Clear the mcell record header.
     */
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pRecord->bCnt = 0;
    pRecord->type = 0;
    pRecord->version = 0;

    /*
     * Put this MCELL on the head of this page's free list.
     */
    pMcell->nextMCId.cell = pBMTPage->nextfreeMCId.cell;
    pMcell->nextMCId.page = pBMTPage->nextfreeMCId.page;
    pMcell->nextVdIndex   = 0;

    pBMTPage->nextfreeMCId.cell = mcellNum;
    pBMTPage->nextfreeMCId.page = pageNum;

    /*
     * For a special case below we need to know if this page is in the
     * RBMT or BMT for ODSv4.
     */
    rbmtFlag = FALSE;
    if (dmnVersion > 3) {
	/*
	 * Init variables
	 */
	pExtents   = pVolume->volRbmt->rbmtLBN;
	numExtents = pVolume->volRbmt->rbmtLBNSize;

	status = convert_page_to_lbn(pExtents, numExtents, pageNum, 
				     &rbmtLbn, &rbmtVol);
	if (SUCCESS == status) {
	    /*
	     * Could be an RBMT page.
	     */
	    if ((volNum == rbmtVol) && (pageLbn == rbmtLbn)) {
		/*
		 * Is an RBMT page.
		 */
		rbmtFlag = TRUE;
	    }
	} /* end if convert_page_to_lbn was successful */
    } /* end if dmnVersion > 3 */

    /*
     * Check if this PAGE needs to be added to the mcell free list?
     * This is done by checking if this page currently has not free
     * mcells.  As we are adding one it will need to be added to the
     * list.
     *
     * Exception to this are page 0 on ODSv3, or RBMT pages.
     */
    if ((TRUE == rbmtFlag) ||
	((3 == dmnVersion) && (0 == pageNum))) {
	/*
	 * Special case.  Can't be on the free list.
	 */
    } else if (0 == pBMTPage->freeMcellCnt) {
	int headPageNum; /* Page the head of the free list is on */
	LBNT headLbn;
	int headVol;
	int modified;
	pageT  *pHeadPage;
	bsMPgT *pHeadBMTPage;

	bsMcellFreeListT *headRecord; /* mcell free list record */

	/*
	 * Init variables
	 */
	pExtents   = pVolume->volRbmt->bmtLBN;
	numExtents = pVolume->volRbmt->bmtLBNSize;

	/*
	 * pBMTPage->nextFreePg is what we need to set.
	 */
	if (3 == dmnVersion) {
	    headPageNum = 1;
	} else {
	    headPageNum = 0;
	}

	status = convert_page_to_lbn(pExtents, numExtents, headPageNum, 
				     &headLbn, &headVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     headPageNum);
	    return FAILURE;
	}
	
	if (headLbn == pPage->pageLBN) {
	    /*
	     * This is the page we already have open.
	     */
	    pHeadPage = pPage;
	} else {
	    /*
	     * Load the page which contains the free list head record.
	     */
	    status = read_page_from_cache(headVol, headLbn, &pHeadPage);
	    if (SUCCESS != status) {
		return status;
	    }
	}

	pHeadBMTPage = (bsMPgT *)pHeadPage->pageBuf;
	pMcell  = &(pHeadBMTPage->bsMCA[0]);
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    if (BSR_MCELL_FREE_LIST == pRecord->type) {
		headRecord = (bsMcellFreeListT *)((char *)pRecord + 
						  sizeof(bsMRT));

		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_179,
						   "Added page to the free mcell list.\n"));
		if (SUCCESS != status) {
		    return status;
		}

		pBMTPage->nextFreePg = headRecord->headPg;
		headRecord->headPg = pageNum;
		break;
	    }
	    /*
	     * Set pRecord to next record  of pMcell.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end mcell record loop */

	if (headLbn == pPage->pageLBN) {
	    /*
	     * This is the page we already have open.
	     * Nothing more to do
	     */
	} else {
	    /*
	     * Release the page which contains the free list head record.
	     */
	    status= release_page_to_cache(headVol, headLbn, 
					  pHeadPage, TRUE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	} /* end check if pages are the same */
    } /* end if adding to free page list */

    /*
     * Increment the number of free mcells on this page.
     */
    pBMTPage->freeMcellCnt = pBMTPage->freeMcellCnt + 1;

    return SUCCESS;
} /* end free_mcell */


/*
 * Function Name: delete_mcell_record
 *
 * Description: This function takes a mcell and removes a record.
 *
 *   NOTE : If the mcell format has already been checked then use
 *          clear_mcell_record.  That functions does a much better
 *          job as it doesn't throw away record you might want to
 *          keep which fall after the desired record to delete.
 *
 * Input parameters:
 *     pPage : The page containing the mcell to be freed.
 *     mcellNum : Which mcell to free.
 *     pRecord:   Pointer to the start of the record to delete.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS
 */
int delete_mcell_record(domainT *domain, pageT *pPage, 
			int mcellNum, bsMRT *pRecord)
{
    char    *funcName = "delete_mcell_record";
    bsMPgT  *pBMTPage;  /* The current RBMT page we are working on */
    int     status;
    int     pageNum;
    int     volNum;
    int     bCnt;
    int     type;
    int     version;
    bsMCT   *pMcell;   /* The current BMT mcell we are working on */
    bsXtntRT   *pXtnt;
    mcellNodeT *mcell;
    mcellNodeT tempMcell;
    nodeT      *node;
    
    /*
     * Initialize variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;    
    pageNum  = pBMTPage->pageId;
    bCnt     = pRecord->bCnt;
    type     = pRecord->type;
    version  = pRecord->version;

    /*
     * Find the record we want.
     */
    switch (pRecord->type) {
	case (BMTR_FS_STAT):
	    tempMcell.mcellId.vol  = volNum;
	    tempMcell.mcellId.page = pageNum;
	    tempMcell.mcellId.cell = mcellNum;
	    mcell = &tempMcell;

	    status = find_node(domain->mcellList, (void **)&mcell, &node);
	    if (SUCCESS == status) {
		MC_CLEAR_FSSTAT(mcell->status);
	    } else if (NOT_FOUND != status) {
		return status;
	    }
	    break;

	case (BSR_XTNTS):
	    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
	    /*
	     * For both ODS types check if the chain points to a 
	     * valid mcell.  If it does the clear the found
	     * pointer.
	     */
	    tempMcell.mcellId.vol  = pXtnt->chainVdIndex;
	    tempMcell.mcellId.page = pXtnt->chainMCId.page;
	    tempMcell.mcellId.cell = pXtnt->chainMCId.cell;
	    mcell = &tempMcell;

	    status = find_node(domain->mcellList, (void **)&mcell, &node);
	    if (SUCCESS == status) {
		MC_CLEAR_FOUND_PTR(mcell->status);
	    } else if (NOT_FOUND != status) {
		return status;
	    }

	    break;
	default:
	    /*
	     * Ignore record type.
	     */
	    break;
    } /* end switch */

    /*
     * Truncating the record is easy, just set the header to zero.
     */
    pRecord->bCnt = 0;
    pRecord->type = 0;
    pRecord->version = 0;

    status = add_fixed_message(&(pPage->messages), BMT,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_180,
				       "Truncated mcell (%d,%d,%d) type %d, bCnt %d, version %d.\n"),
			       volNum, pageNum, mcellNum, type, 
			       bCnt, version);
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
} /* end delete_mcell_record */


/*
 * Function Name: clear_mcell_record
 *
 * Description: This function takes a mcell record, processes the contents
 *              as it removes the record.  This should be called AFTER the
 *              mcell has been checked to be in valid format.
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     pPage : The page containing the mcell to be freed.
 *     mcellNum : Which mcell to free.
 *     recNum: Which record to clear.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE
 */
int clear_mcell_record(domainT *domain, pageT *pPage, 
		       int mcellNum, int recNum)
{
    char    *funcName = "clear_mcell_record";
    bsMPgT  *pBMTPage; /* The current BMT page we are working on */
    bsMCT   *pMcell;   /* The current BMT mcell we are working on */
    bsMRT   *pRecord;  /* The current BMT record we are working on */
    bsMRT   *pNextRecord;  /* The next record in the mcell */
    mcellT  mcellId;
    int     x;
    int     status;
    int     memSize;
    int     pageNum;
    int     volNum;
    int     bCnt;
    int     type;
    int     currentRecNum;
    int     dmnVersion;
    int	    extentSize;
    int	    tag;
    int	    setTag;
    LBNT    lbn;
    bsXtntRT      *pXtnt;
    bsXtraXtntRT  *pXtra;
    bsShadowXtntT *pShadow;
    mcellNodeT    *mcell;
    mcellNodeT    tempMcell;
    nodeT         *node;

    /*
     * Load starting values from page passed in.
     */
    volNum     = pPage->vol;
    pBMTPage   = (bsMPgT *)pPage->pageBuf;    
    pMcell     = &(pBMTPage->bsMCA[mcellNum]);
    pRecord    = (bsMRT *)pMcell->bsMR0;
    pageNum    = pBMTPage->pageId;
    dmnVersion = domain->dmnVersion;
    tag        = pMcell->tag.num;
    setTag     = pMcell->bfSetTag.num;

    /*
     * Initialize variables.
     */
    mcellId.vol   = volNum;
    mcellId.page  = pageNum;
    mcellId.cell  = mcellNum;
    currentRecNum = 0;

    /*
     * Loop through record to find the record we want,
     * Process the record, the break out of loop.
     */
    while ((0 != pRecord->type) &&
	   (0 != pRecord->bCnt) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	currentRecNum++;
	if (currentRecNum < recNum) {
	    /*
	     * Set pRecord to next record  of pMcell.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	    continue; /* skip to next record in while loop */
	}

	/*
	 * Break out of while loop with pRecord pointing to the record
	 * we are working on.
	 */
	break;
    } /* end while record exist */

    if ((0 == pRecord->type) || 
	(0 == pRecord->bCnt) ||
	(pRecord >= ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	/*
	 * Internal Error: Stepped off the end of the mcell with out
	 * finding the record we want.
	 */
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_181, 
			 "Internal Error: Unable to find mcell (%d,%d,%d)'s record %d.\n"),
		 volNum, pageNum, mcellNum, recNum);
	return FAILURE;
    }


    /*
     * Found the record we want.
     */
    switch (pRecord->type) {
	case (BMTR_FS_STAT):
	    tempMcell.mcellId.vol  = volNum;
	    tempMcell.mcellId.page = pageNum;
	    tempMcell.mcellId.cell = mcellNum;
	    mcell = &tempMcell;

	    status = find_node(domain->mcellList, (void **)&mcell, &node);
	    if (SUCCESS == status) {
		MC_CLEAR_FSSTAT(mcell->status);
	    } else if (NOT_FOUND != status) {
		return status;
	    }
	    break;

	case (BSR_XTNTS):
	    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

	    /*
	     * For both ODS types check if the chain points to a 
	     * valid mcell.  If it does the clear the found
	     * pointer.
	     */
	    tempMcell.mcellId.vol  = pXtnt->chainVdIndex;
	    tempMcell.mcellId.page = pXtnt->chainMCId.page;
	    tempMcell.mcellId.cell = pXtnt->chainMCId.cell;
	    mcell = &tempMcell;

	    status = find_node(domain->mcellList, (void **)&mcell, &node);
	    if (SUCCESS == status) {
		MC_CLEAR_FOUND_PTR(mcell->status);
	    } else if (NOT_FOUND != status) {
		return status;
	    }

	    if (3 == dmnVersion) {
		/*
		 * On V3 ODS systems the extents are not used. 
		 * Execept for BMT0/RBMT mcells which we are not
		 * being called on.
		 */
	    } else {
		/*
		 * Loop through all the extents in this record.
		 */
		for (x = 0; 
		     x < (pXtnt->firstXtnt.xCnt - 1) && x < (BMT_XTNTS - 1);
		     x++) {
		    lbn = pXtnt->firstXtnt.bsXA[x].vdBlk;
		    extentSize = (pXtnt->firstXtnt.bsXA[x + 1].bsPage -
				  pXtnt->firstXtnt.bsXA[x].bsPage);

		    status = remove_extent_from_sbm(domain, volNum, 
						    lbn, extentSize, 
						    tag, setTag,
						    &mcellId, x);
		    if (SUCCESS != status) {
			return status;
		    }
		}
	    }
	    break;

	case (BSR_SHADOW_XTNTS): 
	    pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));
	    /*
	     * Loop on all extents
	     */
	    for (x = 0; 
		 x < (pShadow->xCnt - 1) && x < (BMT_SHADOW_XTNTS - 1);
		 x++) {
		lbn = pShadow->bsXA[x].vdBlk;
		extentSize = (pShadow->bsXA[x + 1].bsPage -
			      pShadow->bsXA[x].bsPage);

		status = remove_extent_from_sbm(domain, volNum, lbn, 
						extentSize, tag, 
						setTag, &mcellId, x);
		if (SUCCESS != status) {	
		    return status;
		}
	    }
	    break;

	case (BSR_XTRA_XTNTS):
	    pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
	    /*
	     * Loop on all extents
	     */
	    for (x = 0; 
		 x < (pXtra->xCnt - 1) && x < (BMT_XTRA_XTNTS - 1);
		 x++) {
		lbn = pXtra->bsXA[x].vdBlk;
		extentSize = (pXtra->bsXA[x + 1].bsPage -
			      pXtra->bsXA[x].bsPage);

		status = remove_extent_from_sbm(domain, volNum, lbn, 
						extentSize, tag, 
						setTag, &mcellId, x);
		if (SUCCESS != status) {
		    return status;
		}
	    }
	    break;

	default:
	    /*
	     * Non extent based record, nothing to do.
	     */
	    break;
    } /* end of switch */

    /*
     * Still on the record we wish to delete.
     */
    bCnt     = pRecord->bCnt;
    type     = pRecord->type;

    /*
     * Find the record (or the tail) after this record.
     */
    pNextRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 

    /*
     * Loop through all the remaining records.
     */
    memSize = 0;
    while ((0 != pNextRecord->type) &&
	   (0 != pNextRecord->bCnt) &&
	   (pNextRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	memSize += roundup(pNextRecord->bCnt, sizeof(int));

	pNextRecord = (bsMRT *) (((char *)pNextRecord) + 
				 roundup(pNextRecord->bCnt, sizeof(int))); 
    }
    
    if (0 != memSize) {
	/*
	 * If there are more record(s) move them forward.
	 */
	char *destination;
	char *source;

	destination = (char *)pRecord;
	source      = (((char *)pRecord) + 
		       roundup(pRecord->bCnt, sizeof(int)));
	memmove(destination, source, memSize);

	/*
	 * Reset pRecord to new tail record.
	 */
	pRecord = (bsMRT *) (((char *)pRecord) + memSize);
    }

    /*
     * Zero out the tail record.
     */
    pRecord->bCnt = 0;
    pRecord->type = 0;
    pRecord->version = 0;

    status = add_fixed_message(&(pPage->messages), BMT,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_182, 
				       "Removed mcell (%d,%d,%d) record %d : type %d, bCnt %d.\n"),
			       volNum, pageNum, mcellNum, 
			       recNum, type, bCnt);
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
} /* end clear_mcell_record */


/*
 * Function Name: correct_mcell_record_bcnt
 *
 * Description: This function will take a look at a mcell record, and 
 *              figure if the record bCnt matches the type. 
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     pPage : The page containing the mcell to be freed.
 *     mcellNum : Which mcell to free.
 *     pRecord: Pointer to a buffer containing a mcell record.
 *     currentSize: How much of the record is currently used.
 *
 * Returns: SUCCESS, FIXED, CORRUPT
 */
int correct_mcell_record_bcnt(domainT *domain, pageT *pPage, int mcellNum,
			      bsMRT *pRecord, int currentSize)
{
    char *funcName = "correct_mcell_record_bcnt";
    int  realByteCount;
    int  status;
    int  pageNum;
    int  recordType;
    int  volNum;
    int  modified;
    bsMPgT  *pBMTPage; /* The current BMT page we are working on */

    modified   = FALSE;
    volNum     = pPage->vol;
    pBMTPage   = (bsMPgT *)pPage->pageBuf;    
    pageNum    = pBMTPage->pageId;
    realByteCount = 0;

    switch (pRecord->type) {
	case (BSR_XTNTS):
	    realByteCount = sizeof(bsMRT) + sizeof(bsXtntRT);
	    break;

	case (BSR_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsBfAttrT);
	    break;

	case (BSR_VD_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsVdAttrT);
	    break;

	case (BSR_DMN_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsDmnAttrT);
	    break;

	case (BSR_XTRA_XTNTS):
	    realByteCount = sizeof(bsMRT) + sizeof(bsXtraXtntRT);
	    break;

	case (BSR_SHADOW_XTNTS):
	    realByteCount = sizeof(bsMRT) + sizeof(bsShadowXtntT);
	    break;

	case (BSR_MCELL_FREE_LIST):
	    realByteCount = sizeof(bsMRT) + sizeof(bsMcellFreeListT);
	    break;

	case (BSR_BFS_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsBfSetAttrT);
	    break;

	case (BSR_VD_IO_PARAMS):
	    realByteCount = sizeof(bsMRT) + sizeof(vdIoParamsT);
	    break;

	case (BSR_DEF_DEL_MCELL_LIST):
	    realByteCount = sizeof(bsMRT) + sizeof(delLinkRT);
	    break;

	case (BSR_DMN_MATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsDmnMAttrT);
	    break;

	case (BSR_BF_INHERIT_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsBfInheritAttrT);
	    break;

	case (BSR_RSVD17):
	    realByteCount = sizeof(bsMRT) + sizeof(DefbsrRsvd17);
	    break;

	case (BSR_BFS_QUOTA_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof (bsQuotaAttrT);
	    break;

	case (BSR_DMN_TRANS_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsDmnTAttrT);
	    break;

	case (BSR_DMN_SS_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsSSDmnAttrT);
	    break;

	case (BSR_DMN_FREEZE_ATTR):
	    realByteCount = sizeof(bsMRT) + sizeof(bsDmnFreezeAttrT);
	    break;

	case (BMTR_FS_DIR_INDEX_FILE):
	    realByteCount = sizeof(bsMRT) + sizeof(bfTagT);
	    break;

	case (BMTR_FS_INDEX_FILE):
	    realByteCount = sizeof(bsMRT) + sizeof(bsIdxBmtRecT);
	    break;

	case (BMTR_FS_TIME):
	    realByteCount = sizeof(bsMRT) + sizeof(int);
	    break;

	case (BMTR_FS_UNDEL_DIR):
	    realByteCount = sizeof(bsMRT) + sizeof(struct undel_dir_rec);
	    break;

	case (BMTR_FS_STAT):
	    realByteCount = sizeof(bsMRT) + sizeof(statT);
	    break;

	/*
	 * The size of BMTR_FS_DATA will be between 5 and 264.
	 * link_name = a non-NULL terminated character string less 
	 * than 260 characters.   This is a variable length record,
	 * so the best we can do is 'set it to the maximum length.
	 */
	case (BMTR_FS_DATA):
	    if (pRecord->bCnt > 264) {
		realByteCount = 264;
	    } else {
		realByteCount = pRecord->bCnt;
	    }
	    break;

	/*
	 * Variable size record.
	 *
	 *  The size of BSR_PROPLIST_HEAD is NEW_ODS 28 + bufferSize, 
	 *                                   OLD_ODS 24 + bufferSize.
	 *  The size of BSR_PROPLIST_DATA is variable.
	 *
	 */
	case (BSR_PROPLIST_HEAD):
	case (BSR_PROPLIST_DATA):
	default:
	    realByteCount = pRecord->bCnt;
	    break;
    } /* End Switch */

    if (realByteCount != pRecord->bCnt) {
        status = match_record_byte_count(pRecord, pageNum, mcellNum, 
					 FALSE, &recordType);
	if (0 == recordType) {
	    /*
	     * Can't find a better match assume bCnt is incorrect.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_137, 
					       "Modified mcell (%1$d,%2$d,%3$d)'s bCnt from %4$d to %5$d.\n"),
				       volNum, pageNum, mcellNum, 
				       pRecord->bCnt, realByteCount);
	    if (SUCCESS != status) {
		return status;
	    }

            pRecord->bCnt = realByteCount;
            modified = TRUE;
	} else {
	    /*
	     * Found a better record type, assume type is incorrect.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_138, 
					       "Modified mcell (%1$d,%2$d,%3$d)'s record type from %4$d to %5$d.\n"),
				       volNum, pageNum, mcellNum, 
				       pRecord->type, recordType);
	    if (SUCCESS != status) {
		return status;
	    }

	    pRecord->type = recordType;
	    modified = TRUE;
	} /* end if recordType */
    } /* end if/else realByteCount */

    /*
     * Now that we know the size, lets see if this record will
     * overflow the mcell record size.
     */
    if (currentSize + realByteCount > BSC_R_SZ - sizeof(bsMRT)) {
	return CORRUPT;
    }

    if (TRUE == modified) {
	return FIXED;
    }
    return SUCCESS;
} /* end correct_mcell_record_bcnt */

/* end fixfdmn_bmt.c */
