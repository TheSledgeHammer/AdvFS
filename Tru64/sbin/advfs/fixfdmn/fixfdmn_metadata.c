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
#pragma ident "@(#)$RCSfile: fixfdmn_metadata.c,v $ $Revision: 1.1.37.6 $ (DEC) $Date: 2006/03/17 03:07:37 $"

#include "fixfdmn.h"

/*
 * Global
 */
extern nl_catd _m_catd;


/*
 * Function Name: correct_magic_number (3.20)
 *
 * Description: This checks the boot block of the volume and, if
 *		needed, the RBMT to see if this is an AdvFS volume. If
 *		the magic number is corrupt, but the RBMT exists, then
 *		correct the magic number.
 *
 * Input parameters:
 *     domainT: The domain we are working on.
 *     volNum: The volume which needs to be checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, FAILURE, NON_ADVFS
 * 
 */
int correct_magic_number(domainT *domain, int volNum)
{
    char *funcName = "correct_magic_number";
    int	 status;
    int  status2;
    int	 modified;
    pageT    *page;
    pageT    *rbmtPage;
    uint32T  *superBlk;

    /*
     * Init variables
     */
    modified = FALSE;

    /*
     * Read the page starting at MSFS_FAKE_SB_BLK (LBN 16) to check for 
     * the magic number.
     */
    status = read_page_from_cache(volNum, MSFS_FAKE_SB_BLK, &page);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_326, 
			 "Failed to read the super-block on volume %s.\n"), 
		 domain->volumes[volNum].volName);
	return status;
    }
    superBlk = (uint32T *)(page->pageBuf);

    /*
     * If the known magic number is different from magic number 
     * on the page, then read the RBMT/BMT0 page to see if we really
     * have an AdvFS volume.
     */
    if (MSFS_MAGIC != superBlk[MSFS_MAGIC_OFFSET / sizeof(uint32T)]) {
	status = read_page_from_cache(volNum, RBMT_PAGE0, &rbmtPage);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_327,
			     "Failed to read RBMT/BMT0 on volume %s.\n"), 
		     domain->volumes[volNum].volName);
	    return status;
	}

	/*
	 * If this is a valid BMT page, then change the magic number
	 * in the fake super block.
	 */
	status = validate_bmt_page(domain, rbmtPage, 0, FALSE);
	if (SUCCESS == status) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_328, 
			     "Fixing incorrect magic number on volume %s.\n"),
		     domain->volumes[volNum].volName);
	    status = add_fixed_message(&(rbmtPage->messages), misc,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_329, 
					       "Modified magic number from %d to %d.\n"), 
				       superBlk[MSFS_MAGIC_OFFSET / sizeof(uint32T)],
				       MSFS_MAGIC);
	    if (SUCCESS != status) {
		return status;
	    }

	    superBlk[MSFS_MAGIC_OFFSET / sizeof(uint32T)] = MSFS_MAGIC;
	    modified = TRUE;
	    status = FIXED;
	} else {
	    status = NON_ADVFS;
	} /* end if a valid BMT page */
	
	status2 = release_page_to_cache(volNum, RBMT_PAGE0, 
					rbmtPage, FALSE, TRUE);
	if (SUCCESS != status2) {
	    /* No writemsg here - release_page_to_cache wrote one. */
	    return status2;
	} 
    } else {
	status = SUCCESS;
    } /* end if magic number is not correct */

    status2 = release_page_to_cache(volNum, MSFS_FAKE_SB_BLK, page, 
				    modified, FALSE);
    if (SUCCESS != status2) {
	/* No writemsg here - release_page_to_cache wrote one. */
	return status2;
   }

    return status;
} /* end correct_magic_number */


/*
 * Function Name: validate_tag_file_page (3.nn)
 *
 * Description: This is only checking the format of the data, and not
 *		the data itself.  This is used when we need to verify
 *		the page is of the correct format, but the tool is not
 *		ready to process the data. (i.e., the chicken and egg
 *		problem)
 *
 * Input parameters:
 *	tagFilePage: The page to have checks performed on.
 *	pageNum: Which page in the tag file (-1 if unknown).
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE
 * 
 */
int validate_tag_file_page(pageT *tagFilePage, int pageNum)
{
    char *funcName = "validate_tag_file_page";
    int	 numErrors; /* Number of errors found */
    int	 tagCounter;
    int	 startCounter;
    int	 allocTagMapCnt;
    int	 deadTagMapCnt;
    int	 freeTagMapCnt;
    int  volNum;
    LBNT  lbn;
    bsTDirPgT	 *pTagPage;
    bsTDirPgHdrT *pTagPageHdr;
    bsTMapT	 *pTagMap;
    mlBfTagT	 tag;
  
    /*
     * Initialize Variables
     */
    numErrors	   = 0;
    allocTagMapCnt = 0;
    freeTagMapCnt  = 0;
    deadTagMapCnt  = 0;
    volNum         = tagFilePage->vol;
    lbn            = tagFilePage->pageLBN;

    pTagPage	= (bsTDirPgT *)tagFilePage->pageBuf;
    pTagPageHdr = &(pTagPage->tpPgHdr);
    pTagMap	= &(pTagPage->tMapA[0]);

    /* 
     * Handle page 0, different from other pages. 
     */
    if (0 != pageNum) {
	startCounter = 0;
    } else {
	startCounter = 1;
    }

    if (pTagPageHdr->currPage != pageNum) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_330,
			 "Tag page header corrupt : currPage %d != pageNum %d.\n"),
		 pTagPageHdr->currPage, pageNum);
	numErrors++;
    }

    if (pTagPageHdr->nextFreeMap > BS_TD_TAGS_PG) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_331, 
			 "Tag page header corrupt : nextFreeMap %d > %d.\n"),
		 pTagPageHdr->nextFreeMap,  BS_TD_TAGS_PG);
	numErrors++;
    }

    for (tagCounter = startCounter; tagCounter < BS_TD_TAGS_PG; tagCounter++) {
	tag.seq = pTagMap[tagCounter].tmSeqNo;

	if (0 != (tag.seq & BS_TD_IN_USE)) {
	    allocTagMapCnt++;
	    /*
	     * Now check vdIndex, mcell
	     */
	    if (pTagMap[tagCounter].tmBfMCId.cell >  BSPG_CELLS) {
		writemsg(SV_DEBUG, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_332, 
				 "Tag corrupt : mcellId.cell %d > %d.\n"),
			 pTagMap[tagCounter].tmBfMCId.cell, BSPG_CELLS);
		numErrors++;
	    }

	} else if (BS_TD_DEAD_SLOT == tag.seq) {
	    deadTagMapCnt++;
	} else {
	    freeTagMapCnt++;
	    /*
	     * Now check nextMap
	     */
	    if (pTagMap[tagCounter].tmNextMap > BS_TD_TAGS_PG) {
		writemsg(SV_DEBUG,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_333,
				 "Tag corrupt : nextMap %d > %d.\n"),
			 pTagMap[tagCounter].tmNextMap, BS_TD_TAGS_PG);
		numErrors++;
	    }
	} /* end if type of tag */
    } /* end tag for loop */

    if (0 == pageNum) {
	/* 
	 * One less available tag map on page 0 
	 */
	freeTagMapCnt++;
    }

    if (pTagPageHdr->numAllocTMaps != allocTagMapCnt) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_334,
			 "Tag page header corrupt : numAllocTMaps %d != %d.\n"),
		 pTagPageHdr->numAllocTMaps, allocTagMapCnt);
	numErrors++;
    }

    if (pTagPageHdr->numDeadTMaps != deadTagMapCnt) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_335, 
			 "Tag page header corrupt : numDeadTMaps %d != %d.\n"),
		 pTagPageHdr->numDeadTMaps, deadTagMapCnt);
	numErrors++;
    }

    if ((BS_TD_TAGS_PG - 
	 (pTagPageHdr->numAllocTMaps + pTagPageHdr->numDeadTMaps)) 
	!= freeTagMapCnt) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_336, 
			 "Tag page header corrupt : freeTMap %d != %d.\n"),
		 (BS_TD_TAGS_PG - (pTagPageHdr->numAllocTMaps + 
				   pTagPageHdr->numDeadTMaps)),
		 freeTagMapCnt);
	numErrors++;
    }

    if (numErrors > 3) {
	/*
	 * First stab at setting a threshold is 3.
	 *
	 * Page is most likely not a tag file page, or is completely 
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_337,
			 "While validating tag page on volume %d LBN %u, found %d errors.\n"),
		 volNum, lbn, numErrors);
	return FAILURE;
    }
    
    return SUCCESS;
} /* end validate_tag_file_page */


/*
 * Function Name: validate_log_file_page (3.nn)
 *
 * Description: This is only checking the format of the data, and not
 *		the data itself.  This is used when we need to verify
 *		the page is of the correct format, but the tool is not
 *		ready to process the data. (i.e., the chicken and egg
 *		problem)
 *
 * Input parameters:
 *     domain: The domain we are working on.
 *     logFilePage: The page to have checks performed on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE
 * 
 */
int validate_log_file_page(domainT *domain, pageT *logFilePage)
{
    char *funcName = "validate_log_file_page";
    int	 numErrors; /* Number of errors found */
    logPgT	*pLogPage;
    logPgHdrT	*pLogHdr;
    logPgTrlrT	*pLogTrlr;
    bfDomainIdT	*dmnId;	
    int         volNum;
    LBNT        lbn;

    /*
     * Init variables.
     */
    volNum    = logFilePage->vol;
    lbn       = logFilePage->pageLBN;
    pLogPage  = (logPgT *)logFilePage->pageBuf;
    pLogHdr   = &(pLogPage->hdr);
    pLogTrlr  = &(pLogPage->trlr);
    dmnId     = &(domain->dmnId);
    numErrors = 0;

    /*
     * The page type is stored in two locations.
     */
    if (BFM_FTXLOG != pLogHdr->pgType) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_338, 
			 "Log file header pgType incorrect.\n"));
	numErrors++;
    }

    if (BFM_FTXLOG != pLogTrlr->pgType) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_338, 
			 "Log file header pgType incorrect.\n"));
	numErrors++;
    }
	 
    /*
     * The domain Id is stored in two locations.
     */
    if ((domain->dmnId.tv_sec != pLogHdr->dmnId.tv_sec) ||
	(domain->dmnId.tv_usec != pLogHdr->dmnId.tv_usec)) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_339,
			 "Log file header dmnId incorrect.\n"));
	numErrors++;
    }

    if ((domain->dmnId.tv_sec != pLogTrlr->dmnId.tv_sec) ||
	(domain->dmnId.tv_usec != pLogTrlr->dmnId.tv_usec)) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_340, 
			 "Validate log, header dmnId incorrect.\n"));
	numErrors++;
    }

    if (numErrors > 1) {
	/*
	 * Starting with a threshold of 1 for a log page
	 *
	 * Page is most likely not a log file page, or is completely 
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_341, 
			 "While validating log page on volume %d LBN %u, found %d errors.\n"),
		 volNum, lbn, numErrors);
	return FAILURE;
    }

    return SUCCESS;
} /* end validate_log_file_page */


/*
 * Function Name: clear_log (3.36)
 *
 * Description: To mark the log as clear we need find the last valid page
 *              in the log.  Then change the next page to make it look
 *              like the log is complete.  If the log has minor corruptions
 *              we will fix the log, if it has massive corruption or a
 *              a case were we have more than one end page we will call
 *              zero_log to completely empty the log.
 *
 * Input parameters:
 *     domain: The domain who's log is to be cleared.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY
 * 
 */
int clear_log(domainT *domain)
{
    char *funcName = "clear_log";
    int  b;
    int	 extent;
    int	 numExtents;
    LBNT currLbn;
    int  currVol;
    int	 modified;
    int	 page;
    int	 currPage;
    int	 status;
    int  numPageInExtent;
    LBNT extentLbn;
    int  volNum;
    int  maxPages;
    int  lastPage;
    int  recOffSet;
    int  currPageLSN;
    int  priorPageLSN;
    int  firstPageLSN;
    int  lastLSN;
    int  newLSN;
    volumeT    *volume;
    logPgT     *pLogPage;     /* a log page */
    logPgBlksT *pLogPgBlks;
    pagetoLBNT *pExtents;     /* The extents to check */
    pageT      *pCurrentPage; /* The current page we are checking */
    logRecHdrT *recHdr;
    ftxDoneLRT *dlrp;

    /*
     * Initialize variables
     */
    modified = FALSE;
    volNum = domain->logVol;
    volume  = &(domain->volumes[volNum]);
    pExtents = volume->volRbmt->logLBN;
    numExtents = volume->volRbmt->logLBNSize;
    maxPages = pExtents[numExtents].page;
    maxPages--; /* starts at 0, not 1 */
    lastPage = -1;
    priorPageLSN = -1;
    firstPageLSN = -1;
    lastLSN = -1;

    /*
     * Now check every log page.
     */
    currPage = 0;
    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute number of pages in this extent.
	 */
	numPageInExtent = (pExtents[extent + 1].page - pExtents[extent].page);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all log pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currPage++) {
	    currLbn = extentLbn + (page * BLOCKS_PER_PAGE);

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (SUCCESS != status) {
		return status;
	    }
	    
	    pLogPage = (logPgT *)pCurrentPage->pageBuf;

	    /*
	     * Check the log page header.
	     */
	    if ((BFM_FTXLOG != pLogPage->hdr.pgType) ||
		(pLogPage->hdr.dmnId.tv_sec != domain->dmnId.tv_sec) ||
		(pLogPage->hdr.dmnId.tv_usec != domain->dmnId.tv_usec)) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_343,
				 "Log page %d header's domainId incorrect, was %x.%x expecting %x.%x.\n"),
			 currPage,
			 pLogPage->hdr.dmnId.tv_sec,
			 pLogPage->hdr.dmnId.tv_usec,
			 domain->dmnId.tv_sec,
			 domain->dmnId.tv_usec);
			 
		status = add_fixed_message(&(pCurrentPage->messages), log,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_344, 
						   "Modified log page %d's header.\n"),
					   currPage);
		if (SUCCESS != status) {
		    return status;
		}
		pLogPage->hdr.pgType = BFM_FTXLOG;
		pLogPage->hdr.dmnId.tv_sec = domain->dmnId.tv_sec;
		pLogPage->hdr.dmnId.tv_usec = domain->dmnId.tv_usec;
		modified = TRUE;
	    } /* end if header page type */

	    /*
	     * Check the log page trailer.
	     */
	    if ((pLogPage->trlr.pgType != BFM_FTXLOG) ||
		(pLogPage->trlr.dmnId.tv_sec != domain->dmnId.tv_sec) ||
		(pLogPage->trlr.dmnId.tv_usec != domain->dmnId.tv_usec)) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_345, 
				 "Log page %d trailer's domainId incorrect, was %x.%x expecting %x.%x.\n"),
			 currPage,
			 pLogPage->hdr.dmnId.tv_sec,
			 pLogPage->hdr.dmnId.tv_usec,
			 domain->dmnId.tv_sec,
			 domain->dmnId.tv_usec);

		status = add_fixed_message(&(pCurrentPage->messages), log,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_346,
						   "Modified log page %d's trailer.\n"),
					   currPage);
		if (SUCCESS != status) {
		    return status;
		}
		pLogPage->trlr.pgType = BFM_FTXLOG;
		pLogPage->trlr.dmnId.tv_sec = domain->dmnId.tv_sec;
		pLogPage->trlr.dmnId.tv_usec = domain->dmnId.tv_usec;
		modified = TRUE;
	    } /* end if trailer page type */

	    /*
	     * Now check the LSN of this page.  Is it less than LSN
	     * on the prior page.  If this is the case we have found
	     * the last page in the log.
	     */
	    currPageLSN = pLogPage->hdr.thisPageLSN.num;

	    if (0 == currPage) {
		/*
		 * First Page in log.
		 */
		firstPageLSN = currPageLSN;
	    } else {
		/*
		 * All other pages.
		 */
		if (log_lsn_gt(priorPageLSN, currPageLSN)) {
		    if (-1 == lastPage) {
			/*
			 * Found the last page of the log.
			 */
			lastPage = currPage - 1;
			lastLSN = priorPageLSN;
		    } else {
			/*
			 * Found more than one last page.
			 */
			lastPage = -2;
			lastLSN = -2;
		    }
		} /* end if lsn greater than */
	    } /* end if/else currPage equal 0 */

	    /* 
	     * Special Case, last Page in log, need to check for the
	     * wrap case (page 511 to page 0).
	     */

	    if (currPage == maxPages) {
		if (log_lsn_gt(currPageLSN,firstPageLSN)) {
		    if (-1 == lastPage) {
			/*
			 * Found the last page of the log.
			 */
			lastPage = currPage;
			lastLSN = currPageLSN;
		    } else {
			/*
			 * Found more than one last page.
			 */
			lastPage = -2;
			lastLSN = -2;
		    }
		} /* end if lsn greater than */
	    } /* end if currPage equal maxPages */

	    /*
	     * Set priorPageLSN to this page's LSN.
	     */
	    priorPageLSN = currPageLSN;

	    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;
	}/* end for page loop */
    } /* end for extent loop */

    /* 
     * Check to see if more than one log end page.  If this is the case
     * then just zero out the log to make sure that the kernel will be
     * able to always be able to read it.
     */
    if (-2 == lastPage) {
	status = zero_log(domain);
	if (SUCCESS != status) {
	    return status;
	}
	
	/*
	 * log has been zeroed, no need to continue.
	 */
	return SUCCESS;
    }

    /*
     * The LSN needs to be higher than the previous one, but no larger 
     * than 1<<29. The kernel steps this value by 2. 
     */
    newLSN = lastLSN + 2;

    if (-1 == lastPage) {
	/* 
	 * All the pages have the same LSN, this should only happen if
	 * we have zeroed the log.  We are going to set the newLSN to
	 * zero in this case.
	 */
	lastPage = 0;
	newLSN  = 0;
    } else if (lastPage == maxPages) {
	/*
	 * The last page of the log is the actual last page,
	 * so we need to wrap the log back to page 0.
	 */
	lastPage = 0;
    } else {
	/*
	 * Now that we have found the last valid page in the log,
	 * set the page following it to looks like it is complete.
	 */
	lastPage++;
    }

    status = convert_page_to_lbn(pExtents, numExtents, lastPage, 
				 &currLbn, &currVol);
    if (SUCCESS != status) {
	return status;
    }

    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
    if (SUCCESS != status) {
	return status;
    }
   
    pLogPage = (logPgT *)pCurrentPage->pageBuf;
    pLogPgBlks = (logPgBlksT *)pCurrentPage->pageBuf;

    /*
     * Make sure that logPage is clear.
     */
    bzero(pLogPage, PAGESIZE);
    
    /*
     * Fill in the Header values.
     */
    pLogPage->hdr.thisPageLSN.num = newLSN;;
    pLogPage->hdr.pgType = BFM_FTXLOG;
    pLogPage->hdr.dmnId.tv_sec = domain->dmnId.tv_sec;
    pLogPage->hdr.dmnId.tv_usec = domain->dmnId.tv_usec;
    pLogPage->hdr.pgSafe = TRUE;
    pLogPage->hdr.chkBit = 0;
    pLogPage->hdr.curLastRec = -1;
    pLogPage->hdr.prevLastRec = -1;

    pLogPage->hdr.firstLogRec.page = lastPage;
    pLogPage->hdr.firstLogRec.offset = 0;
    pLogPage->hdr.firstLogRec.lsn.num = newLSN;

    /*
     * Set recHdr to first record block.
     */
    recOffSet = 0; /* first record */
    recHdr = (logRecHdrT *) &(pLogPage->data[recOffSet]);

    recHdr->nextRec = logEndOfRecords;
    recHdr->prevRec = logEndOfRecords;
    recHdr->prevClientRec = logEndOfRecords;
    
    recHdr->firstSeg.page = lastPage;
    recHdr->firstSeg.offset = 0;
    recHdr->firstSeg.lsn.num = newLSN;

    recHdr->wordCnt = REC_HDR_WORDS + DONE_LRT_WORDS;
    recHdr->clientWordCnt = DONE_LRT_WORDS;
    recHdr->lsn.num = newLSN;
    recHdr->segment = 0 ;     
    
    /*
     * Creat a Checkpoint record on the page.
     */
    dlrp = (ftxDoneLRT *) &(pLogPage->data[recOffSet + REC_HDR_WORDS]);
    
    dlrp->type = ftxDoneLR;
    dlrp->level = 0;
    dlrp->atomicRPass = 2;
    dlrp->member = 0;
    dlrp->ftxId = 2; 
    dlrp->bfDmnId.tv_sec = domain->dmnId.tv_sec; 
    dlrp->bfDmnId.tv_usec = domain->dmnId.tv_usec; 
    dlrp->crashRedo.page = lastPage;
    dlrp->crashRedo.offset = 0;
    dlrp->crashRedo.lsn.num = newLSN;
    dlrp->agentId = FTA_FTX_CHECKPOINT_LOG;
    dlrp->contOrUndoRBcnt = 0;
    dlrp->rootDRBcnt = 0;
    dlrp->opRedoRBcnt = 0;

    /*
     * Fill in the Trailer values.
     */
    pLogPage->trlr.pgType = BFM_FTXLOG;
    pLogPage->trlr.dmnId.tv_sec = domain->dmnId.tv_sec;
    pLogPage->trlr.dmnId.tv_usec = domain->dmnId.tv_usec;

    /* 
     * Puts the page's first record's LSN into each block in the page
     * (except the first block which already has the LSN in it).  The
     * value overwritten by the LSN is saved in the "lsnOverwriteVal"
     * array of the page's trailer.  Also, sets the 'check bit'
     * (version) of each block.  
     */
    for (b = 1; b < ADVFS_PGSZ_IN_BLKS; b++) {
        /*
         * Put page lsn into each block. 
	 */
        pLogPage->trlr.lsnOverwriteVal[b - 1] = pLogPgBlks->blks[b].lsn;
        pLogPgBlks->blks[b].lsn = pLogPage->hdr.thisPageLSN;

        /*
         * Clear check bit (version) of each block.
         */
	pLogPgBlks->blks[b].lsn.num &= 0xfffffffe;
    }

    status = add_fixed_message(&(pCurrentPage->messages), log,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_342,
				       "Modified log page %d so the log is marked as empty.\n"),
			       lastPage);
    if (SUCCESS != status) {
	return status;
    }

    status = release_page_to_cache(currVol, currLbn, pCurrentPage,
				   TRUE, FALSE);
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
} /* end clear_log */


/*
 * Function Name: zero_log (3.xx)
 *
 * Description: To mark the log as clear we need to reset all pages
 *              of the log to a LSN of '0'.  
 *		
 * Input parameters:
 *     domain: The domain who's log is to be zeroed.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY
 * 
 */
int zero_log(domainT *domain)
{
    char *funcName = "zero_log";
    int	 extent;
    int	 numExtents;
    LBNT currLbn;
    int  currVol;
    int	 modified;
    int	 page;
    int	 currPage;
    int	 status;
    int  numPageInExtent;
    LBNT extentLbn;
    int  volNum;
    volumeT    *volume;
    logPgT     *pLogPage;     /* a log page */
    pagetoLBNT *pExtents;     /* The extents to check */
    pageT      *pCurrentPage; /* The current page we are checking */

    /*
     * Initialize variables
     */
    modified = FALSE;
    volNum = domain->logVol;
    volume  = &(domain->volumes[volNum]);
    pExtents = volume->volRbmt->logLBN;
    numExtents = volume->volRbmt->logLBNSize;

    /*
     * Now reset every log page.
     */
    currPage = 0;
    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute number of pages in this extent.
	 */
	numPageInExtent = (pExtents[extent + 1].page - pExtents[extent].page);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all log pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currPage++) {
	    currLbn = extentLbn + (page * BLOCKS_PER_PAGE);

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (SUCCESS != status) {
		return status;
	    }

	    pLogPage = (logPgT *)pCurrentPage->pageBuf;

	    /*
	     * Make sure that logPage is clear.
	     */
	    bzero(pLogPage, PAGESIZE);

	    /*
	     * Fill in the default values.
	     */
	    pLogPage->hdr.pgType = BFM_FTXLOG;
	    pLogPage->hdr.dmnId.tv_sec = domain->dmnId.tv_sec;
	    pLogPage->hdr.dmnId.tv_usec = domain->dmnId.tv_usec;
	    pLogPage->hdr.pgSafe = TRUE;
	    pLogPage->hdr.curLastRec = 0;
	    pLogPage->hdr.prevLastRec = -1;
	    pLogPage->hdr.thisPageLSN.num = 0;

	    pLogPage->trlr.pgType = BFM_FTXLOG;
	    pLogPage->trlr.dmnId.tv_sec = domain->dmnId.tv_sec;
	    pLogPage->trlr.dmnId.tv_usec = domain->dmnId.tv_usec;

	    status = add_fixed_message(&(pCurrentPage->messages), log,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_342,
					       "Modified log page %d so the log is marked as empty.\n"),
				       currPage);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = release_page_to_cache(currVol, currLbn, pCurrentPage,
					   TRUE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	} /* end page loop */
    } /* end extent loop */

    return SUCCESS;
} /* end zero_log */


/*
 * Function Name: validate_bmt_page (3.32)
 *
 * Description: This is only checking the format of the data, and not
 *		the data itself.  This is used when we need to verify
 *		the page is of the correct format, but the tool is not
 *		ready to process the data. (i.e., the chicken and egg
 *		problem)
 *
 * Input parameters:
 *     domain: Pointer to the domain being checked.
 *     pPage: The page to have checks performed on.
 *     pageId: The id number of the page we are checking. 
 *     rbmtFlag: TRUE if RBMT.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE 
 */
int validate_bmt_page(domainT *domain, pageT *pPage, 
		      int pageId, int rbmtFlag)
{
    char *funcName = "validate_bmt_page";
    int	 status;
    int	 mcell;
    int	 volNum;
    int	 numErrors; /* Number of errors found */

    /*
     * Init variables.
     */
    volNum = pPage->vol;
    numErrors = 0;

    status = validate_bmt_page_header(domain, pPage, pageId, 
				      &numErrors, rbmtFlag);
    if (CORRUPT == status) {
	/*
	 * We will now perform additional checks, to see if header is 
	 * just corrupted, or if this is not a BMT page.
	 */
	for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
	    status = validate_mcell_header(domain, pPage, pageId,
					   mcell, FALSE, &numErrors);
	    if (CORRUPT == status) {
		writemsg(SV_DEBUG, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_347, 
				 "Mcell (%d,%d,%d)'s header is corrupt.\n"),
			 volNum, pageId, mcell);
		/*
		 * Checking of headers should be enough verify the page. 
		 * If needed we could add the following additional check,
		 * but at this point it is felt it would be a waste of 
		 * time.
		 * 
		 * Loop through mcell record:
		 *     Call validate_mcell_records	  
		 *     If call fails:
		 *	   Increment numErrors.
		 */
	    } /* end if header corrupt */
	} /*end mcell loop */
    } /* end if mcell corrupt */

    if (numErrors > 5) {
	/*
	 * As this page has lot of possible values its threshold is 
	 * being set to 5
	 *
	 * Page is most likely not a BMT file page, or is completely 
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_348, 
			 "While validating BMT page on volume %s page %d, found %d errors.\n"),
		 domain->volumes[volNum].volName, pageId, numErrors);
	return FAILURE;
    }

    return SUCCESS;
} /* end validate_bmt_page */


/*
 * Function Name:validate_bmt_page_header (3.33)
 *
 * Description: This function needs to validate the format of the BMT
 *		page header. It does not check to see if the actual
 *		data is correct, but if it falls within valid values.
 *
 * Input parameters:
 *     domain: Pointer to the domain being checked.
 *     pPage: The page to check the header on.
 *     pageId: The BMT page number.
 *     numErrors: Number of errors already found.
 *     rbmtFlag: TRUE if RBMT, FALSE if BMT.
 *
 * Output parameters: 
 *     numErrors: The number of errors found in the bmt header.
 *
 * Returns: SUCCESS, CORRUPT.  
 */
int validate_bmt_page_header(domainT *domain, pageT *pPage, int pageId, 
			     int *numErrors, int rbmtFlag)
{
    char *funcName = "validate_bmt_page_header";
    int	 maxBMTPage;
    int	 volNum;
    int  errorsFound;
    bsMPgT  *bmtPage; /* The current BMT page we are working on */
    volumeT *volume;

    /*
     * Init Variables.
     */
    errorsFound = 0;
    bmtPage = (bsMPgT *)pPage->pageBuf;
    volNum  = pPage->vol;
    volume  = &(domain->volumes[volNum]);

    if (TRUE == rbmtFlag) {
	/*
	 * Chicken and Egg situation, can't get maxRbmtPage at this time.
	 *
	 * Maxbmtpage = volume->volRbmt->maxRbmtPage;
	 */
	maxBMTPage = volume->volSize / BLOCKS_PER_PAGE;
    } else {
	maxBMTPage = volume->volRbmt->maxBmtPage;
    }

    if ((bmtPage->nextfreeMCId.cell < 0) ||
	(bmtPage->nextfreeMCId.cell > BSPG_CELLS)) {
	/*
	 * falls outside of the valid range
	 */
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_349,
			 "BMT page (%d,%d)'s nextFreeMcId = %d, outside valid range.\n"),
		 volNum, pageId, bmtPage->nextfreeMCId.cell);
	errorsFound++;
    }

    if ((bmtPage->nextFreePg < 0) &&
	(bmtPage->nextFreePg > maxBMTPage)) {
	/*
	 * falls outside of the valid range:
	 */
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_350, 
			 "BMT page (%d,%d)'s nextFreePg = %d, outside valid range.\n"), 
		 volNum, pageId, bmtPage->nextFreePg);
	errorsFound++;
    }

    if (bmtPage->pageId != pageId) {
	/*
	 * Not the correct page id.
	 */
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_351, 
			 "BMT page (%d,%d)'s pageId = %d, invalid Page.\n"), 
		 volNum, pageId, bmtPage->pageId);
	errorsFound++;
    }

    if (bmtPage->megaVersion != domain->dmnVersion) {
	/*
	 * Not the correct domain version.
	 */
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_352,
			 "BMT page (%d,%d)'s megaVersion = %d, incorrect value.\n"), 
		 volNum, pageId, bmtPage->megaVersion);
	errorsFound++;
    }

    *numErrors += errorsFound;

    if (errorsFound > 0) {
	/*
	 * Page is most likely not a BMT file page, or is completely 
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_353, 
			 "While validating BMT page header on volume %s page %d, found %d errors.\n"),
		 domain->volumes[volNum].volName, pageId, errorsFound);
	return CORRUPT;
    }

    return SUCCESS;
} /* end validate_bmt_page_header */


/*
 * Function Name:validate_mcell_header (3.34)
 *
 * Description: This function needs to validate the format of the BMT
 *		mcell record.  It does not check to see if the actual
 *		data is correct, but if it falls within valid values.
 *
 * Input parameters:
 *     domain: Pointer to the domain being checked.
 *     pPage: The page to check the header on.
 *     pageId: The page number we are checking.
 *     mcellId: Which mcell on the page to check.
 *     rbmtFlag: FALSE equals BMT, TRUE equals RBMT.
 *     numErrors: The current number of errors found.
 *
 * Output parameters:
 *     numErrors: The number errors found in the mcell header.
 *
 * Returns: SUCCESS, CORRUPT.  
 */
int validate_mcell_header(domainT *domain, pageT *pPage, int pageId,
			  int mcellId, int rbmtFlag, int *numErrors)
{
    char    *funcName = "validate_mcell_header";
    int	    errorsFound; /* Counter of the number of corruptions */
    int	    maxBMTPage;
    int	    volNum;
    bsMPgT  *bmtPage; /* The current BMT page we are working on */
    bsMCT   *pMcell;  /* The current BMT mcell we are working on */
    volumeT *volume;
    
    /*
     * Init Variables.
     */
    errorsFound = 0;
    volNum  = pPage->vol;
    bmtPage = (bsMPgT *)pPage->pageBuf;
    pMcell  = &(bmtPage->bsMCA[mcellId]);

    /*
     * Use BS_MAX_VDI not MAX_VOLUMES as this is a ODS we are looking at.
     */
    if ((pMcell->nextVdIndex < 0) ||
	(pMcell->nextVdIndex > BS_MAX_VDI)) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_354,
			 "Mcell (%d,%d,%d)'s nextVdIndex %d, is outside valid range.\n"), 
		 volNum, pageId, mcellId, pMcell->nextVdIndex);
	errorsFound++;
    } else if (0 != pMcell->nextVdIndex) {
	volume	= &(domain->volumes[pMcell->nextVdIndex]);
	if (-1 == volume->volFD) {
	    writemsg(SV_DEBUG,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_355, 
			     "Mcell (%d,%d,%d)'s nextVdIndex %d, unknown volume.\n"), 
		     volNum, pageId, mcellId, pMcell->nextVdIndex);
	    errorsFound++;
	    /*
	     * No clue what the maxBMTPage is.
	     */
	    maxBMTPage = 0;
	} else {
	    if (TRUE == rbmtFlag) {
		maxBMTPage = volume->volSize / BLOCKS_PER_PAGE;
	    } else {
		maxBMTPage = volume->volRbmt->maxBmtPage;
	    }
	}	

	if ((pMcell->nextMCId.page  < 0) ||
	    (pMcell->nextMCId.page  > maxBMTPage)) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_356, 
			     "Mcell (%d,%d,%d)'s nextMCId.page %d, is outside valid range.\n"), 
		     volNum, pageId, mcellId, pMcell->nextMCId.page);
	    errorsFound++;
	}

	if ((pMcell->nextMCId.cell  < 0) ||
	    (pMcell->nextMCId.cell  >= BSPG_CELLS)) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_357,
			     "Mcell (%d,%d,%d)'s nextMCId.cell %d, is outside valid range.\n"), 
		     volNum, pageId, mcellId, pMcell->nextMCId.cell);
	    errorsFound++;
	}
    } else {
	/*
	 * pMcell->nextVdIndex = 0, so cell and page better be 0.
	 */
	if ((0 != pMcell->nextMCId.cell) ||
	    (0 != pMcell->nextMCId.page)) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_358,
			     "Mcell (%d,%d,%d)'s nextMCId (%d,%d,%d) is invalid.\n"), 
		     volNum, pageId, mcellId, pMcell->nextVdIndex, 
		     pMcell->nextMCId.page, pMcell->nextMCId.cell);
	    errorsFound++;
	}
    } /* if vdIndex */

    /*
     * Can't check pMcell->linkSegment without walking chains.
     */

    /*
     * Valid cases for tag & set tag
     *
     *	     Tag	bfSetTag
     *	     < 0	  -2
     *	       0	   0
     *	     > 0	  -2 or > 0
     */
    if (((pMcell->tag.num < 0) && (-2 != pMcell->bfSetTag.num)) ||
	((0 == pMcell->tag.num) && (0 != pMcell->bfSetTag.num)) ||
	((pMcell->tag.num > 0) && (!((-2 == pMcell->bfSetTag.num) || 
				     (pMcell->bfSetTag.num > 0))))) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_359, 
			 "Mcell (%d,%d,%d)'s tag %d bfSetTag %d is invalid.\n"), 
		 volNum, pageId, mcellId, 
		 pMcell->tag.num, pMcell->bfSetTag.num);
	errorsFound++;
    }

    *numErrors += errorsFound;

    if (errorsFound > 1) {
	/*
	 * Page is most likely not a BMT file page, or is completely
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_360, 
			 "While validating mcell header on (%d,%d,%d), found %d errors.\n"),
		 volNum, pageId, mcellId, errorsFound);
	return CORRUPT;
    }

    return SUCCESS;
} /* end validate_mcell_header */


/*
 * Function Name: reset_free_mcell_links (3.61)
 *
 * Description: This routine sets the next free mcell link in the BMT
 *		page header to the first free mcell on the page, then
 *		links that mcell to the next free one, and so on,
 *		until all of the free mcells on the BMT page are
 *		linked.
 *
 * Input parameters:
 *     page: The BMT page being worked on.
 *     freeMcells: Pointer to an array containing indicators of which
 *		   mcells are free.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, NO_MEMORY.
 * 
 */
int reset_free_mcell_links(pageT *pPage, mcellUsage *freeMcells)
{
    char *funcName = "reset_free_mcell_links";
    int	 prevMcell;
    int	 pageNum;
    int	 mcell;
    int	 tag;
    int	 seq;
    int	 foundFirst;
    int	 dmnVersion;
    bsMPgT     *bmtPage;     /* The current BMT page we are working on */
    bsMCT      *pMcell;	     /* The current BMT mcell we are working on */
    mcellUsage *pMcellArray;
  
    /*
     * Init variables
     */
    bmtPage = (bsMPgT *)pPage->pageBuf;
    prevMcell	      = -1;
    pageNum	      = bmtPage->pageId;
    dmnVersion	      = bmtPage->megaVersion;

    /*
     * If array is not passed in create it.
     */
    assert(NULL != freeMcells);
    pMcellArray = freeMcells;

    /*
     * Deferred Delete List and mcell Free list is a special case 
     * (tag & seq equals 0).
     */
    if (((3 == dmnVersion) && (1 == pageNum)) ||
	((3 != dmnVersion) && (0 == pageNum))) {
	    pMcellArray[0] = USED;
    }

    /*
     * Loop through the mcells on this page to reset the links.
     */
    foundFirst = FALSE;
    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
	if ((FREE == pMcellArray[mcell]) ||
	    (ONLIST == pMcellArray[mcell])) {
	    if (FALSE == foundFirst) {
		foundFirst = TRUE;
		bmtPage->nextfreeMCId.page = pageNum;
		bmtPage->nextfreeMCId.cell = mcell;
		prevMcell = mcell;
	    } else {
		pMcell = &(bmtPage->bsMCA[prevMcell]);
		pMcell->nextMCId.page = pageNum;
		pMcell->nextMCId.cell = mcell;
		prevMcell = mcell;
	    } /* end if not foundFirst */
	} /* end if FREE */
    } /* end mcell loop */
    
    /*
     * When we exit the loop prevMcell is pointing to the last free mcell 
     * on the page. (or -1 if none are found).
     */
    if (-1 != prevMcell) {
	pMcell = &(bmtPage->bsMCA[prevMcell]);
	pMcell->nextMCId.page = 0;
	pMcell->nextMCId.cell = 0;
    }

    return SUCCESS;
} /* end reset_free_mcell_links */


/*
 * Function Name: correct_free_mcell_list (3.63)
 *
 * Description: This function will step through each page in the BMT
 *		which has free mcells. Once on the page it will check
 *		the chain of mcells on this page.  After the chain has
 *		been checked it then loops through the BMT status
 *		array and adds the free pages which are currently not
 *		on the free page list.
 *
 * Input parameters:
 *     volume: The volume to work on.
 *     bmtPageStatus: The array to keep that status bits for each page.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FIXED, CORRUPT, FAILURE.
 * 
 */
int correct_free_mcell_list(volumeT *volume, int *bmtStatusArray)
{
    char *funcName = "correct_free_mcell_list";
    int	 status;
    int	 extent;
    int	 numExtents;
    int	 dmnVersion;
    int	 currPageNum;
    LBNT currLbn;
    int  currVol;
    int	 priorPageNum;
    LBNT priorLbn;
    int  priorVol;
    int	 headPageNum;
    LBNT headLbn;
    int  headVol;
    int	 nextPageNum;
    int	 firstPageNum;
    int	 modified;
    int	 terminator;
    int	 numPageInExtent;
    LBNT extentLbn;
    int	 page;
    pageT  *pHeadPage;	   /* Pointer to first BMT Page */
    pageT  *pCurrPage;	   /* Pointer to current page. */
    pageT  *pPriorPage;	   /* Pointer to prior page. */
    bsMPgT *bmtPage;	   /* The current BMT page we are working on */
    bsMPgT *headBmtPage;   /* The first BMT page */
    bsMPgT *priorBmtPage;  /* The prior BMT page we are working on */
    bsMCT  *pMcell;	   /* The BMT mcell we are working on */
    bsMRT  *pRecord;
    pagetoLBNT *pExtents;
    bsMcellFreeListT *headRecord; /* mcell free list record */

    /*
     * Init variables
     */
    dmnVersion = volume->dmnVersion;
    pExtents   = volume->volRbmt->bmtLBN;
    numExtents = volume->volRbmt->bmtLBNSize;

    if (3 == dmnVersion) {
	headPageNum = 1;
	terminator  = 0;
    } else {
	headPageNum = 0;
	terminator  = -1;
    }

    status = convert_page_to_lbn(pExtents, numExtents, headPageNum, 
				 &headLbn, &headVol);
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Set curr to head.
     */
    currPageNum = headPageNum;
    currLbn	= headLbn;
    currVol	= headVol;
    
    /*
     * Load the page which contains the free list head record.
     */
    status = read_page_from_cache(currVol, currLbn, &pCurrPage);
    if (SUCCESS != status) {
	return status;
    }
      
    bmtPage = (bsMPgT *)pCurrPage->pageBuf;
    pMcell  = &(bmtPage->bsMCA[0]);
    pRecord = (bsMRT *)pMcell->bsMR0;
    firstPageNum = -2;

    while ((0 != pRecord->type) &&
	   (0 != pRecord->bCnt) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	if (BSR_MCELL_FREE_LIST == pRecord->type) {
	    headRecord = (bsMcellFreeListT *)((char *)pRecord + sizeof(bsMRT));
	    firstPageNum = headRecord->headPg;
	    break;
	}
	/*
	 * Set pRecord to next record  of pMcell.
	 */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 
    } /* end mcell record loop */

    /*
     * Release the page which has the head record.
     */
    status = release_page_to_cache(currVol, currLbn, pCurrPage, FALSE, FALSE);
    if (SUCCESS != status) {
	return status;
    }

    if (-2 == firstPageNum) {
	/*
	 * Unable to find the record which contains the head record.
	 */
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_363,
			 "Unable to locate the head of the free mcell list.\n"));
	return CORRUPT;
    }

    if (terminator == firstPageNum) {
	/*
	 * The head points to the terminator
	 */
	pCurrPage = NULL;
    } else {
	/*
	 * Now load the first first page.
	 */
	currPageNum  = firstPageNum;
	priorPageNum = currPageNum;

	status = convert_page_to_lbn(pExtents, numExtents, currPageNum, 
				     &currLbn, &currVol);
	if (SUCCESS != status) {
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 
	modified = FALSE;
    }

    /*
     * Loop through all the free pages in the chain, looking for
     * corruptions.
     */
    while (NULL != pCurrPage) {
	bmtPage = (bsMPgT *)pCurrPage->pageBuf;

	/*
	 * Set the holder for the next page.
	 */
	nextPageNum = bmtPage->nextFreePg;
	
	if (currPageNum == nextPageNum) {
	    /*
	     * We have a loop on the free page list.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_364, 
			     "Modifying volume %s's free mcell list, found loop in list.\n"),
		     volume->volName);

	    status = add_fixed_message(&(pCurrPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_365, 
					       "Modified free mcell list, cleared loop.\n"));
	    if (SUCCESS != status) {
		return status;
	    }
	    nextPageNum = terminator;
	    bmtPage->nextFreePg = terminator;
	    modified = TRUE;
	}

	if (BMTPAGE_FULL != bmtStatusArray[currPageNum]) {
	    bmtStatusArray[currPageNum] = BMTPAGE_ON_LIST;
	} else {
	    /*
	     * Error case, page is on free list, but marked as FULL.
	     * Remove from list.  Special case first page
	     */
	    if (currPageNum != priorPageNum) {
		status = convert_page_to_lbn(pExtents, numExtents, 
					     priorPageNum,
					     &priorLbn, &priorVol);
		if (SUCCESS != status) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
				     "Can't convert BMT page %d to LBN.\n"), 
			     priorPageNum);
		    return FAILURE;
		}

		status = read_page_from_cache(priorVol, priorLbn, 
					      &pPriorPage);
		if (SUCCESS != status) {
		   return status;
		} 

		status = add_fixed_message(&(pPriorPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_367, 
						   "Modified free mcell list, found full page on list.\n"));
		if (SUCCESS != status) {
		    return status;
		}

		priorBmtPage = (bsMPgT *)pPriorPage->pageBuf;
		priorBmtPage->nextFreePg = bmtPage->nextFreePg;

		status = release_page_to_cache(priorVol, priorLbn,
					       pPriorPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    } else {
		/*
		 * The current page and prior page are the same. 
		 * This only happens if the head record points to this
		 * page.
		 */
		if (currPageNum != headPageNum) {
		    status = read_page_from_cache(headVol, headLbn,
						  &pHeadPage);
		    if (SUCCESS != status) {
			return status;
		    }
		} else {
		    pHeadPage = pCurrPage;
		}

		headBmtPage = (bsMPgT *)pHeadPage->pageBuf;
		pMcell	= &(headBmtPage->bsMCA[0]);
		pRecord = (bsMRT *)pMcell->bsMR0;

		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		    if (BSR_MCELL_FREE_LIST == pRecord->type) {
			headRecord = (bsMcellFreeListT *)((char *)pRecord + 
							  sizeof(bsMRT));
			/*
			 * Set head pointer
			 */
			status = add_fixed_message(&(pHeadPage->messages),
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_368,
							   "Modified head of BMT free mcell list from %d to %d.\n"),
						   headRecord->headPg,
						   bmtPage->nextFreePg);
			if (SUCCESS != status) {
			    return status;
			}
			headRecord->headPg = bmtPage->nextFreePg;
			break;
		    }
		    /*
		     * Set pRecord to next record  of pMcell.
		     */
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, sizeof(int))); 
		} /* end mcell record loop */
		if (currPageNum != headPageNum) {
		    status = release_page_to_cache(headVol, headLbn, 
						   pHeadPage, TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		} else {
		    pHeadPage = NULL;
		}
	    } /* end else currPageNum == nextPageNum */
	    bmtPage->nextFreePg = terminator;
	    modified = TRUE;
	}
	status = release_page_to_cache(currVol, currLbn, pCurrPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	modified = FALSE;

	/*
	 * Get the next page.
	 */
	priorPageNum = currPageNum;
	currPageNum = nextPageNum;
	
	if (currPageNum == terminator) {
	    pCurrPage = NULL;
	} else {
	    status = convert_page_to_lbn(pExtents, numExtents, currPageNum,
					 &currLbn, &currVol);
	    if (SUCCESS != status) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
				 "Can't convert BMT page %d to LBN.\n"), 
			 currPageNum);
		return FAILURE;
	    }

	    status = read_page_from_cache(currVol, currLbn, &pCurrPage);
	    if (SUCCESS != status) {
		return status;
	    } 
	} /* end if/else currPageNum == termintor */
    } /* end while loop of pages on chain */

    /*
     * Loop though all pages in BMT, this time we are looking for pages 
     * which are marked as on the free list, but not in the chain.  We 
     * only read the page from disk if it is in error.
     */
    currPageNum = 0;
    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute number of pages in this extent.
	 */
	numPageInExtent = (pExtents[extent + 1].page - pExtents[extent].page);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all bmt file pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currPageNum++) {
	    currLbn = extentLbn + (page * BLOCKS_PER_PAGE);
	    
	    if ((3 == dmnVersion) && (0 == currPageNum)) {
		/*
		 * Special Case: This page is not on the free list.
		 */
		continue;
	    }

	    if (BMTPAGE_FREE == bmtStatusArray[currPageNum]) {
		/*
		 * Page is marked free, but is not on the free list.
		 */
		status = read_page_from_cache(currVol, currLbn, &pCurrPage);
		if (SUCCESS != status) {
		    return status;
		} 
		if (currPageNum != headPageNum) {
		    status = read_page_from_cache(headVol, headLbn, &pHeadPage);
		    if (SUCCESS != status) {
			return status;
		    }
		} else {
		    pHeadPage = pCurrPage;
		}
		    
		bmtPage = (bsMPgT *)pCurrPage->pageBuf;
		headBmtPage = (bsMPgT *)pHeadPage->pageBuf;

		/*
		 * We are looking for mcell 0 of the head page.
		 */
		pMcell  = &(headBmtPage->bsMCA[0]);
		pRecord = (bsMRT *)pMcell->bsMR0;

		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		    if (BSR_MCELL_FREE_LIST == pRecord->type) {
			headRecord = (bsMcellFreeListT *)((char *)pRecord + 
							  sizeof(bsMRT));
			bmtPage->nextFreePg = headRecord->headPg;
			headRecord->headPg = currPageNum;
			break;
		    }
		    /*
		     * Set pRecord to next record  of pMcell.
		     */
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, 
						 sizeof(int))); 
		} /* end mcell record loop */

		if (currPageNum != headPageNum) {
		    status = release_page_to_cache(headVol, headLbn, 
						   pHeadPage, TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		} else {
		    pHeadPage = NULL;
		}

		status = add_fixed_message(&(pCurrPage->messages),
					   BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_369, 
						   "Added page %d to free BMT page list.\n"),
					   currPageNum);
		if (SUCCESS != status) {
		    return status;
		}

		status = release_page_to_cache(currVol, currLbn,
						   pCurrPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    } /* end if page not on free list */
	} /* end for page loop */
    } /* end for extent loop */

    return SUCCESS;
} /* end correct_free_mcell_list */


/*
 * Function Name: clear_deferred_delete_list (3.64)
 *
 * Description: The deferred delete list contains mcells for files (or
 *		parts of files) which are to be deleted, but for one
 *		reason or another can't be done at that time. Normally
 *		at boot time the kernel will process this list and
 *		remove these mcells, but as we will be making changes
 *		to the BMT and mcells this could change one of our
 *		corrections. So instead we are going to walk the
 *		deferred delete list and mark the mcell as not in use.
 *
 * Input parameters:
 *     domain: The domain to clear the deferred delete list on.
 *
 * Output parameters:N/A
 *
 * Returns: SUCCESS, FIXED, FAILURE, NO_MEMORY
 * 
 */
int clear_deferred_delete_list(domainT *domain)
{
    char *funcName = "clear_deferred_delete_list";
    int	 tagNum;
    int  seqNum;
    int	 setTagNum;
    int  setSeqNum;
    int	 status;
    int	 modified;
    int  ddlModified;
    int	 volNum;
    int	 dmnVersion;
    int	 bmtHeadPage;
    int	 currVol;
    LBNT currLbn;
    int	 numExtents;
    LBNT lbn;
    int	 extentSize;
    pagetoLBNT *pExtents;
    volumeT    *volumeArray;
    volumeT    *volume;
    pageT      *pCurrPage; /* Pointer to current page. */
    bsMPgT     *bmtPage;   /* The current BMT page we are working on */
    bsMCT      *pMcell;	   /* The current BMT mcell we are working on */
    bsMRT      *pRecord;   /* Pointer to current mcell record. */
    delLinkRT  *delLink;
    bsXtntRT   *pXtnt;
    bsBfAttrT  *pBSRAttr;
    mcellT     currMcell;
    mcellT     nextDelMcell;
    mcellT     nextMcell;
    mcellT     chainMcell;
 
    /*
     * Init variables.
     */
    dmnVersion	= domain->dmnVersion;
    volumeArray = domain->volumes;
    ddlModified = FALSE;

    if (3 == dmnVersion) {
	bmtHeadPage = 1;
    } else {
	bmtHeadPage = 0;
    }

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	/* 
	 * Check to see if the current volume has a valid file descriptor.
	 */
	if (-1 == volumeArray[volNum].volFD) {
	    continue;
	}

	/*
	 * Setup variables.
	 */
	volume	   = &(volumeArray[volNum]);
	pExtents   = volume->volRbmt->bmtLBN;
	numExtents = volume->volRbmt->bmtLBNSize;
	modified   = FALSE;

	/*
	 * Open the header mcell to get the start of the chain.
	 */
	currMcell.vol  = pExtents[0].volume;
	currMcell.page = bmtHeadPage;
	currMcell.cell = 0;

	status = convert_page_to_lbn(pExtents, numExtents, currMcell.page,
				     &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	}
	bmtPage	 = (bsMPgT *)pCurrPage->pageBuf;
	pMcell	 = &(bmtPage->bsMCA[currMcell.cell]);
	pRecord	 = (bsMRT *)pMcell->bsMR0;

	/*
	 * Initial the next DDL pointer.
	 */
	nextDelMcell.vol  = 0;
	nextDelMcell.page = 0;
	nextDelMcell.cell = 0;

	/*
	 * Now loop through all records in this mcell.
	 */
	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    if (BSR_DEF_DEL_MCELL_LIST == pRecord->type) {
		delLink = (delLinkRT *)((char *)pRecord + sizeof(bsMRT));

		/*
		 * Save the next record.
		 */
		nextDelMcell.page = delLink->nextMCId.page;
		nextDelMcell.cell = delLink->nextMCId.cell;

		if ((0 == nextDelMcell.page) && (0 == nextDelMcell.cell)) {
		    /*
		     * empty DDL
		     */
		    nextDelMcell.vol = 0;
		    
		    if ((0 == delLink->prevMCId.page) &&
			(0 == delLink->prevMCId.cell)) {
			/*
			 * No reason to modify this record.
			 */
			break;
		    }
		} else {
		    nextDelMcell.vol = currMcell.vol;
		}

		/*
		 * Set head of deferred delete list to represent an 
		 * empty list.
		 */
		delLink->nextMCId.page = 0;
		delLink->nextMCId.cell = 0;
		delLink->prevMCId.page = 0;
		delLink->prevMCId.cell = 0;

		status = add_fixed_message(&(pCurrPage->messages), tag,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_53, 
						   "Cleared the deferred delete list.\n"));
		if (SUCCESS != status) {
		    return status;
		}
		modified = TRUE;
		ddlModified = TRUE;
		break;
	    } /* end if DDL record */
	    /*
	     * Set pRecord to next record  of pMcell.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end loop through record */

	/*
	 * Release the page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrPage,
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	modified = FALSE;

	/*
	 * Loop on the DDL
	 */
	while (0 != nextDelMcell.vol) {
	    /*
	     * Set curr to the first primary in DDL.
	     */
	    currMcell.vol  = nextDelMcell.vol;
	    currMcell.page = nextDelMcell.page;
	    currMcell.cell = nextDelMcell.cell;

	    chainMcell.vol  = 0;
	    chainMcell.page = 0;
	    chainMcell.cell = 0;

	    /*
	     * Initial the next DDL pointer.
	     */
	    nextDelMcell.vol  = 0;
	    nextDelMcell.page = 0;
	    nextDelMcell.cell = 0;

	    /*
	     * Loop on all cells in this tag, free when finished.
	     */
	    while (0 != currMcell.vol) {
		/*
		 * Load current page.
		 */
		status = convert_page_to_lbn(pExtents, numExtents,
					     currMcell.page, &currLbn, 
					     &currVol);
		if (SUCCESS != status) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_909,
				     "Truncating deferred delete list, page number %d is invalid.\n"),
			     currMcell.page);
		    /*
		     * We can not continue walking the DDL, as the 
		     * page is invalid.
		     *
		     * Do NOT return failure, instead break out of loop.
		     */
		    break;
		}

		status = read_page_from_cache(currVol, currLbn, &pCurrPage);
		if (SUCCESS != status) {
		  return status;
		}
		bmtPage	 = (bsMPgT *)pCurrPage->pageBuf;
		pMcell	 = &(bmtPage->bsMCA[currMcell.cell]);
		pRecord	 = (bsMRT *)pMcell->bsMR0;

		nextMcell.cell = pMcell->nextMCId.cell;
		nextMcell.page = pMcell->nextMCId.page;
		nextMcell.vol  = pMcell->nextVdIndex;
		tagNum	  = pMcell->tag.num;
		seqNum	  = pMcell->tag.seq;
		setTagNum = pMcell->bfSetTag.num;
		setSeqNum = pMcell->bfSetTag.seq;

		/*
		 * Now loop through all records in this mcell, this mcell
		 * can be the primary or on the chain.
		 */
		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		    switch (pRecord->type) {
		      case (BSR_ATTR):
			pBSRAttr = (bsBfAttrT *)((char *)pRecord + 
						 sizeof(bsMRT));
			/*
			 * does the mcell belong to a clone, if so 
			 * mark clone out of sync.
			 */
			if (0 != pBSRAttr->cloneId) {
			    ddlCloneFileT *pDdlCloneFile;
			    /*
			     * The way this ended up on the DDL is that
			     * the fileset which this file belongs to had
			     * a clone.  The file in question has been
			     * truncated (not deleted) and these extents
			     * need to be assigned to the clone.
			     *
			     * For this release we will be marking the
			     * clone fileset out of sync, as well as the
			     * indivdual file.
			     */
			    
			    /*
			     * Problem: We do not have a tag list at this
			     * point.  So we will need to save off a list
			     * of 'tags' which we will then need to walk
			     * later to mark them out of sync.  This list
			     * will be attached to the domain as we don't
			     * have the fileset structures either.
			     */
			    pDdlCloneFile = (ddlCloneFileT *)ffd_malloc(sizeof(ddlCloneFileT));
			    if (NULL == pDdlCloneFile) {
				writemsg(SV_ERR | SV_LOG_ERR, 
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
						 "Can't allocate memory for %s.\n"),
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_371,
						 "deferred delete clone file"));
				return NO_MEMORY;
			    }
			    
			    pDdlCloneFile->filesetId.num = setTagNum;
			    pDdlCloneFile->filesetId.seq = setSeqNum;
			    pDdlCloneFile->cloneTag.num = tagNum;
			    pDdlCloneFile->cloneTag.seq = seqNum;
			    pDdlCloneFile->next = NULL;

			    /*
			     * add to ddl clone file link list.
			     */
			    pDdlCloneFile->next = domain->ddlClone;
			    domain->ddlClone = pDdlCloneFile;
			}
			break;

		      case (BSR_XTNTS):
			pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
			/*
			 * First save the info on the next DDL entry.
			 */
			nextDelMcell.page = pXtnt->delLink.nextMCId.page;
			nextDelMcell.cell = pXtnt->delLink.nextMCId.cell;

			if ((0 == nextDelMcell.page) && 
			    (0 == nextDelMcell.cell)) {
			    /*
			     * empty DDL
			     */
			    nextDelMcell.vol = 0;
			} else {
			    nextDelMcell.vol = currMcell.vol;
			}

			/*
			 * Save the info on the head of the chain.
			 */
			chainMcell.vol	= pXtnt->chainVdIndex;
			chainMcell.page = pXtnt->chainMCId.page;
			chainMcell.cell = pXtnt->chainMCId.cell;
			break;
		    
		      default:
			/*
			 * All other records can be ignored.
			 */
			break;
		    } /* end of switch */

		    /*
		     * Set pRecord to next record  of pMcell.
		     */
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, sizeof(int))); 
		} /* end of record loop */
		/*
		 * Zero out this mcell. includes freeing extents
		 */
		status = free_mcell(domain, currVol, pCurrPage, currMcell.cell);
		if (SUCCESS != status) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_130,
				     "Can't free mcell %d on volume %s page %d.\n"),
				     currMcell.cell, 
				     volumeArray[currVol].volName,
				     currMcell.page);
		    return FAILURE;
		}
		
		status = add_fixed_message(&(pCurrPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_373, 
						   "Deleted mcell (%d,%d,%d) from deferred delete list.\n"), 
					   currVol, currMcell.page, 
					   currMcell.cell);
		if (SUCCESS != status) {
		    return status;
		}
		modified = TRUE;
		ddlModified = TRUE;

		/*
		 * Release the page
		 */
		status = release_page_to_cache(currVol, currLbn, pCurrPage,
					       modified, FALSE);
		if (SUCCESS != status) {
		  return status;
		}
		modified = FALSE;
		
		/*
		 * at the end of primary, then follow chain.
		 */
		if (0 == nextMcell.vol) {
		    nextMcell.cell = chainMcell.cell;
		    nextMcell.page = chainMcell.page;
		    nextMcell.vol  = chainMcell.vol;

		    chainMcell.vol  = 0;
		    chainMcell.page = 0;
		    chainMcell.cell = 0;
		}
		
		currMcell.cell = nextMcell.cell;
		currMcell.page = nextMcell.page;
		currMcell.vol  = nextMcell.vol;
	    } /* end curr mcell loop*/
	} /* end of deferred delete list loop */
    } /* end volume loop */
    
    if (TRUE == ddlModified) {
	return FIXED;
    }

    return SUCCESS;
} /* end clear_deferred_delete_list */


/*
 * Function Name: correct_clone_files_on_ddl (3.??)
 *
 * Description: The deferred delete list contained mcells for clone files
 *              whose extents were to be moved into the clone. But for one
 * 		reason or another could not be done at the time. Normally 
 *              at boot time the kernel will process the DDL and handle 
 *              these mcells, but as we are deleting the DDL so fixfdmn 
 * 		must handle these mcells.
 *
 *		When we corrected the DDL we created a linked list of tags
 *              which we need to go back and update, after we have fixed
 *              up the BMT and tag files.
 *
 * Input parameters:
 *     domain: The domain to finish clearing the deferred delete list.
 *
 * Output parameters:N/A
 *
 * Returns: SUCCESS, FAILURE
 * 
 */
int correct_clone_files_on_ddl(domainT *domain)
{
    char *funcName = "correct_clone_files_on_ddl";
    ddlCloneFileT *next;
    ddlCloneFileT *current;
    pageT         *pBmtPage;
    bsMPgT        *bmtPage;
    volumeT       *volume;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsBfSetAttrT  *pFsAttr;
    bsBfAttrT     *pBfAttr;
    filesetT      *pFileset;
    tagNodeT      tmpTagNode;
    tagNodeT      *pTagNode;
    nodeT         *node;
    int status;
    int nextPageNum;
    int nextPageVol;
    int nextMcellNum;
    int recordFound;
    int pageNum;
    LBNT pageLBN;
    int pageVol;
    int mcellNum;
    int found;
    int modified;

    /*
     * What needs to be done to do this:
     *   1: find the clones bsBfSetAttrT record and mark its 'state' to 
     *      BFS_OD_OUT_OF_SYNC.
     *
     *   2: >IF< the clone has a primary then we need to modify its 
     * 	    BSRAttr record and set outOfSyncClone to TRUE.
     */
    current = domain->ddlClone;

    /*
     * Walk the DDL list and process each one.
     */
    while (NULL != current) {
	/*
	 * Find the fileset which this entry belongs to.
	 * FUTURE: Consider using a skiplist for filesets
	 */
	pFileset = domain->filesets;
	found = FALSE;
	while (NULL != pFileset) {
	    if ((pFileset->filesetId.dirTag.num == current->filesetId.num) &&
		(pFileset->filesetId.dirTag.seq == current->filesetId.seq)) {
		found = TRUE;
		break;
	    }
	    pFileset = pFileset->next;
	} /* end while pFileset != NULL */
	
	if (found == FALSE) {
	    /*
	     * Fileset not found.  Assume that mcell was corrupt
	     */
	    next = current->next;
	    free(current);
	    current = next;
	    continue;
	}

	/*
	 * We now have the right fileset.
	 */
	volume = &(domain->volumes[pFileset->fsPmcell.vol]);
	pageNum = pFileset->fsPmcell.page;
	mcellNum = pFileset->fsPmcell.cell;

	/*
	 * Set to pageVol so we get into the loop, it will be reset
	 * once we do the convert_page_to_lbn call.
	 */
	pageVol = pFileset->fsPmcell.vol;
	recordFound = FALSE;

	while ((TRUE == FS_IS_CLONE_OUT_SYNC(pFileset->status)) && 
	       (FALSE == recordFound) && 
	       (0 != pageVol)) {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 pageNum, &pageLBN, &pageVol);
	    if (SUCCESS != status) {
		return status;
	    }
	    status = read_page_from_cache(pageVol, pageLBN, &pBmtPage);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;

	    bmtPage = (bsMPgT *)pBmtPage->pageBuf;
	    pMcell = &(bmtPage->bsMCA[mcellNum]);
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    nextPageNum = pMcell->nextMCId.page;
	    nextPageVol = pMcell->nextVdIndex;
	    nextMcellNum = pMcell->nextMCId.cell;

	    /*
	     * Loop through the records looking for type BSR_BFS_ATTR.
	     */
	    while ((FALSE == recordFound) &&
		   (0 != pRecord->type) &&
		   (0 != pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		if (BSR_BFS_ATTR == pRecord->type) {
		    pFsAttr = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));
		    /*
		     * Set the clone out of sync.
		     */
		    status = add_fixed_message(&(pBmtPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_374, 
						       "Modified a clone fileset to mark it out of sync.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		    FS_SET_CLONE_OUT_SYNC(pFileset->status);
		    pFsAttr->state |= BFS_OD_OUT_OF_SYNC;
		    modified = TRUE;
		    recordFound = TRUE;
		} /* end if record type is BSR_BFS_ATTR */
                    
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } /* end while looping through records */

	    status = release_page_to_cache(pageVol, pageLBN, pBmtPage,
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;

	    volume = &(domain->volumes[nextPageVol]);
	    pageNum  = nextPageNum;
	    pageVol  = nextPageVol;
	    mcellNum = nextMcellNum;

	} /* end while attrs not found and page is not 0 */

	/*
	 * We have modified the fileset so it is out of sync, now
	 * we need to modify the clone tag so that it knows it is
	 * out of sync.
	 */
	tmpTagNode.tagSeq.num = current->cloneTag.num;
	pTagNode = &tmpTagNode;
		    
	status = find_node(pFileset->tagList, (void **)&pTagNode, &node);
	if (SUCCESS != status) {
	    if (NOT_FOUND == status) {
		/*
		 * tag does not exist so skip to next entry.
		 */
		next = current->next;
		free(current);
		current = next;
		continue;
	    } else {
		return status;
	    }
	} /* end if status */

	volume = &(domain->volumes[pTagNode->tagPmcell.vol]);
	pageNum = pTagNode->tagPmcell.page;
	mcellNum = pTagNode->tagPmcell.cell;

	/*
	 * Set to pageVol so we get into the loop, it will be reset
	 * once we do the convert_page_to_lbn call.
	 */
	pageVol = pTagNode->tagPmcell.vol;
	recordFound = FALSE;

	while ((FALSE == recordFound) && (0 != pageVol)) {
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 pageNum, &pageLBN, &pageVol);
	    if (SUCCESS != status) {
		return status;
	    }
	    status = read_page_from_cache(pageVol, pageLBN, &pBmtPage);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;

	    bmtPage = (bsMPgT *)pBmtPage->pageBuf;
	    pMcell = &(bmtPage->bsMCA[mcellNum]);
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    nextPageNum = pMcell->nextMCId.page;
	    nextPageVol = pMcell->nextVdIndex;
	    nextMcellNum = pMcell->nextMCId.cell;

	    /*
	     * Loop through the records looking for type BSR_ATTR.
	     */
	    while ((FALSE == recordFound) &&
		   (0 != pRecord->type) &&
		   (0 != pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		if (BSR_ATTR == pRecord->type) {
		    pBfAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));
		    /*
		     * Set the clone out of sync.
		     */
		    status = add_fixed_message(&(pBmtPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_375, 
						       "Modified a clone file to mark it out of sync.\n"));
		    if (SUCCESS != status) {
			return status;
		    }
		    pBfAttr->outOfSyncClone = TRUE;
		    modified = TRUE;
		    recordFound = TRUE;
		} /* end if record type is BSR_BFS_ATTR */
                    
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } /* end while looping through records */

	    status = release_page_to_cache(pageVol, pageLBN, pBmtPage,
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;

	    volume = &(domain->volumes[nextPageVol]);
	    pageNum  = nextPageNum;
	    pageVol  = nextPageVol;
	    mcellNum = nextMcellNum;

	} /* end while attrs not found and page is not 0 */

	next = current->next;
	free(current);
	current = next;
    } /* end of while current != NULL */
    domain->ddlClone = NULL;

    return SUCCESS;
} /* end correct_clone_files_on_ddl */


/*
 * Function Name: correct_tag_file (3.66)
 *
 * Description: This function will check and fix any tag file. The tag
 *		file contains all the primary mcells of tags in the fileset.
 *
 * Input parameters:
 *     domain: The domain to work on.
 *     filesetNum: If -2 check root tag file, else the fileset's tag file.
 *
 * Output parameters:
 *
 * Returns: SUCCESS, FIXED, CORRUPT, FAILURE, NO_MEMORY.
 * 
 */
int correct_tag_file(domainT *domain, int filesetNum)
{
    char *funcName = "correct_tag_file";
    int	 *pageArray;	/* Used to store status on each page */
    int	 fixedTagFile;
    int	 nextFreePage;	/* The number of the next free page  */
    int	 numExtents;
    int	 numPages;
    int	 extent;
    int	 currentPage;
    int	 modified;      /* Flag if modified */
    int  firstModified; /* Flag if modified */
    LBNT firstLbn; 
    LBNT currLbn;
    int	 firstVol; 
    int  currVol;
    int	 isRootTag;
    int	 maxTagPage;
    int	 status;
    int	 page;
    int	 usedTags;
    int	 tagUninitPg;
    int	 numPageInExtent;
    LBNT extentLbn;
    int  foundCorruptedHead;
    char fsName[ML_SET_NAME_SZ + 2];
    pagetoLBNT *pExtents;     /* The extents to check */
    pageT      *pFirstPage;   /* The first page we are checking */
    pageT      *pCurrentPage; /* The current page we are checking */
    filesetT   *fileset;
    bsTDirPgT  *pTagPage;
    bsTDirPgT  *pHeadTagPage;
    
    /*
     * Init variables.
     */
    modified	  = FALSE;
    firstModified = FALSE;
    fixedTagFile  = FALSE;
    nextFreePage  = 0;
    foundCorruptedHead = FALSE;

    if (-2 == filesetNum) {
	/*
	 * Set pExtents to the root tag file extents.
	 */
	pExtents = domain->rootTagLBN;
	numExtents = domain->rootTagLBNSize;
	fileset = NULL;
	strcpy(fsName, "rootTag");
	isRootTag = TRUE;
    } else {
	/*
	 * Find the fileset we are working on.
	 * Set pExtents to the fileset's tag file extents.
	 */
	fileset = domain->filesets;
	while (NULL != fileset) {
	    /*
	     * Don't need to check Options.filesetName here.
	     */
	    if (fileset->filesetId.dirTag.num == filesetNum) {
		pExtents = fileset->tagLBN;
		numExtents = fileset->tagLBNSize;
		isRootTag = FALSE;
		break;
	    } /* end if this is the fileset we want */
	    fileset = fileset->next;
	}
	assert(NULL != fileset);
	strcpy(fsName, fileset->fsName);
    }

    maxTagPage = pExtents[numExtents].page;
    /*
     * Before we can check headers we need to know
     * which pages have been initialized.  But to do that
     * we need to check data on a page.	 Wonderful little
     * chicken and egg.
     * 
     * We know that tmUninitPg is no larger than maxTagPage and
     * is no smaller than (maxTagPage - 7).  This number is 
     * stored in tag 0 of page 0.
     */
    currLbn = pExtents[0].lbn;
    currVol = pExtents[0].volume;

    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
    if (SUCCESS != status) {
	return status;
    } 
    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
    tagUninitPg = pTagPage->tMapA[0].tmUninitPg;

    if ((tagUninitPg > maxTagPage) ||
	(tagUninitPg < (maxTagPage - 7))){
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_376,
			 "Modifying fileset %s's tagUninitPg.\n"), fsName);
	status = add_fixed_message(&(pCurrentPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_377, 
					   "Modified fileset %s's tagUninitPg from %d to %d.\n"),
				   fsName, tagUninitPg, maxTagPage);
	if (SUCCESS != status) {
	    return status;
	}
	pTagPage->tMapA[0].tmUninitPg = maxTagPage;
	tagUninitPg = maxTagPage;
	modified = TRUE;
    }
    
    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				   modified, FALSE);
    if (SUCCESS != status) {
	return status;
    }
    
    if (TRUE == modified) {
	fixedTagFile = TRUE;
	modified = FALSE;
    }

    numPages = pExtents[numExtents].page;
    
    pageArray = (int *)ffd_calloc(numPages, sizeof(int));
    if (NULL == pageArray) {
	writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			  "Can't allocate memory for %s.\n"),
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_378, 
			  "tag page array"));
	return NO_MEMORY;
    }
    bzero(pageArray, sizeof(pageArray));

    /*
     * First fix the header on all the tag file pages.
     */
    currentPage = 0;
    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute number of pages in this extent.
	 */
	numPageInExtent = (pExtents[extent + 1].page - pExtents[extent].page);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all tag file pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currentPage++) {
	    if (currentPage >= tagUninitPg) {
		/*
		 * Uninitialized page - ignore rest of pages.
		 */
		break;
	    }

	    currLbn = extentLbn + (page * BLOCKS_PER_PAGE);

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (SUCCESS != status) {
		return status;
	    } 

	    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;

	    /*
	     * Fix headers first, If we fix the records call it
	     * a second time.
	     */
	    status = correct_tag_page_header(fileset, pCurrentPage, 
					     currentPage, tagUninitPg);
	    if (SUCCESS != status) {
		if (FIXED == status) {
		    modified = TRUE;
		} else {
		    return status;
		}
	    }

	    status = correct_tag_page_records(pCurrentPage, domain, fileset, 
					      currentPage, tagUninitPg);
	    if (SUCCESS != status) {
		if (FIXED == status) {
		    /*
		     * Recall correct_tag_page_header.
		     */
		    status = correct_tag_page_header(fileset, pCurrentPage, 
						     currentPage, tagUninitPg);
		    if (SUCCESS != status) {
			if (FIXED == status) {
			    modified = TRUE;
			} else {
			    return status;
			}
		    }
		    modified = TRUE;
		} else {
		   return status;
		}
	    }

	    usedTags = (pTagPage->tpPgHdr.numAllocTMaps + 
			pTagPage->tpPgHdr.numDeadTMaps);

	    if (0 == currentPage) {
		/*
		 * tag 0 on page 0 special case.
		 */
		usedTags++;
	    }

	    if (usedTags == BS_TD_TAGS_PG) {
		/*
		 * This is a full page.
		 */
		pageArray[currentPage] = TAGPAGE_FULL;
	    } else {
		pageArray[currentPage] = TAGPAGE_FREE;
	    }

	    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    if (TRUE == modified) {
		fixedTagFile = TRUE;
		modified = FALSE;
	    }
	} /* end of for page loop */
    } /* end of for extent loop */

    /*
     * The next loop is to check the free page chain.
     */
    firstLbn = pExtents[0].lbn;
    firstVol = pExtents[0].volume;

    status = read_page_from_cache(firstVol, firstLbn, &pFirstPage);
    if (SUCCESS != status) {
	return status;
    } 

    /*
     * WARNING: the Free list pages are all 'off by 1'.
     */
    pHeadTagPage = (bsTDirPgT *)pFirstPage->pageBuf;
    nextFreePage = pHeadTagPage->tMapA[0].tmFreeListHead - 1;

    if ((nextFreePage < -1) || (nextFreePage > maxTagPage)) {
	/*
	 * Out of bounds so set head tmFreeListHead to 0.
	 */
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_379,
			 "Clearing fileset %s's free tag page list head, out of bounds.\n"),
		 fsName);
	status = add_fixed_message(&(pFirstPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_380, 
					   "Modified fileset %s's free tag page list, tmFreeListHead was out of bounds.\n"),
				   fsName);	
	if (SUCCESS != status) {
	    return status;
	}
	pHeadTagPage->tMapA[0].tmFreeListHead = 0;
	nextFreePage = -1;
	foundCorruptedHead = TRUE;
	firstModified = TRUE;
    }

    /*
     * Now loop until we don't have a next free page.
     */
    while (-1 != nextFreePage) {
	/*
	 * Don't want to re-read first page (page 0).
	 */
	if (nextFreePage != 0) {
	    /*
	     * Find the next page in the chain.
	     */
	    status = convert_page_to_lbn(pExtents, numExtents,
					 nextFreePage,
					 &currLbn, &currVol);
	    if (SUCCESS != status) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_381,
				 "Can't convert tag page %d to LBN.\n"), 
			 nextFreePage);
		return FAILURE;
	    }

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (SUCCESS != status) {
		return status;
	    } 
	} else {
	    pCurrentPage = pFirstPage;
	}/* end if nextFreePage */

	pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
	currentPage = pTagPage->tpCurrPage;

	/*
	 * If we find any problems in the free list we just reset the
	 * head of the list, and reset the entire free page list
	 */
	if (TAGPAGE_FULL == pageArray[currentPage]) {
	    /*	
	     * We have a Full page on the free list, set the head of 	
	     * the list to 0, so we will need to redo the entire list.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_382,
			     "Clearing fileset %s's free tag page list head, tag page %d is full page but on list.\n"),
		     fsName, currentPage );
	    status = add_fixed_message(&(pFirstPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_383, 
					       "Modified fileset %s's free tag page list, found a full page on free list.\n"),
				       fsName);
	    if (SUCCESS != status) {
		return status;
	    }

	    pHeadTagPage->tMapA[0].tmFreeListHead = 0;
	    firstModified = TRUE;
	    pTagPage->tpNextFreePage = 0;
	    modified = TRUE;
	    foundCorruptedHead = TRUE;
	} else if (TAGPAGE_USED == pageArray[currentPage]) {
	    /*
	     * We have a loop in the chain, terminate loop.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_384, 
			     "Clearing fileset %s's free tag page list, loop in list.\n"),
		     fsName);
	    status = add_fixed_message(&(pCurrentPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_385, 
					       "Modified fileset %s's free tag page list, found loop in the free list.\n"),
				       fsName);
	    if (SUCCESS != status) {
		return status;
	    }

	    pHeadTagPage->tMapA[0].tmFreeListHead = 0;
	    firstModified = TRUE;
	    pTagPage->tpNextFreePage = 0;
	    modified = TRUE;
	    foundCorruptedHead = TRUE;

	} else {
	    /*
	     * Page is correct - Mark in-memory array that it is used.
	     */
	    pageArray[currentPage] = TAGPAGE_USED;
	} /* end if TAGPAGE */

	/*
	 * Now we need to get the next free page.
	 */
	if (0 == pTagPage->tpNextFreePage) {
	    /*
	     * End of the list.
	     */
	    nextFreePage = -1;	      
	} else {
	    nextFreePage = pTagPage->tpNextFreePage - 1;
	}

	/*
	 * Free page if we are not on the first page.
	 */
	if (pFirstPage != pCurrentPage) {
	    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    if (TRUE == modified) {
		fixedTagFile = TRUE;
		modified = FALSE;
	    }
	} else {
	    pCurrentPage = NULL;
	    modified = FALSE;
	}
    } /* end while loop */

    /*
     * If we reset head to 0, we need to recreate the array or
     * all pages we had found on the free list will now be lost.
     */
    if (TRUE == foundCorruptedHead) {
	for (currentPage = 0; currentPage < numPages; currentPage++) {
	    if (TAGPAGE_USED == pageArray[currentPage]) {
		pageArray[currentPage] = TAGPAGE_FREE;
	    }
	}
    } /* end if tmFreeListHead reset */

    /*
     * Loop through all the pages, making sure pages are on the free list.
     */
    currentPage = 0;
    for (extent = 0; extent < numExtents; extent++) {
	int numPageInExtent;
	LBNT extentLbn;

	/*
	 * Compute number of pages in this extent.
	 */
	numPageInExtent = pExtents[extent+1].page - pExtents[extent].page;
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all tag file pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currentPage++) {
	    if (TAGPAGE_FREE != pageArray[currentPage]) {
		/*
		 * Skip FULL and USED pages on list.
		 */
		continue;
	    }

	    if (currentPage >= tagUninitPg) {
		/*
		 * Uninitialized page - Ignore it.
		 */
		continue;
	    }

	    if (0 == currentPage) {
		/*
		 * On the first page don't read it twice.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_386, 
				 "Adding fileset %s's tag page %d to free list.\n"),
			 fsName, currentPage);

		status = add_fixed_message(&(pFirstPage->messages),
					   tagFile,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_387, 
						   "Modified fileset %s's tag page, added page to free list.\n"),
					   fsName);
		if (SUCCESS != status) {
		    return status;
		}

		pHeadTagPage->tpNextFreePage = pHeadTagPage->tMapA[0].tmFreeListHead;
		pHeadTagPage->tMapA[0].tmFreeListHead = pHeadTagPage->tpCurrPage + 1; /* WARNING: Off by 1 */
		firstModified = TRUE;
	    } else {
		/*
		 * Compute the LBN of the page we wish to read.
		 */
		currLbn = extentLbn + (page * BLOCKS_PER_PAGE);

		status = read_page_from_cache(currVol, currLbn, 
					      &pCurrentPage);
		if (SUCCESS != status) {
		    return status;
		} 
		pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
		    
		/*
		 * Add this page to the HEAD of the free list.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_386, 
				 "Adding fileset %s's tag page %d to free list.\n"),
			 fsName, currentPage);
		status = add_fixed_message(&(pCurrentPage->messages), tagFile,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_387,
						   "Modified fileset %s's tag page, added page to free list.\n"),
					   fsName);
		if (SUCCESS != status) {
		    return status;
		}

		pTagPage->tpNextFreePage = pHeadTagPage->tMapA[0].tmFreeListHead;
		modified = TRUE;
		pHeadTagPage->tMapA[0].tmFreeListHead = pTagPage->tpCurrPage + 1; /* WARNING: Off by 1 */
		firstModified = TRUE;

		status = release_page_to_cache(currVol, currLbn, 
					       pCurrentPage, modified,
					       FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		if (TRUE == modified) {
		    fixedTagFile = TRUE;
		    modified = FALSE;
		}
	    } /* end if first page */
	} /* end for page loop */
    } /* end for extent loop */

    /*
     * Released the First page.
     */
    if (NULL != pFirstPage) {
	status = release_page_to_cache(firstVol, firstLbn, pFirstPage, 
				       firstModified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == firstModified) {
	    fixedTagFile = TRUE;
	    firstModified = FALSE;
	}
    }

    free(pageArray);

    if (TRUE == fixedTagFile) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_tag_file */


/*
 * Function Name: correct_tag_page_header (3.66)
 *
 * Description: This function will check and fix a tag page
 *		header. This is used for both root tag files and
 *		regular tag files.
 *
 * Input parameters:
 *     fileset: NULL if rootTagfile - the fileset this tag page belongs to.
 *     page: The page to check the header on.
 *     pageNumber: The page number in the tag file.
 *     tagUninitPg: Starting point of uninitialized pages.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FIXED.
 * 
 */
int correct_tag_page_header(filesetT *fileset, pageT *pPage, 
			    int pageNumber, int tagUninitPg)
{
    char *funcName = "correct_tag_page_header";
    int  status;
    int	 modified;   /* Flag to check if page has been modified */
    int	 numAlloced; /* Number of tags allocated */
    int	 numDead;    /* Number of tags which are dead */
    int	 numFree;    /* Number of tags which are free */
    int	 counter;
    char fsName[ML_SET_NAME_SZ + 2];
    bsTDirPgT  *pTagPage;
    pagetoLBNT *pExtents;     /* The extents to check */
    int  maxTagPage;
    int  numExtents;

    /*
     * Init variables.
     */
    modified = FALSE;
    numAlloced	 = 0;
    numDead	 = 0;
    numFree	 = 0;
    pTagPage = (bsTDirPgT *)pPage->pageBuf;

    if (pageNumber >= tagUninitPg) {
	/*
	 * Uninitialized page - ignore it.
	 */
	return SUCCESS;
    }

    if (NULL != fileset) {
	strcpy(fsName, fileset->fsName);
	pExtents = fileset->tagLBN;
	numExtents = fileset->tagLBNSize;
	maxTagPage = pExtents[numExtents].page;
    } else { 
	strcpy(fsName, "rootTag");
    }

    if (pageNumber != pTagPage->tpCurrPage) {
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_388, 
					   "Modified fileset %s's tag page number from %d to %d.\n"),
				   fsName, pTagPage->tpCurrPage, 
				   pageNumber);
	if (SUCCESS != status) {
	    return status;
	}

	pTagPage->tpCurrPage = pageNumber;
	modified = TRUE;
    }

    for (counter = 0 ; counter < BS_TD_TAGS_PG; counter++) {
   
	if (0 != (pTagPage->tMapA[counter].tmSeqNo & BS_TD_IN_USE)) {
	    numAlloced++;
	} else if (BS_TD_DEAD_SLOT == pTagPage->tMapA[counter].tmSeqNo) {
	    numDead++;
	} else {
	    numFree++;
	}
    } /* end for counter loop */

    if (0 == pageNumber) {
	/*
	 * Special Case - This tag is used for free list head
	 */
	numFree--;
    }
    
    if (pTagPage->tpNumAllocTMaps != numAlloced) {
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_389, 
					   "Modified fileset %s's tag page header numAllocTmaps from %d to %d.\n"),
				   fsName, pTagPage->tpNumAllocTMaps,
				   numAlloced);
	if (SUCCESS != status) {
	    return status;
	}

	pTagPage->tpNumAllocTMaps = numAlloced;
	modified = TRUE;
    } /* end if numAlloced */

    if (pTagPage->tpNumDeadTMaps != numDead) {
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_390, 
					   "Modified fileset %s's tag page header numDeadTmaps from %d to %d.\n"),
				   fsName, pTagPage->tpNumDeadTMaps, 
				   numDead);
	if (SUCCESS != status) {
	    return status;
	}

	pTagPage->tpNumDeadTMaps = numDead;
	modified = TRUE;
    }

    /*
     * Future: figure a way to compute maxTagPage for rootTagFile
     */
    if (NULL != fileset) {
	if ((pTagPage->tpNextFreePage < 0) ||
	    (pTagPage->tpNextFreePage > maxTagPage)) {

	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_391, 
					       "Modified fileset %s's tag page header nextFreePage from %d to %d.\n"),
				       fsName, pTagPage->tpNextFreePage, 0);
	    if (SUCCESS != status) {
		return status;
	    }

	    pTagPage->tpNextFreePage = 0;
	    modified = TRUE;
	}
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_392, 
			 "Modifying fileset %s tag page %d's header.\n"),
		 fsName, pageNumber);
	return FIXED;
    }

    return SUCCESS;
} /* end correct_tag_page_header */


/*
 * Function Name: correct_tag_page_records (3.68)
 *
 * Description: This function will check and fix the tag page
 *		records. This is used for both root tag files and
 *		regular tag files.  tMapA is 1 based. Should not
 *		be called on uninitalized pages.
 *
 * Input parameters:
 *     page: The page to check the header on.
 *     domain: which domain.
 *     fileset: Which fileset are we working on, or NULL to indicate root tag
 *     pageNumber: Page we are working on.
 *     tagUninitPg: Start of uninitalized pages.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FIXED, FAILURE or NO_MEMORY.
 * 
 */
int correct_tag_page_records(pageT *pPage, domainT *domain, 
			     filesetT *fileset, int pageNumber, 
			     int tagUninitPg)
{
    char    *funcName = "correct_tag_page_records";
    int	    modified;	     /* Flag to check if page has been modified. */
    int	    status;
    int	    tagEntry;
    int	    page;
    int	    tagNum;
    int     seqNum;
    int	    finished;
    int	    tagArray[BS_TD_TAGS_PG]; /* Used to find loops. */
    int	    currentTMap;     /* Current tag we are working on*/
    int	    prevTMap;	     /* Previous tag we worked on. */
    int	    firstFreeTMap;   /* Head of the free tag list. */
    int	    isRootTag;
    int	    isClone;
    LBNT    currLbn;
    int	    currVol;
    nodeT   *node;
    pageT   *pCurrentPage;    /* The current BMT we are checking */
    volumeT *volume;
    bsMPgT  *bmtPage;
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    bsTDirPgT  *pTagPage;
    mcellNodeT tmpMcellNode;
    mcellNodeT *pMcellNode;
    char       fsName[ML_SET_NAME_SZ + 2];

    /*
     * Init variables.
     */
    modified = FALSE;
    pTagPage = (bsTDirPgT *)pPage->pageBuf;
    page     = pTagPage->tpCurrPage;

    if (pageNumber >= tagUninitPg) {
	/*
	 * Uninitialized page - ignore it.
	 */
	return SUCCESS;
    }

    if (NULL != fileset ) {
	strcpy(fsName, fileset->fsName);
	isRootTag = FALSE;
	if (FS_IS_CLONE(fileset->status)) {
	    isClone = TRUE;
	} else {
	    isClone = FALSE;
	}
    } else { 
	strcpy(fsName, "rootTag");
	isRootTag = TRUE;
	isClone	  = FALSE;
    }

    if ((0 == page) &&
	(0 != (pTagPage->tMapA[0].tmSeqNo & BS_TD_IN_USE))) {
	/*
	 * Tag 0 is marked in use.  This is never valid, so we probably
	 * have an uninitialized page 0 of the tagfile.  Clear the inuse
	 * bit so that the next check doesn't think tag 0 is in use.
	 */
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_912,
					   "Clearing fileset %s's free tag page list head, the IN_USE bit must never be set.\n"),
				   fsName);
	if (SUCCESS != status) {
	    return status;
	}
	pTagPage->tMapA[0].tmSeqNo =
	    ((pTagPage->tMapA[0].tmSeqNo) & (~BS_TD_IN_USE));
	modified = TRUE;
    }	    

    for (tagEntry = 0 ; tagEntry < BS_TD_TAGS_PG; tagEntry++) {
	tagNum = (page * BS_TD_TAGS_PG) + tagEntry;

	if (0 != (pTagPage->tMapA[tagEntry].tmSeqNo & BS_TD_IN_USE)) {
	    /*
	     * Find this mcell in the skiplist.
	     */
	    tmpMcellNode.mcellId.vol  = pTagPage->tMapA[tagEntry].tmVdIndex;
	    tmpMcellNode.mcellId.page = pTagPage->tMapA[tagEntry].tmBfMCId.page;
	    tmpMcellNode.mcellId.cell = pTagPage->tMapA[tagEntry].tmBfMCId.cell;
	    pMcellNode = &tmpMcellNode;

	    status = find_node(domain->mcellList, (void **)&pMcellNode, &node);
	    if (NOT_FOUND == status) {
		/*
		 * Active Tag has invalid primary mcell, Add tag
		 * to free list.
		 */
		status = add_fixed_message(&(pPage->messages), tagFile,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_393, 
						   "Deleting fileset %s's tag %d, primary mcell (%d,%d,%d) was not found.\n"),
					   fsName, tagNum, 
					   tmpMcellNode.mcellId.vol, 
					   tmpMcellNode.mcellId.page, 
					   tmpMcellNode.mcellId.cell);
		if (SUCCESS != status) {
		    return status;
		}

		pTagPage->tMapA[tagEntry].tmSeqNo &= (uint16T)~BS_TD_IN_USE;
		pTagPage->tMapA[tagEntry].tmVdIndex = 0;
		pTagPage->tMapA[tagEntry].tmNextMap = pTagPage->tpNextFreeMap;
		pTagPage->tpNextFreeMap = tagEntry + 1; /* WARNING: Off by 1 */
		pTagPage->tpNumAllocTMaps--; /* Decrement allocated maps */
		modified = TRUE;
		tagArray[tagEntry] = TAG_FREE;
	    } else  if (SUCCESS == status) {
		tagArray[tagEntry] = TAG_USED;
	    } else {
		return status;
	    }
	} else if (BS_TD_DEAD_SLOT == pTagPage->tMapA[tagEntry].tmSeqNo) {
	    /*
	     * Dead tag : set to USED
	     */
	    tagArray[tagEntry] = TAG_USED;
	} else {
	    /*
	     * Tag is free
	     */
	    tagArray[tagEntry] = TAG_FREE;
	}
    } /* end for tagEntry loop */

    /*
     * Special Case for tag '0'.
     */
    if (0 == page) {
	tagArray[0] = TAG_USED;
    }

    firstFreeTMap = pTagPage->tpNextFreeMap;
    currentTMap	  = firstFreeTMap;

    /*
     * Validate the header's first free map.
     *
     * Need to check greater than 1022, because of off by one.
     */
    if ((currentTMap > 1022) || (currentTMap < 0)){
	/*
	 * Invalid value so truncate the chain.
	 */
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_394, 
					   "Modified fileset %s's tpNextFreeMap from %d to %d.\n"),
				   fsName, currentTMap, 0);
	if (SUCCESS != status) {
	    return status;
	}

	pTagPage->tpNextFreeMap = 0;
	modified = TRUE;
	currentTMap =  0;
    }
    currentTMap--; /* off by one */
    prevTMap	  = currentTMap;

    /*
     * Loop on the free chain looking for invalid entries or loops.
     * End of loop means tmNextMap == 0.
     * NOTE: tpNextFreeMap is indexed 1 to 1022, while array is 0 to 1021.
     */
    while (-1 != currentTMap) {
	if ((currentTMap > 1021) || (currentTMap < 0)){
	    /*
	     *	Invalid value so truncate the chain.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_395, 
					       "Modified fileset %s's tMap %d on page %d, truncating the next tMap chain.\n"), 
				       fsName, currentTMap, page);
	    if (SUCCESS != status) {
		return status;
	    }

	    pTagPage->tMapA[prevTMap].tmNextMap = 0;
	    modified = TRUE;
	    currentTMap = -1;
	} else if (TAG_FREE == tagArray[currentTMap]) {
	    /*
	     * We have a free tag (Valid case),  mark on free list.
	     */
	    tagArray[currentTMap] = TAG_FREE_LIST;
	    prevTMap = currentTMap;
	    currentTMap = pTagPage->tMapA[currentTMap].tmNextMap;
	    currentTMap--;
	} else {
	    /*
	     * We have an Loop or an active or dead tag on free list.
	     * So truncate the chain.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_395, 
					       "Modified fileset %s's tMap %d on page %d, truncating the next tMap chain.\n"), 
				       fsName, currentTMap, page);
	    if (SUCCESS != status) {
		return status;
	    }

	    if ((firstFreeTMap - 1) == currentTMap) {
		/*
		 * Head of list
		 */
		pTagPage->tpNextFreeMap = 0;
	    } else {
		pTagPage->tMapA[prevTMap].tmNextMap = 0;
	    }
	    modified = TRUE;
	    currentTMap = -1;
	}
    } /* end while loop */

    /*
     * Loop on all entries looking for free tags not on free chain.
     */
    for (tagEntry = 0 ; tagEntry < BS_TD_TAGS_PG; tagEntry++) {
	if (TAG_FREE == tagArray[tagEntry]) {
	    /*
	     * Free tag not on list, add to head.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_396, 
					       "Modified fileset %s's tMap %d on page %d, added to free list.\n"),
				       fsName, tagEntry, page);
	    if (SUCCESS != status) {
		return status;
	    }

	    pTagPage->tMapA[tagEntry].tmNextMap = pTagPage->tpNextFreeMap;
	    pTagPage->tpNextFreeMap = tagEntry + 1; /* WARNING: Off by 1 */
	    modified = TRUE;
	}
    } /* end for tagEntry loop */

    /*
     * Loop on all entries in tMapA, look for active tags
     */
    for (tagEntry = 0 ; tagEntry < BS_TD_TAGS_PG; tagEntry++) {
	tagNum	 = (page * BS_TD_TAGS_PG) + tagEntry;
	if (0 == (pTagPage->tMapA[tagEntry].tmSeqNo & BS_TD_IN_USE)) {
	    continue;
	}
	/*
	 * Find this mcell in the skiplist.
	 */
	tmpMcellNode.mcellId.vol  = pTagPage->tMapA[tagEntry].tmVdIndex;
	tmpMcellNode.mcellId.page = pTagPage->tMapA[tagEntry].tmBfMCId.page;
	tmpMcellNode.mcellId.cell = pTagPage->tMapA[tagEntry].tmBfMCId.cell;
	pMcellNode = &tmpMcellNode;

	status = find_node(domain->mcellList, (void **)&pMcellNode, 
			   &node);
	if (SUCCESS != status) {
	    /*
	     * This should never fail, as we all ready know it exists.
	     */
	    return status;
	}

	seqNum = pTagPage->tMapA[tagEntry].tmSeqNo;

	if ((MC_IS_FOUND_PTR(pMcellNode->status) &&
	     (FALSE == isClone))) {
	    /*
	     * We have two tags pointing to same primary mcell, 
	     * delete the second one by adding to free list.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_397, 
					       "Deleted fileset %s's tag %d, tag's primary mcell (%d,%d,%d) already in use.\n"),
				       fsName, tagNum,
				       pMcellNode->mcellId.vol,
				       pMcellNode->mcellId.page, 
				       pMcellNode->mcellId.cell);
	    if (SUCCESS != status) {
		return status;
	    }

	    pTagPage->tMapA[tagEntry].tmSeqNo &= (uint16T)~BS_TD_IN_USE;
	    pTagPage->tMapA[tagEntry].tmVdIndex = 0;
	    pTagPage->tMapA[tagEntry].tmNextMap = pTagPage->tpNextFreeMap;
	    pTagPage->tpNextFreeMap = tagEntry + 1; /* WARNING: Off by 1 */
	    pTagPage->tpNumAllocTMaps--; /* Decrement allocated maps */
	    modified = TRUE;
	} else {
	    tagNodeT *pTagNode;

	    /*
	     * We found a mcell which does not have an entry in the
	     * in the tag skip list.  We have seen corruptions where
	     * more than one tag points to the same primary.  So
	     * we need to check the mcell's tag.seq
	     *
	     * If different zero out the tag.
	     */
	    if ((pMcellNode->tagSeq.num != tagNum) ||
		(pMcellNode->tagSeq.seq != seqNum)) {
		/*
		 * The primary mcell has a different tag number.
		 */
		status = add_fixed_message(&(pPage->messages), tagFile,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_398, 
						   "Deleted fileset %s's tag %d.%x, the tag does not match the primary mcell (%d,%d,%d)'s tag %d.%x.\n"),
					   fsName, tagNum, seqNum,
					   pMcellNode->mcellId.vol,
					   pMcellNode->mcellId.page,
					   pMcellNode->mcellId.cell,
					   pMcellNode->tagSeq.num,
					   pMcellNode->tagSeq.seq);
		if (SUCCESS != status) {
		    return status;
		}

		pTagPage->tMapA[tagEntry].tmSeqNo &= (uint16T)~BS_TD_IN_USE;
		pTagPage->tMapA[tagEntry].tmVdIndex = 0;
		pTagPage->tMapA[tagEntry].tmNextMap = pTagPage->tpNextFreeMap;
		pTagPage->tpNextFreeMap = tagEntry + 1; /* WARNING: Off by 1 */
		pTagPage->tpNumAllocTMaps--; /* Decrement allocated maps */
		modified = TRUE;
	    } else if (TRUE == isRootTag) {
		/*
		 * Root fileset
		 */
		MC_SET_FOUND_PTR(pMcellNode->status);
	    } else {
		/*
		 * We only create this for non root filesets
		 */
		pTagNode = (tagNodeT *)ffd_malloc(sizeof(tagNodeT));
		if (NULL == pTagNode) {
		    writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
					 "Can't allocate memory for %s.\n"),
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_403,
					 "tag node"));
		    return NO_MEMORY;
		}

		pTagNode->tagSeq.num = tagNum;
		pTagNode->tagSeq.seq = pTagPage->tMapA[tagEntry].tmSeqNo;
		pTagNode->tagPmcell.vol = pTagPage->tMapA[tagEntry].tmVdIndex;
		pTagNode->tagPmcell.page = pTagPage->tMapA[tagEntry].tmBfMCId.page;
		pTagNode->tagPmcell.cell = pTagPage->tMapA[tagEntry].tmBfMCId.cell;
		pTagNode->status = 0;

		/*
		 * Will be loaded later with valid values.
		 */
		pTagNode->fileType = 0;
		pTagNode->size = 0;
		pTagNode->pagesFound = 0;
		pTagNode->fragSize = 0;
		pTagNode->fragPage = 0;
		pTagNode->fragSlot = 0;
		pTagNode->numLinks = 0;
		pTagNode->numLinksFound = 0;
		pTagNode->parentTagNum.num = 0;
		pTagNode->parentTagNum.seq = 0;
		pTagNode->dirIndexTagNum.num = 0;
		pTagNode->dirIndexTagNum.seq = 0;

		status = insert_node(fileset->tagList, pTagNode);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * Don't mark the mcell node as found for clones.
		 * as this will cause duplicates when we get to
		 * the parent fileset.
		 */
		if (FALSE == isClone) {
			MC_SET_FOUND_PTR(pMcellNode->status);
		} else {
		    /*
		     * When a clone file is changed a NEW primary mcell
		     * is created.	This new mcell's setTag matches 
		     * the clone's filesetId, so we need to mark it
		     * as found.
		     */
		    if ((pMcellNode->setTag.num == fileset->filesetId.dirTag.num) &&
			(pMcellNode->setTag.seq == fileset->filesetId.dirTag.seq)) {
			MC_SET_FOUND_PTR(pMcellNode->status);
		    } /* end if 'new' primary mcell */
		} /* if this is a clone */
	    } /* end if tag/seq off */
	} /* end if not found */
    } /* end looping through tags on page */

    if (TRUE == isRootTag) {
	/*
	 * rootTag file.
	 */
	status = save_fileset_info(domain, pPage);
	if (SUCCESS != status) {
	    return status;
	}
    }

    if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_400,
			 "Modifying fileset %s tag page %d's records.\n"),
		 fsName, pageNumber);
	return FIXED;
    }
    return SUCCESS;
} /* correct_tag_page_records */


/*
 * Function Name: correct_tag_to_mcell (3.??)
 *
 * Description: This function will modify a tag entry to point to the
 *              passed in primary mcell.  It will need to update the
 *              tag file header as well.  
 *
 * Input parameters:
 *     fileset: The fileset we are working on.
 *     tagNum: The tagNum to update.
 *     seqNum: The seqNum of the tag.
 *     volNum: The primary mcell's volume number.
 *     pageNum: The primary mcell's page number.
 *     mcellNum: The primary mcell's mcell number.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE.
 * 
 */
int correct_tag_to_mcell(filesetT *fileset, int tagNum, int seqNum,
                         int volNum, int pageNum, int mcellNum)
{
    char    *funcName = "correct_tag_to_mcell";
    int     status;
    int     modified;        /* Flag to check if page has been modified. */
    int     numExtents;
    int     tagPage;
    LBNT     currLbn;
    int     currVol;
    int     tagPageNum;
    int     tagEntryOnPage;
    int     currentTMap;     /* Current tag we are working on*/
    int     prevTMap;        /* Previous tag we worked on. */
    int     firstFreeTMap;   /* Head of the free tag list. */
    int     freePage;        /* Used to store the nextFreePage */
    int     tagsUsed;
    pagetoLBNT *pExtents;     /* The extents to check */
    pageT      *pCurrentPage; /* The current page we are checking */
    bsTDirPgT  *pTagPage;     /* The current tag page we are working on */
    bsTMapT    *tagEntry;     /* The current tag on the tag page */
    bsTMapT    *prevTagEntry; /* The previous tag on the tag page */
    tagNodeT   *pTagNode;

    /*
     * Set pExtents to the fileset's tag file extents.
     */
    pExtents   = fileset->tagLBN;
    numExtents = fileset->tagLBNSize;

    /*
     * Find the page in the tagfile where our tag's entry would be,
     * 0-1021 on 1st page, 1022-2043 on 2nd page, etc.
     */
    tagPageNum     = tagNum / BS_TD_TAGS_PG;
    tagEntryOnPage = tagNum % BS_TD_TAGS_PG;
 
    status = convert_page_to_lbn(pExtents, numExtents, tagPageNum, 
                                 &currLbn, &currVol);
    if (SUCCESS != status) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_381, 
			 "Can't convert tag page %d to LBN.\n"), 
		 tagPageNum);
        return FAILURE;
    }

    /*
     * Read the tag page
     */
    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
    if (SUCCESS != status) {
        return status;
    }
    modified = FALSE;

    /*
     * Now we have the correct tag page.  Find the correct tag
     */
    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
    tagEntry = &(pTagPage->tMapA[tagEntryOnPage]);

    /*
     * Check to see if this tag is already active.
     */
    if (0 != (tagEntry->tmSeqNo & BS_TD_IN_USE)) {
        /*
         * Where you see this most often is the same tag number but
         * with different seqNo.  As find_node uses seqNo as a
         * key, only one shows up.
         */
        status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
                                       modified, FALSE);
        if (SUCCESS != status) {
            return status;
        }
	/*
	 * No writemsg needed - calling function will print the message.
	 */
        return FAILURE;
    }

    /*
     * Now update the tag headers and tag free list
     * Note off by 1
     */
    firstFreeTMap = pTagPage->tpNextFreeMap;
    firstFreeTMap--;

    /*
     * Check to see if the head is the one we want.
     */
    if (tagEntryOnPage == firstFreeTMap) {
        pTagPage->tpNextFreeMap = tagEntry->tmNextMap;
    } else {
        currentTMap = firstFreeTMap;
        prevTMap = firstFreeTMap;

        /*
         * Loop on the free chain looking for our entry.
         * End of loop means tmNextMap == 0.
         * NOTE: tpNextFreeMap and tpNextMap are indexed 1 to 1022, 
         *       while array is 0 to 1021.
         */
        while (-1 != currentTMap) {
	    assert((currentTMap <= 1021) && (currentTMap >= 0));

            if (currentTMap == tagEntryOnPage) {
                /*
                 * Found the one we are looking for, update pointers.
                 */
                prevTagEntry = &(pTagPage->tMapA[prevTMap]);
                prevTagEntry->tmNextMap = tagEntry->tmNextMap;
                break; 
            }

            prevTMap = currentTMap;
            currentTMap = pTagPage->tMapA[currentTMap].tmNextMap;
            currentTMap--;
        } /* end of while loop*/
	assert(currentTMap != -1);
    } /* end of if/else tagNum == firstFreeTmap */

    /*
     * Increment the number of used tags.
     */
    pTagPage->tpNumAllocTMaps++;

    /*
     * Update the tag to point to this primary.
     */
    status = add_fixed_message(&(pCurrentPage->messages), tagFile,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_401, 
				       "Modified fileset %s's tag %d.%x to have primary mcell of (%d,%d,%d).\n"),
			       fileset->fsName, tagNum, seqNum, 
			       volNum, pageNum, mcellNum);
    if (SUCCESS != status) {
	return status;
    }
    tagEntry->tmVdIndex = volNum;
    tagEntry->tmBfMCId.page = pageNum;
    tagEntry->tmBfMCId.cell = mcellNum;
    tagEntry->tmSeqNo = ((uint16T)seqNum | BS_TD_IN_USE);
    modified = TRUE;

    /*
     * Need to save this pages, nextFreePage incase the
     * page needs to be removed from the freePageList chain.
     */
    freePage = pTagPage->tpNextFreePage;
    tagsUsed = pTagPage->tpNumAllocTMaps + pTagPage->tpNumDeadTMaps;
    if (0 == tagPageNum) {
	tagsUsed++;
    }

    /*
     * Release the page.
     */
    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
                                   modified, FALSE);
    if (SUCCESS != status) {
        return status;
    }
    modified = FALSE;

    /*
     * If this page is on the freeTagPage list, and is no longer
     * free we need to remove the page from the list.
     */
    if (BS_TD_TAGS_PG == tagsUsed) {
	int nextFreePage;
	int pageRemoved;

	/*
	 * Init variables.
	 */
	pageRemoved = FALSE;

	/*
	 * Need to walk the free list, to remove this page.
	 * First find the head of the free page chain.
	 */
	currLbn = pExtents[0].lbn;
	currVol = pExtents[0].volume;

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	/*
	 * WARNING: the Free list pages are all 'off by 1'.
	 */
	pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
	nextFreePage = pTagPage->tMapA[0].tmFreeListHead - 1;

	if (nextFreePage == tagPageNum) {
	    /*
	     * The head points to the page we want to remove,
	     * so just move the pointer.
	     */
	    status = add_fixed_message(&(pCurrentPage->messages), tagFile,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_402,
					       "Removed fileset %s's tag page %d from free tag page list.\n"),
				       fileset->fsName, tagPageNum);

	    if (SUCCESS != status) {
		return status;
	    }

	    pTagPage->tMapA[0].tmFreeListHead = freePage;
	    pageRemoved = TRUE;
	    modified = TRUE;
	}

	/*
	 * Release the head page.
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	modified = FALSE;

	/*
	 * Walk the list until we remove it or find the end.
	 */
	while ((-1 != nextFreePage) &&
	       (TRUE != pageRemoved)) {
	    /*
	     * Find the next page in the chain.
	     */
	    status = convert_page_to_lbn(pExtents, numExtents,
					 nextFreePage,
					 &currLbn, &currVol);
	    if (SUCCESS != status) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_381, 
				 "Can't convert tag page %d to LBN.\n"), 
			 nextFreePage);
		return FAILURE;
	    }

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (SUCCESS != status) {
		return status;
	    } 
	    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
	    nextFreePage = pTagPage->tpNextFreePage - 1;

	    if (nextFreePage == tagPageNum) {
		/*
		 * This page points to the page we want to remove,
		 * so just move the pointer.
		 */
		status = add_fixed_message(&(pCurrentPage->messages), tagFile,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_402, 
						   "Removed fileset %s's tag page %d from free tag page list.\n"),
					   fileset->fsName, tagPageNum);
		
		if (SUCCESS != status) {
		    return status;
		}
		pTagPage->tpNextFreePage = freePage;
		pageRemoved = TRUE;
		modified = TRUE;
	    }
	
	    /*
	     * Release the head page.
	     */
	    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;
	} /* end of while we walk the list. */
    } /* end if we need to remove page from free list */
    
    /*
     * Insert tag into skiplist
     */
    pTagNode = (tagNodeT *)ffd_malloc(sizeof(tagNodeT));
    if (NULL == pTagNode) {
        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_403, 
			 "tag node"));
        return NO_MEMORY;
    }

    pTagNode->tagSeq.num = tagNum;
    pTagNode->tagSeq.seq = seqNum;
    pTagNode->tagPmcell.vol = volNum;
    pTagNode->tagPmcell.page = pageNum;
    pTagNode->tagPmcell.cell = mcellNum;
    pTagNode->status = 0;

    /*
     * Will be loaded later with valid values.
     */
    pTagNode->fileType = 0;
    pTagNode->size = 0;
    pTagNode->pagesFound = 0;
    pTagNode->fragSize = 0;
    pTagNode->fragPage = 0;
    pTagNode->fragSlot = 0;
    pTagNode->numLinks = 0;
    pTagNode->numLinksFound = 0;
    pTagNode->parentTagNum.num = 0;
    pTagNode->parentTagNum.seq = 0;
    pTagNode->dirIndexTagNum.num = 0;
    pTagNode->dirIndexTagNum.seq = 0;
    
    status = insert_node(fileset->tagList, pTagNode);
    if (SUCCESS != status) {
        return status;
    }

    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_404,
		     "Modifying tag %d.%x to point to (%d,%d,%d).\n"),
	     tagNum, seqNum, volNum, pageNum, mcellNum);

    return SUCCESS;
} /* end correct_tag_to_mcell */


/* 
 * Function Name: correct_nodes (3.69) 
 *
 * Description: This routine checks the mcell and tag nodes and fixes 
 *		mcell links in the BMT. It is called after the root tag 
 *		file and the fileset tag files have been checked. It 
 *		also saves information in the tag list nodes and collects 
 *		SBM info from extent records.
 *
 * Input parameters:
 *     domain: The domain being worked on.
 *
 * Output parameters: N/A
 *
 * Returns: FIXED or SUCCESS
 *
 */
int correct_nodes (domainT *domain) 
{
    char *funcName = "correct_nodes";
    int	 status;
    int	 fixedBMT; /* Flag set if we fixed anything in the BMT. */
    int  modified;
    int	 volNum;
    int	 pageNum;
    int	 bmtXtnt;
    int	 bmtLBNSize;
    LBNT currLBN;
    int	 fsFound;
    LBNT nextLBN;
    int	 nextPageNum;
    int	 mcellNum;
    int	 extentSize;
    int	 x;
    int	 neg2;
    int  tagFound;
    int	 tagNum;
    int	 tagSeq;
    int	 isClone;
    int  odsVersion;
    char fsName[ML_SET_NAME_SZ + 2];
    nodeT         *node;
    pageT         *pPage;
    bsMPgT	  *bmtPage;
    bsMCT	  *pMcell;
    bsMRT	  *pRecord;
    bsXtntRT	  *pXtnt;
    bsXtraXtntRT  *pXtra;
    bsShadowXtntT *pShadow;
    mcellNodeT	  tmpMcellNode;
    mcellNodeT	  *pMcellNode;
    tagNodeT	  tmpTagNode;
    tagNodeT	  *pTagNode;
    filesetT	  *fileset;
    pagetoLBNT	  *bmtLBN;
    mcellT        sbmMcell;
    pagetoLBNT    extentArray[BMT_XTRA_XTNTS];

    /*
     * Init variables
     */
    fixedBMT = FALSE;
    modified = FALSE;
    pMcellNode = &tmpMcellNode;
    pTagNode = &tmpTagNode;
    odsVersion = domain->dmnVersion;
    
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	if (-1 == domain->volumes[volNum].volFD) {
	    /*
	     * Not a valid volume, continue on to next volume
	     */
	    continue;
	}

	/*
	 * Get the BMT extent array and loop through it's entries.
	 */
	pageNum = 0;
	bmtLBN = domain->volumes[volNum].volRbmt->bmtLBN;
	bmtLBNSize = domain->volumes[volNum].volRbmt->bmtLBNSize;

	for (bmtXtnt = 0; bmtXtnt < bmtLBNSize; bmtXtnt++) {
	    /* 
	     * Loop through each page of the extent. 
	     */
	    for (pageNum = bmtLBN[bmtXtnt].page;
		 pageNum < bmtLBN[bmtXtnt+1].page;
		 pageNum++) {

		if ((3 == domain->dmnVersion) && (0 == pageNum)) {
		    /*
		     * Skip BMT page 0 since it was already processed.
		     */
		    continue;
		}
		currLBN = (bmtLBN[bmtXtnt].lbn + 
			   (BLOCKS_PER_PAGE * (pageNum - bmtLBN[bmtXtnt].page)));

		/*
		 * If we are at the end of the extent, then get the next 
		 * page and LBN from the next extent, otherwise just 
		 * increment them.
		 */
		if (pageNum == bmtLBN[bmtXtnt+1].page - 1) {
		    nextLBN = bmtLBN[bmtXtnt+1].lbn;
		    nextPageNum = bmtLBN[bmtXtnt+1].page;
		} else {
		    nextLBN = currLBN + BLOCKS_PER_PAGE;
		    nextPageNum = pageNum + 1;
		}

		status = read_page_from_cache(bmtLBN[bmtXtnt].volume, 
					      currLBN, &pPage);
		if (SUCCESS != status) {
		    return status;
		} 
		bmtPage = (bsMPgT *)pPage->pageBuf;

		for (mcellNum = 0; mcellNum < BSPG_CELLS; mcellNum++) {
		    pMcell = &(bmtPage->bsMCA[mcellNum]);
		    tagNum = pMcell->tag.num;
		    tagSeq = pMcell->tag.seq;
		    sbmMcell.vol = volNum;
		    sbmMcell.page = pageNum;
		    sbmMcell.cell = mcellNum;

		    /*
		     * Check to see if any free mcells are in the mcell 
		     * node list. If so, delete the node from the list.
		     */
		    if ((0 == pMcell->tag.num) && 
			(0 == pMcell->bfSetTag.num)) {
			/*
			 * Special case ODSV3 page 1 mcell 0 and
			 *		ODSV4 page 0 mcell 0.
			 */
			if (((3 == domain->dmnVersion) && (1 == pageNum)) ||
			    ((3 != domain->dmnVersion) && (0 == pageNum))) {
			    if (0 == mcellNum) {
				/*
				 * Continue on to next mcell.
				 */
				continue;
			    }
			}

			tmpMcellNode.mcellId.vol  = volNum;
			tmpMcellNode.mcellId.page = pageNum;
			tmpMcellNode.mcellId.cell = mcellNum;
			pMcellNode = &tmpMcellNode;

			status = find_node(domain->mcellList,
					   (void **)&pMcellNode, &node);
			if (SUCCESS != status) {
			    if (NOT_FOUND == status) {
				/*
				 * Expected case, not in the list as free.
				 */
				continue;
			    } else {
				return status;
			    }
			}

			/*
			 * Should not be in the list so free it.
			 */
			writemsg(SV_DEBUG, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_405, 
					 "Deleting mcell node (%d,%d,%d).\n"),
				 volNum, pageNum, mcellNum);

			status = delete_node(domain->mcellList, 
					     (void **)&pMcellNode); 
			if (SUCCESS == status) {
			    /* If the node was found, free whatever is
			     * now pointed to. 
			     */
			    free(pMcellNode);
			} else {
			    return status;
			}
			/*
			 * Continue on to the next mcell.
			 */
			continue;
		    } /* end if mcell is free */
		    
		    /*
		     * The mcell is active, so see if we found the fileset
		     * it is supposed to be in.	  Need to handle special 
		     * case:  fileset '-2'.
		     */
		    if (-2 == pMcell->bfSetTag.num) {
			fsFound = TRUE;
			neg2	= TRUE;
			isClone = FALSE;
			strcpy(fsName, "rootTag");
		    } else {
			fileset = domain->filesets;
			fsFound = FALSE;
			neg2	= FALSE;

			/*
			 * We are NOT going to check Options.filesetName
			 * for this loop.
			 */
			while (NULL != fileset) {
			    if (pMcell->bfSetTag.num == 
				fileset->filesetId.dirTag.num) {
				fsFound = TRUE;
				break;
			    }
			    fileset = fileset->next;
			}

			/*
			 * If we didn't find the fileset then free the 
			 * mcell on the BMT page and continue looping 
			 * on mcells.
			 */
			if (FALSE == fsFound) {
			    status = free_mcell(domain, volNum, 
						pPage, mcellNum);
			    if (SUCCESS != status) {
				writemsg(SV_ERR | SV_LOG_ERR, 
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_130,
						 "Can't free mcell %d on volume %s page %d.\n"),
					 mcellNum,
					 domain->volumes[volNum].volName,
					 pageNum);
				return FAILURE;
			    }
			    
			    status = add_fixed_message(&(pPage->messages),
						       BMT,
						       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_406, 
							       "Deleted mcell (%d,%d,%d), belonged to unknown fileset.\n"), 
						       volNum, pageNum, mcellNum);
			    if (SUCCESS != status) {
				return status;
			    }

			    modified = TRUE;
			    continue;
			} 

			if (FS_IS_CLONE(fileset->status)) {
			    /*
			     * What this means is a 'modified' file
			     * in the orginal fileset, so a copy-on-write
			     * created this mcell.
			     */
			    isClone = TRUE;
			} else {
			    isClone = FALSE;
			}
			strcpy(fsName, fileset->fsName);
		    } /* end if normal fileset (else clause) */

		    /*
		     * If the mcell has a next pointer, check it
		     * to see if it's in the skip list and if we have
		     * any loops to it.
		     */
		    if (0 != pMcell->nextVdIndex) {
			tmpMcellNode.mcellId.vol = pMcell->nextVdIndex;
			tmpMcellNode.mcellId.page = pMcell->nextMCId.page;
			tmpMcellNode.mcellId.cell = pMcell->nextMCId.cell;
			pMcellNode = &tmpMcellNode;
				
			status = find_node(domain->mcellList,
					   (void **)&pMcellNode, &node);
			if ((NOT_FOUND == status) ||
			    ((SUCCESS == status) &&
			     (MC_IS_FOUND_PTR(pMcellNode->status)))) {
			    /* 
			     * If the next mcell isn't in the list or if 
			     * the chain bit is already set for it, then 
			     * truncate the chain.
			     */
			    status = add_fixed_message(&(pPage->messages),
						       BMT,
						       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_407, 
							       "Modified mcell (%d,%d,%d) next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
						       volNum, pageNum, mcellNum,
						       pMcell->nextVdIndex,
						       pMcell->nextMCId.page,
						       pMcell->nextMCId.cell,
						       0, 0, 0);
			    if (SUCCESS != status) {
				return status;
			    }

			    pMcell->nextVdIndex = 0;
			    pMcell->nextMCId.page = 0;
			    pMcell->nextMCId.cell = 0;
			    modified = TRUE;
			} else if (SUCCESS == status) {
			    /*
			     * Set the chain bit on the next mcell because
			     * this one points to it. 
			     */
			    MC_SET_FOUND_PTR(pMcellNode->status);
			} else {
			    return status;
			}
		    } /* end if mcell has a next pointer */

		    /*
		     * For active mcells, loop on the records looking for 
		     * extents to add to the SBM or file size. Also check 
		     * for tag existence for trashcans. First get the tag 
		     * node
		     */
		    if (FALSE == neg2) {
			if ((NULL != Options.filesetName) &&
			   (0 != strcmp(Options.filesetName, fileset->fsName))) {
			    /*
			     * Not the fileset we want.
			     * The tag node can not EXIST.
			     */
			    tagFound = FALSE;	
			    tmpMcellNode.mcellId.vol = volNum;
			    tmpMcellNode.mcellId.page = pageNum;
			    tmpMcellNode.mcellId.cell = mcellNum;
			    pMcellNode = &tmpMcellNode;
			    
			    /*
			     * Find the mcell to mark it found.
			     */
			    status = find_node(domain->mcellList,
					       (void **)&pMcellNode, &node);
			    if (SUCCESS != status) {
				return status;
			    }

			    MC_SET_FOUND_PTR(pMcellNode->status); 
			} else {
			    /*
			     * Working on ALL filesets or the fileset we want.
			     */
			    tmpTagNode.tagSeq.num = tagNum;
			    pTagNode = &tmpTagNode;

			    /*
			     * Find the tag which belongs to this mcell, 
			     * it will be used later.
			     */
			    status = find_node(fileset->tagList, 
					       (void **)&pTagNode, &node);
			    if (SUCCESS == status &&
				(tagSeq == pTagNode->tagSeq.seq)) {
				tagFound = TRUE;
			    } else if ((NOT_FOUND == status) ||
				       ((SUCCESS == status) &&
					(tagSeq != pTagNode->tagSeq.seq))) {
				tagFound = FALSE;
				/* 
				 * If the tag does not exist and this is
				 * the primary mcell then we will attach
				 * this mcell to the tag.  
				 */
				tmpMcellNode.mcellId.vol = volNum;
				tmpMcellNode.mcellId.page = pageNum;
				tmpMcellNode.mcellId.cell = mcellNum;
				pMcellNode = &tmpMcellNode;

				status = find_node(domain->mcellList,
						   (void **)&pMcellNode, &node);
				if (SUCCESS != status) {
				    return status;
				}

				if ((MC_IS_PRIMARY(pMcellNode->status)) &&
				    (FALSE == MC_IS_FOUND_PTR(pMcellNode->status))) {
				    /* 
				     * Need to fix tag file so that it
				     * points to this mcell.  
				     */
				    status = correct_tag_to_mcell(fileset, tagNum,
								  tagSeq, volNum,
								  pageNum, mcellNum);
				    if (SUCCESS != status) {
					if (NO_MEMORY == status) {
					    return status;
					} else {
					    writemsg(SV_DEBUG,
						     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_408, 
							     "Duplicate tag %d - different seq - Setting to missing.\n"),
						     tagNum);
					    MC_SET_MISSING_TAG(pMcellNode->status);
					}
				    } else {
					MC_SET_FOUND_PTR(pMcellNode->status); 

					/* 
					 * Last function created the
					 * tagNode now go out and find it, used later
					 */
					tmpTagNode.tagSeq.num = tagNum;
					pTagNode = &tmpTagNode;

					status = find_node(fileset->tagList, 
							   (void **)&pTagNode, &node);
					if (SUCCESS != status) {
					    return status;
					}
					tagFound = TRUE;
				    } /* end if correct_tag_to_mcell */
				} else {
				    MC_SET_MISSING_TAG(pMcellNode->status);
				} /* end if primary */
			    } else if (SUCCESS != status) {
				return status;
			    }/* end if not found */
			} /* end if single fileset/multiple fileset */
		    } /* end if false == neg2 */			

		    pRecord  = (bsMRT *)pMcell->bsMR0;
		    while ((0 != pRecord->type) &&
			   (0 != pRecord->bCnt) &&
			   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
			switch (pRecord->type) {
			  case (BSR_XTNTS):
			    pXtnt = (bsXtntRT *)((char *)pRecord +
						 sizeof(bsMRT));

			    if ((3 == odsVersion) || 
				(BSXMT_STRIPE == pXtnt->type)) {
				/*
				 * On ODSV3 fields are not initializied.
				 *
				 * Striped file fields are not initialized
				 */
			    } else {
				/*
				 * Loop through the extents in this record.
				 */
				for (x = 0; 
				     x < pXtnt->firstXtnt.xCnt && x < BMT_XTNTS;
				     x++) {
				    extentArray[x].volume = volNum;
				    extentArray[x].page = pXtnt->firstXtnt.bsXA[x].bsPage;
				    extentArray[x].lbn = pXtnt->firstXtnt.bsXA[x].vdBlk;
				    if (x < (pXtnt->firstXtnt.xCnt -1)) {
					extentSize = (pXtnt->firstXtnt.bsXA[x + 1].bsPage -
						      pXtnt->firstXtnt.bsXA[x].bsPage);
					if ((FALSE == neg2) &&
					    (TRUE == tagFound)) {
					    pTagNode->pagesFound += extentSize;
					}
				    }
				} /* end for xtnt loop */

				status = collect_sbm_info(domain, 
							  extentArray,
							  pXtnt->firstXtnt.xCnt,
							  pMcell->tag.num, 
							  pMcell->bfSetTag.num,
							  &sbmMcell, 0);
				if (SUCCESS != status) {
				    return status;
				}
			    } /* end if odsVersion not 3 */
				  
			    if (0 != pXtnt->chainVdIndex) {
				mcellNodeT tmpChainNode;
				mcellNodeT *pChainNode;
						
				tmpChainNode.mcellId.vol = pXtnt->chainVdIndex;
				tmpChainNode.mcellId.page = pXtnt->chainMCId.page;
				tmpChainNode.mcellId.cell = pXtnt->chainMCId.cell;
				pChainNode = &tmpChainNode;
						
				status = find_node(domain->mcellList,
						   (void **)&pChainNode, &node);
				if ((NOT_FOUND == status) || 
				    ((SUCCESS == status) &&
				     (MC_IS_FOUND_PTR(pChainNode->status)))){
				    /*
				     * Clear the chain pointer if we didn't
				     * find the mcell node or if the chain 
				     * bit is already set in the node.
				     */
				    status = add_fixed_message(&(pPage->messages), 
							       BMT,
							       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_471, 
								       "Modified mcell (%d,%d,%d)'s xtnt chain from (%d,%d,%d) to (%d,%d,%d).\n"),
							       volNum, pageNum, mcellNum,
							       pXtnt->chainVdIndex,
							       pXtnt->chainMCId.page,
							       pXtnt->chainMCId.cell,
							       0, 0, 0);
				    if (SUCCESS != status) {
					return status;
				    }

				    pXtnt->chainVdIndex = 0;
				    pXtnt->chainMCId.page = 0;
				    pXtnt->chainMCId.cell = 0;
				    pXtnt->firstXtnt.mcellCnt = 1;
				    modified = TRUE;
				} else if (SUCCESS == status) {
				    MC_SET_FOUND_PTR(pChainNode->status);
				} else {
				    return status;
				}/* end if status on find_node */
			    } /* end if chain pointer exists in record */
			      
			  break;

			  case (BSR_SHADOW_XTNTS):
			    pShadow = (bsShadowXtntT *)((char *)pRecord +
							sizeof(bsMRT));
			    /*
			     * Loop through the extents in this record.
			     */
			    for (x = 0; 
				 x < pShadow->xCnt && x < BMT_SHADOW_XTNTS;
				 x++) {
				extentArray[x].volume = volNum;
				extentArray[x].page = pShadow->bsXA[x].bsPage;
				extentArray[x].lbn = pShadow->bsXA[x].vdBlk;				
				if (x < pShadow->xCnt - 1) {
				    extentSize = (pShadow->bsXA[x + 1].bsPage -
						  pShadow->bsXA[x].bsPage);
				    if ((FALSE == neg2) &&
					(TRUE == tagFound)) {
					pTagNode->pagesFound += extentSize;
				    }
				}
			    } /* end for xtnt loop */

			    status = collect_sbm_info(domain, 
						      extentArray,
						      pShadow->xCnt,
						      pMcell->tag.num, 
						      pMcell->bfSetTag.num,
						      &sbmMcell, 0);
			    if (SUCCESS != status) {
				return status;
			    }
			  break;
			      
			  case (BSR_XTRA_XTNTS):
			    pXtra = (bsXtraXtntRT *)((char *)pRecord + 
						     sizeof(bsMRT));
			    /*
			     * Loop through the extents.
			     */
			    for (x = 0; 
				 x < pXtra->xCnt && x < BMT_XTRA_XTNTS;
				 x++) {
				extentArray[x].volume = volNum;
				extentArray[x].page = pXtra->bsXA[x].bsPage;
				extentArray[x].lbn = pXtra->bsXA[x].vdBlk;

				if (x < pXtra->xCnt - 1) {
				    extentSize = (pXtra->bsXA[x + 1].bsPage -
						  pXtra->bsXA[x].bsPage);
				    if ((FALSE == neg2) &&
					(TRUE == tagFound)) {
					pTagNode->pagesFound += extentSize;
				    }
				}
			    } /* end for xtnt loop */

			    status = collect_sbm_info(domain, 
						      extentArray,
						      pXtra->xCnt,
						      pMcell->tag.num, 
						      pMcell->bfSetTag.num,
						      &sbmMcell, 0);
			    if (SUCCESS != status) {
				return status;
			    }
			  break;

			  case (BMTR_FS_STAT):
			      /*
			       * The loading of information from fsstat
			       * has been moved to correct_bmt_chains.
			       */
			  break;
				    
			  case (BMTR_FS_UNDEL_DIR):
			    /*
			     * Testing shows that it is OK for this to point 
			     * to a tag which no longer exists.	  I used the
			     * following test to prove this:
			     * 1) Create 2 dirs.
			     * 2) Attach dir1 as dir2's trashcan.
			     * 3) delete dir1.
			     *
			     * This leave the undel_dir record still attached
			     * to dir2, but causes no problems.
			     */
			  break;
				
			  default:
			    /*
			     * Just ignore any non-extent based record.
			     */
			  break;
			} /* switch on record type */
	    
			/*
			 * Point to the next record in the mcell.
			 */
			pRecord = (bsMRT *) (((char *)pRecord) + 
					     roundup(pRecord->bCnt, sizeof(int))); 
		    } /* end while loop on records */
		} /* end for each mcell on the page */

		status = release_page_to_cache(bmtLBN[bmtXtnt].volume, 
					       currLBN, pPage, 
					       modified, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		if (TRUE == modified) {
		    fixedBMT = TRUE;
		}
		modified = FALSE;

	    } /* end for each page of the extent */
	} /* end for each extent in the BMT */
    } /* end for each volume in the domain */


    /*
     * Update the domain status to indicate that the SBMs
     * have been loaded.
     */    
    DMN_SET_SBM_LOADED(domain->status);

    if (TRUE == fixedBMT) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_nodes */


/* 
 * Function Name: correct_orphans(3.nn) 
 *
 * Description: This routine checks all the mcells to see if they
 *              are orphans, and if they are it calls free_orphan
 *              on the orphan chain.  An orphan means that no other
 *              mcell or tag entries points to the mcell.
 *
 * Input parameters:
 *     domain: The domain being worked on.
 *     
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, FIXED or NO_MEMORY
 *
 */
int correct_orphans(domainT *domain) 
{
    char *funcName = "correct_orphans";
    int	 status;
    int	 fixedBMT; /* Flag set if we fixed anything in the BMT. */
    int  freeMcell;
    int	 volNum;
    int	 pageNum;
    int	 mcellNum;
    int	 tagNum;
    int	 tagSeq;
    int  setTagNum;
    int	 fsFound;
    nodeT *node;
    nodeT *tagNode;
    mcellNodeT tmpMcellNode;
    mcellNodeT *pMcellNode;
    tagNodeT   tmpTagNode;
    tagNodeT   *pTagNode;
    filesetT   *fileset;

    /*
     * Init variables
     */
    fixedBMT = FALSE;

    /*
     * Find the first mcell and walk the list.
     * Because we might delete this node in the middle of the loop we
     * need some way to get access to the next node in the list.
     */
    status = find_first_node(domain->mcellList, (void **)&pMcellNode, 
			     &node);
    if (NOT_FOUND == status) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_410, 
			  "Can't find first mcell node for domain %s.\n"),
                  domain->dmnName);
        corrupt_exit(NULL);
    }
    else if (SUCCESS != status) {
	return status;
    }
    
    while (NULL != pMcellNode) {
	/*
	 * Move next to current, and find next.
	 */
	freeMcell = FALSE;
	volNum	 = pMcellNode->mcellId.vol;
	pageNum  = pMcellNode->mcellId.page;
	mcellNum = pMcellNode->mcellId.cell;
	 
	if (MC_IS_MISSING_TAG(pMcellNode->status)) {
	    /*
	     * Because we did not free the mcell when we couldn't find the
	     * tag.seq, and we only re-hooked up the primaries we need 
	     * to re-check if the mcell has a valid tag.  If not this time
	     * we can delete it.
	     */
	    tagNum = pMcellNode->tagSeq.num;
	    tagSeq = pMcellNode->tagSeq.seq;
	    setTagNum = pMcellNode->setTag.num;

	    tmpTagNode.tagSeq.num = tagNum; 
	    pTagNode = &tmpTagNode;

	    fileset = domain->filesets;
	    fsFound = FALSE;
	    
	    /*
	     * We are NOT going to check Options.filesetName
	     * for this loop.
	     */
	    while (NULL != fileset) {
		if (setTagNum == fileset->filesetId.dirTag.num) {
		    fsFound = TRUE;
		    break;
		}
		fileset = fileset->next;
	    }

	    if (TRUE == fsFound) {
		status = find_node(fileset->tagList, 
				   (void **)&pTagNode, &tagNode);
		if (SUCCESS != status) {
		    if (NOT_FOUND == status) {
			/*
			 * We can't find a tag which this mcell belongs
			 * to, so we might as well delete it.
			 */
			freeMcell = TRUE;
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_411,
					 "Deleting mcell (%d,%d,%d), its tag %d.%x does not exist.\n"),
				 volNum, pageNum, mcellNum, 
				 tagNum, tagSeq);

		    } else {
			return status;
		    }
		} else {
		    if (tagSeq != pTagNode->tagSeq.seq) {
			/*
			 * We can't find a tag which this mcell belongs
			 * to, so we might as well delete it.
			 */
			freeMcell = TRUE;
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_411,
					 "Deleting mcell (%d,%d,%d), its tag %d.%x does not exist.\n"),
				 volNum, pageNum, mcellNum, 
				 tagNum, tagSeq);
		    } else {
			/*
			 * Found a tag which this belongs to.
			 */
			MC_CLEAR_MISSING_TAG(pMcellNode->status);
		    }
		}
	    } else {
		/*
		 * Unable to find fileset this mcell belongs to.
		 */
		freeMcell = TRUE;
	    }
	} /* end if is missing tag */

	/*
	 * Check if this mcell have something pointing to it, or
	 * if it was marked to be deleted due to missing tag.
	 */
	if ((FALSE == MC_IS_FOUND_PTR(pMcellNode->status)) ||
	    (TRUE == freeMcell)) {
	    status = free_orphan(domain, volNum, pageNum, mcellNum);
	    if (SUCCESS != status) {
		return status;
	    }
	    freeMcell = TRUE;
	    fixedBMT  = TRUE;
	} /* end if mcell is orphaned */

	/*
	 * Locate the next mcell to work on.
	 */
	if (TRUE == freeMcell) {
	    /*
	     * Need to locate the next mcell, without using node.
	     *
	     * we have volNum,pageNum,mcellNum
	     */
	    tmpMcellNode.mcellId.vol = volNum;
	    tmpMcellNode.mcellId.page = pageNum;
	    tmpMcellNode.mcellId.cell = mcellNum;
	    pMcellNode = &tmpMcellNode;

	    status = find_node_or_next(domain->mcellList,
				       (void **)&pMcellNode, &node);
	    if (SUCCESS != status) {
		if (NOT_FOUND == status) {
		    pMcellNode = NULL;
		} else {
		    return status;
		}
	    }
	} else {
	    status = find_next_node(domain->mcellList, 
				    (void **)&pMcellNode, 
				    &node);
	    if (SUCCESS != status) {
		if (NOT_FOUND == status) {
		    pMcellNode = NULL;
		} else {
		    return status;
		}
	    }
	} /* else node not freed */
    } /* end mcell node loop */

    if (TRUE == fixedBMT) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_orphans */


/*
 * Function Name: correct_bmt_chains (3.74)
 *
 * Description: This is called after we have loaded all tags into the
 *		tag list. At this point we have already handled any
 *		loops which may exist, so we do not need to worry
 *		about getting into mcell loops. Also each individual
 *		mcell has already been checked. This function will
 *		check data which crosses mcells, such as extents and 
 *		proplists. To do this the function will walk the tag
 *		list, and call correct_bmt_chain_by_tag on each tag.
 *
 * Input parameters:
 *     fileset: The fileset to be checked.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, FIXED, NO_MEMORY, CORRUPT
 * 
 */
int correct_bmt_chains(filesetT *fileset)
{
    char       *funcName = "correct_bmt_chains";
    int	       status;
    int        bmtModified;
    tagNodeT   *pTagNode;
    tagNodeT   *pNextTagNode;
    nodeT      *nextNode;

    /*
     * Init variables
     */
    bmtModified	= FALSE;

    /*
     * find the first tag node and walk the chain.
     * Because we might delete this node in the middle of the loop we
     * need someway to get access to the next node in the list.
     *
     * We load the first node into nextNode so we can walk the loop.
     */
    status = find_first_node(fileset->tagList, (void **)&pNextTagNode,
			     &nextNode);
    if (NOT_FOUND == status) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_198,
			  "Can't find first tag node for fileset '%s'.\n"),
                  fileset->fsName);
        corrupt_exit(NULL);
    }
    else if (SUCCESS != status) {
	return status;
    }

    /*
     * The first node found should always be tag 1, if not we have 
     * corruption which has not been fixed.
     */
    if ((NULL == pNextTagNode) ||
	(1 != pNextTagNode->tagSeq.num)) {
	return CORRUPT;
    }  
    
    while (NULL != pNextTagNode) {
	/*
	 * Assign current to the next node, then find next.
	 */
	pTagNode = pNextTagNode;

	/*
	 * Locate the next tag to work on.
	 */
	status = find_next_node(fileset->tagList, (void **)&pNextTagNode, 
				&nextNode);
	if (SUCCESS != status) {
	    if (NOT_FOUND == status) {
		pNextTagNode = NULL;
	    } else {
		return status;
	    }
	}

	status = correct_bmt_chain_by_tag(fileset, pTagNode);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		bmtModified = TRUE;
	    } else {
		return status;
	    }
	}
    } /* end tag loop */

    if (TRUE == bmtModified) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_bmt_chains */


/*
 * Function Name: correct_chain_mcell_count (3.nn)
 *
 * Description: Each extent chain has a count of the number of mcells
 *              in it.  This checks and corrects the count.
 *
 * 		There are three different locations that this value
 *              can be stored.
 *
 *             1) Normal files and ODSV3 in the shadow record
 *             2) Normal files and ODSV4 in the xtnt record
 *             3) Striped files (both version) in EACH shadow record.
 * 
 * NOTE: the count is stored as a short, not an int.
 *
 * Input parameters:
 *     fileset: The fileset to be checked.
 *     chainMcell: This tag's chainMcell.
 *     xtntMcell: This tag's xtntMcell.
 *     numMcells: Collected earlier for normal files to know how many mcells.
 *     isStriped: Used to know if the file is Striped.
 *
 * Return Values: SUCCESS, FIXED, FAILURE, NO_MEMORY
 */
int correct_chain_mcell_count(filesetT *fileset, 
			      mcellT chainMcell, 
			      mcellT xtntMcell, 
			      int numMcells, 
			      int isStriped)
{
    char *funcName = "correct_chain_mcell_count";
    int  status;
    int  modified;
    int  bmtModified;
    int  found;
    int  odsVersion;
    int  currVol;
    LBNT  currLbn;
    uint16T mcellCnt;
    mcellT  shadowMcell;
    mcellT  currentMcell;
    mcellT  nextMcell;
    domainT *domain;
    volumeT *volume;
    pageT   *pCurrentPage;
    bsMPgT  *bmtPage;
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    nodeT   *node;
    bsXtntRT *pXtnt;
    bsShadowXtntT *pShadow;

    /*
     * Init variables
     */
    domain = fileset->domain;
    odsVersion = domain->dmnVersion;
    modified = FALSE;
    bmtModified = FALSE;
    mcellCnt = (uint16T)numMcells;

    if (FALSE == isStriped) {
	/*
	 * Normal file
	 */
	if (3 == odsVersion) {
	    /*
	     * The mcell we want is located in chainMcell.
	     */
	    currentMcell.vol  = chainMcell.vol;
	    currentMcell.page = chainMcell.page;
	    currentMcell.cell = chainMcell.cell;
	} else {
	    /*
	     * The mcell we want is located in xtntMcell.
	     */
	    currentMcell.vol  = xtntMcell.vol;
	    currentMcell.page = xtntMcell.page;
	    currentMcell.cell = xtntMcell.cell;
	}

	if (currentMcell.vol == 0) {
	    /*
	     * We don't have anyway to check, so just return SUCCESS.
	     */
	    return SUCCESS;
	}

	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, &currLbn, 
				     &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);
	pRecord = (bsMRT *)pMcell->bsMR0;
	
	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    if (3 == odsVersion) {
		if (BSR_SHADOW_XTNTS == pRecord->type) {
		    pShadow = (bsShadowXtntT *)((char *)pRecord +
						sizeof(bsMRT));

		    if (pShadow->mcellCnt != mcellCnt) {
			status = add_fixed_message(&(pCurrentPage->messages), 
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_431,
							   "Modified mcell (%d,%d,%d)'s xtnt mcell count from %d to %d.\n"),
						   currentMcell.vol, 
						   currentMcell.page,
						   currentMcell.cell, 
						   pShadow->mcellCnt,
						   mcellCnt);
			if (SUCCESS != status) {
			    return status;
			}
			pShadow->mcellCnt = mcellCnt;
			modified = TRUE;
		    }
		}
	    } else {
		if (BSR_XTNTS == pRecord->type) {
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		    if (pXtnt->firstXtnt.mcellCnt != mcellCnt) {
			status = add_fixed_message(&(pCurrentPage->messages), 
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_431, 
							   "Modified mcell (%d,%d,%d)'s xtnt mcell count from %d to %d.\n"),
						   currentMcell.vol, 
						   currentMcell.page,
						   currentMcell.cell, 
						   pXtnt->firstXtnt.mcellCnt,
						   mcellCnt);
			if (SUCCESS != status) {
			    return status;
			}

			pXtnt->firstXtnt.mcellCnt = mcellCnt;
			modified = TRUE;
		    }
		}
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}
    } else {
	/*
	 * Striped file: Special case we don't have a count which we can
	 * use so we will need to walk the chain counting the mcells, then
	 * when we hit the NEXT shadow (or end) modifiy the prior shadow
	 */
	shadowMcell.vol  = chainMcell.vol;
	shadowMcell.page = chainMcell.page;
 	shadowMcell.cell = chainMcell.cell;
	
	while (0 != shadowMcell.vol) {
	    /*
	     * we still have a shadow we are working on.
	     */
	    currentMcell.vol  = shadowMcell.vol;
	    currentMcell.page = shadowMcell.page;
	    currentMcell.cell = shadowMcell.cell;
	    mcellCnt = 0;

	    while (0 != currentMcell.vol) {
		mcellCnt++; /* count of how many record in chain */
		/*
		 * Read the current page the mcell is located on.
		 */
		volume = &(domain->volumes[currentMcell.vol]);
		status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					     volume->volRbmt->bmtLBNSize, 
					     currentMcell.page, &currLbn, 
					     &currVol);
		if (SUCCESS != status) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
				     "Can't convert BMT page %d to LBN.\n"), 
			     currentMcell.page);
		    return FAILURE;
		}

		status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
		if (SUCCESS != status) {
		    return status;
		} 

		bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
		pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);

		nextMcell.vol  = pMcell->nextVdIndex;
		nextMcell.page = pMcell->nextMCId.page;
		nextMcell.cell = pMcell->nextMCId.cell;

		/*
		 * Release the page, to stop dead locks
		 */
		 
		status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}

		if (0 == nextMcell.vol) {
		    /*
		     * end of chain, check shadows count
		     */
		    volume = &(domain->volumes[shadowMcell.vol]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 shadowMcell.page, 
						 &currLbn, 
						 &currVol);
		    if (SUCCESS != status) {
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
					 "Can't convert BMT page %d to LBN.\n"), 
				 currentMcell.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(currVol, currLbn, 
						  &pCurrentPage);
		    if (SUCCESS != status) {
			return status;
		    } 

		    bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
		    pMcell  = &(bmtPage->bsMCA[shadowMcell.cell]);
		    pRecord = (bsMRT *)pMcell->bsMR0;
	
		    while ((0 != pRecord->type) &&
			   (0 != pRecord->bCnt) &&
			   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
			if (BSR_SHADOW_XTNTS == pRecord->type) {
			    pShadow = (bsShadowXtntT *)((char *)pRecord +
							sizeof(bsMRT));

			    if (pShadow->mcellCnt != mcellCnt) {
				status = add_fixed_message(&(pCurrentPage->messages), 
							   BMT,
							   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_431,
								   "Modified mcell (%d,%d,%d)'s xtnt mcell count from %d to %d.\n"),
							   currentMcell.vol, 
							   currentMcell.page,
							   currentMcell.cell, 
							   pShadow->mcellCnt,
							   mcellCnt);
				if (SUCCESS != status) {
				    return status;
				}

				pShadow->mcellCnt = mcellCnt;
				modified = TRUE;
			    }
			}
			pRecord = (bsMRT *) (((char *)pRecord) + 
					     roundup(pRecord->bCnt, sizeof(int))); 
		    } /* end record loop */

		    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
						   modified, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		    if (TRUE == modified) {
			bmtModified = TRUE;
			modified = FALSE;
		    }

		    /*
		     * No more shadows to process.
		     */
		    currentMcell.vol = nextMcell.vol;
		    currentMcell.page = nextMcell.page;
		    currentMcell.cell = nextMcell.cell;

		    shadowMcell.vol = 0;
		    shadowMcell.page = 0;
		    shadowMcell.cell = 0;
		} else {
		    /*	
		     * We need to check if the next mcell has a shadow.
		     */
		    volume = &(domain->volumes[nextMcell.vol]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 nextMcell.page, &currLbn, 
						 &currVol);
		    if (SUCCESS != status) {
			writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
					 "Can't convert BMT page %d to LBN.\n"), 
				 currentMcell.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(currVol, currLbn, 
						  &pCurrentPage);
		    if (SUCCESS != status) {
			return status;
		    } 

		    bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
		    pMcell  = &(bmtPage->bsMCA[nextMcell.cell]);
		    pRecord = (bsMRT *)pMcell->bsMR0;
		    found   = FALSE;

		    while ((0 != pRecord->type) &&
			   (0 != pRecord->bCnt) &&
			   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
			if (BSR_SHADOW_XTNTS == pRecord->type) {
			    found = TRUE;
			}
			pRecord = (bsMRT *) (((char *)pRecord) + 
					     roundup(pRecord->bCnt, sizeof(int))); 
		    }

		    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
						   FALSE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }

		    if (FALSE == found) {
			/*
			 * Move to the next mcell.
			 */
			currentMcell.vol = nextMcell.vol;
			currentMcell.page = nextMcell.page;
			currentMcell.cell = nextMcell.cell;
		    } else {
			/*
			 * we found the next shadow, now we can check
			 * the mcell count on the prior shadow.
			 */
			volume = &(domain->volumes[shadowMcell.vol]);
			status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						     volume->volRbmt->bmtLBNSize, 
						     shadowMcell.page, 
						     &currLbn, 
						     &currVol);
			if (SUCCESS != status) {
			    writemsg(SV_ERR | SV_LOG_ERR, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
					     "Can't convert BMT page %d to LBN.\n"), 
				     currentMcell.page);
			    return FAILURE;
			}

			status = read_page_from_cache(currVol, currLbn, 
						      &pCurrentPage);
			if (SUCCESS != status) {
			    return status;
			} 

			bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
			pMcell  = &(bmtPage->bsMCA[shadowMcell.cell]);
			pRecord = (bsMRT *)pMcell->bsMR0;
	
			while ((0 != pRecord->type) &&
			       (0 != pRecord->bCnt) &&
			       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
			    if (BSR_SHADOW_XTNTS == pRecord->type) {
				pShadow = (bsShadowXtntT *)((char *)pRecord +
							    sizeof(bsMRT));

				if (pShadow->mcellCnt != mcellCnt) {
				    status = add_fixed_message(&(pCurrentPage->messages), 
							       BMT,
							       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_431,
								       "Modified mcell (%d,%d,%d)'s xtnt mcell count from %d to %d.\n"),
							       currentMcell.vol, 
							       currentMcell.page,
							       currentMcell.cell, 
							       pShadow->mcellCnt,
							       mcellCnt);
				    if (SUCCESS != status) {
					return status;
				    }

				    pShadow->mcellCnt = mcellCnt;
				    modified = TRUE;
				}
			    }
			    pRecord = (bsMRT *) (((char *)pRecord) + 
						 roundup(pRecord->bCnt, sizeof(int))); 
			} /* end record loop */

			status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
						       modified, FALSE);
			if (SUCCESS != status) {
			    return status;
			}
			if (TRUE == modified) {
			    bmtModified = TRUE;
			    modified = FALSE;
			}

			/*
			 * Move to the next shadow record.
			 */
			currentMcell.vol = 0; 
			currentMcell.page = 0;
			currentMcell.cell = 0;

			shadowMcell.vol = nextMcell.vol;
			shadowMcell.page = nextMcell.page;
			shadowMcell.cell = nextMcell.cell;
		    } /* end if found */
		} /* end nextMcell.vol equal 0 */
	    } /* end currentMcell loop */
	} /* end shadowMcell loop */
    } /* end if not striped */

    if (bmtModified == TRUE) {
	return FIXED;
    }
    
    return SUCCESS;
} /* end correct_chain_mcell_count */


/*
 * Function Name: correct_chain_frag (3.nn)
 *
 * Description: The following are frag related checks and corrections
 *
 * 1) The file has a chain record, but the shadow/xtra contains
 *    no extents.
 *    A) This tag is marked as having a frag and the size is 
 *       larger than the frag.
 *    B) The tag is marked as having no frag but the size is 
 *       smaller than a page (sparse frag)
 *  Either case set the size to 0.
 *
 * 2) process this file's frag
 *
 * Note: we don't have to check the filesize against the extents. If the
 * size is larger than the extents, there is a hole at the end of the file.
 * If the size is smaller than the extents, the file will be truncated or
 * have a frag allocated if needed the next time the file is opened and
 * closed.
 *
 * Input parameters:
 *     fileset: The fileset to be checked.
 *     pTagNode: The tag node we are working on.
 *     chainMcell: The value of this tags chainMcell.
 *     lastExtentPage: The last page listed in the extents
 *     foundFsData: Is this a symlink
 *     noXtnts: Does this file have noXtnts in its first Xtnt record.
 *
 * Return Values: SUCCESS, FIXED, FAILURE, NO_MEMORY
 */
int correct_chain_frag(filesetT *fileset, tagNodeT *pTagNode, 
		       mcellT chainMcell, int lastExtentPage,
		       int foundFsData, int noXtnts)
{
    char *funcName = "correct_chain_frag";
    int  status;
    LBNT  currLbn;
    int  currVol;
    int  odsVersion;
    int  modified;
    int  bmtModified;
    long newSize;
    mcellT  currentMcell;
    mcellT  primaryMcell;
    domainT *domain;
    volumeT *volume;
    pageT   *pCurrentPage;
    bsMPgT  *bmtPage;
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    nodeT   *node;
    statT   *pStat;
    statT   alignedStat;  /* needed to fix alignment errors */
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;

    /*
     * Init variables
     */
    domain = fileset->domain;
    odsVersion = domain->dmnVersion;
    modified = FALSE;
    bmtModified = FALSE;

    primaryMcell.vol  = pTagNode->tagPmcell.vol;
    primaryMcell.page = pTagNode->tagPmcell.page;
    primaryMcell.cell = pTagNode->tagPmcell.cell;

    currentMcell.vol  = primaryMcell.vol;
    currentMcell.page = primaryMcell.page;
    currentMcell.cell = primaryMcell.cell;

    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (0 != currentMcell.vol) {
	/*
	 * Read the mcell page (first get LBN).
	 */
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, 
				     &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);
	    
	/*
	 * Now loop through primary chain's records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    if (BMTR_FS_STAT == pRecord->type) {
		pStat = (statT *)((char *)pRecord + sizeof(bsMRT));

		/*
		 * Need to align this struct or we get unaligned 
		 * error messages.
		 */
		memcpy(&alignedStat, (char *)pStat, sizeof(statT));

		/*
		 * Check for frag overlap with a file page.
		 */
		if (BF_FRAG_ANY != pTagNode->fragSize &&
		    pTagNode->fragPage < lastExtentPage) {
		    status = add_fixed_message(&(pCurrentPage->messages), 
					       BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_432,
						       "Modified mcell (%d,%d,%d)'s FSSTAT record, cleared overlapping frag.\n"),
					       currentMcell.vol,
					       currentMcell.page,
					       currentMcell.cell);
		    if (SUCCESS != status) {
			return status;
		    }

		    pTagNode->fragSize = BF_FRAG_ANY;
		    pTagNode->fragSlot = 0;
		    pTagNode->fragPage = 0;
		    alignedStat.fragId.type = BF_FRAG_ANY;
		    alignedStat.fragId.frag = 0;
		    alignedStat.fragPageOffset = 0;

		    /*
		     * Copy the aligned structure into the record.
		     */
		    memcpy((char *)pStat, &alignedStat, sizeof(statT));
		    modified = TRUE;
		}

		/*
		 * Task 1: 
		 *  On ODSV3 file has no extents, but has a chain record 
		 *  On ODSV4 file has no extents.
		 *
		 *  1) If file has a frag, then it should be the	
		 *     correct size.
		 *  2) If file does not have a frag, then file could be
		 *     a sparse file, but should be larger than 1 page.
		 *     This check is only for ODSV3
		 *
		 *  Note that this test cannot be done on clones.
		 */
		if ((TRUE == noXtnts) &&
		    (FALSE == FS_IS_CLONE(fileset->status)) &&
		    ((BF_FRAG_ANY != alignedStat.fragId.type) &&
		     (alignedStat.st_size > (alignedStat.fragId.type  * ONE_K)))) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_433, 
				     "Modifying mcell (%d,%d,%d) FSSTAT record's st_size.\n"),
			     currentMcell.vol,
			     currentMcell.page,
			     currentMcell.cell);

		    status = add_fixed_message(&(pCurrentPage->messages), 
					       BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_434,
						       "Modified mcell (%d,%d,%d)'s FSSTAT record, st_size was %ld now %ld.\n"),
					       currentMcell.vol,
					       currentMcell.page,
					       currentMcell.cell,
					       alignedStat.st_size,
					       0);
		    if (SUCCESS != status) {
			return status;
		    }

		    pTagNode->size = 0;
		    alignedStat.st_size = 0;

		    if (alignedStat.fragId.type != BF_FRAG_ANY) {
			pTagNode->fragSize = BF_FRAG_ANY;
			pTagNode->fragSlot = 0;
			pTagNode->fragPage = 0;

			alignedStat.fragId.type = BF_FRAG_ANY;
			alignedStat.fragId.frag = 0;
			alignedStat.fragPageOffset = 0;

			status = add_fixed_message(&(pCurrentPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_435,
							   "Modified mcell (%d,%d,%d)'s FSSTAT record, deleted frag.\n"),
						   currentMcell.vol,
						   currentMcell.page,
						   currentMcell.cell);
			if (SUCCESS != status) {
			    return status;
			}
		    } /* end if not FRAG_ANY */
		    
		    /*
		     * Copy the aligned structure into the record.
		     */
		    memcpy((char *)pStat, &alignedStat, sizeof(statT));
		    modified = TRUE;
		} /* end task 2 */

		/*
		 * Task 2 : Check the frag if it's in a fileset we're 
		 * checking and if the frag file was selected to be 
		 * checked.
		 */    
		if ((BF_FRAG_ANY != pTagNode->fragSize) &&
		    ((NULL == Options.type) ||
		     (OT_IS_FRAG(Options.typeStatus)))) {
		    status = collect_frag_info(fileset, 
					       pTagNode->fragSize,
					       pTagNode->fragSlot,
					       pMcell->tag, 
					       pMcell->bfSetTag);
		    if (SUCCESS != status) {
			/*
			 * If we couldn't collect the frag info, 
			 * then delete the frag since it probably 
			 * points to someplace outside of the 
			 * frag file.
			 */

			/*
			 * Adjust the size of the file, roundup the size to
			 * the nearest K, then subtract the MAX size of 
			 * the frag.
			 */
			newSize = (roundup(pTagNode->size, ONE_K) - 
				   (pTagNode->fragSize * ONE_K));

			status = add_fixed_message(&(pCurrentPage->messages), 
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_434, 
							   "Modified mcell (%d,%d,%d)'s FSSTAT record, st_size was %ld now %ld.\n"),
						   currentMcell.vol,
						   currentMcell.page,
						   currentMcell.cell,
						   alignedStat.st_size,
						   newSize);
			if (SUCCESS != status) {
			    return status;
			}

			pTagNode->size = newSize;
			alignedStat.st_size = newSize;

			pTagNode->fragSize = BF_FRAG_ANY;
			pTagNode->fragSlot = 0;
			pTagNode->fragPage = 0;

			alignedStat.fragId.type = BF_FRAG_ANY;
			alignedStat.fragId.frag = 0;
			alignedStat.fragPageOffset = 0;

			status = add_fixed_message(&(pCurrentPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_435, 
							   "Modified mcell (%d,%d,%d)'s FSSTAT record, deleted frag.\n"),
						   currentMcell.vol,
						   currentMcell.page,
						   currentMcell.cell);
			if (SUCCESS != status) {
			    return status;
			}

			/*
			 * Copy the aligned structure into the record.
			 */
			memcpy((char *)pStat, &alignedStat, sizeof(statT));
			modified = TRUE;
		    }
		} /* end if file has a frag */
#if 0
                /*
                 * This code is commented out but left in case
                 * we need to use it later. If so, A, B, C, and D need to be
                 * defined as longs since the definitions were removed from 
                 * the start of this routine.
                 *
                 * We may want to think about adding a flag to give the users
                 * the option to have fixfdmn truncate or frag these files 
                 * (the kernel will automatically do this the next time the
                 * file is opened and closed). In this case we may want to 
                 * count the number of files and amount of storage that could 
                 * be released if the flag is not specified so the user can 
                 * determine if it is worth running with the flag to recover 
                 * the extra storage.
                 */
                 
                /*
                 * Task 3 : Check the size of the file
                 *      This cannot be done on clones.
                 *
                 * size the fs_stat thinks the file is:
                 *  A = pTagNode->size 
                 *
                 * number of pages the extents contains:
                 *  B = lastExtentPage * PAGESIZE
                 *
                 * Max size of the frag
                 *  C = pTagNode->fragSize * ONE_K
                 *
                 * Round up to handle wasted space.
                 *  (Large files don't have frags, so round up to 8k)
                 *  D = roundup(A, ONE_K || PAGE_SIZE)
                 *
                 * Normal case:
                 *   D == B + C
                 *   D >  B + C   (Hole at end of file)
                 *
                 * Error case:
                 *   D <  B + C    ==> D = B+C
                 */
                A = pTagNode->size;
                B = lastExtentPage * PAGESIZE;
                C = pTagNode->fragSize * ONE_K;
                if (BF_FRAG_ANY == pTagNode->fragSize) {
                    /*
                     * Could have full page frag
                     */
                    D = roundup(A, PAGESIZE);
                } else {
                    D = roundup(A, ONE_K);
                }

                if ((D < (B + C)) && (FALSE == FS_IS_CLONE(fileset->status))) {
                    writemsg(SV_VERBOSE | SV_LOG_FOUND,
                             catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_433,
                                     "Modifying mcell (%d,%d,%d) FSSTAT record's st_size.\n"),
                             currentMcell.vol,
                             currentMcell.page,
                             currentMcell.cell);

                    status = add_fixed_message(&(pCurrentPage->messages), 
                                               BMT,
                                               catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_434, 
                                                       "Modified mcell (%d,%d,%d)'s FSSTAT record, st_size was %ld now %ld.\n"),
                                               currentMcell.vol,
                                               currentMcell.page,
                                               currentMcell.cell,
                                               alignedStat.st_size,
                                               B + C);
                    if (SUCCESS != status) {
                        return status;
                    }

                    pTagNode->size = B + C;
                    alignedStat.st_size = B + C;
                    memcpy((char *)pStat, &alignedStat, sizeof(statT));
                    modified = TRUE;
                } /* end if D less than B+C */
#endif
		break;
	    } /* end if fs_stat record */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */
	    
	/*
	 * Set current to next mcell
	 */
	currentMcell.vol  = pMcell->nextVdIndex;
	currentMcell.page = pMcell->nextMCId.page;
	currentMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrentPage, modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}
    } /* end while loop of the primary chain */

    if (bmtModified == TRUE) {
	return FIXED;
    }
    
    return SUCCESS;
} /* end correct_chain_frag */


/*
 * Function Name: correct_empty_chain (3.nn)
 *
 * Description: Clear a BSR_XTNT chain record, and mark the mcell it 	
 *              points to as an orphan.
 *
 * Input parameters:
 *     fileset: The fileset to be checked.
 *     pTagNode: The tag node we are working on.
 *
 * Return Values: SUCCESS, FIXED, FAILURE, NO_MEMORY
 */
int correct_empty_chain(filesetT *fileset, tagNodeT *pTagNode)
{
    char *funcName = "correct_empty_chain";
    int  status;
    LBNT  currLbn;
    int  currVol;
    int  modified;
    mcellT  currentMcell;
    mcellT  primaryMcell;
    domainT *domain;
    volumeT *volume;
    pageT   *pCurrentPage;
    bsMPgT  *bmtPage;
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    bsXtntRT  *pXtnt;
    nodeT   *node;
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;

    /*
     * Init variables
     */
    domain = fileset->domain;
    modified = FALSE;

    /*
     * This file is empty, should not have a chain.
     */
    primaryMcell.vol  = pTagNode->tagPmcell.vol;
    primaryMcell.page = pTagNode->tagPmcell.page;
    primaryMcell.cell = pTagNode->tagPmcell.cell;

    currentMcell.vol  = primaryMcell.vol;
    currentMcell.page = primaryMcell.page;
    currentMcell.cell = primaryMcell.cell;

    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (0 != currentMcell.vol) {
	/*
	 * Read the mcell page (first get LBN).
	 */
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, 
				     &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);
	    
	/*
	 * Now loop through primary chain's records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    if (BSR_XTNTS == pRecord->type) {
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		if (0 == pXtnt->chainVdIndex) {
		    /*
		     * Chain already empty
		     */
		    break;
		} else {
		    /*
		     * We are going to truncate the chain chain, 	
		     * we need to mark the chain as MISSING.
		     */
		    tmpMcellNode.mcellId.vol  = pXtnt->chainVdIndex;
		    tmpMcellNode.mcellId.page = pXtnt->chainMCId.page;
		    tmpMcellNode.mcellId.cell = pXtnt->chainMCId.cell;
		    pMcellNode = &tmpMcellNode;
		
		    status = find_node(domain->mcellList, 
				       (void **)&pMcellNode, &node);
		    if (SUCCESS != status) {
			/*
			 * Should already be found.
			 */
			return status;
		    }

		    /*
		     * Mark this mcell as an orphan.
		     */
		    MC_CLEAR_FOUND_PTR(pMcellNode->status);

		    /*
		     * Truncate the chainMcell 
		     */
		    status = add_fixed_message(&(pCurrentPage->messages), 
					       BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_471,
						       "Modified mcell (%d,%d,%d)'s xtnt chain from (%d,%d,%d) to (%d,%d,%d).\n"),
					       currentMcell.vol, 
					       currentMcell.page,
					       currentMcell.cell, 
					       pXtnt->chainVdIndex,
					       pXtnt->chainMCId.page,
					       pXtnt->chainMCId.cell,
					       0, 0, 0);
		    if (SUCCESS != status) {
			return status;
		    }	
			
		    pXtnt->chainVdIndex = 0;
		    pXtnt->chainMCId.page = 0;
		    pXtnt->chainMCId.cell = 0;
		    pXtnt->firstXtnt.mcellCnt = 1;
		    modified = TRUE;
		    break;
		}
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */

	/*
	 * Set current to next mcell
	 */
	currentMcell.vol  = pMcell->nextVdIndex;
	currentMcell.page = pMcell->nextMCId.page;
	currentMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrentPage, TRUE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
    } /* end while loop of the primary chain */
    if (TRUE == modified) {
	return FIXED;
    }
    return SUCCESS;
} /* end correct_empty_chain */


/*
 * Function Name: correct_quota_files (3.73)
 *
 * Description: This routine checks the user and group quota files for
 *		the specified fileset. The only known corruption in
 *		the user/group quota files that can prevent mounting
 *		of a fileset involves the quota file size. For other
 *		corruptions in the user/group quota files, the user
 *		needs to run quotacheck instead of fixfdmn.
 *
 * Input parameters:
 *     fileset: The fileset which needs its quota files checked.
 *
 * Output parameters:
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY
 * 
 */
int correct_quota_files(filesetT *fileset)
{
    char	*funcName = "correct_quota_files";

    domainT	*domain;
    int		tagnum;
    int		status;
    tagNodeT	tmpTagNode;
    tagNodeT	*tagNode;
    nodeT	*currNode;
    pagetoLBNT	*extents;
    int		extentSize;
    long	lastpage;
    char        quotaType[10];

    domain = fileset->domain;

    /*
     * Don't bother with clone filesets.
     */
    if (FS_IS_CLONE(fileset->status)) {
	/*
	 * FUTURE: Figure out what should be done here.
	 */
	return SUCCESS;
    }

    for (tagnum = 4 ;	/* User Quota File  */
	 tagnum <= 5 ;	/* Group Quota File */
	 tagnum++)
    {
	if (4 == tagnum) {
	    strcpy(quotaType, "user");
	} else {
	    strcpy(quotaType, "group");
	}

	tmpTagNode.tagSeq.num = tagnum;
	tagNode = &tmpTagNode;
	status = find_node(fileset->tagList, (void **)&tagNode, &currNode);
	if (NOT_FOUND == status) {
	    /*
	     * Didn't find correct tagNode - Go on to next one.
	     */
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_436, 
			     "No %s quota file found in fileset %s.\n"),
		     quotaType, fileset->fsName);
	    /*
	     * FUTURE: create missing quota file.
	     */
	    continue;
	} else if (SUCCESS != status) {
	    return status;
	}

	status = collect_extents(domain, fileset, &(tagNode->tagPmcell), FALSE,
				 &extents, &extentSize, FALSE);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_437, 
			     "Could not collect extents for %s quota file in fileset %s.\n"), 
		     quotaType, fileset->fsName);
	    /*
	     * FUTURE: deal with corrupt primary mcell.
	     */
	}
	lastpage = extents[extentSize].page;
	free(extents);

	if (tagNode->size <= (lastpage - 1) * PAGESIZE ||
	    tagNode->size > lastpage * PAGESIZE)
	{
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_438,
			     "Modifying fileset %s's %s quota file size in FSSTAT record.\n"),
		     fileset->fsName, quotaType);
	    tagNode->size = lastpage * PAGESIZE;
	    status = correct_fsstat(fileset, tagNode);
	    if (SUCCESS != status) {
		return status;
	    }
	}
    } /* end loop on user and group quota files */
    return SUCCESS;
} /* end correct_quota_files */


/*
 * Function Name: correct_fsstat (3.nn)
 *
 * Description: This routine is called after another routine has made
 *		a change in the tagNodeT structure of a tag.  It takes
 *		the information in the tagNode structure and modifies
 *		the FS_STAT structure in the primary/next mcell of the
 *		tag.
 *
 * Input parameters:
 *	fileset: The fileset this tag lives in.
 *	tagNode: The tag node structure containing the new information
 *		 to be written to the FS_STAT structure.
 *
 * Returns: SUCCESS, FAILURE or NO_MEMORY
 *
 */
int correct_fsstat(filesetT *fileset, tagNodeT *tagNode)
{
    char	*funcName = "correct_fsstat";
    domainT	*domain;
    mcellT	*primaryMcell;
    mcellT	currentMcell;
    volumeT	*volume;
    LBNT	currLbn;
    int		currVol;
    pageT	*pPage;
    mcellT	nextMcell;
    int		modified;
    bsMPgT	*bmtPage;
    bsMCT	*pMcell;
    bsMRT	*pRecord;
    statT	*pStat;
    statT       alignedStat; /* needed to fix alignment errors */
    int		status;

    /*
     * Init variables.
     */
    domain = fileset->domain;
    primaryMcell = &(tagNode->tagPmcell);
    currentMcell.vol  = primaryMcell->vol;
    currentMcell.page = primaryMcell->page;
    currentMcell.cell = primaryMcell->cell;
    
    assert(FALSE == T_IS_CLONE_ORIG_PMCELL(tagNode->status));

    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (currentMcell.vol != 0) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, &currLbn, 
				     &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol, currLbn, &pPage);
	if (SUCCESS != status) {
	    return status;
	} 
	
	modified = FALSE;
	bmtPage = (bsMPgT *)pPage->pageBuf;
	pMcell = &(bmtPage->bsMCA[currentMcell.cell]);
	
	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;
	
	pRecord = (bsMRT *)pMcell->bsMR0;
	
	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    
	    if (pRecord->type == BMTR_FS_STAT) {
		pStat = (statT *)((char *)pRecord + sizeof(bsMRT));

		/*
		 * Copy the known values into aligned stat.
		 */
		memcpy(&alignedStat, (char *)pStat,  sizeof(statT));

		alignedStat.st_size        = tagNode->size;
		alignedStat.st_mode        = tagNode->fileType;
		alignedStat.fragId.type    = tagNode->fragSize;
		alignedStat.fragId.frag    = tagNode->fragSlot;
		alignedStat.fragPageOffset = tagNode->fragPage;
		alignedStat.st_nlink       = tagNode->numLinks;
		alignedStat.dir_tag.num    = tagNode->parentTagNum.num;
		alignedStat.dir_tag.seq    = tagNode->parentTagNum.seq;

		/*
		 * Need to align this struct or we get unaligned
		 * error messages.
		 */
		memcpy((char *)pStat, &alignedStat, sizeof(statT));
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_439,
						   "Modified mcell (%d,%d,%d)'s FSSTAT record.\n"),
					   currentMcell.vol,
					   currentMcell.page,
					   currentMcell.cell);
		if (SUCCESS != status) {
		    return status;
		}

		modified = TRUE;

		/*
		 * Found record - can break out of record loop.
		 */
		break;
	    }			
	    pRecord = (bsMRT *) (((char *)pRecord) +
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */
	
	status = release_page_to_cache(currVol, currLbn, pPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	
	if (modified == TRUE) {
	    /*
	     * We found the FS_STAT record and fixed it.
	     */
	    return SUCCESS;
	}
	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMcell.vol != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currentMcell.vol   = nextMcell.vol;
	    currentMcell.page  = nextMcell.page;
	    currentMcell.cell  = nextMcell.cell;
	} else {
	    currentMcell.vol   = 0;
	    currentMcell.page  = 0;
	    currentMcell.cell  = 0;
	}/* end if nextMcell exists */
    }/* end chain loop */

    /*
     * Couldn't find an FSSTAT record for this tag - this should be
     * impossible as correct_bmt_chains will either create an FSSTAT
     * record for every file, or delete the tag.
     */
    writemsg(SV_ERR | SV_LOG_ERR, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_440, 
		     "Couldn't correct FSSTAT record for file (%d.%x).\n"),
	     tagNode->tagSeq.num, tagNode->tagSeq.seq);
    return FAILURE;
} /* end correct_fsstat */


/*
 * Function Name: validate_fileset_name
 *
 * Description: This takes a fileset name and performs checks to see if
 *              to see if the filename is invalid.
 *
 * Input parameters:
 *     fsName: Pointer to the fileset name being checked. 
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE
 * 
 */
int validate_fileset_name(char *fsName)
{
    char *funcName = "validate_fileset_name";
    char tmpName[ML_SET_NAME_SZ + 2];
    char *cp;

    strncpy(tmpName, fsName, ML_SET_NAME_SZ + 1);
    tmpName[ML_SET_NAME_SZ + 1] = '\0';

    if (strlen(tmpName) == 0) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_441, 
			 "Zero length fileset name.\n"));
	return FAILURE;
    }

    if (strlen(tmpName) >= ML_SET_NAME_SZ) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_442, 
			 "Fileset name length too long.\n"));
	return FAILURE;
    }

    if (cp = strpbrk(tmpName, "/# :*?\t\n\f\r\v")) { /* Copied from mkfset.c */
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_443, 
			 "Fileset name contains illegal character(s).\n"));
	return FAILURE;
    }

    return SUCCESS;
} /* end validate_fileset_name */


/*
 * Function Name: delete_tag
 *
 * Description: Given a tag and a fileset completly remove this tag.
 *		It also removes the extents from the sbm, and the mcell
 *              from the skip list by calling free_mcell.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     tagNum: The tag number we are deleting.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY
 *
 * NOTE: As this follows chains/next make sure all PAGES are released.
 * 
 */
int delete_tag(filesetT *fileset, int tagNum)
{
    char *funcName = "delete_tag";
    int	 status;
    int	 seqNum;	 /* The sequence number of the tag being deleted */
    int	 tagEntryOnPage; /* Which entry on the tag page */
    int	 tagPageNum;
    LBNT tagPageLbn;
    int  tagPageVol;
    int	 currPage;
    LBNT currLbn;
    int  currVol;
    int	 numExtents;
    int  tmpTagsPerPage;
    domainT *domain;
    volumeT *volume;
    mcellT  primaryMcell;
    mcellT  currentMcell;
    mcellT  nextMcell;
    mcellT  chainMcell;
    pagetoLBNT *pExtents;     /* The extents to check */
    pageT      *pCurrentPage; /* The current page we are checking */
    pageT      *pHeadPage;    /* The head page of the tag page free list */
    bsTDirPgT  *pTagPage;     /* The current tag page we are working on */
    bsTDirPgT  *pHeadTagPage; /* The head tag page free list*/
    bsMPgT     *pBmtPage;     /* The current BMT page we are working on */
    bsTMapT    *tagEntry;     /* The current tag on the tag page */
    bsTMapT    *headTagEntry; /* The tag on the head tag page */
    bsMCT      *pMcell;	      /* The current BMT mcell we are working on */ 
    bsMRT      *pRecord;      /* The current BMT record we are working on */ 
    tagNodeT   *pTagNode;     /* The tag entry in the skip list */
    tagNodeT   tagNode;
    mcellNodeT *pMcellNode;
    mcellNodeT mcellNode;
    nodeT      *node;

    /*
     * Initialize variables.
     */
    domain     = fileset->domain;
    pExtents   = fileset->tagLBN;
    numExtents = fileset->tagLBNSize;
    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    if (tagNum <= 5) {
	writemsg(SV_DEBUG | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_444,
			 "Not deleting fileset %s tag %d from tag file because tag is a metadata file.\n"),
		 fileset->fsName, tagNum);
	return SUCCESS;
    }

    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_445, 
		     "Deleting fileset %s tag %d from tag file.\n"), 
	     fileset->fsName, tagNum);

    /*
     * Find the page in the tagfile where our tag's entry would be,
     * 0-1021 on 1st page, 1022-2043 on 2nd page, etc.
     */
    tagPageNum = tagNum / BS_TD_TAGS_PG;
    tagEntryOnPage = tagNum % BS_TD_TAGS_PG;
 
    status = convert_page_to_lbn(pExtents, numExtents, tagPageNum, 
				 &tagPageLbn, &tagPageVol);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_381,
			 "Can't convert tag page %d to LBN.\n"), 
		 tagPageNum);
	return FAILURE;
    }
	
    status = read_page_from_cache(tagPageVol, tagPageLbn, &pCurrentPage);
    if (SUCCESS != status) {
	return status;
    } 

    /*
     * Now we have the correct tag page.  Find the correct tag
     * and save the primary mcell.
     */
    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
    tagEntry = &(pTagPage->tMapA[tagEntryOnPage]);
    seqNum   = tagEntry->tmSeqNo;  /* Save for later in function */
    
    primaryMcell.vol  = tagEntry->tmVdIndex;
    primaryMcell.page = tagEntry->tmBfMCId.page;
    primaryMcell.cell = tagEntry->tmBfMCId.cell;

    /*
     * Now that we have the primary mcell, if this is a clone file, and
     * the tag points to the original file's mcell, DON'T delete that
     * mcell chain.
     */
    if (FS_IS_CLONE(fileset->status)) {
	tagNode.tagSeq.num = tagNum;
	pTagNode = &tagNode;

	status = find_node(fileset->tagList, (void **)&pTagNode, &node);
	if (SUCCESS == status) {
	    if (T_IS_CLONE_ORIG_PMCELL(pTagNode->status)) {
		primaryMcell.vol = 0;
		primaryMcell.page = 0;
		primaryMcell.cell = 0;
	    }
	} else {
	    /*
	     * Not found should never happen.
	     */
	    return status;
	}
    }

    /*
     * Mark this tag as not used.
     */
    tagEntry->tmSeqNo &= (uint16T)~BS_TD_IN_USE;
    tagEntry->tmVdIndex = 0;
    tagEntry->tmNextMap = pTagPage->tpNextFreeMap;
    pTagPage->tpNextFreeMap = tagEntryOnPage + 1; /* WARNING: Off by 1 */

    /*
     * Have to deal with page 0 (specifically tag 0) special case
     * being used but not counted as allocated.
     */
    if (0 == tagPageNum) {
	tmpTagsPerPage = BS_TD_TAGS_PG - 1;
    } else {
	tmpTagsPerPage = BS_TD_TAGS_PG;
    }

    /*
     * Does the page need to be added to the free tag page list.
     */
    if (tmpTagsPerPage == (pTagPage->tpNumAllocTMaps +
			   pTagPage->tpNumDeadTMaps)) {
	int headVol;
	LBNT headLbn;

	/*
	 * This was a full page, which now has a free element.
	 */
	if (0 != tagPageNum) {
	    /*
	     * This page is NOT the same page as the head of the list.
	     */
	    status = convert_page_to_lbn(pExtents, numExtents, 0, 
					 &headLbn, &headVol);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = read_page_from_cache(headVol, headLbn,
					  &pHeadPage);
	    if (SUCCESS != status) {
		return status;
	    }
	} else {
	    pHeadPage = pCurrentPage;
	}

	/*
	 * Have the head page, add the free page to the head of the list.
	 */
	pHeadTagPage = (bsTDirPgT *)pHeadPage->pageBuf;
	headTagEntry = &(pHeadTagPage->tMapA[0]);

	pTagPage->tpNextFreePage = headTagEntry->tmFreeListHead;
	headTagEntry->tmFreeListHead = pTagPage->tpCurrPage + 1; /* WARNING: Off by 1 */

	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_386, 
			 "Adding fileset %s's tag page %d to free list.\n"),
		 fileset->fsName, tagPageNum);
	status = add_fixed_message(&(pCurrentPage->messages), tagFile,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_387, 
					   "Modified fileset %s's tag page, added page to free list.\n"),
				   fileset->fsName);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Release the page if needed.
	 */
	if (0 != tagPageNum) {
	    status = release_page_to_cache(headVol, headLbn, pHeadPage, 
					   TRUE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	} else {
	    pHeadPage = NULL;
	}
    }
    /*
     * Decrement allocated maps 
     */
    pTagPage->tpNumAllocTMaps--; 

    status = add_fixed_message(&(pCurrentPage->messages), tagFile,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_447,
				       "Deleted fileset %s's tag %d.%x from tag file.\n"),
			       fileset->fsName, tagNum, seqNum);
    if (SUCCESS != status) {
	return status;
    }

    status = release_page_to_cache(tagPageVol, tagPageLbn, pCurrentPage, 
				   TRUE, FALSE);
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Now we need to follow the primary and remove all mcells on it.
     */
    currentMcell.vol   = primaryMcell.vol;
    currentMcell.page  = primaryMcell.page;
    currentMcell.cell  = primaryMcell.cell;
	
    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (0 != currentMcell.vol) {
	/*
	 * Read the current mcell's page (first get LBN).
	 */
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	pBmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	 = &(pBmtPage->bsMCA[currentMcell.cell]);

	/*
	 * Save the next mcell for the next pass through the loop
	 */
	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Now loop through the mcell records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    int	     extent;
	    int slot;
	    int size;
	    bsXtntRT *pXtnt;
	    statT *pStat;
            statT alignedStat; /* needed to fix alignment errors */

	    /*
	     * Only concerned with BSR_XTNTS to find the chain and
	     * BMTR_FS_STAT to clear bits allocated to the frag.
	     */
	    if (BSR_XTNTS == pRecord->type) {
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		/*
		 * Set next Chain to follow it, after we follow this 
		 * chain.
		 */
		chainMcell.vol	= pXtnt->chainVdIndex;
		chainMcell.page = pXtnt->chainMCId.page;
		chainMcell.cell = pXtnt->chainMCId.cell;
	    } /* end if BSR_XTNT */
	    else if (BMTR_FS_STAT == pRecord->type) {
	        /*
	         * Clear the bits for this frag from the fileset slot array.
	         */
                pStat    = (statT *)((char *)pRecord + sizeof(bsMRT));
                memcpy(&alignedStat, (char *)pStat, sizeof(statT));
		size = alignedStat.fragId.type;
		slot = alignedStat.fragId.frag;
		
		if (BF_FRAG_ANY != size) {
		    set_frag_unused(fileset, slot, size);
		}
	    } /* end else if BMTR_FS_STAT */
	    
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */

	/*
	 * Now that we have the info we need from the mcell
	 */
	status = free_mcell(domain, currVol, pCurrentPage, currentMcell.cell);
	if (SUCCESS != status) {
	    return status;
	}

	status = add_fixed_message(&(pCurrentPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_448, 
					   "Deleted mcell (%d,%d,%d), belongs to tag %d.%x which is being deleted.\n"), 
				   currVol,
				   currentMcell.page, 
				   currentMcell.cell,
				   tagNum, seqNum);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       TRUE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (0 == nextMcell.vol) {
	    if (0 != chainMcell.vol) {
		nextMcell.cell = chainMcell.cell;
		nextMcell.page = chainMcell.page;
		nextMcell.vol  = chainMcell.vol;
	    } else {
		nextMcell.cell = 0;
		nextMcell.page = 0;
		nextMcell.vol  = 0;
	    }
	    chainMcell.vol  = 0;
	    chainMcell.page = 0;
	    chainMcell.cell = 0;
	}

	/*
	 * Verify that the next mcell exits.
	 */
	mcellNode.mcellId.vol  = nextMcell.vol;
	mcellNode.mcellId.page = nextMcell.page;
	mcellNode.mcellId.cell = nextMcell.cell;
	pMcellNode = &mcellNode;
	    
	status = find_node(domain->mcellList, (void **)&pMcellNode, &node);
	if (SUCCESS != status) {
	    nextMcell.vol  = 0;
	    nextMcell.page = 0;
	    nextMcell.cell = 0;
	} 

	currentMcell.cell = nextMcell.cell;
	currentMcell.page = nextMcell.page;
	currentMcell.vol  = nextMcell.vol;

    } /* end following chain */

    /*
     * Remove this tag from in-memory skiplist
     */
    tagNode.tagSeq.num = tagNum;
    pTagNode = &tagNode;

    status = delete_node(fileset->tagList, (void **)&pTagNode);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_449,
			 "Internal Error : deletion of node %d.%x failed in delete_tag.\n"),
		 tagNum,seqNum);
	return status;
    }
    free(pTagNode);

    /*
     * Directory entries will be caught and cleaned in correct_all_dirs
     */
    return SUCCESS;
} /* delete_tag */


/*
 * Function Name: correct_chain_fsstat
 *
 * Description:	 This is called when you can't find the fsstat record.
 *		 It then adds one or delete the tag.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     primaryMcell: The information needed to locate the primary mcell.
 *     tagNum: The tag we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, DELETED, FAILURE, NO_MEMORY
 */
int correct_chain_fsstat(filesetT *fileset, mcellT primaryMcell, int tagNum)
{
    char    *funcName = "correct_chain_fsstat";
    int	    modified;
    int	    status;
    int	    currVol;
    LBNT    currLbn;
    int	    bytesUsed;
    int     currentTagNum;
    int     currentSeqNum;
    mcellT  currentMcell;     /* The mcell currently being worked on */
    mcellT  nextMcell;	      /* The next mcell on the next chain */
    volumeT *volume;
    pageT   *pCurrentPage;    /* The current page we are checking */
    bsMPgT  *bmtPage;	  /* The current BMT page we are working on */
    bsMCT   *pMcell;	  /* The current BMT mcell we are working on */
    bsMRT   *pRecord;
    domainT *domain;

    /*
     * Init variables.
     */
    domain = fileset->domain;
    modified = FALSE;

    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_450,
		     "Fixing FSSTAT record on tag %d's mcell chain.\n"), 
	     tagNum);

    /*
     * Can we create/find an mcell to hold the fsstat record 
     * Walk the main chain looking for space in one of its mcells
     * if an mcell has space then add a default fsstat record to
     * it otherwise delete the tag.
     */
    
    /*
     * Start at primary
     */
    currentMcell.vol  = primaryMcell.vol;
    currentMcell.page = primaryMcell.page;
    currentMcell.cell = primaryMcell.cell;

    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (0 != currentMcell.vol) {
	bytesUsed = 0;

	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, &currLbn, 
				     &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage	   = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	   = &(bmtPage->bsMCA[currentMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;
	currentTagNum  = pMcell->tag.num;
	currentSeqNum  = pMcell->tag.seq;

	/*
	 * Now loop through primary's next records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    bytesUsed += roundup(pRecord->bCnt,(sizeof(int)));
	    pRecord = (bsMRT *) (((char *)pRecord) + roundup(pRecord->bCnt, 
							     sizeof(int))); 
	} /* end record loop */
	
	/*
	 * At this point pRecord points to the end of the mcell.
	 */
	if (BS_USABLE_MCELL_SPACE > (bytesUsed + sizeof(statT))) {
	    /*	
	     * We have space in this mcell to add the fstat record.
	     */
	    bsMRT    mcellRecordHeader;
	    statT    newFsStat;
	    tagNodeT tmpTagNode;
	    tagNodeT *pTagNode;
	    nodeT    *node;
	    statT    *pStat;

	    /*
	     * First load the mcell record header.
	     */
	    mcellRecordHeader.bCnt = sizeof(bsMRT) + sizeof(statT);
	    mcellRecordHeader.type = BMTR_FS_STAT;
	    mcellRecordHeader.version = 0;
	    
	    /*
	     * memcpy the record header in.
	     */
	    memcpy((char *)pRecord, &mcellRecordHeader, sizeof(bsMRT));

	    /*
	     * We need the the tag node to get some of the info.
	     */
	    tmpTagNode.tagSeq.num = currentTagNum;
	    pTagNode = &tmpTagNode;
		    
	    status = find_node(fileset->tagList, (void **)&pTagNode, &node);
	    if (SUCCESS != status) {
		return status;
	    }
	    
	    /*
	     * Now load the newFsStat structure.
	     */
	    newFsStat.st_ino.num     = currentTagNum;
	    newFsStat.st_ino.seq     = currentSeqNum;
	    newFsStat.st_mode        = S_IRWXG | S_IFREG; /* reg file*/
	    newFsStat.st_uid         = 0; /* root */
	    newFsStat.st_gid         = 0; /* system */
	    newFsStat.st_size        = pTagNode->size;
	    newFsStat.st_atime       = 0; /* Unknown access time */
	    newFsStat.st_uatime      = 0; /* Unknown access time */
	    newFsStat.st_mtime       = 0; /* Unknown modify time */
	    newFsStat.st_umtime      = 0; /* Unknown modify time */
	    newFsStat.st_ctime       = 0; /* Unknown creation time */
	    newFsStat.st_uctime      = 0; /* Unknown creation time */
	    newFsStat.st_flags       = 0; /* Unknown flag type */
	    newFsStat.dir_tag.num    = 0; /* Unknown parent tag */
	    newFsStat.dir_tag.seq    = 0; /* Unknown parent seq */
	    newFsStat.fragId.type    = 0; /* Unknown fragId type */
	    newFsStat.fragId.frag    = 0; /* Unknown fragId frag */
	    newFsStat.fragPageOffset = 0; /* Unknown frag page offset */
	    newFsStat.st_nlink       = 1; /* Most likely nlink */

	    /*
	     * Load up our in-memory tagnode information.
	     */
	    pTagNode->size = newFsStat.st_size;
	    pTagNode->fileType = newFsStat.st_mode;
	    pTagNode->fragSize = newFsStat.fragId.type;
	    pTagNode->fragSlot = newFsStat.fragId.frag;
	    pTagNode->fragPage = newFsStat.fragPageOffset;
	    pTagNode->numLinks = newFsStat.st_nlink;
	    pTagNode->parentTagNum.num = newFsStat.dir_tag.num;
	    pTagNode->parentTagNum.seq = newFsStat.dir_tag.seq;

	    /*
	     * Now that we have load the structure we need to memcpy this
	     * out to the page.
	     */
	    pStat = (statT *)((char *)pRecord + sizeof(bsMRT));	    

	    memcpy((char *)pStat, &newFsStat, sizeof(statT));

	    /*
	     * Now we need to add a trailing mcell record header.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + roundup(pRecord->bCnt, 
							     sizeof(int))); 
	    mcellRecordHeader.bCnt = 0;
	    mcellRecordHeader.type = 0;
	    mcellRecordHeader.version = 0;
	    
	    /*
	     * memcpy the record header in.
	     */
	    memcpy((char *)pRecord, &mcellRecordHeader, sizeof(bsMRT));

	    status = add_fixed_message(&(pCurrentPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_451, 
					       "Modified tag %d.%x's mcell (%d,%d,%d), added FSSTAT record.\n"),
				       currentTagNum, currentSeqNum,
				       currentMcell.vol,
				       currentMcell.page,
				       currentMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	}

	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    /*
	     * We have fixed the problem so we don't need to continue.
	     */
	    return SUCCESS;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMcell.vol != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currentMcell.vol  = nextMcell.vol;
	    currentMcell.page = nextMcell.page;
	    currentMcell.cell = nextMcell.cell;
	} else {
	    currentMcell.vol  = 0;
	    currentMcell.page = 0;
	    currentMcell.cell = 0;
	}/* end if nextMcell exists */
    } /* end chain loop */

    /*
     * We were not able to find space to add the fsstat record so 
     * we need to delete the tag.
     */
    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_452, 
		     "Deleting fileset %s tag %d, can't add an FSSTAT record.\n"), 
	     fileset->fsName, tagNum);
    status = delete_tag(fileset, tagNum);
    if (SUCCESS != status) {
	return status;
    }
    return DELETED;
} /* end correct_chain_fsstat */


/*
 * Function Name: correct_chain_proplist
 *
 * Description: If a tag has any (1 or more) proplists, check them.
 *		Note that if any of the property lists are corrupt, *ALL*
 *		proplists will be deleted from this tag.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     primaryMcell: The information needed to locate the primary mcell.
 *     tagNum: The tag we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, FAILURE
 */
int correct_chain_proplist(filesetT *fileset, mcellT primaryMcell, int tagNum)
{
    char    *funcName = "correct_chain_proplist";
    int	    fixed;
    int	    modified;
    int	    status;
    int	    dmnVersion;
    int	    currVol;
    LBNT    currLbn;	 
    int	    foundHead;
    int	    plCorrupt;
    int	    plType;
    int	    plNameLen;
    int	    plValLen;
    int	    plNum;
    int	    plSeg;
    int	    plSizeUsed;
    int     recNum;
    mcellT  currentMcell;     /* The mcell currently being worked on */
    mcellT  nextMcell;	      /* The next mcell on the next chain */
    volumeT *volume;
    pageT   *pCurrentPage;    /* The current page we are checking */
    bsMPgT  *bmtPage;	      /* The current BMT page we are working on */
    bsMCT   *pMcell;	      /* The current BMT mcell we are working on */
    bsMRT   *pRecord;
    domainT *domain;

    /*
     * Init variables.
     */
    domain     = fileset->domain;
    dmnVersion = domain->dmnVersion;
    foundHead  = FALSE;
    plCorrupt  = FALSE;
    plNum      = 0;
    fixed      = FALSE;
    modified   = FALSE;

    /*
     * Start at primary
     */
    currentMcell.vol  = primaryMcell.vol;
    currentMcell.page = primaryMcell.page;
    currentMcell.cell = primaryMcell.cell;

    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (0 != currentMcell.vol) {
	volume = &(domain->volumes[currentMcell.vol]);

	/*
	 * Read the current mcell page (first get LBN).
	 */
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, 
				     &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currentMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Now loop through primary's next records.
	 * The first loop is looking for problems.  If we
	 * find any then we will fix the chain.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    bsPropListHeadT *pPlHead;
	    bsPropListHeadT_v3 *pPlHeadODSV3;
	    bsPropListPageT *pPlPage;
	    bsPropListPageT_v3 *pPlPageODSV3;

	    /*
	     * We are only concerned with the proplist record types at 
	     * this point.
	     */
	    switch (pRecord->type) {
	      case BSR_PROPLIST_HEAD:
		  /*
		   * We have two versions of this record based on 
		   * ODS version.
		   */
		  pPlHead = (bsPropListHeadT *)((char *)pRecord + 
						sizeof(bsMRT));
		  pPlHeadODSV3 = (bsPropListHeadT_v3 *)((char *)pRecord + 
							sizeof(bsMRT));

		  /*
		   * Check to see if we are in the middle of different 
		   * proplist.
		   */
		  if (TRUE == foundHead) {
		      writemsg(SV_VERBOSE | SV_LOG_FOUND,
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_913,
				       "Found property list head record when data record should be found.\n"));
		      plCorrupt = TRUE;
		      break;
		  }

		  if (3 == dmnVersion) {
		      plSizeUsed = pRecord->bCnt - BSR_PROPLIST_HEAD_SIZE_V3;
		      plType	 = pPlHeadODSV3->flags;
		      plNameLen	 = pPlHeadODSV3->namelen;
		      plValLen	 = pPlHeadODSV3->valuelen;
		      plNum	 = 0;
		      plSeg	 = 0;
		  } else {
		      plSizeUsed = pRecord->bCnt - BSR_PROPLIST_HEAD_SIZE;
		      plType	 = pPlHead->flags;
		      plNameLen	 = pPlHead->namelen;
		      plValLen	 = pPlHead->valuelen;
		      plNum	 = pPlHead->pl_num;
		      plSeg	 = 0;
		  } 

		  if (plType & BSR_PL_DELETED) {
		      /*
		       * This proplist record has been deleted.
		       */
		      foundHead = FALSE;
		      plSizeUsed = 0;
		  }
		  else if (plNameLen > PROPLIST_NAME_MAX) {
		      writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_921,
				       "Proplist name length %d is larger than maximum %d.\n"),
			       plNameLen, PROPLIST_NAME_MAX);
		      plCorrupt = TRUE;
		      foundHead = FALSE;
		      plSizeUsed = 0;
		  }
		  else if (plType & BSR_PL_PAGE) {
		      /*
		       * This is an OLD PL which has not been used since
		       * v4.0 (vanilla).  This record takes up the whole
		       * page.	Make sure it is not larger than 1 page.
		       */
		      if (PL_ENTRY_SIZE(plNameLen, plValLen) > BSR_PROPLIST_PAGE_SIZE) {
			  writemsg(SV_VERBOSE | SV_LOG_FOUND, 
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_455,
					   "Proplist head size %d is larger than page size %d.\n"),
				   PL_ENTRY_SIZE(plNameLen, plValLen), 
				   BSR_PROPLIST_PAGE_SIZE);
			  plCorrupt = TRUE;
		      }
		      if (PL_ENTRY_SIZE(plNameLen, plValLen) > plSizeUsed) {
			  writemsg(SV_VERBOSE | SV_LOG_FOUND,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_454, 
					   "Proplist head size %d is larger than size used %d.\n"),
				   PL_ENTRY_SIZE(plNameLen, plValLen), plSizeUsed);
			  plCorrupt = TRUE;
		      }
		      foundHead = FALSE;
		      plSizeUsed = 0;
		  }
		  else if (plSizeUsed < PL_ENTRY_SIZE(plNameLen, plValLen)) {
		      /*
		       * We should be checking for the BSR_PL_LARGE bit,
		       * but the kernel ignores it, so we have to also.
		       */
		      /*
		       * This record is in multiple mcells;
		       */
		      foundHead = TRUE;
		  } 
		  else {
		      /*
		       * This proplist is completely in this record;
		       *
		       * Make sure that plNameLen and plValLen are not 
		       * larger than this mcell recordSize.
		       */
		      if (PL_ENTRY_SIZE(plNameLen, plValLen) > BSC_R_SZ) {
			  writemsg(SV_VERBOSE | SV_LOG_FOUND,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_453,
					   "Proplist head size %d is larger than the record size %d.\n"),
				   PL_ENTRY_SIZE(plNameLen, plValLen), BSC_R_SZ);
			  plCorrupt = TRUE;
		      }
		      if (PL_ENTRY_SIZE(plNameLen, plValLen) > plSizeUsed) {
			  writemsg(SV_VERBOSE | SV_LOG_FOUND,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_454,
					   "Proplist head size %d is larger than size used %d.\n"),
				   PL_ENTRY_SIZE(plNameLen, plValLen), plSizeUsed);
			  plCorrupt = TRUE;
		      }
		      foundHead = FALSE;
		      plSizeUsed = 0;
		  } 

		  break;

	      case BSR_PROPLIST_DATA:
		  /*
		   * We have two versions of this record based on 
		   * ODS version.
		   */

		  pPlPage = (bsPropListPageT *)((char *)pRecord + 
					       sizeof(bsMRT));
		  pPlPageODSV3 = (bsPropListPageT_v3 *)((char *)pRecord + 
							sizeof(bsMRT));

		  if (FALSE == foundHead) {
		      /*
		       * At a record which requires a head, but it doesn't
		       * exist.
		       */
		      writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_456,
				       "Missing Head of prop list.\n"));
		      plCorrupt = TRUE;
		      break;
		  }

		  if (3 == dmnVersion) {
		      plSizeUsed = plSizeUsed + pRecord->bCnt -	sizeof(bsMRT);
		  } else {
		      plSizeUsed = plSizeUsed + pRecord->bCnt - 
			sizeof(bsMRT) - NUM_SEG_SIZE;
		      if (plNum != pPlPage->pl_num) {
			  writemsg(SV_VERBOSE | SV_LOG_FOUND,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_457,
					   "Proplist num %d does not match expected %d.\n"),
				   pPlPage->pl_num, plNum);
			  plCorrupt = TRUE;
		      }
		      
		      if (plSeg != pPlPage->pl_seg) {
			  writemsg(SV_VERBOSE | SV_LOG_FOUND,
				  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_458,
					  "Proplist seg %d does not match expected %d.\n"),
				  pPlPage->pl_seg, plSeg);
			  plCorrupt = TRUE;
		      }
		      plSeg++;
		  }

		  if (plSizeUsed >= PL_ENTRY_SIZE(plNameLen, plValLen)) {
		      /*
		       * end of current proplist
		       */
		      foundHead = FALSE;
		      plSizeUsed = 0;
		  }

		  break;

	      default:
		  break;
	    } /* end of switch */
	    pRecord = (bsMRT *) (((char *)pRecord) + roundup(pRecord->bCnt, 
							     sizeof(int))); 
	} /* end record loop */
		    
	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (0 != nextMcell.vol) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currentMcell.vol   = nextMcell.vol;
	    currentMcell.page  = nextMcell.page;
	    currentMcell.cell  = nextMcell.cell;
	}  else {
	    currentMcell.vol   = 0;
	    currentMcell.page  = 0;
	    currentMcell.cell  = 0;
	} /* end if nextMcell exists */
    } /* end next mcell loop */

    /*
     * Check to see if last proplist is not complete
     */
    if (0 != plSizeUsed) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_914,
			 "Missing data in property list.\n"));
	plCorrupt = TRUE;
    }

    /*
     * Check if any of the proplists are not corrupt.  If they are
     * delete the proplist from this tag.
     */
    if (TRUE == plCorrupt) {
	/*
	 * Start at primary
	 */
	currentMcell.vol   = primaryMcell.vol;
	currentMcell.page  = primaryMcell.page;
	currentMcell.cell  = primaryMcell.cell;

	/*
	 * Loop until currentMcell is empty [0,0,0]
	 */
	while (0 != currentMcell.vol) {
	    volume = &(domain->volumes[currentMcell.vol]);

	    /*
	     * Read the current mcell page (first get LBN).
	     */
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize, 
					 currentMcell.page, 
					 &currLbn, &currVol);
	    if (SUCCESS != status) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
				 "Can't convert BMT page %d to LBN.\n"), 
			 currentMcell.page);
		return FAILURE;
	    }

	    status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	    if (SUCCESS != status) {
		return status;
	    } 

	    bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	    pMcell  = &(bmtPage->bsMCA[currentMcell.cell]);

	    nextMcell.vol  = pMcell->nextVdIndex;
	    nextMcell.page = pMcell->nextMCId.page;
	    nextMcell.cell = pMcell->nextMCId.cell;

	    /*
	     * Now loop through primary's next records a second time
	     * The first time we looked for problems. If we found any then
	     * this time we will fix them.
	     */
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    recNum = 0;

	    while ((0 != pRecord->type) &&
		   (0 != pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		recNum++;
		/*
		 * We are only concerned with the proplist record types.
		 */
		switch (pRecord->type) {
		  case BSR_PROPLIST_HEAD:
		  case BSR_PROPLIST_DATA:
		      /*
		       * Delete this record as the proplist corrupt.
		       */
		    status = clear_mcell_record(domain, pCurrentPage, 
						 currentMcell.cell, 
						 recNum);
		    if (SUCCESS != status) {
			return status;
		    }
		    modified = TRUE;
		    break;
		  default:
		    break;
		}
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } /* end record loop */
		    
	    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    if (TRUE == modified) {
		modified = FALSE;
	    }

	    /*
	     * Do we have another mcell in this chain 
	     */
	    if (0 != nextMcell.vol) {
		/*
		 * Reset pointers for the next pass.
		 */
		currentMcell.vol   = nextMcell.vol;
		currentMcell.page  = nextMcell.page;
		currentMcell.cell  = nextMcell.cell;
	    }  else {
		currentMcell.vol   = 0;
		currentMcell.page  = 0;
		currentMcell.cell  = 0;
	    } /* end if nextMcell exists */
	} /* end chain loop */
    } /* end if prop list corrupt */

    if (TRUE == plCorrupt) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_chain_proplist */


/*
 * Function Name: correct_chain_tag_and_set
 *
 * Description: check to see if all the tag and setTag match.
 *              This also includes rechecking the FSSTAT record
 *              if it exists in the currentMcell.
 *
 * NOTE: This function can modify multiple pages.  It currently only 
 *       returns FIXED if the current bmtPage is modified.  If another
 *       page is modified it returns SUCCESS.  We could pass a flag which 
 * 	 would let us know, but we are already at 7 variables.
 *
 * NOTE: For clone filesets we only check the tag, not the setTag.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     currMcell: The information on the current mcell.
 *     prevMcell: The information on the previous mcell.
 *     primTagNum: The tag number of this chain
 *     primSeqNum: The tag seq of this chain
 *     nextTagNum: The next mcells tag number
 *     firstXtnt: Are we on the first xtnt record
 *     pPage: The page we are working on.
 *
 * Output parameters:
 *     pPage: The page we are working on.
 *
 * Side effect:
 *     previous page could be modified.
 *
 * Returns: SUCCESS, FIXED, FIXED_PRIOR, NO_MEMORY, FAILURE
 */
int correct_chain_tag_and_set(filesetT *fileset, 
			      mcellT currMcell, mcellT prevMcell, 
			      int primTagNum, int primSeqNum, 
			      int nextTagNum, int firstXtnt,
			      pageT *pPage)
{
    char    *funcName = "correct_chain_tag_and_set";
    int	    status;
    int	    modified;
    int	    currTagNum;
    int	    currSeqNum;
    int	    prevVol;
    LBNT    prevLbn;
    int     fixedPrior;
    int     fsstatNum;
    int     fsstatSeq;
    int	    filesetId;
    int	    filesetIdSeq;
    volumeT *volume;
    bsMPgT  *bmtPage;
    statT   *pStat;
    statT   alignedStat;  /* needed to fix alignment errors */
    pageT   *pPrevPage;	  /* The previous page we are checking */
    bsMPgT  *bmtPrevPage; /* The previous BMT page we are working on */
    bsMCT   *pMcell;	  /* The current BMT mcell we are working on */
    bsMCT   *pPrevMcell; 
    bsMRT   *pRecord;
    domainT *domain;
    mcellNodeT *pMcellNode;
    mcellNodeT mcellNode;
    nodeT   *node;

    /*
     * Initialize variables.
     */
    modified	   = FALSE;
    domain	   = fileset->domain;
    filesetId	   = fileset->filesetId.dirTag.num;
    filesetIdSeq   = fileset->filesetId.dirTag.seq;
    bmtPage        = (bsMPgT *)pPage->pageBuf;
    pMcell	   = &(bmtPage->bsMCA[currMcell.cell]);
    currTagNum     = pMcell->tag.num;
    currSeqNum     = pMcell->tag.seq;
    fixedPrior     = FALSE;
    fsstatNum      = 0;
    fsstatSeq      = 0;

    /*
     * Check if the mcell has the correct tag number.
     */
    if ((primTagNum != currTagNum) || (primSeqNum != currSeqNum)) {
	/*
	 * Something's wrong.
	 * We'll need to modify the mcell skiplist for this node
	 * in addition to the on disk structure(s).
	 */
	mcellNode.mcellId.vol  = currMcell.vol;
	mcellNode.mcellId.page = currMcell.page;
	mcellNode.mcellId.cell = currMcell.cell;
	pMcellNode = &mcellNode;
	status = find_node(domain->mcellList, (void **)&pMcellNode,
			   &node);
	if (SUCCESS != status) {
	    return status;
	} 

	/*
	 * Does this mcell have an fsstat record, if so get
	 * the tag number from it as well.
	 */
	if (MC_IS_FSSTAT(pMcellNode->status)) {
	    pRecord = (bsMRT *)pMcell->bsMR0;
	
	    while ((0 != pRecord->type) &&
		   (0 != pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		/*
		 * If it's not a FSSTAT record, we don't care about it.
		 */
		if (BMTR_FS_STAT == pRecord->type) {
		    pStat    = (statT *)((char *)pRecord + sizeof(bsMRT));

		    fsstatNum = pStat->st_ino.num;
		    fsstatSeq = pStat->st_ino.seq;
		}
		pRecord = (bsMRT *)(((char *)pRecord) +
				    roundup(pRecord->bCnt, sizeof(int)));
	    } /* end while records exist */
	} /* end if mcell has fsstat record */

	/*
	 * Check the rest of chain looking for patterns that 
	 * might help us.  We also will check the fsstat record
	 * if the mcell has one.  
	 *
	 *     Primary	Current	 Fsstat   Next
	 * Valid Cases
	 *	  4	   4	 (4)	  NULL
	 *	  4	   4	 (4)	  4
	 *
	 * Special Case: if fsstat exists in mcell then 
	 *               if fsstat's inode matches Current
	 *        4        5     (5)      any    ->  4 NULL
	 *
	 * Case #1
	 *	  4	   5     (!5)     NULL   ->  4 4 NULL
	 * Case #2
	 *	  4	   5     (!5)     4      ->  4 4 4
	 * Case #3 
	 *	  4	   5     (!5)     !4     ->  4 NULL
	 *
	 * NOTE: if correct_mcell_records was called then current
	 *       will always match the fsstat record.  We may want
	 *       to consider changing this so the check is done in
	 *       chains (or here) instead.
	 *       On aussie c_m_r changed the fsstat inode for only
	 *       5 mcells, so it may not be worth the engineer time
	 *       to do this.
	 */
	if (primTagNum == currTagNum) {
	    /*
	     * Tags the same, Sequence numbers are off.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_466, 
					       "Modified mcell (%d,%d,%d) tag's seq from %x to %x.\n"),
				       currMcell.vol, currMcell.page,
				       currMcell.cell, 
				       pMcell->tag.seq, primSeqNum);
	    if (SUCCESS != status) {
		return status;
	    }

	    pMcellNode->tagSeq.seq = primSeqNum;
	    pMcell->tag.seq = primSeqNum;

	    if (MC_IS_FSSTAT(pMcellNode->status)) {
		if (fsstatSeq != primSeqNum) {
		    pRecord = (bsMRT *)pMcell->bsMR0;
		    
		    while ((0 != pRecord->type) &&
			   (0 != pRecord->bCnt) &&
			   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
			/*
			 * If it's not the FSSTAT record, we don't 
			 * care about it.
			 */
			if (BMTR_FS_STAT == pRecord->type) {
			    pStat = (statT *)((char *)pRecord + sizeof(bsMRT));
			    /*
			     * Need to align this struct or we get 
			     * unaligned error messages.
			     */
			    memcpy(&alignedStat, (char *)pStat, sizeof(statT));
			    alignedStat.st_ino.seq = primSeqNum;

			    /*
			     * Copy the aligned structure into the record.
			     */
			    memcpy((char *)pStat, &alignedStat, sizeof(statT));
			} /* end if FSSTAT record */
			pRecord = (bsMRT *)(((char *)pRecord) +
					    roundup(pRecord->bCnt, sizeof(int)));
		    }	/* end of while records exist */
		} /* end if seq numbers different */
	    } /* end if this mcell has an fsstat record */

	    modified = TRUE;
	} else if (((MC_IS_FSSTAT(pMcellNode->status)) && 
		    (fsstatNum == currTagNum)) ||
		   ((0 != nextTagNum) && (nextTagNum != primTagNum))) {
	    /*
	     * This is the special case were the mcell has an fsstat and
	     * its inode matches the current tag num.  
	     *
	     * OR
	     *
	     * Case #3 : Current & Next different from Primary.
	     *
	     * Either case truncate the chain.
	     */

	    /*
	     * Load the page which contains the chain mcell.
	     */
	    if ((currMcell.page != prevMcell.page) ||
		(currMcell.vol != prevMcell.vol)) {
		volume = &(domain->volumes[prevMcell.vol]);
		status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					     volume->volRbmt->bmtLBNSize, 
					     prevMcell.page,
					     &prevLbn, &prevVol);
		if (SUCCESS != status) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
				     "Can't convert BMT page %d to LBN.\n"), 
			     prevMcell.page);
		    return FAILURE;
		}

		status = read_page_from_cache(prevVol,prevLbn, &pPrevPage);
		if (SUCCESS != status) {
		    return status;
		} 
		
		bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
	    } else {
		/*
		 * Both mcells on same page.
		 */
		bmtPrevPage = bmtPage;
	    }

	    pPrevMcell  = &(bmtPrevPage->bsMCA[prevMcell.cell]);
	    pRecord = (bsMRT *)pPrevMcell->bsMR0;

	    if (FALSE == firstXtnt) {
		/*
		 * Modify the next chain.
		 */
		status = add_fixed_message(&(pPrevPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_422, 
						   "Modified mcell (%d,%d,%d)'s next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
					   prevMcell.vol, 
					   prevMcell.page,
					   prevMcell.cell, 
					   pPrevMcell->nextVdIndex,
					   pPrevMcell->nextMCId.page,
					   pPrevMcell->nextMCId.cell,
					   0, 0, 0);
		if (SUCCESS != status) {
		    return status;
		}
		pPrevMcell->nextMCId.page = 0;
		pPrevMcell->nextMCId.cell = 0;
		pPrevMcell->nextVdIndex = 0;

	    } else {
		/*
		 * Need to modify the previous XTNT chain, not the
		 * previous next chain.
		 */

		/*
		 * Now loop through prevMcells records.
		 */
		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pPrevMcell->bsMR0[BSC_R_SZ])))) {
		    bsXtntRT *pXtnt;

		    /*
		     * Only concerned with BSR_XTNTS
		     */
		    if (BSR_XTNTS == pRecord->type) {
			pXtnt = (bsXtntRT *)((char *)pRecord +
					     sizeof(bsMRT));

			status = add_fixed_message(&(pPrevPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_471, 
							   "Modified mcell (%d,%d,%d)'s xtnt chain from (%d,%d,%d) to (%d,%d,%d).\n"),
						   prevMcell.vol, 
						   prevMcell.page,
						   prevMcell.cell,
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   0, 0, 0);
			if (SUCCESS != status) {
			    return status;
			}

			pXtnt->chainVdIndex = 0;
			pXtnt->chainMCId.page = 0;
			pXtnt->chainMCId.cell = 0;
			pXtnt->firstXtnt.mcellCnt = 1;
  
		    } /* end if BSR_XTNT */
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, sizeof(int))); 
		} /* end record loop */
	    } /* end if prevMcell is BSR_XTNTS mcell */

	    /*
	     * Release the page if needed.
	     */
	    if ((currMcell.page != prevMcell.page) ||
		(currMcell.vol != prevMcell.vol)) {
		/*
		 * The two mcells are on different pages.
		 */
		status = release_page_to_cache(prevVol, prevLbn, pPrevPage, 
					       TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}    
	    } else {
		/*
		 * Both mcells on same page.
		 */
		pPrevPage = NULL;
	    }

	    /*
	     * Mark this mcell as an orphan.
	     */
	    MC_CLEAR_FOUND_PTR(pMcellNode->status);
	    fixedPrior = TRUE;
	} else {
	    /*
	     * case #1: Primary and Current different, with no next.
	     * case #2: Primary & Next the same, different from current
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_467, 
					       "Modified mcell (%d,%d,%d)'s tag from %d.%x to %d.%x.\n"),
				       currMcell.vol, currMcell.page,
				       currMcell.cell, 
				       pMcell->tag.num, pMcell->tag.seq,
				       primTagNum, primSeqNum);
	    if (SUCCESS != status) {
		return status;
	    }

  	    status = update_sbm_mcell_tag_and_set_info( domain,
  							&currMcell,
  						        pMcell->tag.num,
  							pMcell->bfSetTag.num,
  							primTagNum,
  							pMcell->bfSetTag.num,
							pPage);
  	    if (SUCCESS != status) {
  		return status;
  	    }

	    pMcellNode->tagSeq.num = primTagNum;
	    pMcellNode->tagSeq.seq = primSeqNum;
	    pMcell->tag.num = primTagNum;
	    pMcell->tag.seq = primSeqNum;

	    /*
	     * fsstat st_ino record will be corrected in correct_bmt_chains
	     */
	    modified = TRUE;
	} /* end case 1 & 2 */
    } /* tag or sequence numbers different */

    /*
     * Check if the mcell has the correct setTag.
     */
    if ((filesetId != pMcell->bfSetTag.num) || 
	(filesetIdSeq != pMcell->bfSetTag.seq)) {
	/*
	 * We have already at this point checked the tag number.
	 * we are going to play the numbers, and assume that if
	 * setTag is wrong we are going to reset it.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_468, 
					   "Modified mcell (%d,%d,%d)'s set tag from %d.%x to %d.%x.\n"),
				   currMcell.vol, 
				   currMcell.page,
				   currMcell.cell, 
				   pMcell->bfSetTag.num, 
				   pMcell->bfSetTag.seq,
				   filesetId,
				   filesetIdSeq);
	if (SUCCESS != status) {
	    return status;
	}

  	status = update_sbm_mcell_tag_and_set_info( domain,
  						    &currMcell,
  						    pMcell->tag.num,
  						    pMcell->bfSetTag.num,
  						    pMcell->tag.num,
  						    filesetId,
						    pPage);
  	if (SUCCESS != status) {
  	    return status;
  	}

	pMcell->bfSetTag.num = filesetId;
	pMcell->bfSetTag.seq = filesetIdSeq;
	modified = TRUE;
    }

    if (TRUE == fixedPrior) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_469,
			 "Truncating mcell (%d,%d,%d)'s next chain.\n"),
		 prevMcell.vol, prevMcell.page, prevMcell.cell);
	return FIXED_PRIOR;
    } else if (TRUE == modified) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_470,
			 "Modifying mcell (%d,%d,%d)'s tag and set tag.\n"),
		 currMcell.vol, currMcell.page, currMcell.cell);
	return FIXED;
    }

    return SUCCESS;
} /* end correct_chain_tag_and_set */


/*
 * Function Name: append_page_to_file (3.nn)
 *
 * Description: This function tacks a page onto the end of a file.
 *		It will modify the extent map of the file, and the
 *		in-memory SBM to indicate that the page is in use.
 *		FUTURE: If necessary, it will also add a free page
 *		to the BMT.  Handling striped files is also a FUTURE.
 *
 *		NOTE:  This routine CANNOT be called before the SBM
 *		overlaps have been handled and cleared.
 *
 * Input parameters:
 *	fileset: The fileset the file is in.
 *	tag: The tag of the file to have the page appended to.
 *
 * Output parameters:
 *	foundPage: Whether a free page was found or not.
 *
 * Return Values: SUCCESS, FAILURE, NO_MEMORY
 *
 */
int append_page_to_file(filesetT *fileset, tagNodeT *tag, int *foundPage)
{
    char		*funcName = "append_page_to_file";
    domainT		*domain;
    int			status;
    int			currVol;
    LBNT		currLbn;
    pageT		*page;
    int			headFreePageNum;
    int			headFreeVol;
    LBNT		headFreeLbn;
    pageT		*headFreePage;
    int			freePageNum;
    int			freeVol;
    LBNT		freeLbn;
    pageT		*freePage;
    int			primVol;
    LBNT		primLbn;
    pageT		*primPage;
    int			nextVol;
    LBNT		nextLbn;
    int			done;
    int			foundBsrXtntsRecord;
    int			headFreeModified;
    mcellT		currentMcell;
    mcellT		chainMcell;
    mcellT		nextMcell;
    volumeT		*volume;
    bsMPgT		*bmtPage;
    bsMCT		*pMcell;
    bsMRT		*pRecord;
    bsMPgT		*headFreeBmtPage;
    bsMCT		*pHeadFreeMcell;
    bsMRT		*pHeadFreeRecord;
    bsMcellFreeListT	*headRecord;
    bsMPgT		*freeBmtPage;
    bsMCT		*pFreeMcell;
    bsMRT		*pFreeRecord;
    int			freeMcellNum;
    bsMCT		*pNewMcell;
    bsMRT		*pNewRecord;
    bsXtntRT		*pXtnt;
    bsXtraXtntRT	*pXtraXtnt;
    bsShadowXtntT	*pShadowXtnt;
    int			xCnt;
    bsXtraXtntRT	*pNewXtra;
    LBNT		lastXtntLbn;
    int			lastXtntSize;
    int			nextPage;
    mcellT		fakeMcell;
    pagetoLBNT		fakeExtentArray[2];

    domain = fileset->domain;
    *foundPage = FALSE;
    foundBsrXtntsRecord = FALSE;
    headFreeModified = FALSE;
    done = FALSE;

    /*
     * Walk the BMT chain looking for the last mcell for this tag with extents.
     */
    currentMcell.vol  = tag->tagPmcell.vol;
    currentMcell.page = tag->tagPmcell.page;
    currentMcell.cell = tag->tagPmcell.cell;

    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    /*
     * Loop through primary next list.
     */
    while (0 != currentMcell.vol && FALSE == foundBsrXtntsRecord) {
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize,
				     currentMcell.page, &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"),
		     currentMcell.page);
	    return FAILURE;
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
		foundBsrXtntsRecord = TRUE;
	        pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		if (BSXMT_STRIPE == pXtnt->type) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_475,
				     "Internal Error: Attempting to add a page to a stripe file.\n"));
		    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_476, 
				     "This is not yet supported, so the next message\n"));
		    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_477, 
				     "will erroneously tell you there are no free pages\n"));
		    writemsg(SV_ERR | SV_LOG_ERR | SV_CONT, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_478,
				     "in this domain.\n"));
		    *foundPage = FALSE;
		    status = release_page_to_cache(currVol, currLbn, page,
						   FALSE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		    /*
		     * This allows us to continue, faking a different error
		     * as the writemsgs above explain.
		     */
		    return SUCCESS;
		}
		/*
		 * Set chainMcell to follow the extent chain later.
		 */
		chainMcell.vol  = pXtnt->chainVdIndex;
		chainMcell.page = pXtnt->chainMCId.page;
		chainMcell.cell = pXtnt->chainMCId.cell;
		
		break;  /* break out of record loop */
	    }
	    
	    /*
	     * Point to the next record.
	     */
	    pRecord = (bsMRT *)(((char *)pRecord) +
				roundup(pRecord->bCnt, sizeof(int)));
	} /* end record loop */
	
	/*
	 * If the BSR_XTNTS record has not been found yet, or if there
	 * are chain records, release this page.  Thus, if the XTNTS record
	 * has been found AND there is no chain, this page is held onto.
	 */
	if (FALSE == foundBsrXtntsRecord || 0 != chainMcell.vol) {
	    status = release_page_to_cache(currVol, currLbn, page,
					   FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	}
	
	/*
	 * Reset pointers for the next pass.
	 */
	currentMcell.vol  = nextMcell.vol;
	currentMcell.page = nextMcell.page;
	currentMcell.cell = nextMcell.cell;
    } /* end loop on primary next list */
    
    assert(TRUE == foundBsrXtntsRecord);
    
    /*
     * Now start looking through the extent mcell chain.
     */

    currentMcell.vol  = chainMcell.vol;
    currentMcell.page = chainMcell.page;
    currentMcell.cell = chainMcell.cell;
    
    /* 
     * Note that either all pages have been released if there is a
     * chain, or that we have a page open and will never enter this loop
     * if the chainMcell volume is zero.
     */
    while (0 != currentMcell.vol) {
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize,
				     currentMcell.page,
				     &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"),
		     currentMcell.page);
	    return FAILURE;
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
	 * Now loop through looking for the XTRA or SHADOW record.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	
	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (BSR_XTRA_XTNTS == pRecord->type ||
		BSR_SHADOW_XTNTS == pRecord->type)
	    {
		break; /* out of record loop */
	    }
	    pRecord = (bsMRT *)(((char *)pRecord) +
				roundup(pRecord->bCnt, sizeof(int)));
	} /* end record loop */
	
	assert((BSR_XTRA_XTNTS == pRecord->type) ||
	       (BSR_SHADOW_XTNTS == pRecord->type));
	/*
	 * If this is the last mcell, keep it, else release it and
	 * find the next one.
	 */
	if (0 != nextMcell.vol) {
	    status = release_page_to_cache(currVol, currLbn, page,
					   FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	}
	
	/*
	 * Reset pointers for the next pass.
	 */
	currentMcell.vol  = nextMcell.vol;
	currentMcell.page = nextMcell.page;
	currentMcell.cell = nextMcell.cell;
    } /* end chain loop */
    
    /*
     * Now there's a page open, with pRecord pointing to the last
     * record with extents for this tag.
     *
     * Find the vol/lbn for the last page to determine where we want
     * to grab space from.
     */
    switch (pRecord->type) {
	case BSR_XTNTS:
	    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
	    if (3 == domain->dmnVersion) {
		xCnt = 1;
		lastXtntLbn = -1;
		lastXtntSize = 0;
		nextPage = 0;
	    } else {
		xCnt = pXtnt->firstXtnt.xCnt;
	    
		lastXtntLbn = pXtnt->firstXtnt.bsXA[xCnt - 2].vdBlk;
		lastXtntSize = (pXtnt->firstXtnt.bsXA[xCnt - 1].bsPage -
				pXtnt->firstXtnt.bsXA[xCnt - 2].bsPage);
		nextPage = pXtnt->firstXtnt.bsXA[xCnt - 1].bsPage;
	    }
	    break;
	case BSR_XTRA_XTNTS:
	    pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
	    xCnt = pXtraXtnt->xCnt;
	    
	    lastXtntLbn = pXtraXtnt->bsXA[xCnt - 2].vdBlk;
	    lastXtntSize = (pXtraXtnt->bsXA[xCnt - 1].bsPage -
			    pXtraXtnt->bsXA[xCnt - 2].bsPage);
	    nextPage = pXtraXtnt->bsXA[xCnt - 1].bsPage;
	    break;
	case BSR_SHADOW_XTNTS:
	    pShadowXtnt = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));
	    xCnt = pShadowXtnt->xCnt;
	    
	    lastXtntLbn = pShadowXtnt->bsXA[xCnt - 2].vdBlk;
	    lastXtntSize = (pShadowXtnt->bsXA[xCnt - 1].bsPage -
			    pShadowXtnt->bsXA[xCnt - 2].bsPage);
	    nextPage = pShadowXtnt->bsXA[xCnt - 1].bsPage;
	    break;
	default:
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_479, 
			     "Internal Error: Non-extent record in append page.\n"));
	    return FAILURE;
	    break;
    }

    /*
     * nextVol and nextLbn point to the next page that we hope is free.
     * If not, they will get reset to point to a free page.
     */
    nextVol = currVol;
    nextLbn = lastXtntLbn + (BLOCKS_PER_PAGE * lastXtntSize);
    
    /*
     * If the last extent is a hole, it will be dealt with later.
     */
    if ((LBNT)-1 == lastXtntLbn) {
	*foundPage = FALSE;
    } else {
	*foundPage = is_page_free(domain, nextVol, nextLbn);
    }
    
    if (TRUE == *foundPage) {
	/*
	 * The page immediately following the last page of the file is
	 * free.  This is the easiest case to deal with.  Extend the
	 * final extent page by 1, and tell the SBM it's been used.
	 */
	switch (pRecord->type) {
	    case BSR_XTNTS:
		assert(3 != domain->dmnVersion);
		pXtnt->firstXtnt.bsXA[xCnt - 1].bsPage += 1;
		break;
	    case BSR_XTRA_XTNTS:
		pXtraXtnt->bsXA[xCnt - 1].bsPage += 1;
		break;
	    case BSR_SHADOW_XTNTS:
		pShadowXtnt->bsXA[xCnt - 1].bsPage += 1;
		break;
	    default:
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_480,
				 "Internal Error: Record type changed in append page.\n"));
		return FAILURE;
		break;
	}
	done = TRUE;
    } else {
	/*
	 * The page following the last page of the file was not free.  Now
	 * search the current mcell's volume for a free page.  Start early
	 * and work toward the end so that if fixfdmn has to grab more pages
	 * for this file, the following page will hopefully be free also.
	 */
	for (nextLbn = 0 ; /* TODO: 32? 112???? */
	     nextLbn <= domain->volumes[nextVol].volSize - BLOCKS_PER_PAGE ;
	     nextLbn += BLOCKS_PER_PAGE)
	{
	    if (TRUE == is_page_free(domain, nextVol, nextLbn)) {
		*foundPage = TRUE;
		break;
	    }
	}
    }
    
    if (FALSE == *foundPage) {
	/*
	 * Nothing free on this volume, walk through all volumes looking
	 * for a free page, except the current one.
	 */
	for (nextVol = 1 ; nextVol <= domain->highVolNum ; nextVol++) {
	    if (-1 == domain->volumes[nextVol].volFD ||
		nextVol == currVol) {
		/*
		 * Not a valid volume, or it has already been checked.
		 */
		continue;
	    }
	    
	    for (nextLbn = 0 ; /* TODO: 32?  112???? */
		 nextLbn <= domain->volumes[nextVol].volSize - BLOCKS_PER_PAGE;
		 nextLbn += BLOCKS_PER_PAGE)
	    {
		if (TRUE == is_page_free(domain, nextVol, nextLbn)) {
		    *foundPage = TRUE;
		    break; /* out of LBN loop */
		}
	    }
	    
	    if (TRUE == *foundPage) {
		break; /* out of volume loop */
	    }
	} /* End for volume loop */
    } /* End if page not on desired volume. */
    
    if (FALSE == *foundPage) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_481, 
			 "No free pages in this domain.\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_482,
			 "Can't add page to tag %d.%x in fileset %s.\n"),
		 tag->tagSeq.num, tag->tagSeq.seq, fileset->fsName);
	return SUCCESS;
    }
    
    if (nextVol == currVol && FALSE == done) {
	/*
	 * Page is on this volume, try to add extent to this mcell.
	 */
	switch (pRecord->type) {
	    case BSR_XTNTS:
		if (4 <= domain->dmnVersion && xCnt < BMT_XTNTS) {
		    pXtnt->firstXtnt.xCnt += 1;
		    xCnt += 1;
		    pXtnt->firstXtnt.bsXA[xCnt - 2].vdBlk = nextLbn;
		    pXtnt->firstXtnt.bsXA[xCnt - 1].bsPage =
			pXtnt->firstXtnt.bsXA[xCnt - 2].bsPage + 1;
		    pXtnt->firstXtnt.bsXA[xCnt - 1].vdBlk = -1;
		    done = TRUE;
		}
		break;
	    case BSR_XTRA_XTNTS:
		if (xCnt < BMT_XTRA_XTNTS) {
		    pXtraXtnt->xCnt += 1;
		    xCnt += 1;
		    pXtraXtnt->bsXA[xCnt - 2].vdBlk = nextLbn;
		    pXtraXtnt->bsXA[xCnt - 1].bsPage =
			pXtraXtnt->bsXA[xCnt - 2].bsPage + 1;
		    pXtraXtnt->bsXA[xCnt - 1].vdBlk = -1;
		    done = TRUE;
		}
		break;
	    case BSR_SHADOW_XTNTS:
		if (xCnt < BMT_SHADOW_XTNTS) {
		    pShadowXtnt->xCnt += 1;
		    xCnt += 1;
		    pShadowXtnt->bsXA[xCnt - 2].vdBlk = nextLbn;
		    pShadowXtnt->bsXA[xCnt - 1].bsPage =
			pShadowXtnt->bsXA[xCnt - 2].bsPage + 1;
		    pShadowXtnt->bsXA[xCnt - 1].vdBlk = -1;
		    done = TRUE;
		}
		break;
	    default:
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_483,
				 "Internal Error: record type changed in append page.\n"));
		return FAILURE;
	}
    } /* end if new page on same volume as old page */
    
    if (FALSE == done) {
	/*
	 * Need to get a new mcell, look at head of BMT free mcell list.
	 */
	if (3 == domain->dmnVersion) {
	    headFreePageNum = 1;
	} else {
	    headFreePageNum = 0;
	}
	status = convert_page_to_lbn(domain->volumes[nextVol].volRbmt->bmtLBN,
				     domain->volumes[nextVol].volRbmt->bmtLBNSize,
				     headFreePageNum, &headFreeLbn,
				     &headFreeVol);
	if (SUCCESS != status) {
	    return status;
	}
	
	if (headFreeVol == currVol && headFreeLbn == currLbn) {
	    /*
	     * We've already got it open and don't want to open it again.
	     */
	    headFreePage = page;
	} else {
	    /*
	     * Read first BMT page to get head of free mcell list.
	     */
	    status = read_page_from_cache(headFreeVol, headFreeLbn,
					  &headFreePage);
	    if (SUCCESS != status) {
		return status;
	    }
	}
	
	headFreeBmtPage = (bsMPgT *)headFreePage->pageBuf;
	pHeadFreeMcell = &(headFreeBmtPage->bsMCA[0]);
		       
	/*
	 * Now loop through records looking for BSR_MCELL_FREE_LIST
	 */
	pHeadFreeRecord = (bsMRT *)pHeadFreeMcell->bsMR0;
	freePageNum = -1; /* TODO: is this domain version dependent? */

	while ((0 != pHeadFreeRecord->type) &&
	       (0 != pHeadFreeRecord->bCnt) &&
	       (pHeadFreeRecord < ((bsMRT *) &(pHeadFreeMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * BSR_MCELL_FREE_LIST is the only type we care about.
	     */
	    if (BSR_MCELL_FREE_LIST == pHeadFreeRecord->type) {
		headRecord = (bsMcellFreeListT *)((char *)pRecord +
						  sizeof(bsMRT));
		freePageNum = headRecord->headPg; /* TODO: 0 when none free? */
		break;
	    }
	    /*
	     * Point to the next record.
	     */
	    pHeadFreeRecord = (bsMRT *)(((char *)pHeadFreeRecord) +
				    roundup(pHeadFreeRecord->bCnt,
					    sizeof(int)));
	} /* end record loop */

	assert(-1 != freePageNum); /* TODO: is this domain version dependent?*/

	/*
	 * Now read the first BMT page that has a free mcell.
	 */
	status = convert_page_to_lbn(domain->volumes[headFreeVol].volRbmt->bmtLBN,
				     domain->volumes[headFreeVol].volRbmt->bmtLBNSize,
				     freePageNum, &freeLbn, &freeVol);
	if (SUCCESS != status) {
	    return status;
	}

	if (freeVol == currVol && freeLbn == currLbn) {
	    freePage = page;
	}
	else if (freeVol == headFreeVol && freeLbn == headFreeLbn) {
	    freePage = headFreePage;
	    /*
	     * Need to set this to fake out release page, because the
	     * headFreePage is the only one which might not be modified.
	     */
	    headFreeModified = TRUE;
	}
	else {
	    status = read_page_from_cache(freeVol, freeLbn,
					  &freePage);
	    if (SUCCESS != status) {
		return status;
	    }
	}

	freeBmtPage = (bsMPgT *)freePage->pageBuf;

	if (0 == freeBmtPage->freeMcellCnt) {
	    /*
	     * No free mcells on this volume.
	     * FUTURE: Add a new page to the BMT on this volume, or
	     * search other volumes for a free mcell.
	     *
	     * For now, pretend the whole domain is full.
	     */
	    if ((freeVol != headFreeVol || freeLbn != headFreeLbn) &&
		(freeVol != currVol     || freeLbn != currLbn))
	    {
		status = release_page_to_cache(freeVol, freeLbn, freePage,
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    }
	    if (headFreeVol != currVol || headFreeLbn != currLbn) {
		status = release_page_to_cache(headFreeVol, headFreeLbn,
					       headFreePage, FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    }
	    status = release_page_to_cache(currVol, currLbn,
					   page, FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    *foundPage = FALSE;
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_484,
			     "Can't add page because there are no free mcells.\n"));
	    return SUCCESS;
	}

	assert(freePageNum == freeBmtPage->nextfreeMCId.page);

	freeMcellNum = freeBmtPage->nextfreeMCId.cell;
	pFreeMcell = &(freeBmtPage->bsMCA[freeMcellNum]);
	assert(0 == pFreeMcell->tag.num);

	/*
	 * Now take this mcell off the free list for this page.
	 */
	freeBmtPage->nextfreeMCId.page = pFreeMcell->nextMCId.page;
	freeBmtPage->nextfreeMCId.cell = pFreeMcell->nextMCId.cell;
	freeBmtPage->freeMcellCnt--;

	if (0 == freeBmtPage->freeMcellCnt) {
	    /*
	     * That was the last free mcell on this page - remove this page
	     * from the free mcell page list.
	     */
	    headRecord->headPg = freeBmtPage->nextFreePg;
	    freeBmtPage->nextFreePg = 0;
	    headFreeModified = TRUE;
	}

	/*
	 * Got the new mcell.  Set the previous one to point to it.
	 */
	if (BSR_XTNTS == pRecord->type) {
	    assert(0 == pXtnt->chainVdIndex);
	    pXtnt->chainVdIndex   = freeVol;
	    pXtnt->chainMCId.page = freePageNum;
	    pXtnt->chainMCId.cell = freeMcellNum;
	} else {
	    assert(0 == pMcell->nextVdIndex);
	    pMcell->nextVdIndex   = freeVol;
	    pMcell->nextMCId.page = freePageNum;
	    pMcell->nextMCId.cell = freeMcellNum;
	}

	/*
	 * Now initialize the new mcell.
	 */
	pNewMcell = pFreeMcell;

	pNewMcell->bfSetTag.num  = pMcell->bfSetTag.num;
	pNewMcell->bfSetTag.seq  = pMcell->bfSetTag.seq;
	pNewMcell->tag.num       = pMcell->tag.num;
	pNewMcell->tag.seq       = pMcell->tag.seq;
	pNewMcell->nextVdIndex   = 0;
	pNewMcell->nextMCId.page = 0;
	pNewMcell->nextMCId.cell = 0;

	pNewRecord = (bsMRT *)pNewMcell->bsMR0;
	pNewRecord->type = BSR_XTRA_XTNTS;
	pNewRecord->bCnt = 264;
	/* TODO: Anything else set in pNewRecord? */
	pNewXtra = (bsXtraXtntRT *)((char *)pNewRecord + sizeof(bsMRT));
	pNewXtra->blksPerPage = BLOCKS_PER_PAGE;
	pNewXtra->xCnt = 2;
	pNewXtra->bsXA[0].bsPage = nextPage;
	pNewXtra->bsXA[0].vdBlk  = nextLbn;
	pNewXtra->bsXA[1].bsPage = nextPage + 1;
	pNewXtra->bsXA[1].vdBlk  = -1;

	/*
	 * Now set the trailing record.
	 */
	pNewRecord = (bsMRT *) (((char *)pNewRecord) + 
				roundup(pNewRecord->bCnt, sizeof(int)));
	pNewRecord->type = 0;
	pNewRecord->bCnt = 0;
	/* TODO: Anything else set in pNewRecord? */

	/*
	 * Release the pages grabbed to find the new mcell.
	 */
	if ((freeVol != headFreeVol || freeLbn != headFreeLbn) &&
	    (freeVol != currVol     || freeLbn != currLbn))
	{
	    status = release_page_to_cache(freeVol, freeLbn, freePage,
					   TRUE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	}
	if (headFreeVol != currVol || headFreeLbn != currLbn) {
	    status = release_page_to_cache(headFreeVol, headFreeLbn,
					   headFreePage,
					   headFreeModified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	}

	/*
	 * Final step in allocating new mcell is to increment mcellCnt.
	 */

	/*
	 * Walk the BMT primary next chain looking for the BSR_XTNTS record.
	 */
	currentMcell.vol  = tag->tagPmcell.vol;
	currentMcell.page = tag->tagPmcell.page;
	currentMcell.cell = tag->tagPmcell.cell;
	foundBsrXtntsRecord = FALSE;

	/*
	 * Loop through primary next list.
	 */
	while (0 != currentMcell.vol && FALSE == foundBsrXtntsRecord) {
	    volume = &(domain->volumes[currentMcell.vol]);
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize,
				     currentMcell.page, &primLbn, &primVol);
	    if (SUCCESS != status) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
				 "Can't convert BMT page %d to LBN.\n"),
			 currentMcell.page);
		return FAILURE;
	    }

	    if (primVol == currVol && primLbn == currLbn) {
		primPage = page;
	    } else {
		status = read_page_from_cache(primVol, primLbn, &primPage);
		if (SUCCESS != status) {
		    return status;
		}
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
		    foundBsrXtntsRecord = TRUE;
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		    /*
		     * Increment mcellCnt - the whole point of these loops.
		     */
		    pXtnt->firstXtnt.mcellCnt += 1;
		    break;  /* break out of record loop */
		}
	    
		/*
		 * Point to the next record.
		 */
		pRecord = (bsMRT *)(((char *)pRecord) +
				    roundup(pRecord->bCnt, sizeof(int)));
	    } /* end record loop */
	
	    /*
	     * If the BSR_XTNTS record has not been found yet,
	     * release this page.
	     */
	    if (FALSE == foundBsrXtntsRecord &&
		(primVol != currVol || primLbn != currLbn))
	    {
		status = release_page_to_cache(primVol, primLbn, primPage,
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    }
	
	    /*
	     * Reset pointers for the next pass.
	     */
	    currentMcell.vol  = nextMcell.vol;
	    currentMcell.page = nextMcell.page;
	    currentMcell.cell = nextMcell.cell;
	} /* end loop on primary next list */

	assert(TRUE == foundBsrXtntsRecord);

	if (primVol != currVol || primLbn != currLbn) {
	    status = release_page_to_cache(primVol, primLbn, primPage,
					   TRUE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	}
	/*
	 * Else this page will be release modified below.
	 */
	
    } /* end if new mcell needed */

    /*
     * Fake an mcell and extent array to pass to collect_sbm_info.
     */
    fakeMcell.vol = nextVol;
    fakeMcell.page = 1;
    fakeMcell.cell = 1;

    fakeExtentArray[0].volume = nextVol;
    fakeExtentArray[0].lbn    = nextLbn;
    fakeExtentArray[0].page   = 0;
    fakeExtentArray[1].volume = nextVol;
    fakeExtentArray[1].lbn    = (LBNT)-1;
    fakeExtentArray[1].page   = 1;

    status = collect_sbm_info(domain, fakeExtentArray, 2,
			      tag->tagSeq.num,
			      fileset->filesetId.dirTag.num,
			      &fakeMcell, 0);
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Release the BMT page we're working on.
     */
    status = release_page_to_cache(currVol, currLbn, page, TRUE, FALSE);
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Increment the FS_STAT record's file size by pagesize.
     */
    tag->size += PAGESIZE;
    status = correct_fsstat(fileset, tag);
    if (SUCCESS != status) {
	return status;
    }

    writemsg(SV_VERBOSE | SV_LOG_FIXED,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_485,
		     "Added page (%d.%d) to tag %d.%x in fileset %s.\n"),
	     nextVol, nextLbn,
	     tag->tagSeq.num, tag->tagSeq.seq, fileset->fsName);

    return SUCCESS;
} /* end append_page_to_file */


/*
 * Function Name: free_orphan (3.nn)
 *
 * Description: This function will free an orphan and any mcells that
 *              might be part of the orphan's chain(s).  This function
 *              is recursive as the chains we are following could be
 *              very corrupt and have multiple 'nextChains'.
 *             
 *
 * Input parameters:
 *   domain : The domain we are working on.
 *   volNum : The mcell's volume number
 *   pageNum: The mcell's page number
 *   mcellNum: The mcell's number.
 *
 * Return Values: SUCCESS, FAILURE, NO_MEMORY
 */
int free_orphan(domainT *domain, int volNum, int pageNum, int mcellNum)
{
    char *funcName = "free_orphan";
    int status;
    LBNT currLbn;
    int currVol;
    int nextVol;
    int nextPage;
    int nextMcell;
    int chainVol;
    int chainPage;
    int chainMcell;
    pageT      *pPage;
    bsMPgT     *pBMTPage;  /* The current BMT page we are working on */
    bsMCT      *pMcell;    /* The mcell we are working on. */
    bsMRT      *pRecord;   /* The record we are working on. */
    volumeT    *volume;
    bsXtntRT   *pXtnt;
    mcellNodeT *mcell;
    mcellNodeT tempMcell;
    nodeT      *node;

    /*
     * Init variables.
     */
    volume = &(domain->volumes[volNum]);
    chainVol   = 0;
    chainPage  = 0;
    chainMcell = 0;

    if ((volNum < 1) || 
	(volNum > MAX_VOLUMES) ||
	(-1 == domain->volumes[volNum].volFD)) {
	/*
	 * Invalid volume, so no need to do anything.
	 */
	return SUCCESS;
    }

    /*
     * Check if this mcell exists.
     */
    tempMcell.mcellId.vol  = volNum;
    tempMcell.mcellId.page = pageNum;
    tempMcell.mcellId.cell = mcellNum;
    mcell = &tempMcell;

    status = find_node(domain->mcellList, (void **)&mcell, &node);
    if (SUCCESS != status) {
	if (NOT_FOUND == status) {
	    return SUCCESS;
	} else {
	    return status;
	}
    }
    
    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_486, 
		     "Deleting mcell (%d,%d,%d), it is an orphaned mcell.\n"), 
	     volNum, pageNum, mcellNum);

    /*
     * Now that we know them mcell exists, find the page contains it.
     */
    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				 volume->volRbmt->bmtLBNSize, 
				 pageNum, &currLbn, &currVol);
    if (SUCCESS != status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			 "Can't convert BMT page %d to LBN.\n"), 
		 pageNum);
	return FAILURE;
    }

    status = read_page_from_cache(currVol, currLbn, &pPage);
    if (SUCCESS != status) {
	return status;
    } 
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;

    /*
     * Save the next chain.
     */
    nextVol   = pMcell->nextVdIndex;
    nextPage  = pMcell->nextMCId.page;
    nextMcell = pMcell->nextMCId.cell;

    /*
     * Walk the records to see if it has a chain chain.
     */
    while ((0 != pRecord->type) &&
	   (0 != pRecord->bCnt) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	if (BSR_XTNTS == pRecord->type) {
	    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		  
	    chainVol   = pXtnt->chainVdIndex;
	    chainPage  = pXtnt->chainMCId.page;
	    chainMcell = pXtnt->chainMCId.cell;

	} /* end if BSR_XTNT */
	
	/*
	 * Set pRecord to next record  of pMcell.
	 */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(int))); 
    } /* end of walking the records */

    /*
     * Now we can finally free this mcell.
     */
    status = free_mcell(domain, volNum, pPage, mcellNum);
    if (SUCCESS != status) {
	return status;
    }
    /*
     * Release page
     */
    status = release_page_to_cache(currVol, currLbn, 
				   pPage, TRUE, FALSE);
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Now that page is released (to stop deadlock), free its pointers.
     */
    status = free_orphan(domain, nextVol, nextPage, nextMcell);
    if (SUCCESS != status) {
	return status;
    }
    
    status = free_orphan(domain, chainVol, chainPage, chainMcell);
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
} /* end free_orphan */


/*
 * Function Name: correct_chain_inherit_attr
 *
 * Description:	 This is called when you can't find the 
 *	         BSR_BF_INHERIT_ATTR record on a directory.
 *		 It then adds one or deletes the tag.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     primaryMcell: The information needed to locate the primary mcell.
 *     tagNum: The tag we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, DELETED, FAILURE, NO_MEMORY
 */
int correct_chain_inherit_attr(filesetT *fileset,
			       mcellT primaryMcell, 
			       int tagNum)
{
    char    *funcName = "correct_chain_inherit_attr";
    int	    modified;
    int	    status;
    int	    currVol;
    LBNT     currLbn;
    int	    bytesUsed;
    int     currentTagNum;
    int     currentSeqNum;
    int     x;
    mcellT  currentMcell;     /* The mcell currently being worked on */
    mcellT  nextMcell;	      /* The next mcell on the next chain */
    volumeT *volume;
    pageT   *pCurrentPage;    /* The current page we are checking */
    bsMPgT  *bmtPage;	  /* The current BMT page we are working on */
    bsMCT   *pMcell;	  /* The current BMT mcell we are working on */
    bsMRT   *pRecord;
    domainT *domain;

    /*
     * Init variables.
     */
    domain = fileset->domain;
    modified = FALSE;

    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_924,
		     "Fixing BSR_BF_INHERIT_ATTR record on tag %d's mcell chain.\n"), 
	     tagNum);

    /*
     * Can we create/find an mcell to hold the inherit record 
     * Walk the main chain looking for space in one of its mcells
     * if an mcell has space then add a default inherit record to
     * it otherwise delete the tag.
     */
    
    /*
     * Start at primary
     */
    currentMcell.vol  = primaryMcell.vol;
    currentMcell.page = primaryMcell.page;
    currentMcell.cell = primaryMcell.cell;

    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (0 != currentMcell.vol) {
	bytesUsed = 0;

	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currentMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, &currLbn, 
				     &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage	   = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	   = &(bmtPage->bsMCA[currentMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;
	currentTagNum  = pMcell->tag.num;
	currentSeqNum  = pMcell->tag.seq;

	/*
	 * Now loop through primary's next records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    bytesUsed += roundup(pRecord->bCnt,(sizeof(int)));
	    pRecord = (bsMRT *) (((char *)pRecord) + roundup(pRecord->bCnt, 
							     sizeof(int))); 
	} /* end record loop */
	
	/*
	 * At this point pRecord points to the end of the mcell.
	 */
	if (BS_USABLE_MCELL_SPACE > (bytesUsed + sizeof(bsBfInheritAttrT))) {
	    /*	
	     * We have space in this mcell to add the inherit record.
	     */
	    bsMRT    mcellRecordHeader;
	    bsBfInheritAttrT newInherit;
	    bsBfInheritAttrT *pInherit;

	    /*
	     * First load the mcell record header.
	     */
	    mcellRecordHeader.bCnt = sizeof(bsMRT) + sizeof(bsBfInheritAttrT);
	    mcellRecordHeader.type = BSR_BF_INHERIT_ATTR;
	    mcellRecordHeader.version = 0;
	    
	    /*
	     * memcpy the record header in.
	     */
	    memcpy((char *)pRecord, &mcellRecordHeader, sizeof(bsMRT));

	    /*
	     * Now load the newInherit structure with default values.
	     */
	    newInherit.dataSafety  = BFD_NIL;
	    newInherit.reqServices = 1;
	    newInherit.optServices = 0;
	    newInherit.extendSize  = 0;
	    newInherit.rsvd1       = 0;
	    newInherit.rsvd2       = 0;
	    newInherit.acl.num     = 0;
	    newInherit.acl.seq     = 0;
	    newInherit.rsvd_sec1   = 0; 
	    newInherit.rsvd_sec2   = 0; 
	    newInherit.rsvd_sec3   = 0; 
	    for (x = 0; x < BS_CLIENT_AREA_SZ; x++) {
		newInherit.clientArea[x] = 0;
	    }

	    /*
	     * Now that we have loaded the structure we need to memcpy this
	     * out to the page.
	     */
	    pInherit = (bsBfInheritAttrT *)((char *)pRecord + 
					    sizeof(bsMRT));    

	    memcpy((char *)pInherit, &newInherit, 
		   sizeof(bsBfInheritAttrT));

	    /*
	     * Now we need to add a trailing mcell record header.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + roundup(pRecord->bCnt, 
							     sizeof(int))); 
	    mcellRecordHeader.bCnt = 0;
	    mcellRecordHeader.type = 0;
	    mcellRecordHeader.version = 0;
	    
	    /*
	     * memcpy the record header in.
	     */
	    memcpy((char *)pRecord, &mcellRecordHeader, sizeof(bsMRT));

	    status = add_fixed_message(&(pCurrentPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_925, 
					       "Modified tag %d.%x's mcell (%d,%d,%d), added BSR_BF_INHERIT_ATTR record.\n"),
				       currentTagNum, currentSeqNum,
				       currentMcell.vol,
				       currentMcell.page,
				       currentMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }

	    modified = TRUE;
	}

	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    /*
	     * We have fixed the problem so we don't need to continue.
	     */
	    return SUCCESS;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMcell.vol != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currentMcell.vol  = nextMcell.vol;
	    currentMcell.page = nextMcell.page;
	    currentMcell.cell = nextMcell.cell;
	} else {
	    currentMcell.vol  = 0;
	    currentMcell.page = 0;
	    currentMcell.cell = 0;
	}/* end if nextMcell exists */
    } /* end chain loop */

    /*
     * We were not able to find space to add the inherit record so 
     * we need to delete the tag.
     */
    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_926, 
		     "Deleting fileset %s tag %d, can't add an BSR_BF_INHERIT_ATTR record.\n"), 
	     fileset->fsName, tagNum);
    status = delete_tag(fileset, tagNum);
    if (SUCCESS != status) {
	return status;
    }
    return DELETED;
} /* end correct_chain_inherit_attr */


/*
 * Function Name: correct_bmt_chain_by_tag (3.74)
 *
 * Description: This is called after we have loaded all tags into the
 *		tag list. At this point we have already handled any
 *		loops which may exist, so we do not need to worry
 *		about getting into mcell loops. Also each individual
 *		mcell has already been checked. This function will
 *		check data which crosses mcells, such as extents and 
 *		proplists. 
 *
 *	        This function will follow a tag's nextMcell chain as
 *	        well as its chainMcell chain. This function most
 *	        likely will be very slow, as it will need to load each
 *	        mcell on the chains, and depending on how the mcells 
 *	        are distributed on disk this could mean reading a new 
 *	        page for each mcell.
 *
 *		A large portion of this code will involve pattern
 *		matching. For example: with a 3 mcell chain, and the
 *		first and the third mcell both say they belong to
 *		mcell 42, but the middle ones says it belong to
 *		mcell 2000, we will assume that the 2nd mcell's tag
 *		number has become corrupt and fix it. (ie [42] ->
 *		[2000] -> [42] becomes [42] -> [42] -> [42]).
 *
 *		There are some cases where we will not be able to match
 *		patterns and will have to take an educated guess on
 *		what is correct. For example: with a 3 mcell chain,
 *		the first mcell says it belongs to 42, the second
 *		belongs to 2000, the third says it belongs to 13. At
 *		this point we don't know if the corruption is in the
 *		tag number, or is the chain pointing to an incorrect
 *		mcell. Unless there are other patterns we can locate
 *		we will have to take a best guess to this one, which
 *		might mean truncating the mcell chain.	(ie [42] -> 
 *		[2000] -> [13] is truncated to just a single mcell
 *		[42])
 *
 * NOTE: must be called on tag 1 of the fileset before called on any
 *       tag in the fileset.
 *
 * Input parameters:
 *     fileset: The fileset to be checked.
 *     pTagNode: The tag node we are working on.
 * 
 * Output Parameters:
 *
 * Returns: SUCCESS, FAILURE, FIXED, NO_MEMORY, CORRUPT
 */
int correct_bmt_chain_by_tag(filesetT *fileset, 
			     tagNodeT *pTagNode)
{
    char       *funcName = "correct_bmt_chain_by_tag";
    int	       status;
    int        redo;
    int        recNum;
    int	       modified;
    int        bmtModified;
    int	       primTagNum;
    int        currTagNum;
    int        nextTagNum; 
    int	       primSeqNum;
    int        currSeqNum;
    int        nextSeqNum;
    int	       currVol;
    LBNT       currLbn;
    int	       prevVol;
    LBNT       prevLbn;
    int	       odsVersion;
    int	       lastXtntEntry; /* the xtnt we are working on */
    int	       noXtnts;
    int        lastExtentPage;
    int        byteCount;
    mcellT     prevMcell;
    mcellT     currMcell;     /* The mcell currently being worked on */
    mcellT     nextMcell;     /* The next mcell on the next chain */
    mcellT     primaryMcell;  /* Used to store the primary mcell of the tag */
    mcellT     chainMcell;    /* The next mcell on the chain */
    mcellT     xtntMcell;     /* The mcell which contains the xtnt record*/
    pageT      *pCurrPage;    /* The current page we are checking */
    pageT      *pPrevPage;    /* The previous page we are checking */
    domainT    *domain;
    volumeT    *volume;
    bsMPgT     *bmtPage;     /* The current BMT page we are working on */
    bsMPgT     *bmtPrevPage; /* The previous BMT page we are working on */
    bsMCT      *pMcell;	     /* The current BMT mcell we are working on */
    bsMCT      *pPrevMcell;  /* The previous  BMT mcell we are working on */
    bsMRT      *pRecord;
    tagNodeT   *pNextTagNode;
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;
    nodeT      *nextNode;
    nodeT      *tmpNode;
    tagNodeT   tmpTagNode;
    tagNodeT   *pTempTagNode;
    statT      *pStat;
    statT      alignedStat; /* needed to fix alignment errors */
    chainFoundT found;

    /*
     * Special case: Don't want to process clone primary mcells if the 
     * mcell is also in the parent fileset.
     */
    if (FS_IS_CLONE(fileset->status)) {
	status = copy_clone_tag_data(fileset, pTagNode);
	if (SUCCESS != status) {
	    /*
	     * Should always return SUCCESS.
	     */
	    return status;
	}

	if (TRUE == T_IS_CLONE_ORIG_PMCELL(pTagNode->status)) {
	    return SUCCESS;
	}
    } /* end if fileset is a clone */

    /*
     * Init variables
     */
    domain	= fileset->domain;
    modified	= FALSE;
    bmtModified	= FALSE;
    odsVersion	= domain->dmnVersion; 
    found.stripeCnt = 0;
    found.mcellCnt = 0;
    found.pageCnt  = 0;

    primaryMcell.vol  = pTagNode->tagPmcell.vol;
    primaryMcell.page = pTagNode->tagPmcell.page;
    primaryMcell.cell = pTagNode->tagPmcell.cell;

    primTagNum = pTagNode->tagSeq.num;
    primSeqNum = pTagNode->tagSeq.seq;
    currTagNum = primTagNum;
    currSeqNum = primSeqNum;

    /*
     * Check all the next and xtnt chains for this tag.
     *
     * NOTE: this should be called before all the other checks 
     * in this function.
     */
    status = correct_tag_chains(fileset, pTagNode);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    bmtModified = TRUE;
	} else {
	    return status;
	}
    }

    /*
     * Check the tag and set for all the mcells in this tag.
     */
    status = correct_tag_and_set(fileset, pTagNode);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    bmtModified = TRUE;
	} else {
	    return status;
	}
    }

    /*
     * Check the primary mcell chain.
     *
     * This also locates the start of the chain mcell chain
     */
    status = correct_bmt_primary_chain(fileset, pTagNode, &chainMcell, 
				       &xtntMcell, &found);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    bmtModified = TRUE;
	} else {
	    return status;
	}
    }

    /*
     * Check the extent mcell chain.
     */
    redo = TRUE;
    while (redo) {
	redo = FALSE;

	status = correct_bmt_extent_chain(fileset, pTagNode, xtntMcell,
					  &chainMcell, &found);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		bmtModified = TRUE;
	    } else if (RESTART == status) {
		bmtModified = TRUE;
		redo = TRUE;
	    } else {
		return status;
	    }
	}
    } /* end while redo */

    noXtnts = FALSE;
    if (0 == found.pageCnt) {
	noXtnts = TRUE;
    }

    /*
     * Check the xtnt chain's mcell count.
     */
    status = correct_chain_mcell_count(fileset, chainMcell, xtntMcell,
				       found.mcellCnt, found.isStriped);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    bmtModified = TRUE;
	} else {
	    return status;
	}
    }

    /*
     * Check frag related stuff, which couldn't be done earlier.
     */
    status = correct_chain_frag(fileset, pTagNode, chainMcell, 
				found.pageCnt, found.foundFsData, noXtnts);
    if (SUCCESS != status) {
	if (FIXED == status) {
	    bmtModified = TRUE;
	} else {
	    return status;
	}
    }

    /*
     * If proplist exists check and fix it.
     */
    if (TRUE == found.foundPropList) {
	status = correct_chain_proplist(fileset, primaryMcell, 
					currTagNum);
	if (FIXED == status) {
	    bmtModified = TRUE;
	} else if (SUCCESS != status) {
	    return status;
	} 
    } /* end if foundPropList */
	
    /*
     * Special case for the frag file (tag 1).
     */
    if (1 == currTagNum) {
	/*
	 * Populate the fragLBN array and frag slot array.
	 */
	writemsg(SV_VERBOSE | SV_LOG_INFO,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_428,
			 "Checking fileset %s's frag file group headers.\n"),
		 fileset->fsName);
	/* 
	 * Set up the arrays needed for frag file checking.
	 */
	status = set_frag_file_arrays(fileset, domain);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_429,
			     "Error setting frag file array on fileset %s.\n"),
		     fileset->fsName);
	    failed_exit();
	}	
    } else {
	/*
	 * All tags except for frag file (tag 1).
	 */
	int deleted = FALSE;

	/*
	 * Handle the case where tag does not have required records.
	 *
	 * WARNING : Do not do anything after these checks which
	 * will require the tag node, as it may not exist.
	 */
	if ((FALSE == found.foundBsrXtnts) && (FALSE == found.foundFsData)) {
	    /*
	     * From testing the only file type I could find that doesn't
	     * have an xtnt record is a symbolic link.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_430,
			     "Deleting fileset %s's tag %d, missing BSR_XTNT record.\n"),
		     fileset->fsName, currTagNum);

	    status = delete_tag(fileset, currTagNum);
	    if (SUCCESS != status) {
		return status;
	    }
	    deleted = TRUE;
	    bmtModified = TRUE;
	}
 
	if ((FALSE == deleted) &&  /* already deleted */
	    (3 != currTagNum) &&   /* tag 3 (.tags) is a special case */
	    (FALSE == found.foundInherit) &&
	    (TRUE  == S_ISDIR( pTagNode->fileType))) {
	    /*
	     * This call may delete the tag node.   
	     */
	    status = correct_chain_inherit_attr(fileset, primaryMcell, currTagNum);
	    if (SUCCESS != status) {
		if (DELETED == status) {
		    deleted = TRUE;
		} else {
		    return status;
		}
	    }
	    bmtModified = TRUE;
	}

	if ((FALSE == deleted) && 
	    (FALSE == found.foundFsStat)) {
	    /*
	     * This call may delete the tag node.   
	     */
	    status = correct_chain_fsstat(fileset, primaryMcell, currTagNum);
	    if (SUCCESS != status) {
		if (DELETED == status) {
		    deleted = TRUE;
		} else {
		    return status;
		}
	    }
	    bmtModified = TRUE;
	} /* end if no fsstat record */
    } /* end if/else tag 1 */

    if (TRUE == bmtModified) {
	return FIXED;
    }	
    return SUCCESS;
} /* end correct_bmt_chain_by_tag */


/*
 * Function Name: copy_clone_tag_data
 *
 * Description:	This is called on clone tags to determine
 *	if the clones shares its primary mcell with the
 *	original, and if it does, copy the tag information
 *	from the parent to the clone.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     pTagNode: The tag node we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, NOT_FOUND, FAILURE
 */
int copy_clone_tag_data(filesetT *fileset, 
			tagNodeT *pTagNode)
{
    char       *funcName = "copy_clone_tag_data";
    int status;
    domainT    *domain;
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;
    nodeT      *tmpNode;
    tagNodeT   tmpTagNode;
    tagNodeT   *pTempTagNode;

    /*
     * Init variables
     */
    domain = fileset->domain;

    /*
     * Check if this mcell belongs to the parent.
     */
    tmpMcellNode.mcellId.vol  = pTagNode->tagPmcell.vol;
    tmpMcellNode.mcellId.page = pTagNode->tagPmcell.page;
    tmpMcellNode.mcellId.cell = pTagNode->tagPmcell.cell;
    pMcellNode = &tmpMcellNode;

    status = find_node(domain->mcellList, (void **)&pMcellNode, &tmpNode);
    if (SUCCESS != status) {
	/*
	 * Should always be found.
	 */
	return status;
    }

    /*
     * Now check this mcell's setTag to see if it belongs
     * to the parent fileset or the clone fileset.
     */
    if (fileset->origCloneTag.num == pMcellNode->setTag.num) {
	/*
	 * This primary belongs to the parent fileset.
	 */
	T_SET_CLONE_ORIG_PMCELL(pTagNode->status);
	
	/*
	 * Now copy the original file's tagNode data into this tagNode.
	 */
	tmpTagNode.tagSeq.num = pTagNode->tagSeq.num;
	pTempTagNode = &tmpTagNode;
	status = find_node(fileset->origClonePtr->tagList,
			   (void **)&pTempTagNode, &tmpNode);
	if (SUCCESS != status) {
		/*
	     	 * Should always be found.
	     	 * If Not found delete the file in the clone		
	     	 */
	     if( status == NOT_FOUND) {		
	 	writemsg(SV_VERBOSE | SV_LOG_FOUND,
                	 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_251,
                         	 "Deleting fileset %s's tag %d.%x because there is no link and the fileset is a clone.\n"),
                 		fileset->fsName,pTagNode->tagSeq.num, pTagNode->tagSeq.seq);
        	status = delete_tag(fileset, pTagNode->tagSeq.num);
        	if (SUCCESS != status) {
            		return status;
        	}
	    	return NOT_FOUND;
	    }
	    return status;	
	
	}

	pTagNode->size	     = pTempTagNode->size;
	pTagNode->fileType   = pTempTagNode->fileType;
	pTagNode->pagesFound = pTempTagNode->pagesFound;
	pTagNode->fragSize   = pTempTagNode->fragSize;
	pTagNode->fragPage   = pTempTagNode->fragPage;
	pTagNode->fragSlot   = pTempTagNode->fragSlot;
	pTagNode->numLinks   = pTempTagNode->numLinks;
	pTagNode->parentTagNum.num = pTempTagNode->parentTagNum.num;
	pTagNode->parentTagNum.seq = pTempTagNode->parentTagNum.seq;
	if (T_IS_DIR_INDEX(pTempTagNode->status)) {
	    T_SET_DIR_INDEX(pTagNode->status);
	}
    }
    return SUCCESS;
} /* end copy_clone_tag_data */


/*
 * Function Name: correct_bmt_primary_chain
 *
 * Description:	This is called to check the primary mcell chain.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     pTagNode: The tag node we are working on.
 *
 * Output parameters:  
 *     chain: The start of the next chain mcell
 *     xtnt:  The start of the xtnt chain mcell
 *     found: statistics for this chain.
 *
 * Returns: SUCCESS, FIXED, FAILURE, NO_MEMORY
 */
int correct_bmt_primary_chain(filesetT *fileset, 
			      tagNodeT *pTagNode,
			      mcellT   *chain,
			      mcellT   *xtnt,
			      chainFoundT *found)
{
    char       *funcName = "correct_bmt_primary_chain";
    int        status;
    int	       modified;
    int        bmtModified;
    int        byteCount;
    int        recNum;
    int        reprocessRecord;
    int        odsVersion;
    int	       primTagNum;
    int	       primSeqNum;
    int	       currVol;
    LBNT       currLbn;
    int	       prevVol;
    LBNT       prevLbn; 
    int        foundBsrAttr;
    int        foundDirIndex;
    mcellT     primMcell;     /* Used to store primary mcell of the tag */
    mcellT     currMcell;     /* The mcell currently being worked on */
    mcellT     chainMcell;    /* The next mcell on the chain */
    mcellT     xtntMcell;     /* The mcell which contains the xtnt record*/
    mcellT     nextMcell;     /* The next mcell on the next chain */
    mcellT     prevMcell;     /* The previous mcell on the chain */
    domainT    *domain;
    volumeT    *volume;
    pageT      *pCurrPage;    /* The current page we are checking */
    pageT      *pPrevPage;    /* The previous page we are checking */
    bsMPgT     *bmtPage;      /* The current BMT page we are working on */
    bsMPgT     *bmtPrevPage;  /* The previous BMT page we are working on */
    bsMCT      *pMcell;	      /* The current BMT mcell we are working on */
    bsMCT      *pPrevMcell;   /* The previous  BMT mcell we are working on */
    bsMRT      *pRecord;
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;
    nodeT      *tmpNode;
    tagNodeT   tmpTagNode;
    tagNodeT   *pTempTagNode;
    statT      *pStat;
    statT      alignedStat;   /* needed to fix alignment errors */
    int        currLinkSegment;

    /*
     * Init variables
     */
    domain      = fileset->domain;
    odsVersion  = domain->dmnVersion;
    modified    = FALSE;
    bmtModified = FALSE;
    currLinkSegment = 0;

    /*
     * Initialize the temporary variables which will be
     * assigned to output parameters at the end of the function.
     */
    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    xtntMcell.vol  = 0;
    xtntMcell.page = 0;
    xtntMcell.cell = 0;
 
    /*
     * Need to know if we have a striped file, as striped extents are 
     * handled differently.
     */
    found->isStriped = FALSE;

    /*
     * Most files have an fsStat record.  We need a way to 
     * know if we have found one in the chain.
     */
    found->foundFsStat = FALSE;

    /*
     * A file can only have one BSR_XTNT or BMTR_FS_DATA record.
     */
    found->foundBsrXtnts = FALSE;
    found->foundFsData   = FALSE;

    foundBsrAttr = FALSE; /* This is only needed locally */
    foundDirIndex = FALSE; /* This is only needed locally */

    /*
     * Does the tag have any prop lists?
     */
    found->foundPropList = FALSE;

    /*
     * Directories must have an inherit record.
     */
    found->foundInherit = FALSE;

    /*
     * Initialize mcell pointers.  We are setting prevMcell to 
     * the currMcell to make the loop simpler.
     */
    primMcell.vol  = pTagNode->tagPmcell.vol;
    primMcell.page = pTagNode->tagPmcell.page;
    primMcell.cell = pTagNode->tagPmcell.cell;

    currMcell.vol  = primMcell.vol;
    currMcell.page = primMcell.page;
    currMcell.cell = primMcell.cell;

    prevMcell.vol  = currMcell.vol;
    prevMcell.page = currMcell.page;
    prevMcell.cell = currMcell.cell;

    primTagNum = pTagNode->tagSeq.num;
    primSeqNum = pTagNode->tagSeq.seq;

    /*
     * Loop until currMcell is empty [0,0,0]
     */
    while (0 != currMcell.vol) {
	/*
	 * Read the mcell page (first get LBN).
	 */
	volume = &(domain->volumes[currMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMcell.page, &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	if (0 == nextMcell.vol) {
	    /*
	     * End of next mcell chain.
	     */
	    nextMcell.vol  = 0;
	    nextMcell.page = 0;
	    nextMcell.cell = 0;
	} /* end if nextMcell.vol */

	/*
	 * Now loop through primary chain's records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	recNum  = 0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    bsXtntRT  *pXtnt;
	    bsBfAttrT *pBSRAttr;
	    bfTagT    *pDirIndex;

	    /*
	     * Init variables.
	     */
	    recNum++;
	    reprocessRecord = FALSE;
		
	    /*
	     * Multiple checks are done here.
	     */
	    switch (pRecord->type) {
		case BMTR_FS_DATA: /* 254 */
		    if (TRUE == found->foundFsData) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrPage,
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    found->foundFsData = TRUE;
		    break;

		case BMTR_FS_STAT: /* 255 */
		    if (TRUE == found->foundFsStat) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrPage,
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    found->foundFsStat = TRUE;

		    /*
		     * We are on the first fsstat record,
		     * This section was originally in correct_nodes
		     */
		    pStat = (statT *)((char *)pRecord + sizeof(bsMRT));

		    /*
		     * Need to align this struct or we get 
		     * unaligned error messages.
		     */
		    memcpy(&alignedStat, (char *)pStat, sizeof(statT));

		    /*
		     * At this point we have verified the tag number on
		     * the mcell which contains this fs_stat record.
		     * One of the fields in the record is st_ino which
		     * needs to match the tag number of this mcell.
		     */
		    if ((alignedStat.st_ino.num != pMcell->tag.num) ||
			(alignedStat.st_ino.seq != pMcell->tag.seq)) {
			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_418,
							   "Modified mcell (%d,%d,%d)'s st_ino from %d.%x to %d.%x.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   alignedStat.st_ino.num, 
						   alignedStat.st_ino.seq,
						   pMcell->tag.num,
						   pMcell->tag.seq);
			if (SUCCESS != status) {
			    return status;
			}

			alignedStat.st_ino.num = pMcell->tag.num;
			alignedStat.st_ino.seq = pMcell->tag.seq;
			/*
			 * Copy the aligned structure into the record.
			 */
			memcpy((char *)pStat, &alignedStat, sizeof(statT));
			modified = TRUE;
		    }

		    pTagNode->size = alignedStat.st_size;
		    pTagNode->fileType = alignedStat.st_mode;
		    pTagNode->fragSize = alignedStat.fragId.type;
		    pTagNode->fragSlot = alignedStat.fragId.frag;
		    pTagNode->fragPage = alignedStat.fragPageOffset;
		    pTagNode->numLinks = alignedStat.st_nlink;
		    pTagNode->parentTagNum.num = alignedStat.dir_tag.num;
		    pTagNode->parentTagNum.seq = alignedStat.dir_tag.seq;

		    /*
		     * collect_frag_info moved to later in this function.
		     */

		    break;
		    
		case BMTR_FS_INDEX_FILE: /* 250 */
		    /*
		     * All tags (except for directory indexes) should 
		     * have a FsStat record. To make the code simpler 
		     * directory indexes will be 'marked' as having a 
		     * fsStat record.
		     */
		    T_SET_DIR_INDEX(pTagNode->status);
		    found->foundFsStat = TRUE;
		    break;

		case BMTR_FS_DIR_INDEX_FILE: /* 249 */
		    if (TRUE == foundDirIndex) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrPage,
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    foundDirIndex = TRUE;
		    /*
		     * This directory has a dir index file, store its
		     * tag.seq number.
		     */
		    pDirIndex = (bfTagT *)((char *)pRecord + sizeof(bsMRT));
		    pTagNode->dirIndexTagNum.num = pDirIndex->num;
		    pTagNode->dirIndexTagNum.seq = pDirIndex->seq;
		    break;

		case BSR_XTNTS:
		    if (TRUE == found->foundBsrXtnts) {
			/*
			 * There should only be one bsrXtnt record 
			 * per tag.
			 */
			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_419, 
							   "Modified mcell (%d,%d,%d), removed an extra BSR_XTNT record.\n"),
						   currMcell.vol, 
						   currMcell.page,
						   currMcell.cell);
			if (SUCCESS != status) {
			    return status;
			}

			status = clear_mcell_record(domain,
						    pCurrPage,
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    } 
			
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    found->foundBsrXtnts = TRUE;
		    xtntMcell.vol = currMcell.vol;
		    xtntMcell.page = currMcell.page;
		    xtntMcell.cell = currMcell.cell;
			
		    if (BSXMT_STRIPE == pXtnt->type) {
			found->isStriped = TRUE;
		    }

		    if ((3 == odsVersion) || (TRUE == found->isStriped)) {
			/*	
			 * The extents in this record are only used in 
			 * BMT0 for ODSV3. The extent fields are not 
			 * initialized for mcells in normal BMT pages,
			 * we can safely ignore them.
			 *
			 * They are also not initialized for stripe files.
			 */
		    } else {
			if (pXtnt->firstXtnt.xCnt == 1) {
			    /*
			     * No extents stored in BS_XTNTS
			     */
			    found->pageCnt = 0;
			} else {
			    found->pageCnt = pXtnt->firstXtnt.bsXA[1].bsPage;
			}
			found->mcellCnt++;
		    }

		    /*
		     * Set chainMcell so we can follow it after we are
		     * done with the current chain.
		     */
		    chainMcell.vol  = pXtnt->chainVdIndex;
		    chainMcell.page = pXtnt->chainMCId.page;
		    chainMcell.cell = pXtnt->chainMCId.cell;

		    break;

		case BSR_PROPLIST_HEAD:
		case BSR_PROPLIST_DATA:
		    /*
		     * We will need to check this tags proplist.
		     */
		    found->foundPropList = TRUE;
		    break;
		
		case BSR_XTRA_XTNTS:
		case BSR_SHADOW_XTNTS:
		    /*
		     * Should not exist in primary chain.
		     */
		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_420,
						       "Modified mcell (%d,%d,%d), removed invalid record type %d.\n"), 
					       currMcell.vol, 
					       currMcell.page, 
					       currMcell.cell, 
					       pRecord->type);
		    if (SUCCESS != status) {
			return status;
		    }

		    status = clear_mcell_record(domain,
						pCurrPage, 
						currMcell.cell, 
						recNum);
		    if (SUCCESS != status) {
			return status;
		    }
		    reprocessRecord = TRUE;
		    modified = TRUE;
		    break;
		    
		case BSR_ATTR: /* 2 */
		    if (TRUE == foundBsrAttr) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrPage,
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    foundBsrAttr = TRUE;

		    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));
		    if (/*TRUE ==*/ pBSRAttr->deleteWithClone) {
			if (FALSE == FS_IS_ORIGFS(fileset->status)) {
			    /*
			     * This fileset does not have a clone, so the
			     * deleteWithCloneFlag should not be set.
			     */
			    status = add_fixed_message(&(pCurrPage->messages), BMT,
						       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_152, 
							       "Modified mcell (%d,%d,%d)'s bsrAttr deleteWithClone from %d to %d.\n"),
						       currMcell.vol,
						       currMcell.page,
						       currMcell.cell,
						       pBSRAttr->deleteWithClone,
						       0);
			    if (SUCCESS != status) {
				return status;
			    }

			    pBSRAttr->deleteWithClone = 0;
			    modified = TRUE;
			}
			else {
			    /*
			     * This file has been deleted, but its clone
			     * needs to keep its mcell data around.
			     */
			    T_SET_DELETE_WITH_CLONE(pTagNode->status);
			}
		    }
		    break;

		case BSR_BF_INHERIT_ATTR: /* 16 */
		    if (TRUE == found->foundInherit) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrPage,
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    found->foundInherit = TRUE;
		    break;

		default:
		    break;
	    } /* end switch */
	    if (FALSE == reprocessRecord) {
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } else {
		/*
		 * Reset record number
		 */
		recNum--;
	    }
	} /* end record loop */

	/*
	 * As of 10/1/2003, the kernel only checks that linkSegment is 0
	 * on primary mcells, and never looks at linkSegment otherwise.
	 * It also will occasionally spit out the "wrong" linkSegment,
	 * so checking it in fixfdmn now will cause "uncorrupted" domains
	 * to be reported as corrupt.
	 */
	if ((0 == currLinkSegment) &&  /* We must be on the Primary mcell. */
	    (pMcell->linkSegment != currLinkSegment))
	{
	    status = add_fixed_message(&(pCurrPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1,
					       FIXFDMN_929,
					       "Modified mcell (%d,%d,%d)'s linkSegment from %d to %d.\n"),
				       currMcell.vol,
				       currMcell.page,
				       currMcell.cell,
				       pMcell->linkSegment,
				       currLinkSegment);
	    if (SUCCESS != status) {
		return status;
	    }

	    pMcell->linkSegment = currLinkSegment;
	    modified = TRUE;
	}
	currLinkSegment++;

	/*
	 * We could have cleared all records from this mcell.
	 * If so, we need to delete this entire mcell.  We will
	 * base this off of its bCnt, we need to save this info
	 * to make sure we reset prevmcell correctly
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	byteCount = pRecord->bCnt;

	if (0 == byteCount) {
	    /*
	     * This mcell has no records.
	     * If it has a next chain, reset prevMcell to point to it.
	     * If it doesn't have a next chain then truncate prevMcell.
	     */
	    if ((currMcell.vol == primMcell.vol) &&
		(currMcell.page == primMcell.page) &&
		(currMcell.cell == primMcell.cell)) {
		int tagPageNum;
		int tagEntryOnPage;
		LBNT tagPageLbn;
		int tagPageVol;
		int numExtents;
		pagetoLBNT *pExtents; /* The extents to check */
		pageT      *tagPage;  /* The tag page we are checking */
		bsTDirPgT  *pTagPage; /* The tag page we are working on */
		bsTMapT    *tagEntry; /* The tag on the tag page */

		/*
		 * The primary mcell is empty we need to modify the
		 * entry in the tag file.
		 */
		tagPageNum = primTagNum / BS_TD_TAGS_PG;
		tagEntryOnPage = primTagNum % BS_TD_TAGS_PG;
		pExtents = fileset->tagLBN;
		numExtents = fileset->tagLBNSize;
 
		status = convert_page_to_lbn(pExtents, numExtents, tagPageNum, 
					     &tagPageLbn, &tagPageVol);
		if (SUCCESS != status) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_381,
				     "Can't convert tag page %d to LBN.\n"), 
			     tagPageNum);
		    return FAILURE;
		}

		/*
		 * Read the tag page
		 */
		status = read_page_from_cache(tagPageVol, tagPageLbn, &tagPage);
		if (SUCCESS != status) {
		    return status;
		}
		modified = FALSE;

		/*
		 * Now we have the correct tag page.  Find the correct tag
		 */
		pTagPage = (bsTDirPgT *)tagPage->pageBuf;
		tagEntry = &(pTagPage->tMapA[tagEntryOnPage]);

		status = add_fixed_message(&(tagPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_421,
						   "Modified tag %d's primary mcell from (%d,%d,%d) to (%d,%d,%d).\n"),
					   primTagNum,
					   tagEntry->tmVdIndex,
					   tagEntry->tmBfMCId.page,
					   tagEntry->tmBfMCId.cell,
					   nextMcell.vol,
					   nextMcell.page,
					   nextMcell.cell);
		if (SUCCESS != status) {
		    return status;
		}

		primMcell.vol  = nextMcell.vol;	   
		primMcell.page = nextMcell.page;
		primMcell.cell = nextMcell.cell;

		pTagNode->tagPmcell.vol  = primMcell.vol;
		pTagNode->tagPmcell.page = primMcell.page;
		pTagNode->tagPmcell.cell = primMcell.cell;

		tagEntry->tmVdIndex     = nextMcell.vol;	   
		tagEntry->tmBfMCId.page = nextMcell.page;
		tagEntry->tmBfMCId.cell = nextMcell.cell;

		status = release_page_to_cache(tagPageVol, tagPageLbn,
					       tagPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    } else {
		/*
		 * Other mcell
		 */
		if ((currMcell.page != prevMcell.page) ||
		    (currMcell.vol != prevMcell.vol)) {
		    volume = &(domain->volumes[prevMcell.vol]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 prevMcell.page,
						 &prevLbn, &prevVol);
		    if (SUCCESS != status) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
					 "Can't convert BMT page %d to LBN.\n"), 
				 prevMcell.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, &pPrevPage);
		    if (SUCCESS != status) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrPage;
		}

		pPrevMcell = &(bmtPrevPage->bsMCA[prevMcell.cell]);

		status = add_fixed_message(&(pPrevPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_422, 
						   "Modified mcell (%d,%d,%d)'s next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
					   prevMcell.vol, 
					   prevMcell.page,
					   prevMcell.cell, 
					   pPrevMcell->nextVdIndex,
					   pPrevMcell->nextMCId.page,
					   pPrevMcell->nextMCId.cell,
					   nextMcell.vol,
					   nextMcell.page,
					   nextMcell.cell);
		if (SUCCESS != status) {
		    return status;
		}

		pPrevMcell->nextVdIndex   = nextMcell.vol;	    
		pPrevMcell->nextMCId.page = nextMcell.page;
		pPrevMcell->nextMCId.cell = nextMcell.cell;

		if ((currMcell.page != prevMcell.page) ||
		    (currMcell.vol != prevMcell.vol)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }    
		} else {
		    bmtPrevPage = NULL;
		    modified = TRUE;
		} /* end if on same page or not */
	    } /* else primary mcell or other mcell */

	    /*
	     * Now that we've set the previous mcell to point to the next
	     * mcell, clear the next pointer in the current mcell so that
	     * free_mcell won't clear the found bit in the next mcell.
	     */
	    pMcell->nextVdIndex   = 0;
	    pMcell->nextMCId.page = 0;
	    pMcell->nextMCId.cell = 0;
	    /*
	     * Now that we have the info we need from the mcell
	     */
	    status = free_mcell(domain, currVol,
				pCurrPage, currMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = add_fixed_message(&(pCurrPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_423, 
					       "Deleted mcell (%d,%d,%d), has no records in it.\n"), 
				       currVol,
				       currMcell.page, 
				       currMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = TRUE;
	}	


	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrPage, modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}

	/*
	 * Do we have a nextMcell which we need to check.
	 */
	if (0 != nextMcell.vol) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    if (0 != byteCount) {
		prevMcell.vol  = currMcell.vol;
		prevMcell.page = currMcell.page;
		prevMcell.cell = currMcell.cell;
	    } else {
		/*
		 * This mcell was deleted so DON'T reset prevMcell.
		 */
	    }

	    currMcell.vol  = nextMcell.vol;
	    currMcell.page = nextMcell.page;
	    currMcell.cell = nextMcell.cell;
	} else {
	    currMcell.vol  = 0;
	    currMcell.page = 0;
	    currMcell.cell = 0;
	}/* end if nextMcell exists */
    } /* end while loop of the primary and next chain */


    /*
     * Load chain from chainMcell.
     * Load xtnt  from xtntMcell.
     */
    chain->vol  = chainMcell.vol;
    chain->page = chainMcell.page;
    chain->cell = chainMcell.cell;

    xtnt->vol   = xtntMcell.vol;
    xtnt->page  = xtntMcell.page;
    xtnt->cell  = xtntMcell.cell;

    if (TRUE == bmtModified) {
	return FIXED;
    }	

    return SUCCESS;
} /* end correct_bmt_primary_chain */


/*
 * Function Name: correct_bmt_extent_chain
 *
 * Description:	This is called to check the extent chain.
 *
 *  One check which is done is to figure out the size of the file
 *  based on the extents.  Striped files need to be handled differently.
 *
 *  NOTE: It is possible to delete an mcell in the middle of the chain.
 *  To simplify the code, we will return RESTART to the caller and they
 *  should re-issue the call to this function.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     pTagNode: The tag node we are working on.
 *     xtntMcell: The head of the extent mcell chain.
 *     chainMcell: The start of the chain mcell
 *     found: statistics for this chain.
 *
 * Output parameters: 
 *     found: statistics for this chain.
 *     chainMcell: The start of the chain mcell
 *
 * Returns: SUCCESS, FIXED, RESTART, FAILURE, NO_MEMORY 
 */
int correct_bmt_extent_chain(filesetT *fileset, 
			     tagNodeT *pTagNode,
			     mcellT   xtntMcell,
			     mcellT   *chainMcell,
			     chainFoundT *found)
{
    char       *funcName = "correct_bmt_extent_chain";
    int        status;
    int	       modified;
    int	       nextModified;
    int        bmtModified;
    int        foundXtnt;
    int        foundShadow;
    int        foundXtra;
    int        reprocessRecord;
    int        odsVersion;
    int	       currVol;
    LBNT       currLbn;
    int	       prevVol;
    LBNT       prevLbn;
    int        recNum;
    int	       primTagNum;
    int	       primSeqNum;
    int        currTagNum;
    int        currSeqNum;
    int        lastPage;
    int        pageCnt;
    int        mcellCnt;
    mcellT     chain;       /* The next mcell on the chain */
    mcellT     currMcell;   /* The mcell currently being worked on */
    mcellT     nextMcell;   /* The next mcell on the next chain */
    mcellT     prevMcell;   /* The previous mcell on the chain */
    domainT    *domain;
    volumeT    *volume;
    pageT      *pCurrPage;   /* The current page we are checking */
    pageT      *pPrevPage;   /* The previous page we are checking */
    bsMPgT     *bmtPage;     /* The current BMT page we are working on */
    bsMPgT     *bmtPrevPage; /* The previous BMT page we are working on */
    bsMPgT     *bmtNextPage; /* The next BMT page we are working on */
    bsMCT      *pMcell;	     /* The current BMT mcell we are working on */
    bsMCT      *pPrevMcell;  /* The previous BMT mcell we are working on */
    bsMCT      *pNextMcell;  /* The next BMT mcell we are working on */
    bsMRT      *pRecord;
    bsMRT      *pNextRecord;
    bsXtntRT      *pXtnt;
    bsShadowXtntT *pShadow;
    bsXtraXtntRT  *pXtraXtnt;
    bsXtraXtntRT  *pNextXtraXtnt;
    nodeT      *tmpNode;
    tagNodeT   tmpTagNode;
    tagNodeT   *pTempTagNode;

    /*
     * Init variables
     */
    domain = fileset->domain;
    odsVersion = domain->dmnVersion;
    modified = FALSE;
    bmtModified = FALSE;
    lastPage = 0;

    /*
     * If the chain exists, then for ODSV3 we MUST have a SHADOW record.
     * For ODSV4 we need either a SHADOW or XTRA record.  If we don't
     * have what we need then we need to truncate the chain.  As we don't
     * want to collect the extents if we will be clearing the chain, this
     * means we need to walk the chain twice.
     *
     * Another check, if the file is not striped then it should only
     * have one shadow record.  We will need to delete any extra 
     * shadow records.
     */
    chain.vol  = chainMcell->vol;
    chain.page = chainMcell->page;
    chain.cell = chainMcell->cell;
    pageCnt = found->pageCnt;
    mcellCnt = found->mcellCnt;

    if ((3 == odsVersion) || (TRUE == found->isStriped)) {
	/*
	 * ODSv3 files and striped files extent chains do not start in the
	 * BSR_XTNTS record, but start in the the chain mcell.
	 */
	currMcell.vol  = chain.vol;
	currMcell.page = chain.page;
	currMcell.cell = chain.cell;
    } else {
	/*
	 * ODSv4 non-striped  Start on xtntMcell.
	 */
	currMcell.vol  = xtntMcell.vol;
	currMcell.page = xtntMcell.page;
	currMcell.cell = xtntMcell.cell;
    }

    primTagNum = pTagNode->tagSeq.num;
    primSeqNum = pTagNode->tagSeq.seq;
    currTagNum = primTagNum;
    currSeqNum = primSeqNum;
	
    foundXtnt = FALSE;
    foundShadow = FALSE;
    foundXtra = FALSE;
    found->stripeCnt = 0;

    /*
     * Loop until chain is empty [0,0,0].
     */
    while (0 != currMcell.vol) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMcell.page, &currLbn, 
				     &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMcell.cell]);
	
	/*
	 * Now loop through records, checking to see if this chain 
	 * has a Shadow or Xtra extent.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	recNum = 0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * Init variables
	     */
	    recNum++;

	    switch (pRecord->type) {
		case BSR_XTNTS:
		    foundXtnt = TRUE;
		    break;

		case BSR_SHADOW_XTNTS:
		    if (FALSE == found->isStriped) {
			if (FALSE == foundShadow) {
			    /*
			     * Only need to deal with the first
			     * shadow record in a non stripped file.
			     * Any addtional shadow records will be
			     * removed below.
			     */
			    found->stripeCnt = 1;
			} 
                    } else {
			found->stripeCnt++;
		    } /* if not striped */
		    foundShadow = TRUE;
		    break;

		case BSR_XTRA_XTNTS:
		    foundXtra = TRUE;
		    break;

		default:
		    /*
		     * No other type should be in chain, if there are 
		     * records then they will be deleted when we collect
		     * the extents
		     */
		    break;
	    } /* end switch */

	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */

    	/*
	 * Set current to the next mcell.
	 */
	if ((currMcell.vol  == xtntMcell.vol) &&
	    (currMcell.page == xtntMcell.page) &&
	    (currMcell.cell == xtntMcell.cell)) {
	    /*
	     * On primary xtnt mcell, move to chain.
	     */
	    currMcell.vol  = chain.vol;
	    currMcell.page = chain.page;
	    currMcell.cell = chain.cell;
	} else {
	    /*
	     * On a chain mcell, move to next.
	     */
	    currMcell.vol  = pMcell->nextVdIndex;
	    currMcell.page = pMcell->nextMCId.page;
	    currMcell.cell = pMcell->nextMCId.cell;
	}

	/*
	 * Release the Page.
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrPage, FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
    } /* end chain while loop */

    /*
     * Now we need to check the following if the chainMcell exists
     *
     * 1) If ODSV3 do we have a shadow record.
     * 2) if ODSV4 do we have a shadow or xtra record
     *
     * If not free the chainMcell.
     */
    if ((0 != chain.vol) &&
	(((3 == odsVersion) && (FALSE == foundShadow)) ||
	 ((3 != odsVersion) &&
	  ((FALSE == found->isStriped) && (FALSE == foundXtra)) ||
	  ((TRUE == found->isStriped) && (FALSE == foundShadow))))) {
	status = correct_empty_chain(fileset, pTagNode);
	if (SUCCESS != status) {
	    if (FIXED != status) {
		return status;
	    } else {
		bmtModified = TRUE;
	    }
	}

	/*
	 * Finally set the local chain variable to 0,0,0
	 */
	chain.vol  = 0;
	chain.page = 0;
	chain.cell = 0;
    } else if (0 == chain.vol) {
	/*
	 * We don't have a chain, all the extents are stored in the
	 * XTRA record.  Don't need to do anything.
	 */
    } else {
	/*
	 * We have a valid chain, and it contains extents.
	 *
	 * Throw away the pageCnt from the BSR_XTNT record.
	 */
	pageCnt = 0;
    }

    /*
     * Now we need to re-follow chainMcell which contains the extents.
     */
    foundShadow = FALSE;

    currMcell.vol  = xtntMcell.vol;
    currMcell.page = xtntMcell.page;
    currMcell.cell = xtntMcell.cell;

    prevMcell.vol  = currMcell.vol;
    prevMcell.page = currMcell.page;
    prevMcell.cell = currMcell.cell;

    /*
     * Loop until chainMcell is empty [0,0,0]
     */
    while (0 != currMcell.vol) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMcell.page, &currLbn, 
				     &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}
    
	status = read_page_from_cache(currVol,currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMcell.cell]);

	if ((currMcell.vol  == xtntMcell.vol) &&
	    (currMcell.page == xtntMcell.page) &&
	    (currMcell.cell == xtntMcell.cell)) {
	    /*
	     * On primary xtnt mcell, set nextMcell to chain.
	     */
	    nextMcell.vol  = chain.vol;
	    nextMcell.page = chain.page;
	    nextMcell.cell = chain.cell;
	} else {
	    nextMcell.vol  = pMcell->nextVdIndex;
	    nextMcell.page = pMcell->nextMCId.page;
	    nextMcell.cell = pMcell->nextMCId.cell;
	}

	/*
	 * Now loop through records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	recNum = 0;

	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * Init variables
	     */	
	    reprocessRecord = FALSE;
	    recNum++;

	    /*
	     * Multiple checks are done here
	     *
	     * Check Xtra record to make sure that xCnt is valid.
	     *
	     * Delete record that should not be here.
	     */
	    switch (pRecord->type) {
		case BSR_SHADOW_XTNTS:
		    pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));

		    /*
		     * Three different cases:
		     *
		     * Case 1) ODSv3 non striped file has only 1 shadow
		     * Case 2) ODsv4 non striped file has no shadows
		     * Case 3) striped files have multiple shadows.
		     */
		    if (FALSE == found->isStriped) {
			if (((3 == odsVersion) && (TRUE == foundShadow)) ||
			    (3 != odsVersion)) {
			    /*
			     * Corruption: ODSv3 non striped file with
			     * more than one shadow. OR ODSv4 non striped
			     * file with a shadow.
			     *
			     * Throw this record away.
			     */
			    status = add_fixed_message(&(pCurrPage->messages), BMT,
						       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_427,
							       "Modified chain mcell (%d,%d,%d), deleted invalid record type %d.\n"),
						       currMcell.vol, 
						       currMcell.page, 
						       currMcell.cell, 
						       pRecord->type);
			    if (SUCCESS != status) {
				return status;
			    }

			    status = clear_mcell_record(domain,
							pCurrPage, 
							currMcell.cell, 
							recNum);
			    if (SUCCESS != status) {
				return status;
			    }
			    reprocessRecord = TRUE;
			    modified = TRUE;
			} else {
			    /*
			     * first shadow on a ODSv3 non striped file.
			     *
			     * Switch to new stripe,  Save lastPage and
			     * start fresh with this record.
			     */
			    pageCnt = 0;
			    lastPage = pShadow->bsXA[pShadow->xCnt-1].bsPage;
			    mcellCnt = mcellCnt + 1;
			    foundShadow = TRUE;
			} /* end if foundShadow */
		    } else {
			/*
			 * We are in striped file, and have a shadow
			 * record.
			 *
			 * Switch to new stripe,  Save lastPage and
			 * start fresh with this record
			 */
			pageCnt = pageCnt + lastPage;
			lastPage = pShadow->bsXA[pShadow->xCnt-1].bsPage;
			mcellCnt = mcellCnt + 1;
			foundShadow = TRUE;
		    } /* end if isStripe */
		    break;

		case BSR_XTRA_XTNTS:
		    pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    if ((3 != odsVersion) && (pXtraXtnt->xCnt < 2)) {
			/*
			 * Starting with ODSv4, you can no longer have
			 * an xtra xtnt record which has a xCnt less 
			 * than 2.
			 *
			 * Throw away the record as it is bogus.
			 */
			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_933,
							   "Modified mcell (%d,%d,%d), deleted BSR_XTRA_XTNTS record, no extents.\n"),
						   currMcell.vol, 
						   currMcell.page, 
						   currMcell.cell);

			if (SUCCESS != status) {
			    return status;
			}

			status = clear_mcell_record(domain,
						    pCurrPage, 
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
		    }

		    /*
		     * Don't update lastPage and mcellCnt if we need 
		     * to reprocess the record.
		     */
		    if (FALSE == reprocessRecord) {
			/*
			 * Keep track of last page num in the extent chain
			 */
			lastPage = pXtraXtnt->bsXA[pXtraXtnt->xCnt-1].bsPage;

			mcellCnt = mcellCnt + 1;
		    }
		    break;

		default:
		    if ((currMcell.vol  == xtntMcell.vol) &&
			(currMcell.page == xtntMcell.page) &&
			(currMcell.cell == xtntMcell.cell)) {
			/*
			 * On primary xtnt mcell, which has already been
			 * checked, and could have multiple record types.
			 */
		    } else {
			/*
			 * No other type should be in chain.
			 */
			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_427,
							   "Modified chain mcell (%d,%d,%d), deleted invalid record type %d.\n"),
						   currMcell.vol, 
						   currMcell.page, 
						   currMcell.cell, 
						   pRecord->type);
			if (SUCCESS != status) {
			    return status;
			}

			status = clear_mcell_record(domain,
						    pCurrPage, 
						    currMcell.cell, 
						    recNum);
			if (SUCCESS != status) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
		    }
		    break;
	    } /* end switch */

	    if (FALSE == reprocessRecord) {
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } else {
		/*
		 * Reset record number
		 */
		recNum--;
	    }
	} /* end record loop */

	/* 
	 * We could have cleared all records from this mcell.  If so, we
	 * need to delete this entire mcell.  We will base this off of
	 * its bCnt.  Once all the chain pointers have been corrected,
	 * return RESTART so the caller can recall this function.  
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	if (0 == pRecord->bCnt) {
	    /*
	     * This mcell has no records.
	     * If it has a next chain, reset prevMcell to point to it.
	     * If it doesn't have a next chain then truncate prevMcell.
	     */
	    if ((currMcell.vol  == xtntMcell.vol) &&
		(currMcell.page == xtntMcell.page) &&
		(currMcell.cell == xtntMcell.cell)) {
		/*
		 * Should never get here, as this was checked while in
		 * the primary chain check.
		 */
		return FAILURE;
	    } else if ((currMcell.vol  == chain.vol) &&
		       (currMcell.page == chain.page) &&
		       (currMcell.cell == chain.cell)) {
		/*
		 * Need to modify BSR_XTNT chain chain
		 */
		if ((currMcell.page != xtntMcell.page) ||
		    (currMcell.vol  != xtntMcell.vol)) {

		    volume = &(domain->volumes[xtntMcell.vol]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 xtntMcell.page,
						 &prevLbn, &prevVol);
		    if (SUCCESS != status) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
					 "Can't convert BMT page %d to LBN.\n"), 
				 xtntMcell.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, &pPrevPage);
		    if (SUCCESS != status) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrPage;
		} /* end if on different pages */

		pPrevMcell = &(bmtPrevPage->bsMCA[xtntMcell.cell]);
	    
		/*
		 * Now loop through primary chain's records.
		 */
		pRecord = (bsMRT *)pPrevMcell->bsMR0;

		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pPrevMcell->bsMR0[BSC_R_SZ]))))
		    {
		    bsXtntRT  *pXtnt;

		    if (BSR_XTNTS == pRecord->type) {
			pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
			/*
			 * Modify the chainMcell 
			 */
			status = add_fixed_message(&(pCurrPage->messages), 
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_471,
							   "Modified mcell (%d,%d,%d)'s xtnt chain from (%d,%d,%d) to (%d,%d,%d).\n"),
						   xtntMcell.vol, 
						   xtntMcell.page,
						   xtntMcell.cell, 
						   pXtnt->chainVdIndex,
						   pXtnt->chainMCId.page,
						   pXtnt->chainMCId.cell,
						   nextMcell.vol,
						   nextMcell.page,
						   nextMcell.cell);
			if (SUCCESS != status) {
			    return status;
			}
			
			pXtnt->chainVdIndex   = nextMcell.vol;
			pXtnt->chainMCId.page = nextMcell.page;
			pXtnt->chainMCId.cell = nextMcell.cell;

			chain.vol  = nextMcell.vol;
			chain.page = nextMcell.page;
			chain.cell = nextMcell.cell;
		    }
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, sizeof(int))); 
		} /* end record loop */

		if ((currMcell.page != xtntMcell.page) ||
		    (currMcell.vol != xtntMcell.vol)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }    
		} else {
		    bmtPrevPage = NULL;
		    modified = TRUE;
		} /* end if on same page or not */
	    } else {
		/*
		 * Here we need to reset the next pointer, not the chain
		 * pointer.
		 */
		if ((currMcell.page != prevMcell.page) ||
		    (currMcell.vol != prevMcell.vol)) {
		    volume = &(domain->volumes[prevMcell.vol]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 prevMcell.page,
						 &prevLbn, &prevVol);
		    if (SUCCESS != status) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
					 "Can't convert BMT page %d to LBN.\n"), 
				 prevMcell.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, &pPrevPage);
		    if (SUCCESS != status) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrPage;
		}

		pPrevMcell = &(bmtPrevPage->bsMCA[prevMcell.cell]);

		status = add_fixed_message(&(pPrevPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_422,
						   "Modified mcell (%d,%d,%d)'s next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
					   prevMcell.vol, 
					   prevMcell.page,
					   prevMcell.cell, 
					   pPrevMcell->nextVdIndex,
					   pPrevMcell->nextMCId.page,
					   pPrevMcell->nextMCId.cell,
					   nextMcell.vol,
					   nextMcell.page,
					   nextMcell.cell);
		if (SUCCESS != status) {
		    return status;
		}

		pPrevMcell->nextVdIndex   = nextMcell.vol;	    
		pPrevMcell->nextMCId.page = nextMcell.page;
		pPrevMcell->nextMCId.cell = nextMcell.cell;

		if ((currMcell.page != prevMcell.page) ||
		    (currMcell.vol != prevMcell.vol)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }    
		} else {
		    bmtPrevPage = NULL;
		    modified = TRUE;
		} /* end if on same page or not */
	    } /* end if chain vs prev mcell */

	    /*
	     * Now that we've set the previous mcell to point to the next
	     * mcell, clear the next pointer in the current mcell so that
	     * free_mcell won't clear the found bit in the next mcell.
	     */
	    pMcell->nextVdIndex   = 0;
	    pMcell->nextMCId.page = 0;
	    pMcell->nextMCId.cell = 0;

	    /*
	     * Now that we have the info we need from the mcell
	     */
	    status = free_mcell(domain, currVol, pCurrPage, currMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = add_fixed_message(&(pCurrPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_423,
					       "Deleted mcell (%d,%d,%d), has no records in it.\n"), 
				       currVol, currMcell.page, currMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = TRUE;

	    /*
	     * To simplify the code we are going to return RESTART
	     * and recheck this entire chain.
	     */
	    status = release_page_to_cache(currVol, currLbn, pCurrPage, 
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;
	    chainMcell->vol  = chain.vol;
	    chainMcell->page = chain.page;
	    chainMcell->cell = chain.cell;
	    return RESTART;
	} /* end if pRecord->bCnt is 0 */

	/*
	 * Check mcell boundaries.
	 *
	 * NOTE: if currMcell has been freed above this function
	 * returns SUCCESS, as currMcell has no records.
	 */ 
	status = correct_mcell_extent_boundary(domain,
					       pCurrPage,
					       currMcell,	
					       &nextMcell);
	if (SUCCESS != status) {
	    if (FIXED == status) {
		modified = TRUE;
	    } else {
		return status;
	    }
	}

	status = release_page_to_cache(currVol, currLbn, pCurrPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}
    
	/*
	 * Do we have another mcell in this chain 
	 */
	if (0 != nextMcell.vol) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    prevMcell.vol  = currMcell.vol;
	    prevMcell.page = currMcell.page;
	    prevMcell.cell = currMcell.cell;

	    currMcell.vol  = nextMcell.vol;
	    currMcell.page = nextMcell.page;
	    currMcell.cell = nextMcell.cell;
	} else {
	    currMcell.vol  = 0;
	    currMcell.page = 0;
	    currMcell.cell = 0;
	}/* end if nextMcell exists */
    } /* end chain while loop */

    /*
     * Save the last page number.
     *
     * Note for stripes we have multiple last pages, so make sure
     * you add the lastPage to what is already stored in pageCnt.
     */
    pageCnt = pageCnt + lastPage;

    chainMcell->vol  = chain.vol;
    chainMcell->page = chain.page;
    chainMcell->cell = chain.cell;
    found->pageCnt = pageCnt;
    found->mcellCnt = mcellCnt;

    if (TRUE == bmtModified) {
	return FIXED;
    }	
        
    return SUCCESS;
} /* end correct_bmt_extent_chain */


/*
 * Function Name: correct_mcell_extent_boundary
 *
 * Description: This function checks the extents in the current extent
 *   record with the extents in the following extent record to see if
 *   the extent page numbers in these records match with one another.
 *
 * To do this we need to keep track of 4 different page numbers:
 *
 * pageA which is the second to last page in the current record.
 * pageB which is the last page in the current record.
 * pageC which is the first page in the next record.
 * pageD which is the second page in the next record.
 *
 * In a non corrupt case pageB should be equal to pageC
 *
 * Error case 1: pageB < pageC
 *
 *    Fix: pageC = pageB
 *
 * Error case 2: pageB > pageC
 *
 *      If pageD > pageB then 
 *         pageC = page B
 *	Else if pageC > pageA then
 *         pageB = page C
 *	Else
 *	   delete the 2nd record
 *     reset the next chain and retest.
 *  
 * Input parameters:
 *     domain: Pointer to the domain structure
 *     pPage: The page we are working on.
 *     currMcell: The information on the current mcell.
 *     nextMcell: The next mcell in the chain.
 *
 * Output parameters:
 *     nextMcell: The next mcell in the chain, which can change
 *                if corruption is found.
 *
 * Returns: SUCCESS, FIXED, FAILURE, NO_MEMORY 
 */
int correct_mcell_extent_boundary(domainT *domain,
				  pageT   *pPage,
				  mcellT  currMcell,	
				  mcellT  *nextMcell)
{
    char *funcName = "correct_mcell_extent_boundary";
    int  status;
    int  modified;
    int  nextModified;
    int  tag;
    int  setTag;
    LBNT  lbn;
    int  xCnt;
    int  extentSize;
    int  currVol;
    int  checkedNext;
    int  pageA;
    int  pageB;
    int  pageC;
    int  pageD;
    LBNT lbnA;
    LBNT lbnC;
    int  found;
    int  nextPage;
    int	 nextVol;
    LBNT nextLbn;
    int  xtntType;
    pageT  *pNextPage;    /* The next page we are checking */
    bsMPgT *bmtPage;      /* The current BMT page we are working on */
    bsMPgT *nextBmtPage;  /* The next BMT page we are working on */
    bsMCT  *pMcell;	  /* The current BMT mcell we are working on */
    bsMCT  *pNextMcell;   /* The previous  BMT mcell we are working on */
    bsMRT  *pRecord;      /* The current BMT record we are working on */
    bsMRT  *pNextRecord;  /* The next BMT record we are working on */
    bsXtntRT      *pXtnt;
    bsShadowXtntT *pShadow;
    bsXtraXtntRT  *pXtraXtnt;
    bsXtraXtntRT  *pNextXtraXtnt;
    volumeT       *volume;
    pagetoLBNT extentArray[2];

    /*
     * Init variables
     */
    modified = FALSE;
    currVol  = currMcell.vol;
    bmtPage  = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(bmtPage->bsMCA[currMcell.cell]);
    tag      = pMcell->tag.num;
    setTag   = pMcell->bfSetTag.num;
    xtntType = 0;
    checkedNext = FALSE;
    nextModified = FALSE;

    while (FALSE == checkedNext) {
	if (0 == nextMcell->vol) {
	    /* 
	     * There is no nextMcell, so there is nothing else to check
	     * so return SUCCESS.  We can get in this case after we have
	     * removed an mcell from the chain, so we need to check the
	     * status of modified.  
	     */
	    if (TRUE == modified) {
		return FIXED;
	    }	
	    return SUCCESS;
	}

	/*
	 * Init variables
	 */
	found = FALSE;
	pageA = 0;
	pageB = 0;
	pageC = 0;
	pageD = 0;
	    
	/* 
	 * Store the LBN for pageA and PageC so fixfdmn can
	 * free the extent starting at those LBNs, as needed.
	 */
	lbnA  = (LBNT)-1;
	lbnC  = (LBNT)-1;

	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((FALSE == found) &&
	       (0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    switch (pRecord->type) {
		case BSR_XTNTS:
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    xtntType = BSR_XTNTS;

		    if ((3 == domain->dmnVersion) || 
			(BSXMT_STRIPE == pXtnt->type) ||
			(1 == pXtnt->firstXtnt.xCnt)) {
			/*
			 * This record is not loaded with  extent info 
			 * for ODSv3, striped files or when the extent
			 * chain has been migrated off this volume.
			 */
			return SUCCESS;
		    }
			
		    xCnt  = pXtnt->firstXtnt.xCnt;
		    pageA = pXtnt->firstXtnt.bsXA[0].bsPage;
		    pageB = pXtnt->firstXtnt.bsXA[1].bsPage;
		    lbnA  = pXtnt->firstXtnt.bsXA[0].vdBlk;
		    found = TRUE;

		    break;

		case BSR_XTRA_XTNTS:
		    pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    xtntType = BSR_XTRA_XTNTS;

		    xCnt  = pXtraXtnt->xCnt;
		    pageA = pXtraXtnt->bsXA[xCnt - 2].bsPage;
		    pageB = pXtraXtnt->bsXA[xCnt - 1].bsPage;
		    lbnA  = pXtraXtnt->bsXA[xCnt - 2].vdBlk;
		    found = TRUE;
		    break;

		case BSR_SHADOW_XTNTS:
		    pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));
		    xtntType = BSR_SHADOW_XTNTS;

		    if (1 == pShadow->xCnt) {
			/*
			 * This record is not loaded with extent info 
			 * when the extent chain has been migrated 
			 * off this volume.
			 */
			return SUCCESS;
		    }

		    xCnt  = pShadow->xCnt;
		    pageA = pShadow->bsXA[xCnt - 2].bsPage;
		    pageB = pShadow->bsXA[xCnt - 1].bsPage;
		    lbnA  = pShadow->bsXA[xCnt - 2].vdBlk;
		    found = TRUE;
		    break;

		default:
		    /*
		     * No other type should be found.
		     */
		    break;
	    } /* end switch */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */

	if (FALSE == found) {
	    /*
	     * How the heck did this happen?  It shouldn't as
	     * this mcell should have already been checked.
	     */
	    return FAILURE;
	}	

	/*
	 * Keep track of nextPage, as nextMcell can change
	 * out from under you.
	 */
	nextPage = nextMcell->page;

	/*
	 * Now load the next mcell, which could be on
	 * a differnt page or volume.  
	 */
	if ((currMcell.page != nextMcell->page) ||
	    (currMcell.vol != nextMcell->vol)) {
	    volume = &(domain->volumes[nextMcell->vol]);
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize, 
					 nextMcell->page, &nextLbn, &nextVol);
	    if (SUCCESS != status) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
				 "Can't convert BMT page %d to LBN.\n"), 
			 nextMcell->page);
		return FAILURE;
	    }

	    status = read_page_from_cache(nextVol, nextLbn, &pNextPage);
	    if (SUCCESS != status) {
		return status;
	    } 
	    nextModified = FALSE;
	    nextBmtPage = (bsMPgT *)pNextPage->pageBuf;
	} else {
	    /*
	     * Both mcells on same volume and page.
	     */
	    nextVol     = currVol;
	    pNextPage   = pPage;
	    nextBmtPage = bmtPage;
	} /* end if on different pages */

	pNextMcell = &(nextBmtPage->bsMCA[nextMcell->cell]);
	pNextRecord = (bsMRT *)pNextMcell->bsMR0;
	found = FALSE;

	while ((FALSE == found) &&
	       (0 != pNextRecord->type) &&
	       (0 != pNextRecord->bCnt) &&
	       (pNextRecord < ((bsMRT *) &(pNextMcell->bsMR0[BSC_R_SZ])))) {
	    switch (pNextRecord->type) {
		case BSR_XTRA_XTNTS:
		    pNextXtraXtnt = (bsXtraXtntRT *)((char *)pNextRecord + 
						     sizeof(bsMRT));
		    pageC = pNextXtraXtnt->bsXA[0].bsPage;
		    pageD = pNextXtraXtnt->bsXA[1].bsPage;
		    lbnC  = pNextXtraXtnt->bsXA[0].vdBlk;
		    found = TRUE;
		    break; 

		case BSR_SHADOW_XTNTS:
		    /*
		     * Striped file.  No need to check the boundary
		     * condition, as this record starts the next
		     * chain of extents.  Just release the page 
		     * if needed and return.
		     */
		    if ((currMcell.page != nextPage) ||
			(currMcell.vol != nextVol)) {
			status = release_page_to_cache(nextVol, 
						       nextLbn, 
						       pNextPage, 
						       nextModified, 
						       FALSE);
			if (SUCCESS != status) {
			    return status;
			}
		    } else {
			nextBmtPage = NULL;
		    } /* end if on same page or not */

		    if (TRUE == modified) {
			return FIXED;
		    }	
		    return SUCCESS;
		    break;

		default:
		    /*
		     * No other type should be found.
		     */
		    break;

	    } /* end switch */

	    pNextRecord = (bsMRT *) (((char *)pNextRecord) + 
				     roundup(pNextRecord->bCnt, sizeof(int))); 
	} /* end next record loop */

	if (FALSE == found) {
	    /*
	     * Again how the heck could this happen?  It shouldn't.
	     */
	    /*
	     * Did not not find the extent record.  The next page
	     * is still open, so release it before returning FAILURE.
	     */
	    if ((currMcell.page != nextPage) ||
		(currMcell.vol != nextVol)) {
		status = release_page_to_cache(nextVol, 
					       nextLbn, 
					       pNextPage, 
					       nextModified, 
					       FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    } else {
		nextBmtPage = NULL;
	    } /* end if on same page or not */
	    return FAILURE;
	}	

	/*
	 * We now have all four page variables loaded.
	 * 
	 * First mark the SBM pages free.
	 *
	 * Note: we will only be modifying the XTRA record, as in the
	 * other cases we have already returned.  This allows us to use
	 * the pNextXtraXtnt variable which was loaded above.  
	 */
	if (pageB < pageC) { 
	    lbn = lbnC;
	    extentSize = pageD - pageC;
	    status = remove_extent_from_sbm(domain, 
					    nextVol, 
					    lbn, 
					    extentSize,
					    tag, 
					    setTag,
					    nextMcell,
					    0);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = add_fixed_message(&(pNextPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_461,
					       "Modified mcell (%d,%d,%d) xtra xtnt entry %d's page from %u to %d.\n"),
				       nextMcell->vol,
				       nextMcell->page,
				       nextMcell->cell,
				       0,	
				       pNextXtraXtnt->bsXA[0].bsPage,
				       pageB);
	    if (SUCCESS != status) {
		return status;
	    }

	    /*
	     * Replace this extent with a "Hole".
	     */
	    pNextXtraXtnt->bsXA[0].bsPage = pageB;
	    pNextXtraXtnt->bsXA[0].vdBlk = (LBNT)-1;
	    nextModified = TRUE;
	    checkedNext = TRUE;
	} else if (pageB > pageC) {
	    if (pageD > pageB) {
		lbn = lbnC;
		extentSize = pageD - pageC;
		status = remove_extent_from_sbm(domain, 
						nextVol, 
						lbn, 
						extentSize,
						tag, 
						setTag,
						nextMcell,
						0);
		if (SUCCESS != status) {
		    return status;
		}

		status = add_fixed_message(&(pNextPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_461,
						   "Modified mcell (%d,%d,%d) xtra xtnt entry %d's page from %d to %d.\n"),
					   nextMcell->vol,
					   nextMcell->page,
					   nextMcell->cell,
					   0,	
					   pNextXtraXtnt->bsXA[0].bsPage,
					   pageB);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * Replace this extent with a "Hole".
		 */
		pNextXtraXtnt->bsXA[0].bsPage = pageB;
		pNextXtraXtnt->bsXA[0].vdBlk = (LBNT)-1;
		nextModified = TRUE;
		checkedNext = TRUE;
	    } else if (pageC > pageA) {
		lbn = lbnA;
		extentSize = pageB - pageA;
		status = remove_extent_from_sbm(domain, 
						currVol, 
						lbn, 
						extentSize,
						tag, 
						setTag,
						&currMcell,
						xCnt - 2);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * As we are actually modifying an extent, and not
		 * just making it a hole, add the new extent to 
		 * the SBM.
		 *
		 * Fake an extent Array to handle the extents.
		 */
		extentArray[0].volume = currVol;
		extentArray[0].lbn    = lbnA;
		extentArray[0].page   = pageA;
		extentArray[1].volume = currVol;
		extentArray[1].lbn    = (LBNT)-1;
		extentArray[1].page   = pageC;

		status = collect_sbm_info(domain, 
					  extentArray,
					  2, 
					  tag, 
					  setTag, 
					  &currMcell,
					  xCnt - 2);
		if (SUCCESS != status) {
		    return status;
		}

		switch (xtntType) {
		    case BSR_XTNTS:
			status = add_fixed_message(&(pPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_459,
							   "Modified mcell (%d,%d,%d) xtnt entry %d's page from %d to %d.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   xCnt - 1,
						   pXtnt->firstXtnt.bsXA[xCnt - 1].bsPage,
						   pageC);
			if (SUCCESS != status) {
			    return status;
			}

			pXtnt->firstXtnt.bsXA[xCnt - 1].bsPage = pageC;
			break;

		    case BSR_XTRA_XTNTS:
			status = add_fixed_message(&(pPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_461,
							   "Modified mcell (%d,%d,%d) xtra xtnt entry %d's page from %d to %d.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   xCnt - 1,
						   pXtraXtnt->bsXA[xCnt - 1].bsPage,
						   pageC);
			if (SUCCESS != status) {
			    return status;
			}

			pXtraXtnt->bsXA[xCnt - 1].bsPage = pageC;
			break;

		    case BSR_SHADOW_XTNTS:
			status = add_fixed_message(&(pPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_463,
							   "Modified mcell (%d,%d,%d) shadow xtnt entry %d's page from %u to %d.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   xCnt - 1,
						   pShadow->bsXA[xCnt - 1].bsPage,
						   pageC);
			if (SUCCESS != status) {
			    return status;
			}

			pShadow->bsXA[xCnt - 1].bsPage = pageC;
			break;

		    default:
			/*
			 * No other type should be found.
			 */
			break;
		}

		modified = TRUE;
		checkedNext = TRUE;
	    } else {
		/*
		 * Delete nextMcell, reset the next chain so that 
		 * the current mcell points to the nextMcell's 
		 * next mcell.
		 */
		if (xtntType == BSR_XTNTS) {
		    pXtnt->chainVdIndex   = pNextMcell->nextVdIndex;
		    pXtnt->chainMCId.page = pNextMcell->nextMCId.page;
		    pXtnt->chainMCId.cell = pNextMcell->nextMCId.cell;
		} else {
		    /*
		     * BSR_XTRA_XTNTS or BSR_SHADOW_XTNTS
		     */
		    pMcell->nextVdIndex   = pNextMcell->nextVdIndex;
		    pMcell->nextMCId.page = pNextMcell->nextMCId.page;
		    pMcell->nextMCId.cell = pNextMcell->nextMCId.cell;

		}
		modified = TRUE;

		/*
		 * We must reset the next pointer or free_mcell
		 * will mark that mcell as having no parent.
		 */
		pNextMcell->nextVdIndex   = 0;
		pNextMcell->nextMCId.page = 0;
		pNextMcell->nextMCId.cell = 0;
		nextModified = TRUE;

		/*
		 * free_mcell will handle the release of this
		 * mcell's extents.	
		 */
		status = free_mcell(domain, 
				    nextVol,
				    pNextPage, 
				    nextMcell->cell);
		if (SUCCESS != status) {
		    return status;
		}

		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_934,
						   "Deleted mcell (%d,%d,%d), invalid extent records.\n"), 
					   nextVol,
					   nextMcell->page, 
					   nextMcell->cell);
		if (SUCCESS != status) {
		    return status;
		}

		/*
		 * Reset nextMcell.
		 */
		nextMcell->vol  = pMcell->nextVdIndex;
		nextMcell->page = pMcell->nextMCId.page;
		nextMcell->cell = pMcell->nextMCId.cell;
		modified = TRUE;

		/*
		 * As we reset nextMcell we will need to recheck it.
		 */
		checkedNext = FALSE;
	    }
	} else {
	    /*
	     * They match: no corruption.
	     */
	    checkedNext = TRUE;
	}

	/*
	 * Release the page as needed.
	 */
	if ((currMcell.page != nextPage) ||
	    (currMcell.vol != nextVol)) {
	    status = release_page_to_cache(nextVol, nextLbn, 
					   pNextPage, nextModified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	} else {
	    nextBmtPage = NULL;
	    if (TRUE == nextModified) {
		modified = TRUE;
	    }
	} /* end if on same page or not */
	nextModified = FALSE;
    } /* end while checkedNext */

    if (TRUE == modified) {
	return FIXED;
    }	

    return SUCCESS;
} /* end correct_mcell_extent_boundary */


/*
 * Function Name: correct_tag_and_set
 *
 * Description: Walk this mcell chain and check to see if all the tag 
 *              and setTag match.
 *
 * NOTE: This function can modify multiple pages.
 *
 * NOTE: For clone filesets we only check the tag, not the setTag.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     pTagNode: The tag node we are working on.
 *
 * Output parameters:
 *
 * Side effect:
 *
 * Returns: SUCCESS, FIXED, FIXED_PRIOR, NO_MEMORY, FAILURE
 */
int correct_tag_and_set(filesetT *fileset,
			tagNodeT *pTagNode) 
{
    char    *funcName = "correct_tag_and_set";
    int	    status;
    int	    modified;
    int     bmtModified;
    int     primTagNum; 
    int     primSeqNum; 
    int     currTagNum; 
    int     currSeqNum; 
    int     nextTagNum; 
    int     nextSeqNum; 
    int	    currVol;
    LBNT    currLbn;
    int     foundXtnt;
    int     firstXtnt;
    mcellT  primMcell;
    mcellT  prevMcell;
    mcellT  currMcell;
    mcellT  nextMcell;
    mcellT  chainMcell;
    mcellT  xtntMcell;
    domainT *domain;
    volumeT *volume;
    pageT   *pCurrPage;  /* The current page we are checking */
    bsMPgT  *bmtPage;    /* The current BMT page we are working on */
    bsMCT   *pMcell;	 /* The current BMT mcell we are working on */
    bsMRT   *pRecord;    /* The current BMT record we are working on */
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;
    nodeT      *tmpNode;
    tagNodeT   tmpTagNode;
    tagNodeT   *pTempTagNode;

    /*
     * Init variables
     */
    modified = FALSE;
    bmtModified = FALSE;
    firstXtnt = FALSE;
    foundXtnt = FALSE;

    primTagNum = pTagNode->tagSeq.num;
    primSeqNum = pTagNode->tagSeq.seq;

    primMcell.vol  = pTagNode->tagPmcell.vol;
    primMcell.page = pTagNode->tagPmcell.page;
    primMcell.cell = pTagNode->tagPmcell.cell;

    currMcell.vol  = primMcell.vol;
    currMcell.page = primMcell.page;
    currMcell.cell = primMcell.cell;

    currTagNum = primTagNum;
    currSeqNum = primSeqNum;

    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    xtntMcell.vol  = 0;
    xtntMcell.page = 0;
    xtntMcell.cell = 0;

    /*
     * Start prevMcell at Primary
     */
    prevMcell.vol  = primMcell.vol;
    prevMcell.page = primMcell.page;
    prevMcell.cell = primMcell.cell;

    domain = fileset->domain;

    /*
     * Check both the primary chain and the xtnt chain.
     */
    while (0 != currMcell.vol) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMcell.page, &currLbn, 
				     &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol,currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage	   = (bsMPgT *)pCurrPage->pageBuf;
	pMcell	   = &(bmtPage->bsMCA[currMcell.cell]);

	/*
	 * Save this mcell's next chain
	 */
	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * If this mcell has a BSR_XTNTS record save the xtnt chain 
	 * pointer, so we can later check the extent chain.
	 *
	 * Once we find the BSR_XTNTS record we can skip this check.
	 */
	if (FALSE == foundXtnt) {
	    pRecord = (bsMRT *)pMcell->bsMR0;

	    /*
	     * Loop through all the records in this mcell.
	     */
	    while ((0 != pRecord->type) &&
		   (0 != pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))))
	    {
		bsXtntRT  *pXtnt;

		/*
		 * The only record type we care about is BSR_XTNTS
		 */
		if (BSR_XTNTS == pRecord->type) {
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		    /*
		     * Save this mcell, will need it later
		     */
		    xtntMcell.vol  = currMcell.vol;
		    xtntMcell.page = currMcell.page;
		    xtntMcell.cell = currMcell.cell;

		    /*
		     * Save the chainMcell
		     */
		    chainMcell.vol  = pXtnt->chainVdIndex;
		    chainMcell.page = pXtnt->chainMCId.page;
		    chainMcell.cell = pXtnt->chainMCId.cell;

		    break; /* out of record loop */
		} /* end if BSR_XTNT */
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } /* end record loop */
	} /* end if we haven't already found an BSR_XTNT record */

	/*
	 * Find nextMcell's tagNum (via skip list).
	 */
	if (0 != nextMcell.vol) {
	    tmpMcellNode.mcellId.vol  = nextMcell.vol;
	    tmpMcellNode.mcellId.page = nextMcell.page;
	    tmpMcellNode.mcellId.cell = nextMcell.cell;
	    pMcellNode = &tmpMcellNode;
		
	    status = find_node(domain->mcellList, (void **)&pMcellNode, &tmpNode);
	    if (SUCCESS != status) {
		return status;
	    } 

	    nextTagNum = pMcellNode->tagSeq.num;
	    nextSeqNum = pMcellNode->tagSeq.seq;
	} else {
	    /*
	     * End of next mcell chain.
	     */
	    nextMcell.vol  = 0;
	    nextMcell.page = 0;
	    nextMcell.cell = 0;
	    nextTagNum = 0;
	    nextSeqNum = 0;
	} /* end if nextMcell.vol */

	/*
	 * Check if this mcell has the correct tag and set number.
	 */
	status = correct_chain_tag_and_set(fileset, currMcell, 
					   prevMcell, primTagNum, 
					   primSeqNum, nextTagNum,
					   firstXtnt, pCurrPage);
	if (FIXED == status) {
	    modified = TRUE;
	} else if (FIXED_PRIOR == status) {
	    /* 
	     * The mcell which pointed to this mcell was just truncated.
	     *
	     * We might be able to find a tag which this mcell belongs to.
	     * If we can we need to re-attach it, and then recall this
	     * function on it so that its chains can be fixed.
	     *
	     * Then we need to break out of the loop.
	     */
	    
	    /*
	     * Release page to stop possible deadlocks.
	     */
	    status = release_page_to_cache(currVol, currLbn, 
					   pCurrPage, TRUE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }

	    bmtModified = TRUE;
	    modified = FALSE;

	    /*
	     * Is this mcell a primary mcell for a different tag.
	     */
	    tmpMcellNode.mcellId.vol  = currMcell.vol;
	    tmpMcellNode.mcellId.page = currMcell.page;
	    tmpMcellNode.mcellId.cell = currMcell.cell;
	    pMcellNode = &tmpMcellNode;

	    status = find_node(domain->mcellList, (void **)&pMcellNode, &tmpNode);
	    if (SUCCESS != status) {
		return status;
	    }

	    if (MC_IS_PRIMARY(pMcellNode->status)) {
		/*
		 * Now we know its a primary mcell, now we need to check
		 * if the tag that this mcell believes it belongs to is
		 * free.
		 */
		tmpTagNode.tagSeq.num = currTagNum;
		pTempTagNode = &tmpTagNode;

		/*
		 * Find the tag which this mcell says it belongs to.
		 */
		status = find_node(fileset->tagList, 
				   (void **)&pTempTagNode, &tmpNode);
		if (SUCCESS == status) {
		    /*
		     * The tag already has a primary, so at this time
		     * we will just assume the one it has is correct.
		     * As we have not reattched it, will be handled
		     * when we get to orphans.
		     *
		     * FUTURE: Is there a way to decide which one is
		     *         actually the correct primary?
		     *		[Check the tag sequence numbers?]
		     */
		    MC_SET_MISSING_TAG(pMcellNode->status);
		} else if (NOT_FOUND == status) {
		    /*
		     * Now we have a tag which doesn't exist and a
		     * primary mcell for it.  So attach it and create
		     * the tagNode.
		     */
		    status = correct_tag_to_mcell(fileset, 
						  currTagNum,
						  currSeqNum, 
						  currMcell.vol,
						  currMcell.page,
						  currMcell.cell);
		    if (SUCCESS != status) {
			if (NO_MEMORY == status) {
			    return status;
			} else {
			    writemsg(SV_DEBUG,
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_408, 
					     "Duplicate tag %d - different seq - Setting to missing.\n"),
				     currTagNum);
			    MC_SET_MISSING_TAG(pMcellNode->status);
			}
		    } else {
			MC_SET_FOUND_PTR(pMcellNode->status); 

			/* 
			 * correct_tag_to_mcell created the tag node 
			 */
			tmpTagNode.tagSeq.num = currTagNum;
			pTempTagNode = &tmpTagNode;

			status = find_node(fileset->tagList, 
					   (void **)&pTempTagNode, &tmpNode);
			if (SUCCESS != status) {
			    return status;
			}

			/* 
			 * We have attached the mcell chain to a tag.
			 * The tag node could be already have been
			 * passed by in the tag loop so we need to
			 * process the chain again.  It is possible this
			 * tag chain will be processed twice.  
			 */
			status = correct_bmt_chain_by_tag(fileset, pTempTagNode);
			if (SUCCESS != status) {
			    if (FIXED == status) {
				bmtModified = TRUE;
			    } else {
				return status;
			    }
			}
		    } /* end if correct_tag_to_mcell */
		} /* end NOT_FOUND */
	    } /* end IS PRIMARY */
	    /* 
	     * The chain was truncated we can break out of loop 
	     */
	    break; 
	} else if (SUCCESS != status) {
	    return status;
	}
    
	/*
	 * Have now checked the chain pointers and the tag and seqs.
	 * Set current to the next mcell.
	 */
	if ((pMcell->nextVdIndex == 0) && (chainMcell.vol != 0)) {
	    /*
	     * Reached the end of the next chain.  Now we need to 
	     * follow the extent chain.
	     */
	    currMcell.vol  = chainMcell.vol;
	    currMcell.page = chainMcell.page;
	    currMcell.cell = chainMcell.cell;

	    /*
	     * Set prevMcell to the xtntMcell
	     */
	    prevMcell.vol  = xtntMcell.vol;
	    prevMcell.page = xtntMcell.page;
	    prevMcell.cell = xtntMcell.cell;

	    firstXtnt = TRUE;

	    /*
	     * clear chainMcell
	     */
	    chainMcell.vol  = 0;
	    chainMcell.page = 0;
	    chainMcell.cell = 0;
	} else {
	    /*
	     * Set prevMcell to the currMcell
	     */
	    prevMcell.vol  = currMcell.vol;
	    prevMcell.page = currMcell.page;
	    prevMcell.cell = currMcell.cell;

	    /*
	     * More pages on the current chain.
	     */
	    currMcell.vol  = pMcell->nextVdIndex;
	    currMcell.page = pMcell->nextMCId.page;
	    currMcell.cell = pMcell->nextMCId.cell;

	    firstXtnt = FALSE;
	}

	/*
	 * Release the Page.
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrPage, modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}
    } /* end currMcell while loop */

    if (TRUE == bmtModified) {
	return FIXED;
    }	

    return SUCCESS;
} /* end correct_tag_and_set */


/*
 * Function Name: correct_tag_chains
 *
 * Description: Walk this mcell chain and check that the next and
 *              xtnt chain pointers point to valid mcells.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     pTagNode: The tag node we are working on.
 *
 * Output parameters:
 *
 * Side effect:
 *
 * Returns: SUCCESS, FIXED, FIXED_PRIOR, NO_MEMORY, FAILURE
 */
int correct_tag_chains(filesetT *fileset,
		       tagNodeT *pTagNode) 
{
    char *funcName = "correct_tag_chains";
    int	 status;
    int	 modified;
    int  bmtModified;
    int	 currVol;
    LBNT currLbn;
    int  foundXtnt;
    int  primTagNum; 
    int  primSeqNum; 
    int  nextTagNum; 
    int  nextSeqNum; 
    int  byteCount;
    int	 prevVol;
    LBNT prevLbn;
    mcellT primMcell;
    mcellT prevMcell;
    mcellT currMcell;
    mcellT nextMcell;
    mcellT xtntMcell;
    mcellT chainMcell;
    mcellT chain;          /* Needed to prevent infinite loop on extent chain */
    domainT *domain;
    volumeT *volume;
    pageT   *pCurrPage;    /* The current page we are checking */
    pageT   *pPrevPage;    /* The previous page we are checking */
    bsMPgT  *bmtPage;      /* The current BMT page we are working on */
    bsMPgT  *bmtPrevPage;  /* The previous BMT page we are working on */
    bsMCT   *pMcell;	   /* The current BMT mcell we are working on */
    bsMCT   *pPrevMcell;   /* The previous BMT mcell we are working on */
    bsMRT   *pRecord;      /* The current BMT record we are working on */
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;
    nodeT      *tmpNode;

    /*
     * Init variables
     */
    primTagNum = pTagNode->tagSeq.num;
    primSeqNum = pTagNode->tagSeq.seq;

    primMcell.vol  = pTagNode->tagPmcell.vol;
    primMcell.page = pTagNode->tagPmcell.page;
    primMcell.cell = pTagNode->tagPmcell.cell;

    currMcell.vol  = primMcell.vol;
    currMcell.page = primMcell.page;
    currMcell.cell = primMcell.cell;

    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    chain.vol  = 0;
    chain.page = 0;
    chain.cell = 0;

    xtntMcell.vol  = 0;
    xtntMcell.page = 0;
    xtntMcell.cell = 0;

    /*
     * Start prevMcell at Primary
     */
    prevMcell.vol  = primMcell.vol;
    prevMcell.page = primMcell.page;
    prevMcell.cell = primMcell.cell;
    
    foundXtnt   = FALSE;
    modified    = FALSE;
    bmtModified = FALSE;

    domain = fileset->domain;

    /*
     * Check both the primary chain and the xtnt chain.
     */
    while (0 != currMcell.vol) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMcell.page, &currLbn, 
				     &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128, 
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol,currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage	   = (bsMPgT *)pCurrPage->pageBuf;
	pMcell	   = &(bmtPage->bsMCA[currMcell.cell]);

	/*
	 * Save this mcell's next chain
	 */
	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Does the mcell have any records?
	 *
	 * If not delete the mcell, and relink the chain.
	 *
	 * We will base this off of its bCnt, we need to save this info
	 * to make sure we reset prevmcell correctly
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	byteCount = pRecord->bCnt;

	if (0 == byteCount) {
	    /*
	     * This mcell has no records.
	     * If it has a next chain, reset prevMcell to point to it.
	     * If it doesn't have a next chain then truncate prevMcell.
	     * If it is the primary mcell modify the tag file.
	     */
	    if ((currMcell.vol == primMcell.vol) &&
		(currMcell.page == primMcell.page) &&
		(currMcell.cell == primMcell.cell)) {
		/*
		 * Primary Mcell
		 */
		int tagPageNum;
		int tagEntryOnPage;
		LBNT tagPageLbn;
		int tagPageVol;
		int numExtents;
		pagetoLBNT *pExtents; /* The extents to check */
		pageT      *tagPage;  /* The tag page we are checking */
		bsTDirPgT  *pTagPage; /* The tag page we are working on */
		bsTMapT    *tagEntry; /* The tag on the tag page */

		/*
		 * The primary mcell is empty we need to modify the
		 * entry in the tag file.
		 */
		tagPageNum = primTagNum / BS_TD_TAGS_PG;
		tagEntryOnPage = primTagNum % BS_TD_TAGS_PG;
		pExtents = fileset->tagLBN;
		numExtents = fileset->tagLBNSize;
 
		status = convert_page_to_lbn(pExtents, numExtents, 
					     tagPageNum, 
					     &tagPageLbn, 
					     &tagPageVol);
		if (SUCCESS != status) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_381,
				     "Can't convert tag page %d to LBN.\n"), 
			     tagPageNum);
		    return FAILURE;
		}

		/*
		 * Read the tag page
		 */
		status = read_page_from_cache(tagPageVol, tagPageLbn, &tagPage);
		if (SUCCESS != status) {
		    return status;
		}
		modified = FALSE;

		/*
		 * Now we have the correct tag page.  Find the correct tag
		 */
		pTagPage = (bsTDirPgT *)tagPage->pageBuf;
		tagEntry = &(pTagPage->tMapA[tagEntryOnPage]);

		status = add_fixed_message(&(tagPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_421,
						   "Modified tag %d's primary mcell from (%d,%d,%d) to (%d,%d,%d).\n"),
					   primTagNum,
					   tagEntry->tmVdIndex,
					   tagEntry->tmBfMCId.page,
					   tagEntry->tmBfMCId.cell,
					   nextMcell.vol,
					   nextMcell.page,
					   nextMcell.cell);
		if (SUCCESS != status) {
		    return status;
		}

		primMcell.vol  = nextMcell.vol;	   
		primMcell.page = nextMcell.page;
		primMcell.cell = nextMcell.cell;

		pTagNode->tagPmcell.vol  = primMcell.vol;
		pTagNode->tagPmcell.page = primMcell.page;
		pTagNode->tagPmcell.cell = primMcell.cell;

		tagEntry->tmVdIndex     = nextMcell.vol;	   
		tagEntry->tmBfMCId.page = nextMcell.page;
		tagEntry->tmBfMCId.cell = nextMcell.cell;

		status = release_page_to_cache(tagPageVol, tagPageLbn,
					       tagPage, TRUE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}

	    } else if ((currMcell.vol  == chain.vol) &&
		       (currMcell.page == chain.page) &&
		       (currMcell.cell == chain.cell)) {
		/*
		 * Need to modify BSR_XTNT chain chain
		 */
		if ((currMcell.vol  != xtntMcell.vol) ||
		    (currMcell.page != xtntMcell.page)) {

		    volume = &(domain->volumes[xtntMcell.vol]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 xtntMcell.page,
						 &prevLbn, &prevVol);
		    if (SUCCESS != status) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
					 "Can't convert BMT page %d to LBN.\n"), 
				 xtntMcell.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, &pPrevPage);
		    if (SUCCESS != status) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrPage;
		} /* end if on different pages */

		pPrevMcell = &(bmtPrevPage->bsMCA[xtntMcell.cell]);
	    
		/*
		 * Now loop through the xtnt mcell's records.
		 */
		pRecord = (bsMRT *)pPrevMcell->bsMR0;

		while ((0 != pRecord->type) &&
		       (0 != pRecord->bCnt) &&
		       (pRecord < ((bsMRT *) &(pPrevMcell->bsMR0[BSC_R_SZ]))))
		    {
		    bsXtntRT  *pXtnt;

		    if (BSR_XTNTS == pRecord->type) {
			pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
			/*
			 * Modify the chainMcell 
			 */
			status = add_fixed_message(&(pCurrPage->messages), 
						   BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_471,
							   "Modified mcell (%d,%d,%d)'s xtnt chain from (%d,%d,%d) to (%d,%d,%d).\n"),
						   xtntMcell.vol, 
						   xtntMcell.page,
						   xtntMcell.cell, 
						   pXtnt->chainVdIndex,
						   pXtnt->chainMCId.page,
						   pXtnt->chainMCId.cell,
						   nextMcell.vol,
						   nextMcell.page,
						   nextMcell.cell);
			if (SUCCESS != status) {
			    return status;
			}
			
			pXtnt->chainVdIndex   = nextMcell.vol;
			pXtnt->chainMCId.page = nextMcell.page;
			pXtnt->chainMCId.cell = nextMcell.cell;

			break; /* out of record loop */
		    }
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, sizeof(int))); 
		} /* end record loop */

		if ((currMcell.page != xtntMcell.page) ||
		    (currMcell.vol != xtntMcell.vol)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }    
		} else {
		    bmtPrevPage = NULL;
		    modified = TRUE;
		} /* end if on same page or not */

		/*
		 * We have a new start of the chain.
		 */
		chain.vol  = nextMcell.vol;
		chain.page = nextMcell.page;
		chain.cell = nextMcell.cell;
	    } else {
		/*
		 * Here we need to reset the next pointer, not the chain
		 * pointer.
		 */
		if ((currMcell.vol  != prevMcell.vol) ||
		    (currMcell.page != prevMcell.page)) {
		    
		    volume = &(domain->volumes[prevMcell.vol]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 prevMcell.page,
						 &prevLbn, &prevVol);
		    if (SUCCESS != status) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
					 "Can't convert BMT page %d to LBN.\n"), 
				 prevMcell.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, &pPrevPage);
		    if (SUCCESS != status) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrPage;
		}

		pPrevMcell = &(bmtPrevPage->bsMCA[prevMcell.cell]);

		status = add_fixed_message(&(pPrevPage->messages), BMT,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_422,
						   "Modified mcell (%d,%d,%d)'s next chain from (%d,%d,%d) to (%d,%d,%d).\n"),
					   prevMcell.vol, 
					   prevMcell.page,
					   prevMcell.cell, 
					   pPrevMcell->nextVdIndex,
					   pPrevMcell->nextMCId.page,
					   pPrevMcell->nextMCId.cell,
					   nextMcell.vol,
					   nextMcell.page,
					   nextMcell.cell);
		if (SUCCESS != status) {
		    return status;
		}

		pPrevMcell->nextVdIndex   = nextMcell.vol;	    
		pPrevMcell->nextMCId.page = nextMcell.page;
		pPrevMcell->nextMCId.cell = nextMcell.cell;

		if ((currMcell.vol  != prevMcell.vol) ||
		    (currMcell.page != prevMcell.page)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }    
		} else {
		    bmtPrevPage = NULL;
		    modified = TRUE;
		} /* end if on same page or not */
	    }

	    /*
	     * Now that we've set the previous mcell to point to the next
	     * mcell, clear the next pointer in the current mcell so that
	     * free_mcell won't clear the found bit in the next mcell.
	     */
	    pMcell->nextVdIndex   = 0;
	    pMcell->nextMCId.page = 0;
	    pMcell->nextMCId.cell = 0;

	    /*
	     * Now that we have the info we need from the mcell
	     */
	    status = free_mcell(domain, currVol, pCurrPage, currMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = add_fixed_message(&(pCurrPage->messages), BMT,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_423,
					       "Deleted mcell (%d,%d,%d), has no records in it.\n"), 
				       currVol, currMcell.page, currMcell.cell);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = TRUE;

	    /*
	     * Release the current Page.
	     */
	    status = release_page_to_cache(currVol, currLbn, 
					   pCurrPage, modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    if (TRUE == modified) {
		bmtModified = TRUE;
		modified = FALSE;
	    }

	    /*
	     * Set currMcell to nextMcell and continue loop 
	     * with new currMcell.
	     */
	    currMcell.vol  = nextMcell.vol;
	    currMcell.page = nextMcell.page;
	    currMcell.cell = nextMcell.cell;

	    continue;

	} /* end if byteCount is 0 */

	/*
	 * If this mcell has a BSR_XTNTS record save the xtnt chain 
	 * pointer, so we can later check the extent chain.
	 *
	 * Once we find the BSR_XTNTS record we can skip this check.
	 */
	if (FALSE == foundXtnt) {
	    pRecord = (bsMRT *)pMcell->bsMR0;

	    /*
	     * Loop through all the records in this mcell.
	     */
	    while ((0 != pRecord->type) &&
		   (0 != pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))))
	    {
		bsXtntRT  *pXtnt;

		/*
		 * The only record type we care about is BSR_XTNTS
		 */
		if (BSR_XTNTS == pRecord->type) {
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		    /*
		     * Save the chainMcell and xtntMcell for later
		     */
		    chainMcell.vol  = pXtnt->chainVdIndex;
		    chainMcell.page = pXtnt->chainMCId.page;
		    chainMcell.cell = pXtnt->chainMCId.cell;

		    xtntMcell.vol  = currMcell.vol;
		    xtntMcell.page = currMcell.page;
		    xtntMcell.cell = currMcell.cell;

		    /*
		     * Check if the chainMcell actually exists 
		     * (done by checking skip list).
		     */
		    if (0 != chainMcell.vol) {
			tmpMcellNode.mcellId.vol  = chainMcell.vol;
			tmpMcellNode.mcellId.page = chainMcell.page;
			tmpMcellNode.mcellId.cell = chainMcell.cell;
			pMcellNode = &tmpMcellNode;
		
			status = find_node(domain->mcellList, 
					   (void **)&pMcellNode, 
					   &tmpNode);
			if (SUCCESS != status) {
			    if (NOT_FOUND == status) {
				/*
				 * The chainMcell pointer points to a 
				 * bogus mcell, truncate the chain.
				 */
				status = add_fixed_message(&(pCurrPage->messages),
							   BMT,
							   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_930,
								   "Modified mcell (%d,%d,%d)'s chain pointer from (%d,%d,%d) to (%d,%d,%d).\n"),
							   currMcell.vol,
							   currMcell.page,
							   currMcell.cell, 
							   chainMcell.vol,
							   chainMcell.page, 
							   chainMcell.cell,
							   0, 0, 0);
				if (SUCCESS != status) {
				    return status;
				}

				pXtnt->chainVdIndex   = 0;
				pXtnt->chainMCId.page = 0;
				pXtnt->chainMCId.cell = 0;

				/*
				 * End the extent mcell chain.
				 */
				chainMcell.vol  = 0;
				chainMcell.page = 0;
				chainMcell.cell = 0;
				modified = TRUE;
			    } else {
				return status;
			    } /* end if/else NOT_FOUND */
			} else {
			    /*
			     * found what we were looking for
			     */
			    foundXtnt = TRUE;
			}/* end if SUCCESS */
		    } /* end if chainMcell.vol == 0 */
		} /* end if BSR_XTNT */
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(int))); 
	    } /* end record loop */
	} /* end if we haven't already found an BSR_XTNT record */

	/*
	 * Check if the nextMcell exists (via skip list).
	 */
	if (0 != nextMcell.vol) {
	    tmpMcellNode.mcellId.vol  = nextMcell.vol;
	    tmpMcellNode.mcellId.page = nextMcell.page;
	    tmpMcellNode.mcellId.cell = nextMcell.cell;
	    pMcellNode = &tmpMcellNode;
		
	    status = find_node(domain->mcellList, (void **)&pMcellNode, &tmpNode);
	    if (SUCCESS != status) {
		if (NOT_FOUND == status) {
		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_425,
						       "Modified mcell (%d,%d,%d)'s next pointer from (%d,%d,%d) to (%d,%d,%d).\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell, 
					       nextMcell.vol,
					       nextMcell.page, 
					       nextMcell.cell,
					       0, 0, 0);
		    if (SUCCESS != status) {
			return status;
		    }

		    pMcell->nextVdIndex   = 0;
		    pMcell->nextMCId.page = 0;
		    pMcell->nextMCId.cell = 0;
		    /*
		     * End the next mcell chain.
		     */
		    nextMcell.vol  = 0;
		    nextMcell.page = 0;
		    nextMcell.cell = 0;
		    nextTagNum = 0;
		    nextSeqNum = 0;
		    modified = TRUE;
		} else {
		    return status;
		} /* end if/else NOT_FOUND */
	    } else { /* found the next mcell node */
		nextTagNum = pMcellNode->tagSeq.num;
		nextSeqNum = pMcellNode->tagSeq.seq;
	    } /* end if SUCCESS */
	} else {
	    /*
	     * End of next mcell chain.
	     */
	    nextMcell.vol  = 0;
	    nextMcell.page = 0;
	    nextMcell.cell = 0;
	    nextTagNum = 0;
	    nextSeqNum = 0;
	} /* end if nextMcell.vol */

	/*
	 * Have now checked the chain and next pointers.
	 * Set current to the next mcell.
	 */
	if ((pMcell->nextVdIndex == 0) && 
	    (chainMcell.vol != 0)) {
	    /*
	     * Reached the end of the next chain.  Now we need to 
	     * follow the extent chain.
	     */
	    currMcell.vol  = chainMcell.vol;
	    currMcell.page = chainMcell.page;
	    currMcell.cell = chainMcell.cell;

	    /*
	     * Set prevMcell to the xtntMcell
	     */
	    prevMcell.vol  = xtntMcell.vol;
	    prevMcell.page = xtntMcell.page;
	    prevMcell.cell = xtntMcell.cell;

	    /*
	     * Save chainMcell in chain, and then clear chainMcell
	     */
	    chain.vol  = chainMcell.vol;
	    chain.page = chainMcell.page;
	    chain.cell = chainMcell.cell;

	    chainMcell.vol  = 0;
	    chainMcell.page = 0;
	    chainMcell.cell = 0;
	} else {
	    /*
	     * Set prevMcell to the currMcell
	     */
	    prevMcell.vol  = currMcell.vol;
	    prevMcell.page = currMcell.page;
	    prevMcell.cell = currMcell.cell;

	    /*
	     * More pages on the current chain.
	     */
	    currMcell.vol  = pMcell->nextVdIndex;
	    currMcell.page = pMcell->nextMCId.page;
	    currMcell.cell = pMcell->nextMCId.cell;
	}

	/*
	 * Release the Page.
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrPage, modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}
    } /* end currMcell while loop */

    if (TRUE == bmtModified) {
	return FIXED;
    }	

    return SUCCESS;
} /* end correct_tag_chain */


/*
 * Function Name: correct_dir_extents
 *
 * Description: This walks the chain to fix all the extent records in it.
 *
 * WARNING: Must be called after the SBM has been cleared of all overlaps.
 *
 * NOTE: This function is called from fixfdmn_dir.c
 *
 *       Don't have to worry about stripes for directories.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     tag: The tag node.
 *     numXtnts: Number of extents in array.
 *     xtntArray: The list of extents for this tag.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY
 */
int correct_dir_extents(filesetT *fileset, 
			tagNodeT *tag,
			int numXtnts,
			pagetoLBNT *xtntArray)
{
    char *funcName = "correct_dir_extents";
    int	 modified;
    int	 status;
    int	 extent;
    int  deleteMcell;
    int	 currVol;
    LBNT currLbn;
    int	 currXtntEntry;
    int	 odsVersion;
    int  foundXtnt;
    int  crossedBoundary;
    mcellT  currMcell;   /* The mcell currently being worked on */
    mcellT  nextMcell;	 /* The next mcell on the next chain */
    mcellT  chainMcell;	 /* The chain mcell on the next chain */
    volumeT *volume;
    pageT   *pCurrPage;  /* The current page we are checking */
    bsMPgT  *bmtPage;	 /* The current BMT page we are working on */
    bsMCT   *pMcell;	 /* The current BMT mcell we are working on */
    bsMRT   *pRecord;
    domainT *domain;
    bsXtntRT *pXtnt;
    bsShadowXtntT *pShadow;
    bsXtraXtntRT  *pXtraXtnt;

    /*
     * Initialize variables.
     */
    modified = FALSE;
    domain = fileset->domain;
    odsVersion = domain->dmnVersion;
    currXtntEntry = 0;
    foundXtnt = FALSE;
    deleteMcell = FALSE;
	
    /*
     * Start at primary
     */
    currMcell.vol  = tag->tagPmcell.vol;
    currMcell.page = tag->tagPmcell.page;
    currMcell.cell = tag->tagPmcell.cell;

    chainMcell.vol  = 0;
    chainMcell.page = 0;
    chainMcell.cell = 0;

    /*
     * Loop until currMcell is empty [0,0,0]
     */
    while ((0 != currMcell.vol) && (FALSE == foundXtnt)){
	/*
	 * Read the primary mcell page (first get LBN).
	 */
	volume = &(domain->volumes[currMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMcell.page, &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage	= (bsMPgT *)pCurrPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	/*
	 * Now loop through primary next chain records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((FALSE == foundXtnt) &&
	       (0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * Only concerned with BSR_XTNTS
	     */
	    if (BSR_XTNTS == pRecord->type) {
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		foundXtnt = TRUE;
		
		/*
		 * Set next Chain to follow it, after we follow this 
		 * chain.
		 */
		chainMcell.vol	= pXtnt->chainVdIndex;
		chainMcell.page = pXtnt->chainMCId.page;
		chainMcell.cell = pXtnt->chainMCId.cell;

		/*
		 * Loop through all the extents in this record.
		 */
		if (3 == odsVersion) {
		    /*	
		     * The extents for this are only used in BMT0 on
		     * ODSv3. The fields are not initialized for other
		     * records.
		     */
		} else if (currVol != xtntArray[0].volume) {
		    /*
		     * The xtnt record is empty, because the extents
		     * have been migrated to another voume.
		     */
		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_459, 
						       "Modified mcell (%d,%d,%d) xtnt entry %d's page from %d to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell,
					       0,  
					       pXtnt->firstXtnt.bsXA[0].bsPage,
					       0);
		    if (SUCCESS != status) {
			return status;
		    }

		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_402,
						       "Modified mcell (%d,%d,%d) xtnt entry %d's LBN from %u to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell,
					       0,	 
					       pXtnt->firstXtnt.bsXA[0].vdBlk,
					       -1);
		    if (SUCCESS != status) {
			return status;
		    }

		    /*
		     * Initialize the BSR_XTNT record.
		     */
		    pXtnt->firstXtnt.xCnt = 1;
		    pXtnt->firstXtnt.bsXA[0].bsPage = 0;
		    pXtnt->firstXtnt.bsXA[0].vdBlk  = -1;
		    modified = TRUE;
		    
		    /*
		     * Set currXtntEntry to 0.
		     */
		    currXtntEntry = 0;

		} else {
		    /*
		     * Modify the BSR_XTNT's extents
		     */
		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_459, 
						       "Modified mcell (%d,%d,%d) xtnt entry %d's page from %d to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell,
					       0,  
					       pXtnt->firstXtnt.bsXA[0].bsPage,
					       xtntArray[0].page);
		    if (SUCCESS != status) {
			return status;
		    }

		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_402,
						       "Modified mcell (%d,%d,%d) xtnt entry %d's LBN from %u to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell,
					       0,	 
					       pXtnt->firstXtnt.bsXA[0].vdBlk,
					       xtntArray[0].lbn);
		    if (SUCCESS != status) {
			return status;
		    }
		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_459, 
						       "Modified mcell (%d,%d,%d) xtnt entry %d's page from %d to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell,
					       1,  
					       pXtnt->firstXtnt.bsXA[1].bsPage,
					       xtntArray[1].page);
		    if (SUCCESS != status) {
			return status;
		    }

		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_402,
						       "Modified mcell (%d,%d,%d) xtnt entry %d's LBN from %u to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell,
					       1,	 
					       pXtnt->firstXtnt.bsXA[1].vdBlk,
					       -1);
		    if (SUCCESS != status) {
			return status;
		    }

		    /*
		     * Reload the BSR_XTNT record from the xtntArray.
		     */
		    pXtnt->firstXtnt.xCnt = 2;
		    pXtnt->firstXtnt.bsXA[0].bsPage = xtntArray[0].page;
		    pXtnt->firstXtnt.bsXA[0].vdBlk  = xtntArray[0].lbn;
		    pXtnt->firstXtnt.bsXA[1].bsPage = xtntArray[1].page;
		    pXtnt->firstXtnt.bsXA[1].vdBlk = -1;
		    modified = TRUE;
		    
		    /*
		     * Set currXtntEntry
		     */
		    currXtntEntry = 1;

		}/* end odsVersion */

	    } /* end if BSR_XTNT */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    modified = FALSE;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (0 != nextMcell.vol) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currMcell.vol   = nextMcell.vol;
	    currMcell.page  = nextMcell.page;
	    currMcell.cell  = nextMcell.cell;

	} else {
	    currMcell.vol   = 0;
	    currMcell.page  = 0;
	    currMcell.cell  = 0;
	}/* end if nextMcell exists */
    } /* end primary chain */

    /*
     * Initialize variables to start on the extent chain.
     */
    currMcell.vol   = chainMcell.vol;
    currMcell.page  = chainMcell.page;
    currMcell.cell  = chainMcell.cell;
	
    /*
     * Loop until currMcell is empty [0,0,0]
     */
    while (0 != currMcell.vol) {
	volume = &(domain->volumes[currMcell.vol]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMcell.page, &currLbn, &currVol);
	if (SUCCESS != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"), 
		     currMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrPage);
	if (SUCCESS != status) {
	    return status;
	} 

	bmtPage	= (bsMPgT *)pCurrPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currMcell.cell]);

	nextMcell.vol  = pMcell->nextVdIndex;
	nextMcell.page = pMcell->nextMCId.page;
	nextMcell.cell = pMcell->nextMCId.cell;

	pRecord = (bsMRT *)pMcell->bsMR0;

	if (TRUE == deleteMcell) {
	    /* 
	     * We have used all the extents in the xtnt array, so we no
	     * longer need this mcell, so clear the the mcell record
	     * header to delete it.  
	     */
	    pRecord  = (bsMRT *)pMcell->bsMR0;
	    pRecord->bCnt = 0;
	    pRecord->type = 0;
	    pRecord->version = 0;
	} 
 
	/*
	 * Now loop through records.
	 */
	while ((0 != pRecord->type) &&
	       (0 != pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    
	    crossedBoundary = FALSE;
	    
	    switch (pRecord->type) {
		case BSR_XTRA_XTNTS:
		    pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    /*	
		     * Loop on all extents
		     */
		    for (extent = 0; 
			 (extent < BMT_XTRA_XTNTS && 
			  currXtntEntry <= numXtnts &&
			  crossedBoundary == FALSE);
			 extent++) {
			LBNT lbn;

			/*
			 * Now modify the ODS.
			 */
			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_461,
							   "Modified mcell (%d,%d,%d) xtra xtnt entry %d's page from %d to %d.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   extent,	
						   pXtraXtnt->bsXA[extent].bsPage,
						   xtntArray[currXtntEntry].page);

			if (SUCCESS != status) {
			    return status;
			}
			
			if (currVol != xtntArray[currXtntEntry].volume) {
			    /*
			     * Crossed a volume boundary
			     */
			    crossedBoundary = TRUE;
			    lbn = (LBNT)-1;
			} else  if ((extent < (BMT_XTRA_XTNTS - 1)) && 
				    (currXtntEntry < numXtnts)) {
			    /*
			     * Not the last extent in this mcell
			     */
			    lbn = xtntArray[currXtntEntry].lbn; 
			} else {
			    /*
			     * Last extent in this mcell
			     */
			    lbn = (LBNT)-1;
			}
			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_462,
							   "Modified mcell (%d,%d,%d) xtra xtnt entry %d's LBN from %u to %u.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   extent,	 
						   pXtraXtnt->bsXA[extent].vdBlk,
						   lbn);
			if (SUCCESS != status) {
			    return status;
			}

			pXtraXtnt->bsXA[extent].bsPage = xtntArray[currXtntEntry].page;
			pXtraXtnt->bsXA[extent].vdBlk = lbn;
			modified = TRUE;

			currXtntEntry++;

		    } /* end extent for loop */

		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_931,
						       "Modified mcell (%d,%d,%d)'s xCnt from %d to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell, 
					       pXtraXtnt->xCnt,
					       extent);
		    if (SUCCESS != status) {
			return status;
		    }

		    pXtraXtnt->xCnt = extent;

		    currXtntEntry--;
		    modified = TRUE;

		    break;

		case BSR_SHADOW_XTNTS:
		    /*
		     * This will only be for ODSv3, and then
		     * only the first mcell in the chain.
		     */
		    pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));
					      
		    /*
		     * Loop on all extents
		     */
		    for (extent = 0;
			 (extent < BMT_SHADOW_XTNTS &&
			  currXtntEntry <= numXtnts &&
			  crossedBoundary == FALSE);
			 extent++) {
			LBNT lbn;

			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_463, 
							   "Modified mcell (%d,%d,%d) shadow xtnt entry %d's page from %d to %d.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   extent,
						   pShadow->bsXA[extent].bsPage,
						   xtntArray[currXtntEntry].page);
			if (SUCCESS != status) {
			    return status;
			}

			if (currVol != xtntArray[currXtntEntry].volume) {
			    /*
			     * Crossed volume boundaries
			     */
			    lbn = (LBNT) -1;
			    crossedBoundary = TRUE;
			} else if ((extent < (BMT_SHADOW_XTNTS - 1)) && 
				   (currXtntEntry < numXtnts)) {
			    /*
			     * Not the last extent in this mcell
			     */
			    lbn = xtntArray[currXtntEntry].lbn; 
			} else {
			    /*
			     * Last extent in this mcell
			     */
			    lbn = (LBNT) -1;
			}

			status = add_fixed_message(&(pCurrPage->messages), BMT,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_464, 
							   "Modified mcell (%d,%d,%d) shadow xtnt entry %d's LBN from %u to %u.\n"),
						   currMcell.vol,
						   currMcell.page,
						   currMcell.cell,
						   extent,
						   pShadow->bsXA[extent].vdBlk,
						   lbn);
			if (SUCCESS != status) {
			    return status;
			}

			pShadow->bsXA[extent].bsPage = xtntArray[currXtntEntry].page;
			pShadow->bsXA[extent].vdBlk = lbn;
			modified = TRUE;

			currXtntEntry++;

		    } /* end for extent loop */

		    status = add_fixed_message(&(pCurrPage->messages), BMT,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_931,
						       "Modified mcell (%d,%d,%d)'s xCnt from %d to %d.\n"),
					       currMcell.vol,
					       currMcell.page,
					       currMcell.cell, 
					       pShadow->xCnt,
					       extent);
		    if (SUCCESS != status) {
			return status;
		    }
		    pShadow->xCnt = extent;

		    pShadow->bsXA[extent].vdBlk = -1;
		    currXtntEntry--;
		    modified = TRUE;
		    break;
		    
		default:
		    break;
	    } /* End switch of record type */

	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */

	/*
	 * Release Page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrPage, 
				       modified, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	if (TRUE == modified) {
	    modified = FALSE;
	}

	/*
	 * Do we have more extents in the xtntArray?
	 */
	if (currXtntEntry >= numXtnts) {
	    deleteMcell = TRUE;
	    /*
	     * No more extents, so we can free the rest of the mcells
	     * in this chain.  We DON'T want to remove the extents from
	     * the SBM on these mcells.
	     */
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (0 != nextMcell.vol) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currMcell.vol   = nextMcell.vol;
	    currMcell.page  = nextMcell.page;
	    currMcell.cell  = nextMcell.cell;
	} else {
	    currMcell.vol   = 0;
	    currMcell.page  = 0;
	    currMcell.cell  = 0;
	}/* end if nextMcell exists */
    } /* end chain loop */

    /*
     * We might have left some empty mcells at the end of the extent
     * chain as part of correcting the problems.  So call
     * correct_bmt_chain_by_tag to clean them up.
     */
    status = correct_bmt_chain_by_tag(fileset, tag);
    if (SUCCESS != status) {
	if (FIXED != status) {
	    return status;
	}
    } 

    return SUCCESS;
} /* end correct_dir_extents */

/* end fixfdmn_metadata.c */
