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

/* XXX - These definitions were lifted from ftx_privates.h and are
   intended to be exact duplicates.  The compile-time heartache
   created by trying to include kernel header ftx_privates.h was very
   substantial, so we duped the definitions here in despair. Ick. */

typedef enum { ftxNilLR, ftxDoneLR } ftxLRTypeT;

typedef struct {
    ftxLRTypeT type;            /* ftx Log Record Type */
    signed level : 8;           /* ftx node level */
    unsigned atomicRPass : 8;   /* atomic recovery pass number */
    signed member : 16;         /* ftx node member */
    ftxIdT ftxId;               /* ftx tree globally unique id */
    bfDomainIdT bfDmnId;        /* bitfile domain global unique id */
    logRecAddrT crashRedo;      /* crash redo start log address */
    ftxAgentIdT fdl_agentId;    /* ftx opx agent for this ftx node */
    uint32_t contOrUndoRBcnt;   /* continuation/undo opx record byte count */
    uint32_t rootDRBcnt;        /* root done opx record byte count */
    uint32_t opRedoRBcnt;       /* redo opx record byte count */
} ftxDoneLRT;

/*
 * Global
 */
extern nl_catd _m_catd;

/* private prototypes */

static int validate_bmt_page_header(domainT  *domain,
				    pageT    *pPage, 
				    pageNumT pageId, 
				    int      *numErrors,
				    int      is_rbmt);

static int validate_mcell_header(domainT  *domain,
				 pageT    *pPage,
				 pageNumT pageId,
				 cellNumT mcid,
				 int      *numErrors);

static int correct_tag_page_header(domainT  *domain,
				   filesetT *fileset,
				   pageT    *pPage,
				   pageNumT pageNumber,
				   pageNumT tagUninitPg);

static int correct_bmt_chain_by_tag(filesetT *fileset, 
				    tagNodeT *pTagNode,
				    int *sizeRecordArray, 
				    int **recordArray,
				    xtntNumT  *sizeXtntArray, 
				    fobtoLBNT **xtntArray);

static int correct_chain_tag_and_set(filesetT *fileset, 
				     mcidT currMCId, 
				     mcidT prevMCId, 
				     tagNumT primTagNum, 
				     uint32_t primSeqNum, 
				     tagNumT nextTagNum, 
				     pageT *pPage);

static int correct_chain_ACL(filesetT *fileset, 
			     mcidT    primaryMCId, 
			     tagNumT  tagNum);

static int delete_acl(domainT *domain, 
		      mcidT   primaryMCId);

static int get_fset_attr(domainT      *domain,
			 bfTagT       setTag, 
			 bsBfSetAttrT *pFsetAttr);

static int correct_snap_linkage(filesetT *head,
				filesetT *pNode);

static int correct_snap_level(filesetT *head,
			      filesetT *pNode);

static int update_snap_fsattr(filesetT *pFilset);

 
/*
 * Function Name: is_advfs_volume
 *
 * Description: This checks the fake superblock of the volume and, if
 *		needed, the RBMT to see if this is really an AdvFS
 *		volume. No corrections can be made to any errors found
 *		this early, we're just checking.
 *
 *     volNum: The volume which needs to be checked.
 *
 * Output parameters: N/A 
 *
 * Returns: TRUE, FALSE
 * 
 */

int 
is_advfs_volume(volumeT *volume)
{
    char *funcName = "correct_magic_number";
    size_t bytes;
    pageBufT pageBuf;
    bsMPgT   *rbmtPage;
    advfs_fake_superblock_t *superBlk;

    /*
     * Read the Superblock
     */

    if (lseek(volume->volFD, ADVFS_FAKE_SB_BLK * (int64_t)DEV_BSIZE, SEEK_SET) 
	== (off_t)-1) {

        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_192,
			 "Can't lseek to block %ld on '%s'.\n"),
		 ADVFS_FAKE_SB_BLK, volume->volName); 
        perror(Prog);
        return FALSE;
    }

    bytes = read(volume->volFD, pageBuf, ADVFS_METADATA_PGSZ);

    if (bytes == -1) {

        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_193, 
			 "Can't read page at block %ld on '%s'.\n"), 
		 ADVFS_FAKE_SB_BLK, volume->volName);
        perror(Prog);
        return FALSE;

    } else if (bytes != ADVFS_METADATA_PGSZ) {

        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_194,
			 "Can't read page at block %ld on '%s', only read %ld of %d bytes.\n"),
		 ADVFS_FAKE_SB_BLK, volume->volName, 
		 bytes, ADVFS_METADATA_PGSZ);
        return FALSE;

    }
      
    superBlk = (advfs_fake_superblock_t *) pageBuf;

    if ( !ADVFS_VALID_FS_MAGIC(superBlk) ) {

	/* if the fake superblock's magic number is wrong, read the
	   RBMT and see if it's valid. If the RBMT is valid we assume
	   that the superblock is corrupted but that we indeed have an
	   ADVFS file system here. */

        writemsg(SV_VERBOSE, 
		 catgets(_m_catd, S_FSCK_1, FSCK_183, 
			 "Superblock's AdvFS magic number corrupt or missing\n"));

	if (lseek(volume->volFD, RBMT_START_BLK * 
		  (int64_t)DEV_BSIZE, SEEK_SET) == (off_t)-1) {

	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_192,
			     "Can't lseek to block %ld on '%s'.\n"),
		     RBMT_START_BLK, volume->volName); 
	    perror(Prog);
	    return FALSE;
	}

	bytes = read(volume->volFD, pageBuf, ADVFS_METADATA_PGSZ);

	if (bytes == -1) {

	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_193, 
			     "Can't read page at block %ld on '%s'.\n"), 
		     RBMT_START_BLK, volume->volName);
	    perror(Prog);
	    return FALSE;

	} else if (bytes != ADVFS_METADATA_PGSZ) {

	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_194,
			     "Can't read page at block %ld on '%s', only read %ld of %d bytes.\n"),
		     RBMT_START_BLK, volume->volName, bytes, ADVFS_METADATA_PGSZ);
	    return FALSE;

	}
      
	rbmtPage = (bsMPgT *) pageBuf;

	if (rbmtPage->bmtMagicNumber != RBMT_MAGIC) {
	    writemsg(SV_VERBOSE, 
		     catgets(_m_catd, S_FSCK_1, FSCK_176, 
			     "RBMT's magic number also corrupt or missing\n"));
	    return FALSE;
	}
    }
    return TRUE;

} /* end is_advfs_volume */

/*
 * Function Name: correct_magic_number
 *
 * Description: This checks the fake superblock of the volume and, if
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

int 
correct_magic_number(domainT *domain, 
		     volNumT volNum)
{
    char *funcName = "correct_magic_number";
    int	 status;
    int  status2;
    int	 modified;
    pageT    *page;
    pageT    *rbmtPage;
    advfs_fake_superblock_t *superBlk;

    /*
     * Init variables
     */
    modified = FALSE;

    /*
     * Read and verify/fix the fake superblock.
     */

    status = read_page_from_cache(volNum, ADVFS_FAKE_SB_BLK, &page);
    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_326, 
			 "Failed to read the super-block on volume %s.\n"), 
		 domain->volumes[volNum].volName);
	return FAILURE;
    }

    superBlk = (advfs_fake_superblock_t *) page->pageBuf;

    if (!ADVFS_VALID_FS_MAGIC(superBlk)) {

	/* if the fake superblock's magic number is wrong, read the
	   RBMT and see if it's valid. If the RBMT is valid we assume
	   that the superblock is corrupted, so we write the correct
	   magic number in the superblock. */

	status = read_page_from_cache(volNum, RBMT_START_BLK, &rbmtPage);

	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_327,
			     "Failed to read RBMT on volume %s.\n"), 
		     domain->volumes[volNum].volName);
	    return FAILURE;
	}

	status = validate_bmt_page(domain, rbmtPage, 0, TRUE);

	if (status == SUCCESS) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_328, 
			     "Fixing superblock magic number on volume %s.\n"),
		     domain->volumes[volNum].volName);
	    status = add_fixed_message(&(rbmtPage->messages), misc,
				       catgets(_m_catd, S_FSCK_1, FSCK_329, 
					       "Modified magic number from 0x%x to 0x%x.\n"), 
				       superBlk->adv_fs_magic, ADVFS_MAGIC);
	    if (status != SUCCESS) {
		return FAILURE;
	    }

            ADVFS_SET_FS_MAGIC(superBlk);
	    modified = TRUE;
	    status = FIXED;

	} else {
	    status = NON_ADVFS;
	} /* end if a valid BMT page */
	
	/* done with the RBMT, release it to the cache. */
	
	status2 = release_page_to_cache(volNum, RBMT_START_BLK, rbmtPage, 
					FALSE, TRUE);
	if (status2 != SUCCESS) {
	    /* No writemsg here - release_page_to_cache wrote one. */
	    return FAILURE;
	} 

    } /* end if magic number is not correct */

    /* release the fixed superblock back to the cache */

    status2 = release_page_to_cache(volNum, ADVFS_FAKE_SB_BLK, page, 
				    modified, FALSE);
    if (status2 != SUCCESS) {
	/* No writemsg here - release_page_to_cache wrote one. */
	return FAILURE;
   }

    return status;

} /* end correct_magic_number */


/*
 * Function Name: validate_tag_file_page
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
int 
validate_tag_file_page(domainT *domain,
		       pageT *tagFilePage, 
		       pageNumT pageNum,
		       int isRoot)
{
    char *funcName = "validate_tag_file_page";
    int	 numErrors; /* Number of errors found */
    int	 tagCounter;
    int	 startCounter;
    int	 allocTagMapCnt;
    int	 freeTagMapCnt;
    volNumT volNum;
    lbnNumT lbn;
    bsTDirPgT	 *pTagPage;
    bsTDirPgHdrT *pTagPageHdr;
    bsTMapT	 *pTagMap;
    bfTagT  	 tag;
  
    /*
     * Initialize Variables
     */
    numErrors	   = 0;
    allocTagMapCnt = 0;
    freeTagMapCnt  = 0;
    volNum         = tagFilePage->vol;
    lbn            = tagFilePage->pageLBN;

    pTagPage	= (bsTDirPgT *)tagFilePage->pageBuf;
    pTagPageHdr = &(pTagPage->tpPgHdr);
    pTagMap	= &(pTagPage->tMapA[0]);

    /* 
     * Handle page 0, different from other pages. 
     */
    if (pageNum != 0) {
	startCounter = 0;
    } else {
	startCounter = 1;
    }

    /* check markers */

    if (isRoot) {
	if (pTagPageHdr->magicNumber != RTTAG_MAGIC) {
	    writemsg(SV_DEBUG,
		     catgets(_m_catd, S_FSCK_1, FSCK_932, 
			     "%s page header corrupt : magic # xpd: 0x%x fnd: 0x%x.\n"),
		     isRoot ? "Root tag" : "Tag",
		     RTTAG_MAGIC, pTagPageHdr->magicNumber);
	    numErrors++;
	}
    } else {
	if (pTagPageHdr->magicNumber != TAG_MAGIC) {
	    writemsg(SV_DEBUG,
		     catgets(_m_catd, S_FSCK_1, FSCK_932, 
			     "%s page header corrupt : magic # xpd: 0x%x fnd: 0x%x.\n"),
		     isRoot ? "Root tag" : "Tag",
		     TAG_MAGIC, pTagPageHdr->magicNumber);
	    numErrors++;
	}
    }

    if (!COOKIE_EQ(pTagPageHdr->fsCookie, domain->fsCookie)) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_933, 
			 "%s page header corrupt : fsCookie bad.\n"),
		 isRoot ? "Root tag" : "Tag");
	numErrors++;
    }

    /* check page number */

    if (pTagPageHdr->currPage != pageNum) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_330,
			 "Tag page header corrupt : currPage %ld != pageNum %ld.\n"),
		 pTagPageHdr->currPage, pageNum);
	numErrors++;
    }

    /* check freeMap */
    
    if (pTagPageHdr->nextFreeMap > BS_TD_TAGS_PG) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_331, 
			 "Tag page header corrupt : nextFreeMap %d > %d.\n"),
		 pTagPageHdr->nextFreeMap,  BS_TD_TAGS_PG);
	numErrors++;
    }

    for (tagCounter = startCounter; tagCounter < BS_TD_TAGS_PG; tagCounter++) {

	if (pTagMap[tagCounter].tmFlags & BS_TD_IN_USE) {

	    allocTagMapCnt++;
	    /*
	     * Now check mcell
	     */
	    if (pTagMap[tagCounter].tmBfMCId.cell > BSPG_CELLS) {
		writemsg(SV_DEBUG, 
			 catgets(_m_catd, S_FSCK_1, FSCK_332, 
				 "Tag corrupt : mcid.cell %d > %d.\n"),
			 pTagMap[tagCounter].tmBfMCId.cell, BSPG_CELLS);
		numErrors++;
	    }

	} else {
	    freeTagMapCnt++;
	    /*
	     * Now check nextMap
	     */
	    if (pTagMap[tagCounter].tmNextMap > BS_TD_TAGS_PG) {
		writemsg(SV_DEBUG,
			 catgets(_m_catd, S_FSCK_1, FSCK_333,
				 "Tag corrupt : nextMap %d > %d.\n"),
			 pTagMap[tagCounter].tmNextMap, BS_TD_TAGS_PG);
		numErrors++;
	    }
	} /* end if type of tag */
    } /* end tag for loop */

    if (pageNum == 0) {
	/* 
	 * One less available tag map on page 0 
	 */
	freeTagMapCnt++;
    }

    if (pTagPageHdr->numAllocTMaps != allocTagMapCnt) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_334,
			 "Tag page header corrupt : numAllocTMaps %d != %d.\n"),
		 pTagPageHdr->numAllocTMaps, allocTagMapCnt);
	numErrors++;
    }

    if (numErrors > 4) {
	/*
	 * First stab at setting a threshold is 4.
	 *
	 * Page is most likely not a tag file page, or is completely 
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN,
		 catgets(_m_catd, S_FSCK_1, FSCK_337,
			 "While validating tag page on volume %d LBN %ld, found %d errors.\n"),
		 volNum, lbn, numErrors);
	return FAILURE;
    }
    
    return SUCCESS;
} /* end validate_tag_file_page */


/*
 * Function Name: validate_log_file_page
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
int 
validate_log_file_page(domainT *domain, 
		       pageT *logFilePage)
{
    char *funcName = "validate_log_file_page";
    int	 numErrors; /* Number of errors found */
    logPgT	*pLogPage;
    logPgHdrT	*pLogHdr;
    logPgTrlrT	*pLogTrlr;
    volNumT     volNum;
    lbnNumT     lbn;

    /*
     * Init variables.
     */
    volNum    = logFilePage->vol;
    lbn       = logFilePage->pageLBN;
    pLogPage  = (logPgT *)logFilePage->pageBuf;
    pLogHdr   = &(pLogPage->hdr);
    pLogTrlr  = &(pLogPage->trlr);
    numErrors = 0;

    /* check page markers */

    if (pLogHdr->magicNumber != LOG_MAGIC) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_932, 
			 "%s page header corrupt : magic # xpd: 0x%x fnd: 0x%x.\n"),
		 "Log", LOG_MAGIC, pLogHdr->magicNumber);
	numErrors++;
    }

    if (!COOKIE_EQ(pLogHdr->fsCookie, domain->fsCookie)) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_933, 
			 "%s page header corrupt : fsCookie bad.\n"), "Log");
	numErrors++;
    }

    /*
     * The page type is stored in two locations.
     */

    if (BFM_FTXLOG != pLogHdr->pgType) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_338, 
			 "Log page header pgType incorrect.\n"));
	numErrors++;
    }

    if (BFM_FTXLOG != pLogTrlr->pgType) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_338, 
			 "Log page header pgType incorrect.\n"));
	numErrors++;
    }
	 
    /*
     * The FS cookie is stored in two locations.
     */
    if (!COOKIE_EQ(domain->fsCookie, pLogHdr->fsCookie)) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_339,
			 "Log page header fsCookie incorrect.\n"));
	numErrors++;
    }

    if (!COOKIE_EQ(domain->fsCookie, pLogTrlr->fsCookie)) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_340, 
			 "Log page trailer fsCookie incorrect.\n"));
	numErrors++;
    }

    if (numErrors > 2) {
	/*
	 * Starting with a threshold of 2 for a log page
	 *
	 * Page is most likely not a log file page, or is completely 
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN, 
		 catgets(_m_catd, S_FSCK_1, FSCK_341, 
			 "While validating log page on volume %d LBN %ld, found %d errors.\n"),
		 volNum, lbn, numErrors);
	return FAILURE;
    }

    return SUCCESS;
} /* end validate_log_file_page */


/*
 * Function Name: clear_log
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
int 
clear_log(domainT *domain)
{
    char *funcName = "clear_log";
    int b;
    int extent;
    int status;
    int modified;
    int recOffSet;
    uint32_t   currPageLSN;
    uint32_t   priorPageLSN;
    uint32_t   firstPageLSN;
    uint32_t   lastLSN;
    uint32_t   newLSN;
    lsnT       lsn1;
    lsnT       lsn2;
    lbnNumT    currLbn;
    volNumT    currVol;
    pageNumT   page;
    pageNumT   currPage;
    pageNumT   numPageInExtent;
    lbnNumT    extentLbn;
    volNumT    volNum;
    pageNumT   maxPages;
    pageNumT   lastPage;
    volumeT    *volume;
    logPgT     *pLogPage;     /* a log page */
    logPgBlksT *pLogPgBlks;
    fobtoLBNT  *pExtents;     /* The extents to check */
    xtntNumT   numExtents;
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
    maxPages = FOB2PAGE(pExtents[numExtents].fob);
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
	numPageInExtent = FOB2PAGE(pExtents[extent + 1].fob - 
				   pExtents[extent].fob);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all log pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currPage++) {
	    currLbn = extentLbn + (page * ADVFS_METADATA_PGSZ_IN_BLKS);

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (status != SUCCESS) {
		return status;
	    }
	    
	    pLogPage = (logPgT *)pCurrentPage->pageBuf;

	    /*
	     * Check the header magic number.
	     */

	    if (pLogPage->hdr.magicNumber != LOG_MAGIC) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_936,
				 "Log page %ld header's magic number incorrect, was 0x%x expecting 0x%x\n"),
			 currPage, pLogPage->hdr.magicNumber, LOG_MAGIC);
			 
		status = add_fixed_message(&(pCurrentPage->messages), log,
					   catgets(_m_catd, S_FSCK_1, FSCK_344, 
						   "Modified log page %ld's header.\n"),
					   currPage);
		if (status != SUCCESS) {
		    return status;
		}
		pLogPage->hdr.magicNumber = LOG_MAGIC;
		modified = TRUE;
	    }

	    /*
	     * Check the header fsCookie.
	     */

	    if (!COOKIE_EQ(pLogPage->hdr.fsCookie, domain->fsCookie)) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_343,
				 "Log page %ld header's fsCookie incorrect, was %x.%x expecting %x.%x.\n"),
			 currPage,
			 pLogPage->hdr.fsCookie.id_sec,
			 pLogPage->hdr.fsCookie.id_usec,
			 domain->fsCookie.id_sec,
			 domain->fsCookie.id_usec);
			 
		status = add_fixed_message(&(pCurrentPage->messages), log,
					   catgets(_m_catd, S_FSCK_1, FSCK_344, 
						   "Modified log page %ld's header.\n"),
					   currPage);
		if (status != SUCCESS) {
		    return status;
		}
		pLogPage->hdr.fsCookie = domain->fsCookie;
		modified = TRUE;
	    }

	    /*
	     * Check the header page type.
	     */

	    if (BFM_FTXLOG != pLogPage->hdr.pgType) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_937,
				 "Log page %ld header's page type incorrect, was %d expecting %d\n"),
			 currPage, pLogPage->hdr.pgType, BFM_FTXLOG);

		status = add_fixed_message(&(pCurrentPage->messages), log,
					   catgets(_m_catd, S_FSCK_1, FSCK_344, 
						   "Modified log page %ld's header.\n"),
					   currPage);
		if (status != SUCCESS) {
		    return status;
		}
		pLogPage->hdr.pgType = BFM_FTXLOG;
		modified = TRUE;
	    }

	    /*
	     * Check the trailer fsCookie.
	     */

	    if (!COOKIE_EQ(pLogPage->trlr.fsCookie, domain->fsCookie)) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_345, 
				 "Log page %ld trailer's fsCookie incorrect, was %x.%x expecting %x.%x.\n"),
			 currPage,
			 pLogPage->trlr.fsCookie.id_sec,
			 pLogPage->trlr.fsCookie.id_usec,
			 domain->fsCookie.id_sec,
			 domain->fsCookie.id_usec);
			 
		status = add_fixed_message(&(pCurrentPage->messages), log,
					   catgets(_m_catd, S_FSCK_1, FSCK_344, 
						   "Modified log page %ld's header.\n"),
					   currPage);
		if (status != SUCCESS) {
		    return status;
		}
		pLogPage->trlr.fsCookie = domain->fsCookie;
		modified = TRUE;
	    }

	    /*
	     * Check the trailer page type.
	     */

	    if (BFM_FTXLOG != pLogPage->trlr.pgType) {
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_938,
				 "Log page %ld trailer's page type incorrect, was %d expecting %d\n"),
			 currPage, pLogPage->trlr.pgType, BFM_FTXLOG);
	 
		status = add_fixed_message(&(pCurrentPage->messages), log,
					   catgets(_m_catd, S_FSCK_1, FSCK_344, 
						   "Modified log page %ld's header.\n"),
					   currPage);
		if (status != SUCCESS) {
		    return status;
		}
		pLogPage->trlr.pgType = BFM_FTXLOG;
		modified = TRUE;
	    }

	    /*
	     * Now check the LSN of this page.  Is it less than LSN
	     * on the prior page.  If this is the case we have found
	     * the last page in the log.
	     */
	    currPageLSN = pLogPage->hdr.thisPageLSN.num;

	    if (currPage == 0) {
		/*
		 * First Page in log.
		 */
		firstPageLSN = currPageLSN;
	    } else {
		/*
		 * All other pages.
		 */
		lsn1.num = priorPageLSN;
		lsn2.num = currPageLSN;
		if (log_lsn_gt(lsn1, lsn2)) {
		    if (lastPage == -1) {
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
		lsn1.num = currPageLSN;
		lsn2.num = firstPageLSN;
		if (log_lsn_gt(lsn1, lsn2)) {
		    if (lastPage == -1) {
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
	    if (status != SUCCESS) {
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
    if (lastPage == -2) {
	status = zero_log(domain);
	if (status != SUCCESS) {
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

    if (lastPage == -1) {
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
    if (status != SUCCESS) {
	return status;
    }

    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
    if (status != SUCCESS) {
	return status;
    }
   
    pLogPage = (logPgT *)pCurrentPage->pageBuf;
    pLogPgBlks = (logPgBlksT *)pCurrentPage->pageBuf;

    /*
     * Make sure that logPage is clear.
     */
    bzero(pLogPage, ADVFS_METADATA_PGSZ);
    
    /*
     * Fill in the Header values.
     */
    pLogPage->hdr.thisPageLSN.num = newLSN;;
    pLogPage->hdr.magicNumber = LOG_MAGIC;
    pLogPage->hdr.fsCookie = domain->fsCookie;
    pLogPage->hdr.pgType = BFM_FTXLOG;
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
    dlrp->bfDmnId = domain->fsCookie; 
    dlrp->crashRedo.page = lastPage;
    dlrp->crashRedo.offset = 0;
    dlrp->crashRedo.lsn.num = newLSN;
    dlrp->fdl_agentId = FTA_FTX_CHECKPOINT_LOG;
    dlrp->contOrUndoRBcnt = 0;
    dlrp->rootDRBcnt = 0;
    dlrp->opRedoRBcnt = 0;

    /*
     * Fill in the Trailer values.
     */
    pLogPage->trlr.pgType = BFM_FTXLOG;
    pLogPage->trlr.fsCookie = domain->fsCookie;

    /* 
     * Puts the page's first record's LSN into each block in the page
     * (except the first block which already has the LSN in it).  The
     * value overwritten by the LSN is saved in the "lsnOverwriteVal"
     * array of the page's trailer.  Also, sets the 'check bit'
     * (version) of each block.  
     */
    for (b = 1; b < ADVFS_METADATA_PGSZ_IN_BLKS; b++) {
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
			       catgets(_m_catd, S_FSCK_1, FSCK_342,
				       "Modified log page %ld so the log is marked as empty.\n"),
			       lastPage);
    if (status != SUCCESS) {
	return status;
    }

    status = release_page_to_cache(currVol, currLbn, pCurrentPage,
				   TRUE, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    return SUCCESS;
} /* end clear_log */


/*
 * Function Name: zero_log
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
int 
zero_log(domainT *domain)
{
    char *funcName = "zero_log";
    int	 status;
    lbnNumT   extentLbn;
    lbnNumT   currLbn;
    volNumT   currVol;
    volNumT   volNum;
    pageNumT  page;
    pageNumT  currPage;
    pageNumT  numPageInExtent;
    volumeT   *volume;
    logPgT    *pLogPage;     /* a log page */
    fobtoLBNT *pExtents;     /* The extents to check */
    xtntNumT  numExtents;
    xtntNumT  extent;
    pageT     *pCurrentPage; /* The current page we are checking */

    /*
     * Initialize variables
     */
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
	numPageInExtent = FOB2PAGE(pExtents[extent + 1].fob - 
				   pExtents[extent].fob);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all log pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currPage++) {
	    currLbn = extentLbn + (page * ADVFS_METADATA_PGSZ_IN_BLKS);

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (status != SUCCESS) {
		return status;
	    }

	    pLogPage = (logPgT *)pCurrentPage->pageBuf;

	    /*
	     * Make sure that logPage is clear.
	     */
	    bzero(pLogPage, ADVFS_METADATA_PGSZ);

	    /*
	     * Fill in the default values.
	     */
	    pLogPage->hdr.pgType = BFM_FTXLOG;
	    pLogPage->hdr.magicNumber = LOG_MAGIC;
	    pLogPage->hdr.fsCookie = domain->fsCookie;
	    pLogPage->hdr.pgSafe = TRUE;
	    pLogPage->hdr.curLastRec = 0;
	    pLogPage->hdr.prevLastRec = -1;
	    pLogPage->hdr.thisPageLSN.num = 0;

	    pLogPage->trlr.pgType = BFM_FTXLOG;
	    pLogPage->trlr.fsCookie = domain->fsCookie;

	    status = add_fixed_message(&(pCurrentPage->messages), log,
				       catgets(_m_catd, S_FSCK_1, FSCK_342,
					       "Modified log page %ld so the log is marked as empty.\n"),
				       currPage);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = release_page_to_cache(currVol, currLbn, pCurrentPage,
					   TRUE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	} /* end page loop */
    } /* end extent loop */

    return SUCCESS;
} /* end zero_log */


/*
 * Function Name: validate_bmt_page
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
int 
validate_bmt_page(domainT *domain,
		  pageT *pPage, 
		  pageNumT pageId,
		  int is_rbmt)
{
    char *funcName = "validate_bmt_page";
    int	 status;
    int	 numErrors; /* Number of errors found */
    cellNumT mcell;
    volNumT volNum;

    /*
     * Init variables.
     */
    volNum = pPage->vol;
    numErrors = 0;

    status = validate_bmt_page_header(domain, pPage, pageId, 
				      &numErrors, is_rbmt);
    if (status == CORRUPT) {
	/*
	 * We validate the mcell headers in this page to see if header
	 * is just corrupted, or if this is really not a BMT page.
	 */
	for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
	    status = validate_mcell_header(domain, pPage, pageId,
					   mcell, &numErrors);
	    if (status == CORRUPT) {
		writemsg(SV_DEBUG, 
			 catgets(_m_catd, S_FSCK_1, FSCK_347, 
				 "Mcell (%d,%ld,%d)'s header is corrupt.\n"),
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

    if (numErrors >= 3) {
	/*
	 * This page could have a few bad values and still be a BMT,
	 * so we set the error threshold to 3.  If we're here, the
	 * page is most likely not a BMT file page, or is completely
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN, 
		 catgets(_m_catd, S_FSCK_1, FSCK_348, 
			 "While validating %s page on volume %s page %ld, found %d %s.\n"),
		 is_rbmt ? "RBMT" : "BMT", domain->volumes[volNum].volName, 
		 pageId, numErrors, numErrors > 1 ? "errors" : "error");
	return FAILURE;
    }

    return SUCCESS;
} /* end validate_bmt_page */


/*
 * Function Name:validate_bmt_page_header
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
static int 
validate_bmt_page_header(domainT *domain, 
			 pageT *pPage, 
			 pageNumT pageId, 
			 int *numErrors,
			 int is_rbmt)
{
    char *funcName = "validate_bmt_page_header";
    uint64_t maxBmtFob;
    int      errorsFound;
    volNumT  volNum;
    bsMPgT   *bmtPage; /* The current BMT page we are working on */
    volumeT  *volume;

    /*
     * Init Variables.
     */
    errorsFound = 0;
    bmtPage = (bsMPgT *)pPage->pageBuf;
    volNum  = pPage->vol;
    volume  = &(domain->volumes[volNum]);

    /* check the magic number.  If the magic number is wrong, that's a
       pretty good indicator that this is not a BMT/RBMT, so that
       should count for more than one error towards the caller's count
       threshold vlaue. */

    if (is_rbmt) {

	if (bmtPage->bmtMagicNumber != RBMT_MAGIC) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FSCK_1, FSCK_920,
			     "volume %d RBMT magicNum bad, fnd 0x%x, xpd 0x%x\n"),
		     volNum, bmtPage->bmtMagicNumber, RBMT_MAGIC);
	    errorsFound++;
	    *numErrors++; /* emphasize this error */
	}
    } else {
	
	if (bmtPage->bmtMagicNumber != BMT_MAGIC) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FSCK_1, FSCK_922,
			     "volume %d BMT magicNum bad, fnd 0x%x, xpd 0x%x\n"),
		     volNum, bmtPage->bmtMagicNumber, RBMT_MAGIC);
	    errorsFound++;
	    *numErrors++; /* emphasize this error */
	}
    }

    /*
     * We guestimate the FOB value by calculating how many FOBs are
     * in the volume.  We then use this value to decide if an mcell
     * page address *could* be out of range.
     */

    if ((bmtPage->bmtNextFreeMcell == CELL_TERM) ||
	(bmtPage->bmtNextFreeMcell > BSPG_CELLS)) {
	/*
	 * falls outside of the valid range
	 */
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_349,
			 "%s page (%d,%ld)'s nextFreeMCId.cell = %d, outside valid range.\n"),
		 is_rbmt ? "RBMT" : "BMT", volNum, pageId, 
		 bmtPage->bmtNextFreeMcell);
	errorsFound++;
    }

    /* if we don't yet know the volume size, skip this check */
    
    maxBmtFob = volume->volSize / ADVFS_FOBS_PER_DEV_BSIZE;

    if (bmtPage->bmtNextFreePg != BMT_PG_TERM &&
	bmtPage->bmtNextFreePg > FOB2PAGE(maxBmtFob)) {
	/*
	 * falls outside of the valid range:
	 */
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_350, 
			 "%s page (%d,%ld)'s nextFreePg = %d, outside valid range.\n"), 
		 is_rbmt ? "RBMT" : "BMT", volNum, pageId, 
		 bmtPage->bmtNextFreePg);
	errorsFound++;
    }

    if (bmtPage->bmtPageId != pageId) {
	/*
	 * Not the correct page id.
	 */
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_351, 
			 "%s page (%d,%ld)'s pageId = %ld, invalid Page.\n"), 
		 is_rbmt ? "RBMT" : "BMT", volNum, pageId, bmtPage->bmtPageId);
	errorsFound++;
    }

/*
 * TODO-ODS: verify vdIndex and vdCookie from BMT page header
 *           verify nextFreeMcell and freeMcellCnt make sense
 */

    *numErrors += errorsFound;

    if (errorsFound) {
	/*
	 * Page is most likely not a BMT file page, or is completely 
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN, 
		 catgets(_m_catd, S_FSCK_1, FSCK_353, 
			 "While validating %s page header on volume %s page %ld, found %d errors.\n"),
		 is_rbmt ? "RBMT" : "BMT", domain->volumes[volNum].volName, 
		 pageId, errorsFound);
	return CORRUPT;
    }

    return SUCCESS;
} /* end validate_bmt_page_header */


/*
 * Function Name:validate_mcell_header
 *
 * Description: This function needs to validate the format of the BMT
 *		mcell record.  It does not check to see if the actual
 *		data is correct, but if it falls within valid values.
 *
 * Input parameters:
 *     domain: Pointer to the domain being checked.
 *     pPage: The page to check the header on.
 *     pageId: The page number we are checking.
 *     mcid: Which mcell on the page to check.
 *     rbmtFlag: FALSE equals BMT, TRUE equals RBMT.
 *     numErrors: The current number of errors found.
 *
 * Output parameters:
 *     numErrors: The number errors found in the mcell header.
 *
 * Returns: SUCCESS, CORRUPT.  
 */
static int 
validate_mcell_header(domainT *domain, 
		      pageT *pPage, 
		      pageNumT pageId,
		      cellNumT mcid, 
		      int *numErrors)
{
    char    *funcName = "validate_mcell_header";
    int	    errorsFound; /* Counter of the number of corruptions */
    pageNumT maxBmtFob;
    volNumT volNum;
    bsMPgT  *bmtPage; /* The current BMT page we are working on */
    bsMCT   *pMcell;  /* The current BMT mcell we are working on */
    volumeT *volume;
    
    /*
     * Init Variables.
     */
    errorsFound = 0;
    volNum  = pPage->vol;
    bmtPage = (bsMPgT *)pPage->pageBuf;
    pMcell  = &(bmtPage->bsMCA[mcid]);

    /*
     * Use BS_MAX_VDI not MAX_VOLUMES as this is an ODS we are looking at.
     */
    if (FETCH_MC_VOLUME(pMcell->mcNextMCId) == VOL_TERM ||
	FETCH_MC_VOLUME(pMcell->mcNextMCId) > BS_MAX_VDI) {
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_354,
			 "Mcell (%d,%ld,%d)'s mcNextMCId.volume %d, is outside valid range.\n"), 
		 volNum, pageId, mcid, FETCH_MC_VOLUME(pMcell->mcNextMCId));
	errorsFound++;
    } else if (pMcell->mcNextMCId.volume != 0) {

	/* if this mcell is linked to another, sniff at the link to
	   see if it makes sense */

	volume	= &(domain->volumes[pMcell->mcNextMCId.volume]);
	if (volume->volFD == -1) {
	    writemsg(SV_DEBUG,
		     catgets(_m_catd, S_FSCK_1, FSCK_355, 
			     "Mcell (%d,%ld,%d)'s mcNextMCId.volume %d, unknown volume.\n"), 
		     volNum, pageId, mcid, pMcell->mcNextMCId.volume);
	    errorsFound++;

	    maxBmtFob = 0;
	} else {
	    /*
	     * maxBmtFob is the largest FOB number we could ever find
	     * in the RBMT or BMT, based on how many FOBs there are in
	     * the volume.
	     */
	    maxBmtFob = volume->volSize / ADVFS_FOBS_PER_DEV_BSIZE;
	}	

	if (FETCH_MC_PAGE(pMcell->mcNextMCId) == PAGE_TERM ||
	    pMcell->mcNextMCId.page > FOB2PAGE(maxBmtFob)) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FSCK_1, FSCK_356, 
			     "Mcell (%d,%ld,%d)'s mcNextMCId.page %ld, is outside valid range.\n"), 
		     volNum, pageId, mcid, FETCH_MC_PAGE(pMcell->mcNextMCId));
	    errorsFound++;
	}

	if (FETCH_MC_CELL(pMcell->mcNextMCId) == CELL_TERM ||
	    (pMcell->mcNextMCId.cell >= BSPG_CELLS)) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FSCK_1, FSCK_357,
			     "Mcell (%d,%ld,%d)'s mcNextMCId.cell %d, is outside valid range.\n"), 
		     volNum, pageId, mcid, FETCH_MC_CELL(pMcell->mcNextMCId));
	    errorsFound++;
	}
    } else {
	/*
	 * This mcell is not linked to another mcell
	 * (pMcell->mcNextMCId.volume = 0), so the page # must must be self.
	 */

	if (pMcell->mcNextMCId.page != pageId) {
	    writemsg(SV_DEBUG, 
		     catgets(_m_catd, S_FSCK_1, FSCK_358,
			     "Mcell (%d,%ld,%d)'s mcNnextMCId (%d,%ld,%d) is invalid.\n"), 
		     volNum, pageId, mcid, pMcell->mcNextMCId.volume, 
		     pMcell->mcNextMCId.page, pMcell->mcNextMCId.cell);
	    errorsFound++;
	}
    } /* if MCId.volume */

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
    if ((BS_BFTAG_RSVD(pMcell->mcTag) && !(IS_ROOT_TAG(pMcell->mcBfSetTag)) ||
	(pMcell->mcTag.tag_num == 0 && pMcell->mcBfSetTag.tag_num != 0) ||
	(BS_BFTAG_REG(pMcell->mcTag) && !(IS_ROOT_TAG(pMcell->mcBfSetTag) || 
					pMcell->mcBfSetTag.tag_num > 0)))) {

	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_359, 
			 "Mcell (%d,%ld,%d)'s tag %d bfSetTag %d is invalid.\n"), 
		 volNum, pageId, mcid, 
		 pMcell->mcTag.tag_num, pMcell->mcBfSetTag.tag_num);
	errorsFound++;
    }

    *numErrors += errorsFound;

    if (errorsFound > 1) {
	/*
	 * Page is most likely not a BMT file page, or is completely
	 * corrupted.
	 */
	writemsg(SV_DEBUG | SV_LOG_WARN,
		 catgets(_m_catd, S_FSCK_1, FSCK_360, 
			 "While validating mcell header on (%d,%ld,%d), found %d errors.\n"),
		 volNum, pageId, mcid, errorsFound);
	return CORRUPT;
    }

    return SUCCESS;
} /* end validate_mcell_header */


/*
 * Function Name: reset_free_mcell_links
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
int 
reset_free_mcell_links(pageT *pPage, 
		       mcellUsage *freeMcells)
{
    char *funcName = "reset_free_mcell_links";
    int	 foundFirst;
    cellNumT prevMcell;
    cellNumT mcell;
    volNumT pageNum;
    bsMPgT     *bmtPage;     /* The current BMT page we are working on */
    bsMCT      *pMcell;	     /* The current BMT mcell we are working on */
    mcellUsage *pMcellArray;
  
    /*
     * Init variables
     */
    bmtPage = (bsMPgT *)pPage->pageBuf;
    prevMcell	      = -1;
    pageNum	      = bmtPage->bmtPageId;

    /*
     * If array is not passed in create it.
     */
    assert(NULL != freeMcells);
    pMcellArray = freeMcells;

    /*
     * Deferred Delete List and mcell Free list is a special case 
     * (tag & seq equals 0).
     */
    if (pageNum == 0) {
	pMcellArray[0] = USED;
    }

    /*
     * Loop through the mcells on this page to reset the links.
     */
    foundFirst = FALSE;
    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
	if ((FREE == pMcellArray[mcell]) ||
	    (ONLIST == pMcellArray[mcell])) {
	    if (foundFirst == FALSE) {
		foundFirst = TRUE;
		bmtPage->bmtNextFreeMcell = mcell;
		prevMcell = mcell;
	    } else {
		pMcell = &(bmtPage->bsMCA[prevMcell]);
                pMcell->mcNextMCId.volume = bmtPage->bmtVdIndex; 
		pMcell->mcNextMCId.page = pageNum;
		pMcell->mcNextMCId.cell = mcell;
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
        pMcell->mcNextMCId.volume = 0;
	pMcell->mcNextMCId.page = 0;
	pMcell->mcNextMCId.cell = 0;
    }

    return SUCCESS;
} /* end reset_free_mcell_links */


/*
 * Function Name: correct_free_mcell_list
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
int 
correct_free_mcell_list(volumeT *volume, 
			int *bmtStatusArray)
{
    char *funcName = "correct_free_mcell_list";
    int	 status;
    int	 extent;
    int	 modified;
    int	 terminator;
    pageNumT currPageNum;
    lbnNumT  currLbn;
    volNumT  currVol;
    pageNumT priorPageNum;
    lbnNumT  priorLbn;
    volNumT  priorVol;
    pageNumT headPageNum;
    lbnNumT  headLbn;
    volNumT  headVol;
    pageNumT nextPageNum;
    pageNumT firstPageNum;
    pageNumT numPageInExtent;
    lbnNumT  extentLbn;
    pageNumT page;
    pageT  *pHeadPage;	   /* Pointer to first BMT Page */
    pageT  *pCurrPage;	   /* Pointer to current page. */
    pageT  *pPriorPage;	   /* Pointer to prior page. */
    bsMPgT *bmtPage;	   /* The current BMT page we are working on */
    bsMPgT *headBmtPage;   /* The first BMT page */
    bsMPgT *priorBmtPage;  /* The prior BMT page we are working on */
    bsMCT  *pMcell;	   /* The BMT mcell we are working on */
    bsMRT  *pRecord;
    fobtoLBNT *pExtents;
    xtntNumT numExtents;
    bsMcellFreeListT *headRecord; /* mcell free list record */

    /*
     * Init variables
     */
    pExtents   = volume->volRbmt->bmtLBN;
    numExtents = volume->volRbmt->bmtLBNSize;
    headPageNum = 0;
    terminator  = -1;

    status = convert_page_to_lbn(pExtents, numExtents, headPageNum, 
				 &headLbn, &headVol);
    if (status != SUCCESS) {
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
    if (status != SUCCESS) {
	return status;
    }
      
    bmtPage = (bsMPgT *)pCurrPage->pageBuf;
    pMcell  = &(bmtPage->bsMCA[0]);
    pRecord = (bsMRT *)pMcell->bsMR0;
    firstPageNum = -2;

    while ((pRecord->type != BSR_NIL) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	if (pRecord->type == BSR_MCELL_FREE_LIST) {
	    headRecord = (bsMcellFreeListT *)((char *)pRecord + sizeof(bsMRT));
	    firstPageNum = headRecord->headPg;
	    break;
	}
	/*
	 * Set pRecord to next record  of pMcell.
	 */

	pRecord = NEXT_MREC(pRecord);

    } /* end mcell record loop */

    /*
     * Release the page which has the head record.
     */
    status = release_page_to_cache(currVol, currLbn, pCurrPage, FALSE, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    if (firstPageNum == -2) {
	/*
	 * Unable to find the record which contains the head record.
	 */
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_363,
			 "Unable to locate the head of the free mcell list.\n"));
	return CORRUPT;
    }

    if (firstPageNum == terminator) {
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
	if (status != SUCCESS) {
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrPage);
	if (status != SUCCESS) {
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
	nextPageNum = bmtPage->bmtNextFreePg;
	
	if (currPageNum == nextPageNum) {
	    /*
	     * We have a loop on the free page list.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_364, 
			     "Modifying volume %s's free mcell list, found loop in list.\n"),
		     volume->volName);

	    status = add_fixed_message(&(pCurrPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_365, 
					       "Modified free mcell list, cleared loop.\n"));
	    if (status != SUCCESS) {
		return status;
	    }
	    nextPageNum = terminator;
	    bmtPage->bmtNextFreePg = terminator;
	    modified = TRUE;
	}

	if (bmtStatusArray[currPageNum] != BMTPAGE_FULL) {
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
		if (status != SUCCESS) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_128,
				     "Can't convert BMT page %ld to LBN.\n"), 
			     priorPageNum);
		    return FAILURE;
		}

		status = read_page_from_cache(priorVol, priorLbn, 
					      &pPriorPage);
		if (status != SUCCESS) {
		   return status;
		} 

		status = add_fixed_message(&(pPriorPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_367, 
						   "Modified free mcell list, found full page on list.\n"));
		if (status != SUCCESS) {
		    return status;
		}

		priorBmtPage = (bsMPgT *)pPriorPage->pageBuf;
		priorBmtPage->bmtNextFreePg = bmtPage->bmtNextFreePg;

		status = release_page_to_cache(priorVol, priorLbn,
					       pPriorPage, TRUE, FALSE);
		if (status != SUCCESS) {
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
		    if (status != SUCCESS) {
			return status;
		    }
		} else {
		    pHeadPage = pCurrPage;
		}

		headBmtPage = (bsMPgT *)pHeadPage->pageBuf;
		pMcell	= &(headBmtPage->bsMCA[0]);
		pRecord = (bsMRT *)pMcell->bsMR0;

		while ((pRecord->type != BSR_NIL) &&
		       (pRecord->bCnt != 0) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		    if (pRecord->type == BSR_MCELL_FREE_LIST) {
			headRecord = (bsMcellFreeListT *)((char *)pRecord + 
							  sizeof(bsMRT));
			/*
			 * Set head pointer
			 */
			status = add_fixed_message(&(pHeadPage->messages),
						   BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_368,
							   "Modified head of BMT free mcell list from %d to %d.\n"),
						   headRecord->headPg,
						   bmtPage->bmtNextFreePg);
			if (status != SUCCESS) {
			    return status;
			}
			headRecord->headPg = bmtPage->bmtNextFreePg;
			break;
		    }
		    /*
		     * Set pRecord to next record  of pMcell.
		     */

		    pRecord = NEXT_MREC(pRecord);

		} /* end mcell record loop */
		if (currPageNum != headPageNum) {
		    status = release_page_to_cache(headVol, headLbn, 
						   pHeadPage, TRUE, FALSE);
		    if (status != SUCCESS) {
			return status;
		    }
		} else {
		    pHeadPage = NULL;
		}
	    } /* end else currPageNum == nextPageNum */
	    bmtPage->bmtNextFreePg = terminator;
	    modified = TRUE;
	}
	status = release_page_to_cache(currVol, currLbn, pCurrPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
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
	    if (status != SUCCESS) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_128, 
				 "Can't convert BMT page %ld to LBN.\n"), 
			 currPageNum);
		return FAILURE;
	    }

	    status = read_page_from_cache(currVol, currLbn, &pCurrPage);
	    if (status != SUCCESS) {
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
	numPageInExtent = FOB2PAGE(pExtents[extent + 1].fob - 
				   pExtents[extent].fob);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent, loop through all BMT file pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currPageNum++) {

	    currLbn = extentLbn + (page * ADVFS_METADATA_PGSZ_IN_BLKS);
	    
	    if (bmtStatusArray[currPageNum] == BMTPAGE_FREE) {
		/*
		 * Page is marked free, but is not on the free list.
		 */
		status = read_page_from_cache(currVol, currLbn, &pCurrPage);
		if (status != SUCCESS) {
		    return status;
		} 
		if (currPageNum != headPageNum) {
		    status = read_page_from_cache(headVol, headLbn, &pHeadPage);
		    if (status != SUCCESS) {
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

		while ((pRecord->type != BSR_NIL) &&
		       (pRecord->bCnt != 0) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

		    if (pRecord->type == BSR_MCELL_FREE_LIST) {
			headRecord = (bsMcellFreeListT *)((char *)pRecord + 
							  sizeof(bsMRT));
			bmtPage->bmtNextFreePg = headRecord->headPg;
			headRecord->headPg = currPageNum;
			break;
		    }
		    /*
		     * Set pRecord to next record  of pMcell.
		     */

		    pRecord = NEXT_MREC(pRecord);

		} /* end mcell record loop */

		if (currPageNum != headPageNum) {
		    status = release_page_to_cache(headVol, headLbn, 
						   pHeadPage, TRUE, FALSE);
		    if (status != SUCCESS) {
			return status;
		    }
		} else {
		    pHeadPage = NULL;
		}

		status = add_fixed_message(&(pCurrPage->messages),
					   BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_369, 
						   "Added page %ld to free BMT page list.\n"),
					   currPageNum);
		if (status != SUCCESS) {
		    return status;
		}

		status = release_page_to_cache(currVol, currLbn,
						   pCurrPage, TRUE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
	    } /* end if page not on free list */
	} /* end for page loop */
    } /* end for extent loop */

    return SUCCESS;
} /* end correct_free_mcell_list */


/*
 * Function Name: clear_deferred_delete_list
 *
 * Description: The deferred delete list is a list of mcells of files 
 *              (or parts of files) which are to be deleted from the
 *              disk, but for one reason or another were not deleted.
 *              Normally at mount time the kernel will process this
 *              list and remove these mcells as part of FS activation,
 *              but as we will be making changes to the BMT and mcells
 *              that could change one of our corrections when the FS
 *              is next activated.  So instead we are going to walk
 *              the deferred delete list and mark the mcells as not in
 *              use.
 *
 * Input parameters:
 *     domain: The domain to clear the deferred delete list on.
 *
 * Output parameters:N/A
 *
 * Returns: SUCCESS, FIXED, FAILURE, NO_MEMORY
 * 
 */
int 
clear_deferred_delete_list(domainT *domain)
{
    char *funcName = "clear_deferred_delete_list";
    int	 status;
    int	 modified;
    int  ddlModified;
    volNumT    volNum;
    pageNumT   bmtHeadPage;
    volNumT    currVol;
    lbnNumT    currLbn;
    lbnNumT    lbn;
    fobtoLBNT  *pExtents;
    xtntNumT   numExtents;
    volumeT    *volumeArray;
    volumeT    *volume;
    pageT      *pCurrPage; /* Pointer to current page. */
    bsMPgT     *bmtPage;   /* The current BMT page we are working on */
    bsMCT      *pMcell;	   /* The current BMT mcell we are working on */
    bsMRT      *pRecord;   /* Pointer to current mcell record. */
    delLinkRT  *delLink;
    bsXtntRT   *pXtnt;
    bsBfAttrT  *pBSRAttr;
    bsBfSetAttrT fsetAttr;
    mcidT      currMCId;
    mcidT      nextDelMCId;
    mcidT      nextMCId;
    mcidT      chainMCId;
 
    /*
     * Init variables.
     */
    volumeArray = domain->volumes;
    ddlModified = FALSE;

    bmtHeadPage = 0;

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	/* 
	 * Check to see if the current volume has a valid file descriptor.
	 */
	if (volumeArray[volNum].volFD == -1) {
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
	currMCId.volume  = pExtents[0].volume;
	currMCId.page = bmtHeadPage;
	currMCId.cell = 0;

	status = convert_page_to_lbn(pExtents, numExtents, currMCId.page,
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrPage);
	if (status != SUCCESS) {
	    return status;
	}
	bmtPage	 = (bsMPgT *)pCurrPage->pageBuf;
	pMcell	 = &(bmtPage->bsMCA[currMCId.cell]);
	pRecord	 = (bsMRT *)pMcell->bsMR0;

	/*
	 * Init the next DDL pointer.
	 */
	ZERO_MCID(nextDelMCId);

	/*
	 * Now loop through this mcell looking for the DDL.
	 */
	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BSR_DEF_DEL_MCELL_LIST) {
		delLink = (delLinkRT *)((char *)pRecord + sizeof(bsMRT));

		/*
		 * Save the next record.
		 */
		nextDelMCId.page = delLink->nextMCId.page;
		nextDelMCId.cell = delLink->nextMCId.cell;

		if ((nextDelMCId.page == 0) && (nextDelMCId.cell == 0)) {
		    /*
		     * empty DDL
		     */
		    nextDelMCId.volume = 0;
		    
		    if ((delLink->prevMCId.page == 0) &&
			(delLink->prevMCId.cell == 0)) {
			/*
			 * DDL is already empty.
			 */
			break;
		    }
		} else {
		    nextDelMCId.volume = currMCId.volume;
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
					   catgets(_m_catd, S_FSCK_1, FSCK_53, 
						   "Cleared the deferred delete list.\n"));
		if (status != SUCCESS) {
		    return status;
		}
		modified = TRUE;
		ddlModified = TRUE;
		break;
	    } /* end if DDL record */
	    /*
	     * Set pRecord to next record  of pMcell.
	     */

	    pRecord = NEXT_MREC(pRecord);

	} /* end loop through record */

	/*
	 * Release the page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrPage,
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	modified = FALSE;

	/*
	 * Loop to process each file (mcell chain) on the DDL
	 */
	while (nextDelMCId.volume != 0) {
	    /*
	     * Set curr to the first primary in DDL.
	     */
	    currMCId = nextDelMCId;
	    ZERO_MCID(chainMCId);
	    ZERO_MCID(nextDelMCId);

	    /*
	     * Loop on all mcells in this file, free when finished.
	     */
	    while (currMCId.volume != 0) {
		/*
		 * Load current page.
		 */
		status = convert_page_to_lbn(pExtents, numExtents,
					     currMCId.page, &currLbn, 
					     &currVol);
		if (status != SUCCESS) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
			     catgets(_m_catd, S_FSCK_1, FSCK_909,
				     "Truncating deferred delete list, page number %ld is invalid.\n"),
			     currMCId.page);
		    /*
		     * We can not continue walking the DDL, as the 
		     * page is invalid.
		     *
		     * Do NOT return failure, instead break out of loop.
		     */
		    break;
		}

		status = read_page_from_cache(currVol, currLbn, &pCurrPage);
		if (status != SUCCESS) {
		  return status;
		}
		bmtPage	 = (bsMPgT *)pCurrPage->pageBuf;
		pMcell	 = &(bmtPage->bsMCA[currMCId.cell]);
		pRecord	 = (bsMRT *)pMcell->bsMR0;

		nextMCId = pMcell->mcNextMCId;

		/*
		 * Now loop through all records in this mcell, this mcell
		 * can be the primary or on the chain.
		 */
		while ((pRecord->type != BSR_NIL) &&
		       (pRecord->bCnt != 0) &&
		       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

		    switch (pRecord->type) {
		      case (BSR_ATTR):
			pBSRAttr = (bsBfAttrT *)((char *)pRecord + 
						 sizeof(bsMRT));
			/*
			 * If this file is part of a snap, we need to
			 * mark it and it's fileset out of sync.  We
			 * lookup the fileset Tag file for this mcell
			 * to see if it's a snap.
			 */

			status = get_fset_attr(domain,
					       pMcell->mcBfSetTag, 
					       &fsetAttr);

			if (status != SUCCESS) {
			    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
				     catgets(_m_catd, S_FSCK_1, FSCK_994,
					     "BSR_BFS_ATTR record for tagdir %ld.%u not found.\n"),
				     pMcell->mcBfSetTag.tag_num, 
				     pMcell->mcBfSetTag.tag_seq);
			    return (FAILURE);
			}

			if (fsetAttr.bfsaSnapLevel) {

			    ddlSnapFileT *pDdlSnapFile;

			    /*
			     * This file's parent fileset directory is
			     * a snapshot.  A file whose parent is a
			     * snapshot could end up on the DDL if the
			     * parent fileset was being deleted at the
			     * time that the system crashed.  If fsck
			     * has been run "-o nolog" then the domain
			     * was not activated by fsck and so the
			     * DDL was not porcessed and could contain
			     * files.  Prior to clearing the DDL, we
			     * need build a list of the snap files we
			     * find and the fileset that owns each
			     * one.  Then later we'll come back and
			     * mark the files and their file sets as
			     * "out of sync", so that the system will
			     * do the right thing when it runs.
			     */
			    
			    /*
			     * Problem: We do not have a tag list at this
			     * point.  So we will need to save off a list
			     * of 'tags' which we will then need to walk
			     * later to mark them out of sync.  This list
			     * will be attached to the domain as we don't
			     * have the fileset structures either.
			     */
			    pDdlSnapFile = (ddlSnapFileT *)ffd_malloc(sizeof(ddlSnapFileT));
			    if (pDdlSnapFile == NULL) {
				writemsg(SV_ERR | SV_LOG_ERR, 
					 catgets(_m_catd, S_FSCK_1, FSCK_10,
						 "Can't allocate memory for %s.\n"),
					 catgets(_m_catd, S_FSCK_1, FSCK_371,
						 "deferred delete snap file"));
				return NO_MEMORY;
			    }
			    
			    pDdlSnapFile->setTag = pMcell->mcBfSetTag;
			    pDdlSnapFile->fileTag = pMcell->mcTag;
			    /*
			     * add to domain's ddl snap file linked list.
			     */
			    pDdlSnapFile->next = domain->ddlSnapList;
			    domain->ddlSnapList = pDdlSnapFile;

			} /* end: if this file is a snap */

			break;

		      case (BSR_XTNTS):
			pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
			/*
			 * First save the info on the next DDL entry.
			 */
			nextDelMCId.page = pXtnt->delLink.nextMCId.page;
			nextDelMCId.cell = pXtnt->delLink.nextMCId.cell;

			if ((nextDelMCId.page == 0) && 
			    (nextDelMCId.cell == 0)) {
			    /*
			     * empty DDL
			     */
			    nextDelMCId.volume = 0;
			} else {
			    nextDelMCId.volume = currMCId.volume;
			}

			/*
			 * Save the info on the head of the chain.
			 */
			chainMCId = pXtnt->chainMCId;
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

		    pRecord = NEXT_MREC(pRecord);

		} /* end of record loop */
		/*
		 * Zero out this mcell. includes freeing extents
		 */
		status = free_mcell(domain, currVol, pCurrPage, currMCId.cell);
		if (status != SUCCESS) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_130,
				     "Can't free mcell %d on volume %s BMT page %ld.\n"),
				     currMCId.cell, 
				     volumeArray[currVol].volName,
				     currMCId.page);
		    return FAILURE;
		}
		
		status = add_fixed_message(&(pCurrPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_373, 
						   "Deleted mcell (%d,%ld,%d) from deferred delete list.\n"), 
					   currVol, currMCId.page, 
					   currMCId.cell);
		if (status != SUCCESS) {
		    return status;
		}
		modified = TRUE;
		ddlModified = TRUE;

		/*
		 * Release the page
		 */
		status = release_page_to_cache(currVol, currLbn, pCurrPage,
					       modified, FALSE);
		if (status != SUCCESS) {
		  return status;
		}
		modified = FALSE;
		
		/*
		 * at the end of primary, then follow chain.
		 */
		if (nextMCId.volume == 0) {
		    nextMCId = chainMCId;

		    ZERO_MCID(chainMCId);
		}
		
		currMCId = nextMCId;

	    } /* end curr mcell loop*/
	} /* end of deferred delete list loop */
    } /* end volume loop */
    
    if (ddlModified == TRUE) {
	return FIXED;
    }

    return SUCCESS;
} /* end clear_deferred_delete_list */

/*
 * Function Name: get_fset_attr
 *
 * Description: this function looks up a fileset tag file and retieves 
 *              the bsBfSetAttr record from the tag file.
 *
 *        NOTE: It would be nice if we could find the target filset by
 *              simply looking through the domain->fileset entries,
 *              but they aren't set up yet when we're called by
 *              clear_deferred_delete_list.
 *
 * Input parameters:
 *     domain: the domain in which the tag file lives
 *        tag: the tag of the target fileset tag directory 
 *  pFsetAttr: ptr to a bsBfSetAttr rec to be filled in by this function
 *
 * Output parameters:N/A
 *
 * Returns: SUCCESS, FAILURE
 * 
 */
static int
get_fset_attr(domainT      *domain,
	      bfTagT       setTag,
	      bsBfSetAttrT *pFsetAttr)
{
    bsBfSetAttrT pAttr;
    lbnNumT      currLbn;
    volNumT      currVol;
    pageT        *pCurrentPage;
    pageNumT     pageNum;
    tagNumT      tagNum;
    bsTDirPgT    *pTagPage;
    int          tagEntry;
    mcidT        tagMCId;
    mcidT        currMCId;
    bsMPgT       *bmtPage;
    volumeT      *volume;
    bsMCT        *pMcell;
    bsMRT        *pRecord;
    int          status;
    int          found = FALSE;

    /* get the MCID for the fileset tag file from the root tag file */

    pageNum = setTag.tag_num / BS_TD_TAGS_PG;

    status = convert_page_to_lbn(domain->rootTagLBN,
				 domain->rootTagLBNSize,
				 pageNum, &currLbn, &currVol);

    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_370, 
			 "Can not convert root tag page %ld to LBN.\n"),
		 pageNum);
	return status;
    }

    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
    if (status != SUCCESS) {
	return status;
    }

    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
    currMCId = pTagPage->tMapA[setTag.tag_num % BS_TD_TAGS_PG].tmBfMCId;

    status = release_page_to_cache(currVol, currLbn, pCurrentPage,
				   FALSE, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    volume = &(domain->volumes[currMCId.volume]);

    /*
     * Loop through the fileset tag file's BMT mcells looking for the
     * BSR_BFS_ATTR record.
     */

    while (currMCId.volume != 0) {

	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize,
				     currMCId.page, &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"),
		     pageNum);
	    return status;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);

	if (status != SUCCESS) {
	    return status;
	}
            
	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell = &(bmtPage->bsMCA[0]);
	pRecord = (bsMRT *)pMcell->bsMR0;

	/*
	 * Loop through this mcell's the records looking for
	 * type BSR_BFS_ATTR, then return a copy.
	 */

	while (found == FALSE  &&
	       pRecord->type != 0  &&
	       pRecord->bCnt != 0  &&
	       pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))) {

	    if (pRecord->type == BSR_BFS_ATTR) {

		*pFsetAttr = 
		    *((bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT)));

		found = TRUE;
	    }

	    pRecord = NEXT_MREC(pRecord);

	} /* end: loop through mcell records */

	status = release_page_to_cache(currVol, currLbn, pCurrentPage,
				       FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Set current to the next mcell.
	 */

	if (found) {
	    return (SUCCESS);
	}

	currMCId = pMcell->mcNextMCId;

    } /* loop through mcells */

    return (FAILURE);

} /* get_fset_attr */

/*
 * Function Name: correct_snap_files_on_ddl
 *
 * Description: The deferred delete list contained mcells for snap files,
 *              probably owned by a fileset that was being deleted at
 *              the time of a crash.  Normally the DDL is processed by
 *              activating the file system.  This is either when the
 *              FS is mounted, or by fcsk when it is run without "-o
 *              nolog".  So if there are DDL files it must be that
 *              activation has not run, so we have to deal with that
 *              now.
 *
 *              When we cleared the DDL we created a linked list of
 *              snap file tags attched to our "domain" struct.  We now
 *              need to go back and deal with those after we fix up
 *              the BMT and tag files.
 *
 * Input parameters:
 *     domain: The domain to finish clearing the deferred delete list.
 *
 * Output parameters:N/A
 *
 * Returns: SUCCESS, FAILURE
 * 
 */
int 
correct_snap_files_on_ddl(domainT *domain)
{
    char *funcName = "correct_snap_files_on_ddl";
    ddlSnapFileT  *nextSnapFile;
    ddlSnapFileT  *currSnapFile;
    pageT         *pCurrentPage;
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
    mcidT         currMCId;
    mcidT         nextMCId;
    lbnNumT       pageLBN;
    volNumT       pageVol;
    pageNumT      tagPageNum;
    tagNumT       tagEntryOnPage;
    bsTDirPgT     *pTagPage;

    int status;
    int found;
    int modified;

    currSnapFile = domain->ddlSnapList;

    /*
     * Walk the DDL list and process each file.
     */
    while (currSnapFile) {
	/*
	 * Find the fileset which this entry belongs to.
	 */
	pFileset = domain->filesets;
	found = FALSE;
	while (pFileset) {
	    if ((pFileset->filesetId.dirTag.tag_num == 
		 currSnapFile->setTag.tag_num) &&
		(pFileset->filesetId.dirTag.tag_seq == 
		 currSnapFile->setTag.tag_seq)) {
		found = TRUE;
		break;
	    }
	    pFileset = pFileset->next;
	} /* end while pFileset != NULL */
	
	if (found == FALSE) {
	    /*
	     * Fileset not found.  Assume that mcell was corrupt
	     */
	    nextSnapFile = currSnapFile->next;
	    free(currSnapFile);
	    currSnapFile = nextSnapFile;
	    continue;
	}

	/*
	 * We now have the right fileset.
	 */

	currMCId = pFileset->primMCId;
	volume = &(domain->volumes[currMCId.volume]);
	found = FALSE;

	/* Find the BSR_BFS_ATTR rec in the snapshot fileset and use
	   it to mark the fileset as OUT_OF_SYNC */

	while (!(FS_IS_SNAP_OUT_SYNC(pFileset->status)) && 
	       found == FALSE && 
	       currMCId.page) {

	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currMCId.page, &pageLBN, &pageVol);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = read_page_from_cache(pageVol, pageLBN, &pCurrentPage);

	    if (status != SUCCESS) {
		return status;
	    }
	    modified = FALSE;

	    bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	    pMcell = &(bmtPage->bsMCA[0]);
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    nextMCId = pMcell->mcNextMCId;
	    found = FALSE;

	    /*
	     * Loop through this mcell's records looking for type
	     * BSR_BFS_ATTR.
	     */
	    while ((found == FALSE) &&
		   (pRecord->type != BSR_NIL) &&
		   (pRecord->bCnt != 0) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

		if (BSR_BFS_ATTR == pRecord->type) {
		    pFsAttr = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));
		   /* Set the snapshot as out of sync. */
		    status = add_fixed_message(&(pCurrentPage->messages), BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_374, 
						       "Modified a snapshot to mark it out of sync.\n"));
		    if (status != SUCCESS) {
			return status;
		    }
		    FS_SET_SNAP_OUT_SYNC(pFileset->status);
		    pFsAttr->flags |= BFS_OD_OUT_OF_SYNC;
		    modified = TRUE;
		    found = TRUE;
		} /* end if record type is BSR_BFS_ATTR */
                    
		pRecord = NEXT_MREC(pRecord);
	    } /* end while looping through records */

	    status = release_page_to_cache(pageVol, pageLBN, pCurrentPage,
					   modified, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = FALSE;
	    volume = &(domain->volumes[nextMCId.volume]);
	    currMCId = nextMCId;

	} /* end while attrs not found and page is not 0 */

	/*
	 * Now we've marked the snapshot fileset as out of sync. Next
	 * we mark the DDL snap file's entry in the snap's tag dir
	 * to indicate that the file is out of sync.
	 */

	tagPageNum     = currSnapFile->fileTag.tag_num / BS_TD_TAGS_PG;
	tagEntryOnPage = currSnapFile->fileTag.tag_num % BS_TD_TAGS_PG;
 
	status = convert_page_to_lbn(pFileset->tagLBN,
				     pFileset->tagLBNSize,
				     tagPageNum,
				     &pageLBN, &pageVol);

	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_381, 
			     "Can't convert tag page %ld to LBN.\n"), 
		     tagPageNum);
	    return FAILURE;
	}

	/*
	 * Read the tag page
	 */
	status = read_page_from_cache(pageVol, pageLBN, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	}
	modified = FALSE;

	/*
	 * Now we have the correct tag page.  Find the correct tag and
	 * mark it out of sync, and release the page to the cache..
	 */

	pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
	pTagPage->tMapA[tagEntryOnPage].tmFlags |= BS_TD_OUT_OF_SYNC_SNAP;

	status = release_page_to_cache(pageVol, pageLBN, pCurrentPage,
				       TRUE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	nextSnapFile = currSnapFile->next;
	free(currSnapFile);
	currSnapFile = nextSnapFile;

    } /* end while: DDL processing loop */

    domain->ddlSnapList = NULL;

    return SUCCESS;

} /* end correct_snap_files_on_ddl */


/*
 * Function Name: correct_tag_file
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
int 
correct_tag_file(domainT *domain, 
		 tagNumT filesetNum)
{
    char *funcName = "correct_tag_file";
    tagPageStateT *pageArray;	/* Used to store status on each page */
    int	 fixedTagFile;
    int	 modified;      /* Flag if modified */
    int  firstModified; /* Flag if modified */
    int	 status;
    uint32_t  usedTags;
    int       foundCorruptedHead;
    char      fsName[BS_SET_NAME_SZ + 2];
    pageNumT  nextFreePage;	/* The number of the next free page  */
    pageNumT  numPages;
    pageNumT  currentPage;
    lbnNumT   firstLbn; 
    lbnNumT   currLbn;
    volNumT   firstVol; 
    volNumT   currVol;
    pageNumT  maxTagPage;
    pageNumT  page;
    pageNumT  tagUninitPg;
    pageNumT  numPageInExtent;
    lbnNumT   extentLbn;
    fobtoLBNT *pExtents;     /* The extents to check */
    xtntNumT  numExtents;
    xtntNumT  extent;
    pageT     *pFirstPage;   /* The first page we are checking */
    pageT     *pCurrentPage; /* The current page we are checking */
    filesetT  *fileset;
    bsTDirPgT *pTagPage;
    bsTDirPgT *pHeadTagPage;
    
    /*
     * Init variables.
     */
    modified	  = FALSE;
    firstModified = FALSE;
    fixedTagFile  = FALSE;
    nextFreePage  = 0;
    foundCorruptedHead = FALSE;

    if (filesetNum == -2) {
	/*
	 * Set pExtents to the root tag file extents.
	 */
	pExtents = domain->rootTagLBN;
	numExtents = domain->rootTagLBNSize;
	fileset = NULL;
	strcpy(fsName, "rootTag");
    } else {
	/*
	 * Find the fileset we are working on.
	 * Set pExtents to the fileset's tag file extents.
	 */
	fileset = domain->filesets;
	while (fileset != NULL) {
	    if (fileset->filesetId.dirTag.tag_num == filesetNum) {
		pExtents = fileset->tagLBN;
		numExtents = fileset->tagLBNSize;
		break;
	    } /* end if this is the fileset we want */
	    fileset = fileset->next;
	}
	assert(fileset != NULL);
	strcpy(fsName, fileset->fsName);
    }

    maxTagPage = FOB2PAGE(pExtents[numExtents].fob);
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
    if (status != SUCCESS) {
	return status;
    }

    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
    tagUninitPg = pTagPage->tMapA[0].tmUninitPg;

    if ((tagUninitPg > maxTagPage) ||
	(tagUninitPg < (maxTagPage - 7))){
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_376,
			 "Modifying '%s' file system tag file tagUninitPg entry.\n"), fsName);
	status = add_fixed_message(&(pCurrentPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_377, 
					   "Modified '%s' file system tagUninitPg from %d to %d.\n"),
				   fsName, tagUninitPg, maxTagPage);
	if (status != SUCCESS) {
	    return status;
	}
	pTagPage->tMapA[0].tmUninitPg = maxTagPage;
	tagUninitPg = maxTagPage;
	modified = TRUE;
    }
    
    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				   modified, FALSE);
    if (status != SUCCESS) {
	return status;
    }
    
    if (modified == TRUE) {
	fixedTagFile = TRUE;
	modified = FALSE;
    }

    numPages = FOB2PAGE(pExtents[numExtents].fob);
    
    pageArray = (int *)ffd_calloc(numPages, sizeof(*pageArray));
    if (pageArray == NULL) {
	writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FSCK_1, FSCK_10,
			  "Can't allocate memory for %s.\n"),
		  catgets(_m_catd, S_FSCK_1, FSCK_378, 
			  "tag page array"));
	return NO_MEMORY;
    }

    bzero(pageArray, sizeof(*pageArray) * numPages);

    /*
     * First fix the header on all the tag file pages.
     */
    currentPage = 0;
    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute number of pages in this extent.
	 */
	numPageInExtent = FOB2PAGE(pExtents[extent + 1].fob - 
				   pExtents[extent].fob);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * loop thru the initialized tag file pages in this extent
	 */
	for (page = 0 ; page < numPageInExtent; page++, currentPage++) {

	    if (currentPage >= tagUninitPg) {
		/*
		 * Uninitialized page - ignore rest of pages.
		 */
		break;
	    }

	    currLbn = extentLbn + (page * ADVFS_METADATA_PGSZ_IN_BLKS);

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (status != SUCCESS) {
		return status;
	    } 

	    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;

	    /*
	     * Fix headers first, If we fix the records call it
	     * a second time.
	     */
	    status = correct_tag_page_header(domain, fileset, pCurrentPage, 
					     currentPage, tagUninitPg);
	    if (status != SUCCESS) {
		if (status == FIXED) {
		    modified = TRUE;
		} else {
		    return status;
		}
	    }

	    status = correct_tag_page_records(pCurrentPage, domain, fileset, 
					      currentPage, tagUninitPg);
	    if (status != SUCCESS) {
		if (status == FIXED) {
		    /*
		     * Recall correct_tag_page_header.
		     */
		    status = correct_tag_page_header(domain, 
						     fileset, 
						     pCurrentPage, 
						     currentPage,
						     tagUninitPg);
		    if (status != SUCCESS) {
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

	    usedTags = (pTagPage->tpNumAllocTMaps);

	    if (currentPage == 0) {
		/*
		 * tag 0 on page 0 special case.
		 */
		usedTags += 2;
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
	    if (status != SUCCESS) {
		return status;
	    }
	    if (modified == TRUE) {
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
    if (status != SUCCESS) {
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
		 catgets(_m_catd, S_FSCK_1, FSCK_379,
			 "Clearing '%s' file system free tag page list head, out of bounds.\n"),
		 fsName);
	status = add_fixed_message(&(pFirstPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_380, 
					   "Modified '%s' file system free tag page list, tmFreeListHead out of bounds.\n"),
				   fsName);	
	if (status != SUCCESS) {
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
    while (nextFreePage != -1) {
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
	    if (status != SUCCESS) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_381,
				 "Can't convert tag page %ld to LBN.\n"), 
			 nextFreePage);
		return FAILURE;
	    }

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (status != SUCCESS) {
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
	if (pageArray[currentPage] == TAGPAGE_FULL) {
	    /*	
	     * We have a Full page on the free list, set the head of 	
	     * the list to 0, so we will need to redo the entire list.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_389,
			     "Clearing '%s' file system free tag page list head,\n"),
		     fsName);
	    writemsg(SV_VERBOSE | SV_LOG_FOUND | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_390,
			     "tag page %ld is full page but on list.\n"),
		     currentPage );
	    status = add_fixed_message(&(pFirstPage->messages), tagFile,
				       catgets(_m_catd, S_FSCK_1, FSCK_383, 
					       "Modified '%s' FS free tag page list, found a full page on free list.\n"),
				       fsName);
	    if (status != SUCCESS) {
		return status;
	    }

	    pHeadTagPage->tMapA[0].tmFreeListHead = 0;
	    firstModified = TRUE;
	    pTagPage->tpNextFreePage = 0;
	    modified = TRUE;
	    foundCorruptedHead = TRUE;
	} else if (pageArray[currentPage] == TAGPAGE_USED) {
	    /*
	     * We have a loop in the chain, terminate loop.
	     */
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_384, 
			     "Clearing '%s' file system free tag page list, loop in list.\n"),
		     fsName);
	    status = add_fixed_message(&(pCurrentPage->messages), tagFile,
				       catgets(_m_catd, S_FSCK_1, FSCK_385, 
					       "Modified '%s' file system free tag page list, found loop in the free list.\n"),
				       fsName);
	    if (status != SUCCESS) {
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
	if (pTagPage->tpNextFreePage == 0) {
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
	    if (status != SUCCESS) {
		return status;
	    }
	    if (modified == TRUE) {
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
    if (foundCorruptedHead == TRUE) {
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
	pageNumT numPageInExtent;
	lbnNumT  extentLbn;

	/*
	 * Compute number of pages in this extent.
	 */
	numPageInExtent = FOB2PAGE(pExtents[extent+1].fob - 
				   pExtents[extent].fob);
	extentLbn = pExtents[extent].lbn;
	currVol = pExtents[extent].volume;
	
	/*
	 * for each extent loop through all tag file pages.
	 */
	for (page = 0 ; page < numPageInExtent; page++, currentPage++) {

	    if (pageArray[currentPage] != TAGPAGE_FREE) {
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

	    if (currentPage == 0) {
		/*
		 * On the first page don't read it twice.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_386, 
				 "Adding '%s' file system tag page %ld to free list.\n"),
			 fsName, currentPage);

		status = add_fixed_message(&(pFirstPage->messages),
					   tagFile,
					   catgets(_m_catd, S_FSCK_1, FSCK_387, 
						   "Modified '%s' file system tag page, added page to free list.\n"),
					   fsName);
		if (status != SUCCESS) {
		    return status;
		}

		pHeadTagPage->tpNextFreePage = pHeadTagPage->tMapA[0].tmFreeListHead;
		pHeadTagPage->tMapA[0].tmFreeListHead = pHeadTagPage->tpCurrPage + 1; /* WARNING: Off by 1 */
		firstModified = TRUE;
	    } else {
		/*
		 * Compute the LBN of the page we wish to read.
		 */
		currLbn = extentLbn + (page * ADVFS_METADATA_PGSZ_IN_BLKS);

		status = read_page_from_cache(currVol, currLbn, 
					      &pCurrentPage);
		if (status != SUCCESS) {
		    return status;
		} 
		pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
		    
		/*
		 * Add this page to the HEAD of the free list.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_386, 
				 "Adding '%s' file system tag page %ld to free list.\n"),
			 fsName, currentPage);
		status = add_fixed_message(&(pCurrentPage->messages), tagFile,
					   catgets(_m_catd, S_FSCK_1, FSCK_387,
						   "Modified '%s' file system tag page, added page to free list.\n"),
					   fsName);
		if (status != SUCCESS) {
		    return status;
		}

		pTagPage->tpNextFreePage = pHeadTagPage->tMapA[0].tmFreeListHead;
		modified = TRUE;
		pHeadTagPage->tMapA[0].tmFreeListHead = pTagPage->tpCurrPage + 1; /* WARNING: Off by 1 */
		firstModified = TRUE;

		status = release_page_to_cache(currVol, currLbn, 
					       pCurrentPage, modified,
					       FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		if (modified == TRUE) {
		    fixedTagFile = TRUE;
		    modified = FALSE;
		}
	    } /* end if first page */
	} /* end for page loop */
    } /* end for extent loop */

    /*
     * Released the First page.
     */
    if (pFirstPage != NULL) {
	status = release_page_to_cache(firstVol, firstLbn, pFirstPage, 
				       firstModified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	if (firstModified == TRUE) {
	    fixedTagFile = TRUE;
	    firstModified = FALSE;
	}
    }

    free(pageArray);

    if (fixedTagFile == TRUE) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_tag_file */


/*
 * Function Name: correct_tag_page_header
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

static int 
correct_tag_page_header(domainT *domain,
			filesetT *fileset, 
			pageT *pPage, 
			pageNumT pageNumber, 
			pageNumT tagUninitPg)
{
    char      *funcName = "correct_tag_page_header";
    int       status;
    int	      modified;   /* Flag to check if page has been modified */
    uint32_t  counter;
    uint32_t  magic;
    char      fsName[BS_SET_NAME_SZ + 2];
    bsTDirPgT *pTagPage;
    fobtoLBNT *pExtents;     /* The extents to check */
    xtntNumT  numExtents;
    pageNumT  maxTagPage;
    tagNumT   numAlloced; /* Number of tags allocated */
    tagNumT   numFree;    /* Number of tags which are free */

    /*
     * Init variables.
     */
    modified = FALSE;
    numAlloced	 = 0;
    numFree	 = 0;
    pTagPage = (bsTDirPgT *)pPage->pageBuf;

    if (pageNumber >= tagUninitPg) {
	/*
	 * Uninitialized page - ignore it.
	 */
	return SUCCESS;
    }

    if (fileset != NULL) {
	strcpy(fsName, fileset->fsName);
	pExtents = fileset->tagLBN;
	numExtents = fileset->tagLBNSize;
	maxTagPage = FOB2PAGE(pExtents[numExtents].fob);
	magic = TAG_MAGIC;
    } else { 
	strcpy(fsName, "root");
	magic = RTTAG_MAGIC;
    }

    /* correct markers */

    if (pTagPage->tpPgHdr.magicNumber != magic) {
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_934, 
					   "Modified '%s' file system tag page magic number from 0x%x to 0x%x.\n"),
				   fsName, 
				   pTagPage->tpPgHdr.magicNumber, magic);
	if (status != SUCCESS) {
	    return status;
	}

	pTagPage->tpPgHdr.magicNumber = magic;
	modified = TRUE;
    }

    if (!COOKIE_EQ(pTagPage->tpPgHdr.fsCookie, domain->fsCookie)) {
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_935, 
					   "Modified '%s' file system tag page fsCookie.\n"),
				   fsName);
	if (status != SUCCESS) {
	    return status;
	}

	pTagPage->tpPgHdr.fsCookie = domain->fsCookie;
	modified = TRUE;
    }

    /* check page number */

    if (pageNumber != pTagPage->tpCurrPage) {
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_388, 
					   "Modified '%s' file system tag page number from %ld to %ld.\n"),
				   fsName, pTagPage->tpCurrPage, 
				   pageNumber);
	if (status != SUCCESS) {
	    return status;
	}

	pTagPage->tpCurrPage = pageNumber;
	modified = TRUE;
    }

    for (counter = 0 ; counter < BS_TD_TAGS_PG; counter++) {
   
	if ((pTagPage->tMapA[counter].tmFlags & BS_TD_IN_USE) != 0) {
	    numAlloced++;
	} else {
	    numFree++;
	}
    } /* end for counter loop */

    if (pageNumber == 0) {
	/*
	 * Special Case - This tag is used for free list head
	 */
	numFree--;
    }
    
    if (pTagPage->tpNumAllocTMaps != numAlloced) {
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_382, 
					   "Modified '%s' file system tag page header numAllocTmaps from %d to %d.\n"),
				   fsName, pTagPage->tpNumAllocTMaps,
				   numAlloced);
	if (status != SUCCESS) {
	    return status;
	}

	pTagPage->tpNumAllocTMaps = numAlloced;
	modified = TRUE;
    } /* end if numAlloced */

    /*
     * Future: figure a way to compute maxTagPage for rootTagFile
     */
    if (fileset != NULL) {
	if ((pTagPage->tpNextFreePage < 0) ||
	    (pTagPage->tpNextFreePage > maxTagPage)) {

	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FSCK_1, FSCK_391, 
					       "Modified '%s' file system tag page header nextFreePage from %d to %d.\n"),
				       fsName, pTagPage->tpNextFreePage, 0);
	    if (status != SUCCESS) {
		return status;
	    }

	    pTagPage->tpNextFreePage = 0;
	    modified = TRUE;
	}
    }

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_392, 
			 "Modifying '%s' file system tag page %ld's header.\n"),
		 fsName, pageNumber);
	return FIXED;
    }

    return SUCCESS;
} /* end correct_tag_page_header */

/*
 * Function Name: correct_tag_page_records
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
int 
correct_tag_page_records(pageT *pPage, 
			 domainT *domain, 
			 filesetT *fileset, 
			 pageNumT pageNumber, 
			 pageNumT tagUninitPg)
{
    char       *funcName = "correct_tag_page_records";
    int	       modified;  /* Flag to check if page has been modified. */
    int	       status;
    uint32_t   tagEntry;
    tagNumT    tagNum;
    tagSeqT    seqNum;
    int	       tagArray[BS_TD_TAGS_PG]; /* Used to find loops. */
    int	       currentTMap;     /* Current tag we are working on*/
    int	       prevTMap;	     /* Previous tag we worked on. */
    int	       firstFreeTMap;   /* Head of the free tag list. */
    int	       isRootTag;
    pageNumT   page;
    nodeT      *node;
    bsTDirPgT  *pTagPage;
    mcellNodeT tmpMcellNode;
    mcellNodeT *pMcellNode;
    char       fsName[BS_SET_NAME_SZ + 2];
    filesetT   *fsPtr;

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

    if (fileset != NULL) {
	strcpy(fsName, fileset->fsName);
	isRootTag = FALSE;
    } else { 
	strcpy(fsName, "rootTag");
	isRootTag = TRUE;
    }

    if ((page == 0) && (pTagPage->tMapA[0].tmFlags & BS_TD_IN_USE)) {
	/*
	 * Tag 0 is marked in use.  This is never valid, so we probably
	 * have an uninitialized page 0 of the tagfile.  Clear the inuse
	 * bit so that the next check doesn't think tag 0 is in use.
	 */
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_912,
					   "Clearing '%s' file system free tag page list head, the IN_USE bit must never be set.\n"),
				   fsName);
	if (status != SUCCESS) {
	    return status;
	}
	pTagPage->tMapA[0].tmFlags &= ~BS_TD_IN_USE;
	modified = TRUE;
    }	    

    /* 
     * loop thru all the tag entries in the current page marking each
     * entry as either TAG_FREE or TAG_USED in a corresponding array.
     * We'll then use the array to detect invalid entries or loops in
     * the tag chain.
     */

    for (tagEntry = 0 ; tagEntry < BS_TD_TAGS_PG; tagEntry++) {
	tagNum = (page * BS_TD_TAGS_PG) + tagEntry;

	if (pTagPage->tMapA[tagEntry].tmFlags & BS_TD_IN_USE) {
	    /*
	     * Find this mcell in the skiplist.
	     */
	    tmpMcellNode.mcid = pTagPage->tMapA[tagEntry].tmBfMCId;
	    pMcellNode = &tmpMcellNode;

	    status = find_node(domain->mcellList, (void **)&pMcellNode, &node);
	    if (status == NOT_FOUND) {
		/*
		 * Active Tag has invalid primary mcell, Add tag
		 * to free list.
		 */
		status = add_fixed_message(&(pPage->messages), tagFile,
					   catgets(_m_catd, S_FSCK_1, FSCK_393, 
						   "Deleting %s file system tag %ld, primary mcell (%d,%ld,%d) not found.\n"),
					   fsName, tagNum, 
					   tmpMcellNode.mcid.volume, 
					   tmpMcellNode.mcid.page, 
					   tmpMcellNode.mcid.cell);
		if (status != SUCCESS) {
		    return status;
		}

		pTagPage->tMapA[tagEntry].tmFlags &= ~BS_TD_IN_USE;
		pTagPage->tMapA[tagEntry].tmBfMCId.volume = 0;
		pTagPage->tMapA[tagEntry].tmNextMap = pTagPage->tpNextFreeMap;
		pTagPage->tpNextFreeMap = tagEntry + 1; /* WARNING: Off by 1 */
		pTagPage->tpNumAllocTMaps--; /* Decrement allocated maps */
		modified = TRUE;
		tagArray[tagEntry] = TAG_FREE;
	    } else  if (status == SUCCESS) {
		tagArray[tagEntry] = TAG_USED;
	    } else {
		return status;
	    }
	} else {
	    /*
	     * Tag is free
	     */
	    tagArray[tagEntry] = TAG_FREE;
	}
    } /* end for tagEntry loop */

    /*
     * Special Case for tag '0' on page 0.
     */
    if (page == 0) {
	tagArray[0] = TAG_USED;
    }

    /* 
     * Special case for page == 0, tag1 of a fileset tag file.  Tag 1
     * of the fileset tag directory under Tru64 was used for the frag
     * file, with tag 2 being for the root directory file.  Frag files
     * were eliminated for the HP/UX port, but certain kernel code
     * breaks if the root directory tag is changed to tag1, so it was
     * left as Tag 2 and Tag 1 is left free.  BUT,
     * pTagPage->tpNextFreeMap does not show Tag 1 as the first free
     * map entry (!) so Tag 1 is never actually used.  We need to list
     * it as TAG_USED here so that the loop detection code below
     * doesn't complain.
     */

    if (page == 0 && !isRootTag) {
	tagArray[1] = TAG_USED;
    }

    firstFreeTMap = pTagPage->tpNextFreeMap;
    currentTMap	  = firstFreeTMap;

    /*
     * Range check the header's first free map.  Need to check GREATER
     * THAN BS_TD_TAGS_PG because free entry numbers are 'off by one'.
     */
    if ((currentTMap > BS_TD_TAGS_PG) || (currentTMap < 0)){
	/*
	 * Invalid value so truncate the chain.
	 */
	status = add_fixed_message(&(pPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_394, 
					   "Modified '%s' file system tpNextFreeMap from %d to %d.\n"),
				   fsName, currentTMap, 0);
	if (status != SUCCESS) {
	    return status;
	}

	pTagPage->tpNextFreeMap = 0;
	modified = TRUE;
	currentTMap = 0;
    }
    currentTMap--; /* off by one: convert free map entry number to tag
		      array index */
    prevTMap = currentTMap;

    /*
     * Loop on the free chain looking for invalid entries or loops.
     * End of loop means tmNextMap == 0.  
     *
     * NOTE: tpNextFreeMap is indexed 1 to BS_TD_TAGS_PG, while array
     * is 0 to BS_TD_TAGS_PG-1.
     */
    while (currentTMap != -1) {

	if ((currentTMap > BS_TD_TAGS_PG-1) || (currentTMap < 0)){
	    /*
	     *	Invalid value so truncate the chain.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FSCK_1, FSCK_395, 
					       "Modified '%s' FS tMap %d on page %ld, truncating the next tMap chain.\n"), 
				       fsName, currentTMap, page);
	    if (status != SUCCESS) {
		return status;
	    }

	    pTagPage->tMapA[prevTMap].tmNextMap = 0;
	    modified = TRUE;
	    currentTMap = -1;
	} else if (tagArray[currentTMap] == TAG_FREE) {
	    /*
	     * We have a free tag (Valid case), mark on free list.
	     */
	    tagArray[currentTMap] = TAG_FREE_LIST;
	    prevTMap = currentTMap;
	    currentTMap = pTagPage->tMapA[currentTMap].tmNextMap;
	    currentTMap--;
	} else {
	    /*
	     * We either have a loop or an active tag on the free
	     * list.  Truncate the chain.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FSCK_1, FSCK_395, 
					       "Modified '%s' FS tMap %d on page %ld, truncating the next tMap chain.\n"), 
				       fsName, currentTMap, page);
	    if (status != SUCCESS) {
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
     * At this point, all the tag entries in the free chain are marked
     * as LIST_TAG_FREE in the tag array.  Any remaining tags marked
     * TAG_FREE are free yet are not on the free chain.  We place them
     * there now.
     */
    for (tagEntry = 0 ; tagEntry < BS_TD_TAGS_PG; tagEntry++) {

	if (tagArray[tagEntry] == TAG_FREE) {
	    /*
	     * Free tag not on list, add to head.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FSCK_1, FSCK_396, 
					       "Modified '%s' file system tMap %d on page %ld, added to free list.\n"),
				       fsName, tagEntry, page);
	    if (status != SUCCESS) {
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

	tagNum = (page * BS_TD_TAGS_PG) + tagEntry;
	if (!(pTagPage->tMapA[tagEntry].tmFlags & BS_TD_IN_USE)) {
	    continue;
	}
	/*
	 * Find this mcell in the skiplist.
	 */
	tmpMcellNode.mcid = pTagPage->tMapA[tagEntry].tmBfMCId;
	pMcellNode = &tmpMcellNode;

	status = find_node(domain->mcellList, (void **)&pMcellNode, 
			   &node);
	if (status != SUCCESS) {
	    /*
	     * This should never fail, as we already know it exists.
	     */
	    return status;
	}

	seqNum = pTagPage->tMapA[tagEntry].tmSeqNo;

	if (MC_IS_FOUND_PTR(pMcellNode->status) &&
	    !FS_IS_SNAP(fileset->status)) {

	    /*
	     * We have two tags pointing to same primary mcell, 
	     * delete the second one by adding to free list.
	     */
	    status = add_fixed_message(&(pPage->messages), tagFile,
				       catgets(_m_catd, S_FSCK_1, FSCK_397, 
					       "Deleted '%s' FS tag %d, tag's primary mcell (%d,%ld,%d) already in use.\n"),
				       fsName, tagNum,
				       pMcellNode->mcid.volume,
				       pMcellNode->mcid.page, 
				       pMcellNode->mcid.cell);
	    if (status != SUCCESS) {
		return status;
	    }

	    pTagPage->tMapA[tagEntry].tmFlags &= ~BS_TD_IN_USE;
	    pTagPage->tMapA[tagEntry].tmBfMCId.volume = 0;
	    pTagPage->tMapA[tagEntry].tmNextMap = pTagPage->tpNextFreeMap;
	    pTagPage->tpNextFreeMap = tagEntry + 1; /* WARNING: Off by 1 */
	    pTagPage->tpNumAllocTMaps--; /* Decrement allocated maps */
	    modified = TRUE;
	} else {
	    tagNodeT *pTagNode;

	    /*
	     * We found an mcell for which we have not yet created an
	     * entry in the tagnode skip list.  We have seen
	     * corruptions where more than one tag points to the same
	     * primary mcell. We check to see if the pointed-to mcell
	     * is pointing back at this tag (tag_num and tag_seq).
	     */
	    if ((pMcellNode->tagSeq.tag_num != tagNum) ||
		(pMcellNode->tagSeq.tag_seq != seqNum)) {
		/*
		 * The primary mcell that this tag is pointing to has
		 * a tag that is not for the tag we're now looking at.
		 * Oops.  We assume that the tag the mcell has is
		 * valid and so clear this tag and place it on the
		 * free list.
		 */
		status = add_fixed_message(&(pPage->messages), tagFile,
					   catgets(_m_catd, S_FSCK_1, FSCK_398, 
						   "Deleted '%s' FS tag %ld.%u, mismatch with the primary mcell (%d,%ld,%d)'s tag %ld.%u.\n"),
					   fsName, tagNum, seqNum,
					   pMcellNode->mcid.volume,
					   pMcellNode->mcid.page,
					   pMcellNode->mcid.cell,
					   pMcellNode->tagSeq.tag_num,
					   pMcellNode->tagSeq.tag_seq);
		if (status != SUCCESS) {
		    return status;
		}

		pTagPage->tMapA[tagEntry].tmFlags &= ~BS_TD_IN_USE;
		pTagPage->tMapA[tagEntry].tmBfMCId.volume = 0;
		pTagPage->tMapA[tagEntry].tmNextMap = pTagPage->tpNextFreeMap;
		pTagPage->tpNextFreeMap = tagEntry + 1; /* WARNING: Off by 1 */
		pTagPage->tpNumAllocTMaps--; /* Decrement allocated maps */
		modified = TRUE;
	    } else if (isRootTag == TRUE) {
		/*
		 * Root fileset
		 */
		MC_SET_FOUND_PTR(pMcellNode->status);
	    } else {
		/*
		 * We create new tag node entries in the tag skiplist
		 * for tags in non-root filesets.
		 */
		pTagNode = (tagNodeT *)ffd_malloc(sizeof(tagNodeT));
		if (NULL == pTagNode) {
		    writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FSCK_1, FSCK_10, 
					 "Can't allocate memory for %s.\n"),
				 catgets(_m_catd, S_FSCK_1, FSCK_403,
					 "tag node"));
		    return NO_MEMORY;
		}

		pTagNode->tagSeq.tag_num = tagNum;
		pTagNode->tagSeq.tag_seq = pTagPage->tMapA[tagEntry].tmSeqNo;
		pTagNode->tagMCId = pTagPage->tMapA[tagEntry].tmBfMCId;
		pTagNode->status = 0;

		/*
		 * Will be loaded later with valid values.
		 */
		pTagNode->fileType = 0;
		pTagNode->size = 0;
		pTagNode->fobsFound = 0;
		pTagNode->numLinks = 0;
		pTagNode->numLinksFound = 0;
		pTagNode->parentTagNum.tag_num = 0;
		pTagNode->parentTagNum.tag_seq = 0;
		pTagNode->dirIndexTagNum.tag_num = 0;
		pTagNode->dirIndexTagNum.tag_seq = 0;

		status = insert_node(fileset->tagList, pTagNode);
		if (status != SUCCESS) {
		    return status;
		}

		MC_SET_FOUND_PTR(pMcellNode->status);

	    } /* end if tag/seq off */
	} /* end if not found */
    } /* end looping through tags on page */

    if (isRootTag == TRUE) {
	/*
	 * rootTag file.
	 */
	status = save_fileset_info(domain, pPage);
	if (status != SUCCESS) {
	    return status;
	}

	/* We now have a list of fileset structs hanging off the
	   domain struct, one for each fileset and snap. We walk down
	   that list and check/correct the snapshot linkages to
	   child/sibling/parent filesets and also the snap levels. */

	for (fsPtr = domain->filesets; fsPtr; fsPtr = fsPtr->next) {
	    correct_snap_linkage(domain->filesets, fsPtr);
	}

	for (fsPtr = domain->filesets; fsPtr; fsPtr = fsPtr->next) {
	    correct_snap_level(domain->filesets, fsPtr);
	}

	/* Now we walk down the fileset list again to update the on-disk
	   bsFsSetAttrT records corresponding filesets whose snap links
	   have been corrected, if any. */

	for (fsPtr = domain->filesets; fsPtr; fsPtr = fsPtr->next) {
	    if (fsPtr->linkFixed) {
		update_snap_fsattr(fsPtr);
	    }
	}

    } /* end: root tag file processing */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_400,
			 "Modifying '%s' file system tag page %ld's records.\n"),
		 fsName, pageNumber);
	return FIXED;
    }
    return SUCCESS;
} /* correct_tag_page_records */


/*
 * Function Name: correct_snap_linkage
 *
 * Description: This function checks fileset linkages to parent, 
 *              child, and sibling.  If no existing fileset struct can
 *              be matched to a linkage, then the linkage is zeroed.
 *
 * Input parameters:
 *     head: the head of a forward linked list of all fileset structs 
 *           in the domain being checked.
 *    pNode: The fileset from the list currently being checked.
 *
 * Output parameters: N/A
 *
 */
static int
correct_snap_linkage(filesetT *head,
		     filesetT *pNode)
{
    filesetT *pFS;
    filesetT *fset;
    uint16_t levels;
    
    /* if we have a parent link check/correct it. */

    if (pNode->parentSetTag.tag_num) {

	/* find the parent fileset */

	for (pFS = head; pFS; pFS = pFS->next) {
	    if (BS_BFTAG_EQL(pNode->parentSetTag, pFS->filesetId.dirTag))
		break;
	}

	if (!pFS) {

	    /* if we couldn't find our parent, look for a fileset
	       whose child link points to us... */

	    for (pFS = head; pFS; pFS = pFS->next) {
		if (BS_BFTAG_EQL(pFS->childSetTag, pNode->filesetId.dirTag))
		    break;
	    }

	    if (pFS) {
		/* we found our parent, fix the linkage */
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_793,
				 "FS '%s' parent tag link corrupt, changing to reference '%s'.\n"),
			 pNode->fsName, pFS->fsName);

		pNode->parentSetTag = pFS->childSetTag;
	    } else {
		/* No parent found so assume our parent link is bogus
		   and zero it */

		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_794,
				 "FS '%s' parent tag link corrupt. Zeroing tag %ld.\n"),
			 pNode->fsName, 
			 pNode->parentSetTag.tag_num);
		pNode->parentSetTag.tag_num = NULL;
		pNode->parentSetTag.tag_seq = 0;
	    }

	    pNode->linkFixed = TRUE;
	}
    } /* end: parent tag correction */

    /* if we have a child link check/correct it. */

    if (pNode->childSetTag.tag_num) {

	/* find our child link's fileset struct... */

	for (pFS = head; pFS; pFS = pFS->next) {
	    if (BS_BFTAG_EQL(pFS->filesetId.dirTag, pNode->childSetTag))
		break;
	}

	if (!pFS) {

	    /* We couldn't find our child, so look for a fileset whose
	       parent link points to us... */

	    for (pFS = head; pFS; pFS = pFS->next) {
		if (BS_BFTAG_EQL(pFS->parentSetTag, pNode->filesetId.dirTag))
		    break;
	    }

	    if (pFS) {
		/* we found our child, fix the linkage */
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_784,
				 "FS '%s' child tag link corrupt, changing to reference '%s'.\n"),
			 pNode->fsName, pFS->fsName);

		pNode->childSetTag = pFS->parentSetTag;
	    } else {
		/* assume our child link is bogus and zero it */
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_785,
				 "FS '%s' child tag link corrupt. Zeroing tag %ld.\n"),
			 pNode->fsName, pNode->childSetTag.tag_num);
		pNode->childSetTag.tag_num = NULL;
		pNode->childSetTag.tag_seq = 0;
	    }
	    pNode->linkFixed = TRUE;
	}
    } /* end: child tag correction */

    /* if we have a sibling link, check/correct it. */

    if (pNode->sibSetTag.tag_num) {

	/* find our sibling's fileset struct */

	for (pFS = head; pFS; pFS = pFS->next) {
	    if (BS_BFTAG_EQL(pFS->filesetId.dirTag, pNode->sibSetTag))
		break;
	}

	if (!pFS) {

	    /* if we couldn't find our sibling, we have to assume that
	       our sibling link is corrupt and simply zero it.  If the
	       sibling pointers were bidirectional we could try to
	       find a peer fileset that's pointing back to us, but
	       there are no back pointers. */

	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_781,
			     "FS '%s' sibling tag link corrupt. Zeroing tag %ld.\n"),
		     pNode->fsName, pNode->childSetTag.tag_num);
	    pNode->sibSetTag.tag_num = NULL;
	    pNode->sibSetTag.tag_seq = 0;
	    pNode->linkFixed = TRUE;
	}
    } /* end: sibling tag correction */

    return SUCCESS;

} /* correct_snap_linkage */

/*
 * Function Name: correct_snap_level
 *
 * Description: This function checks/corrects the fileset snapLevel
 *              value by counting how many parent links are between
 *              this node and the root fileset.
 *
 * NOTE: this function is called after correct_snap_linkage since in
 * order to get an accurate level couunt the linkages must already
 * have been made consistent.
 *
 * Input parameters:
 *     head: the head of a forward linked list of all fileset structs 
 *           in the domain being checked.
 *    pNode: The fileset from the list currently being checked.
 *
 * Output parameters: N/A
 *
 */
static int
correct_snap_level(filesetT *head,
		   filesetT *pNode)
{
    filesetT *pFS;
    filesetT *pCurrSet = pNode;
    int level;

    /* verify that the snap level count value is correct */

    for (level = 0; pCurrSet->parentSetTag.tag_num; level++) {

	/* find the fileset for the current parent tag. */

	for (pFS = head; pFS; pFS = pFS->next) {
	    if (BS_BFTAG_EQL(pCurrSet->parentSetTag, pFS->filesetId.dirTag)) {
		pCurrSet = pFS;
		break;
	    }
	}
    }

    if (pNode->snapLevel != level) {

	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_777,
			 "FS '%s' snapLevel incorrect. Changing from %d to %d.\n"),
		 pNode->fsName, pNode->snapLevel, level);

	pNode->snapLevel = level;
	pNode->linkFixed = TRUE;
    }

    return SUCCESS;

} /* correct_snap_level */

/*
 * Function Name: update_snap_fsattr
 *
 * Description: This function changes a fileset's bsBfSetAttrT 
 *              record's snap elements on-disk.
 *
 * Input parameters:
 *    pFileset - fileset whose snap values need to be changed on disk
 *
 * Output parameters: N/A
 *
 */
static int
update_snap_fsattr(filesetT *pFileset)
{
    mcidT         currMCId;
    mcidT         nextMCId;
    volumeT       *volume;
    lbnNumT       pageLBN;
    volNumT       pageVol;
    pageT         *pBmtPage;
    bsMPgT        *bmtPage;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsBfSetAttrT  *pFsAttr;

    int status;
    int found;

    if (!pFileset->linkFixed)
	return (SUCCESS);

    currMCId = pFileset->primMCId;
    volume = &(pFileset->domain->volumes[currMCId.volume]);
    found = FALSE;

    while (!found && currMCId.volume) {

	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize,
				     currMCId.page, &pageLBN, &pageVol);
	if (status != SUCCESS) {
	    return status;
	}

	status = read_page_from_cache(pageVol, pageLBN, &pBmtPage);

	if (status != SUCCESS) {
	    return status;
	}

	bmtPage = (bsMPgT *)pBmtPage->pageBuf;
	pMcell = &(bmtPage->bsMCA[0]);
	pRecord = (bsMRT *)pMcell->bsMR0;
	nextMCId = pMcell->mcNextMCId;
	found = FALSE;

	/*
	 * Loop through this mcell's records looking for type
	 * BSR_BFS_ATTR.
	 */
	while ((found == FALSE) &&
	       (pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (BSR_BFS_ATTR == pRecord->type) {
		pFsAttr = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));
	    
		/* update the snap linkage. */
		status = 
		    add_fixed_message(&(pBmtPage->messages), BMT,
				      catgets(_m_catd, S_FSCK_1, FSCK_778, 
					      "Corrected snap linkage in BSR_BFS_ATTR for '%s' file system.\n"),
				      pFileset->fsName);
		if (status != SUCCESS) {
		    return status;
		}

		pFsAttr->bfsaSnapLevel = pFileset->snapLevel;
		pFsAttr->bfsaParentSnapSet.dirTag = pFileset->parentSetTag;
		pFsAttr->bfsaFirstChildSnapSet.dirTag = pFileset->childSetTag;
		pFsAttr->bfsaNextSiblingSnap.dirTag = pFileset->sibSetTag;
		found = TRUE;
	    } /* end if record type is BSR_BFS_ATTR */
                    
	    pRecord = NEXT_MREC(pRecord);
	} /* end while looping through records */

	status = release_page_to_cache(pageVol, pageLBN, pBmtPage,
				       TRUE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	volume = &(pFileset->domain->volumes[nextMCId.volume]);
	currMCId = nextMCId;

    } /* end while attrs not found */

    return (SUCCESS);

} /* update_snap_fsattr */


/*
 * Function Name: correct_tag_to_mcell
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
int 
correct_tag_to_mcell(filesetT *fileset, 
		     tagNumT  tagNum, 
		     uint32_t seqNum,
		     mcidT    primMCId)
{
    char     *funcName = "correct_tag_to_mcell";
    int      status;
    int      modified;        /* Flag to check if page has been modified. */
    uint32_t currentTMap;     /* Current tag we are working on*/
    uint32_t prevTMap;        /* Previous tag we worked on. */
    uint32_t firstFreeTMap;   /* Head of the free tag list. */
    uint32_t tagsUsed;
    lbnNumT  currLbn;
    volNumT  currVol;
    pageNumT tagPageNum;
    pageNumT freePage;        /* Used to store the nextFreePage */
    fobtoLBNT *pExtents;      /* The extents to check */
    xtntNumT numExtents;
    pageT      *pCurrentPage; /* The current page we are checking */
    bsTDirPgT  *pTagPage;     /* The current tag page we are working on */
    tagNumT    tagEntryOnPage;
    bsTMapT    *tagEntry;     /* The current tag on the tag page */
    bsTMapT    *prevTagEntry; /* The previous tag on the tag page */
    tagNodeT   *pTagNode;

    /*
     * Set pExtents to the fileset's tag file extents.
     */
    pExtents   = fileset->tagLBN;
    numExtents = fileset->tagLBNSize;

    /*
     * Find the page in the tagfile where our tag's entry would be.
     */
    tagPageNum     = tagNum / BS_TD_TAGS_PG;
    tagEntryOnPage = tagNum % BS_TD_TAGS_PG;
 
    status = convert_page_to_lbn(pExtents, numExtents, tagPageNum, 
                                 &currLbn, &currVol);
    if (status != SUCCESS) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_381, 
			 "Can't convert tag page %ld to LBN.\n"), 
		 tagPageNum);
        return FAILURE;
    }

    /*
     * Read the tag page
     */
    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
    if (status != SUCCESS) {
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
    if (tagEntry->tmFlags & BS_TD_IN_USE) {
        /*
         * Where you see this most often is the same tag number but
         * with different seqNo.  As find_node uses seqNo as a
         * key, only one shows up.
         */
        status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
                                       modified, FALSE);
        if (status != SUCCESS) {
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
	 *
         * NOTE: tpNextFreeMap and tpNextMap are indexed 1 to BS_TD_TAGS_PG, 
         *       while array is 0 to BS_TD_TAGS_PG-1.
         */
        while ((int32_t)currentTMap != -1) {
	    assert((currentTMap < BS_TD_TAGS_PG) && 
		   ((int32_t)currentTMap >= 0));

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
			       catgets(_m_catd, S_FSCK_1, FSCK_401, 
				       "Modified '%s' file system tag %ld.%u to have primary mcell of (%d,%ld,%d).\n"),
			       fileset->fsName, tagNum, seqNum, 
			       primMCId.volume, primMCId.page, primMCId.cell);
    if (status != SUCCESS) {
	return status;
    }
    tagEntry->tmBfMCId = primMCId;
    tagEntry->tmSeqNo = seqNum;
    tagEntry->tmFlags = BS_TD_IN_USE;
    modified = TRUE;

    /*
     * Need to save this pages, nextFreePage incase the
     * page needs to be removed from the freePageList chain.
     */
    freePage = pTagPage->tpNextFreePage;
    tagsUsed = pTagPage->tpNumAllocTMaps;
    if (tagPageNum == 0) {
	tagsUsed++;
    }

    /*
     * Release the page.
     */
    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
                                   modified, FALSE);
    if (status != SUCCESS) {
        return status;
    }
    modified = FALSE;

    /*
     * If this page is on the freeTagPage list, and is no longer
     * free we need to remove the page from the list.
     */
    if (tagsUsed == BS_TD_TAGS_PG) {
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
	if (status != SUCCESS) {
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
				       catgets(_m_catd, S_FSCK_1, FSCK_402,
					       "Removed '%s' file system's tag page %ld from free tag page list.\n"),
				       fileset->fsName, tagPageNum);

	    if (status != SUCCESS) {
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
	if (status != SUCCESS) {
	    return status;
	}
	modified = FALSE;

	/*
	 * Walk the list until we remove it or find the end.
	 */
	while ((nextFreePage != -1) &&
	       (pageRemoved != TRUE)) {
	    /*
	     * Find the next page in the chain.
	     */
	    status = convert_page_to_lbn(pExtents, numExtents,
					 nextFreePage,
					 &currLbn, &currVol);
	    if (status != SUCCESS) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_381, 
				 "Can't convert tag page %ld to LBN.\n"), 
			 nextFreePage);
		return FAILURE;
	    }

	    status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	    if (status != SUCCESS) {
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
					   catgets(_m_catd, S_FSCK_1, FSCK_402, 
						   "Removed '%s' file system's tag page %ld from free tag page list.\n"),
					   fileset->fsName, tagPageNum);
		
		if (status != SUCCESS) {
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
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = FALSE;
	} /* end of while we walk the list. */
    } /* end if we need to remove page from free list */
    
    /*
     * Insert tag into skiplist
     */
    pTagNode = (tagNodeT *)ffd_malloc(sizeof(tagNodeT));
    if (pTagNode == NULL) {
        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FSCK_1, FSCK_403, 
			 "tag node"));
        return NO_MEMORY;
    }

    pTagNode->tagSeq.tag_num = tagNum;
    pTagNode->tagSeq.tag_seq = seqNum;
    pTagNode->tagMCId = primMCId;
    pTagNode->status = 0;

    /*
     * Will be loaded later with valid values.
     */
    pTagNode->fileType = 0;
    pTagNode->size = 0;
    pTagNode->fobsFound = 0;
    pTagNode->numLinks = 0;
    pTagNode->numLinksFound = 0;
    pTagNode->parentTagNum.tag_num = 0;
    pTagNode->parentTagNum.tag_seq = 0;
    pTagNode->dirIndexTagNum.tag_num = 0;
    pTagNode->dirIndexTagNum.tag_seq = 0;
    
    status = insert_node(fileset->tagList, pTagNode);
    if (status != SUCCESS) {
        return status;
    }

    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FSCK_1, FSCK_404,
		     "Modifying tag %ld.%u to point to (%d,%ld,%d).\n"),
	     tagNum, seqNum, primMCId.volume, primMCId.page, primMCId.cell);

    return SUCCESS;
} /* end correct_tag_to_mcell */


/* 
 * Function Name: correct_nodes
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
int 
correct_nodes(domainT *domain) 
{
    char *funcName = "correct_nodes";
    int	 status;
    int	 fixedBMT; /* Flag set if we fixed anything in the BMT. */
    int  modified;
    xtntNumT bmtXtnt;
    int	     fsFound;
    int	     extentSize;
    int	     x;
    int	     rootTag;
    int      tagFound;
    tagNumT  tagNum;
    tagSeqT  tagSeq;
    char fsName[BS_SET_NAME_SZ + 2];
    volNumT  volNum;
    pageNumT pageNum;
    lbnNumT  currLBN;
    cellNumT mcellNum;
    nodeT         *node;
    pageT         *pPage;
    bsMPgT	  *bmtPage;
    bsMCT	  *pMcell;
    bsMRT	  *pRecord;
    bsXtntRT	  *pXtnt;
    bsXtraXtntRT  *pXtra;
    mcellNodeT	  tmpMcellNode;
    mcellNodeT	  *pMcellNode;
    tagNodeT	  tmpTagNode;
    tagNodeT	  *pTagNode;
    filesetT	  *fileset;
    fobtoLBNT	  *bmtLBN;
    xtntNumT      bmtLBNSize;
    mcidT         sbmMCId;
    fobtoLBNT     extentArray[BMT_XTRA_XTNTS];

    /*
     * Init variables
     */
    fixedBMT = FALSE;
    modified = FALSE;
    pMcellNode = &tmpMcellNode;
    pTagNode = &tmpTagNode;
    
    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

	if (domain->volumes[volNum].volFD == -1) {
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
	    for (pageNum = FOB2PAGE(bmtLBN[bmtXtnt].fob);
		 pageNum < FOB2PAGE(bmtLBN[bmtXtnt+1].fob);
		 pageNum++) {

		currLBN = bmtLBN[bmtXtnt].lbn + 
		          (ADVFS_METADATA_PGSZ_IN_BLKS * 
			   (pageNum - FOB2PAGE(bmtLBN[bmtXtnt].fob)));

		status = read_page_from_cache(bmtLBN[bmtXtnt].volume, 
					      currLBN, &pPage);
		if (status != SUCCESS) {
		    return status;
		} 
		bmtPage = (bsMPgT *)pPage->pageBuf;

		/* loop through all the mcells in the page */

		for (mcellNum = 0; mcellNum < BSPG_CELLS; mcellNum++) {

		    pMcell = &(bmtPage->bsMCA[mcellNum]);
		    tagNum = pMcell->mcTag.tag_num;
		    tagSeq = pMcell->mcTag.tag_seq;
		    sbmMCId.volume = volNum;
		    sbmMCId.page = pageNum;
		    sbmMCId.cell = mcellNum;

		    /*
		     * Check to see if any free mcells are in the mcell 
		     * node list. If so, delete the node from the list.
		     */
		    if (pMcell->mcTag.tag_num == 0 && 
			pMcell->mcBfSetTag.tag_num == 0) {

			if (pageNum == 0 && mcellNum == 0) {
			    /*
			     * Continue on to next mcell.
			     */
			    continue;
			}

			tmpMcellNode.mcid.volume = volNum;
			tmpMcellNode.mcid.page = pageNum;
			tmpMcellNode.mcid.cell = mcellNum;
			pMcellNode = &tmpMcellNode;

			status = find_node(domain->mcellList,
					   (void **)&pMcellNode, &node);
			if (status != SUCCESS) {
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
				 catgets(_m_catd, S_FSCK_1, FSCK_405, 
					 "Deleting mcell node (%d,%ld,%d).\n"),
				 volNum, pageNum, mcellNum);

			status = delete_node(domain->mcellList, 
					     (void **)&pMcellNode); 
			if (status == SUCCESS) {
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
		     * case: root tag file.
		     */
		    if (IS_ROOT_TAG(pMcell->mcBfSetTag)) {
			fsFound = TRUE;
			rootTag	= TRUE;
			strcpy(fsName, "rootTag");
		    } else {
			fileset = domain->filesets;
			fsFound = FALSE;
			rootTag	= FALSE;

			while (fileset != NULL) {
			    if (pMcell->mcBfSetTag.tag_num == 
				fileset->filesetId.dirTag.tag_num) {
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
			if (fsFound == FALSE) {
			    status = free_mcell(domain, volNum, 
						pPage, mcellNum);
			    if (status != SUCCESS) {
				writemsg(SV_ERR | SV_LOG_ERR, 
					 catgets(_m_catd, S_FSCK_1, FSCK_130,
						 "Can't free mcell %d on volume %s BMT page %ld.\n"),
					 mcellNum,
					 domain->volumes[volNum].volName,
					 pageNum);
				return FAILURE;
			    }
			    
			    status = add_fixed_message(&(pPage->messages),
						       BMT,
						       catgets(_m_catd, S_FSCK_1, FSCK_406, 
							       "Deleted mcell (%d,%ld,%d), belonged to unknown file system.\n"), 
						       volNum, pageNum, mcellNum);
			    if (status != SUCCESS) {
				return status;
			    }

			    modified = TRUE;
			    continue;
			} 

			strcpy(fsName, fileset->fsName);
		    } /* end if normal fileset (else clause) */

		    /*
		     * If the mcell has a next pointer, check it
		     * to see if it's in the skip list and if we have
		     * any loops to it.
		     */
		    if (pMcell->mcNextMCId.volume != 0) {

			tmpMcellNode.mcid = pMcell->mcNextMCId;
			pMcellNode = &tmpMcellNode;
				
			status = find_node(domain->mcellList,
					   (void **)&pMcellNode, &node);
			if ((status == NOT_FOUND) ||
			    ((status == SUCCESS) &&
			     (MC_IS_FOUND_PTR(pMcellNode->status)))) {
			    /* 
			     * If the next mcell isn't in the list or if 
			     * the chain bit is already set for it, then 
			     * truncate the chain.
			     */
			    status = add_fixed_message(&(pPage->messages),
						       BMT,
						       catgets(_m_catd, S_FSCK_1, FSCK_407, 
							       "Modified mcell (%d,%ld,%d) next chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
						       volNum, pageNum, mcellNum,
						       pMcell->mcNextMCId.volume,
						       pMcell->mcNextMCId.page,
						       pMcell->mcNextMCId.cell,
						       0, 0, 0);
			    if (status != SUCCESS) {
				return status;
			    }

			    ZERO_MCID(pMcell->mcNextMCId);
			    modified = TRUE;
			} else if (status == SUCCESS) {
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
		    if (rootTag == FALSE) {
			/*
			 * Working on ALL filesets or the fileset we want.
			 */
			tmpTagNode.tagSeq.tag_num = tagNum;
			pTagNode = &tmpTagNode;

			/*
			 * Find the tag which belongs to this mcell, 
			 * it will be used later.
			 */
			status = find_node(fileset->tagList, 
					   (void **)&pTagNode, &node);
			if (status == SUCCESS &&
			    (tagSeq == pTagNode->tagSeq.tag_seq)) {
			    tagFound = TRUE;
			} else if ((NOT_FOUND == status) ||
				   ((status == SUCCESS) &&
				    (tagSeq != pTagNode->tagSeq.tag_seq))) {
			    tagFound = FALSE;
			    /* 
			     * If the tag does not exist and this is
			     * the primary mcell then we will attach
			     * this mcell to the tag.  
			     */
			    tmpMcellNode.mcid.volume = volNum;
			    tmpMcellNode.mcid.page = pageNum;
			    tmpMcellNode.mcid.cell = mcellNum;
			    pMcellNode = &tmpMcellNode;

			    status = find_node(domain->mcellList,
					       (void **)&pMcellNode, &node);
			    if (status != SUCCESS) {
				return status;
			    }

			    if ((MC_IS_PRIMARY(pMcellNode->status)) &&
				(MC_IS_FOUND_PTR(pMcellNode->status) == FALSE)) {
				/* 
				 * Need to fix tag file so that it
				 * points to this mcell.  
				 */
				status = correct_tag_to_mcell(fileset, 
							      tagNum,
							      tagSeq, 
							      pMcellNode->mcid);
				if (status != SUCCESS) {
				    if (status == NO_MEMORY) {
					return status;
				    } else {
					writemsg(SV_DEBUG,
						 catgets(_m_catd, S_FSCK_1, FSCK_408, 
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
				    tmpTagNode.tagSeq.tag_num = tagNum;
				    pTagNode = &tmpTagNode;

				    status = find_node(fileset->tagList, 
						       (void **)&pTagNode, &node);
				    if (status != SUCCESS) {
					return status;
				    }
				    tagFound = TRUE;
				} /* end if correct_tag_to_mcell */
			    } else {
				MC_SET_MISSING_TAG(pMcellNode->status);
			    } /* end if primary */
			} else if (status != SUCCESS) {
			    return status;
			}/* end if not found */
		    } /* end if rootTag == FALSE */			

		    /* loop through all the records in the mcell */

		    pRecord  = (bsMRT *)pMcell->bsMR0;
		    while ((pRecord->type != BSR_NIL) &&
			   (pRecord->bCnt != 0) &&
			   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
			switch (pRecord->type) {

			  case (BSR_XTNTS):
			    pXtnt = (bsXtntRT *)((char *)pRecord +
						 sizeof(bsMRT));

			    /*
			     * Loop through the extents in this record.
			     */
			    for (x = 0; 
				 x < pXtnt->xCnt && x < BMT_XTNTS;
				 x++) {
				extentArray[x].volume = volNum;
				extentArray[x].fob = pXtnt->bsXA[x].bsx_fob_offset;
				extentArray[x].lbn = pXtnt->bsXA[x].bsx_vd_blk;
				if (x < (pXtnt->xCnt -1)) {
				    extentSize = 
					   pXtnt->bsXA[x + 1].bsx_fob_offset -
					   pXtnt->bsXA[x].bsx_fob_offset;
				    if ((rootTag == FALSE) &&
					(tagFound == TRUE)) {
					pTagNode->fobsFound += extentSize;
				    }
				}
			    } /* end for xtnt loop */

			    status = collect_sbm_info(domain, 
						      extentArray,
						      pXtnt->xCnt,
						      pMcell->mcTag.tag_num, 
						      pMcell->mcBfSetTag.tag_num,
						      &sbmMCId, 0);
			    if (status != SUCCESS) {
				return status;
			    }
				  
			    if (pXtnt->chainMCId.volume != 0) {
				mcellNodeT tmpChainNode;
				mcellNodeT *pChainNode;
						
				tmpChainNode.mcid = pXtnt->chainMCId;
				pChainNode = &tmpChainNode;
						
				status = find_node(domain->mcellList,
						   (void **)&pChainNode, &node);
				if (status == NOT_FOUND || 
				    (status == SUCCESS &&
				     MC_IS_FOUND_PTR(pChainNode->status))){
				    /*
				     * Clear the chain pointer if we didn't
				     * find the mcell node or if the chain 
				     * bit is already set in the node.
				     */
				    status = add_fixed_message(&(pPage->messages), 
							       BMT,
							       catgets(_m_catd, S_FSCK_1, FSCK_409, 
								       "Modified mcell (%d,%ld,%d) xtnt chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
							       volNum, pageNum, mcellNum,
							       pXtnt->chainMCId.volume,
							       pXtnt->chainMCId.page,
							       pXtnt->chainMCId.cell,
							       0, 0, 0);
				    if (status != SUCCESS) {
					return status;
				    }

				    ZERO_MCID(pXtnt->chainMCId);
				    pXtnt->mcellCnt = 1;
				    modified = TRUE;
				} else if (status == SUCCESS) {
				    MC_SET_FOUND_PTR(pChainNode->status);
				} else {
				    return status;
				}/* end if status on find_node */
			    } /* end if chain pointer exists in record */
			      
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
				extentArray[x].fob = pXtra->bsXA[x].bsx_fob_offset;
				extentArray[x].lbn = pXtra->bsXA[x].bsx_vd_blk;

				if (x < pXtra->xCnt - 1) {
				    extentSize = 
					    pXtra->bsXA[x + 1].bsx_fob_offset -
					    pXtra->bsXA[x].bsx_fob_offset;
				    if ((rootTag == FALSE) &&
					(tagFound == TRUE)) {
					pTagNode->fobsFound += extentSize;
				    }
				}
			    } /* end for xtnt loop */

			    status = collect_sbm_info(domain, 
						      extentArray,
						      pXtra->xCnt,
						      pMcell->mcTag.tag_num, 
						      pMcell->mcBfSetTag.tag_num,
						      &sbmMCId, 0);
			    if (status != SUCCESS) {
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
			pRecord = NEXT_MREC(pRecord);

		    } /* end while loop on records */
		} /* end for each mcell on the page */

		status = release_page_to_cache(bmtLBN[bmtXtnt].volume, 
					       currLBN, pPage, 
					       modified, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		if (modified == TRUE) {
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

    if (fixedBMT == TRUE) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_nodes */


/* 
 * Function Name: correct_orphans
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
int 
correct_orphans(domainT *domain) 
{
    char *funcName = "correct_orphans";
    int	 status;
    int	 fixedBMT; /* Flag set if we fixed anything in the BMT. */
    tagNumT tagNum;
    tagSeqT tagSeq;
    tagNumT setTagNum;
    int	 fsFound;
    cellNumT freeMcell;
    volNumT  volNum;
    pageNumT pageNum;
    cellNumT mcellNum;
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
		  catgets(_m_catd, S_FSCK_1, FSCK_410, 
			  "Can't find first mcell node for domain %s.\n"),
                  domain->dmnName);
        corrupt_exit(NULL);
    }
    else if (status != SUCCESS) {
	return status;
    }
    
    while (NULL != pMcellNode) {
	/*
	 * Move next to current, and find next.
	 */
	freeMcell = FALSE;
	volNum	 = FETCH_MC_VOLUME(pMcellNode->mcid);
	pageNum  = FETCH_MC_PAGE(pMcellNode->mcid);
	mcellNum = FETCH_MC_CELL(pMcellNode->mcid);
	 
	if (MC_IS_MISSING_TAG(pMcellNode->status)) {
	    /*
	     * Because we did not free the mcell when we couldn't find the
	     * tag.tag_seq, and we only re-hooked up the primaries we need 
	     * to re-check if the mcell has a valid tag.  If not this time
	     * we can delete it.
	     */
	    tagNum = pMcellNode->tagSeq.tag_num;
	    tagSeq = pMcellNode->tagSeq.tag_seq;
	    setTagNum = pMcellNode->setTag.tag_num;

	    tmpTagNode.tagSeq.tag_num = tagNum; 
	    pTagNode = &tmpTagNode;

	    fileset = domain->filesets;
	    fsFound = FALSE;
	    
	    while (NULL != fileset) {
		if (setTagNum == fileset->filesetId.dirTag.tag_num) {
		    fsFound = TRUE;
		    break;
		}
		fileset = fileset->next;
	    }

	    if (fsFound == TRUE) {
		status = find_node(fileset->tagList, 
				   (void **)&pTagNode, &tagNode);
		if (status != SUCCESS) {
		    if (NOT_FOUND == status) {
			/*
			 * We can't find a tag which this mcell belongs
			 * to, so we might as well delete it.
			 */
			freeMcell = TRUE;
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FSCK_1, FSCK_411,
					 "Deleting mcell (%d,%ld,%d), its tag %ld.%u does not exist.\n"),
				 volNum, pageNum, mcellNum, 
				 tagNum, tagSeq);

		    } else {
			return status;
		    }
		} else {
		    if (tagSeq != pTagNode->tagSeq.tag_seq) {
			/*
			 * We can't find a tag which this mcell belongs
			 * to, so we might as well delete it.
			 */
			freeMcell = TRUE;
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FSCK_1, FSCK_411,
					 "Deleting mcell (%d,%ld,%d), its tag %ld.%u does not exist.\n"),
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
	if ((MC_IS_FOUND_PTR(pMcellNode->status) == FALSE) ||
	    (freeMcell == TRUE)) {
	    status = free_orphan(domain, volNum, pageNum, mcellNum);
	    if (status != SUCCESS) {
		return status;
	    }
	    freeMcell = TRUE;
	    fixedBMT  = TRUE;
	} /* end if mcell is orphaned */

	/*
	 * Locate the next mcell to work on.
	 */
	if (freeMcell == TRUE) {
	    /*
	     * Need to locate the next mcell, without using node.
	     *
	     * we have volNum,pageNum,mcellNum
	     */
	    tmpMcellNode.mcid.volume = volNum;
	    tmpMcellNode.mcid.page = pageNum;
	    tmpMcellNode.mcid.cell = mcellNum;
	    pMcellNode = &tmpMcellNode;

	    status = find_node_or_next(domain->mcellList,
				       (void **)&pMcellNode, &node);
	    if (status != SUCCESS) {
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
	    if (status != SUCCESS) {
		if (NOT_FOUND == status) {
		    pMcellNode = NULL;
		} else {
		    return status;
		}
	    }
	} /* else node not freed */
    } /* end mcell node loop */

    if (fixedBMT == TRUE) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_orphans */


/*
 * Function Name: correct_bmt_chains
 *
 * Description: This is called after we have loaded all tags into the
 *		tag list. At this point we have already handled any
 *		loops which may exist, so we do not need to worry
 *		about getting into mcell loops. Also each individual
 *		mcell has already been checked. This function will
 *		check data which crosses mcells, such as extents.
 *		To do this the function will walk the tag
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
int 
correct_bmt_chains(filesetT *fileset)
{
    char       *funcName = "correct_bmt_chains";
    int	       status;
    int        bmtModified;
    int	       *recordArray;
    int	       sizeRecordArray;
    fobtoLBNT  *xtntArray;    /* A temp list of extents this tag has */
    xtntNumT   sizeXtntArray; /* Size of the xtnt array */
    tagNodeT   *pTagNode;
    tagNodeT   *pNextTagNode;
    nodeT      *nextNode;

    /*
     * Init variables
     */
    bmtModified	= FALSE;

    /*
     * We create an array to store extents for each file and realloc
     * to a larger size as needed.  We also allocate space to keep
     * track of where each record starts.
     */
    sizeRecordArray = 100; /* Best guess of starting size */
    recordArray = (int *)ffd_calloc(sizeRecordArray + 1, sizeof(int));
    if (recordArray == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_412,
			 "recordArray"));
	return NO_MEMORY;
    }

    sizeXtntArray = 1000; /* Best guess of starting size */
    xtntArray = (fobtoLBNT *)ffd_calloc(sizeXtntArray+1, sizeof(fobtoLBNT));
    if (xtntArray == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_413,
			 "extent list"));
	return NO_MEMORY;
    }
    
    /*
     * find the first tag node and walk the chain.  Because we might
     * delete this node in the middle of the loop we need a way to get
     * access to the next node in the list.
     *
     * We load the first node into nextNode so we can walk the loop.
     */
    status = find_first_node(fileset->tagList, (void **)&pNextTagNode,
			     &nextNode);
    if (status == NOT_FOUND) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FSCK_1, FSCK_198,
			  "Can't find first tag node for '%s' file system.\n"),
                  fileset->fsName);
        corrupt_exit(NULL);
    }
    else if (status != SUCCESS) {
	return status;
    }

    /*
     * The first node found should always be tag 2 (tag 1 is not used
     * in page 0 and the root dir must always be tag (inode) 2, a UNIX
     * convention.  If not we have corruption which has not been
     * fixed.
     */
    if (pNextTagNode == NULL || pNextTagNode->tagSeq.tag_num != 2) {
	return CORRUPT;
    }  

    /* Each tag references the primary mcell for user file.  This loop
       walks the mcell chain for each tag, making corrcetions as
       needed.  Loop until all tags done. */
    
    while (pNextTagNode != NULL) {
	/*
	 * Assign current to the next node, then find next.
	 */
	pTagNode = pNextTagNode;

	/*
	 * Locate the next tag to work on.
	 */
	status = find_next_node(fileset->tagList, (void **)&pNextTagNode, 
				&nextNode);
	if (status != SUCCESS) {
	    if (status == NOT_FOUND) {
		pNextTagNode = NULL;
	    } else {
		return status;
	    }
	}

	status = correct_bmt_chain_by_tag(fileset, pTagNode,
					  &sizeRecordArray, &recordArray,
					  &sizeXtntArray, &xtntArray);
	if (status != SUCCESS) {
	    if (status == FIXED) {
		bmtModified = TRUE;
	    } else {
		return status;
	    }
	}
    } /* end tag loop */

    free (recordArray);
    free (xtntArray);

    if (bmtModified == TRUE) {
	return FIXED;
    }

    return SUCCESS;
} /* end correct_bmt_chains */


/*
 * Function Name: correct_bmt_chain_by_tag
 *
 * Description: This is called after we have loaded all tags into the
 *		tag skip list. At this point we have already handled any
 *		loops which may exist, so we do not need to worry
 *		about getting into mcell loops. Also each individual
 *		mcell has already been checked. This function will
 *		check data which crosses mcells, such as extents.
 *
 *	        This fucntion will follow a tag's nextMcell chain as
 *	        well as its chainMcell chain. This function can be
 *	        slow, as it loads each mcell on the chains, and
 *	        depending on how the mcells are distributed on disk
 *	        this could mean reading a new page for each mcell
 *	        (worst case).
 *
 *		A large portion of this code will involve pattern
 *		matching. For example: with a 3 mcell chain, and the
 *		first and the third mcell both say they belong to
 *		mcell 42, but the middle ones says it belong to
 *		mcell 2000, we will assume that the 2nd mcell's tag
 *		number has become corrupt and fix it. (ie [42] ->
 *		[2000] -> [42] becomes [42] -> [42] -> [42]).
 *
 *		There are some cases were we will not be able to match
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
 *     sizeRecordArray: The size of the array.
 *     recordArray: A dynamic array to contain the location of records.
 *     sizeXtntArray: The size of the Xtnt Array
 *     xtntArray: A dynamic array to contain the xtnt record.
 *
 * Returns: SUCCESS, FAILURE, FIXED, NO_MEMORY, CORRUPT
 */
static int 
correct_bmt_chain_by_tag(filesetT *fileset, 
			 tagNodeT *pTagNode,
			 int *sizeRecordArray, 
			 int **recordArray,
			 xtntNumT  *sizeXtntArray, 
			 fobtoLBNT **xtntArray)
{
    char       *funcName = "correct_bmt_chain_by_tag";
    int	       status;
    int        recNum;
    int	       modified;
    int        bmtModified;
    tagNumT    primTagNum;
    tagNumT    currentTagNum;
    tagNumT    nextTagNum; 
    tagSeqT    primSeqNum;
    tagSeqT    currentSeqNum;
    int	       extent;
    int	       whichRecord;
    xtntNumT   lastXtntEntry; /* the xtnt we are working on */
    int        reprocessRecord;
    int        foundXtra;
    int        foundFsData;
    int        foundBsrAttr;
    int	       foundAcl;
    int	       foundFsStat;	 
    int	       foundBsrXtnts;
    int        byteCount;
    volNumT    currVol;
    lbnNumT    currLbn;
    volNumT    prevVol;
    lbnNumT    prevLbn;
    uint32_t   linkSeg;
    mcidT      prevMCId;
    mcidT      currMCId;  /* The mcell currently being worked on */
    mcidT      nextMCId;     /* The next mcell on the next chain */
    mcidT      primMCId;  /* Used to store primary mcell of the tag */
    mcidT      chainMCId;    /* The next mcell on the chain */
    mcidT      xtntMCId;     /* The mcell which contains the xtnt record*/
    pageT      *pCurrentPage; /* The current page we are checking */
    pageT      *pPrevPage;    /* The prev page we are checking */
    domainT    *domain;
    volumeT    *volume;
    bsMPgT     *bmtPage;     /* The current BMT page we are working on */
    bsMPgT     *bmtPrevPage; /* The prev BMT page we are working on */
    bsMCT      *pMcell;	     /* The current BMT mcell we are working on */
    bsMCT      *pPrevMcell;  /* The prev  BMT mcell we are working on */
    bsMRT      *pRecord;
    mcellNodeT *pMcellNode;
    mcellNodeT tmpMcellNode;
    nodeT      *tmpNode;
    tagNodeT   tmpTagNode;
    tagNodeT   *pTempTagNode;
    statT      *pStat;
    statT      alignedStat; /* needed to fix alignment errors */

    /*
     * Init variables
     */
    domain	= fileset->domain;
    modified	= FALSE;
    bmtModified	= FALSE;

    if (FS_IS_SNAP(fileset->status)) {

	/*
	 * This fileset is a snapshot.
	 *
	 * Background: Initially, a snapshot's tag file is an exact
	 * copy of the parent tag file.  As files are subsequently
	 * accessed in the parent, new primary mcells are created for
	 * those files in the snashot and the snapshot tag file is
	 * updated to point to the new primary mcells.  The primary
	 * mcells of files that have not been accessed since the snap
	 * was created are dual referenced through both the parent and
	 * snap tag files.
	 */

	/*
	 * get this mcell's info from the mcell list.
	 */
	tmpMcellNode.mcid = pTagNode->tagMCId;
	pMcellNode = &tmpMcellNode;

	status = find_node(domain->mcellList, (void **)&pMcellNode, 
			   &tmpNode);
	if (status != SUCCESS) {
	    /*
	     * Should always be found.
	     */
	    return status;
	}

	/*
	 * Now check this mcell's setTag to see if it belongs
	 * to the parent fileset or the snap fileset.
	 */
	if (fileset->parentSetTag.tag_num == pMcellNode->setTag.tag_num) {
	    /*
	     * This primary is double referenced through both the
	     * parent and snap tag directories so it still claims the
	     * parent fileset as it's owner.  We set a flag in the
	     * snap's tagNode to indicate that this tag's primary
	     * mcell (and the rest of it's mcells) also belongs to the
	     * original fileset.
	     * 
	     * This is useful to other parts of fsck that are looking
	     * through the mcells.  When extent arrays are created (by
	     * collect_extents()) each array entry for this file will
	     * have isOrig == TRUE, to indicate this same meaning.
	     */

	    T_SET_SNAP_ORIG_PMCELL(pTagNode->status);

	    /*
	     * We don't want to process this tagnode twice (once
	     * through the original and once again now through the
	     * snap), so we just make this snap's tagnode a copy of
	     * the parent one and our work here is done.
	     */
	    tmpTagNode.tagSeq.tag_num = pTagNode->tagSeq.tag_num;
	    pTempTagNode = &tmpTagNode;
	    status = find_node(fileset->origSnapPtr->tagList,
			       (void **)&pTempTagNode, &tmpNode);
	    if (status != SUCCESS) {
		/*
		 * Should always be found.
		 */
		return status;
	    }

	    pTagNode->size		= pTempTagNode->size;
	    pTagNode->fileType		= pTempTagNode->fileType;
	    pTagNode->fobsFound	        = pTempTagNode->fobsFound;
	    pTagNode->numLinks		= pTempTagNode->numLinks;
	    pTagNode->parentTagNum	= pTempTagNode->parentTagNum;
	    if (T_IS_DIR_INDEX(pTempTagNode->status)) {
		T_SET_DIR_INDEX(pTagNode->status);
	    }
	    
	    return SUCCESS;
	}
    } /* end: if fileset is a snap */

    /*
     * Zero out xtntArray
     */
    bzero(*xtntArray, *sizeXtntArray * sizeof(fobtoLBNT));
    lastXtntEntry = 0;

    /*
     * Set up variables to mark which record we are on.
     */
    whichRecord = 0;
    (*recordArray)[whichRecord] = -1;

    /*
     * init these booleans to show what records we've found.
     */
    foundFsStat = FALSE;
    foundBsrXtnts = FALSE;
    foundFsData = FALSE;
    foundBsrAttr = FALSE;
    foundAcl = FALSE;

    /*
     * Initialize mcell pointers.  We are setting prevMCId to 
     * the currMCId to make the loop simpler.
     */

    linkSeg = 0;
    primMCId = pTagNode->tagMCId;
    currMCId = primMCId;
    prevMCId = currMCId;
    ZERO_MCID(chainMCId);

    primTagNum    = pTagNode->tagSeq.tag_num;
    primSeqNum    = pTagNode->tagSeq.tag_seq;
    currentTagNum = primTagNum;
    currentSeqNum = primSeqNum;

    /*
     * Loop through mcell linked list (mcNextMCId) and extent chain
     * (mcChainMCId)
     */
    while (currMCId.volume != 0) {
	/*
	 * Read the mcell page (first get LBN).
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, 
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCId.cell]);

	/* check/correct the link segment number */

	if (pMcell->mcLinkSegment != linkSeg) {

	    /* special case: link segment numbers do not increment for
	       mcells containing an ACL record.  If this isn't an ACL
	       record, correct the link segment number. */

	    pRecord = (bsMRT *)pMcell->bsMR0;

	    if (pRecord->type != BSR_ACL_REC) {
		status = add_fixed_message(&(pCurrentPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_399, 
						   "Modified link segment in mcell (%d,%ld,%d) from %d to %d.\n"),
					   currMCId.volume, 
					   currMCId.page,
					   currMCId.cell, 
					   pMcell->mcLinkSegment,
					   linkSeg);
		if (status != SUCCESS) {
		    return status;
		}
		pMcell->mcLinkSegment = linkSeg++;
		modified = TRUE;	    
	    }
	} else {
	    linkSeg++;
	}

	nextMCId = pMcell->mcNextMCId;

	/* if this is not the end of the mcell list... */

	if (nextMCId.volume != 0) {
	    /*
	     * Check if the nextMCId exists (via skip list).
	     */
	    tmpMcellNode.mcid = nextMCId;
	    pMcellNode = &tmpMcellNode;

	    status = find_node(domain->mcellList, (void **)&pMcellNode, 
			       &tmpNode);
	    if (status != SUCCESS) {
		if (status == NOT_FOUND) {
		    /* if the "next" mcell doesn't exist, terminate
		       the linkage */
		    status = add_fixed_message(&(pCurrentPage->messages), BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_407, 
						       "Modified mcell (%d,%ld,%d) next chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					       currMCId.volume, 
					       currMCId.page,
					       currMCId.cell, 
					       pMcell->mcNextMCId.volume,
					       pMcell->mcNextMCId.page,
					       pMcell->mcNextMCId.cell,
					       0, 0, 0);
		    if (status != SUCCESS) {
			return status;
		    }
		    ZERO_MCID(pMcell->mcNextMCId);
		    ZERO_MCID(nextMCId);
		    nextTagNum = 0;
		    modified = TRUE;
		} else {
		    return status;
		}
	    } else {  /* "next" pMcellNode was found */
		nextTagNum = pMcellNode->tagSeq.tag_num;
	    }
	} else {
	    /*
	     * End of next mcell list.
	     */
	    ZERO_MCID(nextMCId);
	    nextTagNum = 0;
	} /* end if nextMCId.volume */

	/*
	 * Check if this mcell has the correct tag and set number.
	 */
	status = correct_chain_tag_and_set(fileset, currMCId, 
					   prevMCId, primTagNum, 
					   primSeqNum, nextTagNum, 
					   pCurrentPage);
	if (status == FIXED) {
	    modified = TRUE;
	} else if (status == FIXED_PRIOR) {
	    /* 
	     * The nextMCId link of the mcell which pointed to this
	     * mcell (prevMCId) was incorrect and therefore zero'd
	     * when the previous mcell was checked.  This leaves
	     * currMCId as an orphan.
	     * 
	     * If we can find a tag which this mcell belongs to we'll
	     * re-attach it, and then re-call this function on it so
	     * that its chains can be fixed.
	     *
	     * Then we need to break out of the primary next loop.
	     * and we know there are no more mcells in the current chain
	     * except for the chain chain which will be looked at in the 
	     * next loop.
	     */
	    
	    /*
	     * Release page to stop deadlocks.
	     */
	    status = release_page_to_cache(currVol, currLbn, 
					   pCurrentPage, TRUE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    if (modified == TRUE) {
		bmtModified = TRUE;
		modified = FALSE;
	    }

	    /*
	     * Is this mcell a primary mcell for a different tag.
	     */
	    tmpMcellNode.mcid = currMCId;
	    pMcellNode = &tmpMcellNode;

	    status = find_node(domain->mcellList, (void **)&pMcellNode, 
			       &tmpNode);
	    if (status != SUCCESS) {
		return status;
	    }
	    
	    if (MC_IS_PRIMARY(pMcellNode->status)) {
		/*
		 * Now we know its a primary mcell, now we need to check
		 * if the tag that this mcell believes it belongs to is
		 * free.
		 */
		tmpTagNode.tagSeq.tag_num = currentTagNum;
		pTempTagNode = &tmpTagNode;

		/*
		 * Find the tag which this mcell says it belongs to.
		 */
		status = find_node(fileset->tagList, 
				   (void **)&pTempTagNode, &tmpNode);
		if (status == SUCCESS) {
		    /*
		     * The tag already has a primary, so at this time
		     * we will just assume the one it has is correct.
		     * As we have not reattached it, it will be handled
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
						  currentTagNum,
						  currentSeqNum, 
						  currMCId);
		    if (status != SUCCESS) {
			if (NO_MEMORY == status) {
			    return status;
			} else {
			    writemsg(SV_DEBUG,
				     catgets(_m_catd, S_FSCK_1, FSCK_408, 
					     "Duplicate tag %d - different seq - Setting to missing.\n"),
				     currentTagNum);
			    MC_SET_MISSING_TAG(pMcellNode->status);
			}
		    } else {
			MC_SET_FOUND_PTR(pMcellNode->status); 

			/* 
			 * correct_tag_to_mcell created the tag node. 
			 */
			tmpTagNode.tagSeq.tag_num = currentTagNum;
			pTempTagNode = &tmpTagNode;

			status = find_node(fileset->tagList, 
					   (void **)&pTempTagNode, 
					   &tmpNode);
			if (status != SUCCESS) {
			    return status;
			}
			
			/*
			 * We have attached the mcell chain to a tag.
			 * The tag node could have already been passed
			 * by in the tag loop so we need to process
			 * the chain again.  It is possible this tag chain
			 * will be processed twice.
			 */
			status = correct_bmt_chain_by_tag(fileset, 
							  pTempTagNode,
							  sizeRecordArray, 
							  recordArray,
							  sizeXtntArray, 
							  xtntArray);
			if (status != SUCCESS) {
			    if (status == FIXED) {
				bmtModified = TRUE;
			    } else {
				return status;
			    }
			}
		    } /* end if correct_tag_to_mcell */
		} /* end NOT_FOUND */
	    } /* end IS PRIMARY */
	    break; /* The chain was truncated we can break out of loop */
	} else if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Now loop through primary chain's records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	recNum  = 0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
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
	     * Check to see if this record could exceed the
	     * record Array.
	     */
	    if (whichRecord + 1 > *sizeRecordArray) {
		/*	
		 * Double the size of the record Array.
		 */
		*sizeRecordArray = *sizeRecordArray * 2;

		*recordArray = (int *)ffd_realloc(*recordArray,
						 (*sizeRecordArray + 1) *
						 sizeof(int));
		if (*recordArray == NULL) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_10,
				     "Can't allocate memory for %s.\n"),
			     catgets(_m_catd, S_FSCK_1, FSCK_416,
				     "record array"));
		    return NO_MEMORY;
		}

		/*
		 * Clear the newly added section.
		 */
		bzero(&((*recordArray)[whichRecord]),
		      ((*sizeRecordArray - whichRecord) * sizeof(int)));
	    } /* end if: need to double record Array */

	    /*
	     * check to see if this record could exceed the 
	     * extent list.
	     */
	    if (lastXtntEntry +  BMT_XTRA_XTNTS > *sizeXtntArray) {
		/*	
		 * Double the size of the extent list.
		 */
		*sizeXtntArray = *sizeXtntArray * 2;

		*xtntArray = (fobtoLBNT *)ffd_realloc(*xtntArray,
						      (*sizeXtntArray + 1) *
						      sizeof(fobtoLBNT));
		if (*xtntArray == NULL) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_10,
				     "Can't allocate memory for %s.\n"),
			     catgets(_m_catd, S_FSCK_1, FSCK_417,
				     "extent array"));
		    return NO_MEMORY;
		}

		/*
		 * Clear the newly added section.
		 */
		bzero(&((*xtntArray)[lastXtntEntry]),
		      ((*sizeXtntArray - lastXtntEntry) * sizeof(fobtoLBNT)));

	    } /* end if: need to double the xtntArray */

	    /*
	     * Multiple checks are done here, the main one is that
	     * we need to collect the extents to see if we have 
	     * overlaps between different mcell records.
	     */
	    switch (pRecord->type) {
	        case BMTR_FS_DATA: /* 254 */
		    if (foundFsData == TRUE) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrentPage,
						    currMCId.cell, 
						    recNum);
			if (status != SUCCESS) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    foundFsData = TRUE;
		    break;

	        case BMTR_FS_STAT: /* 255 */
		    if (foundFsStat == TRUE) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrentPage,
						    currMCId.cell, 
						    recNum);
			if (status != SUCCESS) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    foundFsStat = TRUE;

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
		    if (!BS_BFTAG_EQL(alignedStat.st_ino, pMcell->mcTag)) {
			status = add_fixed_message(&(pCurrentPage->messages), BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_418,
							   "Modified mcell (%d,%ld,%d)'s st_ino from %ld.%u to %ld.%u.\n"),
						   currMCId.volume,
						   currMCId.page,
						   currMCId.cell,
						   alignedStat.st_ino.tag_num, 
						   alignedStat.st_ino.tag_seq,
						   pMcell->mcTag.tag_num,
						   pMcell->mcTag.tag_seq);
			if (status != SUCCESS) {
			    return status;
			}

			alignedStat.st_ino = pMcell->mcTag;

			/*
			 * Copy the aligned structure into the record.
			 */
			memcpy((char *)pStat, &alignedStat, sizeof(statT));
			modified = TRUE;
		    }

		    pTagNode->size = alignedStat.st_size;
		    pTagNode->fileType = alignedStat.st_mode;
		    pTagNode->numLinks = alignedStat.st_nlink;
		    pTagNode->parentTagNum = alignedStat.dir_tag;
#ifdef NOTNOW
		    printf("Set tagNode->parentTagNum to %ld.%u\n",
			   alignedStat.dir_tag.tag_num,
			   alignedStat.dir_tag.tag_seq);
#endif /* NOTNOW */
		    break;
		    
	        case BMTR_FS_INDEX_FILE: /* 250 */
		    /*
		     * All tags (except for directory indexes) should 
		     * have a FsStat record.	To make the code simpler 
		     * directory indexes will be 'marked' as having a 
		     * fsStat record.
		     */
		    T_SET_DIR_INDEX(pTagNode->status);
		    foundFsStat = TRUE;
		    break;

	        case BMTR_FS_DIR_INDEX_FILE: /* 249 */
		    /*
		     * This directory has a dir index file, store its
		     * tag.tag_seq number.
		     */
		    pDirIndex = (bfTagT *)((char *)pRecord + sizeof(bsMRT));
		    pTagNode->dirIndexTagNum.tag_num = pDirIndex->tag_num;
		    pTagNode->dirIndexTagNum.tag_seq = pDirIndex->tag_seq;
		    break;

	        case BSR_XTNTS:
		    if (foundBsrXtnts == TRUE) {
			/*
			 * There should only be one bsrXtnt record 
			 * per tag.
			 */
			status = add_fixed_message(&(pCurrentPage->messages), BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_419, 
							   "Modified mcell (%d,%ld,%d), removed an extra BSR_XTNT record.\n"),
						   currMCId.volume,
						   currMCId.page,
						   currMCId.cell);
			if (status != SUCCESS) {
			    return status;
			}

			status = clear_mcell_record(domain,
						    pCurrentPage,
						    currMCId.cell, 
						    recNum);
			if (status != SUCCESS) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    } 
			
		    foundBsrXtnts = TRUE;

		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    xtntMCId = currMCId;
			
		    /*
		     * Loop through all the extents in this record.
		     */
		    (*recordArray)[whichRecord] = lastXtntEntry;
		    whichRecord++;
		    (*recordArray)[whichRecord] = -1;

		    for (extent = 0; 
			 extent < pXtnt->xCnt && extent < BMT_XTNTS;
			 extent++) {

			(*xtntArray)[lastXtntEntry].volume = 
			    currMCId.volume;
			(*xtntArray)[lastXtntEntry].fob = 
			    pXtnt->bsXA[extent].bsx_fob_offset;
			(*xtntArray)[lastXtntEntry].lbn = 
			    pXtnt->bsXA[extent].bsx_vd_blk;

			lastXtntEntry++;

		    }/* end extent for loop */

		    /*
		     * Set chainMCId so we can follow it after we are
		     * done with the current chain.
		     */
		    chainMCId = pXtnt->chainMCId;

		    /*
		     * Check if the chainMCId exists (via skip list).
		     */
		    if (chainMCId.volume != 0)
			{
			    tmpMcellNode.mcid = chainMCId;
			    pMcellNode = &tmpMcellNode;
		
			    status = find_node(domain->mcellList, 
					       (void **)&pMcellNode, &tmpNode);
			    if (status != SUCCESS) {
				if (status == NOT_FOUND) {
				    status = add_fixed_message(&(pCurrentPage->messages), 
							       BMT,
							       catgets(_m_catd, S_FSCK_1, FSCK_409, 
								       "Modified mcell (%d,%ld,%d) xtnt chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
							       currMCId.volume,
							       currMCId.page,
							       currMCId.cell, 
							       chainMCId.volume,
							       chainMCId.page, 
							       chainMCId.cell,
							       0, 0, 0);
				    if (status != SUCCESS) {
					return status;
				    }
			
				    ZERO_MCID(pXtnt->chainMCId);
				    pXtnt->mcellCnt = 1;
				    ZERO_MCID(chainMCId);
				    modified = TRUE;
				} else {
				    return status;
				}
			    }
			} /* check if next chain exists */

		    /*
		     * If we still have a chain we need to check its tag.
		     */
		    if (chainMCId.volume != 0) {
			status = correct_bsr_xtnt_tag(fileset,
						      currMCId,
						      chainMCId,
						      pCurrentPage);
			if (status == FIXED) {
			    /*
			     * Reset chainMCId as it could have been
			     * truncated on us.
			     */
			    chainMCId = pXtnt->chainMCId;
			    modified = TRUE;
			} else if (status != SUCCESS) {
			    return status;
			}
		    }

		    break;
		    
	        case BSR_ACL_REC:
		    foundAcl = TRUE;
		    break;
		
	        case BSR_XTRA_XTNTS:
		    break;
		    
	        case (BSR_ATTR): /* 2 */
		    if (foundBsrAttr == TRUE) {
			/*
			 * More than one of these record are found.
			 */
			status = clear_mcell_record(domain,
						    pCurrentPage,
						    currMCId.cell, 
						    recNum);
			if (status != SUCCESS) {
			    return status;
			}
			reprocessRecord = TRUE;
			modified = TRUE;
			break;
		    }
		    foundBsrAttr = TRUE;

		    pBSRAttr = (bsBfAttrT *)((char *)pRecord + 
					     sizeof(bsMRT));
		    if (pBSRAttr->bfat_del_child_cnt) {
			/*
			 * This file has been deleted from the parent,
			 * but lives on with the snapshot.  When the
			 * file was deleted from the parent it's disk
			 * blocks were not really freed.  Instead, the
			 * mcells from the parent were "reparented" to
			 * this snap to save the work of COWing all
			 * the deleted file's blocks.  We set the flag
			 * here to indicate that the file must not be
			 * deleted until this snapshot is.
			 */
			T_SET_DELETE_WITH_SNAP(pTagNode->status);
		    }

		    break;

	        default:
		    break;
	    } /* end switch */
	    if (reprocessRecord == FALSE) {
		pRecord = NEXT_MREC(pRecord);

	    } else {
		/*
		 * Reset record number
		 */
		recNum--;
	    }
	} /* end record loop */

	/*
	 * This could be a mcell with no record, or we made it
	 * this way with the clear_mcell_record call.  We will
	 * base this off of its bCnt, we need to save this info
	 * to make sure we reset prevmcell correctly
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	byteCount = pRecord->bCnt;

	if (byteCount == 0) {
	    /*
	     * This mcell has no records.
	     * If it has a next chain, reset prevMCId to point to it.
	     * If it doesn't have a next chain the truncate prevMCId.
	     */
	    if (MCIDS_EQ(currMCId, primMCId)) {

		int tagEntryOnPage;
		pageNumT tagPageNum;
		lbnNumT  tagPageLbn;
		volNumT  tagPageVol;
		fobtoLBNT *pExtents; /* The extents to check */
		xtntNumT  numExtents;
		pageT     *tagPage;  /* The tag page we are checking */
		bsTDirPgT *pTagPage; /* The tag page we are working on */
		bsTMapT   *tagEntry; /* The tag on the tag page */

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
		if (status != SUCCESS) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_381,
				     "Can't convert tag page %ld to LBN.\n"), 
			     tagPageNum);
		    return FAILURE;
		}

		/*
		 * Read the tag page
		 */
		status = read_page_from_cache(tagPageVol, tagPageLbn, 
					      &tagPage);
		if (status != SUCCESS) {
		    return status;
		}
		modified = FALSE;

		/*
		 * Now we have the correct tag page.  Find the correct tag
		 */
		pTagPage = (bsTDirPgT *)tagPage->pageBuf;
		tagEntry = &(pTagPage->tMapA[tagEntryOnPage]);

		status = add_fixed_message(&(tagPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_421,
						   "Modified tag %d's primary mcell from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					   primTagNum,
					   tagEntry->tmBfMCId.volume,
					   tagEntry->tmBfMCId.page,
					   tagEntry->tmBfMCId.cell,
					   nextMCId.volume,
					   nextMCId.page,
					   nextMCId.cell);
		if (status != SUCCESS) {
		    return status;
		}

		primMCId = nextMCId;   
		pTagNode->tagMCId = primMCId;
		tagEntry->tmBfMCId = nextMCId;	   

		status = release_page_to_cache(tagPageVol, tagPageLbn,
					       tagPage, TRUE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
	    } else {
		/*
		 * Other mcell
		 */
		if ((currMCId.page != prevMCId.page) ||
		    (currMCId.volume != prevMCId.volume)) {
		    volume = &(domain->volumes[prevMCId.volume]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 prevMCId.page,
						 &prevLbn, &prevVol);
		    if (status != SUCCESS) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FSCK_1, FSCK_128,
					 "Can't convert BMT page %ld to LBN.\n"), 
				 prevMCId.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, 
						  &pPrevPage);
		    if (status != SUCCESS) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrentPage;
		}

		pPrevMcell = &(bmtPrevPage->bsMCA[prevMCId.cell]);

		status = add_fixed_message(&(pPrevPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_422, 
						   "Modified mcell (%d,%ld,%d)'s next chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					   prevMCId.volume,
					   prevMCId.page,
					   prevMCId.cell, 
					   pPrevMcell->mcNextMCId.volume,
					   pPrevMcell->mcNextMCId.page,
					   pPrevMcell->mcNextMCId.cell,
					   nextMCId.volume,
					   nextMCId.page,
					   nextMCId.cell);
		if (status != SUCCESS) {
		    return status;
		}

		pPrevMcell->mcNextMCId = nextMCId;	    

		if ((currMCId.page != prevMCId.page) ||
		    (currMCId.volume != prevMCId.volume)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (status != SUCCESS) {
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

	    ZERO_MCID(pMcell->mcNextMCId);

	    /*
	     * Now that we have the info we need from the mcell
	     */
	    status = free_mcell(domain, currVol,
				pCurrentPage, currMCId.cell);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = add_fixed_message(&(pCurrentPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_423, 
					       "Deleted mcell (%d,%ld,%d), has no records in it.\n"), 
				       currVol,
				       currMCId.page, 
				       currMCId.cell);
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = TRUE;
	}	

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrentPage, modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	if (modified == TRUE) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}

	/*
	 * Do we have a nextMCId which we need to check.
	 */
	if (nextMCId.volume) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    if (byteCount) {
		prevMCId = currMCId;
	    } else {
		/*
		 * This mcell was deleted so DON'T reset prevMCId.
		 */
	    }

	    currMCId = nextMCId;
	} else {
	    ZERO_MCID(currMCId);
	}/* end if nextMCId exists */
    } /* end while loop of the primary and next chain */

    /*
     * If the chain exists, then we MUST have an XTRA record.  If we don't
     * have what we need then we need to truncate the chain.  As we don't
     * want to collect the extents if we will be clearing the chain this
     * means we need to walk the chain twice.
     */
    currMCId = chainMCId;
    foundXtra = FALSE;
	
    /*
     * Loop until chainMCId is empty [0,0,0].
     */
    while (currMCId.volume != 0) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, &currLbn, 
				     &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCId.cell]);
	
	/*
	 * Now loop through records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	recNum = 0;

	while ((BSR_NIL != pRecord->type) &&
	       (pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * Init variables
	     */
	    reprocessRecord = FALSE;
	    recNum++;

	    /*
	     * We need to see if this chain has an Xtra extent.
	     */
	    if (pRecord->type == BSR_XTRA_XTNTS) {
		foundXtra = TRUE;
	    } else {
		/*
		 * No other type should be in chain, if they are 
		 * records then they will be deleted when we collect
		 * the extents
		 */
	    }

	    if (reprocessRecord == FALSE) {
		pRecord = NEXT_MREC(pRecord);

	    } else {
		/*
		 * Reset record number
		 */
		recNum--;
	    }
	} /* end record loop */

	/*
	 * Set current to the next mcell.
	 */
	currMCId = pMcell->mcNextMCId;

	/*
	 * Release the Page.
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrentPage, FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
    } /* end chain while loop */

    /*
     * Now if the chainMCId exists we need to verify that we have an
     * Xtra extent rec.  If not free the chainMCId.
     */
    if (chainMCId.volume != 0 && foundXtra == FALSE) {
	status = correct_empty_chain(fileset, pTagNode);
	if (status != SUCCESS) {
	    if (status != FIXED) {
		return status;
	    } else {
		bmtModified = TRUE;
	    }
	}

	/*
	 * Finally set the local chain variable to 0,0,0
	 */
	ZERO_MCID(chainMCId);
    }

    /*
     * Now we need to re-follow chainMCId which contains the extents.
     */
    currMCId = chainMCId;
    prevMCId = currMCId;

    /*
     * Loop until chainMCId is empty [0,0,0]
     */
    while (currMCId.volume != 0) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, &currLbn, 
				     &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCId.cell]);
	
	nextMCId = pMcell->mcNextMCId;

	/*
	 * Check if the nextMCId exists (via skip list).
	 */
	if (nextMCId.volume != 0) {
	    tmpMcellNode.mcid = nextMCId;
	    pMcellNode = &tmpMcellNode;
		
	    status = find_node(domain->mcellList, (void **)&pMcellNode, 
			       &tmpNode);
	    if (status != SUCCESS) {
		if (NOT_FOUND == status) {
		    status = add_fixed_message(&(pCurrentPage->messages),
					       BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_425,
						       "Modified mcell (%d,%ld,%d)'s next pointer from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					       currMCId.volume,
					       currMCId.page,
					       currMCId.cell, 
					       nextMCId.volume,
					       nextMCId.page, 
					       nextMCId.cell,
					       0, 0, 0);
		    if (status != SUCCESS) {
			return status;
		    }

		    ZERO_MCID(pMcell->mcNextMCId);
		    /*
		     * End the next mcell chain.
		     */
		    ZERO_MCID(nextMCId);
		    nextTagNum = 0;
		    modified = TRUE;
		} else {
		    return status;
		} /* end if/else NOT_FOUND */
	    } else {
		nextTagNum = pMcellNode->tagSeq.tag_num;
	    } /* end if SUCCESS */
	} else {
	    /*
	     * End of next mcell chain.
	     */
	    ZERO_MCID(nextMCId);
	    nextTagNum = 0;
	} /* end if nextMCId */
	    
	/*
	 * Check if this mcell has the correct tag and set number.
	 */
	status = correct_chain_tag_and_set(fileset, currMCId, 
					   prevMCId, primTagNum, 
					   primSeqNum, nextTagNum, 
					   pCurrentPage);
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
	     * Release page to stop deadlocks.
	     */
	    status = release_page_to_cache(currVol, currLbn, 
					   pCurrentPage, TRUE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    if (modified == TRUE) {
		bmtModified = TRUE;
		modified = FALSE;
	    }

	    /*
	     * Is this mcell a primary mcell for a different tag.
	     */
	    tmpMcellNode.mcid = currMCId;
	    pMcellNode = &tmpMcellNode;

	    status = find_node(domain->mcellList, (void **)&pMcellNode, 
			       &tmpNode);
	    if (status != SUCCESS) {
		return status;
	    }

	    if (MC_IS_PRIMARY(pMcellNode->status)) {
		/*
		 * Now we know its a primary mcell, now we need to check
		 * if the tag that this mcell believes it belongs to is
		 * free.
		 */
		tmpTagNode.tagSeq.tag_num = currentTagNum;
		pTempTagNode = &tmpTagNode;

		/*
		 * Find the tag which this mcell says it belongs to.
		 */
		status = find_node(fileset->tagList, 
				   (void **)&pTempTagNode, &tmpNode);
		if (status == SUCCESS) {
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
						  currentTagNum,
						  currentSeqNum, 
						  currMCId);
		    if (status != SUCCESS) {
			if (NO_MEMORY == status) {
			    return status;
			} else {
			    writemsg(SV_DEBUG,
				     catgets(_m_catd, S_FSCK_1, FSCK_408, 
					     "Duplicate tag %d - different seq - Setting to missing.\n"),
				     currentTagNum);
			    MC_SET_MISSING_TAG(pMcellNode->status);
			}
		    } else {
			MC_SET_FOUND_PTR(pMcellNode->status); 

			/* 
			 * correct_tag_to_mcell created the tag node 
			 */
			tmpTagNode.tagSeq.tag_num = currentTagNum;
			pTempTagNode = &tmpTagNode;

			status = find_node(fileset->tagList, 
					   (void **)&pTempTagNode, 
					   &tmpNode);
			if (status != SUCCESS) {
			    return status;
			}

			/*
			 * We have attached the mcell chain to a tag.
			 * The tag node could have already been passed
			 * by in the tag loop so we need to process
			 * the chain again.  It is possible this tag chain
			 * will be processed twice.
			 */
			status = correct_bmt_chain_by_tag(fileset, 
							  pTempTagNode,
							  sizeRecordArray, 
							  recordArray,
							  sizeXtntArray, 
							  xtntArray);
			if (status != SUCCESS) {
			    if (status == FIXED) {
				bmtModified = TRUE;
			    } else {
				return status;
			    }
			}
		    } /* end if correct_tag_to_mcell */
		} /* end NOT_FOUND */
	    } /* end IS PRIMARY */
	    break; /* The chain was truncated we can break out of loop */
	} else if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Now loop through records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	recNum = 0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    bsXtraXtntRT  *pXtraXtnt;

	    /*
	     * Init variables
	     */	
	    reprocessRecord = FALSE;
	    recNum++;

	    /*
	     * Check to see if this record could exceed the
	     * record Array.
	     */
	    if (whichRecord + 1 > *sizeRecordArray) {
		/*	
		 * Double the size of the record Array.
		 */
		*sizeRecordArray = *sizeRecordArray * 2;

		*recordArray = (int *)ffd_realloc(*recordArray,
						 (*sizeRecordArray + 1) *
						 sizeof(int));
		if (NULL == *recordArray) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_10,
				     "Can't allocate memory for %s.\n"),
			     catgets(_m_catd, S_FSCK_1, FSCK_416,
				     "record array"));
		    return NO_MEMORY;
		}

		/*
		 * Clear the newly added section.
		 */
		bzero(&((*recordArray)[whichRecord]),
		      ((*sizeRecordArray - whichRecord) * sizeof(int)));
	    } /* if need to double record Array */

	    /*
	     * check to see if this record could exceed the extent list.
	     */
	    if (lastXtntEntry +  BMT_XTRA_XTNTS > *sizeXtntArray) {
		/*	
		 * Double the size of the extent list.
		 */
		*sizeXtntArray = *sizeXtntArray * 2;

		*xtntArray = (fobtoLBNT *)ffd_realloc(*xtntArray,
						      (*sizeXtntArray + 1) *
						      sizeof(fobtoLBNT));
		if (NULL == *xtntArray) {
		    writemsg(SV_ERR | SV_LOG_ERR, 
			     catgets(_m_catd, S_FSCK_1, FSCK_10,
				     "Can't allocate memory for %s.\n"),
			     catgets(_m_catd, S_FSCK_1, FSCK_417,
				     "extent array"));
		    return NO_MEMORY;
		}

		/*
		 * Clear the newly added section.
		 */
		bzero(&((*xtntArray)[lastXtntEntry]),
		      ((*sizeXtntArray - lastXtntEntry) * sizeof(fobtoLBNT)));

	    } /* End expand xtntArray if needed */

	    /*
	     * Multiple checks are done here, the main one is that
	     * we need to collect the extents to see if we have 
	     * overlaps between different mcell records.
	     * Also delete record that should not be here.
	     */
	    if (pRecord->type == BSR_XTRA_XTNTS) {
		pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + 
					     sizeof(bsMRT));

		(*recordArray)[whichRecord] = lastXtntEntry;
		whichRecord++;
		(*recordArray)[whichRecord] = -1;

		/*	
		 * Loop on all extents
		 */
		for (extent = 0; 
		     extent < pXtraXtnt->xCnt && extent < BMT_XTRA_XTNTS;
		     extent++) {
		    (*xtntArray)[lastXtntEntry].volume = currMCId.volume;
		    (*xtntArray)[lastXtntEntry].fob = 
		                     pXtraXtnt->bsXA[extent].bsx_fob_offset;
		    (*xtntArray)[lastXtntEntry].lbn = 
			             pXtraXtnt->bsXA[extent].bsx_vd_blk;
		    lastXtntEntry++;
		} /* end extent for loop */
		break;
	    } else {

		/*
		 * No other type should be in chain.
		 */
		status = add_fixed_message(&(pCurrentPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_427,
						   "Modified chain mcell (%d,%ld,%d), deleted invalid record type %d.\n"),
					   currMCId.volume, 
					   currMCId.page, 
					   currMCId.cell, 
					   pRecord->type);
		if (status != SUCCESS) {
		    return status;
		}

		status = clear_mcell_record(domain,
					    pCurrentPage, 
					    currMCId.cell, 
					    recNum);
		if (status != SUCCESS) {
		    return status;
		}
		reprocessRecord = TRUE;
		modified = TRUE;
		break;
	    }

	    if (reprocessRecord == FALSE) {
		pRecord = NEXT_MREC(pRecord);
	    } else {
		/*
		 * Reset record number
		 */
		recNum--;
	    }
	} /* end record loop */

	/*
	 * This could be a mcell with no record, or we made it
	 * this way with the clear_mcell_record call.  We will
	 * base this off of its bCnt, we need to save this info
	 * to make sure we reset prevmcell correctly
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	byteCount = pRecord->bCnt;

	if (byteCount == 0) {
	    /*
	     * This mcell has no records.
	     * If it has a next chain, reset prevMCId to point to it.
	     * If it doesn't have a next chain the truncate prevMCId.
	     */
	    if (MCIDS_EQ(currMCId, chainMCId)) {
		/*
		 * Need to modify BSR_XTNT chain chain
		 */
		if ((currMCId.page != xtntMCId.page) ||
		    (currMCId.volume != xtntMCId.volume)) {

		    volume = &(domain->volumes[xtntMCId.volume]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 xtntMCId.page,
						 &prevLbn, &prevVol);
		    if (status != SUCCESS) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FSCK_1, FSCK_128,
					 "Can't convert BMT page %ld to LBN.\n"), 
				 xtntMCId.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, 
						  &pPrevPage);
		    if (status != SUCCESS) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrentPage;
		} /* end if on different pages */

		pPrevMcell = &(bmtPrevPage->bsMCA[xtntMCId.cell]);
	    
		/*
		 * Now loop through primary chain's records.
		 */
		pRecord = (bsMRT *)pPrevMcell->bsMR0;

		while ((pRecord->type != BSR_NIL) &&
		       (pRecord->bCnt != 0) &&
		       (pRecord < ((bsMRT *) &(pPrevMcell->bsMR0[BSC_R_SZ])))) {

		    bsXtntRT  *pXtnt;

		    if (pRecord->type == BSR_XTNTS) {
			pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
			/*
			 * Modify the chainMCId 
			 */
			status = add_fixed_message(&(pCurrentPage->messages), 
						   BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_409,
							   "Modified mcell (%d,%ld,%d) xtnt chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
						   xtntMCId.volume, 
						   xtntMCId.page,
						   xtntMCId.cell, 
						   pXtnt->chainMCId.volume,
						   pXtnt->chainMCId.page,
						   pXtnt->chainMCId.cell,
						   nextMCId.volume,
						   nextMCId.page,
						   nextMCId.cell);
			if (status != SUCCESS) {
			    return status;
			}
			
			pXtnt->chainMCId = nextMCId;
		    }

		    pRecord = NEXT_MREC(pRecord);

		} /* end record loop */

		if ((currMCId.page != xtntMCId.page) ||
		    (currMCId.volume != xtntMCId.volume)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (status != SUCCESS) {
			return status;
		    }    
		} else {
		    bmtPrevPage = NULL;
		    modified = TRUE;
		} /* end if on same page or not */
	    } else {
		if ((currMCId.page != prevMCId.page) ||
		    (currMCId.volume != prevMCId.volume)) {
		    volume = &(domain->volumes[prevMCId.volume]);
		    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
						 volume->volRbmt->bmtLBNSize, 
						 prevMCId.page,
						 &prevLbn, &prevVol);
		    if (status != SUCCESS) {
			writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FSCK_1, FSCK_128,
					 "Can't convert BMT page %ld to LBN.\n"), 
				 prevMCId.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(prevVol, prevLbn, 
						  &pPrevPage);
		    if (status != SUCCESS) {
			return status;
		    } 
		
		    bmtPrevPage = (bsMPgT *)pPrevPage->pageBuf;
		} else {
		    /*
		     * Both mcells on same page.
		     */
		    bmtPrevPage = bmtPage;
		    pPrevPage = pCurrentPage;
		}

		pPrevMcell = &(bmtPrevPage->bsMCA[prevMCId.cell]);

		status = add_fixed_message(&(pPrevPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_422,
						   "Modified mcell (%d,%ld,%d)'s next chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					   prevMCId.volume,
					   prevMCId.page,
					   prevMCId.cell, 
					   pPrevMcell->mcNextMCId.volume,
					   pPrevMcell->mcNextMCId.page,
					   pPrevMcell->mcNextMCId.cell,
					   nextMCId.volume,
					   nextMCId.page,
					   nextMCId.cell);
		if (status != SUCCESS) {
		    return status;
		}

		pPrevMcell->mcNextMCId = nextMCId;	    

		if ((currMCId.page != prevMCId.page) ||
		    (currMCId.volume != prevMCId.volume)) {
		    status = release_page_to_cache(prevVol, prevLbn, 
						   pPrevPage, TRUE, FALSE);
		    if (status != SUCCESS) {
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

	    ZERO_MCID(pMcell->mcNextMCId);

	    /*
	     * Now that we have the info we need from the mcell
	     */
	    status = free_mcell(domain, currVol,
				pCurrentPage, currMCId.cell);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = add_fixed_message(&(pCurrentPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_423,
					       "Deleted mcell (%d,%ld,%d), has no records in it.\n"), 
				       currVol,
				       currMCId.page, 
				       currMCId.cell);
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = TRUE;
	} /* end if bytecount is 0 */

	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	if (modified == TRUE) {
	    bmtModified = TRUE;
	    modified = FALSE;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMCId.volume != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    if (byteCount != 0) {
		prevMCId = currMCId;
	    } else {
		/*
		 * This mcell was deleted so DON'T reset prevMCId, unless
 		 * this mcell was the start of the chain, in which case
 		 * we need to reset chainMcell as well.
 		 */
 		if (MCIDS_EQ(currMCId, chainMCId))
 		{
 		    chainMCId = nextMCId;
 		    prevMCId = nextMCId;
 		}
	    }
	    currMCId = nextMCId;
	} else {
	    ZERO_MCID(currMCId);
	}/* end if nextMCId exists */
    } /* end chain while loop */

    /*
     * start the check of extents
     */
    status = correct_extent_array(lastXtntEntry, *recordArray, *xtntArray);
    if (status == FIXED) {
	/*
	 * Array has been changed, make sure it goes out to disk.
	 */
	status = correct_chain_extents(fileset, primMCId, *xtntArray);
	if (status != SUCCESS) {
	    return status;
	}
	bmtModified = TRUE;
    } else if (status != SUCCESS) {
	return status;
    } 

    /*
     * Check the xtnt chains mcell count.
     */
    status = correct_chain_mcell_count(fileset, chainMCId, xtntMCId,
				       whichRecord);
    if (status != SUCCESS) {
	if (FIXED == status) {
	    bmtModified = TRUE;
	} else {
	    return status;
	}
    }

    /*
     * If an ACL exists check and fix it.
     */
    if (foundAcl == TRUE) {
	status = correct_chain_ACL(fileset, primMCId, currentTagNum);
	if (status == FIXED) {
	    bmtModified = TRUE;
	} else if (status != SUCCESS) {
	    return status;
	} 
    } /* end if foundAcl */

    /*
     * Handle the case where tag does not have required records.
     *
     * WARNING : Do not do anything after these checks which
     * will require the tag node, as it may not exist.
     */
    if ((foundBsrXtnts == FALSE) && (foundFsData == FALSE)) {
	/*
	 * From testing the only file type I could find that doesn't
	 * have an xtnt record is the symbloic links.
	 */
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_430,
			 "Deleting '%s' file system tag %d, missing BSR_XTNT record.\n"),
		 fileset->fsName, currentTagNum);

	status = delete_tag(fileset, currentTagNum);
	if (status != SUCCESS) {
	    return status;
	}
	bmtModified = TRUE;
    } else if (foundFsStat == FALSE) {
	/*
	 * This call may delete the tag node.  
	 */
	status = correct_chain_fsstat(fileset, primMCId, 
				      currentTagNum);
	if (status != SUCCESS) {
	    return status;
	}
	bmtModified = TRUE;
    } /* end if no fsstat record */

    if (bmtModified == TRUE) {
	return FIXED;
    }	
    return SUCCESS;
} /* end correct_bmt_chain_by_tag */


/*
 * Function Name: correct_chain_mcell_count
 *
 * Description: Each extent chain has a count of the number of mcells
 *              in it.  This checks and corrects the count.
 *
 *             we can use the 'whichRecord' variable as the count.
 * 
 * NOTE: the count is stored as a short, not an int.
 *
 * Input parameters:
 *     fileset: The fileset to be checked.
 *     chainMCId: This tag's chainMCId.
 *     xtntMCId: This tag's xtntMCId.
 *     whichRecord: Used for normal files to know how many mcells.
 *     whichStripe: Used to know if the file is Striped.
 *
 * Return Values: SUCCESS, FIXED, FAILURE, NO_MEMORY
 */
int 
correct_chain_mcell_count(filesetT *fileset, 
			  mcidT chainMCId, 
			  mcidT xtntMCId, 
			  int whichRecord)
{
    char *funcName = "correct_chain_mcell_count";
    int  status;
    int  modified;
    int  bmtModified;
    volNumT currVol;
    lbnNumT currLbn;
    int32_t mcellCnt;
    mcidT   currMCId;
    domainT *domain;
    volumeT *volume;
    pageT   *pCurrentPage;
    bsMPgT  *bmtPage;
    bsMCT   *pMcell;
    bsMRT   *pRecord;
    bsXtntRT *pXtnt;

    /*
     * Init variables
     */
    domain = fileset->domain;
    modified = FALSE;
    bmtModified = FALSE;
    mcellCnt = (uint32_t) whichRecord;

    /*
     * The mcell we want is located in xtntMCId.
     */
    currMCId = xtntMCId;

    /*
     * Read the current page the mcell is located on.
     */
    volume = &(domain->volumes[currMCId.volume]);
    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				 volume->volRbmt->bmtLBNSize, 
				 currMCId.page, &currLbn, 
				 &currVol);
    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_128,
			 "Can't convert BMT page %ld to LBN.\n"), 
		 currMCId.page);
	return FAILURE;
    }

    status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
    if (status != SUCCESS) {
	return status;
    } 

    bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
    pMcell  = &(bmtPage->bsMCA[currMCId.cell]);
    pRecord = (bsMRT *)pMcell->bsMR0;
	
    while ((BSR_NIL != pRecord->type) &&
	   (pRecord->bCnt) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	if (BSR_XTNTS == pRecord->type) {
	    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

	    if (pXtnt->mcellCnt != mcellCnt) {
		status = add_fixed_message(&(pCurrentPage->messages), 
					   BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_431, 
						   "Modified mcell (%d,%ld,%d)'s xtnt mcell count from %d to %d.\n"),
					   currMCId.volume, 
					   currMCId.page,
					   currMCId.cell, 
					   pXtnt->mcellCnt,
					   mcellCnt);
		if (status != SUCCESS) {
		    return status;
		}

		pXtnt->mcellCnt = mcellCnt;
		modified = TRUE;
	    }
	}

	pRecord = NEXT_MREC(pRecord);

    } /* end record loop */

    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				   modified, FALSE);
    if (status != SUCCESS) {
	return status;
    }
    if (modified == TRUE) {
	bmtModified = TRUE;
	modified = FALSE;
    }

    if (bmtModified == TRUE) {
	return FIXED;
    }
    
    return SUCCESS;
} /* end correct_chain_mcell_count */


/*
 * Function Name: correct_empty_chain
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
int 
correct_empty_chain(filesetT *fileset, 
		    tagNodeT *pTagNode)
{
    char *funcName = "correct_empty_chain";
    int  status;
    int  modified;
    lbnNumT currLbn;
    volNumT currVol;
    mcidT  currMCId;
    mcidT  primMCId;
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
    primMCId = pTagNode->tagMCId;
    currMCId = primMCId;

    /*
     * Loop until currMCId is empty [0,0,0]
     */
    while (currMCId.volume != 0) {
	/*
	 * Read the mcell page (first get LBN).
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, 
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCId.cell]);
	    
	/*
	 * Now loop through primary chain's records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((BSR_NIL != pRecord->type) &&
	       (pRecord->bCnt) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    if (BSR_XTNTS == pRecord->type) {
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		if (pXtnt->chainMCId.volume == 0) {
		    /*
		     * Chain already empty
		     */
		    break;
		} else {
		    /*
		     * We are going to truncate the chain chain, 	
		     * we need to mark the chain as MISSING.
		     */
		    tmpMcellNode.mcid = pXtnt->chainMCId;
		    pMcellNode = &tmpMcellNode;
		
		    status = find_node(domain->mcellList, 
				       (void **)&pMcellNode, &node);
		    if (status != SUCCESS) {
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
		     * Truncate the chainMCId 
		     */
		    status = add_fixed_message(&(pCurrentPage->messages), 
					       BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_409,
						       "Modified mcell (%d,%ld,%d) xtnt chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					       currMCId.volume, 
					       currMCId.page,
					       currMCId.cell, 
					       pXtnt->chainMCId.volume,
					       pXtnt->chainMCId.page,
					       pXtnt->chainMCId.cell,
					       0, 0, 0);
		    if (status != SUCCESS) {
			return status;
		    }	
		    
		    ZERO_MCID(pXtnt->chainMCId);
		    pXtnt->mcellCnt = 1;
		    modified = TRUE;
		    break;
		}
	    }
	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	/*
	 * Set current to next mcell
	 */
	currMCId = pMcell->mcNextMCId;

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, 
				       pCurrentPage, TRUE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
    } /* end while loop of the primary chain */
    if (modified == TRUE) {
	return FIXED;
    }
    return SUCCESS;
} /* end correct_empty_chain */


/*
 * Function Name: correct_quota_files
 *
 * Description: This routine checks the user and group quota files for
 *		the specified fileset. The only known corruption in
 *		the user/group quota files that can prevent mounting
 *		of a fileset involves the quota file size. For other
 *		corruptions in the user/group quota files, the user
 *		needs to run quotacheck instead of fsck.
 *
 * Input parameters:
 *     fileset: The fileset which needs its quota files checked.
 *
 * Output parameters:
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY
 * 
 */
int 
correct_quota_files(filesetT *fileset)
{
    char	*funcName = "correct_quota_files";

    domainT	*domain;
    tagNumT	tagnum;
    int		status;
    tagNodeT	tmpTagNode;
    tagNodeT	*tagNode;
    nodeT	*currNode;
    fobtoLBNT	*extents;
    xtntNumT	extentSize;
    pageNumT	lastpage;
    char        quotaType[10];

    domain = fileset->domain;

    /*
     * Don't bother with snapshot filesets.
     */
    if (FS_IS_SNAP(fileset->status)) {
	/*
	 * FUTURE: Figure out what should be done here.
	 */
	return SUCCESS;
    }

    for (tagnum = 4 ;	/* User Quota File  */
	 tagnum <= 5 ;	/* Group Quota File */
	 tagnum++)
    {
	if (tagnum == 4) {
	    strcpy(quotaType, "user");
	} else {
	    strcpy(quotaType, "group");
	}

	tmpTagNode.tagSeq.tag_num = tagnum;
	tagNode = &tmpTagNode;
	status = find_node(fileset->tagList, (void **)&tagNode, &currNode);

	if (status == NOT_FOUND) {
	    /*
	     * Didn't find correct tagNode - Go on to next one.
	     */
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_436, 
			     "No %s quota file found in '%s' file system.\n"),
		     quotaType, fileset->fsName);
	    /*
	     * FUTURE: create missing quota file.
	     */
	    continue;
	} else if (status != SUCCESS) {
	    return status;
	}

	status = collect_extents(domain, fileset, &(tagNode->tagMCId), FALSE,
				 &extents, &extentSize, FALSE);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_437, 
			     "Could not collect extents for %s quota file in '%s' file system.\n"), 
		     quotaType, fileset->fsName);
	    /*
	     * FUTURE: deal with corrupt primary mcell.
	     */
	}
	lastpage = FOB2PAGE(extents[extentSize].fob);
	free(extents);

	if (tagNode->size <= (lastpage - 1) * ADVFS_METADATA_PGSZ ||
	    tagNode->size > lastpage * ADVFS_METADATA_PGSZ)
	{
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_438,
			     "Modifying '%s' file system %s quota file size in FSSTAT record.\n"),
		     fileset->fsName, quotaType);
	    tagNode->size = lastpage * ADVFS_METADATA_PGSZ;
	    status = correct_fsstat(fileset, tagNode);
	    if (status != SUCCESS) {
		return status;
	    }
	}
    } /* end loop on user and group quota files */
    return SUCCESS;
} /* end correct_quota_files */


/*
 * Function Name: correct_fsstat
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
int 
correct_fsstat(filesetT *fileset, 
	       tagNodeT *tagNode)
{
    char	*funcName = "correct_fsstat";
    domainT	*domain;
    mcidT	*primMCId;
    mcidT	currMCId;
    volumeT	*volume;
    lbnNumT	currLbn;
    volNumT	currVol;
    pageT	*pPage;
    mcidT	nextMCId;
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
    primMCId = &(tagNode->tagMCId);
    currMCId = *primMCId;
    
    assert(T_IS_SNAP_ORIG_PMCELL(tagNode->status) == FALSE);

    /*
     * Loop until currMCId is empty [0,0,0]
     */
    while (currMCId.volume != 0) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, &currLbn, 
				     &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol, currLbn, &pPage);
	if (status != SUCCESS) {
	    return status;
	} 
	
	modified = FALSE;
	bmtPage = (bsMPgT *)pPage->pageBuf;
	pMcell = &(bmtPage->bsMCA[currMCId.cell]);
	
	nextMCId = pMcell->mcNextMCId;
	
	pRecord = (bsMRT *)pMcell->bsMR0;
	
	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    
	    if (pRecord->type == BMTR_FS_STAT) {
		pStat = (statT *)((char *)pRecord + sizeof(bsMRT));

		/*
		 * Copy the known values into aligned stat.
		 */
		memcpy(&alignedStat, (char *)pStat,  sizeof(statT));

		alignedStat.st_size        = tagNode->size;
		alignedStat.st_mode        = tagNode->fileType;
		alignedStat.st_nlink       = tagNode->numLinks;
		alignedStat.dir_tag.tag_num    = tagNode->parentTagNum.tag_num;
		alignedStat.dir_tag.tag_seq    = tagNode->parentTagNum.tag_seq;

		/*
		 * Need to align this struct or we get unaligned
		 * error messages.
		 */
		memcpy((char *)pStat, &alignedStat, sizeof(statT));
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_439,
						   "Modified mcell (%d,%ld,%d)'s FSSTAT record.\n"),
					   currMCId.volume,
					   currMCId.page,
					   currMCId.cell);
		if (status != SUCCESS) {
		    return status;
		}

		modified = TRUE;

		/*
		 * Found record - can break out of record loop.
		 */
		break;
	    }			
	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */
	
	status = release_page_to_cache(currVol, currLbn, pPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
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
	if (nextMCId.volume != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currMCId = nextMCId;
	} else {
	    ZERO_MCID(currMCId);
	}/* end if nextMCId exists */
    }/* end chain loop */

    /*
     * Couldn't find an FSSTAT record for this tag - this should be
     * impossible as correct_bmt_chains will either create an FSSTAT
     * record for every file, or delete the tag.
     */
    writemsg(SV_ERR | SV_LOG_ERR, 
	     catgets(_m_catd, S_FSCK_1, FSCK_440, 
		     "Couldn't correct FSSTAT record for file (%ld.%u).\n"),
	     tagNode->tagSeq.tag_num, tagNode->tagSeq.tag_seq);
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
int 
validate_fileset_name(char *fsName)
{
    char *funcName = "validate_fileset_name";
    char tmpName[BS_SET_NAME_SZ + 2];
    char *cp;

    strncpy(tmpName, fsName, BS_SET_NAME_SZ + 1);
    tmpName[BS_SET_NAME_SZ + 1] = '\0';

    if (strlen(tmpName) == 0) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FSCK_1, FSCK_441, 
			 "Zero length file system name.\n"));
	return FAILURE;
    }

    if (strlen(tmpName) >= BS_SET_NAME_SZ) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_442, 
			 "File system name length too long.\n"));
	return FAILURE;
    }

    if (cp = strpbrk(tmpName, "/# :*?\t\n\f\r\v")) { /* Copied from mkfset.c */
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_443, 
			 "File system name contains illegal character(s).\n"));
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
int 
delete_tag(filesetT *fileset, 
	   tagNumT tagNum)
{
    char *funcName = "delete_tag";
    int	       status;
    int        tmpTagsPerPage;
    int	       tagEntryOnPage; /* Which entry on the tag page */
    pageNumT   tagPageNum;
    lbnNumT    tagPageLbn;
    volNumT    tagPageVol;
    lbnNumT    currLbn;
    volNumT    currVol;
    domainT    *domain;
    volumeT    *volume;
    mcidT      primMCId;
    mcidT      currMCId;
    mcidT      nextMCId;
    mcidT      chainMCId;
    fobtoLBNT  *pExtents;      /* The extents to check */
    xtntNumT   numExtents;
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
    tagSeqT    seqNum;	 /* The sequence number of the tag being deleted */
    mcellNodeT *pMcellNode;
    mcellNodeT mcellNode;
    nodeT      *node;

    /*
     * Initialize variables.
     */
    domain     = fileset->domain;
    pExtents   = fileset->tagLBN;
    numExtents = fileset->tagLBNSize;

    ZERO_MCID(chainMCId);

    if (tagNum <= 5) {
	writemsg(SV_DEBUG | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_444,
			 "Not deleting '%s' tag %d from tag file because tag is a metadata file.\n"),
		 fileset->fsName, tagNum);
	return SUCCESS;
    }

    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
	     catgets(_m_catd, S_FSCK_1, FSCK_445, 
		     "Deleting '%s' file system tag %d from tag file.\n"), 
	     fileset->fsName, tagNum);

    /*
     * Find the page in the tagfile where our tag's entry would be.
     */
    tagPageNum = tagNum / BS_TD_TAGS_PG;
    tagEntryOnPage = tagNum % BS_TD_TAGS_PG;
 
    status = convert_page_to_lbn(pExtents, numExtents, tagPageNum, 
				 &tagPageLbn, &tagPageVol);
    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_381,
			 "Can't convert tag page %ld to LBN.\n"), 
		 tagPageNum);
	return FAILURE;
    }
	
    status = read_page_from_cache(tagPageVol, tagPageLbn, &pCurrentPage);
    if (status != SUCCESS) {
	return status;
    } 

    /*
     * Now we have the correct tag page.  Find the correct tag
     * and save the primary mcell.
     */
    pTagPage = (bsTDirPgT *)pCurrentPage->pageBuf;
    tagEntry = &(pTagPage->tMapA[tagEntryOnPage]);
    seqNum   = tagEntry->tmSeqNo;  /* Save for later in function */
    
    primMCId = tagEntry->tmBfMCId;

    /*
     * Now that we have the primary mcell, if this is a snapshot file
     * and the tag points to the original file's mcell, DON'T delete
     * that mcell chain.  Instead, we just zero the reference to it
     * here, which will cause us to skip the loop below that actually
     * deletes the mcell chain, but allows us to delete the tag.
     */
    if (FS_IS_SNAP(fileset->status)) {
	tagNode.tagSeq.tag_num = tagNum;
	pTagNode = &tagNode;

	status = find_node(fileset->tagList, (void **)&pTagNode, &node);
	if (status == SUCCESS) {
	    if (T_IS_SNAP_ORIG_PMCELL(pTagNode->status)) {
		ZERO_MCID(primMCId);
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
    tagEntry->tmFlags &= ~BS_TD_IN_USE;
    tagEntry->tmBfMCId.volume = 0;
    tagEntry->tmNextMap = pTagPage->tpNextFreeMap;
    pTagPage->tpNextFreeMap = tagEntryOnPage + 1; /* WARNING: Off by 1 */

    /*
     * Have to deal with page 0 (specifically tag 0) special case
     * being used but not counted as allocated.
     */
    if (tagPageNum == 0) {
	tmpTagsPerPage = BS_TD_TAGS_PG - 1;
    } else {
	tmpTagsPerPage = BS_TD_TAGS_PG;
    }

    /*
     * Does the page need to be added to the free tag page list.
     */
    if (tmpTagsPerPage == pTagPage->tpNumAllocTMaps) {
	volNumT headVol;
	lbnNumT headLbn;

	/*
	 * This was a full page, which now has a free element.
	 */
	if (tagPageNum != 0) {
	    /*
	     * This page is NOT the same page as the head of the list.
	     */
	    status = convert_page_to_lbn(pExtents, numExtents, 0, 
					 &headLbn, &headVol);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = read_page_from_cache(headVol, headLbn,
					  &pHeadPage);
	    if (status != SUCCESS) {
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
		 catgets(_m_catd, S_FSCK_1, FSCK_386, 
			 "Adding '%s' file system tag page %ld to free list.\n"),
		 fileset->fsName, tagPageNum);
	status = add_fixed_message(&(pCurrentPage->messages), tagFile,
				   catgets(_m_catd, S_FSCK_1, FSCK_387, 
					   "Modified '%s' file system tag page, added page to free list.\n"),
				   fileset->fsName);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Release the page if needed.
	 */
	if (tagPageNum != 0) {
	    status = release_page_to_cache(headVol, headLbn, pHeadPage, 
					   TRUE, FALSE);
	    if (status != SUCCESS) {
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
			       catgets(_m_catd, S_FSCK_1, FSCK_447,
				       "Deleted '%s' file system tag %ld.%u from tag file.\n"),
			       fileset->fsName, tagNum, seqNum);
    if (status != SUCCESS) {
	return status;
    }

    status = release_page_to_cache(tagPageVol, tagPageLbn, pCurrentPage, 
				   TRUE, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    /*
     * Now we need to follow the primary and remove all mcells on it.
     */
    currMCId = primMCId;
	
    /*
     * Loop until currMCId is empty [0,0,0]
     */
    while (currMCId.volume) {
	/*
	 * Read the current mcell's page (first get LBN).
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	pBmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	 = &(pBmtPage->bsMCA[currMCId.cell]);

	/*
	 * Save the next mcell for the next pass through the loop
	 */
	nextMCId = pMcell->mcNextMCId;

	/*
	 * Now loop through the mcell records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    bsXtntRT *pXtnt;

	    /*
	     * Only concerned with BSR_XTNTS to find the chain.
	     */
	    if (BSR_XTNTS == pRecord->type) {
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		/*
		 * Set next Chain to follow it, after we follow this 
		 * chain.
		 */
		chainMCId = pXtnt->chainMCId;
	    } /* end if BSR_XTNT */
	    
	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	/*
	 * Now that we have the info we need from the mcell
	 */
	status = free_mcell(domain, currVol, pCurrentPage, currMCId.cell);
	if (status != SUCCESS) {
	    return status;
	}

	status = add_fixed_message(&(pCurrentPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_448, 
					   "Deleted mcell (%d,%ld,%d), belongs to tag %ld.%u which is being deleted.\n"), 
				   currVol,
				   currMCId.page, 
				   currMCId.cell,
				   tagNum, seqNum);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       TRUE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMCId.volume == 0) {
	    if (chainMCId.volume != 0) {
		nextMCId = chainMCId;
	    } else {
		ZERO_MCID(nextMCId);
	    }
	    ZERO_MCID(chainMCId);
	}

	/*
	 * Verify that the next mcell exits.
	 */
	mcellNode.mcid = nextMCId;
	pMcellNode = &mcellNode;
	    
	status = find_node(domain->mcellList, (void **)&pMcellNode, &node);
	if (status != SUCCESS) {
	    ZERO_MCID(nextMCId);
	} 

	currMCId = nextMCId;

    } /* end following chain */

    /*
     * Remove this tag from in-memory skiplist
     */
    tagNode.tagSeq.tag_num = tagNum;
    pTagNode = &tagNode;

    status = delete_node(fileset->tagList, (void **)&pTagNode);
    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_449,
			 "Internal Error : deletion of node %ld.%u failed in delete_tag.\n"),
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
 *     primMCId: The information needed to locate the primary mcell.
 *     tagNum: The tag we are working on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY
 */
int 
correct_chain_fsstat(filesetT *fileset, 
		     mcidT primMCId, 
		     tagNumT tagNum)
{
    char    *funcName = "correct_chain_fsstat";
    int	    modified;
    int	    status;
    int	    bytesUsed;
    tagNumT currentTagNum;
    tagSeqT currentSeqNum;
    volNumT currVol;
    lbnNumT currLbn;
    mcidT  currMCId;     /* The mcell currently being worked on */
    mcidT  nextMCId;	      /* The next mcell on the next chain */
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
	     catgets(_m_catd, S_FSCK_1, FSCK_450,
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
    currMCId = primMCId;

    /*
     * Loop until currMCId is empty [0,0,0]
     */
    while (currMCId.volume != 0) {
	bytesUsed = 0;

	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, 
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage	   = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	   = &(bmtPage->bsMCA[currMCId.cell]);

	nextMCId = pMcell->mcNextMCId;
	currentTagNum  = pMcell->mcTag.tag_num;
	currentSeqNum  = pMcell->mcTag.tag_seq;

	/*
	 * Now loop through primary's next records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    bytesUsed += roundup(pRecord->bCnt,(sizeof(uint64_t)));
	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */
	
	/*
	 * At this point pRecord points to the end of the mcell.
	 */
	if ((BS_USABLE_MCELL_SPACE - bytesUsed) > sizeof(statT)) {
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
	    
	    /*
	     * memcpy the record header in.
	     */
	    memcpy((char *)pRecord, &mcellRecordHeader, sizeof(bsMRT));

	    /*
	     * We need the the tag node to get some of the info.
	     */
	    tmpTagNode.tagSeq.tag_num = currentTagNum;
	    pTagNode = &tmpTagNode;
		    
	    status = find_node(fileset->tagList, (void **)&pTagNode, &node);
	    if (status != SUCCESS) {
		return status;
	    }
	    
	    /*
	     * Now load the newFsStat structure.
	     */
	    newFsStat.st_ino.tag_num     = currentTagNum;
	    newFsStat.st_ino.tag_seq     = currentSeqNum;
	    newFsStat.st_mode        = S_IRWXG | S_IFREG; /* reg file*/
	    newFsStat.st_uid         = 0; /* root */
	    newFsStat.st_gid         = 0; /* system */
	    newFsStat.st_size        = pTagNode->size;
	    newFsStat.st_atime       = 0; /* Unknown access time */
	    newFsStat.st_natime      = 0; /* Unknown access time */
	    newFsStat.st_mtime       = 0; /* Unknown modify time */
	    newFsStat.st_nmtime      = 0; /* Unknown modify time */
	    newFsStat.st_ctime       = 0; /* Unknown creation time */
	    newFsStat.st_nctime      = 0; /* Unknown creation time */
	    newFsStat.st_flags       = 0; /* Unknown flag type */
	    newFsStat.dir_tag.tag_num    = 0; /* Unknown parent tag */
	    newFsStat.dir_tag.tag_seq    = 0; /* Unknown parent seq */
	    newFsStat.st_nlink       = 1; /* Most likely nlink */

	    /*
	     * Load up our in-memory tagnode information.
	     */
	    pTagNode->size = newFsStat.st_size;
	    pTagNode->fileType = newFsStat.st_mode;
	    pTagNode->numLinks = newFsStat.st_nlink;
	    pTagNode->parentTagNum.tag_num = newFsStat.dir_tag.tag_num;
	    pTagNode->parentTagNum.tag_seq = newFsStat.dir_tag.tag_seq;

	    /*
	     * Now that we have load the structure we need to memcpy this
	     * out to the page.
	     */
	    pStat = (statT *)((char *)pRecord + sizeof(bsMRT));	    

	    memcpy((char *)pStat, &newFsStat, sizeof(statT));

	    /*
	     * Now we need to add a trailing mcell record header.
	     */
	    pRecord = NEXT_MREC(pRecord);

	    mcellRecordHeader.bCnt = 0;
	    mcellRecordHeader.type = BSR_NIL;
	    
	    /*
	     * memcpy the record header in.
	     */
	    memcpy((char *)pRecord, &mcellRecordHeader, sizeof(bsMRT));

	    status = add_fixed_message(&(pCurrentPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_451, 
					       "Modified tag %ld.%u's mcell (%d,%ld,%d), added FSSTAT record.\n"),
				       currentTagNum, currentSeqNum,
				       currMCId.volume,
				       currMCId.page,
				       currMCId.cell);
	    if (status != SUCCESS) {
		return status;
	    }

	    modified = TRUE;
	}

	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	if (modified == TRUE) {
	    /*
	     * We have fixed the problem so we don't need to continue.
	     */
	    return SUCCESS;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMCId.volume != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currMCId = nextMCId;
	} else {
	    ZERO_MCID(currMCId);
	}/* end if nextMCId exists */
    } /* end chain loop */

    /*
     * We were not able to find space to add the fsstat record so 
     * we need to delete the tag.
     */
    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FSCK_1, FSCK_452, 
		     "Deleting '%s' file system tag %d, can't add an FSSTAT record.\n"), 
	     fileset->fsName, tagNum);
    status = delete_tag(fileset, tagNum);
    if (status != SUCCESS) {
	return status;
    }
    return SUCCESS;
} /* end correct_chain_fsstat */

/*
 * Function Name: correct_chain_ACL
 *
 * Description: If a tag has ACLs, check them.  Note that if any of the 
 *              ACL records are corrupt, *ALL* ACL info will be
 *              deleted from this tag and file ownership will revert
 *              to root.
 *
 * Input parameters:
 *     fileset: the fileset owning the file to be checked. 
 *     primaryMCId: the first mcell of a file to be ACL chain checked
 *     tagNum: The tag for the file to be checked.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, FAILURE
 */
static int 
correct_chain_ACL(filesetT *fileset, 
		  mcidT primaryMCId, 
		  tagNumT tagNum)
{
    char    *funcName = "correct_chain_ACL";
    int     i;
    int	    modified;
    int     delete;
    int	    status;
    volNumT currVol;
    lbnNumT currLbn;	 
    mcidT   currentMCId;     /* The mcell currently being worked on */
    mcidT   nextMCId;	      /* The next mcell on the next chain */
    volumeT *volume;
    pageT   *pCurrentPage;    /* The current page we are checking */
    bsMPgT  *bmtPage;	      /* The current BMT page we are working on */
    bsMCT   *pMcell;	      /* The current BMT mcell we are working on */
    bsMCT   *pPrevMcell;      /* the previous BMT mcell we are working on */
    bsMRT   *pRecord;
    domainT *domain;

    uint32_t          linkSeg;
    uint16_t          mcellAclCount;
    bsr_acl_rec_t     *ACL;
    bsr_acl_rec_hdr_t *header;
    advfs_acl_t       *entry;

    /*
     * Init variables.
     */
    domain     = fileset->domain;
    modified   = FALSE;
    delete     = FALSE;
    linkSeg    = 0;
    mcellAclCount = 0;

    /*
     * begin the search at the primary mcell
     */
    currentMCId = primaryMCId;

    /*
     * Loop through the current file's (tag's) mcell list looking for
     * ACL records to check.
     */

    while (currentMCId.volume) {
	volume = &(domain->volumes[currentMCId.volume]);

	/*
	 * Read the current mcell page (first get LBN).
	 */
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMCId.page, 
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currentMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currentMCId.cell]);

	nextMCId = pMcell->mcNextMCId;

	/*
	 * Now loop through the records in current mcell looking
	 * for an ACL record.
	 */

	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    /*
	     * We are only concerned with the ACL record.
	     */
	    if (pRecord->type != BSR_ACL_REC) {
		pRecord = NEXT_MREC(pRecord);
		continue;
	    }

	    /* found an ACL record, check it.  If we find an error, we
	       delete the record.  The ACL record consumes an entire
	       mcell so to delete the ACL record is to delete the
	       mcell, (add it to the page's mcell free list). */

	    ACL = (bsr_acl_rec_t *)MREC_TO_REC(pRecord);
	    header = &(ACL->bar_hdr);
	    entry = ACL->bar_acl;

	    if (mcellAclCount == 0) {
		/* set this once */
		mcellAclCount = header->bar_acl_total_cnt;
	    }

	    /* verify that all ACL records agree on the total ACL
	       count for the file */

	    if (header->bar_acl_total_cnt != mcellAclCount) {

		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_983,
				 "Tag %ld Mcell (%d,%ld,%d) has inconsistent ACL totals.\n"),
			 tagNum, currentMCId.volume, currentMCId.page,
			 currentMCId.cell);
		delete = TRUE;
		break;
	    }

	    /* range check the total ACL count for the file */

	    if (header->bar_acl_total_cnt < ADVFS_NUM_BASE_ENTRIES+1 ||
		header->bar_acl_total_cnt > ADVFS_MAX_ACL_ENTRIES) {

		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_981,
				 "Tag %ld Mcell (%d,%ld,%d) ACL count out of range.\n"), 
			 tagNum, currentMCId.volume, currentMCId.page,
			 currentMCId.cell);
		delete = TRUE;
		break;
	    }

	    /* range check the ACL count for this mcell only */

	    if (header->bar_acl_cnt == 0 ||
		header->bar_acl_cnt > ADVFS_ACLS_PER_MCELL) {

		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_982,
				 "Tag %ld Mcell (%d,%ld,%d) ACL record entry count out of range.\n"),
			 tagNum, currentMCId.volume, currentMCId.page,
			 currentMCId.cell);
		delete = TRUE;
		break;
	    }

	    /* verify the ACL type, which can only be ADVFS_ACL_TYPE */

	    if (header->bar_acl_type != ADVFS_ACL_TYPE) {

		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_986, 
				 "Fixing ACL type code in mcell (%d,%ld,%d).\n"),
			 currentMCId.volume,
			 currentMCId.page,
			 currentMCId.cell);
		status = add_fixed_message(&(pCurrentPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_980,
						   "Modified mcell (%d,%ld,%d) ACL type from %d to ADVFS_ACL_TYPE (%d).\n"),
					   currentMCId.volume,
					   currentMCId.page,
					   currentMCId.cell,
					   header->bar_acl_type,	
					   ADVFS_ACL_TYPE);

		if (status != SUCCESS) {
		    return(FAILURE);
		}
		header->bar_acl_type = ADVFS_ACL_TYPE;
		modified = TRUE;
	    }

	    /* verify the ACL link segment number.  This link segment
	       pertains to the ACL priovate links for the file's mcell
	       chain's ACL records, as distinct from the mcell link
	       segments found in the mcell headers and checked
	       elsewhere. */

	    if (header->bar_acl_link_seg != linkSeg) {

		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_987, 
				 "Fixing ACL link seg in mcell (%d,%ld,%d).\n"),
			 currentMCId.volume,
			 currentMCId.page,
			 currentMCId.cell);
		status = add_fixed_message(&(pCurrentPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_988,
						   "Modified tag %ld mcell (%d,%ld,%d) ACL link segment from %d to %d.\n"),
					   tagNum, 
					   currentMCId.volume,
					   currentMCId.page,
					   currentMCId.cell,
					   header->bar_acl_link_seg,
					   linkSeg);

		if (status != SUCCESS) {
		    return(FAILURE);
		}
		header->bar_acl_link_seg = linkSeg;
		modified = TRUE;
	    }

	    linkSeg++;

	    /* verify legal ACL entry types */

	    for (i = 0; i < header->bar_acl_cnt; i++) {

		if (!(entry[i].aa_a_type &= 
		      ADVFS_USER_OBJ |
		      ADVFS_USER |
		      ADVFS_GROUP_OBJ |
		      ADVFS_GROUP |
		      ADVFS_CLASS_OBJ |
		      ADVFS_OTHER_OBJ |
		      ADVFS_ACL_DEFAULT)) {

		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FSCK_1, FSCK_990,
				     "Tag %ld Mcell (%d,%ld,%d) ACL entry[%d] type 0x%x invaild\n"),
			     tagNum, currentMCId.volume, 
			     currentMCId.page,
			     currentMCId.cell, i, entry[i].aa_a_type);
		    delete = TRUE;
		    break;
		}
		
	    } /* end: scan the ACL individual entries */

	    pRecord = NEXT_MREC(pRecord);

	} /* end loop: scan the current mcell for ACLs */

	/* if we plan to delete the ACL, we're done here.  Release the
	   page as unmodified and break out of the loop to go do the
	   deletion. */

	if (delete) {
	    status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
					   FALSE, FALSE);
	    break;
	}

	/*
	 * Release Page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	modified = FALSE;

	if (nextMCId.volume) {
	    currentMCId = nextMCId;
	} else {
	    ZERO_MCID(currentMCId);
	}

    } /* end loop: scan through all the file's mcells */

    /* found an uncorrectable error in the ACL and so we delete it. */

    if (delete) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_989,
			 "ACL error, removing ACL for file tag %ld.\n"), 
		 tagNum);
	return (delete_acl(domain, primaryMCId)); 
    }

    return SUCCESS;


} /* correct_chain_ACL */

/*
 * Function Name: delete_acl
 *
 * Description: This hunts down the ACL recs in a particular tag and
 *              removes them, changing the file ownership to "root".
 *              We change the ownership cuz if the ACLs are gone the
 *              only user that we're certain is allowed to access the
 *              file is root.  If we can't find an ACL, we return
 *              FAILURE.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     primMCId: primary mcell pointer for the file of interest.
 *
 * Output parameters: N/A 
 *
 * Returns: FIXED, FAILURE, NO_MEMORY
 */
static int
delete_acl(domainT *domain, 
	   mcidT primaryMCId)
{
    char    *funcName = "delete_acl";
    int     modified = FALSE;
    int     deleted = FALSE;
    int	    status;
    volumeT *volume;
    volNumT currVol;
    lbnNumT currLbn;
    bsMPgT  *bmtPage;	      /* the BMT page we are working on */
    mcidT   currentMCId;      /* the current mcell we're looking at */
    mcidT   nextMCId;         /* the next mcell we'll look at */
    pageT   *pCurrentPage;    /* the current page we are checking */
    bsMCT   *pPrevMcell;      /* the previous BMT mcell we are working on */
    bsMCT   *pMcell;	      /* the current BMT mcell we are working on */
    bsMRT   *pRecord;
    statT   *pStat;
    bfTagT  aclFile;       /* the tag of the file owning the ACL */

    /*
     * begin the search at the primary mcell
     */

    currentMCId = primaryMCId;

    /*
     * Loop through the current file's (tag's) mcell list deleting all
     * ACL records until we reach the end.
     */

    while (currentMCId.volume) {
	volume = &(domain->volumes[currentMCId.volume]);

	/*
	 * Read the current mcell page (first get LBN).
	 */
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMCId.page, 
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currentMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currentMCId.cell]);

	if (MCID_EQL(currentMCId, primaryMCId)) {
	    pPrevMcell = pMcell;
	}

	nextMCId = pMcell->mcNextMCId;	    

	/*
	 * Now loop through all the mcell records looking for an ACL
	 * record.  If one is found, delete it.
	 */

	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type != BSR_ACL_REC) {
		pRecord = NEXT_MREC(pRecord);
		/* if this mcell has no acl, it becomes "previous" */
		pPrevMcell = pMcell;
		continue;
	    }

	    /* found an ACL record. The ACL consumes an entire mcell
	       so to delete the ACL record is to delete the mcell,
	       which we do her by adding it to the page's free
	       list. */

	    pPrevMcell->mcNextMCId = nextMCId;

	    memset((void *)pMcell, 0, sizeof(*pMcell));

	    if (bmtPage->bmtFreeMcellCnt) {
		pMcell->mcNextMCId.cell = bmtPage->bmtNextFreeMcell;
		bmtPage->bmtNextFreeMcell = currentMCId.cell;
		bmtPage->bmtFreeMcellCnt++;
	    } else {
		/* if this is the only (and hence last) free mcell,
		   zero the forward linkage */
		ZERO_MCID(pMcell->mcNextMCId);
		bmtPage->bmtNextFreeMcell = currentMCId.cell; 
		bmtPage->bmtFreeMcellCnt = 1;
	    }

	    modified = TRUE;
	    deleted = TRUE;
	    break;
	}

	/*
	 * Release Page
	 */

	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	modified = FALSE;
	currentMCId = nextMCId;

    } /* end loop: scan through all the file's mcells */

    /* 
     * If we deleted the ACLs, we don't know who's allowed to access
     * this file anymore, so change the owner to root. 
     *
     * If we didn't delete an ACL record, something's real wrong!
     */

    assert(deleted);

    /* find the fs_fstat record in the file and change the ownership
       to root */

    currentMCId = primaryMCId;

    while (currentMCId.volume) {

	volume = &(domain->volumes[currentMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMCId.page, 
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currentMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currentMCId.cell]);
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BMTR_FS_STAT) {
		/* found fs_stat record, munge it. */

		pStat = (statT *)MREC_TO_REC(pRecord);
		pStat->st_uid = 0;
		pStat->st_gid = 0;
		pStat->st_mode &= (S_IFMT | 0400);

		aclFile.tag_num = pStat->st_ino.tag_num;

		modified = TRUE;
		break;
	    }

	    pRecord = NEXT_MREC(pRecord);
	}

	/*
	 * Release Page
	 */

	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	if (modified)
	    break;

	currentMCId = pMcell->mcNextMCId;

    } /* end loop: scan through all the file's mcells */

    status = add_fixed_message(&(pCurrentPage->messages), BMT,
			       catgets(_m_catd, S_FSCK_1, FSCK_984, 
				       "Deleted ACL from tag %ld, changed file owner to root\n"),
			       aclFile.tag_num);

    return FIXED;

 }/* delete_acl */

/*
 * Function Name: correct_chain_extents
 *
 * Description: This walks the chain to fix all the extent records in it.
 *
 *              Also need to remove/add all changes to to extents to
 *              the SBM.  When modifying the FOB offset of an extent
 *              (other than the first one), the previous extent's FOB
 *              offset must also be modified:
 *
 *               Original          Modified
 *               FOB     LBN       FOB      LBN
 *               0       1000      0        1000
 *               80      2000      120       2000
 *               160      -1       150       -1
 *
 *               In the orignal case we see the following extents:
 *               10 pages at LBN 1000
 *               10 pages at LBN 2000
 *
 *               After Modifying we see:
 *               15 pages at LBN 1000
 *               5 pages at LBN 2000
 *
 *              NOTE: when modifying LBNs only one extent is effected.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     primMCId: primary mcell pointer for the file of interest.
 *     xtntArray: The list of extents for the file of interest.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY
 */
int 
correct_chain_extents(filesetT *fileset, 
		      mcidT primMCId, 
		      fobtoLBNT *xtntArray)
{
    char      *funcName = "correct_chain_extents";
    int	      modified;
    int	      status;
    volNumT   currVol; 
    lbnNumT   currLbn;
    int	      currXtntEntry;
    mcidT     currMCId;     /* The mcell currently being worked on */
    mcidT     nextMCId;	/* The next mcell on the next chain */
    mcidT     chainMCId;	/* The chain mcell on the next chain */
    volumeT   *volume;
    pageT     *pCurrentPage;    /* The current page we are checking */
    bsMPgT    *bmtPage;	      /* The current BMT page we are working on */
    bsMCT     *pMcell;	      /* The current BMT mcell we are working on */
    bsMRT     *pRecord;
    domainT   *domain;
    tagNumT   tag;
    tagNumT   setTag;
    lbnNumT   lbn;
    fobNumT   extentSize;
    fobtoLBNT extentArray[2];

    /*
     * Initialize variables.
     */
    modified = FALSE;
    domain = fileset->domain;
    currXtntEntry = 0;
	
    /*
     * Start at primary
     */
    currMCId = primMCId;

    ZERO_MCID(chainMCId);

    /*
     * Loop until currMCId is empty [0,0,0]
     */
    while (currMCId.volume != 0) {
	/*
	 * Read the primary mcell page (first get LBN).
	 */
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage	= (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currMCId.cell]);
	tag     = pMcell->mcTag.tag_num;
	setTag  = pMcell->mcBfSetTag.tag_num;

	nextMCId = pMcell->mcNextMCId;

	/*
	 * Now loop through primary next chain records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    int	     extent;
	    bsXtntRT *pXtnt;

	    /*
	     * Only concerned with BSR_XTNTS
	     */
	    if (BSR_XTNTS == pRecord->type) {
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		/*
		 * Loop through all the extents in this record.
		 */
		for (extent = 0; 
		     extent < pXtnt->xCnt && extent < BMT_XTNTS;
		     extent++) {
		    /*
		     * Check if the extent from the BMT matches the
		     * corresponding extent from the "file of
		     * interest" extent map passed in by the caller.
		     * If they mismatch, we "correct" the BMT extent
		     * by making it look like the extent from the
		     * caller's passed-in extent map.
		     */
		    if (xtntArray[currXtntEntry].fob != 
			pXtnt->bsXA[extent].bsx_fob_offset) {
			/*
			 * First remove BMT extent(s) from SBM.
			 */
			if (extent != 0) {
			    /* 
			     * if this is not the first extent (extent
			     * == 0) we remove the previous extent's
			     * SBM entry, then the current one too.
			     */
			    lbn = pXtnt->bsXA[extent - 1].bsx_vd_blk;
			    extentSize = (pXtnt->bsXA[extent].bsx_fob_offset -
					  pXtnt->bsXA[extent - 1].bsx_fob_offset);

			    status = remove_extent_from_sbm(domain, 
							    currVol, 
							    lbn, 
							    extentSize,
							    tag, 
							    setTag,
							    &currMCId,
							    (extent - 1));
			    if (status != SUCCESS) {
				return status;
			    }
			} /* end if extent != 0 */
			     
			/* 
			 * remove the current extent's SBM entry 
			 */
			lbn = pXtnt->bsXA[extent].bsx_vd_blk;
			extentSize = (pXtnt->bsXA[extent + 1].bsx_fob_offset -
				      pXtnt->bsXA[extent].bsx_fob_offset);

			status = remove_extent_from_sbm(domain, 
							currVol, 
							lbn, 
							extentSize, 
							tag, 
							setTag,
							&currMCId,
							extent);
			if (status != SUCCESS) {
			    return status;
			}

			/*
			 * Next modify the ODS.
			 */
			status = add_fixed_message(&(pCurrentPage->messages), BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_459, 
							   "Modified mcell (%d,%ld,%d) xtnt entry %d's FOB from %d to %d.\n"),
						   currMCId.volume,
						   currMCId.page,
						   currMCId.cell,
						   extent,  
						   pXtnt->bsXA[extent].bsx_fob_offset,
						   xtntArray[currXtntEntry].fob);
			if (status != SUCCESS) {
			    return status;
			}

			pXtnt->bsXA[extent].bsx_fob_offset = 
			                     xtntArray[currXtntEntry].fob;
			modified = TRUE;

			/*
			 * Now add the new extent(s) to the SBM.
			 * Fake an extent Array(s) to handle the 
			 * extent(s).
			 */
			if (extent != 0) {
			    /* 
			     * if this is not the first extent
			     * (extent == 0) we add the previous
			     * extent's SBM entry as well as the
			     * current one.
			     */
			    extentArray[0].volume = currVol;
			    extentArray[0].lbn = pXtnt->bsXA[extent - 1].bsx_vd_blk;
			    extentArray[0].fob = pXtnt->bsXA[extent - 1].bsx_fob_offset;
			    extentArray[1].volume = currVol;
			    extentArray[1].lbn = pXtnt->bsXA[extent].bsx_vd_blk;
			    extentArray[1].fob = pXtnt->bsXA[extent].bsx_fob_offset;

			    status = collect_sbm_info(domain, 
						      extentArray,
						      2, tag, setTag, 
						      &currMCId,
						      (extent - 1));
			    if (status != SUCCESS) {
				return status;
			    }
			} /* end if extent != 0 */

			extentArray[0].volume = currVol;
			extentArray[0].lbn = pXtnt->bsXA[extent].bsx_vd_blk;
			extentArray[0].fob = pXtnt->bsXA[extent].bsx_fob_offset;
			extentArray[1].volume = currVol;
			extentArray[1].lbn = pXtnt->bsXA[extent + 1].bsx_vd_blk;
			extentArray[1].fob = pXtnt->bsXA[extent + 1].bsx_fob_offset;

			status = collect_sbm_info(domain, extentArray,
						  2, tag, setTag, 
						  &currMCId, extent);
			if (status != SUCCESS) {
			    return status;
			}
		    } /* end if FOB number different */

		    /*
		     * If the LBN in the caller's extent map entry
		     * doesn't match the LBN in the corresponding
		     * entry og the BMT, we "correct" the BMT by
		     * making it look like the caller's extent map.
		     */
		    if (xtntArray[currXtntEntry].lbn != 
			pXtnt->bsXA[extent].bsx_vd_blk) {
			/*
			 * First remove old extent from SBM.
			 */
			lbn = pXtnt->bsXA[extent].bsx_vd_blk;
			extentSize = (pXtnt->bsXA[extent + 1].bsx_fob_offset -
				      pXtnt->bsXA[extent].bsx_fob_offset);

			status = remove_extent_from_sbm(domain, 
							currVol, 
							lbn, 
							extentSize, 
							tag, setTag,
							&currMCId,
							extent);
			if (status != SUCCESS) {
			    return status;
			}

			/*
			 * Next modify the ODS.
			 */
			status = add_fixed_message(&(pCurrentPage->messages), BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_460,
							   "Modified mcell (%d,%ld,%d) xtnt entry %d's LBN from %ld to %ld.\n"),
						   currMCId.volume,
						   currMCId.page,
						   currMCId.cell,
						   extent,  
						   pXtnt->bsXA[extent].bsx_vd_blk,
						   xtntArray[currXtntEntry].lbn);
			if (status != SUCCESS) {
			    return status;
			}

			pXtnt->bsXA[extent].bsx_vd_blk = 
			                        xtntArray[currXtntEntry].lbn; 
			modified = TRUE;

			/*
			 * Now add the new extent to the SBM, we will
			 * Fake an extent Array to handle the extent.
			 */
			extentArray[0].volume = currVol;
			extentArray[0].lbn    = pXtnt->bsXA[extent].bsx_vd_blk;
			extentArray[0].fob   = pXtnt->bsXA[extent].bsx_fob_offset;
			extentArray[1].volume = currVol;
			extentArray[1].lbn    = pXtnt->bsXA[extent + 1].bsx_vd_blk;
			extentArray[1].fob   = pXtnt->bsXA[extent + 1].bsx_fob_offset;

			status = collect_sbm_info(domain, extentArray,
						  2, tag, setTag, 
						  &currMCId, extent);
			if (status != SUCCESS) {
			    return status;
			}
		    } /* end if lbn different */
		    currXtntEntry++;
		}/* end extent for loop */

		/*
		 * Set next Chain to follow it, after we follow this 
		 * chain.
		 */
		chainMCId = pXtnt->chainMCId;

	    } /* end if BSR_XTNT */

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	if (modified == TRUE) {
	    modified = FALSE;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMCId.volume != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currMCId = nextMCId;
	} else {
	    ZERO_MCID(currMCId);
	}/* end if nextMCId exists */
    } /* end primary chain */

    /*
     * Initialize variables
     */
    currMCId = chainMCId;
	
    /*
     * Loop until currMCId is empty [0,0,0]
     */
    while (currMCId.volume != 0) {
	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currMCId.page, &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol,currLbn, &pCurrentPage);
	if (status != SUCCESS) {
	    return status;
	} 

	bmtPage	= (bsMPgT *)pCurrentPage->pageBuf;
	pMcell	= &(bmtPage->bsMCA[currMCId.cell]);
	tag     = pMcell->mcTag.tag_num;
	setTag  = pMcell->mcBfSetTag.tag_num;

	nextMCId = pMcell->mcNextMCId;
	/*
	 * Now loop through records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    int		  extent;
	    bsXtraXtntRT  *pXtraXtnt;

	    pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
	    /*	
	     * Loop on all extents
	     */
	    for (extent = 0; 
		 extent < pXtraXtnt->xCnt && extent < BMT_XTRA_XTNTS;
		 extent++) {
		/*
		 * Check the page.
		 */
		if (xtntArray[currXtntEntry].fob != 
		    pXtraXtnt->bsXA[extent].bsx_fob_offset) {
		    /*
		     * First remove old extent(s) from SBM.
		     */
		    if (extent != 0) {
			/* 
			 * Only do this if NOT on first extent.
			 */
			lbn = pXtraXtnt->bsXA[extent - 1].bsx_vd_blk;
			extentSize = (pXtraXtnt->bsXA[extent].bsx_fob_offset -
				      pXtraXtnt->bsXA[extent - 1].bsx_fob_offset);

			status = remove_extent_from_sbm(domain, 
							currVol, 
							lbn, 
							extentSize, 
							tag, setTag,
							&currMCId,
							(extent - 1));
			if (status != SUCCESS) {
			    return status;
			}
		    } /* end if extent != 0 */

		    /*
		     * First remove old extent from SBM.
		     */
		    lbn = pXtraXtnt->bsXA[extent].bsx_vd_blk;
		    extentSize = (pXtraXtnt->bsXA[extent + 1].bsx_fob_offset -
				  pXtraXtnt->bsXA[extent].bsx_fob_offset);

		    status = remove_extent_from_sbm(domain, currVol, 
						    lbn, extentSize, 
						    tag, setTag,
						    &currMCId,
						    extent);
		    if (status != SUCCESS) {
			return status;
		    }

		    /*
		     * Now modify the ODS.
		     */
		    status = add_fixed_message(&(pCurrentPage->messages), BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_461,
						       "Modified mcell (%d,%ld,%d) xtra xtnt entry %d's FOB from %ld to %ld.\n"),
					       currMCId.volume,
					       currMCId.page,
					       currMCId.cell,
					       extent,	
					       pXtraXtnt->bsXA[extent].bsx_fob_offset,
					       xtntArray[currXtntEntry].fob);

		    if (status != SUCCESS) {
			return status;
		    }

		    pXtraXtnt->bsXA[extent].bsx_fob_offset = 
			                        xtntArray[currXtntEntry].fob;
		    modified = TRUE;
			
		    if (extent != 0) {
			/* 
			 * Only do this if NOT on first extent.
			 */
			/*
			 * Now add the new extent to the SBM, we will
			 * Fake an extent Array to handle the extent.
			 */
			extentArray[0].volume = currVol;
			extentArray[0].lbn    = pXtraXtnt->bsXA[extent - 1].bsx_vd_blk;
			extentArray[0].fob   = pXtraXtnt->bsXA[extent - 1].bsx_fob_offset;
			extentArray[1].volume = currVol;
			extentArray[1].lbn    = pXtraXtnt->bsXA[extent].bsx_vd_blk;
			extentArray[1].fob   = pXtraXtnt->bsXA[extent].bsx_fob_offset;

			status = collect_sbm_info(domain, extentArray,
						  2, tag, setTag, 
						  &currMCId,
						  (extent - 1));
			if (status != SUCCESS) {
			    return status;
			}
		    } /* end if extent != 0 */

		    /*
		     * Now add the new extent to the SBM, we will
		     * Fake an extent Array to handle the extent.
		     */
		    extentArray[0].volume = currVol;
		    extentArray[0].lbn    = pXtraXtnt->bsXA[extent].bsx_vd_blk;
		    extentArray[0].fob   = pXtraXtnt->bsXA[extent].bsx_fob_offset;
		    extentArray[1].volume = currVol;
		    extentArray[1].lbn    = pXtraXtnt->bsXA[extent + 1].bsx_vd_blk;
		    extentArray[1].fob   = pXtraXtnt->bsXA[extent + 1].bsx_fob_offset;

		    status = collect_sbm_info(domain, extentArray,
					      2, tag, setTag, 
					      &currMCId, extent);
		    if (status != SUCCESS) {
			return status;
		    }
		} /* end if page number different */

		/*
		 * Check the LBN.
		 */
		if (xtntArray[currXtntEntry].lbn != 
		    pXtraXtnt->bsXA[extent].bsx_vd_blk) {
		    /*
		     * First remove old extent from SBM.
		     */
		    lbn = pXtraXtnt->bsXA[extent].bsx_vd_blk;
		    extentSize = (pXtraXtnt->bsXA[extent + 1].bsx_fob_offset -
				  pXtraXtnt->bsXA[extent].bsx_fob_offset);

		    status = remove_extent_from_sbm(domain, currVol, 
						    lbn, extentSize, 
						    tag, setTag,
						    &currMCId,
						    extent);
		    if (status != SUCCESS) {
			return status;
		    }

		    /*
		     * Now modify the ODS.
		     */
		    status = add_fixed_message(&(pCurrentPage->messages), BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_462,
						       "Modified mcell (%d,%ld,%d) xtra xtnt entry %d's LBN from %ld to %ld.\n"),
					       currMCId.volume,
					       currMCId.page,
					       currMCId.cell,
					       extent,	 
					       pXtraXtnt->bsXA[extent].bsx_vd_blk,
					       xtntArray[currXtntEntry].lbn);
		    if (status != SUCCESS) {
			return status;
		    }

		    pXtraXtnt->bsXA[extent].bsx_vd_blk = 
			                       xtntArray[currXtntEntry].lbn; 
		    modified = TRUE;

		    /*
		     * Now add the new extent to the SBM, we will
		     * Fake an extent Array to handle the extent.
		     */
		    extentArray[0].volume = currVol;
		    extentArray[0].lbn = pXtraXtnt->bsXA[extent].bsx_vd_blk;
		    extentArray[0].fob = pXtraXtnt->bsXA[extent].bsx_fob_offset;
		    extentArray[1].volume = currVol;
		    extentArray[1].lbn = pXtraXtnt->bsXA[extent + 1].bsx_vd_blk;
		    extentArray[1].fob = pXtraXtnt->bsXA[extent + 1].bsx_fob_offset;

		    status = collect_sbm_info(domain, extentArray,
					      2, tag, setTag, 
					      &currMCId, extent);
		    if (status != SUCCESS) {
			return status;
		    }
		} /* end if lbn different */
		currXtntEntry++;
	    } /* end extent for loop */

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */

	/*
	 * Release Page
	 */
	status = release_page_to_cache(currVol, currLbn, pCurrentPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	if (modified == TRUE) {
	    modified = FALSE;
	}

	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMCId.volume) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currMCId = nextMCId;
	} else {
	    ZERO_MCID(currMCId);
	}/* end if nextMCId exists */

    } /*end chain loop */
    return SUCCESS;
} /* end correct_chain_extents */


/*
 * Function Name: correct_extent_array
 *
 * Description: check to see if the extents need to be fixed.
 *
 * Input parameters:
 *     numExtents: Number of extents in the extent list.
 *     recordArray: An array which is needed to handle crossing of records.
 *     xtntArray: The list of extents.
 *
 * Output parameters:
 *     xtntArray: The modified list of extents.
 *
 * Returns: SUCCESS, FIXED
 */
int 
correct_extent_array(xtntNumT numExtents, 
		     int *recordArray, 
		     fobtoLBNT *xtntArray)
{
    char *funcName = "correct_extent_array";
    int  nextRecord;
    xtntNumT extent;
    int	 fixed;
    
    fixed = FALSE;
    
    if (xtntArray[0].fob != 0) {
	/*
	 * First extent better start at FOB 0.
	 */
	fixed = TRUE;
	xtntArray[0].fob = 0;
    }

    /*
     * Don't need to check the first two.
     * 
     * Assumptions going into this, each mcell record was already
     * fixed, so we only need to deal with boundary errors.  To
     * make this code simpler we are going to doing these checks 
     * on all extents.
     *
     * if fobC < fobA then set fobC to fobB
     * else if fobC < fobB set fobB to fobC
     *
     * Make the extent a hole (-1).
     */
    nextRecord = 1;

    for (extent = 2; extent < numExtents; extent++) {
	bf_fob_t fobA;
	bf_fob_t fobB;
	bf_fob_t fobC;
	int recordExtent;
	    
	fobA = xtntArray[extent - 2].fob;
	fobB = xtntArray[extent - 1].fob;
	fobC = xtntArray[extent].fob;
	recordExtent = recordArray[nextRecord];

	if (extent == recordExtent) {
	    /*
	     * Have a new record so check the boundaries.
	     */
	    nextRecord++;

	    /*
	     * First extent in a new record. The last extent fob and 
	     * the current extent fob must match.
	     */
	    if (fobC != fobB) {
		xtntArray[extent].fob = xtntArray[extent -1].fob;
		xtntArray[extent].lbn = - 1;
		fixed = TRUE;
	    }
	} else {
	    /*
	     * Not the start of a record.
	     */
	    if (fobC < fobA) {
		/*
		 * fobC set to value of fobB.
		 */
		xtntArray[extent].fob = xtntArray[extent -1].fob;
		xtntArray[extent].lbn = - 1;
		xtntArray[extent - 1].lbn = - 1;
		fixed = TRUE;
	    } else if (fobC < fobB) {
		/*
		 * fobB set to value of fobC.
		 */
		xtntArray[extent -1].fob = xtntArray[extent].fob;
		xtntArray[extent].lbn = - 1;
		xtntArray[extent - 1].lbn = - 1;
		fixed = TRUE;
	    }
	}
    }
    if (fixed == TRUE) {
	writemsg(SV_DEBUG | SV_LOG_INFO,
		 catgets(_m_catd, S_FSCK_1, FSCK_465, 
			 "Modified extent array.\n"));
	return FIXED;
    }
    return SUCCESS;
} /* end correct_extent_array */


/*
 * Function Name: correct_chain_tag_and_set
 *
 * Description: check to see if all the tag and setTag match.
 *              This also includes rechecking the FSSTAT record
 *              if it exists in the currMCId.
 *
 * NOTE: This function can modify multiple pages.  It currently only 
 *       returns FIXED if the current bmtPage is modified.  If another
 *       page is modified it returns SUCCESS.  We could pass a flag which 
 * 	 would let us know, but we are already at 7 variables.
 *
 * NOTE: For snapshot filesets we only check the tag, not the setTag.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     currMCId: The information on the current mcell.
 *     prevMCId: The information on the previous mcell.
 *     primTagNum: The tag number of this chain
 *     primSeqNum: The tag seq of this chain
 *     nextTagNum: The next mcells tag number
 *     pPage: The page we are working on.
 *
 * Output parameters:
 *     pPage: The page we are working on.
 *
 * Side effect:
 *     prev page could be modified.
 *
 * Returns: SUCCESS, FIXED, FIXED_PRIOR, NO_MEMORY, FAILURE
 */
static int 
correct_chain_tag_and_set(filesetT *fileset, 
			  mcidT currMCId, 
			  mcidT prevMCId, 
			  tagNumT primTagNum, 
			  uint32_t primSeqNum, 
			  tagNumT nextTagNum, 
			  pageT *pPage)
{
    char    *funcName = "correct_chain_tag_and_set";
    int	    status;
    int	    modified;
    tagNumT currentTagNum;
    tagSeqT currentSeqNum;
    volNumT prevVol;
    lbnNumT prevLbn;
    int     fixedPrior;
    tagNumT fsstatNum;
    tagSeqT fsstatSeq;
    tagNumT filesetId;
    tagSeqT filesetIdSeq;
    volumeT *volume;
    bsMPgT  *bmtPage;
    statT   *pStat;
    statT   alignedStat;  /* needed to fix alignment errors */
    pageT   *pPrevPage;	  /* The prev page we are checking */
    bsMPgT  *bmtPrevPage; /* The previous BMT page we are working on */
    bsMCT   *pMcell;	  /* The current BMT mcell we are working on */
    bsMCT   *pPrevMcell; 
    domainT *domain;
    mcellNodeT *pMcellNode;
    mcellNodeT mcellNode;
    nodeT   *node;
    bsMRT   *pRecord;

    /*
     * Initialize variables.
     */
    modified	   = FALSE;
    domain	   = fileset->domain;
    filesetId	   = fileset->filesetId.dirTag.tag_num;
    filesetIdSeq   = fileset->filesetId.dirTag.tag_seq;
    bmtPage        = (bsMPgT *)pPage->pageBuf;
    pMcell	   = &(bmtPage->bsMCA[currMCId.cell]);
    currentTagNum  = pMcell->mcTag.tag_num;
    currentSeqNum  = pMcell->mcTag.tag_seq;
    fixedPrior     = FALSE;
    fsstatNum      = 0;
    fsstatSeq      = 0;

    /*
     * Check if the mcell has the correct tag number.
     */
    if ((primTagNum != currentTagNum) || (primSeqNum != currentSeqNum)) {
	/*
	 * Something's wrong.
	 * We'll need to modify the mcell skiplist for this node
	 * in addition to the on disk structure(s).
	 */
	mcellNode.mcid = currMCId;
	pMcellNode = &mcellNode;
	status = find_node(domain->mcellList, (void **)&pMcellNode,
			   &node);
	if (status != SUCCESS) {
	    return status;
	} 

	/*
	 * Does this mcell have an fsstat record, if so get
	 * the tag number from it as well.
	 */
	if (MC_IS_FSSTAT(pMcellNode->status)) {
	    pRecord = (bsMRT *)pMcell->bsMR0;
	
	    while ((pRecord->type != BSR_NIL) &&
		   (pRecord->bCnt != 0) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
		/*
		 * If it's not a FSSTAT record, we don't care about it.
		 */
		if (BMTR_FS_STAT == pRecord->type) {
		    pStat    = (statT *)((char *)pRecord + sizeof(bsMRT));

		    fsstatNum = pStat->st_ino.tag_num;
		    fsstatSeq = pStat->st_ino.tag_seq;
		    break;
		}
		pRecord = NEXT_MREC(pRecord);

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
	if (primTagNum == currentTagNum) {
	    /*
	     * Tags the same, Sequence numbers are off.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_466, 
					       "Modified mcell (%d,%ld,%d) tag's seq from %x to %x.\n"),
				       currMCId.volume, 
				       currMCId.page,
				       currMCId.cell, 
				       pMcell->mcTag.tag_seq, primSeqNum);
	    if (status != SUCCESS) {
		return status;
	    }

	    pMcellNode->tagSeq.tag_seq = primSeqNum;
	    pMcell->mcTag.tag_seq = primSeqNum;

	    if (MC_IS_FSSTAT(pMcellNode->status)) {
		if (fsstatSeq != primSeqNum) {
		    pRecord = (bsMRT *)pMcell->bsMR0;
		    
		    while ((BSR_NIL != pRecord->type) &&
			   (pRecord->bCnt) &&
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
			    alignedStat.st_ino.tag_seq = primSeqNum;

			    /*
			     * Copy the aligned structure into the record.
			     */
			    memcpy((char *)pStat, &alignedStat, sizeof(statT));
			} /* end if FSSTAT record */

			pRecord = NEXT_MREC(pRecord);

		    }	/* end of while records exist */
		} /* end if seq numbers different */
	    } /* end if this mcell has an fsstat record */

	    modified = TRUE;
	} else if (((MC_IS_FSSTAT(pMcellNode->status)) && 
		    (fsstatNum == currentTagNum)) ||
		   ((nextTagNum != 0) && (nextTagNum != primTagNum))) {
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
	    if ((currMCId.page != prevMCId.page) ||
		(currMCId.volume != prevMCId.volume)) {
		volume = &(domain->volumes[prevMCId.volume]);
		status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					     volume->volRbmt->bmtLBNSize, 
					     prevMCId.page,
					     &prevLbn, &prevVol);
		if (status != SUCCESS) {
		    writemsg(SV_ERR | SV_LOG_ERR,
			     catgets(_m_catd, S_FSCK_1, FSCK_128,
				     "Can't convert BMT page %ld to LBN.\n"), 
			     prevMCId.page);
		    return FAILURE;
		}

		status = read_page_from_cache(prevVol,prevLbn, &pPrevPage);
		if (status != SUCCESS) {
		    return status;
		} 
		
		bmtPrevPage    = (bsMPgT *)pPrevPage->pageBuf;
		pPrevMcell = &(bmtPrevPage->bsMCA[prevMCId.cell]);
		status = add_fixed_message(&(pPrevPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_422, 
						   "Modified mcell (%d,%ld,%d)'s next chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					   prevMCId.volume, 
					   prevMCId.page,
					   prevMCId.cell, 
					   pPrevMcell->mcNextMCId.volume,
					   pPrevMcell->mcNextMCId.page,
					   pPrevMcell->mcNextMCId.cell,
					   0, 0, 0);
		if (status != SUCCESS) {
		    return status;
		}

		ZERO_MCID(pPrevMcell->mcNextMCId);

		status = release_page_to_cache(prevVol, prevLbn, pPrevPage, 
					       TRUE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}    
	    } else {
		/*
		 * Both mcells on same page.
		 */
		bmtPrevPage = bmtPage;
		pPrevMcell = &(bmtPrevPage->bsMCA[prevMCId.cell]);
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_422,
						   "Modified mcell (%d,%ld,%d)'s next chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					   prevMCId.volume, 
					   prevMCId.page,
					   prevMCId.cell, 
					   pPrevMcell->mcNextMCId.volume,
					   pPrevMcell->mcNextMCId.page,
					   pPrevMcell->mcNextMCId.cell,
					   0, 0, 0);
		if (status != SUCCESS) {
		    return status;
		}
		ZERO_MCID(pPrevMcell->mcNextMCId);
		modified = TRUE;
	    } /* end if on same page or not */

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
				       catgets(_m_catd, S_FSCK_1, FSCK_467, 
					       "Modified mcell (%d,%ld,%d)'s tag from %ld.%u to %ld.%u.\n"),
				       currMCId.volume, 
				       currMCId.page,
				       currMCId.cell, 
				       pMcell->mcTag.tag_num, 
				       pMcell->mcTag.tag_seq,
				       primTagNum, primSeqNum);
	    if (status != SUCCESS) {
		return status;
	    }

  	    status = update_sbm_mcell_tag_and_set_info( domain,
  							&currMCId,
  						        pMcell->mcTag.tag_num,
  							pMcell->mcBfSetTag.tag_num,
  							primTagNum,
  							pMcell->mcBfSetTag.tag_num);
  	    if (status != SUCCESS) {
  		return status;
  	    }

	    pMcellNode->tagSeq.tag_num = primTagNum;
	    pMcellNode->tagSeq.tag_seq = primSeqNum;
	    pMcell->mcTag.tag_num = primTagNum;
	    pMcell->mcTag.tag_seq = primSeqNum;

	    /*
	     * fsstat st_ino record will be corrected in correct_bmt_chains
	     */
	    modified = TRUE;
	} /* end case 1 & 2 */
    } /* tag or sequence numbers different */

    /*
     * Check if the mcell has the correct setTag.
     */
    if ((filesetId != pMcell->mcBfSetTag.tag_num) || 
	(filesetIdSeq != pMcell->mcBfSetTag.tag_seq)) {
	/*
	 * We have already at this point checked the tag number.
	 * we are going to play the numbers, and assume that if
	 * setTag is wrong we are going to reset it.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_468, 
					   "Modified mcell (%d,%ld,%d)'s set tag from %ld.%u to %ld.%u.\n"),
				   currMCId.volume, 
				   currMCId.page,
				   currMCId.cell, 
				   pMcell->mcBfSetTag.tag_num, 
				   pMcell->mcBfSetTag.tag_seq,
				   filesetId,
				   filesetIdSeq);
	if (status != SUCCESS) {
	    return status;
	}

  	status = update_sbm_mcell_tag_and_set_info( domain,
  						    &currMCId,
  						    pMcell->mcTag.tag_num,
  						    pMcell->mcBfSetTag.tag_num,
  						    pMcell->mcTag.tag_num,
  						    filesetId);
  	if (status != SUCCESS) {
  	    return status;
  	}

	pMcell->mcBfSetTag.tag_num = filesetId;
	pMcell->mcBfSetTag.tag_seq = filesetIdSeq;
	modified = TRUE;
    }

    if (fixedPrior == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_469,
			 "Truncating mcell (%d,%ld,%d)'s next chain.\n"),
		 prevMCId.volume, prevMCId.page, prevMCId.cell);
	return FIXED_PRIOR;
    } else if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_470,
			 "Modifying mcell (%d,%ld,%d)'s tag and set tag.\n"),
		 currMCId.volume, currMCId.page, currMCId.cell);
	return FIXED;
    }

    return SUCCESS;
} /* end correct_chain_tag_and_set */


/*
 * Function Name: correct_bsr_xtnt_tag
 *
 * Description: check to see if the tag matches the mcell which
 *              contains the BSR_XTNT and what it points to.
 *
 * NOTE: This function can modify multiple pages.  
 *
 * NOTE: The setTag does not need to handled in this function, it will
 *       be checked and corrected in the correct_chain_tag_and_set
 *       function.
 *
 * Input parameters:
 *     fileset: Pointer to the fileset being checked. 
 *     bmtXtntMcell: The information on the bmtXtnt mcell.
 *     chainMCId: The information on the chain mcell.
 *     pPage: The page which contains the bmt_xtnt record
 *
 * Output parameters:
 *     pPage: The page we are working on.
 *
 * Side effect:
 *     prev page could be modified.
 *
 * Returns: SUCCESS, FIXED, NO_MEMORY, FAILURE
 */
int 
correct_bsr_xtnt_tag(filesetT *fileset, 
		     mcidT bmtXtntMcell, 
		     mcidT chainMCId, 
		     pageT *pPage)
{
    char *funcName = "correct_bsr_xtnt_tag";
    int	 status;
    int	 modified;
    int  fixedPrior;
    tagNumT bmtXtntTagNum;
    tagSeqT bmtXtntSeqNum;
    tagNumT bmtChainTagNum;
    tagSeqT bmtChainSeqNum;
    tagNumT nextTagNum;
    volNumT chainVol;
    lbnNumT chainLbn;
    volumeT *volume;
    domainT *domain;
    pageT   *pChainPage;  
    bsMPgT  *pBmtXtntPage;
    bsMPgT  *pBmtChainPage;
    bsMCT   *pBmtXtntMcell;	  
    bsMCT   *pBmtChainMcell; 
    bsMRT   *pRecord;
    mcellNodeT *pMcellNode;
    mcellNodeT mcellNode;
    nodeT      *node;

    /*
     * Initialize variables.
     */
    modified	   = FALSE;
    fixedPrior     = FALSE;
    domain	   = fileset->domain;
    pBmtXtntPage   = (bsMPgT *)pPage->pageBuf;
    pBmtXtntMcell  = &(pBmtXtntPage->bsMCA[bmtXtntMcell.cell]);
    bmtXtntTagNum  = pBmtXtntMcell->mcTag.tag_num;
    bmtXtntSeqNum  = pBmtXtntMcell->mcTag.tag_seq;

    /*
     * Load the page which contains the chain mcell.
     */
    if ((bmtXtntMcell.page != chainMCId.page) ||
	(bmtXtntMcell.volume != chainMCId.volume)) {
	/*
	 * The two mcells are on different pages.
	 */
	volume = &(domain->volumes[chainMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     chainMCId.page,
				     &chainLbn, &chainVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     chainMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(chainVol, chainLbn, &pChainPage);
	if (status != SUCCESS) {
	    return status;
	} 
	pBmtChainPage = (bsMPgT *)pChainPage->pageBuf;
	pBmtChainMcell = &(pBmtChainPage->bsMCA[chainMCId.cell]);

    } else {
	/*
	 * Both mcells on same page.
	 */
	pBmtChainPage = pBmtXtntPage;
	pBmtChainMcell = &(pBmtChainPage->bsMCA[chainMCId.cell]);
    }
    
    bmtChainTagNum  = pBmtChainMcell->mcTag.tag_num;
    bmtChainSeqNum  = pBmtChainMcell->mcTag.tag_seq;

    /*
     * See if the bmtChainMcell has a valid next mcell.
     */
    mcellNode.mcid = pBmtChainMcell->mcNextMCId;
    pMcellNode = &mcellNode;
    status = find_node(domain->mcellList, (void **)&pMcellNode, &node);
    if (status != SUCCESS) {
	if (NOT_FOUND == status) {
	    nextTagNum = 0;
	} else {
	    return status;
	}
    } else {
	nextTagNum = pMcellNode->tagSeq.tag_num; 
    }

    /*
     * Now find the bmtChainMcell node.
     */
    mcellNode.mcid = chainMCId;
    pMcellNode = &mcellNode;
    status = find_node(domain->mcellList, (void **)&pMcellNode, &node);
    if (status != SUCCESS) {
	/*
	 * Should never fail
	 */
	return status;
    }

    /*
     * Check if the chain mcell has the correct tag number.
     */
    if ((bmtXtntTagNum != bmtChainTagNum) || 
	(bmtXtntSeqNum != bmtChainSeqNum)) {
	/*
	 * We have already checked that Primary and Xtnt are the same.
	 *
	 * Valid Cases
	 *       Xtnt	        Chain  Next
	 *        4              4     NULL
	 *        4              4     4
	 *
	 * Error Cases
	 * Case 1
	 *        4              5     NULL 	->  4  4 NULL
	 * Case 2
	 *        4              5     4	->  4  4 NULL
	 * Case 3
	 *        4              5     !4	->  4 NULL
	 *
	 */
	if (bmtXtntTagNum == bmtChainTagNum) {
	    /*
	     * Tags the same, Sequence numbers are off.
	     */
	    status = add_fixed_message(&(pChainPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_466, 
					       "Modified mcell (%d,%ld,%d) tag's seq from %x to %x.\n"),
				       chainMCId.volume, chainMCId.page,
				       chainMCId.cell, 
				       pBmtChainMcell->mcTag.tag_seq, 
				       bmtXtntSeqNum);
	    if (status != SUCCESS) {
		return status;
	    }
	    pBmtChainMcell->mcTag.tag_seq = bmtXtntSeqNum;
	    pMcellNode->tagSeq.tag_seq  = bmtXtntSeqNum;
	    modified = TRUE;
	} else if (nextTagNum && (nextTagNum != bmtXtntTagNum)) {
	    /*
	     * Case #3 : Xtnt & Next different.
	     * Truncate the chain chain - page was passed into us, but we
	     * still need to find the correct record.
	     */
	    /*
	     * Now loop through primary next chain records.
	     */
	    pRecord = (bsMRT *)pBmtXtntMcell->bsMR0;

	    while ((BSR_NIL != pRecord->type) &&
		   (pRecord->bCnt) &&
		   (pRecord < ((bsMRT *) &(pBmtXtntMcell->bsMR0[BSC_R_SZ])))) {
		bsXtntRT *pXtnt;

		/*
		 * Only concerned with BSR_XTNTS
		 */
		if (BSR_XTNTS == pRecord->type) {
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_471, 
						       "Modified mcell (%d,%ld,%d)'s xtnt chain from (%d,%ld,%d) to (%d,%ld,%d).\n"),
					       bmtXtntMcell.volume, 
					       bmtXtntMcell.page,
					       bmtXtntMcell.cell,
					       chainMCId.volume,
					       chainMCId.page,
					       chainMCId.cell,
					       0, 0, 0);
		    if (status != SUCCESS) {
			return status;
		    }
		    ZERO_MCID(pXtnt->chainMCId);
		    pXtnt->mcellCnt = 1;
		    
		    /*
		     * Mark this mcell as an orphan.
		     */
		    MC_CLEAR_FOUND_PTR(pMcellNode->status);
		    fixedPrior = TRUE;
		} /* end if BSR_XTNT */

		pRecord = NEXT_MREC(pRecord);

	    } /* end record loop */
	} else {
	    /*
	     * case #1: Xtnt and Chain different, with no next.
	     * case #2: Xtnt & Next the same, different from chain
	     */
	    status = add_fixed_message(&(pChainPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_467, 
					       "Modified mcell (%d,%ld,%d)'s tag from %ld.%u to %ld.%u.\n"),
				       chainMCId.volume, 
				       chainMCId.page,
				       chainMCId.cell, 
				       pBmtChainMcell->mcTag.tag_num, 
				       pBmtChainMcell->mcTag.tag_seq,
				       bmtXtntTagNum, bmtXtntSeqNum);
	    if (status != SUCCESS) {
		return status;
	    }

  	    status = update_sbm_mcell_tag_and_set_info( domain,
  							&chainMCId,
  							pBmtChainMcell->mcTag.tag_num,
  							pBmtChainMcell->mcBfSetTag.tag_num,
  							bmtXtntTagNum,
  							pBmtChainMcell->mcBfSetTag.tag_num);
  
            if (status != SUCCESS) {
                return status;
            }

	    pMcellNode->tagSeq.tag_num = bmtXtntTagNum;
	    pMcellNode->tagSeq.tag_seq = bmtXtntSeqNum;

	    pBmtChainMcell->mcTag.tag_num = bmtXtntTagNum;
	    pBmtChainMcell->mcTag.tag_seq = bmtXtntSeqNum;
	    modified = TRUE;
	} /* end case 1 & 2 */
    } /* tag or sequence numbers different */

    /*
     * Release the page if needed.
     */
    if ((bmtXtntMcell.page != chainMCId.page) ||
	(bmtXtntMcell.volume != chainMCId.volume)) {
	/*
	 * The two mcells are on different pages.
	 */
	status = release_page_to_cache(chainVol, chainLbn, pChainPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}    
    } else {
	/*
	 * Both mcells on same page.
	 */
	pChainPage = NULL;
    }

    if (fixedPrior == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_472, 
			 "Truncating mcell (%d,%ld,%d)'s BSR_XTNT chain.\n"),
		 bmtXtntMcell.volume, bmtXtntMcell.page, bmtXtntMcell.cell);
	return FIXED;
    } else if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_473, 
			 "Modifying mcell (%d,%ld,%d)'s tag.\n"),
		 chainMCId.volume, chainMCId.page, chainMCId.cell);
	return FIXED;
    }
    return SUCCESS;
} /* end correct_bsr_xtnt_tag */


/*
 * Function Name: append_page_to_file
 *
 * Description: This function tacks a page onto the end of a file.
 *		It will modify the extent map of the file, and the
 *		in-memory SBM to indicate that the page is in use.
 *		FUTURE: If necessary, it will also add a free page
 *		to the BMT.
 *
 *		NOTE:  This routine CANNOT be called before the SBM
 *		overlaps have been handled and cleared.
 *
 *              NOTE: This routine assumes we're adding an
 *              ADVFS_METADATA_PGSZ page to a file whose pages are
 *              also of ADVFS_METADATA_PGSZ (rather than pages sized
 *              for a user file!).
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
int 
append_page_to_file(filesetT *fileset, 
		    tagNodeT *tag, 
		    int *foundPage)
{
    char		*funcName = "append_page_to_file";
    domainT		*domain;
    int			status;
    volNumT		currVol;
    lbnNumT		currLbn;
    pageT		*page;
    pageNumT		headFreePageNum;
    volNumT		headFreeVol;
    lbnNumT		headFreeLbn;
    pageT		*headFreePage;
    pageNumT		freePageNum;
    volNumT		freeVol;
    lbnNumT		freeLbn;
    pageT		*freePage;
    volNumT		primVol;
    lbnNumT		primLbn;
    pageT		*primPage;
    volNumT		nextVol;
    lbnNumT		nextLbn;
    int			done;
    int			foundBsrXtntsRecord;
    int			headFreeModified;
    mcidT		currMCId;
    mcidT		chainMCId;
    mcidT		nextMCId;
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
    cellNumT		freeMcellNum;
    bsMCT		*pNewMcell;
    bsMRT		*pNewRecord;
    bsXtntRT		*pXtnt;
    bsXtraXtntRT	*pXtraXtnt;
    int			xCnt;
    bsXtraXtntRT	*pNewXtra;
    lbnNumT		lastXtntLbn;
    int			lastXtntSize;
    bf_fob_t		nextFob;
    mcidT		fakeMcell;
    fobtoLBNT		fakeExtentArray[2];

    domain = fileset->domain;
    *foundPage = FALSE;
    foundBsrXtntsRecord = FALSE;
    headFreeModified = FALSE;
    done = FALSE;

    /*
     * Walk the BMT chain looking for the last mcell for this tag with extents.
     */
    currMCId = tag->tagMCId;
    ZERO_MCID(chainMCId);

    /*
     * Loop through primary next list.
     */
    while (currMCId.volume != 0 && foundBsrXtntsRecord == FALSE) {

	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize,
				     currMCId.page, &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"),
		     currMCId.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &page);
	if (status != SUCCESS) {
	    return status;
	}

	bmtPage = (bsMPgT *)page->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCId.cell]);

	nextMCId = pMcell->mcNextMCId;

	/*
	 * Now loop through primary next records.
	 */

	pRecord = (bsMRT *)pMcell->bsMR0;
	
	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * If it's not a BSR_XTNTS record, we don't care about it.
	     */
	    if (pRecord->type == BSR_XTNTS) {
		foundBsrXtntsRecord = TRUE;
	        pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		/*
		 * Set chainMCId to follow the extent chain later.
		 */
		chainMCId = pXtnt->chainMCId;
		break;  /* break out of record loop */
	    }
	    
	    /*
	     * Point to the next record.
	     */

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */
	
	/*
	 * If the BSR_XTNTS record has not been found yet, or if there
	 * are chain records, release this page.  Thus, if the XTNTS record
	 * has been found AND there is no chain, this page is held onto.
	 */
	if (foundBsrXtntsRecord == FALSE || chainMCId.volume != 0) {
	    status = release_page_to_cache(currVol, currLbn, page,
					   FALSE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	}
	
	/*
	 * Reset pointers for the next pass.
	 */
	currMCId = nextMCId;
    } /* end loop on primary next list */
    
    assert(foundBsrXtntsRecord == TRUE);
    
    /*
     * Now start looking through the extent mcell chain.
     */

    currMCId = chainMCId;
    
    /* 
     * Note that either all pages have been released if there is a
     * chain, or that we have a page open and will never enter this loop
     * if the chainMCId volume is zero.
     */
    while (currMCId.volume != 0) {

	volume = &(domain->volumes[currMCId.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize,
				     currMCId.page,
				     &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"),
		     currMCId.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol, currLbn, &page);
	if (status != SUCCESS) {
	    return status;
	}
	
	bmtPage = (bsMPgT *)page->pageBuf;
	pMcell  = &(bmtPage->bsMCA[currMCId.cell]);
	
	nextMCId = pMcell->mcNextMCId;
	
	/*
	 * Now loop through looking for the XTRA record.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	
	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BSR_XTRA_XTNTS) {
		break; /* out of record loop */
	    }

	    pRecord = NEXT_MREC(pRecord);

	} /* end record loop */
	
	assert(pRecord->type == BSR_XTRA_XTNTS);
	/*
	 * If this is the last mcell, keep it, else release it and
	 * find the next one.
	 */
	if (nextMCId.volume != 0) {
	    status = release_page_to_cache(currVol, currLbn, page,
					   FALSE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	}
	
	/*
	 * Reset pointers for the next pass.
	 */
	currMCId = nextMCId;
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
	    xCnt = pXtnt->xCnt;
	    
	    lastXtntLbn = pXtnt->bsXA[xCnt - 2].bsx_vd_blk;
	    lastXtntSize = (pXtnt->bsXA[xCnt - 1].bsx_fob_offset -
			    pXtnt->bsXA[xCnt - 2].bsx_fob_offset);
	    nextFob = pXtnt->bsXA[xCnt - 1].bsx_fob_offset;
	    break;
	case BSR_XTRA_XTNTS:
	    pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
	    xCnt = pXtraXtnt->xCnt;
	    
	    lastXtntLbn = pXtraXtnt->bsXA[xCnt - 2].bsx_vd_blk;
	    lastXtntSize = (pXtraXtnt->bsXA[xCnt - 1].bsx_fob_offset -
			    pXtraXtnt->bsXA[xCnt - 2].bsx_fob_offset);
	    nextFob = pXtraXtnt->bsXA[xCnt - 1].bsx_fob_offset;
	    break;
	default:
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_479, 
			     "Internal Error: Non-extent record in append page.\n"));
	    return FAILURE;
    }

    /*
     * nextVol and nextLbn point to the next page that we hope is free.
     * If not, they will get reset to point to a free page.
     */
    nextVol = currVol;
    nextLbn = lastXtntLbn + (lastXtntSize * ADVFS_FOBS_PER_DEV_BSIZE);
    
    /*
     * If the last extent is a hole, it will be dealt with later.
     */
    if (lastXtntLbn == XTNT_TERM) {
	*foundPage = FALSE;
    } else {
	*foundPage = is_page_free(domain, nextVol, nextLbn);
    }
    
    if (*foundPage == TRUE) {
	/*
	 * The page immediately following the last page of the file is
	 * free.  This is the easiest case to deal with.  Extend the
	 * final extent page by 1, and tell the SBM it's been used.
	 */
	switch (pRecord->type) {
	    case BSR_XTNTS:
		pXtnt->bsXA[xCnt - 1].bsx_fob_offset += ADVFS_METADATA_PGSZ_IN_FOBS;
		break;
	    case BSR_XTRA_XTNTS:
		pXtraXtnt->bsXA[xCnt - 1].bsx_fob_offset += ADVFS_METADATA_PGSZ_IN_FOBS;
		break;
	    default:
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_483,
				 "Internal Error: record type changed in append page.\n"));
		return FAILURE;
	}
	done = TRUE;
    } else {
	/*
	 * The page following the last page of the file was not free.
	 * Now search the current mcell's volume for a free page.
	 * Start early and work toward the end so that if fsck has to
	 * grab more pages for this file, the following page will
	 * hopefully be free also.  This scheme should work fine with
	 * all the storage allocation/ reservation schemes planned
	 */
	for (nextLbn = ADVFS_RESERVED_BLKS;
	     nextLbn <= domain->volumes[nextVol].volSize - 
		             ADVFS_METADATA_PGSZ_IN_BLKS;
	     nextLbn += ADVFS_METADATA_PGSZ_IN_BLKS)
	{
	    if (is_page_free(domain, nextVol, nextLbn) == TRUE) {
		*foundPage = TRUE;
		break;
	    }
	}
    }
    
    if (*foundPage == FALSE) {
	/*
	 * Nothing free on this volume, walk through all volumes looking
	 * for a free page, except the current one.
	 */
	for (nextVol = 1 ; nextVol <= domain->highVolNum ; nextVol++) {
	    if (domain->volumes[nextVol].volFD == -1 ||
		nextVol == currVol) {
		/*
		 * Not a valid volume, or it has already been checked.
		 */
		continue;
	    }
	    
	    for (nextLbn = ADVFS_RESERVED_BLKS;
		 nextLbn <= domain->volumes[nextVol].volSize - 
		             ADVFS_METADATA_PGSZ_IN_BLKS;
		 nextLbn += ADVFS_METADATA_PGSZ_IN_BLKS)
	    {
		if (is_page_free(domain, nextVol, nextLbn) == TRUE) {
		    *foundPage = TRUE;
		    break; /* out of LBN loop */
		}
	    }
	    
	    if (*foundPage == TRUE) {
		break; /* out of volume loop */
	    }
	} /* End for volume loop */
    } /* End if page not on desired volume. */
    
    if (*foundPage == FALSE) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_481, 
			 "No free pages in this storage domain.\n"));
	writemsg(SV_ERR | SV_LOG_ERR | SV_CONT,
		 catgets(_m_catd, S_FSCK_1, FSCK_482,
			 "Can't add page to tag %ld.%u in '%s' file system.\n"),
		 tag->tagSeq.tag_num, tag->tagSeq.tag_seq, fileset->fsName);
	return SUCCESS;
    }
    
    if (nextVol == currVol && done == FALSE) {
	/*
	 * Page is on this volume, try to add extent to this mcell.
	 */
	switch (pRecord->type) {
	    case BSR_XTNTS:
		if (xCnt < BMT_XTNTS) {
		    pXtnt->xCnt += 1;
		    xCnt += 1;
		    pXtnt->bsXA[xCnt - 2].bsx_vd_blk = nextLbn;
		    pXtnt->bsXA[xCnt - 1].bsx_fob_offset =
			pXtnt->bsXA[xCnt - 2].bsx_fob_offset + ADVFS_METADATA_PGSZ_IN_FOBS;
		    pXtnt->bsXA[xCnt - 1].bsx_vd_blk = -1;
		    done = TRUE;
		}
		break;
	    case BSR_XTRA_XTNTS:
		if (xCnt < BMT_XTRA_XTNTS) {
		    pXtraXtnt->xCnt += 1;
		    xCnt += 1;
		    pXtraXtnt->bsXA[xCnt - 2].bsx_vd_blk = nextLbn;
		    pXtraXtnt->bsXA[xCnt - 1].bsx_fob_offset =
			pXtraXtnt->bsXA[xCnt - 2].bsx_fob_offset + ADVFS_METADATA_PGSZ_IN_FOBS;
		    pXtraXtnt->bsXA[xCnt - 1].bsx_vd_blk = -1;
		    done = TRUE;
		}
		break;
	    default:
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_483,
				 "Internal Error: record type changed in append page.\n"));
		return FAILURE;
	}
    } /* end if new page on same volume as old page */
    
    if (done == FALSE) {
	/*
	 * Need to get a new mcell, look at head of BMT free mcell list.
	 */
	headFreePageNum = 0;

	status = convert_page_to_lbn(domain->volumes[nextVol].volRbmt->bmtLBN,
				     domain->volumes[nextVol].volRbmt->bmtLBNSize,
				     headFreePageNum, &headFreeLbn,
				     &headFreeVol);
	if (status != SUCCESS) {
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
	    if (status != SUCCESS) {
		return status;
	    }
	}
	
	headFreeBmtPage = (bsMPgT *)headFreePage->pageBuf;
	pHeadFreeMcell = &(headFreeBmtPage->bsMCA[0]);
		       
	/*
	 * Now loop through records looking for BSR_MCELL_FREE_LIST
	 */
	pHeadFreeRecord = (bsMRT *)pHeadFreeMcell->bsMR0;
	freePageNum = -1;

	while ((pHeadFreeRecord->type != BSR_NIL) &&
	       (pHeadFreeRecord->bCnt != 0) &&
	       (pHeadFreeRecord < ((bsMRT *) &(pHeadFreeMcell->bsMR0[BSC_R_SZ])))) {
	    /*
	     * BSR_MCELL_FREE_LIST is the only type we care about.
	     */
	    if (pHeadFreeRecord->type == BSR_MCELL_FREE_LIST) {
		headRecord = (bsMcellFreeListT *)((char *)pRecord +
						  sizeof(bsMRT));
		freePageNum = headRecord->headPg; /* TODO: 0 when none free? */
		break;
	    }
	    /*
	     * Point to the next record.
	     */
	    pHeadFreeRecord = NEXT_MREC(pHeadFreeRecord);

	} /* end record loop */

	assert(freePageNum != -1);

	/*
	 * Now read the first BMT page that has a free mcell.
	 */
	status = convert_page_to_lbn(domain->volumes[headFreeVol].volRbmt->bmtLBN,
				     domain->volumes[headFreeVol].volRbmt->bmtLBNSize,
				     freePageNum, &freeLbn, &freeVol);
	if (status != SUCCESS) {
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
	    if (status != SUCCESS) {
		return status;
	    }
	}

	freeBmtPage = (bsMPgT *)freePage->pageBuf;

	if (freeBmtPage->bmtFreeMcellCnt == 0) {
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
		if (status != SUCCESS) {
		    return status;
		}
	    }
	    if (headFreeVol != currVol || headFreeLbn != currLbn) {
		status = release_page_to_cache(headFreeVol, headFreeLbn,
					       headFreePage, FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
	    }
	    status = release_page_to_cache(currVol, currLbn,
					   page, FALSE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    *foundPage = FALSE;
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_484,
			     "Can't add page because there are no free mcells.\n"));
	    return SUCCESS;
	}


	freeMcellNum = freeBmtPage->bmtNextFreeMcell;
	pFreeMcell = &(freeBmtPage->bsMCA[freeMcellNum]);
	assert(pFreeMcell->mcTag.tag_num == 0);

	/*
	 * Now take this mcell off the free list for this page.
	 */
	freeBmtPage->bmtNextFreeMcell = pFreeMcell->mcNextMCId.cell;
	freeBmtPage->bmtFreeMcellCnt--;

	if (freeBmtPage->bmtFreeMcellCnt == 0) {
	    /*
	     * That was the last free mcell on this page - remove this page
	     * from the free mcell page list.
	     */
	    headRecord->headPg = freeBmtPage->bmtNextFreePg;
	    freeBmtPage->bmtNextFreePg = 0;
	    headFreeModified = TRUE;
	}

	/*
	 * Got the new mcell.  Set the previous one to point to it.
	 */
	if (pRecord->type == BSR_XTNTS) {
	    assert(pXtnt->chainMCId.volume == 0);
	    pXtnt->chainMCId.volume = freeVol;
	    pXtnt->chainMCId.page = freePageNum;
	    pXtnt->chainMCId.cell = freeMcellNum;
	} else {
	    assert(pMcell->mcNextMCId.volume == 0);
	    pMcell->mcNextMCId.volume = freeVol;
	    pMcell->mcNextMCId.page = freePageNum;
	    pMcell->mcNextMCId.cell = freeMcellNum;
	}

	/*
	 * Now initialize the new mcell.
	 */
	pNewMcell = pFreeMcell;

	pNewMcell->mcBfSetTag.tag_num  = pMcell->mcBfSetTag.tag_num;
	pNewMcell->mcBfSetTag.tag_seq  = pMcell->mcBfSetTag.tag_seq;
	pNewMcell->mcTag.tag_num       = pMcell->mcTag.tag_num;
	pNewMcell->mcTag.tag_seq       = pMcell->mcTag.tag_seq;
	ZERO_MCID(pNewMcell->mcNextMCId);
	pNewRecord = (bsMRT *)pNewMcell->bsMR0;
	pNewRecord->type = BSR_XTRA_XTNTS;
	pNewRecord->bCnt = sizeof(bsMRT) + sizeof(bsXtraXtntRT);

	pNewXtra = (bsXtraXtntRT *)((char *)pNewRecord + sizeof(bsMRT));
	pNewXtra->xCnt = 2;
	pNewXtra->bsXA[0].bsx_fob_offset = nextFob;
	pNewXtra->bsXA[0].bsx_vd_blk = nextLbn;
	pNewXtra->bsXA[1].bsx_fob_offset = nextFob + volume->userAllocSz;
	pNewXtra->bsXA[1].bsx_vd_blk = -1;

	/*
	 * Now set the trailing record.
	 */
	pNewRecord = NEXT_MREC(pNewRecord);
	pNewRecord->type = BSR_NIL;
	pNewRecord->bCnt = 0;

	/*
	 * Release the pages grabbed to find the new mcell.
	 */
	if ((freeVol != headFreeVol || freeLbn != headFreeLbn) &&
	    (freeVol != currVol     || freeLbn != currLbn)) {
	    status = release_page_to_cache(freeVol, freeLbn, freePage,
					   TRUE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	}
	if (headFreeVol != currVol || headFreeLbn != currLbn) {
	    status = release_page_to_cache(headFreeVol, headFreeLbn,
					   headFreePage,
					   headFreeModified, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	}

	/*
	 * Final step in allocating new mcell is to increment mcellCnt.
	 */

	/*
	 * Walk the BMT primary next chain looking for the BSR_XTNTS record.
	 */
	currMCId = tag->tagMCId;
	foundBsrXtntsRecord = FALSE;

	/*
	 * Loop through primary next list.
	 */
	while (currMCId.volume != 0 && foundBsrXtntsRecord == FALSE) {

	    volume = &(domain->volumes[currMCId.volume]);
	    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
					 volume->volRbmt->bmtLBNSize,
					 currMCId.page, 
					 &primLbn, &primVol);
	    if (status != SUCCESS) {
		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_128,
				 "Can't convert BMT page %ld to LBN.\n"),
			 currMCId.page);
		return FAILURE;
	    }

	    if (primVol == currVol && primLbn == currLbn) {
		primPage = page;
	    } else {
		status = read_page_from_cache(primVol, primLbn, &primPage);
		if (status != SUCCESS) {
		    return status;
		}
	    }

	    bmtPage = (bsMPgT *)page->pageBuf;
	    pMcell  = &(bmtPage->bsMCA[currMCId.cell]);

	    nextMCId = pMcell->mcNextMCId;

	    /*
	     * Now loop through primary next records.
	     */

	    pRecord = (bsMRT *)pMcell->bsMR0;
	
	    while ((pRecord->type != BSR_NIL) &&
		   (pRecord->bCnt != 0) &&
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
		    pXtnt->mcellCnt += 1;
		    break;  /* break out of record loop */
		}
	    
		/*
		 * Point to the next record.
		 */

		pRecord = NEXT_MREC(pRecord);

	    } /* end record loop */
	
	    /*
	     * If the BSR_XTNTS record has not been found yet,
	     * release this page.
	     */
	    if (foundBsrXtntsRecord == FALSE &&
		(primVol != currVol || primLbn != currLbn))
	    {
		status = release_page_to_cache(primVol, primLbn, primPage,
					       FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
	    }
	
	    /*
	     * Reset pointers for the next pass.
	     */
	    currMCId = nextMCId;
	} /* end loop on primary next list */

	assert(foundBsrXtntsRecord == TRUE);

	if (primVol != currVol || primLbn != currLbn) {
	    status = release_page_to_cache(primVol, primLbn, primPage,
					   TRUE, FALSE);
	    if (status != SUCCESS) {
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
    fakeMcell.volume = nextVol;
    fakeMcell.page = 1;
    fakeMcell.cell = 1;

    fakeExtentArray[0].volume = nextVol;
    fakeExtentArray[0].lbn    = nextLbn;
    fakeExtentArray[0].fob    = 0;
    fakeExtentArray[1].volume = nextVol;
    fakeExtentArray[1].lbn    = -1;
    fakeExtentArray[1].fob    = ADVFS_METADATA_PGSZ_IN_FOBS;

    status = collect_sbm_info(domain, fakeExtentArray, 2,
			      tag->tagSeq.tag_num,
			      fileset->filesetId.dirTag.tag_num,
			      &fakeMcell, 0);
    if (status != SUCCESS) {
	return status;
    }

    /*
     * Release the BMT page we're working on.
     */
    status = release_page_to_cache(currVol, currLbn, page, TRUE, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    /*
     * Increment the FS_STAT record's file size by pagesize.
     */
    tag->size += ADVFS_METADATA_PGSZ;
    status = correct_fsstat(fileset, tag);
    if (status != SUCCESS) {
	return status;
    }

    writemsg(SV_VERBOSE | SV_LOG_FIXED,
	     catgets(_m_catd, S_FSCK_1, FSCK_485,
		     "Added page (%d.%ld) to tag %ld.%u in '%s' file system.\n"),
	     nextVol, nextLbn,
	     tag->tagSeq.tag_num, tag->tagSeq.tag_seq, fileset->fsName);

    return SUCCESS;
} /* end append_page_to_file */


/*
 * Function Name: free_orphan
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
int 
free_orphan(domainT *domain, 
	    volNumT volNum, 
	    pageNumT pageNum, 
	    cellNumT mcellNum)
{
    char *funcName = "free_orphan";
    int status;
    lbnNumT currLbn;
    volNumT currVol;
    volNumT nextVol;
    pageNumT nextPage;
    cellNumT nextMCId;
    volNumT chainVol;
    pageNumT chainPage;
    cellNumT chainMcell;
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
    tempMcell.mcid.volume  = volNum;
    tempMcell.mcid.page = pageNum;
    tempMcell.mcid.cell = mcellNum;
    mcell = &tempMcell;

    status = find_node(domain->mcellList, (void **)&mcell, &node);
    if (status != SUCCESS) {
	if (NOT_FOUND == status) {
	    return SUCCESS;
	} else {
	    return status;
	}
    }
    
    writemsg(SV_VERBOSE | SV_LOG_FOUND,
	     catgets(_m_catd, S_FSCK_1, FSCK_486, 
		     "Deleting mcell (%d,%ld,%d), it is an orphaned mcell.\n"), 
	     volNum, pageNum, mcellNum);

    /*
     * Now that we know them mcell exists, find the page contains it.
     */
    status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				 volume->volRbmt->bmtLBNSize, 
				 pageNum, &currLbn, &currVol);
    if (status != SUCCESS) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_128,
			 "Can't convert BMT page %ld to LBN.\n"), 
		 pageNum);
	return FAILURE;
    }

    status = read_page_from_cache(currVol, currLbn, &pPage);
    if (status != SUCCESS) {
	return status;
    } 
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;

    /*
     * Save the next chain.
     */
    nextVol   = pMcell->mcNextMCId.volume;
    nextPage  = pMcell->mcNextMCId.page;
    nextMCId = pMcell->mcNextMCId.cell;

    /*
     * Walk the records to see if it has a chain chain.
     */
    while ((pRecord->type != BSR_NIL) &&
	   (pRecord->bCnt) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	if (BSR_XTNTS == pRecord->type) {
	    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		  
	    chainVol   = FETCH_MC_VOLUME(pXtnt->chainMCId);
	    chainPage  = FETCH_MC_PAGE(pXtnt->chainMCId);
	    chainMcell = FETCH_MC_CELL(pXtnt->chainMCId);

	} /* end if BSR_XTNT */
	
	/*
	 * Set pRecord to next record  of pMcell.
	 */

	pRecord = NEXT_MREC(pRecord);

    } /* end of walking the records */

    /*
     * Now we can finally free this mcell.
     */
    status = free_mcell(domain, volNum, pPage, mcellNum);
    if (status != SUCCESS) {
	return status;
    }
    /*
     * Release page
     */
    status = release_page_to_cache(currVol, currLbn, 
				   pPage, TRUE, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    /*
     * Now that page is released (to stop deadlock), free its pointers.
     */
    status = free_orphan(domain, nextVol, nextPage, nextMCId);
    if (status != SUCCESS) {
	return status;
    }
    
    status = free_orphan(domain, chainVol, chainPage, chainMcell);
    if (status != SUCCESS) {
	return status;
    }

    return SUCCESS;
}

/* end metadata.c */
