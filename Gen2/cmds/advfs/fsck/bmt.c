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
 *      On-disk structure fixer.
 *
 * Date:
 *      Mon Jan 10 13:20:44 PST 2000
 */
/*
 * HISTORY
 * 
 */

#include "fsck.h"

/*
 * Global
 */
extern nl_catd _m_catd;

/* private prototypes */


static int correct_bsr_xtnts_record(pageT    *pPage,
				    cellNumT mcellNum, 
				    bsMRT    *pRecord);

static int correct_mcell_header(pageT	  *pPage,
				volNumT  volNum,
				pageNumT pageNum,
				cellNumT cellNum,
				domainT  *domain);
 
static int correct_mcell_records(domainT  *domain,
				 pageT	   *pPage, 
				 volNumT  volNum,
				 pageNumT pageNum,
				 cellNumT mcellNum,
				 int      *isPrimary,
				 int      *isFsstat);

static int correct_bsr_attr_record(pageT    *pPage, 
				   cellNumT mcellNum, 
				   bsMRT    *pRecord);

static int correct_bsr_xtra_xtnts_record(domainT *domain,
					 volNumT volNum,
					 pageT   *pPage, 
					 cellNumT mcellNum, 
					 bsMRT	  *pRecord);

static int correct_bsr_shadow_xtnts_record(domainT *domain,
					   volNumT  volNum,
					   pageT   *pPage, 
					   cellNumT mcellNum, 
					   bsMRT   *pRecord);

static int correct_bsr_mcell_free_list_record(domainT *domain,
					      pageT   *pPage, 
					      cellNumT mcellNum, 
					      bsMRT   *pRecord);

static int correct_bsr_bfs_attr_record(domainT *domain,
				       pageT	*pPage, 
				       cellNumT mcellNum, 
				       bsMRT	*pRecord);

static int correct_bsr_def_del_mcell_list_record(domainT *domain,
						 pageT	  *pPage, 
						 cellNumT mcellNum, 
						 bsMRT	  *pRecord);

static int correct_bsr_bf_inherit_attr_record(pageT *pPage, 
					      cellNumT mcellNum, 
					      bsMRT *pRecord);

static int correct_bsr_rsvd17_record(pageT *pPage, 
				     cellNumT mcellNum, 
				     bsMRT *pRecord);

static int correct_bsr_bfs_quota_attr_record(domainT *domain,
					     pageT   *pPage, 
					     cellNumT mcellNum, 
					     bsMRT   *pRecord);

static int correct_bsr_acl_record(domainT *domain,
				  pageT   *pPage, 
				  cellNumT mcellNum, 
				  bsMRT   *pRecord);

static int correct_bmtr_fs_dir_index_file(pageT *pPage, 
					  cellNumT mcellNum, 
					  bsMRT *pRecord);

static int correct_bmtr_fs_index_file_record(pageT *pPage, 
					     cellNumT mcellNum, 
					     bsMRT *pRecord);

static int correct_bmtr_fs_time_record(domainT  *domain,
				       pageT    *pPage, 
				       cellNumT mcellNum, 
				       bsMRT    *pRecord);

static int correct_bmtr_fs_undel_dir_record(pageT    *pPage, 
					    cellNumT mcellNum, 
					    bsMRT    *pRecord);

static int correct_bmtr_fs_data_record(pageT    *pPage, 
				       cellNumT mcellNum, 
				       bsMRT    *pRecord);

static int correct_bmtr_fs_stat_record(pageT    *pPage, 
				       cellNumT mcellNum, 
				       bsMRT    *pRecord);

static int correct_mcell_record_bcnt(domainT  *domain, 
				     pageT    *pPage, 
				     cellNumT mcellNum,
				     bsMRT    *pRecord, 
				     int      currentSize);


/*
 * Function Name: free_mcell
 *
 * Description: This function removes (frees) an mcell from a BMT page.
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
int 
free_mcell(domainT *domain, 
	   volNumT volNum, 
	   pageT *pPage, 
	   cellNumT mcellNum)
{
/* TODO-ODS: Could use volume number from BMT header (bmtVdIndex) instead of
 *           passing in volNum. This assumes that the BMT header is verified
 *           before this routine is called.
 */
    char     *funcName = "free_mcell";
    bsMPgT   *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT    *pMcell;    /* The mcell we are working on. */
    bsMRT    *pRecord;   /* The record we are working on. */
    volumeT  *pVolume;
    int      status;
    int      x; 
    lbnNumT  lbn;
    xtntNumT extentSize;
    tagNumT  tag;
    tagNumT  setTag;
    pageNumT pageNum;
    lbnNumT  pageLbn;
    lbnNumT  rbmtLbn;
    volNumT  rbmtVol;
    xtntNumT numExtents;
    int      is_rbmt;
    fobtoLBNT     *pExtents;
    bsXtntRT      *pXtnt;
    bsXtraXtntRT  *pXtra;
    mcidT	  mcellId;
    mcellNodeT    *mcell;
    mcellNodeT    tempMcell;
    nodeT         *node;

    /*
     * Init variables.
     */
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pageNum  = pBMTPage->bmtPageId;
    pageLbn  = pPage->pageLBN;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    pVolume  = &(domain->volumes[volNum]);
    tag      = pMcell->mcTag.tag_num;
    setTag   = pMcell->mcBfSetTag.tag_num;

    /*
     * Save the mcell ID for use within the loop.
     */
    mcellId.volume = volNum;
    mcellId.page = pageNum;
    mcellId.cell = mcellNum;

    /*
     * Delete mcell node from skip list.
     */
    tempMcell.mcid = mcellId;
    mcell = &tempMcell;

    status = delete_node(domain->mcellList, (void *)&mcell);
    if (SUCCESS == status) {
	free(mcell);
    } else if (NOT_FOUND != status) {
	return status;
    }

    if ((pMcell->mcTag.tag_num == 0) && (pMcell->mcBfSetTag.tag_num == 0)) {
	/*
	 * This mcell is already free, don't need to do anything.
	 */
	return SUCCESS;
    }

    /*
     * Check the next chain if it points to a valid mcell.
     * If it does then clear the found pointer.
     */
    tempMcell.mcid  = pMcell->mcNextMCId;
    mcell = &tempMcell;

    /*
     * TODO-ODS: Could save some time by checking if pMcell->mcNextMCId
     *           is NIL which indicates there is no next mcell.
     */
    status = find_node(domain->mcellList, (void **)&mcell, &node);
    if (SUCCESS == status) {
	MC_CLEAR_FOUND_PTR(mcell->status);
    } else if (NOT_FOUND != status) {
	return status;
    }

    /*
     * Now free the extents from the in-memory SBM (assuming it exists).
     */
    if (pVolume->volSbmInfo != NULL) {
        while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    switch (pRecord->type) {
	      case (BSR_XTNTS):
		  pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		  
		  /*
		   * For both ODS types check if the chain points to a 
		   * valid mcell.  If it does the clear the found
		   * pointer.
		   */
		  tempMcell.mcid  = pXtnt->chainMCId;
		  mcell = &tempMcell;

		  status = find_node(domain->mcellList, 
				     (void **)&mcell, &node);
		  if (SUCCESS == status) {
		      MC_CLEAR_FOUND_PTR(mcell->status);
		  } else if (NOT_FOUND != status) {
		      return status;
		  }

		  /*
		   * Loop through all the extents in this record.
		   */
		  for (x = 0; 
		       x < (pXtnt->xCnt - 1) && x < (BMT_XTNTS - 1);
		       x++) {
		      lbn = pXtnt->bsXA[x].bsx_vd_blk;
		      extentSize = (pXtnt->bsXA[x + 1].bsx_fob_offset -
				    pXtnt->bsXA[x].bsx_fob_offset);

		      status = remove_extent_from_sbm(domain, volNum, lbn, 
						      extentSize, tag,
						      setTag,
						      &mcellId, x);
		      if (status != SUCCESS) {
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
		      lbn = pXtra->bsXA[x].bsx_vd_blk;
		      extentSize = (pXtra->bsXA[x + 1].bsx_fob_offset -
				    pXtra->bsXA[x].bsx_fob_offset);

		      status = remove_extent_from_sbm(domain, volNum, lbn, 
						      extentSize, tag, setTag,
						      &mcellId, x);
		      if (status != SUCCESS) {
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
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
	} /* end while record exist */
    } /* end if sbm array exists */

    /*
     * Mark the mcell as free.
     * - clear (bzero) the entire mcell header so that reserved or new
     *   fields don't have to be removed or added with future changes
     *   NOTE: also inits nextMCId to NIL which is set below with a valid
     *         mcellId unless this is the only free mcell on the page
     */
    bzero(pMcell, sizeof(bs_mcell_hdr_t));

    /*
     * Clear the mcell record header.
     */
    pRecord = (bsMRT *)pMcell->bsMR0;
    pRecord->bCnt = 0;
    pRecord->type = BSR_NIL;

    /*
     * Put this MCELL on the head of this page's free list.
     * - if freeMcellCnt NOT zero then set nextMCId, otherwise it has
     *   already been set to NIL via bzero above to clear mcell header 
     */
    if (pBMTPage->bmtFreeMcellCnt != 0) {
        pMcell->mcNextMCId.volume = volNum;
        pMcell->mcNextMCId.page = pageNum;
        pMcell->mcNextMCId.cell = pBMTPage->bmtNextFreeMcell;
    }

    pBMTPage->bmtNextFreeMcell = mcellNum;

    /*
     * For a special case below we need to know if this page is in the
     * RBMT or BMT for ODSv4.
     */
    is_rbmt = FALSE;

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
	    is_rbmt = TRUE;
	}
    } /* end if convert_page_to_lbn was successful */

    /*
     * Check if this PAGE needs to be added to the mcell free list?
     * This is done by checking if this page currently has not free
     * mcells.  As we are adding one it will need to be added to the
     * list.
     */
    if (is_rbmt == TRUE) {
	/*
	 * Special case.  Can't be on the free list.
	 */
    } else if (pBMTPage->bmtFreeMcellCnt == 0) {
	pageNumT headPageNum; /* Page the head of the free list is on */
	lbnNumT  headLbn;
	volNumT  headVol;
	pageT  *pHeadPage;
	bsMPgT *pHeadBMTPage;

	bsMcellFreeListT *headRecord; /* mcell free list record */

	/*
	 * Init variables
	 */
	pExtents   = pVolume->volRbmt->bmtLBN;
	numExtents = pVolume->volRbmt->bmtLBNSize;

	/*
	 * pBMTPage->bmtNextFreePg is what we need to set.
	 */
	headPageNum = 0;

	status = convert_page_to_lbn(pExtents, numExtents, headPageNum, 
				     &headLbn, &headVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
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
	    if (status != SUCCESS) {
		return status;
	    }
	}

	pHeadBMTPage = (bsMPgT *)pHeadPage->pageBuf;
	pMcell  = &(pHeadBMTPage->bsMCA[0]);
	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (BSR_MCELL_FREE_LIST == pRecord->type) {
		headRecord = (bsMcellFreeListT *)((char *)pRecord + 
						  sizeof(bsMRT));

		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_179,
						   "Added page to the free mcell list.\n"));
		if (status != SUCCESS) {
		    return status;
		}

		pBMTPage->bmtNextFreePg = headRecord->headPg;
		headRecord->headPg = pageNum;
		break;
	    }
	    /*
	     * Set pRecord to next record  of pMcell.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
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
	    if (status != SUCCESS) {
		return status;
	    }
	} /* end check if pages are the same */
    } /* end if adding to free page list */

    /*
     * Increment the number of free mcells on this page.
     */
    pBMTPage->bmtFreeMcellCnt = pBMTPage->bmtFreeMcellCnt + 1;

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
int 
delete_mcell_record(domainT *domain, 
		    pageT *pPage, 
		    cellNumT mcellNum, 
		    bsMRT *pRecord)
{
    char     *funcName = "delete_mcell_record";
    bsMPgT   *pBMTPage;  /* The current RBMT page we are working on */
    int      status;
    pageNumT pageNum;
    volNumT volNum;
    int      bCnt;
    int      type;
    bsXtntRT *pXtnt;
    mcellNodeT *mcell;
    mcellNodeT tempMcell;
    nodeT      *node;
    
    /*
     * Initialize variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;    
    pageNum  = pBMTPage->bmtPageId;
    bCnt     = pRecord->bCnt;
    type     = pRecord->type;

    /*
     * Find the record we want.
     */
    switch (pRecord->type) {
	case (BMTR_FS_STAT):
	    tempMcell.mcid.volume = volNum;
	    tempMcell.mcid.page = pageNum;
	    tempMcell.mcid.cell = mcellNum;
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
	    tempMcell.mcid  = pXtnt->chainMCId;
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
    pRecord->type = BSR_NIL;

    status = add_fixed_message(&(pPage->messages), BMT,
			       catgets(_m_catd, S_FSCK_1, FSCK_180,
				       "Truncated mcell (%d,%ld,%d) type %d, bCnt %d\n"),
			       volNum, pageNum, mcellNum, type, bCnt);
    if (status != SUCCESS) {
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
int 
clear_mcell_record(domainT *domain, 
		   pageT *pPage, 
		   cellNumT mcellNum, 
		   int recNum)
{
    char     *funcName = "clear_mcell_record";
    bsMPgT   *pBMTPage; /* The current BMT page we are working on */
    bsMCT    *pMcell;   /* The current BMT mcell we are working on */
    bsMRT    *pRecord;  /* The current BMT record we are working on */
    bsMRT    *pNextRecord;  /* The next record in the mcell */
    mcidT   mcellId;
    int      x;
    int      status;
    int      memSize;
    pageNumT pageNum;
    volNumT  volNum;
    int      bCnt;
    int      type;
    int      currentRecNum;
    fobNumT  extentSize;
    tagNumT  tag;
    tagNumT  setTag;
    lbnNumT  lbn;
    bsXtntRT      *pXtnt;
    bsXtraXtntRT  *pXtra;
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
    pageNum    = pBMTPage->bmtPageId;
    tag        = pMcell->mcTag.tag_num;
    setTag     = pMcell->mcBfSetTag.tag_num;

    /*
     * Initialize variables.
     */
    mcellId.volume = volNum;
    mcellId.page = pageNum;
    mcellId.cell = mcellNum;
    currentRecNum = 0;

    /*
     * Loop through record to find the record we want,
     * Process the record, the break out of loop.
     */
    while ((pRecord->type != BSR_NIL) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	currentRecNum++;
	if (currentRecNum < recNum) {
	    /*
	     * Set pRecord to next record  of pMcell.
	     */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
	    continue; /* skip to next record in while loop */
	}

	/*
	 * Break out of while loop with pRecord pointing to the record
	 * we are working on.
	 */
	break;
    } /* end while record exist */

    if ((pRecord->type == BSR_NIL) || 
	(pRecord->bCnt == 0) ||
	(pRecord >= ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	/*
	 * Internal Error: Stepped off the end of the mcell with out
	 * finding the record we want.
	 */
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_181, 
			 "Internal Error: Unable to find mcell (%d,%ld,%d)'s record %d.\n"),
		 volNum, pageNum, mcellNum, recNum);
	return FAILURE;
    }


    /*
     * Found the record we want.
     */
    switch (pRecord->type) {
	case (BMTR_FS_STAT):
	    tempMcell.mcid.volume = volNum;
	    tempMcell.mcid.page = pageNum;
	    tempMcell.mcid.cell = mcellNum;
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
	    tempMcell.mcid = pXtnt->chainMCId;
	    mcell = &tempMcell;

	    status = find_node(domain->mcellList, (void **)&mcell, &node);
	    if (status == SUCCESS) {
		MC_CLEAR_FOUND_PTR(mcell->status);
	    } else if (NOT_FOUND != status) {
		return status;
	    }

	    /*
	     * Loop through all the extents in this record.
	     */
	    for (x = 0; 
		 x < (pXtnt->xCnt - 1) && x < (BMT_XTNTS - 1);
		 x++) {
		lbn = pXtnt->bsXA[x].bsx_vd_blk;
		extentSize = (pXtnt->bsXA[x + 1].bsx_fob_offset -
			      pXtnt->bsXA[x].bsx_fob_offset);

		status = remove_extent_from_sbm(domain, volNum, 
						lbn, extentSize, 
						tag, setTag,
						&mcellId, x);
		if (status != SUCCESS) {
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
		lbn = pXtra->bsXA[x].bsx_vd_blk;
		extentSize = (pXtra->bsXA[x + 1].bsx_fob_offset -
			      pXtra->bsXA[x].bsx_fob_offset);

		status = remove_extent_from_sbm(domain, volNum, lbn, 
						extentSize, tag, 
						setTag, &mcellId, x);
		if (status != SUCCESS) {
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
			     roundup(pRecord->bCnt, sizeof(uint64_t))); 

    /*
     * Loop through all the remaining records.
     */
    memSize = 0;
    while ((pNextRecord->type != BSR_NIL) &&
	   (pNextRecord->bCnt != 0) &&
	   (pNextRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	memSize += roundup(pNextRecord->bCnt, sizeof(uint64_t));

	pNextRecord = (bsMRT *) (((char *)pNextRecord) + 
				 roundup(pNextRecord->bCnt, sizeof(uint64_t)));
    }
    
    if (memSize != 0) {
	/*
	 * If there are more record(s) move them forward.
	 */
	char *destination;
	char *source;

	destination = (char *)pRecord;
	source      = (((char *)pRecord) + 
		       roundup(pRecord->bCnt, sizeof(uint64_t)));
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
    pRecord->type = BSR_NIL;

    status = add_fixed_message(&(pPage->messages), BMT,
			       catgets(_m_catd, S_FSCK_1, FSCK_182, 
				       "Removed mcell (%d,%ld,%d) record %d : type %d, bCnt %d.\n"),
			       volNum, pageNum, mcellNum, recNum, type, bCnt);
    if (status != SUCCESS) {
	return status;
    }

    return SUCCESS;
} /* end clear_mcell_record */


/*
 * Function Name: correct_bmt_mcell_data
 *
 * Description: This routine checks and fixes the mcell data in the
 *              BMT and populate the mcell skiplist. This routine is
 *              called before the root tag file and tag file are read
 *              so it cannot validate set tags or tags.
 *
 * Input parameters:
 *     domain: The domain being worked on.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, FAILURE, or NO_MEMORY 
*/
int 
correct_bmt_mcell_data(domainT *domain)
{
    char *funcName = "correct_bmt_mcell_data";
    int  fixedBMT; /* Flag set if we fixed anything in the BMT. */
    lbnNumT nextLBN;  /* LBN of the start of the next BMT page. */
    lbnNumT currLBN;  /* Logical block number of the start of the BMT page. */
    int  inExtent; /* Indicates that the page is not the first in an extent. */
    int  *bmtPageStatus; /* Array for each BMT page which indicate if the page
			    is full and if it is on the free list.*/
    mcellUsage freeMcell[BSPG_CELLS]; /* Array which indicate if the mcell is 
					 FREE, USED, and then used to see if a
					 link to it was used (ONLIST). */
    int  freeListCount; /* number of mcells in the free mcell list*/
    int  linksReset; /* Have we reset the links in the mcell free list*/
    pageNumT pageNumber; /* The number of the BMT page being processed. */
    pageNumT nextPageNumber;
    volNumT volNum;
    int  modified;
    pageNumT page;
    cellNumT mcell;
    volNumT vol;
    int  status;
    int  isPrimary;
    int  isFsstat;
    pageT  *pPage;  /* The current cache page we are working on */
    bsMPgT *bmtPage; /* The current BMT page we are working on */
    bsMCT  *pMcell;  /* The current BMT mcell we are working on */
    fobtoLBNT *bmtLBNArray;
    int  bmtLBNSize;
    xtntNumT bmtXtnt;
    volumeT *volume;


    /* for each volume in the storage domain... */

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

	if (domain->volumes[volNum].volFD == -1) {
	    continue;
	}
	
	/*
	 * Init per volume variables.
	 */
	fixedBMT = FALSE;
	pageNumber = 0;
	volume = &(domain->volumes[volNum]);
	bmtLBNArray = volume->volRbmt->bmtLBN;
	bmtLBNSize  = volume->volRbmt->bmtLBNSize;
	
	bmtPageStatus = (int *)
	                ffd_calloc(FOB2PAGE(bmtLBNArray[bmtLBNSize].fob), 
				   sizeof(int));
	if (bmtPageStatus == NULL) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FSCK_1, FSCK_109, 
			     "BMT page status"));
	    return NO_MEMORY;
	}

	/* for each extent in the BMT... */

	for (bmtXtnt = 0; bmtXtnt < bmtLBNSize; bmtXtnt++) {

	    inExtent = FALSE;

	    /* for each page in the current extent... */

	    for (pageNumber = FOB2PAGE(bmtLBNArray[bmtXtnt].fob);
		 pageNumber < FOB2PAGE(bmtLBNArray[bmtXtnt+1].fob);
		 pageNumber++) {

		/* calculate the LBN for the start of this page within
		   the extent */

		currLBN = (bmtLBNArray[bmtXtnt].lbn + 
			   (ADVFS_METADATA_PGSZ_IN_BLKS * 
			    (pageNumber - FOB2PAGE(bmtLBNArray[bmtXtnt].fob))));

		if (pageNumber == FOB2PAGE(bmtLBNArray[bmtXtnt + 1].fob) - 1) {
		    nextLBN = bmtLBNArray[bmtXtnt + 1].lbn;
		    nextPageNumber = FOB2PAGE(bmtLBNArray[bmtXtnt + 1].fob);
		} else {
		    nextLBN = currLBN + ADVFS_METADATA_PGSZ_IN_BLKS;
		    nextPageNumber = pageNumber + 1;
		}

		/* read a page of BMT data */

		modified = FALSE;
		status = read_page_from_cache(bmtLBNArray[bmtXtnt].volume, 
					      currLBN, &pPage);
		if (status != SUCCESS) {
		    return status;
		} 
		bmtPage = (bsMPgT *)pPage->pageBuf;

		/* make sure it's a legit BMT page */

		status = validate_bmt_page(domain, pPage, pageNumber, FALSE);
		if (status != SUCCESS) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FSCK_1, FSCK_110,
				     "Invalid BMT page on volume %s page %ld, reinitializing it.\n"),
			     volume->volName, pageNumber);
		    status = add_fixed_message(&(pPage->messages), BMT,
					       catgets(_m_catd, S_FSCK_1, FSCK_111, 
						       "Reinitialized BMT page %ld.\n"),
					       pageNumber);
		    if (status != SUCCESS) {
			return status;
		    }

		    /*
		     * Bogus BMT page, init page header to empty.
		     */

		    bzero(bmtPage, sizeof(bsMPgT));
                    bmtPage->bmtFsCookie = domain->fsCookie;
		    bmtPage->bmtMagicNumber == BMT_MAGIC;
		    bmtPage->bmtPageId = pageNumber;
		    bmtPage->bmtNextFreePg = 0;
		    bmtPage->bmtNextFreeMcell = 0;
		    bmtPage->bmtFreeMcellCnt = BSPG_CELLS;
                    bmtPage->bmtVdIndex = volNum;
                    bmtPage->bmtVdCookie = volume->vdCookie;

		    /* link all the mcells together */

		    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {

			pMcell = &(bmtPage->bsMCA[mcell]);
                        pMcell->mcNextMCId.volume = volNum;
			pMcell->mcNextMCId.page = pageNumber;
			pMcell->mcNextMCId.cell = mcell + 1;
		    }

		    pMcell = &(bmtPage->bsMCA[LAST_MCELL]);
                    pMcell->mcNextMCId.volume = 0;
		    pMcell->mcNextMCId.page = 0;
		    pMcell->mcNextMCId.cell = 0;

		    fixedBMT = TRUE;

		    status = release_page_to_cache(bmtLBNArray[bmtXtnt].volume,
						   currLBN, pPage, 
						   TRUE, FALSE);
		    if (status != SUCCESS) {
			return status;
		    }
		    continue;

		} /* end if: invalid BMT page */

		/*
		 * We have a legit BMT page, look for header errors
		 * and fix them.
		 */

		status = correct_bmt_page_header(pageNumber, volNum,
						 inExtent, nextLBN,
						 nextPageNumber,
						 pPage, domain,
						 FALSE);
		if (status != SUCCESS) {
		    if (status == FIXED) {
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
		 * Now walk the mcells fixing any problems.
		 */
		for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
		    status = correct_mcell_header(pPage, volNum,
						  pageNumber, 
						  mcell, domain);
		    if (status != SUCCESS) {
			if (status == FIXED) {
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

		    if (pMcell->mcTag.tag_num == 0  &&
			pMcell->mcBfSetTag.tag_num == 0  &&
			!(mcell == 0 && pageNumber == 0)) {
		        freeMcell[mcell] = FREE;
		    } else {
			mcellNodeT *pMcellNode;

			freeMcell[mcell] = USED;
			status = correct_mcell_records(domain, pPage, 
						       volNum, pageNumber,
						       mcell, &isPrimary,
						       &isFsstat);

			if (status != SUCCESS) {
			    if (status == FIXED) {
				modified = TRUE;
			    } else if (status == DELETED) {
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
			if (pMcellNode == NULL) {
			    writemsg(SV_ERR | SV_LOG_ERR, 
				     catgets(_m_catd, S_FSCK_1, FSCK_10, 
					     "Can't allocate memory for %s.\n"),
				     catgets(_m_catd, S_FSCK_1, FSCK_112, 
					     "mcell node"));
			    return NO_MEMORY;
			}

			pMcellNode->mcid.volume = volNum;
			pMcellNode->mcid.page = pageNumber;
			pMcellNode->mcid.cell = mcell;
			pMcellNode->status = 0;
			pMcellNode->tagSeq = pMcell->mcTag;
			pMcellNode->setTag = pMcell->mcBfSetTag;

			if (isPrimary == TRUE) {
			    MC_SET_PRIMARY(pMcellNode->status);
			}

			if (isFsstat == TRUE) {
			    MC_SET_FSSTAT(pMcellNode->status);
			}

			if (mcell == 0 && pageNumber == 0) {
			    /*
			     * Special case no one pointes to this mcell.
			     */
			    MC_SET_FOUND_PTR(pMcellNode->status);
			}

			status = insert_node(domain->mcellList, 
					     pMcellNode);
			if (status != SUCCESS) {
			    return status;
			}
		    } /* end if free or full */
		} /* end mcell loop */

		/*
		 * Now we are going to check the free mcell list.
		 */
		freeListCount = 0;
		linksReset    = FALSE;
                mcell         = bmtPage->bmtNextFreeMcell;
                if ((bmtPage->bmtNextFreeMcell == 0) &&
                    (bmtPage->bmtFreeMcellCnt == 0)) {
                    page = 0;
                    vol = 0;
                } else {
                    page = bmtPage->bsMCA[mcell].mcNextMCId.page;
                    vol = bmtPage->bsMCA[mcell].mcNextMCId.volume;
                }

		/*
		 * Loop until the end of the list (0,0,0)
		 */
		while (page != 0 || mcell != 0 || vol != 0) {
		    if (freeMcell[mcell] == FREE) {
			freeMcell[mcell] = ONLIST;
			freeListCount++;
			pMcell = &(bmtPage->bsMCA[mcell]);
			page  = pMcell->mcNextMCId.page;
                        vol = pMcell->mcNextMCId.volume;
			mcell = pMcell->mcNextMCId.cell;
		    } else {
			/*
			 * There is a free mcell pointing to an active
			 * one or there is a loop in the free mcell 
			 * list, so reset the links on the free mcells.
			 * Active mcells pointing to free ones will be
			 * caught in correct_nodes.
			 */
			writemsg(SV_VERBOSE | SV_LOG_FOUND,
				 catgets(_m_catd, S_FSCK_1, FSCK_113, 
					 "Invalid free mcell links on volume %s BMT page %ld.\n"),
				 volume->volName, pageNumber);
			status = reset_free_mcell_links(pPage, freeMcell);
			if (status != SUCCESS) {
			    return status;
			}
			    
			status = add_fixed_message(&(pPage->messages),
						   BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_114, 
							   "Reset free mcell links on BMT page %ld.\n"),
						   pageNumber);
			if (status != SUCCESS) {
			    return status;
			}

			linksReset = TRUE;
			modified   = TRUE;
			mcell      = 0;
			page       = 0;
                        vol        = 0;
		    } /* end of if on FREE list */
		} /* end of while loop on free list */

		if (linksReset == FALSE) {
		    if (freeListCount != bmtPage->bmtFreeMcellCnt) {
			writemsg(SV_VERBOSE | SV_LOG_FOUND, 
				 catgets(_m_catd, S_FSCK_1, FSCK_113,
					 "Invalid free mcell links on volume %s BMT page %ld.\n"),
				 volume->volName, pageNumber);
			/*
			 * Not all of the free mcells were in the free 
			 * list, so reset all of the links.
			 */
			status = reset_free_mcell_links(pPage, freeMcell);
			if (status != SUCCESS) {
			    return status;
			}
			linksReset = TRUE;
			modified   = TRUE;
			status = add_fixed_message(&(pPage->messages),
						   BMT,
						   catgets(_m_catd, S_FSCK_1, FSCK_114, 
							   "Reset free mcell links on BMT page %ld.\n"),
						   pageNumber);
			if (status != SUCCESS) {
			    return status;
			}
		    }
		}

		if (bmtPageStatus != NULL) {
		    if (bmtPage->bmtFreeMcellCnt == 0) {
			bmtPageStatus[pageNumber] = BMTPAGE_FULL;
		    } else {
			bmtPageStatus[pageNumber] = BMTPAGE_FREE;
		    }
		}

		status = release_page_to_cache(bmtLBNArray[bmtXtnt].volume,
					       currLBN, pPage, 
					       modified, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		if (modified == TRUE) {
		    fixedBMT = TRUE;
		}
		modified = FALSE;
		inExtent = TRUE;
	    } /* End page number loop */
	} /* End extent loop */

	status = correct_free_mcell_list(volume, bmtPageStatus);
	if (status != SUCCESS) {
	    if (status == FIXED) {
		modified = TRUE;
		fixedBMT = TRUE;
	    } else {
		return status;
	    }
	}
	free (bmtPageStatus);
    } /* End active volume */
    
    if (fixedBMT == TRUE) {
	writelog(SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_50,
			 "Fixed errors in BMT mcell data.\n"));
	return FIXED;
    }
    return SUCCESS;
}/* end correct_bmt_mcell_data */


/*
 * Function Name: correct_bmt_page_header
 *
 * Description: This routine checks and fixes a BMT page header.
 *
 * Input parameters:
 *     pageNumber: The BMT page number that this page should be.
 *     volNum: The volume we are working on.
 *     inExtent: Indicating that we are not at the first block of an extent.
 *     nextLBN: The LBN of the next page to be processed.
 *     nextPageNumber: Needed for special case.
 *     page: The BMT page to process.
 *     domain: The domain being worked on.
 *     is_rbmt: Is this bmt page in the RBMT
 *
 * Output parameters: N/A 
 *
 * Returns: FIXED, SUCCESS, or NO_MEMORY
 */
int 
correct_bmt_page_header(pageNumT pageNumber, 
			volNumT volNum, 
			int inExtent, 
			lbnNumT nextLBN, 
			pageNumT nextPageNumber,
			pageT *pPage, 
			domainT *domain, 
			int is_rbmt)
{
    char   *funcName = "correct_bmt_page_header";
    cellNumT freeCount; /* Count of free mcells on page. */
    bsMPgT *bmtPage;  /* The current BMT page we are working on */
    bsMCT  *pMcell;   /* The current BMT mcell we are working on */
    int    modified;
    int    status;
    cellNumT mcell;
    pageNumT maxBmtFob;
    volumeT *volume;
    int magicNum;

    /*
     * Init Variables.
     */
    modified = FALSE;
    bmtPage = (bsMPgT *)pPage->pageBuf;
    volume = &(domain->volumes[volNum]);

    if (is_rbmt != TRUE) {
	maxBmtFob = volume->volRbmt->bmtLBN[volume->volRbmt->bmtLBNSize].fob;
    }

    /* 
     * validate/fix magic number 
     */

    if (is_rbmt) {
	magicNum = RBMT_MAGIC;
    } else {
	magicNum = BMT_MAGIC;
    }

    if (bmtPage->bmtMagicNumber != magicNum) {

	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_918, 
			 "Invalid Magic Number on volume %s %s page %ld.\n"),
		 volume->volName, is_rbmt ? "RBMT" : "BMT", pageNumber);

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_919,
					   "Modified %s page %ld Magic Number from 0x%x to 0x%x.\n"),
				   is_rbmt ? "RBMT" : "BMT", pageNumber, 
				   bmtPage->bmtMagicNumber, magicNum);
	if (status != SUCCESS) {
	    return status;
	}
	bmtPage->bmtMagicNumber = magicNum;
	modified = TRUE;
    }	

    /* 
     * validate/fix fsCookie (domain's original identity)
     */

    if (!(COOKIE_EQ(bmtPage->bmtFsCookie, domain->fsCookie))) {

	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_925, 
			 "Invalid fsCookie on volume %s %s page %ld.\n"),
		 is_rbmt ? "RBMT" : "BMT", volume->volName, pageNumber);

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_924,
					   "Modified %s page %ld fsCookie from %ld.%ld to %ld.%ld\n"),
				   is_rbmt ? "RBMT" : "BMT", pageNumber, 
                                   bmtPage->bmtFsCookie.id_sec, 
				   bmtPage->bmtFsCookie.id_usec,
				   domain->fsCookie.id_sec, 
				   domain->fsCookie.id_usec);
	if (status != SUCCESS) {
	    return status;
	}
	bmtPage->bmtFsCookie = domain->fsCookie;
	modified = TRUE;
    }	

    /* 
     * validate/fix ODS version info (RBMT only)
     */

    if (is_rbmt && 
	!(VERSION_EQ(bmtPage->bmtODSVersion, domain->ODSVersion))) {

	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_116, 
			 "Invalid ODS version on volume %s %s page %ld.\n"),
		 volume->volName, is_rbmt ? "RBMT" : "BMT", pageNumber);

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_117,
					   "Modified %s page %ld ODS version from %d/%d to %d/%d.\n"),
				   is_rbmt ? "RBMT" : "BMT", pageNumber, 
				   bmtPage->bmtODSVersion.odv_major, 
				   bmtPage->bmtODSVersion.odv_minor, 
				   domain->ODSVersion.odv_major, 
				   domain->ODSVersion.odv_minor);
	if (status != SUCCESS) {
	    return status;
	}
        bmtPage->bmtODSVersion = domain->ODSVersion;
	modified = TRUE;
    }

    if (bmtPage->bmtVdIndex != volNum) {
	/* check/correct the volume index */
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_108, 
			 "Invalid vol index on volume %s's %s page %ld.\n"),
		 volume->volName, is_rbmt ? "RBMT" : "BMT", pageNumber);
	    
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_120, 
					   "Modified vol %s's vol index from %d to %d on %s page %ld.\n"),
				   volume->volName, bmtPage->bmtVdIndex,
				   volNum, is_rbmt ? "RBMT" : "BMT", 
				   pageNumber);
	if (status != SUCCESS) {
	    return status;
	}

	bmtPage->bmtVdIndex = volNum;
	modified = TRUE;
    }

    if (bmtPage->bmtPageId != pageNumber) { 
       /*
	* if the page is within an extent, then number is corrupt, so fix it.
	*/
        if (inExtent == TRUE) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_118, 
			     "Invalid page number on volume %s's %s page %ld.\n"),
		     volume->volName, is_rbmt ? "RBMT" : "BMT", pageNumber);
	    
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_119, 
					       "Modified %s page %ld's number from %ld to %ld.\n"),
				       is_rbmt ? "RBMT" : "BMT", pageNumber, 
				       bmtPage->bmtPageId, pageNumber);
	    if (status != SUCCESS) {
		return status;
	    }

	    bmtPage->bmtPageId = pageNumber;
	    modified = TRUE;
	} else if (nextLBN == XTNT_TERM) {
	    /*
	     * If this is the last extent, handle that here.
	     */

	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_118,
			     "Invalid page number on volume %s's %s page %ld.\n"),
		     volume->volName, is_rbmt ? "RBMT" : "BMT", pageNumber);

	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_119, 
					       "Modified %s page %ld's number from %ld to %ld.\n"),
				       is_rbmt ? "RBMT" : "BMT", pageNumber, 
				       bmtPage->bmtPageId, pageNumber);
	    if (status != SUCCESS) {
		return status;
	    }
	    bmtPage->bmtPageId = pageNumber;
	    modified = TRUE;
	} else {
	    pageT *pTempPage;
	    bsMPgT *bmtTempPage; 
	    /*
	     * If this page is at the start of an extent, there could be 
	     * corruption in the extent, so see if the next page has the next 
	     * page number in sequence. if so, then set the page number on 
	     * this page.
	     */

	    status = read_page_from_cache(volNum, nextLBN, &pTempPage);
	    if (status != SUCCESS) {
	        return status;
	    } 

	    bmtTempPage = (bsMPgT *)pTempPage->pageBuf;

	    if (bmtTempPage->bmtPageId == pageNumber + 1) { 
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_118, 
				 "Invalid page number on volume %s's %s page %ld.\n"),
			 volume->volName, is_rbmt ? "RBMT" : "BMT", 
			 pageNumber);
		
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_119, 
						   "Modified %s page %ld's number from %ld to %ld.\n"),
					   is_rbmt ? "RBMT" : "BMT", 
					   pageNumber, bmtPage->bmtPageId,
					   pageNumber);
		if (status != SUCCESS) {
		    return status;
		}
	        bmtPage->bmtPageId = pageNumber;
		modified = TRUE;
	    } else { 

	        /* 
		 * At this point, we can't tell if we just have
		 * corrupted page numbers on multiple pages, or if there
		 * are actually missing BMT pages. In either case, we
		 * just exit here. 
		 * 
		 * FUTURE: We may want to search the disk looking for
		 * missing BMT pages or allocate some empty pages to
		 * fill in the missing ones.
		 */
 		writemsg(SV_ERR | SV_LOG_ERR,
			 catgets(_m_catd, S_FSCK_1, FSCK_121, 
				 "Volume %s %s page %ld header's page ID can not be fixed.\n"), 
			 volume->volName, is_rbmt ? "RBMT" : "BMT", 
			 pageNumber);

	        corrupt_exit(NULL);
	    }
	    status = release_page_to_cache(volNum, nextLBN, pTempPage,
					   FALSE, FALSE);
	    if (status != SUCCESS) {
	        return status;
	    }
	}/* end if start of extent or middle */
    } /* end if page number is incorrect */


    /*
     * Check if nextFreePg is out of bounds.  For RBMT the next page
     * link is not used so it should always be ZERO.
     */
    if (is_rbmt == TRUE) {
	if (bmtPage->bmtNextFreePg != 0) {
	    writemsg(SV_VERBOSE | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_956, 
			     "Volume %s RBMT page %ld's nextfreePg should be 0, not %ld.\n"), 
			 volume->volName, bmtPage->bmtNextFreePg);
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_122, 
					       "Modified RBMT page %ld's nextFreePg from %ld to %ld.\n"),
				       pageNumber, bmtPage->bmtNextFreePg, 0);
	    if (status != SUCCESS) {
		return status;
	    }
	    bmtPage->bmtNextFreePg = NULL;
	    modified = TRUE;
	}
    } else {
	if (bmtPage->bmtNextFreePg != BMT_PG_TERM  &&
	    bmtPage->bmtNextFreePg > FOB2PAGE(maxBmtFob)) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_123, 
					       "Modified BMT page %ld's nextFreePg from %ld to %ld.\n"),
				       pageNumber, bmtPage->bmtNextFreePg,
				       PAGE_TERM);
	    if (status != SUCCESS) {
		return status;
	    }
	    bmtPage->bmtNextFreePg = PAGE_TERM;
	    modified = TRUE;
	}
    }

    /*
     * Check if the free mcell count in the header is correct.
     *
     * Future: We may need to special case PL_PAGE.
     */
    freeCount = 0;

    for (mcell = 0; mcell < BSPG_CELLS; mcell++) {
        pMcell = &(bmtPage->bsMCA[mcell]);
	if ((pMcell->mcTag.tag_num == 0) && (pMcell->mcBfSetTag.tag_num == 0)) {
	    freeCount++;
	}
    }

    /*
     * Special case BMT page 0.  Count will be off by one due to mcell
     * 0.  Make sure we don't do this on the rbmt page
     */
    if (is_rbmt == FALSE) {
	if (pageNumber == 0) {
	    freeCount--;
	}
    }

    if (bmtPage->bmtFreeMcellCnt != freeCount) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_124, 
					   "Modified %s page %ld's freeCount from %d to %d.\n"),
				   is_rbmt ? "RBMT" : "BMT", pageNumber, 
				   bmtPage->bmtFreeMcellCnt, freeCount);
	if (status != SUCCESS) {
	    return status;
	}
        bmtPage->bmtFreeMcellCnt = freeCount;
	modified = TRUE;
    }

/*
 * TODO-ODS: Verify/Reset bmtVdIndex and bmtVdCookie in BMT header.
 */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_125, 
			 "Found problems on volume %s %s page %ld's header.\n"),
		 volume->volName, is_rbmt ? "RBMT" : "BMT", pageNumber);
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bmt_page_header */


/*
 * Function Name: correct_mcell_header
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
static int 
correct_mcell_header(pageT *pPage, 
		     volNumT volNum, 
		     pageNumT pageNum, 
		     cellNumT cellNum, 
		     domainT *domain)
{
    char     *funcName = "correct_mcell_header";
    bsMPgT   *pBMTPage;  /* The current (R)BMT page we are working on */
    bsMCT    *pMcell;    /* The mcell we are working on. */
    volNumT  nextVolNum;
    int      modified;   /* Flag letting us know if we have made changes. */
    int      status;
    volumeT  *volume;

    /*
     * Init Variables.
     */
    modified = FALSE;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[cellNum]);
    volume = &(domain->volumes[volNum]);

    if ((pMcell->mcTag.tag_num == 0) && (pMcell->mcBfSetTag.tag_num == 0)) {
        /* 
	 * Will handle case of one but not both tags being '0' when we
	 * follow chains, as we could find the correct tag and or
	 * settag at that point.
	 */

	/*
	 * Looks like this mcell is free (both tags == 0).
	 * Check/correct the volume number.  Volume number is zero for
	 * the last free mcell on the page.
	 */

        if (pMcell->mcNextMCId.volume != 0  && 
	    pMcell->mcNextMCId.volume != volNum) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_126, 
					       "Modified mcell (%d,%ld,%d)'s nextMCId.volume from %d to %d.\n"),
				       volNum, pageNum, cellNum,
				       pMcell->mcNextMCId.volume, 0);
	    if (status != SUCCESS) {
		return status;
	    }
	    if (pMcell->mcNextMCId.cell == 0) {
		pMcell->mcNextMCId.volume = 0;
	    } else {
		pMcell->mcNextMCId.volume = volNum;
	    }
	    modified = TRUE;
	}
	/*
	 * make sure nextMCId is on this page (free mcell).
	 */
	if (pMcell->mcNextMCId.page != pageNum) {
	    if ((pMcell->mcNextMCId.page == 0) && 
		(pMcell->mcNextMCId.cell == 0) &&
                (pMcell->mcNextMCId.volume == 0)) {
	        /*
		 * special case - last free mcell on list.
		 */
	    } else {
	        /*
		 * will be re-added to free list later.
		 */
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_127, 
						   "Modified mcell (%d,%ld,%d)'s nextMCId %ld,%ld,%ld to %ld,%ld.\n"),
					   volNum, pageNum, cellNum,
                                           pMcell->mcNextMCId.volume,
					   pMcell->mcNextMCId.page,
					   pMcell->mcNextMCId.cell,
					   0L, 0L);
		if (status != SUCCESS) {
		    return status;
		}
		pMcell->mcNextMCId.page = 0;
                pMcell->mcNextMCId.volume = volNum;
		pMcell->mcNextMCId.cell = 0;
		modified = TRUE;
	    }
	}
    } else {
	    
	/*
	 * Else mcell tag and parent set tag are not zero.
	 */

        /* 
	 * now we check to see if tag and seq are valid.  
	 *
	 * The primary mcell of a fileset tag dir lists the root tag
	 * dir number (-2) as it's fileset (parent) tag num.
	 */
        if ((pMcell->mcBfSetTag.tag_num == -2) && (pMcell->mcTag.tag_num > 0)) {
	    pageT  *pTempPage;
	    bsMPgT *pTempBMTPage;  
	    bsMCT  *pTempMcell;
	    bsMRT  *pRecord;
	    int    otherPage;
	    lbnNumT tmpLbn;
	    volNumT tmpVol;

	    otherPage = FALSE;

	    /*
	     * This could be the primary mcell of a fileset tag file.
	     */
	    if (pMcell->mcNextMCId.volume == 0) {
	        /*
		 * The tail of the chain, trust the header.
		 */
	    } else {
	        /*
		 * Could be the head, or in the middle of the chain.
		 */
	        if (FETCH_MC_VOLUME(pMcell->mcNextMCId) == volNum &&
		    FETCH_MC_PAGE(pMcell->mcNextMCId) == pageNum) {
		    /*
		     * Next mcell on the same BMT page
		     */
		    pTempMcell = &(pBMTPage->bsMCA[pMcell->mcNextMCId.cell]);
		} else {
		    fobtoLBNT *bmtLbn;
		    xtntNumT size;

		    /*
		     * mcell falls on another BMT page.
		     */
		    otherPage  = TRUE;
		    nextVolNum = FETCH_MC_VOLUME(pMcell->mcNextMCId);
		    bmtLbn     = domain->volumes[nextVolNum].volRbmt->bmtLBN;
		    size       = domain->volumes[nextVolNum].volRbmt->bmtLBNSize;

		    status = convert_page_to_lbn(bmtLbn,
						 size,
						 pMcell->mcNextMCId.page,
						 &tmpLbn, 
						 &tmpVol);
		    if (status != SUCCESS) {
		        writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FSCK_1, FSCK_128,
					 "Can't convert BMT page %ld to LBN.\n"), 
				 pMcell->mcNextMCId.page);
			return FAILURE;
		    }

		    status = read_page_from_cache(tmpVol, tmpLbn, &pTempPage);
		    if (status != SUCCESS) {
		        return status;
		    } 

		    pTempBMTPage = (bsMPgT *)pTempPage->pageBuf;
		    pTempMcell   = &(pTempBMTPage->bsMCA[pMcell->mcNextMCId.cell]);
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
					       catgets(_m_catd, S_FSCK_1, FSCK_129, 
						       "Freed mcell %d on BMT page %ld, the next chain was corrupt.\n"),
					       cellNum, pageNum);
		    if (status != SUCCESS) {
			return status;
		    }

		    /*
		     * Free Original mcell, not the next 
		     */
		    status = free_mcell(domain, volNum, pPage, cellNum);
		    if (status != SUCCESS) {
		        writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FSCK_1, FSCK_130,
					 "Can't free mcell %d on volume %s BMT page %ld.\n"), 
				 cellNum, volume->volName, pageNum);
			return FAILURE;
		    }
		    modified = TRUE;
		}

		if (otherPage == TRUE) {
		   status = release_page_to_cache(tmpVol, tmpLbn, pTempPage, 
						  FALSE, FALSE);
		   if (status != SUCCESS) {
		       return status;
		   }
		}
	    } /* end if/else not head of fileset primary chain */
	} /* end if a fileset mcell */ 
    } /* end if empty/valid mcell */
    
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_133,
			 "Found corruption in mcell (%d,%ld,%d)'s header.\n"),
		 volNum, pageNum, cellNum);
        return FIXED;
    }

    return SUCCESS;
} /* end correct_mcell_header */


/*
 * Function Name:correct_mcell_records
 *
 * Description: This routine checks and fixes mcell records in the
 *              BMT. It is called from correct_bmt_mcell_data.
 *
 * Input parameters:
 *     domain:    The domain we are working on.
 *     page:      The BMT page which mcell is on.
 *     volNum:    The volume we are working on.
 *     pageNum:   The BMT page number we are working on.
 *     mcellNum:  The BMT mcell to be checked and fixed
 *     isPrimary: This mcell is the primary mcell.
 *     isFsstat:  This mcell contains the fsstat record.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE, CORRUPT, FIXED.  
 */
static int 
correct_mcell_records(domainT *domain, 
		      pageT *pPage, 
		      volNumT volNum, 
		      pageNumT pageNum, 
		      cellNumT mcellNum,
		      int *isPrimary, 
		      int *isFsstat)
{
    char    *funcName = "correct_mcell_record";
    bsMPgT  *pBMTPage; /* The current RBMT page we are working on */
    bsMCT   *pMcell;   /* Pointer to current mcell. */
    bsMRT   *pRecord;  /* Pointer to current mcell record. */
    int     modified;  /* Let the tool know if page has been modified. */
    int     status;
    int     sizeUsed;

    /*
     * Init variables
     */
    modified = FALSE;
    sizeUsed = 0;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    pRecord  = (bsMRT *)pMcell->bsMR0;
    *isPrimary = FALSE;
    *isFsstat  = FALSE;
    
    /*
     * Now loop through all records in this mcell.
     */
    while (pRecord->type && pRecord->bCnt &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	status = correct_mcell_record_bcnt(domain, pPage, mcellNum, 
					   pRecord, sizeUsed);
	if (status != SUCCESS) {
	    if (status == FIXED) {
		modified = TRUE;
	    } else if (CORRUPT == status) {
		/*
		 * This record would overwrite the next mcell.  So we are 
		 * just going to truncate the mcell.
		 */
		pRecord->bCnt = 0;
		pRecord->type = BSR_NIL;
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
		status = correct_bsr_attr_record(pPage, mcellNum, pRecord);
		if ((SUCCESS == status) || (status == FIXED)) {
		    *isPrimary = TRUE;
		} /* end if success or fixed */
		break;

	    case (BSR_XTRA_XTNTS):
		status = correct_bsr_xtra_xtnts_record(domain, volNum, 
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
							    pRecord);
		break;

	    case (BSR_BFS_QUOTA_ATTR):
		status = correct_bsr_bfs_quota_attr_record(domain, pPage, 
							   mcellNum, 
							   pRecord);
		break;

	    case (BSR_ACL_REC):
		correct_bsr_acl_record(domain, pPage, mcellNum, pRecord);
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
						     mcellNum, pRecord);
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
		if ((SUCCESS == status) || (status == FIXED)) {
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

	    /*
	     * This group of records are only found in RBMT, 
	     * so they are invalid record.
	     */
	    case (BSR_VD_ATTR):
	    case (BSR_DMN_ATTR):
	    case (BSR_VD_IO_PARAMS):
	    case (BSR_DMN_MATTR):
	    case (BSR_DMN_TRANS_ATTR):

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
			 catgets(_m_catd, S_FSCK_1, FSCK_135, 
				 "Unknown BMT record %d in mcell (%d,%ld,%d).\n"),
			 pRecord->type, volNum, pageNum, mcellNum);
		break;
	} /* End Switch */

	if (status != SUCCESS) {
	    if (status == FIXED) {
	        modified = TRUE;
	    } else {
	        return status;
	    }
	}

	/*
	 * Increment sizeUsed by record size rounded up.
	 */
	sizeUsed += roundup(pRecord->bCnt, sizeof(uint64_t));
	
	/*
	 * Set pRecord to next record  of pMcell.
	 */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(uint64_t))); 
    } /* end pRecord loop */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_136, 
			 "Found corruption in mcell (%d,%ld,%d).\n"),
		 volNum, pageNum, mcellNum);
        return FIXED;
    }

    return SUCCESS;
} /* end correct_mcell_records */


/*
 * Function Name: correct_bsr_xtnts_record
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
static int 
correct_bsr_xtnts_record(pageT *pPage, 
			 cellNumT mcellNum, 
			 bsMRT *pRecord)
{
    char     *funcName = "correct_bsr_xtnts_record";
    bsMPgT   *pBMTPage;  /* The current BMT page we are working on */
    int      modified;   /* Let the tool know if page has been modified. */
    pageNumT pageNum;
    volNumT  volNum;
    int      status;
    bsXtntRT *pXtnt;

    /*
     * Init Variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum = pBMTPage->bmtPageId;
    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * mcellCnt should be 1 if chain is 0,0.  Greater than 1 if chain
     * is valid, so set to 2 for now we will set the correct value
     * when we follow the chains.
     *
     * This is true UNLESS the file is striped.
     */
    if (pXtnt->mcellCnt == 0) {
        if ((pXtnt->chainMCId.page == 0) && 
	    (pXtnt->chainMCId.cell == 0)) {
	    pXtnt->mcellCnt = 1;
	} else {
	    pXtnt->mcellCnt = 2;
	}
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_142, 
					   "Modified mcell (%d,%ld,%d)'s mcellCnt from %d to %d.\n"),
				   volNum, pageNum, mcellNum, 0,
				   pXtnt->mcellCnt);
	if (status != SUCCESS) {
	    return status;
	}

	modified = TRUE;
    } else if (pXtnt->mcellCnt == 1) {
        /*
	 * Should not have a chain.
	 */
        if ((pXtnt->chainMCId.page != 0) ||
	    (pXtnt->chainMCId.cell != 0) ||
	    (pXtnt->chainMCId.volume != 0)) {

	    ZERO_MCID(pXtnt->chainMCId);
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_143,
					       "Modified mcell (%d,%ld,%d)'s chain to (%d,%ld,%d), as mcellCnt was %d.\n"),
				       volNum, pageNum, mcellNum, 0, 0, 0, 1);
	    if (status != SUCCESS) {
	        return status;
	    }
		
	    modified = TRUE;
	}
    } else {
        /*
	 * Chain better be set, if not mcellCnt is wrong.
	 */
        if ((pXtnt->chainMCId.page == 0) &&
	    (pXtnt->chainMCId.cell == 0) &&
	    (pXtnt->chainMCId.volume == 0)) {
	    pXtnt->mcellCnt = 1;
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_144, 
					       "Modified mcell (%d,%ld,%d)'s mcellCnt to %d, because chain is empty.\n"),
				       volNum, pageNum, mcellNum, 1);
	    if (status != SUCCESS) {
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
     */
	
    if ((pXtnt->xCnt == 2) &&
	(pXtnt->bsXA[0].bsx_fob_offset == 0) &&
	(pXtnt->bsXA[1].bsx_vd_blk == XTNT_TERM)) {
      /*
       * This is valid for a file with an extent.
       */
    } else if ((pXtnt->xCnt == 1) &&
	       (pXtnt->bsXA[0].bsx_fob_offset == 0) &&
	       (pXtnt->bsXA[0].bsx_vd_blk == XTNT_TERM)) {
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

	if ((pXtnt->xCnt == 2) && (pXtnt->bsXA[1].bsx_fob_offset == 0)) {
	    /*
	     * Error case
	     *                    xcnt = 2, 0  X
	     *                              0 -1
	     * Fix
	     *
	     *                    xcnt = 1, 0  -1
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_142, 
					       "Modified mcell (%d,%ld,%d)'s mcellCnt from %d to %d.\n"),
				       volNum, pageNum, mcellNum,
				       pXtnt->mcellCnt, 1);
	    if (SUCCESS != status) {
		return status;
	    }

	    pXtnt->bsXA[0].bsx_fob_offset = 0;
	    pXtnt->bsXA[0].bsx_vd_blk = -1;
	    pXtnt->mcellCnt       = 1;
	    modified = TRUE;
	} else if ((pXtnt->xCnt == 1) &&
		   ((pXtnt->bsXA[0].bsx_fob_offset != 0) || 
		    (pXtnt->bsXA[0].bsx_vd_blk != XTNT_TERM))) {
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_145, 
					       "Modified BMT page %ld mcell %d, xCnt is %d, but no extents in record.\n"),
				       pageNum, mcellNum, 1);
	    if (status != SUCCESS) {
		return status;
	    }

	    pXtnt->bsXA[0].bsx_fob_offset = 0;
	    pXtnt->bsXA[0].bsx_vd_blk  = XTNT_TERM;
	    modified = TRUE;

	} else if ((pXtnt->xCnt == 2) &&
		   ((pXtnt->bsXA[0].bsx_fob_offset != 0) || 
		    (pXtnt->bsXA[1].bsx_vd_blk != XTNT_TERM))) {

	    pXtnt->bsXA[0].bsx_fob_offset = 0;
	    pXtnt->bsXA[1].bsx_vd_blk  = XTNT_TERM;
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_146, 
					       "Modified BMT page %ld mcell %d, extent record was not terminated.\n"),
				       pageNum, mcellNum);
	    if (status != SUCCESS) {
		return status;
	    }

	    modified = TRUE;
	} else {
	    if (pXtnt->bsXA[0].bsx_vd_blk != XTNT_TERM ||
		pXtnt->bsXA[1].bsx_vd_blk == XTNT_TERM) {
		/*
		 * Has a valid LBN or has a "hole".
		 */
		pXtnt->xCnt = 2;
		pXtnt->bsXA[0].bsx_fob_offset = 0;
		pXtnt->bsXA[1].bsx_vd_blk  = XTNT_TERM;
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_147, 
						   "Modified BMT page %ld mcell %d, xCnt set to %d.\n"),
					   pageNum, mcellNum, 2);
		if (status != SUCCESS) {
		    return status;
		}

		modified = TRUE;
	    } else {
		/*
		 * A single extent
		 */
		pXtnt->xCnt = 1;
		pXtnt->bsXA[0].bsx_fob_offset = 0;
		pXtnt->bsXA[0].bsx_vd_blk  = XTNT_TERM;
		status = add_fixed_message(&(pPage->messages), BMT,
					   catgets(_m_catd, S_FSCK_1, FSCK_147,
						   "Modified BMT page %ld mcell %d, xCnt set to %d.\n"),
					   pageNum, mcellNum, 1);
		if (status != SUCCESS) {
		    return status;
		}

		modified = TRUE;
	    } /* end if single vs two extents */
	} /* end if different extent corruption types */
    } /* end if/else extent checks */

#ifdef Tru64
    /* XXX - this code was brought over from Tru64 but does not work
       as-is because HP/UX user file block sizes are now variable
       between domains (ie: BLOCKS_PER_PAGE can not be a constant
       value).  We added fields to the volume structure embedded in
       the domain struct that hold the page size for user and metadata
       pages on that volume (even though at the moment those values
       are constant domain wide, that will someday change).  We can
       use that data here in place of BLOCKS_PER_PAGE once we
       determine if this is a metadata file or a user file.  In order
       to do that we need to change the call args up the calling
       sequence to pass in "domain" and the page type (meta or user).
       We'll do that work soon as we get a chance. */

    /*
     * The first extent's bsx_vd_blk must either "-1", "-2" or be evenly 
     * divisable by BLOCKS_PER_PAGE.
     */
    if ((pXtnt->bsXA[0].bsx_vd_blk != XTNT_TERM) &&
	(pXtnt->bsXA[0].bsx_vd_blk != COWED_HOLE) &&
	(pXtnt->bsXA[0].bsx_vd_blk % BLOCKS_PER_PAGE)) {
	/*
	 * LBN not on page boundary, make it a hole (-1).
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_460,
					   "Modified mcell (%d,%ld,%d) xtnt entry %d's LBN from %ld to %ld.\n"),
				   volNum, pageNum, mcellNum,
				   0, 
				   pXtnt->bsXA[0].bsx_vd_blk, -1);
	if (SUCCESS != status) {
	    return status;
	}
	pXtnt->bsXA[0].bsx_vd_blk = -1;
	modified = TRUE;
    }
#endif /* Tru64 */

    /*
     * Don't need to check the delLink or delRst at this point.
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_XTNTS");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_xtnts_record */


/*
 * Function Name: correct_bsr_attr_record
 *
 * Description: This routine checks and fixes mcell records of
 *              type BSR_ATTR.
 *
 * Input parameters:
 *     pPage: Pointer to the page data this record is on.
 *     mcellNum: The mcell that this record belongs to.
 *     pRecord: Pointer to the record being checked.
 *     RBMTflag: Set to TRUE if checking RBMT, FALSE if BMT.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FIXED, DELETED.  
 */
static int 
correct_bsr_attr_record(pageT *pPage, 
			cellNumT mcellNum, 
			bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_attr_record";
    bsMPgT *pBMTPage; /* The current RBMT page we are working on */
    int    modified;  /* Let the tool know if page has been modified. */
    int    status;
    pageNumT pageNum;
    volNumT volNum;
    bsBfAttrT   *pBSRAttr;
   
    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum = pBMTPage->bmtPageId;
    pBSRAttr = (bsBfAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check each field in BSR_ATTR looking for corruptions:
     */
    if ((pBSRAttr->state < BSRA_INVALID) || 
	(pBSRAttr->state > BSRA_VALID)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_149, 
					   "Modified mcell (%d,%ld,%d)'s bsrAttr state from %d to BSRA_VALID.\n"),
				   volNum, pageNum, mcellNum, 
				   pBSRAttr->state);
	if (status != SUCCESS) {
	    return status;
	}

        pBSRAttr->state = BSRA_VALID;
	modified = TRUE;
    }

    /* XXX - this could be a user file xtnt so the size is not
       necessarily the metadata file size.  We need to find a new way
       to check this... */

#ifdef NOTNOW
    if (pBSRAttr->bfPgSz != ADVFS_METADATA_PGSZ_IN_BLKS) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_150, 
					   "Modified mcell (%d,%ld,%d)'s bsrAttr bfPgSz from %d to %d.\n"),
				   volNum, pageNum, mcellNum, 
				   pBSRAttr->bfPgSz, 
				   ADVFS_METADATA_PGSZ_IN_BLKS);
	if (status != SUCCESS) {
	    return status;
	}

        pBSRAttr->bfPgSz = ADVFS_METADATA_PGSZ_IN_BLKS;
	modified = TRUE;
    }
#endif /* NOTNOW */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_attr_record */


/*
 * Function Name: correct_bsr_xtra_xtnts_record
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
static int 
correct_bsr_xtra_xtnts_record(domainT *domain, 
			      volNumT volNum, 
			      pageT *pPage, 
			      cellNumT mcellNum, 
			      bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_xtra_xtnts_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    pageNumT pageNum;
    int    extent;
    volumeT *volume;
    bsXtraXtntRT *pXtra;

    /*
     * Init variables.
     */
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum = pBMTPage->bmtPageId;
    pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
    volume = &(domain->volumes[volNum]);

    /*
     * Check each field in BSR_XTRA_XTNTS looking for corruptions:
     */

    /*
     * Need to make sure the xCnt is with in valid range.
     */
    if ((pXtra->xCnt < 1) || (pXtra->xCnt > BMT_XTRA_XTNTS)) {
	/*
	 * As we will be be setting the xCnt to the actual number in 
	 * the following block ofcode, all we need to do is set xCnt 
	 * to BMT_XTRA_XTNTS and set the last bsx_vd_blk to 0.
	 */
	pXtra->xCnt = BMT_XTRA_XTNTS;

	/* 
	 * The only valid values for the last vdblk are -1 and 0, but
	 * garbage has been known to show up here when the entry in the
	 * array is not in use.
	 *
	 * If bsx_vd_blk should not be 0 but instead -1, it will be set
	 * correctly in the next block of code.  
	 */
	pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk = 0;
	modified = TRUE;

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_156, 
					   "Modified mcell (%1$d,%2$d,%3$d)'s xCnt to %4$d.\n"),
				   volNum, pageNum, mcellNum, 
				   pXtra->xCnt);
	if (status != SUCCESS) {
	    return status;
	}
    }

    if (pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk != XTNT_TERM) {
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
	 * B) A bogus fob offset
	 * C) A bogus block number
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
	    if (pXtra->bsXA[x].bsx_fob_offset >= 
		pXtra->bsXA[x + 1].bsx_fob_offset) {
	        /* 
		 * page number does not increment. - valid end case 
		 */
	        endExtent = x;
	        found = TRUE;
	    } else if (pXtra->bsXA[x].bsx_vd_blk == XTNT_TERM) {
	        if (pXtra->bsXA[x + 1].bsx_vd_blk == XTNT_TERM) {
		    /* 
		     * Hole at end - valid end case 
		     */
		    endExtent = x + 1;
		    found = TRUE;
		}
	    } else if ((((pXtra->bsXA[x+1].bsx_fob_offset - pXtra->bsXA[x].bsx_fob_offset) * ADVFS_FOBS_PER_DEV_BSIZE) + pXtra->bsXA[x].bsx_vd_blk) >
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

	if (found == FALSE) {
	    /*
	     * Did not find an end, assume xCnt correct.
	     */
	    pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk = XTNT_TERM;
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_162, 
					       "Modified mcell (%d,%ld,%d)'s bsx_vd_blk to %ld.\n"),
				       volNum, pageNum, mcellNum, XTNT_TERM);
	    if (status != SUCCESS) {
		return status;
	    }

	    modified = TRUE;
	} else {
	    /* 
	     * Found an end point 
	     */
	    if (pXtra->bsXA[endExtent].bsx_vd_blk != XTNT_TERM) {
	        if (endExtent + 1  < pXtra->xCnt) {
		    /*
		     * Double corruption.
		     */
		    pXtra->xCnt = endExtent + 1;
		}
		pXtra->bsXA[pXtra->xCnt - 1].bsx_vd_blk = XTNT_TERM;
	    } else {
		pXtra->xCnt = endExtent + 1;
	    }
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_156, 
					       "Modified mcell (%1$d,%2$d,%3$d)'s xCnt to %4$d.\n"),

				       volNum, pageNum, mcellNum, 
				       pXtra->xCnt);
	    if (status != SUCCESS) {
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
    if (pXtra->bsXA[0].bsx_fob_offset > pXtra->bsXA[1].bsx_fob_offset) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_158, 
					   "Modified mcell (%d,%ld,%d) extent %ld's FOB offset from %ld to %ld.\n"),
				   volNum, pageNum, mcellNum,
				   0,
				   pXtra->bsXA[0].bsx_fob_offset, 
				   pXtra->bsXA[1].bsx_fob_offset);
	if (status != SUCCESS) {
	    return status;
	}

	pXtra->bsXA[0].bsx_fob_offset = pXtra->bsXA[1].bsx_fob_offset;
	pXtra->bsXA[0].bsx_vd_blk = XTNT_TERM;
	modified = TRUE;
    }

    /*
     * Now we can loop through the rest of the extents in the mcell.
     * If this mcell has only 2 extents we don't enter the loop
     *
     * Error case #2 
     *   if fobC < fobA then set fobC to fobB
     *
     * Error case #3
     *   if fobC < fobB then set fobB to fobC
     */
    for (extent = 2; extent < pXtra->xCnt; extent++) {
	bf_fob_t fobA, fobB, fobC;

	fobA = pXtra->bsXA[extent -2].bsx_fob_offset;
	fobB = pXtra->bsXA[extent -1].bsx_fob_offset;
	fobC = pXtra->bsXA[extent].bsx_fob_offset;

	if (fobC < fobA) {
	    /*
	     * fobC set to value of fobB.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_159, 
					       "Modified mcell (%d,%ld,%d)'s extent FOB range out of order.\n"),
				       volNum, pageNum, mcellNum);
	    if (status != SUCCESS) {
		return status;
	    }

	    pXtra->bsXA[extent].bsx_fob_offset = 
	      pXtra->bsXA[extent - 1].bsx_fob_offset;
	    pXtra->bsXA[extent].bsx_vd_blk = XTNT_TERM;
	    pXtra->bsXA[extent - 1].bsx_vd_blk = XTNT_TERM;
	    modified = TRUE;
	} else if (fobC < fobB) {
	    /*
	     * fobB set to value of fobC.
	     */
	    status = add_fixed_message(&(pPage->messages), BMT,
				       catgets(_m_catd, S_FSCK_1, FSCK_159,
					       "Modified mcell (%d,%ld,%d)'s extent FOB range out of order.\n"),
				       volNum, pageNum, mcellNum);
	    if (status != SUCCESS) {
		return status;
	    }

	    pXtra->bsXA[extent - 1].bsx_fob_offset = 
	      pXtra->bsXA[extent].bsx_fob_offset;
	    pXtra->bsXA[extent].bsx_vd_blk = XTNT_TERM;
	    pXtra->bsXA[extent - 1].bsx_vd_blk = XTNT_TERM;
	    modified = TRUE;
	}
    } /* end of extents in mcell */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148,
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_XTRA_XTNTS");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bsr_xtra_xtnts_record */



/*
 * Function Name:correct_bsr_mcell_free_list_record
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
static int
correct_bsr_mcell_free_list_record(domainT *domain, 
				   pageT *pPage, 
				   cellNumT mcellNum, 
				   bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_mcell_free_list_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    int    modified;   /* Let the tool know if page has been modified. */
    int    currPage;
    int    status;
    pageNumT pageNum;
    volNumT volNum;
    int    maxBmtFob;
    signed int firstPage;

    bsMcellFreeListT *pMcellFreeList;
    volumeT *volume;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum = pBMTPage->bmtPageId;
    pMcellFreeList = (bsMcellFreeListT *)((char *)pRecord + sizeof(bsMRT));
    currPage = pBMTPage->bmtPageId;
    volume = &(domain->volumes[volNum]);
    maxBmtFob = volume->volRbmt->bmtLBN[volume->volRbmt->bmtLBNSize].fob;

    /*
     * Check each field in BSR_MCELL_FREE_LIST looking for corruptions:
     */
    if  (mcellNum != 0 || currPage != 0) {
        /*
	 * This record should only appear in mcell 0 on the first BMT 
	 * page.  if it appears elsewhere it is invalid type.
	 * Delete the record.
	 */
        status = delete_mcell_record(domain, pPage, mcellNum, pRecord);
	if (status != SUCCESS) {
	    return status;
	}
	/* 
	 * We currently just truncate the record. In the future we may 
	 * need to delete the entire mcell, possibly the entire chain
	 * depending on the situation.  If needed delete mcell from mcell 
	 * list.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_163, 
					   "Modified BMT page %ld mcell %d - Deleted invalid record %d.\n"),
				   pageNum, mcellNum, pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}

	modified = TRUE;
    }

    firstPage = -1;

    /*
     * pMcellFreeList.headPg will be recalculated in free_mcell_list.
     */
    if (((signed int)(pMcellFreeList->headPg) < firstPage) ||
	((signed int)(pMcellFreeList->headPg) > FOB2PAGE(maxBmtFob))) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_164, 
					   "Modified mcell (%d,%ld,%d)'s headPg entry.\n"),
				   volNum, pageNum, mcellNum,
				   pMcellFreeList->headPg,
				   firstPage);
	if (status != SUCCESS) {
	    return status;
	}
	pMcellFreeList->headPg = firstPage;
	modified = TRUE;
    }

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_MCELL_FREE_LIST");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_mcell_free_list_record */


/*
 * Function Name:correct_bsr_bfs_attr_record
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
static int
correct_bsr_bfs_attr_record(domainT *domain, 
			    pageT *pPage, 
			    cellNumT mcellNum, 
			    bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_bfs_attr_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    pageNumT pageNum;
    volNumT volNum;
    bsBfSetAttrT *pAttr;

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;
    pAttr    = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check each field in BSR_BFS_ATTR looking for corruptions:
     */
    if ((pAttr->bfSetId.domainId.id_sec != domain->dmnId.id_sec) ||
	(pAttr->bfSetId.domainId.id_usec != domain->dmnId.id_usec)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_165,
					   "Modified mcell (%d,%ld,%d)'s domain ID from %x.%x to %x.%x\n"),
				   volNum, pageNum, mcellNum,
				   pAttr->bfSetId.domainId.id_sec, 
				   pAttr->bfSetId.domainId.id_usec,
				   domain->dmnId.id_sec, 
				   domain->dmnId.id_usec);
	if (status != SUCCESS) {
	    return status;
	}

        pAttr->bfSetId.domainId.id_sec = domain->dmnId.id_sec;
	pAttr->bfSetId.domainId.id_usec = domain->dmnId.id_usec;
	modified = TRUE;
    }

    if ((pAttr->bfSetId.dirTag.tag_num != pMcell->mcTag.tag_num) ||
	(pAttr->bfSetId.dirTag.tag_seq != pMcell->mcTag.tag_seq)) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_166, 
					   "Modified mcell (%d,%ld,%d)'s file system tag from %lx.%x to %lx.%x\n"),
				   volNum, pageNum, mcellNum,
				   pAttr->bfSetId.dirTag.tag_num, 
				   pAttr->bfSetId.dirTag.tag_seq, 
				   pMcell->mcTag.tag_num, pMcell->mcTag.tag_seq);
	if (status != SUCCESS) {
	    return status;
	}

        pAttr->bfSetId.dirTag.tag_num =  pMcell->mcTag.tag_num;
	pAttr->bfSetId.dirTag.tag_seq =  pMcell->mcTag.tag_seq;
	modified = TRUE;
    }

    status = validate_fileset_name(pAttr->setName);
    if (status != SUCCESS) {
        char   setName[32];
	/*
	 * Create a new name for the fileset.
	 */
	sprintf(setName, "fsck_filesystem_%d", pAttr->bfSetId.dirTag.tag_num);
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_168, 
					   "Modified invalid file system name to '%s'.\n"),
				   setName);
	if (status != SUCCESS) {
	    return status;
	}

	strncpy(pAttr->setName, setName, 32);
	modified = TRUE;
    }

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_BFS_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_bfs_attr_record */


/*
 * Function Name: correct_bsr_def_del_mcell_list_record
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
static int
correct_bsr_def_del_mcell_list_record(domainT *domain, 
				      pageT *pPage, 
				      cellNumT mcellNum, 
				      bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_def_del_mcell_list_record";
    bsMPgT *pBMTPage; /* The current RBMT page we are working on */
    int    modified;  /* Let the tool know if page has been modified. */
    int    status;
    pageNumT pageNum;
    volNumT volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum = pBMTPage->bmtPageId;

    /*
     * Check each field in BSR_DEF_DEL_MCELL_LIST looking for corruptions:
     */
    if (mcellNum != 0 || pageNum != 0) {
        /*
	 * This record should only appear on the mcell 0 on the first BMT 
	 * page.  if it appears elsewhere it is invalid type.
	 * Delete the record.
	 */
        status = delete_mcell_record(domain, pPage, mcellNum, pRecord);
	if (status != SUCCESS) {
	    return status;
	}
	/* 
	 * We currently just truncate the record. In the future we may 
	 * need to delete the entire mcell, possibly the entire chain
	 * depending on the situation.  If needed delete mcell from mcell 
	 * list.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_163, 
					   "Modified BMT page %ld mcell %d - Deleted invalid record %d.\n"),
				   pageNum, mcellNum, pRecord->type);
	if (status != SUCCESS) {
	    return status;
	}

	modified = TRUE;
    }

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_DEF_DEL_MCELL_LIST");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_def_del_mcell_list_record */


/*
 * Function Name:correct_bsr_bf_inherit_attr_record
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
static int
correct_bsr_bf_inherit_attr_record(pageT *pPage, 
				   cellNumT mcellNum, 
				   bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_bf_inherit_attr_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    pageNumT pageNum;
    volNumT volNum;
    bsBfInheritAttrT *pInherit;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;
    pInherit = (bsBfInheritAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Check each field in BSR_BF_INHERIT_ATTR looking for corruptions:
     */
    if (pInherit->reqServices != 1) {
        pInherit->reqServices = 1;

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_170, 
					   "Cleared mcell (%d,%ld,%d)'s inheritable attributes.\n"),
				   volNum, pageNum, mcellNum);
	if (status != SUCCESS) {
	    return status;
	}

	modified = TRUE;
    }

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_BF_INHERIT_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_bf_inherit_attr_record */


/*
 * Function Name: correct_bsr_bfs_quota_attr_record
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
static int
correct_bsr_bfs_quota_attr_record(domainT *domain, 
				  pageT *pPage, 
				  cellNumT mcellNum, 
				  bsMRT *pRecord)
{
    char   *funcName = "correct_bsr_bfs_quota_attr_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    pageNumT pageNum;
    volNumT volNum;
    int64_t  hardBlockLimit;   /* hard block limit */
    int64_t  softBlockLimit;   /* soft block limit */
    int64_t  hardFileLimit;    /* hard file limit */
    int64_t  softFileLimit;    /* soft file limit */
    bsQuotaAttrT *pQuotaAttr;
    fsQuotaT *pFsQuota;      /* Used to store the quotas */

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum = pBMTPage->bmtPageId;
    pQuotaAttr = (bsQuotaAttrT *)((char *)pRecord + sizeof(bsMRT));

    /*
     * Quota values are stored as 2 ints, but in reality they are longs.
     */
    hardBlockLimit = (uint64_t)pQuotaAttr->blkHLimit;
    softBlockLimit = (uint64_t)pQuotaAttr->blkSLimit;
    hardFileLimit = (uint64_t)pQuotaAttr->fileHLimit;
    softFileLimit = (uint64_t)pQuotaAttr->fileSLimit;

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
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_171,
			 "file system quotas"));
	return NO_MEMORY;
    }

    pFsQuota->filesetTag.tag_num = pMcell->mcTag.tag_num;
    pFsQuota->filesetTag.tag_seq = pMcell->mcTag.tag_seq;
    pFsQuota->hardBlockLimit = hardBlockLimit;
    pFsQuota->softBlockLimit = softBlockLimit;
    pFsQuota->hardFileLimit = hardFileLimit;
    pFsQuota->softFileLimit = softFileLimit;
    pFsQuota->blockTimeLimit = pQuotaAttr->blkTLimit;
    pFsQuota->fileTimeLimit = pQuotaAttr->fileTLimit;
    pFsQuota->quotaStatus = pQuotaAttr->quotaStatus;
    pFsQuota->next = domain->filesetQuota;
    domain->filesetQuota = pFsQuota;

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_BFS_QUOTA_ATTR");
        return FIXED;
    }

    return SUCCESS;
} /* end correct_bsr_bfs_quota_attr_record */

/*
 * Function Name: correct_bsr_acl_rec
 *
 * Description: This routine checks and fixes mcell records of type

 *              BSR_ACL_REC.  A more detailed check is done at a later
 *              stage when we follow the chain of ACLs for each file
 *              (correct_chain_ACL()).  ACLs can form a chain that
 *              crosses many mcells and certain of the ACL record's
 *              internal values are relevant to the chaining, so they
 *              are checked when the chain followed.
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
static int
correct_bsr_acl_record(domainT *domain,
		       pageT   *pPage, 
		       cellNumT mcellNum, 
		       bsMRT   *pRecord)
{
    char   *funcName = "correct_bsr_acl_record";
    bsMPgT *pBMTPage;	   /* The current RBMT page we are working on */
    uint64_t alignedFlags; /* Gets rid of unaligned access errors. */
    int    modified;	   /* Let the tool know if page has been modified. */
    int    status;
    pageNumT pageNum;
    volNumT volNum;
    bsr_acl_rec_t *ACL;
    size_t recSize = sizeof(bsMRT) + sizeof(bsr_acl_rec_t);

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;

    if (pRecord->bCnt != recSize) {
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_985, 
					   "Modified mcell (%d,%ld,%d)'s ACL rec size from %d to %d.\n"),
				   volNum, pageNum, mcellNum,
				   pRecord->bCnt, recSize);
	if (status != SUCCESS) {
	    return status;
	}
	
	pRecord->bCnt = recSize;
	modified = TRUE;
    }

    ACL = (bsr_acl_rec_t *)MREC_TO_REC(pRecord);
    
    if (ACL->bar_hdr.bar_acl_type != ADVFS_ACL_TYPE) {

	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_980,
					   "Modified mcell (%d,%ld,%d) ACL type from %d to ADVFS_ACL_TYPE (%d).\n"),
				   volNum, pageNum, mcellNum,
				   ACL->bar_hdr.bar_acl_type, ADVFS_ACL_TYPE);

	if (status != SUCCESS) {
	    return(FAILURE);
	}
	ACL->bar_hdr.bar_acl_type = ADVFS_ACL_TYPE;
	modified = TRUE;
    }

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BSR_ACL_REC");
        return FIXED;
    }

    return SUCCESS;

} /* end correct_bsr_acl_record */

/*
 * Function Name: correct_bmtr_fs_dir_index_file
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
static int
correct_bmtr_fs_dir_index_file(pageT *pPage, 
			       cellNumT mcellNum, 
			       bsMRT *pRecord)
{
    char     *funcName = "correct_bmtr_fs_dir_index_file";
    bsMPgT   *pBMTPage;  /* The current RBMT page we are working on */
    int      modified;   /* Let the tool know if page has been modified. */
    pageNumT pageNum;
    volNumT  volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;

    /*
     * The field tag, could be 0 in very rare case.  This means the best we 
     * can do is check the high order bit.  if we know the number of extents 
     * in the tag file we can compute a high-water mark (page * 1022).
     * We can check this when we follow chains.
     */
    
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_DIR_INDEX_FILE");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_dir_index_file */


/*
 * Function Name: correct_bmtr_fs_index_file_record
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
static int 
correct_bmtr_fs_index_file_record(pageT *pPage, 
				  cellNumT mcellNum, 
				  bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_index_file_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    int    modified;   /* Let the tool know if page has been modified. */
    pageNumT pageNum;
    volNumT volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum = pBMTPage->bmtPageId;

    /*	
     * fname_page   = 1 or more, but no larger than max extent page.
     * ffree_page   = 1 or more, but no larger than max extent page.
     * fname_levels = Max of 4, see bs_index.h for more details.
     * ffree_levels = Max of 4, see bs_index.h for more details.
     */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_INDEX_FILE");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_index_file_record */


/*
 * Function Name: correct_bmtr_fs_time_record
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
static int 
correct_bmtr_fs_time_record(domainT *domain, 
			    pageT *pPage, 
			    cellNumT mcellNum, 
			    bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_time_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    status;
    pageNumT pageNum;
    volNumT volNum;

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;

    if (pMcell->mcTag.tag_num != 2) {
        /*
	 * record type only appears for tag 2 of each fileset.
	 * As 2 records have this bCnt we might delete a dir index
	 * or a trashcan, but those are not critical files.
	 */
        status = delete_mcell_record(domain, pPage, mcellNum, pRecord);
	if (status != SUCCESS) {
	    return status;
	}
	/* 
	 * We currently just truncate the record. In the future we may 
	 * need to delete the entire mcell, possibly the entire chain
	 * depending on the situation.  If needed delete mcell from mcell 
	 * list.
	 */
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_178, 
					   "Deleted BMT page %ld mcell %d record, invalid location.\n"),
				   pageNum, mcellNum);
	if (status != SUCCESS) {
	    return status;
	}

	modified = TRUE;
    }

    /*
     * Field in record can not be incorrect.
     */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148,
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_TIME");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_time_record */


/*
 * Function Name:correct_bmtr_fs_undel_dir_record
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
static int 
correct_bmtr_fs_undel_dir_record(pageT *pPage, 
				 cellNumT mcellNum, 
				 bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_undel_dir_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    int    modified;   /* Let the tool know if page has been modified. */
    pageNumT pageNum;
    volNumT volNum;

    /*
     * Init variables
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;

    /*
     * dirTag  = Tag number of a Valid directory, could check the high bit
     *
     * At this point there is no tag list, a better place to do the 
     * detailed check would be during the chain check.
     */

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_UNDEL_DIR");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_undel_dir_record */


/*
 * Function Name: correct_bmtr_fs_data_record
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
static int 
correct_bmtr_fs_data_record(pageT *pPage, 
			    cellNumT mcellNum, 
			    bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_data_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    bsMCT  *pMcell;    /* Pointer to current mcell. */
    int    modified;   /* Let the tool know if page has been modified. */
    int    recordType;
    int    status;
    pageNumT pageNum;
    volNumT volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    pMcell   = &(pBMTPage->bsMCA[mcellNum]);
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;

    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148, 
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_DATA");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_data_record */


/*
 * Function Name: correct_bmtr_fs_stat_record
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
static int 
correct_bmtr_fs_stat_record(pageT *pPage, 
			    cellNumT mcellNum, 
			    bsMRT *pRecord)
{
    char   *funcName = "correct_bmtr_fs_stat_record";
    bsMPgT *pBMTPage;  /* The current RBMT page we are working on */
    int    modified;   /* Let the tool know if page has been modified. */
    pageNumT pageNum;
    volNumT volNum;

    /*
     * Init variables.
     */
    volNum   = pPage->vol;
    pBMTPage = (bsMPgT *)pPage->pageBuf;
    modified = FALSE;
    pageNum  = pBMTPage->bmtPageId;

    /*
     * Special case: we will not correct st_ino record in the fsstat
     * record until we get to correct_bmt_chains.  If we were to set it
     * here we could be wrong 50% of the time, once we are the chains
     * we have a much higher (maybe 100%?) chance of setting the correct
     * value.
     * 
     * We also check the frag data in chains
     */
    if (modified == TRUE) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_148,
			 "Found corruption in mcell (%d,%ld,%d)'s %s record.\n"),
		 volNum, pageNum, mcellNum, "BMTR_FS_STAT");
        return FIXED;
    }
    return SUCCESS;
} /* end correct_bmtr_fs_stat_record */


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
static int 
correct_mcell_record_bcnt(domainT *domain, 
			  pageT *pPage, 
			  cellNumT mcellNum,
			  bsMRT *pRecord, 
			  int currentSize)
{
    char *funcName = "correct_mcell_record_bcnt";
    int  realByteCount;
    int  status;
    int  recordType;
    pageNumT pageNum;
    volNumT volNum;
    int  modified;
    bsMPgT  *pBMTPage; /* The current BMT page we are working on */

    modified   = FALSE;
    volNum     = pPage->vol;
    pBMTPage   = (bsMPgT *)pPage->pageBuf;    
    pageNum    = pBMTPage->bmtPageId;
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
	    realByteCount = sizeof(bsMRT) + sizeof(struct bsUndelDir);
	    break;

	case (BMTR_FS_STAT):
	    realByteCount = sizeof(bsMRT) + sizeof(statT);
	    break;

	/*
	 * The size of BMTR_FS_DATA must fit in the usable record
	 * space, BS_USABLE_MCELL_SPACE, 280 bytes on 4/5/04. This is
	 * a variable length record, so the best we can do is set it
	 * to the maximum length.
	 */
	case (BMTR_FS_DATA):
	    if (pRecord->bCnt > BS_USABLE_MCELL_SPACE) {
		realByteCount = BS_USABLE_MCELL_SPACE;
	    } else {
		realByteCount = pRecord->bCnt;
	    }
	    break;

        case (BSR_ACL_REC):
	    realByteCount = sizeof(bsMRT) + sizeof(bsr_acl_rec_t);
	    break;

        case (BSR_RUN_TIMES):
	    realByteCount = sizeof(bsMRT) + sizeof(bsRunTimesRT);
	    break;

        case (BSR_DMN_NAME):
	    realByteCount = sizeof(bsMRT) + sizeof(bsDmnNameT);
	    break;

        case (BSR_DMN_UMT_THAW_TIME):
	    realByteCount = sizeof(bsMRT) + sizeof(bsDmnUmntThawT);
	    break;

	default:
	    realByteCount = pRecord->bCnt;
	    break;
    } /* End Switch */

    if (realByteCount != pRecord->bCnt) {

	/* In Tru64, we called match_record_byte_count() (see below)
	   to try to guess the record type based on the bCnt.  The
	   record size values have all changed for HP/UX and we
	   haven't reassessed the size values used in
	   match_record_byte_count(), at least not yet.  This function
	   is of questionable value anyway, since if the bCnt and type
	   mismatch it assumes it's the type that's wrong, which may
	   not be the case. Also, this is a mantenance headache when
	   record sizes change. So instead, we leave the type alone
	   and simply rewrite the bCnt. */
	
	status = add_fixed_message(&(pPage->messages), BMT,
				   catgets(_m_catd, S_FSCK_1, FSCK_137, 
					   "Modified mcell (%1$d,%2$d,%3$d)'s bCnt from %4$d to %5$d.\n"),
				   volNum, pageNum, mcellNum, 
				   pRecord->bCnt, realByteCount);
	if (status != SUCCESS) {
	    return status;
	}

	pRecord->bCnt = realByteCount;
	modified = TRUE;
    } /* end if/else realByteCount */

    /*
     * Now that we know the size, lets see if this record will
     * overflow the mcell record size (including the trailing NULL
     * record).
     */
    if (currentSize + realByteCount > BSC_R_SZ - sizeof(bsMRT)) {
	return CORRUPT;
    }

    if (modified == TRUE) {
	return FIXED;
    }
    return SUCCESS;
} /* end correct_mcell_record_bcnt */

#ifdef TRU64
/*
 * Function Name:match_record_byte_count
 *
 * Description: This function will take a look at a buffer and try and
 *              figure out what mcell record type it is. Make sure
 *              that V4 ODS do not end up on V3 domains.
 *
 * Input parameters:
 *     pRecord: Pointer to a buffer containing a mcell record.
 *     pageNum: The page number in the BMT/RBMT this record is on.
 *     mcellNum: The mcell number that this record belongs to.
 *     is_rbmt: Set to TRUE if checking RBMT, FALSE if BMT.
 *
 * Output parameters: 
 *     recordType: The best guess on what the record type is.
 *
 * Returns: SUCCESS
 */

static int 
match_record_byte_count(bsMRT *pRecord, 
			pageNumT pageNum, 
			cellNumT mcellNum,
			int is_rbmt, 
			int *recordType)
{
    char *funcName = "match_record_byte_count";
    int  bCnt;    /* Size of the record (from header) */
    bsBfClAttrT  *pBsBfClAttr;
    bsBfAttrT    *pBsBfAttr;


    bCnt = pRecord->bCnt;
    *recordType = BSR_NIL;

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
          if (TRUE == is_rbmt) {
	      *recordType = BSR_DMN_ATTR;
	  }
	  break;

      case (28):
          if (is_rbmt == TRUE) {
	      *recordType = BSR_VD_IO_PARAMS;
	  }
	  break;

      case (32):
          if (is_rbmt == TRUE) {
	      *recordType = BSR_DMN_FREEZE_ATTR;
	  }
	  break;

      case (40):
          if (is_rbmt == TRUE) {
	      *recordType = BSR_VD_ATTR; 
	  }
	  break;

      case (52):
          if (is_rbmt == TRUE) {
	      *recordType = BSR_DMN_MATTR;
	  }
	  break;

      case (60):
          if (is_rbmt == TRUE) {
	      *recordType = BSR_DMN_SS_ATTR;
	  }
	  break;

      case (64):
	  *recordType = BSR_BFS_QUOTA_ATTR;
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

	  if (ADVFS_METADATA_PGSZ_IN_BLKS == pBsBfAttr->bfPgSz) {
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

      default:
	  /*
	   * These are variable length records, or records which we don't care
	   * if they are deleted.
	   *
	   *  The size of BMTR_FS_DATA will be between 5 and 255.
	   */
	  break;
    } /* End Switch */

    return SUCCESS;
} /* end match_record_byte_count */
#endif /* TRU64 */

/* end bmt.c */
