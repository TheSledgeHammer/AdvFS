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
 *      Routines to support SBM manipulation for the AdvFS on-disk 
 *      structure fixer.
 *
 * Date:
 *      Tue Oct 17 12:20:44 PDT 2000
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

/*
 * Private prototypes
 */

static int noteOverlaps(uint64_t startBit,
			uint64_t endBit,
			uint32_t *obm,
			overlapNodeT *template,
			volumeT *volume);

static int look_for_overlapped_clust_owner(domainT *domain,
					   volNumT volNum,
					   clustNumT startBit,
					   clustNumT bitCount,
					   int *statusFlag,
					   overlapNodeT **overlapListHead,
					   extentIdT *extentAddr);

static int remove_reserved_file_overlaps(domainT *domain,
					 volNumT volNum,
					 overlapNodeT **overlapListHead);

static int find_overlap_node_by_lbn(overlapNodeT *listHead, 
				    overlapNodeT *extentNode,
				    overlapNodeT **foundNode);

static int find_overlap_node_by_mcell(overlapNodeT *listHead, 
				      overlapNodeT *extentNode,
				      overlapNodeT **foundNode);

static int insert_overlap_data_by_lbn(overlapNodeT **listHead,
				      overlapNodeT *node);

static int insert_overlap_data_by_mcell(overlapNodeT **listHead, 
					overlapNodeT *node);

static int remove_overlap_node_from_lists(overlapNodeT *overlapNode,
					  overlapNodeT **fileListHead,
					  overlapNodeT **lbnListHead);



/*
 * Function Name: add_tag_to_delete_list (3.xx) 
 *
 * Description: The purpose of this routine to add a tag to the list
 *		of files that are to be deleted.
 *
 * Input parameters:
 *      tag:			The tag being worked on.
 *      settag:			The settag being worked on.
 *      deleteFileListHead:	The list of files to be deleted.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or NO_MEMORY.
 * 
 */
int 
add_tag_to_delete_list(tagNumT	 tag,
		       tagNumT	 settag,
		       fileNodeT **deleteFileListHead)
{
    char *funcName = "add_tag_to_delete_list";
    fileNodeT	*aFile;
    fileNodeT	*currNode;
    fileNodeT	*prevNode;

    assert (deleteFileListHead != NULL);

    /*
     * If the tag number is less than or equal to BFM_RSVD_TAGS (AdvFS
     * reserved and pseudo reserved files), or the set number is less
     * than 0, then return immediately.  These files should never be
     * placed onto the list of files which are to be deleted.
     */

    if ((tag <= BFM_RSVD_TAGS) || (settag <= 0)) {
	return SUCCESS;
    }

    aFile = (fileNodeT *)ffd_malloc (sizeof(fileNodeT));

    if (aFile == NULL) {
	writemsg (SV_ERR | SV_LOG_ERR,
		  catgets(_m_catd, S_FSCK_1, FSCK_10,
			  "Can't allocate memory for %s.\n"),
		  catgets(_m_catd, S_FSCK_1, FSCK_683, 
			  "file deletion node"));
	return NO_MEMORY;
    }

    aFile->tagnum = tag;
    aFile->setnum = settag;

    /*
     * Check to see if the list has any data in it yet.
     * If not, then this is the first node in the list.
     */
    if (*deleteFileListHead == NULL) {
	*deleteFileListHead = aFile;
	aFile->next	    = NULL;
	return SUCCESS;
    }

    /*
     * Walk through the list until the proper place to insert
     * this node is found.
     */

    currNode = *deleteFileListHead;
    prevNode = NULL;

    while (currNode != NULL) {
	/*
	 * Begin by checking the fileset numbers, which is the primary key.
	 */
	if (aFile->setnum < currNode->setnum) {
	    break;
	} else if (aFile->setnum > currNode->setnum) {
	    prevNode = currNode;
	    currNode = currNode->next;
	    continue;
	}

	/*
	 * Found the correct fileset.  Next check the tag numbers.
	 */
	if (aFile->tagnum < currNode->tagnum) {
	    break;
	} else if (aFile->tagnum > currNode->tagnum) {
	    prevNode = currNode;
	    currNode = currNode->next;
	    continue;
	}

	/*
	 * All of the positioning data has been matched.
	 * A node with this key must already be in the list.
	 * Free the new node, and return.
	 */

	free (aFile);
	return SUCCESS;
    }

    /*
     * Upon exit from the preceeding loop, either currNode is NULL,
     * or one of the fields of currNode > the same field of node.
     *
     * If currNode is NULL, then we simply add the node to the end
     * of the list.
     *
     * Otherwise insert the node between prevNode and currNode;
     */

    if (currNode == NULL) {
	/* 
	 * Link this node onto the end of the list. 
	 */
	prevNode->next = aFile;
	aFile->next    = NULL;
    } else {
	/* 
	 * Insert the node between prevNode and currNode.
	 */
	if (prevNode == NULL) {
	    /* 
	     * This becomes the head of the list. 
	     */
	    aFile->next         = *deleteFileListHead;
	    *deleteFileListHead = aFile;
	} else {
	    aFile->next    = prevNode->next;
	    prevNode->next = aFile;
	}
    } /* End if (currNode == NULL) */

    return SUCCESS;

} /* End add_tag_to_delete_list */


/*
 * Function Name: collect_sbm_info
 *
 * Description: This routine sets bits in the in-memory SBM
 *		corresponding to the block ranges of the extents
 *		passed in.  If a cluster is already allocated, this is
 *		considered an overlap and we set bits in an overlap
 *		bit map and also add overlap nodes to the appropriate
 *		overlap link lists.  The calling routine should check
 *		if an extent runs off the end of the disk and fix the
 *		extent in the mcell.
 *
 * Input parameters:
 *      domain:		The domain being worked on.
 *      extentArray:    An array of extents mapping a file.
 *      arraySize:      The number of records in the extent array.
 *      tag:		The file tag being worked on.
 *      settag:		The fileset tag being worked on.
 *	xtntMcell:	The address of the mcell claiming these extents.
 *	xtntNumOffset:	Number of extent record "missing" from the input 
 *			extent array.  This value is added to the array
 *			index when setting the extent number for the overlap
 *			nodes.
 *
 * The following assumptions are made about the inputs:
 *
 *   First,  the starting lbn must be on a cluster boundary.
 *   Second, the volume must be open.
 *   Third,  we must have an in-memory SBM and overlap bitmap 
 *           present.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, NO_MEMORY, or FAILURE.
 *
 */
int 
collect_sbm_info(domainT   *domain,
		 fobtoLBNT *extentArray,
		 int        arraySize,
		 tagNumT    tag,
		 tagNumT    settag,
		 mcidT      *xtntMcell,
		 xtntNumT   xtntNumOffset)
{
    char *funcName = "collect_sbm_info";
    filesetT  *pFileset;
    uint64_t  i, j;
    int       status;
    lbnNumT   startLbn;
    volNumT   volNum;
    volumeT   *volume;
    bf_fob_t  fobCount;
    clustNumT bitCount;
    clustNumT bitOffset;
    clustNumT startBit;
    clustNumT endBit;
    uint64_t  startWord;
    uint64_t  endWord;
    uint32_t  *sbm;          /* Pointer to SBM array.   */
    uint32_t  *obm;          /* Pointer to overlap bitmap array. */
    uint32_t  mask;
    uint32_t  overlapFound;
    const uint32_t allSet = UINT_MAX;
    overlapNodeT template;

    /*
     * Loop through each extent in the array.  NOTE: because of the
     * extent terminator entry (lbn: -1) there is always one more
     * entent entry that there are extents.
     */

    for (i = 0; i < (arraySize - 1); i++) {

	volNum   = extentArray[i].volume;
	startLbn = extentArray[i].lbn;

	/* negative LBN's aren't interesting for our purposes here. */

	if ((startLbn == (lbnNumT)XTNT_TERM) || 
	    (startLbn == (lbnNumT)PERM_HOLE_START)) {
	    continue;
	}

	volume       = &(domain->volumes[volNum]);
	sbm          = volume->volSbmInfo->sbm;
	obm          = volume->volSbmInfo->overlapBitMap;
	overlapFound = FALSE;

	/* 
	 * compute values for SBM start & end bits, SBM start & end
	 * words, and fob and cluster counts for this extent.  
	 *
	 * startWord: the 32-bit word offset into the SBM where the
	 *            bits of the current extent begins
	 * startBit: bit number in sbm[startWord] where the extent
	 *           begins.  
	 * endWord: the 32-bit word offset into the SBM where the
	 *          bits of the current extent end
	 * endBit: the bit number in THE ENTIRE SBM BIT ARRAY
	 *         where the extent ends (!).
	 *
	 * NOTE: an SBM cluster is a number of logical blocks NOT a
	 *       number of FOBs.
	 */
	
	fobCount = extentArray[i+1].fob - extentArray[i].fob;
	bitCount = FOB2CLUST(fobCount);
	bitOffset = LBN2CLUST(startLbn);
	startWord = bitOffset / SBM_BITS_LONG;
        startBit = bitOffset - (startWord * SBM_BITS_LONG);
	endWord = (bitOffset + bitCount - 1) / SBM_BITS_LONG;
	endBit = bitOffset + bitCount - 1;    /* ending bit number */

	/* load overlap node values that don't change (in this loop) */

	template.xtntId.cellAddr     = *xtntMcell;
	template.xtntId.extentNumber = i + xtntNumOffset;
	template.tagnum	             = tag;
	template.setnum              = settag;
	template.extentStartLbn	     = startLbn;
	template.extentClustCount    = bitCount;
	template.nextByMcell	     = NULL;
	template.nextByLbn           = NULL;

	/* 
	 * Set SBM bits covered by the extent, noting any overlaps.
	 */

	if (startWord == endWord) {

	    /*
	     * If the extent is confined to one SBM word, this is
	     * easy...
	     */

	    mask = allSet << startBit;
	    mask &= allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1)
			      - endBit);

	    if (sbm[startWord] & mask) {
		obm[startWord] |= sbm[startWord] & mask;
		overlapFound = TRUE;
	    }

	    sbm[startWord] |= mask;

	} else {

	    /* 
	     * The extent crosses SBM word boundaries (possibly more than
	     * one).  Deal with it.
	     */

	    mask = allSet << startBit;

	    if (sbm[startWord] & mask) {
		obm[startWord] |= sbm[startWord] & mask;
		overlapFound = TRUE;
	    }

	    sbm[startWord] |= mask;

	    for (j = startWord + 1; j < endWord; j++) {

		if (sbm[j] & allSet) {
		    obm[j] |= sbm[j] & allSet;
		    overlapFound = TRUE;
		}
		sbm[j] = allSet;
	    }

	    mask = allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1)
			      - endBit);

	    if (sbm[endWord] & mask) {
		obm[endWord] |= sbm[endWord] & mask;
		overlapFound = TRUE;
	    }

	    sbm[endWord] |= mask;
	}

	/* if overlaps were found, do the housekeeping for that */

	if (overlapFound) {

	    /*
	     * If the extent being checked belongs to an AdvFS
	     * Reserved File, then the domain is unrecoverably
	     * corrupt, so just exit.
	     */

	    if ((int64_t)tag < 0) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_684, 
				  "Reserved file %d overlaps another reserved file.\n"),
			  tag);
		corrupt_exit(NULL);
	    }

	    /* 
	     * complain to the user about finding this overlap.  We
	     * want to include the fileset name in the message if
	     * there is one, so hunt that down first.  
	     */

	    pFileset = domain->filesets;

	    while (NULL != pFileset) {
		if (pFileset->filesetId.dirTag.tag_num == settag) {
		    break;
		}
		pFileset = pFileset->next;
	    }

	    if (NULL == pFileset) {
		writemsg (SV_VERBOSE | SV_LOG_FOUND,
			  catgets(_m_catd, S_FSCK_1, FSCK_685,
				  "Mcell (%d,%ld,%d) extent %ld for tag %ld of FS tag '%ld' overlaps %ld FOBs of other data on volume %s starting at LBN %ld.\n"),
			  xtntMcell->volume, xtntMcell->page, xtntMcell->cell,
			  i + xtntNumOffset, tag, settag, fobCount,
			  volume->volName, startLbn);
	    } else {
		writemsg (SV_VERBOSE | SV_LOG_FOUND,
			  catgets(_m_catd, S_FSCK_1, FSCK_686, 
				  "Mcell (%d,%ld,%d) extent %ld for tag %ld of FS tag '%s' overlaps %ld FOBs of other data on volume %s starting at LBN %ld.\n"),
			  xtntMcell->volume, xtntMcell->page, xtntMcell->cell,
			  i + xtntNumOffset, tag, pFileset->fsName,
			  fobCount, volume->volName, startLbn);
	    }

	    /* create overlap nodes for each consecutive run of
	       overlaps. */

	    if ((status = noteOverlaps(bitOffset, endBit, obm, 
				       &template, volume)) != SUCCESS)
		return status;
	}
    }

    return SUCCESS;

} /* End collect_sbm_info */


/*
 * Function Name: noteOverlaps
 *
 * Description: This routine tests a range of the overlap bitmap.
 *		The overlap bitmap has a bit set for each cluster that
 *		was found to overlap with a cluster already claimed by
 *		another extent.  We look for runs of consecutive set
 *		bits and create an overlap node for each run.  The
 *		overlap nodes are linked into a chain off the volume
 *		structure.
 *
 * NOTE: the overlap bitmap here is thought of as a bit array begining
 *       with bit 0 of obm[0] and endinf with the last bit in the OBM
 *       (obm[last]).  The start and end bits are bit numbers counted
 *       from bit 0 in obm[0].
 *
 * Input parameters:
 *      startBit:  The first bit of the overlap bitmap's test range.
 *                 (a bit offset into the ENTIRE SBM ARRAY)
 *        endBit:  The last bit of the overlap bitmap's test range.
 *                 (a bit offset into the ENTIRE SBM ARRAY)
 *           obm:  The overlap bitmap (SBM) containing the test range.
 *      template:  An overlapNode containing values for new nodes.
 *        volume:  The volume struct that the overlapNodes link onto.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, or NO_MEMORY.
 *
 */

static int
noteOverlaps(uint64_t startBit,
	     uint64_t endBit,
	     uint32_t *obm,
	     overlapNodeT *template,
	     volumeT *volume)
{
    overlapNodeT *overlap;
    overlapNodeT **listHead;  /* Pointer to an overlap list. */
    clustNumT firstClust;     /* the 1st cluster of an overlap run */
    clustNumT overlapSize;    /* size of an overlap in SBM clusters */
    clustNumT bitNum;
    uint32_t testMask;
    uint64_t index;
    int status;

    /* create an index to the fist SBM overlap word in the test range */

    index = startBit / SBM_BITS_LONG;

    /* init the testMask to begin with the starting bit */

    bitNum = index * SBM_BITS_LONG;
    testMask = 0;

    for (bitNum = 0; bitNum < startBit; bitNum++) {
	if (!testMask)
	    testMask = 1;
	else
	    testMask <<= 1;
    }

    /* 
     * Test consecutive bits in the overlap bitmap test range until
     * they're all tested.  Create an overlap node for each separate
     * run of consecutive overlap bits.
     *
     * When we get here, bitNum == startBit (the SBM bit offset for
     * the start of the test range) and the testMask is set to the
     * first bit in the test range.  The bitNum value will be
     * advanced to indicate the bit number currently under test as the
     * bit search moves ahead.
     */

    firstClust = 0;

    while (bitNum <= endBit) {
	    
	if (testMask == 0) {
	    testMask = 1;
	    index++;
	}
	
	if (obm[index] & testMask) {

	    /* we found an overlap.  If it's the 1st overlap in a
	       new run, init the tracking variables.  Otherwise,
	       count the bit and move on. */
		
	    if (firstClust == 0) {
		firstClust = bitNum;
		overlapSize = 1;
	    } else {
		overlapSize++;
	    }
	}

	/* 
	 * if we've found the end of a run .OR. if we're within a run
	 * and we've hit the end of the test range, note the run by
	 * creating and storing an overlap node. 
	 */

	if ((!(obm[index] & testMask) && firstClust != 0) ||
	    (firstClust != 0 && bitNum == endBit)) {

	    overlap = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));

	    if (NULL == overlap) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_10, 
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FSCK_1, FSCK_687, 
				  "overlap node"));
		return (NO_MEMORY);
	    }

	    /* fill in the overlap node and add it to the lists */

	    *overlap = *template;
	    overlap->overlapStartClust = firstClust;
	    overlap->overlapClustCount = overlapSize;

	    listHead = &volume->volSbmInfo->overlapListByFile;
	    status   = insert_overlap_data_by_mcell(listHead, overlap);

	    if (status != SUCCESS) {
		return status;
	    }

	    listHead = &volume->volSbmInfo->overlapListByLbn;
	    status   = insert_overlap_data_by_lbn(listHead, overlap);
	
	    if (status != SUCCESS) {
		return status;
	    }

	    /* reset firstClust to indicate that the overlap run
	       is complete */

	    firstClust = 0;
	}

	/* shift the testMask and bump the bit number we're testing */

	testMask <<= 1;
	bitNum++;
    }

    return SUCCESS;

} /* end noteOverlaps */


/*
 * Function Name: correct_sbm (3.77)
 *
 * Description: This function will compare the on-disk SBM to the
 *		in-memory SBM.  If they are different, we modify the
 *		on-disk SBM to reflect the in-memory SBM.
 *
 * Input parameters:
 *     domain: Pointer to the domain being checked.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, FIXED, NO_MEMORY.
 * 
 */
int 
correct_sbm(domainT *domain)
{
    char  *funcName = "correct_sbm";
    bsStgBmT  *pSbmPage; /* Pointer to a SBM page read from disk. */
    int       bitNum;
    lbnNumT   currLbn;
    volNumT   currVol;
    lbnNumT   extentLbn;
    uint64_t  mapIndex;
    int       fixedSbm; /* Flag indicating at least one SBM page was fixed. */
    int       modified;
    int       fixedMarker;
    uint64_t  numExtents;
    pageNumT  numPagesInExtent;
    pageNumT  sbmExtentPage;
    uint64_t  sbmIndex; /* Index into the SBM array. */
    uint64_t  sizeOfSbm;
    int       status;
    volNumT   volNum;
    uint64_t  tempSize;
    clustNumT  bitOffset;
    pageT      *pCurrentPage; /* The current page we are checking */
    fobtoLBNT  *pExtents;     /* The extents to check */
    xtntNumT   extent;
    uint32_t   xor32;
    uint32_t   sbmWord;
    uint32_t   *mapLongs;
    uint32_t   *sbm;          /* Pointer to SBM array.  */
    volumeT    *volume;       /* Pointer to selected volume structure.*/

    /*
     * If the SBM has not yet been loaded, we're done here
     */

    if (!DMN_IS_SBM_LOADED(domain->status)) {
	return (SUCCESS);
    }

    fixedSbm = FALSE;
    fixedMarker = FALSE;

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {

	volume = &(domain->volumes[volNum]);
	if (volume->volFD == -1) {
	    continue;
	}

	/*
	 * Comapre each page of the on-disk SBM with the in-memory
	 * SBM.
	 */

	bitNum     = 0;
	pExtents   = volume->volRbmt->sbmLBN;
	numExtents = volume->volRbmt->sbmLBNSize;
	sbm        = volume->volSbmInfo->sbm;
	sbmIndex   = 0;
	sbmWord    = sbm[sbmIndex];
	sizeOfSbm  = ((volume->volSize / ADVFS_BS_CLUSTSIZE) / 
		      SBM_BITS_LONG) + 1;

	for (extent = 0; extent < numExtents; extent++) {
	    /*
	     * Compute number of pages in this extent.
	     */
	    currVol = pExtents[extent].volume;
	    extentLbn = pExtents[extent].lbn;
	    numPagesInExtent = FOB2PAGE(pExtents[extent + 1].fob -
				pExtents[extent].fob);
 
	    /*
	     * Loop thru each page in this SBM extent
	     */
	    for (sbmExtentPage = 0;
		 (sbmExtentPage < numPagesInExtent) && (sbmIndex < sizeOfSbm);
		 sbmExtentPage++) {

		currLbn = extentLbn + (sbmExtentPage * 
				       ADVFS_METADATA_PGSZ_IN_BLKS);

		status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
		if (status != SUCCESS) {
		    return status;
		}
     
		modified = FALSE;
		pSbmPage = (bsStgBmT *) pCurrentPage->pageBuf;
		mapLongs = (uint32_t *) pSbmPage->mapInt;

		/* check the page markers */

		if (pSbmPage->magicNumber != SBM_MAGIC) {
		    writemsg(SV_DEBUG,
			     catgets(_m_catd, S_FSCK_1, FSCK_965, 
				     "%s page header corrupt : magic # bad.\n"),
			     "SBM");
		    add_fixed_message(&(pCurrentPage->messages), SBM,
			     catgets(_m_catd, S_FSCK_1, FSCK_932, 
				     "%s page header corrupt : magic # xpd: 0x%x fnd: 0x%x.\n"),
			     "SBM", SBM_MAGIC, pSbmPage->magicNumber);



		    pSbmPage->magicNumber = SBM_MAGIC;
		    fixedMarker = TRUE;
		}

		if (pSbmPage->pageNumber != sbmExtentPage) {
		    writemsg(SV_DEBUG,
			     catgets(_m_catd, S_FSCK_1, FSCK_964, 
				   "SBM page header corrupt : Page # bad.\n"));
		    add_fixed_message(&(pCurrentPage->messages), SBM,
			     catgets(_m_catd, S_FSCK_1, FSCK_950, 
				     "SBM page header corrupt : Page # xpd: %ld fnd: %ld.\n"),
			     sbmExtentPage, pSbmPage->pageNumber);

		    pSbmPage->pageNumber = sbmExtentPage;
		    fixedMarker = TRUE;
		}

		if (!COOKIE_EQ(pSbmPage->fsCookie, domain->fsCookie)) {
		    writemsg(SV_DEBUG,
			     catgets(_m_catd, S_FSCK_1, FSCK_933, 
				     "%s page header corrupt : fsCookie bad.\n"),
			     "SBM");
		    add_fixed_message(&(pCurrentPage->messages), SBM,
				      catgets(_m_catd, S_FSCK_1, FSCK_963, 
					      "Modified SBM page %ld fsCookie from %x.%x to %x.%x\n"),
				      sbmExtentPage,
				      pSbmPage->fsCookie.id_sec,
				      pSbmPage->fsCookie.id_usec,
				      domain->fsCookie.id_sec,
				      domain->fsCookie.id_usec);

		    pSbmPage->fsCookie = domain->fsCookie;
		    fixedMarker = TRUE;
		}

		/* 
		 * Generate the XOR value for "this" page of the
		 * in-memory SBM, and compare that with the on-disk
		 * XOR value.
		 *
		 * If these don't match, then modify the page.  If
		 * they do match, check the page data.
		 */

		xor32 = 0;
		for (mapIndex = 0; mapIndex < SBM_LONGS_PG; mapIndex++) {
		    if ((sbmIndex + mapIndex) < sizeOfSbm) {
			xor32 ^= sbm[sbmIndex + mapIndex];
		    }
		}

		/* 
		 * Calculate the number of bytes used by the
		 * bitmap.
		 */
		tempSize = MIN (SBM_LONGS_PG, (sizeOfSbm - sbmIndex));
		tempSize = tempSize * sizeof(uint32_t);

		if (xor32 != pSbmPage->xor) {
		    writemsg (SV_VERBOSE | SV_LOG_FOUND,
			      catgets(_m_catd, S_FSCK_1, FSCK_692, 
				      "Mismatch in SBM XOR on volume %s, SBM page %ld.  Expected %08x, found %08x.\n"),
			      volume->volName, 
			      FOB2PAGE(pExtents[extent].fob) + sbmExtentPage,
			      xor32, pSbmPage->xor);
		    modified = TRUE;
		} else {
		    /* 
		     * The disk XOR matches memory XOR.  Check to
		     * see if the data matches.  
		     */

		    if (memcmp(&mapLongs[0], &sbm[sbmIndex], tempSize)) {
			modified = TRUE;
			writemsg (SV_VERBOSE | SV_LOG_FOUND,
				  catgets(_m_catd, S_FSCK_1, FSCK_689, 
					  "Mismatch in SBM data on volume %s, SBM page %ld.\n"),
				  volume->volName, 
				  FOB2PAGE(pExtents[extent].fob) + sbmExtentPage);
		    }		
		} /* End if (xor32 != pSbmPage->xor) */

		if (modified == TRUE) {
		    /* 
		     * The page needs to be fixed.  Determine which
		     * pages are incorrectly marked in the SBM.
		     */
		    tempSize = MIN (SBM_LONGS_PG, (sizeOfSbm - sbmIndex));

		    for (mapIndex = 0; mapIndex < tempSize; mapIndex++) {

			if (mapLongs[mapIndex] != sbm[sbmIndex + mapIndex]) {
			    /*
			     * There is at least one discrepancy
			     * between the on-disk SBM and the
			     * in-memory SBM.  Compute the bit offset
			     * into the extent for the word having the
			     * discrepancy.
			     */
			    bitOffset = (((FOB2PAGE(pExtents[extent].fob) + 
					   sbmExtentPage) * 
					  SBM_LONGS_PG) + mapIndex) * 
				        SBM_BITS_LONG;

			    /*
			     * Now check each bit of this word.
			     */
			    for (bitNum = 0, sbmWord = 1;
				 bitNum < SBM_BITS_LONG;
				 bitNum++, sbmWord << 1) {

				if ((mapLongs[mapIndex] & sbmWord) !=
				    (sbm[sbmIndex + mapIndex] & sbmWord)) {
				    if ((mapLongs [mapIndex] & sbmWord) == 0) {
					writemsg (SV_DEBUG | SV_LOG_FOUND,
						  catgets(_m_catd, S_FSCK_1, FSCK_690, 
							  "SBM cluster at LBN %ld incorrectly marked free.\n"),
						  (bitOffset + bitNum) * ADVFS_BS_CLUSTSIZE);
				    } else {
					writemsg (SV_DEBUG | SV_LOG_FOUND,
						  catgets(_m_catd, S_FSCK_1, FSCK_691, 
							  "SBM cluster at LBN %ld incorrectly marked used.\n"),
						  (bitOffset + bitNum) * ADVFS_BS_CLUSTSIZE);
				    } /* end if ((mapLongs [mapIndex] & sbmWord) == 0) */
				} /* end if ((mapLongs[mapIndex] & sbmWord)... */
			    } /* end for (bitNum = 0, sbmWord = 1;... */
			} /* end if (mapLongs [mapIndex] != sbm[sbmIndex + mapIndex]) */
		    } /* end for (mapIndex = 0; ... */

		    tempSize = MIN (SBM_LONGS_PG, (sizeOfSbm - sbmIndex));
		    tempSize = tempSize * sizeof(uint32_t);

		    if (tempSize < SBM_LONGS_PG * sizeof(uint32_t)) {
			bzero(mapLongs, SBM_LONGS_PG * sizeof(uint32_t));
		    }

		    pSbmPage->xor    = xor32;
		    memcpy (&mapLongs[0], &sbm[sbmIndex], tempSize);
		} /* end if (TRUE == modified) */

		sbmIndex += SBM_LONGS_PG;

		if (modified == TRUE || fixedMarker == TRUE) {
		    add_fixed_message(&(pCurrentPage->messages), SBM,
				      catgets(_m_catd, S_FSCK_1, FSCK_694, 
					      "SBM page %ld fixed.\n"),
				      FOB2PAGE(pExtents[extent].fob) + 
				      sbmExtentPage);
		    fixedSbm = TRUE;
		    modified = TRUE;
		}

		status = release_page_to_cache(currVol, currLbn, pCurrentPage,
					       modified, FALSE);
		if (status != SUCCESS) {
		    return status;
		}

		modified = FALSE;

	    }/* End for (sbmExtentPage = 0; ... */
        } /* End for (extent = 0; extent < numExtents; extent++) */
    } /* End for (volNum = 1; volNum <= domain->highVolNum; volNum++) */
    
    if (fixedSbm == TRUE) {
	return FIXED;
    }

    return SUCCESS;
} /* End correct_sbm */


/*
 * Function Name: correct_sbm_overlaps
 *
 * Description: This function corrects SBM overlaps by deleting all files
 *		that have some data overlap.
 *
 *		The current design of fixfdmn calls for the deletion
 *		of all files that "share" disk blocks, and that is the
 *		purpose of the correct_sbm_overlaps routine.  However,
 *		the current in-memory SBM and overlap bitmap structures
 *		do not keep track of which files "own" which disk pages.
 *		The only data known is which "files" overlap pages that
 *		were allocated to other file(s).  As a result, a search
 *		must be made for the file(s) that own the overlapped
 *		pages.
 *
 *		Because files can span multiple volumes, all files that
 *		are overlapped must be found before attempting to delete
 *		any files -- otherwise deleting a file on volume 1 might
 *		remove an overlap on volume 2, which could remove overlap
 *		data in volume 2's SBM structures.
 *
 *		As a result, the work of correct_sbm_overlaps is divided
 *		into three phases:
 *
 *		   1) build a list of tags (files) to be deleted
 *		      from the data in the volume overlap lists.  
 *
 *		   2) find all of the files that "own" pages on
 *		      the disk that are overlapped by other files.
 *		      Overlapped user files are added to the list
 *		      of files to be deleted.
 *
 *		   3) delete all files that on the to-be-deleted
 *		      list.
 *
 *		The second phase, finding the "owner" of overlapped
 *		pages, is the most complicated.  Each volume in the
 *		domain is handled separately, and only volumes that
 *		have at least one overlapped page are examined.
 *
 *		For each volume the routine first copies the overlap
 *		list.  While creating this copy, the first and last
 *		overlapped pages on this volume are noted.  Note that
 *		pages will appear in the list as many times as they
 *		they were overlapped.  
 *
 *		The next step is to check to see if any of the reserved
 *		files (RBMT, SBM, Root Tag, LOG, BMT, & Misc File)
 *		were overlapped.  Sections of overlaps that point to
 *		pages within a reserved file must be removed from the
 *		overlap list.
 *
 *		Because the overlap nodes simply identify the Mcell
 *		which incurred the overlap and not the extent within the
 *		Mcell, each Mcell in the overlap list is checked to make
 *		sure that extent records within that Mcell don't overlap
 *		each other.  Note that all internal corruption of this
 *		form will be found here.
 *
 *		Then while overlaps remain, the routine reads the BMT.  
 *		As an extent is found, it is checked to see if some part
 *		of it falls between the first and last overlapped pages
 *		of that volume.  If so, then handle the detailed overlap
 *		processing by calling look_for_overlapped_clust_owner.
 *
 * Input parameters:
 *     domain: Pointer to the domain being checked.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, FIXED, NO_MEMORY.
 * 
 */
int 
correct_sbm_overlaps(domainT *domain)
{
    char  *funcName = "correct_sbm_overlaps";
    bsMCT	*pMcell;  /* The current BMT mcell we are working on */
    bsMPgT	*bmtPage; /* The current BMT page we are working on */
    bsMRT	*pRecord;
    bsXtntRT	*pXtnt;
    bsXtraXtntRT *pXtra;
    extentIdT	extentAddr;
    fileNodeT	*currFile;
    fileNodeT	*deleteFileListHead;
    fileNodeT	*prevFile;
    filesetT	*pFileset;
    bf_fob_t bmtExtentFob;
    lbnNumT  currLbn;
    volNumT  currVol;
    uint64_t firstOverlappedClust;
    uint64_t extent;
    lbnNumT  extentLbn;
    uint64_t extentSize;
    tagNumT  lastFilesetNo;
    uint32_t ix;		/* for loop variable. */
    uint32_t mcell;
    uint64_t lastOverlappedClust;
    uint64_t numExtents;
    bf_fob_t numFobsInBMTExtent;
    uint64_t overlapCount;		/* for loop variable. */
    uint64_t clustNumber;
    bf_fob_t startFob;
    int      status;
    int      statusFlag;
    volNumT  volNum;
    uint64_t tempSize;
    overlapNodeT *lookupNode;	/* Pointer to an overlap node. */
    overlapNodeT *overlay;	/* Pointer to new overlap node. */
    overlapNodeT *overlapListCopyByLbn;	/* Ptr to copy of the overlap list. */
    pageT	 *pCurrentPage; /* The current page we are checking */
    fobtoLBNT	 *pExtents; /* The extents to check */

    volumeT	 *volume; /* Pointer to selected volume structure.*/

    /*
     * If the SBM has not yet been loaded, do not correct
     * any on-disk SBMs.
     */

    if (!DMN_IS_SBM_LOADED(domain->status)) {
	return (SUCCESS);
    }

    deleteFileListHead = NULL;

    /*
     * Build the initial list of files to be deleted from the
     * overlap lists.
     */

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	volume  = &(domain->volumes[volNum]);
	if (volume->volFD == -1) {
	    continue;
	}

	lookupNode = volume->volSbmInfo->overlapListByLbn;

	while (lookupNode != NULL) {
	    status = add_tag_to_delete_list (lookupNode->tagnum,
					     lookupNode->setnum,
					     &deleteFileListHead);
	    if (status != SUCCESS) {
		return status;
	    }
	    lookupNode = lookupNode->nextByLbn;
	} /* End while (lookupNode != NULL) */
    } /* End for (volNum = 1; volNum <= domain->highVolNum; volNum++) */

    ZERO_MCID(extentAddr.cellAddr);

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	volume  = &(domain->volumes[volNum]);
	if (volume->volFD == -1) {
	    continue;
	}

	lookupNode = volume->volSbmInfo->overlapListByLbn;

	if (lookupNode == NULL) {
	    /*
	     *  No overlaps detected on this volume.
	     */
	    writemsg (SV_DEBUG, 
		      catgets(_m_catd, S_FSCK_1, FSCK_695,
			      "No overlaps detected on volume %s.\n"),
		      domain->volumes[volNum].volName);
	    continue;
	}

	extentAddr.cellAddr.volume  = volNum;

	/*
	 * Copy the overlap list for this volume.  Also note the
	 * first and last pages overlapped on this volume.
	 *
	 * NOTE:  The last overlapped page will actually be the
	 *	  page number beyond the last overlapped page at
	 *	  the end of this loop.
	 */


	firstOverlappedClust  =	lookupNode->overlapStartClust;
	lastOverlappedClust   =	lookupNode->overlapStartClust +
				lookupNode->overlapClustCount;
	overlapCount	     =  0;
	overlapListCopyByLbn =	NULL;


	while (lookupNode != NULL) {
	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (overlay == NULL) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_10,
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FSCK_1, FSCK_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlay->xtntId.cellAddr     = lookupNode->xtntId.cellAddr;
	    overlay->xtntId.extentNumber = lookupNode->xtntId.extentNumber;
	    overlay->tagnum		 = lookupNode->tagnum;
	    overlay->setnum		 = lookupNode->setnum;
	    overlay->overlapStartClust	 = lookupNode->overlapStartClust;
	    overlay->overlapClustCount	 = lookupNode->overlapClustCount;
	    overlay->extentStartLbn	 = lookupNode->extentStartLbn;
	    overlay->extentClustCount	 = lookupNode->extentClustCount;
	    overlay->nextByMcell	 = NULL;
	    overlay->nextByLbn		 = NULL;

	    status = insert_overlap_data_by_lbn (&overlapListCopyByLbn, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    overlapCount++;

	    /*
	     * Check if the outside bounds of overlaid LBNs needs to be updated.
	     */

	    if (lookupNode->overlapStartClust < firstOverlappedClust) {
		firstOverlappedClust = lookupNode->overlapStartClust;
	    }

	    tempSize =	lookupNode->overlapStartClust +
			lookupNode->overlapClustCount;
	    if (lastOverlappedClust < tempSize) {
		lastOverlappedClust = tempSize;
	    }

	    lookupNode = lookupNode->nextByLbn;
	} /* End while (NULL != lookupNode) */

	/*
	 * Decrement to the actual last page overlapped.
	 */
	lastOverlappedClust = lastOverlappedClust - 1;

	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_697, 
			 "Found %d overlaps on volume %s.\n"),
		 overlapCount, domain->volumes[volNum].volName);

	/*
	 * Before looking through the BMT, remove any overlap
	 * data that overwrites reserved files.
	 */

	status = remove_reserved_file_overlaps (domain, volNum,
						&overlapListCopyByLbn);
	if (status != SUCCESS) {
	    return status;
	}


	/*
	 *  Next find all of the "overlapped" files by reading
	 *  through the BMT for each volume.
	 *
	 *  We now have a local copy of the overlap list, but
	 *  it is only linked in page order.  Call the routine
	 *  look_for_overlapped_clust_owner for each extent that
	 *  has some clusters between the first and last overlapped
	 *  clusters of this volume.
	 *
	 *  The look* routine determines if the file owning this
	 *  extent needs to be placed onto the files-to-be-deleted
	 *  list, and it also modifies the overlap list nodes so
	 *  that these "owned" pages are no longer referenced.
	 *
	 *  As long as there are nodes on the temporary overlap
	 *  list, read through each page of the on-disk BMT and
	 *  look for extents that match those in the overlap list.
	 */

	pExtents	= volume->volRbmt->bmtLBN;
	numExtents	= volume->volRbmt->bmtLBNSize;

	for (extent = 0;
	     extent < numExtents && NULL != overlapListCopyByLbn;
	     extent++) {
	    /*
	     * Compute number of FOBs in this extent.
	     */
	    currVol	= pExtents[extent].volume;
	    extentLbn	= pExtents[extent].lbn;
	    numFobsInBMTExtent = (pExtents[extent + 1].fob -
				   pExtents[extent].fob);

	    /*
	     * For each extent loop through all pages.
	     * EXCEPTION:
	     */

	    startFob = 0;

	    for (bmtExtentFob = startFob;
		 bmtExtentFob < numFobsInBMTExtent &&
		     overlapListCopyByLbn != NULL;
		 bmtExtentFob++) {

		extentAddr.cellAddr.page = FOB2PAGE(pExtents[extent].fob + 
						    bmtExtentFob);

		currLbn = extentLbn + (bmtExtentFob / ADVFS_FOBS_PER_DEV_BSIZE);

		status = read_page_from_cache(currVol, currLbn, &pCurrentPage);

		if (status != SUCCESS) {
		    return status;
		}     

		bmtPage = (bsMPgT *)pCurrentPage->pageBuf;
		for (mcell = 0;
		     mcell < BSPG_CELLS && NULL != overlapListCopyByLbn;
		     mcell++) {

		    extentAddr.cellAddr.cell = mcell;

		    pMcell	 = &(bmtPage->bsMCA[mcell]);
		    pRecord	 =  (bsMRT *)pMcell->bsMR0;

		    while ((overlapListCopyByLbn != NULL)  &&
			   (pRecord->type != BSR_NIL) &&
			   (pRecord->bCnt != 0) &&
			   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

			switch (pRecord->type) {
			    case (BSR_XTNTS):

				pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

				/*
				 * Loop through the extents in this record.  End
				 * when either they have all been processed, or
				 * when the overlap list is empty.  Remember that
				 * the overlap list can be modified by the routine
				 * look_for_overlapped_clust_owner.
				 */
				for (ix = 0; 
				     overlapListCopyByLbn != NULL &&
				     ix < (pXtnt->xCnt - 1) &&
				     ix < (BMT_XTNTS - 1);
				     ix++) {

				    if (pXtnt->bsXA[ix].bsx_vd_blk == XTNT_TERM || 
					pXtnt->bsXA[ix].bsx_vd_blk == (uint64_t)PERM_HOLE_START) {
					continue;
				    }

				    clustNumber = pXtnt->bsXA[ix].bsx_vd_blk /
					          ADVFS_BS_CLUSTSIZE;

				    extentSize = FOB2CLUST(pXtnt->bsXA[ix + 1].bsx_fob_offset -
							   pXtnt->bsXA[ix].bsx_fob_offset);

				    /*
				     * If the extent starts after the last
				     * overlapped page, or ends before the first
				     * overlapped page, then don't call the
				     * overlap check routine.
				     */
				    if ((clustNumber > lastOverlappedClust) ||
				       ((clustNumber + extentSize) < firstOverlappedClust)) {
					continue;
				    }

				    statusFlag = FALSE;
				    if (pMcell->mcTag.tag_num <= 5 ||
					IS_ROOT_TAG(pMcell->mcBfSetTag)) {
					statusFlag = TRUE;
				    }
				    extentAddr.extentNumber = ix;
				    status = look_for_overlapped_clust_owner (
							domain,
							volNum,
							clustNumber,
							extentSize,
							&statusFlag,
							&overlapListCopyByLbn,
							&extentAddr);
				    if (status != SUCCESS) {
					if (status == CORRUPT) {
					    /*
					     * This is part 2 of a message,
					     * so use SV_CONT.  The first
					     * part was output by look_for...
					     */
					    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
						      catgets(_m_catd, S_FSCK_1, FSCK_698, 
							      "File system tag %ld, file tag %ld.\n"),
						      pMcell->mcBfSetTag.tag_num,
						      pMcell->mcTag.tag_num);
					    corrupt_exit(NULL);
					}
					return status;
				    } else {
					if (statusFlag == TRUE) {
					    status = add_tag_to_delete_list (
							pMcell->mcTag.tag_num, 
							pMcell->mcBfSetTag.tag_num,
							&deleteFileListHead);
					    if (status != SUCCESS) {
						return status;
					    }
					} /* End if (TRUE == statusFlag) */
				    } /* End if (status != SUCCESS) */
				} /* End for loop over extent records */
				break;
			    
			    case (BSR_XTRA_XTNTS):
				pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));

				/*
				 * Loop on extents until exhausted,
				 * or there are no more overlap nodes.
				 */
				for (ix = 0;
				     overlapListCopyByLbn != NULL &&
				     ix < (pXtra->xCnt - 1) &&
				     ix < (BMT_XTRA_XTNTS - 1);
				     ix++) {

				    if ((pXtra->bsXA[ix].bsx_vd_blk == XTNT_TERM) || 
					(pXtra->bsXA[ix].bsx_vd_blk == (uint64_t)PERM_HOLE_START)) {
					continue;
				    }

				    clustNumber = pXtnt->bsXA[ix].bsx_vd_blk /
					          ADVFS_BS_CLUSTSIZE;

				    extentSize = FOB2CLUST(pXtnt->bsXA[ix + 1].bsx_fob_offset -
							   pXtnt->bsXA[ix].bsx_fob_offset);

				    /*
				     * Call overlap check routine.
				     */
				    if ((clustNumber > lastOverlappedClust) ||
				       ((clustNumber + extentSize) < firstOverlappedClust)) {
					continue;
				    }

				    statusFlag = FALSE;
				    if (pMcell->mcTag.tag_num <= 5 ||
					IS_ROOT_TAG(pMcell->mcBfSetTag)) {
					statusFlag = TRUE;
				    }
				    extentAddr.extentNumber = ix;
				    status = look_for_overlapped_clust_owner (
							domain,
							volNum,
							clustNumber,
							extentSize,
							&statusFlag,
							&overlapListCopyByLbn,
							&extentAddr);
				    if (status != SUCCESS) {
					if (status == CORRUPT) {
					    /*
					     * This is part 2 of a message,
					     * so use SV_CONT.  The first
					     * part was output by look_for...
					     */
					    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
						      catgets(_m_catd, S_FSCK_1, FSCK_698, 
							      "File system tag %ld, file tag %ld.\n"),
						      pMcell->mcBfSetTag.tag_num,
						      pMcell->mcTag.tag_num);
					    corrupt_exit(NULL);
					}
					return status;
				    } else {
					if (statusFlag == TRUE) {
					    status = add_tag_to_delete_list (
							pMcell->mcTag.tag_num, 
							pMcell->mcBfSetTag.tag_num,
							&deleteFileListHead);
					    if (status != SUCCESS) {
						return status;
					    }
					} /* End if (statusFlag == TRUE) */
				    } /* End if (status != SUCCESS) */
				} /* End for loop over extent records */
				break;

			    default:
				/*
				 * Non extent based record.
				 */
				break;
			} /* End switch (pRecord->type) */
	    
			/*
			 * Set pRecord to next record of pMcell.
			 */
			pRecord = (bsMRT *) (((char *)pRecord) + 
					roundup(pRecord->bCnt, sizeof(uint64_t))); 
		    } /* End while ((NULL != overlapListCopyByLbn) && record exists ... */
		} /* End for (mcell = 0; mcell < BSPG_CELLS && ... */


		status = release_page_to_cache(currVol, currLbn, pCurrentPage,
					       FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}

	    }/* End for (bmtExtentPage = startPage; ... */
        } /* End for (extent = 0; extent < numExtents && ... */

	/*
	 * All overlap nodes should be gone.  If not, there is an error.
	 */

	if (overlapListCopyByLbn != NULL) {
	    lookupNode   = overlapListCopyByLbn;
	    overlapCount = 0;
	    while (lookupNode != NULL) {
		overlapCount++;
		lookupNode = lookupNode->nextByLbn;
	    }

	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FSCK_1, FSCK_699, 
			      "Internal Error: Volume %s still has %d overlap nodes.\n"),
		      domain->volumes[volNum].volName, overlapCount);
	    failed_exit();
	} /* End if (NULL != overlapListCopyByLbn) */
    } /* End for (volNum = 1; volNum <= domain->highVolNum; volNum++) */

    /*
     *  OK.  Now we have a list of all of the files that must
     *  be deleted in order to "correct" all of the overlaps.
     *  Go through the fileList and call delete_tag for each
     *  file.  Because delete_tag requires a pointer to the
     *  fileset that owns the file, if "this" node's fileset
     *  number doesn't match the previous node's fileset
     *  number, then we must run through the fileset list
     *  looking for the correct fileset structure.
     */

    currFile = deleteFileListHead;
    if (currFile == NULL) {
	/*
	 * No files to delete.
	 */
	return SUCCESS;
    }

    pFileset      = NULL;
    lastFilesetNo = -1;

    while (currFile != NULL) {
	if (currFile->setnum != lastFilesetNo) {
	    pFileset = domain->filesets;

	    while (pFileset != NULL) {
		if (pFileset->filesetId.dirTag.tag_num == currFile->setnum) {
		    break;
		}
		pFileset = pFileset->next;
	    }
	} /* End if (currFile->setnum != lastFilesetNo) */

	if (pFileset == NULL) {
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FSCK_1, FSCK_700,
			      "Internal Error: Invalid parent setTag (%ld) found in %s.\n"),
		      currFile->setnum, funcName);
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FSCK_1, FSCK_701,
			      "Detected on-disk corruption that cannot be fixed.\n"));
	    corrupt_exit(NULL);
	} /* End if (NULL == pFileset) */


	lastFilesetNo = currFile->setnum;

	status = delete_tag (pFileset, currFile->tagnum);
	if (status != SUCCESS) {
	    return status;
	}

	prevFile = currFile;
	currFile = currFile->next;

	/*
	 * Now free the node that was just processed.
	 */
	free (prevFile);
    } /* End while (NULL != currFile) */

    return FIXED;
} /* End correct_sbm_overlaps */


/*
 * Function Name: remove_extent_from_sbm (3.63)
 *
 * Description: This routine will clear bits of the in-memory SBM
 *		that correspond to the extent passed to it.  Also
 *		it will remove associated entries from the overlap
 *		list.  A check is made to see if the "freed" pages
 *		should still be flagged in the overlap bit map.
 *
 * Input parameters:
 *      domain:		 The domain being worked on.
 *      volNum:		 The number of the volume containing the extent.
 *      startLbn:	 The logical block number of the start of the extent.
 *      extentFobCount:  The number of FOBs in the extent.
 *      tag:		 The tag being worked on.
 *      settag:		 The settag being worked on.
 *	xtntMcell:	 The address of the mcell that claimed these pages.
 *	extentNumber:	 The extent number within the mcell record claimed...
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, NO_MEMORY, or FAILURE.
 * 
 */
int 
remove_extent_from_sbm(domainT  *domain,
		       volNumT  volNum,
		       lbnNumT  startLbn,
		       fobNumT  extentFobCount,
		       tagNumT  tag,
		       tagNumT  settag,
		       mcidT    *xtntMcell,
		       uint64_t extentNumber)
{
    char *funcName = "remove_extent_from_sbm";
    uint64_t startWord;    /* SBM index for the start of this extent. */
    uint64_t endWord;      /* SBM index for the end of this extent. */
    clustNumT startBit;  /* First Volume Cluster Number of this range. */
    clustNumT endBit;    /* Last Volume Cluster Number of this range. */
    clustNumT bitCount;
    clustNumT bitOffset;
    int64_t clusters;      /* note: signed cluster count */
    uint32_t startMask;
    uint32_t endMask;
    uint32_t mask;
    const uint32_t allSet = UINT_MAX;
    uint64_t i;
    uint64_t overlapSize;
    int  status;
    clustNumT    extentFirstVolClust;
    clustNumT    extentLastVolClust;
    overlapNodeT **fileListHead;    /* Pointer to an overlap list. */
    overlapNodeT **lbnListHead;	    /* Pointer to an overlap list. */
    overlapNodeT *lookupNode;       /* Pointer to an overlap node. */
    overlapNodeT *overlay;          /* Pointer to new overlap node. */
    overlapNodeT *tempFileListHead; /* Pointer to an overlap list. */
    overlapNodeT *tempLbnListHead;  /* Pointer to an overlap list. */
    short   overlapFound;  /* Boolean - TRUE if overlaps detected. */
    uint32_t  *sbm;           /* Pointer to SBM array.   */
    uint32_t  *obm; /* Pointer to overlapping bitmap array. */
    volumeT *volume;        /* Pointer to selected volume structure.*/
    fobtoLBNT extentArray[2];

    /*
     * Test for sanity in the input arguments.
     *   First,  the starting lbn must be on a page boundary.
     *   Second, the volume must be open.
     *   Third,  we must have an in-memory SBM present.
     */

    if ((startLbn == XTNT_TERM) || (startLbn == (uint64_t)PERM_HOLE_START)) {
	return SUCCESS;
    }

    volume  = &(domain->volumes[volNum]);

    assert(startLbn % ADVFS_BS_CLUSTSIZE == 0);

    writemsg(SV_VERBOSE | SV_LOG_INFO,
	     catgets(_m_catd, S_FSCK_1, FSCK_705, 
		     "Mcell (%d,%ld,%d) extent %ld for tag %ld in FS tag %ld, is being removed from volume %s. (%ld FOBs starting at LBN %ld)\n"),
	     xtntMcell->volume, xtntMcell->page, xtntMcell->cell,
	     extentNumber,
	     tag, settag, volume->volName, extentFobCount, startLbn);

    sbm = volume->volSbmInfo->sbm;
    obm = volume->volSbmInfo->overlapBitMap;

    fileListHead  = &volume->volSbmInfo->overlapListByFile;
    lbnListHead   = &volume->volSbmInfo->overlapListByLbn;

    overlapFound  = FALSE;

    extentFirstVolClust = LBN2CLUST(startLbn);
    extentLastVolClust  = extentFirstVolClust + FOB2CLUST(extentFobCount - 1);

    /* 
     * Ensure that the temporary lists are empty. 
     */
    tempFileListHead = NULL;
    tempLbnListHead  = NULL;

    /*
     * Break Volume Page Numbers into SBM indicies and bit offsets.
     */

    bitOffset = LBN2CLUST(startLbn);
    startBit = bitOffset % SBM_BITS_LONG;
    startWord = bitOffset / SBM_BITS_LONG;
    bitCount = FOB2CLUST(extentFobCount);
    endWord = (bitOffset + bitCount -1) / SBM_BITS_LONG;
    endBit = bitOffset + bitCount - 1;    /* ending bit number */

    /* 
     * determine if an overlap is present
     */

    if (startWord == endWord) {
	/*
	 * If the bit range is confined to one SBM word, this is
	 * easy...
	 */

	mask = allSet << startBit;
	mask &= allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1)
			   - endBit);

	if (obm[startWord] & mask) {
	    overlapFound = TRUE;
	}

    } else {

	/* 
	 * The bit range crosses SBM word boundaries (possibly more
	 * than one).  Deal with it.
	 */

	startMask = allSet << startBit;

	if (obm[startWord] & startMask) {
	    overlapFound = TRUE;
	}

	for (i = startWord + 1; i < endWord; i++) {

	    if (obm[i] & allSet) {
		overlapFound = TRUE;
	    }
	}

	endMask = allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1)
			     - endBit);

	if (obm[endWord] & endMask) {
	    overlapFound = TRUE;
	}
    }

    /*  
     * If no overlaps were detected, simply clear the bits in the SBM
     * and return.  There is no need to touch the overlap bit map.  
     */

    if (overlapFound == FALSE) {
	sbm[startWord] &= ~startMask;

	for (i = startWord + 1; i < endWord; i++) {
	    sbm[i] = 0;
	}

	if (startWord != endWord) {
	    sbm[endWord] &= ~endMask;
	}

	return SUCCESS;

    } /* End if (overlapFound == FALSE) */

    writemsg (SV_VERBOSE | SV_LOG_INFO,
	      catgets(_m_catd, S_FSCK_1, FSCK_706, 
		      "Mcell (%d,%ld,%d) extent %ld is overlapped on volume %s.\n"),
	      xtntMcell->volume, xtntMcell->page, xtntMcell->cell,
	      extentNumber,
	      volume->volName);

    /*
     * Process the overlap data. 
     *
     * To get here, there must be at least one overlapping page.  Rather
     * than go through the bits one at a time, the idea is to simply
     * re-construct the overlap node data.
     *
     * Begin by finding all overlap nodes that are for >this< extent.
     * Insert these into the temporary overlap file list, and remove
     * them from the "real" overlap list.  Note that because the
     * overlap nodes are linked in two separate lists (by file and
     * by LBN) two separate searches of the overlap list are required.
     *
     *  NOTE:  A set of "prev" pointers would be >really< helpful 
     *         here, and would improve the performance substantially.
     *
     * Next search the list by LBN, and find all overlapping nodes that
     * reference the pages mapped by the extent being removed.  Insert
     * these nodes into the temporary overlap LBN list.  Again, these
     * nodes must be removed from both lists.
     *
     * Note that for the temporary lists there is no need to link the
     * nodes both by file and by LBN.  For simplicity's sake, we'll only
     * insert them into the list by file.  (This should be the "natural"
     * order anyway.)
     *
     * Now work through the temporary overlap file list from the
     * beginning to the end.  The existence of an overlap node for this
     * extent indicates that some portion (or all) of the extent was not
     * the first claimant of these pages.
     *
     * For each entry found:
     *
     * Any pages between the "last first allocated pages" and the start
     * of this overlap section had first claim on the pages.  Those bits
     * need to be cleared in the SBM, as well as in the overlap bit map.
     * (More later.)
     *
     * Save the last overlapped page + 1 as the "last first allocated
     * pages" before processing the next node.
     *
     * Remove this node from the temporary file list, and free the node.
     *
     * At the end of the loop, clear any bits between the "last free..."
     * and the end of the extent.
     *
     * The question now is: What should be done for the "overlapped"
     * pages?  The answer is that we simply clear all of the overlapped
     * bits covered by this extent.  Some other extent claimed those
     * pages, but we don't know (or perhaps care) which extent that was.
     *
     * The next step is to work our way through the overlapped LBN list.
     * For each node, the area to be checked must be bound by the limits
     * of the extent being removed.
     *
     * Presumably only overlap nodes that somehow reference this part of
     * the bit map will be in the list.
     *
     * Processing of this list will be very similar to the code in the
     * collect_sbm_info routine.  The difference will basically surround
     * the following issue:
     *
     * Part of an overlap node may now be the "first claimant" to pages
     * freed by the deleted extent.  Such a node may need to be removed
     * entirely, shortened (on either end or both ends), or split into
     * multiple new overlap nodes.
     */


    lookupNode = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
    if (lookupNode == NULL) {
	writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FSCK_1, FSCK_10, 
			  "Can't allocate memory for %s.\n"),
		  catgets(_m_catd, S_FSCK_1, FSCK_687, 
			  "overlap node"));
	return NO_MEMORY;
    }

    lookupNode->xtntId.cellAddr	        = *xtntMcell;
    lookupNode->xtntId.extentNumber	= extentNumber;
    lookupNode->tagnum			= tag;
    lookupNode->setnum			= settag;
    lookupNode->extentStartLbn  	= startLbn;
    lookupNode->extentClustCount 	= FOB2CLUST(extentFobCount);
    lookupNode->overlapStartClust	= 0;
    lookupNode->overlapClustCount	= 0;
    lookupNode->nextByMcell		= NULL;
    lookupNode->nextByLbn		= NULL;

    overlay = NULL;

    /*
     * The remove_overlap_node_from_lists routine modifies the list,
     * which may result in it becoming empty.
     */

    while (*fileListHead != NULL) {
	status = find_overlap_node_by_mcell(*fileListHead, lookupNode, 
					    &overlay);
	if (status == NOT_FOUND) {
	    break;
	} else if (status != SUCCESS) {
	    return status;
	}

	writemsg (SV_VERBOSE | SV_LOG_INFO,
		  catgets(_m_catd, S_FSCK_1, FSCK_707, 
			  "Mcell (%d,%ld,%d) extent %ld for tag %ld of FS tag %ld overlaps %ld SBM clusters of other data starting at LBN %ld.\n"),
		  overlay->xtntId.cellAddr.volume,
		  overlay->xtntId.cellAddr.page,
		  overlay->xtntId.cellAddr.cell,
		  overlay->xtntId.extentNumber,
		  overlay->tagnum,
		  overlay->setnum,
		  overlay->overlapClustCount,
		  CLUST2LBN(overlay->overlapStartClust));


	status = remove_overlap_node_from_lists(overlay,
						fileListHead,
						lbnListHead);
	if (status == NOT_FOUND) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_708,
			     "Internal Error: Failed to remove node from overlap lists.\n"));
	    return FAILURE;
	} else if (status != SUCCESS) {
	    return status;
	}

	status = insert_overlap_data_by_mcell(&tempFileListHead, overlay);
	if (status != SUCCESS) {
	    return status;
	}
    }

    /*
     * The remove_overlap_node_from_lists routine modifies the list,
     * which may result in it becoming empty.
     */

    while (*lbnListHead != NULL) {
	status = find_overlap_node_by_lbn(*lbnListHead, lookupNode, &overlay);
	if (status == NOT_FOUND) {
	    break;
	} else if (status != SUCCESS) {
	    return status;
	}

	writemsg (SV_VERBOSE | SV_LOG_INFO,
		  catgets(_m_catd, S_FSCK_1, FSCK_709,
			  "Extent in mcell (%d,%ld,%d) overlapped by mcell (%d,%ld,%d) at LBN %d for %d FOBs.\n"),
		  xtntMcell->volume, xtntMcell->page, xtntMcell->cell,
		  overlay->xtntId.cellAddr.volume,
		  overlay->xtntId.cellAddr.page,
		  overlay->xtntId.cellAddr.cell,
		  CLUST2LBN(overlay->overlapStartClust),
		  CLUST2FOB(overlay->overlapClustCount));

	status = remove_overlap_node_from_lists (overlay,
						 fileListHead,
						 lbnListHead);
	if (status == NOT_FOUND) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_708,
			     "Internal Error: Failed to remove node from overlap lists.\n"));
	    return FAILURE;
	} else if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Insert the node into a list linked by Mcell ID, not LBN.
	 */
	status = insert_overlap_data_by_mcell(&tempLbnListHead, overlay);
	if (status != SUCCESS) {
	    return status;
	}
    }

    /*
     * Free the lookup node...
     */
    free(lookupNode);

    /*
     * We have now removed all nodes from the overlap lists that
     * reference pages within the extent to be removed.  The
     * next step is to clear pages of the SBM that were "owned"
     * by the extent to be removed.
     */

    startBit = LBN2CLUST(startLbn);

    while (tempFileListHead) {

	overlay = tempFileListHead;
	tempFileListHead = overlay->nextByMcell;

	startBit = overlay->overlapStartClust;
	endBit = startBit + overlay->overlapClustCount;
	clusters = endBit - startBit + 1;

	if (clusters > 0) {
	    /*
	     * The clusters from startBit to endBit were first
	     * "owned" by this extent.  They must now be marked free
	     * in the SBM.
	     */

	    writemsg (SV_DEBUG | SV_LOG_INFO,
		      catgets(_m_catd, S_FSCK_1, FSCK_710,
			      "Marking %ld clusters free in the SBM for volume %s, starting at cluster %ld.\n"),
		      bitCount, volume->volName, startBit);

	    startWord = startBit / SBM_BITS_LONG;
	    endWord = endBit / SBM_BITS_LONG;
	    
	    if (startWord == endWord) {

		mask = allSet << startBit % SBM_BITS_LONG;
		mask &= allSet >> (((endWord * SBM_BITS_LONG) + 
				    SBM_BITS_LONG - 1) - endBit);
		sbm[startWord] = ~mask;

	    } else {

		startMask = allSet << startBit % SBM_BITS_LONG;
		sbm[startWord] = ~startMask;

		for (i = startWord + 1; i < endWord; i++)
		    sbm[i] = 0;
	    
		endMask = allSet >> (((endWord * SBM_BITS_LONG) + 
				      SBM_BITS_LONG - 1) - endBit);
		sbm[endWord] = ~endMask;
	    }
	}
	
	free(overlay);

    } /* End while (NULL != tempFileListHead) */


    /*
     * Now process the final portion of the range.
     * These pages were not processed by the loop, as it only deals
     * with pages that are "in front" of the overlap node.
     */

    startBit = endBit + 1;

    if (startBit <= extentLastVolClust) {

	endBit = extentLastVolClust;
	clusters = endBit - startBit + 1;

	if (clusters > 0) {
	    /*
	     * The pages between from rangeStartClust to rangeEndPage
	     * were first "owned" by this extent.  These also must be
	     * marked free in the SBM.
	     */

	    writemsg (SV_DEBUG | SV_LOG_INFO,
		      catgets(_m_catd, S_FSCK_1, FSCK_710, 
			      "Marking %ld clusters free in the SBM for volume %s, starting at cluster %ld.\n"),
		      clusters, volume->volName, startBit);

	    startWord = startBit / SBM_BITS_LONG;
	    endWord = endBit / SBM_BITS_LONG;

	    if (startWord == endWord) {

		mask = allSet << startBit % SBM_BITS_LONG;
		mask &= allSet >> (((endWord * SBM_BITS_LONG) + 
				    SBM_BITS_LONG - 1) - endBit);
		sbm[startWord] = ~mask;

	    } else {

		startMask = allSet << startBit % SBM_BITS_LONG;
		sbm[startWord] = ~startMask;

		for (i = startWord + 1; i < endWord; i++)
		    sbm[i] = 0;
	    
		endMask = allSet >> (((endWord * SBM_BITS_LONG) + 
				    SBM_BITS_LONG - 1) - endBit);
		sbm[endWord] = ~endMask;
	    }

	} /* End if (clusters) */
    } /* End if (startBit < extentLastVolClust) */


    /*
     * Now clear the bits in the overlap bitmap that map this extent.
     * This removes all traces of this extent from the bitmaps.  This
     * must be done before the remaining overlap nodes are collected.
     */

    bitOffset = LBN2CLUST(startLbn);
    startBit = bitOffset % SBM_BITS_LONG;
    startWord = bitOffset / SBM_BITS_LONG;
    bitCount = FOB2CLUST(extentFobCount);
    endWord = (bitOffset + bitCount -1) / SBM_BITS_LONG;
    endBit = bitOffset + bitCount - 1;    /* ending bit number */

    if (startWord == endWord) {
	
	mask = allSet << startBit;
	mask &= allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1) 
			   - endBit);
	obm[startWord] = ~mask;

    } else {

	mask = allSet << startBit;
	obm[startWord] = ~startMask;

	for (i = startWord + 1; i < endWord; i++)
	    obm[i] = 0;
	    
	endMask = allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1) 
			     - endBit);
	obm[endWord] = ~endMask;
    }


    /*
     * Now it is time to re-process the nodes that overlapped the
     * extent that was removed.
     *
     *
     * The next step is to work our way through the overlapped LBN list.
     * Each node is logically broken into three sections:
     *
     *		1 - pages in front of the removed extent
     *		2 - pages overlapping the removed extent
     *		3 - pages beyond the removed extent
     *
     * 
     * Cases 1 and 3 will be handled by creating a new overlap node
     * mapping the particular pages outside the range of the extent.
     * The new overlap node(s) will then be inserted into the overlap
     * lists.
     *
     * NOTE:	The "extent" information for these nodes will actually
     *		be the range of pages "checked" when creating this node.
     *		As a result, they will have the same values as the
     *		overlap data.
     *
     * The pages within the removed extent will be re-collected by
     * calling collect_sbm_info.
     */

    while (tempLbnListHead != NULL) {
	lookupNode = tempLbnListHead;
	tempLbnListHead = lookupNode->nextByMcell;

	/*
	 * The range<xxx> variables are used to define the portion of
	 * the bitmap that must be examined for this overlap node.
	 *
	 * The range starts on the greater of the current overlap node
	 * start page and the first volume page of the extent.
	 *
	 * The range ends on the lesser of the current overlap node
	 * end page, or the last volume page of the extent.
	 *
	 * Handle Case 1:
	 *	The lookupNode refers to an overlap that begins
	 *	before the start of the removed extent.
	 */

	if (lookupNode->overlapStartClust < extentFirstVolClust) {
	    overlapSize = extentFirstVolClust - lookupNode->overlapStartClust;

	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (NULL == overlay) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_10,
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FSCK_1, FSCK_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlay->xtntId	       = lookupNode->xtntId;
	    overlay->tagnum	       = lookupNode->tagnum;
	    overlay->setnum	       = lookupNode->setnum;
	    overlay->overlapStartClust = lookupNode->overlapStartClust;
	    overlay->overlapClustCount = overlapSize;
	    overlay->extentStartLbn    = CLUST2LBN(overlay->overlapStartClust);
	    overlay->extentClustCount  = overlay->overlapClustCount;
	    overlay->nextByMcell       = NULL;
	    overlay->nextByLbn	       = NULL;

	    status = insert_overlap_data_by_mcell(fileListHead, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = insert_overlap_data_by_lbn(lbnListHead, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    lookupNode->overlapStartClust = extentFirstVolClust;
	    lookupNode->overlapClustCount = lookupNode->overlapClustCount -
		                            overlapSize;

	    overlay		= NULL;
	    overlapSize		= 0;

	} /* End if (lookupNode->overlapStartClust < extentFirstVolClust) */


	/*
	 * Handle Case 3:
	 *	The lookupNode refers to an overlap that ends beyond
	 *	the end of the removed extent.
	 */

	if (extentLastVolClust <
	    (lookupNode->overlapStartClust + 
	     lookupNode->overlapClustCount - 1)) {

	    overlapSize = lookupNode->overlapStartClust +
			  lookupNode->overlapClustCount - 1 -
			  extentLastVolClust;

	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (NULL == overlay) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_10, 
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FSCK_1, FSCK_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlay->xtntId	       = lookupNode->xtntId;
	    overlay->tagnum	       = lookupNode->tagnum;
	    overlay->setnum	       = lookupNode->setnum;
	    overlay->overlapStartClust = extentLastVolClust + 1;
	    overlay->overlapClustCount = overlapSize;
	    overlay->extentStartLbn    = CLUST2LBN(overlay->overlapStartClust);
	    overlay->extentClustCount  = overlay->overlapClustCount;
	    overlay->nextByMcell       = NULL;
	    overlay->nextByLbn	       = NULL;

	    status = insert_overlap_data_by_mcell(fileListHead, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    status = insert_overlap_data_by_lbn(lbnListHead, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    lookupNode->overlapClustCount = lookupNode->overlapClustCount -
					    overlapSize;

	    overlay		= NULL;
	    overlapSize		= 0;

	} /* End if Case 3 */

	/*
	 * Finally, handle Case 2:
	 *	Collect the pages of the lookupNode that overlapped the
	 *	removed extent.
	 */
	
	/*
	 * Fake an extent Array to handle the overlap.
	 */
	extentArray[0].volume = volNum;
	extentArray[0].lbn    = CLUST2LBN(lookupNode->overlapStartClust);
	extentArray[0].fob    = 0;
	extentArray[1].volume = volNum;
	extentArray[1].lbn    = -1;
	extentArray[1].fob    = CLUST2FOB(lookupNode->overlapClustCount);

	status = collect_sbm_info (domain, extentArray, 2,
				   lookupNode->tagnum,
				   lookupNode->setnum,
				   &lookupNode->xtntId.cellAddr,
				   lookupNode->xtntId.extentNumber);

	if (status != SUCCESS) {
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FSCK_1, FSCK_711, 
			      "Failed to re-collect extent data for tag %ld, set %ld on volume %s.\n"),
		      lookupNode->tagnum,
		      lookupNode->setnum,
		      volume->volName);
	    return status;
	}

	/*
	 * Lastly, free the original overlap node...
	 */
	free (lookupNode);

    } /* End while (NULL != tempLbnListHead) */

    return SUCCESS;

} /* End remove_extent_from_sbm */


/*
 * Function Name: find_overlap_node_by_lbn (3.xx)
 *
 * Description: The purpose of this routine is to search the
 *		overlap list headed by listHead for any node
 *		that has an overlap area that overlaps the
 *		pages mapped by the input extent.
 *
 *		Note that the list allows duplicate keys.
 *		The logical key for the list is:
 *
 *		overlapStartClust, overlapSize, xtntId.cellAddr.volume, 
 *		xtntId.cellAddr.page, xtntId.cellAddr.cell,
 *		xtntId.extentNumber, (extentStartLbn, extentSize,)
 *		tagnum, setnum
 *
 * Input parameters:
 *     listHead:	A pointer to the head of a list sorted by the overlap LBN.
 *     extentNode:	A pointer to an overlapNode for the desired extent.
 *
 * Output parameters:
 *     foundNode:	A pointer to a matching overlap node from the list,
 *			or NULL if no matching node was found.
 *
 *
 * Returns: SUCCESS or NOT_FOUND.
 * 
 */
static int 
find_overlap_node_by_lbn(overlapNodeT *listHead, 
			 overlapNodeT *extentNode,
			 overlapNodeT **foundNode)
{
    char *funcName = "find_overlap_node_by_lbn";

    overlapNodeT *currNode;

    currNode   = listHead;
    *foundNode = NULL;

    while (currNode != NULL) {
	/*
	 *  If the overlap described by the current node ends before the
	 *  start of the desired extent, skip to the next node in the list.
	 */
	if ((currNode->overlapStartClust + currNode->overlapClustCount - 1) <
	    (extentNode->extentStartLbn / ADVFS_BS_CLUSTSIZE)) {
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 *  If the overlap described by the current node starts after the
	 *  end of the desired extent, then no nodes in the list overlap
	 *  the specified extent.
	 */
	if (currNode->overlapStartClust >
	   ((extentNode->extentStartLbn / ADVFS_BS_CLUSTSIZE) +
	     extentNode->extentClustCount - 1)) {
	    break;
	}

	/*
	 *  The current node >must< describe an overlap that references
	 *  some of the pages referenced by this extent.  Return this node.
	 */
	
	*foundNode = currNode;

	return SUCCESS;
    }

    /*
     *  To get here, either the end of the list was reached without
     *  detecting any nodes that referenced pages mapped by the extent,
     *  or a node was found that was beyond the extent area.
     */
    return NOT_FOUND;
} /* End find_overlap_node_by_lbn */


/*
 * Function Name: find_overlap_node_by_mcell (3.xx)
 *
 * Description: The purpose of this routine is to search the 
 *		overlap list headed by listHead for any node
 *		belonging to the mcell and extent passed in.
 *
 *		Note that the list allows duplicate keys.
 *		The logical key for the list is:
 *
 *		xtntId.cellAddr.volume,  xtntId.cellAddr.page,
 *		xtntId.cellAddr.cell, xtntId.extentNumber,
 *		tagnum, setnum, extentStartLbn, extentSize,
 *		overlapStartClust, overlapSize
 *
 *		Also, note that the extent and overlap data
 *		is treated as inexact for the find operation.
 *		A node is considered to match if the extent
 *		described by the node is a subset of the
 *		extentNode's extent.  The same is true for the
 *		overlap area fields.
 *
 * Input parameters:
 *     listHead:	A pointer to the head of a list sorted by mcell ID.
 *     extentNode:	A pointer to an overlapNode for the desired extent.
 *
 * Output parameters:
 *     foundNode:	A pointer to a matching overlap node from the list,
 *			or NULL if no matching node was found.
 *
 *
 * Returns: SUCCESS or NOT_FOUND.
 * 
 */
static int 
find_overlap_node_by_mcell(overlapNodeT *listHead, 
			   overlapNodeT *extentNode,
			   overlapNodeT **foundNode)
{
    char *funcName = "find_overlap_node_by_mcell";
    overlapNodeT *currNode;

    currNode   = listHead;
    *foundNode = NULL;

    /*
     * Walk through the list until a node is found that matches all of
     * the "exact match" information, or we've gotten past where
     * such a node might be fonnd.  If we do find a match, check for
     * a "fuzzy" match.
     */

    while (currNode != NULL) {
	/*
	 * The mcell volume number should be identical for every node
	 * on the list to the volume we are working on, but the test
	 * is left in "just in case."
	 */
	if (currNode->xtntId.cellAddr.volume > 
	    extentNode->xtntId.cellAddr.volume) {
	    break;
	} else if (currNode->xtntId.cellAddr.volume < 
		   extentNode->xtntId.cellAddr.volume) {
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The mcell volume fields match.  Next try the mcell page numbers.
	 */
	if (currNode->xtntId.cellAddr.page > extentNode->xtntId.cellAddr.page) {
	    break;
	} else if (currNode->xtntId.cellAddr.page < extentNode->xtntId.cellAddr.page) {
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The Mcell page numbers match.  Next check the Mcell cell numbers.
	 */
	if (currNode->xtntId.cellAddr.cell > extentNode->xtntId.cellAddr.cell) {
	    break;
	} else if (currNode->xtntId.cellAddr.cell < extentNode->xtntId.cellAddr.cell) {
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The Mcell cell numbers match.  Next check the extent numbers.
	 */
	if (currNode->xtntId.extentNumber > extentNode->xtntId.extentNumber) {
	    break;
	} else if (extentNode->xtntId.extentNumber > currNode->xtntId.extentNumber) {
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The extent numbers match.  Next check the tag numbers.
	 */
	if (currNode->tagnum > extentNode->tagnum) {
	    break;
	} else if (currNode->tagnum < extentNode->tagnum) {
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The tag numbers match.  Next check the fileset number.
	 */

	if (currNode->setnum > extentNode->setnum) {
	    break;
	} else if (currNode->setnum < extentNode->setnum) {
	    currNode = currNode->nextByMcell;
	    continue;
	}


	/*
	 * The fileset numbers match.
	 * Next try a "fuzzy" match of the extent data.
	 */

	/*
	 * Unlike finding overlaps that overlap an extent, here the
	 * idea is to find nodes that either exactly match the desired
	 * extent area, or that are a proper subset of that area.
	 * Thus the both ends overlap node's extent area must be within
	 * the range of the desired extent.
	 */

	if (currNode->extentStartLbn < extentNode->extentStartLbn) {
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The current node's extent area starts at the beginning of
	 * or beyond the desired extent's extent area.  Now check that
	 * it doesn't start beyond the end of the desired area.
	 */
	if ((currNode->extentStartLbn / ADVFS_BS_CLUSTSIZE) >
	    ((extentNode->extentStartLbn / ADVFS_BS_CLUSTSIZE) +
	      extentNode->extentClustCount - 1)) {
	    break;
	}

	/*
	 * Next check that the current node's extent area doesn't end
	 * beyond the end of the desired area.
	 */
	if (((currNode->extentStartLbn / ADVFS_BS_CLUSTSIZE) +
	      currNode->extentClustCount - 1) >
	    ((extentNode->extentStartLbn / ADVFS_BS_CLUSTSIZE) +
	      extentNode->extentClustCount - 1)) {
	    currNode = currNode->nextByMcell;
	    continue;
	}



	/*
	 * The node here must be a match.  There is no need to check
	 * the ends of the overlap range, because they must be a proper
	 * subset of the extent.
	 */

	/*
	 * All of the required data has been matched.  Return a pointer
	 * the current node.
	 */

	*foundNode = currNode;

	return SUCCESS;

    } /* End while (currNode != NULL) */

    return NOT_FOUND;

} /* End find_overlap_node_by_mcell */


/*
 * Function Name: insert_overlap_data_by_lbn (3.xx)
 *
 * Description: The purpose of this routine is to insert the
 *		overlap node into the list headed by listHead
 *		in the correct order.
 *
 *		Note that the list allows duplicate keys.
 *		The logical key for the list is:
 *
 *		overlapStartClust, overlapSize, xtntId.cellAddr.volume, 
 *		xtntId.cellAddr.page, xtntId.cellAddr.cell,
 *		xtntId.extentNumber, (extentStartLbn, extentSize,)
 *		tagnum, setnum
 *
 *
 * Input parameters:
 *     listHead:	A pointer to the head of a list sorted by lbn.
 *     node:		A pointer to the overlapNode to be added to the list.
 *
 * Output parameters: N/A
 *
 *
 * Returns: SUCCESS.
 * 
 */
static int 
insert_overlap_data_by_lbn(overlapNodeT **listHead, 
			   overlapNodeT *node)
{
    char *funcName = "insert_overlap_data_by_lbn";

    overlapNodeT *currNode;
    overlapNodeT *prevNode;

    /*
     * Check to see if the list has any data in it yet.
     * If not, then this is a simple case.
     * Else find the proper place to insert this node into the list.
     */
    if (*listHead == NULL) {
	*listHead = node;
	node->nextByLbn = NULL;
	return SUCCESS;
    }

    currNode = *listHead;
    prevNode = NULL;

    /*
     * Walk through the list until the proper place to insert
     * this node is found.
     */

    while (currNode != NULL) {
	/*
	 * Start by checking the overlap starting page numbers.
	 */
	if (currNode->overlapStartClust > node->overlapStartClust) {
	    break;
	} else if (currNode->overlapStartClust < node->overlapStartClust) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The overlaps begin on the same page.  Next try their sizes.
	 */
	if (currNode->overlapClustCount > node->overlapClustCount) {
	    break;
	} else if (currNode->overlapClustCount < node->overlapClustCount) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The overlaps begin and end in the same place.  For every node on
	 * the list, the mcell volume number should be identical to the
	 * number of the volume being worked.  The test is left in "just in case."
	 */
	if (currNode->xtntId.cellAddr.volume > 
	    node->xtntId.cellAddr.volume) {
	    break;
	} else if (currNode->xtntId.cellAddr.volume < 
		   node->xtntId.cellAddr.volume) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The Mcell volume numbers match.  (What a surprise!)
	 * Next check the Mcell page numbers.
	 */
	if (currNode->xtntId.cellAddr.page > node->xtntId.cellAddr.page) {
	    break;
	} else if (currNode->xtntId.cellAddr.page < node->xtntId.cellAddr.page) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The Mcell page numbers match.  Next check the Mcell cell numbers.
	 */
	if (currNode->xtntId.cellAddr.cell > node->xtntId.cellAddr.cell) {
	    break;
	} else if (currNode->xtntId.cellAddr.cell < node->xtntId.cellAddr.cell) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The Mcell IDs match.  Next check the extent numbers.
	 */
	if (currNode->xtntId.extentNumber > node->xtntId.extentNumber) {
	    break;
	} else if (currNode->xtntId.extentNumber < node->xtntId.extentNumber) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The extent numbers match.  Next try the extent data.
	 */
	if (currNode->extentStartLbn > node->extentStartLbn) {
	    break;
	} else if (currNode->extentStartLbn < node->extentStartLbn) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	if (currNode->extentClustCount > node->extentClustCount) {
	    break;
	} else if (currNode->extentClustCount < node->extentClustCount) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The extents begin and end at the same place.
	 * Next compare the tag numbers.
	 */
	if (currNode->tagnum > node->tagnum) {
	    break;
	} else if (currNode->tagnum < node->tagnum) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * The tag numbers match.  Finally check the fileset numbers.
	 */
	if (currNode->setnum > node->setnum) {
	    break;
	} else if (currNode->setnum < node->setnum) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 * All of the positioning data has been matched.  Simply
	 * insert the node after the current node.
	 */

	node->nextByLbn     = currNode->nextByLbn;
	currNode->nextByLbn = node;

	return SUCCESS;
    } /* End while (currNode != NULL) */

    /*
     * Upon exit from the preceeding loop, either currNode is NULL,
     * or one of the fields of currNode > the same field of node.
     *
     * If currNode is NULL, then we simply add the node to the end
     * of the list.
     *
     * Otherwise insert the node between prevNode and currNode;
     */

    if (currNode == NULL) {
	/* 
	 * Link this node onto the end of the list. 
	 * Note: previous node cannot be NULL, because that case
	 * was handled at the top of the routine.
	 */
	prevNode->nextByLbn = node;
	node->nextByLbn     = NULL;
    } else {
	/* 
	 * Insert the node between prevNode and currNode.
	 */
	if (prevNode == NULL) {
	    /* 
	     * This becomes the head of the list. 
	     */
	    node->nextByLbn = *listHead;
	    *listHead       = node;
	} else {
	    node->nextByLbn     = prevNode->nextByLbn;
	    prevNode->nextByLbn = node;
	}
    } /* End if (currNode == NULL) */
    return SUCCESS;
} /* End insert_overlap_data_by_lbn */


/*
 * Function Name: insert_overlap_data_by_mcell (3.xx)
 *
 * Description: The purpose of this routine is to insert the
 *		overlap node into the list headed by listHead
 *		in the correct order.
 *
 *		Note that the list allows duplicate keys.
 *		The logical key for the list is:
 *
 *		xtntId.cellAddr.volume,  xtntId.cellAddr.page,
 *		xtntId.cellAddr.cell, xtntId.extentNumber,
 *		tagnum, setnum, extentStartLbn, extentSize,
 *		overlapStartClust, overlapSize
 *
 * Input parameters:
 *     listHead:	A pointer to the head of a list sorted by file.
 *     node:		A pointer to the overlapNode to be added to the list.
 *
 * Output parameters: N/A
 *
 *
 * Returns: SUCCESS.
 * 
 */
static int 
insert_overlap_data_by_mcell(overlapNodeT **listHead, 
			     overlapNodeT *node)
{
    char *funcName = "insert_overlap_data_by_mcell";
    overlapNodeT *currNode;
    overlapNodeT *prevNode;

    /*
     * Check to see if the list has any data in it yet.
     * If not, then this is a simple case.
     * Else find the proper place to insert this node into the list.
     */
    if (*listHead == NULL) {
	*listHead = node;
	node->nextByMcell = NULL;
	return SUCCESS;
    }

    currNode = *listHead;
    prevNode = NULL;

    /*
     * Walk through the list until the proper place to insert
     * this node is found.
     */

    while (currNode != NULL) {
	/*
	 * The mcell volume number should be identical for every node
	 * on the list to the volume we are working on, but the test
	 * is left in "just in case."
	 */
	if (currNode->xtntId.cellAddr.volume > node->xtntId.cellAddr.volume) {
	    break;
	} else if (currNode->xtntId.cellAddr.volume < 
		   node->xtntId.cellAddr.volume) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The Mcell volume numbers match.  Next check the Mcell page numbers.
	 */
	if (currNode->xtntId.cellAddr.page > node->xtntId.cellAddr.page) {
	    break;
	} else if (currNode->xtntId.cellAddr.page < node->xtntId.cellAddr.page) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The Mcell page numbers match.  Next check the Mcell cell numbers.
	 */
	if (currNode->xtntId.cellAddr.cell > node->xtntId.cellAddr.cell) {
	    break;
	} else if (currNode->xtntId.cellAddr.cell < node->xtntId.cellAddr.cell) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The Mcell cell numbers match.  Next check the extent numbers.
	 */
	if (currNode->xtntId.extentNumber > node->xtntId.extentNumber) {
	    break;
	} else if (currNode->xtntId.extentNumber < node->xtntId.extentNumber) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The extent numbers match.  Next check the tag numbers.
	 */
	if (currNode->tagnum > node->tagnum) {
	    break;
	} else if (currNode->tagnum < node->tagnum) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The tag numbers match.  Next check the fileset numbers.
	 */
	if (currNode->setnum > node->setnum) {
	    break;
	} else if (currNode->setnum < node->setnum) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The fileset numbers match.  Next try the extent data.
	 */
	if (currNode->extentStartLbn > node->extentStartLbn) {
	    break;
	} else if (currNode->extentStartLbn < node->extentStartLbn) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	if (currNode->extentClustCount > node->extentClustCount) {
	    break;
	} else if (currNode->extentClustCount < node->extentClustCount) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The extent data matches.  Next check the overlap starting
	 * page numbers.
	 */
	if (currNode->overlapStartClust > node->overlapStartClust) {
	    break;
	} else if (currNode->overlapStartClust < node->overlapStartClust) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The overlaps begin on the same page.  Next try their sizes.
	 */
	if (currNode->overlapClustCount > node->overlapClustCount) {
	    break;
	} else if (currNode->overlapClustCount < node->overlapClustCount) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * All of the positioning data has been matched.  Simply
	 * insert the node after the current node.
	 */

	node->nextByMcell     = currNode->nextByMcell;
	currNode->nextByMcell = node;

	return SUCCESS;
    }

    /*
     * Upon exit from the preceeding loop, either currNode is NULL,
     * or one of the fields of currNode > the same field of node.
     *
     * If currNode is NULL, then we simply add the node to the end
     * of the list.
     *
     * Otherwise insert the node between prevNode and currNode;
     */

    if (currNode == NULL) {
	/* 
	 * Link this node onto the end of the list. 
	 * Note: previous node cannot be NULL, because that case
	 * was handled at the top of the routine.
	 */
	prevNode->nextByMcell = node;
	node->nextByMcell     = NULL;
    } else {
	/* 
	 * Insert the node between prevNode and currNode.
	 */
	if (prevNode == NULL) {
	    /* 
	     * This becomes the head of the list. 
	     */
	    node->nextByMcell = *listHead;
	    *listHead         = node;
	} else {
	    node->nextByMcell     = prevNode->nextByMcell;
	    prevNode->nextByMcell = node;
	}
    }
    return SUCCESS;
} /* End insert_overlap_data_by_mcell */


/*
 * Function Name: is_page_free
 *
 * Description: This routine checks the in-memory SBM to see if the
 *		page passed in is free or in use.
 *
 *		FALSE is returned either if the page is in use,
 *		or if any of the parameters is invalid.
 *
 * Input parameters:
 *      domain:  The domain being worked on.
 *      volume:  The number of the volume containing the page.
 *      pageLBN: The logical block number of the Volume Page to check.
 *
 * Output parameters: N/A
 *
 * Returns: TRUE or FALSE.
 *
 */
int 
is_page_free(domainT *domain, 
	     volNumT volNum, 
	     lbnNumT pageLBN)
{
    char *funcName = "is_page_free";
    int  offset;	/* Bit-offset within word for 
			   building the page mask. */
    uint64_t sbmIndex;	/* Index into the SBM for this page.  */
    uint32_t pageMask;
    uint32_t *sbm;	/* Pointer to SBM array.   */
    volumeT  *volume;	/* Pointer to selected volume structure. */

    /*
     * Test for sanity in the input arguments.
     *   First,  the starting LBN must be on a page boundary.
     *   Second, the volume must be open.
     *   Third,  the page number must be valid for the volume.
     *   Fourth, we must have an in-memory SBM present.
     */
    volume  = &(domain->volumes[volNum]);

    assert(volume->volFD != -1);
    assert(volume->volSbmInfo != NULL);

    if ((pageLBN % ADVFS_BS_CLUSTSIZE) != 0) {
	writemsg (SV_DEBUG, 
		  catgets(_m_catd, S_FSCK_1, FSCK_702, 
			  "Internal Error in %s: LBN argument (%ld) is not on an SBM cluster boundary.\n"),
		  funcName, pageLBN);
	return FALSE;
    }

    if (pageLBN > volume->volSize) {
	writemsg (SV_DEBUG,
		  catgets(_m_catd, S_FSCK_1, FSCK_703, 
			  "Internal Error: LBN (%ld) is larger than volume size.\n"), pageLBN);
	return FALSE;
    }

    sbm = volume->volSbmInfo->sbm;

    /* 
     * Convert the page LBN to an SBM index and bit offset. 
     */
    sbmIndex = LBN2CLUST(pageLBN) / SBM_BITS_LONG;
    offset = LBN2CLUST(pageLBN) % SBM_BITS_LONG;

    /* 
     * Construct a bit mask.   
     */
    pageMask = 1 << offset;

    /* 
     * Now test if this bit is set. 
     */

    if ((sbm[sbmIndex] & pageMask) != 0) {
	return FALSE;
    }

    /*
     * Well, it passes all of the tests. The page must be free.
     */
    return TRUE;

} /* End is_page_free */


/*
 * Function Name: look_for_overlapped_clust_owner
 *
 * Description: In the current design of fixfdmn, it is not known
 *		which file "owns" a given page on disk.  If a page
 *		is overlapped, though, fixfdmn saves the identity
 *		of the overlapping file.
 *
 *		Because we cannot know before hand which file is the
 *		"true owner" of an overlapped page, fixfdmn will
 *		delete all files that contain overlapped pages.  (As
 *		of the time that correct_sbm_overlaps is called.)
 *
 *		The purpose of this routine is to determine if >this<
 *		extent was the original claimant of overlapped pages.
 *		If >this< extent is on the overlap list for these
 *		pages, then it is NOT the original claimant of these
 *		pages.  If not, then the "owner" of these pages
 *		has been found.
 *
 *		As an optimization, even if this file owns pages within
 *		>this< extent, the status flag variable is set to TRUE
 *		if, and only if, the file does not appear anywhere on
 *		the overlap list.
 *
 *		--------------------------
 *
 *		The first thing done by this routine is to check the
 *		overlap bit map to see if any pages within this extent
 *		were overlapped.  If not, then the routine is finished.
 *
 *		If an overlap was found, then the temporary overlap list
 *		is checked for nodes that reference any of the pages
 *		within this extent.  Matching nodes are placed onto
 *		one of two lists:  the Mcell list, or the LBN list.
 *		Nodes are placed onto the Mcell list if the Mcell ID
 *		matches that passed in; otherwise they are placed onto
 *		the LBN list.
 *
 *		If no nodes are found, then these pages must already
 *		have been found, so the routine returns SUCCESS.
 *
 *		If no nodes are found for this Mcell, then this Mcell
 *		"owns" these pages, but it does not appear on the list
 *		of files to be deleted.  Signal that this file should
 *		be added to the "files to be deleted" list.
 *
 *		Lastly, all references to the "owned" pages are removed
 *		from the overlap list.
 *
 *
 * Input parameters:
 *      domain:		 The domain being worked on.
 *      volNum:		 The number of the volume containing the pages.
 *      startBit:	 The cluster number of the first page in the range.
 *      bitCount:	 The number of clusters in the range.
 *      statausFlag:     see below
 *      overlapListHead: The overlap list to examine/modify.
 *	extentAddr:	 The address of the extent that claims these pages.
 *
 * Output parameters: N/A
 *
 * Input/Output parameters:
 *      statusFlag:	 A boolean flag.
 *
 *			 On input, is TRUE for AdvFS Reserved and pseudo-
 *			 reserved files.  For normal files it will be
 *			 FALSE.
 *
 *			 On output, is TRUE the file was overlapped, but
 *			 was not on the overlap list.  This means that it
 *			 should be added to the list of files to be deleted.
 *
 * Returns: SUCCESS, NO_MEMORY, FAILURE, or CORRUPT.
 * 
 */
static int 
look_for_overlapped_clust_owner(domainT	  *domain,
				volNumT   volNum,
				clustNumT bitOffset,
				clustNumT bitCount,
				int	  *statusFlag,
				overlapNodeT **overlapListHead,
				extentIdT *extentAddr)
{
    char *funcName = "look_for_overlapped_clust_owner";
    uint64_t  startWord; /* SBM index for the start of this extent. */
    uint64_t  endWord;   /* SBM index for the end of this extent. */
    clustNumT startBit;  /* Last Volume Cluster Number of this range. */
    clustNumT endBit;  /* Last Volume Cluster Number of this range. */
    uint32_t  mask;
    uint64_t  i;
    const uint32_t allSet = UINT_MAX;
    uint64_t overlapEndClust;   /* Cluster number of last overlapped page. */
    int      overlappedOnlyFlag;/* TRUE if extent is overlapped, but doesn't overlap. */
    uint64_t overlapSize;
    int      specialFileFlag;       /* Input status flag value. */
    int      status;
    overlapNodeT *currOverlapNode;      /* Pointer to an overlap node. */
    overlapNodeT *lookupNode;           /* Pointer to an overlap node. */
    overlapNodeT *lastLbnNode;          /* Pointer to an overlap node. */
    overlapNodeT *lastMcellNode;        /* Pointer to an overlap node. */
    overlapNodeT *overlay;              /* Pointer to new overlap node. */
    overlapNodeT *prevNonMatchingOverlapNode;  /* Pntr to an overlap node. */
    overlapNodeT *tempMcellListHead;    /* Pointer to an overlap list. */
    overlapNodeT *tempLbnListHead;      /* Pointer to an overlap list. */
    int          overlapFound;  /* Boolean - TRUE if overlaps detected. */
    uint32_t     *obm;           /* Pointer to overlapping bitmap array. */
    volumeT      *volume;        /* Pointer to selected volume structure.*/

    /*
     * Begin by handling the status flags:
     *	    saving the input status flag
     *	    set the overlapped-only flag to FALSE
     *	    set the output status to FALSE.
     */

    specialFileFlag    = *statusFlag;
    overlappedOnlyFlag = FALSE;
    *statusFlag        = FALSE;

    volume  = &(domain->volumes[volNum]);

    assert(volume->volFD != -1);
    assert(volume->volSbmInfo != NULL);

    obm = volume->volSbmInfo->overlapBitMap;
    overlapFound  = FALSE;

    /*
     * Break Volume Page Numbers into SBM indicies and bit offsets.
     */

    startBit = bitOffset % SBM_BITS_LONG;
    startWord = bitOffset / SBM_BITS_LONG;
    endBit = bitOffset + bitCount - 1;    /* ending bit number */
    endWord = endBit / SBM_BITS_LONG;

    /* 
     * determine if an overlap is present
     */

    if (startWord == endWord) {
	/*
	 * If the bit range is confined to one SBM word, this is
	 * easy...
	 */

	mask = allSet << startBit;
	mask &= allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1)
			   - endBit);

	if (obm[startWord] & mask) {
	    overlapFound = TRUE;
	}

    } else {

	/* 
	 * The bit range crosses SBM word boundaries (possibly more
	 * than one).  Deal with it.
	 */

	mask = allSet << startBit;

	if (obm[startWord] & mask) {
	    overlapFound = TRUE;
	}

	for (i = startWord + 1; i < endWord; i++) {

	    if (obm[i] & allSet) {
		overlapFound = TRUE;
	    }
	}

	mask = allSet >> (((endWord * SBM_BITS_LONG) + SBM_BITS_LONG - 1)
			  - endBit);

	if (obm[endWord] & mask) {
	    overlapFound = TRUE;
	}
    }

    /*  
     * If no overlaps were detected, return SUCCESS.
     */

    if (overlapFound == FALSE) {
	return SUCCESS;
    }

    /*
     * The volume overlap bit map shows that an overlap was detected
     * somewhere within this extent.  It may be that the owner of
     * these pages has already been found.  Check the overlap list
     * (not the one attached to the volume structure, but the one
     * passed in) for nodes that reference any of these pages.  If
     * no nodes are found, then no more processing is required.
     * Return SUCCESS.
     *
     * Begin by finding all overlap nodes that reference pages in
     * >this< extent.  Insert each node into one of the temporary
     * overlap lists: if this node's extent ID matches the extent
     * ID, then put it on the Mcell list, otherwise put it on
     * the LBN list.
     */

    lookupNode			= *overlapListHead;
    currOverlapNode		= NULL;
    lastLbnNode			= NULL;
    lastMcellNode		= NULL;
    prevNonMatchingOverlapNode	= NULL;

    /* 
     * Ensure that the temporary lists are empty. 
     */
    tempMcellListHead		= NULL;
    tempLbnListHead		= NULL;

    while (lookupNode != NULL) {
	/*
	 *  If the overlap described by the lookup node starts after
	 *  the end of the desired extent, then no more nodes in the
	 *  list overlap the specified extent.
	 */
	if (lookupNode->overlapStartClust > (startBit + bitCount - 1)) {
	    break;
	}

	/*
	 *  If the overlap described by the lookup node ends before
	 *  the start of the desired extent, skip to the next node
	 *  in the list.
	 */
	if ((lookupNode->overlapStartClust + 
	     lookupNode->overlapClustCount - 1) < startBit) {
	    prevNonMatchingOverlapNode = lookupNode;
	    lookupNode = lookupNode->nextByLbn;
	    continue;
	}

	/*
	 *  The current node must reference pages within this extent.
	 *
	 *  This node will become the new tail of one of the two
	 *  temporary overlap lists, so save a pointer to it in order
	 *  to truncate that list after it has been removed from the
	 *  overlap list.
	 *
	 *  If the extent ID of the overlap node matches this extent
	 *  ID, then add this node to the end of the temporary mcell
	 *  overlap list, otherwise add it to the end of the temporary
	 *  LBN overlap list.
	 */

	currOverlapNode = lookupNode;

	if ((extentAddr->cellAddr.volume == 
	     lookupNode->xtntId.cellAddr.volume)  &&
	    (extentAddr->cellAddr.page == lookupNode->xtntId.cellAddr.page) &&
	    (extentAddr->cellAddr.cell == lookupNode->xtntId.cellAddr.cell) &&
	    (extentAddr->extentNumber  == lookupNode->xtntId.extentNumber))  {

	    if (tempMcellListHead == NULL) {
		tempMcellListHead = lookupNode;
	    } else {
		lastMcellNode->nextByLbn = lookupNode;
	    }
	    lastMcellNode = lookupNode;
	} else {
	    writemsg (SV_VERBOSE | SV_LOG_INFO,
		      catgets(_m_catd, S_FSCK_1, FSCK_704, 
			      "Extent in mcell (%d,%ld,%d) overlaid by mcell (%d,%ld,%d) from LBN %ld for %ld blocks.\n"),
		      extentAddr->cellAddr.volume,
		      extentAddr->cellAddr.page,
		      extentAddr->cellAddr.cell,
		      lookupNode->xtntId.cellAddr.volume,
		      lookupNode->xtntId.cellAddr.page,
		      lookupNode->xtntId.cellAddr.cell,
		      lookupNode->overlapStartClust * ADVFS_BS_CLUSTSIZE,
		      lookupNode->overlapClustCount * ADVFS_BS_CLUSTSIZE);

	    if (tempLbnListHead == NULL) {
		tempLbnListHead = lookupNode;
	    } else {
		lastLbnNode->nextByLbn = lookupNode;
	    }
	    lastLbnNode = lookupNode;
	}

	/*
	 * Now remove the node from the overlap list.
	 */

	if (prevNonMatchingOverlapNode == NULL)  {
	    *overlapListHead = lookupNode->nextByLbn;
	} else {
	    prevNonMatchingOverlapNode->nextByLbn = lookupNode->nextByLbn;
	}

	/*
	 * Look up the next node on the overlap list.
	 */
	lookupNode = lookupNode->nextByLbn;

	/*
	 * Ensure that the tail of the temporary list does not point
	 * into the overlap list.
	 */
	currOverlapNode->nextByLbn = NULL;
	currOverlapNode            = NULL;
	
    } /* End while (NULL != lookupNode) */


    /*
     * If no matching overlap nodes were found, we're done.
     */

    if ((tempLbnListHead == NULL) && (tempMcellListHead == NULL)) {
	return SUCCESS;
    }

    /*
     * The nodes on the temporary Mcell list detail which pages
     * are "not owned" by this extent.  If there are no nodes on
     * this list, then the extent "owns" all of the pages.
     *
     * If there are no nodes for this Mcell on the overlap list, then
     * this file was overlaid by other files, but didn't overlay any
     * other files.  Therefore it may not appear in the files-to-be-
     * deleted list.  Set the flag to indicate that the file should
     * be added to the list of files which are to be deleted.
     */

    if (tempMcellListHead == NULL) {
	/*
	 * Set the flag to indicate that this extent was overlapped,
	 * but that it did not overlap any other extents.
	 */
	overlappedOnlyFlag = TRUE;
    }

    /*
     * Work through the temporary LBN overlap list.  For each node,
     * loop through the temporary Mcell overlap list and remove
     * clusters that are not declared overlapped in the Mcell overlap
     * list.  Some diagrams might help here.  These diagrams show a 40
     * cluster region.
     *
     * Clusters:               .123456789.123456789.123456789.123456789
     * Extent clusters:            aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
     * Extent Overlap Nodes:         oooo       oooo    oooooo
     * Overlap Node 1:          ...wwwww
     * Overlap Node 2:                        xxxx
     * Overlap Node 3:                                     yyyyy
     * Overlap Node 4:                        zzzzzzzzzzzzzzzzzzz......
     *
     * In this diagram, the extent starts at cluster 4 and runs for 30
     * clusters.  When this extent was collected, it generated 3
     * overlap nodes: the first at cluster 6 for 4 clusters, the
     * second at cluster 17 for 4 clusters, and the third at cluster
     * 25 for 6 clusters.
     *
     * Overlap Node 1 runs from cluster 1 for 8 clusters.  It overlaps
     * this extent (and possibly others) from cluster 4 for 5
     * clusters.  Note that this overlap begins before the first page
     * of the extent.  (That's what the dots indicate.)
     *
     * Overlap Node 2 overlapped this extent (and possibly others)
     * from cluster 15 for 4 clusters.  This overlap node starts
     * before this extent's second overlap node, but ends in the midst
     * of said node.
     *
     * Overlap Node 3 overlapped this extent (and possibly others)
     * from cluster 28 for 5 clusters.  This overlap node starts in the
     * middle of this extent's third overlap node, and ends beyond
     * the said overlap.
     *
     * Overlap Node 4 overlapped this extent (and at least one other)
     * from cluster 15 for 25 clusters.  This overlap node starts
     * before this extent's second overlap node, and ends beyond the
     * third extent.  Again, the dots denote overlapped pages outside
     * the range of this extent.
     *
     *
     * When finished with Overlap Node 1, there should be two nodes on
     * the overlap list: one beginning at cluster 1 for 3 clusters
     * (the last cluster before the start of the extent), and another
     * beginning at cluster 6 for 3 cluster.  (Clusters 4 and 5 were
     * "owned" by this extent.)
     *
     * Processing Overlap Node 2 should result in a single node on the
     * overlap list.  The overlap now starts at cluster 17 for 2
     * clusters.
     *
     * Processing Overlap Node 3 should also result in a single node
     * on the overlap list.  The new node should start at cluster 28
     * and have a size of 3 clusters.
     *
     * Processing Overlap Node 4 should result in three nodes on the
     * overlap list.  The first two nodes will have the same size and
     * location as the second and third overlap nodes for this extent.
     * The third node will begin at cluster 34 (beyond this extent)
     * and will run for 6 clusters.
     *
     *
     *
     * To simplify the code, when working through the temporary LBN overlap
     * list, each node will first be logically divided into three sections:
     *
     *		1 - clusters in front of the extent
     *		2 - clusters within the extent 
     *		3 - clusters beyond the esxtent
     *
     * Clusters in Cases 1 amd 3 are, by definition, not "owned" by
     * this extent.  These two cases are handled in the same way: a
     * new overlap node will be created for those clusters, and these
     * nodes will be added to the overlap list.
     *
     * For Case 2, only clusters matching the Extent Overlap nodes are
     * saved.
     */

    while (tempLbnListHead != NULL) {

	lookupNode      = tempLbnListHead;
	tempLbnListHead = lookupNode->nextByLbn;

	/*
	 * If the extent being processed is for a "special" file,
	 * then check to see if the overlap node is also for a
	 * "special" file.  If so, return CORRUPT!
	 */

	if (specialFileFlag == TRUE) {
	    if ((lookupNode->tagnum <= BFM_RSVD_TAGS) || 
		(lookupNode->setnum == (uint64_t)-2)) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_684, 
				  "Reserved file %d overlaps another reserved file.\n"),
			  lookupNode->tagnum);
		return CORRUPT;
	    }
	}

	overlapEndClust = (lookupNode->overlapStartClust +
			   lookupNode->overlapClustCount - 1);

	/*
	 * Handle case 1 pages first.
	 */

	if (lookupNode->overlapStartClust < startBit) {
	
	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (overlay == NULL) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_10,
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FSCK_1, FSCK_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlapSize = startBit - lookupNode->overlapStartClust;

	    overlay->xtntId              = lookupNode->xtntId;
	    overlay->tagnum		 = lookupNode->tagnum;
	    overlay->setnum		 = lookupNode->setnum;
	    overlay->overlapStartClust	 = lookupNode->overlapStartClust;
	    overlay->extentClustCount	 = overlay->overlapClustCount;
	    overlay->overlapClustCount	 = overlapSize;
	    overlay->extentStartLbn	 = overlay->overlapStartClust *
					   ADVFS_BS_CLUSTSIZE;
	    overlay->nextByMcell	 = NULL;
	    overlay->nextByLbn		 = NULL;

	    status   = insert_overlap_data_by_lbn (overlapListHead, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    lookupNode->overlapStartClust = startBit;
	    lookupNode->overlapClustCount = lookupNode->overlapClustCount - 
		                            overlapSize;
	    overlay		= NULL;
	    overlapSize		= 0;

	} /* End if (lookupNode->overlapStartClust < startBit) */

	/*
	 * Handle case 3 pages next.
	 */

	if (endBit < overlapEndClust) {
	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (overlay == NULL) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_10, 
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FSCK_1, FSCK_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlapSize = overlapEndClust - endBit;

	    overlay->xtntId	        = lookupNode->xtntId;
	    overlay->tagnum		= lookupNode->tagnum;
	    overlay->setnum		= lookupNode->setnum;
	    overlay->overlapStartClust	= endBit + 1;
	    overlay->overlapClustCount	= overlapSize;
	    overlay->extentStartLbn	= overlay->overlapStartClust *
		                          ADVFS_BS_CLUSTSIZE;
	    overlay->extentClustCount	= overlay->overlapClustCount;
	    overlay->nextByMcell	= NULL;
	    overlay->nextByLbn		= NULL;

	    status   = insert_overlap_data_by_lbn (overlapListHead, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    lookupNode->overlapClustCount = (lookupNode->overlapClustCount -
					     overlapSize);
	    overlapEndClust = endBit;
	    overlay		= NULL;
	    overlapSize		= 0;

	} /* End if (endBit < (lookupNode->overlapStartClust+ ...*/


	/*
	 * Now process the Case 2 pages.
	 * Only save overlap data for "unowned" pages.
	 *
	 * If there are no Extent Overlap nodes, then free whatever
	 * is left of the LBN Overlap node, and go on to the next
	 * LBN Overlap node.
	 */

	if (tempMcellListHead == NULL) {
	    free (lookupNode);
	    /*
	     * Iterate the while (NULL != tempLbnListHead) loop.
	     */
	    continue;
	}


	/*
	 * Go through the Extent Overlap list, and only save those parts of
	 * the LBN Overlap node that match the Extent Overlap nodes.
	 */

	currOverlapNode = tempMcellListHead;

	while (currOverlapNode != NULL) {

	    /*
	     * If the LBN Overlap node ends before the Extent Overlap node
	     * starts, all the pages were owned, so break out of this loop
	     * and free the LBN Overlap node.
	     */
	    if (overlapEndClust < currOverlapNode->overlapStartClust) {
		break;
	    }


	    /*
	     * Now if this LBN Overlap node starts after the end of the
	     * current Extent Overlap node, skip to the next Extent Overlap.
	     */

	    if (lookupNode->overlapStartClust >
		(currOverlapNode->overlapStartClust + 
		 currOverlapNode->overlapClustCount - 1)) {
		currOverlapNode = currOverlapNode->nextByLbn;
		/*
		 * Iterate the while (currOverlapNode != NULL) loop.
		 */
		continue;
	    }

	    /*
	     * This is very similar to the "outside the extent"
	     * processing.  First, pages before the Extent Overlap
	     * node are trimmed from the overlap node.
	     */

	    if (lookupNode->overlapStartClust <
		currOverlapNode->overlapStartClust) {

		/*
		 * Compute the number of pages to trim from the
		 * existing overlap node.
		 */
		overlapSize = currOverlapNode->overlapStartClust -
			      lookupNode->overlapStartClust;

		lookupNode->overlapStartClust = currOverlapNode->overlapStartClust;
		lookupNode->overlapClustCount -= overlapSize;

		if (lookupNode->overlapClustCount <= 0) {
		    /*
		     * Break out of the while (NULL != currOverlapNode)
		     * loop, and free the LBN overlap node.
		     *
		     * By the way, this code should never execute.
		     */
		    break;
		}
	    } /* End if (lookupNode->overlapStartClust < ... */


	    /*
	     * Now if this LBN Overlap node ends before the end of the
	     * current Extent Overlap node, add it to the overlap List
	     * and go on to the next LBN Overlap node.
	     */

	    if (overlapEndClust <
		(currOverlapNode->overlapStartClust +
		 currOverlapNode->overlapClustCount)) {

		status = insert_overlap_data_by_lbn (overlapListHead, lookupNode);
		if (status != SUCCESS) {
		    return status;
		}
		/*
		 * Clear the pointer to this node so that it won't be
		 * free'd after exiting this loop.
		 */
		lookupNode = NULL;

		/*
		 * Break out of the while (NULL != currOverlapNode) loop.
		 */
		break;
	    }


	    /*
	     * This LBN Overlap node ends beyond the current Extent
	     * Overlap node.  Divide it into two pieces:  the "left"
	     * half is placed into a new overlap node and is added
	     * to the overlap List.  The right half is saved in the
	     * current LBN Overlap node, and we check the next Extent
	     * Overlap node.
	     */

	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (overlay == NULL) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FSCK_1, FSCK_10,
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FSCK_1, FSCK_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlapSize = currOverlapNode->overlapStartClust +
			  currOverlapNode->overlapClustCount -
	    		  lookupNode->overlapStartClust;

	    overlay->xtntId	        = lookupNode->xtntId;
	    overlay->tagnum		= lookupNode->tagnum;
	    overlay->setnum		= lookupNode->setnum;
	    overlay->overlapStartClust	= lookupNode->overlapStartClust;
	    overlay->overlapClustCount	= overlapSize;
	    overlay->extentStartLbn	= overlay->overlapStartClust *
					  ADVFS_BS_CLUSTSIZE;
	    overlay->extentClustCount	= overlay->overlapClustCount;
	    overlay->nextByMcell	= NULL;
	    overlay->nextByLbn		= NULL;

	    status   = insert_overlap_data_by_lbn (overlapListHead, overlay);
	    if (status != SUCCESS) {
		return status;
	    }

	    lookupNode->overlapStartClust += overlapSize;
	    lookupNode->overlapClustCount -= overlapSize;

	    overlay	= NULL;
	    overlapSize = 0;
	    currOverlapNode = currOverlapNode->nextByLbn;

	} /* End while (currOverlapNode != NULL) */

	if (lookupNode != NULL) {
	    free (lookupNode);
	}

    } /* End while (tempLbnListHead != NULL) */

    /*
     * Now put the Extent Overlap nodes back onto the overlap list.
     */

    while (tempMcellListHead != NULL) {
	lookupNode = tempMcellListHead;
	tempMcellListHead = lookupNode->nextByLbn;

	status = insert_overlap_data_by_lbn (overlapListHead, lookupNode);
	if (status != SUCCESS) {
	    return status;
	}
    }

    /*
     * Finished processing this extent.
     */
    if ((overlappedOnlyFlag == TRUE) && (specialFileFlag == FALSE)) {
	*statusFlag = TRUE;
    }

    return SUCCESS;
} /* End look_for_overlapped_clust_owner */


/*
 * Function Name: remove_overlap_node_from_lists (3.xx)
 *
 * Description: The purpose of this routine is to remove the
 *		overlap node from the overlap lists fileListHead
 *		and lbnListHead.
 *
 *
 * Input parameters:
 *     overlapNode:  A pointer to the overlapNode to be removed from the lists.
 *     fileListHead: A pointer to the head of a list sorted by file.
 *     lbnListHead:  A pointer to the head of a list sorted by lbn.
 *
 * Output parameters:
 *     N/A
 *
 *
 * Returns: SUCCESS or NOT_FOUND
 * 
 */
static int 
remove_overlap_node_from_lists(overlapNodeT *overlapNode,
			       overlapNodeT **fileListHead,
			       overlapNodeT **lbnListHead)
{
    char *funcName = "remove_overlap_node_from_lists";
    overlapNodeT *currNode;
    overlapNodeT *prevNode;

    prevNode = NULL;
    currNode = *fileListHead;

    while (currNode != NULL) {
	if (currNode == overlapNode) {
	    break;
	}
	prevNode = currNode;
	currNode = currNode->nextByMcell;
    }

    /*
     * Upon exit from the preceeding loop, either currNode is NULL,
     * or currNode == overlapNode.
     *
     * If currNode is NULL, return NOT_FOUND.
     *
     * Otherwise, remove the node from the list by setting the previous node's
     * nextByMcell pointer to the current node's nextByMcell pointer.  Then clear
     * the current node's nextByMcell pointer.
     */

    if (currNode == NULL) {
	return NOT_FOUND;
    } else {
	/* 
	 * Remove the current node from the list.
	 */
	if (prevNode == NULL) {
	    /* 
	     * The current node was the head of the list. 
	     */
	    *fileListHead = currNode->nextByMcell;
	} else {
	    prevNode->nextByMcell = currNode->nextByMcell;
	}
	currNode->nextByMcell = NULL;
    }

    /*
     *  Next search the LBN list.
     */
    prevNode = NULL;
    currNode = *lbnListHead;

    while (currNode != NULL) {
	if (currNode == overlapNode) {
	    break;
	}
	prevNode = currNode;
	currNode = currNode->nextByLbn;
    }

    /*
     * Upon exit from the preceeding loop, either currNode is NULL,
     * or currNode == overlapNode.
     *
     * If currNode is NULL, we have an internal error!
     *
     * Otherwise, remove the node from the list by setting the previous node's
     * nextByMcell pointer to the current node's nextByMcell pointer.  Then clear
     * the current node's nextByMcell pointer.
     */

    assert(currNode != NULL);

    /* 
     * Remove the current node from the list.
     */
    if (prevNode == NULL) {
	/* 
	 * The current node was the head of the list. 
	 */
	*lbnListHead = currNode->nextByLbn;
    } else {
	prevNode->nextByLbn = currNode->nextByLbn;
    }
    currNode->nextByLbn = NULL;

    return SUCCESS;
} /* End remove_overlap_node_from_lists */


/*
 * Function Name: remove_reserved_file_overlaps (3.xx)
 *
 * Description: This routine calls look_for_overlapped_clust_owner
 *              for every extent of the important subsystems.
 *              (As defined by load_subsystem_extents.)
 *
 * Input parameters:
 *	domain: 	 The domain being checked.
 *      volNum:		 The number of the volume containing the pages.
 *      overlapListHead: The overlap list to examine/modify.
 * 
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY
 *
 */
static int 
remove_reserved_file_overlaps(domainT	*domain,
			      volNumT	volNum,
			      overlapNodeT **overlapListHead)
{
    char 	*funcName = "remove_reserved_file_overlaps";
    extentIdT	extentAddr;
    xtntNumT	extent;
    lbnNumT	extentLbn;
    fobtoLBNT	*pExtents; /* The extents to check */
    xtntNumT 	numExtents;
    clustNumT 	numClustsInExtent;
    clustNumT	clustOffeset;
    int		status;
    int		statusFlag;
    volumeT	*volume;        /* Pointer to selected volume structure.*/

    volume  = &(domain->volumes[volNum]);


    /*
     * Set an invalid mcell ID.  No nodes in the overlap list
     * should have >this< mcell ID, so these pages will be removed
     * from all nodes referencing them.
     */

    extentAddr.cellAddr.volume	= -1;
    extentAddr.cellAddr.page	= -1;
    extentAddr.cellAddr.cell	= -1;

    /*
     * Clear overlaps of the RBMT extents.
     */

    pExtents	= volume->volRbmt->rbmtLBN;
    numExtents	= volume->volRbmt->rbmtLBNSize;

    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute the number of clusters in this extent.
	 */

	extentAddr.extentNumber = extent;
	extentLbn	  = pExtents[extent].lbn;
	clustOffeset	  = LBN2CLUST(extentLbn);
	numClustsInExtent = FOB2CLUST(pExtents[extent + 1].fob - 
				      pExtents[extent].fob);

	/*
	 * NOTE:
	 *	  The statusFlag variable is an in/out argument to
	 *	  look_for_overlapped_clust_owner.  On input, it is
	 *	  TRUE if this extent belongs to a reserved or a
	 *	  special file.  On output, it is set to TRUE if
	 *	  the extent was overlapped by some other file, but
	 *	  did not overlap any other extents.
	 *
	 *	  If the statusFlag returns as TRUE, the normal
	 *	  action would be to add the file to the list of
	 *	  files to be deleted.  Since the reserved files
	 *	  should never be deleted, this routine ignores
	 *	  the returned statusFlag.
	 */
	statusFlag = TRUE;
	status     = look_for_overlapped_clust_owner (domain,  
						      volNum,
						      clustOffeset,
						      numClustsInExtent,
						      &statusFlag,
						      overlapListHead,
						      &extentAddr);
	if (status != SUCCESS) {
	    if (status == CORRUPT) {
		/*
		 * This is part 2 of a message,
		 * so use SV_CONT.  The first
		 * part was output by look_for...
		 */
		writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
			  catgets(_m_catd, S_FSCK_1, FSCK_712,
				  "The RBMT for volume %s was overlapped.\n"),
			  volume->volName);
		corrupt_exit(NULL);
	    }
	    return status;
	}
    } /* End for (extent = 0; extent < numExtents; extent++) */


    /*
     * Clear overlaps of the SBM extents.
     */

    pExtents	= volume->volRbmt->sbmLBN;
    numExtents	= volume->volRbmt->sbmLBNSize;

    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute the number of pages in this extent.
	 */
	extentAddr.extentNumber = extent;
	extentLbn	  = pExtents[extent].lbn;
	clustOffeset	  = LBN2CLUST(extentLbn);
	numClustsInExtent = FOB2CLUST(pExtents[extent + 1].fob - 
				      pExtents[extent].fob);

	statusFlag = TRUE;
	status     = look_for_overlapped_clust_owner (domain,  volNum,
						     clustOffeset,
						     numClustsInExtent,
						     &statusFlag,
						     overlapListHead,
						     &extentAddr);
	if (status != SUCCESS) {
	    if (status == CORRUPT) {
		/*
		 * This is part 2 of a message,
		 * so use SV_CONT.  The first
		 * part was output by look_for...
		 */
		writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
			  catgets(_m_catd, S_FSCK_1, FSCK_713, 
				  "The SBM for volume %s was overlapped.\n"),
			  volume->volName);
		corrupt_exit(NULL);
	    }
	    return status;
	}
    } /* End for (extent = 0; extent < numExtents; extent++) */


    /*
     * Clear overlaps of the root tag extents.
     */

    if (volNum == domain->rootTagVol) {

	pExtents	= domain->rootTagLBN;
	numExtents	= domain->rootTagLBNSize;

	for (extent = 0; extent < numExtents; extent++) {
	    /*
	     * Compute the number of pages in this extent.
	     */

	    extentAddr.extentNumber = extent;
	    extentLbn = pExtents[extent].lbn;
	    clustOffeset = LBN2CLUST(extentLbn);
	    numClustsInExtent = FOB2CLUST(pExtents[extent + 1].fob - 
				      pExtents[extent].fob);

	    statusFlag = TRUE;
	    status     = look_for_overlapped_clust_owner (domain,  volNum,
							 clustOffeset,
							 numClustsInExtent,
							 &statusFlag,
							 overlapListHead,
							 &extentAddr);
	    if (status != SUCCESS) {
		if (status == CORRUPT) {
		    /*
		     * This is part 2 of a message,
		     * so use SV_CONT.  The first
		     * part was output by look_for...
		     */
		    writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
			      catgets(_m_catd, S_FSCK_1, FSCK_714,
				      "The root tag file for volume %s was overlapped.\n"),
			      volume->volName);
		    corrupt_exit(NULL);
		}
		return status;
	    }
	} /* End for (extent = 0; extent < numExtents; extent++) */
    } /* End if (volNum == domain->rootTagVol) */


    /*
     * Clear overlaps of the transaction log extents.
     */

    pExtents	= volume->volRbmt->logLBN;
    numExtents	= volume->volRbmt->logLBNSize;

    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute the number of pages in this extent.
	 */
	extentAddr.extentNumber = extent;
	extentLbn = pExtents[extent].lbn;
	clustOffeset = LBN2CLUST(extentLbn);
	numClustsInExtent = FOB2CLUST(pExtents[extent + 1].fob - 
				      pExtents[extent].fob);

	statusFlag = TRUE;
	status     = look_for_overlapped_clust_owner(domain, volNum,
						     clustOffeset,
						     numClustsInExtent,
						     &statusFlag,
						     overlapListHead,
						     &extentAddr);
	if (status != SUCCESS) {
	    if (status == CORRUPT) {
		/*
		 * This is part 2 of a message,
		 * so use SV_CONT.  The first
		 * part was output by look_for...
		 */
		writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
			  catgets(_m_catd, S_FSCK_1, FSCK_715, 
				  "The transaction log file for volume %s was overlapped.\n"),
			  volume->volName);
		corrupt_exit(NULL);
	    }
	    return status;
	}
    } /* End for (extent = 0; extent < numExtents; extent++) */


    /*
     * Clear overlaps of the BMT extents.
     */

    pExtents	= volume->volRbmt->bmtLBN;
    numExtents	= volume->volRbmt->bmtLBNSize;

    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute the number of pages in this extent.
	 */
	extentAddr.extentNumber = extent;
	extentLbn = pExtents[extent].lbn;
	clustOffeset = LBN2CLUST(extentLbn);
	numClustsInExtent = FOB2CLUST(pExtents[extent + 1].fob - 
				      pExtents[extent].fob);

	statusFlag = TRUE;
	status     = look_for_overlapped_clust_owner (domain,  volNum,
						     clustOffeset,
						     numClustsInExtent,
						     &statusFlag,
						     overlapListHead,
						     &extentAddr);
	if (status != SUCCESS) {
	    if (status == CORRUPT) {
		/*
		 * This is part 2 of a message,
		 * so use SV_CONT.  The first
		 * part was output by look_for...
		 */
		writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
			  catgets(_m_catd, S_FSCK_1, FSCK_716, 
				  "The BMT for volume %s was overlapped.\n"),
			  volume->volName);
		corrupt_exit(NULL);
	    }
	    return status;
	}
    } /* End for (extent = 0; extent < numExtents; extent++) */


    /*
     * Clear overlaps of the MISC extents.
     */

    pExtents	= volume->volRbmt->miscLBN;
    numExtents	= volume->volRbmt->miscLBNSize;

    for (extent = 0; extent < numExtents; extent++) {
	/*
	 * Compute the number of pages in this extent.
	 */
	extentAddr.extentNumber = extent;
	extentLbn = pExtents[extent].lbn;
	clustOffeset = LBN2CLUST(extentLbn);
	numClustsInExtent = FOB2CLUST(pExtents[extent + 1].fob - 
				      pExtents[extent].fob);

	statusFlag = TRUE;
	status     = look_for_overlapped_clust_owner(domain,  volNum,
						     clustOffeset,
						     numClustsInExtent,
						     &statusFlag,
						     overlapListHead,
						     &extentAddr);
	if (status != SUCCESS) {
	    if (status == CORRUPT) {
		/*
		 * This is part 2 of a message,
		 * so use SV_CONT.  The first
		 * part was output by look_for...
		 */
		writemsg (SV_ERR | SV_LOG_ERR | SV_CONT,
			  catgets(_m_catd, S_FSCK_1, FSCK_717, 
				  "The miscellaneous bitfile for volume %s was overlapped.\n"),
			  volume->volName);
		corrupt_exit(NULL);
	    }
	    return status;
	}
    } /* End for (extent = 0; extent < numExtents; extent++) */

    return SUCCESS;
} /* End remove_reserved_file_overlaps */


/*
 * Function Name: update_sbm_mcell_tag_and_set_info (3.xx)
 *
 * Description: This routine updates in-memory SBM data structures
 *		to reflect a change of "ownership" for a given
 *		mcell.  This occurs when the tag and/or set numbers
 *		associated with a given mcell are changed.
 *
 * Input parameters:
 *      domain:		 The domain being worked on.
 *	currMcell:	 The address of the mcell that was updated.
 *      oldTag:		 The old tag number associated with this mcell.
 *      oldSetTag:	 The old set tag number associated with this mcell.
 *      newTag:		 The new tag number associated with this mcell.
 *      newSetTag:	 The new set tag number associated with this mcell.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS.
 * 
 */
int 
update_sbm_mcell_tag_and_set_info(domainT *domain,
				  mcidT   *currMcell,
				  tagNumT oldTag,
				  tagNumT oldSetTag,
				  tagNumT newTag,
				  tagNumT newSetTag)
{
    char *funcName = "update_sbm_mcell_tag_and_set_info";
    int  status;

    overlapNodeT **fileListHead;	/* Pointer to an overlap list. */
    overlapNodeT **lbnListHead; 	/* Pointer to an overlap list. */
    overlapNodeT *currNode;		/* Pointer to an overlap node. */
    overlapNodeT *foundNode;		/* Pointer to an overlap node. */

    volumeT *volume;        /* Pointer to selected volume structure.*/

    writemsg(SV_DEBUG | SV_LOG_INFO,
	     catgets(_m_catd, S_FSCK_1, FSCK_718, 
		     "Modifying SBM tag and set info for mcell (%d,%ld,%d) from (%ld,%ld) to (%ld,%ld).\n"),
	     currMcell->volume, currMcell->page, currMcell->cell,
	     oldTag, oldSetTag, newTag, newSetTag);

    volume  = &(domain->volumes[currMcell->volume]);

    fileListHead  = &volume->volSbmInfo->overlapListByFile;
    lbnListHead   = &volume->volSbmInfo->overlapListByLbn;

    /*
     * Process the overlap lists.
     *
     * For each overlap nodes that belongs to >this< mcell:
     *     1. Remove the node from the overlap lists.
     *     2. Update the tag and settag data for the node
     *     3. Re-insert the node back into the overlap lists.
     */


    while (TRUE) {

	/*
	 * Walk through the list until a node is found that matches all of
	 * the "exact match" information, or we've gotten past where
	 * such a node might be fonnd.  If we do find a match, check for
	 * a "fuzzy" match.
	 */

	currNode  = *fileListHead;
	foundNode = NULL;

	while (currNode != NULL) {
	    /*
	     * The mcell volume number should be identical for every node
	     * on the list to the volume we are working on, but the test
	     * is left in "just in case."
	     */
	    if (currNode->xtntId.cellAddr.volume > currMcell->volume) {
		break;
	    } else if (currNode->xtntId.cellAddr.volume < currMcell->volume) {
		currNode = currNode->nextByMcell;
		continue;
	    }

	    /*
	     * The mcell volume fields match.  Next try the mcell page numbers.
	     */
	    if (currNode->xtntId.cellAddr.page > currMcell->page) {
		break;
	    } else if (currNode->xtntId.cellAddr.page < currMcell->page) {
		currNode = currNode->nextByMcell;
		continue;
	    }

	    /*
	     * The Mcell page numbers match.  Next check the Mcell cell numbers.
	     */
	    if (currNode->xtntId.cellAddr.cell > currMcell->cell) {
		break;
	    } else if (currNode->xtntId.cellAddr.cell < currMcell->cell) {
		currNode = currNode->nextByMcell;
		continue;
	    }

	    /*
	     * The Mcell cell numbers match.  Next check the tag numbers.
	     */
	    if (currNode->tagnum > oldTag) {
		break;
	    } else if (currNode->tagnum < oldTag) {
		currNode = currNode->nextByMcell;
		continue;
	    }

	    /*
	     * The tag numbers match.  Next check the fileset number.
	     */
	    if (currNode->setnum > oldSetTag) {
		break;
	    } else if (currNode->setnum < oldSetTag) {
		currNode = currNode->nextByMcell;
		continue;
	    }


	    /*
	     * The node here must be a match.  There is no need to check
	     * the ends of the overlap range, because they must be a proper
	     * subset of the extent.
	     */

	    /*
	     * All of the required data has been matched.  Return a pointer
	     * the current node.
	     */

	    foundNode = currNode;
	    break;
	} /* end while (currNode != NULL) */


	/*
	 * If no node found, then we must be done.
	 */
	if (foundNode == NULL) {
	    break; /* out of while (TRUE) */
	}

	status = remove_overlap_node_from_lists(foundNode,
						fileListHead,
						lbnListHead);
	if (NOT_FOUND == status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_708,
			     "Internal Error: Failed to remove node from overlap lists.\n"));
	    return FAILURE;
	} else if (status != SUCCESS) {
	    return status;
	}

	foundNode->tagnum = newTag;
	foundNode->setnum = newSetTag;

	status = insert_overlap_data_by_mcell (fileListHead, foundNode);
	if (status != SUCCESS) {
	    return status;
	}

	status = insert_overlap_data_by_lbn (lbnListHead, foundNode);
	if (status != SUCCESS) {
	    return status;
	}
    } /* end while (TRUE) */

    return SUCCESS;

} /* End update_sbm_mcell_tag_and_set_info */


/*
 * Function Name: validate_sbm_page (3.nn)
 *
 * Description: This is only checking the format of the data, and not
 *		the data itself.  This is used when we need to verify
 *		the page is of the correct format, but the tool is not
 *		ready to process the data. (I.e., the chicken and egg
 *		problem.)
 *
 * Input parameters:
 *     sbmPage: 	The page to have checks performed on.
 *     odsVersion:	ODS version number.
 *
 * Output parameters: N/A 
 *
 * Returns: SUCCESS, FAILURE
 * 
 */
int 
validate_sbm_page(pageT *sbmPage)
{
    char *funcName = "validate_sbm_page";
    bsStgBmT *pSbmPage; /* Pointer to a SBM page. */
    int      mapIndex;
    uint32_t *mapWords;
    uint32_t  xor32;

    /*
     * Establish pointers into the map area of the SBM page.
     * This skips the SBM page header.
     */

    pSbmPage = (bsStgBmT *) sbmPage->pageBuf;
    mapWords = (uint32_t *)  pSbmPage->mapInt;
    xor32    = 0;

    /* 
     * Simply compute the XOR value for "this" page, and compare that
     * with the XOR value stored for this page.
     *
     * If these don't match, then modify the page.  If they do match,
     * compare the contents of the page.
     */

    for (mapIndex = 0; mapIndex < SBM_LONGS_PG; mapIndex++) {
        xor32 ^= mapWords[mapIndex];
    }
	
    if (xor32 != pSbmPage->xor) {
        writemsg (SV_DEBUG | SV_LOG_FOUND,
		  catgets(_m_catd, S_FSCK_1, FSCK_720, 
			  "XOR value for SBM Page %ld is not correct.\n"),
		  (sbmPage->pageLBN / ADVFS_METADATA_PGSZ_IN_BLKS));
	return FAILURE;
    }

    return SUCCESS;

} /* End validate_sbm_page */

/* End fixfdmn_sbm.c */
