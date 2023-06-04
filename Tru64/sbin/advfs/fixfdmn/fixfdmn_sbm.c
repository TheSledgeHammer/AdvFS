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
#pragma ident "@(#)$RCSfile: fixfdmn_sbm.c,v $ $Revision: 1.1.37.4 $ (DEC) $Date: 2006/03/17 03:08:16 $"

#include "fixfdmn.h"

/*
 * Global
 */
extern nl_catd _m_catd;



/*
 * Function Name: add_tag_to_delete_list (3.xx) 
 *
 * Description: The purpose of this routine to add a tag to the list
 *		of files that are to be deleted.
 *
 *		Note that the tag number must be greater 5, and
 *		the set number must be greater than 0 in order
 *              for the file to be placed onto the delete list.
 *
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
int add_tag_to_delete_list (int		tag,
			    int		settag,
			    fileNodeT	**deleteFileListHead)
{
    char *funcName = "add_tag_to_delete_list";
    fileNodeT	*aFile;
    fileNodeT	*currNode;
    fileNodeT	*prevNode;
    int		status;

    assert (NULL != deleteFileListHead);

    /*
     * If the tag number is less than or equal to 5 (AdvFS
     * reserved and pseudo reserved files), or the set number
     * is less than 0 (tag files), then return immediately.
     * These files should never be placed onto the list of
     * files which are to be deleted.
     */

    if ((tag <= 5) || (settag <= 0)) {
	return SUCCESS;
    }


    aFile = (fileNodeT *)ffd_malloc (sizeof(fileNodeT));
    if (NULL == aFile) {
	writemsg (SV_ERR | SV_LOG_ERR,
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			  "Can't allocate memory for %s.\n"),
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_683, 
			  "file deletion node"));
	return NO_MEMORY;
    }

    aFile->tagnum = tag;
    aFile->setnum = settag;

    /*
     * Check to see if the list has any data in it yet.
     * If not, then this is the first node in the list.
     */
    if (NULL == *deleteFileListHead) {
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
     * or one of the fields of currNode > the same field of aFile.
     *
     * If currNode is NULL, then we simply add the node to the end
     * of the list.
     *
     * Otherwise insert the node between prevNode and currNode;
     */


    if (NULL == currNode) {
	/* 
	 * Link this node onto the end of the list. 
	 */
	prevNode->next = aFile;
	aFile->next    = NULL;
    } else {
	/* 
	 * Insert the node between prevNode and currNode.
	 */
	if (NULL == prevNode) {
	    /* 
	     * This becomes the head of the list. 
	     */
	    aFile->next         = *deleteFileListHead;
	    *deleteFileListHead = aFile;
	} else {
	    aFile->next    = prevNode->next;
	    prevNode->next = aFile;
	}
    } /* End if (NULL == currNode) */

    return SUCCESS;
} /* End add_tag_to_delete_list */


/*
 * Function Name: collect_sbm_info (3.70)
 *
 * Description: This routine sets bits in the in-memory SBM
 *		corresponding to the extents in existing files. If a
 *		page is already allocated it sets a bit indicating
 *		that there is an overlap for that page. Calling
 *		routine should check if extent runs off the end of the
 *		disk and fix the extent in the mcell.
 *
 * Input parameters:
 *      domain:		The domain being worked on.
 *      extentArray:    An array which contains extents from an mcell.
 *      arraySize:      The number of records in the extent array.
 *      tag:		The tag being worked on.
 *      settag:		The settag being worked on.
 *	xtntMcell:	The address of the mcell that claimed these pages.
 *	xtntNumOffset:	Number of extent record "missing" from the input 
 *			extent array.  This value is added to the array
 *			index when setting the extent number for the overlap
 *			nodes.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, NO_MEMORY, or FAILURE.
 *
 */
int collect_sbm_info (domainT    *domain,
		      pagetoLBNT *extentArray,
		      int        arraySize,
		      int        tag,
		      int        settag,
		      mcellT     *xtntMcell,
		      int	 xtntNumOffset)
{
    char *funcName = "collect_sbm_info";
    filesetT	*pFileset;
    int  bitNum;        /* For loop variable -- bit number w/in a word. */
    int  i;
    int  offset;	/* Bit-offset within word.  Used for building */
			/* the word masks for each end of the extent. */
    int  status;
    int  volNum;
    LBNT startLbn;
    overlapNodeT **listHead;  /* Pointer to an overlap list. */
    overlapNodeT *overlay;    /* Pointer to new overlap node. */
    pageNumberT bmIndex;             /* For loop index into the bitmaps. */
    pageNumberT currentPage;
    pageNumberT extentFirstVolPage;  /* First Vol Page Num of this extent. */
    pageNumberT extentLastVolPage;   /* Last  Vol Page Num of this extent. */
    pageNumberT extentPageCount;
    pageNumberT firstWordIndex;      /* SBM index for the start of extent. */
    pageNumberT lastWordIndex;       /* SBM index for the end of extent. */
    pageNumberT mapSize;
    pageNumberT overlapBeginVolPage; /* Vol Page # of first overlapped page. */
    pageNumberT overlapSize;
    short   overlapFound;     /* Boolean - TRUE if overlaps detected.  */
    storageMapT	*storageMap;	/* V5 "SBM" */
    uint64  firstWordMask;    /* 64-bit (machine word size) mask.  */
    uint64  lastWordMask;
    uint64  testMask;
    uint64  testWord;
    uint64  *sbm;             /* Pointer to SBM array.   */
    uint64  *overlapBitMap;   /* Pointer to overlapping bitmap array. */
    volumeT *volume;          /* Pointer to selected volume structure.*/

    /*
     * Loop through all the extents in the array.
     * NOTE: it takes two records to make an extent.
     */
    for (i = 0; i < (arraySize - 1); i++) {
	volNum   = extentArray[i].volume;
	startLbn = extentArray[i].lbn;
	extentPageCount = extentArray[i+1].page - extentArray[i].page;

	/*
	 * The following assumptions are made about the input arguments.
	 *   First,  the starting lbn must be on a page boundary.
	 *   Second, the volume must be open.
	 *   Third,  we must have an in-memory SBM present.
	 */

	if (((LBNT) -1 == startLbn) || ((LBNT) -2 == startLbn)) {
	    continue;
	}

	volume             = &(domain->volumes[volNum]);
	sbm                = volume->volSbmInfo->sbm;
	storageMap         = volume->volSbmInfo->storageMap;
	mapSize		   = volume->volSbmInfo->storageMapSize;
	overlapBitMap      = volume->volSbmInfo->overlapBitMap;
	overlapFound       = FALSE;
	extentFirstVolPage = startLbn / BLOCKS_PER_PAGE;
	extentLastVolPage  = extentFirstVolPage + extentPageCount - 1;

	/*
	 * Break Volume Page Numbers into SBM indicies and bit offsets.
	 *
	 * Because the word size is a power of 2, we can simply divide
	 * the Volume Page Number into two pieces.  The lower 6 bits
	 * become the bit offset within the bit maskword, and the
	 * upper bits are the word index.
	 *
	 * Note that these pieces are obtained by using division and
	 * remainder operators, rather than simple shifts and masks.
	 */

	firstWordIndex = extentFirstVolPage / BITS_PER_LONG;
	lastWordIndex  = extentLastVolPage  / BITS_PER_LONG;

	offset         = extentFirstVolPage % BITS_PER_LONG;

	/*
	 * Now construct bit map masks.
	 *
	 * The following picture may help describe the logic:
	 *
	 * Index Bit Map          Extent Bits
	 * ----- ---------------  ----------------
	 *   0   000000000000000  000000FF80000000 1
	 *   1   000000000000000
	 *   2   000000000000000  FFFFFFFFFFFFFFFF 2
	 *   3   000000000000000
	 *   4   078000010000300  FF00000000000000 3
	 *   5   00FFFF0002003C0  FFFFFFFFFFFFFFFF
	 *   6   000010000000FFF  000000000000FFFF
	 *   7   000000000000000
	 *
	 *
	 * In Example 1, the extent is entirely mapped in SBM word 0,
	 * so the last word index is the same as the first word index.
	 * This extent maps 9 pages.  The mask is contructed by setting 
	 * the low 9 bits of the word, and shifting them "offset" bits
	 * to the left.
	 *
	 * Example 2 is similar, but in this case the entire word is used
	 * to represent the extent, which is 64 pages long.
	 *
	 * In Example 3. the first word index is 4, the last word index is
	 * 6.  All middle words (in this case simply word 5) use all bits.
	 *
	 * If Example 3 were to start 8 pages later, then the bit offset
	 * would be 0 bits and the first word index would be 5.  Since we
	 * know that the extent is larger than 64 pages, we know that all
	 * bits within word 5 must be used by this extent.  In this case,
	 * we simply set all bits in that mask.
	 *
	 * Were Example 3 to end in word 5, we have the other end case.
	 * In this case, the bit offset for the last bit used would be
	 * 63, so again we set all bits in that mask.
	 */

	if (lastWordIndex == firstWordIndex) { 
	    /* 
	     * This extent is mapped by one word of the SBM. 
	     */
	    if (BITS_PER_LONG == extentPageCount) {
		/* 
		 * Whole word is used. 
		 */
		firstWordMask = ALL_BITS_SET;
	    } else {
		firstWordMask = ((1L << extentPageCount) - 1) << offset;
	    }
	} else {
	    if (0 == offset) {
		/*
		 * Whole word is used. 
		 */
		firstWordMask = ALL_BITS_SET;
	    } else {
		firstWordMask = ((1L << (BITS_PER_LONG - offset)) - 1) << offset;
	    }

	    offset = extentLastVolPage % BITS_PER_LONG;
	    if ((BITS_PER_LONG - 1) == offset) {
		/* 
		 * Whole word is used. 
		 */
		lastWordMask = ALL_BITS_SET;
	    } else {
		lastWordMask = (1L << (offset + 1)) - 1;
	    }
	} /* End if (lastWordIndex == firstWordIndex) */

	/*
	 * Now check for overlaps.  
	 */
	if (0 != (sbm[firstWordIndex] & firstWordMask)) {
	    /* 
	     * We have an overlap.  Set a flag. 
	     */
	    overlapFound = TRUE;
	    /* 
	     * Set the appropriate bits in the overlap bit map. 
	     */
	    overlapBitMap[firstWordIndex] = (overlapBitMap[firstWordIndex] | 
					    (sbm[firstWordIndex] & firstWordMask));
	    	
	}

	for (bmIndex = firstWordIndex + 1; 
	     bmIndex < lastWordIndex; 
	     bmIndex++) {
	    if (0 != sbm[bmIndex]) {
		overlapFound = TRUE;
		overlapBitMap[bmIndex] = (overlapBitMap[bmIndex] |
					 sbm[bmIndex]);
	    }
	}

	if (lastWordIndex != firstWordIndex) {
	    if (0 != (sbm[lastWordIndex] & lastWordMask)) {
		overlapFound = TRUE;
		overlapBitMap[lastWordIndex] =	(overlapBitMap[lastWordIndex] |
						(sbm[lastWordIndex] & lastWordMask));
	     }	
	}


	/*
	 * Currently fixfdmn collects extents for all of the
	 * AdvFS Reserved Files before beginning to collect
	 * extent data for "normal" files.
	 *
	 * Therefore, if an overlap is found while collecting an
	 * extent belonging to an AdvFS Reserved File, then the
	 * domain is unrecoverably CORRUPT, so just exit.
	 */

	if ((TRUE == overlapFound) && (tag < 0)) {
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_684, 
			      "Reserved file %d overlaps another reserved file.\n"),
		      tag);
	    corrupt_exit(NULL);
	}


	/*
	 * Now update the in-memory Storage Map and, if necessary,
	 * create nodes on the in-memory overlap list.
	 *
	 * If no overlaps were found, then simply update the in-memory
	 * Storage Map to indicate that these pages are "owned" by this
	 * tag.  
	 *
	 * If any overlaps were found, note their size and location.  
	 * In this case, only non-overlapped pages are marked as "owned"
	 * by this tag in the in-memory Storage Map.
	 */

	if (FALSE == overlapFound) {
	    /*
	     * Update the in-memory Storage Map for this volume.
	     */

	    /*
	     * This code is de-linked from Will Leslie's on-disk
	     * Storage Map format in that the high bit of the set
	     * tag is not used as a flag to indicate whether or not
	     * the page is in use.  Instead, this code uses the
	     * in-memory SBM to denote if a page is free or allocated.
	     *
	     * At this point, none of the pages within this extent
	     * can be allocated to another file, so simply mark all
	     * of the pages of this extent that are mapped by the
	     * in-memory storage map as being owned by this file.
	     */

            for (currentPage = extentFirstVolPage;
	         currentPage <= extentLastVolPage;
		 currentPage++) {
		 storageMap[currentPage].setTag  = settag;
		 storageMap[currentPage].fileTag = tag;
            }
	} /* End if (FALSE == overlapFound) */

	/*
	 * If any overlaps were found, note their size and location.  
	 *
	 * If at least one overlap was detected, then go through
	 * the bit map again in order to construct the overlap nodes. 
	 */
	if (TRUE == overlapFound) {
	    /*
	     * Each overlap node contains the following data:
	     *
	     *	the ID of the extent that describes this extent
	     *	tag of fileset
	     *	tag of file
	     *	the LBN of the start of the extent being collected
	     *	the size of the extent being collected, in pages
	     *	the Volume Page Number where the overlap begins
	     *	the size of the overlap, in pages
	     *
	     * Therefore, we have to locate overlapped pages, and
	     * determine how many consecutive pages are overlapped.
	     */

	    /*
	     * First try to find the name of this fileset.
	     */
	    pFileset = domain->filesets;

	    while (NULL != pFileset) {
		if (pFileset->filesetId.dirTag.num == settag) {
		    break;
		}
		pFileset = pFileset->next;
	    }

	    if (NULL == pFileset) {
		writemsg (SV_VERBOSE | SV_LOG_FOUND,
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_685,
				  "Mcell (%d,%d,%d) extent %d for tag %d of fileset %d overlaps %d pages of other data on volume %s starting at LBN %u.\n"),
			  xtntMcell->vol,
			  xtntMcell->page,
			  xtntMcell->cell,
			  i + xtntNumOffset,
			  tag,
			  settag,
			  extentPageCount,
			  volume->volName,
			  startLbn);
	    } else {
		writemsg (SV_VERBOSE | SV_LOG_FOUND,
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_686, 
				  "Mcell (%d,%d,%d) extent %d for tag %d of fileset %s overlaps %d pages of other data on volume %s starting at LBN %u.\n"),
			  xtntMcell->vol,
			  xtntMcell->page,
			  xtntMcell->cell,
			  i + xtntNumOffset,
			  tag,
			  pFileset->fsName,
			  extentPageCount,
			  volume->volName,
			  startLbn);
	    }

	    /* 
	     * Pre-initialize local variables for the while loop. 
	     */
	    bmIndex = firstWordIndex;
	    overlapBeginVolPage = (pageNumberT) -1;
	    overlapSize = 0;
	    testMask = firstWordMask;

	    /* 
	     * Process each word of the bitmap individually.
	     */
	    while (bmIndex <= lastWordIndex)  {
		if (0 == (sbm[bmIndex] & testMask)) { 
		    /* 
		     * No overlap detected in this word. 
		     *
		     * Update the Storage Map to indicate that
		     * these pages are owned by this file.
		     */

		    for (currentPage = BITS_PER_LONG * bmIndex;
			 currentPage < BITS_PER_LONG * (bmIndex + 1);
			 currentPage++) {
		        if ((currentPage >= extentFirstVolPage) &&
			    (currentPage <= extentLastVolPage)) {
		            storageMap[currentPage].setTag  = settag;
			    storageMap[currentPage].fileTag = tag;
			}
	            }


		    if (overlapSize > 0) { 
			/*
			 * We have detected the end of an overlap.
			 * Create a new overlap node, populate it,
			 * save it into both lists, and re-set.
			 */
			overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
			if (NULL == overlay) {
			    writemsg (SV_ERR | SV_LOG_ERR,
				      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
					      "Can't allocate memory for %s.\n"),
				      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_687, 
					      "overlap node"));
			    return NO_MEMORY;
			}

			overlay->xtntId.cellAddr.vol	= xtntMcell->vol;
			overlay->xtntId.cellAddr.page	= xtntMcell->page;
			overlay->xtntId.cellAddr.cell	= xtntMcell->cell;
			overlay->xtntId.extentNumber	= i + xtntNumOffset;
			overlay->tagnum			= tag;
			overlay->setnum         	= settag;
			overlay->extentStartLbn		= startLbn;
			overlay->extentPageCount	= extentPageCount;
			overlay->overlapStartPage	= overlapBeginVolPage;
			overlay->overlapPageCount	= overlapSize;
			overlay->nextByMcell	 	= NULL;
			overlay->nextByLbn	 	= NULL;

			listHead = &volume->volSbmInfo->overlapListByFile;
			status   = insert_overlap_data_by_mcell (listHead,
							         overlay);
			if (SUCCESS != status) {
			    return status;
			}

			listHead = &volume->volSbmInfo->overlapListByLbn;
			status   = insert_overlap_data_by_lbn (listHead,
							       overlay);
			if (SUCCESS != status) {
			    return status;
			}

			overlay = NULL;
			overlapBeginVolPage = (pageNumberT) -1;
			overlapSize  = 0;
		    } /* End if (overlapSize > 0) */
		} else {
		    /* 
		     * Overlapping pages found in this bitmap word... 
		     */
		    testWord	= (sbm[bmIndex] & testMask);
		    bitNum	= 0;
		    currentPage	= BITS_PER_LONG * bmIndex;


		    while (bitNum < BITS_PER_LONG) {
			/* 
			 * If we are not "in" an overlap, then look for the
			 * start of a new overlap.  If found, save the page
			 * number where the overlap begins.  (This might
			 * not be found if we've already passed all of the
			 * overlaps in this word.)
			 */
			if (0 == overlapSize) {
			    while (bitNum < BITS_PER_LONG) {
				if (1 == (testWord & 1)) {
				    break;
				}

				/*
				 * This page is not (yet) overlapped,
                                 * then mark this page as being owned
				 * by this file.
				 */
				if ((currentPage >= extentFirstVolPage) &&
			            (currentPage <= extentLastVolPage)) {
				    storageMap[currentPage].setTag  = settag;
				    storageMap[currentPage].fileTag = tag;
				}
				/*
				 * Position to test the next page.
				 */
				testWord = testWord >> 1;
				bitNum++;
				currentPage++;
			    } /* End while (bitNum < BITS_PER_LONG)  */

			    /*
			     * We get to this point because either:
			     *  1) an overlap was found, or
			     *  2) we've reached the end of the word.
			     * 
			     * If no further overlaps are found within
			     * this word, then bitnum == BITS_PER_LONG
			     * on exit from the previous while loop.
			     */
			
			    if (bitNum < BITS_PER_LONG) {
				/*
				 * The start of an overlap has been detected.
				 */
				overlapBeginVolPage = ((bmIndex * BITS_PER_LONG) +
						       bitNum);
				overlapSize = 1;
				testWord = testWord >> 1;
				bitNum++;
				currentPage++;
			    }
			} /* End if (0 == overlapSize) */

			/* 
			 * If we haven't reached the end of the word,
			 * then count the number of consecutively 
			 * overlapped pages.
			 */
			while (bitNum < BITS_PER_LONG) {
			    if (0 == (testWord & 1)) {
				break;
			    }
			    testWord = testWord >> 1;
			    bitNum++;
			    currentPage++;
			    overlapSize++;
			}

			/*
			 * We get to this point because either:
			 *  1) the end of the overlap was found, or
			 *  2) we've reached the end of the word,
			 *     in which case currentPage points to
			 *     the first page mapped by the next word.
			 *     and bitnum == BITS_PER_LONG.
			 */

			if (bitNum < BITS_PER_LONG) { 
			    /* 
			     * End of overlap detected.  Construct a node. 
			     */
			    if (overlapSize > 0) {  
				/* 
				 * Construct an overlap node, and re-set. 
				 */
				overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
				if (NULL == overlay) {
				    writemsg (SV_ERR | SV_LOG_ERR,
					      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
						      "Can't allocate memory for %s.\n"),
					      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_687, 
						      "overlap node"));
				    return NO_MEMORY;
				}

				overlay->xtntId.cellAddr.vol	= xtntMcell->vol;
				overlay->xtntId.cellAddr.page	= xtntMcell->page;
				overlay->xtntId.cellAddr.cell	= xtntMcell->cell;
				overlay->xtntId.extentNumber	= i + xtntNumOffset;
				overlay->tagnum			= tag;
				overlay->setnum			= settag;
				overlay->extentStartLbn 	= startLbn;
				overlay->extentPageCount	= extentPageCount;
				overlay->overlapStartPage	= overlapBeginVolPage;
				overlay->overlapPageCount	= overlapSize;
				overlay->nextByMcell		= NULL;
				overlay->nextByLbn		= NULL;

				listHead = &volume->volSbmInfo->overlapListByFile;
				status   = insert_overlap_data_by_mcell (listHead,
								         overlay);
				if (SUCCESS != status) {
				    return status;
				}

				listHead = &volume->volSbmInfo->overlapListByLbn;
				status   = insert_overlap_data_by_lbn (listHead,
								       overlay);
				if (SUCCESS != status) {
				    return status;
				}

				overlay = NULL;
			    } /* End if (overlapSize > 0) */

			    /*
			     * "This" page showed the end of the previous
			     * overlap, so therefore it is owned by this
			     * file.  Update the in-memory Storage Map to
			     * indicate that this file owns this page.
			     */
			    if ((currentPage >= extentFirstVolPage) &&
				(currentPage <= extentLastVolPage)) {
				storageMap[currentPage].setTag  = settag;
				storageMap[currentPage].fileTag = tag;
		            }

			    /* 
			     * Position to look for a new overlap. 
			     */
			    overlapBeginVolPage = (pageNumberT) -1;
			    overlapSize = 0;
			    testWord	= testWord >> 1;
			    bitNum++;
			    currentPage++;

			} /* End if (bitNum < BITS_PER_LONG) */
		    } /* End while (bitNum < BITS_PER_LONG) */
		} /* End if (0 == (sbm[bmIndex] & testMask))  else block */

		/* 
		 * Increment to the next word... 
		 */
		bmIndex++;

		/* 
		 * Create the new test mask and size. 
		 */
		if (bmIndex == lastWordIndex) {
		    testMask = lastWordMask;
		} else {
		    if (bmIndex == (firstWordIndex + 1)) {
			testMask = ALL_BITS_SET;
		    }
		}
	    } /* End while (bmIndex <= lastWordIndex) */

	    if (0 != overlapSize) { 
		/* 
		 * Construct an overlap node, and insert it into the list. 
		 */
		overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
		if (NULL == overlay) {
		    writemsg (SV_ERR | SV_LOG_ERR,
			      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
				      "Can't allocate memory for %s.\n"),
			      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_687,
				      "overlap node"));
		    return NO_MEMORY;
		}

		overlay->xtntId.cellAddr.vol	= xtntMcell->vol;
		overlay->xtntId.cellAddr.page	= xtntMcell->page;
		overlay->xtntId.cellAddr.cell	= xtntMcell->cell;
		overlay->xtntId.extentNumber	= i + xtntNumOffset;
		overlay->tagnum			= tag;
		overlay->setnum			= settag;
		overlay->extentStartLbn		= startLbn;
		overlay->extentPageCount	= extentPageCount;
		overlay->overlapStartPage	= overlapBeginVolPage;
		overlay->overlapPageCount	= overlapSize;
		overlay->nextByMcell		= NULL;
		overlay->nextByLbn		= NULL;

		listHead = &volume->volSbmInfo->overlapListByFile;
		status   = insert_overlap_data_by_mcell (listHead, overlay);
		if (SUCCESS != status) {
		    return status;
		}

		listHead = &volume->volSbmInfo->overlapListByLbn;
		status   = insert_overlap_data_by_lbn (listHead, overlay);
		if (SUCCESS != status) {
		    return status;
		}

		overlay = NULL;
	    } /* End if (0 != overlapSize) */
	} /* End if (TRUE == overlapFound) */

	/*
	 * Finally, ensure that all of these pages are marked as in-use by
	 * the SBM. 
	 */
	sbm[firstWordIndex] = sbm[firstWordIndex] | firstWordMask;

	for (bmIndex = firstWordIndex + 1;  
	     bmIndex < lastWordIndex;  
	     bmIndex++) {
	    sbm[bmIndex] = ALL_BITS_SET;
	}

	if (lastWordIndex != firstWordIndex) {
	    sbm[lastWordIndex] = sbm[lastWordIndex] | lastWordMask;
	}

    } /* End for (i = 0; i < (arraySize - 1); i++) */

    return SUCCESS;
} /* End collect_sbm_info */


/*
 * Function Name: correct_sbm (3.??)
 *
 * Description: This function will compare the on-disk SBM to the
 *		in-memory SBM.  If they are different then modify the
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
int correct_sbm (domainT *domain)
{
    char  *funcName = "correct_sbm";
    bsStgBmT  *pSbmPage; /* Pointer to a SBM page. */
#ifdef V5_SM
    bsStgMapT *pSmPage;  /* Pointer to a SM page.  */
#endif
    char  *mapBytes;
    char  *v3Map;
    int  bitNum;
    int  currVol;
    int  extent;
    int  fixedSbm; /* Flag indicating at least one SBM page was fixed. */
    int  modified;
    int  numExtents;
    int  status;
    int  volNum;
    int  tempSize;
    LBNT currLbn;
    LBNT currPageLBN;
    LBNT extentLbn;
    LBNT nextPageLBN;
    LBNT lbnNum;
    pageNumberT mapIndex;
    pageNumberT numPagesInExtent;
    pageNumberT sbmExtentPage;
    pageNumberT sbmIndex; /* Index into the SBM array. */
    pageNumberT sizeOfSbm;
    pageNumberT sizeOfSM;
    pageT       *pCurrentPage; /* The current page we are checking */
    pageT       *pPage;
    pagetoLBNT  *pExtents;     /* The extents to check */
#ifdef V5_SM
    storageMapT   *onDiskUsageMap;		/* V5 "SBM" */
#endif
    storageMapT   *storageMap;			/* local "SBM" */
    uint32T     *mapWords;
    uint32T     *xor32ptr;
    uint32T     xor32;
    uint64      bitMask;       /* 64-bit mask -- used to set bits. */
    uint64      sbmWord;
    uint64      xor64;
    uint64      *mapLongs;
    uint64      *sbm;          /* Pointer to SBM array.  */
    volumeT	*volume;       /* Pointer to selected volume structure.*/

    /*
     * If the SBM has not been loaded, do not correct
     * any on-disk SBMs.
     */

    if (!DMN_IS_SBM_LOADED(domain->status)) {
	return (SUCCESS);
    }

    /*
     * If working with AdvFS On Disk Structure version 3, then
     * allocate memory for the "larger" bitmap style.
     */
    if (3 == domain->dmnVersion) {
	v3Map = (char *)ffd_calloc (SBM_MAP_BYTES_PER_PAGE, sizeof(char));
	if (NULL == v3Map) {
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			      "Can't allocate memory for %s.\n"),
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_688,
			      "internal SBM"));
	    return NO_MEMORY;
	}
    }

    fixedSbm = FALSE;

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	volume  = &(domain->volumes[volNum]);
	if (-1 == volume->volFD) {
	    continue;
	}

	/*
	 * Read through each page of the on-disk SBM and
	 * compare the on-disk SBM with the in-memory SBM.
	 */

	bitNum		= 0; /* Used for ODS V3 domains. */
	pExtents	= volume->volRbmt->sbmLBN;
	numExtents	= volume->volRbmt->sbmLBNSize;
	sbm		= volume->volSbmInfo->sbm;
	sbmIndex	= 0;
	sbmWord		= sbm[sbmIndex];
	sizeOfSbm	= ((volume->volSize / BLOCKS_PER_PAGE) / BITS_PER_LONG) + 1;
	sizeOfSM	=  (volume->volSize / BLOCKS_PER_PAGE) + 1;
	storageMap	= volume->volSbmInfo->storageMap;
	xor32ptr	= (uint32T *)&xor64;

	for (extent = 0; extent < numExtents; extent++) {
	    /*
	     * Compute number of pages in this extent.
	     */
	    currVol = pExtents[extent].volume;
	    extentLbn = pExtents[extent].lbn;
	    numPagesInExtent = (pExtents[extent + 1].page -
				pExtents[extent].page);
 
	    /*
	     * For each extent loop through all pages.
	     */
	    for (sbmExtentPage = 0;
		 (sbmExtentPage < numPagesInExtent) && (sbmIndex < sizeOfSbm);
		 sbmExtentPage++) {
		currLbn = extentLbn + (sbmExtentPage * BLOCKS_PER_PAGE);

		status = read_page_from_cache(currVol, currLbn, &pCurrentPage);
		if (SUCCESS != status) {
		    return status;
		}
     
		modified	= FALSE;
#ifdef V5_SM
		pSmPage		= (bsStgMapT *)   pCurrentPage->pageBuf;
		onDiskUsageMap	= (storageMapT *) pSmPage->bfIndx;
#endif
		pSbmPage	= (bsStgBmT *)    pCurrentPage->pageBuf;
		mapBytes	= (char *)        pSbmPage->mapInt;
		mapLongs	= (uint64 *)      pSbmPage->mapInt;

		switch (domain->dmnVersion) {
		    case (3):
			/*
			 * AdvFS On Disk Structure version 3.
			 *
			 * Each page is represented by a byte, which is
			 * supposed to be either 0x00 or 0xFF.  Create a V3
			 * style in-memory SBM page, and then compare that
			 * with the on-disk page.  
			 */
			bzero(v3Map, SBM_MAP_BYTES_PER_PAGE);

			mapIndex = 0;
			/*
			 * First expand whole SBM words.
			 */
			while ((sbmIndex < sizeOfSbm) &&
				(SBM_MAP_BYTES_PER_PAGE >
				(mapIndex + BITS_PER_LONG - bitNum))) {
			    if (0 == bitNum) {
				sbmWord = sbm[sbmIndex];
			    }
			    while (bitNum < BITS_PER_LONG) {
				if (0 == (sbmWord & 1)) {
				    v3Map[mapIndex] = 0x00;
				} else {
				    v3Map[mapIndex] = 0xFF;
				}
				bitNum++;
				sbmWord = sbmWord >> 1;
				mapIndex++;
			    } /* End while (bitNum < BITS_PER_LONG) */
			    /*
			     * The current word has now been processed.
			     * Prepare for the next word.
			     */
			    sbmIndex++;
			    bitNum = 0;
			} /* End while ((sbmIndex < sizeOfSbm) && ... */

			/* 
			 * At this point we do not have enough room left in
			 * the v3Map to completely expand the current sbm
			 * word, but we still need to fill the v3Map.
			 * Unpack the new word until the map is full.  
			 */
			if (sbmIndex < sizeOfSbm) {
			    sbmWord = sbm[sbmIndex];
			    while (mapIndex < SBM_MAP_BYTES_PER_PAGE) {
				if (0 == (sbmWord & 1)) {
				    v3Map[mapIndex] = 0x00;
				} else {
				    v3Map[mapIndex] = 0xFF;
				}
				bitNum++;
				mapIndex++;
				sbmWord = sbmWord >> 1;
			    } /* End while (mapIndex < SBM_MAP_BYTES_PER_PAGE) */
			} /* End if (sbmIndex < sizeOfSbm) */


			/*
			 * The XOR in the on-disk SBM may or may not
			 * match what "should" be there.  Currently,
			 * we don't update the V3 page if the XOR
			 * value is wrong.  However, if the bitmap
			 * itself is wrong, then we'll put in the
			 * correct XOR value.
			 *
			 * Check to see if the data matches.
			 */
			tempSize = MIN (SBM_MAP_BYTES_PER_PAGE, mapIndex);

			if (0 != memcmp (&mapLongs[0], &v3Map[0], tempSize)) {
			    /* 
			     * The page needs to be fixed.  Determine which
			     * pages are incorrectly marked in the SBM.
			     */

			    writemsg (SV_VERBOSE | SV_LOG_FOUND,
				      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_689, 
					      "Mismatch in SBM data on volume %s, SBM page %d.\n"),
				      volume->volName, pExtents[extent].page + sbmExtentPage);

			    for (mapIndex = 0;
				 mapIndex < SBM_MAP_BYTES_PER_PAGE;
				 mapIndex++) {
				if (mapBytes [mapIndex] != v3Map[mapIndex]) {
				    /*
				     * Compute the volume page number and LBN 
				     * to which this map byte refers.
				     */
				    lbnNum = pExtents[extent].page + sbmExtentPage;
				    lbnNum = lbnNum * SBM_MAP_BYTES_PER_PAGE;
				    lbnNum = lbnNum + mapIndex;
				    /*
				     * Convert the volume page number into an LBN.
				     */
				    lbnNum = lbnNum * BLOCKS_PER_PAGE;
				    if (0 == mapBytes [mapIndex]) {
					writemsg (SV_DEBUG | SV_LOG_FOUND,
						catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_690, 
							"Page at LBN %u incorrectly marked free.\n"),
						lbnNum);
				    } else {
					writemsg (SV_DEBUG | SV_LOG_FOUND,
						catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_691, 
							"Page at LBN %u incorrectly marked used.\n"),
						lbnNum);
				    }
				}
			    }


			    /* 
			     * Now generate the XOR value for "this"
			     * page of the in-memory SBM.
			     */
			    xor64 = 0;
			    for (mapIndex = 0;
				 mapIndex < SBM_MAP_LONGS_PER_PAGE;
				 mapIndex++) {
				xor64 = xor64 ^ ((long *)v3Map)[mapIndex];
			    }

			    xor32            = xor32ptr[0] ^ xor32ptr[1];
			    modified         = TRUE;
			    pSbmPage->lgSqNm = 0;
			    pSbmPage->xor    = xor32;
			    memcpy (&mapLongs[0], &v3Map[0], 
				    SBM_MAP_BYTES_PER_PAGE);
			}

			break;
			    
		    case (4): 
			/* 
			 * AdvFS On Disk Structure version 4.
			 *
			 * Generate the XOR value for "this" page of the
			 * in-memory SBM, and compare that with the on-disk
			 * XOR value.
			 *
			 * If these don't match, then modify the page.  If
			 * they do match, compare the contents of the page.
			 */

			xor64 = 0;
			for (mapIndex = 0;
			     mapIndex < SBM_MAP_LONGS_PER_PAGE;
			     mapIndex++) {
			    if ((sbmIndex + mapIndex) < sizeOfSbm) {
				xor64 = xor64 ^ sbm[sbmIndex + mapIndex];
			    }
			}
			xor32 = xor32ptr[0] ^ xor32ptr[1];

			/* 
			 * Calculate the number of bytes used by the
			 * bitmap.
			 */
			tempSize = MIN (SBM_MAP_LONGS_PER_PAGE,
					(sizeOfSbm - sbmIndex));
			tempSize = tempSize * sizeof(uint64);

			if (xor32 != pSbmPage->xor) {
			    writemsg (SV_VERBOSE | SV_LOG_FOUND,
				      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_692, 
					      "Mismatch in SBM XOR on volume %s, SBM page %d.  Expected %08x, found %08x.\n"),
				      volume->volName, pExtents[extent].page + sbmExtentPage,
				      xor32, pSbmPage->xor);
			    modified = TRUE;
			} else {
			    /* 
			     * The disk XOR matches memory XOR.  Check to
			     * see if the data matches.  
			     */

			    if (0 != memcmp (&mapLongs[0], &sbm[sbmIndex],
					     tempSize)) {
				modified = TRUE;
				writemsg (SV_VERBOSE | SV_LOG_FOUND,
					  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_689, 
						  "Mismatch in SBM data on volume %s, SBM page %d.\n"),
					  volume->volName, pExtents[extent].page + sbmExtentPage);
			    }		
			} /* End if (xor32 != pSbmPage->xor) */

			if (TRUE == modified) {
			    /* 
			     * The page needs to be fixed.  Determine which
			     * pages are incorrectly marked in the SBM.
			     */
			    tempSize = tempSize / sizeof(uint64);
			    for (mapIndex = 0;
				 mapIndex < tempSize;
				 mapIndex++) {
				if (mapLongs [mapIndex] != sbm[sbmIndex + mapIndex]) {
				    /*
				     * There is at least one discrepancy between
				     * the on-disk SBM and the in-memory SBM.
				     * First compute the page number for the
				     * first bit in this word.
				     */
				    lbnNum = pExtents[extent].page + sbmExtentPage;
				    lbnNum = lbnNum * SBM_MAP_LONGS_PER_PAGE;
				    lbnNum = lbnNum + mapIndex;
				    lbnNum = lbnNum * BITS_PER_LONG;
				    /*
				     * Now check each bit of this word.
				     */
				    for (bitNum = 0, sbmWord = 1L;
					 bitNum < BITS_PER_LONG;
					 bitNum++, sbmWord << 1) {
					if ((mapLongs[mapIndex] & sbmWord) !=
					    (sbm[sbmIndex + mapIndex] & sbmWord)) {
					    if (0 == (mapLongs [mapIndex] & sbmWord)) {
						writemsg (SV_DEBUG | SV_LOG_FOUND,
							  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_690, 
								  "Page at LBN %u incorrectly marked free.\n"),
							  (lbnNum + bitNum) * BLOCKS_PER_PAGE);
					    } else {
						writemsg (SV_DEBUG | SV_LOG_FOUND,
							  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_691, 
								  "Page at LBN %u incorrectly marked used.\n"),
							  (lbnNum + bitNum) * BLOCKS_PER_PAGE);
					    } /* end if (0 == (mapLongs [mapIndex] & sbmWord)) */
					} /* end if ((mapLongs[mapIndex] & sbmWord)... */
				    } /* end for (bitNum = 0, sbmWord = 1L;... */
				} /* end if (mapLongs [mapIndex] != sbm[sbmIndex + mapIndex]) */
			    } /* end for (mapIndex = 0; ... */

			    tempSize = MIN (SBM_MAP_LONGS_PER_PAGE,
					    (sizeOfSbm - sbmIndex));
			    tempSize = tempSize * sizeof(uint64);

			    if (tempSize < SBM_MAP_BYTES_PER_PAGE) {
				bzero(mapLongs, SBM_MAP_BYTES_PER_PAGE);
			    }

			    pSbmPage->lgSqNm = 0;
			    pSbmPage->xor = xor32;
			    memcpy (&mapLongs[0], &sbm[sbmIndex], tempSize);
			} /* end if (TRUE == modified) */

			sbmIndex += SBM_MAP_LONGS_PER_PAGE;

			break;


#ifdef V5_SM
		    case (5):
			/*
			 * This message is here in case someone turns on
			 * V5_SM before the code here is tested. This needs
			 * to be removed in order to test the V5 code.
			 */
			writemsg (SV_ERR | SV_LOG_ERR,
				  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_693, 
					  "Internal Error: Unsupported version of the SBM.\n"));
			return FAILURE;
			break;
			
			/* 
			 * AdvFS On Disk Structure version 5.
			 *
			 * AdvFS ODS V5 replaced the SBM with a Storage
			 * Map.  Each page on the volume is represented by
			 * an 8-byte value that either declares the page
			 * to be free, or holds the tag and set tag of the
			 * file that "owns" this page.
			 *
			 * When a page is freed from a file, it is simply
			 * marked as being "available".  The tag and set
			 * tag values are not cleared.  Thus fixfdmn cannot
			 * use memcmp to compare the in-memory SM with that
			 * found on the disk.  Instead, each entry must be
			 * examined independently.
			 *
			 * There are issues with the XOR field as well.
			 * Because of the afore-mentioned issue, it makes
			 * no sense to compute the XOR of the in-memory SM
			 * up front.  Instead, each entry of the SM must
			 * be checked first.  If there are no errors, then
			 * an XOR of the on-disk data should be compared
			 * the the on-disk XOR.  If there are errors, then
			 * the in-memory SM must be updated with historical
			 * data from the on-disk SM.  (The tag/settag.)
			 * Once this is done, the XOR can be calculated
			 * and the page updated on disk.
			 */

			/*
			 * Clear the XOR long.
			 */
			xor64 = 0;

			/*
			 * Next check each entry in the Storage Map,
			 * and update it as necessary.  On a "real"
			 * difference, set the modified flag to TRUE.
			 */

			for (mapIndex = 0;
			     mapIndex < SUM_ENTRIES_PER_PAGE;
			     mapIndex++) {
			    tempSize = sbmIndex + mapIndex;
			    if (tempSize < sizeOfSM) {

				/*
				 * If the in-memory SM indicates that this page
				 * is free then:
				 *   If page overlaps are counted then:
				 *      If the set number associated with this
				 *      page is the "Counter Set Number" then:
				 *         1. Preserve the in-memory entry.
				 *         2. If the in-memory entry is not
				 *            the same as the on-disk entry then:
				 *               1. Set modified to TRUE.
				 *               2. If the on-disk entry is >also<
				 *                  an overlap count, then:
				 *                    1.  Output a message to
				 *                        the log indicating that
				 *                        overlaps were previously
				 *                        detected for this page,
				 *                        but a different number
				 *                        of overlaps was found.
				 *                  Else
				 *                     1. Output a message to the log
				 *                        indicating that this page was
				 *                        overlapped "n" times.
				 *                  Endif
				 *            Else
				 *               1. Output a message to the log
				 *                  indicating that previously
				 *                  detected overlaps remained
				 *                  on the disk.
				 *            Endif
				 *      Else this page was not overlaped, so:
				 *         1. Preserve the in-memory entry.
				 *         2. Compare the in-memory entry with
				 *            the on-disk entry.  If different,
				 *            then set modified to TRUE.
				 *      Endif
				 *   Else (page overlaps are NOT being counted)
				 *     is equal to the in-memory COUNTer key, then
				 *     set modified to TRUE,
				 *   else
				 *     check the on-disk data.  If it is also
				 *     marked free there, then copy the on-disk
				 *     entry into memory.  If it is NOT marked free,
				 *     set modified to TRUE.
				 *   fi
				 */
				if (SM_PAGE_IS_FREE (storageMap[tempSize].setTag)) {
				    if (TRUE == domain->countOverlaps) &&
				       (SM_COUNTER_SET_NUM (storageMap[tempSize].setTag)) {
					writemsg (SV_VERBOSE | SV_LOG_INFO,
						  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_ZZZ,
							  "Page %d was overlapped %d times.\n"),
						  tempSize,
						  storageMap[tempSize].fileTag);
				    }
				    if (SM_PAGE_IS_FREE (onDiskUsageMap[mapIndex].setTag)) {
					storageMap[tempSize].setTag =
					    onDiskUsageMap[mapIndex].setTag;
				    } else {
					modified = TRUE;
					if (0 != onDiskUsageMap[mapIndex].fileTag) {
					    storageMap[tempSize].setTag =
						SM_FREE_PAGE(onDiskUsageMap[mapIndex].setTag);
					}
				    } /* End if on-disk page is free. */
				    storageMap[tempSize].fileTag =
					onDiskUsageMap[mapIndex].fileTag;
				    if (-BFM_BFSDIR == settag) {
					storageMap[bmIndex].setTag = 0;
				    } else {
					storageMap[bmIndex].setTag = settag;
				    }
				    storageMap[bmIndex].fileTag  = tag;
				} else {
				    /*
				     * The in-memory SM indicates that this page
				     * is used.  Check against the on-disk data.
				     */
				} /* End if page is free. */

				xor64 = xor64 ^ sbm[sbmIndex + mapIndex];
			    } /* End if (tempSize < sizeOfSbm) */
			} /* End for (mapIndex = 0; mapIndex < SBM_MAP_LONGS_PER_PAGE; */

			xor32 = xor32ptr[0] ^ xor32ptr[1];

			/* 
			 * Calculate the number of bytes used by the
			 * bitmap.
			 */
			tempSize = MIN (SBM_MAP_LONGS_PER_PAGE,
					(sizeOfSbm - sbmIndex));
			tempSize = tempSize * sizeof(uint64);

			if (xor32 != pSbmPage->xor) {
			    writemsg (SV_VERBOSE | SV_LOG_FOUND,
				      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_692, 
					      "Mismatch in SBM XOR on volume %s, SBM page %d.  Expected %08x, found %08x.\n"),
				      volume->volName, pExtents[extent].page + sbmExtentPage,
				      xor32, pSbmPage->xor);
			    modified = TRUE;
			} else {
			    /* 
			     * The disk XOR matches memory XOR.  Check to
			     * see if the data matches.  
			     */

			    if (0 != memcmp (&mapLongs[0], &sbm[sbmIndex],
					     tempSize)) {
				modified = TRUE;
				writemsg (SV_VERBOSE | SV_LOG_FOUND,
					  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_689, 
						  "Mismatch in SBM data on volume %s, SBM page %d.\n"),
					  volume->volName, pExtents[extent].page + sbmExtentPage);
			    }		
			} /* End if (xor32 != pSbmPage->xor) */

			if (TRUE == modified) {
			    /* 
			     * The page needs to be fixed.  Determine which
			     * pages are incorrectly marked in the SBM.
			     */
			    tempSize = tempSize / sizeof(uint64);
			    for (mapIndex = 0;
				 mapIndex < tempSize;
				 mapIndex++) {
				if (mapLongs[mapIndex] != sbm[sbmIndex + mapIndex]) {
				    /*
				     * There is at least one discrepancy between
				     * the on-disk SBM and the in-memory SBM.
				     * First compute the page number for the
				     * first bit in this word.
				     */
				    lbnNum = pExtents[extent].page + sbmExtentPage;
				    lbnNum = lbnNum * SBM_MAP_LONGS_PER_PAGE;
				    lbnNum = lbnNum + mapIndex;
				    lbnNum = lbnNum * BITS_PER_LONG;
				    /*
				     * Now check each bit of this word.
				     */
				    for (bitNum = 0, sbmWord = 1L;
					 bitNum < BITS_PER_LONG;
					 bitNum++, sbmWord << 1) {
					if ((mapLongs[mapIndex] & sbmWord) !=
					    (sbm[sbmIndex + mapIndex] & sbmWord)) {
					    if (0 == (mapLongs [mapIndex] & sbmWord)) {
						writemsg (SV_DEBUG | SV_LOG_FOUND,
							  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_690, 
								  "Page at LBN %u incorrectly marked free.\n"),
							  (lbnNum + bitNum) * BLOCKS_PER_PAGE);
					    } else {
						writemsg (SV_DEBUG | SV_LOG_FOUND,
							  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_691, 
								  "Page at LBN %u incorrectly marked used.\n"),
							  (lbnNum + bitNum) * BLOCKS_PER_PAGE);
					    } /* end if (0 == (mapLongs [mapIndex] & sbmWord)) */
					} /* end if ((mapLongs[mapIndex] & sbmWord)... */
				    } /* end for (bitNum = 0, sbmWord = 1L;... */
				} /* end if (mapLongs [mapIndex] != sbm[sbmIndex + mapIndex]) */
			    } /* end for (mapIndex = 0; ... */

			    tempSize = MIN (SBM_MAP_LONGS_PER_PAGE,
					    (sizeOfSbm - sbmIndex));
			    tempSize = tempSize * sizeof(uint64);

			    if (tempSize < SBM_MAP_BYTES_PER_PAGE) {
				bzero(mapLongs, SBM_MAP_BYTES_PER_PAGE);
			    }

			    pSbmPage->lgSqNm = 0;
			    pSbmPage->xor    = xor32;
			    memcpy (&mapLongs[0], &sbm[sbmIndex], tempSize);
			} /* end if (TRUE == modified) */

			sbmIndex += SBM_MAP_LONGS_PER_PAGE;

			break;
#endif


		    default:
			/*
			 * Unsupported AdvFS Domain version.
			 */
			writemsg (SV_ERR | SV_LOG_ERR,
				  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_693, 
					  "Internal Error: Unsupported version of the SBM.\n"));
			return FAILURE;
			break;
		} /* End switch (domain->dmnVersion) */


		if (TRUE == modified) {
		    add_fixed_message(&(pCurrentPage->messages), SBM,
				      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_694, 
					      "SBM page %d fixed.\n"),
				      pExtents[extent].page + sbmExtentPage);
		    fixedSbm = TRUE;
		}

		status = release_page_to_cache(currVol, currLbn, pCurrentPage,
					       modified, FALSE);
		if (SUCCESS != status) {
		    return status;
		}

		modified = FALSE;

	    }/* End for (sbmExtentPage = 0; ... */
        } /* End for (extent = 0; extent < numExtents; extent++) */
    } /* End for (volNum = 1; volNum <= domain->highVolNum; volNum++) */

    if (3 == domain->dmnVersion) {
	free (v3Map);
    }
    
    if (TRUE == fixedSbm) {
	return FIXED;
    }

    return SUCCESS;
} /* End correct_sbm */


/*
 * Function Name: correct_sbm_overlaps (3.??)
 *
 * Description: This function corrects SBM overlaps by deleting all files
 *		that have some data overlap.
 *
 *		The current design of fixfdmn calls for the deletion
 *		of all files that "share" disk blocks, and that is the
 *		purpose of the correct_sbm_overlaps routine.  
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
 *		      Note:
 *		      The original design specification for fixfdmn 
 *		      used a simple bitmap to keep track of all of
 *		      the pages on a volume.  When an overlap was
 *		      found, however, data about the overlapping
 *		      extent is saved in a special structure.  The
 *		      only way to find the overlapped file, then,
 *		      was to read through the BMT to find the extent
 *		      that first claimed the page.
 *
 *		      With the introduction of the Storage Map
 *		      structure, fixfdmn can now consult the storage
 *		      map to determine which file "owns" an overlapped
 *		      page.
 *
 *		   3) delete all files that are on the to-be-deleted
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
 *		files (RBMT/BMT0, SBM, Root Tag, LOG, BMT, & Misc File)
 *		were overlapped.  Sections of overlaps that point to
 *		pages within a reserved file must be removed from the
 *		overlap list.
 *
 *		adding an extent number to the
 *		overlap node structure.  This makes it simple to detect
 *		overlapping extents defined within a single Mcell.
 *
 *		The routine then walks through the copy of the overlap
 *		list, checking each entry to see if those pages are
 *		present in the in-memory storage map.  For each mapped
 *		page, the routine adds the page owner to the list
 *		of files to be deleted, and updates the overlap node
 *		to remove the page from the node.  If all pages are
 *		found within the storage map, then the overlap node
 *		is freed.
 *
 *		Then while overlaps remain, the routine reads the BMT.  
 *		As an extent is found, it is checked to see if some part
 *		of it falls between the first and last overlapped pages
 *		of that volume.  If so, then handle the detailed overlap
 *		processing by calling look_for_overlapped_page_owner.
 *
 * Input parameters:
 *     domain: Pointer to the domain being checked.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, FIXED, NO_MEMORY.
 * 
 */
int correct_sbm_overlaps (domainT *domain)
{
    char  *funcName = "correct_sbm_overlaps";
    bsMCT	*pMcell;  /* The current BMT mcell we are working on */
    bsMPgT	*bmtPage; /* The current BMT page we are working on */
    bsMRT	*pRecord;
    bsShadowXtntT *pShadow;
    bsXtntRT	*pXtnt;
    bsXtraXtntRT *pXtra;
    extentIdT	extentAddr;
    fileNodeT	*currFile;
    fileNodeT	*deleteFileListHead;
    fileNodeT	*prevFile;
    filesetT	*pFileset;
    int  bmtExtentPage;
    LBNT currLbn;
    LBNT currPageLBN;
    int  currVol;
    int  extent;
    LBNT extentLbn;
    int  extentSize;
    int  ix;		/* for loop variable. */
    int  lastFilesetNo;
    int	 mcell;
    int  numExtents;
    int  numPagesInBMTExtent;
    int  overlapCount;		/* for loop variable. */
    int  status;
    int  statusFlag;
    int  volNum;
    int  tempSize;
    overlapNodeT *lookupNode;	/* Pointer to an overlap node. */
    overlapNodeT *overlay;	/* Pointer to new overlap node. */
    overlapNodeT *overlapListCopyByLbn;	/* Pointer to copy of overlap list. */
    overlapNodeT *overlapListPtr;	/* Pointer to copy of overlap list. */
    pageNumberT  firstOverlappedPage;
    pageNumberT  lastOverlappedPage;
    pageNumberT  mapSize;
    pageNumberT  overlapSize;
    pageNumberT  pageNumber;
    pageNumberT  startPage;
    pageT	 *pCurrentPage; /* The current page we are checking */
    pageT	 *pPage;
    pagetoLBNT	 *pExtents; /* The extents to check */
    storageMapT	 *storageMap;	/* V5 "SBM" */
    volumeT	 *volume; /* Pointer to selected volume structure.*/

    /*
     * If the SBM has not been loaded, do not correct
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
	if (-1 == volume->volFD) {
	    continue;
	}

	lookupNode = volume->volSbmInfo->overlapListByLbn;

	while (NULL != lookupNode) {
	    status = add_tag_to_delete_list (lookupNode->tagnum,
					     lookupNode->setnum,
					     &deleteFileListHead);
	    if (SUCCESS != status) {
		return status;
	    }
	    lookupNode = lookupNode->nextByLbn;
	} /* End while (NULL != lookupNode) */
    } /* End for (volNum = 1; volNum <= domain->highVolNum; volNum++) */

    /*
     * Process the overlap list for each volume separately.
     */
    extentAddr.cellAddr.vol  = 0;
    extentAddr.cellAddr.page = 0;
    extentAddr.cellAddr.cell = 0;

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	volume  = &(domain->volumes[volNum]);
	if (-1 == volume->volFD) {
	    continue;
	}

	storageMap	= volume->volSbmInfo->storageMap;
	mapSize		= volume->volSbmInfo->storageMapSize;

	lookupNode = volume->volSbmInfo->overlapListByLbn;

	if (NULL == lookupNode) {
	    /*
	     *  No overlaps detected on this volume.
	     */
	    writemsg (SV_DEBUG, 
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_695,
			      "No overlaps detected on volume %s.\n"),
		      domain->volumes[volNum].volName);
	    continue;
	}

	extentAddr.cellAddr.vol  = volNum;

	/*
	 * This volume has at least one overlap.
	 *
	 * Walk through the the overlap list for this volume, and
	 * create a copy of the overlap list for those overlapped
	 * pages that are not present in the in-memory storage map.
	 * For nodes placed onto the overlap list copy, note the
	 * "first" and "last" pages noted in the new list.
	 *
	 * Note that if the status of the in-memory storage map is
	 * SM_FULL, then no overlap nodes should be copied.  If not,
	 * however, there is a possibility that there will be nodes
	 * on the "copy" of the overlap list.  The owners of pages
	 * on the copy list will be found by reading through the
	 * BMT again.
	 *
	 * NOTE:  The last overlapped page will actually be the
	 *	  page number beyond the last overlapped page at
	 *	  the end of this loop.
	 */

	firstOverlappedPage  =	lookupNode->overlapStartPage;
	lastOverlappedPage   =	lookupNode->overlapStartPage +
				lookupNode->overlapPageCount;
	overlapCount	     =  0;
	overlapListCopyByLbn =	NULL;


	while (NULL != lookupNode) {
	    overlapCount++;	/* Increment count of "original" overlap nodes. */

	    /*
	     * Walk through the in-memory storage map and add each
	     * file to the list of files to be deleted.
	     */

	    for (overlapSize = lookupNode->overlapStartPage;
		 overlapSize < lookupNode->overlapStartPage + lookupNode->overlapPageCount;
	         overlapSize++) {
		status = add_tag_to_delete_list (storageMap[overlapSize].fileTag,
						 storageMap[overlapSize].setTag,
						 &deleteFileListHead);
		if (SUCCESS != status) {
		    return status;
		}
	    } /* End for (overlapSize = lookupNode->overlapStartPage; ... */


	    lookupNode = lookupNode->nextByLbn;
	} /* End while (NULL != lookupNode) */


	/*
	 * Decrement to the actual last page overlapped.
	 */
	lastOverlappedPage = lastOverlappedPage - 1;

	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_697, 
			 "Found %d overlaps on volume %s.\n"),
		 overlapCount, domain->volumes[volNum].volName);

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
    if (NULL == currFile) {
	/*
	 * No files to delete.
	 */
	return SUCCESS;
    }

    pFileset      = NULL;
    lastFilesetNo = -1;

    while (NULL != currFile) {
	if (currFile->setnum != lastFilesetNo) {
	    pFileset = domain->filesets;

	    while (NULL != pFileset) {
		if (pFileset->filesetId.dirTag.num == currFile->setnum) {
		    break;
		}
		pFileset = pFileset->next;
	    }
	} /* End if (currFile->setnum != lastFilesetNo) */


	/*
	 * The following case should only be true if the user
	 * specified a "-m" option.
	 */
	if (NULL == pFileset) {
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_700,
			      "Internal Error: Invalid fileset number (%d) in %s.\n"),
		      currFile->setnum, funcName);
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_701,
			      "Detected on-disk corruption that cannot be fixed.\n"));
	    corrupt_exit(NULL);
	} /* End if (NULL == pFileset) */


	lastFilesetNo = currFile->setnum;

	status = delete_tag (pFileset, currFile->tagnum);
	if (SUCCESS != status) {
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
 *		overlapStartPage, overlapSize, xtntId.cellAddr.vol, 
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
int find_overlap_node_by_lbn	(overlapNodeT *listHead, 
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
	if ((currNode->overlapStartPage + currNode->overlapPageCount - 1) <
		    (extentNode->extentStartLbn / BLOCKS_PER_PAGE)) {
	    currNode = currNode->nextByLbn;
	    continue;
	}

	/*
	 *  If the overlap described by the current node starts after the
	 *  end of the desired extent, then no nodes in the list overlap
	 *  the specified extent.
	 */
	if (currNode->overlapStartPage >
	   ((extentNode->extentStartLbn / BLOCKS_PER_PAGE) +
	     extentNode->extentPageCount - 1)) {
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
 *		xtntId.cellAddr.vol,  xtntId.cellAddr.page,
 *		xtntId.cellAddr.cell, xtntId.extentNumber,
 *		tagnum, setnum, extentStartLbn, extentSize,
 *		overlapStartPage, overlapSize
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
int find_overlap_node_by_mcell  (overlapNodeT *listHead, 
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
	if (currNode->xtntId.cellAddr.vol > extentNode->xtntId.cellAddr.vol) {
	    break;
	} else if (currNode->xtntId.cellAddr.vol < extentNode->xtntId.cellAddr.vol) {
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
	} else if (currNode->xtntId.extentNumber < extentNode->xtntId.extentNumber) {
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
	if ((currNode->extentStartLbn   / BLOCKS_PER_PAGE) >
	    ((extentNode->extentStartLbn / BLOCKS_PER_PAGE) +
	      extentNode->extentPageCount - 1)) {
	    break;
	}

	/*
	 * Next check that the current node's extent area doesn't end
	 * beyond the end of the desired area.
	 */
	if (((currNode->extentStartLbn   / BLOCKS_PER_PAGE) +
	      currNode->extentPageCount - 1) >
	    ((extentNode->extentStartLbn / BLOCKS_PER_PAGE) +
	      extentNode->extentPageCount - 1)) {
	    currNode = currNode->nextByMcell;
	    continue;
	}

	/*
	 * The node here must be a match.  There is no need to check
	 * the ends of the overlap range, because they must be a proper
	 * subset of the extent.
	 *
	 * All of the required data has been matched.  Return a pointer
	 * the current node.
	 */

	*foundNode = currNode;

	return SUCCESS;

    } /* End while (currNode != NULL) */

    return NOT_FOUND;

} /* End find_overlap_node_by_mcell */


/*
 * Function Name: init_sbm_structures (3.xx)
 *
 * Description: The purpose of this routine is to initialize
 *		the "SBM" structures for a particular volume.
 *
 * Input parameters:
 *	volume:		A pointer to the volume structure that
 *			needs its SBM structures initialized.
 * 
 * Returns: SUCCESS, or NO_MEMORY
 *
 */
int init_sbm_structures (volumeT	*volume)
{
    char *funcName = "init_sbm_structures";
    pageNumberT ix;		/* while loop variable */
    pageNumberT maxVolPage;	/* Maximum Page Number for the volume. */
    pageNumberT sizeOfSbm;
    rbmtInfoT	*volRbmt;
    sbmInfoT	*volSbmInfo;
    storageMapT	*storageMap = NULL;	/* V5 "SBM" */
    uint64	*sbm;		/* Pointer to SBM array. */
    uint64	*overlapBitMap; /* Pointer to overlapping SBM array. */

    if (NULL == volume->volSbmInfo) {
	volSbmInfo = (sbmInfoT *)ffd_malloc(sizeof(sbmInfoT));
	if (NULL == volSbmInfo) {
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			      "Can't allocate memory for %s.\n"),
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_763,
			      "volume sbmInfo structure"));
	    return NO_MEMORY;
	}
	volume->volSbmInfo = volSbmInfo;
    }

    volume->volSbmInfo->storageMapSize   = 0;

    maxVolPage = volume->volSize / BLOCKS_PER_PAGE;
    sizeOfSbm  = (maxVolPage / BITS_PER_WORD) + 1;

    sbm = (uint64 *)ffd_calloc(sizeOfSbm, sizeof(uint64));
    if (NULL == sbm) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_764,
			 "storage bit map"));
	return NO_MEMORY;
    }
    bzero(sbm,  sizeOfSbm * sizeof(uint64));

    overlapBitMap = (uint64 *)ffd_calloc(sizeOfSbm, sizeof(uint64));
    if (NULL == overlapBitMap) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_765, 
			 "overlap bit map"));
	return NO_MEMORY;
    }
    bzero(overlapBitMap, sizeOfSbm * sizeof(uint64));


    storageMap = (storageMapT *)ffd_calloc(maxVolPage, sizeof(storageMapT));
    if (NULL == storageMap) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_927, 
			 "storage map"));
        return NO_MEMORY;
    }

    bzero(storageMap, maxVolPage * sizeof(storageMapT));

    volume->volSbmInfo->sbm		  = sbm;
    volume->volSbmInfo->overlapBitMap	  = overlapBitMap;
    volume->volSbmInfo->overlapListByFile = NULL;
    volume->volSbmInfo->overlapListByLbn  = NULL;
    volume->volSbmInfo->storageMap	  = storageMap;
    volume->volSbmInfo->storageMapSize   = maxVolPage;

    return SUCCESS;
} /* End init_sbm_structures */


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
 *		overlapStartPage, overlapSize, xtntId.cellAddr.vol, 
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
int insert_overlap_data_by_lbn (overlapNodeT **listHead, overlapNodeT *node)
{
    char *funcName = "insert_overlap_data_by_lbn";
    overlapNodeT *currNode;
    overlapNodeT *prevNode;

    /*
     * Check to see if the list has any data in it yet.
     * If not, then this is a simple case.
     * Else find the proper place to insert this node into the list.
     */
    if (NULL == *listHead) {
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
	if (currNode->overlapStartPage > node->overlapStartPage) {
	    break;
	} else if (currNode->overlapStartPage < node->overlapStartPage) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}


	/*
	 * The overlaps begin on the same page.  Next try their sizes.
	 */
	if (currNode->overlapPageCount > node->overlapPageCount) {
	    break;
	} else if (currNode->overlapPageCount < node->overlapPageCount) {
	    prevNode = currNode;
	    currNode = currNode->nextByLbn;
	    continue;
	}


	/*
	 * The overlaps begin and end in the same place.  For every node on
	 * the list, the mcell volume number should be identical to the
	 * number of the volume being worked.  The test is left in "just in case."
	 */
	if (currNode->xtntId.cellAddr.vol > node->xtntId.cellAddr.vol) {
	    break;
	} else if (currNode->xtntId.cellAddr.vol < node->xtntId.cellAddr.vol) {
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

	if (currNode->extentPageCount > node->extentPageCount) {
	    break;
	} else if (currNode->extentPageCount < node->extentPageCount) {
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

    if (NULL == currNode) {
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
	if (NULL == prevNode) {
	    /* 
	     * This becomes the head of the list. 
	     */
	    node->nextByLbn = *listHead;
	    *listHead       = node;
	} else {
	    node->nextByLbn     = prevNode->nextByLbn;
	    prevNode->nextByLbn = node;
	}
    } /* End if (NULL == currNode) */
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
 *		xtntId.cellAddr.vol,  xtntId.cellAddr.page,
 *		xtntId.cellAddr.cell, xtntId.extentNumber,
 *		tagnum, setnum, extentStartLbn, extentSize,
 *		overlapStartPage, overlapSize
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
int insert_overlap_data_by_mcell (overlapNodeT **listHead, 
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
    if (NULL == *listHead) {
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
	if (currNode->xtntId.cellAddr.vol > node->xtntId.cellAddr.vol) {
	    break;
	} else if (currNode->xtntId.cellAddr.vol < node->xtntId.cellAddr.vol) {
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

	if (currNode->extentPageCount > node->extentPageCount) {
	    break;
	} else if (currNode->extentPageCount < node->extentPageCount) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}


	/*
	 * The extent data matches.  Next check the overlap starting
	 * page numbers.
	 */
	if (currNode->overlapStartPage > node->overlapStartPage) {
	    break;
	} else if (currNode->overlapStartPage < node->overlapStartPage) {
	    prevNode = currNode;
	    currNode = currNode->nextByMcell;
	    continue;
	}


	/*
	 * The overlaps begin on the same page.  Next try their sizes.
	 */
	if (currNode->overlapPageCount > node->overlapPageCount) {
	    break;
	} else if (currNode->overlapPageCount < node->overlapPageCount) {
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


    if (NULL == currNode) {
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
	if (NULL == prevNode) {
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
 * Function Name: is_page_free (3.x)
 *
 * Description: This routine checks the in-memory SBM to see if the
 *		page passed in is free or in use.
 *
 *		FALSE is returned either if the page is in use,
 *		or if any of the parameters is invalid.
 *
 * Input parameters:
 *      domain:  The domain being worked on.
 *      volNum:  The number of the volume containing the page.
 *      pageLBN: The logical block number of the Volume Page to check.
 *
 * Output parameters: N/A
 *
 * Returns: TRUE or FALSE.
 *
 */
int is_page_free (domainT *domain, int volNum, LBNT pageLBN)
{
    char *funcName = "is_page_free";
    int  volPage;	/* First Volume Page of this extent.  */
    int  offset;	/* Bit-offset within word.  Used for  */
			/*   building the page mask. */
    int  sbmIndex;	/* Index into the SBM for this page.  */
    int  status;
    uint64  pageMask;	/* 64-bit (machine word size) mask.  */
    uint64  *sbm;	/* Pointer to SBM array.   */
    volumeT  *volume;	/* Pointer to selected volume structure. */

    /*
     * Test for sanity in the input arguments.
     *   First,  the starting LBN must be on a page boundary.
     *   Second, the volume must be open.
     *   Third,  the page number must be valid for the volume.
     *   Fourth, we must have an in-memory SBM present.
     */
    volume  = &(domain->volumes[volNum]);

    assert(-1 != volume->volFD);
    assert(NULL != volume->volSbmInfo);


    if (0 != (pageLBN % BLOCKS_PER_PAGE)) {
	writemsg (SV_DEBUG, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_702, 
			  "Internal Error in %s: LBN argument (%u) is not on a page boundary.\n"),
		  funcName, pageLBN);
	return FALSE;
    }


    if (pageLBN > volume->volSize) {
	writemsg (SV_DEBUG,
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_703, 
			  "Internal Error: LBN (%u) is larger than volume size.\n"), pageLBN);
	return FALSE;
    }

    sbm = volume->volSbmInfo->sbm;

    /* 
     * Convert the page LBN to an SBM index and bit offset. 
     */
    volPage  = pageLBN / BLOCKS_PER_PAGE;
    sbmIndex = volPage / BITS_PER_LONG;
    offset   = volPage % BITS_PER_LONG;

    /* 
     * Construct a bit mask.   
     */
    pageMask = 1L << offset;

    /* 
     * Now test if this bit is set. 
     */

    if (0 != (sbm[sbmIndex] & pageMask)) {
	return FALSE;
    }

    /*
     * Well, it passes all of the tests. The page must be free.
     */
    return TRUE;
} /* End is_page_free */


/*
 * Function Name: load_sbm (3.xx)
 *
 * Description: Fill the in-memory SBM by reading the on-disk SBM
 *		off each volume.
 *
 *		This routine never modifies disk pages, it only
 *		reads them.
 *
 *		This routine will be called if the user specified
 *		that extent records within the BMT should not be
 *		checked.  In such a case, collect_sbm_info would
 *		never be called (either for files in the BMT or
 *		for the reserved files noted in the RBMT), and
 *		overlaps would never be detected.  However, cleanup
 *		of some things, such as the deferred delete list,
 *		would modify the SBM.  Thus, fixfdmn will still need
 *		to have an in-memory SBM.
 *
 *  FUTURE:
 *	If ODS V5, we should load the in-memory storage map as well as 
 *      the bit map.
 *
 * Input parameters:
 *     domain: The domain being checked.
 * 
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 *
 */
int load_sbm (domainT *domain)
{
    char  *funcName = "load_sbm";
    bsStgBmT *pSbmPage; /* Pointer to a SBM page. */
    char  *mapBytes;
    int  bitNum;
    LBNT  currLbn;
    LBNT  currPageLBN;
    int  currVol;
    int  extent;
    LBNT extentLbn;
    int  mapIndex;
    LBNT nextPageLBN;
    int  numExtents;
    int  numPagesInExtent;
    int  sbmExtentPage;
    int  sbmIndex;  /* Index into the SBM array. */
    int  sizeOfSbm;
    int  status;
    int  volNum;
    pageT      *pCurrentPage;  /* The current page we are checking */
    pageT      *pPage;
    pagetoLBNT *pExtents;      /* The extents to check */
    uint64     bitMask;        /* 64-bit mask -- used to set bits. */
    uint64     sbmWord;        /* 64-bit (machine word size) mask. */
    uint64     *mapLongs;
    uint64     *overlapBitMap; /* Pointer to overlapping SBM array. */
    uint64     *sbm;           /* Pointer to SBM array.  */
    volumeT    *volume;        /* Pointer to selected volume structure.*/

    for (volNum = 1; volNum <= domain->highVolNum; volNum++) {
	volume  = &(domain->volumes[volNum]);
	if (-1 == volume->volFD) {
	    continue;
	}

	/* 
	 * Read through each page of the on-disk SBM and convert the
	 * data to the in-memory SBM format.
	 *
	 * The only reason to have the overlap bit map here is to ensure
	 * that it is cleared.  (Since we aren't processing file
	 * extents, we cannot have discovered any overlapping extents.)
	 */
	sizeOfSbm = ((volume->volSize / BLOCKS_PER_PAGE) / BITS_PER_LONG) + 1;

	/* 
	 * Clear the SBM structures.  
	 */
	sbm = volume->volSbmInfo->sbm;
	overlapBitMap = volume->volSbmInfo->overlapBitMap;
	bzero(sbm,  sizeOfSbm * sizeof(uint64));
	bzero(overlapBitMap, sizeOfSbm * sizeof(uint64));

	/*
	 * Get the SBM extent data.
	 */
	pExtents = volume->volRbmt->sbmLBN;
	numExtents = volume->volRbmt->sbmLBNSize;

	/*
	 * Compute the number of pages in the first extent.
	 */
	extent = 0;
	currVol = pExtents[extent].volume;
	extentLbn = pExtents[extent].lbn;
	numPagesInExtent = (pExtents[extent + 1].page - pExtents[extent].page);

	/*
	 * Read in the first page of the first extent.
	 */
	sbmExtentPage  = 0;
	currLbn   = extentLbn;

	/*
	 * Initialize variables used within the loops below.
	 * Establish pointers into the map area of the SBM page.
	 * This skips the SBM page header.
	 */
	bitMask  = 1;
	bitNum   = 0;
	sbmWord  = 0;

	/*
	 * Now begin converting the data.
	 *
	 * Because volume sizes are not always an integral multiple of
	 *
	 * BITS_PER_LONG * BLOCKS_PER_PAGE
	 *
	 * we know there will probably be "unused" space at the end of
	 * the SBM file.  I'm told that the trailing blocks of the SBM
	 * are initially zeroed.
	 * 
	 * Also the size of the in-memory SBM was rounded up for similar
	 * reasons.
	 *
	 * Because of these issues, this loop terminates when the last
	 * word of the in-memory SBM has been filled.
	 */
	sbmIndex = 0;

	while (sbmIndex < sizeOfSbm) {
	    status = read_page_from_cache (currVol, currLbn, 
					   &pCurrentPage);
	    if (SUCCESS != status) {
		return status;
	    }
	    pSbmPage = (bsStgBmT *) pCurrentPage->pageBuf;
	    mapBytes = (char *) pSbmPage->mapInt;
	    mapLongs = (uint64 *) pSbmPage->mapInt;
	    mapIndex = 0;

	    switch (domain->dmnVersion) {
		case (3):
		    /*
		     * AdvFS On Disk Structure version 3.
		     *
		     * Each page is represented by a byte, which is
		     * supposed to be either 0x00 or 0xFF.  Check each
		     * byte -- assume a non-zero value means that the
		     * page is in use.
		     * 
		     * Note that the inner for loop does not initialize
		     * either bitNum or bitMask.  This is initialized on
		     * the outside of the loop.  On subsequent iterations
		     * of the loop, the SBM map word being built will
		     * simply continue to be filled.  
		     */

		    while ((sbmIndex < sizeOfSbm) &&
			   (SBM_MAP_BYTES_PER_PAGE >
			   (mapIndex + BITS_PER_LONG - bitNum))) {
			while (bitNum < BITS_PER_LONG) {
			    if (0 != mapBytes[mapIndex]) {
				sbmWord = sbmWord | bitMask;
			    }
			    bitNum++;
			    bitMask = bitMask << 1;
			    mapIndex++;
			} /* End while (bitNum < BITS_PER_LONG) */
			/* 
			 * The current word has now been filled.  Save it,
			 * and prepare to create the next word.  
		   	 */
			sbm[sbmIndex] = sbmWord;
			sbmIndex++;
			sbmWord = 0;
			bitNum  = 0;
			bitMask = 1;
		    } /* End while ((sbmIndex < sizeOfSbm) && ... */

		    /* 
		     * At this point, there are not have enough bytes in the
		     * current page to completely fill the next word of the
		     * SBM, yet we cannot ignore these trailing bytes.
		     * Begin filling the next word.
		     *
		     * If there are more pages to the SBM, it will be
		     * completed during the next iteration of the enclosing
		     * loop.
		     *
		     * If there are no more pages, then the partially filled
		     * word is complete, so it is saved. 
		     */

		    while ((sbmIndex < sizeOfSbm) &&
			   (mapIndex < SBM_MAP_BYTES_PER_PAGE)) {
			if (0 != mapBytes[mapIndex]) {
			    sbmWord = sbmWord | bitMask;
			}
			bitNum++;
			bitMask = bitMask << 1;
			mapIndex++;
		    }
		    break;

			    
		case (4): 
		    /* 
		     * AdvFS On Disk Structure version 4:
		     *
		     * Each page is represented in the bit map
		     * by a single bit -- which is either 0 or 1.
		     * Simply copy the data.
		     *
		     * There are no "partial words" to deal with here.
		     */

		    while ((sbmIndex < sizeOfSbm) &&
			   (mapIndex < SBM_MAP_LONGS_PER_PAGE)) {
			sbm[sbmIndex] = mapLongs[mapIndex];
			mapIndex++;
			sbmIndex++;
		    }
		    break;


		default:
		    /*
		     * Unsupported AdvFS Domain version.
		     */
		    writemsg (SV_ERR | SV_LOG_ERR,
			      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_693,
				      "Internal Error: Unsupported version of the SBM.\n"));
		    return FAILURE;
		    break;
	    } /* End switch (domain->dmnVersion) */



	    /*
	     * We have finished processing the current
	     * page of the SBM.  Release it now.
	     */
	    status = release_page_to_cache (currVol, currLbn, pCurrentPage,
					    FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }

	    /*
	     * Time to get the next page from the on-disk SBM --
	     * assuming, of course, that there >is< a next page.
	     */
	    sbmExtentPage++;

	    if (sbmExtentPage >= numPagesInExtent) {
		/*
		 * We have reached the end of the current extent.
		 * Check to see if there are more extents available.
		 *
		 * If there is another extent, reset sbmExtentPage
		 * so that the correct next page will be read.
		 *
		 * If there are no more extents, we are done loading
		 * the SBM.  If needed, save the last word into the
		 * in-memory SBM and break out of the enclosing loop.
		 */
		extent++;
		if (extent < numExtents) {
		    currVol          =  pExtents[extent].volume;
		    extentLbn        =  pExtents[extent].lbn;
		    numPagesInExtent = (pExtents[extent + 1].page -
					pExtents[extent].page);
		    sbmExtentPage = 0;
		} else {
		    /* 
		     * There are no more extents or pages in the SBM.
		     * If processing ODS V3, we may need to save the
		     * final word of the in-memory SBM.
		     *
		     * Because it is unlikely that the volume size will
		     * be evenly divisible by the page size, the SBM
		     * will probably not map the entire volume.  Those
		     * last few blocks will simply be unused.
		     *
		     * To prevent an infinite loop here, we force the
		     * loop to exit by setting the sbmIndex equal to
		     * the size of the SBM array.
		     */
		    if ((sbmIndex < sizeOfSbm) && (3 == domain->dmnVersion)) {
			sbm[sbmIndex] = sbmWord;
			sbmIndex = sizeOfSbm;
			sbmWord  = 0;
			bitNum   = 0;
			bitMask  = 1;
			/*
			 * Break out of the while (sbmIndex < sizeOfSbm) loop.
			 * This will cause the next volume to be processed.
			 */
			break;
		    }
		} /* End if (extent < numExtents) block */
	    } /* End if (sbmExtentPage >= numPagesInExtent) */

	    /*
	     * Because we could have switched to a new extent,
	     * the following code cannot be in an else block.
	     */
	    currLbn = extentLbn + (sbmExtentPage * BLOCKS_PER_PAGE);
	} /* End while (sbmIndex < sizeOfSbm) */
    } /* End for (volNum = 1; volNum <= domain->highVolNum; volNum++) */

    /*
     * Update the domain status to indicate that the SBMs
     * have been loaded.
     */    
    DMN_SET_SBM_LOADED(domain->status);

    return SUCCESS;
} /* End load_sbm */


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
 *      extentPageCount: The number of pages in the extent.
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
int remove_extent_from_sbm (domainT *domain,
			    int     volNum,
			    LBNT    startLbn,
			    int     extentPageCount,
			    int     tag,
			    int     settag,
			    mcellT  *xtntMcell,
			    int	    extentNumber)
{
    char *funcName = "remove_extent_from_sbm";
    int  bitNum;   /* For loop variable -- bit number w/in a word. */
    int  offset;   /* Bit-offset within word.  Used for building */
                   /* the word masks for each end of the extent. */
    int  status;
    int  tempOffset;		/* Bit-offset within word.  Used for */
				/* building the masks for the page range. */
    overlapNodeT **fileListHead;    /* Pointer to an overlap list. */
    overlapNodeT **lbnListHead;	    /* Pointer to an overlap list. */
    overlapNodeT *lookupNode;       /* Pointer to an overlap node. */
    overlapNodeT *overlay;          /* Pointer to new overlap node. */
    overlapNodeT *tempFileListHead; /* Pointer to an overlap list. */
    overlapNodeT *tempLbnListHead;  /* Pointer to an overlap list. */
    pageNumberT bmIndex;	    /* For loop index into the bitmaps. */
    pageNumberT currentPage;
    pageNumberT extentFirstVolPage; /* First Vol Page Number of this extent. */
    pageNumberT extentLastVolPage;  /* Last  Vol Page Number of this extent. */
    pageNumberT firstWordIndex;	    /* SBM index for the start of extent. */
    pageNumberT lastWordIndex;	    /* SBM index for the end of extent. */
    pageNumberT mapSize;
    pageNumberT overlapBeginVolPage;/* Vol Page # of first overlapped page. */
    pageNumberT overlapSize;
    pageNumberT rangeEndPage;	    /* Right "edge" of page range. */
    pageNumberT rangeFirstWordIndex;/* SBM index of the start of page range. */
    pageNumberT rangeLastWordIndex; /* SBM index of the end of page range. */
    pageNumberT rangePages;	    /* Cound of pages within the page range. */
    pageNumberT rangeStartPage;     /* Left  "edge" of page range. */
    short   overlapFound;	    /* Boolean - TRUE if overlaps detected. */
    storageMapT	*storageMap;	    /* V5 "SBM" */
    uint64  firstWordMask;	    /* 64-bit (machine word size) mask. */
    uint64  lastWordMask;
    uint64  rangeFirstWordMask;
    uint64  rangeLastWordMask;
    uint64  testMask;
    uint64  testWord;
    uint64  *sbm;		    /* Pointer to SBM array.   */
    uint64  *overlapBitMap;	    /* Pointer to overlapping bitmap array. */
    volumeT *volume;		    /* Pointer to selected volume structure.*/
    pagetoLBNT extentArray[2];

    /*
     * Test for sanity in the input arguments.
     *   the extent is not a hole
     *   the starting lbn must be on a page boundary
     */

    if (((LBNT) -1 == startLbn) || ((LBNT) -2 == startLbn)) {
	return SUCCESS;
    }

    volume  = &(domain->volumes[volNum]);

    assert(0 == (startLbn % BLOCKS_PER_PAGE));

    writemsg(SV_VERBOSE | SV_LOG_INFO,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_705, 
		     "Mcell (%d,%d,%d) extent %d for tag %d in fileset %d, is being removed from volume %s. (%d pages starting at LBN %u)\n"),
	     xtntMcell->vol, xtntMcell->page, xtntMcell->cell,
	     extentNumber,
	     tag, settag, volume->volName, extentPageCount, startLbn);

    sbm           = volume->volSbmInfo->sbm;
    overlapBitMap = volume->volSbmInfo->overlapBitMap;
    storageMap    = volume->volSbmInfo->storageMap;
    mapSize       = volume->volSbmInfo->storageMapSize;

    fileListHead  = &volume->volSbmInfo->overlapListByFile;
    lbnListHead   = &volume->volSbmInfo->overlapListByLbn;

    overlapFound  = FALSE;

    extentFirstVolPage = startLbn / BLOCKS_PER_PAGE;
    extentLastVolPage  = extentFirstVolPage + extentPageCount - 1;

    tempFileListHead = NULL;
    tempLbnListHead  = NULL;

    /*
     * Break Volume Page Numbers into SBM indicies and bit offsets.
     *
     * Because the word size is a power of 2, we can simply divide
     * the Volume Page Number into two pieces.  The lower 6 bits
     * become the bit offset within the bit maskword, and the
     * upper bits are the word index.
     *
     * Note that these pieces are obtained by using division and
     * remainder operators, rather than simple shifts and masks.
     */

    firstWordIndex = extentFirstVolPage / BITS_PER_LONG;
    lastWordIndex  = extentLastVolPage  / BITS_PER_LONG;

    offset         = extentFirstVolPage % BITS_PER_LONG;

    /*
     * Now construct bit map masks.
     *
     * The following picture may help describe the logic:
     *
     * Index Bit Map          Extent Bits
     * ----- ---------------  ----------------
     *   0   000000000000000  000000FF80000000 1
     *   1   000000000000000
     *   2   000000000000000  FFFFFFFFFFFFFFFF 2
     *   3   000000000000000
     *   4   078000010000300  FF00000000000000 3
     *   5   00FFFF0002003C0  FFFFFFFFFFFFFFFF
     *   6   000010000000FFF  000000000000FFFF
     *   7   000000000000000
     *
     *
     * In Example 1, the extent is entirely mapped in SBM word 0,
     * so the last word index is the same as the first word index.
     * This extent maps 9 pages.  The mask is contructed by setting 
     * the low 9 bits of the word, and shifting them "offset" bits
     * to the left.
     *
     * Example 2 is similar, but in this case the entire word is used
     * to represent the extent, which is 64 pages long.
     *
     * In Example 3. the first word index is 4, the last word index is
     * 6.  All middle words (in this case simply word 5) use all bits.
     *
     * If Example 3 were to either start in the fifth word, then the
     * bit offset would be 0 bits.  Since we know that the extent maps
     * more than one page, we know that all bits within word 5 must be
     * used by this extent.  In this case, we simply set all bits in
     * that mask.
     *
     * Were Example 3 to end in word 5, we have the other end case.
     * In this case, the bit offset for the last bit used would be
     * 63, so again we set all bits in that mask.
     */

    if (lastWordIndex == firstWordIndex) { 
	/* 
	 * This extent is mapped by one word of the SBM. 
	 */
	if (BITS_PER_LONG == extentPageCount) {
	    /* 
	     * Whole word is used. 
	     */
	    firstWordMask = ALL_BITS_SET;
	} else {
	    firstWordMask = ((1L << extentPageCount) - 1) << offset;
	}
    } else {
	if (0 == offset) {
	    /*
	     * Whole word is used. 
	     */
	    firstWordMask = ALL_BITS_SET;
	} else {
	    firstWordMask = ((1L << (BITS_PER_LONG - offset)) - 1) << offset;
	}

	offset = extentLastVolPage % BITS_PER_LONG;
	if ((BITS_PER_LONG - 1) == offset) {
	    /* 
	     * Whole word is used. 
	     */
	    lastWordMask = ALL_BITS_SET;
	} else {
	    lastWordMask = (1L << (offset + 1)) - 1;
	}
    } /* End if (lastWordIndex == firstWordIndex) */


    /*
     * Now check to see if an overlap was detected within this extent. 
     */
    if (0 != (overlapBitMap[firstWordIndex] & firstWordMask)) {
	/* 
	 * We have an overlap.  Set a flag. 
	 */
	overlapFound = TRUE;
    }


    for (bmIndex = firstWordIndex + 1;
	 bmIndex < lastWordIndex && (FALSE == overlapFound);
	 bmIndex++) {
	if (0 != overlapBitMap[bmIndex]) {
	    overlapFound = TRUE;
	}
    }

    if ((lastWordIndex != firstWordIndex) && (FALSE == overlapFound)) {
	if (0 != (overlapBitMap[lastWordIndex] & lastWordMask)) {
	    overlapFound = TRUE;
	}
    }

    /*  
     * If no overlaps were detected, simply clear the bits in the SBM
     * and return.  There is no need to touch the overlap bit map.  
     */

    if (FALSE == overlapFound) {
	sbm[firstWordIndex] = sbm[firstWordIndex] & ~firstWordMask;

	for (bmIndex = firstWordIndex + 1; bmIndex < lastWordIndex; bmIndex++) {
	    sbm[bmIndex] = 0;
	}

	if (lastWordIndex != firstWordIndex) {
	    sbm[lastWordIndex] = sbm[lastWordIndex] & ~lastWordMask;
	}

	/*
         * Update the storage map as is appropriate.
	 */
	for (currentPage = extentFirstVolPage;
             currentPage <= extentLastVolPage;
	     currentPage++) {
	    storageMap[currentPage].setTag  = 0;
	    storageMap[currentPage].fileTag = 0;
	}


	return SUCCESS;
    } /* End if (FALSE == overlapFound) */

    writemsg (SV_VERBOSE | SV_LOG_INFO,
	      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_706, 
		      "Mcell (%d,%d,%d) extent %d is overlapped on volume %s.\n"),
	      xtntMcell->vol, xtntMcell->page, xtntMcell->cell,
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
     * For each entry found do:
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
    if (NULL == lookupNode) {
	writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			  "Can't allocate memory for %s.\n"),
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_687, 
			  "overlap node"));
	return NO_MEMORY;
    }

    lookupNode->xtntId.cellAddr.vol	= xtntMcell->vol;
    lookupNode->xtntId.cellAddr.page	= xtntMcell->page;
    lookupNode->xtntId.cellAddr.cell	= xtntMcell->cell;
    lookupNode->xtntId.extentNumber	= extentNumber;
    lookupNode->tagnum			= tag;
    lookupNode->setnum			= settag;
    lookupNode->extentStartLbn  	= startLbn;
    lookupNode->extentPageCount 	= extentPageCount;
    lookupNode->overlapStartPage	= 0;
    lookupNode->overlapPageCount	= 0;
    lookupNode->nextByMcell		= NULL;
    lookupNode->nextByLbn		= NULL;

    overlay = NULL;

    /*
     * The remove_overlap_node_from_lists routine modifies the list,
     * which may result in it becoming empty.
     */

    while (NULL != *fileListHead) {
	status = find_overlap_node_by_mcell (*fileListHead, lookupNode, &overlay);
	if (NOT_FOUND == status) {
	    break;
	} else if (SUCCESS != status) {
	    return status;
	}

	writemsg (SV_VERBOSE | SV_LOG_INFO,
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_707, 
			  "Mcell (%d,%d,%d) extent %d for tag %d of fileset %d overlaps %d pages of other data starting at LBN %u.\n"),
		  overlay->xtntId.cellAddr.vol,
		  overlay->xtntId.cellAddr.page,
		  overlay->xtntId.cellAddr.cell,
		  overlay->xtntId.extentNumber,
		  overlay->tagnum,
		  overlay->setnum,
		  overlay->overlapPageCount,
		  (overlay->overlapStartPage * BLOCKS_PER_PAGE));


	status = remove_overlap_node_from_lists	(overlay,
						 fileListHead,
						 lbnListHead);
	if (NOT_FOUND == status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_708,
			     "Internal Error: Failed to remove node from overlap lists.\n"));
	    return FAILURE;
	} else if (SUCCESS != status) {
	    return status;
	}

	status = insert_overlap_data_by_mcell (&tempFileListHead, overlay);
	if (SUCCESS != status) {
	    return status;
	}
    }

    /*
     * NOTE:
     * The remove_overlap_node_from_lists routine modifies the list,
     * which may result in it becoming empty.
     */

    while (NULL != *lbnListHead) {
	status = find_overlap_node_by_lbn (*lbnListHead, lookupNode, &overlay);
	if (NOT_FOUND == status) {
	    break;
	} else if (SUCCESS != status) {
	    return status;
	}

	writemsg (SV_VERBOSE | SV_LOG_INFO,
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_709,
			  "Extent in mcell (%d,%d,%d) overlapped by mcell (%d,%d,%d) at LBN %u for %d pages.\n"),
		  xtntMcell->vol, xtntMcell->page, xtntMcell->cell,
		  overlay->xtntId.cellAddr.vol,
		  overlay->xtntId.cellAddr.page,
		  overlay->xtntId.cellAddr.cell,
		  (overlay->overlapStartPage * BLOCKS_PER_PAGE),
		  overlay->overlapPageCount);

	status = remove_overlap_node_from_lists (overlay,
						 fileListHead,
						 lbnListHead);
	if (NOT_FOUND == status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_708,
			     "Internal Error: Failed to remove node from overlap lists.\n"));
	    return FAILURE;
	} else if (SUCCESS != status) {
	    return status;
	}


	/*
	 * Insert the node into a list linked by Mcell ID, not LBN.
	 */
	status = insert_overlap_data_by_mcell (&tempLbnListHead, overlay);
	if (SUCCESS != status) {
	    return status;
	}
    }

    /*
     * Free the temporary lookup node as it is no longer needed.
     */
    free (lookupNode);


    /*
     * We have now removed all nodes from the overlap lists that
     * reference pages within the extent to be removed.  The
     * next step is to clear pages of the SBM that were "owned"
     * by the extent to be removed.
     */

    rangeStartPage = startLbn / BLOCKS_PER_PAGE;

    while (NULL != tempFileListHead) {
	overlay  = tempFileListHead;
	tempFileListHead = overlay->nextByMcell;

	rangeEndPage	= overlay->overlapStartPage - 1;
	rangePages	= rangeEndPage - rangeStartPage + 1;

	if (0 < rangePages) {
	    /*
	     * The pages between from rangeStartPage to rangeEndPage
	     * were first "owned" by this extent.  They must be marked
	     * free in both the SBM and the storage map.
	     */

	    writemsg (SV_DEBUG | SV_LOG_INFO,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_710,
			      "Marking %d pages free in the SBM for volume %s, starting at page %d.\n"),
		      rangePages, volume->volName, rangeStartPage);

	    /*
	     * Begin by constructing bit map masks to be used to free
	     * the pages that were first owned by this extent.  Break
	     * the first rangeStartPage and rangeEndPage (page numbers)
	     * into SBM indicies and bit offset.
	     */

	    rangeFirstWordIndex = rangeStartPage / BITS_PER_LONG;
	    rangeLastWordIndex  = rangeEndPage   / BITS_PER_LONG;

	    tempOffset          = rangeStartPage % BITS_PER_LONG;


	    if (rangeLastWordIndex == rangeFirstWordIndex) { 
		/* 
		 * This page range is mapped by one word of the SBM. 
		 */
		if (BITS_PER_LONG == rangePages) {
		    /* 
		     * Whole word is used. 
		     */
		    rangeFirstWordMask = ALL_BITS_SET;
		} else {
		    rangeFirstWordMask = ((1L << rangePages) - 1) << tempOffset;
		}
	    } else {
		if (0 == tempOffset) {
		    /*
		     * Whole word is used. 
		     */
		    rangeFirstWordMask = ALL_BITS_SET;
		} else {
		    rangeFirstWordMask = ((1L << (BITS_PER_LONG - tempOffset)) - 1) << tempOffset;
		}


		tempOffset = rangeEndPage % BITS_PER_LONG;

		if ((BITS_PER_LONG - 1) == tempOffset) {
		    /* 
		     * Whole word is used. 
		     */
		    rangeLastWordMask = ALL_BITS_SET;
		} else {
		    rangeLastWordMask = (1L << (tempOffset + 1)) - 1;
		}
	    } /* End if (rangeLastWordIndex == rangeFirstWordIndex) */

	    /*
	     * Now use the range masks to clear the bits in the SBM.
	     */

	    sbm[rangeFirstWordIndex] = sbm[rangeFirstWordIndex] & ~rangeFirstWordMask;

	    for (bmIndex = rangeFirstWordIndex + 1;
		 bmIndex < rangeLastWordIndex; 
		 bmIndex++) {
		sbm[bmIndex] = 0;
	    }

	    if (rangeLastWordIndex != rangeFirstWordIndex) {
		sbm[rangeLastWordIndex] = sbm[rangeLastWordIndex] & ~rangeLastWordMask;
	    }

	    /*
             * Update the storage map as is appropriate.
	     */
            for (currentPage = rangeStartPage;
		 currentPage < rangeEndPage + 1;
		 currentPage++) {
		if ((currentPage >= extentFirstVolPage) &&
	            (currentPage <= extentLastVolPage)) {
		    storageMap[currentPage].setTag  = 0;
		    storageMap[currentPage].fileTag = 0;
		}
            }
	} /* End if (0 < rangePages) */

	rangeStartPage = rangeEndPage + overlay->overlapPageCount + 1;
	free (overlay);
    } /* End while (NULL != tempFileListHead) */


    /*
     * Now process the final portion of the range.
     * These pages were not processed by the loop, as it only deals
     * with pages that are "in front" of the overlap node.
     */

    if (rangeStartPage <= extentLastVolPage) {
	rangeEndPage	= extentLastVolPage;
	rangePages	= rangeEndPage - rangeStartPage + 1;

	if (0 < rangePages) {
	    /*
	     * The pages between from rangeStartPage to rangeEndPage were first
	     * "owned" by this extent.  These also must be marked free in the
	     * SBM.
	     */

	    writemsg (SV_DEBUG | SV_LOG_INFO,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_710, 
			      "Marking %d pages free in the SBM for volume %s, starting at page %d.\n"),
		      rangePages, volume->volName, rangeStartPage);

	    /*
	     * Begin by constructing bit map masks to be used to free
	     * the pages that were first owned by this extent.  Break
	     * the first rangeStartPage and rangeEndPage (page numbers) into
	     * SBM indicies and bit offset.
	     */

	    rangeFirstWordIndex = rangeStartPage / BITS_PER_LONG;
	    rangeLastWordIndex  = rangeEndPage   / BITS_PER_LONG;

	    tempOffset          = rangeStartPage % BITS_PER_LONG;


	    if (rangeLastWordIndex == rangeFirstWordIndex) { 
		/* 
		 * This page range is mapped by one word of the SBM. 
		 */
		if (BITS_PER_LONG == rangePages) {
		    /* 
		     * Whole word is used. 
		     */
		    rangeFirstWordMask = ALL_BITS_SET;
		} else {
		    rangeFirstWordMask = ((1L << rangePages) - 1) << tempOffset;
		}
	    } else {
		if (0 == tempOffset) {
		    /*
		     * Whole word is used. 
		     */
		    rangeFirstWordMask = ALL_BITS_SET;
		} else {
		    rangeFirstWordMask = ((1L << (BITS_PER_LONG - tempOffset)) - 1) << tempOffset;
		}


		tempOffset = rangeEndPage % BITS_PER_LONG;

		if ((BITS_PER_LONG - 1) == tempOffset) {
		    /* 
		     * Whole word is used. 
		     */
		    rangeLastWordMask = ALL_BITS_SET;
		} else {
		    rangeLastWordMask = (1L << (tempOffset + 1)) - 1;
		}
	    } /* End if (rangeLastWordIndex == rangeFirstWordIndex) */

	    /*
	     * Now use the range masks to clear the bits in the SBM.
	     */

	    sbm[rangeFirstWordIndex] = sbm[rangeFirstWordIndex] & ~rangeFirstWordMask;

	    for (bmIndex = rangeFirstWordIndex + 1;
		 bmIndex < rangeLastWordIndex; 
		 bmIndex++) {
		sbm[bmIndex] = 0;
	    }

	    if (rangeLastWordIndex != rangeFirstWordIndex) {
		sbm[rangeLastWordIndex] = sbm[rangeLastWordIndex] & ~rangeLastWordMask;
	    }

	    /*
             * Update the storage map as is appropriate.
	     */
	    for (currentPage = rangeStartPage;
		 currentPage < rangeEndPage + 1;
		 currentPage++) {
		if ((currentPage >= extentFirstVolPage) &&
		    (currentPage <= extentLastVolPage)) {
	            storageMap[currentPage].setTag  = 0;
		    storageMap[currentPage].fileTag = 0;
		}
	    }
	} /* End if (0 < rangePages) */
    } /* End if (rangeStart < extentLastVolPage) */


    /*
     * Now clear the bits in the overlap bitmap that map this extent.
     * This removes all traces of this extent from the bitmaps.  This
     * must be done before the remaining overlap nodes are collected.
     */

    overlapBitMap[firstWordIndex] = overlapBitMap[firstWordIndex] & ~firstWordMask;

    for (bmIndex = firstWordIndex + 1; bmIndex < lastWordIndex; bmIndex++) {
	overlapBitMap[bmIndex] = 0;
    }

    if (lastWordIndex != firstWordIndex) {
	overlapBitMap[lastWordIndex] = overlapBitMap[lastWordIndex] & ~lastWordMask;
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

    while (NULL != tempLbnListHead) {
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

	if (lookupNode->overlapStartPage < extentFirstVolPage) {
	    overlapSize = extentFirstVolPage - lookupNode->overlapStartPage;

	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (NULL == overlay) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlay->xtntId.cellAddr.vol	= lookupNode->xtntId.cellAddr.vol;
	    overlay->xtntId.cellAddr.page	= lookupNode->xtntId.cellAddr.page;
	    overlay->xtntId.cellAddr.cell	= lookupNode->xtntId.cellAddr.cell;
	    overlay->xtntId.extentNumber	= lookupNode->xtntId.extentNumber;
	    overlay->tagnum			= lookupNode->tagnum;
	    overlay->setnum			= lookupNode->setnum;
	    overlay->overlapStartPage		= lookupNode->overlapStartPage;
	    overlay->overlapPageCount		= overlapSize;
	    overlay->extentStartLbn		= overlay->overlapStartPage *
						  BLOCKS_PER_PAGE;
	    overlay->extentPageCount		= overlay->overlapPageCount;
	    overlay->nextByMcell		= NULL;
	    overlay->nextByLbn			= NULL;


	    status = insert_overlap_data_by_mcell (fileListHead, overlay);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = insert_overlap_data_by_lbn (lbnListHead, overlay);
	    if (SUCCESS != status) {
		return status;
	    }

	    lookupNode->overlapStartPage = extentFirstVolPage;
	    lookupNode->overlapPageCount = lookupNode->overlapPageCount -
					   overlapSize;

	    overlay		= NULL;
	    overlapBeginVolPage = (pageNumberT) -1;
	    overlapSize		= 0;
	} /* End if (lookupNode->overlapStartPage < extentFirstVolPage) */


	/*
	 * Handle Case 3:
	 *	The lookupNode refers to an overlap that ends beyond
	 *	the end of the removed extent.
	 */

	if (extentLastVolPage <
	    (lookupNode->overlapStartPage + lookupNode->overlapPageCount - 1)) {

	    overlapSize = lookupNode->overlapStartPage +
			  lookupNode->overlapPageCount - 1 -
			  extentLastVolPage;

	    overlay = (overlapNodeT *)ffd_malloc (sizeof(overlapNodeT));
	    if (NULL == overlay) {
		writemsg (SV_ERR | SV_LOG_ERR,
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
				  "Can't allocate memory for %s.\n"),
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_687,
				  "overlap node"));
		return NO_MEMORY;
	    }

	    overlay->xtntId.cellAddr.vol	= lookupNode->xtntId.cellAddr.vol;
	    overlay->xtntId.cellAddr.page	= lookupNode->xtntId.cellAddr.page;
	    overlay->xtntId.cellAddr.cell	= lookupNode->xtntId.cellAddr.cell;
	    overlay->xtntId.extentNumber	= lookupNode->xtntId.extentNumber;
	    overlay->tagnum			= lookupNode->tagnum;
	    overlay->setnum			= lookupNode->setnum;
	    overlay->overlapStartPage		= extentLastVolPage + 1;
	    overlay->overlapPageCount		= overlapSize;
	    overlay->extentStartLbn		= overlay->overlapStartPage *
						  BLOCKS_PER_PAGE;
	    overlay->extentPageCount		= overlay->overlapPageCount;
	    overlay->nextByMcell		= NULL;
	    overlay->nextByLbn			= NULL;


	    status = insert_overlap_data_by_mcell (fileListHead, overlay);
	    if (SUCCESS != status) {
		return status;
	    }

	    status = insert_overlap_data_by_lbn (lbnListHead, overlay);
	    if (SUCCESS != status) {
		return status;
	    }

	    lookupNode->overlapPageCount = lookupNode->overlapPageCount -
					   overlapSize;

	    overlay		= NULL;
	    overlapBeginVolPage = (pageNumberT) -1;
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
	extentArray[0].lbn    = (lookupNode->overlapStartPage * BLOCKS_PER_PAGE);
	extentArray[0].page   = 0;
	extentArray[1].volume = volNum;
	extentArray[1].lbn    = (LBNT) -1;
	extentArray[1].page   = lookupNode->overlapPageCount;

	status = collect_sbm_info (domain, extentArray, 2,
				   lookupNode->tagnum,
				   lookupNode->setnum,
				   &lookupNode->xtntId.cellAddr,
				   lookupNode->xtntId.extentNumber);
	if (SUCCESS != status) {
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_711, 
			      "Failed to re-collect extent data for tag %d, set %d on volume %s.\n"),
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
int remove_overlap_node_from_lists (overlapNodeT *overlapNode,
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

    if (NULL == currNode) {
	return NOT_FOUND;
    }
    
    /* 
     * Remove the current node from the list.
     */
    if (NULL == prevNode) {
	/* 
	 * The current node was the head of the list. 
	 */
	*fileListHead = currNode->nextByMcell;
    } else {
	prevNode->nextByMcell = currNode->nextByMcell;
    }
    currNode->nextByMcell = NULL;

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

    assert(NULL != currNode);

    /* 
     * Remove the current node from the list.
     */
    if (NULL == prevNode) {
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
int update_sbm_mcell_tag_and_set_info ( domainT *domain,
					mcellT  *currMcell,
					int     oldTag,
					int     oldSetTag,
					int     newTag,
					int     newSetTag,
					pageT   *pPage)
{
    char *funcName = "update_sbm_mcell_tag_and_set_info";
    int  status;
    int  i;
    overlapNodeT **fileListHead;	/* Pointer to an overlap list. */
    overlapNodeT **lbnListHead; 	/* Pointer to an overlap list. */
    overlapNodeT *currNode;		/* Pointer to an overlap node. */
    overlapNodeT *foundNode;		/* Pointer to an overlap node. */
    pageNumberT mapIndex;
    pageNumberT mapSize;
    pageNumberT pageNum;
    pageNumberT page;
    storageMapT	*storageMap;	/* V5 "SBM" */
    volumeT *volume;        /* Pointer to selected volume structure.*/
    bsMCT *pMcell;
    bsMPgT *bmtPage;
    bsMRT *pRecord;
    bsXtntRT *pXtnt;
    bsXtraXtntRT *pXtra;
    bsShadowXtntT *pShadow;
    LBNT currLBN;
    int currVol;
    int extentPages;

    writemsg(SV_DEBUG | SV_LOG_INFO,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_718, 
		     "Modifying SBM tag and set info for mcell (%d,%d,%d) from (%d,%d) to (%d,%d).\n"),
	     currMcell->vol, currMcell->page, currMcell->cell,
	     oldTag, oldSetTag, newTag, newSetTag);

    volume  = &(domain->volumes[currMcell->vol]);

    storageMap    = volume->volSbmInfo->storageMap;
    mapSize       = volume->volSbmInfo->storageMapSize;

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
	 * such a node might be found.  If we do find a match, check for
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
	    if (currNode->xtntId.cellAddr.vol > currMcell->vol) {
		break;
	    } else if (currNode->xtntId.cellAddr.vol < currMcell->vol) {
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
	if (NULL == foundNode) {
	    break; /* out of while (TRUE) */
	}

	status = remove_overlap_node_from_lists	(foundNode,
						 fileListHead,
						 lbnListHead);
	if (NOT_FOUND == status) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_708,
			     "Internal Error: Failed to remove node from overlap lists.\n"));
	    return FAILURE;
	} else if (SUCCESS != status) {
	    return status;
	}

	foundNode->tagnum = newTag;
	foundNode->setnum = newSetTag;

	status = insert_overlap_data_by_mcell (fileListHead, foundNode);
	if (SUCCESS != status) {
	    return status;
	}

	status = insert_overlap_data_by_lbn (lbnListHead, foundNode);
	if (SUCCESS != status) {
	    return status;
	}
    } /* end while (TRUE) */

    /*
     * Get the extents for the mcell and change the storage map tag
     * and set tag for each page in the extents.
     */
    
    bmtPage = (bsMPgT *)pPage->pageBuf;
    pMcell = &(bmtPage->bsMCA[currMcell->cell]);
    pRecord = (bsMRT *)pMcell->bsMR0;
        
    while ((0 != pRecord->type) &&
           (0 != pRecord->bCnt) &&
           (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))){
           
        switch (pRecord->type) {
            case (BSR_XTNTS):
		if (3 == domain->dmnVersion) {
		    break; /* BSR_XTNTS not used in old ODS. */
		}
                pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
            
                /*
                 * Loop through the extents in this record and
                 * change the owner for them in the storageMap.
                 */
                for (i = 0; 
                     i < (pXtnt->firstXtnt.xCnt -1) &&
                     i < (BMT_XTNTS - 1);
                     i++) {
                    pageNum = pXtnt->firstXtnt.bsXA[i].vdBlk;
                    
                    if (-1 == pageNum || -2 == pageNum) {
                        continue;
                    }
                    pageNum = pageNum / BLOCKS_PER_PAGE;
                    extentPages = pXtnt->firstXtnt.bsXA[i+1].bsPage -
                                  pXtnt->firstXtnt.bsXA[i].bsPage;
                    
                    /*
                     * For each page, reset the tag and set tag.
                     */
                    for (page = pageNum;
                         page < pageNum + extentPages;
                         page++) {
                        
                        /*
                         * Don't know if it's necessary to check it
                         * the tag and set tag match the old ones, or
                         * what to do if they don't. This could be
                         * something to research.
                         */ 
                        storageMap[page].fileTag = newTag;
	                storageMap[page].setTag = newSetTag;
	    
                    }  /* end for each page in the extent */
                } /* end for each extent in the record */
		break;

            case (BSR_SHADOW_XTNTS):
                pShadow = (bsShadowXtntT *)((char *)pRecord + sizeof(bsMRT));
            
                /*
                 * Loop through the extents in this record and
                 * change the owner for them in the storageMap.
                 */
                for (i = 0; 
                     i < (pShadow->xCnt - 1) &&
                     i < (BMT_SHADOW_XTNTS - 1);
                     i++) {
                    pageNum = pShadow->bsXA[i].vdBlk;
                    
                    if (-1 == pageNum || -2 == pageNum) {
                        continue;
                    }
                    pageNum = pageNum / BLOCKS_PER_PAGE;
                    extentPages = pShadow->bsXA[i+1].bsPage -
                                  pShadow->bsXA[i].bsPage;
                    
                    /*
                     * For each page, reset the tag and set tag.
                     */
                    for (page = pageNum;
                         page < pageNum + extentPages;
                         page++) {
                        
                        /*
                         * Don't know if it's necessary to check it
                         * the tag and set tag match the old ones, or
                         * what to do if they don't. This could be
                         * something to research.
                         */ 
                        storageMap[page].fileTag = newTag;
	                storageMap[page].setTag = newSetTag;
	    
                    }  /* end for each page in the extent */
                } /* end for each extent in the record */
		break;

            case (BSR_XTRA_XTNTS):
                pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
            
                /*
                 * Loop through the extents in this record and
                 * change the owner for them in the storageMap.
                 */
                for (i = 0; 
                     i < (pXtra->xCnt -1) &&
                     i < (BMT_XTRA_XTNTS - 1);
                     i++) {
                    pageNum = pXtra->bsXA[i].vdBlk;
                    
                    if (-1 == pageNum || -2 == pageNum) {
                        continue;
                    }
                    pageNum = pageNum / BLOCKS_PER_PAGE;
                    extentPages = pXtra->bsXA[i+1].bsPage -
                                  pXtra->bsXA[i].bsPage;
                    
                    /*
                     * For each page, reset the tag and set tag.
                     */
                    for (page = pageNum;
                         page < pageNum + extentPages;
                         page++) {
                        
                        /*
                         * Don't know if it's necessary to check it
                         * the tag and set tag match the old ones, or
                         * what to do if they don't. This could be
                         * something to research.
                         */ 
                        storageMap[page].fileTag = newTag;
	                storageMap[page].setTag = newSetTag;
	    
                    }  /* end for each page in the extent */
                } /* end for each extent in the record */
		break;

            default:
                /*
                 * Not a record with extents, just skip it.
                 */
                break; 
       	    
        } /* end switch on record type */
        
        /*
         * Get next record of mcell.
         */
        pRecord = (bsMRT *)(((char *)pRecord) + 
            roundup(pRecord->bCnt, sizeof(int)));
            
    } /* end while not at end of mcell */	    
    
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
int validate_sbm_page (pageT *sbmPage, int odsVersion, int pgNum)
{
    char *funcName = "validate_sbm_page";
    bsStgBmT *pSbmPage; /* Pointer to a SBM page. */
    char     *mapBytes;
    int      mapIndex;
    int      status;
    int      numErrors; /* Number of errors found */
    uint32T  *mapWords;
    uint32T  *xor32ptr;
    uint32T  xor32;
    uint64   xor64;     /* 64-bit (machine word size) mask. */
    uint64   *mapLongs;

    /*
     * For ODS V3 domains, the SBM is more of a byte map.  Each
     * page is represented by a byte in which the bits are either
     * all clear or all set.  There is an XOR value stored at
     * the beginning of each page, but apparently this was not
     * well maintained and could be incorrect.  With ODS V3 the
     * XOR value was never checked.
     *
     * As with the ODS V3 domains, ODS V4 domains have an XOR
     * value in the SBM page header.  In ODS V4, however, this
     * XOR value is actually used by the kernel to check the
     * validity of the page.
     *
     * Unlike ODS V3 the ODS V4 SBM is a true bit map.  As a
     * result there is no rational sanity check that can be made.
     *
     * Therefore, for each page of the SBM, this routine will
     * do the following:
     *
     * For ODS V3, each byte of the map will be checked
     * for valid values.  A byte may be either 0, or 0xFF.
     *
     * Calculate the XOR value that "should" be present.
     *
     * Compare the calculated XOR with the on-disk XOR.
     *
     * For ODS V3 they may not match.
     * For ODS V4 it must match.
     */

    /*
     * Establish pointers into the map area of the SBM page.
     * This skips the SBM page header.
     */
    pSbmPage = (bsStgBmT *) sbmPage->pageBuf;
    mapBytes = (char   *)   pSbmPage->mapInt;
    mapWords = (uint32T *)  pSbmPage->mapInt;
    mapLongs = (uint64 *)   pSbmPage->mapInt;

    mapIndex = 0;
    xor32    = 0;
    xor64    = 0;
    xor32ptr = (uint32T *) &xor64;

    switch (odsVersion) {
	case (3):
	    /*
	     * AdvFS On Disk Structure version 3.
	     *
	     * Each page is represented by a byte, which is
	     * supposed to be either 0x00 or 0xFF.  Compute
	     * the XOR value for "this" page, and also check
	     * each byte of the map for valid values.
	     */

	    numErrors = 0;

	    for (mapIndex = 0; mapIndex < SBM_LONGS_PG; mapIndex++) {
		xor32 = xor32 ^ mapWords[mapIndex];
	    }

	    for (mapIndex = 0; mapIndex < SBM_MAP_BYTES_PER_PAGE; mapIndex++) {
		if ((0 != mapBytes[mapIndex]) && (0xFF != mapBytes[mapIndex])) {
		    numErrors++;
		}
	    }	

            /*
             * Using 10 as the number of errors to fail on was just
             * a guess. This can be changed in the future if we have
             * more data justifying a different number.
             */
	    if (numErrors > 10) {
		writemsg (SV_DEBUG | SV_LOG_WARN,
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_719, 
				  "Found too many errors (%d) on an SBM page.\n"), 
			  numErrors);
		return FAILURE;
	    }	

	    break;
			    
	case (4): 
	    /* 
	     * AdvFS On Disk Structure version 4:
	     *
	     * Simply compute the XOR value for "this" page,
	     * and compare that with the XOR value stored
	     * for this page.
	     *
	     * If these don't match, then modify the page.  If
	     * they do match, compare the contents of the page.
	     */

	    for (mapIndex = 0; mapIndex < SBM_MAP_LONGS_PER_PAGE; mapIndex++) {
		xor64 = xor64 ^ mapLongs[mapIndex];
	    }
	    xor32 = xor32ptr[0] ^ xor32ptr[1];

	    if (xor32 != pSbmPage->xor) {
		writemsg (SV_DEBUG | SV_LOG_FOUND,
			  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_720, 
				  "XOR value for SBM Page %d is not correct.\n"),
			  pgNum);
		return FAILURE;
	    }

	    break;
        /*
         * Need to add a case for V5 ODS when if becomes available.
         */

	default:
	    /*
	     * Unsupported On Disk Structure version.
	     */
	    writemsg (SV_ERR | SV_LOG_ERR,
		      catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_693,
			      "Internal Error: Unsupported version of the SBM.\n"));
	    failed_exit();
    } /* End switch (odsVersion) */

    return SUCCESS;
} /* End validate_sbm_page */
/* End fixfdmn_sbm.c */
