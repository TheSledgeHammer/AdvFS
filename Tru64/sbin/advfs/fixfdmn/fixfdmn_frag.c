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
 *      Weds Jan 17 14:55:50 PST 2001
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fixfdmn_frag.c,v $ $Revision: 1.1.29.1 $ (DEC) $Date: 2006/03/17 03:04:55 $"

#include "fixfdmn.h"

/*
 * Global
 */
extern nl_catd _m_catd;


/*
 * Function Name: set_frag_file_arrays (3.69)
 *
 * Description: This function is used to allocate and set up the arrays
 *              needed to process the frag file.
 *
 * Input parameters:
 *     fileset: The fileset we are working on.
 *     domain: The domain we are working on.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY.
 * 
 */
int set_frag_file_arrays (filesetT *fileset, domainT *domain) 
{
    pagetoLBNT *fragXtnts;  /* Local pointer to fragLBN array */
    unsigned long (*slotArray)[3];
    pageT *pPage;
    bsMPgT *bmtPage;
    bsMCT *pMcell;
    bsMRT *pRecord;
    grpHdrT *pFragGrp;
    mcellT mcell;
    mcellT nextMcell;
    tagNodeT *pTagNode;
    tagNodeT tmpTagNode;
    nodeT *node;
    volumeT *volume;
    bfTagT checkTag;
    char *groupUse;
    int maxSlotNum;
    int lastPage;
    int groupPage;
    int slotArraySize;
    int slotNum;
    int modified;
    int currPage;
    int pageNum;
    int currVol;
    LBNT currLBN;
    int numSlots;
    int endNumSlots;
    int numGroups;
    int group;
    int fixed;
    int status;
    int firstFrag;
    int grpNumInFile;
    
    /* Initialize variables */
    
    grpNumInFile = 0;
    modified = FALSE;
    fixed = FALSE;

    /*
     * Get the first tag node from the tag list, it should be for tag 1
     * which is the frag file.
     */   
    status = find_first_node(fileset->tagList, (void **)&pTagNode, &node);
    
    if ((NOT_FOUND == status) || (pTagNode->tagSeq.num != 1))
    {
        /*
         * If the tag for the frag file doesn't exist, this is
         * a failure since we currently can't mount without one.
         *
         * FUTURE: should we create one here in the next version? We
         * could also mark the fileset as bad and try to process the rest.
         */
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_266, 
			  "No frag file found for fileset %s.\n"),
                  fileset->fsName);
        corrupt_exit(NULL);
    }
    else if (SUCCESS != status)
    {
        return status;
    }
    
    if (FS_IS_CLONE(fileset->status))
    {
        /*
         * If the fileset is a clone, see if the frag file mcell is the
         * same as that of the frag file of the original fileset. If so,
         * then don't bother with creating the arrays for the clone.
         */
        if (T_IS_CLONE_ORIG_PMCELL(pTagNode->status)) {
            return SUCCESS;
        }
    } /* end if fileset is a clone */
    /* 
     * Call collect_extents with the primary mcell for the frag file 
     * (tag 1) to load the frag file extents into fileset.fragLBN array.
     */
    
    status = collect_extents(domain, fileset, &(pTagNode->tagPmcell), FALSE,
                             &(fileset->fragLBN), &(fileset->fragLBNSize),
                             FALSE);
    if (SUCCESS != status)
    {
        return status;
    }
    
    if (0 == fileset->fragLBNSize)
    {
        /*
         * This may be OK in the future when filesets can be set to
         * not have any frags.
         */
        writemsg(SV_DEBUG | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_267, 
			"Frag file for fileset %s has no extents.\n"),
		 fileset->fsName);
        return SUCCESS;
    }
                              
    fragXtnts = fileset->fragLBN;
    
    /*
     * Check if the total number of pages is divisible by 16 only if this
     * is an original fileset.
     *
     * FUTURE: call a routine to correct the extents (probably truncate
     * the frag file at the last 16 page boundary or add zeroed out pages
     * to the end of the file).
     */
    if ((!FS_IS_CLONE(fileset->status)) &&
        (fragXtnts[fileset->fragLBNSize].page % BF_FRAG_GRP_PGS != 0))
    {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_268,
			 "Frag file not on 16 page boundary for fileset %s.\n"),
                 fileset->fsName);
        return FAILURE;
    }
    
    /*
     * Compute the size needed for the frag slot array and get the
     * storage for the array. We need to multiply the slots by 3 since
     * this is a two dimensional array with the second dimension of 3.
     */
    lastPage = fragXtnts[fileset->fragLBNSize].page;
    if (FS_IS_CLONE(fileset->status))
    {
        /*
         * If this is a clone, use the size of the original fileset
         * frag file if it is larger because we can get frags in the 
         * clone pointing into the original.
         */
        if (lastPage < fileset->origClonePtr->fragLBN[fileset->origClonePtr->fragLBNSize].page)
        {
            lastPage = fileset->origClonePtr->fragLBN[fileset->origClonePtr->fragLBNSize].page;
        }
    }
    maxSlotNum = lastPage*8;
    slotArraySize = ((maxSlotNum/BITS_PER_LONG)+1)*3;
    
    slotArray = (unsigned long (*)[])ffd_calloc(slotArraySize, sizeof(long));
    if (NULL == slotArray)
    {
        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_269, 
			 "Can't allocate memory for %s for fileset %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_270, 
			 "frag slot array"), fileset->fsName);
        return NO_MEMORY;
    }
    
    fileset->fragSlotArray = slotArray;
    slotNum = 0;
    grpNumInFile = 0;

    numGroups = lastPage/BF_FRAG_GRP_PGS;
    groupUse = (char *)ffd_calloc(numGroups, sizeof(char));

    if (NULL == groupUse)
    {
        writemsg(SV_ERR | SV_LOG_ERR,
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_272,
			 "group use array"));
        return NO_MEMORY;
    }

    fileset->groupUse = groupUse;
        
    /*
     * Initialize the group use array to something that isn't a 
     * valid frag size.
     */   
    for (group = 0; group < numGroups; group++)
    {
        groupUse[group] = -1;
    }

    /*
     * Walk through all the frag file pages which start a group and 
     * check the group header for valid data.
     */
    for (pageNum = 0; 
         pageNum < fragXtnts[fileset->fragLBNSize].page; 
         pageNum += BF_FRAG_GRP_PGS, 
             slotNum += BF_FRAG_GRP_SLOTS, grpNumInFile++)
    {
        status = convert_page_to_lbn(fragXtnts, fileset->fragLBNSize,
                                     pageNum, &currLBN, &currVol);
        if (status != SUCCESS)
        {
            writemsg(SV_ERR | SV_LOG_ERR, 
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_273,
			     "Can't convert frag page %d to LBN for fileset %s.\n"),
                     pageNum, fileset->fsName);
            return FAILURE;
        }   
        if( (currLBN == (LBNT) -1) || (currLBN == (LBNT)-2) )
        {
            /* If the page is in a hole in the file [frag file is sparse],
             * then mark the corresponding slots in fragSlot array as 
             * unusable and continue the loop.
             */
            set_unusable_frag_slots(fileset, slotNum, BF_FRAG_GRP_PGS*8, FALSE);
            continue;
        }

        /*
         * For the group starting on this page, check the header 
         * and mark it as unusable in the slot array.
         */
                
        status = read_page_from_cache(currVol, currLBN, &pPage);
        if (status != SUCCESS)
        {
            return status;
        }
  
        /*
         * Check the frag group header.
         */
                
        pFragGrp = (grpHdrT *)pPage->pageBuf;
            
        /*
         * The frag file version should be 1. We should not be
         * looking at any domains which have a frag version of 0
         * since the domain version would not be valid.
         */
        if (pFragGrp->version != 1)
        {
	    status = add_fixed_message(&(pPage->messages), frag,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_274, 
					       "Modified fileset %s frag group %d's version from %d to %d.\n"),
				       fileset->fsName, grpNumInFile,
				       pFragGrp->version, 1);
	    if (SUCCESS != status) 
	    {
	        return status;
	    }

            pFragGrp->version = 1;
            modified = TRUE;
        }
                
        /*
         * The nextFreeFrag and lastFreeFrag fields were used
         * in version 0 frag files only and should now be only -1.
         * The nextFreeGrp field which follows these will be checked
         * later in this routine.
         */
        if ((pFragGrp->nextFreeFrag != -1) || 
            (pFragGrp->lastFreeFrag != -1))
        {
	    status = add_fixed_message(&(pPage->messages), frag,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_275,
					       "Modified fileset %s frag group %d's next and last free frag fields to %d.\n"),
				       fileset->fsName, grpNumInFile, -1);
	    if (SUCCESS != status) 
	    {
		return status;
	    }

            pFragGrp->nextFreeFrag = -1;
            pFragGrp->lastFreeFrag = -1;
            modified = TRUE;
        }
            
        /*
         * The self field should contain the current page number.
         * Check the fragType field later.
         */
        if (pFragGrp->self != pageNum)
        {
	    status = add_fixed_message(&(pPage->messages), frag,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_276, 
					       "Modified fileset %s frag group %d's self group header field.\n"),
				       fileset->fsName, grpNumInFile);
	    if (SUCCESS != status) 
	    {
	        return status;
	    }
            pFragGrp->self = pageNum;
            modified = TRUE;
        }
            
        /*
         * Check the fileset ID for this group. For clones use the
         * tag and sequence number of the original fileset. Don't
         * bother checking the domain Id part since it could be
         * different for split mirror dual mounted domains.
         */
        if (FS_IS_CLONE(fileset->status))
        {
            checkTag.num = fileset->origClonePtr->filesetId.dirTag.num;
            checkTag.seq = fileset->origClonePtr->filesetId.dirTag.seq;
        }
        else
        {
            checkTag.num = fileset->filesetId.dirTag.num;
            checkTag.seq = fileset->filesetId.dirTag.seq;
        }
        if ((pFragGrp->setId.domainId.tv_sec != domain->dmnId.tv_sec) ||
            (pFragGrp->setId.domainId.tv_usec != domain->dmnId.tv_usec))
        {
	    status = add_fixed_message(&(pPage->messages), frag,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_277,
					       "Modified fileset %s frag group %d's domain ID from %x.%x to %x.%x.\n"),
				       fileset->fsName, grpNumInFile,
				       pFragGrp->setId.domainId.tv_sec, 
				       pFragGrp->setId.domainId.tv_usec,
				       domain->dmnId.tv_sec, domain->dmnId.tv_usec);
	    if (SUCCESS != status) 
	    {
                return status;
	    }

            pFragGrp->setId.domainId.tv_sec = domain->dmnId.tv_sec;
            pFragGrp->setId.domainId.tv_usec = domain->dmnId.tv_usec;
            modified = TRUE;
        
        }
        if ((pFragGrp->setId.dirTag.num != checkTag.num) ||
            (pFragGrp->setId.dirTag.seq != checkTag.seq))
        {
	    status = add_fixed_message(&(pPage->messages), frag,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_278,
					       "Modified fileset %s frag group %d's fileset ID from %x.%x to %x.%x.\n"),
				       fileset->fsName, grpNumInFile,
				       pFragGrp->setId.dirTag.num, pFragGrp->setId.dirTag.seq,
				       checkTag.num, checkTag.seq);
	    if (SUCCESS != status) 
	    {
                return status;
	    }

            pFragGrp->setId.dirTag.num = checkTag.num;
            pFragGrp->setId.dirTag.seq = checkTag.seq;
            modified = TRUE;
        }
        
        /*
         * Check the frag type and set it in the groupUse array.
         * If it's invalid, set it to -1 for now and we'll see
         * if we can figure out what it is later.
         */
        if ((pFragGrp->fragType < 0) || (pFragGrp->fragType > 7))
        {
            writemsg(SV_VERBOSE | SV_LOG_FOUND,
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_279,
			     "Invalid frag type for group %d in fileset %s.\n"),
                     grpNumInFile, fileset->fsName);
            pFragGrp->fragType = -1;
            modified = TRUE;
        }
        groupUse[grpNumInFile] = pFragGrp->fragType;
            
        /*
         * Compute the number of slots that we need to mark as
         * unusable for this group header and the number to mark
         * at the end of the group.
         */
        numSlots = 1;
        endNumSlots = 1;
        
        if (BF_FRAG_5K == pFragGrp->fragType)
        {
            endNumSlots = 2;
        }
        else if (BF_FRAG_1K == pFragGrp->fragType)
        {
            endNumSlots = 0;
        }
        
        if (domain->dmnVersion > 3)
        {
            if ((BF_FRAG_2K == pFragGrp->fragType) ||
                (BF_FRAG_6K == pFragGrp->fragType))
            {
                numSlots = 2;
                endNumSlots = 0;
            }
            else if (BF_FRAG_4K == pFragGrp->fragType)
            {
                numSlots = 4;
                endNumSlots = 0;
            }
        }
        else if (BF_FRAG_4K == pFragGrp->fragType)
        {
            endNumSlots = 3;
        }
            
        /*
         * The firstFrag field should be the starting slot number 
         * for this group. If the type is BF_FRAG_ANY it may also
         * be zero.
         */
        firstFrag = (grpNumInFile*BF_FRAG_GRP_SLOTS) + 1;
        if (pFragGrp->firstFrag != firstFrag)
        {
            if (pFragGrp->fragType != BF_FRAG_ANY ||
               pFragGrp->firstFrag != 0)
            {
		status = add_fixed_message(&(pPage->messages), frag,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_280, 
						   "Modified fileset %s frag group %d's first frag from %d to %d.\n"),
					   fileset->fsName,
					   grpNumInFile, 
					   pFragGrp->firstFrag,
					   firstFrag);
		if (SUCCESS != status) 
	        {
	            return status;
                }

                pFragGrp->firstFrag = firstFrag;
                modified = TRUE;
            }
        }
                    
        /*
         * Mark the group header slots as unusable slots, but also 
         * set the flag to mark them as used. This will make it
         * easier to check entire groups to see if they should
         * belong to a free list or not. Also set as unusable any
         * extra slots at the end of the list.
         */
        set_unusable_frag_slots(fileset, slotNum, numSlots, TRUE);
        if (endNumSlots != 0)
        {
            set_unusable_frag_slots(fileset, 
                                slotNum + BF_FRAG_GRP_SLOTS - endNumSlots,
                                endNumSlots, TRUE);
        }

        status = release_page_to_cache(currVol, currLBN, pPage,
                                       modified, FALSE);
        if (status != SUCCESS)
        {
            return status;
        }
             
        if (TRUE == modified)
        {
            fixed = TRUE;
            modified = FALSE;
        }
    } /* end for each page which starts a group in the frag file */

    
    if (TRUE == fixed)
    {
        writemsg(SV_VERBOSE | SV_LOG_FOUND,
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_281,
			 "Found problems in frag file headers for fileset %s.\n"),
                 fileset->fsName);
    }
    return SUCCESS;
} /* end set_frag_file_arrays */


/*
 * Function Name: set_unusable_frag_slots (3.n)
 *
 * Description: This routine sets bits in the frag slot array corresponding
 *              to the portions of the frag file which are unusable as frags
 *              for user files. The unusable bits are always set before any
 *              slots are marked as used from the collection of the frag data.
 *
 * Input parameters:
 *     fileset: The fileset being worked on.
 *     slot: The slot number in the frag file.
 *     size: The size of the unusable portion in 1K increments.
 *     setUsed: A flag to indicate that the slots should also be marked as used.
 *
 * Output parameters: N/A
 *
 * Returns: N/A
 * 
 */
void set_unusable_frag_slots (filesetT *fileset, int slot, 
                             int size, int setUsed) 
{
    char *funcName = "set_unusable_frag_slots";
    unsigned long (*slotArray)[3];
    unsigned long startMask;
    unsigned long endMask;
    int startIndex;
    int endIndex;
    int startOffset;
    int endOffset;
    int index;
    int alreadySet;
    
    alreadySet = FALSE;
    
    assert(NULL != fileset->fragSlotArray);
     
    slotArray = fileset->fragSlotArray;
    
    /*
     * Compute the starting and ending index into the array for the 
     * slot number and build the mask for the slot size. Note this can 
     * be larger that for the largest frag because sparse holes in the 
     * frag file are marked as unusable.
     */
    startIndex = slot / BITS_PER_LONG;
    endIndex = (slot + size - 1) / BITS_PER_LONG;
    startOffset = slot % BITS_PER_LONG;
    endOffset = (slot + size - 1) % BITS_PER_LONG;
    endMask = 0;

    /*
     * If all of the bits to set are in the same word, check to see
     * if we need to set all of the bits or not, and set up the mask.
     */ 
    if (startIndex == endIndex)
    {
        if (BITS_PER_LONG == size)
        {
            startMask = ALL_BITS_SET;
        }
        else
        {
            startMask = ((1L << size) - 1) << startOffset;
        }
    }
    else
    {
        /*
         * If the starting offset is zero, then the whole first
         * word is used.
         */
        if (0 == startOffset)
        {
            startMask = ALL_BITS_SET;
        }
        else
        {
            startMask = ((1L << (BITS_PER_LONG - startOffset)) - 1) << startOffset;
        }
        
        /*
         * Check if we need to use the whole last word.
         */
        if ((BITS_PER_LONG - 1) == endOffset)
        {
            endMask = ALL_BITS_SET;
        }
        else
        {
            endMask = (1L << (endOffset + 1)) - 1;
        }
    } /* end if startIndex equals endIndex */
    
    /*
     * Now set the bits in the slot array and see if any have already 
     * been set just as a sanity check.
     */
    if ((slotArray[startIndex][FRAG_UNUSABLE] & startMask) != 0)
    {
        alreadySet = TRUE;
    }
    
    slotArray[startIndex][FRAG_UNUSABLE] |= startMask;
    
    /*
     * Mark it also as used if needed. This will be used when checking
     * entire groups to see if they should be included in any of the free
     * lists. It is easier to check the entire group including the header
     * to see if it's used instead of checking the header for unusable and
     * the rest of the group as used.
     */
    
    if (TRUE == setUsed)
    {
        slotArray[startIndex][FRAG_USED] |= startMask;
    }
    
    /*
     * If we are setting bits in only one word, this loop should not
     * execute, otherwise it will set all 1's into the words in the
     * array between the startIndex and endIndex.
     */
    for (index = startIndex+1; index < endIndex; index++)
    {
        if (slotArray[index][FRAG_UNUSABLE] != 0)
        {
            alreadySet = TRUE;
        }
        slotArray[index][FRAG_UNUSABLE] = ALL_BITS_SET;
        
        if (TRUE == setUsed)
        {
            slotArray[index][FRAG_USED] = ALL_BITS_SET;
        }
    }
    
    if (endMask != 0)
    {
        if ((slotArray[endIndex][FRAG_UNUSABLE] & endMask) != 0)
        {
            alreadySet = TRUE;
        }
        slotArray[endIndex][FRAG_UNUSABLE] |= endMask;
        
        if (TRUE == setUsed)
        {
            slotArray[endIndex][FRAG_USED] |= endMask;
        }
    }
    
    if (TRUE == alreadySet)
    {
        writemsg(SV_DEBUG,
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_282,
			 "Slots marked as unusable in frag slot array were already set\n"));
        writemsg(SV_DEBUG | SV_CONT, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_283,
			 "in word(s) %d to %d.\n"), startIndex, endIndex);
    }
    
    return;
} /* end set_unusable_frag_slots */


/*
 * Function Name: collect_frag_info (3.72)
 *
 * Description: This routine sets bits in the frag slot array corresponding
 *              to the frags in existing files. If a slot is already allocated,
 *              it sets a bit indicating that there is an overlap for that slot.
 *
 * Input parameters:
 *     fileset: The fileset being worked on.
 *     size: The size of the frag.
 *     slot: The slot number in the frag file.
 *     tag: The tag being worked on.
 *     settag: The settag for the tag.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 * 
 */
int collect_frag_info (filesetT *fileset, int size,
                       int slot, bfTagT tag, bfTagT settag) 
{
    char *funcName = "collect_frag_info";
    char *groupUse;
    unsigned long (*slotArray)[3];
    unsigned long startMask;
    unsigned long endMask;
    fragOverlapT *pFragNode;
    fragOverlapT *currPtr;
    fragOverlapT *nextPtr;
    int startIndex;
    int endIndex;
    int startOffset;
    int endOffset;
    int maxSlotNum;
    int lastPage;
    int groupNum;
         
    if (NULL == fileset->fragSlotArray)
    {
	if (FS_IS_CLONE(fileset->status)) 
	{
	    /*
	     * Clone uses original filesets frag array.
	     */
	    return SUCCESS;
	}

        /*
         * There is no frag file for this fileset, so fail
         * on setting bits. We need to determine if this is
         * actually an internal error or just a debug. The
         * frag will be deleted in either case.
         */
	writemsg(SV_VERBOSE | SV_LOG_FOUND, 
	         catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_284, 
			 "No frag slot array has been allocated for fileset %s.\n"),
		 fileset->fsName);
        return FAILURE;
    }
    
    /*
     * Verify that the frag size is valid - it should be 1-7 since we
     * don't call this routine if the size is 0.
     */
    if ((size < 1) || (size > 7))
    {
        writemsg(SV_VERBOSE | SV_LOG_FOUND, 
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_285, 
			 "Size of %dK invalid for frag at slot %d in fileset %s.\n"),
                 size, slot, fileset->fsName);
        return FAILURE;
    }
    /*
     * Make sure the slot fits within the limits of the slot array.
     * If not, then the frag record or frag file is probably corrupt 
     * and the frag record will be deleted when FAILURE is returned.
     */
    lastPage = fileset->fragLBN[fileset->fragLBNSize].page;
    if (FS_IS_CLONE(fileset->status))
    {
        /*
         * If this is a clone, use the size of the original fileset
         * frag file if it is larger because we can get frags in the 
         * clone pointing into the original.
         */
        if (lastPage < fileset->origClonePtr->fragLBN[fileset->origClonePtr->fragLBNSize].page)
        {
            lastPage = fileset->origClonePtr->fragLBN[fileset->origClonePtr->fragLBNSize].page;
        }

    }
    maxSlotNum = lastPage*8;

    if ((slot < 0) || (slot + size > maxSlotNum))
    {
        writemsg(SV_VERBOSE | SV_LOG_FOUND, 
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_286, 
			 "%dK frag at slot %d is outside of frag file for fileset %s.\n"),
                 size, slot, fileset->fsName);
        return FAILURE;
    }

    /*
     * See if the slot matches the type for the frag group it's in.
     */
    groupUse = fileset->groupUse;
    groupNum = slot / BF_FRAG_GRP_SLOTS;
    
    if ((groupUse[groupNum] != size) && (-1 != groupUse[groupNum]))
    {
        writemsg(SV_VERBOSE | SV_LOG_FOUND,
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_287, 
			 "%dK size of frag at slot %d doesn't match group type of %dK.\n"),
                 size, slot, groupUse[groupNum]);
        return FAILURE;
    }
        
    slotArray = fileset->fragSlotArray;
     
    /*
     * Compute the starting and ending index into the array for the 
     * slot number and build the mask for the slot size. Usually this
     * should not be larger than 7 bits, however it is possible for the
     * bits to span 2 words.
     */
    startIndex = slot / BITS_PER_LONG;
    endIndex = (slot + size - 1) / BITS_PER_LONG;
    startOffset = slot % BITS_PER_LONG;
    endOffset = (slot + size - 1) % BITS_PER_LONG;
    endMask = 0;

    /*
     * Set up the masks for the first or both words as needed.
     */ 
    if (startIndex == endIndex)
    {
        startMask = ((1L << size) - 1) << startOffset;
    }
    else
    {
        startMask = ((1L << (BITS_PER_LONG - startOffset)) - 1) << startOffset;
        endMask = (1L << (endOffset + 1)) - 1;
    } /* end if startIndex equals endIndex */
    
    /*
     * Now see if any of the bits in the slot array are unusable. If so,
     * then this is a failure and the frag should be deleted.
     */
    if (((slotArray[startIndex][FRAG_UNUSABLE] & startMask) != 0) ||
        (startIndex != endIndex &&
	 ((slotArray[endIndex][FRAG_UNUSABLE] & endMask) != 0)))
    {
        if (FS_IS_CLONE(fileset->status))
        {
            unsigned long (*origArray)[3];
            
            /*
             * If this is a clone, see if the frag slot is usable in the
             * original fileset. If so, then set it as used. We don't care
             * if the bits are already set since we probably found it from
             * the original fileset.
             *
             * FUTURE: get the tagNode from the original for this tag and
             * make sure the frag matches before setting the slot array bits.
             */
            origArray = fileset->origClonePtr->fragSlotArray;
            if (((origArray[startIndex][FRAG_UNUSABLE] & startMask) != 0) ||
                (startIndex != endIndex &&
	         ((origArray[endIndex][FRAG_UNUSABLE] & endMask) != 0)))
            {
                writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_288, 
				 "Frag at slot %d of size %dK overlaps unusable frag file slots for fileset %s.\n"),
			 slot, size, fileset->origClonePtr->fsName);
                return FAILURE;
            }            
            /*
             * Set in-use bits for frag in original fileset's slot array.
             */
        
            origArray[startIndex][FRAG_USED] |= startMask;
            if (startIndex != endIndex)
            {
                origArray[endIndex][FRAG_USED] |= endMask;
            }
        }
        else
        {
            writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_288,
			     "Frag at slot %d of size %dK overlaps unusable frag file slots for fileset %s.\n"),
		     slot, size, fileset->fsName);
            return FAILURE;
        }
    }
    
    /*
     * If groupUse is -1, we don't know what the group type is, so
     * put this type in for the use and we'll put it in the group
     * later.
     *
     * FUTURE: Make sure the slot and size match up with the slot
     * boundaries within the group.
     */
    if (-1 == groupUse[groupNum])
    {
        groupUse[groupNum] = size;
    }
     
    /*
     * See if the frag overlaps any slots already in use, if so, put it
     * on the overlap list and mark the overlap bits.
     */
     
    if (((slotArray[startIndex][FRAG_USED] & startMask) != 0) ||
        (startIndex != endIndex &&
	 ((slotArray[endIndex][FRAG_USED] & endMask) != 0)))
    {
        slotArray[startIndex][FRAG_OVERLAP] |= startMask;
        if (startIndex != endIndex)
        {
            slotArray[endIndex][FRAG_OVERLAP] |= endMask;
        }
        
        pFragNode = (fragOverlapT *)ffd_malloc(sizeof(fragOverlapT));
        if (NULL == pFragNode)
        {
            writemsg(SV_ERR | SV_LOG_ERR, 
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_290,
			     "frag overlap structure"));
            return NO_MEMORY;
        }
        
        pFragNode->startSlot = slot;
        pFragNode->fragSize = size;
        pFragNode->tagSeq.num = tag.num;
        pFragNode->tagSeq.seq = tag.seq;
        pFragNode->setTag.num = settag.num;
        pFragNode->setTag.seq = settag.seq;
        pFragNode->nextFrag = NULL;
        
        currPtr = fileset->fragOverlapHead;
        
        /*
         * Add the node to the overlap list sorted by slot number. We
         * can have multiple nodes with the same slot number.
         */
        if (NULL == currPtr || pFragNode->startSlot <= currPtr->startSlot)
        {
            /*
             * The node goes at the head of the list if there
             * are no other nodes or if the slot is lower than
             * that of the first node on the list.
             */
            nextPtr = fileset->fragOverlapHead;
            fileset->fragOverlapHead = pFragNode;
            pFragNode->nextFrag = nextPtr;
        }
        else
        {
            /*
             * The node goes somewhere after the first node so
             * insert the node in the list by slot number. The
             * list can have multiple entries with the same slot number.
             */
            nextPtr = currPtr->nextFrag;
            while (nextPtr != NULL)
            {
                if (pFragNode->startSlot <= nextPtr->startSlot)
                {
                    /*
                     * Insert the node here.
                     */
                    currPtr->nextFrag = pFragNode;
                    pFragNode->nextFrag = nextPtr;
                    break;
                }
                else
                {
                    currPtr = nextPtr;
                    nextPtr = nextPtr->nextFrag;
                }
            }
            if (NULL == nextPtr)
            {
                /*
                 * We hit the end of the list without inserting
                 * the node, so put it at the end.
                 */
                 currPtr->nextFrag = pFragNode;
            }
        } /* end if frag overlap head is NULL */
    }
    else
    {
        /*
         * Set in-use bits for frag in slot array.
         */
        
        slotArray[startIndex][FRAG_USED] |= startMask;
        if (startIndex != endIndex)
        {
            slotArray[endIndex][FRAG_USED] |= endMask;
        }
        
    } /* end if frag overlaps slots in use */
    return SUCCESS;
} /* end collect_frag_info */


/*
 * Function Name: correct_frag_overlaps (3.n)
 *
 * Description: This function is used to delete overlapping frags from
 *              the frag file.
 *
 * Input parameters:
 *     domain: The domain being worked on.
 *     fileset: The fileset being worked on.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FIXED.
 * 
 */
int correct_frag_overlaps(filesetT *fileset, domainT *domain) 
{
    char *funcName = "correct_frag_overlaps";
    fragOverlapT *overlapPtr;
    tagNodeT *pTagNode;
    nodeT *node;
    int status;
    
    if (NULL == fileset->fragSlotArray)
    {
        /*
         * There is no frag file for this fileset, so just return.
         */
        return SUCCESS;
    }
    
    /*
     * If there are any overlapping frags, find the frags they overlap
     * and delete those from the appropriate mcells. Then delete the 
     * frags in the list.
     */
    if (fileset->fragOverlapHead != NULL)
    {
        /*
         * Look through the tag list to see if we can find which
         * tags overlap the slots in the overlap list.
         */
        status = find_first_node(fileset->tagList, (void **)&pTagNode, &node);
        
        if (SUCCESS != status)
        {
            if (NOT_FOUND == status)
            {
                writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_291,
				 "Internal error - can't find first node in tag list for fileset %s.\n"),
			 fileset->fsName);
            return status;
            }
            else
            {
                return status;
            }
        }
        
        while (pTagNode != NULL)
        {
            /*
             * If the tag has a frag, see if it overlaps 
             * any on the overlap list.
             */
            if (pTagNode->fragSlot > BF_FRAG_ANY)
            {
                /*
                 * FUTURE: Check the overlap bits for the slot first
                 * and just continue if there are no overlaps.
                 */
                 
                /*
                 * Loop through the overlap list to check for
                 * slot overlaps.
                 */
                overlapPtr = fileset->fragOverlapHead;
                
                while (overlapPtr != NULL)
                {
                    /*
                     * If it overlaps any other frag, then delete the
                     * frag from the metadata and node. Note that we
                     * could be deleting the frag from the tag specified
                     * in the overlap list. This is OK since we don't
                     * delete any of the overlap list until after all of 
                     * the tags are checked.
                     */
                    if ((pTagNode->fragSlot >= overlapPtr->startSlot) &&
                        ((pTagNode->fragSlot + pTagNode->fragSize) <= 
                        ((overlapPtr->startSlot + overlapPtr->fragSize))))
                    {
                        /*
                         * Delete the size of the frag from the file size
                         * (i.e. use only the pages attributed to extents).
                         */
                        pTagNode->size = (pTagNode->size / PAGESIZE) * PAGESIZE;
                        pTagNode->fragSize = BF_FRAG_ANY;
                        pTagNode->fragSlot = 0;
                        pTagNode->fragPage = 0;
                        
			writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_292,
					 "Deleted fileset %s tag (%d.%x)'s frag.\n"),
				 fileset->fsName, 
				 pTagNode->tagSeq.num,
				 pTagNode->tagSeq.seq);

                        status = correct_fsstat(fileset, pTagNode);
                        if (status != SUCCESS)
                        {
                            return status;
                        }

                        if (0 == pTagNode->size)
                        {
                            /*
                             * If the frag was the only thing in the file,
                             * then make sure we're not leaving an empty
                             * chain around.
                             */
                            status = correct_empty_chain(fileset, pTagNode);
                            if (status != FIXED && status != SUCCESS)
                            {
                                return status;
                            }
                        }
                        break;                      
                    } /* end if frag overlaps frag in list */
                    
                    overlapPtr = overlapPtr->nextFrag;
                    
                } /* end while overlapPtr is not NULL */
            } /* end if tag has a frag */
            
            status = find_next_node(fileset->tagList, (void **)&pTagNode, &node);
            if (SUCCESS != status && NOT_FOUND != status)
            {
                return status;
            }
            
        } /* end while tag pointer is not NULL */
        
        /*
         * We should have deleted all of the frag metadata which was
         * involved in frag overlaps and cleared the corresponding frag
         * data in the tag nodes. So now just free the frag overlap list.
         */
        
        overlapPtr = fileset->fragOverlapHead;
        
        while (overlapPtr != NULL)
        {
            fragOverlapT *thisFrag;

            /*
             * Clear the used bits for this frag.
             */
            thisFrag = overlapPtr;
            
            set_frag_unused(fileset, thisFrag->startSlot, thisFrag->fragSize);           
            
            /*
             * Save the pointer to the next frag overlap and free this one.
             */
            overlapPtr = overlapPtr->nextFrag;
            free(thisFrag);
        } /* end while overlap pointer is not NULL */
    } /* end if frag overlap list is not NULL */
    
    return SUCCESS;
} /* end correct_frag_overlaps */

/*
 * Function Name: correct_frag_file (3.73)
 *
 * Description: This function is used to check and fix problems in the
 *              frag file.
 *
 * Input parameters:
 *     domain: The domain being worked on.
 *     fileset: The fileset being worked on.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FIXED or NO_MEMORY.
 * 
 */
int correct_frag_file(filesetT *fileset, domainT *domain) 
{
    char *funcName = "correct_frag_file";
    char *groupUse;    /* Array indicating frag size for group */
    char *groupOnList; /* Array indicating if group is on the free list */
    unsigned long (*slotArray)[3];
    unsigned long (*origArray)[3];
    pagetoLBNT *fragXtnts;  /* Local pointer to fragLBN array */
    pageT *pPage;
    bsMPgT *bmtPage;
    bsMCT *pMcell;
    bsMRT *pRecord;
    grpHdrT *pFragGrp;
    mcellT mcell;
    mcellT nextMcell;
    volumeT *volume;
    unsigned long startMask;
    unsigned long endMask;
    pageT *pGrpPage;
    bsBfSetAttrT *pFsAttr;
    int groupVol;
    LBNT groupLBN;
    int startOffset;
    int endOffset;
    int status;
    int pageNum;
    int index;
    int i;
    int j;
    int modified;
    int fixed;
    int grpNumInFile;
    int numGroups;
    int group;
    int groupPage;
    int prevPage;
    int changePrev;
    int attrModified;
    int attrFound;
    LBNT currLBN;
    int currVol;
    int nextGrpVol;
    LBNT nextGrpLBN;
    int nextGrpPage;
    int startIndex;
    int endIndex;
    int slotNum;
    int startSlot;
    int freeSlotCount;
    unsigned short freeSlotList[BF_FRAG_GRP_SLOTS];
    int resetList;
    unsigned short freeIndex;  /* Index for stepping through free list. */
    unsigned long slotMask[BITS_PER_LONG] =
        { 0x0000000000000001L, 0x0000000000000002L, 0x0000000000000004L,
          0x0000000000000008L, 0x0000000000000010L, 0x0000000000000020L,
          0x0000000000000040L, 0x0000000000000080L, 0x0000000000000100L,
          0x0000000000000200L, 0x0000000000000400L, 0x0000000000000800L,
          0x0000000000001000L, 0x0000000000002000L, 0x0000000000004000L,
          0x0000000000008000L, 0x0000000000010000L, 0x0000000000020000L,
          0x0000000000040000L, 0x0000000000080000L, 0x0000000000100000L,
          0x0000000000200000L, 0x0000000000400000L, 0x0000000000800000L,
          0x0000000001000000L, 0x0000000002000000L, 0x0000000004000000L,
          0x0000000008000000L, 0x0000000010000000L, 0x0000000020000000L,
          0x0000000040000000L, 0x0000000080000000L, 0x0000000100000000L,
          0x0000000200000000L, 0x0000000400000000L, 0x0000000800000000L,
          0x0000001000000000L, 0x0000002000000000L, 0x0000004000000000L,
          0x0000008000000000L, 0x0000010000000000L, 0x0000020000000000L,
          0x0000040000000000L, 0x0000080000000000L, 0x0000100000000000L,
          0x0000200000000000L, 0x0000400000000000L, 0x0000800000000000L,
          0x0001000000000000L, 0x0002000000000000L, 0x0004000000000000L,
          0x0008000000000000L, 0x0010000000000000L, 0x0020000000000000L,
          0x0040000000000000L, 0x0080000000000000L, 0x0100000000000000L,
          0x0200000000000000L, 0x0400000000000000L, 0x0800000000000000L,
          0x1000000000000000L, 0x2000000000000000L, 0x4000000000000000L,
          0x8000000000000000L };   

    modified = FALSE;
    fixed = FALSE;
    resetList = FALSE;
    attrModified = FALSE;
    attrFound = FALSE;
    slotArray = fileset->fragSlotArray;

    if (NULL == fileset->fragSlotArray)
    {
        /*
         * There is no frag file for this fileset, so just return.
         */
        return SUCCESS;
    }

    /*
     * If this is a clone, get the slot array from the original
     * fileset for use when we process the group free lists later.
     */
    if (FS_IS_CLONE(fileset->status))
    {
        origArray = fileset->origClonePtr->fragSlotArray;
    }
    
    /*
     * Go through pages in the frag file which start group headers 
     * and check the free frag slots to see if they match what's in 
     * the slot array. Also check the frag size in groupUse and mark 
     * if they have any free slots in groupOnList.
     */
    grpNumInFile = 0;
    fragXtnts = fileset->fragLBN;
    slotNum = 0;
    numGroups = fragXtnts[fileset->fragLBNSize].page/BF_FRAG_GRP_PGS;
    groupUse = fileset->groupUse;

    groupOnList = (char *)ffd_calloc(numGroups, sizeof(char));

    if (NULL == groupOnList)
    {
        writemsg(SV_ERR | SV_LOG_ERR,
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_293, 
			 "group status array"));
        return NO_MEMORY;
    }
    
    /*
     * Initialize the status array (-1 means the group is full,
     * TRUE means it was found on the free group list,
     * FALSE means it has free slots and was not found on the list).
     *
     * FUTURE: Use better values so this is easier to understand.
     */   
    for (group = 0; group < numGroups; group++)
    {
        groupOnList[group] = -1;
    }

    for (pageNum = 0; 
         pageNum < fragXtnts[fileset->fragLBNSize].page; 
         pageNum += BF_FRAG_GRP_PGS, 
             slotNum += BF_FRAG_GRP_SLOTS, grpNumInFile++)
    {
        status = convert_page_to_lbn(fileset->fragLBN, fileset->fragLBNSize,
                                     pageNum, &currLBN, &currVol);
        if (status != SUCCESS)
        {
            writemsg(SV_ERR | SV_LOG_ERR, 
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_273, 
			     "Can't convert frag page %d to LBN for fileset %s.\n"),
                     pageNum, fileset->fsName);
            return FAILURE;
        }   
        
        if ( (currLBN == (LBNT) -1 ) || (currLBN == (LBNT) -2 ))
        {
            /* If the page is in a hole in the file [frag file is sparse],
             * then skip it.
             */
            continue;
        }

        /*
         * If the page exists, then read it.
         */                
        status = read_page_from_cache(currVol, currLBN, &pPage);
        if (status != SUCCESS)
        {
            return status;
        }
         
        /*
         * Point to the frag group header.
         */
                
        pFragGrp = (grpHdrT *)pPage->pageBuf;
        
        /*
         * If the frag type was corrupt and we found frags for this
         * group, then set the type to whatever was found, otherwise
         * set the type to BF_FRAG_ANY.
         */
        if (-1 == pFragGrp->fragType)
        {
            if (-1 == groupUse[grpNumInFile])
            {
                groupUse[grpNumInFile] = BF_FRAG_ANY;
            }
            status = add_fixed_message(&(pPage->messages), frag,
				       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_295, 
					       "Modified fileset %s frag group %d's type to %dK.\n"),
				       fileset->fsName, grpNumInFile, groupUse[grpNumInFile]);
            if (SUCCESS != status) 
            {
                return status;
            }
            
            pFragGrp->fragType = groupUse[grpNumInFile];
            modified = TRUE;
        }

        /*
         * Build the masks to check for free slots in this group.
         */
        startIndex = slotNum / BITS_PER_LONG;
        endIndex = startIndex + 1;

        /*
         * Determine the starting slot in the free list for the frags
         * based on frag type. It is always 1 in V3 domains, but changes
         * in V4+ domains for frags of size 2K, 4K, and 6K. This will be
         * used to determine which index into the free list array will be
         * the first accessed in various loops.
         */ 
        startSlot = 1;
        if (domain->dmnVersion > 3)
        {
            if ((BF_FRAG_2K == pFragGrp->fragType) ||
                (BF_FRAG_6K == pFragGrp->fragType))
            {
                startSlot = 2;
            }
            else if (BF_FRAG_4K == pFragGrp->fragType)
            {
                startSlot = 4;
            }
        }

        /*
         * Since each group is 16 pages, we should always be testing
         * two consecutive long words in the slot array for a group.
         */
        
        if (((slotArray[startIndex][FRAG_USED] |
                  slotArray[startIndex][FRAG_UNUSABLE]) != ALL_BITS_SET) ||
            ((slotArray[endIndex][FRAG_USED] | 
                  slotArray[endIndex][FRAG_UNUSABLE]) != ALL_BITS_SET))
        {
            /*
             * Originally mark all groups with free slots as not on the
             * free list. Then we'll mark them when we follow the free
             * lists. Groups that don't have free slots have a value of -1.
             */
            groupOnList[grpNumInFile] = FALSE;
            
            /*
             * We have some free frags in the group, so count them
             * and make sure they match the free list in the group.
             */
            for (i = 0; i < BF_FRAG_GRP_SLOTS; i++)
            {
                freeSlotList[i] = 0;
            }
             
            freeSlotCount = 0;
            j = 0; /* index into local freeSlotList */

            for (index = startIndex; index <= endIndex; index++)
            {
                for (i = 0; i < BITS_PER_LONG; i++)
                {
                    /*
                     * If the slot is unusable or in use, mark it as used.
                     */
                    if (((slotArray[index][FRAG_UNUSABLE] & slotMask[i]) != 0) ||
                        ((slotArray[index][FRAG_USED] & slotMask[i]) != 0)) 
                    {
                        FG_SET_USED(freeSlotList[j]);
                    }
                    else
                    {
                        FG_SET_FREE(freeSlotList[j]);
                        freeSlotCount++;
                    }
                    j++;
                } /* end for each slot in the frag group */
            } /* end for startIndex to endIndex */

            /*
             * For clones, just make sure that all used frags are not on the
             * free list.
             *
             * FUTURE: Remove the frag from the free list.
             */
            if (FS_IS_CLONE(fileset->status))
            {
                freeIndex = pFragGrp->freeList[0];
                while (freeIndex != ALL_BITS_SHORT)
                {
                    if (FG_IS_USED(freeSlotList[freeIndex]))
                    {
                        /*
                         * If we have a slot on the free list marked
                         * used, then just write an INFO message for now.
                         */
                        writemsg(SV_ERR | SV_LOG_INFO, 
                                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_296,
					 "Slot %d used but marked free in group %d for fileset %s.\n"),
                                 freeSlotList[freeIndex], grpNumInFile, 
                                 fileset->fsName);
                        break;
                    }
                    if (FG_IS_ONFRLIST(freeSlotList[freeIndex])) 
	            {
                        writemsg(SV_ERR | SV_LOG_INFO, 
                                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_297,
					 "Loop on free list at slot %d of group %d for fileset %s.\n"),
                                 freeIndex, grpNumInFile, fileset->fsName);
                        break;
	            }
                    FG_SET_ONFRLIST(freeSlotList[freeIndex]);
                    freeIndex = pFragGrp->freeList[freeIndex];
                }
            }
            else
            {
                resetList = FALSE;
       
                /*
                 * Check if all of the free frags can be found by following
                 * the links in the free list. FreeList[0] points to the first
                 * free frag in the list. The list is terminated by all ones.
                 */
                if (pFragGrp->fragType != BF_FRAG_ANY)
                {
                   freeIndex = pFragGrp->freeList[0];
                   while (freeIndex != ALL_BITS_SHORT)
                   {
                        if (FG_IS_USED(freeSlotList[freeIndex]))
                        {
                            /*
                             * If we have a slot on the free list marked
                             * as used, then we need to reset the list.
                             */
                            resetList = TRUE;
                            break;
                        }
                        if (FG_IS_ONFRLIST(freeSlotList[freeIndex])) 
	                {
		            resetList = TRUE;
                            break;
	                }
                        FG_SET_ONFRLIST(freeSlotList[freeIndex]);
                        freeIndex = pFragGrp->freeList[freeIndex];
                    } /* end while freeIndex != ALL_BITS_SHORT */
            
                    if (FALSE == resetList)
                    {
                        /*
                         * Check if the number of free frags in the header match 
                         * the number that were found.
                         */            
                        if (pFragGrp->freeFrags != freeSlotCount/pFragGrp->fragType)
                        {
                            resetList = TRUE;
                        }
                        else
                        {
                            /*
                             * Now step through the list to see if any free frags
                             * exist which haven't been found on the free list.
                             */
                            for (i = startSlot;
                                 i < BF_FRAG_GRP_SLOTS - 1;
                                 i += pFragGrp->fragType)
                             {
                                if (FG_IS_FREE(freeSlotList[i]))
                                {
                                    resetList = TRUE;
                                    break;
                                }
                            }
                        }
                    } /* end if resetList is FALSE */

                    /*
                     * If we don't have the correct number of free frags or the
                     * free list doesn't match what's allocated, then reset the
                     * free list based on what was allocated.
                     */
                   if (TRUE == resetList)
                    {
                        freeIndex = 0;
                        freeSlotCount = 0;
                        for (i = startSlot; 
                             i < BF_FRAG_GRP_SLOTS - 1 + pFragGrp->fragType; 
                             i += pFragGrp->fragType)
                        {
                           if (FG_IS_FREE(freeSlotList[i]) ||
                                FG_IS_ONFRLIST(freeSlotList[i]))
                            {
                                pFragGrp->freeList[freeIndex] = i;
                                freeIndex = i;
                                freeSlotCount++;
                            }
                        }
                        status = add_fixed_message(&(pPage->messages), frag,
						   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_298, 
							   "Modified fileset %s frag group %d's free frag list.\n"),
						   fileset->fsName, grpNumInFile);
                        if (SUCCESS != status)
                        {
		           return status;
                        }

                        /*
                         * Set the last free slot to all ones to end the list
                         * and set the number of free frags in the header.
                         */
                        pFragGrp->freeList[freeIndex] = ALL_BITS_SHORT;
                        pFragGrp->freeFrags = freeSlotCount;
                        modified = TRUE;
                
                    } /* end if we need to reset the free frag list */
                } /* end if group frag type is not BF_FRAG_ANY */
            } /* end if fileset is a clone */
        } /* end if free slots exist in this frag group */
        else
        {
            /*
             * If there are no free frags then freeFrags should be 0
             * and freeList[0] should be -1.
             */
            if (pFragGrp->freeList[0] != ALL_BITS_SHORT)
            {
                status = add_fixed_message(&(pPage->messages), frag,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_298, 
						   "Modified fileset %s frag group %d's free frag list.\n"),
					   fileset->fsName, grpNumInFile);
                if (SUCCESS != status)
                {
                    return status;
                }
                pFragGrp->freeList[0] = ALL_BITS_SHORT;
                modified = TRUE;
            } /* end if freeList[0] != ALL_BITS_SHORT */
            
            if (pFragGrp->freeFrags != 0)
            {
                status = add_fixed_message(&(pPage->messages), frag,
					   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_299, 
						   "Set free frags to 0 for full frag group %d in fileset %s.\n"),
					   grpNumInFile, fileset->fsName);
                if (SUCCESS != status)
                {
                    return status;
                }
                pFragGrp->freeFrags = 0;
                modified = TRUE;
            }
        } /* end else no free slots in this frag group */
        
        status = release_page_to_cache(currVol, currLBN, pPage,
                                       modified, FALSE);
        if (status != SUCCESS)
        {
            return status;
        }
            
        if (TRUE == modified)
        {
            fixed = TRUE;
            modified = FALSE;
        }
    } /* end for each page which starts a group in the frag file */

    /*
     * If this is a clone, don't bother checking the free group links.
     * So just return from here.
     */
    if (FS_IS_CLONE(fileset->status))
    {
        free(groupOnList);
        if (TRUE == fixed)
        {
            return FIXED;
        }
    
        return SUCCESS;
    }
    /*
     * Check the group free lists starting with the free group 
     * pointers in the BSR_BFS_ATTR record and following the links 
     * in the group headers. First get the page and LBN for the tag 
     * file primary mcell so we can get the record off disk.
     */
    volume = &(domain->volumes[fileset->fsPmcell.vol]);

    mcell.vol = fileset->fsPmcell.vol;
    mcell.page = fileset->fsPmcell.page;
    mcell.cell = fileset->fsPmcell.cell;

    while (mcell.vol != 0 && FALSE == attrFound)
    {
        status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
                                     volume->volRbmt->bmtLBNSize,
                                     mcell.page, &currLBN, &currVol);
        if (SUCCESS != status)
        {
            writemsg(SV_ERR | SV_LOG_ERR, 
                     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_128,
			     "Can't convert BMT page %d to LBN.\n"),
		     mcell.page);
            return FAILURE;
        }
    
        status = read_page_from_cache(currVol, currLBN, &pPage);
        if (SUCCESS != status)
        {
              return status;
        }
    
        bmtPage = (bsMPgT *)pPage->pageBuf;
        pMcell = &(bmtPage->bsMCA[mcell.cell]);
    
        nextMcell.vol = pMcell->nextVdIndex;
        nextMcell.page = pMcell->nextMCId.page;
        nextMcell.cell = pMcell->nextMCId.cell;
    
        /*
         * Index to the frag file free group pointers in the 
         * BSR_BFS_ATTR record.
         */
     
        pRecord = (bsMRT *)pMcell->bsMR0;
    
        while ((FALSE == attrFound) &&
               (pRecord->type != 0) &&
               (pRecord->bCnt != 0) &&
               (pRecord < ((bsMRT *)&(pMcell->bsMR0[BSC_R_SZ]))))
        {
            if (BSR_BFS_ATTR != pRecord->type)
            {
                pRecord = (bsMRT *)(((char *)pRecord) + 
                               roundup(pRecord->bCnt, sizeof(int)));
                continue;
            }
        
            attrFound = TRUE;
            pFsAttr = (bsBfSetAttrT *)((char *)pRecord + sizeof(bsMRT));
            for (group = 0; group < BFS_FRAG_MAX; group++)
            {
                groupPage = pFsAttr->fragGrps[group].firstFreeGrp;
                prevPage = groupPage;
                changePrev = FALSE;
                
                /*
                 * Check to see if this page is within the frag
                 * file or if we already saw this group while
                 * following links, or if it's full. If so set
                 * the pointers in the record to -1 to indicate 
                 * that we can't find any groups for this type.
                 */
                if ((groupPage >= fragXtnts[fileset->fragLBNSize].page) ||
                    (groupPage < -1) ||
                    ((-1 != groupPage) && 
                    (FALSE != groupOnList[groupPage/BF_FRAG_GRP_PGS])))
                {
	            writemsg(SV_VERBOSE | SV_LOG_FOUND,
		             catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_301,
				     "Bad pointer for %dK frag free list for fileset %s.\n"),
			     group,fileset->fsName);
			    
                    pFsAttr->fragGrps[group].firstFreeGrp = -1;
                    pFsAttr->fragGrps[group].lastFreeGrp = -1;
                    attrModified = TRUE;
                    groupPage = -1;
                }
                
                /*
                 * Now walk through the frag group headers and mark
                 * those which we've found to see if we have links
                 * from the first free group to the last.
                 */
                while (-1 != groupPage)
                {
                    status = convert_page_to_lbn(fileset->fragLBN,
                                                 fileset->fragLBNSize,
                                                 groupPage, &groupLBN,
                                                 &groupVol);
                    if (SUCCESS != status)
                    {
                        writemsg(SV_ERR | SV_LOG_ERR,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_273,
					 "Can't convert frag page %d to LBN for fileset %s.\n"),
                                 groupPage, fileset->fsName);
                        return FAILURE;
                    }
                    
                    if ((LBNT)-1 == groupLBN)
                    {
                        /*
                         * FUTURE: need to handle this case where the
                         * page doesn't exist in the frag extents.
                         * Should probably terminate the group list
                         * or reset the pointers in the fileset attributes.
                         */
                        groupPage = -1;
                        continue;
                    }
                        
                    /*
                     * Make sure we aren't going to try to read the same
                     * page we currently have open. This would mean that
                     * the BMT and the frag file overlap, so just return
                     * failure in this case.
                     */
                    if ((groupLBN == currLBN) && (groupVol == currVol))
                    {
                        writemsg(SV_ERR | SV_LOG_ERR, 
                                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_302,
					 "BMT and frag file for fileset %s overlap at LBN %u.\n"),
                                 fileset->fsName, currLBN);
                        corrupt_exit(NULL);
                    }
                    
                    status = read_page_from_cache(groupVol, groupLBN, &pGrpPage);
                    if (SUCCESS != status)
                    {
                        return status;
                    }
                    
                    pFragGrp = (grpHdrT *)pGrpPage->pageBuf;
                    if (pFragGrp->fragType != group)
                    {
                        /*
                         * The group frag type doesn't match the type
                         * of the list we are following. So truncate
                         * the list and set a flag to truncate to set
                         * the list pointer on the previous page to -1.
                         */
                        changePrev = TRUE;
                        if (prevPage == groupPage)
                        {
                            /*
                             * We are on the first page of the group, so set
                             * set the first and last group pointers in the 
                             * BSR_BFS_ATTR record to -1.
                             */
		            status = add_fixed_message(&(pGrpPage->messages),
						       frag,
						       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_303,
							       "Modified fileset %s nextFreeGrp from %d to %d for type %dK.\n"),
						       fileset->fsName,
						       pFragGrp->nextFreeGrp,
						       -1, group);
		            if (SUCCESS != status) 
		            {
			        return status;
			    }

                            pFsAttr->fragGrps[group].firstFreeGrp = -1;
                            pFsAttr->fragGrps[group].lastFreeGrp = -1;
                            attrModified = TRUE;
                        } /* end if prevPage equals groupPage */
                        
                        /*
                         * Set the last group pointer in the attributes record
                         * to end the list with the previous page.
                         */   
                        if (0 != group)
                        {
                            int prevValue;
                            
                            prevValue = prevPage;
                            if (prevPage == groupPage)
                            {
                                prevValue = -1;
                            }
                            /*
                             * Pointer to last group is ignored
                             * for 0K frags so don't set it unless
                             * the frag type is 1-7K.
                             */
			    status = add_fixed_message(&(pPage->messages),
						       frag,
						       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_304, 
							       "Modified fileset %s lastFreeGrp from %d to %d for type %dK.\n"),
						       fileset->fsName,
						       pFsAttr->fragGrps[group].lastFreeGrp,
						       prevValue, group);
			    if (SUCCESS != status) 
			    {
				return status;
			    }
                            pFsAttr->fragGrps[group].lastFreeGrp = prevValue;
                            attrModified = TRUE;
                        } /* end if group is not 0 */
                    } /* end if frag type doesn't match group */
                    else
                    {
                        if (-1 == pFragGrp->nextFreeGrp)
                        {
                            /*
                             * If we are at the end of the list, mark this
                             * group as on the list.
                             */
                            groupOnList[groupPage/BF_FRAG_GRP_PGS] = TRUE;
                            
                            /*
                             * If the last group pointer in the attributes doesn't
                             * point to this group, then set it.
                             */
                            if ((0 != group) &&
                                (pFsAttr->fragGrps[group].lastFreeGrp != groupPage))
                            {
			        status = add_fixed_message(&(pPage->messages),
							   frag,
							   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_304,
								   "Modified fileset %s lastFreeGrp from %d to %d for type %dK.\n"),
							   fileset->fsName,
							   pFsAttr->fragGrps[group].lastFreeGrp,
							   groupPage, group);
			        if (SUCCESS != status) 
			        {
				    return status;
			        }
                                pFsAttr->fragGrps[group].lastFreeGrp = groupPage;
                                attrModified = TRUE;
                            }
                        }
                        else
                        {
                            /*
                             * Set groupOnList for this group so we'll know if
                             * see it more than once.
                             */
                            groupOnList[groupPage/BF_FRAG_GRP_PGS] = TRUE;
                    
                            /*
                             * If we are not at the end of the list, see
                             * if the next group is in a valid frag file page.
                             */
                            nextGrpPage = pFragGrp->nextFreeGrp;
                        
                            status = convert_page_to_lbn(fileset->fragLBN,
                                                         fileset->fragLBNSize,
                                                         nextGrpPage, 
                                                         &nextGrpLBN,
                                                         &nextGrpVol);
                            if (SUCCESS != status)
                            {
		                writemsg(SV_VERBOSE | SV_LOG_FOUND,
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_305, 
						 "nextFreeGrp page %d on fileset %s is out of bounds, set to -1.\n"),
					 nextGrpPage, fileset->fsName);
                                nextGrpLBN = (LBNT)-1;
                            }
                        
                            if (( (nextGrpLBN == (LBNT) -1 ) || (nextGrpLBN == (LBNT) -2 )) ||
                                (FALSE != groupOnList[nextGrpPage/BF_FRAG_GRP_PGS]))
                            {
                                /*
                                 * If the next group is not within the frag
                                 * file or we've already looked at it or it's
                                 * full, then truncate this list.
                                 */
		                status = add_fixed_message(&(pGrpPage->messages), frag,
							   catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_306, 
								   "Modified fileset %s nextFreeGrp from %d to %d.\n"),
							   fileset->fsName, 
							   pFragGrp->nextFreeGrp, -1);
			        if (SUCCESS != status)
			        {
				    return status;
	                        }

                                pFragGrp->nextFreeGrp = -1;
                                modified = TRUE;
                            
                                if (0 != group)
                                {
                                    /*
                                     * Pointer to last group is ignored
                                     * for 0K frags so don't set it unless
                                     * the frag type is 1-7K.
                                     */
			            status = add_fixed_message(&(pPage->messages),
							       frag,
							       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_304,
								       "Modified fileset %s lastFreeGrp from %d to %d for type %dK.\n"),
							       fileset->fsName,
							       pFsAttr->fragGrps[group].lastFreeGrp,
							       groupPage, group);
			            if (SUCCESS != status) 
			            {
				        return status;
			            }
                                    pFsAttr->fragGrps[group].lastFreeGrp = groupPage;
                                    attrModified = TRUE;
                                }
                            } /* end if next group LBN < 0 or next groupUse is not FALSE */
                        } /* end if nextFreeGrp is not -1 */
                    } /* end else frag type doesn't match group */
                    
                    status = release_page_to_cache(groupVol, groupLBN,
                                                   pGrpPage, modified, FALSE);
                    if (SUCCESS != status)
                    {
                        return status;
                    }
                    /*
                     * If we truncated the list and may have to change the
                     * previous page, then see if we really had a previous
                     * page. If so, read it in and change the nextFreeGrp
                     * pointer.
                     */
                    if (TRUE == changePrev)
                    {
                        if (prevPage != groupPage)
                        {
                            status = convert_page_to_lbn(fileset->fragLBN,
                                                         fileset->fragLBNSize,
                                                         prevPage, &groupLBN,
                                                         &groupVol);
                            if (SUCCESS != status)
                            {
                                writemsg(SV_ERR | SV_LOG_ERR,
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_273, 
						 "Can't convert frag page %d to LBN for fileset %s.\n"),
					 prevPage, fileset->fsName);
                                return FAILURE;
                            }
                        
                            /*
                             * Make sure we aren't going to try to read the same
                             * page we currently have open. This would mean that
                             * the BMT and the frag file overlap, so just return
                             * failure in this case.
                             */
                            if ((groupLBN == currLBN) && (groupVol == currVol))
                            {
                                writemsg(SV_ERR | SV_LOG_ERR, 
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_302, 
						 "BMT and frag file for fileset %s overlap at LBN %u.\n"),
					 fileset->fsName, currLBN);
                                corrupt_exit(NULL);
                            }
                    
                            status = read_page_from_cache(groupVol, groupLBN, &pGrpPage);
                            if (SUCCESS != status)
                            {
                                return status;
                            }
                            pFragGrp = (grpHdrT *)pGrpPage->pageBuf;
		            status = add_fixed_message(&(pGrpPage->messages), frag,
						       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_306, 
							       "Modified fileset %s nextFreeGrp from %d to %d.\n"),
						       fileset->fsName, 
						       pFragGrp->nextFreeGrp, -1);
			    if (SUCCESS != status)
			    {
				return status;
	                    }

                            pFragGrp->nextFreeGrp = -1;
                            modified = TRUE;
                    
                            status = release_page_to_cache(groupVol, groupLBN,
                                                       pGrpPage, modified, FALSE);
                            if (SUCCESS != status)
                           {
                                return status;
                            }
                        } /* end if prevPage is not groupPage */
                        changePrev = FALSE;
                        groupPage = -1;
                    }
                    else
                    {
                        prevPage = groupPage;
                        groupPage = pFragGrp->nextFreeGrp;
                    } /* end if we needed to change the previous page */
                    
                    if (TRUE == modified)
                    {
                        fixed = TRUE;
                        modified = FALSE;  
                    }
                    
                } /* end while groupPage is not -1 */
            } /* end for each frag type [0-7] */
                
            /*
             * Now loop through the groupOnList array and see if we have 
             * any groups with free slots which are not on their respective
             * free list. If so, add them to the beginning of the list.
             */
            for (grpNumInFile = 0; grpNumInFile < numGroups; grpNumInFile++)
            {
                if (FALSE == groupOnList[grpNumInFile])
                {
                    /*
                     * Read the group header in and add it to the
                     * start of the free list for the frag type.
                     */
                    pageNum = grpNumInFile * BF_FRAG_GRP_PGS;
                        
                    status = convert_page_to_lbn(fileset->fragLBN, 
                                                 fileset->fragLBNSize,
                                                 pageNum, &groupLBN, &groupVol);
                    if (status != SUCCESS)
                    {
                        writemsg(SV_ERR | SV_LOG_ERR, 
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_273,
					 "Can't convert frag page %d to LBN for fileset %s.\n"),
				 pageNum, fileset->fsName);
                        return FAILURE;
                    }   
        
                    status = read_page_from_cache(groupVol, groupLBN, &pGrpPage);
                    if (status != SUCCESS)
                    {
                        return status;
                    }
         
                    /*
                     * Point to the frag group header.
                     */
                
                    pFragGrp = (grpHdrT *)pGrpPage->pageBuf;
                    
                    group = pFragGrp->fragType;
                    
                    /*
                     * Add this group to the head of the list for it's type.
                     */
		    status = add_fixed_message(&(pPage->messages),
					       frag,
					       catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_307, 
						       "Added group %d to %dK frag free list for fileset %s.\n"),
					       grpNumInFile, group,
					       fileset->fsName);
                    if (SUCCESS != status) 
                    {
                        return status;
                    }
                    pFragGrp->nextFreeGrp = pFsAttr->fragGrps[group].firstFreeGrp;
                    pFsAttr->fragGrps[group].firstFreeGrp = pageNum;
                    if (-1 == pFsAttr->fragGrps[group].lastFreeGrp)
                    {
                        /*
                         * If the last free group pointer is -1, then also
                         * set it to point to this group. In this case, the
                         * group will be the only one on the list.
                         */
                        pFsAttr->fragGrps[group].lastFreeGrp = pageNum;
                    }
                    modified = TRUE;
                    attrModified = TRUE;
                    
                    status = release_page_to_cache(groupVol, groupLBN,
                                                   pGrpPage, modified, FALSE);
                    if (SUCCESS != status)
                    {
                        return status;
                    }
                    if (TRUE == modified)
                    {
                        fixed = TRUE;
                        modified = FALSE;
                    }
                } /* end if groupOnList is FALSE */
            } /* end for each group in the frag file */
            break;
        } /* end while records exist in the fsPmcell */

        status = release_page_to_cache(currVol, currLBN, pPage, attrModified, FALSE);
    
        if (SUCCESS != status)
        {
            return status;
        }
        if (TRUE == attrModified)
        {
            fixed = TRUE;
            attrModified = FALSE;
        }
        /*
         * If we have more mcells in the chain, then follow them if
         * we haven't found the record yet.
         */
        if (nextMcell.vol != 0)
        {
            mcell.vol = nextMcell.vol;
            mcell.page = nextMcell.page;
            mcell.cell = nextMcell.cell;
        }
        else
        {
            mcell.vol = 0;
            mcell.page = 0;
            mcell.cell = 0;
        }
    
    } /* end while current mcell exists */

    free(groupOnList);
    
    /*
     * If we never found the BSR_BFS_ATTR record, then return failure.
     *
     * FUTURE: add the record and see if we can fill in the values later.
     */
    if (FALSE == attrFound)
    {
        writemsg(SV_ERR | SV_LOG_ERR, 
                 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_308,
			 "Fileset attributes were not found for fileset %s.\n"),
                 fileset->fsName);
        return FAILURE;
    }
    
    if (TRUE == fixed)
    {
        return FIXED;
    }
    
    return SUCCESS;
} /* end correct_frag_file */

/*
 * Function Name: set_frag_unused (3.x)
 *
 * Description: This function is used to clear the used bits for a frag
 *              in the frag slot array for the particular fileset. This
 *              should be called only when a frag is going to be deleted,
 *              so we don't care if there are any problems with clearing
 *              the bits for the frag. We also don't care if the bits for
 *              the slot were never set.
 *
 * Input parameters:
 *     fileset: The fileset being worked on.
 *     slot: The slot number of the frag.
 *     size: The size of the frag.
 *
 * Output parameters: N/A
 *
 * Returns: N/A
 * 
 */
void set_frag_unused(filesetT *fileset, int slot, int size) 
{
    char *funcName = "set_frag_unused";
    unsigned long (*slotArray)[3];
    unsigned long startMask;
    unsigned long endMask;
    int startIndex;
    int endIndex;
    int startOffset;
    int endOffset;
    int maxSlotNum;
    int lastPage;
    
    slotArray = fileset->fragSlotArray;
    
    if (NULL == slotArray)
    {
        return;
    }

    /*
     * Make sure the slot fits within the limits of the slot array.
     * If not, then the frag record or frag file is probably corrupt 
     * and the frag record will be deleted when FAILURE is returned.
     */
    lastPage = fileset->fragLBN[fileset->fragLBNSize].page;
    if (FS_IS_CLONE(fileset->status))
    {
        /*
         * If this is a clone, use the size of the original fileset
         * frag file if it is larger because we can get frags in the 
         * clone pointing into the original.
         */
        if (lastPage < fileset->origClonePtr->fragLBN[fileset->origClonePtr->fragLBNSize].page)
        {
            lastPage = fileset->origClonePtr->fragLBN[fileset->origClonePtr->fragLBNSize].page;
        }

    }
    maxSlotNum = lastPage*8;
    if ((slot < 0) || (slot + size > maxSlotNum))
    {
        return;
    }
    
    /*
     * Get the index and offset for this frag in the slot array.
     */
    startIndex = slot / BITS_PER_LONG;
    endIndex = (slot + size - 1) / BITS_PER_LONG;
    startOffset = slot % BITS_PER_LONG;
    endOffset = (slot + size - 1) % BITS_PER_LONG;
    endMask = 0;
            
    /*
     * Set up the masks needed for this frag.
     */
    if (startIndex == endIndex)
    {
        startMask = ((1L << size) - 1) << startOffset;
    }
    else
    {
        startMask = ((1L << (BITS_PER_LONG - startOffset)) - 1) << startOffset;
        endMask = (1L << (endOffset + 1)) - 1;
    }
            
    /*
     * Clear the used bits for this frag. We don't bother with the
     * overlap bits since we don't use them again.
     */
    slotArray[startIndex][FRAG_USED] = slotArray[startIndex][FRAG_USED] 
                                       & ~startMask;
    if (startIndex != endIndex)
    {
        slotArray[endIndex][FRAG_USED] = slotArray[endIndex][FRAG_USED] 
                                         & ~endMask;
    }
    return;
            
} /* end set_frag_unused */
/* end fixfdmn_frag.c */
