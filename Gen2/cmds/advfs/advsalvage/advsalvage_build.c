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

#ifndef lint
#endif

#include "advsalvage.h"

/*
 * Global
 */

/*
 * Private protos
 */


/*
 * Function Name: build_tree
 *
 * Description:
 *  This function is what is referred to as PASS 1 of salvage. It
 *  attempts to read the BMT and load as much information about the
 *  filesystem into our two main structures, the fileset tag pointer
 *  array and the fileset directory tree.
 *
 * Input parameters:
 *  domain: The domain we are working on. 
 *  fileset: The fileset which we need to build the tree for. 
 *
 * Returns: 
 * Status value - SUCCESS, FAILURE or NO_MEMORY.
 */
int build_tree(domainInfoT *domain,
	       filesetLLT  *fileset)
{
    char             *funcName = "build_tree";
    extentT          *fsTagFileBuffer;
    int              status;
    int64_t          numExtents;
    int64_t          extent;
    bs_meta_page_t   page;
    int              counter;
    tagNumT          tagCounter  = 0;
    bs_meta_page_t   pageCounter = 0;
    tagNumT          parentTagNum;
    bs_meta_page_t   maxPages;
    tagNumT          maxTags;
    int64_t          bytesRead;
    filesetTreeNodeT *parent;
    filesetTreeNodeT *child;
    filesetTreeNodeT *headLostFound;
    void             *pData;

    /* 
     * We have four cases here:
     * 1) Recover all files
     * 2) Recover a specific file or directory
     * 3) Recover a 'tag' (which could be a file or a directory)
     * 4) Recover only changed files 
     */

    /*
     * This call malloc's the fsTagFile Buffer - Make sure to free it 
     * This call also sets up the tag array 
     */

    status = setup_fileset(domain, fileset, &fsTagFileBuffer, &numExtents);
    if (SUCCESS != status)
    {
        return status;
    }

    /* 
     * Setup of top of tree, This includes tag 2 and lost_found.
     */
    status = setup_fs_tree(fileset);
    if (SUCCESS != status)
    {
        free(fsTagFileBuffer);
        return status;
    }

    status = setup_tag_2(fileset, fsTagFileBuffer);
    if (SUCCESS != status)
    {
        if (NO_MEMORY == status)
	{
	    return status;
	}

        if (NULL != Options.pathName) 
        {
            writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_51, 
	             "Recovery of '%s' by specific pathname not possible. %s\n%s%s"),
		     fileset->fsName, fileset->fsName,
		     catgets(mcat, S_SALVAGE_1, SALVAGE_52, 
		     " may still be recoverable in its entirety. If this is desired,\n"),
		     catgets(mcat, S_SALVAGE_1, SALVAGE_53, 
                     " reissue command without the specific pathname option.\n"));
            free(fsTagFileBuffer);
            return FAILURE;
        }
	else
        {
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_54, 
		     "Setting search by volume - setup_tag_2 failed\n"));
            FS_SET_PASS2_NEEDED(fileset->status);
        }
    }

    /*
     * Set headLostFound.
     */
    headLostFound = fileset->dirTreeHead->nextSibling;

    /*
     * If pathname was specified, do it. Specified pathnames vs specified
     * tag numbers are handled differently, and special case where tag = 2,
     * we treat this as "restore whole fileset". In this case, clear the
     * option string so as not to confuse subsequent functions.
     */
    if (NULL != Options.pathName) 
    {
        if (strncmp(Options.pathName, ".tags/", 6) == 0)
        {
            if (strcmp(Options.pathName, ".tags/2") == 0)
            {
                free(Options.pathName);
                Options.pathName = NULL;
            }
            else
            {
                /*
                 * For array size, use "numExtents - 1", since in this array,
                 * the "lbn = -1" terminator array entry is included in 
                 * numExtents, unlike the bmt lbn arrays. This will make
                 * the convert_page_to_lbn() function work correctly.
                 */
                status = trim_requested_tagnum(fileset, fsTagFileBuffer,
                                                numExtents - 1);
                if (SUCCESS != status) 
                {
                    free(fsTagFileBuffer);  
                    return status;
                }

                if (D_IS_PASS1_COMPLETED(fileset->domain->status))
                {
                    free(fsTagFileBuffer);
                    return SUCCESS;
                }
             }
        }
        else
        {
	    status = trim_requested_pathname(fileset, fsTagFileBuffer);
            if (SUCCESS != status)
            {
                free(fsTagFileBuffer);
                return status;
            }
        }
    }

    assert((fsTagFileBuffer[numExtents - 1].fob % ADVFS_METADATA_PGSZ_IN_FOBS) == 0);
    maxPages = fsTagFileBuffer[numExtents - 1].fob / ADVFS_METADATA_PGSZ_IN_FOBS;
    maxTags  = maxPages * BS_TD_TAGS_PG;

    /*
     * Loop through all the tags in this fileset.
     * 
     * This is done by looping through all extents, in the fileset tag file.
     * then looping through all pages in each extent,
     * then looping through all the tags per page.
     */
    for (extent = 0; extent < numExtents; extent++) 
    {
        bs_meta_page_t numPage;
	int fd;
	bf_vd_blk_t lbn;
	bf_vd_blk_t extentLbn;

	/*
	 * Compute number of pages in this extent.
	 */
        assert(((fsTagFileBuffer[extent + 1].fob-fsTagFileBuffer[extent].fob) %
                ADVFS_METADATA_PGSZ_IN_FOBS) == 0);
	numPage = (fsTagFileBuffer[extent + 1].fob -
		   fsTagFileBuffer[extent].fob) / ADVFS_METADATA_PGSZ_IN_FOBS;

	fd  = domain->volumes[fsTagFileBuffer[extent].volume].volFd;
	extentLbn = fsTagFileBuffer[extent].lbn;

        if (extentLbn == -1) {
            continue;
        }

	/*
	 * for each extent loop through all pages.
	 */
        for (page = 0 ; page < numPage; page++, pageCounter++) 
	{
	    bsTDirPgT     tagPage;
	  
	    lbn = extentLbn + (page * BLOCKS_PER_PAGE);

	    /* 
	     * load tag page into tagPage buffer.
	     */
	    status = read_page_by_lbn(fd, &tagPage, lbn, &bytesRead);
	    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
	    {
	        return FAILURE;
	    }

	    /*
	     * Tag page not initialized, or Page with no alloced tags
	     */
	    if ((pageCounter != tagPage.tpPgHdr.currPage) ||
		(0 == tagPage.tpPgHdr.numAllocTMaps))
	    {
	        /*
		 * Increment tag counter by number of tags on a page
		 */
	        tagCounter += BS_TD_TAGS_PG;
		continue;
	    }

	    /*
	     * Error checking on tag page.
	     */
	    if ((-1 == tagPage.tpPgHdr.currPage)     ||
		(-1 == tagPage.tpPgHdr.nextFreePage) ||
		(-1 == tagPage.tpPgHdr.nextFreeMap)  ||
		(tagPage.tpPgHdr.numAllocTMaps > BS_TD_TAGS_PG)||
		(-1 == tagPage.tpPgHdr.numAllocTMaps)||
		(-1 == tagPage.tpPgHdr.rsvd1) ||
		(-1 == tagPage.tpPgHdr.rsvd2))
	    {
	        /*
		 * Increment tag counter by number of tags on a page
		 */
	        tagCounter += BS_TD_TAGS_PG;
		FS_SET_PASS2_NEEDED(fileset->status);
		writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_55, 
			 "Corrupt tag page %ld in '%s', skipping it\n"),
			 pageCounter, fileset->fsName);
		writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_56, 
                         "Setting search by volume\n"));
		continue;
	    }

	    tagCounter = tagPage.tpPgHdr.currPage * BS_TD_TAGS_PG;
	    /*
	     * Check to see if we need to enlarge the tag array.
	     */
	    if (tagCounter >= fileset->tagArraySize)
	    {
		status = enlarge_tag_array(fileset, tagCounter);
		if (SUCCESS != status)
		{
		    if (FAILURE != status)
		    {
		      return status;
		    }
		    continue;
		}
	    }

	    /*
	     * Loop through all tags on each page.
	     */
	    for (counter = 0;
		 counter < BS_TD_TAGS_PG; 
		 counter++, tagCounter++)
	    {
	        filesetTreeNodeT *treeNode;
		bfMCIdT          current;

		if (0 != Options.progressReport) {
		    if ((tagCounter % 1000) == 0) {
			writemsg(SV_ALL | SV_CONT, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_280,
					 "Checking tag %ld out of a maximum of %ld, on '%s'.\r"), 
				 tagCounter, maxTags, fileset->fsName);
		    }
		}

		/*
		 * Want to start with tag 3, tags 1 & 2 (unused and
                 * fileset root) already done. Also, bypass anything marked
                 * IGNORE or DEAD (this can occur if specific pathname request
		 * has trimmed tree at top level).
		 */
		if (tagCounter < 3 || fileset->tagArray[tagCounter] != NULL)
		{
		    continue;
		}
	    
		if (!(tagPage.tMapA[counter].tm_u.tm_s3.flags & BS_TD_IN_USE))
		{
		    continue;
		}

		current.volume = tagPage.tMapA[counter].tm_u.tm_s3.bfMCId.volume;
		current.page   = tagPage.tMapA[counter].tm_u.tm_s3.bfMCId.page;
		current.cell   = tagPage.tMapA[counter].tm_u.tm_s3.bfMCId.cell;

		/*
		 * Do error checking on mcell.
		 */
		if  ((current.cell >= BSPG_CELLS) ||
		     (current.cell < 0) ||
		     (current.volume < 1) ||
		     (current.volume > MAX_VOLUMES) ||
		     (TRUE == domain->volumes[current.volume].badVolNum))
		{
		    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, 
			     SALVAGE_57, 
			     "Setting search by volume - error in mcell\n"));
		    FS_SET_PASS2_NEEDED(fileset->status);
		    continue;
		}

		/* 
		 * Create and load an element in our array 
		 */
		status = load_tag_from_mcell(domain, fileset, tagCounter, 
					     current, &parentTagNum);
		if (FAILURE == status)
		{
		    /*
		     * Failed to read tag, set to DEAD.
		     */
		    fileset->tagArray[tagCounter] = DEAD;
		    continue;
		} 
		else if (PARTIAL == status)
		{
		    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, 
			     SALVAGE_58, 
			     "Setting search by volume - Partial\n"));
		    FS_SET_PASS2_NEEDED(fileset->status);
		}
		else if (NO_MEMORY == status)
		{
		    return NO_MEMORY;
		}

		/*
		 * Does this tag exist, and not have a first instance.
		 */
		if (fileset->tagArray[tagCounter] == NULL || 
		    fileset->tagArray[tagCounter] == IGNORE || 
		    fileset->tagArray[tagCounter] == DEAD)
		{
		    continue;
		}

		status = add_first_instance(fileset, tagCounter, parentTagNum);
		if (SUCCESS != status)
		{
		    return status;
		}
	    } /* end for each tag */
	} /* end for each page*/
    } /* end for each extent*/

    if (0 != Options.progressReport) {
	/*
	 * Add a line feed to the progress messages.
	 */
	writemsg(SV_ALL | SV_CONT, "\n");
    }

    status = relink_lost_found(fileset);
    if (SUCCESS != status)
    {
        free(fsTagFileBuffer);
        return status;
    }

    free(fsTagFileBuffer);
    return SUCCESS;
} /* end build_tree */


/*
 * Function Name: p2_build_tree
 *
 * Description:
 *  This is the main function of what has been called pass 2,
 *  the main difference betweeb PASS1 and PASS2 is that we do
 *  not follow the mcell chains.  Instead we check every known
 *  mcell one at a time.
 *
 * Input parameters:
 *  domain: The domain we are working on. 
 *  volId:  The volumes which we need to build the tree for. 
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE or NO_MEMORY 
 */
int p2_build_tree (domainInfoT *domain,
		   int         volId)
{
    char             *funcName = "p2_build_tree";
    int              status;
    int              mcell;
    int              x;
    int64_t          extent;
    int64_t          numExtents;
    tagNumT          parentTagNum;
    int              fd;
    int64_t          bmtMagic;
    bs_meta_page_t   maxPages;
    int64_t          maxMcells;
    int64_t          mcellCounter;
    int64_t          bytesRead;
    bsMPgT           BMTbuffer;
    bsMPgT           mcellBuffer;
    bsMCT            *pMcell;
    bsMRT            *pRecord;
    bsXtntRT         *pXtnt;
    bsXtraXtntRT     *pXtraData;
    extentT          *extentArray = NULL;
    filesetLLT       *fileset;
    filesetTreeNodeT *treeNode;
    filesetTreeNodeT *parent;
    filesetTreeNodeT *child;
    filesetTreeNodeT *headLostFound;

    /*
     * Initalization
     */
    numExtents  = 0;
    extent      = 0;
    fd          = domain->volumes[volId].volFd; 
    bmtMagic    = 6;
    mcellCounter = 0;

    /* 
     * Read the first BMT page from the disk, from the known location 
     */
    status = read_page_by_lbn(fd, &BMTbuffer,
			      domain->volumes[volId].rbmtArray[0].lbn,
			      &bytesRead);
    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
    {
        /* 
	 * Function only returns FAILURE on read or lseek errors 
	 */
        return FAILURE;
    }

    /*
     * loop through all mcell in BMT.
     */
    for (mcell = 0; mcell < BSPG_CELLS; mcell++)
    {
        /*
	 * point to the current BMT mcell.
	 */
        pMcell = &(BMTbuffer.bsMCA[mcell]);

	/*
	 * Check to see if this mcell belongs to BMT (ie 0 == -tag % bmtMagic)
	 */

	if ((pMcell->mcTag.tag_num != 0) &&
	    (0 == -(pMcell->mcTag.tag_num) % bmtMagic))
	{
	    /* 
	     * Found a valid BMT mcell, point to the first record.
	     */
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    
	    /*
	     * Now loop through all records in this mcell.
	     */
	    while ((pRecord->type != 0) &&
		   (pRecord->bCnt != 0) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	    {
	        switch (pRecord->type) 
		{
		  case (BSR_XTNTS):
		    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    numExtents += pXtnt->xCnt - 1;
		    break;

		  case (BSR_XTRA_XTNTS): 
		    pXtraData = (bsXtraXtntRT *)((char *)pRecord + 
						 sizeof(bsMRT));
		    numExtents += pXtraData->xCnt - 1;
		    break;

		  default:
		    /*
		     * A record type we don't care about.
		     */
		    break;
		}
		/*
		 * Point to the next record.
		 */
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(uint64_t)));
	    } /* end while */
	} 
    } /* end for loop */

    if (0 == numExtents)
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_249, 
				 "No BMT extents found\n"));
        return FAILURE;
    }

    /* 
     * Calloc space for extentArray - Need numExtent 
     */
    extentArray = (extentT *) salvage_calloc(numExtents + 1, sizeof(extentT));
    if (NULL == extentArray) 
    {
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_59, 
		 "calloc() failed\n"));
	return NO_MEMORY;
    }

    /*
     * Loop through all the mcells a second time, to load extent Array.
     */
    for (mcell = 0; mcell < BSPG_CELLS; mcell++)
    {
	/*
	 * point to the current BMT mcell.
	 */
	pMcell = &(BMTbuffer.bsMCA[mcell]);

	/*
	 * Check to see if this mcell belongs to BMT (ie 0 == -tag % bmtMagic)
	 */
	if ((0 != pMcell->mcTag.tag_num) && 
	    (0 == -(pMcell->mcTag.tag_num) % bmtMagic))
	{
	    /* 
	     * Found a valid BMT mcell, point to it.
	     */
	    pRecord = (bsMRT *)pMcell->bsMR0;
	    
	    /*
	     * Now loop through all records in this mcell.
	     */
	    while ((pRecord->type != 0) &&
		   (pRecord->bCnt != 0) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	    {
		switch(pRecord->type)
		{
		    case(BSR_XTNTS):
		      /* 
		       * Set up data buffer 
		       */
		      pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    
		      /* 
		       * For each extent record in this buffer process 
		       * the data 
		       */
		      for (x = 0; 
			   ((x < pXtnt->xCnt) && (x < BMT_XTNTS));
			   x++) 
		      {
			  extentArray[extent].fob = 
			    pXtnt->bsXA[x].bsx_fob_offset;
		    
                          if (pXtnt->bsXA[x].bsx_vd_blk != XTNT_TERM &&
                              pXtnt->bsXA[x].bsx_vd_blk != PERM_HOLE_START)
			  {
			      extentArray[extent].lbn =
				pXtnt->bsXA[x].bsx_vd_blk;
			      extent++;
			  }
			  else 
			  {
			      extentArray[extent].lbn = -1;
			      /*
			       * The only entry which should be -1, is the last
			       * entry in each record.  We need to save it in 
			       * case the next record is bad.  Otherwise we 
			       * overwrite it.  So DON'T increment the extent.
			       */
			  }
		      } /* end for loop */
		      break;

		    case(BSR_XTRA_XTNTS):
		      /* 
		       * Set up data buffer 
		       */
		      pXtraData = (bsXtraXtntRT *)((char *)pRecord + 
						   sizeof(bsMRT));
		      /* 
		       * For each extent record in this buffer process 
		       * the data 
		       */
		      for (x = 0; 
			   ((x < pXtraData->xCnt) && (x < BMT_XTRA_XTNTS));
			   x++) 
		      {
			  extentArray[extent].fob = pXtraData->bsXA[x].bsx_fob_offset;
			  
                          if (pXtraData->bsXA[x].bsx_vd_blk != XTNT_TERM &&
                              pXtraData->bsXA[x].bsx_vd_blk != PERM_HOLE_START)
			  {
			      extentArray[extent].lbn = 
				pXtraData->bsXA[x].bsx_vd_blk;
			      extent++;
			  }
			  else 
			  {
			      extentArray[extent].lbn = -1;
			      /*
			       * The only entry which should be -1, is the last
			       * entry in each record.  We need to save it in 
			       * case the next record is bad.  Otherwise we 
			       * overwrite it.  So DON'T increment the extent.
			       */
			  }
		      } /* end for loop */
		      break;

		    default:
		      /*
		       * A record type we don't care about.
		       */
		      break;
		}
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(uint64_t)));
 	    } /* end while */
	} 
    } /* end for loop */


    assert((extentArray[numExtents].fob % ADVFS_METADATA_PGSZ_IN_FOBS) == 0);
    maxPages = extentArray[numExtents].fob / ADVFS_METADATA_PGSZ_IN_FOBS;
    maxMcells = maxPages * BSPG_CELLS;

    /*
     * Loop through all the extent records we just collected.
     */
    for (extent = 0; extent <= numExtents; extent++)
    {
        bs_meta_page_t page;
	bf_vd_blk_t lbn;
        
	/*
	 * Verify that extent is not a hole.
	 */
        if (-1 == extentArray[extent].lbn)
	{
	    continue;
	}

	/*
	 * Loop through all the pages in the extent.
	 */
        assert ((extentArray[extent].fob % ADVFS_METADATA_PGSZ_IN_FOBS) ==0 );
	for (page = extentArray[extent].fob / ADVFS_METADATA_PGSZ_IN_FOBS, 
             lbn  = extentArray[extent].lbn;
	     page < (extentArray[extent + 1].fob / ADVFS_METADATA_PGSZ_IN_FOBS);
	     page++, lbn += BLOCKS_PER_PAGE )
	{
	    int freeMcell[BSPG_CELLS];
	    int nextFree;

	    /* 
	     * load new page into mcellPage buffer.
	     */
	    status = read_page_by_lbn(fd, &mcellBuffer, lbn, &bytesRead);
	    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
	    {
		writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_60, 
			 "Skipping current page, read failed\n"));
		continue;
	    }

	    /*
	     * Simple Check to see if a valid BMT page. 
	     */
	    if ((mcellBuffer.bmtFsCookie.id_sec != domain->fsCookie.id_sec) ||
	        (mcellBuffer.bmtFsCookie.id_usec != domain->fsCookie.id_usec))
	    {
		continue;
	    }

	    /*
	     * Detailed Check if valid BMT page.
	     */
	    if (FALSE == validate_page(mcellBuffer))
	    {
		continue;
	    }

	    /*
	     * Initialize array of mcells to to ignore (ie free mcells).
	     */
	    for (mcell = 0; mcell < BSPG_CELLS; mcell++)
	    {
		freeMcell[mcell] = FALSE;
		pMcell = &(mcellBuffer.bsMCA[mcell]);
		
		/* The mcell is free if the header tag and set tag are 0. */
		if (0 == pMcell->mcBfSetTag.tag_num && 0 == pMcell->mcTag.tag_num)
		{
		    freeMcell[mcell] = TRUE;
		}
	    }

	    /*
	     * If we decide to handle deferred mcells, this would be the place.
	     */

	    /* 
	     * Loop through all mcells on this page.
	     */
	    for (mcell = 0; mcell < BSPG_CELLS; mcell++)
	    {
		int              found;
		tagNumT          tag;
		int              seq;
		tagNumT          setTag;
		int              setSeq;

		if (0 != Options.progressReport) {
		    if ((mcellCounter % 1000) == 0) {
			writemsg(SV_ALL | SV_CONT, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_281,
					 "Checking mcell %d out of %d, on volume %s.\r"),
				 mcellCounter, maxMcells,
				 domain->volumes[volId].volName);
		    }
		}
		mcellCounter++;

		if (TRUE == freeMcell[mcell])
		{
		    continue;
		}
     
		pMcell = &(mcellBuffer.bsMCA[mcell]);

		tag    = pMcell->mcTag.tag_num;
		seq    = pMcell->mcTag.tag_seq;
		setTag = pMcell->mcBfSetTag.tag_num;
		setSeq = pMcell->mcBfSetTag.tag_seq;

		if ((1 > tag) || (0 == setTag) || (0 >= seq) || (0 >= setSeq))
		{
		    /*
		     * Unused mcell
		     */
		    continue;
		}

		if (0 > setTag)
		{
		    /*
		     * Reserved mcell skip
		     */
		    continue;
		}

		/*
		 * Need to find which fileset this tag belongs to.
		 */
		status = find_fileset(domain, tag, setTag, &fileset);
		if (SUCCESS != status)
		{
		    if (INVALID == status)
		    {
			/*
			 * setTag belongs to a fileset we are not recovering.
			 */
			continue;
		    }
		    else
		    {
			return status;
		    }
		}

		/*
		 * Check to see if we need to enlarge the tag array.
		 */
		if (tag >= fileset->tagArraySize)
		{
		    status = enlarge_tag_array(fileset, tag);
		    if (SUCCESS != status)
		    {
		        if (FAILURE != status)
			{
			    return status;
			}
			continue;
		    }
		}

		/*
		 * Check to see if this is a IGNORE/DEAD tag.
		 */
		if ((IGNORE == fileset->tagArray[tag]) ||
		    (DEAD == fileset->tagArray[tag]))
		{
		    continue;
		}

		/* 
		 * Create and load an element in our array 
		 */
		status = p2_load_tag_from_mcell(domain, fileset, tag, 
						pMcell, &parentTagNum,
						volId);
		if (FAILURE == status)
		{
		    /*
		     * Failed to read tag, set to DEAD.
		     */
		    fileset->tagArray[tag] = DEAD;
		    continue;
		} 
		else if (NO_MEMORY == status)
		{
		    return status;
		}
		
		/*
		 * Does this tag still exist.
		 */
		if (fileset->tagArray[tag] == NULL || 
		    fileset->tagArray[tag] == IGNORE || 
		    fileset->tagArray[tag] == DEAD)
		{
		    continue;
		}

		/*
		 * Does the first instance of a tag exist?
		 */
		if (NULL == fileset->tagArray[tag]->firstInstance)
		{
		    status = add_first_instance(fileset, tag, parentTagNum);
		    if (SUCCESS != status)
		    {
		        return status;
		    }
		} /* end if first instance*/
		else
		{
		    /*
		     * Node exists.
		     */
		    if (parentTagNum != MISSING_PARENT) 
		    {
		      fileset->tagArray[tag]->firstInstance->parentTagNum = 
			parentTagNum;
		    }
		} /* end else NOT first instance. */
	    } /* end mcell for loop*/
	} /* end page for loop*/
    } /* end extent for loop*/

    if (0 != Options.progressReport) {
	/*
	 * Add a line feed to the progress messages.
	 */
	writemsg(SV_ALL | SV_CONT, "\n");
    }

    free(extentArray);
    return SUCCESS;
} /* end p2_build_tree */


/*
 * Function Name: p3_build_tree
 *
 * Description:
 *  This is the main function of what has been called pass 3.  
 *  This pass searches the entire disk looking for what it
 *  believes are BMT pages.  It will perform a series of tests
 *  to increase the odds that the page in question is actually a
 *  BMT page.  Then it parses the page, NOT following links and
 *  adds the data into the tree.
 *
 * Input parameters:
 *  domain: The domain we are working on. 
 *  volId: The volumes which we need to build the tree for. 
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE or NO_MEMORY.
 */
int p3_build_tree (domainInfoT *domain,
		   int         volId)
{
    char        *funcName = "p3_build_tree";
    int         status;
    bsMPgT      BMTbuffer;
    bs_meta_page_t logicalPageNumber;
    int         mcell;
    int         fd;
    bsMCT       *pMcell;
    bsMRT       *pRecord;
    filesetLLT  *fileset;
    int64_t     bytesRead;

    /*
     * Initalization
     */
    logicalPageNumber = 0;
    fd                = domain->volumes[volId].volFd; 

    /*
     * Loop through all pages from the volume.
     */
    status = read_page_by_lbn(fd, &BMTbuffer, logicalPageNumber, &bytesRead);

    while ((SUCCESS == status) && (bytesRead == ADVFS_METADATA_PGSZ))
    {
	if (0 != Options.progressReport) {
	    if ((logicalPageNumber % 1000) == 0) {
		writemsg(SV_ALL | SV_CONT,
			 catgets(mcat, S_SALVAGE_1, SALVAGE_282,
				 "Searching page %ld on volume %s.\r"),
			 logicalPageNumber, 
			 domain->volumes[volId].volName);
	    }
	}

	/*
	 * Simple check, weed out a large number of non-BMT pages,
	 * by checking if the page has the correct fsCookie. 
	 */
	if ((BMTbuffer.bmtFsCookie.id_sec == domain->fsCookie.id_sec) &&
	    (BMTbuffer.bmtFsCookie.id_usec == domain->fsCookie.id_usec))
	{
	    if (TRUE == validate_page(BMTbuffer))
	    {
		/* 
		 * We have found what we believe to be a Full page of 
		 * mcells that need to be processed.
		 */
		status = p3_process_valid_page(domain, &BMTbuffer, volId);
		if (SUCCESS != status)
		{
		    if (INVALID == status)
		    {
			writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, 
                                 SALVAGE_61, 
                                 "Unable to process a valid page\n"));
			continue;
		    }
		    else
		    {
			return status;
		    }
		}
	    }
	}

	/*
	 * Read the next page from the volume.
	 */
        logicalPageNumber++;
        status = read_page_by_lbn(fd, &BMTbuffer, 
				  logicalPageNumber * BLOCKS_PER_PAGE, 
				  &bytesRead);
    }

    if (0 != Options.progressReport) {
	/*
	 * Add a line feed to the progress messages.
	 */
	writemsg(SV_ALL | SV_CONT, "\n");
    }

    return SUCCESS;
} /* p3_build_tree */


/*
 * Function Name: load_tag_from_mcell
 *
 * Description:
 *  This function is given a tag number and a mcell, it then creates
 *  the required structures and loads them with data from the mcell.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  fileset: The fileset this tag belongs to. 
 *  tagNumber: The tag we are loading. 
 *  tagMcell: The primary mcell of this tag. 
 *
 * Output parameters:
 *  parentTag: The parent tag number of this tag.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, NO_MEMORY or PARTIAL.
 */
int load_tag_from_mcell(domainInfoT *domain,
			filesetLLT  *fileset,
			tagNumT     tagNumber,
			bfMCIdT     tagMcell,
			tagNumT     *parentTag)
{
    char         *funcName = "load_tag_from_mcell";
    filesetTagT  *currentTag;
    bfMCIdT      currentMcell;
    int          recordType;
    void         *pData;
    bsMPgT       mcellPage;
    bsMCT        *pMcell;
    bsMRT        *pRecord;
    int          size;
    bf_vd_blk_t  lbn;
    int          fd;
    int64_t      bytesRead;
    int          status;

    *parentTag = MISSING_PARENT;

    currentMcell.volume = tagMcell.volume;
    currentMcell.page   = tagMcell.page;
    currentMcell.cell   = tagMcell.cell;
    
    /*
     * Get the LBN and FD of the page which contains the primary mcell
     */
    status = bmt_page_to_lbn(domain, currentMcell.volume, currentMcell.page, 
			     &lbn, &fd);
    if (FAILURE == status)
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_62, 
                 "%s : Cannot convert BMT volume %d page %ld to lbn\n"),
		 funcName, currentMcell.volume, currentMcell.page);
	return FAILURE;
    }
    else if (INVALID == status)
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_63, 
                 "Cannot convert BMT page to lbn - Invalid\n"));
        return FAILURE;
    }

    /* 
     * load primary mcell page into mcellPage buffer.
     */
    status = read_page_by_lbn(fd, &mcellPage, lbn, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
    {
        return FAILURE;
    }

    /* 
     * Locate the primary mcell we want in mcellPage. 
     */
    pMcell = &(mcellPage.bsMCA[currentMcell.cell]);

    /*
     * Check to see if this tag is in use.
     */
    if (pMcell->mcTag.tag_seq == 0)
    {
	/*
	 * Tag not in use, not a success or a failure.
	 */
	return PARTIAL;
    }

    /* 
     * Check if tag does not exist in Array.
     */
    if (NULL == fileset->tagArray[tagNumber]) 
    {
        /*
	 * Create an element in the tag Array 
	 */
        status = create_tag_array_element(&(fileset->tagArray[tagNumber]));
        if (SUCCESS != status)
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_64, 
                     "Failed to create tag %ld in '%s'\n"),
		     tagNumber, fileset->fsName);
	    return status;
	}
    }
    currentTag = fileset->tagArray[tagNumber];

    /*
     * Set the sequence number for this tag.  If the tag.tag_num or tag.seq
     * changes in the chain we have a bad chain.
     */
    currentTag->seqNum = pMcell->mcTag.tag_seq;

    /*
     * Loop through the mcell chain.
     */
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
	    pData = (char*)pRecord + sizeof(bsMRT);

	    /* 
	     * Read records for this mcell:
	     *
	     * At this point we should only be reading records which deal with
	     * Individual files.  If we hit records which should not be part
	     * of a file, the we have data corruption. 
	     *
	     * Valid Types for FILES : 1,2,16,19,20,249,250,251,252,254,255
	     * Invalid types for FILES : 3,4,7-15,17,18,251
	     *
	     * SPECIAL Types for FILES : 5,6 
	     * We should only get record types 5,6 if we follow the chain
	     * from record 1. We should never run into them here.         
	     */
	    switch (pRecord->type) 
	    {
	        case BSR_XTNTS:
	            /* 
		     * If this record has a chain follow it.
		     */

                    status = add_extents_to_tag(domain, currentTag, 
                                                pData, currentMcell.volume);

		    if (SUCCESS != status)
		    {
			if (NO_MEMORY == status)
			{
			    return status;
			}
			writemsg(SV_DEBUG, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_65, 
                                 "Failed to add extent to tag %ld in '%s'\n"),
				 tagNumber, fileset->fsName);
		    }

		    S_SET_PARTIAL(currentTag->status);
		    break;

		case BSR_ATTR:
		case BMTR_FS_TIME:
		case BSR_BF_INHERIT_ATTR:
		case BMTR_FS_DIR_INDEX_FILE:
		    /*
		     * We are IGNORING these record types.
		     */
		    break;
		case BMTR_FS_INDEX_FILE:
		    /*
		     * This is an 'invisible' file which we do not
		     * want to collect information about.  So free
		     * the node and mark IGNORE
		     */
		    delete_tag_array_entry(fileset,tagNumber);
		    return SUCCESS;
		    break;

		case BSR_ACL_REC:
                    /*
                     * Create or add to the tag's ACL buffer
                     */
		    status = add_tag_acl_data(currentTag, pData);
                    if (status == NO_SPACE) {
			writemsg(SV_DEBUG, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_78, 
                                 "Too many ACLs found for tag %ld\n"),
				 tagNumber);
                    }
                    break;

		case BMTR_FS_UNDEL_DIR:
		    /*
		     * This file has a trashcan.
		     */
		    S_SET_TRASHCAN(currentTag->status);
		    break;

		case BMTR_FS_STAT:
		    *parentTag = ((statT *) pData)->dir_tag.tag_num;

		    status = create_tag_attr(&currentTag->attrs, pData);

		    if (SUCCESS != status) 
		    {
		        if (INVALID == status) 
			{
			    writemsg(SV_DEBUG, 
				     catgets(mcat, S_SALVAGE_1, SALVAGE_67,
				     "Attributes record corrupt for tag %ld in '%s'\n"),
				     tagNumber, fileset->fsName);
			    writemsg(SV_DEBUG, 
				     catgets(mcat, S_SALVAGE_1, SALVAGE_56,
				     "Setting search by volume\n"));
			    FS_SET_PASS2_NEEDED(fileset->status);
			} 
			else 
			{
			    return status;
			}
		    }

		    switch (((statT *) pData)->st_mode & S_IFMT) 
		    {
		        case S_IFREG:
		            currentTag->fileType = FT_REGULAR;
			    break;
			case S_IFDIR:
			    currentTag->fileType = FT_DIRECTORY;
			    break;
			case S_IFBLK:
			case S_IFCHR:
			    currentTag->fileType = FT_SPECIAL;
			    
			    if (NULL == currentTag->addAttrs)
			    {
				dev_t    *tmpDev;

				tmpDev = (dev_t *) salvage_malloc(sizeof(dev_t));
				if (NULL == tmpDev) 
				{
				    writemsg(SV_ERR, catgets(mcat, 
                                             S_SALVAGE_1, SALVAGE_68, 
                                             "malloc() failed\n"));
				    return NO_MEMORY;
				}

				*tmpDev = ((statT *)pData)->st_rdev;
				currentTag->addAttrs = tmpDev;
			    }

			    break;
			case S_IFIFO:
			    currentTag->fileType = FT_FIFO;
			    break;

			case S_IFLNK:
			    currentTag->fileType = FT_SYMLINK;
			    currentTag->attrs->size = 0;
			    break;
			
			case S_IFSOCK:
			    /*
			     * Don't save sockets - delete the entry and
			     * mark as ignore.
			     */
			    delete_tag_array_entry(fileset,tagNumber);
		            return SUCCESS;
		            break;

			default:
			    currentTag->fileType = FT_UNKNOWN;
			    break;
		    } /* end switch */

		    if (currentTag->bytesFound == currentTag->attrs->size) 
		    {
		        S_SET_COMPLETE(currentTag->status);
		    } 
		    else if (currentTag->bytesFound < currentTag->attrs->size) 
		    {
		        /*
			 * Found a file which has less bytes than its size.
			 */
		        writemsg(SV_DEBUG, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_70, 
				 "%s tag %ld - size does not match %d != %d\n"),
				 fileset->fsName, tagNumber, 
				 currentTag->bytesFound, 
				 currentTag->attrs->size);
			writemsg(SV_DEBUG, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_56, 
				 "Setting search by volume\n"));
			FS_SET_PASS2_NEEDED(fileset->status);
		    } 
		    else if (currentTag->bytesFound > 
			     (currentTag->attrs->size + ADVFS_METADATA_PGSZ)) 
		    {
		        S_SET_MORE(currentTag->status);
			writemsg(SV_DEBUG, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_71, 
				 "Setting search by volume - More\n"));
			FS_SET_PASS2_NEEDED(fileset->status);
		    }
		    break;

		case BMTR_FS_DATA:
		    /*
		     * Symbolic Links
		     */
		    currentTag->fileType = FT_SYMLINK;
		    /*
		     * Don't use strlen on the string,  NOT NULL terminated.
		     */
		    size = (pRecord->bCnt - sizeof(bsMRT));
		    currentTag->addAttrs = salvage_malloc(size + 1);
		    if (NULL == currentTag->addAttrs) 
		    {
			writemsg(SV_ERR, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_68, 
				 "malloc() failed\n"));
			return NO_MEMORY;
		    }

		    strncpy(currentTag->addAttrs,pData,size);
		    ((char *)currentTag->addAttrs)[size] = '\0';
		    break;

		default:
		    writemsg(SV_DEBUG, 
			     catgets(mcat, S_SALVAGE_1, SALVAGE_72, 
			     "Found an invalid record type - %d\n"),
			     pRecord->type);
		    writemsg(SV_DEBUG, 
			     catgets(mcat, S_SALVAGE_1, SALVAGE_56, 
			     "Setting search by volume\n"));
		    FS_SET_PASS2_NEEDED(fileset->status);
		    break;
	    } /* end switch */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
        } /* end while loop of records */
	
        /* 
	 * Find next mcell
	 */
	if (SUCCESS != find_next_mcell(domain, &currentMcell, 
				       &pMcell, &mcellPage, FALSE)) 
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_73, 
		     "find_next_mcell() failed\n"));
	    FS_SET_PASS2_NEEDED(fileset->status);
	    return PARTIAL;
	}

	if ((pMcell != NULL) &&
	    ((currentTag->seqNum != pMcell->mcTag.tag_seq) ||
	    (tagNumber != pMcell->mcTag.tag_num))) {
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_74, 
		    "load_tag_from_mcell:next mcell tag.tag_seq %ld.%x != %ld.%x\n"),
		     tagNumber, currentTag->seqNum,
		     pMcell->mcTag.tag_num, pMcell->mcTag.tag_seq);
	    FS_SET_PASS2_NEEDED(fileset->status);
	    return PARTIAL;
	}
    } /* end while loop of mcells */

    return SUCCESS;
} /* end load_tag_from_mcell */


/*
 * Function Name: p2_load_tag_from_mcell
 *
 * Description:
 *  This is the pass2 version of load_tag_from_mcell. As we can be
 *  checking mcells which we already accessed we need to make sure we
 *  don't duplicate our effort.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  fileset: The fileset this tag belongs to. 
 *  tagNumber: The tag we are loading. 
 *  pMcell: A pointer to the mcell we are working on.
 *  volume: The volume we are working on.
 *
 * Output parameters:
 *  parentTag: The parent tag number of this tag.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY. 
 */
int p2_load_tag_from_mcell (domainInfoT *domain,
			    filesetLLT  *fileset,
			    tagNumT     tagNumber,
			    bsMCT       *pMcell,
			    tagNumT     *parentTag,
			    int         volume)
{
    char        *funcName = "p2_load_tag_from_mcell";
    filesetTagT *currentTag;
    int         recordType;
    void        *pData;
    int         status;
    bsMPgT      mcellPage;
    bsMRT       *pRecord;
    int         size;

    *parentTag = MISSING_PARENT;

    /* 
     * Check if tag does not exist in Array.
     */
    if (NULL == fileset->tagArray[tagNumber])
    {
        /*
	 * Create an element in the tag Array 
	 */
        status = create_tag_array_element(&(fileset->tagArray[tagNumber]));
        if (SUCCESS != status)
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_75, 
		     "Failed to create tag array element %ld of %ld\n"),
		     tagNumber, fileset->tagArraySize);
            return status;
	}

	/*
	 * Set the initial sequence number for this tag.  
	 */
	fileset->tagArray[tagNumber]->seqNum = pMcell->mcTag.tag_seq;

    }
    else if ((IGNORE == fileset->tagArray[tagNumber]) ||
	     (DEAD   == fileset->tagArray[tagNumber]))
    {
	return SUCCESS;
    }
    else
    {
	if (S_IS_COMPLETE(fileset->tagArray[tagNumber]->status))
	{
	    /*
	     * This tag is marked complete so no need to process
	     * this mcell, it was handled in a prior pass.
	     */
	    return SUCCESS;
	}
    }
    currentTag = fileset->tagArray[tagNumber];

    /*
     * Need to check seq number.
     *   The following situations could happen:
     *   A) Seq number is correct.
     *   B) Seq number is smaller than the current one.
     *   C) Seq number is larger than the current one.
     */
    if (pMcell->mcTag.tag_seq == currentTag->seqNum )
    {
        /*
	 * Do nothing special, this is the normal case.
	 */
    }
    else if (pMcell->mcTag.tag_seq < currentTag->seqNum)
    {
        /*
	 * This sequence number is smaller, therefor it is an old
	 * record which we can ignore.
	 */
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_76, 
		 "p2_load_tag_from_mcell tag %ld has smaller seq num: %x < %x\n"), 
		 tagNumber, pMcell->mcTag.tag_seq, currentTag->seqNum);
	return SUCCESS;
    }
    else
    {
        /*
	 * This sequence number is larger, delete tag array entry and
	 * tree node (if it exists), and create a new tag array entry.
	 */
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_77, 
		 "p2_load_tag_from_mcell tag %ld has larger seq num: %x > %x\n"), 
		 tagNumber, pMcell->mcTag.tag_seq, currentTag->seqNum);

	if (NULL == currentTag->firstInstance)
	{
	    /*
	     * No tree node, so just delete array entry.
	     */
	  delete_tag_array_entry(fileset,tagNumber);
	}
	else 
	{
	    /*
	     * Need to handle the case were the node in
	     * question is a directory with children.
	     */
	    if (currentTag->fileType == FT_DIRECTORY )
	    {
		filesetTreeNodeT *pCurr = NULL;
		filesetTreeNodeT *pNext = NULL;
		filesetTreeNodeT *pTopLostFound;

		/*
		 * If children exist they need to goto lost+found.
		 */
		pTopLostFound = fileset->dirTreeHead->nextSibling;
		pCurr = currentTag->firstInstance->children;

		while ( pCurr != NULL )
		{
		    pNext = pCurr->nextSibling;
		    move_node_in_tree( pCurr, pTopLostFound );
		    pCurr = pNext;
		}
	    }
	    /*
	     * At this point the node will have no children.
	     */
	    delete_node (&currentTag->firstInstance, fileset);
	}
	
	/*
	 * Create new element in the tag Array 
	 */
	status = create_tag_array_element(&(fileset->tagArray[tagNumber]));
	if (SUCCESS != status)
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_75, 
		     "Failed to create tag array element %ld of %ld\n"),
		     tagNumber, fileset->tagArraySize);
	    return status;
	}

	/*
	 * Set the initial sequence number for this tag.  
	 */
	fileset->tagArray[tagNumber]->seqNum = pMcell->mcTag.tag_seq;

	/*
	 * Reset currentTag
	 */
	currentTag = fileset->tagArray[tagNumber];
    }

    /* 
     * We need to step through this mcell record 
     */
    pRecord = (bsMRT *)pMcell->bsMR0;

    /*
     * Now loop through records.
     */
    while ((pRecord->type != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
    {
	pData = (char*)pRecord + sizeof(bsMRT);

	/* 
	 * Read records for this mcell:
	 *
	 * At this point we should only be reading records which deal with
	 * Individual files.  If we hit records which should not be part
	 * of a file, the we have data corruption. 
	 *
	 * Valid Types for FILES   : 1,5,6,19,250,252,254,255
	 * IGNORE Type             : 2,16,20,249,251
	 * Invalid types for FILES : 3,4,7-15,17,18,251
	 *
	 */
	switch (pRecord->type) 
	{
	  case BSR_XTNTS:
	  case BSR_XTRA_XTNTS:

            status = p2_add_extents_to_tag(domain, currentTag, pData,
                                            pRecord->type, volume);

            if (SUCCESS != status)
            {
                if (NO_MEMORY == status)
                {
                    return status;
                }
                writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_65,
                         "Failed to add extent to tag %ld in '%s'\n"),
                         tagNumber, fileset->fsName);
            }

            S_SET_PARTIAL(currentTag->status);
            break;

	  case BSR_ATTR:
	  case BSR_BF_INHERIT_ATTR:
	  case BMTR_FS_TIME:
	  case BMTR_FS_DIR_INDEX_FILE:
	    /*
	     * We are IGNORING these record types.
	     */
	    break;

          case BSR_ACL_REC:
            /*
             * Create or add to the tag's ACL buffer
             */
            status = add_tag_acl_data(currentTag, pData);
            if (status == NO_SPACE) {
                writemsg(SV_DEBUG, 
                            catgets(mcat, S_SALVAGE_1, SALVAGE_78, 
                            "Too many ACLs found for tag %ld\n"),
                            tagNumber);
            }
            break;

	  case BMTR_FS_UNDEL_DIR:
	    /*
	     * This file has a trashcan.
	     */
	    S_SET_TRASHCAN(currentTag->status);
	    break;

	  case BMTR_FS_STAT:
	    *parentTag = ((statT *) pData)->dir_tag.tag_num;
	    
	    if (FT_DEFAULT == currentTag->fileType)
	    {
		switch (((statT *) pData)->st_mode & S_IFMT) 
		{
		  case S_IFREG:
		    currentTag->fileType = FT_REGULAR;
		    break;
		  case S_IFDIR:
		    currentTag->fileType = FT_DIRECTORY;
		    break;
		  case S_IFBLK:
		  case S_IFCHR:
		    currentTag->fileType = FT_SPECIAL;

		    if (NULL == currentTag->addAttrs)
		    {
			dev_t    *tmpDev;

			tmpDev = (dev_t *) salvage_malloc(sizeof(dev_t));
			if (NULL == tmpDev) 
			{
			    writemsg(SV_ERR, 
				     catgets(mcat, S_SALVAGE_1, SALVAGE_68,
				     "malloc() failed\n"));
			    return NO_MEMORY;
			}

			*tmpDev = ((statT *)pData)->st_rdev;
		    
			currentTag->addAttrs = tmpDev;
		    }

		    break;
		  case S_IFIFO:
		    currentTag->fileType = FT_FIFO;
		    break;
		  case S_IFLNK:
		    currentTag->fileType = FT_SYMLINK;
		    break;
		  case S_IFSOCK:
                    /*
                     * Don't save sockets - delete the entry and
                     * mark as ignore if we can. If the number of
                     * links is greater than 1 and the size is not 0,
                     * then it probably isn't a socket, so set the type
                     * to unknown.
                     */
                    if (NULL == currentTag->firstInstance)
                    {
                        delete_tag_array_entry(fileset,tagNumber);
			return SUCCESS;
                    }
                    else if ((((statT *) pData)->st_nlink == 1) ||
                             (((statT *) pData)->st_size == 0))                    
                    {
                        status = delete_node (&currentTag->firstInstance, 
                                              fileset);
			return status;
                    }
                    else
                    {
                        currentTag->fileType = FT_UNKNOWN;
                    }
                    break;		    
		  default:
		    currentTag->fileType = FT_UNKNOWN;
		    break;
		} /* end switch */
	    }

	    /*
	     * Need to make sure both types match.  The reason for this
	     * is we could have found the tag with out finding the attribute.
	     */
	    if (NULL != currentTag->firstInstance)
	    {
		currentTag->firstInstance->fileType = currentTag->fileType;
	    }

	    if (NULL == currentTag->attrs)
	    {  
		status = create_tag_attr(&currentTag->attrs, pData);
		if (status != SUCCESS) 
		{
		    if (INVALID == status) 
		    {
			writemsg(SV_DEBUG, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_84, 
				 "Attribute data corrupt on tag %ld in '%s'\n"),
				 tagNumber, fileset->fsName);
		    } 
		    else 
		    {
			return status;
		    }
		}
	    }
	    break;

	  case BMTR_FS_DATA:
	    /*
	     * Symbolic Links
	     */
	    if ((NULL == currentTag->addAttrs) &&
		((FT_SYMLINK == currentTag->fileType) ||
		 (FT_DEFAULT == currentTag->fileType)))
	    {
		currentTag->fileType = FT_SYMLINK;
		/*
		 * Don't use strlen on the string,  NOT NULL terminated.
		 */
		size = (pRecord->bCnt - sizeof(bsMRT));
		currentTag->addAttrs = salvage_malloc(size + 1);
		if (NULL == currentTag->addAttrs) 
		{
		    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_68,
			     "malloc() failed\n"));
		    return NO_MEMORY;
		}
		strncpy(currentTag->addAttrs,pData,size);
		((char *)currentTag->addAttrs)[size] = '\0';
	    }
	    break;

	  case BMTR_FS_INDEX_FILE:
	    /*
	     * This is an 'invisable' file which we do not
	     * want to collect information about.  So free
	     * the node and mark IGNORE
	     */
	    if (NULL == currentTag->firstInstance)
	    {
		delete_tag_array_entry(fileset,tagNumber);
	    }
	    else 
	    {
		delete_node (&currentTag->firstInstance, fileset);
	    }

	    return SUCCESS;
	    break;
	    
	  default:
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_72, 
		     "Found an invalid record type - %d\n"),
		     pRecord->type);
	    break;
	} /* end switch */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(uint64_t))); 
    } /* end while */

    /*
     * If we have attributes check to see if file is complete.
     */
    if (NULL != currentTag->attrs) 
    {
	if (currentTag->bytesFound == currentTag->attrs->size) 
	{
	    S_SET_COMPLETE(currentTag->status);
	} 
	else if (currentTag->bytesFound >(currentTag->attrs->size + 
                                          ADVFS_METADATA_PGSZ)) 
	{
	    S_SET_MORE(currentTag->status);
	}
    }

    return SUCCESS;
} /* end p2_load_tag_from_mcell */



/*
 * Function Name: load_fileset_tag_extents
 *
 * Description:
 *  Given the fileset tag file's mcell load all of the tags into the
 *  buffer.
 *
 * Input parameters:
 *  domain: The domain we are working on. 
 *  fsTagFileMcell: The mcell which is the head of the next chain.
 *
 * Output parameters:
 *  tagExtentBuffer: A buffer containing all tag extents in the 
 *                   fileset tag file. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or PARTIAL.
 *
 */
int load_fileset_tag_extents(domainInfoT *domain,
			     bfMCIdT     *fsTagFileMcell,
			     extentT     *tagExtentBuffer)
{   
    char          *funcName = "load_fileset_tag_extents";
    bsMPgT        mcellPage;
    int64_t       counter = 0;
    bf_vd_blk_t   lbn;
    int           fd;
    bfMCIdT       current;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsXtraXtntRT  *pXtraXtnt;
    bsXtntRT      *pXtnt;
    int           x;
    int64_t       bytesRead;
    int           status;
    int           followChain = FALSE;

    if (NULL == fsTagFileMcell) {
	/*
	 * Don't have the primary mcell for the this filesets
	 * tag file.
	 */
	return FAILURE;
    }

    /*
     * Initialize current
     */
    current.volume = fsTagFileMcell->volume;
    current.page   = fsTagFileMcell->page;
    current.cell   = fsTagFileMcell->cell;

    /*
     * Get New LBN and FD
     */
    if (SUCCESS != bmt_page_to_lbn(domain, current.volume, current.page, 
				   &lbn, &fd)) 
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_62, 
                 "%s : Cannot convert BMT volume %d page %ld to lbn\n"),
		 funcName, current.volume, current.page);
        return FAILURE;
    }
  
    /* 
     * load new page into mcellPage buffer.
     */
    status = read_page_by_lbn(fd, &mcellPage, lbn, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
    {
        return FAILURE;
    }

    /* 
     * Get the cell we want 
     */
    pMcell = &(mcellPage.bsMCA[current.cell]);
  
    /*
     * Loop through the NEXT chain
     */
    while (NULL != pMcell) 
    {
        /* 
	 * Now on mcell we need to step through this record 
	 */
        pRecord = (bsMRT *)pMcell->bsMR0;
 
	/*
	 * Now loop through records.
	 */
	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	{
	    switch (pRecord->type)
	    {
	      case BSR_XTNTS:
		pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));

		/*
		 * Loop through all the extents in this record.
		 */
		for (x = 0; 
			x < pXtnt->xCnt && x < BMT_XTNTS;
			x++, counter++) 
		{
		    tagExtentBuffer[counter].volume = current.volume;
		    tagExtentBuffer[counter].fob = pXtnt->bsXA[x].bsx_fob_offset;

		    /*
		     * As this is stored as a uint64_t, not an
                     * int64_t we need to handle it this way.
		     */
		    if (pXtnt->bsXA[x].bsx_vd_blk != XTNT_TERM &&
			pXtnt->bsXA[x].bsx_vd_blk != PERM_HOLE_START)
		    {
			tagExtentBuffer[counter].lbn = pXtnt->bsXA[x].bsx_vd_blk;
		    } 
		    else 
		    {
			tagExtentBuffer[counter].lbn =  -1;
		    }
		}
		followChain = TRUE;
		break;
	      case BSR_XTRA_XTNTS:
	        pXtraXtnt = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		
		/*
		 * Loop on all extents
		 */
		for (x = 0; 
		     x < pXtraXtnt->xCnt && x < BMT_XTRA_XTNTS;
		     x++, counter++) 
		{
		    tagExtentBuffer[counter].volume = current.volume;
		    tagExtentBuffer[counter].fob = pXtraXtnt->bsXA[x].bsx_fob_offset;

		    /*
		     * As this is stored as a uint64_t, not a int64_t
		     * we need to handle it this way.
		     */
                    if (pXtraXtnt->bsXA[x].bsx_vd_blk != XTNT_TERM &&
                        pXtraXtnt->bsXA[x].bsx_vd_blk != PERM_HOLE_START)
		    {
		        tagExtentBuffer[counter].lbn =pXtraXtnt->bsXA[x].bsx_vd_blk;
		    } 
		    else 
		    {
		        tagExtentBuffer[counter].lbn =  -1;
		    }
		} /* end for loop */
		break;

	      default:
		break;
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
	} /* end while record loop */
 
	/* 
	 * Find next mcell
	 */
	if (SUCCESS != find_next_mcell(domain, &current, 
				       &pMcell, &mcellPage, followChain)) 
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_86, 
                     "Broken mcell chain - returning PARTIAL\n"));
	    return PARTIAL;
	}
	followChain = FALSE;
    } /* end while mcell chain loop */

    return SUCCESS;
} /* end load_fileset_tag_extents */


/*
 * Function Name: load_filesets
 *
 * Description:
 *  Search the root tag file to find information on all the
 *  filesystems in this domain. Once a fileset is located, store the
 *  information about this fileset in the domain's data structures.
 *
 * Input parameters:
 *  mcellBuffer: A pointer to a mcell Page 
 *  pData: A pointer to a buffer of extent data. 
 *  domain: The domain we are working on. 
 *  volume: The volume we are working on.
 *  bufferType: Which type of buffer (BSR_XTNTS or BSR_XTRA_XTNTS)
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, PARTIAL, or NO_MEMORY.  
 */
int load_filesets(bsMPgT      *mcellBuffer,
		  void        *pData,
		  domainInfoT *domain,
		  int         volume,
		  int         bufferType)
{
    char         *funcName = "load_filesets";
    bsXtntRT     *pXtnt;
    bsXtraXtntRT *pXtra;
    int64_t      extent;
    bs_meta_page_t page;
    bs_meta_page_t pagesInExtent;
    int          fd;
    bf_vd_blk_t  lbn;
    int          status;
    
    /*
     * The most common situation, the other buffer type is the exception.
     */
    if (BSR_XTNTS == bufferType) 
    {
        pXtnt = (bsXtntRT *)pData;
    
	/*
	 * Loop through all the extents in this record.
	 */

	for (extent = 0; 
	     ((extent < pXtnt->xCnt) && ( extent < BMT_XTNTS + 1));
	     extent++) 
	{
	    /*
	     * If the extent points to a valid location on disk.
	     */
            if (XTNT_TERM != pXtnt->bsXA[extent].bsx_vd_blk &&
                PERM_HOLE_START != pXtnt->bsXA[extent].bsx_vd_blk)
	    {
	        pagesInExtent = (pXtnt->bsXA[extent + 1].bsx_fob_offset/8 -
				 pXtnt->bsXA[extent].bsx_fob_offset/8);
		/*
		 * Loop through all the pages in the extent.
		 */
		for (page = 0; page < pagesInExtent; page++) 
		{
		    /*
		     * Get New LBN and FD
		     */
		    fd = domain->volumes[volume].volFd;    
 		    lbn = pXtnt->bsXA[extent].bsx_vd_blk + 
		      (page * BLOCKS_PER_PAGE);

		    status = process_root_tag_file_page(domain,fd,lbn);
		    if (SUCCESS != status)
		    {
		        return status;
		    }
		} /* end for loop */
	    }
	} /* end for loop */
    }
    else if (BSR_XTRA_XTNTS == bufferType) 
    {
        pXtra = (bsXtraXtntRT *)pData;

	/*
	 * Loop through all the extents in this record.
	 */
	for (extent = 0; 
	     ((extent < pXtra->xCnt) && ( extent < BMT_XTRA_XTNTS));
	     extent++) 
	{ 
	    /*
	     * If the extent points to a valid location on disk.
	     */
            if (XTNT_TERM != pXtra->bsXA[extent].bsx_vd_blk &&
                PERM_HOLE_START != pXtra->bsXA[extent].bsx_vd_blk)
	    {
	        pagesInExtent = (pXtra->bsXA[extent + 1].bsx_fob_offset/8 -
				 pXtra->bsXA[extent].bsx_fob_offset/8);
		
		/*
		 * Loop through all the pages in the extent.
		 */
		for (page = 0; page < pagesInExtent; page++) 
		{
		    /*
		     * Get New LBN and FD
		     */
		    fd = domain->volumes[volume].volFd;    
 		    lbn = pXtra->bsXA[extent].bsx_vd_blk + 
		      (page * BLOCKS_PER_PAGE);

		    status = process_root_tag_file_page(domain,fd,lbn); 
		    if (SUCCESS != status)
		    {
		        return status;
		    }
		} /* end for pages loop */
	    }
	} /* end for extent loop */
    }
    return SUCCESS;
} /* load_filesets */


/*
 * Function Name: process_root_tag_file_page
 *
 * Description:
 *  This functions takes the root tag file, and processes all
 *  entries in it.  This creates all the in-memory structures
 *  required to access the filesets.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  fd: The fd of the volume we are workin on.
 *  lbn: The logical disk block number.
 *
 * Returns: SUCCESS, FAILURE, PARTIAL or NO_MEMORY
 */
int process_root_tag_file_page(domainInfoT *domain,
			       int         fd,
			       bf_vd_blk_t lbn)
{
    char       *funcName = "process_root_tag_file_page";
    bsTDirPgT  tagBuffer;
    int        counter;
    bfMCIdT    current;
    filesetLLT *fileset;
    bsMCT      *pMcell;
    bsMRT      *pRecord;
    int64_t    bytesRead;
    int        foundFileset;
    int        status;

    /*
     * Read a page of tags.
     */
    status = read_page_by_lbn(fd, &tagBuffer, lbn, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
    {
        return FAILURE;
    }

    /*
     * Loop through all tags in this page.
     */
    for (counter = 0; counter < BS_TD_TAGS_PG; counter++) 
    {
        /*
	 * The tag in question is marked in use.
	 */
        if (tagBuffer.tMapA[counter].tm_u.tm_s3.flags & BS_TD_IN_USE) 
	{
	    bsMPgT mcellPage;
	    bf_vd_blk_t tempLbn;
	    int    tempFd;

	    foundFileset = FALSE;
	    
	    /* 
	     * Create fileset element 
	     */
	    status = create_fileset_element(&fileset, domain);
	    if (SUCCESS != status)
	    {
	        return status;
	    }

	    /*
	     * Initialize current to primary mcell for fileset.
	     */
	    current.volume = tagBuffer.tMapA[counter].tm_u.tm_s3.bfMCId.volume;
	    current.page   = tagBuffer.tMapA[counter].tm_u.tm_s3.bfMCId.page;
	    current.cell   = tagBuffer.tMapA[counter].tm_u.tm_s3.bfMCId.cell;
	
	    /*
	     * Get New LBN and FD for primary mcell.
	     */
	    if (SUCCESS != bmt_page_to_lbn(domain, current.volume, current.page,
					   &tempLbn, &tempFd)) 
	    {
		writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_62, 
                         "%s : Cannot convert BMT volume %d page %ld to lbn\n"),
			 funcName, current.volume, current.page);
	        return FAILURE;
	    }

	    /* 
	     * load new page into mcellPage buffer.
	     */
	    status = read_page_by_lbn(tempFd, &mcellPage, tempLbn, &bytesRead);
	    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
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
		    bsXtntRT     *pXtnt;
		    bsBfSetAttrT *pAttr;
		    bsQuotaAttrT *pQuota;
	    
		    switch (pRecord->type) 
		    {
		        case BSR_XTNTS:
		            /* 
			     * Set up data buffer 
			     */
		            pXtnt = (bsXtntRT *)((char *)pRecord + 
						 sizeof(bsMRT));
			    /*
			     * Malloc space to save mcell pointer.
			     */
			    fileset->tagFileMcell = salvage_malloc(sizeof(bfMCIdT));
			    if (NULL == fileset->tagFileMcell) 
			    {
			        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, 
					 SALVAGE_68, "malloc() failed\n"));
				return NO_MEMORY;
			    }

			    /*
			     * Start with the current mcell.
			     */
			    fileset->tagFileMcell->volume = current.volume;
			    fileset->tagFileMcell->page   = current.page;
			    fileset->tagFileMcell->cell   = current.cell;
			    foundFileset = TRUE;
			    break;

			case BSR_BFS_QUOTA_ATTR:
			    /* 
			     * Set up data buffer 
			     */
			    pQuota = (bsQuotaAttrT *)((char *)pRecord + 
						      sizeof(bsMRT));
			    
			    fileset->quota = salvage_malloc(sizeof(struct filesetQuota));
			    if (NULL == fileset->quota) 
			    {
			        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, 
					 SALVAGE_68, "malloc() failed\n"));
				return NO_MEMORY;
			    }
			    
			    fileset->quota->hardBlockLimit = pQuota->blkHLimit;
			    fileset->quota->softBlockLimit = pQuota->blkSLimit;
			    fileset->quota->hardFileLimit = pQuota->fileHLimit;
			    fileset->quota->softFileLimit = pQuota->fileSLimit;
			    fileset->quota->blockTimeLimit = pQuota->blkTLimit;
			    fileset->quota->fileTimeLimit = pQuota->fileTLimit;

			    break;

			case BSR_BFS_ATTR:
			    /* 
			     * Set up data buffer 
			     */
			    pAttr = (bsBfSetAttrT *)((char *)pRecord + 
						     sizeof(bsMRT));

			    /*
			     * Found a snap fileset.
			     */
			    if (0 != pAttr->bfsaSnapLevel)
			    {
				FS_SET_SNAP(fileset->status);
				writemsg(SV_VERBOSE, 
					 catgets(mcat, S_SALVAGE_1, 
					 SALVAGE_87, 
					 "'%s' is a snapshot\n"),
					 pAttr->setName);
			    }

			    /*
			     * This fileset has a snap.  Let the user know.
			     */
                            if (!BS_BFS_EQL(pAttr->bfsaFirstChildSnapSet,nilMlBfSetIdT))
			    {
			        FS_SET_HAS_SNAP(fileset->status);
				writemsg(SV_VERBOSE, 
					 catgets(mcat, S_SALVAGE_1, 
					 SALVAGE_88, 
					 "'%s' has a snapshot\n"),
					 pAttr->setName );
			    }
			    
			    /*
			     * Compute this filesets name
			     */
			    fileset->fsName = salvage_malloc(strlen(pAttr->setName)+1);
			    if (NULL == fileset->fsName) 
			    {
			        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1,
					 SALVAGE_68, "malloc() failed\n"));
				return NO_MEMORY;
			    }
			    strcpy(fileset->fsName,pAttr->setName);

			    fileset->filesetId.domainId.id_sec  = 
			      pAttr->bfSetId.domainId.id_sec;
			    fileset->filesetId.domainId.id_usec = 
			      pAttr->bfSetId.domainId.id_usec;
			    fileset->filesetId.dirTag.tag_num = 
			      pAttr->bfSetId.dirTag.tag_num;
			    fileset->filesetId.dirTag.tag_seq = 
			      pAttr->bfSetId.dirTag.tag_seq;
			    break;

			default:
			    break;
		    } /* end switch */
		    pRecord = (bsMRT *) (((char *)pRecord) + 
					 roundup(pRecord->bCnt, sizeof(uint64_t))); 
		} /* end while */
		
		/* 
		 * Find next mcell
		 */
		if (SUCCESS != find_next_mcell(domain, &current, 
					       &pMcell, &mcellPage, FALSE)) 
		{
		    writemsg(SV_DEBUG, 
                             catgets(mcat, S_SALVAGE_1, SALVAGE_73,
			     "find_next_mcell() failed\n"));
		    return FAILURE;
		}
	    } /* end while */
	    
	    /*
	     * If the the primary mcell is not found for this fileset.
	     * Remove the file set and continue.
	     */
	    if (FALSE == foundFileset) {
		if (NULL != fileset->fsName) {
		    free(fileset->fsName);
		}
		if (NULL != fileset->quota) {
		    free(fileset->quota);
		}
		if (fileset->tagFileMcell) {
		    free(fileset->tagFileMcell);
		}
		free(fileset);
		
		writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_92, 
					   "Setting search by volume - invalid primary mcell\n"));

		D_SET_PASS2_NEEDED(domain->status);
		continue;
	    }

	    /*
	     * Check to see if we found a name for this fileset.
	     */
	    if (NULL == fileset->fsName) 
	    {
	        /*
		 * Fileset name was not found.  Set Pass2.
		 */
	         writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_89, 
			  "Setting search by volume - no file system/snapshot name\n"));
	         FS_SET_PASS2_NEEDED(fileset->status);
	    }

	    /*
	     * Check to see if this is the fileset we need to recover.
	     * Make sure we check for NULL first.
	     */
	    if (NULL !=  Options.setName)
	    {
	        /*
		 * Check to see if we are recovering by settag number.
		 */
	        if ('#' == Options.setName[0])
		{
		    int64_t filesetNum;

		    filesetNum = atol(&(Options.setName[1]));

		    if (filesetNum != fileset->filesetId.dirTag.tag_num)
		    {
		        /*
			 * Not a settag we wish to restore.
			 */
		        FS_SET_NOT_RESTORE(fileset->status);	  
		    }
		}
		else if	(strcmp(fileset->fsName, Options.setName) != 0)
		{
		    /*
		     * Not a fileset we are interested in
		     */
		    FS_SET_NOT_RESTORE(fileset->status);	  
		}
	    }

	    /* 
	     * Is this the first fileset found 
	     */
	    if (NULL == domain->filesets) 
	    {
		domain->filesets = fileset;
	    } 
	    else 
	    {
		filesetLLT   *current;
	
		/* 
		 * Have already found filesets, set current to first one 
		 */
		current = domain->filesets; 
	  
		/*
		 * Add this fileset to the end of the list.
		 */
		while (current->next != NULL) 
		{
		    current = current->next;
		} /* end while */
     
		current->next = fileset;
	   } 
	}
    } /* end for loop */

    return SUCCESS;
} /* end process_root_tag_file_page */


/*
 * Function Name: load_bmt_lbn_array
 *
 * Description: 
 *  Given a BMT buffer and the number of BMT extents, load the array
 *  with BMT page to LBN. This routine assumes all BMT extents are
 *  located in the RBMT (or BMT0).
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  volume: The volume we are working on.
 *  BMTbuffer: A pointer to the BMT buffer. 
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.  
 */
int load_bmt_lbn_array(domainInfoT *domain,
		       volMapT     *volume,
		       bsMPgT      *BMTbuffer)
{
    char         *funcName = "load_bmt_lbn_array";
    int          status;
    int          counter;
    int          bmtCell;
    int64_t      extent = 0;
    int          fd;
    bs_meta_page_t currentPage = 0;
    int64_t      bytesRead;
    bf_vd_blk_t  lbn;
    bsMCT        *pMcell;
    bsMRT        *pRecord;
    bsXtraXtntRT *pXtra;
    bsXtntRT     *pXtnt;
    extentT      *bmt2Lbn;
    int64_t      lastExtent;
    bsMPgT       buffer;

    fd         = volume->volFd;
    bmt2Lbn    = volume->extentArray;  
    lastExtent = volume->extentArraySize;

    /*
     * Handle the extents in RBMT page 0 cell 4, these are NOT hard coded
     */

    pMcell  = &(BMTbuffer->bsMCA[4]);
    pRecord = (bsMRT *)pMcell->bsMR0;

    while ((pRecord->type != 0) && 
	   (pRecord->bCnt != 0) && 
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ]))))
    {
	if (pRecord->type == BSR_XTNTS) 
	{
	    pXtnt = (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
	    
	    for (counter = 0; 
		 counter < pXtnt->xCnt && counter < BMT_XTNTS; 
		 counter++) 
	    {
		bmt2Lbn[extent].fob = 
		    pXtnt->bsXA[counter].bsx_fob_offset;

		/*
		 * As this is stored as a uint64_t, not a int64_t
		 * we need to handle it this way.
		 */
		if (pXtnt->bsXA[counter].bsx_vd_blk != XTNT_TERM &&
		    pXtnt->bsXA[counter].bsx_vd_blk != PERM_HOLE_START)
		{
		    bmt2Lbn[extent].lbn = 
			pXtnt->bsXA[counter].bsx_vd_blk;
		    extent++;
		} 
		else 
		{
		    bmt2Lbn[extent].lbn =  -1;
		}

	    }
	}
	pRecord = (bsMRT *) (((char *)pRecord) + 
				roundup(pRecord->bCnt, sizeof(uint64_t))); 
    } /* end while */

    /*
     * Find the next cell of records.
     */
    if (SUCCESS != find_bmt_chain_ptr(domain, BMTbuffer, &bmtCell )) 
    {
        return FAILURE;
    }

    /*
     * No additional BMT cells return SUCCESS.
     */
    if (0 == bmtCell)
    {
        return SUCCESS;
    }

    pMcell = &(BMTbuffer->bsMCA[bmtCell]);

    while (NULL != pMcell) 
    {
        /* 
	 * Now on cell we need to step through this record
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
	     * This must always be BSR_XTRA_XTNTS 
	     */
	    switch (pRecord->type)
	    {
	      case BSR_XTRA_XTNTS:
	        /* 
		 * Set up data buffer 
		 */
	        pXtra =  (bsXtraXtntRT *)((char*)pRecord + sizeof(bsMRT));

		/* 
		 * For each extent record in this buffer process the data 
		 */
		for (counter = 0; 
		     ((counter < pXtra->xCnt) && (counter < BMT_XTRA_XTNTS));
		     counter++) 
		{
		    bmt2Lbn[extent].fob = pXtra->bsXA[counter].bsx_fob_offset;
		    
                    if (pXtra->bsXA[counter].bsx_vd_blk != XTNT_TERM &&
                        pXtra->bsXA[counter].bsx_vd_blk != PERM_HOLE_START)
		    {
		        bmt2Lbn[extent].lbn = pXtra->bsXA[counter].bsx_vd_blk;
			extent++;
		    }
		    else 
		    {
		        bmt2Lbn[extent].lbn = -1;
			/*
			 * The only entry which should be -1, is the last
			 * entry in each record.  We need to save it in case
			 * the next record is bad.  Otherwise we overwrite it.
			 * So DON'T increment extent.
			 */
		    }

		} /* end for loop */
		break;

	      default:
		/*
		 * Corrupt record, set LBN to -2;
		 * Leave page as what ever value it is currently
		 * set to.  This will be the last entry of the
		 * previous record.  Which would have had a lbn of -2.
		 */
		bmt2Lbn[extent].lbn  = -2;
		extent ++;

		break;
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
	} /* end while pRecord loop */

	/*
	 * We might move to another page.
	 */
	bmtCell = pMcell->mcNextMCId.cell;

	if (!((pMcell->mcNextMCId.volume == 0) &&
	      (pMcell->mcNextMCId.page   == 0) &&
	      (pMcell->mcNextMCId.cell   == 0)))
	{
	    /*
	     * Do we move to the next page
	     */
	    if (pMcell->mcNextMCId.page != currentPage)
	    {
		currentPage = pMcell->mcNextMCId.page;

		/*
		 * Set next page
		 */
		lbn = volume->rbmtArray[currentPage].lbn;
		
		/*
		 * Load the next page.
		 */
		status = read_page_by_lbn(fd, &buffer, lbn, &bytesRead);
		if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
		{
		    return FAILURE;
		}

		pMcell = &(buffer.bsMCA[bmtCell]);
	    }
	    else
	    {
		if (0 == currentPage)
		{
		    pMcell = &(BMTbuffer->bsMCA[bmtCell]);
		}
		else
		{
		    pMcell = &(buffer.bsMCA[bmtCell]);
		}
	    }
	} 
	else 
	{
	    bmtCell = -1;
	    pMcell = NULL;
	}
    } /* end while loop */

    return SUCCESS;
} /* end load_bmt_lbn_array */


/*
 * Function Name: find_number_extents
 *
 * Description:
 *  Compute the number of extents in a file.
 *
 * Input parameters:
 *  domain:  The domain we are working on.
 *  current: The mcell describing the file.
 *
 * Output parameters:
 *  extents: return the number of file extents.
 *
 * Returns: SUCCESS or FAILURE.
 */
int find_number_extents(domainInfoT *domain,
		        bfMCIdT     current,
			int64_t     *extents)
{    
    char          *funcName = "find_number_extents";
    bf_vd_blk_t   lbn;
    int           fd;
    int64_t       bytesRead;
    int           status;
    bsMPgT        mcellPage;
    int64_t       numExtents = 0;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsXtraXtntRT  *pXtra;
    bsXtntRT      *pXtnt;
    int           followChain = TRUE;
    *extents = -1;
    
    /*
     * Check to make sure there extents.
     */
    if ((0 == current.volume) && (0 == current.page) && (0 == current.cell)) 
    {
	*extents = 0;
	return SUCCESS;
    }

    /*
     * Get New LBN and FD
     */
    if (SUCCESS != bmt_page_to_lbn(domain, current.volume, current.page,
				   &lbn, &fd)) 
    {
	writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_62, 
                 "%s : Cannot convert BMT volume %d page %ld to lbn\n"),
		 funcName, current.volume, current.page);
        return FAILURE;
    }

    /* 
     * load the mcell page into mcellPage buffer.
     */
    status = read_page_by_lbn(fd, &mcellPage, lbn, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
    {
        return FAILURE;
    }

    /* 
     * Locate the mcell we want in mcellPage. 
     */
    pMcell = &(mcellPage.bsMCA[current.cell]);

    /*
     * Loop through records to load extents
     */
    while (NULL != pMcell) 
    {
        /* 
	 * Now on cell we need to step through this record
	 */
        pRecord = (bsMRT *)pMcell->bsMR0;

	/*
	 * Now loop through records.
	 */
	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	{
	    switch (pRecord->type) 
	    {
		case BSR_XTNTS:
		    /* 
		     * Set up data buffer 
		     */
		    pXtnt =  (bsXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    numExtents += pXtnt->xCnt - 1;
		    break;

		case BSR_XTRA_XTNTS:
		    /* 
		     * Set up data buffer 
		     */
		    pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));
		    numExtents += pXtra->xCnt - 1;
		    break;

		default:
		    /*
		     * Found invalid record, assume corruption.
		     */
		    numExtents++;
		    break;
	    } /* end switch */
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
	} /* end while loop */
	/* 
	 * Find next mcell
	 */
	if (SUCCESS != find_next_mcell(domain, &current, 
				       &pMcell, &mcellPage, followChain))
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_73, 
                     "find_next_mcell() failed\n"));
	    return FAILURE;
	}
	followChain = FALSE;
    } /* end while loop */
    
    *extents = numExtents;
    return SUCCESS;
} /* end find_number_extents */


/*
 * Function Name: load_domain_attributes
 *
 * Description:
 *  Follow the next mcell pointer from BMT/RBMT page 0 mcell 0, load the 
 *  data from this mcell into our structures.
 *
 * Input parameters:
 *  BMTbuffer: A pointer to the BMT buffer. 
 *  domain: The domain we are working on. 
 *  volNumber: The volume we are working on. 
 *
 * Returns:
 *  Status value - SUCCESS.  
 */
int load_domain_attributes(bsMPgT      *BMTbuffer,
			   domainInfoT *domain,
			   int         *volNumber)
{
    char          *funcName = "load_domain_attributes";
    bsMPgT        mcellPage;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsVdAttrT     *pVdAttr;
    bsDmnAttrT    *pDmnAttr;
    bsDmnMAttrT   *pDmnMAttr;
    int           volId1 = 0;
    int           volId2 = 0;
    int           volId3 = 0;

    /* 
     * Locate mcell 0 in BMTObuffer. 
     */
    pMcell = &(BMTbuffer->bsMCA[0]);

    /*
     * Get which volume ID from the NEXT pointer of mcell 0.
     */
    volId1 = pMcell->mcNextMCId.volume;

    /*
     * Follow the NEXT pointer to find the mcell we need.
     * ODS Version >=4 : this will always be 6.
     */
    pMcell = &(BMTbuffer->bsMCA[pMcell->mcNextMCId.cell]);

    /*
     * Compute from this mcell's BMT tag number its volume id
     */
    volId2 = (-1 * pMcell->mcTag.tag_num / 6);
    
    /* 
     * Now on cell we need to step through this record
     */
    pRecord = (bsMRT *)pMcell->bsMR0;

    while ((pRecord->type != 0) &&
	   (pRecord->bCnt != 0) &&
	   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
    {
        switch (pRecord->type) 
	{
	    case BSR_VD_ATTR:
	        /* 
		 * Set up data buffer 
		 */
	        pVdAttr = (bsVdAttrT *)((char *)pRecord + sizeof(bsMRT));
		volId3 = pVdAttr->vdIndex;
		break;

	    case BSR_DMN_ATTR:
		/* 
		 * Set up data buffer 
		 */
		pDmnAttr = (bsDmnAttrT *)((char *)pRecord + sizeof(bsMRT));
	      
		/*
		 * If this domain doesn't have a dmn ID give it one.
		 */
		if (domain->dmnId.id_sec == 0)
		{
		    domain->dmnId.id_sec = pDmnAttr->bfDomainId.id_sec;
		    domain->dmnId.id_usec = pDmnAttr->bfDomainId.id_usec;
		} 
		else if ((domain->dmnId.id_sec != 
			  pDmnAttr->bfDomainId.id_sec) ||
			 (domain->dmnId.id_usec != 
			  pDmnAttr->bfDomainId.id_usec))
		{
		    writemsg(SV_VERBOSE, 
			     catgets(mcat, S_SALVAGE_1, SALVAGE_90, 
                             "Storage domain ID does not match attribute record - continuing to process\n"));
		}
		break;

	    case BSR_DMN_MATTR:
		/* 
		 * Set up data buffer 
		 */
		pDmnMAttr = (bsDmnMAttrT *)((char *)pRecord + sizeof(bsMRT));
		domain->numberVolumes = pDmnMAttr->vdCnt;
		break;

	    default:
		break;
	} /* end switch */
	pRecord = (bsMRT *) (((char *)pRecord) + 
			     roundup(pRecord->bCnt, sizeof(uint64_t))); 
    } /* end while loop */

    if ((volId1 == volId2) && (volId2 == volId3)) 
    {
	/*
	 * All three match - No corruption.
	 */
	*volNumber = volId1;
    } 
    else if ((volId1 == volId2) || (volId1 == volId3)) 
    {
	/*
	 * Two of the three match, volId1 not corrupt.
	 */
	*volNumber = volId1;
    } 
    else if (volId2 == volId3) 
    {
	/*
	 * Two of the three match, volId1 corrupt.
	 */
	*volNumber = volId2;
    } 
    else 
    {
	/*
	 * No match - Have chosen to use volId1.
	 */
	*volNumber = volId1;
    }

    return SUCCESS;
} /* end load_domain_attributes */


/*
 * Function Name: add_extents_to_tag
 *
 * Description:
 *
 *  Parse the extent in BSR_XTNTS record, add it to the link list of 
 *  extents for this tag.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  tag: A pointer to a tag, which these extents belong to. 
 *  pdata: A pointer to a data buffer, used to find this tag's extents. 
 *  xtntVolume: The volume which the xtnt record is on.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, NO_MEMORY or INVALID
 *
 * Side Effects:
 *  The extent structures will be added to the passed in tag.
 */
int add_extents_to_tag(domainInfoT *domain,
		       filesetTagT *tag,
		       bsXtntRT    *pData,
		       int         xtntVolume)
{
    char         *funcName = "add_extents_to_tag";
    int           volume;
    bf_vd_blk_t   diskBlock;
    int64_t       size;
    bs_meta_page_t page;
    extentLLT     *currentExtent;
    bsMPgT        mcellPage;
    bsMCT         *pMcell;
    bsMRT         *pRecord;
    bsXtntRT      *pXtnt;
    bsXtraXtntRT  *pXtra;
    bfMCIdT       current;
    bf_vd_blk_t   lbn;
    int           fd;
    int           x;
    int           status;
    int64_t       bytesRead;

    /*
     * We need to load extents from the BSR_XTNT record.
     */
    /*
     * Ignore record with out extents. (xCnt <= 1)
     */
    if (pData->xCnt > 1)
    {
	x = 0;
	diskBlock = pData->bsXA[x].bsx_vd_blk;
	page = (pData->bsXA[x].bsx_fob_offset * ADVFS_FOB_SZ)/8;
	size = (pData->bsXA[x + 1].bsx_fob_offset - 
		pData->bsXA[x].bsx_fob_offset) * ADVFS_FOB_SZ;

	if (NULL == tag->extentHead) 
	{
	    /* 
	     * Create first extent of tag 
	     */
	    status = create_extent_element(&tag->extentHead, xtntVolume, 
					    diskBlock, size, page);
	    if (SUCCESS != status)
	    {
		return status;
	    }
	
	    tag->extentTail  = tag->extentHead;
	    tag->bytesFound += size;
	} 
	else 
	{
	    /* 
	     * We should NEVER get here, as this should always
	     * be the first extent in pass 1.  
	     */
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_91, 
					"First Xtnt is not head of chain\n"));
	}
    }

    /*
     * Initialize current
     */
    current.volume = pData->chainMCId.volume;
    current.page   = pData->chainMCId.page;
    current.cell   = pData->chainMCId.cell;

    /*
     * Check to see if a chain of mcells.
     */
    if ((0 == current.volume) && (0 == current.page) && (0 == current.cell)) 
    {
        /*
	 * No chain of extents.
	 */
        return SUCCESS;
    }

    if (TRUE == domain->volumes[current.volume].badVolNum)
    {
	/*
	 * This chain starts on a bad volume.  Go to pass 2.
	 */
	D_SET_PASS2_NEEDED(domain->status);
	return FAILURE;
    }

    /*
     * Get New LBN and FD
     */
    status = bmt_page_to_lbn(domain, current.volume, current.page, &lbn, &fd); 
    if (SUCCESS != status)
    {
	writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_62, 
                 "%s : Cannot convert BMT volume %d page %ld to lbn\n"),
		 funcName, current.volume, current.page);
        return status;
    }

    /* 
     * load the mcell page into mcellPage buffer.
     */
    status = read_page_by_lbn(fd, &mcellPage, lbn, &bytesRead);
    if ((SUCCESS != status) || (bytesRead != ADVFS_METADATA_PGSZ))
    {
        return FAILURE;
    }

    /* 
     * Locate the mcell we want in mcellPage. 
     */
    pMcell = &(mcellPage.bsMCA[current.cell]);

    /*
     * Loop through mcells
     */
    while (NULL != pMcell) 
    {
        /* 
	 * Now on cell we need to step through this record
	 */
        pRecord = (bsMRT *)pMcell->bsMR0;

	/*
	 * Now loop through records.
	 */
	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	{
	    switch (pRecord->type) 
	    {
	         case BSR_XTRA_XTNTS:
		     pXtra = (bsXtraXtntRT *)((char *)pRecord + sizeof(bsMRT));

		     /*
		      * Loop through extents, skip last one
		      */
		     for (x = 0; 
			  ((x < pXtra->xCnt -1) && (x < BMT_XTRA_XTNTS));
			  x++) 
		     {
		         volume = current.volume;
			 diskBlock = pXtra->bsXA[x].bsx_vd_blk;
			 page = (pXtra->bsXA[x].bsx_fob_offset * ADVFS_FOB_SZ)/8;
			 size = (pXtra->bsXA[x+1].bsx_fob_offset - 
				 pXtra->bsXA[x].bsx_fob_offset) * ADVFS_FOB_SZ;

			 if (NULL == tag->extentHead) 
			 {
			     /* 
			      * Create first extent of tag 
			      */
			     status = create_extent_element(&tag->extentHead,
							    volume, diskBlock,
							    size, page);
			     if (SUCCESS != status)
			     {
				 return status;
			     }

			     tag->extentTail  = tag->extentHead;
			     tag->bytesFound += size;
			 } 
			 else 
			 {
			     /* 
			      * Create all additional extents 
			      */
			     currentExtent = tag->extentTail;
			     status = create_extent_element(&currentExtent->next,
							    volume, diskBlock,
							    size, page);
			     if (SUCCESS != status)
			     {
				 return status;
			     }

			     tag->extentTail  = currentExtent->next;
			     tag->bytesFound += size;
			 }
		     } /* end for loop */
		     break;

		 default:
		     break;
	     } /* end switch */
	     pRecord = (bsMRT *) (((char *)pRecord) + 
				  roundup(pRecord->bCnt, sizeof(uint64_t))); 
	} /* end while loop on records */
	/* 
	 * Find next mcell
	 */
	if (SUCCESS != find_next_mcell(domain, &current, &pMcell, 
				       &mcellPage, FALSE)) 
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_73, 
                     "find_next_mcell() failed\n"));
	    return FAILURE;
	}
    } /* end while loop*/

    return SUCCESS;
} /* end add_extents_to_tag */


/*
 * Function Name: p2_add_extents_to_tag
 *
 * Description:
 *  This is the Pass 2 version of add_extents_to_tag, it needs to
 *  handle the situation were the extent record have already been
 *  found.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  tag: A pointer to a tag, which these extents belong to. 
 *  pdata: A pointer to a data buffer, used to find this tag's extents. 
 *  type: The extent record type.
 *  volume: The volume this extent record is for.
 *
 * Returns:
 *  Status value - SUCCESS, INVALID or NO_MEMORY. 
 *
 * Side Effects:
 *  The extent structures will be added to the passed in tag.  
 */
int p2_add_extents_to_tag (domainInfoT *domain,
			   filesetTagT *tag,
			   void        *pData,
			   int         type,
			   int         volume)
{
    char          *funcName = "p2_add_extents_to_tag";
    int           status;
    bf_vd_blk_t   diskBlock;
    int64_t       size;
    bs_meta_page_t page;
    extentLLT     *currentExtent;
    bsXtntRT      *pXtnt;
    bsXtraXtntRT  *pXtra;
    int           x;

    switch (type) 
    {
      case BSR_XTNTS:
	pXtnt = (bsXtntRT *)pData;

	for (x = 0; ((x < pXtnt->xCnt - 1) && (x < BMT_XTNTS)); x++) 
	{
	    /*
	     * Normally One extent in this record type.
	     */
	    diskBlock = pXtnt->bsXA[x].bsx_vd_blk;
	    page = (pXtnt->bsXA[x].bsx_fob_offset * ADVFS_FOB_SZ)/8;
	    size = (pXtnt->bsXA[x+1].bsx_fob_offset - 
		    pXtnt->bsXA[x].bsx_fob_offset) * ADVFS_FOB_SZ;

	    if (NULL == tag->extentHead) 
	    {
		/* 
		 * Create first extent of tag 
		 */
		status = create_extent_element(&tag->extentHead, volume, 
						diskBlock, size, page);
		if (SUCCESS != status)
		{
		    return status;
		}

		tag->extentTail  = tag->extentHead;
		tag->bytesFound += size;
		} 
	    else 
	    {
		int duplicate = FALSE;
		/*
		 * Loop through all this tags extents.
		 */
		currentExtent = tag->extentHead;
		while (NULL != currentExtent)
		{
		    if ((currentExtent->volume == volume) &&
			(currentExtent->diskBlock == diskBlock) &&
			(currentExtent->extentSize == size) &&
			(currentExtent->byteInFile == page))
		    {
			duplicate = TRUE;
			break;
		    }
		    currentExtent =  currentExtent->next;
		}
	    
		if (FALSE == duplicate)
		{
		    /* 
		     * Create additional extents 
		     */
		    currentExtent = tag->extentTail;
		    status = create_extent_element(&currentExtent->next, 
						    volume, diskBlock, 
						    size, page);
		    if (SUCCESS != status)
		    {
			return status;
		    }
		    tag->extentTail  = currentExtent->next;
		    tag->bytesFound += size;
		}
	    }
	}
	break;

      case BSR_XTRA_XTNTS:
	pXtra = (bsXtraXtntRT *)pData;

	/*
	 * Loop through extents, skip last one
	 */
	for (x = 0; 
	     ((x < pXtra->xCnt -1) && (x < BMT_XTRA_XTNTS));
	     x++) 
	{
	    diskBlock = pXtra->bsXA[x].bsx_vd_blk;
	    page = (pXtra->bsXA[x].bsx_fob_offset * ADVFS_FOB_SZ)/8;
	    size = (pXtra->bsXA[x+1].bsx_fob_offset - 
		    pXtra->bsXA[x].bsx_fob_offset) * ADVFS_FOB_SZ;

	    if (NULL == tag->extentHead) 
	    {
		/* 
		 * Create first extent of tag 
		 */
	        status = create_extent_element(&tag->extentHead, volume,
					       diskBlock, size, page);
		if (SUCCESS != status)
		{
		    return status;
		}

		tag->extentTail  = tag->extentHead;
		tag->bytesFound += size;
	    } 
	    else 
	    {
	        int duplicate = FALSE;
	        /*
		 * Loop through all this tags extents.
		 */
	        currentExtent = tag->extentHead;
		while (NULL != currentExtent)
		{
		    if ((currentExtent->volume == volume) &&
			(currentExtent->diskBlock == diskBlock) &&
			(currentExtent->extentSize == size) &&
			(currentExtent->byteInFile == page))
		    {
			duplicate = TRUE;
			break;
		    }
		    currentExtent =  currentExtent->next;
		}
		
		if (FALSE == duplicate)
		{
		    /* 
		     * Create additional extents 
		     */
		    currentExtent = tag->extentTail;
		    status = create_extent_element(&currentExtent->next, 
						   volume, diskBlock, 
						   size, page);
		    if (SUCCESS != status)
		    {
			return status;
		    }
		    
		    tag->extentTail  = currentExtent->next;
		    tag->bytesFound += size;
		}
	    }
	} /* end for loop */
	break;

      default:
	break;
      } /* end switch */
    return SUCCESS;
} /* end p2_add_extents_to_tag */


/*
 * Function Name: sort_and_fill_extents
 *
 * Description:
 *  This function scans the extent list for a tag in the tag array,
 *  and sorts the entries according to the sequential order of the file
 *  data. If any holes are found which are due to missing extents,
 *  extend records are created to fill those holes, and the new extent
 *  records are flagged as such.
 *
 * Input parameters:
 *  tag: Pointer to an entry in the Tag Array. 
 *
 * Returns:
 *  status value - SUCCESS, FAILURE or NO_MEMORY. 
 */
int sort_and_fill_extents (filesetTagT *tag)
{
    char *funcName = "sort_and_fill_extents";
    extentLLT *pCurr = NULL;
    int64_t totalCalcSize = 0;
    int64_t holeSize = 0;
    int status = 0;
    int firstFlag = 0;
    int unusedEndSpace = 0;
    int foundAHole = 0;
    int64_t nExtentRecs = 0;
    int needsSort = 0;
    int64_t lastOffset = -1;
    
    /*
     * If the tag's extent status == EMPTY, there is nothing to sort.
     */
    if (S_IS_EMPTY(tag->status) || tag->extentHead == NULL)
    {
        return SUCCESS;
    }

    /*
     * If the tag is already marked complete and sorted, don't do it again.
     */
    if (S_IS_EXT_SORTED(tag->status))
    {
        return SUCCESS;
    }

    /*
     * Check the extent record list to see if they need sorting.
     * Count them as you go - this will tell the list sorting function
     * whether it needs to do the count.
     */
    pCurr = tag->extentHead;
    while (pCurr != NULL)
    {
        nExtentRecs++;

        if (pCurr->byteInFile < lastOffset)
        {
            needsSort = 1;        /* Don't break - keep counting till end */
        }

        lastOffset = pCurr->byteInFile;
        pCurr = pCurr->next;
    }

    /*
     * If needed, sort the extent records, by the byteInFile field.
     * Then, reset the extent tail pointer to the end.
     */
    if (needsSort == 1)
    {
        status = sort_linked_list( (void *)&tag->extentHead, 
                                   LISTSORT_EXTENT_OFFSET, nExtentRecs );
        if (status != SUCCESS)
        {
            return FAILURE;
        }

	pCurr = tag->extentHead;
        while (pCurr != NULL && pCurr->next != NULL) 
        {
            pCurr = pCurr->next;
        }
        tag->extentTail = pCurr;
    }

    /*
     * If the first extent's byteInFile is greater than 0, then there is an
     * extent missing at the beginning of the file. Fill it, then increment
     * the total size, and set the current extent pointer to the extent AFTER 
     * the new one (i.e., the record after the new hole, the old "1st" rec).
     */
    pCurr = tag->extentHead;

    if (pCurr->byteInFile > 0)
    {
        holeSize = tag->extentHead->byteInFile;
        firstFlag = 1;
        status = fill_extent_hole(tag, pCurr, holeSize, firstFlag);
        if (SUCCESS != status)
        {
            return status;
	}

        foundAHole = 1;
        totalCalcSize += holeSize;
        pCurr = tag->extentHead->next;        
    }

    /*
     * For each extent (not including the last extent), see if there is a
     * hole between the current extent's offset + size and the next extent's
     * offset. If we find a hole, fill it. Increment the total size found,
     * and advance the record pointer (i.e., advance to the new hole. The
     * loop will then (unconditionally) advance to the next record).
     * 
     */
    while (pCurr != NULL)
    {
        int64_t currExtentEnd = 0;

        totalCalcSize += pCurr->extentSize;

        if (pCurr != tag->extentTail)
        {
            currExtentEnd = pCurr->byteInFile + pCurr->extentSize;
            holeSize = pCurr->next->byteInFile - currExtentEnd;

            if (holeSize > 0)
            {
                firstFlag = 0;
                status = fill_extent_hole(tag, pCurr, holeSize, firstFlag);
                if (SUCCESS != status)
                {
                    return status;
                }
    
                foundAHole = 1;
                totalCalcSize += holeSize;
                pCurr = pCurr->next;
            }
            else if (holeSize < 0)
	    {
		/*
		 * Extents overlap. In a future release, we will make decisions
		 * on how to handle this, e.g., throw one away? Make 2 files?
		 * etc.
		 */
            }
        }

        pCurr = pCurr->next;
    }

    /*
     * If no attribute record for tag, we are done - we cannot perform 
     * checks based on attribute size vs found size.
     */
    if (tag->attrs == NULL)
    {
        return SUCCESS;
    }

    /*
     * If the tag's attribute size is greater than the size we have found
     * so far, then one or more extents must be missing at the end. Make
     * a "missing hole" extent to make up the difference.
     */
    if (tag->attrs->size > totalCalcSize)
    {

        pCurr = tag->extentTail;
        holeSize = tag->attrs->size - totalCalcSize;
        firstFlag = 0;
        status = fill_extent_hole(tag, pCurr, holeSize, firstFlag);
        if (SUCCESS != status)
        {
            return status;
        }
        
        foundAHole = 1;
        totalCalcSize += holeSize;
        pCurr = pCurr->next;
        tag->extentTail = pCurr;
    }
    
    /*
     * Check the size consistency for the tag. Note: totalCalcSize is in 
     * bytes, and is incremented by the size of the extent records. The tag's 
     * size attibute in the attributes record is the only source for the
     * actual EXACT size of the file.
     */
    if (totalCalcSize == tag->attrs->size)
    {
        if (foundAHole == 1)
        {
            S_SET_PARTIAL(tag->status);
        }
        else
        {
            S_SET_COMPLETE(tag->status);
            S_SET_EXT_SORTED(tag->status);
        }
        return SUCCESS;
    }

    unusedEndSpace = totalCalcSize - tag->attrs->size;

    /*
     * If the unusedEndSpace represents a partially used last page(i.e., 
     * the totalCalcSize is greater than the tag's size attribute by 
     * less than 1 page) then we simply adjust the size of the last extent 
     * to reflect the actual size. Otherwise, the file is more than 1 page
     * bigger than expected.
     */
    if (unusedEndSpace < ADVFS_METADATA_PGSZ)
    {
        int64_t lastOffset = tag->extentTail->byteInFile;

        tag->extentTail->extentSize -= unusedEndSpace;
        tag->bytesFound -= unusedEndSpace;        

        if (foundAHole == 1)
        {
            S_SET_PARTIAL(tag->status);
        }
        else
        {
            S_SET_COMPLETE(tag->status);
        }
    } else {
        tagNumT tagNum = tag->firstInstance->tagNum;

        if (foundAHole == 1)
        {
            S_SET_PARTIAL(tag->status);
        }
        else
        {
            S_SET_MORE(tag->status);
        }

        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_97, 
                 "extent data for tag %ld greater than expected\n"), tagNum);
    }

    /*
     * If tag is complete, mark it so we won't spend the cycles to sort it
     * again.
     */
    if (S_IS_COMPLETE(tag->status))
    {
        S_SET_EXT_SORTED(tag->status);
    }

    return SUCCESS;
}


/*
 * Function Name: fill_extent_hole
 *
 * Description:
 *  This function inserts a new extent record where a missing hole has
 *  been found.
 *
 * Input parameters:
 *  pTag: Pointer to the fileset tag.
 *  pCurr: Pointer to the current extent record.
 *  holeSize: Size of the hole to be filled, in bytes. 
 *  firstFlag: Flag to known whcih code path to use.
 *
 * Side effects:
 *  pCurr->next is updated.
 *  pTag->extentHead is updated.
 *
 * Returns:
 *  status value - SUCCESS, FAILURE or NO_MEMORY
 */
int fill_extent_hole (filesetTagT *pTag,
		      extentLLT   *pCurr,
		      int64_t     holeSize,
		      int64_t     firstFlag)
{
    char *funcName = "fill_extent_hole";
    extentLLT *pNew = NULL;
    int64_t byteInFile = 0;
    int64_t extentSize = 0;
    int status = 0;

    /*
     * If the hole is in the beginning of the list, handle it differently,
     * i.e., it starts at byte 0, and its length = the byte offset of the
     * next (pCurr) record.
     */
    if (firstFlag == 1)
    {
        byteInFile = 0;
        extentSize = pCurr->byteInFile;
    }
    else
    {
        byteInFile = pCurr->byteInFile + pCurr->extentSize;
        extentSize = holeSize;
    }

    /*
     * Create the extent record, using -2 in the volume and disk block
     * fields, to flag this extent as a "missing hole".
     */
    status = create_extent_element(&pNew, -2, -2, extentSize, byteInFile);
    if (SUCCESS != status)
    {
	return status;
    }

    /*
     * If the hole is in the beginning of the list, insert the new record
     * at the list head, else insert it after the current extent.
     */
    if (firstFlag == 1)
    {
        pNew->next = pTag->extentHead;
        pTag->extentHead = pNew;
    }
    else
    {
        pNew->next = pCurr->next;
        pCurr->next = pNew;
    }

    return SUCCESS;
}


/*
 * Function Name: trim_requested_pathname
 *
 * Description:
 *   This function is called when restoration of a specific pathname is 
 *   requested on the command line. The top level "child files" of the 
 *   fileset top directory are checked to determine which one (if any) 
 *   match the requested path, and all but the matching entry are marked
 *   IGNORE.
 *
 * Input parameters:
 *  fileset: The fileset this tag belongs to. 
 *  extentBuf: Array of extent entries for the fileset tag file.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */
int trim_requested_pathname(filesetLLT *fileset,
			    extentT    *extentBuf)
{
    char *funcName = "trim_requested_pathname";
    int status = 0;
    filesetTreeNodeT *pNode = NULL;
    fsDirDataT currDirData;
    fs_dir_entry dirEnt;

    /*
     * Pathname is NOT a tag number.
     * Get the names and tag numbers for the file entries directly under
     * tag 2.
     */
    bzero((void *)&currDirData, sizeof(fsDirDataT));
    currDirData.fileset = fileset;
    currDirData.volumes = fileset->domain->volumes;
    currDirData.dirTagNum = 2;

    /*
     * Loop thru the directory data buffer, while return = valid dir entry
     * record.
     */
    while ((status = get_next_direntry(&currDirData, &dirEnt)) == SUCCESS)
    {
        char *fileName  = dirEnt.fs_dir_name_string;
        int nameLen     = dirEnt.fs_dir_namecount;
        tagNumT tagNum = dirEnt.fs_dir_bs_tag_num;
        char namBuf[NAMEMAX+1];
        int isInPath = 0;

        if (strcmp(fileName, ".") == 0 || strcmp(fileName, "..") == 0)
        {
            continue;
        }

        /*
         * Copy (non-null-terminated) name into null-terminated buffer.
         */
        bzero(namBuf, sizeof(namBuf));
        strncpy(namBuf, fileName, nameLen);
        if (SUCCESS != check_name_in_path(namBuf, Options.pathName,
					  &isInPath ))
        {
            return FAILURE;
        }

        /*
         * If this one *is* in the requested path, leave it NULL, then its
         * creation wil be allowed later in the main loop. Otherwise, mark
         * it ignore.
         */
        if (isInPath == 0)
        {
            fileset->tagArray[tagNum] = IGNORE;
        }
    }

    if (status == NO_MORE_ENTRIES)
    {
        return SUCCESS;
    }
    else
    {
        return status;
    }
}



/*
 * Function Name: trim_requested_tagnum
 *
 * Description:
 *   This function is called when restoration of a specific file is 
 *   requested on the command line, by tag number.
 *
 * Input parameters:
 *  fileset: The fileset this tag belongs to. 
 *  tagfileExtentArray: Array of extent entries for the fileset tag file.
 *  arraySize: Number of extent recs in tagfileExtentArray.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

#define NOTIFY_CANT_RECOVER_BY_TAGNUM \
    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_98, \
             "Recovery by specific tag number not possible. File system/snapshot may\n")); \
    writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_99, \
         "    still be recoverable in its entirety. If this is desired,\n")); \
    writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_100, \
             "    reissue command without the specific tag number option.\n"));

#define NOTIFY_DIR_READ_ERROR \
    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_101, \
             "Error reading directory tag %ld, for target tag %ld (%s).\n"), \
            currTagNum, tagToFind, fileset->fsName );

#define NOTIFY_PARTIAL_LOSTFOUND \
    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_102, \
             "Target tag's hierarchy partially restored in lost+found. Additional\n") ); \
    writemsg(SV_ERR | SV_CONT, catgets(mcat, S_SALVAGE_1, SALVAGE_103, \
            "    files, other than those requested, may also be restored.\n"));

int trim_requested_tagnum(filesetLLT *fileset, 
			  extentT    *tagfileExtArray,
			  int64_t    arraySize )
{
    char      *funcName = "trim_requested_tagnum";
    tagNumT   tagToFind;
    tagNumT   currTagNum;
    tagNumT   parentTagNum;
    char      *pTagNumStr;
    int       status;
    int       isComplete = 0;
    filesetTreeNodeT *pNewNode;
    filesetTreeNodeT *pPrevNode;
    filesetTreeNodeT *pCurrNode;

    /*
     * Convert passed-in string into tag number.
     */
    pTagNumStr = strstr(Options.pathName, "/");
    pTagNumStr++;
    tagToFind  = atoi(pTagNumStr);

    /*
     * Check the tag number for validity.
     */
    if (tagToFind < 3 || tagToFind >= fileset->tagArraySize)
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_104, 
		 "Tag number %ld outside valid range, file system/snapshot '%s'\n"), 
		 tagToFind, fileset->fsName);
        return FAILURE;
    }

    /*
     * Load the tag (and the tree node into lost+found) for our tag number.
     */
    status = load_tag_from_tagnum(fileset, tagToFind, 
				  tagfileExtArray, arraySize);
    if (status == FAILURE || status == NO_MEMORY)
    {
        NOTIFY_CANT_RECOVER_BY_TAGNUM;
        return FAILURE;
    }
    else if (status == INVALID)
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_105, 
		 "Cannot find tag %ld, file system/snapshot '%s'\n"), tagToFind,
		 fileset->fsName);
        NOTIFY_CANT_RECOVER_BY_TAGNUM;
        return FAILURE;
    }
    else if (status == PARTIAL)
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_106, 
	         "Failed to get complete parent tag data for tag %ld, file system/snapshot '%s'\n"),
		 tagToFind, fileset->fsName);
        NOTIFY_PARTIAL_LOSTFOUND;
        return SUCCESS;                           
    }

    /*
     * If chosen tag is not a directory, we simply set the completion flag, 
     * since there is nothing left to do, other than insert_filenames(). This
     * tells build_tree() that we are done. This will be the case even if we
     * fail in the next step, which is to build the target node's tree from
     * the bottom up, since a failure will leave the partially built tree
     * in lost+found.
     */
    pNewNode = fileset->tagArray[tagToFind]->firstInstance;

    if (pNewNode->fileType != FT_DIRECTORY)
    {
        D_SET_PASS1_COMPLETED(fileset->domain->status);
        isComplete = 1;
    }

    /*
     * Start with the "tagToFind"'s parent tag, loop till we get to tag 2.
     * Get the parent tag, load it, and insert it in the tree as the parent 
     * (i.e., "on top") of the previous tag.
     */
    currTagNum = pNewNode->parentTagNum;
    pPrevNode = pNewNode;

    while (currTagNum != 2)
    {
        status = load_tag_from_tagnum(fileset, currTagNum, 
                                      tagfileExtArray, arraySize);
        if (status == FAILURE || status == NO_MEMORY)
        {
            NOTIFY_CANT_RECOVER_BY_TAGNUM;
            return FAILURE;
        }
        else if (status == PARTIAL)
        {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_107, 
                 "Failed to get parent tag data for tag %ld, file system/snapshot '%s'\n"),
		 currTagNum, fileset->fsName);
            NOTIFY_PARTIAL_LOSTFOUND;
            return SUCCESS;                           
        }

        pCurrNode = fileset->tagArray[currTagNum]->firstInstance;

        if ((pCurrNode->fileType == FT_UNKNOWN) || 
	    (pCurrNode->fileType == FT_DEFAULT))
        {
            pCurrNode->fileType = FT_DIRECTORY;
            fileset->tagArray[currTagNum]->fileType = FT_DIRECTORY;
        }

        if (pCurrNode->fileType == FT_DIRECTORY)
        {
            move_node_in_tree(pPrevNode, pCurrNode);
        }
        else
	{
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_108, 
		   "Tag %ld incorrect filetype, in target tag %ld hierarchy,\n"),
		     currTagNum, tagToFind);
            writemsg(SV_DEBUG | SV_CONT, 
                     catgets(mcat, S_SALVAGE_1, SALVAGE_109, 
                     "    file system/snapshot '%s'\n"), fileset->fsName);
	    NOTIFY_PARTIAL_LOSTFOUND;
            return SUCCESS;
        }

        /*
         * If our target node is NOT a directory, build_tree() will continue,
         * so mark all siblings of our current target node to IGNORE.
         * If this fails, just stop - build_tree() will end up restoring
         * more than what we want, but this is not a failure.
         */
        if (1 != isComplete)
        {
            status = prune_siblings(fileset, pCurrNode, pPrevNode,
				    tagfileExtArray, arraySize);
            if (status != SUCCESS)
            {
                NOTIFY_DIR_READ_ERROR;
                NOTIFY_PARTIAL_LOSTFOUND;
                return SUCCESS;
            }
        }

        currTagNum = pCurrNode->parentTagNum;
        pPrevNode = pCurrNode;
    }

    /*
     * Made it all the way to the top without problems.
     */
    if (currTagNum == 2)
    {
        move_node_in_tree(pPrevNode, fileset->dirTreeHead);

        if (1 != isComplete)
        {
            status = prune_siblings(fileset, fileset->dirTreeHead, pPrevNode,
				    tagfileExtArray, arraySize);
            if (status != SUCCESS)
            {
                NOTIFY_DIR_READ_ERROR;
                NOTIFY_PARTIAL_LOSTFOUND;
                return SUCCESS;
            }
        }
    }

    return SUCCESS;
}


/*
 * Function Name: load_tag_from_tagnum
 *
 * Description:
 *   This function encapsulates a series of building steps for a tag, 
 *   including locating a tag's primary mcell, loading the tag structure
 *   into the tag array, and creating a "first instance" node in the tree.
 *
 * Input parameters:
 *  fileset: The fileset this tag belongs to. 
 *  tagNum: Tag number for the new tag structure to be built.
 *  tagfileExtentArray: Array of extent entries for the fileset tag file.
 *  arraySize: Number of extent recs in tagfileExtentArray.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, PARTIAL, or INVALID.
 */
int load_tag_from_tagnum(filesetLLT *fileset,
			 tagNumT    tagNum,
			 extentT    *tagfileExtArray,
			 int64_t    arraySize)
{
    char      *funcName = "load_tag_from_tagnum";
    int       status;
    bfMCIdT   primMcell;
    tagNumT   parentTagNum;
    filesetTreeNodeT *pNewNode;
    filesetTreeNodeT *headLostFound = fileset->dirTreeHead->nextSibling;

    /*
     * Check the tag number for validity.
     */
    if (tagNum < 3 || tagNum >= fileset->tagArraySize)
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_104, 
                 "Tag number %ld outside valid range, file system/snapshot '%s'\n"), 
		 tagNum, fileset->fsName);
        return FAILURE;
    }

    /*
     * Find the location for the requested tag's primary mcell.
     */
    status = find_tag_primary_mcell_location(fileset, tagNum, tagfileExtArray,
					     arraySize, &primMcell);
    if (status != SUCCESS)
    {
        return status;
    }

    /*
     * Load the tag's metadata into the tag array. If this returns PARTIAL,
     * then find_next_mcell() failed. This means some mcells for this tag
     * weren't found, which means we may be missing attributes, or extents,
     * etc. In building our target tag's node hierarchy, we need the parent
     * tag number to continue, which is initialized to -1 in the
     * load_tag_from_mcell(). If this is still -1, we have to stop here.
     * If extents are missing, we continue, although this might result in
     * missing names, and some pruning (if necessary) won't get done.
     */
    status = load_tag_from_mcell(fileset->domain, fileset, tagNum, primMcell, 
				 &parentTagNum);
    if (status == FAILURE || status == NO_MEMORY)
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_110, 
                 "load_tag_from_mcell() failed\n"));
        return status;
    }
    else if (status == PARTIAL)
    {
        if (parentTagNum == MISSING_PARENT)
        {
            return PARTIAL;
        }
    }

    /*
     * Add the node for the tag at the top of lost+found.
     */
    status = add_new_tree_node(fileset, tagNum, parentTagNum, NULL, 0,
			       headLostFound, &pNewNode);
    if (status != SUCCESS)
    {
        return FAILURE;
    }

    fileset->tagArray[tagNum]->firstInstance = pNewNode;

    return SUCCESS;
}


/*
 * Function Name: find_tag_primary_mcell_location
 *
 * Description:
 *   This function locates the primary mcell for a tag number.
 *
 * Input parameters:
 *  fileset: The fileset this tag belongs to. 
 *  tagToFind: Tag number whose primary mcell we wish to find.
 *  tagfileExtentArray: Array of extent entries for the fileset tag file.
 *  arraySize: Number of extent recs in tagfileExtentArray.
 *
 * Output parameters:
 *  primMcell: Mcell location for the tag's primary mcell.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, or INVALID.
 */

int find_tag_primary_mcell_location(filesetLLT *fileset,
				    tagNumT    tagToFind, 
				    extentT    *tagfileExtArray,
				    int64_t    arraySize,
				    bfMCIdT    *primMcell)
{
    char      *funcName = "find_tag_primary_mcell_location";
    tagNumT   tagEntryOnPage;
    char      *pTagNumStr;
    int       status;
    bs_meta_page_t pageNum;
    int       volNum;
    int       volFd;
    bf_vd_blk_t lbn;
    bsTDirPgT tagPage;
    bsTMapT   *tagEntry;

    /*
     * Find the page in the tagfile where our tag's entry would be,
     * 0-1021 on 1st page, 1022-2043 on 2nd page, etc.
     */
    pageNum = tagToFind / BS_TD_TAGS_PG;
    tagEntryOnPage = tagToFind % BS_TD_TAGS_PG;
    
    /*
     * Get the location for the page, read the tag page, and index to the 
     * right entry to get the tag's primary mcell location.
     */
    status = convert_page_to_lbn(tagfileExtArray, arraySize, pageNum, &lbn, 
				 &volNum);
    if (status != SUCCESS)
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_111, 
                 "Cannot convert page %ld to lbn, file system/snapshot '%s'\n"), 
		 pageNum, fileset->fsName);
	return FAILURE;
    }
    volFd = fileset->domain->volumes[volNum].volFd;

    status = read_page_by_lbn(volFd, &tagPage, lbn, NULL);
    if (status != SUCCESS)
    {
	return FAILURE;
    }

    tagEntry = &(tagPage.tMapA[tagEntryOnPage]);

    /*
     * Check to make sure the tag is actually in use.
     */
    if (!(tagEntry->tm_u.tm_s3.flags & BS_TD_IN_USE))
    {
        writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_112, 
                 "Tag %ld is not in use, file system/snapshot '%s'\n"), tagToFind,
                 fileset->fsName );
        return INVALID;
    }

    /*
     * Set the values for the tag's primary mcell.
     */
    primMcell->volume = tagEntry->tm_u.tm_s3.bfMCId.volume;
    primMcell->page   = tagEntry->tm_u.tm_s3.bfMCId.page;
    primMcell->cell   = tagEntry->tm_u.tm_s3.bfMCId.cell;

    return SUCCESS;
}


/*
 * Function Name: prune_siblings
 *
 * Description:
 *  This function marks all siblings of a node we wish to save as IGNORE.
 *
 * Input parameters:
 *  fileset: The fileset we are currently working on.
 *  pDirNode: Pointer to a directory node in the tree containing the child
 *            node we wish to save.
 *  pSaveNode: Pointer to a tree node we wish to save.
 *  tagfileExtentArray: Array of extent entries for the fileset tag file.
 *  arraySize: Number of extent recs in tagfileExtentArray.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */
int prune_siblings(filesetLLT       *fileset,
		   filesetTreeNodeT *pDirNode,
		   filesetTreeNodeT *pSaveNode,
		   extentT          *tagfileExtArray,
		   int64_t          arraySize)
{
    char   *funcName = "prune_siblings";
    fsDirDataT currDirData;
    fs_dir_entry dirEnt;
    int status = 0;
    filesetTreeNodeT *pCurrNode;

    bzero((void *)&currDirData, sizeof(fsDirDataT));
    currDirData.fileset = fileset;
    currDirData.volumes = fileset->domain->volumes;
    currDirData.dirTagNum = pDirNode->tagNum;

    /*
     * Loop thru the directory data buffer, while return = valid dir entry
     * record.
     */
    while ((status = get_next_direntry(&currDirData, &dirEnt)) == SUCCESS)
    {
        /*
         * Get tmp variables for some stuff.
         */
        char *fileName  = dirEnt.fs_dir_name_string;
        tagNumT tagNum = dirEnt.fs_dir_bs_tag_num;
        
        if (strcmp(fileName, ".") == 0 || strcmp(fileName, "..") == 0)
        {    
            continue;
        }

        /*
         * Any entry with a tag number other than the child node we are 
         * saving gets marked as IGNORE.
         */
        if (tagNum != pSaveNode->tagNum)
        {
            fileset->tagArray[tagNum] = IGNORE;
        }
    }

    if (status == NO_MORE_ENTRIES)
    {
        return SUCCESS;
    }
    else
    {
        return status;
    }
}


/*
 * Function Name: delete_tag_array_entry
 *
 * Description:
 *   This function deletes an entry from the tag array. The problem is
 *   we could have hard links to this array entry, so we only delete it
 *   if all the hardlinks have been deleted. This is the same way UNIX
 *   handles files when a delete is issued, it only truly deletes the
 *   file if all links to the file have been deleted. We assume this is
 *   called on a valid entry.
 *
 * Input parameters:
 *  fileset: The fileset this tag belongs to. 
 *  tagNum: The tag we are deleting.
 *
 * Returns:
 *  Status value - SUCCESS.
 */
int delete_tag_array_entry(filesetLLT *fileset, 
			   tagNumT    tagNum)
{
    char         *funcName = "delete_tag_array_entry";
    extentLLT    *extent;
    extentLLT    *tmpExtent;
    filesetTagT  *pTag = fileset->tagArray[tagNum];

    /*
     * Decrement linksFound.
     */
    pTag->linksFound--;
    
    /*
     * Check linksFound attribute.  If we still have more than 1 known
     * link in the tree, we do nothing.  
     */

    if (pTag->linksFound > 0)
    {
        return SUCCESS;
    }

    /*
     * Free all of the extent records for this tag.
     */
    extent = pTag->extentHead;
    while (extent != NULL)
    {
        tmpExtent = extent;
        extent = extent->next;
        free(tmpExtent);
    }

    /*
     * Free the attributes for this tag.
     */
    if (pTag->attrs != NULL)
    {
        free(pTag->attrs);
    }

    /*
     * Free the ACL buffer for this tag.
     */
    if (pTag->acls != NULL) {
        free(pTag->acls);
        pTag->acls = NULL;
    }

    /*
     * Free the additional attributes for this tag.
     */
    if (pTag->addAttrs != NULL)
    {
        free(pTag->addAttrs);
    }

    /*
     * Free the array entry for this tag.
     */
    free(pTag);
    fileset->tagArray[tagNum] = IGNORE;

    return SUCCESS;
}


/*
 * Function Name: build_cleanup
 *
 * Description:
 *	
 * Go through all filesets which are to be recovered.
 * A large portion of this use to be in p2_build_tree
 * but was moved because it needs to done after all volumes
 * are recovered.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *
 * Returns:
 *  Status value - SUCCESS, NO_MEMORY
 */
int build_cleanup (domainInfoT *domain)
{
    char             *funcName = "build_cleanup";
    filesetLLT       *fileset;
    int              status;
    tagNumT          tagNum;
    filesetTagT      *tag = NULL;
    filesetTreeNodeT *child;
    filesetTreeNodeT *headLostFound;

    /*
     * If filesets exist.
     */
    if (NULL != domain->filesets)
    {
	/*
	 * Loop through all the filesets.
	 */
        writemsg(SV_NORMAL | SV_DATE, 
		 catgets(mcat, S_SALVAGE_1, SALVAGE_113, 
                 "Cleaning up memory structures:\n"));
		
	for (fileset = domain->filesets; 
	     fileset != NULL; 
	     fileset = fileset->next) 
	{
	    /*
	     * Is this a fileset we wish to restore?
	     */
	    if (FS_IS_RESTORE(fileset->status))
	    {
		filesetTreeNodeT *pNode = NULL;

		writemsg(SV_VERBOSE, 
			 catgets(mcat, S_SALVAGE_1, SALVAGE_114, 
                         "Cleaning up memory structures for '%s'\n"), 
			 fileset->fsName);

		/*
		 * Loop through all the tags in the array.  We are looking
		 * for tags which we haven't create a tree node for.  At 
		 * this point we have no clue what the parent tag number 
		 * is.
		 */
		for (tagNum = 2; tagNum < fileset->tagArraySize; tagNum++)
		{
		    tag = fileset->tagArray[tagNum];

		    /*
		     * Does this tag exist, and not have a first instance.
		     */
		    if (tag == NULL || tag == IGNORE || tag == DEAD)
		    {
			continue;
		    }

		    if (NULL == tag->firstInstance)
		    {
			filesetTreeNodeT *treeNode;
			
			status = create_unnamed_tree_node(&treeNode);
			if (SUCCESS != status)
			{
			    return status;
			}

			/*
			 * Load newly created treeNode with values.
			 */
			fileset->activeNodes++;
			tag->linksFound++;
			treeNode->fileType = tag->fileType;
			treeNode->tagNum = tagNum;
			tag->firstInstance = treeNode;
			treeNode->parentTagNum = MISSING_PARENT;

			/*
			 * Set headLostFound.
			 */
			headLostFound = fileset->dirTreeHead->nextSibling;

			/* 
			 * Add treeNode to lost and found list, 
			 * we will relink it later
			 */
			child                   = headLostFound->children;
			headLostFound->children = treeNode;
			treeNode->parent        = headLostFound;
			treeNode->nextSibling   = child ;
			treeNode->prevSibling   = NULL;
			if (NULL != child) {
			    child->prevSibling	= treeNode;
			}
		    } /* end if need to create node */
		} /* End loop through all tags */
		
		/*
		 * Now that we have created all these new nodes
		 * relink the lost and found.  We may have created
		 * parent nodes.
		 */
		status = relink_lost_found(fileset);
		if (SUCCESS != status)
		{
		    return status;
		}

	    } /* end if RESTORE this fileset */
	    FS_SET_PASS2_COMPLETED(fileset->status);
	} /* end fileset for loop */
    } /* end if any fileset exist */

    return SUCCESS;
}


/*
 * Function Name: p3_process_valid_page
 *
 * Description:
 *  Once a BMT page has been verified (by the caller), then this
 *  function will then parse the valid BMT page.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  BMTbuffer: A pointer to the current BMT page we are workin on.
 *  volID: The volume we are currently working on.
 *
 * Returns: SUCCESS, FAILURE or NO_MEMORY
 */
int p3_process_valid_page (domainInfoT *domain, 
			   bsMPgT      *BMTbuffer,
			   int         volId)
{
    char       *funcName = "p3_process_valid_page";
    int        mcell;
    int        status;
    bsMCT      *pMcell;
    bsMRT      *pRecord;
    void       *pData;
    filesetLLT *fileset;
    tagNumT    parentTagNum;

    /*
     * loop through all mcells in this BMT page.
     */
    for (mcell = 0; mcell < BSPG_CELLS; mcell++)
    {
	tagNumT  tag;
	int      seq;
	tagNumT  setTag;
	int      setSeq;
	tagNumT  fsTag;

        /*
	 * point to the current BMT mcell.
	 */
        pMcell = &(BMTbuffer->bsMCA[mcell]);

	tag    = pMcell->mcTag.tag_num;
	seq    = pMcell->mcTag.tag_seq;
	setTag = pMcell->mcBfSetTag.tag_num;
	setSeq = pMcell->mcBfSetTag.tag_seq;

	/*
	 * Special Case.  Reserved BMT mcell.
	 */
	if (setTag < 0)
	{
	    /*
	     * BMT mcell - Need to step through this mcell record 
	     */
	    pRecord = (bsMRT *)pMcell->bsMR0;

	    /*
	     * Now loop through records.
	     */
	    while ((pRecord->type != 0) &&
		   (pRecord->bCnt != 0) &&
		   (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) 
	    {
		bsBfSetAttrT  *pAttr;
		bsQuotaAttrT  *pQuota;

		pData = (char*)pRecord + sizeof(bsMRT);

		/* 
		 * Read records for this mcell:
		 *
		 * At this point we should only be reading records which deal
		 * with BMT related record.  If we hit records which should 
		 * not be here then we have data corruption. 
		 *
		 * Valid Types for reserve bitfile   : 3,8,18
		 * IGNORE Type                       : 1,2,4-6,15,17,21
		 * Invalid types for reserve bitfile : 7-14,16,19,20,22-255
		 *
		 */
		switch (pRecord->type) 
		{
		    case BSR_XTNTS:
		    case BSR_ATTR:
		    case BSR_DMN_ATTR:
		    case BSR_XTRA_XTNTS:
		    case BSR_DMN_MATTR:
		    case BSR_DMN_TRANS_ATTR:
		    case 17:

		    /*
		     * Ignore these mcell record types..
		     */
		    break;

		    case BSR_VD_ATTR:
		    /*
		     * We might be able to compute correct volume from this.
		     */
		    break;

		    case BSR_BFS_QUOTA_ATTR:

		    /*
		     * NOTE: we are using the tag to find the fileset.
		     */
		    status = find_fileset(domain, tag, tag, &fileset);
		    if (SUCCESS != status)
		    {
			if (INVALID == status)
			{
			    /*
			     * setTag belongs to a fileset we are not
			     * recovering.
			     */
			    break;
			}
			else
			{
			    return status;
			}
		    }

		    /* 
		     * Set up data buffer 
		     */
		    pQuota = (bsQuotaAttrT *)pData;
		    
		    fileset->quota = salvage_malloc(sizeof(struct filesetQuota));
		    if (NULL == fileset->quota) 
		    {
			writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, 
				 SALVAGE_68, "malloc() failed\n"));
			return NO_MEMORY;
		    }
			    
		    fileset->quota->hardBlockLimit = pQuota->blkHLimit;
		    fileset->quota->softBlockLimit = pQuota->blkSLimit;
		    fileset->quota->hardFileLimit = pQuota->fileHLimit;
		    fileset->quota->softFileLimit = pQuota->fileSLimit;
		    fileset->quota->blockTimeLimit = pQuota->blkTLimit;
		    fileset->quota->fileTimeLimit = pQuota->fileTLimit;

		    break;
		  
		    case BSR_BFS_ATTR:
		    /* 
		     * Set up data buffer 
		     */
		    pAttr = (bsBfSetAttrT *)pData;
		    
		    fsTag = pAttr->bfSetId.dirTag.tag_num;

		    /*
		     * NOTE: we are not using setTag to fileset.
		     */
		    status = find_fileset(domain, tag, fsTag, &fileset);
		    if (SUCCESS != status)
		    {
			if (INVALID == status)
			{
			    /*
			     * setTag belongs to a fileset we are not
			     * recovering.
			     */
			    break;
			}
			else
			{
			    return status;
			}
		    }

		    /*
		     * Found a snap fileset.
		     */
		    if (0 != pAttr->bfsaSnapLevel)
		    {
			FS_SET_SNAP(fileset->status);
			writemsg(SV_VERBOSE, catgets(mcat, S_SALVAGE_1, 
				 SALVAGE_87, "'%s' is a snapshot\n"),
				 pAttr->setName);
		    }

		    /*
		     * This fileset has a snap.  Let the user know.
		     */
                    if (!BS_BFS_EQL(pAttr->bfsaFirstChildSnapSet,nilMlBfSetIdT))
		    {
			FS_SET_HAS_SNAP(fileset->status);
			writemsg(SV_VERBOSE, catgets(mcat, S_SALVAGE_1, 
                                 SALVAGE_88, "'%s' has a snapshot\n"),
				 pAttr->setName );
		    }
			    
		    /*
		     * If we do not have the name for this fileset,
		     * then name it.  If we have a name assume it is correct.
		     */
		    if (NULL == fileset->fsName)
		    {
			writemsg(SV_VERBOSE, 
				 catgets(mcat, S_SALVAGE_1, SALVAGE_118, 
                                 "Naming file sytem/snapshot tag %ld to '%s'\n"),
				 fileset->filesetId.dirTag.tag_num, 
				 pAttr->setName);
			
			fileset->fsName = salvage_malloc(strlen(pAttr->setName)+1);
			if (NULL == fileset->fsName) 
			{
			    writemsg(SV_ERR, 
				     catgets(mcat, S_SALVAGE_1, SALVAGE_68,
				     "malloc() failed\n"));
			    return NO_MEMORY;
			}
			strcpy(fileset->fsName,pAttr->setName);
			
			/*
			 * Free names in tree.
			 */
			free(fileset->dirTreeHead->name);
			free(fileset->dirTreeHead->nextSibling->name);

			fileset->dirTreeHead->name = 
			  salvage_malloc(strlen(pAttr->setName)+1);
			if (NULL == fileset->dirTreeHead->name) 
			{
			    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, 
				     SALVAGE_68, "malloc() failed\n"));
			    return NO_MEMORY;
			}
			strcpy(fileset->dirTreeHead->name,pAttr->setName);

			fileset->dirTreeHead->nextSibling->name = 
			  salvage_malloc(strlen(pAttr->setName)+1);
			if (NULL == fileset->dirTreeHead->nextSibling->name) 
			{
			    writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, 
                                     SALVAGE_68, "malloc() failed\n"));
			    return NO_MEMORY;
			}
			strcpy(fileset->dirTreeHead->nextSibling->name,
			       pAttr->setName);

			fileset->filesetId.domainId.id_sec  = 
			  pAttr->bfSetId.domainId.id_sec;
			fileset->filesetId.domainId.id_usec = 
			  pAttr->bfSetId.domainId.id_usec;
			fileset->filesetId.dirTag.tag_num = 
			  pAttr->bfSetId.dirTag.tag_num;
			fileset->filesetId.dirTag.tag_seq = 
			  pAttr->bfSetId.dirTag.tag_seq;
		    }

		    /*
		     * Check to see if this is the fileset we need to recover.
		     * Make sure we check for NULL first.
		     */
		    if (NULL != Options.setName)
		    {
			/*
			 * Check to see if we are recovering by settag number.
			 */
			if ('#' == Options.setName[0])
			{
			    int64_t filesetNum;

			    filesetNum = atol(&(Options.setName[1]));

			    if (filesetNum != fileset->filesetId.dirTag.tag_num)
			    {
				/*
				 * Not a settag we wish to restore.
				 */
			        writemsg(SV_VERBOSE, catgets(mcat, 
					 S_SALVAGE_1, SALVAGE_119, 
					 "Skipping fileset '%s'\n"),
					 fileset->fsName);
				FS_SET_NOT_RESTORE(fileset->status);	  
			    }
			}
			else if	(strcmp(fileset->fsName, Options.setName) != 0)
			{
			    /*
			     * Not a fileset we are interested in
			     */
			    writemsg(SV_VERBOSE, catgets(mcat, S_SALVAGE_1,
				     SALVAGE_119, "Skipping fileset '%s'\n"),
				     fileset->fsName);
			    FS_SET_NOT_RESTORE(fileset->status);	  
			}
		    }
		    break;

		    default:
		    writemsg(SV_DEBUG, 
			     catgets(mcat, S_SALVAGE_1, SALVAGE_72, 
			     "Found an invalid record type - %d\n"),
			     pRecord->type);
		    break;
		} /* end switch */
		pRecord = (bsMRT *) (((char *)pRecord) + 
				     roundup(pRecord->bCnt, sizeof(uint64_t))); 
	    } /* end while */
	    continue;
	} /* end if  reserve bit file*/

	if ((0 == tag) && (0 == setTag) && (0 == seq) && (0 == setSeq))
	{
	    /*
	     * Free Mcell.
	     */
	    continue;
	}

	if ((tag < 1) || (0 == setTag) || (0 >= seq) || (setSeq < 0))
	{
	    /*
	     * Uninitialized mcell.
	     */
	    continue;
	}

	status = find_fileset(domain, tag, setTag, &fileset);
	if (SUCCESS != status)
	{
	    if (INVALID == status)
	    {
		/*
		 * setTag belongs to a fileset we are not recovering.
		 */
		continue;
	    }
	    else
	    {
		return status;
	    }
	}

	/*
	 * Check to see if we need to enlarge the tag array.
	 */
	if (tag >= fileset->tagArraySize)
	{
	    status = enlarge_tag_array(fileset, tag);
	    if (SUCCESS != status)
	    {
	        if (FAILURE != status)
		{
		    return status;
		}
		continue;
	    }
	}

	/* 
	 * Create and load an element in our array 
	 */
	status = p2_load_tag_from_mcell(domain, fileset, tag, pMcell, 
					&parentTagNum, volId);
	if (FAILURE == status)
	{
	    /*
	     * Failed to read tag, set to DEAD.
	     */
	    fileset->tagArray[tag] = DEAD;
	    continue;
	} 
	else if (NO_MEMORY == status)
	{
	    return status;
	}

	/*
	 * Does this tag still exist.
	 */
	if (fileset->tagArray[tag] == NULL || 
	    fileset->tagArray[tag] == IGNORE || 
	    fileset->tagArray[tag] == DEAD)
	{
	    continue;
	}

	/*
	 * Found the first instance of a tag
	 */
	if (NULL == fileset->tagArray[tag]->firstInstance)
	{
	    status = add_first_instance(fileset, tag, parentTagNum);
	    if (SUCCESS != status)
	    {
		return status;
	    }
	}
	else
	{
	    /*
	     * Node already exists.
	     */
	    if (parentTagNum != MISSING_PARENT) 
	    {
	        fileset->tagArray[tag]->firstInstance->parentTagNum = 
		  parentTagNum;
	    }

	    /*
	     * If File is not a directory and
	     * If we have attributes for this file and
	     * If this tag's mtime is older than recover date.
	     */
	    if ((FT_DIRECTORY != fileset->tagArray[tag]->fileType) &&
		(NULL != fileset->tagArray[tag]->attrs) &&
		(Options.recoverDate.tv_sec > 
		 fileset->tagArray[tag]->attrs->mtime.tv_sec))
	    {
		delete_node (&fileset->tagArray[tag]->firstInstance, fileset);
	    }
	}
    } /*end mcell loop */
    return SUCCESS;
} /* end p3_process_valid_page */


/*
 * Function Name: find_fileset
 *
 * Description:
 *  Locate the fileset we need to work on.  If it does not
 *  exist then create it.
 *
 * Input parameters:
 *  domain: The domain we are working on.
 *  tag: The tag we are working on, used to see if we should enlarge the
 *       tag array.
 *  setTag: The ID of the fileset we are looking for
 *
 * Output parameter:
 * foundFileset: A pointer to the fileset we are looking for.
 *
 * Returns: SUCCESS, FAILURE, INVALID or NO_MEMORY.
 *
 * Side Effects:
 *  Will increase the size of the tag array if needed.
 */
int find_fileset (domainInfoT *domain,
		  tagNumT     tag,
		  tagNumT     setTag,
		  filesetLLT  **foundFileset)
{
    char       *funcName = "find_fileset";
    int        status;
    filesetLLT *fileset;

    /*
     * Find the proper fileset.
     */
    fileset = domain->filesets;
    *foundFileset = NULL;

    /*
     * Do not find fileset on reserved bitfiles.
     */
    if (setTag < 0)
    {
	return INVALID;
    }

    while (NULL != fileset) 
    {
	if (setTag == fileset->filesetId.dirTag.tag_num)
	{
	    break;
	}
	else
	{
	    fileset = fileset->next;
	}
    } /* end while loop */

    /*
     * Check to see if this a fileset we should be restoring.
     */
    if ((NULL != fileset) && (FS_IS_NOT_RESTORE(fileset->status)))
    {
	return INVALID;
    }

    /*
     * If fileset is equal NULL we have found a new fileset.
     */
    if (NULL == fileset)
    {
	/* 
	 * Create fileset element 
	 */
        status = create_fileset_element(&fileset, domain);
        if (SUCCESS != status)
	{
	    return status;
	}

	writemsg(SV_VERBOSE, catgets(mcat, S_SALVAGE_1, SALVAGE_120, 
		 "Found a reference to a new file system/snapshot tag %ld\n"), setTag);

	fileset->filesetId.dirTag.tag_num = setTag;
	fileset->next    = domain->filesets;
	domain->filesets = fileset;
    }

    *foundFileset = fileset;

    /*
     * Check to see if we need to enlarge the tag array.
     */
    if (tag >= fileset->tagArraySize)
    {
        status = enlarge_tag_array(fileset, tag);
	if (SUCCESS != status)
	{
	    if (FAILURE == status)
	    {
	        if (1021 >= fileset->tagArraySize)
		{
		    /*
		     * enlarge to the minimum array size
		     */
		    status = enlarge_tag_array(fileset, 1021);
		    if (SUCCESS != status)
		    {
			return status;
		    }
		}
		else
		{
		    return INVALID;
		}
	    }
	}
    }

    if (NULL == fileset->dirTreeHead)
    {
	/* 
	 * Setup the node tree, AFTER tag array created.
	 */
        status = setup_fs_tree(fileset);
	if (SUCCESS != status)
	{
	    return status;
	}
    }
		    
    return SUCCESS;
} /* end find_fileset */


/*
 * Function Name: enlarge_tag_array
 *
 * Description:
 *   Enlarge the size of the tag array.
 *
 * Input parameters:
 *  fileset: The fileset we are working on.
 *  tag: The tag number which must fit in the new tag array.
 *
 * Returns: SUCCESS, FAILURE or NO_MEMORY.
 */
int enlarge_tag_array (filesetLLT  *fileset,
		       tagNumT     tag)
{
    char        *funcName = "enlarge_tag_array";
    int64_t     newSize;
    int64_t     oldSize;
    filesetTagT **oldArray; 

    newSize = (((tag / 1022) + 1) * 1022);
    oldSize  = fileset->tagArraySize;

    writemsg(SV_DEBUG, 
	     catgets(mcat, S_SALVAGE_1, SALVAGE_121, 
             "Trying to increase the size of '%s' tag array from %ld to %ld\n"),
	     fileset->fsName, oldSize, newSize);

    if (newSize > MAX_ADVFS_TAGS)
    {
	return FAILURE;
    }

    if (newSize > fileset->maxSize)
    {
        if (newSize >= Options.tagHardLimit)
	{
	    /*
	     * The user has requested we no longer enlarge the array.
	     */
	    return FAILURE;
	}

        /*
	 * This tag is to large, we can either increment fileset->maxSize
	 * and call realloc (which could fail).  Or not handle tags this 
	 * large.
	 */
        if (check_tag_array_size(fileset->maxSize, newSize))
	{
	    fileset->maxSize = newSize;
	}
	else
	{
	    Options.tagHardLimit = newSize;
	    return FAILURE;
	}
    }

    fileset->tagArray = 
      (filesetTagT **) salvage_realloc(fileset->tagArray,  
			      newSize * sizeof(filesetTagT *));
    if (NULL == fileset->tagArray) 
    {
	writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_122, 
                 "realloc() failed; Old size %ld New size %ld\n"), 
		 oldSize, newSize);

	return NO_MEMORY;
    }

    bzero((void *)&fileset->tagArray[oldSize],
	  (newSize - oldSize) * sizeof(filesetTagT *));

    fileset->tagArraySize = newSize;

    return SUCCESS;
} /* end enlarge_tag_array */



/*
 * Function Name: add_first_instance
 *
 * Description:
 *   Add the first instance of this tag to the tree.
 *
 * Input parameters:
 *   fileset: The fileset to add the first instance to.
 *   tag: The tag we are adding the tag to.
 *   parentTagNum: The parent tag number of the tag being added.
 *
 * Returns: SUCCESS, FAILURE or NO_MEMORY
 */
int add_first_instance(filesetLLT  *fileset,
		       tagNumT     tag,
		       tagNumT     parentTagNum)
{
    char             *funcName = "add_first_instance";
    int              status;
    filesetTreeNodeT *treeNode;
    filesetTreeNodeT *parent;
    filesetTreeNodeT *child;
    filesetTreeNodeT *headLostFound;

    /*
     * Check to see if this tag's parent exists AND
     * if this tag's parent is marked IGNORE AND
     * if this is this file is NOT a hard link.
     */
    if ((parentTagNum <= fileset->tagArraySize) &&
	(parentTagNum >= 0) &&
	(IGNORE == fileset->tagArray[parentTagNum]) && 
	(NULL != fileset->tagArray[tag]->attrs) &&
	(1 == fileset->tagArray[tag]->attrs->numLinks))
    {
	delete_tag_array_entry (fileset,tag);
	return SUCCESS;
    }

    /*
     * If File is not a directory and
     * If we have attributes for this file and
     * If this tag's mtime is older than recover date.
     */
    if ((FT_DIRECTORY != fileset->tagArray[tag]->fileType) &&
	(NULL != fileset->tagArray[tag]->attrs) &&
	(Options.recoverDate.tv_sec > 
	 fileset->tagArray[tag]->attrs->mtime.tv_sec))
    {
	delete_tag_array_entry (fileset, tag);
	return SUCCESS;
    }

    /*
     * Check to see if parentTagNumber is in the array.
     */
    if (parentTagNum >= fileset->tagArraySize)
    {
	status = enlarge_tag_array(fileset, parentTagNum);
	if (SUCCESS != status)
	{
	    if (FAILURE != status)
	    {
		return status;
	    }
	    parentTagNum = MISSING_PARENT;
	    writemsg(SV_DEBUG, catgets(mcat, S_SALVAGE_1, SALVAGE_123, 
                     "Setting tag %ld parent to MISSING\n"), tag);
	}
    }

    /* 
     * Create an element which will be added to the dir Tree.
     */
    status = create_unnamed_tree_node(&treeNode);
    if (SUCCESS != status)
    {
	return status;
    }
  
    /*
     * Load with values.
     */
    fileset->activeNodes++;
    fileset->tagArray[tag]->linksFound++;
    treeNode->fileType = fileset->tagArray[tag]->fileType;
    treeNode->parentTagNum = parentTagNum;
    treeNode->tagNum       = tag;
    fileset->tagArray[tag]->firstInstance = treeNode;

    /*
     * Set headLostFound.
     */
    headLostFound = fileset->dirTreeHead->nextSibling;

    /* 
     * Check to see if this entry's parent already exists 
     */
    if ((parentTagNum > 0) &&
	(parentTagNum <= fileset->tagArraySize) &&
	(NULL   != fileset->tagArray[parentTagNum]) && 
	(IGNORE != fileset->tagArray[parentTagNum]) &&
	(DEAD   != fileset->tagArray[parentTagNum]) &&
	(FT_DIRECTORY ==  fileset->tagArray[parentTagNum]->fileType) &&
	(NULL   != fileset->tagArray[parentTagNum]->firstInstance)) 
    {
	parent = fileset->tagArray[parentTagNum]->firstInstance;
	insert_node_in_tree(treeNode, parent);
    } 
    else 
    {
	/* 
	 * Add treeNode to lost and found list, relink it later.
	 */
	insert_node_in_tree(treeNode, headLostFound);   
    } /* end if parent exists*/
    return SUCCESS;
} 



/*
 * Function Name: validate_fileset
 *
 * Description:
 *   Check to see if the fileset has a name, if not name it.
 *
 * Input parameters:
 *   fileset: The fileset to check.
 *
 * Returns: SUCCESS or NO_MEMORY.
 */
int validate_fileset(filesetLLT  *fileset)
{
    char *funcName = "validate_fileset";

    /*
     * Name already exists.
     */
    if (NULL != fileset->fsName)
    {
	return SUCCESS;
    }

    fileset->fsName = salvage_malloc(strlen(fileset->dirTreeHead->name) + 1);
    if (NULL == fileset->fsName)
    {
        writemsg(SV_ERR, catgets(mcat, S_SALVAGE_1, SALVAGE_68, 
                 "malloc() failed\n"));
	return NO_MEMORY;
    }

    strcpy(fileset->fsName, fileset->dirTreeHead->name);

    return SUCCESS;
} /* end validate_fileset */

/* end salvage_build.c */
