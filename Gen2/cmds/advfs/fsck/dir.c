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

/*
 * Global
 */
extern nl_catd _m_catd;

/* private prototypes */

static int correct_dir_index(filesetT *fileset,
			     tagNodeT *dirTag);

static int correct_dirs_recurse(filesetT *fileset, 
				tagNodeT *tag, 
				char *path);

static int validate_dir_index_free_space(idxNodeT  *page,
					 int64_t   *currFreeOffset,
					 pageNumT  pageNum,
					 fobtoLBNT *dirXtnt,
					 xtntNumT  dirXtntSize);

static int validate_dir_index_page(idxNodeT  *page,
				   int       tree,
				   pageNumT  pageNum,
				   fobtoLBNT *idxXtnt,
				   xtntNumT  idxXtntSize);

static int validate_dir_index_leaf(idxNodeT  *page,
				   pageNumT  pageNum,
				   fobtoLBNT *dirXtnt,
				   xtntNumT  dirXtntSize);

static int move_lost_file(filesetT *fileset,
			  tagNodeT *tag);

static int clear_tag_index(filesetT *fileset,
			   tagNodeT *tagNode);

static int get_min_dirsize(fs_dir_entry *dir_p);


/*
 * Function Name: correct_all_dirs
 *
 * Description: This routine checks the directory hierarchy.
 *		It first calls correct_dirs_recurse with tag 2 (the top
 *		level (root) directory), then loops through and calls
 *		move_lost_file on each file not found in the first pass,
 *		then resets each file's numLinks field in its FSSTAT
 *		structure if necessary.
 *
 *		NOTE: This must be called after overlapping files have
 *			been deleted!
 *
 * Input parameters:
 *     tag: The tag of the directory to be searched down from.
 *
 * Output parameters:
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 * 
 */
int 
correct_all_dirs(filesetT *fileset)
{
    char	*funcName = "correct_all_dirs";
    tagNodeT	tmpTagNode;
    tagNodeT	*tagNode;
    tagNodeT	*nextNode;
    nodeT	*currNode;
    int		status;

    tmpTagNode.tagSeq.tag_num = BFM_BFSDIR; /* tagnum of fileset's root dir */
    tagNode = &tmpTagNode;

    status = find_node(fileset->tagList, (void **)&tagNode, &currNode);
    if (status == NOT_FOUND) {
	/*
	 * Didn't find correct tagNode - return failure.
	 */
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_197, 
			 "No root directory (tagnum %ld) for file system %s.\n"),
		 BFM_BFSDIR, fileset->fsName);
	return FAILURE;
    } else if (status != SUCCESS) {
	return status;
    }

    /*
     * Call correct_dirs_recurse with the root directory tag of this
     * fileset.  This will check the directory on-disk structure for
     * every directory found in the hierarchy, and increment
     * numLinksFound for every directory and file found.
     */
    status = correct_dirs_recurse(fileset, tagNode, ".");
    if (status != SUCCESS) {
	return status;
    }

    /*
     * First loop through the tag list looking for unlinked files.  Call
     * move_lost_file on every file with numLinksFound still at 0.  Then
     * do a second loop on all tags resetting numLinks to numLinksFound
     * if they differ.
     */

    /*
     * find the first tag node and walk the chain looking for lost files.
     */
    status = find_first_node(fileset->tagList, (void **)&tagNode, 
			     &currNode);
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
    
    while (tagNode != NULL) {
	/*
	 * Locate the next node first, in case we delete this node.
	 */
	status = find_next_node(fileset->tagList, (void **)&nextNode, 
				&currNode);
	if (status != SUCCESS) {
	    if (status == NOT_FOUND) {
		nextNode = NULL;
	    } else {
		return status;
	    }
	}

	if (tagNode->numLinksFound == 0) {
	    status = move_lost_file(fileset, tagNode);
	    if (status != SUCCESS) {
		return status;
	    }
	} /* End if no links found */

	tagNode = nextNode;
    } /* end next tag loop */

    /*
     * Don't correct numLinks on snapshots - nothing else to do in
     * directories, so return if this is a snapshot.
     */
    if (FS_IS_SNAP(fileset->status)) {
	return SUCCESS;
    }

    /*
     * find the first tag node and walk the chain, correcting numLinks.
     */
    status = find_first_node(fileset->tagList, (void **)&tagNode, 
			     &currNode);
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
    
    while (tagNode != NULL) {
	/*
	 * Locate the next node first, in case we delete this node.
	 */
	status = find_next_node(fileset->tagList, (void **)&nextNode, 
				&currNode);
	if (status != SUCCESS) {
	    if (status == NOT_FOUND) {
		nextNode = NULL;
	    } else {
		return status;
	    }
	}

	/*
	 * If this is a directory index that no directory points to,
	 * delete it.
	 */
	if (T_IS_DIR_INDEX(tagNode->status) == TRUE &&
	    tagNode->numLinksFound == 0)
	{
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_199,
			     "Deleting directory index %ld.%u of file system %s because nothing points to it.\n"), 
		     tagNode->tagSeq.tag_num, tagNode->tagSeq.tag_seq, 
		     fileset->fsName);
	    status = delete_tag(fileset, tagNode->tagSeq.tag_num);
	    if (status != SUCCESS) {
		return status;
	    }
	}
	/*
	 * Special case DELETE_WITH_SNAP because AdvFS doesn't bother
	 * to clean up after itself by resetting numLinks to 0 like it
	 * should.  Also special case DIR_INDEX which has no fsstat
	 * record.  Lastly, special case the group quota file, which
	 * doesn't live in a directory.
	 */
	else if (tagNode->numLinks != tagNode->numLinksFound &&
		 T_IS_DIR_INDEX(tagNode->status) == FALSE &&
		 T_IS_DELETE_WITH_SNAP(tagNode->status) == FALSE &&
		 tagNode->tagSeq.tag_num != 5) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_200,
			     "Resetting %s's tag (%ld.%u) numLinks from %d to %d.\n"),
		     fileset->fsName,
		     tagNode->tagSeq.tag_num, tagNode->tagSeq.tag_seq,
		     tagNode->numLinks, tagNode->numLinksFound);
	    tagNode->numLinks = tagNode->numLinksFound;
	    status = correct_fsstat(fileset, tagNode);
	    if (status != SUCCESS) {
		return status;
	    }
	}

	tagNode = nextNode;
    } /* end next tag loop */

    return SUCCESS;
} /* end correct_all_dirs */


/*
 * Function Name: correct_dirs_recurse
 *
 * Description: This is a recursive routine that checks the directory
 *		hierarchy. It is initially called with tag 2 (the top
 *		level directory) for each fileset.
 *
 *		NOTE: This must be called after overlapping files have
 *			been deleted!
 *
 * Input parameters:
 *	fileset: The fileset this tag lives in.
 *	tag: The tag of the directory to be searched down from.
 *
 * Output parameters:
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 * 
 */

static int 
correct_dirs_recurse(filesetT *fileset, 
		     tagNodeT *tag, 
		     char *path)
{
    char		*funcName = "correct_dirs_recurse";
    int			status;
    int			modified;
    fobtoLBNT		*xtntArray;
    xtntNumT		xtntArraySize;
    xtntNumT		currXtnt;
    xtntNumT		newXtnt;
    fobNumT		holeSize;
    pageNumT		page;
    pageT		*pPage;
    pageNumT		numPagesInXtnt;
    lbnNumT		currLbn;
    volNumT		currVol;
    fs_dir_entry	*dirEntry;
    bfTagT		entryTag;
    int64_t		entry;		/* Which entry in the entire dir */
    int			numErrs;	/* Count errors to see if this is */
					/* really a directory. */
    tagNodeT		tmpTagNode;
    tagNodeT		*entryTagNode;
    tagNodeT		*origTagNode;
    nodeT		*node;
    int			minEntrySize;
    int			maxEntrySize;
    int			lastEntryOffset;
    tagNumT		firstTagNum;
    uint16_t		entrySize;
    uint16_t            nameCount;
    char		*name;
    int			i;
    char		newPath[MAXPATHLEN];

    entry = 1;
    numErrs = 0;
    minEntrySize = MIN_DIR_ENTRY_SIZE;

    /* get a list of extents for the directory file indicated by "tag" */

    status = collect_extents(fileset->domain, fileset, &(tag->tagMCId),
			     FALSE, &xtntArray, &xtntArraySize, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    if (xtntArraySize == 0) {
	/*
	 * No extents - this is not a valid directory,
	 * so change it to a normal file.
	 */
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_201, 
			 "Resetting file type for '%s' (%ld.%u) to regular file because it has no extents.\n"), 
		 path, tag->tagSeq.tag_num, tag->tagSeq.tag_seq);
	/* Clear all the file type bits. */
	tag->fileType &= ~S_IFMT;
	/* Set regular file bits. */
	tag->fileType |= S_IFREG;

	status = correct_fsstat(fileset, tag);
	if (status != SUCCESS) {
	    return status;
	}
	free(xtntArray);
	return SUCCESS;
    }

    /*
     * loop through each extent in this directory file
     */
    for (currXtnt = 0 ; currXtnt < xtntArraySize ; currXtnt++) {
	/*
	 * Compute a few variables for this extent
	 */
	numPagesInXtnt = FOB2PAGE(xtntArray[currXtnt+1].fob - 
				  xtntArray[currXtnt].fob);
	currVol = xtntArray[currXtnt].volume;
	currLbn = xtntArray[currXtnt].lbn;

	if (currLbn == XTNT_TERM) { /* found a hole in the directory! */
	    /*
	     * This is a hole in a directory - these are not allowed.
	     * Except for directory indices, the FOB a directory entry
	     * is on is irrelevant, so get rid of the hole by shifting
	     * all extents back one entry.
	     *
	     * E.g.,    OLD		NEW
	     *		fob	lbn	fob	lbn
	     *		0	x	0	x
	     *		10	y	10	y
	     *		15	-1	15	z
	     *		50	z	30	q
	     *		65	q	55	-1
	     *		90	-1	55	-1
	     *
	     * The problem with this is it leaves a "hole" at the end of
	     * the file that should be deleted.
	     * this needs to be fixed.
	     *
	     * It's tolerable if and only if the filesize is adjusted to
	     * match the last inuse fob.  This is fixed at the end of
	     * this routine.
	     */
	    /*
	     * XXX - FUTURE: Directory indices have to be dealt with
	     * as well.
	     */
	    holeSize = xtntArray[currXtnt + 1].fob - xtntArray[currXtnt].fob;
	    for (newXtnt = currXtnt ; newXtnt < xtntArraySize ; newXtnt++) {
		xtntArray[newXtnt].volume = xtntArray[newXtnt + 1].volume;
		xtntArray[newXtnt].lbn = xtntArray[newXtnt + 1].lbn;
		xtntArray[newXtnt + 1].fob -= holeSize;
	    }
	    status = correct_chain_extents(fileset, tag->tagMCId, xtntArray);
	    if (status != SUCCESS) {
		return status;
	    }

	    xtntArraySize--;
	    if (currXtnt == xtntArraySize) {
		/*
		 * This was the last extent.  Break the loop on extents.
		 */
		break;
	    }
	    /*
	     * Reset the variables for this extent
	     */
	    numPagesInXtnt = FOB2PAGE(xtntArray[currXtnt+1].fob -
				      xtntArray[currXtnt].fob);
	    currVol = xtntArray[currXtnt].volume;
	    currLbn = xtntArray[currXtnt].lbn;

	    assert(currLbn != XTNT_TERM);

	} /* end: found a hole in the directory */

	/*
	 * loop through each page in this extent. (directories always
	 * consume an integral number of pages, since they're
	 * considered metadata files).
	 */
	for (page = 0 ; page < numPagesInXtnt ; page++) {
	    status = read_page_from_cache(currVol, currLbn, &pPage);
	    if (status != SUCCESS) {
		return status;
	    }

	    if (P_IS_MODIFIED(pPage->status) &&
		xtntArray[currXtnt].isOrig == FALSE) {

		/* if P_IS_MODIFIED then we previously corrected this
		   directory page and cached it for disk update.  And,
		   since isOrig == FALSE, we know that this page is
		   not dual referenced by both a parent and a snap
		   fileset.  So the only reason we could be revisiting
		   this page now that this page belongs to two
		   different files.  That means corruption, so we
		   delete the directory. */

		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FSCK_1, FSCK_202, 
				 "Page %ld (vol %d, lbn %ld) in directory '%s' (tag %ld.%u) overlaps other metadata.  Deleting directory.\n"),
			 page + FOB2PAGE(xtntArray[currXtnt].fob), 
			 currVol, currLbn, path, 
			 tag->tagSeq.tag_num, tag->tagSeq.tag_seq);

		status = release_page_to_cache(currVol, currLbn, pPage,
					       FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		status = delete_tag(fileset, tag->tagSeq.tag_num);
		if (status != SUCCESS) {
		    return status;
		}
		return SUCCESS;
	    }

	    dirEntry = (fs_dir_entry *)pPage->pageBuf;
	    maxEntrySize = DIRBLKSIZ;
	    lastEntryOffset = 0;
	    modified = FALSE;

	    if (xtntArray[currXtnt].isOrig == TRUE) {

		/* this directory file is referenced by both the
		   parent and child tag dirs (no mods to the file
		   since the snap was taken).  This means that all the
		   tag/MCId file entries in this directory file claim
		   the parent FS as the file owner. Also we also
		   expect each of this directory's file entries
		   (tag/MCId) to be found in the snapshot tag file (we
		   already checked the parent tag file).  We verify
		   that here, and if the tag entry is not found in the
		   snap tag dir we create one now. */

		while ((uint64_t)dirEntry < 
		       (uint64_t)pPage->pageBuf + ADVFS_METADATA_PGSZ) {

		    if (dirEntry->fs_dir_bs_tag_num != 0) {

			name = dirEntry->fs_dir_name_string;
			tmpTagNode.tagSeq.tag_num = 
			    dirEntry->fs_dir_bs_tag_num;
			entryTagNode = &tmpTagNode;
			status = find_node(fileset->tagList,
					   (void **)&entryTagNode, &node);

			if (status == NOT_FOUND) {
			    /*
			     * This tag doesn't exist in the snap.
			     * But we know it exists in the original
			     * fileset because this directory was
			     * already checked.  So, link the snapped
			     * file's tag to the original's primary
			     * mcell.
			     */
			    writemsg(SV_VERBOSE | SV_LOG_FOUND,
				     catgets(_m_catd, S_FSCK_1, FSCK_203, 
					     "Snap %s's tag %d (%s/%s) not found. Linking tag to '%s' FS tag %d's primary mcell.\n"),
				     fileset->fsName,
				     tmpTagNode.tagSeq.tag_num,
				     path, name,
				     fileset->origSnapPtr->fsName,
				     tmpTagNode.tagSeq.tag_num);

			    GETTAG(entryTag, dirEntry);
			    tmpTagNode.tagSeq.tag_num = entryTag.tag_num;
			    origTagNode = &tmpTagNode;
			    status = find_node(fileset->origSnapPtr->tagList,
					       (void **)&origTagNode, &node);
			    if (status != SUCCESS) {
				return status;
			    }
			    status = correct_tag_to_mcell(fileset,
							  entryTag.tag_num,
							  entryTag.tag_seq,
							  origTagNode->tagMCId);
			    if (status != SUCCESS) {
				return status;
			    }
			    tmpTagNode.tagSeq.tag_num = entryTag.tag_num;
			    entryTagNode = &tmpTagNode;
			    status = find_node(fileset->tagList,
					       (void **)&entryTagNode, &node);
			    if (status != SUCCESS) {
				return status;
			    }
			    /*
			     * Now copy the data from the original's tagNode.
			     */
			    entryTagNode->size	     = origTagNode->size;
			    entryTagNode->fileType   = origTagNode->fileType;
			    entryTagNode->fobsFound = origTagNode->fobsFound;
			    entryTagNode->numLinks   = origTagNode->numLinks;
			    entryTagNode->parentTagNum.tag_num = 
			      origTagNode->parentTagNum.tag_num;
			    entryTagNode->parentTagNum.tag_seq = 
			      origTagNode->parentTagNum.tag_seq;
			    if (T_IS_DIR_INDEX(origTagNode->status)) {
				T_SET_DIR_INDEX(entryTagNode->status);
			    }
			    /* indicate that this file is referenced
			       in both child and parent filesets */
			    T_SET_SNAP_ORIG_PMCELL(entryTagNode->status);
			} else if (status != SUCCESS) {
			    return status;
			}
			if (entryTagNode != NULL) {
			    entryTagNode->numLinksFound++;
			    if (S_IFDIR == (entryTagNode->fileType & S_IFMT) &&
				entryTagNode->tagSeq.tag_num != 
				tag->tagSeq.tag_num &&
				entryTagNode->tagSeq.tag_num != 
				tag->parentTagNum.tag_num)
			    {
				if (MAXPATHLEN <= (strlen(path) +
						   strlen(name))) {
				    writemsg(SV_VERBOSE | SV_LOG_INFO,
					     catgets(_m_catd, S_FSCK_1, FSCK_204, 
						     "Truncating path '%s/%s' to '.../%s' for fsck output and log messages.\n"),
					     path, name, name);
				    strcpy(newPath, "...");
				} else {
				    strcpy(newPath, path);
				}
				strcat(newPath, "/");
				strcat(newPath, name);
				status = correct_dirs_recurse(fileset,
							      entryTagNode,
							      newPath);
				if (status != SUCCESS) {
				    return status;
				}
			    } /* End if directory */
			} /* End if entry tagnode found */
		    } /* End if inuse entry */
		    entryTagNode = NULL;
		    dirEntry = (fs_dir_entry *)((char *)dirEntry +
						dirEntry->fs_dir_size);
		} /* End while dir entries in a snapshot directory */
	    } else {
		/*
		 * This directory file is not dual referenced by both
		 * a parent and a snap.  That means that this must be
		 * the first time we've visited this page.  We now
		 * check each entry in the page.
		 */

		while ((int64_t)dirEntry < 
		       (int64_t)pPage->pageBuf + ADVFS_METADATA_PGSZ) {
		    
		    /*
		     * Check for "." as the first entry.
		     */
		    if (entry == 1) {
			if (dirEntry->fs_dir_bs_tag_num != tag->tagSeq.tag_num)
			{
			    dirEntry->fs_dir_bs_tag_num = tag->tagSeq.tag_num;
			    numErrs++;
			    modified = TRUE;
			}
			if (dirEntry->fs_dir_size < 
			      get_min_dirsize(dirEntry)) {
			    dirEntry->fs_dir_size = get_min_dirsize(dirEntry);
			    numErrs++;
			    modified = TRUE;
			}
			if (dirEntry->fs_dir_namecount != 1) {
			    dirEntry->fs_dir_namecount = 1;
			    numErrs++;
			    modified = TRUE;
			}
			if (strncmp(".", dirEntry->fs_dir_name_string, 2)) {
			    dirEntry->fs_dir_name_string[0] = '.';
			    dirEntry->fs_dir_name_string[1] = '\0';
			    dirEntry->fs_dir_name_string[2] = '\0';
			    dirEntry->fs_dir_name_string[3] = '\0';
			    numErrs++;
			    modified = TRUE;
			}
			/*
			 * GETTAG fetches the tag num and seq values
			 * from their stashes in the directory entry.
			 */

			GETTAG(entryTag, dirEntry);
			if (entryTag.tag_num != tag->tagSeq.tag_num) {
			    PUTTAG_NUM(tag->tagSeq.tag_num, dirEntry);
			    numErrs++;
			    modified = TRUE;
			}
			if (entryTag.tag_seq != tag->tagSeq.tag_seq) {
			    PUTTAG_SEQ(tag->tagSeq.tag_seq, dirEntry);
			    numErrs++;
			    modified = TRUE;
			}
			if (modified == TRUE) {
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FSCK_1, FSCK_205,
					     "Corrected '.' entry in directory '%s', (%ld.%u).\n"),
				     path, tag->tagSeq.tag_num, 
				     tag->tagSeq.tag_seq);
			}
			tag->numLinksFound++;
		    } /* End check for "." entry */
		    /*
		     * Check for ".." as the second entry.
		     */
		    else if (entry == 2) {
			if (dirEntry->fs_dir_bs_tag_num !=
			    tag->parentTagNum.tag_num)
			{
			    /* special case: the root directory has no
			       parent, so the parent tag value will be
			       0.0.  This means that the ".." dir
			       entry's parent is not the same */

			    if (dirEntry->fs_dir_bs_tag_num != ROOT_FILE_TAG) {
				dirEntry->fs_dir_bs_tag_num =
				    tag->parentTagNum.tag_num;
				numErrs++;
				modified = TRUE;
			    }
			}
			if (dirEntry->fs_dir_size < 
			      get_min_dirsize(dirEntry)) {
			    dirEntry->fs_dir_size = get_min_dirsize(dirEntry);
			    numErrs++;
			    modified = TRUE;
			}
			if (dirEntry->fs_dir_namecount != 2) {
			    dirEntry->fs_dir_namecount = 2;
			    numErrs++;
			    modified = TRUE;
			}
			if (strncmp("..", dirEntry->fs_dir_name_string, 3)) {
			    dirEntry->fs_dir_name_string[0] = '.';
			    dirEntry->fs_dir_name_string[1] = '.';
			    dirEntry->fs_dir_name_string[2] = '\0';
			    dirEntry->fs_dir_name_string[3] = '\0';
			    numErrs++;
			    modified = TRUE;
			}
			GETTAG(entryTag, dirEntry);
			if (entryTag.tag_num != ROOT_FILE_TAG) {
			    if (entryTag.tag_num != tag->parentTagNum.tag_num) {
				PUTTAG_NUM(tag->parentTagNum.tag_num, dirEntry);
				entryTag.tag_num = tag->parentTagNum.tag_num;
				numErrs++;
				modified = TRUE;
			    }
			    if (entryTag.tag_seq != tag->parentTagNum.tag_seq) {
				PUTTAG_SEQ(tag->parentTagNum.tag_seq, dirEntry);
				numErrs++;
				modified = TRUE;
			    }
			}

			if (modified == TRUE) {
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FSCK_1, FSCK_206, 
					     "Corrected '..' entry in directory '%s', (%ld.%u).\n"),
				     path, tag->tagSeq.tag_num, 
				     tag->tagSeq.tag_seq);
			}
			/*
			 * This may not be a directory.  If the 
			 * entry on this page isn't correct, change the
			 * FS_STAT for this dir to be a regular file, and
			 * don't modify this page at all.
			 */

			/*
			 * numErrs can be as high as 12.  10 was picked as a
			 * best guess, nothing magical about it.  FUTURE:
			 * Might want to just do 1 error for "." and another
			 * for "..".
			 */
			if (numErrs >= 10) {
			    /*
			     * If the checks that can be made on the tagNode
			     * match up with what a directory should look
			     * like, then read the second page if it exists,
			     * or read the last 1k of this page to see if
			     * they look like dir entries.
			     *
			     * Dir size must be a multiple of
			     * ADVFS_METADATA_PGSZ, they must have at
			     * least 2 links, and the number of actual
			     * pages must equal the file size.
			     */
			    if (tag->size % ADVFS_METADATA_PGSZ == 0 &&
				tag->numLinks >= 2 &&
				(tag->fobsFound == tag->size / ADVFS_FOB_SZ))
			    {
				lbnNumT newLbn;
				volNumT newVol;
				pageT *newPage;

				if (tag->size > ADVFS_METADATA_PGSZ) {
				    status = convert_page_to_lbn(xtntArray,
								 xtntArraySize,
								 1,
								 &newLbn,
								 &newVol);
				    if (status != SUCCESS) {
					return status;
				    }
				    status = read_page_from_cache(newVol,
								  newLbn,
								  &newPage);
				    if (status != SUCCESS) {
					return status;
				    }
				}

				firstTagNum = dirEntry->fs_dir_bs_tag_num;
				entrySize = dirEntry->fs_dir_size;
				nameCount = dirEntry->fs_dir_namecount;
				name = dirEntry->fs_dir_name_string;

				if ((entrySize <= DIRBLKSIZ) &&
				    (entrySize >= minEntrySize) &&
				    (entrySize % sizeof(int64_t) == 0))
				{
				    GETTAG(entryTag, dirEntry);
				}

				if ((entrySize <= DIRBLKSIZ) &&
				    (entrySize >= minEntrySize) &&
				    (entrySize % sizeof(int64_t) == 0) &&
				    (firstTagNum == 0)) {
				    /*
				     * This is a valid dir entry.
				     * Consider the file as a directory
				     * by resetting numErrs.
				     */
				    writemsg(SV_DEBUG | SV_LOG_INFO,
					     catgets(_m_catd, S_FSCK_1, FSCK_207,
						     "Valid directory (free entry) - not resetting file type.\n"));
				    numErrs = 0;
				}
				else if ((firstTagNum != 0) &&
					 (nameCount >= 1) &&
					 (nameCount <= FS_DIR_MAX_NAME) &&
					 (entrySize >= minEntrySize) &&
					 (entrySize <= DIRBLKSIZ) &&
					 (entrySize % sizeof(int64_t) == 0) &&
					 (entrySize >= 
					  get_min_dirsize(dirEntry)) &&
					 (firstTagNum == entryTag.tag_num)) {
				    /*
				     * Also looks like a valid dir entry.
				     * Not performing a find_node because it's
				     * overkill if the format is all correct.
				     */
				    writemsg(SV_DEBUG | SV_LOG_INFO, 
					     catgets(_m_catd, S_FSCK_1, FSCK_208,
						     "Valid directory (used entry) - not resetting file type.\n"));
				    numErrs = 0;
				}

				if (tag->size > ADVFS_METADATA_PGSZ) {
				    status = release_page_to_cache(newVol, newLbn,
								   newPage,
								   FALSE, FALSE);
				    if (status != SUCCESS) {
					return status;
				    }
				}
			    } /* End if tag node checks */
			    /*
			     * If we didn't reset numErrs, change the FS_STAT
			     * record to assume this is a regular file.
			     */
			    if (numErrs != 0)
			    {
				/*
				 * Reset file type in taglist to regular file.
				 */
				writemsg(SV_VERBOSE | SV_LOG_FIXED,
					 catgets(_m_catd, S_FSCK_1, FSCK_209, 
						 "Not a directory - resetting file type for '%s' (%ld.%u) to regular file.\n"),
					 path,
					 tag->tagSeq.tag_num, 
					 tag->tagSeq.tag_seq);
				/* Clear all the file type bits. */
				tag->fileType &= ~S_IFMT;
				/* Set regular file bits. */
				tag->fileType |= S_IFREG;

				status = correct_fsstat(fileset, tag);
				if (status != SUCCESS) {
				    return status;
				}

				/*
				 * Decrement this tag's numLinksFound, since
				 * this file no longer has a link to itself.
				 * It was incremented in the "." check above.
				 */
				tag->numLinksFound--;

				/*
				 * We are releasing this page declaring it
				 * to have not been modified.  The page
				 * cannot have already been in the write
				 * cache because it was checked when we
				 * first read it.
				 */
				status = release_page_to_cache(currVol,
							       currLbn,
							       pPage,
							       FALSE, FALSE);
				if (status != SUCCESS) {
				    return status;
				}
				free(xtntArray);
				return SUCCESS;
			    }
			} /* End if numErrs >= 10 */

			tmpTagNode.tagSeq.tag_num =
			    dirEntry->fs_dir_bs_tag_num;
			entryTagNode = &tmpTagNode;
			status = find_node(fileset->tagList,
					   (void **)&entryTagNode, &node);
			if (status != SUCCESS) {
			    return status;
			}
			entryTagNode->numLinksFound++;
		    } /* End check for ".." entry */
		    /*
		     * Free space entry
		     */
		    else if (dirEntry->fs_dir_bs_tag_num == 0) {
			/*
			 * Entry size is the only thing to check on free
			 * entries.
			 */
			if (dirEntry->fs_dir_size == 0  ||
			    (dirEntry->fs_dir_size % sizeof(int64_t)) ||
			    dirEntry->fs_dir_size > maxEntrySize)
			{
			    dirEntry->fs_dir_size = maxEntrySize;
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FSCK_1, FSCK_210, 
					     "Adjusted free entry size in directory '%s' (%ld.%u).\n"),
				     path, tag->tagSeq.tag_num, 
				     tag->tagSeq.tag_seq);
			    modified = TRUE;
			}
		    }
		    /*
		     * In use, non "." or ".." entry
		     */
		    else {
			/*
			 * FUTURE: check for duplicate name entries.
			 * This will not go into the first release of
			 * fsck, but should be done in a subsequent
			 * release.
			 */

			/*
			 * Make shorter variable names for the rest of this
			 * section.
			 */
			firstTagNum =
			    dirEntry->fs_dir_bs_tag_num;
			entrySize = dirEntry->fs_dir_size;
			nameCount = dirEntry->fs_dir_namecount;
			name = dirEntry->fs_dir_name_string;
			if ((entrySize >= minEntrySize) &&
			    (entrySize <= maxEntrySize) &&
			    (entrySize % sizeof(int64_t)) == 0)
			{
			    GETTAG(entryTag, dirEntry);
			}

			/*
			 * Perform several checks - if any of them fail,
			 * set the entry to free, and lengthen it to the end
			 * of this disk block.
			 */

			if ((nameCount < 1) ||
			    (nameCount > FS_DIR_MAX_NAME) ||
			    (entrySize < minEntrySize) ||
			    (entrySize > maxEntrySize) ||
			    (entrySize % sizeof(int64_t)) != 0 ||
			    (entrySize < get_min_dirsize(dirEntry)) ||
			    (firstTagNum != entryTag.tag_num))
			{
			    dirEntry->fs_dir_bs_tag_num = 0;
			    dirEntry->fs_dir_size = maxEntrySize;
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FSCK_1, FSCK_211, 
					     "Removing invalid entry '%s' from directory '%s', (%ld.%u).\n"), 
				     dirEntry->fs_dir_name_string, path, 
				     tag->tagSeq.tag_num, tag->tagSeq.tag_seq);
			    modified = TRUE;
			}
			else {
			    /*
			     * Entry is internally consistent, now check
			     * if the tag it refers to exists.
			     */
			    tmpTagNode.tagSeq.tag_num = entryTag.tag_num;
			    entryTagNode = &tmpTagNode;
			    status = find_node(fileset->tagList,
					       (void **)&entryTagNode, &node);
			    if (status == NOT_FOUND) {
				dirEntry->fs_dir_bs_tag_num = 0;
				/*
				 * Don't increase dir entry size, as there
				 * should be valid dir entries after this
				 * in this block.
				 */
				writemsg(SV_VERBOSE | SV_LOG_FIXED, 
					 catgets(_m_catd, S_FSCK_1, FSCK_212, 
						 "Removing entry (%ld.%u) from directory tag '%s' (%ld.%u) because tag not found.\n"), 
					 entryTag.tag_num, 
					 entryTag.tag_seq, 
					 path,
					 tag->tagSeq.tag_num, 
					 tag->tagSeq.tag_seq);
				modified = TRUE;
			    }
			    else if (status != SUCCESS) {
				return status;
			    }

			    if ((status == SUCCESS) &&
				(entryTagNode->tagSeq.tag_seq != entryTag.tag_seq)) {
				writemsg(SV_VERBOSE | SV_LOG_FIXED,
					 catgets(_m_catd, S_FSCK_1, FSCK_213,
						 "Modified entry (%ld.%u) to (%ld.%u) in directory '%s' (%ld.%u).\n"),
					 entryTag.tag_num, entryTag.tag_seq,
					 entryTag.tag_num,
					 entryTagNode->tagSeq.tag_seq, path,
					 tag->tagSeq.tag_num, 
					 tag->tagSeq.tag_seq);
				PUTTAG_SEQ(entryTagNode->tagSeq.tag_seq, dirEntry);
				modified = TRUE;
			    }

			    if (name[nameCount] != '\0') {
				/*
				 * Check last possible NULL character.
				 */
				i = roundup((nameCount + 1), sizeof(int)) - 1;
				if ('\0' == name[i]) {
				    while ('\0' == name[i] && i > nameCount) {
					i--;
				    }
				    i++;
				    dirEntry->fs_dir_namecount = i;
				}
				else {
				    dirEntry->fs_dir_name_string[nameCount] =
					'\0';
				}
				writemsg(SV_VERBOSE | SV_LOG_FIXED, 
					 catgets(_m_catd, S_FSCK_1, FSCK_214,
						 "Adjusted name '%s' entry in directory '%s' (%ld.%u).\n"),
					 name, path,
					 tag->tagSeq.tag_num, tag->tagSeq.tag_seq);
				modified = TRUE;
			    }
			} /* End of else internally consistent check */

			if (dirEntry->fs_dir_bs_tag_num >= 3 &&
			    dirEntry->fs_dir_bs_tag_num <= 5 &&
			    tag->tagSeq.tag_num != 2)
			{
			    /*
			     * This is a link to '.tags', or 'quotas'
			     * in a directory other than the root
			     * directory.  Blow it away, since it
			     * shouldn't be here.
			     */
			    dirEntry->fs_dir_bs_tag_num = 0;
			    /*
			     * Don't increase dir entry size, as there should
			     * be valid dir entries after this in this block.
			     */
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FSCK_1, FSCK_215, 
					     "Removing entry '%s' (%ld.%u) from directory '%s' (%ld.%u)\n"), 
				     name, entryTag.tag_num, 
				     entryTag.tag_seq, 
				     path, tag->tagSeq.tag_num, 
				     tag->tagSeq.tag_seq);
			    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT, 
				     catgets(_m_catd, S_FSCK_1, FSCK_216, 
					     "because it shouldn't exist anywhere but the root directory.\n"));
			    modified = TRUE;
			}

			if (dirEntry->fs_dir_bs_tag_num != 0 &&
			    entryTagNode != NULL) {
			    /*
			     * Got a good entry
			     */
			    entryTagNode->numLinksFound++;

			    if (S_IFDIR == (entryTagNode->fileType & S_IFMT)) {
				if (entryTagNode->numLinksFound > 1) {
				    /*
				     * Already found this directory entry -
				     * can't allow hard linked directories,
				     * so remove this one.
				     */
				    dirEntry->fs_dir_bs_tag_num = 0;
				    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
					     catgets(_m_catd, S_FSCK_1, FSCK_217,
						     "Removing duplicate entry '%s' from directory '%s' (%ld.%u).\n"), 
					     name, path,
					     tag->tagSeq.tag_num, 
					     tag->tagSeq.tag_seq);
				    modified = TRUE;

				    entryTagNode->numLinksFound--;
				}
				else {
				    if ((entryTagNode->parentTagNum.tag_num !=
				         tag->tagSeq.tag_num) ||
					(entryTagNode->parentTagNum.tag_seq !=
				         tag->tagSeq.tag_seq))
				    {
					writemsg(SV_VERBOSE | SV_LOG_FIXED,
						 catgets(_m_catd, S_FSCK_1, FSCK_218, 
							 "Resetting '%s' (%ld.%u)'s parent from (%ld.%u) to '%s' (%ld.%u).\n"), 
						 name,
						 entryTagNode->tagSeq.tag_num,
						 entryTagNode->tagSeq.tag_seq,
						 entryTagNode->parentTagNum.tag_num,
						 entryTagNode->parentTagNum.tag_seq,
						 path,
						 tag->tagSeq.tag_num,
						 tag->tagSeq.tag_seq);
					entryTagNode->parentTagNum.tag_num =
					    tag->tagSeq.tag_num;
					entryTagNode->parentTagNum.tag_seq =
					    tag->tagSeq.tag_seq;
					status = correct_fsstat(fileset,
								entryTagNode);
					if (status != SUCCESS) {
					    return status;
					}
				    }
				    if ((strlen(path) + strlen(name)) >= 
					MAXPATHLEN) {
					writemsg(SV_VERBOSE | SV_LOG_INFO,
						 catgets(_m_catd, S_FSCK_1, FSCK_204,
							 "Truncating path '%s/%s' to '.../%s' for fsck output and log messages.\n"),
						 path, name, name);
					strcpy(newPath, "...");
				    } else {
					strcpy(newPath, path);
				    }
				    strcat(newPath, "/");
				    strcat(newPath, name);

				    status = correct_dirs_recurse(fileset,
								  entryTagNode,
								  newPath);
				    if (status != SUCCESS) {
					return status;
				    }
				}
			    } /* End if directory entry */

			    /*
			     * Don't check the parent.  It gets changed on a
			     * regular basis, and can easily be "corrupted" by
			     * making a hardlink, touching it in dir2, then rm'ing
			     * it from dir2.
			     */
			    /*if ((1 == entryTagNode->numLinks) &&
			     *    (1 == entryTagNode->numLinksFound) &&
			     *    ((entryTagNode->parentTagNum.tag_num !=
			     *      tag->tagSeq.tag_num) ||
			     *     (entryTagNode->parentTagNum.tag_seq !=
			     *      tag->tagSeq.tag_seq)))
			     *{
			     *    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     *	     catgets(_m_catd, S_FSCK_1, FSCK_219, 
			     *               "Resetting (%ld.%u)'s parent from (%ld.%u) to (%ld.%u).\n"), 
			     *	     entryTagNode->tagSeq.tag_num, 
			     *	     entryTagNode->tagSeq.tag_seq, 
			     *	     entryTagNode->parentTagNum.tag_num, 
			     *	     entryTagNode->parentTagNum.tag_seq, 
			     *	     tag->tagSeq.tag_num, tag->tagSeq.tag_seq);
			     *    entryTagNode->parentTagNum.tag_num = tag->tagSeq.tag_num;
			     *    entryTagNode->parentTagNum.tag_seq = tag->tagSeq.tag_seq;
			     *    status = correct_fsstat(fileset, entryTagNode);
			     *    if (status != SUCCESS) {
			     *	return status;
			     *    }
			     *}
			     */
			} /* End if valid entry */
		    } /* End if found a good entry */

		    if ((lastEntryOffset + dirEntry->fs_dir_size) 
			!= ADVFS_METADATA_PGSZ) {
			lastEntryOffset += dirEntry->fs_dir_size;
		    }
		    maxEntrySize -= dirEntry->fs_dir_size;
		    if (maxEntrySize == 0) {
			maxEntrySize = DIRBLKSIZ;
		    }
		    entryTagNode = NULL;
		    dirEntry = (fs_dir_entry *)((char *)dirEntry +
				    dirEntry->fs_dir_size);
		    entry++;
		} /* End loop on each dir entry on this page */
	    } /* End if: primMCId in multiple tagdirs, else: not */

	    status = release_page_to_cache(currVol, currLbn, pPage,
					   modified, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    modified = FALSE;

	    currLbn += ADVFS_METADATA_PGSZ_IN_BLKS;
	} /* End loop on each page */
    } /* End loop on each extent */

    /*
     * Validate fsstat size
     */
    while (xtntArray[xtntArraySize - 1].lbn == XTNT_TERM) {
	/*
	 * There was a hole in the file, which has been moved to the end
	 * of the file.  Now decrement the extent array size to match the
	 * end of the real extents.
	 */
	xtntArraySize--;
    }

    if (tag->size != xtntArray[xtntArraySize].fob * ADVFS_FOB_SZ &&
	T_IS_SNAP_ORIG_PMCELL(tag->status) == FALSE)
    {
	writemsg(SV_VERBOSE | SV_LOG_FIXED,
		 catgets(_m_catd, S_FSCK_1, FSCK_222, 
			 "Modified size of '%s' (%ld.%u) from %d to %d.\n"),
		 path, tag->tagSeq.tag_num, tag->tagSeq.tag_seq, 
		 tag->size, xtntArray[xtntArraySize].fob * ADVFS_FOB_SZ);
	tag->size = xtntArray[xtntArraySize].fob * ADVFS_FOB_SZ;
	correct_fsstat(fileset, tag);
	if (status != SUCCESS) {
	    return status;
	}
    }

    free(xtntArray);

    status = correct_dir_index(fileset, tag);
    if (status != SUCCESS) {
	return status;
    }

    return SUCCESS;
} /* end correct_dirs_recurse */


/*
 * Function Name: correct_dir_index
 *
 * Description: This routine checks and corrects a directory index file.
 *		Initially, it will simply check it, and if it finds a problem,
 *		will remove the index file.  If there is no directory index
 *		associated with this directory, return SUCCESS immediately.
 *
 *		NOTE: This must be called after overlapping files have
 *			been deleted!
 *
 * Input parameters:
 *	fileset: The fileset this directory index lives in.
 *	dirTag: The tag node of the directory associated with the index file
 *		to be checked.
 *
 * Output parameters:
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 * 
 */
static int 
correct_dir_index(filesetT *fileset, 
		  tagNodeT *dirTag)
{
    char		*funcName = "correct_dir_index";
    domainT		*domain;
    volumeT		*volume;
    tagNodeT		*indexTag;
    tagNodeT		tmpTagNode;
    nodeT		*node;
    int			foundIdxRec;
    int			status;
    bsIdxBmtRecT	*pIdxBmtRec;
    bsIdxBmtRecT	idxBmtRec;
    int64_t		currFreeOffset;
    mcidT		primaryMcell;
    mcidT		currentMcell;
    mcidT		nextMcell;
    lbnNumT		currLbn;
    volNumT		currVol;
    pageT		*currPage;
    bsMPgT		*bmtPage;
    bsMCT		*pMcell;
    bsMRT		*pRecord;
    fobtoLBNT		*idxXtnt;
    xtntNumT		idxXtntSize;
    fobtoLBNT		*dirXtnt;
    xtntNumT		dirXtntSize;
    int			tree;
    pageNumT		currPageNum;
    uint32_t		levels;
    uint32_t		level;
    uint64_t		nextLevelPageNum;
    idxNodeT		*indexPage;
    uint64_t		i;
    pageNumT		childPage;
    lbnNumT		childLbn;
    volNumT		childVol;

    domain = fileset->domain;
    foundIdxRec = FALSE;
    currFreeOffset = 0;

    /*
     * Return immediately if there is no associated index file.
     */
    if (dirTag->dirIndexTagNum.tag_num == 0) {
	return SUCCESS;
    }

    /*
     * Get the tagNode for the index file.
     */
    tmpTagNode.tagSeq.tag_num = dirTag->dirIndexTagNum.tag_num;
    indexTag = &tmpTagNode;
    status = find_node(fileset->tagList, (void **)&indexTag, &node);
    if (status == NOT_FOUND) {
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_223, 
			 "Deleting directory %ld.%u's link to directory index %ld.%u, index not found.\n"),
		 dirTag->tagSeq.tag_num, dirTag->tagSeq.tag_seq, 
		 dirTag->dirIndexTagNum.tag_num, dirTag->dirIndexTagNum.tag_seq);
	status = clear_tag_index(fileset, dirTag);
	if (status != SUCCESS) {
	    return status;
	}
	/*
	 * Nothing more to check.
	 */
	return SUCCESS;
    } else if (status != SUCCESS) {
	return status;
    }

    if (indexTag->tagSeq.tag_seq != dirTag->dirIndexTagNum.tag_seq) {
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_224,
			 "Deleting directory %ld.%u's link to directory index %ld.%u, sequence numbers don't match.\n"),
		 dirTag->tagSeq.tag_num, dirTag->tagSeq.tag_seq, 
		 dirTag->dirIndexTagNum.tag_num, 
		 dirTag->dirIndexTagNum.tag_seq);
	status = clear_tag_index(fileset, dirTag);
	if (status != SUCCESS) {
	    return status;
	}
	/*
	 * Nothing more to check.
	 */
	return SUCCESS;
    }

    /*
     * If this is not a dir index, or if another directory already points
     * to this index, clear this link to it and return.  Else, set
     * numLinksFound to 1 to indicate this index has been found.
     */
    if (indexTag->numLinksFound || T_IS_DIR_INDEX(indexTag->status) == FALSE) {
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_225, 
			 "Deleting directory %ld.%u's link to directory index %ld.%u, not an index or already referenced.\n"),
		 dirTag->tagSeq.tag_num, dirTag->tagSeq.tag_seq, 
		 dirTag->dirIndexTagNum.tag_num, 
		 dirTag->dirIndexTagNum.tag_seq);
	status = clear_tag_index(fileset, dirTag);
	if (status != SUCCESS) {
	    return status;
	}
	/*
	 * Nothing more to check.
	 */
	return SUCCESS;
    } else {
	indexTag->numLinksFound = 1;
    }

    /*
     * Now find the BMTR_FS_INDEX_FILE record in the primary mcell chain
     * to get the root page and number of levels in each tree.
     */
    primaryMcell = indexTag->tagMCId;
    currentMcell = primaryMcell;

    /*
     * Loop until currentMcell is empty [0,0,0], or we found the record.
     */
    while (currentMcell.volume && foundIdxRec == FALSE) {
	/*
	 * Read the current mcell's page (first get LBN).
	 */
	volume = &(domain->volumes[currentMcell.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, &currLbn, &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128,
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}
	
	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (status != SUCCESS) {
	    return status;
	} 
	
	bmtPage = (bsMPgT *)currPage->pageBuf;
	pMcell	 = &(bmtPage->bsMCA[currentMcell.cell]);
	
	/*
	 * Save the next mcell for the next pass through the loop
	 */
	nextMcell = pMcell->mcNextMCId;

	/*
	 * Now loop through the mcell records.
	 */
	pRecord = (bsMRT *)pMcell->bsMR0;
	
	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {
	    
	    if (BMTR_FS_INDEX_FILE == pRecord->type) {
		foundIdxRec = TRUE;
		pIdxBmtRec = (bsIdxBmtRecT *)((char *)pRecord + sizeof(bsMRT));
		
		idxBmtRec.fname_page   = pIdxBmtRec->fname_page;
		idxBmtRec.ffree_page   = pIdxBmtRec->ffree_page;
		idxBmtRec.fname_levels = pIdxBmtRec->fname_levels;
		idxBmtRec.ffree_levels = pIdxBmtRec->ffree_levels;
		
		/*
		 * Found what we wanted - break out of this loop.
		 */
		break;
		
	    } /* end if BMTR_FS_INDEX_FILE */
	    
	    pRecord = (bsMRT *) (((char *)pRecord) + 
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
	} /* end record loop */
	
	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, currPage, 
				       FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	
	currentMcell = nextMcell;

    } /* end loop on mcells */
    
    if (foundIdxRec == FALSE) {
	/*
	 * BMTR_FS_INDEX_FILE record not found -
	 * remove the pointer to it from the directory.  Note that this
	 * should never happen because T_IS_DIR_INDEX status bit would
	 * not have been set.
	 */
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FSCK_1, FSCK_227,
			 "Internal error, but able to continue.\n"));
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_228, 
			 "Deleting directory %ld.%u's directory index %ld.%u, BMT index record not found.\n"),
		 dirTag->tagSeq.tag_num, dirTag->tagSeq.tag_seq, 
		 dirTag->dirIndexTagNum.tag_num, 
		 dirTag->dirIndexTagNum.tag_seq);
	indexTag->numLinksFound = 0;
	status = clear_tag_index(fileset, dirTag);
	if (status != SUCCESS) {
	    return status;
	}
	/*
	 * Nothing more to check.
	 */
	return SUCCESS;
    }

    /*
     * Collect the extents for both the directory and index files.
     */
    status = collect_extents(domain, fileset, &(indexTag->tagMCId), FALSE,
			     &idxXtnt, &idxXtntSize, FALSE);
    if (status != SUCCESS) {
	return status;
    }
    status = collect_extents(domain, fileset, &(dirTag->tagMCId), FALSE,
			     &dirXtnt, &dirXtntSize, FALSE);
    if (status != SUCCESS) {
	return status;
    }
    
    /*
     * Check validity of BMTR_FS_INDEX_FILE record.
     */
    if (idxBmtRec.fname_page < 0 ||
	idxBmtRec.fname_page >= FOB2PAGE(idxXtnt[idxXtntSize].fob) ||
	idxBmtRec.ffree_page < 0 ||
	idxBmtRec.ffree_page >= FOB2PAGE(idxXtnt[idxXtntSize].fob) ||
	idxBmtRec.fname_levels < 0 ||
	idxBmtRec.fname_levels > IDX_MAX_BTREE_LEVELS ||
	idxBmtRec.ffree_levels < 0 ||
	idxBmtRec.ffree_levels > IDX_MAX_BTREE_LEVELS)
    {
	writemsg(SV_VERBOSE | SV_LOG_FIXED,
		 catgets(_m_catd, S_FSCK_1, FSCK_229, 
			 "Deleting directory %ld.%u's directory index %ld.%u, invalid BMTR_FS_INDEX_FILE.\n"),
		 dirTag->tagSeq.tag_num, dirTag->tagSeq.tag_seq, 
		 dirTag->dirIndexTagNum.tag_num, 
		 dirTag->dirIndexTagNum.tag_seq);
	indexTag->numLinksFound = 0;
	status = clear_tag_index(fileset, dirTag);
	if (status != SUCCESS) {
	    return status;
	}
	/*
	 * Nothing more to check.
	 */
	return SUCCESS;
    }

    /*
     * FNAME is 0, FFREE is 1, so this will only loop twice, once for
     * each of the two dir index trees.
     */
    for (tree = FNAME ; tree <= FFREE ; tree++) {
	if (FNAME == tree) {
	    currPageNum = idxBmtRec.fname_page;
	    levels      = idxBmtRec.fname_levels;
	} else {
	    currPageNum = idxBmtRec.ffree_page;
	    levels      = idxBmtRec.ffree_levels;
	}
	
	for (level = 0 ; level <= levels ; level++) {
	    nextLevelPageNum = -1;
	    
	    while (currPageNum >= 0) {
		status = convert_page_to_lbn(idxXtnt, idxXtntSize, currPageNum,
					     &currLbn, &currVol);
		if (status != SUCCESS) {
		    if (status != SUCCESS || currLbn == XTNT_TERM) {
			writemsg(SV_VERBOSE | SV_LOG_FIXED,
				 catgets(_m_catd, S_FSCK_1, FSCK_230,
					 "Deleting directory %ld.%u's directory index %ld.%u, invalid page %ld.\n"),
				 dirTag->tagSeq.tag_num, dirTag->tagSeq.tag_seq, 
				 dirTag->dirIndexTagNum.tag_num, 
				 dirTag->dirIndexTagNum.tag_seq, currPageNum);
			status = CORRUPT;
		    }
		}
		status = read_page_from_cache(currVol, currLbn, &currPage);
		if (status != SUCCESS) {
		    return status;
		}
		indexPage = (idxNodeT *)currPage->pageBuf;

		/*
		 * If this is the leftmost node at this level, save the
		 * pointer to the leftmost node of the next level.
		 */
		if (tree == FNAME) {
		    if (indexPage->sib.leftmost != 0) {
			nextLevelPageNum = indexPage->data[0].loc.node_page;
		    }
		} else {
		    if (indexPage->sib.page_left == -1) {
			nextLevelPageNum = indexPage->data[0].loc.node_page;
		    }
		}
		
		status = validate_dir_index_page(indexPage, tree, currPageNum,
						 idxXtnt, idxXtntSize);
		if (status == CORRUPT) {
		    status = release_page_to_cache(currVol, currLbn,
						   currPage, FALSE, FALSE);
		    if (status != SUCCESS) {
			return status;
		    }
		    indexTag->numLinksFound = 0;
		    status = clear_tag_index(fileset, dirTag);
		    if (status != SUCCESS) {
			return status;
		    }
		    /*
		     * Nothing more to check.
		     */
		    return SUCCESS;
		}
		else if (status != SUCCESS) {
		    return status;
		}
		
		if (level < levels) {
		    /*
		     * This is an intermediate node, so check that all
		     * the pointers to its childres do point to valid
		     * pages.
		     */
		    for (i = 0 ; i < indexPage->total_elements ; i++) {
			childPage = indexPage->data[i].loc.node_page;
			
			status = convert_page_to_lbn(idxXtnt, idxXtntSize,
						     childPage,
						     &childLbn, &childVol);
			if (status != SUCCESS || childLbn == XTNT_TERM) {
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FSCK_1, FSCK_231,
					     "Deleting directory %ld.%u's directory index %ld.%u, invalid child %d of page %ld.\n"),
				     dirTag->tagSeq.tag_num, 
				     dirTag->tagSeq.tag_seq, 
				     dirTag->dirIndexTagNum.tag_num, 
				     dirTag->dirIndexTagNum.tag_seq, 
				     childPage, currPageNum);
			    /*
			     * This status is checked about 15 lines down.
			     */
			    status = CORRUPT;
			    break;
			}
		    }
		} else {
		    if (tree == FNAME) {
			status = validate_dir_index_leaf(indexPage,
							 currPageNum,
							 dirXtnt, dirXtntSize);
		    } else {
			status = validate_dir_index_free_space(indexPage,
							       &currFreeOffset,
							       currPageNum,
							       dirXtnt,
							       dirXtntSize);
		    }
		}
		if (status == CORRUPT) {
		    status = release_page_to_cache(currVol, currLbn,
						   currPage, FALSE, FALSE);
		    if (status != SUCCESS) {
			return status;
		    }
		    indexTag->numLinksFound = 0;
		    status = clear_tag_index(fileset, dirTag);
		    if (status != SUCCESS) {
			return status;
		    }
		    /*
		     * Nothing more to check.
		     */
		    return SUCCESS;
		}
		else if (status != SUCCESS) {
		    return status;
		}

		currPageNum = indexPage->page_right;

		status = release_page_to_cache(currVol, currLbn, currPage,
					       FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
	    } /* End while loop on all pages at this level */

	    if (nextLevelPageNum == -1) {
		/*
		 * Error - the first page should have set nextLevelPageNum.
		 * This dir index is corrupt, so toast it.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FSCK_1, FSCK_232,
				 "Deleting directory %ld.%u's directory index %ld.%u, invalid level links.\n"),
			 dirTag->tagSeq.tag_num, dirTag->tagSeq.tag_seq, 
			 dirTag->dirIndexTagNum.tag_num, 
			 dirTag->dirIndexTagNum.tag_seq);
		indexTag->numLinksFound = 0;
		status = clear_tag_index(fileset, dirTag);
		if (status != SUCCESS) {
		    return status;
		}
		/*
		 * Nothing more to check.
		 */
		return SUCCESS;
	    }
	    currPageNum = nextLevelPageNum;
	} /* End for loop on all levels. */
    } /* End for loop on both trees. */

    return SUCCESS;
} /* end correct_dir_index */


/*
 * Function Name: validate_dir_index_page (3.nn)
 *
 * Description: This routine checks a directory index page
 *
 *		NOTE: This must be called after overlapping files have
 *			been deleted!
 *
 * Input parameters:
 *	page: The page of the directory index to check.
 *	tree: Whether this is an FNAME page or an FFREE page.
 *	pageNum: Which page number within the directory index this is.
 *
 * Output parameters:
 *
 * Returns: SUCCESS, CORRUPT, FAILURE, or NO_MEMORY.
 * 
 */
static int 
validate_dir_index_page(idxNodeT  *page, 
			int       tree, 
			pageNumT  pageNum,
			fobtoLBNT *idxXtnt, 
			xtntNumT  idxXtntSize)
{
    char	*funcName = "validate_dir_index_page";
    uint64_t	i;
    idxNodeT	*rightPage;
    int		status;
    volNumT	currVol;
    lbnNumT	currLbn;
    pageT	*currPage;
    
    if (page->total_elements > IDX_MAX_ELEMENTS) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_233, 
			 "Directory index has too many (%d) elements on page %ld.\n"),
		 page->total_elements, pageNum);
        return CORRUPT;
    }

    if ((page->reserved[0] != 0) ||
	(page->reserved[1] != 0))
    {
	writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FSCK_1, FSCK_234,
			 "Directory index page %ld's reserved fields not zeroed.\n"),
		 pageNum);
	/*
	 * Don't return CORRUPT, since this error can be ignored.
	 */
    }

    /*
     * Due to total_elements being unsigned, the for loop
     * below will fail under this rare but valid condition.
     * Must check for less than 2 because 0 and 1 will both
     * overflow when subtracting 2 for the for loop end
     * condition below.
     */
    if (page->total_elements < 2 && page->page_right == -1) {
	return SUCCESS;
    }

    /*
     * Can't check last entry since it is a special case.
     * Note that total_elements is apparently the number of
     * elements plus one, and page[total_elements - 2] is the
     * last valid element.
     */
    for (i = 0 ; i < page->total_elements - 2 ; i++) {
        if (page->data[i].search_key > page->data[i+1].search_key) {
            writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_235, 
			     "Directory index, page %ld has out of order elements.\n"),
		     pageNum);
	    writemsg(SV_VERBOSE | SV_LOG_FOUND | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_236,
			     "Total is %d, element %d key %d > element %d key %d.\n"),
		     page->total_elements, i, page->data[i].search_key,
		     i+1, page->data[i+1].search_key);
	    return CORRUPT;
        }
    }

    /*
     * Now check for consistency between this page and the next page
     * to the right on its same level.
     */
    if (page->page_right != -1) {
	if (page->total_elements == 0) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_237,
			     "Directory index with too few entries on page %ld.\n"),
		     pageNum);
	    return CORRUPT;
	}
	if (page->page_right == pageNum) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_238,
			     "Directory index with right sibling of page %ld pointing to itself.\n"),
		     pageNum);
	    return CORRUPT;
	}

	status = convert_page_to_lbn(idxXtnt, idxXtntSize, page->page_right,
				     &currLbn, &currVol);
	if (status != SUCCESS || currLbn == XTNT_TERM) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_239,
			     "Directory index with invalid right sibling of page %ld.\n"),
		     pageNum);
	    return CORRUPT;
	}
	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (status != SUCCESS) {
	    return status;
	}
	rightPage = (idxNodeT *)currPage->pageBuf;

        if ((rightPage->total_elements > 0) &&
	    (page->data[page->total_elements - 2].search_key > 
	     rightPage->data[0].search_key))
        {
            writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_240, 
			     "Directory index, page %ld right sibling page %ld\n"),
		     pageNum, page->page_right);
	    writemsg(SV_VERBOSE | SV_LOG_FOUND | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_241,
			     "has elements smaller than this page.\n"));
	    status = release_page_to_cache(currVol, currLbn, currPage,
					   FALSE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    return CORRUPT;
        }
        if (((tree == FFREE) && (rightPage->sib.page_left != pageNum)) ||
	    ((tree == FNAME) && (rightPage->sib.leftmost != 0)))
	{
            writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		     catgets(_m_catd, S_FSCK_1, FSCK_242, 
			     "Directory index, page %ld right sibling page %ld mismatch.\n"), 
		     pageNum, page->page_right);
	    status = release_page_to_cache(currVol, currLbn, currPage,
					   FALSE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    return CORRUPT;
        }
	status = release_page_to_cache(currVol, currLbn, currPage,
				       FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
    }

    return SUCCESS;
} /* end validate_dir_index_page */


/*
 * Function Name: validate_dir_index_leaf (3.nn)
 *
 * Description: This routine validates that a leaf node of a directory
 *	index is not corrupt.  It should only be called on FNAME nodes.
 *
 *		NOTE: This must be called after overlapping files have
 *			been deleted!
 *
 * Input parameters:
 *	page: The page of the directory index to check.
 *	pageNum: Which page number within the directory index this is.
 *
 * Returns: SUCCESS, CORRUPT, FAILURE, or NO_MEMORY.
 * 
 */
static int 
validate_dir_index_leaf(idxNodeT *page, 
			pageNumT pageNum,
			fobtoLBNT *dirXtnt, 
			xtntNumT dirXtntSize)
{
    char		*funcName = "validate_dir_index_leaf";
    int			i, j;
    uint64_t            key;
    uint64_t            tmpkey;
    char		filename[ADVFS_METADATA_PGSZ];
    int64_t		offset;
    int64_t		dirPageNum;
    int64_t		dirPageOffset;
    int			status;
    lbnNumT		currLbn;
    volNumT		currVol;
    pageT		*dirPage;
    fs_dir_entry	*dirEntry;
    int			minEntrySize;
    int			maxEntrySize;

    minEntrySize = MIN_DIR_ENTRY_SIZE;
    maxEntrySize = DIRBLKSIZ;

    for (i = 0 ; i < page->total_elements ; i++) {
	offset = page->data[i].loc.dir_offset;
	dirPageNum = offset / ADVFS_METADATA_PGSZ;
	dirPageOffset = offset % ADVFS_METADATA_PGSZ;

	status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
				     &currLbn, &currVol);
	if (status != SUCCESS || currLbn == XTNT_TERM) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		     catgets(_m_catd, S_FSCK_1, FSCK_246, 
			     "Directory index with bad directory offset.\n"));
	    return CORRUPT;
	}
	status = read_page_from_cache(currVol, currLbn, &dirPage);
	if (status != SUCCESS) {
	    return status;
	}
	
	/*
	 * Have to check for dir entry validity because the page offset might
	 * point to the middle of a real dir entry instead of the beginning.
	 */
	dirEntry = (fs_dir_entry *)((char *)dirPage->pageBuf + dirPageOffset);

	/* 
	 * calculate the expected size of the directory entry.  Add
	 * the directory header (first three elements of the directory
	 * entry struct) to the namesize and pad up to the next 4 byte
	 * boundary, then add space for the trailing tag sequence
	 * value.  Then pad the whole thing up to the next 8 byte
	 * boundary.
	 */

	if (dirEntry->fs_dir_bs_tag_num == 0 ||
	    dirEntry->fs_dir_namecount < 1 ||
	    dirEntry->fs_dir_namecount > FS_DIR_MAX_NAME ||
	    dirEntry->fs_dir_size < minEntrySize ||
	    dirEntry->fs_dir_size > maxEntrySize ||
	    dirEntry->fs_dir_size < get_min_dirsize(dirEntry)) {

	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_244, 
			     "Directory index points to deleted or invalid directory entry.\n"));
	    status = release_page_to_cache(currVol, currLbn, dirPage,
					   FALSE, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }
	    return CORRUPT;
	} /* end if invalid directory entry. */
	strncpy(filename, dirEntry->fs_dir_name_string,
		dirEntry->fs_dir_namecount);
	
	/*
	 * Calculate hash key
	 */
	key = 0;
	for (j = 0 ; j < dirEntry->fs_dir_namecount ; j++) {
	    if ('\0' == filename[j]) {
		break;
	    }
	    key = (key << 4) + filename[j];
	    tmpkey = key & 0xF000000000000000;
	    if (0 != tmpkey) {
		key ^= tmpkey >> 56;
	    }
	    key &= ~tmpkey;
	} /* End hash key for loop */
	key = key << 8;
	
	status = release_page_to_cache(currVol, currLbn, dirPage,
				       FALSE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
	/*
	 * Now check the key versus what was stored.
	 */
	if (key != page->data[i].search_key) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_245, 
			     "Directory index with invalid hash entry.\n"));
	    return CORRUPT;
	}
    } /* End for loop on FNAME entries */

    return SUCCESS;
} /* end validate_dir_index_leaf */


/*
 * Function Name: validate_dir_index_free_space (3.nn)
 *
 * Description: This routine validates the free space referred to from
 *	a page in the FFREE btree.
 *
 *		NOTE: This must be called after overlapping files have
 *			been deleted!
 *
 * Input parameters:
 *	page: The page of the directory index to check.
 *	currFreeOffset: The last offset that was checked in the directory.
 *	pageNum: Which page number within the directory index this is.
 *
 * Output parameters:
 *	currFreeOffset: The last offset checked by this call in the directory.
 *
 * Returns: SUCCESS, CORRUPT, FAILURE, or NO_MEMORY.
 * 
 */
static int 
validate_dir_index_free_space(idxNodeT *page,
			      int64_t *currFreeOffset,
			      pageNumT pageNum,
			      fobtoLBNT *dirXtnt,
			      xtntNumT dirXtntSize)
{
    char		*funcName = "validate_dir_index_free_space";
    pageNumT		dirPageNum;
    int			dirPageOffset;
    pageT		*dirPage;
    lbnNumT		currLbn;
    volNumT		currVol;
    uint64_t		i;
    int			status;
    int64_t		offset;
    int64_t		size;
    fs_dir_entry	*dirEntry;
    bfTagT		tag;

    dirPageNum = *currFreeOffset / ADVFS_METADATA_PGSZ;
    dirPageOffset = *currFreeOffset % ADVFS_METADATA_PGSZ;

    status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
				 &currLbn, &currVol);
    if (status != SUCCESS || currLbn == XTNT_TERM) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FSCK_1, FSCK_246, 
			 "Directory index with bad directory offset.\n"));
	return CORRUPT;
    }
    status = read_page_from_cache(currVol, currLbn, &dirPage);
    if (status != SUCCESS) {
	return status;
    }

    for (i = 0;
	 i < page->total_elements && 
	 dirPageNum < FOB2PAGE(dirXtnt[dirXtntSize].fob);
	 i++) {
	offset = page->data[i].search_key;
	size   = page->data[i].loc.free_size;

	while (*currFreeOffset < offset &&
	       dirPageNum < FOB2PAGE(dirXtnt[dirXtntSize].fob)) {
	    /*
	     * This should be used space - verify it.
	     */
	    dirEntry = (fs_dir_entry *)((char *)dirPage->pageBuf +
					dirPageOffset);
	    if (dirEntry->fs_dir_bs_tag_num == 0) {
		/*
		 * It's free - check if it was marked lost special case
		 * for directory index code problems.
		 */
		GETTAG(tag, dirEntry);
		if (tag.tag_seq != (uint32_t)-1) {
		    /*
		     * This is not the special case that's OK.
		     */
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FSCK_1, FSCK_247,
				     "Directory index with missing free space.\n"));
		    status = release_page_to_cache(currVol, currLbn, dirPage,
						   FALSE, FALSE);
		    if (status != SUCCESS) {
			return status;
		    }
		    return CORRUPT;
		}
	    } /* end if free directory entry */

	    *currFreeOffset += dirEntry->fs_dir_size;
	    dirPageNum = *currFreeOffset / ADVFS_METADATA_PGSZ;
	    dirPageOffset = *currFreeOffset % ADVFS_METADATA_PGSZ;

	    if (*currFreeOffset % ADVFS_METADATA_PGSZ == 0 &&
		dirPageNum < FOB2PAGE(dirXtnt[dirXtntSize].fob)) {
		/*
		 * Reached end of page.  Release current and get new one.
		 */
		status = release_page_to_cache(currVol, currLbn, dirPage,
					       FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}

		status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
					     &currLbn, &currVol);
		if (status != SUCCESS || currLbn == XTNT_TERM) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FSCK_1, FSCK_246,
				     "Directory index with bad directory offset.\n"));
		    return CORRUPT;
		}
		status = read_page_from_cache(currVol, currLbn, &dirPage);
		if (status != SUCCESS) {
		    return status;
		}
	    } /* End if end of page */
	} /* End while looking at used space */

	while (*currFreeOffset < offset + size &&
	       dirPageNum < FOB2PAGE(dirXtnt[dirXtntSize].fob)) {
	    /*
	     * This should be free space - verify it.
	     */
	    dirEntry = (fs_dir_entry *)((char *)dirPage->pageBuf +
					dirPageOffset);
	    if (dirEntry->fs_dir_bs_tag_num != 0) {
		/*
		 * It's in use - error.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FSCK_1, FSCK_248, 
				 "Directory index with an in use directory entry marked as free.\n"));
		status = release_page_to_cache(currVol, currLbn, dirPage,
					       FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		return CORRUPT;
	    } /* End if dir entry is in use */
	    *currFreeOffset += dirEntry->fs_dir_size;
	    dirPageNum = *currFreeOffset / ADVFS_METADATA_PGSZ;
	    dirPageOffset = *currFreeOffset % ADVFS_METADATA_PGSZ;

	    if (*currFreeOffset % ADVFS_METADATA_PGSZ == 0 &&
		dirPageNum < FOB2PAGE(dirXtnt[dirXtntSize].fob)) {
		/*
		 * Reached end of page.  Release current and get new one.
		 */
		status = release_page_to_cache(currVol, currLbn, dirPage,
					       FALSE, FALSE);
		if (status != SUCCESS) {
		    return status;
		}
		status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
					     &currLbn, &currVol);
		if (status != SUCCESS || currLbn == XTNT_TERM) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FSCK_1, FSCK_246,
				     "Directory index with bad directory offset.\n"));
		    return CORRUPT;
		}
		status = read_page_from_cache(currVol, currLbn, &dirPage);
		if (status != SUCCESS) {
		    return status;
		}
	    } /* End if end of page */
	} /* End while looking at free space */
    } /* End for loop on each freespace entry in this index page/node. */

    status = release_page_to_cache(currVol, currLbn, dirPage,
				   FALSE, FALSE);
    if (status != SUCCESS) {
	return status;
    }
    return SUCCESS;
} /* end validate_dir_index_free_space */


/*
 * Function Name: clear_tag_index
 *
 * Description: If the directory index is corrupt we need to modify the
 *              metadata of the directory which points to the dir index.
 *
 * Input parameters:
 *    fileset: The fileset this tag lives in.
 *    tagNode: The tag node structure containing the new information
 *             to be written to the BMTR_FS_DIR_INDEX_FILE structure.
 *
 * Returns: SUCCESS or FAILURE.
 *
 */
static int 
clear_tag_index(filesetT *fileset, 
		tagNodeT *tagNode)
{
    char	*funcName = "clear_tag_index";
    domainT	*domain;
    mcidT	*primaryMcell;
    mcidT	currentMcell;
    volumeT	*volume;
    lbnNumT	currLbn;
    volNumT	currVol;
    pageT	*pPage;
    mcidT	nextMcell;
    int		modified;
    bsMPgT	*bmtPage;
    bsMCT	*pMcell;
    bsMRT	*pRecord;
    bfTagT	*pDirIndex;
    int		status;

    /*
     * If this tag's mcell is referenced by both an original and a
     * snapshot tagdir, do nothing.
     */
    if (T_IS_SNAP_ORIG_PMCELL(tagNode->status)) {
	return SUCCESS;
    }

    domain = fileset->domain;

    primaryMcell = &(tagNode->tagMCId);
    currentMcell = *primaryMcell;
    
    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (currentMcell.volume != 0) {
	/*
	 * Read the current page the mcell is located on.
	 */
	volume = &(domain->volumes[currentMcell.volume]);
	status = convert_page_to_lbn(volume->volRbmt->bmtLBN,
				     volume->volRbmt->bmtLBNSize, 
				     currentMcell.page, &currLbn, 
				     &currVol);
	if (status != SUCCESS) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_128, 
			     "Can't convert BMT page %ld to LBN.\n"), 
		     currentMcell.page);
	    return FAILURE;
	}

	status = read_page_from_cache(currVol, currLbn, &pPage);
	if (status != SUCCESS) {
	    return status;
	} 

	modified = FALSE;
	bmtPage = (bsMPgT *)pPage->pageBuf;
	pMcell = &(bmtPage->bsMCA[currentMcell.cell]);
	nextMcell = pMcell->mcNextMCId;

	pRecord = (bsMRT *)pMcell->bsMR0;

	while ((pRecord->type != BSR_NIL) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BMTR_FS_DIR_INDEX_FILE) {
		/*
		 * You only need to set the num,seq to 0 to disable
		 * directory index file, no need to delete the record.
		 */
		pDirIndex = (bfTagT *)((char *)pRecord + sizeof(bsMRT));
		pDirIndex->tag_num = 0;
		pDirIndex->tag_seq = 0;
		tagNode->dirIndexTagNum.tag_num = 0;
		tagNode->dirIndexTagNum.tag_seq = 0;
		modified = TRUE;
		break;
	    }
	    pRecord = (bsMRT *) (((char *)pRecord) +
				 roundup(pRecord->bCnt, sizeof(uint64_t))); 
	} /* end record loop */

	status = release_page_to_cache(currVol, currLbn, pPage, 
				       modified, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	if (modified == TRUE) {
	    /*
	     * We found the BMTR_FS_DIR_INDEX record and fixed it.
	     */
	    return SUCCESS;
	}
	/*
	 * Do we have another mcell in this chain 
	 */
	if (nextMcell.volume != 0) {
	    /*
	     * Reset pointers for the next pass.
	     */
	    currentMcell = nextMcell;
	} else {
	    ZERO_MCID(currentMcell);
	}/* end if nextMcell exists */
    }/* end chain loop */

    /*
     * Couldn't find an BMTR_FS_DIR_INDEX record for this tag - 
     * this should be impossible as correct_bmt_chains will only mark
     * the dir as having one if it realy doea.
     */
    writemsg(SV_DEBUG,
	     catgets(_m_catd, S_FSCK_1, FSCK_249, 
		     "Internal error, non-fatal error so continuing on.\n"));
    writemsg(SV_VERBOSE | SV_LOG_WARN,
	     catgets(_m_catd, S_FSCK_1, FSCK_250, 
		     "Could not find BMTR_FS_DIR_INDEX record for file (%ld.%u).\n"),
	     tagNode->tagSeq.tag_num, tagNode->tagSeq.tag_seq);
    return SUCCESS;
} /* end clear_tag_index */


/*
 * Function Name: move_lost_file
 *
 * Description: This routine attaches unattached files into a
 *              fileset. If the file's parentTag exists and is a
 *              directory, but that directory does not have an entry
 *              for this file, one will be created. Otherwise the
 *              file's parent will be set to the root of the fileset,
 *              and an entry in that directory will be created for
 *              this file.
 *
 *		If this file's parent is a lost directory, link
 *		that directory up instead.
 *
 *		FUTURE: If the file's parent is not valid, we would like to
 *		put this file into a lost+found directory, but since AdvFS
 *		doesn't create one by default, we would have to create it,
 *		and that is not getting into the first release.
 *
 *		NOTE: This must be called after overlapping files have
 *			been deleted!
 *
 * Input parameters:
 *     tag: The tag of the file to be moved.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS or FAILURE.
 *
 */
static int 
move_lost_file(filesetT *fileset, 
	       tagNodeT *tag)
{
    char		*funcName = "move_lost_file";
    char		tempName[MAXPATHLEN]; /* The new name of the file. */
    int			foundpage;
    int			modified;
    tagNodeT		tmpParentTagNode;
    tagNodeT		*pParentTagNode;
    int			status;
    nodeT		*currNode;
    tagNumT		firstTagNum;
    int			nameCount;
    int			entrySize;
    fs_dir_entry	*dirEntry;
    fs_dir_entry	*newDirEntry;
    fs_dir_entry	*nextDirEntry;
    bfTagT		entryTag;
    fobtoLBNT		*xtntArray;
    xtntNumT		xtntArraySize;
    xtntNumT		currXtnt;
    pageNumT		numPagesInXtnt;
    volNumT		currVol;
    lbnNumT		currLbn;
    pageNumT		page;
    pageT		*pPage;
    int			oldEntrySize;
    int			minEntrySize;
    int			lostFound;

    foundpage = FALSE;
    modified = FALSE;
    lostFound = FALSE;
    minEntrySize = MIN_DIR_ENTRY_SIZE;

    /*
     * Special case for the group quota file (which does not live in a
     * directory and so has no directgory parent), 'delete with snapshot'
     * files, and directory indices - don't move them.
     */
    if (T_IS_DELETE_WITH_SNAP(tag->status) ||
	T_IS_DIR_INDEX(tag->status) ||
	tag->tagSeq.tag_num == 5) /* quota file */ {
	return SUCCESS;
    }

    /*
     * Special case for snapshots - delete the tag rather than modify a
     * directory.
     */
    if (FS_IS_SNAP(fileset->status)) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_251, 
			 "Deleting '%s' file system tag %ld.%u because there is no link and the file system is a snapshot.\n"),
		 fileset->fsName,
		 tag->tagSeq.tag_num, tag->tagSeq.tag_seq);
	status = delete_tag(fileset, tag->tagSeq.tag_num);
	if (status != SUCCESS) {
	    return status;
	}
	return SUCCESS;
    }
    /*
     * Special case for zero length files - there's no data here, so why
     * should we create a link to it?
     */
    if (tag->size == 0) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FSCK_1, FSCK_252, 
			 "Deleting '%s' file system tag %ld.%u because there is no link and it has no data.\n"),
		 fileset->fsName,
		 tag->tagSeq.tag_num, tag->tagSeq.tag_seq);
	status = delete_tag(fileset, tag->tagSeq.tag_num);
	if (status != SUCCESS) {
	    return status;
	}
	return SUCCESS;
    }

    /*
     * Create a name for this file.
     */
    switch (tag->tagSeq.tag_num) {
	case 3:
	    if (sprintf(tempName, ".tags") < 0) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_253,
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    if (tag->parentTagNum.tag_num != 2) {
		tag->parentTagNum.tag_num = 2;
		lostFound = TRUE;
	    }
	    break;
	case 4:
	    if (sprintf(tempName, "quotas") < 0) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_253, 
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    if (tag->parentTagNum.tag_num != 2) {
		tag->parentTagNum.tag_num = 2;
		lostFound = TRUE;
	    }
	    break;
	case 5:
	    if (sprintf(tempName, ".group.usage") < 0) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_253, 
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    if (tag->parentTagNum.tag_num != 2) {
		tag->parentTagNum.tag_num = 2;
		lostFound = TRUE;
	    }
	    break;
	default:	
	    if (sprintf(tempName, "fsck_lost_file_%d.%d",
			    tag->tagSeq.tag_num, tag->tagSeq.tag_seq) < 0) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_253,
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    break;
    }

    /*
     * Check if this tag's parent exists.
     */
    tmpParentTagNode.tagSeq.tag_num = tag->parentTagNum.tag_num;
    pParentTagNode = &tmpParentTagNode;
	
    status = find_node(fileset->tagList, (void *)&pParentTagNode, &currNode);
    if (status != SUCCESS) {
	if ((status == NOT_FOUND) ||
	    ((SUCCESS == status) &&
	     (pParentTagNode->tagSeq.tag_seq != tag->parentTagNum.tag_seq))) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		     catgets(_m_catd, S_FSCK_1, FSCK_254, 
			     "Not able to find %s's tag %d's parent %ld.%u.\n"),
		     fileset->fsName, tag->tagSeq.tag_num,
		     tag->parentTagNum.tag_num, tag->parentTagNum.tag_seq);
	    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_255,
			     "Resetting node's parent to root dir.\n"));
	    tag->parentTagNum.tag_num = 2;
	    lostFound = TRUE;
	} else {
	    return status;
	}
    } else {
	/*
	 * found the parent node, verify that it is a directory.
	 */
	if (pParentTagNode->fileType & S_IFMT != S_IFDIR) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FSCK_1, FSCK_256,
			     "File system %s's tag %d's parent tag %d is not a directory (%d).\n"),
		     fileset->fsName, tag->tagSeq.tag_num, 
		     pParentTagNode->tagSeq.tag_num, 
		     pParentTagNode->fileType);
	    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT,
		     catgets(_m_catd, S_FSCK_1, FSCK_255, 
			     "Resetting node's parent to root dir.\n"));
	    tag->parentTagNum.tag_num = 2;
	    lostFound = TRUE;
	}
	else {
	    /*
	     * It is a directory, if it's also lost, move it instead.
	     */
	    if (pParentTagNode->numLinksFound == 0) {
		status = move_lost_file(fileset, pParentTagNode);
		if (status != SUCCESS) {
		    return status;
		}
		if (tag->numLinksFound != 0) {
		    /*
		     * Relinking the parent got this file found.  No need
		     * to continue trying to relink this file.
		     */
		    return SUCCESS;
		}
		/*
		 * This file is still not found.  If its parent is also
		 * still lost, or if the parent has been changed to a
		 * regular file, try to link this file to our lost+found
		 * (root) dir.  Else, falling through will try to add
		 * this file to its parent directory.
		 */
		if ((pParentTagNode->numLinksFound == 0) ||
		    (pParentTagNode->fileType & S_IFMT != S_IFDIR)) {
		    /*
		     * Parent is still lost, or not a directory,
		     * so move this file to lost+found.
		     */
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FSCK_1, FSCK_257, 
				     "File system %s's tag %d's parent tag %d is not valid.\n"),
			     fileset->fsName, tag->tagSeq.tag_num, 
			     pParentTagNode->tagSeq.tag_num);
		    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT,
			     catgets(_m_catd, S_FSCK_1, FSCK_255,
				     "Resetting node's parent to root dir.\n"));
		    tag->parentTagNum.tag_num = 2;
		    lostFound = TRUE;
		}
	    }
	}
    }
    if (lostFound == TRUE) {
	tmpParentTagNode.tagSeq.tag_num = tag->parentTagNum.tag_num;
	pParentTagNode = &tmpParentTagNode;
	status = find_node(fileset->tagList, (void **)&pParentTagNode,
			   &currNode);
	if (status == NOT_FOUND) {
	    /*
	     * Didn't find correct tag - return failure.
	     */
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FSCK_1, FSCK_197,
			     "No root directory (tagnum %ld) for file system %s.\n"),
		     fileset->fsName);
	    return FAILURE;
	} else if (status != SUCCESS) {
	    return status;
	}

	tag->parentTagNum.tag_seq = pParentTagNode->tagSeq.tag_seq;
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_258,
			 "Setting file (%ld.%u)'s parent to (%ld.%u).\n"),
		 tag->tagSeq.tag_num, tag->tagSeq.tag_seq,
		 pParentTagNode->tagSeq.tag_num, 
		 pParentTagNode->tagSeq.tag_seq);
	status = correct_fsstat(fileset, tag);
	if (status != SUCCESS) {
	    return status;
	}
    } else {
	/*
	 * The parent exists, double check the sequence number.
	 */
	if (tag->parentTagNum.tag_seq != pParentTagNode->tagSeq.tag_seq) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FSCK_1, FSCK_259, 
			     "Setting file (%ld.%u)'s parent from (%ld.%u) to (%ld.%u).\n"),
		     tag->tagSeq.tag_num, tag->tagSeq.tag_seq,
		     tag->parentTagNum.tag_num, tag->parentTagNum.tag_seq,
		     pParentTagNode->tagSeq.tag_num, 
		     pParentTagNode->tagSeq.tag_seq);
	    tag->parentTagNum.tag_seq = pParentTagNode->tagSeq.tag_seq;
	    status = correct_fsstat(fileset, tag);
	    if (status != SUCCESS) {
		return status;
	    }
	}
    } /* end if moving file to lost+found */

    /*
     * The tag's parentTagNum now points to a valid directory.
     * Look for a free entry in the directory large enough for this
     * entry.
     */

    /*
     * Initialize newDirEntry and short variable names.  The entrySize
     * value could be computed below in get_min_dirsize(), but that
     * function needs a directory entry struct, which we're now in the
     * process og creating (chicken/egg), so we steal that algorhythm
     * for use here.
     */

    firstTagNum = tag->tagSeq.tag_num;
    nameCount = strlen(tempName);

    entrySize = DIR_HEADER_SIZE + nameCount;
    entrySize += 4 - (entrySize % 4);
    entrySize += sizeof(tagSeqT);
    entrySize = (entrySize + 7) & ~7;

    newDirEntry = (fs_dir_entry *)ffd_malloc(entrySize);
    if (NULL == newDirEntry) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_261, 
			 "directory entry"));
	return NO_MEMORY;
    }

    bzero(newDirEntry, entrySize);

    newDirEntry->fs_dir_bs_tag_num = firstTagNum;
    newDirEntry->fs_dir_size = entrySize;
    newDirEntry->fs_dir_namecount = nameCount;
    strcpy(newDirEntry->fs_dir_name_string, tempName);

    PUTTAG_NUM(tag->tagSeq.tag_num, newDirEntry);
    PUTTAG_SEQ(tag->tagSeq.tag_seq, newDirEntry);

    /*
     * Collect parent directory extents - ensure there are extents.
     */
    status = collect_extents(fileset->domain, fileset,
			     &(pParentTagNode->tagMCId),
			     FALSE, &xtntArray, &xtntArraySize, FALSE);
    if (status != SUCCESS) {
	return status;
    }

    /*
     * loop through each extent in this directory - This routine will only
     * modify one page, so once that's done, break out of the loops.
     */
    for (currXtnt = 0 ; currXtnt < xtntArraySize && modified == FALSE ;
	 currXtnt++) {
	/*
	 * Compute a few variables for this extent
	 */
	numPagesInXtnt = FOB2PAGE(xtntArray[currXtnt+1].fob - 
				  xtntArray[currXtnt].fob);
	currVol = xtntArray[currXtnt].volume;
	currLbn = xtntArray[currXtnt].lbn;
	if (currLbn == XTNT_TERM) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_262, 
			     "Internal error, sparse directory should have already been corrected.\n"));
	    return FAILURE;
	}
	/*
	 * loop through each page in this extent
	 */

	for (page = 0 ; page < numPagesInXtnt && modified == FALSE ; page++) {
	    status = read_page_from_cache(currVol, currLbn, &pPage);
	    if (status != SUCCESS) {
		return status;
	    }

	    dirEntry = (fs_dir_entry *)pPage->pageBuf;

	    /*
	     * loop through each entry on this page
	     */
	    while (((int64_t)dirEntry < 
		   (int64_t)pPage->pageBuf + ADVFS_METADATA_PGSZ) &&
		   modified == FALSE) {
		/*
		 * Is the entry free, and is it large enough for the new one?
		 */
		if ((dirEntry->fs_dir_bs_tag_num == 0) &&
		    (dirEntry->fs_dir_size >= newDirEntry->fs_dir_size)) {
		    /*
		     * Add entry here because there is space for it.
		     */
		    oldEntrySize = dirEntry->fs_dir_size;
		    memcpy(dirEntry, newDirEntry, entrySize);
		    /*
		     * Check if we can split this entry into the used one
		     * and a free one of everything after it, or if there
		     * isn't space for a free one and we need to expand
		     * the used entry to cover the entire old free one.
		     */
		    if (entrySize + minEntrySize < oldEntrySize) {
			/*
			 * Set free space entry following this one.
			 */
			nextDirEntry = (fs_dir_entry *)((char *)dirEntry +
							dirEntry->fs_dir_size);
			nextDirEntry->fs_dir_bs_tag_num = 0;
			nextDirEntry->fs_dir_size =
			    oldEntrySize - entrySize;
		    } else {
			dirEntry->fs_dir_size = oldEntrySize;
		    }
		    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FSCK_1, FSCK_263, 
				     "Added directory entry '%s' to directory %d in '%s' file system.\n"),
			     tempName, pParentTagNode->tagSeq.tag_num,
			     fileset->fsName);
		    modified = TRUE;
		} /* end if entry is free and is large enough for new entry */
		/*
		 * In use entry
		 */
		else {
		    /*
		     * FUTURE: check for duplicate name entries.
		     * This will not go into the first release of
		     * fsck, but should be done in a subsequent
		     * release.
		     */
		}
		dirEntry = (fs_dir_entry *)((char *)dirEntry +
			    dirEntry->fs_dir_size);
	    } /* End loop on each entry on this page */

	    status = release_page_to_cache(currVol, currLbn, pPage,
					   modified, FALSE);
	    if (status != SUCCESS) {
		return status;
	    }

	    currLbn += ADVFS_METADATA_PGSZ_IN_BLKS;

	} /* End for loop on each page */
    } /* End for loop on each extent */

    if (modified == FALSE) {
	/*
	 * There were no free entries large enough for this filename.
	 *
	 * See if a page can be added to this directory.
	 */
	status = append_page_to_file(fileset, pParentTagNode, &foundpage);
	if (status != SUCCESS) {
	    return status;
	}

	if (foundpage == FALSE) {
	    writemsg(SV_NORMAL | SV_LOG_WARN, 
		     catgets(_m_catd, S_FSCK_1, FSCK_481, 
			     "No free pages in this storage domain.\n"));
	    writemsg(SV_NORMAL | SV_LOG_WARN | SV_CONT, 
		     catgets(_m_catd, S_FSCK_1, FSCK_265,
			     "Tag %ld.%u in '%s' file system remains inaccessible.\n"),
		     tag->tagSeq.tag_num, tag->tagSeq.tag_seq, fileset->fsName);
	    free(xtntArray);
	    free(newDirEntry);
	    return SUCCESS;
	}

	/*
	 * Collect parent directory extents again.  Needed because
	 * append_page_to_file changed the extents out from under us.
	 */
	free(xtntArray);
	status = collect_extents(fileset->domain, fileset,
				 &(pParentTagNode->tagMCId),
				 FALSE, &xtntArray, &xtntArraySize, FALSE);
	if (status != SUCCESS) {
	    return status;
	}

	/*
	 * Set currVol and currLbn to the new last page of the dir,
	 * and pull it up to be initialized.
	 */
	currVol = xtntArray[xtntArraySize - 1].volume;
	numPagesInXtnt = FOB2PAGE(xtntArray[xtntArraySize].fob -
				  xtntArray[xtntArraySize - 1].fob);
	currLbn = (xtntArray[xtntArraySize - 1].lbn +
		   (numPagesInXtnt - 1) * ADVFS_METADATA_PGSZ_IN_BLKS);

	status = read_page_from_cache(currVol, currLbn, &pPage);
	if (status != SUCCESS) {
	    return status;
	}
	bzero(pPage->pageBuf, ADVFS_METADATA_PGSZ);
	
	dirEntry = (fs_dir_entry *)pPage->pageBuf;

	/*
	 * Add entry here.
	 */
	memcpy(dirEntry, newDirEntry, entrySize);
	
	dirEntry = (fs_dir_entry *)((char *)dirEntry + entrySize);

	/*
	 * Now set the free entry filling out the first block.
	 */
	dirEntry->fs_dir_bs_tag_num = 0;
	dirEntry->fs_dir_size = DIRBLKSIZ - entrySize;

	dirEntry = (fs_dir_entry *)((char *)dirEntry + DIRBLKSIZ - entrySize);

	/*
	 * Now set the 15 free entries on the rest of the page.
	 */
	while ((int64_t)dirEntry < 
	       (int64_t)pPage->pageBuf + ADVFS_METADATA_PGSZ) {
	    dirEntry->fs_dir_bs_tag_num = 0;
	    dirEntry->fs_dir_size = DIRBLKSIZ;

	    dirEntry = (fs_dir_entry *)((char *)dirEntry + DIRBLKSIZ);
	}

	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FSCK_1, FSCK_263, 
			 "Added directory entry '%s' to directory %d in '%s' file system.\n"),
		 tempName, pParentTagNode->tagSeq.tag_num,
		 fileset->fsName);

	status = release_page_to_cache(currVol, currLbn, pPage, TRUE, FALSE);
	if (status != SUCCESS) {
	    return status;
	}
    } /* End if had to allocate more space in directory */

    free(xtntArray);
    free(newDirEntry);

    /*
     * Now do final cleanup work.
     */
    tag->numLinksFound++;
    if (S_IFDIR == (tag->fileType & S_IFMT)) {
	status = correct_dirs_recurse(fileset, tag, tempName);
	if (status != SUCCESS) {
	    return status;
	}
    }

    return SUCCESS;
} /* end move_lost_file */

/*
 * Function Name: get_min_dirsize
 *
 * Description: This routine computes the size of an entry in a
 *              directory based on the size of the entry name, header
 *              size, and normal padding.  This code was stolen from
 *              the kernel's fs_assemble_dir(), which has not changed
 *              in millenia.
 *
 * NOTE: it is possible that a directory entry size could be larger
 * than the value calculated here.  This can happen if a new directory
 * entry is placed near the end of a disk block, or is placed into a
 * previously used entry slot, for example.  But we're guaranteed that
 * a directory entry can never be smaller than the value caluclated in
 * this function, hence the name "get_MIN_dirsize".
 */

static int
get_min_dirsize(fs_dir_entry *dir_p)
{
    int size = 0;

    /*
     * compute the size of the header plus the name string
     */

    size = DIR_HEADER_SIZE + dir_p->fs_dir_namecount;

    /*
     * pad the size to account for the string space ending on the next
     * 4 byte boundary.  If the record already ends on a 4 byte
     * boundary then pad to ensure the name string is null terminated
     */

    size += 4 - (size % 4);
    
    /*
     * the tag's sequence number is after the padded namestring
     */

    size += sizeof(tagSeqT);

    /* Pad up to the next 8 byte boundary cuz that's where the next
       directory entry is stored. */

    size = (size + 7) & ~7;

#ifdef DEBUG
    if (size != dir_p->fs_dir_size)
	printf("'%s' computed size = %d, dir_p->fs_dir_size = %d\n", 
	       dir_p->fs_dir_name_string, size, dir_p->fs_dir_size);
#endif /* DEBUG */

    return (size);

} /* get_min_dirsize */

/* end fsck_dir.c */


