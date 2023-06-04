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
 *      Advance File System
 *
 * Abstract:
 *
 *      On-disk structure fixer.
 *
 * Date:
 *      Wed Nov 29 11:44:33 PST 2000
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fixfdmn_dir.c,v $ $Revision: 1.1.27.3 $ (DEC) $Date: 2006/03/17 03:04:39 $"

#include "fixfdmn.h"

/*
 * Global
 */
extern nl_catd _m_catd;


/*
 * Function Name: correct_all_dirs (3.72)
 *
 * Description: This routine checks the directory hierarchy.
 *		It first calls correct_dirs_recurse with tag 2 (the top
 *		level directory), then loops through and calls
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
int correct_all_dirs(filesetT *fileset)
{
    char	*funcName = "correct_all_dirs";

    tagNodeT	tmpTagNode;
    tagNodeT	*tagNode;
    tagNodeT	*nextNode;
    nodeT	*currNode;
    int		status;
    int		tagnum;

    tmpTagNode.tagSeq.num = 2; /* tagnum of a fileset's root directory */
    tagNode = &tmpTagNode;

    status = find_node(fileset->tagList, (void **)&tagNode, &currNode);
    if (NOT_FOUND == status) {
	/*
	 * Didn't find correct tagNode - return failure.
	 */
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_197, 
			 "No root directory (tagnum 2) for fileset %s.\n"),
		 fileset->fsName);
	return FAILURE;
    } else if (SUCCESS != status) {
	return status;
    }

    /*
     * Call correct_dirs_recurse with tag 2, the root directory of this
     * fileset.  This will check the directory on-disk structure for every
     * directory found in the hierarchy, and increment numLinksFound for
     * every directory and file found.
     */
    status = correct_dirs_recurse(fileset, tagNode, ".");
    if (SUCCESS != status) {
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
    if (NOT_FOUND == status) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_198,
			  "Can't find first tag node for fileset %s.\n"),
                  fileset->fsName);
        corrupt_exit(NULL);
    }
    else if (SUCCESS != status) {
	return status;
    }
    
    while (NULL != tagNode) {
	/*
	 * Locate the next node first, in case we delete this node.
	 */
	status = find_next_node(fileset->tagList, (void **)&nextNode, 
				&currNode);
	if (SUCCESS != status) {
	    if (NOT_FOUND == status) {
		nextNode = NULL;
	    } else {
		return status;
	    }
	}

	if (0 == tagNode->numLinksFound) {
	    status = move_lost_file(fileset, tagNode);
	    if (SUCCESS != status) {
		return status;
	    }
	} /* End if no links found */

	tagNode = nextNode;
    } /* end next tag loop */

    /*
     * Don't correct numLinks on clones - nothing else to do in directories,
     * so return if this is a clone.
     */
    if (FS_IS_CLONE(fileset->status)) {
	return SUCCESS;
    }

    /*
     * find the first tag node and walk the chain, correcting numLinks.
     */
    status = find_first_node(fileset->tagList, (void **)&tagNode, 
			     &currNode);
    if (NOT_FOUND == status) {
        writemsg (SV_ERR | SV_LOG_ERR, 
		  catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_198,
			  "Can't find first tag node for fileset %s.\n"),
                  fileset->fsName);
        corrupt_exit(NULL);
    }
    else if (SUCCESS != status) {
	return status;
    }
    
    while (NULL != tagNode) {
	/*
	 * Locate the next node first, in case we delete this node.
	 */
	status = find_next_node(fileset->tagList, (void **)&nextNode, 
				&currNode);
	if (SUCCESS != status) {
	    if (NOT_FOUND == status) {
		nextNode = NULL;
	    } else {
		return status;
	    }
	}

	/*
	 * If this is a directory index that no directory points to,
	 * delete it.
	 */
	if (TRUE == T_IS_DIR_INDEX(tagNode->status) &&
	    0 == tagNode->numLinksFound)
	{
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_199,
			     "Deleting directory index %d.%x of fileset %s because nothing points to it.\n"), 
		     tagNode->tagSeq.num, tagNode->tagSeq.seq, fileset->fsName);
	    status = delete_tag(fileset, tagNode->tagSeq.num);
	    if (SUCCESS != status) {
		return status;
	    }
	}
	/*
	 * Special case DELETE_WITH_CLONE because AdvFS doesn't bother
	 * to clean up after itself by resetting numLinks to 0 like it
	 * should.  Also special case DIR_INDEX which has no fsstat record.
	 */
	else if (tagNode->numLinks != tagNode->numLinksFound &&
	    FALSE == T_IS_DIR_INDEX(tagNode->status) &&
	    FALSE == T_IS_DELETE_WITH_CLONE(tagNode->status)) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_200,
			     "Resetting %s's tag (%d.%x) numLinks from %d to %d.\n"),
		     fileset->fsName,
		     tagNode->tagSeq.num, tagNode->tagSeq.seq,
		     tagNode->numLinks, tagNode->numLinksFound);
	    tagNode->numLinks = tagNode->numLinksFound;
	    status = correct_fsstat(fileset, tagNode);
	    if (SUCCESS != status) {
		return status;
	    }
	}

	tagNode = nextNode;
    } /* end next tag loop */

    return SUCCESS;
} /* end correct_all_dirs */


/*
 * Function Name: correct_dirs_recurse (3.nn)
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
int correct_dirs_recurse(filesetT *fileset, tagNodeT *tag, char *path)
{
    char		*funcName = "correct_dirs_recurse";
    int			status;
    int			modified;
    int                 fixed;
    pagetoLBNT		*xtntArray;
    int			xtntArraySize;
    int			currXtnt;
    int			newXtnt;
    int			holeSize;
    int			page;
    pageT		*pPage;
    int			numPagesInXtnt;
    LBNT		currLbn;
    int			currVol;
    dirRec		*pageDirRec;
    fs_dir_entry	*dirEntry;
    bfTagT		*entryTag;
    long		entry;		/* Which entry in the entire dir */
    int			numErrs;	/* Count errors to see if this is */
					/* really a directory. */
    tagNodeT		tmpTagNode;
    tagNodeT		*entryTagNode;
    tagNodeT		*origTagNode;
    nodeT		*node;
    int			minEntrySize;
    int			maxEntrySize;
    int			lastEntryOffset;
    uint32T		firstTagNum;
    uint16T		entrySize;
    uint16T		nameCount;
    char		*name;
    int			i;
    char		newPath[MAXPATHLEN];

    entry = 1;
    numErrs = 0;
    minEntrySize = sizeof(int) + sizeof(bfTagT) + sizeof(directory_header);

    status = collect_extents(fileset->domain, fileset, &(tag->tagPmcell),
			     FALSE, &xtntArray, &xtntArraySize, FALSE);
    if (SUCCESS != status) {
	return status;
    }

    if (0 == xtntArraySize) {
	/*
	 * No extents - this is not a valid directory,
	 * so change it to a normal file.
	 */
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_201, 
			 "Resetting file type for '%s' (%d.%x) to regular file because it has no extents.\n"), 
		 path, tag->tagSeq.num, tag->tagSeq.seq);
	/* Clear all the file type bits. */
	tag->fileType &= ~S_IFMT;
	/* Set regular file bits. */
	tag->fileType |= S_IFREG;

	status = correct_fsstat(fileset, tag);
	if (SUCCESS != status) {
	    return status;
	}
	free(xtntArray);
	return SUCCESS;
    }

    /*
     * loop through each extent in this directory
     */
    fixed = FALSE;
    for (currXtnt = 0 ; currXtnt < xtntArraySize ; currXtnt++) {
	/*
	 * Compute a few variables for this extent
	 */
	numPagesInXtnt = xtntArray[currXtnt+1].page - xtntArray[currXtnt].page;
	currVol = xtntArray[currXtnt].volume;
	currLbn = xtntArray[currXtnt].lbn;
	if ( (currLbn == (LBNT)-1) || (currLbn == (LBNT)-2) ) {
	    fixed = TRUE;
	    /*
	     * This is a hole in a directory - these are not allowed.
	     * Except for directory indeces, the page a directory entry
	     * is on is irrelevant, so get rid of the hole by shifting
	     * all extents back one entry.
	     *
	     * E.g.,    OLD		NEW
	     *		page	lbn	page	lbn
	     *		0	x	0	x
	     *		10	y	10	y
	     *		15	-1	15	z
	     *		50	z	30	q
	     *		65	q	55	-1
	     *		90	-1	55	-1
	     *
	     * The problem with this is it leaves a "hole" at the end of
	     * the file that should be deleted.  this needs to be fixed.
	     *
	     * It's tolerable if and only if the filesize is adjusted to
	     * match the last inuse page.  This is fixed at the end of
	     * this routine.
	     */
	    /*
	     * FUTURE: Directory indeces have to be dealt with as well.
	     */
	    holeSize = xtntArray[currXtnt + 1].page - xtntArray[currXtnt].page;
	    for (newXtnt = currXtnt ; newXtnt < xtntArraySize ; newXtnt++) {
		xtntArray[newXtnt].volume = xtntArray[newXtnt + 1].volume;
		xtntArray[newXtnt].lbn = xtntArray[newXtnt + 1].lbn;
		xtntArray[newXtnt + 1].page -= holeSize;
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
	    numPagesInXtnt = (xtntArray[currXtnt+1].page -
			      xtntArray[currXtnt].page);
	    currVol = xtntArray[currXtnt].volume;
	    currLbn = xtntArray[currXtnt].lbn;

	    assert(currLbn >= 0);
	} /* end if currLbn < 0) */
    } /* end loop on extents */
    
    if (TRUE == fixed) {
	/*
	 * Made changes to the extent array. Need to put them
	 * on disk.
	 */
	status = correct_dir_extents(fileset, tag, 
				     xtntArraySize, xtntArray);
	if (SUCCESS != status) {
	    return status;
	}
    } /* end fixed the extent chain */

    /*
     * loop through each extent in this directory
     */
    for (currXtnt = 0 ; currXtnt < xtntArraySize ; currXtnt++) {
	/*
	 * Compute a few variables for this extent
	 */
	numPagesInXtnt = xtntArray[currXtnt+1].page - xtntArray[currXtnt].page;
	currVol = xtntArray[currXtnt].volume;
	currLbn = xtntArray[currXtnt].lbn;

	/*
	 * loop through each page in this extent
	 */
	for (page = 0 ; page < numPagesInXtnt ; page++) {
	    status = read_page_from_cache(currVol, currLbn, &pPage);
	    if (SUCCESS != status) {
		return status;
	    }

	    if (P_IS_MODIFIED(pPage->status) &&
		FALSE == xtntArray[currXtnt].isOrig)
	    {
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_202, 
				 "Page %d (%d.%d) in directory '%s' (%d.%x) overlaps other metadata.  Deleting this directory.\n"),
			 page + xtntArray[currXtnt].page, currVol, currLbn,
			 path, tag->tagSeq.num, tag->tagSeq.seq);
		status = release_page_to_cache(currVol, currLbn, pPage,
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		status = delete_tag(fileset, tag->tagSeq.num);
		if (SUCCESS != status) {
		    return status;
		}
		return SUCCESS;
	    }

	    pageDirRec = (dirRec *)((char *)pPage->pageBuf + PAGESIZE
				    - sizeof(dirRec));
	    dirEntry = (fs_dir_entry *)pPage->pageBuf;
	    maxEntrySize = DIRBLKSIZ;
	    lastEntryOffset = 0;
	    modified = FALSE;

	    if (TRUE == xtntArray[currXtnt].isOrig) {
		while ((long)dirEntry < (long)pPage->pageBuf + PAGESIZE) {
		    if (0 != dirEntry->fs_dir_header.fs_dir_bs_tag_num) {
			name = dirEntry->fs_dir_name_string;
			tmpTagNode.tagSeq.num = 
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num;
			entryTagNode = &tmpTagNode;
			status = find_node(fileset->tagList,
					   (void **)&entryTagNode, &node);
			if (NOT_FOUND == status) {
			    /*
			     * This tag doesn't exist in the clone.  But we
			     * know it exists in the original fileset because
			     * this directory was already checked.  So, link
			     * the clone's tag to the original's primary mcell.
			     */
			    writemsg(SV_VERBOSE | SV_LOG_FOUND,
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_203, 
					     "Clone %s's tag %d (%s/%s) not found.  Linking this tag to fileset %s's tag %d's primary mcell.\n"),
				     fileset->fsName,
				     tmpTagNode.tagSeq.num,
				     path, name,
				     fileset->origClonePtr->fsName,
				     tmpTagNode.tagSeq.num);

			    entryTag = GETTAGP(dirEntry);
			    tmpTagNode.tagSeq.num = entryTag->num;
			    origTagNode = &tmpTagNode;
			    status = find_node(fileset->origClonePtr->tagList,
					       (void **)&origTagNode, &node);
			    if (SUCCESS != status) {
				return status;
			    }
			    status = correct_tag_to_mcell(fileset,
							  entryTag->num,
							  entryTag->seq,
							  origTagNode->tagPmcell.vol,
							  origTagNode->tagPmcell.page,
							  origTagNode->tagPmcell.cell);
			    if (SUCCESS != status) {
				return status;
			    }
			    tmpTagNode.tagSeq.num = entryTag->num;
			    entryTagNode = &tmpTagNode;
			    status = find_node(fileset->tagList,
					       (void **)&entryTagNode, &node);
			    if (SUCCESS != status) {
				return status;
			    }
			    /*
			     * Now copy the data from the original's tagNode.
			     */
			    entryTagNode->size	     = origTagNode->size;
			    entryTagNode->fileType   = origTagNode->fileType;
			    entryTagNode->pagesFound = origTagNode->pagesFound;
			    entryTagNode->fragSize   = origTagNode->fragSize;
			    entryTagNode->fragPage   = origTagNode->fragPage;
			    entryTagNode->fragSlot   = origTagNode->fragSlot;
			    entryTagNode->numLinks   = origTagNode->numLinks;
			    entryTagNode->parentTagNum.num = origTagNode->parentTagNum.num;
			    entryTagNode->parentTagNum.seq = origTagNode->parentTagNum.seq;
			    if (T_IS_DIR_INDEX(origTagNode->status)) {
				T_SET_DIR_INDEX(entryTagNode->status);
			    }
			    T_SET_CLONE_ORIG_PMCELL(entryTagNode->status);
			} else if (SUCCESS != status) {
			    return status;
			}
			if (NULL != entryTagNode) {
			    entryTagNode->numLinksFound++;
			    if (S_IFDIR == (entryTagNode->fileType & S_IFMT) &&
				entryTagNode->tagSeq.num != tag->tagSeq.num &&
				entryTagNode->tagSeq.num != tag->parentTagNum.num)
			    {
				if (MAXPATHLEN <= (strlen(path) +
						   strlen(name))) {
				    writemsg(SV_VERBOSE | SV_LOG_INFO,
					     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_204, 
						     "Truncating path '%s/%s' to '.../%s' for fixfdmn output and log messages.\n"),
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
				if (SUCCESS != status) {
				    return status;
				}
			    } /* End if directory */
			} /* End if entry tagnode found */
		    } /* End if inuse entry */
		    entryTagNode = NULL;
		    dirEntry = (fs_dir_entry *)((char *)dirEntry +
						dirEntry->fs_dir_header.fs_dir_size);
		} /* End while dir entries in a cloned directory */
	    } else {
		/*
		 * This page hasn't been checked yet.
		 */

		/*
		 * loop through each entry on this page
		 */
		while ((long)dirEntry < (long)pPage->pageBuf + PAGESIZE) {
		    /*
		     * Check for "." as the first entry.
		     */
		    if (1 == entry) {
			if (dirEntry->fs_dir_header.fs_dir_bs_tag_num !=
			    tag->tagSeq.num)
			{
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num =
				tag->tagSeq.num;
			    numErrs++;
			    modified = TRUE;
			}
			if (20 != dirEntry->fs_dir_header.fs_dir_size) {
			    dirEntry->fs_dir_header.fs_dir_size = 20;
			    numErrs++;
			    modified = TRUE;
			}
			if (1 != dirEntry->fs_dir_header.fs_dir_namecount) {
			    dirEntry->fs_dir_header.fs_dir_namecount = 1;
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
			 * GETTAGP calculates the pointer to the "hidden"
			 * tag/seq at the end of an AdvFS directory entry.
			 */
			entryTag = GETTAGP(dirEntry);
			if (entryTag->num != tag->tagSeq.num) {
			    entryTag->num = tag->tagSeq.num;
			    numErrs++;
			    modified = TRUE;
			}
			if (entryTag->seq != tag->tagSeq.seq) {
			    entryTag->seq = tag->tagSeq.seq;
			    numErrs++;
			    modified = TRUE;
			}
			if (TRUE == modified) {
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_205,
					     "Corrected . directory entry in directory '%s' (%d.%x).\n"),
				     path, tag->tagSeq.num, tag->tagSeq.seq);
			}
			tag->numLinksFound++;
		    } /* End check for "." entry */
		    /*
		     * Check for ".." as the second entry.
		     */
		    else if (2 == entry) {
			if (dirEntry->fs_dir_header.fs_dir_bs_tag_num !=
			    tag->parentTagNum.num)
			{
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num =
				tag->parentTagNum.num;
			    numErrs++;
			    modified = TRUE;
			}
			if (20 != dirEntry->fs_dir_header.fs_dir_size) {
			    dirEntry->fs_dir_header.fs_dir_size = 20;
			    numErrs++;
			    modified = TRUE;
			}
			if (2 != dirEntry->fs_dir_header.fs_dir_namecount) {
			    dirEntry->fs_dir_header.fs_dir_namecount = 2;
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
			entryTag = GETTAGP(dirEntry);
			if (entryTag->num != tag->parentTagNum.num) {
			    entryTag->num = tag->parentTagNum.num;
			    numErrs++;
			    modified = TRUE;
			}
			if (entryTag->seq != tag->parentTagNum.seq) {
			    entryTag->seq = tag->parentTagNum.seq;
			    numErrs++;
			    modified = TRUE;
			}
			if (TRUE == modified) {
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_206, 
					     "Corrected .. directory entry in directory '%s' (%d.%x).\n"),
				     path, tag->tagSeq.num, tag->tagSeq.seq);
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
			     * Dir size must be a multiple of 8192 (PAGESIZE),
			     * they must have at least 2 links, and the number
			     * of actual pages must equal the file size.
			     */
			    if (0 == tag->size % PAGESIZE &&
				tag->numLinks >= 2 &&
				tag->pagesFound == tag->size / PAGESIZE)
			    {
				LBNT newLbn;
				int newVol;
				pageT *newPage;
				fs_dir_entry *newDirEntry;

				if (tag->size > PAGESIZE) {
				    status = convert_page_to_lbn(xtntArray,
								 xtntArraySize,
								 1,
								 &newLbn,
								 &newVol);
				    if (SUCCESS != status) {
					return status;
				    }
				    status = read_page_from_cache(newVol,
								  newLbn,
								  &newPage);
				    if (SUCCESS != status) {
					return status;
				    }
				    newDirEntry =
					(fs_dir_entry *)pPage->pageBuf;
				}
				else {
				    newDirEntry =
					(fs_dir_entry *)(pPage->pageBuf +
							 7 * ONE_K);
				}

				firstTagNum =
				    dirEntry->fs_dir_header.fs_dir_bs_tag_num;
				entrySize =
				    dirEntry->fs_dir_header.fs_dir_size;
				nameCount =
				    dirEntry->fs_dir_header.fs_dir_namecount;
				name = dirEntry->fs_dir_name_string;

				if ((entrySize <= DIRBLKSIZ) &&
				    (entrySize >= minEntrySize) &&
				    (0 == entrySize % sizeof(int)))
				{
				    entryTag = GETTAGP(dirEntry);
				}

				if ((entrySize <= DIRBLKSIZ) &&
				    (entrySize >= 0) &&
				    (0 == entrySize % sizeof(int)) &&
				    (0 == firstTagNum)) {
				    /*
				     * This is a valid dir entry.
				     * Consider the file as a directory
				     * by resetting numErrs.
				     */
				    writemsg(SV_DEBUG | SV_LOG_INFO,
					     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_207,
						     "Valid directory (free entry) - not resetting file type.\n"));
				    numErrs = 0;
				}
				else if ((0 != firstTagNum) &&
					 (nameCount >= 1) &&
					 (nameCount <= FS_DIR_MAX_NAME) &&
					 (entrySize >= minEntrySize) &&
					 (entrySize <= DIRBLKSIZ) &&
					 (0 == entrySize % sizeof(int)) &&
					 ((roundup((nameCount + 1),
						   sizeof(int)) +
					   sizeof(directory_header) +
					   sizeof(bfTagT)) <= entrySize) &&
					 (firstTagNum == entryTag->num)) {
				    /*
				     * Also looks like a valid dir entry.
				     * Not performing a find_node because it's
				     * overkill if the format is all correct.
				     */
				    writemsg(SV_DEBUG | SV_LOG_INFO, 
					     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_208,
						     "Valid directory (used entry) - not resetting file type.\n"));
				    numErrs = 0;
				}

				if (tag->size > PAGESIZE) {
				    status = release_page_to_cache(newVol, newLbn,
								   newPage,
								   FALSE, FALSE);
				    if (SUCCESS != status) {
					return status;
				    }
				}
			    } /* End if tag node checks */
			    /*
			     * If we didn't reset numErrs, change the FS_STAT
			     * record to assume this is a regular file.
			     */
			    if (0 != numErrs)
			    {
				/*
				 * Reset file type in taglist to regular file.
				 */
				writemsg(SV_VERBOSE | SV_LOG_FIXED,
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_209, 
						 "Not a directory - resetting file type for '%s' (%d.%x) to regular file.\n"),
					 path,
					 tag->tagSeq.num, tag->tagSeq.seq);
				/* Clear all the file type bits. */
				tag->fileType &= ~S_IFMT;
				/* Set regular file bits. */
				tag->fileType |= S_IFREG;

				status = correct_fsstat(fileset, tag);
				if (SUCCESS != status) {
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
				if (SUCCESS != status) {
				    return status;
				}
				free(xtntArray);
				return SUCCESS;
			    }
			} /* End if numErrs >= 10 */
			tmpTagNode.tagSeq.num =
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num;
			entryTagNode = &tmpTagNode;
			status = find_node(fileset->tagList,
					   (void **)&entryTagNode, &node);
			if (SUCCESS != status) {
			    return status;
			}
			entryTagNode->numLinksFound++;
		    } /* End check for ".." entry */
		    /*
		     * Free space entry
		     */
		    else if (0 == dirEntry->fs_dir_header.fs_dir_bs_tag_num) {
			/*
			 * Entry size is the only thing to check on free
			 * entries.
			 */
			if (0 == dirEntry->fs_dir_header.fs_dir_size ||
			    0 != (dirEntry->fs_dir_header.fs_dir_size %
				  sizeof(int)) ||
			    dirEntry->fs_dir_header.fs_dir_size > maxEntrySize)
			{
			    dirEntry->fs_dir_header.fs_dir_size = maxEntrySize;
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_210, 
					     "Adjusted free entry size in directory '%s' (%d.%x).\n"),
				     path, tag->tagSeq.num, tag->tagSeq.seq);
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
			 * fixfdmn, but should be done in a subsequent
			 * release.
			 */

			/*
			 * Make shorter variable names for the rest of this
			 * section.
			 */
			firstTagNum =
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num;
			entrySize = dirEntry->fs_dir_header.fs_dir_size;
			nameCount = dirEntry->fs_dir_header.fs_dir_namecount;
			name = dirEntry->fs_dir_name_string;
			if ((entrySize >= minEntrySize) &&
			    (entrySize <= maxEntrySize) &&
			    (0 == entrySize % sizeof(int)))
			{
			    entryTag = GETTAGP(dirEntry);
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
			    (0 != entrySize % sizeof(int)) ||
			    ((roundup((nameCount + 1), sizeof(int)) +
			      sizeof(bfTagT) + sizeof(directory_header)) >
			     entrySize) ||
			    (firstTagNum != entryTag->num) )
			{
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num = 0;
			    dirEntry->fs_dir_header.fs_dir_size = maxEntrySize;
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_211, 
					     "Removing invalid entry from directory '%s' (%d.%x).\n"), 
				     path, tag->tagSeq.num, tag->tagSeq.seq);
			    modified = TRUE;
			}
			else {
			    /*
			     * Entry is internally consistent, now check
			     * if the tag it refers to exists.
			     */
			    tmpTagNode.tagSeq.num = entryTag->num;
			    entryTagNode = &tmpTagNode;
			    status = find_node(fileset->tagList,
					       (void **)&entryTagNode, &node);
			    if (NOT_FOUND == status) {
				dirEntry->fs_dir_header.fs_dir_bs_tag_num = 0;
				/*
				 * Don't increase dir entry size, as there
				 * should be valid dir entries after this
				 * in this block.
				 */
				writemsg(SV_VERBOSE | SV_LOG_FIXED, 
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_212, 
						 "Removing entry (%d.%x) from directory tag '%s' (%d.%x) because tag not found.\n"), 
					 entryTag->num, entryTag->seq, path,
					 tag->tagSeq.num, tag->tagSeq.seq);
				modified = TRUE;
			    }
			    else if (SUCCESS != status) {
				return status;
			    }

			    if ((SUCCESS == status) &&
				(entryTagNode->tagSeq.seq != entryTag->seq)) {
				writemsg(SV_VERBOSE | SV_LOG_FIXED,
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_213,
						 "Modified entry (%d.%x) to (%d.%x) in directory '%s' (%d.%x).\n"),
					 entryTag->num, entryTag->seq,
					 entryTag->num,
					 entryTagNode->tagSeq.seq, path,
					 tag->tagSeq.num, tag->tagSeq.seq);
				entryTag->seq = entryTagNode->tagSeq.seq;
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
				    dirEntry->fs_dir_header.fs_dir_namecount =
					i;
				}
				else {
				    dirEntry->fs_dir_name_string[nameCount] =
					'\0';
				}
				writemsg(SV_VERBOSE | SV_LOG_FIXED, 
					 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_214,
						 "Adjusted name '%s' entry in directory '%s' (%d.%x).\n"),
					 name, path,
					 tag->tagSeq.num, tag->tagSeq.seq);
				modified = TRUE;
			    }
			} /* End of else internally consistent check */

			if (dirEntry->fs_dir_header.fs_dir_bs_tag_num >= 3 &&
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num <= 5 &&
			    tag->tagSeq.num != 2)
			{
			    /*
			     * This is a link to .tags, quota.group, or
			     * quota.user in a directory other than the
			     * root directory.
			     * Blow it away, since it shouldn't be here.
			     */
			    dirEntry->fs_dir_header.fs_dir_bs_tag_num = 0;
			    /*
			     * Don't increase dir entry size, as there should
			     * be valid dir entries after this in this block.
			     */
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_215, 
					     "Removing entry '%s' (%d.%x) from directory '%s' (%d.%x)\n"), 
				     name, entryTag->num, entryTag->seq, 
				     path, tag->tagSeq.num, tag->tagSeq.seq);
			    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_216, 
					     "because it shouldn't exist anywhere but the root directory.\n"));
			    modified = TRUE;
			}

			if (0 != dirEntry->fs_dir_header.fs_dir_bs_tag_num &&
			    NULL != entryTagNode) {
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
				    dirEntry->fs_dir_header.fs_dir_bs_tag_num =
					0;
				    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
					     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_217,
						     "Removing duplicate directory entry '%s' from directory '%s' (%d.%x).\n"), 
					     name, path,
					     tag->tagSeq.num, tag->tagSeq.seq);
				    modified = TRUE;

				    entryTagNode->numLinksFound--;
				}
				else {
				    if ((entryTagNode->parentTagNum.num !=
				         tag->tagSeq.num) ||
					(entryTagNode->parentTagNum.seq !=
				         tag->tagSeq.seq))
				    {
					writemsg(SV_VERBOSE | SV_LOG_FIXED,
						 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_218, 
							 "Resetting '%s' (%d.%x)'s parent from (%d.%x) to '%s' (%d.%x).\n"), 
						 name,
						 entryTagNode->tagSeq.num,
						 entryTagNode->tagSeq.seq,
						 entryTagNode->parentTagNum.num,
						 entryTagNode->parentTagNum.seq,
						 path,
						 tag->tagSeq.num,
						 tag->tagSeq.seq);
					entryTagNode->parentTagNum.num =
					    tag->tagSeq.num;
					entryTagNode->parentTagNum.seq =
					    tag->tagSeq.seq;
					status = correct_fsstat(fileset,
								entryTagNode);
					if (SUCCESS != status) {
					    return status;
					}
				    }
				    if (MAXPATHLEN <= (strlen(path) +
						       strlen(name))) {
					writemsg(SV_VERBOSE | SV_LOG_INFO,
						 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_204,
							 "Truncating path '%s/%s' to '.../%s' for fixfdmn output and log messages.\n"),
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
				    if (SUCCESS != status) {
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
			     *    ((entryTagNode->parentTagNum.num !=
			     *      tag->tagSeq.num) ||
			     *     (entryTagNode->parentTagNum.seq !=
			     *      tag->tagSeq.seq)))
			     *{
			     *    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     *	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_219, 
			     *               "Resetting (%d.%x)'s parent from (%d.%x) to (%d.%x).\n"), 
			     *	     entryTagNode->tagSeq.num, 
			     *	     entryTagNode->tagSeq.seq, 
			     *	     entryTagNode->parentTagNum.num, 
			     *	     entryTagNode->parentTagNum.seq, 
			     *	     tag->tagSeq.num, tag->tagSeq.seq);
			     *    entryTagNode->parentTagNum.num = tag->tagSeq.num;
			     *    entryTagNode->parentTagNum.seq = tag->tagSeq.seq;
			     *    status = correct_fsstat(fileset, entryTagNode);
			     *    if (SUCCESS != status) {
			     *	return status;
			     *    }
			     *}
			     */
			} /* End if valid entry */
		    } /* End if found a good entry */

		    if (PAGESIZE != (lastEntryOffset +
				     dirEntry->fs_dir_header.fs_dir_size)) {
			lastEntryOffset += dirEntry->fs_dir_header.fs_dir_size;
		    }
		    maxEntrySize -= dirEntry->fs_dir_header.fs_dir_size;
		    if (0 == maxEntrySize) {
			maxEntrySize = DIRBLKSIZ;
		    }
		    entryTagNode = NULL;
		    dirEntry = (fs_dir_entry *)((char *)dirEntry +
						dirEntry->fs_dir_header.fs_dir_size);
		    entry++;
		} /* End loop on each entry on this page */

		if (pageDirRec->lastEntry_offset != lastEntryOffset) {
		    writemsg(SV_VERBOSE | SV_LOG_FIXED,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_220,
				     "Resetting last entry offset for directory '%s' (%d.%x) page %d from %d to %d.\n"),
			     path, tag->tagSeq.num, tag->tagSeq.seq, 
			     page + xtntArray[currXtnt].page,
			     pageDirRec->lastEntry_offset, lastEntryOffset);
		    pageDirRec->lastEntry_offset = lastEntryOffset;
		    modified = TRUE;
		}
		/*
		 * The '1' in the following if statement should use "SeqType".
		 *
		 * Unfortunately, "#define SeqType 1;" is in fs_dir.h.
		 * Note the trailing ';'.  In the kernel, it's only used in
		 * the format of "pageDirRec->pageType = SeqType;", which
		 * leaves a trailing ';', or a no-op.  No big deal.  But it
		 * means that the following code, which should SeqType where
		 * it *can't* have that trailing ';', won't compile.
		 */
		if (1 != pageDirRec->pageType) {
		    writemsg(SV_VERBOSE | SV_LOG_FIXED,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_221,
				     "Resetting page type for directory '%s' (%d.%x) page %d from %d to %d.\n"),
			     path, tag->tagSeq.num, tag->tagSeq.seq, 
			     page + xtntArray[currXtnt].page,
			     pageDirRec->pageType, 1);
		    pageDirRec->pageType = 1;
		    modified = TRUE;
		}
	    } /* End if original page or cloned page */

	    status = release_page_to_cache(currVol, currLbn, pPage,
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    modified = FALSE;

	    currLbn += BLOCKS_PER_PAGE;
	} /* End loop on each page */
    } /* End loop on each extent */

    /*
     * Validate fsstat size
     */
    while ((LBNT)-1 == xtntArray[xtntArraySize - 1].lbn) {
	/*
	 * There was a hole in the file, which has been moved to the end
	 * of the file.  Now decrement the extent array size to match the
	 * end of the real extents.
	 */
	xtntArraySize--;
    }

    if (tag->size != xtntArray[xtntArraySize].page * PAGESIZE &&
	FALSE == T_IS_CLONE_ORIG_PMCELL(tag->status))
    {
	writemsg(SV_VERBOSE | SV_LOG_FIXED,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_222, 
			 "Modified size of '%s' (%d.%x) from %d to %d.\n"),
		 path, tag->tagSeq.num, tag->tagSeq.seq, tag->size,
		 xtntArray[xtntArraySize].page * PAGESIZE);
	tag->size = xtntArray[xtntArraySize].page * PAGESIZE;
	correct_fsstat(fileset, tag);
	if (SUCCESS != status) {
	    return status;
	}
    }

    free(xtntArray);

    status = correct_dir_index(fileset, tag);
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
} /* end correct_dirs_recurse */


/*
 * Function Name: correct_dir_index (3.nn)
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
int correct_dir_index(filesetT *fileset, tagNodeT *dirTag)
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
    long		currFreeOffset;
    mcellT		primaryMcell;
    mcellT		currentMcell;
    mcellT		nextMcell;
    LBNT		currLbn;
    int			currVol;
    pageT		*currPage;
    bsMPgT		*bmtPage;
    bsMCT		*pMcell;
    bsMRT		*pRecord;
    pagetoLBNT		*idxXtnt;
    int			idxXtntSize;
    pagetoLBNT		*dirXtnt;
    int			dirXtntSize;
    int			tree;
    int			currPageNum;
    int			levels;
    int			level;
    int			nextLevelPageNum;
    idxNodeT		*indexPage;
    int			i;
    int			childPage;
    LBNT		childLbn;
    int			childVol;

    domain = fileset->domain;
    foundIdxRec = FALSE;
    currFreeOffset = 0;

    /*
     * Return immediately if there is no associated index file.
     */
    if (0 == dirTag->dirIndexTagNum.num) {
	return SUCCESS;
    }

    /*
     * Get the tagNode for the index file.
     */
    tmpTagNode.tagSeq.num = dirTag->dirIndexTagNum.num;
    indexTag = &tmpTagNode;
    status = find_node(fileset->tagList, (void **)&indexTag, &node);
    if (NOT_FOUND == status) {
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_223, 
			 "Deleting directory %d.%x's link to directory index %d.%x, index not found.\n"),
		 dirTag->tagSeq.num, dirTag->tagSeq.seq, 
		 dirTag->dirIndexTagNum.num, dirTag->dirIndexTagNum.seq);
	status = clear_tag_index(fileset, dirTag);
	if (SUCCESS != status) {
	    return status;
	}
	/*
	 * Nothing more to check.
	 */
	return SUCCESS;
    } else if (SUCCESS != status) {
	return status;
    }

    if (indexTag->tagSeq.seq != dirTag->dirIndexTagNum.seq) {
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_224,
			 "Deleting directory %d.%x's link to directory index %d.%x, sequence numbers don't match.\n"),
		 dirTag->tagSeq.num, dirTag->tagSeq.seq, 
		 dirTag->dirIndexTagNum.num, dirTag->dirIndexTagNum.seq);
	status = clear_tag_index(fileset, dirTag);
	if (SUCCESS != status) {
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
    if (0 != indexTag->numLinksFound ||
	FALSE == T_IS_DIR_INDEX(indexTag->status))
    {
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_225, 
			 "Deleting directory %d.%x's link to directory index %d.%x, not an index or already referenced.\n"),
		 dirTag->tagSeq.num, dirTag->tagSeq.seq, 
		 dirTag->dirIndexTagNum.num, dirTag->dirIndexTagNum.seq);
	status = clear_tag_index(fileset, dirTag);
	if (SUCCESS != status) {
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
    primaryMcell.vol  = indexTag->tagPmcell.vol;
    primaryMcell.page = indexTag->tagPmcell.page;
    primaryMcell.cell = indexTag->tagPmcell.cell;

    currentMcell.vol  = primaryMcell.vol;
    currentMcell.page = primaryMcell.page;
    currentMcell.cell = primaryMcell.cell;

    /*
     * Loop until currentMcell is empty [0,0,0], or we found the record.
     */
    while (0 != currentMcell.vol && FALSE == foundIdxRec) {
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
	
	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (SUCCESS != status) {
	    return status;
	} 
	
	bmtPage = (bsMPgT *)currPage->pageBuf;
	pMcell	 = &(bmtPage->bsMCA[currentMcell.cell]);
	
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
				 roundup(pRecord->bCnt, sizeof(int))); 
	} /* end record loop */
	
	/*
	 * Release page
	 */
	status = release_page_to_cache(currVol, currLbn, currPage, 
				       FALSE, FALSE);
	if (SUCCESS != status) {
	    return status;
	}
	
	currentMcell.cell = nextMcell.cell;
	currentMcell.page = nextMcell.page;
	currentMcell.vol  = nextMcell.vol;
    } /* end loop on mcells */
    
    if (FALSE == foundIdxRec) {
	/*
	 * BMTR_FS_INDEX_FILE record not found -
	 * remove the pointer to it from the directory.  Note that this
	 * should never happen because T_IS_DIR_INDEX status bit would
	 * not have been set.
	 */
	writemsg(SV_DEBUG,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_227,
			 "Internal error, but able to continue.\n"));
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_228, 
			 "Deleting directory %d.%x's directory index %d.%x, BMT index record not found.\n"),
		 dirTag->tagSeq.num, dirTag->tagSeq.seq, 
		 dirTag->dirIndexTagNum.num, dirTag->dirIndexTagNum.seq);
	indexTag->numLinksFound = 0;
	status = clear_tag_index(fileset, dirTag);
	if (SUCCESS != status) {
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
    status = collect_extents(domain, fileset, &(indexTag->tagPmcell), FALSE,
			     &idxXtnt, &idxXtntSize, FALSE);
    if (SUCCESS != status) {
	return status;
    }
    status = collect_extents(domain, fileset, &(dirTag->tagPmcell), FALSE,
			     &dirXtnt, &dirXtntSize, FALSE);
    if (SUCCESS != status) {
	return status;
    }
    
    /*
     * Check validity of BMTR_FS_INDEX_FILE record.
     */
    if (idxBmtRec.fname_page < 0 ||
	idxBmtRec.fname_page >= idxXtnt[idxXtntSize].page ||
	idxBmtRec.ffree_page < 0 ||
	idxBmtRec.ffree_page >= idxXtnt[idxXtntSize].page ||
	idxBmtRec.fname_levels < 0 ||
	idxBmtRec.fname_levels > IDX_MAX_BTREE_LEVELS ||
	idxBmtRec.ffree_levels < 0 ||
	idxBmtRec.ffree_levels > IDX_MAX_BTREE_LEVELS)
    {
	writemsg(SV_VERBOSE | SV_LOG_FIXED,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_229, 
			 "Deleting directory %d.%x's directory index %d.%x, invalid BMTR_FS_INDEX_FILE.\n"),
		 dirTag->tagSeq.num, dirTag->tagSeq.seq, 
		 dirTag->dirIndexTagNum.num, dirTag->dirIndexTagNum.seq);
	indexTag->numLinksFound = 0;
	status = clear_tag_index(fileset, dirTag);
	if (SUCCESS != status) {
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
		if (SUCCESS != status) {
		    if (SUCCESS != status || (childLbn == (LBNT)-1) || (childLbn == (LBNT)-2)) {
			writemsg(SV_VERBOSE | SV_LOG_FIXED,
				 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_230,
					 "Deleting directory %d.%x's directory index %d.%x, invalid page %d.\n"),
				 dirTag->tagSeq.num, dirTag->tagSeq.seq, 
				 dirTag->dirIndexTagNum.num, 
				 dirTag->dirIndexTagNum.seq, currPageNum);
			status = CORRUPT;
		    }
		}
		status = read_page_from_cache(currVol, currLbn, &currPage);
		if (SUCCESS != status) {
		    return status;
		}
		indexPage = (idxNodeT *)currPage->pageBuf;

		/*
		 * If this is the leftmost node at this level, save the
		 * pointer to the leftmost node of the next level.
		 */
		if (FNAME == tree) {
		    if (0 != indexPage->sib.leftmost) {
			nextLevelPageNum = indexPage->data[0].loc.node_page;
		    }
		} else {
		    if (-1 == indexPage->sib.page_left) {
			nextLevelPageNum = indexPage->data[0].loc.node_page;
		    }
		}
		
		status = validate_dir_index_page(indexPage, tree, currPageNum,
						 idxXtnt, idxXtntSize);
		if (CORRUPT == status) {
		    status = release_page_to_cache(currVol, currLbn,
						   currPage, FALSE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		    indexTag->numLinksFound = 0;
		    status = clear_tag_index(fileset, dirTag);
		    if (SUCCESS != status) {
			return status;
		    }
		    /*
		     * Nothing more to check.
		     */
		    return SUCCESS;
		}
		else if (SUCCESS != status) {
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
			if (SUCCESS != status ||( childLbn == (LBNT)-1)||( childLbn == (LBNT)-2) ) {
			    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
				     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_231,
					     "Deleting directory %d.%x's directory index %d.%x, invalid child %d of page %d.\n"),
				     dirTag->tagSeq.num, 
				     dirTag->tagSeq.seq, 
				     dirTag->dirIndexTagNum.num, 
				     dirTag->dirIndexTagNum.seq, 
				     childPage, currPageNum);
			    /*
			     * This status is checked about 15 lines down.
			     */
			    status = CORRUPT;
			    break;
			}
		    }
		} else {
		    if (FNAME == tree) {
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
		if (CORRUPT == status) {
		    status = release_page_to_cache(currVol, currLbn,
						   currPage, FALSE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		    indexTag->numLinksFound = 0;
		    status = clear_tag_index(fileset, dirTag);
		    if (SUCCESS != status) {
			return status;
		    }
		    /*
		     * Nothing more to check.
		     */
		    return SUCCESS;
		}
		else if (SUCCESS != status) {
		    return status;
		}

		currPageNum = indexPage->page_right;

		status = release_page_to_cache(currVol, currLbn, currPage,
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
	    } /* End while loop on all pages at this level */

	    if (-1 == nextLevelPageNum) {
		/*
		 * Error - the first page should have set nextLevelPageNum.
		 * This dir index is corrupt, so toast it.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_232,
				 "Deleting directory %d.%x's directory index %d.%x, invalid level links.\n"),
			 dirTag->tagSeq.num, dirTag->tagSeq.seq, 
			 dirTag->dirIndexTagNum.num, 
			 dirTag->dirIndexTagNum.seq);
		indexTag->numLinksFound = 0;
		status = clear_tag_index(fileset, dirTag);
		if (SUCCESS != status) {
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
int validate_dir_index_page(idxNodeT *page, int tree, int pageNum,
			    pagetoLBNT *idxXtnt, int idxXtntSize)
{
    char	*funcName = "validate_dir_index_page";
    int		i;
    idxNodeT	*rightPage;
    int		status;
    int		currVol;
    LBNT	currLbn;
    pageT	*currPage;
    
    if (page->total_elements > IDX_MAX_ELEMENTS) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_233, 
			 "Directory index has too many (%d) elements on page %d.\n"),
		 page->total_elements, pageNum);
        return CORRUPT;
    }

    if ((0 != page->reserved[0]) ||
	(0 != page->reserved[1]))
    {
	writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_234,
			 "Directory index page %d's reserved fields not zeroed.\n"),
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
    if (page->total_elements < 2 && -1 == page->page_right) {
	return SUCCESS;
    }

    /*
     * Can't check last entry since it is a special case.
     * Note that total_elements is apparently the number of
     * elements plus one, and page[total_elements - 2] is the
     * last valid element.
     */
    for (i = 0 ; i < page->total_elements - 2 ; i++) {
        if(page->data[i].search_key > page->data[i+1].search_key) {
            writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_235, 
			     "Directory index, page %d has out of order elements.\n"),
		     pageNum);
	    writemsg(SV_VERBOSE | SV_LOG_FOUND | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_236,
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
    if (page->page_right != -1)
    {
	if (0 == page->total_elements) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_237,
			     "Directory index with too few entries on page %d.\n"),
		     pageNum);
	    return CORRUPT;
	}
	if (page->page_right == pageNum) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_238,
			     "Directory index with right sibling of page %d pointing to itself.\n"),
		     pageNum);
	    return CORRUPT;
	}

	status = convert_page_to_lbn(idxXtnt, idxXtntSize, page->page_right,
				     &currLbn, &currVol);
	if (SUCCESS != status || ( currLbn == (LBNT)-1 ) ||( currLbn == (LBNT)-2)) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_239,
			     "Directory index with invalid right sibling of page %d.\n"),
		     pageNum);
	    return CORRUPT;
	}
	status = read_page_from_cache(currVol, currLbn, &currPage);
	if (SUCCESS != status) {
	    return status;
	}
	rightPage = (idxNodeT *)currPage->pageBuf;

        if ((rightPage->total_elements > 0) &&
	    (page->data[page->total_elements - 2].search_key > 
	     rightPage->data[0].search_key))
        {
            writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_240, 
			     "Directory index, page %d right sibling page %d\n"),
		     pageNum, page->page_right);
	    writemsg(SV_VERBOSE | SV_LOG_FOUND | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_241,
			     "has elements smaller than this page.\n"));
	    status = release_page_to_cache(currVol, currLbn, currPage,
					   FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    return CORRUPT;
        }
        if (((tree == FFREE) && (pageNum != rightPage->sib.page_left)) ||
	    ((tree == FNAME) && (0 != rightPage->sib.leftmost)))
	{
            writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_242, 
			     "Directory index, page %d right sibling page %d mismatch.\n"), 
		     pageNum, page->page_right);
	    status = release_page_to_cache(currVol, currLbn, currPage,
					   FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    return CORRUPT;
        }
	status = release_page_to_cache(currVol, currLbn, currPage,
				       FALSE, FALSE);
	if (SUCCESS != status) {
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
int validate_dir_index_leaf(idxNodeT *page, int pageNum,
			    pagetoLBNT *dirXtnt, int dirXtntSize)
{
    char		*funcName = "validate_dir_index_leaf";
    int			i, j;
    int			ret;
    unsigned long	key;
    unsigned long	tmpkey;
    char		filename[PAGESIZE];
    long		offset;
    long		dirPageNum;
    long		dirPageOffset;
    int			status;
    LBNT		currLbn;
    int			currVol;
    pageT		*dirPage;
    fs_dir_entry	*dirEntry;
    int			minEntrySize;
    int			maxEntrySize;

    minEntrySize = sizeof(int) + sizeof(bfTagT) + sizeof(directory_header);
    maxEntrySize = DIRBLKSIZ;

    for (i = 0 ; i < page->total_elements ; i++) {
	offset = page->data[i].loc.dir_offset;
	dirPageNum = offset / PAGESIZE;
	dirPageOffset = offset % PAGESIZE;

	status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
				     &currLbn, &currVol);
	if (SUCCESS != status || (currLbn == (LBNT)-1) || (currLbn == (LBNT)-2)) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_246, 
			     "Directory index with bad directory offset.\n"));
	    return CORRUPT;
	}
	status = read_page_from_cache(currVol, currLbn, &dirPage);
	if (SUCCESS != status) {
	    return status;
	}
	
	/*
	 * Have to check for dir entry validity because the page offset might
	 * point to the middle of a real dir entry instead of the beginning.
	 */
	dirEntry = (fs_dir_entry *)((char *)dirPage->pageBuf + dirPageOffset);
	if (0 == dirEntry->fs_dir_header.fs_dir_bs_tag_num ||
	    dirEntry->fs_dir_header.fs_dir_namecount < 1 ||
	    dirEntry->fs_dir_header.fs_dir_namecount > FS_DIR_MAX_NAME ||
	    dirEntry->fs_dir_header.fs_dir_size < minEntrySize ||
	    dirEntry->fs_dir_header.fs_dir_size > maxEntrySize ||
	    ((roundup((dirEntry->fs_dir_header.fs_dir_namecount + 1),
		      sizeof(int)) +
	      sizeof(bfTagT) + sizeof(directory_header)) > 
	     dirEntry->fs_dir_header.fs_dir_size))
	{
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_244, 
			     "Directory index that points to deleted or invalid directory entry.\n"));
	    status = release_page_to_cache(currVol, currLbn, dirPage,
					   FALSE, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }
	    return CORRUPT;
	} /* end if invalid directory entry. */
	strncpy(filename, dirEntry->fs_dir_name_string,
		dirEntry->fs_dir_header.fs_dir_namecount);
	
	/*
	 * Calculate hash key
	 */
	key = 0;
	for (j = 0 ; j < dirEntry->fs_dir_header.fs_dir_namecount ; j++) {
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
	if (SUCCESS != status) {
	    return status;
	}
	/*
	 * Now check the key versus what was stored.
	 */
	if(key != page->data[i].search_key) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_245, 
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
int validate_dir_index_free_space(idxNodeT *page,
				  long *currFreeOffset,
				  int pageNum,
				  pagetoLBNT *dirXtnt,
				  int dirXtntSize)
{
    char		*funcName = "validate_dir_index_free_space";
    int			dirPageNum;
    int			dirPageOffset;
    pageT		*dirPage;
    LBNT		currLbn;
    int			currVol;
    int			i;
    int			status;
    long		offset;
    long		size;
    fs_dir_entry	*dirEntry;
    bfTagT		*tag;

    dirPageNum = *currFreeOffset / PAGESIZE;
    dirPageOffset = *currFreeOffset % PAGESIZE;

    status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
				 &currLbn, &currVol);
    if (SUCCESS != status || (currLbn == (LBNT) -1) || (currLbn == (LBNT) -2)) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_246, 
			 "Directory index with bad directory offset.\n"));
	return CORRUPT;
    }
    status = read_page_from_cache(currVol, currLbn, &dirPage);
    if (SUCCESS != status) {
	return status;
    }

    for (i = 0 ;
	 i < page->total_elements && dirPageNum < dirXtnt[dirXtntSize].page ;
	 i++) {
	offset = page->data[i].search_key;
	size   = page->data[i].loc.free_size;

	while (*currFreeOffset < offset &&
	       dirPageNum < dirXtnt[dirXtntSize].page) {
	    /*
	     * This should be used space - verify it.
	     */
	    dirEntry = (fs_dir_entry *)((char *)dirPage->pageBuf +
					dirPageOffset);
	    if (0 == dirEntry->fs_dir_header.fs_dir_bs_tag_num) {
		/*
		 * It's free - check if it was marked lost special case
		 * for directory index code problems.
		 */
		tag = GETTAGP(dirEntry);
		if (-1 != tag->seq) {
		    /*
		     * This is not the special case that's OK.
		     */
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_247,
				     "Directory index with missing free space.\n"));
		    status = release_page_to_cache(currVol, currLbn, dirPage,
						   FALSE, FALSE);
		    if (SUCCESS != status) {
			return status;
		    }
		    return CORRUPT;
		}
	    } /* end if free directory entry */
	    *currFreeOffset += dirEntry->fs_dir_header.fs_dir_size;
	    dirPageNum = *currFreeOffset / PAGESIZE;
	    dirPageOffset = *currFreeOffset % PAGESIZE;
	    if (0 == *currFreeOffset % PAGESIZE &&
		dirPageNum < dirXtnt[dirXtntSize].page) {
		/*
		 * Reached end of page.  Release current and get new one.
		 */
		status = release_page_to_cache(currVol, currLbn, dirPage,
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}

		status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
					     &currLbn, &currVol);
		if (SUCCESS != status || (currLbn == (LBNT) -1) || (currLbn == (LBNT) -2)) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_246,
				     "Directory index with bad directory offset.\n"));
		    return CORRUPT;
		}
		status = read_page_from_cache(currVol, currLbn, &dirPage);
		if (SUCCESS != status) {
		    return status;
		}
	    } /* End if end of page */
	} /* End while looking at used space */

	while (*currFreeOffset < offset + size &&
	       dirPageNum < dirXtnt[dirXtntSize].page) {
	    /*
	     * This should be free space - verify it.
	     */
	    dirEntry = (fs_dir_entry *)((char *)dirPage->pageBuf +
					dirPageOffset);
	    if (0 != dirEntry->fs_dir_header.fs_dir_bs_tag_num) {
		/*
		 * It's in use - error.
		 */
		writemsg(SV_VERBOSE | SV_LOG_FOUND,
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_248, 
				 "Directory index with an in use directory entry marked as free.\n"));
		status = release_page_to_cache(currVol, currLbn, dirPage,
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		return CORRUPT;
	    } /* End if dir entry is in use */
	    *currFreeOffset += dirEntry->fs_dir_header.fs_dir_size;
	    dirPageNum = *currFreeOffset / PAGESIZE;
	    dirPageOffset = *currFreeOffset % PAGESIZE;
	    if (0 == *currFreeOffset % PAGESIZE &&
		dirPageNum < dirXtnt[dirXtntSize].page) {
		/*
		 * Reached end of page.  Release current and get new one.
		 */
		status = release_page_to_cache(currVol, currLbn, dirPage,
					       FALSE, FALSE);
		if (SUCCESS != status) {
		    return status;
		}
		status = convert_page_to_lbn(dirXtnt, dirXtntSize, dirPageNum,
					     &currLbn, &currVol);
		if (SUCCESS != status || (currLbn == (LBNT)-1) || (currLbn == (LBNT)-2)) {
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_246,
				     "Directory index with bad directory offset.\n"));
		    return CORRUPT;
		}
		status = read_page_from_cache(currVol, currLbn, &dirPage);
		if (SUCCESS != status) {
		    return status;
		}
	    } /* End if end of page */
	} /* End while looking at free space */
    } /* End for loop on each freespace entry in this index page/node. */

    status = release_page_to_cache(currVol, currLbn, dirPage,
				   FALSE, FALSE);
    if (SUCCESS != status) {
	return status;
    }
    return SUCCESS;
} /* end validate_dir_index_free_space */


/*
 * Function Name: clear_tag_index (3.nn)
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
int clear_tag_index(filesetT *fileset, tagNodeT *tagNode)
{
    char	*funcName = "clear_tag_index";
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
    bfTagT	*pDirIndex;
    int		status;

    /*
     * If this is a clone with the original file's primary mcell,
     * don't do anything.
     */
    if (T_IS_CLONE_ORIG_PMCELL(tagNode->status)) {
	return SUCCESS;
    }

    domain = fileset->domain;

    primaryMcell = &(tagNode->tagPmcell);
    
    currentMcell.vol   = primaryMcell->vol;
    currentMcell.page  = primaryMcell->page;
    currentMcell.cell  = primaryMcell->cell;
    
    /*
     * Loop until currentMcell is empty [0,0,0]
     */
    while (0 != currentMcell.vol) {
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

	while ((pRecord->type != 0) &&
	       (pRecord->bCnt != 0) &&
	       (pRecord < ((bsMRT *) &(pMcell->bsMR0[BSC_R_SZ])))) {

	    if (pRecord->type == BMTR_FS_DIR_INDEX_FILE) {
		/*
		 * You only need to set the num,seq to 0 to disable
		 * directory index file, no need to delete the record.
		 */
		pDirIndex = (bfTagT *)((char *)pRecord + sizeof(bsMRT));
		pDirIndex->num = 0;
		pDirIndex->seq = 0;
		tagNode->dirIndexTagNum.num = 0;
		tagNode->dirIndexTagNum.seq = 0;
		modified = TRUE;
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
	     * We found the BMTR_FS_DIR_INDEX record and fixed it.
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
     * Couldn't find an BMTR_FS_DIR_INDEX record for this tag - 
     * this should be impossible as correct_bmt_chains will only mark
     * the dir as having one if it realy doea.
     */
    writemsg(SV_DEBUG,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_249, 
		     "Internal error, non-fatal error so continuing on.\n"));
    writemsg(SV_VERBOSE | SV_LOG_WARN,
	     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_250, 
		     "Could not find BMTR_FS_DIR_INDEX record for file (%d.%x).\n"),
	     tagNode->tagSeq.num, tagNode->tagSeq.seq);
    return SUCCESS;
} /* end clear_tag_index */


/*
 * Function Name: move_lost_file (3.11)
 *
 * Description: This routine attaches unattached files into a
 *              fileset. If the file's parentTag exists and is a
 *              directory, but does not have an entry for this file,
 *              one will be created. Otherwise the file's parent will
 *              be set to the root of the fileset, and an entry in
 *              that directory will be created for this file.
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
int move_lost_file(filesetT *fileset, tagNodeT *tag)
{
    char		*funcName = "move_lost_file";
    char		tempName[MAXPATHLEN]; /* The new name of the file. */
    int			foundpage;
    int			modified;
    tagNodeT		tmpParentTagNode;
    tagNodeT		*pParentTagNode;
    int			status;
    nodeT		*currNode;
    int			firstTagNum;
    int			nameCount;
    char		*name;
    int			entrySize;
    fs_dir_entry	*dirEntry;
    fs_dir_entry	*newDirEntry;
    fs_dir_entry	*nextDirEntry;
    bfTagT		*entryTag;
    dirRec		*pageDirRec;
    pagetoLBNT		*xtntArray;
    int			xtntArraySize;
    int			currXtnt;
    int			numPagesInXtnt;
    int			currVol;
    LBNT		currLbn;
    int			page;
    pageT		*pPage;
    int			oldEntrySize;
    int			minEntrySize;
    domainT		*domain;
    int			lostFound;

    domain = fileset->domain;
    foundpage = FALSE;
    modified = FALSE;
    lostFound = FALSE;
    minEntrySize = sizeof(int) + sizeof(bfTagT) + sizeof(directory_header);

    /*
     * Special case frag file, delete with clone files, and
     * directory indeces - don't move them.
     */
    if (1 == tag->tagSeq.num ||
	T_IS_DELETE_WITH_CLONE(tag->status) ||
	T_IS_DIR_INDEX(tag->status))
    {
	return SUCCESS;
    }

    /*
     * Special case for clones - delete the tag rather than modify a
     * directory.
     */
    if (FS_IS_CLONE(fileset->status)) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_251, 
			 "Deleting fileset %s's tag %d.%x because there is no link and the fileset is a clone.\n"),
		 fileset->fsName,
		 tag->tagSeq.num, tag->tagSeq.seq);
	status = delete_tag(fileset, tag->tagSeq.num);
	if (SUCCESS != status) {
	    return status;
	}
	return SUCCESS;
    }
    /*
     * Special case for zero length files - there's no data here, so why
     * should we create a link to it?
     */
    if (0 == tag->size) {
	writemsg(SV_VERBOSE | SV_LOG_FOUND,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_252, 
			 "Deleting fileset %s's tag %d.%x because there is no link and it has no data.\n"),
		 fileset->fsName,
		 tag->tagSeq.num, tag->tagSeq.seq);
	status = delete_tag(fileset, tag->tagSeq.num);
	if (SUCCESS != status) {
	    return status;
	}
	return SUCCESS;
    }

    /*
     * Create a name for this file.
     */
    switch (tag->tagSeq.num) {
	case 3:
	    if (0 > sprintf(tempName, ".tags")) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_253,
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    if (tag->parentTagNum.num != 2) {
		tag->parentTagNum.num = 2;
		lostFound = TRUE;
	    }
	    break;
	case 4:
	    if (0 > sprintf(tempName, "quota.user")) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_253, 
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    if (tag->parentTagNum.num != 2) {
		tag->parentTagNum.num = 2;
		lostFound = TRUE;
	    }
	    break;
	case 5:
	    if (0 > sprintf(tempName, "quota.group")) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_253, 
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    if (tag->parentTagNum.num != 2) {
		tag->parentTagNum.num = 2;
		lostFound = TRUE;
	    }
	    break;
	default:	
	    if (0 > sprintf(tempName, "fixfdmn_lost_file_%d.%x",
			    tag->tagSeq.num, tag->tagSeq.seq)) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_253,
				 "sprintf failed.\n"));
		perror(Prog);
		return FAILURE;
	    }
	    break;
    }

    /*
     * Check if this tag's parent exists.
     */
    tmpParentTagNode.tagSeq.num = tag->parentTagNum.num;
    pParentTagNode = &tmpParentTagNode;
	
    status = find_node(fileset->tagList, (void *)&pParentTagNode, &currNode);
    if (SUCCESS != status) {
	if ((NOT_FOUND == status) ||
	    ((SUCCESS == status) &&
	     (pParentTagNode->tagSeq.seq != tag->parentTagNum.seq))) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_254, 
			     "Not able to find %s's tag %d's parent %d.%x.\n"),
		     fileset->fsName, tag->tagSeq.num,
		     tag->parentTagNum.num, tag->parentTagNum.seq);
	    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_255,
			     "Resetting node's parent to root dir.\n"));
	    tag->parentTagNum.num = 2;
	    lostFound = TRUE;
	} else {
	    return status;
	}
    } else {
	/*
	 * found the parent node, verify that it is a directory.
	 */
	if (S_IFDIR != (pParentTagNode->fileType & S_IFMT)) {
	    writemsg(SV_VERBOSE | SV_LOG_FOUND,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_256,
			     "Fileset %s's tag %d's parent tag %d is not a directory (%d).\n"),
		     fileset->fsName, tag->tagSeq.num, 
		     pParentTagNode->tagSeq.num, 
		     pParentTagNode->fileType);
	    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_255, 
			     "Resetting node's parent to root dir.\n"));
	    tag->parentTagNum.num = 2;
	    lostFound = TRUE;
	}
	else {
	    /*
	     * It is a directory, if it's also lost, move it instead.
	     */
	    if (0 == pParentTagNode->numLinksFound) {
		status = move_lost_file(fileset, pParentTagNode);
		if (SUCCESS != status) {
		    return status;
		}
		if (0 != tag->numLinksFound) {
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
		if ((0 == pParentTagNode->numLinksFound) ||
		    (S_IFDIR != (pParentTagNode->fileType & S_IFMT))) {
		    /*
		     * Parent is still lost, or not a directory,
		     * so move this file to lost+found.
		     */
		    writemsg(SV_VERBOSE | SV_LOG_FOUND,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_257, 
				     "Fileset %s's tag %d's parent tag %d is not valid.\n"),
			     fileset->fsName, tag->tagSeq.num, 
			     pParentTagNode->tagSeq.num);
		    writemsg(SV_VERBOSE | SV_LOG_FIXED | SV_CONT,
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_255,
				     "Resetting node's parent to root dir.\n"));
		    tag->parentTagNum.num = 2;
		    lostFound = TRUE;
		}
	    }
	}
    }
    if (TRUE == lostFound) {
	tmpParentTagNode.tagSeq.num = tag->parentTagNum.num;
	pParentTagNode = &tmpParentTagNode;
	status = find_node(fileset->tagList, (void **)&pParentTagNode,
			   &currNode);
	if (NOT_FOUND == status) {
	    /*
	     * Didn't find correct tag - return failure.
	     */
	    writemsg(SV_ERR | SV_LOG_ERR,
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_197,
			     "No root directory (tagnum 2) for fileset %s.\n"),
		     fileset->fsName);
	    return FAILURE;
	} else if (SUCCESS != status) {
	    return status;
	}

	tag->parentTagNum.seq = pParentTagNode->tagSeq.seq;
	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_258,
			 "Setting file (%d.%x)'s parent to (%d.%x).\n"),
		 tag->tagSeq.num, tag->tagSeq.seq,
		 pParentTagNode->tagSeq.num, pParentTagNode->tagSeq.seq);
	status = correct_fsstat(fileset, tag);
	if (SUCCESS != status) {
	    return status;
	}
    } else {
	/*
	 * The parent exists, double check the sequence number.
	 */
	if (tag->parentTagNum.seq != pParentTagNode->tagSeq.seq) {
	    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_259, 
			     "Setting file (%d.%x)'s parent from (%d.%x) to (%d.%x).\n"),
		     tag->tagSeq.num, tag->tagSeq.seq,
		     tag->parentTagNum.num, tag->parentTagNum.seq,
		     pParentTagNode->tagSeq.num, pParentTagNode->tagSeq.seq);
	    tag->parentTagNum.seq = pParentTagNode->tagSeq.seq;
	    status = correct_fsstat(fileset, tag);
	    if (SUCCESS != status) {
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
     * Initialize newDirEntry and short variable names
     */
    firstTagNum = tag->tagSeq.num;
    nameCount = strlen(tempName);
    name = tempName;
    entrySize = (sizeof(directory_header) +
		 roundup(nameCount + 1, sizeof(int)) + 
		 sizeof(bfTagT));

    newDirEntry = (fs_dir_entry *)ffd_malloc(entrySize);
    if (NULL == newDirEntry) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_261, 
			 "directory entry"));
	return NO_MEMORY;
    }
    bzero(newDirEntry, entrySize);
    newDirEntry->fs_dir_header.fs_dir_bs_tag_num = firstTagNum;
    newDirEntry->fs_dir_header.fs_dir_size = entrySize;
    newDirEntry->fs_dir_header.fs_dir_namecount = nameCount;
    strcpy(newDirEntry->fs_dir_name_string, tempName);
    entryTag = GETTAGP(newDirEntry);
    entryTag->num = tag->tagSeq.num;
    entryTag->seq = tag->tagSeq.seq;

    /*
     * Collect parent directory extents - ensure there are extents.
     */
    status = collect_extents(fileset->domain, fileset,
			     &(pParentTagNode->tagPmcell),
			     FALSE, &xtntArray, &xtntArraySize, FALSE);
    if (SUCCESS != status) {
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
	numPagesInXtnt = xtntArray[currXtnt+1].page - xtntArray[currXtnt].page;
	currVol = xtntArray[currXtnt].volume;
	currLbn = xtntArray[currXtnt].lbn;
	if ((currLbn == (LBNT) -1 )|| (currLbn == (LBNT)-2)) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_262, 
			     "Internal error, sparse directory should have already been corrected.\n"));
	    return FAILURE;
	}
	/*
	 * loop through each page in this extent
	 */

	for (page = 0 ; page < numPagesInXtnt && modified == FALSE ; page++) {
	    status = read_page_from_cache(currVol, currLbn, &pPage);
	    if (SUCCESS != status) {
		return status;
	    }

	    pageDirRec = (dirRec *)((char *)pPage->pageBuf + PAGESIZE
				    - sizeof(dirRec));
	    dirEntry = (fs_dir_entry *)pPage->pageBuf;

	    /*
	     * loop through each entry on this page
	     */
	    while ((long)dirEntry < (long)pPage->pageBuf + PAGESIZE &&
		   modified == FALSE) {
		/*
		 * Is the entry free, and is it large enough for the new one?
		 * Also ensure we won't blast the page's dirRec.
		 */
		if ((0 == dirEntry->fs_dir_header.fs_dir_bs_tag_num) &&
		    (dirEntry->fs_dir_header.fs_dir_size >=
		     newDirEntry->fs_dir_header.fs_dir_size) &&
		    ((long)dirEntry + newDirEntry->fs_dir_header.fs_dir_size <
		     (long)pageDirRec)) {
		    /*
		     * Add entry here because there is space for it.
		     */
		    oldEntrySize = dirEntry->fs_dir_header.fs_dir_size;
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
							dirEntry->fs_dir_header.fs_dir_size);
			nextDirEntry->fs_dir_header.fs_dir_bs_tag_num = 0;
			nextDirEntry->fs_dir_header.fs_dir_size =
			    oldEntrySize - entrySize;
			/*
			 * Check if pageDirRec->lastEntry_offset needs to be
			 * adjusted.
			 */
			if (((long)nextDirEntry - (long)pPage->pageBuf) >
			    pageDirRec->lastEntry_offset) {
			    pageDirRec->lastEntry_offset =
				(long)nextDirEntry - (long)pPage->pageBuf;
			}
		    } else {
			dirEntry->fs_dir_header.fs_dir_size = oldEntrySize;
		    }
		    writemsg(SV_VERBOSE | SV_LOG_FIXED, 
			     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_263, 
				     "Added directory entry '%s' to directory %d in fileset '%s'.\n"),
			     tempName, pParentTagNode->tagSeq.num,
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
		     * fixfdmn, but should be done in a subsequent
		     * release.
		     */
		}
		dirEntry = (fs_dir_entry *)((char *)dirEntry +
			    dirEntry->fs_dir_header.fs_dir_size);
	    } /* End loop on each entry on this page */

	    status = release_page_to_cache(currVol, currLbn, pPage,
					   modified, FALSE);
	    if (SUCCESS != status) {
		return status;
	    }

	    currLbn += BLOCKS_PER_PAGE;
	} /* End for loop on each page */
    } /* End for loop on each extent */

    if (FALSE == modified) {
	/*
	 * There were no free entries large enough for this filename.
	 *
	 * See if a page can be added to this directory.
	 */
	status = append_page_to_file(fileset, pParentTagNode, &foundpage);
	if (SUCCESS != status) {
	    return status;
	}

	if (FALSE == foundpage) {
	    writemsg(SV_NORMAL | SV_LOG_WARN, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_481, 
			     "No free pages in this domain.\n"));
	    writemsg(SV_NORMAL | SV_LOG_WARN | SV_CONT, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_265,
			     "Tag %d.%x in fileset %s remains inaccessible.\n"),
		     tag->tagSeq.num, tag->tagSeq.seq, fileset->fsName);
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
				 &(pParentTagNode->tagPmcell),
				 FALSE, &xtntArray, &xtntArraySize, FALSE);
	if (SUCCESS != status) {
	    return status;
	}

	/*
	 * Set currVol and currLbn to the new last page of the dir,
	 * and pull it up to be initialized.
	 */
	currVol = xtntArray[xtntArraySize - 1].volume;
	numPagesInXtnt = (xtntArray[xtntArraySize].page -
			  xtntArray[xtntArraySize - 1].page);
	currLbn = (xtntArray[xtntArraySize - 1].lbn +
		   (numPagesInXtnt - 1) * BLOCKS_PER_PAGE);

	status = read_page_from_cache(currVol, currLbn, &pPage);
	if (SUCCESS != status) {
	    return status;
	}
	bzero(pPage->pageBuf, PAGESIZE);
	
	pageDirRec = (dirRec *)((char *)pPage->pageBuf + PAGESIZE
				- sizeof(dirRec));
	dirEntry = (fs_dir_entry *)pPage->pageBuf;

	/*
	 * Add entry here.
	 */
	memcpy(dirEntry, newDirEntry, entrySize);
	
	dirEntry = (fs_dir_entry *)((char *)dirEntry + entrySize);

	/*
	 * Now set the free entry filling out the first block.
	 */
	dirEntry->fs_dir_header.fs_dir_bs_tag_num = 0;
	dirEntry->fs_dir_header.fs_dir_size = DIRBLKSIZ - entrySize;

	dirEntry = (fs_dir_entry *)((char *)dirEntry + DIRBLKSIZ - entrySize);

	/*
	 * Now set the 15 free entries on the rest of the page.
	 */
	while ((long)dirEntry < (long)pPage->pageBuf + PAGESIZE) {
	    dirEntry->fs_dir_header.fs_dir_bs_tag_num = 0;
	    dirEntry->fs_dir_header.fs_dir_size = DIRBLKSIZ;

	    dirEntry = (fs_dir_entry *)((char *)dirEntry + DIRBLKSIZ);
	}

	/*
	 * And initialize the dirRec structure.
	 */
	pageDirRec->lastEntry_offset = PAGESIZE - DIRBLKSIZ;
	pageDirRec->largestFreeSpace = 0;
	pageDirRec->pageType = SeqType;

	writemsg(SV_VERBOSE | SV_LOG_FIXED, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_263, 
			 "Added directory entry '%s' to directory %d in fileset '%s'.\n"),
		 tempName, pParentTagNode->tagSeq.num,
		 fileset->fsName);

	status = release_page_to_cache(currVol, currLbn, pPage, TRUE, FALSE);
	if (SUCCESS != status) {
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
	if (SUCCESS != status) {
	    return status;
	}
    }

    return SUCCESS;
} /* end move_lost_file */

/* end fixfdmn_dir.c */

