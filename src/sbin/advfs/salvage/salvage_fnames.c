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
 *      On-disk structure salvager.
 *
 * Date:
 *
 *      Wed Feb 05 15:00:00 1997
 *
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: salvage_fnames.c,v $ $Revision: 1.1.30.1 $ (DEC) $Date: 2006/04/06 03:21:30 $"

#include "salvage.h"

/*
 * Global variables
 */
extern nl_catd _m_catd;

/*
 * Local defines
 */

#define ENDREC_OFFSET (PAGESIZE - sizeof(dirRec) )

/*
 * Local static prototypes
 */

static int process_node (filesetTreeNodeT *pNode,
                         procNodeArgsT    *procArgs);

static int read_dir_data (fsDirDataT *cd );

static int get_dirrec_state (fs_dir_entry *pRec);

static int check_name_and_links (fsDirDataT       *currDirData,
                                 filesetTreeNodeT *pFirstNode,
                                 int              tagNum,
                                 char             *fileName,
                                 int              nameLen);

static int set_filename (filesetTreeNodeT *node, 
                         char             *fileName, 
                         int              nameLen,
                         filesetLLT       *fileset);

static int add_hard_link (filesetLLT       *fileset,
                          int              tagNum,
                          filesetTreeNodeT *treeNode );

static int trim_relative_pathname( filesetLLT *fileset,
                                   char       *pathName );

static int check_requested_file_found (filesetLLT       *fileset,
                                       filesetTreeNodeT *pCurrDirNode, 
                                       char             *reqPath, 
                                       int              *reqPathFound);



/*
 * Design Specification 3.6.1 insert_filenames
 *
 * Description
 *  This function is the frontend for  this functionality.  It makes some
 *  simple checks then calls walk tree on the fileset (and lost+found). 
 *  When done it has filled in the names for the files in the file
 *  directory tree structure.
 *
 * Input parameters:
 *  fileset: Pointer to fileset structure.
 *
 * Output parameters:
 *  reqPathFound: Pointer to "requested path found" flag.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */

int insert_filenames (filesetLLT *fileset,
                      int        *reqPathFound)
{
    char *funcName = "insert_filenames";
    filesetTagT *pTag = NULL;
    filesetTreeNodeT *pTop = NULL;
    procNodeArgsT procArgs;
    int status = 0;
    extern long nodeCounter;

    /*
     * If recovering a specific path, initialize "requested path found" 
     * flag to "initial value" (for pathname), or "don't check" (for tag 
     * number). If/when we find the files we are looking for, it will be
     * set to "found". If we hit the requested files' parent and do *not* find
     * the files, it is set to "not found", meaning we never will find them.
     * Otherwise, set it to "found", but in this case it is "dont care".
     */
    if ( Options.pathName != NULL )
    {
        if ( strncmp( Options.pathName, ".tags/", 6 ) == 0 )
        {
            *reqPathFound = REQUESTED_PATH_NOCHECK;
        }
        else
        {
            *reqPathFound = REQUESTED_PATH_INITVAL;
        }
    }
    else
    {
        *reqPathFound = REQUESTED_PATH_FOUND;
    }

    /*
     * If recovering a specific date range, check and trim the tree for 
     * empty dirs.
     */
    if ( Options.recoverDate.tv_sec > 0 )
    {
        status = trim_tree_ignore_dirs( fileset );
	if (SUCCESS != status) {
	    return status;
	}
    }        

    /*
     * Setup the argument struct for process_dir, and do the walk on the
     * fileset tree.
     */     
    procArgs.fileset = fileset;
    procArgs.reqPathFound = reqPathFound;

    nodeCounter = 0;
    pTop = fileset->dirTreeHead;
    status = walk_tree( pTop, PARENT_FIRST, process_node, &procArgs );
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Do the walk on the fileset's lost+found tree.
     */
    pTop = fileset->dirTreeHead->nextSibling;
    status = walk_tree( pTop, PARENT_FIRST, process_node, &procArgs );
    if (SUCCESS != status) {
	return status;
    }

    if ((0 != Options.progressReport) && (nodeCounter >= 1000)) {
	/*
	 * Add a line feed to the progress messages.
	 */
	writemsg(SV_ALL | SV_CONT, "\n");
    }

    /*
     * If we were looking for a specific pathname and didn't find it, set 
     * the flag to indicate such.
     */
    if ( *(procArgs.reqPathFound) == REQUESTED_PATH_INITVAL )
    {
        *reqPathFound = REQUESTED_PATH_NOT_FOUND;
    }

    return SUCCESS;
}



/*
 * Design Specification 3.6.2 process_node
 *
 * Description
 *  This function fills in the names for the files in the file
 *  directory tree structure.
 *
 * Input parameters:
 *  pNode: Pointer to the current node in the tree.
 *  procArgs: Pointer to struct containing other argument data.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY.
 *
 * Side effect:
 *  procArgs.reqPathFound: - Flag indicating whether the path to a
 *  specifically requested file (or files) has a) been visited and
 *  completed, and b) if any files were found (if not found, there is
 *  no purpose in continuing to look). The exception would be if a user
 *  requested a specific directory to be restored, and the directory
 *  contained no files, salvage will restore the empty directory as
 *  requested.
 */

static int 
process_node (filesetTreeNodeT *pNode,
              procNodeArgsT    *procArgs)
{
    char *funcName = "process_node";
    extern long nodeCounter;
    int status = 0;
    int checkPathFlag = 0;
    filesetLLT *fileset      = procArgs->fileset;
    int *reqPathFound = procArgs->reqPathFound;

    nodeCounter++;

    if (0 != Options.progressReport) {
	if ((nodeCounter % 1000) == 0) {
	    writemsg(SV_ALL | SV_CONT,
		     catgets(_m_catd, S_SALVAGE_1, SALVAGE_283, 
			     "Processing file %d out of %d, on fileset %s.\r"),
		     nodeCounter, fileset->activeNodes, fileset->fsName);
	}
    }

    /*
     * Check if current node is top of lost+found tree - this is a special
     * case, i.e., the node has no corresponding directory file - just
     * bypass it.
     */
    if ( pNode == fileset->dirTreeHead->nextSibling )
    {
        return SUCCESS;
    }

    /*
     * Check if we will be checking for pathname trimming, etc on this
     * node.
     */
    if ( Options.pathName != NULL && 
         *reqPathFound != READ_ERROR_OCCURRED &&
         *reqPathFound != REQUESTED_PATH_NOCHECK )
    {
        checkPathFlag = 1;
    }

    /*
     * If recovering a specific path, check to see if the current node is
     * part of the requested path. If not, trim it off. The top of the 
     * fileset tree is not checked.
     */
    if ( checkPathFlag == 1  &&  pNode != fileset->dirTreeHead )
    {
        int isInPath = 0;

        status = check_node_in_path( fileset, pNode, Options.pathName, 
                                     &isInPath );
	if (SUCCESS != status) {
	    return status;
	}
        
        if ( isInPath == 0 )
        {
            if ( pNode->fileType != FT_DIRECTORY )
            {
                status = delete_node( &pNode, fileset );
		if (SUCCESS != status) {
		    return status;
		}
            }
            else
            {
                status = delete_subtree( &pNode, fileset );
		if (SUCCESS != status) {
		    return status;
		}
            }

            return SUCCESS;
        }
    }

    /*
     * If this node is not a directory, bypass this tag - no names to read.
     */
    if ( pNode->fileType != FT_DIRECTORY )
    {
        return SUCCESS;
    }

    /*
     * Got a directory to parse. Process it.
     */
    status = process_dir_data( fileset, pNode, checkPathFlag );
    if ( status != SUCCESS )
    {
        /*
         * Process_dir_data() returned an error, which would be caused by a
         * failure in read_dir_data(), resulting in directory nodes going
         * unread and portions of the tree going unnamed. This means that
         * if we are restoring by pathname, we can no longer 1) check our 
         * pathnames and 2) trim the tree as we go, nor can we 3) check for 
         * requested file found (if applicable). So, prompt for abort, and
         * set the flag so we will not ask more than once.
         */
        checkPathFlag = 0;
        
        if ( *reqPathFound != READ_ERROR_OCCURRED )
        {
            writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_124, 
                     "Error occurred reading directory %s, fileset %s\n"),
                     pNode->name, fileset->fsName );
            if ( want_abort() )
            {
                return FAILURE;
            }
        }
        *reqPathFound = READ_ERROR_OCCURRED;
    }

    /*
     * If restoring a specific path, check whether we have 1) found the
     * file(s) we are looking for, or 2) finished the path without finding
     * anything (i.e., any chance of finding the requested files is gone).
     * If no hope, bail out.
     */
    if ((checkPathFlag == 1) && 
	(*reqPathFound == REQUESTED_PATH_INITVAL ))
    {
        status = check_requested_file_found( fileset, pNode, Options.pathName,
                                             reqPathFound );
	if (SUCCESS != status) {
	    return status;
	}
        
        if ( *reqPathFound == REQUESTED_PATH_NOT_FOUND )
        {
            writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_125, 
                     "Requested files not found: %s\n"),
                     Options.pathName );
            return FAILURE;
        }
    }

    return SUCCESS;
}


/* 
 * Design Specification 3.6.3 process_dir_data
 *
 * Description:
 *  This function contains the main loop for stepping through the
 *  directory entries for a given directory data buffer, and inserting
 *  the names into the file directory tree structure.
 *
 * Input parameters:
 *  fileset: Pointer to the current fileset structure.
 *  pDirNode:  Pointer to directory node being processed.
 *  checkPathFlag: Governs whether non-existent dir entries should be created.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY.
 */

int 
process_dir_data (filesetLLT       *fileset, 
                  filesetTreeNodeT *pDirNode,
                  int               checkPathFlag )
{
    char *funcName = "process_dir_data";
    fsDirDataT currDirData;
    fs_dir_entry dirEnt;
    int status = 0;

    bzero( (void *)&currDirData, sizeof(fsDirDataT) );
    currDirData.fileset = fileset;
    currDirData.volumes = fileset->domain->volumes;
    currDirData.dirTagNum = pDirNode->tagNum;

    /*
     * Loop thru the directory data buffer, while return = valid dir entry
     * record.
     */
    while ( ( status = get_next_direntry(&currDirData, &dirEnt) ) == SUCCESS )
    {
        /*
         * Get tmp variables for some stuff.
         */
        char *fileName = dirEnt.fs_dir_name_string;
        int nameLen    = dirEnt.fs_dir_header.fs_dir_namecount;
        uint32T tagNum = dirEnt.fs_dir_header.fs_dir_bs_tag_num;
        filesetTagT *pTag;
        uint32T parentTagNum;

        if ( strcmp(fileName, ".") == 0 || strcmp(fileName, ".." ) == 0 )
        {    
            continue;
        }

	if (tagNum > fileset->tagArraySize)
	{
	    writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_126, 
                     "File %s's tag %d is larger than  arraySize %d\n"),
		     fileName, tagNum, fileset->tagArraySize);
	    continue;
	}
	pTag = fileset->tagArray[tagNum];
	parentTagNum = currDirData.dirTagNum;

        /*
         * If tag for this dir entry exists in tag pointer array, do the name.
         * Note: the pFirstInstance pointer may point to a node which is NOT 
         * in the directory we are processing, i.e., the dir entry is a hard
         * link which has not yet been created in the tree.
         */
        if ( IS_VALID_TAG_POINTER( pTag) )
        {
            filesetTreeNodeT *pFirstInstance = pTag->firstInstance;

            status = check_name_and_links( &currDirData, pFirstInstance, 
                                           tagNum, fileName, nameLen );
	    if (SUCCESS != status) {
		return status;
	    }
        }

        /*
         * Not a valid tag entry - create "empty shell" tag/node structs, to 
         * be detected and handled during the file restoration phase.
         * However, if we are working with a "trimmed tree" (restoring a
         * specific pathname, not the whole fileset), don't do it.
         */
        else if ((pTag == NULL) && (checkPathFlag == 1))
        {
            filesetTreeNodeT *pNewNode = NULL;

            status = create_tag_array_element( &fileset->tagArray[tagNum] );
	    if (SUCCESS != status) {
		return status;
	    }

            pTag = fileset->tagArray[tagNum];
            pTag->fileType = FT_UNKNOWN;
            S_SET_EMPTY( pTag->status );    /* Extent status = empty */

            status = add_new_tree_node( fileset, tagNum, parentTagNum,
                                        fileName, nameLen, pDirNode,
                                        &pNewNode );
	    if (SUCCESS != status) {
		return status;
	    }

            pTag->firstInstance = pNewNode;
        }
    }   /* end  while */

    /*
     * When the loop terminates, this is the status from the 
     * get_next_direntry() call - there is no other way to get here.
     */
    if ( status == NO_MORE_ENTRIES )
    {
        return SUCCESS;
    }
    else
    {
        /*
         * If we're here, it is because read_dir_data() failed, e.g., 
         * corrupt disk block, bad read request, etc. This will probably
         * result in portions of the tree to go unnamed.
         */
        return status;
    }
}
 


/* 
 * Design Specification 3.6.4 get_next_direntry
 *
 * Description:
 *  This function parses through the directory data buffer,
 *  returning a pointer to the next valid directory entry in the
 *  buffer.
 *
 * Input parameters:
 *  cd: Pointer to current directory data structure.
 *  pReturn: Pointer to (fully allocated) directory entry structure. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, or NO_MORE_ENTRIES.
 */

int
get_next_direntry (fsDirDataT    *cd,
                   fs_dir_entry  *pReturn)
{
    char *funcName = "get_next_direntry";
    char *datBuf = cd->fsDirBuf;
    int currentOffset = cd->currentOffset;
    filesetTagT *pDirTag = cd->fileset->tagArray[cd->dirTagNum];
    char *dirName = pDirTag->firstInstance->name;
    fs_dir_entry *pDirEnt = NULL;
    int status = 0;
    int prevRecSize = 0;
    int isDirFileCorrupt = 0;

    /*
     * If this context has already been exhausted:
     */
    if ( cd->currentOffset == -1 )
    {
        return NO_MORE_ENTRIES;
    }

    /* 
     * Special case for very first call. We read the first page (until we
     * get a good one) and check the first entry. If not valid, we drop 
     * thru, and at this point we are in the same state as if this were *not* 
     * the very first call.
     */
    while ( cd->currDirEntry == NULL )
    {
        status = read_dir_data( cd );
	if (SUCCESS != status) {
	    return status;
	}

        pDirEnt = (fs_dir_entry *)datBuf;

        /*
         * Check the state of the directory entry. 0 = valid, active entry,
         * >0 = unused/deleted, <0 = corruption of some kind.
         */
        status = get_dirrec_state( pDirEnt);
        if ( status == DIRENTRY_VALID )
        {
            cd->currDirEntry = pDirEnt;
            memcpy( pReturn, pDirEnt, sizeof(fs_dir_entry));
            return SUCCESS;
        }
        else if ( status == DIRENTRY_CORRUPT_PAGE )
        {
            isDirFileCorrupt = 1;
            writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_127, 
		     "Corrupt page in directory %s, tag %d, fileset %s\n"), 
		     dirName, cd->dirTagNum, cd->fileset->fsName );
        }
    }

    /*
     * Get the record size for the previous entry, and increment offset to 
     * get next entry.
     */
    prevRecSize = cd->currDirEntry->fs_dir_header.fs_dir_size;
    currentOffset += prevRecSize;

    /*
     * Loop each dir entry in FS data (the tags in these entries all 
     * represent files which are CHILDREN of the current dir buffer.
     * This loop is in two parts - the outer loop moves thru each page
     * of the directory, the inner loops thru the entries in a page until
     * the end of a page is reached. The end of a page is known to be
     * less than the offset of the "dirRec" record and the end of each
     * page.
     */

    do                                                   /* Page loop */
    {
        while ( currentOffset < ENDREC_OFFSET )          /* Within a page */
        {
            pDirEnt = (void *)&datBuf[currentOffset];
            cd->currDirEntry = pDirEnt;
        
            status = get_dirrec_state( pDirEnt );
            if ( status == DIRENTRY_VALID )
            {
                cd->currentOffset = currentOffset;
                memcpy( pReturn, pDirEnt, sizeof(fs_dir_entry));
                return SUCCESS;
            }
            else
            {
                if ( status == DIRENTRY_CORRUPT_PAGE )
                {
                    isDirFileCorrupt = 1;
                    writemsg(SV_DEBUG, 
			     catgets(_m_catd, S_SALVAGE_1, SALVAGE_128, 
			     "Corrupt directory page, tag %d, fileset %s\n"),
			     cd->dirTagNum, cd->fileset->fsName );
                    break;
                }
                else if ( status < 0 )
                {
                    isDirFileCorrupt = 1;
                    writemsg(SV_DEBUG, 
			     catgets(_m_catd, S_SALVAGE_1, SALVAGE_127, 
			     "Corrupt page in directory %s, tag %d, fileset %s\n"), 
			     dirName, cd->dirTagNum, cd->fileset->fsName );
                }
                /*
                 * else ( >0 ), deleted or unused - ignore the record
                 */

                currentOffset += pDirEnt->fs_dir_header.fs_dir_size;
            }
        }   /* end while */

        currentOffset = 0;
    }
    while ( ( status = read_dir_data( cd ) ) == SUCCESS );

    if ( status == NO_MORE_ENTRIES )
    {
        cd->currentOffset = -1;
    }

    if ( isDirFileCorrupt == 1 )
    {
        S_SET_CORRUPT( pDirTag->status );
    }

    return status;
}



/*
 * Design Specification 3.6.5 read_dir_data
 *
 * Description:
 *  This function reads the directory data for the current directory
 *  tag into a buffer.
 *
 * Input parameters:
 *  cd: Pointer to current directory data structure. 
 *
 * Side Effects:
 *  A data pointer is set within the cd data structure.
 *  A counter is initialized within the cd data structure. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE, or NO_MORE_ENTRIES.
 */

static int 
read_dir_data (fsDirDataT *cd )
{
    char *funcName = "read_dir_data";
    int status = 0;
    int volFd = 0;
    LBNT lbn;
    int blocksIntoExtent = 0;
    filesetTagT *pDirTag = cd->fileset->tagArray[cd->dirTagNum];
    
    /*
     * In corrupt domains (e.g., Pass 3), some directory files may show up
     * with no extent records. Simply bail out.
     */
    if ( pDirTag->extentHead ==  NULL )
    {
        return NO_MORE_ENTRIES;
    }

    /*
     * First time - initialize
     */
    if ( cd->currExtentRec == NULL )
    {
        cd->nPages = pDirTag->bytesFound / PAGESIZE;
        cd->currExtentRec = pDirTag->extentHead;
        cd->nPagesInExtent = cd->currExtentRec->extentSize / PAGESIZE;
        cd->currExtentPage = 0;
    }    
    else
    {
        /*
         * End of page: get next page from current extent.
         */
        if ( cd->currExtentPage + 1 < cd->nPagesInExtent )
        {
            cd->currExtentPage++;
        }

        /*
         * End of extent - move to next extent and reinitialize page counter.
         */
        else
        {
            cd->currExtentRec = cd->currExtentRec->next;
            if ( cd->currExtentRec ==  NULL )
            {
                return NO_MORE_ENTRIES;
            }

            cd->nPagesInExtent = cd->currExtentRec->extentSize / PAGESIZE;
            cd->currExtentPage = 0;
        }
    }

    /*
     * If missing/sparse hole, try next extent (until end).
     */
    while ( cd->currExtentRec->volume < 0 )
    {
        cd->currExtentRec = cd->currExtentRec->next;
        if ( cd->currExtentRec ==  NULL )
        {
            return NO_MORE_ENTRIES;
        }

        cd->nPagesInExtent = cd->currExtentRec->extentSize / PAGESIZE;
        cd->currExtentPage = 0;
    }

    /*
     * Read the page
     */
    volFd = cd->volumes[cd->currExtentRec->volume].volFd;
    blocksIntoExtent = cd->currExtentPage * BLOCKS_PER_PAGE;
    lbn   = cd->currExtentRec->diskBlock + blocksIntoExtent;

    status = read_page_by_lbn( volFd, (void *)cd->fsDirBuf, lbn, NULL);
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
}



/* 
 * Design Specification 3.6.6 get_dirrec_state
 *
 * Description:
 *  This function checks a directory entry for validity.
 *
 * Input parameters:
 *  pRec: Pointer to directory structure. 
 *
 * Returns: 
 *  State value for the directory record: 0 = valid & in-use,
 *  >0 = deleted, unused, <0 = corrupt.
 */

static int 
get_dirrec_state (fs_dir_entry *pRec)
{
    char *funcName = "get_dirrec_state";
    uint32T tag = 0;
    uint16T namLen = 0;
    uint16T recLen = 0;
    uint32T *pHidnTag = NULL;
    uint32T *pHidnSeq = NULL;
    uint32T hidnSeq = 0;
    char namBuf[MAXPATHLEN];
    char *pbuf;
    int hiddenOffset = 0;
    int pad = 0;    
    int wordSize = sizeof(uint32T);
    int i = 0;

    bzero( namBuf, MAXPATHLEN );

    /*
     * Parse stuff from the current record.
     */
    pbuf = (char *)pRec;
    tag = pRec->fs_dir_header.fs_dir_bs_tag_num;
    namLen = pRec->fs_dir_header.fs_dir_namecount;
    recLen = pRec->fs_dir_header.fs_dir_size;

    /*
     * Do a couple of sanity checks. Since the Record Length datum is
     * critical for finding records in succession, certain bogus values can 
     * hose up our ability to step thru the page.
     */
    if ( recLen < ( sizeof(directory_header) ) || 
         recLen % sizeof(int) != 0  ||
         recLen > 512 )
    {
        return DIRENTRY_CORRUPT_PAGE;
    }

    /*
     * Name too long.
     */
    if ( namLen > FS_DIR_MAX_NAME )
    {
        return DIRENTRY_CORRUPT_NAMLEN;
    }

    /*
     * Calculate the offset to locate the trailing (or "hidden") tag:sequence
     * numbers at the end of the record. The trailing tag number ("HidnTag")
     * tells us a lot about the state of the record.
     */
    pad = wordSize - (pRec->fs_dir_header.fs_dir_namecount % wordSize);
    hiddenOffset = 8 + pRec->fs_dir_header.fs_dir_namecount + pad;
    pHidnTag = (uint32T *)&pbuf[hiddenOffset];
    pHidnSeq = (uint32T *)&pbuf[hiddenOffset + sizeof(uint32T)];
    hidnSeq = *pHidnSeq & 0xfff;

    /*
     * If both tag number and trailing tag number are 0, this is a new,
     * unused record. The length should also be zero, if not something is
     * wrong.
     */
    if ( tag == 0 && *pHidnTag == 0 )
    {
        if ( namLen == 0 )
        {
            return DIRENTRY_NEVERUSED;
        }
        else
        {
            return DIRENTRY_CORRUPT;
        }
    }

    /*
     * When a file is deleted, its directory entry is not wiped. Its tag
     * number is set to 0, but its trailing tag keeps the old value.
     */
    if ( tag != *pHidnTag )
    {
        if ( tag == 0 )
        {
            return DIRENTRY_DELETED;     /* Might be used later for Pass 3 */
        }
        else
        {
            return DIRENTRY_CORRUPT_MISMATCH;
        }
    }

    strncpy( namBuf, pRec->fs_dir_name_string, namLen );

    /*
     * Look for illegal characters in the name. This code once tested for
     * any control character, but in the kernel these are allowed (as opposed
     * to most shells). Currently the only disallowed characters are '\0' 
     * and "/".
     */
    for ( i = 0 ; i < namLen ; i++ )
    {
        if ( namBuf[i] == '\0' || namBuf[i] == '/' )
        {
            return DIRENTRY_BADCHAR;
        }
    }

    return DIRENTRY_VALID;
}



/* 
 * Design Specification 3.6.7 check_name_and_links
 *
 * Description
 *  This function checks for, and resolves, hard-link ambiguities for a
 *  tag, and sets the name in the tree node.
 *
 * Input parameters: 
 *  currDirData: Pointer to current directory data structure.  
 *  pFirstNode: Pointer to the tag's firstInstance node in
 *              the file directory tree.  
 *  tagNum: An integer representing the file's tag number.
 *  fileName: A character buffer containing the file's name.
 *  nameLen: An integer containing the length of the file's name.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY.  
 */

static int 
check_name_and_links (fsDirDataT       *currDirData,
                      filesetTreeNodeT *pFirstNode,
                      int              tagNum,
                      char             *fileName,
                      int              nameLen)
{
    char *funcName = "check_name_and_links";
    int status = 0;
    filesetLLT *fileset = currDirData->fileset;
    int dirTagNum = currDirData->dirTagNum;
    filesetTagT *pDirTag = fileset->tagArray[dirTagNum];
    filesetTreeNodeT *pDirNode = pDirTag->firstInstance;

    /*
     * Check if the node's parent tag number is the same as the tag number
     * of the current directory being processed AND the name field is NULL.
     * If so, this node is a "first instance" node for the tag. This is the
     * simple case, just fill in the name.
     */
    if ( pFirstNode->parentTagNum == dirTagNum && pFirstNode->name == NULL )
    {
        status = set_filename( pFirstNode, fileName, nameLen, fileset );
	if (SUCCESS != status) {
	    return status;
	}
    }

    /*
     * Parent directory different OR name already present. Either case 
     * represents an additional hard link, whether in the same directory or
     * a different one. Both cases handled the same.
     */
    else
    {
        filesetTreeNodeT *pTopLostFound= fileset->dirTreeHead->nextSibling;

        /*
         * If node is a direct child of the lost+found tree, we have found 
         * its parent. Move it, and set its name if necessary.
         */
        if ( pFirstNode->parent == pTopLostFound )
        {
            move_node_in_tree( pFirstNode, pDirNode );

            if ( pFirstNode->name == NULL )
            {
                status = set_filename( pFirstNode, fileName, nameLen, fileset );
		if (SUCCESS != status) {
		    return status;
		}
            }
        }

        /*
         * Node is NOT a direct child of lost+found. This directory entry
         * represents a hard link which we have not seen before. Add the 
         * link in the current directory.
         */
        else
        {
            filesetTreeNodeT *pNewNode = NULL;

	    /* Cannot have directory hardlinks.  Drop on floor. */
	    if (fileset->tagArray[tagNum]->fileType == FT_DIRECTORY) {
		writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_126, 
			 "Tag %d is a directory and cannot be hardlinked.\n"),
			 tagNum);
		return SUCCESS;
	    }

            status = add_new_tree_node( fileset, tagNum, dirTagNum,
                                        fileName, nameLen, pDirNode, 
                                        &pNewNode );
	    if (SUCCESS != status) {
		return status;
	    }

            status = add_hard_link( fileset, tagNum, pNewNode );
	    if (SUCCESS != status) {
		return status;
	    }
        }
    }

    return SUCCESS;
}


/* 
 * Design Specification 3.6.8 set_filename
 *
 * Description:
 *  This function copies the filename string into the specified node
 *  in the file directory tree.
 *
 * Input parameters:
 *  node: Pointer to node in file directory tree where name will be inserted.
 *  fileName: Character string containing the filename.
 *  nameLen: Number of characters in the name string. 
 *  fileset: Pointer to current fileset.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY 
 */

static int
set_filename (filesetTreeNodeT *node, 
              char             *fileName, 
              int              nameLen,
              filesetLLT       *fileset)
{
    char *funcName = "set_filename";

    /*
     * If name already present, do some checks.
     */
    if ( node->name != NULL )
    {
        /*
         * If existing name and new name are the same, we are done.
         */
        if ( strlen(node->name) == nameLen &&
             memcmp( node->name, fileName, nameLen ) == 0 )
        {
            return SUCCESS;
        }

        /*
         * The new name is NOT equal the old one. The only way this is OK
         * is if the old name was generated from the tag number, i.e., 
         * previously "missing" name. Otherwise, this is corruption.
         * A "generated" name is based on tag number, and follows the 
         * pattern "tag_12345678". If generated, a bit is set in the tag's
         * status field.
         */
        else
        {
            filesetTagT *pTag = fileset->tagArray[node->tagNum];

            /*
             * Old name is different from new one and was NOT generated,
             * something is wrong.
             * Note: the name portion of the tag's status word applies
             * to all links for the tag, i.e., if "CREATED", this means
             * that *one of* the links for the tag has a created name. To
             * find it, we have to do the string pattern match.
             */
            if ( S_IS_REAL( pTag->status) )
            {
                writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_129, 
			 "Filename corruption in tag %d, fileset %s\n"),
                         node->tagNum, fileset->fsName );
                return FAILURE;
            }        
            
            /*
             * One of the links for this tag has a created/altered name. Is
             * this it? Check for the pattern. Imbedded "." is allowed.
             */
            else
            {
                int i;
                int isGenerated = 0;

                if ( strncmp(node->name, 
			     catgets(_m_catd, S_SALVAGE_1, SALVAGE_130, 
			     "tag_"), 4 ) == 0 && 
                     isdigit( node->name[strlen(node->name)-1] ) )
                {
                    isGenerated = 1;
		    i = 4;
                    while (node->name[i] != '\0' )
                    {
                        if ( ! isdigit(node->name[i]) && node->name[i] != '.' )
                        {
                            isGenerated = 0;
                            break;
                        }
                        i++;
                    }
                }

                /*
                 * If not generated, this is also a problem.
                 */
                if (isGenerated == 0)
                {
                    writemsg(SV_ERR, 
			     catgets(_m_catd, S_SALVAGE_1, SALVAGE_129, 
			     "Filename corruption in tag %d, fileset %s\n"),
                             node->tagNum, fileset->fsName );
                    return FAILURE;
                }

                /*
                 * OK, free the generated name.
                 */
                free( node->name );
            }
        }
    }        

    /*
     * Allocate for the new name, copy it in.
     */
    node->name = salvage_malloc( nameLen + 1 );
    CHECK_MALLOC( node->name, funcName, nameLen + 1 );
    
    strncpy( node->name, fileName, nameLen + 1 );
    S_SET_REAL(fileset->tagArray[node->tagNum]->status);

    return SUCCESS;
}
 


/* 
 * Design Specification 3.6.9 add_hard_link
 *
 * Description
 *  This function adds a hard link entry to the fileset's list of hard
 *  links for this tag.
 *
 * Input parameters: 
 *  fileset: Pointer to current fileset.
 *  tagNum: An integer representing the file's tag number.
 *  treeNode: Pointer to tree node which represents additional hard link.
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY.
 */

static int
add_hard_link (filesetLLT       *fileset,
               int              tagNum,
               filesetTreeNodeT *treeNode )
{
    char *funcName = "add_hard_link";
    filesetLinksT *pTagWithLink = NULL;
    tagLinksT *pTagLink = NULL;

    /*
     * First, start with the fileset's pointer to tags with hard links,
     * and look down the list for this tag.
     */
    pTagWithLink = fileset->hardLinks;
    
    while ( pTagWithLink != NULL )
    {
        if ( pTagWithLink->tag == tagNum )
            break;

        pTagWithLink = pTagWithLink->nextTag;
    }

    /*
     * If not found, create one.
     */
    if ( pTagWithLink == NULL )
    {
        pTagWithLink = salvage_malloc( sizeof(filesetLinksT) );
        CHECK_MALLOC( pTagWithLink, funcName, sizeof(filesetLinksT) );

        pTagWithLink->tag = tagNum;
        pTagWithLink->links = NULL;
        pTagWithLink->nextTag = fileset->hardLinks;
        fileset->hardLinks = pTagWithLink;
    }

    /*
     * Now, add the link for this tag.
     */
    pTagLink = salvage_malloc( sizeof(tagLinksT) );
    CHECK_MALLOC( pTagLink, funcName, sizeof(tagLinksT) );

    pTagLink->treeNode = treeNode;
    pTagLink->next = pTagWithLink->links;
    pTagWithLink->links = pTagLink;

    return SUCCESS;
}



/* 
 * Design Specification 3.6.10 check_node_in_path
 *
 * Description
 *  This function tests whether the current tree node is within a specific
 *  pathname.
 *
 * Input parameters: 
 *  pNode: Pointer to current node in file directory tree.
 *  testPath: String containing requested pathname.
 *
 * Output parameters:
 *  isInPath: Pointer to flag indicating whether node is in the specified
 *  path (1 = TRUE, 0 = FALSE).
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY.
 */

int
check_node_in_path (filesetLLT       *fileset,
                    filesetTreeNodeT *pNode, 
                    char             *testPath, 
                    int              *isInPath)
{
    char *funcName = "check_node_in_path";
    char *nodePath;
    int status = 0;

    status = get_pathname( &nodePath, pNode );
    if (SUCCESS != status) {
	return status;
    }

    status = trim_relative_pathname( fileset, nodePath );
    if (SUCCESS != status) {
	return status;
    }

    status = check_name_in_path( nodePath, testPath, isInPath );
    if (SUCCESS != status) {
	return status;
    }

    free( nodePath );
    return SUCCESS;
}


/* 
 * Design Specification 3.6.11 trim_relative_pathname
 *
 * Description
 *  This function trims the fileset name from the beginning of a pathname
 *  string returned from get_pathname(), to convert the string into a 
 *  pathname relative to the fileset mount point.
 *
 * Input parameters: 
 *  fileset: Pointer to the fileset struct, containing fileset name.
 *
 * Input/Output parameters:
 *  pathName: Address of character string to be modified.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY.
 */
static int trim_relative_pathname(filesetLLT *fileset,
				  char       *pathName)
{
    char *funcName = "trim_relative_pathname";
    char *tmpBuf = NULL;
    char *tmpPath = NULL;
    char *tmpStr = NULL;
    char *tmpFsName = NULL;
    char *fsName = fileset->dirTreeHead->name;
    char *lostFoundName = fileset->dirTreeHead->nextSibling->name;

    /*
     * Make copies of the string to be modified - we are doing things this
     * way to get around some "unaligned access" problems.
     */
    tmpBuf = salvage_malloc(strlen(pathName) + 1 );
    CHECK_MALLOC( tmpBuf, funcName, strlen(pathName) + 1 );
    strcpy( tmpBuf, pathName );
    tmpPath = tmpBuf;

    /*
     * Strip off leading "./", if any.
     */
    while ( tmpPath[0] == '.' && tmpPath[1] == '/' )
        tmpPath+=2;

    /*
     * Find the first ocurrence of "/", if any. Separate the first part
     * of the path from rest.
     */
    tmpFsName = tmpPath;
    tmpStr = strstr( tmpPath, "/" );

    if ( tmpStr == NULL )
    {
        tmpStr = strchr( tmpPath, '\0' );
    }
    else
    {
        tmpStr[0] = '\0';
        tmpStr++;
    }

    if ( strcmp( tmpFsName, fsName ) != 0 &&
         strcmp( tmpFsName, lostFoundName ) != 0 )
    {
        free( tmpBuf );
        return FAILURE;
    }

    strcpy( pathName, tmpStr );

    free( tmpBuf );
    return SUCCESS;
}



/*
 * Design Specification 3.6.12 check_name_in_path
 *
 * Description
 *  This function tests whether a filename is within a specific
 *  pathname.
 *
 * Input parameters:
 *  filePath: String containing name of file to be tested.
 *  testPath: String containing requested pathname.
 *
 * Output parameters:
 *  isInPath: Pointer to flag indicating whether node is in the specified
 *  path (1 = TRUE, 0 = FALSE).
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */
int check_name_in_path(char *filePath,
                       char *testPath,
                       int  *isInPath)
{
    char *funcName = "check_name_in_path";
    int index = 0;
    int status = 0;

    *isInPath = 1;

    /*
     * Strip off leading "./", if any, from the path strings.
     */
    while (filePath[0] == '.' && filePath[1] == '/' )
        filePath+=2;

    while (testPath[0] == '.' && testPath[1] == '/' )
        testPath+=2;

    /*
     * Check if the two path strings are identical, up to the end of
     * the *shortest* of the two. For example, this means that "foo" is
     * in "foo/bar", and "foo/bar" is in "foo".
     */
    while ( filePath[index] != '\0' && testPath[index] != '\0' )
    {
        if ( filePath[index] != testPath[index] )
        {
            *isInPath = 0;
            return SUCCESS;
        }

        index++;
    }

    /*
     * If the next character for *both* strings is a string terminator,
     * then both strings are the same length, and therefore identical.
     */
    if ( filePath[index] == '\0' && testPath[index] == '\0' )
    {
        return SUCCESS;
    }

    /*
     * One string is longer than the other. If the next character in the
     * longer string is *not* a "/", then the paths are not compatible,
     * e.g., "foo" is in "foo/bar", but not in "foobar".
     */
    else if ( ( filePath[index] == '\0' && testPath[index]  != '/' ) ||
              ( testPath[index]  == '\0' && filePath[index] != '/' ) )
    {
        *isInPath = 0;
    }

    return SUCCESS;
}



/* 
 * Design Specification 3.6.13 check_requested_files_found
 *
 * Description
 *
 *  This function examines the current directory which has just been
 *  searched, examines the requested pathname, and determines whether
 *  the requested pathname restoration has been satisfied, including a)
 *  whether the requested path has been visited, and b) whether any of
 *  the requested files were found.
 *
 *  In the case where the user requested a directory to be restored,
 *  and no files were found in that directory, this is considered a
 *  success. However, if the user requested a specific file (not a
 *  directory), and the parent directory for that file does NOT contain
 *  the requested file, this is considered a "not found" failure.
 *
 * Input parameters: 
 *  fileset: Pointer to current fileset structure.
 *  pCurrDirNode: Pointer to directory node which has just been processed.
 *  reqPath: String containing requested pathname.
 *
 * Output parameters:
 *  reqPathFound: Pointer to flag indicating whether request was completed 
 *  and satisfied.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY. 
 */
static int check_requested_file_found (filesetLLT       *fileset,
				       filesetTreeNodeT *pCurrDirNode, 
				       char             *reqPath, 
				       int              *reqPathFound)
{
    char *funcName = "check_requested_file_found";
    char *currPath = NULL;
    char *reqPathParent = NULL;
    char *reqPathLeaf = NULL;
    char *nullStr = "";
    int status = 0;

    /*
     * Get the pathname for the current directory, and trim off the leading
     * fileset name.
     */
    status = get_pathname( &currPath, pCurrDirNode );
    if (SUCCESS != status) {
	return status;
    }
    
    status = trim_relative_pathname( fileset, currPath );
    if (SUCCESS != status) {
	return status;
    }


    /*
     * If the requested pathname = the current directory path, then a 
     * directory name was requested - empty or not, this is success.
     */
    if ( strcmp( currPath, reqPath) == 0 )
    {
        *reqPathFound = REQUESTED_PATH_FOUND;
        free( currPath );
        return SUCCESS;
    }

    /*
     * Make a copy of the requested path - we will cut off the leaf name.
     */
    reqPathParent = salvage_malloc( strlen(reqPath) + 1 );
    CHECK_MALLOC( reqPathParent, funcName, strlen(reqPath) + 1 );
    strcpy( reqPathParent, reqPath );

    /*
     * Split the requested path into the leaf name and the path of its
     * parent. Look backwards, from the end, for a "/" character.
     */
    reqPathLeaf = &reqPathParent[strlen(reqPathParent) - 1];
    while ( reqPathLeaf[0] != '/' && strcmp(reqPathLeaf, reqPath) != 0 )
    {
        reqPathLeaf--;
    }

    /*
     * No "/" character found.
     */
    if ( strcmp( reqPathLeaf, reqPath) == 0 )
    {
        free( reqPathParent );
        reqPathParent = nullStr;        
    }
    else
    {
        /* 
	 * Cut it off, Move back up off the old "/" position
	 */
        reqPathLeaf[0] = '\0'; 
        reqPathLeaf++;             
    }

    /*
     * If the requested path parent = current directory path, then look thru
     * all child nodes of the current directory for the leaf filename. If it's
     * not there, we never will find it.
     */
    if ( strcmp( currPath, reqPathParent ) == 0 )
    {
        filesetTreeNodeT *pNode = pCurrDirNode->children;
    
        while ( pNode != NULL )
        {
            if ((pNode->name != NULL) &&
		(strcmp( pNode->name, reqPathLeaf ) == 0 ))
            {
                *reqPathFound = REQUESTED_PATH_FOUND;
                break;
            }

            pNode = pNode->nextSibling;
        }

        if ( pNode == NULL )
        {
            *reqPathFound = REQUESTED_PATH_NOT_FOUND;
        }
    }        

    free( currPath );
    free( reqPathParent );
    return SUCCESS;
}
