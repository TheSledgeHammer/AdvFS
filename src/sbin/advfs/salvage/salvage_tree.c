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
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: salvage_tree.c,v $ $Revision: 1.1.17.3 $ (DEC) $Date: 2002/03/22 21:04:36 $"

#include "salvage.h"

/*
 * Global Variables
 */
extern nl_catd _m_catd;

/*
 * Local static prototypes
 */
static int remove_node_from_tree (filesetTreeNodeT *nodeToRemove);

static int save_hard_links (filesetTreeNodeT *pNode,
                            filesetLLT       *fileset);

static int validate_node_name(filesetTreeNodeT *pNode,
			      filesetLLT       *fileset);

static int validate_node_links (filesetTagT *pTag, 
                                filesetLLT  *fileset);

static int generate_node_name ( filesetTreeNodeT* pNode );

static int check_tag_ignore_dir (filesetTreeNodeT *pCurr,
                                 filesetLLT       *fileset);

static filesetTreeNodeT *find_hardlink_in_tree (int         tagNum,
                                                filesetLLT  *fileset);

static int check_dup_node_names( filesetTreeNodeT *pDirNode,
                                 filesetLLT       *fileset );

static int replace_dup_node_name( filesetTreeNodeT *pNode, 
                                 filesetLLT        *fileset,
                                  int              *isSameTag );

static void simple_sort( void **array, 
                         int  nArray, 
                         void *firstFieldAddr, 
                         int  dataLen );

static void subsort( void **array, 
                     int  left, 
                     int  right, 
                     int  offset, 
                     int  dataLen );

static void swap ( void **array, 
                   int  i, 
                   int  j );

static int create_treenode_array( void             ***pArray, 
                                  filesetTreeNodeT *nodeList, 
                                  int              *nArray );

static void reorder_treenode_list( void             **pArray, 
                                   int              nArray, 
                                   filesetTreeNodeT **nodeList );

static int create_extent_array( void      ***pArray, 
                                extentLLT *nodeList, 
                                int       *nArray );

static void reorder_extent_list( void      **pArray, 
                                 int       nArray, 
                                 extentLLT **nodeList );


/*
 * Design Specification 3.5.1 add_new_tree_node
 *
 * Description:
 *  This function creates a new node in the file directory tree structure.
 *  The node is created as a child of the directory whose data is currently
 *  being processed, and a filename is inserted into the "name" field
 *  of the new node.
 *
 * Input parameters:
 *  fileset: Pointer to current fileset.
 *  tagNum: Tag number for the new node.
 *  parentTag: Parent tag number for the new node.
 *  name: Character pointer to filename string.
 *  len: Integer length of filename string.
 *  pDirNode: Pointer to tree node under which new node will be added.
 *
 * Output paramaters:
 *  pNewNode: Address of pointer for new node.
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY
 */

int add_new_tree_node (filesetLLT       *fileset,
                       int              tagNum, 
                       int              parentTag,
                       char             *name, 
                       int              len,
                       filesetTreeNodeT *pDirNode,
                       filesetTreeNodeT **pNewNode)
{
    char *funcName = "add_new_tree_node";
    int status = 0;
    filesetTagT *pTag = fileset->tagArray[tagNum];

    fileset->activeNodes++;

    /*
     * Create the initialized node struct.
     */
    status = create_unnamed_tree_node( pNewNode );
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Set some fields in the new node.
     */
    (*pNewNode)->tagNum = tagNum;
    (*pNewNode)->parentTagNum = parentTag;
 
    if ( IS_VALID_TAG_POINTER( pTag) )
    {
        (*pNewNode)->fileType = pTag->fileType;
    }

    /*
     * If name is provided, allocate memory for the name field, copy it in, 
     * and terminate the string.
     */
    if ( name != NULL )
    {
        (*pNewNode)->name = salvage_malloc( len + 1 );
        CHECK_MALLOC( (*pNewNode)->name, funcName, len + 1 );

        strncpy( (*pNewNode)->name, name, len );
        (*pNewNode)->name[len] = '\0';
    }

    /*
     * Insert the node in the tree, and increment the hard links counter.
     */
    insert_node_in_tree( *pNewNode, pDirNode );
    if ( IS_VALID_TAG_POINTER( pTag) )
    {
        (pTag->linksFound)++;
    }

    return SUCCESS;
}
 

/*
 * Design Specification  3.5.2 insert_node_in_tree
 *
 * Description:
 *  This function inserts a new node into the file directory tree structure.
 *  The new node is inserted as a child of the "parent" node parameter,
 *  at the head of the list of "child" nodes for that parent.
 *
 * Input parameters:
 *  newNode: Pointer to new filesetTreeNodeT data structure to be inserted 
 *           in tree.
 *  parentNode: Pointer to node, under which insertion will take place.
 *
 * Returns:
 *  Status value - SUCCESS.
 */

int
insert_node_in_tree (filesetTreeNodeT *newNode,
                     filesetTreeNodeT *parentNode)
{
    char *funcName = "insert_node_in_tree";

    newNode->parent = parentNode;
    newNode->nextSibling = parentNode->children;
    newNode->prevSibling = NULL;
    if (NULL != parentNode->children) {
	parentNode->children->prevSibling = newNode;
    }
    parentNode->children = newNode;

    return SUCCESS;
}


/*
 * Design Specification 3.5.3 delete_subtree
 *
 * Description:
 *  This function recursively deletes a node in the file directory
 *  free structure, including all child nodes of that node.
 *
 * Input parameters:
 *  topNodeToDelete: Address of pointer to tree node where recursive 
 *                   deletion begins. 
 *  fileset: A pointer to the current fileset.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

int
delete_subtree (filesetTreeNodeT **topNodeToDelete,
                filesetLLT       *fileset)
{
    char *funcName = "delete_subtree";
    int status = 0;
    filesetTreeNodeT *pCurr = NULL;
    filesetTreeNodeT *pNext = NULL;

    /*
     * Start at the first sibling in the top node's child list, loop thru 
     * each node in the list. If a node is a directory, call ourself
     * recursively, using the current directory as the new "top node".
     */
    pCurr = (*topNodeToDelete)->children;

    while ( pCurr != NULL )
    {
        pNext = pCurr->nextSibling;

        if ( pCurr->fileType == FT_DIRECTORY )
        {
            status = delete_subtree( &pCurr, fileset );
	    if (SUCCESS != status) {
		return status;
	    }
        }
        else
        {
            status = delete_node( &pCurr, fileset );
	    if (SUCCESS != status) {
		return status;
	    }
        }

        pCurr = pNext;
    }

    status = delete_node( topNodeToDelete, fileset );
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
}


/*
 * Design Specification 3.5.4 remove_node_from_tree
 *
 * Description:
 *  This function removes the specified node from the tree. This task
 *  is a bit less simple than it first appears, since the nodes in the
 *  tree do not have two-way pointers. If the node is the "first" child
 *  of its parent, the deletion is simple. If not, the function must
 *  start at the first child, and serially search the sibling list, so
 *  that the node which points to the "node to be deleted" can be
 *  updated. NB: This function only "disconnects" the node from the tree,
 *  leaving the node and its pointer intact - it is the responsibility of the
 *  calling function to move/delete/free the node as required.
 *
 * Input parameters:
 *  nodeToRemove: Address of pointer to tree node data structure to be deleted.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

static int
remove_node_from_tree (filesetTreeNodeT *nodeToRemove)
{
    char *funcName = "remove_node_from_tree";
    filesetTreeNodeT *pTemp = NULL;

    if ( nodeToRemove->parent == NULL )
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_220, "Parent of node to be removed does not exist.\n"));
        return FAILURE;
    }

    /*
     * Check the parent node of the node to be removed. If the parent's
     * "children" pointer points to the node to be removed, then this is 
     * head of a sibling list. Cut it off.
     */
    if ( nodeToRemove->parent->children == nodeToRemove )
    {
        nodeToRemove->parent->children = nodeToRemove->nextSibling;
	if (NULL != nodeToRemove->nextSibling) {
	    nodeToRemove->nextSibling->prevSibling = NULL;
	}
    }
    else if ( nodeToRemove->parent->children != NULL)
    {
	assert (NULL != nodeToRemove->prevSibling);
	nodeToRemove->prevSibling->nextSibling = nodeToRemove->nextSibling;
	if (NULL != nodeToRemove->nextSibling) {
	    nodeToRemove->nextSibling->prevSibling = nodeToRemove->prevSibling;
	}
    }
    else
    {
        /*
	 * Parent does not know of its children yet.
	 * So we can't remove it from parent's children list.
	 */
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_254,
		 "Parent does not know of its children nodes.\n"));
    }

    nodeToRemove->parent = NULL;
    nodeToRemove->nextSibling = NULL;
    nodeToRemove->prevSibling = NULL;

    return SUCCESS;
}


/*
 * Design Specification 3.5.5 delete_node
 *
 * Description:
 *  This function deletes the specified node from the tree, along with 
 *  its associated data in the tag pointer array.
 *
 * Input parameters:
 *  nodeToDelete: Address of pointer to tree node data structure to be deleted.
 *  fileset: A pointer to the current fileset.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

int
delete_node (filesetTreeNodeT **nodeToDelete,
             filesetLLT       *fileset)
{
    char *funcName = "delete_node";
    int tagNum = (*nodeToDelete)->tagNum;
    filesetTagT *pTag = fileset->tagArray[tagNum];
    int status = 0;

    status = remove_node_from_tree( *nodeToDelete );
    if (SUCCESS != status) {
	return status;
    }

    if (NULL != (*nodeToDelete)->name)
    {
	free( (*nodeToDelete)->name );
    }
    free( (*nodeToDelete) );
    *nodeToDelete = NULL;

    delete_tag_array_entry( fileset, tagNum );
    
    return SUCCESS;
}



/*
 * Design Specification 3.5.6 move_node_in_tree
 *
 * Description:
 *  This function moves a node in the file directory tree from one location
 * to another.
 *
 * Input parameters:
 *  pNodeToMove: Address of pointer to tree node data structure to be moved.
 *  pNewParent: Pointer to new location under which insertion will take place.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

int move_node_in_tree (filesetTreeNodeT *pNodeToMove,
                       filesetTreeNodeT *pNewParent)
{
    char *funcName = "move_node_in_tree";
    int status;

    status = remove_node_from_tree( pNodeToMove );
    if (SUCCESS != status) {
	return status;
    }
    status = insert_node_in_tree( pNodeToMove, pNewParent );
    if (SUCCESS != status) {
	return status;
    }
    
    return SUCCESS;
}


/*
 * Design Specification 3.5.7 walk_tree
 *
 * Description:
 *  This function recursively walks through the file directory tree structure.
 *  A pointer to an "action" function is passed as an argument to
 *  walk_tree, and this function is called to operate on each node. As
 *  walk_tree encounters each node and calls the action function, a pointer to
 *  the current node in the walk is passed to the action function, along with
 *  a pointer to an user-specified data argument.
 *
 * Note: 
 *  If the action function performs operations which could add
 *  nodes to the file directory tree, these new nodes should be added
 *  at the end of the sibling list. If added to areas of the tree which
 *  have already been visited by walk_tree, these nodes will not be
 *  encountered.
 *
 * Input parameters:
 *  pStartNode: Pointer to a starting-point node in the file tree structure.
 *  parentChildOrder: Indicates if a parent node should be acted upon before
 *                    or after its children.
 *  funcPtr: Pointer to an action function to be called for each node
 *           in the subtree.
 *  optArg: Pointer to optional data to be passed to action function as
 *          second argument.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. In this case, when walk_tree returns
 *  FAILURE to itself, this causes the recursive action of walk_tree to
 *  terminate, with all walk_tree frames to pop back up to the top.
 */

int walk_tree (filesetTreeNodeT *pStartNode,
               int              parentChildOrder,  
               int              (*funcPtr)(),
               void             *optArg)
{
    char *funcName = "walk_tree";
    int status = 0;
    filesetTreeNodeT *pCurr = NULL;
    filesetTreeNodeT *pNext = NULL;

    if ( parentChildOrder == PARENT_FIRST )
    {
        status = funcPtr( pStartNode, optArg );
	if (SUCCESS != status) {
	    return status;
	}
    }

    pCurr = pStartNode->children;
    while ( pCurr != NULL )
    {
        pNext = pCurr->nextSibling;                          /* Save it now */

        if ( pCurr->fileType == FT_DIRECTORY )
        {
            status = walk_tree( pCurr, parentChildOrder , funcPtr, optArg );
	    if (SUCCESS != status) {
		return status;
	    }
        }
        else
        {
            status = funcPtr( pCurr, optArg );
	    if (SUCCESS != status) {
		return status;
	    }
        }

        pCurr = pNext;
    }

    if ( parentChildOrder  == CHILD_FIRST )
    {
        status = funcPtr( pStartNode, optArg );
	if (SUCCESS != status) {
	    return status;
	}
    }
    return SUCCESS; 
}



/*
 * Design Specification 3.5.8 relink_lost_found 
 *
 * Description:
 *  This function attempts to move nodes from lost+found into the main
 *  tree.
 *
 * Input parameters:
 *  fileset: The fileset which we need to relink. 
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */

int relink_lost_found (filesetLLT *fileset)
{
    char *funcName = "relink_lost_found";
    filesetTreeNodeT *pCurr = NULL;
    filesetTreeNodeT *pNext = NULL;
    filesetTagT *pTag = NULL;
    int parentTagNum = 0;
    filesetTagT *pParentTag = NULL;
    filesetTreeNodeT *pTopLostFound;
    int status = 0;

    if ((NULL == fileset->dirTreeHead) ||
	(NULL == fileset->dirTreeHead->nextSibling))
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_221, "Fileset tree doesn't exist\n"));
	return FAILURE;
    }
    pTopLostFound = fileset->dirTreeHead->nextSibling;

    /*
     * Start at the first node under lost+found, loop thru each node which
     * is a *direct* child of lost+found.
     */
    pCurr = pTopLostFound->children;

    while ( pCurr != NULL )
    {
        pNext = pCurr->nextSibling;

        /*
         * If node's tag number or parent tag number is bogus, leave it.
         */
        if ( pCurr->tagNum > fileset->tagArraySize || 
             pCurr->parentTagNum > fileset->tagArraySize )
        {
            pCurr = pNext;
            continue;
        }

        /*
         * Get the pointer to the current node's tag entry, and its parent
         * tag number.
         */
        pTag = fileset->tagArray[pCurr->tagNum];
        parentTagNum = pCurr->parentTagNum;


        /*
         * If never found parent's tag, or parent tag entry is NULL or DEAD,
         * leave the node where it is.
         */
        if ( parentTagNum == MISSING_PARENT  || 
             fileset->tagArray[parentTagNum] == NULL  || 
             fileset->tagArray[parentTagNum] == DEAD )
        {
            pCurr = pNext;
            continue;
        }

        /*
         * Parent tag is marked ignore - we want to delete it. 
         */
        else if ( fileset->tagArray[parentTagNum] == IGNORE )
        {
            pParentTag = fileset->tagArray[parentTagNum];

            /*
             * Simple non-directory node - check, delete if links == 1
             * (if > 1 link, there could be another hard link somewhere, so 
             * leave it).
             */
            if ( pTag->fileType != FT_DIRECTORY )
            {
                if ( pTag->attrs != NULL && pTag->attrs->numLinks == 1 )
                {
                    status = delete_node( &pCurr, fileset );
		    if (SUCCESS != status) {
			return status;
		    }
                }
            }

            /*
             * Node is a directory. We want to delete the subtree, but
             * first we will walk down thru the subtree looking for
             * tags with more than one hard link - we might need them later. If 
             * found, we will save them off at the head of lost+found before 
             * we delete the rest.
             */
            else
            {
                int status = 0;    

                status = walk_tree( pCurr, PARENT_FIRST, save_hard_links, 
                                    fileset );
		if (SUCCESS != status) {
		    return status;
		}

                status = delete_subtree( &pCurr, fileset );
		if (SUCCESS != status) {
		    return status;
		}
            }
        }   /* end else if */

        /*
         * Parent tag entry exists. Get a pointer to the tag's firstInstance
         * node in the tree, and move the node under that new parent (if
         * parent is not a directory, something is wrong - leave it).
         */
        else
        {
            filesetTreeNodeT *pParentNode = NULL;

            pParentTag = fileset->tagArray[parentTagNum];
            pParentNode = pParentTag->firstInstance;

            if ( pParentTag->fileType == FT_DIRECTORY )
            {
                move_node_in_tree( pCurr, pParentNode );
            }
            else
            {
                writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_222, "Parent of tag %d is tag %d, which is not a directory\n"),
                         pCurr->tagNum, pParentNode->tagNum );
            }
        }   /* end else */

        pCurr = pNext;
    }   /* end while */

    return SUCCESS;
}


/*
 * Design Specification 3.5.9 save_hard_links 
 *
 * Description:
 *  This function is a walk_tree action function, called from
 *  relink_lost_found - the current node is checked for certain hard link
 *  conditions, and if the node's characteristics meet those conditions, the
 *  node is moved other locations in the tree.
 *
 * Input parameters:
 *  pNode: A pointer to the node in the tree. 
 *  fileset: A pointer to the current fileset.
 *
 * Output parameters:
 *  pathName: A pointer to a malloced path name for the passed in node. 
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

static int 
save_hard_links (filesetTreeNodeT *pNode,
                 filesetLLT       *fileset)
{
    char *funcName = "save_hard_links";
    filesetTagT *pTag = NULL;
    int status;

    /*
     * If the current node is a directory, do nothing - walk_tree will 
     * take us down.
     */
    if ( pNode->fileType == FT_DIRECTORY )
    {
        return SUCCESS;
    }

    /*
     * Not a directory - if more than 1 hard link, save the node off to the
     * beginning of the lost+found list. If attrs are missing, do the same,
     * since we don't know - there might be another link somewhere we'll
     * need.
     */
    pTag = fileset->tagArray[pNode->tagNum];
    
    if ( pTag->attrs == NULL || pTag->attrs->numLinks > 1 )
    {
        filesetTreeNodeT *lostAndFoundHead = fileset->dirTreeHead->nextSibling;

        status = move_node_in_tree( pNode, lostAndFoundHead );
	if (SUCCESS != status) {
	    return status;
	}
    }

    return SUCCESS;
}



/*
 * Design Specification 3.5.10 get_pathname 
 *
 * Description:
 *  Given a pointer to a node in the tree compute the full pathname.
 *  The returned pathName will be in the form: './usr/staff/foobar'.
 *
 *  NOTE: This function can be called BEFORE insert_filenames() and 
 *  validate_tree(), when trimming by path. 
 *
 * Input parameters:
 *  pNode: A pointer to the node in the tree. 
 *
 * Output parameters:
 *  pathName: A pointer to a malloced path name for the passed in node. 
 *
 * Returns:
 *  Status value - SUCCESS, FAILURE or NO_MEMORY.
 *
 * Side Effects:
 *  pathName will be malloced and loaded.
 */

int get_pathname(char             **pathName,
		 filesetTreeNodeT *pNode)
{
    char *funcName = "get_pathname";
    int index = 0;           
    int nameLen = 0;
    int status = 0;
    char tmpBuffer[MAXPATHLEN];
    filesetTreeNodeT *pCurr = pNode;
    char *pTmpBuffer;
    int pathLen;
    int checkFree = FALSE;

    pathLen = MAXPATHLEN;
    bzero( tmpBuffer, sizeof(tmpBuffer) );
    
    pTmpBuffer = tmpBuffer;

    if ( pNode->name == NULL )
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_223, "Name in tree node for tag %d is null\n"), 
		 pNode->tagNum );
        status = generate_node_name( pNode );
	if (SUCCESS != status) {
	    return status;
	}
    }

    /*
     * Copy name of current node into end of the buffer.
     */
    nameLen = strlen( pNode->name );

    if (MAXPATHLEN - nameLen - 1 < 2)
    {
        char *pTmp2Buffer;
	int  newPathLen;

	/*
	 * We need to tack on at minimum two more characters
	 * './' so if index is less than 2 we do not have 
	 * room to add them.
	 */

	newPathLen = pathLen * 2;

	pTmp2Buffer = (char *)salvage_malloc(newPathLen);
	CHECK_MALLOC( pTmp2Buffer, funcName, newPathLen);  

	checkFree  = TRUE;
	pTmpBuffer = pTmp2Buffer;
	pathLen    = newPathLen;
    }

    index = pathLen - nameLen - 1;
    strncpy( &pTmpBuffer[index], pNode->name, nameLen);
    index--;
    pTmpBuffer[index] = '/';

    /* 
     * Set current to tag's parent, loop upwards until top of tree, copying
     * node names into the buffer from right to left.
     */
    pCurr = pNode->parent;

    while ( pCurr != NULL )
    {    
        if ( pCurr->name == NULL )
        {
            writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_223, "Name in tree node for tag %d is null\n"), 
		     pNode->tagNum );

            status = generate_node_name( pNode );
	    if (SUCCESS != status) {
		return status;
	    }
        }

        nameLen = strlen( pCurr->name ); 

	if (index - nameLen < 2)
	{
	    char *pTmp2Buffer;
	    int  newPathLen;

	    /*
	     * We need to tack on at minimum two more characters
	     * './' so if index is less than 2 we do not have 
	     * room to add them.
	     */

	    newPathLen = pathLen * 2;
	    
	    pTmp2Buffer = (char *)salvage_malloc(newPathLen);
	    CHECK_MALLOC( pTmp2Buffer, funcName, newPathLen);  

	    strcpy(&pTmp2Buffer[pathLen + index], 
		   &pTmpBuffer[index]);
	    if (checkFree) {
	        free(pTmpBuffer);
	    }
	    else {
	        checkFree = TRUE;
	    }
	    pTmpBuffer = pTmp2Buffer;
	    index      += pathLen;
	    pathLen    = newPathLen;
	}

        index -= nameLen;
        strncpy( &pTmpBuffer[index], pCurr->name, nameLen );
        index--;

        pTmpBuffer[index] = '/';

        pCurr = pCurr->parent;
    }

    index--;
    pTmpBuffer[index] = '.';

    *pathName = salvage_malloc( strlen( &pTmpBuffer[index] ) + 1 );
    CHECK_MALLOC( *pathName, funcName, strlen( &pTmpBuffer[index] ) + 1 );  
  
    strcpy( *pathName, &pTmpBuffer[index] );

    if (checkFree) {
        free(pTmpBuffer);
    }

    return SUCCESS;
}



/*
 * Design Specification 3.5.11 validate_tree
 *
 * Description:
 *  This is the top-level function for a group of functions which
 *  check for various soft error conditions in the salvage data
 *  structures.
 *
 *  These checks are for the purpose of resolving ambiguous, missing,
 *  and/or conflicting data which result from problems with the state
 *  of the domain and filesets which salvage is attempting to
 *  recover. Decisions are made as to the best compromise regarding
 *  partial recovery, and the salvage data structures are repaired as
 *  necessary, prior to the actual file data retrieval.
 *
 * Input parameters:
 *  fileset: Pointer to fileset data structure.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */
int validate_tree (filesetLLT *fileset) 
{
    char *funcName = "validate_tree";
    int status = 0;
    int tagNum = 0;
    filesetTagT *pTag = NULL;
    filesetTreeNodeT *pTop = NULL;  

    /*
     * Check each valid tag for correct hard link count.
     */
    for ( tagNum = 3 ; tagNum < fileset->tagArraySize ; tagNum++ )
    {
        pTag = fileset->tagArray[tagNum];
        
        if ( IS_VALID_TAG_POINTER( pTag ) )
        {
            status = validate_node_links( pTag, fileset );
	    if (SUCCESS != status) {
		return status;
	    }
        }
    }

    /*
     * Walk the tree, and check for non-null node names. This check also
     * looks for duplicate node names within a directory.
     */
    pTop = fileset->dirTreeHead;
    status = walk_tree( pTop, PARENT_FIRST, validate_node_name, fileset );
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Walk the lost and found tree, doing the same.
     */
    pTop = fileset->dirTreeHead->nextSibling;
    status = walk_tree( pTop, PARENT_FIRST, validate_node_name, fileset );
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
}


/*
 * Design Specification 3.5.12 validate_node_links
 *
 * Description:
 *  Check for and resolve any possible conflicts between a tag's known
 *  number of hard links, and the number which were found.
 *
 * Input parameters:
 *  pTag: Pointer to a file's entry in the tag pointer array.
 *  fileset: Pointer to fileset data structure.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */

static int 
validate_node_links (filesetTagT  *pTag, 
                     filesetLLT   *fileset)
{
    char *funcName = "validate_node_links";
    int tagNum;
    filesetTreeNodeT *pNode = NULL;
    int status = 0;

    /*
     * If no attributes record, we can't check anything. Bail out.
     */
    if ( pTag->attrs == NULL )
    {
        return SUCCESS;
    }

    /*
     * Directories, by definition, cannot have hard links, but all directories
     * have their instance, as well as "." and "..", resulting in 
     * numLinks > 1. In essence, don't check them.
     */
    if ( pTag->fileType == FT_DIRECTORY )
    {
        return SUCCESS;
    }

    /*
     * Verify tag's first instance pointer is not null. If null, this is a
     * salvage data corruption. If OK, do the dereference.
     */
    if ( pTag->firstInstance != NULL )
    {
        tagNum = pTag->firstInstance->tagNum;
    }
    else
    {
        writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_224, "Missing first instance node for tag in fileset '%s'\n"),
		 fileset->fsName );
        return SUCCESS;
    }

    /*
     * Check for the "special case", in which the number of links found is
     * greater than the expected number of links *by exactly one*, AND the
     * name field in the "first instance" tree node is NULL - this occurs
     * when the "original" link was deleted, and none of the others have
     * been touched yet. In this case, our current node is the one which
     * was supposed to have been deleted, so we find another link to be
     * the tag's "first instance", and delete this one..
     */
    if ( ( pTag->linksFound == pTag->attrs->numLinks + 1 ) &&
         ( pTag->firstInstance->name == NULL ) )
    {
        pNode = find_hardlink_in_tree( tagNum, fileset );
        if ( pNode != NULL )
        {
            status = delete_node( &pTag->firstInstance, fileset );
	    if (SUCCESS != status) {
		return status;
	    }
            pTag->firstInstance = pNode;
        }
    }

    /*
     * Check for too many links.
     */
    else if ( pTag->linksFound > pTag->attrs->numLinks )
    {
        S_SET_TOO_MANY_LINKS( pTag->status );
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_225, "Corrupt data for tag %d - too many hard links found\n"), 
		 tagNum );
    }

    /*
     * Check for links missing. Only done if we are NOT recovering a specific
     * path, since otherwise, parts of the tree may have been pruned, 
     * removing some links which might otherwise have been found.
     */
    else if ( pTag->linksFound < pTag->attrs->numLinks &&
              Options.pathName == NULL )
    {
        S_SET_TOO_FEW_LINKS( pTag->status );
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_226, "Missing hard links for tag %d\n"), tagNum );
    }

    return SUCCESS;
}



/*
 * Design Specification 3.5.13 validate_node_name
 *
 * Description:
 *  This function performs checks on the node names and hard links for
 *  nodes in the file directory tree structure. 
 *  structures.
 *
 * Input parameters:
 *  pNode: Current node in the tree.
 *  fileset: Pointer to fileset data structure.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */

static int 
validate_node_name(filesetTreeNodeT *pNode,
                   filesetLLT       *fileset)
{
    char *funcName = "validate_node_name";
    int status = 0;
    filesetTagT *pTag = fileset->tagArray[pNode->tagNum];

    if (!(IS_VALID_TAG_POINTER( pTag)))
    {
        writemsg(SV_DEBUG, catgets(_m_catd, S_SALVAGE_1, SALVAGE_255, 
		 "Node exists, but tag array entry %d is invalid.  Deleting node.\n"), 
		 pNode->tagNum);

	status = remove_node_from_tree( pNode );
	if (SUCCESS != status) {
	    return status;
	}
    
	free( pNode->name );
	free( pNode );
	pNode = NULL;

	return SUCCESS;
    }

    /*
     * If node's name is NULL, generate the name based on the "tag_<tagnum>"
     * template, copy into the node's name field.
     */
    if ( pNode->name == NULL )
    {
        status = generate_node_name( pNode );
	if (SUCCESS != status) {
	    return status;
	}
        S_SET_CREATED( pTag->status );
        S_SET_HAS_GEN_NAMES( (fileset->tagArray[pNode->parent->tagNum])->status );
    }

    /*
     * If we are at the end of a sibling list, do the duplicate node name
     * check for all siblings at the current level.
     */
    if ( pNode->nextSibling == NULL && pNode->parent != NULL )
    {
        status = check_dup_node_names( pNode->parent, fileset );
	if (SUCCESS != status) {
	    return status;
	}
    }

    return SUCCESS;
}


/*
 * Design Specification 3.5.14 generate_node_name
 *
 * Description:
 *  This function generates a name field for a tree node. The name is
 *  based on the tag number.
 *
 * Input parameters:
 *  pNode: Pointer to a node in the file directory tree.
 *
 * Returns:
 *  Status value - SUCCESS or NO_MEMORY. 
 */

static int
generate_node_name ( filesetTreeNodeT* pNode )
{
    char *funcName = "generate_node_name";
    char tmpBuf[TAGNUM_BUFSIZ];

    /*
     * Generate the name based on the "tag_<tagnum>" template, copy into 
     * the node's name field.
     */
    sprintf( tmpBuf, catgets(_m_catd, S_SALVAGE_1, SALVAGE_227, "tag_%d"), pNode->tagNum );
    pNode->name = salvage_malloc( strlen(tmpBuf) + 1 );
    CHECK_MALLOC( pNode->name, funcName,strlen(tmpBuf) + 1 );
    strcpy( pNode->name, tmpBuf );

    return SUCCESS;
}


/*
 * Design Specification 3.5.15 trim_tree_ignore_dirs
 *
 * Description:
 *  In the case that salvage is performing a date-based file selection,
 *  this function is called to scan the file directory tree, looking for empty
 *  directories. When found, the empty directories are deleted.

 *
 * Input parameters:
 *  fileset: Pointer to fileset data structure.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE. 
 */

int trim_tree_ignore_dirs (filesetLLT *fileset)
{
    char *funcName = "trim_tree_ignore_dirs";
    int status = 0;
    filesetTreeNodeT *pTop = fileset->dirTreeHead;

    /*
     * Start at the top of the fileset's tree.
     */
    status = walk_tree( pTop, CHILD_FIRST, check_tag_ignore_dir, fileset );
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Do lost+found
     */
    pTop = pTop->nextSibling;
    status = walk_tree( pTop, CHILD_FIRST, check_tag_ignore_dir, fileset );
    if (SUCCESS != status) {
	return status;
    }

    return SUCCESS;
}


/*
 * Design Specification 3.5.16 check_tag_ignore_dir
 *
 * Description:
 *  This function is called as the walk_tree action function, as part of the
 *  trim_tree_ignore_dirs procedure. As the tree walk progresses, this
 *  function checks whether the tag represents an empty directory. If so, the
 *  directory is deleted.
 *
 * Input parameters:
 *  pNode: Pointer to the current node.
 *  fileset: Pointer to fileset data structure.
 *
 * Returns:
 *  Status value - SUCCESS or FAILURE.
 */

static int
check_tag_ignore_dir (filesetTreeNodeT *pNode,
                      filesetLLT       *fileset)
{
    char *funcName = "check_tag_ignore_dir";
    filesetTagT *pTag = fileset->tagArray[pNode->tagNum];
    int status = 0;
    
    /*
     * If this node is a directory with no child nodes, and its timestamp
     * is older than the specified recover date, then delete it from the
     * tree.
     */
    if ( pNode->tagNum != 2                && 
         pNode->tagNum != 0                && 
         pNode->fileType == FT_DIRECTORY   &&
         pNode->children == NULL           &&
         pTag->attrs     != NULL           &&
         Options.recoverDate.tv_sec > pTag->attrs->mtime.tv_sec )
    {
        status = delete_node( &pNode, fileset );
	if (SUCCESS != status) {
	    return status;
	}
    }

    return SUCCESS;
}


/*
 * Design Specification 3.5.17 find_hardlink_in_tree
 *
 * Description:
 *  This function locates a node, with a specific tag number, in the fileset's
 *  list of tags with additional hard links. The located node represents a
 *  hardlink which is NOT the tag's "first instance" node.
 *
 * Input parameters:
 *  tagNum: tag number to search for in tree. 
 *  fileset: Pointer to fileset data structure.
 *
 * Returns:
 *  Pointer to tree node, or NULL if not found. 
 */

static filesetTreeNodeT *
find_hardlink_in_tree (int         tagNum,
                       filesetLLT  *fileset)
{
    char *funcName = "find_hardlink_in_tree";
    filesetLinksT *pCurr = fileset->hardLinks;
    tagLinksT *pLink = NULL;

    /*
     * Loop thru the fileset's list of tags with hard links, until we  find
     * our tag number.
     */
    while ( pCurr != NULL && pCurr->tag != tagNum )
    {
        pCurr = pCurr->nextTag;
    }

    if ( pCurr == NULL )
    {
        return NULL;
    }

    /*
     * If we found the tag in the list, go to its list of hard links, and
     * return the first one.
     */
    pLink = pCurr->links;

    if ( pLink != NULL)
    {
        return pLink->treeNode;
    }
    else
    {
        return NULL;
    }
}



/*
 * Design Specification 3.5.18 check_dup_node_names
 *
 * Description:
 *  This function checks the child nodes of a directory node, and resolves
 *  duplicate names.
 *
 * Input parameters:
 *  pDirNode: Pointer to directory node.
 *
 * Returns:
 *  SUCCESS of FAILURE.
 */

static int
check_dup_node_names(filesetTreeNodeT *pDirNode, 
		     filesetLLT       *fileset )
{
    char *funcName = "check_dup_node_names";
    filesetTreeNodeT *pNext;
    filesetTreeNodeT *pCurr;
    filesetTreeNodeT *pPrev;
    int status = 0;

    if ( pDirNode->children == NULL ) {
        return SUCCESS;
    }

    /*
     * First, sort the linked list of child nodes, by name, for this
     * directory.
     */
    status = sort_linked_list( (void *)&pDirNode->children,
                               LISTSORT_NODE_NAME, COUNT_UNKNOWN );
    if (SUCCESS != status) {
	return status;
    }

    /*
     * Start at the (new) head of the sorted list of child nodes, and look
     * for duplicates (since sorted, they will be adjacent). If we find one,
     * keep looking for more (there may be more than 2), and append the
     * tag number to each dup. If tag numbers are also the same (corruption
     * causing dup names may well also cause dup tags), start a "same tag"
     * counter to increment and append.
     */
    pCurr = pDirNode->children;
    pPrev = pDirNode;

    while ( pCurr->nextSibling != NULL )
    {
        int isSameTag = 0;

        pNext = pCurr->nextSibling;
        if ( strcmp( pCurr->name, pNext->name) == 0 )
        {
            filesetTreeNodeT *pSave;
            int nDups = 1;
            int icount = 0;
            int isFirst = pCurr == pDirNode->children ? 1 : 0;

            /*
             * We must see if any of the nodes with the same name also have
             * the same tag number. To do this we sort this portion of the
             * list on the nodes' tag number. First we count them.
             */
            pSave = pCurr;
            while ( pNext != NULL && strcmp( pCurr->name, pNext->name) == 0 )
            {
                nDups++;
                pCurr = pNext;
                pNext = pCurr->nextSibling;
            }
            pCurr = pSave;
            pNext = pCurr->nextSibling;

            /*
             * Now sort the portion of the list with the duplicate names,
	     * by tag number, using the result of the count operation. If 
             * only two dups, don't bother, since they are adjacent anyway.
             */
            if ( nDups > 2 )
            {
                status = sort_linked_list( (void *)&pCurr, LISTSORT_NODE_TAGNUM,
                                           nDups );
		if (SUCCESS != status) {
		    return status;
		}

                if ( isFirst == 1 )
                    pDirNode->children = pCurr;
                else
                    pPrev->nextSibling = pCurr;
            }

            /*
             * Now check for adjacent nodes with identical tag numbers.
             * If any are duplicates, we will append the dup counter to
             * the altered name.
             */
            pSave = pCurr;
            pNext = pCurr->nextSibling;

            for ( icount = 0 ; icount < nDups && pNext != NULL ; icount++ )
            {
                if ( pCurr->tagNum == pNext->tagNum )
                {
                    isSameTag = 1;
                    break;
                }
                pCurr = pNext;
                pNext = pCurr->nextSibling;
            }

            pCurr = pSave;
            pNext = pCurr->nextSibling;

            for ( icount = 0 ; icount < nDups && pNext != NULL ; icount++ )
            {
                status = replace_dup_node_name( pCurr, fileset, &isSameTag );
		if (SUCCESS != status) {
		    return status;
		}
                pCurr = pNext;
                pNext= pCurr->nextSibling;
            }
            status = replace_dup_node_name( pCurr, fileset, &isSameTag );
	    if (SUCCESS != status) {
		return status;
	    }
            /*
             * Check for NULL pNext here and break to prevent segment fault
             * when updating pCurr at end of while loop.
             */
            if (NULL == pNext)
            {
                break;
            }
        }

        pPrev = pCurr;
        pCurr = pCurr->nextSibling;
    }

    return SUCCESS;
}


/*
 * Design Specification 3.5.19 replace_dup_node_name
 *
 * Description:
 *  This static function frees the old node name and alters the name to 
 *  resolve duplicate names.
 *
 * Input parameters:
 *  pDirNode: Pointer to directory node.
 *  fileset: Pointer to current fileset structure.
 *
 * Input/output parameters:
 *  isSameTag: flag/counter for nodes with both the same name and tag number.
 *
 * Returns:
 *  SUCCESS, FAILURE or NO_MEMORY.
 */

static int
replace_dup_node_name( filesetTreeNodeT *pNode, 
                       filesetLLT       *fileset,
                       int              *isSameTag )
{
    char *funcName = "replace_dup_node_name";
    char namBuf[NAME_MAX];
    filesetTagT *pTag = fileset->tagArray[pNode->tagNum];        

    if ( strlen(pNode->name) > NAME_MAX - TAGNUM_BUFSIZ )    /* Too long ? */
        pNode->name[NAME_MAX - TAGNUM_BUFSIZ] = '\0'; /* Leave room for tag# */

    sprintf( namBuf, "%s.%d", pNode->name, pNode->tagNum );

    if ( *isSameTag > 0 )
    {
        char tmpBuf[TAGNUM_BUFSIZ];

        sprintf( tmpBuf, ".%d", *isSameTag );
        strcat( namBuf, tmpBuf );
        (*isSameTag)++;
    }

    free( pNode->name );
    pNode->name = salvage_malloc( strlen(namBuf) + 1 );
    CHECK_MALLOC( pNode->name, funcName, strlen(namBuf) + 1 );

    strcpy( pNode->name, namBuf );
    S_SET_ALTERED( pTag->status );
    S_SET_HAS_ALT_NAMES( (fileset->tagArray[pNode->parent->tagNum])->status );

    return SUCCESS;
}



/*
 * Design Specification 3.5.20 sort_linked_list
 *
 * Description:
 *  This function sorts a linked list, of a specified type, according to
 *  predefined criteria for that type.
 *
 * Input parameters:
 *  listHead: Address of pointer to linked list head.
 *  type: Constant which defines datatype of linked list:
 *        LISTSORT_NODE_NAME - tree node, sort by name.
 *        LISTSORT_NODE_TAGNUM - tree node, sort by tag number.
 *        LISTSORT_EXTENT_OFFSET - extent, sort by offset (byteInFile).
 *
 * Input/output parameters:
 *  nRecs: number of elements in list/array - if == 0, count them.
 *
 * Returns:
 *  SUCCESS, FAILURE or NO_MEMORY.
 *
 * Side Effect:
 *  listHead: The list pointed at will be sorted.
 */
int sort_linked_list(void **listHead, 
		     int  type, 
		     int  nRecs)
{
    char *funcName = "sort_linked_list";
    void **pArray;
    int status = 0;

    if ( *listHead == NULL ) {
        return SUCCESS;
    }

    switch ( type )
    {
        /*
         * Procedure for sorting list of tree nodes, based on node name.
         */
        case LISTSORT_NODE_NAME:
        {
            filesetTreeNodeT *pNode = (filesetTreeNodeT *)*listHead;
            int dataLen = 0;                    /* 0 means null-term. string */

            status = create_treenode_array( &pArray,
                                            (filesetTreeNodeT *)*listHead,
                                            &nRecs );
	    if (SUCCESS != status) {
		return status;
	    }

            simple_sort( pArray, nRecs, &pNode->name, dataLen );

            reorder_treenode_list( pArray, nRecs, 
                                   (filesetTreeNodeT **)listHead );
            break;
        }

        /*
         * Procedure for sorting list of tree nodes, based on node's tag number.
         */
            case LISTSORT_NODE_TAGNUM:
            {
                filesetTreeNodeT *pNode = (filesetTreeNodeT *)*listHead;
                int dataLen = sizeof( pNode->tagNum );

                status = create_treenode_array( &pArray,
                                                (filesetTreeNodeT *)*listHead,
                                                &nRecs);
		if (SUCCESS != status) {
		    return status;
		}

                simple_sort( pArray, nRecs, &pNode->tagNum, dataLen );

                reorder_treenode_list( pArray, nRecs,
                                       (filesetTreeNodeT **)listHead );
                break;
            }

        /*
         * Procedure for sorting list of tag extents, based on extent byte 
         * offset.
         */
            case LISTSORT_EXTENT_OFFSET:
            {
                extentLLT *pNode = (extentLLT *)*listHead;
                int dataLen = sizeof( pNode->byteInFile );

                status = create_extent_array( &pArray, (extentLLT *)*listHead, 
                                              &nRecs );
		if (SUCCESS != status) {
		    return status;
		}

                simple_sort( pArray, nRecs, &pNode->byteInFile, dataLen );

                reorder_extent_list( pArray, nRecs, (extentLLT **)listHead );
                break;
            }

        /*
         * Unknown sort type - salvage data corruption.
         */
            default:
                return FAILURE;
                break;
    }

    free( pArray );
    return SUCCESS;
}


/*
 * Design Specification 3.5.21 simple_sort
 *
 * Description:
 *  Static function: Entry point for the actual sort operation.
 *
 * Input parameters:
 *  array: array of pointers to be sorted.
 *  nArray: number of elements in the array.
 *  firstFieldAddr: address of sort field in 1st element in array.
 *  dataLen: length, in bytes, of sort field - 0 means null-term. string.
 */

static void
simple_sort(void **array, 
	    int  nArray, 
	    void *firstFieldAddr, 
	    int  dataLen )
{
    void subsort();
    void swap();
    int startSubscript = 0;
    int endSubscript = nArray - 1;
    int fieldOffset = (long)firstFieldAddr - (long)array[0];

    subsort( array, startSubscript, endSubscript, fieldOffset, dataLen  );
}


/*
 * Design Specification 3.5.22 subsort
 *
 * Description:
 *  Static function: Recursive work unit in the sort procedure.
 *
 * Input parameters:
 *  array: array of pointers to be sorted.
 *  left: beginning subscript in array to be sorted.
 *  right: ending subscript in array to be sorted.
 *  offset: offset, in bytes, of sort field within record.
 *  dataLen: length, in bytes, of sort field - 0 means null-term. string.
 */

static void 
subsort (void **array, 
	 int  left, 
	 int  right, 
	 int  offset, 
	 int  dataLen )
{
    char *funcName = "subsort";
    int i = 0;
    int last = 0;

    if ( left >= right ) {
        return;
    }

    swap ( array, left, (left+right)/2 );
    last = left;

    for ( i = left+1 ; i <= right ; i++ )
    {
        if ( dataLen == 0 )  
        {
	    /* 
	     * String compare 
	     */
            char *pCurr;
            char *pLeft;

            memcpy( &pCurr, (char *)((long)(array[i])+offset), 
                    sizeof(char *) );
            memcpy( &pLeft, (char *)((long)(array[left])+offset),
                    sizeof(char *) );

            if ( strcmp(pCurr, pLeft) < 0 )
                swap ( array, ++last, i );
        }
        else if ( dataLen == 8 )
        {
	    /* 
	     * binary compare, long 
	     */
            long *pCurr;
            long *pLeft;

            pCurr = (long *)((long)(array[i])+offset);
            pLeft = (long *)((long)(array[left])+offset);

            if ( *pCurr < *pLeft )
                swap ( array, ++last, i );
        }
        else if ( dataLen == 4 )
        {
	    /* 
	     * binary compare, int 
	     */
            int *pCurr;
            int *pLeft;

            pCurr = (int *)((long)(array[i])+offset);
            pLeft = (int *)((long)(array[left])+offset);

            if ( *pCurr < *pLeft )
                swap ( array, ++last, i );
        }
    }

    swap ( array, left, last );
    subsort ( array, left, last - 1, offset, dataLen );
    subsort ( array, last+1, right, offset, dataLen );
}


/*
 * Design Specification 3.5.23 swap
 *
 * Description:
 *  Static function: swap two elements in the array.
 *
 * Input parameters:
 *  array: array of pointers to be sorted.
 *  i: first element to be swapped.
 *  j: second element to be swapped.
 */

static void 
swap (void **array, 
      int  i, 
      int  j)
{
    void *temp;

    temp = array[i];
    array[i] = array[j];
    array[j] = temp;
}



/*
 * Design Specification 3.5.24 create_treenode_array
 *
 * Description:
 *  This static function creates an array of pointers, with each element
 *  pointing to a node in the linked list. Specific for list of sibling
 *  nodes in the fileset tree.
 *
 * Input parameters:
 *  pArray: Pointer to array of pointers to be created.
 *  nodeList: Pointer to linked list.
 *
 * Output parameters:
 *  nArray: number of elements in list/array.
 *
 * Returns:
 *  SUCCESS, FAILURE or NO_MEMORY.
 */

static int
create_treenode_array(void             ***pArray, 
		      filesetTreeNodeT *nodeList, 
		      int              *nArray )
{
    char *funcName = "create_treenode_array";
    int i = 0;
    filesetTreeNodeT *pNode = NULL;

    /*
     * If the number of elements in the array was passed in as 0, we need to 
     * first count the nodes in the list. This number will be returned to 
     * the caller (if != 0, the caller already counted them).
     */
    if ( *nArray == COUNT_UNKNOWN )
    {
        pNode = nodeList;
        while ( pNode != NULL )
        {
            (*nArray)++;
            pNode = pNode->nextSibling;
        }    
    }

    /*
     * Allocate the array of pointers, using the number of elements plus 1.
     * The extra pointer will save the "next" ptr for the last node in the
     * list, which would normally be null, but not necessarily (if we are
     * sorting *part* of a list). Then loop thru each node in the list,
     * and set each pointer in the array to each sequential node address,
     * with the extra pointer saving the terminator.
     */
    *pArray = salvage_calloc( (*nArray) + 1, sizeof(void *) );
    CHECK_MALLOC( *pArray, funcName, *nArray * sizeof(void *) );

    pNode = nodeList;
    for ( i = 0 ; i < (*nArray) ; i++ )
    {
        if ( pNode == NULL )
        {
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_154, 
			 	     "pNode does not exist.\n"));
            return FAILURE;
        }

        (*pArray)[i] = pNode;
        pNode = pNode->nextSibling;
    }
    (*pArray)[*nArray] = pNode;

    return SUCCESS;
}


/*
 * Design Specification 3.5.25 reorder_treenode_list
 *
 * Description:
 *  This static function re-orders a linked list, according to the sorted
 *  order of an array of pointers to the nodes in the list. Specific for
 *  list of sibling nodes in the fileset tree.
 *
 * Input parameters:
 *  pArray: Sorted array of pointers to the nodes in the linked list.
 *  nArray: Number of elements in the array/list.
 *  nodeList: Pointer to linked list.
 */

static void
reorder_treenode_list(void             **pArray, 
		      int              nArray, 
		      filesetTreeNodeT **nodeList)
{
    int i = 0;
    filesetTreeNodeT *pTmp = NULL;
    
    /*
     * Start at the beginning of the array, and reorder the list according
     * the array order. The last node gets the saved terminating pointer
     * from the "extra" array element.
     */
    pTmp = pArray[0];
    pTmp->prevSibling = NULL;
    pTmp->nextSibling = pArray[1];
    for ( i = 1 ; i < nArray ; i++ )
    {
        pTmp = pArray[i];
	pTmp->prevSibling = pArray[i-1];
        pTmp->nextSibling = pArray[i+1];
    }

    *nodeList = pArray[0];
}


/*
 * Design Specification 3.5.26 create_extent_array
 *
 * Description:
 *  This static function creates an array of pointers, with each element
 *  pointing to a node in the linked list. Specific for list of extents
 *  for a tag in the fileset tag array.
 *
 * Input parameters:
 *  pArray: Pointer to array of pointers to be created.
 *  nodeList: Pointer to linked list.
 *
 * Output parameters:
 *  nArray: number of elements in list/array.
 *
 * Returns:
 *  SUCCESS, FAILURE or NO_MEMORY
 */

static int
create_extent_array(void      ***pArray, 
		    extentLLT *nodeList, 
		    int       *nArray)
{
    char *funcName = "create_extent_array";
    int i = 0;
    extentLLT *pNode = nodeList;

    /*
     * If the number of elements in the array was passed in as 0, we need to 
     * first count the nodes in the list. This number will be returned to 
     * the caller (if != 0, the caller already counted them).
     */
    if ( *nArray == COUNT_UNKNOWN )
    {
        pNode = nodeList;
        while ( pNode != NULL )
        {
            (*nArray)++;
            pNode = pNode->next;
        }    
    }

    /*
     * Allocate the array of pointers, using the number of elements plus 1.
     * The extra pointer will save the "next" ptr for the last node in the
     * list, which would normally be null, but not necessarily (if we are
     * sorting *part* of a list). Then loop thru each node in the list,
     * and set each pointer in the array to each sequential node address,
     * with the extra pointer saving the terminator.
     */
    *pArray = salvage_calloc( (*nArray) + 1, sizeof(void *) );
    CHECK_MALLOC( *pArray, funcName, *nArray * sizeof(void *) );

    pNode = nodeList;
    for ( i = 0 ; i < (*nArray) ; i++ )
    {
        if ( pNode == NULL )
        {
	    writemsg(SV_ERR, catgets(_m_catd, S_SALVAGE_1, SALVAGE_154, 
				     "pNode does not exist.\n"));
            return FAILURE;
        }

        (*pArray)[i] = pNode;
        pNode = pNode->next;
    }
    (*pArray)[*nArray] = pNode;

    return SUCCESS;
}


/*
 * Design Specification 3.5.27 reorder_extent_list
 *
 * Description:
 *  This static function re-orders a linked list, according to the sorted
 *  order of an array of pointers to the nodes in the list. Specific for 
 *  list of extents for a tag in the fileset tag array.
 *
 * Input parameters:
 *  pArray: Sorted array of pointers to the nodes in the linked list.
 *  nArray: Number of elements in the array/list.
 *  nodeList: Pointer to linked list.
 */

static void
reorder_extent_list(void      **pArray, 
		    int       nArray, 
		    extentLLT **nodeList)
{
    int i = 0;
    extentLLT *pTmp = NULL;
    
    /*
     * Start at the beginning of the array, and reorder the list according
     * the array order. The last node gets the saved terminating pointer
     * from the "extra" array element.
     */
    pTmp = pArray[0];
    for ( i = 1 ; i < nArray ; i++ )
    {
        pTmp->next = pArray[i];
        pTmp = pArray[i];
    }

    pTmp->next = pArray[nArray];
    *nodeList = pArray[0];
}


