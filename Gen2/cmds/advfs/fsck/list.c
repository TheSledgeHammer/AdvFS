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
 */


#include "fsck.h"

/*
 * Global
 */
extern nl_catd _m_catd;



/*
 * Function Name: compare_tags (3.78)
 *
 * Description: This function compares two tags' tagNum and
 *              tagSeq fields to determine whether they're equal or
 *              which is greater than the other.
 *
 *		Note that this is only referred to by a pointer to this
 *		function.  It is never called directly.
 *
 * Input parameters:
 *     a: A pointer to a tag.
 *     b: A pointer to a tag.
 *
 * Output parameters: N/A
 *
 * Returns: -1 if a is less than b.
 *           0 if a is equal to b.
 *          +1 if a is greater than b.
 * 
 */
int 
compare_tags(void *a, 
	     void *b)
{
    char  *funcName = "compare_tags";

    assert(NULL != (tagNodeT *)a);
    assert(NULL != (tagNodeT *)b);

    if (((tagNodeT *)a)->tagSeq.tag_num < ((tagNodeT *)b)->tagSeq.tag_num) {
	return -1;
    }
    if (((tagNodeT *)a)->tagSeq.tag_num > ((tagNodeT *)b)->tagSeq.tag_num) {
	return +1;
    }

    return 0;
} /* end compare_tags */



/* 
 * Function Name: compare_mcell_nodes (3.79)
 *
 * Description: This function compares two mcells' volume,
 *              page, and cell to determine whether they're
 *              equal or which is greater than the other.  
 *
 *		Note that this is only referred to by a pointer to this
 *		function.  It is never called directly.
 *
 * Input parameters:
 *     a: A pointer to an mcell.
 *     b: A pointer to an mcell.
 *
 * Output parameters: N/A
 *
 * Returns: -1 if a is less than b.
 *           0 if a is equal to b.
 *          +1 if a is greater than b.
 */

int 
compare_mcell_nodes(void *a, 
		    void *b)
{
    char  *funcName = "compare_mcell_nodes";

    assert(NULL != (mcellNodeT *)a);
    assert(NULL != (mcellNodeT *)b);

    /* 
     * Note that it compares the page number, then the mcell number,
     * then the volume number for performance reasons.  Skiplists 
     * perform a lot of comparisons, and the sooner a difference can
     * be found the better.
     */

    if (((mcellNodeT *)a)->mcid.page < ((mcellNodeT *)b)->mcid.page) {
	return -1;
    }
    if (((mcellNodeT *)a)->mcid.page > ((mcellNodeT *)b)->mcid.page) {
	return +1;
    }
    if (((mcellNodeT *)a)->mcid.cell < ((mcellNodeT *)b)->mcid.cell) {
	return -1;
    }
    if (((mcellNodeT *)a)->mcid.cell > ((mcellNodeT *)b)->mcid.cell) {
	return +1;
    }
    if (((mcellNodeT *)a)->mcid.volume  < ((mcellNodeT *)b)->mcid.volume) {
	return -1;
    }
    if (((mcellNodeT *)a)->mcid.volume  > ((mcellNodeT *)b)->mcid.volume) {
	return +1;
    }

    return 0;
} /* end compare_mcell_nodes */



/*
 * Function Name: create_list (3.80)
 *
 * Description: Malloc's and initializes a nodeless list structure.
 *
 * Input parameters:
 *     compare: A pointer to a function for comparing keys.
 *     max: A pointer to a data structure having the maximum key allowed.
 *	This *must* reside in memory that will exist for the duration of
 *	the usage of the list.  Malloc'ing that memory is highly recommended.
 *
 * Output parameters: 
 *     list: A pointer to a pointer to the newly malloc'ed list.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 */
int 
create_list(int (*compare)(void *a, void *b),
	    listT **list,
	    void *max)
{
    char  *funcName = "create_list";
    int i;
    int status;
    listT *pList;

    pList = (listT *)ffd_malloc(sizeof(listT));
    if (pList == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"), 
		 catgets(_m_catd, S_FSCK_1, FSCK_310, 
			 "list"));
	return NO_MEMORY;
    }

    pList->header = NEW_NODE_OF_LEVEL(MAXLEVELS);
    if (pList->header == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_312, 
			 "list header"));
	return NO_MEMORY;
    }

    pList->tail = NEW_NODE_OF_LEVEL(0);
    if (pList->tail == NULL) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_313,
			 "list tail"));
	return NO_MEMORY;
    }

    for (i = 0 ; i < MAXLEVELS ; i++) {
	pList->header->forward[i] = pList->tail;
    }
    pList->level = 0;
    pList->tail->data = max;
    pList->size = 0;

    pList->randomBits = random();
    pList->randomsLeft = BITS_IN_RANDOM/2;

    pList->compare = compare;

#ifdef PTHREADS
    status = pthread_mutex_init(&pList->lock, NULL);

    if (ENOMEM == status) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_187,
			 "mutex init"));
	return NO_MEMORY;
    } else {
	assert(0 == status);
    }
#endif /* PTHREADS */

    *list = pList;
    return SUCCESS;
} /* end create_list */


/*
 * Function Name: random_level
 *
 * Description: Generate a random level for a node to be inserted.
 *  This level has the following probabilities:
 *	level 1: .75
 *	level 2: .75 * .25
 *	level 3: .75 * .25^2
 *	level 4: .75 * .25^3
 *	level 5: .75 * .25^4
 *	level 6: .75 * .25^5
 *	level 7: .75 * .25^6
 *	level 8: .75 * .25^7
 *	level 9: .75 * .25^8
 *	etc.
 *
 * Input parameters:
 *     list: A pointer to the list containing the random numbers.
 *
 * Output parameters: N/A
 *
 * Returns: The level to put the new node at.
 */
int 
random_level(listT *list)
{
    int level;
    int b;

    level = 0;

    do {
	b = list->randomBits & 3;	/* Random 2-bit number (0-3) */
	if (0 == b) {
	    level++;
	    if (MAXLEVEL == level) {  /* Ensures this isn't an infinite loop */
		return MAXLEVEL;
	    }
	}
	list->randomBits >>= 2;	/* Right shift randomBits by 2 */
	list->randomsLeft--;	/* Decrement number of random numbers left */
	if (0 == list->randomsLeft) {
	    /* Ran out of random bits.  Get new ones. */
	    list->randomBits = random();
	    list->randomsLeft = BITS_IN_RANDOM / 2;
	}
    } while (0 == b);

    return level;
}


/*
 * Function Name: insert_node (3.81)
 *
 * Description: Malloc a new node structure, and insert that node in
 *              the list. This is an O(log(n)) operation.
 *
 *		It does not allow entries with duplicate keys.
 *
 * Input parameters:
 *     list: A pointer to the list to insert a node in.
 *     data: A pointer to the new node's data.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, NO_MEMORY, or NODE_EXISTS.
 */
int 
insert_node(listT *list, 
	    void *data)
{
    char  *funcName = "insert_node";
    nodeT *node; /* Pointer to node to insert. */
    nodeT *temp; /* Temporary node pointer. */
    int   level; /* Current level searching on. */
    nodeT *update[MAXLEVELS]; /* Array of node pointers  */
    int   status;

#ifdef PTHREADS
    status = pthread_rwlock_wrlock(&list->lock);
    assert(0 == status);
#endif /* PTHREADS */

    temp = list->header;
    level = list->level;
    do {
	while (-1 == list->compare(temp->forward[level]->data, data)) {
	    temp = temp->forward[level];
	}
	update[level] = temp;
	level--;
    } while (level >= 0);
    
    if (0 == list->compare(temp->forward[0]->data, data)) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_314,
			 "Tried to insert a node already in list.\n"));
#ifdef PTHREADS
	status = pthread_rwlock_unlock(&list->lock);
	assert(0 == status);
#endif /* PTHREADS */

	return NODE_EXISTS;
    }

    level = random_level(list);
    if (level > list->level) {	/* Cap level at current list->level + 1 */
	(list->level)++;
	level = list->level;
	update[level] = list->header;
    }

    node = NEW_NODE_OF_LEVEL(level);
    if (NULL == node) {
#ifdef PTHREADS
	status = pthread_rwlock_unlock(&list->lock);
	assert(0 == status);
#endif /* PTHREADS */

	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_315, 
			 "list node"));
	return NO_MEMORY;
    }
    node->data = data;

    do {
	temp = update[level];
	node->forward[level] = temp->forward[level];
	temp->forward[level] = node;
	level--;
    } while (level >= 0);

    (list->size)++;

#ifdef PTHREADS
    status = pthread_rwlock_unlock(&list->lock);
    assert(0 == status);
#endif /* PTHREADS */

    return SUCCESS;
} /* end insert_node */



/*
 * Function Name: delete_node (3.82)
 *
 * Description: Removes a node from a list, and returns a pointer to
 *              the data from that node. Also frees the node
 *              structure. This routine is an O(log(n)) operation.
 *
 * Input parameters:
 *     list: A pointer to the list to remove a node from.
 *     data: A pointer to the data of the node to remove. This pointer
 *           is reset to point at the actual key that was in the list, so 
 *           the caller can free it if necessary.
 *
 * Output parameters: 
 *     data: A pointer to the data of the deleted node, so the caller
 *           can free it if necessary.
 *
 * Returns: SUCCESS, FAILURE, or NOT_FOUND.
 */
int delete_node(listT *list, void **data)
{
    char  *funcName = "delete_node";
    nodeT *node;              /* A pointer to the node to be removed. */
    nodeT *update[MAXLEVELS]; /* An array of pointers to nodes */
    nodeT *temp;              /* Temporary pointer to a node. */
    int   level;              /* Temporary level marker. */
    int   status;

#ifdef PTHREADS
    status = pthread_rwlock_wrlock(&list->lock);
    assert(0 == status);
#endif /* PTHREADS */

    temp = list->header;
    level = list->level;

    do {
	while (-1 == list->compare(temp->forward[level]->data, *data)) {
	    temp = temp->forward[level];
	}
	update[level] = temp;
	level--;
    } while (level >= 0);
    node = temp->forward[0];

    if (0 != list->compare(node->data, *data)) {
	writemsg(SV_DEBUG, 
		 catgets(_m_catd, S_FSCK_1, FSCK_316, 
			 "Could not find node to be deleted.\n"));
#ifdef PTHREADS
	status = pthread_rwlock_unlock(&list->lock);
	assert(0 == status);
#endif /* PTHREADS */

	return NOT_FOUND;
    }

    /* Set the pointers needed to remove this node from the list */
    for (level = 0 ;
	 level <= list->level && update[level]->forward[level] == node ;
	 level++) {
	update[level]->forward[level] = node->forward[level];
    }

    while (list->level > 0 &&
	   list->header->forward[list->level] == list->tail) {
	(list->level)--;
    }

    (list->size)--;
    *data = node->data;
    free(node);

#ifdef PTHREADS
    status = pthread_rwlock_unlock(&list->lock);
    assert(0 == status);
#endif /* PTHREADS */

    return SUCCESS;
} /* end delete_node */


/*
 * Function Name: find_node (3.83)
 *
 * Description: Returns a pointer to the data associated with the
 *              requested key. It also returns a pointer to the node
 *              itself for use by find_next_node. This routine is an
 *              O(log(n)) operation.
 *
 * Input parameters:
 *     list: A pointer to the list to search.
 *     data: A pointer to data which contains the key of the desired node.
 *
 * Output parameters: 
 *     data: A pointer to the data associated with the given key in this list.
 *     node: A pointer to the node associated with the given key in
 *           this list. This should ONLY be used as an input parameter to 
 *           find_next_node.
 *
 * Returns: SUCCESS, FAILURE, or NOT_FOUND.
 */

int find_node(listT *list, void **data, nodeT **node)
{
    char  *funcName = "find_node";
    int   level; /* Temporary level marker. */
    int   status;

#ifdef PTHREADS
    status = pthread_rwlock_rdlock(&list->lock);
    assert(0 == status);
#endif /* PTHREADS */

    *node = list->header;
    level = list->level;

    do {
	while (list->compare((*node)->forward[level]->data, *data) == -1) {
	    *node = (*node)->forward[level];
	}
	level--;
    } while (level >= 0);

    *node = (*node)->forward[0];

    if (list->compare((*node)->data, *data) != 0) {
	*node = NULL;
#ifdef PTHREADS
	status = pthread_rwlock_unlock(&list->lock);
	assert(status == 0);
#endif /* PTHREADS */

	return NOT_FOUND;
    }

    *data = (*node)->data;
#ifdef PTHREADS
    status = pthread_rwlock_unlock(&list->lock);
    assert(status == 0);
#endif /* PTHREADS */

    return SUCCESS;
}/* end find_node */



/*
 * Function Name: find_node_or_next (3.nn)
 *
 * Description: Returns a pointer to the data associated with the
 *              requested key, or if that key does not exist, it returns
 *		a pointer to the data associated with the next key in
 *		the list. It also returns a pointer to the node
 *              itself for use by find_next_node. This routine is an
 *              O(log(n)) operation.
 *
 * Input parameters:
 *     list: A pointer to the list to search.
 *     data: A pointer to data which contains the key of the desired node.
 *
 * Output parameters: 
 *     data: A pointer to the data associated with the given key
 *	     or the following key in this list.
 *     node: A pointer to the node associated with the given key
 *	     or the following key in this list. This should ONLY
 *	     be used as an input parameter to find_next_node.
 *
 * Returns: SUCCESS, FAILURE, or NOT_FOUND.
 */

int find_node_or_next(listT *list, void **data, nodeT **node)
{
    char  *funcName = "find_node";
    int   level; /* Temporary level marker. */
    int   status;

#ifdef PTHREADS
    status = pthread_rwlock_rdlock(&list->lock);
    assert(status == 0);
#endif /* PTHREADS */

    *node = list->header;
    level = list->level;

    do {
	while (list->compare((*node)->forward[level]->data, *data) == -1) {
	    *node = (*node)->forward[level];
	}
	level--;
    } while (level >= 0);

    *node = (*node)->forward[0];

    if (*node == list->tail) {
	*node = NULL;

#ifdef PTHREADS
	status = pthread_rwlock_unlock(&list->lock);
	assert(status == 0);
#endif /* PTHREADS */

	return NOT_FOUND;
    }

    *data = (*node)->data;
#ifdef PTHREADS
    status = pthread_rwlock_unlock(&list->lock);
    assert(status == 0);
#endif /* PTHREADS */

    return SUCCESS;
}/* end find_node_or_next */



/*
 * Function Name: find_first_node (3.84)
 *
 * Description: Returns a pointer to the first node in the
 *              list. Calling this followed by successive calls to
 *              find_next_node will visit every node in the list. This
 *              routine is an O(1) operation.
 *
 * Input parameters:
 *     list: A pointer to the list.
 *
 * Output parameters: 
 *     data: A pointer to the data associated with the first (lowest
 *           ordered) node in the list.
 *     node: A pointer to the the first (lowest ordered) node in the
 *           list. This should ONLY be used as an input parameter to 
 *           find_next_node.
 *
 * Returns: SUCCESS, FAILURE, or NOT_FOUND.
 */

int find_first_node(listT *list, void **data, nodeT **node)
{
    char  *funcName = "find_first_node";
    int   status;

#ifdef PTHREADS
    status = pthread_rwlock_rdlock(&list->lock);
    assert(status == 0);
#endif /* PTHREADS */

    *node = list->header->forward[0];

    assert(*node != NULL);

    if (*node == list->tail) {
	*data = NULL;
	*node = NULL;
#ifdef PTHREADS
	status = pthread_rwlock_unlock(&list->lock);
	assert(status == 0);
#endif /* PTHREADS */

	return NOT_FOUND;
    }

    *data = (*node)->data;
#ifdef PTHREADS
    status = pthread_rwlock_unlock(&list->lock);
    assert(status == 0);
#endif /* PTHREADS */

    return SUCCESS;
} /* end find_first_node */



/*
 * Function Name: find_next_node (3.85)
 *
 * Description: Returns a pointer to the node immediately greater than
 *              the node passed in. Calling find_first_node followed
 *              by successive calls to this routine will visit every
 *              node in the list. This routine is an O(1) operation.
 *
 * Input parameters:
 *     list: A pointer to the list.
 *     node: An input pointer to the current node, which is reset to
 *           point to the next node.
 *
 * Output parameters: 
 *     data: A pointer to the next node's data.
 *     node: An input pointer to the current node, which is reset to
 *           point to the next node.
 *
 * Returns: SUCCESS, FAILURE, or NOT_FOUND.
 */
int find_next_node(listT *list, void **data, nodeT **node)
{
    char  *funcName = "find_next_node";
    int status;

#ifdef PTHREADS
    status = pthread_rwlock_rdlock(&list->lock);
    assert(status == 0);
#endif /* PTHREADS */

    *node = (*node)->forward[0];

    if (*node == list->tail) {
	*data = NULL;
	*node = NULL;
#ifdef PTHREADS
	status = pthread_rwlock_unlock(&list->lock);
	assert(0 == status);
#endif /* PTHREADS */
	return NOT_FOUND;
    }

    *data = (*node)->data;
#ifdef PTHREADS
    status = pthread_rwlock_unlock(&list->lock);
    assert(status == 0);
#endif /* PTHREADS */

    return SUCCESS;

} /* end find_next_node */

/* end fixfdmn_list.c */
