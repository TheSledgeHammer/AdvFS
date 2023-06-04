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

/*

OPERATION NOTES:
  
When fcsk wants a file page from the disk, it requests the page from
the cache.  The cache has separate read and write sections.  When a
page request comes in the cache checks the write-cache first.  This is
an unlimited size skiplist of pages that have been "modified" by fsck
but not yet changed on disk. If the requested page is in the
write-cache, the cache locks the page and returns a pointer to
it.  Otherwise, the read cache is checked. 

If the page is found in the read cache it is moved to the top of the
read-cache's Least Recently Used stack (making that page the Most
Recently Used page), and a pointer to that page is passed back to the
caller.

If the requested page is not in either cache, the page is read from
the disk.  If the read cache is full, the LRU (Least Recently Used)
page is discarded and the newly read page is marked as the MRU (Most
Recently Used) page, and a pointer to the page is returned to the
caller.

When fcsk is through with a page it calls the cache to "release" the
page with a "modify" argument.  If "modify" is true, the page is added
to the write cache.  Otherwise the page is returned to the read cache
as the MRU.

 - - - - - - - - - - - -

The locking poriton of this code was devised to protect cache entries
when multi-threading is in use.  A multi-threading scheme is intended
to speed up operations on multiple filesets at one time through
parallel execution, but has not been implemented at this time.  Yet
the cache locking mechanism has been designed and implemented and uses
the HP/UX pthreads library (it was ported from Tru64, where it used
the DECthreads library).

The locking code is currently compiled out on a PTHREADS definition.
This is because 1) multiple threads are not yet implemented so the
locking is not needed, and 2) there is currently no 64-bit static
pthread library being built.  The static library is needed because
fsck can be run in single user mode before shared libraries are
enabled.

*/


#include "fsck.h"

/*
 * Globals
 */
extern cacheT *cache;
extern nl_catd _m_catd;



/*
 * Function Name: create_cache
 *
 * Description: Creates the data cache.  Two arrays of types pageT,
 *              pageBufT, are created to form the memory used in the 
 *              cache.
 *
 * Input parameters:
 *     cache: A pointer to a pointer to the cache to be created.
 *
 * Output parameters: 
 *     cache: A pointer to a pointer to the cache which has been created.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY
 */

int create_cache(domainT *domain)
{
    char *funcName = "create_cache";
    int  status;
    int  i;                       /* A counter. */
    pageT    *rCacheArray;	  /* Array of read cache page header info */
    pageBufT *rCacheArrayData;    /* Array of pages for the read cache */
    pageT    *maxPage;


    cache = (cacheT *)ffd_malloc(sizeof(cacheT));
    if (NULL == cache) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_184, "cache"));
	return NO_MEMORY;
    }

#ifdef PTHREADS
    status = pthread_mutex_init(&(cache->lock), NULL);
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

    status = pthread_mutex_lock(cache->lock);
    assert(0 == status);
#endif /* PTHREADS */

    maxPage = (pageT *)ffd_malloc(sizeof(pageT));
    if (NULL == maxPage) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_185, 
			 "page list tail"));
	return NO_MEMORY;
    }
    maxPage->vol = MAX_VOLUMES;
    maxPage->pageLBN = -1;
    status = create_list(&compare_pages, &cache->readCache, maxPage);
    if (SUCCESS != status) {
	return status;
    }
    status = create_list(&compare_pages, &cache->writeCache, maxPage);
    if (SUCCESS != status) {
	return status;
    }

    cache->writeHits   = 0;
    cache->readHits    = 0;
    cache->misses      = 0;
    cache->pagesLocked = 0;

    cache->maxSize = INITIAL_CACHE_SIZE;
    pageT       *pageArray;       /* start adrs of cache page descriptors */
    pageBufT    *pageArrayData;   /* start adrs of cache pages */

    /* allocate the data for tha cache page array and the list of
       pageT structs that define each page.  We preserve the addresses
       of these arrays in the cache struct so that they can be freed
       in free_cache() below. */

    cache->pageArray = (pageT *)ffd_calloc(cache->maxSize, sizeof(pageT));
    rCacheArray = cache->pageArray;

    cache->pageArrayData = (pageBufT *)ffd_calloc(cache->maxSize, 
						  sizeof(pageBufT));
    rCacheArrayData = cache->pageArrayData;

    if (NULL == rCacheArray ||
	NULL == rCacheArrayData)
    {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FSCK_1, FSCK_186, 
			 "cache data"));
	return NO_MEMORY;
    }

    for (i = 0 ; i < cache->maxSize ; i++) {
	rCacheArray[i].vol     = 0;
	rCacheArray[i].pageLBN = 0;
	rCacheArray[i].pageBuf = &(rCacheArrayData[i]);
	rCacheArray[i].status  = 0;
	rCacheArray[i].messages = NULL;
	rCacheArray[i].nextMRU = &(rCacheArray[i - 1]);
	rCacheArray[i].nextLRU = &(rCacheArray[i + 1]);
#ifdef PTHREADS
	status = pthread_mutex_init(&(rCacheArray[i].lock), NULL);
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
    }

    cache->MRU = &rCacheArray[0];
    rCacheArray[0].nextMRU = NULL;
    cache->LRU = &rCacheArray[cache->maxSize - 1];
    rCacheArray[cache->maxSize - 1].nextLRU = NULL;

    cache->domain = domain;

#ifdef PTHREADS
    status = pthread_mutex_unlock(cache->lock);
    assert(0 == status);
#endif /* PTHREADS */

    return SUCCESS;
} /* end create_cache */

/*
 * Function Name: free_cache
 *
 * Description: 
 *
 *   Free memory previously allocated for the cache and also that
 *   allocated for the cached page skip list.
 *
 *  The page cache is made up of four separate memory allocations made
 *  in the create_cache() function above.
 *  
 *  The read and write caches are separate skip list structures
 *  allocated in separate calls to the create_list() function.  The
 *  pageT structures describe each cache page and are gang allocated
 *  as a single array whose elements are stored an element at a time
 *  in the cache.  Similarly, the page buffers (for disk reads) are
 *  gang allocated as a single array whose elements are stored an
 *  element at a time in the cache along with their corresponding
 *  pageT elements.  What all this means is that pageT structs and
 *  page buffers can't be freed individually, but are freed
 *  collectively by freeing the arrays of which they are part.
 *
 * Input parameters:
 *     none. (the cache pointer is global)
 *
 * Output parameters: 
 *     none.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY
 */

int free_cache()
{
    pageT *page;
    nodeT *pNode;

    /* free the nodes in the skip lists */

    while (find_first_node(cache->readCache, (void *)&page, &pNode) 
	   == SUCCESS) {
	delete_node(cache->readCache, (void **)&page);
    }

    while (find_first_node(cache->writeCache, (void *)&page, &pNode) 
	   == SUCCESS) {
	delete_node(cache->writeCache, (void **)&page);
    }

    /* free the memory allocated to the cache itself. The cache's
       "linked list" of pageT structs (MRU-LRU) is actually
       implemented as a single array of pageT structs. It is pointers
       to these structs that is returned from the find_node calls
       (above).  To free them all we just have to free the array. */

    free(cache->writeCache);      /* free the write cache skip list struct */
    free(cache->readCache);       /* free the read cache skip list struct */
    free(cache->pageArray);       /* free the page data array */
    free(cache->pageArrayData);   /* free the pageT struct array */
    free(cache);                  /* free the cache struct itself */

    return (SUCCESS);

} /* end free_cache */


/*
 * Function Name: compare_pages
 *
 * Description: This function compares the volume/LBN of two pages to
 *		determine if they're the same page, or which one is
 *		"less" than the other. Note that it is the caller's
 *		responsibility to ensure that the LBN is evenly
 *		divisible by 16.
 *
 *		Note that this is only referred to by a pointer to this
 *		function.  It is never called directly.
 *
 * Input parameters:
 *     a: A pointer to the first pageT.
 *     b: A pointer to the second pageT.
 *
 * Output parameters: N/A
 *
 * Returns: -1 if a is less than b.
 *           0 if a is equal to b.
 *          +1 if a is greater than b.
 */
int compare_pages(void *a, void *b)
{
    char *funcName = "compare_pages";

    assert(NULL != (pageT *)a);
    assert(NULL != (pageT *)b);

    /*
     * Note that it compares the logical block number, then the volume
     * number for performance reasons.  Skiplists perform a lot of
     * comparisons, and the sooner a difference can be found the better.
     */

    if (((pageT *)a)->pageLBN < ((pageT *)b)->pageLBN) {
	return -1;
    }
    if (((pageT *)a)->pageLBN > ((pageT *)b)->pageLBN) {
	return +1;
    }
    if (((pageT *)a)->vol     < ((pageT *)b)->vol) {
	return -1;
    }
    if (((pageT *)a)->vol     > ((pageT *)b)->vol) {
	return +1;
    }
    return 0;
} /* end compare_pages */



/*
 * Function Name: read_page_from_cache
 *
 * Description: Returns a pointer to a specific volume's page. That
 *              page is stored in the cache, and if it was already in
 *              the cache, it is not re-read from disk.  The returned
 *              page is locked, and it is the caller's responsibility
 *              to call release_page_to_cache to unlock the page.
 *              Note that it is the caller's responsibility to ensure
 *              that the LBN is evenly divisible by 16.
 *
 * Input parameters:
 *     volume: Volume number the requested page lives on.
 *     lbn: Logical block number of the first block in the requested page.
 *
 * Output parameters: 
 *     page: Pointer to a pointer to a page structure with data for the 
 *           requested page.
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 *
 * Side Effects: If every page in the readCache is locked, this routine
 *               will double the size of the readCache.
 */
int read_page_from_cache(volNumT volume, lbnNumT lbn, pageT **page)
{
    char 	 *funcName = "read_page_from_cache";
    int		 status;
    uint64_t	 i;
    pageT	 *cachePage;
    pageT	 tempPage;
    nodeT	 *node;
    struct page	 *rCacheArray;	  /* Array of read cache page header info */
    pageBufT	 *rCacheArrayData; /* Array of pages for the read cache */
    size_t	 bytes;

#ifdef PTHREADS
    status = pthread_mutex_lock(cache->lock);
    assert(status == 0);
#endif /* PTHREADS */

    tempPage.vol = volume;
    tempPage.pageLBN = lbn;
    cachePage = &tempPage;

    status = find_node(cache->writeCache, (void *)&cachePage, &node);
    if (status == SUCCESS) {

#ifdef PTHREADS
	status = pthread_mutex_lock(cachePage->lock);
	if (status != 0) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_188, 
			     "Can't obtain mutex to lock page %ld,%d - Might already be open.\n"),
		     volume, lbn);
	    return FAILURE;
	}
#endif /* PTHREADS */

	*page = cachePage;
	cache->writeHits++;

#ifdef PTHREADS
	status = pthread_mutex_unlock(cache->lock);
	if (status != 0) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_189, 
			     "Can't release mutex lock.\n"));
	    return FAILURE;
	}
#endif /* PTHREADS */

	return SUCCESS;
    }

    /*
     * We now know the page is not in the write cache.
     */

    cachePage = &tempPage;
    status = find_node(cache->readCache, (void *)&cachePage, &node);
    if (status == SUCCESS) {
	/*
	 * Pull page out of the LRU stack.
	 */
	if (cachePage->nextMRU == NULL) {
	    cache->MRU = cachePage->nextLRU;
	} else {
	    cachePage->nextMRU->nextLRU = cachePage->nextLRU;
	}
	if (cachePage->nextLRU == NULL) {
	    cache->LRU = cachePage->nextMRU;
	} else {
	    cachePage->nextLRU->nextMRU = cachePage->nextMRU;
	}

	/*
	 * Put page in MRU position of the LRU stack.
	 */
	cache->MRU->nextMRU = cachePage;
	cachePage->nextLRU = cache->MRU;
	cachePage->nextMRU = NULL;
	cache->MRU = cachePage;

#ifdef PTHREADS
	status = pthread_mutex_lock(cachePage->lock);
	if (status != 0) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_188,
			     "Can't obtain mutex to lock page %ld,%d - Might already be open.\n"),
		     volume, lbn);
	    return FAILURE;
	}
	cache->pagesLocked++;
#endif /* PTHREADS */

	*page = cachePage;
	cache->readHits++;

#ifdef PTHREADS
	status = pthread_mutex_unlock(cache->lock);
	if (status != 0) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_189,
			     "Can't release mutex lock.\n"));
	    return FAILURE;
	}
#endif /* PTHREADS */

	return SUCCESS;
    }

    /*
     * The requested page is not in either cache.  Read it from disk.
     */

    /*
     * If every page in the readCache is locked, double the readCache size.
     */
#ifdef PTHREADS
    if (cache->pagesLocked == cache->maxSize) {
	writemsg(SV_DEBUG | SV_LOG_INFO, 
		 catgets(_m_catd, S_FSCK_1, FSCK_190, 
			 "Doubling read cache size.\n"));
	rCacheArray = (pageT *)ffd_calloc(cache->maxSize, sizeof(pageT));
	rCacheArrayData =
	    (pageBufT *)ffd_calloc(cache->maxSize, sizeof(pageBufT));

	if (rCacheArray == NULL ||
	    rCacheArrayData == NULL)
	{
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FSCK_1, FSCK_10, 
			     "Can't allocate memory for %s.\n"), 
		     catgets(_m_catd, S_FSCK_1, FSCK_186, 
			     "cache data"));
	    return NO_MEMORY;
	}

	for (i = 0 ; i < cache->maxSize ; i++) {
	    rCacheArray[i].vol     = 0;
	    rCacheArray[i].pageLBN = 0;
	    rCacheArray[i].pageBuf = &(rCacheArrayData[i]);
	    rCacheArray[i].status  = 0;
	    rCacheArray[i].messages  = NULL;
	    rCacheArray[i].nextMRU = &(rCacheArray[i - 1]);
	    rCacheArray[i].nextLRU = &(rCacheArray[i + 1]);

	    status = pthread_mutex_init(&(rCacheArray[i].lock), NULL);
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
	}

	cache->LRU->nextLRU = &rCacheArray[0];
	rCacheArray[0].nextMRU = cache->LRU;
	cache->LRU = &rCacheArray[cache->maxSize - 1];
	rCacheArray[cache->maxSize - 1].nextLRU = NULL;

	cache->maxSize *= 2;
    } /* Finished doubling cache size if needed. */

    /*
     * Now we know there is at least one unlocked page in the readCache.
     * Ensure that the LRU page is not locked.
     */
    i = 0;
    while (pthread_mutex_trylock(cache->LRU->lock) != 0) {
	/*
	 * Move the LRU page to the MRU slot because it's locked.
	 */
	cache->MRU->nextMRU = cache->LRU;
	cache->LRU->nextLRU = cache->MRU;
	cache->LRU = cache->LRU->nextMRU;
	cache->MRU = cache->MRU->nextMRU;
	cache->MRU->nextMRU = NULL;
	cache->LRU->nextLRU = NULL;
	i++;
	if (i > cache->maxSize) {
	    writemsg(SV_ERR | SV_LOG_ERR,
		    catgets(_m_catd, S_FSCK_1, FSCK_191,
			    "Internal error: Infinite loop while walking read cache.\n"));
	    failed_exit();
	}
    }
    status = pthread_mutex_unlock(cache->LRU->lock);
    assert(0 == status);
#endif /* PTHREADS */

    /*
     * Drop LRU page of readCache on the floor
     */
    if (cache->readCache->size >= cache->maxSize) {
	status = delete_node(cache->readCache, (void **)&cache->LRU);
	if (status != SUCCESS) {
	    return status;
	}
    }
    /*
     * Now push that LRU page into the MRU slot and use it.
     */

    cache->MRU->nextMRU = cache->LRU;
    cache->LRU->nextLRU = cache->MRU;
    cache->MRU = cache->LRU;
    cache->LRU = cache->LRU->nextMRU;
    cache->MRU->nextMRU = NULL;
    cache->LRU->nextLRU = NULL;

    /*
     * Initialize cachePage data
     */
    cachePage = cache->MRU;

    cachePage->vol = volume;
    cachePage->pageLBN = lbn;
    cachePage->status = 0;
    cachePage->messages = NULL;

    /*
     * Read the page from disk.
     */
    if ((lseek(cache->domain->volumes[cachePage->vol].volFD,
			   cachePage->pageLBN * (int64_t)DEV_BSIZE,
			   SEEK_SET)) == (off_t)-1) {
        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_192, 
			 "Can't lseek to block %ld on '%s'.\n"),
		 lbn, cache->domain->volumes[cachePage->vol].volName);
	perror(Prog);
	return FAILURE;
    }

    bytes = read(cache->domain->volumes[cachePage->vol].volFD,
		 *(cachePage->pageBuf), ADVFS_METADATA_PGSZ);

    if (bytes == (ssize_t)-1) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FSCK_1, FSCK_193, 
			 "Can't read page at block %ld on '%s'.\n"),
		 lbn, cache->domain->volumes[cachePage->vol].volName);
	perror(Prog);
	return FAILURE;
    }
    else if (bytes != (ssize_t) ADVFS_METADATA_PGSZ) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_194, 
			 "Can't read page at block %ld on '%s', only read %ld of %d bytes.\n"),
		 lbn, cache->domain->volumes[cachePage->vol].volName,
		 (ssize_t)bytes, ADVFS_METADATA_PGSZ);
	return FAILURE;
    }

    /*
     * Put cachePage back into readCache, and return it to user.
     */
    status = insert_node(cache->readCache, cachePage);
    if (status != SUCCESS) {
	return status;
    }

#ifdef PTHREADS
    status = pthread_mutex_lock(cachePage->lock);
    if (status != 0) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FSCK_1, FSCK_188, 
			 "Can't obtain mutex to lock page %ld,%d - Might already be open.\n"),
		 volume, lbn);
	return FAILURE;
    }
    cache->pagesLocked++;
#endif /* PTHREADS */

    *page = cachePage;
    cache->misses++;

#ifdef PTHREADS
    status = pthread_mutex_unlock(cache->lock);
    assert(status == 0);
#endif /* PTHREADS */

    return SUCCESS;
} /* end read_page_from_cache */



/*
 * Function Name: release_page_to_cache
 *
 * Description: This routine unlocks a page to allow use by other
 *              threads. It will also modify the page and move it to
 *              the write cache if the modified flag is set. It will
 *              also copy the page to the write cache if the
 *              keepPageForever flag is set.
 *
 * Input parameters:
 *     volume: Volume number the page lives on.
 *     lbn: Logical block number of the first block in the page.
 *     page: A pointer to the page structure.
 *     modified: Flag indicating if the thread changed this page or not.
 *     keepPageForever: Flag indicating if this page should be moved
 *                      to the writeCache without modification.
 *
 * Output parameters: N/A
 *
 * Returns: SUCCESS, FAILURE, or NO_MEMORY.
 */
int release_page_to_cache(volNumT volume, lbnNumT lbn, pageT *page,
                          int modified, int keepPageForever)
{
    char  *funcName = "release_page_to_cache";
    int   status;
    pageT *readPage;  /* A pointer to a read page. */
    pageT *writePage; /* A pointer to a write page. */
    pageT tempPage;
    nodeT *node;
    int   foundInWriteCache;

    tempPage.vol     = volume;
    tempPage.pageLBN = lbn;
    foundInWriteCache = TRUE;

#ifdef PTHREADS
    status = pthread_mutex_lock(cache->lock);
    assert(0 == status);
#endif /* PTHREADS */

    if (modified == TRUE || keepPageForever == TRUE) {
	writePage = &tempPage;

	status = find_node(cache->writeCache, (void *)&writePage, &node);
	if (status == SUCCESS) {
	    assert(writePage == page);

	    if (modified == TRUE) {
		P_SET_MODIFIED(writePage->status);
	    }
	} else {
	    /*
	     * This page isn't in the writeCache.  It better be in
	     * the readCache.
	     */
	    readPage = &tempPage;
	    status = find_node(cache->readCache, (void *)&readPage, &node);
	    assert(status == SUCCESS);
	    assert(readPage == page);
	    foundInWriteCache = FALSE;

	    /*
	     * Remove page from readCache.
	     */
	    status = delete_node(cache->readCache, (void **)&readPage);
	    if (status != SUCCESS) {
		return status;
	    }

	    /*
	     * Pull page out of LRU chain.
	     */
	    if (readPage->nextMRU == NULL) {
		cache->MRU = readPage->nextLRU;
	    } else {
		readPage->nextMRU->nextLRU = readPage->nextLRU;
	    }
	    if (readPage->nextLRU == NULL) {
		cache->LRU = readPage->nextMRU;
	    } else {
		readPage->nextLRU->nextMRU = readPage->nextMRU;
	    }

	    /*
	     * Add page at end of LRU chain so it'll be dropped next.
	     */
	    readPage->nextMRU = cache->LRU;
	    readPage->nextLRU = NULL;
	    cache->LRU->nextLRU = readPage;
	    cache->LRU = readPage;

	    /*
	     * Malloc page for writeCache.
	     */
	    writePage = (pageT *)ffd_malloc(sizeof(pageT));
	    if (writePage == NULL) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_10, 
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FSCK_1, FSCK_195,
				 "write cache page"));
		return NO_MEMORY;
	    }
	    writePage->pageBuf = (pageBufT *)ffd_malloc(sizeof(pageBufT));
	    if (writePage->pageBuf == NULL) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FSCK_1, FSCK_10, 
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FSCK_1, FSCK_196,
				 "write cache page buffer"));
		return NO_MEMORY;
	    }
	    writePage->vol	= readPage->vol;
	    writePage->pageLBN	= readPage->pageLBN;
	    memcpy(writePage->pageBuf, readPage->pageBuf, ADVFS_METADATA_PGSZ);
	    writePage->status	= readPage->status;
	    writePage->messages	= readPage->messages;
#ifdef PTHREADS
	    status = pthread_mutex_unlock(readPage->lock);
	    assert(0 == status);

	    status = pthread_mutex_init(&(writePage->lock), NULL);
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

	    /* 
	     * This needs to be locked only because it's brand-new and
	     * about to be unlocked. 
	     */

	    status = pthread_mutex_lock(writePage->lock);
	    assert(0 == status);
#endif /* PTHREADS */

	    if (modified == TRUE) {
		P_SET_MODIFIED(writePage->status);
	    }

	    status = insert_node(cache->writeCache, writePage);
	    if (status != SUCCESS) {
		return status;
	    }
	    page = writePage;
	}
    }

    readPage = &tempPage;
    status = find_node(cache->writeCache, (void *)&readPage, &node);
    if (status != SUCCESS || readPage != page) {
	status = find_node(cache->readCache, (void *)&readPage, &node);
	assert(status == SUCCESS);
	assert(readPage == page);
	foundInWriteCache = FALSE;
    }

#ifdef PTHREADS
    status = pthread_mutex_unlock(page->lock);
    assert(0 == status);

    if (foundInWriteCache == FALSE) {
	(cache->pagesLocked)--;
    }

    status = pthread_mutex_unlock(cache->lock);
    assert(0 == status);
#endif /* PTHREADS */

    return SUCCESS;
} /* end release_page_to_cache */

/* end fixfdmn_cache.c */
