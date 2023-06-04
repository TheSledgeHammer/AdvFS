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
 *      Mon Jan 10 13:20:44 PST 2000
 */
/*
 * HISTORY
 */
#pragma ident "@(#)$RCSfile: fixfdmn_cache.c,v $ $Revision: 1.1.29.1 $ (DEC) $Date: 2006/03/17 03:04:25 $"

#include "fixfdmn.h"

/*
 * Globals
 */
extern cacheT *cache;
extern nl_catd _m_catd;



/*
 * Function Name: create_cache (3.86)
 *
 * Description: Creates the data cache.  Three arrays are malloc'ed,
 *		of types pageT, pageBufT, and pthread_mutex_t, with
 *              nothing pointing to the arrays themselves,
 *              only pointers into each individual entry in each
 *              array. These array can thus never be freed, but it's
 *              only a theoretical memory leak since they're designed
 *              to last the entire life of the program anyway.
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
    int  i;            /* A counter. */
    struct page	*rCacheArray;	  /* Array of read cache page header info */
    pageBufT	*rCacheArrayData; /* Array of pages for the read cache */
    pthread_mutex_t *lockArray;   /* A pointer to an array of locks. */
    pageT *maxPage;


    cache = (cacheT *)ffd_malloc(sizeof(cacheT));
    if (NULL == cache) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_184, "cache"));
	return NO_MEMORY;
    }

    status = ffd_mutex_create(&(cache->lock));
    if (SUCCESS != status) {
	return status;
    }
    status = tis_mutex_lock(cache->lock);
    assert(0 == status);

    maxPage = (pageT *)ffd_malloc(sizeof(pageT));
    if (NULL == maxPage) {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_185, 
			 "page list tail"));
	return NO_MEMORY;
    }
    maxPage->vol = MAX_VOLUMES;
    maxPage->pageLBN = MAXUINT;
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

    rCacheArray = (pageT *)ffd_calloc(cache->maxSize, sizeof(pageT));
    rCacheArrayData = (pageBufT *)ffd_calloc(cache->maxSize, sizeof(pageBufT));
    lockArray =
	(pthread_mutex_t *)ffd_calloc(cache->maxSize, sizeof(pthread_mutex_t));
    if (NULL == rCacheArray ||
	NULL == rCacheArrayData ||
	NULL == lockArray)
    {
	writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			 "Can't allocate memory for %s.\n"),
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_186, 
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
	rCacheArray[i].lock    = &lockArray[i];
	status = tis_mutex_init(&(lockArray[i]));
	if (ENOMEM == status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
			     "Can't allocate memory for %s.\n"),
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_187,
			     "mutex init"));
	    return NO_MEMORY;
	} else {
	    assert(0 == status);
	}
    }

    cache->MRU = &rCacheArray[0];
    rCacheArray[0].nextMRU = NULL;
    cache->LRU = &rCacheArray[cache->maxSize - 1];
    rCacheArray[cache->maxSize - 1].nextLRU = NULL;

    cache->domain = domain;

    status = tis_mutex_unlock(cache->lock);
    assert(0 == status);

    return SUCCESS;
} /* end create_cache */



/*
 * Function Name: compare_pages (3.87)
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
 * Function Name: read_page_from_cache (3.88)
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
int read_page_from_cache(int volume, LBNT lbn, pageT **page)
{
    char 	 *funcName = "read_page_from_cache";
    int		 status;
    int		 i;
    pageT	 *cachePage;
    pageT	 tempPage;
    nodeT	 *node;
    struct page	 *rCacheArray;	  /* Array of read cache page header info */
    pageBufT	 *rCacheArrayData; /* Array of pages for the read cache */
    pthread_mutex_t *lockArray;
    off_t	 bytes;

    status = tis_mutex_lock(cache->lock);
    assert(0 == status);

    tempPage.vol = volume;
    tempPage.pageLBN = lbn;
    cachePage = &tempPage;

    status = find_node(cache->writeCache, (void *)&cachePage, &node);
    if (SUCCESS == status) {
	status = tis_mutex_lock(cachePage->lock);
	if (0 != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_188, 
			     "Can't obtain mutex to lock page %d,%d - Might already be open.\n"),
		     volume, lbn);
	    return FAILURE;
	}
	*page = cachePage;
	cache->writeHits++;
	status = tis_mutex_unlock(cache->lock);
	if (0 != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_189, 
			     "Can't release mutex lock.\n"));
	    return FAILURE;
	}
	return SUCCESS;
    }

    /*
     * We now know the page is not in the write cache.
     */

    cachePage = &tempPage;
    status = find_node(cache->readCache, (void *)&cachePage, &node);
    if (SUCCESS == status) {
	/*
	 * Pull page out of the LRU stack.
	 */
	if (NULL == cachePage->nextMRU) {
	    cache->MRU = cachePage->nextLRU;
	} else {
	    cachePage->nextMRU->nextLRU = cachePage->nextLRU;
	}
	if (NULL == cachePage->nextLRU) {
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

	status = tis_mutex_lock(cachePage->lock);
	if (0 != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_188,
			     "Can't obtain mutex to lock page %d,%d - Might already be open.\n"),
		     volume, lbn);
	    return FAILURE;
	}
	cache->pagesLocked++;
	*page = cachePage;
	cache->readHits++;
	status = tis_mutex_unlock(cache->lock);
	if (0 != status) {
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_189,
			     "Can't release mutex lock.\n"));
	    return FAILURE;
	}
	return SUCCESS;
    }

    /*
     * The requested page is not in either cache.  Read it from disk.
     */

    /*
     * If every page in the readCache is locked, double the readCache size.
     */
    if (cache->pagesLocked == cache->maxSize) {
	writemsg(SV_DEBUG | SV_LOG_INFO, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_190, 
			 "Doubling read cache size.\n"));
	rCacheArray = (pageT *)ffd_calloc(cache->maxSize, sizeof(pageT));
	rCacheArrayData =
	    (pageBufT *)ffd_calloc(cache->maxSize, sizeof(pageBufT));
	lockArray = (pthread_mutex_t *)ffd_calloc(cache->maxSize, sizeof(pthread_mutex_t));
	if (NULL == rCacheArray ||
	    NULL == rCacheArrayData ||
	    NULL == lockArray)
	{
	    writemsg(SV_ERR | SV_LOG_ERR, 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
			     "Can't allocate memory for %s.\n"), 
		     catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_186, 
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
	    rCacheArray[i].lock    = &lockArray[i];
	    status = tis_mutex_init(&(lockArray[i]));
	    if (ENOMEM == status) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10,
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_187, 
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
    while (0 != tis_mutex_trylock(cache->LRU->lock)) {
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
		    catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_191,
			    "Internal error: Infinite loop while walking read cache.\n"));
	    failed_exit();
	}
    }
    status = tis_mutex_unlock(cache->LRU->lock);
    assert(0 == status);

    /*
     * Drop LRU page of readCache on the floor
     */
    if (cache->readCache->size >= cache->maxSize) {
	status = delete_node(cache->readCache, (void **)&cache->LRU);
	if (SUCCESS != status) {
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
    if ((off_t)-1 == lseek(cache->domain->volumes[cachePage->vol].volFD,
			   cachePage->pageLBN * (long)DEV_BSIZE,
			   SEEK_SET)) {
        writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_192, 
			 "Can't lseek to block %d on '%s'.\n"),
		 lbn, cache->domain->volumes[cachePage->vol].volName);
	perror(Prog);
	return FAILURE;
    }

    bytes = read(cache->domain->volumes[cachePage->vol].volFD,
		 *(cachePage->pageBuf), PAGESIZE);

    if ((off_t)-1 == bytes) {
        writemsg(SV_ERR | SV_LOG_ERR, 
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_193, 
			 "Can't read page at block %d on '%s'.\n"),
		 lbn, cache->domain->volumes[cachePage->vol].volName);
	perror(Prog);
	return FAILURE;
    }
    else if (PAGESIZE != bytes) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_194, 
			 "Can't read page at block %d on '%s', only read %d of %d bytes.\n"),
		 lbn, cache->domain->volumes[cachePage->vol].volName,
		 bytes, PAGESIZE);
	return FAILURE;
    }

    /*
     * Put cachePage back into readCache, and return it to user.
     */
    status = insert_node(cache->readCache, cachePage);
    if (SUCCESS != status) {
	return status;
    }

    status = tis_mutex_lock(cachePage->lock);
    if (0 != status) {
	writemsg(SV_ERR | SV_LOG_ERR,
		 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_188, 
			 "Can't obtain mutex to lock page %d,%d - Might already be open.\n"),
		 volume, lbn);
	return FAILURE;
    }
    cache->pagesLocked++;
    *page = cachePage;
    cache->misses++;

    status = tis_mutex_unlock(cache->lock);
    assert(0 == status);

    return SUCCESS;
} /* end read_page_from_cache */



/*
 * Function Name: release_page_to_cache (3.89)
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
int release_page_to_cache(int volume, LBNT lbn, pageT *page,
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

    status = tis_mutex_lock(cache->lock);
    assert(0 == status);

    if (TRUE == modified || TRUE == keepPageForever) {
	writePage = &tempPage;

	status = find_node(cache->writeCache, (void *)&writePage, &node);
	if (SUCCESS == status) {
	    assert(writePage == page);

	    if (TRUE == modified) {
		P_SET_MODIFIED(writePage->status);
	    }
	} else {
	    /*
	     * This page isn't in the writeCache.  It better be in
	     * the readCache.
	     */
	    readPage = &tempPage;
	    status = find_node(cache->readCache, (void *)&readPage, &node);
	    assert(SUCCESS == status);
	    assert(readPage == page);
	    foundInWriteCache = FALSE;

	    /*
	     * Remove page from readCache.
	     */
	    status = delete_node(cache->readCache, (void *)&readPage);
	    if (SUCCESS != status) {
		return status;
	    }

	    /*
	     * Pull page out of LRU chain.
	     */
	    if (NULL == readPage->nextMRU) {
		cache->MRU = readPage->nextLRU;
	    } else {
		readPage->nextMRU->nextLRU = readPage->nextLRU;
	    }
	    if (NULL == readPage->nextLRU) {
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
	    if (NULL == writePage) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_195,
				 "write cache page"));
		return NO_MEMORY;
	    }
	    writePage->pageBuf = (pageBufT *)ffd_malloc(sizeof(pageBufT));
	    if (NULL == writePage->pageBuf) {
		writemsg(SV_ERR | SV_LOG_ERR, 
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_10, 
				 "Can't allocate memory for %s.\n"),
			 catgets(_m_catd, S_FIXFDMN_1, FIXFDMN_196,
				 "write cache page buffer"));
		return NO_MEMORY;
	    }
	    writePage->vol	= readPage->vol;
	    writePage->pageLBN	= readPage->pageLBN;
	    memcpy(writePage->pageBuf, readPage->pageBuf, PAGESIZE);
	    writePage->status	= readPage->status;
	    writePage->messages	= readPage->messages;
	    status = tis_mutex_unlock(readPage->lock);
	    assert(0 == status);
	    status = ffd_mutex_create(&(writePage->lock));
	    if (SUCCESS != status) {
		return status;
	    }
	    /* 
	     * This needs to be locked only because it's brand-new and
	     * about to be unlocked. 
	     */
	    status = tis_mutex_lock(writePage->lock);
	    assert(0 == status);

	    if (TRUE == modified) {
		P_SET_MODIFIED(writePage->status);
	    }

	    status = insert_node(cache->writeCache, writePage);
	    if (SUCCESS != status) {
		return status;
	    }

	    page = writePage;
	}
    }

    readPage = &tempPage;
    status = find_node(cache->writeCache, (void *)&readPage, &node);
    if (SUCCESS != status || readPage != page) {
	status = find_node(cache->readCache, (void *)&readPage, &node);
	assert(SUCCESS == status);
	assert(readPage == page);
	foundInWriteCache = FALSE;
    }

    status = tis_mutex_unlock(page->lock);
    assert(0 == status);

    if (FALSE == foundInWriteCache) {
	(cache->pagesLocked)--;
    }
    status = tis_mutex_unlock(cache->lock);
    assert(0 == status);

    return SUCCESS;
} /* end release_page_to_cache */

/* end fixfdmn_cache.c */
