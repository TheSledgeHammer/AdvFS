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
 *
 * Copyright (c) 2002-2004 Hewlett-Packard Development Company, L.P.
 *
 * Abstract:
 *
 *      This module defines several routines for managing and
 *      accessing a general purpose buffer pool.  Each pool
 *      consists of one or more subpools where the actual buffers
 *      are maintained.  In other words, a buffer pools consists of
 *      one or more subpools, where each subpools consists of one
 *      or more buffers.  The main purpose of the subpools is so that
 *      a pool client can dynamically expand a pool by some number
 *      of buffers.  This could have been achieved by allocating
 *      each buffer dynmically one at a time and keeping them on
 *      linked lists.  However, for historical reasons, the original
 *      buffer pool had the requirement that buffers be contiguous.
 *      Also, scanning linked lists is not terribly efficient.  The
 *      current implementation uses indexing into arrays to access
 *      buffers.
 *
 *      When a buffer pool is expanded a new subpool is created to 
 *      contain the additional buffers.
 *
 *      All buffers in a single pool (and its subpools) are the same
 *      size.  The size is determined when the pool is created.
 *
 *      All buffer pool routines are thread-safe.
 *
 */

#include <advfs/backup.h>
#include <advfs/util.h>
#include <stdarg.h>
#include <sys/signal.h>
#include "libvdump_msg.h"

extern nl_catd catd;
extern char *sys_errlist[];

/* buffer pool private definitions */

#define NIL_BUF -1

/*
 * Buffer sub-pool descriptor 
 */

typedef struct sub_pool {
    int32_t num_bufs;               /* number of buffers in sub pool        */
    char *bufs;                     /* array of 'num_bufs * buf_size' bytes */
    int32_t bufs_avail;             /* number of buffers that are available */
    int32_t free_buf;               /* head of free buffer list             */
    int32_t *free_bufs;             /* free buffer list                     */
    int32_t *buf_stats;             /* buffer usage statistics              */
    struct sub_pool *nxt_sub_pool;  /* ptr to next sub pool descriptor      */
} sub_pool_t;

static const sub_pool_t nil_sub_pool = { 0 };

/* 
 * Buffer pool descriptor definition 
 */

typedef struct buf_pool {
    pthread_mutex_t mutex;          /* synchronization mutex                */
    pthread_cond_t cv;              /* synchronization condition variable   */
    int32_t deleted;                /* flag indicating pool is deleted      */
    int32_t waiters;                /* num threads waiting for a free buffer*/
    int32_t buf_size;               /* number of bytes per buffer           */
    int32_t num_bufs;               /* number of buffers in buffer pool     */
    int32_t bufs_avail;             /* number of buffers that are available */
    sub_pool_t *sub_pool;           /* ptr to first subpool                 */
    sub_pool_t *sub_pool_hint;      /* subpool that should have a free buf  */
    int32_t total_waits;            /* num of times thread waited for a buf */
    int32_t total_requests;         /* num requests for a buffer            */
} buf_pool_t;

static const buf_pool_t nil_buf_pool = { 0 };

/*
 * buf_pool_buf_size
 *
 * Returns the buffer size (in terms of bytes) of the buffers
 * that the pool contains.
 */

int32_t
buf_pool_buf_size(
    buf_pool_handle_t buf_pool_h  /* in - handle to buf pool */
    )
{
    buf_pool_t *buf_pool = (buf_pool_t *) buf_pool_h;

    return buf_pool->buf_size;
}

/*
 * buf_pool_create - Creates a buffer pool.  The buffers
 * are maintained in a array and there is a simple 'free list'
 * array used to keep track of allocated and available buffers.
 * This info is maintained on a per-subpool basis.  This routine
 * creates a pool with one subpool.
 *
 * return ERROR or OKAY.
 */

int
buf_pool_create(
   buf_pool_handle_t *buf_pool_h,  /* out - handle to new buf pool */
   int32_t num_bufs,               /* in - number of bufs in pool */
   int32_t buf_size                /* in - bytes per block */
   )
{
    buf_pool_t *buf_pool = NULL;
    sub_pool_t *sub_pool = NULL;
    int i;

    /*----------------------------------------------------------------------*/

    if ((buf_size < MIN_BUF_SZ) || (buf_size > MAX_BUF_SZ)) {
        fprintf( stderr, catgets(catd, S_BUFPOOL_UTIL1, BUFPOOL_UTIL1, 
            "%s: invalid buffer size %d\n"), Prog, buf_size );
        return ERROR;
    }

    num_bufs = MAX( num_bufs, 1 ); /* has to be at least one! */
 
    /* allocate storage for buffer pool and initialize buf pool descriptor */
    buf_pool = (buf_pool_t *) malloc( sizeof( buf_pool_t ) );
    if (buf_pool == NULL) {goto _error;}

    *buf_pool = nil_buf_pool;
    pthread_mutex_init(&buf_pool->mutex, NULL);
    pthread_cond_init(&buf_pool->cv, NULL);
    buf_pool->num_bufs = num_bufs;
    buf_pool->bufs_avail = num_bufs;

    /* allocate storage for a subpool */
    sub_pool = (sub_pool_t *) malloc( sizeof( sub_pool_t ) );
    if (sub_pool == NULL) {goto _error;}

    /* allocate storage for the buffers */
    sub_pool->bufs = malloc( num_bufs * buf_size );
    if (sub_pool->bufs == NULL) {goto _error;}

    /* initialize the free list */
    sub_pool->num_bufs = num_bufs;
    sub_pool->bufs_avail = num_bufs;
    sub_pool->free_buf = 0;

    sub_pool->free_bufs  = (int *) malloc (num_bufs * sizeof( int ) );
    if (sub_pool->free_bufs == NULL) {goto _error;}

    for (i = 0; i < (num_bufs - 1); i++) {
        sub_pool->free_bufs[i] = i + 1;
    }

    sub_pool->free_bufs[num_bufs - 1] = NIL_BUF;

    /* initialize buffer statistics */
    sub_pool->buf_stats = (int *) malloc (num_bufs * sizeof( int ) );
    if (sub_pool->buf_stats == NULL) {goto _error;}

    for (i = 0; i < num_bufs; i++) {
        sub_pool->buf_stats[i] = 0;
    }

    /* setup subpool pointers */
    sub_pool->nxt_sub_pool = sub_pool; /* circular list */
    buf_pool->sub_pool = sub_pool;
    buf_pool->sub_pool_hint = sub_pool;
    buf_pool->buf_size = buf_size;

    *buf_pool_h = (buf_pool_handle_t) buf_pool;

    return OKAY;

_error:

    /*
     * Orderly cleanup...
     */
    if (sub_pool != NULL) {
        if (sub_pool->bufs != NULL) {
            free( sub_pool->bufs );
        }
        if (sub_pool->free_bufs != NULL) {
            free( sub_pool->free_bufs );
        }
        if (sub_pool->buf_stats != NULL) {
            free( sub_pool->buf_stats );
        }

        free( sub_pool );
    }

    if (buf_pool != NULL) {
        free( buf_pool );
    }

    return ERROR;
}

/* end buf_pool_create */

/*
 * buf_pool_delete - Marks buffer pool as deleted.  The storage
 * is not deallocated.
 *
 * TODO:   
 *      1) Need to add code to terminate threads waiting for
 *         a buffer.
 *      2) Need to add a new routine dealloc_buf_pool() to deallocate
 *         the heap space used by the buffer pool.  However, since
 *         both backup and restore delete the pool only when they
 *         are about to terminate, why bother!
 */

void
buf_pool_delete(
    buf_pool_handle_t buf_pool_h  /* in - pool handle */
    )
{
    int i;
    buf_pool_t *buf_pool;
    sub_pool_t *sub_pool;
    
    /*----------------------------------------------------------------------*/

    buf_pool = (buf_pool_t *) buf_pool_h;

    pthread_mutex_lock(&buf_pool->mutex);

    buf_pool->deleted = TRUE;

    pthread_mutex_unlock(&buf_pool->mutex);

    pthread_cond_broadcast(&buf_pool->cv);
      
}

/* end buf_pool_delete */

/*
 * buf_pool_expand 
 * 
 * Expands the buffer pool by 'num_bufs' buffers.  This routine will
 * create a new subpool for the specified pool.  The new buffers
 * are allocated in the new subpool.
 *
 * returns ERROR or OKAY.
 */

int
buf_pool_expand(
    buf_pool_handle_t buf_pool_h,       /* in - buffer pool handle */
    int num_bufs                        /* in - number of buffers to add */
    )
{
    buf_pool_t *buf_pool = NULL;
    sub_pool_t *sub_pool = NULL;
    int i;

    /*----------------------------------------------------------------------*/

    buf_pool = (buf_pool_t *) buf_pool_h;

    num_bufs = MAX( num_bufs, 1 ); /* has to be at least 1 */

    /* allocate storage for new subpool */
    sub_pool = (sub_pool_t *) malloc( sizeof( sub_pool_t ) );
    if (sub_pool == NULL) {goto _error;}

    /* allocate storage for new subpool's buffers */
    sub_pool->bufs = malloc( num_bufs * buf_pool->buf_size );
    if (sub_pool->bufs == NULL) {goto _error;}

    /* initialize the free list */
    sub_pool->num_bufs = num_bufs;
    sub_pool->bufs_avail = num_bufs;
    sub_pool->free_buf = 0;

    sub_pool->free_bufs  = (int *) malloc (num_bufs * sizeof( int ) );
    if (sub_pool->free_bufs == NULL) {goto _error;}

    for (i = 0; i < (num_bufs - 1); i++) {
        sub_pool->free_bufs[i] = i + 1;
    }
    
    sub_pool->free_bufs[num_bufs - 1] = NIL_BUF;

    /* initialize buffer statistics */
    sub_pool->buf_stats = (int *) malloc (num_bufs * sizeof( int ) );
    if (sub_pool->buf_stats == NULL) {goto _error;}

    for (i = 0; i < num_bufs; i++) {
        sub_pool->buf_stats[i] = 0;
    }

    pthread_mutex_lock(&buf_pool->mutex);

    /* add subpool to pool */
    sub_pool->nxt_sub_pool = buf_pool->sub_pool->nxt_sub_pool;
    buf_pool->sub_pool->nxt_sub_pool = sub_pool;

    buf_pool->num_bufs += num_bufs;
    buf_pool->bufs_avail += num_bufs;

    if (sub_pool->bufs_avail > buf_pool->sub_pool_hint->bufs_avail) {
        /* 
         * We try to have the hint point to the 'best' subpool.
         * The best one being the one with the most available buffers.
         */
        buf_pool->sub_pool_hint = sub_pool;
    }

    /*
     * If there are other threads waiting for a buffer, wake them so that
     * they can allocate a buffer
     */
    pthread_mutex_unlock(&buf_pool->mutex );

    pthread_cond_broadcast(&buf_pool->cv);

    return OKAY;

_error:

    /*
     * Orderly cleanup...
     */
    if (sub_pool != NULL) {
        if (sub_pool->bufs != NULL) {
            free( sub_pool->bufs );
        }
        if (sub_pool->free_bufs != NULL) {
            free( sub_pool->free_bufs );
        }
        if (sub_pool->buf_stats != NULL) {
            free( sub_pool->buf_stats );
        }

        free( sub_pool );
    }

    return ERROR;
}

/* end buf_pool_expand */

/*
 * buf_allocate - Allocates a buffer from the buffer pool.  If
 * no buffers are available then the routine blocks the calling
 * thread until a buffer is released by another thread.
 *
 * returns ERROR or OKAY.
 */

int
buf_allocate(
    buf_pool_handle_t buf_pool_h,  /* in - buf pool handle */
    char **buf                     /* out - ptr to allocated buffer */
    )
{
    buf_pool_t *buf_pool;
    sub_pool_t *sub_pool;
    int prev_free_buf;

    /*----------------------------------------------------------------------*/

    buf_pool = (buf_pool_t *) buf_pool_h;

    pthread_mutex_lock(&buf_pool->mutex);

    while (!buf_pool->deleted && buf_pool->bufs_avail == 0) {
        /* 
         * there are no free buffers so we wait until one is released 
         */
        buf_pool->waiters++;
        pthread_cond_wait(&buf_pool->cv, &buf_pool->mutex);
        buf_pool->waiters--;

        buf_pool->total_waits++;
    }

    if (buf_pool->deleted) {
        pthread_mutex_unlock(&buf_pool->mutex);
        return ERROR;
    }

    sub_pool = buf_pool->sub_pool_hint;

    if (sub_pool->bufs_avail == 0) {
        /*
         * The subpool hint is wrong so we search the other subpools
         * for a subpool with a free buffer.
         */
        do {
            sub_pool = sub_pool->nxt_sub_pool;
        } while ((sub_pool->bufs_avail == 0) && 
                 (sub_pool != buf_pool->sub_pool_hint));

        if (sub_pool == buf_pool->sub_pool_hint) {
            /*
             * buf_pool->bufs_avail says there are free bufs yet none of the
             * subpools contain free bufs!
             */
            abort_prog( catgets(catd, S_BUFPOOL_UTIL1, BUFPOOL_UTIL6, "%s: cannot find free buffer."), Prog );
        }

        buf_pool->sub_pool_hint = sub_pool;
    }

    /* 
     * Allocate the buffer at the head of the free list
     */
    *buf = &sub_pool->bufs[sub_pool->free_buf * buf_pool->buf_size];
    sub_pool->buf_stats[sub_pool->free_buf] += 1;

    /*
     * Change free_buf index to next free buffer and update free_bufs list.
     */
    prev_free_buf = sub_pool->free_buf;
    sub_pool->free_buf = sub_pool->free_bufs[sub_pool->free_buf];
    sub_pool->free_bufs[prev_free_buf] = NIL_BUF;
    sub_pool->bufs_avail--;

    buf_pool->bufs_avail--;
    buf_pool->total_requests++;

    pthread_mutex_unlock(&buf_pool->mutex);

    return OKAY;
}

/* end buf_allocate */

/*
 * buf_release - Puts a buffer back on the free buffer list.
 * Wake up any threads waiting for a buffer.
 *
 * returns ERROR or OKAY.
 */

int
buf_release(
    buf_pool_handle_t buf_pool_h,  /* bufpool handle */
    char *buf                      /* in - ptr to buffer */
    )
{
    buf_pool_t *buf_pool;
    sub_pool_t *sub_pool;
    int released_buf_num;

    /*----------------------------------------------------------------------*/

    buf_pool = (buf_pool_t *) buf_pool_h;

    pthread_mutex_lock(&buf_pool->mutex);

    sub_pool = buf_pool->sub_pool_hint;

    /*
     * Search for the subpool that this buffer belongs to.
     */
    do {
        if ((buf >= sub_pool->bufs) && 
            (buf < (sub_pool->bufs+ sub_pool->num_bufs*buf_pool->buf_size))) {
            break;
        }
        sub_pool = sub_pool->nxt_sub_pool;
    } while ((sub_pool != buf_pool->sub_pool_hint));

    if ((buf < sub_pool->bufs) || 
        (buf >= sub_pool->bufs + sub_pool->num_bufs * buf_pool->buf_size)) {
        abort_prog( catgets(catd, S_BUFPOOL_UTIL1, BUFPOOL_UTIL7, 
            "%s: tried to deallocate invalid buffer <0x%08x>."), 
            Prog, buf );
    }

    /* Calculate buffer number of the buffer to be released */
    released_buf_num = (buf - sub_pool->bufs) / buf_pool->buf_size;

    /* Put the buffer at head of the subpool's free list */
    sub_pool->free_bufs[released_buf_num] = sub_pool->free_buf;
    sub_pool->free_buf = released_buf_num;
    sub_pool->bufs_avail++;

    buf_pool->bufs_avail++;

    if (sub_pool->bufs_avail > buf_pool->sub_pool_hint->bufs_avail) {
        /* 
         * We try to have the hint point to the 'best' subpool.
         * The best one being the one with the most available buffers.
         */
        buf_pool->sub_pool_hint = sub_pool;
    }

    /*
     * If there are threads waiting for a buffer, wake them so that
     * they can allocate a buffer
     */

    pthread_mutex_unlock(&buf_pool->mutex);
    pthread_cond_broadcast(&buf_pool->cv);

    return OKAY;
}

/* end buf_release */

/* end bufpool_util.c */
