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
 */

#include <ms_public.h>
#include <ms_privates.h>
#include <ms_assert.h>
#include <sys/vm_arena_iface.h>

#define ADVFS_MODULE MS_MODE

extern kmem_handle_t advfs_misc_arena;

#if defined(ADVFS_SMP_ASSERT)
    /* Enable ms_malloc()/ms_free() debugging.
     * But this is only valid if AdvFS Asserts
     * are also enabled.  That helps to prevent
     * accidentially leaving the debugging
     * enabled since we try very hard to disable
     * ADVFS_SMP_ASSERT before shipping the
     * product.
     */
    #define ADVFS_MS_MALLOC_DEBUG 
#endif /* ADVFS_SMP_ASSERT */

#if defined(ADVFS_MS_MALLOC_DEBUG) && defined(ADVFS_SMP_ASSERT)
/*
 * When ADVFS_MS_MALLOC_DEBUG is defined, place safety zones around the
 * AdvFS magic and allocation size fields.  We fill them in during
 * advfs_ms_malloc() and check them during advfs_ms_free().
 */
struct msHdr {
    uint32_t safetyZone1;
    uint32_t magic;
    uint32_t safetyZone2;
    uint32_t size;
    uint64_t safetyZone3;



    /* Memory we allocate from the arena is always 16-byte aligned (see
     * Arena Allocator ERS). The field below is padding to make the header
     * 16 bytes. This is necessary to make the memory returned to the caller
     * 16 byte aligned, which is needed by spinlocks on PA (at least there
     * is a check for 16 byte alignment on PA). 
     */
    uint64_t safetyZone4; /* Padding to 16 byte multiple (for pa-risc locks) */
};
#else /* ADVFS_MS_MALLOC_DEBUG && ADVFS_SMP_ASSERT */
struct msHdr {
    uint32_t magic;
    uint32_t size;

    /* Memory we allocate from the arena is always 16-byte aligned (see
     * Arena Allocator ERS). The field below is padding to make the header
     * 16 bytes. This is necessary to make the memory returned to the caller
     * 16 byte aligned, which is needed by spinlocks on PA (at least there
     * is a check for 16 byte alignment on PA). 
     */
    uint64_t msHdrPadding1;         /* Padding structure out to 16 bytes */
};
#endif /* ADVFS_MS_MALLOC_DEBUG && ADVFS_SMP_ASSERT */

#if defined(ADVFS_MS_MALLOC_DEBUG) && defined(ADVFS_SMP_ASSERT)
/*
 * Allocate a structure for keeping lots of AdvFS
 * advfs_ms_malloc()/advfs_ms_free()
 * stats that can be dumped in the debugger to see where AdvFS is using
 * memory and if the allocates match the frees.  
 *
 * Assuming granularity of 8
 *     ms_malloc_stats.buckets[0] counts  0 -  7 byte sized allocations
 *     ms_malloc_stats.buckets[1] counts  8 - 15 byte sized allocations
 *     ms_malloc_stats.buckets[2] counts 16 - 23 byte sized allocations
 *     etc...
 *
 * ms_malloc_stats.line[row][column] should be a sparsely filled 2
 * dimensional array that keeps track of the source line number from where
 * the ms_malloc() call was made.  The first index is based on the size of
 * the malloc.  The 2nd index is based on the ms_malloc() source code line
 * number.  When ms_malloc_stats.line is dumped by a debugger it will be
 * dumped row by row, so the first row will be the counts of ms_malloc()
 * source lines that requested 0-7 byte sized allocations, the second row
 * will be counts of ms_malloc() source lines that requested 8-15 byte sized
 * allocations, etc...
 *
 * If you just want to see the lines for a given sized allocation, then just
 * display the specific row that corresponds to that size:
 *
 *     (debugger)   print ms_malloc_stats.line[5]
 *     $11 = {
 *       0 <repeats 2965 times>,
 *       2957660,
 *       0 <repeats 143 times>,
 *       93750,
 *       0 <repeats 2890 times>
 *     }
 *
 * This would say that for the 40-47 byte allocation size, there were 2 call
 * locations.  The first was from line 2965 (2,957,660 calls from this
 * location), and the second was from line 2965+1+143 == 3109 (93,750 calls
 * from this location).  NOTE:  If a given ms_malloc() allocates a variable
 * sized value, its line number could show up in more than one line[row]
 * (for example the allocation of msgQ entries).
 *
 * Finding the lines just requires using egrep -n to search all the AdvFS
 * source files looking for ms_malloc or ms_rad_malloc.  For example:
 *
 *     cd ./src/kern/common/fs/advfs/
 *     egrep -n "ms_malloc|ms_rad_malloc" bs/* fs/* osf/* | egrep "2965|3109"
 *
 * That should quickly locate the potential ms_malloc() calls.  In a few
 * cases there may be 2 ms_malloc() calls that have the same line number in
 * different files.  It should be very easy to tell which one you are
 * interested in based on the size of the allocation each call makes.  In
 * this example you are only looking for calls that request 40-47 bytes
 * of memory.
 *
 */
#define MALLOC_GRANULARITY_TRACKED (8)
#define MALLOC_MAX_SIZE_TRACKED (1024)  /* even multiple of granularity */
#define MALLOC_BUCKETS (MALLOC_MAX_SIZE_TRACKED/MALLOC_GRANULARITY_TRACKED)+1
#define MALLOC_LINE_MAX (6000)
typedef struct {
    int64_t malloc_cnt;                 /* times ms_malloc() is called */
    int64_t free___cnt;                 /* times ms_free() is called */
    int64_t malloc_cumulative_size;     /* total bytes allocated */
    int64_t free___cumulative_size;     /* total bytes freed */
    int64_t size_too_big;               /* size bigger than tracked size */
    int64_t size_too_big_cumulative;
    int64_t line_too_big;               /* line # larger than tracked lines */
    int64_t buckets[MALLOC_BUCKETS];    /* inc on malloc, decrement on free */
    int64_t cumulative[MALLOC_BUCKETS]; /* cumulative requests per bucket */
    int64_t line[MALLOC_BUCKETS][MALLOC_LINE_MAX];  /* tracks malloc location */
    int64_t size_too_big_line[MALLOC_LINE_MAX];  /* malloc loc for big sizes */
} ms_malloc_stats_t;
ms_malloc_stats_t ms_malloc_stats;      /* use debugger to print this global */
#endif /* ADVFS_MS_MALLOC_DEBUG && ADVFS_SMP_ASSERT */
char *
advfs_ms_malloc(
    size_t size,         /* in */
    int ln,              /* in */
    char *fn,            /* in */
    int flag,            /* in */
    int bzero_flag,	 /* in */
    int rad_id)          /* in */
{
    struct msHdr *hdr;
    size_t amt = size + sizeof( struct msHdr );

    MS_SMP_ASSERT(size != 0);

    hdr = kmem_arena_varalloc(advfs_misc_arena, amt, flag);
    if( hdr == (struct msHdr *)NULL ) {
        return( (char *)NULL );
    }
    if( bzero_flag ) {
	bzero( (char *)hdr, amt );
    }
#ifdef ADVFS_SMP_ASSERT
    else {
        /* Set poison values to help spot callers failing to init the memory*/
        if ( AdvfsEnableAsserts ) {
            char *tmp; 
            size_t i;

            tmp = (char *)hdr;
            for (i=0; i < amt; i++, tmp++)
                *tmp = (char)0xAA;
        }
    }
#endif /* ADVFS_SMP_ASSERT */

    hdr->magic = ALLOC_MAGIC;
    hdr->size = (uint32_t) amt;

#   if defined(ADVFS_MS_MALLOC_DEBUG) && defined(ADVFS_SMP_ASSERT)
    {
    /*
     * Record allocation stats
     */
        uint32_t bucket;
        uint64_t dummy;

        if ( AdvfsEnableAsserts ) {
            MS_SMP_ASSERT( (int)size > 0 );    /* accidential neg value */

            bucket = ((uint32_t) size)/MALLOC_GRANULARITY_TRACKED;

            hdr->safetyZone1 = 0xa5a5a5a5L;
            hdr->safetyZone2 = 0x5a5a5a5aL;
            hdr->safetyZone3 = 0xa5a5a5a5a5a5a5a5LL;
            hdr->safetyZone4 = 0x5a5a5a5a5a5a5a5aLL;

            ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.malloc_cnt,&dummy);
            ADVFS_ATOMIC_FETCH_ADD(&ms_malloc_stats.malloc_cumulative_size,
                                   size,
                                   &dummy);

            if ( ln < MALLOC_LINE_MAX ) {
                if ( bucket < MALLOC_BUCKETS ) {
                    ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.line[bucket][ln],
                                            &dummy);
                }
                else {
                    ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.size_too_big_line[ln],
                                            &dummy);
                }
            }
            else {
                ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.line_too_big,
                                        &dummy);
            }

            if ( bucket < MALLOC_BUCKETS ) {
                ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.buckets[bucket],
                                        &dummy);
                ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.cumulative[bucket],
                                        &dummy);
            }
            else {
                ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.size_too_big,
                                        &dummy);
                ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.size_too_big_cumulative,
                                        &dummy);
            }
        }
    }
#   endif /* ADVFS_MS_MALLOC_DEBUG && ADVFS_SMP_ASSERT */

    return( (char *)hdr + sizeof( struct msHdr ) );
}

void
advfs_ms_free(
    void *ptr,           /* in */
    int ln,              /* in */
    char *fn)            /* in */
{
    struct msHdr *hdr;

    if( ptr == 0 ) {
        return;
    }
    hdr = (struct msHdr *)((char *)ptr - sizeof( struct msHdr ));
    MS_SMP_ASSERT( hdr->magic == ALLOC_MAGIC );
    hdr->magic |= MAGIC_DEALLOC;

#   if defined(ADVFS_MS_MALLOC_DEBUG) && defined(ADVFS_SMP_ASSERT)
    /*
     * Record free stats
     */
    {
        uint32_t size;
        uint32_t bucket;
        uint64_t dummy;

        if ( AdvfsEnableAsserts ) {

            size = hdr->size - sizeof(struct msHdr);
            bucket = size/MALLOC_GRANULARITY_TRACKED;

            MS_SMP_ASSERT( hdr->safetyZone1 == 0xa5a5a5a5L
            &&             hdr->safetyZone2 == 0x5a5a5a5aL
            &&             hdr->safetyZone3 == 0xa5a5a5a5a5a5a5a5LL
            &&             hdr->safetyZone4 == 0x5a5a5a5a5a5a5a5aLL );
            MS_SMP_ASSERT( (int)size > 0 );    /* accidential neg value */

            ADVFS_ATOMIC_FETCH_INCR(&ms_malloc_stats.free___cnt,&dummy);
            ADVFS_ATOMIC_FETCH_ADD(&ms_malloc_stats.free___cumulative_size,
                              size,
                             &dummy);
            if ( bucket < MALLOC_BUCKETS ) {
                ADVFS_ATOMIC_FETCH_DECR(&ms_malloc_stats.buckets[bucket],&dummy);
            }
            else {
                ADVFS_ATOMIC_FETCH_DECR(&ms_malloc_stats.size_too_big,&dummy);
            }
        }
    }
#   endif /* ADVFS_MS_MALLOC_DEBUG && ADVFS_SMP_ASSERT */

    kmem_arena_free(hdr, M_WAITOK);
}


int
ms_copyin( void *src, void *dest, int len )
{
    return copyin( src, dest, len );
}


int
ms_copyout( void *src, void *dest, int len )
{
    return copyout( src, dest, len );
}

