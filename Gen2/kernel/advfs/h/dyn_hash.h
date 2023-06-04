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

#ifndef _KERN_DYN_HASH_H_
#define _KERN_DYN_HASH_H_

#include <advfs/e_dyn_hash.h>
#include <advfs/ms_generic_locks.h>

/*
 * This is dictates the maximum size of the synamic hash table.
 * Each slot in the index table points to a segment of the dynamic
 * hash table that is twice the size of the segment in the previous
 * slot (except the first two slots). Thus a maximum hash table size
 * of (2**DYN_INDEX_TABLE_SIZE-1) * initial_size.
 */
#define DYN_INDEX_TABLE_SIZE 32

/*
 * This defines the number of message buffers available for sending a
 * message to the dynamic hashtable kernel thread. 
 */
#define DYN_MAX_MSG_Q_ENTRIES 128


#define DYN_THREAD_TIMEOUT 30

int msb(uint64_t in);

extern ADVMTX_DYNHASHTHREAD_T  DynHashThreadLock;

/* The bucket must be at 64 bytes in size because the embedded
 * locks could cause cache line thrashing otherwise. Need to
 * add explicit padding because these are emmbedded as contiguous
 * memory.
 * 
 * The siblingp must be declared as volatile to ensure that
 * the compiler never optimizes (caches) a read of this field
 * inside a loop.
 */

typedef struct dyn_bucket {
    ADVSMP_DYNHASH_BUCKET_T bucket_lock;   /* lock for this bucket          */
    struct dyn_bucket * volatile siblingp; /* bucket holding chain          */
    void              *chainp;             /* first element in bucket       */
    uint32_t           split_count;        /* # of splits for this bucket   */
    uint32_t           insert_count;       /* incremented on each insertion */
    uint64_t           element_count;      /* # of elements in bucket       */
    uint64_t           controlling_bkno;   /* bucket number of controlling  */
					   /* bucket                        */
    uint64_t           padding[11-(sizeof(spin_t)/sizeof(uint64_t))];
                                           /* keep this 128 bytes in size   */
                                           /* to avoid lock pinging in      */
                                           /* the cache                     */
} dyn_bucketT;

/*
 * The hashtable header.
 * Contains an index table that points to the actual buckets of
 * the hashtable. This index does put an upper bound on the number of
 * buckets but this is a rather large upper bound (2**32 + initial size).
 * 
 * Note the hash table size (current_buckets) is volatile so that the
 * compiler does not try to optimize out a read of this field if we
 * are in a loop.
 */

typedef struct dyn_hashtable {
    uint64_t           initial_buckets;        /* Initial size of table     */
    volatile uint64_t  current_buckets;        /* current number of buckets */
    uint64_t           max_chain_length;       /* max. elements in a chain  */
    uint64_t           offset_hash_links;      /* offset within element     */
    uint64_t           (*hash_function) (void *);   /* pointer to hash      */
                                                    /* function             */
    uint32_t           initial_index_shift;         /* index calculation    */
                                                    /* optimization         */
    int                element_to_bucket_ratio;     /* condition for        */
                                                    /* doubling table       */
    uint64_t           split_interval;              /* minimum time between */
                                                    /* doubling table       */
    uint64_t           last_split_time;             /* time in usec of last */
                                                    /* time table doubled   */
    uint32_t           double_pending;              /* don't send message   */
    uint64_t           pad1;
    uint64_t           pad2;
    dyn_bucketT       *index[DYN_INDEX_TABLE_SIZE]; /* array of ptrs to     */
                                                    /* bucket segments      */
} dyn_hashtableT;


/*
 * The message that is sent to the kernel thread when a chain
 * exceeds its maximum length.
 */

typedef struct dyn_hashthread_msg{
    dyn_hashtableT       *hashtable;
    dyn_bucketT          *dh_bucket;
} dyn_hashthread_msgT;

void
dyn_hashthread();


#define DYN_GET_HASHLINKS(_hashtable , _element)                             \
((dyn_hashlinksT *) ((char *) (_element) + (_hashtable)->offset_hash_links))
    
#define DYN_UNGET_HASHLINKS(_hashtable , _hashlinks)                         \
    ((void *) ((char *) (_hashlinks) - (_hashtable)->offset_hash_links))
    
#define DYN_HASH_GET_KEY(_hashtable,_hlinks,_element)     \
    ((uint64_t) ((_hashtable)->hash_function == NULL)?    \
     (((dyn_hashlinks_w_keyT *)(_hlinks))->dh_key):       \
     ((*((_hashtable)->hash_function))(_element))) 

/* Return a pointer to the bucket                                      */
/* h - hashtbable pointer                                              */
/* k - key from hash function  (THIS WILL BE CHANGED AND RETURNED !!!) */
/* s - size of hash table (must be passed not obtained from h)         */
/* bp - where pointer to bucket is returned to caller                  */

#define  DYN_KEY_TO_BUCKET_PTR(_h,_k,_s,_bp)                                             \
{                                                                                        \
long _ti,hb;                                                                             \
_k= (_k) & ((_s)-1);                                                                     \
hb= msb(_k);                                                                             \
_ti = (((long)(hb-(_h)->initial_index_shift) > 0) ? (hb-(_h)->initial_index_shift) : 0); \
_bp = ((dyn_bucketT*) (((char *) ((_h)->index[(_ti)])) +                                 \
        ( ((_k)&((((_h)->initial_buckets)<<((_ti>0)?(_ti-1):_ti)) - 1))*                 \
        (sizeof(dyn_bucketT)))));        \
}

#define DYN_UNLOCK_BUCKET(_bucketp) ((spin_unlock(&(_bucketp)->bucket_lock)))

#ifdef DYN_HASH_DEBUG
        
struct {
    
/* The following are caller statistics */
    
    uint64_t size_miss;         /* size changed in DYN_LOCK_BUCKET */
    uint64_t sbptr_miss;        /* sibling pointer changed in DYN_LOCK_BUCKET*/
    uint64_t longest_chain;      /* longest chain length realized */
    uint64_t element_inserted;  /* dyn_hash_insert was called*/
    uint64_t message_sent;      /* message sent to kernel thread */
    uint64_t unable_to_send_msg;/* Couldn't get a free message */
    uint64_t element_removed;   /* dyn_hash_remove was called */
    uint64_t double_no_mem;     /* no memory to double table */
    
/* The following are kernel thread statistics */
    
    uint64_t message_received;          /* kernel thread received a message */
    uint64_t chain_already_split;       /*dropped messages because ... */
    uint64_t chain_dropped_below_thold; /*dropped message because ... */
    uint64_t split_interval_guard;      /*dropped message because ... */
    uint64_t max_table_size;            /*dropped message because ... */
    uint64_t ratio_guard;               /*dropped message because ... */
    uint64_t chain_split;               /*thread split a chain */
    uint64_t largest_missed_splits;     /* largest split count difference */
    uint64_t table_doubled;             /* number of table doubles */
    uint64_t walked_chains;
    uint64_t setup_timeout;
    uint64_t timeout_expired;
}dyn_hash_stats;

#endif

#endif  /* _KERN_DYN_HASH_H_ */

