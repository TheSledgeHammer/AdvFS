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

#include <sys/proc_iface.h>
#include <sys/kdaemon_thread.h>
#include <sys/types.h>
#include <sys/kernel.h>		/* kernel global lbolt */

#include <ms_public.h>
#include <bs_public.h>
#include <dyn_hash.h>
#include <bs_msg_queue.h>
#include <ms_assert.h>

#define TRUE  1
#define FALSE 0

/*
 * The following global variables used by the dynamic hash table
 * routines.
 */

/*
 * Lock used to insure only a single kernel thread
 * is created for all dynamic hash tables. 
 */

static int32_t   DynHashThreadStarted = FALSE;
ADVMTX_DYNHASHTHREAD_T   DynHashThreadLock;

msgQHT dyn_hashthread_msgQH;

void
dyn_hashthread_alarm(dyn_hashtableT* hashtable);

static
void 
dyn_init_hashthread();

int
msb(uint64_t in);


static
void
dyn_split_chain(
        dyn_hashtableT *hashtable,
        dyn_bucketT    *bucketp);


static
int
dyn_double_hashtable(
                     dyn_hashtableT * hashtable);


/*
 * This sets up a lock for starting the dynamic hash table thread.
 * It avoids to subsystems racing to create the daemon. This will
 * be more of an issues if configurable subsystems are ever
 * introduced
 */

void
dyn_kernel_thread_setup()
{
   DynHashThreadStarted = FALSE;
}

/*  
 * This initializes the dynamic hashtable. The passed in arguments should
 * be carefully considered. Especially element_to_bucket_ratio and 
 * bucket_length_threshold. 
 */

void *
dyn_hashtable_init( uint64_t initial_bucket_count,
                    uint64_t bucket_length_threshold,
                    int      element_to_bucket_ratio,
                    uint64_t split_interval,
                    uint64_t offset_to_hashlinks,
                    uint64_t (*hash_function) ()
    )
{
    dyn_hashtableT *tablep;
    uint64_t bucket,RoundUp2,shift;
    dyn_bucketT *bsegment;
    ADVMTX_DYNHASHTHREAD_LOCK(&DynHashThreadLock);
    if ( !DynHashThreadStarted )
    {
        DynHashThreadStarted=TRUE;
        dyn_init_hashthread();
    }
    ADVMTX_DYNHASHTHREAD_UNLOCK(&DynHashThreadLock);
    tablep =(dyn_hashtableT *) ms_malloc( sizeof(dyn_hashtableT));
    /* Round up to next power of 2 */
    for(RoundUp2=1,shift=0;
        RoundUp2<initial_bucket_count;
        RoundUp2<<=1,shift++)    /*void*/;
    
    initial_bucket_count=RoundUp2;
    
    tablep->index[0] =(dyn_bucketT *) ms_malloc(sizeof(dyn_bucketT)*initial_bucket_count);
    
    /* Set up the Hashtable itself */
    tablep->initial_buckets     = initial_bucket_count;
    tablep->initial_index_shift = shift-1;;
    tablep->current_buckets     = initial_bucket_count;
    tablep->max_chain_length    = bucket_length_threshold;
    tablep->offset_hash_links   = offset_to_hashlinks;
    tablep->hash_function       = hash_function;
    tablep->element_to_bucket_ratio = element_to_bucket_ratio;
    tablep->split_interval      = split_interval * HZ / 1000000; 
    /* Convert usec to ticks */
    tablep->last_split_time     = 0;
    tablep->double_pending=FALSE;
    
    /* Initialize each bucket in the bucket segment */
    bsegment=tablep->index[0];
    
    for(bucket=0;bucket<initial_bucket_count;bucket++)
    {
        ADVSMP_DYNHASH_BUCKETLOCK_INIT(&bsegment[bucket].bucket_lock);

        bsegment[bucket].split_count=0;
        bsegment[bucket].element_count = 0;
        bsegment[bucket].insert_count = 0;
        bsegment[bucket].chainp = (dyn_hashlinksT *) NULL;
        bsegment[bucket].siblingp = &bsegment[bucket];
        bsegment[bucket].controlling_bkno = bucket;
    }
    return ((void *)tablep);
}   


/* This is the function to remove a dynamic hashtable. This is currently
 * untested. 
 */

void
dyn_hashtable_destroy( void *hashtable)
{
    uint64_t segment,segment_size;
    uint64_t buckets;
    dyn_hashtableT *hashtablep;
    dyn_bucketT *segmentp;
    panic("Untested CODE");
    hashtablep = (dyn_hashtableT *) hashtable;
    segment_size=hashtablep->initial_buckets;
    /* Release the memory associated with each bucket segment */
    for (segment=0;
         segment <= (msb(hashtablep->current_buckets) -
                     hashtablep->initial_index_shift);
         segment ++)
    {
        if (segment > 1) segment_size += segment_size;
        segmentp = hashtablep->index[segment];
        for (buckets = 0;buckets<segment_size;buckets++)
        {
            ADVSMP_DYNHASH_BUCKETLOCK_DESTROY(&segmentp[buckets].bucket_lock);
        }
        ms_free(segmentp);
    }
    /* release the hash table itself */
    ms_free(hashtablep);
    
    /* The kernel thread should probably be stopped if there are */
    /* no more dynamic hashtables (this would require a count in the */
    /* dyn_hashthread_startup structure. */
}

/* 
 * This is an internal routine and should only be called in this file.
 *
 * Lock the bucket and return the pointer to the controlling bucket
 * (ie the siblingp).  After we lock the bucket we must check that the
 * hash table size did not change.  If the table doubled and the
 * bucket we were trying to lock split on us we might miss the element
 * (it may have been moved to a differnent chain), thus we must try
 * again with the new size.  
 *
 * If the size didn't change but the sibling pointer did then the
 * bucket was split out from under us while we were waiting for the
 * lock. This means the lock we obtained is not for the bucket that
 * contains our element. We must unlock and relock. 
 */

static dyn_bucketT *
dyn_lock_bucket (dyn_hashtableT* hashtable, 
                 uint64_t key)
{
    uint64_t size,tmp_key;
    dyn_bucketT *controlling_bptr,*bptr;
    while(TRUE)
    {
        /* It is important that the following statment (or any of the 
         * references to hashtable->current_buckets) not be optimized
         * by the compiler. The field has been declared volatile and must
         * be re-read each time through this loop since it may change
         * out from under us.
         */

        /* Need to refetch the key each time through this loop
         * since DYN_KEY_TO_BUCKET_PTR changes it. A change in
         * hash table size will send us back through the loop and 
         * we know have 1 more bit of the key we need to examine !
         */

        tmp_key=key;
        
        size = hashtable->current_buckets;
        DYN_KEY_TO_BUCKET_PTR(hashtable,tmp_key,size,bptr);
        controlling_bptr= bptr->siblingp;
        MS_SMP_ASSERT(controlling_bptr->siblingp == controlling_bptr);
        while(TRUE)
        {
            ADVSMP_DYNHASH_BUCKETLOCK_LOCK(&controlling_bptr->bucket_lock);
            if (size != hashtable->current_buckets)
            {
#               ifdef DYN_HASH_DEBUG
                    dyn_hash_stats.size_miss++;
#               endif
                DYN_UNLOCK_BUCKET(controlling_bptr);
                break;
            }
            if (bptr->siblingp != controlling_bptr)
            {
#               ifdef DYN_HASH_DEBUG
                    dyn_hash_stats.sbptr_miss++;
#               endif
                /* The bucket was split while we were waiting for the lock.
                   Unlock the bucket and get the siblingp again (it is now
                   a self pointer) and lock the new controlling bucket.
                   */
                DYN_UNLOCK_BUCKET(controlling_bptr);
                controlling_bptr = bptr->siblingp;
            }
            else
            {
                return(controlling_bptr);
            }
        }
    }
    /* NOTREACHED */
}

/*
 *  Returns to the caller a pointer to the hash chain that is associated 
 *  with the passed in key. The returned pointer points to the top of the
 * structure being hashed and should be type cast accordingly. The chains
 * are maintained in a circular list and the order elements are placed on
 * the list will not be altered by these routines.
 *
 * The insert count is incremented for each insertion to this chain (it
 * is never decremented).
 */

void *
dyn_hash_obtain_chain( void *hashtable,
                       uint64_t key,
                       uint64_t *insert_count)
{
    dyn_bucketT *bptr;
    bptr = dyn_lock_bucket((dyn_hashtableT *)hashtable,key);
    if (insert_count != NULL) *insert_count = bptr->insert_count;
    return(bptr->chainp);
}



/*
 * This will unlock the bucket associated with the passed in key.
 * It is the counterpart to dyn_hash_obtain_chain.
 */

void 
dyn_hash_release_chain( void * hashtable,
                        uint64_t key)
{
    dyn_bucketT *bptr;
    DYN_KEY_TO_BUCKET_PTR((dyn_hashtableT *)hashtable,key,
                          ((dyn_hashtableT *)hashtable)->current_buckets,bptr);
    DYN_UNLOCK_BUCKET(bptr->siblingp);
}


/*
 * Routine to insert an element in to the hash table. The element's
 * hash value (key) will be obtained from the element and then the
 * element will be placed on the corresponding hash chain at the head of
 * the chain. Even though the cahin can split the relative ordering amongst
 * elements will not change.
 *
 * An insertion may cause the chain to exceed its maximum length in which
 * case a message will be sent to the dynamic hash table daemon requesting
 * that the cahin be split (this may cause the table to double).
 *
 * If obtain lock is FALSE than it is assumed the caller already holds the
 * hash chain's lock. This routine will return with the hash chain unlocked.
 */

void 
dyn_hash_insert( void *hashtable,
                 void * elementp,
                 int obtain_lock)
    
{
    dyn_hashlinksT *hash_links,*head_links,*tail_links;
    dyn_bucketT *controlling_bptr;
    uint64_t key;
    dyn_hashthread_msgT *msgp;
    
    hash_links = DYN_GET_HASHLINKS((dyn_hashtableT *)hashtable,elementp);
    key = DYN_HASH_GET_KEY((dyn_hashtableT *)hashtable,hash_links,elementp);
    if (obtain_lock)
    {
        controlling_bptr=dyn_lock_bucket((dyn_hashtableT *)hashtable,key);
    }
    else
    {
        DYN_KEY_TO_BUCKET_PTR((dyn_hashtableT *)hashtable,key,
                              ((dyn_hashtableT *)hashtable)->current_buckets,
                              controlling_bptr);
        controlling_bptr=controlling_bptr->siblingp;
        MS_SMP_ASSERT(ADVSMP_DYNHASH_BUCKETLOCK_OWNED(&controlling_bptr->bucket_lock));
    }
    if (controlling_bptr->chainp == NULL)
    {
        hash_links->dh_next = hash_links->dh_prev = elementp;
    }
    else
    {
        /* Maintain a circular list amongst the chain (not the bucket) */
        head_links = DYN_GET_HASHLINKS((dyn_hashtableT *)hashtable,controlling_bptr->chainp);
        tail_links = DYN_GET_HASHLINKS((dyn_hashtableT *)hashtable,head_links->dh_prev);
        tail_links->dh_next = elementp;
        hash_links->dh_prev = head_links->dh_prev;
        head_links->dh_prev = elementp;
        hash_links->dh_next = controlling_bptr->chainp;
    }
    controlling_bptr->chainp = elementp;
    controlling_bptr->element_count++;
#   ifdef DYN_HASH_DEBUG
        if (controlling_bptr->element_count >dyn_hash_stats.longest_chain)
            dyn_hash_stats.longest_chain = controlling_bptr->element_count;
#   endif
    controlling_bptr->insert_count++;

#   ifdef DYN_HASH_DEBUG
        dyn_hash_stats.element_inserted++;
#   endif

    /*
     * Unlock the bucket. Can't hold lock across      
     * potential call to dyn_msg_alloc and we don't  
     * need it any more anyway.                       
     */
     DYN_UNLOCK_BUCKET(controlling_bptr);
    if (controlling_bptr->element_count > 
        (((dyn_hashtableT*) hashtable)->max_chain_length))
    {
        /*
         *
         * The chain is too long. Send a message to the 
         * dynamic hashthread to split this chain. If no
         * messages are available then just return. The 
         * next insertion to this chain will attempt to 
         * get a message. Eventually we'll get through. 
         *                                              
         * NOTE: the key is now actually the bucket     
         * number. The above macros (DYN_KEY_TO_BUCKET_PTR)
         * changed it.         
         *                                                
         */
        
        if (((dyn_hashtableT*)hashtable)->double_pending)
        {
            long table_double_count;

            /* Calculate the number of times the hash table has doubled 
             * by finding the number of shifts (same as multiplying by 2)
             * since its initial size.
             */

            table_double_count = msb(((dyn_hashtableT*)hashtable)->current_buckets) - 
                ((dyn_hashtableT*)hashtable)->initial_index_shift - 1;

            /* If the number of table doubles equals the number of bucket splits
             * then the bucket can not split any more and a table doubling will
             * be required. But there already is an outstanding request to
             * double the table so we'll skip sending the message
             */

            if (controlling_bptr->split_count == table_double_count)
            {
                /* Do not send a message. The thread is already
                 * aware.
                 */
                return;
            }
        }

        msgp = msgq_alloc_msg( dyn_hashthread_msgQH);
        if (msgp != NULL)
        {
            msgp->hashtable     = (dyn_hashtableT*) hashtable;
            msgp->dh_bucket        = controlling_bptr;
            msgq_send_msg(dyn_hashthread_msgQH, msgp);
#           ifdef DYN_HASH_DEBUG
            dyn_hash_stats.message_sent++;
        }
        else
        {
            dyn_hash_stats.unable_to_send_msg++;
#           endif
            
        }
    }
}

/*
 * Removes an element from the chain it is on. This routine will find
 * the chain corresponding to the elements hash key and remove the 
 * element from that chain. It is assumed that the element is currently
 * on a chain.
 * 
 * If obtain lock is FALSE then the caller must have lock the hash chain
 * via dyn_hash_obtain_chain. This routine returns with the hash chain
 * UNLOCKED.
 */

void 
dyn_hash_remove( void * hashtable,
                 void * element,
                 int obtain_lock)
{
    dyn_hashlinksT *hash_links,*next_links, *prev_links;
    dyn_bucketT *bptr;
    uint64_t key;
    
    hash_links = DYN_GET_HASHLINKS((dyn_hashtableT *)hashtable,element);
    key = DYN_HASH_GET_KEY((dyn_hashtableT *)hashtable,hash_links,element);
    if (obtain_lock)
    {
        bptr=dyn_lock_bucket((dyn_hashtableT *)hashtable,key);
    }
    else
    {
        DYN_KEY_TO_BUCKET_PTR((dyn_hashtableT *)hashtable,
                              key,
                              ((dyn_hashtableT *) hashtable)->current_buckets,
                              bptr);
        bptr=bptr->siblingp;
        MS_SMP_ASSERT(ADVSMP_DYNHASH_BUCKETLOCK_OWNED(&bptr->bucket_lock));
    }
    
    next_links = (dyn_hashlinksT *) 
        DYN_GET_HASHLINKS((dyn_hashtableT *)hashtable,hash_links->dh_next);
    prev_links = (dyn_hashlinksT *) 
        DYN_GET_HASHLINKS((dyn_hashtableT *)hashtable,hash_links->dh_prev);                            
    /* No need to special case - just a nop if only one on list */
    
    next_links->dh_prev = hash_links->dh_prev;
    prev_links->dh_next = hash_links->dh_next;
    
    if (bptr->chainp == element)
    {
        if (hash_links->dh_next == element) /* Only one on list */
        {
            bptr->chainp = NULL;
        }
        else
        {
            bptr->chainp = hash_links->dh_next;
        }
    }
    bptr->element_count--;
    hash_links->dh_next = hash_links->dh_prev = NULL;
    MS_SMP_ASSERT((long)bptr->element_count >= 0);
#   ifdef DYN_HASH_DEBUG
        dyn_hash_stats.element_removed++;
#   endif
    DYN_UNLOCK_BUCKET(bptr);
}

/*
 * Sets up the message queue and starts the initial hash thread.
 * Currently uses advfs message queueing utilities. These utilities
 * should eventually be moved to the kern subsystem.
 */

static
void
dyn_init_hashthread()
{
    extern proc_t *Advfsd;
    statusT sts;
    tid_t tid;

    sts = msgq_create( &dyn_hashthread_msgQH,       /* returned handle to queue */
                       DYN_MAX_MSG_Q_ENTRIES,       /* max message in queue */
                       sizeof(dyn_hashthread_msgT), /* max size of each message */
                       TRUE,                        /* allow q size growth */
                       0);                          /* rad Id */
    if (sts != ESUCCESS) {
        panic( "dyn_hashthread: can't create msq queue");
    }

    if (kdaemon_thread_create((void (*)(void *))dyn_hashthread,
                              NULL,
                              Advfsd,
                              &tid,
                              KDAEMON_THREAD_CREATE_DETACHED)) {
        panic( "dyn_init_hashthread_thread: can't create dyn_hashthread thread" );
    }
}

/* Because there is only ONE thread per hashtable (and currently for
 * all hashtables) we can assume that the fields in the hashtable
 * itself are stable.  (ie. if there are two messages to double the
 * table from the same chain then the table split count will reflect
 * the table has already doubled)
 *
 * This loops forever checking for messages. The thread will either
 *
 * 1) Drop the message.
 * 2) Split a chain.
 * 3) Double the hash table then split a chain.
 *
 * Because of the asynchronous nature of this thread a number
 * of conditions can occur before a message is processed.
 * A message will be dropped if any of the following are true:
 * 
 * 1) The chain's length is already below the maximium.
 * 2) The time interval has not been exceeded
 * 3) The ratio of elements to buckets is not met.
 * 4) No memory is available to double the table
 * 5) The largest table size is reached (2**DYN_INDEX_TABLE_SIZE-1) * initial_size.
 */

void
dyn_hashthread()
{
    int i,j;
    dyn_bucketT *bucket_segment;
    dyn_hashtableT* hashtable;
    dyn_hashthread_msgT *msg;
    uint64_t total_element_count;
    uint64_t segment_size;
    long table_split_count;

    while(TRUE)
    {
        msg = (dyn_hashthread_msgT *) msgq_recv_msg( dyn_hashthread_msgQH );
#       ifdef DYN_HASH_DEBUG
            dyn_hash_stats.message_received++;
#       endif
        hashtable = msg->hashtable;
        table_split_count = msb(hashtable->current_buckets) - 
            hashtable->initial_index_shift - 1;
        MS_SMP_ASSERT(msg->dh_bucket->split_count <= msg->dh_bucket->split_count);
        MS_SMP_ASSERT(msg->dh_bucket == msg->dh_bucket->siblingp);
        if (msg->dh_bucket->element_count <= hashtable->max_chain_length)
        { /* Make sure a deletion hasn't brought us below the threshold 
             or a split was already done */
#           ifdef DYN_HASH_DEBUG
                dyn_hash_stats.chain_dropped_below_thold++;
#           endif
            goto freemsg;
        }
        if (msg->dh_bucket->split_count < table_split_count)
        {
            /* We have siblings so just split the chain amongst the siblings.*/
            dyn_split_chain(hashtable,msg->dh_bucket);
            goto freemsg;
        }
        /* First lets make sure we haven't violated any of the safeguards */
        if ((((uint64_t)lbolt - hashtable->last_split_time) <= 
             hashtable->split_interval) ||
            (table_split_count >= (DYN_INDEX_TABLE_SIZE -1) ))
        {
#           ifdef DYN_HASH_DEBUG
                if (((uint64_t)lbolt - hashtable->last_split_time) <= 
                    hashtable->split_interval) 
                    dyn_hash_stats.split_interval_guard++;
                else
                    dyn_hash_stats.max_table_size++;
#           endif
            goto freemsg;
         }
        /* If the double_pending flag is set then a request got in
         * before we set this. We failed the element to bucket ratio
         * test and can not double table. We have set a timeout that
         * will clear this flag. Until then draop any requests on the
         * floor.
         */
        if (!hashtable->double_pending)
        {            
            hashtable->double_pending=TRUE;
            segment_size=hashtable->initial_buckets;
            total_element_count=0;

            /* Add up all the elements counts in each bucket 
             * Since there is no global hash table lock we can not
             * keep a count because it would drift. We are only looking for
             * a ballpark figure here
             */

            for(i=0;
                i<=table_split_count;
                i++)
            {
                bucket_segment = hashtable->index[i];
                if (i > 1) segment_size<<=1;
                for(j=0;j<segment_size;j++)
                {
                    /* Only count controlling buckets */
                    if(bucket_segment[j].siblingp == &bucket_segment[j])
                        total_element_count+=bucket_segment[j].element_count;
                }
            }
        
#           ifdef DYN_HASH_DEBUG
                dyn_hash_stats.walked_chains++;
#           endif
            if ((((hashtable->element_to_bucket_ratio >= 0) && 
                  ((total_element_count/hashtable->current_buckets) <
                   hashtable->element_to_bucket_ratio)))
                ||
                ((hashtable->element_to_bucket_ratio < 0) &&
                 ((hashtable->current_buckets/total_element_count)>=
                  -(hashtable->element_to_bucket_ratio))))
            {
#               ifdef DYN_HASH_DEBUG
                    dyn_hash_stats.ratio_guard++;
#               endif

                /* 
                 * When hash tables get large this ratio may cause
                 * requests to fail too often. Set a flag indicating
                 * that no more doubling request should be sent. Set a
                 * 30 second timeout.
                 */

#               ifdef DYN_HASH_DEBUG
                    dyn_hash_stats.setup_timeout++;
#               endif
                hashtable->double_pending=TRUE;

                timeout((void(*))dyn_hashthread_alarm,hashtable,
                        DYN_THREAD_TIMEOUT*HZ);
                goto freemsg;
            }
            if (dyn_double_hashtable(hashtable) == ENOMEM)
            {
#               ifdef DYN_HASH_DEBUG
                    dyn_hash_stats.double_no_mem++;       
#               endif
                hashtable->double_pending=FALSE;
                goto freemsg;
            }
            hashtable->double_pending=FALSE;
            dyn_split_chain(hashtable,msg->dh_bucket);
        }

    freemsg:       
        msgq_free_msg( dyn_hashthread_msgQH, msg);
    }
}

void
dyn_hashthread_alarm(dyn_hashtableT* hashtable)
{
#   ifdef DYN_HASH_DEBUG
        dyn_hash_stats.timeout_expired++;
#   endif
    hashtable->double_pending=FALSE;
}

/*
 * This routine will split a chain. Each time the hash table is
 * doubled in size, each bucket in the new half of the table will point
 * to its corresponding index in the original half (or parent
 * bucket). When the parent bucket reaches the chain maximium, the chain
 * will be split up amongst the sibling buckets and each sibling bucket
 * will now become a parent (thier pointers will be made into self
 * pointers). 
 *
 * The sibling buckets are located by comparing the split count of the table
 * with the split count of the parent bucket. A difference of 1 means 1 sibling,
 * 2 means 3 siblings, 3 means 7 siblings, etc...
 * These siblings will be one table size apart, where the table size is the size
 * of the hastable when the split difference was zero.
 *
 * The buckets will be setup and the chain rehashed amongst all the
 * new buckets.  the last thing the most be done is setting in the new
 * self pointers. A memory barrier instrustion must be issued to
 * insure that all bucket set is complete. Once the self pointer is
 * stored the bucket becomes visible.
 *
 */
static
void
dyn_split_chain( dyn_hashtableT * hashtable,
                 dyn_bucketT * bucketp
    )
    
{
    uint64_t missed_splits, bucket_increment, siblings, bucket_number;
    uint64_t i,key,elements,size,split_count;
    dyn_bucketT *sbptr;
    dyn_hashlinksT *hash_links,*next_links,*head_links,*tail_links;
    void *elementp;
    ADVSMP_DYNHASH_BUCKETLOCK_LOCK(&bucketp->bucket_lock);
    MS_SMP_ASSERT (msb(hashtable->current_buckets) - 
            hashtable->initial_index_shift -1 > 0);
    MS_SMP_ASSERT (bucketp->siblingp == bucketp);
    if (bucketp->element_count <= hashtable->max_chain_length)
    { /* Make sure a deletion hasn't brought us below the threshold */ 
#       ifdef DYN_HASH_DEBUG
            dyn_hash_stats.chain_dropped_below_thold++;
#       endif
        ADVSMP_DYNHASH_BUCKETLOCK_UNLOCK(&bucketp->bucket_lock);
        return;
    }
    
    size = hashtable->current_buckets;
    split_count = msb(size) - hashtable->initial_index_shift-1;
    /* We will now locate each sibling bucket */
    missed_splits = split_count - bucketp->split_count;
#   ifdef DYN_HASH_DEBUG
        dyn_hash_stats.chain_split++;
        if (missed_splits > dyn_hash_stats.largest_missed_splits)
            dyn_hash_stats.largest_missed_splits=missed_splits    ;
#   endif
    
    MS_SMP_ASSERT (missed_splits> 0);
    siblings = (1<<missed_splits)- 1;
    /* the increment is equal to the size of the hashtable the last
       time the bucket was split
       */
    bucket_increment = size >> missed_splits;
    bucket_number = bucketp->controlling_bkno;
    
    for (i=1;i<=siblings;i++)
    {
        /* Since the controlling bucket number is smallest 
         * don't bother starting the loop at zero
         */

        key = bucket_number+i*bucket_increment;
        /* The following macro will not dereference siblingp.
         */
        DYN_KEY_TO_BUCKET_PTR(hashtable,key,size,sbptr);
        MS_SMP_ASSERT(sbptr != bucketp);
        MS_SMP_ASSERT(sbptr->controlling_bkno == bucket_number);
        MS_SMP_ASSERT(sbptr->siblingp == bucketp);

        sbptr->element_count=0;
        sbptr->split_count=split_count;
        sbptr->insert_count = bucketp->insert_count;
        sbptr->chainp = NULL;
        sbptr->controlling_bkno=key;

        ADVSMP_DYNHASH_BUCKETLOCK_INIT(&sbptr->bucket_lock);
    }
    
    hash_links = DYN_GET_HASHLINKS(hashtable,bucketp->chainp);
    elements = bucketp->element_count;
    bucketp->element_count = 0;
    bucketp->chainp = NULL;
    bucketp->split_count = split_count;
    
    /* Rehash the elements into the new buckets. We will start at the end of the
       chain and work backwards in order to perserve the order.
       */
    
    hash_links = DYN_GET_HASHLINKS(hashtable,hash_links->dh_prev);
    for (i=0;i<elements;i++)
    {
        elementp = DYN_UNGET_HASHLINKS(hashtable,hash_links);
        next_links=DYN_GET_HASHLINKS(hashtable,hash_links->dh_prev);
        key = DYN_HASH_GET_KEY(hashtable,
                               hash_links,
                               elementp);
        DYN_KEY_TO_BUCKET_PTR (hashtable,key,size,sbptr);
        MS_SMP_ASSERT (sbptr->siblingp == bucketp);
        if (sbptr->chainp == NULL)
        {
            hash_links->dh_next = hash_links->dh_prev = elementp;
        }
        else /* Insert at head of list - maintaining a circular list */
        {
            /* Maintain a circular list amongst the chain (not the bucket) */
            /* the follwing does this sbptr->chainp->dh_prev->dh_next = hash_links; */
            head_links=DYN_GET_HASHLINKS(hashtable,sbptr->chainp);
            tail_links=DYN_GET_HASHLINKS(hashtable,head_links->dh_prev);
            tail_links->dh_next = elementp; /* point tail to new head */
            
            /* the follwing does this hash_links->dh_prev = sbptr->chainp->dh_prev; */
            hash_links->dh_prev = head_links->dh_prev; /* point new head to tail */
            
            
            /* the follwing does this sbptr->chainp->dh_prev = hash_links; */
            head_links->dh_prev = elementp; /* point old head to new head */
            
            hash_links->dh_next = sbptr->chainp; /* point new head to old head */
        }
        sbptr->chainp = elementp;
        sbptr->element_count++;
        hash_links=next_links;
    }
    
    /**********************************************************************/
    /*                                                                    */
    /* Now we must ensure memory coherence. Issue a memory barrier to    */  
    /* ensure that all of the above changes get out to memory before we   */
    /* update the sibling pointers. The ALPHA architecture allows out of  */
    /* order stores which we must guard against. Once we set the sibling  */
    /* pointer in the bucket the other fields must be initialized.        */
    /*                                                                    */
    /**********************************************************************/
    
    _flush_globals();
    
    for (i=0;i<=siblings;i++)
    {
        key = bucket_number+i*bucket_increment;
        DYN_KEY_TO_BUCKET_PTR(hashtable,key,size,sbptr);
        /*  NOTE:
         *  This assumes atomic stores which is a feature of the ALPHA architecture.
         */
        sbptr->siblingp = sbptr;
    }
    ADVSMP_DYNHASH_BUCKETLOCK_UNLOCK(&bucketp->bucket_lock);
}


/* This routine will double the size of the hashtable.  A new table is
 * allocated that is equal in size to the current hash table.  Since
 * the new buckets will be siblings and point to their corresponding
 * parent buckets in the exiting table, and the parent buckets contain
 * self pointers ( or the existing siblings already point to the
 * parent) It will only be necessary to do a bcopy. All pointers will
 * be correct. All other bucket information is not used.
 * 
 * The last thing that must be done is to place the new size into
 * the hashtable. A memory barrier must be executed before doing this to 
 * insure that all instructions are completed prior to doing this (this 
 * due to instruction re-ording on ALPHA's).
 */

static
int
dyn_double_hashtable(dyn_hashtableT * hashtable)
{
    uint64_t next_idx,i,buckets;
    dyn_bucketT *newbucket;

    next_idx = msb(hashtable->current_buckets) - 
        hashtable->initial_index_shift;
#   ifdef Tru64_to_HPUX_Port_Comments
    /*
     * ----> This was a MALLOC_VAR with M_TIMEDWAIT, more research needed
     */
#   endif /* Tru64_to_HPUX_Port_Comments */
    hashtable->index[next_idx] = (dyn_bucketT *)ms_malloc(sizeof(dyn_bucketT)*hashtable->current_buckets);
    newbucket = hashtable->index[next_idx];
    buckets = hashtable->initial_buckets;
    for (i=0;i<next_idx;i++)
    {
        bcopy(hashtable->index[i],newbucket,buckets*sizeof(dyn_bucketT));
        newbucket = (dyn_bucketT *) ((char *) newbucket +
                                     buckets*sizeof(dyn_bucketT));
        if (i > 0) buckets <<= 1; /* special case first doubling */
    }
#   ifdef DYN_HASH_DEBUG
    
        newbucket = hashtable->index[next_idx];
        for (i=0;i<hashtable->current_buckets;i++)
        {
            newbucket->chainp = NULL;
            newbucket->split_count = -1; 
            newbucket->insert_count = -1;    
            newbucket->element_count = -1;   
            newbucket =(dyn_bucketT *)((char *) newbucket + sizeof(dyn_bucketT)); 
        }
    
#   endif

    hashtable->last_split_time = (uint64_t) lbolt;

    /*
     *                                                                    
     * Now we must ensure memory coherence. Issue a memory barrier to     
     * ensure that all of the above changes get out to memory before we   
     * update the size. The ALPHA architecture allows out of              
     * order stores which we must guard against. Once we set the size in  
     * these buckets are available so we must insure they are properly    
     * setup.                                                             
     *                                                                    
     */

    _flush_globals();
    hashtable->current_buckets <<= 1;
#   ifdef DYN_HASH_DEBUG
        dyn_hash_stats.table_doubled++;
#   endif
    return(ESUCCESS);
}

/*
 *  msb - most significant bit
 *
 *  This function takes a uint64_t as input and returns
 *  the index of the highest order bit set. 
 */
int
msb(uint64_t in)
{
  if(           in <= 0x000000000000FFFF ) {     /* MSB in bits <00:15> */
    if(         in <= 0x00000000000000FF ) {     /* MSB in bits <00:07> */
      if(       in <= 0x000000000000000F ) {     /* MSB in bits <00:03> */
        if(     in <= 0x0000000000000003 ) {     /* MSB in bits <00:01> */
          if(   in &  0x0000000000000002 ) return 1;         /*    <01> */
          else if( in )                    return 0;         /*    <00> */
               else                        return -1;        /* Invalid */
        } else {                                 /* MSB in bits <02:03> */
          if(   in &  0x0000000000000008 ) return 3;         /*    <03> */
          else                             return 2;         /*    <02> */
        }
      } else {                                   /* MSB in bits <04:07> */
        if(     in <= 0x000000000000003F ) {     /* MSB in bits <04:05> */
          if(   in &  0x0000000000000020 ) return 5;         /*    <05> */
          else                             return 4;         /*    <04> */
        } else {                                 /* MSB in bits <06:07> */
          if(   in &  0x0000000000000080 ) return 7;         /*    <07> */
          else                             return 6;         /*    <06> */
        }
      }
    } else {                                     /* MSB in bits <08:15> */
      if(       in <= 0x0000000000000FFF ) {     /* MSB in bits <08:11> */
        if(     in <= 0x00000000000003FF ) {     /* MSB in bits <08:09> */
          if(   in &  0x0000000000000200 ) return 9;         /*    <09> */
          else                             return 8;         /*    <08> */
        } else {                                 /* MSB in bits <10:11> */
          if(   in &  0x0000000000000800 ) return 11;        /*    <11> */
          else                             return 10;        /*    <10> */
        }
      } else {                                   /* MSB in bits <12:15> */
        if(     in <= 0x0000000000003FFF ) {     /* MSB in bits <12:13> */
          if(   in &  0x0000000000002000 ) return 13;        /*    <13> */
          else                             return 12;        /*    <12> */
        } else {                                 /* MSB in bits <14:15> */
          if   (in &  0x0000000000008000 ) return 15;        /*    <15> */
          else                             return 14;        /*    <14> */
        }
      }
    }
  } else {                                       /* MSB in bits <16:63> */
    if(         in <= 0x00000000FFFFFFFF ) {     /* MSB in bits <16:31> */
      if(       in <= 0x0000000000FFFFFF ) {     /* MSB in bits <16:23> */
        if(     in <= 0x00000000000FFFFF ) {     /* MSB in bits <16:19> */
          if(   in <= 0x000000000003FFFF ) {     /* MSB in bits <16:17> */
            if( in &  0x0000000000020000 ) return 17;        /*    <17> */
            else                           return 16;        /*    <16> */
          } else {                               /* MSB in bits <18:19> */
            if( in &  0x0000000000080000 ) return 19;        /*    <19> */
            else                           return 18;        /*    <18> */
          }
        } else {                                 /* MSB in bits <20:23> */
          if(   in <= 0x00000000003FFFFF ) {     /* MSB in bits <20:21> */
            if( in &  0x0000000000200000 ) return 21;        /*    <21> */
            else                           return 20;        /*    <20> */
          } else {                               /* MSB in bits <22:23> */
            if( in &  0x0000000000800000 ) return 23;        /*    <23> */
            else                           return 22;        /*    <22> */
          }
        }
      } else {                                   /* MSB in bits <24:31> */
        if(     in <= 0x000000000FFFFFFF ) {     /* MSB in bits <24:27> */
          if(   in <= 0x0000000003FFFFFF ) {     /* MSB in bits <08:09> */
            if( in &  0x0000000002000000 ) return 25;        /*    <25> */
            else                           return 24;        /*    <24> */
          } else {                               /* MSB in bits <26:27> */
            if( in &  0x0000000008000000 ) return 27;        /*    <27> */
            else                           return 26;        /*    <26> */
          }
        } else {                                 /* MSB in bits <28:31> */
          if(   in <= 0x000000003FFFFFFF ) {     /* MSB in bits <28:29> */
            if( in &  0x0000000020000000 ) return 29;        /*    <29> */
            else                           return 28;        /*    <28> */
          } else {                               /* MSB in bits <30:31> */
            if( in &  0x0000000080000000 ) return 31;        /*    <31> */
            else                           return 30;        /*    <30> */
          }
        }
      }
    } else {
      if(         in <= 0x0000FFFFFFFFFFFFUL ) { /* MSB in bits <32:47> */
        if(       in <= 0x000000FFFFFFFFFFUL ) { /* MSB in bits <32:39> */
          if(     in <= 0x0000000FFFFFFFFFUL ) { /* MSB in bits <32:35> */
            if(   in <= 0x00000003FFFFFFFFUL ) { /* MSB in bits <32:33> */
              if( in &  0x0000000200000000UL ) return 33;    /*    <33> */
              else                             return 32;    /*    <32> */
            } else {                             /* MSB in bits <34:35> */
              if( in &  0x0000000800000000UL ) return 35;    /*    <35> */
              else                             return 34;    /*    <34> */
            }
          } else {                               /* MSB in bits <36:39> */
            if(   in <= 0x0000003FFFFFFFFFUL ) { /* MSB in bits <36:37> */
              if( in &  0x0000002000000000UL ) return 37;    /*    <37> */
              else                             return 36;    /*    <36> */
            } else {                             /* MSB in bits <38:39> */
              if( in &  0x0000008000000000UL ) return 39;    /*    <39> */
              else                             return 38;    /*    <38> */
            }
          }
        } else {                                 /* MSB in bits <40:47> */
          if(     in <= 0x00000FFFFFFFFFFFUL ) { /* MSB in bits <40:43> */
            if(   in <= 0x000003FFFFFFFFFFUL ) { /* MSB in bits <48:41> */
              if( in &  0x0000020000000000UL ) return 41;    /*    <41> */
              else                             return 40;    /*    <40> */
            } else {                             /* MSB in bits <42:43> */
              if( in &  0x0000080000000000UL ) return 43;    /*    <43> */
              else                             return 42;    /*    <42> */
            }
          } else {                               /* MSB in bits <44:47> */
            if(   in <= 0x00003FFFFFFFFFFFUL ) { /* MSB in bits <44:45> */
              if( in &  0x0000200000000000UL ) return 45;    /*    <45> */
              else                             return 44;    /*    <44> */
            } else {                             /* MSB in bits <46:47> */
              if( in &  0x0000800000000000UL ) return 47;    /*    <47> */
              else                             return 46;    /*    <46> */
            }
          }
        }
      } else {                                   /* MSB in bits <48:63> */
        if(       in <= 0x00FFFFFFFFFFFFFFUL ) { /* MSB in bits <48:55> */
          if(     in <= 0x000FFFFFFFFFFFFFUL ) { /* MSB in bits <48:51> */
            if(   in <= 0x0003FFFFFFFFFFFFUL ) { /* MSB in bits <48:49> */
              if( in &  0x0002000000000000UL ) return 49;    /*    <49> */
              else                             return 48;    /*    <48> */
            } else {                             /* MSB in bits <50:51> */
              if( in &  0x0008000000000000UL ) return 51;        /*    <51> */
              else                             return 50;        /*    <50> */
            }
          } else {                               /* MSB in bits <52:55> */
            if(   in <= 0x003FFFFFFFFFFFFFUL ) { /* MSB in bits <52:53> */
              if( in &  0x0020000000000000UL ) return 53;    /*    <53> */
              else                             return 52;    /*    <52> */
            } else {                             /* MSB in bits <54:55> */
              if( in &  0x0080000000000000UL ) return 55;    /*    <55> */
              else                             return 54;    /*    <54> */
            }
          }
        } else {                                 /* MSB in bits <56:63> */
          if(     in <= 0x0FFFFFFFFFFFFFFFUL ) { /* MSB in bits <56:97> */
            if(   in <= 0x03FFFFFFFFFFFFFFUL ) { /* MSB in bits <56:57> */
              if( in &  0x0200000000000000UL ) return 57;    /*    <57> */
              else                             return 56;    /*    <56> */
            } else {                             /* MSB in bits <58:59> */
              if( in &  0x0800000000000000UL ) return 59;    /*    <59> */
              else                             return 58;    /*    <58> */
            }
          } else {                               /* MSB in bits <60:63> */
            if(   in <= 0x3FFFFFFFFFFFFFFFUL ) { /* MSB in bits <60:61> */
              if( in &  0x2000000000000000UL ) return 61;    /*    <61> */
              else                             return 60;    /*    <60> */
            } else {                             /* MSB in bits <62:63> */
              if( in &  0x8000000000000000UL ) return 63;    /*    <63> */
              else                             return 62;    /*    <62> */
            }
          }
        }
      }
    }
  }
}

#ifdef DYN_HASH_DEBUG

/* 
 * This routine is used to search for duplicate keys in the hash table. 
 * It walks all chains in the table looking for duplicates. The assumption
 * that no duplicate keys should exists may not and probably doesn't apply
 * to all hash tables.
 */


void 
dyn_hash_debug_search( void *hashtable,
                       void * elementp)
    
{
    int bucket,matches=0;
    dyn_hashtableT *hashtablep;
    dyn_hashlinks_w_keyT *hash_links, *head_links;
    dyn_bucketT *tmp_bptr;
    dyn_bucketT *controlling_bptr;
    uint64_t key;
    
    hashtablep = (dyn_hashtableT *) hashtable;
    hash_links = (dyn_hashlinks_w_keyT *) DYN_GET_HASHLINKS(hashtablep,elementp);
    key = DYN_HASH_GET_KEY(hashtablep,hash_links,elementp);
    bucket=0;
    do {
        controlling_bptr=dyn_lock_bucket((dyn_hashtableT *)hashtablep,bucket);
        hash_links=head_links = (dyn_hashlinks_w_keyT *) DYN_GET_HASHLINKS(hashtablep,controlling_bptr->chainp);
        if (hash_links != NULL)
        {
            do {
                if (hash_links->dh_key == key)
                {
                    matches++;
                    DYN_KEY_TO_BUCKET_PTR(hashtablep,key,hashtablep->current_buckets,tmp_bptr);
                    if (controlling_bptr != tmp_bptr->siblingp)
                    {
                        panic("key on wrong chain");
                    }
                }
                hash_links = (dyn_hashlinks_w_keyT *) DYN_GET_HASHLINKS(hashtablep,hash_links->dh_links.dh_next);
            } while (hash_links != head_links);
        }
        DYN_UNLOCK_BUCKET(controlling_bptr);
        bucket++;
    } while(bucket < hashtablep->current_buckets);
    if (matches != 1)
    {
        panic("Duplicate Hash Key Found");
    }
}
#endif

#ifdef ADVFS_SMP_ASSERT
/* 
 * This routine is used to search for duplicate keys in the hash table. 
 * It walks all chains in the table looking for duplicates. The assumption
 * that no duplicate keys should exists may not and probably doesn't apply
 * to all hash tables.
 */

void 
dyn_hash_debug_chains( dyn_hashtableT *hashtablep,
                       void *compare_value,
                       void (*compare_func)())
    
{
    int32_t bucket;
    dyn_hashlinks_w_keyT *hash_links, *head_links;
    dyn_bucketT *controlling_bptr;
    bucket=0;
    do {
        controlling_bptr=dyn_lock_bucket((dyn_hashtableT *)hashtablep,bucket);
        if (controlling_bptr->chainp != NULL) {
            hash_links =
                head_links = (dyn_hashlinks_w_keyT *) 
                              DYN_GET_HASHLINKS(hashtablep,
                                                controlling_bptr->chainp);
            do {
                ((*compare_func)(DYN_UNGET_HASHLINKS(hashtablep,hash_links), compare_value));
                hash_links = (dyn_hashlinks_w_keyT *) DYN_GET_HASHLINKS(hashtablep,hash_links->dh_links.dh_next);
            } while (hash_links != head_links);
        }
        DYN_UNLOCK_BUCKET(controlling_bptr);
        bucket++;
    } while(bucket < hashtablep->current_buckets);
}

#endif
