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

#ifndef _E_KERN_DYN_HASH_H_
#define _E_KERN_DYN_HASH_H_

typedef struct dyn_hashlinks {
   void  *dh_next;       
   void  *dh_prev;
} dyn_hashlinksT;

typedef struct dyn_hashlinks_w_key{
   dyn_hashlinksT dh_links;
   uint64_t       dh_key;
} dyn_hashlinks_w_keyT;

extern void *
dyn_hashtable_init(uint64_t initial_bucket_count,
                   uint64_t bucket_length_threshold,
                   int      bucket_to_element_ratio,
                   uint64_t split_interval,
                   uint64_t offset_to_hashlinks,
                   uint64_t (*hash_function) () );

extern void 
dyn_hashtable_destroy( void *hashtable);
 
extern void * 
dyn_hash_obtain_chain( void *hashtable,
                       uint64_t key,
                       uint64_t *insert_count);

extern void 
dyn_hash_release_chain( void * hashtable,
                        uint64_t key);

extern void 
dyn_hash_insert( void *hashtable,
                 void * element,
                 int obtain_lock);

extern void 
dyn_hash_remove( void * hashtable,
                 void * element,
                 int obtain_lock);

extern void 
dyn_kernel_thread_setup();

#endif  /* _E_KERN_DYN_HASH_H_ */


