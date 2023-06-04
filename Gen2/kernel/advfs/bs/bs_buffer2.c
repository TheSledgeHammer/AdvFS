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

#define ADVFS_MODULE BS_BUFFER2

/*
 * Set the define so that we wire the page on ref in order to avoid
 * the possibility of looping forever from an error condition such
 * as domain panic.
 */
#define WIRE_METADATA_ON_REF

/*
 * Don't define WIRE_METADATA_ON_PIN as there would be a performance
 * hit and we effectively prevent vhand from stealing pages by 
 * incrementing the write count.
 */

#include <sys/types.h>
#include <sys/param.h>
#include <machine/sys/clock.h>
#include <ms_public.h>
#include <ms_privates.h>
#include <ms_logger.h>
#include <bs_extents.h>
#include <bs_public.h>
#include <ms_assert.h>
#include <ms_osf.h>
#include <sys/vm_arena_iface.h> 
#include <sys/vnode.h>
#include <sys/buf.h>
#include <vfast.h>
#include <sys/spinlock.h>

extern kmem_handle_t advfs_bsbuf_arena;

/* 
 * advfs_init_bsbuf_hashtable - Initialize the dynamic hashtable to
 * hold bsbufs for all meta-data pages that were brought into memory
 * through pin page.
 */

void * BsBufHashTable;

void
advfs_init_bsbuf_hashtable()
{

    BsBufHashTable = dyn_hashtable_init((uint64_t)BS_BSBUF_INITIAL_SIZE,
                                        (uint64_t)BS_BSBUF_HASH_CHAIN_LENGTH,
                                        BS_BSBUF_ELEMENTS_TO_BUCKETS,
                                        (uint64_t)BS_BSBUF_USECS_BETWEEN_SPLITS,
                                        (uint64_t)advfs_offsetof(struct bsBuf, bsb_hashlink),
                                        NULL);
    if (BsBufHashTable == NULL) {
        ADVFS_SAD0("advfs_init_bsbuf_hashtable: can't get space for hash table");
    }
}
    
/* 
 * advfs_obtain_bsbuf
 *
 * This function will return a bsBuf to the caller. It will either
 * create a new bsBuf or return an existing bsBuf if it is found in
 * the hashtable. If a new bsBuf is created it will also be inserted
 * into the hashtable. A status will be returned to the caller
 * indicating if it was found in the table or not (this will be useful
 * for debugging purposes).
 *
 * Advfs_obtain_bsbuf() will only be called for writes of meta-data by
 * either advfs_getpage() for write intent or advfs_putpage() for a
 * dirty meta-data page. Advfs_getpage for read or advfs_putpage for
 * clean pages (meta-data or VHAND) will not call this to get a
 * bsBuf. Furthermore the only time a bsBuf should be created is for
 * the advfs_getpage call and only on the 1st PFDAT.
 *
 * A read (or rbf_refpg) will not get a bsBuf. This means that clean
 * pages (PFDATs) may or may not have a bsBuf depending how they were
 * initially brought in to the page cache. When a meta-data PFDAT is
 * found in the cache the bsBuf hashtable will always be checked and a
 * new bsBuf created if necessary. Similarly when a PFDAT is being
 * invalidated, advfs_putpage() gets called to service the invalidate
 * and calls advfs_remove_bsbuf() to check the hashtable to see if
 * a bsBuf exists and whether all the PFDATs currently associated with
 * the bsBuf are invalid before freeing the bsBuf.
 *
 * Synchronization
 *
 * At least one of the PFDATs that make up the meta-data page for
 * which this bsBuf is being requested must be locked via either a
 * call to fcache_page_alloc or fcache_page_scan.
 *
 * Bumping the bsb_writeref in the bsBuf while the page lock is held
 * will prevent VHAND from stealing the any of the PFDATs associated
 * with this bsBuf if they are dirty.
 * 
 * A bsBuf will only be created for the 1st PFDAT of a meta-data
 * page. This is because the 1st PFDAT is always looked up first and
 * if the bsBuf does not exist, it will be created and since the
 * bsb_writeref is bumped, the 1st PFDAT and therefore the bsBuf can
 * not be stolen.
 * 
 * We need to set the bsBuf's bsb_pfdats_bitmap for the 1st page when
 * a bsBuf is created before inserting the bsBuf in the hash
 * table. This prevents the following race:
 * 
 * 1. The 1st PFDAT is not in the cache, the 2nd PFDAT is in the cache
 * and clean, there is no bsBuf for this meta-page in the hashtable.
 *
 * 2. The 1st PFDAT is returned locked and the bsBuf is created and
 * inserted in the hash table with the bsb_pfdats_bitmap bit for this
 * PFDAT set.
 *
 * 3. Meanwhile VHAND chooses the 2nd PFDAT to be
 * invalidated. Advfs_putpage allows this since the 2nd PFDAT is
 * clean. Advfs_putpage will call advfs_remove_bsbuf() as part of the
 * invalidation request who checks if the bsBuf is found in the
 * bsBuf hash table. Since the bit in the bsb_pfdats_bitmap is set
 * AdvFS will not remove the bsBuf from the table.
 *
 * 4. The 2nd PFDAT is looked up /created and the hash table lookup
 * finds the bsBuf.
 *
 * NOTE: found will be used to indicate if the caller expects to
 * find the bsbuf or not. If TRUE then this is a lookup of the bsBuf
 * and the caller is expecting to find it. No action to the bsBuf will
 * be taken. If FALSE we will perform some setup and potentially create
 * the bsBuf
 *
 * RETURN VALUES:
 *
 * if found flag is TRUE (this means only lookup the bsbuf). 
 *       returns bsbuf, a cookie and the hashchain locked.
 *       or not bsbuf, no cookie and no lock.
 * if found is false
 *      returns a bsbuf (new or found) with no locks held.
 *
 */

struct bsBuf *
advfs_obtain_bsbuf(
    struct vnode *vp,       /* metadata file's vnode */
    off_t offset,           /* byte offset of page in the file */
    void **cookie,           /* Info to be passed to removebuf */
    int *lookup             /* input - TRUE means a lookup, don't create
                            * a bsBuf and return with chain locked!
                            * FALSE means create a bsBuf (if not
                            * lookup) and DON't return with the
                            * chain locked. */
                              )
{
    uint64_t key, insert_count = 0, old_insert_count = 0;
    struct bsBuf *bsBuf,*bsBuf_head,*new_bsBuf = NULL;
    bfAccessT *bfap;
    off_t adj_offset;    
    bfap = VTOA(vp);

    adj_offset = rounddown(offset, bfap->bfPageSz * ADVFS_FOB_SZ);

    /* Get our bucket key from the vnode and offset. */

    key = BS_BSBUF_GET_KEY(vp,offset);
    
    /* Get the bucket pointer and lock the bucket. */
    
rescan_chain:
    bsBuf_head = bsBuf = BS_BSBUF_HASH_LOCK(key, &insert_count);
    *cookie = NULL;
    do
    {
        if (bsBuf == NULL) break;
        if ((new_bsBuf != NULL) &&
            (insert_count == old_insert_count)) {
            /* no need to rescan the chain if there have been no
             * inserts since we dropped the lock and aquired a new
             * bsBuf (further down this function) */
            break;
        }
        if ((bsBuf->bsb_foffset == adj_offset) && 
            (bsBuf->bsb_bfaccess == bfap)) {
            /* The bsBuf exists in the hashtable */

            if (*lookup == FALSE) {
                spin_lock(&bsBuf->bsb_lock);
                /* We are always called with the 1st PFDAT. This avoids
                 * racing with remove_bsbuf. Not sure the lock is
                 * necessary.
                 */
                bsBuf->bsb_writeref++;
                bsBuf->bsb_pfdatbitmap |= (METADATA_FIRST_HALF |
                                           METADATA_SECOND_HALF);
                spin_unlock(&bsBuf->bsb_lock);
                BS_BSBUF_HASH_UNLOCK(key);
            } else {
                /* The remove bsbuf routine needs to cookie to avoid having
                 * to reobtain the hash chain lock. Only pass it back for
                 * a lookup. (lookup == TRUE)
                 */
                *cookie = (void *) bsBuf_head;
            }

            MS_SMP_ASSERT(BS_BFTAG_EQL(bsBuf->bsb_tag,bfap->tag));
            *lookup=TRUE;
            if (new_bsBuf != NULL) {
                /* this is the second scan of the chain if new_bsBuf
                 * is not NULL.  So we free the memory if we lookup the
                 * bsBuf we wanted in the chain already (from an insert 
                 * while we didn't hold the lock) */
                advfs_bs_free_bsbuf(new_bsBuf, FALSE);
            }
            return(bsBuf);
        }
        bsBuf = bsBuf->bsb_hashlink.dh_links.dh_next;
    }while (bsBuf != bsBuf_head);
    /* We didn't find it in the hashtable */
    if ( *lookup == FALSE) {
        /* We are creating a new bsbuf. We must not hold the chain locked
         * or we might block vhand.  We'll have to rescan the hash chain
         * after reaquiring the chain lock since the bsBuf we want may 
         * have been inserted while we didn't hold the chain lock.
         */

        if (new_bsBuf == NULL) {
            int rescan = 0;
            /* this path runs if we haven't yet allocated a new bsBuf,
             * otherwise we skip ahead and just do the insert. We can't
             * wait for the first call to get_bsbuf or we might block
             * vhand */
            new_bsBuf = advfs_bs_get_bsbuf(FALSE); 

            if (new_bsBuf == NULL) {
                BS_BSBUF_HASH_UNLOCK(key);
                rescan = 1;
                old_insert_count = insert_count; /* save off the insert count */
                new_bsBuf = advfs_bs_get_bsbuf(TRUE); /* now we can wait since
                                                       * we don't hold the lock */
            }
            new_bsBuf->bsb_foffset = offset;
            new_bsBuf->bsb_bfaccess = bfap;
#ifdef ADVFS_SMP_ASSERT 
            new_bsBuf->bsb_tag = bfap->tag;
            new_bsBuf->bsb_domainId = bfap->dmnP->domainId;
#endif
            new_bsBuf->bsb_pfdatbitmap |= (METADATA_FIRST_HALF |
                                   METADATA_SECOND_HALF);
            new_bsBuf->bsb_hashlink.dh_key = key;
            new_bsBuf->bsb_writeref++;
            if (rescan) {
                /* we dropped the chain lock, so we need to rescan the
                 * the chain for a possible racing insert */
                goto rescan_chain;
            }
        }
        BS_BSBUF_HASH_INSERT(new_bsBuf,FALSE); /* unlocks the chain */
    } else {
        /* In the lookup case of this routine we don't need to 
         * return with the hashchain lock held since the caller
         * is always holding the pages locked and thus blocking
         * a busbuf from being associated with the pages. NOTE 
         * the bsbuf could actually get created in pin page but
         * the caller will block in getpage waiting on the page
         * locks we are holding. 
         */
        bsBuf=NULL;
        BS_BSBUF_HASH_UNLOCK(key);
    }
    MS_SMP_ASSERT( (offset % bfap->bfPageSz * ADVFS_FOB_SZ) == 0);
    *lookup = FALSE;
    /* If we are returning here, we either have a new bsBuf OR we did a lookup
     * only and didn't find the bsBuf we asked for.  In the first case we need 
     * to return the new_bsBuf.  In the second case, we should return NULL.
     * Since the new_bsBuf will be NULL in the second case anyway, we can just
     * return new_bsBuf unconditionally at this point */
    return(new_bsBuf);
}

/* advfs_remove_bsbuf
 *
 * This function is called when page cache page invalidation or
 * free requests are made on metadata pages only.
 * This function clears a bsBuf's pfdat association and potentially
 * removes the bsBuf from the global bsBuf hash table when there are
 * no pfdat associations remaining. Optionally, the bsBuf structure
 * memory will be freed when there are no pfdat associations remaining.
 * 
 * Assumptions:
 * We will assume that a PFDAT can exist without a bsBuf. This is true
 * for reading meta-data.
 * We assume a metadata page consists of a two pfdat's. This routine
 * will need to change to support larger metadata pages sizes.
 *
 * Synchronization
 *
 * The page lock is held for this PFDAT.  The PFDAT page lock is
 * always held prior to accessing the bsBuf from the hashtable.  The
 * hashtable chain lock and the bsBuf lock must be held when checking
 * the bsb_pfdats_bitmap and/or removing the bsBuf from the hashtable.
 *
 * We need to coordinate with bs_pinpg/advfs_obtain_bsbuf. A bsBuf is
 * only created when a meta-data page is faulted in for writing (ie
 * bs_pinpg).  Before calling fault bs_pinpg will first lookup/create
 * the bsBuf in the hashtable and while holding the hashtable chain
 * lock, increment the writeRef in the bsBuf.
 * THe chain lock is then released and fault is called.
 *
 * The passed in cookie requires that the hash chain lock be held.
 * otherwise a NULL cookie should be passed (not used currently).
 * The cookie is just the start of the hashchain. We can't drop the
 * chain lock between calls.
 */

int
advfs_remove_bsbuf(
                   struct vnode *vp,   /* metadata file's vnode */
                   off_t start_offset, /* Starting file page byte offset */
                   size_t size,        /* Byte length of request to clear */
                   void *cookie)        /* Returned from obtain_bsBuf */
{
    uint64_t key;
    bfAccessT *bfap;
    struct bsBuf *bsBuf;
    struct bsBuf *chainStart = (struct bsBuf *)cookie;
    off_t curoffset;

    bfap = VTOA(vp);
    MS_SMP_ASSERT(size <= bfap->bfPageSz * ADVFS_FOB_SZ);
    key = BS_BSBUF_GET_KEY(vp, start_offset);
    if (chainStart == NULL) {
    /* Lock the bucket that the potential bsBuf lives on */
        chainStart = bsBuf = BS_BSBUF_HASH_LOCK(key, NULL);
    } else {
        bsBuf = chainStart;
    }
    /* Search the hash bucket for our metadata file's bsBuf. */
    while (bsBuf != NULL) {
        if ((bsBuf->bsb_foffset == (start_offset & (~VM_PAGE_SZ))) &&
            (bsBuf->bsb_bfaccess == bfap)) {
            /* The bsBuf exists in the hashtable */

            /* I think the hashtable bucket lock is enough
             * and we shouldn't need the bsb_lock.
             */
            spin_lock(&bsBuf->bsb_lock);

            /* WARNING This needs to change if we ever go to a 
             * other than 8K metadata page size. Since there are
             * no plans for this and performance is important
             * we will code this for performance.
             */
            if (bsBuf->bsb_writeref == 0) {
                if (start_offset & VM_PAGE_SZ){
                    /* This is the 2nd PFDAT */
                    bsBuf->bsb_pfdatbitmap &= ~METADATA_SECOND_HALF;
                    MS_SMP_ASSERT(size == VM_PAGE_SZ);
                } else if( size == VM_PAGE_SZ) {
                    /* This is the 1st PFDAT */
                    bsBuf->bsb_pfdatbitmap &= ~METADATA_FIRST_HALF;
                } else {
                    /* We are removing both PFDATs */
                    bsBuf->bsb_pfdatbitmap = METADATA_NONE;
                }

                /* If there are no dirty pages in the cache for this
                 * bsBuf, then remove the bsBuf from the bsBuf hash table.
                 * Also, free the bsBuf only on an explicit request by caller.
                 *
                 * We could encouter a writeRef. This means that a bs_pinpg
                 * came along and reffed the bsBuf before we were able to
                 * complete the i/o. In this case that thread should now
                 * be blocked in page_alloc waiting for us to release the
                 * page locks. The pages are going to be brought right back
                 * in so we will (actually must) leave the bsBuf in the
                 * hashtable.
                 */
                if (bsBuf->bsb_pfdatbitmap == METADATA_NONE){
                    spin_unlock(&bsBuf->bsb_lock);
                    BS_BSBUF_HASH_REMOVE(bsBuf,FALSE); /* unlocks chain lock */
                    advfs_bs_free_bsbuf(bsBuf, FALSE);
                    return(TRUE);
                }
            }

            spin_unlock(&bsBuf->bsb_lock);
             break;
        } else {
            bsBuf = (struct bsBuf*)bsBuf->bsb_hashlink.dh_links.dh_next;
            if (bsBuf == chainStart) {
                break;
            }
        }
    }
    BS_BSBUF_HASH_UNLOCK(key);
    return (FALSE);
}

/*
 * advfs_bs_get_buf
 * 
 * Allocates and optionally zeroes a buf structure.
 *
 * Callers who will overwrite the entire buf structure may request no 
 * memory zeroing. 
 */
struct buf *
advfs_bs_get_buf (int32_t wait,   /*In: TRUE/FALSE flag to wait for memory */
                  int32_t zeromem)/*In: TRUE/FALSE flag to zero memory */
{
    struct buf *bp;

    if (zeromem) {
        /* Ms_malloc implicitly zeroes the entire buf structure memory. */
        if (wait) 
            bp = (struct buf *) ms_malloc(sizeof(struct buf));
        else
            bp = (struct buf *) ms_malloc_no_wait(sizeof(struct buf));
    } else {
        /* Instruct ms_malloc not to zero the memory. */
        if (wait)
            bp = (struct buf *) ms_malloc_no_bzero(sizeof(struct buf));
        else
            bp = (struct buf *) ms_malloc_no_wait_or_bzero(sizeof(struct buf));
    }
    return(bp);
}

/* advfs_bs_free_buf
 * 
 * Frees a buf structure obtained from advfs_bs_get_buf() back to memory.
 */
void
advfs_bs_free_buf(struct buf * bufp) /* in: Buf structure to free */
{
    ms_free(bufp);
}

/* advfs_bs_bsbuf_ctor
 *
 * bsBuf structure memory arena constructor. This will allocate
 * the structure's spinlock. 
 * The kernel memory arena component calls this function.
 */
int
advfs_bs_bsbuf_ctor(void *bsbufp,         /* in: */
                    size_t bsBufSize,     /* in: */
                    int flags)            /* in: */
{
    struct bsBuf *bp;

    bp = (struct bsBuf *) bsbufp;
    ADVSMP_BSBUFLOCK_INIT( &bp->bsb_lock );

    bp->bsb_magicid = (BSBUFMAGIC | MAGIC_DEALLOC);   
    return(EOK);
}

/* advfs_bs_bsbuf_dtor
 *
 * BsBuf structure memory arena destructor. This will deallocate
 * the structure's spinlock. 
 * The kernel memory arena component calls this function.
 */
void
advfs_bs_bsbuf_dtor(void *bsbufp,         /* in: */
                    size_t bsBufSize,     /* in: */
                    int flags)            /* in: */
{
    struct bsBuf *bp;

    bp = (struct bsBuf *) bsbufp;
    MS_SMP_ASSERT(bp->bsb_magicid == (BSBUFMAGIC | MAGIC_DEALLOC));
    spin_destroy(&bp->bsb_lock);
}    

/*
 * advfs_bs_get_bsbuf
 * 
 * Allocates an bsBuf structure from the memory allocator.
 * 
 */
struct bsBuf *
advfs_bs_get_bsbuf (int32_t wait)   /*In: TRUE/FALSE flag to wait for memory */
{
    struct bsBuf * bsbufp;

    bsbufp = kmem_arena_alloc(advfs_bsbuf_arena, (wait) ? M_WAITOK : M_NOWAIT);

    /* Initialize the structure if it exists, except for the bsb_lock
     * that the memory arena already initialized.
     */
    if (bsbufp) {
        MS_SMP_ASSERT(bsbufp->bsb_magicid == (BSBUFMAGIC | MAGIC_DEALLOC));
        bsbufp->bsb_metafwd = NULL;
        bsbufp->bsb_metabwd = NULL;
        bsbufp->bsb_iolistfwd = NULL;
        bsbufp->bsb_writeref = 0;
        bsbufp->bsb_flushseq = nilLSN;
        bsbufp->bsb_firstlsn = nilLSN;
        bsbufp->bsb_origlogrec = logNilRecord;
        bsbufp->bsb_currentlogrec = logNilRecord;
        bsbufp->bsb_flags = 0;
        /* Illegal offset will cause assert later if offset never gets set */
        bsbufp->bsb_foffset = -1;
        bsbufp->bsb_fcachemapdesc = 0;
        bsbufp->bsb_mwllist = 0;
        bzero((caddr_t)&bsbufp->bsb_hashlink, sizeof(dyn_hashlinks_w_keyT));
        bsbufp->bsb_pfdatbitmap = METADATA_NONE;
        bsbufp->bsb_magicid = BSBUFMAGIC;
    }
    return(bsbufp);
}

/* advfs_bs_free_bsbuf
 * 
 * Frees an bsBuf structure back to the memory allocator.
 * Wait flag indicates whether or not caller allows this routine to sleep.
 *
 */
void
advfs_bs_free_bsbuf(struct bsBuf * bsbufp, /* in: bsBuf structure to free */
                    int32_t wait)          /* in: TRUE/FALSE to allow sleep */
{
    MS_SMP_ASSERT(bsbufp->bsb_metafwd == NULL);
    MS_SMP_ASSERT(bsbufp->bsb_metabwd == NULL);
    
    bsbufp->bsb_magicid |= MAGIC_DEALLOC;
    (void) kmem_arena_free(bsbufp, (wait) ? M_WAITOK : M_NOWAIT);
}

/*
 * bs_refpg
 *
 * Reference a metadata page for *reading* given the file page byte offset.
 * Return an opaque reference handle and the page's virtual address.
 * The caller uses the handle to pass to bs_derefpg().
 *
 * This refpg interface is only for metadata and log data files.
 */
statusT
bs_refpg(
         bfPageRefHT *bfPageRefH, /* out */
         void **bfPageAddr,       /* out - virtual address of data */
         struct bfAccess *bfap,   /* in */
         bs_meta_page_t page,     /* in */
         ftxHT ftxH,              /* in: transaction handle or FtxNilFtxH */
         meta_flags_t mflags)     /* flags */
{
    fcache_map_dsc_t   *fcacheDesc;
    faf_status_t faultStatus;
    uint64_t dummy;
    size_t size = 0; /* Default to the map descriptor */
    statusT sts=0;
    struct advfs_pvt_param priv_param;
    off_t foffset = page * bfap->bfPageSz * ADVFS_FOB_SZ;
    

    if (bfap->bfaFlags & BFA_OUT_OF_SYNC) {
        return E_OUT_OF_SYNC_SNAPSHOT;
    }

    /* Metadata files only */
    MS_SMP_ASSERT((bfap->dataSafety == BFD_METADATA) ||
                  (bfap->dataSafety == BFD_LOG));

    /* Update statistics counters. */
    if (AdvfsLockStats) {
        ADVFS_ATOMIC_FETCH_INCR(&bfap->dmnP->bcStat.refCnt, &dummy);
    }

    /*
     * Clear out the priv_params so the app_flag field can be correctly set
     * in getpage.
     */
    priv_param = Nil_advfs_pvt_param;
    priv_param.app_starting_offset = foffset;
    priv_param.app_total_bytes = bfap->bfPageSz * ADVFS_FOB_SZ;

    MS_SMP_ASSERT( bfap->bfVnode.v_vfsp->vfs_version == 1);

    /* Obtain a UFC map descriptor. */
    fcacheDesc = fcache_as_map(bfap->dmnP->metadataVas,
                               &bfap->bfVnode,
                               foffset,
                               bfap->bfPageSz * ADVFS_FOB_SZ, /*pg byte size */
                               0L,      /*page size hint */
                               FAM_READ,
                               &sts);
    if (sts != EOK) {
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
        return sts;
    }
    MS_SMP_ASSERT( fcacheDesc->fm_size == bfap->bfPageSz * ADVFS_FOB_SZ);

    /* Fault in the page cache pages.
     * Refpg will always request the page mapping be wired (FAF_MLOCK).
     * Derefpg must release the wired map lock reference.
     */

/* Turned off wiring of meta-data. Currently we are seeing a performance
 * improvement by removing wiring. If the wire code paths improve we
 * may want to revisit this. Wiring will keep VHAND from picking these
 * at all. We will deny VHAND in putpage but not calling putpage at all
 * may be a win. Currently we are finding that wiring is more costly
 */

    sts = fcache_as_fault(fcacheDesc,
                          NULL,  /*use UFC descriptor's virtual addr.*/
                          &size, 
                          &faultStatus,
                          (uintptr_t)&priv_param,  
#ifdef WIRE_METADATA_ON_REF
                          (FAF_READ | FAF_SYNC | FAF_MLOCK));
#else
                          (FAF_READ | FAF_SYNC));
#endif
    
    if (sts) {
        /* Ignore secondary error */
        (void) fcache_as_unmap(fcacheDesc, NULL, 0, FAU_FREE);
        *bfPageRefH = NULL;
        *bfPageAddr = NULL;
        return (sts);
    }


    /* Pass back the opaque refpg handle and virtual address of the data. 
     * The caller will pass this handle into bs_derefpg() to release the page.
     */
    *bfPageRefH = (bfPageRefHT) fcacheDesc;
    *bfPageAddr = (void *)fcacheDesc->fm_vaddr;

    /* If we did any IO, check the page. */
    if ((mflags & MF_VERIFY_PAGE) &&
        (priv_param.app_flags & APP_IO_PERFORMED)) {
        /*
         * This check will do quick basic checks on the metapage to make
         * sure we got the right one.  
         */
        advfs_verify_meta_page( bfap, foffset, *bfPageAddr );
    }

    return (EOK);
}

/*
 * bs_derefpg
 *
 * Given a reference handle, dereference the buffer page.
 *
 * SMP: enter with no explicit locks held.
 */
statusT
bs_derefpg(
           bfPageRefHT bfPageRefH,      /* in */
           bfPageCacheHintT cacheHint   /* in */
           )
{
    fcache_map_dsc_t   *fcacheDesc;

    /* Convert opaque refpg handle into the file cache descriptor. */
    fcacheDesc = (fcache_map_dsc_t *) bfPageRefH;

    /*
     * We need to unwire the page.
     */
#ifdef WIRE_METADATA_ON_REF
    fcache_as_munlock(fcacheDesc, 
                      fcacheDesc->fm_vaddr,
                      fcacheDesc->fm_size,
                      FAMU_MUNLOCK);
#endif

    /* Release the mapping based upon caller's hint. This implicitly
     * decrements the wire reference count on the metadata page.
     */
    return(fcache_as_unmap(fcacheDesc,
                         fcacheDesc->fm_vaddr,
                         0, /*use default size in mapping */
                         ((cacheHint== BS_CACHE_IT )? FAU_CACHE : FAU_FREE)));
}

/*
 * bs_pinpg
 *
 * Reference a metadata/log page for *writing* given a bitfile handle and
 * the page number in the bitfile.  Return a reference handle
 * and the page's address, if any. 
 *
 * This pin operation is only for metadata and log data files.
 *
 * SMP:  Enter and leave with no locks held.
 */
statusT
bs_pinpg(bfPageRefHT *bfPageRefH,   /* out */
         void **bfPageAddr,         /* out */
         struct bfAccess *bfap,     /* in */
         bs_meta_page_t page,       /* in */
         ftxHT ftxH,                /* in: transaction handle or FtxNilFtxH */
         meta_flags_t mflags)       /* flags */

{
    struct advfs_pvt_param priv_param;
    fcache_map_dsc_t *fcacheDesc;
    faf_status_t faultStatus;
    struct bsBuf *bsbufp;
    void *cookie;
    size_t size = 0; /* Default to the map descriptor */
    int sts;
    int lookup = FALSE;
    off_t foffset = page * bfap->bfPageSz * ADVFS_FOB_SZ;

    if (bfap->bfaFlags & BFA_OUT_OF_SYNC) {
        return E_OUT_OF_SYNC_SNAPSHOT;
    }


    /* Metadata files only */
    MS_SMP_ASSERT((bfap->dataSafety == BFD_METADATA) ||
                  (bfap->dataSafety == BFD_LOG));

    /* Tell getpage to increment the metadata page's pinpg write reference
     * counter. Set the metadata page overwrite hint based on pinner's request.
     */
    priv_param = Nil_advfs_pvt_param;
    if (mflags & MF_OVERWRITE)
        priv_param.app_flags = APP_METAPG_OVERWRITE;
    priv_param.app_parent_ftx = ftxH;

    /* Obtain a UFC map descriptor. */
    fcacheDesc = fcache_as_map(bfap->dmnP->metadataVas,
                               &bfap->bfVnode,
                               foffset,
                               bfap->bfPageSz * ADVFS_FOB_SZ, /*pg byte size */
                               0,      /*page size hint */
                               FAM_WRITE,
                               &sts);
    if (sts != EOK)
        return sts;

    /* We now need to obtain the bsbuf before calling into
     * advfs_getpage.  It has been determined that forcing a fault
     * (via the GPAGE flag) is too expensive. The call to obtain the
     * bsbuf will set the found/lookup flag to FALSE. This will
     * instruct obtain_bsbuf to return a bsbuf to the caller by either
     * finding it in the hash table or creating a new one (and also
     * placing it in the hash table). Obtain_bsbuf will also increment
     * the writeref and mark both pages resident before returning.
    */

    bsbufp = advfs_obtain_bsbuf(&bfap->bfVnode,foffset,&cookie,&lookup);
    MS_SMP_ASSERT((bfap->dataSafety != BFD_LOG) ||
                  (bsbufp->bsb_writeref > 0));

    /* Fault in the page cache and retrieve the bsBuf pointer.
     * Pinpg requires the fault to always call our getpage (FAF_GPAGE).
     * Pinpg will always request the page mapping be wired (FAF_MLOCK).
     * Unpinpg must release the wired map lock.
     */
    sts = fcache_as_fault(fcacheDesc,
                          NULL,  /*use UFC descriptor's virtual addr.*/
                          &size, 
                          &faultStatus,
                          (uintptr_t)&priv_param,
#ifdef WIRE_METADATA_ON_PIN
                          (FAF_WRITE | FAF_SYNC | FAF_MLOCK)
#else
                          (FAF_WRITE | FAF_SYNC)
#endif
        );
    
    if (sts != EOK) {
        /* This gets very tricky to clean up after. The problem is that 
         * we have already set the writeref in the bsbuf and marked the
         * pages resident. We now have a couple of problems. First we
         * do not know if the pages really are or are not resident. So
         * we can't just clear the bits. Second after we set the writeref
         * another thread could have attempted to synchromously flush these
         * pages and ended up waiting for us to flush them in which case
         * they are relying on us to wake them up.
         * So about the only thing we can do here is to flush and invalidate
         * the pages. If this fails then we must panic the domain since
         * this qualifies as being unable to write meta-data.
         */
        int sts2;
 
        spin_lock(&bsbufp->bsb_lock);
        bsbufp->bsb_writeref--;
        spin_unlock(&bsbufp->bsb_lock);
        sts2=fcache_vn_flush(&bfap->bfVnode, 
                        foffset,
                        bfap->bfPageSz*ADVFS_FOB_SZ,
                        NULL,
                        FVF_WRITE | FVF_SYNC | FVF_INVAL);
        if (sts2 != EOK) {
            domain_panic(bfap->dmnP,"bs_pinpg can't invalidate metapage");
        }

        /*
         * We weren't able to fault in the page, yet we may have a bsbuf
         * that needs to be free'd.
         */
        lookup = TRUE; /* flag indicates to take out hash chain lock on success */
        bsbufp = advfs_obtain_bsbuf(&bfap->bfVnode,foffset,&cookie,&lookup);
        if (bsbufp) {
            if (bsbufp->bsb_writeref==0 && bsbufp->bsb_metafwd==NULL) {
                int removed_bsbuf;
                /* call releases the hash chain lock */
                removed_bsbuf = advfs_remove_bsbuf(&bfap->bfVnode, foffset, 
                                                   bfap->bfPageSz*ADVFS_FOB_SZ, 
                                                   cookie);
                MS_SMP_ASSERT(removed_bsbuf);
            } else {
                /* release the hash chain lock */
                BS_BSBUF_HASH_UNLOCK(bsbufp->bsb_hashlink.dh_key);
            }
        }

        goto error_cleanup;
    }    
    /* Retrieve our bsBuf pointer returned by the fault.
     * Save the UFC map descriptor into the bsBuf for use by bs_unpinpg().
     * All pinners of the same page use the same UFC map descriptor.
     */
    MS_SMP_ASSERT(size == ADVFS_METADATA_PGSZ_IN_FOBS * ADVFS_FOB_SZ);


    MS_SMP_ASSERT(bsbufp && bsbufp->bsb_magicid == BSBUFMAGIC);

    bsbufp->bsb_fcachemapdesc = fcacheDesc;
    MS_SMP_ASSERT(bsbufp->bsb_fcachemapdesc != NULL);

    /* Pass back the pinpg handle and virtual address of the data. */
    *bfPageRefH = (bfPageRefHT) bsbufp;
    *bfPageAddr = (void *)fcacheDesc->fm_vaddr;

    /*If we did any IO, check the page. But do not verify on an overwrite. */
    if ((mflags & MF_VERIFY_PAGE) && !(mflags & MF_OVERWRITE) &&
        (priv_param.app_flags & APP_IO_PERFORMED)) {
        /*
         * This check will do quick basic checks on the metapage to make
         * sure we got the right one.  
         */
        advfs_verify_meta_page( bfap, foffset, *bfPageAddr );
    }

    return (EOK);

error_cleanup:
    /* Ignore secondary error. */
    fcache_as_unmap(fcacheDesc, NULL, 0, FAU_FREE);
    *bfPageRefH = NULL;
    *bfPageAddr = NULL;
    return (sts);
}

/*
 * bs_unpinpg
 *
 * Given a buffer page handle and the type of unpin,
 * ( clean, dirty, or immediate ) unpin the metadata or log page.
 * Do not use on user data files.
 */

statusT
bs_unpinpg(
           bfPageRefHT bfPageRefH,      /* in */
           logRecAddrT wrtAhdLogAddr,   /* in - LOG case only */
           bsUnpinModeT modeNcache      /* in */
           )
{
    struct bsBuf *bsbufp;
    statusT sts, error = EOK;
    fcache_map_dsc_t *fcache_descp;
    logDescT * logdescp;
    bfPageCacheHintT cacheHint = modeNcache.cacheHint;
    lsnT lsn;
    int32_t unmap_flags;
    int32_t do_flush = FALSE;
    int32_t do_logflush = FALSE; /* Only BS_LOG_FLUSH case sets this to TRUE */
    uint64_t dummy;

    /* vfast related fields */
    struct timeval new_time;
    int32_t wday, totHotCnt;

    /* Convert the pinpg handle to a bsBuf and get the UFC map descriptor. */
    bsbufp = (struct bsBuf *)bfPageRefH;
    MS_SMP_ASSERT(bsbufp && bsbufp->bsb_magicid == BSBUFMAGIC);
    fcache_descp = bsbufp->bsb_fcachemapdesc;
    MS_SMP_ASSERT(fcache_descp);

    /* Keep panic to prevent data corruption if bsb_writeref really is 0. */
    if( bsbufp->bsb_writeref == 0 ) {
        ADVFS_SAD0("bs_unpinpg: called with buffer not pinned");
    }

    /*
     * Send a page write message to autotune/vfast/smartstore if enough have
     * occurred.  Skip reserved files since they can't be migrated anyway.
     */
    if (SS_is_running &&
       (!BS_BFTAG_RSVD (bsbufp->bsb_bfaccess->tag)) &&
	bsbufp->bsb_bfaccess->dmnP->ssDmnInfo.ssDmnSmartPlace &&
       (bsbufp->bsb_bfaccess->dmnP->ssDmnInfo.ssDmnState == SS_ACTIVATED ||
	bsbufp->bsb_bfaccess->dmnP->ssDmnInfo.ssDmnState == SS_SUSPEND) ) {

	new_time = get_system_time();
	GETCURRWDAY (new_time.tv_sec, wday);
	ADVFS_ATOMIC_FETCH_INCR (&bsbufp->bsb_bfaccess->ssHotCnt[wday], &dummy);
	for (totHotCnt = 0, wday = 0; wday < SS_HOT_TRACKING_DAYS; wday++) {
	    totHotCnt += bsbufp->bsb_bfaccess->ssHotCnt[wday];
	}
	if (totHotCnt >
	    bsbufp->bsb_bfaccess->dmnP->ssDmnInfo.ssAccessThreshHits) {
	    ss_snd_hot (HOT_ADD_UPDATE_WRITE, bsbufp->bsb_bfaccess);
	}
    }

    /* Update statistics counters. */
    if (AdvfsLockStats) {
        switch (modeNcache.rlsMode) {
        case BS_MOD_SYNC:
           ADVFS_ATOMIC_FETCH_INCR(
                &bsbufp->bsb_bfaccess->dmnP->bcStat.unpinCnt.blocking,
                &dummy);
           break;
        case BS_MOD_LAZY:
            ADVFS_ATOMIC_FETCH_INCR(
                &bsbufp->bsb_bfaccess->dmnP->bcStat.unpinCnt.lazy,
                &dummy);         
            break;
        case BS_NOMOD:
            ADVFS_ATOMIC_FETCH_INCR(
                &bsbufp->bsb_bfaccess->dmnP->bcStat.unpinCnt.clean,
                &dummy);
            break;
        case BS_LOG_PAGE:
            ADVFS_ATOMIC_FETCH_INCR(
                &bsbufp->bsb_bfaccess->dmnP->bcStat.unpinCnt.log,
                &dummy);
            break;
        }
    }
    spin_lock(&bsbufp->bsb_lock);

    switch(modeNcache.rlsMode) {
    case BS_MOD_SYNC:
        /* This case requires the caller to be the last unpin.
         * Clear the UFC map descriptor since this is the last pin.
         * Set the bsBuf flag indicating the unpin modified the page.
         * 
         * Callers of this unpin case must ensure they are the last
         * unpinner. This is the way AdvFS is designed and should
         * remain. If not, then you are making a conscious decision
         * that it is correct to write the metadata to disk while some
         * other pinner is modifying the metadata and allowing the
         * possibility of data going to disk in an unknown state.
         */
        MS_SMP_ASSERT(bsbufp->bsb_bfaccess->dataSafety == BFD_METADATA);
        MS_SMP_ASSERT(bsbufp->bsb_writeref == 1);
        bsbufp->bsb_writeref--;
        bsbufp->bsb_fcachemapdesc = NULL;
        spin_unlock(&bsbufp->bsb_lock);
        unmap_flags = FAU_SYNC | FAU_CACHE;
        break;

    case BS_MOD_LAZY:
        MS_SMP_ASSERT(bsbufp->bsb_bfaccess->dataSafety == BFD_METADATA ||
                      bsbufp->bsb_bfaccess->dataSafety == BFD_LOG);
        MS_SMP_ASSERT(bsbufp->bsb_writeref > 0);
        /* Last unpin determines whether to do IO now or later.
         * Clear the UFC map descriptor only if this is the last pin.
         * Clear the flush waiting flag if starting IO.
         * In either case, set the bsBuf flag indicating this unpinner
         * modified the page.
         */
        if (--bsbufp->bsb_writeref == 0) {
            bsbufp->bsb_fcachemapdesc = NULL;
            if (bsbufp->bsb_flags & BSBUFFLG_FLUSHNEEDED) {
                do_flush = TRUE;
            }
            bsbufp->bsb_flags &= ~(BSBUFFLG_FLUSHNEEDED);
        }
        spin_unlock(&bsbufp->bsb_lock);
        unmap_flags = do_flush ? (FAU_ASYNC | FAU_CACHE) :
                                 (FAU_WBEHIND | FAU_CACHE);
        break;

    case BS_NOMOD:
        MS_SMP_ASSERT(bsbufp->bsb_bfaccess->dataSafety == BFD_METADATA ||
                      bsbufp->bsb_bfaccess->dataSafety == BFD_LOG);
        MS_SMP_ASSERT(bsbufp->bsb_writeref > 0);
        --bsbufp->bsb_writeref;
    
        /* Since we pages only become dirty if they are modified
         * and we are claiming that we didn't modify any pages
         * then we have very little work to do here.
         */

        /* Leave the page state as is if there are other pinners. */
        if (bsbufp->bsb_writeref) {
            spin_unlock(&bsbufp->bsb_lock);
        }
        else {
            /* This is the last unpin.
             * Clear the UFC map descriptor only on the last pin.
             * If a flush thread is waiting for the last unpin then setup
             * to have the unmap do a normal flush.
             */
            bsbufp->bsb_fcachemapdesc = NULL;
            if (bsbufp->bsb_flags & BSBUFFLG_FLUSHNEEDED) {
                bsbufp->bsb_flags &= ~(BSBUFFLG_FLUSHNEEDED);
                do_flush = TRUE;
            }
            spin_unlock(&bsbufp->bsb_lock);
        }

        /* Setup to do asynchronously flush the page or not during
         * the unmap as determined above.
         */
	unmap_flags = do_flush ? (FAU_ASYNC | FAU_CACHE) : FAU_CACHE;
        break;

    case BS_LOG_PAGE:
        /* Log files only */
        MS_SMP_ASSERT(bsbufp->bsb_bfaccess->dataSafety == BFD_LOG);
        /*
         * Log pages are placed on the dirtybuf list by get_clean_pg() only
         * after that routine calls bs_pinpg() in order to keep track
         * of lsn's. Pinpg/unpinpg does not make any assumption that a bsBuf
         * is or is not on a log's dirtyBufList.
         *
         * The bsb_flushseq must be set before bsb_writeref is decremented.
         * Assumes caller is properly ordering these unpins according to
         * increasing larger LSN values.
         *
         * Give incoming LSN to page only if it is larger than the
         * existing LSN. This could happen if at IO completion we
         * serviced a page in front of this page on the dirtyBufList, in which
         * case we would have put its LSN into this page's bsb_flushseq. 
         */
        spin_lock(&bsbufp->bsb_bfaccess->dmnP->ftxLogP->dirtyBufLock); 
        if (LSN_EQ_NIL(bsbufp->bsb_flushseq)) {
            bsbufp->bsb_flushseq = wrtAhdLogAddr.lsn;
        }
        spin_unlock(&bsbufp->bsb_bfaccess->dmnP->ftxLogP->dirtyBufLock); 

        /* Clear the UFC map descriptor only on the last pin.
         * The assertion is that this must be the last unpinner.
         */
        if (--bsbufp->bsb_writeref == 0) {
             bsbufp->bsb_fcachemapdesc = NULL;       
	}
        MS_SMP_ASSERT(!bsbufp->bsb_writeref);

        /* If a flush routine is waiting for the unpin of this dirty log page,
         * then tell the UFC unmap routine to initiate IO and clear the 
         * flush waiting flag.
         * Otherwise, determine if a periodic log flush should be done.
         */
        if (bsbufp->bsb_flags & BSBUFFLG_FLUSHNEEDED) {
            do_flush = TRUE;
            bsbufp->bsb_flags &= ~(BSBUFFLG_FLUSHNEEDED);
        }
        else {
            /* 
             * Setup an asynchronous log flush every n pages only if 
             * not doing IO on the current log page during the UFC unmap
             * below. Save some info from bsBuf before unlocking it.
             */
            if ((((bsbufp->bsb_foffset) % (LOG_FLUSH_THRESHOLD_PGS *
                 ADVFS_METADATA_PGSZ_IN_FOBS * ADVFS_FOB_SZ)) == 0)) {
                do_logflush = TRUE;
                logdescp = bsbufp->bsb_bfaccess->dmnP->ftxLogP;
                lsn = bsbufp->bsb_flushseq;
            }
        }
        spin_unlock(&bsbufp->bsb_lock);
	unmap_flags = do_flush ? (FAU_ASYNC | FAU_CACHE) :
                                 (FAU_WBEHIND | FAU_CACHE); 
        break;

    default:
        /* This is an invalid case. */
        spin_unlock(&bsbufp->bsb_lock);
        error = EBAD_PARAMS;
        unmap_flags = FAU_CACHE;
        break;
    } /* end switch */

    /*
     * We need to unwire the page.
     */
#ifdef WIRE_METADATA_ON_PIN
    fcache_as_munlock(fcache_descp, 
                      fcache_descp->fm_vaddr,
                      fcache_descp->fm_size,
                      FAMU_MUNLOCK);
#endif

    /* Release the metadata page with the caching flags from above.
     * Unmap implicitly decrements the wire reference count on the page.
     */
    sts = fcache_as_unmap(fcache_descp,
                          fcache_descp->fm_vaddr,
                          0, /*use default size in mapping */
                          unmap_flags);
    /* Return first error */
    if (sts && (error == EOK)) {
        error = sts;
    }

    /* Initiate an asynchronous log flush if setup by the BS_LOG_CASE above.
     * The log flush must happen after releasing the UFC map descriptor.
     */
    if (do_logflush){
        advfs_lgr_flush(logdescp, lsn, TRUE);
    }
    return(error);
}
#ifdef ADVFS_SMP_ASSERT

/* March thru the entire bsBuf hashtable and make sure
 * there are no bsBufs still in the table that are part of
 * the passed in domain ID.
 */

void bs_bsbuf_hashtable_dbug_compare(struct bsBuf *bsBuf,
                                bfDomainIdT *domainId)
{
    MS_SMP_ASSERT(!BS_UID_EQL(bsBuf->bsb_domainId, 
                              *domainId));
}

#endif
